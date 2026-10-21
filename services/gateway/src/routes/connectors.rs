//! Monitoring, docs and pipeline connections (New Relic, Confluence,
//! CloudWatch metrics, Airflow) that a team turns on for its agents, and the
//! check of the team's AWS role that says whether it can read metrics and
//! Airflow yet.
//!
//! Admins only. Keys are sealed for the team and never returned. Every test
//! call is written to the data-source activity log like an agent's lookup.
//! Coderhelm never changes anything in a team's AWS account: when the role
//! lacks a permission, the page links to a stack the team adds itself.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::live::aws::AwsTarget;
use common::live::confluence::{check_site, Confluence};
use common::live::connectors::{self, check_page_ids, ConnKind, Connector};
use common::live::http::Https;
use common::live::newrelic::NewRelic;
use common::live::opensearch::{check_endpoint, check_index, OpenSearch};
use common::live::rest::{check_base, check_header, check_path, Rest, MAX_PATHS};
use common::live::store::{self, Activity};
use common::live::{airflow, identity, metrics, secret, StepError};
use serde_json::{json, Value};
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// The add-on stack that gives the existing CoderHelmLogReader role read
/// access to metrics and Airflow.
pub const OPS_TEMPLATE_URL: &str =
    "https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader-ops.yaml";
pub const OPS_STACK_NAME: &str = "CoderHelmLogReaderOps";
const MAX_LIST: usize = 50;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if matches!(claims.role.as_str(), "admin" | "owner") {
        Ok(())
    } else {
        Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins can manage data sources.",
        ))
    }
}

/// A connection's id from the path: a kind, or `rest:<name>` for one of the
/// team's REST APIs.
fn kind_of(raw: &str) -> Result<(ConnKind, String), (StatusCode, Json<Value>)> {
    let (k, name) = raw.split_once(':').unwrap_or((raw, ""));
    let kind =
        ConnKind::parse(k).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such connection."))?;
    if !name.is_empty() {
        if !connectors::nameable(kind) {
            return Err(fail(StatusCode::NOT_FOUND, "No such connection."));
        }
        connectors::check_name(name).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    }
    Ok((kind, name.to_string()))
}

fn step(e: StepError) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": e.message, "fix": e.fix, "step": e.step })),
    )
}

/// A list of short strings from the body, each checked by `ok`.
fn strings(
    body: &Value,
    key: &str,
    ok: impl Fn(&str) -> bool,
    what: &str,
) -> Result<Option<Vec<String>>, (StatusCode, Json<Value>)> {
    let Some(v) = body.get(key) else {
        return Ok(None);
    };
    let list: Vec<String> = v
        .as_array()
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, &format!("{what} must be a list.")))?
        .iter()
        .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty())
        .collect();
    if list.len() > MAX_LIST {
        return Err(fail(StatusCode::BAD_REQUEST, &format!("Too many {what}.")));
    }
    if let Some(bad) = list.iter().find(|s| !ok(s)) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("{bad} isn't a valid {what}."),
        ));
    }
    Ok(Some(list))
}

/// The team's AWS connections (the log reader role), by account.
async fn aws_connections(
    state: &AppState,
    team_id: &str,
) -> Result<Vec<HashMap<String, AttributeValue>>, (StatusCode, Json<Value>)> {
    state
        .dynamo
        .query()
        .table_name(&state.config.aws_insights_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":p", AttributeValue::S("AWS_CONN#".into()))
        .send()
        .await
        .map(|o| o.items().to_vec())
        .map_err(|e| internal("list aws connections", e.to_string()))
}

fn attr(i: &HashMap<String, AttributeValue>, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// The log reader role of one connected account (or the only one), with its
/// private ID, in `region` (or the connection's).
async fn log_reader(
    state: &AppState,
    team_id: &str,
    account: Option<&str>,
    region: Option<&str>,
) -> Result<AwsTarget, (StatusCode, Json<Value>)> {
    let conns = aws_connections(state, team_id).await?;
    let pick = match account.filter(|a| !a.is_empty()) {
        Some(a) => conns.iter().find(|c| attr(c, "account_id") == a),
        None if conns.len() == 1 => conns.first(),
        None => None,
    };
    let c = pick.ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            if conns.is_empty() {
                "Connect your AWS account in Settings → Integrations first."
            } else {
                "Pick which AWS account to use."
            },
        )
    })?;
    let region = region
        .filter(|r| !r.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| attr(c, "region"));
    identity::log_reader_target(&attr(c, "role_arn"), &region, &attr(c, "external_id"))
        .map_err(step)
}

/// The link that adds the metrics and Airflow permissions to the team's
/// existing CoderHelmLogReader role, as a stack in its own account.
pub fn ops_stack_url(region: &str) -> String {
    format!(
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?stackName={OPS_STACK_NAME}&templateURL={}&param_RoleName=CoderHelmLogReader",
        urlencoding::encode(OPS_TEMPLATE_URL)
    )
}

async fn record(
    state: &AppState,
    claims: &Claims,
    c: &Connector,
    what: &str,
    r: &Result<String, StepError>,
    ms: u128,
) {
    let (outcome, detail) = match r {
        Ok(note) => ("ok", note.clone()),
        Err(e) if e.is_refusal() => ("refused", e.message.clone()),
        Err(e) => ("failed", e.message.clone()),
    };
    let _ = store::log_activity(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &Activity {
            source_id: c.kind().activity_id(),
            source_name: c.label(),
            actor: claims.email.clone(),
            repo: String::new(),
            pr: 0,
            query: what.to_string(),
            outcome,
            detail,
            ms,
        },
    )
    .await;
}

/// One read that shows the connection works. Returns a line for the page.
async fn try_connection(
    state: &AppState,
    claims: &Claims,
    c: &Connector,
    key: &str,
) -> Result<String, StepError> {
    match c.kind() {
        ConnKind::NewRelic => {
            let a = NewRelic {
                key,
                region: &c.region,
                accounts: &c.account_ids,
                http: &Https,
            }
            .accounts()
            .await?;
            let n = a.data.as_array().map(|x| x.len()).unwrap_or(0);
            if n == 0 {
                return Err(StepError::new(
                    "read",
                    "The key works but sees none of the accounts you listed.",
                    "Check the account ids, or leave them empty to allow every account the key sees.",
                ));
            }
            Ok(format!("Signed in; {}", common::live::count(n, "account")))
        }
        ConnKind::Confluence => {
            let who = Confluence {
                conn: c,
                token: key,
                http: &Https,
            }
            .whoami()
            .await?;
            Ok(format!("Signed in as {who}"))
        }
        ConnKind::OpenSearch => {
            let os = OpenSearch {
                conn: c,
                password: key,
                http: &Https,
            };
            let who = os.whoami().await?;
            let shared = os.indexes().await?;
            if shared.text == "No shared indexes." {
                return Err(StepError::new(
                    "read",
                    "Signed in, but the user sees none of the indexes you listed.",
                    "Check the index names, or leave the list empty to allow every index the user sees.",
                ));
            }
            Ok(who)
        }
        ConnKind::Rest => {
            let first = c.paths.first().cloned().unwrap_or_default();
            let a = Rest {
                conn: c,
                key,
                http: &Https,
            }
            .get(&first, None)
            .await?;
            Ok(format!(
                "Signed in; {} answered ({} characters)",
                a.asked,
                a.text.len()
            ))
        }
        ConnKind::Metrics => {
            let t = c.aws.as_ref().ok_or_else(|| {
                StepError::new("reach", "No AWS role is set.", "Pick the AWS account.")
            })?;
            let session = common::live::aws::assume(&state.sts, t, &claims.email).await?;
            if metrics::probe(&session).await? {
                Ok("The role can read CloudWatch metrics".into())
            } else {
                Err(StepError::new(
                    "sign_in",
                    "The CoderHelm role can't read CloudWatch metrics yet.",
                    "Add the read permissions with the one-click stack in Settings → Integrations, then check again.",
                ))
            }
        }
        ConnKind::Airflow => {
            let t = c.aws.as_ref().ok_or_else(|| {
                StepError::new("reach", "No AWS role is set.", "Pick the AWS account.")
            })?;
            let session = common::live::aws::assume(&state.sts, t, &claims.email).await?;
            let Some(envs) = airflow::list_environments(&session).await? else {
                return Err(StepError::new(
                    "sign_in",
                    "The CoderHelm role can't read Airflow yet.",
                    "Add the read permissions with the one-click stack in Settings → Integrations, then check again.",
                ));
            };
            for e in &c.environments {
                if !envs.contains(e) {
                    return Err(StepError::new(
                        "read",
                        format!("There's no MWAA environment called {e} in {}.", t.region),
                        format!("Environments there: {}.", envs.join(", ")),
                    ));
                }
                let get = airflow::build("health", &json!({ "environment": e }), &c.environments)?;
                airflow::read(&airflow::Mwaa { session: &session }, &get).await?;
            }
            Ok(format!("The role can read {}", c.environments.join(", ")))
        }
    }
}

/// GET /api/connectors — every kind, connected or not, and the team's AWS
/// accounts the metrics and Airflow connections can use.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let saved = connectors::list(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
    )
    .await
    .map_err(|e| internal("list connectors", e))?;
    // Metrics and Airflow are abilities of the AWS account (Integrations).
    // One entry per kind; REST APIs one per connected API, then one to add
    // another.
    let mut items: Vec<Value> = Vec::new();
    for k in ConnKind::ALL
        .iter()
        .filter(|k| !k.uses_aws() && !connectors::nameable(**k))
    {
        let mine: Vec<&Connector> = saved.iter().filter(|c| c.kind() == *k).collect();
        items.extend(mine.iter().map(|c| c.public_json()));
        if mine.is_empty() || connectors::nameable(*k) {
            // Adding another needs a name (the unnamed slot may be taken).
            let needs_name = *k == ConnKind::Rest || !mine.is_empty();
            let id = if mine.is_empty() {
                k.as_str().to_string()
            } else {
                format!("{}:", k.as_str())
            };
            items.push(json!({ "kind": k.as_str(), "id": id, "label": k.label(), "connected": false, "needs_name": needs_name }));
        }
    }
    let aws: Vec<Value> = aws_connections(&state, &claims.team_id)
        .await?
        .iter()
        .map(|c| {
            json!({
                "account": attr(c, "account_id"),
                "region": attr(c, "region"),
                "role": attr(c, "role_arn").rsplit('/').next().unwrap_or(""),
            })
        })
        .collect();
    Ok(Json(json!({ "connectors": items, "aws_accounts": aws })))
}

/// PUT /api/connectors/:kind — saves a connection's settings (and key, when
/// one is sent), tests it, and turns it on when the test passes and
/// `enabled` is true.
pub async fn save(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let (kind, mut name) = kind_of(&kind)?;
    // A new named connection (a REST API, another OpenSearch cluster) is
    // named in the body (the page's add form); saving one under an existing
    // name updates that one.
    if connectors::nameable(kind) && name.is_empty() {
        if let Some(n) = body["name"]
            .as_str()
            .map(str::trim)
            .filter(|n| !n.is_empty())
        {
            connectors::check_name(n).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
            name = n.to_string();
        }
    }
    if connectors::nameable(kind) {
        return Err(fail(
            StatusCode::GONE,
            "REST APIs and OpenSearch are data sources now. Add them in Data sources.",
        ));
    }
    if kind.uses_aws() {
        // Metrics and Airflow are abilities of the connected AWS account now,
        // found by checking its role.
        return Err(fail(
            StatusCode::GONE,
            "CloudWatch metrics and Airflow come with your AWS account now. Turn them on in Settings → Integrations.",
        ));
    }
    let table = &state.config.settings_table_name;
    let mut c = connectors::get(&state.dynamo, table, &claims.team_id, kind, &name)
        .await
        .map_err(|e| internal("get connector", e))?
        .unwrap_or_else(|| Connector {
            name: name.clone(),
            ..Connector::new(kind)
        });
    let text = |k: &str| body[k].as_str().map(str::trim).map(str::to_string);

    // Key: replaced only when one is sent.
    let mut key = String::new();
    if let Some(k) = text("key").filter(|k| !k.is_empty()) {
        if k.len() > 4000 || k.chars().any(char::is_control) {
            return Err(fail(StatusCode::BAD_REQUEST, "Paste the key as one line."));
        }
        c.key_sealed = secret::seal(
            &state.kms,
            &state.config.secrets_key_id,
            &claims.team_id,
            &k,
        )
        .await
        .map_err(|e| internal("seal connector key", e))?;
        c.key_hint = secret::hint(&k);
        key = k;
    } else if !c.key_sealed.is_empty() {
        key = secret::open(&state.kms, &claims.team_id, &c.key_sealed)
            .await
            .map_err(|e| internal("open connector key", e))?;
    }
    if let Some(g) = body["instructions"].as_str() {
        let g = g.trim();
        if g.chars().count() > connectors::MAX_INSTRUCTIONS {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                &format!(
                    "Keep the guide under {} characters.",
                    connectors::MAX_INSTRUCTIONS
                ),
            ));
        }
        c.instructions = g.to_string();
    }
    if let Some(r) = strings(
        &body,
        "repos",
        |r| r.contains('/') && r.len() <= 200,
        "repository",
    )? {
        c.repos = r;
    }
    match kind {
        ConnKind::NewRelic => {
            if let Some(r) = text("region") {
                if !matches!(r.as_str(), "us" | "eu") {
                    return Err(fail(StatusCode::BAD_REQUEST, "The region is us or eu."));
                }
                c.region = r;
            }
            if c.region.is_empty() {
                c.region = "us".into();
            }
            if let Some(a) = strings(
                &body,
                "account_ids",
                |a| a.len() <= 20 && a.chars().all(|ch| ch.is_ascii_digit()),
                "account id",
            )? {
                c.account_ids = a;
            }
            if key.is_empty() {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Paste a New Relic user key (it starts with NRAK-).",
                ));
            }
        }
        ConnKind::Confluence => {
            if let Some(s) = text("site") {
                c.site = check_site(&s).map_err(step)?;
            }
            if let Some(e) = text("email") {
                if !e.contains('@') || e.len() > 200 {
                    return Err(fail(
                        StatusCode::BAD_REQUEST,
                        "Enter the email of the Atlassian account the token belongs to.",
                    ));
                }
                c.email = e;
            }
            if let Some(sp) = strings(
                &body,
                "spaces",
                |s| {
                    s.len() <= 255
                        && s.chars()
                            .all(|ch| ch.is_ascii_alphanumeric() || "~_".contains(ch))
                },
                "space key",
            )? {
                c.spaces = sp;
            }
            if let Some(w) = body["write_enabled"].as_bool() {
                c.write_enabled = w;
            }
            if let Some(p) = strings(&body, "write_pages", |_| true, "page id")? {
                check_page_ids(&p).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
                c.write_pages = p;
            }
            if c.site.is_empty() || c.email.is_empty() || key.is_empty() {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Enter the site, the email and an API token.",
                ));
            }
        }
        ConnKind::OpenSearch => {
            if let Some(s) = text("site") {
                c.site = check_endpoint(&s).map_err(step)?;
            }
            if let Some(u) = text("email") {
                if u.is_empty() || u.len() > 200 || u.chars().any(char::is_control) {
                    return Err(fail(
                        StatusCode::BAD_REQUEST,
                        "Enter the cluster user CoderHelm signs in as.",
                    ));
                }
                c.email = u;
            }
            if let Some(i) = strings(&body, "indexes", |i| check_index(i).is_ok(), "index name")? {
                c.indexes = i;
            }
            if let Some(h) = strings(
                &body,
                "hidden_fields",
                |h| !h.is_empty() && h.len() <= 100,
                "field name",
            )? {
                c.hidden_fields = h;
            }
            if c.site.is_empty() || c.email.is_empty() || key.is_empty() {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Enter the cluster address, a read-only user and its password.",
                ));
            }
        }
        ConnKind::Rest => {
            if let Some(s) = text("site") {
                c.site = check_base(&s).map_err(step)?;
            }
            if let Some(h) = text("header_name") {
                c.header_name = check_header(&h).map_err(step)?;
            }
            if let Some(p) = strings(
                &body,
                "paths",
                |p| check_path(p).is_ok(),
                "path (like /v1/items)",
            )? {
                if p.len() > MAX_PATHS {
                    return Err(fail(
                        StatusCode::BAD_REQUEST,
                        &format!("At most {MAX_PATHS} paths."),
                    ));
                }
                c.paths = p;
            }
            if let Some(h) = strings(
                &body,
                "hidden_fields",
                |h| !h.is_empty() && h.len() <= 100,
                "field name",
            )? {
                c.hidden_fields = h;
            }
            if c.site.is_empty() || c.header_name.is_empty() || c.paths.is_empty() || key.is_empty()
            {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Enter the API address, the header the key goes in, the key and at least one path agents may read.",
                ));
            }
        }
        ConnKind::Metrics | ConnKind::Airflow => {
            if let Some(r) = text("region").filter(|r| !r.is_empty()) {
                if !super::log_analyzer::valid_region(&r) {
                    return Err(fail(
                        StatusCode::BAD_REQUEST,
                        "That AWS region isn't valid.",
                    ));
                }
                c.region = r;
            }
            let t = log_reader(
                &state,
                &claims.team_id,
                text("account").as_deref(),
                Some(c.region.as_str()),
            )
            .await?;
            c.region = t.region.clone();
            c.aws = Some(t);
            if kind == ConnKind::Airflow {
                if let Some(e) = strings(
                    &body,
                    "environments",
                    |e| {
                        e.len() <= 80
                            && e.chars()
                                .all(|ch| ch.is_ascii_alphanumeric() || "-_".contains(ch))
                    },
                    "environment name",
                )? {
                    c.environments = e;
                }
                if c.environments.is_empty() {
                    return Err(fail(
                        StatusCode::BAD_REQUEST,
                        "Name at least one MWAA environment.",
                    ));
                }
            }
        }
    }
    let started = std::time::Instant::now();
    let tested = try_connection(&state, &claims, &c, &key).await;
    record(
        &state,
        &claims,
        &c,
        "test the connection",
        &tested,
        started.elapsed().as_millis(),
    )
    .await;
    match &tested {
        Ok(note) => {
            c.status = "ready".into();
            c.status_message = note.clone();
        }
        Err(e) => {
            c.status = "error".into();
            c.status_message = format!("{} {}", e.message, e.fix);
        }
    }
    c.enabled = body["enabled"].as_bool().unwrap_or(c.enabled) && c.status == "ready";
    c.updated_at = chrono::Utc::now().to_rfc3339();
    c.updated_by = claims.email.clone();
    connectors::put(&state.dynamo, table, &claims.team_id, &c)
        .await
        .map_err(|e| internal("save connector", e))?;
    info!(team_id = %claims.team_id, id = %c.id(), status = %c.status, "Connection saved");
    Ok(Json(json!({ "connector": c.public_json() })))
}

/// POST /api/connectors/:kind/test — tests a saved connection again.
pub async fn test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
) -> Reply {
    admin(&claims)?;
    let (kind, name) = kind_of(&kind)?;
    let table = &state.config.settings_table_name;
    let mut c = connectors::get(&state.dynamo, table, &claims.team_id, kind, &name)
        .await
        .map_err(|e| internal("get connector", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That connection isn't set up."))?;
    let key = if c.key_sealed.is_empty() {
        String::new()
    } else {
        secret::open(&state.kms, &claims.team_id, &c.key_sealed)
            .await
            .map_err(|e| internal("open connector key", e))?
    };
    let started = std::time::Instant::now();
    let tested = try_connection(&state, &claims, &c, &key).await;
    record(
        &state,
        &claims,
        &c,
        "test the connection",
        &tested,
        started.elapsed().as_millis(),
    )
    .await;
    let (status, message) = match &tested {
        Ok(n) => ("ready", n.clone()),
        Err(e) => ("error", format!("{} {}", e.message, e.fix)),
    };
    c.status = status.into();
    c.status_message = message;
    if status != "ready" {
        c.enabled = false;
    }
    connectors::put(&state.dynamo, table, &claims.team_id, &c)
        .await
        .map_err(|e| internal("save connector", e))?;
    Ok(Json(json!({ "connector": c.public_json() })))
}

/// DELETE /api/connectors/:kind — removes a connection and its key.
pub async fn remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
) -> Reply {
    admin(&claims)?;
    let (kind, name) = kind_of(&kind)?;
    connectors::delete(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        kind,
        &name,
    )
    .await
    .map_err(|e| internal("delete connector", e))?;
    info!(team_id = %claims.team_id, kind = kind.as_str(), name, "Connection removed");
    Ok(Json(json!({ "ok": true })))
}

#[derive(serde::Deserialize)]
pub struct AwsCheckQuery {
    account: Option<String>,
}

/// GET /api/connectors/aws-check?account= — takes on the team's log reader
/// role and tries one read of logs, metrics and Airflow. When metrics or
/// Airflow can't be read, returns the one-click link that adds them.
pub async fn aws_check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<AwsCheckQuery>,
) -> Reply {
    admin(&claims)?;
    let t = log_reader(&state, &claims.team_id, q.account.as_deref(), None).await?;
    let check = identity::check(
        &state.sts,
        &t,
        vec!["Log alerts".into()],
        &claims.email,
        true,
    )
    .await;
    let missing = check.missing();
    Ok(Json(json!({
        "check": check,
        "missing": missing,
        "update_url": if missing.is_empty() { None } else { Some(ops_stack_url(&t.region)) },
        "stack_name": OPS_STACK_NAME,
        "template_url": OPS_TEMPLATE_URL,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_update_link_opens_the_add_on_stack_for_the_existing_role() {
        let u = ops_stack_url("us-west-2");
        assert!(u.starts_with("https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/quickcreate?"));
        assert!(u.contains("stackName=CoderHelmLogReaderOps"));
        assert!(u.contains("templateURL=https%3A%2F%2Fcoderhelm-public.s3.amazonaws.com%2Fcfn%2Fcoderhelm-log-reader-ops.yaml"));
        assert!(u.contains("param_RoleName=CoderHelmLogReader"));
    }

    #[test]
    fn lists_are_checked() {
        let body = json!({ "account_ids": ["123", "abc"] });
        assert!(strings(
            &body,
            "account_ids",
            |a| a.chars().all(|c| c.is_ascii_digit()),
            "account id"
        )
        .is_err());
        let body = json!({ "account_ids": ["123", " "] });
        assert_eq!(
            strings(
                &body,
                "account_ids",
                |a| a.chars().all(|c| c.is_ascii_digit()),
                "account id"
            )
            .unwrap(),
            Some(vec!["123".to_string()])
        );
        assert_eq!(strings(&json!({}), "x", |_| true, "x").unwrap(), None);
        assert!(strings(&json!({ "x": "notalist" }), "x", |_| true, "x").is_err());
    }
}
