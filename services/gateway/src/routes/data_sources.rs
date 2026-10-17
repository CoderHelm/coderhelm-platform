//! Data sources: systems a team connects so the reviewer can look facts up.
//!
//! Every call that reaches a team's system goes through `common::live`, which
//! allows reads only, to the one registered address. These routes add who may
//! do it (admins), where the key is kept (sealed, never returned) and the
//! record of each lookup.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::live::api::{self, ApiSource, Check, ConnectReport};
use common::live::aws::{self as live_aws, AwsTarget};
use common::live::graphql::check_read_only;
use common::live::net::parse_endpoint;
use common::live::redact::{hidden_terms, is_hidden};
use common::live::store::{self, Activity, Kind, Source};
use common::live::{db, logs};
use common::live::{secret, StepError};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// Most characters of a lookup result returned to the dashboard.
const TRY_RESULT_CHARS: usize = 20_000;
const MAX_KEY_CHARS: usize = 4_000;
const MAX_HIDDEN_FIELDS: usize = 100;
const MAX_REPOS: usize = 200;
const MAX_LOG_GROUPS: usize = 50;

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

fn table(state: &AppState) -> &str {
    &state.config.settings_table_name
}

fn text<'a>(body: &'a Value, key: &str) -> &'a str {
    body[key].as_str().unwrap_or("").trim()
}

fn key_from(body: &Value) -> Result<&str, (StatusCode, Json<Value>)> {
    let key = text(body, "key");
    if key.is_empty() || key.len() > MAX_KEY_CHARS || key.chars().any(|c| c.is_control()) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Paste the key as a single line of text.",
        ));
    }
    Ok(key)
}

async fn load(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<Source, (StatusCode, Json<Value>)> {
    store::get_source(&state.dynamo, table(state), team_id, id)
        .await
        .map_err(|e| internal("load data source", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That data source no longer exists."))
}

async fn save(
    state: &AppState,
    team_id: &str,
    source: &Source,
) -> Result<(), (StatusCode, Json<Value>)> {
    store::put_source(&state.dynamo, table(state), team_id, source)
        .await
        .map_err(|e| internal("save data source", e))
}

async fn save_schema(state: &AppState, team_id: &str, id: &str, report: &ConnectReport) {
    let Some(schema) = &report.schema else { return };
    let put = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(Source::schema_s3_key(team_id, id))
        .content_type("text/plain")
        .body(schema.text.clone().into_bytes().into())
        .send()
        .await;
    if let Err(e) = put {
        warn!(error = %e, "Could not store the API's description; lookups still work");
    }
}

fn apply_report(source: &mut Source, report: &ConnectReport) {
    source.auth = report.auth;
    source.status = "ready".to_string();
    source.status_message.clear();
    source.last_checked_at = chrono::Utc::now().to_rfc3339();
    if let Some(schema) = &report.schema {
        source.has_mutations = schema.has_mutations;
        source.kinds = Some(schema.kinds);
        source.schema_available = true;
        source.samples = schema.samples.clone();
    }
}

fn report_json(report: &ConnectReport) -> Value {
    json!({
        "ok": report.ok,
        "checks": report.checks,
        "kinds": report.schema.as_ref().map(|s| s.kinds),
        "has_mutations": report.schema.as_ref().map(|s| s.has_mutations).unwrap_or(false),
        "schema_available": report.schema.is_some(),
    })
}

/// GET /api/data-sources
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let mut sources = store::list_sources(&state.dynamo, table(&state), &claims.team_id)
        .await
        .map_err(|e| internal("list data sources", e))?;
    sources.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    let sources: Vec<Value> = sources.iter().map(Source::public_json).collect();
    Ok(Json(json!({ "sources": sources })))
}

/// GET /api/data-sources/activity?limit=50&before=<cursor>
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Reply {
    admin(&claims)?;
    let limit = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
    let before = q.get("before").map(String::as_str);
    let events = store::list_activity(&state.dynamo, table(&state), &claims.team_id, limit, before)
        .await
        .map_err(|e| internal("list lookups", e))?;
    Ok(Json(json!({ "events": events })))
}

/// POST /api/data-sources/api/probe  { url }
pub async fn probe(Extension(claims): Extension<Claims>, Json(body): Json<Value>) -> Reply {
    admin(&claims)?;
    Ok(Json(json!(api::probe(text(&body, "url")).await)))
}

/// POST /api/data-sources/api/check  { url, key } — checks without saving.
pub async fn check(Extension(claims): Extension<Claims>, Json(body): Json<Value>) -> Reply {
    admin(&claims)?;
    let key = key_from(&body)?;
    let report = api::connect_checks(text(&body, "url"), key).await;
    Ok(Json(report_json(&report)))
}

/// POST /api/data-sources/api  { url, key, name? } — checks, then saves, off.
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let key = key_from(&body)?;
    let url = text(&body, "url");
    let endpoint =
        parse_endpoint(url).map_err(|e| fail(StatusCode::BAD_REQUEST, &e.to_string()))?;

    let existing = store::list_sources(&state.dynamo, table(&state), &claims.team_id)
        .await
        .map_err(|e| internal("list data sources", e))?;
    if existing.len() >= store::MAX_SOURCES {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You've reached the limit of connected sources. Remove one first.",
        ));
    }
    if existing.iter().any(|s| s.url == endpoint.url.as_str()) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That API is already connected.",
        ));
    }

    let report = api::connect_checks(url, key).await;
    if !report.ok {
        let mut out = report_json(&report);
        out["error"] = json!("The connection checks didn't pass.");
        return Err((StatusCode::BAD_REQUEST, Json(out)));
    }

    let sealed = secret::seal(
        &state.kms,
        &state.config.secrets_key_id,
        &claims.team_id,
        key,
    )
    .await
    .map_err(|e| internal("seal data source key", e))?;
    let name = match text(&body, "name") {
        "" => endpoint.host.clone(),
        n => common::truncate_str(n, 80).to_string(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let mut source = Source {
        id: ulid::Ulid::new().to_string(),
        kind: Kind::Api,
        name,
        url: endpoint.url.to_string(),
        auth: report.auth,
        key_sealed: sealed,
        key_hint: secret::hint(key),
        // Saved switched off: a person turns it on after reviewing what it can do.
        enabled: false,
        setup_complete: false,
        status: "ready".to_string(),
        status_message: String::new(),
        has_mutations: false,
        kinds: None,
        schema_available: false,
        samples: Vec::new(),
        hidden_fields: store::default_hidden_fields(),
        repos: Vec::new(),
        last_checked_at: now.clone(),
        created_at: now,
        created_by: claims.email.clone(),
        aws: None,
    };
    apply_report(&mut source, &report);
    save_schema(&state, &claims.team_id, &source.id, &report).await;
    save(&state, &claims.team_id, &source).await?;
    info!(team = %claims.team_id, source = %source.id, "Data source connected");
    Ok(Json(
        json!({ "source": source.public_json(), "checks": report.checks }),
    ))
}

fn string_list(v: &Value, max: usize) -> Option<Vec<String>> {
    let arr = v.as_array()?;
    let mut out: Vec<String> = arr
        .iter()
        .filter_map(|x| x.as_str())
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty() && x.len() <= 120)
        .collect();
    out.dedup();
    out.truncate(max);
    Some(out)
}

/// PUT /api/data-sources/:id  { enabled?, hidden_fields?, repos?, name? }
pub async fn update(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let mut source = load(&state, &claims.team_id, &id).await?;
    if let Some(enabled) = body["enabled"].as_bool() {
        source.enabled = enabled;
        if enabled {
            source.setup_complete = true;
        }
    }
    if let Some(hidden) = string_list(&body["hidden_fields"], MAX_HIDDEN_FIELDS) {
        source.hidden_fields = hidden;
    }
    if let Some(repos) = string_list(&body["repos"], MAX_REPOS) {
        source.repos = repos;
    }
    if let (Kind::Logs, Some(groups)) = (
        source.kind,
        string_list(&body["log_groups"], MAX_LOG_GROUPS),
    ) {
        if groups.is_empty() {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Keep at least one log group, or remove the source.",
            ));
        }
        if let Some(a) = source.aws.as_mut() {
            a.log_groups = groups;
            source.kinds = Some(a.log_groups.len());
        }
    }
    match text(&body, "name") {
        "" => {}
        n => source.name = common::truncate_str(n, 80).to_string(),
    }
    save(&state, &claims.team_id, &source).await?;
    info!(team = %claims.team_id, source = %id, enabled = source.enabled, "Data source updated");
    Ok(Json(json!({ "source": source.public_json() })))
}

/// DELETE /api/data-sources/:id
pub async fn remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    store::delete_source(&state.dynamo, table(&state), &claims.team_id, &id)
        .await
        .map_err(|e| internal("delete data source", e))?;
    let _ = state
        .s3
        .delete_object()
        .bucket(&state.config.bucket_name)
        .key(Source::schema_s3_key(&claims.team_id, &id))
        .send()
        .await;
    info!(team = %claims.team_id, source = %id, "Data source removed");
    Ok(Json(json!({ "ok": true })))
}

/// POST /api/data-sources/:id/key  { key } — checks the new key, then swaps it.
pub async fn replace_key(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let key = key_from(&body)?;
    let mut source = load(&state, &claims.team_id, &id).await?;
    if source.kind != Kind::Api {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This source has no key. To change its access, run the setup script again.",
        ));
    }
    let report = api::connect_checks(&source.url, key).await;
    if !report.ok {
        let mut out = report_json(&report);
        out["error"] = json!("The connection checks didn't pass. The old key is still in place.");
        return Err((StatusCode::BAD_REQUEST, Json(out)));
    }
    source.key_sealed = secret::seal(
        &state.kms,
        &state.config.secrets_key_id,
        &claims.team_id,
        key,
    )
    .await
    .map_err(|e| internal("seal data source key", e))?;
    source.key_hint = secret::hint(key);
    apply_report(&mut source, &report);
    save_schema(&state, &claims.team_id, &id, &report).await;
    save(&state, &claims.team_id, &source).await?;
    Ok(Json(
        json!({ "source": source.public_json(), "checks": report.checks }),
    ))
}

async fn open_key(
    state: &AppState,
    team_id: &str,
    source: &Source,
) -> Result<String, (StatusCode, Json<Value>)> {
    secret::open(&state.kms, team_id, &source.key_sealed)
        .await
        .map_err(|e| internal("open data source key", e))
}

/// POST /api/data-sources/:id/test — reruns the checks with what is saved.
pub async fn test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let mut source = load(&state, &claims.team_id, &id).await?;
    let (ok, checks, mut out) = match source.kind {
        Kind::Api => {
            let key = open_key(&state, &claims.team_id, &source).await?;
            let report = api::connect_checks(&source.url, &key).await;
            if report.ok {
                apply_report(&mut source, &report);
                save_schema(&state, &claims.team_id, &id, &report).await;
            }
            (report.ok, report.checks.clone(), report_json(&report))
        }
        Kind::Logs => {
            let t = aws_of(&source)?.clone();
            let (mut checks, session) = role_checks(&state, &t, &claims.email).await;
            if let Ok(session) = &session {
                if let Err(e) = logs::list_groups(session).await {
                    checks.push(Check {
                        id: "logs",
                        label: "See your log groups",
                        ok: false,
                        detail: e.message.clone(),
                        fix: Some(e.fix.clone()),
                    });
                }
            }
            let ok = checks.iter().all(|c| c.ok);
            (ok, checks.clone(), json!({ "ok": ok, "checks": checks }))
        }
        Kind::Database => {
            let t = aws_of(&source)?.clone();
            let (mut checks, session) = role_checks(&state, &t, &claims.email).await;
            let report = db::connect_checks(session, &t).await;
            checks.extend(report.checks.into_iter().filter(|c| c.id != "role"));
            if report.ok {
                source.kinds = report.tables;
                if let Some(schema) = &report.schema {
                    source.schema_available = true;
                    source.samples = db_samples(schema);
                    save_db_schema(&state, &claims.team_id, &id, schema).await;
                }
            }
            (
                report.ok,
                checks.clone(),
                json!({ "ok": report.ok, "checks": checks, "kinds": source.kinds }),
            )
        }
    };
    source.last_checked_at = chrono::Utc::now().to_rfc3339();
    if ok {
        source.status = "ready".to_string();
        source.status_message.clear();
    } else {
        source.status = "error".to_string();
        source.status_message = checks
            .iter()
            .find(|c| !c.ok)
            .map(|c| format!("{} {}", c.detail, c.fix.clone().unwrap_or_default()))
            .unwrap_or_default();
    }
    save(&state, &claims.team_id, &source).await?;
    out["source"] = source.public_json();
    Ok(Json(out))
}

/// GET /api/data-sources/:id/samples
pub async fn samples(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let source = load(&state, &claims.team_id, &id).await?;
    match source.kind {
        Kind::Api => {}
        Kind::Logs => {
            let group = source
                .aws
                .as_ref()
                .and_then(|a| a.log_groups.iter().find(|g| !g.ends_with('*')).cloned())
                .unwrap_or_default();
            return Ok(Json(json!({ "samples": [
                { "label": "Latest lines", "query": "fields @timestamp, @message | sort @timestamp desc | limit 20", "log_groups": [group], "since": "1h" },
                { "label": "Errors in the last day", "query": "fields @timestamp, @message | filter @message like /(?i)error|exception/ | sort @timestamp desc | limit 20", "log_groups": [group], "since": "24h" },
                { "label": "Errors per hour", "query": "filter @message like /(?i)error/ | stats count(*) by bin(1h)", "log_groups": [group], "since": "24h" },
            ] })));
        }
        Kind::Database => {
            let samples: Vec<Value> = source
                .samples
                .iter()
                .filter(|(_, q)| {
                    common::live::sql::check_read_only_sql(q, &source.hidden_fields).is_ok()
                })
                .map(|(label, query)| json!({ "label": label, "query": query }))
                .collect();
            return Ok(Json(json!({ "samples": samples })));
        }
    }
    // A sample that asks for a field this team has since hidden would be
    // refused, so it isn't offered.
    let hidden = hidden_terms(&source.hidden_fields);
    let allowed = |query: &str| {
        check_read_only(query)
            .map(|q| !q.all_fields.iter().any(|f| is_hidden(f, &hidden)))
            .unwrap_or(false)
    };
    let mut samples: Vec<Value> = source
        .samples
        .iter()
        .filter(|(_, query)| allowed(query))
        .map(|(label, query)| json!({ "label": label, "query": query }))
        .collect();
    if samples.is_empty() {
        samples.push(json!({ "label": "Check that it answers", "query": "{ __typename }" }));
    }
    Ok(Json(json!({ "samples": samples })))
}

fn step_json(e: &StepError) -> Value {
    json!({ "step": e.step, "message": e.message, "fix": e.fix })
}

/// The outcome of one lookup, whichever kind of source it went to.
struct Tried {
    text: String,
    fields: Vec<String>,
    result: String,
    hidden_count: usize,
    ms: u128,
    note: String,
}

async fn record(
    state: &AppState,
    team_id: &str,
    source: &Source,
    actor: &str,
    query: &str,
    outcome: &Result<Tried, StepError>,
) {
    let (label, detail, ms) = match outcome {
        Ok(t) => ("ok", t.note.clone(), t.ms),
        Err(e) if e.is_refusal() => ("refused", e.message.clone(), 0),
        Err(e) => ("failed", e.message.clone(), 0),
    };
    let logged = store::log_activity(
        &state.dynamo,
        table(state),
        team_id,
        &Activity {
            source_id: source.id.clone(),
            source_name: source.name.clone(),
            actor: actor.to_string(),
            repo: String::new(),
            pr: 0,
            query: query.to_string(),
            outcome: label,
            detail,
            ms,
        },
    )
    .await;
    if let Err(e) = logged {
        warn!(error = %e, "Could not record a lookup");
    }
}

fn aws_of(source: &Source) -> Result<&AwsTarget, (StatusCode, Json<Value>)> {
    source.aws.as_ref().ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "This source is missing its AWS details. Connect it again.",
        )
    })
}

/// POST /api/data-sources/:id/try  { query, log_groups?, since?, until? } —
/// runs one lookup the way the agents would, and records it. Works while the
/// source is switched off, so a person can see what it does before turning it
/// on.
pub async fn try_lookup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let source = load(&state, &claims.team_id, &id).await?;
    let query = body["query"].as_str().unwrap_or("");

    let outcome: Result<Tried, StepError> = match source.kind {
        Kind::Api => {
            let key = open_key(&state, &claims.team_id, &source).await?;
            let src = ApiSource {
                url: source.url.clone(),
                auth: source.auth,
                key,
            };
            api::run_lookup(&src, query, None, &source.hidden_fields, TRY_RESULT_CHARS)
                .await
                .map(|l| Tried {
                    text: l.query,
                    fields: l.fields,
                    result: l.result,
                    hidden_count: l.hidden_count,
                    ms: l.ms,
                    note: String::new(),
                })
        }
        Kind::Database => {
            let t = aws_of(&source)?;
            match common::live::sql::check_read_only_sql(query, &source.hidden_fields) {
                Err(e) => Err(e),
                Ok(_) => match live_aws::assume(&state.sts, t, &claims.email).await {
                    Err(e) => Err(e),
                    Ok(session) => match db::verify_read_only_user(&session, t).await {
                        Err(e) => Err(e),
                        Ok(()) => {
                            db::run_sql(&session, t, query, &source.hidden_fields, TRY_RESULT_CHARS)
                                .await
                                .map(|l| Tried {
                                    text: l.statement,
                                    fields: l.tables,
                                    result: l.result,
                                    hidden_count: l.hidden_count,
                                    ms: l.ms,
                                    note: common::live::count(l.rows, "row"),
                                })
                        }
                    },
                },
            }
        }
        Kind::Logs => {
            let t = aws_of(&source)?;
            let groups = string_list(&body["log_groups"], 10).unwrap_or_default();
            match logs::check_search(
                t,
                &groups,
                query,
                body["since"].as_str(),
                body["until"].as_str(),
                chrono::Utc::now().timestamp(),
            ) {
                Err(e) => Err(e),
                Ok(search) => match live_aws::assume(&state.sts, t, &claims.email).await {
                    Err(e) => Err(e),
                    Ok(session) => {
                        logs::run_search(&session, &search, &source.hidden_fields, TRY_RESULT_CHARS)
                            .await
                            .map(|l| Tried {
                                text: search.query.clone(),
                                fields: search.groups.clone(),
                                result: l.result,
                                hidden_count: l.hidden_count,
                                ms: l.ms,
                                note: format!(
                                    "{}, {:.1} MB scanned{}",
                                    common::live::count(l.rows, "line"),
                                    l.scanned_mb,
                                    if l.partial { ", stopped early" } else { "" }
                                ),
                            })
                    }
                },
            }
        }
    };
    record(
        &state,
        &claims.team_id,
        &source,
        &claims.email,
        query,
        &outcome,
    )
    .await;

    Ok(Json(match outcome {
        Ok(t) => json!({
            "ok": true,
            "query": t.text,
            "fields": t.fields,
            "result": t.result,
            "hidden_count": t.hidden_count,
            "ms": t.ms as u64,
            "note": t.note,
        }),
        Err(e) if e.is_refusal() => json!({
            "ok": false,
            "query": query,
            "fields": [],
            "hidden_count": 0,
            "ms": 0,
            "refused": format!("{} {}", e.message, e.fix),
        }),
        Err(e) => json!({
            "ok": false,
            "query": query,
            "fields": [],
            "hidden_count": 0,
            "ms": 0,
            "error": step_json(&e),
        }),
    }))
}

// ── AWS: logs and databases ────────────────────────────────────────────────

async fn external_id(state: &AppState, team_id: &str) -> Result<String, (StatusCode, Json<Value>)> {
    super::log_analyzer::team_external_id(state, team_id)
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong on our side. Try again.",
            )
        })
}

/// The team's existing AWS connections (made for log alerts), which a logs
/// source can reuse without new setup.
async fn existing_connections(state: &AppState, team_id: &str) -> Vec<Value> {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.aws_insights_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S("AWS_CONN#".to_string()),
        )
        .send()
        .await;
    let Ok(out) = out else { return Vec::new() };
    out.items()
        .iter()
        .filter_map(|i| {
            let role_arn = i.get("role_arn")?.as_s().ok()?.clone();
            live_aws::check_role_arn(&role_arn).ok()?;
            let region = i
                .get("region")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "us-east-1".into());
            let groups: Vec<String> = i
                .get("log_groups")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default();
            Some(json!({
                "role_arn": role_arn,
                "account": role_arn.split(':').nth(4).unwrap_or(""),
                "region": region,
                "log_groups": groups,
            }))
        })
        .collect()
}

/// GET /api/data-sources/aws/setup — the script to paste into AWS CloudShell,
/// and any AWS connection the team already has.
pub async fn aws_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let ext = external_id(&state, &claims.team_id).await?;
    let our_account = state
        .sts
        .get_caller_identity()
        .send()
        .await
        .ok()
        .and_then(|o| o.account().map(str::to_string))
        .ok_or_else(|| internal("read our account id", "no account".into()))?;
    Ok(Json(json!({
        "script": live_aws::setup_script(&our_account, &ext),
        "existing": existing_connections(&state, &claims.team_id).await,
        "role_prefix": live_aws::ROLE_PREFIX,
    })))
}

/// The AWS target a request names: a pasted setup line, or an existing
/// connection's role. The private ID is always the team's own.
async fn target_from(
    state: &AppState,
    team_id: &str,
    body: &Value,
) -> Result<AwsTarget, (StatusCode, Json<Value>)> {
    let mut t = match text(body, "code") {
        "" => {
            let t = AwsTarget {
                role_arn: text(body, "role_arn").to_string(),
                region: match text(body, "region") {
                    "" => "us-east-1".to_string(),
                    r => r.to_string(),
                },
                ..Default::default()
            };
            live_aws::check_target(&t)
                .map_err(|e| fail(StatusCode::BAD_REQUEST, &e.to_string()))?;
            t
        }
        code => live_aws::parse_setup_code(code)
            .map_err(|e| fail(StatusCode::BAD_REQUEST, &e.to_string()))?,
    };
    t.external_id = external_id(state, team_id).await?;
    Ok(t)
}

/// Proves Coderhelm can use the role, and that nobody can without the private
/// ID. Returns the session and the checks to show.
async fn role_checks(
    state: &AppState,
    t: &AwsTarget,
    actor: &str,
) -> (Vec<Check>, Result<live_aws::Session, StepError>) {
    const LABEL: &str = "Use the role in your AWS account";
    if live_aws::opens_without_private_id(&state.sts, t).await {
        let e = StepError::new(
            "sign_in",
            "That role can be used without your private ID, so other Coderhelm accounts could reach it.",
            "Run the setup script again: it puts the private ID back on the role.",
        );
        return (
            vec![Check {
                id: "role",
                label: LABEL,
                ok: false,
                detail: e.message.clone(),
                fix: Some(e.fix.clone()),
            }],
            Err(e),
        );
    }
    match live_aws::assume(&state.sts, t, actor).await {
        Ok(s) => (
            vec![Check {
                id: "role",
                label: LABEL,
                ok: true,
                detail: "Coderhelm can use the role, and only with your private ID.".into(),
                fix: None,
            }],
            Ok(s),
        ),
        Err(e) => (
            vec![Check {
                id: "role",
                label: LABEL,
                ok: false,
                detail: e.message.clone(),
                fix: Some(e.fix.clone()),
            }],
            Err(e),
        ),
    }
}

/// POST /api/data-sources/aws/connect  { code } | { role_arn, region } —
/// checks the role and lists what it can reach. Saves nothing.
pub async fn aws_connect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let t = target_from(&state, &claims.team_id, &body).await?;
    let (mut checks, session) = role_checks(&state, &t, &claims.email).await;
    let Ok(session) = session else {
        return Ok(Json(json!({ "ok": false, "checks": checks })));
    };
    let groups = match logs::list_groups(&session).await {
        Ok(g) => {
            checks.push(Check {
                id: "logs",
                label: "See your log groups",
                ok: true,
                detail: format!("It can see {} log groups.", g.len()),
                fix: None,
            });
            g
        }
        Err(e) => {
            checks.push(Check {
                id: "logs",
                label: "See your log groups",
                ok: false,
                detail: e.message.clone(),
                fix: Some(e.fix.clone()),
            });
            Vec::new()
        }
    };
    let database = (!t.cluster_arn.is_empty()).then(|| {
        json!({ "cluster": t.cluster_arn.rsplit(':').next().unwrap_or(""), "database": t.database })
    });
    Ok(Json(json!({
        "ok": true,
        "checks": checks,
        "account": t.account(),
        "region": t.region,
        "role_arn": t.role_arn,
        "log_groups": groups,
        "database": database,
    })))
}

fn new_aws_source(kind: Kind, name: String, t: AwsTarget, email: &str) -> Source {
    let now = chrono::Utc::now().to_rfc3339();
    Source {
        id: ulid::Ulid::new().to_string(),
        kind,
        name,
        url: String::new(),
        auth: common::live::api::AuthStyle::XApiKey,
        key_sealed: String::new(),
        key_hint: String::new(),
        // Saved switched off: a person turns it on after trying it.
        enabled: false,
        setup_complete: false,
        status: "ready".to_string(),
        status_message: String::new(),
        has_mutations: false,
        kinds: None,
        schema_available: false,
        samples: Vec::new(),
        hidden_fields: store::default_hidden_fields(),
        repos: Vec::new(),
        last_checked_at: now.clone(),
        created_at: now,
        created_by: email.to_string(),
        aws: Some(t),
    }
}

async fn room_for_one_more(
    state: &AppState,
    team_id: &str,
) -> Result<Vec<Source>, (StatusCode, Json<Value>)> {
    let existing = store::list_sources(&state.dynamo, table(state), team_id)
        .await
        .map_err(|e| internal("list data sources", e))?;
    if existing.len() >= store::MAX_SOURCES {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You've reached the limit of connected sources. Remove one first.",
        ));
    }
    Ok(existing)
}

/// POST /api/data-sources/logs  { code | role_arn+region, log_groups, name? }
pub async fn create_logs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let mut t = target_from(&state, &claims.team_id, &body).await?;
    t.cluster_arn.clear();
    t.secret_arn.clear();
    t.database.clear();
    t.log_groups = string_list(&body["log_groups"], MAX_LOG_GROUPS).unwrap_or_default();
    if t.log_groups.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Choose at least one log group to share.",
        ));
    }
    room_for_one_more(&state, &claims.team_id).await?;
    let (checks, session) = role_checks(&state, &t, &claims.email).await;
    if session.is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The connection checks didn't pass.", "checks": checks })),
        ));
    }
    let name = match text(&body, "name") {
        "" => format!("Logs in AWS account {}", t.account()),
        n => common::truncate_str(n, 80).to_string(),
    };
    let groups = t.log_groups.len();
    let mut source = new_aws_source(Kind::Logs, name, t, &claims.email);
    source.kinds = Some(groups);
    source.schema_available = true;
    save(&state, &claims.team_id, &source).await?;
    info!(team = %claims.team_id, source = %source.id, "Logs source connected");
    Ok(Json(
        json!({ "source": source.public_json(), "checks": checks }),
    ))
}

async fn save_db_schema(state: &AppState, team_id: &str, id: &str, schema: &str) {
    let put = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(Source::schema_s3_key(team_id, id))
        .content_type("text/plain")
        .body(schema.as_bytes().to_vec().into())
        .send()
        .await;
    if let Err(e) = put {
        warn!(error = %e, "Could not store the database's description; lookups still work");
    }
}

fn db_samples(schema: &str) -> Vec<(String, String)> {
    schema
        .split("\n\n")
        .filter_map(|block| {
            block
                .lines()
                .next()?
                .strip_prefix("table ")
                .map(str::to_string)
        })
        .take(3)
        .flat_map(|t| {
            [
                (
                    format!("Count rows in {t}"),
                    format!("SELECT count(*) FROM {t}"),
                ),
                (
                    format!("Look at a few rows of {t}"),
                    format!("SELECT * FROM {t} LIMIT 5"),
                ),
            ]
        })
        .collect()
}

/// POST /api/data-sources/database  { code, name? } — checks the database
/// user can only read, then saves the source, switched off.
pub async fn create_database(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let mut t = target_from(&state, &claims.team_id, &body).await?;
    if t.cluster_arn.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That line doesn't include a database. Run the setup script again and choose one.",
        ));
    }
    t.log_groups.clear();
    let existing = room_for_one_more(&state, &claims.team_id).await?;
    if existing.iter().any(|s| {
        s.aws
            .as_ref()
            .is_some_and(|a| a.cluster_arn == t.cluster_arn && a.database == t.database)
    }) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That database is already connected.",
        ));
    }
    let (mut checks, session) = role_checks(&state, &t, &claims.email).await;
    let report = db::connect_checks(session, &t).await;
    // connect_checks reports the role step itself; keep ours, which also
    // covers the private ID.
    checks.extend(report.checks.into_iter().filter(|c| c.id != "role"));
    if !report.ok {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The connection checks didn't pass.", "checks": checks })),
        ));
    }
    let name = match text(&body, "name") {
        "" => format!(
            "{} database",
            t.cluster_arn.rsplit(':').next().unwrap_or("AWS")
        ),
        n => common::truncate_str(n, 80).to_string(),
    };
    let mut source = new_aws_source(Kind::Database, name, t, &claims.email);
    source.kinds = report.tables;
    if let Some(schema) = &report.schema {
        source.schema_available = true;
        source.samples = db_samples(schema);
        save_db_schema(&state, &claims.team_id, &source.id, schema).await;
    }
    save(&state, &claims.team_id, &source).await?;
    info!(team = %claims.team_id, source = %source.id, "Database source connected");
    Ok(Json(
        json!({ "source": source.public_json(), "checks": checks }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_samples_come_from_the_first_tables() {
        let schema = "table orders\n  id: integer\n\ntable billing.invoices\n  id: uuid\n";
        let s = db_samples(schema);
        assert_eq!(s.len(), 4);
        assert_eq!(s[0].1, "SELECT count(*) FROM orders");
        assert_eq!(s[3].1, "SELECT * FROM billing.invoices LIMIT 5");
        for (_, q) in &s {
            assert!(
                common::live::sql::check_read_only_sql(q, &store::default_hidden_fields()).is_ok(),
                "{q}"
            );
        }
    }
}
