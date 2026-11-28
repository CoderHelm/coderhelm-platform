//! The streaming function's health probe: what Chat needs of AWS, asked with
//! Chat's own role.
//!
//! After an update the updater watches the new release (lambda/updater,
//! `probe_chat`). The worker answers `{"health": {}}`; Chat runs in its own
//! Lambda with its own role, so a grant missing there (a member check added
//! to every stream, with no read of the users table granted) breaks Chat
//! while the worker and the gateway stay healthy. This probe makes Chat's own
//! calls:
//!
//! - the member check every stream runs first (`require_sso::check_scoped`),
//!   for a member no team has: a clean refusal means the check could run, a
//!   server error means it couldn't (the 503 Chat answers then);
//! - one call per DynamoDB action `infra/streaming-dynamodb.json` lists for
//!   the stream routes (the list `stream_access` holds the code to), each on
//!   a key no record has. A write carries a condition that can never hold,
//!   so nothing is written: DynamoDB checks the grant before the condition.
//!
//! A grant the role lacks answers AccessDenied here exactly as it would for
//! Chat, and the updater rolls the release back as for a failed worker probe.
//!
//! Only a direct invoke of the function reaches it: the Lambda Web Adapter
//! turns the invoke's request context into the `x-amzn-request-context`
//! header, and the probe answers only when that names `PROBE_HOST`, which no
//! request through the function URL or CloudFront can carry (AWS writes
//! those contexts). Anything else gets a 404.

use std::sync::Arc;

use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, Select};
use axum::{extract::State, http::HeaderMap, http::StatusCode, Json};
use serde_json::{json, Value};
use tracing::{error, info};

use crate::auth::jwt::{self, SignInProof};
use crate::AppState;

/// The route the updater's probe invokes.
pub const PROBE_PATH: &str = "/api/stream/probe";
/// The request context's domain of the updater's invoke (`.invalid` never resolves).
pub const PROBE_HOST: &str = "coderhelm-probe.invalid";
/// The header the Lambda Web Adapter puts the invoke's request context in.
const CONTEXT_HEADER: &str = "x-amzn-request-context";
/// The member and team the probe asks for: no record has them.
pub const PROBE_TEAM: &str = "TEAM#coderhelm-probe";
const PROBE_MEMBER: &str = "USER#coderhelm-probe";
const PROBE_SK: &str = "PROBE#coderhelm";

/// What the stream routes call, by table (infra/streaming-dynamodb.json).
const GRANTED: &str = include_str!("../../../../infra/streaming-dynamodb.json");

/// The actions the probe can make without changing anything.
pub const PROBED_ACTIONS: &[&str] = &[
    "dynamodb:GetItem",
    "dynamodb:BatchGetItem",
    "dynamodb:Query",
    "dynamodb:Scan",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
];

/// `[(resource, action)]` from the file: `users`, `users/index/gsi2`.
pub fn granted() -> Vec<(String, String)> {
    let v: Value = serde_json::from_str(GRANTED).unwrap_or(Value::Null);
    let mut out = Vec::new();
    for (resource, actions) in v["dynamodb"].as_object().into_iter().flatten() {
        for a in actions.as_array().into_iter().flatten() {
            if let Some(a) = a.as_str() {
                out.push((resource.clone(), a.to_string()));
            }
        }
    }
    out
}

/// The deployed table a resource of the file names ("main" is the main table).
fn table_of(state: &AppState, resource: &str) -> Option<String> {
    let c = &state.config;
    let name = resource.split('/').next().unwrap_or("");
    let t = match name {
        "main" => &c.table_name,
        "teams" => &c.teams_table_name,
        "users" => &c.users_table_name,
        "settings" => &c.settings_table_name,
        "repos" => &c.repos_table_name,
        "runs" => &c.runs_table_name,
        "analytics" => &c.analytics_table_name,
        "events" => &c.events_table_name,
        "jira-tokens" => &c.jira_tokens_table_name,
        "jira-events" => &c.jira_events_table_name,
        "plans" => &c.plans_table_name,
        "jira-config" => &c.jira_config_table_name,
        "aws-insights" => &c.aws_insights_table_name,
        "infra" => &c.infra_table_name,
        "banners" => &c.banners_table_name,
        "mcp-configs" => &c.mcp_configs_table_name,
        "waitlist" => &c.waitlist_table_name,
        "traces" => &c.traces_table_name,
        "tests" => &c.tests_table_name,
        _ => return None,
    };
    Some(t.clone())
}

/// Whether the request is the updater's direct invoke (see the module docs).
pub fn is_direct_invoke(headers: &HeaderMap) -> bool {
    headers
        .get(CONTEXT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| serde_json::from_str::<Value>(v).ok())
        .is_some_and(|c| c["domainName"].as_str() == Some(PROBE_HOST))
}

/// How one call came back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Allowed (answered, or refused only by its condition or its key).
    Allowed,
    /// The role may not (or AWS refused it for a reason Chat would hit too).
    Refused,
    /// AWS was busy or didn't answer: says nothing about the release.
    Unknown,
}

/// What an AWS answer means for the grant.
pub fn outcome_of(code: Option<&str>) -> Outcome {
    match code {
        None => Outcome::Unknown,
        Some("ConditionalCheckFailedException" | "ValidationException") => Outcome::Allowed,
        Some(
            "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "RequestLimitExceeded"
            | "InternalServerError"
            | "ServiceUnavailable",
        ) => Outcome::Unknown,
        Some(_) => Outcome::Refused,
    }
}

fn result<T, E: ProvideErrorMetadata + std::fmt::Display>(r: Result<T, E>) -> (Outcome, String) {
    match r {
        Ok(_) => (Outcome::Allowed, String::new()),
        Err(e) => {
            let code = e.code().map(str::to_string);
            let detail = match &code {
                Some(c) => format!("{c}: {}", e.message().unwrap_or("")),
                None => e.to_string(),
            };
            (outcome_of(code.as_deref()), detail)
        }
    }
}

fn key() -> std::collections::HashMap<String, AttributeValue> {
    [
        ("pk".to_string(), AttributeValue::S(PROBE_TEAM.into())),
        ("sk".to_string(), AttributeValue::S(PROBE_SK.into())),
    ]
    .into_iter()
    .collect()
}

/// One call of `action` on the table, changing nothing.
async fn call(
    state: &AppState,
    table: &str,
    index: Option<&str>,
    action: &str,
) -> (Outcome, String) {
    let d = &state.dynamo;
    // Never holds: the item must exist and not exist.
    let never = "attribute_exists(#p) AND attribute_not_exists(#p)";
    match action {
        "dynamodb:GetItem" => result(
            d.get_item()
                .table_name(table)
                .set_key(Some(key()))
                .send()
                .await,
        ),
        "dynamodb:BatchGetItem" => {
            let keys = KeysAndAttributes::builder().keys(key()).build();
            match keys {
                Ok(k) => result(d.batch_get_item().request_items(table, k).send().await),
                Err(e) => (Outcome::Unknown, e.to_string()),
            }
        }
        "dynamodb:Query" => {
            let pk = index
                .map(|i| format!("{i}pk"))
                .unwrap_or_else(|| "pk".into());
            let mut q = d
                .query()
                .table_name(table)
                .key_condition_expression("#p = :p")
                .expression_attribute_names("#p", pk)
                .expression_attribute_values(":p", AttributeValue::S(PROBE_TEAM.into()))
                .limit(1);
            if let Some(i) = index {
                q = q.index_name(i);
            }
            result(q.send().await)
        }
        "dynamodb:Scan" => {
            let mut q = d.scan().table_name(table).limit(1).select(Select::Count);
            if let Some(i) = index {
                q = q.index_name(i);
            }
            result(q.send().await)
        }
        "dynamodb:PutItem" => result(
            d.put_item()
                .table_name(table)
                .set_item(Some(key()))
                .condition_expression(never)
                .expression_attribute_names("#p", "pk")
                .send()
                .await,
        ),
        "dynamodb:UpdateItem" => result(
            d.update_item()
                .table_name(table)
                .set_key(Some(key()))
                .update_expression("SET #x = :x")
                .condition_expression(never)
                .expression_attribute_names("#p", "pk")
                .expression_attribute_names("#x", "probe")
                .expression_attribute_values(":x", AttributeValue::Bool(true))
                .send()
                .await,
        ),
        "dynamodb:DeleteItem" => result(
            d.delete_item()
                .table_name(table)
                .set_key(Some(key()))
                .condition_expression(never)
                .expression_attribute_names("#p", "pk")
                .send()
                .await,
        ),
        other => (
            Outcome::Unknown,
            format!("{other} isn't one the probe makes"),
        ),
    }
}

/// The member check every stream runs, for a member no team has.
async fn member_check(state: &AppState) -> (Outcome, String) {
    let proof = SignInProof::new(true, "google");
    let claims = jwt::create_session_token(
        PROBE_MEMBER,
        PROBE_TEAM,
        "probe@coderhelm-probe.invalid",
        "member",
        None,
        &proof,
        &state.secrets.jwt_secret,
        60,
    )
    .ok()
    .and_then(|t| jwt::validate_token(&t, &state.secrets.jwt_secret).ok());
    let Some(claims) = claims else {
        return (Outcome::Refused, "a stream token couldn't be made".into());
    };
    match super::require_sso::check_scoped(state, &claims, &proof).await {
        Ok(()) => (Outcome::Allowed, String::new()),
        Err(r) if r.status().is_server_error() => (
            Outcome::Refused,
            format!(
                "the member check failed ({}), so Chat answers 503",
                r.status()
            ),
        ),
        // The probe's member doesn't exist: refused cleanly, as it should be.
        Err(_) => (Outcome::Allowed, String::new()),
    }
}

/// Every check, as `(what, outcome, detail)`.
pub async fn run(state: &AppState) -> Vec<(String, Outcome, String)> {
    let mut out = Vec::new();
    let (o, d) = member_check(state).await;
    out.push(("member check".to_string(), o, d));
    for (resource, action) in granted() {
        let what = format!("{action} on {resource}");
        let Some(table) = table_of(state, &resource) else {
            out.push((
                what,
                Outcome::Refused,
                "a table this install doesn't name".into(),
            ));
            continue;
        };
        if !PROBED_ACTIONS.contains(&action.as_str()) {
            out.push((what, Outcome::Unknown, "not one the probe makes".into()));
            continue;
        }
        let index = resource.split("/index/").nth(1);
        let (o, d) = call(state, &table, index, &action).await;
        out.push((what, o, d));
    }
    out
}

/// POST /api/stream/probe (the updater's direct invoke only).
pub async fn probe(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if !is_direct_invoke(&headers) {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "Not found" })));
    }
    let checks = run(&state).await;
    let refused: Vec<String> = checks
        .iter()
        .filter(|(_, o, _)| *o == Outcome::Refused)
        .map(|(w, _, d)| format!("{w}: {d}"))
        .collect();
    let unknown = checks
        .iter()
        .filter(|(_, o, _)| *o == Outcome::Unknown)
        .count();
    if refused.is_empty() {
        info!(checks = checks.len(), unknown, "Chat probe passed");
    } else {
        error!(refused = ?refused, "Chat probe failed: the streaming role is refused what Chat calls");
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": refused.is_empty() && unknown == 0,
            "refused": refused,
            "unknown": unknown,
            "checks": checks.len(),
            "version": std::env::var("CODERHELM_VERSION").unwrap_or_default(),
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_aws;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn invoke(host: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(PROBE_PATH)
            .header(CONTEXT_HEADER, json!({ "domainName": host }).to_string())
            .body(Body::empty())
            .unwrap()
    }

    async fn answer(app: axum::Router, req: Request<Body>) -> (StatusCode, Value) {
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let b = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn the_probe_makes_every_call_the_stream_routes_are_granted() {
        let f = fake_aws::start().await;
        let state = fake_aws::state_all_tables(&f.url);
        let app = crate::build_app(state.clone());
        let (status, body) = answer(app, invoke(PROBE_HOST)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["ok"], true, "{body}");
        // One real call per listed grant: the recording has each.
        let made = f.store.dynamo_calls();
        for (resource, action) in granted() {
            assert!(
                made.contains(&(action.clone(), resource.clone())),
                "the probe never made {action} on {resource}: {made:?}"
            );
        }
        // And it changed nothing.
        assert!(f.store.items("settings", PROBE_TEAM).is_empty());
    }

    #[tokio::test]
    async fn a_member_record_that_cant_be_read_fails_the_probe() {
        let f = fake_aws::start().await;
        let state = fake_aws::state_all_tables(&f.url);
        f.store.fail_reads("users", PROBE_TEAM);
        let (_, body) = answer(crate::build_app(state), invoke(PROBE_HOST)).await;
        assert_eq!(body["ok"], false, "{body}");
        let refused = body["refused"].to_string();
        assert!(refused.contains("member check"), "{refused}");
    }

    #[tokio::test]
    async fn only_the_updaters_invoke_reaches_it() {
        let f = fake_aws::start().await;
        let state = fake_aws::state_all_tables(&f.url);
        let app = crate::build_app(state);
        let (status, _) = answer(app.clone(), invoke("abc.lambda-url.us-east-1.on.aws")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let plain = Request::builder()
            .method("POST")
            .uri(PROBE_PATH)
            .body(Body::empty())
            .unwrap();
        assert_eq!(answer(app, plain).await.0, StatusCode::NOT_FOUND);
        // None of the probe's calls ran (the router's own reads may).
        let made = f.store.dynamo_calls();
        for call in [
            ("dynamodb:GetItem", "users"),
            ("dynamodb:PutItem", "settings"),
        ] {
            assert!(
                !made.contains(&(call.0.to_string(), call.1.to_string())),
                "{made:?}"
            );
        }
    }

    #[test]
    fn every_listed_action_is_one_the_probe_makes() {
        for (resource, action) in granted() {
            assert!(
                PROBED_ACTIONS.contains(&action.as_str()),
                "{action} on {resource}: add it to the probe (it must change nothing)"
            );
        }
        assert!(!granted().is_empty());
    }

    #[test]
    fn what_an_answer_means() {
        assert_eq!(outcome_of(Some("AccessDeniedException")), Outcome::Refused);
        assert_eq!(
            outcome_of(Some("ResourceNotFoundException")),
            Outcome::Refused
        );
        assert_eq!(
            outcome_of(Some("ConditionalCheckFailedException")),
            Outcome::Allowed
        );
        assert_eq!(outcome_of(Some("ThrottlingException")), Outcome::Unknown);
        assert_eq!(outcome_of(None), Outcome::Unknown);
    }
}
