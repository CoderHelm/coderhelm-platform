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
use common::live::api::{self, ApiSource, ConnectReport};
use common::live::net::parse_endpoint;
use common::live::store::{self, Activity, Source};
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

/// GET /api/data-sources/activity?limit=50
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Reply {
    admin(&claims)?;
    let limit = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
    let events = store::list_activity(&state.dynamo, table(&state), &claims.team_id, limit)
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

/// POST /api/data-sources/:id/test — reruns the checks with the saved key.
pub async fn test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let mut source = load(&state, &claims.team_id, &id).await?;
    let key = open_key(&state, &claims.team_id, &source).await?;
    let report = api::connect_checks(&source.url, &key).await;
    if report.ok {
        apply_report(&mut source, &report);
        save_schema(&state, &claims.team_id, &id, &report).await;
    } else {
        source.status = "error".to_string();
        source.status_message = report
            .checks
            .iter()
            .find(|c| !c.ok)
            .map(|c| format!("{} {}", c.detail, c.fix.clone().unwrap_or_default()))
            .unwrap_or_default();
        source.last_checked_at = chrono::Utc::now().to_rfc3339();
    }
    save(&state, &claims.team_id, &source).await?;
    let mut out = report_json(&report);
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
    let mut samples: Vec<Value> = source
        .samples
        .iter()
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

/// POST /api/data-sources/:id/try  { query } — runs one lookup the way the
/// reviewer would, and records it. Works while the source is switched off, so
/// a person can see what it does before turning it on.
pub async fn try_lookup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let source = load(&state, &claims.team_id, &id).await?;
    let query = body["query"].as_str().unwrap_or("");
    let key = open_key(&state, &claims.team_id, &source).await?;
    let src = ApiSource {
        url: source.url.clone(),
        auth: source.auth,
        key,
    };

    let outcome = api::run_lookup(&src, query, None, &source.hidden_fields, TRY_RESULT_CHARS).await;
    let (label, detail, ms) = match &outcome {
        Ok(l) => ("ok", String::new(), l.ms),
        Err(e) if e.is_refusal() => ("refused", e.message.clone(), 0),
        Err(e) => ("failed", e.message.clone(), 0),
    };
    let logged = store::log_activity(
        &state.dynamo,
        table(&state),
        &claims.team_id,
        &Activity {
            source_id: source.id.clone(),
            source_name: source.name.clone(),
            actor: claims.email.clone(),
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

    Ok(Json(match outcome {
        Ok(l) => json!({
            "ok": true,
            "query": l.query,
            "fields": l.fields,
            "result": l.result,
            "hidden_count": l.hidden_count,
            "ms": l.ms as u64,
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
