//! Security fixes from the dashboard and from GitHub's webhooks
//! (`common::security_fixes` has the rules, `worker::passes::security_fixes`
//! the work).
//!
//! - `GET /api/security/overview` — the counts, the sources, the settings'
//!   switches. Any team member.
//! - `GET /api/security/findings?status=&tier=&repo=&cursor=` — the findings.
//! - `GET /api/security/findings/{id}` — one finding and its timeline.
//! - `POST /api/security/findings/{id}/dismiss` — with a reason and an
//!   optional end date; synced to the Dependabot alert when the app may.
//!   Admin.
//! - `POST /api/security/findings/{id}/reopen` — Admin.
//! - `POST /api/security/scan` — check one repository (or all) now. Admin.
//! - `GET /api/security/config`, `PUT /api/security/config` — the settings
//!   and the defaults; saving is Admin.
//! - `GET /api/security/preview` — what a dry run would open and post now.
//!   Admin.
//! - The `dependabot_alert` and `security_advisory` webhooks, and a push to
//!   a default branch that touches a dependency file, queue a
//!   `security_event` for the worker when the team turned this on.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Path, Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::security_fixes::{
    self as sec, Dismissed, Finding, FindingState, SecurityConfig, Status, Tier,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, SecurityEventMessage, WorkerMessage};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Findings shown per page.
const PAGE: usize = 50;
const MAX_FINDINGS: usize = 5_000;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn bad(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

fn code(c: StatusCode) -> (StatusCode, Json<Value>) {
    (
        c,
        Json(json!({ "error": c.canonical_reason().unwrap_or("error") })),
    )
}

async fn get_item(state: &AppState, pk: &str, sk: &str) -> Result<Option<Item>, StatusCode> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map(|o| o.item().cloned())
        .map_err(|e| {
            error!(sk, error = %e, "security: read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// The team's settings: saved, or the defaults.
pub(crate) async fn load_config(
    state: &AppState,
    team_id: &str,
) -> Result<Option<SecurityConfig>, StatusCode> {
    Ok(get_item(state, team_id, sec::CONFIG_SK)
        .await?
        .and_then(|i| SecurityConfig::from_json(&s(&i, "config"))))
}

async fn load_findings(
    state: &AppState,
    team_id: &str,
) -> Result<Vec<(Finding, FindingState)>, StatusCode> {
    let mut out = vec![];
    let mut start: Option<Item> = None;
    loop {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(sec::FINDING_PREFIX))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "security: could not read the findings");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for i in resp.items() {
            if let Ok(f) = serde_json::from_str::<Finding>(&s(i, "finding")) {
                let st = serde_json::from_str(&s(i, "state")).unwrap_or_default();
                out.push((f, st));
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() && out.len() < MAX_FINDINGS => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

async fn load_finding(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<(Finding, FindingState), StatusCode> {
    if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(StatusCode::NOT_FOUND);
    }
    let item = get_item(state, team_id, &sec::finding_sk(id))
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
    let f =
        serde_json::from_str::<Finding>(&s(&item, "finding")).map_err(|_| StatusCode::NOT_FOUND)?;
    let st = serde_json::from_str(&s(&item, "state")).unwrap_or_default();
    Ok((f, st))
}

async fn save_finding(
    state: &AppState,
    team_id: &str,
    f: &Finding,
    st: &FindingState,
) -> Result<(), StatusCode> {
    let fj = serde_json::to_string(f).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let sj = serde_json::to_string(st).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::finding_sk(&f.id)))
        .item("finding", attr_s(&fj))
        .item("state", attr_s(&sj))
        .item("status", attr_s(f.status.as_str()))
        .item("tier", attr_s(f.tier.map(|t| t.as_str()).unwrap_or("")))
        .item("repo", attr_s(&f.repo))
        .item("updated_at", attr_s(&f.updated_at))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| {
            error!(error = %e, "security: could not save a finding");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn add_event(state: &AppState, team_id: &str, id: &str, text: &str, link: Option<&str>) {
    let now = chrono::Utc::now();
    let n = now.timestamp_subsec_micros() % 1000;
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::event_sk(id, &now, n)))
        .item("at", attr_s(&now.to_rfc3339()))
        .item("text", attr_s(common::truncate_str(text, 500)))
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + sec::EVENT_TTL_DAYS * 86_400).to_string()),
        );
    if let Some(l) = link {
        put = put.item("link", attr_s(l));
    }
    if let Err(e) = put.send().await {
        warn!(error = %e, "security: could not record a timeline line");
    }
}

// ── Overview ────────────────────────────────────────────────────────────

/// GET /api/security/overview
pub async fn overview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await?;
    let now = chrono::Utc::now();
    let mut out = sec::overview_counts(&findings, now);
    let team_state = get_item(&state, &claims.team_id, sec::STATE_SK)
        .await?
        .unwrap_or_default();
    let kev = get_item(&state, sec::GLOBAL_PK, sec::KEV_SK)
        .await?
        .unwrap_or_default();
    let opt = |i: &Item, k: &str| Some(s(i, k)).filter(|v| !v.is_empty());
    let dependabot = match s(&team_state, "dependabot").as_str() {
        "off" => "off",
        "no_permission" => "no_permission",
        "on" => "on",
        // Not read yet: "on" until a read says otherwise.
        _ => "on",
    };
    let permission_missing = team_state
        .get("permission_missing")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    out["enabled"] = json!(cfg.enabled);
    out["dry_run"] = json!(cfg.dry_run);
    out["sources"] = json!({
        "dependabot": dependabot,
        "osv": { "synced_at": opt(&team_state, "osv_synced_at"), "mode": "api" },
        "kev_synced_at": opt(&kev, "synced_at"),
        "epss_synced_at": opt(&team_state, "epss_synced_at"),
    });
    out["permission_missing"] = json!(permission_missing);
    Ok(Json(out))
}

// ── Findings ────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct ListQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Pure: the findings a list query shows, in order (tier, then due date),
/// and the cursor of the next page.
pub fn list_page(
    findings: Vec<Finding>,
    q: &ListQuery,
) -> Result<(Vec<Finding>, Option<String>), StatusCode> {
    let status = q.status.as_deref().unwrap_or("open");
    if !matches!(status, "open" | "fixed" | "dismissed" | "all") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tier = match q.tier.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Some(Tier::parse(t).ok_or(StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let repo = q.repo.as_deref().filter(|r| !r.is_empty());
    if repo.is_some_and(|r| !sec::valid_repo(r)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let offset: usize = match q.cursor.as_deref().filter(|c| !c.is_empty()) {
        Some(c) => c.parse().map_err(|_| StatusCode::BAD_REQUEST)?,
        None => 0,
    };
    let mut list: Vec<Finding> = findings
        .into_iter()
        .filter(|f| match status {
            "open" => f.status.is_open(),
            "fixed" => f.status == Status::Fixed,
            "dismissed" => matches!(f.status, Status::Dismissed | Status::Withdrawn),
            _ => true,
        })
        .filter(|f| tier.is_none_or(|t| f.tier == Some(t)))
        .filter(|f| repo.is_none_or(|r| f.repo.eq_ignore_ascii_case(r)))
        .collect();
    list.sort_by(|a, b| {
        a.tier
            .unwrap_or(Tier::P3)
            .cmp(&b.tier.unwrap_or(Tier::P3))
            .then_with(|| a.due_at.cmp(&b.due_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    let total = list.len();
    let page: Vec<Finding> = list.into_iter().skip(offset).take(PAGE).collect();
    let next = (offset + PAGE < total).then(|| (offset + PAGE).to_string());
    Ok((page, next))
}

/// GET /api/security/findings
pub async fn list_findings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, StatusCode> {
    let findings = load_findings(&state, &claims.team_id).await?;
    let (page, next) = list_page(findings.into_iter().map(|(f, _)| f).collect(), &q)?;
    Ok(Json(json!({ "findings": page, "next_cursor": next })))
}

/// GET /api/security/findings/{id}
pub async fn get_finding(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let (f, _) = load_finding(&state, &claims.team_id, &id).await?;
    let mut timeline = vec![];
    let mut start: Option<Item> = None;
    for _ in 0..10 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(&claims.team_id))
            .expression_attribute_values(":pfx", attr_s(&sec::event_prefix(&f.id)))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "security: could not read the timeline");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for i in resp.items() {
            let link = s(i, "link");
            timeline.push(json!({
                "at": s(i, "at"),
                "text": s(i, "text"),
                "link": if link.is_empty() { Value::Null } else { json!(link) },
            }));
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(Json(json!({ "finding": f, "timeline": timeline })))
}

#[derive(Deserialize)]
pub struct DismissBody {
    pub reason: String,
    #[serde(default)]
    pub until: Option<String>,
}

/// Pure: a dismissal's reason and end date, checked.
pub fn check_dismiss(
    b: &DismissBody,
    today: chrono::NaiveDate,
) -> Result<(String, Option<String>), &'static str> {
    let reason = b.reason.trim();
    if reason.is_empty() {
        return Err("a reason is required");
    }
    if reason.chars().count() > 500 {
        return Err("the reason is at most 500 characters");
    }
    let until = match b.until.as_deref().map(str::trim).filter(|u| !u.is_empty()) {
        None => None,
        Some(u) => {
            let d = chrono::NaiveDate::parse_from_str(u.get(..10).unwrap_or(u), "%Y-%m-%d")
                .map_err(|_| "until is a date (YYYY-MM-DD)")?;
            if d < today {
                return Err("until is in the past");
            }
            Some(d.format("%Y-%m-%d").to_string())
        }
    };
    Ok((reason.to_string(), until))
}

/// GitHub's reason word for a dismissal, from the verdict.
pub fn dismissed_reason(f: &Finding) -> &'static str {
    match f.verdict {
        sec::Verdict::NotReachable | sec::Verdict::NotLoaded => "not_used",
        _ => "tolerable_risk",
    }
}

/// Changes the finding's Dependabot alert; whether GitHub took it. A refusal
/// (no write permission) leaves it recorded in CoderHelm only.
async fn sync_alert(state: &AppState, team_id: &str, f: &Finding, body: &Value) -> bool {
    let Some(alert) = &f.dependabot_alert else {
        return false;
    };
    let Ok(iid) = super::api::get_team_installation_id(state, team_id).await else {
        return false;
    };
    let Ok(token) = crate::auth::github_app::get_installation_token(state, iid).await else {
        return false;
    };
    let url = format!(
        "https://api.github.com/repos/{}/dependabot/alerts/{}",
        f.repo, alert.number
    );
    match state
        .http
        .patch(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => true,
        Ok(r) => {
            info!(repo = %f.repo, alert = alert.number, status = r.status().as_u16(), "security: the Dependabot alert wasn't changed (recorded in CoderHelm only)");
            false
        }
        Err(e) => {
            warn!(repo = %f.repo, error = %e, "security: could not reach GitHub for the Dependabot alert");
            false
        }
    }
}

/// POST /api/security/findings/{id}/dismiss
pub async fn dismiss(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<DismissBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(code)?;
    let (reason, until) = check_dismiss(&body, chrono::Utc::now().date_naive()).map_err(bad)?;
    let (mut f, st) = load_finding(&state, &claims.team_id, &id)
        .await
        .map_err(code)?;
    let now = chrono::Utc::now().to_rfc3339();
    f.status = Status::Dismissed;
    f.dismissed = Some(Dismissed {
        by: claims.email.clone(),
        reason: reason.clone(),
        until: until.clone(),
        at: now.clone(),
    });
    f.updated_at = now;
    save_finding(&state, &claims.team_id, &f, &st)
        .await
        .map_err(code)?;
    let synced = sync_alert(
        &state,
        &claims.team_id,
        &f,
        &json!({
            "state": "dismissed",
            "dismissed_reason": dismissed_reason(&f),
            "dismissed_comment": common::truncate_str(&format!("Dismissed in CoderHelm by {}: {reason}", claims.email), 280),
        }),
    )
    .await;
    add_event(
        &state,
        &claims.team_id,
        &f.id,
        &format!(
            "Dismissed by {}: {reason}{}{}",
            claims.email,
            until
                .as_deref()
                .map(|u| format!(" (until {u})"))
                .unwrap_or_default(),
            if synced {
                ". The Dependabot alert was dismissed too"
            } else {
                ""
            }
        ),
        None,
    )
    .await;
    info!(team = %claims.team_id, finding = %f.id, by = %claims.email, synced, "Security finding dismissed");
    Ok(Json(json!({ "finding": f, "synced": synced })))
}

/// POST /api/security/findings/{id}/reopen
pub async fn reopen(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let (mut f, st) = load_finding(&state, &claims.team_id, &id).await?;
    let was_dismissed = f.status == Status::Dismissed;
    f.status = match &f.pr {
        Some(p) if p.state == "open" => Status::PrOpen,
        _ => Status::Open,
    };
    f.dismissed = None;
    f.updated_at = chrono::Utc::now().to_rfc3339();
    save_finding(&state, &claims.team_id, &f, &st).await?;
    let synced =
        was_dismissed && sync_alert(&state, &claims.team_id, &f, &json!({ "state": "open" })).await;
    add_event(
        &state,
        &claims.team_id,
        &f.id,
        &format!(
            "Reopened by {}{}",
            claims.email,
            if synced {
                ". The Dependabot alert was reopened too"
            } else {
                ""
            }
        ),
        None,
    )
    .await;
    Ok(Json(json!({ "finding": f, "synced": synced })))
}

#[derive(Deserialize, Default)]
pub struct ScanBody {
    #[serde(default)]
    pub repo: Option<String>,
}

/// POST /api/security/scan
pub async fn scan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    body: Option<Json<ScanBody>>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(3)?;
    let repo = body
        .and_then(|Json(b)| b.repo)
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty());
    if repo.as_deref().is_some_and(|r| !sec::valid_repo(r)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let msg = WorkerMessage::SecurityEvent(SecurityEventMessage {
        team_id: claims.team_id.clone(),
        installation_id: 0,
        kind: "scan".into(),
        repo: repo.clone().unwrap_or_default(),
        ..Default::default()
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &msg).await?;
    info!(team = %claims.team_id, repo = ?repo, by = %claims.email, "Security check queued");
    Ok((StatusCode::ACCEPTED, Json(json!({ "status": "queued" }))))
}

// ── Settings ────────────────────────────────────────────────────────────

/// GET /api/security/config
pub async fn get_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    Ok(Json(
        json!({ "config": cfg, "defaults": SecurityConfig::default() }),
    ))
}

/// PUT /api/security/config
pub async fn put_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(cfg): Json<SecurityConfig>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(code)?;
    cfg.validate().map_err(|e| bad(&e))?;
    let cfg = cfg.cleaned();
    let text = serde_json::to_string(&cfg).map_err(|_| code(StatusCode::INTERNAL_SERVER_ERROR))?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(sec::CONFIG_SK))
        .item("config", attr_s(&text))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .item("updated_by", attr_s(&claims.email))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "security: could not save the settings");
            code(StatusCode::INTERNAL_SERVER_ERROR)
        })?;
    if let Err(e) = common::settings_index::add(
        &state.dynamo,
        &state.config.settings_table_name,
        sec::INDEX_NAME,
        &claims.team_id,
    )
    .await
    {
        // The daily rebuild finds it.
        warn!(error = %e, "security: could not index the team");
    }
    info!(team = %claims.team_id, enabled = cfg.enabled, dry_run = cfg.dry_run, by = %claims.email, "Security settings saved");
    Ok(Json(json!({ "config": cfg })))
}

/// GET /api/security/preview
pub async fn preview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let cfg = load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    let findings = load_findings(&state, &claims.team_id).await?;
    Ok(Json(sec::preview(&findings, &cfg, chrono::Utc::now())))
}

// ── Webhooks ────────────────────────────────────────────────────────────

/// Whether the team turned security fixes on.
async fn enabled(state: &AppState, team_id: &str) -> bool {
    matches!(load_config(state, team_id).await, Ok(Some(c)) if c.enabled)
}

async fn queue(state: &AppState, msg: SecurityEventMessage) -> Result<StatusCode, StatusCode> {
    super::github_webhook::send_to_queue(
        state,
        &state.config.ticket_queue_url,
        &WorkerMessage::SecurityEvent(msg),
    )
    .await
}

/// Pure: the event a `dependabot_alert` webhook queues, if any.
pub fn dependabot_event(
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Option<SecurityEventMessage> {
    let action = payload["action"].as_str().unwrap_or("");
    if !matches!(
        action,
        "created"
            | "reopened"
            | "auto_reopened"
            | "reintroduced"
            | "fixed"
            | "dismissed"
            | "auto_dismissed"
            | "withdrawn"
    ) {
        return None;
    }
    let repo = payload["repository"]["full_name"].as_str()?;
    let number = payload["alert"]["number"].as_u64()?;
    sec::valid_repo(repo).then(|| SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "dependabot_alert".into(),
        repo: repo.to_string(),
        action: action.to_string(),
        alert_number: number,
        ghsa_id: payload["alert"]["security_advisory"]["ghsa_id"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    })
}

/// Pure: the event a `security_advisory` webhook queues. `team_id` empty:
/// every team with security fixes on.
pub fn advisory_event(
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Option<SecurityEventMessage> {
    let action = payload["action"].as_str().unwrap_or("");
    if !matches!(action, "published" | "updated" | "withdrawn") {
        return None;
    }
    let ghsa = payload["security_advisory"]["ghsa_id"].as_str()?;
    (ghsa.starts_with("GHSA-") && ghsa.len() < 40).then(|| SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "security_advisory".into(),
        action: action.to_string(),
        ghsa_id: ghsa.to_string(),
        ..Default::default()
    })
}

/// Pure: whether a push changed a dependency file.
pub fn push_touches_dependencies(payload: &Value) -> bool {
    payload["commits"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|c| {
            ["added", "modified", "removed"].iter().any(|k| {
                c[*k]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p.as_str())
                    .any(sec::is_dependency_file)
            })
        })
}

/// The `dependabot_alert` webhook.
pub(crate) async fn on_dependabot_alert(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let Some(msg) = dependabot_event(payload, installation_id, team_id) else {
        return Ok(StatusCode::OK);
    };
    if !enabled(state, team_id).await {
        return Ok(StatusCode::OK);
    }
    info!(team_id, repo = %msg.repo, action = %msg.action, alert = msg.alert_number, "Dependabot alert queued for security fixes");
    queue(state, msg).await
}

/// The `security_advisory` webhook (`team_id` empty when GitHub sent it
/// without an installation).
pub(crate) async fn on_security_advisory(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let Some(msg) = advisory_event(payload, installation_id, team_id) else {
        return Ok(StatusCode::OK);
    };
    if !team_id.is_empty() && !enabled(state, team_id).await {
        return Ok(StatusCode::OK);
    }
    info!(team_id, ghsa = %msg.ghsa_id, action = %msg.action, "GitHub advisory queued for security fixes");
    queue(state, msg).await
}

/// A push to a default branch that changed a dependency file.
pub(crate) async fn on_default_branch_push(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    repo: &str,
) {
    if !push_touches_dependencies(payload) || !enabled(state, team_id).await {
        return;
    }
    let msg = SecurityEventMessage {
        team_id: team_id.to_string(),
        installation_id,
        kind: "push".into(),
        repo: repo.to_string(),
        ..Default::default()
    };
    if let Err(e) = queue(state, msg).await {
        warn!(team_id, repo, code = %e, "security: could not queue the inventory refresh");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(id: &str, tier: Tier, status: Status, due: &str, repo: &str) -> Finding {
        Finding {
            id: id.into(),
            tier: Some(tier),
            status,
            due_at: due.into(),
            repo: repo.into(),
            ..Default::default()
        }
    }

    #[test]
    fn findings_are_filtered_sorted_and_paged() {
        let all = vec![
            f("a", Tier::P2, Status::Open, "2026-12-01", "acme/web"),
            f("b", Tier::P0, Status::PrOpen, "2026-11-12", "acme/web"),
            f("c", Tier::P0, Status::Open, "2026-11-11", "acme/api"),
            f("d", Tier::P1, Status::Fixed, "2026-11-20", "acme/web"),
            f("e", Tier::P3, Status::Dismissed, "2027-01-01", "acme/web"),
        ];
        let q = ListQuery::default();
        let (page, next) = list_page(all.clone(), &q).unwrap();
        assert_eq!(
            page.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            vec!["c", "b", "a"]
        );
        assert_eq!(next, None);
        let q = ListQuery {
            status: Some("fixed".into()),
            ..Default::default()
        };
        assert_eq!(list_page(all.clone(), &q).unwrap().0.len(), 1);
        let q = ListQuery {
            status: Some("all".into()),
            tier: Some("p0".into()),
            repo: Some("acme/web".into()),
            ..Default::default()
        };
        assert_eq!(list_page(all.clone(), &q).unwrap().0[0].id, "b");
        for bad in [
            ListQuery {
                status: Some("weird".into()),
                ..Default::default()
            },
            ListQuery {
                tier: Some("P9".into()),
                ..Default::default()
            },
            ListQuery {
                repo: Some("../x".into()),
                ..Default::default()
            },
            ListQuery {
                cursor: Some("x".into()),
                ..Default::default()
            },
        ] {
            assert_eq!(
                list_page(all.clone(), &bad).unwrap_err(),
                StatusCode::BAD_REQUEST
            );
        }
        let many: Vec<Finding> = (0..120)
            .map(|i| {
                f(
                    &format!("{i:03}"),
                    Tier::P2,
                    Status::Open,
                    "2026-12-01",
                    "acme/web",
                )
            })
            .collect();
        let (p1, n1) = list_page(many.clone(), &ListQuery::default()).unwrap();
        assert_eq!((p1.len(), n1.as_deref()), (50, Some("50")));
        let (p3, n3) = list_page(
            many,
            &ListQuery {
                cursor: Some("100".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!((p3.len(), n3), (20, None));
    }

    #[test]
    fn a_dismissal_needs_a_reason_and_a_future_date() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 11, 10).unwrap();
        let b = |r: &str, u: Option<&str>| DismissBody {
            reason: r.into(),
            until: u.map(String::from),
        };
        assert!(check_dismiss(&b(" ", None), today).is_err());
        assert!(check_dismiss(&b(&"x".repeat(501), None), today).is_err());
        assert!(check_dismiss(&b("build only", Some("soon")), today).is_err());
        assert!(check_dismiss(&b("build only", Some("2026-11-09")), today).is_err());
        assert_eq!(
            check_dismiss(&b(" build only ", Some("2027-01-31")), today).unwrap(),
            ("build only".to_string(), Some("2027-01-31".to_string()))
        );
        assert_eq!(check_dismiss(&b("ok", Some("")), today).unwrap().1, None);
        let mut fi = Finding::default();
        assert_eq!(dismissed_reason(&fi), "tolerable_risk");
        fi.verdict = sec::Verdict::NotLoaded;
        assert_eq!(dismissed_reason(&fi), "not_used");
    }

    #[test]
    fn webhooks_queue_only_what_matters() {
        let p = json!({"action": "created", "repository": {"full_name": "acme/web"}, "alert": {"number": 88, "security_advisory": {"ghsa_id": "GHSA-q9fx-2w5c-8m4r"}}});
        let m = dependabot_event(&p, 7, "TEAM#a").unwrap();
        assert_eq!(
            (m.kind.as_str(), m.repo.as_str(), m.alert_number),
            ("dependabot_alert", "acme/web", 88)
        );
        for action in [
            "fixed",
            "dismissed",
            "auto_dismissed",
            "withdrawn",
            "reopened",
        ] {
            let mut q = p.clone();
            q["action"] = json!(action);
            assert!(dependabot_event(&q, 7, "TEAM#a").is_some(), "{action}");
        }
        let mut q = p.clone();
        q["action"] = json!("assignees_changed");
        assert!(dependabot_event(&q, 7, "TEAM#a").is_none());
        let adv =
            json!({"action": "withdrawn", "security_advisory": {"ghsa_id": "GHSA-q9fx-2w5c-8m4r"}});
        let m = advisory_event(&adv, 0, "").unwrap();
        assert!(m.team_id.is_empty() && m.action == "withdrawn");
        assert!(advisory_event(
            &json!({"action": "published", "security_advisory": {"ghsa_id": "nope"}}),
            0,
            ""
        )
        .is_none());
        let push =
            json!({"commits": [{"added": [], "modified": ["web/pnpm-lock.yaml"], "removed": []}]});
        assert!(push_touches_dependencies(&push));
        assert!(!push_touches_dependencies(
            &json!({"commits": [{"modified": ["src/a.ts"]}]})
        ));
    }

    #[test]
    fn a_saved_config_is_validated() {
        let ok: SecurityConfig =
            serde_json::from_value(json!({"enabled": true, "labels": ["security"]})).unwrap();
        assert!(ok.validate().is_ok() && ok.dry_run);
        let bad: SecurityConfig =
            serde_json::from_value(json!({"due_days": {"P0": 0, "P1": 7, "P2": 30, "P3": 90}}))
                .unwrap();
        assert!(bad.validate().is_err());
    }
}
