//! The plugins API: sources (the team's private marketplace), installed
//! plugins, runs, drafts and approvals, and the audit log.
//!
//! Every rule lives in `common::plugins`; these handlers check the caller's
//! role (the capability table in `permissions` names each one), turn the
//! request into an engine call and the answer into JSON. Every route
//! refuses with `flag_off` while the team's `plugins` flag is off.
//!
//! The MCP servers keep their routes at `/api/plugins`, `/api/plugins/
//! catalog` and `/api/plugins/:id/...` (the Connections page); plugin
//! packages are under `/api/plugins/installed`, `/api/plugins/available`,
//! `/api/plugins/sources`, `/api/plugins/runs` and `/api/plugins/drafts`.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    Extension, Json,
};
use common::plugins::kv::Kv;
use common::plugins::source::{self, Track};
use common::plugins::{
    audit, brain, chat, code, drafts, flag, install, kv, limits, run, schedule, signed, Refusal,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn refused(r: Refusal) -> (StatusCode, Json<Value>) {
    let status = match r.code {
        code::FLAG_OFF
        | code::FORBIDDEN
        | code::SAME_PERSON
        | code::NOT_IN_GROUP
        | code::NEEDS_SECOND_ADMIN
        | code::NOT_GRANTED => StatusCode::FORBIDDEN,
        code::NOT_FOUND => StatusCode::NOT_FOUND,
        code::INVALID => StatusCode::BAD_REQUEST,
        code::LIMIT_REACHED => StatusCode::TOO_MANY_REQUESTS,
        code::STORE => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::CONFLICT,
    };
    if status == StatusCode::INTERNAL_SERVER_ERROR {
        tracing::error!(error = %r.error, "Plugin store error");
    }
    (status, Json(json!({ "error": r.error, "code": r.code })))
}

fn kv(state: &AppState) -> kv::Dynamo {
    kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

fn queue(state: &AppState) -> schedule::Sqs {
    schedule::Sqs {
        client: state.sqs.clone(),
        url: state.config.ticket_queue_url.clone(),
    }
}

fn scheduler(state: &AppState) -> schedule::EventBridge {
    schedule::EventBridge {
        client: state.scheduler.clone(),
        group: state.config.plugin_schedule_group.clone(),
        role_arn: state.config.plugin_scheduler_role_arn.clone(),
        queue_arn: state.config.ticket_queue_arn.clone(),
    }
}

fn usage(state: &AppState) -> limits::Dynamo {
    limits::Dynamo {
        client: state.dynamo.clone(),
        analytics_table: state.config.analytics_table_name.clone(),
        settings_table: state.config.settings_table_name.clone(),
    }
}

fn runs_table(state: &AppState) -> run::RunsTable {
    run::RunsTable {
        client: state.dynamo.clone(),
        table: state.config.runs_table_name.clone(),
    }
}

fn cipher(state: &AppState) -> common::mcp_credentials::Kms<'_> {
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

async fn on(state: &AppState, team: &str) -> Result<kv::Dynamo, (StatusCode, Json<Value>)> {
    let kv = kv(state);
    flag::require(&kv, team).await.map_err(refused)?;
    Ok(kv)
}

fn is_admin(claims: &Claims) -> bool {
    claims.role_level() >= 3
}

fn actor(claims: &Claims) -> drafts::Actor {
    drafts::Actor::new(&claims.email, is_admin(claims))
}

async fn admins(state: &AppState, team: &str) -> usize {
    super::tools::admin_count(state, team).await
}

/// The team's own GitHub App installation.
async fn team_installation(state: &AppState, team: &str) -> Option<u64> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".into()),
        )
        .send()
        .await
        .ok()?;
    let item = out.item()?;
    item.get("github_install_id")
        .or_else(|| item.get("installation_id"))
        .and_then(|v| v.as_n().ok().or_else(|| v.as_s().ok()))
        .and_then(|n| n.parse().ok())
        .filter(|n: &u64| *n > 0)
}

async fn send(state: &AppState, body: Value) -> Result<(), (StatusCode, Json<Value>)> {
    use common::plugins::schedule::Queue;
    queue(state)
        .send(&body)
        .await
        .map_err(|e| refused(Refusal::new(code::STORE, e)))
}

/// The tokens a plugin used this month.
async fn used_tokens(state: &AppState, team: &str, plugin: &str) -> u64 {
    use common::plugins::limits::UsageRead;
    usage(state).plugin_month(team, plugin).await.unwrap_or(0)
}

/// The contract's `Installed`, with the triggers' forms as the dashboard
/// reads them and whether this person may approve what is waiting.
async fn installed_view(
    state: &AppState,
    kv: &kv::Dynamo,
    team: &str,
    p: &install::Installed,
    claims: &Claims,
) -> Value {
    let snap = install::current_snapshot(kv, team, p).await.ok();
    let snap = match (&snap, &p.pending) {
        (None, Some(pd)) => common::plugins::package::snapshot(kv, team, &pd.version.hash)
            .await
            .ok()
            .flatten(),
        _ => snap,
    };
    let mut v = p.view(snap.as_ref(), used_tokens(state, team, &p.id).await);
    if let Some(ts) = v["triggers"].as_array_mut() {
        for t in ts.iter_mut() {
            let mut channels = Vec::new();
            if let Some(form) = t["form"].as_object_mut() {
                for (_, f) in form.iter_mut() {
                    if let Some(list) = f.as_array() {
                        channels.extend(list.iter().cloned());
                        *f = json!("list");
                    }
                }
            }
            if !channels.is_empty() {
                t["channels"] = Value::Array(channels);
            }
        }
    }
    let why = match &p.pending {
        None => None,
        Some(_) if !is_admin(claims) => Some(code::FORBIDDEN),
        Some(pd)
            if pd.requested_by.eq_ignore_ascii_case(&claims.email)
                && admins(state, team).await > 1 =>
        {
            Some(code::NEEDS_SECOND_ADMIN)
        }
        Some(_) => None,
    };
    v["can_approve_pending"] = json!(p.pending.is_some() && why.is_none());
    v["why_not"] = json!(why);
    v
}

fn run_view(r: &run::RunRecord, name: &str) -> Value {
    let mut v = serde_json::to_value(r).unwrap_or_default();
    v["plugin_name"] = json!(name);
    v
}

// ── Flag and team settings ──────────────────────────────────────────────────

/// GET /api/plugins/flag
pub async fn get_flag(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let on = flag::enabled(&kv(&state), &claims.team_id).await;
    Ok(Json(
        json!({ "enabled": on, "can_toggle": super::api::is_platform_admin(&claims) }),
    ))
}

/// GET /api/plugins/settings
pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let s = flag::settings(&kv, &claims.team_id)
        .await
        .map_err(refused)?;
    Ok(Json(json!({
        "autonomy_cap": s.cap(),
        "admins": admins(&state, &claims.team_id).await,
        "slack_team_id": s.slack_team_id,
        "slack_signing_secret_set": !s.slack_signing_secret_sealed.is_empty(),
        "teams_webhook_set": !s.teams_secret_sealed.is_empty(),
    })))
}

#[derive(Deserialize)]
pub struct SettingsBody {
    #[serde(default)]
    autonomy_cap: Option<common::tool_registry::Risk>,
    #[serde(default)]
    slack_team_id: Option<String>,
    #[serde(default)]
    slack_signing_secret: Option<String>,
    #[serde(default)]
    teams_secret: Option<String>,
}

/// PUT /api/plugins/settings { autonomy_cap, slack_team_id?, slack_signing_secret?, teams_secret? }
pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SettingsBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| (s, Json(json!({ "error": "Only admins can change plugin settings.", "code": code::FORBIDDEN }))))?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let mut s = flag::settings(&kv, team).await.map_err(refused)?;
    if let Some(c) = body.autonomy_cap {
        s.autonomy_cap = c;
    }
    let seal = |purpose: &'static str, v: String| {
        let state = state.clone();
        let team = team.to_string();
        async move {
            use base64::Engine;
            use common::mcp_credentials::Cipher;
            let ctx = std::collections::HashMap::from([
                ("team".to_string(), team),
                ("purpose".to_string(), purpose.to_string()),
            ]);
            cipher(&state)
                .encrypt(ctx, v.as_bytes())
                .await
                .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                .map_err(|e| refused(Refusal::new(code::STORE, e)))
        }
    };
    if let Some(id) = body.slack_team_id {
        let id = id.trim().to_string();
        if !id.is_empty() && !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(refused(Refusal::new(
                code::INVALID,
                "That isn't a Slack workspace id.",
            )));
        }
        if !s.slack_team_id.is_empty() {
            let _ = kv
                .delete(kv::CHAT_INDEX, &format!("slack|{}", s.slack_team_id))
                .await;
        }
        if !id.is_empty() {
            kv.put(
                kv::CHAT_INDEX,
                &format!("slack|{id}"),
                &json!({ "team": team }),
                kv::Expect::Any,
            )
            .await
            .map_err(|e| refused(Refusal::new(code::STORE, e.to_string())))?;
        }
        s.slack_team_id = id;
    }
    if let Some(sec) = body.slack_signing_secret.filter(|x| !x.trim().is_empty()) {
        s.slack_signing_secret_sealed = seal("plugin-slack", sec.trim().to_string()).await?;
    }
    if let Some(sec) = body.teams_secret.filter(|x| !x.trim().is_empty()) {
        s.teams_secret_sealed = seal("plugin-teams", sec.trim().to_string()).await?;
        if s.teams_token.is_empty() {
            s.teams_token = common::plugins::new_id();
            kv.put(
                kv::CHAT_INDEX,
                &format!("teams|{}", s.teams_token),
                &json!({ "team": team }),
                kv::Expect::Any,
            )
            .await
            .map_err(|e| refused(Refusal::new(code::STORE, e.to_string())))?;
        }
    }
    flag::save_settings(&kv, team, &s).await.map_err(refused)?;
    audit::record(
        &kv,
        team,
        audit::Event::new(
            &claims.email,
            audit::ActorKind::Person,
            audit::action::SETTINGS_SAVED,
            "ok",
        )
        .detail(format!("autonomy cap {}", s.cap().as_str())),
    )
    .await;
    Ok(Json(
        json!({ "autonomy_cap": s.cap(), "teams_webhook_path": if s.teams_token.is_empty() { String::new() } else { format!("/webhooks/teams/plugins/{}", s.teams_token) } }),
    ))
}

// ── Sources ─────────────────────────────────────────────────────────────────

/// Reads GitHub to decide how a repository can be read.
struct GitHubAccess<'a> {
    state: &'a AppState,
}

#[async_trait::async_trait]
impl source::AccessCheck for GitHubAccess<'_> {
    async fn is_public(&self, repo: &str) -> Result<bool, String> {
        let r = self
            .state
            .http
            .get(format!("https://api.github.com/repos/{repo}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !r.status().is_success() {
            return Ok(false);
        }
        let v: Value = r.json().await.map_err(|e| e.to_string())?;
        Ok(v["private"] == json!(false))
    }
    async fn installation_reads(&self, installation_id: u64, repo: &str) -> Result<bool, String> {
        let token = crate::auth::github_app::get_installation_token(self.state, installation_id)
            .await
            .map_err(|e| e.to_string())?;
        let r = self
            .state
            .http
            .get(format!("https://api.github.com/repos/{repo}"))
            .header("Authorization", format!("token {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(r.status().is_success())
    }
}

/// GET /api/plugins/sources
pub async fn list_sources(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let list = source::list(&kv, &claims.team_id).await.map_err(refused)?;
    Ok(Json(json!({ "sources": list })))
}

#[derive(Deserialize)]
pub struct AddSource {
    url: String,
    #[serde(default)]
    track: Option<Track>,
}

/// POST /api/plugins/sources { url, track? }
pub async fn add_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<AddSource>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(
                json!({ "error": "Only admins can add plugin sources.", "code": code::FORBIDDEN }),
            ),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let inst = team_installation(&state, &claims.team_id).await;
    let src = source::add(
        &kv,
        &GitHubAccess { state: &state },
        &claims.team_id,
        inst,
        &body.url,
        body.track,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    send(&state, json!({ "type": "plugin_sync", "team_id": claims.team_id, "source_id": src.id, "force": true, "reason": "admin" })).await?;
    Ok(Json(json!({ "source": src })))
}

/// POST /api/plugins/sources/:id/sync
pub async fn sync_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can sync sources.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let (src, _) = source::get(&kv, &claims.team_id, &id)
        .await
        .map_err(refused)?;
    send(&state, json!({ "type": "plugin_sync", "team_id": claims.team_id, "source_id": id, "force": true, "reason": "admin" })).await?;
    Ok(Json(json!({ "source": src })))
}

/// DELETE /api/plugins/sources/:id
pub async fn remove_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can remove sources.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    source::remove(&kv, &claims.team_id, &id, &claims.email)
        .await
        .map_err(refused)?;
    Ok(Json(json!({ "ok": true })))
}

/// GET /api/plugins/available — every plugin the team's sources hold.
pub async fn available(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let installed: Vec<String> = install::list(&kv, team)
        .await
        .map_err(refused)?
        .into_iter()
        .map(|p| p.id)
        .collect();
    let mut out = Vec::new();
    for s in source::list(&kv, team).await.map_err(refused)? {
        for f in &s.plugins {
            let snap = if f.hash.is_empty() {
                None
            } else {
                common::plugins::package::snapshot(&kv, team, &f.hash)
                    .await
                    .ok()
                    .flatten()
            };
            let (tools, triggers, outputs) = match &snap {
                Some(sn) => {
                    let p = install::Installed::default();
                    let v = p.view(Some(sn), 0);
                    (
                        v["tools"].clone(),
                        v["triggers"].clone(),
                        v["outputs"].clone(),
                    )
                }
                None => (json!([]), json!([]), json!([])),
            };
            out.push(json!({
                "source_id": s.id, "path": f.path, "name": f.name, "version": f.version,
                "description": f.description, "installed": installed.contains(&f.name),
                "problems": f.problems, "tools": tools, "triggers": triggers, "outputs": outputs,
            }));
        }
    }
    Ok(Json(json!({ "plugins": out })))
}

// ── Installed ───────────────────────────────────────────────────────────────

/// GET /api/plugins/installed
pub async fn list_installed(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let mut out = Vec::new();
    for p in install::list(&kv, &claims.team_id).await.map_err(refused)? {
        out.push(installed_view(&state, &kv, &claims.team_id, &p, &claims).await);
    }
    Ok(Json(json!({ "plugins": out })))
}

/// GET /api/plugins/installed/:id
pub async fn get_installed(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let (p, _) = install::get(&kv, &claims.team_id, &id)
        .await
        .map_err(refused)?;
    let versions: Vec<Value> = p
        .history
        .iter()
        .map(|v| json!({ "hash": v.hash, "sha": v.sha, "version": v.version, "created_at": v.at, "approved_by": v.approved_by }))
        .collect();
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await, "versions": versions }),
    ))
}

#[derive(Deserialize)]
pub struct InstallBody {
    source_id: String,
    #[serde(default)]
    path: String,
}

/// POST /api/plugins/install { source_id, path }
pub async fn install_plugin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<InstallBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can install plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let p = install::install(
        &kv,
        team,
        &body.source_id,
        &body.path,
        &claims.email,
        admins(&state, team).await,
    )
    .await
    .map_err(refused)?;
    if p.active() {
        let m = install::current_snapshot(&kv, team, &p)
            .await
            .ok()
            .map(|s| s.manifest);
        install::sync_crons(&kv, &scheduler(&state), team, &p, m.as_ref()).await;
    }
    info!(team, plugin = %p.id, status = ?p.status, "Plugin installed");
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, team, &p, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct HashBody {
    hash: String,
}

/// POST /api/plugins/installed/:id/approve { hash }
pub async fn approve_pending(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<HashBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can approve plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let p = install::approve(
        &kv,
        team,
        &id,
        &body.hash,
        &claims.email,
        admins(&state, team).await,
    )
    .await
    .map_err(refused)?;
    let m = install::current_snapshot(&kv, team, &p)
        .await
        .ok()
        .map(|s| s.manifest);
    install::sync_crons(&kv, &scheduler(&state), team, &p, m.as_ref()).await;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, team, &p, &claims).await }),
    ))
}

/// POST /api/plugins/installed/:id/decline { hash }
pub async fn decline_pending(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<HashBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can decline plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let p = install::decline(&kv, &claims.team_id, &id, &body.hash, &claims.email)
        .await
        .map_err(refused)?;
    Ok(Json(match p {
        Some(p) => {
            json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await })
        }
        None => json!({ "plugin": null, "removed": true }),
    }))
}

#[derive(Deserialize)]
pub struct OptionsBody {
    #[serde(default)]
    auto_update: Option<bool>,
    #[serde(default)]
    track: Option<Track>,
}

/// PUT /api/plugins/installed/:id/options { auto_update?, track? }
pub async fn set_options(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<OptionsBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| (s, Json(json!({ "error": "Only admins can change update options.", "code": code::FORBIDDEN }))))?;
    let kv = on(&state, &claims.team_id).await?;
    let (p, resync) = install::set_options(
        &kv,
        &claims.team_id,
        &id,
        body.auto_update,
        body.track,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    if resync {
        send(&state, json!({ "type": "plugin_sync", "team_id": claims.team_id, "plugin_id": id, "reason": "admin" })).await?;
    }
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await }),
    ))
}

/// POST /api/plugins/installed/:id/rollback
pub async fn rollback(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can roll back plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let p = install::rollback(&kv, team, &id, &claims.email)
        .await
        .map_err(refused)?;
    let m = install::current_snapshot(&kv, team, &p)
        .await
        .ok()
        .map(|s| s.manifest);
    install::sync_crons(&kv, &scheduler(&state), team, &p, m.as_ref()).await;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, team, &p, &claims).await }),
    ))
}

/// POST /api/plugins/installed/:id/sync
pub async fn sync_plugin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can sync plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let (p, _) = install::get(&kv, &claims.team_id, &id)
        .await
        .map_err(refused)?;
    send(&state, json!({ "type": "plugin_sync", "team_id": claims.team_id, "plugin_id": id, "force": true, "reason": "admin" })).await?;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct SettingsValues {
    values: BTreeMap<String, String>,
}

/// PUT /api/plugins/installed/:id/settings { values }
pub async fn set_plugin_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<SettingsValues>,
) -> Reply {
    claims.require_role(3).map_err(|s| (s, Json(json!({ "error": "Only admins can change plugin settings.", "code": code::FORBIDDEN }))))?;
    let kv = on(&state, &claims.team_id).await?;
    let p = install::set_settings(
        &kv,
        &cipher(&state),
        &claims.team_id,
        &id,
        &body.values,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct LimitBody {
    monthly_tokens: u64,
}

/// PUT /api/plugins/installed/:id/limit { monthly_tokens }
pub async fn set_limit(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<LimitBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can set plugin limits.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let p = install::set_limit(
        &kv,
        &claims.team_id,
        &id,
        body.monthly_tokens,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct ApproversBody {
    #[serde(default)]
    members: Vec<String>,
    #[serde(default = "one")]
    min: u32,
    #[serde(default)]
    notify: Option<String>,
}

fn one() -> u32 {
    1
}

/// PUT /api/plugins/installed/:id/approvers { members, min, notify? }
pub async fn set_approvers(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<ApproversBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can choose approvers.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let p = install::set_approvers(
        &kv,
        &claims.team_id,
        &id,
        body.members,
        body.min,
        body.notify,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, &claims.team_id, &p, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct EnabledBody {
    enabled: bool,
}

/// PUT /api/plugins/installed/:id/enabled { enabled }
pub async fn set_enabled(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Reply {
    claims.require_role(3).map_err(|s| (s, Json(json!({ "error": "Only admins can turn plugins on or off.", "code": code::FORBIDDEN }))))?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let p = install::set_enabled(&kv, team, &id, body.enabled, &claims.email)
        .await
        .map_err(refused)?;
    let m = install::current_snapshot(&kv, team, &p)
        .await
        .ok()
        .map(|s| s.manifest);
    install::sync_crons(&kv, &scheduler(&state), team, &p, m.as_ref()).await;
    Ok(Json(
        json!({ "plugin": installed_view(&state, &kv, team, &p, &claims).await }),
    ))
}

/// DELETE /api/plugins/installed/:id
pub async fn uninstall(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only admins can uninstall plugins.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    install::uninstall(&kv, &scheduler(&state), &claims.team_id, &id, &claims.email)
        .await
        .map_err(refused)?;
    Ok(Json(json!({ "ok": true })))
}

// ── Runs ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RunBody {
    #[serde(default)]
    inputs: BTreeMap<String, Value>,
    #[serde(default)]
    repo: Option<String>,
}

/// POST /api/plugins/installed/:id/runs { inputs, repo? }
pub async fn start_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<RunBody>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't start plugin runs.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = kv(&state);
    let r = run::request(
        &kv,
        &queue(&state),
        Some(&runs_table(&state)),
        &usage(&state),
        &claims.team_id,
        &id,
        "manual",
        body.inputs,
        body.repo.filter(|r| !r.is_empty()),
        None,
        &claims.email,
    )
    .await
    .map_err(refused)?;
    Ok(Json(json!({ "run": run_view(&r, &id) })))
}

#[derive(Deserialize)]
pub struct RunsQuery {
    #[serde(default)]
    plugin: Option<String>,
}

/// GET /api/plugins/runs?plugin=
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RunsQuery>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let runs = run::list(&kv, &claims.team_id, q.plugin.as_deref(), 100)
        .await
        .map_err(refused)?;
    let v: Vec<Value> = runs.iter().map(|r| run_view(r, &r.plugin_id)).collect();
    Ok(Json(json!({ "runs": v })))
}

async fn approvers_of(kv: &kv::Dynamo, team: &str, plugin: &str) -> install::Approvers {
    install::get(kv, team, plugin)
        .await
        .map(|(p, _)| p.approvers)
        .unwrap_or_default()
}

async fn draft_view(kv: &kv::Dynamo, team: &str, a: &drafts::Artifact, claims: &Claims) -> Value {
    let versions = drafts::versions(kv, team, &a.id).await.unwrap_or_default();
    let approvers = approvers_of(kv, team, &a.plugin_id).await;
    drafts::view(a, &versions, &approvers, Some(&actor(claims)))
}

/// GET /api/plugins/runs/:run_id
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(run_id): Path<String>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let r = run::get(&kv, team, &run_id).await.map_err(refused)?;
    let mut arts = Vec::new();
    for a in drafts::list(&kv, team, None, None, Some(&run_id))
        .await
        .map_err(refused)?
    {
        arts.push(draft_view(&kv, team, &a, &claims).await);
    }
    let events = audit::for_filter(
        &kv,
        team,
        &audit::Filter {
            run_id: Some(run_id.clone()),
            ..Default::default()
        },
    )
    .await;
    Ok(Json(
        json!({ "run": run_view(&r, &r.plugin_id), "artifacts": arts, "audit": events }),
    ))
}

// ── Drafts ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct DraftsQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    plugin: Option<String>,
}

/// GET /api/plugins/drafts?status=in_review|all&plugin=
pub async fn list_drafts(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<DraftsQuery>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let status = match q.status.as_deref() {
        None | Some("all") | Some("") => None,
        Some(s) => Some(
            serde_json::from_value::<drafts::Status>(json!(s))
                .map_err(|_| refused(Refusal::new(code::INVALID, "Unknown status.")))?,
        ),
    };
    let team = claims.team_id.as_str();
    let mut out = Vec::new();
    for a in drafts::list(&kv, team, status, q.plugin.as_deref(), None)
        .await
        .map_err(refused)?
    {
        out.push(draft_view(&kv, team, &a, &claims).await);
    }
    Ok(Json(json!({ "drafts": out })))
}

/// GET /api/plugins/drafts/:id
pub async fn get_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let kv = on(&state, &claims.team_id).await?;
    let (a, _) = drafts::get(&kv, &claims.team_id, &id)
        .await
        .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, &claims.team_id, &a, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct EditBody {
    content: String,
    #[serde(default)]
    title: Option<String>,
}

/// PUT /api/plugins/drafts/:id { content, title? }
pub async fn edit_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<EditBody>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't edit drafts.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let min = approvers_of(&kv, team, &a.plugin_id).await.min;
    let a = drafts::edit(
        &kv,
        &scheduler(&state),
        team,
        &id,
        &body.content,
        body.title.as_deref(),
        min,
        &actor(&claims),
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

/// POST /api/plugins/drafts/:id/submit
pub async fn submit_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(1).map_err(|s| (s, Json(json!({ "error": "Viewers can't send drafts for review.", "code": code::FORBIDDEN }))))?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let min = approvers_of(&kv, team, &a.plugin_id).await.min;
    let a = drafts::submit(&kv, team, &id, min, &actor(&claims))
        .await
        .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct ApproveBody {
    version: u32,
    #[serde(default)]
    publish_at: Option<String>,
}

fn parse_time(
    s: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, (StatusCode, Json<Value>)> {
    match s.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(t) => chrono::DateTime::parse_from_rfc3339(t)
            .map(|d| Some(d.with_timezone(&chrono::Utc)))
            .map_err(|_| {
                refused(Refusal::new(
                    code::INVALID,
                    "The publish time must be a date and time with its time zone.",
                ))
            }),
    }
}

/// POST /api/plugins/drafts/:id/approve { version, publish_at? }
pub async fn approve_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<ApproveBody>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't approve drafts.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let at = parse_time(body.publish_at.as_deref())?;
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let approvers = approvers_of(&kv, team, &a.plugin_id).await;
    let a = drafts::approve(
        &kv,
        &scheduler(&state),
        &queue(&state),
        team,
        &id,
        body.version,
        at,
        &approvers,
        &actor(&claims),
        "dashboard",
        chrono::Utc::now(),
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

#[derive(Deserialize)]
pub struct RejectBody {
    version: u32,
    #[serde(default)]
    reason: String,
}

/// POST /api/plugins/drafts/:id/reject { version, reason? }
pub async fn reject_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<RejectBody>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't reject drafts.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let approvers = approvers_of(&kv, team, &a.plugin_id).await;
    let a = drafts::reject(
        &kv,
        team,
        &id,
        body.version,
        &body.reason,
        &approvers,
        &actor(&claims),
        "dashboard",
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

#[derive(Deserialize, Default)]
pub struct StopBody {
    #[serde(default)]
    reason: String,
}

/// POST /api/plugins/drafts/:id/stop { reason? }
pub async fn stop_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<StopBody>>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't stop drafts.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let approvers = approvers_of(&kv, team, &a.plugin_id).await;
    let reason = body.map(|b| b.0.reason).unwrap_or_default();
    let a = drafts::stop(
        &kv,
        &scheduler(&state),
        team,
        &id,
        &reason,
        &approvers,
        &actor(&claims),
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

/// POST /api/plugins/drafts/:id/cancel
pub async fn cancel_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(1).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Viewers can't cancel a publish.", "code": code::FORBIDDEN })),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let team = claims.team_id.as_str();
    let (a, _) = drafts::get(&kv, team, &id).await.map_err(refused)?;
    let approvers = approvers_of(&kv, team, &a.plugin_id).await;
    let a = drafts::cancel(
        &kv,
        &scheduler(&state),
        team,
        &id,
        &approvers,
        &actor(&claims),
    )
    .await
    .map_err(refused)?;
    Ok(Json(
        json!({ "draft": draft_view(&kv, team, &a, &claims).await }),
    ))
}

// ── Audit and brain ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    plugin: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
}

/// GET /api/audit?plugin=&action=&cursor=
pub async fn audit_log(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<AuditQuery>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(
                json!({ "error": "Only admins can read the audit log.", "code": code::FORBIDDEN }),
            ),
        )
    })?;
    let kv = on(&state, &claims.team_id).await?;
    let filter = audit::Filter {
        plugin_id: q.plugin.filter(|s| !s.is_empty()),
        action: q.action.filter(|s| !s.is_empty()),
        ..Default::default()
    };
    let cursor = q
        .cursor
        .filter(|c| c.len() <= 64 && c.chars().all(|x| x.is_ascii_alphanumeric()));
    let (events, next) = audit::page(
        &kv,
        &claims.team_id,
        &filter,
        cursor.as_deref(),
        audit::PAGE,
    )
    .await
    .map_err(|e| refused(Refusal::new(code::STORE, e)))?;
    Ok(Json(json!({ "events": events, "next": next })))
}

/// GET /api/plugins/brain-sources
pub async fn brain_sources(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    Ok(Json(
        json!({ "sources": brain::sources(&kv(&state), &claims.team_id).await }),
    ))
}

// ── Public: signed email approval ───────────────────────────────────────────

#[derive(Deserialize)]
pub struct TokenQuery {
    #[serde(default)]
    t: String,
}

fn page(title: &str, body: &str) -> Html<String> {
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    };
    Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{t}</title>\
         <style>body{{font-family:system-ui,sans-serif;max-width:560px;margin:48px auto;padding:0 16px;color:#18181b}}pre{{white-space:pre-wrap;background:#f4f4f5;padding:12px;border-radius:8px}}button{{font-size:16px;padding:10px 18px;margin-right:8px;border-radius:8px;border:1px solid #d4d4d8;background:#fff;cursor:pointer}}button.primary{{background:#18181b;color:#fff}}</style></head>\
         <body><h1>{t}</h1>{body}</body></html>",
        t = esc(title),
    ))
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn link_draft(
    state: &AppState,
    token: &str,
) -> Result<(signed::ApprovalLink, drafts::Artifact, kv::Dynamo), String> {
    let link = signed::verify(
        &state.secrets.jwt_secret,
        token,
        chrono::Utc::now().timestamp(),
    )
    .map_err(|e| match e {
        signed::TokenError::Expired => {
            "This link has expired. Open the draft in CoderHelm instead.".to_string()
        }
        _ => "This link isn't valid.".to_string(),
    })?;
    let kv = kv(state);
    if !flag::enabled(&kv, &link.team_id).await {
        return Err("Plugins are not turned on for this team.".into());
    }
    let (a, _) = drafts::by_approval(&kv, &link.team_id, &link.approval_id)
        .await
        .map_err(|e| e.error)?;
    Ok((link, a, kv))
}

/// GET /plugins/approve?t= — shows the draft version the link names; changes nothing.
pub async fn email_approve_page(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TokenQuery>,
) -> Response {
    match link_draft(&state, &q.t).await {
        Err(e) => (
            StatusCode::BAD_REQUEST,
            page("Can't open this link", &format!("<p>{}</p>", esc(&e))),
        )
            .into_response(),
        Ok((link, a, kv)) => {
            let content = drafts::version(&kv, &link.team_id, &a.id, link.version)
                .await
                .map(|v| v.content)
                .unwrap_or_default();
            let stale = a.version != link.version;
            let body = format!(
                "<p>{} from the {} plugin, version {}.</p><pre>{}</pre>{}\
                 <form method=\"post\"><input type=\"hidden\" name=\"t\" value=\"{}\">\
                 <button class=\"primary\" name=\"decision\" value=\"approve\">Approve this version</button>\
                 <button name=\"decision\" value=\"reject\">Reject</button></form>",
                esc(&a.title),
                esc(&a.plugin_name),
                link.version,
                esc(common::truncate_str(&content, 4000)),
                if stale { "<p><strong>The draft changed since this email. Approving this link will be refused; open the new version in CoderHelm.</strong></p>" } else { "" },
                esc(&q.t),
            );
            page("Approve a draft", &body).into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct EmailDecision {
    t: String,
    decision: String,
}

type Item = std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;

/// The role of the team member with this email, if one is on the team now
/// (an invite doesn't count).
fn current_role(items: &[Item], email: &str) -> Option<String> {
    let get = |i: &Item, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    items
        .iter()
        .find(|i| {
            get(i, "email").trim().eq_ignore_ascii_case(email.trim())
                && get(i, "status") != "invited"
        })
        .map(|i| match get(i, "role").as_str() {
            "" => "member".to_string(),
            r => r.to_string(),
        })
}

/// The person an email link was sent to, as they are now: still on the team
/// and still allowed to approve plugin drafts. A link outlives neither.
async fn email_actor(state: &AppState, team: &str, email: &str) -> Result<drafts::Actor, String> {
    let gone = || "You aren't on this team any more, so this link no longer works.".to_string();
    let mut items = Vec::new();
    let mut start = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(
                ":pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team.into()),
            )
            .expression_attribute_values(
                ":p",
                aws_sdk_dynamodb::types::AttributeValue::S("USER#".into()),
            )
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| {
                warn!(error = %e, "Team not read for an email approval");
                "CoderHelm couldn't check this link just now. Try again.".to_string()
            })?;
        items.extend(out.items().iter().cloned());
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    let role = current_role(&items, email).ok_or_else(gone)?;
    if !super::roles::holder_for_role(state, team, &role)
        .await
        .holds(CAP_APPROVE)
    {
        return Err("Your role can't approve plugin drafts any more.".into());
    }
    Ok(drafts::Actor::new(email, false))
}

/// The capability behind every plugin draft decision.
const CAP_APPROVE: &str = "plugins.approve";

/// Takes a person who left the team off every plugin's approvers. Logged on
/// failure; the removal itself has happened (and an email link checks the
/// team anyway).
pub async fn member_removed(state: &AppState, team: &str, email: &str, by: &str) {
    match install::drop_approver(&kv(state), team, email, by).await {
        Ok(changed) if !changed.is_empty() => {
            info!(team_id = %team, plugins = changed.len(), "Removed member taken off plugin approvers")
        }
        Ok(_) => {}
        Err(r) => warn!(team_id = %team, error = %r.error, "Plugin approvers not updated"),
    }
}

/// POST /plugins/approve (form: t, decision) — approves or rejects exactly
/// the version the link names, as the approver it names.
pub async fn email_approve(
    State(state): State<Arc<AppState>>,
    axum::Form(f): axum::Form<EmailDecision>,
) -> Response {
    let (link, a, kv) = match link_draft(&state, &f.t).await {
        Ok(x) => x,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                page("Can't use this link", &format!("<p>{}</p>", esc(&e))),
            )
                .into_response()
        }
    };
    let team = link.team_id.as_str();
    let who = match email_actor(&state, team, &link.email).await {
        Ok(w) => w,
        Err(e) => {
            return (
                StatusCode::FORBIDDEN,
                page("Can't use this link", &format!("<p>{}</p>", esc(&e))),
            )
                .into_response()
        }
    };
    let approvers = approvers_of(&kv, team, &a.plugin_id).await;
    let result = if f.decision == "reject" {
        drafts::reject(
            &kv,
            team,
            &a.id,
            link.version,
            "Rejected from the email link",
            &approvers,
            &who,
            "email",
        )
        .await
    } else {
        drafts::approve(
            &kv,
            &scheduler(&state),
            &queue(&state),
            team,
            &a.id,
            link.version,
            None,
            &approvers,
            &who,
            "email",
            chrono::Utc::now(),
        )
        .await
    };
    match result {
        Ok(d) => page(
            if f.decision == "reject" {
                "Rejected"
            } else {
                "Approved"
            },
            &format!("<p>{} is now {:?}.</p>", esc(&d.title), d.status).to_lowercase(),
        )
        .into_response(),
        Err(r) => (
            StatusCode::CONFLICT,
            page("Not done", &format!("<p>{}</p>", esc(&r.error))),
        )
            .into_response(),
    }
}

// ── Public: briefs from Slack and Teams (signature verified) ────────────────

async fn chat_team(state: &AppState, key: &str) -> Option<String> {
    kv(state)
        .get(kv::CHAT_INDEX, key)
        .await
        .ok()
        .flatten()?
        .data["team"]
        .as_str()
        .map(str::to_string)
}

async fn open_secret(state: &AppState, team: &str, purpose: &str, sealed: &str) -> Option<String> {
    use base64::Engine;
    use common::mcp_credentials::Cipher;
    let blob = base64::engine::general_purpose::STANDARD
        .decode(sealed)
        .ok()?;
    let ctx = std::collections::HashMap::from([
        ("team".to_string(), team.to_string()),
        ("purpose".to_string(), purpose.to_string()),
    ]);
    String::from_utf8(cipher(state).decrypt(ctx, &blob).await.ok()?).ok()
}

async fn start_chat_brief(state: &AppState, team: &str, text: &str, who: &str) -> String {
    let kv = kv(state);
    let Some((plugin, brief)) = chat::parse_brief(text) else {
        return "Write: brief <plugin> <what you need>".into();
    };
    let inputs = BTreeMap::from([("brief".to_string(), json!(brief))]);
    match run::request(
        &kv,
        &queue(state),
        Some(&runs_table(state)),
        &usage(state),
        team,
        &plugin,
        "manual",
        inputs,
        None,
        None,
        who,
    )
    .await
    {
        Ok(r) => {
            audit::record(
                &kv,
                team,
                audit::Event::new(
                    who,
                    audit::ActorKind::Person,
                    audit::action::CHAT_BRIEF,
                    "ok",
                )
                .plugin(&plugin, &r.plugin_version)
                .run(&r.run_id),
            )
            .await;
            format!("Started {plugin}. Its drafts will wait for review in CoderHelm.")
        }
        Err(e) => format!("Couldn't start {plugin}: {}", e.error),
    }
}

/// POST /webhooks/slack/plugins — a Slack slash command, verified with the
/// team's Slack app signing secret.
pub async fn slack_command(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let text = String::from_utf8_lossy(&body).to_string();
    let form = chat::slack_form(&text);
    let slack_team = form.get("team_id").cloned().unwrap_or_default();
    let refuse = || (StatusCode::UNAUTHORIZED, "Not verified").into_response();
    let Some(team) = chat_team(&state, &format!("slack|{slack_team}")).await else {
        return refuse();
    };
    let Ok(s) = flag::settings(&kv(&state), &team).await else {
        return refuse();
    };
    let Some(secret) = open_secret(
        &state,
        &team,
        "plugin-slack",
        &s.slack_signing_secret_sealed,
    )
    .await
    else {
        return refuse();
    };
    let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");
    if !chat::verify_slack(
        &secret,
        h("x-slack-request-timestamp"),
        &body,
        h("x-slack-signature"),
        chrono::Utc::now().timestamp(),
    ) {
        warn!(team, "Slack plugin command failed verification");
        audit::record(
            &kv(&state),
            &team,
            audit::Event::new(
                "slack",
                audit::ActorKind::System,
                audit::action::CHAT_REFUSED,
                "refused",
            )
            .detail("signature"),
        )
        .await;
        return refuse();
    }
    if !flag::enabled(&kv(&state), &team).await {
        return Json(json!({ "response_type": "ephemeral", "text": "Plugins are not turned on for this team." })).into_response();
    }
    let who = format!(
        "slack:{}",
        form.get("user_name").cloned().unwrap_or_default()
    );
    let reply = start_chat_brief(
        &state,
        &team,
        form.get("text").map(String::as_str).unwrap_or(""),
        &who,
    )
    .await;
    Json(json!({ "response_type": "ephemeral", "text": reply })).into_response()
}

/// POST /webhooks/teams/plugins/:token — a Teams outgoing webhook message,
/// verified with its HMAC secret.
pub async fn teams_message(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let refuse = || (StatusCode::UNAUTHORIZED, "Not verified").into_response();
    let Some(team) = chat_team(&state, &format!("teams|{token}")).await else {
        return refuse();
    };
    let Ok(s) = flag::settings(&kv(&state), &team).await else {
        return refuse();
    };
    // The token named the team; it must still match the team's current one
    // (it changes when the webhook is set up again), compared in constant time.
    if s.teams_token.is_empty()
        || !bool::from(subtle::ConstantTimeEq::ct_eq(
            s.teams_token.as_bytes(),
            token.as_bytes(),
        ))
    {
        return refuse();
    }
    let Some(secret) = open_secret(&state, &team, "plugin-teams", &s.teams_secret_sealed).await
    else {
        return refuse();
    };
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !chat::verify_teams(&secret, &body, auth) {
        warn!(team, "Teams plugin message failed verification");
        audit::record(
            &kv(&state),
            &team,
            audit::Event::new(
                "teams",
                audit::ActorKind::System,
                audit::action::CHAT_REFUSED,
                "refused",
            )
            .detail("signature"),
        )
        .await;
        return refuse();
    }
    let msg: Value = serde_json::from_slice(&body).unwrap_or_default();
    let who = format!("teams:{}", msg["from"]["name"].as_str().unwrap_or(""));
    let reply = start_chat_brief(&state, &team, msg["text"].as_str().unwrap_or(""), &who).await;
    Json(json!({ "type": "message", "text": reply })).into_response()
}

/// The plugin sources a push to `owner/name` on the team's installation
/// updates: each is synced (the webhook path; public sources are polled).
pub async fn on_push(state: &AppState, team: &str, installation_id: u64, repo: &str) {
    let kv = kv(state);
    if !flag::enabled(&kv, team).await {
        return;
    }
    let Ok(sources) = source::list(&kv, team).await else {
        return;
    };
    for s in sources.iter().filter(|s| s.repo.eq_ignore_ascii_case(repo)) {
        if s.access == source::Access::Installation && s.installation_id != installation_id {
            continue;
        }
        let body = json!({ "type": "plugin_sync", "team_id": team, "source_id": s.id, "reason": "webhook" });
        if let Err(e) = send(state, body).await {
            warn!(team, source = %s.id, error = ?e.1 .0, "Couldn't queue a plugin source sync");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;

    fn rec(email: &str, role: &str, status: &str) -> Item {
        let mut i = Item::new();
        i.insert("email".into(), AttributeValue::S(email.into()));
        if !role.is_empty() {
            i.insert("role".into(), AttributeValue::S(role.into()));
        }
        if !status.is_empty() {
            i.insert("status".into(), AttributeValue::S(status.into()));
        }
        i
    }

    /// An email link counts only for someone on the team now: an invite or a
    /// removed member is no one.
    #[test]
    fn an_email_approval_needs_someone_on_the_team_now() {
        let items = vec![
            rec("admin@a.io", "admin", ""),
            rec("plain@a.io", "", "active"),
            rec("invited@a.io", "member", "invited"),
        ];
        assert_eq!(
            current_role(&items, "ADMIN@a.io ").as_deref(),
            Some("admin")
        );
        assert_eq!(
            current_role(&items, "plain@a.io").as_deref(),
            Some("member")
        );
        assert_eq!(current_role(&items, "invited@a.io"), None);
        assert_eq!(current_role(&items, "left@a.io"), None);
    }

    /// The link checks the person and their role before anything changes, and
    /// a removal takes them off the approver lists.
    #[test]
    fn the_email_link_checks_the_team_before_deciding() {
        let me = include_str!("plugin_pkgs.rs");
        let start = me.find("pub async fn email_approve(").unwrap();
        let f = &me[start..start + me[start..].find("\n}\n").unwrap()];
        let check = f.find("email_actor(").unwrap();
        assert!(check < f.find("drafts::reject(").unwrap());
        assert!(check < f.find("drafts::approve(").unwrap());
        assert!(me.contains("holds(CAP_APPROVE)"));
        assert!(crate::permissions::CAPABILITIES
            .iter()
            .any(|c| c.key == CAP_APPROVE && c.handlers.contains(&"plugin_pkgs::approve_draft")));
        let users = include_str!("users.rs");
        let start = users.find("pub async fn remove_user(").unwrap();
        let f = &users[start..start + users[start..].find("\n}\n").unwrap()];
        assert!(f.find(".delete_item()").unwrap() < f.find("plugin_pkgs::member_removed").unwrap());
    }
}
