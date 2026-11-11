//! "Use in Bedrock": the dashboard's routes for using a team's knowledge
//! from Amazon Bedrock, both ways.
//!
//! - **Live, over MCP**: the knowledge keys AgentCore Gateway (or any
//!   program) signs in to `/mcp` with (`GET/POST /api/knowledge/keys`,
//!   `DELETE /api/knowledge/keys/:id`; rules in `crate::knowledge_keys`).
//! - **Copied into a Bedrock knowledge base**: the sync's settings and
//!   status (`GET/PUT /api/knowledge/bedrock-sync`, `POST
//!   /api/knowledge/bedrock-sync/run`; rules in
//!   `common::knowledge::bedrock_sync`).
//!
//! Only people who manage knowledge bases use these (`knowledge_manage`).
//! Making or revoking a key and changing the sync are written to the
//! team's audit log first.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::knowledge::bedrock_sync::{self as bs, Config, Target};
use common::knowledge::search::Store;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::knowledge_keys::{self as keys, KeyStore, StoredKey};
use crate::models::Claims;
use crate::routes::roles::{self, AuditEntry};
use crate::AppState;

type ApiError = (StatusCode, Json<Value>);

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

/// People who manage knowledge bases (admins, and custom roles holding
/// `knowledge_manage`, which run as admin here).
fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|s| fail(s, "Only admins set up Bedrock."))
}

fn signed_in(claims: &Claims) -> Result<(), ApiError> {
    if claims.support_view.is_some() || claims.api_key.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "This is changed from a signed-in session.",
        ));
    }
    Ok(())
}

fn now_str(now: chrono::DateTime<chrono::Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// How a program reaches `/mcp`: its address and the OAuth endpoints
/// AgentCore Gateway's credential provider needs.
fn connect_info() -> Value {
    let issuer = crate::mcp_oauth::issuer();
    json!({
        "mcp_url": crate::mcp_oauth::resource(),
        "scope": keys::KNOWLEDGE_SCOPE,
        "token_endpoint": format!("{issuer}/oauth/token"),
        "authorization_endpoint": format!("{issuer}/oauth/authorize"),
        "issuer": issuer,
        "discovery_url": format!("{issuer}/.well-known/oauth-authorization-server"),
    })
}

/// The bases a key may be given by `claims`: Chat's for them, not read with
/// each person's own sign-in.
async fn giveable(
    state: &AppState,
    claims: &Claims,
) -> Result<Vec<common::knowledge::Base>, ApiError> {
    let holder = roles::holder_for(state, claims).await;
    let bases = super::knowledge::store(state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let mut out =
        super::knowledge::chat_readable(bases, &holder, &super::knowledge::person(claims));
    out.retain(|b| !b.per_person());
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

pub fn key_view(k: &StoredKey, now: chrono::DateTime<chrono::Utc>) -> Value {
    let status = if k.revoked_at.is_some() {
        "revoked"
    } else if k.is_expired(now) {
        "expired"
    } else {
        "active"
    };
    json!({
        "id": k.id,
        "name": k.name,
        "hint": k.hint,
        "client_id": keys::client_id(&k.id),
        "bases": k.bases,
        "created_at": k.created_at,
        "created_by": k.created_by_email,
        "expires_at": k.expires_at,
        "last_used_at": k.last_used_at,
        "revoked_at": k.revoked_at,
        "revoked_by": k.revoked_by,
        "status": status,
    })
}

// ── Knowledge keys ──────────────────────────────────────────────────

/// GET /api/knowledge/keys
pub async fn list_keys(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let now = chrono::Utc::now();
    let all = keys::store(&state)
        .list(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge keys", e))?;
    let bases = giveable(&state, &claims).await?;
    Ok(Json(json!({
        "keys": all.iter().map(|k| key_view(k, now)).collect::<Vec<_>>(),
        "bases": bases.iter().map(|b| json!({ "id": b.id, "name": b.name, "named_readers": b.named_readers() })).collect::<Vec<_>>(),
        "connect": connect_info(),
        "limits": { "max_keys": keys::MAX_KEYS_PER_TEAM, "max_expiry_days": keys::MAX_EXPIRY_DAYS },
    })))
}

#[derive(Debug, Deserialize)]
pub struct NewKeyBody {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub bases: Vec<String>,
    #[serde(default)]
    pub expires_in_days: Option<i64>,
}

/// POST /api/knowledge/keys — the key is in the answer once, never again.
pub async fn create_key(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<NewKeyBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    signed_in(&claims)?;
    let now = chrono::Utc::now();
    let store = keys::store(&state);
    let live = store
        .list(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge keys", e))?
        .iter()
        .filter(|k| k.is_live(now))
        .count();
    let readable: Vec<String> = giveable(&state, &claims)
        .await?
        .into_iter()
        .map(|b| b.id)
        .collect();
    let (name, bases, days) = keys::check_new(
        &body.name,
        &body.bases,
        &readable,
        body.expires_in_days,
        live,
    )
    .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let n = keys::generate();
    let key = StoredKey {
        id: n.id.clone(),
        team_id: claims.team_id.clone(),
        name: name.clone(),
        hash: n.hash,
        bases: bases.clone(),
        created_by: claims.sub.clone(),
        created_by_email: claims.email.clone(),
        created_at: now_str(now),
        expires_at: days.map(|d| now_str(now + chrono::Duration::days(d))),
        revoked_at: None,
        revoked_by: None,
        last_used_at: None,
        hint: keys::hint(&n.token),
    };
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "knowledge_key_created",
            role_id: &key.id,
            role_name: &name,
            detail: format!(
                "Reads {} knowledge base(s) over MCP. {}",
                bases.len(),
                match &key.expires_at {
                    Some(at) => format!("Expires {}.", &at[..10]),
                    None => "Doesn't expire.".into(),
                }
            ),
        },
    )
    .await?;
    store
        .create(&key)
        .await
        .map_err(|e| internal("knowledge key save", e))?;
    info!(team_id = %claims.team_id, key_id = %key.id, "Knowledge key created");
    Ok(Json(json!({
        "key": key_view(&key, now),
        "token": n.token,
        "connect": connect_info(),
    })))
}

/// DELETE /api/knowledge/keys/:key_id — it stops at once, its access tokens
/// with it.
pub async fn revoke_key(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    signed_in(&claims)?;
    if key_id.len() != 26 || !key_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(fail(StatusCode::NOT_FOUND, "No such key."));
    }
    let store = keys::store(&state);
    let Some(k) = store
        .get(&key_id)
        .await
        .map_err(|e| internal("knowledge key", e))?
        .filter(|k| k.team_id == claims.team_id)
    else {
        return Err(fail(StatusCode::NOT_FOUND, "No such key."));
    };
    if k.revoked_at.is_some() {
        return Err(fail(StatusCode::CONFLICT, "This key is already revoked."));
    }
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "knowledge_key_revoked",
            role_id: &k.id,
            role_name: &k.name,
            detail: format!("Key {} revoked.", k.hint),
        },
    )
    .await?;
    let at = now_str(chrono::Utc::now());
    store
        .revoke(&claims.team_id, &key_id, &claims.email, &at)
        .await
        .map_err(|e| internal("knowledge key revoke", e))?;
    Ok(Json(json!({ "status": "revoked", "id": key_id })))
}

// ── Copying into a Bedrock knowledge base ───────────────────────────

fn self_hosted() -> bool {
    common::live::aws::install_read_role().is_some()
}

/// The settings as the page shows them: never the lease.
fn config_view(cfg: &Config) -> Value {
    let mut status = serde_json::to_value(&cfg.status).unwrap_or_default();
    if let Some(o) = status.as_object_mut() {
        o.remove("lease_until");
        let syncing = cfg.status.lease_until > chrono::Utc::now().timestamp();
        o.insert("syncing".into(), Value::Bool(syncing));
    }
    json!({
        "enabled": cfg.enabled,
        "target": cfg.target,
        "bases": cfg.bases,
        "acknowledged_at": cfg.acknowledged_at,
        "acknowledged_by": cfg.acknowledged_by,
        "updated_at": cfg.updated_at,
        "updated_by": cfg.updated_by,
        "status": status,
    })
}

/// Asks the worker to sync the team now.
async fn start(state: &AppState, team: &str) -> Result<(), String> {
    let name = &state.config.worker_function_name;
    if name.is_empty() {
        return Err("no worker configured".into());
    }
    let payload = json!({ "bedrock_sync": { "team_id": team } });
    state
        .lambda
        .invoke()
        .function_name(name)
        .invocation_type(aws_sdk_lambda::types::InvocationType::Event)
        .payload(aws_sdk_lambda::primitives::Blob::new(
            serde_json::to_vec(&payload).map_err(|e| e.to_string())?,
        ))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("worker invoke: {e}"))
}

/// The one-click role update for a custom data source in a connected
/// account (hosted, or a self-hosted install writing another account).
async fn role_update(state: &AppState, team: &str, cfg: &Config) -> Option<Value> {
    let Target::Custom {
        knowledge_base_arn, ..
    } = &cfg.target
    else {
        return None;
    };
    let arn = common::knowledge::remote::parse_kb_arn(knowledge_base_arn).ok()?;
    let own = common::live::aws::install_read_role();
    if own.as_deref().and_then(|r| r.split(':').nth(4)) == Some(arn.account.as_str()) {
        return None;
    }
    let bases = super::knowledge::store(state)
        .bases(team)
        .await
        .unwrap_or_default();
    super::knowledge::grant_for(state, team, &bases, &arn.account, &arn.region, None, None).await
}

/// GET /api/knowledge/bedrock-sync
pub async fn get_sync(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let table = &state.config.settings_table_name;
    let cfg = bs::load(&state.dynamo, table, &claims.team_id)
        .await
        .map_err(|e| internal("bedrock sync", e))?
        .unwrap_or_default();
    let bases = super::knowledge::store(&state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let mut listed: Vec<Value> = bases
        .iter()
        .map(|b| {
            json!({
                "id": b.id,
                "name": b.name,
                "can_copy": bs::can_copy(b),
                "named_readers": b.named_readers(),
                "read_role": b.read_role,
            })
        })
        .collect();
    listed.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(Json(json!({
        "sync": config_view(&cfg),
        "bases": listed,
        "self_hosted": self_hosted(),
        "warning": bs::ACCESS_WARNING,
        "role_update": role_update(&state, &claims.team_id, &cfg).await,
        "setup": setup_view(&state, &claims.team_id).await,
        "one_click": match launch_facts() {
            Ok(f) => json!({ "available": true, "region": f.region, "account": f.account }),
            Err(m) => json!({ "available": false, "reason": m }),
        },
    })))
}

#[derive(Debug, Deserialize)]
pub struct SyncBody {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub target: Target,
    #[serde(default)]
    pub bases: Vec<String>,
    /// The admin read the access warning and agrees.
    #[serde(default)]
    pub acknowledged: bool,
}

/// What a save may hold: checked target, known local bases, and the
/// warning confirmed before anything is copied.
pub fn check_sync(
    body: &SyncBody,
    team_bases: &[common::knowledge::Base],
    self_hosted: bool,
) -> Result<(Target, Vec<String>), String> {
    if !body.enabled {
        // Turning it off keeps the target, so the next sync can remove
        // every copy.
        return Ok((body.target.clone(), Vec::new()));
    }
    if !body.acknowledged {
        return Err(format!("Confirm the warning first: {}", bs::ACCESS_WARNING));
    }
    let target = bs::check_target(&body.target, self_hosted)?;
    Ok((target, check_bases(&body.bases, team_bases)?))
}

/// The bases chosen to copy: the team's own, kept here, each once, at
/// least one.
pub fn check_bases(
    chosen: &[String],
    team_bases: &[common::knowledge::Base],
) -> Result<Vec<String>, String> {
    let mut bases = Vec::new();
    for id in chosen {
        let Some(b) = team_bases.iter().find(|b| &b.id == id) else {
            return Err("Pick knowledge bases of this team.".into());
        };
        if !bs::can_copy(b) {
            return Err(format!(
                "{} is connected in place, so it can't be copied.",
                b.name
            ));
        }
        if !bases.contains(id) {
            bases.push(id.clone());
        }
    }
    if bases.is_empty() {
        return Err("Pick at least one knowledge base to copy.".into());
    }
    Ok(bases)
}

/// The team's name for each document's metadata.
async fn team_name(state: &AppState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".into()),
        )
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| {
            ["name", "github_org"]
                .iter()
                .find_map(|k| i.get(*k).and_then(|v| v.as_s().ok()).cloned())
        })
        .unwrap_or_default()
}

/// PUT /api/knowledge/bedrock-sync — saves, checks the data source, and
/// queues a sync (which also removes what is no longer chosen).
pub async fn save_sync(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SyncBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    signed_in(&claims)?;
    let table = &state.config.settings_table_name;
    let team_bases = super::knowledge::store(&state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let (target, bases) = check_sync(&body, &team_bases, self_hosted())
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let before = bs::load(&state.dynamo, table, &claims.team_id)
        .await
        .map_err(|e| internal("bedrock sync", e))?
        .unwrap_or_default();
    let target = if !body.enabled && target == Target::None {
        before.target.clone()
    } else {
        target
    };
    // A target that moved: the copies in the old one are removed first.
    if before.enabled && before.status.documents > 0 && before.target != target && body.enabled {
        return Err(fail(
            StatusCode::CONFLICT,
            "Turn syncing off and wait for its copies to be removed before you point it somewhere else.",
        ));
    }
    let mut check_note = None;
    if body.enabled {
        // The target is reachable and the data source is the right kind.
        match bs::session_for(
            &state.sts,
            &state.dynamo,
            &state.config.aws_insights_table_name,
            &claims.team_id,
            &target,
        )
        .await
        {
            Ok(session) => {
                if let Some(arn) = target.knowledge_base_arn() {
                    let region = common::knowledge::remote::parse_kb_arn(arn)
                        .map(|a| a.region)
                        .unwrap_or_default();
                    if let Err(e) =
                        bs::check_data_source(&bs::agent_client(&session, &region), &target).await
                    {
                        check_note = Some(e);
                    }
                }
            }
            Err(e) => check_note = Some(e),
        }
    }
    let now = now_str(chrono::Utc::now());
    let mut cfg = before.clone();
    cfg.enabled = body.enabled;
    cfg.target = target;
    cfg.bases = bases.clone();
    cfg.updated_at = now.clone();
    cfg.updated_by = claims.email.clone();
    cfg.team_name = team_name(&state, &claims.team_id).await;
    if body.enabled {
        cfg.acknowledged_at = now.clone();
        cfg.acknowledged_by = claims.email.clone();
    }
    let names: Vec<String> = team_bases
        .iter()
        .filter(|b| bases.contains(&b.id))
        .map(|b| b.name.clone())
        .collect();
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "bedrock_sync_changed",
            role_id: "bedrock-sync",
            role_name: "Copy to Bedrock",
            detail: if body.enabled {
                format!(
                    "Copies {} into a Bedrock knowledge base ({}). Confirmed that everyone with Bedrock access to it reads them.",
                    common::truncate_str(&names.join(", "), 400),
                    cfg.target.kind()
                )
            } else {
                "Turned copying to Bedrock off; its copies are removed.".into()
            },
        },
    )
    .await?;
    bs::save(&state.dynamo, table, &claims.team_id, &cfg)
        .await
        .map_err(|e| internal("bedrock sync save", e))?;
    if let Err(e) = start(&state, &claims.team_id).await {
        warn!(error = %e, "Bedrock sync not started; the sweep runs it");
    }
    Ok(Json(json!({
        "sync": config_view(&cfg),
        "check": check_note,
        "role_update": role_update(&state, &claims.team_id, &cfg).await,
    })))
}

/// GET /api/knowledge/bedrock-sync/template — the S3 folder's stack, for a
/// self-hosted install's AWS admin.
pub async fn export_template(
    Extension(claims): Extension<Claims>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    admin(&claims)?;
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/x-yaml"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"coderhelm-bedrock-export.yaml\"",
            ),
        ],
        bs::EXPORT_TEMPLATE,
    )
        .into_response())
}

/// POST /api/knowledge/bedrock-sync/run — "Sync now".
pub async fn run_sync(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    signed_in(&claims)?;
    let cfg = bs::load(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
    )
    .await
    .map_err(|e| internal("bedrock sync", e))?;
    if cfg.is_none() {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "Set up copying to Bedrock first.",
        ));
    }
    start(&state, &claims.team_id)
        .await
        .map_err(|e| internal("bedrock sync start", e))?;
    Ok(Json(json!({ "status": "queued" })))
}

// ── "Set up in AWS" (crate::bedrock_setup) ──────────────────────────

/// What a setup link needs from the install, or why it can't make one.
pub struct LaunchFacts {
    pub template_url: String,
    pub region: String,
    pub prefix: String,
    pub account: String,
    pub report_url: String,
}

/// The install's facts for a setup link, or what is missing in plain words.
pub fn launch_facts() -> Result<LaunchFacts, String> {
    let own = common::live::aws::install_read_role()
        .ok_or("Setting up in AWS with one button is for self-hosted installs.")?;
    let account = own.split(':').nth(4).unwrap_or("").to_string();
    let prefix = std::env::var("RESOURCE_PREFIX")
        .unwrap_or_default()
        .trim()
        .to_string();
    let region = std::env::var("AWS_REGION").unwrap_or_default();
    let template_url =
        crate::bedrock_setup::template_url(&common::public_urls::log_reader_template_url())
            .ok_or("This install's release doesn't publish the export stack's template.")?;
    let api = common::public_urls::api_url_from_env().trim_end_matches('/');
    if account.len() != 12 || prefix.is_empty() || region.is_empty() || !api.starts_with("https://")
    {
        return Err(
            "This install doesn't know its own account, prefix, region or address yet.".into(),
        );
    }
    Ok(LaunchFacts {
        template_url,
        region,
        prefix,
        account,
        report_url: format!("{api}{}", crate::bedrock_setup::REPORT_PATH),
    })
}

/// A knowledge base's service role's name (`bedrock:GetKnowledgeBase` with
/// the install's own role), or `None` when it can't be read.
async fn kb_role_name(region: &str, id: &str) -> Option<String> {
    let s = common::live::aws::own_session(region)?;
    let kb = bs::agent_client(&s, region)
        .get_knowledge_base()
        .knowledge_base_id(id)
        .send()
        .await
        .ok()?;
    let arn = kb.knowledge_base()?.role_arn().to_string();
    arn.rsplit('/')
        .next()
        .map(str::to_string)
        .filter(|r| !r.is_empty())
}

/// The team's current setup, as the page shows it.
async fn setup_view(state: &AppState, team: &str) -> Value {
    use crate::bedrock_setup::SetupStore;
    let st = crate::bedrock_setup::store(state);
    let now = chrono::Utc::now().timestamp();
    match st.current(team).await {
        Ok(h) if !h.is_empty() => match st.by_hash(&h).await {
            Ok(Some(s)) if s.team_id == team => crate::bedrock_setup::view(&s, now),
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

#[derive(Debug, Deserialize)]
pub struct SetupBody {
    #[serde(default)]
    pub bases: Vec<String>,
    #[serde(default)]
    pub acknowledged: bool,
    /// Make a new knowledge base (the default), or only the bucket.
    #[serde(default = "yes")]
    pub create_knowledge_base: bool,
    /// An existing knowledge base to add the data source to instead.
    #[serde(default)]
    pub knowledge_base_id: String,
    #[serde(default)]
    pub knowledge_base_role_name: String,
}

fn yes() -> bool {
    true
}

/// An existing knowledge base's id and role name, checked.
pub fn check_existing(id: &str, role: &str) -> Result<Option<(String, String)>, String> {
    let id = id.trim();
    let role = role.trim();
    if id.is_empty() {
        return Ok(None);
    }
    if id.len() != 10
        || !id
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        return Err("Pick a knowledge base from the list.".into());
    }
    if role.len() > 64
        || !role
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+=,.@_-".contains(c))
    {
        return Err("That isn't an IAM role name.".into());
    }
    Ok(Some((id.to_string(), role.to_string())))
}

/// POST /api/knowledge/bedrock-sync/setup — "Set up in AWS": a one-time
/// link that opens the export stack with everything filled in. Copying
/// starts when the stack reports it is ready.
pub async fn start_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SetupBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    signed_in(&claims)?;
    if !body.acknowledged {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("Confirm the warning first: {}", bs::ACCESS_WARNING),
        ));
    }
    let team_bases = super::knowledge::store(&state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let bases =
        check_bases(&body.bases, &team_bases).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let existing = check_existing(&body.knowledge_base_id, &body.knowledge_base_role_name)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let facts = launch_facts().map_err(|m| {
        fail(
            StatusCode::CONFLICT,
            &format!("{m} Enter the stack's outputs yourself instead."),
        )
    })?;
    // An existing knowledge base's service role, read with the install's
    // own role when the admin didn't name it, so the stack can let it read
    // the folder.
    let existing = match existing {
        Some((id, role)) if role.is_empty() => {
            let role = kb_role_name(&facts.region, &id).await.unwrap_or_default();
            Some((id, role))
        }
        e => e,
    };
    let cfg = bs::load(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
    )
    .await
    .map_err(|e| internal("bedrock sync", e))?
    .unwrap_or_default();
    if cfg.enabled && cfg.status.documents > 0 {
        return Err(fail(
            StatusCode::CONFLICT,
            "Copying is on. Turn it off and wait for its copies to be removed before you set up a new knowledge base.",
        ));
    }
    let (token, hash) = crate::bedrock_setup::generate();
    let now = chrono::Utc::now();
    let creates = existing.is_none() && body.create_knowledge_base;
    let setup = crate::bedrock_setup::Setup {
        team_id: claims.team_id.clone(),
        token_hash: hash,
        created_at: now_str(now),
        created_by: claims.email.clone(),
        expires_at: now.timestamp() + crate::bedrock_setup::TOKEN_TTL_SECS,
        stack_name: format!("{}{}", facts.prefix, crate::bedrock_setup::STACK_SUFFIX),
        region: facts.region.clone(),
        bases: bases.clone(),
        acknowledged_by: claims.email.clone(),
        creates_knowledge_base: creates,
        knowledge_base_id: existing.as_ref().map(|e| e.0.clone()).unwrap_or_default(),
        state: crate::bedrock_setup::WAITING.into(),
        updated_at: now_str(now),
        ..Default::default()
    };
    let names: Vec<String> = team_bases
        .iter()
        .filter(|b| bases.contains(&b.id))
        .map(|b| b.name.clone())
        .collect();
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "bedrock_sync_setup",
            role_id: "bedrock-sync",
            role_name: "Copy to Bedrock",
            detail: format!(
                "Started a setup in AWS ({}) to copy {} once the stack reports it is ready. Confirmed that everyone with Bedrock access to it reads them.",
                if creates { "a new knowledge base" } else if existing.is_some() { "an existing knowledge base" } else { "a bucket only" },
                common::truncate_str(&names.join(", "), 400),
            ),
        },
    )
    .await?;
    crate::bedrock_setup::start(&crate::bedrock_setup::store(&state), &setup)
        .await
        .map_err(|e| internal("bedrock setup", e))?;
    let url = crate::bedrock_setup::launch_url(
        &facts.template_url,
        &facts.region,
        &facts.prefix,
        &facts.account,
        &facts.report_url,
        &token,
        creates,
        existing.as_ref().map(|(k, r)| (k.as_str(), r.as_str())),
    );
    info!(team_id = %claims.team_id, "Bedrock setup link made");
    Ok(Json(json!({
        "setup": crate::bedrock_setup::view(&setup, now.timestamp()),
        "launch_url": url,
    })))
}

/// DELETE /api/knowledge/bedrock-sync/setup — the link stops working.
pub async fn cancel_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    use crate::bedrock_setup::SetupStore;
    admin(&claims)?;
    signed_in(&claims)?;
    let st = crate::bedrock_setup::store(&state);
    let h = st
        .current(&claims.team_id)
        .await
        .map_err(|e| internal("bedrock setup", e))?;
    if let Some(mut s) = st
        .by_hash(&h)
        .await
        .map_err(|e| internal("bedrock setup", e))?
    {
        if !s.done() {
            s.state = crate::bedrock_setup::REPLACED.into();
            s.version += 1;
            s.updated_at = now_str(chrono::Utc::now());
            let _ = st
                .save(&s)
                .await
                .map_err(|e| internal("bedrock setup", e))?;
        }
    }
    st.set_current(&claims.team_id, "")
        .await
        .map_err(|e| internal("bedrock setup", e))?;
    Ok(Json(json!({ "status": "cancelled" })))
}

/// POST /api/knowledge/bedrock-setup/report — the export stack's reporter,
/// with the one-time token (no session: the token is the proof). On
/// `ready`, copying starts with what the stack made.
pub async fn report_setup(
    State(state): State<Arc<AppState>>,
    Json(r): Json<crate::bedrock_setup::Report>,
) -> Result<Json<Value>, ApiError> {
    let own = common::live::aws::install_read_role().unwrap_or_default();
    let account = own.split(':').nth(4).unwrap_or("").to_string();
    let prefix = std::env::var("RESOURCE_PREFIX")
        .unwrap_or_default()
        .trim()
        .to_string();
    let now = chrono::Utc::now();
    let st = crate::bedrock_setup::store(&state);
    let (setup, target) = match crate::bedrock_setup::receive(
        &st,
        &r,
        &account,
        &prefix,
        now.timestamp(),
        &now_str(now),
    )
    .await
    .map_err(|e| internal("bedrock setup", e))?
    {
        Ok(x) => x,
        Err(refused) => {
            warn!(step = %common::truncate_str(&r.step, 20), "Bedrock setup report refused");
            return Err(fail(StatusCode::CONFLICT, &refused.message()));
        }
    };
    info!(team_id = %setup.team_id, state = %setup.state, "Bedrock setup report");
    if let Some(target) = target {
        let table = &state.config.settings_table_name;
        let mut cfg = bs::load(&state.dynamo, table, &setup.team_id)
            .await
            .map_err(|e| internal("bedrock sync", e))?
            .unwrap_or_default();
        let at = now_str(now);
        cfg.enabled = true;
        cfg.target = target;
        cfg.bases = setup.bases.clone();
        cfg.acknowledged_at = setup.created_at.clone();
        cfg.acknowledged_by = setup.acknowledged_by.clone();
        cfg.updated_at = at;
        cfg.updated_by = setup.created_by.clone();
        cfg.team_name = team_name(&state, &setup.team_id).await;
        bs::save(&state.dynamo, table, &setup.team_id, &cfg)
            .await
            .map_err(|e| internal("bedrock sync save", e))?;
        if let Err(e) = start(&state, &setup.team_id).await {
            warn!(error = %e, "Bedrock sync not started; the sweep runs it");
        }
    }
    Ok(Json(json!({ "status": setup.state })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(id: &str) -> common::knowledge::Base {
        common::knowledge::Base {
            id: id.into(),
            name: id.to_uppercase(),
            ..Default::default()
        }
    }

    #[test]
    fn a_sync_needs_the_warning_confirmed_and_only_the_teams_own_bases() {
        let kb = "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345";
        let target = Target::Custom {
            knowledge_base_arn: kb.into(),
            data_source_id: "DS12345678".into(),
        };
        let team = vec![
            base("a"),
            base("b"),
            common::knowledge::search::testing::remote_base("r", "Remote", "111122223333"),
        ];
        let body = |bases: &[&str], ack: bool| SyncBody {
            enabled: true,
            target: target.clone(),
            bases: bases.iter().map(|b| b.to_string()).collect(),
            acknowledged: ack,
        };
        let err = check_sync(&body(&["a"], false), &team, false).unwrap_err();
        assert!(err.contains("no per-person access"), "{err}");
        assert_eq!(
            check_sync(&body(&["a", "a"], true), &team, false)
                .unwrap()
                .1,
            vec!["a"]
        );
        assert!(check_sync(&body(&["zzz"], true), &team, false).is_err());
        assert!(check_sync(&body(&["r"], true), &team, false).is_err());
        assert!(check_sync(&body(&[], true), &team, false).is_err());
        // Off: nothing chosen, the target kept so the copies can be removed.
        let off = SyncBody {
            enabled: false,
            ..body(&["a"], false)
        };
        let (t, b) = check_sync(&off, &team, false).unwrap();
        assert_eq!((t, b.len()), (target, 0));
    }

    #[test]
    fn a_key_view_never_holds_its_hash() {
        let k = StoredKey {
            id: "01jbkey0000000000000000001".into(),
            team_id: "T".into(),
            name: "AgentCore".into(),
            hash: "deadbeef".into(),
            bases: vec!["a".into()],
            created_by: "U".into(),
            created_by_email: "a@x.com".into(),
            created_at: "2026-11-10T00:00:00Z".into(),
            expires_at: None,
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
            hint: "chkb_01jbke...abcd".into(),
        };
        let v = key_view(&k, chrono::Utc::now());
        assert!(!v.to_string().contains("deadbeef"));
        assert_eq!(v["client_id"], "chkbc_01jbkey0000000000000000001");
        assert_eq!(v["status"], "active");
    }
}
