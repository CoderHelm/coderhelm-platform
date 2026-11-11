//! Connected sources of a knowledge base: picking what to sync from Notion,
//! Confluence, Sanity, Google Drive, GitHub or a website, signing in with
//! one click, and keeping it in sync (`common::knowledge::sources`).
//!
//! Admins (holders of `knowledge_manage`) add, change, pause, sync and
//! remove sources. Who can read what a source brings in follows the base's
//! own read setting, exactly like its uploaded files: everyone who can read
//! the base can read every synced page, whatever their own access in the
//! source app. The page says so when a source is added.
//!
//! Sign-ins reuse the remote MCP OAuth flow and its callback page; tokens
//! are sealed per team by KMS and never reach the browser. Sanity also
//! syncs through a member's own Sanity sign-in on its remote MCP server
//! (the one Connected apps uses): the source records whose sign-in it reads
//! with, the page says so, and when that sign-in stops working any admin
//! can sign in again from the source. Syncs run in the
//! worker; this module only starts them (asynchronously) and shows their
//! state. Providers' change events arrive at `/webhooks/knowledge/<token>`.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use common::knowledge::sources::{
    self as ks, clean_selection, connect, hook_pk, oauth, sanity, sanity_mcp, shown_status, store,
    webhook, Connector, Kind, Selected, Source, Status, MAX_SOURCES_PER_BASE,
};
use common::knowledge::{self as kb, FileMeta};
use common::mcp_remote::store::Owner;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong. Try again.",
    )
}

fn forbidden(_: StatusCode) -> (StatusCode, Json<Value>) {
    fail(
        StatusCode::FORBIDDEN,
        "Only admins add and change knowledge sources.",
    )
}

fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Shown when a source is added: what it means for who can read it.
pub const VISIBILITY_NOTE: &str = "Everyone who can read this knowledge base can read what you add, even if they can't open it in the app it comes from.";

fn kind_of(raw: &str) -> Result<Kind, (StatusCode, Json<Value>)> {
    Kind::parse(raw).ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such kind of source."))
}

pub(crate) fn sync_store(state: &AppState) -> store::AwsSyncStore {
    store::AwsSyncStore {
        dynamo: state.dynamo.clone(),
        s3: state.s3.clone(),
        table: state.config.settings_table_name.clone(),
        bucket: state.config.bucket_name.clone(),
    }
}

/// The install's OAuth app for `kind`, from its secrets: the provider's own
/// two keys only ([`oauth::App::from_keys`]). Google Drive never borrows the
/// Google sign-in client; without `google_drive_client_id` and
/// `google_drive_client_secret` it is not set up.
pub(crate) fn app(state: &AppState, kind: Kind) -> oauth::App {
    app_from(&state.secrets, kind)
}

pub(crate) fn app_from(s: &crate::models::Secrets, kind: Kind) -> oauth::App {
    oauth::App::from_keys(kind, |key| s.oauth_key(key))
}

async fn github_token(state: &AppState, team: &str) -> Option<String> {
    let id = super::graph::installation_id(state, team).await?;
    crate::auth::github_app::get_installation_token(state, id)
        .await
        .map_err(|e| warn!(error = %e, "No GitHub token for knowledge sources"))
        .ok()
}

pub(crate) async fn env(state: &AppState, team: &str, kind: Option<Kind>) -> connect::Env {
    let github = if kind.is_none() || kind == Some(Kind::Github) {
        github_token(state, team).await
    } else {
        None
    };
    connect::Env {
        dynamo: state.dynamo.clone(),
        kms: state.kms.clone(),
        secrets_key_id: state.config.secrets_key_id.clone(),
        settings_table: state.config.settings_table_name.clone(),
        mcp_table: super::plugins::mcp_table(state).to_string(),
        team: team.to_string(),
        http: Arc::new(common::mcp_remote::http::GuardedHttps),
        github_token: github,
        aws: Some(connect::AwsEnv {
            sts: state.sts.clone(),
            accounts_table: state.config.aws_insights_table_name.clone(),
            s3: state.s3.clone(),
            bucket: state.config.bucket_name.clone(),
            actor: "knowledge".into(),
        }),
    }
}

/// Settings the page may see (no secrets are ever in options).
fn public_options(o: &Value) -> Value {
    let mut v = o.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("sanity_hook_id");
    }
    v
}

fn ts(secs: i64) -> String {
    if secs <= 0 {
        return String::new();
    }
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

/// A source as the page shows it.
pub fn source_json(s: &Source, now: i64) -> Value {
    let status = shown_status(s, now);
    json!({
        "id": s.id,
        "kind": s.kind_key(),
        "label": s.kind.map(Kind::label).unwrap_or(""),
        "name": s.name,
        "selection": s.selection,
        "options": public_options(&s.options),
        "paused": s.paused,
        "status": status,
        "error": if status == Status::Error { serde_json::to_value(&s.error).unwrap_or(Value::Null) } else { Value::Null },
        "progress": if status == Status::Syncing { serde_json::to_value(&s.progress).unwrap_or(Value::Null) } else { Value::Null },
        "items": s.items,
        "failed_items": s.failed_items,
        "skipped_items": s.skipped_items,
        "truncated": s.truncated,
        "last_sync_at": s.last_sync_at,
        "last_success_at": s.last_success_at,
        "next_sync_at": if s.paused { String::new() } else { ts(s.next_sync_at) },
        "live_updates": s.webhook,
        "created_at": s.created_at,
        "created_by": s.created_by,
        "connection": if s.signin.is_some() { "sign_in" } else { "" },
        "via": via_of(s),
        "via_known": s.used.is_some() || !via_of(s).is_empty(),
        "account": s.used.as_ref().map(|u| u.account.clone()).unwrap_or_default(),
        "connected_used_at": s.used.as_ref().map(|u| u.used_at.clone()).unwrap_or_default(),
        "use_saved_key": s.use_saved_key,
        "webhook_seen_at": s.webhook_seen_at,
        "interval_mins": s.interval_mins(),
        "signin": s.signin.as_ref().map(|si| json!({
            "name": si.who(),
            "email": si.email,
            "account": si.account,
        })),
    })
}

/// How a source reaches its provider: what its last sync used, else what
/// its kind can only use (empty when only a sync can tell).
pub fn via_of(s: &Source) -> &str {
    if let Some(u) = &s.used {
        return &u.via;
    }
    if s.signin.is_some() {
        return "sign_in";
    }
    match s.kind {
        Some(Kind::Web) => "public",
        Some(Kind::Archive) => "upload",
        Some(Kind::S3) => "aws_role",
        Some(Kind::Github) => "github_app",
        Some(Kind::Sanity) => "saved_key",
        _ if s.use_saved_key => "saved_key",
        _ => "",
    }
}

/// The ways a kind can be switched between for one source ("Change
/// connection"): the team's sign-in and its saved API key.
pub fn switchable(kind: Kind) -> bool {
    matches!(kind, Kind::Notion | Kind::Confluence)
}

/// What happens to a source when the person who connected it leaves the
/// team, in one sentence.
pub fn if_they_leave(via: &str, who: &str, label: &str) -> String {
    let who = if who.trim().is_empty() {
        "the person who connected it"
    } else {
        who.trim()
    };
    match via {
        "oauth" => format!("It belongs to the team, so it keeps working if {who} leaves. Reconnect only if {label} stops accepting it."),
        "sign_in" => format!("It uses {who}'s own {label} sign-in. If {who} leaves or the sign-in stops working, syncing stops (pages already here stay searchable), admins are told, and any admin can sign in again to take it over."),
        "saved_key" => format!("It uses the API token saved for the team, so it keeps working if {who} leaves, until someone revokes that token in {label}."),
        "github_app" => "It uses the Coderhelm GitHub App, installed for the organization, so it doesn't depend on any one person.".to_string(),
        "aws_role" => "It uses the team's read-only role in that AWS account, so it doesn't depend on any one person.".to_string(),
        "public" => "It reads public pages, so no one's access is involved.".to_string(),
        "upload" => "It reads an export someone uploaded; nothing is connected, so nothing changes when they leave.".to_string(),
        _ => "It shows here after its next sync.".to_string(),
    }
}

/// GET /api/knowledge/:kb_id/sources/:source_id/connection — how a source
/// connects, for the people who manage it: whose connection, when it was
/// made, its scopes and whether it still works, and what it can switch to.
/// Never a secret.
pub async fn source_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    let kind = src
        .kind
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "This source has no kind."))?;
    let env = env(&state, &claims.team_id, Some(kind)).await;
    let rt = env.runtime();
    let via = via_of(&src).to_string();
    let used = src.used.clone().unwrap_or_default();
    // The sign-in record a sync uses: the team's, or the member's own.
    let token = match (via.as_str(), &src.signin) {
        ("sign_in", Some(si)) => match Owner::parse(&si.owner) {
            Some(o) => rt
                .store
                .get_token(&claims.team_id, &si.server_id, &o)
                .await
                .ok()
                .flatten(),
            None => None,
        },
        ("oauth", _) => rt
            .store
            .get_token(&claims.team_id, &oauth::connection_id(kind), &Owner::Team)
            .await
            .ok()
            .flatten(),
        _ => None,
    };
    let by = match (&src.signin, &token) {
        (Some(si), _) => si.email.clone(),
        (None, Some(t)) if !t.connected_by.is_empty() => t.connected_by.clone(),
        _ => used.by.clone(),
    };
    let who = src
        .signin
        .as_ref()
        .map(|si| si.who().to_string())
        .unwrap_or_else(|| by.clone());
    let team = connect::status(
        &env,
        kind,
        app(&state, kind).is_set()
            || super::hybrid_install::brokered(&state, kind)
                .await
                .is_some(),
        &claims.email,
    )
    .await;
    let (oauth_ok, _, _) = env.oauth_state(kind).await;
    let key_ok = match kind {
        Kind::Notion => env
            .plugin_credentials("notion")
            .await
            .and_then(|c| c.get("api_key").cloned())
            .is_some_and(|k| !k.trim().is_empty()),
        Kind::Confluence => env.confluence_token().await.is_some(),
        _ => false,
    };
    Ok(Json(json!({
        "via": via,
        "owner": if src.signin.is_some() { "member" } else if via.is_empty() { "" } else { "team" },
        "by": by,
        "account": if used.account.is_empty() { src.signin.as_ref().map(|s| s.account.clone()).unwrap_or_default() } else { used.account.clone() },
        "connected_at": token.as_ref().map(|t| t.connected_at.clone()).filter(|t| !t.is_empty()).unwrap_or(used.connected_at.clone()),
        "used_at": used.used_at,
        "token": token.as_ref().map(|t| json!({
            "status": match t.status {
                common::mcp_remote::store::Status::Connected => "working",
                _ => "stopped",
            },
            "status_reason": t.status_reason,
            "expires_at": t.expires_at.map(ts),
            "scopes": t.scope.split_whitespace().collect::<Vec<_>>(),
        })),
        "if_they_leave": if_they_leave(&via, &who, kind.label()),
        "team": {
            "connected": team.connected,
            "via": team.via,
            "connected_by": team.connected_by,
            "reconnect_reason": team.reconnect_reason,
            "can_sign_in": team.can_sign_in,
            "has_saved_key": team.has_saved_key,
        },
        "switch": {
            "oauth": switchable(kind) && oauth_ok && via != "oauth",
            "saved_key": switchable(kind) && key_ok && via != "saved_key",
            "my_sign_in": src.signin.is_some(),
        },
    })))
}

/// The sources of a base.
pub(crate) async fn sources_of(state: &AppState, team: &str, kb_id: &str) -> Vec<Source> {
    let mut v = store::base_sources(
        &state.dynamo,
        &state.config.settings_table_name,
        team,
        kb_id,
    )
    .await
    .unwrap_or_default();
    v.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    v
}

/// Asks the worker to sync a source now (asynchronously).
pub(crate) async fn start_sync(
    state: &AppState,
    team: &str,
    kb_id: &str,
    id: &str,
    trigger: &str,
) -> Result<(), String> {
    let name = &state.config.worker_function_name;
    if name.is_empty() {
        return Err("no worker configured".into());
    }
    let payload = json!({ "knowledge_sync": { "team_id": team, "kb_id": kb_id, "source_id": id, "trigger": trigger } });
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

async fn base_exists(
    state: &AppState,
    team: &str,
    kb_id: &str,
) -> Result<bool, (StatusCode, Json<Value>)> {
    use common::knowledge::search::Store;
    Ok(super::knowledge::store(state)
        .base(team, kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .is_some())
}

// ── Connected apps ──────────────────────────────────────────────────────────

/// GET /api/knowledge/connectors — every kind of source with whether the
/// team can use it now and how it would connect.
pub async fn connectors(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let env = env(&state, &claims.team_id, None).await;
    let mut out = Vec::new();
    for kind in Kind::ALL {
        let st = connect::status(
            &env,
            kind,
            app(&state, kind).is_set()
                || super::hybrid_install::brokered(&state, kind)
                    .await
                    .is_some(),
            &claims.email,
        )
        .await;
        // "Connected as": the provider account of this person's sign-in.
        let account = if st.signed_in {
            match connect::find_signin(&env.runtime(), kind, &claims.email).await {
                Ok(Some(si)) if kind == Kind::Sanity => {
                    connect::sanity_signin(env.runtime(), si.server, si.owner)
                        .account()
                        .await
                }
                _ => String::new(),
            }
        } else {
            String::new()
        };
        out.push(json!({
            "kind": kind.key(),
            "label": kind.label(),
            "blurb": kind.blurb(),
            "connected": st.connected,
            "via": st.via,
            "can_sign_in": st.can_sign_in,
            "reconnect_reason": st.reconnect_reason,
            "connected_by": st.connected_by,
            "hint": st.hint,
            "member_sign_in": st.member_sign_in,
            "signed_in": st.signed_in,
            "signed_in_as": if account.is_empty() { claims.email.clone() } else { account },
            "has_saved_key": st.has_saved_key,
            "token_path": st.token_path,
            "live_updates": kind.has_webhooks(),
            "picker": if kind == Kind::Web { "address" } else { "tree" },
        }));
    }
    Ok(Json(
        json!({ "connectors": out, "visibility_note": VISIBILITY_NOTE }),
    ))
}

/// The `return_to` a sign-in start may send (`{"return_to": "/knowledge/..."}`):
/// a Knowledge page only, else nothing (the callback goes to Knowledge).
pub fn return_to_of(body: &[u8]) -> String {
    let v: Value = serde_json::from_slice(body).unwrap_or_default();
    let p = common::mcp_remote::oauth::clean_return_path(v["return_to"].as_str().unwrap_or(""));
    if p == "/knowledge" || p.starts_with("/knowledge/") || p.starts_with("/knowledge?") {
        p
    } else {
        String::new()
    }
}

/// What a sign-in that isn't set up says: one sentence, then who can fix it.
fn not_set_up(kind: Kind) -> String {
    format!(
        "{label} sign-in isn't set up on this Coderhelm yet. Ask whoever runs Coderhelm to set up {label} sign-in.",
        label = kind.label()
    )
}

/// POST /api/knowledge/connectors/:kind/connect — start the one-click
/// sign-in. Returns where to send the browser; the existing sign-in
/// callback page finishes it.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
    body: Bytes,
) -> Reply {
    let return_to = return_to_of(&body);
    claims.require_role(3).map_err(forbidden)?;
    let kind = kind_of(&kind)?;
    // A hybrid install that chose CoderHelm's app for this provider signs in
    // through CoderHelm's broker (its client id is the install id).
    let brokered = super::hybrid_install::brokered(&state, kind).await;
    let own = app(&state, kind);
    let hosted = common::hybrid::hosted_url();
    let via = match &brokered {
        Some(c) => oauth::Via::Broker {
            hosted: &hosted,
            install_id: &c.install_id,
            secret: &c.secret,
        },
        None => oauth::Via::App(&own),
    };
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    let redirect = super::mcp_servers::redirect_uri(&state.config.stage);
    let added_at = now();
    let who = oauth::Starter {
        user_sub: &claims.sub,
        user_email: &claims.email,
        added_at: &added_at,
        redirect_uri: &redirect,
        return_to: &return_to,
    };
    let url = oauth::start_sign_in(&rt, kind, via, &who)
        .await
        .map_err(|e| match e {
            oauth::StartError::NotSetUp => fail(StatusCode::BAD_REQUEST, &not_set_up(kind)),
            oauth::StartError::NotOAuth => fail(
                StatusCode::BAD_REQUEST,
                "This kind of source doesn't sign in.",
            ),
            oauth::StartError::Internal(e) => internal("knowledge connection", e),
            oauth::StartError::Refused(e) => fail(StatusCode::BAD_REQUEST, &e),
        })?;
    info!(team = %claims.team_id, kind = kind.key(), "Knowledge sign-in started");
    Ok(Json(json!({ "authorize_url": url })))
}

/// Starts `claims`' own sign-in to `kind` through its remote MCP server, on
/// the team's Connected apps entry for it when that signs each person in,
/// else on Knowledge's own record. Returns where to send the browser (a
/// popup; the shared callback page tells the page that opened it).
async fn start_member_signin(
    state: &AppState,
    claims: &Claims,
    kind: Kind,
    return_to: &str,
) -> Result<String, (StatusCode, Json<Value>)> {
    let rt = super::mcp_servers::runtime(state, &claims.team_id);
    let mut rec = connect::signin_target(&rt, kind)
        .await
        .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e))?;
    let url = rt
        .start_sign_in_for(
            &mut rec,
            &Owner::user(&claims.email),
            &claims.sub,
            &claims.email,
            &super::mcp_servers::redirect_uri(&state.config.stage),
            super::mcp_servers::metadata_document_url(&state.config.stage).as_deref(),
            Some(common::mcp_remote::oauth::Flow::Knowledge),
            return_to,
        )
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    info!(team = %claims.team_id, kind = kind.key(), server = %rec.id, "Knowledge member sign-in started");
    Ok(url)
}

/// POST /api/knowledge/connectors/:kind/signin — sign in with your own
/// account for the provider (Sanity), with nothing for an admin to set up.
/// Answers `signed_in` when you already are, else where to send a popup.
pub async fn signin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
    body: Bytes,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let kind = kind_of(&kind)?;
    if connect::signin_url(kind).is_none() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This kind of source doesn't sign in this way.",
        ));
    }
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    if connect::find_signin(&rt, kind, &claims.email)
        .await
        .map_err(|e| connector_fail(kind, &e))?
        .is_some()
    {
        return Ok(Json(json!({ "signed_in": true })));
    }
    let url = start_member_signin(&state, &claims, kind, &return_to_of(&body)).await?;
    Ok(Json(json!({ "signed_in": false, "authorize_url": url })))
}

/// This person's working sign-in for `kind`, or the error that asks them
/// to sign in.
async fn member_signin(
    state: &AppState,
    claims: &Claims,
    kind: Kind,
) -> Result<connect::SignedIn, (StatusCode, Json<Value>)> {
    let rt = super::mcp_servers::runtime(state, &claims.team_id);
    connect::find_signin(&rt, kind, &claims.email)
        .await
        .map_err(|e| connector_fail(kind, &e))?
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": format!("You aren't signed in to {} yet.", kind.label()),
                    "fix": format!("Sign in with {} to continue.", kind.label()),
                    "action": "sign_in",
                })),
            )
        })
}

/// The member's name from the team's records (empty when there is none).
async fn member_name(state: &AppState, team: &str, sub: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(sub.to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| {
            r.item()
                .and_then(|i| i.get("name"))
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .unwrap_or_default()
}

/// What a new or re-pointed source records about the sign-in it uses.
async fn signin_for_source(
    state: &AppState,
    claims: &Claims,
    si: &connect::SignedIn,
) -> ks::SignIn {
    let account = connect::sanity_signin(
        super::mcp_servers::runtime(state, &claims.team_id),
        si.server.clone(),
        si.owner.clone(),
    )
    .account()
    .await;
    let name = member_name(state, &claims.team_id, &claims.sub).await;
    si.signin(&claims.email, &name, &account)
}

/// DELETE /api/knowledge/connectors/:kind — sign out (and revoke where the
/// provider allows). Sources that used it show "Reconnect".
pub async fn disconnect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let kind = kind_of(&kind)?;
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    let Some(rec) = rt
        .store
        .get_server(&claims.team_id, &oauth::connection_id(kind))
        .await
        .map_err(|e| internal("knowledge connection", e))?
    else {
        return Ok(Json(json!({ "ok": true, "revoked": false })));
    };
    let revoked = rt
        .disconnect(&rec, &common::mcp_remote::store::Owner::Team)
        .await
        .map_err(|e| internal("knowledge disconnect", e))?;
    info!(team = %claims.team_id, kind = kind.key(), revoked, "Knowledge sign-in removed");
    Ok(Json(json!({ "ok": true, "revoked": revoked })))
}

#[derive(Debug, Deserialize)]
pub struct BrowseQuery {
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub q: String,
    /// `sign_in`: browse with your own sign-in (Sanity), not the team's key.
    #[serde(default)]
    pub via: String,
    /// Amazon S3: the account, bucket and region to browse.
    #[serde(default)]
    pub account: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub region: String,
}

/// GET /api/knowledge/connectors/:kind/browse?parent=&q= — one level of the
/// picker (the top level without `parent`), narrowed by `q`.
pub async fn browse(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
    Query(q): Query<BrowseQuery>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let kind = kind_of(&kind)?;
    let query = common::truncate_str(q.q.trim(), 200);
    let parent = q
        .parent
        .as_deref()
        .filter(|p| !p.is_empty() && p.len() <= 2_000);
    if q.via == "sign_in" && kind == Kind::Sanity {
        let si = member_signin(&state, &claims, kind).await?;
        let mut c = connect::sanity_signin(
            super::mcp_servers::runtime(&state, &claims.team_id),
            si.server,
            si.owner,
        );
        if parent.is_none() {
            c.hints = sanity_repo_projects(&state, &claims.team_id).await;
        }
        let nodes = c
            .browse(parent, query)
            .await
            .map_err(|e| connector_fail(kind, &e))?;
        return Ok(Json(json!({
            "nodes": nodes.into_iter().take(500).collect::<Vec<_>>(),
            "options": {},
        })));
    }
    let env = env(&state, &claims.team_id, Some(kind)).await;
    let options = if kind == Kind::S3 {
        json!({ "account": q.account.trim(), "bucket": q.bucket.trim(), "region": q.region.trim() })
    } else {
        json!({})
    };
    if kind == Kind::Archive {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "An archive is uploaded, not browsed.",
        ));
    }
    let opened = match connect::open(&env, kind, &options).await {
        Ok(o) => o,
        Err(e) if kind == Kind::S3 => return Err(s3_fail(&state, &claims, &options, &e).await),
        Err(e) => return Err(connector_fail(kind, &e)),
    };
    let nodes = match opened.connector.browse(parent, query).await {
        Ok(n) => n,
        Err(e) if kind == Kind::S3 => return Err(s3_fail(&state, &claims, &options, &e).await),
        Err(e) => return Err(connector_fail(kind, &e)),
    };
    Ok(Json(json!({
        "nodes": nodes.into_iter().take(500).collect::<Vec<_>>(),
        "options": public_options(&opened.options),
    })))
}

/// An S3 bucket that couldn't be read, in plain words, with the one-click
/// update that names the bucket for the team's role when that is all it
/// lacks.
async fn s3_fail(
    state: &AppState,
    claims: &Claims,
    options: &Value,
    e: &ks::ConnectorError,
) -> (StatusCode, Json<Value>) {
    let (status, Json(mut body)) = connector_fail(Kind::S3, e);
    if let (ks::ConnectorError::Forbidden(_), Ok(w)) = (e, ks::s3::where_of(options)) {
        use common::knowledge::search::Store;
        let bases = super::knowledge::store(state)
            .bases(&claims.team_id)
            .await
            .unwrap_or_default();
        let env = env(state, &claims.team_id, Some(Kind::S3)).await;
        let region = env
            .aws_accounts()
            .await
            .into_iter()
            .find(|a| a.account == w.account)
            .map(|a| a.region)
            .unwrap_or_else(|| w.region.clone());
        body["fix"] = json!("Update the CoderHelmLogReader role in that account so it may read this bucket (the link below names it in the role's update stack), then try again.");
        body["grant"] = super::knowledge::grant_for(
            state,
            &claims.team_id,
            &bases,
            &w.account,
            &region,
            None,
            Some(&w.bucket),
        )
        .await
        .unwrap_or(Value::Null);
    }
    (status, Json(body))
}

/// Where the team's scan for Sanity projects named in its repositories is
/// kept, and for how long it is reused.
const SANITY_REPO_PROJECTS_SK: &str = "KNOWLEDGE#SANITY_REPO_PROJECTS";
const SANITY_REPO_PROJECTS_TTL_SECS: i64 = 24 * 60 * 60;

/// The Sanity projects the team's repositories name (id, repository), from
/// the cached scan when it is under a day old. Empty when the team has no
/// GitHub App or the scan fails: the picker then shows Sanity's own list.
async fn sanity_repo_projects(state: &AppState, team: &str) -> Vec<(String, String)> {
    use aws_sdk_dynamodb::types::AttributeValue as Av;
    let table = &state.config.settings_table_name;
    let key = |k: &str, v: &str| (k.to_string(), Av::S(v.to_string()));
    if let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", Av::S(team.to_string()))
        .key("sk", Av::S(SANITY_REPO_PROJECTS_SK.into()))
        .send()
        .await
    {
        if let Some(item) = out.item() {
            let at = item
                .get("at")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<i64>().ok())
                .unwrap_or(0);
            if now_secs() - at < SANITY_REPO_PROJECTS_TTL_SECS {
                let found = item
                    .get("found")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| ks::sanity_repos::from_json(s))
                    .unwrap_or_default();
                return found;
            }
        }
    }
    let Some(token) = github_token(state, team).await else {
        return Vec::new();
    };
    let http = common::mcp_remote::http::GuardedHttps;
    let found = match ks::sanity_repos::scan(&http, &format!("Bearer {token}")).await {
        Ok(f) => f,
        Err(e) => {
            warn!(team, error = %e, "Couldn't read the repositories for Sanity projects");
            return Vec::new();
        }
    };
    let _ = state
        .dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(
            [
                key("pk", team),
                key("sk", SANITY_REPO_PROJECTS_SK),
                key("found", &ks::sanity_repos::to_json(&found)),
                ("at".to_string(), Av::N(now_secs().to_string())),
            ]
            .into_iter()
            .collect(),
        ))
        .send()
        .await
        .map_err(|e| warn!(team, error = %e, "Couldn't keep the Sanity repository scan"));
    found
}

fn connector_fail(kind: Kind, e: &ks::ConnectorError) -> (StatusCode, Json<Value>) {
    let se = e.for_source(kind);
    let status = match e {
        ks::ConnectorError::Config(_) => StatusCode::BAD_REQUEST,
        ks::ConnectorError::Reconnect(_) | ks::ConnectorError::Forbidden(_) => StatusCode::CONFLICT,
        ks::ConnectorError::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(json!({ "error": se.message, "fix": se.fix, "action": se.action })),
    )
}

// ── Sources ─────────────────────────────────────────────────────────────────

/// What a person sets on a source.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SourceInput {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub selection: Option<Vec<Selected>>,
    #[serde(default)]
    pub options: Option<Value>,
    #[serde(default)]
    pub paused: Option<bool>,
    /// `sign_in`: read through the adding member's own sign-in (Sanity)
    /// instead of the team's saved key.
    #[serde(default)]
    pub via: Option<String>,
}

/// The name a source gets when none is given: what was picked.
pub fn suggested_name(kind: Kind, sel: &[Selected]) -> String {
    match sel {
        [] => kind.label().to_string(),
        [one] => common::truncate_str(&one.title, 80).to_string(),
        [first, rest @ ..] => format!(
            "{} and {} more",
            common::truncate_str(&first.title, 60),
            rest.len()
        ),
    }
}

/// Checks a source's options: only known keys, plain values, and Sanity's
/// type mappings in shape.
pub fn clean_options(kind: Kind, raw: &Value) -> Result<Value, String> {
    let Some(m) = raw.as_object() else {
        return Ok(json!({}));
    };
    let allowed: &[&str] = match kind {
        Kind::Sanity => &["project_id", "studio_url", "studio_urls", "types"],
        Kind::Confluence => &["cloud_id", "site"],
        Kind::S3 => &["account", "bucket", "region"],
        _ => &[],
    };
    let mut out = serde_json::Map::new();
    for (k, v) in m {
        if !allowed.contains(&k.as_str()) {
            continue;
        }
        if k == "studio_urls" {
            // One Studio address per project, for a source reading several.
            let Some(urls) = v.as_object() else {
                return Err("Studio addresses must be an object.".into());
            };
            if urls.len() > 50 {
                return Err("Give up to 50 Studio addresses.".into());
            }
            for (p, u) in urls {
                let u = u.as_str().unwrap_or("");
                if !sanity::valid_project(p) {
                    return Err(format!("\"{p}\" isn't a Sanity project id."));
                }
                if u.len() > 300 || (!u.is_empty() && !u.starts_with("https://")) {
                    return Err("The Studio address must start with https://".into());
                }
            }
        } else if k == "types" {
            let Some(types) = v.as_object() else {
                return Err("Type mappings must be an object.".into());
            };
            if types.len() > 50 {
                return Err("Map up to 50 types.".into());
            }
            for (t, map) in types {
                if !sanity::valid_name(t) || !map.is_object() {
                    return Err(format!("The mapping for \"{t}\" isn't valid."));
                }
                if let Some(u) = map["url"].as_str() {
                    if !u.is_empty() && !u.starts_with("https://") {
                        return Err("A link template must start with https://".into());
                    }
                }
            }
        } else if let Some(sv) = v.as_str() {
            if sv.len() > 300 {
                return Err(format!("{k} is too long."));
            }
            if k == "studio_url" && !sv.is_empty() && !sv.starts_with("https://") {
                return Err("The Studio address must start with https://".into());
            }
        } else {
            return Err(format!("{k} must be text."));
        }
        out.insert(k.clone(), v.clone());
    }
    Ok(Value::Object(out))
}

/// Creates a source on `kb_id` and starts its first sync. Shared with
/// "create a knowledge base with its first source".
pub(crate) async fn add_source(
    state: &AppState,
    claims: &Claims,
    kb_id: &str,
    input: &SourceInput,
) -> Result<Value, (StatusCode, Json<Value>)> {
    super::knowledge::refuse_if_connected(state, &claims.team_id, kb_id).await?;
    let kind = kind_of(&input.kind)?;
    if kind == Kind::Archive {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Upload an archive from the knowledge base's page instead.",
        ));
    }
    let selection = clean_selection(input.selection.as_deref().unwrap_or_default())
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let existing = sources_of(state, &claims.team_id, kb_id).await;
    if existing.len() >= MAX_SOURCES_PER_BASE {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("A knowledge base can have up to {MAX_SOURCES_PER_BASE} sources."),
        ));
    }
    let mut options = clean_options(kind, input.options.as_ref().unwrap_or(&Value::Null))
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    if kind == Kind::S3 {
        let w = ks::s3::where_of(&options).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
        options["bucket"] = json!(w.bucket);
    }
    if input.via.as_deref() == Some("sign_in") {
        return add_signin_source(state, claims, kb_id, kind, selection, options, input).await;
    }
    // Opening it checks the connection works before anything is saved, and
    // settles which site or project it reads.
    let env = env(state, &claims.team_id, Some(kind)).await;
    let opened = match connect::open(&env, kind, &options).await {
        Ok(o) => o,
        Err(e) if kind == Kind::S3 => return Err(s3_fail(state, claims, &options, &e).await),
        Err(e) => return Err(connector_fail(kind, &e)),
    };
    // An import from S3 reads the bucket once first: nothing is saved
    // unless the team's role can list what was picked.
    if kind == Kind::S3 {
        let first = selection.first().map(|s| s.id.as_str());
        if let Err(e) = opened.connector.browse(first, "").await {
            return Err(s3_fail(state, claims, &opened.options, &e).await);
        }
    }
    if let (Some(o), Some(resolved)) = (options.as_object_mut(), opened.options.as_object()) {
        for (k, v) in resolved {
            o.entry(k.clone()).or_insert(v.clone());
        }
    }
    let name = input
        .name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|n| common::truncate_str(n, 80).to_string())
        .unwrap_or_else(|| suggested_name(kind, &selection));
    let mut src = Source {
        id: kb::new_id(),
        kb_id: kb_id.to_string(),
        kind: Some(kind),
        name,
        selection,
        options,
        created_at: now(),
        created_by: claims.email.clone(),
        next_sync_at: now_secs(),
        ..Default::default()
    };
    let mut setup = Value::Null;
    match kind {
        Kind::Sanity => {
            let token = webhook::new_token();
            let url = format!(
                "{}/webhooks/knowledge/{token}",
                common::public_urls::api_url(&state.config.stage)
            );
            src.hook = hook_pk(&token);
            let (dataset, types) = sanity_scope(&src.selection);
            let project = src.options["project_id"].as_str().unwrap_or("").to_string();
            let auth = env
                .plugin_credentials("sanity")
                .await
                .and_then(|c| c.get("api_token").cloned())
                .map(|t| format!("Bearer {}", t.trim()))
                .unwrap_or_default();
            match sanity::register_webhook(
                env.http.as_ref(),
                &project,
                &auth,
                &dataset,
                &types,
                &url,
                &token,
            )
            .await
            {
                Ok(id) => {
                    src.webhook = true;
                    src.options["sanity_hook_id"] = json!(id);
                }
                Err(e) => {
                    info!(error = %e, "Sanity webhook not registered; showing manual setup");
                    setup = json!({ "url": url, "secret": token, "dataset": dataset });
                }
            }
        }
        Kind::Notion => {
            if let Some(h) = env.oauth_header(kind).await.ok().flatten() {
                if let Some(ws) = ks::notion::workspace(env.http.as_ref(), &h).await {
                    src.account = ws;
                }
            }
            src.webhook = !src.account.is_empty()
                && state
                    .secrets
                    .notion_webhook_token
                    .as_deref()
                    .is_some_and(|t| !t.is_empty());
        }
        Kind::Github => src.webhook = true,
        _ => {}
    }
    let st = sync_store(state);
    if !src.hook.is_empty() {
        state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", AttributeValue::S(src.hook.clone()))
            .item("sk", AttributeValue::S("HOOK".into()))
            .item("team_id", AttributeValue::S(claims.team_id.clone()))
            .item("kb_id", AttributeValue::S(kb_id.to_string()))
            .item("source_id", AttributeValue::S(src.id.clone()))
            .send()
            .await
            .map_err(|e| internal("knowledge webhook record", e.to_string()))?;
    }
    use common::knowledge::sources::store::SyncStore;
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("create knowledge source", e))?;
    st.put_registry(&claims.team_id, &src)
        .await
        .map_err(|e| internal("register knowledge source", e))?;
    if let Err(e) = start_sync(state, &claims.team_id, kb_id, &src.id, ks::trigger::ADDED).await {
        warn!(error = %e, "First knowledge sync not started; the sweep starts it");
    }
    info!(team = %claims.team_id, kb = %kb_id, source = %src.id, kind = kind.key(), "Knowledge source added");
    Ok(json!({
        "source": source_json(&src, now_secs()),
        "setup": setup,
        "visibility_note": VISIBILITY_NOTE,
    }))
}

/// Creates a source read through the adding member's own sign-in (Sanity's
/// remote MCP server). There is no webhook through a sign-in: it syncs on a
/// schedule, each time asking only for what changed.
async fn add_signin_source(
    state: &AppState,
    claims: &Claims,
    kb_id: &str,
    kind: Kind,
    selection: Vec<Selected>,
    mut options: Value,
    input: &SourceInput,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if kind != Kind::Sanity {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("{} doesn't sync through a sign-in like this.", kind.label()),
        ));
    }
    let probe = Source {
        selection: selection.clone(),
        ..Default::default()
    };
    let projects =
        sanity_mcp::selection_projects(&probe).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let si = member_signin(state, claims, kind).await?;
    // Checks the sign-in can read every project before anything is saved.
    connect::sanity_signin(
        super::mcp_servers::runtime(state, &claims.team_id),
        si.server.clone(),
        si.owner.clone(),
    )
    .check_projects(&projects)
    .await
    .map_err(|e| connector_fail(kind, &e))?;
    set_sanity_projects(&mut options, &projects);
    let name = input
        .name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|n| common::truncate_str(n, 80).to_string())
        .unwrap_or_else(|| suggested_name(kind, &selection));
    let src = Source {
        id: kb::new_id(),
        kb_id: kb_id.to_string(),
        kind: Some(kind),
        name,
        selection,
        options,
        created_at: now(),
        created_by: claims.email.clone(),
        next_sync_at: now_secs(),
        signin: Some(signin_for_source(state, claims, &si).await),
        ..Default::default()
    };
    use common::knowledge::sources::store::SyncStore;
    let st = sync_store(state);
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("create knowledge source", e))?;
    st.put_registry(&claims.team_id, &src)
        .await
        .map_err(|e| internal("register knowledge source", e))?;
    if let Err(e) = start_sync(state, &claims.team_id, kb_id, &src.id, ks::trigger::ADDED).await {
        warn!(error = %e, "First knowledge sync not started; the sweep starts it");
    }
    info!(team = %claims.team_id, kb = %kb_id, source = %src.id, kind = kind.key(), "Knowledge source added through a sign-in");
    Ok(json!({
        "source": source_json(&src, now_secs()),
        "setup": Value::Null,
        "visibility_note": VISIBILITY_NOTE,
    }))
}

/// Records the projects a sign-in source reads. `project_id` keeps the
/// project it was first saved with (documents and cursors saved before a
/// source could read several projects are that project's), so it is set
/// only when missing.
fn set_sanity_projects(options: &mut Value, projects: &[String]) {
    if !options.is_object() {
        *options = json!({});
    }
    let has = options["project_id"]
        .as_str()
        .is_some_and(|p| !p.is_empty());
    if !has {
        if let Some(first) = projects.first() {
            options["project_id"] = json!(first);
        }
    }
    options["project_ids"] = json!(projects);
}

/// The dataset and picked types of a Sanity selection.
fn sanity_scope(sel: &[Selected]) -> (String, Vec<String>) {
    let mut dataset = String::new();
    let mut types = Vec::new();
    let mut whole = false;
    for s in sel {
        if let Some((ds, ty)) = sanity::split_id(&s.id) {
            if dataset.is_empty() {
                dataset = ds.to_string();
            }
            match ty {
                Some(t) => types.push(t.to_string()),
                None => whole = true,
            }
        }
    }
    if whole {
        types.clear();
    }
    (dataset, types)
}

/// POST /api/knowledge/:kb_id/sources — add a source and start importing.
pub async fn create_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
    Json(input): Json<SourceInput>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    if !base_exists(&state, &claims.team_id, &kb_id).await? {
        return Err(fail(StatusCode::NOT_FOUND, "No such knowledge base."));
    }
    add_source(&state, &claims, &kb_id, &input).await.map(Json)
}

async fn load_source(
    state: &AppState,
    team: &str,
    kb_id: &str,
    id: &str,
) -> Result<Source, (StatusCode, Json<Value>)> {
    use common::knowledge::sources::store::SyncStore;
    sync_store(state)
        .source(team, kb_id, id)
        .await
        .map_err(|e| internal("knowledge source", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such source."))
}

/// PUT /api/knowledge/:kb_id/sources/:source_id — rename, change what is
/// picked or its settings, pause or resume.
pub async fn update_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
    Json(input): Json<SourceInput>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let mut src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    let kind = src
        .kind
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "This source has no kind."))?;
    let mut resync = false;
    if let Some(n) = input.name.as_deref().map(str::trim) {
        if n.is_empty() {
            return Err(fail(StatusCode::BAD_REQUEST, "Give the source a name."));
        }
        src.name = common::truncate_str(n, 80).to_string();
    }
    if let Some(sel) = &input.selection {
        src.selection = clean_selection(sel).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
        if src.signin.is_some() {
            let projects = sanity_mcp::selection_projects(&src)
                .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
            set_sanity_projects(&mut src.options, &projects);
        }
        resync = true;
    }
    if let Some(o) = &input.options {
        let mut cleaned = clean_options(kind, o).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
        // Keep what was worked out before (the site, the registered hook).
        for keep in [
            "cloud_id",
            "site",
            "sanity_hook_id",
            "project_id",
            "project_ids",
        ] {
            if cleaned.get(keep).is_none() {
                if let Some(v) = src.options.get(keep) {
                    cleaned[keep] = v.clone();
                }
            }
        }
        src.options = cleaned;
        resync = true;
    }
    if let Some(p) = input.paused {
        if src.paused && !p {
            resync = true;
        }
        src.paused = p;
    }
    // "Change connection": the team's sign-in or its saved API key.
    if let Some(v) = input.via.as_deref().filter(|v| !v.is_empty()) {
        let use_key = match v {
            "saved_key" => true,
            "oauth" => false,
            _ => {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Switch to the team's sign-in or its saved API key.",
                ))
            }
        };
        if !switchable(kind) || src.signin.is_some() {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                &format!("{} sources can't switch connection.", kind.label()),
            ));
        }
        let env = env(&state, &claims.team_id, Some(kind)).await;
        let ok = if use_key {
            match kind {
                Kind::Notion => env
                    .plugin_credentials("notion")
                    .await
                    .and_then(|c| c.get("api_key").cloned())
                    .is_some_and(|k| !k.trim().is_empty()),
                _ => env.confluence_token().await.is_some(),
            }
        } else {
            env.oauth_state(kind).await.0
        };
        if !ok {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                &if use_key {
                    format!(
                        "The team has no {} API token saved. Save one first.",
                        kind.label()
                    )
                } else {
                    format!(
                        "The team isn't signed in to {}. Sign in first.",
                        kind.label()
                    )
                },
            ));
        }
        if src.use_saved_key != use_key {
            src.use_saved_key = use_key;
            if src.status == Status::Error {
                src.status = Status::Queued;
                src.error = None;
            }
            resync = true;
        }
    }
    use common::knowledge::sources::store::SyncStore;
    let st = sync_store(&state);
    if resync {
        src.next_sync_at = now_secs();
        // Re-read everything: a changed mapping changes every item's text.
        if input.options.is_some() {
            if let Ok(items) = st.items(&claims.team_id, &kb_id, &source_id).await {
                for mut f in items {
                    f.remote_version.clear();
                    let _ = st.put_item(&claims.team_id, &f).await;
                }
            }
        }
    }
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("update knowledge source", e))?;
    st.put_registry(&claims.team_id, &src)
        .await
        .map_err(|e| internal("register knowledge source", e))?;
    if resync && !src.paused {
        if let Err(e) = start_sync(
            &state,
            &claims.team_id,
            &kb_id,
            &source_id,
            ks::trigger::EDITED,
        )
        .await
        {
            warn!(error = %e, "Knowledge sync not started");
        }
    }
    Ok(Json(json!({ "source": source_json(&src, now_secs()) })))
}

/// POST /api/knowledge/:kb_id/sources/:source_id/signin — "Sign in again":
/// any admin points a sign-in source at their own sign-in (when the one it
/// used stopped working, or its member left). Already signed in: the source
/// switches to their sign-in and syncs now. Not yet: answers where to send a
/// popup; the page calls this again once it closes.
pub async fn source_signin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let mut src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    let kind = src
        .kind
        .filter(|k| connect::signin_url(*k).is_some())
        .ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "This source doesn't sync through a sign-in.",
            )
        })?;
    if src.signin.is_none() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This source uses the team's API token, not a sign-in.",
        ));
    }
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    let Some(si) = connect::find_signin(&rt, kind, &claims.email)
        .await
        .map_err(|e| connector_fail(kind, &e))?
    else {
        let back = format!("/knowledge/base/?id={}", urlencoding::encode(&kb_id));
        let url = start_member_signin(&state, &claims, kind, &back).await?;
        return Ok(Json(json!({ "authorize_url": url })));
    };
    src.signin = Some(signin_for_source(&state, &claims, &si).await);
    if src.status == Status::Error {
        src.status = Status::Queued;
        src.error = None;
    }
    src.next_sync_at = now_secs();
    use common::knowledge::sources::store::SyncStore;
    let st = sync_store(&state);
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("update knowledge source", e))?;
    st.put_registry(&claims.team_id, &src)
        .await
        .map_err(|e| internal("register knowledge source", e))?;
    if let Err(e) = start_sync(
        &state,
        &claims.team_id,
        &kb_id,
        &source_id,
        ks::trigger::SIGN_IN,
    )
    .await
    {
        warn!(error = %e, "Knowledge sync not started after signing in again");
    }
    info!(team = %claims.team_id, kb = %kb_id, source = %source_id, "Knowledge source signed in again");
    Ok(Json(json!({ "source": source_json(&src, now_secs()) })))
}

/// POST /api/knowledge/:kb_id/sources/:source_id/sync — sync now (works
/// while paused too).
pub async fn sync_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let mut src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    start_sync(
        &state,
        &claims.team_id,
        &kb_id,
        &source_id,
        ks::trigger::MANUAL,
    )
    .await
    .map_err(|e| internal("start knowledge sync", e))?;
    if shown_status(&src, now_secs()) != Status::Syncing {
        src.status = Status::Syncing;
        src.progress = Some(ks::Progress {
            phase: "listing".into(),
            done: 0,
            total: 0,
        });
    }
    Ok(Json(json!({ "source": source_json(&src, now_secs()) })))
}

/// Items one "read these again" names at most.
pub const MAX_RETRY: usize = 500;

#[derive(Debug, Deserialize)]
pub struct RetryBody {
    pub item_ids: Vec<String>,
}

/// POST /api/knowledge/:kb_id/sources/:source_id/retry — read chosen items
/// again on a sync now (items that couldn't be read or were left out):
/// their version marker is forgotten, so the sync fetches them whatever the
/// app says changed.
pub async fn retry_items(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
    Json(body): Json<RetryBody>,
) -> Reply {
    use common::knowledge::sources::store::SyncStore;
    claims.require_role(3).map_err(forbidden)?;
    if body.item_ids.is_empty() || body.item_ids.len() > MAX_RETRY {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("Pick between 1 and {MAX_RETRY} pages to read again."),
        ));
    }
    let src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    let st = sync_store(&state);
    let wanted: std::collections::HashSet<&str> =
        body.item_ids.iter().map(String::as_str).collect();
    let mut queued = 0;
    for mut f in st
        .items(&claims.team_id, &kb_id, &source_id)
        .await
        .map_err(|e| internal("knowledge items", e))?
        .into_iter()
        .filter(|f| wanted.contains(f.id.as_str()))
    {
        f.remote_version.clear();
        st.put_item(&claims.team_id, &f)
            .await
            .map_err(|e| internal("knowledge item", e))?;
        queued += 1;
    }
    if queued == 0 {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "None of those pages are in this source.",
        ));
    }
    start_sync(
        &state,
        &claims.team_id,
        &kb_id,
        &source_id,
        ks::trigger::RETRY,
    )
    .await
    .map_err(|e| internal("start knowledge sync", e))?;
    info!(team = %claims.team_id, kb = %kb_id, source = %source_id, queued, "Knowledge items read again");
    Ok(Json(
        json!({ "queued": queued, "source": source_json(&src, now_secs()) }),
    ))
}

/// Removes every trace of a source: its items and their indexes, its
/// webhook (here and, for Sanity, in the project), its schedule, its record.
pub(crate) async fn remove_source(
    state: &AppState,
    team: &str,
    src: &Source,
) -> Result<usize, String> {
    use common::knowledge::sources::store::SyncStore;
    let st = sync_store(state);
    let items = st.items(team, &src.kb_id, &src.id).await?;
    for f in &items {
        st.delete_item(team, f).await?;
    }
    if src.kind == Some(Kind::Archive) {
        // Every upload of the archive goes too.
        let v = src.options["version"]
            .as_u64()
            .unwrap_or(0)
            .max(src.options["pending"]["version"].as_u64().unwrap_or(0)) as u32;
        for n in 1..=v {
            st.delete_object(
                team,
                &ks::archive::archive_key(team, &src.kb_id, &src.id, n),
            )
            .await;
        }
    }
    if !src.hook.is_empty() {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", AttributeValue::S(src.hook.clone()))
            .key("sk", AttributeValue::S("HOOK".into()))
            .send()
            .await;
    }
    if src.kind == Some(Kind::Sanity) {
        if let Some(hook_id) = src.options["sanity_hook_id"]
            .as_str()
            .filter(|h| !h.is_empty())
        {
            let env = env(state, team, Some(Kind::Sanity)).await;
            if let Some(token) = env
                .plugin_credentials("sanity")
                .await
                .and_then(|c| c.get("api_token").cloned())
            {
                sanity::delete_webhook(
                    env.http.as_ref(),
                    src.options["project_id"].as_str().unwrap_or(""),
                    &format!("Bearer {}", token.trim()),
                    hook_id,
                )
                .await;
            }
        }
    }
    store::delete_registry(
        &state.dynamo,
        &state.config.settings_table_name,
        team,
        &src.kb_id,
        &src.id,
    )
    .await?;
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(ks::source_sk(&src.kb_id, &src.id)))
        .send()
        .await
        .map_err(|e| format!("delete source: {e}"))?;
    Ok(items.len())
}

/// DELETE /api/knowledge/:kb_id/sources/:source_id — remove the source and
/// everything it brought in.
pub async fn delete_source(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let src = load_source(&state, &claims.team_id, &kb_id, &source_id).await?;
    let removed = remove_source(&state, &claims.team_id, &src)
        .await
        .map_err(|e| internal("delete knowledge source", e))?;
    info!(team = %claims.team_id, kb = %kb_id, source = %source_id, removed, "Knowledge source removed");
    Ok(Json(json!({ "ok": true, "removed": removed })))
}

// ── Change events ───────────────────────────────────────────────────────────

async fn hook_target(state: &AppState, token: &str) -> Option<(String, String, String)> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(hook_pk(token)))
        .key("sk", AttributeValue::S("HOOK".into()))
        .send()
        .await
        .ok()?;
    let item = r.item()?;
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    Some((s("team_id")?, s("kb_id")?, s("source_id")?))
}

/// POST /webhooks/knowledge/:token — a provider says something changed.
/// Sanity posts to each source's own address (the token is also its
/// signing secret); Notion posts to `/webhooks/knowledge/notion` for the
/// whole install. A verified event only starts a sync; content is always
/// read back with the source's own connection.
/// A provider's change event (the fields Coderhelm reads; the rest is
/// ignored). Sanity sends the changed document's projection; Notion sends
/// an event naming its workspace.
#[derive(utoipa::ToSchema)]
#[schema(example = json!({ "_id": "post-returns", "_type": "post", "_rev": "q8PZ1v3VaaO9ShV3lKgqXm" }))]
#[allow(dead_code)]
pub struct KnowledgeWebhookBody {
    /// Sanity: the changed document's id.
    #[schema(rename = "_id", example = "post-returns")]
    id: Option<String>,
    /// Sanity: the changed document's type.
    #[schema(rename = "_type", example = "post")]
    doc_type: Option<String>,
    /// Sanity: the document's new revision.
    #[schema(rename = "_rev", example = "q8PZ1v3VaaO9ShV3lKgqXm")]
    rev: Option<String>,
    /// Notion: the event, such as `page.content_updated`.
    #[schema(rename = "type", example = "page.content_updated")]
    event_type: Option<String>,
    /// Notion: the workspace the event is about.
    #[schema(example = "1a2b3c4d-0000-4000-8000-5e6f7a8b9c0d")]
    workspace_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/webhooks/knowledge/{token}",
    tag = "Webhooks",
    operation_id = "receiveKnowledgeWebhook",
    summary = "Knowledge source change",
    description = "Tells CoderHelm that pages of a connected knowledge source changed, so it syncs \
        that source now instead of at its next scheduled sync. Sanity posts to the address shown \
        for each Sanity source (CoderHelm registers it itself when the token may manage webhooks), \
        signed with `sanity-webhook-signature`: `t=<ms>,v1=` and the base64url HMAC-SHA256 of \
        `<t>.<body>`, keyed with the token in the address; a signature older than five minutes is \
        refused. Notion posts to `/webhooks/knowledge/notion` for the whole install, signed with \
        `X-Notion-Signature`: `sha256=` and the hex HMAC-SHA256 of the body, keyed with the \
        subscription's verification token. An event only starts a sync: what changed is always read \
        back from the provider with the source's own connection.",
    params(("token" = String, Path, description = "The source's webhook token, or `notion` for the install's Notion subscription.")),
    request_body(content = KnowledgeWebhookBody, description = "The provider's event.", content_type = "application/json"),
    responses(
        (status = 200, description = "Accepted; a sync starts unless the source is paused."),
        (status = 401, description = "The signature is missing, wrong or too old."),
        (status = 404, description = "No source has this address."),
    ),
    security(("sanity_signature" = []), ("notion_signature" = [])),
)]
pub async fn webhook(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");
    if token == webhook::NOTION_PATH {
        return notion_event(&state, header("x-notion-signature"), &body).await;
    }
    if !webhook::valid_token(&token) {
        return StatusCode::NOT_FOUND;
    }
    let Some((team, kb_id, source_id)) = hook_target(&state, &token).await else {
        return StatusCode::NOT_FOUND;
    };
    if let Err(why) = webhook::verify_sanity(
        header("sanity-webhook-signature"),
        &body,
        &token,
        now_secs(),
    ) {
        warn!(team = %team, source = %source_id, why, "Knowledge webhook refused");
        return StatusCode::UNAUTHORIZED;
    }
    use common::knowledge::sources::store::SyncStore;
    let st = sync_store(&state);
    let Ok(Some(mut src)) = st.source(&team, &kb_id, &source_id).await else {
        return StatusCode::NOT_FOUND;
    };
    src.webhook = true;
    src.webhook_seen_at = now();
    let paused = src.paused;
    let _ = st.put_source(&team, &src).await;
    if !paused {
        if let Err(e) = start_sync(&state, &team, &kb_id, &source_id, ks::trigger::CHANGE).await {
            warn!(error = %e, "Knowledge sync not started from webhook");
        }
    }
    StatusCode::OK
}

async fn notion_event(state: &AppState, signature: &str, body: &[u8]) -> StatusCode {
    let event: Value = serde_json::from_slice(body).unwrap_or_default();
    // Notion's one-time subscription check carries the token to save as
    // `notion_webhook_token`; it is unsigned and starts nothing.
    if let Some(vt) = event
        .get("verification_token")
        .and_then(|v| v.as_str())
        .filter(|_| event.get("type").is_none())
    {
        // The operator pastes this into Notion's "Verify" step and into the
        // deployment's secret; the gateway log (operators only) is the only
        // place Notion sends it.
        info!(verification_token = %common::truncate_str(vt, 200), "Notion webhook subscription check: paste this token in Notion's Verify step and save it as notion_webhook_token");
        return StatusCode::OK;
    }
    let secret = state
        .secrets
        .notion_webhook_token
        .clone()
        .unwrap_or_default();
    if let Err(why) = webhook::verify_notion(signature, body, &secret) {
        warn!(why, "Notion webhook refused");
        return StatusCode::UNAUTHORIZED;
    }
    let Some(ws) = webhook::notion_workspace(&event) else {
        return StatusCode::OK;
    };
    let rows = store::registry(&state.dynamo, &state.config.settings_table_name)
        .await
        .unwrap_or_default();
    for row in rows
        .iter()
        .filter(|r| r.kind == "notion" && r.account == ws && !r.paused)
    {
        if let Some((team, kb_id, id)) = ks::parse_registry_sk(&row.sk) {
            let _ = start_sync(state, &team, &kb_id, &id, ks::trigger::CHANGE).await;
        }
    }
    StatusCode::OK
}

/// A push to a repository's default branch: sync the team's GitHub sources
/// that read it.
pub(crate) async fn on_push(state: &AppState, team: &str, repo: &str) {
    let bases = {
        use common::knowledge::search::Store;
        super::knowledge::store(state)
            .bases(team)
            .await
            .unwrap_or_default()
    };
    for b in bases {
        for s in sources_of(state, team, &b.id).await {
            if s.kind != Some(Kind::Github) || s.paused {
                continue;
            }
            let reads = s.selection.iter().any(|sel| {
                ks::github::split_id(&sel.id).is_some_and(|(_, r, _)| r.eq_ignore_ascii_case(repo))
            });
            if reads {
                let _ = start_sync(state, team, &b.id, &s.id, ks::trigger::CHANGE).await;
            }
        }
    }
}

/// The kinds of a base's sources (apps first, then `upload`), for lists and
/// the chat's source picker.
pub(crate) fn kinds(files: &[FileMeta], sources: &[Source]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for s in sources {
        let k = s.kind_key().to_string();
        if !k.is_empty() && !out.contains(&k) {
            out.push(k);
        }
    }
    if files.iter().any(|f| !f.is_synced()) {
        out.push("upload".into());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(title: &str) -> Selected {
        Selected {
            id: title.to_lowercase(),
            title: title.into(),
            kind: "page".into(),
            include_children: true,
            ..Default::default()
        }
    }

    #[test]
    fn names_come_from_what_was_picked() {
        assert_eq!(
            suggested_name(Kind::Notion, &[sel("Engineering")]),
            "Engineering"
        );
        assert_eq!(
            suggested_name(Kind::Notion, &[sel("Engineering"), sel("HR"), sel("Ops")]),
            "Engineering and 2 more"
        );
        assert_eq!(suggested_name(Kind::Web, &[]), "Website");
    }

    #[test]
    fn options_are_checked_per_kind() {
        let ok = clean_options(
            Kind::Sanity,
            &json!({"project_id": "abc", "types": {"post": {"title": "title", "url": "https://x.example/{slug.current}"}}, "evil": 1}),
        )
        .unwrap();
        assert!(ok.get("evil").is_none());
        assert!(clean_options(
            Kind::Sanity,
            &json!({"types": {"post": {"url": "javascript:x"}}})
        )
        .is_err());
        assert!(clean_options(Kind::Sanity, &json!({"studio_url": "http://x"})).is_err());
        assert_eq!(
            clean_options(Kind::Notion, &json!({"project_id": "x"})).unwrap(),
            json!({})
        );
        let per_project =
            json!({"studio_urls": {"abc123": "https://a.sanity.studio", "other1": ""}});
        assert_eq!(
            clean_options(Kind::Sanity, &per_project).unwrap(),
            per_project
        );
        assert!(clean_options(
            Kind::Sanity,
            &json!({"studio_urls": {"abc123": "http://a.example"}})
        )
        .is_err());
        assert!(clean_options(
            Kind::Sanity,
            &json!({"studio_urls": {"NOT A PROJECT": "https://a.example"}})
        )
        .is_err());
    }

    #[test]
    fn a_sign_in_source_records_every_project_and_keeps_its_first() {
        let mut o = json!({});
        set_sanity_projects(&mut o, &["abc123".into(), "other1".into()]);
        assert_eq!(
            o,
            json!({"project_id": "abc123", "project_ids": ["abc123", "other1"]})
        );
        // Documents saved before belong to the first project: it stays.
        set_sanity_projects(&mut o, &["other1".into()]);
        assert_eq!(
            o,
            json!({"project_id": "abc123", "project_ids": ["other1"]})
        );
        let two = Source {
            selection: ["dataset:abc123:production", "type:other1:staging:post"]
                .iter()
                .map(|i| Selected {
                    id: i.to_string(),
                    title: i.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        assert_eq!(
            sanity_mcp::selection_projects(&two).unwrap(),
            ["abc123", "other1"]
        );
    }

    #[test]
    fn a_source_never_shows_its_hook_id_and_paused_sources_have_no_next_sync() {
        let s = Source {
            id: "s".into(),
            kind: Some(Kind::Sanity),
            options: json!({"project_id": "p", "sanity_hook_id": "h"}),
            paused: true,
            next_sync_at: 10,
            ..Default::default()
        };
        let v = source_json(&s, 0);
        assert!(v["options"].get("sanity_hook_id").is_none());
        assert_eq!(v["next_sync_at"], "");
        assert_eq!(v["kind"], "sanity");
    }

    #[test]
    fn a_sign_in_source_says_whose_sign_in_it_uses_and_nothing_secret() {
        let s = Source {
            id: "s".into(),
            kind: Some(Kind::Sanity),
            signin: Some(ks::SignIn {
                server_id: "ks-sanity".into(),
                owner: "user#ada@example.com".into(),
                email: "ada@example.com".into(),
                name: "Ada".into(),
                account: "Ada L".into(),
                alerted_at: String::new(),
            }),
            ..Default::default()
        };
        let v = source_json(&s, 0);
        assert_eq!(v["connection"], "sign_in");
        assert_eq!(v["signin"]["name"], "Ada");
        assert_eq!(v["signin"]["account"], "Ada L");
        assert!(v["signin"].get("owner").is_none() && v["signin"].get("server_id").is_none());
        // Options can never name a sign-in.
        let o = clean_options(
            Kind::Sanity,
            &json!({"signin": {"owner": "user#bo@example.com"}}),
        )
        .unwrap();
        assert_eq!(o, json!({}));
        let plain = source_json(&Source::default(), 0);
        assert_eq!(plain["connection"], "");
        assert!(plain["signin"].is_null());
    }

    #[test]
    fn sanity_scope_is_one_dataset_and_its_types() {
        let s = |id: &str| Selected {
            id: id.into(),
            ..Default::default()
        };
        assert_eq!(
            sanity_scope(&[s("type:production:post"), s("type:production:faq")]),
            ("production".into(), vec!["post".into(), "faq".into()])
        );
        assert_eq!(
            sanity_scope(&[s("dataset:production"), s("type:production:post")])
                .1
                .len(),
            0
        );
    }

    #[test]
    fn kinds_list_uploads_then_sources() {
        let up = FileMeta::default();
        let synced = FileMeta {
            source_id: "s".into(),
            ..Default::default()
        };
        let notion = Source {
            kind: Some(Kind::Notion),
            ..Default::default()
        };
        assert_eq!(
            kinds(&[up, synced.clone()], std::slice::from_ref(&notion)),
            ["notion", "upload"]
        );
        assert_eq!(kinds(&[synced], &[notion]), ["notion"]);
    }

    #[test]
    fn a_sign_in_returns_only_to_a_knowledge_page() {
        let r = |b: &str| super::return_to_of(b.as_bytes());
        assert_eq!(
            r(r#"{"return_to":"/knowledge/base/?id=kb1"}"#),
            "/knowledge/base/?id=kb1"
        );
        assert_eq!(r(r#"{"return_to":"/knowledge/"}"#), "/knowledge/");
        assert_eq!(r(r#"{"return_to":"/settings/"}"#), "");
        assert_eq!(r(r#"{"return_to":"//evil.example/knowledge/"}"#), "");
        assert_eq!(r(r#"{"return_to":"/knowledgeX"}"#), "");
        assert_eq!(r(""), "", "an older dashboard sends no body");
    }

    #[test]
    fn a_source_says_how_it_connects() {
        let mut s = Source {
            kind: Some(Kind::Confluence),
            ..Default::default()
        };
        assert_eq!(
            via_of(&s),
            "",
            "only a sync can tell for an app with two ways in"
        );
        s.use_saved_key = true;
        assert_eq!(via_of(&s), "saved_key");
        s.used = Some(ks::UsedConnection {
            via: "oauth".into(),
            ..Default::default()
        });
        assert_eq!(via_of(&s), "oauth", "what the last sync used wins");
        for (kind, via) in [
            (Kind::Web, "public"),
            (Kind::Archive, "upload"),
            (Kind::S3, "aws_role"),
            (Kind::Github, "github_app"),
            (Kind::Sanity, "saved_key"),
        ] {
            let s = Source {
                kind: Some(kind),
                ..Default::default()
            };
            assert_eq!(via_of(&s), via, "{kind:?}");
        }
        let signin = Source {
            kind: Some(Kind::Sanity),
            signin: Some(ks::SignIn::default()),
            ..Default::default()
        };
        assert_eq!(via_of(&signin), "sign_in");
        let v = source_json(&s, 0);
        assert_eq!(v["via"], "oauth");
        assert_eq!(v["interval_mins"], 60);
        assert!(v.get("webhook_seen_at").is_some());
    }

    #[test]
    fn leaving_is_said_plainly_for_every_connection() {
        let team = if_they_leave("oauth", "Ada", "Confluence");
        assert!(team.contains("belongs to the team") && team.contains("Ada"));
        let own = if_they_leave("sign_in", "Ada", "Sanity");
        assert!(
            own.contains("Ada's own Sanity sign-in") && own.contains("any admin can sign in again")
        );
        assert!(if_they_leave("oauth", " ", "Notion").contains("the person who connected it"));
        for via in [
            "saved_key",
            "github_app",
            "aws_role",
            "public",
            "upload",
            "",
        ] {
            assert!(!if_they_leave(via, "Ada", "Notion").is_empty());
        }
        assert!(switchable(Kind::Notion) && switchable(Kind::Confluence));
        assert!(!switchable(Kind::Sanity) && !switchable(Kind::GoogleDrive));
    }
}
