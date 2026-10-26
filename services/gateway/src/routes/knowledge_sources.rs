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

/// The install's OAuth app for `kind`, from its secrets.
pub(crate) fn app(state: &AppState, kind: Kind) -> oauth::App {
    let s = &state.secrets;
    let pick = |a: &Option<String>, b: &Option<String>| {
        a.clone()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| b.clone())
            .unwrap_or_default()
    };
    match kind {
        Kind::Notion => oauth::App {
            client_id: pick(&s.notion_client_id, &None),
            client_secret: pick(&s.notion_client_secret, &None),
        },
        Kind::Confluence => oauth::App {
            client_id: pick(&s.atlassian_client_id, &None),
            client_secret: pick(&s.atlassian_client_secret, &None),
        },
        Kind::GoogleDrive => oauth::App {
            client_id: pick(&s.google_drive_client_id, &s.google_client_id),
            client_secret: pick(&s.google_drive_client_secret, &s.google_client_secret),
        },
        _ => oauth::App::default(),
    }
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
        "truncated": s.truncated,
        "last_sync_at": s.last_sync_at,
        "last_success_at": s.last_success_at,
        "next_sync_at": if s.paused { String::new() } else { ts(s.next_sync_at) },
        "live_updates": s.webhook,
        "created_at": s.created_at,
        "created_by": s.created_by,
        "connection": if s.signin.is_some() { "sign_in" } else { "" },
        "signin": s.signin.as_ref().map(|si| json!({
            "name": si.who(),
            "email": si.email,
            "account": si.account,
        })),
    })
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
) -> Result<(), String> {
    let name = &state.config.worker_function_name;
    if name.is_empty() {
        return Err("no worker configured".into());
    }
    let payload = json!({ "knowledge_sync": { "team_id": team, "kb_id": kb_id, "source_id": id } });
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
            app(&state, kind).is_set() || super::hybrid_install::brokered(kind).is_some(),
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
            "live_updates": kind.has_webhooks(),
            "picker": if kind == Kind::Web { "address" } else { "tree" },
        }));
    }
    Ok(Json(
        json!({ "connectors": out, "visibility_note": VISIBILITY_NOTE }),
    ))
}

/// POST /api/knowledge/connectors/:kind/connect — start the one-click
/// sign-in. Returns where to send the browser; the existing sign-in
/// callback page finishes it.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kind): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let kind = kind_of(&kind)?;
    let provider = oauth::provider(kind).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "This kind of source doesn't sign in.",
        )
    })?;
    // A hybrid install that chose CoderHelm's app for this provider signs in
    // through CoderHelm's broker (its client id is the install id).
    let brokered = super::hybrid_install::brokered(kind);
    let a = match &brokered {
        Some(c) => oauth::App {
            client_id: c.install_id.clone(),
            client_secret: c.secret.clone(),
        },
        None => app(&state, kind),
    };
    if !a.is_set() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!(
                "{} sign-in isn't set up on this Coderhelm yet. Ask whoever runs it to add the {} app.",
                kind.label(),
                kind.label()
            ),
        ));
    }
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    let id = oauth::connection_id(kind);
    let sealed = rt
        .seal_client_secret(&id, a.client_secret.trim())
        .await
        .map_err(|e| internal("seal knowledge client", e))?;
    let mut rec = if brokered.is_some() {
        oauth::brokered_connection_record(
            kind,
            &common::hybrid::hosted_url(),
            a.client_id.trim(),
            &sealed,
        )
    } else {
        oauth::connection_record(kind, &provider, a.client_id.trim(), &sealed)
    };
    rec.added_by = claims.email.clone();
    rec.added_at = now();
    if let Ok(Some(old)) = rt.store.get_server(&claims.team_id, &id).await {
        rec.status = old.status;
        rec.status_reason = old.status_reason;
        rec.added_by = old.added_by;
        rec.added_at = old.added_at;
    }
    rt.store
        .put_server(&claims.team_id, &rec)
        .await
        .map_err(|e| internal("knowledge connection", e))?;
    let url = rt
        .start_sign_in(
            &mut rec,
            &common::mcp_remote::store::Owner::Team,
            &claims.sub,
            &claims.email,
            &super::mcp_servers::redirect_uri(&state.config.stage),
            None,
        )
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
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
) -> Result<String, (StatusCode, Json<Value>)> {
    let rt = super::mcp_servers::runtime(state, &claims.team_id);
    let mut rec = connect::signin_target(&rt, kind)
        .await
        .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e))?;
    let url = rt
        .start_sign_in(
            &mut rec,
            &Owner::user(&claims.email),
            &claims.sub,
            &claims.email,
            &super::mcp_servers::redirect_uri(&state.config.stage),
            super::mcp_servers::metadata_document_url(&state.config.stage).as_deref(),
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
    let url = start_member_signin(&state, &claims, kind).await?;
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
        let c = connect::sanity_signin(
            super::mcp_servers::runtime(&state, &claims.team_id),
            si.server,
            si.owner,
        );
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
    let opened = connect::open(&env, kind, &json!({}))
        .await
        .map_err(|e| connector_fail(kind, &e))?;
    let nodes = opened
        .connector
        .browse(parent, query)
        .await
        .map_err(|e| connector_fail(kind, &e))?;
    Ok(Json(json!({
        "nodes": nodes.into_iter().take(500).collect::<Vec<_>>(),
        "options": public_options(&opened.options),
    })))
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
        Kind::Sanity => &["project_id", "studio_url", "types"],
        Kind::Confluence => &["cloud_id", "site"],
        _ => &[],
    };
    let mut out = serde_json::Map::new();
    for (k, v) in m {
        if !allowed.contains(&k.as_str()) {
            continue;
        }
        if k == "types" {
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
    let kind = kind_of(&input.kind)?;
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
    if input.via.as_deref() == Some("sign_in") {
        return add_signin_source(state, claims, kb_id, kind, selection, options, input).await;
    }
    // Opening it checks the connection works before anything is saved, and
    // settles which site or project it reads.
    let env = env(state, &claims.team_id, Some(kind)).await;
    let opened = connect::open(&env, kind, &options)
        .await
        .map_err(|e| connector_fail(kind, &e))?;
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
    if let Err(e) = start_sync(state, &claims.team_id, kb_id, &src.id).await {
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
    let project =
        sanity_mcp::selection_project(&probe).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let si = member_signin(state, claims, kind).await?;
    // Checks the sign-in can read the project before anything is saved.
    connect::sanity_signin(
        super::mcp_servers::runtime(state, &claims.team_id),
        si.server.clone(),
        si.owner.clone(),
    )
    .datasets(&project)
    .await
    .map_err(|e| connector_fail(kind, &e))?;
    options["project_id"] = json!(project);
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
    if let Err(e) = start_sync(state, &claims.team_id, kb_id, &src.id).await {
        warn!(error = %e, "First knowledge sync not started; the sweep starts it");
    }
    info!(team = %claims.team_id, kb = %kb_id, source = %src.id, kind = kind.key(), "Knowledge source added through a sign-in");
    Ok(json!({
        "source": source_json(&src, now_secs()),
        "setup": Value::Null,
        "visibility_note": VISIBILITY_NOTE,
    }))
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
            let project = sanity_mcp::selection_project(&src)
                .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
            src.options["project_id"] = json!(project);
        }
        resync = true;
    }
    if let Some(o) = &input.options {
        let mut cleaned = clean_options(kind, o).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
        // Keep what was worked out before (the site, the registered hook).
        for keep in ["cloud_id", "site", "sanity_hook_id", "project_id"] {
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
        if let Err(e) = start_sync(&state, &claims.team_id, &kb_id, &source_id).await {
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
        let url = start_member_signin(&state, &claims, kind).await?;
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
    if let Err(e) = start_sync(&state, &claims.team_id, &kb_id, &source_id).await {
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
    start_sync(&state, &claims.team_id, &kb_id, &source_id)
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
        if let Err(e) = start_sync(&state, &team, &kb_id, &source_id).await {
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
            let _ = start_sync(state, &team, &kb_id, &id).await;
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
                let _ = start_sync(state, team, &b.id, &s.id).await;
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
}
