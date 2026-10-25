//! Remote MCP servers: added by address (or picked from the catalog where the
//! vendor runs one), signed in to with OAuth, a static key or nothing, and
//! their tools allowed one by one.
//!
//! - Admins add, set up and remove servers, pick the allowed tools and
//!   approve another admin's pick. With two or more admins, a widened pick
//!   waits for a second admin (as team tools do).
//! - A shared connection is made by an admin, for the whole team. A personal
//!   one is made by each person for themselves.
//! - The OAuth callback lands on a dashboard page, which posts the `code`
//!   and `state` here with the person's session: the `state` is single use,
//!   bound to this team and this person, and the answer's `iss` is checked
//!   before the code is exchanged.
//!
//! Everything that reaches a server goes through `common::mcp_remote`
//! (public HTTPS addresses only, timeouts, size caps). No route returns a
//! token, key or client secret.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::mcp_remote::http::check_url;
use common::mcp_remote::runtime::{detect, Detected, Runtime};
use common::mcp_remote::store::{AuthKind, OAuthSettings, Owner, ServerRecord, Sharing, Status};
use common::mcp_remote::{oauth, tools};
use serde_json::{json, Value};
use tracing::{error, info};

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
        "Something went wrong on our side. Try again.",
    )
}

fn forbidden(code: StatusCode) -> (StatusCode, Json<Value>) {
    fail(code, "Only admins can change MCP servers.")
}

fn member_only(code: StatusCode) -> (StatusCode, Json<Value>) {
    fail(code, "Ask a member of the team to do this.")
}

/// Holds the "integrations" capability (admins, owners, and custom roles
/// given it): sets servers up, picks tools, connects a shared server.
fn is_admin_role(claims: &Claims) -> bool {
    claims.holder().holds("integrations")
}

/// Where the dashboard's callback page lives for this stage.
pub fn redirect_uri(stage: &str) -> String {
    format!(
        "{}/settings/integrations/mcp-callback/",
        dashboard_origin(stage)
    )
}

fn dashboard_origin(stage: &str) -> &'static str {
    common::public_urls::app_url(stage)
}

/// Coderhelm's Client ID Metadata Document (served by the dashboard). Only
/// on the hosted dashboard: the document's static copy names the hosted
/// callback, so other installs register dynamically instead.
pub fn metadata_document_url(stage: &str) -> Option<String> {
    let origin = dashboard_origin(stage);
    (origin == common::public_urls::stage_app_url("prod"))
        .then(|| format!("{origin}/oauth/mcp-client.json"))
}

pub(crate) fn runtime(state: &AppState, team: &str) -> Runtime {
    Runtime::aws(
        state.dynamo.clone(),
        super::plugins::mcp_table(state),
        state.kms.clone(),
        &state.config.secrets_key_id,
        team,
    )
}

async fn load(rt: &Runtime, id: &str) -> Result<ServerRecord, (StatusCode, Json<Value>)> {
    rt.store
        .get_server(&rt.team, id)
        .await
        .map_err(|e| internal("get mcp server", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That server isn't set up."))
}

/// The owner whose connection this person manages on `s`.
fn owner_of(s: &ServerRecord, claims: &Claims) -> Owner {
    match s.sharing {
        Sharing::Personal if s.auth == AuthKind::OAuth => Owner::user(&claims.email),
        _ => Owner::Team,
    }
}

async fn reply_server(rt: &Runtime, s: &ServerRecord, claims: &Claims) -> Value {
    let mine = if s.sharing == Sharing::Personal {
        rt.store
            .get_token(&rt.team, &s.id, &Owner::user(&claims.email))
            .await
            .ok()
            .flatten()
    } else {
        rt.store
            .get_token(&rt.team, &s.id, &Owner::Team)
            .await
            .ok()
            .flatten()
    };
    s.public_json(mine.as_ref(), is_admin_role(claims))
}

/// GET /api/mcp-servers — the team's remote servers.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let rt = runtime(&state, &claims.team_id);
    let servers = rt
        .store
        .list_servers(&rt.team)
        .await
        .map_err(|e| internal("list mcp servers", e))?;
    let mut out = Vec::new();
    for s in &servers {
        out.push(reply_server(&rt, s, &claims).await);
    }
    Ok(Json(json!({
        "servers": out,
        "can_edit": is_admin_role(&claims),
        "redirect_uri": redirect_uri(&state.config.stage),
    })))
}

/// GET /api/mcp-servers/:id
pub async fn get_one(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let rt = runtime(&state, &claims.team_id);
    let s = load(&rt, &id).await?;
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// What detection found, for the dashboard. Nothing secret is in it.
fn detected_json(d: &Detected) -> Value {
    match d {
        Detected::Open { tools, .. } => json!({ "auth": "none", "tool_count": tools.len() }),
        Detected::Header => json!({ "auth": "header" }),
        Detected::OAuth(p) => {
            let a = &p.auth_server;
            json!({
                "auth": "oauth",
                "issuer": a.issuer,
                "resource": p.resource,
                "scopes": p.scopes,
                // Whether Coderhelm can get a client id by itself.
                "registration": if a.cimd_supported || a.registration_endpoint.is_some() { "automatic" } else { "manual" },
            })
        }
    }
}

/// POST /api/mcp-servers/detect { url } — how a server signs in.
pub async fn detect_auth(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let url = body["url"].as_str().unwrap_or("").trim().to_string();
    check_url(&url).map_err(|e| fail(StatusCode::BAD_REQUEST, &e.to_string()))?;
    let rt = runtime(&state, &claims.team_id);
    let d = detect(rt.http.as_ref(), &url)
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    Ok(Json(
        json!({ "url": url, "detected": detected_json(&d), "redirect_uri": redirect_uri(&state.config.stage) }),
    ))
}

/// The id a new server gets: from its name, unique in the team, never a
/// catalog id or a built-in tool prefix.
fn new_id(name: &str, taken: &BTreeSet<String>) -> String {
    let base = tools::slug(name);
    let reserved = |id: &str| {
        taken.contains(id)
            || id == "github"
            || super::plugins::catalog_json()
                .iter()
                .any(|p| p["id"].as_str() == Some(id))
    };
    if !reserved(&base) {
        return base;
    }
    (2..1000)
        .map(|n| format!("{}-{n}", &base[..base.len().min(20)]))
        .find(|id| !reserved(id))
        .unwrap_or_else(|| format!("s-{}", ulid::Ulid::new().to_string().to_ascii_lowercase()))
}

/// POST /api/mcp-servers — add a server by address, or connect a catalog
/// entry through its vendor's remote server.
///
/// `{ url, name?, sharing?, header_name?, header_value?, client_id?, client_secret? }`
/// or `{ catalog_id, sharing? }`.
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let existing = rt
        .store
        .list_servers(&rt.team)
        .await
        .map_err(|e| internal("list mcp servers", e))?;
    let catalog_id = body["catalog_id"].as_str().unwrap_or("").trim().to_string();
    let (id, name, url) = if catalog_id.is_empty() {
        let url = body["url"].as_str().unwrap_or("").trim().to_string();
        let ep = check_url(&url).map_err(|e| fail(StatusCode::BAD_REQUEST, &e.to_string()))?;
        let name = body["name"]
            .as_str()
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(|n| common::truncate_str(n, 60).to_string())
            .unwrap_or_else(|| ep.host.clone());
        let taken: BTreeSet<String> = existing.iter().map(|s| s.id.clone()).collect();
        (new_id(&name, &taken), name, url)
    } else {
        let entry = super::plugins::catalog_json()
            .into_iter()
            .find(|p| p["id"].as_str() == Some(catalog_id.as_str()))
            .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That catalog entry doesn't exist."))?;
        let url = entry["remote_url"].as_str().unwrap_or("").to_string();
        if url.is_empty() {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "This server has no remote sign-in. Use a key instead.",
            ));
        }
        (
            catalog_id.clone(),
            entry["name"].as_str().unwrap_or(&catalog_id).to_string(),
            url,
        )
    };
    if let Some(dup) = existing
        .iter()
        .find(|s| s.id == id || s.url.trim_end_matches('/') == url.trim_end_matches('/'))
    {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("{} is already set up.", dup.name), "id": dup.id })),
        ));
    }

    let mut s = ServerRecord::new(&id, &name, &url);
    s.catalog_id = catalog_id;
    s.added_by = claims.email.to_ascii_lowercase();
    s.added_at = chrono::Utc::now().to_rfc3339();
    s.changed_by = s.added_by.clone();
    let detected = detect(rt.http.as_ref(), &url)
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    match &detected {
        Detected::Open { tools, era } => {
            s.auth = AuthKind::None;
            s.tools = tools.clone();
            s.era = era.as_str().into();
            s.tools_listed_at = s.added_at.clone();
            s.status = Status::Connected;
        }
        Detected::Header => {
            let header_name = body["header_name"]
                .as_str()
                .map(str::trim)
                .filter(|h| !h.is_empty())
                .unwrap_or("Authorization")
                .to_string();
            let value = body["header_value"]
                .as_str()
                .unwrap_or("")
                .trim()
                .to_string();
            if value.is_empty() {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "This server needs a key. Paste the header and value from the vendor.",
                ));
            }
            if !valid_header_name(&header_name) {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "That header name isn't valid.",
                ));
            }
            s.auth = AuthKind::Header;
            s.header_name = header_name;
            s.header_sealed = rt
                .seal_header(&s.id, &value)
                .await
                .map_err(|e| internal("seal mcp key", e))?;
            // The key is checked before anything is saved.
            let (tools, skipped, era) = rt.list_tools(&s, &Owner::Team).await.map_err(|e| {
                fail(
                    StatusCode::BAD_REQUEST,
                    &format!("The server didn't accept that key: {e}"),
                )
            })?;
            s.tools = tools;
            s.skipped = skipped;
            s.era = era.as_str().into();
            s.tools_listed_at = s.added_at.clone();
            s.status = Status::Connected;
        }
        Detected::OAuth(plan) => {
            s.auth = AuthKind::OAuth;
            s.sharing = Sharing::parse(body["sharing"].as_str().unwrap_or("shared"));
            let mut o = OAuthSettings::from_plan(plan);
            let client_id = body["client_id"].as_str().unwrap_or("").trim().to_string();
            if !client_id.is_empty() {
                set_manual_client(
                    &rt,
                    &s.id,
                    &mut o,
                    &client_id,
                    body["client_secret"].as_str(),
                )
                .await?;
            }
            s.oauth = Some(o);
            s.status = Status::NotConnected;
        }
    }
    rt.store
        .put_server(&rt.team, &s)
        .await
        .map_err(|e| internal("save mcp server", e))?;
    info!(team = %rt.team, server = %s.id, auth = s.auth.as_str(), by = %claims.email, "MCP server added");
    Ok(Json(json!({
        "server": reply_server(&rt, &s, &claims).await,
        "detected": detected_json(&detected),
    })))
}

fn valid_header_name(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 64
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && !matches!(
            h.to_ascii_lowercase().as_str(),
            "host"
                | "content-type"
                | "content-length"
                | "mcp-session-id"
                | "mcp-protocol-version"
                | "mcp-method"
                | "mcp-name"
                | "accept"
        )
}

async fn set_manual_client(
    rt: &Runtime,
    id: &str,
    o: &mut OAuthSettings,
    client_id: &str,
    secret: Option<&str>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if client_id.len() > 512 {
        return Err(fail(StatusCode::BAD_REQUEST, "That client id is too long."));
    }
    o.client_id = client_id.to_string();
    o.registration = oauth::Registration::Manual.as_str().into();
    match secret.map(str::trim).filter(|s| !s.is_empty()) {
        Some(sec) => {
            o.client_secret_sealed = rt
                .seal_client_secret(id, sec)
                .await
                .map_err(|e| internal("seal client secret", e))?;
            o.client_auth = if o
                .token_auth_methods
                .iter()
                .any(|m| m == "client_secret_basic")
                || o.token_auth_methods.is_empty()
            {
                oauth::ClientAuth::ClientSecretBasic
            } else {
                oauth::ClientAuth::ClientSecretPost
            };
        }
        None => {
            o.client_secret_sealed.clear();
            o.client_auth = oauth::ClientAuth::None;
        }
    }
    Ok(())
}

/// PUT /api/mcp-servers/:id/client { client_id, client_secret? } — a client
/// an admin registered with the vendor, for servers without registration.
pub async fn set_client(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    let mut o = s.oauth.clone().ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "This server doesn't sign in with OAuth.",
        )
    })?;
    let client_id = body["client_id"].as_str().unwrap_or("").trim();
    if client_id.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Paste the client id."));
    }
    set_manual_client(
        &rt,
        &s.id,
        &mut o,
        client_id,
        body["client_secret"].as_str(),
    )
    .await?;
    s.oauth = Some(o);
    rt.store
        .put_server(&rt.team, &s)
        .await
        .map_err(|e| internal("save mcp client", e))?;
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// PUT /api/mcp-servers/:id/key { header_name?, header_value } — replace a
/// static key (checked against the server before it is saved).
pub async fn set_key(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    if s.auth != AuthKind::Header {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This server doesn't use a key.",
        ));
    }
    if let Some(h) = body["header_name"]
        .as_str()
        .map(str::trim)
        .filter(|h| !h.is_empty())
    {
        if !valid_header_name(h) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "That header name isn't valid.",
            ));
        }
        s.header_name = h.to_string();
    }
    let value = body["header_value"].as_str().unwrap_or("").trim();
    if value.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Paste the key."));
    }
    s.header_sealed = rt
        .seal_header(&s.id, value)
        .await
        .map_err(|e| internal("seal mcp key", e))?;
    rt.refresh_tool_list(&mut s, &Owner::Team)
        .await
        .map_err(|e| {
            fail(
                StatusCode::BAD_REQUEST,
                &format!("The server didn't accept that key: {e}"),
            )
        })?;
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// POST /api/mcp-servers/:id/connect — start an OAuth sign-in. Admins
/// connect a shared server; anyone connects their own on a personal one.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(1).map_err(member_only)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    if s.auth != AuthKind::OAuth {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This server doesn't sign in with OAuth.",
        ));
    }
    if s.sharing == Sharing::Shared && !is_admin_role(&claims) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins connect a server the whole team shares.",
        ));
    }
    let owner = owner_of(&s, &claims);
    let url = rt
        .start_sign_in(
            &mut s,
            &owner,
            &claims.sub,
            &claims.email,
            &redirect_uri(&state.config.stage),
            metadata_document_url(&state.config.stage).as_deref(),
        )
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    info!(team = %rt.team, server = %s.id, owner = %owner.key(), "MCP sign-in started");
    Ok(Json(json!({ "authorize_url": url })))
}

/// POST /api/mcp-servers/oauth/callback { state, code?, error?, iss? }
pub async fn oauth_callback(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(1).map_err(member_only)?;
    let st = body["state"].as_str().unwrap_or("");
    if st.is_empty() || st.len() > 200 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That sign-in link is missing its state.",
        ));
    }
    let rt = runtime(&state, &claims.team_id);
    let (mut s, owner) = rt
        .finish_sign_in(
            st,
            body["code"].as_str(),
            body["error"].as_str().filter(|e| !e.is_empty()),
            body["iss"].as_str().filter(|e| !e.is_empty()),
            &claims.sub,
            &claims.email,
        )
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    // List the tools now so the picker has them. A failure here leaves the
    // connection saved; the status says why.
    // A knowledge source's sign-in has no tools to list.
    if common::knowledge::sources::oauth::is_source_connection(&s.catalog_id) {
        info!(team = %rt.team, server = %s.id, "Knowledge source connected");
        return Ok(Json(json!({
            "server": { "id": s.id, "name": s.name, "catalog_id": s.catalog_id },
            "tools_error": Value::Null,
        })));
    }
    let listed = rt.refresh_tool_list(&mut s, &owner).await;
    if let Err(e) = &listed {
        info!(team = %rt.team, server = %s.id, error = %e, "Tools not listed after sign-in");
    }
    let s = load(&rt, &s.id).await?;
    info!(team = %rt.team, server = %s.id, owner = %owner.key(), "MCP server connected");
    Ok(Json(json!({
        "server": reply_server(&rt, &s, &claims).await,
        "tools_error": listed.err().map(|e| e.to_string()),
    })))
}

/// DELETE /api/mcp-servers/:id/connection — disconnect (and revoke) the
/// shared connection (admins) or your own (anyone, on a personal server).
pub async fn disconnect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(1).map_err(member_only)?;
    let rt = runtime(&state, &claims.team_id);
    let s = load(&rt, &id).await?;
    if s.sharing == Sharing::Shared && !is_admin_role(&claims) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins disconnect a shared server.",
        ));
    }
    let owner = owner_of(&s, &claims);
    let revoked = rt
        .disconnect(&s, &owner)
        .await
        .map_err(|e| internal("disconnect mcp server", e))?;
    info!(team = %rt.team, server = %s.id, owner = %owner.key(), revoked, "MCP server disconnected");
    let s = load(&rt, &id).await?;
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await, "revoked": revoked }),
    ))
}

/// DELETE /api/mcp-servers/:id — remove a server and every connection to it.
pub async fn remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let s = load(&rt, &id).await?;
    rt.remove(&s)
        .await
        .map_err(|e| internal("remove mcp server", e))?;
    info!(team = %rt.team, server = %s.id, by = %claims.email, "MCP server removed");
    Ok(Json(json!({ "status": "removed", "id": id })))
}

/// PUT /api/mcp-servers/:id/tools { allowed: [names] }
pub async fn set_tools(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    let pick: BTreeSet<String> = body["allowed"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let admins = super::tools::admin_count(&state, &claims.team_id).await;
    tools::save_allowed(&mut s, &pick, &claims.email.to_ascii_lowercase(), admins);
    rt.store
        .put_server(&rt.team, &s)
        .await
        .map_err(|e| internal("save mcp tools", e))?;
    info!(team = %rt.team, server = %s.id, allowed = s.allowed.len(), waiting = tools::awaiting_approval(&s).len(), "MCP tools picked");
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// POST /api/mcp-servers/:id/approve — a second admin approves the pick.
pub async fn approve(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    let admins = super::tools::admin_count(&state, &claims.team_id).await;
    tools::approve(&mut s, &claims.email, admins).map_err(|e| fail(StatusCode::FORBIDDEN, &e))?;
    rt.store
        .put_server(&rt.team, &s)
        .await
        .map_err(|e| internal("approve mcp tools", e))?;
    info!(team = %rt.team, server = %s.id, by = %claims.email, "MCP tools approved");
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// POST /api/mcp-servers/:id/check — list the tools again with the saved
/// connection (the "check again" button).
pub async fn check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    let owner = owner_of(&s, &claims);
    let result = rt.refresh_tool_list(&mut s, &owner).await;
    let s = load(&rt, &id).await?;
    Ok(Json(json!({
        "server": reply_server(&rt, &s, &claims).await,
        "error": result.err().map(|e| e.to_string()),
    })))
}

/// PUT /api/mcp-servers/:id/prompt { custom_prompt }
pub async fn set_prompt(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let prompt = body["custom_prompt"].as_str().unwrap_or("").to_string();
    if prompt.len() > 4000 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Instructions are too long (4000 characters at most).",
        ));
    }
    let rt = runtime(&state, &claims.team_id);
    let mut s = load(&rt, &id).await?;
    s.custom_prompt = prompt;
    rt.store
        .put_server(&rt.team, &s)
        .await
        .map_err(|e| internal("save mcp prompt", e))?;
    Ok(Json(
        json!({ "server": reply_server(&rt, &s, &claims).await }),
    ))
}

/// GET /api/mcp-servers/:id/activity — the last tool calls.
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let logs = rt
        .store
        .list_logs(&rt.team, &id, 25)
        .await
        .map_err(|e| internal("list mcp activity", e))?;
    Ok(Json(
        json!({ "calls": logs.iter().map(|l| l.json()).collect::<Vec<_>>() }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_callback_and_metadata_document_follow_the_stage() {
        assert_eq!(
            redirect_uri("prod"),
            "https://app.coderhelm.com/settings/integrations/mcp-callback/"
        );
        assert!(redirect_uri("dev").starts_with("http://localhost:3000/"));
        assert_eq!(
            metadata_document_url("prod").as_deref(),
            Some("https://app.coderhelm.com/oauth/mcp-client.json")
        );
        assert!(metadata_document_url("dev").is_none());
    }

    #[test]
    fn ids_never_take_a_catalog_or_builtin_name() {
        let taken: BTreeSet<String> = ["docs".to_string()].into();
        assert_eq!(new_id("Docs", &taken), "docs-2");
        assert_eq!(new_id("Linear", &BTreeSet::new()), "linear-2");
        assert_eq!(new_id("GitHub", &BTreeSet::new()), "github-2");
        assert_eq!(new_id("Acme Wiki", &BTreeSet::new()), "acme-wiki");
        assert!(tools::valid_server_id(&new_id(
            "Ünïcode!!",
            &BTreeSet::new()
        )));
    }

    #[test]
    fn header_names_are_checked() {
        assert!(valid_header_name("Authorization"));
        assert!(valid_header_name("X-API-Key"));
        for bad in ["", "Host", "Mcp-Session-Id", "X Key", "X-Key\r\nEvil: 1"] {
            assert!(!valid_header_name(bad), "{bad}");
        }
    }

    #[test]
    fn detection_replies_carry_nothing_secret() {
        let d = Detected::Header;
        assert_eq!(detected_json(&d)["auth"], "header");
    }
}
