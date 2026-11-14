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
use common::mcp_remote::{oauth, own_app, tools};
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

/// Where an install's own Client ID Metadata Document is served (the API's
/// public `/integrations/` paths, which a self-hosted install's address
/// routes to the API).
pub const METADATA_DOCUMENT_PATH: &str = "/integrations/mcp-client.json";

/// Coderhelm's Client ID Metadata Document: the `client_id` a sign-in
/// server that supports them fetches to learn who is asking and where it
/// may send people back.
///
/// The hosted dashboard serves a static copy naming the hosted callback.
/// Every other install with an https address serves its own (see
/// [`metadata_document`]), naming its own callback. Without one, an install
/// falls back to dynamic registration, which some services (a hosting
/// platform's MCP server among them) accept only from apps they approved,
/// so the document is how a self-hosted install signs in to them.
pub fn metadata_document_url(stage: &str) -> Option<String> {
    let origin = dashboard_origin(stage);
    if origin == common::public_urls::stage_app_url("prod") {
        return Some(format!("{origin}/oauth/mcp-client.json"));
    }
    let api = common::public_urls::api_url(stage);
    api.starts_with("https://")
        .then(|| format!("{api}{METADATA_DOCUMENT_PATH}"))
}

/// The metadata document this install serves at [`metadata_document_url`]:
/// its `client_id` is its own address, and it names this install's callback.
/// `None` where the install serves none (the hosted one's is static).
pub fn metadata_document(stage: &str) -> Option<Value> {
    let id = metadata_document_url(stage)?;
    if !id.ends_with(METADATA_DOCUMENT_PATH) {
        return None;
    }
    let app = dashboard_origin(stage);
    Some(json!({
        "client_id": id,
        "client_name": "Coderhelm",
        "client_uri": app,
        "redirect_uris": [redirect_uri(stage)],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }))
}

/// GET /integrations/mcp-client.json — this install's Client ID Metadata
/// Document (public: sign-in servers fetch it with no sign-in).
pub async fn client_metadata_document(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    metadata_document(&state.config.stage)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
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
    // A key for a catalog server whose vendor's remote server takes one
    // (`Authorization: Bearer <key>`): connect with it instead of signing in.
    let mut catalog_key: Option<String> = None;
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
        if let Some(key) = body["key"]
            .as_str()
            .map(str::trim)
            .filter(|k| !k.is_empty())
        {
            if entry["remote_key"].as_bool() != Some(true) {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "This server doesn't take a key. Use its sign-in instead.",
                ));
            }
            catalog_key = Some(bearer(key));
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
    let detected = match &catalog_key {
        // The vendor documents the key; no need to ask the server how it
        // signs in (it would answer OAuth, which a key replaces).
        Some(_) => Detected::Header,
        None => detect(rt.http.as_ref(), &url)
            .await
            .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?,
    };
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
            let value = catalog_key.clone().unwrap_or_else(|| {
                body["header_value"]
                    .as_str()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            });
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

/// A pasted key as an `Authorization` value: `Bearer <key>`, unless the
/// person already pasted it that way.
fn bearer(key: &str) -> String {
    if key.len() > 7 && key[..7].eq_ignore_ascii_case("bearer ") {
        format!("Bearer {}", key[7..].trim())
    } else {
        format!("Bearer {key}")
    }
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

/// PUT /api/mcp-servers/apps/:catalog_id { client_id, client_secret? } — the
/// team's own OAuth app for a catalog server whose package takes a token
/// (Figma). Connecting it is then `POST /api/mcp-servers/app-<id>/connect`,
/// and removing it `DELETE /api/mcp-servers/app-<id>`. A blank secret keeps
/// the saved one for the same client id; another client id ends the old
/// sign-in.
pub async fn save_own_app(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(catalog_id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = runtime(&state, &claims.team_id);
    let s = own_app::save(
        &rt,
        &catalog_id,
        body["client_id"].as_str().unwrap_or(""),
        body["client_secret"].as_str().unwrap_or(""),
        &claims.email,
        &chrono::Utc::now().to_rfc3339(),
    )
    .await
    .map_err(|e| match e {
        own_app::SaveError::NoOwnApp => fail(
            StatusCode::NOT_FOUND,
            "This server can't use your own app. Use a key instead.",
        ),
        own_app::SaveError::Invalid(m) => fail(StatusCode::BAD_REQUEST, m),
        own_app::SaveError::NotSetUp => fail(StatusCode::BAD_REQUEST, "Paste the app's client id."),
        own_app::SaveError::Internal(e) => internal("save own app", e),
    })?;
    info!(team = %rt.team, server = %s.id, by = %claims.email, "Own app saved");
    Ok(Json(
        json!({ "own_app": own_app_json(&state, &rt, &s).await }),
    ))
}

/// A catalog server's sign-in, for its card: never the secret. `source` is
/// whose app it uses (`coderhelm` or `own`).
pub async fn own_app_json(state: &AppState, rt: &Runtime, s: &ServerRecord) -> Value {
    let token = rt
        .store
        .get_token(&rt.team, &s.id, &Owner::Team)
        .await
        .ok()
        .flatten();
    let (status, reason) = match &token {
        Some(t) => (t.status, t.status_reason.clone()),
        None => (Status::NotConnected, String::new()),
    };
    let o = s.oauth.as_ref();
    let coderhelm_id = own_app::catalog_of(&s.catalog_id)
        .map(|c| coderhelm_app(state, c).client_id)
        .unwrap_or_default();
    let source = own_app::source(s, &coderhelm_id);
    json!({
        "id": s.id,
        "source": source.as_str(),
        // CoderHelm's client id is no secret, but it is no business of the card's.
        "client_id": match source {
            own_app::Source::Own => o.map(|o| o.client_id.as_str()).unwrap_or(""),
            own_app::Source::Coderhelm => "",
        },
        "has_secret": source == own_app::Source::Own && o.is_some_and(|o| !o.client_secret_sealed.is_empty()),
        "status": status.as_str(),
        "status_reason": reason,
        "connected_by": token.as_ref().map(|t| t.connected_by.as_str()).unwrap_or(""),
        "connected_at": token.as_ref().map(|t| t.connected_at.as_str()).unwrap_or(""),
        "redirect_uri": redirect_uri(&state.config.stage),
    })
}

/// CoderHelm's own public app with the vendor of `catalog_id`, from this
/// deployment's secret (`mcp_catalog::OwnApp::coderhelm_keys`). Not set where
/// the secret has no such keys.
pub(crate) fn coderhelm_app(
    state: &AppState,
    catalog_id: &str,
) -> common::knowledge::sources::oauth::App {
    coderhelm_app_from(&state.secrets, catalog_id)
}

pub(crate) fn coderhelm_app_from(
    secrets: &crate::models::Secrets,
    catalog_id: &str,
) -> common::knowledge::sources::oauth::App {
    let Some(app) = common::mcp_catalog::own_app(catalog_id) else {
        return Default::default();
    };
    let read = |k: &str| {
        secrets
            .oauth_key(k)
            .map(|v| v.trim().to_string())
            .unwrap_or_default()
    };
    common::knowledge::sources::oauth::App {
        client_id: read(app.coderhelm_keys.0),
        client_secret: read(app.coderhelm_keys.1),
    }
}

/// Whether the one-click sign-in with CoderHelm's app works here for
/// `catalog_id`: through the hosted broker on a hybrid install that chose
/// it, or with the app in this deployment's secret.
pub async fn coderhelm_app_ready(state: &AppState, catalog_id: &str) -> bool {
    common::mcp_catalog::own_app(catalog_id).is_some()
        && (coderhelm_app(state, catalog_id).is_set()
            || super::hybrid_install::brokered_id(state, catalog_id)
                .await
                .is_some())
}

/// POST /api/mcp-servers/apps/:catalog_id/coderhelm — sign in with
/// CoderHelm's app for a catalog server whose package takes a token (Figma).
/// Makes it the team's sign-in (in place of the team's own app, if one was
/// saved) and answers where to send the browser; the dashboard's callback
/// page finishes it as for any other sign-in.
pub async fn connect_coderhelm_app(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(catalog_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    if common::mcp_catalog::own_app(&catalog_id).is_none() {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "This server can't sign in with CoderHelm's app. Use a key instead.",
        ));
    }
    // A hybrid install that chose CoderHelm's app signs in through the
    // hosted broker; anywhere else, with the app in this deployment's secret.
    let brokered = super::hybrid_install::brokered_id(&state, &catalog_id).await;
    let app = coderhelm_app(&state, &catalog_id);
    let hosted = common::hybrid::hosted_url();
    let via = match &brokered {
        Some(c) => common::knowledge::sources::oauth::Via::Broker {
            hosted: &hosted,
            install_id: &c.install_id,
            secret: &c.secret,
        },
        None => common::knowledge::sources::oauth::Via::App(&app),
    };
    let rt = runtime(&state, &claims.team_id);
    let mut s = own_app::use_coderhelm_app(
        &rt,
        &catalog_id,
        via,
        &claims.email,
        &chrono::Utc::now().to_rfc3339(),
    )
    .await
    .map_err(|e| match e {
        own_app::SaveError::NotSetUp => fail(
            StatusCode::BAD_REQUEST,
            "CoderHelm's app isn't set up on this install. Use your own app or a key instead.",
        ),
        own_app::SaveError::NoOwnApp => fail(StatusCode::NOT_FOUND, "This server has no sign-in."),
        own_app::SaveError::Invalid(m) => fail(StatusCode::BAD_REQUEST, m),
        own_app::SaveError::Internal(e) => internal("use coderhelm app", e),
    })?;
    let url = rt
        .start_sign_in(
            &mut s,
            &Owner::Team,
            &claims.sub,
            &claims.email,
            &redirect_uri(&state.config.stage),
            None,
        )
        .await
        .map_err(|e| {
            warn!(team = %rt.team, server = %s.id, error = %e, "CoderHelm app sign-in could not start");
            fail(StatusCode::BAD_REQUEST, &e)
        })?;
    info!(team = %rt.team, server = %s.id, brokered = brokered.is_some(), "CoderHelm app sign-in started");
    Ok(Json(json!({
        "authorize_url": url,
        "server_id": s.id,
        "expires_in": SIGN_IN_SECS,
        "own_app": own_app_json(&state, &rt, &s).await,
    })))
}

/// How long a started sign-in can still be finished, for the page that
/// waits on it: after this it says the sign-in timed out.
pub const SIGN_IN_SECS: i64 = common::mcp_remote::store::PENDING_TTL_SECS;

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

/// A sign-in error, plus where to create the key when the error sends the
/// person to one and the server is in the catalog.
fn with_key_help(e: &str, id: &str) -> String {
    match common::mcp_catalog::key_help(id) {
        Some(help) if e.ends_with("Use a key instead.") => format!("{e} {help}"),
        _ => e.to_string(),
    }
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
        .map_err(|e| {
            warn!(team = %rt.team, server = %s.id, error = %e, "MCP sign-in could not start");
            fail(StatusCode::BAD_REQUEST, &with_key_help(&e, &s.id))
        })?;
    info!(team = %rt.team, server = %s.id, owner = %owner.key(), "MCP sign-in started");
    Ok(Json(json!({
        "authorize_url": url,
        "server_id": s.id,
        "expires_in": SIGN_IN_SECS,
    })))
}

/// DELETE /api/mcp-servers/:id/sign-in — cancel the sign-ins this person
/// started for a connection and didn't finish (an MCP server, a catalog
/// app `app-<id>`, a knowledge source `ks-<id>`). Their `state` stops
/// working at once, so an answer that arrives later is refused. Nothing
/// that is connected changes.
pub async fn cancel_sign_in(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    claims.require_role(1).map_err(member_only)?;
    if id.is_empty() || id.len() > 200 {
        return Err(fail(StatusCode::NOT_FOUND, "That server isn't set up."));
    }
    let rt = runtime(&state, &claims.team_id);
    let cancelled = rt.end_pending(&id, None, Some(&claims.sub)).await;
    info!(team = %rt.team, server = %id, cancelled, "Sign-in cancelled");
    Ok(Json(json!({ "cancelled": cancelled })))
}

/// POST /api/mcp-servers/oauth/callback { state, code?, error?, error_description?, iss? }
///
/// The one place every OAuth sign-in that returns to the dashboard's
/// callback page finishes (MCP servers, catalog apps, knowledge sources, and
/// a hybrid install's sign-ins through CoderHelm's app). Whether it finishes
/// or not, the answer says which kind of sign-in it was (`flow`) and where
/// the page goes back to (`return_to`), so the page never guesses; a failure
/// carries the provider's own error code and words.
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
            "That sign-in link is missing its state. Start the sign-in again.",
        ));
    }
    let rt = runtime(&state, &claims.team_id);
    let text = |k: &str| body[k].as_str().filter(|e| !e.trim().is_empty());
    let answer = common::mcp_remote::runtime::Answer {
        state: st,
        code: text("code"),
        error: text("error"),
        error_description: text("error_description"),
        iss: text("iss"),
    };
    let done = match rt.finish(&answer, &claims.sub, &claims.email).await {
        Ok(d) => d,
        Err(f) => {
            warn!(
                team = %rt.team,
                flow = f.flow.map(|x| x.tag()).unwrap_or(""),
                catalog = %f.catalog_id,
                code = %f.code,
                error = %f.message,
                "OAuth sign-in didn't finish"
            );
            return Err(callback_failed(&f));
        }
    };
    let flow = done.flow.tag();
    let return_to = done.return_to.clone();
    let owner = done.owner;
    let mut s = done.server;
    // A catalog server's own-app sign-in has no tools either: its package
    // gets the token. Check the token with the vendor's API now, so a wrong
    // scope or an unpublished app shows on the card instead of in a run.
    if let Some(catalog) = own_app::catalog_of(&s.catalog_id) {
        let checked = own_app::check(&rt, &s).await;
        info!(team = %rt.team, server = %s.id, ok = checked.is_ok(), "Own app signed in");
        return Ok(Json(json!({
            "server": { "id": s.id, "name": s.name, "catalog_id": catalog },
            "tools_error": checked.err(),
            "own_app": true,
            "flow": flow,
            "return_to": return_to,
        })));
    }
    // A knowledge source's sign-in has no tools to list.
    if common::knowledge::sources::oauth::is_source_connection(&s.catalog_id) {
        info!(team = %rt.team, server = %s.id, "Knowledge source connected");
        return Ok(Json(json!({
            "server": { "id": s.id, "name": s.name, "catalog_id": s.catalog_id },
            "tools_error": Value::Null,
            "flow": flow,
            "return_to": return_to,
        })));
    }
    // List the tools now so the picker has them. A failure here leaves the
    // connection saved; the status says why.
    let listed = rt.refresh_tool_list(&mut s, &owner).await;
    if let Err(e) = &listed {
        info!(team = %rt.team, server = %s.id, error = %e, "Tools not listed after sign-in");
    }
    let s = load(&rt, &s.id).await?;
    info!(team = %rt.team, server = %s.id, owner = %owner.key(), "MCP server connected");
    Ok(Json(json!({
        "server": reply_server(&rt, &s, &claims).await,
        "tools_error": listed.err().map(|e| e.to_string()),
        "flow": flow,
        "return_to": return_to,
    })))
}

/// The callback's answer for a sign-in that didn't finish: the words, the
/// provider's code, and where it came from, so the page shows the right
/// title and sends the person back to the page that started it.
pub fn callback_failed(f: &common::mcp_remote::runtime::Failed) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": f.message,
            "code": f.code,
            "flow": f.flow.map(|x| x.tag()),
            "return_to": f.return_to,
            "server_name": f.server_name,
            "catalog_id": f.catalog_id,
        })),
    )
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
    fn a_self_hosted_install_serves_its_own_metadata_document() {
        common::public_urls::override_for_this_thread(Some(common::public_urls::Override {
            urls: common::public_urls::from_single("https://coderhelm.example.com"),
            site: None,
            self_hosted: true,
        }));
        let id = metadata_document_url("prod").unwrap();
        assert_eq!(
            id,
            "https://coderhelm.example.com/integrations/mcp-client.json"
        );
        let doc = metadata_document("prod").unwrap();
        assert_eq!(doc["client_id"], id.as_str());
        assert_eq!(
            doc["redirect_uris"],
            json!(["https://coderhelm.example.com/settings/integrations/mcp-callback/"])
        );
        assert_eq!(doc["token_endpoint_auth_method"], "none");
        common::public_urls::override_for_this_thread(None);
        // The hosted dashboard keeps its static copy and serves none here.
        assert!(metadata_document("prod").is_none());
    }

    #[test]
    fn a_refused_sign_in_says_where_to_create_the_key() {
        let e = common::mcp_remote::oauth::registration_refused(403, &json!({}));
        let m = with_key_help(&e, "figma");
        assert!(m.contains("Personal access tokens"), "{m}");
        assert!(m.contains("https://help.figma.com/"), "{m}");
        // Errors that don't send the person to a key are left alone.
        assert_eq!(with_key_help("Try again.", "figma"), "Try again.");
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
    fn a_pasted_key_becomes_one_bearer_value() {
        assert_eq!(bearer("lin_api_abc"), "Bearer lin_api_abc");
        assert_eq!(bearer("Bearer lin_api_abc"), "Bearer lin_api_abc");
        assert_eq!(bearer("bearer  lin_api_abc"), "Bearer lin_api_abc");
    }

    #[test]
    fn catalog_keys_go_only_to_servers_that_take_them() {
        // Every catalog entry that takes a key at its remote server has one,
        // and the dashboard is told so in the catalog it reads.
        let json = super::super::plugins::catalog_json();
        for e in &json {
            if e["remote_key"].as_bool() == Some(true) {
                assert!(!e["remote_url"].as_str().unwrap_or("").is_empty(), "{e}");
            }
        }
        assert!(json
            .iter()
            .any(|e| e["id"] == "linear" && e["remote_key"] == true));
        assert!(json
            .iter()
            .any(|e| e["id"] == "vercel" && e["remote_key"] == false));
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

    #[test]
    fn a_failed_callback_says_where_it_came_from() {
        let f = common::mcp_remote::runtime::Failed {
            message: "Confluence didn't grant access (access_denied).".into(),
            code: "access_denied".into(),
            flow: Some(oauth::Flow::Knowledge),
            return_to: "/knowledge/".into(),
            server_name: "Confluence".into(),
            catalog_id: "source:confluence".into(),
        };
        let (status, Json(v)) = callback_failed(&f);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            v,
            json!({
                "error": "Confluence didn't grant access (access_denied).",
                "code": "access_denied",
                "flow": "ks",
                "return_to": "/knowledge/",
                "server_name": "Confluence",
                "catalog_id": "source:confluence",
            })
        );
        let unmatched = common::mcp_remote::runtime::Failed { flow: None, ..f };
        assert_eq!(callback_failed(&unmatched).1 .0["flow"], Value::Null);
    }
}
