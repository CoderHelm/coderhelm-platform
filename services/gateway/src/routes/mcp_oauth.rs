//! The HTTP side of signing in to CoderHelm's MCP server (`crate::mcp_oauth`
//! has the rules):
//!
//! - public, for MCP clients: the two metadata documents, `POST
//!   /oauth/register`, `GET /oauth/authorize`, `POST /oauth/token` and `POST
//!   /oauth/revoke`;
//! - for the dashboard, with the person's session: the consent page's
//!   `POST /api/mcp/authorize/describe` and `POST /api/mcp/authorize`, and
//!   the Connected apps page's `GET /api/mcp/connections` and
//!   `DELETE /api/mcp/connections/:id`.
//!
//! Everyone on a team may connect an app as themselves: what the app can do
//! is what they can do, checked on every call. Each person sees and revokes
//! their own connections; people who manage the team's API keys see and
//! revoke everyone's.

use axum::{
    extract::{Extension, Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Form, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::jwt::SignInProof;
use crate::mcp_oauth::{self as oauth, OAuthStore, StoredGrant, READ_SCOPE, SCOPES};
use crate::middleware::rate_limit::{self, ClientIp};
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

fn oauth_error(status: StatusCode, error: &str, description: &str) -> Response {
    (
        status,
        Json(json!({ "error": error, "error_description": description })),
    )
        .into_response()
}

fn unavailable() -> Response {
    oauth_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "temporarily_unavailable",
        "CoderHelm couldn't answer just now. Try again shortly.",
    )
}

fn too_many() -> Response {
    let mut r = oauth_error(
        StatusCode::TOO_MANY_REQUESTS,
        "slow_down",
        "Too many requests from this address. Try again in a few minutes.",
    );
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("300"));
    r
}

// ── Metadata ────────────────────────────────────────────────────────

/// The protected resource metadata (RFC 9728), for `/mcp`.
pub fn resource_metadata(issuer: &str) -> Value {
    json!({
        "resource": oauth::resource_for(issuer),
        "authorization_servers": [issuer],
        "scopes_supported": SCOPES,
        "bearer_methods_supported": ["header"],
        "resource_name": "CoderHelm",
        "resource_documentation": "https://docs.coderhelm.com/mcp",
    })
}

/// The authorization server metadata (RFC 8414).
pub fn server_metadata(issuer: &str) -> Value {
    json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/oauth/authorize"),
        "token_endpoint": format!("{issuer}/oauth/token"),
        "registration_endpoint": format!("{issuer}/oauth/register"),
        "revocation_endpoint": format!("{issuer}/oauth/revoke"),
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        // client_credentials: knowledge keys only (crate::knowledge_keys),
        // for programs such as AgentCore Gateway; people sign in with a code.
        "grant_types_supported": ["authorization_code", "refresh_token", "client_credentials"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_basic", "client_secret_post"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": SCOPES,
        "authorization_response_iss_parameter_supported": true,
        "service_documentation": "https://docs.coderhelm.com/mcp",
    })
}

/// GET /.well-known/oauth-protected-resource and .../mcp
pub async fn protected_resource() -> Json<Value> {
    Json(resource_metadata(&oauth::issuer()))
}

/// GET /.well-known/oauth-authorization-server
pub async fn authorization_server() -> Json<Value> {
    Json(server_metadata(&oauth::issuer()))
}

// ── Registration ────────────────────────────────────────────────────

/// POST /oauth/register — dynamic client registration (RFC 7591). Open to
/// anyone, as MCP clients need; 20 a day per address, and a registration
/// alone can do nothing: every sign-in still asks the person.
pub async fn register(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    body: axum::body::Bytes,
) -> Response {
    if rate_limit::allow_shared(&state, "mcp_register", &ip.0, 20, 86_400)
        .await
        .is_err()
    {
        return too_many();
    }
    let Ok(reg) = serde_json::from_slice::<oauth::Registration>(&body) else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client_metadata",
            "The body must be a JSON object (RFC 7591).",
        );
    };
    let now = chrono::Utc::now();
    let client = match oauth::register(&reg, now) {
        Ok(c) => c,
        Err((code, msg)) => return oauth_error(StatusCode::BAD_REQUEST, code, &msg),
    };
    if let Err(e) = oauth::store(&state).put_client(&client).await {
        error!(error = %e, "MCP client not registered");
        return unavailable();
    }
    info!(client_id = %client.client_id, client_name = %client.client_name, "MCP client registered");
    (
        StatusCode::CREATED,
        Json(oauth::registration_answer(&client, now.timestamp())),
    )
        .into_response()
}

// ── Authorization ───────────────────────────────────────────────────

/// A refusal shown to the person, when the app can't be trusted with it.
fn refusal_page(message: &str) -> Response {
    let escaped = message
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>CoderHelm</title></head>\
         <body style=\"font-family:system-ui,sans-serif;background:#09090b;color:#e4e4e7;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0;padding:16px\">\
         <main style=\"max-width:28rem\"><h1 style=\"font-size:1.25rem\">CoderHelm couldn't connect this app</h1><p style=\"color:#a1a1aa;line-height:1.5\">{escaped}</p></main></body></html>"
    );
    let mut r = (StatusCode::BAD_REQUEST, html).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    r
}

/// GET /oauth/authorize — checks the app's request and sends the person to
/// the dashboard's consent page, with the request signed into the link.
pub async fn authorize(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    Query(p): Query<oauth::AuthorizeParams>,
) -> Response {
    if rate_limit::allow(&ip, "mcp_authorize", 60, 300).is_err() {
        return refusal_page(
            "Too many sign-in attempts from this address. Try again in a few minutes.",
        );
    }
    let client = match p.client_id.as_deref() {
        Some(id) => match oauth::store(&state).get_client(id).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "MCP client not read");
                return refusal_page("CoderHelm couldn't answer just now. Try again shortly.");
            }
        },
        None => None,
    };
    let issuer = oauth::issuer();
    let req = match oauth::check_authorize(&p, client.as_ref(), &oauth::resource_for(&issuer)) {
        Ok(r) => r,
        Err(e) => {
            return match oauth::error_redirect(&e, &issuer) {
                Some(url) => Redirect::to(&url).into_response(),
                None => match e {
                    oauth::AuthorizeError::Page(m) => refusal_page(&m),
                    _ => refusal_page("The app's request is not valid."),
                },
            }
        }
    };
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let Ok(signed) = oauth::sign_request(&req, &state.secrets.jwt_secret, now) else {
        return refusal_page("CoderHelm couldn't answer just now. Try again shortly.");
    };
    let app = common::public_urls::app_url_from_env();
    Redirect::to(&format!(
        "{app}/authorize/?request={}",
        urlencoding::encode(&signed)
    ))
    .into_response()
}

// ── Tokens ──────────────────────────────────────────────────────────

/// POST /oauth/token (form-encoded, RFC 6749 3.2).
pub async fn token(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    headers: axum::http::HeaderMap,
    Form(form): Form<oauth::TokenForm>,
) -> Response {
    if rate_limit::allow_shared(&state, "mcp_token", &ip.0, 120, 300)
        .await
        .is_err()
    {
        return too_many();
    }
    if form.grant_type.as_deref() == Some("client_credentials") {
        return knowledge_key_token(&state, &headers, &form).await;
    }
    let store = oauth::store(&state);
    match oauth::token(
        &store,
        &form,
        &oauth::issuer(),
        &state.secrets.jwt_secret,
        chrono::Utc::now(),
    )
    .await
    {
        Ok(Ok(t)) => {
            let mut r = Json(t).into_response();
            r.headers_mut()
                .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
            r
        }
        Ok(Err(e)) => oauth_error(StatusCode::BAD_REQUEST, e.error, &e.description),
        Err(e) => {
            error!(error = %e, "MCP token request failed");
            unavailable()
        }
    }
}

/// `grant_type=client_credentials` for a knowledge key (`crate::knowledge_keys`).
async fn knowledge_key_token(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    form: &oauth::TokenForm,
) -> Response {
    use crate::knowledge_keys as keys;
    let basic = keys::basic_credentials(headers);
    let (id, secret) = match &basic {
        Some((i, s)) => (Some(i.as_str()), Some(s.as_str())),
        None => (form.client_id.as_deref(), form.client_secret.as_deref()),
    };
    match keys::client_credentials(
        &keys::store(state),
        id,
        secret,
        form.scope.as_deref(),
        &oauth::issuer(),
        &state.secrets.jwt_secret,
        chrono::Utc::now(),
    )
    .await
    {
        Ok(Ok(t)) => {
            let mut r = Json(t).into_response();
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r.headers_mut()
                .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
            r
        }
        Ok(Err((code, msg))) => {
            let status = if code == "invalid_client" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_REQUEST
            };
            oauth_error(status, code, msg)
        }
        Err(e) => {
            error!(error = %e, "Knowledge key token request failed");
            unavailable()
        }
    }
}

#[derive(Deserialize, Default)]
pub struct RevokeForm {
    #[serde(default)]
    token: String,
}

/// POST /oauth/revoke (RFC 7009): always `200`, whatever the token was.
pub async fn revoke(
    State(state): State<Arc<AppState>>,
    ip: ClientIp,
    Form(form): Form<RevokeForm>,
) -> Response {
    if rate_limit::allow_shared(&state, "mcp_token", &ip.0, 120, 300)
        .await
        .is_err()
    {
        return too_many();
    }
    match oauth::revoke_token(
        &oauth::store(&state),
        form.token.trim(),
        &state.secrets.jwt_secret,
        chrono::Utc::now(),
    )
    .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            error!(error = %e, "MCP token not revoked");
            unavailable()
        }
    }
}

// ── The consent page (dashboard, signed in) ─────────────────────────

#[derive(Deserialize)]
pub struct RequestBody {
    request: String,
}

/// The request and its app, when the link is ours, current, and the app is
/// still registered.
async fn pending(
    state: &AppState,
    signed: &str,
) -> Result<(oauth::PendingRequest, oauth::StoredClient), ApiError> {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let req = oauth::read_request(signed, &state.secrets.jwt_secret, now).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "This link has expired or isn't valid. Go back to the app and connect again.",
        )
    })?;
    let client = oauth::store(state)
        .get_client(&req.client_id)
        .await
        .map_err(|e| internal("MCP client not read", e))?
        .ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "This app's registration has expired. Go back to the app and connect again.",
            )
        })?;
    Ok((req, client))
}

/// Only a person's own session decides: never a support view or an API key.
fn own_session(claims: &Claims) -> Result<(), ApiError> {
    if claims.support_view.is_some() || claims.api_key.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the person signing in can connect an app.",
        ));
    }
    Ok(())
}

/// POST /api/mcp/authorize/describe — what the consent page shows.
pub async fn describe_request(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RequestBody>,
) -> Result<Json<Value>, ApiError> {
    own_session(&claims)?;
    let (req, client) = pending(&state, &body.request).await?;
    let holder = claims.holder();
    let scopes: Vec<Value> = SCOPES
        .iter()
        .map(|s| {
            json!({
                "key": s,
                "label": oauth::scope_label(s),
                "requested": req.scopes.iter().any(|r| r == s),
                "always": *s == READ_SCOPE,
            })
        })
        .collect();
    Ok(Json(json!({
        "client_id": client.client_id,
        "client_name": client.client_name,
        "client_uri": client.client_uri,
        "redirect_host": oauth::redirect_host(&req.redirect_uri),
        "registered_at": client.created_at,
        "scopes": scopes,
        "email": claims.email,
        "team_id": claims.team_id,
        // Acting needs a role that can act; the page says so instead of
        // letting someone grant what they can't use.
        "can_act": holder.holds("runs") || holder.holds("plans") || holder.holds("reviews"),
        "mcp_url": oauth::resource(),
    })))
}

#[derive(Deserialize)]
pub struct DecisionBody {
    request: String,
    approve: bool,
    #[serde(default)]
    allow_write: bool,
    /// Searching the knowledge bases the person can read (`knowledge:read`).
    #[serde(default)]
    allow_knowledge: bool,
}

/// POST /api/mcp/authorize — the person's answer. Gives back where to send
/// their browser: the app, with a code or with `access_denied`.
pub async fn decide_request(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
    Json(body): Json<DecisionBody>,
) -> Result<Json<Value>, ApiError> {
    own_session(&claims)?;
    let (req, client) = pending(&state, &body.request).await?;
    let issuer = oauth::issuer();
    let mut params: Vec<(&str, String)> = Vec::new();
    if !body.approve {
        params.push(("error", "access_denied".into()));
        params.push((
            "error_description",
            "The person said no in CoderHelm.".into(),
        ));
    } else {
        let proof = proof.map(|p| p.0).unwrap_or_default();
        let who = oauth::Consent {
            team_id: claims.team_id.clone(),
            user_id: claims.sub.clone(),
            email: claims.email.clone(),
            mfa: proof.mfa,
            via: proof.via.clone(),
            allow_write: body.allow_write,
            allow_knowledge: body.allow_knowledge,
        };
        let (code, stored) = oauth::approve(&req, &client, &who, chrono::Utc::now());
        // Written before the code goes out: no connection without a record.
        roles::record(
            &state,
            &claims,
            AuditEntry {
                action: "mcp_connected",
                role_id: &client.client_id,
                role_name: &client.client_name,
                detail: format!(
                    "Connected {} ({}) to use CoderHelm as them: {}.",
                    client.client_name,
                    oauth::redirect_host(&req.redirect_uri),
                    stored
                        .scopes
                        .iter()
                        .map(|s| oauth::scope_label(s).to_lowercase())
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            },
        )
        .await?;
        oauth::store(&state)
            .put_code(&stored)
            .await
            .map_err(|e| internal("MCP code not saved", e))?;
        params.push(("code", code));
    }
    if let Some(s) = &req.state {
        params.push(("state", s.clone()));
    }
    params.push(("iss", issuer));
    let pairs: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
    Ok(Json(json!({
        "redirect_to": oauth::redirect_with(&req.redirect_uri, &pairs),
    })))
}

// ── Connected apps (dashboard, signed in) ───────────────────────────

/// Sees and revokes everyone's connections: the people who manage the
/// team's API keys.
fn manages_all(claims: &Claims) -> bool {
    claims.support_view.is_none() && claims.holder().holds("api_keys")
}

/// A connection as the page shows it. Never a token or its hash.
pub fn connection_view(g: &StoredGrant, me: &str, now: chrono::DateTime<chrono::Utc>) -> Value {
    let expired =
        chrono::DateTime::parse_from_rfc3339(&g.refresh_expires_at).is_ok_and(|t| t <= now);
    let status = if g.revoked_at.is_some() {
        "revoked"
    } else if expired {
        "expired"
    } else {
        "active"
    };
    json!({
        "id": g.id,
        "client_name": g.client_name,
        "redirect_host": g.redirect_host,
        "scopes": g.scopes,
        "email": g.email,
        "mine": g.user_id == me,
        "created_at": g.created_at,
        "last_used_at": g.last_used_at,
        "revoked_at": g.revoked_at,
        "status": status,
    })
}

/// GET /api/mcp/connections — this person's connected apps (everyone's, for
/// people who manage API keys), and the address to add.
pub async fn list_connections(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    let all = manages_all(&claims);
    let now = chrono::Utc::now();
    let grants = oauth::store(&state)
        .list_grants(&claims.team_id)
        .await
        .map_err(|e| internal("Connected apps not listed", e))?;
    let connections: Vec<Value> = grants
        .iter()
        .filter(|g| all || g.user_id == claims.sub)
        .map(|g| connection_view(g, &claims.sub, now))
        .collect();
    Ok(Json(json!({
        "connections": connections,
        "team_wide": all,
        "mcp_url": oauth::resource(),
        "scopes": SCOPES.iter().map(|s| json!({ "key": s, "label": oauth::scope_label(s) })).collect::<Vec<_>>(),
    })))
}

/// DELETE /api/mcp/connections/:id — ends a connection: its tokens stop at
/// once. Your own, or anyone's for people who manage API keys.
pub async fn revoke_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    own_session(&claims)?;
    let store = oauth::store(&state);
    let grant = store
        .get_grant(&id)
        .await
        .map_err(|e| internal("Connected app not read", e))?
        .filter(|g| g.team_id == claims.team_id && g.revoked_at.is_none())
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That app isn't connected."))?;
    if grant.user_id != claims.sub && !manages_all(&claims) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "You can remove only the apps you connected. Ask an admin to remove someone else's.",
        ));
    }
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "mcp_revoked",
            role_id: &grant.id,
            role_name: &grant.client_name,
            detail: format!(
                "Removed {} ({}), connected by {}.",
                grant.client_name, grant.redirect_host, grant.email
            ),
        },
    )
    .await?;
    let at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let done = store
        .revoke_grant(&claims.team_id, &id, &claims.display_name(), &at)
        .await
        .map_err(|e| internal("Connected app not removed", e))?;
    if !done {
        return Err(fail(StatusCode::NOT_FOUND, "That app isn't connected."));
    }
    Ok(Json(json!({ "status": "revoked", "id": id })))
}

/// Is this request headed for one of the public sign-in routes? (For CORS.)
pub fn is_public_oauth_path(path: &str) -> bool {
    matches!(path, "/oauth/register" | "/oauth/token" | "/oauth/revoke")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_oauth::tests::{connect, Mem};

    #[test]
    fn the_metadata_points_clients_at_this_install() {
        let r = resource_metadata("https://ch.example.org");
        assert_eq!(r["resource"], "https://ch.example.org/mcp");
        assert_eq!(r["authorization_servers"][0], "https://ch.example.org");
        assert_eq!(
            r["scopes_supported"],
            json!(["runs:read", "runs:write", "knowledge:read"])
        );
        let s = server_metadata("https://ch.example.org");
        assert_eq!(s["issuer"], "https://ch.example.org");
        assert_eq!(
            s["registration_endpoint"],
            "https://ch.example.org/oauth/register"
        );
        assert_eq!(s["code_challenge_methods_supported"], json!(["S256"]));
        assert_eq!(
            s["token_endpoint_auth_methods_supported"],
            json!(["none", "client_secret_basic", "client_secret_post"])
        );
        assert_eq!(
            s["grant_types_supported"],
            json!(["authorization_code", "refresh_token", "client_credentials"])
        );
    }

    #[tokio::test]
    async fn a_connection_shows_no_token_and_its_status() {
        let mem = Mem::default();
        connect(&mem, "TEAM#a", "USER#ann", true).await;
        let g = mem.list_grants("TEAM#a").await.unwrap().remove(0);
        let v = connection_view(&g, "USER#ann", chrono::Utc::now());
        assert_eq!(v["status"], "active");
        assert_eq!(v["mine"], true);
        assert!(v.get("refresh_hash").is_none());
        let text = v.to_string();
        assert!(!text.contains(&g.refresh_hash));
        let v = connection_view(
            &g,
            "USER#bob",
            chrono::Utc::now() + chrono::Duration::days(31),
        );
        assert_eq!(v["status"], "expired");
        assert_eq!(v["mine"], false);
    }

    fn claims(role: &str) -> Claims {
        Claims {
            sub: "USER#me".into(),
            team_id: "TEAM#a".into(),
            email: "me@example.com".into(),
            role: role.into(),
            github_login: None,
            aud: "coderhelm-dashboard".into(),
            exp: 0,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
        }
    }

    #[test]
    fn only_key_managers_see_everyones_connections() {
        assert!(manages_all(&claims("admin")));
        assert!(manages_all(&claims("owner")));
        assert!(!manages_all(&claims("member")));
        assert!(!manages_all(&claims("viewer")));
        assert!(own_session(&claims("member")).is_ok());
        let mut c = claims("admin");
        c.api_key = Some(crate::models::ApiKeyRef {
            id: "k".into(),
            name: "k".into(),
            scopes: vec![],
            rate_limit_per_minute: 1,
            expires_at: None,
            created_by: String::new(),
        });
        assert!(own_session(&c).is_err(), "an API key never connects apps");
        assert!(is_public_oauth_path("/oauth/token"));
        assert!(!is_public_oauth_path("/oauth/authorize"));
    }
}
