use axum::{
    extract::{MatchedPath, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::api_keys::{self, Outcome, RateWindow};
use crate::auth::jwt;
use crate::models::{ApiKeyRef, Claims};
use crate::openapi;
use crate::permissions::{self, Access, Capability};
use crate::routes::{roles, sign_in, support_access};
use crate::AppState;

/// Session TTL: 7 days. Used by both token creation and cookie Max-Age.
pub const SESSION_TTL_SECS: u64 = 604_800;

/// How often a member's "last seen" is written while they use the app. A
/// session renews itself, so people who stay signed in never sign in again:
/// activity, not sign-ins, says who uses CoderHelm.
const SEEN_EVERY: Duration = Duration::from_secs(3600);

/// Extract and validate JWT from cookie, inject Claims into request extensions.
/// Implements a sliding session: if the token is past 50% of its lifetime,
/// a fresh token + cookie is issued automatically so active users stay logged in.
pub async fn require_auth(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // A team API key: its own path, and never a session's (no cookies, no
    // support view, no sliding refresh).
    if let Some(token) = api_keys::bearer_key(req.headers()).map(str::to_string) {
        return Ok(api_key_request(&state, req, next, &token).await);
    }

    let cookie_header = req
        .headers()
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let token = extract_cookie(cookie_header, "__Host-coderhelm_session")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let mut claims = jwt::validate_token(token, &state.secrets.jwt_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    // Only a support-view token carries a support view; a session never does.
    claims.support_view = None;

    // A support view: the platform admin's own session plus a support-view
    // cookie. Enforced here for every route: reads only, every request
    // written to the team's audit log, and the team's grant checked on each
    // request (cached at most a minute). Leaving the view always works.
    if let Some(support_token) =
        extract_cookie(cookie_header, support_access::SUPPORT_COOKIE).filter(|t| !t.is_empty())
    {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        if !support_access::is_exit(&method, &path) {
            let route = req
                .uri()
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| path.clone());
            let access = support_access::store(&state);
            let Some(sv) = support_access::check_session(
                &access,
                &claims,
                support_token,
                &state.secrets.jwt_secret,
            )
            .await
            else {
                return Ok(support_access::refused(
                    "Support view has ended. Open it again from the ticket if the team still allows it.",
                    true,
                ));
            };
            let now = chrono::Utc::now().timestamp().max(0) as u64;
            if let Some(why) = support_access::support_view_refusal(&method, &path) {
                if let Err(e) =
                    support_access::record(&access, &sv, "refused", &method, &route, now).await
                {
                    tracing::warn!(error = %e, "Support view audit not written");
                }
                return Ok(support_access::refused(why, false));
            }
            // A read is served only once it is in the team's log.
            if let Err(e) = support_access::record(&access, &sv, "read", &method, &route, now).await
            {
                tracing::error!(error = %e, "Support view audit not written; read refused");
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            req.extensions_mut()
                .insert(support_access::view_claims(&sv));
            let response = next.run(req).await;
            return Ok(hide_in_answer(response).await);
        }
    }

    // Check if token needs refresh (sliding session)
    let now = chrono::Utc::now().timestamp() as u64;
    let token_age = now.saturating_sub(claims.iat);
    let needs_refresh = token_age > SESSION_TTL_SECS / 2;

    // Every request is decided here, from the capability its handler belongs
    // to (`permissions::resolve_access`), for built-in and custom roles alike.
    let handler = req
        .extensions()
        .get::<MatchedPath>()
        .and_then(|m| permissions::routed_handler(req.method().as_str(), m.as_str()));
    // How this session signed in. On a self-hosted install that requires
    // two-factor sign-in, a session without a second factor reaches only the
    // screens that set one up; the rule is read on every request, so sessions
    // from before it was turned on are held at their next request.
    let proof = jwt::read_proof(token, &state.secrets.jwt_secret);
    // The session against the member's record, read on every request: a
    // removal, a role change, a two-factor reset or turning two-factor off
    // ends every session issued before it.
    if let Err(ended) = sign_in::check_member_session(&state, &claims).await {
        return Ok(ended);
    }
    if sign_in::applies() {
        if let Err(held) = sign_in::check_session(&state, &proof, handler).await {
            return Ok(held);
        }
    }

    let holder = roles::holder_for(&state, &claims).await;
    match permissions::resolve_access(&holder, handler) {
        Access::Deny { capability } => return Ok(refused_for(capability)),
        Access::Allow { effective_role } => {
            if holder.custom.is_some() {
                claims.role = effective_role.to_string();
                claims.custom = Some(holder);
            }
        }
    }

    note_activity(&state, &claims.team_id, &claims.sub).await;
    req.extensions_mut().insert(claims.clone());
    req.extensions_mut().insert(proof.clone());
    let mut response = next.run(req).await;

    // The refreshed token carries the member's CURRENT role, and a removed
    // member gets no refresh (their session runs out), so role changes and
    // removals on the team page reach existing sessions.
    let current_role = if needs_refresh {
        current_role(&state, &claims).await
    } else {
        None
    };

    if let Some(role) = current_role {
        // The refreshed session keeps how it signed in.
        if let Ok(new_token) = jwt::create_session_token(
            &claims.sub,
            &claims.team_id,
            &claims.email,
            &role,
            claims.github_login.as_deref(),
            &proof,
            &state.secrets.jwt_secret,
            SESSION_TTL_SECS,
        ) {
            let cookie = format!(
                "__Host-coderhelm_session={new_token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
            );
            if let Ok(val) = cookie.parse() {
                response.headers_mut().append("set-cookie", val);
            }
        }
    }

    Ok(response)
}

/// A request made with a team API key. The key is checked and counted
/// against its rate limit; only the public routes are open to it; and what it
/// may do there is decided by `permissions::resolve_access` from its scopes,
/// cut down to what the admin who made it holds now. Every error it gets has
/// a JSON body with `error` and `code`.
async fn api_key_request(
    state: &Arc<AppState>,
    mut req: Request,
    next: Next,
    token: &str,
) -> Response {
    let now = chrono::Utc::now();
    let store = api_keys::store(state);
    let accepted = match api_keys::authenticate(&store, token, now).await {
        Ok(Outcome::Accepted(a)) => *a,
        Ok(Outcome::Refused(r)) => {
            return key_error(StatusCode::UNAUTHORIZED, r.code(), r.message(), None)
        }
        Ok(Outcome::RateLimited(w)) => {
            let mut res = key_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                &format!(
                    "This API key may make {} requests a minute. Try again in {} seconds.",
                    w.limit, w.reset_secs
                ),
                None,
            );
            if let Ok(v) = w.reset_secs.to_string().parse() {
                res.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
            }
            return with_rate_headers(res, &w);
        }
        Err(e) => {
            tracing::error!(error = %e, "API key check failed");
            return key_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "CoderHelm couldn't check this API key just now. Try again shortly.",
                None,
            );
        }
    };
    let (key, maker, rate) = (accepted.key, accepted.maker, accepted.rate);

    let method = req.method().as_str().to_ascii_uppercase();
    let handler = req
        .extensions()
        .get::<MatchedPath>()
        .and_then(|m| permissions::routed_handler(&method, m.as_str()));
    if !openapi::is_key_route(&method, handler) {
        return with_rate_headers(
            key_error(
                StatusCode::FORBIDDEN,
                "not_available_to_api_keys",
                "API keys can call only the public endpoints listed at https://docs.coderhelm.com/api. This one is for the dashboard.",
                None,
            ),
            &rate,
        );
    }

    // What the key's maker holds right now (their custom role included).
    let maker_claims = Claims {
        sub: key.created_by.clone(),
        team_id: key.team_id.clone(),
        email: maker.email.clone(),
        role: maker.role.clone(),
        github_login: maker.github_login.clone(),
        aud: "coderhelm-dashboard".into(),
        exp: 0,
        iat: 0,
        support_view: None,
        custom: None,
        api_key: None,
    };
    let maker_holder = roles::holder_for(state, &maker_claims).await;
    let holder = api_keys::key_holder(&key, &maker_holder);
    let effective_role = match permissions::resolve_access(&holder, handler) {
        Access::Deny { capability } => {
            return with_rate_headers(
                key_error(
                    StatusCode::FORBIDDEN,
                    "missing_scope",
                    &format!(
                        "This API key doesn't have the \"{}\" scope ({}).",
                        capability.key, capability.label
                    ),
                    Some(capability.key),
                ),
                &rate,
            )
        }
        Access::Allow { effective_role } => effective_role,
    };
    let at = now.timestamp().max(0) as u64;
    let claims = Claims {
        sub: format!("APIKEY#{}", key.id),
        team_id: key.team_id.clone(),
        email: maker.email,
        role: effective_role.to_string(),
        github_login: maker.github_login,
        aud: "coderhelm-api".into(),
        exp: at,
        iat: at,
        support_view: None,
        api_key: Some(ApiKeyRef {
            id: key.id.clone(),
            name: key.name.clone(),
            scopes: holder.caps.iter().map(|c| c.to_string()).collect(),
            rate_limit_per_minute: key.rate_limit_per_minute,
            expires_at: key.expires_at.clone(),
            created_by: key.created_by_name.clone(),
        }),
        custom: Some(holder),
    };
    req.extensions_mut().insert(claims);
    let response = next.run(req).await;
    with_rate_headers(json_error_body(response).await, &rate)
}

/// An error for an API key request.
fn key_error(status: StatusCode, code: &str, message: &str, scope: Option<&str>) -> Response {
    let body = openapi::ErrorBody {
        error: message.to_string(),
        code: code.to_string(),
        scope: scope.map(str::to_string),
    };
    (status, axum::Json(body)).into_response()
}

fn with_rate_headers(mut res: Response, w: &RateWindow) -> Response {
    let h = res.headers_mut();
    for (name, value) in [
        ("ratelimit-limit", u64::from(w.limit)),
        ("ratelimit-remaining", u64::from(w.remaining)),
        ("ratelimit-reset", w.reset_secs),
    ] {
        if let Ok(v) = value.to_string().parse() {
            h.insert(name, v);
        }
    }
    res
}

/// For an API key request, an error answer always has a JSON body with
/// `error` and `code`: an empty body gets one from its status, and a JSON
/// body without `code` gets the status's code.
async fn json_error_body(response: Response) -> Response {
    let status = response.status();
    if status.as_u16() < 400 {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .unwrap_or_default();
    let mut value = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) if v.is_object() => v,
            // Plain text: keep it as the message.
            _ => serde_json::json!({ "error": String::from_utf8_lossy(&bytes).trim() }),
        }
    };
    if value["error"].as_str().is_none_or(str::is_empty) {
        value["error"] = serde_json::json!(status.canonical_reason().unwrap_or("Error"));
    }
    if value["code"].as_str().is_none() {
        value["code"] = serde_json::json!(openapi::default_code(status));
    }
    let out = serde_json::to_vec(&value).unwrap_or_default();
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    parts.headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, axum::body::Body::from(out))
}

/// The answer when someone's role doesn't include what they asked for.
fn refused_for(capability: &Capability) -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": format!(
                "Your role doesn't include this ({}). Ask an admin or owner if you need it.",
                capability.label
            ),
            "capability": capability.key,
        })),
    )
        .into_response()
}

/// A support-view answer with its credential fields hidden
/// ([`support_access::hide_secrets`]). Answers that are not JSON pass as
/// they are; a JSON answer that cannot be read is refused.
async fn hide_in_answer(response: Response) -> Response {
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, 16 * 1024 * 1024).await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    support_access::hide_secrets(&mut value);
    let out = serde_json::to_vec(&value).unwrap_or_default();
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, axum::body::Body::from(out))
}

/// Whether to write `last_seen_at` for this member now: at most once per
/// [`SEEN_EVERY`] per member in this instance (each instance writes on its own,
/// so it's at most once an hour per instance).
fn due_to_note(
    seen: &mut HashMap<(String, String), Instant>,
    key: (String, String),
    now: Instant,
) -> bool {
    match seen.get(&key) {
        Some(at) if now.duration_since(*at) < SEEN_EVERY => false,
        _ => {
            // Keep the map small in a long-lived instance.
            if seen.len() > 10_000 {
                seen.retain(|_, at| now.duration_since(*at) < SEEN_EVERY);
            }
            seen.insert(key, now);
            true
        }
    }
}

/// Records that the member is using the app (shown on the team page). One
/// attribute, only on a record that exists; a failure is ignored.
async fn note_activity(state: &AppState, team_id: &str, user_id: &str) {
    static SEEN: OnceLock<Mutex<HashMap<(String, String), Instant>>> = OnceLock::new();
    if team_id.is_empty() || user_id.is_empty() {
        return;
    }
    let due = SEEN
        .get_or_init(Default::default)
        .lock()
        .map(|mut m| {
            due_to_note(
                &mut m,
                (team_id.to_string(), user_id.to_string()),
                Instant::now(),
            )
        })
        .unwrap_or(false);
    if !due {
        return;
    }
    use aws_sdk_dynamodb::types::AttributeValue;
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(user_id.to_string()))
        .update_expression("SET last_seen_at = :now")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":now", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

/// The member's role on their record now; `None` when they are no longer on
/// the team (or the lookup failed, in which case the old token simply runs on).
async fn current_role(state: &AppState, claims: &crate::models::Claims) -> Option<String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(claims.team_id.clone()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(claims.sub.clone()),
        )
        .send()
        .await
        .ok()?
        .item?;
    if item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        == Some("invited")
    {
        return None;
    }
    Some(
        item.get("role")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "member".to_string()),
    )
}

fn extract_cookie<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header
        .split(';')
        .map(|c| c.trim())
        .find(|c| c.starts_with(&format!("{name}=")))
        .and_then(|c| c.split_once('='))
        .map(|(_, v)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_is_noted_once_an_hour_per_member() {
        let mut seen = HashMap::new();
        let t0 = Instant::now();
        let a = || ("TEAM#1".to_string(), "u1".to_string());
        assert!(due_to_note(&mut seen, a(), t0));
        assert!(!due_to_note(&mut seen, a(), t0 + Duration::from_secs(60)));
        assert!(due_to_note(&mut seen, ("TEAM#1".into(), "u2".into()), t0));
        assert!(due_to_note(
            &mut seen,
            a(),
            t0 + SEEN_EVERY + Duration::from_secs(1)
        ));
    }

    /// The middleware finds the routed handler from the matched path it sees
    /// as a layer of the nested API router, the way main.rs builds it.
    #[tokio::test]
    async fn the_middleware_sees_which_handler_a_request_is_routed_to() {
        use axum::{body::Body, routing::put, Router};
        use tower::ServiceExt;

        async fn seen(req: Request, next: Next) -> Response {
            let handler = req
                .extensions()
                .get::<MatchedPath>()
                .and_then(|m| permissions::routed_handler(req.method().as_str(), m.as_str()))
                .unwrap_or("none")
                .to_string();
            let mut res = next.run(req).await;
            res.headers_mut()
                .insert("x-handler", handler.parse().unwrap());
            res
        }
        let api = Router::new()
            .route("/users/:user_id/role", put(|| async { "ok" }))
            .route("/roles/:role_id", put(|| async { "ok" }))
            .layer(axum::middleware::from_fn(seen));
        let app = Router::new().nest("/api", api);
        for (path, want) in [
            ("/api/users/u1/role", "users::update_role"),
            ("/api/roles/r1", "roles::update_role"),
        ] {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.headers()["x-handler"], want, "{path}");
        }
    }

    #[test]
    fn a_refusal_names_the_missing_capability() {
        let cap = permissions::capability("repos").unwrap();
        let res = refused_for(cap);
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_extract_cookie() {
        let header = "other=abc; __Host-coderhelm_session=xyz123; another=def";
        assert_eq!(
            extract_cookie(header, "__Host-coderhelm_session"),
            Some("xyz123")
        );
    }

    #[test]
    fn test_extract_cookie_missing() {
        assert_eq!(
            extract_cookie("other=abc", "__Host-coderhelm_session"),
            None
        );
    }
}
