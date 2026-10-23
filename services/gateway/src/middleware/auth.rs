use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::auth::jwt;
use crate::routes::support_access;
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

    note_activity(&state, &claims.team_id, &claims.sub).await;
    req.extensions_mut().insert(claims.clone());
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
        if let Ok(new_token) = jwt::create_token(
            &claims.sub,
            &claims.team_id,
            &claims.email,
            &role,
            claims.github_login.as_deref(),
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
