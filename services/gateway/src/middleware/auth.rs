use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

use crate::auth::jwt;
use crate::AppState;

/// Session TTL: 7 days. Used by both token creation and cookie Max-Age.
pub const SESSION_TTL_SECS: u64 = 604_800;

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

    let claims = jwt::validate_token(token, &state.secrets.jwt_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    // Check if token needs refresh (sliding session)
    let now = chrono::Utc::now().timestamp() as u64;
    let token_age = now.saturating_sub(claims.iat);
    let needs_refresh = token_age > SESSION_TTL_SECS / 2;

    req.extensions_mut().insert(claims.clone());
    let mut response = next.run(req).await;

    if needs_refresh {
        if let Ok(new_token) = jwt::create_token(
            &claims.sub,
            &claims.team_id,
            &claims.email,
            &claims.role,
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
