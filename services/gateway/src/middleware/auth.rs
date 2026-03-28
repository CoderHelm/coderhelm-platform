use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

use crate::auth::jwt;
use crate::models::Claims;
use crate::AppState;

/// Extract and validate JWT from cookie, inject Claims into request extensions.
/// JWT secret is loaded from AppState (sourced from Secrets Manager), never from
/// request extensions or defaults.
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

    let token = extract_cookie(cookie_header, "d3ftly_session")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let claims = jwt::validate_token(&token, &state.secrets.jwt_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
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
        let header = "other=abc; d3ftly_session=xyz123; another=def";
        assert_eq!(extract_cookie(header, "d3ftly_session"), Some("xyz123"));
    }

    #[test]
    fn test_extract_cookie_missing() {
        assert_eq!(extract_cookie("other=abc", "d3ftly_session"), None);
    }
}
