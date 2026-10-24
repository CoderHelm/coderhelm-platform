use crate::models::Claims;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

pub fn create_token(
    user_id: &str,
    team_id: &str,
    email: &str,
    role: &str,
    github_login: Option<&str>,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now().timestamp() as u64;
    let claims = Claims {
        sub: user_id.to_string(),
        team_id: team_id.to_string(),
        email: email.to_string(),
        role: role.to_string(),
        github_login: github_login.map(|s| s.to_string()),
        aud: "coderhelm-dashboard".to_string(),
        iat: now,
        exp: now + ttl_secs,
        support_view: None,
        custom: None,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// Audience of the brain chat's short-lived stream token: accepted only by
/// `/api/brain/stream`, never as a session or a plan chat token.
pub const BRAIN_AUDIENCE: &str = "coderhelm-brain";

/// A short-lived token for `audience` carrying the member's stored role.
pub fn create_scoped_token(
    claims: &Claims,
    audience: &str,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now().timestamp() as u64;
    let scoped = Claims {
        sub: claims.sub.clone(),
        team_id: claims.team_id.clone(),
        email: claims.email.clone(),
        role: claims.session_role().to_string(),
        github_login: claims.github_login.clone(),
        aud: audience.to_string(),
        iat: now,
        exp: now + ttl_secs,
        support_view: None,
        custom: None,
    };
    encode(
        &Header::default(),
        &scoped,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// A token minted by [`create_scoped_token`] for `audience`.
pub fn validate_scoped_token(
    token: &str,
    audience: &str,
    secret: &str,
) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.set_audience(&[audience]);
    Ok(decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?
    .claims)
}

pub fn validate_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.set_audience(&["coderhelm-dashboard"]);
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?;
    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_jwt() {
        let secret = "test-jwt-secret-at-least-32-chars!";
        let token = create_token(
            "user-1",
            "team-1",
            "user@example.com",
            "owner",
            Some("octocat"),
            secret,
            3600,
        )
        .unwrap();
        let claims = validate_token(&token, secret).unwrap();
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.team_id, "team-1");
        assert_eq!(claims.email, "user@example.com");
        assert_eq!(claims.role, "owner");
        assert_eq!(claims.github_login.as_deref(), Some("octocat"));
    }

    #[test]
    fn a_brain_token_is_not_a_session_and_a_session_is_not_a_brain_token() {
        let secret = "test-jwt-secret-at-least-32-chars!";
        let session = create_token("u", "t", "e@x.com", "member", None, secret, 300).unwrap();
        let claims = validate_token(&session, secret).unwrap();
        let brain = create_scoped_token(&claims, BRAIN_AUDIENCE, secret, 300).unwrap();
        assert!(validate_token(&brain, secret).is_err());
        assert!(validate_scoped_token(&session, BRAIN_AUDIENCE, secret).is_err());
        let back = validate_scoped_token(&brain, BRAIN_AUDIENCE, secret).unwrap();
        assert_eq!((back.sub.as_str(), back.role.as_str()), ("u", "member"));
    }

    #[test]
    fn test_invalid_token_rejected() {
        let result = validate_token("not.a.valid.token", "secret");
        assert!(result.is_err());
    }
}
