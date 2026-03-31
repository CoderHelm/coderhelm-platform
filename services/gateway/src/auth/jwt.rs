use crate::models::Claims;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

pub fn create_token(
    user_id: &str,
    tenant_id: &str,
    email: &str,
    role: &str,
    github_login: Option<&str>,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now().timestamp() as u64;
    let claims = Claims {
        sub: user_id.to_string(),
        tenant_id: tenant_id.to_string(),
        email: email.to_string(),
        role: role.to_string(),
        github_login: github_login.map(|s| s.to_string()),
        iat: now,
        exp: now + ttl_secs,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

pub fn validate_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
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
            "tenant-1",
            "user@example.com",
            "owner",
            Some("octocat"),
            secret,
            3600,
        )
        .unwrap();
        let claims = validate_token(&token, secret).unwrap();
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.tenant_id, "tenant-1");
        assert_eq!(claims.email, "user@example.com");
        assert_eq!(claims.role, "owner");
        assert_eq!(claims.github_login.as_deref(), Some("octocat"));
    }

    #[test]
    fn test_invalid_token_rejected() {
        let result = validate_token("not.a.valid.token", "secret");
        assert!(result.is_err());
    }
}
