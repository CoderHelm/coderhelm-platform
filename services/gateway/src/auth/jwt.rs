use crate::models::Claims;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// How a session signed in, carried in the session token beside its claims:
/// whether the sign-in passed a second factor, and the way in (`password`,
/// `google` or `github`). A token from before this existed reads as no second
/// factor, so a required second factor reaches old sessions too.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct SignInProof {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mfa: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<String>,
}

impl SignInProof {
    pub fn new(mfa: bool, via: &str) -> Self {
        Self {
            mfa,
            via: Some(via.to_string()),
        }
    }
}

/// A session token: the claims and the sign-in proof, flattened into one.
#[derive(Serialize)]
struct SessionToken<'a> {
    #[serde(flatten)]
    claims: &'a Claims,
    #[serde(flatten)]
    proof: &'a SignInProof,
}

pub fn create_token(
    user_id: &str,
    team_id: &str,
    email: &str,
    role: &str,
    github_login: Option<&str>,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    create_session_token(
        user_id,
        team_id,
        email,
        role,
        github_login,
        &SignInProof::default(),
        secret,
        ttl_secs,
    )
}

/// A session token that records how the person signed in.
#[allow(clippy::too_many_arguments)]
pub fn create_session_token(
    user_id: &str,
    team_id: &str,
    email: &str,
    role: &str,
    github_login: Option<&str>,
    proof: &SignInProof,
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
        api_key: None,
    };
    encode(
        &Header::default(),
        &SessionToken {
            claims: &claims,
            proof,
        },
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// The sign-in proof of a session token. Call it only on a token
/// [`validate_token`] accepted; anything unreadable is no proof.
pub fn read_proof(token: &str, secret: &str) -> SignInProof {
    let mut validation = Validation::default();
    validation.set_audience(&["coderhelm-dashboard"]);
    decode::<SignInProof>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map(|t| t.claims)
    .unwrap_or_default()
}

/// Audience of the short token between a Google or GitHub sign-in and its
/// second step: accepted only by `POST /auth/2fa/verify`.
pub const SECOND_STEP_AUDIENCE: &str = "coderhelm-2fa";

/// Who is between a federated sign-in and their two-factor code.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SecondStep {
    pub sub: String,
    pub team_id: String,
    pub email: String,
    pub role: String,
    #[serde(default)]
    pub github_login: Option<String>,
    pub via: String,
    /// This token's id: its wrong codes are counted, and it dies after five.
    #[serde(default)]
    pub jti: String,
    pub aud: String,
    pub iat: u64,
    pub exp: u64,
}

pub fn create_second_step(
    step: &SecondStep,
    secret: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = chrono::Utc::now().timestamp() as u64;
    let s = SecondStep {
        jti: if step.jti.is_empty() {
            ulid::Ulid::new().to_string()
        } else {
            step.jti.clone()
        },
        aud: SECOND_STEP_AUDIENCE.to_string(),
        iat: now,
        exp: now + ttl_secs,
        ..step.clone()
    };
    encode(
        &Header::default(),
        &s,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

pub fn validate_second_step(
    token: &str,
    secret: &str,
) -> Result<SecondStep, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.set_audience(&[SECOND_STEP_AUDIENCE]);
    Ok(decode::<SecondStep>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?
    .claims)
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
        // A scoped token is minted from a signed-in session; it never
        // carries an API key, which only reaches the public routes.
        api_key: None,
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
    fn a_session_carries_its_sign_in_proof_and_old_tokens_have_none() {
        let secret = "test-jwt-secret-at-least-32-chars!";
        let proof = SignInProof::new(true, "google");
        let t =
            create_session_token("u", "t", "e@x.com", "member", None, &proof, secret, 300).unwrap();
        assert_eq!(validate_token(&t, secret).unwrap().sub, "u");
        assert_eq!(read_proof(&t, secret), proof);
        let old = create_token("u", "t", "e@x.com", "member", None, secret, 300).unwrap();
        assert!(!read_proof(&old, secret).mfa);
        // A proof is read only from a token signed with the secret.
        assert!(!read_proof(&t, "another-secret-of-at-least-32-chars").mfa);
    }

    #[test]
    fn a_second_step_token_is_not_a_session_and_a_session_is_not_one() {
        let secret = "test-jwt-secret-at-least-32-chars!";
        let step = SecondStep {
            sub: "USER#1".into(),
            team_id: "TEAM#a".into(),
            email: "a@x.com".into(),
            role: "member".into(),
            github_login: None,
            via: "github".into(),
            jti: String::new(),
            aud: String::new(),
            iat: 0,
            exp: 0,
        };
        let t = create_second_step(&step, secret, 300).unwrap();
        assert!(validate_token(&t, secret).is_err());
        let back = validate_second_step(&t, secret).unwrap();
        assert_eq!(back.sub, "USER#1");
        // Each token gets its own id (wrong codes are counted against it).
        assert_eq!(back.jti.len(), 26);
        let other =
            validate_second_step(&create_second_step(&step, secret, 300).unwrap(), secret).unwrap();
        assert_ne!(back.jti, other.jti);
        let session = create_token("u", "t", "e@x.com", "member", None, secret, 300).unwrap();
        assert!(validate_second_step(&session, secret).is_err());
    }

    #[test]
    fn test_invalid_token_rejected() {
        let result = validate_token("not.a.valid.token", "secret");
        assert!(result.is_err());
    }
}
