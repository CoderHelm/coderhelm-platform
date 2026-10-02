use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::AppState;

#[derive(Serialize)]
struct JwtClaims {
    iat: u64,
    exp: u64,
    iss: String,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    token: String,
}

/// Get a GitHub installation access token for posting comments.
pub async fn get_installation_token(
    state: &AppState,
    installation_id: u64,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = JwtClaims {
        iat: now.saturating_sub(60),
        exp: now + 600,
        iss: state.secrets.github_app_id.clone(),
    };

    let key = EncodingKey::from_rsa_pem(state.secrets.github_private_key.as_bytes())?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)?;

    let mut req = state
        .http
        .post(format!(
            "https://api.github.com/app/installations/{installation_id}/access_tokens"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot");
    // Pin the token format (stateless JWT-style by default) instead of
    // following GitHub's rollout; tokens are handled as opaque strings.
    if let Some(mode) = common::stateless_s2s_token_mode() {
        req = req.header(common::STATELESS_S2S_TOKEN_HEADER, mode);
    }
    let resp: TokenResponse = req.send().await?.error_for_status()?.json().await?;

    Ok(resp.token)
}
