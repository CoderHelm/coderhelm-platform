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
///
/// On a hybrid install with no GitHub App of its own, the token comes from
/// CoderHelm's token broker instead (scoped to what the install was linked
/// to; common::hybrid::client).
pub async fn get_installation_token(
    state: &AppState,
    installation_id: u64,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if common::hybrid::client::github_via_broker(&state.secrets.github_private_key) {
        return common::hybrid::client::github_token(&state.http, installation_id)
            .await
            .map_err(|e| e.to_string().into());
    }
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

/// The app's JWT (RS256, ten minutes), for `/app` calls.
pub fn app_jwt(state: &AppState) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
    Ok(encode(&Header::new(Algorithm::RS256), &claims, &key)?)
}

/// A token for one installation, narrowed to `body` (repositories and
/// permissions), with its expiry: what the hosted token broker hands out.
pub async fn mint_scoped(
    state: &AppState,
    installation_id: u64,
    body: &common::hybrid::broker::MintBody,
) -> Result<(String, String), (Option<u16>, String)> {
    let jwt = app_jwt(state).map_err(|e| (None, e.to_string()))?;
    let resp = state
        .http
        .post(format!(
            "https://api.github.com/app/installations/{installation_id}/access_tokens"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(body)
        .send()
        .await
        .map_err(|e| (None, e.without_url().to_string()))?;
    let code = resp.status().as_u16();
    let v: serde_json::Value = resp.json().await.unwrap_or_default();
    if !(200..300).contains(&code) {
        return Err((
            Some(code),
            v["message"]
                .as_str()
                .unwrap_or("GitHub refused")
                .to_string(),
        ));
    }
    Ok((
        v["token"].as_str().unwrap_or_default().to_string(),
        v["expires_at"].as_str().unwrap_or_default().to_string(),
    ))
}

static APP_SLUG: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

/// The app's slug (`GET /app`), read once.
pub async fn app_slug(state: &AppState) -> Option<String> {
    if let Ok(s) = std::env::var("GITHUB_APP_SLUG") {
        if !s.trim().is_empty() {
            return Some(s.trim().to_string());
        }
    }
    APP_SLUG
        .get_or_try_init(|| async {
            let jwt = app_jwt(state).map_err(|_| ())?;
            let v: serde_json::Value = state
                .http
                .get("https://api.github.com/app")
                .header("Authorization", format!("Bearer {jwt}"))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "Coderhelm-bot")
                .send()
                .await
                .map_err(|_| ())?
                .json()
                .await
                .map_err(|_| ())?;
            v["slug"].as_str().map(str::to_string).ok_or(())
        })
        .await
        .ok()
        .cloned()
}
