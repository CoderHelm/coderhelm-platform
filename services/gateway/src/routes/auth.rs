use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::jwt;
use crate::AppState;

#[derive(Deserialize)]
pub struct CallbackParams {
    code: String,
    #[allow(dead_code)]
    state: Option<String>,
}

/// Redirect user to GitHub OAuth authorize page.
pub async fn login(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client_id = &state.secrets.github_client_id;
    let redirect_uri = if state.config.stage == "prod" {
        "https://api.coderhelm.com/auth/callback"
    } else {
        "http://localhost:3000/auth/callback"
    };
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope=read:user,read:org"
    );
    Redirect::temporary(&url)
}

/// GitHub OAuth callback — exchange code for token, create JWT session.
pub async fn callback(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CallbackParams>,
) -> Result<Response, StatusCode> {
    // Exchange code for access token
    let client = reqwest::Client::new();
    let token_resp = client
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .json(&serde_json::json!({
            "client_id": state.secrets.github_client_id,
            "client_secret": state.secrets.github_client_secret,
            "code": params.code,
        }))
        .send()
        .await
        .map_err(|e| {
            error!("GitHub token exchange failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let token_data: serde_json::Value = token_resp.json().await.map_err(|e| {
        error!("Failed to parse GitHub token response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let access_token = token_data["access_token"]
        .as_str()
        .ok_or(StatusCode::BAD_GATEWAY)?;

    // Fetch user profile
    let user_resp = client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {access_token}"))
        .header("User-Agent", "Coderhelm")
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let user: serde_json::Value = user_resp
        .json()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let github_id = user["id"].as_u64().ok_or(StatusCode::BAD_GATEWAY)?;
    let github_login = user["login"].as_str().ok_or(StatusCode::BAD_GATEWAY)?;

    // Fetch user's app installations to find tenant
    let installs_resp = client
        .get("https://api.github.com/user/installations")
        .header("Authorization", format!("Bearer {access_token}"))
        .header("User-Agent", "Coderhelm")
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let installs: serde_json::Value = installs_resp
        .json()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    // Find all Coderhelm installations the user has access to
    let all_installations: Vec<(u64, String)> = installs["installations"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|i| i["app_slug"].as_str() == Some("coderhelm"))
        .filter_map(|i| {
            let id = i["id"].as_u64()?;
            let org = i["account"]["login"].as_str().unwrap_or("unknown");
            Some((id, org.to_string()))
        })
        .collect();

    if all_installations.is_empty() {
        error!("User {github_login} has no Coderhelm installation");
        return Err(StatusCode::FORBIDDEN);
    }

    let installation_id = all_installations[0].0;
    let tenant_id = format!("TENANT#{installation_id}");
    let user_id = format!("USER#{github_id}");

    // Upsert user into ALL tenant user tables so they can switch later
    let now = chrono::Utc::now().to_rfc3339();
    for (inst_id, _org) in &all_installations {
        let tid = format!("TENANT#{inst_id}");
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .item("pk", attr_s(&tid))
            .item("sk", attr_s(&user_id))
            .item("github_id", attr_n(github_id))
            .item("github_login", attr_s(github_login))
            .item("email", attr_s(user["email"].as_str().unwrap_or("")))
            .item(
                "avatar_url",
                attr_s(user["avatar_url"].as_str().unwrap_or("")),
            )
            .item("role", attr_s("member"))
            .item("updated_at", attr_s(&now))
            // GSI1: github_id → tenant lookup
            .item("gsi1pk", attr_s(&format!("GHUSER#{github_id}")))
            .item("gsi1sk", attr_s(&tid))
            .send()
            .await;
    }

    info!(github_login, installation_id, "User authenticated");

    // Create JWT
    let token = jwt::create_token(
        &user_id,
        &tenant_id,
        github_login,
        &state.secrets.jwt_secret,
        86400, // 24 hours
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Set cookie and redirect to dashboard
    let dashboard_url = if state.config.stage == "prod" {
        "https://app.coderhelm.com"
    } else {
        "http://localhost:3000"
    };

    let cookie = format!(
        "coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=86400"
    );

    Ok((
        [
            (header::SET_COOKIE, cookie),
            (header::LOCATION, dashboard_url.to_string()),
        ],
        StatusCode::FOUND,
    )
        .into_response())
}

/// Logout — clear session cookie.
pub async fn logout() -> impl IntoResponse {
    let cookie = "coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
    ([(header::SET_COOKIE, cookie.to_string())], StatusCode::OK)
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}
