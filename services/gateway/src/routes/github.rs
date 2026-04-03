use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

#[derive(Deserialize)]
pub struct LinkInstallationRequest {
    pub installation_id: u64,
}

/// POST /api/github/link-installation
///
/// Called after the user returns from the GitHub App install flow with
/// `?installation_id=<id>&setup_action=install`. Verifies the installation
/// exists via the GitHub API, then writes the link into both DynamoDB tables.
pub async fn link_installation(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<LinkInstallationRequest>,
) -> Result<Json<Value>, StatusCode> {
    // Only admins/owners can link installations
    let role_level = role_to_level(&claims.role);
    if role_level < 3 {
        return Err(StatusCode::FORBIDDEN);
    }

    let installation_id = body.installation_id;

    // Verify the installation belongs to this GitHub App by calling the GitHub API
    let (org_login, _account_id) = verify_installation(&state, installation_id).await?;

    // Check that no other team already owns this installation (via GSI)
    if let Some(existing_team) =
        super::github_webhook::resolve_team_by_installation(&state, installation_id).await
    {
        if existing_team != claims.team_id {
            warn!(
                installation_id,
                existing_team,
                requesting_team = %claims.team_id,
                "Installation already linked to another team"
            );
            return Ok(Json(
                json!({ "error": "This GitHub installation is already linked to another team." }),
            ));
        }
        // Already linked to this team — idempotent success
        return Ok(Json(json!({
            "status": "connected",
            "github_org": org_login,
            "installation_id": installation_id,
        })));
    }

    let now = chrono::Utc::now().to_rfc3339();

    // Write to teams table (github_installation_id + github_org)
    state
        .dynamo
        .update_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .update_expression(
            "SET github_installation_id = :iid, github_org = :org, updated_at = :now",
        )
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .expression_attribute_values(":org", attr_s(&org_login))
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update teams table: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Write to main table (github_install_id) for sync_repos compatibility
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .update_expression("SET github_install_id = :iid, updated_at = :now")
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update main table: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(
        installation_id,
        org = org_login.as_str(),
        team_id = %claims.team_id,
        linked_by = %claims.email,
        "GitHub installation linked to team via dashboard"
    );

    // Sync repos from the newly linked installation
    let repos = super::github_webhook::fetch_installation_repos(&state, installation_id).await;
    for repo in &repos {
        let full = format!("{}/{}", repo.owner, repo.name);
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.repos_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s(&format!("REPO#{full}")))
            .item("repo_name", attr_s(&full))
            .item(
                "enabled",
                aws_sdk_dynamodb::types::AttributeValue::Bool(false),
            )
            .item("ticket_source", attr_s("github"))
            .item("created_at", attr_s(&now))
            .send()
            .await;
    }

    Ok(Json(json!({
        "status": "connected",
        "github_org": org_login,
        "installation_id": installation_id,
        "repos_synced": repos.len(),
    })))
}

/// GET /api/github/installation-status
///
/// Returns the current GitHub App installation status for the team.
pub async fn installation_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch team: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = match result.item() {
        Some(i) => i,
        None => return Ok(Json(json!({ "status": "not_connected" }))),
    };

    let installation_id = item
        .get("github_installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok());

    let github_org = item
        .get("github_org")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());

    match (installation_id, github_org) {
        (Some(id), Some(org)) => Ok(Json(json!({
            "status": "connected",
            "github_org": org,
            "installation_id": id,
        }))),
        _ => Ok(Json(json!({ "status": "not_connected" }))),
    }
}

/// Verify an installation exists and belongs to our GitHub App.
/// Returns (org_login, account_id).
async fn verify_installation(
    state: &AppState,
    installation_id: u64,
) -> Result<(String, u64), StatusCode> {
    // Generate App JWT to call the GitHub API
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = serde_json::json!({
        "iat": now.saturating_sub(60),
        "exp": now + 600,
        "iss": state.secrets.github_app_id,
    });

    let key = jsonwebtoken::EncodingKey::from_rsa_pem(state.secrets.github_private_key.as_bytes())
        .map_err(|e| {
            error!("Invalid GitHub App private key: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .map_err(|e| {
        error!("Failed to sign App JWT: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // GET /app/installations/:installation_id
    let resp = state
        .http
        .get(format!(
            "https://api.github.com/app/installations/{installation_id}"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await
        .map_err(|e| {
            error!("GitHub API request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    if !resp.status().is_success() {
        warn!(
            installation_id,
            status = %resp.status(),
            "GitHub installation not found or not accessible"
        );
        return Err(StatusCode::NOT_FOUND);
    }

    let body: Value = resp.json().await.map_err(|e| {
        error!("Failed to parse GitHub response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let org_login = body["account"]["login"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let account_id = body["account"]["id"].as_u64().unwrap_or(0);

    Ok((org_login, account_id))
}

fn role_to_level(role: &str) -> u8 {
    match role {
        "owner" => 4,
        "admin" => 3,
        "billing" => 2,
        "member" => 1,
        _ => 0,
    }
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}
