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
/// `?installation_id=<id>&setup_action=install`.
///
/// An installation id from the browser proves nothing: anyone can read one
/// off a webhook, a URL or a log. A new link is made only by the GitHub
/// sign-in step (`/auth/github`), where GitHub confirms with the user's own
/// token that they can access the installation, and links it then. This
/// endpoint confirms an installation that step already linked to the team,
/// and otherwise tells the dashboard to send the user through it.
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

    let owner = super::github_webhook::resolve_team_by_installation(&state, installation_id).await;
    match link_decision(owner.as_deref(), &claims.team_id) {
        LinkDecision::Linked => Ok(Json(json!({
            "status": "connected",
            "github_org": org_login,
            "installation_id": installation_id,
        }))),
        LinkDecision::OtherTeam => {
            warn!(
                installation_id,
                existing_team = owner.as_deref().unwrap_or(""),
                requesting_team = %claims.team_id,
                "Installation already linked to another team"
            );
            Ok(Json(
                json!({ "error": "This GitHub installation is already linked to another team." }),
            ))
        }
        LinkDecision::NeedsGithubSignIn => {
            let auth_url = github_connect_url(&state.config.stage);
            info!(
                installation_id,
                team_id = %claims.team_id,
                "Installation not linked yet — the user must confirm access through GitHub sign-in"
            );
            Ok(Json(json!({
                "error": format!(
                    "One more step: connect your GitHub account so CoderHelm can confirm with \
                     GitHub that you have access to {org_login}. Open {auth_url} and the \
                     installation is linked when you come back."
                ),
                "code": "github_auth_required",
                "auth_url": auth_url,
                "github_org": org_login,
                "installation_id": installation_id,
            })))
        }
    }
}

/// What link-installation does with an installation, given the team that
/// owns it now (if any).
#[derive(Debug, PartialEq, Eq)]
enum LinkDecision {
    /// Already this team's: nothing to do.
    Linked,
    /// Another team's: refused.
    OtherTeam,
    /// Nobody's yet: only a verified GitHub sign-in may link it.
    NeedsGithubSignIn,
}

fn link_decision(owner: Option<&str>, team_id: &str) -> LinkDecision {
    match owner {
        Some(o) if o == team_id => LinkDecision::Linked,
        Some(_) => LinkDecision::OtherTeam,
        None => LinkDecision::NeedsGithubSignIn,
    }
}

/// The GitHub sign-in that connects the user's GitHub account and links the
/// installations GitHub confirms they can access.
/// It is on this deployment's API (a self-hosted install has its own).
fn github_connect_url(stage: &str) -> String {
    format!("{}/auth/github", common::public_urls::api_url(stage))
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

/// Query for [`app_install`].
#[derive(Deserialize, Default)]
pub struct AppInstallQuery {
    /// Passed on to GitHub, which hands it back on the setup redirect.
    #[serde(default)]
    pub state: Option<String>,
    /// Open this installation's settings instead of a new installation.
    #[serde(default)]
    pub installation_id: Option<u64>,
}

/// Where this deployment's GitHub App is installed (or an installation of it
/// configured). The slug is this install's own app, never the hosted one.
pub fn app_install_url(slug: &str, q: &AppInstallQuery) -> String {
    let slug = urlencoding::encode(slug);
    match q.installation_id {
        Some(id) => format!("https://github.com/apps/{slug}/installations/{id}"),
        None => match q.state.as_deref().filter(|s| !s.is_empty()) {
            Some(st) => format!(
                "https://github.com/apps/{slug}/installations/new?state={}",
                urlencoding::encode(st)
            ),
            None => format!("https://github.com/apps/{slug}/installations/new"),
        },
    }
}

/// GET /auth/github/app: sends the browser to this deployment's GitHub App
/// on GitHub (the dashboard links here, so a self-hosted install's buttons
/// open its own app). Before the app exists, back to the dashboard's GitHub
/// settings.
pub async fn app_install(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<AppInstallQuery>,
) -> axum::response::Redirect {
    match crate::auth::github_app::app_slug(&state).await {
        Some(slug) => axum::response::Redirect::to(&app_install_url(&slug, &q)),
        None => axum::response::Redirect::to(&format!(
            "{}/settings/integrations/github/",
            common::public_urls::app_url(&state.config.stage)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_installation_already_linked_here_is_confirmed() {
        assert_eq!(
            link_decision(Some("TEAM#a"), "TEAM#a"),
            LinkDecision::Linked
        );
        assert_eq!(
            link_decision(Some("TEAM#b"), "TEAM#a"),
            LinkDecision::OtherTeam
        );
        // An unlinked id from the browser is never linked from here.
        assert_eq!(
            link_decision(None, "TEAM#a"),
            LinkDecision::NeedsGithubSignIn
        );
    }

    #[test]
    fn the_app_link_is_this_installs_own_app() {
        let q = |state: Option<&str>, id: Option<u64>| AppInstallQuery {
            state: state.map(str::to_string),
            installation_id: id,
        };
        assert_eq!(
            app_install_url("acme-coderhelm", &q(None, None)),
            "https://github.com/apps/acme-coderhelm/installations/new"
        );
        assert_eq!(
            app_install_url("acme-coderhelm", &q(Some("a b&c"), None)),
            "https://github.com/apps/acme-coderhelm/installations/new?state=a%20b%26c"
        );
        assert_eq!(
            app_install_url("acme-coderhelm", &q(Some("x"), Some(42))),
            "https://github.com/apps/acme-coderhelm/installations/42"
        );
    }

    #[test]
    fn sign_in_url_follows_the_stage() {
        assert_eq!(
            github_connect_url("prod"),
            "https://api.coderhelm.com/auth/github"
        );
        assert_eq!(
            github_connect_url("dev"),
            format!("{}/auth/github", common::public_urls::stage_api_url("dev"))
        );
    }
}
