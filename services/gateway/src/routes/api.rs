use axum::extract::Query;
use axum::response::IntoResponse;
use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use super::run_search;
use super::run_trigger;
use crate::auth::jwt;
use crate::middleware::auth::SESSION_TTL_SECS;
use crate::models::{
    Claims, FeedbackMessage, InfraAnalyzeMessage, NotificationPrefs, OnboardMessage, OnboardRepo,
    TicketMessage, TicketSource, WorkerMessage,
};
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;

/// Who is calling: a person signed in to the dashboard, or a team API key.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Me {
    /// The caller's id: `USER#...` for a person, `APIKEY#<id>` for an API key.
    #[schema(example = "APIKEY#01jb8x6f4m2c9qz3t7v5w1k0ha")]
    pub user_id: String,
    /// The team every request acts on.
    #[schema(example = "TEAM#4817263")]
    pub team_id: String,
    /// The person's GitHub login. For an API key, its maker's.
    #[schema(example = "octocat")]
    pub github_login: Option<String>,
    /// The person's email. For an API key, its maker's.
    #[schema(example = "dev@example.com")]
    pub email: String,
    /// The person's picture. Always null for an API key.
    #[schema(example = "https://avatars.githubusercontent.com/u/583231")]
    pub avatar_url: Option<String>,
    /// The built-in role requests run as: `viewer`, `member`, `admin` or
    /// `owner`. For a custom role or an API key, the highest built-in role
    /// whose capabilities are all held; `capabilities` says exactly what is.
    #[schema(example = "viewer")]
    pub role: String,
    /// The person's custom role, when they have one. Null for an API key.
    pub custom_role: Option<crate::permissions::CustomRoleRef>,
    /// The capability keys held. For an API key, its scopes as they stand on
    /// this request: never more than its maker holds now.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = json!(["view", "runs"]))]
    pub capabilities: Option<Vec<String>>,
    /// The team's account status: `active` or `suspended`.
    #[schema(example = "active")]
    pub status: String,
    /// How the caller signed in: `email`, `google`, `github`, or `api_key`.
    #[schema(example = "api_key")]
    pub auth_provider: String,
    /// Whether the person turned on two-factor sign-in.
    #[schema(example = false)]
    pub mfa_enabled: bool,
    /// Whether the caller administers the whole CoderHelm install (its
    /// operators, set in the install's configuration). Always false for an
    /// API key.
    #[schema(example = false)]
    pub platform_admin: bool,
    /// Set only in a support view (dashboard only).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(ignore)]
    pub support_view: Option<Value>,
    /// Set only when the caller is an API key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<MeApiKey>,
    /// The team's feature flags (the dashboard reads them on every page).
    pub flags: MeFlags,
}

/// A team's feature flags.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct MeFlags {
    /// Plugins are turned on for the team.
    #[schema(example = false)]
    pub plugins: bool,
}

async fn me_flags(state: &AppState, team_id: &str) -> MeFlags {
    let kv = common::plugins::kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    };
    MeFlags {
        plugins: common::plugins::flag::enabled(&kv, team_id).await,
    }
}

/// The API key a request was made with.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct MeApiKey {
    /// The key's id, the part after `chk_` and before the next `_`.
    #[schema(example = "01jb8x6f4m2c9qz3t7v5w1k0ha")]
    pub id: String,
    /// The name the admin gave it.
    #[schema(example = "Deploy pipeline")]
    pub name: String,
    /// Who made it. The key acts within that person's access.
    #[schema(example = "dev@example.com")]
    pub created_by: String,
    /// When it stops working (RFC 3339), or null if it doesn't expire.
    #[schema(example = "2027-01-01T00:00:00Z")]
    pub expires_at: Option<String>,
    /// Requests the key may make in a minute.
    #[schema(example = 600)]
    pub rate_limit_per_minute: u32,
}

/// The team's account status (`active` unless suspended).
async fn team_status(state: &AppState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|i| i.get("status").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_else(|| "active".to_string())
}

/// GET /api/me — who is calling.
#[utoipa::path(
    get,
    path = "/api/me",
    tag = "Account",
    operation_id = "getMe",
    summary = "Get the caller",
    description = "Returns who the request is authenticated as. With an API key this is the key \
        itself: its name, who made it, when it expires, its rate limit, and in `capabilities` the \
        scopes it holds right now (cut down to what its maker holds). Call it first to check a key works.",
    responses(
        (status = 200, description = "The caller.", body = Me),
    ),
    security(("api_key" = [])),
)]
pub async fn me(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Me>, StatusCode> {
    // An API key is no person: answer for the key and its maker.
    if let Some(key) = &claims.api_key {
        return Ok(Json(Me {
            user_id: claims.sub.clone(),
            team_id: claims.team_id.clone(),
            github_login: claims.github_login.clone(),
            email: claims.email.clone(),
            avatar_url: None,
            role: claims.holder().floor().to_string(),
            custom_role: None,
            capabilities: Some(key.scopes.clone()),
            status: team_status(&state, &claims.team_id).await,
            auth_provider: "api_key".into(),
            mfa_enabled: false,
            platform_admin: false,
            support_view: None,
            api_key: Some(MeApiKey {
                id: key.id.clone(),
                name: key.name.clone(),
                created_by: key.created_by.clone(),
                expires_at: key.expires_at.clone(),
                rate_limit_per_minute: key.rate_limit_per_minute,
            }),
            flags: me_flags(&state, &claims.team_id).await,
        }));
    }
    // A support-view session is no person on the team: answer for the view
    // itself instead of looking up a member record.
    if let Some(sv) = &claims.support_view {
        return Ok(Json(Me {
            user_id: claims.sub.clone(),
            team_id: claims.team_id.clone(),
            github_login: None,
            email: sv.admin_email.clone(),
            avatar_url: None,
            role: claims.role.clone(),
            custom_role: None,
            capabilities: None,
            status: "active".into(),
            auth_provider: "support".into(),
            mfa_enabled: false,
            platform_admin: false,
            support_view: Some(json!({ "team_id": sv.team_id, "until": sv.until })),
            api_key: None,
            flags: me_flags(&state, &claims.team_id).await,
        }));
    }
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let team_status = team_status(&state, &claims.team_id).await;

    // Derive auth provider from user record
    let auth_provider = if item
        .get("github_login")
        .and_then(|v| v.as_s().ok())
        .is_some()
    {
        "github"
    } else if claims.sub.starts_with("USER#Google_") {
        "google"
    } else {
        "email"
    };

    // Check MFA status from Cognito (only for email users)
    let mfa_enabled = if auth_provider == "email" {
        state
            .cognito
            .admin_get_user()
            .user_pool_id(&state.config.cognito_user_pool_id)
            .username(&claims.email)
            .send()
            .await
            .ok()
            .and_then(|u| u.user_mfa_setting_list)
            .map(|list| list.iter().any(|s| s == "SOFTWARE_TOKEN_MFA"))
            .unwrap_or(false)
    } else {
        false
    };

    Ok(Json(Me {
        user_id: claims.sub.clone(),
        team_id: claims.team_id.clone(),
        github_login: claims.github_login.clone(),
        email: claims.email.clone(),
        avatar_url: item.get("avatar_url").and_then(|v| v.as_s().ok()).cloned(),
        // For a custom role, `role` is the built-in role the member's requests
        // run as outside their role's capabilities (pages check it), and
        // `custom_role` and `capabilities` say what they hold.
        role: match &claims.custom {
            Some(h) => h.floor().to_string(),
            None => item
                .get("role")
                .and_then(|v| v.as_s().ok())
                .map_or(claims.role.clone(), |r| r.clone()),
        },
        custom_role: claims.custom.as_ref().and_then(|h| h.custom.clone()),
        capabilities: Some(
            crate::permissions::CAPABILITIES
                .iter()
                .filter(|c| claims.holder().holds(c.key))
                .map(|c| c.key.to_string())
                .collect(),
        ),
        status: team_status,
        auth_provider: auth_provider.into(),
        mfa_enabled,
        // Admin of the whole install (PLATFORM_ADMIN_EMAILS): the dashboard shows
        // the admin pages; every admin route checks again.
        platform_admin: is_platform_admin(&claims),
        support_view: None,
        api_key: None,
        flags: me_flags(&state, &claims.team_id).await,
    }))
}

/// GET /api/teams — list all teams the current user has access to.
pub async fn list_teams(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let github_id = claims
        .sub
        .strip_prefix("USER#")
        .ok_or(StatusCode::BAD_REQUEST)?;

    // Try GSI1 (GitHub users) first, then fall back to GSI2 (email users)
    let gsi1_result = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi1")
        .key_condition_expression("gsi1pk = :gk")
        .expression_attribute_values(":gk", attr_s(&format!("GHUSER#{github_id}")))
        .send()
        .await
        .ok();

    let (team_ids, _gsi2_items) = if let Some(ref result) = gsi1_result {
        let ids: Vec<String> = result
            .items()
            .iter()
            .filter_map(|item| item.get("gsi1sk").and_then(|v| v.as_s().ok()).cloned())
            .collect();
        if !ids.is_empty() {
            (ids, vec![])
        } else {
            // Fall back to GSI2 (email)
            let all_items = state
                .dynamo
                .query()
                .table_name(&state.config.users_table_name)
                .index_name("gsi2")
                .key_condition_expression("gsi2pk = :pk")
                .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", claims.email)))
                .send()
                .await
                .map(|r| r.items().to_vec())
                .unwrap_or_default();

            // Process any pending invites — convert them to real user records
            let real_user_sk = format!("USER#{github_id}");
            let now = chrono::Utc::now().to_rfc3339();
            for item in &all_items {
                let is_invite = item
                    .get("status")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s == "invited")
                    .unwrap_or(false);
                if !is_invite {
                    continue;
                }
                let invite_tid = match item.get("pk").and_then(|v| v.as_s().ok()) {
                    Some(t) => t.to_string(),
                    None => continue,
                };
                let invite_role = item
                    .get("role")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_else(|| "member".to_string());

                // Create real user record in the inviting team
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.users_table_name)
                    .item("pk", attr_s(&invite_tid))
                    .item("sk", attr_s(&real_user_sk))
                    .item("email", attr_s(&claims.email))
                    .item("role", attr_s(&invite_role))
                    .item("avatar_url", attr_s(""))
                    .item("updated_at", attr_s(&now))
                    .item("gsi2pk", attr_s(&format!("EMAIL#{}", claims.email)))
                    .item("gsi2sk", attr_s(&invite_tid))
                    .item("status", attr_s("active"))
                    // Never overwrite an existing member record.
                    .condition_expression("attribute_not_exists(pk)")
                    .send()
                    .await;

                // Delete the invite placeholder
                if let Some(invite_sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
                    let _ = state
                        .dynamo
                        .delete_item()
                        .table_name(&state.config.users_table_name)
                        .key("pk", attr_s(&invite_tid))
                        .key("sk", attr_s(invite_sk))
                        .send()
                        .await;
                    info!(
                        email = %claims.email,
                        team_id = %invite_tid,
                        role = %invite_role,
                        "Processed pending invite on list_teams"
                    );
                }
            }

            let ids: Vec<String> = all_items
                .iter()
                .filter_map(|item| item.get("pk").and_then(|v| v.as_s().ok()).cloned())
                .collect();
            // Deduplicate (invite + real record may both point to same team)
            let mut unique_ids: Vec<String> = Vec::new();
            for id in ids {
                if !unique_ids.contains(&id) {
                    unique_ids.push(id);
                }
            }
            (unique_ids, all_items)
        }
    } else {
        (vec![], vec![])
    };

    // Load org name + status for each team
    let mut teams: Vec<Value> = Vec::new();
    for tid in &team_ids {
        let meta = state
            .dynamo
            .get_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(tid))
            .key("sk", attr_s("META"))
            .send()
            .await
            .ok()
            .and_then(|r| r.item);

        let org = meta
            .as_ref()
            .and_then(|item| item.get("github_org"))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| claims.email.clone());

        let status = meta
            .as_ref()
            .and_then(|item| item.get("status"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "active".to_string());

        teams.push(json!({
            "team_id": tid,
            "org": org,
            "status": status,
            "current": *tid == claims.team_id,
        }));
    }

    Ok(Json(json!({ "teams": teams })))
}

/// POST /api/teams/switch — switch to a different team.
pub async fn switch_team(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<jwt::SignInProof>>,
    Json(body): Json<Value>,
) -> Result<axum::response::Response, StatusCode> {
    let target_team = body["team_id"].as_str().ok_or(StatusCode::BAD_REQUEST)?;

    // Verify user exists in target team and get their role there
    let target_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(target_team))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .item
        .ok_or(StatusCode::FORBIDDEN)?;

    let target_role = target_item
        .get("role")
        .and_then(|v| v.as_s().ok())
        .map_or("member", |v| v);

    // Re-issue JWT with new team_id and correct role, keeping how the
    // session signed in.
    let token = jwt::create_session_token(
        &claims.sub,
        target_team,
        &claims.email,
        target_role,
        claims.github_login.as_deref(),
        &proof.map(|p| p.0).unwrap_or_default(),
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cookie = format!(
        "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
    );

    Ok((
        [(axum::http::header::SET_COOKIE, cookie)],
        Json(json!({ "team_id": target_team })),
    )
        .into_response())
}

/// PUT /api/teams/rename — rename the current team (owner only).
pub async fn rename_team(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let new_name = body["name"]
        .as_str()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && s.len() <= 100)
        .ok_or(StatusCode::BAD_REQUEST)?;

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .update_expression("SET github_org = :name")
        .expression_attribute_values(":name", attr_s(new_name))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to rename team: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team = %claims.team_id, new_name = %new_name, "Team renamed");
    Ok(Json(json!({ "status": "renamed", "name": new_name })))
}

/// The signup allowlist and the teams overview are platform-wide, so only the
/// platform admin may read or change them. A team owner role is not enough:
/// anyone who signs up owns their own team. The session's email must be on
/// the list AND have been verified by CoderHelm itself (Google or an email
/// code): a GitHub-only identity carries whatever email its profile shows,
/// so it never counts.
pub(crate) fn is_platform_admin(claims: &Claims) -> bool {
    let admins = std::env::var("PLATFORM_ADMIN_EMAILS")
        .unwrap_or_else(|_| common::public_urls::default_platform_admins().to_string());
    // A support-view session is the team's read-only view, never the admin's
    // own powers: the admin routes refuse it.
    claims.support_view.is_none()
        && verified_identity(&claims.sub)
        && platform_admin_listed(&admins, &claims.email)
}

/// Whether a session's user id is one whose email CoderHelm verified: not a
/// GitHub-only identity (`USER#<github numeric id>`).
fn verified_identity(sub: &str) -> bool {
    let id = sub.strip_prefix("USER#").unwrap_or(sub);
    !id.is_empty() && !id.chars().all(|c| c.is_ascii_digit())
}

fn platform_admin_listed(admins: &str, email: &str) -> bool {
    let email = email.trim();
    !email.is_empty()
        && admins
            .split(',')
            .map(str::trim)
            .any(|a| !a.is_empty() && a.eq_ignore_ascii_case(email))
}

/// GET /api/allowlist — list allowed emails (platform admin only).
pub async fn list_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        return Err(StatusCode::FORBIDDEN);
    }

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s("ALLOWLIST"))
        .expression_attribute_values(":prefix", attr_s("EMAIL#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let emails: Vec<String> = result
        .items()
        .iter()
        .filter_map(|item| item.get("email").and_then(|v| v.as_s().ok()).cloned())
        .collect();

    Ok(Json(json!({ "emails": emails })))
}

/// POST /api/allowlist — add an email to the allowlist (platform admin only).
pub async fn add_to_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        return Err(StatusCode::FORBIDDEN);
    }

    let email = body["email"]
        .as_str()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && (s.contains('@') || s.starts_with("*@")))
        .ok_or(StatusCode::BAD_REQUEST)?;

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s("ALLOWLIST"))
        .item("sk", attr_s(&format!("EMAIL#{email}")))
        .item("email", attr_s(&email))
        .item("added_by", attr_s(&claims.email))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to add to allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %email, added_by = %claims.email, "Added to allowlist");
    Ok(Json(json!({ "status": "added", "email": email })))
}

/// DELETE /api/allowlist — remove an email from the allowlist (platform admin only).
pub async fn remove_from_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_platform_admin(&claims) {
        return Err(StatusCode::FORBIDDEN);
    }

    let email = body["email"]
        .as_str()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s("ALLOWLIST"))
        .key("sk", attr_s(&format!("EMAIL#{email}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to remove from allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %email, removed_by = %claims.email, "Removed from allowlist");
    Ok(Json(json!({ "status": "removed", "email": email })))
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RunsQuery {
    /// Only runs from this source, such as `github`, `jira` or `alert`.
    #[param(example = "jira")]
    source: Option<String>,
    /// Runs per page, 1 to 100. Default 50.
    #[param(minimum = 1, maximum = 100, example = 20)]
    limit: Option<i32>,
    /// `next` from the previous page (the last run id it covered).
    cursor: Option<String>,
    /// Search words: ticket key or number, title, repo, PR number, status.
    #[param(example = "WEB-42")]
    q: Option<String>,
    /// Comma-separated exact statuses, e.g. `failed,cancelled`.
    #[param(example = "failed,cancelled")]
    status: Option<String>,
    /// One repo, `owner/name` (exact, case-insensitive).
    #[param(example = "acme/web")]
    repo: Option<String>,
    /// Comma-separated ways the run was started (`trigger.kind`): `jira`,
    /// `github`, `coderhelm`, `alert`, `plan`, `dashboard`, `chat`.
    #[param(example = "coderhelm,alert")]
    started_by: Option<String>,
}

/// One run in a list.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunSummary {
    /// The run's id, a ULID. Newer runs sort after older ones.
    #[schema(example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")]
    pub run_id: Option<String>,
    /// Where the run is: `pending`, `running`, `awaiting_ci`, `completed`,
    /// `failed`, `needs_input`, `needs_outside_change`, `cancelled`,
    /// `retried` or `closed`. Treat a value you don't know as in progress.
    #[schema(example = "completed")]
    pub status: Option<String>,
    /// What started it, such as `github`, `jira` or `alert`.
    #[schema(example = "jira")]
    pub ticket_source: Option<String>,
    /// The ticket's key or issue number in its source.
    #[schema(example = "WEB-42")]
    pub ticket_id: Option<String>,
    /// The ticket's title.
    #[schema(example = "Add rate limiting to the login form")]
    pub title: Option<String>,
    /// The repository, `owner/name`.
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// The pull request the run opened.
    #[schema(example = "https://github.com/acme/web/pull/318")]
    pub pr_url: Option<String>,
    /// That pull request's number.
    #[schema(example = 318)]
    pub pr_number: Option<u64>,
    /// What the run cost in model usage, in US dollars, as recorded.
    #[schema(example = 1.42)]
    pub cost_usd: Option<f64>,
    /// Input tokens used.
    #[schema(example = 182340)]
    pub tokens_in: Option<u64>,
    /// Output tokens used.
    #[schema(example = 20511)]
    pub tokens_out: Option<u64>,
    /// How long the run took, in seconds.
    #[schema(example = 734)]
    pub duration_s: Option<u64>,
    /// The step the run is on or ended on, such as `triage`, `plan`,
    /// `implement`, `review` or `pr`.
    #[schema(example = "pr")]
    pub current_pass: Option<String>,
    /// When the run started (RFC 3339).
    #[schema(example = "2026-10-22T14:03:11Z")]
    pub created_at: Option<String>,
    /// How the run was started and by whom, worked out from the fields
    /// above and what started it. Show this instead of an internal
    /// `ticket_id` such as `E2E-FIX-…`.
    pub trigger: run_trigger::RunTrigger,
}

/// A page of runs.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunsPage {
    /// Runs that match, newest first. Can be short, or empty, while `next`
    /// is set: older runs are still unread.
    pub runs: Vec<RunSummary>,
    /// Pass as `cursor` to read on; null when there are no older runs.
    #[schema(example = "01JB8W2Q7N4D6R8S0T2V4X6Z8A")]
    pub next: Option<String>,
    /// How many runs were read to fill this page (searches and filters skip some).
    #[schema(example = 50)]
    pub scanned: usize,
}

/// GET /api/runs — list runs for the team (from runs table).
///
/// Reads the team's runs newest first and keeps the ones that pass the search
/// and filters (see `run_search`), until the page is full or `SCAN_BUDGET`
/// runs are read. A page can be short or empty while `next` is set: older
/// runs are still unread.
#[utoipa::path(
    get,
    path = "/api/runs",
    tag = "Runs",
    operation_id = "listRuns",
    summary = "List runs",
    description = "Lists the team's runs, newest first, one page at a time. Search with `q` and \
        filter by `status`, `source` and `repo`. Each page reads at most a few hundred runs, so a \
        page can come back short, or empty, with `next` set: keep reading until `next` is null.",
    params(RunsQuery),
    responses(
        (status = 200, description = "A page of runs.", body = RunsPage),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<RunsQuery>,
) -> Result<Json<RunsPage>, StatusCode> {
    let want = params.limit.unwrap_or(50).clamp(1, 100) as usize;
    let filter = run_search::RunFilter::new(
        params.q.as_deref(),
        params.status.as_deref(),
        params.source.as_deref(),
    )
    .with_repo(params.repo.as_deref())
    .with_started_by(params.started_by.as_deref());
    let mut start = params.cursor.as_deref().filter(|c| c.len() <= 64).map(|c| {
        std::collections::HashMap::from([
            ("team_id".to_string(), attr_s(&claims.team_id)),
            ("run_id".to_string(), attr_s(c)),
        ])
    });
    let mut matched: Vec<run_search::Item> = Vec::new();
    let mut read = 0;
    let mut next: Option<String> = None;
    let names: Vec<(String, String)> = run_search::LIST_ATTRS
        .iter()
        .enumerate()
        .map(|(i, a)| (format!("#a{i}"), a.to_string()))
        .collect();
    let projection = names
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    loop {
        let budget = run_search::SCAN_BUDGET - read;
        let result = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :tid")
            .expression_attribute_values(":tid", attr_s(&claims.team_id))
            .projection_expression(&projection)
            .set_expression_attribute_names(Some(names.iter().cloned().collect()))
            .scan_index_forward(false) // newest first (ULID sorts lexicographically)
            .limit(budget.min(200) as i32)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!("Failed to query runs: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        let items = result.items();
        let (found, n) = run_search::take_matches(items, &filter, want - matched.len(), budget);
        read += n;
        matched.extend(found.into_iter().cloned());
        let last_read = n.checked_sub(1).and_then(|i| item_s(&items[i], "run_id"));
        if n < items.len() {
            // Stopped inside this page: carry on after the last run read.
            next = last_read.map(str::to_string);
            break;
        }
        match result.last_evaluated_key().filter(|k| !k.is_empty()) {
            Some(key) if matched.len() < want && read < run_search::SCAN_BUDGET => {
                start = Some(key.clone())
            }
            Some(_) => {
                next = last_read.map(str::to_string);
                break;
            }
            None => break,
        }
    }

    let str_of =
        |item: &run_search::Item, k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let num_of = |item: &run_search::Item, k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
    };
    let lookups = run_trigger::load_lookups(&state, &claims.team_id, matched.iter()).await;
    let runs: Vec<RunSummary> = matched
        .iter()
        .map(|item| RunSummary {
            trigger: run_trigger::trigger(item, &lookups),
            run_id: str_of(item, "run_id"),
            status: str_of(item, "status"),
            ticket_source: str_of(item, "ticket_source"),
            ticket_id: str_of(item, "ticket_id"),
            title: str_of(item, "title"),
            repo: str_of(item, "repo"),
            pr_url: str_of(item, "pr_url"),
            pr_number: num_of(item, "pr_number"),
            cost_usd: item
                .get("cost_usd")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<f64>().ok()),
            tokens_in: num_of(item, "tokens_in"),
            tokens_out: num_of(item, "tokens_out"),
            duration_s: num_of(item, "duration_s"),
            current_pass: str_of(item, "current_pass"),
            created_at: str_of(item, "created_at"),
        })
        .collect();

    Ok(Json(RunsPage {
        runs,
        next,
        scanned: read,
    }))
}

/// One step a run went through.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunPass {
    /// The step, such as `triage`, `plan`, `implement`, `test`, `security`,
    /// `pr`, `review`, `feedback` or `ci_fix`.
    #[schema(example = "implement")]
    pub pass: Option<String>,
    /// When it started (RFC 3339).
    #[schema(example = "2026-10-22T14:05:40Z")]
    pub started_at: Option<String>,
}

/// A note the run wrote while it worked.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunNote {
    /// What it said.
    #[schema(example = "Tests pass; opening the pull request.")]
    pub message: Option<String>,
    /// When (RFC 3339).
    #[schema(example = "2026-10-22T14:15:02Z")]
    pub timestamp: Option<String>,
}

/// The run stopped before its pull request because a code generator needed
/// public values the repository's codegen environment doesn't set.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct CodegenEnvMissing {
    /// The repository, `owner/name`.
    #[schema(example = "acme/web")]
    pub repo: String,
    /// The generator that failed.
    #[schema(example = "pnpm run typegen")]
    pub command: String,
    /// The public variable names it needs that aren't set. Set them in the
    /// dashboard under Settings, Repositories, the repository's Codegen
    /// environment (public values only, never secrets), then retry the run.
    #[schema(example = json!(["SANITY_STUDIO_PROJECT_ID", "SANITY_STUDIO_DATASET"]))]
    pub vars: Vec<String>,
}

/// A checkpoint commit a fix round saved on its WIP branch before it stopped.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct PushedCommit {
    /// The commit's SHA.
    #[schema(example = "4f2a9c1e7b3d5a6c8e0f1a2b3c4d5e6f7a8b9c0d")]
    pub sha: String,
    /// Its subject line.
    #[schema(example = "WEB-42: Add the gallery tabs")]
    pub message: String,
}

/// The last fix round on the run's open pull request didn't finish. The
/// run's status stays `completed` (the pull request is open); this says
/// which round stopped, why, and what it pushed.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RoundIncomplete {
    /// The round: `ci_fix` (CI failed and CoderHelm was fixing it),
    /// `review_fix` (CoderHelm's reviewer asked for changes) or `feedback`
    /// (a person's review or comments).
    #[schema(example = "review_fix")]
    pub kind: Option<String>,
    /// Which fix of the run it was (1 for the first; 0 when not numbered).
    #[schema(example = 3)]
    pub number: u32,
    /// Why it stopped, in plain words.
    #[schema(example = "the review fix round ran out of its time budget 3 times in a row")]
    pub reason: String,
    /// The run's status as a person reads it.
    #[schema(
        example = "Pull request open; the last fix round didn't finish: the review fix round ran out of its time budget 3 times in a row."
    )]
    pub summary: String,
    /// The checkpoint commits the round saved on `wip_branch` before it
    /// stopped, oldest first (never on the pull request).
    pub pushed: Vec<PushedCommit>,
    /// The files those commits changed.
    #[schema(example = json!(["src/blocks/gallery/tabs.tsx"]))]
    pub files: Vec<String>,
    /// The branch the round's work so far is saved on. A round that didn't
    /// finish never pushes to the pull request; Resume continues from here.
    #[schema(example = "coderhelm-wip/web-42-gallery")]
    pub wip_branch: String,
    /// When it stopped (RFC 3339).
    #[schema(example = "2026-10-22T15:40:02Z")]
    pub at: String,
    /// Whether `POST /api/runs/{run_id}/retry` resumes the round on the same
    /// pull request.
    #[schema(example = true)]
    pub can_resume: bool,
}

/// One run, in full.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Run {
    /// The run's id, a ULID: the same id as in the path.
    #[schema(example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")]
    pub run_id: String,
    /// Where the run is: `pending`, `running`, `awaiting_ci`, `completed`,
    /// `failed`, `needs_input`, `needs_outside_change`, `cancelled`,
    /// `retried` or `closed`. Treat a value you don't know as in progress.
    #[schema(example = "completed")]
    pub status: Option<String>,
    /// What started it, such as `github`, `jira` or `alert`.
    #[schema(example = "jira")]
    pub ticket_source: Option<String>,
    /// The ticket's key or issue number in its source.
    #[schema(example = "WEB-42")]
    pub ticket_id: Option<String>,
    /// The ticket's title.
    #[schema(example = "Add rate limiting to the login form")]
    pub title: Option<String>,
    /// The repository, `owner/name`.
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// The branch the run pushed.
    #[schema(example = "coderhelm/web-42-login-rate-limit")]
    pub branch: Option<String>,
    /// The pull request the run opened.
    #[schema(example = "https://github.com/acme/web/pull/318")]
    pub pr_url: Option<String>,
    /// That pull request's number.
    #[schema(example = 318)]
    pub pr_number: Option<u64>,
    /// Every pull request, when the work spans several repositories.
    #[schema(example = json!(["https://github.com/acme/web/pull/318"]))]
    pub pr_urls: Option<Vec<String>>,
    /// Their numbers, in the same order.
    #[schema(example = json!([318]))]
    pub pr_numbers: Option<Vec<u64>>,
    /// Their branches, in the same order.
    #[schema(example = json!(["coderhelm/web-42-login-rate-limit"]))]
    pub branches: Option<Vec<String>>,
    /// The step the run is on or ended on.
    #[schema(example = "pr")]
    pub current_pass: Option<String>,
    /// Input tokens, not counting cache reads and writes.
    #[schema(example = 182340)]
    pub tokens_in: u64,
    /// Output tokens.
    #[schema(example = 20511)]
    pub tokens_out: u64,
    /// Tokens read from the prompt cache.
    #[schema(example = 905112)]
    pub cache_read_tokens: u64,
    /// Tokens written to the prompt cache.
    #[schema(example = 64002)]
    pub cache_write_tokens: u64,
    /// What the run cost in model usage, in US dollars, as recorded.
    #[schema(example = 1.42)]
    pub cost_usd: f64,
    /// The files the run changed.
    #[schema(example = json!(["src/auth/login.ts", "src/auth/login.test.ts"]))]
    pub files_modified: Option<Vec<String>>,
    /// The MCP servers the run used.
    #[schema(example = json!([]))]
    pub mcp_servers: Option<Vec<String>>,
    /// The team standards the run was given in full ("always apply").
    #[schema(example = json!(["api-error-format"]))]
    pub applied_standards: Vec<String>,
    /// How long the run took, in seconds.
    #[schema(example = 734)]
    pub duration_s: Option<u64>,
    /// Why the run failed, when it did.
    #[schema(example = json!(null))]
    pub error: Option<String>,
    /// Why an agent loop stopped: agents were paused, or an approval nobody
    /// answered in time.
    #[schema(example = json!(null))]
    pub policy_note: Option<String>,
    /// The approval card that timed out, when one did.
    #[schema(example = json!(null))]
    pub policy_approval: Option<String>,
    /// Sign-in prompts the run raised: connections it could have used that
    /// its person hadn't made (or the team hadn't set up).
    #[schema(value_type = Vec<Object>, example = json!([]))]
    pub sign_in_prompts: Vec<Value>,
    /// Where the fix belongs: `code`, or somewhere outside the code.
    #[schema(example = "code")]
    pub fix_location: Option<String>,
    /// The report written when (part of) the fix is a change outside the code.
    #[schema(example = json!(null))]
    pub outside_change_report: Option<String>,
    /// A lesson recorded for the repository when the fix was elsewhere.
    #[schema(example = json!(null))]
    pub lesson: Option<String>,
    /// Who closed the run on purpose.
    #[schema(example = json!(null))]
    pub closed_by: Option<String>,
    /// When it was closed (RFC 3339).
    #[schema(example = json!(null))]
    pub closed_at: Option<String>,
    /// Why it was closed.
    #[schema(example = json!(null))]
    pub close_reason: Option<String>,
    /// The status it had when it was closed.
    #[schema(example = json!(null))]
    pub closed_from: Option<String>,
    /// When the run started (RFC 3339).
    #[schema(example = "2026-10-22T14:03:11Z")]
    pub created_at: Option<String>,
    /// When it last changed (RFC 3339).
    #[schema(example = "2026-10-22T14:15:25Z")]
    pub updated_at: Option<String>,
    /// The steps it went through, in order.
    #[schema(example = json!([{"pass": "triage", "started_at": "2026-10-22T14:03:11Z"}, {"pass": "implement", "started_at": "2026-10-22T14:05:40Z"}, {"pass": "pr", "started_at": "2026-10-22T14:14:50Z"}]))]
    pub pass_history: Option<Vec<RunPass>>,
    /// Notes it wrote while working, in order.
    #[schema(example = json!([{"message": "Tests pass; opening the pull request.", "timestamp": "2026-10-22T14:15:02Z"}]))]
    pub progress_notes: Option<Vec<RunNote>>,
    /// What the run asked when it stopped as `needs_input`, one question
    /// each. Answer them with `POST /api/runs/{run_id}/answer`. Empty when
    /// it asked nothing it can take answers for here.
    pub questions: Vec<RunQuestion>,
    /// The comment with the questions on the ticket or issue, when known.
    #[schema(example = "https://github.com/acme/web/issues/42#issuecomment-2210")]
    pub questions_url: Option<String>,
    /// Answers people gave to its questions in CoderHelm.
    pub answers: Vec<RunAnswer>,
    /// The reply with those answers on the ticket or issue, when known.
    #[schema(example = json!(null))]
    pub answers_url: Option<String>,
    /// The run that picked the answers up and continued the work.
    #[schema(example = json!(null))]
    pub continued_run_id: Option<String>,
    /// How the run was started and by whom (as in the runs list).
    pub trigger: run_trigger::RunTrigger,
    /// Set when the run's pull request is open but the last fix round on it
    /// (a CI fix, a review fix or a feedback round) didn't finish. Null
    /// otherwise. Resume it with `POST /api/runs/{run_id}/retry`.
    #[schema(example = json!(null))]
    pub round_incomplete: Option<RoundIncomplete>,
    /// Set when the run stopped before its pull request because a code
    /// generator needed public values the repository doesn't set. Null
    /// otherwise.
    #[schema(example = json!(null))]
    pub codegen_env_missing: Option<CodegenEnvMissing>,
}

/// The public values a run's code generator was missing, as the API shows them.
fn codegen_env_missing_of(item: &run_search::Item) -> Option<CodegenEnvMissing> {
    let m = item.get("codegen_env_missing")?.as_m().ok()?;
    let s = |k: &str| {
        m.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let vars: Vec<String> = m
        .get("vars")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    (!vars.is_empty()).then(|| CodegenEnvMissing {
        repo: s("repo"),
        command: s("command"),
        vars,
    })
}

/// The run's unfinished fix round, as the API shows it.
fn round_incomplete_of(item: &run_search::Item) -> Option<RoundIncomplete> {
    let inc = common::run_round::Incomplete::from_item(item)?;
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("");
    let pr_number = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    Some(RoundIncomplete {
        kind: inc.kind.map(|k| k.as_str().to_string()),
        number: inc.number,
        summary: inc.status_line(),
        reason: inc.reason,
        pushed: inc
            .pushed
            .into_iter()
            .map(|p| PushedCommit {
                sha: p.sha,
                message: p.message,
            })
            .collect(),
        files: inc.files,
        wip_branch: inc.wip_branch,
        at: inc.at,
        can_resume: common::run_round::may_resume(status, pr_number, true),
    })
}

/// A question a run asked a person.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunQuestion {
    /// The question's id within the run (`q1`, `q2`, ...).
    #[schema(example = "q1")]
    pub id: String,
    /// The question, in plain words.
    #[schema(example = "Which environments get the new rate limit?")]
    pub question: String,
    /// Why it matters: what it blocks.
    #[schema(example = "config/limits.yml holds one value per environment.")]
    pub why: Option<String>,
    /// Candidate answers the run found in the code or the data. When
    /// `decision` is set, these are the choices.
    #[schema(example = json!(["Production only", "Every environment"]))]
    pub suggestions: Vec<String>,
    /// Set when the question is a choice CoderHelm records on the ticket and
    /// honours on every later run, such as what to do about a pull request
    /// someone else opened (`open_pr:owner/name#number`).
    #[schema(example = json!(null))]
    pub decision: Option<String>,
}

/// An answer given in CoderHelm to a run's question.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunAnswer {
    /// The question it answers.
    #[schema(example = "q1")]
    pub question_id: String,
    /// That question, as asked.
    #[schema(example = "Which environments get the new rate limit?")]
    pub question: String,
    /// The answer, as written.
    #[schema(example = "Production only.")]
    pub answer: String,
    /// Who answered, as the ticket shows them.
    #[schema(example = "Pat Doe")]
    pub author: String,
    /// When (RFC 3339).
    #[schema(example = "2026-10-22T14:20:00Z")]
    pub answered_at: String,
}

/// GET /api/runs/:run_id — get single run detail (from runs table).
#[utoipa::path(
    get,
    path = "/api/runs/{run_id}",
    tag = "Runs",
    operation_id = "getRun",
    summary = "Get a run",
    description = "Returns one run in full: its status and current step, the pull requests and \
        branches it opened, the files it changed, token use and cost, and the notes it wrote \
        while working. Poll it to follow a run; a few seconds apart is plenty.",
    params(("run_id" = String, Path, description = "The run's id.", example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")),
    responses(
        (status = 200, description = "The run.", body = Run),
        (status = 404, description = "No run with this id in the team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Run>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    // Fallback: if run-level tokens are 0/missing, sum from per-pass traces
    let tokens_in = item
        .get("tokens_in")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let tokens_out = item
        .get("tokens_out")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let cache_read = item
        .get("cache_read_tokens")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let cache_write = item
        .get("cache_write_tokens")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let cost_usd = item
        .get("cost_usd")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .unwrap_or(0.0);

    let (tokens_in, tokens_out, cache_read, cache_write, cost_usd) = if tokens_in == 0 {
        // Sum from traces as fallback
        if let Ok(traces) = state
            .dynamo
            .query()
            .table_name(&state.config.traces_table_name)
            .key_condition_expression("team_id = :tid AND begins_with(sk, :prefix)")
            .expression_attribute_values(":tid", attr_s(&claims.team_id))
            .expression_attribute_values(":prefix", attr_s(&format!("RUN#{run_id}#PASS#")))
            .send()
            .await
        {
            let mut ti: u64 = 0;
            let mut to: u64 = 0;
            let mut cr: u64 = 0;
            let mut cw: u64 = 0;
            for t in traces.items() {
                ti += t
                    .get("input_tokens")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
                to += t
                    .get("output_tokens")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
                cr += t
                    .get("cache_read_tokens")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
                cw += t
                    .get("cache_write_tokens")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
            }
            // Estimate cost from traces (use Opus rates as default)
            let cost = if ti > 0 {
                let input_rate = 15.0 / 1_000_000.0;
                let output_rate = 75.0 / 1_000_000.0;
                (ti as f64 * input_rate)
                    + (to as f64 * output_rate)
                    + (cr as f64 * input_rate * 0.1)
                    + (cw as f64 * input_rate * 1.25)
            } else {
                0.0
            };
            (ti, to, cr, cw, if cost > 0.0 { cost } else { cost_usd })
        } else {
            (tokens_in, tokens_out, cache_read, cache_write, cost_usd)
        }
    } else {
        (tokens_in, tokens_out, cache_read, cache_write, cost_usd)
    };

    let lookups = run_trigger::load_lookups(&state, &claims.team_id, [item]).await;
    Ok(Json(run_of(
        item,
        &run_id,
        RunUsage {
            tokens_in,
            tokens_out,
            cache_read,
            cache_write,
            cost_usd,
        },
        &lookups,
    )))
}

/// A run's token use and cost: the run's own, or summed from its traces.
struct RunUsage {
    tokens_in: u64,
    tokens_out: u64,
    cache_read: u64,
    cache_write: u64,
    cost_usd: f64,
}

/// The run `get_run` answers with, from its row in the runs table.
fn run_of(
    item: &run_search::Item,
    run_id: &str,
    usage: RunUsage,
    lookups: &run_trigger::Lookups,
) -> Run {
    let RunUsage {
        tokens_in,
        tokens_out,
        cache_read,
        cache_write,
        cost_usd,
    } = usage;
    let st = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let num = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
    };
    let strings = |k: &str| {
        item.get(k).and_then(|v| v.as_l().ok()).map(|list| {
            list.iter()
                .filter_map(|v| v.as_s().ok().cloned())
                .collect::<Vec<_>>()
        })
    };
    let maps = |k: &str| {
        item.get(k).and_then(|v| v.as_l().ok()).map(|list| {
            list.iter()
                .filter_map(|e| e.as_m().ok())
                .map(|m| {
                    let get = |f: &str| m.get(f).and_then(|v| v.as_s().ok()).cloned();
                    (
                        get("pass"),
                        get("started_at"),
                        get("message"),
                        get("timestamp"),
                    )
                })
                .collect::<Vec<_>>()
        })
    };
    let asked = run_asked(item);
    Run {
        trigger: run_trigger::trigger(item, lookups),
        run_id: run_id.to_string(),
        status: st("status"),
        ticket_source: st("ticket_source"),
        ticket_id: st("ticket_id"),
        title: st("title"),
        repo: st("repo"),
        branch: st("branch"),
        pr_url: st("pr_url"),
        pr_number: num("pr_number"),
        pr_urls: strings("pr_urls"),
        pr_numbers: item
            .get("pr_numbers")
            .and_then(|v| v.as_l().ok())
            .map(|list| {
                list.iter()
                    .filter_map(|v| v.as_n().ok())
                    .filter_map(|n| n.parse::<u64>().ok())
                    .collect()
            }),
        branches: strings("branches"),
        current_pass: st("current_pass"),
        tokens_in,
        tokens_out,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cost_usd,
        files_modified: strings("files_modified"),
        mcp_servers: strings("mcp_servers"),
        // Team standards the run's passes were given in full ("always apply").
        applied_standards: item
            .get("applied_standards")
            .and_then(|v| v.as_ss().ok())
            .map(|set| {
                let mut v: Vec<String> = set.to_vec();
                v.sort_unstable();
                v
            })
            .unwrap_or_default(),
        duration_s: num("duration_s"),
        error: st("error_message"),
        policy_note: st("policy_note"),
        policy_approval: st("policy_approval").filter(|s| !s.is_empty()),
        sign_in_prompts: item
            .get("sign_in_prompts")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str::<Vec<Value>>(s).ok())
            .unwrap_or_default(),
        // Where the fix belongs, and the report when (part of) it is a change
        // outside the code.
        fix_location: st("fix_location"),
        outside_change_report: st("outside_change_report"),
        // A lesson recorded for the repository when the fix was elsewhere.
        lesson: st("lesson"),
        // A person closed the run on purpose (`run_close`): who, when, why,
        // and the status it had then.
        closed_by: item
            .get("closed_by_name")
            .or_else(|| item.get("closed_by"))
            .and_then(|v| v.as_s().ok())
            .cloned(),
        closed_at: st("closed_at"),
        close_reason: st("close_reason"),
        closed_from: st("closed_from"),
        created_at: st("created_at"),
        updated_at: st("updated_at"),
        pass_history: maps("pass_history").map(|v| {
            v.into_iter()
                .map(|(pass, started_at, _, _)| RunPass { pass, started_at })
                .collect()
        }),
        progress_notes: maps("progress_notes").map(|v| {
            v.into_iter()
                .map(|(_, _, message, timestamp)| RunNote { message, timestamp })
                .collect()
        }),
        questions: asked
            .as_ref()
            .map(|a| {
                a.questions
                    .iter()
                    .map(|q| RunQuestion {
                        id: q.id.clone(),
                        question: q.question.clone(),
                        why: (!q.why.is_empty()).then(|| q.why.clone()),
                        suggestions: q.suggestions.clone(),
                        decision: (!q.decision.is_empty()).then(|| q.decision.clone()),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        questions_url: asked.and_then(|a| a.comment_url),
        answers: run_answers(item)
            .into_iter()
            .map(|a| RunAnswer {
                question_id: a.question_id,
                question: a.question,
                answer: a.answer,
                author: a.author,
                answered_at: a.answered_at,
            })
            .collect(),
        answers_url: st("answers_url"),
        continued_run_id: st("continued_run_id"),
        round_incomplete: round_incomplete_of(item),
        codegen_env_missing: codegen_env_missing_of(item),
    }
}

/// What a run asked when it stopped (see `common::run_questions`).
pub(crate) fn run_asked(item: &run_search::Item) -> Option<common::run_questions::Asked> {
    item.get(common::run_questions::FIELD_QUESTIONS)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
}

/// The answers given on a run in CoderHelm.
pub(crate) fn run_answers(item: &run_search::Item) -> Vec<common::run_questions::Answer> {
    item.get(common::run_questions::FIELD_ANSWERS)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

/// What a pass kept about what it did (e.g. which team agents reviewed a
/// plan): its JSON, or the text as kept when it isn't JSON, or null.
fn trace_detail(raw: Option<&String>) -> Value {
    match raw {
        Some(s) => serde_json::from_str(s).unwrap_or_else(|_| json!(s)),
        None => Value::Null,
    }
}

/// GET /api/runs/:run_id/traces — fetch per-pass trace records for a run.
pub async fn get_run_traces(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if state.config.traces_table_name.is_empty() {
        return Ok(Json(json!({ "traces": [] })));
    }

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.traces_table_name)
        .key_condition_expression("team_id = :tid AND begins_with(sk, :prefix)")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("RUN#{run_id}#PASS#")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query traces: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut traces: Vec<Value> = result
        .items()
        .iter()
        .map(|item| {
            json!({
                "pass": item.get("pass").and_then(|v| v.as_s().ok()),
                "duration_ms": item.get("duration_ms").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "input_tokens": item.get("input_tokens").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "output_tokens": item.get("output_tokens").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "cache_read_tokens": item.get("cache_read_tokens").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "cache_write_tokens": item.get("cache_write_tokens").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "error": item.get("error").and_then(|v| v.as_s().ok()),
                "timestamp": item.get("timestamp").and_then(|v| v.as_s().ok()),
                "cost_usd": item.get("cost_usd").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()),
                "detail": trace_detail(item.get("detail").and_then(|v| v.as_s().ok())),
            })
        })
        .collect();
    // In the order the passes ran: a repeated pass ("plan:2") sorts by key
    // after every "plan…" name, not after the first plan.
    traces.sort_by(|a, b| a["timestamp"].as_str().cmp(&b["timestamp"].as_str()));

    Ok(Json(json!({ "traces": traces })))
}

/// GET /api/runs/:run_id/agent-log — fetch conversation logs from S3.
pub async fn get_agent_log(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Verify the run belongs to this team
    let _ = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("run_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to verify run ownership for agent-log: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .ok_or(StatusCode::NOT_FOUND)?;

    // List all conversation log files under runs/{team_id}/{run_id}/conversation/
    let prefix = format!("runs/{}/{}/conversation/", claims.team_id, run_id);
    let list_result = state
        .s3
        .list_objects_v2()
        .bucket(&state.config.bucket_name)
        .prefix(&prefix)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list agent-log objects: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut passes: Vec<Value> = Vec::new();
    for obj in list_result.contents() {
        let key = obj.key().unwrap_or_default();
        let pass_name = key
            .strip_prefix(&prefix)
            .unwrap_or(key)
            .trim_end_matches(".json");
        if let Ok(output) = state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(key)
            .send()
            .await
        {
            if let Ok(bytes) = output.body.collect().await {
                if let Ok(log) = serde_json::from_slice::<Value>(&bytes.into_bytes()) {
                    passes.push(json!({
                        "pass": pass_name,
                        "conversation": log
                    }));
                }
            }
        }
    }

    // Also fetch live tool_call events from events table
    let mut live_events: Vec<Value> = Vec::new();
    if !state.config.events_table_name.is_empty() {
        if let Ok(result) = state
            .dynamo
            .query()
            .table_name(&state.config.events_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(&format!("RUN#{run_id}")))
            .expression_attribute_values(":prefix", attr_s("TOOL#"))
            .send()
            .await
        {
            for item in result.items() {
                let payload_str = item
                    .get("payload")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default();
                let payload: Value = serde_json::from_str(&payload_str).unwrap_or_default();
                let timestamp = item
                    .get("timestamp")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default();
                live_events.push(json!({
                    "tool": payload.get("tool").and_then(|v| v.as_str()).unwrap_or(""),
                    "input_summary": payload.get("input_summary").and_then(|v| v.as_str()).unwrap_or(""),
                    "duration_ms": payload.get("duration_ms").and_then(|v| v.as_u64()).unwrap_or(0),
                    "is_error": payload.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false),
                    "timestamp": timestamp,
                }));
            }
        }
    }

    Ok(Json(
        json!({ "passes": passes, "live_events": live_events }),
    ))
}

/// GET /api/runs/:run_id/openspec — fetch the four openspec files from S3.
pub async fn get_run_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // First verify the run belongs to this team
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("ticket_id, repo")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for openspec: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?;

    // Worker writes openspec under a repo-scoped prefix; fall back to the
    // legacy ticket-only prefix for runs that predate the scoping.
    let repo_scoped = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .and_then(|r| r.split_once('/'))
        .map(|(owner, name)| {
            format!(
                "teams/{}/runs/{}/openspec/{owner}__{name}",
                claims.team_id,
                ticket_id.to_lowercase()
            )
        });
    let legacy = format!(
        "teams/{}/runs/{}/openspec",
        claims.team_id,
        ticket_id.to_lowercase()
    );
    let prefixes: Vec<String> = repo_scoped.into_iter().chain([legacy]).collect();

    let mut files = serde_json::Map::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let mut found = false;
        for prefix in &prefixes {
            if found {
                break;
            }
            let key = format!("{prefix}/{name}");
            if let Ok(output) = state
                .s3
                .get_object()
                .bucket(&state.config.bucket_name)
                .key(&key)
                .send()
                .await
            {
                if let Ok(bytes) = output.body.collect().await {
                    if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                        files.insert(name.replace(".md", ""), Value::String(text));
                        found = true;
                    }
                }
            }
        }
    }

    Ok(Json(Value::Object(files)))
}

/// What a run action did.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct RunActionResult {
    /// `retrying` after a retry, `resuming` when a retry resumed the run's
    /// unfinished fix round, `cancelled` after a cancel.
    #[schema(example = "retrying")]
    pub status: String,
}

/// POST /api/runs/:run_id/retry — re-enqueue a failed run to the ticket queue.
#[utoipa::path(
    post,
    path = "/api/runs/{run_id}/retry",
    tag = "Runs",
    operation_id = "retryRun",
    summary = "Retry a run",
    description = "Starts the run's ticket again as a new run. Only a run that is `failed`, \
        `needs_input`, `needs_outside_change`, `cancelled` or `closed` can be retried; the old run \
        becomes `retried`, so retrying it twice is refused with `400`. Find the new run with \
        `GET /api/runs?q=<ticket id>`.\n\n\
        A `completed` run whose `round_incomplete` is set (its pull request is open but the last \
        fix round on it didn't finish) is resumed instead: the same run picks the round up again \
        on the same pull request, with a fresh time budget, and the answer's status is \
        `resuming`.",
    params(("run_id" = String, Path, description = "The run's id.", example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")),
    responses(
        (status = 200, description = "The ticket is queued again (`retrying`), or the unfinished round is resumed on the same run (`resuming`).", body = RunActionResult,
            example = json!({ "status": "retrying" })),
        (status = 400, description = "The run is not in a state that can be retried.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No run with this id in the team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["runs"])),
)]
pub async fn retry_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<RunActionResult>, StatusCode> {
    claims.require_role(1)?; // member+
                             // Block if token limit exceeded
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // Fetch the failed run
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    // The run's pull request is open and its last fix round didn't finish:
    // resume that round on the same run instead of starting the ticket over.
    if let Some(inc) = common::run_round::Incomplete::from_item(item) {
        let pr_number = item
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        if common::run_round::may_resume(status, pr_number, true) {
            resume_round(&state, &claims, &run_id, item, &inc, pr_number).await?;
            return Ok(Json(RunActionResult {
                status: "resuming".into(),
            }));
        }
    }
    if !matches!(
        status,
        "failed" | "needs_input" | "needs_outside_change" | "cancelled" | "closed"
    ) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let ticket = retry_message(&state, &claims, item).await?;
    // Dashboard retry: force-delete existing lock so re-enqueue always succeeds
    delete_ticket_lock(&state, &claims.team_id, &ticket.ticket_id).await;
    let message = WorkerMessage::Ticket(ticket);

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize retry message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Mark the old run as retried so it can't be retried again
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression("SET #status = :s, status_run_id = :sri, updated_at = :t")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("retried"))
        .expression_attribute_values(":sri", attr_s(&format!("retried#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;

    info!(run_id, "Run retried — dispatched to SQS");

    Ok(Json(RunActionResult {
        status: "retrying".into(),
    }))
}

/// The message that starts a run's ticket again, from the run's record:
/// the ticket, its repository (cleared for Jira, so the worker picks it
/// fresh), its body for Jira and alerts (GitHub re-reads the issue) and the
/// person asking.
pub(crate) async fn retry_message(
    state: &AppState,
    claims: &Claims,
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Result<TicketMessage, StatusCode> {
    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Prefer stored installation_id; fall back to reading from team META
    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(state, &claims.team_id).await?;
    }
    let issue_number = item
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let ticket_source = item
        .get("ticket_source")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("github");
    let source = match ticket_source {
        "jira" => TicketSource::Jira,
        "alert" => TicketSource::Alert,
        _ => TicketSource::Github,
    };

    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    // For Jira tickets, clear repo_owner/repo_name so the worker's
    // select_repo LLM can pick the correct repo fresh (the old run
    // may have been routed to the wrong repo).
    let (retry_owner, retry_name) = if matches!(source, TicketSource::Jira) {
        (String::new(), String::new())
    } else {
        (parts[0].to_string(), parts[1].to_string())
    };

    // For Jira, restore the ticket body from the run record (GitHub re-fetches from API)
    let body = if matches!(source, TicketSource::Jira | TicketSource::Alert) {
        item.get("ticket_body")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    Ok(TicketMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        source,
        ticket_id: ticket_id.to_string(),
        title: item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        body,
        repo_owner: retry_owner,
        repo_name: retry_name,
        issue_number,
        sender: claims
            .github_login
            .clone()
            .unwrap_or_else(|| claims.email.clone()),
        requester: common::requester::Requester::member(
            claims.github_login.as_deref(),
            &claims.email,
        ),
        image_attachments: item
            .get("image_attachments")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default(),
        ..Default::default()
    })
}

/// How Resume picks up a run's unfinished fix round.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResumeRoute {
    /// The round's own instructions are kept: park it, and the resume runs
    /// it first with a fresh budget.
    Parked(common::run_round::Round),
    /// Nothing kept to run directly: the resume reads CI and the review on
    /// the head again, which brings the fix back.
    Recheck,
    /// A person's comments: the feedback round reads them again.
    Feedback,
}

fn resume_route(
    inc: &common::run_round::Incomplete,
    now: chrono::DateTime<chrono::Utc>,
) -> ResumeRoute {
    use common::run_round::Kind;
    match inc.kind {
        Some(Kind::Feedback) => ResumeRoute::Feedback,
        Some(_) if !inc.feedback.trim().is_empty() => match inc.resume_round(now) {
            Some(r) => ResumeRoute::Parked(r),
            None => ResumeRoute::Recheck,
        },
        _ => ResumeRoute::Recheck,
    }
}

/// Resumes a run's unfinished fix round on its open pull request: the run
/// goes back to work under the same id, and the round gets a fresh budget.
/// A person asking is one more fix: one slot of the automated-fix budget is
/// given back so the round isn't refused for the budget it already used.
async fn resume_round(
    state: &AppState,
    claims: &Claims,
    run_id: &str,
    item: &run_search::Item,
    inc: &common::run_round::Incomplete,
    pr_number: u64,
) -> Result<(), StatusCode> {
    let text = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let repo = text("repo");
    let (owner, name) = repo
        .split_once('/')
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(state, &claims.team_id).await?;
    }
    let now = chrono::Utc::now();
    let route = resume_route(inc, now);
    let (status, pass) = match route {
        ResumeRoute::Feedback => ("running", "feedback"),
        _ => ("awaiting_ci", "awaiting_ci"),
    };
    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(run_id))
        .condition_expression("#s = :from")
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", common::run_round::INCOMPLETE_FIELD)
        .expression_attribute_names("#round", common::run_round::FIELD)
        .expression_attribute_values(":from", attr_s(&text("status")))
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(":p", attr_s(pass))
        .expression_attribute_values(":t", attr_s(&now.to_rfc3339()));
    update = match &route {
        ResumeRoute::Parked(round) => update
            .update_expression(
                "SET #s = :s, status_run_id = :sri, current_pass = :p, updated_at = :t, #round = :r \
                 REMOVE #inc",
            )
            .expression_attribute_values(":r", round.to_attr()),
        _ => update.update_expression(
            "SET #s = :s, status_run_id = :sri, current_pass = :p, updated_at = :t \
             REMOVE #inc, #round",
        ),
    };
    update.send().await.map_err(|e| {
        if common::run_fail::is_condition_failed(&e) {
            StatusCode::CONFLICT
        } else {
            error!("Failed to resume the run's round: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    })?;
    // One automated-fix slot back for the person's ask (best effort).
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("ADD auto_fix_pushes :minus")
        .condition_expression("auto_fix_pushes > :zero")
        .expression_attribute_values(":minus", AttributeValue::N("-1".into()))
        .expression_attribute_values(":zero", AttributeValue::N("0".into()))
        .send()
        .await;
    let (queue, message) = match route {
        ResumeRoute::Feedback => (
            &state.config.feedback_queue_url,
            WorkerMessage::Feedback(FeedbackMessage {
                team_id: claims.team_id.clone(),
                installation_id,
                run_id: run_id.to_string(),
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                review_id: 0,
                ..Default::default()
            }),
        ),
        _ => (
            &state.config.ticket_queue_url,
            WorkerMessage::Resume(common::messages::ResumeMessage {
                team_id: claims.team_id.clone(),
                run_id: run_id.to_string(),
                installation_id,
                ..Default::default()
            }),
        ),
    };
    let body = serde_json::to_string(&message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .sqs
        .send_message()
        .queue_url(queue)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue the resumed round: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    info!(run_id, pr_number, "Unfinished fix round resumed");
    Ok(())
}

/// Close a PR and delete a branch via the GitHub App token. Best-effort.
async fn github_reset_branch(
    state: &AppState,
    installation_id: u64,
    owner: &str,
    repo: &str,
    branch: &str,
    pr_number: u64,
) {
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            warn!("reset: could not get installation token: {e}");
            return;
        }
    };
    // Close the PR (if any) so the fresh run doesn't reuse it.
    if pr_number > 0 {
        let url = format!("https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}");
        let _ = state
            .http
            .patch(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .json(&serde_json::json!({ "state": "closed" }))
            .send()
            .await;
    }
    // Delete the working branch so the run starts from a clean base. GitHub
    // 422s if it's already gone — fine.
    if !branch.is_empty() {
        let url = format!("https://api.github.com/repos/{owner}/{repo}/git/refs/heads/{branch}");
        let _ = state
            .http
            .delete(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await;
    }
}

/// Whether a run may be started over. A run still in flight would have its
/// branch deleted out from under the worker and race a duplicate run (cancel
/// it first); a merged run shipped, and starting over would rebuild the same
/// work as a second PR. Same rules as `trigger_gate`.
fn reset_allowed(status: &str) -> bool {
    !matches!(
        status,
        "pending" | "queued" | "running" | "awaiting_ci" | "merged"
    )
}

/// POST /api/runs/:run_id/reset — close the PR, delete the branch, and start
/// a brand-new run from scratch. Unlike retry, works on finished runs of any
/// outcome (not in-flight or merged ones, see `reset_allowed`) — the "start
/// over cleanly" control for a run whose branch is corrupted or whose work
/// should be thrown away.
pub async fn reset_and_rerun(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for reset: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    if !reset_allowed(status) {
        return Err(StatusCode::CONFLICT);
    }

    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(&state, &claims.team_id).await?;
    }

    let branch = item
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let pr_number = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    // Clean the GitHub side: close PR + delete branch.
    github_reset_branch(
        &state,
        installation_id,
        parts[0],
        parts[1],
        &branch,
        pr_number,
    )
    .await;

    let ticket_source = item
        .get("ticket_source")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("github");
    let source = match ticket_source {
        "jira" => TicketSource::Jira,
        "alert" => TicketSource::Alert,
        _ => TicketSource::Github,
    };
    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let issue_number = item
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    delete_ticket_lock(&state, &claims.team_id, ticket_id).await;

    let (r_owner, r_name) = if matches!(source, TicketSource::Jira) {
        (String::new(), String::new())
    } else {
        (parts[0].to_string(), parts[1].to_string())
    };
    let body = if matches!(source, TicketSource::Jira | TicketSource::Alert) {
        item.get("ticket_body")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        source,
        ticket_id: ticket_id.to_string(),
        title: item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        body,
        repo_owner: r_owner,
        repo_name: r_name,
        issue_number,
        sender: claims
            .github_login
            .clone()
            .unwrap_or_else(|| claims.email.clone()),
        requester: common::requester::Requester::member(
            claims.github_login.as_deref(),
            &claims.email,
        ),
        image_attachments: item
            .get("image_attachments")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default(),
        ..Default::default()
    });
    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize reset message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue reset run: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression("SET #status = :s, status_run_id = :sri, updated_at = :t")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("retried"))
        .expression_attribute_values(":sri", attr_s(&format!("retried#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;

    info!(
        run_id,
        "Run reset — branch cleaned and fresh run dispatched"
    );
    Ok(Json(json!({ "status": "resetting" })))
}

/// POST /api/runs/:run_id/re-review — re-enqueue feedback for a completed run.
pub async fn re_review_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+
                             // Block if token limit exceeded
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for re-review: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    // A closed run stays closed until a person reopens it; a re-review would
    // set it running again behind their back.
    if status == "merged" || status == common::run_status::CLOSED {
        return Err(StatusCode::CONFLICT);
    }

    let pr_number = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if pr_number == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(&state, &claims.team_id).await?;
    }

    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        run_id: run_id.clone(),
        repo_owner: parts[0].to_string(),
        repo_name: parts[1].to_string(),
        pr_number,
        review_id: 0,
        review_body: String::new(),
        comments: vec![],
        // Not a GitHub comment: nobody to address on the PR.
        trigger_author: String::new(),
        ..Default::default()
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize re-review message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Set status to running BEFORE sending SQS — prevents the feedback handler's
    // terminal-run guard from seeing stale "completed" status and skipping.
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t, current_pass = :p \
             REMOVE error_message, #err",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":p", attr_s("feedback"))
        .send()
        .await;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue re-review: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(run_id, pr_number, "Re-review dispatched to feedback queue");
    Ok(Json(json!({ "status": "re-reviewing" })))
}

/// POST /api/runs/:run_id/cancel — cancel a running job.
#[utoipa::path(
    post,
    path = "/api/runs/{run_id}/cancel",
    tag = "Runs",
    operation_id = "cancelRun",
    summary = "Cancel a run",
    description = "Stops a run that is `running`. The run becomes `cancelled`, and the work it \
        has not done yet is dropped; a pull request it already opened stays open. A run in any \
        other state answers `400`, so cancelling twice is harmless.",
    params(("run_id" = String, Path, description = "The run's id.", example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")),
    responses(
        (status = 200, description = "The run is cancelled.", body = RunActionResult,
            example = json!({ "status": "cancelled" })),
        (status = 400, description = "The run is not running (or doesn't exist).", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["runs"])),
)]
pub async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<RunActionResult>, StatusCode> {
    claims.require_role(1)?; // member+
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t, cancelled_by = :cb, cancel_reason = :cr",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("cancelled"))
        .expression_attribute_values(":sri", attr_s(&format!("cancelled#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cb", attr_s(&claims.email))
        .expression_attribute_values(":cr", attr_s("cancelled_by_user"))
        .condition_expression("#s = :running")
        .expression_attribute_values(":running", attr_s("running"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to cancel run {run_id}: {e}");
            StatusCode::BAD_REQUEST
        })?;

    info!(run_id, "Run cancelled by user");
    Ok(Json(RunActionResult {
        status: "cancelled".into(),
    }))
}

/// A repository connected to the team.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Repo {
    /// `owner/name`.
    #[schema(example = "acme/web")]
    pub name: String,
    /// Whether CoderHelm works on it.
    #[schema(example = true)]
    pub enabled: Option<bool>,
    /// Where its tickets come from: `github` or `jira`.
    #[schema(example = "github")]
    pub ticket_source: Option<String>,
    /// How far CoderHelm got reading it when it was connected, such as
    /// `pending`, `ready` or `failed`.
    #[schema(example = "ready")]
    pub onboard_status: Option<String>,
    /// Why reading it failed, when it did.
    #[schema(example = json!(null))]
    pub onboard_error: Option<String>,
    /// What the repository is for, as the team set it, such as `web` or `backend`.
    #[schema(example = "web")]
    pub role: Option<String>,
}

/// The team's repositories.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct ReposList {
    /// Every connected repository, in name order.
    pub repos: Vec<Repo>,
}

/// GET /api/repos — list repos configured for this team.
#[utoipa::path(
    get,
    path = "/api/repos",
    tag = "Repositories",
    operation_id = "listRepos",
    summary = "List repositories",
    description = "Lists the repositories connected to the team through the CoderHelm GitHub App, \
        whether CoderHelm works on each, and where its tickets come from. Not paged: a team's \
        repositories come back in one answer.",
    responses(
        (status = 200, description = "The team's repositories.", body = ReposList),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<ReposList>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repos: Vec<Repo> = result
        .items()
        .iter()
        .filter_map(|item| {
            let st = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
            Some(Repo {
                name: st("repo_name")?,
                enabled: item.get("enabled").and_then(|v| v.as_bool().ok()).copied(),
                ticket_source: st("ticket_source"),
                onboard_status: st("onboard_status"),
                onboard_error: st("onboard_error"),
                role: st("role"),
            })
        })
        .collect();

    Ok(Json(ReposList { repos }))
}

/// POST /api/repos/sync — fetch repos from GitHub API and write missing ones to DynamoDB.
/// Used when the initial installation webhook didn't include repos (e.g. "All repositories" access).
pub async fn sync_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
                             // Get installation_id from META record
    let meta = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch team meta: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let installation_id = meta
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or_else(|| {
            warn!("No installation_id found for team");
            StatusCode::NOT_FOUND
        })?;

    // Fetch repos from GitHub API
    let token = crate::auth::github_app::get_installation_token(&state, installation_id)
        .await
        .map_err(|e| {
            error!("Failed to get installation token: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let mut fetched: Vec<OnboardRepo> = Vec::new();
    let mut page = 1u32;
    loop {
        let url =
            format!("https://api.github.com/installation/repositories?per_page=100&page={page}");
        let resp = state
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await
            .map_err(|e| {
                error!("GitHub API request failed: {e}");
                StatusCode::BAD_GATEWAY
            })?
            .error_for_status()
            .map_err(|e| {
                error!("GitHub API error: {e}");
                StatusCode::BAD_GATEWAY
            })?;

        let body: Value = resp.json().await.map_err(|e| {
            error!("Failed to parse GitHub response: {e}");
            StatusCode::BAD_GATEWAY
        })?;

        if let Some(arr) = body["repositories"].as_array() {
            for r in arr {
                if let Some(full_name) = r["full_name"].as_str() {
                    let parts: Vec<&str> = full_name.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        fetched.push(OnboardRepo {
                            owner: parts[0].to_string(),
                            name: parts[1].to_string(),
                            default_branch: r["default_branch"]
                                .as_str()
                                .unwrap_or("main")
                                .to_string(),
                        });
                    }
                }
            }
            if arr.len() < 100 {
                break;
            }
        } else {
            break;
        }
        page += 1;
    }

    // Write any repos not already in DynamoDB
    let now = chrono::Utc::now().to_rfc3339();
    let mut added = 0u64;
    for repo in &fetched {
        let full = format!("{}/{}", repo.owner, repo.name);
        let sk = format!("REPO#{full}");

        // Only write if not already present
        let exists = state
            .dynamo
            .get_item()
            .table_name(&state.config.repos_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .projection_expression("pk")
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned())
            .is_some();

        if !exists {
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.repos_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s(&sk))
                .item("repo_name", attr_s(&full))
                .item("enabled", AttributeValue::Bool(false))
                .item("ticket_source", attr_s("github"))
                .item("created_at", attr_s(&now))
                .send()
                .await;
            added += 1;
        }
    }

    info!(total = fetched.len(), added, "Synced repos from GitHub API");

    Ok(Json(json!({ "total": fetched.len(), "added": added })))
}

/// GET /api/integrations/jira/check — quick Jira integration readiness check.
pub async fn get_jira_integration_check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let repos_result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repo_items = repos_result.items();
    let repo_count = repo_items.len();
    let enabled_repos: Vec<_> = repo_items
        .iter()
        .filter(|item| {
            item.get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true)
        })
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?;
            Some(sk.strip_prefix("REPO#")?.to_string())
        })
        .collect();
    let enabled_repo_count = enabled_repos.len();

    let runs_result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .scan_index_forward(false)
        .limit(25)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let jira_runs: Vec<_> = runs_result
        .items()
        .iter()
        .filter(|item| {
            item.get("ticket_source")
                .and_then(|v| v.as_s().ok())
                .map(|v| v == "jira")
                .unwrap_or(false)
        })
        .collect();
    let jira_seen = !jira_runs.is_empty();
    let last_jira_event_at = jira_runs
        .first()
        .and_then(|item| item.get("created_at"))
        .and_then(|v| v.as_s().ok())
        .cloned();

    // Count actual Jira webhook events from the events table
    let jira_event_count = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .select(aws_sdk_dynamodb::types::Select::Count)
        .send()
        .await
        .map(|r| r.count())
        .unwrap_or(0);

    // Load per-team JIRA secret from DynamoDB
    let team_secret = load_jira_secret(&state, &claims.team_id).await;
    let secret_configured = team_secret.is_some() || state.secrets.jira_webhook_secret.is_some();

    // Load installation_id from team META
    let meta_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    let installation_id = meta_result
        .as_ref()
        .and_then(|item| item.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let webhook_url = team_secret.as_ref().map(|(token, _)| {
        format!(
            "{}/webhooks/jira/{token}",
            common::public_urls::api_url(&state.config.stage)
        )
    });

    // Load or generate forge_secret for the Forge app integration
    let jira_config = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    let forge_secret = jira_config
        .as_ref()
        .and_then(|item| item.get("forge_secret"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());

    // Forge app is connected if it has registered its trigger URLs
    let forge_connected = jira_config
        .as_ref()
        .and_then(|item| item.get("add_comment_url"))
        .and_then(|v| v.as_s().ok())
        .is_some();

    let ready = (secret_configured || forge_connected) && enabled_repo_count > 0;

    let forge_secret = match forge_secret {
        Some(s) => s,
        None => {
            use rand::Rng;
            let secret: String = rand::rng()
                .sample_iter(&rand::distr::Alphanumeric)
                .take(48)
                .map(char::from)
                .collect();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s("JIRA#config"))
                .update_expression("SET forge_secret = :fs")
                .expression_attribute_values(":fs", attr_s(&secret))
                .send()
                .await;
            secret
        }
    };

    Ok(Json(json!({
        "ready": ready,
        "secret_configured": secret_configured,
        "configured_repos": {
            "total": repo_count,
            "enabled": enabled_repo_count
        },
        "enabled_repos": enabled_repos,
        "jira_events_seen": jira_seen,
        "last_jira_event_at": last_jira_event_at,
        "jira_event_count": jira_event_count,
        "installation_id": installation_id,
        "team_id": claims.team_id.strip_prefix("TEAM#").unwrap_or(&claims.team_id),
        "webhook_url": webhook_url,
        "forge_secret": forge_secret,
    })))
}

/// POST /api/integrations/jira/secret — generate a Jira webhook token + signing secret.
/// Creates an opaque URL token (stored in jira-tokens table) and a separate HMAC
/// signing secret the user pastes into Jira's webhook config for signature verification.
pub async fn generate_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    use rand::Rng;

    let token: String = rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    let webhook_secret: String = rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();

    let now = chrono::Utc::now().to_rfc3339();

    // Delete old token from jira-tokens table if one exists
    let old = load_jira_secret(&state, &claims.team_id).await;
    if let Some(old_token) = old.as_ref().map(|(t, _)| t) {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(old_token))
            .send()
            .await;
    }

    // Load installation_id
    let install_id = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| {
            item.get("github_install_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
        })
        .unwrap_or(0);

    // Store token + webhook_secret on team record (main table)
    state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_config_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("JIRA_SECRET"))
        .item("secret", attr_s(&token))
        .item("webhook_secret", attr_s(&webhook_secret))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to store JIRA token: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Store token → team mapping in jira-tokens table (with conditional write)
    let put_result = state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_tokens_table_name)
        .item("token", attr_s(&token))
        .item("team_id", attr_s(&claims.team_id))
        .item("installation_id", AttributeValue::N(install_id.to_string()))
        .item("webhook_secret", attr_s(&webhook_secret))
        .item("created_at", attr_s(&now))
        .condition_expression("attribute_not_exists(#t)")
        .expression_attribute_names("#t", "token")
        .send()
        .await;

    if let Err(e) = put_result {
        if e.as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false)
        {
            error!("JIRA token collision detected — client should retry");
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s("JIRA_SECRET"))
                .send()
                .await;
            return Err(StatusCode::CONFLICT);
        }
        error!("Failed to store JIRA token lookup: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    info!(team_id = %claims.team_id, "Generated Jira webhook token + signing secret");
    Ok(Json(
        json!({ "token": token, "webhook_secret": webhook_secret }),
    ))
}

/// DELETE /api/integrations/jira/secret — remove the Jira webhook token.
pub async fn delete_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
                             // Delete token from jira-tokens table first
    let old = load_jira_secret(&state, &claims.team_id).await;
    if let Some((old_token, _)) = old {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA_SECRET"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete JIRA token: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team_id = %claims.team_id, "Deleted Jira webhook token");
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/integrations/jira — remove the entire Jira integration.
pub async fn delete_jira_integration(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(4)?; // owner only
    let tid = &claims.team_id;

    // 1. Delete the webhook token from the jira-tokens lookup table
    if let Some((old_token, _)) = load_jira_secret(&state, tid).await {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    // 2. Delete all items in the jira-config table for this team (config, secret, projects)
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_config_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s(tid))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query jira config for delete: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in result.items() {
        if let (Some(pk), Some(sk)) = (item.get("pk"), item.get("sk")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", pk.clone())
                .key("sk", sk.clone())
                .send()
                .await;
        }
    }

    // 3. Delete jira events for this team
    if let Ok(events) = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .send()
        .await
    {
        for item in events.items() {
            if let (Some(tid_attr), Some(eid)) = (item.get("team_id"), item.get("event_id")) {
                let _ = state
                    .dynamo
                    .delete_item()
                    .table_name(&state.config.jira_events_table_name)
                    .key("team_id", tid_attr.clone())
                    .key("event_id", eid.clone())
                    .send()
                    .await;
            }
        }
    }

    info!(team_id = %tid, "Deleted entire Jira integration");
    Ok(StatusCode::NO_CONTENT)
}

/// Load per-team JIRA token and webhook secret from DynamoDB.
/// Returns (token, webhook_secret) if configured.
pub async fn load_jira_secret(state: &AppState, team_id: &str) -> Option<(String, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("JIRA_SECRET".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)?;
    let token = item.get("secret").and_then(|v| v.as_s().ok()).cloned()?;
    let webhook_secret = item
        .get("webhook_secret")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    Some((token, webhook_secret))
}

/// GET /api/integrations/jira/events — list recent Jira webhook events for the Events tab.
pub async fn get_jira_events(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let limit: i32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .min(100);

    let events =
        crate::routes::jira_webhook::list_jira_events(&state, &claims.team_id, limit).await?;

    Ok(Json(json!({ "events": events, "total": events.len() })))
}

/// POST /integrations/jira/forge-register — called by the Forge admin page to register trigger URLs.
/// Public endpoint, verified by checking the team exists.
pub async fn forge_register_urls(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let raw_team_id = body
        .get("team_id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    // Hosted side of hybrid: a Forge app set up for a self-hosted install
    // registers its links with that install, through the relay.
    if common::hybrid::sign::is_install_id(raw_team_id) {
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        if let Some(code) = super::hybrid::relay_jira(&state, raw_team_id, "register", &bytes).await
        {
            return Ok(code);
        }
    }
    let team_id = if raw_team_id.starts_with("TEAM#") {
        raw_team_id.to_string()
    } else {
        format!("TEAM#{raw_team_id}")
    };
    let forge_secret = body
        .get("forge_secret")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let list_projects_url = body
        .get("list_projects_url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let create_ticket_url = body
        .get("create_ticket_url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let add_comment_url = body
        .get("add_comment_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let site_url = body.get("site_url").and_then(|v| v.as_str()).unwrap_or("");
    // Newer app versions also register the release-notes triggers (Confluence
    // publish + Jira issue lookup). Older installs omit them → stored empty.
    let https_or_empty = |k: &str| -> &str {
        body.get(k)
            .and_then(|v| v.as_str())
            .filter(|u| u.starts_with("https://"))
            .unwrap_or("")
    };
    let publish_release_url = https_or_empty("publish_release_url");
    let get_issues_url = https_or_empty("get_issues_url");
    // Sent only when Confluence answered the app on its site; agents then
    // read Confluence through the app (`common::live::jira_app`).
    let confluence_trigger_url = https_or_empty(common::live::jira_app::CONFLUENCE_TRIGGER_FIELD);

    // Verify the team exists
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .map_err(|e| {
            error!("forge-register: failed to query team META: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item
        .ok_or_else(|| {
            warn!("forge-register: team not found: {team_id}");
            StatusCode::NOT_FOUND
        })?;

    // Verify forge_secret matches the stored secret
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .map_err(|e| {
            error!("forge-register: failed to load JIRA#config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item;
    let stored_secret = config_item
        .as_ref()
        .and_then(|item| item.get("forge_secret"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str());
    // Compared in constant time, as the Forge webhook compares it.
    match stored_secret {
        Some(s) if super::jira_webhook::forge_secret_matches(s, forge_secret) => {}
        Some(_) => {
            warn!(team_id, "forge-register: invalid forge_secret");
            return Err(StatusCode::FORBIDDEN);
        }
        None => {
            warn!(
                team_id,
                "forge-register: no forge_secret configured — run /jira/check first"
            );
            return Err(StatusCode::FORBIDDEN);
        }
    }

    // Save trigger URLs to JIRA#config
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&team_id))
        .key("sk", attr_s("JIRA#config"))
        .update_expression(
            "SET list_projects_url = :lpu, create_ticket_url = :ctu, add_comment_url = :acu, site_url = :su, publish_release_url = :pru, get_issues_url = :giu, confluence_trigger_url = :cfu, updated_at = :ua",
        )
        .expression_attribute_values(":pru", attr_s(publish_release_url))
        .expression_attribute_values(":giu", attr_s(get_issues_url))
        .expression_attribute_values(":cfu", attr_s(confluence_trigger_url))
        .expression_attribute_values(":lpu", attr_s(list_projects_url))
        .expression_attribute_values(":ctu", attr_s(create_ticket_url))
        .expression_attribute_values(":acu", attr_s(add_comment_url))
        .expression_attribute_values(":su", attr_s(site_url))
        .expression_attribute_values(":ua", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("forge-register: failed to save URLs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team_id, "Forge trigger URLs registered");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/integrations/jira/config — get Jira config (trigger URLs, default project, label, projects).
pub async fn get_jira_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Load JIRA#config item
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to load Jira config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item;

    let get_str = |item: &std::collections::HashMap<String, AttributeValue>, key: &str| {
        item.get(key)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };

    let (
        default_project,
        trigger_label,
        list_projects_url,
        create_ticket_url,
        add_comment_url,
        site_url,
    ) = if let Some(ref item) = config_item {
        (
            get_str(item, "default_project"),
            get_str(item, "trigger_label"),
            get_str(item, "list_projects_url"),
            get_str(item, "create_ticket_url"),
            get_str(item, "add_comment_url"),
            get_str(item, "site_url"),
        )
    } else {
        (
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        )
    };

    // Load enabled projects (JIRA#PROJECT#<key> items)
    let projects_result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_config_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("JIRA#PROJECT#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query Jira projects: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let projects: Vec<Value> = projects_result
        .items()
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?;
            let key = sk.strip_prefix("JIRA#PROJECT#")?;
            let name = item
                .get("project_name")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or(key);
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let repo = item
                .get("repo")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            Some(json!({ "key": key, "name": name, "enabled": enabled, "repo": repo }))
        })
        .collect();

    Ok(Json(json!({
        "default_project": default_project,
        "trigger_label": trigger_label,
        "list_projects_url": list_projects_url,
        "create_ticket_url": create_ticket_url,
        "add_comment_url": add_comment_url,
        "site_url": site_url,
        "projects": projects,
    })))
}

/// PUT /api/integrations/jira/config — update Jira config.
pub async fn update_jira_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let now = chrono::Utc::now().to_rfc3339();

    let mut update_expr = vec!["updated_at = :ua".to_string()];
    let mut attr_vals: Vec<(String, AttributeValue)> = vec![(":ua".to_string(), attr_s(&now))];

    for (field, key) in [
        ("default_project", "default_project"),
        ("trigger_label", "trigger_label"),
        ("list_projects_url", "list_projects_url"),
        ("create_ticket_url", "create_ticket_url"),
        ("add_comment_url", "add_comment_url"),
        ("site_url", "site_url"),
    ] {
        if let Some(val) = body.get(field).and_then(|v| v.as_str()) {
            update_expr.push(format!("{key} = :{key}"));
            attr_vals.push((format!(":{key}"), attr_s(val)));
        }
    }

    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .update_expression(format!("SET {}", update_expr.join(", ")));

    for (k, v) in attr_vals {
        update = update.expression_attribute_values(k, v);
    }

    update.send().await.map_err(|e| {
        error!("Failed to update Jira config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    info!(team_id = %claims.team_id, "Updated Jira config");
    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/integrations/jira/projects — save enabled/disabled project list.
pub async fn update_jira_projects(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let projects = body
        .get("projects")
        .and_then(|v| v.as_array())
        .ok_or(StatusCode::BAD_REQUEST)?;

    for proj in projects {
        let key = proj
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or(StatusCode::BAD_REQUEST)?;
        let name = proj.get("name").and_then(|v| v.as_str()).unwrap_or(key);
        let enabled = proj
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let repo = proj.get("repo").and_then(|v| v.as_str()).unwrap_or("");

        let mut put = state
            .dynamo
            .put_item()
            .table_name(&state.config.jira_config_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s(&format!("JIRA#PROJECT#{key}")))
            .item("project_name", attr_s(name))
            .item("enabled", AttributeValue::Bool(enabled));

        if !repo.is_empty() {
            put = put.item("repo", attr_s(repo));
        }

        put.send().await.map_err(|e| {
            error!("Failed to save Jira project {key}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    info!(team_id = %claims.team_id, count = projects.len(), "Updated Jira projects");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/integrations/jira/projects/fetch — proxy call to Forge web trigger to list projects.
pub async fn fetch_jira_projects(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Load the list-projects URL from Jira config
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to load Jira config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item
        .ok_or(StatusCode::NOT_FOUND)?;

    let url = config_item
        .get("list_projects_url")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            warn!(team_id = %claims.team_id, "No list_projects_url configured");
            StatusCode::NOT_FOUND
        })?;

    let resp = state
        .http
        .post(url)
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to call Forge list-projects: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    if !resp.status().is_success() {
        error!(
            status = resp.status().as_u16(),
            "Forge list-projects returned error"
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    let body: Value = resp.json().await.map_err(|e| {
        error!("Failed to parse Forge list-projects response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(body))
}

/// POST /api/integrations/jira/check — validate a candidate Jira payload before wiring automation.
pub async fn validate_jira_integration_payload(
    Extension(_claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let top = |key: &str| body.get(key).and_then(|v| v.as_str()).map(str::to_string);
    let nested = |path1: &str, path2: &str| {
        body.get(path1)
            .and_then(|v| v.get(path2))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };

    let repo_owner = top("repo_owner").or_else(|| nested("coderhelm", "repo_owner"));
    let repo_name = top("repo_name").or_else(|| nested("coderhelm", "repo_name"));

    let installation_id = body
        .get("installation_id")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            body.get("coderhelm")
                .and_then(|v| v.get("installation_id"))
                .and_then(|v| v.as_u64())
        });
    let team_id = top("team_id").or_else(|| nested("coderhelm", "team_id"));

    let issue_key = body
        .get("issue")
        .and_then(|v| v.get("key"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let issue_summary = body
        .get("issue")
        .and_then(|v| v.get("fields"))
        .and_then(|v| v.get("summary"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("title"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let mut missing = Vec::new();
    if repo_owner.is_none() {
        missing.push("repo_owner (or coderhelm.repo_owner)");
    }
    if repo_name.is_none() {
        missing.push("repo_name (or coderhelm.repo_name)");
    }
    if installation_id.is_none() && team_id.is_none() {
        missing.push("installation_id (or team_id)");
    }
    if issue_key.is_none() {
        missing.push("issue.key (or issue.id)");
    }
    if issue_summary.is_none() {
        missing.push("issue.fields.summary (or issue.title)");
    }

    let valid = missing.is_empty();

    Ok(Json(json!({
        "valid": valid,
        "missing": missing,
        "normalized_preview": {
            "repo_owner": repo_owner,
            "repo_name": repo_name,
            "installation_id": installation_id,
            "team_id": team_id,
            "ticket_id": issue_key,
            "title": issue_summary,
            "event": body.get("webhookEvent").and_then(|v| v.as_str()).unwrap_or("unknown")
        },
        "next_step": if valid {
            "Payload shape looks good. Point Jira automation/webhook to /webhooks/jira."
        } else {
            "Fix missing fields above and run this check again."
        }
    })))
}

/// Valid repo role designations. A repo's role tells the Jira repo-selector which
/// repo owns a domain, so an ambiguous ticket (e.g. a Tailwind change matching two
/// frontends) resolves to the team's designated repo instead of an LLM guess.
pub const VALID_REPO_ROLES: &[&str] = &["web", "backend", "mobile", "cms", "infra", "data"];

/// POST /api/repos/:owner/:name/role — set (or clear, with "") a repo's role.
pub async fn set_repo_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let role = body["role"].as_str().unwrap_or("").trim().to_lowercase();
    let now = chrono::Utc::now().to_rfc3339();

    let base = state
        .dynamo
        .update_item()
        .table_name(&state.config.repos_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("REPO#{repo}")))
        // Only designate a repo that exists (was synced) — never mint a stub.
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_names("#role", "role")
        .expression_attribute_values(":ua", attr_s(&now));

    let update = if role.is_empty() {
        base.update_expression("SET updated_at = :ua REMOVE #role")
    } else {
        if !VALID_REPO_ROLES.contains(&role.as_str()) {
            return Err(StatusCode::BAD_REQUEST);
        }
        base.update_expression("SET #role = :role, updated_at = :ua")
            .expression_attribute_values(":role", attr_s(&role))
    };

    update.send().await.map_err(|e| {
        error!("Failed to set repo role for {repo}: {e}");
        // ConditionalCheckFailed (unknown repo) is a client error.
        StatusCode::BAD_REQUEST
    })?;

    Ok(StatusCode::OK)
}

/// POST /api/repos/:owner/:name — update repo config.
pub async fn update_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let enabled = body["enabled"].as_bool().unwrap_or(true);
    let now = chrono::Utc::now().to_rfc3339();

    if enabled {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.repos_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&format!("REPO#{repo}")))
            .update_expression(
                "SET enabled = :e, ticket_source = :ts, repo_name = :rn, onboard_status = :os, updated_at = :ua",
            )
            .expression_attribute_values(":e", AttributeValue::Bool(true))
            .expression_attribute_values(":ts", attr_s("github"))
            .expression_attribute_values(":rn", attr_s(&repo))
            .expression_attribute_values(":os", attr_s("pending"))
            .expression_attribute_values(":ua", attr_s(&now))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to update repo: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    } else {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.repos_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&format!("REPO#{repo}")))
            .update_expression("SET enabled = :e, updated_at = :ua")
            .expression_attribute_values(":e", AttributeValue::Bool(false))
            .expression_attribute_values(":ua", attr_s(&now))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to update repo: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    // When enabling a repo, trigger onboarding to generate voice, instructions, etc.
    if enabled {
        let installation_id = get_team_installation_id(&state, &claims.team_id).await?;

        // Fetch actual default branch from GitHub API
        let default_branch =
            match crate::auth::github_app::get_installation_token(&state, installation_id).await {
                Ok(token) => {
                    let url = format!("https://api.github.com/repos/{owner}/{name}");
                    match state
                        .http
                        .get(&url)
                        .header("Authorization", format!("Bearer {token}"))
                        .header("Accept", "application/vnd.github+json")
                        .header("User-Agent", "Coderhelm-bot")
                        .send()
                        .await
                    {
                        Ok(resp) => resp
                            .json::<Value>()
                            .await
                            .ok()
                            .and_then(|v| v["default_branch"].as_str().map(String::from))
                            .unwrap_or_else(|| "main".to_string()),
                        Err(_) => "main".to_string(),
                    }
                }
                Err(_) => "main".to_string(),
            };

        let message = WorkerMessage::Onboard(OnboardMessage {
            team_id: claims.team_id.clone(),
            installation_id,
            repos: vec![OnboardRepo {
                owner: owner.clone(),
                name: name.clone(),
                default_branch,
            }],
        });

        if let Ok(body) = serde_json::to_string(&message) {
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched onboarding for newly enabled repo");
        }

        // Trigger per-repo infrastructure analysis
        let per_repo_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: Some(repo.clone()),
        });
        if let Ok(body) = serde_json::to_string(&per_repo_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let sk = format!("INFRA#REPO#{repo}");
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s(&sk))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched per-repo infra analysis");
        }

        // Also trigger team-wide infrastructure analysis
        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched team-wide infra analysis");
        }
    } else {
        // When disabling a repo, remove its per-repo infra and refresh team-wide
        let sk = format!("INFRA#REPO#{repo}");
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.infra_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .send()
            .await;

        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched infra refresh after repo disable");
        }
    }

    Ok(StatusCode::OK)
}

/// DELETE /api/repos/:owner/:name — remove a repo from this team.
pub async fn delete_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.repos_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("REPO#{repo}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete repo: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// A whole-number counter on an analytics item (0 when absent).
fn n_attr(
    item: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    key: &str,
) -> u64 {
    item.and_then(|i| i.get(key))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0)
}

/// GET /api/stats — pre-computed analytics (O(1) read from analytics table).
pub async fn get_stats(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Read current month + all-time aggregates in parallel
    let now = chrono::Utc::now();
    let month_period = now.format("%Y-%m").to_string();

    let (month_result, alltime_result) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s(&month_period))
            .send(),
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s("ALL_TIME"))
            .send()
    );

    let month = month_result.map_err(|e| {
        error!("Failed to read monthly stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let alltime = alltime_result.map_err(|e| {
        error!("Failed to read all-time stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let extract = |item: Option<
        &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    >| {
        let total: u64 = item
            .and_then(|i| i.get("total_runs"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let completed: u64 = item
            .and_then(|i| i.get("completed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let failed: u64 = item
            .and_then(|i| i.get("failed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let cost: f64 = item
            .and_then(|i| i.get("total_cost_usd"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0.0);
        let tokens_in: u64 = item
            .and_then(|i| i.get("total_tokens_in"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let tokens_out: u64 = item
            .and_then(|i| i.get("total_tokens_out"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let t = common::usage::UsageTotals::from_item(item);
        json!({
            "total_runs": total,
            "completed": completed,
            "failed": failed,
            "in_progress": total.saturating_sub(completed + failed),
            "total_cost_usd": cost,
            "total_tokens_in": tokens_in,
            "total_tokens_out": tokens_out,
            "cache_read_tokens": t.cache_read,
            "cache_write_tokens": t.cache_write,
            "total_tokens": t.total(),
            "cache_hit_rate": t.cache_hit_rate(),
            // Calls whose reply broke off, their output estimated, and calls
            // added later from the fallback queue (common::usage).
            "estimated_calls": n_attr(item, common::usage::ESTIMATED_CALLS),
            "recovered_calls": n_attr(item, common::usage::RECOVERED_CALLS),
            "merge_rate": if total > 0 { completed as f64 / total as f64 } else { 0.0 },
        })
    };

    Ok(Json(json!({
        "month": extract(month.item()),
        "all_time": extract(alltime.item()),
    })))
}

/// GET /api/stats/history — last 6 months of analytics for charts.
pub async fn get_stats_history(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now();
    // Build period keys for the last 6 months
    let periods: Vec<String> = (0..6)
        .map(|i| {
            let d = now - chrono::Duration::days(i * 30);
            d.format("%Y-%m").to_string()
        })
        .collect();

    // Batch get all 6 months
    let mut months = Vec::new();
    for period in &periods {
        let result = state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s(period))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to read stats for {period}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let item = result.item();
        let get_n = |key: &str| -> u64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0)
        };
        let get_f = |key: &str| -> f64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0.0)
        };
        months.push(json!({
            "period": period,
            "total_runs": get_n("total_runs"),
            "completed": get_n("completed"),
            "failed": get_n("failed"),
            "total_cost_usd": get_f("total_cost_usd"),
            "total_tokens_in": get_n("total_tokens_in"),
            "total_tokens_out": get_n("total_tokens_out"),
            "cache_read_tokens": get_n("cache_read_tokens"),
            "cache_write_tokens": get_n("cache_write_tokens"),
            "total_tokens": common::usage::UsageTotals::from_item(item).total(),
        }));
    }

    // Reverse so oldest is first (for chart x-axis)
    months.reverse();

    Ok(Json(json!({ "months": months })))
}

// ─── Notification preferences ───────────────────────────────────────

/// GET /api/notifications — get notification preferences for calling user.
pub async fn get_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prefs = result
        .item()
        .map(NotificationPrefs::from_item)
        .unwrap_or_default();

    Ok(Json(json!(prefs)))
}

/// PUT /api/notifications — update notification preferences.
pub async fn update_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);

    let stored = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let prefs = stored
        .item()
        .map(NotificationPrefs::from_item)
        .unwrap_or_default()
        .merged_with(&body);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("email_run_complete", attr_bool(prefs.email_run_complete))
        .item("email_run_failed", attr_bool(prefs.email_run_failed))
        .item(
            "email_weekly_summary",
            attr_bool(prefs.email_weekly_summary),
        )
        .item("email_token_warning", attr_bool(prefs.email_token_warning))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Instructions ───────────────────────────────────────────────────

/// Look up the GitHub installation ID from the team's META record in the main table.
pub(crate) async fn get_team_installation_id(
    state: &AppState,
    team_id: &str,
) -> Result<u64, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .projection_expression("github_install_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch team META for installation_id: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    result
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&id| id > 0)
        .ok_or_else(|| {
            warn!(team_id, "No github_install_id found for team");
            StatusCode::BAD_REQUEST
        })
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

/// Force-delete a ticket lock (dashboard retry, start over and close).
pub(crate) async fn delete_ticket_lock(state: &AppState, team_id: &str, ticket_id: &str) {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(sk))
        .send()
        .await;
}

fn attr_bool(val: bool) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::Bool(val)
}

/// Teams keep their working playbooks here (testing, logging, PR and commit
/// conventions), which run past 10 KB; every agent in a run reads them.
const MAX_INSTRUCTIONS_BYTES: usize = 32_768; // 32 KB limit

/// GET /api/instructions/global — get global custom instructions.
pub async fn get_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "INSTRUCTIONS#GLOBAL").await
}

/// PUT /api/instructions/global — update global custom instructions.
pub async fn update_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "INSTRUCTIONS#GLOBAL", content).await
}

/// GET /api/instructions/repo/:owner/:name — get per-repo custom instructions.
pub async fn get_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/instructions/repo/:owner/:name — update per-repo custom instructions.
pub async fn update_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

/// Validate repo path parameter to prevent DynamoDB key injection.
/// GET /api/repos/:owner/:name/codegen-env — the repository's public
/// codegen environment (admins). Its values are public by rule, so they are
/// shown as kept.
pub async fn get_codegen_env(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&common::codegen_env::sk(&repo)))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to read the codegen environment for {repo}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item;
    let mut vars: Vec<(String, String)> = item
        .and_then(|i| i.get("env").and_then(|v| v.as_m().ok()).cloned())
        .map(|m| {
            m.into_iter()
                .filter_map(|(k, v)| v.as_s().ok().map(|s| (k, s.clone())))
                .collect()
        })
        .unwrap_or_default();
    vars.sort();
    Ok(Json(json!({
        "repo": repo,
        "vars": vars.iter().map(|(n, v)| json!({"name": n, "value": v})).collect::<Vec<_>>(),
    })))
}

/// The variables a codegen environment write carries, checked
/// ([`common::codegen_env::validate`]): public values only.
fn codegen_vars(body: &Value) -> Result<Vec<(String, String)>, String> {
    let vars: Vec<(String, String)> = body["vars"]
        .as_array()
        .ok_or("Send `vars`: a list of {name, value}.")?
        .iter()
        .map(|v| {
            (
                v["name"].as_str().unwrap_or("").trim().to_string(),
                v["value"].as_str().unwrap_or("").trim().to_string(),
            )
        })
        .filter(|(n, _)| !n.is_empty())
        .collect();
    common::codegen_env::validate(&vars)?;
    Ok(vars)
}

/// PUT /api/repos/:owner/:name/codegen-env — replace the repository's public
/// codegen environment (admins). Names or values that look like credentials
/// are refused with the reason; an empty list removes it.
pub async fn put_codegen_env(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let fail = |code: StatusCode, msg: &str| (code, Json(json!({ "error": msg })));
    // admin+
    let allowed = claims.require_role(3);
    allowed.map_err(|c| fail(c, "Only admins can change the codegen environment."))?;
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo).map_err(|c| fail(c, "Not a repository name."))?;
    let vars = codegen_vars(&body).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let key = common::codegen_env::sk(&repo);
    let res = if vars.is_empty() {
        state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&key))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    } else {
        state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s(&key))
            .item(
                "env",
                AttributeValue::M(vars.iter().map(|(n, v)| (n.clone(), attr_s(v))).collect()),
            )
            .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
            .item("updated_by", attr_s(&claims.email))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    res.map_err(|e| {
        error!("Failed to write the codegen environment for {repo}: {e}");
        fail(StatusCode::INTERNAL_SERVER_ERROR, "Could not save.")
    })?;
    info!(repo, count = vars.len(), "Codegen environment saved");
    Ok(Json(json!({ "repo": repo, "count": vars.len() })))
}

fn validate_repo_name(repo: &str) -> Result<(), StatusCode> {
    if repo.is_empty()
        || repo.len() > 200
        || repo.contains("..")
        || repo.contains('\0')
        || !repo
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

async fn get_instructions_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let content = result
        .item()
        .and_then(|item| item.get("content"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();

    Ok(Json(json!({ "content": content })))
}

async fn update_instructions_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
    content: &str,
) -> Result<StatusCode, StatusCode> {
    if content.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item("content", attr_s(content))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Must-rules ─────────────────────────────────────────────────────

const MAX_RULES_BYTES: usize = 10_240; // 10KB limit

/// GET /api/rules/global — get global must-rules.
pub async fn get_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_rules_inner(&state, &claims.team_id, "RULES#GLOBAL").await
}

/// PUT /api/rules/global — update global must-rules.
pub async fn update_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    update_rules_inner(&state, &claims.team_id, "RULES#GLOBAL", rules).await
}

/// GET /api/rules/repo/:owner/:name — get per-repo must-rules.
pub async fn get_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("RULES#REPO#{repo}");
    get_rules_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/rules/repo/:owner/:name — update per-repo must-rules.
pub async fn update_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    let sk = format!("RULES#REPO#{repo}");
    update_rules_inner(&state, &claims.team_id, &sk, rules).await
}

async fn get_rules_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rules: Vec<String> = result
        .item()
        .and_then(|item| item.get("rules"))
        .and_then(|v| v.as_l().ok())
        .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();

    Ok(Json(json!({ "rules": rules })))
}

async fn update_rules_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
    rules: &[Value],
) -> Result<StatusCode, StatusCode> {
    let rule_strings: Vec<String> = rules
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let serialized = serde_json::to_string(&rule_strings).unwrap_or_default();
    if serialized.len() > MAX_RULES_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let rule_attrs: Vec<aws_sdk_dynamodb::types::AttributeValue> = rule_strings
        .iter()
        .map(|s| aws_sdk_dynamodb::types::AttributeValue::S(s.clone()))
        .collect();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item(
            "rules",
            aws_sdk_dynamodb::types::AttributeValue::L(rule_attrs),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Voice settings ─────────────────────────────────────────────────

/// GET /api/voice/global — get global voice settings.
pub async fn get_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "VOICE#GLOBAL").await
}

/// PUT /api/voice/global — update global voice settings.
pub async fn update_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "VOICE#GLOBAL", content).await
}

/// GET /api/voice/repo/:owner/:name — get voice/tone settings for a repo.
pub async fn get_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("VOICE#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/voice/repo/:owner/:name — update voice/tone settings for a repo.
pub async fn update_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("VOICE#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

// ─── Agents context ─────────────────────────────────────────────────

/// GET /api/agents/global — get global agents context.
pub async fn get_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "AGENTS#GLOBAL").await
}

/// PUT /api/agents/global — update global agents context.
pub async fn update_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "AGENTS#GLOBAL", content).await
}

/// GET /api/agents/repo/:owner/:name — get agents context for a repo.
pub async fn get_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("AGENTS#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/agents/repo/:owner/:name — update agents context for a repo.
pub async fn update_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("AGENTS#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

/// POST /api/repos/:owner/:name/regenerate — re-run onboard (voice + agents) for a repo.
pub async fn regenerate_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let installation_id = get_team_installation_id(&state, &claims.team_id).await?;

    let message = WorkerMessage::Onboard(OnboardMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        repos: vec![OnboardRepo {
            owner: parts[0].to_string(),
            name: parts[1].to_string(),
            default_branch: "main".to_string(),
        }],
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize onboard message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send regenerate message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(repo = %repo, "Dispatched regeneration to SQS");
    Ok(Json(json!({ "status": "regenerating" })))
}

/// GET /api/settings/budget — the team's monthly token limits.
pub async fn get_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let limits = common::token_limits::load(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
    )
    .await
    .map_err(|e| {
        error!("Failed to fetch token limit: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(budget_json(&limits)))
}

/// The limits body: each of the three limits (0 = no limit). A team still on
/// the old single limit has `legacy: true` and its `max_tokens`; when that
/// limit counts new input + output only it is also shown as `max_new_tokens`,
/// which counts the same tokens.
pub fn budget_json(limits: &common::token_limits::TokenLimits) -> Value {
    use common::token_limits::LimitKind;
    let mut body = json!({
        "legacy": limits.legacy,
        "max_tokens": 0,
        "limit_counts_cache": false,
    });
    for kind in LimitKind::ALL {
        body[kind.field()] = json!(limits.get(kind).unwrap_or(0));
    }
    for l in &limits.limits {
        if let LimitKind::Legacy { counts_cache } = l.kind {
            body["max_tokens"] = json!(l.max);
            body["limit_counts_cache"] = json!(counts_cache);
            if !counts_cache {
                body[LimitKind::NewTokens.field()] = json!(l.max);
            }
        }
    }
    body
}

/// PUT /api/settings/budget — set the monthly token limits.
///
/// Body: `max_new_tokens`, `max_cache_read_tokens`, `max_cache_write_tokens`
/// (whole numbers; null or 0 = no limit). Saving them replaces the old single
/// limit. A body with only `max_tokens` (an older client) still sets the old
/// limit.
pub async fn update_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin or owner
    let parsed = common::token_limits::parse_new_limits(&body).map_err(|bad| {
        warn!(field = bad.0, "Rejected token limit: not a whole number");
        StatusCode::BAD_REQUEST
    })?;
    if let Some(limits) = parsed {
        common::token_limits::write_new_limits(
            &state.dynamo,
            &state.config.settings_table_name,
            &claims.team_id,
            &limits,
        )
        .await
        .map_err(|e| {
            error!("Failed to update token limits: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        return Ok(StatusCode::OK);
    }

    let max_tokens = body["max_tokens"].as_u64().unwrap_or(0);
    // An update, not a put: keeps the item's other settings (counts_cache).
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(common::token_limits::SETTINGS_SK))
        .update_expression("SET max_tokens = :m")
        .expression_attribute_values(
            ":m",
            aws_sdk_dynamodb::types::AttributeValue::N(max_tokens.to_string()),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update token limit: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/usage — token usage for the current month + configured limits.
#[utoipa::path(
    get,
    path = "/api/usage",
    tag = "Usage",
    operation_id = "getUsage",
    summary = "Get this month's usage",
    description = "Returns the tokens the team used this calendar month (UTC), split into new \
        input, output, cache reads and cache writes, per feature, and against each limit the team \
        set. When a limit is reached, endpoints that start work answer `429` with \
        `token_limit_reached` until the month turns or an admin raises it.",
    responses(
        (status = 200, description = "This month's usage.", body = Usage),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn get_usage(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Usage>, StatusCode> {
    let month = chrono::Utc::now().format("%Y-%m").to_string();

    let (analytics_result, limit_result) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s(&month))
            .send(),
        common::token_limits::load(
            &state.dynamo,
            &state.config.settings_table_name,
            &claims.team_id,
        )
    );

    let analytics = analytics_result.map_err(|e| {
        error!("Failed to read analytics: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let limits = limit_result.map_err(|e| {
        error!("Failed to read token limit: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(usage_body(&month, analytics.item(), &limits)))
}

/// The usage body: tokens only (no money). `total_tokens` is all four parts.
/// `limits` lists each set limit with what it counts this month (`kind`:
/// `new_tokens` = new input + output, `cache_read`, `cache_write`, or
/// `all_tokens` for an old single limit that counts cache too).
/// `max_tokens` / `counted_tokens` / `limit_counts_cache` describe the old
/// single limit for older clients (0 / new input + output once the new limits
/// are saved).
#[cfg(test)]
pub fn usage_json(
    month: &str,
    analytics: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    limits: &common::token_limits::TokenLimits,
) -> Value {
    serde_json::to_value(usage_body(month, analytics, limits)).unwrap_or_default()
}

/// One feature's share of the month's tokens.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct UsageFeature {
    /// The feature, such as `coder`, `reviewer`, `plan_chat` or `log_analyzer`.
    #[schema(example = "reviewer")]
    pub feature: String,
    /// All four parts together.
    #[schema(example = 2410022)]
    pub total_tokens: u64,
    /// New input tokens (not from the cache).
    #[schema(example = 301200)]
    pub tokens_in: u64,
    /// Tokens read from the prompt cache.
    #[schema(example = 1900410)]
    pub cache_read_tokens: u64,
    /// Tokens written to the prompt cache.
    #[schema(example = 151002)]
    pub cache_write_tokens: u64,
    /// Output tokens.
    #[schema(example = 57410)]
    pub tokens_out: u64,
}

/// One limit the team set, and what it counts this month.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct UsageLimit {
    /// What it counts: `new_tokens` (new input and output), `cache_read`,
    /// `cache_write`, or `all_tokens` (an older single limit that counts cache too).
    #[schema(example = "new_tokens")]
    pub kind: String,
    /// The limit.
    #[schema(example = 20000000)]
    pub max: u64,
    /// What it counts so far this month.
    #[schema(example = 4310227)]
    pub used: u64,
}

/// The team's token usage this month.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Usage {
    /// The month, `YYYY-MM` (UTC).
    #[schema(example = "2026-10")]
    pub month: String,
    /// All tokens: new input, cache reads, cache writes and output.
    #[schema(example = 18233019)]
    pub total_tokens: u64,
    /// New input tokens (not from the cache).
    #[schema(example = 3011022)]
    pub tokens_in: u64,
    /// Tokens read from the prompt cache.
    #[schema(example = 13100400)]
    pub cache_read_tokens: u64,
    /// Tokens written to the prompt cache.
    #[schema(example = 822392)]
    pub cache_write_tokens: u64,
    /// Output tokens.
    #[schema(example = 1299205)]
    pub tokens_out: u64,
    /// New input plus output: what the `new_tokens` limit counts.
    #[schema(example = 4310227)]
    pub new_tokens: u64,
    /// Cache reads divided by all input, from 0 to 1.
    #[schema(example = 0.77)]
    pub cache_hit_rate: f64,
    /// Each limit that is set, in display order. Empty when none is.
    pub limits: Vec<UsageLimit>,
    /// True when the limits were read from the older single-limit setting.
    #[schema(example = false)]
    pub limits_legacy: bool,
    /// What the older single limit counts (kept for older clients).
    #[schema(example = 4310227)]
    pub counted_tokens: u64,
    /// Whether the older single limit counts cache tokens (kept for older clients).
    #[schema(example = false)]
    pub limit_counts_cache: bool,
    /// Usage per feature.
    pub features: Vec<UsageFeature>,
    /// Runs started this month.
    #[schema(example = 37)]
    pub total_runs: u64,
    /// The older single limit, 0 when it isn't used (kept for older clients).
    #[schema(example = 0)]
    pub max_tokens: u64,
}

/// The usage body as a type (see [`usage_json`]).
pub fn usage_body(
    month: &str,
    analytics: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    limits: &common::token_limits::TokenLimits,
) -> Usage {
    use common::token_limits::LimitKind;
    let t = common::usage::UsageTotals::from_item(analytics);
    let features: Vec<UsageFeature> = common::usage::feature_totals(analytics)
        .into_iter()
        .map(|(f, ft)| UsageFeature {
            feature: f,
            total_tokens: ft.total(),
            tokens_in: ft.tokens_in_uncached,
            cache_read_tokens: ft.cache_read,
            cache_write_tokens: ft.cache_write,
            tokens_out: ft.tokens_out,
        })
        .collect();
    let uses = limits.uses(&t);
    let limit_rows: Vec<UsageLimit> = uses
        .iter()
        .map(|u| UsageLimit {
            kind: u.kind.key().to_string(),
            max: u.max,
            used: u.used,
        })
        .collect();
    let (max_tokens, counted, counts_cache) = uses
        .iter()
        .find_map(|u| match u.kind {
            LimitKind::Legacy { counts_cache } => Some((u.max, u.used, counts_cache)),
            _ => None,
        })
        .unwrap_or((0, t.uncached_total(), false));
    Usage {
        month: month.to_string(),
        total_tokens: t.total(),
        tokens_in: t.tokens_in_uncached,
        cache_read_tokens: t.cache_read,
        cache_write_tokens: t.cache_write,
        tokens_out: t.tokens_out,
        new_tokens: t.uncached_total(),
        cache_hit_rate: t.cache_hit_rate(),
        limits: limit_rows,
        limits_legacy: limits.legacy,
        counted_tokens: counted,
        limit_counts_cache: counts_cache,
        features,
        total_runs: analytics
            .and_then(|i| i.get("total_runs"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0),
        max_tokens,
    }
}

/// GET /api/settings/workflow — get workflow preferences.
pub async fn get_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#WORKFLOW"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let commit_openspec = result
        .item()
        .and_then(|i| i.get("commit_openspec"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false); // default: off

    let default_destination = result
        .item()
        .and_then(|i| i.get("default_destination"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "github".to_string());

    let allow_plan_log_analyzer = result
        .item()
        .and_then(|i| i.get("allow_plan_log_analyzer"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false); // default: off

    Ok(Json(json!({
        "commit_openspec": commit_openspec,
        "default_destination": default_destination,
        "allow_plan_log_analyzer": allow_plan_log_analyzer,
    })))
}

/// PUT /api/settings/workflow — update workflow preferences.
pub async fn update_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let commit_openspec = body["commit_openspec"].as_bool().unwrap_or(false);
    let default_destination = body["default_destination"]
        .as_str()
        .filter(|d| *d == "github" || *d == "jira")
        .unwrap_or("github");
    let allow_plan_log_analyzer = body["allow_plan_log_analyzer"].as_bool().unwrap_or(false);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("SETTINGS#WORKFLOW"))
        .item(
            "commit_openspec",
            aws_sdk_dynamodb::types::AttributeValue::Bool(commit_openspec),
        )
        .item("default_destination", attr_s(default_destination))
        .item(
            "allow_plan_log_analyzer",
            aws_sdk_dynamodb::types::AttributeValue::Bool(allow_plan_log_analyzer),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// The git conventions as the dashboard shows them: the team's templates
/// (empty = default), the defaults and the tokens a template may use.
fn git_conventions_json(c: &common::git_conventions::GitConventions) -> Value {
    use common::git_conventions as gc;
    json!({
        "branch": c.branch.clone().unwrap_or_default(),
        "commit": c.commit.clone().unwrap_or_default(),
        "pr_title": c.pr_title.clone().unwrap_or_default(),
        "defaults": {
            "branch": gc::DEFAULT_BRANCH,
            "commit": gc::DEFAULT_COMMIT,
            "pr_title": gc::DEFAULT_PR_TITLE,
        },
        "tokens": gc::TOKENS,
    })
}

/// GET /api/settings/git-conventions — the team's branch, commit and PR
/// title templates.
pub async fn get_git_conventions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(common::git_conventions::SETTINGS_SK))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch git conventions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let c = result
        .item()
        .map(common::git_conventions::GitConventions::from_item)
        .unwrap_or_default();
    Ok(Json(git_conventions_json(&c)))
}

/// The templates from a PUT body, each validated. An error names the field
/// and what to change.
fn parse_git_conventions(
    body: &Value,
) -> Result<common::git_conventions::GitConventions, (&'static str, String)> {
    use common::git_conventions::{validate, GitConventions, Kind};
    let field =
        |name: &'static str, kind: Kind| -> Result<Option<String>, (&'static str, String)> {
            let v = body[name].as_str().unwrap_or("").trim();
            validate(kind, v).map_err(|e| (name, e))?;
            Ok((!v.is_empty()).then(|| v.to_string()))
        };
    Ok(GitConventions {
        branch: field("branch", Kind::Branch)?,
        commit: field("commit", Kind::Commit)?,
        pr_title: field("pr_title", Kind::PrTitle)?,
    })
}

/// PUT /api/settings/git-conventions — save the templates. An empty field
/// keeps CoderHelm's default; an invalid one is refused with the reason.
pub async fn update_git_conventions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // admin+
    if let Err(s) = claims.require_role(3) {
        return Err((s, Json(json!({ "error": "Only admins can change this." }))));
    }
    let c = parse_git_conventions(&body).map_err(|(field, error)| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error, "field": field })),
        )
    })?;
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(common::git_conventions::SETTINGS_SK))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()));
    for (k, v) in [
        ("branch", &c.branch),
        ("commit", &c.commit),
        ("pr_title", &c.pr_title),
    ] {
        if let Some(v) = v {
            put = put.item(k, attr_s(v));
        }
    }
    put.send().await.map_err(|e| {
        error!("Failed to save git conventions: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not save. Try again." })),
        )
    })?;
    Ok(Json(git_conventions_json(&c)))
}

/// GET /api/settings/model-provider — get model provider configuration.
pub async fn get_model_provider(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch model provider settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let Some(item) = result.item() else {
        return Ok(Json(json!({
            "provider": "anthropic",
            "api_key_masked": null,
            "primary_model": null,
            "heavy_model": null,
            "deployment_provider": deployment_provider(),
        })));
    };

    let provider = item
        .get("provider")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "anthropic".to_string());

    let api_key_masked = item.get("api_key").and_then(|v| v.as_s().ok()).map(|key| {
        if key.len() > 4 {
            format!("sk-ant-...{}", &key[key.len() - 4..])
        } else {
            "****".to_string()
        }
    });

    // A retired saved id is shown as the model that now runs in its place,
    // with the old id, so the page can say it changed.
    let saved = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let (primary_saved, heavy_saved) = (saved("primary_model"), saved("heavy_model"));
    let current = |m: &Option<String>| -> Option<String> {
        m.as_deref()
            .map(|id| common::model_ids::current(id).to_string())
    };
    let retired: Vec<&String> = [&primary_saved, &heavy_saved]
        .into_iter()
        .flatten()
        .filter(|m| common::model_ids::is_retired(m))
        .collect();

    Ok(Json(json!({
        "provider": provider,
        "api_key_masked": api_key_masked,
        "primary_model": current(&primary_saved),
        "heavy_model": current(&heavy_saved),
        "retired_models": retired,
        "deployment_provider": deployment_provider(),
    })))
}

/// How this deployment calls Claude: "bedrock" (its own AWS account, no team
/// key needed) or "anthropic" (each team's key).
fn deployment_provider() -> &'static str {
    if common::model_provider::is_bedrock() {
        "bedrock"
    } else {
        "anthropic"
    }
}

/// PUT /api/settings/model-provider — configure Anthropic API key and models.
pub async fn update_model_provider(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let provider = "anthropic";

    let api_key_from_body = body["api_key"].as_str().map(|s| s.to_string());
    let primary_model = body["primary_model"]
        .as_str()
        .ok_or(StatusCode::BAD_REQUEST)?;
    let heavy_model = body["heavy_model"]
        .as_str()
        .ok_or(StatusCode::BAD_REQUEST)?;

    // What a team may pick is the release's manifest (common::model_ids).
    if !common::model_ids::is_selectable(primary_model)
        || !common::model_ids::is_selectable(heavy_model)
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    // On Bedrock the install's own role calls the model: only the model
    // choices are saved, with no key to check.
    if common::model_provider::is_bedrock() {
        let now = chrono::Utc::now().to_rfc3339();
        state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
            .item("provider", attr_s(provider))
            .item("primary_model", attr_s(primary_model))
            .item("heavy_model", attr_s(heavy_model))
            .item("updated_at", attr_s(&now))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to save model provider settings: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        return Ok(Json(json!({ "status": "ok", "provider": "bedrock" })));
    }

    // If no new key provided, try to reuse existing one
    let api_key = if let Some(key) = api_key_from_body {
        key
    } else {
        let existing = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to fetch existing model provider: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        existing
            .item()
            .and_then(|item| item.get("api_key"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .ok_or(StatusCode::BAD_REQUEST)?
    };

    // Validate the key (and its access to the chosen model) via the Models API
    // — no generation, so it works the same for thinking and non-thinking models.
    let client = reqwest::Client::new();
    // A transient failure (5xx, 408, 429, 529) is retried and, if it lasts,
    // reported as the service being unavailable, not as a bad key.
    let models_url = format!("https://api.anthropic.com/v1/models/{primary_model}");
    let test_resp = common::model_retry::send(
        || {
            client
                .get(&models_url)
                .header("x-api-key", &api_key)
                .header("anthropic-version", "2023-06-01")
        },
        &common::model_retry::INTERACTIVE_BACKOFF_SECS,
        "validate_key",
    )
    .await
    .map_err(|e| {
        error!("Anthropic validation request failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if common::model_retry::is_transient_status(test_resp.status) {
        error!(
            status = test_resp.status,
            "Anthropic unavailable while validating a key"
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if !test_resp.is_success() {
        return Ok(Json(json!({ "error": "Invalid API key" })));
    }

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .item("provider", attr_s(provider))
        .item("api_key", attr_s(&api_key))
        .item("primary_model", attr_s(primary_model))
        .item("heavy_model", attr_s(heavy_model))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to save model provider settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "status": "ok", "provider": "anthropic" })))
}

/// DELETE /api/settings/model-provider — remove API key configuration.
pub async fn delete_model_provider(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(4)?; // owner only
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete model provider settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

type RunItem = std::collections::HashMap<String, AttributeValue>;

fn item_s<'a>(item: &'a RunItem, key: &str) -> Option<&'a str> {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
}

/// Runs whose status is `status` and that pass `keep`, newest first, at most
/// `max`. Reads the `status-index` GSI (`status_run_id` = `<status>#<run_id>`)
/// page by page. The health checks used to query the whole team partition
/// with `Limit` 20/50 before the status filter, so they only ever looked at
/// the oldest 20 or 50 runs and missed nearly every stuck or failed run.
async fn runs_with_status(
    state: &AppState,
    team_id: &str,
    status: &str,
    keep: impl Fn(&RunItem) -> bool,
    max: usize,
) -> Result<Vec<RunItem>, ()> {
    let mut out: Vec<RunItem> = Vec::new();
    let mut start: Option<RunItem> = None;
    // A hard stop, so one team's long history cannot make this page slow.
    for _ in 0..20 {
        let result = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .index_name("status-index")
            .key_condition_expression("team_id = :tid AND begins_with(status_run_id, :p)")
            .expression_attribute_values(":tid", attr_s(team_id))
            .expression_attribute_values(":p", attr_s(&format!("{status}#")))
            .scan_index_forward(false)
            .limit(100)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| error!("health: status-index query failed: {e}"))?;
        for item in result.items() {
            // The index can lag the table; trust the item's own status.
            if item_s(item, "status") == Some(status) && keep(item) {
                out.push(item.clone());
                if out.len() >= max {
                    return Ok(out);
                }
            }
        }
        match result.last_evaluated_key() {
            Some(key) => start = Some(key.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// GET /api/health — system health check (stale runs, the team's failed
/// messages, queue depths).
pub async fn health(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let now = chrono::Utc::now();
    let mut checks: Vec<Value> = Vec::new();
    let mut status = "healthy";

    // 1. Crashed runs — "running" with no update for over 20 minutes (worker likely died or timed out)
    let crash_cutoff = (now - chrono::Duration::minutes(20)).to_rfc3339();
    if let Ok(items) = runs_with_status(
        &state,
        &claims.team_id,
        "running",
        |item| item_s(item, "updated_at").is_some_and(|t| t < crash_cutoff.as_str()),
        20,
    )
    .await
    {
        let crashed: Vec<Value> = items
            .iter()
            .map(|item| {
                let run_id = item.get("run_id").and_then(|v| v.as_s().ok()).cloned().unwrap_or_default();
                // Auto-mark crashed runs as timed_out
                let dynamo = state.dynamo.clone();
                let table = state.config.runs_table_name.clone();
                let tid = claims.team_id.clone();
                let rid = run_id.clone();
                tokio::spawn(async move {
                    // Only while still running (it may have finished since
                    // the read); the pass is the one the record names.
                    let failure = common::run_fail::RunFailure {
                        reason: "Run timed out (worker process was terminated)",
                        only_from: Some("running"),
                        ..Default::default()
                    };
                    if let Err(e) =
                        common::run_fail::fail_run(&dynamo, &table, &tid, &rid, &failure).await
                    {
                        tracing::warn!(run_id = %rid, error = %e, "Could not mark a crashed run failed");
                    }
                });
                json!({
                    "run_id": &run_id,
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                    "auto_fixed": true,
                })
            })
            .collect();

        let count = crashed.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "crashed_runs",
            "status": if count == 0 { "ok" } else { "critical" },
            "count": count,
            "items": crashed,
        }));
    }

    // 2. Stale queued — sitting in queue for over 30 minutes
    let stale_cutoff = (now - chrono::Duration::minutes(30)).to_rfc3339();
    if let Ok(items) = runs_with_status(
        &state,
        &claims.team_id,
        "queued",
        |item| item_s(item, "created_at").is_some_and(|t| t < stale_cutoff.as_str()),
        20,
    )
    .await
    {
        let stale_runs: Vec<Value> = items
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = stale_runs.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "stale_queued",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": stale_runs,
        }));
    }

    // 3. Failed messages: this team's own, from the same team-filtered list
    // the dashboard shows (never the shared queue's depth).
    if let Some(check) = super::dlq::team_health_check(&state, &claims.team_id).await {
        if check["status"] != "ok" {
            status = "degraded";
        }
        checks.push(check);
    }

    // 4. Queue depths (tickets, ci-fix, feedback)
    for (name, url) in [
        ("tickets", &state.config.ticket_queue_url),
        ("ci_fix", &state.config.ci_fix_queue_url),
        ("feedback", &state.config.feedback_queue_url),
    ] {
        if let Ok(attrs) = state
            .sqs
            .get_queue_attributes()
            .queue_url(url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(
                aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .send()
            .await
        {
            let visible: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let in_flight: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(
                        &aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
                    )
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            checks.push(json!({
                "name": name,
                "status": "ok",
                "visible": visible,
                "in_flight": in_flight,
            }));
        }
    }

    // 5. Recent failed runs (last 24h)
    let error_cutoff = (now - chrono::Duration::hours(24)).to_rfc3339();
    if let Ok(items) = runs_with_status(
        &state,
        &claims.team_id,
        "failed",
        |item| item_s(item, "created_at").is_some_and(|t| t > error_cutoff.as_str()),
        50,
    )
    .await
    {
        let failed_runs: Vec<Value> = items
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "error": item_s(item, "error_message").or_else(|| item_s(item, "error")),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = failed_runs.len();
        checks.push(json!({
            "name": "failed_runs_24h",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": failed_runs,
        }));
    }

    Ok(Json(json!({
        "status": status,
        "checked_at": now.to_rfc3339(),
        "checks": checks,
    })))
}

/// POST /api/account/reset — wipe all team data across all tables.
/// Keeps the TEAM META record so the account identity remains.
pub async fn reset_account(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }
    info!(team_id = %claims.team_id, user = %claims.email, "Account reset requested");

    let tid = &claims.team_id;

    // Helper: delete all items with pk=team_id from a table
    async fn wipe_table(
        dynamo: &aws_sdk_dynamodb::Client,
        table: &str,
        tid: &str,
    ) -> Result<(), StatusCode> {
        let result = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(
                ":pk",
                aws_sdk_dynamodb::types::AttributeValue::S(tid.to_string()),
            )
            .projection_expression("pk, sk")
            .send()
            .await
            .map_err(|e| {
                error!("Failed to query {table} for reset: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        for item in result.items() {
            if let (Some(pk), Some(sk)) = (item.get("pk"), item.get("sk")) {
                let _ = dynamo
                    .delete_item()
                    .table_name(table)
                    .key("pk", pk.clone())
                    .key("sk", sk.clone())
                    .send()
                    .await;
            }
        }
        Ok(())
    }

    // Delete jira token from jira-tokens table (keyed differently)
    if let Some((old_token, _)) = load_jira_secret(&state, tid).await {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    // Wipe all domain tables (pk/sk keyed)
    let tables = [
        &state.config.plans_table_name,
        &state.config.jira_config_table_name,
        &state.config.repos_table_name,
        &state.config.settings_table_name,
        &state.config.infra_table_name,
    ];

    for table in &tables {
        wipe_table(&state.dynamo, table, tid).await?;
    }

    // Wipe jira-events table (keyed by team_id, event_id)
    let jira_events = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .projection_expression("team_id, event_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query jira-events for reset: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in jira_events.items() {
        if let (Some(pk), Some(sk)) = (item.get("team_id"), item.get("event_id")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_events_table_name)
                .key("team_id", pk.clone())
                .key("event_id", sk.clone())
                .send()
                .await;
        }
    }

    // Wipe runs table (keyed by team_id, run_id)
    let runs = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .projection_expression("team_id, run_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs for reset: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in runs.items() {
        if let (Some(pk), Some(sk)) = (item.get("team_id"), item.get("run_id")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", pk.clone())
                .key("run_id", sk.clone())
                .send()
                .await;
        }
    }

    // Delete WELCOME_SENT from main table (keep META, TEAM)
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tid))
        .key("sk", attr_s("WELCOME_SENT"))
        .send()
        .await;

    info!(team_id = %claims.team_id, "Account data reset complete");
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::{
        budget_json, platform_admin_listed, reset_allowed, run_of, usage_json, verified_identity,
        RunUsage,
    };
    use serde_json::json;

    /// The run detail page shows and copies `run_id`; `get_run` answers with
    /// it even when the row has nothing but a status.
    #[test]
    fn get_run_answers_with_the_run_id() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let item = std::collections::HashMap::from([
            (
                "run_id".to_string(),
                A::S("01JB8X6F4M2C9QZ3T7V5W1K0HA".into()),
            ),
            ("status".to_string(), A::S("completed".into())),
        ]);
        let usage = || RunUsage {
            tokens_in: 0,
            tokens_out: 0,
            cache_read: 0,
            cache_write: 0,
            cost_usd: 0.0,
        };
        let run = serde_json::to_value(run_of(
            &item,
            "01JB8X6F4M2C9QZ3T7V5W1K0HA",
            usage(),
            &Default::default(),
        ))
        .unwrap();
        assert_eq!(run["run_id"], "01JB8X6F4M2C9QZ3T7V5W1K0HA");
        assert_eq!(run["status"], "completed");
        // The id comes from the request, so a row without the attribute
        // still answers with it.
        let bare =
            std::collections::HashMap::from([("status".to_string(), A::S("running".into()))]);
        let run =
            serde_json::to_value(run_of(&bare, "01JB8Y", usage(), &Default::default())).unwrap();
        assert_eq!(run["run_id"], "01JB8Y");
        assert_eq!(run["questions"], json!([]));
        assert_eq!(run["answers"], json!([]));
    }

    /// What a run stored when it stopped to ask comes back on `get_run`:
    /// each question with its reason and candidate answers, the comment's
    /// address, the answers given and the run that continued.
    #[test]
    fn get_run_returns_the_questions_the_run_stored() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let asked = json!({
            "questions": [
                {"id": "q1", "question": "Which environments get the new limit?",
                 "why": "Each environment has its own value.",
                 "suggestions": ["Production only", "Every environment"]},
                {"id": "q2", "question": "What is the new limit?"}
            ],
            "asked_at": "2026-10-27T10:00:00Z",
            "comment_url": "https://example.atlassian.net/browse/ABC-1?focusedCommentId=10001"
        });
        let answers = json!([{
            "question_id": "q1", "question": "Which environments get the new limit?",
            "answer": "Production only", "author": "Pat Doe",
            "answered_at": "2026-10-27T10:05:00Z"
        }]);
        let item = std::collections::HashMap::from([
            ("status".to_string(), A::S("retried".into())),
            ("questions".to_string(), A::S(asked.to_string())),
            ("answers".to_string(), A::S(answers.to_string())),
            ("continued_run_id".to_string(), A::S("01JB8Z".into())),
        ]);
        let run = serde_json::to_value(run_of(
            &item,
            "01JB8Y",
            RunUsage {
                tokens_in: 0,
                tokens_out: 0,
                cache_read: 0,
                cache_write: 0,
                cost_usd: 0.0,
            },
            &Default::default(),
        ))
        .unwrap();
        assert_eq!(
            run["questions"],
            json!([
                {"id": "q1", "question": "Which environments get the new limit?",
                 "why": "Each environment has its own value.",
                 "suggestions": ["Production only", "Every environment"], "decision": null},
                {"id": "q2", "question": "What is the new limit?", "why": null, "suggestions": [],
                 "decision": null}
            ])
        );
        assert_eq!(
            run["questions_url"],
            "https://example.atlassian.net/browse/ABC-1?focusedCommentId=10001"
        );
        assert_eq!(run["answers"][0]["answer"], "Production only");
        assert_eq!(run["answers"][0]["author"], "Pat Doe");
        assert_eq!(run["continued_run_id"], "01JB8Z");
    }

    /// A run whose PR is open and whose last fix round didn't finish reads
    /// as such, never as failed, and offers Resume.
    #[test]
    fn an_unfinished_round_shows_on_the_run_with_resume() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        use common::run_round::{Incomplete, Kind, Pushed, Round};
        let round = Round::start(
            Kind::ReviewFix,
            3,
            "abc",
            "Add the tabs",
            true,
            chrono::Utc::now(),
        );
        let mut inc = Incomplete::of(
            &round,
            "the review fix round ran out of its time budget",
            "2026-10-22T15:40:02Z",
        );
        inc.pushed = vec![Pushed {
            sha: "4f2a9c1".into(),
            message: "Add the tabs".into(),
        }];
        let item = std::collections::HashMap::from([
            ("status".to_string(), A::S("completed".into())),
            ("pr_number".to_string(), A::N("41".into())),
            (
                common::run_round::INCOMPLETE_FIELD.to_string(),
                inc.to_attr(),
            ),
        ]);
        let usage = RunUsage {
            tokens_in: 0,
            tokens_out: 0,
            cache_read: 0,
            cache_write: 0,
            cost_usd: 0.0,
        };
        let run = serde_json::to_value(run_of(&item, "r1", usage)).unwrap();
        let r = &run["round_incomplete"];
        assert_eq!(run["status"], "completed");
        assert_eq!(r["kind"], "review_fix");
        assert_eq!(r["number"], 3);
        assert_eq!(r["can_resume"], true);
        assert_eq!(r["pushed"][0]["sha"], "4f2a9c1");
        assert!(r["summary"]
            .as_str()
            .unwrap()
            .starts_with("Pull request open; the last fix round didn't finish"));
        // Without one, the field is null.
        let plain =
            std::collections::HashMap::from([("status".to_string(), A::S("completed".into()))]);
        let usage = RunUsage {
            tokens_in: 0,
            tokens_out: 0,
            cache_read: 0,
            cache_write: 0,
            cost_usd: 0.0,
        };
        assert!(
            serde_json::to_value(run_of(&plain, "r2", usage)).unwrap()["round_incomplete"]
                .is_null()
        );
    }

    #[test]
    fn resume_parks_a_round_it_can_run_and_rechecks_one_it_cannot() {
        use common::run_round::{Incomplete, Kind, Round, State};
        let now = chrono::Utc::now();
        let round = Round::start(Kind::ReviewFix, 2, "abc", "Add the tabs", true, now);
        let inc = Incomplete::of(&round, "out of time", "t");
        match super::resume_route(&inc, now) {
            super::ResumeRoute::Parked(r) => {
                assert_eq!(r.state, State::Pending);
                assert_eq!(r.attempt, 1);
                assert_eq!(r.feedback, "Add the tabs");
                assert_eq!(r.kind, Kind::ReviewFix);
            }
            other => panic!("{other:?}"),
        }
        // The sweep keeps no instructions: the resume reads CI and review again.
        let swept = Incomplete {
            kind: Some(Kind::ReviewFix),
            ..Default::default()
        };
        assert_eq!(
            super::resume_route(&swept, now),
            super::ResumeRoute::Recheck
        );
        let fb = Incomplete {
            kind: Some(Kind::Feedback),
            feedback: "x".into(),
            ..Default::default()
        };
        assert_eq!(super::resume_route(&fb, now), super::ResumeRoute::Feedback);
    }

    #[test]
    fn codegen_env_writes_take_public_values_only() {
        let ok = super::codegen_vars(&json!({"vars": [
            {"name": "SANITY_STUDIO_PROJECT_ID", "value": "abc123"},
            {"name": "  ", "value": "ignored"}
        ]}))
        .unwrap();
        assert_eq!(
            ok,
            vec![("SANITY_STUDIO_PROJECT_ID".to_string(), "abc123".to_string())]
        );
        assert!(super::codegen_vars(
            &json!({"vars": [{"name": "SANITY_AUTH_TOKEN", "value": "x"}]})
        )
        .is_err());
        assert!(super::codegen_vars(&json!({})).is_err());
        assert!(super::codegen_vars(&json!({"vars": []}))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_run_stopped_for_missing_codegen_values_names_them() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let item = std::collections::HashMap::from([
            ("status".to_string(), A::S("failed".into())),
            (
                "codegen_env_missing".to_string(),
                A::M(std::collections::HashMap::from([
                    ("repo".to_string(), A::S("acme/web".into())),
                    ("command".to_string(), A::S("pnpm run typegen".into())),
                    (
                        "vars".to_string(),
                        A::L(vec![A::S("SANITY_STUDIO_PROJECT_ID".into())]),
                    ),
                ])),
            ),
        ]);
        let usage = RunUsage {
            tokens_in: 0,
            tokens_out: 0,
            cache_read: 0,
            cache_write: 0,
            cost_usd: 0.0,
        };
        let run = serde_json::to_value(run_of(&item, "r", usage)).unwrap();
        assert_eq!(
            run["codegen_env_missing"]["vars"][0],
            "SANITY_STUDIO_PROJECT_ID"
        );
        assert_eq!(run["codegen_env_missing"]["repo"], "acme/web");
    }

    #[test]
    fn github_only_identities_are_never_platform_admins() {
        assert!(verified_identity("USER#Google_1234567890"));
        assert!(verified_identity(
            "USER#2f1c9a4e-1b2c-4d5e-8f90-123456789abc"
        ));
        assert!(!verified_identity("USER#123456"));
        assert!(!verified_identity("USER#"));
        assert!(!verified_identity(""));
    }

    #[test]
    fn usage_shows_all_four_parts_tokens_only() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let call = common::usage::CallUsage {
            uncached_input: 120_000,
            cache_read: 61_400_000,
            cache_write_5m: 8_800_000,
            cache_write_1h: 0,
            output: 1_000_000,
        };
        let (_, names, values) = common::usage::add_expression("run", "claude-opus-5-5", &call);
        let mut analytics: std::collections::HashMap<String, A> = names
            .iter()
            .map(|(ph, a)| (a.clone(), values[&ph.replace("#a", ":v")].clone()))
            .collect();
        analytics.insert("total_runs".into(), A::N("3".into()));
        let mut limit = std::collections::HashMap::new();
        limit.insert("max_tokens".to_string(), A::N("200000000".into()));
        let legacy =
            |cc: Option<&str>| common::token_limits::TokenLimits::from_item_with(Some(&limit), cc);

        let off = usage_json("2026-10", Some(&analytics), &legacy(None));
        let parts = [
            "tokens_in",
            "cache_read_tokens",
            "cache_write_tokens",
            "tokens_out",
        ];
        let sum: u64 = parts.iter().map(|k| off[k].as_u64().unwrap()).sum();
        assert_eq!(off["total_tokens"].as_u64().unwrap(), sum);
        assert_eq!(sum, 71_320_000);
        assert_eq!(
            off["counted_tokens"], 1_120_000,
            "old rule while the flag is off"
        );
        assert_eq!(off["limit_counts_cache"], false);
        assert_eq!(off["max_tokens"], 200_000_000u64);
        assert_eq!(off["limits_legacy"], true);
        assert_eq!(
            off["limits"],
            json!([{ "kind": "new_tokens", "max": 200_000_000u64, "used": 1_120_000u64 }])
        );
        assert_eq!(off["features"][0]["feature"], "run");
        assert_eq!(off["features"][0]["total_tokens"], 71_320_000u64);
        let rate = off["cache_hit_rate"].as_f64().unwrap();
        assert!((rate - 61_400_000.0 / 70_320_000.0).abs() < 1e-9);

        let on = usage_json("2026-10", Some(&analytics), &legacy(Some("true")));
        assert_eq!(on["counted_tokens"], on["total_tokens"]);
        assert_eq!(on["limits"][0]["kind"], "all_tokens");
        assert_eq!(on["limits"][0]["used"], 71_320_000u64);
        // No money on the usage view.
        let text = on.to_string();
        assert!(!text.contains("cost") && !text.contains("usd"), "{text}");

        // The three new limits: only the set ones are listed, each with its
        // own count.
        let mut new = std::collections::HashMap::new();
        new.insert("max_new_tokens".to_string(), A::N("5000000".into()));
        new.insert("max_cache_read_tokens".to_string(), A::N("0".into()));
        new.insert("max_cache_write_tokens".to_string(), A::N("9000000".into()));
        new.insert("max_tokens".to_string(), A::N("1".into()));
        let split = common::token_limits::TokenLimits::from_item_with(Some(&new), None);
        let body = usage_json("2026-10", Some(&analytics), &split);
        assert_eq!(body["limits_legacy"], false);
        assert_eq!(body["max_tokens"], 0, "old limit not read once split");
        assert_eq!(
            body["limits"],
            json!([
                { "kind": "new_tokens", "max": 5_000_000u64, "used": 1_120_000u64 },
                { "kind": "cache_write", "max": 9_000_000u64, "used": 8_800_000u64 },
            ])
        );
        assert_eq!(body["new_tokens"], 1_120_000u64);
    }

    #[test]
    fn budget_shows_the_three_limits_and_the_old_one_as_fallback() {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        use common::token_limits::TokenLimits;
        let item = |pairs: &[(&str, A)]| -> std::collections::HashMap<String, A> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        };

        let none = budget_json(&TokenLimits::default());
        assert_eq!(
            none,
            json!({
                "legacy": false, "max_tokens": 0, "limit_counts_cache": false,
                "max_new_tokens": 0, "max_cache_read_tokens": 0, "max_cache_write_tokens": 0,
            })
        );

        // Old limit, new input + output: the same tokens as the new-tokens limit.
        let old = item(&[("max_tokens", A::N("3000000".into()))]);
        let b = budget_json(&TokenLimits::from_item_with(Some(&old), None));
        assert_eq!(b["legacy"], true);
        assert_eq!(b["max_tokens"], 3_000_000u64);
        assert_eq!(b["max_new_tokens"], 3_000_000u64);
        assert_eq!(b["max_cache_read_tokens"], 0);

        // Old limit counting cache too: not shown as a new-tokens limit.
        let all = item(&[
            ("max_tokens", A::N("3000000".into())),
            ("counts_cache", A::Bool(true)),
        ]);
        let b = budget_json(&TokenLimits::from_item_with(Some(&all), None));
        assert_eq!(b["limit_counts_cache"], true);
        assert_eq!(b["max_new_tokens"], 0);

        let split = item(&[
            ("max_new_tokens", A::N("0".into())),
            ("max_cache_read_tokens", A::N("40000000".into())),
        ]);
        let b = budget_json(&TokenLimits::from_item_with(Some(&split), None));
        assert_eq!(b["legacy"], false);
        assert_eq!(b["max_cache_read_tokens"], 40_000_000u64);
        assert_eq!(b["max_new_tokens"], 0);
        assert_eq!(b["max_cache_write_tokens"], 0);
    }

    #[test]
    fn only_listed_emails_are_platform_admins() {
        let admins = "admin@coderhelm.com, ops@coderhelm.com";
        assert!(platform_admin_listed(admins, "admin@coderhelm.com"));
        assert!(platform_admin_listed(admins, "Ops@CoderHelm.com"));
        assert!(!platform_admin_listed(admins, "owner@example.com"));
        assert!(!platform_admin_listed(admins, ""));
        assert!(!platform_admin_listed("", "admin@coderhelm.com"));
        assert!(!platform_admin_listed(" , ", ""));
    }

    #[test]
    fn start_over_refuses_runs_in_flight_and_merged_runs() {
        for status in ["pending", "queued", "running", "awaiting_ci", "merged"] {
            assert!(!reset_allowed(status), "{status} must not be started over");
        }
        for status in [
            "completed",
            "failed",
            "needs_input",
            "cancelled",
            "closed",
            "retried",
            "success",
        ] {
            assert!(reset_allowed(status), "{status} may be started over");
        }
    }
}

#[cfg(test)]
mod git_conventions_tests {
    use super::{git_conventions_json, parse_git_conventions};
    use serde_json::json;

    #[test]
    fn saves_valid_templates_and_blank_keeps_the_default() {
        let c = parse_git_conventions(&json!({
            "branch": " fix/{ticket_lower}-{slug} ",
            "commit": "{ticket}: {summary}",
            "pr_title": ""
        }))
        .unwrap();
        assert_eq!(c.branch.as_deref(), Some("fix/{ticket_lower}-{slug}"));
        assert_eq!(c.commit.as_deref(), Some("{ticket}: {summary}"));
        assert_eq!(c.pr_title, None);
        let shown = git_conventions_json(&c);
        assert_eq!(shown["pr_title"], "");
        assert_eq!(shown["defaults"]["branch"], "coderhelm/{ticket_lower}");
        assert!(shown["tokens"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t == "slug"));
    }

    #[test]
    fn refuses_an_invalid_template_naming_the_field() {
        let (field, error) =
            parse_git_conventions(&json!({ "branch": "fix it/{ticket}" })).unwrap_err();
        assert_eq!(field, "branch");
        assert!(error.contains("valid git branch name"), "{error}");
        let (field, error) =
            parse_git_conventions(&json!({ "pr_title": "{ticket} {owner}" })).unwrap_err();
        assert_eq!(field, "pr_title");
        assert!(error.contains("`{owner}` is not a token"), "{error}");
        let (field, _) = parse_git_conventions(&json!({ "branch": "fix/{slug}" })).unwrap_err();
        assert_eq!(field, "branch");
    }
}

#[cfg(test)]
mod trace_detail_tests {
    use super::trace_detail;
    use serde_json::{json, Value};

    #[test]
    fn a_passs_detail_reads_back_as_json_or_as_kept() {
        let kept = r#"{"step":"plan","revised":true,"reviews":[{"agent":"spec-review","gaps":"No rollback task"}]}"#.to_string();
        let d = trace_detail(Some(&kept));
        assert_eq!(d["revised"], true);
        assert_eq!(d["reviews"][0]["gaps"], "No rollback task");
        assert_eq!(trace_detail(Some(&"cut {".to_string())), json!("cut {"));
        assert_eq!(trace_detail(None), Value::Null);
    }
}
