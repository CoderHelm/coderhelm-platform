use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::jwt;
use crate::middleware::auth::SESSION_TTL_SECS;
use crate::AppState;

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

/// Pick which team a login session should land on, given the user's membership
/// records (users-table GSI2 by email) and their Cognito sub.
///
/// Every user gets an auto-created PERSONAL team (`TEAM#{cognito_sub}`) on first
/// login. If they are ALSO an active member of a real team they were invited
/// into, they should land THERE — where the model key and repos live — not on
/// their empty personal team. The old logic just took the first non-`invited`
/// record, and the always-active personal team usually won, so an invited user
/// (e.g. a teammate invited to a company team) got prompted for their own Anthropic key.
///
/// Preference order (all restricted to active, non-`invited` `USER#` records):
///   1. a team the user was ADDED to — `role != "owner"` (member/admin). This
///      is the strongest signal and works regardless of team-id format (the
///      personal team's id is `TEAM#{cognito_sub}` for email users but
///      `TEAM#{ulid}` for Google users, so an id check alone isn't portable).
///   2. else a team that is NOT the Cognito personal team (covers a rare
///      co-owner invited into a real team, for email/Cognito users).
///   3. else any active membership (their personal team).
///
/// Returns `None` when the user has no team yet (first login → create personal).
fn select_session_team<'a>(
    items: &'a [HashMap<String, AttributeValue>],
    cognito_sub: &str,
) -> Option<&'a HashMap<String, AttributeValue>> {
    let personal = format!("TEAM#{cognito_sub}");
    let sval = |it: &HashMap<String, AttributeValue>, k: &str| {
        it.get(k).and_then(|v| v.as_s().ok()).cloned()
    };
    let usable = |it: &HashMap<String, AttributeValue>| {
        sval(it, "sk")
            .map(|s| s.starts_with("USER#"))
            .unwrap_or(false)
            && sval(it, "status").map(|s| s != "invited").unwrap_or(true)
    };
    items
        .iter()
        .find(|it| usable(it) && sval(it, "role").as_deref() != Some("owner"))
        .or_else(|| {
            items
                .iter()
                .find(|it| usable(it) && sval(it, "pk").as_deref() != Some(personal.as_str()))
        })
        .or_else(|| items.iter().find(|it| usable(it)))
}

/// Compute Cognito SECRET_HASH = Base64(HMAC-SHA256(client_secret, username + client_id))
pub fn cognito_secret_hash(client_secret: &str, username: &str, client_id: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(client_secret.as_bytes()).expect("HMAC key length");
    mac.update(username.as_bytes());
    mac.update(client_id.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

// ── Closed beta allowlist ───────────────────────────────────────────

/// Check if an email is on the closed beta allowlist.
/// Returns true if the allowlist is empty (open access) or if the email is listed.
pub async fn is_email_allowed(state: &AppState, email: &str) -> bool {
    let email = normalize_email(email);

    // Check for exact email match
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s("ALLOWLIST"))
        .key("sk", attr_s(&format!("EMAIL#{email}")))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    if result.is_some() {
        return true;
    }

    // Check for domain wildcard (e.g. EMAIL#*@company.com)
    if let Some(domain) = email.split('@').nth(1) {
        let domain_result = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s("ALLOWLIST"))
            .key("sk", attr_s(&format!("EMAIL#*@{domain}")))
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned());

        if domain_result.is_some() {
            return true;
        }
    }

    false
}

// ── Request bodies ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    #[allow(dead_code)]
    pub name: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct VerifyEmailRequest {
    pub email: String,
    pub code: String,
}

#[derive(Deserialize)]
pub struct ForgotPasswordRequest {
    pub email: String,
}

#[derive(Deserialize)]
pub struct ConfirmResetRequest {
    pub email: String,
    pub code: String,
    pub new_password: String,
}

#[derive(Deserialize)]
pub struct MfaChallengeRequest {
    pub session: String,
    pub code: String,
    pub email: String,
}

#[derive(Deserialize)]
pub struct CallbackParams {
    code: String,
    state: Option<String>,
}

#[derive(Deserialize)]
pub struct GoogleCallbackParams {
    code: String,
    state: Option<String>,
}

// ── Signup ───────────────────────────────────────────────────────────

/// POST /auth/signup — Create account with email + password via Cognito.
pub async fn signup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SignupRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let email = body.email.trim().to_lowercase();
    if email.is_empty() || body.password.len() < 8 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Email and password (min 8 chars) are required"
            })),
        ));
    }

    // Closed beta: check allowlist
    if !is_email_allowed(&state, &email).await {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Coderhelm is in closed beta. Join the waitlist to get notified when we open up."
            })),
        ));
    }

    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    let mut req = state
        .cognito
        .sign_up()
        .client_id(&state.config.cognito_client_id)
        .secret_hash(&hash)
        .username(&email)
        .password(&body.password)
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value(&email)
                .build()
                .unwrap(),
        );

    if let Some(name) = &body.name {
        req = req.user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("name")
                .value(name)
                .build()
                .unwrap(),
        );
    }

    let result = req.send().await.map_err(|e| {
        let msg = format!("{e}");
        if msg.contains("UsernameExistsException") {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "An account with this email already exists"
                })),
            );
        }
        error!("Cognito signup failed: {e}");
        let user_msg = if msg.contains("InvalidPasswordException")
            || msg.contains("Password did not conform")
        {
            "Password must contain at least 8 characters, including uppercase, lowercase, a number, and a special character"
        } else if msg.contains("InvalidParameterException") {
            "Invalid email or password format"
        } else {
            "Signup failed — please try again"
        };
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": user_msg })),
        )
    })?;

    let cognito_sub = result.user_sub();

    info!(email = %email, cognito_sub = %cognito_sub, "User signed up, verification pending");

    Ok(Json(serde_json::json!({
        "status": "verification_required",
        "message": "Check your email for a verification code"
    })))
}

// ── Verify email ─────────────────────────────────────────────────────

/// POST /auth/verify-email — Confirm signup with verification code.
pub async fn verify_email(
    State(state): State<Arc<AppState>>,
    Json(body): Json<VerifyEmailRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let email = body.email.trim().to_lowercase();

    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    state
        .cognito
        .confirm_sign_up()
        .client_id(&state.config.cognito_client_id)
        .secret_hash(&hash)
        .username(&email)
        .confirmation_code(&body.code)
        .send()
        .await
        .map_err(|e| {
            error!("Email verification failed: {e}");
            StatusCode::BAD_REQUEST
        })?;

    // Fetch the confirmed user to get cognito_sub
    let user_result = state
        .cognito
        .admin_get_user()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .username(&email)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch user after verification: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let cognito_sub = user_result.username();

    // Someone invited (or already on a team) lands there at first sign-in:
    // no personal team for them. A failed lookup falls back to making one, as
    // before.
    let records = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi2")
        .key_condition_expression("gsi2pk = :pk")
        .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", normalize_email(&email))))
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    if !needs_personal_team(&records, chrono::Utc::now()) {
        info!(email = %email, "Email verified; invited or already on a team, so no personal team");
        send_welcome_email(&state, &email).await;
        return Ok(Json(serde_json::json!({
            "status": "verified",
            "message": "Email verified. You can now log in."
        })));
    }

    // Create a personal team for the new user
    let team_id = format!("TEAM#{cognito_sub}");
    let user_id = format!("USER#{cognito_sub}");
    let now = chrono::Utc::now().to_rfc3339();

    // Create team META record
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&team_id))
        .item("sk", attr_s("META"))
        .item("status", attr_s("active"))
        .item("created_at", attr_s(&now))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;

    // Also create teams table record (for future GitHub/Jira linking)
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.teams_table_name)
        .item("team_id", attr_s(&team_id))
        .item("sk", attr_s("META"))
        .item("created_at", attr_s(&now))
        .condition_expression("attribute_not_exists(team_id)")
        .send()
        .await;

    // Create user record
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .item("pk", attr_s(&team_id))
        .item("sk", attr_s(&user_id))
        .item("email", attr_s(&email))
        .item("role", attr_s("owner"))
        .item("avatar_url", attr_s(""))
        .item("updated_at", attr_s(&now))
        .item(
            "gsi2pk",
            attr_s(&format!("EMAIL#{}", normalize_email(&email))),
        )
        .item("gsi2sk", attr_s(&team_id))
        .send()
        .await;

    info!(email = %email, team_id = %team_id, "Email verified, user + team created");

    // Send welcome email
    send_welcome_email(&state, &email).await;

    Ok(Json(serde_json::json!({
        "status": "verified",
        "message": "Email verified. You can now log in."
    })))
}

// ── Login (email/password) ───────────────────────────────────────────

/// POST /auth/login — Authenticate with email + password.
pub async fn login_email(
    State(state): State<Arc<AppState>>,
    Json(body): Json<LoginRequest>,
) -> Result<Response, StatusCode> {
    let email = body.email.trim().to_lowercase();

    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    let auth_result = state
        .cognito
        .initiate_auth()
        .client_id(&state.config.cognito_client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &email)
        .auth_parameters("PASSWORD", &body.password)
        .auth_parameters("SECRET_HASH", &hash)
        .send()
        .await
        .map_err(|e| {
            let msg = format!("{e}");
            if msg.contains("NotAuthorizedException") {
                StatusCode::UNAUTHORIZED
            } else if msg.contains("UserNotConfirmedException") {
                StatusCode::FORBIDDEN
            } else {
                error!("Cognito auth failed: {e}");
                StatusCode::UNAUTHORIZED
            }
        })?;

    // Check if MFA challenge is required
    if let Some(challenge) = auth_result.challenge_name() {
        if *challenge == aws_sdk_cognitoidentityprovider::types::ChallengeNameType::SoftwareTokenMfa
        {
            let session = auth_result.session().unwrap_or_default();
            return Ok(Json(serde_json::json!({
                "status": "mfa_required",
                "session": session,
            }))
            .into_response());
        }
    }

    // No MFA — issue JWT
    let auth_result = auth_result
        .authentication_result()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let access_token = auth_result
        .access_token()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    issue_session_from_cognito(&state, &email, access_token).await
}

// ── MFA verify ───────────────────────────────────────────────────────

/// POST /auth/mfa/verify — Complete MFA challenge during login.
pub async fn mfa_verify(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MfaChallengeRequest>,
) -> Result<Response, StatusCode> {
    let email = body.email.trim().to_lowercase();
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    let result = state
        .cognito
        .respond_to_auth_challenge()
        .client_id(&state.config.cognito_client_id)
        .challenge_name(aws_sdk_cognitoidentityprovider::types::ChallengeNameType::SoftwareTokenMfa)
        .session(&body.session)
        .challenge_responses("SOFTWARE_TOKEN_MFA_CODE", &body.code)
        .challenge_responses("USERNAME", &email)
        .challenge_responses("SECRET_HASH", &hash)
        .send()
        .await
        .map_err(|e| {
            error!("MFA verification failed: {e}");
            StatusCode::UNAUTHORIZED
        })?;

    let auth_result = result
        .authentication_result()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let access_token = auth_result
        .access_token()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get user email from Cognito
    let user = state
        .cognito
        .get_user()
        .access_token(access_token)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get user after MFA: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let email = user
        .user_attributes()
        .iter()
        .find(|a| a.name() == "email")
        .and_then(|a| a.value().map(|v| v.to_string()))
        .unwrap_or_default();

    issue_session_from_cognito(&state, &email, access_token).await
}

// ── Forgot password ──────────────────────────────────────────────────

/// POST /auth/forgot-password — Send password reset code.
pub async fn forgot_password(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ForgotPasswordRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let email = body.email.trim().to_lowercase();
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    // Always return success to prevent email enumeration
    let _ = state
        .cognito
        .forgot_password()
        .client_id(&state.config.cognito_client_id)
        .secret_hash(&hash)
        .username(&email)
        .send()
        .await;

    Ok(Json(serde_json::json!({
        "status": "sent",
        "message": "If an account exists, a reset code was sent"
    })))
}

/// POST /auth/confirm-reset — Reset password with code.
pub async fn confirm_reset(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ConfirmResetRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let email = body.email.trim().to_lowercase();
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &email,
        &state.config.cognito_client_id,
    );

    state
        .cognito
        .confirm_forgot_password()
        .client_id(&state.config.cognito_client_id)
        .secret_hash(&hash)
        .username(&email)
        .confirmation_code(&body.code)
        .password(&body.new_password)
        .send()
        .await
        .map_err(|e| {
            error!("Password reset confirmation failed: {e}");
            StatusCode::BAD_REQUEST
        })?;

    Ok(Json(serde_json::json!({
        "status": "reset",
        "message": "Password was reset successfully"
    })))
}

// ── Google OAuth ─────────────────────────────────────────────────────

/// CSRF `state` for OAuth: random value carried both in the authorize URL and
/// an HttpOnly cookie; the callback requires them to match. Without it, an
/// attacker could complete the dance with their own `code` in a victim's
/// browser (login-CSRF / session fixation).
fn oauth_state_cookie(name: &str, value: &str) -> String {
    format!("{name}={value}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600")
}

fn validate_oauth_state(req_cookies: &str, cookie_name: &str, query_state: Option<&str>) -> bool {
    let Some(expected) = extract_cookie(req_cookies, cookie_name) else {
        return false;
    };
    query_state.is_some_and(|s| !s.is_empty() && s == expected)
}

/// GET /auth/google — Redirect to Cognito hosted UI for Google login.
pub async fn google_login(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let redirect_uri = format!(
        "{}/auth/google/callback",
        common::public_urls::api_url(&state.config.stage)
    );

    let domain = &state.config.cognito_domain;
    let client_id = &state.config.cognito_client_id;

    // Custom domains (contain '.') use https://{domain}/oauth2/...
    // Cognito prefix domains use https://{domain}.auth.{region}.amazoncognito.com/oauth2/...
    let base = if domain.contains('.') {
        format!("https://{domain}")
    } else {
        format!("https://{domain}.auth.us-east-1.amazoncognito.com")
    };

    let csrf = ulid::Ulid::new().to_string();
    let url = format!(
        "{base}/oauth2/authorize?\
         response_type=code&client_id={client_id}&\
         redirect_uri={redirect_uri}&\
         identity_provider=Google&\
         state={csrf}&\
         scope=openid+email+profile+aws.cognito.signin.user.admin"
    );

    (
        [(
            header::SET_COOKIE,
            oauth_state_cookie("__Host-oauth_state_g", &csrf),
        )],
        Redirect::temporary(&url),
    )
}

/// GET /auth/google/callback — Handle Cognito/Google OAuth callback.
pub async fn google_callback(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<GoogleCallbackParams>,
) -> Result<Response, StatusCode> {
    let cookies = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !validate_oauth_state(cookies, "__Host-oauth_state_g", params.state.as_deref()) {
        warn!("Google OAuth callback with missing/mismatched CSRF state");
        return Err(StatusCode::FORBIDDEN);
    }
    let redirect_uri = format!(
        "{}/auth/google/callback",
        common::public_urls::api_url(&state.config.stage)
    );

    let domain = &state.config.cognito_domain;
    let client_id = &state.config.cognito_client_id;

    // Exchange code for tokens with Cognito token endpoint
    let base = if domain.contains('.') {
        format!("https://{domain}")
    } else {
        format!("https://{domain}.auth.us-east-1.amazoncognito.com")
    };
    let token_url = format!("{base}/oauth2/token");

    let client_secret = &state.cognito_client_secret;

    let token_resp = state
        .http
        .post(&token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &params.code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", &redirect_uri),
        ])
        .send()
        .await
        .map_err(|e| {
            error!("Google token exchange failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let token_data: serde_json::Value = token_resp.json().await.map_err(|e| {
        error!("Failed to parse Cognito token response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let access_token = token_data["access_token"].as_str().ok_or_else(|| {
        error!("No access_token in Cognito response: {token_data}");
        StatusCode::BAD_GATEWAY
    })?;

    // Get user info from Cognito
    let user = state
        .cognito
        .get_user()
        .access_token(access_token)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get Cognito user: {e:?}");
            StatusCode::BAD_GATEWAY
        })?;

    let cognito_sub = user.username();
    let email = user
        .user_attributes()
        .iter()
        .find(|a| a.name() == "email")
        .and_then(|a| a.value().map(|v| v.to_string()))
        .unwrap_or_default();

    let name = user
        .user_attributes()
        .iter()
        .find(|a| a.name() == "name")
        .and_then(|a| a.value().map(|v| v.to_string()));

    let picture = user
        .user_attributes()
        .iter()
        .find(|a| a.name() == "picture")
        .and_then(|a| a.value().map(|v| v.to_string()));

    // Check if user already exists (by normalized email GSI2)
    let existing = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi2")
        .key_condition_expression("gsi2pk = :pk")
        .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", normalize_email(&email))))
        .send()
        .await
        .ok()
        .and_then(|r| r.items)
        .unwrap_or_default();

    // Closed beta: only allow new users on the allowlist (existing users can always log in)
    if existing.is_empty() && !is_email_allowed(&state, &email).await {
        let dash = common::public_urls::app_url(&state.config.stage);
        let redirect = format!("{dash}?error=closed_beta");
        return Ok(Redirect::temporary(&redirect).into_response());
    }

    // Accept pending invites first: Google sign-ins never accepted them, so an
    // invited teammate got a personal team and the invite stayed pending.
    let accepted =
        accept_pending_invites(&state, &existing, &format!("USER#{cognito_sub}"), &email).await;
    let pool = landing_records(&existing, accepted);

    // Prefer a real team the user joined over their personal team (dropped the
    // .limit(1) above so all memberships are visible to the picker).
    let selected = select_session_team(&pool, cognito_sub);
    let (team_id, user_id, role) = if let Some(item) = selected {
        let tid = item
            .get("pk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let uid = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let r = item
            .get("role")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "member".to_string());
        touch_last_login(&state, &tid, &uid).await;
        (tid, uid, r)
    } else {
        // New user — create personal team
        let tid = format!("TEAM#{}", ulid::Ulid::new().to_string().to_lowercase());
        let uid = format!("USER#{cognito_sub}");
        let now = chrono::Utc::now().to_rfc3339();

        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.table_name)
            .item("pk", attr_s(&tid))
            .item("sk", attr_s("META"))
            .item("status", attr_s("active"))
            .item("created_at", attr_s(&now))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;

        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .item("pk", attr_s(&tid))
            .item("sk", attr_s(&uid))
            .item("email", attr_s(&email))
            .item("avatar_url", attr_s(picture.as_deref().unwrap_or("")))
            .item("role", attr_s("owner"))
            .item("name", attr_s(name.as_deref().unwrap_or("")))
            .item("updated_at", attr_s(&now))
            .item(
                "gsi2pk",
                attr_s(&format!("EMAIL#{}", normalize_email(&email))),
            )
            .item("gsi2sk", attr_s(&tid))
            .send()
            .await;

        info!(email = %email, team_id = %tid, "Google user created");
        (tid, uid, "owner".to_string())
    };

    // Issue JWT
    let token = jwt::create_token(
        &user_id,
        &team_id,
        &email,
        &role,
        None,
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let dashboard_url = common::public_urls::app_url(&state.config.stage);

    let cookie = format!(
        "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
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

// ── GitHub OAuth (connect GitHub, not identity) ──────────────────────

/// GET /auth/github — Redirect to GitHub OAuth (for connecting GitHub account).
pub async fn github_login(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client_id = &state.secrets.github_client_id;
    let redirect_uri = format!(
        "{}/auth/github/callback",
        common::public_urls::api_url(&state.config.stage)
    );
    let csrf = ulid::Ulid::new().to_string();
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope=read:user,read:org&state={csrf}"
    );
    (
        [(
            header::SET_COOKIE,
            oauth_state_cookie("__Host-oauth_state_gh", &csrf),
        )],
        Redirect::temporary(&url),
    )
}

/// GET /auth/github/callback — GitHub OAuth callback.
/// Works in two modes:
/// 1. If user has a session cookie → connect GitHub to existing account
/// 2. If no session → legacy login flow (creates account from GitHub)
pub async fn github_callback(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CallbackParams>,
    req: axum::extract::Request,
) -> Result<Response, StatusCode> {
    let cookies = req
        .headers()
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !validate_oauth_state(cookies, "__Host-oauth_state_gh", params.state.as_deref()) {
        warn!("GitHub OAuth callback with missing/mismatched CSRF state");
        return Err(StatusCode::FORBIDDEN);
    }

    // Exchange code for access token
    let client = &state.http;
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

    // Fetch user's app installations to find team
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

    let our_app_id: u64 = state.secrets.github_app_id.parse().unwrap_or(0);

    let all_installations: Vec<(u64, String)> = installs["installations"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|i| i["app_id"].as_u64() == Some(our_app_id))
        .filter_map(|i| {
            let id = i["id"].as_u64()?;
            let org = i["account"]["login"].as_str().unwrap_or("unknown");
            Some((id, org.to_string()))
        })
        .collect();

    // Check if user already has a session (connecting GitHub to existing account)
    let cookie_header = req
        .headers()
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let existing_claims = extract_cookie(cookie_header, "__Host-coderhelm_session")
        .and_then(|token| jwt::validate_token(token, &state.secrets.jwt_secret).ok());

    let now = chrono::Utc::now().to_rfc3339();

    if let Some(claims) = existing_claims {
        // User is already logged in — connect GitHub to their account
        // Update their user record with github_id and github_login
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.users_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&claims.sub))
            .update_expression(
                "SET github_id = :gid, github_login = :gl, \
                 gsi1pk = :g1pk, gsi1sk = :g1sk, updated_at = :now",
            )
            .expression_attribute_values(":gid", attr_n(github_id))
            .expression_attribute_values(":gl", attr_s(github_login))
            .expression_attribute_values(":g1pk", attr_s(&format!("GHUSER#{github_id}")))
            .expression_attribute_values(":g1sk", attr_s(&claims.team_id))
            .expression_attribute_values(":now", attr_s(&now))
            .send()
            .await;

        // Link GitHub installations to the user's team in both tables
        for (inst_id, org) in &all_installations {
            let owner = super::github_webhook::resolve_team_by_installation(&state, *inst_id).await;
            if !may_link_installation(owner.as_deref(), &claims.team_id) {
                warn!(
                    installation_id = inst_id,
                    org,
                    team_id = %claims.team_id,
                    owner = owner.as_deref().unwrap_or(""),
                    "Installation already belongs to another team — not linking it here"
                );
                continue;
            }
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.teams_table_name)
                .key("team_id", attr_s(&claims.team_id))
                .key("sk", attr_s("META"))
                .update_expression(
                    "SET github_installation_id = :iid, github_org = :org, updated_at = :now, \
                     installation_linked_at = if_not_exists(installation_linked_at, :now)",
                )
                .expression_attribute_values(
                    ":iid",
                    aws_sdk_dynamodb::types::AttributeValue::N(inst_id.to_string()),
                )
                .expression_attribute_values(":org", attr_s(org))
                .expression_attribute_values(":now", attr_s(&now))
                .send()
                .await;

            // Also write to main table for worker compatibility
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s("META"))
                .update_expression(
                    "SET github_install_id = :iid, github_org = :org, updated_at = :now",
                )
                .expression_attribute_values(
                    ":iid",
                    aws_sdk_dynamodb::types::AttributeValue::N(inst_id.to_string()),
                )
                .expression_attribute_values(":org", attr_s(org))
                .expression_attribute_values(":now", attr_s(&now))
                .send()
                .await;

            info!(
                installation_id = inst_id,
                org,
                team_id = %claims.team_id,
                "Linked GitHub installation to team"
            );

            // Sync repos from the installation (only add repos not already present)
            let repos = super::github_webhook::fetch_installation_repos(&state, *inst_id).await;
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
                    .condition_expression("attribute_not_exists(pk)")
                    .send()
                    .await;
            }
            info!(
                installation_id = inst_id,
                repos_synced = repos.len(),
                team_id = %claims.team_id,
                "Synced repos for connected GitHub installation"
            );
        }

        // Re-issue JWT with github_login (keep the user's existing team)
        let token = jwt::create_token(
            &claims.sub,
            &claims.team_id,
            &claims.email,
            &claims.role,
            Some(github_login),
            &state.secrets.jwt_secret,
            SESSION_TTL_SECS,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        info!(github_login, "GitHub connected to existing account");

        let dashboard_url = common::public_urls::app_url(&state.config.stage);

        let cookie = format!(
            "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
        );

        return Ok((
            [
                (header::SET_COOKIE, cookie),
                (header::LOCATION, dashboard_url.to_string()),
            ],
            StatusCode::FOUND,
        )
            .into_response());
    }

    // No existing session — GitHub-only login flow
    let github_user_id = format!("USER#{github_id}");
    let email = user["email"].as_str().unwrap_or("");
    let normalized_email = normalize_email(email);

    // 1. Try to find an existing user by GitHub ID (GSI1 across all teams)
    let existing_by_github = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi1")
        .key_condition_expression("gsi1pk = :gpk")
        .expression_attribute_values(":gpk", attr_s(&format!("GHUSER#{github_id}")))
        .limit(1)
        .send()
        .await
        .ok()
        .and_then(|r| r.items().first().cloned());

    // 2. If not found by GitHub ID, try by email
    let existing_by_email = if existing_by_github.is_none() && !normalized_email.is_empty() {
        state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .index_name("gsi2")
            .key_condition_expression("gsi2pk = :email")
            .expression_attribute_values(":email", attr_s(&format!("EMAIL#{normalized_email}")))
            .send()
            .await
            .ok()
            .and_then(|r| {
                // Prefer a real team the user joined over their personal team
                // (github_id serves as the own-id for the personal-team check).
                let items = r.items().to_vec();
                select_session_team(&items, &github_id.to_string()).cloned()
            })
    } else {
        None
    };

    let existing_user = existing_by_github.or(existing_by_email);

    let has_existing_record = existing_user.is_some();

    // Closed beta: check allowlist for new GitHub users
    if !has_existing_record && !email.is_empty() && !is_email_allowed(&state, email).await {
        let dash = common::public_urls::app_url(&state.config.stage);
        let redirect = format!("{dash}?error=closed_beta");
        return Ok(Redirect::temporary(&redirect).into_response());
    }

    // A first-time sign-in with no invite, from an org whose installation a
    // team already owns: do NOT mint a new team that also claims the
    // installation (each such team split the org and stole its webhooks).
    // The person needs an invite from that team; an invited person already
    // has a record and never reaches this branch.
    if !has_existing_record {
        for (inst_id, org) in &all_installations {
            if let Some(owner) =
                super::github_webhook::resolve_team_by_installation(&state, *inst_id).await
            {
                info!(
                    github_login,
                    installation_id = inst_id,
                    org = org.as_str(),
                    owner_team = %owner,
                    "First GitHub sign-in for an installation another team owns — asking for an invite"
                );
                let dash = common::public_urls::app_url(&state.config.stage);
                return Ok(Redirect::temporary(&ask_for_invite_url(dash, org)).into_response());
            }
        }
    }

    let (team_id, user_id, role) = if let Some(ref item) = existing_user {
        // Existing user — extract team_id, user_id, role
        let tid = item
            .get("pk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let uid = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| github_user_id.clone());
        let r = item
            .get("role")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "member".to_string());
        (tid, uid, r)
    } else {
        // Brand new user — create a new team with ULID
        let new_team_id = format!("TEAM#{}", ulid::Ulid::new().to_string().to_lowercase());
        let uid = github_user_id.clone();

        // Create team META in main table
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.table_name)
            .item("pk", attr_s(&new_team_id))
            .item("sk", attr_s("META"))
            .item("status", attr_s("active"))
            .item("plan", attr_s("free"))
            .item("run_count_mtd", attr_n(0))
            .item("created_at", attr_s(&now))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;

        (new_team_id, uid, "owner".to_string())
    };

    // Upsert the member record. An UPDATE that only sets the GitHub fields:
    // a put here used to replace the whole record, wiping a linked member's
    // email (GitHub's is often empty), status, name, invited_by and email
    // index keys.
    let upsert = github_signin_update(
        existing_user.as_ref(),
        &GithubProfile {
            id: github_id,
            login: github_login,
            email,
            avatar_url: user["avatar_url"].as_str().unwrap_or(""),
            name: user["name"].as_str().unwrap_or(""),
        },
        &team_id,
        &role,
        &now,
    );
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&team_id))
        .key("sk", attr_s(&user_id))
        .update_expression(upsert.expression)
        .set_expression_attribute_names(upsert.names)
        .set_expression_attribute_values(Some(upsert.values))
        .send()
        .await
    {
        error!(github_login, team_id = %team_id, "Failed to upsert GitHub user record: {e}");
    }
    // The session carries the member's own email when GitHub has none.
    let email = existing_user
        .as_ref()
        .and_then(|item| item.get("email"))
        .and_then(|v| v.as_s().ok())
        .filter(|e| !e.is_empty())
        .map_or(email, |e| e.as_str());

    // Ensure teams table record exists (for new users without an existing record)
    if !has_existing_record {
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.teams_table_name)
            .item("team_id", attr_s(&team_id))
            .item("sk", attr_s("META"))
            .item("created_at", attr_s(&now))
            .condition_expression("attribute_not_exists(team_id)")
            .send()
            .await;

        // Send welcome email for new GitHub signups
        send_welcome_email(&state, email).await;
    }

    // Link GitHub installations to the team in the teams table — only when no
    // OTHER team owns the installation already.
    let link_target = match all_installations.first() {
        Some(&(inst_id, _)) => {
            let owner = super::github_webhook::resolve_team_by_installation(&state, inst_id).await;
            if may_link_installation(owner.as_deref(), &team_id) {
                Some(inst_id)
            } else {
                warn!(
                    github_login,
                    installation_id = inst_id,
                    team_id = %team_id,
                    owner = owner.as_deref().unwrap_or(""),
                    "Installation already belongs to another team — not linking it here"
                );
                None
            }
        }
        None => None,
    };
    if link_target.is_some() {
        let (inst_id, ref org) = all_installations[0];
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.teams_table_name)
            .key("team_id", attr_s(&team_id))
            .key("sk", attr_s("META"))
            .update_expression(
                "SET github_installation_id = :iid, github_org = :org, updated_at = :now, \
                 installation_linked_at = if_not_exists(installation_linked_at, :now)",
            )
            .expression_attribute_values(
                ":iid",
                aws_sdk_dynamodb::types::AttributeValue::N(inst_id.to_string()),
            )
            .expression_attribute_values(":org", attr_s(org))
            .expression_attribute_values(":now", attr_s(&now))
            .send()
            .await;

        // Also write to main table for worker compatibility
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&team_id))
            .key("sk", attr_s("META"))
            .update_expression("SET github_install_id = :iid, github_org = :org, updated_at = :now")
            .expression_attribute_values(
                ":iid",
                aws_sdk_dynamodb::types::AttributeValue::N(inst_id.to_string()),
            )
            .expression_attribute_values(":org", attr_s(org))
            .expression_attribute_values(":now", attr_s(&now))
            .send()
            .await;

        info!(
            github_login,
            installation_id = inst_id,
            org = org.as_str(),
            team_id = %team_id,
            "Linked GitHub installation to team"
        );

        // Sync repos from the installation (only add repos not already present)
        let repos = super::github_webhook::fetch_installation_repos(&state, inst_id).await;
        for repo in &repos {
            let full = format!("{}/{}", repo.owner, repo.name);
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.repos_table_name)
                .item("pk", attr_s(&team_id))
                .item("sk", attr_s(&format!("REPO#{full}")))
                .item("repo_name", attr_s(&full))
                .item(
                    "enabled",
                    aws_sdk_dynamodb::types::AttributeValue::Bool(false),
                )
                .item("ticket_source", attr_s("github"))
                .item("created_at", attr_s(&now))
                .condition_expression("attribute_not_exists(pk)")
                .send()
                .await;
        }
    }

    info!(
        github_login,
        %role,
        %team_id,
        "User authenticated via GitHub"
    );

    let token = jwt::create_token(
        &user_id,
        &team_id,
        email,
        &role,
        Some(github_login),
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let dashboard_url = common::public_urls::app_url(&state.config.stage);

    let cookie = format!(
        "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
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

/// Record when a member last signed in (shown on the team page). Only that one
/// attribute is written, and only on a record that exists.
async fn touch_last_login(state: &AppState, team_id: &str, user_id: &str) {
    if team_id.is_empty() || user_id.is_empty() {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(user_id))
        .update_expression("SET last_login_at = :now")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

/// The GitHub profile fields a sign-in may write.
struct GithubProfile<'a> {
    id: u64,
    login: &'a str,
    email: &'a str,
    avatar_url: &'a str,
    name: &'a str,
}

/// An `UpdateItem` expression and its values.
#[derive(Debug)]
struct RecordUpdate {
    expression: String,
    names: Option<HashMap<String, String>>,
    values: HashMap<String, AttributeValue>,
}

/// The write a GitHub sign-in makes to a member record. It always refreshes the
/// GitHub id/login, GSI1 keys, avatar and timestamps. On an existing record it
/// never touches role, status, invited_by, or an email/name already set: it
/// fills email (and the GSI2 email keys) or name only when the record has none.
/// A brand-new record also gets its role, status and creation time.
fn github_signin_update(
    existing: Option<&HashMap<String, AttributeValue>>,
    gh: &GithubProfile<'_>,
    team_id: &str,
    role: &str,
    now: &str,
) -> RecordUpdate {
    let field = |k: &str| {
        existing
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .map_or("", |v| v.as_str())
    };
    let mut sets = vec![
        "github_id = :gid",
        "github_login = :gl",
        "gsi1pk = :g1pk",
        "gsi1sk = :team",
        "updated_at = :now",
        "last_login_at = :now",
    ];
    let mut values = HashMap::from([
        (":gid".to_string(), attr_n(gh.id)),
        (":gl".to_string(), attr_s(gh.login)),
        (":g1pk".to_string(), attr_s(&format!("GHUSER#{}", gh.id))),
        (":team".to_string(), attr_s(team_id)),
        (":now".to_string(), attr_s(now)),
    ]);
    let mut names = HashMap::new();
    if !gh.avatar_url.is_empty() {
        sets.push("avatar_url = :avatar");
        values.insert(":avatar".to_string(), attr_s(gh.avatar_url));
    }
    let normalized = normalize_email(gh.email);
    if field("email").is_empty() && !normalized.is_empty() {
        sets.extend(["email = :email", "gsi2pk = :g2pk", "gsi2sk = :team"]);
        values.insert(":email".to_string(), attr_s(gh.email));
        values.insert(":g2pk".to_string(), attr_s(&format!("EMAIL#{normalized}")));
    }
    if field("name").is_empty() && !gh.name.is_empty() {
        sets.push("#n = :name");
        names.insert("#n".to_string(), "name".to_string());
        values.insert(":name".to_string(), attr_s(gh.name));
    }
    if existing.is_none() {
        sets.extend(["#r = :role", "#st = :active", "created_at = :now"]);
        names.insert("#r".to_string(), "role".to_string());
        names.insert("#st".to_string(), "status".to_string());
        values.insert(":role".to_string(), attr_s(role));
        values.insert(":active".to_string(), attr_s("active"));
    }
    RecordUpdate {
        expression: format!("SET {}", sets.join(", ")),
        names: (!names.is_empty()).then_some(names),
        values,
    }
}

/// A team may link an installation only when no other team owns it.
fn may_link_installation(owner: Option<&str>, team_id: &str) -> bool {
    owner.is_none_or(|o| o == team_id)
}

/// Where a first-time sign-in lands when their org's installation already
/// belongs to a team: the dashboard tells them to ask that team's admin for an
/// invite.
fn ask_for_invite_url(dash: &str, org: &str) -> String {
    format!(
        "{dash}?error=ask_admin_invite&org={}",
        urlencoding::encode(org)
    )
}

// ── Logout ───────────────────────────────────────────────────────────

/// POST /auth/logout — Clear session cookie.
pub async fn logout() -> impl IntoResponse {
    let cookie = "__Host-coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
    ([(header::SET_COOKIE, cookie.to_string())], StatusCode::OK)
}

// ── Helpers ──────────────────────────────────────────────────────────

/// After Cognito auth, look up user in DynamoDB and issue a session JWT.
async fn issue_session_from_cognito(
    state: &Arc<AppState>,
    email: &str,
    access_token: &str,
) -> Result<Response, StatusCode> {
    // Get Cognito user info
    let user = state
        .cognito
        .get_user()
        .access_token(access_token)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get Cognito user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let cognito_sub = user.username();

    // Look up user by email (GSI2) — includes personal team + any pending invites
    let existing = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi2")
        .key_condition_expression("gsi2pk = :pk")
        .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", normalize_email(email))))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to look up user by email: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = existing.items().to_vec();

    // Accept pending invites FIRST, so a teammate's first sign-in lands on the
    // team that invited them instead of minting a personal team that then
    // lingers, empty, in the admin overview.
    let accepted =
        accept_pending_invites(state, &items, &format!("USER#{cognito_sub}"), email).await;
    let pool = landing_records(&items, accepted);

    // Prefer a real team the user was added to over their auto-created personal
    // team (see select_session_team) — else an invited teammate lands on their
    // empty personal team and gets prompted for an Anthropic key.
    let active_item = select_session_team(&pool, cognito_sub);

    let (team_id, user_id, role, github_login) = if let Some(item) = active_item {
        let tid = item
            .get("pk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let uid = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let r = item
            .get("role")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "member".to_string());
        let gl = item
            .get("github_login")
            .and_then(|v| v.as_s().ok())
            .cloned();
        (tid, uid, r, gl)
    } else {
        // First login — create personal team
        let tid = format!("TEAM#{cognito_sub}");
        let uid = format!("USER#{cognito_sub}");
        let now = chrono::Utc::now().to_rfc3339();

        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.table_name)
            .item("pk", attr_s(&tid))
            .item("sk", attr_s("META"))
            .item("status", attr_s("active"))
            .item("created_at", attr_s(&now))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;

        // Create teams table record
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.teams_table_name)
            .item("team_id", attr_s(&tid))
            .item("sk", attr_s("META"))
            .item("created_at", attr_s(&now))
            .condition_expression("attribute_not_exists(team_id)")
            .send()
            .await;

        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .item("pk", attr_s(&tid))
            .item("sk", attr_s(&uid))
            .item("email", attr_s(email))
            .item("role", attr_s("owner"))
            .item("avatar_url", attr_s(""))
            .item("updated_at", attr_s(&now))
            .item(
                "gsi2pk",
                attr_s(&format!("EMAIL#{}", normalize_email(email))),
            )
            .item("gsi2sk", attr_s(&tid))
            .send()
            .await;

        info!(email = %email, "Created new user + team on first login");
        (tid, uid, "owner".to_string(), None)
    };

    touch_last_login(state, &team_id, &user_id).await;

    let token = jwt::create_token(
        &user_id,
        &team_id,
        email,
        &role,
        github_login.as_deref(),
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cookie = format!(
        "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
    );

    Ok((
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({
            "status": "ok",
            "team_id": team_id,
        })),
    )
        .into_response())
}

/// Accept every live invite among `items` (the person's records by email):
/// a member record in each inviting team, and the invite removed. Returns the
/// member records it made, so the sign-in can land on one of them. An expired
/// invite is skipped and stays on the team page, marked expired.
async fn accept_pending_invites(
    state: &AppState,
    items: &[HashMap<String, AttributeValue>],
    real_user_id: &str,
    email: &str,
) -> Vec<HashMap<String, AttributeValue>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut accepted = Vec::new();
    for item in items {
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
        if crate::routes::users::invite_expired(item, chrono::Utc::now()) {
            info!(team_id = %invite_tid, "Skipped an expired invite on login");
            continue;
        }
        let invite_role = item
            .get("role")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "member".to_string());
        let record = HashMap::from([
            ("pk".to_string(), attr_s(&invite_tid)),
            ("sk".to_string(), attr_s(real_user_id)),
            ("email".to_string(), attr_s(email)),
            ("role".to_string(), attr_s(&invite_role)),
            ("avatar_url".to_string(), attr_s("")),
            ("updated_at".to_string(), attr_s(&now)),
            (
                "gsi2pk".to_string(),
                attr_s(&format!("EMAIL#{}", normalize_email(email))),
            ),
            ("gsi2sk".to_string(), attr_s(&invite_tid)),
            ("status".to_string(), attr_s("active")),
            (
                "invited_by".to_string(),
                attr_s(
                    item.get("invited_by")
                        .and_then(|v| v.as_s().ok())
                        .map_or("", |v| v),
                ),
            ),
        ]);
        // Already a member of that team (e.g. linked to GitHub): keep their
        // record as it is and just drop the invite.
        let created = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .set_item(Some(record.clone()))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await
            .is_ok();

        if let Some(invite_sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.users_table_name)
                .key("pk", attr_s(&invite_tid))
                .key("sk", attr_s(invite_sk))
                .send()
                .await;
            info!(email = %email, team_id = %invite_tid, role = %invite_role, "Processed pending invite on login");
        }
        if created {
            accepted.push(record);
        }
    }
    accepted
}

/// The records a sign-in may land on: the person's memberships (never a bare
/// invite) plus the ones just made from accepted invites.
fn landing_records(
    items: &[HashMap<String, AttributeValue>],
    accepted: Vec<HashMap<String, AttributeValue>>,
) -> Vec<HashMap<String, AttributeValue>> {
    items
        .iter()
        .filter(|i| {
            i.get("status")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                != Some("invited")
        })
        .cloned()
        .chain(accepted)
        .collect()
}

/// Whether confirming an email sign-up should make a personal team: only when
/// the email has no membership and no live invite. Someone invited lands on
/// the inviting team at first sign-in; a personal team made here would sit
/// empty forever (most of the admin overview's duplicate teams).
fn needs_personal_team(
    items: &[HashMap<String, AttributeValue>],
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    !items.iter().any(|i| {
        let user = i
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .is_some_and(|s| s.starts_with("USER#"));
        let invited = i
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            == Some("invited");
        user && (!invited || !crate::routes::users::invite_expired(i, now))
    })
}

fn extract_cookie<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header
        .split(';')
        .map(|c| c.trim())
        .find(|c| c.starts_with(&format!("{name}=")))
        .and_then(|c| c.split_once('='))
        .map(|(_, v)| v)
}

/// Normalize an email: lowercase + strip plus-alias (user+tag@domain.com → user@domain.com)
pub(crate) fn normalize_email(email: &str) -> String {
    let email = email.trim().to_lowercase();
    if let Some((local, domain)) = email.split_once('@') {
        let base = local.split('+').next().unwrap_or(local);
        format!("{base}@{domain}")
    } else {
        email
    }
}

// ── Waitlist ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct WaitlistRequest {
    pub email: String,
    pub reason: Option<String>,
    /// Where they work, if they said.
    pub company: Option<String>,
}

/// An optional free-text field, trimmed and cut to `max` characters.
fn short_text(v: Option<&str>, max: usize) -> String {
    v.unwrap_or("").trim().chars().take(max).collect()
}

/// The waitlist record for one sign-up: keyed by the normalised email, with
/// what they typed, when, and the optional company and message.
fn waitlist_item(
    normalized: &str,
    email: &str,
    body: &WaitlistRequest,
    now: &str,
) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    let mut item = HashMap::from([
        ("email".to_string(), attr_s(normalized)),
        ("original_email".to_string(), attr_s(email)),
        ("created_at".to_string(), attr_s(now)),
        ("status".to_string(), attr_s("new")),
    ]);
    for (key, value) in [
        ("reason", short_text(body.reason.as_deref(), 500)),
        ("company", short_text(body.company.as_deref(), 120)),
    ] {
        if !value.is_empty() {
            item.insert(key.to_string(), attr_s(&value));
        }
    }
    item
}

/// POST /auth/waitlist — join the closed beta waitlist.
pub async fn join_waitlist(
    State(state): State<Arc<AppState>>,
    Json(body): Json<WaitlistRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let email = body.email.trim().to_lowercase();
    if email.is_empty() || !email.contains('@') {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid email address" })),
        ));
    }

    // Basic email regex validation
    static EMAIL_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$").unwrap()
    });
    if !EMAIL_RE.is_match(&email) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid email format" })),
        ));
    }

    // Normalize: strip plus-alias (user+tag@domain.com → user@domain.com) for dedup
    let normalized = normalize_email(&email);

    // Check if normalized email already exists
    let existing = state
        .dynamo
        .get_item()
        .table_name(&state.config.waitlist_table_name)
        .key("email", attr_s(&normalized))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    if existing.is_some() {
        return Ok(Json(serde_json::json!({
            "status": "already_joined",
            "message": "You're already on the list! We'll notify you when Coderhelm opens up."
        })));
    }

    let now = chrono::Utc::now().to_rfc3339();
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.waitlist_table_name)
        .set_item(Some(waitlist_item(&normalized, &email, &body, &now)))
        // Two quick submits must not reset a sign-up an admin already invited.
        .condition_expression("attribute_not_exists(email)");

    put.send().await.map_err(|e| {
        let e = aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string();
        if e.contains("ConditionalCheckFailed") {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "already_joined",
                    "message": "You're already on the list! We'll notify you when Coderhelm opens up."
                })),
            );
        }
        error!("Failed to save waitlist entry: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "Internal error" })),
        )
    })?;

    info!(email = %email, normalized = %normalized, "Waitlist signup");
    Ok(Json(serde_json::json!({
        "status": "joined",
        "message": "You're on the list! We'll notify you when Coderhelm opens up."
    })))
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Send the welcome email to a single address (best-effort, non-blocking).
async fn send_welcome_email(state: &AppState, email: &str) {
    let template_name = format!("{}-welcome", state.config.ses_template_prefix);
    let template_data = serde_json::json!({ "org": "", "repo_count": 0 }).to_string();

    if let Err(e) = state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(
            aws_sdk_sesv2::types::Destination::builder()
                .to_addresses(email)
                .build(),
        )
        .content(
            aws_sdk_sesv2::types::EmailContent::builder()
                .template(
                    aws_sdk_sesv2::types::Template::builder()
                        .template_name(&template_name)
                        .template_data(&template_data)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
    {
        error!("Failed to send welcome email to {email}: {e}");
    } else {
        info!(email, "Welcome email sent");
    }
}

#[cfg(test)]
mod team_select_tests {
    use super::{select_session_team, AttributeValue, HashMap};

    /// Build a membership record: (team_id pk, USER# sk, role, optional status).
    fn rec(
        pk: &str,
        sk: &str,
        role: &str,
        status: Option<&str>,
    ) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".into(), AttributeValue::S(pk.into()));
        m.insert("sk".into(), AttributeValue::S(sk.into()));
        m.insert("role".into(), AttributeValue::S(role.into()));
        if let Some(s) = status {
            m.insert("status".into(), AttributeValue::S(s.into()));
        }
        m
    }
    fn pk_of(it: &HashMap<String, AttributeValue>) -> &str {
        it.get("pk").and_then(|v| v.as_s().ok()).unwrap()
    }

    #[test]
    fn prefers_joined_team_over_personal() {
        // A user who owns a personal team and is a member of a company team (both active).
        let sub = "7498f458";
        let items = vec![
            rec("TEAM#7498f458", "USER#7498f458", "owner", None),
            rec("TEAM#Google_123", "USER#7498f458", "member", None),
        ];
        assert_eq!(
            pk_of(select_session_team(&items, sub).unwrap()),
            "TEAM#Google_123"
        );
        // Order must not matter.
        let items_rev: Vec<_> = items.into_iter().rev().collect();
        assert_eq!(
            pk_of(select_session_team(&items_rev, sub).unwrap()),
            "TEAM#Google_123"
        );
    }

    #[test]
    fn solo_user_lands_on_personal() {
        let items = vec![rec("TEAM#abc", "USER#abc", "owner", None)];
        assert_eq!(
            pk_of(select_session_team(&items, "abc").unwrap()),
            "TEAM#abc"
        );
    }

    #[test]
    fn pending_invite_is_skipped() {
        // Real-team membership still "invited" → not chosen; land on personal.
        let items = vec![
            rec("TEAM#abc", "USER#abc", "owner", None),
            rec("TEAM#Real", "USER#abc", "member", Some("invited")),
        ];
        assert_eq!(
            pk_of(select_session_team(&items, "abc").unwrap()),
            "TEAM#abc"
        );
    }

    #[test]
    fn google_user_ulid_personal_still_resolves_by_role() {
        // Google personal team is a ulid (id check can't help) — role does.
        let sub = "Google_999";
        let items = vec![
            rec("TEAM#01julid", "USER#Google_999", "owner", None),
            rec("TEAM#Real", "USER#Google_999", "member", None),
        ];
        assert_eq!(
            pk_of(select_session_team(&items, sub).unwrap()),
            "TEAM#Real"
        );
    }

    #[test]
    fn no_team_yet_returns_none() {
        assert!(select_session_team(&[], "abc").is_none());
    }
}

#[cfg(test)]
mod installation_link_tests {
    use super::*;

    #[test]
    fn link_only_unowned_or_own_installations() {
        assert!(may_link_installation(None, "TEAM#a"));
        assert!(may_link_installation(Some("TEAM#a"), "TEAM#a"));
        assert!(!may_link_installation(Some("TEAM#b"), "TEAM#a"));
    }

    #[test]
    fn invite_url_names_the_org_safely() {
        assert_eq!(
            ask_for_invite_url("https://app.example.com", "acme co"),
            "https://app.example.com?error=ask_admin_invite&org=acme%20co"
        );
    }
}

#[cfg(test)]
mod github_signin_update_tests {
    use super::*;

    fn profile<'a>(email: &'a str) -> GithubProfile<'a> {
        GithubProfile {
            id: 4242,
            login: "sam-dev",
            email,
            avatar_url: "https://avatars.example.com/u/4242",
            name: "Sam Dev",
        }
    }

    fn sets(u: &RecordUpdate) -> Vec<String> {
        u.expression
            .trim_start_matches("SET ")
            .split(", ")
            .map(|s| s.split(" = ").next().unwrap().to_string())
            .collect()
    }

    fn existing(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), attr_s(v)))
            .collect()
    }

    #[test]
    fn linked_member_keeps_email_status_name_role_and_invited_by() {
        let rec = existing(&[
            ("email", "sam@example.com"),
            ("status", "active"),
            ("name", "Sam"),
            ("role", "admin"),
            ("invited_by", "USER#someone"),
            ("gsi2pk", "EMAIL#sam@example.com"),
        ]);
        let u = github_signin_update(Some(&rec), &profile(""), "TEAM#t1", "member", "now");
        let fields = sets(&u);
        for kept in ["email", "gsi2pk", "gsi2sk", "#n", "#r", "#st", "invited_by"] {
            assert!(
                !fields.contains(&kept.to_string()),
                "{kept} must not be written: {fields:?}"
            );
        }
        for set in [
            "github_id",
            "github_login",
            "gsi1pk",
            "gsi1sk",
            "avatar_url",
            "updated_at",
            "last_login_at",
        ] {
            assert!(
                fields.contains(&set.to_string()),
                "{set} missing: {fields:?}"
            );
        }
        assert_eq!(u.values[":g1pk"], attr_s("GHUSER#4242"));
        assert_eq!(u.values[":gid"], AttributeValue::N("4242".into()));
        assert!(u.names.is_none());

        // Even when GitHub has a (different) email, the member's stays.
        let u = github_signin_update(
            Some(&rec),
            &profile("other@example.com"),
            "TEAM#t1",
            "member",
            "now",
        );
        assert!(!sets(&u).contains(&"email".to_string()));
    }

    #[test]
    fn github_only_record_gains_email_when_github_shares_one() {
        let rec = existing(&[("email", ""), ("github_login", "sam-dev")]);
        let u = github_signin_update(
            Some(&rec),
            &profile("Sam+x@Example.com"),
            "TEAM#t1",
            "member",
            "now",
        );
        let fields = sets(&u);
        assert!(fields.contains(&"email".to_string()));
        assert_eq!(u.values[":g2pk"], attr_s("EMAIL#sam@example.com"));
        assert!(
            fields.contains(&"#n".to_string()),
            "name filled when missing"
        );
        assert!(
            !fields.contains(&"#r".to_string()),
            "role untouched on existing"
        );
    }

    #[test]
    fn empty_github_email_never_writes_an_empty_email_index_key() {
        let u = github_signin_update(None, &profile(""), "TEAM#t1", "owner", "now");
        let fields = sets(&u);
        assert!(!fields.contains(&"gsi2pk".to_string()));
        assert!(!fields.contains(&"email".to_string()));
        // A new record gets its role and status.
        assert!(fields.contains(&"#r".to_string()) && fields.contains(&"#st".to_string()));
        assert_eq!(u.values[":role"], attr_s("owner"));
        assert_eq!(u.values[":active"], attr_s("active"));
    }

    #[test]
    fn expression_placeholders_all_have_values() {
        for (rec, email) in [
            (None, "a@example.com"),
            (Some(existing(&[("email", "")])), ""),
        ] {
            let u = github_signin_update(rec.as_ref(), &profile(email), "TEAM#t1", "member", "now");
            for token in u
                .expression
                .split(|c: char| !(c.is_alphanumeric() || c == ':' || c == '#' || c == '_'))
            {
                if token.starts_with(':') {
                    assert!(u.values.contains_key(token), "{token} has no value");
                }
                if token.starts_with('#') {
                    assert!(
                        u.names.as_ref().is_some_and(|n| n.contains_key(token)),
                        "{token} has no name"
                    );
                }
            }
            for key in u.values.keys() {
                assert!(
                    u.expression.contains(key.as_str()),
                    "{key} unused (DynamoDB rejects it)"
                );
            }
        }
    }
}

#[cfg(test)]
mod first_sign_in_tests {
    use super::*;

    fn rec(
        pk: &str,
        sk: &str,
        role: &str,
        status: Option<&str>,
        sent: Option<&str>,
    ) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::from([
            ("pk".to_string(), attr_s(pk)),
            ("sk".to_string(), attr_s(sk)),
            ("role".to_string(), attr_s(role)),
        ]);
        if let Some(s) = status {
            m.insert("status".into(), attr_s(s));
        }
        if let Some(t) = sent {
            m.insert("updated_at".into(), attr_s(t));
        }
        m
    }

    fn at(t: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(t).unwrap().into()
    }

    #[test]
    fn an_invited_email_gets_no_personal_team() {
        let now = at("2026-10-22T00:00:00Z");
        // Nobody knows this email: a personal team, as always.
        assert!(needs_personal_team(&[], now));
        // A live invite: they land on the inviting team instead.
        let invite = rec(
            "TEAM#co",
            "USER#invite_1",
            "member",
            Some("invited"),
            Some("2026-10-20T00:00:00Z"),
        );
        assert!(!needs_personal_team(&[invite], now));
        // Already a member somewhere (e.g. linked through GitHub).
        let member = rec("TEAM#co", "USER#123", "member", None, None);
        assert!(!needs_personal_team(&[member], now));
    }

    #[test]
    fn an_expired_invite_still_gets_a_personal_team() {
        let now = at("2026-10-22T00:00:00Z");
        let old = rec(
            "TEAM#co",
            "USER#invite_1",
            "member",
            Some("invited"),
            Some("2026-07-01T00:00:00Z"),
        );
        assert!(needs_personal_team(&[old], now));
    }

    #[test]
    fn a_first_sign_in_lands_on_the_team_whose_invite_it_accepted() {
        let sub = "abc";
        let invite = rec("TEAM#co", "USER#invite_1", "member", Some("invited"), None);
        // Before: only the invite, so nothing to land on and a personal team was made.
        assert!(
            select_session_team(&landing_records(std::slice::from_ref(&invite), vec![]), sub)
                .is_none()
        );
        let accepted = rec("TEAM#co", "USER#abc", "member", Some("active"), None);
        let pool = landing_records(&[invite], vec![accepted]);
        assert_eq!(pool.len(), 1, "the invite itself is never a landing record");
        let chosen = select_session_team(&pool, sub).unwrap();
        assert_eq!(
            chosen.get("pk").and_then(|v| v.as_s().ok()).unwrap(),
            "TEAM#co"
        );
    }

    #[test]
    fn waitlist_records_keep_what_was_typed_and_cut_long_text() {
        let body = WaitlistRequest {
            email: "Dee+beta@Beta.test".into(),
            reason: Some(format!("  {}  ", "x".repeat(600))),
            company: Some("  Beta Co  ".into()),
        };
        let item = waitlist_item(
            "dee@beta.test",
            "dee+beta@beta.test",
            &body,
            "2026-10-22T00:00:00Z",
        );
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        assert_eq!(s("email"), "dee@beta.test");
        assert_eq!(s("original_email"), "dee+beta@beta.test");
        assert_eq!(s("company"), "Beta Co");
        assert_eq!(s("reason").chars().count(), 500);
        assert_eq!(s("status"), "new");
        assert_eq!(s("created_at"), "2026-10-22T00:00:00Z");

        let bare = WaitlistRequest {
            email: "a@b.test".into(),
            reason: None,
            company: Some("   ".into()),
        };
        let item = waitlist_item("a@b.test", "a@b.test", &bare, "t");
        assert!(!item.contains_key("reason") && !item.contains_key("company"));
    }
}
