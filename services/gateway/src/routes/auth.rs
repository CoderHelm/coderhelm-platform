use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::jwt;
use crate::AppState;

/// Compute Cognito SECRET_HASH = Base64(HMAC-SHA256(client_secret, username + client_id))
fn cognito_secret_hash(client_secret: &str, username: &str, client_id: &str) -> String {
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
    #[allow(dead_code)]
    state: Option<String>,
}

#[derive(Deserialize)]
pub struct GoogleCallbackParams {
    code: String,
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

/// GET /auth/google — Redirect to Cognito hosted UI for Google login.
pub async fn google_login(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let redirect_uri = if state.config.stage == "prod" {
        "https://api.coderhelm.com/auth/google/callback"
    } else {
        "http://localhost:3001/auth/google/callback"
    };

    let domain = &state.config.cognito_domain;
    let client_id = &state.config.cognito_client_id;

    // Custom domains (contain '.') use https://{domain}/oauth2/...
    // Cognito prefix domains use https://{domain}.auth.{region}.amazoncognito.com/oauth2/...
    let base = if domain.contains('.') {
        format!("https://{domain}")
    } else {
        format!("https://{domain}.auth.us-east-1.amazoncognito.com")
    };

    let url = format!(
        "{base}/oauth2/authorize?\
         response_type=code&client_id={client_id}&\
         redirect_uri={redirect_uri}&\
         identity_provider=Google&\
         scope=openid+email+profile+aws.cognito.signin.user.admin"
    );

    Redirect::temporary(&url)
}

/// GET /auth/google/callback — Handle Cognito/Google OAuth callback.
pub async fn google_callback(
    State(state): State<Arc<AppState>>,
    Query(params): Query<GoogleCallbackParams>,
) -> Result<Response, StatusCode> {
    let redirect_uri = if state.config.stage == "prod" {
        "https://api.coderhelm.com/auth/google/callback"
    } else {
        "http://localhost:3001/auth/google/callback"
    };

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
            ("redirect_uri", redirect_uri),
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
        .limit(1)
        .send()
        .await
        .ok()
        .and_then(|r| r.items)
        .unwrap_or_default();

    // Closed beta: only allow new users on the allowlist (existing users can always log in)
    if existing.is_empty() && !is_email_allowed(&state, &email).await {
        let dash = if state.config.stage == "prod" {
            "https://app.coderhelm.com"
        } else {
            "http://localhost:3000"
        };
        let redirect = format!("{dash}?error=closed_beta");
        return Ok(Redirect::temporary(&redirect).into_response());
    }

    let (team_id, user_id, role) = if let Some(item) = existing.first() {
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
        (tid, uid, r)
    } else {
        // New user — create personal team
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
        86400,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

// ── GitHub OAuth (connect GitHub, not identity) ──────────────────────

/// GET /auth/github — Redirect to GitHub OAuth (for connecting GitHub account).
pub async fn github_login(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client_id = &state.secrets.github_client_id;
    let redirect_uri = if state.config.stage == "prod" {
        "https://api.coderhelm.com/auth/github/callback"
    } else {
        "http://localhost:3000/auth/github/callback"
    };
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope=read:user,read:org"
    );
    Redirect::temporary(&url)
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

    let existing_claims = extract_cookie(cookie_header, "coderhelm_session")
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

        // Link GitHub installations to the user's team in the teams table
        for (inst_id, org) in &all_installations {
            let _ = state
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
        }

        // Re-issue JWT with github_login (keep the user's existing team)
        let token = jwt::create_token(
            &claims.sub,
            &claims.team_id,
            &claims.email,
            &claims.role,
            Some(github_login),
            &state.secrets.jwt_secret,
            86400,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        info!(github_login, "GitHub connected to existing account");

        let dashboard_url = if state.config.stage == "prod" {
            "https://app.coderhelm.com"
        } else {
            "http://localhost:3000"
        };

        let cookie = format!(
            "coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=86400"
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
            .limit(5)
            .send()
            .await
            .ok()
            .and_then(|r| {
                r.items()
                    .iter()
                    .find(|item| {
                        item.get("sk")
                            .and_then(|v| v.as_s().ok())
                            .map(|s| s.starts_with("USER#"))
                            .unwrap_or(false)
                            && item
                                .get("status")
                                .and_then(|v| v.as_s().ok())
                                .map(|s| s != "invited")
                                .unwrap_or(true)
                    })
                    .cloned()
            })
    } else {
        None
    };

    let existing_user = existing_by_github.or(existing_by_email);

    let has_existing_record = existing_user.is_some();

    // Closed beta: check allowlist for new GitHub users
    if !has_existing_record && !email.is_empty() && !is_email_allowed(&state, email).await {
        let dash = if state.config.stage == "prod" {
            "https://app.coderhelm.com"
        } else {
            "http://localhost:3000"
        };
        let redirect = format!("{dash}?error=closed_beta");
        return Ok(Redirect::temporary(&redirect).into_response());
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

    // Upsert user record
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .item("pk", attr_s(&team_id))
        .item("sk", attr_s(&user_id))
        .item("github_id", attr_n(github_id))
        .item("github_login", attr_s(github_login))
        .item("email", attr_s(email))
        .item(
            "avatar_url",
            attr_s(user["avatar_url"].as_str().unwrap_or("")),
        )
        .item("role", attr_s(&role))
        .item("updated_at", attr_s(&now))
        .item("gsi1pk", attr_s(&format!("GHUSER#{github_id}")))
        .item("gsi1sk", attr_s(&team_id))
        .item("gsi2pk", attr_s(&format!("EMAIL#{normalized_email}")))
        .item("gsi2sk", attr_s(&team_id))
        .send()
        .await;

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
    }

    // Link GitHub installations to the team in the teams table
    if !all_installations.is_empty() {
        let (inst_id, ref org) = all_installations[0];
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.teams_table_name)
            .key("team_id", attr_s(&team_id))
            .key("sk", attr_s("META"))
            .update_expression(
                "SET github_installation_id = :iid, github_org = :org, updated_at = :now",
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
            github_login,
            installation_id = inst_id,
            org = org.as_str(),
            team_id = %team_id,
            "Linked GitHub installation to team"
        );

        // Sync repos from the installation
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
        86400,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

// ── Logout ───────────────────────────────────────────────────────────

/// POST /auth/logout — Clear session cookie.
pub async fn logout() -> impl IntoResponse {
    let cookie = "coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
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

    // Find non-invite record (the user's own team) to use for session
    let active_item = items.iter().find(|item| {
        item.get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s != "invited")
            .unwrap_or(true)
    });

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

    // Process any pending invites — create real user records in the inviting teams
    let real_user_id = format!("USER#{cognito_sub}");
    let now = chrono::Utc::now().to_rfc3339();
    for item in &items {
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

        // Create a real user record in the inviting team
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .item("pk", attr_s(&invite_tid))
            .item("sk", attr_s(&real_user_id))
            .item("email", attr_s(email))
            .item("role", attr_s(&invite_role))
            .item("avatar_url", attr_s(""))
            .item("updated_at", attr_s(&now))
            .item(
                "gsi2pk",
                attr_s(&format!("EMAIL#{}", normalize_email(email))),
            )
            .item("gsi2sk", attr_s(&invite_tid))
            .send()
            .await;

        // Delete the invite record
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
    }

    let token = jwt::create_token(
        &user_id,
        &team_id,
        email,
        &role,
        github_login.as_deref(),
        &state.secrets.jwt_secret,
        86400,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cookie = format!(
        "coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=86400"
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

fn extract_cookie<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header
        .split(';')
        .map(|c| c.trim())
        .find(|c| c.starts_with(&format!("{name}=")))
        .and_then(|c| c.split_once('='))
        .map(|(_, v)| v)
}

/// Normalize an email: lowercase + strip plus-alias (user+tag@domain.com → user@domain.com)
fn normalize_email(email: &str) -> String {
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
    let email_re =
        regex::Regex::new(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$").unwrap();
    if !email_re.is_match(&email) {
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
    let reason = body
        .reason
        .as_deref()
        .unwrap_or("")
        .trim()
        .chars()
        .take(500)
        .collect::<String>();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.waitlist_table_name)
        .item("email", attr_s(&normalized))
        .item("original_email", attr_s(&email))
        .item("created_at", attr_s(&now));

    if !reason.is_empty() {
        put = put.item("reason", attr_s(&reason));
    }

    put.send().await.map_err(|e| {
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
