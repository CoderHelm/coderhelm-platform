use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

// ── Request bodies ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct InviteRequest {
    pub email: String,
    pub role: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateRoleRequest {
    pub role: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

#[derive(Deserialize)]
pub struct EnableMfaRequest {
    pub access_token: String,
}

#[derive(Deserialize)]
pub struct VerifyMfaRequest {
    pub access_token: String,
    pub code: String,
    pub session: String,
}

// ── Valid roles ──────────────────────────────────────────────────────

const VALID_ROLES: &[&str] = &["owner", "admin", "billing", "member", "viewer"];

fn is_admin_or_owner(role: &str) -> bool {
    role == "owner" || role == "admin"
}

// ── List users ───────────────────────────────────────────────────────

/// GET /api/users — List all users in the tenant.
pub async fn list_users(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("USER#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list users: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let users: Vec<Value> = result
        .items()
        .iter()
        .map(|item| {
            json!({
                "user_id": item.get("sk").and_then(|v| v.as_s().ok()),
                "email": item.get("email").and_then(|v| v.as_s().ok()),
                "github_login": item.get("github_login").and_then(|v| v.as_s().ok()),
                "avatar_url": item.get("avatar_url").and_then(|v| v.as_s().ok()),
                "role": item.get("role").and_then(|v| v.as_s().ok()).unwrap_or(&"member".to_string()),
                "name": item.get("name").and_then(|v| v.as_s().ok()),
                "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({ "users": users })))
}

// ── Invite user ──────────────────────────────────────────────────────

/// POST /api/users/invite — Invite a user by email.
pub async fn invite_user(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<InviteRequest>,
) -> Result<Json<Value>, StatusCode> {
    // Only admin/owner can invite
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    let email = body.email.trim().to_lowercase();
    if email.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let role = body.role.as_deref().unwrap_or("member");
    if !VALID_ROLES.contains(&role) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Don't allow inviting as owner
    if role == "owner" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let user_id = format!("USER#invite_{}", ulid::Ulid::new());
    let now = chrono::Utc::now().to_rfc3339();

    // Create user record with "invited" status
    state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s(&user_id))
        .item("email", attr_s(&email))
        .item("role", attr_s(role))
        .item("status", attr_s("invited"))
        .item("avatar_url", attr_s(""))
        .item("updated_at", attr_s(&now))
        .item("invited_by", attr_s(&claims.sub))
        .item("gsi2pk", attr_s(&format!("EMAIL#{email}")))
        .item("gsi2sk", attr_s(&claims.tenant_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create invite: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Send invitation email via SES
    let tenant_name = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| {
            i.get("github_org")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| {
            claims
                .email
                .split('@')
                .nth(1)
                .map(|d| d.split('.').next().unwrap_or("your team"))
                .unwrap_or("your team")
                .to_string()
        });

    let _ = state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(
            aws_sdk_sesv2::types::Destination::builder()
                .to_addresses(&email)
                .build(),
        )
        .content(
            aws_sdk_sesv2::types::EmailContent::builder()
                .simple(
                    aws_sdk_sesv2::types::Message::builder()
                        .subject(
                            aws_sdk_sesv2::types::Content::builder()
                                .data(format!(
                                    "You've been invited to {} on Coderhelm",
                                    tenant_name
                                ))
                                .build()
                                .unwrap(),
                        )
                        .body(
                            aws_sdk_sesv2::types::Body::builder()
                                .text(
                                    aws_sdk_sesv2::types::Content::builder()
                                        .data(format!(
                                            "You've been invited to join {} on Coderhelm.\n\n\
                                             Sign up or log in at https://app.coderhelm.com to get started.",
                                            tenant_name
                                        ))
                                        .build()
                                        .unwrap(),
                                )
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await;

    info!(email = %email, role = %role, tenant_id = %claims.tenant_id, "User invited");

    Ok(Json(json!({
        "status": "invited",
        "user_id": user_id,
    })))
}

// ── Update role ──────────────────────────────────────────────────────

/// PUT /api/users/:user_id/role — Change a user's role.
pub async fn update_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
    Json(body): Json<UpdateRoleRequest>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    if !VALID_ROLES.contains(&body.role.as_str()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Prevent changing own role
    if user_id == claims.sub {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Only owners can set admin/owner roles
    if (body.role == "admin" || body.role == "owner") && claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let sk = if user_id.starts_with("USER#") {
        user_id.clone()
    } else {
        format!("USER#{user_id}")
    };

    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #r = :role, updated_at = :now")
        .expression_attribute_names("#r", "role")
        .expression_attribute_values(":role", attr_s(&body.role))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update role: {e}");
            StatusCode::NOT_FOUND
        })?;

    info!(user_id = %sk, new_role = %body.role, tenant_id = %claims.tenant_id, "Role updated");

    Ok(Json(json!({ "status": "updated" })))
}

// ── Remove user ──────────────────────────────────────────────────────

/// DELETE /api/users/:user_id — Remove a user from the tenant.
pub async fn remove_user(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Prevent removing self
    if user_id == claims.sub {
        return Err(StatusCode::BAD_REQUEST);
    }

    let sk = if user_id.starts_with("USER#") {
        user_id.clone()
    } else {
        format!("USER#{user_id}")
    };

    // Check the target user isn't owner (can't remove owner)
    let target = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch target user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(item) = target.item() {
        let target_role = item
            .get("role")
            .and_then(|v| v.as_s().ok())
            .map_or("member", |v| v);
        if target_role == "owner" {
            return Err(StatusCode::FORBIDDEN);
        }
    } else {
        return Err(StatusCode::NOT_FOUND);
    }

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to remove user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(user_id = %sk, tenant_id = %claims.tenant_id, "User removed");

    Ok(Json(json!({ "status": "removed" })))
}

// ── Change password ──────────────────────────────────────────────────

/// PUT /api/users/password — Change own password (Cognito).
pub async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ChangePasswordRequest>,
) -> Result<Json<Value>, StatusCode> {
    // First authenticate with current password to get access token
    let auth_result = state
        .cognito
        .initiate_auth()
        .client_id(&state.config.cognito_client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &claims.email)
        .auth_parameters("PASSWORD", &body.current_password)
        .send()
        .await
        .map_err(|e| {
            error!("Password verification failed: {e}");
            StatusCode::UNAUTHORIZED
        })?;

    let access_token = auth_result
        .authentication_result()
        .and_then(|r| r.access_token())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Change password
    state
        .cognito
        .change_password()
        .access_token(access_token)
        .previous_password(&body.current_password)
        .proposed_password(&body.new_password)
        .send()
        .await
        .map_err(|e| {
            error!("Password change failed: {e}");
            StatusCode::BAD_REQUEST
        })?;

    info!(email = %claims.email, "Password changed");

    Ok(Json(json!({ "status": "changed" })))
}

// ── MFA management ───────────────────────────────────────────────────

/// POST /api/users/mfa/setup — Start TOTP MFA setup. Returns secret + QR URI.
pub async fn mfa_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<EnableMfaRequest>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .cognito
        .associate_software_token()
        .access_token(&body.access_token)
        .send()
        .await
        .map_err(|e| {
            error!("MFA setup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let secret = result.secret_code().unwrap_or_default();
    let qr_uri = format!(
        "otpauth://totp/Coderhelm:{}?secret={}&issuer=Coderhelm",
        claims.email, secret
    );

    Ok(Json(json!({
        "secret": secret,
        "qr_uri": qr_uri,
        "session": result.session(),
    })))
}

/// POST /api/users/mfa/verify — Verify TOTP code and enable MFA.
pub async fn mfa_verify_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<VerifyMfaRequest>,
) -> Result<Json<Value>, StatusCode> {
    // Verify the TOTP token
    let mut req = state
        .cognito
        .verify_software_token()
        .access_token(&body.access_token)
        .user_code(&body.code);

    if !body.session.is_empty() {
        req = req.session(&body.session);
    }

    req.send().await.map_err(|e| {
        error!("MFA verification failed: {e}");
        StatusCode::BAD_REQUEST
    })?;

    // Enable MFA for the user
    state
        .cognito
        .admin_set_user_mfa_preference()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .username(&claims.email)
        .software_token_mfa_settings(
            aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaSettingsType::builder()
                .enabled(true)
                .preferred_mfa(true)
                .build(),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enable MFA: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %claims.email, "MFA enabled");

    Ok(Json(json!({ "status": "enabled" })))
}

/// DELETE /api/users/mfa — Disable TOTP MFA.
pub async fn mfa_disable(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) && claims.email.is_empty() {
        return Err(StatusCode::FORBIDDEN);
    }

    state
        .cognito
        .admin_set_user_mfa_preference()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .username(&claims.email)
        .software_token_mfa_settings(
            aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaSettingsType::builder()
                .enabled(false)
                .preferred_mfa(false)
                .build(),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to disable MFA: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %claims.email, "MFA disabled");

    Ok(Json(json!({ "status": "disabled" })))
}

// ── Helpers ──────────────────────────────────────────────────────────

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}
