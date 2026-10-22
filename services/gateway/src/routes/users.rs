use aws_sdk_dynamodb::types::{AttributeValue, Delete, TransactWriteItem, Update};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::permissions::{self, can_grant, can_manage, role_level, VALID_ROLES};
use crate::routes::auth::{cognito_secret_hash, normalize_email};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

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
pub struct LinkGithubRequest {
    /// The GitHub-only member record to fold in (its `user_id` from the list).
    pub github_user_id: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

#[derive(Deserialize)]
pub struct EnableMfaRequest {
    pub password: String,
}

#[derive(Deserialize)]
pub struct VerifyMfaRequest {
    pub password: String,
    pub code: String,
    #[serde(default)]
    pub session: Option<String>,
}

// ── Record helpers ──────────────────────────────────────────────────

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map_or("", |v| v.as_str())
}

/// A user id without the `USER#` prefix (path params and JWT subs differ).
fn bare_id(id: &str) -> &str {
    id.strip_prefix("USER#").unwrap_or(id)
}

fn user_sk(id: &str) -> String {
    format!("USER#{}", bare_id(id))
}

fn role_of(item: &Item) -> &str {
    match s(item, "role") {
        "" => "member",
        r => r,
    }
}

fn status_of(item: &Item) -> &'static str {
    if s(item, "status") == "invited" {
        "invited"
    } else {
        "active"
    }
}

fn github_id_of(item: &Item) -> Option<String> {
    match item.get("github_id") {
        Some(AttributeValue::N(n)) => Some(n.clone()),
        Some(AttributeValue::S(v)) if !v.is_empty() => Some(v.clone()),
        _ => None,
    }
}

/// How this person signs in, from what their record holds.
fn sign_in_of(item: &Item) -> &'static str {
    let has_email = !s(item, "email").is_empty();
    let has_github = !s(item, "github_login").is_empty() || github_id_of(item).is_some();
    match (has_email, has_github) {
        (true, true) => "email_and_github",
        (false, true) => "github",
        _ => "email",
    }
}

fn opt(v: &str) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        Value::String(v.to_string())
    }
}

/// When the member last signed in or used the app, whichever is later
/// (both are UTC RFC 3339, so they compare as text).
fn last_active(item: &Item) -> &str {
    let login = s(item, "last_login_at");
    let seen = s(item, "last_seen_at");
    if seen > login {
        seen
    } else {
        login
    }
}

/// How long an invite stays good after it was last sent. Resending starts the
/// clock again. An expired invite is not accepted at sign-in, but it stays on
/// the team page (marked expired) until an admin resends or revokes it.
pub(crate) const INVITE_VALID_DAYS: i64 = 30;

fn rfc3339(v: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(v)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

/// When the invite was last sent: `invited_at`, or `updated_at` on invites
/// written before `invited_at` existed.
fn invite_sent_at(item: &Item) -> Option<chrono::DateTime<chrono::Utc>> {
    rfc3339(s(item, "invited_at")).or_else(|| rfc3339(s(item, "updated_at")))
}

/// When a pending invite stops working. `None` for members, and for an invite
/// with no readable date (it is treated as still open).
pub(crate) fn invite_expires_at(item: &Item) -> Option<chrono::DateTime<chrono::Utc>> {
    if status_of(item) != "invited" {
        return None;
    }
    invite_sent_at(item).map(|t| t + chrono::Duration::days(INVITE_VALID_DAYS))
}

pub(crate) fn invite_expired(item: &Item, now: chrono::DateTime<chrono::Utc>) -> bool {
    invite_expires_at(item).is_some_and(|t| t <= now)
}

/// What the team page shows for one record.
fn member_view(item: &Item, self_id: &str, now: chrono::DateTime<chrono::Utc>) -> Value {
    let user_id = bare_id(s(item, "sk"));
    let invited = status_of(item) == "invited";
    let stamp = |t: Option<chrono::DateTime<chrono::Utc>>| {
        t.map_or(Value::Null, |t| {
            Value::String(t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        })
    };
    json!({
        "user_id": user_id,
        "email": opt(s(item, "email")),
        "name": opt(s(item, "name")),
        "github_login": opt(s(item, "github_login")),
        "github_id": github_id_of(item),
        "avatar_url": opt(s(item, "avatar_url")),
        "role": role_of(item),
        "status": status_of(item),
        "sign_in": sign_in_of(item),
        "invited_by": opt(bare_id(s(item, "invited_by"))),
        "last_active_at": opt(last_active(item)),
        "updated_at": opt(s(item, "updated_at")),
        "invited_at": stamp(if invited { invite_sent_at(item) } else { None }),
        "invite_expires_at": stamp(invite_expires_at(item)),
        "invite_expired": invite_expired(item, now),
        "is_self": !user_id.is_empty() && user_id == bare_id(self_id),
    })
}

fn display_key(v: &Value) -> String {
    ["name", "email", "github_login", "user_id"]
        .iter()
        .find_map(|k| v[*k].as_str().filter(|x| !x.is_empty()))
        .unwrap_or("")
        .to_lowercase()
}

/// Active people first (owners, then admins, members, viewers), then pending
/// invites; alphabetical by display name inside each group; id breaks ties.
fn sort_members(members: &mut [Value]) {
    members.sort_by(|a, b| {
        let key = |v: &Value| {
            (
                v["status"] == "invited",
                std::cmp::Reverse(role_level(v["role"].as_str().unwrap_or(""))),
                display_key(v),
                v["user_id"].as_str().unwrap_or("").to_string(),
            )
        };
        key(a).cmp(&key(b))
    });
}

fn active_owner_count(items: &[Item]) -> usize {
    items
        .iter()
        .filter(|i| role_of(i) == "owner" && status_of(i) == "active")
        .count()
}

/// Role-change rules beyond the route's admin check.
fn check_role_change(
    claims: &Claims,
    target_id: &str,
    target_role: &str,
    new_role: &str,
    owners: usize,
) -> Result<(), ApiError> {
    if !VALID_ROLES.contains(&new_role) {
        return Err(fail(StatusCode::BAD_REQUEST, "That isn't a role."));
    }
    if bare_id(target_id) == bare_id(&claims.sub) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You can't change your own role. Ask another owner.",
        ));
    }
    if !can_manage(&claims.role, target_role) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only an owner can change an admin's or owner's role.",
        ));
    }
    if !can_grant(&claims.role, new_role) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only an owner can make someone an admin or owner.",
        ));
    }
    if target_role == "owner" && new_role != "owner" && owners <= 1 {
        return Err(fail(
            StatusCode::CONFLICT,
            "This is the team's last owner. Make someone else an owner first.",
        ));
    }
    Ok(())
}

fn check_removal(
    claims: &Claims,
    target_id: &str,
    target: &Item,
    owners: usize,
) -> Result<(), ApiError> {
    if bare_id(target_id) == bare_id(&claims.sub) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You can't remove yourself from the team.",
        ));
    }
    // A pending invite gives nobody access yet, so revoking one takes nothing
    // away from anyone: any admin may revoke any invite, whatever role it
    // offers and however old it is. The route has already checked for admin.
    if status_of(target) == "invited" {
        return Ok(());
    }
    let target_role = role_of(target);
    if !can_manage(&claims.role, target_role) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only an owner can remove an admin or owner.",
        ));
    }
    if target_role == "owner" && status_of(target) == "active" && owners <= 1 {
        return Err(fail(
            StatusCode::CONFLICT,
            "This is the team's last owner and can't be removed.",
        ));
    }
    Ok(())
}

/// What linking writes, decided from the two records alone.
#[derive(Debug, PartialEq)]
struct LinkPlan {
    github_id: String,
    github_login: String,
    /// Set only when the email record has no avatar of its own.
    avatar_url: Option<String>,
    /// The higher of the two roles: both records are the same person.
    role: String,
}

fn plan_link(claims: &Claims, person: &Item, github: &Item) -> Result<LinkPlan, ApiError> {
    if s(person, "sk") == s(github, "sk") {
        return Err(fail(StatusCode::BAD_REQUEST, "Pick two different records."));
    }
    if status_of(person) != "active" {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Link to someone who has joined, not a pending invite.",
        ));
    }
    if s(person, "email").is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Link the GitHub account to a member who signs in with email.",
        ));
    }
    let Some(github_id) = github_id_of(github) else {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That record has no GitHub account to link.",
        ));
    };
    let github_login = s(github, "github_login");
    if github_login.is_empty() || !s(github, "email").is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Only a GitHub sign-in-only record can be linked into someone.",
        ));
    }
    match github_id_of(person) {
        Some(existing) if existing != github_id => {
            return Err(fail(
                StatusCode::CONFLICT,
                "This member already has a different GitHub account linked.",
            ));
        }
        _ => {}
    }
    for role in [role_of(person), role_of(github)] {
        if !can_manage(&claims.role, role) {
            return Err(fail(
                StatusCode::FORBIDDEN,
                "Only an owner can link records of admins or owners.",
            ));
        }
    }
    let role = if role_level(role_of(github)) > role_level(role_of(person)) {
        role_of(github)
    } else {
        role_of(person)
    };
    let avatar = s(github, "avatar_url");
    Ok(LinkPlan {
        github_id,
        github_login: github_login.to_string(),
        avatar_url: (s(person, "avatar_url").is_empty() && !avatar.is_empty())
            .then(|| avatar.to_string()),
        role: role.to_string(),
    })
}

/// Basic shape check; the invite email itself is the real test.
fn valid_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains('@')
        && !email.chars().any(char::is_whitespace)
        && email.len() <= 254
}

async fn team_items(state: &AppState, team_id: &str) -> Result<Vec<Item>, ApiError> {
    let mut items = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("USER#"))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| internal("Failed to list users", e))?;
        items.extend(out.items().iter().cloned());
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(items)
}

async fn get_member(state: &AppState, team_id: &str, id: &str) -> Result<Item, ApiError> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&user_sk(id)))
        .send()
        .await
        .map_err(|e| internal("Failed to fetch user", e))?
        .item
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That person isn't on this team."))
}

fn require_admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|_| fail(StatusCode::FORBIDDEN, "Only admins and owners can do that."))
}

// ── Permissions ──────────────────────────────────────────────────────

/// GET /api/users/permissions — what each role can do, and what you may grant.
pub async fn get_permissions(Extension(claims): Extension<Claims>) -> Json<Value> {
    Json(json!({
        "roles": permissions::ROLES,
        "capabilities": permissions::CAPABILITIES,
        "your_role": claims.role,
        "grantable_roles": permissions::grantable_roles(&claims.role),
        "can_manage_invites": permissions::can_manage_invites(&claims.role),
        "manageable_roles": permissions::ROLES
            .iter()
            .filter(|r| can_manage(&claims.role, r.key))
            .map(|r| r.key)
            .collect::<Vec<_>>(),
    }))
}

// ── List users ───────────────────────────────────────────────────────

/// GET /api/users — everyone on the team. Pending invites only for admins.
pub async fn list_users(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    let items = team_items(&state, &claims.team_id).await?;
    let show_invites = permissions::can_manage_invites(&claims.role);
    let now = chrono::Utc::now();
    let mut users: Vec<Value> = items
        .iter()
        .filter(|i| show_invites || status_of(i) != "invited")
        .map(|i| member_view(i, &claims.sub, now))
        .collect();
    sort_members(&mut users);
    Ok(Json(json!({ "users": users })))
}

// ── Invite user ──────────────────────────────────────────────────────

/// POST /api/users/invite — Invite a user by email.
pub async fn invite_user(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<InviteRequest>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;

    let email = body.email.trim().to_lowercase();
    if !valid_email(&email) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Enter a valid email address.",
        ));
    }
    let role = body.role.as_deref().unwrap_or("member");
    if role == "owner" {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Invite them as an admin, then make them an owner once they join.",
        ));
    }
    if !VALID_ROLES.contains(&role) {
        return Err(fail(StatusCode::BAD_REQUEST, "That isn't a role."));
    }
    if !can_grant(&claims.role, role) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only an owner can invite an admin.",
        ));
    }
    let gsi2pk = format!("EMAIL#{}", normalize_email(&email));

    // Already on this team (as a member or an open invite)?
    let existing = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi2")
        .key_condition_expression("gsi2pk = :pk AND gsi2sk = :team")
        .expression_attribute_values(":pk", attr_s(&gsi2pk))
        .expression_attribute_values(":team", attr_s(&claims.team_id))
        .send()
        .await
        .map_err(|e| internal("Failed to check existing membership", e))?;
    if let Some(item) = existing.items().first() {
        return Err(fail(
            StatusCode::CONFLICT,
            if status_of(item) == "invited" {
                "They already have an invite. Resend it from Pending invites."
            } else {
                "They're already on the team."
            },
        ));
    }

    // Check if this email already has a Cognito account
    let existing_sub = state
        .cognito
        .list_users()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .filter(format!("email = \"{email}\""))
        .limit(1)
        .send()
        .await
        .ok()
        .and_then(|r| {
            r.users().first().and_then(|u| {
                u.attributes()
                    .iter()
                    .find(|a| a.name() == "sub")
                    .and_then(|a| a.value().map(|v| v.to_string()))
            })
        });

    let now = chrono::Utc::now().to_rfc3339();
    let (sk, status) = match existing_sub {
        // They already have an account — add them directly to the team.
        Some(sub) => (format!("USER#{sub}"), "active"),
        None => (format!("USER#invite_{}", ulid::Ulid::new()), "invited"),
    };

    state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("email", attr_s(&email))
        .item("role", attr_s(role))
        .item("status", attr_s(status))
        .item("avatar_url", attr_s(""))
        .item("updated_at", attr_s(&now))
        .item("invited_at", attr_s(&now))
        .item("invited_by", attr_s(&claims.sub))
        .item("gsi2pk", attr_s(&gsi2pk))
        .item("gsi2sk", attr_s(&claims.team_id))
        // Never overwrite an existing member (their GitHub link, role, name).
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception())
            {
                fail(StatusCode::CONFLICT, "They're already on the team.")
            } else {
                internal("Failed to create invite", e)
            }
        })?;

    let team_name = get_team_name(state.clone(), &claims).await;
    let _ = send_invite_email(&state, &email, &claims.email, &team_name, role).await;

    info!(role = %role, status, team_id = %claims.team_id, "User invited");

    Ok(Json(json!({
        "status": status,
        "user_id": bare_id(&sk),
    })))
}

// ── Resend invite ────────────────────────────────────────────────────

/// POST /api/users/:user_id/resend — Resend an invite email.
pub async fn resend_invite(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let item = get_member(&state, &claims.team_id, &user_id).await?;
    let email = check_resend(&item)?;
    let role = role_of(&item);

    let team_name = get_team_name(state.clone(), &claims).await;
    send_invite_email(&state, email, &claims.email, &team_name, role)
        .await
        .map_err(|_| internal("Failed to resend invite", "email send failed"))?;

    // The email went out, so the invite is good again from now (this is also
    // how an expired invite is renewed). Only while it is still an invite: if
    // they signed in meanwhile, the record is theirs and is left alone.
    let now = chrono::Utc::now();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(s(&item, "sk")))
        .update_expression("SET invited_at = :now, updated_at = :now")
        .condition_expression("#st = :invited")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":now", attr_s(&now.to_rfc3339()))
        .expression_attribute_values(":invited", attr_s("invited"))
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception())
            {
                fail(
                    StatusCode::CONFLICT,
                    "They joined or the invite was revoked while you were looking. Refresh the page.",
                )
            } else {
                internal("Failed to renew invite", e)
            }
        })?;

    info!(team_id = %claims.team_id, "Invite resent");

    let expires = now + chrono::Duration::days(INVITE_VALID_DAYS);
    Ok(Json(json!({
        "status": "sent",
        "invited_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "invite_expires_at": expires.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    })))
}

/// What resending needs from the record: still an invite, with an email.
/// Its role and age don't matter; an expired invite is renewed by resending.
fn check_resend(item: &Item) -> Result<&str, ApiError> {
    if status_of(item) != "invited" {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "They've already joined; there's no invite to resend.",
        ));
    }
    match s(item, "email") {
        "" => Err(fail(StatusCode::BAD_REQUEST, "This invite has no email.")),
        email => Ok(email),
    }
}

// ── Update role ──────────────────────────────────────────────────────

/// PUT /api/users/:user_id/role — Change a user's role.
pub async fn update_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
    Json(body): Json<UpdateRoleRequest>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let items = team_items(&state, &claims.team_id).await?;
    let sk = user_sk(&user_id);
    let target = items
        .iter()
        .find(|i| s(i, "sk") == sk)
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That person isn't on this team."))?;
    let target_role = role_of(target).to_string();
    check_role_change(
        &claims,
        &user_id,
        &target_role,
        &body.role,
        active_owner_count(&items),
    )?;

    let (condition, was) = match s(target, "role") {
        "" => ("attribute_exists(pk) AND attribute_not_exists(#r)", None),
        r => ("attribute_exists(pk) AND #r = :was", Some(r)),
    };
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #r = :role, updated_at = :now")
        .expression_attribute_names("#r", "role")
        .expression_attribute_values(":role", attr_s(&body.role))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .condition_expression(condition);
    if let Some(was) = was {
        req = req.expression_attribute_values(":was", attr_s(was));
    }
    req.send().await.map_err(|e| {
        if e.as_service_error()
            .is_some_and(|se| se.is_conditional_check_failed_exception())
        {
            fail(
                StatusCode::CONFLICT,
                "Their role changed while you were looking. Refresh and try again.",
            )
        } else {
            internal("Failed to update role", e)
        }
    })?;

    info!(user_id = %sk, new_role = %body.role, team_id = %claims.team_id, "Role updated");

    Ok(Json(json!({ "status": "updated", "role": body.role })))
}

// ── Remove user ──────────────────────────────────────────────────────

/// DELETE /api/users/:user_id — Remove a user from the team (or revoke an invite).
pub async fn remove_user(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let items = team_items(&state, &claims.team_id).await?;
    let sk = user_sk(&user_id);
    let target = items
        .iter()
        .find(|i| s(i, "sk") == sk)
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That person isn't on this team."))?;
    check_removal(&claims, &user_id, target, active_owner_count(&items))?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| internal("Failed to remove user", e))?;

    info!(user_id = %sk, team_id = %claims.team_id, "User removed");

    Ok(Json(json!({ "status": "removed" })))
}

// ── Link GitHub account ──────────────────────────────────────────────

/// POST /api/users/:user_id/link-github — fold a GitHub-sign-in-only record
/// into this member's email record, so one person has one row and signs in
/// either way. The email record gains the GitHub id/login (and GSI1 keys); the
/// GitHub-only record is deleted. Both writes happen in one transaction.
pub async fn link_github(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
    Json(body): Json<LinkGithubRequest>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let person = get_member(&state, &claims.team_id, &user_id).await?;
    let github = get_member(&state, &claims.team_id, &body.github_user_id).await?;
    let plan = plan_link(&claims, &person, &github)?;
    let now = chrono::Utc::now().to_rfc3339();

    let mut set = "SET github_id = :gid, github_login = :gl, gsi1pk = :g1pk, gsi1sk = :team, \
                   #r = :role, updated_at = :now"
        .to_string();
    let mut values = HashMap::from([
        (
            ":gid".to_string(),
            AttributeValue::N(plan.github_id.clone()),
        ),
        (":gl".to_string(), attr_s(&plan.github_login)),
        (
            ":g1pk".to_string(),
            attr_s(&format!("GHUSER#{}", plan.github_id)),
        ),
        (":team".to_string(), attr_s(&claims.team_id)),
        (":role".to_string(), attr_s(&plan.role)),
        (":now".to_string(), attr_s(&now)),
    ]);
    if let Some(avatar) = &plan.avatar_url {
        set.push_str(", avatar_url = :avatar");
        values.insert(":avatar".to_string(), attr_s(avatar));
    }
    let gid_val = values[":gid"].clone();
    let update = Update::builder()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(s(&person, "sk")))
        .update_expression(set)
        .condition_expression(
            "attribute_exists(pk) AND (attribute_not_exists(github_id) OR github_id = :gid)",
        )
        .expression_attribute_names("#r", "role")
        .set_expression_attribute_values(Some(values))
        .build()
        .map_err(|e| internal("Failed to build link update", e))?;
    let delete = Delete::builder()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(s(&github, "sk")))
        .condition_expression("github_id = :gid")
        .expression_attribute_values(":gid", gid_val)
        .build()
        .map_err(|e| internal("Failed to build link delete", e))?;

    state
        .dynamo
        .transact_write_items()
        .transact_items(TransactWriteItem::builder().update(update).build())
        .transact_items(TransactWriteItem::builder().delete(delete).build())
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error()
                .is_some_and(|se| se.is_transaction_canceled_exception())
            {
                fail(
                    StatusCode::CONFLICT,
                    "One of the records changed while you were looking. Refresh and try again.",
                )
            } else {
                internal("Failed to link GitHub account", e)
            }
        })?;

    info!(
        team_id = %claims.team_id,
        person = %s(&person, "sk"),
        removed = %s(&github, "sk"),
        "Linked a GitHub sign-in record into a member"
    );

    Ok(Json(json!({
        "status": "linked",
        "user_id": bare_id(s(&person, "sk")),
        "github_login": plan.github_login,
        "role": plan.role,
    })))
}

// ── Change password ──────────────────────────────────────────────────

/// PUT /api/users/password — Change own password (Cognito).
pub async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ChangePasswordRequest>,
) -> Result<Json<Value>, StatusCode> {
    // Verify current password via initiate_auth (succeeds even if MFA challenge is returned)
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &claims.email,
        &state.config.cognito_client_id,
    );

    state
        .cognito
        .initiate_auth()
        .client_id(&state.config.cognito_client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &claims.email)
        .auth_parameters("PASSWORD", &body.current_password)
        .auth_parameters("SECRET_HASH", &hash)
        .send()
        .await
        .map_err(|e| {
            error!("Password verification failed: {e}");
            StatusCode::UNAUTHORIZED
        })?;

    // Use admin API to set the new password (works regardless of MFA status)
    state
        .cognito
        .admin_set_user_password()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .username(&claims.email)
        .password(&body.new_password)
        .permanent(true)
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
    // Authenticate to get a Cognito access token
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &claims.email,
        &state.config.cognito_client_id,
    );

    let auth_result = state
        .cognito
        .initiate_auth()
        .client_id(&state.config.cognito_client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &claims.email)
        .auth_parameters("PASSWORD", &body.password)
        .auth_parameters("SECRET_HASH", &hash)
        .send()
        .await
        .map_err(|e| {
            error!("MFA setup auth failed: {e:?}");
            StatusCode::UNAUTHORIZED
        })?;

    // If Cognito returns a challenge (e.g. SOFTWARE_TOKEN_MFA), user already has MFA enabled
    let access_token = match auth_result
        .authentication_result()
        .and_then(|r| r.access_token())
    {
        Some(token) => token.to_string(),
        None => {
            // User already has MFA enabled — return 409 Conflict
            info!(
                email = %claims.email,
                challenge = ?auth_result.challenge_name(),
                "MFA setup: user already has MFA enabled"
            );
            return Err(StatusCode::CONFLICT);
        }
    };

    let result = state
        .cognito
        .associate_software_token()
        .access_token(&access_token)
        .send()
        .await
        .map_err(|e| {
            error!("MFA setup failed: {e:?}");
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
    // Authenticate to get a Cognito access token
    let hash = cognito_secret_hash(
        &state.cognito_client_secret,
        &claims.email,
        &state.config.cognito_client_id,
    );

    let auth_result = state
        .cognito
        .initiate_auth()
        .client_id(&state.config.cognito_client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &claims.email)
        .auth_parameters("PASSWORD", &body.password)
        .auth_parameters("SECRET_HASH", &hash)
        .send()
        .await
        .map_err(|e| {
            error!("MFA verify auth failed: {e:?}");
            StatusCode::UNAUTHORIZED
        })?;

    let access_token = match auth_result
        .authentication_result()
        .and_then(|r| r.access_token())
    {
        Some(token) => token.to_string(),
        None => {
            info!(
                email = %claims.email,
                challenge = ?auth_result.challenge_name(),
                "MFA verify: user already has MFA, cannot re-setup"
            );
            return Err(StatusCode::CONFLICT);
        }
    };

    // Verify the TOTP token
    let mut req = state
        .cognito
        .verify_software_token()
        .access_token(&access_token)
        .user_code(&body.code);

    if let Some(ref session) = body.session {
        if !session.is_empty() {
            req = req.session(session);
        }
    }

    req.send().await.map_err(|e| {
        error!("MFA verification failed: {e:?}");
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
    if role_level(&claims.role) < 3 && claims.email.is_empty() {
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

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

async fn get_team_name(state: Arc<AppState>, claims: &Claims) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
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
        .unwrap_or_else(|| claims.email.clone())
}

async fn send_invite_email(
    state: &AppState,
    to: &str,
    inviter: &str,
    org: &str,
    role: &str,
) -> Result<(), StatusCode> {
    let template_name = format!("{}-team-invite", state.config.ses_template_prefix);
    let template_data = serde_json::to_string(&json!({
        "inviter": inviter,
        "org": org,
        "role": role,
    }))
    .unwrap_or_default();

    state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(
            aws_sdk_sesv2::types::Destination::builder()
                .to_addresses(to)
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
        .map_err(|e| {
            error!("Failed to send invite email: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_active_is_the_later_of_sign_in_and_use() {
        let mut item: Item = HashMap::new();
        assert_eq!(last_active(&item), "");
        item.insert("last_login_at".into(), attr_s("2026-10-01T09:00:00+00:00"));
        assert_eq!(last_active(&item), "2026-10-01T09:00:00+00:00");
        item.insert("last_seen_at".into(), attr_s("2026-10-21T08:30:00+00:00"));
        assert_eq!(last_active(&item), "2026-10-21T08:30:00+00:00");
    }

    fn claims(sub: &str, role: &str) -> Claims {
        Claims {
            sub: sub.to_string(),
            team_id: "TEAM#t1".to_string(),
            email: "admin@example.com".to_string(),
            role: role.to_string(),
            github_login: None,
            aud: "coderhelm-dashboard".to_string(),
            exp: 0,
            iat: 0,
        }
    }

    fn rec(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| {
                let val = if *k == "github_id" {
                    AttributeValue::N(v.to_string())
                } else {
                    attr_s(v)
                };
                (k.to_string(), val)
            })
            .collect()
    }

    fn email_member(sk: &str, role: &str) -> Item {
        rec(&[
            ("pk", "TEAM#t1"),
            ("sk", sk),
            ("email", "sam@example.com"),
            ("role", role),
            ("status", "active"),
        ])
    }

    fn github_only(sk: &str, role: &str) -> Item {
        rec(&[
            ("pk", "TEAM#t1"),
            ("sk", sk),
            ("email", ""),
            ("github_login", "sam-dev"),
            ("github_id", "4242"),
            ("avatar_url", "https://avatars.example.com/u/4242"),
            ("role", role),
        ])
    }

    #[test]
    fn member_view_reports_sign_in_kind_status_and_self() {
        let gh = member_view(&github_only("USER#4242", "member"), "USER#4242", at(DAY0));
        assert_eq!(gh["user_id"], "4242");
        assert_eq!(gh["email"], Value::Null);
        assert_eq!(gh["github_login"], "sam-dev");
        assert_eq!(gh["github_id"], "4242");
        assert_eq!(gh["sign_in"], "github");
        assert_eq!(gh["status"], "active");
        assert_eq!(gh["is_self"], true);

        let mut owner = email_member("USER#abc", "owner");
        owner.insert("github_login".into(), attr_s("sam-dev"));
        owner.insert("last_login_at".into(), attr_s("2026-01-02T00:00:00Z"));
        let v = member_view(&owner, "abc", at(DAY0));
        assert_eq!(v["sign_in"], "email_and_github");
        assert_eq!(v["last_active_at"], "2026-01-02T00:00:00Z");
        assert_eq!(v["is_self"], true, "bare and prefixed ids match");

        let invite = rec(&[
            ("sk", "USER#invite_01"),
            ("email", "new@example.com"),
            ("status", "invited"),
        ]);
        let v = member_view(&invite, "USER#abc", at(DAY0));
        assert_eq!(v["status"], "invited");
        assert_eq!(v["role"], "member");
        assert_eq!(v["is_self"], false);
    }

    #[test]
    fn members_sort_by_status_then_role_then_name() {
        let mut list = vec![
            json!({"user_id":"5","status":"invited","role":"admin","email":"a@example.com"}),
            json!({"user_id":"4","status":"active","role":"viewer","name":"Ann"}),
            json!({"user_id":"3","status":"active","role":"member","name":"zed"}),
            json!({"user_id":"2","status":"active","role":"member","name":"Bea"}),
            json!({"user_id":"1","status":"active","role":"owner","github_login":"zz"}),
        ];
        sort_members(&mut list);
        let ids: Vec<&str> = list
            .iter()
            .map(|v| v["user_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["1", "2", "3", "4", "5"]);
    }

    #[test]
    fn role_change_rules() {
        let admin = claims("USER#me", "admin");
        let owner = claims("USER#me", "owner");
        assert!(check_role_change(&admin, "x", "member", "viewer", 1).is_ok());
        assert_eq!(
            check_role_change(&admin, "x", "member", "admin", 1)
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN,
            "admins can't promote to admin"
        );
        assert_eq!(
            check_role_change(&admin, "x", "member", "owner", 1)
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN,
            "admins can't promote to owner"
        );
        assert_eq!(
            check_role_change(&admin, "x", "owner", "member", 2)
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            check_role_change(&admin, "me", "member", "viewer", 1)
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST,
            "own role, bare id vs USER# sub"
        );
        assert!(check_role_change(&owner, "x", "member", "owner", 1).is_ok());
        assert!(check_role_change(&owner, "x", "owner", "admin", 2).is_ok());
        assert_eq!(
            check_role_change(&owner, "x", "owner", "admin", 1)
                .unwrap_err()
                .0,
            StatusCode::CONFLICT,
            "the last owner can't be demoted"
        );
        assert_eq!(
            check_role_change(&owner, "x", "member", "root", 1)
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn removal_rules() {
        let admin = claims("USER#me", "admin");
        let owner = claims("USER#me", "owner");
        assert!(check_removal(&admin, "x", &email_member("USER#x", "viewer"), 1).is_ok());
        assert_eq!(
            check_removal(&admin, "x", &email_member("USER#x", "admin"), 1)
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            check_removal(&admin, "USER#me", &email_member("USER#me", "admin"), 1)
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
        assert!(check_removal(&owner, "x", &email_member("USER#x", "owner"), 2).is_ok());
        assert_eq!(
            check_removal(&owner, "x", &email_member("USER#x", "owner"), 1)
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        let invite = rec(&[
            ("sk", "USER#invite_1"),
            ("status", "invited"),
            ("role", "member"),
        ]);
        assert!(
            check_removal(&admin, "invite_1", &invite, 1).is_ok(),
            "revoke"
        );
    }

    #[test]
    fn link_moves_github_identity_onto_the_email_record() {
        let admin = claims("USER#me", "admin");
        let plan = plan_link(
            &admin,
            &email_member("USER#abc", "member"),
            &github_only("USER#4242", "viewer"),
        )
        .unwrap();
        assert_eq!(
            plan,
            LinkPlan {
                github_id: "4242".into(),
                github_login: "sam-dev".into(),
                avatar_url: Some("https://avatars.example.com/u/4242".into()),
                role: "member".into(),
            }
        );
        // Higher role wins; an existing avatar is kept.
        let mut person = email_member("USER#abc", "viewer");
        person.insert("avatar_url".into(), attr_s("https://example.com/me.png"));
        let plan = plan_link(&admin, &person, &github_only("USER#4242", "member")).unwrap();
        assert_eq!(plan.role, "member");
        assert_eq!(plan.avatar_url, None);
    }

    #[test]
    fn link_refuses_unsafe_merges() {
        let admin = claims("USER#me", "admin");
        let owner = claims("USER#me", "owner");
        let person = email_member("USER#abc", "member");
        let gh = github_only("USER#4242", "member");
        let code = |r: Result<LinkPlan, ApiError>| r.unwrap_err().0;

        assert_eq!(
            code(plan_link(&admin, &person, &person)),
            StatusCode::BAD_REQUEST
        );
        // Into a GitHub record, or from an email record: refused.
        assert_eq!(
            code(plan_link(&admin, &gh, &person)),
            StatusCode::BAD_REQUEST
        );
        // Into a pending invite: refused.
        let mut invite = email_member("USER#invite_1", "member");
        invite.insert("status".into(), attr_s("invited"));
        assert_eq!(
            code(plan_link(&admin, &invite, &gh)),
            StatusCode::BAD_REQUEST
        );
        // Person already linked to another GitHub account.
        let mut linked = person.clone();
        linked.insert("github_id".into(), AttributeValue::N("9".into()));
        assert_eq!(code(plan_link(&admin, &linked, &gh)), StatusCode::CONFLICT);
        // Same account again is fine (idempotent).
        linked.insert("github_id".into(), AttributeValue::N("4242".into()));
        assert!(plan_link(&admin, &linked, &gh).is_ok());
        // Admin can't touch an admin's or owner's records; an owner can.
        let admin_gh = github_only("USER#4242", "admin");
        assert_eq!(
            code(plan_link(&admin, &person, &admin_gh)),
            StatusCode::FORBIDDEN
        );
        assert_eq!(plan_link(&owner, &person, &admin_gh).unwrap().role, "admin");
    }

    #[test]
    fn email_validation() {
        assert!(valid_email("sam@example.com"));
        assert!(valid_email("sam+team@sub.example.co"));
        for bad in [
            "",
            "sam",
            "sam@",
            "@example.com",
            "sam@example",
            "sam @example.com",
            "a@b@c.com",
            "sam@.com",
        ] {
            assert!(!valid_email(bad), "{bad}");
        }
    }

    const DAY0: &str = "2026-01-01T12:00:00+00:00";

    fn at(t: &str) -> chrono::DateTime<chrono::Utc> {
        rfc3339(t).unwrap()
    }

    fn invite(role: &str, dates: &[(&str, &str)]) -> Item {
        let mut item = rec(&[
            ("pk", "TEAM#t1"),
            ("sk", "USER#invite_01"),
            ("email", "person@example.com"),
            ("role", role),
            ("status", "invited"),
        ]);
        for (k, v) in dates {
            item.insert(k.to_string(), attr_s(v));
        }
        item
    }

    #[test]
    fn admins_can_revoke_every_pending_invite_whatever_its_role_or_age() {
        let admin = claims("USER#me", "admin");
        let owner = claims("USER#me", "owner");
        let old = [("updated_at", "2025-01-01T00:00:00+00:00")];
        let fresh = [("invited_at", DAY0)];
        for role in ["viewer", "member", "billing", "admin", "owner", ""] {
            for dates in [&[][..], &old[..], &fresh[..]] {
                let item = invite(role, dates);
                assert!(
                    check_removal(&admin, "invite_01", &item, 1).is_ok(),
                    "admin revokes a {role:?} invite ({dates:?})"
                );
                assert!(check_removal(&owner, "invite_01", &item, 1).is_ok());
            }
        }
        // Seated admins and owners are still an owner's call.
        assert_eq!(
            check_removal(&admin, "x", &email_member("USER#x", "admin"), 1)
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn invite_people_routes_refuse_non_admins() {
        for role in ["viewer", "member", "billing", "", "superuser"] {
            assert_eq!(
                require_admin(&claims("USER#me", role)).unwrap_err().0,
                StatusCode::FORBIDDEN,
                "{role:?}"
            );
            assert!(!permissions::can_manage_invites(role));
        }
        for role in ["admin", "owner"] {
            assert!(require_admin(&claims("USER#me", role)).is_ok());
            assert!(permissions::can_manage_invites(role));
        }
    }

    #[test]
    fn invites_expire_after_the_valid_window_and_resending_renews_them() {
        let sent = invite("admin", &[("invited_at", DAY0)]);
        let expires = at(DAY0) + chrono::Duration::days(INVITE_VALID_DAYS);
        assert_eq!(invite_expires_at(&sent), Some(expires));
        assert!(!invite_expired(&sent, at(DAY0)));
        assert!(!invite_expired(
            &sent,
            expires - chrono::Duration::seconds(1)
        ));
        assert!(invite_expired(&sent, expires));

        // Older invites carry no invited_at: their write time (updated_at) counts.
        let legacy = invite(
            "admin",
            &[("updated_at", "2026-01-01T12:00:00.195746945+00:00")],
        );
        assert!(invite_expired(
            &legacy,
            expires + chrono::Duration::seconds(1)
        ));
        assert!(!invite_expired(&legacy, at(DAY0)));

        // A resend sets invited_at, which wins over the old write time.
        let mut renewed = legacy.clone();
        renewed.insert("invited_at".into(), attr_s("2026-03-01T00:00:00+00:00"));
        assert!(!invite_expired(
            &renewed,
            expires + chrono::Duration::days(1)
        ));

        // No readable date: shown as open rather than guessed.
        assert_eq!(invite_expires_at(&invite("member", &[])), None);
        assert!(!invite_expired(
            &invite("member", &[("updated_at", "junk")]),
            at(DAY0)
        ));

        // Members never expire.
        let mut member = email_member("USER#a", "member");
        member.insert("updated_at".into(), attr_s("2000-01-01T00:00:00+00:00"));
        assert_eq!(invite_expires_at(&member), None);
        assert!(!invite_expired(&member, at(DAY0)));
    }

    #[test]
    fn member_view_reports_invite_age_and_expiry() {
        let item = invite("admin", &[("updated_at", DAY0)]);
        let later = at(DAY0) + chrono::Duration::days(INVITE_VALID_DAYS + 1);
        let v = member_view(&item, "USER#me", later);
        assert_eq!(v["status"], "invited");
        assert_eq!(v["role"], "admin");
        assert_eq!(v["invited_at"], "2026-01-01T12:00:00Z");
        assert_eq!(v["invite_expires_at"], "2026-01-31T12:00:00Z");
        assert_eq!(v["invite_expired"], true);
        assert_eq!(
            member_view(&item, "USER#me", at(DAY0))["invite_expired"],
            false
        );

        let m = member_view(&email_member("USER#a", "admin"), "USER#me", later);
        assert_eq!(m["invited_at"], Value::Null);
        assert_eq!(m["invite_expires_at"], Value::Null);
        assert_eq!(m["invite_expired"], false);
    }

    #[test]
    fn resend_works_for_any_pending_invite_including_expired_ones() {
        let old = [("updated_at", "2025-01-01T00:00:00+00:00")];
        for role in ["member", "admin", "owner"] {
            assert_eq!(
                check_resend(&invite(role, &old)).unwrap(),
                "person@example.com"
            );
        }
        assert_eq!(
            check_resend(&email_member("USER#a", "member"))
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST,
            "a member has no invite to resend"
        );
        let mut no_email = invite("member", &[]);
        no_email.insert("email".into(), attr_s(""));
        assert_eq!(
            check_resend(&no_email).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn owner_count_ignores_invites() {
        let mut invite = email_member("USER#invite_1", "owner");
        invite.insert("status".into(), attr_s("invited"));
        let items = vec![
            email_member("USER#a", "owner"),
            invite,
            email_member("USER#b", "admin"),
        ];
        assert_eq!(active_owner_count(&items), 1);
    }
}
