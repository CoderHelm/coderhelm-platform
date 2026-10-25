//! Custom roles: a team's own roles, made by its admins and owners from the
//! capabilities in `permissions::CAPABILITIES`.
//!
//! Storage, in the settings table under the team's key:
//! - `pk = <team_id>`, `sk = CUSTOM_ROLE#<id>`: one role (name, description,
//!   capabilities as a JSON list of keys, the template it started from).
//! - `pk = <team_id>`, `sk = ROLE_AUDIT#<time>#<id>`: one entry in the log of
//!   role changes (created, edited, deleted, members moved, someone given a
//!   custom role).
//!
//! A member's record holds `custom:<id>` as its role. The auth middleware
//! turns that into a [`Holder`] through [`holder_for`] on every request.
//!
//! Only built-in admins and owners manage roles ([`role_manager`]); anyone on
//! the team can see them.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::permissions::{
    self, can_grant, checked_role_capabilities, custom_role_id, Holder, CAPABILITIES,
    CUSTOM_PREFIX, ROLES, VALID_ROLES,
};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

pub const ROLE_PREFIX: &str = "CUSTOM_ROLE#";
pub const AUDIT_PREFIX: &str = "ROLE_AUDIT#";
/// How long an entry stays in the log of role changes.
const AUDIT_DAYS: i64 = 365;
pub const MAX_ROLES: usize = 30;
pub const MAX_NAME: usize = 40;
pub const MAX_DESCRIPTION: usize = 300;
/// How long a custom role read by the auth middleware is reused. An edit made
/// on this instance takes effect at once; on others within this time.
const CACHE_FOR: Duration = Duration::from_secs(30);

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

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map_or("", |v| v.as_str())
}

/// One stored custom role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRole {
    pub id: String,
    pub name: String,
    pub description: String,
    /// The keys as stored. Read through `permissions::custom_capabilities`,
    /// which drops any that are no longer allowed.
    pub capabilities: Vec<String>,
    pub template: String,
    pub created_at: String,
    pub created_by: String,
    pub updated_at: String,
    pub updated_by: String,
}

impl StoredRole {
    fn from_item(item: &Item) -> Option<Self> {
        let id = s(item, "sk").strip_prefix(ROLE_PREFIX)?.to_string();
        let capabilities =
            serde_json::from_str::<Vec<String>>(s(item, "capabilities")).unwrap_or_default();
        Some(Self {
            id,
            name: s(item, "name").to_string(),
            description: s(item, "description").to_string(),
            capabilities,
            template: s(item, "template").to_string(),
            created_at: s(item, "created_at").to_string(),
            created_by: s(item, "created_by").to_string(),
            updated_at: s(item, "updated_at").to_string(),
            updated_by: s(item, "updated_by").to_string(),
        })
    }

    pub fn key(&self) -> String {
        format!("{CUSTOM_PREFIX}{}", self.id)
    }

    pub fn holder(&self) -> Holder {
        Holder::custom(&self.id, Some((&self.name, &self.capabilities)))
    }

    /// What the dashboard shows. `capabilities` is what the role holds now.
    fn view(&self, members: usize) -> Value {
        let holder = self.holder();
        json!({
            "id": self.id,
            "key": self.key(),
            "name": self.name,
            "description": self.description,
            "capabilities": CAPABILITIES
                .iter()
                .filter(|c| holder.holds(c.key))
                .map(|c| c.key)
                .collect::<Vec<_>>(),
            "acts_as": holder.floor(),
            "template": if self.template.is_empty() { Value::Null } else { json!(self.template) },
            "member_count": members,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

// ── Reads ───────────────────────────────────────────────────────────

/// Where a team's role is stored. The team always comes from the session,
/// so a role id never reaches another team's records.
pub fn role_record_key(team_id: &str, id: &str) -> (String, String) {
    (team_id.to_string(), format!("{ROLE_PREFIX}{id}"))
}

pub async fn load_role(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<Option<StoredRole>, String> {
    let (pk, sk) = role_record_key(team_id, id);
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&pk))
        .key("sk", attr_s(&sk))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("get custom role: {e}"))?;
    Ok(out.item().and_then(StoredRole::from_item))
}

pub async fn list_stored(state: &AppState, team_id: &str) -> Result<Vec<StoredRole>, String> {
    let mut roles = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":p", attr_s(ROLE_PREFIX))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("list custom roles: {e}"))?;
        roles.extend(out.items().iter().filter_map(StoredRole::from_item));
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    roles.sort_by_key(|r| r.name.to_lowercase());
    Ok(roles)
}

/// Every member record (and pending invite) of the team.
async fn member_items(state: &AppState, team_id: &str) -> Result<Vec<Item>, String> {
    let mut items = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":p", attr_s("USER#"))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("list members: {e}"))?;
        items.extend(out.items().iter().cloned());
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(items)
}

// ── The holder of a request ─────────────────────────────────────────

type Cache = Mutex<HashMap<(String, String), (Instant, Option<StoredRole>)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn forget(team_id: &str, id: &str) {
    if let Ok(mut c) = cache().lock() {
        c.remove(&role_record_key(team_id, id));
    }
}

/// What the person behind `claims` holds. A built-in role needs no lookup.
/// A custom role is read from the team's own records only (the key is the
/// session's team), so a role id from another team holds nothing here. When
/// the role is gone or can't be read, only the baseline is held.
pub async fn holder_for(state: &AppState, claims: &Claims) -> Holder {
    holder_for_role(state, &claims.team_id, &claims.role).await
}

/// What a member holds from the role on their record, in `team_id`. Used
/// where a person acts without a session (a signed email link).
pub async fn holder_for_role(state: &AppState, team_id: &str, role: &str) -> Holder {
    let Some(id) = custom_role_id(role) else {
        return Holder::builtin(role);
    };
    let key = role_record_key(team_id, id);
    let cached = cache()
        .lock()
        .ok()
        .and_then(|c| c.get(&key).cloned())
        .filter(|(at, _)| at.elapsed() < CACHE_FOR);
    let stored = match cached {
        Some((_, stored)) => stored,
        None => match load_role(state, team_id, id).await {
            Ok(stored) => {
                if let Ok(mut c) = cache().lock() {
                    if c.len() > 5_000 {
                        c.retain(|_, (at, _)| at.elapsed() < CACHE_FOR);
                    }
                    c.insert(key, (Instant::now(), stored.clone()));
                }
                stored
            }
            Err(e) => {
                warn!(error = %e, team_id = %team_id, "Custom role not read; baseline only");
                None
            }
        },
    };
    match stored {
        Some(r) => r.holder(),
        None => Holder::custom(id, None),
    }
}

// ── Rules ───────────────────────────────────────────────────────────

/// Role management is for built-in admins and owners. Checked by every route
/// that changes roles, on top of the auth middleware's capability check, and
/// a custom role can't hold the capability in the first place.
pub fn role_manager(claims: &Claims) -> Result<(), ApiError> {
    if claims.support_view.is_none() && claims.holder().manages_roles() {
        Ok(())
    } else {
        Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins and owners can create, change, delete or give out custom roles.",
        ))
    }
}

#[derive(Deserialize, Default)]
pub struct RoleBody {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Capability keys. When missing on create, the template's are used.
    #[serde(default)]
    pub capabilities: Option<Vec<String>>,
    /// A built-in role to start from: viewer, member or admin.
    #[serde(default)]
    pub template: Option<String>,
}

/// A cleaned name, or why it can't be used. Built-in names and other custom
/// roles' names (ignoring case) are taken.
pub fn check_name(
    name: &str,
    others: &[StoredRole],
    editing: Option<&str>,
) -> Result<String, String> {
    let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
    if name.is_empty() {
        return Err("Give the role a name.".into());
    }
    if name.chars().count() > MAX_NAME {
        return Err(format!("Keep the name to {MAX_NAME} characters."));
    }
    let lower = name.to_lowercase();
    if ROLES
        .iter()
        .any(|r| r.label.to_lowercase() == lower || r.key == lower)
    {
        return Err("That's the name of a built-in role. Pick another.".into());
    }
    if others
        .iter()
        .any(|r| Some(r.id.as_str()) != editing && r.name.to_lowercase() == lower)
    {
        return Err("Another role already has that name.".into());
    }
    Ok(name)
}

pub fn check_description(d: &str) -> Result<String, String> {
    let d = d.trim().to_string();
    if d.chars().count() > MAX_DESCRIPTION {
        return Err(format!(
            "Keep the description to {MAX_DESCRIPTION} characters."
        ));
    }
    Ok(d)
}

/// The capabilities a create or edit asks for, from the body's list or else
/// its template.
fn requested_caps(body: &RoleBody) -> Result<Vec<String>, ApiError> {
    if let Some(list) = &body.capabilities {
        return Ok(list.clone());
    }
    match body.template.as_deref() {
        Some(t) => permissions::template_capabilities(t)
            .filter(|_| t != "owner")
            .map(|v| v.into_iter().map(str::to_string).collect())
            .ok_or_else(|| {
                fail(
                    StatusCode::BAD_REQUEST,
                    "Start from Viewer, Member or Admin.",
                )
            }),
        None => Ok(Vec::new()),
    }
}

/// What a member of the deleted role moves to, checked: a built-in role the
/// person deleting may give (not owner), or another of the team's custom
/// roles, never the role being deleted.
pub fn check_replacement(
    actor: &Claims,
    deleting: &str,
    replacement: &str,
    roles: &[StoredRole],
) -> Result<String, String> {
    if let Some(id) = custom_role_id(replacement) {
        if id == deleting {
            return Err("Pick a different role for its members.".into());
        }
        let Some(r) = roles.iter().find(|r| r.id == id) else {
            return Err("That role no longer exists.".into());
        };
        if !permissions::within(&actor.holder(), &r.holder().caps) {
            return Err("You can't move people to a role with more than your own.".into());
        }
        return Ok(r.key());
    }
    if replacement == "owner" {
        return Err("Members can't be moved to Owner this way.".into());
    }
    if !VALID_ROLES.contains(&replacement) || !can_grant(&actor.role, replacement) {
        return Err("Pick a role you're allowed to give.".into());
    }
    Ok(replacement.to_string())
}

/// Members (and invites) whose record holds this role.
fn holding(items: &[Item], role_key: &str) -> Vec<Item> {
    items
        .iter()
        .filter(|i| s(i, "role") == role_key)
        .cloned()
        .collect()
}

fn role_counts(items: &[Item]) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    for i in items {
        *out.entry(s(i, "role").to_string()).or_insert(0) += 1;
    }
    out
}

// ── The log of role changes ─────────────────────────────────────────

pub struct AuditEntry<'a> {
    pub action: &'a str,
    pub role_id: &'a str,
    pub role_name: &'a str,
    pub detail: String,
}

/// Writes one entry. A role change is refused when its entry can't be
/// written first, so nothing changes without a record of it.
pub async fn record(state: &AppState, claims: &Claims, e: AuditEntry<'_>) -> Result<(), ApiError> {
    let now = chrono::Utc::now();
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let ttl = (now + chrono::Duration::days(AUDIT_DAYS)).timestamp();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item(
            "sk",
            attr_s(&format!("{AUDIT_PREFIX}{at}#{}", ulid::Ulid::new())),
        )
        .item("record_type", attr_s("role_audit"))
        .item("at", attr_s(&at))
        .item("actor", attr_s(&claims.display_name()))
        .item("actor_id", attr_s(&claims.sub))
        .item("action", attr_s(e.action))
        .item("role_id", attr_s(e.role_id))
        .item("role_name", attr_s(e.role_name))
        .item("detail", attr_s(common::truncate_str(&e.detail, 1000)))
        .item("ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await
        .map(|_| ())
        .map_err(|err| internal("Role change log not written", err))
}

/// The labels of capability keys, for a log line.
fn labels(keys: &BTreeSet<&str>) -> String {
    CAPABILITIES
        .iter()
        .filter(|c| keys.contains(c.key))
        .map(|c| c.label)
        .collect::<Vec<_>>()
        .join("; ")
}

// ── Routes ──────────────────────────────────────────────────────────

/// GET /api/roles — the team's custom roles, how many people hold each, and
/// what the editor needs. Anyone on the team may look.
pub async fn list_roles(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    let roles = list_stored(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list roles", e))?;
    let items = member_items(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to count members", e))?;
    let counts = role_counts(&items);
    let actor = claims.holder();
    let manages = role_manager(&claims).is_ok();
    let keys = |f: &dyn Fn(&permissions::Capability) -> bool| -> Vec<&'static str> {
        CAPABILITIES
            .iter()
            .filter(|c| f(c))
            .map(|c| c.key)
            .collect()
    };
    let views: Vec<Value> = roles
        .iter()
        .map(|r| r.view(counts.get(&r.key()).copied().unwrap_or(0)))
        .collect();
    let templates: serde_json::Map<String, Value> = ["viewer", "member", "admin"]
        .iter()
        .map(|t| {
            (
                t.to_string(),
                json!(permissions::template_capabilities(t).unwrap_or_default()),
            )
        })
        .collect();
    // What the editor offers: customizable capabilities. Owner-only ones and
    // role management are never offered.
    Ok(Json(json!({
        "roles": views,
        "can_manage": manages,
        "max_roles": MAX_ROLES,
        "customizable": keys(&|c| permissions::customizable(c)),
        "baseline": keys(&|c| permissions::is_baseline(c)),
        "yours": keys(&|c| actor.holds(c.key)),
        "templates": templates,
    })))
}

/// POST /api/roles — make a custom role.
pub async fn create_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RoleBody>,
) -> Result<Json<Value>, ApiError> {
    role_manager(&claims)?;
    let roles = list_stored(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list roles", e))?;
    if roles.len() >= MAX_ROLES {
        return Err(fail(
            StatusCode::CONFLICT,
            &format!("A team can have up to {MAX_ROLES} custom roles. Delete one first."),
        ));
    }
    let name = check_name(body.name.as_deref().unwrap_or(""), &roles, None)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let description = check_description(body.description.as_deref().unwrap_or(""))
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let caps = checked_role_capabilities(&claims.holder(), &requested_caps(&body)?)
        .map_err(|e| fail(StatusCode::FORBIDDEN, &e.message()))?;
    let template = body
        .template
        .as_deref()
        .filter(|t| permissions::template_capabilities(t).is_some() && *t != "owner")
        .unwrap_or("");

    let id = ulid::Ulid::new().to_string().to_lowercase();
    record(
        &state,
        &claims,
        AuditEntry {
            action: "created",
            role_id: &id,
            role_name: &name,
            detail: format!("Can: {}", labels(&caps)),
        },
    )
    .await?;
    let now = chrono::Utc::now().to_rfc3339();
    let keys: Vec<&str> = caps.iter().copied().collect();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&format!("{ROLE_PREFIX}{id}")))
        .item("record_type", attr_s("custom_role"))
        .item("name", attr_s(&name))
        .item("description", attr_s(&description))
        .item(
            "capabilities",
            attr_s(&serde_json::to_string(&keys).unwrap_or_else(|_| "[]".into())),
        )
        .item("template", attr_s(template))
        .item("created_at", attr_s(&now))
        .item("created_by", attr_s(&claims.display_name()))
        .item("updated_at", attr_s(&now))
        .item("updated_by", attr_s(&claims.display_name()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .map_err(|e| internal("Failed to save role", e))?;
    info!(team_id = %claims.team_id, role_id = %id, "Custom role created");
    let saved = load_role(&state, &claims.team_id, &id)
        .await
        .map_err(|e| internal("Failed to read role back", e))?
        .ok_or_else(|| internal("Failed to read role back", "missing"))?;
    Ok(Json(json!({ "role": saved.view(0) })))
}

/// PUT /api/roles/:role_id — rename, describe or change what a role can do.
pub async fn update_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(role_id): Path<String>,
    Json(body): Json<RoleBody>,
) -> Result<Json<Value>, ApiError> {
    role_manager(&claims)?;
    let roles = list_stored(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list roles", e))?;
    let Some(current) = roles.iter().find(|r| r.id == role_id).cloned() else {
        return Err(fail(StatusCode::NOT_FOUND, "That role doesn't exist."));
    };
    let name = match body.name.as_deref() {
        Some(n) => {
            check_name(n, &roles, Some(&role_id)).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?
        }
        None => current.name.clone(),
    };
    let description = match body.description.as_deref() {
        Some(d) => check_description(d).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?,
        None => current.description.clone(),
    };
    let actor = claims.holder();
    // Someone who doesn't hold everything the role has now can't edit it:
    // their edit would decide over capabilities they don't have.
    if !permissions::within(&actor, &current.holder().caps) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "This role can do things your own role can't, so you can't change it.",
        ));
    }
    let caps = match &body.capabilities {
        Some(list) => checked_role_capabilities(&actor, list)
            .map_err(|e| fail(StatusCode::FORBIDDEN, &e.message()))?,
        None => current.holder().caps,
    };
    let before = current.holder().caps;
    let added: BTreeSet<&str> = caps.difference(&before).copied().collect();
    let removed: BTreeSet<&str> = before.difference(&caps).copied().collect();
    let mut detail = Vec::new();
    if name != current.name {
        detail.push(format!("Renamed from \"{}\"", current.name));
    }
    if description != current.description {
        detail.push("Description changed".to_string());
    }
    if !added.is_empty() {
        detail.push(format!("Added: {}", labels(&added)));
    }
    if !removed.is_empty() {
        detail.push(format!("Removed: {}", labels(&removed)));
    }
    if detail.is_empty() {
        return Ok(Json(json!({ "role": current.view(0), "changed": false })));
    }
    record(
        &state,
        &claims,
        AuditEntry {
            action: "updated",
            role_id: &role_id,
            role_name: &name,
            detail: detail.join(". "),
        },
    )
    .await?;
    let keys: Vec<&str> = caps.iter().copied().collect();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("{ROLE_PREFIX}{role_id}")))
        .update_expression(
            "SET #n = :n, description = :d, capabilities = :c, updated_at = :now, updated_by = :by",
        )
        .condition_expression("attribute_exists(pk) AND updated_at = :was")
        .expression_attribute_names("#n", "name")
        .expression_attribute_values(":n", attr_s(&name))
        .expression_attribute_values(":d", attr_s(&description))
        .expression_attribute_values(
            ":c",
            attr_s(&serde_json::to_string(&keys).unwrap_or_else(|_| "[]".into())),
        )
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .expression_attribute_values(":was", attr_s(&current.updated_at))
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception())
            {
                fail(
                    StatusCode::CONFLICT,
                    "Someone changed this role while you were editing. Reload and try again.",
                )
            } else {
                internal("Failed to update role", e)
            }
        })?;
    forget(&claims.team_id, &role_id);
    info!(team_id = %claims.team_id, role_id = %role_id, "Custom role updated");
    let saved = load_role(&state, &claims.team_id, &role_id)
        .await
        .map_err(|e| internal("Failed to read role back", e))?
        .ok_or_else(|| internal("Failed to read role back", "missing"))?;
    Ok(Json(json!({ "role": saved.view(0), "changed": true })))
}

#[derive(Deserialize, Default)]
pub struct DeleteQuery {
    /// The role its members move to: a built-in key or `custom:<id>`.
    #[serde(default)]
    pub replacement: Option<String>,
}

/// DELETE /api/roles/:role_id?replacement=<role> — delete a role. When people
/// hold it, `replacement` says which role they get instead; without one the
/// answer is 409 with how many hold it.
pub async fn delete_role(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(role_id): Path<String>,
    Query(q): Query<DeleteQuery>,
) -> Result<Json<Value>, ApiError> {
    role_manager(&claims)?;
    let roles = list_stored(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list roles", e))?;
    let Some(current) = roles.iter().find(|r| r.id == role_id).cloned() else {
        return Err(fail(StatusCode::NOT_FOUND, "That role doesn't exist."));
    };
    let items = member_items(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list members", e))?;
    let held = holding(&items, &current.key());
    let replacement = match q.replacement.as_deref().filter(|r| !r.is_empty()) {
        Some(r) => Some(
            check_replacement(&claims, &role_id, r, &roles)
                .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?,
        ),
        None => None,
    };
    if !held.is_empty() && replacement.is_none() {
        let n = held.len();
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": format!(
                    "{n} {} this role. Pick the role they get instead, then delete it.",
                    if n == 1 { "person has" } else { "people have" }
                ),
                "needs_replacement": true,
                "member_count": n,
            })),
        ));
    }
    let replacement_name = replacement.as_deref().map(|r| {
        custom_role_id(r)
            .and_then(|id| roles.iter().find(|x| x.id == id))
            .map_or_else(
                || {
                    ROLES
                        .iter()
                        .find(|x| x.key == r)
                        .map_or(r.to_string(), |x| x.label.to_string())
                },
                |x| x.name.clone(),
            )
    });
    record(
        &state,
        &claims,
        AuditEntry {
            action: "deleted",
            role_id: &role_id,
            role_name: &current.name,
            detail: match &replacement_name {
                Some(to) if !held.is_empty() => {
                    format!("{} moved to {to}", held.len())
                }
                _ => "No one held it".to_string(),
            },
        },
    )
    .await?;

    // Move its members first, each only while they still hold it, so nobody
    // is ever left holding a role that is gone.
    let now = chrono::Utc::now().to_rfc3339();
    let mut moved = 0usize;
    if let Some(to) = &replacement {
        for item in &held {
            let r = state
                .dynamo
                .update_item()
                .table_name(&state.config.users_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s(s(item, "sk")))
                .update_expression("SET #r = :to, updated_at = :now")
                .condition_expression("#r = :from")
                .expression_attribute_names("#r", "role")
                .expression_attribute_values(":to", attr_s(to))
                .expression_attribute_values(":from", attr_s(&current.key()))
                .expression_attribute_values(":now", attr_s(&now))
                .send()
                .await;
            match r {
                Ok(_) => moved += 1,
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|se| se.is_conditional_check_failed_exception()) => {}
                Err(e) => return Err(internal("Failed to move a member", e)),
            }
        }
    }
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("{ROLE_PREFIX}{role_id}")))
        .send()
        .await
        .map_err(|e| internal("Failed to delete role", e))?;
    forget(&claims.team_id, &role_id);
    info!(team_id = %claims.team_id, role_id = %role_id, moved, "Custom role deleted");
    Ok(Json(json!({
        "status": "deleted",
        "moved": moved,
        "replacement": replacement,
    })))
}

/// GET /api/roles/audit — the latest role changes, newest first.
pub async fn list_audit(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    role_manager(&claims)?;
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":p", attr_s(AUDIT_PREFIX))
        .scan_index_forward(false)
        .limit(50)
        .send()
        .await
        .map_err(|e| internal("Failed to list role changes", e))?;
    let entries: Vec<Value> = out
        .items()
        .iter()
        .map(|i| {
            json!({
                "at": s(i, "at"),
                "actor": s(i, "actor"),
                "action": s(i, "action"),
                "role_id": s(i, "role_id"),
                "role_name": s(i, "role_name"),
                // A key's use summary keeps a count; its sentence is made here.
                "detail": if s(i, "action") == "api_key_used" {
                    crate::api_keys::use_summary_detail(
                        i.get("minutes")
                            .and_then(|v| v.as_n().ok())
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(1),
                        s(i, "at"),
                    )
                } else {
                    s(i, "detail").to_string()
                },
            })
        })
        .collect();
    Ok(Json(json!({ "entries": entries })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{resolve_access, Access};

    fn claims(role: &str) -> Claims {
        Claims {
            sub: "USER#me".into(),
            team_id: "TEAM#a".into(),
            email: "me@example.com".into(),
            role: role.into(),
            github_login: None,
            aud: "coderhelm-dashboard".into(),
            exp: 0,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
        }
    }

    fn stored(id: &str, name: &str, caps: &[&str]) -> StoredRole {
        StoredRole {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            capabilities: caps.iter().map(|c| c.to_string()).collect(),
            template: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            updated_at: String::new(),
            updated_by: String::new(),
        }
    }

    /// Claims as the middleware leaves them for a custom-role holder.
    fn custom_claims(role: &StoredRole) -> Claims {
        let holder = role.holder();
        let mut c = claims(&role.key());
        if let Access::Allow { effective_role } = resolve_access(&holder, None) {
            c.role = effective_role.into();
        }
        c.custom = Some(holder);
        c
    }

    #[test]
    fn only_built_in_admins_and_owners_manage_roles() {
        assert!(role_manager(&claims("owner")).is_ok());
        assert!(role_manager(&claims("admin")).is_ok());
        assert_eq!(
            role_manager(&claims("member")).unwrap_err().0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            role_manager(&claims("viewer")).unwrap_err().0,
            StatusCode::FORBIDDEN
        );
        assert!(role_manager(&claims("billing")).is_err());
        // A custom role with every capability it may hold still can't, even
        // when a request runs it as an admin.
        let all: Vec<&str> = CAPABILITIES.iter().map(|c| c.key).collect();
        let everything = stored("r1", "Everything", &all);
        let mut c = custom_claims(&everything);
        assert!(role_manager(&c).is_err());
        c.role = "admin".into();
        assert!(role_manager(&c).is_err());
        c.role = "owner".into();
        assert!(role_manager(&c).is_err());
        // A support view is read-only, whatever role it carries.
        let mut sv = claims("admin");
        sv.support_view = Some(crate::models::SupportView {
            team_id: "TEAM#a".into(),
            until: 0,
            grant_id: "g".into(),
            admin_sub: "USER#s".into(),
            admin_email: "s@example.com".into(),
        });
        assert!(role_manager(&sv).is_err());
    }

    #[test]
    fn the_role_routes_refuse_custom_roles_before_the_handler() {
        let all: Vec<&str> = CAPABILITIES.iter().map(|c| c.key).collect();
        let holder = stored("r1", "Everything", &all).holder();
        for h in [
            "roles::create_role",
            "roles::update_role",
            "roles::delete_role",
            "roles::list_audit",
        ] {
            assert!(
                matches!(resolve_access(&holder, Some(h)), Access::Deny { .. }),
                "{h}"
            );
            assert!(matches!(
                resolve_access(&Holder::builtin("admin"), Some(h)),
                Access::Allow { .. }
            ));
            assert!(matches!(
                resolve_access(&Holder::builtin("member"), Some(h)),
                Access::Deny { .. }
            ));
        }
        // Seeing the roles is for everyone.
        assert!(matches!(
            resolve_access(&holder, Some("roles::list_roles")),
            Access::Allow { .. }
        ));
        assert!(matches!(
            resolve_access(&Holder::builtin("viewer"), Some("roles::list_roles")),
            Access::Allow { .. }
        ));
    }

    #[test]
    fn names_are_cleaned_and_unique() {
        let others = vec![stored("r1", "Release manager", &[])];
        assert_eq!(
            check_name("  QA   lead ", &others, None).unwrap(),
            "QA lead"
        );
        assert!(check_name("", &others, None).is_err());
        assert!(check_name("admin", &others, None).is_err());
        assert!(check_name("Owner", &others, None).is_err());
        assert!(check_name("release MANAGER", &others, None).is_err());
        // Renaming a role to its own name (other case) is fine.
        assert!(check_name("release MANAGER", &others, Some("r1")).is_ok());
        assert!(check_name(&"x".repeat(MAX_NAME + 1), &others, None).is_err());
        assert!(check_description(&"x".repeat(MAX_DESCRIPTION + 1)).is_err());
    }

    #[test]
    fn deleting_a_role_in_use_needs_a_replacement_you_may_give() {
        let roles = vec![
            stored("r1", "QA", &["plans"]),
            stored("r2", "Ops", &["repos", "system"]),
        ];
        let admin = claims("admin");
        // Not the role being deleted, and only roles that exist.
        assert!(check_replacement(&admin, "r1", "custom:r1", &roles).is_err());
        assert!(check_replacement(&admin, "r1", "custom:gone", &roles).is_err());
        assert_eq!(
            check_replacement(&admin, "r1", "custom:r2", &roles).unwrap(),
            "custom:r2"
        );
        assert_eq!(
            check_replacement(&admin, "r1", "member", &roles).unwrap(),
            "member"
        );
        // Never owner; admin only from an owner.
        assert!(check_replacement(&claims("owner"), "r1", "owner", &roles).is_err());
        assert!(check_replacement(&admin, "r1", "admin", &roles).is_err());
        assert!(check_replacement(&claims("owner"), "r1", "admin", &roles).is_ok());
        assert!(check_replacement(&admin, "r1", "superuser", &roles).is_err());

        // Who holds the role being deleted.
        let mk = |sk: &str, role: &str| {
            Item::from([
                ("sk".to_string(), attr_s(sk)),
                ("role".to_string(), attr_s(role)),
            ])
        };
        let items = vec![
            mk("USER#a", "custom:r1"),
            mk("USER#b", "member"),
            mk("USER#invite_c", "custom:r1"),
        ];
        assert_eq!(holding(&items, "custom:r1").len(), 2);
        assert_eq!(holding(&items, "custom:r2").len(), 0);
        assert_eq!(role_counts(&items)["custom:r1"], 2);
    }

    #[test]
    fn templates_start_from_a_built_in_role() {
        let body = RoleBody {
            template: Some("member".into()),
            ..Default::default()
        };
        let caps = requested_caps(&body).unwrap();
        assert!(caps.contains(&"plans".to_string()));
        assert!(!caps.contains(&"repos".to_string()));
        let admin = requested_caps(&RoleBody {
            template: Some("admin".into()),
            ..Default::default()
        })
        .unwrap();
        assert!(admin.contains(&"repos".to_string()));
        assert!(!admin.contains(&"roles".to_string()));
        assert!(requested_caps(&RoleBody {
            template: Some("owner".into()),
            ..Default::default()
        })
        .is_err());
        // A list given with a template wins.
        let picked = requested_caps(&RoleBody {
            template: Some("admin".into()),
            capabilities: Some(vec!["plans".into()]),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(picked, vec!["plans".to_string()]);
    }

    #[test]
    fn a_role_is_looked_up_in_the_sessions_team_only() {
        // The same id in two teams is two records, and the lookup key comes
        // from the session's team.
        assert_ne!(
            role_record_key("TEAM#a", "r1"),
            role_record_key("TEAM#b", "r1")
        );
        assert_eq!(role_record_key("TEAM#a", "r1").0, "TEAM#a");
        assert_eq!(role_record_key("TEAM#a", "r1").1, "CUSTOM_ROLE#r1");
        // A role that isn't one of this team's holds only the baseline.
        let gone = Holder::custom("r1", None);
        assert!(gone.holds("view"));
        assert!(!gone.holds("plans"));
        assert_eq!(gone.floor(), "viewer");
        assert!(matches!(
            resolve_access(&gone, Some("plans::create_plan")),
            Access::Deny { .. }
        ));
    }

    #[test]
    fn a_stored_role_is_read_back_whole() {
        let item = Item::from([
            ("sk".to_string(), attr_s("CUSTOM_ROLE#r9")),
            ("name".to_string(), attr_s("QA")),
            ("capabilities".to_string(), attr_s("[\"plans\",\"reset\"]")),
        ]);
        let r = StoredRole::from_item(&item).unwrap();
        assert_eq!(r.id, "r9");
        assert_eq!(r.key(), "custom:r9");
        let h = r.holder();
        assert!(h.holds("plans"));
        // An owner-only key that somehow got stored is never held.
        assert!(!h.holds("reset"));
        assert!(
            StoredRole::from_item(&Item::from([("sk".to_string(), attr_s("ROLE_AUDIT#x"))]))
                .is_none()
        );
    }
}
