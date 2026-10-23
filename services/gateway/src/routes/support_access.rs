//! Support access: a team admin lets Coderhelm support see the team's
//! workspace for a fixed window, and the team sees every read support made.
//!
//! - Off until a team admin or owner turns it on, for 24 hours, 72 hours or
//!   7 days, optionally tied to one of the team's tickets. They can end it at
//!   any time.
//! - While it is on, a platform admin can open a **support view**: a
//!   separate, short-lived session (its own cookie and JWT audience, so it is
//!   never accepted as a normal session) that reads the team's dashboard as
//!   the team, and nothing more. It acts as no person on the team.
//! - The auth middleware enforces it, not each route: a support-view request
//!   that is not a read is refused with 403 "read-only support view", and a
//!   few reads that reach the team's own systems are refused too
//!   ([`support_view_refusal`]). Every request in a support view is written
//!   to the team's audit log, which the team reads in Settings.
//! - The grant is checked on every request, from a cache at most
//!   [`GRANT_CACHE`] old, so an expired, ended or replaced grant ends the
//!   sessions it allowed.
//!
//! Storage, in the settings table:
//!
//! - `pk = <team_id>`, `sk = SUPPORT_ACCESS`: the team's grant (one at a time;
//!   a new one replaces it).
//! - `pk = <team_id>`, `sk = SUPPORT_AUDIT#<time>#<id>`: one audit entry.
//!   Expires through `ttl` after [`AUDIT_DAYS`].

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use super::support::{self, clip, one_line, scrub, Caller, Fail, Reply};
use crate::models::{Claims, SupportView};
use crate::AppState;

pub const ACCESS_SK: &str = "SUPPORT_ACCESS";
const AUDIT_PREFIX: &str = "SUPPORT_AUDIT#";
/// How long a team can let support in for, in hours.
pub const WINDOWS_HOURS: [u64; 3] = [24, 72, 168];
/// Longest one support-view session lasts before support opens it again.
pub const SESSION_MAX_SECS: u64 = 2 * 3600;
/// How long audit entries are kept.
pub const AUDIT_DAYS: i64 = 400;
/// Oldest grant a request may be checked against.
pub const GRANT_CACHE: Duration = Duration::from_secs(60);
const MAX_NOTE: usize = 500;
const LOG_PAGE: usize = 200;

pub const SUPPORT_COOKIE: &str = "__Host-coderhelm_support";
/// The support-view token's audience. A normal session token is checked for
/// `coderhelm-dashboard`, so one can never stand in for the other.
pub const SUPPORT_AUD: &str = "coderhelm-support-view";
pub const READ_ONLY: &str = "read-only support view";
const OUTBOUND: &str =
    "Support view does not reach the team's own systems (cloud accounts, trackers, connectors).";

// ── Model ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: String,
    pub team_id: String,
    pub granted_by_id: String,
    pub granted_by_email: String,
    pub granted_at: String,
    /// Unix seconds.
    pub until: u64,
    #[serde(default)]
    pub note: String,
    /// The team's ticket it was given for, if any.
    #[serde(default)]
    pub ticket_id: String,
    #[serde(default)]
    pub revoked_at: String,
    #[serde(default)]
    pub revoked_by_email: String,
}

impl Grant {
    pub fn active_at(&self, now: u64) -> bool {
        self.revoked_at.is_empty() && now < self.until
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub at: String,
    /// granted | revoked | session_start | read | refused
    pub kind: String,
    /// Who: the team admin for granted/revoked, the support admin otherwise.
    pub actor_email: String,
    #[serde(default)]
    pub method: String,
    /// The API route read, with its query string.
    #[serde(default)]
    pub route: String,
    #[serde(default)]
    pub ticket_id: String,
    #[serde(default)]
    pub detail: String,
}

/// Where grants and the audit log are kept. A trait so the rules can be
/// tested without DynamoDB.
pub trait AccessStore {
    async fn grant(&self, team_id: &str) -> Result<Option<Grant>, String>;
    async fn put_grant(&self, g: &Grant) -> Result<(), String>;
    async fn audit(&self, team_id: &str, e: &AuditEntry) -> Result<(), String>;
    /// Newest first.
    async fn audit_log(&self, team_id: &str, limit: usize) -> Result<Vec<AuditEntry>, String>;
}

fn now_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn iso(secs: u64) -> String {
    chrono::DateTime::from_timestamp(secs as i64, 0)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn new_id() -> String {
    ulid::Ulid::new().to_string()
}

fn entry(kind: &str, actor: &str, at: u64) -> AuditEntry {
    AuditEntry {
        id: new_id(),
        at: iso(at),
        kind: kind.into(),
        actor_email: actor.into(),
        method: String::new(),
        route: String::new(),
        ticket_id: String::new(),
        detail: String::new(),
    }
}

// ── The rules ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct GrantRequest {
    #[serde(default)]
    pub hours: u64,
    #[serde(default)]
    pub note: Option<String>,
    /// One of the team's tickets. The handler checks it belongs to the team.
    #[serde(default)]
    pub ticket_id: Option<String>,
}

fn require_team_admin(caller: &Caller) -> Result<(), Fail> {
    if caller.in_support_view {
        return Err(Fail::Denied(READ_ONLY.into()));
    }
    if crate::permissions::role_level(&caller.role) < 3 {
        return Err(Fail::Denied(
            "Only a team admin or owner can change support access.".into(),
        ));
    }
    Ok(())
}

/// A team admin lets support in for one of the fixed windows. Replaces any
/// grant the team had, which ends the sessions it allowed.
pub async fn give<S: AccessStore>(
    store: &S,
    caller: &Caller,
    req: &GrantRequest,
    now: u64,
) -> Result<Grant, Fail> {
    require_team_admin(caller)?;
    if !WINDOWS_HOURS.contains(&req.hours) {
        return Err(Fail::BadRequest(
            "Choose 24 hours, 72 hours or 7 days.".into(),
        ));
    }
    let note = clip(
        &scrub(&one_line(req.note.as_deref().unwrap_or(""))),
        MAX_NOTE,
    );
    let g = Grant {
        id: new_id(),
        team_id: caller.team_id.clone(),
        granted_by_id: caller.user_id.clone(),
        granted_by_email: caller.email.clone(),
        granted_at: iso(now),
        until: now + req.hours * 3600,
        note: note.clone(),
        ticket_id: req.ticket_id.clone().unwrap_or_default(),
        revoked_at: String::new(),
        revoked_by_email: String::new(),
    };
    store.put_grant(&g).await.map_err(Fail::Internal)?;
    let mut e = entry("granted", &caller.email, now);
    e.ticket_id = g.ticket_id.clone();
    e.detail = format!(
        "until {}{}",
        iso(g.until),
        if note.is_empty() {
            String::new()
        } else {
            format!(" · {note}")
        }
    );
    store
        .audit(&caller.team_id, &e)
        .await
        .map_err(Fail::Internal)?;
    forget(&caller.team_id);
    Ok(g)
}

/// A team admin ends support access now. `None` when none was on.
pub async fn revoke<S: AccessStore>(
    store: &S,
    caller: &Caller,
    now: u64,
) -> Result<Option<Grant>, Fail> {
    require_team_admin(caller)?;
    let Some(mut g) = store
        .grant(&caller.team_id)
        .await
        .map_err(Fail::Internal)?
        .filter(|g| g.active_at(now))
    else {
        return Ok(None);
    };
    g.revoked_at = iso(now);
    g.revoked_by_email = caller.email.clone();
    store.put_grant(&g).await.map_err(Fail::Internal)?;
    let mut e = entry("revoked", &caller.email, now);
    e.ticket_id = g.ticket_id.clone();
    store
        .audit(&caller.team_id, &e)
        .await
        .map_err(Fail::Internal)?;
    forget(&caller.team_id);
    Ok(Some(g))
}

/// A platform admin opens a support view of a team. Only while the team's
/// grant is on; the session ends with the grant, or after
/// [`SESSION_MAX_SECS`], whichever is first. The start is audited.
pub async fn start<S: AccessStore>(
    store: &S,
    caller: &Caller,
    team_id: &str,
    ticket_id: &str,
    now: u64,
) -> Result<(Grant, SupportView), Fail> {
    if !caller.platform_admin {
        return Err(Fail::Forbidden);
    }
    if caller.in_support_view {
        return Err(Fail::Denied(READ_ONLY.into()));
    }
    if !team_id.starts_with("TEAM#") {
        return Err(Fail::BadRequest("Name the team to view.".into()));
    }
    let g = store
        .grant(team_id)
        .await
        .map_err(Fail::Internal)?
        .filter(|g| g.active_at(now) && g.team_id == team_id)
        .ok_or_else(|| {
            Fail::Denied("This team has not let support in, or its access has ended.".into())
        })?;
    let sv = SupportView {
        team_id: team_id.to_string(),
        until: g.until.min(now + SESSION_MAX_SECS),
        grant_id: g.id.clone(),
        admin_sub: caller.user_id.clone(),
        admin_email: caller.email.clone(),
    };
    let mut e = entry("session_start", &caller.email, now);
    e.ticket_id = ticket_id.to_string();
    e.detail = format!("until {}", iso(sv.until));
    store.audit(team_id, &e).await.map_err(Fail::Internal)?;
    Ok((g, sv))
}

/// Whether a support-view session may go on: the team's grant is the one it
/// was opened under, is still on, and the session has not run out.
pub fn session_ok(grant: Option<&Grant>, sv: &SupportView, now: u64) -> bool {
    now < sv.until
        && grant.is_some_and(|g| g.active_at(now) && g.id == sv.grant_id && g.team_id == sv.team_id)
}

/// Writes one request of a support-view session to the team's audit log.
pub async fn record<S: AccessStore>(
    store: &S,
    sv: &SupportView,
    kind: &str,
    method: &Method,
    route: &str,
    now: u64,
) -> Result<(), String> {
    let mut e = entry(kind, &sv.admin_email, now);
    e.method = method.as_str().to_string();
    e.route = clip(route, 400);
    store.audit(&sv.team_id, &e).await
}

/// Paths (inside `/api`) of reads a support view may not make even though
/// they are GETs. `*` is one path segment. Each one either reaches the
/// team's own systems with the team's stored credentials, writes as it
/// reads, or answers with a credential:
///
/// - log groups, topic alarms, the AWS check: assume the team's AWS role;
/// - Jira projects: calls the team's Jira app;
/// - Jira check: returns the Forge secret, and creates one if missing;
/// - Jira config: returns the Forge trigger URLs, which work as credentials;
/// - CloudFormation link, AWS setup: the team's external id, created if
///   missing;
/// - health: marks stuck runs failed; DLQ: reads the shared queue.
pub const SUPPORT_VIEW_BLOCKED_READS: &[&str] = &[
    "/aws-connections/*/log-groups",
    "/alert-routes/alarms",
    "/connectors/aws-check",
    "/integrations/jira/projects/fetch",
    "/integrations/jira/check",
    "/integrations/jira/config",
    "/aws-connections/cfn-url",
    "/data-sources/aws/setup",
    "/health",
    "/dlq",
];

/// Fields hidden from every answer in a support view, wherever they appear:
/// webhook URLs that post as the team, the AWS external id, Jira's Forge
/// secret and trigger URLs, queue receipt handles. Keys and tokens are never
/// in answers at all (they are sealed and shown only as a hint).
pub const SUPPORT_VIEW_HIDDEN_FIELDS: &[&str] = &[
    "teams_webhook_url",
    "release_notes_email_webhook_url",
    "webhook_url",
    "external_id",
    "cfn_url",
    "forge_secret",
    "list_projects_url",
    "create_ticket_url",
    "add_comment_url",
    "receipt_handle",
    "api_key",
    "token",
    "secret",
    "password",
];
pub const HIDDEN: &str = "[hidden in support view]";

/// Replaces [`SUPPORT_VIEW_HIDDEN_FIELDS`] anywhere in an answer. A field
/// that is empty or null stays as it is, so pages still know it is unset.
pub fn hide_secrets(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                let set = !(val.is_null() || val.as_str() == Some(""));
                if set && val.is_string() && SUPPORT_VIEW_HIDDEN_FIELDS.contains(&k.as_str()) {
                    *val = Value::String(HIDDEN.into());
                } else {
                    hide_secrets(val);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(hide_secrets),
        _ => {}
    }
}

/// The only write a support view may make: leaving it.
pub fn is_exit(method: &Method, path: &str) -> bool {
    method == Method::DELETE && api_path(path) == "/support/view"
}

fn api_path(path: &str) -> &str {
    let p = path.strip_prefix("/api").filter(|p| p.starts_with('/'));
    p.unwrap_or(path).trim_end_matches('/')
}

fn path_matches(pattern: &str, path: &str) -> bool {
    let a: Vec<&str> = pattern.split('/').collect();
    let b: Vec<&str> = path.split('/').collect();
    a.len() == b.len() && a.iter().zip(&b).all(|(p, s)| *p == "*" || p == s)
}

/// Why a support-view session may not make this request, or `None` when it
/// may. Reads only, and not the reads in [`SUPPORT_VIEW_BLOCKED_READS`];
/// leaving the view is the one write allowed.
pub fn support_view_refusal(method: &Method, path: &str) -> Option<&'static str> {
    if is_exit(method, path) {
        return None;
    }
    if *method != Method::GET && *method != Method::HEAD {
        return Some(READ_ONLY);
    }
    let p = api_path(path);
    SUPPORT_VIEW_BLOCKED_READS
        .iter()
        .any(|pat| path_matches(pat, p))
        .then_some(OUTBOUND)
}

/// The claims a support-view request runs with: the team's, as an admin
/// role so its settings pages read, as no person on the team.
pub fn view_claims(sv: &SupportView) -> Claims {
    Claims {
        sub: format!("SUPPORT#{}", sv.grant_id),
        team_id: sv.team_id.clone(),
        email: "support@coderhelm.com".into(),
        role: "admin".into(),
        github_login: None,
        aud: SUPPORT_AUD.into(),
        iat: now_secs(),
        exp: sv.until,
        support_view: Some(sv.clone()),
    }
}

pub fn create_token(sv: &SupportView, secret: &str) -> Result<String, jsonwebtoken::errors::Error> {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &view_claims(sv),
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// A support-view token's view, or `None` if it is not a valid, unexpired
/// support-view token.
pub fn validate_token(token: &str, secret: &str) -> Option<SupportView> {
    let mut v = jsonwebtoken::Validation::default();
    v.set_audience(&[SUPPORT_AUD]);
    let data = jsonwebtoken::decode::<Claims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &v,
    )
    .ok()?;
    let sv = data.claims.support_view?;
    (data.claims.team_id == sv.team_id).then_some(sv)
}

fn cookie(value: &str, max_age: u64) -> String {
    format!(
        "{SUPPORT_COOKIE}={value}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={max_age}"
    )
}

pub fn clear_cookie() -> HeaderValue {
    HeaderValue::from_str(&cookie("", 0)).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// The 403 a support-view request gets, as JSON.
pub fn refused(msg: &str, ended: bool) -> Response {
    let mut r = (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": msg, "support_view": true, "support_view_ended": ended })),
    )
        .into_response();
    if ended {
        r.headers_mut().append(header::SET_COOKIE, clear_cookie());
    }
    r
}

// ── Grant cache ──────────────────────────────────────────────────────────

type Cache = Mutex<HashMap<String, (Instant, Option<Grant>)>>;

fn cache() -> &'static Cache {
    static C: OnceLock<Cache> = OnceLock::new();
    C.get_or_init(Default::default)
}

/// Drops this instance's cached grant for a team (after a change here).
pub fn forget(team_id: &str) {
    if let Ok(mut c) = cache().lock() {
        c.remove(team_id);
    }
}

/// The team's grant, read at most [`GRANT_CACHE`] ago.
pub async fn cached_grant<S: AccessStore>(
    store: &S,
    team_id: &str,
) -> Result<Option<Grant>, String> {
    if let Ok(c) = cache().lock() {
        if let Some((at, g)) = c.get(team_id) {
            if at.elapsed() < GRANT_CACHE {
                return Ok(g.clone());
            }
        }
    }
    let g = store.grant(team_id).await?;
    if let Ok(mut c) = cache().lock() {
        if c.len() > 5_000 {
            c.retain(|_, (at, _)| at.elapsed() < GRANT_CACHE);
        }
        c.insert(team_id.to_string(), (Instant::now(), g.clone()));
    }
    Ok(g)
}

/// Checks a support-view cookie for a request whose own session is
/// `session`: the token is valid, was opened by this same person, who is
/// still a platform admin, and the team's grant still allows it. The view to
/// run the request under, or `None` when the view has ended.
pub async fn check_session<S: AccessStore>(
    store: &S,
    session: &Claims,
    token: &str,
    secret: &str,
) -> Option<SupportView> {
    let sv = validate_token(token, secret)?;
    if sv.admin_sub != session.sub || !super::api::is_platform_admin(session) {
        return None;
    }
    match cached_grant(store, &sv.team_id).await {
        Ok(g) if session_ok(g.as_ref(), &sv, now_secs()) => Some(sv),
        Ok(_) => None,
        Err(e) => {
            // Fail closed: no grant read, no view.
            warn!(error = %e, "Support view: grant could not be read");
            None
        }
    }
}

// ── DynamoDB ─────────────────────────────────────────────────────────────

pub struct DynamoAccess<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

pub fn store(state: &AppState) -> DynamoAccess<'_> {
    DynamoAccess {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn err<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

fn data_of<T: for<'de> Deserialize<'de>>(item: &HashMap<String, AttributeValue>) -> Option<T> {
    item.get("data")
        .and_then(|v| v.as_s().ok())
        .and_then(|d| serde_json::from_str(d).ok())
}

impl AccessStore for DynamoAccess<'_> {
    async fn grant(&self, team_id: &str) -> Result<Option<Grant>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(ACCESS_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(data_of))
    }

    async fn put_grant(&self, g: &Grant) -> Result<(), String> {
        let data = serde_json::to_string(g).map_err(|e| e.to_string())?;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(&g.team_id))
            .item("sk", s(ACCESS_SK))
            .item("data", s(&data))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn audit(&self, team_id: &str, e: &AuditEntry) -> Result<(), String> {
        let data = serde_json::to_string(e).map_err(|e| e.to_string())?;
        let ttl = chrono::Utc::now().timestamp() + AUDIT_DAYS * 86_400;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item("sk", s(&format!("{AUDIT_PREFIX}{}#{}", e.at, e.id)))
            .item("data", s(&data))
            .item("ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn audit_log(&self, team_id: &str, limit: usize) -> Result<Vec<AuditEntry>, String> {
        let r = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(team_id))
            .expression_attribute_values(":p", s(AUDIT_PREFIX))
            .scan_index_forward(false)
            .limit(limit as i32)
            .send()
            .await
            .map_err(err)?;
        Ok(r.items().iter().filter_map(data_of).collect())
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────

fn grant_json(g: Option<&Grant>, now: u64) -> Value {
    match g {
        None => Value::Null,
        Some(g) => json!({
            "id": g.id,
            "granted_by_email": g.granted_by_email,
            "granted_at": g.granted_at,
            "until": iso(g.until),
            "note": g.note,
            "ticket_id": g.ticket_id,
            "revoked_at": g.revoked_at,
            "revoked_by_email": g.revoked_by_email,
            "active": g.active_at(now),
        }),
    }
}

/// GET /api/support/access — the team's grant and audit log (admin and owner).
pub async fn get_access(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    if crate::permissions::role_level(&caller.role) < 3 {
        return Err(
            Fail::Denied("Only a team admin or owner can see support access.".into()).reply(),
        );
    }
    let st = store(&state);
    let now = now_secs();
    let g = st
        .grant(&caller.team_id)
        .await
        .map_err(|e| Fail::Internal(e).reply())?;
    let log = st
        .audit_log(&caller.team_id, LOG_PAGE)
        .await
        .map_err(|e| Fail::Internal(e).reply())?;
    Ok(Json(json!({
        "grant": grant_json(g.as_ref(), now),
        "windows": WINDOWS_HOURS,
        "log": log,
    })))
}

/// POST /api/support/access — let support in: `{hours, note?, ticket_id?}`.
pub async fn grant_access(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(mut req): Json<GrantRequest>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    // A ticket named must be one of the team's own.
    let tickets = support::store(&state);
    if let Some(id) = req
        .ticket_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        support::open_for_team(&tickets, &caller, id)
            .await
            .map_err(Fail::reply)?;
        req.ticket_id = Some(id.to_string());
    } else {
        req.ticket_id = None;
    }
    let now = now_secs();
    let g = give(&store(&state), &caller, &req, now)
        .await
        .map_err(Fail::reply)?;
    info!(team = %caller.team_id, until = g.until, "Support access granted");
    if !g.ticket_id.is_empty() {
        let action = format!("support_access_granted:{}", iso(g.until));
        if let Err(e) = support::note_on_ticket(
            &tickets,
            &caller.team_id,
            &g.ticket_id,
            "customer",
            &caller,
            &action,
        )
        .await
        {
            warn!(error = ?e, "Support access: ticket note not written");
        }
    }
    Ok(Json(json!({ "grant": grant_json(Some(&g), now) })))
}

/// DELETE /api/support/access — end support access now.
pub async fn revoke_access(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let now = now_secs();
    let g = revoke(&store(&state), &caller, now)
        .await
        .map_err(Fail::reply)?;
    if let Some(g) = &g {
        info!(team = %caller.team_id, "Support access ended");
        if !g.ticket_id.is_empty() {
            let tickets = support::store(&state);
            let _ = support::note_on_ticket(
                &tickets,
                &caller.team_id,
                &g.ticket_id,
                "customer",
                &caller,
                "support_access_revoked",
            )
            .await;
        }
    }
    Ok(Json(json!({ "grant": grant_json(g.as_ref(), now) })))
}

#[derive(Debug, Deserialize)]
pub struct StartRequest {
    pub team_id: String,
    #[serde(default)]
    pub ticket_id: Option<String>,
}

/// POST /api/admin/support/view — a platform admin opens a read-only support
/// view of a team that let support in. Sets the support-view cookie.
pub async fn start_view(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<StartRequest>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let caller = Caller::from_claims(&claims);
    let team_id = req.team_id.trim();
    let ticket_id = req.ticket_id.as_deref().map(str::trim).unwrap_or("");
    let now = now_secs();
    let (g, sv) = start(&store(&state), &caller, team_id, ticket_id, now)
        .await
        .map_err(Fail::reply)?;
    let token = create_token(&sv, &state.secrets.jwt_secret)
        .map_err(|e| Fail::Internal(e.to_string()).reply())?;
    info!(team = %team_id, admin = %caller.email, "Support view opened");

    // The team's ticket thread says support looked: the ticket the access was
    // given for, and the ticket support opened it from.
    let tickets = support::store(&state);
    let mut noted: Vec<&str> = Vec::new();
    for id in [g.ticket_id.as_str(), ticket_id] {
        if id.is_empty() || noted.contains(&id) {
            continue;
        }
        noted.push(id);
        if let Err(e) =
            support::note_on_ticket(&tickets, team_id, id, "support", &caller, "support_viewed")
                .await
        {
            warn!(error = ?e, "Support view: ticket note not written");
        }
    }
    let name = support::team_name(&state, team_id).await;
    let mut r = Json(json!({
        "team_id": team_id,
        "team_name": name,
        "until": iso(sv.until),
        "grant_until": iso(g.until),
    }))
    .into_response();
    let max_age = sv.until.saturating_sub(now);
    if let Ok(v) = HeaderValue::from_str(&cookie(&token, max_age)) {
        r.headers_mut().append(header::SET_COOKIE, v);
    }
    Ok(r)
}

/// GET /api/support/view — whether this session is a support view, for the
/// banner.
pub async fn view_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let Some(sv) = &claims.support_view else {
        return Ok(Json(json!({ "active": false })));
    };
    let name = support::team_name(&state, &sv.team_id).await;
    Ok(Json(json!({
        "active": true,
        "team_id": sv.team_id,
        "team_name": name,
        "until": iso(sv.until),
        "read_only": true,
    })))
}

/// DELETE /api/support/view — leave the support view.
pub async fn exit_view(headers: HeaderMap) -> Response {
    let _ = headers;
    let mut r = Json(json!({ "active": false })).into_response();
    r.headers_mut().append(header::SET_COOKIE, clear_cookie());
    r
}

/// When each team's support access ends, for the admin queue (teams with no
/// access on are left out).
pub async fn active_until(state: &AppState, team_ids: &[String]) -> HashMap<String, String> {
    let st = store(state);
    let now = now_secs();
    let mut out = HashMap::new();
    let mut seen: Vec<&String> = Vec::new();
    for t in team_ids {
        if seen.contains(&t) {
            continue;
        }
        seen.push(t);
        if let Ok(Some(g)) = cached_grant(&st, t).await {
            if g.active_at(now) {
                out.insert(t.clone(), iso(g.until));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Mem {
        grants: Mutex<HashMap<String, Grant>>,
        log: Mutex<Vec<(String, AuditEntry)>>,
    }

    impl AccessStore for Mem {
        async fn grant(&self, team_id: &str) -> Result<Option<Grant>, String> {
            Ok(self.grants.lock().unwrap().get(team_id).cloned())
        }
        async fn put_grant(&self, g: &Grant) -> Result<(), String> {
            self.grants
                .lock()
                .unwrap()
                .insert(g.team_id.clone(), g.clone());
            Ok(())
        }
        async fn audit(&self, team_id: &str, e: &AuditEntry) -> Result<(), String> {
            self.log
                .lock()
                .unwrap()
                .push((team_id.to_string(), e.clone()));
            Ok(())
        }
        async fn audit_log(&self, team_id: &str, limit: usize) -> Result<Vec<AuditEntry>, String> {
            let mut v: Vec<AuditEntry> = self
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|(t, _)| t == team_id)
                .map(|(_, e)| e.clone())
                .collect();
            v.reverse();
            v.truncate(limit);
            Ok(v)
        }
    }

    impl Mem {
        fn kinds(&self, team: &str) -> Vec<String> {
            self.log
                .lock()
                .unwrap()
                .iter()
                .filter(|(t, _)| t == team)
                .map(|(_, e)| e.kind.clone())
                .collect()
        }
    }

    fn member(team: &str, role: &str) -> Caller {
        Caller {
            user_id: format!("USER#{role}-{team}"),
            team_id: format!("TEAM#{team}"),
            email: format!("{role}@{team}.example.com"),
            name: role.into(),
            role: role.into(),
            platform_admin: false,
            in_support_view: false,
        }
    }

    fn staff() -> Caller {
        Caller {
            platform_admin: true,
            ..member("staff", "member")
        }
    }

    fn ask(hours: u64) -> GrantRequest {
        GrantRequest {
            hours,
            note: Some("Look at run 7. token=abc123".into()),
            ticket_id: None,
        }
    }

    const NOW: u64 = 1_800_000_000;

    #[tokio::test]
    async fn only_team_admins_and_owners_can_grant_or_revoke() {
        let store = Mem::default();
        for role in ["member", "viewer", "billing"] {
            let c = member("alpha", role);
            assert!(matches!(
                give(&store, &c, &ask(24), NOW).await.unwrap_err(),
                Fail::Denied(_)
            ));
            assert!(matches!(
                revoke(&store, &c, NOW).await.unwrap_err(),
                Fail::Denied(_)
            ));
        }
        // A platform admin is not a team admin of another team.
        assert!(matches!(
            give(&store, &staff(), &ask(24), NOW).await.unwrap_err(),
            Fail::Denied(_)
        ));
        assert!(store.grants.lock().unwrap().is_empty());
        for role in ["admin", "owner"] {
            assert!(give(&store, &member("alpha", role), &ask(72), NOW)
                .await
                .is_ok());
        }
    }

    #[tokio::test]
    async fn grants_come_in_fixed_windows_and_hide_secrets_in_the_note() {
        let store = Mem::default();
        let admin = member("alpha", "admin");
        for bad in [0, 1, 48, 200] {
            assert!(matches!(
                give(&store, &admin, &ask(bad), NOW).await.unwrap_err(),
                Fail::BadRequest(_)
            ));
        }
        let g = give(&store, &admin, &ask(168), NOW).await.unwrap();
        assert_eq!(g.until, NOW + 7 * 86_400);
        assert!(!g.note.contains("abc123"));
        assert!(g.active_at(NOW));
        assert!(!g.active_at(g.until));
        assert_eq!(store.kinds("TEAM#alpha"), ["granted"]);
    }

    #[tokio::test]
    async fn a_platform_admin_without_a_grant_is_refused() {
        let store = Mem::default();
        let err = start(&store, &staff(), "TEAM#alpha", "", NOW)
            .await
            .unwrap_err();
        assert!(matches!(err, Fail::Denied(_)));
        // Nor does a team admin's own power open a view.
        give(&store, &member("alpha", "owner"), &ask(24), NOW)
            .await
            .unwrap();
        assert_eq!(
            start(&store, &member("alpha", "owner"), "TEAM#alpha", "", NOW)
                .await
                .unwrap_err(),
            Fail::Forbidden
        );
        // Another team's grant does not open this one.
        assert!(start(&store, &staff(), "TEAM#beta", "", NOW).await.is_err());
        // Expired.
        assert!(start(&store, &staff(), "TEAM#alpha", "", NOW + 86_400)
            .await
            .is_err());
        // In time.
        let (_, sv) = start(&store, &staff(), "TEAM#alpha", "T1", NOW)
            .await
            .unwrap();
        assert_eq!(sv.team_id, "TEAM#alpha");
        assert_eq!(sv.until, NOW + SESSION_MAX_SECS);
        assert_eq!(sv.admin_sub, staff().user_id);
    }

    #[tokio::test]
    async fn revoking_expiry_or_a_new_grant_ends_open_sessions() {
        let store = Mem::default();
        let owner = member("alpha", "owner");
        let g = give(&store, &owner, &ask(24), NOW).await.unwrap();
        let (_, sv) = start(&store, &staff(), "TEAM#alpha", "", NOW)
            .await
            .unwrap();
        let current = |s: &Mem| s.grants.lock().unwrap().get("TEAM#alpha").cloned();
        assert!(session_ok(current(&store).as_ref(), &sv, NOW + 60));
        // The session's own limit.
        assert!(!session_ok(current(&store).as_ref(), &sv, sv.until));
        // A grant shorter than the session limit ends it.
        let short = SupportView {
            until: g.until + 10,
            ..sv.clone()
        };
        assert!(!session_ok(current(&store).as_ref(), &short, g.until));

        // Revoked: over, even inside the window.
        let ended = revoke(&store, &owner, NOW + 120).await.unwrap().unwrap();
        assert!(!ended.revoked_at.is_empty());
        assert!(!session_ok(current(&store).as_ref(), &sv, NOW + 121));
        assert!(start(&store, &staff(), "TEAM#alpha", "", NOW + 130)
            .await
            .is_err());
        // Revoking again is a no-op.
        assert!(revoke(&store, &owner, NOW + 140).await.unwrap().is_none());

        // A new grant does not revive the old session.
        give(&store, &owner, &ask(24), NOW + 200).await.unwrap();
        assert!(!session_ok(current(&store).as_ref(), &sv, NOW + 201));
        // No grant at all.
        assert!(!session_ok(None, &sv, NOW));
        assert_eq!(
            store.kinds("TEAM#alpha"),
            ["granted", "session_start", "revoked", "granted"]
        );
    }

    #[tokio::test]
    async fn the_audit_log_records_the_start_and_every_request() {
        let store = Mem::default();
        give(&store, &member("alpha", "admin"), &ask(24), NOW)
            .await
            .unwrap();
        let (_, sv) = start(&store, &staff(), "TEAM#alpha", "T9", NOW)
            .await
            .unwrap();
        record(
            &store,
            &sv,
            "read",
            &Method::GET,
            "/runs?status=failed",
            NOW + 5,
        )
        .await
        .unwrap();
        record(
            &store,
            &sv,
            "refused",
            &Method::POST,
            "/runs/R1/retry",
            NOW + 6,
        )
        .await
        .unwrap();
        let log = store.audit_log("TEAM#alpha", 10).await.unwrap();
        assert_eq!(log.len(), 4);
        let start_entry = log.iter().find(|e| e.kind == "session_start").unwrap();
        assert_eq!(start_entry.actor_email, staff().email);
        assert_eq!(start_entry.ticket_id, "T9");
        let read = log.iter().find(|e| e.kind == "read").unwrap();
        assert_eq!(read.route, "/runs?status=failed");
        assert_eq!(read.method, "GET");
        assert_eq!(read.actor_email, staff().email);
        // Nothing lands in another team's log.
        assert!(store.audit_log("TEAM#staff", 10).await.unwrap().is_empty());
    }

    #[test]
    fn answers_lose_their_credentials_in_a_support_view() {
        let mut v = json!({
            "routes": [{ "topic": "t", "teams_webhook_url": "https://hooks.example.com/abc" }],
            "connections": [{ "role_arn": "arn:aws:iam::1:role/r", "external_id": "ext-123", "key_hint": "…9f3a" }],
            "forge_secret": "s3cret",
            "unset": { "webhook_url": "" },
            "count": 3,
        });
        hide_secrets(&mut v);
        let text = v.to_string();
        for gone in ["hooks.example.com", "ext-123", "s3cret"] {
            assert!(!text.contains(gone), "{gone} survived: {text}");
        }
        assert_eq!(v["routes"][0]["topic"], "t");
        assert_eq!(v["connections"][0]["key_hint"], "…9f3a");
        assert_eq!(v["unset"]["webhook_url"], "");
        assert_eq!(v["count"], 3);
    }

    #[test]
    fn support_tokens_are_not_sessions_and_sessions_are_not_support_tokens() {
        let secret = "test-jwt-secret-at-least-32-chars!";
        let sv = SupportView {
            team_id: "TEAM#alpha".into(),
            until: now_secs() + 600,
            grant_id: "G1".into(),
            admin_sub: "USER#staff".into(),
            admin_email: "helper@example.com".into(),
        };
        let token = create_token(&sv, secret).unwrap();
        assert_eq!(validate_token(&token, secret), Some(sv.clone()));
        // Never accepted as a normal dashboard session.
        assert!(crate::auth::jwt::validate_token(&token, secret).is_err());
        // And a normal session is no support token.
        let session = crate::auth::jwt::create_token(
            "USER#staff",
            "TEAM#alpha",
            "helper@example.com",
            "owner",
            None,
            secret,
            600,
        )
        .unwrap();
        assert!(validate_token(&session, secret).is_none());
        // Wrong key.
        assert!(validate_token(&token, "another-secret-of-at-least-32-chars").is_none());
        // Expired.
        let old = create_token(
            &SupportView {
                until: now_secs().saturating_sub(120),
                ..sv
            },
            secret,
        )
        .unwrap();
        assert!(validate_token(&old, secret).is_none());
    }

    #[test]
    fn view_claims_are_the_teams_and_no_ones() {
        let sv = SupportView {
            team_id: "TEAM#alpha".into(),
            until: 10,
            grant_id: "G1".into(),
            admin_sub: "USER#staff".into(),
            admin_email: "admin@coderhelm.com".into(),
        };
        let c = view_claims(&sv);
        assert_eq!(c.team_id, "TEAM#alpha");
        assert!(!c.sub.starts_with("USER#"));
        // Even a listed platform admin's address does not make the view an admin.
        assert!(!crate::routes::api::is_platform_admin(&Claims {
            email: "admin@coderhelm.com".into(),
            ..c
        }));
    }

    /// Every route of the protected API router with its methods, read from
    /// `main.rs` so a route added later is covered without editing this test.
    fn api_routes() -> Vec<(String, Vec<Method>)> {
        let src = include_str!("../main.rs");
        let start = src.find("let api_routes").expect("api_routes in main.rs");
        let end = start
            + src[start..]
                .find("middleware::auth::require_auth")
                .expect("require_auth layer");
        let body = &src[start..end];
        let mut out = Vec::new();
        let mut rest = body;
        while let Some(i) = rest.find(".route(") {
            rest = &rest[i + ".route(".len()..];
            // The call's text, up to its closing parenthesis.
            let mut depth = 1;
            let mut j = 0;
            for (k, ch) in rest.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            j = k;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let call = &rest[..j];
            let q1 = call.find('"').expect("path literal");
            let q2 = q1 + 1 + call[q1 + 1..].find('"').expect("path end");
            let path = call[q1 + 1..q2].to_string();
            let handlers = &call[q2 + 1..];
            let mut methods = Vec::new();
            for (word, m) in [
                ("get(", Method::GET),
                ("post(", Method::POST),
                ("put(", Method::PUT),
                ("delete(", Method::DELETE),
                ("patch(", Method::PATCH),
            ] {
                let found = handlers.match_indices(word).any(|(at, _)| {
                    at == 0 || !handlers[..at].ends_with(|c: char| c.is_alphanumeric() || c == '_')
                });
                if found {
                    methods.push(m);
                }
            }
            assert!(!methods.is_empty(), "no methods read for {path}");
            out.push((path, methods));
            rest = &rest[j..];
        }
        out
    }

    /// A concrete path for a route pattern: `:param` becomes a value.
    fn concrete(path: &str) -> String {
        path.split('/')
            .map(|seg| if seg.starts_with(':') { "x1" } else { seg })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn a_support_view_is_refused_on_every_write_route() {
        let routes = api_routes();
        assert!(
            routes.len() > 150,
            "router read from main.rs: {}",
            routes.len()
        );
        let mut writes = 0;
        for (path, methods) in &routes {
            for m in methods {
                if *m == Method::GET {
                    continue;
                }
                let p = concrete(path);
                if is_exit(m, &p) {
                    continue;
                }
                writes += 1;
                for full in [p.clone(), format!("/api{p}")] {
                    assert_eq!(
                        support_view_refusal(m, &full),
                        Some(READ_ONLY),
                        "{m} {full} must be refused in a support view"
                    );
                }
            }
        }
        assert!(writes > 100, "write routes found: {writes}");
        // The routes that start runs, reviews, merges and emails are among them.
        for (m, p) in [
            (Method::POST, "/runs/x1/retry"),
            (Method::POST, "/reviewer/re-review"),
            (Method::POST, "/reviewer/gate"),
            (Method::POST, "/releases/send"),
            (Method::POST, "/plans/x1/execute"),
            (Method::POST, "/support/tickets"),
            (Method::POST, "/users/invite"),
            (Method::POST, "/admin/support/view"),
            (Method::POST, "/support/access"),
            (Method::DELETE, "/support/access"),
            (Method::PATCH, "/runs"),
            (Method::OPTIONS, "/runs"),
        ] {
            assert_eq!(support_view_refusal(&m, p), Some(READ_ONLY), "{m} {p}");
        }
        // The exit is a route, and the one write allowed.
        assert!(routes
            .iter()
            .any(|(p, ms)| p == "/support/view" && ms.contains(&Method::DELETE)));
        assert_eq!(
            support_view_refusal(&Method::DELETE, "/api/support/view"),
            None
        );
    }

    #[test]
    fn reads_pass_except_those_that_reach_the_teams_own_systems() {
        let routes = api_routes();
        for (path, methods) in &routes {
            if !methods.contains(&Method::GET) {
                continue;
            }
            let p = concrete(path);
            let blocked = SUPPORT_VIEW_BLOCKED_READS
                .iter()
                .any(|pat| path_matches(pat, &p));
            assert_eq!(
                support_view_refusal(&Method::GET, &p).is_some(),
                blocked,
                "GET {p}"
            );
        }
        // Every blocked read names a real route.
        for pat in SUPPORT_VIEW_BLOCKED_READS {
            assert!(
                routes
                    .iter()
                    .any(|(p, ms)| ms.contains(&Method::GET) && path_matches(pat, &concrete(p))),
                "{pat} is not a GET route"
            );
        }
        assert_eq!(support_view_refusal(&Method::GET, "/api/runs"), None);
        assert_eq!(support_view_refusal(&Method::HEAD, "/runs/x1"), None);
        assert!(support_view_refusal(&Method::GET, "/api/aws-connections/c1/log-groups").is_some());
    }
}
