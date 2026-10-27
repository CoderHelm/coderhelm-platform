//! Who may join a self-hosted install, and the two-factor rule.
//!
//! A self-hosted install has exactly one organization: one team, owned by the
//! first admin from setup. Nobody can make another one, and nobody signs up
//! on their own. A person joins only with an admin's invite, or with an email
//! at a domain the admins allow. Every way in goes through [`decide_join`]:
//! email sign-up, the email sign-in that makes a person's first record,
//! Google, GitHub, and the Cognito pre-sign-up trigger
//! (`lambda/pre-signup`, which reads the same cases from
//! `lambda/pre-signup/cases.json`).
//!
//! Storage, in the settings table:
//! - `pk = ORG`, `sk = META`: the organization (its team, the allowed
//!   domains, whether two-factor sign-in is required and since when).
//! - `pk = ALLOWLIST`, `sk = EMAIL#...`: the install config's first-boot
//!   list. The first admin comes from it; when the organization is made, its
//!   other addresses become invites and its domains the allowed domains.
//!   After that, admins change both in Settings, Sign-in.
//!
//! Two-factor: password accounts use Cognito's authenticator-app (TOTP) MFA.
//! Google and GitHub sign-ins never pass through Cognito MFA, so CoderHelm
//! asks for its own authenticator-app code after them (secret sealed with
//! the install's KMS key on the member record). The session token records
//! whether its sign-in passed a second factor ([`SignInProof`]); with the
//! rule on, the auth middleware lets a session without one reach only the
//! screens that set it up ([`gate`]). The rule is read on every request, so
//! sessions from before it was turned on are held at their next request. API
//! keys are not people and are not affected.
//!
//! The hosted deployment keeps its behavior: none of this applies there.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Extension, Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::jwt::{self, SecondStep, SignInProof};
use crate::middleware::auth::SESSION_TTL_SECS;
use crate::models::Claims;
use crate::routes::auth::normalize_email;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

pub const ORG_PK: &str = "ORG";
pub const ORG_SK: &str = "META";
/// Most domains an organization may allow.
pub const MAX_DOMAINS: usize = 20;
/// The pending second step after a Google or GitHub sign-in.
pub const SECOND_STEP_COOKIE: &str = "__Host-coderhelm_2fa";
const SECOND_STEP_SECS: u64 = 300;

/// The words a refused person reads.
pub const NOT_INVITED: &str =
    "This CoderHelm is private to one organization. Ask one of its admins to invite you.";

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn fail_code(code: StatusCode, msg: &str, key: &str) -> ApiError {
    (code, Json(json!({ "error": msg, "code": key })))
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
        .map_or("", String::as_str)
}

/// Admins and owners only.
fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|_| fail(StatusCode::FORBIDDEN, "Only admins and owners can do that."))
}

/// Only on a self-hosted install.
pub fn applies() -> bool {
    super::setup_checks::self_hosted()
}

// ── The organization ────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Org {
    pub team_id: String,
    /// Email domains whose people may join, lowercase.
    pub domains: Vec<String>,
    pub require_2fa: bool,
    /// When the rule was last turned on.
    pub require_2fa_since: Option<String>,
}

impl Org {
    pub fn from_item(item: &Item) -> Option<Org> {
        let team_id = s(item, "team_id");
        if team_id.is_empty() {
            return None;
        }
        let domains = item
            .get("domains")
            .and_then(|v| v.as_l().ok())
            .map(|l| {
                l.iter()
                    .filter_map(|d| d.as_s().ok())
                    .map(|d| d.to_lowercase())
                    .collect()
            })
            .unwrap_or_default();
        Some(Org {
            team_id: team_id.to_string(),
            domains,
            require_2fa: item
                .get("require_2fa")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            require_2fa_since: Some(s(item, "require_2fa_since"))
                .filter(|v| !v.is_empty())
                .map(str::to_string),
        })
    }
}

/// The organization record, if there is one.
pub async fn load_org(state: &AppState) -> Result<Option<Org>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(ORG_PK))
        .key("sk", attr_s(ORG_SK))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.item().and_then(Org::from_item))
}

// ── Who may join ────────────────────────────────────────────────────

/// What a person's join is decided from.
#[derive(Debug, Clone, Default)]
pub struct JoinFacts {
    pub email: String,
    pub org: Option<Org>,
    /// Their records by email (users table, GSI2).
    pub records: Vec<Item>,
    /// On the config list as an admin (an exact address not marked as an
    /// invite): may become the first admin while nobody has signed up.
    pub admin_listed: bool,
    /// On the config list at all (address or domain).
    pub listed: bool,
    /// Someone already has an account on this install.
    pub anyone: bool,
}

/// The decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Join {
    /// Already in the organization: this record.
    Member {
        team_id: String,
        user_id: String,
    },
    /// A live invite to the organization.
    Invite,
    /// Their email domain is allowed.
    Domain,
    /// The first admin: makes the organization.
    FirstAdmin,
    /// An install from before the organization existed: on the config list.
    /// The organization is made at their first sign-in.
    Listed,
    Refused,
}

impl Join {
    pub fn allowed(&self) -> bool {
        *self != Join::Refused
    }
    pub fn key(&self) -> &'static str {
        match self {
            Join::Member { .. } => "existing",
            Join::Invite => "invite",
            Join::Domain => "domain",
            Join::FirstAdmin => "first_admin",
            Join::Listed => "listed",
            Join::Refused => "refused",
        }
    }
}

/// The part of an email after the `@`, lowercase.
pub fn email_domain(email: &str) -> Option<String> {
    let (local, domain) = email.trim().rsplit_once('@')?;
    if local.is_empty() || domain.is_empty() || !domain.contains('.') {
        return None;
    }
    Some(domain.to_lowercase())
}

fn is_invite(item: &Item) -> bool {
    s(item, "status") == "invited"
}

fn is_user(item: &Item) -> bool {
    s(item, "sk").starts_with("USER#")
}

/// The one place a join is decided. Pure: the caller gathers the facts.
pub fn decide_join(f: &JoinFacts, now: chrono::DateTime<chrono::Utc>) -> Join {
    match &f.org {
        Some(org) => {
            let in_org = |i: &&Item| is_user(i) && s(i, "pk") == org.team_id;
            if let Some(m) = f.records.iter().filter(in_org).find(|i| !is_invite(i)) {
                return Join::Member {
                    team_id: org.team_id.clone(),
                    user_id: s(m, "sk").to_string(),
                };
            }
            if f.records
                .iter()
                .filter(in_org)
                .any(|i| is_invite(i) && !super::users::invite_expired(i, now))
            {
                return Join::Invite;
            }
            match email_domain(&f.email) {
                Some(d) if org.domains.contains(&d) => Join::Domain,
                _ => Join::Refused,
            }
        }
        None if !f.anyone => {
            if f.admin_listed {
                Join::FirstAdmin
            } else {
                Join::Refused
            }
        }
        None => {
            if let Some(m) = f.records.iter().find(|i| is_user(i) && !is_invite(i)) {
                Join::Member {
                    team_id: s(m, "pk").to_string(),
                    user_id: s(m, "sk").to_string(),
                }
            } else if f.listed {
                Join::Listed
            } else {
                Join::Refused
            }
        }
    }
}

/// The config list's word on an email: (admin_listed, listed).
async fn config_listing(state: &AppState, email: &str) -> (bool, bool) {
    let get = |sk: String| async move {
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s("ALLOWLIST"))
            .key("sk", attr_s(&sk))
            .send()
            .await
            .ok()
            .and_then(|r| r.item)
    };
    let email = normalize_email(email);
    let exact = get(format!("EMAIL#{email}")).await;
    let admin = exact.as_ref().is_some_and(|i| s(i, "kind") != "invite");
    let domain = match email_domain(&email) {
        Some(d) => get(format!("EMAIL#*@{d}")).await.is_some(),
        None => false,
    };
    (admin, exact.is_some() || domain)
}

async fn records_by_email(state: &AppState, email: &str) -> Result<Vec<Item>, String> {
    state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi2")
        .key_condition_expression("gsi2pk = :pk")
        .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", normalize_email(email))))
        .send()
        .await
        .map(|r| r.items().to_vec())
        .map_err(|e| format!("{e}"))
}

async fn anyone_signed_up(state: &AppState) -> bool {
    !super::setup_checks::setup_open(state).await
}

/// The facts for `email`, with the organization made first on an install
/// from before it existed (see [`ensure_org`]).
pub async fn facts_for(state: &AppState, email: &str) -> Result<JoinFacts, String> {
    let anyone = anyone_signed_up(state).await;
    let org = if anyone {
        ensure_org(state).await?
    } else {
        load_org(state).await?
    };
    let (admin_listed, listed) = config_listing(state, email).await;
    Ok(JoinFacts {
        email: email.to_string(),
        org,
        records: if email.is_empty() {
            Vec::new()
        } else {
            records_by_email(state, email).await?
        },
        admin_listed,
        listed,
        anyone,
    })
}

/// Whether `email` may make an account (`POST /auth/signup`).
pub async fn may_sign_up(state: &AppState, email: &str) -> Result<Join, String> {
    let facts = facts_for(state, email).await?;
    Ok(decide_join(&facts, chrono::Utc::now()))
}

/// The organization, made from the oldest team on an install that has people
/// but no organization record yet (an install from before this existed).
pub async fn ensure_org(state: &AppState) -> Result<Option<Org>, String> {
    if let Some(org) = load_org(state).await? {
        return Ok(Some(org));
    }
    let mut oldest: Option<(String, String)> = None;
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .scan()
            .table_name(&state.config.teams_table_name)
            .filter_expression("sk = :m")
            .expression_attribute_values(":m", attr_s("META"))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("{e}"))?;
        for i in out.items() {
            let key = (s(i, "created_at").to_string(), s(i, "team_id").to_string());
            if key.1.is_empty() {
                continue;
            }
            if oldest.as_ref().is_none_or(|o| key < *o) {
                oldest = Some(key);
            }
        }
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    let Some((_, team_id)) = oldest else {
        return Ok(None);
    };
    info!(%team_id, "Making the organization from the oldest team");
    claim_org(state, &team_id, "adopted", "").await
}

/// Writes the organization record once; whoever comes second reads the
/// first one's. The config's domains, invite list and two-factor default
/// come along (first boot only).
async fn claim_org(
    state: &AppState,
    team_id: &str,
    how: &str,
    creator_email: &str,
) -> Result<Option<Org>, String> {
    let seed = config_seed(state).await;
    let now = chrono::Utc::now().to_rfc3339();
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(ORG_PK))
        .item("sk", attr_s(ORG_SK))
        .item("team_id", attr_s(team_id))
        .item(
            "domains",
            AttributeValue::L(seed.domains.iter().map(|d| attr_s(d)).collect()),
        )
        .item("require_2fa", AttributeValue::Bool(seed.require_2fa))
        .item("created_at", attr_s(&now))
        .item("created_how", attr_s(how))
        .condition_expression("attribute_not_exists(pk)");
    if seed.require_2fa {
        put = put.item("require_2fa_since", attr_s(&now));
    }
    match put.send().await {
        Ok(_) => {
            seed_invites(state, team_id, &seed, creator_email).await;
            load_org(state).await
        }
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            load_org(state).await
        }
        Err(e) => Err(format!("{e}")),
    }
}

/// The install config's first-boot list.
#[derive(Debug, Default)]
struct Seed {
    domains: Vec<String>,
    /// (email, role)
    invites: Vec<(String, String)>,
    require_2fa: bool,
}

async fn config_seed(state: &AppState) -> Seed {
    let items = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s("ALLOWLIST"))
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    seed_from(&items)
}

fn seed_from(items: &[Item]) -> Seed {
    let mut seed = Seed::default();
    for i in items {
        let sk = s(i, "sk");
        if sk == "TWO_FACTOR" {
            seed.require_2fa = i
                .get("require")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
        } else if let Some(d) = sk.strip_prefix("EMAIL#*@") {
            if valid_domain(d) {
                seed.domains.push(d.to_lowercase());
            }
        } else if let Some(e) = sk.strip_prefix("EMAIL#") {
            let role = if s(i, "kind") == "invite" {
                "member"
            } else {
                "admin"
            };
            seed.invites.push((e.to_lowercase(), role.to_string()));
        }
    }
    seed.domains.sort();
    seed.domains.dedup();
    seed
}

/// The config's other addresses as invites (no email is sent; an admin can
/// resend from the team page).
async fn seed_invites(state: &AppState, team_id: &str, seed: &Seed, creator_email: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let creator = normalize_email(creator_email);
    for (email, role) in &seed.invites {
        if normalize_email(email) == creator {
            continue;
        }
        let gsi2pk = format!("EMAIL#{}", normalize_email(email));
        // Someone with a record in the organization already needs no invite.
        let has = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .index_name("gsi2")
            .key_condition_expression("gsi2pk = :pk AND gsi2sk = :t")
            .expression_attribute_values(":pk", attr_s(&gsi2pk))
            .expression_attribute_values(":t", attr_s(team_id))
            .send()
            .await
            .map(|r| r.count() > 0)
            .unwrap_or(true);
        if has {
            continue;
        }
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.users_table_name)
            .item("pk", attr_s(team_id))
            .item("sk", attr_s(&format!("USER#invite_{}", ulid::Ulid::new())))
            .item("email", attr_s(email))
            .item("role", attr_s(role))
            .item("status", attr_s("invited"))
            .item("avatar_url", attr_s(""))
            .item("updated_at", attr_s(&now))
            .item("invited_at", attr_s(&now))
            .item("invited_by", attr_s("config"))
            .item("gsi2pk", attr_s(&gsi2pk))
            .item("gsi2sk", attr_s(team_id))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
    }
}

/// The first admin's sign-in: the organization's team, its META records and
/// their owner record. Returns the team and the user id.
pub async fn create_org(
    state: &AppState,
    user_id: &str,
    email: &str,
    name: &str,
    avatar: &str,
) -> Result<Option<String>, String> {
    let team_id = format!("TEAM#{}", ulid::Ulid::new().to_string().to_lowercase());
    let Some(org) = claim_org(state, &team_id, "first_admin", email).await? else {
        return Ok(None);
    };
    if org.team_id != team_id {
        // Someone else made it first.
        return Ok(None);
    }
    let now = chrono::Utc::now().to_rfc3339();
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
    state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .item("pk", attr_s(&team_id))
        .item("sk", attr_s(user_id))
        .item("email", attr_s(email))
        .item("role", attr_s("owner"))
        .item("status", attr_s("active"))
        .item("name", attr_s(name))
        .item("avatar_url", attr_s(avatar))
        .item("updated_at", attr_s(&now))
        .item("created_at", attr_s(&now))
        .item(
            "gsi2pk",
            attr_s(&format!("EMAIL#{}", normalize_email(email))),
        )
        .item("gsi2sk", attr_s(&team_id))
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    info!(%team_id, "First admin made the organization");
    Ok(Some(team_id))
}

/// Someone whose email domain is allowed: a member record in the
/// organization. Returns their record.
pub async fn join_by_domain(
    state: &AppState,
    org: &Org,
    user_id: &str,
    email: &str,
    name: &str,
    avatar: &str,
) -> Result<Item, String> {
    let now = chrono::Utc::now().to_rfc3339();
    let record = HashMap::from([
        ("pk".to_string(), attr_s(&org.team_id)),
        ("sk".to_string(), attr_s(user_id)),
        ("email".to_string(), attr_s(email)),
        ("role".to_string(), attr_s("member")),
        ("status".to_string(), attr_s("active")),
        ("name".to_string(), attr_s(name)),
        ("avatar_url".to_string(), attr_s(avatar)),
        ("updated_at".to_string(), attr_s(&now)),
        ("created_at".to_string(), attr_s(&now)),
        ("invited_by".to_string(), attr_s("domain")),
        (
            "gsi2pk".to_string(),
            attr_s(&format!("EMAIL#{}", normalize_email(email))),
        ),
        ("gsi2sk".to_string(), attr_s(&org.team_id)),
    ]);
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.users_table_name)
        .set_item(Some(record.clone()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    match put {
        Ok(_) => {
            info!(team_id = %org.team_id, "Joined the organization by email domain");
            Ok(record)
        }
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            Ok(record)
        }
        Err(e) => Err(format!("{e}")),
    }
}

// ── Domains ─────────────────────────────────────────────────────────

/// Email services anyone can sign up for: allowing one would let anyone in.
const PUBLIC_MAIL: &[&str] = &[
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "gmx.com",
    "mail.com",
    "yandex.com",
    "zoho.com",
];

pub fn valid_domain(d: &str) -> bool {
    let d = d.trim();
    if d.len() > 253 || !d.contains('.') {
        return false;
    }
    d.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    }) && d
        .rsplit('.')
        .next()
        .is_some_and(|tld| tld.len() >= 2 && tld.chars().all(|c| c.is_ascii_lowercase()))
}

/// The domains an admin typed, cleaned and checked.
pub fn clean_domains(input: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for raw in input {
        let d = raw.trim().trim_start_matches('@').to_lowercase();
        if d.is_empty() {
            continue;
        }
        if !valid_domain(&d) {
            return Err(format!(
                "\"{raw}\" isn't an email domain such as example.com."
            ));
        }
        if PUBLIC_MAIL.contains(&d.as_str()) {
            return Err(format!(
                "{d} is a public email service: allowing it would let anyone join. Invite those people instead."
            ));
        }
        if !out.contains(&d) {
            out.push(d);
        }
    }
    if out.len() > MAX_DOMAINS {
        return Err(format!("Allow at most {MAX_DOMAINS} domains."));
    }
    out.sort();
    Ok(out)
}

// ── Two-factor: the request gate ────────────────────────────────────

/// What a session without a second factor may still reach while the rule is
/// on: who they are, and the screens that set up a second factor.
pub const SETUP_HANDLERS: &[&str] = &[
    "api::me",
    "sign_in::my_status",
    "users::mfa_setup",
    "users::mfa_verify_setup",
    "sign_in::app_setup",
    "sign_in::app_verify",
];

/// May this session make this request?
pub fn gate(require_2fa: bool, proof: &SignInProof, handler: Option<&str>) -> bool {
    !require_2fa || proof.mfa || handler.is_some_and(|h| SETUP_HANDLERS.contains(&h))
}

/// The answer to a held session.
pub fn two_factor_required() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "Your organization requires two-factor sign-in. Set it up to continue.",
            "code": "two_factor_required",
        })),
    )
        .into_response()
}

/// The middleware's check for one session request on a self-hosted install.
/// The rule is read fresh each time; when it can't be read the request is
/// refused (the safe answer).
pub async fn check_session(
    state: &AppState,
    proof: &SignInProof,
    handler: Option<&str>,
) -> Result<(), Response> {
    if proof.mfa {
        return Ok(());
    }
    let require = match load_org(state).await {
        Ok(org) => org.is_some_and(|o| o.require_2fa),
        Err(e) => {
            error!(error = %e, "Two-factor rule not read; request refused");
            return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
    };
    if gate(require, proof, handler) {
        Ok(())
    } else {
        Err(two_factor_required())
    }
}

/// GET /api/users/2fa — this person's two-factor sign-in: whether the
/// organization requires it, whether this session is held until it is set
/// up, which kind they set up (`app`, `cognito` or none), and which kind fits
/// how they signed in (`cognito` after a password, `app` after Google or
/// GitHub).
pub async fn my_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
) -> Result<Json<Value>, ApiError> {
    if !applies() {
        return Ok(Json(
            json!({ "self_hosted": false, "required": false, "held": false }),
        ));
    }
    let required = load_org(&state)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .is_some_and(|o| o.require_2fa);
    let passed = proof.as_ref().is_some_and(|p| p.mfa);
    let via = via_of(proof.as_ref());
    let item = member(&state, &claims.team_id, &claims.sub)
        .await
        .map_err(|e| internal("Member not read", e))?;
    let factor = match &item {
        Some(i) => factor_of(&state, i).await,
        None => None,
    };
    Ok(Json(json!({
        "self_hosted": true,
        "required": required,
        "held": required && !passed,
        "session_has_2fa": passed,
        "signed_in_with": via,
        "set_up": factor,
        "fits": if via == "password" { "cognito" } else { "app" },
    })))
}

// ── Two-factor: authenticator-app codes (RFC 6238) ──────────────────

const B32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

pub fn base32(bytes: &[u8]) -> String {
    let mut out = String::new();
    let (mut buf, mut bits) = (0u32, 0u32);
    for &b in bytes {
        buf = (buf << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            out.push(B32[((buf >> (bits - 5)) & 31) as usize] as char);
            bits -= 5;
        }
    }
    if bits > 0 {
        out.push(B32[((buf << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// The six-digit code for a 30-second time step.
pub fn totp_at(secret: &[u8], step: u64) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret).expect("HMAC key length");
    mac.update(&step.to_be_bytes());
    let h = mac.finalize().into_bytes();
    let off = (h[19] & 0x0f) as usize;
    let bin = (u32::from(h[off] & 0x7f) << 24)
        | (u32::from(h[off + 1]) << 16)
        | (u32::from(h[off + 2]) << 8)
        | u32::from(h[off + 3]);
    format!("{:06}", bin % 1_000_000)
}

/// The time step a code matches (this step or one either side), newer than
/// `last_used` so a code works once. `None` when it doesn't match.
pub fn totp_check(secret: &[u8], code: &str, unix: u64, last_used: u64) -> Option<u64> {
    let code: String = code.chars().filter(|c| !c.is_whitespace()).collect();
    if code.len() != 6 || !code.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let now = unix / 30;
    [now.saturating_sub(1), now, now + 1]
        .into_iter()
        .filter(|st| *st > last_used)
        .find(|st| {
            use subtle::ConstantTimeEq;
            totp_at(secret, *st)
                .as_bytes()
                .ct_eq(code.as_bytes())
                .into()
        })
}

fn otpauth_uri(email: &str, secret_b32: &str) -> String {
    format!(
        "otpauth://totp/CoderHelm:{}?secret={secret_b32}&issuer=CoderHelm",
        urlencoding::encode(email)
    )
}

fn cipher(state: &AppState) -> common::mcp_credentials::Kms<'_> {
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

fn seal_context(team_id: &str, user_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("user".to_string(), user_id.to_string()),
        ("purpose".to_string(), "two_factor".to_string()),
    ])
}

async fn seal(state: &AppState, team: &str, user: &str, secret: &[u8]) -> Result<String, String> {
    use base64::Engine;
    use common::mcp_credentials::Cipher;
    let blob = cipher(state)
        .encrypt(seal_context(team, user), secret)
        .await?;
    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

async fn unseal(state: &AppState, team: &str, user: &str, sealed: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    use common::mcp_credentials::Cipher;
    let blob = base64::engine::general_purpose::STANDARD
        .decode(sealed)
        .map_err(|e| e.to_string())?;
    cipher(state).decrypt(seal_context(team, user), &blob).await
}

async fn member(state: &AppState, team_id: &str, user_id: &str) -> Result<Option<Item>, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(user_id))
        .consistent_read(true)
        .send()
        .await
        .map(|r| r.item)
        .map_err(|e| format!("{e}"))
}

fn app_enabled(item: &Item) -> bool {
    item.get("totp_enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
        && !s(item, "totp_sealed").is_empty()
}

fn last_step(item: &Item) -> u64 {
    item.get("totp_last_step")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Checks an authenticator-app code against the member's own secret and uses
/// it up (a code works once).
async fn use_app_code(state: &AppState, item: &Item, code: &str) -> Result<bool, String> {
    let (team, user) = (s(item, "pk"), s(item, "sk"));
    let secret = unseal(state, team, user, s(item, "totp_sealed")).await?;
    let unix = chrono::Utc::now().timestamp().max(0) as u64;
    let Some(step) = totp_check(&secret, code, unix, last_step(item)) else {
        return Ok(false);
    };
    let used = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(user))
        .update_expression("SET totp_last_step = :st")
        .condition_expression("attribute_not_exists(totp_last_step) OR totp_last_step < :st")
        .expression_attribute_values(":st", AttributeValue::N(step.to_string()))
        .send()
        .await;
    Ok(used.is_ok())
}

/// Records which second factor a member has (shown to admins).
pub async fn note_factor(state: &AppState, team_id: &str, user_id: &str, factor: Option<&str>) {
    let mut up = state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(user_id))
        .condition_expression("attribute_exists(pk)");
    up = match factor {
        Some(f) => up
            .update_expression("SET two_factor = :f")
            .expression_attribute_values(":f", attr_s(f)),
        None => up.update_expression("REMOVE two_factor"),
    };
    let _ = up.send().await;
}

fn session_cookie(token: &str) -> String {
    format!(
        "__Host-coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
    )
}

/// A fresh session for `claims` that passed a second factor just now.
pub fn session_with_second_factor(
    state: &AppState,
    claims: &Claims,
    via: &str,
) -> Result<String, ApiError> {
    let token = jwt::create_session_token(
        &claims.sub,
        &claims.team_id,
        &claims.email,
        claims.session_role(),
        claims.github_login.as_deref(),
        &SignInProof::new(true, via),
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|e| internal("Session not made", e))?;
    Ok(session_cookie(&token))
}

fn with_cookie(body: Value, cookie: &str) -> Response {
    let mut resp = Json(body).into_response();
    if let Ok(v) = HeaderValue::from_str(cookie) {
        resp.headers_mut().append(header::SET_COOKIE, v);
    }
    resp
}

fn via_of(proof: Option<&Extension<SignInProof>>) -> String {
    proof
        .and_then(|p| p.via.clone())
        .unwrap_or_else(|| "password".to_string())
}

#[derive(Deserialize)]
pub struct CodeBody {
    pub code: String,
}

/// POST /api/users/2fa/app/setup — start the authenticator-app second factor
/// for a Google or GitHub sign-in: a new secret, kept pending until a code
/// from it is entered.
pub async fn app_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    if !applies() {
        return Err(fail(StatusCode::NOT_FOUND, "Not available here."));
    }
    let item = member(&state, &claims.team_id, &claims.sub)
        .await
        .map_err(|e| internal("Member not read", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "You aren't on this team."))?;
    if app_enabled(&item) {
        return Err(fail(
            StatusCode::CONFLICT,
            "Two-factor sign-in is already on for you.",
        ));
    }
    let mut secret = [0u8; 20];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut secret);
    let sealed = seal(&state, &claims.team_id, &claims.sub, &secret)
        .await
        .map_err(|e| internal("Two-factor secret not sealed", e))?;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .update_expression("SET totp_pending = :p")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":p", attr_s(&sealed))
        .send()
        .await
        .map_err(|e| internal("Two-factor setup not saved", e))?;
    let b32 = base32(&secret);
    let label = if claims.email.is_empty() {
        claims.github_login.clone().unwrap_or_default()
    } else {
        claims.email.clone()
    };
    Ok(Json(json!({
        "secret": b32,
        "qr_uri": otpauth_uri(&label, &b32),
    })))
}

/// POST /api/users/2fa/app/verify — finish the setup with a code from the
/// app. The session then counts as signed in with a second factor.
pub async fn app_verify(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
    Json(body): Json<CodeBody>,
) -> Result<Response, ApiError> {
    if !applies() {
        return Err(fail(StatusCode::NOT_FOUND, "Not available here."));
    }
    let item = member(&state, &claims.team_id, &claims.sub)
        .await
        .map_err(|e| internal("Member not read", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "You aren't on this team."))?;
    let pending = s(&item, "totp_pending");
    if pending.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Start the setup first: it shows the code to scan.",
        ));
    }
    let secret = unseal(&state, &claims.team_id, &claims.sub, pending)
        .await
        .map_err(|e| internal("Two-factor secret not read", e))?;
    let unix = chrono::Utc::now().timestamp().max(0) as u64;
    let Some(step) = totp_check(&secret, &body.code, unix, 0) else {
        return Err(fail_code(
            StatusCode::BAD_REQUEST,
            "That code didn't match. Enter the current 6-digit code from the app.",
            "wrong_code",
        ));
    };
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .update_expression(
            "SET totp_sealed = :s, totp_enabled = :t, totp_last_step = :st, two_factor = :f, \
             totp_enabled_at = :now REMOVE totp_pending",
        )
        .condition_expression("totp_pending = :p")
        .expression_attribute_values(":s", attr_s(pending))
        .expression_attribute_values(":p", attr_s(pending))
        .expression_attribute_values(":t", AttributeValue::Bool(true))
        .expression_attribute_values(":st", AttributeValue::N(step.to_string()))
        .expression_attribute_values(":f", attr_s("app"))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| internal("Two-factor not turned on", e))?;
    info!(team_id = %claims.team_id, "Authenticator-app two-factor turned on");
    let via = via_of(proof.as_ref());
    let cookie = session_with_second_factor(&state, &claims, &via)?;
    Ok(with_cookie(json!({ "status": "enabled" }), &cookie))
}

/// DELETE /api/users/2fa/app — turn the authenticator-app second factor off
/// (with a current code). Refused while the organization requires it.
pub async fn app_disable(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
    Json(body): Json<CodeBody>,
) -> Result<Response, ApiError> {
    if !applies() {
        return Err(fail(StatusCode::NOT_FOUND, "Not available here."));
    }
    if load_org(&state)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .is_some_and(|o| o.require_2fa)
    {
        return Err(fail(
            StatusCode::CONFLICT,
            "Your organization requires two-factor sign-in, so it stays on.",
        ));
    }
    let item = member(&state, &claims.team_id, &claims.sub)
        .await
        .map_err(|e| internal("Member not read", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "You aren't on this team."))?;
    if !app_enabled(&item) {
        return Err(fail(
            StatusCode::CONFLICT,
            "Two-factor sign-in is already off.",
        ));
    }
    let key = person_key(&claims.email, &claims.team_id, &claims.sub);
    if let Some(left) = lockout_left(&state, &key).await {
        return Ok(locked_response(left));
    }
    if !use_app_code(&state, &item, &body.code)
        .await
        .map_err(|e| internal("Code not checked", e))?
    {
        if let Some(left) = record_failure(&state, &key).await {
            notify_lockout(&state, &claims.display_name(), left).await;
            return Ok(locked_response(left));
        }
        return Err(fail_code(
            StatusCode::BAD_REQUEST,
            "That code didn't match. Enter the current 6-digit code from the app.",
            "wrong_code",
        ));
    }
    record_success(&state, &key).await;
    clear_app_factor(&state, &claims.team_id, &claims.sub)
        .await
        .map_err(|e| internal("Two-factor not turned off", e))?;
    // Every other session of theirs ends; this one carries on.
    let proof = proof.map(|p| p.0).unwrap_or_default();
    let cookie = end_other_sessions(&state, &claims, &proof).await?;
    Ok(with_cookie(json!({ "status": "disabled" }), &cookie))
}

async fn clear_app_factor(state: &AppState, team: &str, user: &str) -> Result<(), String> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(user))
        .update_expression(
            "REMOVE totp_sealed, totp_enabled, totp_last_step, totp_pending, totp_enabled_at, two_factor",
        )
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

// ── Wrong codes: per-person lockout ─────────────────────────────────
//
// Every place CoderHelm checks a two-factor code counts a wrong one against
// the person (by email, or their record when there is none): the code after
// Google or GitHub, Cognito's code after a password, and turning the app
// code off. Ten wrong codes in 15 minutes pause that person's code sign-in;
// each pause is twice as long as the last (15 minutes up to a day), and the
// count starts over after a quiet day. A pending second-step token also dies
// after five wrong codes. Admins of the organization are told.

/// Wrong codes in this window count toward a pause.
pub const WINDOW_SECS: i64 = 900;
/// Wrong codes in a window that pause sign-in.
pub const MAX_FAILS: u32 = 10;
/// Wrong codes one pending second-step token survives.
pub const STEP_MAX_FAILS: i64 = 5;
const LOCK_PK: &str = "SIGNIN_LOCK";
const STEP_PK: &str = "SIGNIN_STEP";

/// How long the `n`th pause in a row lasts: 15 minutes, doubling, at most a day.
pub fn cooldown_secs(n: u32) -> i64 {
    (900i64 << n.saturating_sub(1).min(7)).min(86_400)
}

/// One person's wrong codes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Attempts {
    pub window_start: i64,
    pub fails: u32,
    pub locked_until: i64,
    /// Pauses so far (the next one is longer).
    pub lockouts: u32,
    pub last_lockout: i64,
}

impl Attempts {
    /// Seconds left in a pause, if paused.
    pub fn locked_for(&self, now: i64) -> Option<i64> {
        (self.locked_until > now).then(|| self.locked_until - now)
    }

    /// One more wrong code. `true` when it starts a pause.
    pub fn fail(mut self, now: i64) -> (Self, bool) {
        if self.last_lockout > 0 && now - self.last_lockout > 86_400 {
            self.lockouts = 0;
        }
        if now - self.window_start >= WINDOW_SECS {
            self.window_start = now;
            self.fails = 0;
        }
        self.fails += 1;
        if self.fails >= MAX_FAILS {
            self.lockouts += 1;
            self.last_lockout = now;
            self.locked_until = now + cooldown_secs(self.lockouts);
            self.fails = 0;
            self.window_start = now;
            return (self, true);
        }
        (self, false)
    }

    /// A right code: the count starts over (earlier pauses still lengthen the next).
    pub fn success(mut self) -> Self {
        self.fails = 0;
        self.window_start = 0;
        self
    }

    fn from_item(i: &Item) -> Self {
        let n = |k: &str| {
            i.get(k)
                .and_then(|v| v.as_n().ok())
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0)
        };
        Attempts {
            window_start: n("window_start"),
            fails: n("fails").max(0) as u32,
            locked_until: n("locked_until"),
            lockouts: n("lockouts").max(0) as u32,
            last_lockout: n("last_lockout"),
        }
    }
}

/// Who wrong codes count against: the email (the same person through every
/// way in), else their record.
pub fn person_key(email: &str, team_id: &str, user_id: &str) -> String {
    let e = normalize_email(email);
    if e.contains('@') {
        format!("EMAIL#{e}")
    } else {
        format!("USER#{team_id}#{user_id}")
    }
}

/// The plain sentence for someone paused.
pub fn locked_message(secs: i64) -> String {
    let mins = ((secs + 59) / 60).max(1);
    let when = if mins >= 120 {
        format!("{} hours", (mins + 59) / 60)
    } else if mins == 1 {
        "a minute".to_string()
    } else {
        format!("{mins} minutes")
    };
    format!(
        "Too many wrong codes. To keep your account safe, sign-in with a code is paused for {when}. If this wasn't you, tell an admin."
    )
}

pub fn locked_response(secs: i64) -> Response {
    let mut resp = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({ "error": locked_message(secs), "code": "locked_out", "retry_after": secs })),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
        resp.headers_mut().insert(header::RETRY_AFTER, v);
    }
    resp
}

async fn load_attempts(state: &AppState, key: &str) -> Result<(Attempts, i64), String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(LOCK_PK))
        .key("sk", attr_s(key))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(match out.item() {
        Some(i) => (
            Attempts::from_item(i),
            i.get("ver")
                .and_then(|v| v.as_n().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        ),
        None => (Attempts::default(), 0),
    })
}

/// Seconds left in this person's pause, if any. A read that fails counts as
/// paused for a minute (the safe answer).
pub async fn lockout_left(state: &AppState, key: &str) -> Option<i64> {
    match load_attempts(state, key).await {
        Ok((a, _)) => a.locked_for(chrono::Utc::now().timestamp()),
        Err(e) => {
            error!(error = %e, "Wrong-code count not read; treated as paused");
            Some(60)
        }
    }
}

async fn save_attempts(state: &AppState, key: &str, a: &Attempts, ver: i64) -> bool {
    let n = |v: i64| AttributeValue::N(v.to_string());
    let ttl = chrono::Utc::now().timestamp() + 3 * 86_400;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(LOCK_PK))
        .item("sk", attr_s(key))
        .item("window_start", n(a.window_start))
        .item("fails", n(i64::from(a.fails)))
        .item("locked_until", n(a.locked_until))
        .item("lockouts", n(i64::from(a.lockouts)))
        .item("last_lockout", n(a.last_lockout))
        .item("ver", n(ver + 1))
        .item("ttl", n(ttl.max(a.locked_until + 86_400)))
        .condition_expression("attribute_not_exists(pk) OR ver = :v")
        .expression_attribute_values(":v", n(ver))
        .send()
        .await
        .is_ok()
}

/// Counts one wrong code. Returns the pause it starts, in seconds, if any.
pub async fn record_failure(state: &AppState, key: &str) -> Option<i64> {
    for _ in 0..5 {
        let Ok((a, ver)) = load_attempts(state, key).await else {
            break;
        };
        let now = chrono::Utc::now().timestamp();
        let (next, locked) = a.fail(now);
        if save_attempts(state, key, &next, ver).await {
            return locked.then(|| next.locked_until - now);
        }
    }
    error!("Wrong code not counted after retries");
    None
}

/// A right code: the count starts over.
pub async fn record_success(state: &AppState, key: &str) {
    if let Ok((a, ver)) = load_attempts(state, key).await {
        if a.fails > 0 {
            save_attempts(state, key, &a.success(), ver).await;
        }
    }
}

/// One more wrong code on a pending second-step token; its new count.
async fn step_failure(state: &AppState, jti: &str) -> i64 {
    let ttl = chrono::Utc::now().timestamp() + 900;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(STEP_PK))
        .key("sk", attr_s(jti))
        .update_expression("ADD fails :one SET #t = :ttl")
        .expression_attribute_names("#t", "ttl")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await
        .ok()
        .and_then(|o| o.attributes)
        .and_then(|a| {
            a.get("fails")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
        })
        // Not counted: treat the token as spent (the safe answer).
        .unwrap_or(STEP_MAX_FAILS)
}

async fn step_failures(state: &AppState, jti: &str) -> i64 {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(STEP_PK))
        .key("sk", attr_s(jti))
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| {
            i.get("fails")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}

/// A pending token is dead after this many wrong codes.
pub fn step_spent(fails: i64) -> bool {
    fails >= STEP_MAX_FAILS
}

/// Tells the organization's admins that someone's code sign-in is paused:
/// a line in the log under Settings, Roles and an email to each admin and
/// owner. Self-hosted installs only (the hosted side has no organization).
pub async fn notify_lockout(state: &AppState, who: &str, secs: i64) {
    if !applies() {
        return;
    }
    let Ok(Some(org)) = load_org(state).await else {
        return;
    };
    let mins = (secs + 59) / 60;
    let detail = format!(
        "Two-factor sign-in paused for {mins} minutes for {who} after too many wrong codes."
    );
    warn!(team_id = %org.team_id, "Two-factor sign-in paused after wrong codes");
    let now = chrono::Utc::now();
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&org.team_id))
        .item(
            "sk",
            attr_s(&format!(
                "{}{at}#{}",
                super::roles::AUDIT_PREFIX,
                ulid::Ulid::new()
            )),
        )
        .item("record_type", attr_s("role_audit"))
        .item("at", attr_s(&at))
        .item("actor", attr_s("Coderhelm"))
        .item("actor_id", attr_s("SYSTEM"))
        .item("action", attr_s("two_factor_lockout"))
        .item("role_id", attr_s(""))
        .item("role_name", attr_s(""))
        .item("detail", attr_s(&detail))
        .item(
            "ttl",
            AttributeValue::N((now + chrono::Duration::days(365)).timestamp().to_string()),
        )
        .send()
        .await;
    let admins: Vec<String> = team_members(state, &org.team_id)
        .await
        .unwrap_or_default()
        .iter()
        .filter(|i| !is_invite(i) && matches!(s(i, "role"), "owner" | "admin"))
        .map(|i| s(i, "email").to_string())
        .filter(|e| e.contains('@'))
        .collect();
    let body = format!(
        "{detail}\n\nIf they lost their phone, check it's really them, then reset their two-factor sign-in in Settings, Sign-in. If it wasn't them, someone may know their password or have their Google or GitHub account: ask them to change it.\n"
    );
    // Through common::mail: the install's own SES, or CoderHelm's relay
    // when an install admin chose it (a fixed template, no names).
    let link = format!("{}/settings/", common::public_urls::app_url_from_env());
    let mut params = serde_json::Map::new();
    params.insert("minutes".into(), serde_json::Value::from(mins));
    params.insert("link".into(), serde_json::Value::from(link));
    for to in admins {
        let m = common::mail::Mail {
            kind: "two_factor_lockout".into(),
            to: to.clone(),
            template: "two_factor_lockout",
            params: params.clone(),
            ses: common::mail::SesBody::Simple {
                subject: "Two-factor sign-in paused after wrong codes".into(),
                text: body.clone(),
                html: None,
                headers: vec![],
            },
        };
        let result = common::mail::send(&super::email_settings::sender(state), &m).await;
        super::deliveries::record_mail(state, &org.team_id, "two_factor_lockout", &to, &result)
            .await;
        if let Err(e) = result {
            warn!(code = %e.code, error = %e.message, "Lockout email not sent");
        }
    }
}

// ── Sessions: ended by a reset, a removal or a role change ──────────

/// The member record field: sessions issued before this time (unix seconds)
/// are refused.
pub const VALID_AFTER: &str = "sessions_valid_after";

/// Why a session no longer counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// No longer on the team.
    Removed,
    /// Ended by a reset, a role change or turning two-factor off.
    Revoked,
}

/// Whether a session issued at `iat` still counts against the member's
/// record now. Pure.
pub fn session_state(item: Option<&Item>, iat: u64) -> Result<(), Ended> {
    let Some(i) = item else {
        return Err(Ended::Removed);
    };
    if is_invite(i) {
        return Err(Ended::Removed);
    }
    let after = i
        .get(VALID_AFTER)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if iat < after {
        return Err(Ended::Revoked);
    }
    Ok(())
}

pub fn session_ended(why: Ended) -> Response {
    let msg = match why {
        Ended::Removed => "You're no longer on this team. Sign in again, or ask an admin.",
        Ended::Revoked => {
            "Your session ended because your access or two-factor sign-in changed. Sign in again."
        }
    };
    let mut resp = (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": msg, "code": "session_ended" })),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(
        "__Host-coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
    ) {
        resp.headers_mut().append(header::SET_COOKIE, v);
    }
    resp
}

/// The middleware's check of one session against the member's record, read
/// fresh on every request. A read that fails refuses the request.
pub async fn check_member_session(state: &AppState, claims: &Claims) -> Result<(), Response> {
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .projection_expression("pk, #st, #va")
        .expression_attribute_names("#st", "status")
        .expression_attribute_names("#va", VALID_AFTER)
        .send()
        .await
    {
        Ok(o) => o.item,
        Err(e) => {
            error!(error = %e, "Member record not read; request refused");
            return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
    };
    session_state(item.as_ref(), claims.iat).map_err(session_ended)
}

/// Ends every session of a member issued before now. `include_now` also ends
/// one issued this very second (someone else's action); without it, a session
/// re-issued right after for the person acting keeps working.
pub async fn end_sessions(
    state: &AppState,
    team_id: &str,
    user_id: &str,
    include_now: bool,
) -> Result<(), String> {
    let at = chrono::Utc::now().timestamp() + i64::from(include_now);
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(user_id))
        .update_expression("SET #va = :at")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_names("#va", VALID_AFTER)
        .expression_attribute_values(":at", AttributeValue::N(at.to_string()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

/// The caller's other sessions end; this one gets a fresh cookie.
pub async fn end_other_sessions(
    state: &AppState,
    claims: &Claims,
    proof: &SignInProof,
) -> Result<String, ApiError> {
    end_sessions(state, &claims.team_id, &claims.sub, false)
        .await
        .map_err(|e| internal("Sessions not ended", e))?;
    let token = jwt::create_session_token(
        &claims.sub,
        &claims.team_id,
        &claims.email,
        claims.session_role(),
        claims.github_login.as_deref(),
        proof,
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|e| internal("Session not made", e))?;
    Ok(session_cookie(&token))
}

// ── Two-factor: the step after Google or GitHub ─────────────────────

/// After a Google or GitHub sign-in: when the person has the
/// authenticator-app factor, the session waits for their code. Returns the
/// redirect to the code screen, or `None` to sign them straight in (no
/// factor yet: with the rule on, the session can then only set one up).
pub async fn second_step(
    state: &AppState,
    step: SecondStep,
    app_url: &str,
) -> Result<Option<Response>, String> {
    let Some(item) = member(state, &step.team_id, &step.sub).await? else {
        return Ok(None);
    };
    if !app_enabled(&item) {
        return Ok(None);
    }
    let token = jwt::create_second_step(&step, &state.secrets.jwt_secret, SECOND_STEP_SECS)
        .map_err(|e| e.to_string())?;
    let mut resp = (
        StatusCode::FOUND,
        [(header::LOCATION, format!("{app_url}/?two_factor=1"))],
    )
        .into_response();
    let cookie = format!(
        "{SECOND_STEP_COOKIE}={token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={SECOND_STEP_SECS}"
    );
    if let Ok(v) = HeaderValue::from_str(&cookie) {
        resp.headers_mut().append(header::SET_COOKIE, v);
    }
    Ok(Some(resp))
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())?
        .split(';')
        .map(str::trim)
        .find_map(|c| c.strip_prefix(name).and_then(|r| r.strip_prefix('=')))
}

/// POST /auth/2fa/verify — the code after a Google or GitHub sign-in. On a
/// match: the session, with its second factor recorded.
pub async fn verify_second_step(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CodeBody>,
) -> Result<Response, ApiError> {
    let token = cookie_value(&headers, SECOND_STEP_COOKIE).ok_or_else(|| {
        fail_code(
            StatusCode::UNAUTHORIZED,
            "The sign-in took too long. Sign in again.",
            "second_step_expired",
        )
    })?;
    let step = jwt::validate_second_step(token, &state.secrets.jwt_secret).map_err(|_| {
        fail_code(
            StatusCode::UNAUTHORIZED,
            "The sign-in took too long. Sign in again.",
            "second_step_expired",
        )
    })?;
    let clear_step = || {
        HeaderValue::from_str(&format!(
            "{SECOND_STEP_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0"
        ))
        .ok()
    };
    let with_cleared = |mut r: Response| {
        if let Some(v) = clear_step() {
            r.headers_mut().append(header::SET_COOKIE, v);
        }
        r
    };
    let spent = || {
        with_cleared(
            fail_code(
                StatusCode::UNAUTHORIZED,
                "Too many wrong codes for this sign-in. Sign in again.",
                "second_step_expired",
            )
            .into_response(),
        )
    };
    // A paused person, or a token with five wrong codes, gets no more tries.
    let key = person_key(&step.email, &step.team_id, &step.sub);
    if let Some(left) = lockout_left(&state, &key).await {
        return Ok(with_cleared(locked_response(left)));
    }
    if step_spent(step_failures(&state, &step.jti).await) {
        return Ok(spent());
    }
    let item = member(&state, &step.team_id, &step.sub)
        .await
        .map_err(|e| internal("Member not read", e))?
        .ok_or_else(|| fail(StatusCode::UNAUTHORIZED, NOT_INVITED))?;
    if !app_enabled(&item)
        || !use_app_code(&state, &item, &body.code)
            .await
            .map_err(|e| internal("Code not checked", e))?
    {
        warn!(team_id = %step.team_id, "Second-step code refused");
        let token_fails = step_failure(&state, &step.jti).await;
        if let Some(left) = record_failure(&state, &key).await {
            let who = [
                step.email.as_str(),
                step.github_login.as_deref().unwrap_or(""),
            ]
            .into_iter()
            .find(|v| !v.is_empty())
            .unwrap_or("a member")
            .to_string();
            notify_lockout(&state, &who, left).await;
            return Ok(with_cleared(locked_response(left)));
        }
        if step_spent(token_fails) {
            return Ok(spent());
        }
        return Err(fail_code(
            StatusCode::UNAUTHORIZED,
            "That code didn't work. Open your authenticator app and enter the current 6-digit code.",
            "wrong_code",
        ));
    }
    record_success(&state, &key).await;
    let session = jwt::create_session_token(
        &step.sub,
        &step.team_id,
        &step.email,
        match s(&item, "role") {
            "" => "member",
            r => r,
        },
        step.github_login.as_deref(),
        &SignInProof::new(true, &step.via),
        &state.secrets.jwt_secret,
        SESSION_TTL_SECS,
    )
    .map_err(|e| internal("Session not made", e))?;
    let mut resp = Json(json!({ "status": "ok", "team_id": step.team_id })).into_response();
    for c in [
        session_cookie(&session),
        format!("{SECOND_STEP_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0"),
    ] {
        if let Ok(v) = HeaderValue::from_str(&c) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    Ok(resp)
}

// ── Settings, Sign-in ───────────────────────────────────────────────

/// Whether a member has a second factor: `app`, `cognito` or none. A record
/// from before `two_factor` was written is checked in Cognito.
async fn factor_of(state: &AppState, item: &Item) -> Option<String> {
    match s(item, "two_factor") {
        "" => {}
        f => return Some(f.to_string()),
    }
    if app_enabled(item) {
        return Some("app".into());
    }
    let email = s(item, "email");
    if email.is_empty() {
        return None;
    }
    let on = state
        .cognito
        .admin_get_user()
        .user_pool_id(&state.config.cognito_user_pool_id)
        .username(email)
        .send()
        .await
        .ok()
        .and_then(|u| u.user_mfa_setting_list)
        .is_some_and(|l| l.iter().any(|m| m == "SOFTWARE_TOKEN_MFA"));
    on.then(|| "cognito".to_string())
}

async fn team_members(state: &AppState, team_id: &str) -> Result<Vec<Item>, String> {
    let mut items = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":u", attr_s("USER#"))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("{e}"))?;
        items.extend(out.items().iter().cloned());
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(items)
}

/// GET /api/sign-in — the organization's sign-in rules, who has two-factor
/// sign-in, and the API keys (which the rule doesn't affect).
pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if !applies() {
        return Ok(Json(json!({ "self_hosted": false })));
    }
    let org = ensure_org(&state)
        .await
        .map_err(|e| internal("Organization not read", e))?;
    let items = team_members(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Members not read", e))?;
    let active: Vec<&Item> = items.iter().filter(|i| !is_invite(i)).collect();
    let factors = futures::future::join_all(active.iter().map(|i| factor_of(&state, i))).await;
    let people: Vec<Value> = active
        .iter()
        .zip(factors)
        .map(|(i, f)| {
            json!({
                "user_id": s(i, "sk").trim_start_matches("USER#"),
                "email": s(i, "email"),
                "name": s(i, "name"),
                "github_login": s(i, "github_login"),
                "role": s(i, "role"),
                "two_factor": f,
                "is_self": s(i, "sk") == claims.sub,
            })
        })
        .collect();
    let invites = items.iter().filter(|i| is_invite(i)).count();
    let now = chrono::Utc::now();
    let keys: Vec<Value> = {
        use crate::api_keys::KeyStore;
        crate::api_keys::store(&state)
            .list(&claims.team_id)
            .await
            .unwrap_or_default()
            .iter()
            .filter(|k| k.is_live(now))
            .map(|k| json!({ "id": k.id, "name": k.name, "created_by": k.created_by_name }))
            .collect()
    };
    Ok(Json(json!({
        "self_hosted": true,
        "is_org": org.as_ref().is_some_and(|o| o.team_id == claims.team_id),
        "domains": org.as_ref().map(|o| o.domains.clone()).unwrap_or_default(),
        "require_2fa": org.as_ref().is_some_and(|o| o.require_2fa),
        "require_2fa_since": org.as_ref().and_then(|o| o.require_2fa_since.clone()),
        "pending_invites": invites,
        "people": people,
        "api_keys": keys,
        "your_session_has_2fa": proof.is_some_and(|p| p.mfa),
    })))
}

#[derive(Deserialize)]
pub struct SettingsBody {
    pub domains: Option<Vec<String>>,
    pub require_2fa: Option<bool>,
}

/// PUT /api/sign-in — change the allowed domains or the two-factor rule.
pub async fn update_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
    Json(body): Json<SettingsBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if !applies() {
        return Err(fail(StatusCode::NOT_FOUND, "Not available here."));
    }
    let org = ensure_org(&state)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .filter(|o| o.team_id == claims.team_id)
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "This team isn't the organization."))?;
    let domains = match &body.domains {
        Some(d) => Some(clean_domains(d).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?),
        None => None,
    };
    let turning_on = body.require_2fa == Some(true) && !org.require_2fa;
    if turning_on && !proof.as_ref().is_some_and(|p| p.mfa) {
        return Err(fail_code(
            StatusCode::CONFLICT,
            "Set up two-factor sign-in for yourself first, then sign in again with it. Then you can require it for everyone.",
            "set_up_yours_first",
        ));
    }
    let mut changes = Vec::new();
    let mut sets = vec!["updated_at = :now", "updated_by = :by"];
    let now = chrono::Utc::now().to_rfc3339();
    let mut values = HashMap::from([
        (":now".to_string(), attr_s(&now)),
        (":by".to_string(), attr_s(&claims.display_name())),
    ]);
    if let Some(d) = &domains {
        if *d != org.domains {
            changes.push(if d.is_empty() {
                "Allowed domains: none (invites only)".to_string()
            } else {
                format!("Allowed domains: {}", d.join(", "))
            });
        }
        sets.push("domains = :d");
        values.insert(
            ":d".into(),
            AttributeValue::L(d.iter().map(|x| attr_s(x)).collect()),
        );
    }
    if let Some(r) = body.require_2fa {
        if r != org.require_2fa {
            changes.push(if r {
                "Two-factor sign-in required for everyone".to_string()
            } else {
                "Two-factor sign-in no longer required".to_string()
            });
        }
        sets.push("require_2fa = :r");
        values.insert(":r".into(), AttributeValue::Bool(r));
        if turning_on {
            sets.push("require_2fa_since = :now");
        }
    }
    if !changes.is_empty() {
        // Nothing changes without its line in the log.
        super::roles::record(
            &state,
            &claims,
            super::roles::AuditEntry {
                action: "sign_in_rules",
                role_id: "",
                role_name: "",
                detail: changes.join(". "),
            },
        )
        .await?;
    }
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(ORG_PK))
        .key("sk", attr_s(ORG_SK))
        .update_expression(format!("SET {}", sets.join(", ")))
        .set_expression_attribute_values(Some(values))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| internal("Sign-in rules not saved", e))?;
    info!(changes = ?changes, "Sign-in rules changed");
    let org = load_org(&state)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .unwrap_or(org);
    Ok(Json(json!({
        "domains": org.domains,
        "require_2fa": org.require_2fa,
        "require_2fa_since": org.require_2fa_since,
    })))
}

#[derive(Deserialize)]
pub struct ResetBody {
    /// How the admin made sure it is really them (logged).
    pub verified_how: String,
}

/// What an admin must say before resetting someone's second factor.
pub fn reset_reason(how: &str) -> Result<String, &'static str> {
    let how = how.trim();
    if how.chars().count() < 10 {
        return Err("Say how you checked it's really them (at least 10 characters), for the log.");
    }
    Ok(how.chars().take(300).collect())
}

/// POST /api/users/:user_id/2fa/reset — turn a member's second factor off
/// after the admin checked who is asking (a lost phone). Logged first; the
/// member sets it up again at their next sign-in when the rule is on.
pub async fn reset_two_factor(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(user_id): Path<String>,
    Json(body): Json<ResetBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if !applies() {
        return Err(fail(StatusCode::NOT_FOUND, "Not available here."));
    }
    let how = reset_reason(&body.verified_how).map_err(|m| fail(StatusCode::BAD_REQUEST, m))?;
    let sk = format!("USER#{}", user_id.trim_start_matches("USER#"));
    if sk == claims.sub {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You can't reset your own. Ask another admin.",
        ));
    }
    let item = member(&state, &claims.team_id, &sk)
        .await
        .map_err(|e| internal("Member not read", e))?
        .filter(|i| !is_invite(i))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That person isn't on this team."))?;
    let target_role = match s(&item, "role") {
        "" => "member",
        r => r,
    };
    if !crate::permissions::can_manage(&claims.role, target_role) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only an owner can reset an admin's or owner's two-factor sign-in.",
        ));
    }
    let who = [
        s(&item, "email"),
        s(&item, "github_login"),
        s(&item, "name"),
    ]
    .into_iter()
    .find(|v| !v.is_empty())
    .unwrap_or("a member")
    .to_string();
    super::roles::record(
        &state,
        &claims,
        super::roles::AuditEntry {
            action: "two_factor_reset",
            role_id: "",
            role_name: "",
            detail: format!("Reset two-factor sign-in for {who}. How it was checked: {how}"),
        },
    )
    .await?;
    let email = s(&item, "email");
    if !email.is_empty() {
        if let Err(e) = state
            .cognito
            .admin_set_user_mfa_preference()
            .user_pool_id(&state.config.cognito_user_pool_id)
            .username(email)
            .software_token_mfa_settings(
                aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaSettingsType::builder()
                    .enabled(false)
                    .preferred_mfa(false)
                    .build(),
            )
            .send()
            .await
        {
            let msg = format!("{e:?}");
            if !msg.contains("UserNotFound") {
                return Err(internal("Cognito two-factor not reset", msg));
            }
        }
    }
    clear_app_factor(&state, &claims.team_id, &sk)
        .await
        .map_err(|e| internal("Two-factor not reset", e))?;
    // Their sessions end: whoever holds one signs in again and sets it up.
    end_sessions(&state, &claims.team_id, &sk, true)
        .await
        .map_err(|e| internal("Sessions not ended", e))?;
    info!(team_id = %claims.team_id, "Two-factor sign-in reset by an admin");
    Ok(Json(json!({ "status": "reset" })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(pk: &str, sk: &str, status: Option<&str>, invited_at: Option<&str>) -> Item {
        let mut m = HashMap::from([
            ("pk".to_string(), attr_s(pk)),
            ("sk".to_string(), attr_s(sk)),
        ]);
        if let Some(st) = status {
            m.insert("status".into(), attr_s(st));
        }
        if let Some(t) = invited_at {
            m.insert("invited_at".into(), attr_s(t));
        }
        m
    }

    /// The cases the Cognito pre-sign-up trigger is tested with too.
    #[derive(Deserialize)]
    struct Case {
        name: String,
        email: String,
        org: Option<CaseOrg>,
        #[serde(default)]
        records: Vec<CaseRecord>,
        #[serde(default)]
        admin_listed: bool,
        #[serde(default)]
        listed: bool,
        #[serde(default)]
        anyone: bool,
        now: String,
        want: String,
    }
    #[derive(Deserialize)]
    struct CaseOrg {
        team_id: String,
        #[serde(default)]
        domains: Vec<String>,
    }
    #[derive(Deserialize)]
    struct CaseRecord {
        pk: String,
        sk: String,
        status: Option<String>,
        invited_at: Option<String>,
    }

    #[test]
    fn every_shared_join_case_decides_as_written() {
        let cases: Vec<Case> =
            serde_json::from_str(include_str!("../../../../lambda/pre-signup/cases.json")).unwrap();
        assert!(cases.len() >= 12);
        for c in cases {
            let facts = JoinFacts {
                email: c.email.clone(),
                org: c.org.map(|o| Org {
                    team_id: o.team_id,
                    domains: o.domains,
                    ..Org::default()
                }),
                records: c
                    .records
                    .iter()
                    .map(|r| rec(&r.pk, &r.sk, r.status.as_deref(), r.invited_at.as_deref()))
                    .collect(),
                admin_listed: c.admin_listed,
                listed: c.listed,
                anyone: c.anyone,
            };
            let now = chrono::DateTime::parse_from_rfc3339(&c.now)
                .unwrap()
                .with_timezone(&chrono::Utc);
            assert_eq!(decide_join(&facts, now).key(), c.want, "{}", c.name);
        }
    }

    fn org(domains: &[&str]) -> Org {
        Org {
            team_id: "TEAM#org".into(),
            domains: domains.iter().map(|d| d.to_string()).collect(),
            ..Org::default()
        }
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-10-26T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn a_stranger_is_refused_once_the_organization_exists() {
        let f = JoinFacts {
            email: "someone@elsewhere.io".into(),
            org: Some(org(&["example.com"])),
            anyone: true,
            // The config list no longer lets anyone in after first boot.
            listed: true,
            admin_listed: true,
            ..JoinFacts::default()
        };
        assert_eq!(decide_join(&f, now()), Join::Refused);
    }

    #[test]
    fn an_allowed_domain_joins_and_a_lookalike_does_not() {
        let mut f = JoinFacts {
            email: "Ann@Example.com".into(),
            org: Some(org(&["example.com"])),
            anyone: true,
            ..JoinFacts::default()
        };
        assert_eq!(decide_join(&f, now()), Join::Domain);
        for e in [
            "ann@notexample.com",
            "ann@example.com.evil.io",
            "ann@sub.example.com",
            "example.com",
            "@example.com",
        ] {
            f.email = e.into();
            assert_eq!(decide_join(&f, now()), Join::Refused, "{e}");
        }
    }

    #[test]
    fn an_invite_to_another_team_does_not_let_anyone_into_the_organization() {
        let f = JoinFacts {
            email: "b@x.io".into(),
            org: Some(org(&[])),
            anyone: true,
            records: vec![
                rec(
                    "TEAM#other",
                    "USER#invite_1",
                    Some("invited"),
                    Some("2026-10-20T00:00:00Z"),
                ),
                rec("TEAM#other", "USER#2", None, None),
            ],
            ..JoinFacts::default()
        };
        assert_eq!(decide_join(&f, now()), Join::Refused);
    }

    #[test]
    fn no_second_organization_after_the_first_admin() {
        // Before anyone signs up, only a listed admin may start it...
        let mut f = JoinFacts {
            email: "admin@x.io".into(),
            admin_listed: true,
            listed: true,
            ..JoinFacts::default()
        };
        assert_eq!(decide_join(&f, now()), Join::FirstAdmin);
        f.admin_listed = false;
        assert_eq!(decide_join(&f, now()), Join::Refused);
        // ...and once it exists, the same person can't start another one.
        f.admin_listed = true;
        f.anyone = true;
        f.org = Some(org(&[]));
        assert_eq!(decide_join(&f, now()), Join::Refused);
    }

    #[test]
    fn domains_are_checked_and_public_mail_refused() {
        assert_eq!(
            clean_domains(&[
                " Example.COM ".into(),
                "@corp.example.org".into(),
                "example.com".into()
            ])
            .unwrap(),
            vec!["corp.example.org".to_string(), "example.com".to_string()]
        );
        assert!(clean_domains(&["gmail.com".into()])
            .unwrap_err()
            .contains("public email"));
        for bad in [
            "example",
            "exa mple.com",
            "-x.com",
            "x..com",
            "x.c0m",
            "*.example.com",
        ] {
            assert!(clean_domains(&[bad.into()]).is_err(), "{bad}");
        }
        let many: Vec<String> = (0..21).map(|i| format!("d{i}.example.com")).collect();
        assert!(clean_domains(&many).is_err());
        assert_eq!(clean_domains(&[]).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn the_config_seed_splits_admins_invites_domains_and_the_rule() {
        let items = vec![
            HashMap::from([("sk".to_string(), attr_s("EMAIL#admin@x.io"))]),
            HashMap::from([
                ("sk".to_string(), attr_s("EMAIL#b@x.io")),
                ("kind".to_string(), attr_s("invite")),
            ]),
            HashMap::from([("sk".to_string(), attr_s("EMAIL#*@x.io"))]),
            HashMap::from([
                ("sk".to_string(), attr_s("TWO_FACTOR")),
                ("require".to_string(), AttributeValue::Bool(true)),
            ]),
        ];
        let seed = seed_from(&items);
        assert_eq!(seed.domains, vec!["x.io".to_string()]);
        assert_eq!(
            seed.invites,
            vec![
                ("admin@x.io".to_string(), "admin".to_string()),
                ("b@x.io".to_string(), "member".to_string())
            ]
        );
        assert!(seed.require_2fa);
    }

    #[test]
    fn the_rule_holds_a_session_without_a_second_factor_on_every_route_but_setup() {
        let none = SignInProof::default();
        let google = SignInProof {
            mfa: false,
            via: Some("google".into()),
        };
        let passed = SignInProof::new(true, "github");
        // Off: everyone goes on.
        assert!(gate(false, &none, Some("api::list_runs")));
        // On: only the setup screens, for password and federated sessions alike.
        for p in [&none, &google] {
            assert!(!gate(true, p, Some("api::list_runs")));
            assert!(!gate(true, p, Some("users::invite_user")));
            assert!(!gate(true, p, Some("sign_in::update_settings")));
            assert!(!gate(true, p, Some("users::mfa_disable")));
            assert!(!gate(true, p, None));
            for h in SETUP_HANDLERS {
                assert!(gate(true, p, Some(h)), "{h}");
            }
        }
        assert!(gate(true, &passed, Some("api::list_runs")));
    }

    #[test]
    fn every_setup_route_is_routed() {
        let routed: Vec<&String> = crate::permissions::api_route_handlers().values().collect();
        for h in SETUP_HANDLERS {
            assert!(routed.iter().any(|r| r == h), "{h} isn't routed under /api");
        }
    }

    #[test]
    fn totp_matches_rfc_6238_and_a_code_works_once() {
        // RFC 6238 appendix B, SHA-1, the last 6 of each 8-digit value.
        let key = b"12345678901234567890";
        for (t, want) in [
            (59u64, "287082"),
            (1_111_111_109, "081804"),
            (1_234_567_890, "005924"),
            (2_000_000_000, "279037"),
        ] {
            assert_eq!(totp_at(key, t / 30), want, "t={t}");
        }
        let t = 1_234_567_890;
        let step = totp_check(key, "005 924", t, 0).unwrap();
        assert_eq!(step, t / 30);
        // The same code again is refused; so is one from far away, or junk.
        assert_eq!(totp_check(key, "005924", t, step), None);
        assert_eq!(totp_check(key, "005924", t + 120, 0), None);
        assert_eq!(totp_check(key, "12345", t, 0), None);
        assert_eq!(totp_check(key, "abcdef", t, 0), None);
        // One step of clock drift either side is fine.
        assert!(totp_check(key, &totp_at(key, t / 30 + 1), t, 0).is_some());
    }

    #[test]
    fn base32_and_the_app_link() {
        assert_eq!(
            base32(b"12345678901234567890"),
            "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
        );
        assert_eq!(base32(b"f"), "MY");
        let uri = otpauth_uri("a+b@x.io", "ABC");
        assert!(uri.starts_with("otpauth://totp/CoderHelm:a%2Bb%40x.io?secret=ABC"));
    }

    #[test]
    fn a_reset_needs_a_reason_for_the_log() {
        assert!(reset_reason("ok").is_err());
        assert!(reset_reason("          ").is_err());
        assert_eq!(
            reset_reason("  Video call, saw their badge ").unwrap(),
            "Video call, saw their badge"
        );
        assert_eq!(reset_reason(&"x".repeat(500)).unwrap().len(), 300);
    }

    #[test]
    fn the_org_record_reads_back() {
        let item = HashMap::from([
            ("team_id".to_string(), attr_s("TEAM#a")),
            (
                "domains".to_string(),
                AttributeValue::L(vec![attr_s("Example.com")]),
            ),
            ("require_2fa".to_string(), AttributeValue::Bool(true)),
            (
                "require_2fa_since".to_string(),
                attr_s("2026-10-26T00:00:00Z"),
            ),
        ]);
        let o = Org::from_item(&item).unwrap();
        assert_eq!(o.domains, vec!["example.com".to_string()]);
        assert!(o.require_2fa);
        assert!(Org::from_item(&HashMap::new()).is_none());
    }

    /// The body of `fn name` in `src`.
    fn body<'a>(src: &'a str, name: &str) -> &'a str {
        let start = src
            .find(&format!("fn {name}("))
            .unwrap_or_else(|| panic!("no fn {name}"));
        let end = src[start..].find("\n}\n").map_or(src.len(), |e| start + e);
        &src[start..end]
    }

    fn before(hay: &str, first: &str, then: &str) -> bool {
        match (hay.find(first), hay.find(then)) {
            (Some(a), Some(b)) => a < b,
            _ => false,
        }
    }

    /// Every way to make an account or a session on a self-hosted install
    /// goes through the join rules before anything is written or a session
    /// is made, and no path makes a team of its own there.
    #[test]
    fn every_sign_up_and_sign_in_path_checks_the_rules_first() {
        let auth = include_str!("auth.rs");
        // Email sign-up: refused before Cognito is asked.
        let signup = body(auth, "signup");
        assert!(before(signup, "sign_in::may_sign_up", ".sign_up()"));
        assert!(signup.contains("UserLambdaValidationException"));
        // Email confirmation makes no personal team.
        let verify = body(auth, "verify_email");
        assert!(before(verify, "sign_in::applies()", "needs_personal_team"));
        // Email sign-in (with or without the Cognito code): the landing.
        let issue = body(auth, "issue_session_from_cognito");
        assert!(before(
            issue,
            "selfhost_landing",
            "Created new user + team on first login"
        ));
        assert!(before(issue, "sign_in::applies()", "TEAM#{cognito_sub}"));
        // Google.
        let google = body(auth, "google_callback_inner");
        assert!(before(google, "selfhost_landing", "Google user created"));
        assert!(before(
            google,
            "sign_in::second_step",
            "Google user created"
        ));
        // GitHub: the organization's records or the landing, never a new team.
        let github = body(auth, "github_callback_inner");
        assert!(before(github, "selfhost_landing", "Brand new user"));
        assert!(before(
            github,
            "sign_in::second_step",
            "SignInProof::new(false, \"github\")"
        ));
        // The landing makes teams only through the organization's own rules.
        let landing = body(auth, "selfhost_landing");
        for word in [
            "decide_join",
            "create_org",
            "join_by_domain",
            "accept_pending_invites",
        ] {
            assert!(landing.contains(word), "{word}");
        }
        assert!(!landing.contains("Ulid::new"));
    }

    /// The middleware reads the rule on every session request, before the
    /// role check and before the handler runs; API keys take their own path.
    #[test]
    fn the_middleware_holds_sessions_on_every_request() {
        let mw = include_str!("../middleware/auth.rs");
        let f = body(mw, "require_auth");
        assert!(before(f, "api_key_request", "sign_in::check_session"));
        assert!(before(
            f,
            "sign_in::check_session",
            "match permissions::resolve_access"
        ));
        assert!(before(f, "sign_in::check_session", "note_activity"));
        // The refreshed session keeps its proof.
        assert!(f.contains("create_session_token") && f.contains("&proof"));
        // Read fresh each time: no cache in front of the rule.
        let check = body(include_str!("sign_in.rs"), "check_session");
        assert!(check.contains("load_org(state)"));
        assert!(!check.to_lowercase().contains("cache"));
    }

    /// A reset is logged before anything changes, and only by an admin.
    #[test]
    fn a_reset_is_logged_first() {
        let f = body(include_str!("sign_in.rs"), "reset_two_factor");
        assert!(before(f, "admin(&claims)?", "roles::record"));
        assert!(before(f, "reset_reason", "roles::record"));
        assert!(before(f, "roles::record", "admin_set_user_mfa_preference"));
        assert!(before(f, "roles::record", "clear_app_factor"));
        assert!(f.contains("can_manage"));
        let u = body(include_str!("sign_in.rs"), "update_settings");
        assert!(before(u, "set_up_yours_first", "roles::record"));
        assert!(before(u, "roles::record", ".update_item()"));
    }

    #[test]
    fn ten_wrong_codes_in_15_minutes_pause_and_each_pause_is_longer() {
        let t0 = 1_800_000_000i64;
        let mut a = Attempts::default();
        for i in 0..9 {
            let (next, locked) = a.fail(t0 + i * 60);
            assert!(!locked, "wrong code {}", i + 1);
            a = next;
        }
        assert_eq!(a.locked_for(t0 + 600), None);
        let (a, locked) = a.fail(t0 + 600);
        assert!(locked, "the 10th wrong code pauses");
        assert_eq!(a.locked_for(t0 + 600), Some(900));
        assert_eq!(a.locked_for(t0 + 600 + 900), None);
        // The next pause is twice as long, then doubles up to a day.
        let mut b = a;
        for _ in 0..10 {
            b = b.fail(t0 + 2000).0;
        }
        assert_eq!(b.locked_for(t0 + 2000), Some(1800));
        assert_eq!(
            (1..=10).map(cooldown_secs).collect::<Vec<_>>(),
            vec![900, 1800, 3600, 7200, 14_400, 28_800, 57_600, 86_400, 86_400, 86_400]
        );
        // After a quiet day, pauses start short again.
        let mut c = b;
        for _ in 0..10 {
            c = c.fail(t0 + 2000 + 2 * 86_400).0;
        }
        assert_eq!(c.lockouts, 1);
        assert_eq!(c.locked_for(t0 + 2000 + 2 * 86_400), Some(900));
    }

    #[test]
    fn wrong_codes_spread_out_or_followed_by_a_right_one_do_not_pause() {
        let t0 = 1_800_000_000i64;
        // Nine wrong codes every 15 minutes: never ten in one window.
        let mut a = Attempts::default();
        for i in 0..36 {
            let (next, locked) = a.fail(t0 + (i / 9) * WINDOW_SECS + i % 9);
            assert!(!locked, "{i}");
            a = next;
        }
        let mut b = Attempts::default();
        for i in 0..9 {
            b = b.fail(t0 + i).0;
        }
        b = b.success();
        assert!(!b.fail(t0 + 10).1, "a right code starts the count over");
        let mut w = Attempts::default();
        for i in 0..9 {
            w = w.fail(t0 + i).0;
        }
        assert!(!w.fail(t0 + WINDOW_SECS + 1).1, "the window moved on");
    }

    #[test]
    fn a_pending_token_dies_after_five_wrong_codes() {
        assert!(!step_spent(4));
        assert!(step_spent(5));
        assert!(step_spent(6));
    }

    #[test]
    fn wrong_codes_count_against_the_person_through_every_way_in() {
        // The same email through Google, GitHub and a password: one count.
        assert_eq!(
            person_key("Ann+x@Example.com", "TEAM#a", "USER#Google_1"),
            "EMAIL#ann@example.com"
        );
        assert_eq!(
            person_key("ann@example.com", "", ""),
            "EMAIL#ann@example.com"
        );
        // No email (GitHub without one): their record.
        assert_eq!(person_key("", "TEAM#a", "USER#9"), "USER#TEAM#a#USER#9");
        assert!(locked_message(900).contains("15 minutes"));
        assert!(locked_message(30).contains("a minute"));
        assert!(locked_message(86_400).contains("24 hours"));
        let r = locked_response(900);
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r.headers()[header::RETRY_AFTER], "900");
    }

    #[test]
    fn a_session_ends_with_a_removal_a_role_change_or_a_reset() {
        let rec = |after: Option<u64>, status: Option<&str>| {
            let mut m: Item = HashMap::from([("pk".to_string(), attr_s("TEAM#a"))]);
            if let Some(a) = after {
                m.insert(VALID_AFTER.into(), AttributeValue::N(a.to_string()));
            }
            if let Some(st) = status {
                m.insert("status".into(), attr_s(st));
            }
            m
        };
        assert_eq!(session_state(Some(&rec(None, None)), 100), Ok(()));
        assert_eq!(session_state(None, 100), Err(Ended::Removed));
        assert_eq!(
            session_state(Some(&rec(None, Some("invited"))), 100),
            Err(Ended::Removed)
        );
        assert_eq!(
            session_state(Some(&rec(Some(200), None)), 100),
            Err(Ended::Revoked)
        );
        assert_eq!(
            session_state(Some(&rec(Some(200), None)), 200),
            Ok(()),
            "issued at the cut"
        );
        assert_eq!(
            session_state(Some(&rec(Some(200), None)), 300),
            Ok(()),
            "a later sign-in"
        );
        let r = session_ended(Ended::Revoked);
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert!(r.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0"));
    }

    /// Every code check counts wrong codes and refuses a paused person first;
    /// every change that must end sessions ends them.
    #[test]
    fn every_code_check_is_capped_and_every_change_ends_sessions() {
        let me = include_str!("sign_in.rs");
        let auth = include_str!("auth.rs");
        let users = include_str!("users.rs");
        let step = body(me, "verify_second_step");
        assert!(before(step, "lockout_left", "use_app_code"));
        assert!(before(step, "step_spent(step_failures", "use_app_code"));
        assert!(before(step, "step_failure(&state", "record_success"));
        assert!(step.contains("record_failure") && step.contains("notify_lockout"));
        let mfa = body(auth, "mfa_verify");
        assert!(before(
            mfa,
            "sign_in::lockout_left",
            ".respond_to_auth_challenge()"
        ));
        assert!(mfa.contains("sign_in::record_failure") && mfa.contains("sign_in::notify_lockout"));
        let off = body(me, "app_disable");
        assert!(before(off, "lockout_left", "use_app_code"));
        assert!(before(off, "clear_app_factor", "end_other_sessions"));
        let reset = body(me, "reset_two_factor");
        assert!(before(
            reset,
            "clear_app_factor",
            "end_sessions(&state, &claims.team_id, &sk, true)"
        ));
        assert!(body(users, "update_role").contains("sign_in::VALID_AFTER"));
        assert!(body(users, "mfa_disable").contains("end_other_sessions"));
        // A removal deletes the record, and the middleware refuses a session
        // with no record.
        assert!(body(users, "remove_user").contains(".delete_item()"));
        let mw = body(include_str!("../middleware/auth.rs"), "require_auth");
        assert!(before(mw, "api_key_request", "check_member_session"));
        assert!(before(
            mw,
            "check_member_session",
            "match permissions::resolve_access"
        ));
        assert!(!body(me, "check_member_session")
            .to_lowercase()
            .contains("cache"));
    }

    #[test]
    fn email_domains() {
        assert_eq!(email_domain("a@B.io").as_deref(), Some("b.io"));
        assert_eq!(email_domain("a@b"), None);
        assert_eq!(email_domain("@b.io"), None);
        assert_eq!(email_domain("nope"), None);
    }
}
