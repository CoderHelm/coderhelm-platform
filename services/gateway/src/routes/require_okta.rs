//! Require Okta sign-in: a hosted team, or a self-hosted install's
//! organization, whose members sign in only through its own Okta.
//!
//! # The rule
//!
//! It is one more sign-in rule beside two-factor sign-in, on the same record
//! (hosted: `pk = <team>`, `sk = SETTINGS#SIGN_IN`; self-hosted: the
//! organization record, `ORG`/`META`): `require_okta` and
//! `require_okta_since` (unix seconds). Only the owner turns it on or off,
//! and every change is in the team's audit log (`sign_in_rules`). It turns
//! on only when the team's Okta sign-in is set up and on, (hosted) every one
//! of its email domains is still proven in DNS, and the owner's own session
//! came from this team's Okta, which proves the whole trip works before
//! anyone depends on it.
//!
//! # While it is on
//!
//! - Every session acting in the team is checked on every request
//!   ([`check`], from `sign_in::check_session`, for dashboard sessions and
//!   connected apps alike). A session from anything but this team's Okta is
//!   refused with `okta_required` and its cookie cleared: sessions from
//!   before the rule end at their next request. A connected app (MCP) works
//!   only from a grant an Okta session made.
//! - Every other way in (password, Google, GitHub, Microsoft) refuses this
//!   team before a record or a session is made ([`issue_check`], and on
//!   hosted [`filter`], which also keeps invites to the team from being
//!   accepted that way). Switching into the team is refused. On hosted only
//!   this team is held back: the person signs in to their other teams as
//!   before, and their sessions there are untouched.
//! - Break-glass: the owner may still sign in with their password and their
//!   second factor (`break_glass` on the sign-in), so a broken Okta can't
//!   lock the team out. A second factor is required, with no exception.
//!   Each such sign-in is written to the team's log before its session is
//!   made, and listed on the Sign-in rules page. The session is marked for
//!   this team only ([`SignInProof::break_glass`]) and is good only while its
//!   holder is still the owner. Connected apps can't be made from it.
//! - New members: an admin's pending invite may be accepted by signing in
//!   through the team's Okta, when Okta's verified email is the invited
//!   email and (hosted) its domain is one the team proved in DNS. That makes
//!   the member record and links the Okta identity in one transaction, with
//!   the same one-identity-per-team row as a link. It never links or takes
//!   over an existing member record ([`invite_for`]).
//! - Members who hadn't linked Okta before the rule: their Okta sign-in
//!   lands only on a link they made before, else they are told to ask an
//!   admin (who can re-invite them, or turn the rule off while they link).
//!   There is no matching by email for existing members.
//! - API keys and CI tokens are not sessions and keep working.
//!
//! # Why accepting an invite through Okta is safe
//!
//! - The invite names the exact email, and only an admin makes one.
//! - Okta's email counts only with `email_verified: true`, from the team's
//!   own Okta (issuer, audience, signature and nonce checked).
//! - Hosted: the email's domain must be proven by this team with a DNS TXT
//!   record, so a team's Okta vouches only for addresses at domains its
//!   owner controls. Self-hosted: the owner alone sets up the organization's
//!   Okta.
//! - The record is new (never an existing member's), and the session it
//!   gets is held to this team on hosted, as every Okta session is: it can't
//!   reach the person's other teams or anything person-wide.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use tracing::{error, info, warn};

use super::oauth_signin::Provider;
use super::sign_in::{self, Rule};
use crate::auth::jwt::SignInProof;
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

/// The refusal's code, for the dashboard and the sign-in screen.
pub const CODE: &str = "okta_required";
/// A break-glass sign-in by an owner with no second factor to pass.
pub const NEEDS_2FA: &str = "break_glass_needs_2fa";
/// The team's log of break-glass sign-ins (settings table, `pk = <team>`).
pub const BREAK_GLASS_PREFIX: &str = "BREAK_GLASS#";
/// How long a break-glass line is kept.
const BREAK_GLASS_DAYS: i64 = 400;
/// How many break-glass sign-ins the Sign-in rules page lists.
pub const BREAK_GLASS_SHOWN: i32 = 20;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map_or("", String::as_str)
}

fn now_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

// ── Sessions ────────────────────────────────────────────────────────

/// What the rule says about one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// No rule, or this team's Okta signed it in.
    Allow,
    /// An owner's break-glass session: allowed while they are still the
    /// owner (read fresh by [`check`]).
    BreakGlass,
    Refuse,
}

/// The rule's word on a session acting in `team_id`, issued at `iat`. Pure.
/// An Okta session is allowed here; that it is still the member's linked
/// identity under the live setup is `sign_in::check_member_session`'s check.
pub fn session_decision(
    rule: &Rule,
    team_id: &str,
    iat: u64,
    proof: &SignInProof,
    app: bool,
) -> Decision {
    if !rule.require_okta {
        return Decision::Allow;
    }
    if super::oidc_signin::signed_in_with_okta(proof.via.as_deref()) {
        return Decision::Allow;
    }
    // A connected app acts only from a grant an Okta session made.
    if app {
        return Decision::Refuse;
    }
    if proof.break_glass.as_deref() == Some(team_id)
        && proof.mfa
        && proof.via.as_deref() == Some("password")
        && iat >= rule.okta_since
    {
        return Decision::BreakGlass;
    }
    Decision::Refuse
}

/// The words a refused person reads.
pub fn refusal_message(self_hosted: bool, team: &str) -> String {
    match (self_hosted, team.trim()) {
        (true, _) => {
            "Your organization requires Okta sign-in. Sign in with Okta to continue.".into()
        }
        (false, "") => "This team requires Okta sign-in. Sign in with Okta to continue.".into(),
        (false, t) => format!("{t} requires Okta sign-in. Sign in with Okta to continue."),
    }
}

/// The team's name as a refusal shows it ("" on a self-hosted install,
/// whose words say "your organization").
pub async fn team_words(state: &AppState, team_id: &str) -> String {
    if sign_in::applies() {
        return String::new();
    }
    sign_in::team_name(state, team_id).await
}

/// Where a refused person goes to sign in with Okta.
pub fn sign_in_url(app_url: &str, team: &str) -> String {
    format!(
        "{}/?signin_error={CODE}&team={}",
        app_url.trim_end_matches('/'),
        urlencoding::encode(team)
    )
}

fn app_url() -> &'static str {
    common::public_urls::app_url_from_env()
}

fn body(team: &str) -> Value {
    json!({
        "error": refusal_message(sign_in::applies(), team),
        "code": CODE,
        "team": team,
        "sign_in_url": sign_in_url(app_url(), team),
    })
}

/// The answer to a session the rule ended: 401, and its cookie cleared.
pub fn session_refused(team: &str) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, Json(body(team))).into_response();
    if let Ok(v) = HeaderValue::from_str(
        "__Host-coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
    ) {
        resp.headers_mut().append(header::SET_COOKIE, v);
    }
    resp
}

/// The answer to a request the rule refuses from a session that stays (a
/// switch into the team, a connection made from a break-glass session).
pub fn refused_json(team: &str) -> ApiError {
    (StatusCode::FORBIDDEN, Json(body(team)))
}

/// The member's role on their record now.
async fn member_role(state: &AppState, team: &str, user: &str) -> Result<Option<String>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(user))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out
        .item
        .filter(|i| s(i, "status") != "invited")
        .map(|i| s(&i, "role").to_string()))
}

/// The rule's check of one request (`sign_in::check_session` has read the
/// rule). A read that fails refuses the request.
pub async fn check(
    state: &AppState,
    rule: &Rule,
    claims: &Claims,
    proof: &SignInProof,
    app: bool,
) -> Result<(), Response> {
    match session_decision(rule, &claims.team_id, claims.iat, proof, app) {
        Decision::Allow => Ok(()),
        Decision::BreakGlass => match member_role(state, &claims.team_id, &claims.sub).await {
            Ok(Some(r)) if r == "owner" => Ok(()),
            Ok(_) => Err(session_refused(&team_words(state, &claims.team_id).await)),
            Err(e) => {
                error!(error = %e, "Member not read for a break-glass session; refused");
                Err(StatusCode::SERVICE_UNAVAILABLE.into_response())
            }
        },
        Decision::Refuse => Err(session_refused(&team_words(state, &claims.team_id).await)),
    }
}

/// [`check`] with the rule read here, for the places that take a session
/// outside the middleware (linking Okta, connecting GitHub, the support
/// view, the answer links in emails).
pub async fn check_claims(
    state: &AppState,
    claims: &Claims,
    proof: &SignInProof,
    app: bool,
) -> Result<(), Response> {
    let rule = sign_in::team_rule(state, &claims.team_id)
        .await
        .map_err(|e| {
            error!(error = %e, "Sign-in rules not read; refused");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        })?;
    check(state, &rule, claims, proof, app).await
}

// ── Sign-ins other than Okta ────────────────────────────────────────

/// A sign-in the rule refuses: the team's words and the code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub team: String,
    pub code: &'static str,
}

impl Refused {
    pub fn message(&self) -> String {
        if self.code == NEEDS_2FA {
            return "A break-glass sign-in needs your second factor. Set up two-factor sign-in on your account first, or sign in with Okta.".into();
        }
        refusal_message(sign_in::applies(), &self.team)
    }

    /// For a sign-in that answers with a redirect (Google, GitHub,
    /// Microsoft): the sign-in screen, with the team named.
    pub fn redirect(&self, provider: Provider, app_url: &str) -> Response {
        warn!(
            provider = provider.slug(),
            code = self.code,
            "Sign-in refused: the team requires Okta sign-in"
        );
        let url = format!(
            "{}&provider={}",
            sign_in_url(app_url, &self.team),
            provider.slug()
        );
        let mut resp = (StatusCode::FOUND, [(header::LOCATION, url)]).into_response();
        for c in super::oauth_signin::clear_cookies(provider) {
            if let Ok(v) = HeaderValue::from_str(&c) {
                resp.headers_mut().append(header::SET_COOKIE, v);
            }
        }
        resp
    }

    /// For a password sign-in (JSON).
    pub fn json(&self) -> Response {
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": self.message(),
                "code": self.code,
                "team": self.team,
            })),
        )
            .into_response()
    }
}

async fn refused(state: &AppState, team_id: &str) -> Refused {
    Refused {
        team: team_words(state, team_id).await,
        code: CODE,
    }
}

/// How a sign-in other than Okta may go on to a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Issue {
    /// No rule (or an Okta sign-in): as before.
    Plain,
    /// The owner's break-glass sign-in: its second factor is required, and
    /// it is logged before its session is made.
    BreakGlass,
}

/// The rule's word on a sign-in, before its second step or its session.
/// Pure: `role` is the member's role on their record.
pub fn issue_decision(
    rule: &Rule,
    via: &str,
    break_glass: bool,
    role: Option<&str>,
) -> Result<Issue, &'static str> {
    if !rule.require_okta || super::oidc_signin::signed_in_with_okta(Some(via)) {
        return Ok(Issue::Plain);
    }
    if via == "password" && break_glass && role == Some("owner") {
        return Ok(Issue::BreakGlass);
    }
    Err(CODE)
}

/// The last check before a sign-in to `team_id` gets its second step or a
/// session, on every way in. `Ok(Err)` is the refusal to send.
pub async fn issue_check(
    state: &AppState,
    team_id: &str,
    user_id: &str,
    via: &str,
    break_glass: bool,
) -> Result<Result<Issue, Refused>, String> {
    let rule = sign_in::team_rule(state, team_id).await?;
    let role = if rule.require_okta && break_glass {
        member_role(state, team_id, user_id).await?
    } else {
        None
    };
    Ok(
        match issue_decision(&rule, via, break_glass, role.as_deref()) {
            Ok(i) => Ok(i),
            Err(_) => Err(refused(state, team_id).await),
        },
    )
}

/// Self-hosted, before the join rules run (which may accept an invite or
/// join a domain): while the organization requires Okta, only an Okta
/// sign-in, or the owner's break-glass password sign-in to the record they
/// already have, goes on. `Ok(Err)` is the refusal to send.
pub async fn selfhost_pre_gate(
    state: &AppState,
    email: &str,
    via: &str,
    break_glass: bool,
) -> Result<Result<(), Refused>, String> {
    if !sign_in::applies() {
        return Ok(Ok(()));
    }
    let rule = sign_in::team_rule(state, "").await?;
    if !rule.require_okta || super::oidc_signin::signed_in_with_okta(Some(via)) {
        return Ok(Ok(()));
    }
    let refuse = Refused {
        team: String::new(),
        code: CODE,
    };
    if via != "password" || !break_glass {
        return Ok(Err(refuse));
    }
    let facts = sign_in::facts_for(state, email).await?;
    Ok(match sign_in::decide_join(&facts, chrono::Utc::now()) {
        sign_in::Join::Member { .. } => Ok(()),
        _ => Err(refuse),
    })
}

/// The teams among a person's records (by `pk`) that require Okta now.
pub async fn gated_teams(state: &AppState, items: &[Item]) -> Result<HashSet<String>, String> {
    let mut seen = HashSet::new();
    let mut gated = HashSet::new();
    for i in items {
        let team = s(i, "pk");
        if team.is_empty() || !seen.insert(team.to_string()) {
            continue;
        }
        if sign_in::team_rule(state, team).await?.require_okta {
            gated.insert(team.to_string());
        }
    }
    Ok(gated)
}

/// What a sign-in other than Okta may land on (hosted).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filtered {
    /// The records and invites of teams that don't require Okta.
    pub kept: Vec<Item>,
    /// A team that requires Okta and was held back, for the refusal's words.
    pub refused: Option<String>,
    /// A break-glass sign-in: the owner's own record on a team that
    /// requires Okta, which the sign-in lands on.
    pub break_glass: Option<Item>,
}

/// Holds back every record and invite of a team in `gated`. Pure. With
/// `break_glass`, the person's owner record on such a team is the landing
/// (never an invite, never another role).
pub fn filter(items: &[Item], gated: &HashSet<String>, break_glass: bool) -> Filtered {
    let mut out = Filtered::default();
    for i in items {
        let team = s(i, "pk");
        if !gated.contains(team) {
            out.kept.push(i.clone());
            continue;
        }
        let owner = s(i, "sk").starts_with("USER#")
            && s(i, "status") != "invited"
            && s(i, "role") == "owner";
        if break_glass && owner && out.break_glass.is_none() {
            out.break_glass = Some(i.clone());
            continue;
        }
        if out.refused.is_none() {
            out.refused = Some(team.to_string());
        }
    }
    out
}

/// The words for a hosted sign-in that found only teams requiring Okta.
pub async fn refused_for(state: &AppState, team: &str) -> Refused {
    refused(state, team).await
}

/// Writes the owner's break-glass sign-in to the team's log and to the
/// Sign-in rules page's list, before its session is made. A write that fails
/// refuses the sign-in.
pub async fn record_break_glass(
    state: &AppState,
    team_id: &str,
    user_id: &str,
    email: &str,
    factor: &str,
) -> Result<(), String> {
    let actor = Claims {
        sub: user_id.to_string(),
        team_id: team_id.to_string(),
        email: email.to_string(),
        role: "owner".into(),
        github_login: None,
        aud: "coderhelm-dashboard".into(),
        exp: 0,
        iat: 0,
        support_view: None,
        custom: None,
        api_key: None,
        held: false,
        via_okta: false,
        okta_sub: None,
    };
    super::roles::record(
        state,
        &actor,
        super::roles::AuditEntry {
            action: "sign_in_break_glass",
            role_id: "",
            role_name: "",
            detail: format!(
                "Break-glass sign-in by the owner with their password and {factor}, while Okta sign-in is required"
            ),
        },
    )
    .await
    .map_err(|(_, Json(v))| v["error"].as_str().unwrap_or("not written").to_string())?;
    let now = chrono::Utc::now();
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item(
            "sk",
            attr_s(&format!("{BREAK_GLASS_PREFIX}{at}#{}", ulid::Ulid::new())),
        )
        .item("at", attr_s(&at))
        .item("user_id", attr_s(user_id))
        .item("email", attr_s(email))
        .item("factor", attr_s(factor))
        .item(
            "ttl",
            AttributeValue::N(
                (now + chrono::Duration::days(BREAK_GLASS_DAYS))
                    .timestamp()
                    .to_string(),
            ),
        )
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))?;
    info!(team_id, "Break-glass sign-in logged");
    Ok(())
}

/// The team's most recent break-glass sign-ins, newest first.
pub async fn break_glass_log(state: &AppState, team_id: &str) -> Result<Vec<Value>, String> {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":p", attr_s(BREAK_GLASS_PREFIX))
        .scan_index_forward(false)
        .limit(BREAK_GLASS_SHOWN)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.items().iter().map(break_glass_view).collect())
}

pub fn break_glass_view(i: &Item) -> Value {
    json!({
        "at": s(i, "at"),
        "who": s(i, "email"),
        "email": s(i, "email"),
        "factor": s(i, "factor"),
    })
}

// ── New members through Okta ────────────────────────────────────────

/// What an Okta sign-in of an identity no member linked finds on the team.
#[derive(Debug, Clone, PartialEq)]
pub enum InviteFor {
    /// A live invite for exactly this email: accepted through Okta.
    Accept(Item),
    /// A member already has this email: Okta never takes over a record.
    ExistingMember,
    None,
}

/// The pending invite an Okta sign-in may accept: the team's live invite
/// for exactly `email` (Okta's verified email), when no member record on the
/// team has that email. Pure.
pub fn invite_for(members: &[Item], email: &str, now: chrono::DateTime<chrono::Utc>) -> InviteFor {
    let want = super::auth::normalize_email(email);
    if want.is_empty() || !want.contains('@') {
        return InviteFor::None;
    }
    let same = |i: &&Item| super::auth::normalize_email(s(i, "email")) == want;
    let users = || members.iter().filter(|i| s(i, "sk").starts_with("USER#"));
    if users().filter(same).any(|i| s(i, "status") != "invited") {
        return InviteFor::ExistingMember;
    }
    match users()
        .filter(same)
        .find(|i| s(i, "status") == "invited" && !super::users::invite_expired(i, now))
    {
        Some(i) => InviteFor::Accept(i.clone()),
        None => InviteFor::None,
    }
}

/// The member record an invite becomes when accepted through Okta, linked
/// to the Okta identity that accepted it.
pub fn invited_member(
    invite: &Item,
    user_id: &str,
    name: &str,
    issuer: &str,
    sub: &str,
    now: &str,
) -> Item {
    let team = s(invite, "pk");
    let email = s(invite, "email");
    let role = match s(invite, "role") {
        "" | "owner" => "member",
        r => r,
    };
    HashMap::from([
        ("pk".to_string(), attr_s(team)),
        ("sk".to_string(), attr_s(user_id)),
        ("email".to_string(), attr_s(email)),
        ("name".to_string(), attr_s(name)),
        ("role".to_string(), attr_s(role)),
        ("status".to_string(), attr_s("active")),
        ("avatar_url".to_string(), attr_s("")),
        ("invited_by".to_string(), attr_s(s(invite, "invited_by"))),
        ("created_at".to_string(), attr_s(now)),
        ("updated_at".to_string(), attr_s(now)),
        (
            "gsi2pk".to_string(),
            attr_s(&format!("EMAIL#{}", super::auth::normalize_email(email))),
        ),
        ("gsi2sk".to_string(), attr_s(team)),
        ("okta_issuer".to_string(), attr_s(issuer)),
        ("okta_sub".to_string(), attr_s(sub)),
        ("okta_linked_at".to_string(), attr_s(now)),
    ])
}

/// Accepts `invite` through Okta in one transaction: the new member record
/// (only if no record has its id), the identity's link row (only if no
/// member holds it), and the invite removed (only while it is still one).
/// Any of them failing leaves everything as it was.
pub async fn accept_invite(state: &AppState, member: &Item, invite: &Item) -> Result<(), String> {
    use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem};
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let team = s(member, "pk");
    let (row_pk, row_sk) =
        super::oidc_signin::link_row(team, s(member, "okta_issuer"), s(member, "okta_sub"));
    let items = vec![
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(&state.config.users_table_name)
                    .set_item(Some(member.clone()))
                    .condition_expression("attribute_not_exists(pk)")
                    .build()
                    .map_err(e)?,
            )
            .build(),
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(&state.config.settings_table_name)
                    .item("pk", attr_s(&row_pk))
                    .item("sk", attr_s(&row_sk))
                    .item("member", attr_s(s(member, "sk")))
                    .condition_expression("attribute_not_exists(pk)")
                    .build()
                    .map_err(e)?,
            )
            .build(),
        TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name(&state.config.users_table_name)
                    .key("pk", attr_s(team))
                    .key("sk", attr_s(s(invite, "sk")))
                    .condition_expression("#st = :inv")
                    .expression_attribute_names("#st", "status")
                    .expression_attribute_values(":inv", attr_s("invited"))
                    .build()
                    .map_err(e)?,
            )
            .build(),
    ];
    state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(items))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

// ── Settings, Sign-in rules ─────────────────────────────────────────

/// What turning the rule on needs, each part shown on the page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Readiness {
    /// A live Okta setup exists.
    pub set_up: bool,
    /// It is turned on.
    pub enabled: bool,
    /// Hosted: every one of its domains is proven by this team (always true
    /// on a self-hosted install, which proves none).
    pub domains_verified: bool,
    /// The caller's own session came from this team's Okta.
    pub session_is_okta: bool,
    pub is_owner: bool,
}

impl Readiness {
    pub fn ready(&self) -> bool {
        self.first_missing().is_none()
    }

    /// The first unmet condition: status, code and words.
    pub fn first_missing(&self) -> Option<(StatusCode, &'static str, &'static str)> {
        if !self.is_owner {
            return Some((
                StatusCode::FORBIDDEN,
                "owner_only",
                "Only the owner can turn Require Okta sign-in on or off.",
            ));
        }
        if !self.set_up || !self.enabled {
            return Some((
                StatusCode::CONFLICT,
                "okta_not_on",
                "Set up Okta sign-in and turn it on first.",
            ));
        }
        if !self.domains_verified {
            return Some((
                StatusCode::CONFLICT,
                "domains_not_verified",
                "Verify each email domain first: its TXT record must still be in DNS.",
            ));
        }
        if !self.session_is_okta {
            return Some((
                StatusCode::CONFLICT,
                "sign_in_with_okta_first",
                "Sign in with your team's Okta first, then turn this on. That proves Okta sign-in works for you before everyone depends on it.",
            ));
        }
        None
    }

    pub fn view(&self) -> Value {
        json!({
            "set_up": self.set_up,
            "enabled": self.enabled,
            "domains_verified": self.domains_verified,
            "session_is_okta": self.session_is_okta,
            "is_owner": self.is_owner,
        })
    }
}

/// What turning the rule on needs right now for this caller. `recheck_dns`
/// reads each domain's TXT record again (turning on); the page shows the
/// stored proofs.
pub async fn readiness(
    state: &AppState,
    claims: &Claims,
    proof: &SignInProof,
    live: Option<&super::oidc_signin::OktaSetup>,
    recheck_dns: bool,
) -> Result<Readiness, String> {
    let domains_verified = match live {
        None => false,
        Some(_) if sign_in::applies() => true,
        Some(l) => super::oidc_signin::domains_proven(state, l, recheck_dns).await?,
    };
    Ok(Readiness {
        set_up: live.is_some(),
        enabled: live.is_some_and(|l| l.enabled),
        domains_verified,
        session_is_okta: claims.via_okta
            && super::oidc_signin::signed_in_with_okta(proof.via.as_deref())
            && claims.support_view.is_none(),
        is_owner: claims.role == "owner" && claims.support_view.is_none(),
    })
}

/// Whether a member's Okta link is to the live setup's issuer.
pub fn okta_linked(member: &Item, issuer: Option<&str>) -> bool {
    issuer.is_some_and(|iss| !s(member, "okta_sub").is_empty() && s(member, "okta_issuer") == iss)
}

/// The active members not linked to the live Okta, for the page and the
/// warning when the rule is turned on.
pub fn unlinked(members: &[Item], issuer: Option<&str>) -> Vec<Value> {
    members
        .iter()
        .filter(|i| s(i, "sk").starts_with("USER#") && s(i, "status") != "invited")
        .filter(|i| !okta_linked(i, issuer))
        .map(|i| {
            json!({
                "user_id": s(i, "sk").trim_start_matches("USER#"),
                "email": s(i, "email"),
                "name": s(i, "name"),
                "role": s(i, "role"),
            })
        })
        .collect()
}

/// While the rule is on, Okta can't be turned off, removed, moved to another
/// issuer or lose a domain: the owner turns the rule off first, so nobody is
/// locked out by a settings change.
pub fn setup_change_refused(rule: &Rule) -> Option<ApiError> {
    rule.require_okta.then(|| {
        (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "Turn off Require Okta sign-in first (Settings, Sign-in rules). Changing Okta while it is required would lock everyone out.",
                "code": "okta_required_on",
            })),
        )
    })
}

/// The time the rule's sessions are measured from.
pub fn since_now() -> u64 {
    now_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(on: bool, since: u64) -> Rule {
        Rule {
            require_okta: on,
            okta_since: since,
            ..Rule::default()
        }
    }

    fn proof(via: &str, mfa: bool, bg: Option<&str>) -> SignInProof {
        SignInProof::new(mfa, via).with_break_glass(bg)
    }

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), attr_s(v)))
            .collect()
    }

    #[test]
    fn with_the_rule_on_only_okta_and_the_owners_logged_break_glass_sessions_stand() {
        let on = rule(true, 1_000);
        let t = "TEAM#a";
        // Every other way in is refused, with or without a second factor.
        for via in ["password", "google", "github", "microsoft"] {
            for mfa in [false, true] {
                assert_eq!(
                    session_decision(&on, t, 2_000, &proof(via, mfa, None), false),
                    Decision::Refuse,
                    "{via} {mfa}"
                );
            }
        }
        // A token from before proofs existed is refused too.
        assert_eq!(
            session_decision(&on, t, 2_000, &SignInProof::default(), false),
            Decision::Refuse
        );
        // Okta: allowed here (the link is checked against the member record).
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("okta", false, None), false),
            Decision::Allow
        );
        // Break-glass: password, a second factor, this team, after the rule.
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("password", true, Some(t)), false),
            Decision::BreakGlass
        );
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("password", false, Some(t)), false),
            Decision::Refuse,
            "never without a second factor"
        );
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("google", true, Some(t)), false),
            Decision::Refuse,
            "password only"
        );
        assert_eq!(
            session_decision(
                &on,
                t,
                2_000,
                &proof("password", true, Some("TEAM#b")),
                false
            ),
            Decision::Refuse,
            "another team's break-glass"
        );
        assert_eq!(
            session_decision(&on, t, 999, &proof("password", true, Some(t)), false),
            Decision::Refuse,
            "from before the rule was turned on"
        );
        // Connected apps: only from an Okta session's grant.
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("password", true, Some(t)), true),
            Decision::Refuse
        );
        assert_eq!(
            session_decision(&on, t, 2_000, &proof("okta", false, None), true),
            Decision::Allow
        );
    }

    #[test]
    fn with_the_rule_off_every_way_in_is_as_before() {
        let off = rule(false, 1_000);
        for via in ["password", "google", "github", "microsoft", "okta"] {
            assert_eq!(
                session_decision(&off, "TEAM#a", 10, &proof(via, false, None), false),
                Decision::Allow
            );
            assert_eq!(
                session_decision(&off, "TEAM#a", 10, &proof(via, false, None), true),
                Decision::Allow
            );
            assert_eq!(issue_decision(&off, via, false, None), Ok(Issue::Plain));
        }
    }

    #[test]
    fn a_sign_in_other_than_okta_is_refused_unless_it_is_the_owners_break_glass() {
        let on = rule(true, 1);
        assert_eq!(issue_decision(&on, "okta", false, None), Ok(Issue::Plain));
        for via in ["google", "github", "microsoft"] {
            assert_eq!(
                issue_decision(&on, via, true, Some("owner")),
                Err(CODE),
                "{via}"
            );
        }
        assert_eq!(
            issue_decision(&on, "password", false, Some("owner")),
            Err(CODE)
        );
        assert_eq!(
            issue_decision(&on, "password", true, Some("admin")),
            Err(CODE)
        );
        assert_eq!(issue_decision(&on, "password", true, None), Err(CODE));
        assert_eq!(
            issue_decision(&on, "password", true, Some("owner")),
            Ok(Issue::BreakGlass)
        );
    }

    #[test]
    fn a_hosted_sign_in_holds_back_only_the_teams_that_require_okta() {
        let a = item(&[("pk", "TEAM#a"), ("sk", "USER#1"), ("role", "owner")]);
        let a_inv = item(&[
            ("pk", "TEAM#c"),
            ("sk", "USER#invite_1"),
            ("status", "invited"),
        ]);
        let b = item(&[("pk", "TEAM#b"), ("sk", "USER#1"), ("role", "member")]);
        let gated: HashSet<String> = ["TEAM#a".to_string(), "TEAM#c".to_string()].into();
        let f = filter(&[a.clone(), a_inv.clone(), b.clone()], &gated, false);
        assert_eq!(f.kept, vec![b.clone()]);
        assert_eq!(f.refused.as_deref(), Some("TEAM#a"));
        assert!(f.break_glass.is_none());
        // Only gated teams: nothing kept, so no personal team is made.
        let f = filter(std::slice::from_ref(&a), &gated, false);
        assert!(f.kept.is_empty() && f.refused.is_some());
        // Break-glass lands on the owner's own record, never an invite.
        let f = filter(&[a.clone(), a_inv.clone(), b.clone()], &gated, true);
        assert_eq!(f.break_glass, Some(a.clone()));
        assert_eq!(f.refused.as_deref(), Some("TEAM#c"));
        let admin = item(&[("pk", "TEAM#a"), ("sk", "USER#1"), ("role", "admin")]);
        assert!(filter(&[admin], &gated, true).break_glass.is_none());
        // No gated team: everything kept.
        let f = filter(&[a.clone(), b.clone()], &HashSet::new(), false);
        assert_eq!(f.kept.len(), 2);
        assert!(f.refused.is_none());
    }

    #[test]
    fn okta_accepts_only_a_live_invite_for_exactly_its_email_and_never_takes_over_a_member() {
        let now = chrono::Utc::now();
        let fresh = now.to_rfc3339();
        let invite = item(&[
            ("pk", "TEAM#a"),
            ("sk", "USER#invite_1"),
            ("status", "invited"),
            ("email", "new@example.com"),
            ("invited_at", &fresh),
            ("role", "admin"),
        ]);
        assert_eq!(
            invite_for(std::slice::from_ref(&invite), "New@Example.com", now),
            InviteFor::Accept(invite.clone())
        );
        // Another email (Okta's verified email is the invited one, or nothing).
        assert_eq!(
            invite_for(std::slice::from_ref(&invite), "other@example.com", now),
            InviteFor::None
        );
        assert_eq!(
            invite_for(std::slice::from_ref(&invite), "", now),
            InviteFor::None
        );
        // An expired invite.
        let old = item(&[
            ("pk", "TEAM#a"),
            ("sk", "USER#invite_2"),
            ("status", "invited"),
            ("email", "new@example.com"),
            ("invited_at", "2020-01-01T00:00:00Z"),
        ]);
        assert_eq!(invite_for(&[old], "new@example.com", now), InviteFor::None);
        // A member with that email already: never linked or taken over.
        let member = item(&[
            ("pk", "TEAM#a"),
            ("sk", "USER#9"),
            ("email", "new@example.com"),
        ]);
        assert_eq!(
            invite_for(&[invite.clone(), member], "new@example.com", now),
            InviteFor::ExistingMember
        );
        // The record it becomes: the invite's email and role (never owner),
        // linked to this identity, active.
        let m = invited_member(
            &invite,
            "USER#okta_abc",
            "New",
            "https://idp.example.com",
            "abc",
            &fresh,
        );
        for (k, v) in [
            ("pk", "TEAM#a"),
            ("sk", "USER#okta_abc"),
            ("email", "new@example.com"),
            ("role", "admin"),
            ("status", "active"),
            ("okta_issuer", "https://idp.example.com"),
            ("okta_sub", "abc"),
            ("gsi2pk", "EMAIL#new@example.com"),
            ("gsi2sk", "TEAM#a"),
        ] {
            assert_eq!(s(&m, k), v, "{k}");
        }
        let owner_invite = item(&[("pk", "TEAM#a"), ("sk", "USER#invite_3"), ("role", "owner")]);
        assert_eq!(
            s(
                &invited_member(&owner_invite, "USER#x", "", "i", "s", &fresh),
                "role"
            ),
            "member"
        );
    }

    #[test]
    fn turning_the_rule_on_needs_every_condition_and_names_the_first_missing() {
        let all = Readiness {
            set_up: true,
            enabled: true,
            domains_verified: true,
            session_is_okta: true,
            is_owner: true,
        };
        assert!(all.ready());
        let code = |r: Readiness| r.first_missing().map(|(_, c, _)| c);
        assert_eq!(
            code(Readiness {
                is_owner: false,
                ..all
            }),
            Some("owner_only")
        );
        assert_eq!(
            code(Readiness {
                set_up: false,
                enabled: false,
                ..all
            }),
            Some("okta_not_on")
        );
        assert_eq!(
            code(Readiness {
                enabled: false,
                ..all
            }),
            Some("okta_not_on")
        );
        assert_eq!(
            code(Readiness {
                domains_verified: false,
                ..all
            }),
            Some("domains_not_verified")
        );
        assert_eq!(
            code(Readiness {
                session_is_okta: false,
                ..all
            }),
            Some("sign_in_with_okta_first")
        );
        assert_eq!(
            Readiness {
                is_owner: false,
                ..all
            }
            .first_missing()
            .unwrap()
            .0,
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn unlinked_members_are_listed_and_invites_are_not() {
        let iss = "https://idp.example.com";
        let linked = item(&[
            ("pk", "T"),
            ("sk", "USER#1"),
            ("okta_issuer", iss),
            ("okta_sub", "s1"),
        ]);
        let other_issuer = item(&[
            ("pk", "T"),
            ("sk", "USER#2"),
            ("okta_issuer", "https://old.example.com"),
            ("okta_sub", "s2"),
            ("email", "b@example.com"),
        ]);
        let none = item(&[("pk", "T"), ("sk", "USER#3"), ("email", "c@example.com")]);
        let invite = item(&[("pk", "T"), ("sk", "USER#invite_1"), ("status", "invited")]);
        let list = unlinked(&[linked, other_issuer, none, invite], Some(iss));
        let emails: Vec<&str> = list.iter().map(|v| v["email"].as_str().unwrap()).collect();
        assert_eq!(emails, ["b@example.com", "c@example.com"]);
        assert_eq!(list[0]["user_id"], "2");
    }

    #[test]
    fn a_refusal_names_the_team_and_links_to_okta_sign_in() {
        assert_eq!(
            refusal_message(false, "Acme"),
            "Acme requires Okta sign-in. Sign in with Okta to continue."
        );
        assert!(refusal_message(false, " ").starts_with("This team requires"));
        assert!(refusal_message(true, "x").starts_with("Your organization requires"));
        assert_eq!(
            sign_in_url("https://app.example.com/", "A & B"),
            "https://app.example.com/?signin_error=okta_required&team=A%20%26%20B"
        );
        let r = session_refused("Acme");
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        let cookies: Vec<_> = r.headers().get_all(header::SET_COOKIE).iter().collect();
        assert!(cookies
            .iter()
            .any(|c| c.to_str().unwrap().contains("Max-Age=0")));
        assert!(setup_change_refused(&rule(true, 1)).is_some());
        assert!(setup_change_refused(&rule(false, 1)).is_none());
    }

    /// The code of a source file, without its tests.
    fn code(src: &str) -> &str {
        src.find("#[cfg(test)]\nmod ").map_or(src, |i| &src[..i])
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

    /// Every place a session token is made, by file. A new one fails this
    /// test until it is gated by the rule and listed here.
    #[test]
    fn every_place_a_session_is_made_is_known() {
        let count = |src: &str| code(src).matches("create_session_token(").count();
        for (file, src, want) in [
            ("auth.rs", include_str!("auth.rs"), 6),
            ("api.rs", include_str!("api.rs"), 1),
            ("sign_in.rs", include_str!("sign_in.rs"), 3),
            ("oidc_signin.rs", include_str!("oidc_signin.rs"), 0),
            ("users.rs", include_str!("users.rs"), 0),
            ("mcp_oauth.rs", include_str!("mcp_oauth.rs"), 0),
            ("github_hybrid.rs", include_str!("github_hybrid.rs"), 0),
            ("hybrid.rs", include_str!("hybrid.rs"), 0),
            ("support_access.rs", include_str!("support_access.rs"), 0),
            (
                "middleware/auth.rs",
                include_str!("../middleware/auth.rs"),
                1,
            ),
            ("mcp_server.rs", include_str!("../mcp_server.rs"), 0),
        ] {
            assert_eq!(count(src), want, "{file}");
        }
    }

    /// Every way in asks the rule before anything is joined or made and
    /// before its second step or its session.
    #[test]
    fn every_way_in_asks_the_rule_first() {
        let auth = code(include_str!("auth.rs"));
        // Google: self-hosted before the join rules; hosted, gated teams held
        // back before invites are accepted; the last word before the step.
        let g = body(auth, "google_callback_inner");
        assert!(before(
            g,
            "selfhost_pre_gate(state, &email, \"google\"",
            "selfhost_landing("
        ));
        assert!(before(
            g,
            "require_okta::filter(",
            "accept_pending_invites("
        ));
        assert!(before(g, "require_okta::filter(", "Google user created"));
        assert_eq!(g.matches("require_okta::issue_check(").count(), 2);
        let hosted = &g[g.find("Google user created").unwrap()..];
        assert!(before(
            hosted,
            "require_okta::issue_check(",
            "sign_in::second_step("
        ));
        let selfhosted = &g[..g.find("Open beta").unwrap()];
        assert!(before(
            selfhosted,
            "require_okta::issue_check(",
            "sign_in::second_step("
        ));
        // GitHub: connecting from a session that still stands only; the
        // same holds for a sign-in, and no reader joins a gated team.
        let gh = body(auth, "github_sign_in_with_token");
        assert!(before(
            gh,
            "check_member_session(state, &claims)",
            "SET github_id = :gid"
        ));
        assert!(before(
            gh,
            "require_okta::check_claims(",
            "SET github_id = :gid"
        ));
        assert!(before(
            gh,
            "selfhost_pre_gate(state, email, \"github\"",
            "selfhost_landing("
        ));
        assert!(before(gh, "require_okta::filter(", "github_landing("));
        assert!(before(gh, ".require_okta", "reader_team = Some(owner)"));
        assert!(before(
            gh,
            "require_okta::issue_check(",
            "sign_in::second_step("
        ));
        // Microsoft and Okta's shared ending.
        let o = body(auth, "finish_oidc");
        assert!(before(
            o,
            "selfhost_pre_gate(state, email, via",
            "selfhost_landing(state, email, &person.new_user_id"
        ));
        assert!(before(
            o,
            "require_okta::filter(",
            "accept_pending_invites("
        ));
        assert!(before(
            o,
            "require_okta::issue_check(",
            "sign_in::second_step("
        ));
        // Password: before the join rules, held back on hosted, then the last
        // word, the second factor, the break-glass log, the session.
        let p = body(auth, "issue_session_from_cognito");
        assert!(before(
            p,
            "selfhost_pre_gate(state, email, \"password\"",
            "selfhost_landing("
        ));
        assert!(before(
            p,
            "require_okta::filter(",
            "accept_pending_invites("
        ));
        assert!(!p.contains("session_json("));
        let ps = body(auth, "password_session");
        assert!(before(
            ps,
            "require_okta::issue_check(",
            "sign_in::password_step("
        ));
        assert!(before(ps, "sign_in::password_step(", "record_break_glass("));
        assert!(before(ps, "record_break_glass(", "session_json("));
        assert!(before(ps, "if !proof.mfa", "record_break_glass("));
        assert!(body(auth, "login_email").contains("body.break_glass"));
        assert!(body(auth, "mfa_verify").contains("body.break_glass"));
        // The second step: the rule's last word and the log before the session.
        let si = code(include_str!("sign_in.rs"));
        let v = body(si, "verify_second_step");
        assert!(before(v, "use_app_code(", "require_okta::issue_check("));
        assert!(before(
            v,
            "require_okta::issue_check(",
            "create_session_token("
        ));
        assert!(before(v, "record_break_glass(", "create_session_token("));
        assert!(body(si, "password_step").contains("step.break_glass && !passed"));
        // A switch into a gated team, and the team list's invites.
        let api = code(include_str!("api.rs"));
        let sw = body(api, "switch_team");
        assert!(before(sw, ".require_okta", "create_session_token("));
        let lt = body(api, "list_teams");
        assert!(before(lt, "gated.contains(&invite_tid)", ".put_item()"));
    }

    /// Every place a session is used checks the rule: the middleware (and
    /// its support view), connected apps, linking Okta, the email answer
    /// links, and new apps.
    #[test]
    fn every_place_a_session_is_used_checks_the_rule() {
        let mw = code(include_str!("../middleware/auth.rs"));
        let ra = body(mw, "require_auth");
        assert!(ra.contains("sign_in::check_session(&state, &claims, &proof, handler, false)"));
        assert!(before(
            ra,
            "require_okta::check_claims(",
            "support_access::check_session("
        ));
        assert!(body(mw, "app_request")
            .contains("sign_in::check_session(state, &claims, &proof, handler, true)"));
        let sa = body(mw, "session_answers_as");
        assert!(sa.contains("check_member_session(") && sa.contains("require_okta::check_claims("));
        let mcp = code(include_str!("../mcp_server.rs"));
        assert_eq!(mcp.matches("check_session(").count(), 2);
        assert_eq!(mcp.matches("None, true").count(), 2);
        let oidc = code(include_str!("oidc_signin.rs"));
        assert!(body(oidc, "session_of").contains("require_okta::check_claims("));
        for (src, f) in [
            (include_str!("agent_policy.rs"), "link_answer"),
            (include_str!("plugin_pkgs.rs"), "email_approve"),
        ] {
            let b = body(code(src), f);
            assert!(b.contains("session_answers_as("), "{f}");
            assert!(!b.contains("auth::signed_in_as("), "{f}");
        }
        let d = body(code(include_str!("mcp_oauth.rs")), "decide_request");
        assert!(before(d, ".require_okta", "oauth::approve("));
        // An invite made while the rule is on waits to be accepted with Okta.
        let inv = body(code(include_str!("users.rs")), "invite_user");
        assert!(inv.contains("existing_sub.filter(|_| !okta_only)"));
    }
}
