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

/// Where a refused person goes to sign in with Okta. The URL carries the
/// team's id only (so the owner's break-glass sign-in can ask for that
/// team), never its name: the sign-in page shows no text from a URL.
pub fn sign_in_url(app_url: &str, team_id: &str) -> String {
    let base = format!("{}/?signin_error={CODE}", app_url.trim_end_matches('/'));
    match team_id.trim() {
        "" => base,
        t => format!("{base}&team_id={}", urlencoding::encode(t)),
    }
}

fn app_url() -> &'static str {
    common::public_urls::app_url_from_env()
}

/// The answer to a session the rule ended: 401, and its cookie cleared.
pub fn session_refused(r: &Refused) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, Json(r.body())).into_response();
    if let Ok(v) = HeaderValue::from_str(
        "__Host-coderhelm_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
    ) {
        resp.headers_mut().append(header::SET_COOKIE, v);
    }
    resp
}

/// The answer to a request the rule refuses from a session that stays (a
/// switch into the team, a connection made from a break-glass session).
pub fn refused_json(r: &Refused) -> ApiError {
    (StatusCode::FORBIDDEN, Json(r.body()))
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
            Ok(_) => Err(session_refused(&refused(state, &claims.team_id).await)),
            Err(e) => {
                error!(error = %e, "Member not read for a break-glass session; refused");
                Err(StatusCode::SERVICE_UNAVAILABLE.into_response())
            }
        },
        Decision::Refuse => Err(session_refused(&refused(state, &claims.team_id).await)),
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

/// A short-lived stream token (plan chat, brain) at its use: the session it
/// was minted from must still stand (the member check, and the team's Okta
/// rule), so ending a session ends its tokens at once.
pub async fn check_scoped(
    state: &AppState,
    claims: &Claims,
    minted_by: &SignInProof,
) -> Result<(), Response> {
    let claims = super::oidc_signin::mark_okta(
        claims.clone(),
        minted_by.via.as_deref(),
        minted_by.okta_sub.clone(),
    );
    sign_in::check_member_session(state, &claims).await?;
    check_claims(state, &claims, minted_by, false).await
}

// ── Sign-ins other than Okta ────────────────────────────────────────

/// A sign-in the rule refuses: the team's words and the code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// The team's name, for the server's own message ("" on self-hosted).
    pub team: String,
    pub team_id: String,
    pub code: &'static str,
}

impl Refused {
    fn body(&self) -> Value {
        json!({
            "error": self.message(),
            "code": self.code,
            "team": self.team,
            "team_id": self.team_id,
            "sign_in_url": sign_in_url(app_url(), &self.team_id),
        })
    }

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
            sign_in_url(app_url, &self.team_id),
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
        (StatusCode::FORBIDDEN, Json(self.body())).into_response()
    }
}

/// The refusal for `team_id`, with its name for the server's message.
pub async fn refused(state: &AppState, team_id: &str) -> Refused {
    Refused {
        team: team_words(state, team_id).await,
        team_id: team_id.to_string(),
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
        team_id: String::new(),
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

/// A break-glass sign-in, asked for: the team it is for (`None`: the
/// person's only owner record on a team that requires Okta).
pub type BreakGlassAsk<'a> = Option<Option<&'a str>>;

/// Holds back every record and invite of a team in `gated`. Pure. With a
/// break-glass ask, the person's owner record on the team asked for (or,
/// with none named, the first such team) is the landing (never an invite,
/// never another role).
pub fn filter(items: &[Item], gated: &HashSet<String>, break_glass: BreakGlassAsk) -> Filtered {
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
        let asked = match break_glass {
            None => false,
            Some(None) => true,
            Some(Some(t)) => t == team,
        };
        if asked && owner && out.break_glass.is_none() {
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

/// An address as an invite and Okta's verified email are compared: trimmed
/// and lowercase, nothing else. `+tag` addresses are other mailboxes here
/// (`normalize_email` folds them, which is right for finding a person's
/// records but never for handing one of them an invite).
pub fn exact_address(email: &str) -> String {
    email.trim().to_lowercase()
}

/// The pending invite an Okta sign-in may accept: the team's live invite
/// for exactly `email` (Okta's verified email, compared with
/// [`exact_address`]), when no member record on the team has that email or
/// one that folds to the same person. Pure.
pub fn invite_for(members: &[Item], email: &str, now: chrono::DateTime<chrono::Utc>) -> InviteFor {
    let want = exact_address(email);
    if want.is_empty() || !want.contains('@') {
        return InviteFor::None;
    }
    let folded = super::auth::normalize_email(email);
    let same_person = |i: &&Item| super::auth::normalize_email(s(i, "email")) == folded;
    let users = || members.iter().filter(|i| s(i, "sk").starts_with("USER#"));
    // Never next to an existing member of the same person (folded), so a
    // `+tag` address can't open a second record for someone on the team.
    if users()
        .filter(same_person)
        .any(|i| s(i, "status") != "invited")
    {
        return InviteFor::ExistingMember;
    }
    match users()
        .filter(|i| exact_address(s(i, "email")) == want)
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

/// The condition that `invite` is still exactly the invite that was read:
/// still an invite, for the same role, sent at the same time (an admin who
/// changed or re-sent it meanwhile makes the acceptance fail).
pub fn invite_unchanged(
    invite: &Item,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let mut names = HashMap::from([
        ("#st".to_string(), "status".to_string()),
        ("#r".to_string(), "role".to_string()),
    ]);
    let mut values = HashMap::from([(":inv".to_string(), attr_s("invited"))]);
    let mut parts = vec!["#st = :inv".to_string()];
    match invite.get("role") {
        Some(r) => {
            parts.push("#r = :r".into());
            values.insert(":r".into(), r.clone());
        }
        None => parts.push("attribute_not_exists(#r)".into()),
    }
    names.insert("#ia".into(), "invited_at".into());
    match invite.get("invited_at") {
        Some(t) => {
            parts.push("#ia = :ia".into());
            values.insert(":ia".into(), t.clone());
        }
        None => parts.push("attribute_not_exists(#ia)".into()),
    }
    (parts.join(" AND "), names, values)
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
                    .set_condition_expression(Some(invite_unchanged(invite).0))
                    .set_expression_attribute_names(Some(invite_unchanged(invite).1))
                    .set_expression_attribute_values(Some(invite_unchanged(invite).2))
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

// ── Writes that would narrow Okta sign-in ───────────────────────────

/// The record that holds a team's sign-in rules: the organization's on a
/// self-hosted install, else the team's own.
pub fn rule_key(team_id: &str) -> (String, String) {
    if sign_in::applies() {
        (sign_in::ORG_PK.to_string(), sign_in::ORG_SK.to_string())
    } else {
        (team_id.to_string(), sign_in::TEAM_RULE_SK.to_string())
    }
}

/// A transaction part that holds only while the team's Okta rule is off.
pub fn rule_off_check(
    state: &AppState,
    team_id: &str,
) -> Result<aws_sdk_dynamodb::types::TransactWriteItem, String> {
    use aws_sdk_dynamodb::types::{ConditionCheck, TransactWriteItem};
    let (pk, sk) = rule_key(team_id);
    Ok(TransactWriteItem::builder()
        .condition_check(
            ConditionCheck::builder()
                .table_name(&state.config.settings_table_name)
                .key("pk", attr_s(&pk))
                .key("sk", attr_s(&sk))
                .condition_expression("attribute_not_exists(#ro) OR #ro <> :on")
                .expression_attribute_names("#ro", "require_okta")
                .expression_attribute_values(":on", AttributeValue::Bool(true))
                .build()
                .map_err(|e| e.to_string())?,
        )
        .build())
}

/// Runs `writes` in one transaction that holds only while the team's Okta
/// rule is off: a change that narrows Okta sign-in can never land beside
/// turning the rule on. A failed condition answers `okta_required_on`.
pub async fn while_rule_off(
    state: &AppState,
    team_id: &str,
    mut writes: Vec<aws_sdk_dynamodb::types::TransactWriteItem>,
) -> Result<(), ApiError> {
    writes.push(rule_off_check(state, team_id).map_err(|e| internal("Write not built", e))?);
    match state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(writes))
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            let e = format!("{e:?}");
            if !(e.contains("TransactionCanceled") || e.contains("ConditionalCheckFailed")) {
                return Err(internal("Okta change not saved", e));
            }
            // Say what really stopped it: the rule, or the record itself
            // (it changed, or belongs to another team).
            let rule_on = sign_in::team_rule(state, team_id)
                .await
                .map(|r| r.require_okta)
                .unwrap_or(true);
            Err(if rule_on {
                setup_change_refused(&Rule {
                    require_okta: true,
                    ..Rule::default()
                })
                .unwrap_or_else(|| internal("Write refused", "rule on"))
            } else {
                (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "That changed meanwhile (for example, the domain now belongs to another team). Reload the page and try again.",
                        "code": "changed",
                    })),
                )
            })
        }
    }
}

/// Transaction parts that hold only while `live` is still the team's Okta
/// setup as it was checked: on, the same issuer, unchanged since; on hosted,
/// each of its domains still proven by this team and not failing.
pub fn setup_unchanged(
    state: &AppState,
    live: &super::oidc_signin::OktaSetup,
) -> Result<Vec<aws_sdk_dynamodb::types::TransactWriteItem>, String> {
    use aws_sdk_dynamodb::types::{ConditionCheck, TransactWriteItem};
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let mut check = ConditionCheck::builder()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&live.team_id))
        .key("sk", attr_s(super::oidc_signin::OKTA_SK))
        .expression_attribute_names("#en", "enabled")
        .expression_attribute_names("#iss", "issuer")
        .expression_attribute_names("#ua", "updated_at")
        .expression_attribute_values(":t", AttributeValue::Bool(true))
        .expression_attribute_values(":iss", attr_s(&live.issuer));
    check = match &live.updated_at {
        Some(at) => check
            .condition_expression("#en = :t AND #iss = :iss AND #ua = :ua")
            .expression_attribute_values(":ua", attr_s(at)),
        None => {
            check.condition_expression("#en = :t AND #iss = :iss AND attribute_not_exists(#ua)")
        }
    };
    let mut out = vec![TransactWriteItem::builder()
        .condition_check(check.build().map_err(e)?)
        .build()];
    if !sign_in::applies() {
        for d in &live.domains {
            out.push(
                TransactWriteItem::builder()
                    .condition_check(
                        ConditionCheck::builder()
                            .table_name(&state.config.settings_table_name)
                            .key(
                                "pk",
                                attr_s(&format!("{}{d}", super::oidc_signin::OKTA_DOMAIN_PREFIX)),
                            )
                            .key("sk", attr_s(super::oidc_signin::OKTA_DOMAIN_SK))
                            .condition_expression(
                                "team_id = :t AND attribute_not_exists(failing_at)",
                            )
                            .expression_attribute_values(":t", attr_s(&live.team_id))
                            .build()
                            .map_err(e)?,
                    )
                    .build(),
            );
        }
    }
    Ok(out)
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    error!("{context}: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "Something went wrong on our side. Try again." })),
    )
}

/// While a team requires Okta, a proven domain whose TXT record is gone is
/// kept (people still sign in) and marked failing for
/// [`super::oidc_signin::FAILING_GRACE_DAYS`] days; the owners hear about it
/// once, by email and in the audit log, with the date it will be released,
/// and the page shows it.
pub async fn domain_failing(state: &AppState, team_id: &str, domain: &str, first: bool) {
    warn!(%domain, "Okta domain proof gone from DNS while Okta sign-in is required; kept");
    if !first {
        return;
    }
    let until = (chrono::Utc::now()
        + chrono::Duration::days(super::oidc_signin::FAILING_GRACE_DAYS))
    .format("%B %-d, %Y")
    .to_string();
    let words = failing_words(&until);
    tell_owners(
        state,
        team_id,
        "okta_domain_failing",
        "An Okta email domain failed its DNS check",
        &words,
        &[("until", until.as_str())],
    )
    .await;
    audit_system(
        state,
        team_id,
        format!("Okta email domain {domain} failed its DNS check; kept until {until} while Okta sign-in is required"),
    )
    .await;
}

/// Why a domain proof was released, for the owners' email and the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    /// Its TXT record is definitely gone, and the rule was off.
    Gone,
    /// It stayed gone for the whole grace period while the rule was on.
    GraceOver,
    /// Another team proved the domain with its own record while this
    /// team's was gone.
    Contender,
}

impl Released {
    /// The email's first sentence (a domain name reads as a link in email,
    /// so none is named).
    pub fn sentence(self) -> String {
        match self {
            Released::Gone => "The DNS record that proved one of your team's Okta email domains is gone, so CoderHelm released the domain.".into(),
            Released::GraceOver => format!(
                "The DNS record that proved one of your team's Okta email domains stayed gone for {} days while Okta sign-in was required, so CoderHelm released the domain.",
                super::oidc_signin::FAILING_GRACE_DAYS
            ),
            Released::Contender => "Another team proved one of your team's Okta email domains with its own DNS record while yours was missing, so the domain is now theirs.".into(),
        }
    }

    pub fn log(self, domain: &str) -> String {
        match self {
            Released::Gone => format!("Okta email domain {domain} released: its DNS record is gone"),
            Released::GraceOver => format!(
                "Okta email domain {domain} released: its DNS record stayed gone past the grace period"
            ),
            Released::Contender => format!(
                "Okta email domain {domain} released: another team proved it while this team's record was gone"
            ),
        }
    }
}

/// A released proof: the owners get one email and the log one line, both
/// saying why.
pub async fn domain_released(state: &AppState, team_id: &str, domain: &str, why: Released) {
    let sentence = why.sentence();
    let words = released_words(&sentence);
    tell_owners(
        state,
        team_id,
        "okta_domain_released",
        "An Okta email domain was released",
        &words,
        &[("reason", sentence.as_str())],
    )
    .await;
    audit_system(state, team_id, why.log(domain)).await;
}

/// The email's words for a failing proof (the same as its template: a
/// domain name in an email reads as a link, so none is named).
pub fn failing_words(until: &str) -> String {
    format!(
        "The DNS record that proves one of your team's Okta email domains is gone. Okta sign-in is required, so CoderHelm keeps the domain until {until} and people can still sign in. Add the TXT record back, then choose Verify domains in Settings, Sign-in rules. If it is still gone then, the domain is released."
    )
}

pub fn released_words(sentence: &str) -> String {
    format!("{sentence} People at it can't sign in with Okta until you add the TXT record back and choose Verify domains in Settings, Sign-in rules.")
}

async fn tell_owners(
    state: &AppState,
    team_id: &str,
    template: &'static str,
    subject: &str,
    words: &str,
    extra: &[(&str, &str)],
) {
    let link = format!(
        "{}/settings/sign-in/",
        common::public_urls::app_url_from_env()
    );
    let members = sign_in::team_members(state, team_id)
        .await
        .unwrap_or_default();
    for owner in members
        .iter()
        .filter(|m| s(m, "role") == "owner" && s(m, "status") != "invited")
    {
        let to = s(owner, "email");
        if !to.contains('@') {
            continue;
        }
        let mut params = serde_json::Map::new();
        params.insert("link".into(), Value::from(link.clone()));
        for (k, v) in extra {
            params.insert((*k).into(), Value::from(*v));
        }
        sign_in::send_mail(
            state,
            team_id,
            template,
            to,
            params,
            subject,
            format!("{words}\n\n{link}\n"),
        )
        .await;
    }
}

async fn audit_system(state: &AppState, team_id: &str, detail: String) {
    let _ = super::roles::record(
        state,
        &system_actor(team_id),
        super::roles::AuditEntry {
            action: "sign_in_okta",
            role_id: "",
            role_name: "",
            detail,
        },
    )
    .await;
}

fn system_actor(team_id: &str) -> Claims {
    Claims {
        sub: "SYSTEM#dns-check".into(),
        team_id: team_id.to_string(),
        email: "CoderHelm".into(),
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
    }
}

/// The support view of a team that requires Okta sign-in. The policy: a
/// CoderHelm platform admin may view such a team only with the team's own
/// support grant (as for every team) AND from a session of their own that
/// passed a second factor; every read is written to the team's audit log.
/// An Okta session is never a platform admin, so a second factor is the bar
/// as strong as the team's own. Pure.
pub fn support_view_allowed(target: &Rule, admin: &SignInProof) -> bool {
    !target.require_okta || admin.mfa
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
        let f = filter(&[a.clone(), a_inv.clone(), b.clone()], &gated, None);
        assert_eq!(f.kept, vec![b.clone()]);
        assert_eq!(f.refused.as_deref(), Some("TEAM#a"));
        assert!(f.break_glass.is_none());
        // Only gated teams: nothing kept, so no personal team is made.
        let f = filter(std::slice::from_ref(&a), &gated, None);
        assert!(f.kept.is_empty() && f.refused.is_some());
        // Break-glass lands on the owner's own record, never an invite.
        let f = filter(&[a.clone(), a_inv.clone(), b.clone()], &gated, Some(None));
        assert_eq!(f.break_glass, Some(a.clone()));
        assert_eq!(f.refused.as_deref(), Some("TEAM#c"));
        let admin = item(&[("pk", "TEAM#a"), ("sk", "USER#1"), ("role", "admin")]);
        assert!(filter(&[admin], &gated, Some(None)).break_glass.is_none());
        // No gated team: everything kept.
        let f = filter(&[a.clone(), b.clone()], &HashSet::new(), None);
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
            sign_in_url("https://app.example.com/", "TEAM#a b"),
            "https://app.example.com/?signin_error=okta_required&team_id=TEAM%23a%20b"
        );
        let r = session_refused(&Refused {
            team: "Acme".into(),
            team_id: "TEAM#a".into(),
            code: CODE,
        });
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

    #[test]
    fn a_support_view_of_a_gated_team_needs_a_second_factor() {
        let on = rule(true, 1);
        assert!(!support_view_allowed(
            &on,
            &SignInProof::new(false, "google")
        ));
        assert!(support_view_allowed(&on, &SignInProof::new(true, "google")));
        assert!(support_view_allowed(
            &rule(false, 1),
            &SignInProof::default()
        ));
        let mw = code(include_str!("../middleware/auth.rs"));
        let ra = body(mw, "require_auth");
        // The admin's own session is checked like every request, then the
        // viewed team's rule, before anything is read for them.
        assert!(before(
            ra,
            "check_member_session(&state, &claims)",
            "support_access::check_session("
        ));
        assert!(before(
            ra,
            "team_rule(&state, &sv.team_id)",
            "support_access::record("
        ));
        assert!(ra.contains("support_view_allowed(&rule, &signed_in)"));
    }

    #[test]
    fn dns_never_releases_a_proof_while_the_rule_is_on_and_turning_on_needs_a_match() {
        use super::super::oidc_signin::{
            contender_takes_over, counts_at_turn_on, gone_outcome, GoneOutcome, ProofCheck,
            FAILING_GRACE_DAYS,
        };
        let now = chrono::Utc::now();
        let ago = |d: i64| (now - chrono::Duration::days(d)).to_rfc3339();
        assert_eq!(gone_outcome(false, None, now), GoneOutcome::Release);
        assert_eq!(gone_outcome(true, None, now), GoneOutcome::MarkFailing);
        assert_eq!(gone_outcome(true, Some(&ago(1)), now), GoneOutcome::Keep);
        // Never kept forever: released once the grace period is over.
        assert_eq!(
            gone_outcome(true, Some(&ago(FAILING_GRACE_DAYS)), now),
            GoneOutcome::GraceOver
        );
        assert_eq!(
            gone_outcome(true, Some("not a time"), now),
            GoneOutcome::GraceOver
        );
        assert_eq!(FAILING_GRACE_DAYS, 7);
        // Another team takes over only when the holder's record is surely gone.
        assert!(contender_takes_over(ProofCheck::Gone));
        assert!(!contender_takes_over(ProofCheck::Unknown));
        assert!(!contender_takes_over(ProofCheck::There));
        assert!(counts_at_turn_on(ProofCheck::There));
        assert!(!counts_at_turn_on(ProofCheck::Unknown));
        assert!(!counts_at_turn_on(ProofCheck::Gone));
        let oidc = code(include_str!("oidc_signin.rs"));
        let f = body(oidc, "fresh_domain_owner");
        assert!(before(f, "gone_outcome(", "release_domain_while_rule_off("));
        assert!(f.contains("domain_released("));
        assert!(f.contains("domain_failing("));
        assert!(body(oidc, "domains_proven").contains("domains_proven_now("));
        assert!(body(oidc, "domains_proven_now").contains("counts_at_turn_on("));
    }

    #[test]
    fn break_glass_takes_the_owner_record_of_the_team_asked_for() {
        let a = item(&[("pk", "TEAM#a"), ("sk", "USER#1"), ("role", "owner")]);
        let b = item(&[("pk", "TEAM#b"), ("sk", "USER#1"), ("role", "owner")]);
        let gated: HashSet<String> = ["TEAM#a".to_string(), "TEAM#b".to_string()].into();
        let items = [a.clone(), b.clone()];
        assert_eq!(
            filter(&items, &gated, Some(Some("TEAM#b"))).break_glass,
            Some(b)
        );
        assert_eq!(
            filter(&items, &gated, Some(Some("TEAM#a"))).break_glass,
            Some(a.clone())
        );
        assert_eq!(filter(&items, &gated, Some(None)).break_glass, Some(a));
        assert!(filter(&items, &gated, Some(Some("TEAM#c")))
            .break_glass
            .is_none());
        assert!(filter(&items, &gated, None).break_glass.is_none());
    }

    #[test]
    fn invites_compare_exact_addresses() {
        assert_eq!(
            exact_address(" Alice+X@Example.com "),
            "alice+x@example.com"
        );
        let now = chrono::Utc::now();
        let fresh = now.to_rfc3339();
        let inv = item(&[
            ("pk", "T"),
            ("sk", "USER#invite_1"),
            ("status", "invited"),
            ("email", "alice+contractor@corp.example"),
            ("invited_at", &fresh),
        ]);
        assert_eq!(
            invite_for(std::slice::from_ref(&inv), "alice@corp.example", now),
            InviteFor::None
        );
        assert_eq!(
            invite_for(
                std::slice::from_ref(&inv),
                "ALICE+contractor@corp.example",
                now
            ),
            InviteFor::Accept(inv)
        );
    }

    #[test]
    fn short_lived_tokens_are_checked_when_used() {
        for (src, f) in [
            (include_str!("plans.rs"), "plan_chat_stream"),
            (include_str!("brain.rs"), "brain_stream"),
        ] {
            let b = body(code(src), f);
            assert!(
                before(b, "validate_scoped_token(", "require_okta::check_scoped("),
                "{f}"
            );
        }
    }

    #[test]
    fn releases_and_client_changes_go_through_their_guards() {
        let oidc = code(include_str!("oidc_signin.rs"));
        for f in ["okta_test", "okta_discard_draft", "okta_save"] {
            assert!(!body(oidc, f).contains("release_domain(&state"), "{f}");
        }
        assert!(body(oidc, "okta_discard_draft").contains("release_domain_while_rule_off("));
        assert!(body(oidc, "okta_test").contains("release_domain_while_rule_off("));
        let v = body(oidc, "okta_verify_domains");
        assert!(before(v, "contender_takes_over(", "domain_released("));
        let save = body(oidc, "okta_save");
        assert!(before(save, "may_change_client(", "save_live("));
        // The turn-on line follows the transaction's outcome.
        let si = code(include_str!("sign_in.rs"));
        let u = body(si, "update_settings");
        assert!(before(
            u,
            ".transact_write_items()",
            "log(changes.join(\". \")).await?;\n        }\n        None"
        ));
        assert!(u.contains("Cancelled, nothing changed"));
    }

    #[test]
    fn a_failed_release_gives_no_owner_unless_the_rule_kept_it() {
        use super::super::oidc_signin::owner_after_failed_release as after;
        // Refused because the team turned the rule on and still holds it.
        assert_eq!(after(Some("TEAM#a"), "TEAM#a", true), Some("TEAM#a".into()));
        // Any other failure fails closed: nobody owns it here.
        assert_eq!(after(Some("TEAM#a"), "TEAM#a", false), None);
        assert_eq!(after(Some("TEAM#b"), "TEAM#a", true), None);
        assert_eq!(after(None, "TEAM#a", true), None);
        let oidc = code(include_str!("oidc_signin.rs"));
        let f = body(oidc, "fresh_domain_owner");
        assert!(before(
            f,
            "release_domain_while_rule_off(",
            "owner_after_failed_release("
        ));
        assert!(!f.contains(".is_err()\n    {\n        return Ok(Some(team));"));
        // A release on the rule-off path tells the owners too.
        assert!(f.contains("Released::Gone"));
    }

    #[test]
    fn a_contender_release_is_one_step_with_the_right_words() {
        let oidc = code(include_str!("oidc_signin.rs"));
        let v = body(oidc, "okta_verify_domains");
        // No "kept until" first: the holder's proof isn't rechecked through
        // the grace path before a contender release.
        assert!(!v.contains("fresh_domain_owner(&state, d, true)"));
        assert!(before(v, "raw_domain_record(", "contender_takes_over("));
        // The delete holds only for exactly the proof read (its team, when
        // it was verified and last checked).
        assert!(v.contains(r#"("verified_at", ":v"), ("checked_at", ":c")"#));
        assert!(v.contains(r#"cond.push(format!("{attr} = {v}"))"#));
        assert!(v.contains("Released::Contender"));
        assert!(Released::Contender
            .sentence()
            .contains("Another team proved"));
        assert!(Released::GraceOver
            .sentence()
            .contains("stayed gone for 7 days"));
        assert!(Released::Gone.sentence().contains("is gone"));
        assert!(Released::Contender
            .log("example.com")
            .contains("another team proved it"));
        for r in [Released::Gone, Released::GraceOver, Released::Contender] {
            assert!(!r.sentence().contains("example.com"));
        }
    }
}
