//! Require single sign-on: a hosted team, or a self-hosted install's
//! organization, whose members sign in only through its own identity
//! provider: its own Okta, or its own Microsoft Entra tenant
//! (routes::entra_sso), one at a time.
//!
//! The feature was never released before this name, so its stored names
//! were chosen fresh: `require_sso` (the provider's slug, `okta` or
//! `microsoft`; absent when off) and `require_sso_since` (unix seconds).
//!
//! # The rule
//!
//! It is one more sign-in rule beside two-factor sign-in, on the same record
//! (hosted: `pk = <team>`, `sk = SETTINGS#SIGN_IN`; self-hosted: the
//! organization record, `ORG`/`META`). Only the owner turns it on or off,
//! and every change is in the team's audit log (`sign_in_rules`). It turns
//! on only when the provider is set up and on, (hosted) every one of its
//! email domains is still proven in DNS, and the owner's own session came
//! from that provider and is linked to them, which proves the whole trip
//! works before anyone depends on it. To change providers, the owner turns
//! it off and on again.
//!
//! # While it is on
//!
//! - Every session acting in the team is checked on every request
//!   ([`check`], from `sign_in::check_session`, for dashboard sessions and
//!   connected apps alike). A session from anything but this team's provider
//!   is refused with `sso_required` and its cookie cleared: sessions from
//!   before the rule end at their next request. A connected app (MCP) works
//!   only from a grant such a session made.
//! - Every other way in (password, Google, GitHub, plain Microsoft, the
//!   other provider) refuses this team before a record or a session is made
//!   ([`issue_check`], and on hosted [`filter`], which also keeps invites to
//!   the team from being accepted that way). Switching into the team is
//!   refused. On hosted only this team is held back: the person signs in to
//!   their other teams as before, and their sessions there are untouched.
//! - There is no bypass: no owner password, no second factor, no support
//!   session gets in. If the provider is down or misconfigured nobody signs
//!   in until it's fixed; the way back is outside the product: on a
//!   self-hosted install the recovery script run with the install's own AWS
//!   credentials (scripts/sso-recovery.py), on hosted CoderHelm support
//!   turning the rule off after verifying the owner out of band
//!   ([`recover`]). Both only turn the rule off; neither makes a session.
//! - New members: an admin's pending invite may be accepted by signing in
//!   through the provider, when its verified email is the invited email and
//!   (hosted) its domain is one the team proved in DNS ([`invite_for`]).
//! - Members who hadn't linked the provider before the rule: an admin gives
//!   them a one-time claim link (routes::sso_claim). There is no matching
//!   by email for existing members, ever; a sign-in without a claim is told
//!   to ask an admin.
//! - API keys and CI tokens are not sessions and keep working.
//!
//! # Why accepting an invite through the provider is safe
//!
//! - The invite names the exact email, and only an admin makes one.
//! - The provider's email counts only when it vouches for it (Okta's
//!   `email_verified: true`, Microsoft's `xms_edov`), from the team's own
//!   Okta or tenant (issuer, audience, signature and nonce checked).
//! - Hosted: the email's domain must be proven by this team with a DNS TXT
//!   record, so a team's provider vouches only for addresses at domains its
//!   owner controls.
//! - The record is new (never an existing member's), and the session it
//!   gets is held to this team on hosted, as every such session is.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Extension, Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{error, info, warn};

use super::oauth_signin::Provider;
use super::sign_in::{self, Rule};
use crate::auth::jwt::SignInProof;
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

/// The refusal's code, for the dashboard and the sign-in screen.
pub const CODE: &str = "sso_required";
/// The rule's stored names (also read by scripts/sso-recovery.py).
pub const RULE_ATTR: &str = "require_sso";
pub const SINCE_ATTR: &str = "require_sso_since";
/// Set by the self-hosted recovery script: the owners are emailed once.
pub const RECOVERY_NOTICE: &str = "sso_recovery_notice";
/// When an email for that notice was last tried (unix seconds).
pub const RECOVERY_TRIED: &str = "sso_recovery_notice_tried";
/// How long to wait before trying the owners' email again.
pub const RECOVERY_RETRY_SECS: u64 = 600;
/// The cut-off: when the rule was last turned on and with which provider.
/// Never cleared, so turning the rule off revives nothing from before it.
pub const CUTOFF_ATTR: &str = "sso_cutoff_at";
pub const CUTOFF_PROVIDER_ATTR: &str = "sso_cutoff_provider";

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

// ── The provider ────────────────────────────────────────────────────

/// The identity provider a team requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SsoProvider {
    /// The team's own Okta (routes::oidc_signin).
    Okta,
    /// The team's own Microsoft Entra tenant (routes::entra_sso).
    Microsoft,
}

impl SsoProvider {
    pub const ALL: [SsoProvider; 2] = [SsoProvider::Okta, SsoProvider::Microsoft];

    pub fn slug(self) -> &'static str {
        match self {
            SsoProvider::Okta => "okta",
            SsoProvider::Microsoft => "microsoft",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SsoProvider::Okta => "Okta",
            SsoProvider::Microsoft => "Microsoft Entra",
        }
    }

    pub fn parse(v: &str) -> Option<SsoProvider> {
        match v.trim() {
            "okta" => Some(SsoProvider::Okta),
            "microsoft" => Some(SsoProvider::Microsoft),
            _ => None,
        }
    }

    /// The stored rule: off when absent or empty. A value this build
    /// doesn't know keeps the rule on and is met by nothing but Okta (it
    /// fails closed; only this code writes it).
    pub fn stored(v: &str) -> Option<SsoProvider> {
        match v.trim() {
            "" => None,
            other => Some(SsoProvider::parse(other).unwrap_or(SsoProvider::Okta)),
        }
    }

    /// Whether a sign-in (its proof) came from this provider, the team's
    /// own: an Okta sign-in (only a team's own Okta signs anyone in), or a
    /// trip through the team's tenant (`entra` set; a plain Microsoft
    /// sign-in has none). That the identity is still the member's linked
    /// one under the live setup is `sign_in::check_member_session`'s check.
    pub fn signed_in(self, proof: &SignInProof) -> bool {
        match self {
            SsoProvider::Okta => proof.via.as_deref() == Some("okta"),
            SsoProvider::Microsoft => {
                proof.via.as_deref() == Some("microsoft")
                    && proof.entra.as_deref().is_some_and(|e| !e.is_empty())
            }
        }
    }
}

/// A member's link to a provider identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    Okta { issuer: String, sub: String },
    Entra { tid: String, oid: String },
}

impl Link {
    /// The identity's one-member-per-team row.
    pub fn row(&self, team: &str) -> (String, String) {
        match self {
            Link::Okta { issuer, sub } => super::oidc_signin::link_row(team, issuer, sub),
            Link::Entra { tid, oid } => super::entra_sso::link_row(team, tid, oid),
        }
    }

    /// The attributes it puts on the member record.
    pub fn member_attrs(&self, at: &str) -> Vec<(&'static str, String)> {
        match self {
            Link::Okta { issuer, sub } => vec![
                ("okta_issuer", issuer.clone()),
                ("okta_sub", sub.clone()),
                ("okta_linked_at", at.to_string()),
            ],
            Link::Entra { tid, oid } => vec![
                ("ms_tid", tid.to_lowercase()),
                ("ms_oid", oid.clone()),
                ("ms_linked_at", at.to_string()),
            ],
        }
    }

    /// The `SET` clause and values that link a member.
    pub fn member_set(&self, at: &str) -> (String, HashMap<String, AttributeValue>) {
        let attrs = self.member_attrs(at);
        let mut values = HashMap::new();
        let mut parts = Vec::new();
        for (i, (k, v)) in attrs.into_iter().enumerate() {
            parts.push(format!("{k} = :l{i}"));
            values.insert(format!(":l{i}"), attr_s(&v));
        }
        (parts.join(", "), values)
    }

    /// The condition that the member has no link to this provider.
    pub fn unlinked_condition(&self) -> &'static str {
        match self {
            Link::Okta { .. } => "attribute_not_exists(okta_sub)",
            Link::Entra { .. } => "attribute_not_exists(ms_oid)",
        }
    }

    /// Whether a member has any link to `provider`.
    pub fn is_linked(member: &Item, provider: SsoProvider) -> bool {
        match provider {
            SsoProvider::Okta => !s(member, "okta_sub").is_empty(),
            SsoProvider::Microsoft => !s(member, "ms_oid").is_empty(),
        }
    }
}

// ── Sessions ────────────────────────────────────────────────────────

/// Whether the rule lets a session or a sign-in (its proof) act in a team.
/// Pure. With no rule, everything as before; with it, only the provider it
/// names.
pub fn allowed(rule: &Rule, proof: &SignInProof) -> bool {
    match rule.require_sso {
        None => true,
        Some(p) => p.signed_in(proof),
    }
}

/// Whether a session, scoped token or app connection issued at `iat` with
/// `proof` still stands: the rule allows it now, and if it was issued
/// before (or at) the last cut-off, it came from the provider required then.
/// Pure.
pub fn session_stands(rule: &Rule, iat: u64, proof: &SignInProof) -> bool {
    if !allowed(rule, proof) {
        return false;
    }
    match rule.sso_cutoff_provider {
        // Inclusive: a session from the very second of the cut-off is from
        // before it.
        Some(p) if rule.sso_cutoff > 0 && iat <= rule.sso_cutoff => p.signed_in(proof),
        _ => true,
    }
}

/// The words a refused person reads.
pub fn refusal_message(self_hosted: bool, team: &str, provider: SsoProvider) -> String {
    let p = provider.name();
    match (self_hosted, team.trim()) {
        (true, _) => {
            format!(
                "Your organization requires single sign-on with {p}. Sign in with {p} to continue."
            )
        }
        (false, "") => {
            format!("This team requires single sign-on with {p}. Sign in with {p} to continue.")
        }
        (false, t) => {
            format!("{t} requires single sign-on with {p}. Sign in with {p} to continue.")
        }
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

/// Where a refused person goes to sign in with the team's provider. The URL
/// carries the team's id and the provider's slug only (so the sign-in page
/// can start the right trip), never the team's name: the sign-in page shows
/// no text from a URL.
pub fn sign_in_url(app_url: &str, team_id: &str, provider: SsoProvider) -> String {
    let base = format!(
        "{}/?signin_error={CODE}&sso={}",
        app_url.trim_end_matches('/'),
        provider.slug()
    );
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
/// switch into the team, a connection made from another session).
pub fn refused_json(r: &Refused) -> ApiError {
    (StatusCode::FORBIDDEN, Json(r.body()))
}

/// The rule's check of one request (`sign_in::check_session` has read the
/// rule).
pub async fn check(
    state: &AppState,
    rule: &Rule,
    claims: &Claims,
    proof: &SignInProof,
) -> Result<(), Response> {
    if session_stands(rule, claims.iat, proof) {
        return Ok(());
    }
    if rule.require_sso.is_none() {
        // The rule is off now, but this session is from before its cut-off
        // and from another way in: it stays ended.
        return Err(sign_in::session_ended(sign_in::Ended::Revoked));
    }
    Err(session_refused(
        &refused(state, &claims.team_id, rule.require_sso).await,
    ))
}

/// Keys follow their maker: while the team requires single sign-on, an API
/// key or a knowledge key works only while the member who made it is still
/// on the team and linked to the required provider. A key of an unlinked
/// maker is suspended (nothing is deleted) and works again once they link.
/// Pure: the maker's record (none: not on the team) and the live setup.
pub fn key_maker_stands(rule: &Rule, maker: Option<&Item>, live: Option<&Live>) -> bool {
    if rule.require_sso.is_none() {
        return true;
    }
    match maker.filter(|m| s(m, "status") != "invited") {
        Some(m) => live.is_some_and(|l| l.enabled() && l.linked(m)),
        None => false,
    }
}

/// [`key_maker_stands`] read now for `maker` on `team_id`.
pub async fn key_maker_allowed(
    state: &AppState,
    team_id: &str,
    maker: &str,
) -> Result<bool, String> {
    let rule = sign_in::team_rule(state, team_id).await?;
    let Some(p) = rule.require_sso else {
        return Ok(true);
    };
    let member = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(maker))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?
        .item;
    let live = live_setup(state, team_id, p).await?;
    Ok(key_maker_stands(&rule, member.as_ref(), live.as_ref()))
}

/// The refusal for a suspended key (its maker isn't linked).
pub const KEY_SUSPENDED: &str = "key_suspended";
pub const KEY_SUSPENDED_WORDS: &str = "This key is suspended: the team requires single sign-on and the member who made it isn't linked to it. It works again once they link their account in Settings, Security.";

/// Ends every app connection (MCP grant) of the team that the rule as it is
/// now refuses: turning the rule on revokes them, refresh tokens included.
pub async fn end_refused_connections(state: &AppState, team_id: &str, rule: &Rule) -> usize {
    use crate::mcp_oauth::OAuthStore;
    let store = crate::mcp_oauth::store(state);
    let grants = match store.list_grants(team_id).await {
        Ok(g) => g,
        Err(e) => {
            error!(error = %e, "App connections not read; none ended");
            return 0;
        }
    };
    let at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut n = 0;
    for g in grants.iter().filter(|g| g.revoked_at.is_none()) {
        if !session_stands(rule, g.created_at_secs, &g.proof())
            && store
                .revoke_grant(&g.team_id, &g.id, "single sign-on required", &at)
                .await
                .is_ok()
        {
            n += 1;
        }
    }
    info!(
        team_id,
        ended = n,
        "App connections the single sign-on rule refuses ended"
    );
    n
}

/// [`check`] with the rule read here, for the places that take a session
/// outside the middleware (linking, connecting GitHub, the support view,
/// the answer links in emails).
pub async fn check_claims(
    state: &AppState,
    claims: &Claims,
    proof: &SignInProof,
) -> Result<(), Response> {
    let rule = sign_in::team_rule(state, &claims.team_id)
        .await
        .map_err(|e| {
            error!(error = %e, "Sign-in rules not read; refused");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        })?;
    check(state, &rule, claims, proof).await
}

/// A short-lived stream token (plan chat, brain) at its use: the session it
/// was minted from must still stand (the member check, and the team's
/// rule), so ending a session ends its tokens at once.
pub async fn check_scoped(
    state: &AppState,
    claims: &Claims,
    minted_by: &SignInProof,
) -> Result<(), Response> {
    let claims = super::oidc_signin::mark_session(claims.clone(), minted_by);
    sign_in::check_member_session(state, &claims).await?;
    check_claims(state, &claims, minted_by).await
}

// ── Other ways in ───────────────────────────────────────────────────

/// A sign-in the rule refuses: the team's words, the provider and the code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// The team's name, for the server's own message ("" on self-hosted).
    pub team: String,
    pub team_id: String,
    pub provider: SsoProvider,
    pub code: &'static str,
}

impl Refused {
    fn body(&self) -> Value {
        json!({
            "error": self.message(),
            "code": self.code,
            "provider": self.provider.slug(),
            "team": self.team,
            "team_id": self.team_id,
            "sign_in_url": sign_in_url(app_url(), &self.team_id, self.provider),
        })
    }

    pub fn message(&self) -> String {
        refusal_message(sign_in::applies(), &self.team, self.provider)
    }

    /// For a sign-in that answers with a redirect (Google, GitHub,
    /// Microsoft, Okta): the sign-in screen, with the team's provider.
    pub fn redirect(&self, provider: Provider, app_url: &str) -> Response {
        warn!(
            provider = provider.slug(),
            required = self.provider.slug(),
            "Sign-in refused: the team requires single sign-on"
        );
        let url = format!(
            "{}&provider={}",
            sign_in_url(app_url, &self.team_id, self.provider),
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
pub async fn refused(state: &AppState, team_id: &str, provider: Option<SsoProvider>) -> Refused {
    Refused {
        team: team_words(state, team_id).await,
        team_id: team_id.to_string(),
        provider: provider.unwrap_or(SsoProvider::Okta),
        code: CODE,
    }
}

/// How a sign-in by `via` (with no team provider identity) came in, as the
/// rule reads it.
pub fn way_in(via: &str) -> SignInProof {
    SignInProof::new(false, via)
}

/// The last check before a sign-in to `team_id` gets its second step or a
/// session, on every way in: `proof` is how it signed in. `Ok(Err)` is the
/// refusal to send.
pub async fn issue_check(
    state: &AppState,
    team_id: &str,
    proof: &SignInProof,
) -> Result<Result<(), Refused>, String> {
    let rule = sign_in::team_rule(state, team_id).await?;
    if allowed(&rule, proof) {
        return Ok(Ok(()));
    }
    Ok(Err(refused(state, team_id, rule.require_sso).await))
}

/// Self-hosted, before the join rules run (which may accept an invite or
/// join a domain): while the organization requires single sign-on, only a
/// sign-in from its provider goes on. `Ok(Err)` is the refusal to send.
pub async fn selfhost_pre_gate(
    state: &AppState,
    proof: &SignInProof,
) -> Result<Result<(), Refused>, String> {
    if !sign_in::applies() {
        return Ok(Ok(()));
    }
    let rule = sign_in::team_rule(state, "").await?;
    if allowed(&rule, proof) {
        return Ok(Ok(()));
    }
    Ok(Err(Refused {
        team: String::new(),
        team_id: String::new(),
        provider: rule.require_sso.unwrap_or(SsoProvider::Okta),
        code: CODE,
    }))
}

/// The teams among a person's records (by `pk`) that a sign-in that came in
/// as `proof` may not land on: those requiring single sign-on with another
/// way in.
pub async fn gated_teams(
    state: &AppState,
    items: &[Item],
    proof: &SignInProof,
) -> Result<HashSet<String>, String> {
    let mut seen = HashSet::new();
    let mut gated = HashSet::new();
    for i in items {
        let team = s(i, "pk");
        if team.is_empty() || !seen.insert(team.to_string()) {
            continue;
        }
        if !allowed(&sign_in::team_rule(state, team).await?, proof) {
            gated.insert(team.to_string());
        }
    }
    Ok(gated)
}

/// What a sign-in other than the team's provider may land on (hosted).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filtered {
    /// The records and invites of teams that don't hold this sign-in back.
    pub kept: Vec<Item>,
    /// A team that was held back, for the refusal's words.
    pub refused: Option<String>,
}

/// Holds back every record and invite of a team in `gated`. Pure.
pub fn filter(items: &[Item], gated: &HashSet<String>) -> Filtered {
    let mut out = Filtered::default();
    for i in items {
        let team = s(i, "pk");
        if !gated.contains(team) {
            out.kept.push(i.clone());
        } else if out.refused.is_none() {
            out.refused = Some(team.to_string());
        }
    }
    out
}

/// The words for a hosted sign-in that found only teams it may not reach.
pub async fn refused_for(state: &AppState, team: &str) -> Refused {
    let provider = sign_in::team_rule(state, team)
        .await
        .ok()
        .and_then(|r| r.require_sso);
    refused(state, team, provider).await
}

// ── New members through the provider ────────────────────────────────

/// What a sign-in of an identity no member linked finds on the team.
#[derive(Debug, Clone, PartialEq)]
pub enum InviteFor {
    /// A live invite for exactly this email: accepted through the provider.
    Accept(Item),
    /// A member already has this email: the provider never takes over a
    /// record (an admin's claim link is the way).
    ExistingMember,
    None,
}

/// An address as an invite and the provider's verified email are compared:
/// trimmed and lowercase, nothing else. `+tag` addresses are other
/// mailboxes here (`normalize_email` folds them, which is right for finding
/// a person's records but never for handing one of them an invite).
pub fn exact_address(email: &str) -> String {
    email.trim().to_lowercase()
}

/// The pending invite a provider sign-in may accept: the team's live invite
/// for exactly `email` (compared with [`exact_address`]), when no member
/// record on the team has that email or one that folds to the same person.
/// Pure.
pub fn invite_for(members: &[Item], email: &str, now: chrono::DateTime<chrono::Utc>) -> InviteFor {
    let want = exact_address(email);
    if want.is_empty() || !want.contains('@') {
        return InviteFor::None;
    }
    let folded = super::auth::normalize_email(email);
    let same_person = |i: &&Item| super::auth::normalize_email(s(i, "email")) == folded;
    let users = || members.iter().filter(|i| s(i, "sk").starts_with("USER#"));
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

/// The member record an invite becomes when accepted through the provider,
/// linked to the identity that accepted it.
pub fn invited_member(invite: &Item, user_id: &str, name: &str, link: &Link, now: &str) -> Item {
    let team = s(invite, "pk");
    let email = s(invite, "email");
    let role = match s(invite, "role") {
        "" | "owner" => "member",
        r => r,
    };
    let mut item = HashMap::from([
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
    ]);
    for (k, v) in link.member_attrs(now) {
        item.insert(k.to_string(), attr_s(&v));
    }
    item
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

/// Accepts `invite` through the provider in one transaction: the new member
/// record (only if no record has its id), the identity's link row (only if
/// no member holds it), and the invite removed (only while it is still
/// one), and on hosted the email's domain still this team's live proof.
/// Any of them failing leaves everything as it was.
pub async fn accept_invite(
    state: &AppState,
    member: &Item,
    invite: &Item,
    link: &Link,
    domain: Option<&str>,
) -> Result<(), String> {
    use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem};
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let team = s(member, "pk");
    let (row_pk, row_sk) = link.row(team);
    let (cond, names, values) = invite_unchanged(invite);
    let mut items = vec![
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
                    .set_condition_expression(Some(cond))
                    .set_expression_attribute_names(Some(names))
                    .set_expression_attribute_values(Some(values))
                    .build()
                    .map_err(e)?,
            )
            .build(),
    ];
    // Hosted: the email's domain is still this team's proof, not failing,
    // held in the same transaction.
    if let Some(d) = domain {
        items.push(super::oidc_signin::domain_still_usable(state, team, d)?);
    }
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

/// The live setup of a provider, as the rule needs it.
#[derive(Debug, Clone)]
pub enum Live {
    Okta(super::oidc_signin::OktaSetup),
    Microsoft(super::entra_sso::EntraSetup),
}

impl Live {
    pub fn enabled(&self) -> bool {
        match self {
            Live::Okta(s) => s.enabled,
            Live::Microsoft(s) => s.enabled,
        }
    }

    pub fn domains(&self) -> &[String] {
        match self {
            Live::Okta(s) => &s.domains,
            Live::Microsoft(s) => &s.domains,
        }
    }

    pub fn team(&self) -> &str {
        match self {
            Live::Okta(s) => &s.team_id,
            Live::Microsoft(s) => &s.team_id,
        }
    }

    /// Whether a member's link is to this live setup.
    pub fn linked(&self, member: &Item) -> bool {
        match self {
            Live::Okta(s) => okta_linked(member, Some(&s.issuer)),
            Live::Microsoft(s) => super::entra_sso::linked_to(member, Some(s)),
        }
    }
}

/// The team's live setup of `provider`, if any.
pub async fn live_setup(
    state: &AppState,
    team: &str,
    provider: SsoProvider,
) -> Result<Option<Live>, String> {
    Ok(match provider {
        SsoProvider::Okta => super::oidc_signin::live_setup_of(state, team)
            .await?
            .map(Live::Okta),
        SsoProvider::Microsoft => super::entra_sso::load(state, team)
            .await?
            .map(Live::Microsoft),
    })
}

/// What turning the rule on with one provider needs, each part shown on the
/// page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readiness {
    pub provider: SsoProvider,
    /// A live setup exists.
    pub set_up: bool,
    /// It is turned on.
    pub enabled: bool,
    /// Hosted: every one of its domains is proven by this team (always true
    /// on a self-hosted install, which proves none).
    pub domains_verified: bool,
    /// The caller's own session came from this provider (and the member
    /// check found it linked to them).
    pub session_from_provider: bool,
    pub is_owner: bool,
}

impl Readiness {
    pub fn ready(&self) -> bool {
        self.first_missing().is_none()
    }

    /// The first unmet condition: status, code and words.
    pub fn first_missing(&self) -> Option<(StatusCode, &'static str, String)> {
        let p = self.provider.name();
        if !self.is_owner {
            return Some((
                StatusCode::FORBIDDEN,
                "owner_only",
                "Only the owner can turn Require single sign-on on or off.".into(),
            ));
        }
        if !self.set_up || !self.enabled {
            return Some((
                StatusCode::CONFLICT,
                "sso_not_on",
                format!("Set up {p} sign-in for the team and turn it on first."),
            ));
        }
        if !self.domains_verified {
            return Some((
                StatusCode::CONFLICT,
                "domains_not_verified",
                "Verify each email domain first: its TXT record must still be in DNS.".into(),
            ));
        }
        if !self.session_from_provider {
            return Some((
                StatusCode::CONFLICT,
                "sign_in_with_provider_first",
                format!("Sign in with your team's {p} first, then turn this on. That proves it works for you before everyone depends on it."),
            ));
        }
        None
    }

    pub fn view(&self) -> Value {
        json!({
            "provider": self.provider.slug(),
            "name": self.provider.name(),
            "set_up": self.set_up,
            "enabled": self.enabled,
            "domains_verified": self.domains_verified,
            "session_from_provider": self.session_from_provider,
            "is_owner": self.is_owner,
            "ready": self.ready(),
        })
    }
}

/// What turning the rule on with `provider` needs right now for this
/// caller. `recheck_dns` reads each domain's TXT record again (turning
/// on); the page shows the stored proofs.
pub async fn readiness(
    state: &AppState,
    claims: &Claims,
    proof: &SignInProof,
    provider: SsoProvider,
    live: Option<&Live>,
    recheck_dns: bool,
) -> Result<Readiness, String> {
    let domains_verified = match live {
        None => false,
        Some(_) if sign_in::applies() => true,
        Some(l) => {
            super::oidc_signin::domains_proven_list(state, l.team(), l.domains(), recheck_dns)
                .await?
        }
    };
    let from_provider = provider.signed_in(proof)
        && claims.support_view.is_none()
        && match (live, provider) {
            (Some(Live::Okta(_)), SsoProvider::Okta) => claims.via_okta,
            (Some(Live::Microsoft(m)), SsoProvider::Microsoft) => claims
                .entra
                .as_deref()
                .and_then(super::entra_sso::split_entra_id)
                .is_some_and(|(tid, _)| tid == m.tid),
            _ => false,
        };
    Ok(Readiness {
        provider,
        set_up: live.is_some(),
        enabled: live.is_some_and(Live::enabled),
        domains_verified,
        session_from_provider: from_provider,
        is_owner: claims.role == "owner" && claims.support_view.is_none(),
    })
}

/// Whether a member's Okta link is to the live setup's issuer.
pub fn okta_linked(member: &Item, issuer: Option<&str>) -> bool {
    issuer.is_some_and(|iss| !s(member, "okta_sub").is_empty() && s(member, "okta_issuer") == iss)
}

/// The active members not linked to `live`, for the page and the warning
/// when the rule is turned on.
pub fn unlinked(members: &[Item], live: Option<&Live>) -> Vec<Value> {
    members
        .iter()
        .filter(|i| s(i, "sk").starts_with("USER#") && s(i, "status") != "invited")
        .filter(|i| !live.is_some_and(|l| l.linked(i)))
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

/// While the rule names `provider`, its setup can't be turned off, removed,
/// moved or lose a domain: the owner turns the rule off first, so nobody is
/// locked out by a settings change.
pub fn setup_change_refused(rule: &Rule, provider: SsoProvider) -> Option<ApiError> {
    (rule.require_sso == Some(provider)).then(|| {
        (
            StatusCode::CONFLICT,
            Json(json!({
                "error": format!("Turn off Require single sign-on first (Settings, Sign-in rules). Changing {} while it is required would lock everyone out.", provider.name()),
                "code": "sso_required_on",
            })),
        )
    })
}

// ── Writes that would narrow sign-in ────────────────────────────────

/// The record that holds a team's sign-in rules: the organization's on a
/// self-hosted install, else the team's own.
pub fn rule_key(team_id: &str) -> (String, String) {
    if sign_in::applies() {
        (sign_in::ORG_PK.to_string(), sign_in::ORG_SK.to_string())
    } else {
        (team_id.to_string(), sign_in::TEAM_RULE_SK.to_string())
    }
}

/// The team whose sign-in rules and provider setups the caller manages: on
/// hosted the team they act in; self-hosted the organization (only from
/// its own team). `None`: not the organization.
pub async fn rule_team(state: &AppState, claims: &Claims) -> Result<Option<String>, String> {
    if sign_in::applies() {
        return Ok(sign_in::load_org(state)
            .await?
            .filter(|o| o.team_id == claims.team_id)
            .map(|o| o.team_id));
    }
    Ok(Some(claims.team_id.clone()))
}

/// A transaction part that holds only while the team's rule doesn't need
/// `provider` (any provider when `None`: the rule is off).
pub fn rule_off_check(
    state: &AppState,
    team_id: &str,
    provider: Option<SsoProvider>,
) -> Result<aws_sdk_dynamodb::types::TransactWriteItem, String> {
    use aws_sdk_dynamodb::types::{ConditionCheck, TransactWriteItem};
    let (pk, sk) = rule_key(team_id);
    let mut c = ConditionCheck::builder()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&pk))
        .key("sk", attr_s(&sk))
        .expression_attribute_names("#rs", RULE_ATTR);
    c = match provider {
        None => c
            .condition_expression("attribute_not_exists(#rs) OR #rs = :off")
            .expression_attribute_values(":off", attr_s("")),
        Some(p) => c
            .condition_expression("attribute_not_exists(#rs) OR #rs <> :p")
            .expression_attribute_values(":p", attr_s(p.slug())),
    };
    Ok(TransactWriteItem::builder()
        .condition_check(c.build().map_err(|e| e.to_string())?)
        .build())
}

/// Runs `writes` in one transaction that holds only while the team's rule
/// doesn't need `provider` (`None`: while it is off): a change that narrows
/// sign-in can never land beside turning the rule on. A failed condition
/// answers `sso_required_on`.
pub async fn while_rule_off(
    state: &AppState,
    team_id: &str,
    provider: Option<SsoProvider>,
    mut writes: Vec<aws_sdk_dynamodb::types::TransactWriteItem>,
) -> Result<(), ApiError> {
    writes.push(
        rule_off_check(state, team_id, provider).map_err(|e| internal("Write not built", e))?,
    );
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
                return Err(internal("Change not saved", e));
            }
            // Say what really stopped it: the rule, or the record itself
            // (it changed, or belongs to another team).
            let rule = sign_in::team_rule(state, team_id).await.ok();
            let blocked = match (&rule, provider) {
                (None, _) => true,
                (Some(r), None) => r.require_sso.is_some(),
                (Some(r), Some(p)) => r.require_sso == Some(p),
            };
            Err(if blocked {
                let p = provider
                    .or(rule.and_then(|r| r.require_sso))
                    .unwrap_or(SsoProvider::Okta);
                setup_change_refused(
                    &Rule {
                        require_sso: Some(p),
                        ..Rule::default()
                    },
                    p,
                )
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

/// Transaction parts that hold only while `live` is still the team's setup
/// as it was checked: on, the same issuer or tenant, unchanged since; on
/// hosted, each of its domains still proven by this team and not failing.
pub fn setup_unchanged(
    state: &AppState,
    live: &Live,
) -> Result<Vec<aws_sdk_dynamodb::types::TransactWriteItem>, String> {
    use aws_sdk_dynamodb::types::{ConditionCheck, TransactWriteItem};
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let (sk, id_attr, id, updated_at) = match live {
        Live::Okta(l) => (
            super::oidc_signin::OKTA_SK,
            "issuer",
            l.issuer.clone(),
            l.updated_at.clone(),
        ),
        Live::Microsoft(l) => (
            super::entra_sso::SETUP_SK,
            "tid",
            l.tid.clone(),
            l.updated_at.clone(),
        ),
    };
    let mut check = ConditionCheck::builder()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(live.team()))
        .key("sk", attr_s(sk))
        .expression_attribute_names("#en", "enabled")
        .expression_attribute_names("#id", id_attr)
        .expression_attribute_names("#ua", "updated_at")
        .expression_attribute_values(":t", AttributeValue::Bool(true))
        .expression_attribute_values(":id", attr_s(&id));
    check = match &updated_at {
        Some(at) => check
            .condition_expression("#en = :t AND #id = :id AND #ua = :ua")
            .expression_attribute_values(":ua", attr_s(at)),
        None => check.condition_expression("#en = :t AND #id = :id AND attribute_not_exists(#ua)"),
    };
    let mut out = vec![TransactWriteItem::builder()
        .condition_check(check.build().map_err(e)?)
        .build()];
    if !sign_in::applies() {
        for d in live.domains() {
            out.push(super::oidc_signin::domain_still_usable(
                state,
                live.team(),
                d,
            )?);
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

// ── Domain proofs while the rule is on ──────────────────────────────

/// While a team requires single sign-on, a proven domain whose TXT record
/// is gone is kept (people still sign in) and marked failing for
/// [`super::oidc_signin::FAILING_GRACE_DAYS`] days; the owners hear about
/// it once, by email and in the audit log, with the date it will be
/// released, and the page shows it.
pub async fn domain_failing(state: &AppState, team_id: &str, domain: &str, first: bool) {
    warn!(%domain, "Domain proof gone from DNS while single sign-on is required; kept");
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
        format!("Email domain {domain} failed its DNS check; kept until {until} while single sign-on is required"),
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
                "The DNS record that proved one of your team's Okta email domains stayed gone for {} days while single sign-on was required, so CoderHelm released the domain.",
                super::oidc_signin::FAILING_GRACE_DAYS
            ),
            Released::Contender => "Another team proved one of your team's Okta email domains with its own DNS record while yours was missing, so the domain is now theirs.".into(),
        }
    }

    pub fn log(self, domain: &str) -> String {
        match self {
            Released::Gone => format!("Email domain {domain} released: its DNS record is gone"),
            Released::GraceOver => format!(
                "Email domain {domain} released: its DNS record stayed gone past the grace period"
            ),
            Released::Contender => format!(
                "Email domain {domain} released: another team proved it while this team's record was gone"
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
        "The DNS record that proves one of your team's Okta email domains is gone. Single sign-on is required, so CoderHelm keeps the domain until {until} and people can still sign in. Add the TXT record back, then choose Verify domains in Settings, Sign-in rules. If it is still gone then, the domain is released."
    )
}

pub fn released_words(sentence: &str) -> String {
    format!("{sentence} People at it can't sign in with your identity provider until you add the TXT record back and choose Verify domains in Settings, Sign-in rules.")
}

/// Emails every owner of the team (essential mail: always sent).
pub async fn tell_owners(
    state: &AppState,
    team_id: &str,
    template: &'static str,
    subject: &str,
    words: &str,
    extra: &[(&str, &str)],
) -> usize {
    let link = format!(
        "{}/settings/sign-in/",
        common::public_urls::app_url_from_env()
    );
    let members = sign_in::team_members(state, team_id)
        .await
        .unwrap_or_default();
    let mut sent = 0;
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
        if sign_in::send_mail(
            state,
            team_id,
            template,
            to,
            params,
            subject,
            format!("{words}\n\n{link}\n"),
        )
        .await
        {
            sent += 1;
        }
    }
    sent
}

/// A line in the team's audit log by the caller (written to `team`, which
/// on self-hosted is the organization).
pub async fn audit(state: &AppState, claims: &Claims, team: &str, detail: String) {
    let mut actor = claims.clone();
    actor.team_id = team.to_string();
    let _ = super::roles::record(
        state,
        &actor,
        super::roles::AuditEntry {
            action: "sign_in_rules",
            role_id: "",
            role_name: "",
            detail,
        },
    )
    .await;
}

pub async fn audit_system(state: &AppState, team_id: &str, detail: String) {
    let _ = super::roles::record(
        state,
        &system_actor(team_id, "SYSTEM#dns-check", "CoderHelm"),
        super::roles::AuditEntry {
            action: "sign_in_rules",
            role_id: "",
            role_name: "",
            detail,
        },
    )
    .await;
}

fn system_actor(team_id: &str, sub: &str, name: &str) -> Claims {
    Claims {
        sub: sub.into(),
        team_id: team_id.to_string(),
        email: name.into(),
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
        entra: None,
    }
}

/// The support view of a team that requires single sign-on. The policy: a
/// CoderHelm platform admin may view such a team only with the team's own
/// support grant (as for every team) AND from a session of their own that
/// passed a second factor; every read is written to the team's audit log.
/// It is read-only and never a session in the team. Pure.
pub fn support_view_allowed(target: &Rule, admin: &SignInProof) -> bool {
    target.require_sso.is_none() || admin.mfa
}

/// The time the rule's sessions are measured from.
pub fn since_now() -> u64 {
    now_secs()
}

// ── Recovery (no bypass inside the product) ─────────────────────────

/// The words next to the switch and in the confirm before turning it on.
pub fn recovery_words(self_hosted: bool, provider: SsoProvider) -> String {
    let p = provider.name();
    if self_hosted {
        format!("If {p} is down or misconfigured, nobody can sign in until it's fixed. There is no way around it inside Coderhelm: to turn the rule off, someone with this install's AWS credentials runs scripts/sso-recovery.py from the Coderhelm platform repository, which writes the audit log and emails every owner.")
    } else {
        format!("If {p} is down or misconfigured, nobody can sign in until it's fixed. There is no way around it inside Coderhelm: an owner contacts Coderhelm support from outside the product, support verifies them out of band, and turns only this rule off. That is written to your audit log and every owner is emailed. Support never gets a session in your team.")
    }
}

#[derive(Deserialize)]
pub struct RecoverBody {
    /// Who at the team was verified, and how (a call, a ticket, a signed
    /// request from the company's domain): kept in the team's audit log.
    #[serde(default)]
    pub verification: String,
}

/// The shortest verification note a recovery takes.
pub const MIN_VERIFICATION: usize = 20;

/// POST /api/admin/teams/:team_id/sso-recovery (hosted, platform admin):
/// turns the team's "Require single sign-on" off after support verified an
/// owner out of band. It needs the admin's own session to have passed a
/// second factor and a verification note, writes the team's audit log and
/// emails every owner. It makes no session and changes nothing else: the
/// admin never gets into the team from it.
pub async fn recover(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<SignInProof>>,
    Path(team_id): Path<String>,
    Json(body): Json<RecoverBody>,
) -> Result<Json<Value>, ApiError> {
    if !super::api::is_platform_admin(&claims) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Platform admins only." })),
        ));
    }
    if sign_in::applies() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "A self-hosted install recovers with scripts/sso-recovery.py.", "code": "self_hosted" }),
            ),
        ));
    }
    if !proof.as_ref().is_some_and(|p| p.mfa) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(
                json!({ "error": "Sign in with your second factor first.", "code": "second_factor" }),
            ),
        ));
    }
    let note = body.verification.trim();
    if note.chars().count() < MIN_VERIFICATION {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Write who you verified at the team and how (at least 20 characters). It goes in the team's audit log.",
                "code": "verification_needed",
            })),
        ));
    }
    let team = team_id.trim().to_string();
    if !team.starts_with("TEAM#") {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Not a team id." })),
        ));
    }
    let rule = sign_in::team_rule(&state, &team)
        .await
        .map_err(|e| internal("Sign-in rules not read", e))?;
    let Some(provider) = rule.require_sso else {
        return Err((
            StatusCode::CONFLICT,
            Json(
                json!({ "error": "That team doesn't require single sign-on.", "code": "sso_not_required" }),
            ),
        ));
    };
    let note: String = note.chars().take(600).collect();
    let detail = format!(
        "Require single sign-on ({}) turned off by CoderHelm support ({}) after verifying the owner out of band: {note}",
        provider.name(),
        claims.display_name()
    );
    // The rule goes off and its audit line is written in one transaction:
    // never one without the other.
    use aws_sdk_dynamodb::types::{Put, TransactWriteItem, Update};
    let b = |e: aws_sdk_dynamodb::error::BuildError| internal("Recovery not built", e);
    let items = vec![
        TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name(&state.config.settings_table_name)
                    .key("pk", attr_s(&team))
                    .key("sk", attr_s(sign_in::TEAM_RULE_SK))
                    .update_expression("REMOVE #rs, #since SET updated_at = :now, updated_by = :by")
                    .condition_expression("#rs = :p")
                    .expression_attribute_names("#rs", RULE_ATTR)
                    .expression_attribute_names("#since", SINCE_ATTR)
                    .expression_attribute_values(":p", attr_s(provider.slug()))
                    .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .expression_attribute_values(":by", attr_s("CoderHelm support"))
                    .build()
                    .map_err(b)?,
            )
            .build(),
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(&state.config.settings_table_name)
                    .set_item(Some(super::roles::audit_item(
                        &system_actor(&team, "SYSTEM#support-recovery", "CoderHelm support"),
                        super::roles::AuditEntry {
                            action: "sign_in_rules",
                            role_id: "",
                            role_name: "",
                            detail,
                        },
                    )))
                    .condition_expression("attribute_not_exists(pk)")
                    .build()
                    .map_err(b)?,
            )
            .build(),
    ];
    state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(items))
        .send()
        .await
        .map_err(|e| {
            let e = format!("{e:?}");
            if e.contains("TransactionCanceled") || e.contains("ConditionalCheckFailed") {
                (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "Nothing changed: the team's rule changed meanwhile, or the audit line couldn't be written. Reload and check.", "code": "changed" })),
                )
            } else {
                internal("Sign-in rules not saved", e)
            }
        })?;
    let emailed = tell_owners(
        &state,
        &team,
        "sso_recovered",
        "Require single sign-on was turned off",
        &recovered_words(provider),
        &[("provider", provider.name())],
    )
    .await;
    info!(team_id = %team, "Require single sign-on turned off by support recovery");
    Ok(Json(
        json!({ "require_sso": Value::Null, "owners_emailed": emailed }),
    ))
}

pub fn recovered_words(provider: SsoProvider) -> String {
    let p = provider.name();
    format!(
        "Require single sign-on ({p}) was turned off for your team from outside the product: by CoderHelm support after verifying an owner, or on a self-hosted install with its recovery script. Your audit log says who and why. People can sign in again the usual ways. When {p} works again, an owner turns the rule back on in Settings, Sign-in rules. If nobody at your team asked for this, contact CoderHelm support now."
    )
}

/// What to do with the recovery script's notice once its emails were
/// tried: removed only when at least one owner was emailed; otherwise kept
/// and tried again later. Pure.
pub fn notice_done(sent: usize) -> bool {
    sent > 0
}

/// Self-hosted: the recovery script turned the rule off and left a notice
/// on the organization record. A request that reads it takes a turn (a
/// conditional write, at most once every [`RECOVERY_RETRY_SECS`]) and emails
/// every owner; the notice goes only once an email went out, so a mail
/// failure is retried on a later read, and logged.
pub async fn send_recovery_notice(state: &AppState, org_team: &str, notice: &str) {
    let (pk, sk) = (sign_in::ORG_PK, sign_in::ORG_SK);
    let now = now_secs();
    let turn = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .update_expression("SET #t = :now")
        .condition_expression("#n = :v AND (attribute_not_exists(#t) OR #t < :before)")
        .expression_attribute_names("#n", RECOVERY_NOTICE)
        .expression_attribute_names("#t", RECOVERY_TRIED)
        .expression_attribute_values(":v", attr_s(notice))
        .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
        .expression_attribute_values(
            ":before",
            AttributeValue::N(now.saturating_sub(RECOVERY_RETRY_SECS).to_string()),
        )
        .send()
        .await
        .is_ok();
    if !turn {
        return;
    }
    let mut parts = notice.splitn(3, '|');
    let provider = parts
        .next()
        .and_then(SsoProvider::parse)
        .unwrap_or(SsoProvider::Okta);
    let by = parts
        .next()
        .filter(|v| !v.is_empty())
        .unwrap_or("an operator");
    info!(
        by,
        "Require single sign-on turned off with the recovery script"
    );
    let sent = tell_owners(
        state,
        org_team,
        "sso_recovered",
        "Require single sign-on was turned off",
        &recovered_words(provider),
        &[("provider", provider.name())],
    )
    .await;
    if !notice_done(sent) {
        warn!("No owner could be emailed about the single sign-on recovery; tried again later");
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .update_expression("REMOVE #n, #t")
        .condition_expression("#n = :v")
        .expression_attribute_names("#n", RECOVERY_NOTICE)
        .expression_attribute_names("#t", RECOVERY_TRIED)
        .expression_attribute_values(":v", attr_s(notice))
        .send()
        .await;
    info!(sent, "Owners emailed about the single sign-on recovery");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(p: Option<SsoProvider>) -> Rule {
        Rule {
            require_sso: p,
            sso_since: 1_000,
            ..Rule::default()
        }
    }

    fn proof(via: &str, mfa: bool) -> SignInProof {
        SignInProof::new(mfa, via)
    }

    fn entra() -> SignInProof {
        SignInProof::new(false, "microsoft").with_entra(Some("t/o".into()))
    }

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), attr_s(v)))
            .collect()
    }

    #[test]
    fn with_okta_required_only_okta_sessions_stand_and_nothing_bypasses_it() {
        let on = rule(Some(SsoProvider::Okta));
        // Every other way in is refused, with or without a second factor:
        // the owner's password and second factor too (no break-glass).
        for via in ["password", "google", "github", "microsoft"] {
            for mfa in [false, true] {
                assert!(!allowed(&on, &proof(via, mfa)), "{via} {mfa}");
            }
        }
        assert!(!allowed(&on, &entra()), "the team's tenant isn't its Okta");
        assert!(
            !allowed(&on, &SignInProof::default()),
            "a token from before proofs"
        );
        assert!(allowed(&on, &proof("okta", false)));
    }

    #[test]
    fn with_microsoft_required_only_the_teams_tenant_counts() {
        let on = rule(Some(SsoProvider::Microsoft));
        // A plain Microsoft sign-in (personal, another tenant, found by
        // email) carries no team tenant identity.
        for via in ["password", "google", "github", "microsoft", "okta"] {
            for mfa in [false, true] {
                assert!(!allowed(&on, &proof(via, mfa)), "{via} {mfa}");
            }
        }
        assert!(allowed(&on, &entra()));
        // The identity only counts on a Microsoft sign-in.
        let forged = SignInProof::new(true, "password").with_entra(Some("t/o".into()));
        assert!(!allowed(&on, &forged));
        let empty = SignInProof::new(false, "microsoft").with_entra(Some(String::new()));
        assert!(!allowed(&on, &empty));
    }

    #[test]
    fn with_the_rule_off_every_way_in_is_as_before() {
        let off = rule(None);
        for via in ["password", "google", "github", "microsoft", "okta"] {
            assert!(allowed(&off, &proof(via, false)));
        }
        assert!(allowed(&off, &entra()));
    }

    #[test]
    fn the_stored_rule_reads_back_and_fails_closed() {
        assert_eq!(SsoProvider::stored(""), None);
        assert_eq!(SsoProvider::stored("okta"), Some(SsoProvider::Okta));
        assert_eq!(
            SsoProvider::stored("microsoft"),
            Some(SsoProvider::Microsoft)
        );
        // Something this build doesn't know keeps the rule on.
        assert_eq!(SsoProvider::stored("saml"), Some(SsoProvider::Okta));
        for p in SsoProvider::ALL {
            assert_eq!(SsoProvider::parse(p.slug()), Some(p));
        }
    }

    #[test]
    fn a_hosted_sign_in_holds_back_only_the_teams_that_refuse_it() {
        let a = item(&[("pk", "TEAM#a"), ("sk", "USER#1"), ("role", "owner")]);
        let a_inv = item(&[
            ("pk", "TEAM#c"),
            ("sk", "USER#invite_1"),
            ("status", "invited"),
        ]);
        let b = item(&[("pk", "TEAM#b"), ("sk", "USER#1"), ("role", "member")]);
        let gated: HashSet<String> = ["TEAM#a".to_string(), "TEAM#c".to_string()].into();
        let f = filter(&[a.clone(), a_inv.clone(), b.clone()], &gated);
        assert_eq!(f.kept, vec![b.clone()]);
        assert_eq!(f.refused.as_deref(), Some("TEAM#a"));
        // Only gated teams: nothing kept, so no personal team is made. The
        // owner's own record is held back like anyone's.
        let f = filter(std::slice::from_ref(&a), &gated);
        assert!(f.kept.is_empty() && f.refused.is_some());
        let f = filter(&[a.clone(), b.clone()], &HashSet::new());
        assert_eq!(f.kept.len(), 2);
        assert!(f.refused.is_none());
    }

    #[test]
    fn the_provider_accepts_only_a_live_invite_for_exactly_its_email_and_never_takes_over_a_member()
    {
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
        assert_eq!(
            invite_for(std::slice::from_ref(&invite), "other@example.com", now),
            InviteFor::None
        );
        assert_eq!(
            invite_for(std::slice::from_ref(&invite), "", now),
            InviteFor::None
        );
        let old = item(&[
            ("pk", "TEAM#a"),
            ("sk", "USER#invite_2"),
            ("status", "invited"),
            ("email", "new@example.com"),
            ("invited_at", "2020-01-01T00:00:00Z"),
        ]);
        assert_eq!(invite_for(&[old], "new@example.com", now), InviteFor::None);
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
        // linked to this identity, active; for either provider.
        let okta = Link::Okta {
            issuer: "https://idp.example.com".into(),
            sub: "abc".into(),
        };
        let m = invited_member(&invite, "USER#okta_abc", "New", &okta, &fresh);
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
        let ms = Link::Entra {
            tid: "TID".into(),
            oid: "o1".into(),
        };
        let m = invited_member(&invite, "USER#microsoft_x_o1", "New", &ms, &fresh);
        assert_eq!(s(&m, "ms_tid"), "tid");
        assert_eq!(s(&m, "ms_oid"), "o1");
        assert_eq!(s(&m, "okta_sub"), "");
        let owner_invite = item(&[("pk", "TEAM#a"), ("sk", "USER#invite_3"), ("role", "owner")]);
        assert_eq!(
            s(
                &invited_member(&owner_invite, "USER#x", "", &okta, &fresh),
                "role"
            ),
            "member"
        );
        // Each provider's row keeps one identity per team.
        assert_eq!(okta.row("TEAM#a").0, "OKTA_LINK#TEAM#a");
        assert_eq!(
            ms.row("TEAM#a"),
            ("ENTRA_LINK#TEAM#a".into(), "tid#o1".into())
        );
        assert_eq!(okta.unlinked_condition(), "attribute_not_exists(okta_sub)");
        assert_eq!(ms.unlinked_condition(), "attribute_not_exists(ms_oid)");
    }

    #[test]
    fn turning_the_rule_on_needs_every_condition_and_names_the_first_missing() {
        for provider in SsoProvider::ALL {
            let all = Readiness {
                provider,
                set_up: true,
                enabled: true,
                domains_verified: true,
                session_from_provider: true,
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
                Some("sso_not_on")
            );
            assert_eq!(
                code(Readiness {
                    enabled: false,
                    ..all
                }),
                Some("sso_not_on")
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
                    session_from_provider: false,
                    ..all
                }),
                Some("sign_in_with_provider_first")
            );
            assert!(all.first_missing().is_none());
            let words = Readiness {
                session_from_provider: false,
                ..all
            }
            .first_missing()
            .unwrap()
            .2;
            assert!(words.contains(provider.name()), "{words}");
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
    }

    #[test]
    fn unlinked_members_are_listed_per_provider_and_invites_are_not() {
        let iss = "https://idp.example.com";
        let okta = Live::Okta(super::super::oidc_signin::OktaSetup {
            team_id: "T".into(),
            issuer: iss.into(),
            ..Default::default()
        });
        let ms = Live::Microsoft(super::super::entra_sso::EntraSetup {
            team_id: "T".into(),
            tid: "tid".into(),
            ..Default::default()
        });
        let linked = item(&[
            ("pk", "T"),
            ("sk", "USER#1"),
            ("okta_issuer", iss),
            ("okta_sub", "s1"),
            ("email", "a@example.com"),
        ]);
        let other_issuer = item(&[
            ("pk", "T"),
            ("sk", "USER#2"),
            ("okta_issuer", "https://old.example.com"),
            ("okta_sub", "s2"),
            ("email", "b@example.com"),
            ("ms_tid", "tid"),
            ("ms_oid", "o2"),
        ]);
        let none = item(&[("pk", "T"), ("sk", "USER#3"), ("email", "c@example.com")]);
        let invite = item(&[("pk", "T"), ("sk", "USER#invite_1"), ("status", "invited")]);
        let members = [linked, other_issuer, none, invite];
        let emails = |l: Vec<Value>| -> Vec<String> {
            l.iter()
                .map(|v| v["email"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(
            emails(unlinked(&members, Some(&okta))),
            ["b@example.com", "c@example.com"]
        );
        assert_eq!(
            emails(unlinked(&members, Some(&ms))),
            ["a@example.com", "c@example.com"]
        );
        assert_eq!(unlinked(&members, None).len(), 3);
    }

    #[test]
    fn a_refusal_names_the_team_and_the_provider() {
        assert_eq!(
            refusal_message(false, "Acme", SsoProvider::Okta),
            "Acme requires single sign-on with Okta. Sign in with Okta to continue."
        );
        assert!(refusal_message(false, " ", SsoProvider::Microsoft)
            .starts_with("This team requires single sign-on with Microsoft Entra"));
        assert!(
            refusal_message(true, "x", SsoProvider::Okta).starts_with("Your organization requires")
        );
        assert_eq!(
            sign_in_url(
                "https://app.example.com/",
                "TEAM#a b",
                SsoProvider::Microsoft
            ),
            "https://app.example.com/?signin_error=sso_required&sso=microsoft&team_id=TEAM%23a%20b"
        );
        let r = session_refused(&Refused {
            team: "Acme".into(),
            team_id: "TEAM#a".into(),
            provider: SsoProvider::Okta,
            code: CODE,
        });
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        let cookies: Vec<_> = r.headers().get_all(header::SET_COOKIE).iter().collect();
        assert!(cookies
            .iter()
            .any(|c| c.to_str().unwrap().contains("Max-Age=0")));
        // The setup lock holds for the provider the rule names only.
        let okta = rule(Some(SsoProvider::Okta));
        assert!(setup_change_refused(&okta, SsoProvider::Okta).is_some());
        assert!(setup_change_refused(&okta, SsoProvider::Microsoft).is_none());
        assert!(setup_change_refused(&rule(None), SsoProvider::Okta).is_none());
        let ms = rule(Some(SsoProvider::Microsoft));
        assert!(setup_change_refused(&ms, SsoProvider::Microsoft).is_some());
    }

    #[test]
    fn a_cut_off_keeps_ending_what_came_before_it_after_the_rule_goes_off() {
        let okta = SignInProof::new(false, "okta");
        let google = SignInProof::new(true, "google");
        let mut r = rule(Some(SsoProvider::Okta));
        r.sso_cutoff = 2_000;
        r.sso_cutoff_provider = Some(SsoProvider::Okta);
        assert!(!session_stands(&r, 1_999, &google));
        assert!(session_stands(&r, 1_999, &okta));
        // The rule is off now; the cut-off stays.
        r.require_sso = None;
        assert!(!session_stands(&r, 1_999, &google), "never revived");
        assert!(
            !session_stands(&r, 2_000, &google),
            "the same second counts as before"
        );
        assert!(session_stands(&r, 2_001, &google), "a sign-in after it");
        assert!(session_stands(&r, 1_999, &okta));
        assert!(session_stands(&rule(None), 1, &google));
        // The script and the owner remove the rule, never the cut-off.
        let script = include_str!("../../../../scripts/sso-recovery.py");
        assert!(!script.contains(CUTOFF_ATTR));
        let si = code(include_str!("sign_in.rs"));
        let u = body(si, "update_settings");
        assert!(u.contains("removes.push(\"#rs\")"));
        assert!(!u.contains("removes.push(\"#rco\")"));
        assert!(u.contains("end_refused_connections("));
    }

    #[test]
    fn keys_follow_their_maker_while_the_rule_is_on() {
        let on = rule(Some(SsoProvider::Okta));
        let live = Live::Okta(super::super::oidc_signin::OktaSetup {
            team_id: "T".into(),
            issuer: "https://idp.example.com".into(),
            enabled: true,
            ..Default::default()
        });
        let linked = item(&[
            ("sk", "USER#1"),
            ("okta_issuer", "https://idp.example.com"),
            ("okta_sub", "s"),
        ]);
        let unlinked = item(&[("sk", "USER#2")]);
        let invited = item(&[("sk", "USER#3"), ("status", "invited")]);
        assert!(key_maker_stands(&on, Some(&linked), Some(&live)));
        assert!(!key_maker_stands(&on, Some(&unlinked), Some(&live)));
        assert!(!key_maker_stands(&on, Some(&invited), Some(&live)));
        assert!(!key_maker_stands(&on, None, Some(&live)), "maker left");
        assert!(!key_maker_stands(&on, Some(&linked), None));
        assert!(
            key_maker_stands(&rule(None), None, None),
            "rule off: as before"
        );
        let mw = code(include_str!("../middleware/auth.rs"));
        assert!(before(
            body(mw, "api_key_request"),
            "key_maker_allowed(",
            "resolve_access("
        ));
        let mcp = code(include_str!("../mcp_server.rs"));
        assert_eq!(mcp.matches("key_follows_maker(state, k)").count(), 2);
    }

    #[test]
    fn recovery_is_outside_the_product_and_said_plainly() {
        for p in SsoProvider::ALL {
            let hosted = recovery_words(false, p);
            assert!(
                hosted.contains(p.name()) && hosted.contains("support"),
                "{hosted}"
            );
            assert!(hosted.contains("never gets a session"), "{hosted}");
            let selfh = recovery_words(true, p);
            assert!(selfh.contains("scripts/sso-recovery.py"), "{selfh}");
            assert!(selfh.contains("AWS credentials"), "{selfh}");
        }
        // The support action turns the rule off only: no session is made.
        let src = code(include_str!("require_sso.rs"));
        let f = body(src, "recover");
        assert!(f.contains("is_platform_admin(&claims)"));
        assert!(f.contains("p.mfa"));
        assert!(f.contains("MIN_VERIFICATION"));
        assert!(f.contains("REMOVE #rs, #since"));
        assert!(f.contains("tell_owners("));
        // The rule goes off with its audit line, in one transaction.
        assert!(before(f, "roles::audit_item(", ".transact_write_items()"));
        assert!(!f.contains("roles::record("));
        assert!(!f.contains("create_session_token"));
        assert!(!f.contains("SET_COOKIE"));
        // The script and the gateway agree on the names.
        let script = include_str!("../../../../scripts/sso-recovery.py");
        assert!(
            script.contains(&format!("\"{RULE_ATTR}\""))
                || script.contains(&format!("REMOVE {RULE_ATTR}, {SINCE_ATTR}"))
        );
        assert!(script.contains(&format!("NOTICE = \"{RECOVERY_NOTICE}\"")));
        assert!(script.contains(super::super::roles::AUDIT_PREFIX));
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
            ("entra_sso.rs", include_str!("entra_sso.rs"), 0),
            ("sso_claim.rs", include_str!("sso_claim.rs"), 0),
            ("require_sso.rs", include_str!("require_sso.rs"), 0),
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
            "selfhost_pre_gate(state, &super::require_sso::way_in(\"google\"))",
            "selfhost_landing("
        ));
        assert!(before(g, "require_sso::filter(", "accept_pending_invites("));
        assert!(before(g, "require_sso::filter(", "Google user created"));
        assert_eq!(g.matches("require_sso::issue_check(").count(), 2);
        let hosted = &g[g.find("Google user created").unwrap()..];
        assert!(before(
            hosted,
            "require_sso::issue_check(",
            "sign_in::second_step("
        ));
        let selfhosted = &g[..g.find("Open beta").unwrap()];
        assert!(before(
            selfhosted,
            "require_sso::issue_check(",
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
            "require_sso::check_claims(",
            "SET github_id = :gid"
        ));
        assert!(before(
            gh,
            "selfhost_pre_gate(state, &super::require_sso::way_in(\"github\"))",
            "selfhost_landing("
        ));
        assert!(before(gh, "require_sso::filter(", "github_landing("));
        assert!(before(gh, ".require_sso", "reader_team = Some(owner)"));
        assert!(before(
            gh,
            "require_sso::issue_check(",
            "sign_in::second_step("
        ));
        // The team's own tenant and claim links end in finish_oidc too: the
        // linked member, a claim accepted in its transaction, or an invite.
        let entra = code(include_str!("entra_sso.rs"));
        let land = body(entra, "entra_land");
        assert!(before(
            land,
            "tenant_of(claims, &setup.tid)",
            "finish_oidc("
        ));
        assert!(before(
            land,
            "linked_record(&members, &tid, &oid)",
            "finish_oidc("
        ));
        assert!(before(land, "sso_claim::redeem(", "invite_for("));
        assert_eq!(land.matches("finish_oidc(").count(), 3);
        assert!(!land.contains("records_by_email"));
        let claim = code(include_str!("sso_claim.rs"));
        let redeem = body(claim, "redeem");
        assert!(before(redeem, "redeemable(", ".transact_write_items()"));
        assert!(redeem.contains("domain_still_usable("));
        assert!(!claim.contains("create_session_token"));
        // Microsoft and Okta's shared ending.
        let o = body(auth, "finish_oidc");
        assert!(before(
            o,
            "selfhost_pre_gate(state, &way_in)",
            "selfhost_landing(state, email, &person.new_user_id"
        ));
        assert!(before(o, "require_sso::filter(", "accept_pending_invites("));
        assert!(before(
            o,
            "require_sso::issue_check(",
            "sign_in::second_step("
        ));
        // Password: before the join rules, held back on hosted, then the last
        // word, the second factor, the session. Nothing lets a password
        // through while the rule is on (no break-glass).
        let p = body(auth, "issue_session_from_cognito");
        assert!(before(
            p,
            "selfhost_pre_gate(state, proof)",
            "selfhost_landing("
        ));
        assert!(before(p, "require_sso::filter(", "accept_pending_invites("));
        assert!(!p.contains("session_json("));
        let ps = body(auth, "password_session");
        assert!(before(
            ps,
            "require_sso::issue_check(",
            "sign_in::password_step("
        ));
        assert!(before(ps, "sign_in::password_step(", "session_json("));
        assert!(!auth.contains("break_glass"));
        // The second step: the rule's last word before the session.
        let si = code(include_str!("sign_in.rs"));
        let v = body(si, "verify_second_step");
        assert!(before(v, "use_app_code(", "require_sso::issue_check("));
        assert!(before(
            v,
            "require_sso::issue_check(",
            "create_session_token("
        ));
        assert!(!si.contains("break_glass"));
        // A switch into a gated team, and the team list's invites.
        let api = code(include_str!("api.rs"));
        let sw = body(api, "switch_team");
        assert!(before(sw, ".require_sso", "create_session_token("));
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
            "require_sso::check_claims(",
            "support_access::check_session("
        ));
        assert!(body(mw, "app_request")
            .contains("sign_in::check_session(state, &claims, &proof, handler, true)"));
        let sa = body(mw, "session_answers_as");
        assert!(sa.contains("check_member_session(") && sa.contains("require_sso::check_claims("));
        let mcp = code(include_str!("../mcp_server.rs"));
        assert_eq!(mcp.matches("check_session(").count(), 2);
        assert_eq!(mcp.matches("None, true").count(), 2);
        let oidc = code(include_str!("oidc_signin.rs"));
        assert!(body(oidc, "session_of").contains("require_sso::check_claims("));
        for (src, f) in [
            (include_str!("agent_policy.rs"), "link_answer"),
            (include_str!("plugin_pkgs.rs"), "email_approve"),
        ] {
            let b = body(code(src), f);
            assert!(b.contains("session_answers_as("), "{f}");
            assert!(!b.contains("auth::signed_in_as("), "{f}");
        }
        let d = body(code(include_str!("mcp_oauth.rs")), "decide_request");
        assert!(before(
            d,
            "require_sso::allowed(&rule, &proof)",
            "oauth::approve("
        ));
        // An invite made while the rule is on waits to be accepted with Okta.
        let inv = body(code(include_str!("users.rs")), "invite_user");
        assert!(inv.contains("existing_sub.filter(|_| !okta_only)"));
    }

    #[test]
    fn a_support_view_of_a_gated_team_needs_a_second_factor() {
        let on = rule(Some(SsoProvider::Okta));
        assert!(!support_view_allowed(
            &on,
            &SignInProof::new(false, "google")
        ));
        assert!(support_view_allowed(&on, &SignInProof::new(true, "google")));
        assert!(support_view_allowed(&rule(None), &SignInProof::default()));
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
        assert!(body(oidc, "domains_proven_list").contains("domains_proven_now("));
        assert!(body(oidc, "domains_proven_now").contains("counts_at_turn_on("));
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
                before(b, "validate_scoped_token(", "require_sso::check_scoped("),
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
        assert!(body(oidc, "okta_discard_draft").contains("release_if_unused("));
        assert!(body(oidc, "okta_test").contains("release_if_unused("));
        assert!(body(oidc, "release_if_unused").contains("release_domain_while_rule_off("));
        // A domain the Microsoft setup lists keeps its proof.
        assert!(before(
            body(oidc, "release_if_unused"),
            "microsoft_uses(",
            "release_domain_while_rule_off("
        ));
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
