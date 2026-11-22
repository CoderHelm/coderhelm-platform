//! A team's own Microsoft Entra tenant, for "Require single sign-on".
//!
//! Sign in with Microsoft (routes::oidc_signin) is one app for the whole
//! deployment, for every work, school and personal account, and it finds
//! people by email. That is fine as one way in among several, but a team
//! that wants Microsoft to be the *only* way in needs Microsoft to vouch
//! for exactly its people. So a team (self-hosted: the organization) can
//! record its own tenant here:
//!
//! - **Setup** (`pk = <team>`, `sk = SETTINGS#ENTRA`): the tenant id (`tid`,
//!   a GUID, never the personal-accounts tenant) and, on hosted, the email
//!   domains it vouches for, each proven with the same DNS TXT proof as an
//!   Okta domain (`OKTA_DOMAIN#<domain>`). Only the owner saves it.
//! - **Test sign-in**: the owner signs in through that tenant
//!   (`login.microsoftonline.com/<tid>/`, the password asked again). Every
//!   check a real sign-in makes runs, the token's issuer must be exactly
//!   the tenant's, and the result is recorded as theirs (`tested_by`,
//!   `tested_oid`). Turning the setup on needs the owner's own test within
//!   the last hour (`CLIENT_TEST_MAX_SECS`, the same rule as a new Okta app)
//!   and, on hosted, every domain proven in DNS now.
//! - **Linking**: a member links their own account in Settings, Security,
//!   from a session of theirs: a trip through the tenant that stores
//!   `ms_tid` and `ms_oid` on their record, with a uniqueness row
//!   `pk = ENTRA_LINK#<team>`, `sk = <tid>#<oid>` written in the same
//!   transaction (one identity, one member). A new tenant drops every link.
//! - **Signing in**: `/auth/microsoft?team_id=<team>` signs in through the
//!   team's tenant only. It lands on the member who linked exactly that
//!   tenant and object id, never by email; else, while the team requires
//!   single sign-on, a claim link (routes::sso_claim) or an invite for
//!   exactly Microsoft's verified email at a proven domain. The session it
//!   makes carries `entra = <tid>/<oid>` and is held to its team on hosted,
//!   as an Okta session is. It lasts only while the link and the live setup
//!   still match (`session_state`).
//! - Microsoft's email counts only with `xms_edov` true, as for every
//!   Microsoft sign-in. Personal accounts and other tenants never count.

use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem, Update};
use axum::{
    extract::{Extension, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use super::oauth_signin::{self, CallbackQuery, Provider, Reason, SignInFailure};
use super::oidc_signin::{self as oidc, Flow};
use super::require_sso::{self, Link, SsoProvider};
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

pub const SETUP_SK: &str = "SETTINGS#ENTRA";
pub const LINK_PREFIX: &str = "ENTRA_LINK#";
/// The tenant of personal Microsoft accounts: never a team's tenant.
pub const CONSUMER_TENANT: &str = "9188040d-6c67-4c5b-b112-36a304b66dad";
const P: Provider = Provider::Microsoft;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s<'a>(item: &'a Item, k: &str) -> &'a str {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map_or("", String::as_str)
}

fn opt(item: &Item, k: &str) -> Option<String> {
    Some(s(item, k))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn fail_code(code: StatusCode, msg: &str, key: &str) -> ApiError {
    (code, Json(json!({ "error": msg, "code": key })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    tracing::error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn now_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

// ── The setup ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntraSetup {
    pub team_id: String,
    /// The tenant id, lowercase.
    pub tid: String,
    pub domains: Vec<String>,
    pub enabled: bool,
    /// The last Test sign-in through this tenant: when, by which member,
    /// as which object id, with which verified email.
    pub tested_at: Option<String>,
    pub tested_by: Option<String>,
    pub tested_oid: Option<String>,
    pub tested_email: Option<String>,
    pub updated_at: Option<String>,
    pub updated_by: Option<String>,
}

impl EntraSetup {
    pub fn from_item(item: &Item) -> Option<EntraSetup> {
        let tid = s(item, "tid");
        if tid.is_empty() {
            return None;
        }
        Some(EntraSetup {
            team_id: s(item, "pk").to_string(),
            tid: tid.to_lowercase(),
            domains: item
                .get("domains")
                .and_then(|v| v.as_l().ok())
                .map(|l| {
                    l.iter()
                        .filter_map(|d| d.as_s().ok())
                        .map(|d| d.to_lowercase())
                        .collect()
                })
                .unwrap_or_default(),
            enabled: item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            tested_at: opt(item, "tested_at"),
            tested_by: opt(item, "tested_by"),
            tested_oid: opt(item, "tested_oid"),
            tested_email: opt(item, "tested_email"),
            updated_at: opt(item, "updated_at"),
            updated_by: opt(item, "updated_by"),
        })
    }

    fn to_item(&self) -> Item {
        let mut item = HashMap::from([
            ("pk".to_string(), attr_s(&self.team_id)),
            ("sk".to_string(), attr_s(SETUP_SK)),
            ("tid".to_string(), attr_s(&self.tid)),
            (
                "domains".to_string(),
                AttributeValue::L(self.domains.iter().map(|d| attr_s(d)).collect()),
            ),
            ("enabled".to_string(), AttributeValue::Bool(self.enabled)),
        ]);
        for (k, v) in [
            ("tested_at", &self.tested_at),
            ("tested_by", &self.tested_by),
            ("tested_oid", &self.tested_oid),
            ("tested_email", &self.tested_email),
            ("updated_at", &self.updated_at),
            ("updated_by", &self.updated_by),
        ] {
            if let Some(v) = v {
                item.insert(k.to_string(), attr_s(v));
            }
        }
        item
    }

    pub fn public(&self) -> Value {
        json!({
            "tenant_id": self.tid,
            "domains": self.domains,
            "enabled": self.enabled,
            "tested_at": self.tested_at,
            "tested_email": self.tested_email,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        })
    }
}

/// The issuer of the tenant's tokens.
pub fn tenant_issuer(tid: &str) -> String {
    format!("{}/{tid}/v2.0", oidc::MS_AUTHORITY)
}

/// A tenant id an owner typed: a GUID (lowercased), never the personal
/// accounts' tenant or a multi-tenant alias.
pub fn clean_tid(input: &str) -> Result<String, &'static str> {
    let t = input.trim().to_lowercase();
    if !oidc::is_guid(&t) {
        return Err("Enter your Microsoft Entra tenant ID: a GUID such as 00000000-0000-0000-0000-000000000000 (Microsoft Entra admin center, Overview).");
    }
    if t == CONSUMER_TENANT {
        return Err("That is the tenant of personal Microsoft accounts. Enter your organization's own tenant ID.");
    }
    Ok(t)
}

/// The member id a new record made through the tenant gets (an invite
/// accepted): a short hash of the tenant and the object id.
pub fn ms_user_id(tid: &str, oid: &str) -> String {
    let h = Sha256::digest(tid.to_lowercase().as_bytes());
    let short: String = h.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("USER#microsoft_{short}_{oid}")
}

/// The identity a session from the tenant carries: `<tid>/<oid>`.
pub fn entra_id(tid: &str, oid: &str) -> String {
    format!("{}/{oid}", tid.to_lowercase())
}

pub fn split_entra_id(v: &str) -> Option<(&str, &str)> {
    let (t, o) = v.split_once('/')?;
    (!t.is_empty() && !o.is_empty()).then_some((t, o))
}

pub async fn load(state: &AppState, team: &str) -> Result<Option<EntraSetup>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(SETUP_SK))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.item().and_then(EntraSetup::from_item))
}

// ── Links and sessions ─────────────────────────────────────────────

pub fn link_row(team: &str, tid: &str, oid: &str) -> (String, String) {
    (
        format!("{LINK_PREFIX}{team}"),
        format!("{}#{oid}", tid.to_lowercase()),
    )
}

/// Whether a member's link is to `setup`'s tenant.
pub fn linked_to(member: &Item, setup: Option<&EntraSetup>) -> bool {
    setup.is_some_and(|l| !s(member, "ms_oid").is_empty() && s(member, "ms_tid") == l.tid)
}

/// The active member who linked exactly this tenant and object id.
pub fn linked_record<'a>(members: &'a [Item], tid: &str, oid: &str) -> Option<&'a Item> {
    if tid.is_empty() || oid.is_empty() {
        return None;
    }
    let tid = tid.to_lowercase();
    members.iter().find(|m| {
        s(m, "sk").starts_with("USER#")
            && s(m, "status") != "invited"
            && s(m, "ms_tid") == tid
            && s(m, "ms_oid") == oid
    })
}

/// Whether a session the tenant signed in at `iat` still stands: the setup
/// is on, the member's link is this session's identity on the live tenant,
/// and nothing ended it since. Pure.
pub fn session_state(
    member: Option<&Item>,
    live: Option<&EntraSetup>,
    iat: u64,
    entra: Option<&str>,
) -> Result<(), super::sign_in::Ended> {
    use super::sign_in::Ended;
    let (Some(m), Some(l), Some((tid, oid))) = (member, live, entra.and_then(split_entra_id))
    else {
        return Err(Ended::Revoked);
    };
    let after = m
        .get("ms_valid_after")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if !l.enabled
        || tid != l.tid
        || s(m, "ms_tid") != l.tid
        || s(m, "ms_oid").is_empty()
        || s(m, "ms_oid") != oid
        || iat < after
    {
        return Err(Ended::Revoked);
    }
    Ok(())
}

async fn drop_links(state: &AppState, team: &str) -> Result<usize, String> {
    let members = super::sign_in::team_members(state, team).await?;
    let mut n = 0;
    for m in members.iter().filter(|m| !s(m, "ms_oid").is_empty()) {
        unlink(state, team, m).await?;
        n += 1;
    }
    // Rows whose member left the team go too.
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s(&format!("{LINK_PREFIX}{team}")))
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    for i in out.items() {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(s(i, "pk")))
            .key("sk", attr_s(s(i, "sk")))
            .send()
            .await;
    }
    Ok(n)
}

async fn unlink(state: &AppState, team: &str, member: &Item) -> Result<(), String> {
    let (pk, sk) = link_row(team, s(member, "ms_tid"), s(member, "ms_oid"));
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&pk))
        .key("sk", attr_s(&sk))
        .send()
        .await;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(s(member, "sk")))
        .update_expression("REMOVE ms_tid, ms_oid, ms_linked_at SET ms_valid_after = :n")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":n", AttributeValue::N(now_secs().to_string()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

// ── The trip through the tenant ─────────────────────────────────────

/// What a trip through the team's tenant is for.
pub enum Mode<'a> {
    SignIn,
    /// The owner's Test sign-in (their member id).
    Test(&'a str),
    /// A member linking their own account (their member id).
    Link(&'a str),
    /// A claim link (routes::sso_claim): the member and the secret's hash.
    Claim(&'a str),
}

/// Begins the trip: the tenant's own sign-in page (no other tenant and no
/// personal account can finish it), with the signed flow cookie.
pub fn start(
    state: &AppState,
    setup: &EntraSetup,
    mode: Mode<'_>,
) -> Result<Response, SignInFailure> {
    let Some((client_id, _)) = oidc::microsoft_client(state) else {
        return Err(SignInFailure::new(
            P,
            Reason::NotSetUp,
            "start",
            "no app registration",
        ));
    };
    let mut flow = Flow::new(P, &setup.team_id, matches!(mode, Mode::Test(_)));
    let ask_again = !matches!(mode, Mode::SignIn | Mode::Claim(_));
    match mode {
        Mode::Test(by) => flow.by = by.to_string(),
        Mode::Link(m) => flow.link = m.to_string(),
        Mode::Claim(c) => flow.claim = c.to_string(),
        Mode::SignIn => {}
    }
    let cookie = oidc::flow_cookie(P, &flow, &state.secrets.jwt_secret)
        .map_err(|e| SignInFailure::new(P, Reason::Server, "start", e))?;
    let csrf = oidc::random_token();
    let redirect_uri = oidc::microsoft_redirect_uri(oidc::api(state));
    let challenge = oidc::pkce_challenge(&flow.verifier);
    let url = oidc::authorize_url(
        &format!("{}/{}/oauth2/v2.0/authorize", oidc::MS_AUTHORITY, setup.tid),
        &[
            ("client_id", client_id),
            ("response_type", "code"),
            ("redirect_uri", &redirect_uri),
            ("response_mode", "query"),
            ("scope", "openid email profile"),
            ("state", &csrf),
            ("nonce", &flow.nonce),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("prompt", if ask_again { "login" } else { "select_account" }),
        ],
    );
    Ok(oidc::to_provider(P, &url, &csrf, &cookie))
}

/// The team whose tenant a sign-in for `team_id` uses (self-hosted: the
/// organization's), with its live, turned-on setup.
pub async fn live_for(state: &AppState, team_id: &str) -> Result<Option<EntraSetup>, String> {
    let team = if super::sign_in::applies() {
        match super::sign_in::load_org(state).await? {
            Some(o) => o.team_id,
            None => return Ok(None),
        }
    } else {
        team_id.to_string()
    };
    Ok(load(state, &team).await?.filter(|s| s.enabled))
}

/// `/auth/microsoft?team_id=<team>`, or a self-hosted install whose
/// organization requires single sign-on with Microsoft: the trip through
/// the team's tenant. `None`: the plain Microsoft sign-in.
pub async fn sign_in_start(state: &AppState, team_id: Option<&str>) -> Option<Response> {
    let team = match team_id.map(str::trim).filter(|t| !t.is_empty()) {
        Some(t) => t.to_string(),
        None if super::sign_in::applies() => {
            let rule = super::sign_in::team_rule(state, "").await.ok()?;
            if rule.require_sso != Some(SsoProvider::Microsoft) {
                return None;
            }
            String::new()
        }
        None => return None,
    };
    let app = common::public_urls::app_url(&state.config.stage);
    Some(match live_for(state, &team).await {
        Ok(Some(setup)) => match start(state, &setup, Mode::SignIn) {
            Ok(r) => r,
            Err(f) => f.into_redirect(app),
        },
        Ok(None) => SignInFailure::new(
            P,
            Reason::NotSetUp,
            "setup",
            "the team has no Microsoft Entra tenant on",
        )
        .into_redirect(app),
        Err(e) => SignInFailure::new(P, Reason::Server, "setup", e).into_redirect(app),
    })
}

/// A claim link for a team that requires single sign-on with Microsoft.
pub async fn start_claim(
    state: &AppState,
    team: &str,
    carried: &str,
) -> Result<Response, &'static str> {
    let setup = load(state, team)
        .await
        .map_err(|_| "server")?
        .filter(|s| s.enabled)
        .ok_or("rule_off")?;
    start(state, &setup, Mode::Claim(carried)).map_err(|_| "server")
}

/// The checked claims of a trip through `tid`: state, code exchange, keys,
/// and a token whose issuer is exactly the tenant's.
async fn tenant_claims(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
    tid: &str,
) -> Result<Value, SignInFailure> {
    let Some((client_id, client_secret)) = oidc::microsoft_client(state) else {
        return Err(SignInFailure::new(
            P,
            Reason::NotSetUp,
            "callback",
            "no app registration",
        ));
    };
    let code = oauth_signin::check_callback(P, cookies, params)?;
    let id_token = oidc::exchange(
        oidc::idp(),
        &format!("{}/{tid}/oauth2/v2.0/token", oidc::MS_AUTHORITY),
        client_id,
        client_secret,
        oidc::ClientAuth::Post,
        code,
        &oidc::microsoft_redirect_uri(oidc::api(state)),
        &flow.verifier,
    )
    .await
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "token_exchange", e))?;
    let jwks = oidc::keys(oidc::idp(), oidc::MS_KEYS)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "keys", e))?;
    let issuer = tenant_issuer(tid);
    let claims = oidc::verify_id_token(
        &id_token,
        &jwks,
        &oidc::Expect {
            issuer: oidc::Issuer::Exact(&issuer),
            client_id,
            nonce: &flow.nonce,
            now: chrono::Utc::now().timestamp() as u64,
        },
    )
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "id_token", e))?;
    tenant_of(&claims, tid)?;
    Ok(claims)
}

/// The token's tenant and object id, only when it is exactly `tid`'s.
pub fn tenant_of(claims: &Value, tid: &str) -> Result<(String, String), SignInFailure> {
    let t = claims["tid"].as_str().unwrap_or_default().to_lowercase();
    let oid = claims["oid"].as_str().unwrap_or_default().to_string();
    if t.is_empty() || t != tid.to_lowercase() || t == CONSUMER_TENANT {
        return Err(SignInFailure::new(
            P,
            Reason::NotOnTeam,
            "tenant",
            "the token is not from the team's tenant",
        ));
    }
    if oid.is_empty() {
        return Err(SignInFailure::new(
            P,
            Reason::Provider,
            "id_token",
            "no oid",
        ));
    }
    if is_guest(claims, &t) {
        return Err(SignInFailure::new(
            P,
            Reason::NotOnTeam,
            "tenant",
            "a guest in the team's tenant, signed in by another directory",
        ));
    }
    Ok((t, oid))
}

/// Whether the token is a guest's in `tid` (B2B): an `idp` claim naming a
/// directory other than the tenant itself (its v2 or v1 issuer), or
/// `acct` = 1. Only the tenant's own members count. Pure.
pub fn is_guest(claims: &Value, tid: &str) -> bool {
    let own = [
        tenant_issuer(tid),
        format!("https://sts.windows.net/{tid}/"),
    ];
    let foreign_idp = claims["idp"]
        .as_str()
        .is_some_and(|idp| !own.iter().any(|o| o.eq_ignore_ascii_case(idp)));
    let guest_acct = claims["acct"].as_u64() == Some(1) || claims["acct"].as_str() == Some("1");
    foreign_idp || guest_acct
}

/// The Microsoft callback for a trip that named a team (`flow.team`): a
/// Test sign-in, a link, a claim or a sign-in through the team's tenant.
pub async fn callback(
    state: &Arc<AppState>,
    ip: &str,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
) -> Response {
    let app = common::public_urls::app_url(&state.config.stage);
    let setup = match load(state, &flow.team).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return if flow.test {
                test_redirect(state, Err("no_setup"))
            } else if !flow.link.is_empty() {
                link_redirect(state, Err("off"))
            } else {
                SignInFailure::new(P, Reason::NotSetUp, "setup", "no tenant set up")
                    .into_redirect(app)
            }
        }
        Err(_) => {
            return SignInFailure::new(P, Reason::Server, "setup", "not read").into_redirect(app)
        }
    };
    if flow.test {
        return test_return(state, cookies, params, flow, &setup).await;
    }
    if !setup.enabled {
        return if !flow.link.is_empty() {
            link_redirect(state, Err("off"))
        } else {
            SignInFailure::new(P, Reason::NotSetUp, "setup", "the tenant is off").into_redirect(app)
        };
    }
    if !flow.link.is_empty() {
        return link_return(state, cookies, params, flow, &setup).await;
    }
    let claims = match tenant_claims(state, cookies, params, flow, &setup.tid).await {
        Ok(c) => c,
        Err(f) => return f.into_redirect(app),
    };
    let claim = Some(flow.claim.as_str()).filter(|c| !c.is_empty());
    match entra_land(state, ip, &setup, &claims, claim).await {
        Ok(r) => r,
        Err(f) => f.into_redirect(app),
    }
}

/// Where a sign-in through the team's tenant lands once its token is
/// checked: the linked member, else (while the team requires single sign-on
/// with Microsoft) the claim it carries or an invite for exactly its
/// verified email at a proven domain. Never by email for anyone already on
/// the team.
pub(crate) async fn entra_land(
    state: &Arc<AppState>,
    ip: &str,
    setup: &EntraSetup,
    claims: &Value,
    claim: Option<&str>,
) -> Result<Response, SignInFailure> {
    let (tid, oid) = tenant_of(claims, &setup.tid)?;
    let email = oidc::microsoft_email(claims)
        .map_err(|e| SignInFailure::new(P, Reason::NoVerifiedEmail, "email", e))?;
    let person = oidc::Person {
        email: email.clone(),
        name: claims["name"].as_str().unwrap_or_default().to_string(),
        new_user_id: ms_user_id(&tid, &oid),
    };
    let entra = Some(entra_id(&tid, &oid));
    let members = super::sign_in::team_members(state, &setup.team_id)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "members", e))?;
    if let Some(record) = linked_record(&members, &tid, &oid) {
        let landing = super::auth::SsoLanding {
            record,
            joined_now: false,
            okta_sub: None,
            entra,
        };
        return super::auth::finish_oidc(state, ip, P, &person, Some(landing)).await;
    }
    let rule = super::sign_in::team_rule(state, &setup.team_id)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "rule", e))?;
    if rule.require_sso != Some(SsoProvider::Microsoft) {
        return Err(SignInFailure::new(
            P,
            Reason::SsoNotLinked,
            "link",
            "no member has linked this Microsoft account",
        ));
    }
    let ask_admin = |d: &str| SignInFailure::new(P, Reason::SsoAskAdmin, "invite", d);
    let link = Link::Entra {
        tid: tid.clone(),
        oid: oid.clone(),
    };
    if let Some(c) = claim {
        let record = super::sso_claim::redeem(
            state,
            &setup.team_id,
            c,
            SsoProvider::Microsoft,
            &link,
            &email,
        )
        .await
        .map_err(|why| ask_admin(&format!("claim not accepted: {why}")))?;
        let landing = super::auth::SsoLanding {
            record: &record,
            joined_now: true,
            okta_sub: None,
            entra,
        };
        return super::auth::finish_oidc(state, ip, P, &person, Some(landing)).await;
    }
    // An invite: hosted, only at a domain this team proved and that isn't
    // failing (Microsoft vouches for the domain owner, the team for itself).
    if !super::sign_in::applies() {
        let domain = super::sign_in::email_domain(&email).unwrap_or_default();
        let ours = setup.domains.contains(&domain)
            && oidc::domain_usable(state, &setup.team_id, &domain)
                .await
                .map_err(|e| SignInFailure::new(P, Reason::Server, "domain", e))?;
        if !ours {
            return Err(ask_admin(
                "the email's domain isn't a proven domain of the team",
            ));
        }
    }
    let invite_domain = (!super::sign_in::applies())
        .then(|| super::sign_in::email_domain(&email))
        .flatten();
    let invite = match require_sso::invite_for(&members, &email, chrono::Utc::now()) {
        require_sso::InviteFor::Accept(i) => i,
        require_sso::InviteFor::ExistingMember => {
            return Err(ask_admin(
                "a member has this email but never linked Microsoft",
            ))
        }
        require_sso::InviteFor::None => {
            return Err(ask_admin(
                "no member linked this account and no invite is waiting",
            ))
        }
    };
    let record = require_sso::invited_member(
        &invite,
        &person.new_user_id,
        &person.name,
        &link,
        &chrono::Utc::now().to_rfc3339(),
    );
    if let Err(e) =
        require_sso::accept_invite(state, &record, &invite, &link, invite_domain.as_deref()).await
    {
        return Err(ask_admin(&e));
    }
    info!(team_id = %setup.team_id, "Invite accepted through the team's Microsoft tenant");
    let landing = super::auth::SsoLanding {
        record: &record,
        joined_now: true,
        okta_sub: None,
        entra,
    };
    super::auth::finish_oidc(state, ip, P, &person, Some(landing)).await
}

// ── Test sign-in ────────────────────────────────────────────────────

const TEST_AUD: &str = "entra_test";
const LINK_AUD: &str = "entra_link";

pub fn test_result_url(app_url: &str, outcome: Result<(), &str>) -> String {
    let base = format!("{}/settings/sign-in/", app_url.trim_end_matches('/'));
    match outcome {
        Ok(()) => format!("{base}?microsoft_test=passed#microsoft"),
        Err(c) => format!(
            "{base}?microsoft_test=failed&reason={}#microsoft",
            urlencoding::encode(c)
        ),
    }
}

pub fn link_result_url(app_url: &str, outcome: Result<(), &str>) -> String {
    let base = format!("{}/settings/security/", app_url.trim_end_matches('/'));
    match outcome {
        Ok(()) => format!("{base}?microsoft_link=linked#microsoft"),
        Err(c) => format!(
            "{base}?microsoft_link=failed&reason={}#microsoft",
            urlencoding::encode(c)
        ),
    }
}

fn redirect_to(url: String) -> Response {
    let mut resp = (StatusCode::FOUND, [(header::LOCATION, url)]).into_response();
    for c in oauth_signin::clear_cookies(P) {
        if let Ok(v) = HeaderValue::from_str(&c) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    resp
}

fn test_redirect(state: &AppState, outcome: Result<(), &str>) -> Response {
    redirect_to(test_result_url(oidc::app(state), outcome))
}

fn link_redirect(state: &AppState, outcome: Result<(), &str>) -> Response {
    redirect_to(link_result_url(oidc::app(state), outcome))
}

/// The end of a Test sign-in: recorded as the owner's who ran it, as the
/// object id it signed in as, only for the setup that was tested. Nobody is
/// signed in.
async fn test_return(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
    setup: &EntraSetup,
) -> Response {
    let claims = match tenant_claims(state, cookies, params, flow, &setup.tid).await {
        Ok(c) => c,
        Err(f) => {
            warn!(
                step = f.step,
                reason = f.reason.code(),
                "Microsoft test sign-in failed"
            );
            let code = match (f.step, f.reason) {
                (_, Reason::Cancelled) => "cancelled",
                (_, Reason::Expired) => "expired",
                ("tenant", _) => "other_tenant",
                ("token_exchange", _) => "token_exchange",
                ("id_token", _) => "id_token",
                _ => "server",
            };
            return test_redirect(state, Err(code));
        }
    };
    let email = match oidc::microsoft_email(&claims) {
        Ok(e) => e,
        Err(_) => return test_redirect(state, Err("no_verified_email")),
    };
    if !super::sign_in::applies()
        && !super::sign_in::email_domain(&email).is_some_and(|d| setup.domains.contains(&d))
    {
        return test_redirect(state, Err("domain"));
    }
    let oid = claims["oid"].as_str().unwrap_or_default();
    let mut up = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&setup.team_id))
        .key("sk", attr_s(SETUP_SK))
        .update_expression("SET tested_at = :t, tested_by = :b, tested_oid = :o, tested_email = :e")
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":b", attr_s(&flow.by))
        .expression_attribute_values(":o", attr_s(oid))
        .expression_attribute_values(":e", attr_s(&email))
        .expression_attribute_values(":tid", attr_s(&setup.tid));
    // Only the setup that was tested: one saved since needs its own test.
    up = match &setup.updated_at {
        Some(ua) => up
            .condition_expression("tid = :tid AND updated_at = :ua")
            .expression_attribute_values(":ua", attr_s(ua)),
        None => up.condition_expression("tid = :tid AND attribute_not_exists(updated_at)"),
    };
    match up.send().await {
        Ok(_) => {
            info!(team_id = %setup.team_id, "Microsoft test sign-in passed");
            test_redirect(state, Ok(()))
        }
        Err(_) => test_redirect(state, Err("changed")),
    }
}

/// Whether the owner may turn the setup on now: their own Test sign-in of
/// exactly this tenant, within the last hour. Pure.
pub fn may_turn_on(
    claims: &Claims,
    setup: &EntraSetup,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), &'static str> {
    if claims.role != "owner" || claims.support_view.is_some() {
        return Err("Only the owner can turn on the team's Microsoft tenant.");
    }
    if setup.tested_by.as_deref() != Some(claims.sub.as_str()) || setup.tested_oid.is_none() {
        return Err("Run Test sign-in with your own Microsoft account first.");
    }
    let fresh = setup
        .tested_at
        .as_deref()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .is_some_and(|t| {
            (now - t.with_timezone(&chrono::Utc)).num_seconds() <= oidc::CLIENT_TEST_MAX_SECS
        });
    if !fresh {
        return Err("Run Test sign-in again: the last one is over an hour old.");
    }
    Ok(())
}

// ── Settings, Sign-in rules ─────────────────────────────────────────

fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|_| fail(StatusCode::FORBIDDEN, "Only admins and owners can do that."))
}

fn owner(claims: &Claims) -> Result<(), ApiError> {
    if claims.role != "owner" || claims.support_view.is_some() {
        return Err(fail_code(
            StatusCode::FORBIDDEN,
            "Only the owner can change the team's Microsoft tenant.",
            "owner_only",
        ));
    }
    Ok(())
}

async fn audit(state: &AppState, claims: &Claims, team: &str, detail: String) {
    require_sso::audit(state, claims, team, detail).await;
}

async fn view(state: &AppState, team: &str) -> Result<Value, ApiError> {
    let setup = load(state, team)
        .await
        .map_err(|e| internal("Microsoft setup not read", e))?;
    let domains = match &setup {
        Some(s) if !super::sign_in::applies() => oidc::domain_proofs(state, team, &s.domains)
            .await
            .map_err(|e| internal("Domains not read", e))?,
        _ => Vec::new(),
    };
    Ok(json!({
        "available": oidc::microsoft_client(state).is_some(),
        "redirect_uri": oidc::microsoft_redirect_uri(oidc::api(state)),
        "setup": setup.as_ref().map(EntraSetup::public),
        "self_hosted": super::sign_in::applies(),
        "domains": domains,
    }))
}

/// GET /api/sign-in/microsoft: the team's (self-hosted: the
/// organization's) Microsoft tenant and its domains' proofs.
pub async fn get_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    Ok(Json(view(&state, &team).await?))
}

async fn setup_team(state: &AppState, claims: &Claims) -> Result<String, ApiError> {
    require_sso::rule_team(state, claims)
        .await
        .map_err(|e| internal("Organization not read", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "This team isn't the organization."))
}

#[derive(Deserialize)]
pub struct SaveBody {
    pub tenant_id: String,
    #[serde(default)]
    pub domains: Vec<String>,
}

/// PUT /api/sign-in/microsoft: save the tenant (and on hosted its email
/// domains). Saved off and untested: the owner runs Test sign-in and turns
/// it on. A new tenant drops every link. Refused while the team requires
/// single sign-on with Microsoft.
pub async fn save_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SaveBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    owner(&claims)?;
    let team = setup_team(&state, &claims).await?;
    let tid = clean_tid(&body.tenant_id).map_err(|m| fail(StatusCode::BAD_REQUEST, m))?;
    let domains = super::sign_in::clean_domains(&body.domains)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    if domains.is_empty() && !super::sign_in::applies() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Add the email domain your people sign in with, such as example.com.",
        ));
    }
    let before = load(&state, &team)
        .await
        .map_err(|e| internal("Microsoft setup not read", e))?;
    let setup = EntraSetup {
        team_id: team.clone(),
        tid: tid.clone(),
        domains,
        enabled: false,
        tested_at: None,
        tested_by: None,
        tested_oid: None,
        tested_email: None,
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_by: Some(claims.display_name()),
    };
    let put = Put::builder()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(setup.to_item()))
        .build()
        .map_err(|e| internal("Microsoft setup not saved", e))?;
    require_sso::while_rule_off(
        &state,
        &team,
        Some(SsoProvider::Microsoft),
        vec![TransactWriteItem::builder().put(put).build()],
    )
    .await?;
    if before.as_ref().is_some_and(|b| b.tid != tid) {
        drop_links(&state, &team)
            .await
            .map_err(|e| internal("Microsoft links not dropped", e))?;
    }
    audit(
        &state,
        &claims,
        &team,
        format!(
            "Microsoft Entra tenant saved: {tid}{}; off until tested and turned on",
            if setup.domains.is_empty() {
                String::new()
            } else {
                format!(" for {}", setup.domains.join(", "))
            }
        ),
    )
    .await;
    Ok(Json(view(&state, &team).await?))
}

/// POST /api/sign-in/microsoft/test: the owner's Test sign-in link.
pub async fn test_link(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    owner(&claims)?;
    let team = setup_team(&state, &claims).await?;
    if load(&state, &team)
        .await
        .map_err(|e| internal("Microsoft setup not read", e))?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "Save the tenant first."));
    }
    let ticket = oidc::signed_ticket(&team, &claims.sub, TEST_AUD, &state.secrets.jwt_secret)
        .map_err(|e| internal("Test link not made", e))?;
    Ok(Json(json!({
        "test_url": format!("{}/auth/microsoft/test?ticket={ticket}", oidc::api(&state).trim_end_matches('/')),
    })))
}

#[derive(Deserialize)]
pub struct TicketQuery {
    #[serde(default)]
    pub ticket: String,
}

/// GET /auth/microsoft/test?ticket=...: only in the browser of the owner
/// who asked, from their own session.
pub async fn test_begin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<TicketQuery>,
) -> Response {
    let Some(t) = oidc::read_signed_ticket(&q.ticket, TEST_AUD, &state.secrets.jwt_secret) else {
        return test_redirect(&state, Err("expired"));
    };
    let session = oidc::session_of(&state, &headers).await.map(|(c, _)| c);
    if !oidc::ticket_is_theirs(&t, session.as_ref()) {
        return test_redirect(&state, Err("not_yours"));
    }
    let setup = match load(&state, t.team()).await {
        Ok(Some(s)) => s,
        _ => return test_redirect(&state, Err("no_setup")),
    };
    match start(&state, &setup, Mode::Test(t.by())) {
        Ok(r) => r,
        Err(_) => test_redirect(&state, Err("server")),
    }
}

#[derive(Deserialize)]
pub struct EnabledBody {
    pub enabled: bool,
}

/// PUT /api/sign-in/microsoft/enabled: turn the tenant on (the owner's own
/// Test sign-in within the hour; hosted: every domain proven in DNS now) or
/// off (refused while the team requires single sign-on with Microsoft).
pub async fn set_enabled(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    owner(&claims)?;
    let team = setup_team(&state, &claims).await?;
    let Some(setup) = load(&state, &team)
        .await
        .map_err(|e| internal("Microsoft setup not read", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "Save the tenant first."));
    };
    if body.enabled {
        may_turn_on(&claims, &setup, chrono::Utc::now())
            .map_err(|m| fail_code(StatusCode::CONFLICT, m, "test_first"))?;
        if !super::sign_in::applies()
            && !oidc::domains_proven_list(&state, &team, &setup.domains, true)
                .await
                .map_err(|e| internal("Domains not read", e))?
        {
            return Err(fail_code(
                StatusCode::CONFLICT,
                "Verify each email domain first: add its TXT record in DNS, then choose Verify domains.",
                "domains_not_verified",
            ));
        }
    }
    let next = EntraSetup {
        enabled: body.enabled,
        ..setup.clone()
    };
    // Conditioned on the setup being exactly the one checked (a save since
    // then is untested), and turning off on the rule not needing it.
    let mut put = Put::builder()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(next.to_item()))
        .expression_attribute_values(":tid", attr_s(&setup.tid));
    put = match &setup.updated_at {
        Some(ua) => put
            .condition_expression("tid = :tid AND updated_at = :ua")
            .expression_attribute_values(":ua", attr_s(ua)),
        None => put.condition_expression("tid = :tid AND attribute_not_exists(updated_at)"),
    };
    let put = put
        .build()
        .map_err(|e| internal("Microsoft setup not saved", e))?;
    let writes = vec![TransactWriteItem::builder().put(put).build()];
    if body.enabled {
        state
            .dynamo
            .transact_write_items()
            .set_transact_items(Some(writes))
            .send()
            .await
            .map_err(|_| {
                fail_code(
                    StatusCode::CONFLICT,
                    "The tenant changed meanwhile. Check it and test it again.",
                    "changed",
                )
            })?;
    } else {
        require_sso::while_rule_off(&state, &team, Some(SsoProvider::Microsoft), writes).await?;
    }
    if setup.enabled != body.enabled {
        audit(
            &state,
            &claims,
            &team,
            format!(
                "Microsoft Entra tenant {} turned {}",
                setup.tid,
                if body.enabled { "on" } else { "off" }
            ),
        )
        .await;
    }
    Ok(Json(view(&state, &team).await?))
}

/// DELETE /api/sign-in/microsoft: remove the tenant and every link to it.
/// Refused while the team requires single sign-on with Microsoft.
pub async fn delete_setup(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    owner(&claims)?;
    let team = setup_team(&state, &claims).await?;
    let del = Delete::builder()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&team))
        .key("sk", attr_s(SETUP_SK))
        .build()
        .map_err(|e| internal("Microsoft setup not removed", e))?;
    require_sso::while_rule_off(
        &state,
        &team,
        Some(SsoProvider::Microsoft),
        vec![TransactWriteItem::builder().delete(del).build()],
    )
    .await?;
    drop_links(&state, &team)
        .await
        .map_err(|e| internal("Microsoft links not dropped", e))?;
    audit(
        &state,
        &claims,
        &team,
        "Microsoft Entra tenant removed".into(),
    )
    .await;
    Ok(Json(view(&state, &team).await?))
}

// ── A member's own link ─────────────────────────────────────────────

/// GET /api/users/microsoft/link
pub async fn link_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    let team = setup_team(&state, &claims)
        .await
        .unwrap_or_else(|_| claims.team_id.clone());
    let live = load(&state, &team)
        .await
        .map_err(|e| internal("Microsoft setup not read", e))?;
    let me = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&team))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|e| internal("Member not read", e))?
        .item;
    let linked = me.as_ref().is_some_and(|m| linked_to(m, live.as_ref()));
    Ok(Json(json!({
        "available": live.as_ref().is_some_and(|l| l.enabled),
        "tenant_id": live.as_ref().map(|l| l.tid.clone()),
        "linked": linked,
        "linked_at": me.as_ref().filter(|_| linked).and_then(|m| opt(m, "ms_linked_at")),
    })))
}

/// POST /api/users/microsoft/link: the link that links the caller's own
/// account in the team's tenant to their member record.
pub async fn link_start(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    if claims.support_view.is_some() {
        return Err(fail(StatusCode::FORBIDDEN, "Not from a support view."));
    }
    let team = setup_team(&state, &claims).await?;
    if team != claims.team_id
        || !load(&state, &team)
            .await
            .map_err(|e| internal("Microsoft setup not read", e))?
            .is_some_and(|l| l.enabled)
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "The team's Microsoft tenant isn't on.",
        ));
    }
    let ticket = oidc::signed_ticket(&team, &claims.sub, LINK_AUD, &state.secrets.jwt_secret)
        .map_err(|e| internal("Link not made", e))?;
    Ok(Json(json!({
        "url": format!("{}/auth/microsoft/link?ticket={ticket}", oidc::api(&state).trim_end_matches('/')),
    })))
}

/// DELETE /api/users/microsoft/link: refused while the team requires single
/// sign-on with Microsoft (the member couldn't sign in again).
pub async fn link_remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    let me = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|e| internal("Member not read", e))?
        .item
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "You aren't on this team."))?;
    let rule = super::sign_in::team_rule(&state, &claims.team_id)
        .await
        .map_err(|e| internal("Sign-in rules not read", e))?;
    if rule.require_sso == Some(SsoProvider::Microsoft) {
        return Err(fail_code(
            StatusCode::CONFLICT,
            "Your team requires single sign-on with Microsoft, so you can't unlink it: you couldn't sign in again.",
            "sso_required_on",
        ));
    }
    unlink(&state, &claims.team_id, &me)
        .await
        .map_err(|e| internal("Link not removed", e))?;
    Ok(Json(json!({ "linked": false })))
}

/// GET /auth/microsoft/link?ticket=...: only in the browser of the member
/// who asked, from their own session.
pub async fn link_begin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<TicketQuery>,
) -> Response {
    let Some(t) = oidc::read_signed_ticket(&q.ticket, LINK_AUD, &state.secrets.jwt_secret) else {
        return link_redirect(&state, Err("expired"));
    };
    let session = oidc::session_of(&state, &headers).await.map(|(c, _)| c);
    if !oidc::ticket_is_theirs(&t, session.as_ref()) {
        return link_redirect(&state, Err("not_yours"));
    }
    let setup = match load(&state, t.team()).await {
        Ok(Some(s)) if s.enabled => s,
        _ => return link_redirect(&state, Err("off")),
    };
    match start(&state, &setup, Mode::Link(t.by())) {
        Ok(r) => r,
        Err(_) => link_redirect(&state, Err("server")),
    }
}

async fn link_return(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
    setup: &EntraSetup,
) -> Response {
    let claims = match tenant_claims(state, cookies, params, flow, &setup.tid).await {
        Ok(c) => c,
        Err(f) => {
            return link_redirect(
                state,
                Err(match (f.step, f.reason) {
                    (_, Reason::Cancelled) => "cancelled",
                    (_, Reason::Expired) => "expired",
                    ("tenant", _) => "other_tenant",
                    _ => "provider",
                }),
            )
        }
    };
    let (tid, oid) = match tenant_of(&claims, &setup.tid) {
        Ok(x) => x,
        Err(_) => return link_redirect(state, Err("other_tenant")),
    };
    let members = match super::sign_in::team_members(state, &flow.team).await {
        Ok(m) => m,
        Err(_) => return link_redirect(state, Err("server")),
    };
    match linked_record(&members, &tid, &oid) {
        Some(m) if s(m, "sk") == flow.link => return link_redirect(state, Ok(())),
        Some(_) => return link_redirect(state, Err("taken")),
        None => {}
    }
    let previous = members
        .iter()
        .find(|m| s(m, "sk") == flow.link)
        .map(|m| (s(m, "ms_tid").to_string(), s(m, "ms_oid").to_string()))
        .filter(|(t, o)| !t.is_empty() && !o.is_empty());
    match link_transaction(state, &flow.team, &flow.link, &tid, &oid, previous).await {
        Ok(()) => {
            info!(team_id = %flow.team, "Microsoft account linked by its member");
            link_redirect(state, Ok(()))
        }
        Err(e) if e.contains("ConditionalCheckFailed") || e.contains("TransactionCanceled") => {
            link_redirect(state, Err("taken"))
        }
        Err(e) => {
            warn!(error = %e, "Microsoft link not saved");
            link_redirect(state, Err("server"))
        }
    }
}

/// One transaction: the uniqueness row (only if nobody holds it), the
/// member's own active record, and a previous link's row removed (which
/// also ends the previous account's sessions).
pub(crate) async fn link_transaction(
    state: &AppState,
    team: &str,
    member: &str,
    tid: &str,
    oid: &str,
    previous: Option<(String, String)>,
) -> Result<(), String> {
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let (row_pk, row_sk) = link_row(team, tid, oid);
    let mut update = Update::builder()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(member))
        .update_expression(if previous.is_some() {
            "SET ms_tid = :t, ms_oid = :o, ms_linked_at = :at, ms_valid_after = :n"
        } else {
            "SET ms_tid = :t, ms_oid = :o, ms_linked_at = :at"
        })
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":t", attr_s(tid))
        .expression_attribute_values(":o", attr_s(oid))
        .expression_attribute_values(":at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":inv", attr_s("invited"));
    // The member's link is still the one read: a link made meanwhile (in
    // another tab) is never overwritten without its row being removed.
    update = match &previous {
        Some((_, prev_oid)) => update
            .condition_expression(
                "attribute_exists(pk) AND (attribute_not_exists(#st) OR #st <> :inv) AND ms_oid = :prev",
            )
            .expression_attribute_values(":prev", attr_s(prev_oid))
            .expression_attribute_values(":n", AttributeValue::N(now_secs().to_string())),
        None => update.condition_expression(
            "attribute_exists(pk) AND (attribute_not_exists(#st) OR #st <> :inv) AND attribute_not_exists(ms_oid)",
        ),
    };
    let mut items = vec![
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(&state.config.settings_table_name)
                    .item("pk", attr_s(&row_pk))
                    .item("sk", attr_s(&row_sk))
                    .item("member", attr_s(member))
                    .condition_expression("attribute_not_exists(pk)")
                    .build()
                    .map_err(e)?,
            )
            .build(),
        TransactWriteItem::builder()
            .update(update.build().map_err(e)?)
            .build(),
    ];
    if let Some((pt, po)) = &previous {
        let (ppk, psk) = link_row(team, pt, po);
        items.push(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(&state.config.settings_table_name)
                        .key("pk", attr_s(&ppk))
                        .key("sk", attr_s(&psk))
                        .build()
                        .map_err(e)?,
                )
                .build(),
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), attr_s(v)))
            .collect()
    }

    const TID: &str = "11111111-2222-3333-4444-555555555555";

    fn setup(enabled: bool) -> EntraSetup {
        EntraSetup {
            team_id: "TEAM#a".into(),
            tid: TID.into(),
            enabled,
            ..Default::default()
        }
    }

    #[test]
    fn a_tenant_is_a_guid_and_never_personal_accounts() {
        assert_eq!(clean_tid(&TID.to_uppercase()), Ok(TID.to_string()));
        assert!(clean_tid(CONSUMER_TENANT).is_err());
        assert!(clean_tid("common").is_err());
        assert!(clean_tid("organizations").is_err());
        assert!(clean_tid("").is_err());
    }

    #[test]
    fn only_the_teams_tenant_counts() {
        let ok = json!({ "tid": TID, "oid": "o1" });
        assert_eq!(
            tenant_of(&ok, TID).unwrap(),
            (TID.to_string(), "o1".to_string())
        );
        assert!(tenant_of(
            &json!({ "tid": "99999999-2222-3333-4444-555555555555", "oid": "o1" }),
            TID
        )
        .is_err());
        assert!(tenant_of(
            &json!({ "tid": CONSUMER_TENANT, "oid": "o1" }),
            CONSUMER_TENANT
        )
        .is_err());
        assert!(tenant_of(&json!({ "tid": TID }), TID).is_err());
        assert_eq!(
            tenant_issuer(TID),
            format!("https://login.microsoftonline.com/{TID}/v2.0")
        );
    }

    #[test]
    fn guests_in_the_tenant_never_count() {
        let member = json!({ "tid": TID, "oid": "o1" });
        assert!(!is_guest(&member, TID));
        let own_idp =
            json!({ "tid": TID, "oid": "o1", "idp": format!("https://sts.windows.net/{TID}/") });
        assert!(!is_guest(&own_idp, TID));
        for g in [
            json!({ "tid": TID, "oid": "o1", "idp": "https://sts.windows.net/99999999-2222-3333-4444-555555555555/" }),
            json!({ "tid": TID, "oid": "o1", "idp": "live.com" }),
            json!({ "tid": TID, "oid": "o1", "acct": 1 }),
            json!({ "tid": TID, "oid": "o1", "acct": "1" }),
        ] {
            assert!(is_guest(&g, TID), "{g}");
            assert!(tenant_of(&g, TID).is_err(), "{g}");
        }
        assert!(tenant_of(&json!({ "tid": TID, "oid": "o1", "acct": 0 }), TID).is_ok());
    }

    #[test]
    fn a_session_stands_only_while_its_link_and_the_setup_match() {
        let m = item(&[("sk", "USER#m"), ("ms_tid", TID), ("ms_oid", "o1")]);
        let id = entra_id(TID, "o1");
        let on = setup(true);
        assert!(session_state(Some(&m), Some(&on), 10, Some(&id)).is_ok());
        assert!(session_state(Some(&m), Some(&setup(false)), 10, Some(&id)).is_err());
        assert!(session_state(Some(&m), None, 10, Some(&id)).is_err());
        assert!(session_state(Some(&m), Some(&on), 10, None).is_err());
        assert!(session_state(Some(&m), Some(&on), 10, Some(&entra_id(TID, "o2"))).is_err());
        let other = entra_id("99999999-2222-3333-4444-555555555555", "o1");
        assert!(session_state(Some(&m), Some(&on), 10, Some(&other)).is_err());
        let mut later = m.clone();
        later.insert("ms_valid_after".into(), AttributeValue::N("11".into()));
        assert!(session_state(Some(&later), Some(&on), 10, Some(&id)).is_err());
        assert!(session_state(None, Some(&on), 10, Some(&id)).is_err());
    }

    #[test]
    fn a_member_id_carries_a_tenant_hash() {
        let a = ms_user_id(TID, "o1");
        let b = ms_user_id("99999999-2222-3333-4444-555555555555", "o1");
        assert!(a.starts_with("USER#microsoft_") && a.ends_with("_o1"));
        assert_ne!(a, b);
        assert!(!a.contains(TID));
    }

    #[test]
    fn turning_on_needs_the_owners_own_fresh_test() {
        let now = chrono::Utc::now();
        let mut c = crate::models::Claims {
            sub: "USER#o".into(),
            team_id: "TEAM#a".into(),
            email: "o@example.com".into(),
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
        };
        let mut s = setup(false);
        assert!(may_turn_on(&c, &s, now).is_err(), "untested");
        s.tested_by = Some("USER#o".into());
        s.tested_oid = Some("o1".into());
        s.tested_at = Some(now.to_rfc3339());
        assert!(may_turn_on(&c, &s, now).is_ok());
        let mut old = s.clone();
        old.tested_at = Some((now - chrono::Duration::hours(2)).to_rfc3339());
        assert!(may_turn_on(&c, &old, now).is_err(), "stale");
        let mut theirs = s.clone();
        theirs.tested_by = Some("USER#x".into());
        assert!(
            may_turn_on(&c, &theirs, now).is_err(),
            "someone else's test"
        );
        c.role = "admin".into();
        assert!(may_turn_on(&c, &s, now).is_err(), "not the owner");
    }
}
