//! Sign in with Microsoft and with Okta: OpenID Connect with PKCE, run from
//! the gateway itself, the way GitHub sign-in is (no Cognito in between).
//!
//! The trip:
//! 1. `/auth/microsoft` (or `POST /auth/okta` with the person's work email)
//!    makes a CSRF state, a nonce and a PKCE verifier. The state goes in the
//!    provider's state cookie (oauth_signin.rs), the nonce and verifier in a
//!    second cookie, signed with the session key so nobody can write their
//!    own, and the browser goes to the provider with the state, the nonce and
//!    the S256 code challenge.
//! 2. The callback checks the state cookie (oauth_signin::check_callback),
//!    exchanges the code with the verifier and the client secret, and checks
//!    the id_token: its RS256 signature against the provider's published
//!    keys, the issuer, the audience (our client id), the nonce, and its
//!    expiry. Anything else lands back on the sign-in screen with a
//!    `signin_error` code.
//! 3. Microsoft: the email comes only from a claim Microsoft vouches for
//!    (below), and the sign-in goes through the same gates as Google and
//!    GitHub (auth::finish_oidc): decide_join on a self-hosted install, the
//!    person's member records by email, and their second factor.
//! 4. Okta: the sign-in lands only on the member record that linked exactly
//!    this Okta identity (issuer and subject), never by email. Then the
//!    member's second factor, as after every way in.
//!
//! # Which email is trusted
//!
//! - **Microsoft** (work, school and personal accounts): the `email` claim,
//!   only when `xms_edov` is true in the same token. Microsoft's optional
//!   claims reference says `email` "isn't guaranteed to be correct", and
//!   defines `xms_edov` as "whether the user's email domain owner has been
//!   verified". `preferred_username` and `verified_primary_email` are never
//!   used (the reference only says the latter is "Sourced from the user's
//!   PrimaryAuthoritativeEmail"). The docs don't say a personal account's
//!   `email` is verified, so personal accounts follow the same rule. The app
//!   registration must list the optional ID token claims `email` and
//!   `xms_edov` (docs/self-host/microsoft-okta-sign-in.md).
//! - **Okta**: the `email` claim with `email_verified: true` is only used
//!   to find the Okta and, on hosted, to check the proven domain; it never
//!   picks the member.
//!
//! # Okta is per organization
//!
//! There is no global Okta client. Each hosted team (and a self-hosted
//! install's one organization) stores its own issuer, client id and client
//! secret (sealed with the deployment's KMS key, like a two-factor secret),
//! in the settings table at `pk = <team>`, `sk = SETTINGS#OKTA`. An admin
//! saves a draft (`SETTINGS#OKTA_DRAFT`), runs **Test sign-in** against it,
//! and only a draft whose test passed becomes the live setup (self-hosted:
//! only the owner saves it or turns it on). While a team's Okta sign-in is
//! on, a row `pk = OKTA_TEAMS`, `sk = <team>` tells `/auth/methods` to show
//! the button.
//!
//! # Linking (both kinds of install)
//!
//! An Okta identity signs in as a member only after that member linked it
//! themselves: **Link Okta** in their own Settings, Security, from a session
//! they started another way, which runs Okta and stores the issuer and
//! subject on their member record (`okta_issuer`, `okta_sub`). Until then
//! Okta sign-in says to sign in another way and link Okta. Every link on a
//! team is dropped when its Okta issuer changes or Okta is removed.
//! One identity links to one member (an `OKTA_LINK#` row written in the
//! same transaction). A session Okta signed in ends at its next request when
//! the link goes, the issuer changes or Okta is off (`okta_valid_after` and
//! the check in sign_in::check_member_session), and it is never a platform
//! admin on any install. Only the owner saves Okta or turns it on, naming
//! the draft they reviewed.
//!
//! # Hosted CoderHelm
//!
//! - Each email domain is proven with a DNS TXT record
//!   (`_coderhelm-okta.<domain>`, an HMAC of the team and domain under a
//!   subkey of the session key). A domain belongs to one team. The record is
//!   read again when Okta is turned on, and at sign-in once a proof is a day
//!   old; a definite answer without the record releases the domain (a DNS
//!   read with no clear answer keeps it; CNAMEs are followed), and so do dropping it
//!   from the setup, removing Okta, or the admin's Release.
//! - An Okta session is held to its team (`Claims::held`): no switching
//!   teams, nothing person-wide (password account, second factor, linking
//!   GitHub, ending other sessions), never a platform admin. Someone on no
//!   other team may still set up their second factor from it. A
//!   self-hosted install has one organization, so its Okta sessions are
//!   not held (they are still never platform admins).
//! - Every request to an identity provider goes through `GuardedHttps`, and
//!   discovery and keys never follow a redirect.

use axum::{
    extract::{Extension, Form, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64URL, Engine};
use jsonwebtoken::jwk::JwkSet;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::oauth_signin::{self, CallbackQuery, Provider, Reason, SignInFailure};
use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;
type ApiError = (StatusCode, Json<Value>);

/// Microsoft's sign-in service.
pub const MS_AUTHORITY: &str = "https://login.microsoftonline.com";
/// The tenant of personal Microsoft accounts (outlook.com, hotmail.com).
#[cfg(test)]
pub const MS_CONSUMER_TENANT: &str = "9188040d-6c67-4c5b-b112-36a304b66dad";
/// Where Microsoft publishes the keys that sign every tenant's tokens.
pub const MS_KEYS: &str = "https://login.microsoftonline.com/common/discovery/v2.0/keys";

/// Clock skew allowed on `exp`, `nbf` and `iat`, in seconds.
const LEEWAY_SECS: u64 = 120;
/// How long the signed flow cookie (nonce and verifier) is good for: the
/// same as the state cookie.
const FLOW_SECS: u64 = oauth_signin::STATE_MAX_AGE_SECS as u64;
/// How long an admin's Test sign-in link works.
const TEST_TICKET_SECS: u64 = 600;

pub const OKTA_SK: &str = "SETTINGS#OKTA";
pub const OKTA_DRAFT_SK: &str = "SETTINGS#OKTA_DRAFT";
pub const OKTA_TEAMS_PK: &str = "OKTA_TEAMS";

// ── PKCE, nonce and the flow cookie ─────────────────────────────────

/// 32 random bytes, base64url: a state, a nonce or a PKCE verifier (43
/// characters, inside RFC 7636's 43 to 128).
pub fn random_token() -> String {
    use rand::RngCore;
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    B64URL.encode(b)
}

/// RFC 7636 S256: base64url(SHA-256(verifier)).
pub fn pkce_challenge(verifier: &str) -> String {
    B64URL.encode(Sha256::digest(verifier.as_bytes()))
}

/// What the callback needs from the start of the trip, signed so it can't
/// be written by hand.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Flow {
    pub nonce: String,
    pub verifier: String,
    /// Okta: the team whose setup this trip uses.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub team: String,
    /// Okta: an admin's Test sign-in of the draft, not a sign-in.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub test: bool,
    /// Okta: a member linking their Okta identity to their own record (that
    /// member's id), not a sign-in.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub link: String,
    /// Okta Test sign-in: the admin who started it (recorded as `tested_by`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
    /// The provider this flow is for, so one provider's cookie never
    /// finishes another's trip.
    pub aud: String,
    pub exp: u64,
}

impl Flow {
    pub fn new(provider: Provider, team: &str, test: bool) -> Flow {
        Flow {
            nonce: random_token(),
            verifier: random_token(),
            team: team.to_string(),
            test,
            link: String::new(),
            by: String::new(),
            aud: format!("oidc_flow_{}", provider.slug()),
            exp: chrono::Utc::now().timestamp() as u64 + FLOW_SECS,
        }
    }
}

pub fn flow_cookie(provider: Provider, flow: &Flow, secret: &str) -> Result<String, String> {
    let name = provider.flow_cookie().ok_or("no flow cookie")?;
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        flow,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| e.to_string())?;
    Ok(format!(
        "{name}={token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age={FLOW_SECS}"
    ))
}

/// The flow this browser started for `provider`, when its cookie is there,
/// signed by us, for this provider and not expired.
pub fn read_flow(provider: Provider, cookie_header: &str, secret: &str) -> Option<Flow> {
    let token = oauth_signin::cookie_value(cookie_header, provider.flow_cookie()?)?;
    let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    v.set_audience(&[format!("oidc_flow_{}", provider.slug())]);
    v.set_required_spec_claims(&["exp", "aud"]);
    v.leeway = 0;
    jsonwebtoken::decode::<Flow>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &v,
    )
    .ok()
    .map(|d| d.claims)
}

// ── The id_token ────────────────────────────────────────────────────

/// Whose tokens these are.
#[derive(Debug, Clone, Copy)]
pub enum Issuer<'a> {
    /// Exactly this issuer (Okta: the setup's issuer).
    Exact(&'a str),
    /// Microsoft's multi-tenant endpoint: the issuer names the token's own
    /// tenant, `https://login.microsoftonline.com/{tid}/v2.0`, and the
    /// tenant must be a GUID.
    MicrosoftTenant,
}

pub struct Expect<'a> {
    pub issuer: Issuer<'a>,
    pub client_id: &'a str,
    pub nonce: &'a str,
    /// Unix seconds.
    pub now: u64,
}

/// A tenant id: a GUID, lowercase or not.
pub fn is_guid(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 5
        && parts
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(p, n)| p.len() == n && p.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Checks an id_token: RS256, signed by one of `keys` (by `kid`), for our
/// client id, from the expected issuer, with our nonce, not expired, not
/// from the future. Returns its claims.
pub fn verify_id_token(token: &str, keys: &JwkSet, want: &Expect) -> Result<Value, String> {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    let header = decode_header(token).map_err(|e| format!("header: {e}"))?;
    if header.alg != Algorithm::RS256 {
        return Err(format!("algorithm {:?} is not RS256", header.alg));
    }
    let kid = header.kid.ok_or("no key id")?;
    let jwk = keys
        .find(&kid)
        .ok_or("signed with a key the provider doesn't publish")?;
    if jwk
        .common
        .public_key_use
        .as_ref()
        .is_some_and(|u| *u != jsonwebtoken::jwk::PublicKeyUse::Signature)
    {
        return Err("key is not for signatures".into());
    }
    let key = DecodingKey::from_jwk(jwk).map_err(|e| format!("key: {e}"))?;
    let mut v = Validation::new(Algorithm::RS256);
    v.set_audience(&[want.client_id]);
    v.set_required_spec_claims(&["exp", "iat", "aud", "iss", "sub"]);
    v.validate_nbf = true;
    v.leeway = LEEWAY_SECS;
    let claims = decode::<Value>(token, &key, &v)
        .map_err(|e| format!("token: {e}"))?
        .claims;
    let iss = claims["iss"].as_str().unwrap_or_default();
    match want.issuer {
        Issuer::Exact(expected) => {
            if iss != expected {
                return Err(format!("issuer {iss} is not {expected}"));
            }
        }
        Issuer::MicrosoftTenant => {
            let tid = claims["tid"].as_str().unwrap_or_default();
            if !is_guid(tid) || iss != format!("{MS_AUTHORITY}/{tid}/v2.0") {
                return Err(format!("issuer {iss} is not the token's tenant"));
            }
        }
    }
    // More than one audience: the token must have been issued to us.
    if claims["aud"].is_array() && claims["azp"].as_str() != Some(want.client_id) {
        return Err("several audiences and azp is not this client".into());
    }
    if want.nonce.is_empty() || claims["nonce"].as_str() != Some(want.nonce) {
        return Err("nonce does not match this browser's sign-in".into());
    }
    match claims["iat"].as_u64() {
        Some(iat) if iat <= want.now + LEEWAY_SECS => {}
        _ => return Err("issued in the future".into()),
    }
    Ok(claims)
}

/// An email address worth looking up: one `@`, something before it, a
/// dotted domain after it, no spaces.
pub fn plain_email(v: &str) -> Option<String> {
    let v = v.trim();
    let (local, domain) = v.split_once('@')?;
    (!local.is_empty()
        && domain.contains('.')
        && !domain.contains('@')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !v.chars().any(char::is_whitespace))
    .then(|| v.to_lowercase())
}

fn truthy(v: &Value) -> bool {
    v.as_bool() == Some(true) || v.as_str() == Some("true")
}

/// The email a Microsoft token vouches for (see the module docs), or why
/// there is none: the `email` claim, only when `xms_edov` is true in the
/// same token, for every kind of account.
pub fn microsoft_email(claims: &Value) -> Result<String, &'static str> {
    let email = claims["email"].as_str().and_then(plain_email);
    if !truthy(&claims["xms_edov"]) {
        return Err("xms_edov is not true: Microsoft didn't vouch for the email's domain");
    }
    email.ok_or("no usable email claim")
}

/// The email an Okta token vouches for: `email` with `email_verified: true`.
pub fn okta_email(claims: &Value) -> Result<String, &'static str> {
    if claims["email_verified"].as_bool() != Some(true) {
        return Err("email_verified is not true");
    }
    claims["email"]
        .as_str()
        .and_then(plain_email)
        .ok_or("no email claim")
}

/// Who signed in, as the rest of sign-in sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub email: String,
    pub name: String,
    /// The member id to give them if they have no record yet.
    pub new_user_id: String,
}

/// A Microsoft account's member id: its tenant and object id, which never
/// change (the `sub` claim differs per app).
pub fn microsoft_person(claims: &Value) -> Result<Person, &'static str> {
    let email = microsoft_email(claims)?;
    let tid = claims["tid"].as_str().unwrap_or_default();
    let oid = claims["oid"]
        .as_str()
        .or_else(|| claims["sub"].as_str())
        .unwrap_or_default();
    Ok(Person {
        email,
        name: claims["name"].as_str().unwrap_or_default().to_string(),
        new_user_id: format!("USER#microsoft_{}_{}", tid.to_lowercase(), oid),
    })
}

/// The member id an Okta identity's new record gets (an invite accepted
/// through Okta): its issuer (a short hash) and subject, as a Microsoft id
/// carries its tenant, so two Okta organizations' subjects never meet.
pub fn okta_user_id(issuer: &str, sub: &str) -> String {
    let h = Sha256::digest(issuer.trim_end_matches('/').as_bytes());
    let short: String = h.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("USER#okta_{short}_{sub}")
}

pub fn okta_person(claims: &Value) -> Result<Person, &'static str> {
    let email = okta_email(claims)?;
    Ok(Person {
        email,
        name: claims["name"].as_str().unwrap_or_default().to_string(),
        new_user_id: okta_user_id(
            claims["iss"].as_str().unwrap_or_default(),
            claims["sub"].as_str().unwrap_or_default(),
        ),
    })
}

// ── Okta issuers and discovery ──────────────────────────────────────

/// An Okta issuer as an admin may type it (`acme.okta.com`,
/// `https://acme.okta.com`, `https://acme.okta.com/oauth2/default`, or the
/// organization's own Okta domain): https, a host name (no IP address, no
/// localhost, no port, no user, no query), no trailing slash.
pub fn clean_issuer(input: &str) -> Result<String, &'static str> {
    const BAD: &str = "Enter your Okta domain, such as acme.okta.com, or its issuer, such as https://acme.okta.com/oauth2/default.";
    let raw = input.trim().trim_end_matches('/');
    if raw.is_empty() {
        return Err(BAD);
    }
    let with_scheme = if raw.contains("://") {
        raw.to_string()
    } else {
        format!("https://{raw}")
    };
    let u = url::Url::parse(&with_scheme).map_err(|_| BAD)?;
    if u.scheme() != "https" {
        return Err("The Okta issuer must start with https://.");
    }
    let host = match u.host() {
        Some(url::Host::Domain(h)) => h.to_lowercase(),
        _ => return Err(BAD),
    };
    if !host.contains('.')
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host == "localhost"
    {
        return Err(BAD);
    }
    if u.port().is_some()
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
    {
        return Err(BAD);
    }
    let path = u.path().trim_end_matches('/');
    Ok(format!("https://{host}{path}"))
}

/// The endpoints an issuer's discovery document names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovery {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub jwks_uri: String,
}

/// Reads `{issuer}/.well-known/openid-configuration`: its issuer must be the
/// configured one exactly, and every endpoint https on the issuer's own host
/// (Okta serves them all there), so a setup can never point CoderHelm's
/// requests anywhere else.
pub fn check_discovery(issuer: &str, doc: &Value) -> Result<Discovery, String> {
    if doc["issuer"].as_str() != Some(issuer) {
        return Err(format!(
            "the discovery document's issuer is {}, not {issuer}",
            doc["issuer"].as_str().unwrap_or("missing")
        ));
    }
    let host = url::Url::parse(issuer)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .ok_or("issuer has no host")?;
    let endpoint = |k: &str| -> Result<String, String> {
        let v = doc[k].as_str().ok_or(format!("no {k}"))?;
        let u = url::Url::parse(v).map_err(|_| format!("{k} is not an address"))?;
        if u.scheme() != "https" || u.host_str() != Some(host.as_str()) || u.port().is_some() {
            return Err(format!("{k} is not https on {host}"));
        }
        Ok(v.to_string())
    };
    Ok(Discovery {
        authorization_endpoint: endpoint("authorization_endpoint")?,
        token_endpoint: endpoint("token_endpoint")?,
        jwks_uri: endpoint("jwks_uri")?,
    })
}

/// Every request to an identity provider goes through CoderHelm's guarded
/// transport (`common::mcp_remote::http::GuardedHttps`): https only, the
/// host resolved first and refused when any answer is private, loopback,
/// link-local or a metadata address, the connection pinned to the address
/// that was checked (no DNS rebinding), no proxy, a time limit and a size
/// cap. A setup's issuer therefore never reaches anything inside the
/// network, whatever its name resolves to.
pub type Idp = dyn common::mcp_remote::http::Http;

pub fn idp() -> &'static Idp {
    &common::mcp_remote::http::GuardedHttps
}

async fn get_json(http: &Idp, url: &str) -> Result<Value, String> {
    use common::mcp_remote::http::HttpRequest;
    // Never follows a redirect: discovery and keys come from exactly the
    // address asked for (the keys from the issuer's own host).
    let resp = http
        .send(HttpRequest::get(url).no_redirects())
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if !(200..300).contains(&resp.status) {
        return Err(format!("{url}: HTTP {}", resp.status));
    }
    resp.json().ok_or_else(|| format!("{url}: not JSON"))
}

pub async fn discover(http: &Idp, issuer: &str) -> Result<Discovery, String> {
    let doc = get_json(http, &format!("{issuer}/.well-known/openid-configuration")).await?;
    check_discovery(issuer, &doc)
}

async fn keys(http: &Idp, jwks_uri: &str) -> Result<JwkSet, String> {
    serde_json::from_value(get_json(http, jwks_uri).await?).map_err(|e| format!("keys: {e}"))
}

/// How the client proves itself at the token endpoint.
pub enum ClientAuth {
    /// `client_secret_post` (Microsoft).
    Post,
    /// `client_secret_basic` (Okta's default for a Web app).
    Basic,
}

/// Trades the code for tokens. Returns the id_token; the provider's error
/// name (never the secret) on a refusal.
#[allow(clippy::too_many_arguments)]
pub async fn exchange(
    http: &Idp,
    token_endpoint: &str,
    client_id: &str,
    client_secret: &str,
    auth: ClientAuth,
    code: &str,
    redirect_uri: &str,
    verifier: &str,
) -> Result<String, String> {
    use common::mcp_remote::http::HttpRequest;
    let mut form = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", verifier),
        ("client_id", client_id),
    ];
    if let ClientAuth::Post = auth {
        form.push(("client_secret", client_secret));
    }
    let mut req = HttpRequest::form(token_endpoint, &form);
    if let ClientAuth::Basic = auth {
        // RFC 6749 2.3.1: both halves form-encoded, then Basic.
        let pair = format!(
            "{}:{}",
            urlencoding::encode(client_id),
            urlencoding::encode(client_secret)
        );
        req = req.header(
            "Authorization",
            &format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(pair)
            ),
        );
    }
    let resp = http
        .send(req)
        .await
        .map_err(|e| format!("token endpoint: {e}"))?;
    let body = resp.json().unwrap_or(Value::Null);
    match body["id_token"].as_str() {
        Some(t) if (200..300).contains(&resp.status) => Ok(t.to_string()),
        _ => Err(format!(
            "HTTP {}: {}",
            resp.status,
            body["error"].as_str().unwrap_or("no id_token")
        )),
    }
}

fn authorize_url(endpoint: &str, params: &[(&str, &str)]) -> String {
    let q: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencoding::encode(v)))
        .collect();
    format!("{endpoint}?{}", q.join("&"))
}

/// The redirect to the provider with both cookies set.
fn to_provider(provider: Provider, url: &str, state: &str, flow: &str) -> Response {
    let mut resp = (StatusCode::SEE_OTHER, [(header::LOCATION, url.to_string())]).into_response();
    for c in [
        oauth_signin::state_cookie(provider, state),
        flow.to_string(),
    ] {
        if let Ok(v) = HeaderValue::from_str(&c) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    resp
}

fn app(state: &AppState) -> &'static str {
    common::public_urls::app_url(&state.config.stage)
}

fn api(state: &AppState) -> &'static str {
    common::public_urls::api_url(&state.config.stage)
}

// ── Microsoft ───────────────────────────────────────────────────────

/// This deployment's own Entra app registration: its client id and secret,
/// from the deployment's secret (`microsoft_client_id`,
/// `microsoft_client_secret`).
pub fn microsoft_ready(client_id: Option<&str>, client_secret: Option<&str>) -> bool {
    client_id.is_some_and(|v| !v.trim().is_empty())
        && client_secret.is_some_and(|v| !v.trim().is_empty())
}

fn microsoft_client(state: &AppState) -> Option<(&str, &str)> {
    let id = state.secrets.microsoft_client_id.as_deref()?.trim();
    let secret = state.secrets.microsoft_client_secret.as_deref()?.trim();
    microsoft_ready(Some(id), Some(secret)).then_some((id, secret))
}

pub fn microsoft_redirect_uri(api_url: &str) -> String {
    format!("{}/auth/microsoft/callback", api_url.trim_end_matches('/'))
}

pub fn okta_redirect_uri(api_url: &str) -> String {
    format!("{}/auth/okta/callback", api_url.trim_end_matches('/'))
}

/// GET /auth/microsoft: to Microsoft's sign-in (work, school and personal
/// accounts), or back to the sign-in screen with `not_set_up` when this
/// deployment has no app registration.
pub async fn microsoft_login(State(state): State<Arc<AppState>>) -> Response {
    const P: Provider = Provider::Microsoft;
    let Some((client_id, _)) = microsoft_client(&state) else {
        return SignInFailure::new(
            P,
            Reason::NotSetUp,
            "start",
            "no microsoft_client_id and microsoft_client_secret in the deployment's secret",
        )
        .into_redirect(app(&state));
    };
    let flow = Flow::new(P, "", false);
    let cookie = match flow_cookie(P, &flow, &state.secrets.jwt_secret) {
        Ok(c) => c,
        Err(e) => {
            return SignInFailure::new(P, Reason::Server, "start", e).into_redirect(app(&state))
        }
    };
    let csrf = random_token();
    let redirect_uri = microsoft_redirect_uri(api(&state));
    let challenge = pkce_challenge(&flow.verifier);
    let url = authorize_url(
        &format!("{MS_AUTHORITY}/common/oauth2/v2.0/authorize"),
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
            ("prompt", "select_account"),
        ],
    );
    to_provider(P, &url, &csrf, &cookie)
}

/// GET /auth/microsoft/callback
pub async fn microsoft_callback(
    State(state): State<Arc<AppState>>,
    ip: crate::middleware::rate_limit::ClientIp,
    headers: HeaderMap,
    Query(params): Query<CallbackQuery>,
) -> Response {
    match microsoft_callback_inner(&state, &ip.0, &headers, &params).await {
        Ok(r) => r,
        Err(f) => f.into_redirect(app(&state)),
    }
}

async fn microsoft_callback_inner(
    state: &Arc<AppState>,
    ip: &str,
    headers: &HeaderMap,
    params: &CallbackQuery,
) -> Result<Response, SignInFailure> {
    const P: Provider = Provider::Microsoft;
    let Some((client_id, client_secret)) = microsoft_client(state) else {
        return Err(SignInFailure::new(
            P,
            Reason::NotSetUp,
            "callback",
            "no app registration",
        ));
    };
    let cookies = super::auth::cookie_header(headers);
    let code = oauth_signin::check_callback(P, cookies, params)?;
    let flow = read_flow(P, cookies, &state.secrets.jwt_secret).ok_or_else(|| {
        SignInFailure::new(P, Reason::Expired, "flow_cookie", "no signed flow cookie")
    })?;
    let id_token = exchange(
        idp(),
        &format!("{MS_AUTHORITY}/common/oauth2/v2.0/token"),
        client_id,
        client_secret,
        ClientAuth::Post,
        code,
        &microsoft_redirect_uri(api(state)),
        &flow.verifier,
    )
    .await
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "token_exchange", e))?;
    let jwks = keys(idp(), MS_KEYS)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "keys", e))?;
    let claims = verify_id_token(
        &id_token,
        &jwks,
        &Expect {
            issuer: Issuer::MicrosoftTenant,
            client_id,
            nonce: &flow.nonce,
            now: chrono::Utc::now().timestamp() as u64,
        },
    )
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "id_token", e))?;
    let person = microsoft_person(&claims)
        .map_err(|e| SignInFailure::new(P, Reason::NoVerifiedEmail, "email", e))?;
    super::auth::finish_oidc(state, ip, P, &person, None, false).await
}

// ── Okta: the setup ─────────────────────────────────────────────────

/// A team's (or the organization's) Okta setup, live or draft.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OktaSetup {
    pub team_id: String,
    pub issuer: String,
    pub client_id: String,
    /// Sealed with the deployment's KMS key, base64.
    pub secret_sealed: String,
    /// The email domains it signs people in for.
    pub domains: Vec<String>,
    pub enabled: bool,
    /// When a Test sign-in of exactly these values last passed, and as whom.
    pub tested_at: Option<String>,
    pub tested_email: Option<String>,
    /// The member who ran that Test sign-in.
    pub tested_by: Option<String>,
    pub updated_at: Option<String>,
    pub updated_by: Option<String>,
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

fn attr_s(v: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(v.to_string())
}

impl OktaSetup {
    pub fn from_item(item: &Item) -> Option<OktaSetup> {
        let issuer = s(item, "issuer");
        if issuer.is_empty() {
            return None;
        }
        Some(OktaSetup {
            team_id: s(item, "pk").to_string(),
            issuer: issuer.to_string(),
            client_id: s(item, "client_id").to_string(),
            secret_sealed: s(item, "client_secret_sealed").to_string(),
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
            tested_email: opt(item, "tested_email"),
            tested_by: opt(item, "tested_by"),
            updated_at: opt(item, "updated_at"),
            updated_by: opt(item, "updated_by"),
        })
    }

    fn to_item(&self, sk: &str) -> Item {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let mut item = HashMap::from([
            ("pk".to_string(), attr_s(&self.team_id)),
            ("sk".to_string(), attr_s(sk)),
            ("issuer".to_string(), attr_s(&self.issuer)),
            ("client_id".to_string(), attr_s(&self.client_id)),
            (
                "client_secret_sealed".to_string(),
                attr_s(&self.secret_sealed),
            ),
            (
                "domains".to_string(),
                A::L(self.domains.iter().map(|d| attr_s(d)).collect()),
            ),
            ("enabled".to_string(), A::Bool(self.enabled)),
        ]);
        for (k, v) in [
            ("tested_at", &self.tested_at),
            ("tested_email", &self.tested_email),
            ("tested_by", &self.tested_by),
            ("updated_at", &self.updated_at),
            ("updated_by", &self.updated_by),
        ] {
            if let Some(v) = v {
                item.insert(k.to_string(), attr_s(v));
            }
        }
        item
    }

    /// What an admin sees: never the secret, sealed or not.
    pub fn public(&self) -> Value {
        json!({
            "issuer": self.issuer,
            "client_id": self.client_id,
            "has_secret": !self.secret_sealed.is_empty(),
            "domains": self.domains,
            "enabled": self.enabled,
            "tested_at": self.tested_at,
            "tested_email": self.tested_email,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        })
    }
}

/// Whether an email's domain is one an Okta setup signs in for. A hosted
/// setup lists its domains; a self-hosted one may leave them out and use the
/// organization's allowed domains, and with neither it is the install's
/// only Okta, for every email.
pub fn okta_covers(
    setup: &OktaSetup,
    org_domains: &[String],
    self_hosted: bool,
    email: &str,
) -> bool {
    let Some(d) = super::sign_in::email_domain(email) else {
        return false;
    };
    if !setup.domains.is_empty() {
        return setup.domains.contains(&d);
    }
    if !self_hosted {
        return false;
    }
    org_domains.is_empty() || org_domains.contains(&d)
}

fn seal_context(team_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("purpose".to_string(), "okta_client_secret".to_string()),
    ])
}

fn cipher(state: &AppState) -> common::mcp_credentials::Kms<'_> {
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

async fn seal_secret(state: &AppState, team_id: &str, secret: &str) -> Result<String, String> {
    use common::mcp_credentials::Cipher;
    let blob = cipher(state)
        .encrypt(seal_context(team_id), secret.as_bytes())
        .await?;
    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

async fn unseal_secret(state: &AppState, setup: &OktaSetup) -> Result<String, String> {
    use common::mcp_credentials::Cipher;
    let blob = base64::engine::general_purpose::STANDARD
        .decode(&setup.secret_sealed)
        .map_err(|e| e.to_string())?;
    let plain = cipher(state)
        .decrypt(seal_context(&setup.team_id), &blob)
        .await?;
    String::from_utf8(plain).map_err(|e| e.to_string())
}

async fn load_setup(
    state: &AppState,
    team_id: &str,
    sk: &str,
) -> Result<Option<OktaSetup>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.item().and_then(OktaSetup::from_item))
}

async fn put_setup(state: &AppState, setup: &OktaSetup, sk: &str) -> Result<(), String> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(setup.to_item(sk)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

async fn delete_setup(state: &AppState, pk: &str, sk: &str) -> Result<(), String> {
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

/// The marker `/auth/methods` reads: on while the team's Okta sign-in is on.
async fn set_marker(state: &AppState, team_id: &str, on: bool) -> Result<(), String> {
    if on {
        state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", attr_s(OKTA_TEAMS_PK))
            .item("sk", attr_s(team_id))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e}"))
    } else {
        delete_setup(state, OKTA_TEAMS_PK, team_id).await
    }
}

/// Whether any team (hosted) or the organization (self-hosted) has Okta
/// sign-in on.
pub async fn okta_available(state: &AppState) -> bool {
    state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s(OKTA_TEAMS_PK))
        .limit(1)
        .send()
        .await
        .map(|r| r.count() > 0)
        .unwrap_or(false)
}

// ── Okta: proving a domain (hosted) ─────────────────────────────────

/// Where a verified domain is recorded: one team per domain (settings
/// table, `pk = OKTA_DOMAIN#<domain>`, `sk = OKTA`). Written only after the
/// domain's DNS shows the team's TXT record.
pub const OKTA_DOMAIN_PREFIX: &str = "OKTA_DOMAIN#";
pub const OKTA_DOMAIN_SK: &str = "OKTA";

/// The TXT record's name: `_coderhelm-okta.<domain>`.
pub fn domain_txt_name(domain: &str) -> String {
    format!("_coderhelm-okta.{domain}")
}

/// The TXT record's value for one team and one domain: an HMAC of both
/// with the deployment's key, so nobody else can work it out and it never
/// needs storing.
pub fn domain_txt_value(secret: &str, team_id: &str, domain: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&proof_key(secret))
        .expect("HMAC takes any key length");
    mac.update(format!("okta-domain|{team_id}|{domain}").as_bytes());
    let tag = hex::encode(mac.finalize().into_bytes());
    format!("coderhelm-okta={}", &tag[..32])
}

/// The key the domain proofs are made with: its own subkey of the
/// deployment's session key, never the session key itself.
fn proof_key(secret: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .expect("HMAC takes any key length");
    mac.update(b"coderhelm/okta-domain-proof/v1");
    mac.finalize().into_bytes().to_vec()
}

/// How long a domain proof is trusted before its TXT record is read again.
pub const PROOF_RECHECK_SECS: i64 = 24 * 3600;

/// Whether a proof last checked at `checked_at` (RFC 3339) must be read again.
pub fn proof_stale(checked_at: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(checked_at)
        .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() >= PROOF_RECHECK_SECS)
        .unwrap_or(true)
}

/// Whether any of a name's TXT strings is exactly the expected value.
pub fn txt_proves(records: &[String], expected: &str) -> bool {
    records
        .iter()
        .any(|r| r.trim().trim_matches('"') == expected)
}

/// The team that proved `domain`, if any, and when it was verified.
async fn domain_owner(state: &AppState, domain: &str) -> Result<Option<(String, String)>, String> {
    Ok(domain_record(state, domain)
        .await?
        .map(|(t, verified, _)| (t, verified)))
}

/// When a proof's TXT record was found gone while Okta sign-in is required
/// (the proof is kept, marked failing), if it was.
pub(crate) async fn domain_failing_since(
    state: &AppState,
    domain: &str,
) -> Result<Option<String>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
        .key("sk", attr_s(OKTA_DOMAIN_SK))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out.item().and_then(|i| opt(i, "failing_at")))
}

/// The proof's team, `verified_at` and `checked_at`.
async fn domain_record(
    state: &AppState,
    domain: &str,
) -> Result<Option<(String, String, String)>, String> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
        .key("sk", attr_s(OKTA_DOMAIN_SK))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(out
        .item()
        .map(|i| {
            let verified = s(i, "verified_at").to_string();
            let checked = opt(i, "checked_at").unwrap_or_else(|| verified.clone());
            (s(i, "team_id").to_string(), verified, checked)
        })
        .filter(|(t, _, _)| !t.is_empty()))
}

/// What a fresh read of a proof's TXT record says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofCheck {
    /// The record shows the team's value.
    There,
    /// A definite answer without it: no such name, or no matching record.
    Gone,
    /// No definite answer (a timeout, SERVFAIL, no resolver): the proof is kept.
    Unknown,
}

/// Reads a lookup's outcome: `Ok` is a definite answer (NXDOMAIN reads as
/// no records), `Err` none.
pub fn proof_check(lookup: &Result<Vec<String>, String>, expected: &str) -> ProofCheck {
    match lookup {
        Ok(records) if txt_proves(records, expected) => ProofCheck::There,
        Ok(_) => ProofCheck::Gone,
        Err(_) => ProofCheck::Unknown,
    }
}

/// Whether `domain`'s TXT record still shows `team`'s value right now.
async fn txt_check(state: &AppState, team: &str, domain: &str) -> ProofCheck {
    let expected = domain_txt_value(&state.secrets.jwt_secret, team, domain);
    proof_check(
        &common::dns_txt::lookup(&domain_txt_name(domain)).await,
        &expected,
    )
}

/// Releases `team`'s proof of `domain` (only its own).
async fn release_domain(state: &AppState, team: &str, domain: &str) -> Result<(), String> {
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
        .key("sk", attr_s(OKTA_DOMAIN_SK))
        .condition_expression("team_id = :t")
        .expression_attribute_values(":t", attr_s(team))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

/// The team that proved `domain`, reading its TXT record again when the
/// proof is over a day old (`recheck_now` reads it whatever its age). A
/// record that is gone releases the domain.
async fn fresh_domain_owner(
    state: &AppState,
    domain: &str,
    recheck_now: bool,
) -> Result<Option<String>, String> {
    let Some((team, _, checked)) = domain_record(state, domain).await? else {
        return Ok(None);
    };
    if !recheck_now && !proof_stale(&checked, chrono::Utc::now()) {
        return Ok(Some(team));
    }
    let check = txt_check(state, &team, domain).await;
    if check == ProofCheck::Unknown {
        // No definite answer: keep the proof and the team, read it next time.
        warn!(%domain, "Okta domain proof not read (no definite DNS answer); kept");
        return Ok(Some(team));
    }
    if check == ProofCheck::There {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
            .key("sk", attr_s(OKTA_DOMAIN_SK))
            .update_expression("SET checked_at = :now REMOVE failing_at")
            .condition_expression("team_id = :t")
            .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
            .expression_attribute_values(":t", attr_s(&team))
            .send()
            .await;
        return Ok(Some(team));
    }
    // While the team requires Okta sign-in, a record that is gone doesn't
    // release the domain at once (that would lock everyone out): the proof is
    // kept and marked failing for a grace period, and the owners are told.
    // After it, the proof goes (the domain's real owner may be another team).
    let rule_on = super::sign_in::team_rule(state, &team).await?.require_okta;
    let failing = domain_failing_since(state, domain).await?;
    match gone_outcome(rule_on, failing.as_deref(), chrono::Utc::now()) {
        GoneOutcome::Release => {}
        GoneOutcome::Keep => return Ok(Some(team)),
        GoneOutcome::GraceOver => {
            warn!(%domain, "Okta domain proof still gone after its grace period; released");
            let released = state
                .dynamo
                .delete_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
                .key("sk", attr_s(OKTA_DOMAIN_SK))
                .condition_expression("team_id = :t AND failing_at = :f")
                .expression_attribute_values(":t", attr_s(&team))
                .expression_attribute_values(":f", attr_s(failing.as_deref().unwrap_or("")))
                .send()
                .await
                .is_ok();
            if released {
                super::require_okta::domain_released(state, &team, domain).await;
            }
            return Ok(None);
        }
        GoneOutcome::MarkFailing => {}
    }
    if rule_on {
        let marked = state
            .dynamo
            .update_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
            .key("sk", attr_s(OKTA_DOMAIN_SK))
            .update_expression("SET failing_at = :now, checked_at = :now")
            .condition_expression("team_id = :t AND attribute_not_exists(failing_at)")
            .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
            .expression_attribute_values(":t", attr_s(&team))
            .send()
            .await
            .is_ok();
        super::require_okta::domain_failing(state, &team, domain, marked).await;
        return Ok(Some(team));
    }
    warn!(%domain, "Okta domain proof gone from DNS; released");
    // Only while the rule is still off (checked in the same write): if the
    // team turned it on meanwhile, the proof stays and is read again later.
    if release_domain_while_rule_off(state, &team, domain)
        .await
        .is_err()
    {
        return Ok(Some(team));
    }
    Ok(None)
}

/// How long a proof whose TXT record is gone is kept while its team
/// requires Okta sign-in, before it is released.
pub const FAILING_GRACE_DAYS: i64 = 7;

/// When a failing proof is released (its grace period ends).
pub fn grace_ends(failing_since: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(failing_since)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc) + chrono::Duration::days(FAILING_GRACE_DAYS))
}

/// What a definite "the record is gone" does to a proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoneOutcome {
    /// The rule is off: released now.
    Release,
    /// The rule is on and this is the first time: kept, marked failing,
    /// owners told.
    MarkFailing,
    /// Already failing, within its grace period: kept.
    Keep,
    /// Failing past its grace period: released, owners told.
    GraceOver,
}

/// Pure: see [`GoneOutcome`]. A failing time that can't be read counts as
/// past its grace (it can't hold a domain forever).
pub fn gone_outcome(
    rule_on: bool,
    failing_since: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> GoneOutcome {
    if !rule_on {
        return GoneOutcome::Release;
    }
    match failing_since {
        None => GoneOutcome::MarkFailing,
        Some(f) => match grace_ends(f) {
            Some(end) if now < end => GoneOutcome::Keep,
            _ => GoneOutcome::GraceOver,
        },
    }
}

/// Whether another team showing the matching TXT record takes a domain over
/// from its holder: only when the holder's own record is definitely gone
/// (a read with no clear answer keeps the holder). Pure.
pub fn contender_takes_over(holder: ProofCheck) -> bool {
    holder == ProofCheck::Gone
}

/// Releases `team`'s proof of `domain` only while the team doesn't require
/// Okta sign-in, checked in the same write.
async fn release_domain_while_rule_off(
    state: &AppState,
    team: &str,
    domain: &str,
) -> Result<(), ApiError> {
    use aws_sdk_dynamodb::types::{Delete, TransactWriteItem};
    let delete = Delete::builder()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
        .key("sk", attr_s(OKTA_DOMAIN_SK))
        .condition_expression("team_id = :t")
        .expression_attribute_values(":t", attr_s(team))
        .build()
        .map_err(|e| internal("Okta domain not released", e))?;
    super::require_okta::while_rule_off(
        state,
        team,
        vec![TransactWriteItem::builder().delete(delete).build()],
    )
    .await
}

/// Whether a fresh TXT read counts when Require Okta sign-in is turned on:
/// only a definite match (a read with no clear answer doesn't). Pure.
pub fn counts_at_turn_on(check: ProofCheck) -> bool {
    check == ProofCheck::There
}

/// Whether every domain of a hosted setup is proven by its own team right
/// now: its TXT record read again and matching (a DNS read with no clear
/// answer doesn't count), and no proof marked failing. Turning Require Okta
/// sign-in on needs this.
async fn domains_proven_now(state: &AppState, setup: &OktaSetup) -> Result<bool, String> {
    if setup.domains.is_empty() {
        return Ok(false);
    }
    for d in &setup.domains {
        let rec = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{d}")))
            .key("sk", attr_s(OKTA_DOMAIN_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("{e}"))?
            .item;
        let ours = rec
            .as_ref()
            .is_some_and(|i| s(i, "team_id") == setup.team_id && s(i, "failing_at").is_empty());
        if !ours || !counts_at_turn_on(txt_check(state, &setup.team_id, d).await) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether every domain of a hosted setup is proven by its own team, each
/// TXT record read again now.
async fn all_domains_proven(state: &AppState, setup: &OktaSetup) -> Result<bool, String> {
    if setup.domains.is_empty() {
        return Ok(false);
    }
    for d in &setup.domains {
        if fresh_domain_owner(state, d, true).await? != Some(setup.team_id.clone()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether every domain of a hosted setup is proven by its team: read again
/// in DNS now (`recheck`), else from the stored proofs (for showing).
pub(crate) async fn domains_proven(
    state: &AppState,
    setup: &OktaSetup,
    recheck: bool,
) -> Result<bool, String> {
    if recheck {
        return domains_proven_now(state, setup).await;
    }
    if setup.domains.is_empty() {
        return Ok(false);
    }
    for d in &setup.domains {
        if domain_owner(state, d).await?.map(|(t, _)| t) != Some(setup.team_id.clone())
            || domain_failing_since(state, d).await?.is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The live, turned-on Okta setup for `email`. Self-hosted: the
/// organization's, when it covers the email. Hosted: the setup of the team
/// that proved the email's domain in DNS, when it is on and lists that
/// domain. The answer depends only on the email's domain, never on whether
/// the person is on any team.
async fn okta_for_email(state: &AppState, email: &str) -> Result<Option<OktaSetup>, String> {
    if super::sign_in::applies() {
        let Some(org) = super::sign_in::load_org(state).await? else {
            return Ok(None);
        };
        return Ok(load_setup(state, &org.team_id, OKTA_SK)
            .await?
            .filter(|s| s.enabled && okta_covers(s, &org.domains, true, email)));
    }
    let Some(domain) = super::sign_in::email_domain(email) else {
        return Ok(None);
    };
    let Some(team) = fresh_domain_owner(state, &domain, false).await? else {
        return Ok(None);
    };
    Ok(load_setup(state, &team, OKTA_SK)
        .await?
        .filter(|s| s.enabled && okta_covers(s, &[], false, email)))
}

// ── Okta: sign-in ───────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct OktaStart {
    #[serde(default)]
    pub email: String,
}

/// POST /auth/okta (a form on the sign-in screen, field `email`): the team
/// whose Okta signs in that email, then to that Okta with the email as the
/// login hint. The email travels in the form body, never in a URL.
pub async fn okta_login(
    State(state): State<Arc<AppState>>,
    Form(body): Form<OktaStart>,
) -> Response {
    const P: Provider = Provider::Okta;
    let fail = |r: Reason, step: &'static str, d: String| {
        SignInFailure::new(P, r, step, d).into_redirect(app(&state))
    };
    let Some(email) = plain_email(&body.email) else {
        return fail(Reason::NoOkta, "email", "not an email address".into());
    };
    let setup = match okta_for_email(&state, &email).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return fail(
                Reason::NoOkta,
                "lookup",
                "no Okta setup covers that email".into(),
            )
        }
        Err(e) => return fail(Reason::Server, "lookup", e),
    };
    match start_okta(&state, &setup, false, Some(&email)).await {
        Ok(r) => r,
        Err(f) => f.into_redirect(app(&state)),
    }
}

/// What an Okta trip is for.
enum Mode<'a> {
    SignIn,
    /// An admin's Test sign-in (that admin's member id).
    Test(&'a str),
    /// A member linking their own Okta identity (their member id).
    Link(&'a str),
}

async fn start_okta(
    state: &AppState,
    setup: &OktaSetup,
    test: bool,
    login_hint: Option<&str>,
) -> Result<Response, SignInFailure> {
    let mode = if test { Mode::Test("") } else { Mode::SignIn };
    start_okta_flow(state, setup, mode, login_hint).await
}

async fn start_okta_flow(
    state: &AppState,
    setup: &OktaSetup,
    mode: Mode<'_>,
    login_hint: Option<&str>,
) -> Result<Response, SignInFailure> {
    const P: Provider = Provider::Okta;
    let d = discover(idp(), &setup.issuer)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "discovery", e))?;
    let test = !matches!(mode, Mode::SignIn);
    let mut flow = Flow::new(P, &setup.team_id, matches!(mode, Mode::Test(_)));
    match mode {
        Mode::Link(member) => flow.link = member.to_string(),
        Mode::Test(by) => flow.by = by.to_string(),
        Mode::SignIn => {}
    }
    let cookie = flow_cookie(P, &flow, &state.secrets.jwt_secret)
        .map_err(|e| SignInFailure::new(P, Reason::Server, "start", e))?;
    let csrf = random_token();
    let redirect_uri = okta_redirect_uri(api(state));
    let challenge = pkce_challenge(&flow.verifier);
    let mut params = vec![
        ("client_id", setup.client_id.as_str()),
        ("response_type", "code"),
        ("redirect_uri", redirect_uri.as_str()),
        ("scope", "openid email profile"),
        ("state", csrf.as_str()),
        ("nonce", flow.nonce.as_str()),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
    ];
    if let Some(h) = login_hint {
        params.push(("login_hint", h));
    }
    if test {
        // A test or a link asks for the password again, so it proves the
        // whole sign-in.
        params.push(("prompt", "login"));
    }
    Ok(to_provider(
        P,
        &authorize_url(&d.authorization_endpoint, &params),
        &csrf,
        &cookie,
    ))
}

/// GET /auth/okta/callback: a sign-in, or an admin's Test sign-in.
pub async fn okta_callback(
    State(state): State<Arc<AppState>>,
    ip: crate::middleware::rate_limit::ClientIp,
    headers: HeaderMap,
    Query(params): Query<CallbackQuery>,
) -> Response {
    const P: Provider = Provider::Okta;
    let cookies = super::auth::cookie_header(&headers);
    let flow = read_flow(P, cookies, &state.secrets.jwt_secret);
    if let Some(f) = flow.as_ref().filter(|f| f.test) {
        return okta_test_return(&state, cookies, &params, f).await;
    }
    if let Some(f) = flow.as_ref().filter(|f| !f.link.is_empty()) {
        return okta_link_return(&state, cookies, &params, f).await;
    }
    let Some(flow) = flow else {
        // The state check names what went wrong (cancelled, expired) first.
        let f = match oauth_signin::check_callback(P, cookies, &params) {
            Err(f) => f,
            Ok(_) => SignInFailure::new(P, Reason::Expired, "flow_cookie", "no signed flow cookie"),
        };
        return f.into_redirect(app(&state));
    };
    match okta_callback_inner(&state, &ip.0, cookies, &params, &flow).await {
        Ok(r) => r,
        Err(f) => f.into_redirect(app(&state)),
    }
}

/// The checked claims of an Okta trip: state, code exchange, keys, token.
async fn okta_claims(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
    setup: &OktaSetup,
) -> Result<Value, SignInFailure> {
    const P: Provider = Provider::Okta;
    let code = oauth_signin::check_callback(P, cookies, params)?;
    let secret = unseal_secret(state, setup)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "secret", e))?;
    let d = discover(idp(), &setup.issuer)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "discovery", e))?;
    let id_token = exchange(
        idp(),
        &d.token_endpoint,
        &setup.client_id,
        &secret,
        ClientAuth::Basic,
        code,
        &okta_redirect_uri(api(state)),
        &flow.verifier,
    )
    .await
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "token_exchange", e))?;
    let jwks = keys(idp(), &d.jwks_uri)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "keys", e))?;
    verify_id_token(
        &id_token,
        &jwks,
        &Expect {
            issuer: Issuer::Exact(&setup.issuer),
            client_id: &setup.client_id,
            nonce: &flow.nonce,
            now: chrono::Utc::now().timestamp() as u64,
        },
    )
    .map_err(|e| SignInFailure::new(P, Reason::Provider, "id_token", e))
}

async fn okta_callback_inner(
    state: &Arc<AppState>,
    ip: &str,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
) -> Result<Response, SignInFailure> {
    const P: Provider = Provider::Okta;
    let setup = load_setup(state, &flow.team, OKTA_SK)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "setup", e))?
        .filter(|s| s.enabled)
        .ok_or_else(|| {
            SignInFailure::new(
                P,
                Reason::NotSetUp,
                "setup",
                "Okta sign-in is off for this team",
            )
        })?;
    let claims = okta_claims(state, cookies, params, flow, &setup).await?;
    okta_land(state, ip, &setup, &claims).await
}

/// Where an Okta sign-in lands once Okta's id_token for `setup` is checked:
/// the covered email, (hosted) its proven domain, then the linked member or,
/// while the team requires Okta, an invite for exactly that email.
pub(crate) async fn okta_land(
    state: &Arc<AppState>,
    ip: &str,
    setup: &OktaSetup,
    claims: &Value,
) -> Result<Response, SignInFailure> {
    const P: Provider = Provider::Okta;
    let setup = setup.clone();
    let person = okta_person(claims)
        .map_err(|e| SignInFailure::new(P, Reason::NoVerifiedEmail, "email", e))?;
    let org_domains = if super::sign_in::applies() {
        super::sign_in::load_org(state)
            .await
            .ok()
            .flatten()
            .map(|o| o.domains)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    if !okta_covers(
        &setup,
        &org_domains,
        super::sign_in::applies(),
        &person.email,
    ) {
        return Err(SignInFailure::new(
            P,
            Reason::NotOnTeam,
            "domain",
            "Okta signed in an email at a domain this setup doesn't cover",
        ));
    }
    if !super::sign_in::applies() {
        // Hosted: the email's domain must be one this team proved in DNS,
        // so a team's Okta can only vouch for addresses its owner controls.
        let domain = super::sign_in::email_domain(&person.email).unwrap_or_default();
        let owner = fresh_domain_owner(state, &domain, false)
            .await
            .map_err(|e| SignInFailure::new(P, Reason::Server, "domain", e))?;
        if owner.as_deref() != Some(setup.team_id.as_str()) {
            return Err(SignInFailure::new(
                P,
                Reason::NotOnTeam,
                "domain",
                "the email's domain is not proven by this team",
            ));
        }
    }
    // The member who linked exactly this Okta identity (issuer and subject),
    // on both kinds of install. Never matched by email.
    let sub = claims["sub"].as_str().unwrap_or_default();
    let members = super::sign_in::team_members(state, &setup.team_id)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "members", e))?;
    if let Some(record) = linked_record(&members, &setup.issuer, sub) {
        return super::auth::finish_oidc(state, ip, P, &person, Some(record), false).await;
    }
    // No member linked this identity. While the team requires Okta sign-in,
    // an admin's invite for exactly Okta's verified email is accepted here
    // (routes::require_okta explains why that is safe); otherwise, and for
    // anyone already on the team, nothing is linked by email.
    let rule = super::sign_in::team_rule(state, &setup.team_id)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Server, "rule", e))?;
    if !rule.require_okta {
        return Err(SignInFailure::new(
            P,
            Reason::OktaNotLinked,
            "link",
            "no member has linked this Okta identity",
        ));
    }
    let ask_admin = |d: &str| SignInFailure::new(P, Reason::OktaAskAdmin, "invite", d);
    // Hosted: a proof kept failing (its TXT record is gone) still signs in
    // members who linked before, but accepts no new member.
    if !super::sign_in::applies() {
        let domain = super::sign_in::email_domain(&person.email).unwrap_or_default();
        if domain_failing_since(state, &domain)
            .await
            .map_err(|e| SignInFailure::new(P, Reason::Server, "domain", e))?
            .is_some()
        {
            return Err(ask_admin(
                "the email's domain proof is failing; no invite is accepted",
            ));
        }
    }
    let invite = match super::require_okta::invite_for(&members, &person.email, chrono::Utc::now())
    {
        super::require_okta::InviteFor::Accept(i) => i,
        super::require_okta::InviteFor::ExistingMember => {
            return Err(ask_admin("a member has this email but never linked Okta"))
        }
        super::require_okta::InviteFor::None => {
            return Err(ask_admin(
                "no member linked this identity and no invite is waiting",
            ))
        }
    };
    if sub.is_empty() {
        return Err(SignInFailure::new(
            P,
            Reason::Provider,
            "id_token",
            "no sub",
        ));
    }
    let record = super::require_okta::invited_member(
        &invite,
        &person.new_user_id,
        &person.name,
        &setup.issuer,
        sub,
        &chrono::Utc::now().to_rfc3339(),
    );
    if let Err(e) = super::require_okta::accept_invite(state, &record, &invite).await {
        // Another record already has this id, or another member holds this
        // identity, or the invite is gone: nothing was written.
        return Err(ask_admin(&e));
    }
    info!(team_id = %setup.team_id, "Invite accepted through Okta sign-in");
    super::auth::finish_oidc(state, ip, P, &person, Some(&record), true).await
}

// ── Okta: linking a member's own Okta identity ──────────────────────

/// The team's live Okta setup (or none).
pub async fn live_setup_of(state: &AppState, team: &str) -> Result<Option<OktaSetup>, String> {
    load_setup(state, team, OKTA_SK).await
}

/// Whether a session Okta signed in at `iat` still stands: Okta is on, the
/// member's link is to the live issuer, and the link (or an unlink or a
/// change since) is no newer than the session.
pub fn okta_session_state(
    member: Option<&Item>,
    live: Option<&OktaSetup>,
    iat: u64,
    okta_sub: Option<&str>,
) -> Result<(), super::sign_in::Ended> {
    use super::sign_in::Ended;
    let (Some(m), Some(l)) = (member, live) else {
        return Err(Ended::Revoked);
    };
    let after = m
        .get("okta_valid_after")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let linked = s(m, "okta_sub");
    // The session's own Okta subject is the member's linked one: a session
    // from an account the member re-linked away from ends.
    if !l.enabled
        || linked.is_empty()
        || okta_sub != Some(linked)
        || s(m, "okta_issuer") != l.issuer
        || iat < after
    {
        return Err(Ended::Revoked);
    }
    Ok(())
}

/// Marks claims made from an MCP connection the way the middleware marks a
/// session: how it signed in, and the Okta subject.
pub fn mark_okta(mut claims: Claims, via: Option<&str>, okta_sub: Option<String>) -> Claims {
    claims.via_okta = signed_in_with_okta(via);
    claims.held = is_held(via, super::setup_checks::self_hosted());
    claims.okta_sub = okta_sub;
    claims
}

/// The row that makes an Okta identity linkable once per team (settings
/// table, `pk = OKTA_LINK#<team>`, `sk = <issuer>#<sub>`): one partition per
/// team, so removing Okta deletes every row of the team directly.
pub fn link_row(team: &str, issuer: &str, sub: &str) -> (String, String) {
    (link_rows_pk(team), format!("{issuer}#{sub}"))
}

pub fn link_rows_pk(team: &str) -> String {
    format!("OKTA_LINK#{team}")
}

/// The first release's row key (`pk = OKTA_LINK#<team>#<issuer>#<sub>`,
/// `sk = LINK`), still deleted wherever a link goes.
pub fn legacy_link_row_key(team: &str, issuer: &str, sub: &str) -> String {
    format!("OKTA_LINK#{team}#{issuer}#{sub}")
}
const LEGACY_LINK_ROW_SK: &str = "LINK";

/// Deletes one link's rows (this layout and the first release's).
pub async fn delete_link_row(state: &AppState, team: &str, issuer: &str, sub: &str) {
    if issuer.is_empty() || sub.is_empty() {
        return;
    }
    let (pk, sk) = link_row(team, issuer, sub);
    let _ = delete_setup(state, &pk, &sk).await;
    let _ = delete_setup(
        state,
        &legacy_link_row_key(team, issuer, sub),
        LEGACY_LINK_ROW_SK,
    )
    .await;
}

/// Deletes every link row of `team` directly, whether or not its member is
/// still on the team.
async fn delete_team_link_rows(state: &AppState, team: &str) -> Result<usize, String> {
    let mut n = 0;
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", attr_s(&link_rows_pk(team)))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("{e}"))?;
        for i in out.items() {
            delete_setup(state, s(i, "pk"), s(i, "sk")).await?;
            n += 1;
        }
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(n)
}

/// The member update a link makes. Re-linking to another Okta account
/// (a previous link existed) also ends the old account's Okta sessions.
pub fn link_update_expression(had_previous: bool) -> &'static str {
    if had_previous {
        "SET okta_issuer = :i, okta_sub = :s, okta_linked_at = :t, okta_valid_after = :n"
    } else {
        "SET okta_issuer = :i, okta_sub = :s, okta_linked_at = :t"
    }
}

fn now_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// Removes a member's link, its uniqueness row, and ends their Okta
/// sessions (`okta_valid_after`). Their other sessions are untouched.
async fn unlink_member(state: &AppState, team: &str, member: &Item) -> Result<(), String> {
    let (issuer, sub) = (s(member, "okta_issuer"), s(member, "okta_sub"));
    delete_link_row(state, team, issuer, sub).await;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(s(member, "sk")))
        .update_expression("REMOVE okta_issuer, okta_sub, okta_linked_at SET okta_valid_after = :n")
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(
            ":n",
            aws_sdk_dynamodb::types::AttributeValue::N(now_secs().to_string()),
        )
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

/// Ends every Okta session on `team` without dropping links (Okta turned off).
async fn end_okta_sessions(state: &AppState, team: &str) -> Result<(), String> {
    let members = super::sign_in::team_members(state, team).await?;
    for m in members.iter().filter(|m| !s(m, "okta_sub").is_empty()) {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.users_table_name)
            .key("pk", attr_s(team))
            .key("sk", attr_s(s(m, "sk")))
            .update_expression("SET okta_valid_after = :n")
            .expression_attribute_values(
                ":n",
                aws_sdk_dynamodb::types::AttributeValue::N(now_secs().to_string()),
            )
            .send()
            .await
            .map_err(|e| format!("{e}"))?;
    }
    Ok(())
}

/// The member record that linked exactly this Okta identity: the same
/// issuer and subject, an active member (never an invite).
pub fn linked_record<'a>(members: &'a [Item], issuer: &str, sub: &str) -> Option<&'a Item> {
    if issuer.is_empty() || sub.is_empty() {
        return None;
    }
    members.iter().find(|m| {
        s(m, "sk").starts_with("USER#")
            && s(m, "status") != "invited"
            && s(m, "okta_issuer") == issuer
            && s(m, "okta_sub") == sub
    })
}

const LINK_AUD: &str = "okta_link";

fn link_ticket(team: &str, by: &str, secret: &str) -> Result<String, String> {
    signed_ticket(team, by, LINK_AUD, secret)
}

fn signed_ticket(team: &str, by: &str, aud: &str, secret: &str) -> Result<String, String> {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &TestTicket {
            team: team.to_string(),
            by: by.to_string(),
            aud: aud.into(),
            exp: chrono::Utc::now().timestamp() as u64 + TEST_TICKET_SECS,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| e.to_string())
}

fn read_signed_ticket(token: &str, aud: &str, secret: &str) -> Option<TestTicket> {
    let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    v.set_audience(&[aud]);
    v.set_required_spec_claims(&["exp", "aud"]);
    v.leeway = 0;
    jsonwebtoken::decode::<TestTicket>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &v,
    )
    .ok()
    .map(|d| d.claims)
}

/// Where linking lands: Settings, Security, with the outcome.
pub fn link_result_url(app_url: &str, outcome: Result<(), &str>) -> String {
    let base = format!("{}/settings/security/", app_url.trim_end_matches('/'));
    match outcome {
        Ok(()) => format!("{base}?okta_link=linked#okta"),
        Err(code) => format!(
            "{base}?okta_link=failed&reason={}#okta",
            urlencoding::encode(code)
        ),
    }
}

fn link_redirect(state: &AppState, outcome: Result<(), &str>) -> Response {
    let mut resp = (
        StatusCode::FOUND,
        [(header::LOCATION, link_result_url(app(state), outcome))],
    )
        .into_response();
    for c in oauth_signin::clear_cookies(Provider::Okta) {
        if let Ok(v) = HeaderValue::from_str(&c) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    resp
}

/// The session a request carries, with how it signed in, only while it
/// still stands against the member's record (the same check every API
/// request makes).
async fn session_of(state: &AppState, headers: &HeaderMap) -> Option<(Claims, Option<String>)> {
    let tok = oauth_signin::cookie_value(
        super::auth::cookie_header(headers),
        "__Host-coderhelm_session",
    )?;
    let mut claims = crate::auth::jwt::validate_token(tok, &state.secrets.jwt_secret).ok()?;
    let proof = crate::auth::jwt::read_proof(tok, &state.secrets.jwt_secret);
    let via = proof.via.clone();
    claims.via_okta = signed_in_with_okta(via.as_deref());
    claims.held = is_held(via.as_deref(), super::setup_checks::self_hosted());
    claims.okta_sub = proof.okta_sub.clone();
    super::sign_in::check_member_session(state, &claims)
        .await
        .ok()?;
    // A team that requires Okta sign-in: only the sessions it still allows.
    super::require_okta::check_claims(state, &claims, &proof, false)
        .await
        .ok()?;
    Some((claims, via))
}

/// Linking is done by the member themselves, from a session they started
/// another way (never from an Okta session), for their own record.
pub fn may_link(ticket: &TestTicket, session: Option<&(Claims, Option<String>)>) -> bool {
    session.is_some_and(|(c, via)| {
        c.sub == ticket.by && c.team_id == ticket.team && !signed_in_with_okta(via.as_deref())
    })
}

/// GET /api/users/okta/link: whether this team (self-hosted: the
/// organization) has Okta sign-in, and whether the caller linked theirs.
pub async fn link_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<crate::auth::jwt::SignInProof>>,
) -> Result<Json<Value>, ApiError> {
    let (team, live) = live_setup_for(&state, &claims).await?;
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
    let linked = me.as_ref().is_some_and(|m| {
        live.as_ref()
            .is_some_and(|l| !s(m, "okta_sub").is_empty() && s(m, "okta_issuer") == l.issuer)
    });
    Ok(Json(json!({
        "available": live.as_ref().is_some_and(|l| l.enabled),
        "issuer": live.as_ref().map(|l| l.issuer.clone()),
        "linked": linked,
        "linked_at": me.as_ref().filter(|_| linked).and_then(|m| opt(m, "okta_linked_at")),
        // Linking needs a session started another way.
        "from_okta": signed_in_with_okta(proof.as_ref().and_then(|p| p.via.as_deref())),
    })))
}

/// The team whose Okta a member links to (self-hosted: the organization's)
/// and its live setup.
async fn live_setup_for(
    state: &AppState,
    claims: &Claims,
) -> Result<(String, Option<OktaSetup>), ApiError> {
    let team = if super::sign_in::applies() {
        super::sign_in::load_org(state)
            .await
            .map_err(|e| internal("Organization not read", e))?
            .map(|o| o.team_id)
            .unwrap_or_else(|| claims.team_id.clone())
    } else {
        claims.team_id.clone()
    };
    let live = load_setup(state, &team, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not read", e))?;
    Ok((team, live))
}

/// POST /api/users/okta/link: the link to Okta that links the caller's own
/// Okta identity to their member record. Refused from an Okta session.
pub async fn link_start(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    proof: Option<Extension<crate::auth::jwt::SignInProof>>,
) -> Result<Json<Value>, ApiError> {
    if signed_in_with_okta(proof.as_ref().and_then(|p| p.via.as_deref())) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Sign in with your password, Google, GitHub or Microsoft to link Okta.",
        ));
    }
    let (team, live) = live_setup_for(&state, &claims).await?;
    if team != claims.team_id || !live.as_ref().is_some_and(|l| l.enabled) {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "Okta sign-in isn't on for this team.",
        ));
    }
    let ticket = link_ticket(&team, &claims.sub, &state.secrets.jwt_secret)
        .map_err(|e| internal("Okta link not made", e))?;
    Ok(Json(json!({
        "url": format!("{}/auth/okta/link?ticket={ticket}", api(&state).trim_end_matches('/')),
    })))
}

/// DELETE /api/users/okta/link: unlink the caller's Okta identity. Their
/// Okta sessions end; their other sessions don't.
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
    // While the team requires Okta sign-in, unlinking would leave the member
    // no way to sign in again.
    if rule_of(&state, &claims.team_id).await?.require_okta {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "Your team requires Okta sign-in, so you can't unlink Okta: you couldn't sign in again. Ask the owner to turn the rule off first.",
                "code": "okta_required_on",
            })),
        ));
    }
    unlink_member(&state, &claims.team_id, &me)
        .await
        .map_err(|e| internal("Okta link not removed", e))?;
    Ok(Json(json!({ "linked": false })))
}

/// GET /auth/okta/link?ticket=...: to the team's Okta to link the member's
/// own identity. Only in the browser of the member who asked, from a
/// session they started another way.
pub async fn link_begin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<TestQuery>,
) -> Response {
    let Some(t) = read_signed_ticket(&q.ticket, LINK_AUD, &state.secrets.jwt_secret) else {
        return link_redirect(&state, Err("expired"));
    };
    if !may_link(&t, session_of(&state, &headers).await.as_ref()) {
        return link_redirect(&state, Err("not_yours"));
    }
    let live = match load_setup(&state, &t.team, OKTA_SK).await {
        Ok(Some(l)) if l.enabled => l,
        _ => return link_redirect(&state, Err("off")),
    };
    match start_okta_flow(&state, &live, Mode::Link(&t.by), None).await {
        Ok(r) => r,
        Err(f) => {
            warn!(step = f.step, detail = %f.detail, "Okta link did not start");
            link_redirect(&state, Err("discovery"))
        }
    }
}

/// The end of a link: the identity Okta vouched for goes on the member's
/// own record, unless another member already linked it.
async fn okta_link_return(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
) -> Response {
    let live = match load_setup(state, &flow.team, OKTA_SK).await {
        Ok(Some(l)) if l.enabled => l,
        _ => return link_redirect(state, Err("off")),
    };
    let claims = match okta_claims(state, cookies, params, flow, &live).await {
        Ok(c) => c,
        Err(f) => {
            warn!(step = f.step, reason = f.reason.code(), "Okta link failed");
            return link_redirect(
                state,
                Err(match f.reason {
                    Reason::Cancelled => "cancelled",
                    Reason::Expired => "expired",
                    _ => "provider",
                }),
            );
        }
    };
    let sub = claims["sub"].as_str().unwrap_or_default();
    let members = match super::sign_in::team_members(state, &flow.team).await {
        Ok(m) => m,
        Err(_) => return link_redirect(state, Err("server")),
    };
    if linked_record(&members, &live.issuer, sub).is_some_and(|m| s(m, "sk") != flow.link) {
        return link_redirect(state, Err("taken"));
    }
    if linked_record(&members, &live.issuer, sub).is_some_and(|m| s(m, "sk") == flow.link) {
        return link_redirect(state, Ok(()));
    }
    // The member's previous link (another identity) gives up its row.
    let previous = members
        .iter()
        .find(|m| s(m, "sk") == flow.link)
        .map(|m| {
            (
                s(m, "okta_issuer").to_string(),
                s(m, "okta_sub").to_string(),
            )
        })
        .filter(|(i, s)| !i.is_empty() && !s.is_empty());
    let res = link_transaction(state, &flow.team, &flow.link, &live.issuer, sub, previous).await;
    if let Err(e) = &res {
        if e.contains("ConditionalCheckFailed") || e.contains("TransactionCanceled") {
            return link_redirect(state, Err("taken"));
        }
    }
    match res {
        Ok(_) => {
            info!(team_id = %flow.team, "Okta identity linked by its member");
            link_redirect(state, Ok(()))
        }
        Err(e) => {
            warn!(error = %e, "Okta link not saved");
            link_redirect(state, Err("server"))
        }
    }
}

/// The link written in one transaction: the uniqueness row (only if no
/// member holds this identity yet) and the member's own record (an active
/// The link written in one transaction: the uniqueness row (only if no
/// member holds this identity yet) and the member's own record (an active
/// member). A re-link removes the member's previous row and ends the
/// previous account's Okta sessions (`okta_valid_after`).
async fn link_transaction(
    state: &AppState,
    team: &str,
    member: &str,
    issuer: &str,
    sub: &str,
    previous: Option<(String, String)>,
) -> Result<(), String> {
    use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem, Update};
    let e = |e: aws_sdk_dynamodb::error::BuildError| e.to_string();
    let (row_pk, row_sk) = link_row(team, issuer, sub);
    let mut update = Update::builder()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(team))
        .key("sk", attr_s(member))
        .update_expression(link_update_expression(previous.is_some()))
        .condition_expression("attribute_exists(pk) AND (attribute_not_exists(#st) OR #st <> :inv)")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":i", attr_s(issuer))
        .expression_attribute_values(":s", attr_s(sub))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":inv", attr_s("invited"));
    if previous.is_some() {
        update = update.expression_attribute_values(
            ":n",
            aws_sdk_dynamodb::types::AttributeValue::N(now_secs().to_string()),
        );
    }
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
    if let Some((pi, ps)) = &previous {
        let (ppk, psk) = link_row(team, pi, ps);
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
        .map_err(|e| format!("{e:?}"))?;
    if let Some((pi, ps)) = &previous {
        // A first-release row of the previous link, if there is one.
        let _ = delete_setup(
            state,
            &legacy_link_row_key(team, pi, ps),
            LEGACY_LINK_ROW_SK,
        )
        .await;
    }
    Ok(())
}

/// Every member's Okta link on `team`, dropped (the issuer changed, or Okta
/// was removed).
async fn drop_links(state: &AppState, team: &str) -> Result<usize, String> {
    let members = super::sign_in::team_members(state, team).await?;
    let mut n = 0;
    for m in members.iter().filter(|m| !s(m, "okta_sub").is_empty()) {
        unlink_member(state, team, m).await?;
        n += 1;
    }
    // Rows of members no longer on the team go too.
    delete_team_link_rows(state, team).await?;
    Ok(n)
}

// ── Okta: Test sign-in ──────────────────────────────────────────────

/// An admin's Test sign-in link: signed, for one team's draft, short-lived.
#[derive(Debug, Serialize, Deserialize)]
pub struct TestTicket {
    team: String,
    by: String,
    aud: String,
    exp: u64,
}

const TEST_AUD: &str = "okta_test";

fn test_ticket(team: &str, by: &str, secret: &str) -> Result<String, String> {
    signed_ticket(team, by, TEST_AUD, secret)
}

fn read_ticket(token: &str, secret: &str) -> Option<TestTicket> {
    read_signed_ticket(token, TEST_AUD, secret)
}

/// A Test sign-in link is used by the admin who made it, in their team.
fn ticket_is_theirs(t: &TestTicket, session: Option<&Claims>) -> bool {
    session.is_some_and(|c| c.sub == t.by && c.team_id == t.team)
}

/// Where a Test sign-in lands: Settings, Sign-in, with the outcome.
pub fn test_result_url(app_url: &str, outcome: Result<(), &str>) -> String {
    let base = format!("{}/settings/sign-in/", app_url.trim_end_matches('/'));
    match outcome {
        Ok(()) => format!("{base}?okta_test=passed#okta"),
        Err(code) => format!(
            "{base}?okta_test=failed&reason={}#okta",
            urlencoding::encode(code)
        ),
    }
}

fn test_redirect(state: &AppState, outcome: Result<(), &str>) -> Response {
    let mut resp = (
        StatusCode::FOUND,
        [(header::LOCATION, test_result_url(app(state), outcome))],
    )
        .into_response();
    for c in oauth_signin::clear_cookies(Provider::Okta) {
        if let Ok(v) = HeaderValue::from_str(&c) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    resp
}

#[derive(Deserialize)]
pub struct TestQuery {
    #[serde(default)]
    pub ticket: String,
}

/// GET /auth/okta/test?ticket=...: an admin's Test sign-in of the team's
/// draft setup. Okta asks for the password again.
pub async fn okta_test_start(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<TestQuery>,
) -> Response {
    let Some(t) = read_ticket(&q.ticket, &state.secrets.jwt_secret) else {
        return test_redirect(&state, Err("expired"));
    };
    // The link works only in the browser of the admin who made it: their own
    // session (sent on this same-site navigation) for the same team.
    let session = session_of(&state, &headers).await.map(|(c, _)| c);
    if !ticket_is_theirs(&t, session.as_ref()) {
        return test_redirect(&state, Err("not_yours"));
    }
    let draft = match load_setup(&state, &t.team, OKTA_DRAFT_SK).await {
        Ok(Some(d)) => d,
        Ok(None) => return test_redirect(&state, Err("no_draft")),
        Err(e) => {
            error!(error = %e, "Okta draft not read");
            return test_redirect(&state, Err("server"));
        }
    };
    match start_okta_flow(&state, &draft, Mode::Test(&t.by), None).await {
        Ok(r) => r,
        Err(f) => {
            warn!(step = f.step, detail = %f.detail, "Okta test did not start");
            test_redirect(
                &state,
                Err(if f.step == "discovery" {
                    "discovery"
                } else {
                    "server"
                }),
            )
        }
    }
}

/// The end of a Test sign-in: the draft is marked tested (as whom) when
/// every check a real sign-in makes passed. Nobody is signed in.
async fn okta_test_return(
    state: &AppState,
    cookies: &str,
    params: &CallbackQuery,
    flow: &Flow,
) -> Response {
    let draft = match load_setup(state, &flow.team, OKTA_DRAFT_SK).await {
        Ok(Some(d)) => d,
        _ => return test_redirect(state, Err("no_draft")),
    };
    let claims = match okta_claims(state, cookies, params, flow, &draft).await {
        Ok(c) => c,
        Err(f) => {
            warn!(step = f.step, reason = f.reason.code(), detail = %f.detail, "Okta test sign-in failed");
            let code = match (f.step, f.reason) {
                (_, Reason::Cancelled) => "cancelled",
                (_, Reason::Expired) => "expired",
                ("token_exchange", _) => "token_exchange",
                ("discovery", _) | ("keys", _) => "discovery",
                ("id_token", _) => "id_token",
                _ => "server",
            };
            return test_redirect(state, Err(code));
        }
    };
    let email = match okta_email(&claims) {
        Ok(e) => e,
        Err(e) => {
            warn!(detail = e, "Okta test sign-in had no verified email");
            return test_redirect(state, Err("no_verified_email"));
        }
    };
    if !draft.domains.is_empty() && !okta_covers(&draft, &[], false, &email) {
        return test_redirect(state, Err("domain"));
    }
    let now = chrono::Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&draft.team_id))
        .key("sk", attr_s(OKTA_DRAFT_SK))
        .update_expression("SET tested_at = :t, tested_email = :e, tested_by = :b")
        // Only the draft that was tested: one saved since then needs its own test.
        .condition_expression("issuer = :i AND client_id = :c AND client_secret_sealed = :s")
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":e", attr_s(&email))
        .expression_attribute_values(":b", attr_s(&flow.by))
        .expression_attribute_values(":i", attr_s(&draft.issuer))
        .expression_attribute_values(":c", attr_s(&draft.client_id))
        .expression_attribute_values(":s", attr_s(&draft.secret_sealed))
        .send()
        .await;
    match res {
        Ok(_) => {
            info!(team_id = %draft.team_id, "Okta test sign-in passed");
            test_redirect(state, Ok(()))
        }
        Err(e) => {
            warn!(error = %e, "Okta test result not saved");
            test_redirect(state, Err("changed"))
        }
    }
}

// ── Okta: Settings, Sign-in ─────────────────────────────────────────

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

/// Admins and owners only.
fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|_| fail(StatusCode::FORBIDDEN, "Only admins and owners can do that."))
}

/// Self-hosted: saving Okta sign-in or turning it on is the owner's alone
/// (an admin's own Okta could otherwise vouch for any member).
pub fn owner_on_self_host(claims: &Claims, _self_hosted: bool) -> Result<(), ApiError> {
    // On both deployments: an Okta setup decides who signs in as whom.
    if claims.role != "owner" {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the owner can save or turn on Okta sign-in.",
        ));
    }
    Ok(())
}

/// The draft the owner saw is the one saved: same issuer, client id and
/// last change.
pub fn draft_matches(draft: &OktaSetup, body: &OktaSaveBody) -> bool {
    body.issuer.as_deref() == Some(draft.issuer.as_str())
        && body.client_id.as_deref() == Some(draft.client_id.as_str())
        && body.updated_at.as_deref() == draft.updated_at.as_deref()
}

/// The team whose Okta setup an admin manages here: on hosted the team
/// they're acting in, self-hosted the organization (from its team only).
async fn setup_team(state: &AppState, claims: &Claims) -> Result<String, ApiError> {
    if super::sign_in::applies() {
        let org = super::sign_in::ensure_org(state)
            .await
            .map_err(|e| internal("Organization not read", e))?
            .filter(|o| o.team_id == claims.team_id)
            .ok_or_else(|| fail(StatusCode::NOT_FOUND, "This team isn't the organization."))?;
        return Ok(org.team_id);
    }
    Ok(claims.team_id.clone())
}

async fn audit(state: &AppState, claims: &Claims, detail: String) -> Result<(), ApiError> {
    super::roles::record(
        state,
        claims,
        super::roles::AuditEntry {
            action: "sign_in_okta",
            role_id: "",
            role_name: "",
            detail,
        },
    )
    .await
}

async fn okta_view(state: &AppState, team: &str) -> Result<Value, ApiError> {
    let live = load_setup(state, team, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not read", e))?;
    let draft = load_setup(state, team, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not read", e))?;
    // Hosted: each email domain is proven with a DNS TXT record first.
    let mut domains: Vec<String> = Vec::new();
    for d in draft
        .iter()
        .chain(live.iter())
        .flat_map(|s| s.domains.iter())
    {
        if !domains.contains(d) {
            domains.push(d.clone());
        }
    }
    let mut proofs = Vec::new();
    if !super::sign_in::applies() {
        for d in &domains {
            let owner = domain_owner(state, d)
                .await
                .map_err(|e| internal("Okta domain not read", e))?;
            let (ours, theirs) = match &owner {
                Some((t, _)) if t == team => (true, false),
                Some(_) => (false, true),
                None => (false, false),
            };
            let failing = if ours {
                domain_failing_since(state, d)
                    .await
                    .map_err(|e| internal("Okta domain not read", e))?
            } else {
                None
            };
            proofs.push(json!({
                "domain": d,
                "txt_name": domain_txt_name(d),
                "txt_value": domain_txt_value(&state.secrets.jwt_secret, team, d),
                "verified_at": owner.filter(|_| ours).map(|(_, at)| at),
                // Kept while Okta sign-in is required, but its TXT record
                // is gone: add it back and verify again.
                "failing_since": failing.clone(),
                // When a failing proof is released unless its record is back.
                "release_after": failing.as_deref().and_then(grace_ends).map(|t| t.to_rfc3339()),
                "other_team": theirs,
            }));
        }
    }
    Ok(json!({
        "callback_url": okta_redirect_uri(api(state)),
        "sign_in_redirect_uri": okta_redirect_uri(api(state)),
        "sign_out_redirect_uri": app(state),
        "live": live.as_ref().map(OktaSetup::public),
        "draft": draft.as_ref().map(OktaSetup::public),
        "self_hosted": super::sign_in::applies(),
        "domains": proofs,
    }))
}

/// GET /api/sign-in/okta: the team's (self-hosted: the organization's) Okta
/// setup and draft, without their secrets, and the callback address to copy
/// into Okta.
pub async fn okta_get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    Ok(Json(okta_view(&state, &team).await?))
}

#[derive(Deserialize)]
pub struct OktaDraftBody {
    pub issuer: String,
    pub client_id: String,
    /// Empty or missing: keep the secret already saved (draft, else live).
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub domains: Vec<String>,
}

/// The sealed secret to keep when the admin leaves the secret box empty:
/// only one saved for exactly this issuer and client id.
pub fn kept_secret(saved: &[Option<OktaSetup>], issuer: &str, client_id: &str) -> Option<String> {
    saved
        .iter()
        .flatten()
        .find(|s| s.issuer == issuer && s.client_id == client_id && !s.secret_sealed.is_empty())
        .map(|s| s.secret_sealed.clone())
}

/// The domains an old draft's claims give up when it is replaced by `new`
/// (or discarded, `None`): its domains that neither the new draft nor the
/// live setup lists.
pub fn released_by_draft(
    old: &OktaSetup,
    new: Option<&OktaSetup>,
    live: Option<&OktaSetup>,
) -> Vec<String> {
    old.domains
        .iter()
        .filter(|d| !new.is_some_and(|n| n.domains.contains(d)))
        .filter(|d| !live.is_some_and(|l| l.domains.contains(d)))
        .cloned()
        .collect()
}

/// POST /api/sign-in/okta/test: saves the values as the draft (the secret
/// sealed) and answers the Test sign-in link. Nothing changes for anyone
/// signing in until a tested draft is saved.
pub async fn okta_test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<OktaDraftBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    let issuer = clean_issuer(&body.issuer).map_err(|m| fail(StatusCode::BAD_REQUEST, m))?;
    let client_id = body.client_id.trim().to_string();
    if client_id.is_empty() || client_id.len() > 200 || client_id.chars().any(char::is_whitespace) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Enter the Client ID from your Okta app.",
        ));
    }
    let domains = super::sign_in::clean_domains(&body.domains)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    if domains.is_empty() && !super::sign_in::applies() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Add the email domain your people sign in with, such as example.com.",
        ));
    }
    let new_secret = body
        .client_secret
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let secret_sealed = match new_secret {
        Some(v) if v.len() > 500 => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "That client secret is too long.",
            ))
        }
        Some(v) => seal_secret(&state, &team, v)
            .await
            .map_err(|e| internal("Okta secret not sealed", e))?,
        None => {
            // A saved secret is reused only for the same issuer and client:
            // it is never sent to a different Okta.
            let draft = load_setup(&state, &team, OKTA_DRAFT_SK)
                .await
                .ok()
                .flatten();
            let live = load_setup(&state, &team, OKTA_SK).await.ok().flatten();
            let kept = kept_secret(&[draft, live], &issuer, &client_id);
            kept.ok_or_else(|| {
                fail(
                    StatusCode::BAD_REQUEST,
                    "Enter the client secret from your Okta app. A saved secret is kept only while the Okta domain and Client ID stay the same.",
                )
            })?
        }
    };
    // Okta's discovery document first: a wrong domain is said here, not at Okta.
    if let Err(e) = discover(idp(), &issuer).await {
        warn!(error = %e, "Okta discovery failed");
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("CoderHelm couldn't read the sign-in settings at {issuer}/.well-known/openid-configuration. Check the Okta domain or issuer."),
        ));
    }
    let draft = OktaSetup {
        team_id: team.clone(),
        issuer,
        client_id,
        secret_sealed,
        domains,
        enabled: false,
        tested_at: None,
        tested_email: None,
        tested_by: None,
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_by: Some(claims.display_name()),
    };
    // A replaced draft gives up domains no longer listed (unless live keeps them).
    if let Ok(Some(old)) = load_setup(&state, &team, OKTA_DRAFT_SK).await {
        let live = load_setup(&state, &team, OKTA_SK).await.ok().flatten();
        for d in released_by_draft(&old, Some(&draft), live.as_ref()) {
            let _ = release_domain_while_rule_off(&state, &team, &d).await;
        }
    }
    put_setup(&state, &draft, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not saved", e))?;
    let ticket = test_ticket(&team, &claims.sub, &state.secrets.jwt_secret)
        .map_err(|e| internal("Okta test link not made", e))?;
    Ok(Json(json!({
        "test_url": format!("{}/auth/okta/test?ticket={ticket}", api(&state).trim_end_matches('/')),
        "draft": draft.public(),
    })))
}

#[derive(Deserialize)]
pub struct OktaSaveBody {
    /// Turn sign-in on with it (the default) or keep it off.
    #[serde(default = "yes")]
    pub enabled: bool,
    /// The draft the owner reviewed: its issuer, client id and `updated_at`.
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

fn yes() -> bool {
    true
}

/// POST /api/sign-in/okta/save: the draft becomes the live setup. Only a
/// draft whose Test sign-in passed.
pub async fn okta_save(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<OktaSaveBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    owner_on_self_host(&claims, super::sign_in::applies())?;
    let team = setup_team(&state, &claims).await?;
    let Some(draft) = load_setup(&state, &team, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not read", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "There's nothing new to save."));
    };
    let current = load_setup(&state, &team, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not read", e))?;
    if save_narrows(current.as_ref(), &draft, body.enabled) {
        // Turning Okta off, moving it to another issuer (which drops every
        // link) or dropping a domain while the team requires it would lock
        // people out.
        if let Some(refused) =
            super::require_okta::setup_change_refused(&rule_of(&state, &team).await?)
        {
            return Err(refused);
        }
    }
    // A new Okta app (client id or secret) while the team requires Okta
    // sign-in: the owner only, from their own Okta session, and only after
    // they ran Test sign-in with it themselves.
    let client_changes = current.as_ref().is_some_and(|l| client_changed(l, &draft));
    let rule_on_now = rule_of(&state, &team).await?.require_okta;
    if client_changes && rule_on_now {
        if let Err(why) = may_change_client(&claims, &draft) {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({ "error": why, "code": "okta_client_change" })),
            ));
        }
    }
    if !draft_matches(&draft, &body) {
        return Err(fail(
            StatusCode::CONFLICT,
            "These settings changed since you last looked. Check them again before saving.",
        ));
    }
    if draft.tested_at.is_none() {
        return Err(fail(
            StatusCode::CONFLICT,
            "Run Test sign-in first. Only settings that signed someone in can be saved.",
        ));
    }
    if body.enabled {
        require_proven(&state, &draft).await?;
    }
    let live = OktaSetup {
        enabled: body.enabled,
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_by: Some(claims.display_name()),
        ..draft.clone()
    };
    audit(
        &state,
        &claims,
        format!(
            "Okta sign-in saved: {} for {}{}",
            live.issuer,
            if live.domains.is_empty() {
                "the organization's domains".to_string()
            } else {
                live.domains.join(", ")
            },
            if live.enabled { ", on" } else { ", off" }
        ),
    )
    .await?;
    let before = load_setup(&state, &team, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not read", e))?;
    // With the rule off, a client change lands only while it stays off
    // (turning it on meanwhile means the stricter check above applies).
    save_live(
        &state,
        &live,
        save_narrows(before.as_ref(), &live, live.enabled) || (client_changes && !rule_on_now),
    )
    .await?;
    if let Some(old) = &before {
        // A new issuer: every member's link was to the old one.
        if old.issuer != live.issuer {
            drop_links(&state, &team)
                .await
                .map_err(|e| internal("Okta links not dropped", e))?;
        }
        // Domains no longer listed are released (only while the rule is off).
        for d in old.domains.iter().filter(|d| !live.domains.contains(d)) {
            let _ = release_domain_while_rule_off(&state, &team, d).await;
        }
    }
    set_marker(&state, &team, live.enabled)
        .await
        .map_err(|e| internal("Okta marker not saved", e))?;
    let _ = delete_setup(&state, &team, OKTA_DRAFT_SK).await;
    info!(team_id = %team, enabled = live.enabled, "Okta sign-in saved");
    Ok(Json(okta_view(&state, &team).await?))
}

#[derive(Deserialize)]
pub struct OktaToggleBody {
    pub enabled: bool,
}

/// PUT /api/sign-in/okta: turn the live setup on or off. On takes a setup
/// whose Test sign-in passed.
pub async fn okta_toggle(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<OktaToggleBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    let Some(live) = load_setup(&state, &team, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not read", e))?
    else {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "Okta sign-in isn't set up yet.",
        ));
    };
    if body.enabled {
        owner_on_self_host(&claims, super::sign_in::applies())?;
    } else if let Some(refused) =
        super::require_okta::setup_change_refused(&rule_of(&state, &team).await?)
    {
        return Err(refused);
    }
    if body.enabled && live.tested_at.is_none() {
        return Err(fail(StatusCode::CONFLICT, "Run Test sign-in first."));
    }
    if body.enabled {
        require_proven(&state, &live).await?;
    }
    if body.enabled != live.enabled {
        audit(
            &state,
            &claims,
            if body.enabled {
                "Okta sign-in turned on".into()
            } else {
                "Okta sign-in turned off".into()
            },
        )
        .await?;
    }
    let next = OktaSetup {
        enabled: body.enabled,
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_by: Some(claims.display_name()),
        ..live
    };
    save_live(&state, &next, !next.enabled).await?;
    set_marker(&state, &team, next.enabled)
        .await
        .map_err(|e| internal("Okta marker not saved", e))?;
    if !next.enabled {
        end_okta_sessions(&state, &team)
            .await
            .map_err(|e| internal("Okta sessions not ended", e))?;
    }
    Ok(Json(okta_view(&state, &team).await?))
}

/// Saves the live setup; a save that narrows Okta sign-in (`narrows`: off,
/// another issuer, a domain dropped) lands only while the team doesn't
/// require Okta sign-in, checked in the same write.
async fn save_live(state: &AppState, setup: &OktaSetup, narrows: bool) -> Result<(), ApiError> {
    if !narrows {
        return put_setup(state, setup, OKTA_SK)
            .await
            .map_err(|e| internal("Okta setup not saved", e));
    }
    use aws_sdk_dynamodb::types::{Put, TransactWriteItem};
    let put = Put::builder()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(setup.to_item(OKTA_SK)))
        .build()
        .map_err(|e| internal("Okta setup not saved", e))?;
    super::require_okta::while_rule_off(
        state,
        &setup.team_id,
        vec![TransactWriteItem::builder().put(put).build()],
    )
    .await
}

/// The team's sign-in rules (the Okta rule is checked before Okta changes).
async fn rule_of(state: &AppState, team: &str) -> Result<super::sign_in::Rule, ApiError> {
    super::sign_in::team_rule(state, team)
        .await
        .map_err(|e| internal("Sign-in rules not read", e))
}

/// Whether `draft` uses another Okta app than the live setup (client id or
/// secret). Pure.
pub fn client_changed(live: &OktaSetup, draft: &OktaSetup) -> bool {
    live.client_id != draft.client_id || live.secret_sealed != draft.secret_sealed
}

/// While the team requires Okta sign-in, who may save a new Okta app: the
/// owner, from their own Okta session, after their own Test sign-in of it.
/// Pure.
pub fn may_change_client(claims: &Claims, draft: &OktaSetup) -> Result<(), &'static str> {
    if claims.role != "owner" || claims.support_view.is_some() {
        return Err("Okta sign-in is required, so only the owner can change the Okta app.");
    }
    if !claims.via_okta {
        return Err(
            "Okta sign-in is required: sign in with your team's Okta, then change the Okta app.",
        );
    }
    if draft.tested_by.as_deref() != Some(claims.sub.as_str()) {
        return Err(
            "Okta sign-in is required: run Test sign-in with the new Okta app yourself first.",
        );
    }
    Ok(())
}

/// Whether saving `draft` over the live setup takes sign-in away from
/// anyone: Okta off, another issuer (every link is dropped), or a domain
/// no longer listed. Pure.
pub fn save_narrows(live: Option<&OktaSetup>, draft: &OktaSetup, enabled: bool) -> bool {
    !enabled
        || live.is_none_or(|l| {
            l.issuer != draft.issuer || l.domains.iter().any(|d| !draft.domains.contains(d))
        })
}

/// Hosted: Okta sign-in turns on only once every one of its email domains
/// is proven by this team in DNS.
async fn require_proven(state: &AppState, setup: &OktaSetup) -> Result<(), ApiError> {
    if super::sign_in::applies() {
        return Ok(());
    }
    if all_domains_proven(state, setup)
        .await
        .map_err(|e| internal("Okta domains not read", e))?
    {
        return Ok(());
    }
    Err((
        StatusCode::CONFLICT,
        Json(json!({
            "error": "Verify each email domain first: add its TXT record in DNS, then choose Verify domains.",
            "code": "domains_not_verified",
        })),
    ))
}

/// POST /api/sign-in/okta/domains/verify (hosted): reads each email
/// domain's `_coderhelm-okta` TXT record and records the domains that show
/// this team's value. A domain proven by another team stays theirs.
pub async fn okta_verify_domains(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    if super::sign_in::applies() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "A self-hosted install doesn't verify domains: its admins run it.",
        ));
    }
    let mut domains: Vec<String> = Vec::new();
    for sk in [OKTA_DRAFT_SK, OKTA_SK] {
        if let Some(s) = load_setup(&state, &team, sk)
            .await
            .map_err(|e| internal("Okta setup not read", e))?
        {
            for d in s.domains {
                if !domains.contains(&d) {
                    domains.push(d);
                }
            }
        }
    }
    let now = chrono::Utc::now().to_rfc3339();
    let mut proven = Vec::new();
    for d in &domains {
        let expected = domain_txt_value(&state.secrets.jwt_secret, &team, d);
        let records = common::dns_txt::lookup(&domain_txt_name(d))
            .await
            .unwrap_or_default();
        if !txt_proves(&records, &expected) {
            continue;
        }
        // Held by another team: read their record again now, so a proof
        // whose TXT record is gone doesn't block this domain's real owner.
        if let Ok(Some((other, _))) = domain_owner(&state, d).await {
            if other != team {
                let _ = fresh_domain_owner(&state, d, true).await;
                // This team shows the matching record and the holder's is
                // definitely gone: the holder's proof (even one kept failing
                // while its rule is on) gives way now.
                if domain_owner(&state, d)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|(t, _)| t == other)
                    && contender_takes_over(txt_check(&state, &other, d).await)
                {
                    let released = state
                        .dynamo
                        .delete_item()
                        .table_name(&state.config.settings_table_name)
                        .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{d}")))
                        .key("sk", attr_s(OKTA_DOMAIN_SK))
                        .condition_expression("team_id = :t")
                        .expression_attribute_values(":t", attr_s(&other))
                        .send()
                        .await
                        .is_ok();
                    if released {
                        warn!(domain = %d, "Okta domain proven by another team; the holder's gone proof released");
                        super::require_okta::domain_released(&state, &other, d).await;
                    }
                }
            }
        }
        let claimed = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{d}")))
            .item("sk", attr_s(OKTA_DOMAIN_SK))
            .item("team_id", attr_s(&team))
            .item("verified_at", attr_s(&now))
            .item("verified_by", attr_s(&claims.display_name()))
            .condition_expression("attribute_not_exists(pk) OR team_id = :t")
            .expression_attribute_values(":t", attr_s(&team))
            .send()
            .await
            .is_ok();
        if claimed {
            proven.push(d.clone());
        }
    }
    if !proven.is_empty() {
        audit(
            &state,
            &claims,
            format!("Okta email domains verified: {}", proven.join(", ")),
        )
        .await?;
    }
    Ok(Json(okta_view(&state, &team).await?))
}

/// DELETE /api/sign-in/okta/domains/:domain: release one of this team's
/// proven domains (Okta sign-in for it stops).
pub async fn okta_release_domain(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    if let Some(refused) = super::require_okta::setup_change_refused(&rule_of(&state, &team).await?)
    {
        return Err(refused);
    }
    let domain = domain.trim().to_lowercase();
    if domain_owner(&state, &domain)
        .await
        .map_err(|e| internal("Okta domain not read", e))?
        .map(|(t, _)| t)
        != Some(team.clone())
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "This team hasn't verified that domain.",
        ));
    }
    audit(
        &state,
        &claims,
        format!("Okta email domain released: {domain}"),
    )
    .await?;
    // Released only while the team doesn't require Okta sign-in, in the
    // same write that checks it.
    {
        use aws_sdk_dynamodb::types::{Delete, TransactWriteItem};
        let delete = Delete::builder()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&format!("{OKTA_DOMAIN_PREFIX}{domain}")))
            .key("sk", attr_s(OKTA_DOMAIN_SK))
            .condition_expression("team_id = :t")
            .expression_attribute_values(":t", attr_s(&team))
            .build()
            .map_err(|e| internal("Okta domain not released", e))?;
        super::require_okta::while_rule_off(
            &state,
            &team,
            vec![TransactWriteItem::builder().delete(delete).build()],
        )
        .await?;
    }
    Ok(Json(okta_view(&state, &team).await?))
}

/// DELETE /api/sign-in/okta: remove the setup and any draft. People who
/// signed in with Okta keep their accounts and sign in another way.
pub async fn okta_delete(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    if let Some(refused) = super::require_okta::setup_change_refused(&rule_of(&state, &team).await?)
    {
        return Err(refused);
    }
    audit(&state, &claims, "Okta sign-in removed".into()).await?;
    // The live setup goes first, only while the team doesn't require Okta
    // sign-in (checked in the same write); then everything that hung on it.
    {
        use aws_sdk_dynamodb::types::{Delete, TransactWriteItem};
        let delete = Delete::builder()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&team))
            .key("sk", attr_s(OKTA_SK))
            .build()
            .map_err(|e| internal("Okta setup not removed", e))?;
        let current = load_setup(&state, &team, OKTA_SK)
            .await
            .map_err(|e| internal("Okta setup not read", e))?;
        super::require_okta::while_rule_off(
            &state,
            &team,
            vec![TransactWriteItem::builder().delete(delete).build()],
        )
        .await?;
        if let Some(live) = current {
            for d in &live.domains {
                let _ = release_domain(&state, &team, d).await;
            }
        }
    }
    // The team's proven domains are released, so their owner can use them again.
    for sk in [OKTA_SK, OKTA_DRAFT_SK] {
        if let Ok(Some(setup)) = load_setup(&state, &team, sk).await {
            for d in &setup.domains {
                let _ = release_domain(&state, &team, d).await;
            }
        }
    }
    drop_links(&state, &team)
        .await
        .map_err(|e| internal("Okta links not dropped", e))?;
    for sk in [OKTA_SK, OKTA_DRAFT_SK] {
        delete_setup(&state, &team, sk)
            .await
            .map_err(|e| internal("Okta setup not removed", e))?;
    }
    set_marker(&state, &team, false)
        .await
        .map_err(|e| internal("Okta marker not removed", e))?;
    Ok(Json(okta_view(&state, &team).await?))
}

/// DELETE /api/sign-in/okta/draft: forget unsaved changes.
pub async fn okta_discard_draft(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    // A discarded draft gives up the domains only it listed.
    if let Ok(Some(old)) = load_setup(&state, &team, OKTA_DRAFT_SK).await {
        let live = load_setup(&state, &team, OKTA_SK).await.ok().flatten();
        for d in released_by_draft(&old, None, live.as_ref()) {
            let _ = release_domain_while_rule_off(&state, &team, &d).await;
        }
    }
    delete_setup(&state, &team, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not removed", e))?;
    Ok(Json(okta_view(&state, &team).await?))
}

/// A session a team's (or the organization's) Okta signed in.
pub fn signed_in_with_okta(via: Option<&str>) -> bool {
    via == Some("okta")
}

/// Whether a session is held to its team: one a team's Okta signed in, on
/// the hosted deployment only (a self-hosted install has one organization,
/// so there is nothing beyond it to hold it from).
pub fn is_held(via: Option<&str>, self_hosted: bool) -> bool {
    signed_in_with_okta(via) && !self_hosted
}

/// Setting up a second factor is person-wide, but someone on no other team
/// has nothing beyond this team to protect: they may set it up from an
/// Okta session, so a team requiring two-factor never strands them.
pub const FACTOR_SETUP: &[&str] = &["sign_in::app_setup", "sign_in::app_verify"];

/// Whether a held session is refused `handler`, given whether the person
/// is on any team besides this one.
pub fn held_refuses(handler: &str, on_other_teams: bool) -> bool {
    HELD_REFUSED.contains(&handler) && (on_other_teams || !FACTOR_SETUP.contains(&handler))
}

/// On hosted CoderHelm a session from a team's Okta speaks for that team
/// only: never the person's other teams, nor anything that belongs to the
/// person rather than the team.
pub fn team_only(claims: &Claims) -> bool {
    claims.held && !super::setup_checks::self_hosted()
}

/// The handlers a hosted Okta session never reaches: moving to another
/// team, and everything person-wide (the password account, the second
/// factor, linking another sign-in, ending the person's other sessions).
pub const HELD_REFUSED: &[&str] = &[
    "api::switch_team",
    "users::change_password",
    "users::mfa_setup",
    "users::mfa_verify_setup",
    "users::mfa_disable",
    "users::link_github",
    "sign_in::app_setup",
    "sign_in::app_verify",
    "sign_in::app_disable",
    "sign_in::replace_start",
    "sign_in::replace_verify",
];

pub const TEAM_ONLY_MESSAGE: &str = "You signed in with your team's Okta, which works for this team only. Sign in with your password, Google, GitHub or Microsoft to do this. That includes setting up two-factor sign-in when you're on other teams too: it protects all of them.";

/// The refusal for a held session asking for a person-wide handler. Setting
/// up a second factor is let through for someone on no other team.
pub async fn held_refusal(
    state: &AppState,
    claims: &Claims,
    handler: Option<&str>,
) -> Option<Response> {
    let h = handler.filter(|h| HELD_REFUSED.contains(h))?;
    if !claims.held {
        return None;
    }
    let others = if FACTOR_SETUP.contains(&h) && claims.email.contains('@') {
        super::sign_in::records_by_email(state, &claims.email)
            .await
            .map(|r| {
                r.iter()
                    .any(|i| s(i, "sk").starts_with("USER#") && s(i, "pk") != claims.team_id)
            })
            .unwrap_or(true)
    } else {
        true
    };
    held_refuses(h, others).then(|| {
        (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": TEAM_ONLY_MESSAGE, "code": "okta_team_only" })),
        )
            .into_response()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;

    const CLIENT: &str = "client-123";
    const TENANT: &str = "72f988bf-86f1-41af-91ab-2d7cd011db47";
    const NOW: u64 = 1_800_000_000;

    struct Signer {
        key: EncodingKey,
        jwks: JwkSet,
    }

    fn signer(kid: &str) -> Signer {
        let mut rng = rand08::thread_rng();
        let private = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let der = private.to_pkcs1_der().unwrap();
        let public = private.to_public_key();
        let jwks: JwkSet = serde_json::from_value(json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": kid,
                "n": B64URL.encode(public.n().to_bytes_be()),
                "e": B64URL.encode(public.e().to_bytes_be()),
            }]
        }))
        .unwrap();
        Signer {
            key: EncodingKey::from_rsa_der(der.as_bytes()),
            jwks,
        }
    }

    fn sign(s: &Signer, kid: &str, claims: &Value) -> String {
        let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
        h.kid = Some(kid.into());
        encode(&h, claims, &s.key).unwrap()
    }

    fn ms_claims() -> Value {
        json!({
            "iss": format!("https://login.microsoftonline.com/{TENANT}/v2.0"),
            "aud": CLIENT,
            "sub": "pairwise-sub",
            "oid": "00000000-0000-0000-0000-00000000abcd",
            "tid": TENANT,
            "iat": NOW - 10,
            "nbf": NOW - 10,
            "exp": NOW + 3600,
            "nonce": "N1",
            "name": "Ada Lovelace",
            "email": "Ada@Example.com",
            "xms_edov": true,
            "preferred_username": "ada@example.com",
        })
    }

    fn ms_expect<'a>(nonce: &'a str) -> Expect<'a> {
        Expect {
            issuer: Issuer::MicrosoftTenant,
            client_id: CLIENT,
            nonce,
            now: NOW,
        }
    }

    // jsonwebtoken checks exp against the real clock: tokens in these tests
    // are dated around the real now.
    fn now() -> u64 {
        chrono::Utc::now().timestamp() as u64
    }

    fn dated(mut c: Value) -> Value {
        let n = now();
        c["iat"] = json!(n - 10);
        c["nbf"] = json!(n - 10);
        c["exp"] = json!(n + 3600);
        c
    }

    #[test]
    fn a_good_microsoft_token_verifies_and_every_bad_one_does_not() {
        let s = signer("k1");
        let n = now();
        let want = Expect {
            now: n,
            ..ms_expect("N1")
        };
        let good = dated(ms_claims());
        let claims = verify_id_token(&sign(&s, "k1", &good), &s.jwks, &want).unwrap();
        assert_eq!(claims["oid"], good["oid"]);

        let bad = |f: &dyn Fn(&mut Value)| {
            let mut c = good.clone();
            f(&mut c);
            verify_id_token(&sign(&s, "k1", &c), &s.jwks, &want).unwrap_err()
        };
        // Another app's token.
        assert!(bad(&|c| c["aud"] = json!("someone-else")).contains("token"));
        // Expired, long enough ago to be past the leeway.
        bad(&|c| c["exp"] = json!(n - 3600));
        // Not yet valid.
        bad(&|c| c["nbf"] = json!(n + 3600));
        // Issued in the future.
        assert!(bad(&|c| c["iat"] = json!(n + 3600)).contains("future"));
        // Another browser's sign-in.
        assert!(bad(&|c| c["nonce"] = json!("N2")).contains("nonce"));
        bad(&|c| {
            c.as_object_mut().unwrap().remove("nonce");
        });
        // The issuer must name the token's own tenant, and the tenant must be a GUID.
        assert!(bad(&|c| c["iss"] =
            json!("https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0"))
        .contains("tenant"));
        bad(&|c| c["iss"] = json!("https://evil.example.com/{TENANT}/v2.0"));
        bad(&|c| {
            c["tid"] = json!("common");
            c["iss"] = json!("https://login.microsoftonline.com/common/v2.0");
        });
        // A v1 issuer is not this endpoint's.
        bad(&|c| c["iss"] = json!(format!("https://sts.windows.net/{TENANT}/")));
        // Several audiences without azp naming us.
        bad(&|c| c["aud"] = json!([CLIENT, "other"]));
        // No subject.
        bad(&|c| {
            c.as_object_mut().unwrap().remove("sub");
        });
        // With azp naming us, several audiences are fine.
        let mut multi = good.clone();
        multi["aud"] = json!([CLIENT, "other"]);
        multi["azp"] = json!(CLIENT);
        assert!(verify_id_token(&sign(&s, "k1", &multi), &s.jwks, &want).is_ok());
        // An empty expected nonce never matches.
        let none = Expect {
            now: n,
            ..ms_expect("")
        };
        assert!(verify_id_token(&sign(&s, "k1", &good), &s.jwks, &none).is_err());
    }

    #[test]
    fn signatures_are_checked_against_the_published_keys() {
        let s = signer("k1");
        let other = signer("k1");
        let want = Expect {
            now: now(),
            ..ms_expect("N1")
        };
        let good = dated(ms_claims());
        // Signed by a key that isn't the provider's, under the provider's key id.
        let forged = sign(&other, "k1", &good);
        assert!(verify_id_token(&forged, &s.jwks, &want)
            .unwrap_err()
            .contains("token"));
        // A key id the provider doesn't publish.
        let unknown = sign(&s, "k9", &good);
        assert!(verify_id_token(&unknown, &s.jwks, &want)
            .unwrap_err()
            .contains("publish"));
        // HS256 with the client id as the secret (an old trick) is refused.
        let mut h = Header::new(jsonwebtoken::Algorithm::HS256);
        h.kid = Some("k1".into());
        let hs = encode(&h, &good, &EncodingKey::from_secret(CLIENT.as_bytes())).unwrap();
        assert!(verify_id_token(&hs, &s.jwks, &want)
            .unwrap_err()
            .contains("RS256"));
        // A tampered payload.
        let t = sign(&s, "k1", &good);
        let mut parts: Vec<String> = t.split('.').map(str::to_string).collect();
        let mut c = good.clone();
        c["email"] = json!("ceo@example.com");
        parts[1] = B64URL.encode(serde_json::to_vec(&c).unwrap());
        assert!(verify_id_token(&parts.join("."), &s.jwks, &want).is_err());
        // An encryption key is not a signing key.
        let mut enc = serde_json::to_value(&s.jwks).unwrap();
        enc["keys"][0]["use"] = json!("enc");
        let enc: JwkSet = serde_json::from_value(enc).unwrap();
        assert!(verify_id_token(&t, &enc, &want)
            .unwrap_err()
            .contains("signatures"));
    }

    #[test]
    fn okta_tokens_need_the_exact_issuer() {
        let s = signer("ok");
        let iss = "https://acme.okta.com/oauth2/default";
        let want = Expect {
            issuer: Issuer::Exact(iss),
            client_id: CLIENT,
            nonce: "N1",
            now: now(),
        };
        let c = dated(json!({
            "iss": iss, "aud": CLIENT, "sub": "00u1", "nonce": "N1",
            "email": "ada@acme.example", "email_verified": true,
        }));
        assert!(verify_id_token(&sign(&s, "ok", &c), &s.jwks, &want).is_ok());
        for other in [
            "https://acme.okta.com",
            "https://acme.okta.com/oauth2/default/",
            "https://evil.okta.com/oauth2/default",
        ] {
            let mut c2 = c.clone();
            c2["iss"] = json!(other);
            assert!(
                verify_id_token(&sign(&s, "ok", &c2), &s.jwks, &want).is_err(),
                "{other}"
            );
        }
    }

    #[test]
    fn microsoft_email_is_taken_only_from_a_verified_claim() {
        // email with xms_edov true (Microsoft: "the user's email domain owner
        // has been verified").
        assert_eq!(microsoft_email(&ms_claims()).unwrap(), "ada@example.com");
        let mut c = ms_claims();
        c["xms_edov"] = json!("true");
        assert!(microsoft_email(&c).is_ok());
        // Without xms_edov, or false: email is not trusted.
        for edov in [json!(false), json!("false"), json!(1), Value::Null] {
            let mut c = ms_claims();
            c["xms_edov"] = edov.clone();
            assert!(microsoft_email(&c).is_err(), "{edov}");
        }
        // preferred_username alone is never enough.
        let mut c = ms_claims();
        c.as_object_mut().unwrap().remove("email");
        assert!(microsoft_email(&c).is_err());
        // verified_primary_email is never a fallback (Microsoft's docs only
        // say it is "Sourced from the user's PrimaryAuthoritativeEmail").
        let mut c = ms_claims();
        c.as_object_mut().unwrap().remove("xms_edov");
        c["verified_primary_email"] = json!(["ceo@example.com"]);
        assert!(microsoft_email(&c).is_err());
        c["xms_edov"] = json!(true);
        c.as_object_mut().unwrap().remove("email");
        assert!(microsoft_email(&c).is_err());
        // xms_edov true but no usable email.
        let mut c = ms_claims();
        c["email"] = json!("not-an-email");
        assert!(microsoft_email(&c).is_err());
        // Personal accounts follow the same rule: no xms_edov, no email.
        let mut c = ms_claims();
        c["tid"] = json!(MS_CONSUMER_TENANT);
        c["email"] = json!("ada@outlook.com");
        assert_eq!(microsoft_email(&c).unwrap(), "ada@outlook.com");
        c.as_object_mut().unwrap().remove("xms_edov");
        assert!(microsoft_email(&c).is_err());
    }

    #[test]
    fn a_microsoft_person_keeps_the_tenant_and_object_id() {
        let p = microsoft_person(&ms_claims()).unwrap();
        assert_eq!(p.email, "ada@example.com");
        assert_eq!(p.name, "Ada Lovelace");
        assert_eq!(
            p.new_user_id,
            format!("USER#microsoft_{TENANT}_00000000-0000-0000-0000-00000000abcd")
        );
    }

    #[test]
    fn okta_email_needs_email_verified_true() {
        let c = json!({ "sub": "00u1", "email": "Ada@Acme.example", "email_verified": true });
        assert_eq!(okta_email(&c).unwrap(), "ada@acme.example");
        // The issuer (a short hash) goes in the id, as Microsoft's tenant does.
        let id = okta_person(&c).unwrap().new_user_id;
        assert_eq!(
            id,
            okta_user_id(c["iss"].as_str().unwrap_or_default(), "00u1")
        );
        assert!(id.starts_with("USER#okta_") && id.ends_with("_00u1"));
        assert_ne!(
            okta_user_id("https://a.okta.example", "00u1"),
            okta_user_id("https://b.okta.example", "00u1")
        );
        assert_eq!(
            okta_user_id("https://a.okta.example/", "x"),
            okta_user_id("https://a.okta.example", "x")
        );
        for v in [json!(false), json!("true"), Value::Null] {
            let mut c2 = c.clone();
            c2["email_verified"] = v.clone();
            assert!(okta_email(&c2).is_err(), "{v}");
        }
        assert!(okta_email(&json!({ "email_verified": true })).is_err());
    }

    #[test]
    fn plain_emails_only() {
        assert_eq!(plain_email(" A@B.co ").as_deref(), Some("a@b.co"));
        for bad in [
            "", "a", "a@", "@b.co", "a@b", "a@@b.co", "a b@c.co", "a@b.co.", "a@.b.co",
        ] {
            assert!(plain_email(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn guids() {
        assert!(is_guid(TENANT));
        assert!(is_guid(&TENANT.to_uppercase()));
        for bad in [
            "",
            "common",
            "organizations",
            "72f988bf-86f1-41af-91ab",
            "72f988bf-86f1-41af-91ab-2d7cd011db4z",
        ] {
            assert!(!is_guid(bad), "{bad}");
        }
    }

    #[test]
    fn pkce_is_s256_of_the_verifier() {
        // RFC 7636 Appendix B.
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        let a = random_token();
        assert_eq!(a.len(), 43);
        assert_ne!(a, random_token());
    }

    #[test]
    fn the_flow_cookie_is_signed_and_tied_to_its_provider() {
        let secret = "s3cret";
        let f = Flow::new(Provider::Microsoft, "", false);
        let c = flow_cookie(Provider::Microsoft, &f, secret).unwrap();
        assert!(c.starts_with("__Host-oauth_flow_ms="));
        assert!(c.contains("HttpOnly") && c.contains("Secure") && c.contains("SameSite=Lax"));
        assert!(!c.contains("Domain="));
        let header = c.split(';').next().unwrap().to_string();
        assert_eq!(
            read_flow(Provider::Microsoft, &header, secret),
            Some(f.clone())
        );
        // Another key, another provider, or a hand-written cookie: nothing.
        assert!(read_flow(Provider::Microsoft, &header, "other").is_none());
        let okta_named = header.replace("__Host-oauth_flow_ms", "__Host-oauth_flow_okta");
        assert!(read_flow(Provider::Okta, &okta_named, secret).is_none());
        assert!(read_flow(Provider::Microsoft, "__Host-oauth_flow_ms=abc", secret).is_none());
        assert!(read_flow(Provider::Microsoft, "", secret).is_none());
        // Expired.
        let mut old = Flow::new(Provider::Okta, "TEAM#1", true);
        old.exp = 1;
        let c = flow_cookie(Provider::Okta, &old, secret).unwrap();
        assert!(read_flow(Provider::Okta, c.split(';').next().unwrap(), secret).is_none());
        // Google and GitHub have none.
        assert!(flow_cookie(Provider::Github, &f, secret).is_err());
    }

    #[test]
    fn issuers_are_https_host_names() {
        let ok = [
            ("acme.okta.com", "https://acme.okta.com"),
            ("https://acme.okta.com/", "https://acme.okta.com"),
            (
                "https://ACME.okta.com/oauth2/default",
                "https://acme.okta.com/oauth2/default",
            ),
            (" login.acme.example ", "https://login.acme.example"),
        ];
        for (input, want) in ok {
            assert_eq!(clean_issuer(input).unwrap(), want, "{input}");
        }
        for bad in [
            "",
            "http://acme.okta.com",
            "https://127.0.0.1",
            "https://[::1]",
            "https://localhost",
            "https://acme.okta.com:8443",
            "https://user:pw@acme.okta.com",
            "https://acme.okta.com/?x=1",
            "https://acme.okta.com/#f",
            "https://metadata.internal",
            "intranet",
            "ftp://acme.okta.com",
        ] {
            assert!(clean_issuer(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn discovery_must_match_the_issuer_and_stay_on_its_host() {
        let iss = "https://acme.okta.com/oauth2/default";
        let doc = json!({
            "issuer": iss,
            "authorization_endpoint": "https://acme.okta.com/oauth2/default/v1/authorize",
            "token_endpoint": "https://acme.okta.com/oauth2/default/v1/token",
            "jwks_uri": "https://acme.okta.com/oauth2/default/v1/keys",
        });
        let d = check_discovery(iss, &doc).unwrap();
        assert!(d.token_endpoint.ends_with("/v1/token"));
        let bad = |k: &str, v: &str| {
            let mut d = doc.clone();
            d[k] = json!(v);
            check_discovery(iss, &d).unwrap_err()
        };
        assert!(bad("issuer", "https://acme.okta.com").contains("issuer"));
        bad("token_endpoint", "https://evil.example.com/token");
        bad("jwks_uri", "http://acme.okta.com/oauth2/default/v1/keys");
        bad("jwks_uri", "https://acme.okta.com:444/keys");
        bad("authorization_endpoint", "https://169.254.169.254/latest");
        let mut missing = doc.clone();
        missing.as_object_mut().unwrap().remove("jwks_uri");
        assert!(check_discovery(iss, &missing).is_err());
    }

    fn setup(domains: &[&str]) -> OktaSetup {
        OktaSetup {
            team_id: "TEAM#1".into(),
            issuer: "https://acme.okta.com".into(),
            client_id: "0oa1".into(),
            secret_sealed: "c2VhbGVk".into(),
            domains: domains.iter().map(|d| d.to_string()).collect(),
            enabled: true,
            ..Default::default()
        }
    }

    #[test]
    fn which_emails_an_okta_setup_covers() {
        let hosted = setup(&["acme.example"]);
        assert!(okta_covers(&hosted, &[], false, "ada@acme.example"));
        assert!(okta_covers(&hosted, &[], false, "Ada@ACME.example"));
        assert!(!okta_covers(&hosted, &[], false, "ada@sub.acme.example"));
        assert!(!okta_covers(&hosted, &[], false, "ada@other.example"));
        assert!(!okta_covers(&hosted, &[], false, "not-an-email"));
        // Hosted without domains covers nobody.
        assert!(!okta_covers(&setup(&[]), &[], false, "ada@acme.example"));
        // Self-hosted: its own domains, else the organization's, else everyone.
        let org = vec!["acme.example".to_string()];
        assert!(okta_covers(&setup(&[]), &org, true, "ada@acme.example"));
        assert!(!okta_covers(&setup(&[]), &org, true, "ada@other.example"));
        assert!(okta_covers(&setup(&[]), &[], true, "ada@other.example"));
        assert!(!okta_covers(
            &setup(&["x.example"]),
            &org,
            true,
            "ada@acme.example"
        ));
    }

    #[test]
    fn the_admin_view_never_shows_the_secret() {
        let v = setup(&["acme.example"]).public();
        let text = v.to_string();
        assert!(!text.contains("c2VhbGVk"), "{text}");
        assert_eq!(v["has_secret"], true);
        assert_eq!(v["issuer"], "https://acme.okta.com");
        // The item keeps it sealed and reads back the same.
        let item = setup(&["acme.example"]).to_item(OKTA_SK);
        assert_eq!(OktaSetup::from_item(&item), Some(setup(&["acme.example"])));
    }

    fn rec(team: &str, sk: &str, status: &str, expires: Option<&str>) -> Item {
        let mut i = HashMap::from([
            ("pk".to_string(), attr_s(team)),
            ("sk".to_string(), attr_s(sk)),
        ]);
        if !status.is_empty() {
            i.insert("status".into(), attr_s(status));
        }
        if let Some(e) = expires {
            i.insert("invite_expires_at".into(), attr_s(e));
        }
        i
    }

    #[test]
    fn test_links_are_signed_and_short_lived() {
        let t = test_ticket("TEAM#1", "USER#a", "k").unwrap();
        let back = read_ticket(&t, "k").unwrap();
        assert_eq!((back.team.as_str(), back.by.as_str()), ("TEAM#1", "USER#a"));
        assert!(read_ticket(&t, "other").is_none());
        assert!(read_ticket("garbage", "k").is_none());
        // A flow cookie is not a test link.
        let f = flow_cookie(
            Provider::Okta,
            &Flow::new(Provider::Okta, "TEAM#1", true),
            "k",
        )
        .unwrap();
        let token = f.split(';').next().unwrap().split_once('=').unwrap().1;
        assert!(read_ticket(token, "k").is_none());
    }

    #[test]
    fn test_results_land_on_the_settings_page() {
        assert_eq!(
            test_result_url("https://app.coderhelm.com/", Ok(())),
            "https://app.coderhelm.com/settings/sign-in/?okta_test=passed#okta"
        );
        assert_eq!(
            test_result_url("https://coderhelm.acme.example", Err("token_exchange")),
            "https://coderhelm.acme.example/settings/sign-in/?okta_test=failed&reason=token_exchange#okta"
        );
    }

    #[test]
    fn the_authorize_url_encodes_every_value() {
        let u = authorize_url(
            "https://acme.okta.com/oauth2/v1/authorize",
            &[
                ("redirect_uri", "https://api.example.com/auth/okta/callback"),
                ("scope", "openid email profile"),
                ("login_hint", "a+b@acme.example"),
            ],
        );
        assert_eq!(
            u,
            "https://acme.okta.com/oauth2/v1/authorize?redirect_uri=https%3A%2F%2Fapi.example.com%2Fauth%2Fokta%2Fcallback&scope=openid%20email%20profile&login_hint=a%2Bb%40acme.example"
        );
    }

    #[test]
    fn microsoft_needs_both_halves_of_its_client() {
        assert!(microsoft_ready(Some("id"), Some("secret")));
        assert!(!microsoft_ready(None, Some("secret")));
        assert!(!microsoft_ready(Some("id"), None));
        assert!(!microsoft_ready(Some(" "), Some("secret")));
        assert!(!microsoft_ready(Some("id"), Some("\n")));
        assert_eq!(
            microsoft_redirect_uri("https://api.coderhelm.com/"),
            "https://api.coderhelm.com/auth/microsoft/callback"
        );
        assert_eq!(
            okta_redirect_uri("https://coderhelm-api.acme.example"),
            "https://coderhelm-api.acme.example/auth/okta/callback"
        );
    }

    // ── The review's attacks ────────────────────────────────────────

    use std::sync::Mutex;

    /// An identity provider that answers from a table and records what was sent.
    struct FakeIdp {
        answers: Vec<(String, common::mcp_remote::http::HttpResponse)>,
        sent: Mutex<Vec<common::mcp_remote::http::HttpRequest>>,
    }

    #[async_trait::async_trait]
    impl common::mcp_remote::http::Http for FakeIdp {
        async fn send(
            &self,
            req: common::mcp_remote::http::HttpRequest,
        ) -> Result<common::mcp_remote::http::HttpResponse, common::mcp_remote::http::HttpError>
        {
            self.sent.lock().unwrap().push(req.clone());
            self.answers
                .iter()
                .find(|(u, _)| *u == req.url)
                .map(|(_, r)| r.clone())
                .ok_or_else(|| common::mcp_remote::http::HttpError::Unreachable("no answer".into()))
        }
    }

    fn answer(status: u16, body: Value) -> common::mcp_remote::http::HttpResponse {
        common::mcp_remote::http::HttpResponse {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    /// Attack 1: whatever email an Okta session carries, it is never a
    /// platform admin.
    #[test]
    fn an_okta_session_is_never_a_platform_admin() {
        std::env::set_var("PLATFORM_ADMIN_EMAILS", "admin@coderhelm.example");
        let mut c = Claims {
            sub: "USER#Google_123".into(),
            team_id: "TEAM#t".into(),
            email: "admin@coderhelm.example".into(),
            role: "owner".into(),
            github_login: None,
            aud: "coderhelm".into(),
            exp: u64::MAX,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
            held: false,
            via_okta: false,
            okta_sub: None,
        };
        assert!(crate::routes::api::is_platform_admin(&c));
        c.held = true;
        assert!(!crate::routes::api::is_platform_admin(&c));
        c.held = false;
        c.sub = "USER#okta_00u1".into();
        assert!(!crate::routes::api::is_platform_admin(&c));
    }

    /// Attack 1: a domain is the team's only with its own TXT value, which
    /// no other team and no other domain shares.
    #[test]
    fn a_domain_is_proven_only_by_this_teams_txt_value() {
        let k = "deployment-key";
        let v = domain_txt_value(k, "TEAM#a", "acme.example");
        assert!(v.starts_with("coderhelm-okta=") && v.len() == "coderhelm-okta=".len() + 32);
        assert_ne!(v, domain_txt_value(k, "TEAM#b", "acme.example"));
        assert_ne!(v, domain_txt_value(k, "TEAM#a", "other.example"));
        assert_ne!(v, domain_txt_value("another-key", "TEAM#a", "acme.example"));
        assert_eq!(
            domain_txt_name("acme.example"),
            "_coderhelm-okta.acme.example"
        );
        assert!(txt_proves(&["v=spf1 -all".into(), v.clone()], &v));
        assert!(txt_proves(&[format!("\"{v}\"")], &v));
        assert!(!txt_proves(
            &[domain_txt_value(k, "TEAM#b", "acme.example")],
            &v
        ));
        assert!(!txt_proves(&[format!("{v}x")], &v));
        assert!(!txt_proves(&[], &v));
    }

    /// Attack 1 and 5: sign-in finds an Okta by the email's proven domain,
    /// never by the person's memberships, and never through an invite.
    #[test]
    fn okta_lookup_reads_no_memberships() {
        let src = include_str!("oidc_signin.rs");
        let body = &src[src.find("async fn okta_for_email(").unwrap()..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(!body.contains("records_by_email"), "lookup by membership");
        assert!(body.contains("domain_owner("));
        // The callback checks the token's email domain is proven by this team.
        let cb = &src[src.find("async fn okta_land(").unwrap()..];
        let cb = &cb[..cb.find("\n}\n").unwrap()];
        assert!(cb.find("domain_owner(").unwrap() < cb.find("finish_oidc(").unwrap());
        // Turning Okta on (save or switch) needs every domain proven.
        for f in ["pub async fn okta_save(", "pub async fn okta_toggle("] {
            let b = &src[src.find(f).unwrap()..];
            let b = &b[..b.find("\n}\n").unwrap()];
            assert!(b.contains("require_proven("), "{f}");
        }
    }

    /// Attack 3: every request to an identity provider goes through the
    /// guarded transport, which refuses private and link-local addresses.
    #[tokio::test]
    async fn the_identity_provider_transport_refuses_private_addresses() {
        for issuer in [
            "https://127.0.0.1",
            "https://169.254.169.254",
            "https://10.0.0.5/oauth2/default",
            "https://[::1]",
        ] {
            let e = discover(idp(), issuer).await.unwrap_err();
            assert!(!e.is_empty(), "{issuer}");
            assert!(exchange(
                idp(),
                &format!("{issuer}/token"),
                "c",
                "s",
                ClientAuth::Basic,
                "code",
                "r",
                "v"
            )
            .await
            .is_err());
        }
        let src = include_str!("oidc_signin.rs");
        let code = &src[..src.find("#[cfg(test)]\nmod tests").unwrap()];
        assert!(!code.contains("reqwest::Client"), "no unguarded client");
        assert!(code.contains("GuardedHttps"));
    }

    /// The token request: Okta's secret goes only in the Basic header, form
    /// encoded; Microsoft's in the body; PKCE always.
    #[tokio::test]
    async fn the_token_request_carries_pkce_and_the_secret_where_it_belongs() {
        let fake = FakeIdp {
            answers: vec![(
                "https://acme.okta.example/token".into(),
                answer(200, json!({ "id_token": "t" })),
            )],
            sent: Mutex::new(Vec::new()),
        };
        let t = exchange(
            &fake,
            "https://acme.okta.example/token",
            "id:1",
            "s&c=t",
            ClientAuth::Basic,
            "C",
            "R",
            "V",
        )
        .await
        .unwrap();
        assert_eq!(t, "t");
        let req = fake.sent.lock().unwrap()[0].clone();
        assert!(!req.body_text().contains("client_secret"));
        assert!(req.body_text().contains("code_verifier=V"));
        let basic = req.header_value("Authorization").unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(basic.trim_start_matches("Basic "))
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "id%3A1:s%26c%3Dt");
        // A refusal names the provider's error, never the secret.
        let fake = FakeIdp {
            answers: vec![(
                "https://x.example/token".into(),
                answer(401, json!({ "error": "invalid_client" })),
            )],
            sent: Mutex::new(Vec::new()),
        };
        let e = exchange(
            &fake,
            "https://x.example/token",
            "id",
            "shh",
            ClientAuth::Post,
            "C",
            "R",
            "V",
        )
        .await
        .unwrap_err();
        assert!(e.contains("invalid_client") && !e.contains("shh"), "{e}");
        assert!(fake.sent.lock().unwrap()[0]
            .body_text()
            .contains("client_secret=shh"));
    }

    /// Attack 4: a saved secret is never reused for another issuer or client.
    #[test]
    fn a_saved_secret_follows_only_its_own_issuer_and_client() {
        let saved = setup(&["acme.example"]);
        let both = [Some(saved.clone()), None];
        assert_eq!(
            kept_secret(&both, "https://acme.okta.com", "0oa1").as_deref(),
            Some("c2VhbGVk")
        );
        assert!(kept_secret(&both, "https://evil.example", "0oa1").is_none());
        assert!(kept_secret(&both, "https://acme.okta.com", "0oa-other").is_none());
        assert!(kept_secret(&[None, None], "https://acme.okta.com", "0oa1").is_none());
        // The live setup's secret counts when the draft is for another client.
        let mut draft = saved.clone();
        draft.client_id = "0oa-new".into();
        draft.secret_sealed = "bmV3".into();
        assert_eq!(
            kept_secret(&[Some(draft), Some(saved)], "https://acme.okta.com", "0oa1").as_deref(),
            Some("c2VhbGVk")
        );
    }

    /// Attack 5: a Test sign-in link works only for the admin who made it.
    #[test]
    fn a_test_link_is_bound_to_its_admin() {
        let t = TestTicket {
            team: "TEAM#1".into(),
            by: "USER#admin".into(),
            aud: TEST_AUD.into(),
            exp: u64::MAX,
        };
        let mut c = Claims {
            sub: "USER#admin".into(),
            team_id: "TEAM#1".into(),
            email: "a@acme.example".into(),
            role: "admin".into(),
            github_login: None,
            aud: "coderhelm".into(),
            exp: u64::MAX,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
            held: false,
            via_okta: false,
            okta_sub: None,
        };
        assert!(ticket_is_theirs(&t, Some(&c)));
        assert!(!ticket_is_theirs(&t, None));
        c.sub = "USER#someone".into();
        assert!(!ticket_is_theirs(&t, Some(&c)));
        c.sub = "USER#admin".into();
        c.team_id = "TEAM#2".into();
        assert!(!ticket_is_theirs(&t, Some(&c)));
    }

    // ── Second review ───────────────────────────────────────────────

    fn linked(team: &str, sk: &str, email: &str, issuer: &str, sub: &str, status: &str) -> Item {
        let mut i = rec(team, sk, status, None);
        i.insert("email".into(), attr_s(email));
        if !issuer.is_empty() {
            i.insert("okta_issuer".into(), attr_s(issuer));
            i.insert("okta_sub".into(), attr_s(sub));
        }
        i
    }

    /// Finding 1: an admin's own Okta asserting the owner's email lands on
    /// nothing. Only the record that linked exactly this issuer and subject.
    #[test]
    fn okta_lands_only_on_the_member_who_linked_that_identity() {
        let iss = "https://acme.okta.com";
        let members = vec![
            linked(
                "TEAM#o",
                "USER#owner",
                "owner@acme.example",
                "",
                "",
                "active",
            ),
            linked(
                "TEAM#o",
                "USER#ann",
                "ann@acme.example",
                iss,
                "00uANN",
                "active",
            ),
            linked(
                "TEAM#o",
                "USER#invite_1",
                "bo@acme.example",
                iss,
                "00uBO",
                "invited",
            ),
        ];
        // Same email as the owner, no link: nothing (email is never used).
        assert!(linked_record(&members, iss, "00uOWNER").is_none());
        assert_eq!(
            s(linked_record(&members, iss, "00uANN").unwrap(), "sk"),
            "USER#ann"
        );
        // Another issuer with the same subject: nothing.
        assert!(linked_record(&members, "https://evil.okta.com", "00uANN").is_none());
        // An invite never, an empty identity never.
        assert!(linked_record(&members, iss, "00uBO").is_none());
        assert!(linked_record(&members, iss, "").is_none());
        assert!(linked_record(&members, "", "00uANN").is_none());
        // The callback lands through the link, never through the email.
        let src = include_str!("oidc_signin.rs");
        let cb = &src[src.find("async fn okta_land(").unwrap()..];
        let cb = &cb[..cb.find("\n}\n").unwrap()];
        assert!(cb.contains("linked_record("));
        assert!(!cb.contains("records_by_email"));
        // Lands only on the link; without one, the not-linked refusal,
        // unless the team requires Okta and an invite for exactly this email
        // is accepted (and linked) first (routes::require_okta).
        let first = cb.find("finish_oidc(").unwrap();
        assert!(cb[..first].contains("if let Some(record) = linked_record("));
        let no_link = cb.find("Reason::OktaNotLinked").unwrap();
        let invite = cb.find("require_okta::accept_invite(").unwrap();
        let second = cb.rfind("finish_oidc(").unwrap();
        assert!(first < no_link && no_link < invite && invite < second);
        assert!(cb.find("if !rule.require_okta").unwrap() < no_link);
        // The hosted domain proof comes before any of it.
        assert!(cb.find("fresh_domain_owner(").unwrap() < first);
    }

    fn claims_for(sub: &str, team: &str, role: &str) -> Claims {
        Claims {
            sub: sub.into(),
            team_id: team.into(),
            email: "a@acme.example".into(),
            role: role.into(),
            github_login: None,
            aud: "coderhelm".into(),
            exp: u64::MAX,
            iat: 0,
            support_view: None,
            custom: None,
            api_key: None,
            held: false,
            via_okta: false,
            okta_sub: None,
        }
    }

    /// Finding 1: a link is made by the member themselves, in their own
    /// browser, from a session they started another way.
    #[test]
    fn only_the_member_links_from_a_session_started_another_way() {
        let t = TestTicket {
            team: "TEAM#o".into(),
            by: "USER#ann".into(),
            aud: LINK_AUD.into(),
            exp: u64::MAX,
        };
        let mine = (
            claims_for("USER#ann", "TEAM#o", "member"),
            Some("password".to_string()),
        );
        assert!(may_link(&t, Some(&mine)));
        let from_okta = (
            claims_for("USER#ann", "TEAM#o", "member"),
            Some("okta".to_string()),
        );
        assert!(!may_link(&t, Some(&from_okta)));
        let admin = (
            claims_for("USER#admin", "TEAM#o", "admin"),
            Some("password".to_string()),
        );
        assert!(!may_link(&t, Some(&admin)));
        assert!(!may_link(&t, None));
        // A link ticket is not a test ticket, nor the other way round.
        let link = link_ticket("TEAM#o", "USER#ann", "k").unwrap();
        assert!(read_ticket(&link, "k").is_none());
        assert!(read_signed_ticket(&link, LINK_AUD, "k").is_some());
        // Links are dropped when the issuer changes or Okta is removed.
        for f in ["pub async fn okta_save(", "pub async fn okta_delete("] {
            let src = include_str!("oidc_signin.rs");
            let b = &src[src.find(f).unwrap()..];
            let b = &b[..b.find("\n}\n").unwrap()];
            assert!(b.contains("drop_links("), "{f}");
        }
    }

    /// Finding 1: on a self-hosted install only the owner saves or turns on
    /// Okta; custom roles never reach the setup.
    #[test]
    fn self_hosted_okta_is_the_owners_and_never_a_custom_roles() {
        assert!(owner_on_self_host(&claims_for("U", "T", "admin"), true).is_err());
        assert!(owner_on_self_host(&claims_for("U", "T", "owner"), true).is_ok());
        // Third review: owner-only on hosted too.
        assert!(owner_on_self_host(&claims_for("U", "T", "admin"), false).is_err());
        assert!(owner_on_self_host(&claims_for("U", "T", "owner"), false).is_ok());
        let cap = crate::permissions::CAPABILITIES
            .iter()
            .find(|c| c.handlers.contains(&"oidc_signin::okta_save"))
            .unwrap();
        assert_eq!(cap.key, "sign_in");
        assert!(crate::permissions::NEVER_IN_CUSTOM_ROLES.contains(&cap.key));
        for h in [
            "oidc_signin::okta_get",
            "oidc_signin::okta_test",
            "oidc_signin::okta_toggle",
            "oidc_signin::okta_delete",
            "oidc_signin::okta_verify_domains",
            "oidc_signin::okta_release_domain",
        ] {
            assert!(cap.handlers.contains(&h), "{h}");
        }
    }

    /// Finding 2: discovery and keys never follow a redirect.
    #[tokio::test]
    async fn discovery_and_keys_never_follow_a_redirect() {
        let fake = FakeIdp {
            answers: vec![(
                "https://acme.okta.example/.well-known/openid-configuration".into(),
                answer(200, json!({})),
            )],
            sent: Mutex::new(Vec::new()),
        };
        let _ = discover(&fake, "https://acme.okta.example").await;
        let _ = keys(&fake, "https://acme.okta.example/keys").await;
        let sent = fake.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|r| !r.follow_redirects));
    }

    /// Finding 3: proofs are read again after a day, and their key is a
    /// subkey of the session key, never the session key itself.
    #[test]
    fn domain_proofs_expire_and_use_their_own_key() {
        let now = chrono::Utc::now();
        assert!(!proof_stale(
            &(now - chrono::Duration::hours(23)).to_rfc3339(),
            now
        ));
        assert!(proof_stale(
            &(now - chrono::Duration::hours(25)).to_rfc3339(),
            now
        ));
        assert!(proof_stale("", now));
        assert!(proof_stale("garbage", now));
        // The value is not an HMAC under the session key itself.
        use hmac::{Hmac, Mac};
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(b"k").unwrap();
        mac.update(b"okta-domain|TEAM#a|acme.example");
        let direct = hex::encode(mac.finalize().into_bytes());
        assert_ne!(
            domain_txt_value("k", "TEAM#a", "acme.example"),
            format!("coderhelm-okta={}", &direct[..32])
        );
        assert_ne!(proof_key("k"), b"k".to_vec());
        // Turning on reads DNS again; dropped domains and removal release.
        let src = include_str!("oidc_signin.rs");
        let proven = &src[src.find("async fn all_domains_proven(").unwrap()..];
        assert!(
            proven[..proven.find("\n}\n").unwrap()].contains("fresh_domain_owner(state, d, true)")
        );
        let save = &src[src.find("pub async fn okta_save(").unwrap()..];
        assert!(save[..save.find("\n}\n").unwrap()].contains("release_domain_while_rule_off("));
        assert!(src.contains("pub async fn okta_release_domain("));
    }

    /// Findings 4: a held session may set up a second factor when the person
    /// is on no other team; self-hosted sessions are never held.
    #[test]
    fn no_dead_end_for_two_factor_and_no_hold_on_self_host() {
        for h in FACTOR_SETUP {
            assert!(!held_refuses(h, false), "{h} for someone on this team only");
            assert!(held_refuses(h, true), "{h} for someone on other teams");
        }
        for h in [
            "api::switch_team",
            "users::link_github",
            "users::change_password",
            "sign_in::app_disable",
        ] {
            assert!(held_refuses(h, false), "{h}");
        }
        assert!(!held_refuses("api::me", true));
        assert!(is_held(Some("okta"), false));
        assert!(
            !is_held(Some("okta"), true),
            "self-hosted: one organization, no hold"
        );
        assert!(!is_held(Some("google"), false));
        let mw = include_str!("../middleware/auth.rs");
        assert!(
            mw.find("claims.held =").unwrap() < mw.find("support_access::check_session").unwrap()
        );
        assert!(mw.contains("oidc_signin::is_held("));
        assert!(mw.contains("held_refusal(&state, &claims, handler)"));
        let auth = include_str!("auth.rs");
        let gh = &auth[auth.find("async fn github_callback_inner(").unwrap()..];
        assert!(
            gh.find("Reason::TeamOnly").unwrap()
                < gh.find("connect GitHub to their account").unwrap()
        );
    }

    /// Finding 4: personal accounts follow the same documented rule.
    #[test]
    fn personal_microsoft_accounts_need_xms_edov_too() {
        let mut c = ms_claims();
        c["tid"] = json!(MS_CONSUMER_TENANT);
        c["email"] = json!("ada@outlook.com");
        assert_eq!(microsoft_email(&c).unwrap(), "ada@outlook.com");
        c.as_object_mut().unwrap().remove("xms_edov");
        assert!(microsoft_email(&c).is_err());
    }

    // ── Third review ────────────────────────────────────────────────

    fn body_src(f: &str) -> &'static str {
        let src: &'static str = include_str!("oidc_signin.rs");
        let b = &src[src.find(f).unwrap_or_else(|| panic!("{f}"))..];
        &b[..b.find("\n}\n").unwrap()]
    }

    fn live(issuer: &str, enabled: bool) -> OktaSetup {
        OktaSetup {
            team_id: "TEAM#o".into(),
            issuer: issuer.into(),
            enabled,
            ..Default::default()
        }
    }

    /// 1: an open Okta session ends when the link, the issuer or Okta itself
    /// goes; the member's other sessions are not checked this way.
    #[test]
    fn okta_sessions_end_with_their_link_or_setup() {
        let iss = "https://acme.okta.com";
        let mut m = linked(
            "TEAM#o",
            "USER#ann",
            "ann@acme.example",
            iss,
            "00uANN",
            "active",
        );
        assert!(okta_session_state(Some(&m), Some(&live(iss, true)), 100, Some("00uANN")).is_ok());
        assert!(
            okta_session_state(Some(&m), Some(&live(iss, false)), 100, Some("00uANN")).is_err(),
            "Okta off"
        );
        assert!(
            okta_session_state(
                Some(&m),
                Some(&live("https://new.okta.com", true)),
                100,
                Some("00uANN")
            )
            .is_err(),
            "issuer changed"
        );
        assert!(
            okta_session_state(Some(&m), None, 100, Some("00uANN")).is_err(),
            "Okta removed"
        );
        m.insert(
            "okta_valid_after".into(),
            aws_sdk_dynamodb::types::AttributeValue::N("200".into()),
        );
        assert!(
            okta_session_state(Some(&m), Some(&live(iss, true)), 100, Some("00uANN")).is_err(),
            "relinked or ended since"
        );
        assert!(okta_session_state(Some(&m), Some(&live(iss, true)), 300, Some("00uANN")).is_ok());
        let unlinked = linked("TEAM#o", "USER#ann", "ann@acme.example", "", "", "active");
        assert!(
            okta_session_state(Some(&unlinked), Some(&live(iss, true)), 300, Some("00uANN"))
                .is_err(),
            "unlinked"
        );
        // Only Okta sessions are checked against the link.
        let si = include_str!("sign_in.rs");
        let cms = &si[si.find("pub async fn check_member_session(").unwrap()..];
        let cms = &cms[..cms.find("\n}\n").unwrap()];
        assert!(cms.find("if claims.via_okta").unwrap() < cms.find("okta_session_state(").unwrap());
        // Unlink, dropping links and turning off all end Okta sessions.
        assert!(body_src("async fn unlink_member(").contains("okta_valid_after"));
        assert!(body_src("async fn drop_links(").contains("unlink_member("));
        assert!(body_src("pub async fn link_remove(").contains("unlink_member("));
        assert!(body_src("pub async fn okta_toggle(").contains("end_okta_sessions("));
    }

    /// 2: only a definite DNS answer releases a proof.
    #[test]
    fn a_failed_dns_read_keeps_the_proof() {
        let v = "coderhelm-okta=1";
        assert_eq!(proof_check(&Ok(vec![v.into()]), v), ProofCheck::There);
        assert_eq!(
            proof_check(&Ok(vec![]), v),
            ProofCheck::Gone,
            "NXDOMAIN or no record"
        );
        assert_eq!(proof_check(&Ok(vec!["other".into()]), v), ProofCheck::Gone);
        assert_eq!(
            proof_check(&Err("no answer".into()), v),
            ProofCheck::Unknown
        );
        let fresh = body_src("async fn fresh_domain_owner(");
        let unknown = fresh.find("ProofCheck::Unknown").unwrap();
        assert!(unknown < fresh.find("release_domain_while_rule_off(").unwrap());
        assert!(
            fresh[unknown..].starts_with("ProofCheck::Unknown {")
                || fresh.contains("return Ok(Some(team));")
        );
    }

    /// 3: Okta sessions are never platform admins, on any deployment; and a
    /// re-issued session keeps its way in.
    #[test]
    fn okta_is_never_platform_admin_even_unheld() {
        std::env::set_var("PLATFORM_ADMIN_EMAILS", "admin@coderhelm.example");
        let mut c = claims_for("USER#Google_1", "TEAM#t", "owner");
        c.email = "admin@coderhelm.example".into();
        assert!(crate::routes::api::is_platform_admin(&c));
        c.via_okta = true;
        c.held = false;
        assert!(
            !crate::routes::api::is_platform_admin(&c),
            "self-hosted Okta session"
        );
        let users = include_str!("users.rs");
        let mfa = &users[users.find("pub async fn mfa_verify_setup(").unwrap()..];
        let mfa = &mfa[..mfa.find("\n}\n").unwrap()];
        assert!(!mfa.contains("session_with_second_factor(&state, &claims, \"password\")"));
        // The current session's whole proof goes on (its way in included).
        assert!(
            mfa.contains("session_with_second_factor(")
                && mfa.contains("proof.as_ref().map(|p| &p.0)")
        );
        let si = include_str!("sign_in.rs");
        let f = &si[si.find("pub fn session_with_second_factor(").unwrap()..];
        assert!(f[..f.find("\n}\n").unwrap()].contains("p.via.clone()"));
    }

    /// 4: one member per Okta identity, enforced by a row written in the
    /// same transaction as the link.
    #[test]
    fn an_okta_identity_is_linked_once_per_team() {
        assert_eq!(
            link_row("TEAM#o", "https://acme.okta.com", "00u1"),
            (
                "OKTA_LINK#TEAM#o".to_string(),
                "https://acme.okta.com#00u1".to_string()
            )
        );
        let tx = body_src("async fn link_transaction(");
        assert!(tx.contains("transact_write_items()"));
        assert!(tx.contains("attribute_not_exists(pk)"));
        assert!(body_src("async fn unlink_member(").contains("delete_link_row("));
        assert!(body_src("async fn okta_link_return(").contains("link_transaction("));
    }

    /// 5: the owner saves exactly the draft they looked at.
    #[test]
    fn save_promotes_only_the_draft_the_owner_saw() {
        let mut d = setup(&["acme.example"]);
        d.updated_at = Some("2026-11-20T10:00:00Z".into());
        let body = |i: &str, c: &str, u: &str| OktaSaveBody {
            enabled: true,
            issuer: Some(i.into()),
            client_id: Some(c.into()),
            updated_at: Some(u.into()),
        };
        assert!(draft_matches(
            &d,
            &body("https://acme.okta.com", "0oa1", "2026-11-20T10:00:00Z")
        ));
        assert!(!draft_matches(
            &d,
            &body("https://evil.okta.com", "0oa1", "2026-11-20T10:00:00Z")
        ));
        assert!(!draft_matches(
            &d,
            &body("https://acme.okta.com", "0oa2", "2026-11-20T10:00:00Z")
        ));
        assert!(!draft_matches(
            &d,
            &body("https://acme.okta.com", "0oa1", "2026-11-20T10:05:00Z")
        ));
        let none = OktaSaveBody {
            enabled: true,
            issuer: None,
            client_id: None,
            updated_at: None,
        };
        assert!(!draft_matches(&d, &none));
    }

    /// 6: a draft's domain claims go with it, unless live still lists them;
    /// verify re-reads another team's proof.
    #[test]
    fn draft_domain_claims_do_not_outlive_the_draft() {
        let old = setup(&["a.example", "b.example", "c.example"]);
        let new = setup(&["a.example"]);
        let lv = setup(&["b.example"]);
        assert_eq!(
            released_by_draft(&old, Some(&new), Some(&lv)),
            vec!["c.example"]
        );
        assert_eq!(released_by_draft(&old, None, None).len(), 3);
        assert!(body_src("pub async fn okta_discard_draft(").contains("released_by_draft("));
        assert!(body_src("pub async fn okta_test(").contains("released_by_draft("));
        assert!(body_src("pub async fn okta_verify_domains(")
            .contains("fresh_domain_owner(&state, d, true)"));
    }

    /// 7: the link and test pages check the session like every request, and
    /// a held record without an email can't set up a factor.
    #[test]
    fn session_checks_and_no_email_factor_setup() {
        assert!(body_src("async fn session_of(").contains("check_member_session("));
        assert!(body_src("pub async fn held_refusal(").contains("claims.email.contains('@')"));
    }

    // ── Final review ────────────────────────────────────────────────

    /// 1: re-linking to another Okta account ends the old account's
    /// sessions: by the subject each session carries, and by
    /// `okta_valid_after` on the re-link itself.
    #[test]
    fn relinking_ends_the_previous_accounts_sessions() {
        let iss = "https://acme.okta.com";
        // The member re-linked from 00uOLD to 00uNEW.
        let m = linked(
            "TEAM#o",
            "USER#ann",
            "ann@acme.example",
            iss,
            "00uNEW",
            "active",
        );
        let on = live(iss, true);
        assert!(okta_session_state(Some(&m), Some(&on), 100, Some("00uNEW")).is_ok());
        assert!(
            okta_session_state(Some(&m), Some(&on), 100, Some("00uOLD")).is_err(),
            "old account"
        );
        assert!(
            okta_session_state(Some(&m), Some(&on), 100, None).is_err(),
            "no subject"
        );
        // The re-link path: a previous link means the update also sets okta_valid_after.
        assert!(link_update_expression(true).contains("okta_valid_after = :n"));
        assert!(!link_update_expression(false).contains("okta_valid_after"));
        let tx = body_src("async fn link_transaction(");
        assert!(tx.contains("link_update_expression(previous.is_some())"));
        assert!(tx.contains("if previous.is_some()"));
        let ret = body_src("async fn okta_link_return(");
        assert!(ret.contains(
            "link_transaction(state, &flow.team, &flow.link, &live.issuer, sub, previous)"
        ));
        // The session token carries the subject, and the check reads it.
        let proof =
            crate::auth::jwt::SignInProof::new(false, "okta").with_okta_sub(Some("00uNEW".into()));
        let t = crate::auth::jwt::create_session_token(
            "USER#ann",
            "TEAM#o",
            "ann@acme.example",
            "member",
            None,
            &proof,
            "k",
            600,
        )
        .unwrap();
        assert_eq!(
            crate::auth::jwt::read_proof(&t, "k").okta_sub.as_deref(),
            Some("00uNEW")
        );
        let si = include_str!("sign_in.rs");
        assert!(si.contains("claims.okta_sub.as_deref()"));
        let auth = include_str!("auth.rs");
        let fin = &auth[auth.find("pub(crate) async fn finish_oidc(").unwrap()..];
        let fin = &fin[..fin.find("\n}\n").unwrap()];
        assert!(fin.contains("with_okta_sub(okta_sub.clone())"));
        assert!(fin.contains("okta_sub: okta_sub.clone()"));
    }

    /// 2: link rows go with their member and with the team's Okta.
    #[test]
    fn link_rows_do_not_outlive_their_member() {
        let users = include_str!("users.rs");
        let rm = &users[users.find("pub async fn remove_user(").unwrap()..];
        let rm = &rm[..rm.find("\n}\n").unwrap()];
        assert!(rm.contains("oidc_signin::delete_link_row("));
        // drop_links (used by okta_delete and an issuer change) deletes the
        // team's rows directly, members or not.
        assert!(body_src("async fn drop_links(").contains("delete_team_link_rows("));
        assert!(body_src("pub async fn okta_delete(").contains("drop_links("));
        assert_eq!(link_rows_pk("TEAM#o"), link_row("TEAM#o", "i", "s").0);
        assert_eq!(
            legacy_link_row_key("TEAM#o", "i", "s"),
            "OKTA_LINK#TEAM#o#i#s"
        );
    }

    /// Self-hosted: an Okta landing still goes through the join rules, and
    /// only onto the linked member's own record.
    #[test]
    fn self_hosted_okta_passes_the_join_rules() {
        use crate::routes::auth::okta_landing_matches;
        let r = linked("TEAM#org", "USER#ann", "ann@acme.example", "", "", "active");
        assert!(okta_landing_matches(Some(&r), "TEAM#org", "USER#ann"));
        assert!(!okta_landing_matches(Some(&r), "TEAM#org", "USER#bo"));
        assert!(!okta_landing_matches(None, "TEAM#org", "USER#ann"));
        let auth = include_str!("auth.rs");
        let fin = &auth[auth.find("pub(crate) async fn finish_oidc(").unwrap()..];
        let okta_branch = &fin[fin.find("if let Some(record) = okta").unwrap()..];
        assert!(
            okta_branch.find("selfhost_landing(").unwrap()
                < okta_branch.find("} else if sign_in::applies()").unwrap()
        );
    }
}
