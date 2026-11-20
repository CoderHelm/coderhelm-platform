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
//! 3. The email comes only from a claim the provider vouches for (below),
//!    and then the sign-in goes through the same gates as Google and GitHub
//!    (auth::finish_oidc): decide_join on a self-hosted install, the
//!    person's member records by email, and their second factor.
//!
//! # Which email is trusted
//!
//! - **Microsoft, personal account** (the consumer tenant
//!   `9188040d-6c67-4c5b-b112-36a304b66dad`): the `email` claim. A Microsoft
//!   account's email is the address it signs in with.
//! - **Microsoft, work or school account** (any other tenant): an Entra
//!   tenant can put any address in `email`, including one at a domain it
//!   doesn't own, and `preferred_username` is a display hint the tenant can
//!   change. So the `email` claim counts only when `xms_edov` (email domain
//!   owner verified) is `true` in the same token, else the first
//!   `verified_primary_email` (an address at one of the tenant's verified
//!   domains). `preferred_username` is never used. With neither, sign-in
//!   stops with `no_verified_email`. The app registration must list the
//!   optional claims `email`, `xms_edov` and `verified_primary_email` for the
//!   ID token (docs/self-host/microsoft-okta-sign-in.md).
//! - **Okta**: the `email` claim, only with `email_verified: true`.
//!
//! # Okta is per organization
//!
//! There is no global Okta client. Each hosted team (and a self-hosted
//! install's one organization) stores its own issuer, client id and client
//! secret (sealed with the deployment's KMS key, like a two-factor secret),
//! in the settings table at `pk = <team>`, `sk = SETTINGS#OKTA`. An admin
//! saves a draft (`SETTINGS#OKTA_DRAFT`), runs **Test sign-in** against it,
//! and only a draft whose test passed becomes the live setup. While a team's
//! Okta sign-in is on, a row `pk = OKTA_TEAMS`, `sk = <team>` tells
//! `/auth/methods` to show the button.
//!
//! On the hosted deployment an Okta organization only vouches for people on
//! its own team: the email has to be on that team already (or invited to
//! it), and the session it makes is held to that team (no switching to the
//! person's other teams; routes::api::switch_team).

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

pub fn okta_person(claims: &Value) -> Result<Person, &'static str> {
    let email = okta_email(claims)?;
    Ok(Person {
        email,
        name: claims["name"].as_str().unwrap_or_default().to_string(),
        new_user_id: format!("USER#okta_{}", claims["sub"].as_str().unwrap_or_default()),
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
    let resp = http
        .send(HttpRequest::get(url))
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
    super::auth::finish_oidc(state, ip, P, &person, None).await
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
const OKTA_DOMAIN_SK: &str = "OKTA";

/// The TXT record's name: `_coderhelm-okta.<domain>`.
pub fn domain_txt_name(domain: &str) -> String {
    format!("_coderhelm-okta.{domain}")
}

/// The TXT record's value for one team and one domain: an HMAC of both
/// with the deployment's key, so nobody else can work it out and it never
/// needs storing.
pub fn domain_txt_value(secret: &str, team_id: &str, domain: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .expect("HMAC takes any key length");
    mac.update(format!("okta-domain|{team_id}|{domain}").as_bytes());
    let tag = hex::encode(mac.finalize().into_bytes());
    format!("coderhelm-okta={}", &tag[..32])
}

/// Whether any of a name's TXT strings is exactly the expected value.
pub fn txt_proves(records: &[String], expected: &str) -> bool {
    records
        .iter()
        .any(|r| r.trim().trim_matches('"') == expected)
}

/// The team that proved `domain`, if any.
async fn domain_owner(state: &AppState, domain: &str) -> Result<Option<(String, String)>, String> {
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
        .map(|i| (s(i, "team_id").to_string(), s(i, "verified_at").to_string()))
        .filter(|(t, _)| !t.is_empty()))
}

/// Whether every domain of a hosted setup is proven by its own team.
async fn all_domains_proven(state: &AppState, setup: &OktaSetup) -> Result<bool, String> {
    if setup.domains.is_empty() {
        return Ok(false);
    }
    for d in &setup.domains {
        if domain_owner(state, d).await?.map(|(t, _)| t) != Some(setup.team_id.clone()) {
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
    let Some((team, _)) = domain_owner(state, &domain).await? else {
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

async fn start_okta(
    state: &AppState,
    setup: &OktaSetup,
    test: bool,
    login_hint: Option<&str>,
) -> Result<Response, SignInFailure> {
    const P: Provider = Provider::Okta;
    let d = discover(idp(), &setup.issuer)
        .await
        .map_err(|e| SignInFailure::new(P, Reason::Provider, "discovery", e))?;
    let flow = Flow::new(P, &setup.team_id, test);
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
        // A test asks for the password again, so it proves the whole sign-in.
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
    let person = okta_person(&claims)
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
        let owner = domain_owner(state, &domain)
            .await
            .map_err(|e| SignInFailure::new(P, Reason::Server, "domain", e))?;
        if owner.map(|(t, _)| t).as_deref() != Some(setup.team_id.as_str()) {
            return Err(SignInFailure::new(
                P,
                Reason::NotOnTeam,
                "domain",
                "the email's domain is not proven by this team",
            ));
        }
    }
    let team = (!super::sign_in::applies()).then_some(setup.team_id.as_str());
    super::auth::finish_oidc(state, ip, P, &person, team).await
}

// ── Okta: Test sign-in ──────────────────────────────────────────────

/// An admin's Test sign-in link: signed, for one team's draft, short-lived.
#[derive(Debug, Serialize, Deserialize)]
struct TestTicket {
    team: String,
    by: String,
    aud: String,
    exp: u64,
}

const TEST_AUD: &str = "okta_test";

fn test_ticket(team: &str, by: &str, secret: &str) -> Result<String, String> {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &TestTicket {
            team: team.to_string(),
            by: by.to_string(),
            aud: TEST_AUD.into(),
            exp: chrono::Utc::now().timestamp() as u64 + TEST_TICKET_SECS,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| e.to_string())
}

fn read_ticket(token: &str, secret: &str) -> Option<TestTicket> {
    let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    v.set_audience(&[TEST_AUD]);
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
    let session = oauth_signin::cookie_value(
        super::auth::cookie_header(&headers),
        "__Host-coderhelm_session",
    )
    .and_then(|tok| crate::auth::jwt::validate_token(tok, &state.secrets.jwt_secret).ok());
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
    match start_okta(&state, &draft, true, None).await {
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
        .update_expression("SET tested_at = :t, tested_email = :e")
        // Only the draft that was tested: one saved since then needs its own test.
        .condition_expression("issuer = :i AND client_id = :c AND client_secret_sealed = :s")
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":e", attr_s(&email))
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
            proofs.push(json!({
                "domain": d,
                "txt_name": domain_txt_name(d),
                "txt_value": domain_txt_value(&state.secrets.jwt_secret, team, d),
                "verified_at": owner.filter(|_| ours).map(|(_, at)| at),
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
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        updated_by: Some(claims.display_name()),
    };
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
    let team = setup_team(&state, &claims).await?;
    let Some(draft) = load_setup(&state, &team, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not read", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "There's nothing new to save."));
    };
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
    put_setup(&state, &live, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not saved", e))?;
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
    put_setup(&state, &next, OKTA_SK)
        .await
        .map_err(|e| internal("Okta setup not saved", e))?;
    set_marker(&state, &team, next.enabled)
        .await
        .map_err(|e| internal("Okta marker not saved", e))?;
    Ok(Json(okta_view(&state, &team).await?))
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

/// DELETE /api/sign-in/okta: remove the setup and any draft. People who
/// signed in with Okta keep their accounts and sign in another way.
pub async fn okta_delete(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let team = setup_team(&state, &claims).await?;
    audit(&state, &claims, "Okta sign-in removed".into()).await?;
    // The team's proven domains are released, so their owner can use them again.
    for sk in [OKTA_SK, OKTA_DRAFT_SK] {
        if let Ok(Some(setup)) = load_setup(&state, &team, sk).await {
            for d in &setup.domains {
                if matches!(domain_owner(&state, d).await, Ok(Some((t, _))) if t == team) {
                    let _ =
                        delete_setup(&state, &format!("{OKTA_DOMAIN_PREFIX}{d}"), OKTA_DOMAIN_SK)
                            .await;
                }
            }
        }
    }
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
    delete_setup(&state, &team, OKTA_DRAFT_SK)
        .await
        .map_err(|e| internal("Okta draft not removed", e))?;
    Ok(Json(okta_view(&state, &team).await?))
}

/// A session a team's (or the organization's) Okta signed in.
pub fn signed_in_with_okta(via: Option<&str>) -> bool {
    via == Some("okta")
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

pub const TEAM_ONLY_MESSAGE: &str = "You signed in with your team's Okta, which works for this team only. Sign in with your password, Google, GitHub or Microsoft to do this.";

/// The refusal for a held session asking for a person-wide handler.
pub fn held_refusal(held: bool, handler: Option<&str>) -> Option<Response> {
    let refused = held
        && !super::setup_checks::self_hosted()
        && handler.is_some_and(|h| HELD_REFUSED.contains(&h));
    refused.then(|| {
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
        assert_eq!(okta_person(&c).unwrap().new_user_id, "USER#okta_00u1");
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

    /// Attack 1: a team admin invites admin@ their target and has their own
    /// Okta assert that email. An invite is never landed on, only an active
    /// member record on the Okta's own team.
    #[test]
    fn hosted_okta_never_lands_on_an_invite() {
        use crate::routes::auth::okta_landing;
        let invite = rec("TEAM#attacker", "USER#invite_1", "invited", None);
        let other = rec("TEAM#victim", "USER#Google_1", "active", None);
        assert!(okta_landing(&[invite.clone(), other.clone()], "TEAM#attacker").is_none());
        let member = rec("TEAM#attacker", "USER#Google_1", "active", None);
        let all = [invite, other, member];
        let landed = okta_landing(&all, "TEAM#attacker").unwrap();
        assert_eq!(s(landed, "pk"), "TEAM#attacker");
        assert_eq!(s(landed, "sk"), "USER#Google_1");
        // A record with no status (older records) is an active member.
        let plain = rec("TEAM#t", "USER#abc", "", None);
        assert!(okta_landing(&[plain], "TEAM#t").is_some());
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
        let cb = &src[src.find("async fn okta_callback_inner(").unwrap()..];
        let cb = &cb[..cb.find("\n}\n").unwrap()];
        assert!(cb.find("domain_owner(").unwrap() < cb.find("finish_oidc(").unwrap());
        // Turning Okta on (save or switch) needs every domain proven.
        for f in ["pub async fn okta_save(", "pub async fn okta_toggle("] {
            let b = &src[src.find(f).unwrap()..];
            let b = &b[..b.find("\n}\n").unwrap()];
            assert!(b.contains("require_proven("), "{f}");
        }
    }

    /// Attack 2: a session a team's Okta signed in reaches nothing
    /// person-wide on hosted CoderHelm.
    #[test]
    fn held_sessions_are_refused_everything_person_wide() {
        for h in HELD_REFUSED {
            let r = held_refusal(true, Some(h)).expect(h);
            assert_eq!(r.status(), StatusCode::FORBIDDEN);
            assert!(held_refusal(false, Some(h)).is_none());
        }
        for h in [
            "api::switch_team",
            "users::link_github",
            "sign_in::app_setup",
            "users::change_password",
        ] {
            assert!(HELD_REFUSED.contains(&h), "{h}");
        }
        assert!(held_refusal(true, Some("api::me")).is_none());
        assert!(held_refusal(true, None).is_none());
        assert!(signed_in_with_okta(Some("okta")));
        for via in [
            Some("google"),
            Some("github"),
            Some("microsoft"),
            Some("password"),
            None,
        ] {
            assert!(!signed_in_with_okta(via), "{via:?}");
        }
        // GitHub connect checks the session's way in before linking anything,
        // and the middleware marks the session before the support view asks.
        let auth = include_str!("auth.rs");
        let gh = &auth[auth.find("async fn github_callback_inner(").unwrap()..];
        assert!(
            gh.find("Reason::TeamOnly").unwrap()
                < gh.find("connect GitHub to their account").unwrap()
        );
        let mw = include_str!("../middleware/auth.rs");
        assert!(
            mw.find("claims.held =").unwrap() < mw.find("support_access::check_session").unwrap()
        );
        assert!(mw.contains("held_refusal(claims.held, handler)"));
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
        };
        assert!(ticket_is_theirs(&t, Some(&c)));
        assert!(!ticket_is_theirs(&t, None));
        c.sub = "USER#someone".into();
        assert!(!ticket_is_theirs(&t, Some(&c)));
        c.sub = "USER#admin".into();
        c.team_id = "TEAM#2".into();
        assert!(!ticket_is_theirs(&t, Some(&c)));
    }
}
