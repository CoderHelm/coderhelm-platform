//! Signing in to CoderHelm's MCP server: OAuth 2.1 the way the MCP
//! authorization spec asks for it, so a person can add CoderHelm to Claude,
//! Cursor, VS Code or any other MCP client and act as themselves.
//!
//! The pieces, in the order a client meets them:
//!
//! 1. `POST /mcp` without a token answers `401` with a `WWW-Authenticate`
//!    header naming this server's protected resource metadata
//!    (`/.well-known/oauth-protected-resource/mcp`), which names the
//!    authorization server (this same API; its metadata is at
//!    `/.well-known/oauth-authorization-server`).
//! 2. The client registers itself (`POST /oauth/register`, RFC 7591). Every
//!    client is public: no secret, PKCE with S256 always.
//! 3. `GET /oauth/authorize` checks the request and sends the person to the
//!    dashboard's consent page with the request signed into the link. The
//!    dashboard shows who is asking and what it may do; the person decides
//!    with their own signed-in session (`/api/mcp/authorize`), and the answer
//!    goes back to the client with a one-time code.
//! 4. `POST /oauth/token` swaps the code (with its PKCE verifier) for an
//!    access token (an hour) and a refresh token (30 days, renewed on use,
//!    replaced on every use; a replaced one used again ends the connection).
//! 5. Every MCP call checks the access token against its connection (a
//!    revoked connection stops at once), then runs as the person through the
//!    same checks a dashboard request gets (`middleware::auth`).
//!
//! The rules live here, apart from DynamoDB, so they are tested without it
//! ([`OAuthStore`]). Storage, in the settings table:
//! - `pk = MCP_CLIENT#<id>`, `sk = MCP_CLIENT`: a registered client;
//! - `pk = MCP_CODE#<hash>`, `sk = MCP_CODE`: a code waiting to be swapped
//!   (deleted by the swap, so it works once);
//! - `pk = MCP_GRANT#<id>`, `sk = MCP_GRANT`: a connection, with its refresh
//!   token's hash, read on every call;
//! - `pk = <team_id>`, `sk = MCP_GRANT#<id>`: the team's copy, listed on the
//!   Connected apps page.

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use subtle::ConstantTimeEq;

/// Reading runs and reviews.
pub const READ_SCOPE: &str = "runs:read";
/// Starting and retrying runs, and asking for reviews.
pub const WRITE_SCOPE: &str = "runs:write";
/// Searching the team's knowledge bases the person (or a knowledge key)
/// can read, exactly as Chat does (`knowledge_search`, `knowledge_list`).
pub const KNOWLEDGE_SCOPE: &str = "knowledge:read";
/// Every scope, in the order they are shown.
pub const SCOPES: &[&str] = &[READ_SCOPE, WRITE_SCOPE, KNOWLEDGE_SCOPE];

/// What a scope lets an app do, as the consent page says it.
pub fn scope_label(scope: &str) -> &'static str {
    match scope {
        READ_SCOPE => "See your team's runs and reviews",
        WRITE_SCOPE => "Start and retry runs, and ask for reviews",
        KNOWLEDGE_SCOPE => "Search the knowledge bases you can read in Chat",
        _ => "",
    }
}

/// An access token lasts an hour.
pub const ACCESS_TTL_SECS: u64 = 3600;
/// A code must be swapped within five minutes.
pub const CODE_TTL_SECS: i64 = 300;
/// The consent page link works for ten minutes.
pub const REQUEST_TTL_SECS: u64 = 600;
/// A refresh token works for 30 days after its last use.
pub const REFRESH_TTL_DAYS: i64 = 30;
/// A registered client nobody signs in with is forgotten after this long.
pub const CLIENT_TTL_DAYS: i64 = 180;

/// The audience of an access token: accepted only by `/mcp`, never as a
/// session, a brain token or a consent link.
pub const ACCESS_AUDIENCE: &str = "coderhelm-mcp";
/// The audience of a consent link's signed request.
pub const REQUEST_AUDIENCE: &str = "coderhelm-mcp-consent";

pub const REFRESH_PREFIX: &str = "chr_";
pub const CODE_PREFIX: &str = "chc_";
pub const CLIENT_PREFIX: &str = "mcpc_";

const MAX_REDIRECT_URIS: usize = 10;
const MAX_URI_LEN: usize = 2000;
const MAX_CLIENT_NAME: usize = 100;
const MAX_STATE_LEN: usize = 1000;

// ── Where things are ────────────────────────────────────────────────

/// The authorization server's identifier: this install's API address. On a
/// self-hosted install that is its one address; on hosted CoderHelm,
/// `https://api.coderhelm.com`.
pub fn issuer() -> String {
    common::public_urls::api_url_from_env().to_string()
}

/// The MCP server's address, what people add to their client.
pub fn resource() -> String {
    resource_for(&issuer())
}

pub fn resource_for(issuer: &str) -> String {
    format!("{issuer}/mcp")
}

/// The protected resource metadata for `/mcp` (RFC 9728).
pub fn resource_metadata_url() -> String {
    format!("{}/.well-known/oauth-protected-resource/mcp", issuer())
}

/// Is `given` this server's resource? A trailing slash is the same address.
pub fn is_our_resource(given: &str, ours: &str) -> bool {
    given.trim_end_matches('/') == ours.trim_end_matches('/')
}

// ── Scopes ──────────────────────────────────────────────────────────

/// The scopes a request asks for. Reading is always included; a scope this
/// server doesn't have is left out (RFC 6749 lets the server grant less),
/// so a client that also asks for something like `offline_access` still
/// signs in.
pub fn parse_scope(raw: Option<&str>) -> BTreeSet<&'static str> {
    let mut out = BTreeSet::from([READ_SCOPE]);
    for word in raw.unwrap_or("").split_whitespace() {
        if let Some(s) = SCOPES.iter().find(|s| **s == word) {
            out.insert(s);
        }
    }
    out
}

/// Scopes as one space-separated string, in [`SCOPES`] order.
pub fn scope_string<'a>(scopes: impl IntoIterator<Item = &'a str>) -> String {
    let set: BTreeSet<&str> = scopes.into_iter().collect();
    SCOPES
        .iter()
        .filter(|s| set.contains(**s))
        .copied()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Stored scope strings, as the known scopes they name.
pub fn known_scopes(stored: &[String]) -> BTreeSet<&'static str> {
    stored
        .iter()
        .filter_map(|s| SCOPES.iter().find(|k| **k == s.as_str()).copied())
        .collect()
}

// ── Redirect addresses ──────────────────────────────────────────────

/// Schemes that are never a place to send a code.
const UNSAFE_SCHEMES: &[&str] = &[
    "javascript",
    "data",
    "file",
    "vbscript",
    "about",
    "blob",
    "ftp",
    "ws",
    "wss",
];

fn is_loopback(u: &url::Url) -> bool {
    u.scheme() == "http"
        && match u.host() {
            Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        }
}

/// Where a client may ask for its code to be sent: `https`, `http` only on
/// this machine (`localhost`, `127.0.0.1`, `[::1]`; RFC 8252), or an app's
/// own scheme (`cursor://...`, `vscode://...`). Never a fragment, never a
/// user name, never a scheme that runs or reads something.
pub fn check_redirect_uri(raw: &str) -> Result<url::Url, String> {
    if raw.len() > MAX_URI_LEN {
        return Err("A redirect address is too long.".into());
    }
    let u = url::Url::parse(raw).map_err(|_| format!("\"{raw}\" isn't an address."))?;
    if u.fragment().is_some() {
        return Err("A redirect address can't have a fragment (#...).".into());
    }
    if !u.username().is_empty() || u.password().is_some() {
        return Err("A redirect address can't carry a user name or password.".into());
    }
    match u.scheme() {
        "https" if u.host().is_some() => Ok(u),
        "https" => Err("An https redirect address needs a host.".into()),
        "http" if is_loopback(&u) => Ok(u),
        "http" => {
            Err("Plain http is allowed only for this computer (localhost or 127.0.0.1).".into())
        }
        s if UNSAFE_SCHEMES.contains(&s) => Err(format!("\"{s}:\" can't be a redirect address.")),
        _ => Ok(u),
    }
}

/// Does `given` match one of the client's registered addresses? Exactly, or
/// for an `http` address on this computer, with any port (RFC 8252 7.3: a
/// native app picks a free port each time).
pub fn redirect_matches(registered: &[String], given: &str) -> bool {
    if registered.iter().any(|r| r == given) {
        return true;
    }
    let Ok(g) = url::Url::parse(given) else {
        return false;
    };
    if !is_loopback(&g) {
        return false;
    }
    registered.iter().any(|r| {
        url::Url::parse(r).is_ok_and(|r| {
            is_loopback(&r)
                && r.host_str() == g.host_str()
                && r.path() == g.path()
                && r.query() == g.query()
        })
    })
}

/// The host a redirect goes to, as the consent page names it (an app's own
/// scheme shows as `cursor://`).
pub fn redirect_host(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(u) => match u.host_str() {
            Some(h) if matches!(u.scheme(), "http" | "https") => match u.port() {
                Some(p) if is_loopback(&u) => format!("{h}:{p}"),
                _ => h.to_string(),
            },
            Some(h) => format!("{}://{h}", u.scheme()),
            None => format!("{}:", u.scheme()),
        },
        Err(_) => String::new(),
    }
}

/// `uri` with `params` added to its query, keeping what it had.
pub fn redirect_with(uri: &str, params: &[(&str, &str)]) -> String {
    match url::Url::parse(uri) {
        Ok(mut u) => {
            {
                let mut q = u.query_pairs_mut();
                for (k, v) in params {
                    q.append_pair(k, v);
                }
            }
            u.to_string()
        }
        Err(_) => uri.to_string(),
    }
}

// ── Clients ─────────────────────────────────────────────────────────

/// A registration request (RFC 7591). Only what CoderHelm uses is read; a
/// `token_endpoint_auth_method` asked for is answered with `none`.
#[derive(Debug, Default, Deserialize)]
pub struct Registration {
    #[serde(default)]
    pub redirect_uris: Vec<String>,
    #[serde(default)]
    pub client_name: Option<String>,
    #[serde(default)]
    pub client_uri: Option<String>,
    #[serde(default)]
    pub grant_types: Option<Vec<String>>,
    #[serde(default)]
    pub response_types: Option<Vec<String>>,
}

/// A registered client. It has no secret: every client here is public and
/// proves each sign-in with PKCE.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredClient {
    pub client_id: String,
    pub client_name: String,
    pub redirect_uris: Vec<String>,
    #[serde(default)]
    pub client_uri: Option<String>,
    pub created_at: String,
}

/// Why a registration was refused: `(error, description)` as RFC 7591 names them.
pub type RegisterError = (&'static str, String);

/// A client's name as it is shown: printable, trimmed, at most 100
/// characters. It is the client's own claim, and the consent page says so.
pub fn clean_name(raw: Option<&str>) -> String {
    let name: String = raw
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let name: String = name.chars().take(MAX_CLIENT_NAME).collect();
    if name.is_empty() {
        "An app without a name".to_string()
    } else {
        name
    }
}

/// Checks a registration and makes the client it describes.
pub fn register(
    reg: &Registration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<StoredClient, RegisterError> {
    if reg.redirect_uris.is_empty() {
        return Err((
            "invalid_redirect_uri",
            "Send at least one redirect address in redirect_uris.".into(),
        ));
    }
    if reg.redirect_uris.len() > MAX_REDIRECT_URIS {
        return Err((
            "invalid_redirect_uri",
            format!("Send at most {MAX_REDIRECT_URIS} redirect addresses."),
        ));
    }
    for uri in &reg.redirect_uris {
        check_redirect_uri(uri).map_err(|e| ("invalid_redirect_uri", e))?;
    }
    if let Some(types) = &reg.grant_types {
        if !types.iter().any(|t| t == "authorization_code")
            || types
                .iter()
                .any(|t| t != "authorization_code" && t != "refresh_token")
        {
            return Err((
                "invalid_client_metadata",
                "grant_types may hold only authorization_code and refresh_token.".into(),
            ));
        }
    }
    if let Some(types) = &reg.response_types {
        if types.iter().any(|t| t != "code") {
            return Err((
                "invalid_client_metadata",
                "response_types may hold only code.".into(),
            ));
        }
    }
    let client_uri = reg
        .client_uri
        .as_deref()
        .filter(|u| u.len() <= MAX_URI_LEN && u.starts_with("https://"))
        .map(str::to_string);
    Ok(StoredClient {
        client_id: format!(
            "{CLIENT_PREFIX}{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        ),
        client_name: clean_name(reg.client_name.as_deref()),
        redirect_uris: reg.redirect_uris.clone(),
        client_uri,
        created_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    })
}

/// The registration answer (RFC 7591 3.2.1). `token_endpoint_auth_method`
/// is always `none`, whatever was asked: every client here is public.
pub fn registration_answer(c: &StoredClient, issued_at: i64) -> serde_json::Value {
    serde_json::json!({
        "client_id": c.client_id,
        "client_id_issued_at": issued_at,
        "client_name": c.client_name,
        "client_uri": c.client_uri,
        "redirect_uris": c.redirect_uris,
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": scope_string(SCOPES.iter().copied()),
    })
}

// ── PKCE ────────────────────────────────────────────────────────────

fn b64url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn is_b64url(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// The S256 challenge of a verifier (RFC 7636 4.2).
pub fn pkce_challenge(verifier: &str) -> String {
    b64url(&Sha256::digest(verifier.as_bytes()))
}

/// Does `verifier` answer `challenge`? Verifiers are 43 to 128 characters
/// of `A-Z a-z 0-9 - . _ ~`. Constant time.
pub fn pkce_matches(verifier: &str, challenge: &str) -> bool {
    let ok_chars = verifier
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~'));
    if !(43..=128).contains(&verifier.len()) || !ok_chars {
        return false;
    }
    let got = pkce_challenge(verifier);
    got.len() == challenge.len() && bool::from(got.as_bytes().ct_eq(challenge.as_bytes()))
}

// ── Authorization requests ──────────────────────────────────────────

/// `GET /oauth/authorize`'s query.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct AuthorizeParams {
    pub response_type: Option<String>,
    pub client_id: Option<String>,
    pub redirect_uri: Option<String>,
    pub code_challenge: Option<String>,
    pub code_challenge_method: Option<String>,
    pub scope: Option<String>,
    pub state: Option<String>,
    pub resource: Option<String>,
}

/// Why an authorization request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizeError {
    /// The client or its redirect address can't be trusted: shown to the
    /// person on a page, never sent anywhere.
    Page(String),
    /// Sent back to the client's (checked) redirect address.
    Redirect {
        redirect_uri: String,
        error: &'static str,
        description: String,
        state: Option<String>,
    },
}

/// A checked request, signed into the consent page's link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRequest {
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub state: Option<String>,
    pub resource: String,
    #[serde(default)]
    pub aud: String,
    #[serde(default)]
    pub iat: u64,
    #[serde(default)]
    pub exp: u64,
}

/// Checks an authorization request against its client. The client and the
/// redirect address are checked first: until both are known good, nothing
/// is sent to the address.
pub fn check_authorize(
    p: &AuthorizeParams,
    client: Option<&StoredClient>,
    resource: &str,
) -> Result<PendingRequest, AuthorizeError> {
    let Some(client) = client else {
        return Err(AuthorizeError::Page(
            "This app isn't registered with CoderHelm, or its registration has expired. Remove CoderHelm from the app and add it again.".into(),
        ));
    };
    if p.client_id.as_deref() != Some(client.client_id.as_str()) {
        return Err(AuthorizeError::Page(
            "The app's request is not valid.".into(),
        ));
    }
    let redirect_uri = match p.redirect_uri.as_deref() {
        Some(r) if redirect_matches(&client.redirect_uris, r) => r.to_string(),
        Some(_) => {
            return Err(AuthorizeError::Page(
                "The app asked to be sent somewhere it didn't register. CoderHelm stopped here to keep your account safe.".into(),
            ))
        }
        None if client.redirect_uris.len() == 1 => client.redirect_uris[0].clone(),
        None => {
            return Err(AuthorizeError::Page(
                "The app didn't say where to send you back to.".into(),
            ))
        }
    };
    let state = p.state.clone().filter(|s| !s.is_empty());
    let back = |error: &'static str, description: &str| AuthorizeError::Redirect {
        redirect_uri: redirect_uri.clone(),
        error,
        description: description.to_string(),
        state: state.clone(),
    };
    if state.as_ref().is_some_and(|s| s.len() > MAX_STATE_LEN) {
        return Err(back("invalid_request", "state is too long."));
    }
    if p.response_type.as_deref() != Some("code") {
        return Err(back(
            "unsupported_response_type",
            "Only response_type=code is supported.",
        ));
    }
    let Some(challenge) = p.code_challenge.as_deref().filter(|c| !c.is_empty()) else {
        return Err(back(
            "invalid_request",
            "PKCE is required: send code_challenge with code_challenge_method=S256.",
        ));
    };
    if p.code_challenge_method.as_deref() != Some("S256") {
        return Err(back(
            "invalid_request",
            "Only code_challenge_method=S256 is supported.",
        ));
    }
    if challenge.len() != 43 || !is_b64url(challenge) {
        return Err(back(
            "invalid_request",
            "code_challenge must be the base64url SHA-256 of the verifier (43 characters).",
        ));
    }
    if let Some(r) = p.resource.as_deref().filter(|r| !r.is_empty()) {
        if !is_our_resource(r, resource) {
            return Err(back(
                "invalid_target",
                "This server signs in only to its own MCP address.",
            ));
        }
    }
    Ok(PendingRequest {
        client_id: client.client_id.clone(),
        redirect_uri,
        code_challenge: challenge.to_string(),
        scopes: parse_scope(p.scope.as_deref())
            .into_iter()
            .map(str::to_string)
            .collect(),
        state,
        resource: resource.to_string(),
        aud: String::new(),
        iat: 0,
        exp: 0,
    })
}

/// Where a refused request goes back to, with the error (and the state the
/// client sent, and who answered: RFC 9207).
pub fn error_redirect(err: &AuthorizeError, issuer: &str) -> Option<String> {
    let AuthorizeError::Redirect {
        redirect_uri,
        error,
        description,
        state,
    } = err
    else {
        return None;
    };
    let mut params = vec![
        ("error", *error),
        ("error_description", description.as_str()),
        ("iss", issuer),
    ];
    if let Some(s) = state {
        params.push(("state", s.as_str()));
    }
    Some(redirect_with(redirect_uri, &params))
}

/// The request, signed for the consent page's link.
pub fn sign_request(
    req: &PendingRequest,
    secret: &str,
    now_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let signed = PendingRequest {
        aud: REQUEST_AUDIENCE.to_string(),
        iat: now_secs,
        exp: now_secs + REQUEST_TTL_SECS,
        ..req.clone()
    };
    encode(
        &Header::default(),
        &signed,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// A consent link's request, when it is ours and still current.
pub fn read_request(token: &str, secret: &str, now_secs: u64) -> Option<PendingRequest> {
    let mut v = Validation::default();
    v.set_audience(&[REQUEST_AUDIENCE]);
    v.validate_exp = false;
    let req = decode::<PendingRequest>(token, &DecodingKey::from_secret(secret.as_bytes()), &v)
        .ok()?
        .claims;
    (req.exp > now_secs).then_some(req)
}

// ── Codes ───────────────────────────────────────────────────────────

/// Who said yes, from their signed-in session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consent {
    pub team_id: String,
    pub user_id: String,
    pub email: String,
    /// The session passed a second factor.
    pub mfa: bool,
    pub via: Option<String>,
    /// The person let the app start runs and ask for reviews.
    pub allow_write: bool,
    /// The person let the app search the knowledge bases they can read.
    pub allow_knowledge: bool,
}

/// A code waiting to be swapped, with everything the swap checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCode {
    pub hash: String,
    pub client_id: String,
    pub client_name: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub scopes: Vec<String>,
    pub resource: String,
    pub team_id: String,
    pub user_id: String,
    pub email: String,
    pub mfa: bool,
    #[serde(default)]
    pub via: Option<String>,
    /// Seconds since the epoch.
    pub expires_at: i64,
}

/// A random secret: 32 bytes, base64url.
fn secret() -> String {
    use rand::RngCore;
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    b64url(&b)
}

/// SHA-256 as hex. Codes and refresh tokens carry 256 random bits, so a
/// fast hash is the right one.
pub fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn hash_matches(token: &str, stored: &str) -> bool {
    let got = hash(token);
    !stored.is_empty()
        && got.len() == stored.len()
        && bool::from(got.as_bytes().ct_eq(stored.as_bytes()))
}

/// The scopes a person granted: reading always, writing and searching
/// knowledge when they allowed each.
pub fn granted_scopes(allow_write: bool, allow_knowledge: bool) -> Vec<String> {
    let mut s = vec![READ_SCOPE.to_string()];
    if allow_write {
        s.push(WRITE_SCOPE.to_string());
    }
    if allow_knowledge {
        s.push(KNOWLEDGE_SCOPE.to_string());
    }
    s
}

/// The person said yes: a one-time code for the client, and what the swap
/// will check it against.
pub fn approve(
    req: &PendingRequest,
    client: &StoredClient,
    who: &Consent,
    now: chrono::DateTime<chrono::Utc>,
) -> (String, StoredCode) {
    let code = format!("{CODE_PREFIX}{}", secret());
    let stored = StoredCode {
        hash: hash(&code),
        client_id: req.client_id.clone(),
        client_name: client.client_name.clone(),
        redirect_uri: req.redirect_uri.clone(),
        code_challenge: req.code_challenge.clone(),
        scopes: granted_scopes(who.allow_write, who.allow_knowledge),
        resource: req.resource.clone(),
        team_id: who.team_id.clone(),
        user_id: who.user_id.clone(),
        email: who.email.clone(),
        mfa: who.mfa,
        via: who.via.clone(),
        expires_at: now.timestamp() + CODE_TTL_SECS,
    };
    (code, stored)
}

// ── Connections ─────────────────────────────────────────────────────

/// One app connected by one person, in one team: what every access token
/// and refresh token belongs to. Revoking it ends both at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredGrant {
    pub id: String,
    pub team_id: String,
    pub user_id: String,
    pub email: String,
    pub client_id: String,
    pub client_name: String,
    /// Where the app's codes went, to tell apps with one name apart.
    pub redirect_host: String,
    pub scopes: Vec<String>,
    pub created_at: String,
    /// When the person said yes, in seconds: sessions ended after this (a
    /// removal, a role change, a reset second factor) end the connection too.
    pub created_at_secs: u64,
    pub mfa: bool,
    #[serde(default)]
    pub via: Option<String>,
    #[serde(default)]
    pub refresh_hash: String,
    #[serde(default)]
    pub prev_refresh_hash: Option<String>,
    #[serde(default)]
    pub refresh_expires_at: String,
    #[serde(default)]
    pub last_used_at: Option<String>,
    #[serde(default)]
    pub revoked_at: Option<String>,
    #[serde(default)]
    pub revoked_by: Option<String>,
}

impl StoredGrant {
    pub fn scope_set(&self) -> BTreeSet<&'static str> {
        known_scopes(&self.scopes)
    }
}

/// The claims of an access token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessClaims {
    pub sub: String,
    pub team_id: String,
    /// The connection.
    pub gid: String,
    pub client_id: String,
    pub scope: String,
    pub aud: String,
    pub iss: String,
    pub iat: u64,
    pub exp: u64,
}

/// An access token for a connection, with `scopes`.
pub fn issue_access(
    g: &StoredGrant,
    scopes: &BTreeSet<&'static str>,
    issuer: &str,
    secret: &str,
    now_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let claims = AccessClaims {
        sub: g.user_id.clone(),
        team_id: g.team_id.clone(),
        gid: g.id.clone(),
        client_id: g.client_id.clone(),
        scope: scope_string(scopes.iter().copied()),
        aud: ACCESS_AUDIENCE.to_string(),
        iss: issuer.to_string(),
        iat: now_secs,
        exp: now_secs + ACCESS_TTL_SECS,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// Why an access token was refused (each is a `401`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessRefusal {
    Invalid,
    Expired,
    Revoked,
}

impl AccessRefusal {
    pub fn description(self) -> &'static str {
        match self {
            Self::Invalid => "The access token isn't valid.",
            Self::Expired => "The access token has expired. Use the refresh token for a new one.",
            Self::Revoked => "This connection was removed in CoderHelm. Sign in again.",
        }
    }
}

/// The claims of an access token signed here for `/mcp`, checked against
/// `now_secs` (so expiry is tested without waiting).
pub fn read_access(
    token: &str,
    secret: &str,
    now_secs: u64,
) -> Result<AccessClaims, AccessRefusal> {
    let mut v = Validation::default();
    v.set_audience(&[ACCESS_AUDIENCE]);
    v.validate_exp = false;
    let c = decode::<AccessClaims>(token, &DecodingKey::from_secret(secret.as_bytes()), &v)
        .map_err(|_| AccessRefusal::Invalid)?
        .claims;
    if c.exp <= now_secs {
        return Err(AccessRefusal::Expired);
    }
    Ok(c)
}

/// A call's connection and the scopes its token holds.
#[derive(Debug, Clone)]
pub struct Authorized {
    pub grant: StoredGrant,
    pub scopes: BTreeSet<&'static str>,
}

/// Checks an access token and its connection. The connection is read on
/// every call: one revoked in the dashboard stops at once, and a token's
/// team, person and app must be the connection's own.
pub async fn authenticate(
    store: &impl OAuthStore,
    token: &str,
    secret: &str,
    now_secs: u64,
) -> Result<Result<Authorized, AccessRefusal>, String> {
    let claims = match read_access(token, secret, now_secs) {
        Ok(c) => c,
        Err(r) => return Ok(Err(r)),
    };
    let Some(grant) = store.get_grant(&claims.gid).await? else {
        return Ok(Err(AccessRefusal::Revoked));
    };
    if grant.team_id != claims.team_id
        || grant.user_id != claims.sub
        || grant.client_id != claims.client_id
    {
        return Ok(Err(AccessRefusal::Invalid));
    }
    if grant.revoked_at.is_some() {
        return Ok(Err(AccessRefusal::Revoked));
    }
    let token_scopes = parse_scope(Some(&claims.scope));
    let scopes = token_scopes
        .intersection(&grant.scope_set())
        .copied()
        .collect();
    Ok(Ok(Authorized { grant, scopes }))
}

// ── The token endpoint ──────────────────────────────────────────────

/// `POST /oauth/token`'s form.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct TokenForm {
    pub grant_type: Option<String>,
    pub code: Option<String>,
    pub redirect_uri: Option<String>,
    pub client_id: Option<String>,
    /// A knowledge key's secret (`grant_type=client_credentials`).
    #[serde(default)]
    pub client_secret: Option<String>,
    pub code_verifier: Option<String>,
    pub refresh_token: Option<String>,
    pub resource: Option<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenAnswer {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub scope: String,
}

/// A refused token request (RFC 6749 5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenError {
    pub error: &'static str,
    pub description: String,
}

fn token_error(error: &'static str, description: &str) -> TokenError {
    TokenError {
        error,
        description: description.to_string(),
    }
}

/// A new refresh token for connection `gid`: `chr_<gid>_<secret>`.
pub fn new_refresh(gid: &str) -> String {
    format!("{REFRESH_PREFIX}{gid}_{}", secret())
}

/// The connection id inside a refresh token.
pub fn refresh_grant_id(token: &str) -> Option<&str> {
    let rest = token.strip_prefix(REFRESH_PREFIX)?;
    let (id, secret) = rest.split_once('_')?;
    (id.len() == 26 && id.chars().all(|c| c.is_ascii_alphanumeric()) && secret.len() == 43)
        .then_some(id)
}

fn refresh_expiry(now: chrono::DateTime<chrono::Utc>) -> String {
    (now + chrono::Duration::days(REFRESH_TTL_DAYS))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn expired(at: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(at).map_or(true, |t| t <= now)
}

/// Answers a token request: a code swapped for a new connection's tokens,
/// or a refresh token swapped for new ones. The store failing is an error
/// (the caller answers 503), never a token.
pub async fn token(
    store: &impl OAuthStore,
    form: &TokenForm,
    issuer: &str,
    secret: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<TokenAnswer, TokenError>, String> {
    match form.grant_type.as_deref() {
        Some("authorization_code") => swap_code(store, form, issuer, secret, now).await,
        Some("refresh_token") => refresh(store, form, issuer, secret, now).await,
        Some(_) => Ok(Err(token_error(
            "unsupported_grant_type",
            "Use authorization_code or refresh_token.",
        ))),
        None => Ok(Err(token_error(
            "invalid_request",
            "grant_type is missing.",
        ))),
    }
}

async fn swap_code(
    store: &impl OAuthStore,
    form: &TokenForm,
    issuer: &str,
    secret: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<TokenAnswer, TokenError>, String> {
    let bad = |d: &str| Ok(Err(token_error("invalid_grant", d)));
    let Some(code) = form.code.as_deref().filter(|c| c.starts_with(CODE_PREFIX)) else {
        return Ok(Err(token_error("invalid_request", "code is missing.")));
    };
    let Some(verifier) = form.code_verifier.as_deref() else {
        return Ok(Err(token_error(
            "invalid_request",
            "code_verifier is missing (PKCE is required).",
        )));
    };
    // Taken out whatever happens next: a code works once, even a code sent
    // with a wrong verifier.
    let Some(c) = store.take_code(&hash(code)).await? else {
        return bad("The code isn't valid, or was already used.");
    };
    if c.expires_at <= now.timestamp() {
        return bad("The code has expired. Sign in again.");
    }
    if form.client_id.as_deref() != Some(c.client_id.as_str()) {
        return bad("The code was given to another app.");
    }
    if form
        .redirect_uri
        .as_deref()
        .is_some_and(|r| r != c.redirect_uri)
    {
        return bad("redirect_uri isn't the one the code was sent to.");
    }
    if !pkce_matches(verifier, &c.code_challenge) {
        return bad("code_verifier doesn't match the code_challenge.");
    }
    if let Some(r) = form.resource.as_deref().filter(|r| !r.is_empty()) {
        if !is_our_resource(r, &c.resource) {
            return Ok(Err(token_error(
                "invalid_target",
                "This server issues tokens only for its own MCP address.",
            )));
        }
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let refresh_token = new_refresh(&id);
    let grant = StoredGrant {
        id,
        team_id: c.team_id,
        user_id: c.user_id,
        email: c.email,
        client_id: c.client_id,
        client_name: c.client_name,
        redirect_host: redirect_host(&c.redirect_uri),
        scopes: c.scopes,
        created_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        created_at_secs: now.timestamp().max(0) as u64,
        mfa: c.mfa,
        via: c.via,
        refresh_hash: hash(&refresh_token),
        prev_refresh_hash: None,
        refresh_expires_at: refresh_expiry(now),
        last_used_at: None,
        revoked_at: None,
        revoked_by: None,
    };
    store.create_grant(&grant).await?;
    let scopes = grant.scope_set();
    let access_token = issue_access(&grant, &scopes, issuer, secret, now.timestamp() as u64)
        .map_err(|e| e.to_string())?;
    Ok(Ok(TokenAnswer {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_SECS,
        refresh_token,
        scope: scope_string(scopes),
    }))
}

async fn refresh(
    store: &impl OAuthStore,
    form: &TokenForm,
    issuer: &str,
    secret: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Result<TokenAnswer, TokenError>, String> {
    let bad = |d: &str| Ok(Err(token_error("invalid_grant", d)));
    let Some(token) = form.refresh_token.as_deref() else {
        return Ok(Err(token_error(
            "invalid_request",
            "refresh_token is missing.",
        )));
    };
    let Some(gid) = refresh_grant_id(token) else {
        return bad("The refresh token isn't valid.");
    };
    let Some(g) = store.get_grant(gid).await? else {
        return bad("This connection no longer exists. Sign in again.");
    };
    if g.revoked_at.is_some() {
        return bad("This connection was removed in CoderHelm. Sign in again.");
    }
    if form.client_id.as_deref().is_some_and(|c| c != g.client_id) {
        return bad("The refresh token belongs to another app.");
    }
    if !hash_matches(token, &g.refresh_hash) {
        if g.prev_refresh_hash
            .as_deref()
            .is_some_and(|p| hash_matches(token, p))
        {
            // A replaced refresh token came back: someone else may hold a
            // copy. End the connection (OAuth 2.1 4.3.1).
            let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            store
                .revoke_grant(&g.team_id, &g.id, "a refresh token used twice", &at)
                .await?;
            return bad(
                "This refresh token was already used, so the connection was ended. Sign in again.",
            );
        }
        return bad("The refresh token isn't valid.");
    }
    if expired(&g.refresh_expires_at, now) {
        return bad("The refresh token has expired. Sign in again.");
    }
    if let Some(r) = form.resource.as_deref().filter(|r| !r.is_empty()) {
        if !is_our_resource(r, &resource_for(issuer)) {
            return Ok(Err(token_error(
                "invalid_target",
                "This server issues tokens only for its own MCP address.",
            )));
        }
    }
    // A narrower set may be asked for; never a wider one.
    let held = g.scope_set();
    let scopes: BTreeSet<&'static str> = match form.scope.as_deref() {
        Some(s) if !s.trim().is_empty() => {
            let asked = parse_scope(Some(s));
            if !asked.is_subset(&held) {
                return Ok(Err(token_error(
                    "invalid_scope",
                    "That is more than this connection was given. Sign in again to ask for it.",
                )));
            }
            asked
        }
        _ => held,
    };
    let next = new_refresh(&g.id);
    let rotated = store
        .rotate_refresh(&g, &hash(next.as_str()), &refresh_expiry(now))
        .await?;
    if !rotated {
        return bad("The refresh token was just used. Use the newest one.");
    }
    let access_token = issue_access(&g, &scopes, issuer, secret, now.timestamp() as u64)
        .map_err(|e| e.to_string())?;
    Ok(Ok(TokenAnswer {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_SECS,
        refresh_token: next,
        scope: scope_string(scopes),
    }))
}

/// `POST /oauth/revoke` (RFC 7009): a refresh token ends its connection; an
/// access token ends the connection it belongs to. Anything else is
/// ignored, and the answer is the same either way.
pub async fn revoke_token(
    store: &impl OAuthStore,
    token: &str,
    secret: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let grant = if let Some(gid) = refresh_grant_id(token) {
        store
            .get_grant(gid)
            .await?
            .filter(|g| hash_matches(token, &g.refresh_hash))
    } else {
        // An expired access token can still be revoked.
        let mut v = Validation::default();
        v.set_audience(&[ACCESS_AUDIENCE]);
        v.validate_exp = false;
        match decode::<AccessClaims>(token, &DecodingKey::from_secret(secret.as_bytes()), &v) {
            Ok(t) => store
                .get_grant(&t.claims.gid)
                .await?
                .filter(|g| g.team_id == t.claims.team_id && g.user_id == t.claims.sub),
            Err(_) => None,
        }
    };
    if let Some(g) = grant.filter(|g| g.revoked_at.is_none()) {
        store
            .revoke_grant(&g.team_id, &g.id, "the app itself", &at)
            .await?;
    }
    Ok(())
}

// ── Where it is kept ────────────────────────────────────────────────

/// A trait so the rules can be tested without DynamoDB.
pub trait OAuthStore {
    async fn put_client(&self, c: &StoredClient) -> Result<(), String>;
    async fn get_client(&self, id: &str) -> Result<Option<StoredClient>, String>;
    async fn put_code(&self, c: &StoredCode) -> Result<(), String>;
    /// Removes the code and returns it: at most one caller ever gets it.
    async fn take_code(&self, hash: &str) -> Result<Option<StoredCode>, String>;
    async fn create_grant(&self, g: &StoredGrant) -> Result<(), String>;
    async fn get_grant(&self, id: &str) -> Result<Option<StoredGrant>, String>;
    /// Replaces the refresh token, only if `g.refresh_hash` is still the
    /// current one. `false` when another request replaced it first.
    async fn rotate_refresh(
        &self,
        g: &StoredGrant,
        new_hash: &str,
        expires_at: &str,
    ) -> Result<bool, String>;
    /// Marks a connection of this team revoked. `false` when the team has
    /// no such live connection.
    async fn revoke_grant(
        &self,
        team_id: &str,
        id: &str,
        by: &str,
        at: &str,
    ) -> Result<bool, String>;
    /// The team's connections, live and revoked, newest first.
    async fn list_grants(&self, team_id: &str) -> Result<Vec<StoredGrant>, String>;
    /// Notes a call (`last_used_at`), at most once a minute per connection.
    async fn note_use(&self, g: &StoredGrant, at: &str) -> Result<(), String>;
}

pub struct DynamoOAuth<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

pub fn store(state: &crate::AppState) -> DynamoOAuth<'_> {
    DynamoOAuth {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

const CLIENT_PK: &str = "MCP_CLIENT#";
const CODE_PK: &str = "MCP_CODE#";
const GRANT_PK: &str = "MCP_GRANT#";
const GRANT_SK: &str = "MCP_GRANT";
/// The team copy's sort key prefix.
pub const TEAM_GRANT_PREFIX: &str = "MCP_GRANT#";

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn err<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

fn data<T: for<'de> Deserialize<'de>>(item: &HashMap<String, AttributeValue>) -> Option<T> {
    item.get("data")
        .and_then(|v| v.as_s().ok())
        .and_then(|d| serde_json::from_str(d).ok())
}

/// A connection from either of its records: the fixed part is `data`, what
/// changes is kept beside it.
fn grant_of(item: &HashMap<String, AttributeValue>) -> Option<StoredGrant> {
    let mut g: StoredGrant = data(item)?;
    let get = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    if let Some(h) = get("refresh_hash") {
        g.refresh_hash = h;
    }
    g.prev_refresh_hash = get("prev_refresh_hash").or(g.prev_refresh_hash);
    if let Some(e) = get("refresh_expires_at") {
        g.refresh_expires_at = e;
    }
    g.last_used_at = get("last_used_at").or(g.last_used_at);
    if let Some(at) = get("revoked_at") {
        g.revoked_at = Some(at);
        g.revoked_by = get("revoked_by");
    }
    Some(g)
}

/// Seconds since the epoch of an RFC 3339 time (0 when unreadable).
fn epoch(at: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(at).map_or(0, |t| t.timestamp())
}

impl OAuthStore for DynamoOAuth<'_> {
    async fn put_client(&self, c: &StoredClient) -> Result<(), String> {
        let ttl = epoch(&c.created_at) + CLIENT_TTL_DAYS * 86_400;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(&format!("{CLIENT_PK}{}", c.client_id)))
            .item("sk", s("MCP_CLIENT"))
            .item("record_type", s("mcp_client"))
            .item(
                "data",
                s(&serde_json::to_string(c).map_err(|e| e.to_string())?),
            )
            .item("ttl", n(ttl))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn get_client(&self, id: &str) -> Result<Option<StoredClient>, String> {
        if !id.starts_with(CLIENT_PREFIX) || id.len() > 64 {
            return Ok(None);
        }
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(&format!("{CLIENT_PK}{id}")))
            .key("sk", s("MCP_CLIENT"))
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(data))
    }

    async fn put_code(&self, c: &StoredCode) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(&format!("{CODE_PK}{}", c.hash)))
            .item("sk", s("MCP_CODE"))
            .item("record_type", s("mcp_code"))
            .item(
                "data",
                s(&serde_json::to_string(c).map_err(|e| e.to_string())?),
            )
            .item("ttl", n(c.expires_at + 60))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn take_code(&self, hash: &str) -> Result<Option<StoredCode>, String> {
        let r = self
            .dynamo
            .delete_item()
            .table_name(self.table)
            .key("pk", s(&format!("{CODE_PK}{hash}")))
            .key("sk", s("MCP_CODE"))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(err)?;
        Ok(r.attributes().and_then(data))
    }

    async fn create_grant(&self, g: &StoredGrant) -> Result<(), String> {
        // The hashes live beside `data`, on the checked record only.
        let fixed = StoredGrant {
            refresh_hash: String::new(),
            prev_refresh_hash: None,
            ..g.clone()
        };
        let body = serde_json::to_string(&fixed).map_err(|e| e.to_string())?;
        let ttl = epoch(&g.refresh_expires_at) + 86_400;
        let main = Put::builder()
            .table_name(self.table)
            .item("pk", s(&format!("{GRANT_PK}{}", g.id)))
            .item("sk", s(GRANT_SK))
            .item("record_type", s("mcp_grant"))
            .item("data", s(&body))
            .item("refresh_hash", s(&g.refresh_hash))
            .item("refresh_expires_at", s(&g.refresh_expires_at))
            .item("ttl", n(ttl))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .map_err(|e| e.to_string())?;
        let team = Put::builder()
            .table_name(self.table)
            .item("pk", s(&g.team_id))
            .item("sk", s(&format!("{TEAM_GRANT_PREFIX}{}", g.id)))
            .item("record_type", s("mcp_grant"))
            .item("data", s(&body))
            .item("refresh_expires_at", s(&g.refresh_expires_at))
            .item("ttl", n(ttl))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .map_err(|e| e.to_string())?;
        self.dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(main).build())
            .transact_items(TransactWriteItem::builder().put(team).build())
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn get_grant(&self, id: &str) -> Result<Option<StoredGrant>, String> {
        if id.len() != 26 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Ok(None);
        }
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(&format!("{GRANT_PK}{id}")))
            .key("sk", s(GRANT_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(grant_of))
    }

    async fn rotate_refresh(
        &self,
        g: &StoredGrant,
        new_hash: &str,
        expires_at: &str,
    ) -> Result<bool, String> {
        let ttl = n(epoch(expires_at) + 86_400);
        let main = Update::builder()
            .table_name(self.table)
            .key("pk", s(&format!("{GRANT_PK}{}", g.id)))
            .key("sk", s(GRANT_SK))
            .update_expression(
                "SET refresh_hash = :new, prev_refresh_hash = :old, refresh_expires_at = :exp, #ttl = :ttl",
            )
            .condition_expression("refresh_hash = :old AND attribute_not_exists(revoked_at)")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":new", s(new_hash))
            .expression_attribute_values(":old", s(&g.refresh_hash))
            .expression_attribute_values(":exp", s(expires_at))
            .expression_attribute_values(":ttl", ttl.clone())
            .build()
            .map_err(|e| e.to_string())?;
        let team = Update::builder()
            .table_name(self.table)
            .key("pk", s(&g.team_id))
            .key("sk", s(&format!("{TEAM_GRANT_PREFIX}{}", g.id)))
            .update_expression("SET refresh_expires_at = :exp, #ttl = :ttl")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":exp", s(expires_at))
            .expression_attribute_values(":ttl", ttl)
            .build()
            .map_err(|e| e.to_string())?;
        let out = self
            .dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().update(main).build())
            .transact_items(TransactWriteItem::builder().update(team).build())
            .send()
            .await;
        match out {
            Ok(_) => Ok(true),
            Err(e) => {
                let text = err(e);
                if text.contains("ConditionalCheckFailed") || text.contains("TransactionCanceled") {
                    Ok(false)
                } else {
                    Err(text)
                }
            }
        }
    }

    async fn revoke_grant(
        &self,
        team_id: &str,
        id: &str,
        by: &str,
        at: &str,
    ) -> Result<bool, String> {
        // The checked record must be this team's: an id from another team
        // matches nothing here.
        let Some(existing) = self.get_grant(id).await? else {
            return Ok(false);
        };
        if existing.team_id != team_id || existing.revoked_at.is_some() {
            return Ok(false);
        }
        // Kept 90 days after revoking, for the page's history.
        let ttl = n(epoch(at) + 90 * 86_400);
        let update = |pk: String, sk: String| {
            Update::builder()
                .table_name(self.table)
                .key("pk", s(&pk))
                .key("sk", s(&sk))
                .update_expression("SET revoked_at = :at, revoked_by = :by, #ttl = :ttl")
                .condition_expression("attribute_exists(pk) AND attribute_not_exists(revoked_at)")
                .expression_attribute_names("#ttl", "ttl")
                .expression_attribute_values(":at", s(at))
                .expression_attribute_values(":by", s(by))
                .expression_attribute_values(":ttl", ttl.clone())
                .build()
                .map_err(|e| e.to_string())
        };
        let out = self
            .dynamo
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .update(update(format!("{GRANT_PK}{id}"), GRANT_SK.into())?)
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .update(update(
                        team_id.to_string(),
                        format!("{TEAM_GRANT_PREFIX}{id}"),
                    )?)
                    .build(),
            )
            .send()
            .await;
        match out {
            Ok(_) => Ok(true),
            Err(e) => {
                let text = err(e);
                if text.contains("ConditionalCheckFailed") || text.contains("TransactionCanceled") {
                    Ok(false)
                } else {
                    Err(text)
                }
            }
        }
    }

    async fn list_grants(&self, team_id: &str) -> Result<Vec<StoredGrant>, String> {
        let r = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(team_id))
            .expression_attribute_values(":p", s(TEAM_GRANT_PREFIX))
            .scan_index_forward(false)
            .send()
            .await
            .map_err(err)?;
        Ok(r.items().iter().filter_map(grant_of).collect())
    }

    async fn note_use(&self, g: &StoredGrant, at: &str) -> Result<(), String> {
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", s(&g.team_id))
            .key("sk", s(&format!("{TEAM_GRANT_PREFIX}{}", g.id)))
            .update_expression("SET last_used_at = :at")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":at", s(at))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    pub const SECRET: &str = "test-jwt-secret-at-least-32-chars!";
    pub const ISSUER: &str = "https://api.example.org";

    /// Clients, codes and connections kept in memory, the way DynamoOAuth keeps them.
    #[derive(Default)]
    pub struct Mem {
        pub clients: Mutex<HashMap<String, StoredClient>>,
        pub codes: Mutex<HashMap<String, StoredCode>>,
        pub grants: Mutex<HashMap<String, StoredGrant>>,
    }

    impl OAuthStore for Mem {
        async fn put_client(&self, c: &StoredClient) -> Result<(), String> {
            self.clients
                .lock()
                .unwrap()
                .insert(c.client_id.clone(), c.clone());
            Ok(())
        }
        async fn get_client(&self, id: &str) -> Result<Option<StoredClient>, String> {
            Ok(self.clients.lock().unwrap().get(id).cloned())
        }
        async fn put_code(&self, c: &StoredCode) -> Result<(), String> {
            self.codes.lock().unwrap().insert(c.hash.clone(), c.clone());
            Ok(())
        }
        async fn take_code(&self, hash: &str) -> Result<Option<StoredCode>, String> {
            Ok(self.codes.lock().unwrap().remove(hash))
        }
        async fn create_grant(&self, g: &StoredGrant) -> Result<(), String> {
            self.grants.lock().unwrap().insert(g.id.clone(), g.clone());
            Ok(())
        }
        async fn get_grant(&self, id: &str) -> Result<Option<StoredGrant>, String> {
            Ok(self.grants.lock().unwrap().get(id).cloned())
        }
        async fn rotate_refresh(
            &self,
            g: &StoredGrant,
            new_hash: &str,
            expires_at: &str,
        ) -> Result<bool, String> {
            let mut all = self.grants.lock().unwrap();
            match all.get_mut(&g.id) {
                Some(cur) if cur.refresh_hash == g.refresh_hash && cur.revoked_at.is_none() => {
                    cur.prev_refresh_hash = Some(cur.refresh_hash.clone());
                    cur.refresh_hash = new_hash.to_string();
                    cur.refresh_expires_at = expires_at.to_string();
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        async fn revoke_grant(
            &self,
            team_id: &str,
            id: &str,
            by: &str,
            at: &str,
        ) -> Result<bool, String> {
            let mut all = self.grants.lock().unwrap();
            match all.get_mut(id) {
                Some(g) if g.team_id == team_id && g.revoked_at.is_none() => {
                    g.revoked_at = Some(at.into());
                    g.revoked_by = Some(by.into());
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        async fn list_grants(&self, team_id: &str) -> Result<Vec<StoredGrant>, String> {
            let mut v: Vec<StoredGrant> = self
                .grants
                .lock()
                .unwrap()
                .values()
                .filter(|g| g.team_id == team_id)
                .cloned()
                .collect();
            v.sort_by(|a, b| b.id.cmp(&a.id));
            Ok(v)
        }
        async fn note_use(&self, g: &StoredGrant, at: &str) -> Result<(), String> {
            if let Some(cur) = self.grants.lock().unwrap().get_mut(&g.id) {
                cur.last_used_at = Some(at.into());
            }
            Ok(())
        }
    }

    pub fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    pub const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";

    pub fn client(mem: &Mem, redirect: &str) -> StoredClient {
        let c = register(
            &Registration {
                redirect_uris: vec![redirect.into()],
                client_name: Some("Claude".into()),
                ..Default::default()
            },
            now(),
        )
        .unwrap();
        mem.clients
            .lock()
            .unwrap()
            .insert(c.client_id.clone(), c.clone());
        c
    }

    pub fn params(c: &StoredClient, scope: &str) -> AuthorizeParams {
        AuthorizeParams {
            response_type: Some("code".into()),
            client_id: Some(c.client_id.clone()),
            redirect_uri: Some(c.redirect_uris[0].clone()),
            code_challenge: Some(pkce_challenge(VERIFIER)),
            code_challenge_method: Some("S256".into()),
            scope: Some(scope.into()),
            state: Some("xyz".into()),
            resource: Some(resource_for(ISSUER)),
        }
    }

    pub fn consent(team: &str, user: &str, write: bool) -> Consent {
        Consent {
            team_id: team.into(),
            user_id: user.into(),
            email: "ann@example.org".into(),
            mfa: false,
            via: Some("password".into()),
            allow_write: write,
            allow_knowledge: false,
        }
    }

    /// A full sign-in: register, authorize, consent, swap. Returns the tokens.
    pub async fn connect(mem: &Mem, team: &str, user: &str, write: bool) -> TokenAnswer {
        let c = client(mem, "https://claude.ai/api/mcp/auth_callback");
        let req = check_authorize(
            &params(&c, "runs:read runs:write"),
            Some(&c),
            &resource_for(ISSUER),
        )
        .unwrap();
        let (code, stored) = approve(&req, &c, &consent(team, user, write), now());
        mem.put_code(&stored).await.unwrap();
        token(
            mem,
            &TokenForm {
                grant_type: Some("authorization_code".into()),
                code: Some(code),
                redirect_uri: Some(c.redirect_uris[0].clone()),
                client_id: Some(c.client_id.clone()),
                code_verifier: Some(VERIFIER.into()),
                resource: Some(resource_for(ISSUER)),
                ..Default::default()
            },
            ISSUER,
            SECRET,
            now(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn refresh_with(mem: &Mem, refresh: &str) -> Result<TokenAnswer, TokenError> {
        token(
            mem,
            &TokenForm {
                grant_type: Some("refresh_token".into()),
                refresh_token: Some(refresh.into()),
                ..Default::default()
            },
            ISSUER,
            SECRET,
            now(),
        )
        .await
        .unwrap()
    }

    async fn auth(mem: &Mem, access: &str) -> Result<Authorized, AccessRefusal> {
        authenticate(mem, access, SECRET, now().timestamp() as u64)
            .await
            .unwrap()
    }

    #[test]
    fn pkce_accepts_only_the_verifier_of_the_challenge() {
        // RFC 7636 appendix B.
        assert_eq!(
            pkce_challenge(VERIFIER),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        let challenge = pkce_challenge(VERIFIER);
        assert!(pkce_matches(VERIFIER, &challenge));
        let mut wrong = VERIFIER.to_string();
        wrong.replace_range(0..1, "e");
        assert!(!pkce_matches(&wrong, &challenge));
        // Too short, or with characters a verifier can't have.
        assert!(!pkce_matches("short", &pkce_challenge("short")));
        let spaced = format!("{} ", &VERIFIER[..43]);
        assert!(!pkce_matches(&spaced, &pkce_challenge(&spaced)));
    }

    #[test]
    fn an_authorization_request_needs_pkce_s256_and_a_registered_redirect() {
        let mem = Mem::default();
        let c = client(&mem, "https://claude.ai/api/mcp/auth_callback");
        let res = resource_for(ISSUER);
        let ok = check_authorize(&params(&c, "runs:read"), Some(&c), &res).unwrap();
        assert_eq!(ok.scopes, vec!["runs:read"]);
        assert_eq!(ok.state.as_deref(), Some("xyz"));

        // No PKCE, or plain PKCE: refused back to the app.
        let mut p = params(&c, "runs:read");
        p.code_challenge = None;
        assert!(matches!(
            check_authorize(&p, Some(&c), &res),
            Err(AuthorizeError::Redirect {
                error: "invalid_request",
                ..
            })
        ));
        let mut p = params(&c, "runs:read");
        p.code_challenge_method = Some("plain".into());
        assert!(matches!(
            check_authorize(&p, Some(&c), &res),
            Err(AuthorizeError::Redirect {
                error: "invalid_request",
                ..
            })
        ));
        let mut p = params(&c, "runs:read");
        p.response_type = Some("token".into());
        assert!(matches!(
            check_authorize(&p, Some(&c), &res),
            Err(AuthorizeError::Redirect {
                error: "unsupported_response_type",
                ..
            })
        ));
        // Another server's resource.
        let mut p = params(&c, "runs:read");
        p.resource = Some("https://evil.example/mcp".into());
        assert!(matches!(
            check_authorize(&p, Some(&c), &res),
            Err(AuthorizeError::Redirect {
                error: "invalid_target",
                ..
            })
        ));

        // An address the app didn't register, or no app: a page, never a redirect.
        let mut p = params(&c, "runs:read");
        p.redirect_uri = Some("https://evil.example/cb".into());
        let e = check_authorize(&p, Some(&c), &res).unwrap_err();
        assert!(matches!(e, AuthorizeError::Page(_)));
        assert_eq!(error_redirect(&e, ISSUER), None);
        assert!(matches!(
            check_authorize(&params(&c, ""), None, &res),
            Err(AuthorizeError::Page(_))
        ));

        // An error sent back carries the state and who answered.
        let mut p = params(&c, "runs:read");
        p.code_challenge = None;
        let url =
            error_redirect(&check_authorize(&p, Some(&c), &res).unwrap_err(), ISSUER).unwrap();
        assert!(url.starts_with("https://claude.ai/api/mcp/auth_callback?error=invalid_request"));
        assert!(url.contains("state=xyz") && url.contains("iss=https%3A%2F%2Fapi.example.org"));
    }

    #[test]
    fn redirect_addresses_are_https_this_computer_or_an_apps_own_scheme() {
        for ok in [
            "https://claude.ai/api/mcp/auth_callback",
            "http://localhost:6274/oauth/callback",
            "http://127.0.0.1:33418/",
            "http://[::1]:5000/cb",
            "cursor://anysphere.cursor-retrieval/oauth/user-coderhelm/callback",
            "vscode://vscode.github-authentication/did-authenticate",
        ] {
            assert!(check_redirect_uri(ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://example.com/cb",
            "javascript:alert(1)",
            "data:text/html,hi",
            "file:///etc/passwd",
            "https://claude.ai/cb#frag",
            "https://user:pw@claude.ai/cb",
            "not a url",
        ] {
            assert!(check_redirect_uri(bad).is_err(), "{bad}");
        }
        // A loopback address matches on any port; nothing else does.
        let reg = vec![
            "http://127.0.0.1/callback".to_string(),
            "https://claude.ai/cb".to_string(),
        ];
        assert!(redirect_matches(&reg, "http://127.0.0.1:51234/callback"));
        assert!(!redirect_matches(&reg, "http://127.0.0.1:51234/other"));
        assert!(!redirect_matches(&reg, "https://claude.ai/cb2"));
        assert!(!redirect_matches(&reg, "https://claude.ai:8443/cb"));
        assert_eq!(redirect_host("https://claude.ai/cb"), "claude.ai");
        assert_eq!(redirect_host("http://127.0.0.1:5123/cb"), "127.0.0.1:5123");
        assert_eq!(
            redirect_host("cursor://anysphere.cursor-retrieval/oauth"),
            "cursor://anysphere.cursor-retrieval"
        );
    }

    #[test]
    fn registration_makes_a_public_client_with_a_cleaned_name() {
        let c = register(
            &Registration {
                redirect_uris: vec!["https://claude.ai/cb".into()],
                client_name: Some("  Claude\u{0007}  Desktop ".into()),
                ..Default::default()
            },
            now(),
        )
        .unwrap();
        assert!(c.client_id.starts_with(CLIENT_PREFIX));
        assert_eq!(c.client_name, "Claude Desktop");
        let a = registration_answer(&c, 1);
        assert_eq!(a["token_endpoint_auth_method"], "none");
        assert!(a.get("client_secret").is_none());
        assert_eq!(clean_name(None), "An app without a name");
        for bad in [
            Registration::default(),
            Registration {
                redirect_uris: vec!["http://example.com/cb".into()],
                ..Default::default()
            },
            Registration {
                redirect_uris: vec!["https://claude.ai/cb".into()],
                grant_types: Some(vec!["client_credentials".into()]),
                ..Default::default()
            },
            Registration {
                redirect_uris: vec!["https://claude.ai/cb".into()],
                response_types: Some(vec!["token".into()]),
                ..Default::default()
            },
        ] {
            assert!(register(&bad, now()).is_err());
        }
    }

    #[test]
    fn scopes_always_include_reading_and_ignore_what_this_server_lacks() {
        assert_eq!(parse_scope(None), BTreeSet::from([READ_SCOPE]));
        assert_eq!(
            parse_scope(Some("runs:write offline_access")),
            BTreeSet::from([READ_SCOPE, WRITE_SCOPE])
        );
        assert_eq!(
            scope_string([WRITE_SCOPE, READ_SCOPE]),
            "runs:read runs:write"
        );
        assert_eq!(granted_scopes(false, false), vec!["runs:read"]);
        assert_eq!(
            granted_scopes(false, true),
            vec!["runs:read", "knowledge:read"]
        );
    }

    #[test]
    fn a_consent_link_is_signed_and_expires() {
        let mem = Mem::default();
        let c = client(&mem, "https://claude.ai/cb");
        let req =
            check_authorize(&params(&c, "runs:read"), Some(&c), &resource_for(ISSUER)).unwrap();
        let t0 = 1_800_000_000;
        let link = sign_request(&req, SECRET, t0).unwrap();
        let back = read_request(&link, SECRET, t0 + 10).unwrap();
        assert_eq!(back.client_id, c.client_id);
        assert!(read_request(&link, SECRET, t0 + REQUEST_TTL_SECS).is_none());
        assert!(read_request(&link, "another-secret-of-at-least-32-chars", t0).is_none());
        // A session or an access token is not a consent link.
        let session =
            crate::auth::jwt::create_token("u", "t", "e@x.com", "member", None, SECRET, 300)
                .unwrap();
        assert!(read_request(&session, SECRET, t0).is_none());
    }

    #[tokio::test]
    async fn a_code_swaps_once_for_tokens_with_the_right_verifier() {
        let mem = Mem::default();
        let c = client(&mem, "https://claude.ai/cb");
        let req = check_authorize(
            &params(&c, "runs:read runs:write"),
            Some(&c),
            &resource_for(ISSUER),
        )
        .unwrap();
        let (code, stored) = approve(&req, &c, &consent("TEAM#a", "USER#ann", true), now());
        assert!(!stored.hash.contains(&code[CODE_PREFIX.len()..]));
        mem.put_code(&stored).await.unwrap();
        let form = |verifier: &str, client_id: &str| TokenForm {
            grant_type: Some("authorization_code".into()),
            code: Some(code.clone()),
            redirect_uri: Some("https://claude.ai/cb".into()),
            client_id: Some(client_id.into()),
            code_verifier: Some(verifier.into()),
            ..Default::default()
        };
        // A wrong verifier: refused, and the code is used up.
        let wrong = "x".repeat(43);
        let e = token(&mem, &form(&wrong, &c.client_id), ISSUER, SECRET, now())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.error, "invalid_grant");
        let e = token(&mem, &form(VERIFIER, &c.client_id), ISSUER, SECRET, now())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.error, "invalid_grant");

        // A fresh code with the right verifier, by the right app: tokens.
        let (code, stored) = approve(&req, &c, &consent("TEAM#a", "USER#ann", true), now());
        mem.put_code(&stored).await.unwrap();
        let mut f = form(VERIFIER, "mcpc_someoneelse");
        f.code = Some(code.clone());
        let e = token(&mem, &f, ISSUER, SECRET, now())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.error, "invalid_grant", "another app can't swap it");

        let (code, stored) = approve(&req, &c, &consent("TEAM#a", "USER#ann", true), now());
        mem.put_code(&stored).await.unwrap();
        let mut f = form(VERIFIER, &c.client_id);
        f.code = Some(code.clone());
        let t = token(&mem, &f, ISSUER, SECRET, now())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.token_type, "Bearer");
        assert_eq!(t.expires_in, ACCESS_TTL_SECS);
        assert_eq!(t.scope, "runs:read runs:write");
        assert!(t.refresh_token.starts_with(REFRESH_PREFIX));
        // Used once: never again.
        let e = token(&mem, &f, ISSUER, SECRET, now())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.error, "invalid_grant");
        // The refresh token is kept only as its hash.
        let g = mem.grants.lock().unwrap().values().next().cloned().unwrap();
        assert_eq!(g.refresh_hash, hash(&t.refresh_token));
        assert_eq!(g.redirect_host, "claude.ai");
    }

    #[tokio::test]
    async fn an_expired_code_is_refused() {
        let mem = Mem::default();
        let c = client(&mem, "https://claude.ai/cb");
        let req =
            check_authorize(&params(&c, "runs:read"), Some(&c), &resource_for(ISSUER)).unwrap();
        let then = now() - chrono::Duration::seconds(CODE_TTL_SECS + 1);
        let (code, stored) = approve(&req, &c, &consent("TEAM#a", "USER#ann", false), then);
        mem.put_code(&stored).await.unwrap();
        let e = token(
            &mem,
            &TokenForm {
                grant_type: Some("authorization_code".into()),
                code: Some(code),
                client_id: Some(c.client_id.clone()),
                code_verifier: Some(VERIFIER.into()),
                ..Default::default()
            },
            ISSUER,
            SECRET,
            now(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(e.error, "invalid_grant");
        assert!(e.description.contains("expired"));
    }

    #[tokio::test]
    async fn an_access_token_works_until_it_expires() {
        let mem = Mem::default();
        let t = connect(&mem, "TEAM#a", "USER#ann", false).await;
        let ok = auth(&mem, &t.access_token).await.unwrap();
        assert_eq!(ok.grant.team_id, "TEAM#a");
        assert_eq!(
            ok.scopes,
            BTreeSet::from([READ_SCOPE]),
            "write was not allowed"
        );
        // An hour later it is expired; the refresh token gets a new one.
        let later = now().timestamp() as u64 + ACCESS_TTL_SECS;
        assert_eq!(
            authenticate(&mem, &t.access_token, SECRET, later)
                .await
                .unwrap()
                .unwrap_err(),
            AccessRefusal::Expired
        );
        // Signed with another secret, or a session token: not an access token.
        assert_eq!(
            auth(&mem, "eyJhbGciOiJIUzI1NiJ9.e30.x").await.unwrap_err(),
            AccessRefusal::Invalid
        );
        let session = crate::auth::jwt::create_token(
            "USER#ann", "TEAM#a", "e@x.com", "owner", None, SECRET, 300,
        )
        .unwrap();
        assert_eq!(
            auth(&mem, &session).await.unwrap_err(),
            AccessRefusal::Invalid
        );
        // ...and an access token is not a session.
        assert!(crate::auth::jwt::validate_token(&t.access_token, SECRET).is_err());
    }

    #[tokio::test]
    async fn refresh_tokens_rotate_and_a_reused_one_ends_the_connection() {
        let mem = Mem::default();
        let t = connect(&mem, "TEAM#a", "USER#ann", true).await;
        let t2 = refresh_with(&mem, &t.refresh_token).await.unwrap();
        assert_ne!(t2.refresh_token, t.refresh_token);
        assert_eq!(t2.scope, "runs:read runs:write");
        auth(&mem, &t2.access_token).await.unwrap();
        // A narrower token may be asked for, never a wider one.
        let t3 = token(
            &mem,
            &TokenForm {
                grant_type: Some("refresh_token".into()),
                refresh_token: Some(t2.refresh_token.clone()),
                scope: Some("runs:read".into()),
                ..Default::default()
            },
            ISSUER,
            SECRET,
            now(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(t3.scope, "runs:read");
        assert_eq!(
            auth(&mem, &t3.access_token).await.unwrap().scopes,
            BTreeSet::from([READ_SCOPE])
        );
        // The replaced token comes back: the connection ends, and every
        // token of it stops.
        let e = refresh_with(&mem, &t2.refresh_token).await.unwrap_err();
        assert_eq!(e.error, "invalid_grant");
        assert_eq!(
            auth(&mem, &t3.access_token).await.unwrap_err(),
            AccessRefusal::Revoked
        );
        assert_eq!(
            refresh_with(&mem, &t3.refresh_token)
                .await
                .unwrap_err()
                .error,
            "invalid_grant"
        );
        // Garbage is refused without a lookup.
        assert_eq!(
            refresh_with(&mem, "chr_nope").await.unwrap_err().error,
            "invalid_grant"
        );
    }

    #[tokio::test]
    async fn a_read_only_connection_cannot_refresh_into_writing() {
        let mem = Mem::default();
        let t = connect(&mem, "TEAM#a", "USER#ann", false).await;
        let e = token(
            &mem,
            &TokenForm {
                grant_type: Some("refresh_token".into()),
                refresh_token: Some(t.refresh_token.clone()),
                scope: Some("runs:read runs:write".into()),
                ..Default::default()
            },
            ISSUER,
            SECRET,
            now(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(e.error, "invalid_scope");
    }

    #[tokio::test]
    async fn revoking_ends_access_and_refresh_at_once() {
        let mem = Mem::default();
        let t = connect(&mem, "TEAM#a", "USER#ann", true).await;
        let gid = auth(&mem, &t.access_token).await.unwrap().grant.id;
        // Another team can't revoke it.
        assert!(!mem
            .revoke_grant("TEAM#b", &gid, "USER#bob", "now")
            .await
            .unwrap());
        auth(&mem, &t.access_token).await.unwrap();
        assert!(mem
            .revoke_grant("TEAM#a", &gid, "USER#ann", "now")
            .await
            .unwrap());
        assert_eq!(
            auth(&mem, &t.access_token).await.unwrap_err(),
            AccessRefusal::Revoked
        );
        assert_eq!(
            refresh_with(&mem, &t.refresh_token)
                .await
                .unwrap_err()
                .error,
            "invalid_grant"
        );
        // Twice finds nothing to revoke.
        assert!(!mem
            .revoke_grant("TEAM#a", &gid, "USER#ann", "now")
            .await
            .unwrap());

        // The app revoking its own token (RFC 7009) ends the connection too.
        let t = connect(&mem, "TEAM#a", "USER#ann", false).await;
        revoke_token(&mem, &t.refresh_token, SECRET, now())
            .await
            .unwrap();
        assert_eq!(
            auth(&mem, &t.access_token).await.unwrap_err(),
            AccessRefusal::Revoked
        );
        let t = connect(&mem, "TEAM#a", "USER#ann", false).await;
        revoke_token(&mem, &t.access_token, SECRET, now())
            .await
            .unwrap();
        assert_eq!(
            refresh_with(&mem, &t.refresh_token)
                .await
                .unwrap_err()
                .error,
            "invalid_grant"
        );
        // Anything else is ignored.
        revoke_token(&mem, "garbage", SECRET, now()).await.unwrap();
    }

    #[tokio::test]
    async fn a_token_works_only_for_its_own_team_person_and_app() {
        let mem = Mem::default();
        let a = connect(&mem, "TEAM#a", "USER#ann", true).await;
        let b = connect(&mem, "TEAM#b", "USER#bob", true).await;
        assert_eq!(
            auth(&mem, &a.access_token).await.unwrap().grant.team_id,
            "TEAM#a"
        );
        assert_eq!(
            auth(&mem, &b.access_token).await.unwrap().grant.team_id,
            "TEAM#b"
        );
        // A token claiming team b on team a's connection is nobody.
        let ga = auth(&mem, &a.access_token).await.unwrap().grant;
        let mut forged = ga.clone();
        forged.team_id = "TEAM#b".into();
        let t = issue_access(
            &forged,
            &BTreeSet::from([READ_SCOPE]),
            ISSUER,
            SECRET,
            now().timestamp() as u64,
        )
        .unwrap();
        assert_eq!(auth(&mem, &t).await.unwrap_err(), AccessRefusal::Invalid);
        let mut forged = ga.clone();
        forged.user_id = "USER#bob".into();
        let t = issue_access(
            &forged,
            &BTreeSet::from([READ_SCOPE]),
            ISSUER,
            SECRET,
            now().timestamp() as u64,
        )
        .unwrap();
        assert_eq!(auth(&mem, &t).await.unwrap_err(), AccessRefusal::Invalid);
        // A token for a write scope its connection doesn't hold gets only reading.
        let ro = connect(&mem, "TEAM#a", "USER#ann", false).await;
        let g = auth(&mem, &ro.access_token).await.unwrap().grant;
        let t = issue_access(
            &g,
            &BTreeSet::from([READ_SCOPE, WRITE_SCOPE]),
            ISSUER,
            SECRET,
            now().timestamp() as u64,
        )
        .unwrap();
        assert_eq!(
            auth(&mem, &t).await.unwrap().scopes,
            BTreeSet::from([READ_SCOPE])
        );
        // Each team lists only its own connections.
        assert!(mem
            .list_grants("TEAM#b")
            .await
            .unwrap()
            .iter()
            .all(|g| g.team_id == "TEAM#b"));
        // Team b's refresh token can't be revoked through team a.
        let gb = auth(&mem, &b.access_token).await.unwrap().grant.id;
        assert!(!mem
            .revoke_grant("TEAM#a", &gb, "USER#ann", "now")
            .await
            .unwrap());
    }
}
