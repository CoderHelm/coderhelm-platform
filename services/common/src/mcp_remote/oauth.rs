//! The OAuth 2.1 pieces of connecting to a remote MCP server: PKCE, the
//! one-time `state`, client registration (a Client ID Metadata Document,
//! dynamic registration, or a client id an admin pastes), the authorization
//! address, the code exchange, refresh and revocation.
//!
//! Every token request names the server as its `resource` (RFC 8707), so a
//! token is only ever issued for, and sent to, that one server. Tokens are
//! never handed to anything else: not to the model, not to another server.

use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::discovery::AuthServer;
use super::http::{HttpRequest, HttpResponse};

/// The name the sign-in page shows.
pub const CLIENT_NAME: &str = "Coderhelm";

fn random_token() -> String {
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    URL_SAFE_NO_PAD.encode(b)
}

/// A PKCE pair: the verifier stays with Coderhelm, the challenge goes in the
/// authorization address.
#[derive(Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

impl Pkce {
    pub fn new() -> Self {
        Self::from_verifier(random_token())
    }
    pub fn from_verifier(verifier: String) -> Self {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Self {
            verifier,
            challenge,
        }
    }
}

impl Default for Pkce {
    fn default() -> Self {
        Self::new()
    }
}

/// A fresh, unguessable `state` value.
pub fn new_state() -> String {
    random_token()
}

/// What a `state` is stored under: its hash, so the stored record alone
/// can't be replayed as a valid `state`.
pub fn state_key(state: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(state.as_bytes()))
}

/// How the client proves itself at the token endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuth {
    /// A public client: PKCE only.
    #[default]
    None,
    ClientSecretPost,
    ClientSecretBasic,
}

impl ClientAuth {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientAuth::None => "none",
            ClientAuth::ClientSecretPost => "client_secret_post",
            ClientAuth::ClientSecretBasic => "client_secret_basic",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "client_secret_post" => ClientAuth::ClientSecretPost,
            "client_secret_basic" => ClientAuth::ClientSecretBasic,
            _ => ClientAuth::None,
        }
    }
}

/// The OAuth client Coderhelm signs in as on one authorization server.
#[derive(Clone, PartialEq, Eq)]
pub struct Client {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub auth: ClientAuth,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .field("auth", &self.auth)
            .finish_non_exhaustive()
    }
}

/// How a server's client id was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    /// Coderhelm's published Client ID Metadata Document.
    MetadataDocument,
    /// RFC 7591 dynamic registration.
    Dynamic,
    /// An admin registers an app with the vendor and pastes its id.
    Manual,
}

impl Registration {
    pub fn as_str(self) -> &'static str {
        match self {
            Registration::MetadataDocument => "metadata_document",
            Registration::Dynamic => "dynamic",
            Registration::Manual => "manual",
        }
    }
}

/// Which registration to use, in the spec's order of preference after a
/// pre-registered client: a metadata document (when the server supports it
/// and Coderhelm publishes one), then dynamic registration, then asking.
pub fn choose_registration(a: &AuthServer, metadata_document_url: Option<&str>) -> Registration {
    if a.cimd_supported && metadata_document_url.is_some() {
        Registration::MetadataDocument
    } else if a.registration_endpoint.is_some() {
        Registration::Dynamic
    } else {
        Registration::Manual
    }
}

/// The address the person's browser is sent to.
pub fn authorize_url(
    a: &AuthServer,
    client_id: &str,
    redirect_uri: &str,
    pkce: &Pkce,
    state: &str,
    scopes: &[String],
    resource: &str,
) -> Result<String, String> {
    let mut u = reqwest::Url::parse(&a.authorization_endpoint)
        .map_err(|_| "The sign-in address isn't valid.".to_string())?;
    {
        let mut q = u.query_pairs_mut();
        q.append_pair("response_type", "code")
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("code_challenge", &pkce.challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", state);
        // A provider's own API (a knowledge source) has no resource
        // indicator; every MCP server has one.
        if !resource.is_empty() {
            q.append_pair("resource", resource);
        }
        if !scopes.is_empty() {
            q.append_pair("scope", &scopes.join(" "));
        }
    }
    Ok(u.to_string())
}

/// The dynamic registration request (RFC 7591). A public client unless the
/// server only takes clients with a secret.
pub fn registration_request(a: &AuthServer, redirect_uri: &str, scopes: &[String]) -> HttpRequest {
    let methods = &a.token_auth_methods;
    let auth = if methods.is_empty() || methods.iter().any(|m| m == "none") {
        ClientAuth::None
    } else if methods.iter().any(|m| m == "client_secret_post") {
        ClientAuth::ClientSecretPost
    } else {
        ClientAuth::ClientSecretBasic
    };
    let mut body = serde_json::json!({
        "client_name": CLIENT_NAME,
        // The install that asks: the callback's origin (a self-hosted
        // install's own address, never the hosted site).
        "client_uri": origin_of(redirect_uri),
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": auth.as_str(),
        "application_type": "web",
    });
    if !scopes.is_empty() {
        body["scope"] = serde_json::json!(scopes.join(" "));
    }
    HttpRequest::json(
        a.registration_endpoint.as_deref().unwrap_or_default(),
        &body,
        super::http::Limit::Metadata,
    )
    .header("Accept", "application/json")
}

/// `https://host[:port]` of a URL, or the URL itself when it has no host.
fn origin_of(u: &str) -> String {
    match url::Url::parse(u) {
        Ok(p) if p.host_str().is_some() => p.origin().ascii_serialization(),
        _ => u.to_string(),
    }
}

/// Reads a registration reply.
pub fn parse_registration(r: &HttpResponse) -> Result<Client, String> {
    let doc = r.json().unwrap_or_default();
    if !(200..300).contains(&r.status) {
        let why = doc["error_description"]
            .as_str()
            .or(doc["error"].as_str())
            .unwrap_or("no reason given");
        return Err(format!(
            "The sign-in server refused to register Coderhelm ({}: {}).",
            r.status,
            crate::truncate_str(why, 200)
        ));
    }
    let client_id = doc["client_id"].as_str().unwrap_or("").to_string();
    if client_id.is_empty() {
        return Err("The sign-in server registered Coderhelm but sent no client id.".into());
    }
    let client_secret = doc["client_secret"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let auth = match doc["token_endpoint_auth_method"].as_str() {
        Some(m) => ClientAuth::parse(m),
        None if client_secret.is_some() => ClientAuth::ClientSecretBasic,
        None => ClientAuth::None,
    };
    Ok(Client {
        client_id,
        client_secret,
        auth,
    })
}

fn with_client(mut req: HttpRequest, pairs: &mut Vec<(String, String)>, c: &Client) -> HttpRequest {
    match (c.auth, &c.client_secret) {
        (ClientAuth::ClientSecretBasic, Some(secret)) => {
            let enc =
                |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
            let basic = STANDARD.encode(format!("{}:{}", enc(&c.client_id), enc(secret)));
            req = req.header("Authorization", &format!("Basic {basic}"));
        }
        (ClientAuth::ClientSecretPost, Some(secret)) => {
            pairs.push(("client_id".into(), c.client_id.clone()));
            pairs.push(("client_secret".into(), secret.clone()));
        }
        _ => pairs.push(("client_id".into(), c.client_id.clone())),
    }
    req
}

fn form(url: &str, mut pairs: Vec<(String, String)>, c: &Client) -> HttpRequest {
    pairs.retain(|(k, v)| !(k == "resource" && v.is_empty()));
    let shell = HttpRequest::form(url, &[]);
    let shell = with_client(shell, &mut pairs, c);
    let refs: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut out = HttpRequest::form(url, &refs);
    out.headers = shell.headers;
    out
}

/// Exchanges an authorization code (with its PKCE verifier) for tokens.
pub fn token_request(
    a: &AuthServer,
    c: &Client,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    resource: &str,
) -> HttpRequest {
    form(
        &a.token_endpoint,
        vec![
            ("grant_type".into(), "authorization_code".into()),
            ("code".into(), code.into()),
            ("code_verifier".into(), verifier.into()),
            ("redirect_uri".into(), redirect_uri.into()),
            ("resource".into(), resource.into()),
        ],
        c,
    )
}

/// Trades a refresh token for a new access token.
pub fn refresh_request(
    token_endpoint: &str,
    c: &Client,
    refresh_token: &str,
    resource: &str,
) -> HttpRequest {
    form(
        token_endpoint,
        vec![
            ("grant_type".into(), "refresh_token".into()),
            ("refresh_token".into(), refresh_token.into()),
            ("resource".into(), resource.into()),
        ],
        c,
    )
}

/// RFC 7009 revocation of one token.
pub fn revoke_request(
    revocation_endpoint: &str,
    c: &Client,
    token: &str,
    hint: &str,
) -> HttpRequest {
    form(
        revocation_endpoint,
        vec![
            ("token".into(), token.into()),
            ("token_type_hint".into(), hint.into()),
        ],
        c,
    )
}

/// The same token request with its fields as a JSON object instead of a
/// form, for providers whose token endpoint takes JSON (Notion, Atlassian).
pub fn as_json_body(req: HttpRequest) -> HttpRequest {
    let pairs: serde_json::Map<String, serde_json::Value> =
        url::form_urlencoded::parse(req.body.as_deref().unwrap_or_default())
            .map(|(k, v)| (k.into_owned(), serde_json::Value::String(v.into_owned())))
            .collect();
    let mut out = req;
    out.headers
        .retain(|(k, _)| !k.eq_ignore_ascii_case("content-type"));
    out.headers
        .push(("Content-Type".into(), "application/json".into()));
    out.body = Some(serde_json::to_vec(&pairs).unwrap_or_default());
    out
}

/// `url` with `extra` query pairs added (a provider's own sign-in options,
/// like Google's `access_type=offline`).
pub fn with_extra_params(url: &str, extra: &[(String, String)]) -> Result<String, String> {
    if extra.is_empty() {
        return Ok(url.to_string());
    }
    let mut u =
        reqwest::Url::parse(url).map_err(|_| "The sign-in address isn't valid.".to_string())?;
    {
        let mut q = u.query_pairs_mut();
        for (k, v) in extra {
            q.append_pair(k, v);
        }
    }
    Ok(u.to_string())
}

/// The tokens for one connection. Never printed, never logged, never given
/// to a model.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenSet {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Unix seconds; `None` when the server gave no lifetime.
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub scope: String,
}

impl std::fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenSet")
            .field("expires_at", &self.expires_at)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// Seconds before expiry at which a token is refreshed instead of used.
pub const REFRESH_EARLY_SECS: i64 = 60;

impl TokenSet {
    pub fn usable_at(&self, now: i64) -> bool {
        self.expires_at.is_none_or(|e| e - REFRESH_EARLY_SECS > now)
    }
}

/// Why a token request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenError {
    /// The OAuth error code (`invalid_grant`, ...), or empty.
    pub code: String,
    pub message: String,
}

impl TokenError {
    /// The grant is gone (expired, revoked, already used): only a person
    /// signing in again fixes it.
    pub fn needs_reconnect(&self) -> bool {
        matches!(
            self.code.as_str(),
            "invalid_grant" | "invalid_client" | "unauthorized_client"
        )
    }
}

/// Reads a token reply. `previous` keeps the refresh token when a refresh
/// reply doesn't rotate it.
pub fn parse_token_response(
    r: &HttpResponse,
    now: i64,
    previous: Option<&TokenSet>,
) -> Result<TokenSet, TokenError> {
    let doc = r.json().unwrap_or_default();
    if !(200..300).contains(&r.status) || doc.get("error").is_some() {
        let code = doc["error"].as_str().unwrap_or("").to_string();
        let desc = doc["error_description"].as_str().unwrap_or("");
        return Err(TokenError {
            message: format!(
                "The sign-in server refused ({}{}{}).",
                if code.is_empty() {
                    r.status.to_string()
                } else {
                    code.clone()
                },
                if desc.is_empty() { "" } else { ": " },
                crate::truncate_str(desc, 200)
            ),
            code,
        });
    }
    let access_token = doc["access_token"].as_str().unwrap_or("").to_string();
    if access_token.is_empty() {
        return Err(TokenError {
            code: String::new(),
            message: "The sign-in server sent no access token.".into(),
        });
    }
    if let Some(t) = doc["token_type"].as_str() {
        if !t.eq_ignore_ascii_case("bearer") {
            return Err(TokenError {
                code: String::new(),
                message: format!("The sign-in server sent a {t} token; only Bearer is supported."),
            });
        }
    }
    let expires_at = doc["expires_in"]
        .as_i64()
        .or_else(|| doc["expires_in"].as_str().and_then(|s| s.parse().ok()))
        .filter(|s| *s > 0)
        .map(|s| now + s);
    Ok(TokenSet {
        access_token,
        refresh_token: doc["refresh_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| previous.and_then(|p| p.refresh_token.clone())),
        expires_at,
        scope: doc["scope"]
            .as_str()
            .map(str::to_string)
            .or_else(|| previous.map(|p| p.scope.clone()))
            .unwrap_or_default(),
    })
}

/// RFC 9207: the `iss` on the authorization response must be the issuer
/// recorded when the flow started. Compared as plain strings, no
/// normalization. When the server says it always sends `iss`, a missing one
/// is refused.
pub fn check_iss(iss_supported: bool, recorded: &str, iss: Option<&str>) -> Result<(), String> {
    match iss {
        Some(i) if i == recorded => Ok(()),
        Some(_) => {
            Err("The sign-in answer came from a different server than the one asked.".into())
        }
        None if iss_supported => Err("The sign-in answer didn't say which server sent it.".into()),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::http::fake::{json, resp};
    use super::*;
    use serde_json::json as j;

    #[test]
    fn provider_apis_without_a_resource_get_json_bodies_and_extra_params() {
        let c = Client {
            client_id: "cid".into(),
            client_secret: Some("sec".into()),
            auth: ClientAuth::ClientSecretBasic,
        };
        let r = refresh_request("https://api.notion.com/v1/oauth/token", &c, "rt", "");
        assert!(!r.body_text().contains("resource"));
        let r = as_json_body(r);
        let v: serde_json::Value = serde_json::from_slice(r.body.as_ref().unwrap()).unwrap();
        assert_eq!(v["grant_type"], "refresh_token");
        assert_eq!(v["refresh_token"], "rt");
        assert_eq!(r.header_value("content-type"), Some("application/json"));
        assert!(r
            .header_value("authorization")
            .unwrap()
            .starts_with("Basic "));
        let u = with_extra_params(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id=a",
            &[("access_type".into(), "offline".into())],
        )
        .unwrap();
        assert!(u.ends_with("client_id=a&access_type=offline"));
    }

    fn auth_server() -> AuthServer {
        AuthServer {
            issuer: "https://auth.example.com".into(),
            authorization_endpoint: "https://auth.example.com/authorize?tenant=t1".into(),
            token_endpoint: "https://auth.example.com/token".into(),
            registration_endpoint: Some("https://auth.example.com/register".into()),
            revocation_endpoint: Some("https://auth.example.com/revoke".into()),
            token_auth_methods: vec!["none".into()],
            ..Default::default()
        }
    }

    fn public_client() -> Client {
        Client {
            client_id: "cid".into(),
            client_secret: None,
            auth: ClientAuth::None,
        }
    }

    #[test]
    fn pkce_follows_rfc_7636() {
        // RFC 7636 appendix B.
        let p = Pkce::from_verifier("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".into());
        assert_eq!(p.challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
        let a = Pkce::new();
        let b = Pkce::new();
        assert_ne!(a.verifier, b.verifier);
        assert!(a.verifier.len() >= 43 && a.verifier.len() <= 128);
    }

    #[test]
    fn states_are_unique_and_stored_hashed() {
        let s1 = new_state();
        let s2 = new_state();
        assert_ne!(s1, s2);
        assert!(s1.len() >= 43);
        assert_ne!(state_key(&s1), s1);
        assert_eq!(state_key(&s1), state_key(&s1));
    }

    #[test]
    fn the_authorize_address_carries_pkce_state_and_resource() {
        let p = Pkce::new();
        let u = authorize_url(
            &auth_server(),
            "cid",
            "https://app.coderhelm.com/settings/integrations/mcp-callback/",
            &p,
            "st",
            &["read".into(), "write".into()],
            "https://mcp.example.com/mcp",
        )
        .unwrap();
        let u = reqwest::Url::parse(&u).unwrap();
        let q: std::collections::HashMap<_, _> = u.query_pairs().into_owned().collect();
        assert_eq!(q["tenant"], "t1");
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["code_challenge"], p.challenge);
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "st");
        assert_eq!(q["resource"], "https://mcp.example.com/mcp");
        assert_eq!(q["scope"], "read write");
        assert!(!q.contains_key("code_verifier"));
        // No scopes: no scope parameter.
        let u = authorize_url(
            &auth_server(),
            "c",
            "https://x.example.com/cb",
            &p,
            "s",
            &[],
            "r",
        )
        .unwrap();
        assert!(!u.contains("scope="));
    }

    #[test]
    fn registration_preference_follows_the_spec() {
        let mut a = auth_server();
        a.cimd_supported = true;
        let doc = Some("https://app.coderhelm.com/oauth/mcp-client.json");
        assert_eq!(choose_registration(&a, doc), Registration::MetadataDocument);
        assert_eq!(choose_registration(&a, None), Registration::Dynamic);
        a.registration_endpoint = None;
        assert_eq!(choose_registration(&a, None), Registration::Manual);
    }

    #[test]
    fn dynamic_registration_asks_for_a_public_client() {
        let r = registration_request(&auth_server(), "https://x.example.com/cb", &[]);
        let body: serde_json::Value = serde_json::from_slice(r.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["token_endpoint_auth_method"], "none");
        assert_eq!(body["redirect_uris"], j!(["https://x.example.com/cb"]));
        assert_eq!(body["client_uri"], "https://x.example.com");
        assert_eq!(
            body["grant_types"],
            j!(["authorization_code", "refresh_token"])
        );
        let c = parse_registration(&json(
            201,
            j!({"client_id": "abc", "token_endpoint_auth_method": "none"}),
        ))
        .unwrap();
        assert_eq!(
            c,
            Client {
                client_id: "abc".into(),
                client_secret: None,
                auth: ClientAuth::None
            }
        );
        let c =
            parse_registration(&json(201, j!({"client_id": "abc", "client_secret": "s"}))).unwrap();
        assert_eq!(c.auth, ClientAuth::ClientSecretBasic);
        assert!(
            parse_registration(&json(400, j!({"error": "invalid_redirect_uri"})))
                .unwrap_err()
                .contains("invalid_redirect_uri")
        );
        assert!(parse_registration(&json(201, j!({}))).is_err());
    }

    #[test]
    fn token_requests_carry_the_verifier_and_resource() {
        let r = token_request(
            &auth_server(),
            &public_client(),
            "code1",
            "ver1",
            "https://x.example.com/cb",
            "https://mcp.example.com/mcp",
        );
        let body = r.body_text();
        for part in [
            "grant_type=authorization_code",
            "code=code1",
            "code_verifier=ver1",
            "client_id=cid",
            "resource=https%3A%2F%2Fmcp.example.com%2Fmcp",
        ] {
            assert!(body.contains(part), "{part} in {body}");
        }
        assert!(r.header_value("Authorization").is_none());
        // A confidential client with basic auth: the secret goes in the header.
        let c = Client {
            client_id: "cid".into(),
            client_secret: Some("sec".into()),
            auth: ClientAuth::ClientSecretBasic,
        };
        let r = refresh_request(
            "https://auth.example.com/token",
            &c,
            "rt",
            "https://mcp.example.com/mcp",
        );
        assert!(r
            .header_value("Authorization")
            .unwrap()
            .starts_with("Basic "));
        assert!(!r.body_text().contains("sec"));
        assert!(r.body_text().contains("grant_type=refresh_token"));
        assert!(r.body_text().contains("resource="));
        let c = Client {
            auth: ClientAuth::ClientSecretPost,
            ..c
        };
        assert!(
            revoke_request("https://auth.example.com/revoke", &c, "rt", "refresh_token")
                .body_text()
                .contains("client_secret=sec")
        );
    }

    #[test]
    fn token_replies_are_read_and_refresh_tokens_kept() {
        let t = parse_token_response(&json(200, j!({"access_token": "at", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt", "scope": "read"})), 1000, None).unwrap();
        assert_eq!(t.expires_at, Some(4600));
        assert_eq!(t.refresh_token.as_deref(), Some("rt"));
        assert!(t.usable_at(1000) && !t.usable_at(4560));
        // A refresh that doesn't rotate keeps the old refresh token.
        let t2 = parse_token_response(
            &json(200, j!({"access_token": "at2", "token_type": "bearer"})),
            2000,
            Some(&t),
        )
        .unwrap();
        assert_eq!(t2.refresh_token.as_deref(), Some("rt"));
        assert_eq!(t2.expires_at, None);
        assert!(t2.usable_at(i64::MAX - 100));
        let e = parse_token_response(
            &json(
                400,
                j!({"error": "invalid_grant", "error_description": "expired"}),
            ),
            0,
            None,
        )
        .unwrap_err();
        assert!(e.needs_reconnect());
        assert!(!parse_token_response(&resp(503, &[], "busy"), 0, None)
            .unwrap_err()
            .needs_reconnect());
        assert!(parse_token_response(
            &json(200, j!({"access_token": "a", "token_type": "mac"})),
            0,
            None
        )
        .is_err());
        let secret = parse_token_response(
            &json(
                200,
                j!({"access_token": "sekrit-access", "refresh_token": "sekrit-refresh"}),
            ),
            0,
            None,
        )
        .unwrap();
        let shown = format!("{secret:?}");
        assert!(!shown.contains("sekrit"));
    }

    #[test]
    fn iss_is_checked_per_rfc_9207() {
        let rec = "https://auth.example.com";
        assert!(check_iss(true, rec, Some(rec)).is_ok());
        assert!(check_iss(true, rec, None).is_err());
        assert!(check_iss(false, rec, Some("https://evil.example.net")).is_err());
        // No normalization: a trailing slash is a different issuer.
        assert!(check_iss(false, rec, Some("https://auth.example.com/")).is_err());
        assert!(check_iss(false, rec, None).is_ok());
    }
}
