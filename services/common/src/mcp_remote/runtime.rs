//! Using a team's remote MCP servers: signing in, keeping tokens fresh,
//! listing and calling tools, disconnecting.
//!
//! The gateway (dashboard and plan chat) and the worker (planner, coder,
//! reviewer) both go through [`Runtime`], so the rules hold for every
//! caller:
//!
//! - Tokens are sealed by KMS for one team, one server and one owner (the
//!   team, or one person). They are opened for one request and never
//!   returned, logged or shown to a model.
//! - An access token about to expire is refreshed first; a refresh only
//!   lands if no other caller refreshed meanwhile (the record's version), so
//!   a rotated refresh token is never lost.
//! - A `401` gets one refresh and one retry. A grant the authorization
//!   server no longer honors marks the connection "needs reconnect" with the
//!   reason, and agents stop getting that server until someone reconnects.
//! - Only allowed, approved tools can be called; every call is written to
//!   the audit trail.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;

use super::client::{AuthHeader, CallError, Conn, Era, RemoteTool, SkippedTool, ToolOutput};
use super::discovery::{self, OAuthPlan};
use super::http::Http;
use super::oauth::{self, Client, ClientAuth, Registration, TokenSet};
use super::store::{
    AuthKind, LogEntry, OAuthSettings, Owner, PendingAuth, ServerRecord, Status, Store,
    TokenRecord, PENDING_TTL_SECS,
};
use super::tools;
use crate::mcp_credentials::Cipher;

const TOKEN_PURPOSE: &str = "mcp-oauth-token";
const CLIENT_PURPOSE: &str = "mcp-oauth-client";
const HEADER_PURPOSE: &str = "mcp-header";

/// KMS with the secrets key, owned (the worker and gateway keep one per
/// request). `key_id` is needed only to seal. Values larger than KMS
/// encrypts directly (a large sign-in's tokens) are sealed in an envelope
/// ([`crate::envelope`]).
pub struct KmsCipher {
    pub client: aws_sdk_kms::Client,
    pub key_id: String,
}

#[async_trait::async_trait]
impl Cipher for KmsCipher {
    async fn encrypt(
        &self,
        context: HashMap<String, String>,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, String> {
        crate::mcp_credentials::Kms {
            client: &self.client,
            key_id: &self.key_id,
        }
        .encrypt(context, plaintext)
        .await
    }
    async fn decrypt(
        &self,
        context: HashMap<String, String>,
        blob: &[u8],
    ) -> Result<Vec<u8>, String> {
        crate::mcp_credentials::Kms {
            client: &self.client,
            key_id: &self.key_id,
        }
        .decrypt(context, blob)
        .await
    }
}

/// Why a server can't be used right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnError {
    /// Nobody has signed in yet (for this owner).
    NotConnected,
    /// A person has to sign in again. The reason says why.
    NeedsReconnect(String),
    /// The server refuses the saved key, or no longer works as set up.
    Failed(String),
    /// Something passing (the server is down, a refresh is under way).
    Temporary(String),
    /// The server is overloaded, rate limiting, timed out or dropped the
    /// connection: asking again later works. `retry_after` is the wait the
    /// server named, in seconds.
    Busy {
        why: String,
        retry_after: Option<u64>,
        rate_limited: bool,
    },
}

impl ConnError {
    /// A failed call as a connection error: the server's overload and a
    /// dropped connection are [`ConnError::Busy`], anything else passing is
    /// [`ConnError::Temporary`].
    fn of_call(e: CallError) -> Self {
        match e {
            CallError::Busy {
                why,
                retry_after,
                rate_limited,
            } => ConnError::Busy {
                why,
                retry_after,
                rate_limited,
            },
            CallError::Unreachable(why) => ConnError::Busy {
                why,
                retry_after: None,
                rate_limited: false,
            },
            other => ConnError::Temporary(other.to_string()),
        }
    }
}

impl std::fmt::Display for ConnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnError::NotConnected => f.write_str("Not connected yet."),
            ConnError::NeedsReconnect(r)
            | ConnError::Failed(r)
            | ConnError::Temporary(r)
            | ConnError::Busy { why: r, .. } => f.write_str(r),
        }
    }
}

/// Waits before asking a sign-in server again, within one run, after a
/// passing refusal (a `5xx`, a `429`, no answer). After the last one the
/// run reports it, marks nothing, and the next sync asks again.
#[cfg(not(test))]
pub const REFRESH_BACKOFF_MS: &[u64] = &[500, 2_000];
#[cfg(test)]
pub const REFRESH_BACKOFF_MS: &[u64] = &[0, 0];

/// How long one refresher holds a token record ([`Store::claim_refresh`]):
/// longer than its tries take (three token requests and the waits between).
pub const REFRESH_LEASE_SECS: i64 = 60;

/// How long a refresher that found another at work waits for its tokens.
#[cfg(not(test))]
const OTHER_REFRESH_WAIT_MS: &[u64] = &[250, 500, 1_000, 2_000];
#[cfg(test)]
const OTHER_REFRESH_WAIT_MS: &[u64] = &[0, 0];

/// A failure to open the saved tokens that is the key service's, not the
/// tokens': throttled, unreachable or timed out. The tokens are kept and
/// nobody is asked to reconnect.
pub fn key_service_passing(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    [
        "throttl",
        "dispatch failure",
        "timeout",
        "timed out",
        "io error",
        "response error",
        "kmsinternal",
        "keyunavailable",
        "dependencytimeout",
        "service unavailable",
    ]
    .iter()
    .any(|w| e.contains(w))
}

/// How a server signs in, found by asking it.
#[derive(Debug, Clone, PartialEq)]
pub enum Detected {
    /// It answered without any sign-in.
    Open {
        tools: Vec<RemoteTool>,
        era: Era,
    },
    /// It wants a key we can't discover: a header and value from the vendor.
    Header,
    OAuth(Box<OAuthPlan>),
}

/// Asks a server how to sign in: an unauthenticated tool list, then the
/// authorization discovery if it answers `401`.
pub async fn detect(http: &dyn Http, url: &str) -> Result<Detected, String> {
    let conn = Conn {
        http,
        url,
        auth: None,
        era: None,
    };
    match conn.list_tools().await {
        Ok((tools, _, era)) => Ok(Detected::Open { tools, era }),
        Err(CallError::Unauthorized(ch)) => {
            match discovery::discover(http, url, ch.as_ref()).await? {
                Some(plan) => Ok(Detected::OAuth(Box::new(plan))),
                None => Ok(Detected::Header),
            }
        }
        Err(CallError::Forbidden(_)) => Ok(Detected::Header),
        Err(e) => Err(e.to_string()),
    }
}

/// How long a server's sign-in metadata is trusted before a sign-in reads
/// it again. Short: a vendor that moves its sign-in server (Atlassian
/// moved from `mcp.atlassian.com/v1` to `auth.atlassian.com`) is followed
/// within minutes, with no record to fix by hand.
pub const DISCOVERY_TTL_SECS: i64 = 300;

/// Whether a record's sign-in settings came from the server's own metadata
/// (a remote MCP server), and so are read again before each sign-in. A
/// provider's own API with addresses fixed in code (a knowledge source, a
/// catalog server's app, the hybrid broker) is not.
pub fn discovers(s: &ServerRecord) -> bool {
    let Some(o) = &s.oauth else { return false };
    s.auth == AuthKind::OAuth
        && !(super::store::is_token_only(&s.catalog_id)
            && o.registration == Registration::Manual.as_str())
}

/// The words for a sign-in page that is failing on the provider's side.
pub fn provider_down(name: &str, status: u16) -> String {
    format!(
        "{name}'s sign-in page is failing on {name}'s side (HTTP {status}), so Coderhelm didn't send you there. Nothing on your side needs changing. Try again in a few minutes."
    )
}

/// Asks the authorization page once before a person is sent to it. A `5xx`
/// (or no answer) means the provider's sign-in is down: the person gets
/// that in plain words instead of the provider's error page. Any other
/// answer (a login page, a redirect, a `4xx` about the request) is left for
/// the browser.
pub async fn probe_authorize(http: &dyn Http, name: &str, url: &str) -> Result<(), String> {
    let mut req = super::http::HttpRequest::get(url);
    req.headers
        .retain(|(k, _)| !k.eq_ignore_ascii_case("accept"));
    req.headers.push(("Accept".into(), "text/html".into()));
    match http.send(req).await {
        Ok(r) if r.status >= 500 => Err(provider_down(name, r.status)),
        Ok(_) | Err(super::http::HttpError::Refused(_)) => Ok(()),
        Err(super::http::HttpError::Unreachable(m)) => Err(format!(
            "Coderhelm couldn't reach {name}'s sign-in page ({m}). Try again in a few minutes."
        )),
    }
}

pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(|| chrono::Utc::now().timestamp())
}

/// What a provider sent back to the dashboard's callback page.
#[derive(Debug, Clone, Copy, Default)]
pub struct Answer<'a> {
    pub state: &'a str,
    pub code: Option<&'a str>,
    pub error: Option<&'a str>,
    /// The provider's own words for `error` (RFC 6749 4.1.2.1).
    pub error_description: Option<&'a str>,
    pub iss: Option<&'a str>,
}

/// A sign-in that finished.
#[derive(Debug, Clone)]
pub struct Finished {
    pub server: ServerRecord,
    pub owner: Owner,
    pub flow: oauth::Flow,
    /// Where the dashboard goes back to (empty: the flow's usual page).
    pub return_to: String,
}

/// A sign-in that didn't finish: why, in words a person can act on, and
/// what the callback page needs to send them back to where they started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failed {
    pub message: String,
    /// The provider's or token endpoint's error code, when there was one.
    pub code: String,
    /// The kind of sign-in, when known (from its record, else its `state`).
    pub flow: Option<oauth::Flow>,
    pub return_to: String,
    /// The server or source it was for, when the sign-in was matched.
    pub server_name: String,
    pub catalog_id: String,
}

/// A record's name as people know it ("Confluence", not "Confluence (knowledge)").
pub fn display_name(s: &ServerRecord) -> String {
    s.name.trim_end_matches(" (knowledge)").to_string()
}

/// The words for a provider's `error` answer, with its own description when
/// it sent one. Only `access_denied` with no other words reads as "not
/// granted", and even then it says the other reason providers use it for
/// (the account may not use the app), because Atlassian, Microsoft and
/// others answer `access_denied` for both.
pub fn provider_error(provider: &str, code: &str, description: &str) -> String {
    let who = if provider.trim().is_empty() {
        "The sign-in page"
    } else {
        provider.trim()
    };
    let desc: String = crate::truncate_str(description.trim(), 300)
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>();
    let said = if desc.is_empty() {
        String::new()
    } else {
        format!(" {who} said: \"{desc}\".")
    };
    match code {
        "access_denied" if desc.is_empty() => format!(
            "{who} answered access_denied: access wasn't granted. Either Cancel was chosen on its sign-in page, or {who} doesn't let this account use Coderhelm's app (for example, the app isn't shared with your organization, or your account can't reach the site). Start again, and ask an admin of {who} if it keeps happening."
        ),
        "access_denied" => format!("{who} didn't grant access (access_denied).{said}"),
        "invalid_scope" => format!(
            "{who} refused the permissions Coderhelm asked for (invalid_scope).{said} Whoever set up the app must add every scope listed in the sign-in setup to it."
        ),
        "unauthorized_client" | "invalid_client" | "invalid_request" => format!(
            "{who} refused the sign-in request ({code}).{said} The app's settings (client id, redirect address) don't match what {who} has registered."
        ),
        "temporarily_unavailable" | "server_error" => {
            format!("{who} had a problem finishing the sign-in ({code}).{said} Start it again.")
        }
        other => format!("{who} returned an error ({other}).{said}"),
    }
}

/// One team's remote servers.
#[derive(Clone)]
pub struct Runtime {
    pub store: Arc<dyn Store>,
    pub cipher: Arc<dyn Cipher>,
    pub http: Arc<dyn Http>,
    pub team: String,
    pub clock: Clock,
}

fn ctx(team: &str, server: &str, purpose: &str, owner: Option<&Owner>) -> HashMap<String, String> {
    let mut m = HashMap::from([
        ("team".to_string(), team.to_string()),
        ("server".to_string(), server.to_string()),
        ("purpose".to_string(), purpose.to_string()),
    ]);
    if let Some(o) = owner {
        m.insert("owner".into(), o.key());
    }
    m
}

impl Runtime {
    /// The production runtime: DynamoDB, KMS and the guarded HTTPS client.
    pub fn aws(
        dynamo: aws_sdk_dynamodb::Client,
        table: &str,
        kms: aws_sdk_kms::Client,
        key_id: &str,
        team: &str,
    ) -> Self {
        Self {
            store: Arc::new(super::store::DynamoStore {
                dynamo,
                table: table.to_string(),
            }),
            cipher: Arc::new(KmsCipher {
                client: kms,
                key_id: key_id.to_string(),
            }),
            http: Arc::new(super::http::GuardedHttps),
            team: team.to_string(),
            clock: system_clock(),
        }
    }

    fn now(&self) -> i64 {
        (self.clock)()
    }

    /// Seals `text`; `what` names it for the error when it is over the
    /// most one sealed value may hold ("Confluence's sign-in tokens").
    async fn seal(
        &self,
        c: HashMap<String, String>,
        text: &[u8],
        what: &str,
    ) -> Result<String, String> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        if let Some(e) = crate::envelope::too_large(what, text.len()) {
            return Err(e);
        }
        Ok(STANDARD.encode(self.cipher.encrypt(c, text).await?))
    }

    async fn open(&self, c: HashMap<String, String>, sealed: &str) -> Result<Vec<u8>, String> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let blob = STANDARD
            .decode(sealed)
            .map_err(|_| "stored value is not valid".to_string())?;
        self.cipher.decrypt(c, &blob).await
    }

    /// Seals a static key header value for one server.
    pub async fn seal_header(&self, server_id: &str, value: &str) -> Result<String, String> {
        self.seal(
            ctx(&self.team, server_id, HEADER_PURPOSE, None),
            value.as_bytes(),
            "The key header's value",
        )
        .await
    }

    /// Seals a pasted or registered client secret for one server.
    pub async fn seal_client_secret(
        &self,
        server_id: &str,
        secret: &str,
    ) -> Result<String, String> {
        self.seal(
            ctx(&self.team, server_id, CLIENT_PURPOSE, None),
            secret.as_bytes(),
            "The client secret",
        )
        .await
    }

    /// Seals a sign-in's tokens. `provider` names who sent them, for the
    /// error when they are too large to store.
    async fn seal_tokens(
        &self,
        server_id: &str,
        owner: &Owner,
        t: &TokenSet,
        provider: &str,
    ) -> Result<String, String> {
        let json = serde_json::to_vec(t).map_err(|_| "tokens are not valid".to_string())?;
        let who = if provider.trim().is_empty() {
            server_id
        } else {
            provider.trim()
        };
        self.seal(
            ctx(&self.team, server_id, TOKEN_PURPOSE, Some(owner)),
            &json,
            &format!("The sign-in tokens {who} sent (access and refresh token)"),
        )
        .await
    }

    async fn open_tokens(
        &self,
        server_id: &str,
        owner: &Owner,
        sealed: &str,
    ) -> Result<TokenSet, String> {
        let plain = self
            .open(
                ctx(&self.team, server_id, TOKEN_PURPOSE, Some(owner)),
                sealed,
            )
            .await?;
        serde_json::from_slice(&plain).map_err(|_| "stored tokens are not valid".to_string())
    }

    /// Coderhelm's client on the server's authorization server, its secret
    /// opened for this request.
    pub async fn client(&self, s: &ServerRecord) -> Result<Client, String> {
        let o = s
            .oauth
            .as_ref()
            .ok_or("This server doesn't sign in with OAuth.")?;
        if o.client_id.is_empty() {
            return Err("Coderhelm has no client on this server's sign-in server yet.".into());
        }
        let client_secret = if o.client_secret_sealed.is_empty() {
            None
        } else {
            let plain = self
                .open(
                    ctx(&self.team, &s.id, CLIENT_PURPOSE, None),
                    &o.client_secret_sealed,
                )
                .await?;
            Some(
                String::from_utf8(plain)
                    .map_err(|_| "stored client secret is not text".to_string())?,
            )
        };
        Ok(Client {
            client_id: o.client_id.clone(),
            client_secret,
            auth: o.client_auth,
        })
    }

    /// Records a connection's status: on the server for the team's
    /// connection, on the person's token for theirs.
    pub async fn mark(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        status: Status,
        reason: &str,
        step_up: &[String],
    ) {
        if *owner == Owner::Team {
            if let Err(e) = self
                .store
                .set_status(&self.team, &s.id, status, reason)
                .await
            {
                tracing::warn!(server = %s.id, error = %e, "Could not record MCP server status");
            }
        }
        if s.auth == AuthKind::OAuth {
            if let Err(e) = self
                .store
                .set_token_status(&self.team, &s.id, owner, status, reason, step_up)
                .await
            {
                tracing::warn!(server = %s.id, error = %e, "Could not record MCP connection status");
            }
        }
    }

    /// The header to sign requests with. For OAuth, refreshes first when the
    /// token is about to expire (or when `force`).
    pub async fn auth_header(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        force: bool,
    ) -> Result<Option<AuthHeader>, ConnError> {
        match s.auth {
            AuthKind::None => Ok(None),
            AuthKind::Header => {
                if s.header_sealed.is_empty() {
                    return Err(ConnError::NotConnected);
                }
                let v = self
                    .open(
                        ctx(&self.team, &s.id, HEADER_PURPOSE, None),
                        &s.header_sealed,
                    )
                    .await
                    .map_err(|_| {
                        ConnError::Failed("The saved key can't be opened. Save it again.".into())
                    })?;
                let v = String::from_utf8(v)
                    .map_err(|_| ConnError::Failed("The saved key is not text.".into()))?;
                Ok(Some(AuthHeader(s.header_name.clone(), v)))
            }
            AuthKind::OAuth => self.bearer(s, owner, force, None).await.map(Some),
        }
    }

    /// A new header after the server refused `refused` (a `401`): the
    /// tokens someone else minted meanwhile when there are some, else a
    /// refresh. A refresh token is never spent to replace a token that was
    /// already replaced.
    pub async fn auth_header_after(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        refused: Option<&AuthHeader>,
    ) -> Result<Option<AuthHeader>, ConnError> {
        match s.auth {
            AuthKind::OAuth => self
                .bearer(s, owner, true, refused.map(|h| h.1.as_str()))
                .await
                .map(Some),
            _ => self.auth_header(s, owner, true).await,
        }
    }

    async fn bearer(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        force: bool,
        refused: Option<&str>,
    ) -> Result<AuthHeader, ConnError> {
        let rec = self
            .store
            .get_token(&self.team, &s.id, owner)
            .await
            .map_err(ConnError::Temporary)?
            .ok_or(ConnError::NotConnected)?;
        if rec.status == Status::NeedsReconnect {
            return Err(ConnError::NeedsReconnect(rec.status_reason.clone()));
        }
        let set = match self.open_tokens(&s.id, owner, &rec.sealed).await {
            Ok(t) => t,
            // The key service didn't answer: the saved sign-in is fine.
            Err(e) if key_service_passing(&e) => {
                return Err(ConnError::Busy {
                    why: "The saved sign-in couldn't be opened just now (the key service didn't answer); retrying.".into(),
                    retry_after: None,
                    rate_limited: false,
                })
            }
            Err(_) => {
                let why = "The saved sign-in can't be opened. Reconnect.";
                self.mark(s, owner, Status::NeedsReconnect, why, &[]).await;
                return Err(ConnError::NeedsReconnect(why.into()));
            }
        };
        let now = self.now();
        let replaced = refused.is_some_and(|r| r != format!("Bearer {}", set.access_token));
        if (!force || replaced) && set.usable_at(now) {
            return Ok(AuthHeader(
                "Authorization".into(),
                format!("Bearer {}", set.access_token),
            ));
        }
        let Some(refresh_token) = set.refresh_token.clone() else {
            let why = "The sign-in expired and the server gave no way to renew it. Reconnect.";
            self.mark(s, owner, Status::NeedsReconnect, why, &[]).await;
            return Err(ConnError::NeedsReconnect(why.into()));
        };
        let o = s
            .oauth
            .as_ref()
            .ok_or_else(|| ConnError::Failed("No sign-in settings.".into()))?;
        let client = self.client(s).await.map_err(ConnError::Failed)?;
        let endpoint = if o.refresh_endpoint.is_empty() {
            &o.token_endpoint
        } else {
            &o.refresh_endpoint
        };
        // One refresher at a time: a provider that rotates refresh tokens
        // (Atlassian) would refuse the second use of the same one, and that
        // refusal would read as a revoked sign-in.
        let claimed = self
            .store
            .claim_refresh(
                &self.team,
                &s.id,
                owner,
                rec.version,
                now,
                now + REFRESH_LEASE_SECS,
            )
            .await
            .map_err(ConnError::Temporary)?;
        if !claimed {
            return self.await_other_refresh(s, owner, rec.version).await;
        }
        let provider = oauth::sign_in_provider(endpoint, &s.catalog_id, &display_name(s));
        let mut attempt = 0;
        let answer = loop {
            let mut req = oauth::refresh_request(endpoint, &client, &refresh_token, &o.resource);
            if o.token_body_json {
                req = oauth::as_json_body(req);
            }
            let got = match self.http.send(req).await {
                Ok(resp) => oauth::parse_token_response(&resp, self.now(), Some(&set)),
                Err(super::http::HttpError::Unreachable(m)) => Err(oauth::TokenError {
                    code: String::new(),
                    message: format!("Couldn't reach the sign-in server: {m}"),
                    status: 0,
                    retry_after: None,
                }),
                Err(super::http::HttpError::Refused(m)) => {
                    self.release_refresh(s, owner, now).await;
                    return Err(ConnError::Temporary(format!(
                        "Couldn't renew the sign-in: {m}"
                    )));
                }
            };
            match got {
                // A passing refusal is asked again within this run, a little
                // later each time; a wait the server named that is longer
                // than that is left to the next sync.
                Err(e)
                    if e.is_passing()
                        && attempt < REFRESH_BACKOFF_MS.len()
                        && e.retry_after.unwrap_or(0) * 1000 <= REFRESH_BACKOFF_MS[attempt] =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        REFRESH_BACKOFF_MS[attempt],
                    ))
                    .await;
                    attempt += 1;
                }
                other => break other,
            }
        };
        if answer.is_err() {
            // The next refresher may try at once (a check by hand right
            // after a sync that met an outage).
            self.release_refresh(s, owner, now).await;
        }
        match answer {
            Ok(new) => {
                let sealed = self
                    .seal_tokens(&s.id, owner, &new, &display_name(s))
                    .await
                    .map_err(ConnError::Temporary)?;
                let next = TokenRecord {
                    sealed,
                    expires_at: new.expires_at,
                    scope: new.scope.clone(),
                    version: rec.version + 1,
                    status: Status::Connected,
                    status_reason: String::new(),
                    ..rec.clone()
                };
                let landed = self
                    .store
                    .put_token(&self.team, &s.id, &next, Some(rec.version))
                    .await
                    .map_err(ConnError::Temporary)?;
                if landed {
                    return Ok(AuthHeader(
                        "Authorization".into(),
                        format!("Bearer {}", new.access_token),
                    ));
                }
                // Someone else refreshed first: use theirs.
                let theirs = self
                    .store
                    .get_token(&self.team, &s.id, owner)
                    .await
                    .map_err(ConnError::Temporary)?
                    .ok_or(ConnError::NotConnected)?;
                let t = self
                    .open_tokens(&s.id, owner, &theirs.sealed)
                    .await
                    .map_err(ConnError::Temporary)?;
                if t.usable_at(now) {
                    Ok(AuthHeader(
                        "Authorization".into(),
                        format!("Bearer {}", t.access_token),
                    ))
                } else {
                    Err(ConnError::Temporary(
                        "The sign-in is being renewed. Try again.".into(),
                    ))
                }
            }
            Err(e) if e.needs_reconnect() => {
                // A person signing in again meanwhile wrote new tokens: the
                // refusal was for the old ones.
                if let Some(h) = self.newer_tokens(s, owner, rec.version).await {
                    return Ok(h);
                }
                let why = format!("{} Reconnect to sign in again.", e.message);
                self.mark(s, owner, Status::NeedsReconnect, &why, &[]).await;
                Err(ConnError::NeedsReconnect(why))
            }
            // Down, overloaded, rate limiting or silent: nothing is marked
            // and the saved tokens stay; the next sync asks again.
            Err(e) if e.is_passing() => Err(ConnError::Busy {
                why: oauth::sign_in_unavailable(&provider, e.status, e.rate_limited()),
                retry_after: e.retry_after,
                rate_limited: e.rate_limited(),
            }),
            // Refused, but not the grant (a bad request, an odd reply):
            // nothing is marked, and the words are the server's.
            Err(e) => Err(ConnError::Temporary(e.message)),
        }
    }

    async fn release_refresh(&self, s: &ServerRecord, owner: &Owner, claimed_at: i64) {
        if let Err(e) = self
            .store
            .release_refresh(&self.team, &s.id, owner, claimed_at + REFRESH_LEASE_SECS)
            .await
        {
            tracing::warn!(server = %s.id, error = %e, "Could not release an MCP token refresh");
        }
    }

    /// The tokens someone else wrote after `version`, when they can be used.
    async fn newer_tokens(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        version: i64,
    ) -> Option<AuthHeader> {
        let t = self
            .store
            .get_token(&self.team, &s.id, owner)
            .await
            .ok()
            .flatten()?;
        if t.version == version || t.status == Status::NeedsReconnect {
            return None;
        }
        let set = self.open_tokens(&s.id, owner, &t.sealed).await.ok()?;
        set.usable_at(self.now()).then(|| {
            AuthHeader(
                "Authorization".into(),
                format!("Bearer {}", set.access_token),
            )
        })
    }

    /// Another refresher holds the record: waits a little for its tokens
    /// instead of sending the same refresh token a second time.
    async fn await_other_refresh(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        version: i64,
    ) -> Result<AuthHeader, ConnError> {
        for ms in OTHER_REFRESH_WAIT_MS {
            if let Some(h) = self.newer_tokens(s, owner, version).await {
                return Ok(h);
            }
            if let Ok(Some(t)) = self.store.get_token(&self.team, &s.id, owner).await {
                if t.status == Status::NeedsReconnect {
                    return Err(ConnError::NeedsReconnect(t.status_reason));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(*ms)).await;
        }
        if let Some(h) = self.newer_tokens(s, owner, version).await {
            return Ok(h);
        }
        Err(ConnError::Busy {
            why: "Another sync is renewing the sign-in right now; retrying.".into(),
            retry_after: None,
            rate_limited: false,
        })
    }

    /// Turns a refused request into the connection's status.
    async fn refused(&self, s: &ServerRecord, owner: &Owner, e: &CallError) -> ConnError {
        match (e, s.auth) {
            (CallError::Unauthorized(_), AuthKind::OAuth) => {
                let why = "The server didn't accept the saved sign-in. Reconnect.";
                self.mark(s, owner, Status::NeedsReconnect, why, &[]).await;
                ConnError::NeedsReconnect(why.into())
            }
            (CallError::Unauthorized(_), AuthKind::Header) => {
                let why = "The server refused the saved key. Save a new one.";
                self.mark(s, owner, Status::Failed, why, &[]).await;
                ConnError::Failed(why.into())
            }
            (CallError::Unauthorized(_), AuthKind::None) => {
                let why = "The server now asks for a sign-in. Remove it and add it again.";
                self.mark(s, owner, Status::Failed, why, &[]).await;
                ConnError::Failed(why.into())
            }
            (CallError::Forbidden(Some(ch)), AuthKind::OAuth)
                if ch.error.as_deref() == Some("insufficient_scope") =>
            {
                let scopes: Vec<String> = ch
                    .scope
                    .as_deref()
                    .unwrap_or("")
                    .split_whitespace()
                    .map(str::to_string)
                    .collect();
                let why = format!(
                    "The server needs more access{}. Reconnect to grant it.",
                    if scopes.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", scopes.join(" "))
                    }
                );
                self.mark(s, owner, Status::NeedsReconnect, &why, &scopes)
                    .await;
                ConnError::NeedsReconnect(why)
            }
            (other, _) => ConnError::Temporary(other.to_string()),
        }
    }

    fn era_of(s: &ServerRecord) -> Option<Era> {
        Era::parse(&s.era)
    }

    async fn remember_era(&self, s: &ServerRecord, era: Era) {
        if s.era != era.as_str() {
            let _ = self.store.set_era(&self.team, &s.id, era.as_str()).await;
        }
    }

    /// Lists the server's tools with the owner's connection.
    pub async fn list_tools(
        &self,
        s: &ServerRecord,
        owner: &Owner,
    ) -> Result<(Vec<RemoteTool>, Vec<SkippedTool>, Era), ConnError> {
        let mut auth = self.auth_header(s, owner, false).await?;
        for attempt in 0..2 {
            let conn = Conn {
                http: self.http.as_ref(),
                url: &s.url,
                auth: auth.clone(),
                era: Self::era_of(s),
            };
            match conn.list_tools().await {
                Ok(out) => {
                    self.remember_era(s, out.2).await;
                    return Ok(out);
                }
                Err(CallError::Unauthorized(_)) if attempt == 0 && s.auth == AuthKind::OAuth => {
                    auth = self.auth_header_after(s, owner, auth.as_ref()).await?;
                }
                Err(e @ (CallError::Unauthorized(_) | CallError::Forbidden(_))) => {
                    return Err(self.refused(s, owner, &e).await)
                }
                Err(e) => return Err(ConnError::of_call(e)),
            }
        }
        Err(ConnError::Temporary("The server didn't answer.".into()))
    }

    /// Lists tools and saves them on the server (the "check again" path).
    /// Tools the server no longer lists drop out of the allow list.
    pub async fn refresh_tool_list(
        &self,
        s: &mut ServerRecord,
        owner: &Owner,
    ) -> Result<(), ConnError> {
        let (tools, skipped, era) = self.list_tools(s, owner).await?;
        let names: std::collections::BTreeSet<String> =
            tools.iter().map(|t| t.name.clone()).collect();
        s.allowed = s.allowed.intersection(&names).cloned().collect();
        s.approved = s.approved.intersection(&names).cloned().collect();
        s.tools = tools;
        s.skipped = skipped;
        s.era = era.as_str().to_string();
        s.tools_listed_at = chrono::Utc::now().to_rfc3339();
        if *owner == Owner::Team || s.auth != AuthKind::OAuth {
            s.status = Status::Connected;
            s.status_reason.clear();
            s.status_at = s.tools_listed_at.clone();
        }
        self.store
            .put_server(&self.team, s)
            .await
            .map_err(ConnError::Temporary)
    }

    /// Calls one allowed tool, and writes the call to the audit trail.
    pub async fn call_tool(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        tool_name: &str,
        args: &Value,
        actor: &str,
    ) -> Result<ToolOutput, String> {
        let Some(tool) = tools::effective(s)
            .into_iter()
            .find(|t| t.name == tool_name)
            .cloned()
        else {
            return Err(format!("{}: that tool isn't available to agents.", s.name));
        };
        let started = Instant::now();
        let out = self.call_inner(s, owner, &tool, args).await;
        let entry = LogEntry {
            server_id: s.id.clone(),
            tool: tool.name.clone(),
            actor: actor.to_string(),
            owner: owner.key(),
            ok: matches!(&out, Ok(o) if !o.is_error),
            ms: started.elapsed().as_millis() as i64,
            detail: match &out {
                Ok(o) if o.is_error => "the tool reported an error".into(),
                Ok(_) => String::new(),
                Err(e) => e.clone(),
            },
            at: chrono::Utc::now().to_rfc3339(),
        };
        if let Err(e) = self.store.put_log(&self.team, &entry, self.now()).await {
            tracing::warn!(server = %s.id, error = %e, "MCP tool call not written to the audit trail");
        }
        out
    }

    async fn call_inner(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        tool: &RemoteTool,
        args: &Value,
    ) -> Result<ToolOutput, String> {
        let mut auth = self
            .auth_header(s, owner, false)
            .await
            .map_err(|e| format!("{}: {e}", s.name))?;
        for attempt in 0..2 {
            let conn = Conn {
                http: self.http.as_ref(),
                url: &s.url,
                auth: auth.clone(),
                era: Self::era_of(s),
            };
            match conn.call_tool(tool, args).await {
                Ok((out, era)) => {
                    self.remember_era(s, era).await;
                    return Ok(out);
                }
                Err(CallError::Unauthorized(_)) if attempt == 0 && s.auth == AuthKind::OAuth => {
                    auth = self
                        .auth_header_after(s, owner, auth.as_ref())
                        .await
                        .map_err(|e| format!("{}: {e}", s.name))?;
                }
                Err(e @ (CallError::Unauthorized(_) | CallError::Forbidden(Some(_)))) => {
                    return Err(format!("{}: {}", s.name, self.refused(s, owner, &e).await))
                }
                Err(e) => return Err(format!("{}: {e}", s.name)),
            }
        }
        Err(format!("{}: the server didn't answer.", s.name))
    }

    /// Calls a tool for Coderhelm itself (a knowledge source reading through
    /// a person's sign-in), not for an agent: the tool doesn't need to be on
    /// the agents' allow list, so the caller must only ever name read-only
    /// tools it knows. Signs with the owner's connection (renewed when about
    /// to expire, and once more on a `401`), sends the token to this server's
    /// own address only, and returns the whole result.
    pub async fn read_tool(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        tool: &RemoteTool,
        args: &Value,
    ) -> Result<Value, ConnError> {
        let mut auth = self.auth_header(s, owner, false).await?;
        for attempt in 0..2 {
            let conn = Conn {
                http: self.http.as_ref(),
                url: &s.url,
                auth: auth.clone(),
                era: Self::era_of(s),
            };
            match conn.call_tool_result(tool, args).await {
                Ok((out, era)) => {
                    self.remember_era(s, era).await;
                    return Ok(out);
                }
                Err(CallError::Unauthorized(_)) if attempt == 0 && s.auth == AuthKind::OAuth => {
                    auth = self.auth_header_after(s, owner, auth.as_ref()).await?;
                }
                Err(e @ (CallError::Unauthorized(_) | CallError::Forbidden(Some(_)))) => {
                    return Err(self.refused(s, owner, &e).await)
                }
                Err(e) => return Err(ConnError::of_call(e)),
            }
        }
        Err(ConnError::Temporary("The server didn't answer.".into()))
    }

    /// Starts a sign-in: makes sure Coderhelm has a client on the server's
    /// authorization server (metadata document, else dynamic registration),
    /// records the PKCE verifier and the expected issuer against a fresh
    /// `state` bound to this person, and returns where to send their browser.
    pub async fn start_sign_in(
        &self,
        s: &mut ServerRecord,
        owner: &Owner,
        user_sub: &str,
        user_email: &str,
        redirect_uri: &str,
        metadata_document_url: Option<&str>,
    ) -> Result<String, String> {
        self.start_sign_in_for(
            s,
            owner,
            user_sub,
            user_email,
            redirect_uri,
            metadata_document_url,
            None,
            "",
        )
        .await
    }

    /// The kind of sign-in a record's flow is when the caller doesn't say.
    pub fn flow_of(&self, s: &ServerRecord) -> oauth::Flow {
        if super::store::holding_expiry(&self.team, 0).is_some() {
            oauth::Flow::Broker
        } else if s.catalog_id.starts_with("source:") {
            oauth::Flow::Knowledge
        } else if super::own_app::catalog_of(&s.catalog_id).is_some() {
            oauth::Flow::App
        } else {
            oauth::Flow::Mcp
        }
    }

    /// [`Self::start_sign_in`] for a given kind of sign-in (`None`: worked
    /// out from the record) and a dashboard page to return to (checked with
    /// [`oauth::clean_return_path`]; empty for the flow's usual page). Both
    /// are kept with the pending sign-in, and the kind goes in the `state`.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_sign_in_for(
        &self,
        s: &mut ServerRecord,
        owner: &Owner,
        user_sub: &str,
        user_email: &str,
        redirect_uri: &str,
        metadata_document_url: Option<&str>,
        flow: Option<oauth::Flow>,
        return_to: &str,
    ) -> Result<String, String> {
        let flow = flow.unwrap_or_else(|| self.flow_of(s));
        let discovered = discovers(s);
        if discovered {
            self.rediscover(s, false).await?;
        }
        match self
            .start_once(
                s,
                owner,
                user_sub,
                user_email,
                redirect_uri,
                metadata_document_url,
                flow,
                return_to,
                discovered,
            )
            .await
        {
            // Settings read a few minutes ago may already be out of date:
            // read them again, once, and try with what the server says now.
            Err(e) if discovered && !self.just_discovered(s) => {
                if self.rediscover(s, true).await.is_err() {
                    return Err(e);
                }
                self.start_once(
                    s,
                    owner,
                    user_sub,
                    user_email,
                    redirect_uri,
                    metadata_document_url,
                    flow,
                    return_to,
                    discovered,
                )
                .await
            }
            r => r,
        }
    }

    fn just_discovered(&self, s: &ServerRecord) -> bool {
        s.oauth
            .as_ref()
            .is_some_and(|o| o.discovered_at >= self.now())
    }

    /// Reads a remote MCP server's sign-in settings again from its own
    /// metadata (its `401`, its protected resource metadata, its
    /// authorization server's metadata) unless they were read less than
    /// [`DISCOVERY_TTL_SECS`] ago (`force` reads them anyway). A catalog
    /// server the vendor moved gets the catalog's new address first. The
    /// client Coderhelm has on the authorization server is kept while the
    /// issuer is the same; a new issuer gets a new client. Saved when
    /// anything changed. When the server can't be asked, the saved settings
    /// stand (the sign-in itself then says what is wrong).
    pub async fn rediscover(&self, s: &mut ServerRecord, force: bool) -> Result<(), String> {
        let now = self.now();
        let mut changed = false;
        if let Some(url) = crate::mcp_catalog::moved_remote_url(&s.id, &s.catalog_id, &s.url) {
            s.url = url.to_string();
            changed = true;
        }
        let Some(old) = s.oauth.clone() else {
            return Ok(());
        };
        let fresh = old.discovered_at > 0 && now - old.discovered_at < DISCOVERY_TTL_SECS;
        if fresh && !force && !changed {
            return Ok(());
        }
        let plan = match detect(self.http.as_ref(), &s.url).await {
            Ok(Detected::OAuth(plan)) => plan,
            Err(e) if changed => return Err(e),
            other => {
                let why = match other {
                    Err(e) => e,
                    Ok(_) => "the server didn't ask for a sign-in".into(),
                };
                tracing::warn!(team = %self.team, server = %s.id, error = %why, "Sign-in settings could not be read again; using the saved ones");
                return Ok(());
            }
        };
        let mut o = OAuthSettings::from_plan(&plan);
        o.discovered_at = now;
        if o.issuer == old.issuer {
            o.client_id = old.client_id.clone();
            o.client_auth = old.client_auth;
            o.client_secret_sealed = old.client_secret_sealed.clone();
            o.registration = old.registration.clone();
        } else if old.registration == Registration::Manual.as_str() && !old.client_id.is_empty() {
            return Err(format!(
                "{} now signs in through {}, where the client id you pasted isn't registered. Register an OAuth app there and paste its client id.",
                display_name(s),
                o.issuer
            ));
        }
        o.extra_authorize = old.extra_authorize.clone();
        o.token_body_json = old.token_body_json;
        o.refresh_endpoint = old.refresh_endpoint.clone();
        if o.issuer != old.issuer || o.resource != old.resource {
            tracing::info!(team = %self.team, server = %s.id, from = %old.issuer, to = %o.issuer, resource = %o.resource, "A server's sign-in moved; following it");
        }
        s.oauth = Some(o);
        self.store.put_server(&self.team, s).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_once(
        &self,
        s: &mut ServerRecord,
        owner: &Owner,
        user_sub: &str,
        user_email: &str,
        redirect_uri: &str,
        metadata_document_url: Option<&str>,
        flow: oauth::Flow,
        return_to: &str,
        probe: bool,
    ) -> Result<String, String> {
        let mut o = s
            .oauth
            .clone()
            .ok_or("This server doesn't sign in with OAuth.")?;
        let stale_document = o.registration == Registration::MetadataDocument.as_str()
            && metadata_document_url != Some(o.client_id.as_str());
        if o.client_id.is_empty() || stale_document {
            match oauth::choose_registration(&o.auth_server(), metadata_document_url) {
                Registration::MetadataDocument => {
                    o.client_id = metadata_document_url.unwrap_or_default().to_string();
                    o.client_auth = ClientAuth::None;
                    o.client_secret_sealed.clear();
                    o.registration = Registration::MetadataDocument.as_str().into();
                }
                Registration::Dynamic => {
                    let req = oauth::registration_request(&o.auth_server(), redirect_uri, &o.scopes);
                    let resp = self.http.send(req).await.map_err(|e| e.to_string())?;
                    let c = oauth::parse_registration(&resp)?;
                    o.client_secret_sealed = match &c.client_secret {
                        Some(sec) => self.seal_client_secret(&s.id, sec).await?,
                        None => String::new(),
                    };
                    o.client_id = c.client_id;
                    o.client_auth = c.auth;
                    o.registration = Registration::Dynamic.as_str().into();
                }
                Registration::Manual => {
                    return Err("This server doesn't let Coderhelm register itself. Register an OAuth app with the vendor (redirect address below) and paste its client id.".into())
                }
            }
            s.oauth = Some(o.clone());
            self.store.put_server(&self.team, s).await?;
        }
        // Scopes the server asked for at run time are added to the rest.
        let mut scopes = o.scopes.clone();
        if let Ok(Some(t)) = self.store.get_token(&self.team, &s.id, owner).await {
            for sc in t.step_up {
                if !scopes.contains(&sc) {
                    scopes.push(sc);
                }
            }
        }
        // A new sign-in ends the ones it replaces: an earlier attempt by this
        // person for this connection can't finish any more, so its record
        // doesn't wait out its ten minutes (or longer, where the table
        // doesn't expire records on its own).
        self.end_pending(&s.id, Some(owner), Some(user_sub)).await;
        let pkce = oauth::Pkce::new();
        let state = oauth::new_state_for(flow);
        let url = oauth::authorize_url(
            &o.auth_server(),
            &o.client_id,
            redirect_uri,
            &pkce,
            &state,
            &scopes,
            &o.resource,
        )?;
        let url = oauth::with_extra_params(&url, &o.extra_authorize)?;
        // Nobody is sent to a sign-in page that is down; nothing is
        // recorded for a sign-in that can't start.
        if probe {
            probe_authorize(self.http.as_ref(), &display_name(s), &url).await?;
        }
        self.store
            .put_pending(
                &self.team,
                &PendingAuth {
                    key: oauth::state_key(&state),
                    server_id: s.id.clone(),
                    owner: owner.clone(),
                    user_sub: user_sub.to_string(),
                    user_email: user_email.to_ascii_lowercase(),
                    verifier: pkce.verifier.clone(),
                    issuer: o.issuer.clone(),
                    redirect_uri: redirect_uri.to_string(),
                    expires_at: self.now() + PENDING_TTL_SECS,
                    flow: flow.tag().to_string(),
                    return_to: oauth::clean_return_path(return_to),
                },
            )
            .await?;
        Ok(url)
    }

    /// Ends the team's sign-ins in progress for `server_id` (for one owner
    /// and one person when given, else all of them), and every one of the
    /// team's that has expired. Returns how many were removed. Never fails:
    /// a record that stays is refused by [`Self::finish`] once it expires.
    pub async fn end_pending(
        &self,
        server_id: &str,
        owner: Option<&Owner>,
        user_sub: Option<&str>,
    ) -> usize {
        let now = self.now();
        let Ok(all) = self.store.list_pending(&self.team).await else {
            return 0;
        };
        let mut ended = 0;
        for p in all {
            let replaced = p.server_id == server_id
                && owner.is_none_or(|o| *o == p.owner)
                && user_sub.is_none_or(|u| u == p.user_sub);
            if (replaced || now > p.expires_at)
                && self.store.delete_pending(&self.team, &p.key).await.is_ok()
            {
                ended += 1;
            }
        }
        ended
    }

    /// Removes the team's expired sign-ins in progress (see [`Self::end_pending`]).
    pub async fn sweep_pending(&self) -> usize {
        self.end_pending("", None, None).await
    }

    /// Finishes a sign-in from the callback. The `state` is consumed first
    /// (it works once), must belong to this person and this team, and the
    /// answer's `iss` must match the issuer recorded at the start. Only then
    /// is the code exchanged.
    pub async fn finish_sign_in(
        &self,
        state: &str,
        code: Option<&str>,
        error: Option<&str>,
        iss: Option<&str>,
        user_sub: &str,
        user_email: &str,
    ) -> Result<(ServerRecord, Owner), String> {
        let a = Answer {
            state,
            code,
            error,
            error_description: None,
            iss,
        };
        self.finish(&a, user_sub, user_email)
            .await
            .map(|f| (f.server, f.owner))
            .map_err(|f| f.message)
    }

    /// [`Self::finish_sign_in`] with the whole answer (the provider's
    /// `error_description` too), saying which kind of sign-in it was and
    /// where it goes back to, whether it finished or not, so the callback
    /// page can send the person back to the page that started it and show
    /// the provider's own reason.
    pub async fn finish(
        &self,
        a: &Answer<'_>,
        user_sub: &str,
        user_email: &str,
    ) -> Result<Finished, Failed> {
        let unmatched = |message: &str| Failed {
            message: message.to_string(),
            code: String::new(),
            flow: oauth::Flow::of_state(a.state),
            return_to: String::new(),
            server_name: String::new(),
            catalog_id: String::new(),
        };
        let p = self
            .store
            .take_pending(&self.team, &oauth::state_key(a.state))
            .await
            .map_err(|e| unmatched(&e))?
            .ok_or_else(|| {
                unmatched("That sign-in was already used or has expired. Start it again.")
            })?;
        let s = self.store.get_server(&self.team, &p.server_id).await;
        let s = match s {
            Ok(Some(s)) => s,
            Ok(None) => {
                let mut f = unmatched("That server was removed while you were signing in.");
                f.flow = oauth::Flow::parse(&p.flow).or(f.flow);
                f.return_to = p.return_to.clone();
                return Err(f);
            }
            Err(e) => return Err(unmatched(&e)),
        };
        let flow = oauth::Flow::parse(&p.flow).unwrap_or_else(|| self.flow_of(&s));
        let fail = |message: String, code: &str| Failed {
            message,
            code: code.to_string(),
            flow: Some(flow),
            return_to: p.return_to.clone(),
            server_name: display_name(&s),
            catalog_id: s.catalog_id.clone(),
        };
        p.check(user_sub, user_email, self.now())
            .map_err(|e| fail(e, ""))?;
        let o = s
            .oauth
            .clone()
            .ok_or_else(|| fail("This server doesn't sign in with OAuth.".into(), ""))?;
        if o.issuer != p.issuer {
            return Err(fail(
                "The server's sign-in settings changed while you were signing in. Start again."
                    .into(),
                "",
            ));
        }
        oauth::check_iss(o.iss_supported, &p.issuer, a.iss).map_err(|e| fail(e, ""))?;
        if let Some(err) = a.error.filter(|e| !e.is_empty()) {
            let code = crate::truncate_str(err.trim(), 80).to_string();
            return Err(fail(
                provider_error(&display_name(&s), &code, a.error_description.unwrap_or("")),
                &code,
            ));
        }
        let code = a
            .code
            .filter(|c| !c.is_empty())
            .ok_or_else(|| fail("The sign-in page sent no code. Start it again.".into(), ""))?;
        let client = self.client(&s).await.map_err(|e| fail(e, ""))?;
        let mut req = oauth::token_request(
            &o.auth_server(),
            &client,
            code,
            &p.verifier,
            &p.redirect_uri,
            &o.resource,
        );
        if o.token_body_json {
            req = oauth::as_json_body(req);
        }
        let resp = self.http.send(req).await.map_err(|e| {
            fail(
                format!("Coderhelm couldn't reach the sign-in server: {e}"),
                "",
            )
        })?;
        let now = self.now();
        let set =
            oauth::parse_token_response(&resp, now, None).map_err(|e| fail(e.message, &e.code))?;
        let sealed = self
            .seal_tokens(&s.id, &p.owner, &set, &display_name(&s))
            .await
            .map_err(|e| fail(e, ""))?;
        let version = self
            .store
            .get_token(&self.team, &s.id, &p.owner)
            .await
            .map_err(|e| fail(e, ""))?
            .map(|t| t.version + 1)
            .unwrap_or(1);
        self.store
            .put_token(
                &self.team,
                &s.id,
                &TokenRecord {
                    owner: p.owner.clone(),
                    sealed,
                    expires_at: set.expires_at,
                    scope: set.scope,
                    version,
                    status: Status::Connected,
                    status_reason: String::new(),
                    step_up: Vec::new(),
                    connected_by: user_email.to_ascii_lowercase(),
                    connected_at: chrono::Utc::now().to_rfc3339(),
                },
                None,
            )
            .await
            .map_err(|e| fail(e, ""))?;
        if p.owner == Owner::Team {
            self.store
                .set_status(&self.team, &s.id, Status::Connected, "")
                .await
                .map_err(|e| fail(e, ""))?;
        }
        Ok(Finished {
            return_to: p.return_to.clone(),
            server: s,
            owner: p.owner,
            flow,
        })
    }

    /// Disconnects one owner: revokes the tokens at the authorization server
    /// (when it offers revocation) and deletes them. Returns whether the
    /// server confirmed a revocation.
    pub async fn disconnect(&self, s: &ServerRecord, owner: &Owner) -> Result<bool, String> {
        let Some(rec) = self.store.get_token(&self.team, &s.id, owner).await? else {
            return Ok(false);
        };
        let mut revoked = false;
        let endpoint = s.oauth.as_ref().and_then(|o| o.revocation_endpoint.clone());
        if let (Some(ep), Ok(set)) = (endpoint, self.open_tokens(&s.id, owner, &rec.sealed).await) {
            if let Ok(client) = self.client(s).await {
                let mut pairs = vec![(set.access_token.clone(), "access_token")];
                if let Some(rt) = &set.refresh_token {
                    pairs.insert(0, (rt.clone(), "refresh_token"));
                }
                for (tok, hint) in pairs {
                    match self
                        .http
                        .send(oauth::revoke_request(&ep, &client, &tok, hint))
                        .await
                    {
                        Ok(r) if r.status == 200 => revoked = true,
                        Ok(r) => {
                            tracing::info!(server = %s.id, status = r.status, "MCP token revocation not confirmed")
                        }
                        Err(e) => {
                            tracing::info!(server = %s.id, error = %e, "MCP token revocation failed")
                        }
                    }
                }
            }
        }
        self.store.delete_token(&self.team, &s.id, owner).await?;
        if *owner == Owner::Team {
            self.store
                .set_status(&self.team, &s.id, Status::NotConnected, "Disconnected.")
                .await?;
        }
        Ok(revoked)
    }

    /// The servers agents may use right now on behalf of `user` (a person's
    /// email, when one is known), each with the owner whose connection is
    /// used: connected, with at least one allowed and approved tool. A
    /// personal server is left out when no person is known or they haven't
    /// connected; a connection that needs reconnecting is left out too.
    pub async fn usable(&self, user: Option<&str>) -> Vec<(ServerRecord, Owner)> {
        let servers = match self.store.list_servers(&self.team).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "Could not list remote MCP servers");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for s in servers {
            if tools::effective(&s).is_empty() {
                continue;
            }
            let Some(owner) = s.owner_for(user) else {
                continue;
            };
            let ok = match s.auth {
                AuthKind::OAuth => matches!(
                    self.store.get_token(&self.team, &s.id, &owner).await,
                    Ok(Some(t)) if t.status == Status::Connected
                ),
                _ => s.status == Status::Connected,
            };
            if ok {
                out.push((s, owner));
            }
        }
        out
    }

    /// Takes a finished sign-in from another partition into this one: the
    /// tokens are opened under the other's keys, sealed again under this
    /// one's, and the other partition's records are deleted. The hosted
    /// broker finishes a hybrid install's provider sign-in in a holding
    /// partition and calls this only when the install redeems the code (with
    /// its secret and the PKCE verifier), so a sign-in someone else started
    /// never replaces the install's own.
    pub async fn adopt_sign_in(
        &self,
        from: &Runtime,
        server_id: &str,
        owner: &Owner,
    ) -> Result<(), String> {
        let t = from
            .store
            .get_token(&from.team, server_id, owner)
            .await?
            .ok_or("There is no finished sign-in to take.")?;
        let set = from.open_tokens(server_id, owner, &t.sealed).await?;
        let name = from
            .store
            .get_server(&from.team, server_id)
            .await
            .ok()
            .flatten()
            .map(|s| display_name(&s))
            .unwrap_or_default();
        let sealed = self.seal_tokens(server_id, owner, &set, &name).await?;
        let version = self
            .store
            .get_token(&self.team, server_id, owner)
            .await?
            .map(|x| x.version + 1)
            .unwrap_or(1);
        self.store
            .put_token(
                &self.team,
                server_id,
                &TokenRecord {
                    sealed,
                    version,
                    ..t
                },
                None,
            )
            .await?;
        if *owner == Owner::Team {
            self.store
                .set_status(&self.team, server_id, Status::Connected, "")
                .await?;
        }
        from.store
            .delete_token(&from.team, server_id, owner)
            .await?;
        from.store.delete_server(&from.team, server_id).await
    }

    /// Removes a server: every connection is disconnected (and revoked)
    /// first, then the server goes.
    pub async fn remove(&self, s: &ServerRecord) -> Result<(), String> {
        for t in self.store.list_tokens(&self.team, &s.id).await? {
            self.disconnect(s, &t.owner).await?;
        }
        self.end_pending(&s.id, None, None).await;
        self.store.delete_server(&self.team, &s.id).await
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! Test helpers for code that runs on a [`Runtime`] (here and in
    //! `knowledge::sources`).

    use super::*;

    /// Stands in for KMS: opening under any other context fails.
    pub struct FakeKms;
    fn key(c: &HashMap<String, String>) -> String {
        let mut v: Vec<_> = c.iter().map(|(k, v)| format!("{k}={v}")).collect();
        v.sort();
        v.join("&")
    }
    #[async_trait::async_trait]
    impl Cipher for FakeKms {
        async fn encrypt(&self, c: HashMap<String, String>, p: &[u8]) -> Result<Vec<u8>, String> {
            let mut out = key(&c).into_bytes();
            out.push(0);
            out.extend(p.iter().map(|b| b ^ 0x33));
            Ok(out)
        }
        async fn decrypt(&self, c: HashMap<String, String>, b: &[u8]) -> Result<Vec<u8>, String> {
            let at = b.iter().position(|x| *x == 0).ok_or("bad")?;
            if b[..at] != *key(&c).as_bytes() {
                return Err("kms decrypt: InvalidCiphertextException".into());
            }
            Ok(b[at + 1..].iter().map(|x| x ^ 0x33).collect())
        }
    }

    /// Saves a sign-in for `owner` on `server_id`, as a finished sign-in
    /// would.
    pub async fn sign_in(
        rt: &Runtime,
        server_id: &str,
        owner: &Owner,
        access: &str,
        refresh: Option<&str>,
        expires_at: Option<i64>,
        by: &str,
    ) {
        let set = TokenSet {
            access_token: access.into(),
            refresh_token: refresh.map(str::to_string),
            expires_at,
            scope: String::new(),
        };
        let sealed = rt.seal_tokens(server_id, owner, &set, "").await.unwrap();
        rt.store
            .put_token(
                &rt.team,
                server_id,
                &TokenRecord {
                    owner: owner.clone(),
                    sealed,
                    expires_at,
                    scope: String::new(),
                    version: 1,
                    status: Status::Connected,
                    status_reason: String::new(),
                    step_up: vec![],
                    connected_by: by.into(),
                    connected_at: String::new(),
                },
                None,
            )
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::super::http::fake::{json as fjson, resp, Fake};
    use super::super::http::Method;
    use super::super::store::fake::MemStore;
    use super::super::store::{OAuthSettings, Sharing};
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicI64, Ordering};

    const MCP: &str = "https://mcp.example.com/mcp";
    const TOKEN: &str = "https://auth.example.com/token";
    const REVOKE: &str = "https://auth.example.com/revoke";

    use super::testing::FakeKms;

    struct Rig {
        rt: Runtime,
        store: Arc<MemStore>,
        http: Arc<Fake>,
        now: Arc<AtomicI64>,
    }

    fn rig(team: &str) -> Rig {
        let store = Arc::new(MemStore::default());
        let http = Arc::new(Fake::default());
        let now = Arc::new(AtomicI64::new(1_000_000));
        let n2 = now.clone();
        Rig {
            rt: Runtime {
                store: store.clone(),
                cipher: Arc::new(FakeKms),
                http: http.clone(),
                team: team.into(),
                clock: Arc::new(move || n2.load(Ordering::SeqCst)),
            },
            store,
            http,
            now,
        }
    }

    /// A hybrid install's provider sign-in is finished in a holding
    /// partition and only taken into the install's own once the install
    /// redeems the code: the tokens move resealed under the install's keys,
    /// and nothing is left behind.
    #[tokio::test]
    async fn a_sign_in_is_adopted_from_a_holding_partition() {
        let store = Arc::new(MemStore::default());
        let http = Arc::new(Fake::default());
        let mk = |team: &str| Runtime {
            store: store.clone(),
            cipher: Arc::new(FakeKms),
            http: http.clone(),
            team: team.into(),
            clock: Arc::new(|| 1_000_000),
        };
        let main = mk("HYBRID#ins_a");
        let hold = mk("HYBRID#ins_a#SIGNIN#fl_1");
        let s = server();
        store.put_server(&main.team, &s).await.unwrap();
        store.put_server(&hold.team, &s).await.unwrap();
        super::testing::sign_in(&main, &s.id, &Owner::Team, "old", None, None, "a").await;
        super::testing::sign_in(&hold, &s.id, &Owner::Team, "new", Some("r"), None, "b").await;

        main.adopt_sign_in(&hold, &s.id, &Owner::Team)
            .await
            .unwrap();

        let t = store
            .get_token(&main.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        let set = main
            .open_tokens(&s.id, &Owner::Team, &t.sealed)
            .await
            .unwrap();
        assert_eq!(set.access_token, "new");
        assert_eq!(set.refresh_token.as_deref(), Some("r"));
        assert_eq!(t.version, 2);
        assert!(store
            .get_token(&hold.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .is_none());
        assert!(store.get_server(&hold.team, &s.id).await.unwrap().is_none());
        // Nothing held: refused, and the install's sign-in stays.
        assert!(main
            .adopt_sign_in(&hold, &s.id, &Owner::Team)
            .await
            .is_err());
        let t = store
            .get_token(&main.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            main.open_tokens(&s.id, &Owner::Team, &t.sealed)
                .await
                .unwrap()
                .access_token,
            "new"
        );
    }

    fn server() -> ServerRecord {
        let mut s = ServerRecord::new("docs", "Docs", MCP);
        s.auth = AuthKind::OAuth;
        s.oauth = Some(OAuthSettings {
            issuer: "https://auth.example.com".into(),
            authorization_endpoint: "https://auth.example.com/authorize".into(),
            token_endpoint: TOKEN.into(),
            registration_endpoint: Some("https://auth.example.com/register".into()),
            revocation_endpoint: Some(REVOKE.into()),
            iss_supported: true,
            resource: MCP.into(),
            scopes: vec!["read".into()],
            client_id: "cid".into(),
            registration: "dynamic".into(),
            ..Default::default()
        });
        s.tools = vec![
            RemoteTool {
                name: "search".into(),
                title: None,
                description: String::new(),
                input_schema: json!({"type": "object"}),
                read_only: Some(true),
                destructive: None,
            },
            RemoteTool {
                name: "delete_all".into(),
                title: None,
                description: String::new(),
                input_schema: json!({"type": "object"}),
                read_only: Some(false),
                destructive: Some(true),
            },
        ];
        s.allowed = ["search".to_string()].into();
        s.approved = s.allowed.clone();
        s
    }

    async fn connect(r: &Rig, s: &ServerRecord, owner: &Owner, tokens: TokenSet) {
        let sealed = r.rt.seal_tokens(&s.id, owner, &tokens, "").await.unwrap();
        r.store
            .put_token(
                &r.rt.team,
                &s.id,
                &TokenRecord {
                    owner: owner.clone(),
                    sealed,
                    expires_at: tokens.expires_at,
                    scope: String::new(),
                    version: 1,
                    status: Status::Connected,
                    status_reason: String::new(),
                    step_up: vec![],
                    connected_by: "a@x.com".into(),
                    connected_at: String::new(),
                },
                None,
            )
            .await
            .unwrap();
    }

    fn tokens(at: &str, exp: Option<i64>) -> TokenSet {
        TokenSet {
            access_token: at.into(),
            refresh_token: Some("rt-1".into()),
            expires_at: exp,
            scope: "read".into(),
        }
    }

    fn answer_tools(f: &Fake, bearer: &'static str) {
        f.with(move |r| {
            (r.url == MCP).then(|| {
                if r.header_value("Authorization") == Some(bearer) {
                    let body: Value = serde_json::from_slice(r.body.as_ref().unwrap()).unwrap();
                    if body["method"] == "tools/call" {
                        fjson(200, json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": "found"}]}}))
                    } else {
                        fjson(200, json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "search", "annotations": {"readOnlyHint": true}}]}}))
                    }
                } else {
                    resp(401, &[("www-authenticate", "Bearer error=\"invalid_token\"")], "")
                }
            })
        });
    }

    #[tokio::test]
    async fn a_fresh_token_is_used_as_is() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 3600))).await;
        answer_tools(&r.http, "Bearer at-1");
        let out =
            r.rt.call_tool(&s, &Owner::Team, "search", &json!({}), "planner")
                .await
                .unwrap();
        assert_eq!(out.text, "found");
        assert!(r.http.sent_to(TOKEN).is_empty());
        // The call is in the audit trail, without any token in it.
        let logs = r.store.list_logs("TEAM#a", "docs", 10).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].ok && logs[0].actor == "planner");
    }

    #[tokio::test]
    async fn an_expiring_token_is_refreshed_and_the_new_one_saved() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 30))).await;
        r.http.on(Method::Post, TOKEN, fjson(200, json!({"access_token": "at-2", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt-2"})));
        answer_tools(&r.http, "Bearer at-2");
        r.rt.call_tool(&s, &Owner::Team, "search", &json!({}), "coder")
            .await
            .unwrap();
        let sent = r.http.sent_to(TOKEN);
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body_text().contains("refresh_token=rt-1"));
        assert!(sent[0].body_text().contains("resource="));
        let rec = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.version, 2);
        let saved =
            r.rt.open_tokens("docs", &Owner::Team, &rec.sealed)
                .await
                .unwrap();
        assert_eq!(saved.access_token, "at-2");
        assert_eq!(saved.refresh_token.as_deref(), Some("rt-2"));
    }

    #[tokio::test]
    async fn a_401_gets_one_refresh_and_retry() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        // Looks fresh, but the server already revoked it.
        connect(
            &r,
            &s,
            &Owner::Team,
            tokens("at-old", Some(1_000_000 + 3600)),
        )
        .await;
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": "at-new", "token_type": "Bearer", "expires_in": 3600}),
            ),
        );
        answer_tools(&r.http, "Bearer at-new");
        let out =
            r.rt.call_tool(&s, &Owner::Team, "search", &json!({}), "coder")
                .await
                .unwrap();
        assert_eq!(out.text, "found");
        assert_eq!(r.http.sent_to(TOKEN).len(), 1);
    }

    #[tokio::test]
    async fn a_dead_grant_marks_needs_reconnect_with_the_reason() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 - 10))).await;
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                400,
                json!({"error": "invalid_grant", "error_description": "refresh token revoked"}),
            ),
        );
        let e =
            r.rt.call_tool(&s, &Owner::Team, "search", &json!({}), "coder")
                .await
                .unwrap_err();
        assert!(e.contains("Reconnect"));
        let back = r.store.get_server("TEAM#a", "docs").await.unwrap().unwrap();
        assert_eq!(back.status, Status::NeedsReconnect);
        assert!(back.status_reason.contains("refresh token revoked"));
        // While it needs reconnecting, nothing is sent at all.
        let before = r.http.sent().len();
        assert!(r
            .rt
            .call_tool(&s, &Owner::Team, "search", &json!({}), "coder")
            .await
            .is_err());
        assert_eq!(r.http.sent().len(), before);
        // A failed call is audited too.
        assert!(r
            .store
            .list_logs("TEAM#a", "docs", 10)
            .await
            .unwrap()
            .iter()
            .all(|l| !l.ok));
    }

    #[tokio::test]
    async fn a_lost_refresh_race_uses_the_winners_token() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 10))).await;
        // While our refresh is in flight, another caller saves a newer token.
        let store = r.store.clone();
        let sealed_winner =
            r.rt.seal_tokens(
                "docs",
                &Owner::Team,
                &tokens("at-winner", Some(1_000_000 + 3600)),
                "",
            )
            .await
            .unwrap();
        r.http.with(move |req| {
            (req.url == TOKEN).then(|| {
                let mut items = store.items.lock().unwrap();
                let k = ("TEAM#a".to_string(), "MCPTOKEN#docs#team".to_string());
                let item = items.get_mut(&k).unwrap();
                item.insert(
                    "version".into(),
                    aws_sdk_dynamodb::types::AttributeValue::N("2".into()),
                );
                item.insert(
                    "token_sealed".into(),
                    aws_sdk_dynamodb::types::AttributeValue::S(sealed_winner.clone()),
                );
                fjson(
                    200,
                    json!({"access_token": "at-mine", "token_type": "Bearer", "expires_in": 3600}),
                )
            })
        });
        let h =
            r.rt.auth_header(&s, &Owner::Team, false)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(h.1, "Bearer at-winner");
    }

    /// The saved sign-in is untouched: still connected, same version, same
    /// tokens, and nothing on the server record asks for a reconnect.
    async fn untouched(r: &Rig) {
        let t = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((t.status, t.version), (Status::Connected, 1));
        assert!(t.status_reason.is_empty());
        let set =
            r.rt.open_tokens("docs", &Owner::Team, &t.sealed)
                .await
                .unwrap();
        assert_eq!(set.refresh_token.as_deref(), Some("rt-1"));
        let s = r.store.get_server("TEAM#a", "docs").await.unwrap().unwrap();
        assert_ne!(s.status, Status::NeedsReconnect);
    }

    async fn expired_rig() -> (Rig, ServerRecord) {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 - 10))).await;
        (r, s)
    }

    fn busy(e: ConnError) -> (String, Option<u64>, bool) {
        match e {
            ConnError::Busy {
                why,
                retry_after,
                rate_limited,
            } => (why, retry_after, rate_limited),
            other => panic!("expected a passing refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_503_from_the_sign_in_server_retries_and_never_asks_to_reconnect() {
        let (r, s) = expired_rig().await;
        r.http
            .on(Method::Post, TOKEN, resp(503, &[], "Service Unavailable"));
        let (why, _, limited) = busy(r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err());
        assert_eq!(
            why,
            "Docs's sign-in server was briefly unavailable (HTTP 503); retrying."
        );
        assert!(!why.contains("Reconnect") && !why.contains("invalid_grant") && !limited);
        // Asked again within the run, a little later each time.
        assert_eq!(r.http.sent_to(TOKEN).len(), 1 + REFRESH_BACKOFF_MS.len());
        untouched(&r).await;
        // The refresh it claimed is given back: a check by hand right after
        // asks the sign-in server itself instead of waiting.
        let before = r.http.sent_to(TOKEN).len();
        assert!(r.rt.auth_header(&s, &Owner::Team, false).await.is_err());
        assert_eq!(
            r.http.sent_to(TOKEN).len(),
            before + 1 + REFRESH_BACKOFF_MS.len()
        );
        // The next sync, with the server back, signs in: nothing was stored
        // that stops it.
        let fresh = rig_after(&r);
        assert!(fresh.rt.auth_header(&s, &Owner::Team, false).await.is_ok());
    }

    /// The same store and HTTP, with the first answer cleared (a later sync).
    fn rig_after(r: &Rig) -> Rig {
        let http = Arc::new(Fake::default());
        http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": "at-2", "token_type": "Bearer", "expires_in": 3600}),
            ),
        );
        // Half an hour later.
        let now = Arc::new(AtomicI64::new(1_000_000 + 1_800));
        let n2 = now.clone();
        Rig {
            rt: Runtime {
                store: r.store.clone(),
                cipher: Arc::new(FakeKms),
                http: http.clone(),
                team: "TEAM#a".into(),
                clock: Arc::new(move || n2.load(Ordering::SeqCst)),
            },
            store: r.store.clone(),
            http,
            now,
        }
    }

    #[tokio::test]
    async fn a_503_that_clears_within_the_run_signs_in() {
        let (r, s) = expired_rig().await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        r.http.with(move |req| {
            (req.url == TOKEN).then(|| {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    resp(503, &[], "")
                } else {
                    fjson(200, json!({"access_token": "at-2", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt-2"}))
                }
            })
        });
        let h =
            r.rt.auth_header(&s, &Owner::Team, false)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(h.1, "Bearer at-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_5xx_that_says_invalid_grant_is_still_passing() {
        let (r, s) = expired_rig().await;
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                502,
                json!({"error": "invalid_grant", "error_description": "upstream"}),
            ),
        );
        let (why, _, _) = busy(r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err());
        assert!(why.contains("(HTTP 502); retrying."), "{why}");
        untouched(&r).await;
    }

    #[tokio::test]
    async fn a_429_is_a_rate_limit_and_its_wait_is_left_to_the_next_sync() {
        let (r, s) = expired_rig().await;
        r.http.on(
            Method::Post,
            TOKEN,
            resp(429, &[("retry-after", "120")], ""),
        );
        let (why, after, limited) =
            busy(r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err());
        assert!(limited && after == Some(120), "{why}");
        assert!(
            why.contains("asked Coderhelm to slow down (HTTP 429)"),
            "{why}"
        );
        // A wait longer than the run's own is not slept through.
        assert_eq!(r.http.sent_to(TOKEN).len(), 1);
        untouched(&r).await;
    }

    /// A sign-in server that never answers (a timeout, a dropped
    /// connection).
    struct Silent(std::sync::atomic::AtomicUsize);
    #[async_trait::async_trait]
    impl Http for Silent {
        async fn send(
            &self,
            _: super::super::http::HttpRequest,
        ) -> Result<super::super::http::HttpResponse, super::super::http::HttpError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(super::super::http::HttpError::Unreachable(
                "operation timed out".into(),
            ))
        }
    }

    #[tokio::test]
    async fn a_sign_in_server_that_doesnt_answer_is_passing() {
        let (mut r, s) = expired_rig().await;
        let silent = Arc::new(Silent(Default::default()));
        r.rt.http = silent.clone();
        let (why, _, _) = busy(r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err());
        assert_eq!(
            why,
            "Docs's sign-in server was briefly unavailable; retrying."
        );
        assert_eq!(
            silent.0.load(Ordering::SeqCst),
            1 + REFRESH_BACKOFF_MS.len()
        );
        untouched(&r).await;
    }

    #[tokio::test]
    async fn a_refused_request_that_isnt_the_grant_marks_nothing() {
        let (r, s) = expired_rig().await;
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                400,
                json!({"error": "invalid_request", "error_description": "missing resource"}),
            ),
        );
        let e = r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err();
        assert!(
            matches!(&e, ConnError::Temporary(m) if m.contains("invalid_request")),
            "{e:?}"
        );
        assert!(!e.to_string().contains("Reconnect"));
        untouched(&r).await;
    }

    #[tokio::test]
    async fn only_a_definitive_refusal_asks_for_a_reconnect() {
        for (status, code) in [(400, "invalid_grant"), (401, "invalid_client")] {
            let (r, s) = expired_rig().await;
            r.http
                .on(Method::Post, TOKEN, fjson(status, json!({"error": code})));
            let e = r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err();
            assert!(
                matches!(e, ConnError::NeedsReconnect(_)),
                "{status} {code}: {e:?}"
            );
            // A definitive refusal is asked once, never again in the run.
            assert_eq!(r.http.sent_to(TOKEN).len(), 1);
        }
    }

    #[tokio::test]
    async fn a_refusal_after_someone_signed_in_again_uses_the_new_sign_in() {
        let (r, s) = expired_rig().await;
        let store = r.store.clone();
        let theirs =
            r.rt.seal_tokens(
                "docs",
                &Owner::Team,
                &tokens("at-new", Some(1_000_000 + 3600)),
                "",
            )
            .await
            .unwrap();
        r.http.with(move |req| {
            (req.url == TOKEN).then(|| {
                let mut items = store.items.lock().unwrap();
                let item = items
                    .get_mut(&("TEAM#a".to_string(), "MCPTOKEN#docs#team".to_string()))
                    .unwrap();
                item.insert(
                    "version".into(),
                    aws_sdk_dynamodb::types::AttributeValue::N("2".into()),
                );
                item.insert(
                    "token_sealed".into(),
                    aws_sdk_dynamodb::types::AttributeValue::S(theirs.clone()),
                );
                fjson(
                    400,
                    json!({"error": "invalid_grant", "error_description": "already used"}),
                )
            })
        });
        let h =
            r.rt.auth_header(&s, &Owner::Team, false)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(h.1, "Bearer at-new");
        let t = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.status, Status::Connected);
    }

    #[tokio::test]
    async fn two_refreshers_at_once_send_the_rotating_refresh_token_once() {
        let (r, s) = expired_rig().await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        r.http.with(move |req| {
            (req.url == TOKEN).then(|| {
                // Atlassian rotates: the first use of rt-1 works, any
                // second one is refused for good.
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    fjson(200, json!({"access_token": "at-2", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt-2"}))
                } else {
                    fjson(400, json!({"error": "invalid_grant", "error_description": "refresh token reused"}))
                }
            })
        });
        // A scheduled sync, and a check by hand whose old token was just
        // refused.
        let old = AuthHeader("Authorization".into(), "Bearer at-1".into());
        let (a, b) = tokio::join!(
            r.rt.auth_header(&s, &Owner::Team, false),
            r.rt.auth_header_after(&s, &Owner::Team, Some(&old))
        );
        assert_eq!(a.unwrap().unwrap().1, "Bearer at-2");
        assert_eq!(b.unwrap().unwrap().1, "Bearer at-2");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let t = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.status, Status::Connected);
    }

    #[tokio::test]
    async fn a_refresher_that_finds_another_at_work_waits_instead_of_sending() {
        let (r, s) = expired_rig().await;
        // Someone else holds the record.
        assert!(r
            .store
            .claim_refresh("TEAM#a", "docs", &Owner::Team, 1, 1_000_000, 1_000_000 + 60)
            .await
            .unwrap());
        let (why, _, _) = busy(r.rt.auth_header(&s, &Owner::Team, false).await.unwrap_err());
        assert!(why.contains("renewing"), "{why}");
        assert!(r.http.sent_to(TOKEN).is_empty());
        // Their tokens land: the waiter takes them.
        let sealed =
            r.rt.seal_tokens(
                "docs",
                &Owner::Team,
                &tokens("at-theirs", Some(1_000_000 + 3600)),
                "",
            )
            .await
            .unwrap();
        let mut rec = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        rec.sealed = sealed;
        rec.version = 2;
        assert!(r
            .store
            .put_token("TEAM#a", "docs", &rec, Some(1))
            .await
            .unwrap());
        let h = r.rt.await_other_refresh(&s, &Owner::Team, 1).await.unwrap();
        assert_eq!(h.1, "Bearer at-theirs");
    }

    #[tokio::test]
    async fn a_key_service_hiccup_keeps_the_saved_sign_in() {
        assert!(key_service_passing(
            "kms decrypt: dispatch failure: io error"
        ));
        assert!(key_service_passing(
            "kms decrypt: service error: ThrottlingException: Rate exceeded"
        ));
        assert!(key_service_passing(
            "kms decrypt: service error: KMSInternalException"
        ));
        assert!(!key_service_passing(
            "kms decrypt: InvalidCiphertextException"
        ));
        assert!(!key_service_passing("stored value is not valid"));
        assert!(!key_service_passing("stored tokens are not valid"));
    }

    #[tokio::test]
    async fn tools_that_are_not_allowed_dont_exist() {
        let r = rig("TEAM#a");
        let s = server();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 3600))).await;
        answer_tools(&r.http, "Bearer at-1");
        let e =
            r.rt.call_tool(&s, &Owner::Team, "delete_all", &json!({}), "coder")
                .await
                .unwrap_err();
        assert!(e.contains("isn't available"));
        assert!(
            r.http.sent().is_empty(),
            "nothing is sent for a tool that isn't allowed"
        );
        // Allowed but not yet approved by a second admin: also refused.
        let mut s2 = server();
        s2.approved.clear();
        assert!(r
            .rt
            .call_tool(&s2, &Owner::Team, "search", &json!({}), "coder")
            .await
            .is_err());
        assert!(r.http.sent().is_empty());
    }

    #[tokio::test]
    async fn insufficient_scope_asks_for_more_on_reconnect() {
        let r = rig("TEAM#a");
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 3600))).await;
        r.http.with(|req| {
            (req.url == MCP).then(|| {
                resp(
                    403,
                    &[(
                        "www-authenticate",
                        "Bearer error=\"insufficient_scope\", scope=\"write\"",
                    )],
                    "",
                )
            })
        });
        let e =
            r.rt.call_tool(&s, &Owner::Team, "search", &json!({}), "coder")
                .await
                .unwrap_err();
        assert!(e.contains("write"));
        let rec = r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.status, Status::NeedsReconnect);
        assert_eq!(rec.step_up, vec!["write"]);
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        let q: HashMap<String, String> = reqwest::Url::parse(&url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert_eq!(q["scope"], "read write");
    }

    /// A sign-in in progress never outlives its use: starting again ends
    /// the earlier attempt, cancelling ends it, and expired ones are swept
    /// (the table's own expiry isn't relied on).
    #[tokio::test]
    async fn a_new_sign_in_ends_the_one_it_replaces_and_expired_ones_are_swept() {
        let r = rig("TEAM#a");
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let state_of = |url: String| -> String {
            reqwest::Url::parse(&url)
                .unwrap()
                .query_pairs()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.into_owned())
                .unwrap()
        };
        let cb = "https://app.example.com/cb/";
        let first = state_of(
            r.rt.start_sign_in(&mut s, &Owner::Team, "USER#1", "a@x.com", cb, None)
                .await
                .unwrap(),
        );
        // Another admin's attempt for the same server is theirs to finish.
        r.rt.start_sign_in(&mut s, &Owner::Team, "USER#2", "b@x.com", cb, None)
            .await
            .unwrap();
        assert_eq!(r.store.list_pending("TEAM#a").await.unwrap().len(), 2);
        // The same person starts again: their first attempt is gone.
        r.rt.start_sign_in(&mut s, &Owner::Team, "USER#1", "a@x.com", cb, None)
            .await
            .unwrap();
        let left = r.store.list_pending("TEAM#a").await.unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.iter().all(|p| p.key != oauth::state_key(&first)));
        assert!(r
            .rt
            .finish_sign_in(&first, Some("code"), None, None, "USER#1", "a@x.com")
            .await
            .unwrap_err()
            .contains("already used or has expired"));
        // Cancelling ends only that person's attempt.
        assert_eq!(
            r.rt.end_pending("docs", Some(&Owner::Team), Some("USER#1"))
                .await,
            1
        );
        assert_eq!(r.store.list_pending("TEAM#a").await.unwrap().len(), 1);
        // Nothing expired yet, so a sweep leaves the other one alone.
        assert_eq!(r.rt.sweep_pending().await, 0);
        r.now.fetch_add(PENDING_TTL_SECS + 1, Ordering::SeqCst);
        assert_eq!(r.rt.sweep_pending().await, 1);
        assert!(r.store.list_pending("TEAM#a").await.unwrap().is_empty());
        // Removing a server takes its sign-ins in progress with it.
        r.rt.start_sign_in(&mut s, &Owner::Team, "USER#1", "a@x.com", cb, None)
            .await
            .unwrap();
        r.rt.remove(&s).await.unwrap();
        assert!(r.store.list_pending("TEAM#a").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sign_in_end_to_end_binds_state_iss_and_person() {
        let r = rig("TEAM#a");
        let mut s = server();
        if let Some(o) = s.oauth.as_mut() {
            o.client_id.clear();
        }
        r.store.put_server("TEAM#a", &s).await.unwrap();
        r.http.on(
            Method::Post,
            "https://auth.example.com/register",
            fjson(
                201,
                json!({"client_id": "dyn-1", "token_endpoint_auth_method": "none"}),
            ),
        );
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        let q: HashMap<String, String> = reqwest::Url::parse(&url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert_eq!(q["client_id"], "dyn-1");
        assert_eq!(q["resource"], MCP);
        let state = q["state"].clone();
        // The registered client was saved.
        assert_eq!(
            r.store
                .get_server("TEAM#a", "docs")
                .await
                .unwrap()
                .unwrap()
                .oauth
                .unwrap()
                .client_id,
            "dyn-1"
        );

        // Someone else can't finish it, and trying uses it up.
        assert!(r
            .rt
            .finish_sign_in(
                &state,
                Some("code"),
                None,
                Some("https://auth.example.com"),
                "USER#2",
                "b@x.com"
            )
            .await
            .is_err());
        assert!(r
            .rt
            .finish_sign_in(
                &state,
                Some("code"),
                None,
                Some("https://auth.example.com"),
                "USER#1",
                "a@x.com"
            )
            .await
            .unwrap_err()
            .contains("already used"));

        r.http.on(Method::Post, TOKEN, fjson(200, json!({"access_token": "at-1", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt-1"})));
        // Wrong issuer: refused before any token request.
        let url2 =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        let q2: HashMap<String, String> = reqwest::Url::parse(&url2)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert!(r
            .rt
            .finish_sign_in(
                &q2["state"],
                Some("c"),
                None,
                Some("https://evil.example.net"),
                "USER#1",
                "a@x.com"
            )
            .await
            .is_err());
        assert!(r.http.sent_to(TOKEN).is_empty());
        // A fresh one, finished by its starter with the right issuer.
        // (Started after the refused one: a person's new sign-in ends
        // their earlier attempt for the same connection.)
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        let q: HashMap<String, String> = reqwest::Url::parse(&url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        let (back, owner) =
            r.rt.finish_sign_in(
                &q["state"],
                Some("code-1"),
                None,
                Some("https://auth.example.com"),
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap();
        assert_eq!(owner, Owner::Team);
        assert_eq!(back.id, "docs");
        let sent = r.http.sent_to(TOKEN);
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body_text().contains("code_verifier="));
        assert!(sent[0].body_text().contains("code=code-1"));
        assert_eq!(
            r.store
                .get_server("TEAM#a", "docs")
                .await
                .unwrap()
                .unwrap()
                .status,
            Status::Connected
        );
        // A sign-in that took too long is refused.
        let url3 =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        let q3: HashMap<String, String> = reqwest::Url::parse(&url3)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        r.now.fetch_add(PENDING_TTL_SECS + 1, Ordering::SeqCst);
        assert!(r
            .rt
            .finish_sign_in(
                &q3["state"],
                Some("c"),
                None,
                Some("https://auth.example.com"),
                "USER#1",
                "a@x.com"
            )
            .await
            .unwrap_err()
            .contains("too long"));
    }

    #[tokio::test]
    async fn the_metadata_document_is_preferred_when_supported() {
        let r = rig("TEAM#a");
        let mut s = server();
        if let Some(o) = s.oauth.as_mut() {
            o.client_id.clear();
            o.cimd_supported = true;
        }
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let doc = "https://app.coderhelm.com/oauth/mcp-client.json";
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.coderhelm.com/cb/",
                Some(doc),
            )
            .await
            .unwrap();
        assert!(url.contains(&format!("client_id={}", urlencoding_like(doc))));
        assert!(
            r.http
                .sent_to("https://auth.example.com/register")
                .is_empty(),
            "no registration request"
        );
    }

    const PRM: &str = "https://mcp.example.com/.well-known/oauth-protected-resource/mcp";
    const NEW_AS: &str = "https://login.example.net/t1";
    const NEW_AUTHORIZE: &str = "https://login.example.net/authorize";

    /// A server that moved its sign-in: its `401` names its protected
    /// resource metadata, which names a new authorization server.
    fn moved_server(f: &Fake, authorize_status: u16) {
        f.with(|r| {
            (r.url == MCP && r.method == Method::Post).then(|| {
                resp(
                    401,
                    &[(
                        "www-authenticate",
                        &format!("Bearer resource_metadata=\"{PRM}\""),
                    )],
                    "",
                )
            })
        });
        f.on(
            Method::Get,
            PRM,
            fjson(
                200,
                json!({"resource": MCP, "authorization_servers": [NEW_AS], "scopes_supported": ["read:docs", "offline_access"]}),
            ),
        );
        f.on(
            Method::Get,
            "https://login.example.net/.well-known/oauth-authorization-server/t1",
            fjson(
                200,
                json!({
                    "issuer": NEW_AS,
                    "authorization_endpoint": NEW_AUTHORIZE,
                    "token_endpoint": "https://login.example.net/oauth/token",
                    "registration_endpoint": "https://login.example.net/t1/register",
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                }),
            ),
        );
        f.on(
            Method::Post,
            "https://login.example.net/t1/register",
            fjson(
                201,
                json!({"client_id": "fresh", "token_endpoint_auth_method": "none"}),
            ),
        );
        f.with(move |r| {
            r.url
                .starts_with(NEW_AUTHORIZE)
                .then(|| resp(authorize_status, &[], ""))
        });
    }

    /// The sign-in starts from what the server says now (its `401`, its
    /// protected resource metadata, its authorization server's metadata),
    /// not from addresses saved when it was added: a vendor that moves its
    /// sign-in server is followed, with a new client there.
    #[tokio::test]
    async fn a_sign_in_starts_from_the_servers_current_metadata() {
        let r = rig("TEAM#a");
        moved_server(&r.http, 302);
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        assert!(url.starts_with(NEW_AUTHORIZE), "{url}");
        let q: HashMap<String, String> = reqwest::Url::parse(&url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
        assert_eq!(q["client_id"], "fresh");
        assert_eq!(q["resource"], MCP);
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["scope"], "read:docs offline_access");
        let saved = r.store.get_server("TEAM#a", "docs").await.unwrap().unwrap();
        let o = saved.oauth.unwrap();
        assert_eq!(o.issuer, NEW_AS);
        assert_eq!(o.client_id, "fresh");
        assert_eq!(o.discovered_at, 1_000_000);
        // Within a few minutes the saved settings are used as they are.
        let before = r.http.sent_to(PRM).len();
        let mut s = r.store.get_server("TEAM#a", "docs").await.unwrap().unwrap();
        r.rt.start_sign_in(
            &mut s,
            &Owner::Team,
            "USER#1",
            "a@x.com",
            "https://app.example.com/cb/",
            None,
        )
        .await
        .unwrap();
        assert_eq!(r.http.sent_to(PRM).len(), before);
        // After that they are read again.
        r.now.fetch_add(DISCOVERY_TTL_SECS + 1, Ordering::SeqCst);
        r.rt.start_sign_in(
            &mut s,
            &Owner::Team,
            "USER#1",
            "a@x.com",
            "https://app.example.com/cb/",
            None,
        )
        .await
        .unwrap();
        assert_eq!(r.http.sent_to(PRM).len(), before + 1);
    }

    /// A sign-in page that answers 5xx is never where a person is sent: the
    /// start says the provider's sign-in is failing on its side, with the
    /// status, and leaves nothing waiting.
    #[tokio::test]
    async fn a_sign_in_page_that_is_down_is_said_plainly() {
        let r = rig("TEAM#a");
        moved_server(&r.http, 500);
        let mut s = server();
        s.name = "Atlassian".into();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let e =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(e, provider_down("Atlassian", 500));
        assert!(e.contains("failing on Atlassian's side (HTTP 500)"), "{e}");
        assert!(r.store.list_pending("TEAM#a").await.unwrap().is_empty());
        // It was asked once with fresh settings, and once more after reading
        // them again: never more.
        assert!(
            r.http
                .sent()
                .iter()
                .filter(|q| q.url.starts_with(NEW_AUTHORIZE))
                .count()
                <= 2
        );
        // A provider's own API with fixed addresses (a knowledge source) is
        // neither read again nor probed.
        let mut k = server();
        k.id = "ks-notion".into();
        k.catalog_id = "source:notion".into();
        k.oauth.as_mut().unwrap().registration = "manual".into();
        assert!(!discovers(&k));
        assert!(discovers(&server()));
    }

    /// A catalog server whose vendor moved it (Atlassian's v1 address to
    /// v2) is moved on its next sign-in, with nothing to fix by hand.
    #[test]
    fn a_catalog_server_follows_the_catalogs_address() {
        assert_eq!(
            crate::mcp_catalog::moved_remote_url(
                "atlassian",
                "atlassian",
                "https://mcp.atlassian.com/v1/mcp"
            ),
            Some("https://mcp.atlassian.com/v2/mcp")
        );
        assert_eq!(
            crate::mcp_catalog::moved_remote_url(
                "atlassian",
                "atlassian",
                "https://mcp.atlassian.com/v2/mcp/"
            ),
            None
        );
        // A server added by address, or another record, keeps its own.
        assert_eq!(
            crate::mcp_catalog::moved_remote_url("wiki", "", "https://mcp.example.com/mcp"),
            None
        );
        assert_eq!(
            crate::mcp_catalog::moved_remote_url(
                "ks-x",
                "atlassian",
                "https://mcp.atlassian.com/v1/mcp"
            ),
            None
        );
    }

    fn urlencoding_like(s: &str) -> String {
        url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }

    #[tokio::test]
    async fn disconnect_revokes_then_forgets() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        connect(&r, &s, &Owner::Team, tokens("at-1", Some(1_000_000 + 3600))).await;
        r.http.on(Method::Post, REVOKE, resp(200, &[], ""));
        assert!(r.rt.disconnect(&s, &Owner::Team).await.unwrap());
        let sent = r.http.sent_to(REVOKE);
        assert_eq!(sent.len(), 2);
        assert!(sent[0]
            .body_text()
            .contains("token_type_hint=refresh_token"));
        assert!(sent[1].body_text().contains("token_type_hint=access_token"));
        assert!(r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            r.store
                .get_server("TEAM#a", "docs")
                .await
                .unwrap()
                .unwrap()
                .status,
            Status::NotConnected
        );
        // A server with no revocation still forgets the tokens.
        let mut s2 = server();
        s2.oauth.as_mut().unwrap().revocation_endpoint = None;
        connect(&r, &s2, &Owner::Team, tokens("at-1", None)).await;
        assert!(!r.rt.disconnect(&s2, &Owner::Team).await.unwrap());
        assert!(r
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn teams_and_people_are_kept_apart() {
        let a = rig("TEAM#a");
        let s = server();
        connect(&a, &s, &Owner::Team, tokens("at-a", None)).await;
        let rec = a
            .store
            .get_token("TEAM#a", "docs", &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        // The same sealed tokens copied under another team don't open.
        let b = rig("TEAM#b");
        assert!(b
            .rt
            .open_tokens("docs", &Owner::Team, &rec.sealed)
            .await
            .is_err());
        // Nor under another server, nor as one person's.
        assert!(a
            .rt
            .open_tokens("docs-2", &Owner::Team, &rec.sealed)
            .await
            .is_err());
        assert!(a
            .rt
            .open_tokens("docs", &Owner::user("a@x.com"), &rec.sealed)
            .await
            .is_err());
        // Another team's store has nothing for this server.
        assert!(b.rt.auth_header(&s, &Owner::Team, false).await.is_err());
        // A personal server uses each person's own connection only.
        let mut p = server();
        p.sharing = Sharing::Personal;
        connect(&a, &p, &Owner::user("dana@x.com"), tokens("at-dana", None)).await;
        let dana =
            a.rt.auth_header(&p, &p.owner_for(Some("Dana@x.com")).unwrap(), false)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(dana.1, "Bearer at-dana");
        assert_eq!(
            a.rt.auth_header(&p, &Owner::user("lee@x.com"), false)
                .await
                .unwrap_err(),
            ConnError::NotConnected
        );
        // A key sealed for one team doesn't open for another.
        let mut h = ServerRecord::new("keyed", "Keyed", MCP);
        h.auth = AuthKind::Header;
        h.header_name = "X-API-Key".into();
        h.header_sealed = a.rt.seal_header("keyed", "k-secret").await.unwrap();
        assert_eq!(
            a.rt.auth_header(&h, &Owner::Team, false)
                .await
                .unwrap()
                .unwrap()
                .1,
            "k-secret"
        );
        assert!(b.rt.auth_header(&h, &Owner::Team, false).await.is_err());
    }

    #[tokio::test]
    async fn only_connected_servers_with_allowed_tools_are_usable() {
        let r = rig("TEAM#a");
        let shared = server();
        r.store.put_server("TEAM#a", &shared).await.unwrap();
        let mut personal = server();
        personal.id = "wiki".into();
        personal.sharing = Sharing::Personal;
        r.store.put_server("TEAM#a", &personal).await.unwrap();
        let mut nothing_allowed = server();
        nothing_allowed.id = "empty".into();
        nothing_allowed.allowed.clear();
        r.store
            .put_server("TEAM#a", &nothing_allowed)
            .await
            .unwrap();
        connect(&r, &nothing_allowed, &Owner::Team, tokens("x", None)).await;
        // Nobody connected yet.
        assert!(r.rt.usable(Some("dana@x.com")).await.is_empty());
        connect(&r, &shared, &Owner::Team, tokens("at", None)).await;
        connect(
            &r,
            &personal,
            &Owner::user("dana@x.com"),
            tokens("at-d", None),
        )
        .await;
        let ids = |v: Vec<(ServerRecord, Owner)>| {
            v.into_iter()
                .map(|(s, o)| (s.id, o.key()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(r.rt.usable(Some("Dana@x.com")).await),
            vec![
                ("docs".to_string(), "team".to_string()),
                ("wiki".to_string(), "user#dana@x.com".to_string())
            ]
        );
        // With no person known (a scheduled run), personal servers are left out.
        assert_eq!(
            ids(r.rt.usable(None).await),
            vec![("docs".to_string(), "team".to_string())]
        );
        // Someone else hasn't connected the personal one.
        assert_eq!(ids(r.rt.usable(Some("lee@x.com")).await).len(), 1);
        // A connection that needs reconnecting is left out.
        r.store
            .set_token_status(
                "TEAM#a",
                "docs",
                &Owner::Team,
                Status::NeedsReconnect,
                "x",
                &[],
            )
            .await
            .unwrap();
        assert!(ids(r.rt.usable(None).await).is_empty());
    }

    #[tokio::test]
    async fn detection_tells_oauth_key_and_open_apart() {
        let f = Fake::default();
        f.with(|r| (r.url == MCP).then(|| resp(401, &[("www-authenticate", "Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource/mcp\"")], "")));
        f.on(
            Method::Get,
            "https://mcp.example.com/.well-known/oauth-protected-resource/mcp",
            fjson(
                200,
                json!({"resource": MCP, "authorization_servers": ["https://auth.example.com"]}),
            ),
        );
        f.on(Method::Get, "https://auth.example.com/.well-known/oauth-authorization-server", fjson(200, json!({
            "issuer": "https://auth.example.com", "authorization_endpoint": "https://auth.example.com/a", "token_endpoint": TOKEN,
            "code_challenge_methods_supported": ["S256"], "registration_endpoint": "https://auth.example.com/register"
        })));
        assert!(matches!(detect(&f, MCP).await.unwrap(), Detected::OAuth(p) if p.resource == MCP));

        let k = Fake::default();
        k.with(|r| (r.url == MCP).then(|| resp(401, &[], "missing api key")));
        assert_eq!(detect(&k, MCP).await.unwrap(), Detected::Header);

        let o = Fake::default();
        o.with(|r| {
            (r.url == MCP).then(|| {
                fjson(
                    200,
                    json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "ping"}]}}),
                )
            })
        });
        assert!(
            matches!(detect(&o, MCP).await.unwrap(), Detected::Open { tools, .. } if tools.len() == 1)
        );

        // A private address is refused before anything is sent.
        let p = Fake::default();
        assert!(detect(&p, "https://10.0.0.5/mcp").await.is_err());
        assert!(detect(&p, "http://mcp.example.com/mcp").await.is_err());
        assert!(p.sent().is_empty());
    }

    #[tokio::test]
    async fn coderhelm_reads_a_tool_agents_arent_allowed_and_renews_first() {
        let r = rig("TEAM#a");
        let mut s = server();
        s.allowed.clear();
        s.approved.clear();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let me = Owner::user("ada@example.com");
        connect(&r, &s, &me, tokens("at-1", Some(1_000_000 + 30))).await;
        r.http.on(Method::Post, TOKEN, fjson(200, json!({"access_token": "at-2", "token_type": "Bearer", "expires_in": 3600, "refresh_token": "rt-2"})));
        answer_tools(&r.http, "Bearer at-2");
        let tool = s.tools[0].clone();
        let out = r.rt.read_tool(&s, &me, &tool, &json!({})).await.unwrap();
        assert_eq!(out["content"][0]["text"], "found");
        // The token went to the server and the token endpoint only.
        for sent in r.http.sent() {
            if sent
                .header_value("Authorization")
                .is_some_and(|h| h.starts_with("Bearer"))
            {
                assert_eq!(sent.url, MCP);
            }
        }
        // Not written to the agents' audit trail: nothing an agent did.
        assert!(r
            .store
            .list_logs("TEAM#a", "docs", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_refused_renewal_means_signing_in_again() {
        let r = rig("TEAM#a");
        let s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let me = Owner::user("ada@example.com");
        connect(&r, &s, &me, tokens("at-1", Some(1_000_000 - 10))).await;
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                400,
                json!({"error": "invalid_grant", "error_description": "Refresh token expired"}),
            ),
        );
        let tool = s.tools[0].clone();
        let e =
            r.rt.read_tool(&s, &me, &tool, &json!({}))
                .await
                .unwrap_err();
        assert!(matches!(e, ConnError::NeedsReconnect(_)), "{e:?}");
        let rec = r
            .store
            .get_token("TEAM#a", "docs", &me)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.status, Status::NeedsReconnect);
        let _ = &r.now;
    }

    fn state_of(url: &str) -> String {
        reqwest::Url::parse(url)
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.to_string())
            .unwrap()
    }

    const ISS: Option<&str> = Some("https://auth.example.com");

    #[tokio::test]
    async fn a_state_carries_its_flow_and_its_record_the_return_path() {
        let r = rig("TEAM#a");
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let url =
            r.rt.start_sign_in_for(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
                Some(oauth::Flow::Knowledge),
                "/knowledge/base/?id=kb1",
            )
            .await
            .unwrap();
        let state = state_of(&url);
        assert!(state.starts_with("ks."), "{state}");
        assert_eq!(oauth::Flow::of_state(&state), Some(oauth::Flow::Knowledge));
        // A provider refusal with its own words: the flow, the page to go
        // back to and the provider's description all come back.
        let f =
            r.rt.finish(
                &Answer {
                    state: &state,
                    code: None,
                    error: Some("access_denied"),
                    error_description: Some("The app is not installed on this site"),
                    iss: ISS,
                },
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap_err();
        assert_eq!(f.flow, Some(oauth::Flow::Knowledge));
        assert_eq!(f.return_to, "/knowledge/base/?id=kb1");
        assert_eq!(f.code, "access_denied");
        assert_eq!(f.server_name, "Docs");
        assert!(
            f.message.contains("The app is not installed on this site"),
            "{}",
            f.message
        );
        // The state was used up by the refusal.
        let again =
            r.rt.finish(
                &Answer {
                    state: &state,
                    code: Some("c"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap_err();
        assert!(again.message.contains("already used"));
        assert_eq!(again.flow, Some(oauth::Flow::Knowledge), "from the tag");
    }

    #[tokio::test]
    async fn a_return_path_off_the_dashboard_is_dropped() {
        let r = rig("TEAM#a");
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let url =
            r.rt.start_sign_in_for(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
                None,
                "https://evil.example.net/",
            )
            .await
            .unwrap();
        let state = state_of(&url);
        assert!(state.starts_with("mcp."), "worked out from the record");
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": "at-1", "token_type": "Bearer", "expires_in": 3600}),
            ),
        );
        let done =
            r.rt.finish(
                &Answer {
                    state: &state,
                    code: Some("c"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap();
        assert_eq!(done.flow, oauth::Flow::Mcp);
        assert_eq!(done.return_to, "");
    }

    #[tokio::test]
    async fn a_state_from_before_flows_still_finishes() {
        let r = rig("TEAM#a");
        let mut s = server();
        s.catalog_id = "source:notion".into();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        // A pending sign-in as an older gateway wrote it: a bare random
        // state, no flow, no return path.
        let state = oauth::new_state();
        let pkce = oauth::Pkce::new();
        r.store
            .put_pending(
                "TEAM#a",
                &PendingAuth {
                    key: oauth::state_key(&state),
                    server_id: "docs".into(),
                    owner: Owner::Team,
                    user_sub: "USER#1".into(),
                    user_email: "a@x.com".into(),
                    verifier: pkce.verifier,
                    issuer: "https://auth.example.com".into(),
                    redirect_uri: "https://app.example.com/cb/".into(),
                    expires_at: 2_000_000_000,
                    flow: String::new(),
                    return_to: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(oauth::Flow::of_state(&state), None);
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(200, json!({"access_token": "at-1", "token_type": "Bearer"})),
        );
        let done =
            r.rt.finish(
                &Answer {
                    state: &state,
                    code: Some("c"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap();
        assert_eq!(done.flow, oauth::Flow::Knowledge, "from the record");
    }

    #[tokio::test]
    async fn a_failed_code_exchange_says_the_token_endpoints_reason() {
        let r = rig("TEAM#a");
        let mut s = server();
        r.store.put_server("TEAM#a", &s).await.unwrap();
        let url =
            r.rt.start_sign_in(
                &mut s,
                &Owner::Team,
                "USER#1",
                "a@x.com",
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        r.http.on(
            Method::Post,
            TOKEN,
            fjson(400, json!({"error": "invalid_grant", "error_description": "authorization code expired"})),
        );
        let f =
            r.rt.finish(
                &Answer {
                    state: &state_of(&url),
                    code: Some("c"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "a@x.com",
            )
            .await
            .unwrap_err();
        assert_eq!(f.code, "invalid_grant");
        assert!(f.message.contains("authorization code expired"));
        assert!(!f.message.contains("not granted"));
    }

    #[test]
    fn provider_errors_are_told_as_the_provider_told_them() {
        let bare = provider_error("Confluence", "access_denied", "");
        assert!(
            bare.contains("access_denied")
                && bare.contains("Cancel")
                && bare.contains("doesn't let this account")
        );
        let said = provider_error("Confluence", "access_denied", "User is not authorized");
        assert!(said.contains("User is not authorized") && !said.contains("Cancel"));
        assert!(provider_error("Notion", "invalid_scope", "").contains("scope"));
        assert!(provider_error("", "server_error", "token exchange failed")
            .starts_with("The sign-in page"));
        assert!(
            provider_error("X", "weird", "a\u{7}b").contains("ab"),
            "control characters dropped"
        );
        assert_eq!(
            display_name(&ServerRecord::new(
                "ks-confluence",
                "Confluence (knowledge)",
                MCP
            )),
            "Confluence"
        );
    }

    /// An Atlassian-sized access token: a JWT of about 5 KB.
    fn big_jwt() -> String {
        format!(
            "eyJraWQiOiJhdXRoLmF0bGFzc2lhbi5jb20iLCJhbGciOiJSUzI1NiJ9.{}.{}",
            "eyJodHRwczovL2F0bGFzc2lhbi5jb20vc3lzdGVtQWNjb3VudElkIjoi".repeat(90),
            "c2lnbmF0dXJl".repeat(30)
        )
    }

    fn enveloped_rt(store: &Arc<MemStore>, http: &Arc<Fake>, team: &str) -> Runtime {
        Runtime {
            store: store.clone(),
            cipher: Arc::new(crate::envelope::Enveloped(
                crate::envelope::testing::FakeKeys::default(),
            )),
            http: http.clone(),
            team: team.into(),
            clock: Arc::new(|| 1_000_000),
        }
    }

    async fn start(rt: &Runtime, s: &mut ServerRecord, who: &str) -> String {
        let url = rt
            .start_sign_in(
                s,
                &Owner::Team,
                who,
                who,
                "https://app.example.com/cb/",
                None,
            )
            .await
            .unwrap();
        state_of(&url)
    }

    /// The hybrid broker with a large sign-in, end to end: the provider's
    /// tokens (over KMS's direct 4 KB) are sealed in the holding partition,
    /// adopted into the install's hosted partition, handed out as the access
    /// token, and sealed again on the install, which signs in with the
    /// broker's token and a grant handle.
    #[tokio::test]
    async fn the_broker_carries_an_atlassian_sized_sign_in_end_to_end() {
        let jwt = big_jwt();
        let refresh = format!("eyJraWQiOiJyZWZyZXNoIn0.{}", "cmVmcmVzaA".repeat(120));
        assert!(jwt.len() + refresh.len() > 5000, "{}", jwt.len());
        let store = Arc::new(MemStore::default());
        let http = Arc::new(Fake::default());
        http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": jwt, "token_type": "Bearer", "expires_in": 3600, "refresh_token": refresh}),
            ),
        );
        let hosted = enveloped_rt(&store, &http, "HYBRID#ins_a");
        let hold = enveloped_rt(&store, &http, "HYBRID#ins_a#SIGNIN#fl_1");
        let mut s = server();
        store.put_server(&hosted.team, &s).await.unwrap();
        store.put_server(&hold.team, &s).await.unwrap();

        // The provider's answer is finished in the holding partition.
        let sub = "hybrid:ins_a";
        let state = start(&hold, &mut s, sub).await;
        let done = hold
            .finish(
                &Answer {
                    state: &state,
                    code: Some("code-1"),
                    iss: ISS,
                    ..Default::default()
                },
                sub,
                sub,
            )
            .await;
        assert!(done.is_ok(), "{:?}", done.err());
        let held = store
            .get_token(&hold.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        let blob = {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.decode(&held.sealed).unwrap()
        };
        assert!(blob.starts_with(crate::envelope::MAGIC), "enveloped");

        // The install redeems the code: the sign-in moves to its partition.
        hosted
            .adopt_sign_in(&hold, &s.id, &Owner::Team)
            .await
            .unwrap();
        let t = store
            .get_token(&hosted.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .unwrap();
        let set = hosted
            .open_tokens(&s.id, &Owner::Team, &t.sealed)
            .await
            .unwrap();
        assert_eq!(set.access_token, jwt);
        assert_eq!(set.refresh_token.as_deref(), Some(refresh.as_str()));
        // Another team can't open the adopted tokens.
        let other = enveloped_rt(&store, &http, "HYBRID#ins_b");
        assert!(other
            .open_tokens(&s.id, &Owner::Team, &t.sealed)
            .await
            .is_err());

        // What the broker's token endpoint hands out: the big access token.
        let header = hosted
            .auth_header(&s, &Owner::Team, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header.1, format!("Bearer {jwt}"));

        // The install signs in at the broker and stores what it got.
        let install_http = Arc::new(Fake::default());
        install_http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": jwt, "token_type": "Bearer", "expires_in": 600, "refresh_token": "hr_handle"}),
            ),
        );
        let install = enveloped_rt(
            &Arc::new(MemStore::default()),
            &install_http,
            "TEAM#install",
        );
        let mut si = server();
        install.store.put_server(&install.team, &si).await.unwrap();
        let state = start(&install, &mut si, "USER#1").await;
        install
            .finish(
                &Answer {
                    state: &state,
                    code: Some("hc_1"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "USER#1",
            )
            .await
            .unwrap();
        let header = install
            .auth_header(&si, &Owner::Team, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header.1, format!("Bearer {jwt}"));
    }

    /// Tokens over the bound: the sign-in fails with words that name the
    /// provider and what was too large, and nothing is saved.
    #[tokio::test]
    async fn tokens_over_the_bound_fail_naming_the_provider() {
        let store = Arc::new(MemStore::default());
        let http = Arc::new(Fake::default());
        http.on(
            Method::Post,
            TOKEN,
            fjson(
                200,
                json!({"access_token": "x".repeat(crate::envelope::MAX_SEALED_BYTES), "token_type": "Bearer", "refresh_token": "r"}),
            ),
        );
        let rt = enveloped_rt(&store, &http, "TEAM#a");
        let mut s = server();
        s.name = "Confluence (knowledge)".into();
        store.put_server(&rt.team, &s).await.unwrap();
        let state = start(&rt, &mut s, "USER#1").await;
        let f = rt
            .finish(
                &Answer {
                    state: &state,
                    code: Some("c"),
                    iss: ISS,
                    ..Default::default()
                },
                "USER#1",
                "USER#1",
            )
            .await
            .unwrap_err();
        assert!(f.message.contains("Confluence sent"), "{}", f.message);
        assert!(f.message.contains("sign-in tokens"), "{}", f.message);
        assert!(f.message.contains("64 KB"), "{}", f.message);
        assert!(!f.message.contains("xxxx"), "no token value");
        assert_eq!(f.server_name, "Confluence");
        assert!(store
            .get_token(&rt.team, &s.id, &Owner::Team)
            .await
            .unwrap()
            .is_none());
    }
}
