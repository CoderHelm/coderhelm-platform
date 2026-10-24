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
    AuthKind, LogEntry, Owner, PendingAuth, ServerRecord, Status, Store, TokenRecord,
    PENDING_TTL_SECS,
};
use super::tools;
use crate::mcp_credentials::Cipher;

const TOKEN_PURPOSE: &str = "mcp-oauth-token";
const CLIENT_PURPOSE: &str = "mcp-oauth-client";
const HEADER_PURPOSE: &str = "mcp-header";

/// KMS with the secrets key, owned (the worker and gateway keep one per
/// request). `key_id` is needed only to seal.
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
}

impl std::fmt::Display for ConnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnError::NotConnected => f.write_str("Not connected yet."),
            ConnError::NeedsReconnect(r) | ConnError::Failed(r) | ConnError::Temporary(r) => {
                f.write_str(r)
            }
        }
    }
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

pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(|| chrono::Utc::now().timestamp())
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

    async fn seal(&self, c: HashMap<String, String>, text: &[u8]) -> Result<String, String> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        if text.len() > crate::mcp_credentials::MAX_PLAINTEXT_BYTES {
            return Err("That value is too long to store.".into());
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
        )
        .await
    }

    async fn seal_tokens(
        &self,
        server_id: &str,
        owner: &Owner,
        t: &TokenSet,
    ) -> Result<String, String> {
        let json = serde_json::to_vec(t).map_err(|_| "tokens are not valid".to_string())?;
        self.seal(
            ctx(&self.team, server_id, TOKEN_PURPOSE, Some(owner)),
            &json,
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
            AuthKind::OAuth => self.bearer(s, owner, force).await.map(Some),
        }
    }

    async fn bearer(
        &self,
        s: &ServerRecord,
        owner: &Owner,
        force: bool,
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
            Err(_) => {
                let why = "The saved sign-in can't be opened. Reconnect.";
                self.mark(s, owner, Status::NeedsReconnect, why, &[]).await;
                return Err(ConnError::NeedsReconnect(why.into()));
            }
        };
        let now = self.now();
        if !force && set.usable_at(now) {
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
        let req = oauth::refresh_request(&o.token_endpoint, &client, &refresh_token, &o.resource);
        let resp = self
            .http
            .send(req)
            .await
            .map_err(|e| ConnError::Temporary(format!("Couldn't renew the sign-in: {e}")))?;
        match oauth::parse_token_response(&resp, now, Some(&set)) {
            Ok(new) => {
                let sealed = self
                    .seal_tokens(&s.id, owner, &new)
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
                let why = format!("{} Reconnect to sign in again.", e.message);
                self.mark(s, owner, Status::NeedsReconnect, &why, &[]).await;
                Err(ConnError::NeedsReconnect(why))
            }
            Err(e) => Err(ConnError::Temporary(e.message)),
        }
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
                    auth = self.auth_header(s, owner, true).await?;
                }
                Err(e @ (CallError::Unauthorized(_) | CallError::Forbidden(_))) => {
                    return Err(self.refused(s, owner, &e).await)
                }
                Err(e) => return Err(ConnError::Temporary(e.to_string())),
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
                        .auth_header(s, owner, true)
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
        let pkce = oauth::Pkce::new();
        let state = oauth::new_state();
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
                },
            )
            .await?;
        oauth::authorize_url(
            &o.auth_server(),
            &o.client_id,
            redirect_uri,
            &pkce,
            &state,
            &scopes,
            &o.resource,
        )
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
        let p = self
            .store
            .take_pending(&self.team, &oauth::state_key(state))
            .await?
            .ok_or("That sign-in was already used or has expired. Start it again.")?;
        p.check(user_sub, user_email, self.now())?;
        let s = self
            .store
            .get_server(&self.team, &p.server_id)
            .await?
            .ok_or("That server was removed while you were signing in.")?;
        let o = s
            .oauth
            .clone()
            .ok_or("This server doesn't sign in with OAuth.")?;
        if o.issuer != p.issuer {
            return Err(
                "The server's sign-in settings changed while you were signing in. Start again."
                    .into(),
            );
        }
        oauth::check_iss(o.iss_supported, &p.issuer, iss)?;
        if let Some(err) = error {
            return Err(match err {
                "access_denied" => "Access was not granted on the sign-in page.".to_string(),
                other => format!(
                    "The sign-in page returned an error ({}).",
                    crate::truncate_str(other, 80)
                ),
            });
        }
        let code = code
            .filter(|c| !c.is_empty())
            .ok_or("The sign-in page sent no code.")?;
        let client = self.client(&s).await?;
        let req = oauth::token_request(
            &o.auth_server(),
            &client,
            code,
            &p.verifier,
            &p.redirect_uri,
            &o.resource,
        );
        let resp = self.http.send(req).await.map_err(|e| e.to_string())?;
        let now = self.now();
        let set = oauth::parse_token_response(&resp, now, None).map_err(|e| e.message)?;
        let sealed = self.seal_tokens(&s.id, &p.owner, &set).await?;
        let version = self
            .store
            .get_token(&self.team, &s.id, &p.owner)
            .await?
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
            .await?;
        if p.owner == Owner::Team {
            self.store
                .set_status(&self.team, &s.id, Status::Connected, "")
                .await?;
        }
        Ok((s, p.owner))
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

    /// Removes a server: every connection is disconnected (and revoked)
    /// first, then the server goes.
    pub async fn remove(&self, s: &ServerRecord) -> Result<(), String> {
        for t in self.store.list_tokens(&self.team, &s.id).await? {
            self.disconnect(s, &t.owner).await?;
        }
        self.store.delete_server(&self.team, &s.id).await
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

    /// Stands in for KMS: opening under any other context fails.
    struct FakeKms;
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
        let sealed = r.rt.seal_tokens(&s.id, owner, &tokens).await.unwrap();
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

        // A fresh one, finished by its starter with the right issuer.
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
        assert!(r.http.sent().is_empty(), "no registration request");
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
}
