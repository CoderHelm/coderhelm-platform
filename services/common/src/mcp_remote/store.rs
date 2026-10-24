//! Where a team's remote MCP servers, their connections and their audit
//! trail are kept: the MCP configs table, under the team's own key.
//!
//! - `MCPSERVER#<id>`: the server, how it signs in, its tools, the allow
//!   list and what is approved, and the shared connection's status.
//! - `MCPTOKEN#<id>#team` or `MCPTOKEN#<id>#user#<email>`: one connection's
//!   tokens, sealed by KMS for that team, server and owner.
//! - `MCPOAUTH#<state hash>`: a sign-in in progress (single use, ten
//!   minutes).
//! - `MCPLOG#<id>#<ulid>`: one tool call, kept 90 days.
//!
//! Every key starts with the team, so nothing here is reachable from another
//! team; sealed values also name the team in their encryption context.

use std::collections::{BTreeSet, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use super::client::{RemoteTool, SkippedTool};
use super::oauth::ClientAuth;

pub const SERVER_PREFIX: &str = "MCPSERVER#";
pub const TOKEN_PREFIX: &str = "MCPTOKEN#";
pub const PENDING_PREFIX: &str = "MCPOAUTH#";
pub const LOG_PREFIX: &str = "MCPLOG#";
/// How long a sign-in may take.
pub const PENDING_TTL_SECS: i64 = 600;
/// How long a tool call is kept in the audit trail.
pub const LOG_TTL_SECS: i64 = 90 * 86_400;

pub type Item = HashMap<String, AttributeValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    None,
    Header,
    OAuth,
}

impl AuthKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthKind::None => "none",
            AuthKind::Header => "header",
            AuthKind::OAuth => "oauth",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "header" => AuthKind::Header,
            "oauth" => AuthKind::OAuth,
            _ => AuthKind::None,
        }
    }
}

/// One connection for the whole team, or one per person.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sharing {
    Shared,
    Personal,
}

impl Sharing {
    pub fn as_str(self) -> &'static str {
        match self {
            Sharing::Shared => "shared",
            Sharing::Personal => "personal",
        }
    }
    pub fn parse(s: &str) -> Self {
        if s == "personal" {
            Sharing::Personal
        } else {
            Sharing::Shared
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    NotConnected,
    Connected,
    NeedsReconnect,
    Failed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::NotConnected => "not_connected",
            Status::Connected => "connected",
            Status::NeedsReconnect => "needs_reconnect",
            Status::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "connected" => Status::Connected,
            "needs_reconnect" => Status::NeedsReconnect,
            "failed" => Status::Failed,
            _ => Status::NotConnected,
        }
    }
}

/// Whose connection a token is.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Owner {
    Team,
    /// A person, by lowercased email.
    User(String),
}

impl Owner {
    pub fn user(email: &str) -> Self {
        Owner::User(email.trim().to_ascii_lowercase())
    }
    pub fn key(&self) -> String {
        match self {
            Owner::Team => "team".into(),
            Owner::User(e) => format!("user#{e}"),
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        if s == "team" {
            Some(Owner::Team)
        } else {
            s.strip_prefix("user#")
                .filter(|e| !e.is_empty())
                .map(|e| Owner::User(e.to_string()))
        }
    }
}

/// How a server signs in with OAuth, from its metadata, plus Coderhelm's
/// client on its authorization server.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OAuthSettings {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub registration_endpoint: Option<String>,
    pub revocation_endpoint: Option<String>,
    pub iss_supported: bool,
    pub cimd_supported: bool,
    pub token_auth_methods: Vec<String>,
    /// The token audience (RFC 8707).
    pub resource: String,
    pub scopes: Vec<String>,
    pub client_id: String,
    pub client_auth: ClientAuth,
    /// Sealed (`mcp-oauth-client`); empty for a public client.
    pub client_secret_sealed: String,
    /// `metadata_document`, `dynamic`, `manual`, or empty before one exists.
    pub registration: String,
    pub legacy: bool,
}

impl OAuthSettings {
    pub fn from_plan(p: &super::discovery::OAuthPlan) -> Self {
        let a = &p.auth_server;
        Self {
            issuer: a.issuer.clone(),
            authorization_endpoint: a.authorization_endpoint.clone(),
            token_endpoint: a.token_endpoint.clone(),
            registration_endpoint: a.registration_endpoint.clone(),
            revocation_endpoint: a.revocation_endpoint.clone(),
            iss_supported: a.iss_supported,
            cimd_supported: a.cimd_supported,
            token_auth_methods: a.token_auth_methods.clone(),
            resource: p.resource.clone(),
            scopes: p.scopes.clone(),
            legacy: p.legacy,
            ..Default::default()
        }
    }

    pub fn auth_server(&self) -> super::discovery::AuthServer {
        super::discovery::AuthServer {
            issuer: self.issuer.clone(),
            authorization_endpoint: self.authorization_endpoint.clone(),
            token_endpoint: self.token_endpoint.clone(),
            registration_endpoint: self.registration_endpoint.clone(),
            revocation_endpoint: self.revocation_endpoint.clone(),
            scopes_supported: Vec::new(),
            token_auth_methods: self.token_auth_methods.clone(),
            cimd_supported: self.cimd_supported,
            iss_supported: self.iss_supported,
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "issuer": self.issuer,
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
            "registration_endpoint": self.registration_endpoint,
            "revocation_endpoint": self.revocation_endpoint,
            "iss_supported": self.iss_supported,
            "cimd_supported": self.cimd_supported,
            "token_auth_methods": self.token_auth_methods,
            "resource": self.resource,
            "scopes": self.scopes,
            "client_id": self.client_id,
            "client_auth": self.client_auth.as_str(),
            "client_secret_sealed": self.client_secret_sealed,
            "registration": self.registration,
            "legacy": self.legacy,
        })
    }

    fn from_json(v: &Value) -> Self {
        let st = |k: &str| v[k].as_str().unwrap_or("").to_string();
        let opt = |k: &str| v[k].as_str().filter(|s| !s.is_empty()).map(str::to_string);
        let list = |k: &str| {
            v[k].as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        Self {
            issuer: st("issuer"),
            authorization_endpoint: st("authorization_endpoint"),
            token_endpoint: st("token_endpoint"),
            registration_endpoint: opt("registration_endpoint"),
            revocation_endpoint: opt("revocation_endpoint"),
            iss_supported: v["iss_supported"].as_bool().unwrap_or(false),
            cimd_supported: v["cimd_supported"].as_bool().unwrap_or(false),
            token_auth_methods: list("token_auth_methods"),
            resource: st("resource"),
            scopes: list("scopes"),
            client_id: st("client_id"),
            client_auth: ClientAuth::parse(&st("client_auth")),
            client_secret_sealed: st("client_secret_sealed"),
            registration: st("registration"),
            legacy: v["legacy"].as_bool().unwrap_or(false),
        }
    }
}

/// A team's remote MCP server.
#[derive(Clone, PartialEq)]
pub struct ServerRecord {
    pub id: String,
    pub name: String,
    pub url: String,
    /// The catalog entry this connects (`linear`), or empty for a server an
    /// admin added by address.
    pub catalog_id: String,
    pub auth: AuthKind,
    pub sharing: Sharing,
    pub oauth: Option<OAuthSettings>,
    /// The header a static key goes in (`Authorization`, `X-API-Key`).
    pub header_name: String,
    /// The key, sealed (`mcp-header`).
    pub header_sealed: String,
    /// `modern` or `legacy` once known.
    pub era: String,
    pub tools: Vec<RemoteTool>,
    pub skipped: Vec<SkippedTool>,
    pub tools_listed_at: String,
    pub allowed: BTreeSet<String>,
    pub approved: BTreeSet<String>,
    /// Who last widened the allow list (they can't approve it themselves).
    pub changed_by: String,
    pub custom_prompt: String,
    pub added_by: String,
    pub added_at: String,
    /// The shared connection's status (or the server's, for keys and no
    /// sign-in). Each person's own connection has its own on its token.
    pub status: Status,
    pub status_reason: String,
    pub status_at: String,
}

impl std::fmt::Debug for ServerRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerRecord")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("auth", &self.auth)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n(item: &Item, k: &str) -> Option<i64> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
}

fn av(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}

fn num(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn set_json(v: &BTreeSet<String>) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".into())
}

impl ServerRecord {
    pub fn new(id: &str, name: &str, url: &str) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            url: url.into(),
            catalog_id: String::new(),
            auth: AuthKind::None,
            sharing: Sharing::Shared,
            oauth: None,
            header_name: String::new(),
            header_sealed: String::new(),
            era: String::new(),
            tools: Vec::new(),
            skipped: Vec::new(),
            tools_listed_at: String::new(),
            allowed: BTreeSet::new(),
            approved: BTreeSet::new(),
            changed_by: String::new(),
            custom_prompt: String::new(),
            added_by: String::new(),
            added_at: String::new(),
            status: Status::NotConnected,
            status_reason: String::new(),
            status_at: String::new(),
        }
    }

    pub fn sk(&self) -> String {
        format!("{SERVER_PREFIX}{}", self.id)
    }

    pub fn to_item(&self, team: &str) -> Item {
        let mut i = Item::new();
        i.insert("pk".into(), av(team));
        i.insert("sk".into(), av(self.sk()));
        i.insert("name".into(), av(&self.name));
        i.insert("url".into(), av(&self.url));
        i.insert("catalog_id".into(), av(&self.catalog_id));
        i.insert("auth".into(), av(self.auth.as_str()));
        i.insert("sharing".into(), av(self.sharing.as_str()));
        if let Some(o) = &self.oauth {
            i.insert("oauth".into(), av(o.to_json().to_string()));
        }
        i.insert("header_name".into(), av(&self.header_name));
        i.insert("header_sealed".into(), av(&self.header_sealed));
        i.insert("era".into(), av(&self.era));
        i.insert(
            "tools".into(),
            av(serde_json::to_string(&self.tools).unwrap_or_else(|_| "[]".into())),
        );
        i.insert(
            "skipped".into(),
            av(serde_json::to_string(&self.skipped).unwrap_or_else(|_| "[]".into())),
        );
        i.insert("tools_listed_at".into(), av(&self.tools_listed_at));
        i.insert("allowed".into(), av(set_json(&self.allowed)));
        i.insert("approved".into(), av(set_json(&self.approved)));
        i.insert("changed_by".into(), av(&self.changed_by));
        i.insert("custom_prompt".into(), av(&self.custom_prompt));
        i.insert("added_by".into(), av(&self.added_by));
        i.insert("added_at".into(), av(&self.added_at));
        i.insert("status".into(), av(self.status.as_str()));
        i.insert("status_reason".into(), av(&self.status_reason));
        i.insert("status_at".into(), av(&self.status_at));
        // Empty strings are fine in DynamoDB non-key attributes.
        i
    }

    pub fn from_item(item: &Item) -> Option<Self> {
        let id = s(item, "sk").strip_prefix(SERVER_PREFIX)?.to_string();
        let set =
            |k: &str| -> BTreeSet<String> { serde_json::from_str(&s(item, k)).unwrap_or_default() };
        let oauth = Some(s(item, "oauth"))
            .filter(|o| !o.is_empty())
            .and_then(|o| serde_json::from_str::<Value>(&o).ok())
            .map(|v| OAuthSettings::from_json(&v));
        Some(Self {
            name: s(item, "name"),
            url: s(item, "url"),
            catalog_id: s(item, "catalog_id"),
            auth: AuthKind::parse(&s(item, "auth")),
            sharing: Sharing::parse(&s(item, "sharing")),
            oauth,
            header_name: s(item, "header_name"),
            header_sealed: s(item, "header_sealed"),
            era: s(item, "era"),
            tools: serde_json::from_str(&s(item, "tools")).unwrap_or_default(),
            skipped: serde_json::from_str(&s(item, "skipped")).unwrap_or_default(),
            tools_listed_at: s(item, "tools_listed_at"),
            allowed: set("allowed"),
            approved: set("approved"),
            changed_by: s(item, "changed_by"),
            custom_prompt: s(item, "custom_prompt"),
            added_by: s(item, "added_by"),
            added_at: s(item, "added_at"),
            status: Status::parse(&s(item, "status")),
            status_reason: s(item, "status_reason"),
            status_at: s(item, "status_at"),
            id,
        })
    }

    /// The owner whose connection agents use for this server on behalf of
    /// `user` (a person's email), or `None` when there is no usable owner:
    /// a personal server used with no person known.
    pub fn owner_for(&self, user: Option<&str>) -> Option<Owner> {
        match (self.auth, self.sharing) {
            (AuthKind::OAuth, Sharing::Personal) => {
                user.filter(|u| !u.trim().is_empty()).map(Owner::user)
            }
            _ => Some(Owner::Team),
        }
    }

    /// What the dashboard sees. Built field by field: no sealed value, no
    /// token and no client secret can reach it.
    pub fn public_json(&self, mine: Option<&TokenRecord>, can_edit: bool) -> Value {
        let tools: Vec<Value> = self
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "title": t.title,
                    "description": crate::truncate_str(&t.description, 600),
                    "access": super::tools::access(t).as_str(),
                    "allowed": self.allowed.contains(&t.name),
                    "approved": self.approved.contains(&t.name),
                    "offerable": super::tools::agent_tool_name(&self.id, &t.name).is_some(),
                })
            })
            .collect();
        let o = self.oauth.as_ref();
        json!({
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "catalog_id": self.catalog_id,
            "auth": self.auth.as_str(),
            "sharing": self.sharing.as_str(),
            "header_name": self.header_name,
            "has_key": !self.header_sealed.is_empty(),
            "issuer": o.map(|o| o.issuer.clone()),
            "registration": o.map(|o| o.registration.clone()),
            "needs_client": o.is_some_and(|o| o.client_id.is_empty() && o.registration_endpoint.is_none() && !o.cimd_supported),
            "has_client": o.is_some_and(|o| !o.client_id.is_empty()),
            "scopes": o.map(|o| o.scopes.clone()).unwrap_or_default(),
            "status": self.status.as_str(),
            "status_reason": self.status_reason,
            "status_at": self.status_at,
            "my_connection": mine.map(|t| json!({
                "status": t.status.as_str(),
                "status_reason": t.status_reason,
                "connected_at": t.connected_at,
            })),
            "tools": tools,
            "skipped": self.skipped,
            "tools_listed_at": self.tools_listed_at,
            "awaiting_approval": super::tools::awaiting_approval(self),
            "changed_by": self.changed_by,
            "custom_prompt": if can_edit { self.custom_prompt.clone() } else { String::new() },
            "added_by": self.added_by,
            "added_at": self.added_at,
        })
    }
}

/// One connection's tokens, sealed.
#[derive(Clone, PartialEq, Eq)]
pub struct TokenRecord {
    pub owner: Owner,
    pub sealed: String,
    pub expires_at: Option<i64>,
    pub scope: String,
    /// Bumped on every write; a refresh only lands if nobody else's did.
    pub version: i64,
    pub status: Status,
    pub status_reason: String,
    /// Scopes a server asked for at run time (403 `insufficient_scope`):
    /// asked for, together with the rest, on the next sign-in.
    pub step_up: Vec<String>,
    pub connected_by: String,
    pub connected_at: String,
}

impl std::fmt::Debug for TokenRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRecord")
            .field("owner", &self.owner)
            .field("version", &self.version)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

pub fn token_sk(server_id: &str, owner: &Owner) -> String {
    format!("{TOKEN_PREFIX}{server_id}#{}", owner.key())
}

impl TokenRecord {
    pub fn to_item(&self, team: &str, server_id: &str) -> Item {
        let mut i = Item::new();
        i.insert("pk".into(), av(team));
        i.insert("sk".into(), av(token_sk(server_id, &self.owner)));
        i.insert("token_sealed".into(), av(&self.sealed));
        if let Some(e) = self.expires_at {
            i.insert("expires_at".into(), num(e));
        }
        i.insert("scope".into(), av(&self.scope));
        i.insert("version".into(), num(self.version));
        i.insert("status".into(), av(self.status.as_str()));
        i.insert("status_reason".into(), av(&self.status_reason));
        i.insert(
            "step_up".into(),
            av(serde_json::to_string(&self.step_up).unwrap_or_else(|_| "[]".into())),
        );
        i.insert("connected_by".into(), av(&self.connected_by));
        i.insert("connected_at".into(), av(&self.connected_at));
        i
    }

    pub fn from_item(item: &Item, server_id: &str) -> Option<Self> {
        let sk = s(item, "sk");
        let owner = sk
            .strip_prefix(&format!("{TOKEN_PREFIX}{server_id}#"))
            .and_then(Owner::parse)?;
        Some(Self {
            owner,
            sealed: s(item, "token_sealed"),
            expires_at: n(item, "expires_at"),
            scope: s(item, "scope"),
            version: n(item, "version").unwrap_or(0),
            status: Status::parse(&s(item, "status")),
            status_reason: s(item, "status_reason"),
            step_up: serde_json::from_str(&s(item, "step_up")).unwrap_or_default(),
            connected_by: s(item, "connected_by"),
            connected_at: s(item, "connected_at"),
        })
    }
}

/// A sign-in in progress.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingAuth {
    /// The hash of the `state` sent to the authorization server.
    pub key: String,
    pub server_id: String,
    pub owner: Owner,
    /// Who started it: the callback must come from the same person.
    pub user_sub: String,
    pub user_email: String,
    pub verifier: String,
    /// The issuer recorded when the flow started (RFC 9207 check).
    pub issuer: String,
    pub redirect_uri: String,
    pub expires_at: i64,
}

impl std::fmt::Debug for PendingAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAuth")
            .field("server_id", &self.server_id)
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl PendingAuth {
    pub fn to_item(&self, team: &str) -> Item {
        let mut i = Item::new();
        i.insert("pk".into(), av(team));
        i.insert("sk".into(), av(format!("{PENDING_PREFIX}{}", self.key)));
        i.insert("server_id".into(), av(&self.server_id));
        i.insert("owner".into(), av(self.owner.key()));
        i.insert("user_sub".into(), av(&self.user_sub));
        i.insert("user_email".into(), av(&self.user_email));
        i.insert("verifier".into(), av(&self.verifier));
        i.insert("issuer".into(), av(&self.issuer));
        i.insert("redirect_uri".into(), av(&self.redirect_uri));
        i.insert("expires_at".into(), num(self.expires_at));
        // DynamoDB removes it on its own once expired (when TTL is on).
        i.insert("ttl".into(), num(self.expires_at));
        i
    }

    pub fn from_item(item: &Item) -> Option<Self> {
        Some(Self {
            key: s(item, "sk").strip_prefix(PENDING_PREFIX)?.to_string(),
            server_id: s(item, "server_id"),
            owner: Owner::parse(&s(item, "owner"))?,
            user_sub: s(item, "user_sub"),
            user_email: s(item, "user_email"),
            verifier: s(item, "verifier"),
            issuer: s(item, "issuer"),
            redirect_uri: s(item, "redirect_uri"),
            expires_at: n(item, "expires_at").unwrap_or(0),
        })
    }

    /// The callback must come back to the person who started the sign-in,
    /// before it expires.
    pub fn check(&self, user_sub: &str, user_email: &str, now: i64) -> Result<(), String> {
        if now > self.expires_at {
            return Err("That sign-in took too long. Start it again.".into());
        }
        if self.user_sub != user_sub || !self.user_email.eq_ignore_ascii_case(user_email) {
            return Err(
                "That sign-in was started by someone else. Start it again yourself.".into(),
            );
        }
        Ok(())
    }
}

/// One tool call, for the audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub server_id: String,
    pub tool: String,
    /// Who or what called it: `plan chat (dana@example.com)`, `coder, run 01J...`.
    pub actor: String,
    pub owner: String,
    pub ok: bool,
    pub ms: i64,
    /// Why it failed, short. Never a token or an argument value.
    pub detail: String,
    pub at: String,
}

impl LogEntry {
    pub fn to_item(&self, team: &str, now: i64) -> Item {
        let mut i = Item::new();
        i.insert("pk".into(), av(team));
        i.insert(
            "sk".into(),
            av(format!(
                "{LOG_PREFIX}{}#{}",
                self.server_id,
                ulid::Ulid::new()
            )),
        );
        i.insert("tool".into(), av(&self.tool));
        i.insert("actor".into(), av(&self.actor));
        i.insert("owner".into(), av(&self.owner));
        i.insert("ok".into(), AttributeValue::Bool(self.ok));
        i.insert("ms".into(), num(self.ms));
        i.insert("detail".into(), av(crate::truncate_str(&self.detail, 300)));
        i.insert("at".into(), av(&self.at));
        i.insert("ttl".into(), num(now + LOG_TTL_SECS));
        i
    }

    pub fn from_item(item: &Item) -> Option<Self> {
        let sk = s(item, "sk");
        let rest = sk.strip_prefix(LOG_PREFIX)?;
        let (server_id, _) = rest.rsplit_once('#')?;
        Some(Self {
            server_id: server_id.to_string(),
            tool: s(item, "tool"),
            actor: s(item, "actor"),
            owner: s(item, "owner"),
            ok: item
                .get("ok")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            ms: n(item, "ms").unwrap_or(0),
            detail: s(item, "detail"),
            at: s(item, "at"),
        })
    }

    pub fn json(&self) -> Value {
        json!({"tool": self.tool, "actor": self.actor, "ok": self.ok, "ms": self.ms, "detail": self.detail, "at": self.at})
    }
}

/// Reads and writes the records above. DynamoDB in production; a map in
/// tests.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get_server(&self, team: &str, id: &str) -> Result<Option<ServerRecord>, String>;
    async fn list_servers(&self, team: &str) -> Result<Vec<ServerRecord>, String>;
    async fn put_server(&self, team: &str, s: &ServerRecord) -> Result<(), String>;
    async fn set_status(
        &self,
        team: &str,
        id: &str,
        status: Status,
        reason: &str,
    ) -> Result<(), String>;
    async fn set_era(&self, team: &str, id: &str, era: &str) -> Result<(), String>;
    async fn delete_server(&self, team: &str, id: &str) -> Result<(), String>;
    async fn get_token(
        &self,
        team: &str,
        id: &str,
        owner: &Owner,
    ) -> Result<Option<TokenRecord>, String>;
    async fn list_tokens(&self, team: &str, id: &str) -> Result<Vec<TokenRecord>, String>;
    /// Writes a token record. With `expected`, only if the stored version is
    /// still that one (`Ok(false)` when someone else wrote first).
    async fn put_token(
        &self,
        team: &str,
        id: &str,
        t: &TokenRecord,
        expected: Option<i64>,
    ) -> Result<bool, String>;
    async fn set_token_status(
        &self,
        team: &str,
        id: &str,
        owner: &Owner,
        status: Status,
        reason: &str,
        step_up: &[String],
    ) -> Result<(), String>;
    async fn delete_token(&self, team: &str, id: &str, owner: &Owner) -> Result<(), String>;
    async fn put_pending(&self, team: &str, p: &PendingAuth) -> Result<(), String>;
    /// Reads and deletes a pending sign-in in one step: a `state` works once.
    async fn take_pending(&self, team: &str, key: &str) -> Result<Option<PendingAuth>, String>;
    async fn put_log(&self, team: &str, e: &LogEntry, now: i64) -> Result<(), String>;
    async fn list_logs(&self, team: &str, id: &str, limit: i32) -> Result<Vec<LogEntry>, String>;
}

/// The MCP configs table.
pub struct DynamoStore {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

impl DynamoStore {
    async fn query_prefix(
        &self,
        team: &str,
        prefix: &str,
        limit: Option<i32>,
        newest_first: bool,
    ) -> Result<Vec<Item>, String> {
        let mut out = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let mut q = self
                .dynamo
                .query()
                .table_name(&self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", av(team))
                .expression_attribute_values(":p", av(prefix))
                .scan_index_forward(!newest_first)
                .set_exclusive_start_key(start.take());
            if let Some(l) = limit {
                q = q.limit(l);
            }
            let r = q
                .send()
                .await
                .map_err(|e| format!("query mcp table: {e}"))?;
            out.extend(r.items().iter().cloned());
            match r.last_evaluated_key() {
                Some(k) if limit.is_none() => start = Some(k.clone()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn get(&self, team: &str, sk: &str) -> Result<Option<Item>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(sk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("get mcp record: {e}"))?;
        Ok(r.item().cloned())
    }

    async fn delete(&self, team: &str, sk: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(sk))
            .send()
            .await
            .map_err(|e| format!("delete mcp record: {e}"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Store for DynamoStore {
    async fn get_server(&self, team: &str, id: &str) -> Result<Option<ServerRecord>, String> {
        Ok(self
            .get(team, &format!("{SERVER_PREFIX}{id}"))
            .await?
            .as_ref()
            .and_then(ServerRecord::from_item))
    }

    async fn list_servers(&self, team: &str) -> Result<Vec<ServerRecord>, String> {
        Ok(self
            .query_prefix(team, SERVER_PREFIX, None, false)
            .await?
            .iter()
            .filter_map(ServerRecord::from_item)
            .collect())
    }

    async fn put_server(&self, team: &str, s: &ServerRecord) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(s.to_item(team)))
            .send()
            .await
            .map_err(|e| format!("put mcp server: {e}"))?;
        Ok(())
    }

    async fn set_status(
        &self,
        team: &str,
        id: &str,
        status: Status,
        reason: &str,
    ) -> Result<(), String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(format!("{SERVER_PREFIX}{id}")))
            .update_expression("SET #s = :s, status_reason = :r, status_at = :t")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", av(status.as_str()))
            .expression_attribute_values(":r", av(crate::truncate_str(reason, 300)))
            .expression_attribute_values(":t", av(chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
        ignore_missing(r.map(|_| ()))
    }

    async fn set_era(&self, team: &str, id: &str, era: &str) -> Result<(), String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(format!("{SERVER_PREFIX}{id}")))
            .update_expression("SET era = :e")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":e", av(era))
            .send()
            .await;
        ignore_missing(r.map(|_| ()))
    }

    async fn delete_server(&self, team: &str, id: &str) -> Result<(), String> {
        self.delete(team, &format!("{SERVER_PREFIX}{id}")).await
    }

    async fn get_token(
        &self,
        team: &str,
        id: &str,
        owner: &Owner,
    ) -> Result<Option<TokenRecord>, String> {
        Ok(self
            .get(team, &token_sk(id, owner))
            .await?
            .and_then(|i| TokenRecord::from_item(&i, id)))
    }

    async fn list_tokens(&self, team: &str, id: &str) -> Result<Vec<TokenRecord>, String> {
        Ok(self
            .query_prefix(team, &format!("{TOKEN_PREFIX}{id}#"), None, false)
            .await?
            .iter()
            .filter_map(|i| TokenRecord::from_item(i, id))
            .collect())
    }

    async fn put_token(
        &self,
        team: &str,
        id: &str,
        t: &TokenRecord,
        expected: Option<i64>,
    ) -> Result<bool, String> {
        let mut req = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(t.to_item(team, id)));
        if let Some(v) = expected {
            req = req
                .condition_expression("version = :v")
                .expression_attribute_values(":v", num(v));
        }
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                let lost = e
                    .as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception());
                if lost {
                    Ok(false)
                } else {
                    Err(format!("put mcp token: {}", e.into_service_error()))
                }
            }
        }
    }

    async fn set_token_status(
        &self,
        team: &str,
        id: &str,
        owner: &Owner,
        status: Status,
        reason: &str,
        step_up: &[String],
    ) -> Result<(), String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(token_sk(id, owner)))
            .update_expression("SET #s = :s, status_reason = :r, step_up = :u")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", av(status.as_str()))
            .expression_attribute_values(":r", av(crate::truncate_str(reason, 300)))
            .expression_attribute_values(
                ":u",
                av(serde_json::to_string(step_up).unwrap_or_else(|_| "[]".into())),
            )
            .send()
            .await;
        ignore_missing(r.map(|_| ()))
    }

    async fn delete_token(&self, team: &str, id: &str, owner: &Owner) -> Result<(), String> {
        self.delete(team, &token_sk(id, owner)).await
    }

    async fn put_pending(&self, team: &str, p: &PendingAuth) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(p.to_item(team)))
            .send()
            .await
            .map_err(|e| format!("put mcp sign-in: {e}"))?;
        Ok(())
    }

    async fn take_pending(&self, team: &str, key: &str) -> Result<Option<PendingAuth>, String> {
        let r = self
            .dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", av(team))
            .key("sk", av(format!("{PENDING_PREFIX}{key}")))
            .condition_expression("attribute_exists(pk)")
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await;
        match r {
            Ok(o) => Ok(o.attributes().and_then(PendingAuth::from_item)),
            Err(e) => {
                let missing = e
                    .as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception());
                if missing {
                    Ok(None)
                } else {
                    Err(format!("take mcp sign-in: {}", e.into_service_error()))
                }
            }
        }
    }

    async fn put_log(&self, team: &str, e: &LogEntry, now: i64) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(e.to_item(team, now)))
            .send()
            .await
            .map_err(|e| format!("put mcp log: {e}"))?;
        Ok(())
    }

    async fn list_logs(&self, team: &str, id: &str, limit: i32) -> Result<Vec<LogEntry>, String> {
        Ok(self
            .query_prefix(team, &format!("{LOG_PREFIX}{id}#"), Some(limit), true)
            .await?
            .iter()
            .filter_map(LogEntry::from_item)
            .collect())
    }
}

/// A status update on a record that was deleted meanwhile is not an error.
fn ignore_missing<E>(
    r: Result<
        (),
        aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
            E,
        >,
    >,
) -> Result<(), String>
where
    E: std::fmt::Debug,
{
    match r {
        Ok(()) => Ok(()),
        Err(e) => {
            let missing = e
                .as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception());
            if missing {
                Ok(())
            } else {
                Err(format!("update mcp record: {e:?}"))
            }
        }
    }
}

/// An in-memory store for tests.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct MemStore {
        pub items: Mutex<std::collections::BTreeMap<(String, String), Item>>,
    }

    impl MemStore {
        fn get(&self, team: &str, sk: &str) -> Option<Item> {
            self.items
                .lock()
                .unwrap()
                .get(&(team.to_string(), sk.to_string()))
                .cloned()
        }
        fn put(&self, team: &str, item: Item) {
            let sk = s(&item, "sk");
            self.items
                .lock()
                .unwrap()
                .insert((team.to_string(), sk), item);
        }
        fn prefix(&self, team: &str, p: &str) -> Vec<Item> {
            self.items
                .lock()
                .unwrap()
                .iter()
                .filter(|((t, sk), _)| t == team && sk.starts_with(p))
                .map(|(_, i)| i.clone())
                .collect()
        }
        fn update(&self, team: &str, sk: &str, f: impl FnOnce(&mut Item)) {
            if let Some(i) = self
                .items
                .lock()
                .unwrap()
                .get_mut(&(team.to_string(), sk.to_string()))
            {
                f(i);
            }
        }
    }

    #[async_trait::async_trait]
    impl Store for MemStore {
        async fn get_server(&self, team: &str, id: &str) -> Result<Option<ServerRecord>, String> {
            Ok(self
                .get(team, &format!("{SERVER_PREFIX}{id}"))
                .as_ref()
                .and_then(ServerRecord::from_item))
        }
        async fn list_servers(&self, team: &str) -> Result<Vec<ServerRecord>, String> {
            Ok(self
                .prefix(team, SERVER_PREFIX)
                .iter()
                .filter_map(ServerRecord::from_item)
                .collect())
        }
        async fn put_server(&self, team: &str, s: &ServerRecord) -> Result<(), String> {
            self.put(team, s.to_item(team));
            Ok(())
        }
        async fn set_status(
            &self,
            team: &str,
            id: &str,
            status: Status,
            reason: &str,
        ) -> Result<(), String> {
            self.update(team, &format!("{SERVER_PREFIX}{id}"), |i| {
                i.insert("status".into(), av(status.as_str()));
                i.insert("status_reason".into(), av(reason));
            });
            Ok(())
        }
        async fn set_era(&self, team: &str, id: &str, era: &str) -> Result<(), String> {
            self.update(team, &format!("{SERVER_PREFIX}{id}"), |i| {
                i.insert("era".into(), av(era));
            });
            Ok(())
        }
        async fn delete_server(&self, team: &str, id: &str) -> Result<(), String> {
            self.items
                .lock()
                .unwrap()
                .remove(&(team.to_string(), format!("{SERVER_PREFIX}{id}")));
            Ok(())
        }
        async fn get_token(
            &self,
            team: &str,
            id: &str,
            owner: &Owner,
        ) -> Result<Option<TokenRecord>, String> {
            Ok(self
                .get(team, &token_sk(id, owner))
                .and_then(|i| TokenRecord::from_item(&i, id)))
        }
        async fn list_tokens(&self, team: &str, id: &str) -> Result<Vec<TokenRecord>, String> {
            Ok(self
                .prefix(team, &format!("{TOKEN_PREFIX}{id}#"))
                .iter()
                .filter_map(|i| TokenRecord::from_item(i, id))
                .collect())
        }
        async fn put_token(
            &self,
            team: &str,
            id: &str,
            t: &TokenRecord,
            expected: Option<i64>,
        ) -> Result<bool, String> {
            if let Some(v) = expected {
                let cur = self
                    .get(team, &token_sk(id, &t.owner))
                    .and_then(|i| n(&i, "version"));
                if cur != Some(v) {
                    return Ok(false);
                }
            }
            self.put(team, t.to_item(team, id));
            Ok(true)
        }
        async fn set_token_status(
            &self,
            team: &str,
            id: &str,
            owner: &Owner,
            status: Status,
            reason: &str,
            step_up: &[String],
        ) -> Result<(), String> {
            self.update(team, &token_sk(id, owner), |i| {
                i.insert("status".into(), av(status.as_str()));
                i.insert("status_reason".into(), av(reason));
                i.insert(
                    "step_up".into(),
                    av(serde_json::to_string(step_up).unwrap()),
                );
            });
            Ok(())
        }
        async fn delete_token(&self, team: &str, id: &str, owner: &Owner) -> Result<(), String> {
            self.items
                .lock()
                .unwrap()
                .remove(&(team.to_string(), token_sk(id, owner)));
            Ok(())
        }
        async fn put_pending(&self, team: &str, p: &PendingAuth) -> Result<(), String> {
            self.put(team, p.to_item(team));
            Ok(())
        }
        async fn take_pending(&self, team: &str, key: &str) -> Result<Option<PendingAuth>, String> {
            Ok(self
                .items
                .lock()
                .unwrap()
                .remove(&(team.to_string(), format!("{PENDING_PREFIX}{key}")))
                .as_ref()
                .and_then(PendingAuth::from_item))
        }
        async fn put_log(&self, team: &str, e: &LogEntry, now: i64) -> Result<(), String> {
            self.put(team, e.to_item(team, now));
            Ok(())
        }
        async fn list_logs(
            &self,
            team: &str,
            id: &str,
            limit: i32,
        ) -> Result<Vec<LogEntry>, String> {
            let mut v: Vec<LogEntry> = self
                .prefix(team, &format!("{LOG_PREFIX}{id}#"))
                .iter()
                .filter_map(LogEntry::from_item)
                .collect();
            v.reverse();
            v.truncate(limit as usize);
            Ok(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ServerRecord {
        let mut s = ServerRecord::new("docs", "Docs", "https://mcp.example.com/mcp");
        s.auth = AuthKind::OAuth;
        s.sharing = Sharing::Personal;
        s.header_sealed = "SEALED-HEADER".into();
        s.oauth = Some(OAuthSettings {
            issuer: "https://auth.example.com".into(),
            client_id: "cid".into(),
            client_secret_sealed: "SEALED-CLIENT".into(),
            resource: "https://mcp.example.com/mcp".into(),
            scopes: vec!["read".into()],
            ..Default::default()
        });
        s.allowed = ["search".to_string()].into();
        s
    }

    #[test]
    fn a_server_round_trips() {
        let s = sample();
        let back = ServerRecord::from_item(&s.to_item("TEAM#a")).unwrap();
        assert!(back == s);
    }

    #[test]
    fn the_dashboard_never_sees_sealed_values() {
        let s = sample();
        let tok = TokenRecord {
            owner: Owner::user("Dana@Example.com"),
            sealed: "SEALED-TOKEN".into(),
            expires_at: None,
            scope: String::new(),
            version: 1,
            status: Status::Connected,
            status_reason: String::new(),
            step_up: vec![],
            connected_by: "dana@example.com".into(),
            connected_at: String::new(),
        };
        let text = s.public_json(Some(&tok), true).to_string();
        for secret in [
            "SEALED-HEADER",
            "SEALED-CLIENT",
            "SEALED-TOKEN",
            "client_secret",
        ] {
            assert!(!text.contains(secret), "{secret}");
        }
        assert!(text.contains("\"has_key\":true"));
    }

    #[test]
    fn owners_and_token_keys() {
        assert_eq!(
            Owner::user(" Dana@Example.com ").key(),
            "user#dana@example.com"
        );
        assert_eq!(Owner::parse("team"), Some(Owner::Team));
        assert_eq!(
            Owner::parse("user#a@b.c"),
            Some(Owner::User("a@b.c".into()))
        );
        assert_eq!(Owner::parse("user#"), None);
        assert_eq!(token_sk("docs", &Owner::Team), "MCPTOKEN#docs#team");
        // A server whose id starts with another's never reads its tokens.
        let t = TokenRecord::from_item(
            &[("sk".to_string(), av("MCPTOKEN#docs-2#team"))].into(),
            "docs",
        );
        assert!(t.is_none());
    }

    #[test]
    fn personal_servers_need_a_person() {
        let s = sample();
        assert_eq!(
            s.owner_for(Some("Dana@x.com")),
            Some(Owner::User("dana@x.com".into()))
        );
        assert_eq!(s.owner_for(None), None);
        let mut shared = sample();
        shared.sharing = Sharing::Shared;
        assert_eq!(shared.owner_for(None), Some(Owner::Team));
    }

    #[test]
    fn a_pending_sign_in_belongs_to_its_starter() {
        let p = PendingAuth {
            key: "k".into(),
            server_id: "docs".into(),
            owner: Owner::Team,
            user_sub: "USER#1".into(),
            user_email: "a@x.com".into(),
            verifier: "v".into(),
            issuer: "https://auth.example.com".into(),
            redirect_uri: "https://app.example.com/cb/".into(),
            expires_at: 1000,
        };
        assert!(p.check("USER#1", "A@x.com", 999).is_ok());
        assert!(p.check("USER#2", "a@x.com", 999).is_err());
        assert!(p.check("USER#1", "b@x.com", 999).is_err());
        assert!(p.check("USER#1", "a@x.com", 1001).is_err());
        let back = PendingAuth::from_item(&p.to_item("TEAM#a")).unwrap();
        assert!(back == p);
    }
}
