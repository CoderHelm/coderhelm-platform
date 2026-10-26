//! Opening a connector with the team's connection, in the gateway (the
//! picker) and the worker (syncs) alike.
//!
//! Each kind is reached through the connections the team already has,
//! newest kind of sign-in first:
//!
//! | kind | 1st | 2nd |
//! |---|---|---|
//! | Notion | Notion sign-in (OAuth) | the Notion token saved on Integrations |
//! | Confluence | Atlassian sign-in (OAuth) | the Confluence email and API token saved on Integrations |
//! | Sanity | a member's Sanity sign-in (the remote MCP server) | the Sanity token and project id saved on Integrations |
//! | Google Drive | Google sign-in (OAuth) | |
//! | GitHub | the Coderhelm GitHub App | |
//! | Website | none needed | |
//!
//! A token is opened for the one call that needs it and never leaves the
//! server.
//!
//! A source read through a sign-in ([`super::SignIn`]) is opened by
//! [`open_source`] with that member's connection on the provider's remote
//! MCP server. Which sign-in a member has is found by [`find_signin`]: their
//! own connection on the team's Connected apps entry for the provider
//! first, then Knowledge's own record for it (`ks-<kind>`, made on first
//! use by asking the server how it signs in), then a connection the team
//! shares. Notion and Confluence keep their own sign-ins and saved keys:
//! their remote MCP servers can't list what a selection covers cheaply
//! enough to keep it in sync (see [`signin_url`]).

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_json::{json, Value};

use super::connector::{Connector, ConnectorError};
use super::mcp::RuntimeTools;
use super::oauth::{catalog_id, connection_id};
use super::sanity_mcp::{self, SanityMcp};
use super::{confluence, gdrive, github, notion, sanity, web, Kind, SignIn, Source};
use crate::live::connectors::{self as conns, ConnKind};
use crate::mcp_remote::http::Http;
use crate::mcp_remote::runtime::{ConnError, Detected, KmsCipher, Runtime};
use crate::mcp_remote::store::{AuthKind, OAuthSettings, Owner, ServerRecord, Sharing, Status};

/// What opening a connector needs.
pub struct Env {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub kms: aws_sdk_kms::Client,
    pub secrets_key_id: String,
    pub settings_table: String,
    /// Where MCP servers, their tokens and plugin credentials are kept.
    pub mcp_table: String,
    pub team: String,
    pub http: Arc<dyn Http>,
    /// An installation token of the team's GitHub App, when it has one.
    pub github_token: Option<String>,
}

/// How a connector reaches its provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Via {
    OAuth,
    /// A member's sign-in on the provider's remote MCP server.
    SignIn,
    SavedKey,
    GithubApp,
    Public,
}

/// A connector ready to call, and the settings it worked out on the way
/// (Confluence: which site a sign-in reaches).
pub struct Opened<'a> {
    pub connector: Box<dyn Connector + 'a>,
    pub via: Via,
    pub options: Value,
}

impl Env {
    pub fn runtime(&self) -> Runtime {
        Runtime {
            store: Arc::new(crate::mcp_remote::store::DynamoStore {
                dynamo: self.dynamo.clone(),
                table: self.mcp_table.clone(),
            }),
            cipher: Arc::new(KmsCipher {
                client: self.kms.clone(),
                key_id: self.secrets_key_id.clone(),
            }),
            http: Arc::new(crate::mcp_remote::http::GuardedHttps),
            team: self.team.clone(),
            clock: crate::mcp_remote::runtime::system_clock(),
        }
    }

    /// The team's sign-in for `kind`: `Ok(None)` when nobody signed in.
    pub async fn oauth_header(&self, kind: Kind) -> Result<Option<String>, ConnectorError> {
        let rt = self.runtime();
        let Some(rec) = rt
            .store
            .get_server(&self.team, &connection_id(kind))
            .await
            .map_err(ConnectorError::Temporary)?
        else {
            return Ok(None);
        };
        match rt.auth_header(&rec, &Owner::Team, false).await {
            Ok(Some(h)) => Ok(Some(h.1)),
            Ok(None) | Err(ConnError::NotConnected) => Ok(None),
            Err(ConnError::NeedsReconnect(why)) => Err(ConnectorError::Reconnect(why)),
            Err(ConnError::Failed(why)) => Err(ConnectorError::Reconnect(why)),
            Err(e) => Err(ConnectorError::Temporary(format!("{e:?}"))),
        }
    }

    /// Is the team signed in to `kind` (and not waiting to reconnect)?
    pub async fn oauth_state(&self, kind: Kind) -> (bool, String, String) {
        let rt = self.runtime();
        match rt
            .store
            .get_token(&self.team, &connection_id(kind), &Owner::Team)
            .await
        {
            Ok(Some(t)) if t.status == Status::Connected => (true, String::new(), t.connected_by),
            Ok(Some(t)) => (false, t.status_reason, t.connected_by),
            _ => (false, String::new(), String::new()),
        }
    }

    /// A plugin's saved credentials (Notion, Sanity), opened.
    pub async fn plugin_credentials(
        &self,
        plugin: &str,
    ) -> Option<crate::mcp_credentials::Credentials> {
        let item = self
            .dynamo
            .get_item()
            .table_name(&self.mcp_table)
            .key("pk", AttributeValue::S(self.team.clone()))
            .key("sk", AttributeValue::S(format!("PLUGIN#{plugin}")))
            .send()
            .await
            .ok()?
            .item()
            .cloned()?;
        let stored = crate::mcp_credentials::stored(&item);
        let cipher = KmsCipher {
            client: self.kms.clone(),
            key_id: self.secrets_key_id.clone(),
        };
        crate::mcp_credentials::load(&cipher, &self.team, plugin, &stored)
            .await
            .ok()
            .filter(|c| !c.is_empty())
    }

    /// The Confluence connection saved with an email and API token (not the
    /// one through the Jira app): its site, email and token.
    pub async fn confluence_token(&self) -> Option<(String, String, String)> {
        let c = conns::get(
            &self.dynamo,
            &self.settings_table,
            &self.team,
            ConnKind::Confluence,
            "",
        )
        .await
        .ok()
        .flatten()?;
        if c.via_jira_app() || c.key_sealed.is_empty() || c.email.is_empty() {
            return None;
        }
        let site = crate::live::confluence::check_site(&c.site).ok()?;
        let key = crate::live::secret::open(&self.kms, &self.team, &c.key_sealed)
            .await
            .ok()?;
        Some((site, c.email, key))
    }
}

fn not_connected(kind: Kind) -> ConnectorError {
    ConnectorError::Reconnect(format!("{} isn't connected.", kind.label()))
}

/// Opens `kind` with the team's connection.
pub async fn open<'a>(
    env: &'a Env,
    kind: Kind,
    options: &Value,
) -> Result<Opened<'a>, ConnectorError> {
    let http = env.http.as_ref();
    let mut opts = if options.is_object() {
        options.clone()
    } else {
        json!({})
    };
    let (connector, via): (Box<dyn Connector + 'a>, Via) = match kind {
        Kind::Notion => {
            if let Some(h) = env.oauth_header(kind).await? {
                (Box::new(notion::Notion { http, auth: h }), Via::OAuth)
            } else {
                let key = env
                    .plugin_credentials("notion")
                    .await
                    .and_then(|c| c.get("api_key").cloned())
                    .filter(|k| !k.trim().is_empty())
                    .ok_or_else(|| not_connected(kind))?;
                (
                    Box::new(notion::Notion {
                        http,
                        auth: format!("Bearer {}", key.trim()),
                    }),
                    Via::SavedKey,
                )
            }
        }
        Kind::Confluence => {
            if let Some(h) = env.oauth_header(kind).await? {
                let cloud = opts["cloud_id"].as_str().unwrap_or("").to_string();
                let (cloud, site) = if cloud.is_empty() {
                    let sites = confluence::accessible_sites(http, &h).await?;
                    let (id, url, _) = sites.into_iter().next().ok_or_else(|| {
                        ConnectorError::Forbidden(
                            "The Atlassian sign-in can't reach any Confluence site.".into(),
                        )
                    })?;
                    (id, url)
                } else {
                    (cloud, opts["site"].as_str().unwrap_or("").to_string())
                };
                if !cloud.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                    return Err(ConnectorError::Config(
                        "The Confluence site id isn't valid.".into(),
                    ));
                }
                opts["cloud_id"] = json!(cloud);
                opts["site"] = json!(site);
                (
                    Box::new(confluence::Confluence {
                        http,
                        api_base: confluence::oauth_base(&cloud),
                        site,
                        auth: h,
                    }),
                    Via::OAuth,
                )
            } else {
                let (site, email, key) = env
                    .confluence_token()
                    .await
                    .ok_or_else(|| not_connected(kind))?;
                use base64::Engine;
                let basic =
                    base64::engine::general_purpose::STANDARD.encode(format!("{email}:{key}"));
                opts["site"] = json!(site);
                (
                    Box::new(confluence::Confluence {
                        http,
                        api_base: site.clone(),
                        site,
                        auth: format!("Basic {basic}"),
                    }),
                    Via::SavedKey,
                )
            }
        }
        Kind::Sanity => {
            let creds = env
                .plugin_credentials("sanity")
                .await
                .ok_or_else(|| not_connected(kind))?;
            let token = creds.get("api_token").cloned().unwrap_or_default();
            let project = opts["project_id"]
                .as_str()
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .or_else(|| creds.get("project_id").cloned())
                .unwrap_or_default();
            if token.trim().is_empty() || project.trim().is_empty() {
                return Err(not_connected(kind));
            }
            opts["project_id"] = json!(project.trim());
            (
                Box::new(sanity::Sanity {
                    http,
                    project_id: project.trim().to_string(),
                    auth: format!("Bearer {}", token.trim()),
                }),
                Via::SavedKey,
            )
        }
        Kind::GoogleDrive => {
            let h = env
                .oauth_header(kind)
                .await?
                .ok_or_else(|| not_connected(kind))?;
            (Box::new(gdrive::Drive { http, auth: h }), Via::OAuth)
        }
        Kind::Github => {
            let t = env.github_token.clone().ok_or_else(|| {
                ConnectorError::Reconnect(
                    "The Coderhelm GitHub App isn't installed for this team.".into(),
                )
            })?;
            (
                Box::new(github::GitHub {
                    http,
                    auth: format!("Bearer {t}"),
                }),
                Via::GithubApp,
            )
        }
        Kind::Web => (Box::new(web::Web { http }), Via::Public),
    };
    Ok(Opened {
        connector,
        via,
        options: opts,
    })
}

// ── Sign-ins through a provider's remote MCP server ─────────────────────────

/// The remote MCP server a kind can sync through with a member's sign-in.
///
/// Only Sanity: its server lists documents by query with their revision and
/// edit time, so a sync can ask for just what changed. Notion's server has
/// no way to list a page's children or read an edit time without fetching
/// each page (and cuts long pages short), and Atlassian's publishes neither
/// its tools' arguments nor their answers; both keep their own sign-in and
/// saved keys.
pub fn signin_url(kind: Kind) -> Option<&'static str> {
    match kind {
        Kind::Sanity => Some(sanity_mcp::MCP_URL),
        _ => None,
    }
}

fn same_server(kind: Kind, url: &str) -> bool {
    match kind {
        Kind::Sanity => sanity_mcp::is_sanity_mcp(url),
        _ => false,
    }
}

/// A sign-in a member can sync with.
#[derive(Debug, Clone)]
pub struct SignedIn {
    pub server: ServerRecord,
    pub owner: Owner,
    /// Who made the connection.
    pub connected_by: String,
}

impl SignedIn {
    /// What a source records about it.
    pub fn signin(&self, email: &str, name: &str, account: &str) -> SignIn {
        SignIn {
            server_id: self.server.id.clone(),
            owner: self.owner.key(),
            email: email.to_ascii_lowercase(),
            name: crate::truncate_str(name.trim(), 120).to_string(),
            account: account.to_string(),
            alerted_at: String::new(),
        }
    }
}

/// The records on `kind`'s remote MCP server, in the order a member's
/// sign-in is looked for: Connected apps entries signed in per person, then
/// Knowledge's own record, then entries the team shares.
fn signin_candidates(kind: Kind, servers: Vec<ServerRecord>) -> Vec<ServerRecord> {
    let mut v: Vec<ServerRecord> = servers
        .into_iter()
        .filter(|s| s.auth == AuthKind::OAuth && same_server(kind, &s.url))
        .collect();
    let rank = |s: &ServerRecord| {
        let knowledge = s.catalog_id == catalog_id(kind);
        match (s.sharing, knowledge) {
            (Sharing::Personal, false) => 0,
            (_, true) => 1,
            _ => 2,
        }
    };
    v.sort_by_key(|s| (rank(s), s.id.clone()));
    v
}

/// Every record on `kind`'s remote MCP server: the team's servers (which
/// leave Knowledge's own connections out) and Knowledge's own record.
async fn records_for(rt: &Runtime, kind: Kind) -> Result<Vec<ServerRecord>, String> {
    let mut servers = rt.store.list_servers(&rt.team).await?;
    if let Some(own) = rt.store.get_server(&rt.team, &connection_id(kind)).await? {
        servers.retain(|s| s.id != own.id);
        servers.push(own);
    }
    Ok(signin_candidates(kind, servers))
}

/// The owner a member's connection has on `s`.
fn owner_on(s: &ServerRecord, email: &str) -> Owner {
    match s.sharing {
        Sharing::Personal => Owner::user(email),
        Sharing::Shared => Owner::Team,
    }
}

/// The working sign-in `email` can sync `kind` with, if any.
pub async fn find_signin(
    rt: &Runtime,
    kind: Kind,
    email: &str,
) -> Result<Option<SignedIn>, ConnectorError> {
    if signin_url(kind).is_none() || email.trim().is_empty() {
        return Ok(None);
    }
    let servers = records_for(rt, kind)
        .await
        .map_err(ConnectorError::Temporary)?;
    for s in servers {
        let owner = owner_on(&s, email);
        if let Ok(Some(t)) = rt.store.get_token(&rt.team, &s.id, &owner).await {
            if t.status == Status::Connected {
                return Ok(Some(SignedIn {
                    connected_by: t.connected_by.clone(),
                    server: s,
                    owner,
                }));
            }
        }
    }
    Ok(None)
}

/// Where `email` signs in to `kind` for Knowledge: the team's Connected apps
/// entry for the provider when it signs each person in (so the one sign-in
/// serves agents too), else Knowledge's own record, made on first use by
/// asking the server how it signs in. The caller starts the sign-in on it.
pub async fn signin_target(rt: &Runtime, kind: Kind) -> Result<ServerRecord, String> {
    let url = signin_url(kind).ok_or("This kind of source doesn't sign in this way.")?;
    let candidates = records_for(rt, kind).await?;
    if let Some(s) = candidates
        .iter()
        .find(|s| s.sharing == Sharing::Personal && s.oauth.is_some())
    {
        return Ok(s.clone());
    }
    let id = connection_id(kind);
    if let Some(s) = candidates
        .into_iter()
        .find(|s| s.id == id && s.oauth.is_some())
    {
        return Ok(s);
    }
    let plan = match crate::mcp_remote::runtime::detect(rt.http.as_ref(), url).await? {
        Detected::OAuth(plan) => plan,
        _ => {
            return Err(format!(
                "{}'s sign-in server didn't answer as expected. Try again in a few minutes.",
                kind.label()
            ))
        }
    };
    let mut rec = ServerRecord::new(&id, &format!("{} (knowledge)", kind.label()), url);
    rec.catalog_id = catalog_id(kind);
    rec.auth = AuthKind::OAuth;
    rec.sharing = Sharing::Personal;
    rec.oauth = Some(OAuthSettings::from_plan(&plan));
    rec.added_at = chrono::Utc::now().to_rfc3339();
    rt.store.put_server(&rt.team, &rec).await?;
    Ok(rec)
}

/// The Sanity connector reading through `server` with `owner`'s sign-in.
pub fn sanity_signin(rt: Runtime, server: ServerRecord, owner: Owner) -> SanityMcp {
    SanityMcp {
        tools: Arc::new(RuntimeTools::new(rt, server, owner, "Sanity")),
        now: chrono::Utc::now().timestamp(),
    }
}

/// Opens a saved source: through its member's sign-in when it has one,
/// else with the team's connection for its kind.
pub async fn open_source<'a>(env: &'a Env, src: &Source) -> Result<Opened<'a>, ConnectorError> {
    let kind = src
        .kind
        .ok_or_else(|| ConnectorError::Config("This source has no kind.".into()))?;
    let Some(si) = &src.signin else {
        return open(env, kind, &src.options).await;
    };
    let rt = env.runtime();
    let gone = || {
        ConnectorError::Reconnect(format!(
            "The {} sign-in this source used was removed.",
            kind.label()
        ))
    };
    let server = rt
        .store
        .get_server(&env.team, &si.server_id)
        .await
        .map_err(ConnectorError::Temporary)?
        .filter(|s| same_server(kind, &s.url))
        .ok_or_else(gone)?;
    let owner = Owner::parse(&si.owner).ok_or_else(gone)?;
    let connector: Box<dyn Connector + 'a> = match kind {
        Kind::Sanity => Box::new(sanity_signin(rt, server, owner)),
        _ => return Err(gone()),
    };
    Ok(Opened {
        connector,
        via: Via::SignIn,
        options: src.options.clone(),
    })
}

/// How a kind stands for this team, for the "Add source" cards.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ConnStatus {
    pub connected: bool,
    /// `oauth`, `sign_in`, `saved_key`, `github_app`, `public`, or empty.
    pub via: String,
    /// One click signs in (the install has the provider's OAuth app).
    pub can_sign_in: bool,
    /// A sign-in that needs reconnecting, with why.
    pub reconnect_reason: String,
    pub connected_by: String,
    /// What to do when it can't connect yet, in plain words.
    pub hint: String,
    /// The person asking can sign in with the provider's own account
    /// through its remote MCP server (no app or key to set up).
    pub member_sign_in: bool,
    /// They have such a sign-in now.
    pub signed_in: bool,
    /// The team also has an API token saved for it.
    pub has_saved_key: bool,
}

/// How `kind` stands for the team and for `member` (the person asking).
/// `app_configured` says whether the install has the kind's OAuth app.
pub async fn status(env: &Env, kind: Kind, app_configured: bool, member: &str) -> ConnStatus {
    let mut st = ConnStatus {
        can_sign_in: app_configured,
        ..Default::default()
    };
    match kind {
        Kind::Web => {
            st.connected = true;
            st.via = "public".into();
        }
        Kind::Github => {
            st.connected = env.github_token.is_some();
            st.via = if st.connected {
                "github_app".into()
            } else {
                String::new()
            };
            if !st.connected {
                st.hint =
                    "Install the Coderhelm GitHub App first (Settings, then Integrations).".into();
            }
        }
        Kind::Sanity => {
            st.member_sign_in = true;
            st.can_sign_in = true;
            st.has_saved_key = env
                .plugin_credentials("sanity")
                .await
                .is_some_and(|c| c.get("api_token").is_some_and(|t| !t.is_empty()));
            if let Ok(Some(si)) = find_signin(&env.runtime(), kind, member).await {
                st.signed_in = true;
                st.connected = true;
                st.via = "sign_in".into();
                st.connected_by = si.connected_by;
            } else if st.has_saved_key {
                st.connected = true;
                st.via = "saved_key".into();
            }
        }
        Kind::Notion | Kind::Confluence | Kind::GoogleDrive => {
            let (signed_in, reason, by) = env.oauth_state(kind).await;
            st.connected_by = by;
            if signed_in {
                st.connected = true;
                st.via = "oauth".into();
            } else {
                st.reconnect_reason = reason;
                let saved = match kind {
                    Kind::Notion => env
                        .plugin_credentials("notion")
                        .await
                        .is_some_and(|c| c.get("api_key").is_some_and(|k| !k.is_empty())),
                    Kind::Confluence => env.confluence_token().await.is_some(),
                    _ => false,
                };
                if saved {
                    st.connected = true;
                    st.via = "saved_key".into();
                } else if !app_configured {
                    st.hint = match kind {
                        Kind::Notion => "Add a Notion integration token on the Integrations page, or ask whoever runs Coderhelm to set up Notion sign-in.".into(),
                        Kind::Confluence => "Save a Confluence email and API token on the Integrations page, or ask whoever runs Coderhelm to set up Atlassian sign-in.".into(),
                        _ => "Ask whoever runs Coderhelm to set up Google sign-in for Drive.".into(),
                    };
                }
            }
        }
    }
    st
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use crate::mcp_remote::http::Method;
    use crate::mcp_remote::runtime::testing::{sign_in, FakeKms};
    use crate::mcp_remote::store::fake::MemStore;

    const TOKEN: &str = "https://mcp.sanity.io/token";

    fn rt(http: Arc<Fake>) -> Runtime {
        Runtime {
            store: Arc::new(MemStore::default()),
            cipher: Arc::new(FakeKms),
            http,
            team: "TEAM#a".into(),
            clock: Arc::new(|| 1_000_000),
        }
    }

    fn sanity_record(id: &str, catalog: &str, sharing: Sharing) -> ServerRecord {
        let mut s = ServerRecord::new(id, "Sanity", sanity_mcp::MCP_URL);
        s.catalog_id = catalog.into();
        s.auth = AuthKind::OAuth;
        s.sharing = sharing;
        s.oauth = Some(OAuthSettings {
            issuer: "https://mcp.sanity.io".into(),
            authorization_endpoint: "https://mcp.sanity.io/authorize".into(),
            token_endpoint: TOKEN.into(),
            registration_endpoint: Some("https://mcp.sanity.io/register".into()),
            resource: "https://mcp.sanity.io".into(),
            client_id: "dyn-client".into(),
            registration: "dynamic".into(),
            ..Default::default()
        });
        s
    }

    /// A fake hosted Sanity MCP server speaking JSON-RPC: lists its tools and
    /// answers `query_documents` from `docs`, only for `bearer`.
    fn sanity_server(http: &Fake, bearer: &'static str, docs: Value) {
        http.with(move |r| {
            if r.url != sanity_mcp::MCP_URL {
                return None;
            }
            if r.header_value("Authorization") != Some(bearer) {
                return Some(fjson(401, json!({"error": "invalid_token"})));
            }
            let body: Value = serde_json::from_slice(r.body.as_ref()?).ok()?;
            let result = match body["method"].as_str()? {
                "tools/list" => json!({"tools": [
                    {"name": "list_projects", "inputSchema": {"type": "object"}},
                    {"name": "list_datasets", "inputSchema": {"type": "object", "properties": {"projectId": {"type": "string"}}}},
                    {"name": "query_documents", "inputSchema": {"type": "object", "properties": {
                        "projectId": {"type": "string"}, "dataset": {"type": "string"},
                        "query": {"type": "string"}, "params": {"type": "object"}, "perspective": {"type": "string"}}}},
                    {"name": "whoami", "inputSchema": {"type": "object"}}
                ]}),
                "tools/call" => {
                    let args = &body["params"]["arguments"];
                    assert_eq!(body["params"]["name"], "query_documents");
                    assert_eq!(args["projectId"], "abc123");
                    let text = format!("Query executed successfully: {}", json!({"chPage": docs}));
                    json!({"content": [{"type": "text", "text": text}]})
                }
                _ => return None,
            };
            Some(fjson(200, json!({"jsonrpc": "2.0", "id": 1, "result": result})))
        });
    }

    fn source() -> Source {
        Source {
            kind: Some(Kind::Sanity),
            selection: vec![super::super::Selected {
                id: "type:abc123:production:post".into(),
                title: "post".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_members_own_connected_apps_sign_in_is_found_first() {
        let http = Arc::new(Fake::default());
        let rt = rt(http);
        let ada = Owner::user("ada@example.com");
        for (id, cat, sh) in [
            ("sanity", "sanity", Sharing::Personal),
            ("ks-sanity", "source:sanity", Sharing::Personal),
            ("sanity-team", "", Sharing::Shared),
        ] {
            rt.store
                .put_server(&rt.team, &sanity_record(id, cat, sh))
                .await
                .unwrap();
        }
        let mut other = sanity_record("figma", "figma", Sharing::Personal);
        other.url = "https://mcp.figma.com/mcp".into();
        rt.store.put_server(&rt.team, &other).await.unwrap();
        sign_in(&rt, "figma", &ada, "f", None, None, "ada@example.com").await;
        assert!(find_signin(&rt, Kind::Sanity, "ada@example.com")
            .await
            .unwrap()
            .is_none());
        sign_in(&rt, "ks-sanity", &ada, "k", None, None, "ada@example.com").await;
        let si = find_signin(&rt, Kind::Sanity, "Ada@Example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(si.server.id, "ks-sanity");
        sign_in(&rt, "sanity", &ada, "c", None, None, "ada@example.com").await;
        let si = find_signin(&rt, Kind::Sanity, "ada@example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            si.server.id, "sanity",
            "the Connected apps sign-in is reused"
        );
        assert_eq!(si.owner, ada);
        // Someone else has no sign-in of their own, so the team's is used.
        assert!(find_signin(&rt, Kind::Sanity, "bo@example.com")
            .await
            .unwrap()
            .is_none());
        sign_in(
            &rt,
            "sanity-team",
            &Owner::Team,
            "t",
            None,
            None,
            "admin@example.com",
        )
        .await;
        let si = find_signin(&rt, Kind::Sanity, "bo@example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (si.server.id.as_str(), si.owner),
            ("sanity-team", Owner::Team)
        );
        // Notion and Confluence don't sync through their MCP servers.
        assert!(find_signin(&rt, Kind::Notion, "ada@example.com")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn a_source_syncs_through_the_mcp_server_with_the_members_token() {
        let http = Arc::new(Fake::default());
        let rt = rt(http.clone());
        let rec = sanity_record("ks-sanity", "source:sanity", Sharing::Personal);
        rt.store.put_server(&rt.team, &rec).await.unwrap();
        let ada = Owner::user("ada@example.com");
        sign_in(
            &rt,
            "ks-sanity",
            &ada,
            "at-1",
            Some("rt-1"),
            Some(1_000_000 + 3600),
            "ada@example.com",
        )
        .await;
        sanity_server(
            &http,
            "Bearer at-1",
            json!([{"_id": "p1", "_type": "post", "_rev": "r1", "_updatedAt": "2026-10-01T00:00:00Z", "title": "Returns"}]),
        );
        let c = sanity_signin(rt, rec, ada);
        let l = c.list(&source()).await.unwrap();
        assert_eq!(l.items.len(), 1);
        assert_eq!(l.items[0].version, "r1");
        assert!(l.complete);
        for sent in http.sent() {
            if sent.header_value("Authorization").is_some() {
                assert_eq!(
                    sent.url,
                    sanity_mcp::MCP_URL,
                    "the sign-in only goes to mcp.sanity.io"
                );
            }
        }
    }

    #[tokio::test]
    async fn an_expired_sign_in_stops_the_sync_with_sign_in_again() {
        let http = Arc::new(Fake::default());
        let rt = rt(http.clone());
        let rec = sanity_record("ks-sanity", "source:sanity", Sharing::Personal);
        rt.store.put_server(&rt.team, &rec).await.unwrap();
        let ada = Owner::user("ada@example.com");
        sign_in(
            &rt,
            "ks-sanity",
            &ada,
            "at-1",
            Some("rt-1"),
            Some(1_000_000 - 5),
            "ada@example.com",
        )
        .await;
        http.on(
            Method::Post,
            TOKEN,
            fjson(400, json!({"error": "invalid_grant"})),
        );
        let store = rt.store.clone();
        let c = sanity_signin(rt, rec, ada.clone());
        let e = c.list(&source()).await.unwrap_err();
        assert!(matches!(e, ConnectorError::Reconnect(_)), "{e:?}");
        let t = store
            .get_token("TEAM#a", "ks-sanity", &ada)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.status, Status::NeedsReconnect);
        let mut src = source();
        src.signin = Some(SignIn {
            email: "ada@example.com".into(),
            name: "Ada".into(),
            ..Default::default()
        });
        assert_eq!(super::super::source_error(&src, &e).action, "sign_in");
    }

    #[test]
    fn only_sanity_signs_in_through_its_mcp_server() {
        assert_eq!(signin_url(Kind::Sanity), Some("https://mcp.sanity.io"));
        for k in [
            Kind::Notion,
            Kind::Confluence,
            Kind::GoogleDrive,
            Kind::Github,
            Kind::Web,
        ] {
            assert!(signin_url(k).is_none());
        }
    }
}
