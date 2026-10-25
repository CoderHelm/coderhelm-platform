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
//! | Sanity | the Sanity token and project id saved on Integrations | |
//! | Google Drive | Google sign-in (OAuth) | |
//! | GitHub | the Coderhelm GitHub App | |
//! | Website | none needed | |
//!
//! A token is opened for the one call that needs it and never leaves the
//! server.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_json::{json, Value};

use super::connector::{Connector, ConnectorError};
use super::oauth::connection_id;
use super::{confluence, gdrive, github, notion, sanity, web, Kind};
use crate::live::connectors::{self as conns, ConnKind};
use crate::mcp_remote::http::Http;
use crate::mcp_remote::runtime::{ConnError, KmsCipher, Runtime};
use crate::mcp_remote::store::{Owner, Status};

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

/// How a kind stands for this team, for the "Add source" cards.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ConnStatus {
    pub connected: bool,
    /// `oauth`, `saved_key`, `github_app`, `public`, or empty.
    pub via: String,
    /// One click signs in (the install has the provider's OAuth app).
    pub can_sign_in: bool,
    /// A sign-in that needs reconnecting, with why.
    pub reconnect_reason: String,
    pub connected_by: String,
    /// What to do when it can't connect yet, in plain words.
    pub hint: String,
}

/// The status of every kind for the team. `apps` says which OAuth apps the
/// install has.
pub async fn status(env: &Env, kind: Kind, app_configured: bool) -> ConnStatus {
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
            let ok = env
                .plugin_credentials("sanity")
                .await
                .is_some_and(|c| c.get("api_token").is_some_and(|t| !t.is_empty()));
            st.connected = ok;
            st.via = if ok {
                "saved_key".into()
            } else {
                String::new()
            };
            if !ok {
                st.hint = "Add a Sanity API token (Viewer is enough) and the project id.".into();
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
