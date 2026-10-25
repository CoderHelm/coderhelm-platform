//! Remote MCP servers (signed in with OAuth, or a key) for plugins.
//!
//! The Connections page's remote servers (`crate::mcp_remote`) serve
//! plugins two ways:
//!
//! - `ref: mcp:<server id>` in a manifest: the run lists and calls that
//!   server's allowed and approved tools through the remote runtime, which
//!   refreshes tokens and writes every call to the server's audit trail.
//! - `conn:<server id>` as a connection setting: an HTTP tool's request is
//!   signed with that server's bearer token, and only when the tool's URL is
//!   on the server's own host, so a token is never sent anywhere else.
//!
//! [`Layered`] puts these in front of the catalog servers that run through
//! the MCP proxy.

use serde_json::Value;

use super::connect::{Connections, RemoteTokens, ToolDef};
use crate::mcp_remote::runtime::Runtime;
use crate::mcp_remote::store::{Owner, ServerRecord};
use crate::mcp_remote::tools;

/// What `tools` and `call` say when the id isn't a remote server.
pub const NOT_REMOTE: &str = "not a remote server";

/// One team's remote servers, used for a plugin run.
pub struct RemoteMcp {
    pub rt: Runtime,
    /// The person the run is for, when known (their own connections to
    /// personal servers).
    pub user: Option<String>,
    /// Who the server's audit trail names (`plugin:<id>, run <run id>`).
    pub actor: String,
}

impl RemoteMcp {
    async fn server(&self, team: &str, id: &str) -> Result<(ServerRecord, Owner), String> {
        if team != self.rt.team {
            return Err(format!("{id}: {NOT_REMOTE}"));
        }
        self.rt
            .usable(self.user.as_deref())
            .await
            .into_iter()
            .find(|(s, _)| s.id == id)
            .ok_or_else(|| format!("{id}: {NOT_REMOTE}"))
    }
}

fn host(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
}

#[async_trait::async_trait]
impl Connections for RemoteMcp {
    async fn tools(&self, team: &str, server: &str) -> Result<Vec<ToolDef>, String> {
        let (s, _) = self.server(team, server).await?;
        Ok(tools::effective(&s)
            .into_iter()
            .map(|t| ToolDef {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.input_schema.clone(),
            })
            .collect())
    }

    async fn call(
        &self,
        team: &str,
        server: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String> {
        let (s, owner) = self.server(team, server).await?;
        let out = self
            .rt
            .call_tool(&s, &owner, tool, args, &self.actor)
            .await?;
        if out.is_error {
            Err(format!(
                "{}: {}",
                s.name,
                crate::truncate_str(&out.text, 500)
            ))
        } else {
            Ok(out.text)
        }
    }
}

#[async_trait::async_trait]
impl RemoteTokens for RemoteMcp {
    async fn bearer(&self, team: &str, connection_id: &str, url: &str) -> Result<String, String> {
        let (s, owner) = self.server(team, connection_id).await.map_err(|_| {
            format!("The {connection_id} connection isn't connected on the Connections page.")
        })?;
        match (host(url), host(&s.url)) {
            (Some(a), Some(b)) if a == b => {}
            _ => {
                return Err(format!(
                    "{}'s token is only sent to {}; this tool calls another address.",
                    s.name,
                    host(&s.url).unwrap_or_default()
                ))
            }
        }
        match self.rt.auth_header(&s, &owner, false).await {
            Ok(Some(h)) if h.0.eq_ignore_ascii_case("authorization") => {
                let v = h.1.trim();
                Ok(v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
                    .unwrap_or(v)
                    .to_string())
            }
            Ok(Some(_)) => Err(format!(
                "{} signs in with a key header, not a bearer token.",
                s.name
            )),
            Ok(None) => Err(format!("{} has no sign-in to send.", s.name)),
            Err(e) => Err(format!("{}: {e}", s.name)),
        }
    }
}

/// Remote servers first, then the catalog servers behind them.
pub struct Layered<'a> {
    pub remote: &'a dyn Connections,
    pub local: &'a dyn Connections,
}

#[async_trait::async_trait]
impl Connections for Layered<'_> {
    async fn tools(&self, team: &str, server: &str) -> Result<Vec<ToolDef>, String> {
        match self.remote.tools(team, server).await {
            Err(e) if e.ends_with(NOT_REMOTE) => self.local.tools(team, server).await,
            other => other,
        }
    }
    async fn call(
        &self,
        team: &str,
        server: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String> {
        match self.remote.call(team, server, tool, args).await {
            Err(e) if e.ends_with(NOT_REMOTE) => self.local.call(team, server, tool, args).await,
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::client::RemoteTool;
    use crate::mcp_remote::http::fake::Fake;
    use crate::mcp_remote::store::fake::MemStore;
    use crate::mcp_remote::store::{AuthKind, Status};
    use crate::plugins::connect::tests::{FakeConnections, XorCipher};
    use crate::plugins::connect::{bearer_for, Https};
    use std::sync::Arc;

    async fn rig(team: &str) -> Runtime {
        let rt = Runtime {
            store: Arc::new(MemStore::default()),
            cipher: Arc::new(XorCipher),
            http: Arc::new(Fake::default()),
            team: team.into(),
            clock: Arc::new(|| 1_000_000),
        };
        let mut s = ServerRecord::new("social", "Social", "https://api.social.example.com/mcp");
        s.auth = AuthKind::Header;
        s.header_name = "Authorization".into();
        s.header_sealed = rt.seal_header("social", "Bearer tok-123").await.unwrap();
        s.status = Status::Connected;
        s.tools = vec![RemoteTool {
            name: "get_stats".into(),
            title: None,
            description: "Read post stats".into(),
            input_schema: serde_json::json!({"type": "object"}),
            read_only: Some(true),
            destructive: None,
        }];
        s.allowed.insert("get_stats".into());
        s.approved.insert("get_stats".into());
        rt.store.put_server(team, &s).await.unwrap();
        rt
    }

    #[tokio::test]
    async fn a_conn_setting_signs_only_requests_to_its_own_server() {
        let r = RemoteMcp {
            rt: rig("TEAM#a").await,
            user: None,
            actor: "plugin:marketing".into(),
        };
        // `conn:<id>` resolves through the remote runtime.
        let t = bearer_for(
            &r,
            "TEAM#a",
            "conn:social",
            "https://api.social.example.com/rest/posts",
        )
        .await;
        assert_eq!(t.unwrap(), "tok-123");
        // Never to another host.
        let e = bearer_for(&r, "TEAM#a", "conn:social", "https://evil.example.net/x")
            .await
            .unwrap_err();
        assert!(e.contains("only sent to"), "{e}");
        // Not a connected server.
        assert!(bearer_for(
            &r,
            "TEAM#a",
            "conn:nope",
            "https://api.social.example.com/x"
        )
        .await
        .is_err());
        // Another team's runtime never reaches it.
        assert!(bearer_for(
            &r,
            "TEAM#b",
            "conn:social",
            "https://api.social.example.com/x"
        )
        .await
        .is_err());
        // A pasted key still works as before.
        assert_eq!(
            bearer_for(&r, "TEAM#a", "key-9", "https://x.io")
                .await
                .unwrap(),
            "key-9"
        );
        // The real sender takes this source of tokens.
        let http = reqwest::Client::new();
        let _sender = Https {
            http: &http,
            remote: &r,
        };
    }

    #[tokio::test]
    async fn remote_tools_come_first_and_catalog_servers_still_work() {
        let r = RemoteMcp {
            rt: rig("TEAM#a").await,
            user: None,
            actor: "plugin:marketing".into(),
        };
        let local = FakeConnections {
            tools: [("sanity".to_string(), vec!["get_document".to_string()])]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let both = Layered {
            remote: &r,
            local: &local,
        };
        let remote_tools = both.tools("TEAM#a", "social").await.unwrap();
        assert_eq!(remote_tools[0].name, "get_stats");
        let catalog_tools = both.tools("TEAM#a", "sanity").await.unwrap();
        assert_eq!(catalog_tools[0].name, "get_document");
        assert!(both
            .call("TEAM#a", "sanity", "get_document", &serde_json::json!({}))
            .await
            .unwrap()
            .contains("ok"));
    }
}
