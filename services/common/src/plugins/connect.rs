//! The seam between plugins and connections.
//!
//! A plugin names the connections it needs in its manifest; the team
//! connects them separately (installing never grants access by itself).
//! Two kinds are used:
//!
//! - **MCP servers** (`ref: mcp:<server>`): the catalog servers on the
//!   Connections page, and remote MCP servers signed in with OAuth. Runs
//!   list and call their tools through [`Connections`], and only the tools
//!   the manifest allows ever reach the model.
//! - **HTTP tools** (`kind: http`): one request through a connection's
//!   token, sent by [`HttpSend`]. The token is opened for that request only;
//!   the model never sees it, and only the manifest's URL is ever called.
//!
//! A connection setting's value is either a remote connection's id
//! (`conn:<server id>`, from the Connections page's OAuth sign-in) or a key
//! an admin pasted (kept sealed). Remote connections come from the remote
//! MCP work (`mcp_remote`), which implements [`RemoteTokens`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A tool a connection offers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub input_schema: Value,
}

/// A team's MCP servers.
#[async_trait::async_trait]
pub trait Connections: Send + Sync {
    /// The server's tools (names without the server prefix).
    async fn tools(&self, team: &str, server: &str) -> Result<Vec<ToolDef>, String>;
    /// Calls one tool; returns its text.
    async fn call(
        &self,
        team: &str,
        server: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String>;
}

/// The response to an HTTP tool's request.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpReply {
    pub status: u16,
    pub body: String,
    /// Where the result can be seen (a `Location` header, or an `id`/`url`
    /// in the body), when there is one.
    pub url: Option<String>,
}

/// Sends one HTTP tool request with a connection's credential.
#[async_trait::async_trait]
pub trait HttpSend: Send + Sync {
    /// `credential` is the opened connection setting (a `conn:` id or a key).
    async fn send(
        &self,
        team: &str,
        credential: &str,
        method: &str,
        url: &str,
        headers: &std::collections::BTreeMap<String, String>,
        body: &Value,
    ) -> Result<HttpReply, String>;
}

/// Bearer tokens for remote (OAuth) connections, by connection id. The
/// remote MCP work implements this over its token store; a token is opened
/// for one request and sent only to the host it was issued for.
#[async_trait::async_trait]
pub trait RemoteTokens: Send + Sync {
    async fn bearer(&self, team: &str, connection_id: &str, url: &str) -> Result<String, String>;
}

/// No remote connections (until the OAuth connections are deployed).
pub struct NoRemote;

#[async_trait::async_trait]
impl RemoteTokens for NoRemote {
    async fn bearer(&self, _team: &str, _id: &str, _url: &str) -> Result<String, String> {
        Err("OAuth connections aren't available yet. Paste a key in the plugin's settings, or connect the service on the Connections page once it offers sign-in.".into())
    }
}

/// The bearer token for a credential: a remote connection's, or the pasted
/// key itself.
pub async fn bearer_for(
    remote: &dyn RemoteTokens,
    team: &str,
    credential: &str,
    url: &str,
) -> Result<String, String> {
    match credential.strip_prefix("conn:") {
        Some(id) => remote.bearer(team, id, url).await,
        None if credential.is_empty() => Err("The connection isn't set.".into()),
        None => Ok(credential.to_string()),
    }
}

/// HTTPS to public addresses only, with a timeout and a size cap.
pub struct Https<'a> {
    pub http: &'a reqwest::Client,
    pub remote: &'a dyn RemoteTokens,
}

#[async_trait::async_trait]
impl HttpSend for Https<'_> {
    async fn send(
        &self,
        team: &str,
        credential: &str,
        method: &str,
        url: &str,
        headers: &std::collections::BTreeMap<String, String>,
        body: &Value,
    ) -> Result<HttpReply, String> {
        if !url.starts_with("https://") {
            return Err("Only https is called.".into());
        }
        // Public addresses only: a private, loopback or metadata address is
        // refused before anything is sent.
        let ep = crate::live::net::parse_endpoint(url).map_err(|e| e.to_string())?;
        crate::live::net::resolve_public(&ep)
            .await
            .map_err(|e| e.to_string())?;
        let token = bearer_for(self.remote, team, credential, url).await?;
        let m = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
        let mut req = self
            .http
            .request(m, url)
            .bearer_auth(token)
            .timeout(std::time::Duration::from_secs(20));
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if method != "GET" {
            req = req.json(body);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(200_000)]).to_string();
        let from_body = serde_json::from_str::<Value>(&text).ok().and_then(|v| {
            v.get("url")
                .or_else(|| v.get("permalink"))
                .or_else(|| v.get("id"))
                .and_then(|x| x.as_str().map(str::to_string))
        });
        Ok(HttpReply {
            status,
            body: text,
            url: location.or(from_body),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A cipher that only opens under the same context (tests).
    pub struct XorCipher;

    fn key(ctx: &HashMap<String, String>) -> Vec<u8> {
        let mut k: Vec<_> = ctx.iter().collect();
        k.sort();
        crate::content_hash(&format!("{k:?}")).into_bytes()
    }

    #[async_trait::async_trait]
    impl crate::mcp_credentials::Cipher for XorCipher {
        async fn encrypt(&self, ctx: HashMap<String, String>, p: &[u8]) -> Result<Vec<u8>, String> {
            let k = key(&ctx);
            let mut out: Vec<u8> = k[..8].to_vec();
            out.extend(p.iter().enumerate().map(|(i, b)| b ^ k[i % k.len()]));
            Ok(out)
        }
        async fn decrypt(&self, ctx: HashMap<String, String>, b: &[u8]) -> Result<Vec<u8>, String> {
            let k = key(&ctx);
            if b.len() < 8 || b[..8] != k[..8] {
                return Err("wrong context".into());
            }
            Ok(b[8..]
                .iter()
                .enumerate()
                .map(|(i, x)| x ^ k[i % k.len()])
                .collect())
        }
    }

    /// MCP servers in memory: tools per server, and every call made.
    #[derive(Default)]
    pub struct FakeConnections {
        pub tools: HashMap<String, Vec<String>>,
        pub calls: Mutex<Vec<(String, String, Value)>>,
    }

    #[async_trait::async_trait]
    impl Connections for FakeConnections {
        async fn tools(&self, _team: &str, server: &str) -> Result<Vec<ToolDef>, String> {
            Ok(self
                .tools
                .get(server)
                .map(|l| {
                    l.iter()
                        .map(|n| ToolDef {
                            name: n.clone(),
                            description: format!("{n} on {server}"),
                            input_schema: serde_json::json!({"type": "object"}),
                        })
                        .collect()
                })
                .unwrap_or_default())
        }
        async fn call(
            &self,
            _team: &str,
            server: &str,
            tool: &str,
            args: &Value,
        ) -> Result<String, String> {
            self.calls
                .lock()
                .unwrap()
                .push((server.into(), tool.into(), args.clone()));
            Ok(format!(
                "{{\"ok\": true, \"tool\": \"{tool}\", \"views\": 120}}"
            ))
        }
    }

    /// HTTP in memory: every request sent.
    #[derive(Default)]
    pub struct FakeHttp {
        pub sent: Mutex<Vec<(String, String, String, Value)>>,
    }

    #[async_trait::async_trait]
    impl HttpSend for FakeHttp {
        async fn send(
            &self,
            _team: &str,
            credential: &str,
            method: &str,
            url: &str,
            _headers: &std::collections::BTreeMap<String, String>,
            body: &Value,
        ) -> Result<HttpReply, String> {
            self.sent.lock().unwrap().push((
                credential.into(),
                method.into(),
                url.into(),
                body.clone(),
            ));
            Ok(HttpReply {
                status: 201,
                body: "{\"id\":\"urn:li:share:1\"}".into(),
                url: Some("https://example.com/posts/1".into()),
            })
        }
    }

    #[tokio::test]
    async fn credentials_are_a_connection_id_or_a_pasted_key() {
        assert_eq!(
            bearer_for(&NoRemote, "T", "key-1", "https://x")
                .await
                .unwrap(),
            "key-1"
        );
        assert!(bearer_for(&NoRemote, "T", "conn:linkedin", "https://x")
            .await
            .is_err());
        assert!(bearer_for(&NoRemote, "T", "", "https://x").await.is_err());
    }
}
