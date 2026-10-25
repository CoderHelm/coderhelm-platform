//! The one interface every source type implements.
//!
//! A connector answers three questions, and the sync engine
//! ([`super::sync`]) does everything else (diffing, hashing, embedding,
//! deleting, status):
//!
//! - [`Connector::browse`]: what can be picked, one level at a time, for the
//!   tree picker (with a search box);
//! - [`Connector::list`]: every item a selection covers now, each with the
//!   provider's version marker and no content, or, from a cursor, only what
//!   changed and what was deleted;
//! - [`Connector::fetch`]: one item as text, with its title, original
//!   address and last edit time.
//!
//! An item a connection can no longer see (`NotFound`, `Forbidden` on
//! fetch) is removed from the base, the same as a deleted one, so revoking
//! access in the provider removes the content here.
//!
//! ## Adding a source type (plugins)
//!
//! The trait is object safe and knows nothing about where a connector comes
//! from. A plugin adds a source type by shipping a connector that implements
//! these three calls (for example as three tools of its MCP server:
//! `browse`, `list`, `fetch`, returning the JSON shapes of [`Node`],
//! [`Listing`] and [`Document`]). The adapter that turns such a plugin into a
//! `Box<dyn Connector>` is the only new code: the picker, the sync engine,
//! scheduling, webhooks, status and citations all work unchanged. See the
//! Knowledge sources page of the developer docs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::chunk::Section;
use super::{Kind, Source, SourceError};
use crate::mcp_remote::http::{Http, HttpError, HttpRequest, HttpResponse};

/// One entry of the picker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub title: String,
    /// `page`, `database`, `space`, `folder`, `file`, `repo`, `path`,
    /// `type`, `dataset`, `url`, `sitemap`.
    pub kind: String,
    /// Opening it lists more under it.
    pub has_children: bool,
    /// It can be picked (a heading like "Datasets" can't).
    pub selectable: bool,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub updated_at: String,
    /// A short note under the title ("Database", "12 documents").
    #[serde(default)]
    pub detail: String,
}

/// One item a selection covers, without its content.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Listed {
    pub external_id: String,
    pub title: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub updated_at: String,
    /// The provider's version marker (edit time, revision, blob sha). Empty
    /// when the provider has none: the item is fetched and its text hash
    /// decides.
    #[serde(default)]
    pub version: String,
    /// Where it sits ("Engineering / Runbooks").
    #[serde(default)]
    pub path: String,
    /// What the connector needs again to fetch it (a MIME type, a type name).
    #[serde(default)]
    pub hint: Value,
}

/// What a selection covers now.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Listing {
    pub items: Vec<Listed>,
    /// Ids the provider says were deleted (incremental listings).
    #[serde(default)]
    pub deleted: Vec<String>,
    /// `items` is everything the selection covers: anything stored and not
    /// in it was deleted or moved out.
    pub complete: bool,
    /// Where the next listing starts (opaque, the connector's own).
    #[serde(default)]
    pub cursor: String,
    /// More items exist than a source keeps.
    #[serde(default)]
    pub truncated: bool,
}

/// One item's content.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Document {
    pub title: String,
    pub url: String,
    /// When it was last edited (RFC 3339 or a date), for "as of".
    pub updated_at: String,
    pub sections: Vec<Section>,
}

impl Document {
    pub fn is_empty(&self) -> bool {
        self.sections.iter().all(|s| s.text.trim().is_empty())
    }
}

/// Why a connector call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorError {
    /// The sign-in is gone or was refused: only reconnecting fixes it.
    Reconnect(String),
    /// The connection can't see this.
    Forbidden(String),
    NotFound(String),
    /// The provider asked us to slow down.
    RateLimited(String),
    /// The source's settings are wrong (a dataset that doesn't exist).
    Config(String),
    /// Anything else; the next sync tries again.
    Temporary(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorError::Reconnect(m)
            | ConnectorError::Forbidden(m)
            | ConnectorError::NotFound(m)
            | ConnectorError::RateLimited(m)
            | ConnectorError::Config(m)
            | ConnectorError::Temporary(m) => f.write_str(m),
        }
    }
}

impl ConnectorError {
    /// On fetching one item: the item is gone for this connection, so its
    /// content is removed.
    pub fn item_gone(&self) -> bool {
        matches!(
            self,
            ConnectorError::NotFound(_) | ConnectorError::Forbidden(_)
        )
    }

    /// The error a source shows, with the fix and its button.
    pub fn for_source(&self, kind: Kind) -> SourceError {
        let name = kind.label();
        let (fix, action) = match self {
            ConnectorError::Reconnect(_) => (format!("Reconnect {name} to sign in again."), "reconnect"),
            ConnectorError::Forbidden(_) => (
                format!("Give the {name} connection access to what you picked, or reconnect with an account that has it."),
                "reconnect",
            ),
            ConnectorError::NotFound(_) => (
                "What you picked was moved or deleted. Edit the source and pick it again.".to_string(),
                "edit",
            ),
            ConnectorError::Config(_) => ("Edit the source to fix its settings.".to_string(), "edit"),
            ConnectorError::RateLimited(_) => (
                format!("{name} asked us to slow down. The next sync picks up where this one stopped."),
                "retry",
            ),
            ConnectorError::Temporary(_) => ("Sync again in a few minutes.".to_string(), "retry"),
        };
        SourceError {
            message: self.to_string(),
            fix,
            action: action.into(),
            at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

/// A source type.
#[async_trait::async_trait]
pub trait Connector: Send + Sync {
    fn kind(&self) -> Kind;

    /// What can be picked under `parent` (`None`: the top level), narrowed
    /// by `query` when it isn't empty.
    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError>;

    /// Every item `source`'s selection covers now (or, from its cursor, what
    /// changed and what was deleted).
    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError>;

    /// One item's content.
    async fn fetch(&self, source: &Source, item: &Listed) -> Result<Document, ConnectorError>;
}

// ── HTTP shared by the connectors ───────────────────────────────────────────

/// Retries a rate-limited request this many times before giving up.
const MAX_RETRIES: usize = 3;
/// Longest wait between retries.
const MAX_WAIT_SECS: u64 = 10;

/// Sends `req`; a 429 or 503 is retried after the provider's
/// `Retry-After` (capped). Anything outside 2xx becomes a
/// [`ConnectorError`] named for `provider`.
pub async fn send(
    http: &dyn Http,
    req: HttpRequest,
    provider: &str,
) -> Result<HttpResponse, ConnectorError> {
    let mut attempt = 0;
    loop {
        let resp = http.send(req.clone()).await.map_err(|e| match e {
            HttpError::Refused(m) => ConnectorError::Config(m),
            HttpError::Unreachable(m) => ConnectorError::Temporary(m),
        })?;
        if matches!(resp.status, 429 | 503) && attempt < MAX_RETRIES {
            attempt += 1;
            let wait = resp
                .header("retry-after")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(attempt as u64)
                .min(MAX_WAIT_SECS);
            tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
            continue;
        }
        return check(resp, provider);
    }
}

/// [`send`] and the reply as JSON.
pub async fn send_json(
    http: &dyn Http,
    req: HttpRequest,
    provider: &str,
) -> Result<Value, ConnectorError> {
    let r = send(http, req, provider).await?;
    r.json().ok_or_else(|| {
        ConnectorError::Temporary(format!(
            "{provider} answered with something that isn't JSON."
        ))
    })
}

/// A reply's status as a result.
pub fn check(resp: HttpResponse, provider: &str) -> Result<HttpResponse, ConnectorError> {
    match resp.status {
        200..=299 => Ok(resp),
        401 => Err(ConnectorError::Reconnect(format!(
            "{provider} didn't accept the saved sign-in."
        ))),
        403 => Err(ConnectorError::Forbidden(format!(
            "The {provider} connection can't see this."
        ))),
        404 | 410 => Err(ConnectorError::NotFound(format!(
            "{provider} has no such page, or the connection can't see it."
        ))),
        429 => Err(ConnectorError::RateLimited(format!(
            "{provider} is limiting how fast we read."
        ))),
        400 | 422 => Err(ConnectorError::Config(format!(
            "{provider} refused the request ({}).",
            crate::truncate_str(&provider_message(&resp), 200)
        ))),
        s => Err(ConnectorError::Temporary(format!(
            "{provider} answered {s}."
        ))),
    }
}

/// The provider's own error message, when its reply has one.
fn provider_message(r: &HttpResponse) -> String {
    let v = r.json().unwrap_or_default();
    v["message"]
        .as_str()
        .or(v["error"]["message"].as_str())
        .or(v["error"].as_str())
        .unwrap_or("")
        .to_string()
}

/// A query string value, percent-encoded.
pub fn enc(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Case-insensitive "contains", for narrowing a picker list by its search
/// box when the provider can't.
pub fn matches(query: &str, title: &str) -> bool {
    let q = query.trim().to_lowercase();
    q.is_empty() || title.to_lowercase().contains(&q)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::{json as fjson, resp, Fake};

    #[tokio::test]
    async fn statuses_become_plain_errors() {
        let http = Fake::default();
        for (status, want) in [
            (401, "Reconnect"),
            (403, "Forbidden"),
            (404, "NotFound"),
            (422, "Config"),
            (500, "Temporary"),
        ] {
            let url = format!("https://api.example.com/{status}");
            http.on(
                crate::mcp_remote::http::Method::Get,
                &url,
                fjson(status, serde_json::json!({"message": "nope"})),
            );
            let e = send(&http, HttpRequest::get(&url), "Example")
                .await
                .unwrap_err();
            assert!(format!("{e:?}").starts_with(want), "{status}: {e:?}");
        }
        let ok = "https://api.example.com/ok";
        http.on(
            crate::mcp_remote::http::Method::Get,
            ok,
            resp(200, &[], "{\"a\":1}"),
        );
        assert_eq!(
            send_json(&http, HttpRequest::get(ok), "Example")
                .await
                .unwrap()["a"],
            1
        );
    }

    #[tokio::test]
    async fn a_rate_limit_is_retried() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let http = Fake::default();
        let n = std::sync::Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        http.with(move |_| {
            Some(if n2.fetch_add(1, Ordering::SeqCst) == 0 {
                resp(429, &[("retry-after", "0")], "")
            } else {
                resp(200, &[], "{}")
            })
        });
        send(
            &http,
            HttpRequest::get("https://api.example.com/x"),
            "Example",
        )
        .await
        .unwrap();
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn errors_name_their_fix() {
        let e = ConnectorError::Reconnect("gone".into()).for_source(Kind::Notion);
        assert_eq!(e.fix, "Reconnect Notion to sign in again.");
        assert_eq!(e.action, "reconnect");
        assert!(ConnectorError::Forbidden("x".into()).item_gone());
        assert!(!ConnectorError::Temporary("x".into()).item_gone());
        assert_eq!(
            ConnectorError::NotFound("x".into())
                .for_source(Kind::Confluence)
                .action,
            "edit"
        );
    }
}
