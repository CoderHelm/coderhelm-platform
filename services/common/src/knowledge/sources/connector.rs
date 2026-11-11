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
    /// The provider's own icon: an emoji (a Notion page's), or an `https://`
    /// image address (a Drive file type, a Notion uploaded icon, a GitHub
    /// owner's avatar). Empty when the provider gives none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub icon: String,
    /// How many entries sit directly under it, when the provider's answer
    /// says so (unknown is `None`, not 0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_count: Option<u64>,
    /// How many items picking it (with everything inside) brings in, itself
    /// included, when known from the same answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_count: Option<u64>,
    /// Its size in bytes, for a file whose provider reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// The titles above it, top first, for a search result that can sit
    /// anywhere ("Engineering", "Runbooks"). Empty at the top or when the
    /// provider doesn't say.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<String>,
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
    /// More items exist than the source keeps ([`Source::limit`]).
    #[serde(default)]
    pub truncated: bool,
    /// Items the selection covers, when the provider can count them (more
    /// than were listed when `truncated`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    /// With `complete`: external id prefixes this listing didn't cover in
    /// full (listed only from a cursor, or not read this time). Stored items
    /// under them stay unless named in `deleted`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partial: Vec<String>,
    /// Part of the selection couldn't be read while the rest was: the sync
    /// keeps what it read and shows this on the source.
    #[serde(skip)]
    pub problem: Option<ConnectorError>,
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
    /// The provider is overloaded, rate limiting, timed out or dropped the
    /// connection, and asking again later works. An item that hits this is
    /// never "couldn't read": it waits and is read again
    /// ([`super::sync`]).
    Busy(Busy),
}

/// A passing refusal: how long the provider asked to wait, and whether it
/// was a rate limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Busy {
    pub message: String,
    /// Seconds the provider asked to wait (`Retry-After`), when it said.
    pub retry_after: Option<u64>,
    /// A rate limit (`429`), as opposed to an overloaded or silent server.
    pub rate_limited: bool,
}

impl ConnectorError {
    /// A rate limit, with the wait the provider named.
    pub fn rate_limited(message: impl Into<String>, retry_after: Option<u64>) -> Self {
        ConnectorError::Busy(Busy {
            message: message.into(),
            retry_after,
            rate_limited: true,
        })
    }

    /// An overloaded server, a timeout or a dropped connection.
    pub fn unavailable(message: impl Into<String>, retry_after: Option<u64>) -> Self {
        ConnectorError::Busy(Busy {
            message: message.into(),
            retry_after,
            rate_limited: false,
        })
    }

    /// Passing: asking again later works (a rate limit, an overloaded or
    /// silent server).
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            ConnectorError::Busy(_) | ConnectorError::RateLimited(_)
        )
    }

    /// The reason word an item that hit this shows and is grouped by
    /// (`rate_limited`, `unavailable`).
    pub fn busy_reason(&self) -> Option<&'static str> {
        match self {
            ConnectorError::RateLimited(_) => Some("rate_limited"),
            ConnectorError::Busy(b) if b.rate_limited => Some("rate_limited"),
            ConnectorError::Busy(_) => Some("unavailable"),
            _ => None,
        }
    }
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorError::Reconnect(m)
            | ConnectorError::Forbidden(m)
            | ConnectorError::NotFound(m)
            | ConnectorError::RateLimited(m)
            | ConnectorError::Config(m)
            | ConnectorError::Temporary(m)
            | ConnectorError::Busy(Busy { message: m, .. }) => f.write_str(m),
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
            ConnectorError::Busy(b) if b.rate_limited => (
                format!("{name} asked us to slow down. The sync picks up where it stopped on its own."),
                "retry",
            ),
            ConnectorError::Busy(_) => (
                format!("{name} didn't answer in time. The sync tries again on its own."),
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

    /// A stored item's id as this connector names it now, when it was saved
    /// under an older form (`None`: unchanged). The sync renames the stored
    /// item before comparing, so it is neither fetched as new nor removed.
    fn current_id(&self, _source: &Source, _external_id: &str) -> Option<String> {
        None
    }
}

// ── HTTP shared by the connectors ───────────────────────────────────────────

/// Sends `req` through the provider's [`super::pace::Gate`] (one per
/// provider and API host): a few calls at a time at the provider's pace,
/// and a passing refusal (`429`, `500` and `502` to `504`, a quota `403`,
/// a timeout or a dropped connection) asked again after `Retry-After` or a
/// backoff with jitter. Anything else outside 2xx becomes a
/// [`ConnectorError`] named for `provider`; a refusal still passing after
/// the retries comes back as [`ConnectorError::Busy`] with the wait to keep.
pub async fn send(
    http: &dyn Http,
    req: HttpRequest,
    provider: &str,
) -> Result<HttpResponse, ConnectorError> {
    let host = url::Url::parse(&req.url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default();
    let gate = super::pace::gate_for(
        &format!("http#{provider}#{host}"),
        &super::pace::Policy::for_provider(provider),
    );
    gate.run(provider, || {
        let req = req.clone();
        async move {
            let resp = http.send(req).await.map_err(|e| match e {
                HttpError::Refused(m) => ConnectorError::Config(m),
                HttpError::Unreachable(m) => ConnectorError::unavailable(
                    format!("{provider} didn't answer in time ({m})."),
                    None,
                ),
            })?;
            check(resp, provider)
        }
    })
    .await
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

/// A `403` that is a quota or rate limit, not a refusal of access: Google's
/// `rateLimitExceeded`, `userRateLimitExceeded` and `RESOURCE_EXHAUSTED`,
/// GitHub's secondary rate limit or spent quota, Atlassian's "rate limit".
fn quota_403(resp: &HttpResponse) -> bool {
    if resp.header("x-ratelimit-remaining") == Some("0") {
        return true;
    }
    let body = resp.text().to_lowercase();
    [
        "ratelimitexceeded",
        "userratelimitexceeded",
        "resource_exhausted",
        "quotaexceeded",
        "rate limit",
        "secondary rate limit",
    ]
    .iter()
    .any(|w| body.contains(w))
}

/// A reply's status as a result.
pub fn check(resp: HttpResponse, provider: &str) -> Result<HttpResponse, ConnectorError> {
    match resp.status {
        200..=299 => Ok(resp),
        401 => Err(ConnectorError::Reconnect(format!(
            "{provider} didn't accept the saved sign-in."
        ))),
        403 | 429 if resp.status == 429 || quota_403(&resp) => Err(ConnectorError::rate_limited(
            format!("{provider} is limiting how fast Coderhelm reads ({}).", resp.status),
            super::pace::retry_after_of(&resp),
        )),
        // Google exports a document up to 10 MB: final, and not about access.
        403 if resp.text().contains("exportSizeLimitExceeded") => Err(ConnectorError::Config(format!(
            "{provider} exports documents up to 10 MB and this one is larger. Split it into smaller documents."
        ))),
        403 => Err(ConnectorError::Forbidden(format!(
            "The {provider} connection can't see this."
        ))),
        404 | 410 => Err(ConnectorError::NotFound(format!(
            "{provider} has no such page, or the connection can't see it."
        ))),
        400 | 422 => Err(ConnectorError::Config(format!(
            "{provider} refused the request ({}).",
            crate::truncate_str(&provider_message(&resp), 200)
        ))),
        s @ (500 | 502..=504) => Err(ConnectorError::unavailable(
            format!("{provider} is busy right now ({s})."),
            super::pace::retry_after_of(&resp),
        )),
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

    #[tokio::test(start_paused = true)]
    async fn statuses_become_plain_errors() {
        let http = Fake::default();
        for (status, want) in [
            (401, "Reconnect"),
            (403, "Forbidden"),
            (404, "NotFound"),
            (422, "Config"),
            (500, "Busy"),
            (501, "Temporary"),
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

    /// An HTTP server answering from a script (the last answer repeats):
    /// `None` is a connection that timed out.
    struct Script {
        answers: std::sync::Mutex<Vec<Option<HttpResponse>>>,
        sent: std::sync::atomic::AtomicUsize,
    }

    impl Script {
        fn new(answers: Vec<Option<HttpResponse>>) -> Self {
            Script {
                answers: std::sync::Mutex::new(answers),
                sent: Default::default(),
            }
        }
        fn sent(&self) -> usize {
            self.sent.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Http for Script {
        async fn send(&self, _: HttpRequest) -> Result<HttpResponse, HttpError> {
            self.sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let mut a = self.answers.lock().unwrap();
            let next = if a.len() > 1 {
                a.remove(0)
            } else {
                a[0].clone()
            };
            next.ok_or_else(|| HttpError::Unreachable("timed out".into()))
        }
    }

    fn google_quota() -> HttpResponse {
        fjson(
            403,
            serde_json::json!({"error": {"code": 403, "message": "User rate limit exceeded.", "errors": [{"domain": "usageLimits", "reason": "userRateLimitExceeded"}]}}),
        )
    }

    /// Every source's HTTP calls share one retry path: each passing shape
    /// (a 429 with Retry-After, a 503, a timeout, Google's 403
    /// rateLimitExceeded) is asked again and the answer comes back; a
    /// real 403 is not asked again.
    #[tokio::test(start_paused = true)]
    async fn every_passing_refusal_is_asked_again_until_it_answers() {
        let ok = || Some(resp(200, &[], "{\"ok\": true}"));
        for (name, first, provider) in [
            (
                "429 with Retry-After",
                Some(resp(429, &[("retry-after", "3")], "")),
                "Notion",
            ),
            ("503", Some(resp(503, &[], "")), "Confluence"),
            ("timeout", None, "Google Drive"),
            (
                "Google 403 rateLimitExceeded",
                Some(google_quota()),
                "Google Drive",
            ),
            (
                "GitHub spent quota",
                Some(resp(
                    403,
                    &[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "5")],
                    "",
                )),
                "GitHub",
            ),
        ] {
            let http = Script::new(vec![first.clone(), first, ok()]);
            let started = tokio::time::Instant::now();
            let url = format!("https://api.example.com/{}", name.replace(' ', "-"));
            let v = send_json(&http, HttpRequest::get(&url), provider).await;
            assert_eq!(v.unwrap()["ok"], true, "{name}");
            assert_eq!(http.sent(), 3, "{name}");
            if name.starts_with("429") {
                assert!(
                    started.elapsed() >= std::time::Duration::from_secs(6),
                    "{name}: each Retry-After waited"
                );
            }
        }
        // A 403 that is about access is final at once.
        let http = Script::new(vec![Some(fjson(
            403,
            serde_json::json!({"message": "no access"}),
        ))]);
        let e = send(
            &http,
            HttpRequest::get("https://api.example.com/denied"),
            "Confluence",
        )
        .await
        .unwrap_err();
        assert!(matches!(e, ConnectorError::Forbidden(_)), "{e:?}");
        assert_eq!(http.sent(), 1);
    }

    /// Still refused after its tries: a passing error with the wait to keep,
    /// which a sync shows as "waiting to retry", never "couldn't read".
    #[tokio::test(start_paused = true)]
    async fn a_refusal_that_lasts_comes_back_busy_with_a_wait() {
        for (first, rate) in [
            (Some(google_quota()), true),
            (Some(resp(429, &[("retry-after", "2")], "")), true),
            (Some(resp(502, &[], "")), false),
            (None, false),
        ] {
            let http = Script::new(vec![first]);
            let e = send(
                &http,
                HttpRequest::get("https://api.example.com/x"),
                "Google Drive",
            )
            .await
            .unwrap_err();
            assert_eq!(http.sent(), super::super::pace::Policy::DEFAULT.retries + 1);
            match e {
                ConnectorError::Busy(b) => {
                    assert_eq!(b.rate_limited, rate, "{b:?}");
                    assert!(b.retry_after.is_some_and(|w| w >= 1));
                }
                other => panic!("{other:?}"),
            }
        }
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
