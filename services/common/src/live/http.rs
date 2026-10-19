//! HTTPS calls to a fixed outside service (New Relic, Confluence), behind a
//! trait so tests can see exactly what would be sent without sending it.
//!
//! The real transport goes only to public addresses, over HTTPS, never
//! through a proxy, never following a redirect, with a timeout and a cap on
//! how much it reads back.

use std::time::Duration;

use serde_json::Value;

use super::net::{parse_endpoint, resolve_public};
use super::StepError;

const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const USER_AGENT: &str = "Coderhelm (read-only lookups)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    /// GraphQL sends its (checked, read-only) query in a POST body.
    Post,
    /// Only the Confluence append, after its own checks.
    Put,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct Request {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Value>,
}

// Written by hand so header values (keys) never reach a log.
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: String,
}

impl Reply {
    pub fn json(&self) -> Option<Value> {
        serde_json::from_str(&self.body).ok()
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, req: Request) -> Result<Reply, StepError>;
}

/// The real transport.
pub struct Https;

#[async_trait::async_trait]
impl Transport for Https {
    async fn send(&self, req: Request) -> Result<Reply, StepError> {
        let ep = parse_endpoint(&req.url)?;
        let addr = resolve_public(&ep).await?;
        let cant_reach = |e: reqwest::Error| {
            StepError::new(
                "reach",
                format!("We couldn't reach {} ({}).", ep.host, short(&e)),
                "Check the address and try again.",
            )
        };
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&ep.host, addr)
            .timeout(TIMEOUT)
            .user_agent(USER_AGENT)
            .build()
            .map_err(cant_reach)?;
        let mut r = match req.method {
            Method::Get => client.get(ep.url.clone()),
            Method::Post => client.post(ep.url.clone()),
            Method::Put => client.put(ep.url.clone()),
        };
        for (k, v) in &req.headers {
            r = r.header(k.as_str(), v.as_str());
        }
        if let Some(b) = &req.body {
            r = r.json(b);
        }
        let mut resp = r.send().await.map_err(cant_reach)?;
        let status = resp.status().as_u16();
        if (300..400).contains(&status) {
            return Err(StepError::new(
                "reach",
                "The service sent us somewhere else, and we don't follow redirects.",
                "Check the address.",
            ));
        }
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(cant_reach)? {
            if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
                break;
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(Reply {
            status,
            body: String::from_utf8_lossy(&buf).into_owned(),
        })
    }
}

fn short(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "it took too long to answer"
    } else if e.is_connect() {
        "the connection failed"
    } else {
        "the request failed"
    }
}

/// A start and end (unix seconds) from `since` (`2h`, `3d` or an RFC 3339
/// time) and `until` (an RFC 3339 time), defaulting to the last `default`
/// and capped at `max` long.
pub fn window(
    since: Option<&str>,
    until: Option<&str>,
    now: i64,
    default: Duration,
    max: Duration,
) -> Result<(i64, i64), StepError> {
    let refuse = |m: &str| {
        StepError::new(
            "read_only",
            m.to_string(),
            "Use since like 2h or 3d, or a time like 2026-01-31T14:00:00Z.",
        )
    };
    let parse_at = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s.trim())
            .ok()
            .map(|d| d.timestamp())
    };
    let end = match until.map(str::trim).filter(|s| !s.is_empty()) {
        None => now,
        Some(u) => parse_at(u).ok_or_else(|| refuse("That end time isn't a time."))?,
    };
    let start = match since.map(str::trim).filter(|s| !s.is_empty()) {
        None => end - default.as_secs() as i64,
        Some(s) => match super::logs::parse_ago(s) {
            Some(d) => end - d.as_secs() as i64,
            None => parse_at(s).ok_or_else(|| refuse("That start time isn't a time."))?,
        },
    };
    if start >= end {
        return Err(refuse("The start time is after the end time."));
    }
    if end - start > max.as_secs() as i64 {
        return Err(refuse(&format!(
            "That covers more than {} days.",
            max.as_secs() / 86_400
        )));
    }
    if end > now + 300 {
        return Err(refuse("That end time is in the future."));
    }
    Ok((start, end))
}

/// A fake transport for tests: records every request and answers from a list.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct Fake {
        pub sent: Mutex<Vec<Request>>,
        pub replies: Mutex<Vec<Reply>>,
    }

    impl Fake {
        pub fn answering(replies: Vec<(u16, &str)>) -> Self {
            Fake {
                sent: Mutex::new(Vec::new()),
                replies: Mutex::new(
                    replies
                        .into_iter()
                        .rev()
                        .map(|(status, body)| Reply {
                            status,
                            body: body.to_string(),
                        })
                        .collect(),
                ),
            }
        }
        pub fn sent(&self) -> Vec<Request> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Transport for Fake {
        async fn send(&self, req: Request) -> Result<Reply, StepError> {
            self.sent.lock().unwrap().push(req);
            self.replies.lock().unwrap().pop().ok_or_else(|| {
                StepError::new("reach", "no more fake replies", "add one to the test")
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: Duration = Duration::from_secs(3600);
    const D14: Duration = Duration::from_secs(14 * 86_400);

    #[test]
    fn a_window_defaults_and_caps() {
        let now = 1_800_000_000;
        assert_eq!(window(None, None, now, H, D14).unwrap(), (now - 3600, now));
        assert_eq!(
            window(Some("2h"), None, now, H, D14).unwrap(),
            (now - 7200, now)
        );
        assert!(window(Some("30d"), None, now, H, D14).is_err());
        assert!(window(Some("soon"), None, now, H, D14).is_err());
        let (s, e) = window(
            Some("2026-01-31T14:00:00Z"),
            Some("2026-01-31T15:00:00Z"),
            now,
            H,
            D14,
        )
        .unwrap();
        assert_eq!(e - s, 3600);
        assert!(window(
            Some("2026-01-31T15:00:00Z"),
            Some("2026-01-31T14:00:00Z"),
            now,
            H,
            D14
        )
        .is_err());
    }

    #[test]
    fn a_request_never_prints_its_headers() {
        let r = Request {
            method: Method::Get,
            url: "https://x.example.com".into(),
            headers: vec![("Authorization".into(), "secret-key".into())],
            body: None,
        };
        assert!(!format!("{r:?}").contains("secret-key"));
    }
}
