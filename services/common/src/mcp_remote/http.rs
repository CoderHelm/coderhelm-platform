//! HTTPS to a remote MCP server and its authorization server, behind a trait
//! so tests can see what would be sent without sending it.
//!
//! The real transport ([`GuardedHttps`]) only ever reaches public addresses:
//! every host is resolved first and refused if ANY answer is private,
//! loopback, link-local or a metadata address, and the connection is pinned
//! to the address that was checked (so the name cannot be re-pointed between
//! the check and the call). It never uses a proxy, refuses plain HTTP, times
//! out, and stops reading at a size cap. Redirects are followed only for GETs
//! (metadata documents), at most a few hops, and every hop is checked again
//! from scratch.

use std::time::Duration;

use crate::live::net::{parse_endpoint, resolve_public};

/// Most hops a metadata GET follows.
pub const MAX_REDIRECTS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Delete,
}

/// What a caller reads back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// Discovery documents, registration and token replies.
    Metadata,
    /// MCP replies (tool lists and tool results).
    Mcp,
    /// A knowledge source's page or file (a Drive PDF, a web page).
    Document,
}

impl Limit {
    pub fn max_bytes(self) -> usize {
        match self {
            Limit::Metadata => 256 * 1024,
            Limit::Mcp => 4 * 1024 * 1024,
            Limit::Document => crate::knowledge::MAX_FILE_BYTES as usize,
        }
    }
    pub fn timeout(self) -> Duration {
        match self {
            Limit::Metadata => Duration::from_secs(15),
            Limit::Mcp | Limit::Document => Duration::from_secs(60),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct HttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub limit: Limit,
}

// Written by hand: header values and bodies carry tokens and secrets, and a
// request must never print them.
impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl HttpRequest {
    pub fn get(url: &str) -> Self {
        Self {
            method: Method::Get,
            url: url.to_string(),
            headers: vec![("Accept".into(), "application/json".into())],
            body: None,
            limit: Limit::Metadata,
        }
    }

    /// A form POST (OAuth token, refresh and revocation requests).
    pub fn form(url: &str, pairs: &[(&str, &str)]) -> Self {
        let mut s = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in pairs {
            s.append_pair(k, v);
        }
        Self {
            method: Method::Post,
            url: url.to_string(),
            headers: vec![
                (
                    "Content-Type".into(),
                    "application/x-www-form-urlencoded".into(),
                ),
                ("Accept".into(), "application/json".into()),
            ],
            body: Some(s.finish().into_bytes()),
            limit: Limit::Metadata,
        }
    }

    pub fn json(url: &str, body: &serde_json::Value, limit: Limit) -> Self {
        Self {
            method: Method::Post,
            url: url.to_string(),
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: Some(serde_json::to_vec(body).unwrap_or_default()),
            limit,
        }
    }

    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }

    /// The value of a header this request carries (case-insensitive name).
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn body_text(&self) -> String {
        self.body
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    /// Names lowercased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl std::fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
    pub fn json(&self) -> Option<serde_json::Value> {
        serde_json::from_slice(&self.body).ok()
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    pub fn is_sse(&self) -> bool {
        self.header("content-type")
            .is_some_and(|c| c.to_ascii_lowercase().starts_with("text/event-stream"))
    }
}

/// Why a request was not answered. `Refused` means our own guard stopped it
/// before anything was sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpError {
    Refused(String),
    Unreachable(String),
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpError::Refused(m) | HttpError::Unreachable(m) => f.write_str(m),
        }
    }
}

#[async_trait::async_trait]
pub trait Http: Send + Sync {
    async fn send(&self, req: HttpRequest) -> Result<HttpResponse, HttpError>;
}

/// The real transport.
pub struct GuardedHttps;

const USER_AGENT: &str = "Coderhelm (MCP client)";
const SOURCE_USER_AGENT: &str = "Coderhelm knowledge sync (+https://coderhelm.com)";

/// Checks an address the way every hop is checked: HTTPS, no user info, not
/// a private or local host. Returns the parsed endpoint.
pub fn check_url(raw: &str) -> Result<crate::live::net::Endpoint, HttpError> {
    let ep = parse_endpoint(raw).map_err(|e| HttpError::Refused(e.message))?;
    if ep.url.fragment().is_some() {
        return Err(HttpError::Refused(
            "The address must not contain a # part.".into(),
        ));
    }
    Ok(ep)
}

#[async_trait::async_trait]
impl Http for GuardedHttps {
    async fn send(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mut url = req.url.clone();
        for hop in 0..=MAX_REDIRECTS {
            let ep = check_url(&url)?;
            let addr = resolve_public(&ep)
                .await
                .map_err(|e| HttpError::Refused(e.message))?;
            let client = reqwest::Client::builder()
                .https_only(true)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .resolve(&ep.host, addr)
                .timeout(req.limit.timeout())
                .user_agent(if req.limit == Limit::Document {
                    SOURCE_USER_AGENT
                } else {
                    USER_AGENT
                })
                .build()
                .map_err(|e| HttpError::Unreachable(reach_text(&ep.host, &e)))?;
            let mut r = match req.method {
                Method::Get => client.get(ep.url.clone()),
                Method::Post => client.post(ep.url.clone()),
                Method::Delete => client.delete(ep.url.clone()),
            };
            for (k, v) in &req.headers {
                r = r.header(k.as_str(), v.as_str());
            }
            if let Some(b) = &req.body {
                r = r.body(b.clone());
            }
            let mut resp = r
                .send()
                .await
                .map_err(|e| HttpError::Unreachable(reach_text(&ep.host, &e)))?;
            let status = resp.status().as_u16();
            if (300..400).contains(&status) {
                let next = resp
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|loc| ep.url.join(loc).ok());
                match next {
                    Some(n) if req.method == Method::Get && hop < MAX_REDIRECTS => {
                        url = n.to_string();
                        continue;
                    }
                    _ => {
                        return Err(HttpError::Refused(format!(
                            "{} sent us to another address, and we don't follow redirects there.",
                            ep.host
                        )))
                    }
                }
            }
            let headers: Vec<(String, String)> = resp
                .headers()
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str()
                        .ok()
                        .map(|v| (k.as_str().to_ascii_lowercase(), v.to_string()))
                })
                .collect();
            let sse = headers.iter().any(|(k, v)| {
                k == "content-type" && v.to_ascii_lowercase().starts_with("text/event-stream")
            });
            let cap = req.limit.max_bytes();
            let mut buf: Vec<u8> = Vec::new();
            loop {
                let chunk = match resp.chunk().await {
                    Ok(Some(c)) => c,
                    Ok(None) => break,
                    // A stream cut short after the answer arrived still has it.
                    Err(_) if sse && super::client::sse_has_response(&buf) => break,
                    Err(e) => return Err(HttpError::Unreachable(reach_text(&ep.host, &e))),
                };
                if buf.len() + chunk.len() > cap {
                    return Err(HttpError::Unreachable(format!(
                        "{} sent more than {} KB, so we stopped reading.",
                        ep.host,
                        cap / 1024
                    )));
                }
                buf.extend_from_slice(&chunk);
                // An event stream may stay open after the reply: stop at it.
                if sse && super::client::sse_has_response(&buf) {
                    break;
                }
            }
            return Ok(HttpResponse {
                status,
                headers,
                body: buf,
            });
        }
        Err(HttpError::Refused("Too many redirects.".into()))
    }
}

fn reach_text(host: &str, e: &reqwest::Error) -> String {
    let why = if e.is_timeout() {
        "it took too long to answer"
    } else if e.is_connect() {
        "the connection failed"
    } else {
        "the request failed"
    };
    format!("We couldn't reach {host} ({why}).")
}

/// A fake transport for tests: answers by URL, records what was sent.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::sync::Mutex;

    type Responder = Box<dyn Fn(&HttpRequest) -> Option<HttpResponse> + Send + Sync>;

    #[derive(Default)]
    pub struct Fake {
        pub sent: Mutex<Vec<HttpRequest>>,
        routes: Mutex<Vec<Responder>>,
    }

    pub fn resp(status: u16, headers: &[(&str, &str)], body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    pub fn json(status: u16, body: serde_json::Value) -> HttpResponse {
        resp(
            status,
            &[("content-type", "application/json")],
            &body.to_string(),
        )
    }

    impl Fake {
        /// Answers requests whose URL equals `url` (and method, when given).
        pub fn on(&self, method: Method, url: &str, r: HttpResponse) -> &Self {
            let url = url.to_string();
            self.routes.lock().unwrap().push(Box::new(move |req| {
                (req.method == method && req.url == url).then(|| r.clone())
            }));
            self
        }
        /// Answers with a function of the request (first match wins).
        pub fn with(
            &self,
            f: impl Fn(&HttpRequest) -> Option<HttpResponse> + Send + Sync + 'static,
        ) -> &Self {
            self.routes.lock().unwrap().push(Box::new(f));
            self
        }
        pub fn sent(&self) -> Vec<HttpRequest> {
            self.sent.lock().unwrap().clone()
        }
        pub fn sent_to(&self, url: &str) -> Vec<HttpRequest> {
            self.sent().into_iter().filter(|r| r.url == url).collect()
        }
    }

    #[async_trait::async_trait]
    impl Http for Fake {
        async fn send(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            check_url(&req.url)?;
            self.sent.lock().unwrap().push(req.clone());
            let routes = self.routes.lock().unwrap();
            for r in routes.iter() {
                if let Some(out) = r(&req) {
                    return Ok(out);
                }
            }
            Ok(resp(404, &[], "not found"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_never_prints_secrets() {
        let r = HttpRequest::form(
            "https://as.example.com/token",
            &[("refresh_token", "rt-secret")],
        )
        .header("Authorization", "Bearer at-secret");
        let s = format!("{r:?}");
        assert!(!s.contains("rt-secret") && !s.contains("at-secret"));
        let resp = HttpResponse {
            status: 200,
            headers: vec![],
            body: b"{\"access_token\":\"at-secret\"}".to_vec(),
        };
        assert!(!format!("{resp:?}").contains("at-secret"));
    }

    #[test]
    fn form_bodies_are_encoded() {
        let r = HttpRequest::form("https://as.example.com/t", &[("a", "x y&z")]);
        assert_eq!(r.body_text(), "a=x+y%26z");
    }

    #[test]
    fn every_hop_is_checked() {
        for bad in [
            "http://mcp.example.com/mcp",
            "https://localhost/mcp",
            "https://127.0.0.1/mcp",
            "https://169.254.169.254/latest",
            "https://10.0.0.8/mcp",
            "https://[::1]/mcp",
            "https://u:p@mcp.example.com/mcp",
            "https://mcp.example.com/mcp#frag",
            "https://db.internal/mcp",
        ] {
            assert!(
                matches!(check_url(bad), Err(HttpError::Refused(_))),
                "{bad}"
            );
        }
        assert!(check_url("https://mcp.example.com/mcp").is_ok());
    }
}
