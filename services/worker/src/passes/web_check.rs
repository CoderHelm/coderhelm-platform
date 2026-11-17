//! `check_url`: agents check that a page on the team's OWN public sites
//! loads, without reading it. A reviewer that can't open a page can only ask
//! people to confirm a redirect target or a docs link works; with this it
//! checks and reports what came back.
//!
//! What a check returns: every hop's status (redirects followed, at most
//! [`MAX_REDIRECTS`]), the final URL, its content type and the page's
//! `<title>`. Asked to `find` terms, it also returns the text around each
//! match in the page's source, including the data the server rendered into
//! it (what a visitor's browser receives), so an agent can see the value a
//! page actually shows.
//!
//! Safety, enforced here and covered by the tests below:
//! - https only, on the default port, no credentials in the URL, a host name
//!   (never an IP literal);
//! - the host must be one of the team's own: a domain an admin listed in the
//!   team's review settings (that domain and its subdomains), a host named in
//!   a URL in the work at hand (the PR's diff, the ticket), a host that
//!   appears in one of the team's connected repositories, or a service's API
//!   host built from an id in those repositories' config (a CMS project id:
//!   see [`config_service`]);
//! - every address the host resolves to must be public (private, loopback,
//!   link-local, cloud metadata, carrier NAT, multicast and reserved ranges
//!   are refused) and the request is pinned to the address that was checked,
//!   so a second DNS answer can't swap it; each redirect is checked again
//!   from scratch (host and addresses), and one that leaves the team's hosts
//!   or https is reported, not followed;
//! - no cookies, no auth headers, no proxy; a fixed user agent; about
//!   [`TIMEOUT`] per URL; at most [`MAX_CHECKS`] URLs per run;
//! - every call, refused ones included, is written to the team's data-source
//!   activity log (reader, PR, URL, outcome), and a result that can't be
//!   logged is withheld.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::live::store::{self, Activity, RunRef};
use reqwest::Url;
use serde_json::{json, Value};
use tracing::warn;

use crate::agent::llm::ToolDefinition;
use crate::clients::github::GitHubClient;
use crate::WorkerState;

pub const TOOL: &str = "check_url";
/// URLs one run (a review, a plan, a coding run) may check.
pub const MAX_CHECKS: usize = 15;
/// Redirects followed per URL.
pub const MAX_REDIRECTS: usize = 5;
/// Time for one URL, all hops included.
pub const TIMEOUT: Duration = Duration::from_secs(8);
/// Bytes of an HTML page searched for its `<title>`.
const TITLE_SCAN_BYTES: usize = 64 * 1024;
/// Bytes of an HTML page read: enough for a server-rendered page's data.
const PAGE_SCAN_BYTES: usize = 4 * 1024 * 1024;
/// Terms one check may look for, and matches shown per term.
const MAX_FIND_TERMS: usize = 8;
const MAX_MATCHES: usize = 3;
/// Characters shown on each side of a match.
const MATCH_CONTEXT: usize = 300;
const MAX_URL_LEN: usize = 2048;
const USER_AGENT: &str = "Mozilla/5.0 (compatible; CoderHelm-LinkCheck/1.0)";
const SOURCE_ID: &str = "web-check";
const SOURCE_NAME: &str = "Public page check";
/// Settings record and attribute holding the team's own public domains.
pub const SETTINGS_SK: &str = "REVIEW_CONFIG#GLOBAL";
pub const SETTINGS_ATTR: &str = common::web_domains::ATTR;

/// Probes a page before end-to-end code is written against it.
pub const PROBE_TOOL: &str = "probe_page";

/// The coder's probe of a page another company serves (a payment form, a
/// sign-in widget), or of the preview: it loads the page with Playwright
/// in the sandbox and returns its test ids and the field names of the JSON
/// it fetches, as a record to commit (`common::e2e::probe`). Selectors and
/// fields no probe observed are a must-fix finding of the review.
pub fn probe_tool() -> ToolDefinition {
    ToolDefinition {
        name: PROBE_TOOL.to_string(),
        description: format!(
            "Load a page in a real browser (Playwright, in the sandbox) and list what is really \
             there: every test id (data-testid, data-test, data-cy, also inside frames) and the \
             field names of the JSON responses it fetches. Use it BEFORE writing end-to-end \
             selectors or reading JSON fields of a page another company serves (or a page on \
             the preview), and build the spec only from what it found. Commit the record it \
             returns at `{}`, and quote what it found in the pull request. Only hosts this run \
             may check are allowed.",
            common::e2e::probe::record_path("<host>")
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The https page to probe" }
            },
            "required": ["url"]
        }),
    }
}

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.to_string(),
        description: format!(
            "Check that pages on the team's OWN public sites load: for each https URL, the \
             status of every hop (redirects followed, up to {MAX_REDIRECTS}), the final URL, its \
             content type and the page title. With `find`, it also returns the text around each \
             match of those terms in the page's source, including the data the server rendered \
             into it: use it to see the value a page actually shows (an id, an address, a price) \
             before explaining why it's wrong. Only the team's \
             hosts can be checked (listed in your instructions, named in a URL in the work at \
             hand, used in the team's repositories, or a service API host built from an id in \
             their config, such as a CMS project's API); others are refused. A JSON or text \
             response is searched with `find` too, so a read-only API query (GET, no \
             credentials) can show the value it returns. Use it before \
             asking anyone to confirm a link, redirect target or page works: check it and \
             report what came back. At most {MAX_CHECKS} URLs per run."
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "urls": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "https URLs to check, e.g. every redirect target the change adds"
                },
                "find": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional: exact text to look for in each page's source, e.g. a record id from the URL and a field name. Up to 8 terms; up to 3 matches each, with the text around them."
                }
            },
            "required": ["urls"]
        }),
    }
}

// ─── Addresses ──────────────────────────────────────────────

fn v4_public(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(o[0] == 0                                     // this network
        || o[0] == 10                               // private
        || o[0] == 100 && (64..128).contains(&o[1]) // carrier NAT
        || o[0] == 127                              // loopback
        || o[0] == 169 && o[1] == 254               // link-local, cloud metadata
        || o[0] == 172 && (16..32).contains(&o[1])  // private
        || o[0] == 192 && o[1] == 0 && o[2] == 0    // protocol assignments
        || o[0] == 192 && o[1] == 0 && o[2] == 2    // documentation
        || o[0] == 192 && o[1] == 88 && o[2] == 99  // 6to4 relay
        || o[0] == 192 && o[1] == 168               // private
        || o[0] == 198 && (o[1] == 18 || o[1] == 19) // benchmarking
        || o[0] == 198 && o[1] == 51 && o[2] == 100 // documentation
        || o[0] == 203 && o[1] == 0 && o[2] == 113  // documentation
        || o[0] >= 224) // multicast, reserved, broadcast
}

/// A public, routable address: the only kind a check may connect to.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4_public(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4_public(v4);
            }
            let s = v6.segments();
            !(v6.is_unspecified()
                || v6.is_loopback()
                || s[0] & 0xfe00 == 0xfc00          // unique local (incl. cloud metadata fd00:ec2::254)
                || s[0] & 0xffc0 == 0xfe80          // link-local
                || s[0] & 0xffc0 == 0xfec0          // site-local (deprecated)
                || s[0] & 0xff00 == 0xff00          // multicast
                || s[0] == 0x0064 && s[1] == 0xff9b // NAT64: a v4 address inside
                || s[0] == 0x0100 && s[1..4] == [0, 0, 0] // discard
                || s[0] == 0x2001 && s[1] == 0x0db8 // documentation
                || s[0] == 0x2002                   // 6to4: a v4 address inside
                || s[0] == 0 && s[1..6] == [0, 0, 0, 0, 0]) // v4-compatible and other ::/80
        }
    }
}

// ─── URLs and hosts ─────────────────────────────────────────

/// A refusal, said to the agent, or a failure while checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckError {
    /// Never sent.
    Refused(String),
    /// Sent, went wrong (DNS, connect, timeout).
    Failed(String),
}

impl CheckError {
    pub fn text(&self) -> &str {
        match self {
            CheckError::Refused(s) | CheckError::Failed(s) => s,
        }
    }
}

fn refused(s: impl Into<String>) -> CheckError {
    CheckError::Refused(s.into())
}

/// The URL as a check may send it: https, default port, no credentials, a
/// host name. The host comes back lowercased.
pub fn vet_url(raw: &str) -> Result<(Url, String), CheckError> {
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > MAX_URL_LEN {
        return Err(refused("That URL is empty or too long."));
    }
    let url = Url::parse(raw).map_err(|_| refused(format!("{raw} isn't a valid URL.")))?;
    if url.scheme() != "https" {
        return Err(refused(format!("{raw}: only https URLs can be checked.")));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(refused(format!(
            "{raw}: URLs with credentials can't be checked."
        )));
    }
    if url.port().is_some_and(|p| p != 443) {
        return Err(refused(format!(
            "{raw}: only the default https port can be checked."
        )));
    }
    let host = match url.host() {
        Some(url::Host::Domain(d)) => d.trim_end_matches('.').to_ascii_lowercase(),
        Some(_) => {
            return Err(refused(format!(
                "{raw}: an IP address can't be checked, only a host name."
            )))
        }
        None => return Err(refused(format!("{raw} has no host."))),
    };
    if !host.contains('.') || host.ends_with(".local") || host.ends_with(".internal") {
        return Err(refused(format!("{raw}: {host} isn't a public host name.")));
    }
    Ok((url, host))
}

/// Host names of the http(s) URLs written in `text` (a diff, a ticket).
pub fn hosts_in(text: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    for scheme in ["https://", "http://"] {
        let mut rest = text;
        while let Some(i) = rest.find(scheme) {
            let tail = &rest[i..];
            let end = tail
                .find(|c: char| {
                    c.is_whitespace()
                        || matches!(
                            c,
                            '"' | '\''
                                | '`'
                                | '<'
                                | '>'
                                | '('
                                | ')'
                                | '['
                                | ']'
                                | '{'
                                | '}'
                                | ','
                                | '\\'
                        )
                })
                .unwrap_or(tail.len());
            if let Ok(u) = Url::parse(&tail[..end]) {
                if let Some(url::Host::Domain(d)) = u.host() {
                    if let Some(d) = common::web_domains::clean(d) {
                        out.insert(d);
                    }
                }
            }
            rest = &tail[scheme.len()..];
        }
    }
    out
}

/// The team's hosts known without a search: listed domains (and their
/// subdomains) and hosts named in the work at hand (exact).
#[derive(Debug, Clone, Default)]
pub struct HostPolicy {
    pub domains: Vec<String>,
    pub named: HashSet<String>,
}

impl HostPolicy {
    pub fn allows(&self, host: &str) -> bool {
        self.named.contains(host)
            || self
                .domains
                .iter()
                .any(|d| host == d || host.ends_with(&format!(".{d}")))
    }
}

/// Decides whether a host is one of the team's.
#[async_trait::async_trait]
pub trait HostGate: Send + Sync {
    async fn allowed(&self, host: &str) -> bool;
}

#[async_trait::async_trait]
impl HostGate for HostPolicy {
    async fn allowed(&self, host: &str) -> bool {
        self.allows(host)
    }
}

/// Services whose API host is built from an account id a repository keeps
/// in its config (`projectId` in a CMS config, an env file), so the host
/// itself never appears in the code: `(host suffixes, the service's name as
/// it appears next to the id)`. The host counts as the team's only when a
/// team repository holds that id next to the service's name.
const CONFIG_SERVICES: &[(&[&str], &str)] = &[
    (&[".api.sanity.io", ".apicdn.sanity.io"], "sanity"),
    (&[".supabase.co"], "supabase"),
];

/// The config id and service name behind a service API host
/// (`abc123de.api.sanity.io` gives `("abc123de", "sanity")`). `None` for
/// any other host, and for one whose first label isn't a plain id.
pub fn config_service(host: &str) -> Option<(String, &'static str)> {
    for (suffixes, service) in CONFIG_SERVICES {
        for suffix in *suffixes {
            let Some(id) = host.strip_suffix(suffix) else {
                continue;
            };
            let plain = (4..=40).contains(&id.len())
                && !id.starts_with('-')
                && !id.ends_with('-')
                && id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                && id.chars().any(|c| c.is_ascii_digit());
            if plain {
                return Some((id.to_string(), service));
            }
        }
    }
    None
}

/// The organization's code search: the repositories (`owner/name`) with a
/// file matching `query`. Behind a trait so the tests can script it.
#[async_trait::async_trait]
pub trait OrgSearch: Send + Sync {
    async fn repos_with(&self, org: &str, query: &str) -> Result<Vec<String>, String>;
}

#[async_trait::async_trait]
impl OrgSearch for GitHubClient {
    async fn repos_with(&self, org: &str, query: &str) -> Result<Vec<String>, String> {
        self.search_org_code(org, query)
            .await
            .map(|hits| hits.into_iter().map(|(repo, _)| repo).collect())
            .map_err(|e| e.to_string())
    }
}

// ─── Requests ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Head,
    Get,
}

/// One response as a check sees it.
#[derive(Debug, Clone, Default)]
pub struct Resp {
    pub status: u16,
    pub location: Option<String>,
    pub content_type: String,
    /// The body (GET of an HTML page only), up to [`PAGE_SCAN_BYTES`].
    pub body: Vec<u8>,
    /// Every response header, as sent (names lowercased).
    pub headers: Vec<(String, String)>,
}

/// How one request is sent beyond its method: the request headers a page
/// fetch carries (never credentials: [`common::coder_shell`] refuses them)
/// and whether any text body is read (scripts and styles too, not only
/// pages and JSON).
#[derive(Debug, Clone, Default)]
pub struct SendOpts {
    pub headers: Vec<(String, String)>,
    pub any_text: bool,
}

/// DNS and HTTP, behind a trait so the tests can script them.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String>;
    /// One request to `url`, connected to `addr` (already checked), no
    /// redirects followed.
    async fn send(&self, method: Method, url: &Url, addr: SocketAddr) -> Result<Resp, String>;
    /// [`Transport::send`] with request headers and the body rule of a
    /// page fetch.
    async fn send_with(
        &self,
        method: Method,
        url: &Url,
        addr: SocketAddr,
        _opts: &SendOpts,
    ) -> Result<Resp, String> {
        self.send(method, url, addr).await
    }
}

/// The real network.
pub struct Net;

#[async_trait::async_trait]
impl Transport for Net {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        tokio::net::lookup_host((host, 443))
            .await
            .map(|it| it.map(|a| a.ip()).collect())
            .map_err(|e| format!("{host} doesn't resolve ({e})"))
    }

    async fn send(&self, method: Method, url: &Url, addr: SocketAddr) -> Result<Resp, String> {
        self.send_with(method, url, addr, &SendOpts::default())
            .await
    }

    async fn send_with(
        &self,
        method: Method,
        url: &Url,
        addr: SocketAddr,
        opts: &SendOpts,
    ) -> Result<Resp, String> {
        let host = url.host_str().unwrap_or_default();
        // Pinned to the checked address; no redirects, cookies or proxy.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(TIMEOUT)
            .user_agent(USER_AGENT)
            .resolve(host, addr)
            .build()
            .map_err(|e| e.to_string())?;
        let req = match method {
            Method::Head => client.head(url.clone()),
            Method::Get => client.get(url.clone()),
        };
        let mut req = req.header("Accept", "text/html,*/*;q=0.8");
        for (k, v) in &opts.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let mut resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                "timed out".to_string()
            } else if e.is_connect() {
                "couldn't connect".to_string()
            } else {
                format!("request failed ({})", e.without_url())
            }
        })?;
        let header = |name: reqwest::header::HeaderName| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        };
        let location = header(reqwest::header::LOCATION);
        let content_type = header(reqwest::header::CONTENT_TYPE).unwrap_or_default();
        let status = resp.status().as_u16();
        let headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        let mut body = Vec::new();
        let wanted = if opts.any_text {
            is_text(&content_type)
        } else {
            is_readable(&content_type)
        };
        if method == Method::Get && wanted {
            while body.len() < PAGE_SCAN_BYTES {
                match resp.chunk().await {
                    Ok(Some(c)) => body.extend_from_slice(&c),
                    _ => break,
                }
            }
            body.truncate(PAGE_SCAN_BYTES);
        }
        Ok(Resp {
            status,
            location,
            content_type,
            body,
            headers,
        })
    }
}

fn is_html(content_type: &str) -> bool {
    let c = content_type.to_ascii_lowercase();
    c.starts_with("text/html") || c.starts_with("application/xhtml")
}

/// A body a page fetch shows: any text (a page, a script, a style, JSON,
/// XML). A type the server didn't name is read as text too.
fn is_text(content_type: &str) -> bool {
    let c = content_type.to_ascii_lowercase();
    c.is_empty()
        || is_readable(&c)
        || c.starts_with("text/")
        || c.contains("javascript")
        || c.contains("ecmascript")
        || c.contains("xml")
        || c.contains("css")
}

/// A body `find` can search: a page, JSON (an API's answer) or plain text.
fn is_readable(content_type: &str) -> bool {
    let c = content_type.to_ascii_lowercase();
    is_html(&c)
        || c.starts_with("application/json")
        || (c.starts_with("application/") && c.contains("+json"))
        || c.starts_with("text/plain")
}

/// The page's `<title>`, plain text, at most 200 characters.
pub fn page_title(html: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(&html[..html.len().min(TITLE_SCAN_BYTES)]);
    let lower = text.to_ascii_lowercase();
    let open = lower.find("<title")?;
    let start = open + lower[open..].find('>')? + 1;
    let end = start + lower[start..].find("</title")?;
    let raw = text.get(start..end)?;
    let decoded = raw
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#x27;", "'")
        .replace("&nbsp;", " ");
    let flat = decoded.split_whitespace().collect::<Vec<_>>().join(" ");
    (!flat.is_empty()).then(|| common::truncate_str(&flat, 200).to_string())
}

/// One hop of a check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hop {
    pub url: String,
    pub status: u16,
}

/// What one URL's check found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checked {
    pub hops: Vec<Hop>,
    pub final_url: String,
    pub content_type: String,
    pub title: Option<String>,
    /// Why the chain stopped before a final answer (a redirect not
    /// followed), if it did.
    pub stopped: Option<String>,
    /// For each term asked for: the text around its matches in the page.
    pub found: Vec<(String, Vec<String>)>,
}

/// The text around each match of `term` in `page`: up to [`MAX_MATCHES`],
/// [`MATCH_CONTEXT`] characters each side, on one line. Data a server
/// renders into a page is JSON inside a script, often escaped again; the
/// escaping is undone so the values read as they are.
pub fn find_in(page: &str, term: &str) -> Vec<String> {
    let plain = unescape_payload(page);
    let mut out = Vec::new();
    if term.is_empty() {
        return out;
    }
    let mut from = 0;
    while let Some(i) = plain[from..].find(term) {
        let at = from + i;
        let start = floor_char(&plain, at.saturating_sub(MATCH_CONTEXT));
        let end = floor_char(&plain, (at + term.len() + MATCH_CONTEXT).min(plain.len()));
        let flat = plain[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        out.push(format!(
            "{}{flat}{}",
            if start > 0 { "…" } else { "" },
            if end < plain.len() { "…" } else { "" }
        ));
        if out.len() == MAX_MATCHES {
            break;
        }
        from = end.max(at + term.len());
    }
    out
}

fn floor_char(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Undoes the string escaping around server-rendered JSON (`\"` and `\\"`
/// for quotes, `\u0026` and friends for markup characters).
fn unescape_payload(s: &str) -> String {
    s.replace("\\\"", "\"")
        .replace("\\\\", "\\")
        .replace("\\\"", "\"")
        .replace("\\u0026", "&")
        .replace("\\u003c", "<")
        .replace("\\u003e", ">")
        .replace("\\u0027", "'")
}

impl Checked {
    /// The last status seen.
    pub fn status(&self) -> Option<u16> {
        self.hops.last().map(|h| h.status)
    }

    /// One block of the tool's answer.
    pub fn render(&self, asked: &str) -> String {
        let mut s = format!("{asked}\n");
        for h in &self.hops {
            s.push_str(&format!("  {} {}\n", h.status, h.url));
        }
        if let Some(why) = &self.stopped {
            s.push_str(&format!("  stopped: {why}\n"));
        } else if let Some(st) = self.status() {
            s.push_str(&format!(
                "  final: {st} {}{}{}\n",
                self.final_url,
                if self.content_type.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", self.content_type)
                },
                self.title
                    .as_ref()
                    .map(|t| format!(" title: \"{t}\""))
                    .unwrap_or_default()
            ));
        }
        for (term, hits) in &self.found {
            if hits.is_empty() {
                s.push_str(&format!("  \"{term}\": not in the page\n"));
            }
            for h in hits {
                s.push_str(&format!("  \"{term}\": {h}\n"));
            }
        }
        s
    }

    /// One line for the activity log and the critic.
    pub fn line(&self) -> String {
        let chain: Vec<String> = self.hops.iter().map(|h| h.status.to_string()).collect();
        format!(
            "{} to {}{}",
            chain.join(" to "),
            self.final_url,
            self.stopped
                .as_ref()
                .map(|w| format!(" (stopped: {w})"))
                .unwrap_or_default()
        )
    }
}

/// Connects-to address for `host`: all its addresses must be public.
async fn vetted_addr(t: &dyn Transport, host: &str) -> Result<SocketAddr, CheckError> {
    let ips = t.resolve(host).await.map_err(CheckError::Failed)?;
    if ips.is_empty() {
        return Err(CheckError::Failed(format!("{host} doesn't resolve")));
    }
    if let Some(bad) = ips.iter().find(|ip| !is_public_ip(**ip)) {
        return Err(refused(format!(
            "{host} resolves to a non-public address ({bad}); it can't be checked."
        )));
    }
    Ok(SocketAddr::new(ips[0], 443))
}

/// The refusal for a host that isn't one of the team's.
fn not_the_teams(host: &str) -> CheckError {
    refused(format!(
        "{host} isn't one of the team's sites. Only hosts listed in the team's review \
         settings, named in a URL in this work, used in the team's repositories, or a \
         service API host built from an id in their config can be checked. A person can \
         allow it by adding the domain in Settings, Integrations, Your websites (a domain \
         covers its subdomains). Don't retry it in this run: record an open item that names \
         {host} and what checking it would have confirmed, then go on without it."
    ))
}

// ─── Page fetches (`curl`, `wget`) ──────────────────────────

/// URLs one `curl` or `wget` line may fetch.
pub const MAX_FETCH_URLS: usize = 3;
/// Time for one fetch, all hops included.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(20);
/// Characters of a body shown.
pub const FETCH_EXCERPT: usize = 6000;

/// What a page fetch got: each hop, the last response's status, every one
/// of its headers, and its body when one was asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fetched {
    pub hops: Vec<Hop>,
    pub status: u16,
    pub final_url: String,
    pub headers: Vec<(String, String)>,
    pub content_type: String,
    pub body: Vec<u8>,
    /// Why the chain stopped before a final answer, if it did.
    pub stopped: Option<String>,
}

/// One read-only request, the way `curl` or `wget` sent it: the same URL
/// rules, team hosts and address checks as [`check_and_find`], every
/// redirect re-checked, followed only when the command asked (`-L`).
pub async fn fetch_page(
    raw: &str,
    req: &common::coder_shell::Fetch,
    gate: &dyn HostGate,
    t: &dyn Transport,
) -> Result<Fetched, CheckError> {
    let (mut url, mut host) = vet_url(raw)?;
    if !gate.allowed(&host).await {
        return Err(not_the_teams(&host));
    }
    let opts = SendOpts {
        headers: req.headers.clone(),
        any_text: req.body,
    };
    let method = if req.head { Method::Head } else { Method::Get };
    let mut out = Fetched::default();
    for hop in 0..=MAX_REDIRECTS {
        let addr = match vetted_addr(t, &host).await {
            Ok(a) => a,
            Err(e) if hop == 0 => return Err(e),
            Err(e) => {
                out.stopped = Some(format!("redirect to {url} not followed: {}", e.text()));
                return Ok(out);
            }
        };
        let resp = match t.send_with(method, &url, addr, &opts).await {
            Ok(r) => r,
            Err(e) if hop == 0 => return Err(CheckError::Failed(format!("{url}: {e}"))),
            Err(e) => {
                out.stopped = Some(format!("{url}: {e}"));
                return Ok(out);
            }
        };
        out.hops.push(Hop {
            url: url.to_string(),
            status: resp.status,
        });
        out.final_url = url.to_string();
        out.status = resp.status;
        out.headers = resp.headers.clone();
        out.content_type = resp.content_type.clone();
        out.body = if req.body && !req.head {
            resp.body.clone()
        } else {
            Vec::new()
        };
        let next = resp
            .location
            .as_deref()
            .filter(|_| req.follow && (300..400).contains(&resp.status));
        let Some(loc) = next else {
            return Ok(out);
        };
        if hop == MAX_REDIRECTS {
            out.stopped = Some(format!("more than {MAX_REDIRECTS} redirects"));
            return Ok(out);
        }
        let Ok(joined) = url.join(loc) else {
            out.stopped = Some(format!("redirect to an invalid URL ({loc})"));
            return Ok(out);
        };
        match vet_url(joined.as_str()) {
            Ok((u, h)) if gate.allowed(&h).await => {
                url = u;
                host = h;
            }
            Ok((u, h)) => {
                out.stopped = Some(format!(
                    "redirects to {u}, outside the team's sites ({h}); not followed"
                ));
                return Ok(out);
            }
            Err(e) => {
                out.stopped = Some(format!("redirects to {joined}; not followed: {}", e.text()));
                return Ok(out);
            }
        }
    }
    Ok(out)
}

impl Fetched {
    /// The answer for one URL: the redirects, the status line, every
    /// header (cookie values left out) and a bounded part of the body.
    pub fn render(&self, asked: &str, req: &common::coder_shell::Fetch) -> String {
        let method = if req.head { "HEAD" } else { "GET" };
        let mut s = format!("{method} {asked}\n");
        if let Some((_, earlier)) = self.hops.split_last() {
            for h in earlier {
                s.push_str(&format!("  {} {} (redirect, followed)\n", h.status, h.url));
            }
        }
        if self.hops.is_empty() {
            if let Some(why) = &self.stopped {
                s.push_str(&format!("stopped: {why}\n"));
            }
            return s;
        }
        s.push_str(&format!("HTTP {} {}\n", self.status, self.final_url));
        for (k, v) in &self.headers {
            if k.eq_ignore_ascii_case("set-cookie") {
                let name = v.split('=').next().unwrap_or("").trim();
                s.push_str(&format!("{k}: {name}=[value not shown]\n"));
            } else {
                s.push_str(&format!("{k}: {v}\n"));
            }
        }
        if let Some(why) = &self.stopped {
            s.push_str(&format!("stopped: {why}\n"));
        }
        if req.head {
            return s;
        }
        if !req.body {
            s.push_str("(body not shown: the command discarded it)\n");
            return s;
        }
        if self.body.is_empty() {
            if is_text(&self.content_type) {
                s.push_str("(empty body)\n");
            } else {
                s.push_str(&format!(
                    "(a {} body, which isn't text: not shown)\n",
                    self.content_type
                ));
            }
            return s;
        }
        let text = String::from_utf8_lossy(&self.body);
        let total = text.chars().count();
        let excerpt: String = text.chars().take(FETCH_EXCERPT).collect();
        let shown = excerpt.chars().count();
        let capped = self.body.len() >= PAGE_SCAN_BYTES;
        s.push_str(&format!(
            "--- body: {} ---\n{excerpt}\n",
            if shown < total || capped {
                format!(
                    "the first {shown} of {}{total} characters",
                    if capped { "more than " } else { "" }
                )
            } else {
                format!("{total} characters")
            }
        ));
        s
    }

    /// One line for the activity log and the evidence.
    pub fn line(&self) -> String {
        let chain: Vec<String> = self.hops.iter().map(|h| h.status.to_string()).collect();
        format!(
            "{} to {}{}",
            chain.join(" to "),
            self.final_url,
            self.stopped
                .as_ref()
                .map(|w| format!(" (stopped: {w})"))
                .unwrap_or_default()
        )
    }
}

/// Checks one URL: HEAD, then GET when HEAD fails, errs, the page is HTML
/// (for its title) or `find` has terms to look for in a JSON or text body;
/// redirects followed with every hop re-vetted.
#[cfg(test)]
pub async fn check(
    raw: &str,
    gate: &dyn HostGate,
    t: &dyn Transport,
) -> Result<Checked, CheckError> {
    check_and_find(raw, &[], gate, t).await
}

/// [`check`], and the text around each match of `find` in the final page.
pub async fn check_and_find(
    raw: &str,
    find: &[String],
    gate: &dyn HostGate,
    t: &dyn Transport,
) -> Result<Checked, CheckError> {
    let (mut url, mut host) = vet_url(raw)?;
    if !gate.allowed(&host).await {
        return Err(not_the_teams(&host));
    }
    let mut out = Checked::default();
    for hop in 0..=MAX_REDIRECTS {
        let addr = match vetted_addr(t, &host).await {
            Ok(a) => a,
            Err(e) if hop == 0 => return Err(e),
            Err(e) => {
                out.stopped = Some(format!("redirect to {url} not followed: {}", e.text()));
                return Ok(out);
            }
        };
        let head = t.send(Method::Head, &url, addr).await;
        let resp = match head {
            Ok(r) if (300..400).contains(&r.status) && r.location.is_some() => r,
            Ok(r)
                if (200..300).contains(&r.status)
                    && !is_html(&r.content_type)
                    && (find.is_empty() || !is_readable(&r.content_type)) =>
            {
                r
            }
            // HEAD refused or errored, the page is HTML (title), or the
            // body holds what `find` asks for: GET.
            _ => match t.send(Method::Get, &url, addr).await {
                Ok(r) => r,
                Err(e) if hop == 0 => return Err(CheckError::Failed(format!("{url}: {e}"))),
                Err(e) => {
                    out.stopped = Some(format!("{url}: {e}"));
                    return Ok(out);
                }
            },
        };
        out.hops.push(Hop {
            url: url.to_string(),
            status: resp.status,
        });
        out.final_url = url.to_string();
        let next = resp
            .location
            .as_deref()
            .filter(|_| (300..400).contains(&resp.status));
        let Some(loc) = next else {
            out.content_type = resp.content_type.clone();
            out.title = if is_html(&resp.content_type) {
                page_title(&resp.body)
            } else {
                None
            };
            if !find.is_empty() {
                let page = String::from_utf8_lossy(&resp.body);
                out.found = find
                    .iter()
                    .map(|term| (term.clone(), find_in(&page, term)))
                    .collect();
            }
            return Ok(out);
        };
        if hop == MAX_REDIRECTS {
            out.stopped = Some(format!("more than {MAX_REDIRECTS} redirects"));
            return Ok(out);
        }
        let Ok(joined) = url.join(loc) else {
            out.stopped = Some(format!("redirect to an invalid URL ({loc})"));
            return Ok(out);
        };
        match vet_url(joined.as_str()) {
            Ok((u, h)) if gate.allowed(&h).await => {
                url = u;
                host = h;
            }
            Ok((u, h)) => {
                out.stopped = Some(format!(
                    "redirects to {u}, outside the team's sites ({h}); not followed"
                ));
                return Ok(out);
            }
            Err(e) => {
                out.stopped = Some(format!("redirects to {joined}; not followed: {}", e.text()));
                return Ok(out);
            }
        }
    }
    Ok(out)
}

// ─── The tool ───────────────────────────────────────────────

/// `check_url` for one run: the team's hosts, the call budget, the log.
pub struct WebCheck {
    team_id: String,
    /// `reviewer`, `coder`, `planner`.
    reader: String,
    repo: String,
    pr: u64,
    run: RunRef,
    /// The org searched for a host the policy doesn't list.
    org: String,
    /// The team's connected repositories (`owner/name`).
    team_repos: Vec<String>,
    policy: HostPolicy,
    /// Hosts the repository search already decided.
    searched: Mutex<HashMap<String, bool>>,
    used: AtomicUsize,
    /// URLs this run may check ([`MAX_CHECKS`] unless set).
    limit: usize,
    /// One line per checked URL, for the critic.
    results: Mutex<Vec<String>>,
    /// Checks that were refused or failed, as the run's lookups: what the
    /// work could not confirm is evidence too, never dropped silently.
    unconfirmed: Mutex<Vec<super::proof::Lookup>>,
}

/// The team's listed domains from its review settings.
pub async fn team_domains(state: &WorkerState, team_id: &str) -> Vec<String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(SETTINGS_SK.to_string()),
        )
        .send()
        .await;
    let list: Vec<String> = match item {
        Ok(o) => o
            .item()
            .and_then(|it| it.get(SETTINGS_ATTR))
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
            .unwrap_or_default(),
        Err(e) => {
            warn!(error = %e, "Could not read the team's web domains");
            Vec::new()
        }
    };
    common::web_domains::clean_all(&list)
}

impl WebCheck {
    /// `work_text` is the work at hand (a diff, a ticket, a plan): hosts in
    /// its URLs may be checked.
    pub async fn load(
        state: &WorkerState,
        team_id: &str,
        repo: &str,
        reader: &str,
        pr: u64,
        work_text: &str,
    ) -> WebCheck {
        let domains = team_domains(state, team_id).await;
        let team_repos = super::plan::fetch_team_repos(state, team_id).await;
        WebCheck::new(team_id, repo, reader, pr, domains, work_text, team_repos)
    }

    pub fn new(
        team_id: &str,
        repo: &str,
        reader: &str,
        pr: u64,
        domains: Vec<String>,
        work_text: &str,
        team_repos: Vec<String>,
    ) -> WebCheck {
        WebCheck {
            team_id: team_id.to_string(),
            reader: reader.to_string(),
            repo: repo.to_string(),
            pr,
            run: RunRef::default(),
            org: repo.split('/').next().unwrap_or_default().to_string(),
            team_repos,
            policy: HostPolicy {
                domains,
                named: hosts_in(work_text),
            },
            searched: Mutex::new(HashMap::new()),
            used: AtomicUsize::new(0),
            limit: MAX_CHECKS,
            results: Mutex::new(Vec::new()),
            unconfirmed: Mutex::new(Vec::new()),
        }
    }

    /// The checks this run could not make (refused or failed), as lookups
    /// for the run's evidence, so what wasn't confirmed reaches the check
    /// before the pull request and its open items.
    pub fn lookups(&self) -> Vec<super::proof::Lookup> {
        self.unconfirmed
            .lock()
            .map(|l| l.clone())
            .unwrap_or_default()
    }

    fn note_unconfirmed(&self, u: &str, why: &str) {
        if let Ok(mut l) = self.unconfirmed.lock() {
            let id = format!("W{}", l.len() + 1);
            l.push(super::proof::Lookup::new(
                id,
                SOURCE_NAME,
                super::proof::LookupOutcome::Failed,
                &format!("check_url {u}"),
                why,
            ));
        }
    }

    pub fn with_run(mut self, run_id: &str, origin: String) -> Self {
        self.run = RunRef {
            run_id: run_id.to_string(),
            origin,
        };
        self
    }

    /// A different number of URLs this run may check: the link and redirect
    /// check before a PR opens checks every entry of the change.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Prompt lines naming the tool and the hosts known up front.
    pub fn prompt_section(&self) -> String {
        let mut known: Vec<&String> = self
            .policy
            .domains
            .iter()
            .chain(self.policy.named.iter())
            .collect();
        known.sort();
        known.dedup();
        let list = if known.is_empty() {
            "none listed; hosts used in the team's repositories can still be checked".to_string()
        } else {
            known
                .iter()
                .take(30)
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "\n## Checking the team's public pages\n`check_url` checks that https pages on the \
             team's own sites load: status of each redirect hop, final URL, content type and \
             title. Before asking anyone to confirm that a link, redirect target or page works, \
             check it and report what came back (\"Checked: all six targets return 200\"). \
             When a ticket says a page shows a wrong value, pass `find` (the record id in the \
             URL, the field or the wrong text) to see what the page really rendered, and start \
             from that value. \
             Hosts known for this work: {list}.\n"
        )
    }

    /// What the URLs checked in this run returned, for the critic.
    pub fn evidence(&self) -> String {
        let lines = self.results.lock().map(|r| r.clone()).unwrap_or_default();
        if lines.is_empty() {
            return String::new();
        }
        format!(
            "\n## Pages checked with check_url during this review\n{}\n",
            lines
                .iter()
                .map(|l| format!("- {l}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }

    fn take(&self) -> bool {
        self.used.fetch_add(1, Ordering::SeqCst) < self.limit
    }

    /// One URL checked by the pipeline itself (not the agent): the same
    /// host rules, addresses and budget as the tool, written to the same
    /// activity log. A result that can't be logged is withheld.
    pub async fn verify(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        u: &str,
    ) -> Result<Checked, CheckError> {
        let gate = RepoGate {
            web: self,
            search: github,
        };
        let (res, ms) = self.checked(&gate, &Net, u, &[]).await;
        let (outcome, detail) = outcome_of(&res);
        if !self.log(state, u, outcome, &detail, ms).await {
            return Err(CheckError::Failed(
                "the check could not be recorded, so its result is withheld".into(),
            ));
        }
        res
    }

    /// [`WebCheck::verify`], looking for `find` in the page.
    pub async fn verify_find(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        u: &str,
        find: &[String],
    ) -> Result<Checked, CheckError> {
        let gate = RepoGate {
            web: self,
            search: github,
        };
        let (res, ms) = self.checked(&gate, &Net, u, find).await;
        let (outcome, detail) = outcome_of(&res);
        if !self.log(state, u, outcome, &detail, ms).await {
            return Err(CheckError::Failed(
                "the check could not be recorded, so its result is withheld".into(),
            ));
        }
        res
    }

    /// Writes one call to the team's activity log; false when it failed.
    async fn log(
        &self,
        state: &WorkerState,
        u: &str,
        outcome: &'static str,
        detail: &str,
        ms: u128,
    ) -> bool {
        let logged = store::log_activity_in(
            &state.dynamo,
            &state.config.settings_table_name,
            &self.team_id,
            &Activity {
                source_id: SOURCE_ID.into(),
                source_name: SOURCE_NAME.into(),
                actor: self.reader.clone(),
                repo: self.repo.clone(),
                pr: self.pr,
                query: common::truncate_str(u, 400).to_string(),
                outcome,
                detail: common::truncate_str(detail, 300).to_string(),
                ms,
            },
            &self.run,
        )
        .await;
        if let Err(e) = logged {
            warn!(error = %e, "Could not record a page check; withholding its result");
            return false;
        }
        true
    }

    /// Runs the tool. `None` when `name` isn't this tool.
    pub async fn handle(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        name: &str,
        input: &Value,
    ) -> Option<String> {
        if name == PROBE_TOOL {
            let gate = RepoGate {
                web: self,
                search: github,
            };
            return Some(self.probe(state, &gate, input).await);
        }
        if name != TOOL {
            return None;
        }
        let gate = RepoGate {
            web: self,
            search: github,
        };
        Some(self.run_urls(state, &gate, &Net, input).await)
    }

    /// Runs [`probe_tool`]: the page's host must be one this run may check.
    async fn probe(&self, state: &WorkerState, gate: &dyn HostGate, input: &Value) -> String {
        let url = input["url"].as_str().unwrap_or("").trim().to_string();
        let (parsed, host) = match vet_url(&url) {
            Ok(v) => v,
            Err(CheckError::Refused(m)) | Err(CheckError::Failed(m)) => {
                return format!("{url}: refused: {m}")
            }
        };
        if !gate.allowed(&host).await {
            return format!(
                "{url}: refused: `{host}` isn't a host this run may load (name it in the work at hand, or it must be used in the team's repositories)"
            );
        }
        let source = match super::skill_run::pack(&[(
            "probe.mjs".to_string(),
            common::e2e::probe::probe_script(parsed.as_str()),
        )]) {
            Ok(b) => b,
            Err(e) => return format!("The probe couldn't be packed: {e}"),
        };
        let sandbox = crate::clients::sandbox::SandboxClient::new(
            &state.codebuild,
            &state.s3,
            &state.logs,
            &state.config.sandbox_bucket_name,
            &state.config.sandbox_project_name,
        )
        .with_egress(crate::agent::policy::sandbox_egress(state, &self.team_id).await);
        let mut hosts: Vec<String> = common::preview::CAPTURE_HOSTS
            .iter()
            .map(|h| h.to_string())
            .collect();
        hosts.push(host.clone());
        let cmd = common::preview::capture_command().replace("capture.mjs", "probe.mjs");
        let key = format!("probe-{}", uuid::Uuid::new_v4().simple());
        let out = sandbox
            .run_script(
                crate::clients::sandbox::ScriptRun {
                    key: &key,
                    source,
                    cmd: &cmd,
                    secret: None,
                    hosts,
                },
                None,
            )
            .await;
        let record = match out {
            Ok(o) => match o.files.iter().find(|(n, _)| n.ends_with("probe.json")) {
                Some((_, body)) => body.clone(),
                None => {
                    return format!(
                        "{url}: the probe gave no record ({})",
                        common::truncate_str(o.output.lines().last().unwrap_or(""), 200)
                    )
                }
            },
            Err(e) => return format!("{url}: the sandbox couldn't run the probe ({e})"),
        };
        format!(
            "Probe of {url}. Commit this record at `{}` and quote the test ids and fields you use in the pull request; write selectors and field reads only from it:\n{}",
            common::e2e::probe::record_path(&host),
            common::truncate_str(&record, 12_000)
        )
    }

    async fn run_urls(
        &self,
        state: &WorkerState,
        gate: &dyn HostGate,
        t: &dyn Transport,
        input: &Value,
    ) -> String {
        let urls = requested(input);
        if urls.is_empty() {
            return "Give `urls`: one or more https URLs.".into();
        }
        let find = find_terms(input);
        let mut out = String::new();
        for u in urls {
            let (outcome, text, detail, ms) = self.one(gate, t, &u, &find).await;
            if !self.log(state, &u, outcome, &detail, ms).await {
                out.push_str(&format!(
                    "{u}\n  The check could not be recorded, so its result is withheld.\n"
                ));
                continue;
            }
            out.push_str(&text);
        }
        out
    }

    /// One URL: `(outcome, answer, log detail, ms)`.
    async fn one(
        &self,
        gate: &dyn HostGate,
        t: &dyn Transport,
        u: &str,
        find: &[String],
    ) -> (&'static str, String, String, u128) {
        let (res, ms) = self.checked(gate, t, u, find).await;
        match res {
            Ok(c) => {
                let line = format!("{u}: {}", c.line());
                if let Ok(mut r) = self.results.lock() {
                    r.push(line);
                }
                let detail = match c.status() {
                    Some(s) => format!("{} · {}", s, c.line()),
                    None => c.line(),
                };
                let left = self.limit.saturating_sub(self.used.load(Ordering::SeqCst));
                (
                    "ok",
                    format!(
                        "{}  ({left} of {} page checks left in this run)\n",
                        c.render(u).trim_end(),
                        self.limit
                    ),
                    detail,
                    ms,
                )
            }
            Err(CheckError::Refused(m)) => {
                self.note_unconfirmed(u, &format!("refused: {m}"));
                ("refused", format!("{u}\n  refused: {m}\n"), m, ms)
            }
            Err(CheckError::Failed(m)) => {
                self.note_unconfirmed(u, &format!("could not check: {m}"));
                if let Ok(mut r) = self.results.lock() {
                    r.push(format!("{u}: could not be checked ({m})"));
                }
                ("failed", format!("{u}\n  could not check: {m}\n"), m, ms)
            }
        }
    }
}

/// What a `curl` or `wget` line got, as the coder's bash tool reports it.
pub struct FetchReport {
    pub text: String,
    /// `ok`, `refused` or `failed` for the line: refused when every URL
    /// was refused.
    pub outcome: &'static str,
    /// A short line for the run log (`HTTP 200`, `refused: ...`).
    pub summary: String,
}

impl WebCheck {
    /// A page fetch from the coder's `bash` (`curl -sI`, `curl -sS`,
    /// `wget -qO-`): the same hosts, addresses, budget and activity log as
    /// `check_url`; the answer has the status, every header and part of
    /// the body.
    pub async fn fetch(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        req: &common::coder_shell::Fetch,
    ) -> FetchReport {
        let gate = RepoGate {
            web: self,
            search: github,
        };
        let mut parts = Vec::new();
        for u in req.urls.iter().take(MAX_FETCH_URLS) {
            let (outcome, text, detail, ms) = self.fetch_one(&gate, &Net, u, req).await;
            let logged = self
                .log(
                    state,
                    &format!("{} {u}", if req.head { "HEAD" } else { "GET" }),
                    outcome,
                    &detail,
                    ms,
                )
                .await;
            parts.push(if logged {
                (outcome, text, detail)
            } else {
                (
                    "failed",
                    format!(
                        "{u}\n  The request could not be recorded, so its result is withheld.\n"
                    ),
                    "not recorded".to_string(),
                )
            });
        }
        fetch_report(parts, req.urls.len())
    }

    /// One URL of a page fetch: `(outcome, answer, log detail, ms)`.
    async fn fetch_one(
        &self,
        gate: &dyn HostGate,
        t: &dyn Transport,
        u: &str,
        req: &common::coder_shell::Fetch,
    ) -> (&'static str, String, String, u128) {
        let started = Instant::now();
        if !self.take() {
            let m = format!(
                "All {} page checks for this run are used (curl and wget count as checks). \
                 Don't fetch pages again in this run; say which weren't read.",
                self.limit
            );
            return ("refused", format!("{u}\n  refused: {m}\n"), m, 0);
        }
        let res = match tokio::time::timeout(FETCH_TIMEOUT, fetch_page(u, req, gate, t)).await {
            Ok(r) => r,
            Err(_) => Err(CheckError::Failed(format!(
                "timed out after {}s",
                FETCH_TIMEOUT.as_secs()
            ))),
        };
        let ms = started.elapsed().as_millis();
        match res {
            Ok(f) => {
                if let Ok(mut r) = self.results.lock() {
                    r.push(format!("{u}: {}", f.line()));
                }
                let detail = format!("{} · {}", f.status, f.line());
                ("ok", f.render(u, req), detail, ms)
            }
            Err(CheckError::Refused(m)) => {
                self.note_unconfirmed(u, &format!("refused: {m}"));
                ("refused", format!("{u}\n  refused: {m}\n"), m, ms)
            }
            Err(CheckError::Failed(m)) => {
                self.note_unconfirmed(u, &format!("could not fetch: {m}"));
                ("failed", format!("{u}\n  could not fetch: {m}\n"), m, ms)
            }
        }
    }
}

/// The report for a fetch line from each URL's `(outcome, answer, detail)`.
fn fetch_report(parts: Vec<(&'static str, String, String)>, asked: usize) -> FetchReport {
    let mut text: String = parts
        .iter()
        .map(|p| p.1.trim_end().to_string())
        .collect::<Vec<_>>()
        .join("\n\n");
    if asked > MAX_FETCH_URLS {
        text.push_str(&format!(
            "\n\nOnly the first {MAX_FETCH_URLS} URLs were fetched: send the rest on another call."
        ));
    }
    let outcome = if parts.iter().any(|p| p.0 == "ok") {
        "ok"
    } else if parts.iter().all(|p| p.0 == "refused") {
        "refused"
    } else {
        "failed"
    };
    let summary = match (parts.len(), parts.first()) {
        (1, Some(("ok", _, detail))) => {
            format!("HTTP {}", detail.split(" · ").next().unwrap_or(""))
        }
        (1, Some((o, _, detail))) => format!("{o}: {detail}"),
        (n, _) => format!(
            "{} of {n} pages fetched",
            parts.iter().filter(|p| p.0 == "ok").count()
        ),
    };
    FetchReport {
        text,
        outcome,
        summary: common::truncate_str(&summary, 180).to_string(),
    }
}

impl WebCheck {
    /// One URL within the run's budget, timed: the check, or why not.
    async fn checked(
        &self,
        gate: &dyn HostGate,
        t: &dyn Transport,
        u: &str,
        find: &[String],
    ) -> (Result<Checked, CheckError>, u128) {
        let started = Instant::now();
        if !self.take() {
            return (
                Err(refused(format!(
                    "All {} page checks for this run are used. Don't call check_url again in this run; say which pages weren't checked.",
                    self.limit
                ))),
                0,
            );
        }
        let res = match tokio::time::timeout(TIMEOUT, check_and_find(u, find, gate, t)).await {
            Ok(r) => r,
            Err(_) => Err(CheckError::Failed(format!(
                "timed out after {}s",
                TIMEOUT.as_secs()
            ))),
        };
        (res, started.elapsed().as_millis())
    }
}

/// A check's outcome and detail for the activity log.
fn outcome_of(res: &Result<Checked, CheckError>) -> (&'static str, String) {
    match res {
        Ok(c) => (
            "ok",
            match c.status() {
                Some(s) => format!("{} · {}", s, c.line()),
                None => c.line(),
            },
        ),
        Err(CheckError::Refused(m)) => ("refused", m.clone()),
        Err(CheckError::Failed(m)) => ("failed", m.clone()),
    }
}

/// The terms a call asks to find in each page, at most [`MAX_FIND_TERMS`].
fn find_terms(input: &Value) -> Vec<String> {
    let mut v: Vec<String> = input["find"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    v.dedup();
    v.truncate(MAX_FIND_TERMS);
    v
}

/// The URLs a call asks for: `urls`, or a single `url`.
fn requested(input: &Value) -> Vec<String> {
    let mut v: Vec<String> = input["urls"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|u| u.as_str().map(|s| s.trim().to_string()))
                .collect()
        })
        .unwrap_or_default();
    if let Some(u) = input["url"].as_str() {
        v.push(u.trim().to_string());
    }
    v.retain(|u| !u.is_empty());
    v.dedup();
    v
}

/// The policy, then a search of the team's repositories for the host, or
/// for the config id a service API host is built from.
struct RepoGate<'a> {
    web: &'a WebCheck,
    search: &'a dyn OrgSearch,
}

impl RepoGate<'_> {
    /// Whether a file matching `query` is in one of the team's repositories.
    async fn in_team_repos(&self, query: &str, host: &str) -> bool {
        match self.search.repos_with(&self.web.org, query).await {
            Ok(repos) => repos.iter().any(|repo| {
                self.web
                    .team_repos
                    .iter()
                    .any(|r| r.eq_ignore_ascii_case(repo))
            }),
            Err(e) => {
                warn!(error = %e, host, "Could not search the team's repositories for a host");
                false
            }
        }
    }
}

#[async_trait::async_trait]
impl HostGate for RepoGate<'_> {
    async fn allowed(&self, host: &str) -> bool {
        if self.web.policy.allows(host) {
            return true;
        }
        if let Some(known) = self
            .web
            .searched
            .lock()
            .ok()
            .and_then(|m| m.get(host).copied())
        {
            return known;
        }
        let found = if self.web.org.is_empty() {
            false
        } else if self.in_team_repos(host, host).await {
            true
        } else if let Some((id, service)) = config_service(host) {
            // The host is built from an id in the config: the id next to
            // the service's name in a team repository makes it the team's.
            self.in_team_repos(&format!("\"{id}\" {service}"), host)
                .await
        } else {
            false
        };
        if let Ok(mut m) = self.web.searched.lock() {
            m.insert(host.to_string(), found);
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scripted DNS and responses: `routes[url] = [(method or any, resp)]`.
    #[derive(Default)]
    struct Fake {
        dns: HashMap<String, Vec<IpAddr>>,
        routes: HashMap<String, Resp>,
        head_fails: HashSet<String>,
        sent: Mutex<Vec<(Method, String, SocketAddr)>>,
    }

    impl Fake {
        fn host(mut self, h: &str, ip: &str) -> Self {
            self.dns
                .entry(h.into())
                .or_default()
                .push(ip.parse().unwrap());
            self
        }
        fn page(mut self, url: &str, status: u16, ct: &str, body: &str) -> Self {
            self.routes.insert(
                url.into(),
                Resp {
                    status,
                    location: None,
                    content_type: ct.into(),
                    body: body.as_bytes().to_vec(),
                    headers: vec![("content-type".into(), ct.into())],
                },
            );
            self
        }
        /// Response headers on a page already added.
        fn headers(mut self, url: &str, h: &[(&str, &str)]) -> Self {
            let r = self.routes.get_mut(url).expect("a page");
            r.headers
                .extend(h.iter().map(|(k, v)| (k.to_string(), v.to_string())));
            self
        }
        fn redirect(mut self, url: &str, status: u16, to: &str) -> Self {
            self.routes.insert(
                url.into(),
                Resp {
                    status,
                    location: Some(to.into()),
                    ..Default::default()
                },
            );
            self
        }
        fn sent_urls(&self) -> Vec<String> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|s| s.1.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl Transport for Fake {
        async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String> {
            self.dns
                .get(host)
                .cloned()
                .ok_or_else(|| format!("{host} doesn't resolve"))
        }
        async fn send(&self, m: Method, url: &Url, addr: SocketAddr) -> Result<Resp, String> {
            self.sent.lock().unwrap().push((m, url.to_string(), addr));
            if m == Method::Head && self.head_fails.contains(url.as_str()) {
                return Ok(Resp {
                    status: 405,
                    ..Default::default()
                });
            }
            let mut r = self.routes.get(url.as_str()).cloned().unwrap_or(Resp {
                status: 404,
                content_type: "text/html".into(),
                ..Default::default()
            });
            if m == Method::Head {
                r.body.clear();
            }
            Ok(r)
        }
    }

    fn policy(domains: &[&str]) -> HostPolicy {
        HostPolicy {
            domains: domains.iter().map(|s| s.to_string()).collect(),
            named: HashSet::new(),
        }
    }

    const PUBLIC: &str = "93.184.215.14";

    #[test]
    fn private_link_local_and_metadata_addresses_are_not_public() {
        for ip in [
            "10.0.0.1",
            "172.16.5.4",
            "172.31.255.255",
            "192.168.1.1",
            "127.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fd00:ec2::254",
            "fe80::1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "64:ff9b::a00:1",
            "2002:a00:1::",
            "ff02::1",
        ] {
            assert!(!is_public_ip(ip.parse().unwrap()), "{ip} must be refused");
        }
        for ip in [
            PUBLIC,
            "8.8.8.8",
            "172.32.0.1",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
        ] {
            assert!(is_public_ip(ip.parse().unwrap()), "{ip} is public");
        }
    }

    #[test]
    fn only_https_host_names_on_the_default_port_without_credentials() {
        assert!(vet_url("https://www.example.com/a").is_ok());
        assert!(vet_url("https://www.example.com:443/a").is_ok());
        for bad in [
            "http://www.example.com/",
            "ftp://www.example.com/",
            "https://user:pw@www.example.com/",
            "https://user@www.example.com/",
            "https://www.example.com:8443/",
            "https://169.254.169.254/latest/meta-data/",
            "https://[::1]/",
            "https://localhost/",
            "https://printer.local/",
            "file:///etc/passwd",
            "not a url",
        ] {
            assert!(
                matches!(vet_url(bad), Err(CheckError::Refused(_))),
                "{bad} must be refused"
            );
        }
        assert_eq!(
            vet_url("https://WWW.Example.COM./x").unwrap().1,
            "www.example.com"
        );
    }

    #[test]
    fn listed_domains_cover_subdomains_named_hosts_are_exact() {
        let mut p = policy(&["example.com"]);
        p.named.insert("docs.example.org".into());
        assert!(p.allows("example.com"));
        assert!(p.allows("www.example.com"));
        assert!(!p.allows("badexample.com"));
        assert!(!p.allows("example.com.evil.net"));
        assert!(p.allows("docs.example.org"));
        assert!(!p.allows("www.docs.example.org"));
        assert!(!p.allows("example.org"));
    }

    #[test]
    fn hosts_come_from_urls_in_the_work() {
        let diff = r#"
+  "/old-pricing" = "https://www.example.com/pricing"
-  "/old" = "https://legacy.example.com/x"
 see (https://docs.example.org/guide), and http://insecure.example.net
 not a host: file.rs, index.html
"#;
        let h = hosts_in(diff);
        for want in [
            "www.example.com",
            "legacy.example.com",
            "docs.example.org",
            "insecure.example.net",
        ] {
            assert!(h.contains(want), "{want}");
        }
        assert_eq!(h.len(), 4);
    }

    #[tokio::test]
    async fn a_live_page_reports_status_type_and_title() {
        let t = Fake::default().host("www.example.com", PUBLIC).page(
            "https://www.example.com/pricing",
            200,
            "text/html; charset=utf-8",
            "<html><head><TITLE>\n Pricing &amp; plans </title></head><body>secret</body>",
        );
        let c = check(
            "https://www.example.com/pricing",
            &policy(&["example.com"]),
            &t,
        )
        .await
        .unwrap();
        assert_eq!(c.status(), Some(200));
        assert_eq!(c.title.as_deref(), Some("Pricing & plans"));
        let r = c.render("https://www.example.com/pricing");
        assert!(r.contains("200"));
        assert!(!r.contains("secret"), "no body beyond the title");
        // HEAD first, then GET for the title.
        let sent = t.sent.lock().unwrap();
        assert_eq!(sent[0].0, Method::Head);
        assert_eq!(sent[1].0, Method::Get);
        assert_eq!(sent[0].2, SocketAddr::new(PUBLIC.parse().unwrap(), 443));
    }

    #[tokio::test]
    async fn head_refused_falls_back_to_get() {
        let mut t = Fake::default().host("www.example.com", PUBLIC).page(
            "https://www.example.com/file.pdf",
            200,
            "application/pdf",
            "",
        );
        t.head_fails
            .insert("https://www.example.com/file.pdf".into());
        let c = check(
            "https://www.example.com/file.pdf",
            &policy(&["example.com"]),
            &t,
        )
        .await
        .unwrap();
        assert_eq!(c.status(), Some(200));
        assert_eq!(c.content_type, "application/pdf");
    }

    #[tokio::test]
    async fn a_missing_page_is_a_404() {
        let t = Fake::default().host("www.example.com", PUBLIC);
        let c = check(
            "https://www.example.com/gone",
            &policy(&["example.com"]),
            &t,
        )
        .await
        .unwrap();
        assert_eq!(c.status(), Some(404));
    }

    /// The shape a server-rendered page carries its data in: JSON escaped
    /// inside a script string.
    const RENDERED: &str = r#"<html><head><title>Pottery classes</title></head><body><script>self.__next_f.push([1,"{\"id\":\"class-42\",\"location\":\"loc-east\",\"locationName\":\"East Studio\",\"address\":\"1 Example Street, Springfield\"}"])</script></body></html>"#;

    #[test]
    fn find_shows_the_rendered_value_unescaped() {
        let hits = find_in(RENDERED, "\"address\"");
        assert_eq!(hits.len(), 1);
        assert!(
            hits[0].contains(r#""address":"1 Example Street, Springfield""#),
            "{hits:?}"
        );
        assert!(
            hits[0].contains(r#""locationName":"East Studio""#),
            "{hits:?}"
        );
        assert!(find_in(RENDERED, "West Studio").is_empty());
    }

    #[test]
    fn find_caps_matches_and_keeps_char_boundaries() {
        let page = format!("{}é", "x id é ".repeat(200));
        let hits = find_in(&page, "id");
        assert_eq!(hits.len(), MAX_MATCHES);
        assert!(find_in("short", "").is_empty());
    }

    #[tokio::test]
    async fn a_check_with_find_returns_what_the_page_rendered() {
        let t = Fake::default().host("example.com", PUBLIC).page(
            "https://example.com/classes",
            200,
            "text/html",
            RENDERED,
        );
        let find = vec!["class-42".to_string(), "West Studio".to_string()];
        let c = check_and_find(
            "https://example.com/classes",
            &find,
            &policy(&["example.com"]),
            &t,
        )
        .await
        .unwrap();
        assert_eq!(c.title.as_deref(), Some("Pottery classes"));
        let text = c.render("https://example.com/classes");
        assert!(text.contains("1 Example Street"), "{text}");
        assert!(text.contains("\"West Studio\": not in the page"), "{text}");
    }

    #[tokio::test]
    async fn redirects_are_followed_and_reported() {
        let t = Fake::default()
            .host("example.com", PUBLIC)
            .host("www.example.com", PUBLIC)
            .redirect("https://example.com/a", 301, "https://www.example.com/a")
            .redirect("https://www.example.com/a", 302, "/b")
            .page("https://www.example.com/b", 200, "text/plain", "");
        let c = check("https://example.com/a", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        let chain: Vec<u16> = c.hops.iter().map(|h| h.status).collect();
        assert_eq!(chain, vec![301, 302, 200]);
        assert_eq!(c.final_url, "https://www.example.com/b");
        assert!(c.stopped.is_none());
    }

    #[tokio::test]
    async fn a_host_outside_the_team_is_refused_and_never_resolved() {
        let t = Fake::default().host("www.example.net", PUBLIC);
        let e = check("https://www.example.net/", &policy(&["example.com"]), &t)
            .await
            .unwrap_err();
        assert!(matches!(e, CheckError::Refused(_)));
        assert!(t.sent_urls().is_empty());
    }

    #[tokio::test]
    async fn a_host_resolving_to_a_private_address_is_refused() {
        for ip in ["10.1.2.3", "169.254.169.254", "127.0.0.1", "fd00:ec2::254"] {
            let t = Fake::default()
                .host("www.example.com", PUBLIC)
                .host("www.example.com", ip);
            let e = check("https://www.example.com/", &policy(&["example.com"]), &t)
                .await
                .unwrap_err();
            assert!(matches!(e, CheckError::Refused(_)), "{ip}");
            assert!(t.sent_urls().is_empty(), "{ip}: nothing sent");
        }
    }

    #[tokio::test]
    async fn every_redirect_is_rechecked() {
        // To a team host that resolves privately: stopped, not sent.
        let t = Fake::default()
            .host("www.example.com", PUBLIC)
            .host("internal.example.com", "10.0.0.5")
            .redirect(
                "https://www.example.com/a",
                301,
                "https://internal.example.com/admin",
            );
        let c = check("https://www.example.com/a", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        assert_eq!(c.hops.len(), 1);
        assert!(c.stopped.as_deref().unwrap().contains("non-public"));
        assert!(!t.sent_urls().iter().any(|u| u.contains("internal")));

        // To the metadata address by IP: refused as an IP literal.
        let t = Fake::default().host("www.example.com", PUBLIC).redirect(
            "https://www.example.com/a",
            302,
            "https://169.254.169.254/latest/meta-data/",
        );
        let c = check("https://www.example.com/a", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        assert!(c.stopped.is_some());
        assert_eq!(t.sent_urls().len(), 1);

        // To another site: reported, not followed.
        let t = Fake::default()
            .host("www.example.com", PUBLIC)
            .host("www.example.net", PUBLIC)
            .redirect(
                "https://www.example.com/a",
                301,
                "https://www.example.net/x",
            );
        let c = check("https://www.example.com/a", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        assert!(c
            .stopped
            .as_deref()
            .unwrap()
            .contains("outside the team's sites"));
        assert!(!t.sent_urls().iter().any(|u| u.contains("example.net")));

        // Downgrade to http: not followed.
        let t = Fake::default().host("www.example.com", PUBLIC).redirect(
            "https://www.example.com/a",
            301,
            "http://www.example.com/a",
        );
        let c = check("https://www.example.com/a", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        assert!(c.stopped.as_deref().unwrap().contains("https"));
    }

    #[tokio::test]
    async fn redirect_loops_stop_after_five() {
        let mut t = Fake::default().host("www.example.com", PUBLIC);
        for i in 0..10 {
            t = t.redirect(
                &format!("https://www.example.com/{i}"),
                301,
                &format!("https://www.example.com/{}", i + 1),
            );
        }
        let c = check("https://www.example.com/0", &policy(&["example.com"]), &t)
            .await
            .unwrap();
        assert_eq!(c.hops.len(), MAX_REDIRECTS + 1);
        assert!(c.stopped.as_deref().unwrap().contains("redirects"));
    }

    #[tokio::test]
    async fn a_run_checks_at_most_fifteen_urls() {
        let web = WebCheck::new(
            "T",
            "o/r",
            "reviewer",
            1,
            vec!["example.com".into()],
            "",
            vec![],
        );
        let t = Fake::default().host("www.example.com", PUBLIC).page(
            "https://www.example.com/x",
            200,
            "text/plain",
            "",
        );
        let mut ok = 0;
        let mut limited = 0;
        for _ in 0..(MAX_CHECKS + 3) {
            let (outcome, text, _, _) = web
                .one(&web.policy, &t, "https://www.example.com/x", &[])
                .await;
            match outcome {
                "ok" => ok += 1,
                _ => {
                    assert!(text.contains("used"));
                    limited += 1
                }
            }
        }
        assert_eq!((ok, limited), (MAX_CHECKS, 3));
        assert!(web.evidence().contains("200"));
    }

    /// The org's code search, scripted: `query -> repositories`.
    #[derive(Default)]
    struct FakeSearch {
        hits: HashMap<String, Vec<String>>,
        asked: Mutex<Vec<String>>,
    }

    impl FakeSearch {
        fn hit(mut self, query: &str, repo: &str) -> Self {
            self.hits.entry(query.into()).or_default().push(repo.into());
            self
        }
    }

    #[async_trait::async_trait]
    impl OrgSearch for FakeSearch {
        async fn repos_with(&self, _org: &str, query: &str) -> Result<Vec<String>, String> {
            self.asked.lock().unwrap().push(query.into());
            Ok(self.hits.get(query).cloned().unwrap_or_default())
        }
    }

    fn team_web() -> WebCheck {
        WebCheck::new(
            "TEAM#t",
            "acme/site",
            "coder",
            0,
            vec![],
            "",
            vec!["acme/site".into(), "acme/studio".into()],
        )
    }

    #[test]
    fn a_service_api_host_gives_its_config_id() {
        assert_eq!(
            config_service("ab12cd34.api.sanity.io"),
            Some(("ab12cd34".into(), "sanity"))
        );
        assert_eq!(
            config_service("ab12cd34.apicdn.sanity.io"),
            Some(("ab12cd34".into(), "sanity"))
        );
        assert_eq!(
            config_service("xyzref0123.supabase.co"),
            Some(("xyzref0123".into(), "supabase"))
        );
        // Not a plain id, or not a service host: nothing derived.
        assert_eq!(config_service("www.api.sanity.io"), None);
        assert_eq!(config_service("a.b.api.sanity.io"), None);
        assert_eq!(config_service("api.sanity.io"), None);
        assert_eq!(config_service("ab12cd34.api.example.com"), None);
    }

    #[tokio::test]
    async fn a_cms_api_host_built_from_the_repos_project_id_is_the_teams() {
        // The repository's config holds the project id, never the host.
        let web = team_web();
        let search = FakeSearch::default().hit("\"ab12cd34\" sanity", "acme/studio");
        let gate = RepoGate {
            web: &web,
            search: &search,
        };
        assert!(gate.allowed("ab12cd34.api.sanity.io").await);
        assert_eq!(
            *search.asked.lock().unwrap(),
            vec!["ab12cd34.api.sanity.io", "\"ab12cd34\" sanity"]
        );
        // Decided once per run.
        assert!(gate.allowed("ab12cd34.api.sanity.io").await);
        assert_eq!(search.asked.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_project_id_only_outside_the_teams_repositories_is_refused() {
        let web = team_web();
        let search = FakeSearch::default().hit("\"ab12cd34\" sanity", "acme/someone-elses");
        let gate = RepoGate {
            web: &web,
            search: &search,
        };
        assert!(!gate.allowed("ab12cd34.api.sanity.io").await);
        // Any other project's API stays refused.
        assert!(!gate.allowed("zz99yy88.api.sanity.io").await);
    }

    #[tokio::test]
    async fn an_ordinary_host_is_never_matched_by_an_id() {
        let web = team_web();
        let search = FakeSearch::default();
        let gate = RepoGate {
            web: &web,
            search: &search,
        };
        assert!(!gate.allowed("ab12cd34.example.com").await);
        assert_eq!(*search.asked.lock().unwrap(), vec!["ab12cd34.example.com"]);
    }

    #[tokio::test]
    async fn a_json_answer_is_searched_with_find() {
        let url = "https://ab12cd34.api.sanity.io/v2023-06-21/data/query/production?query=count(*)";
        let t = Fake::default().host("ab12cd34.api.sanity.io", PUBLIC).page(
            url,
            200,
            "application/json; charset=utf-8",
            r#"{"result":{"total":12,"withField":3}}"#,
        );
        let find = vec!["total".to_string()];
        let c = check_and_find(url, &find, &policy(&["ab12cd34.api.sanity.io"]), &t)
            .await
            .unwrap();
        let text = c.render(url);
        assert!(text.contains("\"total\":12"), "{text}");
        // Read-only: HEAD, then one GET for the body; nothing else.
        let sent = t.sent.lock().unwrap();
        assert!(sent
            .iter()
            .all(|s| matches!(s.0, Method::Head | Method::Get)));
    }

    #[tokio::test]
    async fn a_refusal_says_how_a_person_allows_the_host_and_is_recorded() {
        let web = team_web();
        let t = Fake::default();
        let gate = policy(&[]);
        let (outcome, text, _, _) = web
            .one(&gate, &t, "https://ab12cd34.api.sanity.io/v1/data", &[])
            .await;
        assert_eq!(outcome, "refused");
        assert!(
            text.contains("Settings, Integrations, Your websites"),
            "{text}"
        );
        assert!(text.contains("record an open item"), "{text}");
        let l = web.lookups();
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].outcome, super::super::proof::LookupOutcome::Failed);
        assert!(
            l[0].line.contains("ab12cd34.api.sanity.io"),
            "{}",
            l[0].line
        );
    }

    fn get(url: &str) -> common::coder_shell::Fetch {
        match common::coder_shell::classify(&format!("curl -sS {url}")) {
            common::coder_shell::Shell::Fetch(f) => f,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_page_fetch_returns_the_status_every_header_and_the_body() {
        let web = team_web();
        let url = "https://www.example.com/statics/embed.js";
        let t = Fake::default()
            .host("www.example.com", PUBLIC)
            .page(url, 200, "application/javascript", "window.embed = 1;")
            .headers(
                url,
                &[
                    ("x-frame-options", "SAMEORIGIN"),
                    ("content-security-policy", "frame-ancestors 'self'"),
                    ("set-cookie", "sid=abc123secret; Path=/"),
                ],
            );
        let (outcome, text, _, _) = web
            .fetch_one(&policy(&["example.com"]), &t, url, &get(url))
            .await;
        assert_eq!(outcome, "ok", "{text}");
        assert!(text.contains("HTTP 200"), "{text}");
        assert!(text.contains("x-frame-options: SAMEORIGIN"), "{text}");
        assert!(
            text.contains("content-security-policy: frame-ancestors 'self'"),
            "{text}"
        );
        assert!(text.contains("window.embed = 1;"), "{text}");
        // A cookie's value never reaches the agent.
        assert!(!text.contains("abc123secret") && text.contains("set-cookie: sid="));
        // It was a GET, pinned to the checked address.
        assert_eq!(t.sent.lock().unwrap()[0].0, Method::Get);
    }

    #[tokio::test]
    async fn a_head_fetch_sends_head_and_shows_headers_only() {
        let web = team_web();
        let url = "https://www.example.com/portal/1";
        let t = Fake::default()
            .host("www.example.com", PUBLIC)
            .page(url, 200, "text/html", "<title>Portal</title>")
            .headers(url, &[("x-frame-options", "DENY")]);
        let req = match common::coder_shell::classify(&format!("curl -sI {url}")) {
            common::coder_shell::Shell::Fetch(f) => f,
            other => panic!("{other:?}"),
        };
        let (outcome, text, _, _) = web
            .fetch_one(&policy(&["example.com"]), &t, url, &req)
            .await;
        assert_eq!(outcome, "ok");
        assert!(text.starts_with("HEAD ") && text.contains("x-frame-options: DENY"));
        assert!(!text.contains("<title>"), "{text}");
        assert_eq!(t.sent.lock().unwrap()[0].0, Method::Head);
    }

    #[tokio::test]
    async fn a_fetch_never_reaches_private_metadata_or_unlisted_hosts() {
        let web = team_web();
        let gate = policy(&["example.com"]);
        let t = Fake::default()
            .host("internal.example.com", "10.0.0.5")
            .host("meta.example.com", "169.254.169.254");
        for (u, why) in [
            ("https://internal.example.com/", "non-public"),
            ("https://meta.example.com/latest/meta-data/", "non-public"),
            ("https://169.254.169.254/latest/meta-data/", "IP address"),
            ("https://intranet.local/", "isn't a public host name"),
            ("http://www.example.com/", "only https"),
            (
                "https://other.example.org/",
                "isn't one of the team's sites",
            ),
        ] {
            let (outcome, text, _, _) = web.fetch_one(&gate, &t, u, &get(u)).await;
            assert_eq!(outcome, "refused", "{u}: {text}");
            assert!(text.contains(why), "{u}: {text}");
        }
        assert!(t.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn redirects_are_followed_only_when_asked_and_rechecked() {
        let web = team_web();
        let gate = policy(&["example.com"]);
        let t = Fake::default()
            .host("www.example.com", PUBLIC)
            .redirect("https://www.example.com/old", 301, "/new")
            .page(
                "https://www.example.com/new",
                200,
                "text/html",
                "<p>new</p>",
            );
        let (_, text, _, _) = web
            .fetch_one(
                &gate,
                &t,
                "https://www.example.com/old",
                &get("https://www.example.com/old"),
            )
            .await;
        assert!(text.contains("HTTP 301"), "{text}");
        let mut follow = get("https://www.example.com/old");
        follow.follow = true;
        let (_, text, _, _) = web
            .fetch_one(&gate, &t, "https://www.example.com/old", &follow)
            .await;
        assert!(
            text.contains("301 https://www.example.com/old (redirect"),
            "{text}"
        );
        assert!(
            text.contains("HTTP 200 https://www.example.com/new"),
            "{text}"
        );
        // A redirect off the team's hosts is reported, not followed.
        let t = Fake::default().host("www.example.com", PUBLIC).redirect(
            "https://www.example.com/away",
            302,
            "https://elsewhere.example.net/",
        );
        let mut follow = get("https://www.example.com/away");
        follow.follow = true;
        let (_, text, _, _) = web
            .fetch_one(&gate, &t, "https://www.example.com/away", &follow)
            .await;
        assert!(text.contains("outside the team's sites"), "{text}");
    }

    #[test]
    fn a_long_body_is_cut_and_says_so() {
        let f = Fetched {
            hops: vec![Hop {
                url: "https://www.example.com/a.js".into(),
                status: 200,
            }],
            status: 200,
            final_url: "https://www.example.com/a.js".into(),
            headers: vec![],
            content_type: "application/javascript".into(),
            body: "x".repeat(FETCH_EXCERPT + 500).into_bytes(),
            stopped: None,
        };
        let text = f.render(
            "https://www.example.com/a.js",
            &get("https://www.example.com/a.js"),
        );
        assert!(
            text.contains(&format!(
                "the first {FETCH_EXCERPT} of {}",
                FETCH_EXCERPT + 500
            )),
            "{}",
            &text[..200]
        );
        let report = fetch_report(vec![("ok", text, "200 · 200 to x".into())], 1);
        assert_eq!(
            (report.outcome, report.summary.as_str()),
            ("ok", "HTTP 200")
        );
    }

    #[test]
    fn calls_take_a_list_or_one_url() {
        assert_eq!(
            requested(
                &json!({"urls": ["https://a.example.com", " "], "url": "https://b.example.com"})
            ),
            vec!["https://a.example.com", "https://b.example.com"]
        );
        assert!(requested(&json!({})).is_empty());
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Against the real network: `cargo test -p worker -- --ignored live_check`.
    #[tokio::test]
    #[ignore]
    async fn live_check_of_a_public_page() {
        let gate = HostPolicy {
            domains: vec!["example.com".into()],
            ..Default::default()
        };
        let c = check("https://example.com/", &gate, &Net).await.unwrap();
        assert_eq!(c.status(), Some(200), "{c:?}");
        assert!(c.title.is_some(), "{c:?}");
        let c = check("https://example.com/definitely-missing-page", &gate, &Net)
            .await
            .unwrap();
        assert_eq!(c.status(), Some(404), "{c:?}");
    }
}
