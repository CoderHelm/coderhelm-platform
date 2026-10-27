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
//!   a URL in the work at hand (the PR's diff, the ticket), or a host that
//!   appears in one of the team's connected repositories;
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
             hand, or used in the team's repositories); others are refused. Use it before \
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
}

/// DNS and HTTP, behind a trait so the tests can script them.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String>;
    /// One request to `url`, connected to `addr` (already checked), no
    /// redirects followed.
    async fn send(&self, method: Method, url: &Url, addr: SocketAddr) -> Result<Resp, String>;
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
        let mut resp = req
            .header("Accept", "text/html,*/*;q=0.8")
            .send()
            .await
            .map_err(|e| {
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
        let mut body = Vec::new();
        if method == Method::Get && is_html(&content_type) {
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
        })
    }
}

fn is_html(content_type: &str) -> bool {
    let c = content_type.to_ascii_lowercase();
    c.starts_with("text/html") || c.starts_with("application/xhtml")
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
            "{} → {}{}",
            chain.join(" → "),
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

/// Checks one URL: HEAD, then GET when HEAD fails, errs, or the page is
/// HTML (for its title); redirects followed with every hop re-vetted.
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
        return Err(refused(format!(
            "{host} isn't one of the team's sites. Only hosts listed in the team's review \
             settings, named in a URL in this work, or used in the team's repositories can be \
             checked."
        )));
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
            Ok(r) if (200..300).contains(&r.status) && !is_html(&r.content_type) => r,
            // HEAD refused or errored, or the page is HTML (title): GET.
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
        let gate = RepoGate { web: self, github };
        let (res, ms) = self.checked(&gate, &Net, u, &[]).await;
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
        if name != TOOL {
            return None;
        }
        let gate = RepoGate { web: self, github };
        Some(self.run_urls(state, &gate, &Net, input).await)
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
            Err(CheckError::Refused(m)) => ("refused", format!("{u}\n  refused: {m}\n"), m, ms),
            Err(CheckError::Failed(m)) => {
                if let Ok(mut r) = self.results.lock() {
                    r.push(format!("{u}: could not be checked ({m})"));
                }
                ("failed", format!("{u}\n  could not check: {m}\n"), m, ms)
            }
        }
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

/// The policy, then a search of the team's repositories for the host.
struct RepoGate<'a> {
    web: &'a WebCheck,
    github: &'a GitHubClient,
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
        } else {
            match self.github.search_org_code(&self.web.org, host).await {
                Ok(hits) => hits.iter().any(|(repo, _)| {
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
                },
            );
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
