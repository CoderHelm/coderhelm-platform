//! Link and redirect checks, enforced in code.
//!
//! A reviewed pull request changed a site's redirect map with a target
//! copied from the ticket (the ticket had the slug wrong), described an
//! entry the diff didn't contain, answered a reviewer with a bare "Done",
//! and sat for days on evidence nobody re-ran. A permanent redirect is cached
//! by browsers, so a wrong target is hard to take back. The rules here are
//! the parts of the fix that don't rest on a prompt:
//!
//! - every redirect or link target the diff adds or changes is found in the
//!   diff itself ([`entries`]: redirect maps in HCL, JSON, YAML, JS/TS,
//!   nginx and edge-function style, `source`/`destination` objects, and
//!   Markdown links) and checked live before the PR opens: status, final
//!   URL, title and time ([`run_checks`]); a target that isn't a 200 page,
//!   or one that couldn't be checked, blocks the PR with a plain reason;
//! - each redirect source is requested as it is now (status and Location),
//!   so the description shows a dated before and after ([`Report::section`]);
//! - the description names only entries and files the diff contains
//!   ([`claim_violations`], [`enforce_claims`]);
//! - the evidence is dated, and re-run when a person is asked to merge or
//!   when it is more than a day old ([`refresh_pr`]);
//! - a reply to a review comment points at that evidence, never a bare
//!   "Done" ([`evidenced_reply`]).
//!
//! The checks go through `check_url`'s client (`web_check`): https, the
//! team's own hosts, public addresses only, every call logged.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::LazyLock;
use tracing::{info, warn};

use super::web_check::{CheckError, Checked};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The fence the coder names the sites its redirect maps serve in.
pub const SITES_FENCE: &str = "sites.json";
pub const START: &str = "<!-- coderhelm:links -->";
pub const END: &str = "<!-- /coderhelm:links -->";
const HEADING: &str = "Link and redirect checks";
/// GitHub's anchor for [`HEADING`].
pub const ANCHOR: &str = "link-and-redirect-checks";
const CHECKED_AT: &str = "<!-- checked-at: ";
const SITES_MARK: &str = "<!-- sites: ";
/// Evidence older than this is checked again.
pub const STALE_AFTER_HOURS: i64 = 24;
/// URLs one check of a change may request (sources, targets, old targets).
pub const MAX_URLS: usize = 60;
/// Fix rounds a blocked check gets before the run stops.
const MAX_FIX_ROUNDS: u32 = 1;

/// What the coder is told about redirect and link changes.
pub const CODER_NOTE: &str = "\n## Redirect and link changes\nWhen the change adds or changes a redirect or a link target, check every new target with `check_url` before you finish: it must answer 200 with a page title that is the page the ticket means. Never copy a target from the ticket unchecked; tickets get slugs wrong. Check each redirect source as it is now too. CoderHelm checks them all again before the PR opens and blocks it when a target fails. When a redirect map uses paths without a host, add a fenced ```sites.json block to your closing notes naming the site each map serves: `[{\"file\": \"the map's file\", \"map\": \"the map's name, when the file has several\", \"sources\": [\"https://the host the sources are requested on\", \"https://its staging host, if any\"], \"targets\": \"https://the host relative targets are on, when it differs\"}]`.\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A source path mapped to a target.
    Redirect,
    /// A link in a document.
    Link,
}

/// A redirect or link the diff adds or changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub file: String,
    /// The map the entry is in (`site_redirects`, `redirects`), when the
    /// diff shows it.
    pub block: String,
    pub kind: Kind,
    /// The path or URL redirected (none for a link).
    pub source: Option<String>,
    pub target: String,
    /// What the source pointed at before this change.
    pub old_target: Option<String>,
}

// ─── Reading the diff ───────────────────────────────────────

static KEYED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)(?:^|[\s{,])["']?(source|from|destination|target|to)["']?\s*[:=]\s*["'`]([^"'`]+)["'`]"#)
        .expect("keyed pattern")
});
static KEYED_BARE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*-?\s*(source|from|destination|target|to)\s*:\s*(\S+)\s*$")
        .expect("bare keyed pattern")
});
static QUOTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""([^"]*)"|'([^']*)'|`([^`]*)`"#).expect("quoted pattern"));
static BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^\s*(?:(?:variable|locals|map|const|let|var|export\s+const)\s+)?["']?([A-Za-z_][\w.-]*)["']?\s*(?:[=:]\s*)?(?:\{|\[|\(|map\s*\{)?\s*$|^\s*(?:variable|map)\s+["']?([A-Za-z_][\w.-]*)["']?.*\{\s*$"#)
        .expect("block pattern")
});
static LOCATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*location\s+(?:=\s*|~\*?\s*|\^~\s*)?(\S+)\s*\{\s*$").expect("location pattern")
});
static MD_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\]\((https?://[^)\s]+)\)").expect("markdown link pattern"));
static BARE_URL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"https?://[^\s)\]>`|"'<]+"#).expect("url pattern"));
static CODE_SPAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"`([^`\n]+)`").expect("code span pattern"));

/// A path or URL a redirect or link can point at.
fn url_ish(s: &str) -> bool {
    let s = s.trim();
    !s.is_empty()
        && !s.chars().any(char::is_whitespace)
        && ((s.starts_with('/') && !s.starts_with("//"))
            || s.starts_with("https://")
            || s.starts_with("http://"))
}

/// A redirect source as written, its pattern anchors taken off
/// (`^/old/?$` reads `/old`).
fn unanchor(s: &str) -> String {
    let s = s.trim().trim_start_matches("~*").trim_start_matches('~');
    let s = s.trim_start_matches('^');
    let s = s.strip_suffix('$').unwrap_or(s);
    let s = s
        .strip_suffix("/?")
        .or_else(|| s.strip_suffix("(/)?"))
        .unwrap_or(s);
    s.to_string()
}

/// The path or URL as a request can send it: `None` for a pattern
/// (wildcards, captures, alternatives).
pub fn literal(s: &str) -> Option<String> {
    let s = unanchor(s);
    let pattern = s.chars().any(|c| {
        matches!(
            c,
            '*' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '$' | '\\'
        )
    });
    (url_ish(&s) && !pattern).then_some(s)
}

fn source_ish(s: &str) -> bool {
    url_ish(&unanchor(s))
}

/// A line that maps a source to a target on its own: `"/a" = "/b"`,
/// `"/a": "/b"`, `'/a': '/b',`, `/a: /b`, `/a /b;`, `rewrite ^/a$ /b
/// permanent;`.
fn pair_on_line(line: &str) -> Option<(String, String)> {
    let t = line.trim();
    if t.starts_with('#') || t.starts_with("//") {
        return None;
    }
    let caps: Vec<regex::Captures> = QUOTED.captures_iter(t).collect();
    if caps.len() == 2 {
        let val = |c: &regex::Captures| {
            c.get(1)
                .or_else(|| c.get(2))
                .or_else(|| c.get(3))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default()
        };
        let (a, b) = (caps[0].get(0)?, caps[1].get(0)?);
        let before = t[..a.start()]
            .trim()
            .trim_start_matches(['-', '[', '{'])
            .trim();
        let between = t[a.end()..b.start()].trim();
        let after = t[b.end()..]
            .trim()
            .trim_start_matches([',', ';', ']', '}', ')'])
            .trim();
        let sep_ok = matches!(between, "=" | ":" | "=>" | "," | "->");
        let after_ok = after.is_empty() || after.starts_with('#') || after.starts_with("//");
        let (src, dst) = (val(&caps[0]), val(&caps[1]));
        if before.is_empty() && sep_ok && after_ok && source_ish(&src) && url_ish(&dst) {
            return Some((src, dst));
        }
        return None;
    }
    if !caps.is_empty() {
        return None;
    }
    // Unquoted: YAML `/a: /b`.
    if let Some((a, b)) = t.trim_start_matches("- ").split_once(": ") {
        if source_ish(a) && url_ish(b.trim()) {
            return Some((a.trim().to_string(), b.trim().to_string()));
        }
    }
    // nginx: `rewrite ^/a$ /b permanent;` and map lines `/a /b;`.
    if let Some(stmt) = t.strip_suffix(';') {
        let tokens: Vec<&str> = stmt.split_whitespace().collect();
        if tokens.first() == Some(&"rewrite") && tokens.len() >= 3 {
            if source_ish(tokens[1]) && url_ish(tokens[2]) {
                return Some((tokens[1].to_string(), tokens[2].to_string()));
            }
            return None;
        }
        if tokens.len() == 2 && source_ish(tokens[0]) && url_ish(tokens[1]) {
            return Some((tokens[0].to_string(), tokens[1].to_string()));
        }
    }
    None
}

/// The entries one side of a patch shows: `(block, source, target)`.
#[derive(Default)]
struct Side {
    block: String,
    pending: Option<String>,
    found: Vec<(String, String, String)>,
}

impl Side {
    fn read(&mut self, line: &str) {
        let t = line.trim();
        if let Some(c) = LOCATION.captures(t) {
            if source_ish(&c[1]) {
                self.pending = Some(c[1].to_string());
            }
            return;
        }
        if let Some(c) = BLOCK.captures(t) {
            let name = c.get(1).or_else(|| c.get(2)).map(|m| m.as_str());
            if let Some(n) = name.filter(|n| {
                !matches!(
                    n.to_ascii_lowercase().as_str(),
                    "default" | "value" | "locals" | "type" | "description"
                )
            }) {
                if t.ends_with('{') || t.ends_with('[') || t.ends_with('(') {
                    self.block = n.to_string();
                }
            }
        }
        // `return 301 https://…;` inside a `location` block.
        if let Some(stmt) = t.strip_suffix(';') {
            let tokens: Vec<&str> = stmt.split_whitespace().collect();
            if tokens.len() == 3
                && tokens[0] == "return"
                && tokens[1].starts_with('3')
                && url_ish(tokens[2])
            {
                if let Some(src) = self.pending.take() {
                    self.found
                        .push((self.block.clone(), src, tokens[2].to_string()));
                }
                return;
            }
        }
        let keyed: Vec<(String, String)> = KEYED
            .captures_iter(t)
            .map(|c| (c[1].to_ascii_lowercase(), c[2].to_string()))
            .chain(
                KEYED_BARE
                    .captures(t)
                    .map(|c| (c[1].to_ascii_lowercase(), c[2].to_string())),
            )
            .collect();
        if !keyed.is_empty() {
            for (key, val) in keyed {
                match key.as_str() {
                    "source" | "from" if source_ish(&val) => self.pending = Some(val),
                    "destination" | "target" | "to" if url_ish(&val) => {
                        if let Some(src) = self.pending.take() {
                            self.found.push((self.block.clone(), src, val));
                        }
                    }
                    _ => {}
                }
            }
            return;
        }
        if let Some((src, dst)) = pair_on_line(t) {
            self.found.push((self.block.clone(), src, dst));
        }
    }

    /// A new hunk: the map is what its header names, and a half-read
    /// `source` doesn't carry over.
    fn hunk(&mut self, header: &str) {
        self.pending = None;
        let context = header.rsplit("@@").next().unwrap_or_default();
        if let Some(c) = BLOCK.captures(context.trim()) {
            if let Some(n) = c.get(1).or_else(|| c.get(2)) {
                self.block = n.as_str().to_string();
            }
        }
    }
}

fn is_markdown(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.ends_with(".md") || p.ends_with(".mdx") || p.ends_with(".markdown")
}

/// The redirects and links one file's patch adds or changes.
pub fn file_entries(file: &str, patch: &str) -> Vec<Entry> {
    let mut old = Side::default();
    let mut new = Side::default();
    let mut links: Vec<String> = Vec::new();
    let mut old_links: HashSet<String> = HashSet::new();
    for line in patch.lines() {
        if line.starts_with("@@") {
            old.hunk(line);
            new.hunk(line);
            continue;
        }
        if line.starts_with('\\') {
            continue;
        }
        let (mark, text) = line.split_at(line.chars().next().map_or(0, char::len_utf8));
        if is_markdown(file) {
            let urls = MD_LINK.captures_iter(text).map(|c| c[1].to_string());
            match mark {
                "+" => links.extend(urls),
                "-" | " " => old_links.extend(urls),
                _ => {}
            }
            continue;
        }
        match mark {
            "+" => new.read(text),
            "-" => old.read(text),
            " " => {
                old.read(text);
                new.read(text);
            }
            _ => {}
        }
    }
    let mut out: Vec<Entry> = Vec::new();
    for (block, source, target) in &new.found {
        if old
            .found
            .contains(&(block.clone(), source.clone(), target.clone()))
        {
            continue;
        }
        let old_target = old
            .found
            .iter()
            .find(|(b, s, t)| b == block && s == source && t != target)
            .map(|(_, _, t)| t.clone());
        let e = Entry {
            file: file.to_string(),
            block: block.clone(),
            kind: Kind::Redirect,
            source: Some(source.clone()),
            target: target.clone(),
            old_target,
        };
        if !out.contains(&e) {
            out.push(e);
        }
    }
    for url in links {
        if old_links.contains(&url) || out.iter().any(|e| e.target == url) {
            continue;
        }
        out.push(Entry {
            file: file.to_string(),
            block: String::new(),
            kind: Kind::Link,
            source: None,
            target: url,
            old_target: None,
        });
    }
    out
}

/// Every redirect and link the diff (GitHub's compare payload) adds or
/// changes.
pub fn entries(diff: &Value) -> Vec<Entry> {
    diff["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .flat_map(|f| {
                    file_entries(
                        f["filename"].as_str().unwrap_or_default(),
                        f["patch"].as_str().unwrap_or_default(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

// ─── Where a path is requested ──────────────────────────────

/// The site a redirect map serves, as the coder named it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Site {
    /// The map's file (a path, or its end).
    #[serde(default)]
    pub file: String,
    /// The map's name, when the file has several.
    #[serde(default)]
    pub map: String,
    /// Hosts the sources are requested on (`https://host`): live, staging.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub sources: Vec<String>,
    /// Host relative targets are on, when it isn't the first source host.
    #[serde(default)]
    pub targets: String,
}

/// The text of the last ```` ```fence ```` block in `notes`.
fn fenced<'a>(notes: &'a str, fence: &str) -> Option<&'a str> {
    let marker = format!("```{fence}");
    let start = notes.rfind(&marker)?;
    let body = start + notes[start..].find('\n')? + 1;
    let end = notes[body..]
        .find("```")
        .map(|i| body + i)
        .unwrap_or(notes.len());
    Some(notes[body..end].trim())
}

/// The sites the coder named in its notes; none when it named none or the
/// block doesn't read.
pub fn parse_sites(notes: &str) -> Vec<Site> {
    let Some(raw) = fenced(notes, SITES_FENCE) else {
        return Vec::new();
    };
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        Many(Vec<Site>),
        Wrapped { sites: Vec<Site> },
        One(Site),
    }
    match serde_json::from_str::<OneOrMany>(raw) {
        Ok(OneOrMany::Many(v)) | Ok(OneOrMany::Wrapped { sites: v }) => v,
        Ok(OneOrMany::One(s)) => vec![s],
        Err(e) => {
            warn!(error = %e, "The coder's sites.json block didn't read");
            Vec::new()
        }
    }
}

/// `https://host` of a URL, or of a bare host name.
fn origin(s: &str) -> Option<String> {
    let s = s.trim();
    let with_scheme = if s.contains("://") {
        s.to_string()
    } else {
        format!("https://{s}")
    };
    let u = reqwest::Url::parse(&with_scheme).ok()?;
    Some(format!("https://{}", u.host_str()?.to_ascii_lowercase()))
}

fn site_for<'a>(sites: &'a [Site], e: &Entry) -> Option<&'a Site> {
    sites.iter().find(|s| {
        let file = s.file.trim().trim_start_matches("./");
        (file.is_empty() || e.file == file || e.file.ends_with(&format!("/{file}")))
            && (s.map.trim().is_empty() || s.map.trim() == e.block)
    })
}

/// The hosts an entry's relative source is requested on: the site the coder
/// named, else the team's one listed domain. Empty when neither says.
pub fn source_hosts(e: &Entry, sites: &[Site], domains: &[String]) -> Vec<String> {
    if let Some(s) = site_for(sites, e) {
        let hosts: Vec<String> = s.sources.iter().filter_map(|h| origin(h)).collect();
        if !hosts.is_empty() {
            return hosts;
        }
    }
    match domains {
        [one] => origin(one).into_iter().collect(),
        _ => Vec::new(),
    }
}

/// The URL a target (or old target) is requested at: as written when
/// absolute, else on the site's target host, its first source host, or the
/// team's one listed domain.
pub fn target_url(value: &str, e: &Entry, sites: &[Site], domains: &[String]) -> Option<String> {
    let v = literal(value)?;
    if v.starts_with("http") {
        return Some(v);
    }
    let host = site_for(sites, e)
        .and_then(|s| origin(&s.targets).filter(|_| !s.targets.trim().is_empty()))
        .or_else(|| source_hosts(e, sites, domains).into_iter().next())?;
    Some(format!("{host}{v}"))
}

/// The URLs one source is requested at.
fn source_urls(e: &Entry, sites: &[Site], domains: &[String]) -> Option<Vec<String>> {
    let src = literal(e.source.as_deref()?)?;
    if src.starts_with("http") {
        return Some(vec![src]);
    }
    Some(
        source_hosts(e, sites, domains)
            .into_iter()
            .map(|h| format!("{h}{src}"))
            .collect(),
    )
}

// ─── Checking ───────────────────────────────────────────────

/// One live request, behind a trait so the tests can script it.
#[async_trait::async_trait]
pub trait Checker: Send + Sync {
    async fn check(&self, url: &str) -> Result<Checked, CheckError>;
}

/// What one URL answered, and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub url: String,
    pub at: DateTime<Utc>,
    pub result: Result<Checked, String>,
}

impl Seen {
    /// `301 → https://x/y`, `404`, `200 · "Title"`, or why it failed.
    fn first_hop(&self) -> String {
        match &self.result {
            Err(m) => format!("not checked: {m}"),
            Ok(c) => match (c.hops.first(), c.hops.get(1)) {
                (Some(h), Some(next)) => format!("{} → `{}`", h.status, next.url),
                (Some(h), None) => match &c.stopped {
                    Some(why) => format!("{} ({why})", h.status),
                    None => h.status.to_string(),
                },
                (None, _) => c.stopped.clone().unwrap_or_else(|| "no answer".into()),
            },
        }
    }

    fn page(&self) -> String {
        match &self.result {
            Err(m) => format!("not checked: {m}"),
            Ok(c) => {
                let chain: Vec<String> = c.hops.iter().map(|h| h.status.to_string()).collect();
                let mut s = chain.join(" → ");
                if c.hops.len() > 1 {
                    s.push_str(&format!(" (`{}`)", c.final_url));
                }
                if let Some(t) = &c.title {
                    s.push_str(&format!(" · \"{t}\""));
                }
                if let Some(why) = &c.stopped {
                    s.push_str(&format!(" ({why})"));
                }
                s
            }
        }
    }
}

/// One entry and what its checks found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub entry: Entry,
    /// Each source host's answer now, before the change is live.
    pub before: Vec<Seen>,
    pub target: Option<Seen>,
    pub old: Option<Seen>,
    /// Why this entry blocks the PR.
    pub problems: Vec<String>,
    /// What a person should confirm (a pattern, a title that doesn't name
    /// the page).
    pub notes: Vec<String>,
}

/// Every entry's checks, and the sites they used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub rows: Vec<Row>,
    pub checked_at: DateTime<Utc>,
    pub sites: Vec<Site>,
}

/// Words of a path's last segment, for matching a page title.
fn slug_words(url: &str) -> Vec<String> {
    let path = url.split(['?', '#']).next().unwrap_or_default();
    let path = path.split("://").last().unwrap_or(path);
    let path = path.split_once('/').map_or("", |(_, p)| p);
    let last = path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default();
    let last = last.split('.').next().unwrap_or_default();
    last.split(['-', '_', '+'])
        .map(str::to_ascii_lowercase)
        .filter(|w| w.len() >= 3 && !w.chars().all(|c| c.is_ascii_digit()))
        .collect()
}

/// Whether the page title names what the target's slug says (a shared
/// word, or one starting the other: `plan` and `plans`).
pub fn title_matches(url: &str, title: &str) -> bool {
    let words = slug_words(url);
    if words.is_empty() {
        return true;
    }
    let lower = title.to_ascii_lowercase();
    let title_words: Vec<&str> = lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 3)
        .collect();
    words.iter().any(|w| {
        title_words
            .iter()
            .any(|t| t.starts_with(w.as_str()) || w.starts_with(t))
    })
}

fn is_html(c: &Checked) -> bool {
    let ct = c.content_type.to_ascii_lowercase();
    ct.starts_with("text/html") || ct.starts_with("application/xhtml")
}

/// Why a redirect's target fails, if it does.
fn judge_target(url: &str, seen: &Seen, problems: &mut Vec<String>, notes: &mut Vec<String>) {
    let c = match &seen.result {
        Err(m) => {
            problems.push(format!("`{url}` could not be checked: {m}"));
            return;
        }
        Ok(c) => c,
    };
    let Some(first) = c.hops.first() else {
        problems.push(format!(
            "`{url}` gave no answer{}",
            c.stopped
                .as_ref()
                .map(|w| format!(" ({w})"))
                .unwrap_or_default()
        ));
        return;
    };
    match first.status {
        200 => {}
        s @ 300..=399 => {
            let to = c.hops.get(1).map(|h| h.url.as_str()).unwrap_or("elsewhere");
            problems.push(format!(
                "`{url}` redirects again ({s} to `{to}`): point the entry at the final page"
            ));
            return;
        }
        s => {
            problems.push(format!("`{url}` returns {s}, not 200"));
            return;
        }
    }
    match &c.title {
        None if is_html(c) => problems.push(format!("`{url}` has no page title")),
        Some(t) if !title_matches(url, t) => notes.push(format!(
            "`{url}` is titled \"{t}\", which doesn't name its slug: confirm it is the page the ticket means"
        )),
        _ => {}
    }
}

/// A link's target must load: 2xx at the end of its redirects.
fn judge_link(url: &str, seen: &Seen, problems: &mut Vec<String>) {
    match &seen.result {
        Err(m) => problems.push(format!("`{url}` could not be checked: {m}")),
        Ok(c) => match (c.status(), &c.stopped) {
            (_, Some(why)) => problems.push(format!("`{url}` didn't finish loading: {why}")),
            (Some(s), None) if (200..300).contains(&s) => {}
            (Some(s), None) => problems.push(format!("`{url}` returns {s}")),
            (None, None) => problems.push(format!("`{url}` gave no answer")),
        },
    }
}

/// One URL's answer: from `cache` when it was asked already.
async fn fetch(cache: &mut HashMap<String, Seen>, checker: &dyn Checker, url: &str) -> Seen {
    if let Some(s) = cache.get(url) {
        return s.clone();
    }
    let result = if cache.len() >= MAX_URLS {
        Err(format!("over the {MAX_URLS}-URL limit for one change"))
    } else {
        checker.check(url).await.map_err(|e| e.text().to_string())
    };
    let s = Seen {
        url: url.to_string(),
        at: Utc::now(),
        result,
    };
    cache.insert(url.to_string(), s.clone());
    s
}

/// Checks every entry: each source as it is now, each target, each old
/// target. A URL is requested once; at most [`MAX_URLS`] in all.
pub async fn run_checks(
    entries: &[Entry],
    sites: &[Site],
    domains: &[String],
    checker: &dyn Checker,
) -> Report {
    let mut cache: HashMap<String, Seen> = HashMap::new();
    let mut rows = Vec::new();
    for e in entries {
        let mut row = Row {
            entry: e.clone(),
            before: Vec::new(),
            target: None,
            old: None,
            problems: Vec::new(),
            notes: Vec::new(),
        };
        if e.kind == Kind::Redirect {
            match source_urls(e, sites, domains) {
                None => row.notes.push(format!(
                    "Source `{}` is a pattern: check what it matches by hand",
                    e.source.clone().unwrap_or_default()
                )),
                Some(urls) if urls.is_empty() => row.problems.push(format!(
                    "No site is known for source `{}` in `{}`: name it in a sites.json note, or list the domain in the team's review settings",
                    e.source.clone().unwrap_or_default(),
                    e.file
                )),
                Some(urls) => {
                    for u in urls {
                        let s = fetch(&mut cache, checker, &u).await;
                        if let Err(m) = &s.result {
                            row.problems
                                .push(format!("Source `{u}` could not be checked: {m}"));
                        }
                        row.before.push(s);
                    }
                }
            }
        }
        match (literal(&e.target), target_url(&e.target, e, sites, domains)) {
            (None, _) => row.notes.push(format!(
                "Target `{}` is a pattern: check what it resolves to by hand",
                e.target
            )),
            (Some(_), None) => row.problems.push(format!(
                "No site is known for target `{}` in `{}`: name it in a sites.json note, or list the domain in the team's review settings",
                e.target, e.file
            )),
            (Some(_), Some(u)) => {
                let s = fetch(&mut cache, checker, &u).await;
                match e.kind {
                    Kind::Redirect => judge_target(&u, &s, &mut row.problems, &mut row.notes),
                    Kind::Link => judge_link(&u, &s, &mut row.problems),
                }
                row.target = Some(s);
            }
        }
        if let Some(u) = e
            .old_target
            .as_deref()
            .and_then(|t| target_url(t, e, sites, domains))
        {
            let s = fetch(&mut cache, checker, &u).await;
            row.old = Some(s);
        }
        rows.push(row);
    }
    Report {
        rows,
        checked_at: Utc::now(),
        sites: sites.to_vec(),
    }
}

fn cell(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

fn when(at: &DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M UTC").to_string()
}

impl Report {
    /// Every reason the change is blocked.
    pub fn blocking(&self) -> Vec<String> {
        self.rows
            .iter()
            .flat_map(|r| r.problems.iter().cloned())
            .collect()
    }

    /// The description's section: a dated before-and-after table, what
    /// blocks, what a person should confirm, and (hidden) when it was
    /// checked and the sites it used, for the next check. Empty with no
    /// entries.
    pub fn section(&self) -> String {
        if self.rows.is_empty() {
            return String::new();
        }
        let mut s = format!(
            "{START}\n## {HEADING}\nChecked live by CoderHelm on {}. **Before** is what each source answers now; **After** is where this PR points it, and what that page answers.\n\n| Entry | Before (live now) | After (this PR) | Checked |\n|---|---|---|---|\n",
            when(&self.checked_at)
        );
        for r in &self.rows {
            let e = &r.entry;
            let entry = match (&e.kind, &e.source) {
                (Kind::Redirect, Some(src)) => {
                    let map = if e.block.is_empty() {
                        String::new()
                    } else {
                        format!(", `{}`", e.block)
                    };
                    format!("`{src}` (`{}`{map})", e.file)
                }
                _ => format!("link in `{}`", e.file),
            };
            let before = if r.before.is_empty() {
                "—".to_string()
            } else {
                r.before
                    .iter()
                    .map(|b| format!("`{}`: {}", b.url, b.first_hop()))
                    .collect::<Vec<_>>()
                    .join("<br>")
            };
            let mut after = format!("`{}`", e.target);
            if let Some(t) = &r.target {
                after.push_str(&format!(": {}", t.page()));
            }
            if let (Some(old), Some(o)) = (&e.old_target, &r.old) {
                after.push_str(&format!("<br>was `{old}`: {}", o.page()));
            }
            let at = r
                .target
                .as_ref()
                .map(|t| t.at)
                .or_else(|| r.before.first().map(|b| b.at))
                .unwrap_or(self.checked_at);
            s.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                cell(&entry),
                cell(&before),
                cell(&after),
                at.format("%Y-%m-%d %H:%M")
            ));
        }
        let blocking = self.blocking();
        if !blocking.is_empty() {
            s.push_str("\n**Blocking:**\n");
            for b in &blocking {
                s.push_str(&format!("- {b}\n"));
            }
        }
        let notes: Vec<&String> = self.rows.iter().flat_map(|r| r.notes.iter()).collect();
        if !notes.is_empty() {
            s.push_str("\n**For a person to confirm:**\n");
            for n in notes {
                s.push_str(&format!("- {n}\n"));
            }
        }
        s.push_str(&format!(
            "\n{CHECKED_AT}{} -->\n",
            self.checked_at.to_rfc3339()
        ));
        if !self.sites.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.sites) {
                s.push_str(&format!(
                    "{SITES_MARK}{} -->\n",
                    json.replace("--", "\\u002d-")
                ));
            }
        }
        s.push_str(END);
        s
    }
}

/// The section's text in a description, when it has one.
fn section_of(body: &str) -> Option<&str> {
    let s = body.find(START)?;
    let e = body[s..].find(END)? + s;
    Some(&body[s..e])
}

/// When the description's checks ran.
pub fn checked_at(body: &str) -> Option<DateTime<Utc>> {
    let sec = section_of(body)?;
    let at = sec.find(CHECKED_AT)? + CHECKED_AT.len();
    let end = sec[at..].find(" -->")? + at;
    DateTime::parse_from_rfc3339(sec[at..end].trim())
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// Whether the checks are more than [`STALE_AFTER_HOURS`] old (or missing).
pub fn is_stale(body: &str, now: DateTime<Utc>) -> bool {
    checked_at(body).is_none_or(|at| now - at > chrono::Duration::hours(STALE_AFTER_HOURS))
}

/// The sites the last check used.
pub fn stored_sites(body: &str) -> Vec<Site> {
    let Some(sec) = section_of(body) else {
        return Vec::new();
    };
    let Some(at) = sec.find(SITES_MARK).map(|i| i + SITES_MARK.len()) else {
        return Vec::new();
    };
    let Some(end) = sec[at..].find(" -->").map(|i| i + at) else {
        return Vec::new();
    };
    serde_json::from_str(&sec[at..end]).unwrap_or_default()
}

// ─── What the description may claim ─────────────────────────

/// A statement the description makes that the diff doesn't bear out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// The path, URL or file named.
    pub token: String,
    pub reason: String,
}

fn changed_files(diff: &Value) -> Vec<String> {
    diff["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn patch_text(diff: &Value) -> String {
    diff["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["patch"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// A token that names a file in the repository: `dir/name.ext`.
fn file_like(t: &str) -> bool {
    if t.starts_with('/')
        || t.starts_with('.')
        || t.contains("://")
        || t.chars()
            .any(|c| c.is_whitespace() || matches!(c, '*' | '{' | '<' | '$' | '(' | '='))
        || !t.contains('/')
    {
        return false;
    }
    let last = t.rsplit('/').next().unwrap_or_default();
    last.rsplit_once('.').is_some_and(|(stem, ext)| {
        !stem.is_empty()
            && (1..=6).contains(&ext.len())
            && ext.chars().all(|c| c.is_ascii_alphanumeric())
    })
}

/// The lines of the description's Changes section.
fn changes_section(body: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut inside = false;
    for l in body.lines() {
        let t = l.trim();
        let heading = t.starts_with('#')
            || (t.starts_with("**") && t.trim_end_matches(':').ends_with("**"))
            || t.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.')
                .trim_start()
                .starts_with("**")
                && !t.starts_with("* ");
        if heading {
            let name = t
                .trim_start_matches(|c: char| c == '#' || c.is_ascii_digit() || c == '.')
                .trim()
                .trim_start_matches("**")
                .to_ascii_lowercase();
            if name.starts_with("changes") {
                inside = true;
                continue;
            }
            if t.starts_with('#')
                || name.starts_with("risk")
                || name.starts_with("verification")
                || name.starts_with("summary")
            {
                inside = false;
            }
        }
        if inside {
            out.push(l);
        }
    }
    out
}

/// What the description says that the diff doesn't contain: a redirect or
/// link path or URL that isn't in the diff (when the diff changes
/// redirects or links), and a file named under Changes that the diff doesn't
/// change. CoderHelm's own sections are left out (they're written from the
/// diff).
pub fn claim_violations(body: &str, diff: &Value) -> Vec<Claim> {
    let (_, writer) = super::completeness::take_owned(body);
    let mut out: Vec<Claim> = Vec::new();
    let mut push = |c: Claim| {
        if !out.iter().any(|o| o.token == c.token) {
            out.push(c);
        }
    };
    let files = changed_files(diff);
    for line in changes_section(&writer) {
        for c in CODE_SPAN.captures_iter(line) {
            let t = c[1].trim();
            if file_like(t)
                && !files
                    .iter()
                    .any(|f| f == t || f.ends_with(&format!("/{t}")))
            {
                push(Claim {
                    token: t.to_string(),
                    reason: format!("names `{t}`, which this diff doesn't change"),
                });
            }
        }
    }
    let entries = entries(diff);
    if entries.is_empty() {
        return out;
    }
    let text = patch_text(diff);
    // Hosts the change's own URLs are on: a URL elsewhere (a ticket, docs)
    // isn't an entry.
    let hosts: HashSet<String> = entries
        .iter()
        .flat_map(|e| {
            [
                Some(e.target.as_str()),
                e.source.as_deref(),
                e.old_target.as_deref(),
            ]
        })
        .flatten()
        .filter_map(|v| v.starts_with("http").then(|| origin(v)).flatten())
        .collect();
    let mut tokens: Vec<String> = CODE_SPAN
        .captures_iter(&writer)
        .map(|c| c[1].trim().to_string())
        .filter(|t| url_ish(t))
        .collect();
    tokens.extend(BARE_URL.find_iter(&writer).map(|m| m.as_str().to_string()));
    for raw in tokens {
        let t = raw.trim_end_matches(['.', ',', ';', ':']).to_string();
        if text.contains(&t) {
            continue;
        }
        if t.starts_with("http") {
            let Some(o) = origin(&t) else { continue };
            if !hosts.contains(&o) {
                continue;
            }
            let rest = &t[t.find("://").map_or(0, |i| i + 3)..];
            let path = rest.find('/').map_or("/", |i| &rest[i..]);
            if text.contains(path) {
                continue;
            }
        }
        push(Claim {
            token: t.clone(),
            reason: format!("names `{t}`, which isn't in this diff"),
        });
    }
    out
}

/// What the writer is told when the description claims what the diff
/// doesn't contain.
pub fn revise_prompt(claims: &[Claim]) -> String {
    format!(
        "The description names things this pull request's diff doesn't contain:\n{}\n\nRewrite it so it describes only the entries and files in the diff. Don't describe an entry, file or behaviour the diff doesn't have. Return ONLY the full markdown body again.",
        claims
            .iter()
            .map(|c| format!("- {}", c.reason))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// `body` without the lines that still name something the diff doesn't
/// contain.
pub fn strip_claims(body: &str, claims: &[Claim]) -> String {
    body.lines()
        .filter(|l| !claims.iter().any(|c| l.contains(&c.token)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Checks a description against its diff: one revision by the writer with
/// the reasons, then any line still naming something the diff doesn't
/// contain is removed in code.
#[allow(clippy::too_many_arguments)]
pub async fn enforce_claims(
    state: &WorkerState,
    provider: &crate::agent::provider::ModelProvider,
    system: &str,
    messages: &mut Vec<(String, Vec<Value>)>,
    opts: crate::agent::llm::ConverseOptions,
    usage: &mut crate::models::TokenUsage,
    diff: &Value,
    body: String,
) -> String {
    let first = claim_violations(&body, diff);
    if first.is_empty() {
        return body;
    }
    info!(
        claims = first.len(),
        "Description names what the diff doesn't contain; revising once"
    );
    messages.push((
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": revise_prompt(&first)})],
    ));
    let revised = match crate::agent::provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Coder,
        provider.primary_model_id(),
        system,
        messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        opts,
        None,
        None,
    )
    .await
    {
        Ok(t) if !t.trim().is_empty() => t,
        Ok(_) | Err(_) => body,
    };
    let left = claim_violations(&revised, diff);
    if left.is_empty() {
        return revised;
    }
    warn!(claims = ?left, "Description still names what the diff doesn't contain; removing those lines");
    strip_claims(&revised, &left)
}

// ─── Running it ─────────────────────────────────────────────

/// `check_url`'s client, run by the pipeline for one change.
struct Live<'a> {
    web: super::web_check::WebCheck,
    state: &'a WorkerState,
    github: &'a GitHubClient,
}

#[async_trait::async_trait]
impl Checker for Live<'_> {
    async fn check(&self, url: &str) -> Result<Checked, CheckError> {
        self.web.verify(self.state, self.github, url).await
    }
}

/// Everything a check of one change needs.
struct Change<'a> {
    state: &'a WorkerState,
    github: &'a GitHubClient,
    team_id: &'a str,
    owner: &'a str,
    repo: &'a str,
    pr: u64,
    run_id: &'a str,
    origin: String,
    /// Text whose URLs' hosts may be checked (the diff, the ticket); any
    /// other host must be one of the team's.
    work: String,
}

async fn check_change(c: &Change<'_>, entries: &[Entry], sites: &[Site]) -> Report {
    let domains = super::web_check::team_domains(c.state, c.team_id).await;
    let team_repos = super::plan::fetch_team_repos(c.state, c.team_id).await;
    let web = super::web_check::WebCheck::new(
        c.team_id,
        &format!("{}/{}", c.owner, c.repo),
        "coder",
        c.pr,
        domains.clone(),
        &c.work,
        team_repos,
    )
    .with_run(c.run_id, c.origin.clone())
    .with_limit(MAX_URLS);
    let live = Live {
        web,
        state: c.state,
        github: c.github,
    };
    run_checks(entries, sites, &domains, &live).await
}

/// What the coder is told when the checks block the change.
fn fix_feedback(report: &Report) -> String {
    format!(
        "The link and redirect check failed for these entries. Fix each one: check the right page with `check_url` (a 200 with a title that is the page the ticket means) and point the entry at it, or name the site its map serves in a sites.json block in your closing notes.\n{}",
        report
            .blocking()
            .iter()
            .map(|b| format!("- {b}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// The check before a pull request opens: every redirect and link the
/// change adds or changes, checked live. A failure gets one fix round; what
/// still fails stops the run with the reasons (`Err`). `Ok` carries the
/// description's section (empty when the change has no such entries).
pub async fn gate(
    w: &super::proof::Work<'_>,
    evidence: &mut super::proof::RunEvidence,
    usage: &mut crate::models::TokenUsage,
    has_time: impl Fn() -> bool,
) -> Result<String, String> {
    let mut rounds = 0;
    loop {
        let diff = w
            .github
            .get_diff(
                &w.msg.repo_owner,
                &w.msg.repo_name,
                &w.msg.base_branch,
                w.branch,
            )
            .await
            .map_err(|e| format!("the diff couldn't be read to check its links ({e})"))?;
        let found = entries(&diff);
        if found.is_empty() {
            return Ok(String::new());
        }
        let sites = parse_sites(&evidence.notes);
        let change = Change {
            state: w.state,
            github: w.github,
            team_id: &w.msg.team_id,
            owner: &w.msg.repo_owner,
            repo: &w.msg.repo_name,
            pr: 0,
            run_id: w.run_id,
            origin: super::skills::ticket_origin(
                &w.msg.repo_owner,
                &w.msg.repo_name,
                &w.msg.ticket_id,
                w.msg.issue_number,
            ),
            // Hosts named in the ticket or the diff; a host the coder names
            // only in its notes must be one of the team's (listed, or used
            // in its repositories), like any other check.
            work: format!("{}\n{}\n{}", w.msg.title, w.msg.body, patch_text(&diff)),
        };
        let report = check_change(&change, &found, &sites).await;
        let blocking = report.blocking();
        info!(
            run_id = w.run_id,
            entries = found.len(),
            blocking = blocking.len(),
            rounds,
            "Link and redirect check complete"
        );
        if blocking.is_empty() {
            super::add_progress_note(
                w.state,
                &w.msg.team_id,
                w.run_id,
                &format!(
                    "Link and redirect check passed: {} entr{} checked live",
                    found.len(),
                    if found.len() == 1 { "y" } else { "ies" }
                ),
            )
            .await;
            return Ok(report.section());
        }
        if rounds >= MAX_FIX_ROUNDS || !has_time() {
            return Err(blocking.join("; "));
        }
        rounds += 1;
        super::add_progress_note(
            w.state,
            &w.msg.team_id,
            w.run_id,
            &format!(
                "Link and redirect check found {} problem(s), fixing",
                blocking.len()
            ),
        )
        .await;
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
        let fixed = super::implement::run(
            w.state,
            w.msg,
            w.github,
            w.plan,
            w.branch,
            w.rules,
            w.repo_instructions,
            Some(&fix_feedback(&report)),
            "medium",
            w.provider,
            usage,
            w.file_cache,
            Some(w.run_id),
            None,
            true, // bot-driven link fix — scoped to the PR
        )
        .await;
        match fixed {
            Ok(r) => evidence.merge(r.evidence),
            Err(e) => {
                return Err(format!(
                    "{} (the fix round failed: {e})",
                    blocking.join("; ")
                ))
            }
        }
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "review").await;
    }
}

/// What a check of an open pull request found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refreshed {
    pub checked_at: DateTime<Utc>,
    pub blocking: Vec<String>,
}

/// Checks an open pull request's redirects and links again and rewrites its
/// section of the description, when `force` (a push, a request to merge) or
/// when the evidence there is more than a day old. A diff with no entries
/// drops the section. `None` when nothing was checked.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_pr(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr_number: u64,
    run_id: &str,
    force: bool,
) -> Option<Refreshed> {
    let pr = github.get_pull_request(owner, repo, pr_number).await.ok()?;
    let body = pr["body"].as_str().unwrap_or("").to_string();
    let had = section_of(&body).is_some();
    if had && !force && !is_stale(&body, Utc::now()) {
        return None;
    }
    let base = pr["base"]["ref"].as_str()?;
    let head = pr["head"]["ref"].as_str()?;
    let diff = github.get_diff(owner, repo, base, head).await.ok()?;
    let found = entries(&diff);
    if found.is_empty() {
        if had {
            let without = super::completeness::with_section(
                &body,
                super::completeness::Section::Links,
                String::new(),
            );
            if let Err(e) = github
                .update_pr_body(owner, repo, pr_number, &without)
                .await
            {
                warn!(pr_number, error = %e, "Could not drop the link checks from the description");
            }
        }
        return None;
    }
    let sites = stored_sites(&body);
    let change = Change {
        state,
        github,
        team_id,
        owner,
        repo,
        pr: pr_number,
        run_id,
        origin: format!("{owner}/{repo}#{pr_number}"),
        work: patch_text(&diff),
    };
    let report = check_change(&change, &found, &sites).await;
    let updated = super::completeness::with_section(
        &body,
        super::completeness::Section::Links,
        report.section(),
    );
    if updated != body {
        if let Err(e) = github
            .update_pr_body(owner, repo, pr_number, &updated)
            .await
        {
            warn!(pr_number, error = %e, "Could not save the link checks to the description");
            return None;
        }
    }
    info!(
        pr_number,
        entries = found.len(),
        blocking = report.blocking().len(),
        "Link and redirect checks re-run on the open PR"
    );
    Some(Refreshed {
        checked_at: report.checked_at,
        blocking: report.blocking(),
    })
}

// ─── Replies ────────────────────────────────────────────────

/// A reply that only says it's done: "Done", "Fixed.", "Addressed 👍".
pub fn is_bare_ack(reply: &str) -> bool {
    let words: Vec<String> = reply
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_ascii_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    const ACKS: &[&str] = &[
        "done",
        "fixed",
        "addressed",
        "updated",
        "resolved",
        "ok",
        "okay",
        "applied",
        "changed",
        "yes",
        "sure",
        "thanks",
        "thank",
        "you",
        "good",
        "catch",
        "now",
        "it",
        "this",
        "is",
    ];
    !words.is_empty() && words.len() <= 4 && words.iter().all(|w| ACKS.contains(&w.as_str()))
}

/// A review reply as posted: a bare acknowledgement is replaced by where
/// the evidence is (the description's checks, or the commit), and a reply
/// on a PR whose checks ran this round gains one line pointing to them.
pub fn evidenced_reply(
    reply: &str,
    links: Option<&Refreshed>,
    pr_url: &str,
    pushed: Option<&str>,
) -> String {
    let pointer = links.map(|l| {
        let state = if l.blocking.is_empty() {
            "all pass".to_string()
        } else {
            format!("{} still failing", l.blocking.len())
        };
        format!(
            "Evidence: [{HEADING}]({pr_url}#{ANCHOR}) in the description, checked {} ({state}).",
            when(&l.checked_at)
        )
    });
    if is_bare_ack(reply) {
        return match (pointer, pushed) {
            (Some(p), _) => p,
            (None, Some(sha)) => format!(
                "Pushed in `{}`; the description is updated to match the diff.",
                &sha[..sha.len().min(7)]
            ),
            (None, None) => reply.to_string(),
        };
    }
    match pointer {
        Some(p) if !reply.contains(ANCHOR) => format!("{}\n\n{p}", reply.trim_end()),
        _ => reply.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::web_check::Hop;
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    fn diff(files: &[(&str, &str)]) -> Value {
        json!({"files": files.iter().map(|(f, p)| json!({"filename": f, "patch": p})).collect::<Vec<_>>()})
    }

    #[test]
    fn hcl_map_change_reads_source_target_and_old_target() {
        let patch = "@@ -10,6 +10,7 @@ variable \"site_redirects\" {\n   default = {\n     \"/keep\" = \"https://www.example.com/keep\"\n-    \"/old-plan\" = \"https://www.example.com/change-plan-form\"\n+    \"/old-plan\" = \"https://www.example.com/old-plan\"\n+    \"/join\"   = \"/pricing\"\n   }\n";
        let e = file_entries("envs/global/redirects.tf", patch);
        assert_eq!(e.len(), 2, "{e:?}");
        assert_eq!(e[0].source.as_deref(), Some("/old-plan"));
        assert_eq!(e[0].target, "https://www.example.com/old-plan");
        assert_eq!(
            e[0].old_target.as_deref(),
            Some("https://www.example.com/change-plan-form")
        );
        assert_eq!(e[0].block, "site_redirects");
        assert_eq!(e[1].source.as_deref(), Some("/join"));
        assert_eq!(e[1].old_target, None);
    }

    #[test]
    fn json_yaml_js_nginx_and_object_formats_are_read() {
        let cases: &[(&str, &str, &str, &str)] = &[
            ("r.json", "+  \"/a\": \"/b\",\n", "/a", "/b"),
            (
                "r.yaml",
                "+/a: https://x.example.com/b\n",
                "/a",
                "https://x.example.com/b",
            ),
            ("edge.js", "+  '/a': '/b',\n", "/a", "/b"),
            ("site.conf", "+    /a /b;\n", "/a", "/b"),
            (
                "site.conf",
                "+    rewrite ^/a/?$ /b permanent;\n",
                "^/a/?$",
                "/b",
            ),
            (
                "next.config.js",
                "+  { source: '/a', destination: '/b', permanent: true },\n",
                "/a",
                "/b",
            ),
            (
                "vercel.json",
                "+    {\n+      \"source\": \"/a\",\n+      \"destination\": \"/b\"\n+    }\n",
                "/a",
                "/b",
            ),
            (
                "nginx.conf",
                "+  location = /a {\n+    return 301 https://x.example.com/b;\n+  }\n",
                "/a",
                "https://x.example.com/b",
            ),
        ];
        for (file, patch, src, dst) in cases {
            let e = file_entries(file, patch);
            assert_eq!(e.len(), 1, "{file}: {e:?}");
            assert_eq!(e[0].source.as_deref(), Some(*src), "{file}");
            assert_eq!(e[0].target, *dst, "{file}");
        }
    }

    #[test]
    fn ordinary_config_and_unchanged_entries_are_not_entries() {
        let patch = " \"/a\" = \"/b\"\n+name = \"service\"\n+path = \"/var/lib/app\"\n+  \"timeout\": \"30s\",\n+# \"/x\" = \"/y\"\n";
        assert!(file_entries("main.tf", patch).is_empty());
    }

    #[test]
    fn markdown_links_added_are_entries_and_kept_ones_are_not() {
        let patch = " See [old](https://docs.example.com/old).\n+See [new](https://docs.example.com/new) and [repo](./README.md).\n";
        let e = file_entries("docs/guide.md", patch);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].kind, Kind::Link);
        assert_eq!(e[0].target, "https://docs.example.com/new");
    }

    #[test]
    fn patterns_are_not_literal_and_anchors_come_off() {
        assert_eq!(literal("^/a/?$").as_deref(), Some("/a"));
        assert_eq!(literal("/blog/(.*)"), None);
        assert_eq!(literal("/b/$1"), None);
        assert_eq!(literal("/a?x=1").as_deref(), Some("/a?x=1"));
    }

    #[test]
    fn sites_name_hosts_for_relative_paths() {
        let notes = "Done.\n```sites.json\n[{\"file\": \"redirects.tf\", \"map\": \"site_redirects\", \"sources\": [\"https://go.example.com\", \"staging-go.example.com\"], \"targets\": \"https://www.example.com\"}]\n```\n";
        let sites = parse_sites(notes);
        let e = Entry {
            file: "envs/global/redirects.tf".into(),
            block: "site_redirects".into(),
            kind: Kind::Redirect,
            source: Some("/join".into()),
            target: "/pricing".into(),
            old_target: None,
        };
        assert_eq!(
            source_hosts(&e, &sites, &[]),
            vec!["https://go.example.com", "https://staging-go.example.com"]
        );
        assert_eq!(
            target_url("/pricing", &e, &sites, &[]).as_deref(),
            Some("https://www.example.com/pricing")
        );
        // Another map in the file isn't covered by that site.
        let other = Entry {
            block: "other_redirects".into(),
            ..e.clone()
        };
        assert!(source_hosts(&other, &sites, &[]).is_empty());
        // With one listed domain and no site, that domain.
        assert_eq!(
            source_hosts(&other, &[], &["example.com".into()]),
            vec!["https://example.com"]
        );
        assert!(source_hosts(&other, &[], &["a.com".into(), "b.com".into()]).is_empty());
    }

    #[test]
    fn titles_match_slugs() {
        assert!(title_matches(
            "https://www.example.com/plan-change",
            "Plan Change | Example"
        ));
        assert!(title_matches(
            "https://www.example.com/locations/",
            "Our Location"
        ));
        assert!(!title_matches(
            "https://www.example.com/plan-change",
            "Page not found"
        ));
        assert!(title_matches("https://www.example.com/", "Home"));
    }

    /// Scripted answers: url → (hops, title, content type).
    struct Script {
        pages: HashMap<String, Result<Checked, CheckError>>,
        asked: Mutex<Vec<String>>,
    }

    fn page(url: &str, status: u16, title: Option<&str>) -> Checked {
        Checked {
            hops: vec![Hop {
                url: url.into(),
                status,
            }],
            final_url: url.into(),
            content_type: "text/html".into(),
            title: title.map(String::from),
            ..Default::default()
        }
    }

    fn hop_to(url: &str, status: u16, to: &str) -> Checked {
        Checked {
            hops: vec![
                Hop {
                    url: url.into(),
                    status,
                },
                Hop {
                    url: to.into(),
                    status: 200,
                },
            ],
            final_url: to.into(),
            content_type: "text/html".into(),
            title: Some("Somewhere".into()),
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl Checker for Script {
        async fn check(&self, url: &str) -> Result<Checked, CheckError> {
            self.asked.lock().unwrap().push(url.to_string());
            self.pages
                .get(url)
                .cloned()
                .unwrap_or_else(|| Err(CheckError::Failed("not scripted".into())))
        }
    }

    fn script(pages: Vec<(&str, Result<Checked, CheckError>)>) -> Script {
        Script {
            pages: pages.into_iter().map(|(u, p)| (u.to_string(), p)).collect(),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn entry(src: &str, dst: &str, old: Option<&str>) -> Entry {
        Entry {
            file: "redirects.tf".into(),
            block: "site_redirects".into(),
            kind: Kind::Redirect,
            source: Some(src.into()),
            target: dst.into(),
            old_target: old.map(String::from),
        }
    }

    fn site() -> Vec<Site> {
        vec![Site {
            file: "redirects.tf".into(),
            map: String::new(),
            sources: vec!["https://go.example.com".into()],
            targets: "https://www.example.com".into(),
        }]
    }

    #[tokio::test]
    async fn a_good_change_passes_with_before_and_after_in_the_section() {
        let s = script(vec![
            (
                "https://go.example.com/old-plan",
                Ok(hop_to(
                    "https://go.example.com/old-plan",
                    301,
                    "https://www.example.com/change-plan-form",
                )),
            ),
            (
                "https://www.example.com/plan-change",
                Ok(page(
                    "https://www.example.com/plan-change",
                    200,
                    Some("Plan Change | Example"),
                )),
            ),
            (
                "https://www.example.com/change-plan-form",
                Ok(page(
                    "https://www.example.com/change-plan-form",
                    404,
                    Some("Not found"),
                )),
            ),
        ]);
        let r = run_checks(
            &[entry(
                "/old-plan",
                "/plan-change",
                Some("/change-plan-form"),
            )],
            &site(),
            &[],
            &s,
        )
        .await;
        assert!(r.blocking().is_empty(), "{:?}", r.blocking());
        let sec = r.section();
        assert!(sec.starts_with(START) && sec.ends_with(END));
        assert!(sec.contains(
            "`https://go.example.com/old-plan`: 301 → `https://www.example.com/change-plan-form`"
        ));
        assert!(sec.contains("`/plan-change`: 200 · \"Plan Change \\| Example\""));
        assert!(sec.contains("was `/change-plan-form`: 404"));
        assert!(sec.contains("Checked live by CoderHelm on "));
        // When it ran and the sites it used read back.
        let body = format!("Summary\n\n{sec}\n");
        assert_eq!(checked_at(&body), Some(r.checked_at));
        assert_eq!(stored_sites(&body), site());
        assert!(!is_stale(&body, r.checked_at + chrono::Duration::hours(23)));
        assert!(is_stale(&body, r.checked_at + chrono::Duration::hours(25)));
        assert!(is_stale("no section", Utc::now()));
    }

    #[tokio::test]
    async fn wrong_missing_or_unchecked_targets_block() {
        let s = script(vec![
            (
                "https://go.example.com/a",
                Ok(page("https://go.example.com/a", 404, None)),
            ),
            (
                "https://www.example.com/typo-slug",
                Ok(page(
                    "https://www.example.com/typo-slug",
                    404,
                    Some("Page not found"),
                )),
            ),
            (
                "https://www.example.com/moved",
                Ok(hop_to(
                    "https://www.example.com/moved",
                    301,
                    "https://www.example.com/final",
                )),
            ),
            (
                "https://www.example.com/private",
                Err(CheckError::Refused("non-public address".into())),
            ),
        ]);
        let r = run_checks(
            &[
                entry("/a", "/typo-slug", None),
                entry("/a", "/moved", None),
                entry("/a", "/private", None),
            ],
            &site(),
            &[],
            &s,
        )
        .await;
        let b = r.blocking();
        assert_eq!(b.len(), 3, "{b:?}");
        assert!(b[0].contains("returns 404"));
        assert!(b[1].contains("redirects again (301"));
        assert!(b[2].contains("could not be checked: non-public address"));
        assert!(r.section().contains("**Blocking:**"));
        // The source was requested once for three entries.
        let asked = s.asked.lock().unwrap();
        assert_eq!(asked.iter().filter(|u| u.ends_with("/a")).count(), 1);
    }

    #[tokio::test]
    async fn no_known_site_blocks_and_patterns_go_to_a_person() {
        let s = script(vec![]);
        let r = run_checks(&[entry("/a", "/b", None)], &[], &[], &s).await;
        assert!(r
            .blocking()
            .iter()
            .any(|b| b.contains("No site is known for source `/a`")));
        let r = run_checks(&[entry("/blog/(.*)", "/news/$1", None)], &site(), &[], &s).await;
        assert!(r.blocking().is_empty());
        assert_eq!(r.rows[0].notes.len(), 2);
        assert!(s.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_title_that_doesnt_name_the_page_is_for_a_person() {
        let s = script(vec![
            (
                "https://go.example.com/a",
                Ok(page("https://go.example.com/a", 404, None)),
            ),
            (
                "https://www.example.com/plan-change",
                Ok(page(
                    "https://www.example.com/plan-change",
                    200,
                    Some("Welcome"),
                )),
            ),
        ]);
        let r = run_checks(&[entry("/a", "/plan-change", None)], &site(), &[], &s).await;
        assert!(r.blocking().is_empty());
        assert!(r.section().contains("**For a person to confirm:**"));
    }

    #[test]
    fn claims_about_entries_or_files_outside_the_diff_are_found() {
        let d = diff(&[(
            "envs/global/redirects.tf",
            "@@ -1 +1 @@\n-  \"/old-plan\" = \"https://www.example.com/change-plan-form\"\n+  \"/old-plan\" = \"https://www.example.com/old-plan\"\n",
        )]);
        let body = "## Summary\nPoints `/old-plan` at the right page.\n\n## Changes\n* `envs/global/redirects.tf`: `/old-plan` now goes to https://www.example.com/old-plan.\n* Adds `/old-plan?reload=1` too.\n* Updates `envs/prod/main.tf`.\n\n## Verification\n1. Open https://github.com/acme/infra/pull/1 and https://www.example.com/old-plan.\n";
        let v = claim_violations(body, &d);
        let tokens: Vec<&str> = v.iter().map(|c| c.token.as_str()).collect();
        assert_eq!(
            tokens,
            vec!["envs/prod/main.tf", "/old-plan?reload=1"],
            "{v:?}"
        );
        let stripped = strip_claims(body, &v);
        assert!(!stripped.contains("reload=1") && !stripped.contains("envs/prod/main.tf"));
        assert!(stripped.contains("now goes to"));
        // CoderHelm's own section is never read as the writer's claim.
        let with_section = format!("{body}\n{START}\n## {HEADING}\n`/elsewhere`\n{END}");
        assert_eq!(claim_violations(&with_section, &d).len(), 2);
    }

    #[test]
    fn claims_check_on_a_diff_without_entries_reads_only_files() {
        let d = diff(&[("src/lib.rs", "@@ -1 +1 @@\n-a\n+b\n")]);
        let body = "## Changes\n* `src/lib.rs` changed; see `/health`.\n";
        assert!(claim_violations(body, &d).is_empty());
    }

    #[test]
    fn bare_acknowledgements_are_replaced_with_evidence() {
        assert!(is_bare_ack("Done."));
        assert!(is_bare_ack("Fixed, thanks!"));
        assert!(!is_bare_ack(
            "Done — the target is now `/old-plan`, which returns 200."
        ));
        let at = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        let links = Refreshed {
            checked_at: at,
            blocking: vec![],
        };
        let r = evidenced_reply("Done", Some(&links), "https://github.com/o/r/pull/1", None);
        assert_eq!(
            r,
            "Evidence: [Link and redirect checks](https://github.com/o/r/pull/1#link-and-redirect-checks) in the description, checked 2026-01-02 03:04 UTC (all pass)."
        );
        let r = evidenced_reply("Pointed `/a` at `/b`.", Some(&links), "u", None);
        assert!(r.starts_with("Pointed `/a` at `/b`.\n\nEvidence: "));
        assert_eq!(
            evidenced_reply("Done!", None, "u", Some("abcdef1234")),
            "Pushed in `abcdef1`; the description is updated to match the diff."
        );
        assert_eq!(
            evidenced_reply("Answered: yes.", None, "u", None),
            "Answered: yes."
        );
    }
}
