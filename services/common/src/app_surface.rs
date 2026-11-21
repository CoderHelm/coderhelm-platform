//! What one file says about how an app fits together: the endpoints a
//! server answers, the web requests a client makes, the screens an app has,
//! the packages it pulls in and the settings it names.
//!
//! The code graph's indexer reads every file once per pass; it calls
//! [`scan`] on the files [`wants`] and keeps the result beside the file's
//! symbols (`AF#{path}`), so the app map (`crate::app_map`) is built from
//! the graph without reading a repository again.
//!
//! Detection is by pattern, per framework, and says how it knows:
//!
//! - endpoints: Next.js route handlers and pages API files, Express,
//!   Fastify, Koa and Hono routes, NestJS controllers, axum and actix,
//!   FastAPI, Flask and Django, Rails `config/routes.rb`, Go `net/http`,
//!   gin, chi and echo, and Serverless and SAM templates;
//! - screens: Next.js `app/` and `pages/` routes, Expo Router files, React
//!   Router routes and React Navigation screens;
//! - requests: `fetch`, axios and other HTTP clients, generated clients
//!   (`client.GET("/x")`), `useSWR`, `requests`/`httpx`, `reqwest` and Go's
//!   `http` package. A request whose address is built at run time is kept
//!   with `dynamic` set, never guessed;
//! - packages from imports and manifests, and setting names (never values)
//!   from `.env.example`-style files.
//!
//! Every path is normalized to one shape (`/orders/:id`) so a request can be
//! matched to the endpoint that answers it.

// Every pattern is compiled once, on first use, by `re!` (a `OnceLock`
// per call site): a `re!` inside a loop is a lookup, not a compile.
#![allow(clippy::regex_creation_in_loops)]

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Bumped when detection changes, so graphs built before it are read again.
pub const VERSION: u32 = 1;
/// At most this many items of each kind from one file.
pub const MAX_ITEMS: usize = 200;
/// Files larger than this are not scanned (generated bundles).
pub const MAX_SOURCE_BYTES: usize = 400_000;

/// An endpoint a server answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// `GET`, `POST`, ... or `ANY`.
    pub method: String,
    /// Normalized path template (`/orders/:id`).
    pub path: String,
    /// How it was found, in plain words ("Express route").
    pub framework: String,
    pub line: u32,
}

/// A web request a client makes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub method: String,
    /// Normalized path (`/orders/:p`); empty when the address is built at
    /// run time.
    pub path: String,
    /// What the address starts with: a setting's name
    /// (`NEXT_PUBLIC_API_URL`) or a variable (`apiBase`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base: String,
    /// The host of an absolute address (`api.stripe.com`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
    pub line: u32,
    /// What made it (`fetch`, `axios`, `generated client`, `helper apiGet`).
    pub via: String,
    /// The address is built at run time: nothing to match.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dynamic: bool,
}

/// A screen of a web or mobile app.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Screen {
    /// The route (`/orders/:id`) or, for React Navigation, the screen name.
    pub route: String,
    pub framework: String,
    pub line: u32,
}

/// A router mounted under a prefix (`app.use("/api", orders)`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    pub prefix: String,
    /// The mounted router's name as written.
    pub target: String,
    pub line: u32,
}

/// Everything one file says.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Surface {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<Endpoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<Request>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub screens: Vec<Screen>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Packages imported or depended on, as named (`stripe`, `@aws-sdk/client-s3`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub packages: Vec<String>,
    /// Setting names (`STRIPE_SECRET_KEY`), never their values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    /// A manifest's own package name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub package_name: String,
}

impl Surface {
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
            && self.requests.is_empty()
            && self.screens.is_empty()
            && self.mounts.is_empty()
            && self.packages.is_empty()
            && self.env.is_empty()
            && self.package_name.is_empty()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }

    fn finish(mut self) -> Self {
        self.endpoints.truncate(MAX_ITEMS);
        self.requests.truncate(MAX_ITEMS);
        self.screens.truncate(MAX_ITEMS);
        self.mounts.truncate(MAX_ITEMS);
        self.packages.sort();
        self.packages.dedup();
        self.packages.truncate(MAX_ITEMS);
        self.env.sort();
        self.env.dedup();
        self.env.truncate(MAX_ITEMS);
        let mut seen = std::collections::HashSet::new();
        self.endpoints
            .retain(|e| seen.insert((e.method.clone(), e.path.clone())));
        let mut seen = std::collections::HashSet::new();
        self.screens.retain(|s| seen.insert(s.route.clone()));
        self
    }
}

macro_rules! re {
    ($s:expr) => {{
        static R: OnceLock<Regex> = OnceLock::new();
        R.get_or_init(|| Regex::new($s).expect("pattern"))
    }};
}

fn name_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn ext_of(path: &str) -> &str {
    let n = name_of(path);
    n.rsplit_once('.').map(|(_, e)| e).unwrap_or("")
}

/// Folders never scanned: dependencies and build output.
fn skipped(path: &str) -> bool {
    path.split('/').any(|s| {
        matches!(
            s,
            "node_modules"
                | "vendor"
                | "dist"
                | "build"
                | ".next"
                | "target"
                | "__pycache__"
                | ".venv"
                | "venv"
                | "coverage"
        )
    }) || name_of(path).contains(".min.")
}

const CODE: &[&str] = &[
    "ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs", "py", "rs", "go", "rb",
];

fn is_env_example(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        ".env.example" | ".env.sample" | ".env.template" | ".env.dist" | "env.example"
    )
}

fn is_manifest(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "package.json"
            | "cargo.toml"
            | "go.mod"
            | "requirements.txt"
            | "pyproject.toml"
            | "gemfile"
    )
}

fn is_template(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "serverless.yml" | "serverless.yaml" | "template.yaml" | "template.yml"
    )
}

/// Whether the indexer should scan `path`.
pub fn wants(path: &str) -> bool {
    if skipped(path) {
        return false;
    }
    let name = name_of(path);
    CODE.contains(&ext_of(path)) || is_manifest(name) || is_template(name) || is_env_example(name)
}

/// What `path` says. Empty for a file that says nothing.
pub fn scan(path: &str, source: &str) -> Surface {
    if source.len() > MAX_SOURCE_BYTES || !wants(path) {
        return Surface::default();
    }
    let name = name_of(path);
    let mut s = Surface::default();
    let lines = Lines::new(source);
    if is_env_example(name) {
        env_names(source, &mut s);
    } else if is_manifest(name) {
        manifest(name, source, &mut s);
    } else if is_template(name) {
        templates(source, &lines, &mut s);
    } else {
        match ext_of(path) {
            "ts" | "tsx" | "mts" | "cts" | "js" | "jsx" | "mjs" | "cjs" => {
                js(path, source, &lines, &mut s)
            }
            "py" => python(path, source, &lines, &mut s),
            "rs" => rust(source, &lines, &mut s),
            "go" => go(source, &lines, &mut s),
            "rb" => ruby(path, source, &lines, &mut s),
            _ => {}
        }
    }
    s.finish()
}

// ── Paths ──────────────────────────────────────────────────────────────────

/// One path template in the shape every match uses: a leading slash, no
/// query, no trailing slash, and every parameter written `:name`
/// (`{id}`, `<int:id>`, `[id]` and `:id` alike; a catch-all is `:name*`).
pub fn norm_path(raw: &str) -> String {
    let raw = raw.trim().trim_matches(['"', '\'', '`']);
    let raw = raw.split(['?', '#']).next().unwrap_or("");
    let raw = raw.replace("(.:format)", "");
    let segs: Vec<String> = raw
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .map(norm_segment)
        .collect();
    format!("/{}", segs.join("/"))
}

fn norm_segment(s: &str) -> String {
    let inner = |t: &str| -> String {
        let t = t.rsplit(':').next().unwrap_or(t);
        t.trim_start_matches("...")
            .trim_start_matches('*')
            .trim()
            .to_string()
    };
    if s == "*" || s == "**" {
        return ":rest*".to_string();
    }
    if let Some(rest) = s.strip_prefix('*') {
        return format!(":{}*", if rest.is_empty() { "rest" } else { rest });
    }
    if let Some(t) = s.strip_prefix("[[").and_then(|t| t.strip_suffix("]]")) {
        return format!(":{}*", inner(t));
    }
    if let Some(t) = s.strip_prefix('[').and_then(|t| t.strip_suffix(']')) {
        let star = if t.starts_with("...") { "*" } else { "" };
        return format!(":{}{star}", inner(t));
    }
    if let Some(t) = s.strip_prefix('{').and_then(|t| t.strip_suffix('}')) {
        let t = t.split(':').next().unwrap_or(t);
        let star = if t.starts_with('*') || t.ends_with("...") {
            "*"
        } else {
            ""
        };
        return format!(
            ":{}{star}",
            t.trim_start_matches('*').trim_end_matches("...")
        );
    }
    if let Some(t) = s.strip_prefix('<').and_then(|t| t.strip_suffix('>')) {
        return format!(":{}", inner(t));
    }
    if let Some(t) = s.strip_prefix(':') {
        let t = t.trim_end_matches('?');
        return format!(":{t}");
    }
    s.to_string()
}

/// Joins a mount or controller prefix and a route.
pub fn join_path(prefix: &str, path: &str) -> String {
    let p = norm_path(prefix);
    let r = norm_path(path);
    if p == "/" {
        return r;
    }
    if r == "/" {
        return p;
    }
    format!("{p}{r}")
}

/// A route from file-system routing: the folders after the routing root,
/// with route groups (`(shop)`) and parallel slots (`@modal`) left out.
fn fs_route(segs: &[&str]) -> String {
    let kept: Vec<String> = segs
        .iter()
        .filter(|s| !(s.starts_with('(') && s.ends_with(')')) && !s.starts_with('@'))
        .filter(|s| !s.is_empty() && **s != "index")
        .map(|s| norm_segment(s))
        .collect();
    format!("/{}", kept.join("/"))
}

/// The folders of `path` after its last `root` folder (`app`, `pages`), and
/// the file's own name without its extension. `None` when there is no such
/// folder.
fn after_root<'a>(path: &'a str, root: &str) -> Option<(Vec<&'a str>, &'a str)> {
    let parts: Vec<&str> = path.split('/').collect();
    let idx = parts[..parts.len().saturating_sub(1)]
        .iter()
        .rposition(|p| *p == root)?;
    let file = parts.last()?;
    let stem = file.rsplit_once('.').map(|(s, _)| s).unwrap_or(file);
    Some((parts[idx + 1..parts.len() - 1].to_vec(), stem))
}

// ── Source helpers ─────────────────────────────────────────────────────────

struct Lines {
    starts: Vec<usize>,
}

impl Lines {
    fn new(src: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(src.match_indices('\n').map(|(i, _)| i + 1));
        Self { starts }
    }

    fn at(&self, offset: usize) -> u32 {
        match self.starts.binary_search(&offset) {
            Ok(i) => i as u32 + 1,
            Err(i) => i as u32,
        }
    }
}

/// One piece of an address as written.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Lit(String),
    /// A placeholder with the expression it holds.
    Ph(String),
}

/// The address argument starting at `at`, as pieces, and where it ends.
/// Understands quoted strings, template literals, Python f-strings, Rust
/// `format!`, and `base + "/x" + id` chains. `None` when the argument is
/// something else (a variable alone gives one placeholder).
fn url_arg(src: &str, at: usize) -> Option<Vec<Piece>> {
    let rest = src.get(at..)?;
    let rest = rest.trim_start().trim_start_matches('&');
    let b = rest.as_bytes();
    let first = *b.first()?;
    if let Some(args) = rest.strip_prefix("format!(") {
        return rust_format(args);
    }
    if (first == b'f' || first == b'F') && matches!(b.get(1), Some(b'"') | Some(b'\'')) {
        let (body, _) = quoted(&rest[1..])?;
        return Some(braces(&body, false));
    }
    let mut pieces = Vec::new();
    let mut cur = rest;
    for _ in 0..8 {
        let c = cur.as_bytes().first().copied()?;
        let consumed;
        if c == b'\'' || c == b'"' {
            let (body, used) = quoted(cur)?;
            pieces.push(Piece::Lit(body));
            consumed = used;
        } else if c == b'`' {
            let (body, used) = backtick(cur)?;
            pieces.extend(braces(&body, true));
            consumed = used;
        } else if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            let m = re!(r#"^[A-Za-z_$][\w$]*(?:(?:\.|::)[A-Za-z_$][\w$]*|\[['"][A-Za-z0-9_]+['"]\]|\(\s*['"][A-Za-z0-9_]+['"]\s*\))*"#)
                .find(cur)?;
            pieces.push(Piece::Ph(m.as_str().to_string()));
            consumed = m.end();
        } else {
            break;
        }
        cur = cur[consumed..].trim_start();
        match cur.strip_prefix('+') {
            Some(r) => cur = r.trim_start(),
            None => break,
        }
    }
    (!pieces.is_empty()).then_some(pieces)
}

/// A quoted string at the start of `s`: (body, bytes used).
fn quoted(s: &str) -> Option<(String, usize)> {
    let q = s.chars().next()?;
    let mut out = String::new();
    let mut esc = false;
    for (i, c) in s.char_indices().skip(1) {
        if esc {
            out.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == q {
            return Some((out, i + 1));
        } else if c == '\n' {
            return None;
        } else {
            out.push(c);
        }
        if out.len() > 500 {
            return None;
        }
    }
    None
}

fn backtick(s: &str) -> Option<(String, usize)> {
    let mut depth = 0;
    for (i, c) in s.char_indices().skip(1) {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            '`' if depth <= 0 => return Some((s[1..i].to_string(), i + 1)),
            _ => {}
        }
        if i > 600 {
            return None;
        }
    }
    None
}

/// Splits `${x}` (template literals) or `{x}` (f-strings) placeholders out.
fn braces(body: &str, dollar: bool) -> Vec<Piece> {
    let mut out = Vec::new();
    let mut lit = String::new();
    let mut chars = body.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let opens = if dollar {
            c == '$' && body[i + 1..].starts_with('{')
        } else {
            c == '{' && !body[i + 1..].starts_with('{')
        };
        if opens {
            if dollar {
                chars.next();
            }
            let start = if dollar { i + 2 } else { i + 1 };
            let mut depth = 1;
            let mut end = start;
            for (j, d) in chars.by_ref() {
                match d {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = j;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if !lit.is_empty() {
                out.push(Piece::Lit(std::mem::take(&mut lit)));
            }
            out.push(Piece::Ph(
                body.get(start..end).unwrap_or("").trim().to_string(),
            ));
        } else {
            lit.push(c);
        }
    }
    if !lit.is_empty() {
        out.push(Piece::Lit(lit));
    }
    out
}

/// `format!("{}/orders/{id}", base)`: the positional placeholders take the
/// arguments' names.
fn rust_format(args: &str) -> Option<Vec<Piece>> {
    let (body, used) = quoted(args.trim_start())?;
    let after = args.trim_start()[used..].trim_start();
    let close = after.find(')').unwrap_or(after.len());
    let names: Vec<String> = after[..close]
        .split(',')
        .map(|a| a.trim().trim_start_matches('&').to_string())
        .filter(|a| !a.is_empty())
        .collect();
    let mut next = names.into_iter();
    Some(
        braces(&body, false)
            .into_iter()
            .map(|p| match p {
                Piece::Ph(x) if x.is_empty() || x.starts_with(':') => {
                    Piece::Ph(next.next().unwrap_or_default())
                }
                other => other,
            })
            .collect(),
    )
}

/// A setting's name inside an expression (`process.env.API_URL`,
/// `import.meta.env.VITE_API`, `os.environ["X"]`, `os.getenv("X")`,
/// `env::var("X")`).
fn setting_name(expr: &str) -> Option<String> {
    re!(r#"(?:process\.env|import\.meta\.env)\.([A-Z][A-Z0-9_]*)|(?:process\.env|environ)\[['"]([A-Z][A-Z0-9_]*)['"]\]|(?:getenv|environ\.get|env::var|env\.get|ENV\.fetch|Getenv)\(\s*['"]([A-Z][A-Z0-9_]*)['"]|ENV\[['"]([A-Z][A-Z0-9_]*)['"]\]"#)
        .captures(expr)
        .and_then(|c| {
            c.iter()
                .skip(1)
                .flatten()
                .next()
                .map(|m| m.as_str().to_string())
        })
}

/// An address's shape: (path, base, host, dynamic). `None` when it doesn't
/// look like a web address at all (a plain word, a file name).
fn shape(pieces: &[Piece]) -> Option<(String, String, String, bool)> {
    const MARK: char = '\u{1}';
    let mut base = String::new();
    let mut host = String::new();
    let mut body = String::new();
    for (i, p) in pieces.iter().enumerate() {
        match p {
            Piece::Ph(expr) if i == 0 => {
                let next_lit = matches!(pieces.get(1), Some(Piece::Lit(l)) if l.starts_with('/'));
                if pieces.len() == 1 {
                    // A variable alone: the address is built elsewhere.
                    let b = setting_name(expr).unwrap_or_else(|| expr.clone());
                    return Some((String::new(), b, String::new(), true));
                }
                if next_lit {
                    base = setting_name(expr).unwrap_or_else(|| expr.clone());
                } else {
                    body.push(MARK);
                }
            }
            Piece::Ph(_) => body.push(MARK),
            Piece::Lit(l) => body.push_str(l),
        }
    }
    let body = body.trim();
    let rest = if let Some(after) = body
        .strip_prefix("https://")
        .or_else(|| body.strip_prefix("http://"))
    {
        let (h, p) = after.split_once('/').unwrap_or((after, ""));
        host = h.to_ascii_lowercase();
        if host.contains(MARK) {
            // `https://${host}/x`: the host itself is built at run time.
            base = "a host built at run time".into();
            host.clear();
        }
        format!("/{p}")
    } else if body.starts_with('/') {
        body.to_string()
    } else if !base.is_empty() {
        format!("/{body}")
    } else {
        return None;
    };
    if rest.contains(' ') || rest.len() > 300 {
        return None;
    }
    let rest = rest.split(['?', '#']).next().unwrap_or("");
    let segs: Vec<String> = rest
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.contains(MARK) {
                ":p".to_string()
            } else {
                norm_segment(s)
            }
        })
        .collect();
    Some((format!("/{}", segs.join("/")), base, host, false))
}

/// The text of the call starting at `open` (just after its `(`), up to its
/// matching `)`, at most `max` bytes.
fn call_body(src: &str, open: usize, max: usize) -> &str {
    let mut depth = 1;
    let bytes = src.as_bytes();
    let mut i = open;
    let end = (open + max).min(src.len());
    let mut quote: Option<u8> = None;
    while i < end {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == b'\\' {
                    i += 1;
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'"' | b'\'' | b'`' => quote = Some(c),
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &src[open..i];
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    src.get(open..end).unwrap_or("")
}

fn method_in(text: &str) -> Option<String> {
    re!(r#"(?i)\bmethod\s*[:=]\s*['"`]?([a-z]+)"#)
        .captures(text)
        .map(|c| c[1].to_ascii_uppercase())
}

fn push_request(s: &mut Surface, lines: &Lines, at: usize, src: &str, method: &str, via: &str) {
    let Some(pieces) = url_arg(src, at) else {
        return;
    };
    let Some((path, base, host, dynamic)) = shape(&pieces) else {
        return;
    };
    s.requests.push(Request {
        method: method.to_ascii_uppercase(),
        path,
        base,
        host,
        line: lines.at(at),
        via: via.to_string(),
        dynamic,
    });
}

fn push_endpoint(s: &mut Surface, method: &str, path: &str, framework: &str, line: u32) {
    s.endpoints.push(Endpoint {
        method: method.to_ascii_uppercase(),
        path: norm_path(path),
        framework: framework.to_string(),
        line,
    });
}

fn bare_package(spec: &str) -> Option<String> {
    let spec = spec.trim();
    if spec.is_empty()
        || spec.starts_with('.')
        || spec.starts_with('/')
        || spec.starts_with('~')
        || spec.starts_with("@/")
        || spec.starts_with('#')
        || spec.contains(':') && !spec.starts_with("node:")
    {
        return None;
    }
    let spec = spec.trim_start_matches("node:");
    let mut parts = spec.split('/');
    let first = parts.next()?;
    let name = if first.starts_with('@') {
        format!("{first}/{}", parts.next()?)
    } else {
        first.to_string()
    };
    Some(name)
}

// ── JavaScript and TypeScript ──────────────────────────────────────────────

const HTTP_METHODS: &str = "get|post|put|patch|delete|head|options";

fn js(path: &str, src: &str, lines: &Lines, s: &mut Surface) {
    for c in re!(r#"(?:\bfrom\s+|\bimport\s+|\brequire\(\s*|\bimport\(\s*)['"]([^'"\n]+)['"]"#)
        .captures_iter(src)
    {
        if let Some(p) = bare_package(&c[1]) {
            s.packages.push(p);
        }
    }
    let server = s.packages.iter().any(|p| {
        matches!(
            p.as_str(),
            "express"
                | "fastify"
                | "koa"
                | "@koa/router"
                | "koa-router"
                | "hono"
                | "restify"
                | "polka"
                | "@hapi/hapi"
                | "@nestjs/common"
        )
    }) || src.contains("express.Router(");
    js_screens(path, src, lines, s);
    js_endpoints(path, src, lines, s, server);
    js_requests(src, lines, s, server);
}

fn js_screens(path: &str, src: &str, lines: &Lines, s: &mut Surface) {
    let name = name_of(path);
    let stem = name.rsplit_once('.').map(|(a, _)| a).unwrap_or(name);
    let ext = ext_of(path);
    let ui = matches!(ext, "tsx" | "jsx" | "js" | "ts" | "mdx");
    let native = s
        .packages
        .iter()
        .any(|p| p == "react-native" || p == "expo-router");
    if let Some((segs, _)) = after_root(path, "app") {
        if stem == "page" && ui {
            s.screens.push(Screen {
                route: fs_route(&segs),
                framework: "Next.js page".into(),
                line: 1,
            });
        } else if native
            && ui
            && !stem.starts_with('_')
            && !stem.starts_with('+')
            && stem != "route"
        {
            let mut all = segs.clone();
            all.push(stem);
            s.screens.push(Screen {
                route: fs_route(&all),
                framework: "Expo Router screen".into(),
                line: 1,
            });
        }
    }
    if let Some((segs, stem)) = after_root(path, "pages") {
        let api = segs.first() == Some(&"api");
        if !api && ui && !stem.starts_with('_') && !matches!(stem, "404" | "500") {
            let mut all = segs.clone();
            all.push(stem);
            s.screens.push(Screen {
                route: fs_route(&all),
                framework: "Next.js page".into(),
                line: 1,
            });
        }
    }
    let router = s
        .packages
        .iter()
        .any(|p| p.starts_with("react-router") || p == "@tanstack/react-router");
    if router {
        for m in re!(r#"<Route\b[^>]*?\bpath\s*=\s*\{?\s*['"]([^'"]*)['"]"#).captures_iter(src) {
            let at = m.get(0).map(|x| x.start()).unwrap_or(0);
            s.screens.push(Screen {
                route: norm_path(&m[1]),
                framework: "React Router route".into(),
                line: lines.at(at),
            });
        }
        for m in re!(r#"\bpath\s*:\s*['"]([^'"]*)['"]\s*,\s*(?:element|Component|lazy|component|loader|children)"#)
            .captures_iter(src)
        {
            let at = m.get(0).map(|x| x.start()).unwrap_or(0);
            s.screens.push(Screen {
                route: norm_path(&m[1]),
                framework: "React Router route".into(),
                line: lines.at(at),
            });
        }
    }
    for m in re!(r#"<[A-Za-z]+\.Screen\b[^>]*?\bname\s*=\s*['"]([^'"]+)['"]"#).captures_iter(src) {
        let at = m.get(0).map(|x| x.start()).unwrap_or(0);
        s.screens.push(Screen {
            route: m[1].to_string(),
            framework: "React Navigation screen".into(),
            line: lines.at(at),
        });
    }
}

fn js_endpoints(path: &str, src: &str, lines: &Lines, s: &mut Surface, server: bool) {
    let name = name_of(path);
    let stem = name.rsplit_once('.').map(|(a, _)| a).unwrap_or(name);
    // Next.js route handlers: `app/**/route.ts` exporting GET, POST, ...
    if stem == "route" {
        if let Some((segs, _)) = after_root(path, "app") {
            let route = fs_route(&segs);
            for c in re!(r"export\s+(?:async\s+)?(?:function\s+|const\s+|let\s+)(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b|\bas\s+(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b")
                .captures_iter(src)
            {
                let m = c.get(1).or_else(|| c.get(2)).map(|m| m.as_str()).unwrap_or("");
                let at = c.get(0).map(|x| x.start()).unwrap_or(0);
                push_endpoint(s, m, &route, "Next.js route handler", lines.at(at));
            }
        }
    }
    // Next.js pages API: `pages/api/**`, every method unless it checks.
    if let Some((segs, stem)) = after_root(path, "pages") {
        if segs.first() == Some(&"api") {
            let mut all = segs.clone();
            all.push(stem);
            let route = fs_route(&all);
            let methods: Vec<String> =
                re!(r#"req\.method\s*[!=]==?\s*['"]([A-Z]+)['"]|case\s+['"](GET|POST|PUT|PATCH|DELETE)['"]"#)
                    .captures_iter(src)
                    .filter_map(|c| c.get(1).or_else(|| c.get(2)).map(|m| m.as_str().to_string()))
                    .collect();
            if methods.is_empty() {
                push_endpoint(s, "ANY", &route, "Next.js API route", 1);
            }
            for m in methods {
                push_endpoint(s, &m, &route, "Next.js API route", 1);
            }
        }
    }
    // NestJS controllers.
    let controllers: Vec<(usize, String)> =
        re!(r#"@Controller\(\s*(?:['"`]([^'"`]*)['"`]|\{[^}]*?path\s*:\s*['"`]([^'"`]*)['"`][^}]*\})?\s*\)"#)
            .captures_iter(src)
            .map(|c| {
                let at = c.get(0).map(|x| x.start()).unwrap_or(0);
                let p = c
                    .get(1)
                    .or_else(|| c.get(2))
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default();
                (at, p)
            })
            .collect();
    if !controllers.is_empty() {
        for c in re!(
            r#"@(Get|Post|Put|Patch|Delete|All|Head|Options)\(\s*(?:['"`]([^'"`]*)['"`])?\s*\)"#
        )
        .captures_iter(src)
        {
            let at = c.get(0).map(|x| x.start()).unwrap_or(0);
            let prefix = controllers
                .iter()
                .rev()
                .find(|(o, _)| *o < at)
                .map(|(_, p)| p.as_str())
                .unwrap_or("");
            let route = join_path(prefix, c.get(2).map(|m| m.as_str()).unwrap_or(""));
            let m = if &c[1] == "All" { "ANY" } else { &c[1] };
            push_endpoint(s, m, &route, "NestJS controller", lines.at(at));
        }
    }
    if !server {
        return;
    }
    // Express, Fastify, Koa, Hono: `router.get("/x", ...)`.
    let pat =
        format!(r#"\b([A-Za-z_$][\w$]*)\.({HTTP_METHODS}|all)\(\s*['"`](/[^'"`\n]*)['"`]\s*,"#);
    static ROUTE: OnceLock<Regex> = OnceLock::new();
    let route = ROUTE.get_or_init(|| Regex::new(&pat).expect("pattern"));
    for c in route.captures_iter(src) {
        if requestish(&c[1]) {
            continue;
        }
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let m = if &c[2] == "all" { "ANY" } else { &c[2] };
        push_endpoint(s, m, &c[3], "Express-style route", lines.at(at));
    }
    // `router.route("/x").get(h).post(h)`.
    for c in
        re!(r#"\.route\(\s*['"`](/[^'"`\n]*)['"`]\s*\)\s*\.(?:get|post|put|patch|delete|all)\("#)
            .captures_iter(src)
    {
        let whole = c.get(0).expect("match");
        let open = whole.end() - ".get(".len().min(whole.len());
        let end = src[open..]
            .find(';')
            .map(|i| open + i)
            .unwrap_or(src.len())
            .min(open + 800);
        let chain = &src[whole.start()..end];
        for m in re!(r"\)\s*\.(get|post|put|patch|delete|all)\(").captures_iter(chain) {
            let meth = if &m[1] == "all" { "ANY" } else { &m[1] };
            push_endpoint(
                s,
                meth,
                &c[1],
                "Express-style route",
                lines.at(whole.start()),
            );
        }
    }
    // Mounts: `app.use("/api", orders)`, Hono's `app.route("/api", orders)`.
    for c in re!(r#"\b[A-Za-z_$][\w$]*\.(?:use|route|register)\(\s*['"`](/[^'"`\n]*)['"`]\s*,\s*([A-Za-z_$][\w$.]*)"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        s.mounts.push(Mount {
            prefix: norm_path(&c[1]),
            target: c[2].to_string(),
            line: lines.at(at),
        });
    }
}

/// A receiver that makes requests rather than answering them.
fn requestish(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    !matches!(
        n.as_str(),
        "app" | "router" | "server" | "fastify" | "r" | "routes" | "api_router"
    ) && (n.contains("axios")
        || n.contains("client")
        || n.contains("http")
        || n.contains("api")
        || n.contains("request")
        || n.contains("fetch")
        || n == "ky"
        || n == "got"
        || n == "instance"
        || n == "$")
}

fn js_requests(src: &str, lines: &Lines, s: &mut Surface, server: bool) {
    // fetch("/x", { method: "POST" })
    for m in re!(r"\bfetch\(").find_iter(src) {
        let body = call_body(src, m.end(), 1_500);
        let method = method_in(body).unwrap_or_else(|| "GET".into());
        push_request(s, lines, m.end(), src, &method, "fetch");
    }
    // useSWR("/api/x")
    for m in re!(r"\buseSWR(?:Immutable)?\(").find_iter(src) {
        push_request(s, lines, m.end(), src, "GET", "useSWR");
    }
    // axios("/x") or axios({ url, method })
    for m in re!(r"\baxios\(").find_iter(src) {
        let body = call_body(src, m.end(), 1_500);
        let method = method_in(body).unwrap_or_else(|| "GET".into());
        if body.trim_start().starts_with('{') {
            if let Some(u) = re!(r"\burl\s*:\s*").find(body) {
                push_request(s, lines, m.end() + u.end(), src, &method, "axios");
            }
        } else {
            push_request(s, lines, m.end(), src, &method, "axios");
        }
    }
    // client.get("/x"), api.post(`/x/${id}`), axios.delete(...)
    let pat = format!(r"\b([A-Za-z_$][\w$]*)\.({HTTP_METHODS})\(");
    static CALL: OnceLock<Regex> = OnceLock::new();
    let call = CALL.get_or_init(|| Regex::new(&pat).expect("pattern"));
    for c in call.captures_iter(src) {
        let recv = &c[1];
        if !requestish(recv)
            || (server && !recv.to_ascii_lowercase().contains("axios") && !requestish_strict(recv))
        {
            continue;
        }
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        let via = if recv == "axios" {
            "axios"
        } else {
            "HTTP client"
        };
        let before = s.requests.len();
        push_request(s, lines, end, src, &c[2], via);
        // A client instance carries its own base address.
        if recv != "axios" {
            for r in s.requests[before..]
                .iter_mut()
                .filter(|r| r.base.is_empty() && r.host.is_empty())
            {
                r.base = recv.to_string();
            }
        }
    }
    // Generated clients: client.GET("/orders/{id}")
    for c in re!(r"\b([A-Za-z_$][\w$]*)\.(GET|POST|PUT|PATCH|DELETE)\(").captures_iter(src) {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        let before = s.requests.len();
        push_request(s, lines, end, src, &c[2], "generated client");
        for r in s.requests[before..]
            .iter_mut()
            .filter(|r| r.base.is_empty() && r.host.is_empty())
        {
            r.base = c[1].to_string();
        }
    }
    // A team's own helper: apiGet("/orders"), request("/x", { method }).
    for c in re!(r"\b([a-z][A-Za-z0-9_]*)\(").captures_iter(src) {
        let name = &c[1];
        let low = name.to_ascii_lowercase();
        let helper = (low.contains("fetch")
            || low.contains("request")
            || low.starts_with("api")
            || low.contains("http"))
            && !matches!(name, "fetch" | "useSWR" | "axios" | "require" | "prefetch")
            && ![
                "push",
                "replace",
                "navigate",
                "redirect",
                "href",
                "link",
                "route",
                "prefetch",
                "revalidate",
            ]
            .iter()
            .any(|w| low.contains(w));
        if !helper {
            continue;
        }
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        // Only an address that is plainly a path: a helper is a guess.
        let lead = src[end..].trim_start();
        if !(lead.starts_with("'/") || lead.starts_with("\"/") || lead.starts_with("`/")) {
            continue;
        }
        let body = call_body(src, end, 1_500);
        let method = method_in(body).unwrap_or_else(|| {
            ["post", "put", "patch", "delete"]
                .iter()
                .find(|m| low.contains(*m))
                .map(|m| m.to_ascii_uppercase())
                .unwrap_or_else(|| "GET".into())
        });
        push_request(s, lines, end, src, &method, &format!("helper {name}"));
    }
}

/// In a server file, only an unmistakable client makes requests.
fn requestish_strict(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n.contains("client") || n.contains("http") || n.contains("axios") || n == "ky" || n == "got"
}

// ── Python ─────────────────────────────────────────────────────────────────

fn python(path: &str, src: &str, lines: &Lines, s: &mut Surface) {
    for c in re!(r"(?m)^\s*(?:from\s+([A-Za-z_][\w.]*)\s+import|import\s+([A-Za-z_][\w.]*))")
        .captures_iter(src)
    {
        let m = c
            .get(1)
            .or_else(|| c.get(2))
            .map(|m| m.as_str())
            .unwrap_or("");
        if let Some(top) = m.split('.').next().filter(|t| !t.is_empty()) {
            s.packages.push(top.to_string());
        }
    }
    let prefix =
        re!(r#"(?:APIRouter|Blueprint)\([^)]*?(?:prefix|url_prefix)\s*=\s*['"]([^'"]+)['"]"#)
            .captures(src)
            .map(|c| c[1].to_string())
            .unwrap_or_default();
    for c in re!(r#"@([A-Za-z_][\w]*)\.(get|post|put|patch|delete|options|head|route|api_route)\(\s*['"]([^'"]*)['"]"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let recv = &c[1];
        let route = if recv == "app" {
            c[3].to_string()
        } else {
            join_path(&prefix, &c[3])
        };
        let framework = if s.packages.iter().any(|p| p == "flask") {
            "Flask route"
        } else {
            "FastAPI route"
        };
        match &c[2] {
            "route" | "api_route" => {
                let body = call_body(src, c.get(0).map(|x| x.end()).unwrap_or(0), 500);
                let methods: Vec<String> = re!(r#"methods\s*=\s*[\[(]([^\])]*)[\])]"#)
                    .captures(body)
                    .map(|m| {
                        re!(r#"['"]([A-Za-z]+)['"]"#)
                            .captures_iter(&m[1])
                            .map(|x| x[1].to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                if methods.is_empty() {
                    push_endpoint(s, "GET", &route, framework, lines.at(at));
                }
                for m in methods {
                    push_endpoint(s, &m, &route, framework, lines.at(at));
                }
            }
            m => push_endpoint(s, m, &route, framework, lines.at(at)),
        }
    }
    for c in re!(r#"\.(?:include_router|register_blueprint)\(\s*([A-Za-z_][\w.]*)[^)]*?(?:prefix|url_prefix)\s*=\s*['"]([^'"]+)['"]"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        s.mounts.push(Mount {
            prefix: norm_path(&c[2]),
            target: c[1].to_string(),
            line: lines.at(at),
        });
    }
    if name_of(path) == "urls.py" {
        for c in re!(r#"\bpath\(\s*['"]([^'"]*)['"]\s*,\s*([^,)]+)"#).captures_iter(src) {
            if c[2].trim_start().starts_with("include(") {
                continue;
            }
            let at = c.get(0).map(|x| x.start()).unwrap_or(0);
            push_endpoint(s, "ANY", &c[1], "Django URL", lines.at(at));
        }
    }
    for c in re!(r"\b(requests|httpx|session|client|self\.session|self\.client|http_client|aiohttp_session)\.(get|post|put|patch|delete)\(")
        .captures_iter(src)
    {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        let via = if &c[1] == "httpx" { "httpx" } else { "requests" };
        push_request(s, lines, end, src, &c[2], via);
    }
}

// ── Rust ───────────────────────────────────────────────────────────────────

fn rust(src: &str, lines: &Lines, s: &mut Surface) {
    for c in re!(r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+([A-Za-z_][\w]*)::").captures_iter(src) {
        let top = &c[1];
        if !matches!(top, "crate" | "self" | "super" | "std" | "core" | "alloc") {
            s.packages.push(top.to_string());
        }
    }
    let axum = s.packages.iter().any(|p| p == "axum");
    let framework = if axum { "axum route" } else { "actix route" };
    for m in re!(r#"\.route\(\s*"([^"]+)"\s*,"#).captures_iter(src) {
        let whole = m.get(0).expect("match");
        let body = call_body(src, whole.end(), 3_000);
        let at = whole.start();
        let mut any = false;
        for x in re!(r"\b(get|post|put|patch|delete|head|options|any)(?:_service)?\s*\(")
            .captures_iter(body)
        {
            any = true;
            let meth = if &x[1] == "any" { "ANY" } else { &x[1] };
            push_endpoint(s, meth, &m[1], framework, lines.at(at));
        }
        if !any {
            push_endpoint(s, "ANY", &m[1], framework, lines.at(at));
        }
    }
    for c in re!(r#"#\[(get|post|put|patch|delete|head)\(\s*"([^"]+)""#).captures_iter(src) {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        push_endpoint(s, &c[1], &c[2], "actix route", lines.at(at));
    }
    for c in re!(r#"\.nest(?:_service)?\(\s*"([^"]+)"\s*,\s*([A-Za-z_][\w:]*)"#).captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        s.mounts.push(Mount {
            prefix: norm_path(&c[1]),
            target: c[2].to_string(),
            line: lines.at(at),
        });
    }
    for c in re!(r"\breqwest::(get)\(").captures_iter(src) {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        push_request(s, lines, end, src, "GET", "reqwest");
    }
    for c in re!(r"\b(?:client|http|http_client|self\.client|self\.http|api)\s*\.\s*(get|post|put|patch|delete)\(")
        .captures_iter(src)
    {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        let lead = src[end..].trim_start().trim_start_matches('&');
        if lead.starts_with("format!(") || lead.starts_with("\"http") || lead.starts_with("\"/") {
            push_request(s, lines, end, src, &c[1], "reqwest");
        }
    }
}

// ── Go ─────────────────────────────────────────────────────────────────────

fn go(src: &str, lines: &Lines, s: &mut Surface) {
    for block in re!(r"(?s)\bimport\s*\((.*?)\)").captures_iter(src) {
        for c in re!(r#""([^"]+)""#).captures_iter(&block[1]) {
            s.packages.push(c[1].to_string());
        }
    }
    for c in re!(r#"(?m)^\s*import\s+(?:\w+\s+)?"([^"]+)""#).captures_iter(src) {
        s.packages.push(c[1].to_string());
    }
    // Group prefixes: `api := r.Group("/api")`.
    let mut groups: std::collections::HashMap<String, String> = Default::default();
    for c in re!(r#"\b(\w+)\s*:?=\s*(\w+)\.Group\(\s*"([^"]*)""#).captures_iter(src) {
        let parent = groups.get(&c[2]).cloned().unwrap_or_default();
        groups.insert(c[1].to_string(), join_path(&parent, &c[3]));
    }
    for c in re!(r#"\b(\w+)\.(GET|POST|PUT|PATCH|DELETE|Any|Get|Post|Put|Patch|Delete|HandleFunc|Handle)\(\s*"([^"]+)""#)
        .captures_iter(src)
    {
        let recv = &c[1];
        if recv == "http" && matches!(&c[2], "Get" | "Post") {
            continue; // a request, below
        }
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let prefix = groups.get(recv).cloned().unwrap_or_default();
        let (method, raw) = match &c[2] {
            "HandleFunc" | "Handle" => match c[3].split_once(' ') {
                Some((m, p)) if m.chars().all(|x| x.is_ascii_uppercase()) => (m.to_string(), p.to_string()),
                _ => ("ANY".to_string(), c[3].to_string()),
            },
            "Any" => ("ANY".to_string(), c[3].to_string()),
            m => (m.to_ascii_uppercase(), c[3].to_string()),
        };
        push_endpoint(s, &method, &join_path(&prefix, &raw), "Go route", lines.at(at));
    }
    for c in re!(r"\bhttp\.(Get|Post|Head)\(").captures_iter(src) {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        push_request(s, lines, end, src, &c[1], "Go http");
    }
    for c in re!(r#"\bhttp\.NewRequest(?:WithContext)?\(\s*(?:\w+\s*,\s*)?(?:"([A-Z]+)"|http\.Method(\w+))\s*,"#)
        .captures_iter(src)
    {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        let m = c.get(1).or_else(|| c.get(2)).map(|m| m.as_str()).unwrap_or("GET");
        push_request(s, lines, end, src, m, "Go http");
    }
}

// ── Ruby ───────────────────────────────────────────────────────────────────

fn ruby(path: &str, src: &str, lines: &Lines, s: &mut Surface) {
    for c in re!(r#"(?m)^\s*require\s+['"]([^'"]+)['"]"#).captures_iter(src) {
        if let Some(p) = bare_package(&c[1]) {
            s.packages.push(p);
        }
    }
    if path.ends_with("config/routes.rb") {
        rails_routes(src, lines, s);
    }
    for c in re!(
        r"\b(Faraday|HTTParty|RestClient|HTTP|conn|connection)\.(get|post|put|patch|delete)\(?\s*"
    )
    .captures_iter(src)
    {
        let end = c.get(0).map(|x| x.end()).unwrap_or(0);
        push_request(s, lines, end, src, &c[2], "Ruby HTTP client");
    }
}

fn rails_routes(src: &str, lines: &Lines, s: &mut Surface) {
    // Each `do` opens a scope: (prefix it adds).
    let mut stack: Vec<String> = Vec::new();
    let mut offset = 0usize;
    for line in src.split_inclusive('\n') {
        let t = line.trim();
        let at = offset;
        offset += line.len();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        let prefix: String = stack.concat();
        let opens = t.ends_with(" do") || t.contains(" do |") || t.ends_with(" do\n");
        if t == "end" {
            stack.pop();
            continue;
        }
        if let Some(c) =
            re!(r#"^(?:namespace|scope)\s+(?::(\w+)|['"]([^'"]*)['"]|path:\s*['"]([^'"]*)['"])"#)
                .captures(t)
        {
            let p = c
                .get(1)
                .or_else(|| c.get(2))
                .or_else(|| c.get(3))
                .map(|m| m.as_str())
                .unwrap_or("");
            if opens {
                stack.push(norm_path(p));
            }
            continue;
        }
        if let Some(c) = re!(r"^(resources|resource)\s+:(\w+)(.*)$").captures(t) {
            let plural = &c[1] == "resources";
            let name = &c[2];
            let only: Option<Vec<String>> = re!(r"only:\s*(?:\[([^\]]*)\]|:(\w+))")
                .captures(&c[3])
                .map(|o| {
                    let list = o
                        .get(1)
                        .or_else(|| o.get(2))
                        .map(|m| m.as_str())
                        .unwrap_or("");
                    list.split(',')
                        .map(|x| x.trim().trim_start_matches(':').to_string())
                        .collect()
                });
            let base = join_path(&prefix, name);
            let item = if plural {
                format!("{base}/:id")
            } else {
                base.clone()
            };
            let actions: &[(&str, &str, bool)] = &[
                ("index", "GET", false),
                ("create", "POST", false),
                ("new", "GET", false),
                ("show", "GET", true),
                ("update", "PATCH", true),
                ("destroy", "DELETE", true),
                ("edit", "GET", true),
            ];
            for (action, method, on_item) in actions {
                if only
                    .as_ref()
                    .is_some_and(|o| !o.iter().any(|x| x == action))
                {
                    continue;
                }
                if !plural && *action == "index" {
                    continue;
                }
                let p = match (*action, *on_item) {
                    ("new", _) => format!("{base}/new"),
                    ("edit", _) => format!("{item}/edit"),
                    (_, true) => item.clone(),
                    _ => base.clone(),
                };
                push_endpoint(s, method, &p, "Rails route", lines.at(at));
            }
            if opens {
                let singular = name.trim_end_matches('s');
                stack.push(if plural {
                    format!("/{name}/:{singular}_id")
                } else {
                    format!("/{name}")
                });
            }
            continue;
        }
        if let Some(c) = re!(r#"^(get|post|put|patch|delete|match)\s+['"]([^'"]+)['"]"#).captures(t)
        {
            let method = if &c[1] == "match" { "ANY" } else { &c[1] };
            push_endpoint(
                s,
                method,
                &join_path(&prefix, &c[2]),
                "Rails route",
                lines.at(at),
            );
        } else if let Some(c) = re!(r#"^root\b"#).captures(t) {
            let _ = c;
            push_endpoint(
                s,
                "GET",
                &join_path(&prefix, "/"),
                "Rails route",
                lines.at(at),
            );
        }
        if opens {
            stack.push(String::new());
        }
    }
}

// ── Templates, manifests, settings ─────────────────────────────────────────

fn templates(src: &str, lines: &Lines, s: &mut Surface) {
    for c in re!(r#"(?m)\bhttp(?:Api)?\s*:\s*['"]?(GET|POST|PUT|PATCH|DELETE|ANY|\*|get|post|put|patch|delete|any)\s+(/[^\s'"]*)"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let m = if &c[1] == "*" { "ANY" } else { &c[1] };
        push_endpoint(s, m, &c[2], "Serverless function", lines.at(at));
    }
    // Block style, either order: `path: x` and `method: y` close together.
    for c in re!(r#"(?mi)^\s*path\s*:\s*['"]?([^\s'"]+)['"]?\s*\n\s*method\s*:\s*['"]?(\w+|\*)"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let m = if &c[2] == "*" { "ANY" } else { &c[2] };
        push_endpoint(s, m, &c[1], "Serverless function", lines.at(at));
    }
    for c in re!(r#"(?mi)^\s*method\s*:\s*['"]?(\w+|\*)['"]?\s*\n\s*path\s*:\s*['"]?([^\s'"]+)"#)
        .captures_iter(src)
    {
        let at = c.get(0).map(|x| x.start()).unwrap_or(0);
        let m = if &c[1] == "*" { "ANY" } else { &c[1] };
        push_endpoint(s, m, &c[2], "Serverless function", lines.at(at));
    }
    for e in &mut s.endpoints {
        if e.method == "ANY" || e.method == "X-AMAZON-APIGATEWAY-ANY-METHOD" {
            e.method = "ANY".into();
        }
    }
}

fn manifest(name: &str, src: &str, s: &mut Surface) {
    match name.to_ascii_lowercase().as_str() {
        "package.json" => {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(src) else {
                return;
            };
            s.package_name = v["name"].as_str().unwrap_or("").to_string();
            if let Some(deps) = v["dependencies"].as_object() {
                s.packages.extend(deps.keys().cloned());
            }
        }
        "cargo.toml" => {
            let mut section = String::new();
            for line in src.lines() {
                let t = line.trim();
                if t.starts_with('[') {
                    section = t.trim_matches(['[', ']']).to_string();
                    continue;
                }
                if section == "package" {
                    if let Some(c) = re!(r#"^name\s*=\s*"([^"]+)""#).captures(t) {
                        s.package_name = c[1].to_string();
                    }
                }
                if section == "dependencies"
                    || section.ends_with(".dependencies") && !section.contains("dev")
                {
                    if let Some(c) = re!(r"^([A-Za-z0-9_-]+)\s*=").captures(t) {
                        s.packages.push(c[1].replace('-', "_"));
                    }
                }
            }
        }
        "go.mod" => {
            if let Some(c) = re!(r"(?m)^module\s+(\S+)").captures(src) {
                s.package_name = c[1].to_string();
            }
            for c in
                re!(r"(?m)^\s*(?:require\s+)?([a-z0-9.-]+\.[a-z]+/[^\s]+)\s+v").captures_iter(src)
            {
                s.packages.push(c[1].to_string());
            }
        }
        "requirements.txt" => {
            for line in src.lines() {
                let t = line.trim();
                if t.is_empty() || t.starts_with('#') || t.starts_with('-') {
                    continue;
                }
                if let Some(c) = re!(r"^([A-Za-z0-9_.-]+)").captures(t) {
                    s.packages.push(c[1].to_ascii_lowercase());
                }
            }
        }
        "pyproject.toml" => {
            if let Some(c) = re!(r#"(?m)^name\s*=\s*"([^"]+)""#).captures(src) {
                s.package_name = c[1].to_string();
            }
            if let Some(block) = re!(r"(?s)dependencies\s*=\s*\[(.*?)\]").captures(src) {
                for c in re!(r#"['"]([A-Za-z0-9_.-]+)"#).captures_iter(&block[1]) {
                    s.packages.push(c[1].to_ascii_lowercase());
                }
            }
        }
        "gemfile" => {
            for c in re!(r#"(?m)^\s*gem\s+['"]([^'"]+)['"]"#).captures_iter(src) {
                s.packages.push(c[1].to_string());
            }
        }
        _ => {}
    }
}

fn env_names(src: &str, s: &mut Surface) {
    for c in re!(r"(?m)^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=").captures_iter(src) {
        s.env.push(c[1].to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eps(s: &Surface) -> Vec<String> {
        s.endpoints
            .iter()
            .map(|e| format!("{} {}", e.method, e.path))
            .collect()
    }

    fn reqs(s: &Surface) -> Vec<String> {
        s.requests
            .iter()
            .map(|r| format!("{} {} [{}]", r.method, r.path, r.base))
            .collect()
    }

    #[test]
    fn paths_take_one_shape() {
        assert_eq!(norm_path("/slips/{slipId}/"), "/slips/:slipId");
        assert_eq!(norm_path("slips/<int:slip_id>"), "/slips/:slip_id");
        assert_eq!(norm_path("/slips/:id?x=1"), "/slips/:id");
        assert_eq!(norm_path("/docs/[...slug]"), "/docs/:slug*");
        assert_eq!(norm_path("/files/{*rest}"), "/files/:rest*");
        assert_eq!(norm_path(""), "/");
        assert_eq!(join_path("/api", "/slips"), "/api/slips");
        assert_eq!(join_path("slips", ":id"), "/slips/:id");
    }

    #[test]
    fn next_route_handlers_pages_and_screens() {
        let s = scan(
            "web/src/app/api/slips/[id]/route.ts",
            "export async function GET(req) {}\nexport const PATCH = async () => {}\n",
        );
        assert_eq!(eps(&s), vec!["GET /api/slips/:id", "PATCH /api/slips/:id"]);
        let s = scan(
            "web/src/app/(harbor)/slips/[id]/page.tsx",
            "export default function P() {}",
        );
        assert_eq!(s.screens[0].route, "/slips/:id");
        assert_eq!(s.screens[0].framework, "Next.js page");
        let s = scan("web/pages/api/tide.ts", "if (req.method === 'POST') {}");
        assert_eq!(eps(&s), vec!["POST /api/tide"]);
        let s = scan("web/pages/boats/index.tsx", "export default () => null");
        assert_eq!(s.screens[0].route, "/boats");
        assert!(scan("web/pages/_app.tsx", "").screens.is_empty());
    }

    #[test]
    fn express_nest_and_mounts() {
        let src = r#"
import express from "express";
const router = express.Router();
router.get("/slips/:id", async (req, res) => {});
router.post('/slips', create);
app.use("/api", router);
router.route("/moorings").get(list).delete(clear);
"#;
        let s = scan("api/src/slips.ts", src);
        assert_eq!(
            eps(&s),
            vec![
                "GET /slips/:id",
                "POST /slips",
                "GET /moorings",
                "DELETE /moorings"
            ]
        );
        assert_eq!(s.mounts[0].prefix, "/api");
        assert_eq!(s.mounts[0].target, "router");
        let nest = r#"
import { Controller, Get, Post } from '@nestjs/common';
@Controller('boats')
export class BoatsController {
  @Get(':id') one() {}
  @Post() add() {}
}
"#;
        assert_eq!(
            eps(&scan("api/src/boats.controller.ts", nest)),
            vec!["GET /boats/:id", "POST /boats"]
        );
    }

    #[test]
    fn rust_python_go_rails_and_templates() {
        let axum = r#"
use axum::{routing::{get, post}, Router};
fn app() -> Router {
    Router::new()
        .route("/slips/:id", get(show).delete(remove))
        .route("/slips", post(create))
        .nest("/api", other())
}
"#;
        let s = scan("api/src/main.rs", axum);
        assert_eq!(
            eps(&s),
            vec!["GET /slips/:id", "DELETE /slips/:id", "POST /slips"]
        );
        assert_eq!(s.mounts[0].prefix, "/api");
        let actix = "#[get(\"/tides/{day}\")]\nasync fn tides() {}";
        assert_eq!(
            eps(&scan("svc/src/tides.rs", actix)),
            vec!["GET /tides/:day"]
        );

        let fastapi = r#"
from fastapi import APIRouter
router = APIRouter(prefix="/boats")
@router.get("/{boat_id}")
def one(boat_id: int): ...
@app.post("/login")
def login(): ...
"#;
        assert_eq!(
            eps(&scan("svc/boats.py", fastapi)),
            vec!["GET /boats/:boat_id", "POST /login"]
        );
        let flask = "from flask import Flask\n@app.route('/fuel', methods=['GET', 'POST'])\ndef fuel(): pass\n";
        assert_eq!(
            eps(&scan("svc/app.py", flask)),
            vec!["GET /fuel", "POST /fuel"]
        );
        let django =
            "urlpatterns = [path('docks/<int:pk>/', views.dock), path('api/', include('x'))]";
        assert_eq!(
            eps(&scan("svc/harbor/urls.py", django)),
            vec!["ANY /docks/:pk"]
        );

        let gosrc = r#"
import "net/http"
func main() {
    mux.HandleFunc("GET /slips/{id}", show)
    http.HandleFunc("/health", ok)
    api := r.Group("/api")
    api.POST("/boats", create)
    resp, _ := http.Get("https://weather.example/forecast")
}
"#;
        let s = scan("svc/main.go", gosrc);
        assert_eq!(
            eps(&s),
            vec!["GET /slips/:id", "ANY /health", "POST /api/boats"]
        );
        assert_eq!(s.requests[0].host, "weather.example");

        let rails = r#"
Rails.application.routes.draw do
  root "home#index"
  namespace :api do
    resources :slips, only: [:index, :show]
    get "tides/today", to: "tides#today"
  end
end
"#;
        assert_eq!(
            eps(&scan("harbor/config/routes.rb", rails)),
            vec![
                "GET /",
                "GET /api/slips",
                "GET /api/slips/:id",
                "GET /api/tides/today"
            ]
        );

        let sls = "functions:\n  book:\n    handler: book.handler\n    events:\n      - http:\n          path: slips/{id}/book\n          method: post\n      - httpApi: 'GET /slips'\n";
        assert_eq!(
            eps(&scan("svc/serverless.yml", sls)),
            vec!["GET /slips", "POST /slips/:id/book"]
        );
        let sam = "      Events:\n        Api:\n          Type: Api\n          Properties:\n            Path: /fuel/{dock}\n            Method: get\n";
        assert_eq!(
            eps(&scan("svc/template.yaml", sam)),
            vec!["GET /fuel/:dock"]
        );
    }

    #[test]
    fn requests_keep_their_base_and_say_when_built_at_run_time() {
        let src = r#"
import axios from "axios";
const api = axios.create({ baseURL: process.env.NEXT_PUBLIC_HARBOR_API });
export const slip = (id) => fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/slips/${id}`);
export const book = (id) => fetch(`/api/slips/${id}/book`, { method: "POST" });
export const boats = () => api.get("/boats");
export const remove = (id) => axios.delete(API_BASE + "/slips/" + id);
export const typed = () => client.GET("/moorings/{id}", {});
export const any = (url) => fetch(url);
const data = useSWR("/api/tides");
const s = apiGet("/weather/today");
router.push("/slips/1");
"#;
        let s = scan("web/src/lib/harbor.ts", src);
        assert_eq!(
            reqs(&s),
            vec![
                "GET /slips/:p [NEXT_PUBLIC_HARBOR_API]",
                "POST /api/slips/:p/book []",
                "GET  [url]",
                "GET /api/tides []",
                "GET /boats [api]",
                "DELETE /slips/:p [API_BASE]",
                "GET /moorings/:id [client]",
                "GET /weather/today []",
            ]
        );
        assert!(s.requests.iter().any(|r| r.dynamic && r.base == "url"));
        assert!(s.requests.iter().any(|r| r.via == "helper apiGet"));
        let py = "import requests\nr = requests.post(f\"{settings.BASE}/slips/{sid}/book\")\n";
        assert_eq!(
            reqs(&scan("svc/client.py", py)),
            vec!["POST /slips/:p/book [settings.BASE]"]
        );
        let rs = "let r = client.get(format!(\"{}/tides/{}\", base, day)).send().await;";
        assert_eq!(
            reqs(&scan("svc/src/client.rs", rs)),
            vec!["GET /tides/:p [base]"]
        );
    }

    #[test]
    fn screens_from_routers_and_navigation() {
        let rr = r#"
import { createBrowserRouter, Route } from "react-router-dom";
const r = createBrowserRouter([{ path: "/slips/:id", element: <Slip /> }]);
<Route path="/boats" element={<Boats />} />
"#;
        let s = scan("app/src/routes.tsx", rr);
        let routes: Vec<&str> = s.screens.iter().map(|x| x.route.as_str()).collect();
        assert_eq!(routes, vec!["/boats", "/slips/:id"]);
        let rn = "import { NavigationContainer } from '@react-navigation/native';\n<Stack.Screen name=\"Dock\" component={DockScreen} />";
        assert_eq!(scan("mobile/App.tsx", rn).screens[0].route, "Dock");
        let expo = "import { View } from 'react-native';\nexport default function Tides() {}";
        assert_eq!(
            scan("mobile/app/(tabs)/tides.tsx", expo).screens[0].route,
            "/tides"
        );
    }

    #[test]
    fn packages_and_setting_names_never_values() {
        let pkg = r#"{"name": "harbor-web", "dependencies": {"stripe": "1", "@sanity/client": "6"}, "devDependencies": {"jest": "1"}}"#;
        let s = scan("web/package.json", pkg);
        assert_eq!(s.package_name, "harbor-web");
        assert_eq!(s.packages, vec!["@sanity/client", "stripe"]);
        let env = "STRIPE_SECRET_KEY=sk_test_value\n# note\nNEXT_PUBLIC_HARBOR_API=http://localhost:4000\n";
        let s = scan("web/.env.example", env);
        assert_eq!(s.env, vec!["NEXT_PUBLIC_HARBOR_API", "STRIPE_SECRET_KEY"]);
        assert!(!s.to_json().contains("sk_test_value"));
        // A real settings file is never read.
        assert!(!wants("web/.env"));
        assert!(!wants("web/node_modules/x/index.js"));
        let cargo = "[package]\nname = \"harbor-api\"\n[dependencies]\naws-sdk-s3 = \"1\"\n[dev-dependencies]\nmockall = \"1\"\n";
        let s = scan("api/Cargo.toml", cargo);
        assert_eq!(
            (s.package_name.as_str(), s.packages.clone()),
            ("harbor-api", vec!["aws_sdk_s3".to_string()])
        );
        let js = "import Stripe from 'stripe';\nimport { S3Client } from '@aws-sdk/client-s3';\nimport x from './local';";
        assert_eq!(
            scan("api/src/pay.ts", js).packages,
            vec!["@aws-sdk/client-s3", "stripe"]
        );
    }

    #[test]
    fn round_trips_and_is_bounded() {
        let mut src = String::from("import express from 'express';\n");
        for i in 0..400 {
            src.push_str(&format!("router.get('/r{i}', h);\n"));
        }
        let s = scan("api/big.ts", &src);
        assert_eq!(s.endpoints.len(), MAX_ITEMS);
        assert_eq!(Surface::from_json(&s.to_json()), Some(s));
        assert!(scan("api/huge.ts", &"x".repeat(MAX_SOURCE_BYTES + 1)).is_empty());
    }
}
