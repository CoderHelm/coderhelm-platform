//! The app map: how a team's app fits together across all its repositories,
//! in plain words.
//!
//! It is built from what CoderHelm already keeps, never by reading a
//! repository again: each repository's code graph (file list, import edges,
//! and the per-file facts `crate::app_surface` keeps beside the symbols)
//! and its code map (the area sections, as context for the descriptions).
//!
//! - **Parts** ([`build_parts`]): folders with a package manifest are
//!   packages, services and apps; a large one is split into its feature
//!   folders; files outside any package are grouped by top-level folder.
//!   Each part has a developer name (the package's own name or its folder),
//!   and a plain name and one line on what it does, written by a small
//!   model. Only a new part, or one whose files, endpoints, screens or
//!   packages changed ([`Part::fingerprint`]), is described again
//!   ([`carry_descriptions`]); each description is stamped with the commit
//!   it was written at.
//! - **Connections** ([`assemble`]): imports between parts (resolved by the
//!   graph: certain), a package one part depends on that another part is,
//!   web requests matched to the endpoint that answers them, in the same
//!   repository or across repositories ([`match_request`]), and the outside
//!   services each part uses ([`outside_of_package`], hosts and setting
//!   names).
//! - **Honest uncertainty**: every match says how sure it is
//!   ([`Sure`]): `certain` (the graph resolved it, or a same-origin request
//!   has exactly one endpoint), `likely` (one endpoint answers that method
//!   and path in another repository, or only through a mount prefix), or
//!   `guess` (several could). A request nothing answers, or whose address
//!   is built at run time, is listed as unmatched with the reason, never
//!   drawn as a connection.
//! - **Overlays** ([`place_work`], [`place_health`]): CoderHelm's recent runs
//!   and its security, infrastructure and log findings placed on the parts
//!   and files they concern.
//!
//! Storage, in the settings table: `pk = AM#{team}`, `sk = REPO#{owner/name}`
//! (one [`RepoMap`], gzipped JSON in `data`) and `sk = STATUS`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::app_surface::{Request, Surface};
use crate::code_map::{is_infra, is_package_manifest};

/// A package bigger than this many files is split into its feature folders.
pub const SPLIT_AT: usize = 80;
/// A feature folder needs this many files to be a part of its own.
pub const MIN_SPLIT_FILES: usize = 5;
/// Parts one repository may have.
pub const MAX_PARTS_PER_REPO: usize = 40;
/// Files kept on one part (the count is always exact).
pub const MAX_PART_FILES: usize = 400;
/// Import edges kept for one repository.
pub const MAX_IMPORTS: usize = 30_000;
/// Parts described in one model call.
pub const MAX_DESCRIBE_PER_CALL: usize = 40;

// ── Model ──────────────────────────────────────────────────────────────────

/// What a part is.
pub const KINDS: &[&str] = &[
    "web app",
    "mobile app",
    "server",
    "functions",
    "library",
    "infrastructure",
    "tests",
    "docs",
];

/// One part of the app.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Part {
    /// `{owner/name}#{root}` (`.` for the repository's root).
    pub id: String,
    pub repo: String,
    /// The folder, `""` for the repository's root.
    pub root: String,
    pub kind: String,
    /// What developers call it: the package's own name, or the folder.
    pub dev_name: String,
    /// The plain name.
    pub name: String,
    /// One line on what it does.
    pub about: String,
    /// The commit the description was written at; empty while it has only
    /// the placeholder.
    #[serde(default)]
    pub described_at: String,
    pub files: Vec<String>,
    pub file_count: usize,
    /// What the description depends on; a change means describe again.
    pub fingerprint: String,
}

impl Part {
    pub fn described(&self) -> bool {
        !self.described_at.is_empty()
    }
}

/// One repository's share of the map, as kept.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RepoMap {
    pub repo: String,
    /// The graph commit it was built from.
    pub commit: String,
    pub parts: Vec<Part>,
    /// Per file, what it says (files that say something).
    pub surfaces: BTreeMap<String, Surface>,
    /// Import edges (from, to) the graph resolved.
    pub imports: Vec<(String, String)>,
    pub updated_at: String,
    /// The graph commit a re-read was asked for at (a graph from before the
    /// app map's file facts), so it is asked once.
    #[serde(default)]
    pub reindex_asked_at: String,
}

/// How sure a connection is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sure {
    Guess,
    Likely,
    Certain,
}

impl Sure {
    pub fn as_str(self) -> &'static str {
        match self {
            Sure::Certain => "certain",
            Sure::Likely => "likely",
            Sure::Guess => "guess",
        }
    }
}

/// An endpoint with where it lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointAt {
    pub repo: String,
    pub part: String,
    pub file: String,
    pub method: String,
    pub path: String,
    pub framework: String,
    pub line: u32,
    /// The path it answers under a mount prefix seen in its part (`/api`
    /// + `/slips`): matched only as likely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mounted: Option<String>,
}

/// A request with where it is made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestAt {
    pub repo: String,
    pub part: String,
    pub file: String,
    #[serde(flatten)]
    pub request: Request,
}

/// One request matched (or not) to what answers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallMatch {
    pub from: RequestAt,
    /// The endpoints that may answer it: one when certain or likely,
    /// several for a guess, none when unmatched.
    pub to: Vec<EndpointAt>,
    pub sure: Option<Sure>,
    /// How it was matched, or why not, in plain words.
    pub how: String,
}

/// An outside service a part uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outside {
    /// `stripe`, `aws-s3`, `host:weather.example`.
    pub id: String,
    pub name: String,
    pub what: String,
}

/// A line between two parts, or a part and an outside service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connection {
    pub from: String,
    pub to: String,
    /// `imports`, `calls` or `uses`.
    pub kind: String,
    pub sure: Sure,
    /// How many imports, requests or pieces of evidence it stands for.
    pub count: usize,
    /// Up to a few examples, in plain words.
    pub evidence: Vec<String>,
}

/// The whole team's map, assembled from its repositories.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TeamMap {
    pub parts: Vec<Part>,
    pub outside: Vec<Outside>,
    pub connections: Vec<Connection>,
    pub calls: Vec<CallMatch>,
    pub screens: Vec<ScreenAt>,
    pub repos: Vec<RepoStamp>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScreenAt {
    /// `{repo}#{file}#{route}`.
    pub id: String,
    pub repo: String,
    pub part: String,
    pub file: String,
    pub route: String,
    pub framework: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoStamp {
    pub repo: String,
    pub commit: String,
    pub updated_at: String,
}

// ── Parts ──────────────────────────────────────────────────────────────────

fn hidden(path: &str) -> bool {
    path.split('/').any(|s| s.starts_with('.') && s.len() > 1) && !crate::app_surface::wants(path)
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

fn under(path: &str, root: &str) -> bool {
    root.is_empty() || path == root || path.starts_with(&format!("{root}/"))
}

fn is_test(path: &str) -> bool {
    let low = path.to_ascii_lowercase();
    low.split('/').any(|s| {
        matches!(
            s,
            "test" | "tests" | "__tests__" | "e2e" | "spec" | "specs" | "cypress" | "playwright"
        )
    }) || low.contains(".test.")
        || low.contains(".spec.")
        || low.contains("_test.")
}

fn is_doc(path: &str) -> bool {
    let low = path.to_ascii_lowercase();
    low.ends_with(".md") || low.ends_with(".mdx") || low.ends_with(".rst") || low.ends_with(".txt")
}

/// The part ids of one repository, from its files: roots first.
pub fn part_id(repo: &str, root: &str) -> String {
    format!("{repo}#{}", if root.is_empty() { "." } else { root })
}

/// The roots a repository's files group into, deepest first when matching.
pub fn roots_of(files: &[String]) -> Vec<String> {
    let files: Vec<&String> = files.iter().filter(|f| !hidden(f)).collect();
    let mut roots: BTreeSet<String> = files
        .iter()
        .filter(|f| is_package_manifest(f))
        .map(|f| dir_of(f).to_string())
        .filter(|d| !d.split('/').any(|s| s == "node_modules" || s == "vendor"))
        .collect();
    // Files no package holds: grouped by top-level folder (or the root).
    let has_root = roots.contains("");
    if !has_root {
        for f in &files {
            if roots.iter().any(|r| !r.is_empty() && under(f, r)) {
                continue;
            }
            let top = crate::agent_file::top_dir(f).unwrap_or("");
            roots.insert(top.to_string());
        }
    }
    // A large package is split into its feature folders.
    let mut out: Vec<String> = roots.iter().cloned().collect();
    for root in roots.iter() {
        if out.len() >= MAX_PARTS_PER_REPO {
            break;
        }
        let own: Vec<&&String> = files
            .iter()
            .filter(|f| owner(f, &roots) == Some(root.as_str()))
            .collect();
        if own.len() <= SPLIT_AT {
            continue;
        }
        let src = if root.is_empty() {
            "src".to_string()
        } else {
            format!("{root}/src")
        };
        let base = if own.iter().any(|f| under(f, &src)) {
            src
        } else {
            root.clone()
        };
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for f in &own {
            let rest = if base.is_empty() {
                f.as_str()
            } else {
                match f.strip_prefix(&format!("{base}/")) {
                    Some(r) => r,
                    None => continue,
                }
            };
            if let Some((child, _)) = rest.split_once('/') {
                let full = if base.is_empty() {
                    child.to_string()
                } else {
                    format!("{base}/{child}")
                };
                *counts.entry(full).or_default() += 1;
            }
        }
        let mut big: Vec<(String, usize)> = counts
            .into_iter()
            .filter(|(_, n)| *n >= MIN_SPLIT_FILES)
            .collect();
        if big.len() < 2 {
            continue;
        }
        big.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        for (d, _) in big {
            if out.len() >= MAX_PARTS_PER_REPO {
                break;
            }
            if !out.contains(&d) {
                out.push(d);
            }
        }
    }
    out.sort();
    out
}

/// The deepest root holding `path`.
fn owner<'a, I>(path: &str, roots: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a String>,
{
    roots
        .into_iter()
        .filter(|r| under(path, r))
        .max_by_key(|r| if r.is_empty() { 0 } else { r.len() + 1 })
        .map(|r| r.as_str())
}

/// What one repository has, read from its code graph.
#[derive(Debug, Clone, Default)]
pub struct RepoInput {
    pub repo: String,
    pub commit: String,
    pub files: Vec<String>,
    pub surfaces: BTreeMap<String, Surface>,
    pub imports: Vec<(String, String)>,
}

fn humanize(s: &str) -> String {
    let base = s.rsplit('/').next().unwrap_or(s).trim_start_matches('@');
    let words: Vec<String> = base
        .split(['-', '_', '.', ' '])
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect();
    let joined = words.join(" ");
    let mut c = joined.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => "App".to_string(),
    }
}

fn kind_of(files: &[&String], surfaces: &BTreeMap<String, Surface>) -> &'static str {
    let mut screens = 0;
    let mut endpoints = 0;
    let mut native = false;
    let mut functions = false;
    for f in files {
        if let Some(s) = surfaces.get(f.as_str()) {
            screens += s.screens.len();
            endpoints += s.endpoints.len();
            native |= s
                .packages
                .iter()
                .any(|p| p == "react-native" || p == "expo" || p == "expo-router");
            functions |= s
                .endpoints
                .iter()
                .any(|e| e.framework == "Serverless function");
        }
    }
    let n = files.len().max(1);
    if screens > 0 {
        return if native { "mobile app" } else { "web app" };
    }
    if endpoints > 0 {
        return if functions { "functions" } else { "server" };
    }
    if files.iter().filter(|f| is_infra(f)).count() * 2 > n {
        return "infrastructure";
    }
    if files.iter().filter(|f| is_test(f)).count() * 2 > n {
        return "tests";
    }
    if files.iter().filter(|f| is_doc(f)).count() * 2 > n {
        return "docs";
    }
    "library"
}

fn placeholder_about(kind: &str, screens: usize, endpoints: usize, files: usize) -> String {
    match kind {
        "web app" | "mobile app" => format!("A {kind} with {screens} screens."),
        "server" | "functions" => format!("A {kind} answering {endpoints} endpoints."),
        _ => format!(
            "{} {kind} of {files} files.",
            if kind.starts_with(['a', 'e', 'i', 'o', 'u']) {
                "An"
            } else {
                "A"
            }
        ),
    }
}

/// The parts of one repository, described with placeholders.
pub fn build_parts(input: &RepoInput) -> Vec<Part> {
    let roots = roots_of(&input.files);
    let mut by_root: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
    for f in input.files.iter().filter(|f| !hidden(f)) {
        if let Some(r) = owner(f, &roots) {
            by_root.entry(r).or_default().push(f);
        }
    }
    let mut parts = Vec::new();
    for (root, files) in by_root {
        let manifest_name = files
            .iter()
            .filter(|f| dir_of(f) == root && is_package_manifest(f))
            .filter_map(|f| input.surfaces.get(f.as_str()))
            .map(|s| s.package_name.clone())
            .find(|n| !n.is_empty());
        let dev_name = manifest_name.unwrap_or_else(|| {
            if root.is_empty() {
                input
                    .repo
                    .rsplit('/')
                    .next()
                    .unwrap_or(&input.repo)
                    .to_string()
            } else {
                root.to_string()
            }
        });
        let kind = kind_of(&files, &input.surfaces);
        let screens: usize = files
            .iter()
            .filter_map(|f| input.surfaces.get(f.as_str()))
            .map(|s| s.screens.len())
            .sum();
        let endpoints: usize = files
            .iter()
            .filter_map(|f| input.surfaces.get(f.as_str()))
            .map(|s| s.endpoints.len())
            .sum();
        let mut fp = Sha256::new();
        fp.update(root.as_bytes());
        fp.update(kind.as_bytes());
        fp.update(dev_name.as_bytes());
        for f in &files {
            fp.update(f.as_bytes());
            fp.update(b"\n");
            if let Some(s) = input.surfaces.get(f.as_str()) {
                for e in &s.endpoints {
                    fp.update(format!("E{} {}", e.method, e.path).as_bytes());
                }
                for sc in &s.screens {
                    fp.update(format!("S{}", sc.route).as_bytes());
                }
                for p in &s.packages {
                    fp.update(format!("P{p}").as_bytes());
                }
            }
        }
        let fingerprint = hex::encode(&fp.finalize()[..12]);
        let base_name = if root.is_empty() {
            input.repo.rsplit('/').next().unwrap_or("app")
        } else {
            root
        };
        parts.push(Part {
            id: part_id(&input.repo, root),
            repo: input.repo.clone(),
            root: root.to_string(),
            kind: kind.to_string(),
            dev_name,
            name: humanize(base_name),
            about: placeholder_about(kind, screens, endpoints, files.len()),
            described_at: String::new(),
            file_count: files.len(),
            files: files
                .iter()
                .take(MAX_PART_FILES)
                .map(|f| f.to_string())
                .collect(),
            fingerprint,
        });
    }
    parts
}

/// Keeps the descriptions of parts that did not change (same id and
/// fingerprint) and returns the ids that need a new one: new parts and
/// changed ones. A changed part keeps its older text, with its older stamp,
/// until the new one is written.
pub fn carry_descriptions(old: Option<&RepoMap>, parts: &mut [Part]) -> Vec<String> {
    let before: HashMap<&str, &Part> = old
        .map(|m| m.parts.iter().map(|p| (p.id.as_str(), p)).collect())
        .unwrap_or_default();
    let mut todo = Vec::new();
    for p in parts.iter_mut() {
        match before.get(p.id.as_str()) {
            Some(o) if o.described() => {
                p.name = o.name.clone();
                p.about = o.about.clone();
                p.described_at = o.described_at.clone();
                if o.fingerprint != p.fingerprint {
                    todo.push(p.id.clone());
                }
            }
            _ => todo.push(p.id.clone()),
        }
    }
    todo
}

/// The instructions for describing parts.
pub const DESCRIBE_SYSTEM: &str = "You name the parts of a software product for people who \
don't read code. For each part give a short plain name (2 to 5 words, no code names, no file \
names, no jargon) and one sentence, at most 20 words, on what it does for the people who use the \
product. Use only what the facts given say; when they say little, say what kind of part it is. \
No arrows, no marketing words.";

/// The request that describes `todo` parts.
pub fn describe_prompt(
    parts: &[&Part],
    surfaces: &BTreeMap<String, Surface>,
    areas: &BTreeMap<String, String>,
) -> String {
    let mut out = String::from("Describe each part below. Reply with JSON only: {\"parts\": [{\"id\": \"...\", \"name\": \"...\", \"about\": \"...\"}]}\n");
    for p in parts {
        let mut eps = Vec::new();
        let mut scr = Vec::new();
        let mut pkgs = BTreeSet::new();
        for f in &p.files {
            if let Some(s) = surfaces.get(f) {
                eps.extend(
                    s.endpoints
                        .iter()
                        .map(|e| format!("{} {}", e.method, e.path)),
                );
                scr.extend(s.screens.iter().map(|s| s.route.clone()));
                pkgs.extend(s.packages.iter().cloned());
            }
        }
        eps.truncate(15);
        scr.truncate(15);
        let files: Vec<&str> = p.files.iter().take(25).map(String::as_str).collect();
        let pkgs: Vec<String> = pkgs.into_iter().take(25).collect();
        out.push_str(&format!(
            "\n## id: {}\nRepository: {}\nFolder: {}\nKind: {}\nDeveloper name: {}\nFiles ({}): {}\n",
            p.id,
            p.repo,
            if p.root.is_empty() { "(root)" } else { &p.root },
            p.kind,
            p.dev_name,
            p.file_count,
            files.join(", ")
        ));
        if !eps.is_empty() {
            out.push_str(&format!("Endpoints: {}\n", eps.join(", ")));
        }
        if !scr.is_empty() {
            out.push_str(&format!("Screens: {}\n", scr.join(", ")));
        }
        if !pkgs.is_empty() {
            out.push_str(&format!("Packages: {}\n", pkgs.join(", ")));
        }
        let top = p.root.split('/').next().unwrap_or("");
        if let Some(a) = areas.get(top) {
            out.push_str(&format!(
                "What the code map says of its area: {}\n",
                crate::truncate_str(a, 700)
            ));
        }
    }
    out
}

/// The JSON schema the description reply follows.
pub fn describe_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"parts": {"type": "array", "items": {
            "type": "object",
            "properties": {"id": {"type": "string"}, "name": {"type": "string"}, "about": {"type": "string"}},
            "required": ["id", "name", "about"],
            "additionalProperties": false
        }}},
        "required": ["parts"],
        "additionalProperties": false
    })
}

fn clean_words(s: &str, max: usize) -> String {
    let s: String = s
        .replace(['\u{2192}', '\u{21d2}', '\u{279c}'], " ")
        .replace("->", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    crate::truncate_str(&s, max).trim().to_string()
}

/// Applies a description reply to `parts`: only ids asked for, non-empty
/// text, stamped `commit`. Returns the ids described.
pub fn apply_descriptions(
    reply: &Value,
    todo: &[String],
    parts: &mut [Part],
    commit: &str,
) -> Vec<String> {
    let mut done = Vec::new();
    for d in reply["parts"].as_array().into_iter().flatten() {
        let id = d["id"].as_str().unwrap_or("");
        if !todo.iter().any(|t| t == id) || done.iter().any(|x: &String| x == id) {
            continue;
        }
        let name = clean_words(d["name"].as_str().unwrap_or(""), 60);
        let about = clean_words(d["about"].as_str().unwrap_or(""), 200);
        if name.is_empty() || about.is_empty() {
            continue;
        }
        if let Some(p) = parts.iter_mut().find(|p| p.id == id) {
            p.name = name;
            p.about = about;
            p.described_at = commit.to_string();
            done.push(id.to_string());
        }
    }
    done
}

/// One repository's map from its input: parts, and the facts kept to
/// connect them.
pub fn repo_map(input: &RepoInput, parts: Vec<Part>, now: &str) -> RepoMap {
    let mut imports = input.imports.clone();
    imports.sort();
    imports.dedup();
    imports.truncate(MAX_IMPORTS);
    RepoMap {
        repo: input.repo.clone(),
        commit: input.commit.clone(),
        parts,
        surfaces: input
            .surfaces
            .iter()
            .filter(|(_, s)| !s.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        imports,
        updated_at: now.to_string(),
        reindex_asked_at: String::new(),
    }
}

// ── Outside services ───────────────────────────────────────────────────────

/// (package prefix, id, name, what it is for).
const OUTSIDE: &[(&str, &str, &str, &str)] = &[
    ("stripe", "stripe", "Stripe", "payments"),
    ("@stripe/", "stripe", "Stripe", "payments"),
    ("stripe-go", "stripe", "Stripe", "payments"),
    ("github.com/stripe/", "stripe", "Stripe", "payments"),
    ("@sendgrid/", "sendgrid", "SendGrid", "email"),
    ("sendgrid", "sendgrid", "SendGrid", "email"),
    ("@supabase/", "supabase", "Supabase", "database and sign-in"),
    ("supabase", "supabase", "Supabase", "database and sign-in"),
    ("firebase", "firebase", "Firebase", "app backend"),
    ("firebase-admin", "firebase", "Firebase", "app backend"),
    (
        "@react-native-firebase/",
        "firebase",
        "Firebase",
        "app backend",
    ),
    ("@firebase/", "firebase", "Firebase", "app backend"),
    ("@sanity/", "sanity", "Sanity", "content"),
    ("next-sanity", "sanity", "Sanity", "content"),
    ("sanity", "sanity", "Sanity", "content"),
    ("twilio", "twilio", "Twilio", "text messages and calls"),
    ("@slack/", "slack", "Slack", "messages"),
    ("slack_sdk", "slack", "Slack", "messages"),
    ("openai", "openai", "OpenAI", "AI models"),
    ("@anthropic-ai/", "anthropic", "Anthropic", "AI models"),
    ("anthropic", "anthropic", "Anthropic", "AI models"),
    ("@sentry/", "sentry", "Sentry", "error reports"),
    ("sentry_sdk", "sentry", "Sentry", "error reports"),
    ("sentry", "sentry", "Sentry", "error reports"),
    ("algoliasearch", "algolia", "Algolia", "search"),
    ("mailgun", "mailgun", "Mailgun", "email"),
    ("postmark", "postmark", "Postmark", "email"),
    ("resend", "resend", "Resend", "email"),
    ("@auth0/", "auth0", "Auth0", "sign-in"),
    ("auth0", "auth0", "Auth0", "sign-in"),
    ("@clerk/", "clerk", "Clerk", "sign-in"),
    ("contentful", "contentful", "Contentful", "content"),
    ("@shopify/", "shopify", "Shopify", "store"),
    ("plaid", "plaid", "Plaid", "bank connections"),
    ("@segment/", "segment", "Segment", "analytics"),
    ("posthog", "posthog", "PostHog", "analytics"),
    ("mixpanel", "mixpanel", "Mixpanel", "analytics"),
    (
        "@google-cloud/",
        "google-cloud",
        "Google Cloud",
        "cloud services",
    ),
    ("@azure/", "azure", "Azure", "cloud services"),
    ("redis", "redis", "Redis", "cache"),
    ("ioredis", "redis", "Redis", "cache"),
    ("@prisma/client", "database", "Database", "stored data"),
    ("pg", "database", "Database", "stored data"),
    ("mysql2", "database", "Database", "stored data"),
    ("mongoose", "database", "Database", "stored data"),
    ("mongodb", "database", "Database", "stored data"),
    ("sqlx", "database", "Database", "stored data"),
    ("diesel", "database", "Database", "stored data"),
    ("psycopg2", "database", "Database", "stored data"),
    ("sqlalchemy", "database", "Database", "stored data"),
    ("gorm.io/", "database", "Database", "stored data"),
    ("activerecord", "database", "Database", "stored data"),
];

/// AWS services by the package that names them.
const AWS: &[(&str, &str)] = &[
    ("s3", "S3 file storage"),
    ("dynamodb", "DynamoDB database"),
    ("sqs", "SQS queues"),
    ("sns", "SNS notifications"),
    ("ses", "SES email"),
    ("sesv2", "SES email"),
    ("lambda", "Lambda functions"),
    ("cognito-identity-provider", "Cognito sign-in"),
    ("cognitoidentityprovider", "Cognito sign-in"),
    ("secrets-manager", "Secrets Manager"),
    ("secretsmanager", "Secrets Manager"),
    ("bedrock-runtime", "Bedrock AI models"),
    ("bedrockruntime", "Bedrock AI models"),
    ("eventbridge", "EventBridge events"),
    ("kinesis", "Kinesis streams"),
    ("cloudwatch", "CloudWatch"),
];

fn outside(id: &str, name: &str, what: &str) -> Outside {
    Outside {
        id: id.to_string(),
        name: name.to_string(),
        what: what.to_string(),
    }
}

/// The outside service a package stands for.
pub fn outside_of_package(pkg: &str) -> Option<Outside> {
    let low = pkg.to_ascii_lowercase();
    let aws = low
        .strip_prefix("@aws-sdk/client-")
        .or_else(|| low.strip_prefix("aws_sdk_"))
        .or_else(|| low.strip_prefix("aws-sdk-"));
    if let Some(svc) = aws {
        let (key, what) = AWS
            .iter()
            .find(|(k, _)| *k == svc)
            .map(|(k, w)| (k.to_string(), w.to_string()))
            .unwrap_or_else(|| (svc.to_string(), svc.to_ascii_uppercase()));
        return Some(outside(
            &format!("aws-{key}"),
            &format!("AWS {}", what.split(' ').next().unwrap_or("")),
            &what,
        ));
    }
    if matches!(
        low.as_str(),
        "aws-sdk" | "boto3" | "botocore" | "aws_config" | "aws-amplify"
    ) {
        return Some(outside("aws", "AWS", "cloud services"));
    }
    OUTSIDE
        .iter()
        .find(|(p, _, _, _)| {
            if p.ends_with('/') {
                low.starts_with(p)
            } else {
                low == *p
            }
        })
        .map(|(_, id, name, what)| outside(id, name, what))
}

/// The outside service a host stands for, when it is a known one.
pub fn outside_of_host(host: &str) -> Option<Outside> {
    let h = host.to_ascii_lowercase();
    let known: &[(&str, &str)] = &[
        ("stripe.com", "stripe"),
        ("sendgrid.com", "sendgrid"),
        ("supabase.co", "supabase"),
        ("firebaseio.com", "firebase"),
        ("googleapis.com", "google-cloud"),
        ("sanity.io", "sanity"),
        ("twilio.com", "twilio"),
        ("slack.com", "slack"),
        ("openai.com", "openai"),
        ("anthropic.com", "anthropic"),
        ("amazonaws.com", "aws"),
    ];
    for (suffix, id) in known {
        if h == *suffix || h.ends_with(&format!(".{suffix}")) {
            if *id == "aws" {
                return Some(outside("aws", "AWS", "cloud services"));
            }
            if *id == "google-cloud" {
                return Some(outside("google-cloud", "Google Cloud", "cloud services"));
            }
            return outside_of_package(id);
        }
    }
    None
}

/// The outside service a setting's name points at (`STRIPE_SECRET_KEY`).
pub fn outside_of_setting(name: &str) -> Option<Outside> {
    let first = name
        .split('_')
        .find(|w| !matches!(*w, "NEXT" | "PUBLIC" | "VITE" | "REACT" | "APP" | "EXPO"))?;
    match first {
        "STRIPE" | "SENDGRID" | "SUPABASE" | "FIREBASE" | "SANITY" | "TWILIO" | "SLACK"
        | "OPENAI" | "ANTHROPIC" | "SENTRY" | "ALGOLIA" | "MAILGUN" | "POSTMARK" | "RESEND"
        | "AUTH0" | "CLERK" | "CONTENTFUL" | "PLAID" | "POSTHOG" | "MIXPANEL" => {
            outside_of_package(&first.to_ascii_lowercase())
        }
        _ => None,
    }
}

// ── Matching ───────────────────────────────────────────────────────────────

fn segments(p: &str) -> Vec<&str> {
    p.split('/').filter(|s| !s.is_empty()).collect()
}

/// How well a request path fits an endpoint path: `None` when it doesn't,
/// else the number of literal segments that agree (more is a better fit).
pub fn fit(request: &str, endpoint: &str) -> Option<usize> {
    let r = segments(request);
    let e = segments(endpoint);
    let mut score = 0;
    for (i, es) in e.iter().enumerate() {
        if es.starts_with(':') && es.ends_with('*') {
            return (r.len() >= i).then_some(score);
        }
        let rs = r.get(i)?;
        if es.starts_with(':') || rs.starts_with(':') {
            continue;
        }
        if !es.eq_ignore_ascii_case(rs) {
            return None;
        }
        score += 1;
    }
    (r.len() == e.len()).then_some(score)
}

fn methods_fit(request: &str, endpoint: &str) -> bool {
    endpoint == "ANY"
        || request.eq_ignore_ascii_case(endpoint)
        || (request == "PUT" && endpoint == "PATCH")
        || (request == "PATCH" && endpoint == "PUT")
}

const GENERIC: &[&str] = &[
    "next", "public", "url", "uri", "base", "api", "host", "endpoint", "vite", "react", "app",
    "env", "process", "import", "meta", "config", "settings", "server", "service", "http", "https",
    "root", "origin", "address", "self", "this",
];

fn tokens(s: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if !c.is_alphanumeric() {
            if !cur.is_empty() {
                out.insert(std::mem::take(&mut cur));
            }
            prev_lower = false;
            continue;
        }
        if c.is_uppercase() && prev_lower && !cur.is_empty() {
            out.insert(std::mem::take(&mut cur));
        }
        prev_lower = c.is_lowercase();
        cur.push(c.to_ascii_lowercase());
    }
    if !cur.is_empty() {
        out.insert(cur);
    }
    out.retain(|t| t.len() > 2 && !GENERIC.contains(&t.as_str()));
    out
}

/// Matches one request to the endpoints that may answer it.
pub fn match_request(
    req: &RequestAt,
    endpoints: &[EndpointAt],
    parts: &HashMap<&str, &Part>,
) -> CallMatch {
    let r = &req.request;
    let unmatched = |how: String| CallMatch {
        from: req.clone(),
        to: Vec::new(),
        sure: None,
        how,
    };
    if r.dynamic {
        return unmatched(format!(
            "The address is built at run time{}, so it isn't matched.",
            if r.base.is_empty() {
                String::new()
            } else {
                format!(" (from {})", r.base)
            }
        ));
    }
    let local_host = r.host.is_empty()
        || r.host.starts_with("localhost")
        || r.host.starts_with("127.0.0.1")
        || r.host.starts_with("0.0.0.0");
    let mut cands: Vec<(usize, &EndpointAt)> = endpoints
        .iter()
        .filter(|e| methods_fit(&r.method, &e.method))
        .filter_map(|e| {
            let path = e.mounted.as_deref().unwrap_or(&e.path);
            fit(&r.path, path).map(|s| (s, e))
        })
        .collect();
    let same_origin = r.base.is_empty() && r.host.is_empty();
    if same_origin && cands.iter().any(|(_, e)| e.repo == req.repo) {
        cands.retain(|(_, e)| e.repo == req.repo);
    }
    let best = cands.iter().map(|(s, _)| *s).max();
    cands.retain(|(s, _)| Some(*s) == best);
    // The same route answered both directly and under a mount: keep the
    // direct one.
    if cands.iter().any(|(_, e)| e.mounted.is_none())
        && cands.iter().any(|(_, e)| e.mounted.is_some())
    {
        let direct: Vec<(usize, &EndpointAt)> = cands
            .iter()
            .filter(|(_, e)| e.mounted.is_none())
            .cloned()
            .collect();
        cands = direct;
    }
    if cands.is_empty() {
        let where_ = if !r.host.is_empty() && !local_host {
            format!(" at {}", r.host)
        } else if !r.base.is_empty() {
            format!(" behind {}", r.base)
        } else {
            String::new()
        };
        return unmatched(format!(
            "No endpoint in the team's repositories answers {} {}{where_}.",
            r.method, r.path
        ));
    }
    let helper = r.via.starts_with("helper ");
    if cands.len() == 1 {
        let e = cands[0].1;
        let same = e.repo == req.repo;
        let sure = if !local_host {
            Sure::Guess
        } else if same && same_origin && e.mounted.is_none() && !helper {
            Sure::Certain
        } else {
            Sure::Likely
        };
        let mut how = format!(
            "{} {} answers it",
            e.method,
            e.mounted.as_deref().unwrap_or(&e.path)
        );
        if !same {
            how.push_str(&format!(" in {}", e.repo));
        }
        if e.mounted.is_some() {
            how.push_str(" (under a mount prefix)");
        }
        if !r.base.is_empty() {
            how.push_str(&format!("; the address starts with {}", r.base));
        }
        if helper {
            how.push_str(&format!(
                "; made through {}",
                r.via.trim_start_matches("helper ")
            ));
        }
        how.push('.');
        return CallMatch {
            from: req.clone(),
            to: vec![e.clone()],
            sure: Some(sure),
            how,
        };
    }
    // Several answer it: the address's own name may say which.
    let want = tokens(&format!("{} {}", r.base, r.host));
    let hinted: Vec<&EndpointAt> = cands
        .iter()
        .map(|(_, e)| *e)
        .filter(|e| {
            let p = parts.get(e.part.as_str());
            let have = tokens(&format!(
                "{} {} {}",
                e.repo,
                p.map(|p| p.dev_name.as_str()).unwrap_or(""),
                p.map(|p| p.root.as_str()).unwrap_or("")
            ));
            !want.is_empty() && want.intersection(&have).next().is_some()
        })
        .collect();
    let repos: BTreeSet<&str> = hinted.iter().map(|e| e.repo.as_str()).collect();
    if repos.len() == 1 && local_host {
        let e = hinted[0];
        return CallMatch {
            from: req.clone(),
            to: vec![e.clone()],
            sure: Some(Sure::Likely),
            how: format!(
                "{} {} in {} answers it; picked over {} others because the address's name ({}) names it.",
                e.method,
                e.path,
                e.repo,
                cands.len() - 1,
                r.base
            ),
        };
    }
    let to: Vec<EndpointAt> = cands.iter().take(3).map(|(_, e)| (*e).clone()).collect();
    CallMatch {
        from: req.clone(),
        how: format!(
            "{} endpoints answer {} {}; which one depends on where the address points.",
            cands.len(),
            r.method,
            r.path
        ),
        to,
        sure: Some(Sure::Guess),
    }
}

/// The part of `repo` holding `path`.
pub fn part_of<'a>(map: &'a RepoMap, path: &str) -> Option<&'a Part> {
    map.parts
        .iter()
        .filter(|p| under(path, &p.root))
        .max_by_key(|p| {
            if p.root.is_empty() {
                0
            } else {
                p.root.len() + 1
            }
        })
}

/// Every endpoint, with the mounted paths its part's mounts give it.
pub fn endpoints_of(maps: &[RepoMap]) -> Vec<EndpointAt> {
    let mut out = Vec::new();
    for m in maps {
        let mut mounts: HashMap<&str, BTreeSet<String>> = HashMap::new();
        for (file, s) in &m.surfaces {
            if let Some(p) = part_of(m, file) {
                for mt in &s.mounts {
                    if mt.prefix != "/" {
                        mounts
                            .entry(p.id.as_str())
                            .or_default()
                            .insert(mt.prefix.clone());
                    }
                }
            }
        }
        for (file, s) in &m.surfaces {
            let Some(p) = part_of(m, file) else { continue };
            for e in &s.endpoints {
                let at = |mounted: Option<String>| EndpointAt {
                    repo: m.repo.clone(),
                    part: p.id.clone(),
                    file: file.clone(),
                    method: e.method.clone(),
                    path: e.path.clone(),
                    framework: e.framework.clone(),
                    line: e.line,
                    mounted,
                };
                out.push(at(None));
                for prefix in mounts.get(p.id.as_str()).into_iter().flatten() {
                    if !e.path.starts_with(&format!("{prefix}/")) {
                        out.push(at(Some(crate::app_surface::join_path(prefix, &e.path))));
                    }
                }
            }
        }
    }
    out
}

fn bump(
    conns: &mut BTreeMap<(String, String, String), Connection>,
    from: &str,
    to: &str,
    kind: &str,
    sure: Sure,
    ev: String,
) {
    let c = conns
        .entry((from.to_string(), to.to_string(), kind.to_string()))
        .or_insert_with(|| Connection {
            from: from.to_string(),
            to: to.to_string(),
            kind: kind.to_string(),
            sure,
            count: 0,
            evidence: Vec::new(),
        });
    c.count += 1;
    // The line is as sure as its surest evidence.
    if sure > c.sure {
        c.sure = sure;
    }
    if c.evidence.len() < 5 && !c.evidence.contains(&ev) {
        c.evidence.push(ev);
    }
}

/// The team's map from its repositories' maps.
pub fn assemble(maps: &[RepoMap]) -> TeamMap {
    let parts: Vec<Part> = maps.iter().flat_map(|m| m.parts.iter().cloned()).collect();
    let by_id: HashMap<&str, &Part> = parts.iter().map(|p| (p.id.as_str(), p)).collect();
    let mut conns: BTreeMap<(String, String, String), Connection> = BTreeMap::new();
    let mut outside_seen: BTreeMap<String, Outside> = BTreeMap::new();
    let mut screens = Vec::new();
    // Imports between parts (resolved by the graph).
    for m in maps {
        for (a, b) in &m.imports {
            if let (Some(pa), Some(pb)) = (part_of(m, a), part_of(m, b)) {
                if pa.id != pb.id {
                    bump(
                        &mut conns,
                        &pa.id,
                        &pb.id,
                        "imports",
                        Sure::Certain,
                        format!("{a} imports {b}"),
                    );
                }
            }
        }
    }
    // A package one part depends on that another part is.
    let by_name: HashMap<&str, &Part> = parts
        .iter()
        .filter(|p| !p.dev_name.is_empty() && p.dev_name != p.root)
        .map(|p| (p.dev_name.as_str(), p))
        .collect();
    for m in maps {
        for (file, s) in &m.surfaces {
            let Some(pa) = part_of(m, file) else { continue };
            for pkg in &s.packages {
                if let Some(pb) = by_name.get(pkg.as_str()) {
                    if pb.id != pa.id {
                        bump(
                            &mut conns,
                            &pa.id,
                            &pb.id,
                            "imports",
                            Sure::Certain,
                            format!("{file} uses the package {pkg}"),
                        );
                    }
                }
                if let Some(o) = outside_of_package(pkg) {
                    bump(
                        &mut conns,
                        &pa.id,
                        &format!("outside:{}", o.id),
                        "uses",
                        Sure::Certain,
                        format!("{file} uses {pkg}"),
                    );
                    outside_seen.entry(o.id.clone()).or_insert(o);
                }
            }
            for env in &s.env {
                if let Some(o) = outside_of_setting(env) {
                    bump(
                        &mut conns,
                        &pa.id,
                        &format!("outside:{}", o.id),
                        "uses",
                        Sure::Likely,
                        format!("{file} names the setting {env}"),
                    );
                    outside_seen.entry(o.id.clone()).or_insert(o);
                }
            }
            for sc in &s.screens {
                screens.push(ScreenAt {
                    id: format!("{}#{}#{}", m.repo, file, sc.route),
                    repo: m.repo.clone(),
                    part: pa.id.clone(),
                    file: file.clone(),
                    route: sc.route.clone(),
                    framework: sc.framework.clone(),
                });
            }
        }
    }
    // Requests.
    let endpoints = endpoints_of(maps);
    let mut calls = Vec::new();
    for m in maps {
        for (file, s) in &m.surfaces {
            let Some(pa) = part_of(m, file) else { continue };
            for r in &s.requests {
                if let Some(o) = (!r.host.is_empty())
                    .then(|| outside_of_host(&r.host))
                    .flatten()
                {
                    bump(
                        &mut conns,
                        &pa.id,
                        &format!("outside:{}", o.id),
                        "uses",
                        Sure::Certain,
                        format!("{file} calls {}", r.host),
                    );
                    outside_seen.entry(o.id.clone()).or_insert(o);
                    continue;
                }
                let at = RequestAt {
                    repo: m.repo.clone(),
                    part: pa.id.clone(),
                    file: file.clone(),
                    request: r.clone(),
                };
                let cm = match_request(&at, &endpoints, &by_id);
                if let Some(sure) = cm.sure {
                    for e in &cm.to {
                        if e.part != pa.id {
                            bump(
                                &mut conns,
                                &pa.id,
                                &e.part,
                                "calls",
                                sure,
                                format!("{}:{} {} {}", file, r.line, r.method, r.path),
                            );
                        }
                    }
                } else if !r.host.is_empty()
                    && !r.host.starts_with("localhost")
                    && !r.host.starts_with("127.")
                {
                    // An outside address no known service: named as itself.
                    let o = outside(&format!("host:{}", r.host), &r.host, "an outside address");
                    bump(
                        &mut conns,
                        &pa.id,
                        &format!("outside:{}", o.id),
                        "uses",
                        Sure::Likely,
                        format!("{file} calls {}", r.host),
                    );
                    outside_seen.entry(o.id.clone()).or_insert(o);
                }
                calls.push(cm);
            }
        }
    }
    TeamMap {
        parts,
        outside: outside_seen.into_values().collect(),
        connections: conns.into_values().collect(),
        calls,
        screens,
        repos: maps
            .iter()
            .map(|m| RepoStamp {
                repo: m.repo.clone(),
                commit: m.commit.clone(),
                updated_at: m.updated_at.clone(),
            })
            .collect(),
    }
}

// ── Views ──────────────────────────────────────────────────────────────────

fn part_json(p: &Part, with_files: bool) -> Value {
    let mut v = json!({
        "id": p.id, "repo": p.repo, "root": p.root, "kind": p.kind, "dev_name": p.dev_name,
        "name": p.name, "about": p.about, "described": p.described(), "described_at": p.described_at,
        "file_count": p.file_count,
    });
    if with_files {
        v["files"] = json!(p.files);
    }
    v
}

fn call_json(c: &CallMatch) -> Value {
    json!({
        "repo": c.from.repo, "part": c.from.part, "file": c.from.file, "line": c.from.request.line,
        "method": c.from.request.method, "path": c.from.request.path, "base": c.from.request.base,
        "via": c.from.request.via, "sure": c.sure.map(Sure::as_str).unwrap_or("unmatched"),
        "how": c.how,
        "to": c.to.iter().map(|e| json!({
            "repo": e.repo, "part": e.part, "file": e.file, "line": e.line, "method": e.method,
            "path": e.mounted.as_deref().unwrap_or(&e.path), "framework": e.framework,
        })).collect::<Vec<_>>(),
    })
}

/// The whole map, for the page's first view: parts without their file
/// lists, every line, the outside services, the screens, and counts of
/// what is not matched.
pub fn overview(map: &TeamMap) -> Value {
    let unmatched = map.calls.iter().filter(|c| c.sure.is_none()).count();
    let guessed = map
        .calls
        .iter()
        .filter(|c| c.sure == Some(Sure::Guess))
        .count();
    json!({
        "parts": map.parts.iter().map(|p| {
            let mut v = part_json(p, false);
            v["endpoints"] = json!(endpoint_count(map, &p.id));
            v["screens"] = json!(map.screens.iter().filter(|s| s.part == p.id).count());
            v
        }).collect::<Vec<_>>(),
        "outside": map.outside,
        "connections": map.connections,
        "screens": map.screens,
        "repos": map.repos,
        "requests": {"total": map.calls.len(), "unmatched": unmatched, "guessed": guessed},
    })
}

fn endpoint_count(map: &TeamMap, part: &str) -> usize {
    let mut n = BTreeSet::new();
    for c in &map.calls {
        for e in c.to.iter().filter(|e| e.part == part) {
            n.insert((e.method.clone(), e.path.clone()));
        }
    }
    n.len()
}

/// One part: its files, endpoints, screens, requests (with how each was
/// matched), and its lines in and out.
pub fn part_view(map: &TeamMap, maps: &[RepoMap], id: &str) -> Option<Value> {
    let p = map.parts.iter().find(|p| p.id == id)?;
    let rm = maps.iter().find(|m| m.repo == p.repo)?;
    let mut endpoints = Vec::new();
    for (file, s) in &rm.surfaces {
        if part_of(rm, file).map(|x| x.id.as_str()) != Some(id) {
            continue;
        }
        for e in &s.endpoints {
            let callers = map
                .calls
                .iter()
                .filter(|c| {
                    c.to.iter()
                        .any(|t| t.file == *file && t.path == e.path && t.method == e.method)
                })
                .count();
            endpoints.push(json!({"file": file, "line": e.line, "method": e.method, "path": e.path, "framework": e.framework, "callers": callers}));
        }
    }
    let requests: Vec<Value> = map
        .calls
        .iter()
        .filter(|c| c.from.part == id)
        .map(call_json)
        .collect();
    Some(json!({
        "part": part_json(p, true),
        "endpoints": endpoints,
        "screens": map.screens.iter().filter(|s| s.part == id).collect::<Vec<_>>(),
        "requests": requests,
        "connections": map.connections.iter().filter(|c| c.from == id || c.to == id).collect::<Vec<_>>(),
    }))
}

/// One file: its part, what it says, and what it imports and is imported by.
pub fn file_view(map: &TeamMap, maps: &[RepoMap], repo: &str, path: &str) -> Option<Value> {
    let rm = maps.iter().find(|m| m.repo == repo)?;
    let part = part_of(rm, path)?;
    let in_part = part.files.iter().any(|f| f == path)
        || rm.surfaces.contains_key(path)
        || rm.imports.iter().any(|(a, b)| a == path || b == path);
    if !in_part {
        return None;
    }
    let imports: Vec<&String> = rm
        .imports
        .iter()
        .filter(|(a, _)| a == path)
        .map(|(_, b)| b)
        .take(60)
        .collect();
    let importers: Vec<&String> = rm
        .imports
        .iter()
        .filter(|(_, b)| b == path)
        .map(|(a, _)| a)
        .take(60)
        .collect();
    let s = rm.surfaces.get(path).cloned().unwrap_or_default();
    let requests: Vec<Value> = map
        .calls
        .iter()
        .filter(|c| c.from.repo == repo && c.from.file == path)
        .map(call_json)
        .collect();
    let callers: Vec<Value> = map
        .calls
        .iter()
        .filter(|c| c.to.iter().any(|t| t.repo == repo && t.file == path))
        .map(call_json)
        .collect();
    Some(json!({
        "repo": repo, "path": path, "part": part_json(part, false),
        "endpoints": s.endpoints, "screens": s.screens, "packages": s.packages,
        "outside": s.packages.iter().filter_map(|p| outside_of_package(p)).collect::<Vec<_>>(),
        "imports": imports, "importers": importers, "requests": requests, "callers": callers,
    }))
}

/// Everything behind one screen: the files it reaches through imports, the
/// requests they make, the endpoints that answer them, the parts those live
/// in, and the outside services behind them.
pub fn screen_chain(map: &TeamMap, maps: &[RepoMap], screen_id: &str) -> Option<Value> {
    let sc = map.screens.iter().find(|s| s.id == screen_id)?;
    let rm = maps.iter().find(|m| m.repo == sc.repo)?;
    let reach = |m: &RepoMap, start: &str, depth: usize, max: usize| -> Vec<String> {
        let mut out = vec![start.to_string()];
        let mut seen: HashSet<String> = HashSet::from([start.to_string()]);
        let mut q = VecDeque::from([(start.to_string(), 0usize)]);
        while let Some((f, d)) = q.pop_front() {
            if d >= depth || out.len() >= max {
                continue;
            }
            for (_, b) in m.imports.iter().filter(|(a, _)| *a == f) {
                if seen.insert(b.clone()) {
                    out.push(b.clone());
                    q.push_back((b.clone(), d + 1));
                }
            }
        }
        out
    };
    let files = reach(rm, &sc.file, 4, 80);
    let calls: Vec<&CallMatch> = map
        .calls
        .iter()
        .filter(|c| c.from.repo == sc.repo && files.contains(&c.from.file))
        .collect();
    let mut parts: BTreeSet<String> = BTreeSet::from([sc.part.clone()]);
    let mut endpoint_files: Vec<(String, String)> = Vec::new();
    for c in &calls {
        for e in &c.to {
            parts.insert(e.part.clone());
            endpoint_files.push((e.repo.clone(), e.file.clone()));
        }
    }
    let mut outside_ids: BTreeSet<String> = BTreeSet::new();
    let mut behind: Vec<Value> = Vec::new();
    for (repo, file) in endpoint_files.iter().collect::<BTreeSet<_>>() {
        let Some(m) = maps.iter().find(|m| &m.repo == repo) else {
            continue;
        };
        let reached = reach(m, file, 3, 60);
        for f in &reached {
            if let Some(s) = m.surfaces.get(f) {
                for p in &s.packages {
                    if let Some(o) = outside_of_package(p) {
                        if outside_ids.insert(o.id.clone()) {
                            behind
                                .push(json!({"outside": o, "repo": repo, "file": f, "via": file}));
                        }
                    }
                }
            }
        }
    }
    for f in &files {
        if let Some(s) = rm.surfaces.get(f) {
            for p in &s.packages {
                if let Some(o) = outside_of_package(p) {
                    if outside_ids.insert(o.id.clone()) {
                        behind.push(
                            json!({"outside": o, "repo": sc.repo, "file": f, "via": sc.file}),
                        );
                    }
                }
            }
        }
    }
    let connections: Vec<&Connection> = map
        .connections
        .iter()
        .filter(|c| {
            parts.contains(&c.from)
                && (parts.contains(&c.to)
                    || c.to
                        .strip_prefix("outside:")
                        .is_some_and(|o| outside_ids.contains(o)))
        })
        .collect();
    Some(json!({
        "screen": sc,
        "files": files,
        "requests": calls.iter().map(|c| call_json(c)).collect::<Vec<_>>(),
        "parts": map.parts.iter().filter(|p| parts.contains(&p.id)).map(|p| part_json(p, false)).collect::<Vec<_>>(),
        "outside": behind,
        "connections": connections,
    }))
}

/// Search over parts, screens, endpoints and files.
pub fn search(map: &TeamMap, maps: &[RepoMap], q: &str) -> Value {
    let q = q.trim().to_ascii_lowercase();
    if q.is_empty() {
        return json!({"parts": [], "screens": [], "endpoints": [], "files": []});
    }
    let hit = |s: &str| s.to_ascii_lowercase().contains(&q);
    let parts: Vec<Value> = map
        .parts
        .iter()
        .filter(|p| hit(&p.name) || hit(&p.dev_name) || hit(&p.about) || hit(&p.root))
        .take(20)
        .map(|p| part_json(p, false))
        .collect();
    let screens: Vec<&ScreenAt> = map
        .screens
        .iter()
        .filter(|s| hit(&s.route) || hit(&s.file))
        .take(20)
        .collect();
    let endpoints: Vec<Value> = endpoints_of(maps)
        .into_iter()
        .filter(|e| e.mounted.is_none() && (hit(&e.path) || hit(&format!("{} {}", e.method, e.path))))
        .take(20)
        .map(|e| json!({"repo": e.repo, "part": e.part, "file": e.file, "line": e.line, "method": e.method, "path": e.path}))
        .collect();
    let mut files = Vec::new();
    for p in &map.parts {
        for f in p.files.iter().filter(|f| hit(f)) {
            if files.len() >= 30 {
                break;
            }
            files.push(json!({"repo": p.repo, "part": p.id, "path": f}));
        }
    }
    json!({"parts": parts, "screens": screens, "endpoints": endpoints, "files": files})
}

// ── Overlays ───────────────────────────────────────────────────────────────

/// A CoderHelm run, as the Recent work overlay places it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub run_id: String,
    pub repo: String,
    pub ticket: String,
    pub title: String,
    /// Why, from the run's plan.
    pub why: String,
    pub pr_url: String,
    pub at: String,
    pub status: String,
    /// As the run keeps them: `owner/name/path` when it opened pull
    /// requests in several repositories.
    pub files: Vec<String>,
}

/// A finding, as the Health overlay places it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthItem {
    /// `security`, `infrastructure`, `logs` or `alert`.
    pub kind: String,
    pub id: String,
    pub title: String,
    pub severity: String,
    pub repo: String,
    pub files: Vec<String>,
    /// The dashboard page that shows it.
    pub link: String,
}

/// An overlay item with the parts and files it lands on.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Placed<T> {
    pub item: T,
    /// The parts it lands on.
    pub parts: Vec<String>,
    /// The files, as `{repo}:{path}`.
    pub files: Vec<String>,
    /// It names a repository but no file the map has: shown on the
    /// repository, not on a part.
    pub repo_only: bool,
}

fn split_repo_path<'a>(
    maps: &'a [RepoMap],
    repo: &str,
    f: &'a str,
) -> Option<(&'a RepoMap, String)> {
    // A path prefixed with its repository (`owner/name/src/x.ts`).
    for m in maps {
        if let Some(rest) = f.strip_prefix(&format!("{}/", m.repo)) {
            return Some((m, rest.to_string()));
        }
    }
    maps.iter()
        .find(|m| m.repo.eq_ignore_ascii_case(repo))
        .map(|m| (m, f.trim_start_matches("./").to_string()))
}

fn place(maps: &[RepoMap], repo: &str, files: &[String]) -> (Vec<String>, Vec<String>, bool) {
    let mut parts = BTreeSet::new();
    let mut placed = Vec::new();
    for f in files {
        let Some((m, path)) = split_repo_path(maps, repo, f) else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        if let Some(p) = part_of(m, &path) {
            // The root part only takes files it really holds.
            if p.root.is_empty() && !p.files.contains(&path) && !m.surfaces.contains_key(&path) {
                continue;
            }
            parts.insert(p.id.clone());
            placed.push(format!("{}:{path}", m.repo));
        }
    }
    let known_repo = maps.iter().any(|m| m.repo.eq_ignore_ascii_case(repo));
    let repo_only = parts.is_empty() && known_repo;
    (parts.into_iter().collect(), placed, repo_only)
}

/// Runs placed on the parts and files they changed. Runs in repositories
/// the map doesn't have are left out.
pub fn place_work(maps: &[RepoMap], items: Vec<WorkItem>) -> Vec<Placed<WorkItem>> {
    items
        .into_iter()
        .filter_map(|it| {
            let (parts, files, repo_only) = place(maps, &it.repo, &it.files);
            (!parts.is_empty() || repo_only).then_some(Placed {
                item: it,
                parts,
                files,
                repo_only,
            })
        })
        .collect()
}

/// Findings placed on the parts and files they concern; one that names a
/// repository but no file is kept on the repository.
pub fn place_health(maps: &[RepoMap], items: Vec<HealthItem>) -> Vec<Placed<HealthItem>> {
    items
        .into_iter()
        .filter_map(|it| {
            let (parts, files, repo_only) = place(maps, &it.repo, &it.files);
            (!parts.is_empty() || repo_only).then_some(Placed {
                item: it,
                parts,
                files,
                repo_only,
            })
        })
        .collect()
}

// ── Storage ────────────────────────────────────────────────────────────────

pub fn map_pk(team_id: &str) -> String {
    format!("AM#{team_id}")
}

pub fn repo_sk(repo: &str) -> String {
    format!("REPO#{repo}")
}

pub const STATUS_SK: &str = "STATUS";
/// A build queued or running this long is treated as stuck.
pub const STALE_SECS: i64 = 30 * 60;

pub fn encode(map: &RepoMap) -> Vec<u8> {
    use std::io::Write;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = gz.write_all(serde_json::to_string(map).unwrap_or_default().as_bytes());
    gz.finish().unwrap_or_default()
}

pub fn decode(bytes: &[u8]) -> Option<RepoMap> {
    use std::io::Read;
    let mut s = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut s)
        .ok()?;
    serde_json::from_str(&s).ok()
}

type Dynamo = aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue as A;

pub async fn load_repo(dynamo: &Dynamo, table: &str, team_id: &str, repo: &str) -> Option<RepoMap> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(map_pk(team_id)))
        .key("sk", A::S(repo_sk(repo)))
        .send()
        .await
        .ok()?;
    let b = out.item()?.get("data")?.as_b().ok()?;
    decode(b.as_ref())
}

pub async fn save_repo(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
    map: &RepoMap,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(repo_sk(&map.repo)))
        .item("data", A::B(aws_smithy_types::Blob::new(encode(map))))
        .item("commit", A::S(map.commit.clone()))
        .item("updated_at", A::S(map.updated_at.clone()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| {
            format!(
                "app map write: {}",
                aws_sdk_dynamodb::error::DisplayErrorContext(e)
            )
        })
}

/// Every repository's map of a team. Only ever reads the team's own
/// partition.
pub async fn load_all(dynamo: &Dynamo, table: &str, team_id: &str) -> Result<Vec<RepoMap>, String> {
    let pk = map_pk(team_id);
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let page = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", A::S(pk.clone()))
            .expression_attribute_values(":p", A::S("REPO#".into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                format!(
                    "app map read: {}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(e)
                )
            })?;
        for it in page.items() {
            if let Some(m) = it
                .get("data")
                .and_then(|v| v.as_b().ok())
                .and_then(|b| decode(b.as_ref()))
            {
                out.push(m);
            }
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out.sort_by_key(|a| a.repo.to_lowercase());
    Ok(out)
}

pub async fn delete_repo(dynamo: &Dynamo, table: &str, team_id: &str, repo: &str) {
    let _ = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", A::S(map_pk(team_id)))
        .key("sk", A::S(repo_sk(repo)))
        .send()
        .await;
}

/// Where the team's map build is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// `queued`, `building`, `ready` or `failed`.
    pub state: String,
    pub at: i64,
    pub done: u32,
    pub total: u32,
    #[serde(default)]
    pub error: String,
}

impl Status {
    pub fn is_stale(&self, now: i64) -> bool {
        matches!(self.state.as_str(), "queued" | "building") && now - self.at > STALE_SECS
    }
}

pub async fn status(dynamo: &Dynamo, table: &str, team_id: &str) -> Option<Status> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(map_pk(team_id)))
        .key("sk", A::S(STATUS_SK.into()))
        .send()
        .await
        .ok()?;
    let s = out.item()?.get("json")?.as_s().ok()?;
    serde_json::from_str(s).ok()
}

pub async fn set_status(dynamo: &Dynamo, table: &str, team_id: &str, st: &Status) {
    let _ = dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(STATUS_SK.into()))
        .item("json", A::S(serde_json::to_string(st).unwrap_or_default()))
        .item("state", A::S(st.state.clone()))
        .item("at", A::N(st.at.to_string()))
        .send()
        .await;
}

/// Marks a whole-team build queued: once, unless the last one got stuck,
/// or always with `force`. `Ok(true)` means the caller sends the message.
pub async fn mark_queued(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
    force: bool,
) -> Result<bool, String> {
    let now = chrono::Utc::now().timestamp();
    let st = Status {
        state: "queued".into(),
        at: now,
        ..Default::default()
    };
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(STATUS_SK.into()))
        .item("json", A::S(serde_json::to_string(&st).unwrap_or_default()))
        .item("state", A::S("queued".into()))
        .item("at", A::N(now.to_string()));
    if !force {
        put = put
            .condition_expression(
                "attribute_not_exists(sk) OR NOT (#st IN (:q, :b)) OR #at < :cutoff",
            )
            .expression_attribute_names("#st", "state")
            .expression_attribute_names("#at", "at")
            .expression_attribute_values(":q", A::S("queued".into()))
            .expression_attribute_values(":b", A::S("building".into()))
            .expression_attribute_values(":cutoff", A::N((now - STALE_SECS).to_string()));
    }
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let held = e
                .as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false);
            if held {
                Ok(false)
            } else {
                Err(format!(
                    "{}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(e)
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_surface::scan;

    /// A made-up harbor app in two repositories: a Next.js web app that
    /// books slips, and an Express API that answers it and charges through
    /// Stripe.
    pub(crate) fn harbor() -> (RepoInput, RepoInput) {
        let web_files: Vec<(&str, &str)> = vec![
            ("package.json", r#"{"name": "harbor-web", "dependencies": {"next": "15", "@sanity/client": "6"}}"#),
            (".env.example", "NEXT_PUBLIC_HARBOR_API=http://localhost:4000\nSANITY_PROJECT_ID=x\n"),
            ("src/app/slips/[id]/page.tsx", "import { getSlip } from '../../../lib/harbor';\nexport default function Page() {}"),
            ("src/app/tides/page.tsx", "export default function Tides() { fetch('/api/tides'); }"),
            ("src/app/api/tides/route.ts", "export async function GET() {}"),
            ("src/lib/harbor.ts", "export const getSlip = (id) => fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/v1/slips/${id}`);\nexport const book = (id) => fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/v1/slips/${id}/book`, { method: 'POST' });\nexport const lost = () => fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/lighthouse`);\nexport const any = (u) => fetch(u);"),
        ];
        let api_files: Vec<(&str, &str)> = vec![
            ("package.json", r#"{"name": "harbor-api", "dependencies": {"express": "4", "stripe": "14"}}"#),
            ("src/server.ts", "import express from 'express';\nimport { slips } from './slips';\nconst app = express();\napp.use('/v1', slips);"),
            ("src/slips.ts", "import express from 'express';\nimport { charge } from './billing';\nexport const slips = express.Router();\nslips.get('/slips/:id', (req, res) => {});\nslips.post('/slips/:id/book', async (req, res) => { await charge(); });"),
            ("src/billing.ts", "import Stripe from 'stripe';\nexport async function charge() {}"),
            ("infra/main.tf", "resource \"x\" \"y\" {}"),
        ];
        let input = |repo: &str, files: &[(&str, &str)], imports: Vec<(&str, &str)>| RepoInput {
            repo: repo.to_string(),
            commit: "c1".into(),
            files: files.iter().map(|(p, _)| p.to_string()).collect(),
            surfaces: files
                .iter()
                .map(|(p, s)| (p.to_string(), scan(p, s)))
                .filter(|(_, s)| !s.is_empty())
                .collect(),
            imports: imports
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        };
        (
            input(
                "harbor/web",
                &web_files,
                vec![("src/app/slips/[id]/page.tsx", "src/lib/harbor.ts")],
            ),
            input(
                "harbor/api",
                &api_files,
                vec![
                    ("src/server.ts", "src/slips.ts"),
                    ("src/slips.ts", "src/billing.ts"),
                ],
            ),
        )
    }

    pub(crate) fn harbor_maps() -> Vec<RepoMap> {
        let (web, api) = harbor();
        [web, api]
            .iter()
            .map(|i| repo_map(i, build_parts(i), "now"))
            .collect()
    }

    #[test]
    fn parts_follow_packages_and_folders() {
        let (web, api) = harbor();
        let wp = build_parts(&web);
        assert_eq!(wp.len(), 1);
        assert_eq!(wp[0].kind, "web app");
        assert_eq!(wp[0].dev_name, "harbor-web");
        assert_eq!(wp[0].id, "harbor/web#.");
        let ap = build_parts(&api);
        let kinds: Vec<(&str, &str)> = ap
            .iter()
            .map(|p| (p.root.as_str(), p.kind.as_str()))
            .collect();
        assert_eq!(kinds, vec![("", "server")]);
        // A monorepo: each manifest folder is a part, loose folders by top folder.
        let mono = RepoInput {
            repo: "harbor/mono".into(),
            files: vec![
                "apps/dock/package.json".into(),
                "apps/dock/src/a.ts".into(),
                "packages/tides/package.json".into(),
                "packages/tides/index.ts".into(),
                "infra/main.tf".into(),
                "README.md".into(),
            ],
            ..Default::default()
        };
        let roots: Vec<String> = build_parts(&mono).iter().map(|p| p.root.clone()).collect();
        assert_eq!(roots, vec!["", "apps/dock", "infra", "packages/tides"]);
        // A large package splits into its feature folders.
        let mut files = vec!["package.json".to_string()];
        for f in ["slips", "boats", "tides"] {
            for i in 0..30 {
                files.push(format!("src/{f}/f{i}.ts"));
            }
        }
        let big = RepoInput {
            repo: "harbor/big".into(),
            files,
            ..Default::default()
        };
        let roots: Vec<String> = build_parts(&big).iter().map(|p| p.root.clone()).collect();
        assert_eq!(roots, vec!["", "src/boats", "src/slips", "src/tides"]);
    }

    #[test]
    fn only_new_or_changed_parts_are_described_again() {
        let (web, _) = harbor();
        let mut first = build_parts(&web);
        let todo = carry_descriptions(None, &mut first);
        assert_eq!(todo, vec!["harbor/web#.".to_string()]);
        let reply = json!({"parts": [{"id": "harbor/web#.", "name": "Harbor website", "about": "Lets boaters book slips -> and check tides."}, {"id": "other", "name": "x", "about": "y"}]});
        assert_eq!(
            apply_descriptions(&reply, &todo, &mut first, "c1"),
            vec!["harbor/web#.".to_string()]
        );
        assert_eq!(first[0].about, "Lets boaters book slips and check tides.");
        let old = repo_map(&web, first, "t");
        // A plain edit (same files, same facts): nothing to describe.
        let mut again = build_parts(&web);
        assert!(carry_descriptions(Some(&old), &mut again).is_empty());
        assert_eq!(again[0].name, "Harbor website");
        assert_eq!(again[0].described_at, "c1");
        // A new screen: described again, the older text kept meanwhile.
        let mut changed = web.clone();
        changed.files.push("src/app/fuel/page.tsx".into());
        changed.surfaces.insert(
            "src/app/fuel/page.tsx".into(),
            scan("src/app/fuel/page.tsx", ""),
        );
        let mut parts = build_parts(&changed);
        assert_eq!(
            carry_descriptions(Some(&old), &mut parts),
            vec!["harbor/web#.".to_string()]
        );
        assert_eq!(parts[0].name, "Harbor website");
        assert!(
            describe_prompt(&[&parts[0]], &changed.surfaces, &BTreeMap::new()).contains("/fuel")
        );
    }

    #[test]
    fn requests_match_in_the_same_repo_across_repos_and_not_at_all() {
        let maps = harbor_maps();
        let map = assemble(&maps);
        let find = |path: &str| {
            map.calls
                .iter()
                .find(|c| c.from.request.path == path)
                .unwrap()
        };
        // Same repository, same origin: certain.
        let tides = find("/api/tides");
        assert_eq!(tides.sure, Some(Sure::Certain));
        assert_eq!(tides.to[0].file, "src/app/api/tides/route.ts");
        // Across repositories through a mount prefix: likely, and says so.
        let slip = find("/v1/slips/:p");
        assert_eq!(slip.sure, Some(Sure::Likely));
        assert_eq!(slip.to[0].repo, "harbor/api");
        assert!(slip.how.contains("NEXT_PUBLIC_HARBOR_API"), "{}", slip.how);
        let book = find("/v1/slips/:p/book");
        assert_eq!(book.to[0].method, "POST");
        // Nothing answers it: unmatched, with the reason, and no line drawn.
        let lost = find("/lighthouse");
        assert_eq!(lost.sure, None);
        assert!(lost.how.starts_with("No endpoint"));
        let dynamic = map.calls.iter().find(|c| c.from.request.dynamic).unwrap();
        assert!(dynamic.how.contains("built at run time"));
        let calls: Vec<&Connection> = map
            .connections
            .iter()
            .filter(|c| c.kind == "calls")
            .collect();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            (calls[0].from.as_str(), calls[0].to.as_str()),
            ("harbor/web#.", "harbor/api#.")
        );
        assert_eq!(calls[0].sure, Sure::Likely);
        // Outside services from packages and setting names.
        let ids: Vec<&str> = map.outside.iter().map(|o| o.id.as_str()).collect();
        assert!(
            ids.contains(&"stripe") && ids.contains(&"sanity"),
            "{ids:?}"
        );
    }

    #[test]
    fn several_answers_are_a_guess_unless_the_address_names_one() {
        let ep = |repo: &str, part: &str| EndpointAt {
            repo: repo.into(),
            part: part.into(),
            file: "x.ts".into(),
            method: "GET".into(),
            path: "/boats/:id".into(),
            framework: "Express-style route".into(),
            line: 1,
            mounted: None,
        };
        let parts_v = [
            Part {
                id: "harbor/dock-api#.".into(),
                dev_name: "dock-api".into(),
                ..Default::default()
            },
            Part {
                id: "harbor/fuel-api#.".into(),
                dev_name: "fuel-api".into(),
                ..Default::default()
            },
        ];
        let parts: HashMap<&str, &Part> = parts_v.iter().map(|p| (p.id.as_str(), p)).collect();
        let eps = vec![
            ep("harbor/dock-api", "harbor/dock-api#."),
            ep("harbor/fuel-api", "harbor/fuel-api#."),
        ];
        let req = |base: &str| RequestAt {
            repo: "harbor/web".into(),
            part: "harbor/web#.".into(),
            file: "a.ts".into(),
            request: Request {
                method: "GET".into(),
                path: "/boats/:p".into(),
                base: base.into(),
                line: 3,
                via: "fetch".into(),
                ..Default::default()
            },
        };
        let m = match_request(&req("API_URL"), &eps, &parts);
        assert_eq!(m.sure, Some(Sure::Guess));
        assert_eq!(m.to.len(), 2);
        let m = match_request(&req("NEXT_PUBLIC_DOCK_API_URL"), &eps, &parts);
        assert_eq!(m.sure, Some(Sure::Likely));
        assert_eq!(m.to[0].repo, "harbor/dock-api");
        // Paths: literals beat parameters; a catch-all takes the rest.
        assert_eq!(fit("/boats/1", "/boats/:id"), Some(1));
        assert_eq!(fit("/boats/mine", "/boats/mine"), Some(2));
        assert_eq!(fit("/docs/a/b", "/docs/:slug*"), Some(1));
        assert_eq!(fit("/boats", "/boats/:id"), None);
    }

    #[test]
    fn a_screen_reaches_its_endpoints_and_the_services_behind_them() {
        let maps = harbor_maps();
        let map = assemble(&maps);
        let sc = map
            .screens
            .iter()
            .find(|s| s.route == "/slips/:id")
            .unwrap();
        let chain = screen_chain(&map, &maps, &sc.id).unwrap();
        let files: Vec<&str> = chain["files"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(files.contains(&"src/lib/harbor.ts"));
        let ends: Vec<String> = chain["requests"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|r| {
                r["to"].as_array().unwrap().iter().map(|t| {
                    format!(
                        "{} {} {}",
                        t["repo"].as_str().unwrap(),
                        t["method"].as_str().unwrap(),
                        t["path"].as_str().unwrap()
                    )
                })
            })
            .collect();
        assert!(
            ends.contains(&"harbor/api GET /v1/slips/:id".to_string()),
            "{ends:?}"
        );
        let behind: Vec<&str> = chain["outside"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|o| o["outside"]["id"].as_str())
            .collect();
        assert!(behind.contains(&"stripe"), "{behind:?}");
        let parts: Vec<&str> = chain["parts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["id"].as_str())
            .collect();
        assert_eq!(parts, vec!["harbor/web#.", "harbor/api#."]);
    }

    #[test]
    fn overlays_land_on_their_parts_and_files() {
        let maps = harbor_maps();
        let work = place_work(
            &maps,
            vec![
                WorkItem {
                    run_id: "r1".into(),
                    repo: "harbor/api".into(),
                    files: vec!["src/slips.ts".into()],
                    ..Default::default()
                },
                WorkItem {
                    run_id: "r2".into(),
                    repo: "harbor/web".into(),
                    files: vec![
                        "harbor/web/src/lib/harbor.ts".into(),
                        "harbor/api/src/billing.ts".into(),
                    ],
                    ..Default::default()
                },
                WorkItem {
                    run_id: "r3".into(),
                    repo: "elsewhere/x".into(),
                    files: vec!["a.ts".into()],
                    ..Default::default()
                },
            ],
        );
        assert_eq!(work.len(), 2);
        assert_eq!(work[0].parts, vec!["harbor/api#."]);
        assert_eq!(work[0].files, vec!["harbor/api:src/slips.ts"]);
        assert_eq!(work[1].parts, vec!["harbor/api#.", "harbor/web#."]);
        let health = place_health(
            &maps,
            vec![
                HealthItem {
                    kind: "security".into(),
                    id: "s1".into(),
                    repo: "harbor/api".into(),
                    files: vec!["package.json".into()],
                    ..Default::default()
                },
                HealthItem {
                    kind: "logs".into(),
                    id: "l1".into(),
                    repo: "harbor/web".into(),
                    files: vec![],
                    ..Default::default()
                },
            ],
        );
        assert_eq!(health[0].parts, vec!["harbor/api#."]);
        assert!(!health[0].repo_only);
        assert!(health[1].repo_only && health[1].parts.is_empty());
    }

    #[test]
    fn views_and_storage_round_trip() {
        let maps = harbor_maps();
        let map = assemble(&maps);
        let o = overview(&map);
        assert!(o["requests"]["unmatched"].as_u64().unwrap() >= 2);
        let pv = part_view(&map, &maps, "harbor/api#.").unwrap();
        assert!(pv["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["callers"].as_u64() == Some(1)));
        assert!(part_view(&map, &maps, "other/team#.").is_none());
        let fv = file_view(&map, &maps, "harbor/api", "src/slips.ts").unwrap();
        assert_eq!(fv["imports"], json!(["src/billing.ts"]));
        let s = search(&map, &maps, "slips");
        assert!(!s["endpoints"].as_array().unwrap().is_empty());
        assert_eq!(decode(&encode(&maps[0])), Some(maps[0].clone()));
        assert_eq!(map_pk("t1"), "AM#t1");
        assert!(outside_of_package("@aws-sdk/client-s3")
            .unwrap()
            .name
            .starts_with("AWS"));
        assert_eq!(
            outside_of_package("aws_sdk_dynamodb").unwrap().id,
            "aws-dynamodb"
        );
        assert_eq!(outside_of_host("api.stripe.com").unwrap().id, "stripe");
        assert_eq!(
            outside_of_setting("NEXT_PUBLIC_SUPABASE_URL").unwrap().id,
            "supabase"
        );
    }
}
