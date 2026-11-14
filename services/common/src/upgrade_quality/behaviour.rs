//! Behaviour changes of a major bump are found, never assumed.
//!
//! A dependency pull request said "contract unchanged". A validation
//! library's major had changed the text and the shape of the 400 error
//! bodies of two endpoints. The type check passed, as it does on such a
//! change: the signatures were the same and the answers were not.
//!
//! For a major whose package the repository calls:
//!
//! - [`used_breaking`] takes the breaking changes of its release notes
//!   that name an API the repository calls (`upgrade_notes` read both).
//! - [`plan`] finds the requests that go through those call sites: the
//!   routes declared in the files that call the package ([`routes`], one
//!   table of route syntaxes), each asked the way that reaches the error
//!   paths a major changes: an empty body, a malformed one, and for an
//!   upload a small file and one over the size limit.
//! - The runtime proof sends each request to the application built from
//!   the base branch and from the head (`runtime_proof`, twice each, so an
//!   answer that varies by itself is known). [`compare`] sets the answers
//!   side by side: status, content type and body.
//! - A difference is a known behaviour change, recorded with the request
//!   that shows it and the other repositories of the team that name the
//!   endpoint.
//! - [`Behaviour::unchanged_shown`] is the only thing that lets the word
//!   "unchanged" stand: every request answered the same on both builds,
//!   and at least one reached the code. [`guard`] takes the sentence out of
//!   any text that says it without that, and the section then says what
//!   was and was not checked.
//!
//! Everything here is pure; the requests are sent by the worker's runtime
//! proof.

use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::runtime_proof::{ProbeAnswer, ProbeRequest};
use crate::upgrade_notes::PackageNotes;

/// Requests one proof sends at most: the sandbox keeps only the end of a
/// build's output.
pub const MAX_PROBES: usize = 6;
/// The upload size sent to reach a size limit the code does not state.
const DEFAULT_LIMIT: u64 = 16 * 1024 * 1024;
/// The largest upload a probe sends.
const MAX_UPLOAD: u64 = 64 * 1024 * 1024;

/// A route a file declares.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Route {
    pub method: String,
    pub path: String,
    pub file: String,
    /// The file takes uploads.
    pub upload: bool,
    /// The form field an upload is read from.
    pub field: String,
    /// The size limit the file states, in bytes.
    pub limit: Option<u64>,
}

/// How routes are written. One row per syntax; the first group is the
/// method (or empty for a row whose method is in the third), the second
/// the path.
struct Syntax {
    re: &'static LazyLock<Regex>,
    /// A prefix every route of the file gets, when the file declares one.
    prefix: Option<&'static LazyLock<Regex>>,
}

static DECORATOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"@(Get|Post|Put|Patch)(?:Mapping)?\(\s*(?:value\s*=\s*)?(?:['"]([^'"]*)['"])?"#)
        .unwrap()
});
static DECORATOR_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"@(?:Controller|RequestMapping)\(\s*(?:value\s*=\s*)?['"]([^'"]*)['"]"#).unwrap()
});
static CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b[A-Za-z_]\w*\.(get|post|put|patch)\(\s*['"`](/[^'"`\s]*)['"`]"#).unwrap()
});
static ROUTE_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"\.route\(\s*['"](/[^'"\s]*)['"](?:[^)\n]*methods\s*=\s*\[\s*['"]([A-Za-z]+)['"])?"#,
    )
    .unwrap()
});
static UPLOAD_FIELD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?:single|FileInterceptor|FormFile|array)\(\s*['"](\w+)['"]"#).unwrap()
});
static LIMIT_PRODUCT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:fileSize|max_?(?:file_?|upload_?|content_?)?(?:size|length)|limit)\s*[:=]\s*([0-9][0-9_ *]*)")
        .unwrap()
});
static LIMIT_WORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)['"](\d+)\s*(kb|mb)['"]"#).unwrap());

const SYNTAXES: &[Syntax] = &[
    // `@Post('upload')` under `@Controller('files')`, `@PostMapping("/x")`.
    Syntax {
        re: &DECORATOR,
        prefix: Some(&DECORATOR_PREFIX),
    },
    // `app.post('/x', ...)`, `router.GET("/x", ...)`, `@app.get("/x")`.
    Syntax {
        re: &CALL,
        prefix: None,
    },
];

/// Words that say a file takes uploads.
const UPLOAD_SIGNS: &[&str] = &[
    "multipart",
    "fileinterceptor",
    "uploadfile",
    "formfile",
    "upload.single",
    "upload.array",
    "uploadedfile",
];

/// A path with its parameters filled in (`/files/:id` is `/files/1`).
fn concrete(path: &str) -> String {
    let mut out: Vec<String> = vec![];
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        let param = seg.starts_with(':')
            || (seg.starts_with('{') && seg.ends_with('}'))
            || (seg.starts_with('<') && seg.ends_with('>'));
        out.push(if param {
            "1".to_string()
        } else if seg == "*" {
            "x".to_string()
        } else {
            seg.to_string()
        });
    }
    format!("/{}", out.join("/"))
}

/// A path a shell command and a URL can carry as it is.
fn plain_path(p: &str) -> bool {
    p.starts_with('/')
        && p.len() <= 120
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '.' | '~'))
}

fn limit_of(text: &str) -> Option<u64> {
    if let Some(c) = LIMIT_PRODUCT.captures(text) {
        let product: Option<u64> = c[1]
            .split('*')
            .map(|n| n.replace(['_', ' '], "").parse::<u64>().ok())
            .try_fold(1u64, |acc, n| n.and_then(|n| acc.checked_mul(n)));
        if let Some(p) = product.filter(|p| *p >= 1024) {
            return Some(p);
        }
    }
    LIMIT_WORD.captures(text).and_then(|c| {
        let n: u64 = c[1].parse().ok()?;
        Some(match c[2].to_ascii_lowercase().as_str() {
            "kb" => n * 1024,
            _ => n * 1024 * 1024,
        })
    })
}

/// The routes `text` declares, in the order it declares them. Routes that
/// remove something are never probed, so they are not read.
pub fn routes(file: &str, text: &str) -> Vec<Route> {
    let lower = text.to_ascii_lowercase();
    let upload = UPLOAD_SIGNS.iter().any(|s| lower.contains(s));
    let field = UPLOAD_FIELD
        .captures(text)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| "file".to_string());
    let limit = limit_of(text);
    let mut found: Vec<(usize, String, String)> = vec![];
    for syn in SYNTAXES {
        let prefix = syn
            .prefix
            .and_then(|p| p.captures(text))
            .map(|c| c[1].to_string())
            .unwrap_or_default();
        for c in syn.re.captures_iter(text) {
            let at = c.get(0).map(|m| m.start()).unwrap_or(0);
            let method = c[1].to_ascii_uppercase();
            let path = c.get(2).map(|m| m.as_str()).unwrap_or("");
            found.push((at, method, concrete(&format!("{prefix}/{path}"))));
        }
    }
    for c in ROUTE_CALL.captures_iter(text) {
        let at = c.get(0).map(|m| m.start()).unwrap_or(0);
        let method = c
            .get(2)
            .map(|m| m.as_str().to_ascii_uppercase())
            .unwrap_or_else(|| "GET".to_string());
        if matches!(method.as_str(), "GET" | "POST" | "PUT" | "PATCH") {
            found.push((at, method, concrete(&c[1])));
        }
    }
    found.sort();
    let mut out: Vec<Route> = vec![];
    for (_, method, path) in found {
        if !plain_path(&path) || out.iter().any(|r| r.method == method && r.path == path) {
            continue;
        }
        out.push(Route {
            method,
            path,
            file: file.to_string(),
            upload,
            field: field.clone(),
            limit,
        });
    }
    out
}

const BOUNDARY: &str = "coderhelmprobe";

/// The requests that exercise one route: as it is for a read; for a
/// write, the bodies that reach the paths a major changes.
pub fn requests_for(r: &Route) -> Vec<ProbeRequest> {
    let req =
        |what: String, content_type: &str, body: String, pad_kb: u32, tail: &str| ProbeRequest {
            method: r.method.clone(),
            path: r.path.clone(),
            content_type: content_type.to_string(),
            body,
            pad_kb,
            tail: tail.to_string(),
            what,
        };
    if r.method == "GET" {
        return vec![req("a plain request".into(), "", String::new(), 0, "")];
    }
    if r.upload {
        let head = format!(
            "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{}\"; filename=\"probe.txt\"\r\nContent-Type: text/plain\r\n\r\nprobe",
            r.field
        );
        let tail = format!("\r\n--{BOUNDARY}--\r\n");
        let ct = format!("multipart/form-data; boundary={BOUNDARY}");
        let limit = r.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_UPLOAD);
        let over_kb = (limit / 1024 + 64) as u32;
        return vec![
            req(
                format!("a small upload in the field `{}`", r.field),
                &ct,
                head.clone(),
                0,
                &tail,
            ),
            req(
                match r.limit {
                    Some(l) => format!(
                        "an upload over the size limit the file states ({} KiB)",
                        l / 1024
                    ),
                    None => format!(
                        "an upload of {} MiB (the file states no size limit)",
                        over_kb / 1024
                    ),
                },
                &ct,
                head,
                over_kb,
                &tail,
            ),
        ];
    }
    vec![
        req(
            "an empty JSON object (the validation error)".into(),
            "application/json",
            "{}".into(),
            0,
            "",
        ),
        req(
            "a malformed JSON body (the parser error)".into(),
            "application/json",
            "{\"probe\":".into(),
            0,
            "",
        ),
    ]
}

/// The breaking changes of a package's notes that name an API the
/// repository calls.
pub fn used_breaking(n: &PackageNotes) -> Vec<String> {
    let called: Vec<&str> = n
        .call_sites
        .iter()
        .map(|c| c.api.trim_end_matches("()"))
        .filter(|a| !a.is_empty())
        .collect();
    n.breaking
        .iter()
        .filter(|line| called.iter().any(|api| line.contains(api)))
        .cloned()
        .collect()
}

/// What a proof probes for the majors of a run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Planned {
    pub requests: Vec<ProbeRequest>,
    /// Each probed package with its requests, by index.
    pub of: Vec<(String, Vec<usize>)>,
    /// Each major that is not probed, with why.
    pub not_probed: Vec<(String, String)>,
}

fn files_named(files: &[&str]) -> String {
    let shown: Vec<String> = files.iter().take(3).map(|f| format!("`{f}`")).collect();
    shown.join(", ")
}

/// The requests for the majors among `notes`, through the files that call
/// them. `files`: those call-site files with their text. A major with no
/// call site, or none behind a route, is named as not probed with why.
pub fn plan(notes: &[PackageNotes], files: &[(String, String)]) -> Planned {
    let mut out = Planned::default();
    let mut per: Vec<(String, Vec<ProbeRequest>)> = vec![];
    for n in notes.iter().filter(|n| n.major) {
        let mut sites: Vec<&str> = n.call_sites.iter().map(|c| c.file.as_str()).collect();
        sites.sort();
        sites.dedup();
        if sites.is_empty() {
            out.not_probed.push((
                n.package.clone(),
                if !n.read() {
                    format!(
                        "its release notes were not read ({}), so the APIs it changes are not known",
                        if n.missing.is_empty() { "none were found" } else { &n.missing }
                    )
                } else {
                    "no call of an API its notes name was found in the repository".to_string()
                },
            ));
            continue;
        }
        let mut reqs: Vec<ProbeRequest> = vec![];
        for site in &sites {
            let Some((_, text)) = files.iter().find(|(p, _)| p == site) else {
                continue;
            };
            for r in routes(site, text) {
                for q in requests_for(&r) {
                    if !reqs
                        .iter()
                        .any(|x| x.method == q.method && x.path == q.path && x.what == q.what)
                    {
                        reqs.push(q);
                    }
                }
            }
        }
        if reqs.is_empty() {
            out.not_probed.push((
                n.package.clone(),
                format!(
                    "no route is declared in the files that call it ({}), so its callers are not behind a request CoderHelm can send; a job or a library function has no before-and-after probe here, and the repository's tests are the check",
                    files_named(&sites)
                ),
            ));
        } else {
            per.push((n.package.clone(), reqs));
        }
    }
    // One request of each package in turn, so a package with many routes
    // never takes every place.
    let mut round = 0;
    while out.requests.len() < MAX_PROBES && per.iter().any(|(_, r)| r.len() > round) {
        for (package, reqs) in &per {
            if out.requests.len() >= MAX_PROBES {
                break;
            }
            let Some(q) = reqs.get(round) else {
                continue;
            };
            let idx = out.requests.len();
            out.requests.push(q.clone());
            match out.of.iter_mut().find(|(p, _)| p == package) {
                Some((_, v)) => v.push(idx),
                None => out.of.push((package.clone(), vec![idx])),
            }
        }
        round += 1;
    }
    out
}

/// How one request's answers compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Same,
    Changed,
    /// A build gave no answer.
    #[default]
    NoAnswer,
}

/// One request with its answers on the base branch's build and the head's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Compared {
    /// `POST /files/upload`
    pub request: String,
    pub what: String,
    /// `413 application/json`, or empty for no answer.
    pub base: String,
    pub head: String,
    pub outcome: Outcome,
    /// What differs, in words.
    pub detail: String,
    /// Both answers came from the route (not a 401, 403 or 404).
    pub reached: bool,
    /// The same request was answered differently twice by one build, so
    /// only its status and content type are compared.
    pub varies: bool,
}

fn answer_words(a: &ProbeAnswer) -> String {
    if a.content_type.is_empty() || a.content_type == "-" {
        a.status.to_string()
    } else {
        format!("{} {}", a.status, a.content_type)
    }
}

fn from_route(status: u16) -> bool {
    !matches!(status, 0 | 401 | 403 | 404)
}

/// The answers of the base branch's build and the head's, request by
/// request.
pub fn compare(
    requests: &[ProbeRequest],
    base: &[Option<ProbeAnswer>],
    head: &[Option<ProbeAnswer>],
) -> Vec<Compared> {
    requests
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let (b, h) = (
                base.get(i).and_then(Option::as_ref),
                head.get(i).and_then(Option::as_ref),
            );
            let mut c = Compared {
                request: format!("{} {}", q.method, q.path),
                what: q.what.clone(),
                base: b.map(answer_words).unwrap_or_default(),
                head: h.map(answer_words).unwrap_or_default(),
                ..Default::default()
            };
            let (Some(b), Some(h)) = (b, h) else {
                c.detail = match (b, h) {
                    (None, None) => "neither build answered".to_string(),
                    (None, _) => "the base branch's build did not answer".to_string(),
                    _ => "the head's build did not answer".to_string(),
                };
                return c;
            };
            c.reached = from_route(b.status) && from_route(h.status);
            c.varies = !b.stable || !h.stable;
            let mut diffs: Vec<String> = vec![];
            if b.status != h.status {
                diffs.push(format!("status {} to {}", b.status, h.status));
            }
            if b.content_type != h.content_type {
                diffs.push(format!(
                    "content type `{}` to `{}`",
                    b.content_type, h.content_type
                ));
            }
            if !c.varies && b.sha != h.sha {
                diffs.push(format!(
                    "body before `{}`, after `{}`",
                    crate::truncate_str(&b.excerpt.replace('`', "'"), 120),
                    crate::truncate_str(&h.excerpt.replace('`', "'"), 120)
                ));
            }
            c.outcome = if diffs.is_empty() {
                Outcome::Same
            } else {
                Outcome::Changed
            };
            c.detail = diffs.join("; ");
            c
        })
        .collect()
}

/// What is known of one major's behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Behaviour {
    pub package: String,
    pub from: String,
    pub to: String,
    /// The breaking changes of its notes that name an API the repository
    /// calls.
    pub breaking_used: Vec<String>,
    pub compared: Vec<Compared>,
    /// Why no request was sent; empty when some were.
    pub not_probed: String,
    /// The team's other repositories that name an endpoint whose answer
    /// changed.
    pub consumers: Vec<String>,
    /// The repositories searched for them, in words; empty when none were.
    pub consumers_searched: String,
}

impl Behaviour {
    /// The requests whose answers differ between the two builds.
    pub fn changes(&self) -> Vec<&Compared> {
        self.compared
            .iter()
            .filter(|c| c.outcome == Outcome::Changed)
            .collect()
    }

    /// Whether a probe shows its behaviour unchanged: every request was
    /// answered by both builds, the same, and at least one answer came
    /// from the route itself.
    pub fn unchanged_shown(&self) -> bool {
        !self.compared.is_empty()
            && self.compared.iter().all(|c| c.outcome == Outcome::Same)
            && self.compared.iter().any(|c| c.reached)
    }

    /// The endpoint paths whose answers changed: what a consumer names.
    pub fn changed_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .changes()
            .iter()
            .filter_map(|c| c.request.split_whitespace().nth(1).map(String::from))
            .collect();
        v.sort();
        v.dedup();
        v
    }

    /// What is known, as the pull request states it.
    pub fn section(&self) -> String {
        let mut s = format!("- `{}` {} to {}:\n", self.package, self.from, self.to);
        for b in self.breaking_used.iter().take(5) {
            s.push_str(&format!(
                "  - Its notes list a breaking change in an API this repository calls: {}\n",
                crate::truncate_str(b, 240)
            ));
        }
        if self.compared.is_empty() {
            s.push_str(&format!(
                "  - Not probed: {}. Nothing here says its behaviour is the same as before.\n",
                if self.not_probed.is_empty() {
                    "no request was sent"
                } else {
                    &self.not_probed
                }
            ));
            return s;
        }
        let changes = self.changes();
        let same = self
            .compared
            .iter()
            .filter(|c| c.outcome == Outcome::Same)
            .count();
        for c in &changes {
            s.push_str(&format!(
                "  - Known behaviour change: `{}` ({}) answered {} on the base branch's build and {} on this head's: {}.\n",
                c.request,
                c.what,
                if c.base.is_empty() { "nothing" } else { &c.base },
                if c.head.is_empty() { "nothing" } else { &c.head },
                c.detail
            ));
        }
        if !changes.is_empty() {
            s.push_str(&match (self.consumers.as_slice(), self.consumers_searched.is_empty()) {
                ([], true) => "  - Who could care: the team's other repositories were not searched for these endpoints.\n".to_string(),
                ([], false) => format!(
                    "  - Who could care: no other repository names these endpoints (searched {}).\n",
                    self.consumers_searched
                ),
                (found, _) => format!(
                    "  - Who could care: {} name{} these endpoints.\n",
                    found
                        .iter()
                        .map(|r| format!("`{r}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if found.len() == 1 { "s" } else { "" }
                ),
            });
        }
        let silent: Vec<&Compared> = self
            .compared
            .iter()
            .filter(|c| c.outcome == Outcome::NoAnswer)
            .collect();
        for c in &silent {
            s.push_str(&format!(
                "  - `{}` ({}): {}, so it shows nothing.\n",
                c.request, c.what, c.detail
            ));
        }
        if same > 0 {
            let reached = self
                .compared
                .iter()
                .filter(|c| c.outcome == Outcome::Same && c.reached)
                .count();
            let list: Vec<String> = self
                .compared
                .iter()
                .filter(|c| c.outcome == Outcome::Same)
                .map(|c| format!("`{}` ({}: {})", c.request, c.what, c.head))
                .collect();
            s.push_str(&format!(
                "  - Answered the same by both builds: {}.{}{}\n",
                list.join(", "),
                if reached == 0 {
                    " None of these answers came from the route itself (each was a 401, 403 or 404), so they show nothing about the package."
                } else if reached < same {
                    " Some of these were answered before the route (a 401, 403 or 404) and show nothing about the package."
                } else {
                    ""
                },
                if self.compared.iter().any(|c| c.varies) {
                    " A request answered differently twice by one build is compared by status and content type only."
                } else {
                    ""
                }
            ));
        }
        if self.unchanged_shown() {
            s.push_str(&format!(
                "  - Unchanged on these {} request{}; nothing is claimed beyond them.\n",
                self.compared.len(),
                if self.compared.len() == 1 { "" } else { "s" }
            ));
        }
        s
    }
}

/// What replaces a sentence that calls the behaviour unchanged with no
/// probe behind it.
pub const TAKEN_OUT: &str = "(CoderHelm took out a sentence here that called the contract or the behaviour unchanged: no probe or test recorded on this head shows that. This description lists what was checked; what it does not list was not.)";

static UNCHANGED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(unchanged|not changed|unaffected|remains? the same|stays? the same|identical|no (?:behaviou?r(?:al)?|contract|api|functional|breaking|runtime|observable)[a-z ]{0,20}changes?|does(?:n't| not) change|backwards?[- ]compatible|drop-in)\b",
    )
    .unwrap()
});
static SUBJECT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(contracts?|behaviou?rs?|responses?|apis?|endpoints?|error (?:bodies|body|messages?|shapes?)|interfaces?|outputs?|compatible|drop-in|semantics)\b",
    )
    .unwrap()
});

/// `text` with every sentence that calls a contract or behaviour
/// unchanged taken out, unless a probe shows it (`shown`). The first one
/// leaves [`TAKEN_OUT`] in its place; the others leave nothing.
pub fn guard(text: &str, shown: bool) -> String {
    if shown || !UNCHANGED.is_match(text) {
        return text.to_string();
    }
    let mut noted = false;
    let mut out: Vec<String> = vec![];
    for line in text.split('\n') {
        if !(UNCHANGED.is_match(line) && SUBJECT.is_match(line)) {
            out.push(line.to_string());
            continue;
        }
        // Sentence by sentence: the rest of the line stays.
        let lead_len = line.len() - line.trim_start().len();
        let (lead, body) = line.split_at(lead_len);
        let bullet = body
            .strip_prefix("- ")
            .or_else(|| body.strip_prefix("* "))
            .map(|rest| (&body[..2], rest));
        let (mark, body) = bullet.unwrap_or(("", body));
        let mut kept: Vec<String> = vec![];
        for sentence in split_sentences(body) {
            if UNCHANGED.is_match(&sentence) && SUBJECT.is_match(&sentence) {
                if !noted {
                    kept.push(TAKEN_OUT.to_string());
                    noted = true;
                }
            } else {
                kept.push(sentence);
            }
        }
        if !kept.is_empty() {
            out.push(format!("{lead}{mark}{}", kept.join(" ")));
        }
    }
    out.join("\n")
}

/// A line's sentences, each with its final mark.
fn split_sentences(line: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        cur.push(c);
        if matches!(c, '.' | '!' | '?') && chars.peek().is_none_or(|n| n.is_whitespace()) {
            out.push(cur.trim().to_string());
            cur.clear();
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade_notes::CallSite;

    const UPLOADS: &str = "import { Controller, Post, UseInterceptors } from '@lattice/common';\nimport { FileInterceptor } from '@lattice/platform-http';\n\n@Controller('files')\nexport class FilesController {\n  @Post('upload')\n  @UseInterceptors(FileInterceptor('document', { limits: { fileSize: 2 * 1024 * 1024 } }))\n  upload() {}\n\n  @Get(':id')\n  one() {}\n\n  @Delete(':id')\n  remove() {}\n}\n";

    #[test]
    fn routes_are_read_in_every_syntax_and_a_delete_is_never_probed() {
        let r = routes("src/files.controller.ts", UPLOADS);
        let got: Vec<(&str, &str)> = r
            .iter()
            .map(|x| (x.method.as_str(), x.path.as_str()))
            .collect();
        assert_eq!(got, vec![("POST", "/files/upload"), ("GET", "/files/1")]);
        assert!(r[0].upload);
        assert_eq!(r[0].field, "document");
        assert_eq!(r[0].limit, Some(2 * 1024 * 1024));
        // Call style, with a parameter in braces.
        let r = routes(
            "src/api.js",
            "router.post('/orders', validate(schema), create);\napp.get(`/orders/{id}`, one);\nrouter.delete('/orders/:id', gone);\n",
        );
        let got: Vec<(&str, &str)> = r
            .iter()
            .map(|x| (x.method.as_str(), x.path.as_str()))
            .collect();
        assert_eq!(got, vec![("POST", "/orders"), ("GET", "/orders/1")]);
        // Python decorators and a `route` with its methods; Go's upper case.
        let r = routes(
            "app/main.py",
            "@app.post(\"/items\")\ndef make(): ...\n@bp.route('/legacy', methods=['POST'])\ndef old(): ...\n",
        );
        assert_eq!(r.len(), 2);
        assert_eq!(
            (r[1].method.as_str(), r[1].path.as_str()),
            ("POST", "/legacy")
        );
        let r = routes("main.go", "r.POST(\"/v1/things\", h.Create)\n");
        assert_eq!(r[0].path, "/v1/things");
        // A client call to another host is no route.
        assert!(routes("src/client.ts", "http.get('https://example.test/x')").is_empty());
    }

    #[test]
    fn an_upload_is_probed_small_and_over_the_limit_and_a_write_on_its_error_paths() {
        let r = routes("src/files.controller.ts", UPLOADS);
        let q = requests_for(&r[0]);
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].what, "a small upload in the field `document`");
        assert!(q[0]
            .body
            .contains("name=\"document\"; filename=\"probe.txt\""));
        assert_eq!(
            q[1].what,
            "an upload over the size limit the file states (2048 KiB)"
        );
        assert_eq!(q[1].pad_kb, 2048 + 64);
        assert!(q[1]
            .content_type
            .starts_with("multipart/form-data; boundary="));
        let post = Route {
            method: "POST".into(),
            path: "/orders".into(),
            ..Default::default()
        };
        let whats: Vec<String> = requests_for(&post).into_iter().map(|q| q.what).collect();
        assert_eq!(
            whats,
            vec![
                "an empty JSON object (the validation error)",
                "a malformed JSON body (the parser error)"
            ]
        );
    }

    fn note(package: &str, api: &str, file: &str) -> PackageNotes {
        PackageNotes {
            package: package.into(),
            from: "3.2.0".into(),
            to: "4.0.1".into(),
            major: true,
            versions: vec!["4.0.0".into()],
            breaking: vec![
                format!("`{api}` reports issues in a new format"),
                "`otherThing` was removed".into(),
            ],
            apis: vec![api.into(), "otherThing".into()],
            call_sites: vec![CallSite {
                api: api.into(),
                file: file.into(),
                line: 7,
                text: format!("{api}(body)"),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn the_plan_goes_through_the_call_sites_and_names_what_it_cannot_probe() {
        let notes = vec![
            note("shape-check", "safeParse", "src/api.js"),
            note("queue-kit", "enqueue", "src/jobs/mailer.ts"),
            PackageNotes {
                call_sites: vec![],
                ..note("quiet-kit", "x", "")
            },
            PackageNotes {
                versions: vec![],
                call_sites: vec![],
                missing: "the registry answered 404".into(),
                ..note("unread-kit", "x", "")
            },
            // A bump inside its major is not probed here.
            PackageNotes {
                major: false,
                ..note("minor-kit", "y", "src/api.js")
            },
        ];
        let files = vec![
            (
                "src/api.js".to_string(),
                "router.post('/orders', (req, res) => safeParse(req.body));\n".to_string(),
            ),
            (
                "src/jobs/mailer.ts".to_string(),
                "export async function run() { await enqueue(job) }\n".to_string(),
            ),
        ];
        let p = plan(&notes, &files);
        assert_eq!(p.requests.len(), 2);
        assert_eq!(p.of, vec![("shape-check".to_string(), vec![0, 1])]);
        let why = |pkg: &str| {
            p.not_probed
                .iter()
                .find(|(n, _)| n == pkg)
                .map(|(_, w)| w.as_str())
                .unwrap()
        };
        assert!(why("queue-kit")
            .contains("no route is declared in the files that call it (`src/jobs/mailer.ts`)"));
        assert!(why("quiet-kit").contains("no call of an API its notes name was found"));
        assert!(why("unread-kit")
            .contains("its release notes were not read (the registry answered 404)"));
        assert_eq!(p.not_probed.len(), 3);
        // Only the breaking change whose API is called.
        assert_eq!(
            used_breaking(&notes[0]),
            vec!["`safeParse` reports issues in a new format"]
        );
    }

    #[test]
    fn many_routes_never_take_every_place() {
        let mut text = String::new();
        for i in 0..10 {
            text.push_str(&format!("router.post('/r{i}', h);\n"));
        }
        let notes = vec![
            note("a-kit", "one", "src/a.js"),
            note("b-kit", "two", "src/b.js"),
        ];
        let files = vec![
            ("src/a.js".to_string(), text),
            ("src/b.js".to_string(), "app.get('/b', h);\n".to_string()),
        ];
        let p = plan(&notes, &files);
        assert_eq!(p.requests.len(), MAX_PROBES);
        assert_eq!(p.of[1], ("b-kit".to_string(), vec![1]));
    }

    fn answer(status: u16, sha: &str, excerpt: &str) -> Option<ProbeAnswer> {
        Some(ProbeAnswer {
            status,
            content_type: "application/json".into(),
            sha: sha.into(),
            excerpt: excerpt.into(),
            stable: true,
        })
    }

    fn two_requests() -> Vec<ProbeRequest> {
        requests_for(&Route {
            method: "POST".into(),
            path: "/orders".into(),
            ..Default::default()
        })
    }

    #[test]
    fn a_changed_error_body_is_a_known_behaviour_change() {
        // The review's case: the status is the same, the 400 body is not.
        let reqs = two_requests();
        let base = vec![
            answer(
                400,
                "aaaa",
                r#"{"message":["name must be a string"],"error":"Bad Request"}"#,
            ),
            answer(400, "cccc", r#"{"message":"Unexpected end of JSON input"}"#),
        ];
        let head = vec![
            answer(
                400,
                "bbbb",
                r#"{"issues":[{"path":["name"],"code":"invalid_type"}]}"#,
            ),
            answer(400, "cccc", r#"{"message":"Unexpected end of JSON input"}"#),
        ];
        let c = compare(&reqs, &base, &head);
        assert_eq!(c[0].outcome, Outcome::Changed);
        assert!(c[0]
            .detail
            .starts_with("body before `{\"message\":[\"name must be a string\"]"));
        assert_eq!(c[1].outcome, Outcome::Same);
        assert!(c[0].reached && c[1].reached);
        let b = Behaviour {
            package: "shape-check".into(),
            from: "3.2.0".into(),
            to: "4.0.1".into(),
            compared: c,
            consumers: vec!["acme/mobile".into()],
            consumers_searched: "2 other repositories".into(),
            ..Default::default()
        };
        assert!(!b.unchanged_shown());
        assert_eq!(b.changed_paths(), vec!["/orders"]);
        let text = b.section();
        assert!(text.contains(
            "Known behaviour change: `POST /orders` (an empty JSON object (the validation error)) answered 400 application/json on the base branch's build and 400 application/json on this head's: body before"
        ));
        assert!(text.contains("Who could care: `acme/mobile` names these endpoints."));
        assert!(!text.contains("Unchanged on these"));
    }

    #[test]
    fn unchanged_is_shown_only_by_answers_that_came_from_the_route() {
        let reqs = two_requests();
        let same = vec![answer(400, "aaaa", "x"), answer(400, "bbbb", "y")];
        let b = Behaviour {
            compared: compare(&reqs, &same, &same),
            ..Default::default()
        };
        assert!(b.unchanged_shown());
        assert!(b
            .section()
            .contains("Unchanged on these 2 requests; nothing is claimed beyond them."));
        // The same answers from a sign-in wall show nothing.
        let wall = vec![answer(401, "aaaa", "x"), answer(401, "aaaa", "x")];
        let b = Behaviour {
            compared: compare(&reqs, &wall, &wall),
            ..Default::default()
        };
        assert!(!b.unchanged_shown());
        assert!(b
            .section()
            .contains("None of these answers came from the route itself"));
        // A build that did not answer shows nothing either.
        let b = Behaviour {
            compared: compare(&reqs, &same, &[None, None]),
            ..Default::default()
        };
        assert!(!b.unchanged_shown());
        assert!(b
            .section()
            .contains("the head's build did not answer, so it shows nothing"));
        // No request at all: said as not probed.
        let b = Behaviour {
            package: "queue-kit".into(),
            not_probed: "no route is declared in the files that call it".into(),
            ..Default::default()
        };
        assert!(!b.unchanged_shown());
        assert!(b
            .section()
            .contains("Nothing here says its behaviour is the same as before."));
    }

    #[test]
    fn an_answer_that_varies_by_itself_is_compared_by_status_only() {
        let reqs = two_requests();
        let vary = |sha: &str| {
            Some(ProbeAnswer {
                stable: false,
                ..answer(200, sha, "{\"at\":1}").unwrap()
            })
        };
        let c = compare(
            &reqs,
            &[vary("aaaa"), answer(400, "x", "")],
            &[vary("bbbb"), answer(500, "x", "")],
        );
        assert_eq!(c[0].outcome, Outcome::Same);
        assert!(c[0].varies);
        assert_eq!(c[1].outcome, Outcome::Changed);
        assert_eq!(c[1].detail, "status 400 to 500");
    }

    #[test]
    fn the_word_unchanged_stands_only_on_a_probe() {
        let text = "Bumped 50 packages. Contract unchanged. The build passes.\n- No behaviour changes are expected for the API.\n- `left-trim` is unchanged in the lockfile.";
        let guarded = guard(text, false);
        assert_eq!(
            guarded,
            format!("Bumped 50 packages. {TAKEN_OUT} The build passes.\n- `left-trim` is unchanged in the lockfile.")
        );
        // With a probe that shows it, the text stands.
        assert_eq!(guard(text, true), text);
        // Other ways of saying it.
        for claim in [
            "The API is backward compatible.",
            "This is a drop-in upgrade.",
            "Responses remain the same.",
            "The upgrade does not change the error bodies.",
            "There are no breaking changes to the public interface.",
        ] {
            assert_eq!(guard(claim, false), TAKEN_OUT, "{claim}");
        }
        // A sentence about something else is left alone.
        let other = "The lockfile's format is unchanged. Two lines of the table are identical.";
        assert_eq!(guard(other, false), other);
    }
}
