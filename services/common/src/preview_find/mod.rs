//! Where a pull request's preview lives, read from every place teams
//! publish it, tied to the head commit.
//!
//! A preview's address reaches a pull request in one of a handful of ways,
//! and a team uses whichever its host or its CI gives it:
//!
//! - a deployment on the head commit, its newest status carrying the
//!   address (`environment_url`, else a `target_url` that isn't a log page);
//!   hosting integrations and a workflow job's `environment: {name, url}`
//!   both write these ([`deployments`]);
//! - a commit status whose context or description reads as a deploy, its
//!   `target_url` the preview ([`statuses`]);
//! - a check run whose name or output reads as a preview deploy, the address
//!   in its output or its details link ([`check_runs`]);
//! - a bot's comment on the pull request, usually edited in place on every
//!   push, often a table with one row per app ([`comments`]);
//! - a link in the pull request's description ([`body`]);
//! - the team's own address template, filled in from the pull request's
//!   number, branch and commit ([`expand`]).
//!
//! Each is read into [`Sighting`]s, and [`resolve`] gives the one answer:
//!
//! 1. Only what is tied to the head commit counts first: a deployment,
//!    status or check run on that sha, a comment that names that sha (or,
//!    naming none, was written after the commit). A deploy of an older
//!    commit is never the preview of this one.
//! 2. When several apps publish previews (a monorepo), the one named after
//!    the changed app is kept ([`app_score`]).
//! 3. Of what is up with an address, the most direct source wins
//!    ([`Source`] order). A head deploy still building means wait; one that
//!    failed or was taken down is said as such.
//! 4. Only when nothing on the head carries an address does an untied one
//!    count: the description's link or the team's template.
//!
//! How long to wait is bounded ([`WAIT_BUDGET_MINS`]); nothing here reads
//! the clock, the caller passes the times.

use serde::{Deserialize, Serialize};

/// How long a preview of one head is waited on before the wait is said to
/// have run out.
pub const WAIT_BUDGET_MINS: i64 = 60;
/// How long nothing at all on the head (no deployment, status, check,
/// comment or template) is waited on before there is said to be no preview:
/// hosting integrations start within a minute or two of a push.
pub const GRACE_MINS: i64 = 6;
/// How often a pending preview is looked at again when nothing else on the
/// pull request changed.
pub const POLL_SECS: i64 = 120;

/// Where an address came from, most direct first: the order breaks ties
/// between two sightings that both say the head's preview is up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    #[default]
    Deployment,
    CheckRun,
    CommitStatus,
    Comment,
    Body,
    Template,
}

impl Source {
    /// How the source is named in a sentence.
    pub fn phrase(self) -> &'static str {
        match self {
            Source::Deployment => "the head commit's deployment",
            Source::CheckRun => "the preview check on the head commit",
            Source::CommitStatus => "the deploy status on the head commit",
            Source::Comment => "the preview comment on the pull request",
            Source::Body => "the link in the pull request's description",
            Source::Template => "the team's preview address template",
        }
    }
}

/// What a sighting says of the preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ready {
    Up,
    Building,
    Failed,
    /// Taken down, expired or replaced.
    Gone,
}

/// One place that says something about the head's preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sighting {
    pub source: Source,
    /// The environment, context, check or table row it names (an app in a
    /// monorepo), "" when none.
    pub name: String,
    /// The address, "" when it says only how the deploy stands.
    pub url: String,
    pub ready: Ready,
    /// Tied to the head commit.
    pub tied: bool,
}

/// The preview the change is checked on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Found {
    pub url: String,
    pub source: Source,
    /// The environment, check or app it was named under.
    pub name: String,
    /// Tied to the head commit (false: a description link or the template).
    pub tied: bool,
}

/// The answer [`resolve`] gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Up(Found),
    /// The head's preview is still on its way: what says so.
    Building(String),
    /// The head's preview failed or was taken down: what says so.
    Failed(String),
    Nothing,
}

// ── words ─────────────────────────────────────────────────────────────────

fn words(s: &str) -> Vec<String> {
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect()
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Whether a check, status or environment name (with its description)
/// reads as a deploy of the pull request's own preview: it says preview,
/// deploy or review app, and never a shared environment or a cleanup.
pub fn reads_as_preview(name: &str, text: &str) -> bool {
    use crate::preview::{role_of, Role};
    if matches!(
        role_of(name),
        Role::Production | Role::Staging | Role::Cleanup | Role::E2e
    ) {
        return false;
    }
    let both = format!("{name} {text}");
    let w = words(&both);
    let low = both.to_ascii_lowercase();
    w.iter().any(|x| {
        matches!(
            x.as_str(),
            "preview" | "previews" | "deploy" | "deployment" | "deployed" | "deploying"
        )
    }) || low.contains("review app")
        || low.contains("review-app")
}

fn host_of(url: &str) -> &str {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or("");
    rest.split(['/', '?', '#']).next().unwrap_or("")
}

fn path_of(url: &str) -> &str {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or("");
    match rest.find('/') {
        Some(i) => rest[i..].split(['?', '#']).next().unwrap_or(""),
        None => "",
    }
}

/// An address on the code host itself (a pull request, a run, a file), never
/// a preview.
pub fn is_code_host(url: &str) -> bool {
    let h = host_of(url).to_ascii_lowercase();
    h == "github.com" || h.ends_with(".github.com") || h.ends_with(".githubusercontent.com")
}

/// An address that is a log, dashboard or build page rather than the site:
/// its host starts with a console's name, or its path is a deploy, build or
/// run record.
pub fn is_console(url: &str) -> bool {
    let h = host_of(url).to_ascii_lowercase();
    let first = h.split('.').next().unwrap_or("");
    if matches!(
        first,
        "app" | "dashboard" | "console" | "admin" | "ci" | "build" | "builds" | "logs" | "status"
    ) && h.matches('.').count() >= 2
    {
        return true;
    }
    path_of(url).to_ascii_lowercase().split('/').any(|seg| {
        matches!(
            seg,
            "deploys"
                | "deployments"
                | "builds"
                | "runs"
                | "jobs"
                | "pipelines"
                | "logs"
                | "inspect"
                | "actions"
        )
    })
}

fn is_image(url: &str) -> bool {
    let p = path_of(url).to_ascii_lowercase();
    let h = host_of(url).to_ascii_lowercase();
    [".png", ".svg", ".gif", ".jpg", ".jpeg", ".webp", ".ico"]
        .iter()
        .any(|e| p.ends_with(e))
        || h.contains("badge")
        || p.contains("/badge")
}

/// An address that can be a site: https, a host with a dot, never the code
/// host, a console page or an image.
pub fn site_like(url: &str) -> bool {
    let h = host_of(url);
    url.starts_with("https://")
        && h.contains('.')
        && !h.contains('@')
        && !is_code_host(url)
        && !is_console(url)
        && !is_image(url)
}

/// Every https address in a text with the words around it: a markdown link's
/// text, else the line it sits on.
pub fn urls_in(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in text.lines() {
        for (i, _) in line.match_indices("https://") {
            let rest = &line[i..];
            let end = rest
                .find(|c: char| {
                    c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>' | '<' | ']' | '`' | '|')
                })
                .unwrap_or(rest.len());
            let url = rest[..end].trim_end_matches(['.', ',', ';', ':']);
            // `[text](url)`: the text says what it is.
            let label = line[..i]
                .strip_suffix("](")
                .and_then(|before| before.rfind('[').map(|j| before[j + 1..].to_string()))
                .unwrap_or_else(|| line.to_string());
            if !url.is_empty() && !out.iter().any(|(u, _): &(String, String)| u == url) {
                out.push((url.to_string(), label));
            }
        }
    }
    out
}

/// Words that say an address is the preview.
fn labelled_preview(label: &str, url: &str) -> bool {
    let w = words(label);
    let says = w.iter().any(|x| {
        matches!(
            x.as_str(),
            "preview" | "previews" | "visit" | "open" | "deployment" | "deployed" | "live" | "url"
        )
    });
    let h = host_of(url).to_ascii_lowercase();
    says || h.contains("preview")
        || h.split(['.', '-']).any(|p| {
            p.strip_prefix("pr")
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
        })
        || h.split('.').next().is_some_and(|l| l.contains("-pr-"))
}

/// The preview address in a text. `named`: the text is a preview check's or
/// deploy status's own, so a site address counts even unlabelled; else it
/// must be labelled or shaped as a preview.
pub fn pick_url(text: &str, named: bool) -> Option<String> {
    let all: Vec<(String, String)> = urls_in(text)
        .into_iter()
        .filter(|(u, _)| site_like(u))
        .collect();
    all.iter()
        .find(|(u, l)| labelled_preview(l, u))
        .or_else(|| if named { all.first() } else { None })
        .map(|(u, _)| u.clone())
}

// ── readers: each place a preview is published ─────────────────────────────

fn s<'a>(v: &'a serde_json::Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}

fn same_sha(a: &str, head: &str) -> bool {
    !a.is_empty() && !head.is_empty() && a.eq_ignore_ascii_case(head)
}

/// The head's deployments, each with its statuses (as the deployments and
/// deployment statuses listings return them). A deployment to production
/// or to a shared environment is never a pull request's preview, unless it
/// says it is transient.
pub fn deployments(
    list: &[(serde_json::Value, Vec<serde_json::Value>)],
    head: &str,
) -> Vec<Sighting> {
    use crate::preview::{is_shared_env_label, role_of, Role};
    let mut out = Vec::new();
    for (d, statuses) in list {
        if !same_sha(s(d, "sha"), head) {
            continue;
        }
        let env = match s(d, "environment") {
            "" => s(d, "original_environment"),
            e => e,
        };
        let transient = d["transient_environment"].as_bool().unwrap_or(false);
        let shared = d["production_environment"].as_bool().unwrap_or(false)
            || ((is_shared_env_label(env)
                || matches!(role_of(env), Role::Production | Role::Staging))
                && !env.to_ascii_lowercase().contains("preview"));
        if shared && !transient {
            continue;
        }
        // The newest status says where it stands.
        let mut sts: Vec<&serde_json::Value> = statuses.iter().collect();
        sts.sort_by(|a, b| {
            s(b, "created_at")
                .cmp(s(a, "created_at"))
                .then(b["id"].as_u64().cmp(&a["id"].as_u64()))
        });
        let newest = sts.first();
        let ready = match newest.map(|st| s(st, "state")) {
            Some("success") => Ready::Up,
            Some("failure" | "error") => Ready::Failed,
            Some("inactive") => Ready::Gone,
            // Created, queued, pending or in progress.
            _ => Ready::Building,
        };
        let url = newest
            .map(|st| {
                let env_url = s(st, "environment_url");
                let target = s(st, "target_url");
                let log = s(st, "log_url");
                if site_like(env_url) {
                    env_url.to_string()
                } else if site_like(target) && target != log {
                    target.to_string()
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();
        out.push(Sighting {
            source: Source::Deployment,
            name: env.to_string(),
            url,
            ready,
            tied: true,
        });
    }
    out
}

/// The head's combined commit status: each context that reads as a deploy.
pub fn statuses(combined: &serde_json::Value, head: &str) -> Vec<Sighting> {
    let sha = s(combined, "sha");
    if !sha.is_empty() && !same_sha(sha, head) {
        return Vec::new();
    }
    let mut out = Vec::new();
    for st in combined["statuses"].as_array().into_iter().flatten() {
        let (ctx, desc) = (s(st, "context"), s(st, "description"));
        if !reads_as_preview(ctx, desc) {
            continue;
        }
        let ready = match s(st, "state") {
            "success" => Ready::Up,
            "failure" | "error" => Ready::Failed,
            _ => Ready::Building,
        };
        let target = s(st, "target_url");
        out.push(Sighting {
            source: Source::CommitStatus,
            name: ctx.to_string(),
            url: if site_like(target) {
                target.to_string()
            } else {
                String::new()
            },
            ready,
            tied: true,
        });
    }
    out
}

/// The head's check runs (the check-runs listing): each that reads as a
/// preview deploy, its newest run per name. `covers` are the preview jobs
/// the repository's workflows run for the change (`preview::plan`).
pub fn check_runs(
    runs: &serde_json::Value,
    head: &str,
    covers: &[crate::preview::Covering],
) -> Vec<Sighting> {
    use crate::preview::{is_covering, role_of, Role};
    let mut newest: Vec<(&str, &serde_json::Value)> = Vec::new();
    for r in runs["check_runs"].as_array().into_iter().flatten() {
        let sha = s(r, "head_sha");
        if !sha.is_empty() && !same_sha(sha, head) {
            continue;
        }
        let name = s(r, "name");
        let out_text = format!(
            "{} {}",
            s(&r["output"], "title"),
            s(&r["output"], "summary")
        );
        let preview_job = covers
            .iter()
            .filter(|c| c.role == Some(Role::Preview))
            .any(|c| is_covering(std::slice::from_ref(c), name));
        if role_of(name) == Role::E2e || !(preview_job || reads_as_preview(name, &out_text)) {
            continue;
        }
        if s(r, "conclusion") == "skipped" || s(r, "conclusion") == "neutral" {
            continue;
        }
        match newest.iter_mut().find(|(n, _)| *n == name) {
            Some(slot) if s(r, "started_at") > s(slot.1, "started_at") => slot.1 = r,
            Some(_) => {}
            None => newest.push((name, r)),
        }
    }
    newest
        .into_iter()
        .map(|(name, r)| {
            let ready = match (s(r, "status"), s(r, "conclusion")) {
                ("completed", "success") => Ready::Up,
                ("completed", _) => Ready::Failed,
                _ => Ready::Building,
            };
            let text = format!(
                "{}\n{}\n{}",
                s(&r["output"], "summary"),
                s(&r["output"], "text"),
                s(&r["output"], "title")
            );
            let details = s(r, "details_url");
            let url = pick_url(&text, true)
                .or_else(|| site_like(details).then(|| details.to_string()))
                .unwrap_or_default();
            Sighting {
                source: Source::CheckRun,
                name: name.to_string(),
                url,
                ready,
                tied: true,
            }
        })
        .collect()
}

fn hexish(w: &str) -> bool {
    // A commit's sha mixes digits and letters; a build number or a date
    // is digits only.
    (7..=40).contains(&w.len())
        && w.chars().all(|c| c.is_ascii_hexdigit())
        && w.chars().any(|c| c.is_ascii_digit())
        && w.chars().any(|c| c.is_ascii_alphabetic())
}

/// Whether a text names the head commit (its full sha or a prefix of seven
/// or more), names another commit, or names none.
fn names_commit(text: &str, head: &str) -> Option<bool> {
    let head = head.to_ascii_lowercase();
    let mut other = false;
    for w in text
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| hexish(w))
    {
        if head.starts_with(w) {
            return Some(true);
        }
        other = true;
    }
    other.then_some(false)
}

/// The state a line or a comment says: failed, building, gone or up.
fn says(text: &str) -> Option<Ready> {
    let low = text.to_ascii_lowercase();
    let w = words(text);
    let has = |xs: &[&str]| w.iter().any(|x| xs.contains(&x.as_str()));
    if has(&[
        "failed",
        "failure",
        "error",
        "errored",
        "canceled",
        "cancelled",
    ]) || low.contains('❌')
    {
        return Some(Ready::Failed);
    }
    if has(&[
        "building",
        "queued",
        "pending",
        "deploying",
        "initializing",
        "waiting",
    ]) || low.contains("in progress")
        || low.contains('⏳')
        || low.contains('🔄')
    {
        return Some(Ready::Building);
    }
    if has(&["deleted", "removed", "expired", "inactive", "destroyed"]) || low.contains("torn down")
    {
        return Some(Ready::Gone);
    }
    if has(&[
        "ready",
        "deployed",
        "success",
        "succeeded",
        "live",
        "available",
    ]) || low.contains('✅')
    {
        return Some(Ready::Up);
    }
    None
}

/// A table row's first cell, or "".
fn row_name(line: &str) -> String {
    let t = line.trim();
    if !t.starts_with('|') {
        return String::new();
    }
    let cell = t.trim_matches('|').split('|').next().unwrap_or("");
    let clean: String = cell
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '-' | '_' | '/' | '.'))
        .collect();
    let clean = clean.trim();
    // A header row's or a label cell's words aren't an app.
    if words(clean).iter().all(|w| {
        matches!(
            w.as_str(),
            "name"
                | "project"
                | "app"
                | "status"
                | "preview"
                | "deploy"
                | "latest"
                | "commit"
                | "log"
                | "updated"
                | "url"
                | "inspect"
                | "visit"
        )
    }) {
        return String::new();
    }
    clean.to_string()
}

/// The bot comments on the pull request (the issue comments listing, oldest
/// first). A comment is tied to the head when it names the head commit, or
/// names none and was written or edited at or after `head_at` (the head
/// commit's time); one that names only another commit is an older deploy's.
/// Each line with a preview address is one sighting: a monorepo's table
/// gives one per app.
pub fn comments(list: &[serde_json::Value], head: &str, head_at: &str) -> Vec<Sighting> {
    let mut bots: Vec<&serde_json::Value> = list
        .iter()
        .filter(|c| {
            c["user"]["type"].as_str() == Some("Bot")
                || c["user"]["login"]
                    .as_str()
                    .is_some_and(|l| l.ends_with("[bot]"))
        })
        .collect();
    // The newest word first.
    bots.sort_by(|a, b| {
        let at = |c: &serde_json::Value| match s(c, "updated_at") {
            "" => s(c, "created_at").to_string(),
            u => u.to_string(),
        };
        at(b).cmp(&at(a))
    });
    let mut out = Vec::new();
    for c in bots {
        let body = s(c, "body");
        if !body.contains("https://") {
            continue;
        }
        let updated = match s(c, "updated_at") {
            "" => s(c, "created_at"),
            u => u,
        };
        let tied = match names_commit(body, head) {
            Some(t) => t,
            None => !head_at.is_empty() && updated >= head_at,
        };
        let names_other = names_commit(body, head) == Some(false);
        if names_other {
            // An older commit's deploy: never this head's.
            continue;
        }
        let whole = says(body);
        for line in body.lines() {
            let Some(url) = pick_url(line, false) else {
                continue;
            };
            out.push(Sighting {
                source: Source::Comment,
                name: row_name(line),
                url,
                ready: says(line).or(whole).unwrap_or(Ready::Up),
                tied,
            });
        }
    }
    out
}

/// A preview link in the pull request's description: tied only when the
/// description names the head commit.
pub fn body(text: &str, head: &str) -> Vec<Sighting> {
    let tied = names_commit(text, head) == Some(true);
    text.lines()
        .filter_map(|line| {
            let url = pick_url(line, false)?;
            Some(Sighting {
                source: Source::Body,
                name: row_name(line),
                url,
                ready: Ready::Up,
                tied,
            })
        })
        .collect()
}

// ── the team's template ───────────────────────────────────────────────────

/// What an address template may name.
pub const PLACEHOLDERS: &[&str] = &[
    "{pr}",
    "{branch}",
    "{branch_slug}",
    "{sha}",
    "{short_sha}",
    "{owner}",
    "{repo}",
];

/// The pull request a template is filled in for.
#[derive(Debug, Clone, Copy, Default)]
pub struct Vars<'a> {
    pub pr: u64,
    pub branch: &'a str,
    pub sha: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
}

/// A branch name as hosts put it in an address: lowercase, every run of
/// other characters one `-`, no `-` at either end.
pub fn branch_slug(branch: &str) -> String {
    let mut out = String::new();
    for c in branch.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}

fn braces(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(i) = rest.find('{') {
        match rest[i..].find('}') {
            Some(j) => {
                out.push(&rest[i..i + j + 1]);
                rest = &rest[i + j + 1..];
            }
            None => {
                out.push(&rest[i..]);
                break;
            }
        }
    }
    out
}

/// Checks an address template as a team typed it; the error is one sentence
/// a person can act on. Empty is no template.
pub fn check_template(raw: &str) -> Result<(), String> {
    let t = raw.trim();
    if t.is_empty() {
        return Ok(());
    }
    let rest = t
        .strip_prefix("https://")
        .ok_or("The preview address starts with https://.")?;
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty()
        || t.len() > 300
        || t.chars().any(|c| {
            c.is_whitespace() || c.is_control() || matches!(c, '\'' | '"' | '`' | '\\' | '$' | ';')
        })
    {
        return Err(
            "Enter the preview's address, like https://pr-{pr}.preview.your-domain.example.".into(),
        );
    }
    for b in braces(t) {
        if !PLACEHOLDERS.contains(&b) {
            return Err(format!(
                "`{b}` isn't filled in; use {}.",
                PLACEHOLDERS.join(", ")
            ));
        }
    }
    if !is_per_change(t) {
        return Err(
            "The address needs {pr}, {branch}, {branch_slug}, {sha} or {short_sha}, so each pull request gets its own preview."
                .into(),
        );
    }
    Ok(())
}

/// Whether a template names something of the pull request, so it is that
/// pull request's own address (a fixed address is a shared environment).
pub fn is_per_change(t: &str) -> bool {
    ["{pr}", "{branch}", "{branch_slug}", "{sha}", "{short_sha}"]
        .iter()
        .any(|p| t.contains(p))
}

/// A template filled in for a pull request. A host label longer than DNS
/// allows (63 characters) is cut to fit, as hosts cut a long branch's slug.
/// None when the template isn't valid or names a value the pull request
/// doesn't have.
pub fn expand(template: &str, v: Vars) -> Option<String> {
    let t = template.trim();
    check_template(t).ok()?;
    if t.is_empty() {
        return None;
    }
    let need = |p: &str, have: bool| !t.contains(p) || have;
    if !(need("{pr}", v.pr > 0)
        && need("{branch}", !v.branch.is_empty())
        && need("{branch_slug}", !v.branch.is_empty())
        && need("{sha}", !v.sha.is_empty())
        && need("{short_sha}", !v.sha.is_empty()))
    {
        return None;
    }
    let rest = &t["https://".len()..];
    let cut = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (host_t, tail_t) = rest.split_at(cut);
    let fill = |s: &str, host: bool| {
        let branch = if host {
            branch_slug(v.branch)
        } else {
            v.branch.to_string()
        };
        s.replace("{pr}", &v.pr.to_string())
            .replace("{branch_slug}", &branch_slug(v.branch))
            .replace("{branch}", &branch)
            .replace("{short_sha}", v.sha.get(..7).unwrap_or(v.sha))
            .replace("{sha}", v.sha)
            .replace("{owner}", &branch_slug(v.owner))
            .replace("{repo}", &branch_slug(v.repo))
    };
    let host: Vec<String> = fill(host_t, true)
        .split('.')
        .map(|l| {
            if l.len() > 63 {
                l[..63].trim_end_matches('-').to_string()
            } else {
                l.to_string()
            }
        })
        .collect();
    Some(format!("https://{}{}", host.join("."), fill(tail_t, false)))
}

// ── which app, and the answer ─────────────────────────────────────────────

/// Folders a monorepo keeps its apps under.
const APP_HOMES: &[&str] = &[
    "apps",
    "packages",
    "services",
    "sites",
    "projects",
    "frontends",
    "web",
    "clients",
];

/// The app a changed path belongs to: the folder under a monorepo's apps
/// folder, else the path's first folder.
pub fn app_of(path: &str) -> Option<String> {
    let segs: Vec<&str> = path.split('/').collect();
    if segs.len() < 2 {
        return None;
    }
    if APP_HOMES.contains(&segs[0]) && segs.len() >= 3 {
        return Some(segs[1].to_string());
    }
    Some(segs[0].to_string())
}

/// The apps a repository's tree has: each folder under a monorepo's apps
/// folder.
pub fn apps_of(tree: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in tree {
        let segs: Vec<&str> = p.split('/').collect();
        if segs.len() >= 3 && APP_HOMES.contains(&segs[0]) {
            let a = segs[1].to_string();
            if !out.contains(&a) {
                out.push(a);
            }
        }
    }
    out
}

fn names(name: &str, app: &str) -> bool {
    let (n, a) = (norm(name), norm(app));
    a.len() >= 3 && n.contains(&a)
}

/// How well a preview's name fits the change: it names a changed app (2),
/// names nothing known (0), names an app the change doesn't touch or reads
/// as a backend with no pages (-1).
pub fn app_score(name: &str, changed: &[String], apps: &[String]) -> i32 {
    let changed_apps: Vec<String> = changed.iter().filter_map(|p| app_of(p)).collect();
    if changed_apps.iter().any(|a| {
        names(name, a) && (apps.is_empty() || apps.contains(a) || !APP_HOMES.contains(&a.as_str()))
    }) {
        return 2;
    }
    if apps
        .iter()
        .any(|a| names(name, a) && !changed_apps.contains(a))
    {
        return -1;
    }
    let w = words(name);
    if w.iter().any(|x| {
        matches!(
            x.as_str(),
            "api" | "backend" | "server" | "graphql" | "worker" | "storybook"
        )
    }) {
        return -1;
    }
    0
}

fn describe(s: &Sighting) -> String {
    if s.name.is_empty() {
        s.source.phrase().to_string()
    } else {
        format!("{} `{}`", s.source.phrase(), s.name)
    }
}

/// The one answer from every sighting: see the module's rules.
pub fn resolve(all: &[Sighting], changed: &[String], apps: &[String]) -> Answer {
    fn pick<'a>(
        group: Vec<&'a Sighting>,
        changed: &[String],
        apps: &[String],
    ) -> Vec<&'a Sighting> {
        let named: Vec<i32> = group
            .iter()
            .map(|s| {
                if s.name.is_empty() {
                    0
                } else {
                    app_score(&s.name, changed, apps)
                }
            })
            .collect();
        let best = named.iter().copied().max().unwrap_or(0);
        group
            .into_iter()
            .zip(named)
            .filter(|(_, sc)| *sc == best)
            .map(|(s, _)| s)
            .collect()
    }
    let up_with_url = |g: &[&Sighting]| -> Option<Found> {
        g.iter()
            .filter(|s| s.ready == Ready::Up && !s.url.is_empty())
            .min_by_key(|s| s.source)
            .map(|s| Found {
                url: s.url.clone(),
                source: s.source,
                name: s.name.clone(),
                tied: s.tied,
            })
    };
    let tied = pick(all.iter().filter(|s| s.tied).collect(), changed, apps);
    if let Some(f) = up_with_url(&tied) {
        return Answer::Up(f);
    }
    if let Some(b) = tied
        .iter()
        .filter(|s| s.ready == Ready::Building)
        .min_by_key(|s| s.source)
    {
        return Answer::Building(describe(b));
    }
    let up_without_url = tied.iter().any(|s| s.ready == Ready::Up);
    if !up_without_url {
        if let Some(f) = tied
            .iter()
            .filter(|s| matches!(s.ready, Ready::Failed | Ready::Gone))
            .min_by_key(|s| s.source)
        {
            let how = if f.ready == Ready::Failed {
                "failed"
            } else {
                "was taken down"
            };
            return Answer::Failed(format!("{} {how}", describe(f)));
        }
    }
    let untied = pick(all.iter().filter(|s| !s.tied).collect(), changed, apps);
    match up_with_url(&untied) {
        Some(f) => Answer::Up(f),
        None => Answer::Nothing,
    }
}

/// Minutes from `since` to `now` (RFC 3339), 0 when either can't be read.
pub fn minutes_between(since: &str, now: &str) -> i64 {
    match (
        chrono::DateTime::parse_from_rfc3339(since),
        chrono::DateTime::parse_from_rfc3339(now),
    ) {
        (Ok(a), Ok(b)) => (b - a).num_minutes().max(0),
        _ => 0,
    }
}

/// Seconds from `since` to `now` (RFC 3339); a large number when `since`
/// can't be read, so a look is due.
pub fn seconds_between(since: &str, now: &str) -> i64 {
    match (
        chrono::DateTime::parse_from_rfc3339(since),
        chrono::DateTime::parse_from_rfc3339(now),
    ) {
        (Ok(a), Ok(b)) => (b - a).num_seconds(),
        _ => i64::MAX,
    }
}

#[cfg(test)]
mod tests;
