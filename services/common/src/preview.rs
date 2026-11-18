//! A web change is checked on its preview before it is called ready: the
//! repository's own preview deploy and end-to-end checks run on it, and the
//! changed pages are captured at desktop and phone width.
//!
//! A run once opened a pull request on a web app whose preview deploy and
//! preview end-to-end checks run only with a label. CoderHelm added no
//! label, so every deploy and end-to-end check on the head was skipped, and
//! the description listed them as "skipped (its conditions left this PR
//! out)" next to passing unit tests, said "No screenshot: CoderHelm had no
//! runnable preview" and gave `npm test` as the way to check it. Nothing had
//! been looked at in a browser. A skipped preview check is not evidence: it
//! is a check CoderHelm didn't start.
//!
//! The rules, all read from the repository itself (its workflow files and
//! its tree), nothing about any one repository written here:
//!
//! - [`role_of`] reads what a check is for from its name: a preview deploy,
//!   end-to-end tests, a staging or production deploy, a cleanup.
//! - [`plan`] reads each workflow's `on:` (pull request events, `paths`,
//!   `paths-ignore`, `branches`, the event `types`) and each job's `if:`
//!   (through `workflow_gate`) and returns the preview and end-to-end jobs
//!   that cover the change, the labels that start them (the end-to-end area
//!   labels that match the changed paths among them), whether a push is
//!   needed for a label to count, and what keeps a job from running that
//!   CoderHelm can't change. A staging, production or other shared
//!   environment label is never chosen.
//! - [`gaps`] are the checks that cover the change and didn't run on the
//!   head: an open gap CoderHelm closes, never evidence.
//! - [`e2e_need`] says, from the changed paths and the repository's e2e
//!   suite, whether the change needs a new or updated end-to-end spec and
//!   whether it has one.
//! - [`routes`] picks the pages to capture from the repository's page files
//!   and the changed paths; [`capture_script`] is the sandbox script that
//!   captures them.
//! - Where the preview is and whether it is up for the head commit is
//!   `crate::preview_find`'s: deployments, statuses, checks, bot comments,
//!   the description and the team's address template, tied to the head.
//! - [`State`] is what the run records ([`FIELD`]) and the description and
//!   the run page read.

use serde::{Deserialize, Serialize};

use crate::e2e::workflow_gate::{self, LabelWay};

/// The run attribute holding the [`State`] JSON.
pub const FIELD: &str = "preview_state";

/// Viewport widths a page is captured at: desktop, then phone.
pub const DESKTOP: (u32, u32) = (1280, 800);
pub const PHONE: (u32, u32) = (390, 844);
/// Pages captured per change at most.
pub const MAX_PAGES: usize = 3;
/// The Playwright release the capture script installs when the repository
/// has none of its own in the sandbox.
pub const PLAYWRIGHT: &str = "1.49.1";

// ── what a check is for ────────────────────────────────────────────────────

/// What a check or job is for, read from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Deploys the pull request's own preview.
    Preview,
    /// End-to-end tests (on the preview, or anywhere else a pull request
    /// runs them).
    E2e,
    /// A shared staging environment.
    Staging,
    /// Production or a release.
    Production,
    /// Tears a preview down.
    Cleanup,
    Other,
}

fn words(s: &str) -> Vec<String> {
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect()
}

/// The role a check's name shows. Cleanup first (a preview's cleanup is not
/// its deploy), then the shared environments (an end-to-end run after the
/// staging deploy is staging's), then end-to-end, then the preview.
pub fn role_of(name: &str) -> Role {
    let w = words(name);
    let has = |xs: &[&str]| w.iter().any(|x| xs.contains(&x.as_str()));
    let low = name.to_ascii_lowercase();
    if has(&[
        "cleanup", "clean", "teardown", "destroy", "delete", "remove", "close", "closed",
    ]) {
        return Role::Cleanup;
    }
    if has(&["prod", "production", "release", "publish"]) {
        return Role::Production;
    }
    if has(&["staging", "stage", "stg", "uat"]) {
        return Role::Staging;
    }
    if has(&["e2e", "playwright", "cypress", "acceptance"])
        || low.contains("end-to-end")
        || low.contains("end to end")
    {
        return Role::E2e;
    }
    if has(&["preview", "previews"]) || low.contains("review app") || low.contains("review-app") {
        return Role::Preview;
    }
    Role::Other
}

/// A label CoderHelm never adds: one that deploys to, or runs against, an
/// environment the team shares (staging, production, a release). A preview
/// deploy label is the pull request's own and may be added.
pub fn is_shared_env_label(name: &str) -> bool {
    let w = words(name);
    let has = |xs: &[&str]| w.iter().any(|x| xs.contains(&x.as_str()));
    let low = name.to_ascii_lowercase();
    if has(&[
        "prod",
        "production",
        "staging",
        "stage",
        "stg",
        "uat",
        "release",
    ]) {
        return true;
    }
    low.contains("deploy") && !low.contains("preview")
}

// ── a workflow's triggers ──────────────────────────────────────────────────

/// What starts a workflow on a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Triggers {
    /// `pull_request` or `pull_request_target`.
    pub on_pr: bool,
    /// The pull request event types it lists; empty: GitHub's default
    /// (opened, synchronize, reopened).
    pub types: Vec<String>,
    pub paths: Vec<String>,
    pub paths_ignore: Vec<String>,
    pub branches: Vec<String>,
    /// It can be started by hand (`workflow_dispatch`).
    pub dispatch: bool,
}

fn strings(v: Option<&serde_yaml::Value>) -> Vec<String> {
    match v {
        Some(serde_yaml::Value::String(s)) => vec![s.clone()],
        Some(serde_yaml::Value::Sequence(s)) => s
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// A workflow's triggers, from its text. Never fails: an unreadable file
/// starts on nothing.
pub fn triggers(yaml: &str) -> Triggers {
    let mut t = Triggers::default();
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return t;
    };
    // `on:` is read by YAML 1.1 parsers as the boolean `true`.
    let Some(on) = v.get("on").or_else(|| v.get(serde_yaml::Value::Bool(true))) else {
        return t;
    };
    let names: Vec<String> = match on {
        serde_yaml::Value::String(s) => vec![s.clone()],
        serde_yaml::Value::Sequence(_) => strings(Some(on)),
        serde_yaml::Value::Mapping(m) => m
            .keys()
            .filter_map(|k| k.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    };
    t.dispatch = names.iter().any(|n| n == "workflow_dispatch");
    for ev in ["pull_request", "pull_request_target"] {
        if !names.iter().any(|n| n == ev) {
            continue;
        }
        t.on_pr = true;
        let e = on.get(ev);
        t.types.extend(strings(e.and_then(|e| e.get("types"))));
        t.paths.extend(strings(e.and_then(|e| e.get("paths"))));
        t.paths_ignore
            .extend(strings(e.and_then(|e| e.get("paths-ignore"))));
        t.branches
            .extend(strings(e.and_then(|e| e.get("branches"))));
    }
    t
}

/// GitHub's filter glob: `*` within a path segment, `**` across them, `?`
/// one character.
pub fn glob(pattern: &str, path: &str) -> bool {
    fn go(p: &[u8], s: &[u8]) -> bool {
        match p.first() {
            None => s.is_empty(),
            Some(b'*') if p.get(1) == Some(&b'*') => {
                let rest = &p[2..];
                let rest = rest.strip_prefix(b"/").unwrap_or(rest);
                (0..=s.len()).any(|i| go(rest, &s[i..])) || (p.get(2) == Some(&b'/') && go(rest, s))
            }
            Some(b'*') => {
                let rest = &p[1..];
                let mut i = 0;
                loop {
                    if go(rest, &s[i..]) {
                        return true;
                    }
                    if i >= s.len() || s[i] == b'/' {
                        return false;
                    }
                    i += 1;
                }
            }
            Some(b'?') => !s.is_empty() && s[0] != b'/' && go(&p[1..], &s[1..]),
            Some(c) => s.first() == Some(c) && go(&p[1..], &s[1..]),
        }
    }
    go(pattern.as_bytes(), path.as_bytes())
}

impl Triggers {
    /// Whether a pull request into `base` changing `changed` starts it.
    /// `paths` with `!` exclusions is read in order, as GitHub does.
    pub fn applies(&self, changed: &[String], base: &str) -> bool {
        if !self.on_pr {
            return false;
        }
        if !self.branches.is_empty() && !self.branches.iter().any(|b| glob(b, base)) {
            return false;
        }
        if !self.paths.is_empty() {
            let hit = changed.iter().any(|f| {
                let mut on = false;
                for p in &self.paths {
                    match p.strip_prefix('!') {
                        Some(neg) if glob(neg, f) => on = false,
                        None if glob(p, f) => on = true,
                        _ => {}
                    }
                }
                on
            });
            if !hit {
                return false;
            }
        }
        if !self.paths_ignore.is_empty()
            && !changed.is_empty()
            && changed
                .iter()
                .all(|f| self.paths_ignore.iter().any(|p| glob(p, f)))
        {
            return false;
        }
        true
    }

    /// Adding a label starts it (its types list `labeled`). Otherwise a
    /// label counts only from the next push.
    pub fn starts_on_label(&self) -> bool {
        self.types.iter().any(|t| t == "labeled")
    }
}

// ── the plan ───────────────────────────────────────────────────────────────

/// One preview or end-to-end job that covers the change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Covering {
    /// The check's name as GitHub shows it (the job's `name`, else its key).
    pub check: String,
    pub role: Option<Role>,
    pub workflow: String,
}

/// What it takes to run the preview and its end-to-end checks on the change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// The jobs that cover the change.
    pub covers: Vec<Covering>,
    /// Existing repository labels to add, in order.
    pub add: Vec<String>,
    /// A label only counts from the next push in a workflow that needs one.
    pub needs_push: bool,
    /// What keeps a covering job from running that CoderHelm doesn't
    /// change, one plain line each.
    pub blocked: Vec<String>,
    /// Covering checks whose workflow skips drafts: they run once the pull
    /// request is marked ready, so a draft is marked ready for them and
    /// waits on them as proof (`ready::Decision::Proving`), never treated
    /// as skipped for good.
    pub draft_gated: Vec<String>,
}

impl Plan {
    /// The repository has a preview or end-to-end path for this change.
    pub fn has_path(&self) -> bool {
        !self.covers.is_empty()
    }
}

/// What [`plan`] reads.
#[derive(Debug, Clone, Default)]
pub struct PlanInput<'a> {
    /// Each workflow file: its path and text.
    pub workflows: &'a [(String, String)],
    pub changed: &'a [String],
    pub base: &'a str,
    pub repo_labels: &'a [String],
    pub on_pr: &'a [String],
    /// Labels the team's settings name.
    pub preferred: &'a [String],
    /// The end-to-end areas of the repository's test map that cover the
    /// changed paths (`e2e::coverage`): an area label is chosen from these
    /// when the repository keeps a map, from the paths' names otherwise.
    pub areas: &'a [String],
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// A contains pattern whose text is only the start of a label
/// (`*E2E:*`, from `contains(join(labels), 'E2E:')`): read as that prefix.
fn prefix_of_contains(pattern: &str) -> Option<&str> {
    pattern
        .strip_prefix('*')
        .and_then(|p| p.strip_suffix('*'))
        .filter(|i| !i.is_empty() && workflow_gate::is_bare_prefix(i))
}

/// The area a label names after its pattern's fixed part (`E2E:payments`
/// for `E2E:*` is `payments`).
fn area_of(pattern: &str, label: &str) -> String {
    let low = label.to_ascii_lowercase();
    if let Some(f) = prefix_of_contains(pattern).map(str::to_ascii_lowercase) {
        if low.starts_with(&f) {
            return norm(&label[f.len()..]);
        }
    }
    let fixed = pattern.trim_matches('*');
    let f = fixed.to_ascii_lowercase();
    let rest = if pattern.ends_with('*') && low.starts_with(&f) {
        &label[f.len()..]
    } else if pattern.starts_with('*') && low.ends_with(&f) {
        &label[..label.len() - f.len()]
    } else {
        label
    };
    norm(rest)
}

/// Whether an area names part of the change: a changed path's directory or
/// file name is that area.
pub fn area_matches(area: &str, changed: &[String]) -> bool {
    if area.len() < 3 {
        return false;
    }
    changed.iter().any(|p| {
        p.split('/').any(|seg| {
            let stem = seg.split('.').next().unwrap_or(seg);
            let s = norm(stem);
            !s.is_empty() && (s == area || (area.len() >= 4 && s.contains(area)))
        })
    })
}

/// The repository labels a pattern means for this change: an exact or
/// contains pattern is the one label it names; a prefix or suffix pattern
/// (`E2E:*`) is every label of it whose area matches the changed paths.
fn labels_for(pattern: &str, i: &PlanInput) -> Vec<String> {
    let prefix = prefix_of_contains(pattern);
    if prefix.is_none()
        && (workflow_gate::is_exact(pattern)
            || (pattern.starts_with('*') && pattern.ends_with('*') && pattern.len() > 2))
    {
        return workflow_gate::resolve(pattern, i.repo_labels, i.preferred)
            .into_iter()
            .collect();
    }
    let fits = |l: &str| match prefix {
        Some(p) => l.len() > p.len() && l.to_ascii_lowercase().starts_with(&p.to_ascii_lowercase()),
        None => workflow_gate::matches(pattern, l),
    };
    // The repository's test map names its areas: its labels are those,
    // created on first use when the repository doesn't have one yet.
    if !i.areas.is_empty() {
        let fixed = prefix.unwrap_or(pattern.trim_matches('*'));
        if !pattern.ends_with('*') {
            return Vec::new();
        }
        let mut out = Vec::new();
        for a in i.areas {
            let want = norm(a);
            let existing = i
                .repo_labels
                .iter()
                .find(|l| fits(l) && area_of(pattern, l) == want);
            let label = existing.cloned().unwrap_or_else(|| format!("{fixed}{a}"));
            if fits(&label) && !workflow_gate::is_bare_prefix(&label) {
                push_unique(&mut out, &label);
            }
        }
        return out;
    }
    i.repo_labels
        .iter()
        .filter(|l| !workflow_gate::is_bare_prefix(l) && fits(l))
        .filter(|l| area_matches(&area_of(pattern, l), i.changed))
        .cloned()
        .collect()
}

/// A way that runs the change's own areas comes before one that runs
/// every test.
fn uses_area(w: &LabelWay) -> bool {
    w.need.iter().any(|p| !workflow_gate::is_exact(p))
}

fn push_unique(out: &mut Vec<String>, l: &str) {
    if !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
        out.push(l.to_string());
    }
}

fn has_label(on_pr: &[String], pattern: &str) -> bool {
    on_pr.iter().any(|l| workflow_gate::matches(pattern, l))
}

/// The labels one way needs added, or why it can't be taken.
fn take_way(w: &LabelWay, i: &PlanInput) -> Result<Vec<String>, String> {
    if let Some(f) = w.forbid.iter().find(|f| has_label(i.on_pr, f)) {
        return Err(format!(
            "the label `{}` on the pull request stops it",
            f.trim_matches('*')
        ));
    }
    let mut add = Vec::new();
    for need in &w.need {
        if has_label(i.on_pr, need) {
            continue;
        }
        let real = labels_for(need, i);
        if real.is_empty() {
            return Err(format!(
                "it needs a `{}` label and none in the repository fits this change",
                need.trim_matches('*')
            ));
        }
        if let Some(s) = real.iter().find(|l| is_shared_env_label(l)) {
            return Err(format!(
                "it needs `{s}`, which deploys to an environment the team shares"
            ));
        }
        for l in real {
            push_unique(&mut add, &l);
        }
    }
    Ok(add)
}

/// The preview and end-to-end jobs that cover the change and what starts
/// them. Pure: every case is tested.
pub fn plan(i: &PlanInput) -> Plan {
    let mut out = Plan::default();
    let mut area_patterns: Vec<String> = Vec::new();
    for (path, yaml) in i.workflows {
        let t = triggers(yaml);
        if !t.applies(i.changed, i.base) {
            continue;
        }
        let wf_name = serde_yaml::from_str::<serde_yaml::Value>(yaml)
            .ok()
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(str::to_string))
            .unwrap_or_default();
        let gate = workflow_gate::read(yaml);
        for job in &gate.jobs {
            let shown = if job.name.is_empty() || job.name.contains("${{") {
                job.id.clone()
            } else {
                job.name.clone()
            };
            let role = match role_of(&shown) {
                Role::Other => role_of(&wf_name),
                r => r,
            };
            if !matches!(role, Role::Preview | Role::E2e) {
                continue;
            }
            out.covers.push(Covering {
                check: shown.clone(),
                role: Some(role),
                workflow: path.clone(),
            });
            if gate.draft {
                push_unique(&mut out.draft_gated, &shown);
            }
            if job.unreadable {
                out.blocked.push(format!(
                    "`{shown}`: its `if:` condition can't be read, so CoderHelm can't tell what starts it"
                ));
                continue;
            }
            for w in &job.ways {
                for p in w.need.iter().filter(|p| !workflow_gate::is_exact(p)) {
                    push_unique(&mut area_patterns, p);
                }
            }
            if job.ways.is_empty() {
                continue;
            }
            if job.ways.iter().any(|w| {
                w.need.iter().all(|n| has_label(i.on_pr, n))
                    && !w.forbid.iter().any(|f| has_label(i.on_pr, f))
            }) {
                continue;
            }
            let mut best: Option<((bool, usize), Vec<String>)> = None;
            let mut why: Vec<String> = Vec::new();
            for w in &job.ways {
                match take_way(w, i) {
                    Ok(add) => {
                        let rank = (!uses_area(w), add.len());
                        if best.as_ref().is_none_or(|(r, _)| rank < *r) {
                            best = Some((rank, add));
                        }
                    }
                    Err(e) => push_unique(&mut why, &e),
                }
            }
            match best.map(|(_, a)| a) {
                Some(add) => {
                    for l in &add {
                        push_unique(&mut out.add, l);
                    }
                    if !add.is_empty() && !t.starts_on_label() {
                        out.needs_push = true;
                    }
                }
                None => out
                    .blocked
                    .push(format!("`{shown}` doesn't run: {}", why.join("; "))),
            }
        }
    }
    // The end-to-end areas the change touches run too, when the workflows
    // read area labels.
    if !out.add.is_empty() {
        for p in &area_patterns {
            for l in labels_for(p, i) {
                if !is_shared_env_label(&l) && !has_label(i.on_pr, &l) {
                    push_unique(&mut out.add, &l);
                }
            }
        }
    }
    out.add.retain(|l| !is_shared_env_label(l));
    out
}

/// A check's result on the head, as [`gaps`] and [`progress`] read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Passed,
    Failed,
    Running,
    Skipped,
}

/// Whether a check on the head is one of the covering jobs (a matrix or
/// reusable workflow's check carries the job's name first).
pub fn is_covering(covers: &[Covering], check: &str) -> bool {
    let c = check.trim().to_ascii_lowercase();
    covers.iter().any(|k| {
        let n = k.check.trim().to_ascii_lowercase();
        c == n || c.starts_with(&format!("{n} / ")) || c.starts_with(&format!("{n} ("))
    })
}

/// Whether a skipped check on the head would have covered this change: a
/// preview or end-to-end check skipped on a change with a visible surface.
/// Read from its name, so it holds even when the workflows couldn't be read.
pub fn skipped_is_gap(check: &str, visible: bool) -> bool {
    visible && matches!(role_of(check), Role::Preview | Role::E2e)
}

/// The checks that cover the change and didn't run on the head: CoderHelm
/// starts them, they are never listed as evidence.
pub fn gaps(checks: &[(String, Outcome)], covers: &[Covering], visible: bool) -> Vec<String> {
    checks
        .iter()
        .filter(|(n, o)| {
            *o == Outcome::Skipped && (is_covering(covers, n) || skipped_is_gap(n, visible))
        })
        .map(|(n, _)| n.clone())
        .collect()
}

// ── where the preview stands ───────────────────────────────────────────────

/// Where the preview stands on a head.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// No visible surface, or the repository has no preview path for it.
    #[default]
    NotNeeded,
    /// CoderHelm added what starts the preview; it hasn't reported yet.
    Triggered,
    /// The preview deploy or its end-to-end checks are running.
    Deploying,
    /// The preview is up and its end-to-end checks passed.
    Ready,
    /// The preview is checked: up, end-to-end passed, pages captured (or
    /// why not said).
    Checked,
    /// The preview deploy or its end-to-end checks failed.
    Failed,
    /// Something CoderHelm doesn't change keeps it from running.
    Blocked,
}

/// One captured page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shot {
    /// The page's path (`/join`).
    pub page: String,
    /// `desktop` or `phone`.
    pub width: String,
    /// The image's address.
    pub src: String,
}

/// One end-to-end or preview check's result on the head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    pub name: String,
    pub outcome: Outcome,
    #[serde(default)]
    pub url: String,
}

/// What the run records about the preview of its pull request's head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    pub head: String,
    pub stage: Stage,
    /// The checks that cover the change.
    pub covers: Vec<Covering>,
    /// Labels CoderHelm added to start them.
    pub labels_added: Vec<String>,
    pub url: String,
    /// The preview and end-to-end checks' results on the head.
    pub checks: Vec<CheckResult>,
    pub shots: Vec<Shot>,
    /// One line: why there is no preview, what blocks it, or why no page
    /// was captured.
    pub why: String,
    /// The change needs a new or updated end-to-end spec and has one.
    pub e2e_required: bool,
    pub e2e_covered: bool,
    /// Covering checks that skip drafts: they run once it is ready.
    pub draft_gated: Vec<String>,
    /// The commands that check the changed area ([`how_to_check`]).
    pub how_to_check: String,
    /// The CI jobs that run on the changed paths (`crate::ci_cover`):
    /// "How to check" and Open items name these, never another folder's.
    pub ci_jobs: Vec<crate::ci_cover::CoveringJob>,
    /// The failed end-to-end checks on the head, each as its own report
    /// says (`crate::e2e::failure_read`), with whether this change caused
    /// it.
    pub failures: Vec<crate::e2e::failure_read::FailureRead>,
    /// Where the preview's address came from (`crate::preview_find`).
    pub found: crate::preview_find::Found,
    /// When this head's preview was first waited on (RFC 3339): the wait is
    /// bounded by `preview_find::WAIT_BUDGET_MINS`.
    pub waiting_since: String,
    /// When the preview was last looked at (RFC 3339): a pending preview is
    /// looked at again every `preview_find::POLL_SECS` even when nothing
    /// else on the pull request changed.
    pub looked_at: String,
}

impl State {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(s: &str) -> Option<State> {
        serde_json::from_str(s).ok()
    }

    /// Still on its way: the description and the ready decision wait.
    pub fn pending(&self) -> bool {
        matches!(
            self.stage,
            Stage::Triggered | Stage::Deploying | Stage::Ready
        )
    }

    /// One plain phrase for a status line ("preview checked").
    pub fn phrase(&self) -> Option<&'static str> {
        match self.stage {
            Stage::NotNeeded => None,
            Stage::Triggered | Stage::Deploying => Some("preview deploying"),
            Stage::Ready => Some("preview up, pages being captured"),
            Stage::Checked => Some("preview checked"),
            Stage::Failed => Some("preview checks failed"),
            Stage::Blocked => Some("preview not run"),
        }
    }
}

/// The stage the covering checks on the head show. `triggered`: CoderHelm
/// started them on this head.
pub fn progress(results: &[CheckResult], triggered: bool) -> Stage {
    if results.iter().any(|r| r.outcome == Outcome::Failed) {
        return Stage::Failed;
    }
    if results.iter().any(|r| r.outcome == Outcome::Running) {
        return Stage::Deploying;
    }
    let ran: Vec<&CheckResult> = results
        .iter()
        .filter(|r| r.outcome == Outcome::Passed)
        .collect();
    if !ran.is_empty() && results.iter().all(|r| r.outcome == Outcome::Passed) {
        return Stage::Ready;
    }
    if triggered {
        return Stage::Triggered;
    }
    if !ran.is_empty() {
        return Stage::Ready;
    }
    Stage::Triggered
}

// ── end-to-end specs ───────────────────────────────────────────────────────

/// The repository's end-to-end suite: the folder its runner's config is in.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Suite {
    /// The folder (`e2e`, `apps/web/e2e`), "" for the repository's root.
    pub dir: String,
    pub runner: String,
}

/// The e2e suite in a repository's tree, if it has one.
pub fn e2e_suite(tree: &[String]) -> Option<Suite> {
    let mut found: Vec<Suite> = tree
        .iter()
        .filter(|p| !p.contains("node_modules/"))
        .filter_map(|p| {
            let name = p.rsplit('/').next().unwrap_or(p);
            let runner = if name.starts_with("playwright.config.") {
                "playwright"
            } else if name.starts_with("cypress.config.") || name == "cypress.json" {
                "cypress"
            } else {
                return None;
            };
            let dir = p.rsplit_once('/').map(|(d, _)| d).unwrap_or("").to_string();
            Some(Suite {
                dir,
                runner: runner.to_string(),
            })
        })
        .collect();
    // The suite nearest the root, then the shortest path.
    found.sort_by_key(|s| (s.dir.matches('/').count(), s.dir.len()));
    found.into_iter().next()
}

fn is_test_file(p: &str) -> bool {
    let p = p.to_ascii_lowercase();
    p.contains("/test/")
        || p.contains("/tests/")
        || p.contains("__tests__")
        || p.contains(".test.")
        || p.contains(".spec.")
        || p.contains("_test.")
        || p.contains(".cy.")
        || p.contains("/e2e/")
        || p.starts_with("e2e/")
}

/// Whether a path is an end-to-end spec (or its page object or fixture) of
/// the suite.
pub fn is_e2e_spec(path: &str, suite: &Suite) -> bool {
    let p = path.to_ascii_lowercase();
    let code = [".ts", ".tsx", ".js", ".mjs", ".cjs", ".jsx", ".feature"]
        .iter()
        .any(|e| p.ends_with(e));
    if !code || p.contains("playwright.config.") || p.contains("cypress.config.") {
        return false;
    }
    if suite.dir.is_empty() {
        p.starts_with("e2e/")
            || p.contains("/e2e/")
            || p.starts_with("cypress/")
            || p.contains(".e2e.")
            || p.contains(".cy.")
    } else {
        p.starts_with(&format!("{}/", suite.dir.to_ascii_lowercase()))
    }
}

/// A changed file a person sees in the browser: UI code, styles, pages,
/// a page's actions and hooks. Never a test, a config or generated output.
pub fn is_user_visible(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    if is_test_file(&p) || crate::pr_facts::is_generated(&p) {
        return false;
    }
    let name = p.rsplit('/').next().unwrap_or(&p);
    if name.contains(".config.") || name.starts_with('.') || name.ends_with(".d.ts") {
        return false;
    }
    if crate::pr_facts::is_ui(&p) {
        return true;
    }
    let ext = name.rsplit('.').next().unwrap_or("");
    let web = ["ts", "js", "mjs", "tsx", "jsx", "vue", "svelte"].contains(&ext);
    let at = format!("/{p}");
    web && [
        "/app/",
        "/pages/",
        "/actions/",
        "/hooks/",
        "/views/",
        "/routes/",
        "/screens/",
        "/features/",
    ]
    .iter()
    .any(|d| at.contains(d))
}

/// Whether the change needs an end-to-end spec, and whether it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum E2eNeed {
    /// No visible surface, or the repository has no e2e suite.
    NotNeeded,
    /// A visible change in a repository with a suite: `covered` when the
    /// change adds or updates a spec in it.
    Required {
        suite: Suite,
        visible: Vec<String>,
        covered: bool,
    },
}

impl E2eNeed {
    pub fn missing(&self) -> bool {
        matches!(self, E2eNeed::Required { covered: false, .. })
    }

    /// The one line the coder, the reviewer and the ready check act on
    /// when a spec is missing.
    pub fn instruction(&self) -> Option<String> {
        let E2eNeed::Required {
            suite,
            visible,
            covered: false,
        } = self
        else {
            return None;
        };
        let at = if suite.dir.is_empty() {
            "the repository's end-to-end folder".to_string()
        } else {
            format!("`{}/`", suite.dir)
        };
        Some(format!(
            "The change alters what a person sees in the browser ({}) and the repository has a {} end-to-end suite in {at}, but no end-to-end spec there covers it. Write or update one for the new behaviour, following the suite's own conventions (its folders, page objects, tags and area labels, and the repository's AGENTS.md or CLAUDE.md), so it fails without the change and passes with it.",
            visible
                .iter()
                .take(3)
                .map(|v| format!("`{v}`"))
                .collect::<Vec<_>>()
                .join(", "),
            suite.runner
        ))
    }
}

/// [`E2eNeed`] from the changed paths and the repository's suite.
pub fn e2e_need(changed: &[String], suite: Option<&Suite>) -> E2eNeed {
    let Some(suite) = suite else {
        return E2eNeed::NotNeeded;
    };
    let visible: Vec<String> = changed
        .iter()
        .filter(|p| is_user_visible(p) && !is_e2e_spec(p, suite))
        .cloned()
        .collect();
    if visible.is_empty() {
        return E2eNeed::NotNeeded;
    }
    let covered = changed.iter().any(|p| is_e2e_spec(p, suite));
    E2eNeed::Required {
        suite: suite.clone(),
        visible,
        covered,
    }
}

// ── pages to capture ───────────────────────────────────────────────────────

const GENERIC: &[&str] = &[
    "apps",
    "app",
    "src",
    "web",
    "lib",
    "libs",
    "components",
    "component",
    "pages",
    "page",
    "features",
    "feature",
    "index",
    "use",
    "hooks",
    "hook",
    "actions",
    "action",
    "services",
    "service",
    "test",
    "tests",
    "utils",
    "util",
    "types",
    "packages",
    "client",
    "server",
    "ui",
    "layout",
    "shared",
    "common",
    "modules",
    "module",
    "tsx",
    "jsx",
    "mjs",
    "the",
    "and",
];

fn tokens(path: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in path.chars() {
        if c.is_ascii_alphanumeric() {
            if c.is_ascii_uppercase() && prev_lower && !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
            cur.push(c.to_ascii_lowercase());
        } else {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            prev_lower = false;
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out.retain(|t| t.len() >= 3 && !GENERIC.contains(&t.as_str()));
    out.dedup();
    out
}

/// The route a page file serves, or None for a file that isn't a page (or
/// a dynamic one, which has no address without data).
pub fn route_of(path: &str) -> Option<String> {
    let p = path.replace('\\', "/");
    let segs: Vec<&str> = p.split('/').collect();
    let name = *segs.last()?;
    let stem = name.split('.').next().unwrap_or(name);
    let ext = name.rsplit('.').next().unwrap_or("");
    if !["tsx", "jsx", "ts", "js", "mdx", "vue", "svelte"].contains(&ext) || is_test_file(&p) {
        return None;
    }
    let route_from = |dirs: &[&str]| -> Option<String> {
        let mut parts = Vec::new();
        for d in dirs {
            if d.starts_with('(') && d.ends_with(')') || d.starts_with('@') || d.starts_with('_') {
                continue;
            }
            if d.contains('[') {
                return None;
            }
            parts.push(*d);
        }
        Some(format!("/{}", parts.join("/")))
    };
    // App router: `app/<dirs>/page.tsx`.
    if stem == "page" {
        let at = segs.iter().rposition(|s| *s == "app")?;
        return route_from(&segs[at + 1..segs.len() - 1]);
    }
    // Pages router: `pages/<dirs>/<name>.tsx`.
    let at = segs.iter().rposition(|s| *s == "pages")?;
    if ["_app", "_document", "_error", "404", "500"].contains(&stem)
        || segs[at + 1..].first() == Some(&"api")
        || stem.contains('[')
    {
        return None;
    }
    let mut dirs: Vec<&str> = segs[at + 1..segs.len() - 1].to_vec();
    if stem != "index" {
        dirs.push(stem);
    }
    route_from(&dirs)
}

/// The pages to capture for the change: its changed pages first, then the
/// pages whose path shares the most words with the changed files, at most
/// [`MAX_PAGES`]; the home page when nothing names one.
pub fn routes(tree: &[String], changed: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in changed {
        if let Some(r) = route_of(c) {
            push_unique(&mut out, &r);
        }
    }
    let want: Vec<String> = changed
        .iter()
        .filter(|c| is_user_visible(c))
        .flat_map(|c| tokens(c))
        .collect();
    let mut scored: Vec<(usize, String)> = tree
        .iter()
        .filter_map(|p| {
            let r = route_of(p)?;
            let have = tokens(p);
            let n = have.iter().filter(|t| want.contains(t)).count();
            (n > 0).then_some((n, r))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.len().cmp(&b.1.len())));
    for (_, r) in scored {
        push_unique(&mut out, &r);
    }
    out.truncate(MAX_PAGES);
    if out.is_empty() {
        out.push("/".to_string());
    }
    out
}

/// The file name a page's capture is saved under (`join-desktop`).
pub fn shot_name(page: &str, width: &str) -> String {
    let slug: String = page
        .trim_matches('/')
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let slug = if slug.is_empty() {
        "home".to_string()
    } else {
        slug
    };
    format!("{slug}-{width}")
}

/// The address pages are captured under: the preview's address without
/// its query or fragment and without a trailing `/`, its path kept (a
/// preview served under a path, `https://host/pr-12/`, keeps it).
pub fn capture_base(url: &str) -> String {
    url.split(['?', '#'])
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .to_string()
}

/// How long the capture waits between tries of a page that is still
/// warming up (404, 502, 503 or 504 on a fresh preview), and how many
/// tries it makes.
pub const WARM_TRIES: u32 = 3;
pub const WARM_WAIT_MS: u32 = 15_000;

/// The sandbox script that captures each page at both widths. It writes
/// each image base64-encoded (`<name>.png.b64`) and `shots.json` (each
/// page's status, title and, when the browser was sent to another host,
/// that host) to `$CODERHELM_OUT`. A page still warming up is tried
/// [`WARM_TRIES`] times; one that doesn't load is recorded with its status,
/// never retried into a pass.
///
/// `access` is how the preview is protected (`crate::preview_access`). Its
/// secret is never in the script: the sandbox hands the script its one-time
/// secret in `GH_TOKEN` (the channel every sandbox script's secret takes),
/// the script removes it from its environment at once, sends it only to the
/// preview's own host, and blanks it out of anything it writes.
pub fn capture_script(base: &str, pages: &[String], access: crate::preview_access::Kind) -> String {
    let base = capture_base(base);
    let mut jobs = Vec::new();
    for p in pages {
        for (width, (w, h)) in [("desktop", DESKTOP), ("phone", PHONE)] {
            jobs.push(serde_json::json!({
                "page": p,
                "width": width,
                "w": w,
                "h": h,
                "url": format!("{base}{p}"),
                "name": shot_name(p, width),
            }));
        }
    }
    let jobs = serde_json::to_string(&jobs).unwrap_or_else(|_| "[]".into());
    let base_js = serde_json::to_string(&base).unwrap_or_else(|_| "\"\"".into());
    let kind = serde_json::to_string(access.as_str()).unwrap_or_else(|_| "\"none\"".into());
    format!(
        r#"import {{ chromium }} from 'playwright';
import {{ writeFileSync }} from 'node:fs';
const out = process.env.CODERHELM_OUT || '.';
const secret = (process.env.GH_TOKEN || '').trim();
delete process.env.GH_TOKEN; delete process.env.GITHUB_TOKEN;
const kind = {kind};
const host = new URL({base_js}).host;
const hide = (s) => secret ? String(s).split(secret).join('***') : String(s);
const ctxOpts = {{}};
const headers = {{}};
if (kind === 'header' && secret) {{
  for (const line of secret.split('\n')) {{
    const i = line.indexOf(':');
    if (i > 0) headers[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
  }}
}}
if (kind === 'basic' && secret) {{
  const i = secret.indexOf(':');
  ctxOpts.httpCredentials = {{ username: secret.slice(0, i), password: secret.slice(i + 1), origin: 'https://' + host }};
}}
const withQuery = (u) => (kind === 'query' && secret) ? u + (u.includes('?') ? '&' : '?') + secret : u;
const warming = (s) => [404, 502, 503, 504].includes(s);
const jobs = {jobs};
const results = [];
const browser = await chromium.launch();
for (const j of jobs) {{
  const context = await browser.newContext({{ viewport: {{ width: j.w, height: j.h }}, ...ctxOpts }});
  if (Object.keys(headers).length) {{
    // The secret goes to the preview's own host only, never to a font, a
    // script or an image another host serves.
    await context.route('**/*', (route) => {{
      const req = route.request();
      let h = '';
      try {{ h = new URL(req.url()).host; }} catch (e) {{}}
      if (h === host) route.continue({{ headers: {{ ...req.headers(), ...headers }} }});
      else route.continue();
    }});
  }}
  const page = await context.newPage();
  let status = 0, title = '', error = '', left = '';
  for (let t = 1; t <= {WARM_TRIES}; t++) {{
    error = '';
    try {{
      const res = await page.goto(withQuery(j.url), {{ waitUntil: 'networkidle', timeout: 45000 }});
      status = res ? res.status() : 0;
    }} catch (e) {{ status = 0; error = hide(String(e).slice(0, 300)); }}
    if (!warming(status) && !(status === 0 && error) || t === {WARM_TRIES}) break;
    await new Promise((r) => setTimeout(r, {WARM_WAIT_MS}));
  }}
  try {{
    const now = new URL(page.url()).host;
    if (now && now !== host) left = now;
    title = hide(await page.title());
    if (status > 0 && status < 400 && !left) {{
      const png = await page.screenshot({{ fullPage: false }});
      writeFileSync(`${{out}}/${{j.name}}.png.b64`, png.toString('base64'));
    }}
  }} catch (e) {{ error = error || hide(String(e).slice(0, 300)); }}
  results.push({{ page: j.page, width: j.width, name: j.name, status, title, error, left }});
  await context.close();
}}
await browser.close();
writeFileSync(`${{out}}/shots.json`, JSON.stringify(results));
"#
    )
}

/// The command that runs [`capture_script`] (saved as `capture.mjs`).
pub fn capture_command() -> String {
    format!(
        "npm init -y >/dev/null 2>&1; npm i --no-save --no-audit --no-fund playwright@{PLAYWRIGHT} >/dev/null 2>&1 && npx --no-install playwright install --with-deps chromium >/dev/null 2>&1 && node capture.mjs"
    )
}

/// Hosts the capture needs besides the preview's: the package registry and
/// the browser download.
pub const CAPTURE_HOSTS: &[&str] = &[
    "registry.npmjs.org",
    "cdn.playwright.dev",
    "playwright.download.prss.microsoft.com",
    "playwright.azureedge.net",
];

/// One page's capture as `shots.json` reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Captured {
    pub page: String,
    pub width: String,
    pub name: String,
    pub status: u16,
    pub title: String,
    pub error: String,
    /// The host the browser was sent to instead of the preview's (a
    /// sign-in page), "" when it stayed.
    pub left: String,
}

impl Captured {
    /// The page loaded on the preview itself.
    pub fn shot(&self) -> bool {
        (1..400).contains(&self.status) && self.left.is_empty()
    }
}

/// Where a team gives CoderHelm a protected preview's access.
pub const ACCESS_SETTING: &str = "Settings, Repositories, Preview access";

/// Why no page was captured, from the capture's report: a sign-in wall
/// (the preview answered 401 or 403, or sent the browser to another host),
/// a page that didn't load, or nothing at all. None when one was.
/// `has_access`: the team saved the preview's access, so a wall means that
/// access was refused.
pub fn why_no_shot(report: &[Captured], has_access: bool) -> Option<String> {
    if report.iter().any(Captured::shot) {
        return None;
    }
    let wall = report
        .iter()
        .find(|c| c.status == 401 || c.status == 403 || !c.left.is_empty());
    if let Some(c) = wall {
        let how = if c.left.is_empty() {
            format!("it answered {}", c.status)
        } else {
            format!("it sent the browser to `{}`", c.left)
        };
        return Some(if has_access {
            format!(
                "the preview refused the access saved in {ACCESS_SETTING} ({how}), so its pages couldn't be captured"
            )
        } else {
            format!(
                "the preview asks for a sign-in ({how}), so its pages couldn't be captured; give CoderHelm its access in {ACCESS_SETTING}"
            )
        });
    }
    if let Some(c) = report.iter().find(|c| c.status >= 400) {
        return Some(format!(
            "the preview answered {} for `{}`, so it couldn't be captured",
            c.status, c.page
        ));
    }
    if let Some(c) = report.iter().find(|c| !c.error.is_empty()) {
        return Some(format!(
            "the preview page `{}` didn't load ({})",
            c.page,
            crate::truncate_str(&c.error, 120)
        ));
    }
    Some("the capture in the sandbox gave no result".into())
}

/// The repository's unit test command, from its tree: the runner its
/// config files name. Empty when none is known.
pub fn unit_cmd(tree: &[String]) -> String {
    let has = |prefix: &str| {
        tree.iter().any(|p| {
            !p.contains("node_modules/")
                && p.rsplit('/').next().is_some_and(|n| n.starts_with(prefix))
        })
    };
    if has("vitest.config.") || has("vitest.workspace.") {
        "npx vitest run".into()
    } else if has("jest.config.") {
        "npx jest".into()
    } else if has("pytest.ini") || has("conftest.py") {
        "pytest".into()
    } else if tree.iter().any(|p| p == "go.mod") {
        "go test".into()
    } else {
        String::new()
    }
}

/// The commands that check the changed area by hand: the end-to-end specs
/// the change adds or updates, run against the preview, and the changed
/// unit tests. Empty when none is known.
pub fn how_to_check(changed: &[String], suite: Option<&Suite>, unit_cmd: &str) -> String {
    let mut parts = Vec::new();
    if let Some(s) = suite {
        let specs: Vec<&String> = changed
            .iter()
            .filter(|p| {
                is_e2e_spec(p, s)
                    && (p.contains(".spec.") || p.contains(".cy.") || p.contains(".e2e."))
            })
            .take(3)
            .collect();
        if !specs.is_empty() {
            let rel: Vec<String> = specs
                .iter()
                .map(|p| {
                    p.strip_prefix(&format!("{}/", s.dir))
                        .unwrap_or(p)
                        .to_string()
                })
                .collect();
            let dir = if s.dir.is_empty() {
                String::new()
            } else {
                format!("cd {} && ", s.dir)
            };
            let cmd = if s.runner == "cypress" {
                format!("{dir}npx cypress run --spec {}", rel.join(","))
            } else {
                format!("{dir}npx playwright test {}", rel.join(" "))
            };
            parts.push(format!("`{cmd}` against the preview"));
        }
    }
    let unit: Vec<&String> = changed
        .iter()
        .filter(|p| {
            is_test_file(p)
                && !suite.is_some_and(|s| is_e2e_spec(p, s))
                && (p.contains(".test.") || p.contains(".spec.") || p.contains("_test."))
        })
        .take(4)
        .collect();
    if !unit.is_empty() && !unit_cmd.trim().is_empty() {
        parts.push(format!(
            "`{} {}`",
            unit_cmd.trim(),
            unit.iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    parts.join(", then ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    /// A web repository whose preview deploy and preview end-to-end checks
    /// run only with labels, as many monorepos gate them.
    const PREVIEW_WF: &str = r#"
name: Preview
on:
  pull_request:
    types: [opened, synchronize, reopened, labeled]
    paths:
      - "apps/web/**"
      - "packages/**"
      - "!**/*.md"
jobs:
  deploy_preview:
    name: Deploy Preview Web App
    if: >-
      github.event.pull_request.draft != true && (
      contains(github.event.pull_request.labels.*.name, 'CI:E2E') ||
      contains(join(github.event.pull_request.labels.*.name, ','), 'E2E:'))
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
  e2e_preview:
    name: E2E on preview
    needs: deploy_preview
    if: "!contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')"
    runs-on: ubuntu-latest
    steps:
      - run: echo e2e
  cleanup:
    name: Cleanup PR Preview
    if: github.event.action == 'closed'
    runs-on: ubuntu-latest
    steps:
      - run: echo bye
"#;

    const STAGING_WF: &str = r#"
name: Staging
on:
  pull_request:
    types: [labeled, synchronize]
jobs:
  staging:
    name: Deploy to Staging
    if: contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')
    runs-on: ubuntu-latest
    steps:
      - run: echo staging
  e2e_staging:
    name: E2E after staging deploy
    needs: staging
    runs-on: ubuntu-latest
    steps:
      - run: echo e2e
"#;

    const PROD_WF: &str = r#"
name: Release
on:
  push:
    branches: [main]
jobs:
  deploy:
    name: Deploy to Production
    runs-on: ubuntu-latest
    steps:
      - run: echo prod
"#;

    fn repo_labels() -> Vec<String> {
        s(&[
            "CI:E2E",
            "CI:DEPLOY_STAGING",
            "E2E:signup",
            "E2E:checkout",
            "E2E:account",
            "bug",
        ])
    }

    fn workflows() -> Vec<(String, String)> {
        vec![
            (".github/workflows/preview.yml".into(), PREVIEW_WF.into()),
            (".github/workflows/staging.yml".into(), STAGING_WF.into()),
            (".github/workflows/release.yml".into(), PROD_WF.into()),
        ]
    }

    fn changed() -> Vec<String> {
        s(&[
            "apps/web/src/components/signup/use-signup-form.ts",
            "apps/web/src/lib/validation-utils.ts",
            "apps/web/src/test/lib/validation-utils.test.ts",
        ])
    }

    #[test]
    fn a_label_gated_preview_gets_its_label_and_the_matching_area() {
        let (w, c, l) = (workflows(), changed(), repo_labels());
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &[],
            preferred: &[],
            areas: &[],
        });
        let names: Vec<&str> = p.covers.iter().map(|c| c.check.as_str()).collect();
        assert_eq!(names, vec!["Deploy Preview Web App", "E2E on preview"]);
        // The e2e area the changed paths name runs the change's own specs
        // (before the label that runs every test); never the staging label,
        // never an area the change doesn't touch.
        assert_eq!(p.add, s(&["E2E:signup"]));
        // Both skip drafts: they run once the pull request is ready, and
        // are waited on as proof, never read as skipped for good.
        assert_eq!(
            p.draft_gated,
            s(&["Deploy Preview Web App", "E2E on preview"])
        );
        assert!(!p.needs_push, "the workflow starts on `labeled`");
        assert!(p.blocked.is_empty(), "{:?}", p.blocked);
    }

    #[test]
    fn a_label_counts_from_the_next_push_when_the_workflow_ignores_labeled() {
        let wf = PREVIEW_WF.replace(", labeled]", "]");
        let (w, c, l) = (
            vec![(".github/workflows/preview.yml".to_string(), wf)],
            changed(),
            repo_labels(),
        );
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &[],
            preferred: &[],
            areas: &[],
        });
        assert_eq!(p.add, s(&["E2E:signup"]));
        assert!(p.needs_push);
    }

    #[test]
    fn the_test_map_names_the_area_even_before_its_label_exists() {
        let (w, c) = (workflows(), changed());
        let l = s(&["CI:E2E", "CI:DEPLOY_STAGING"]);
        let areas = s(&["onboarding"]);
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &[],
            preferred: &[],
            areas: &areas,
        });
        assert_eq!(p.add, s(&["E2E:onboarding"]));
    }

    #[test]
    fn a_change_outside_the_workflow_paths_has_no_preview_path() {
        let (w, l) = (workflows(), repo_labels());
        let c = s(&["services/api/src/main.rs", "docs/readme.md"]);
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &[],
            preferred: &[],
            areas: &[],
        });
        assert!(!p.has_path());
        assert!(p.add.is_empty());
    }

    #[test]
    fn labels_already_on_the_pull_request_are_not_added_again() {
        let (w, c, l) = (workflows(), changed(), repo_labels());
        let on = s(&["CI:E2E"]);
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &on,
            preferred: &[],
            areas: &[],
        });
        assert!(p.add.is_empty(), "{:?}", p.add);
    }

    #[test]
    fn a_preview_that_needs_only_a_shared_environment_label_is_blocked_not_labelled() {
        let wf = r#"
on:
  pull_request:
    types: [labeled]
jobs:
  preview:
    name: Deploy Preview
    if: contains(github.event.pull_request.labels.*.name, 'deploy-staging')
    runs-on: ubuntu-latest
    steps: [{run: x}]
"#;
        let w = vec![("p.yml".to_string(), wf.to_string())];
        let (c, l) = (changed(), s(&["deploy-staging"]));
        let p = plan(&PlanInput {
            workflows: &w,
            changed: &c,
            base: "main",
            repo_labels: &l,
            on_pr: &[],
            preferred: &[],
            areas: &[],
        });
        assert!(p.add.is_empty());
        assert_eq!(p.blocked.len(), 1);
        assert!(
            p.blocked[0].contains("environment the team shares"),
            "{}",
            p.blocked[0]
        );
    }

    #[test]
    fn a_preview_deploy_label_is_not_a_shared_environment() {
        assert!(!is_shared_env_label("CI:DEPLOY_PREVIEW"));
        assert!(!is_shared_env_label("CI:E2E"));
        assert!(is_shared_env_label("CI:DEPLOY_STAGING"));
        assert!(is_shared_env_label("deploy"));
        assert!(is_shared_env_label("release-prod"));
    }

    #[test]
    fn check_roles_read_from_their_names() {
        assert_eq!(role_of("Deploy Preview Web App"), Role::Preview);
        assert_eq!(role_of("E2E on preview"), Role::E2e);
        assert_eq!(role_of("Cleanup PR Preview"), Role::Cleanup);
        assert_eq!(role_of("Deploy to Staging"), Role::Staging);
        assert_eq!(role_of("E2E after staging deploy"), Role::Staging);
        assert_eq!(role_of("Deploy to Production"), Role::Production);
        assert_eq!(role_of("Deploy Web App"), Role::Other);
        assert_eq!(role_of("Test CloudFront Cache Behavior"), Role::Other);
        assert_eq!(role_of("unit tests"), Role::Other);
    }

    #[test]
    fn a_skipped_preview_or_e2e_check_on_a_visible_change_is_a_gap() {
        let checks = vec![
            ("Deploy Preview Web App".to_string(), Outcome::Skipped),
            ("E2E on preview".to_string(), Outcome::Skipped),
            ("Deploy to Staging".to_string(), Outcome::Skipped),
            ("E2E after staging deploy".to_string(), Outcome::Skipped),
            ("Cleanup PR Preview".to_string(), Outcome::Skipped),
            ("Deploy to Production".to_string(), Outcome::Skipped),
            ("unit tests".to_string(), Outcome::Passed),
        ];
        // Even with no workflow read: the names say what they cover.
        assert_eq!(
            gaps(&checks, &[], true),
            s(&["Deploy Preview Web App", "E2E on preview"])
        );
        // A change with no visible surface has no preview gap.
        assert!(gaps(&checks, &[], false).is_empty());
    }

    #[test]
    fn the_preview_is_ready_only_once_every_covering_check_passed() {
        let r = |o| CheckResult {
            name: "x".into(),
            outcome: o,
            url: String::new(),
        };
        assert_eq!(progress(&[], true), Stage::Triggered);
        assert_eq!(progress(&[r(Outcome::Skipped)], true), Stage::Triggered);
        assert_eq!(
            progress(&[r(Outcome::Passed), r(Outcome::Running)], true),
            Stage::Deploying
        );
        assert_eq!(
            progress(&[r(Outcome::Passed), r(Outcome::Failed)], true),
            Stage::Failed
        );
        assert_eq!(
            progress(&[r(Outcome::Passed), r(Outcome::Passed)], true),
            Stage::Ready
        );
    }

    #[test]
    fn a_visible_change_needs_an_e2e_spec_in_a_repo_with_a_suite() {
        let tree = s(&[
            "apps/web/e2e/playwright.config.ts",
            "apps/web/e2e/tests/signup.spec.ts",
            "apps/web/src/app/(marketing)/join/page.tsx",
            "apps/web/src/app/account/page.tsx",
            "apps/web/src/app/account/[id]/page.tsx",
        ]);
        let suite = e2e_suite(&tree).unwrap();
        assert_eq!(suite.dir, "apps/web/e2e");
        let need = e2e_need(&changed(), Some(&suite));
        assert!(need.missing(), "{need:?}");
        assert!(need.instruction().unwrap().contains("`apps/web/e2e/`"));
        let mut with_spec = changed();
        with_spec.push("apps/web/e2e/tests/signup.spec.ts".into());
        assert!(!e2e_need(&with_spec, Some(&suite)).missing());
        // No suite, or nothing visible: nothing to write.
        assert_eq!(e2e_need(&changed(), None), E2eNeed::NotNeeded);
        assert_eq!(
            e2e_need(&s(&["apps/web/src/lib/validation-utils.ts"]), Some(&suite)),
            E2eNeed::NotNeeded
        );
    }

    #[test]
    fn pages_to_capture_come_from_the_page_files_the_change_relates_to() {
        let tree = s(&[
            "apps/web/src/app/page.tsx",
            "apps/web/src/app/(marketing)/signup/page.tsx",
            "apps/web/src/app/account/page.tsx",
            "apps/web/src/app/account/[id]/page.tsx",
            "apps/web/src/pages/_app.tsx",
        ]);
        assert_eq!(routes(&tree, &changed()), s(&["/signup"]));
        assert_eq!(route_of("apps/web/src/app/page.tsx").as_deref(), Some("/"));
        assert_eq!(route_of("apps/web/src/app/account/[id]/page.tsx"), None);
        assert_eq!(
            route_of("web/pages/about/team.tsx").as_deref(),
            Some("/about/team")
        );
        assert_eq!(routes(&tree, &s(&["apps/web/src/styles/x.css"])), s(&["/"]));
    }

    #[test]
    fn the_capture_script_takes_each_page_at_desktop_and_phone_width() {
        use crate::preview_access::Kind;
        let js = capture_script(
            "https://pr-1.preview-web.example.com/",
            &s(&["/signup"]),
            Kind::None,
        );
        assert!(js.contains("https://pr-1.preview-web.example.com/signup"));
        assert!(js.contains("signup-desktop") && js.contains("signup-phone"));
        assert!(js.contains("\"w\":390") && js.contains("\"w\":1280"));
        // A preview served under a path keeps it; its query is dropped.
        let js = capture_script(
            "https://previews.example.com/pr-12/?x=1",
            &s(&["/signup"]),
            Kind::None,
        );
        assert!(js.contains("https://previews.example.com/pr-12/signup"));
        let report = vec![Captured {
            status: 401,
            page: "/signup".into(),
            ..Default::default()
        }];
        let why = why_no_shot(&report, false).unwrap();
        assert!(
            why.contains("sign-in") && why.contains(ACCESS_SETTING),
            "{why}"
        );
        assert!(why_no_shot(&report, true).unwrap().contains("refused"));
        // Sent to a sign-in page on another host: a wall, never a shot.
        let sent = vec![Captured {
            status: 200,
            left: "sso.example.org".into(),
            ..Default::default()
        }];
        assert!(why_no_shot(&sent, false)
            .unwrap()
            .contains("sso.example.org"));
        let ok = vec![Captured {
            status: 200,
            ..Default::default()
        }];
        assert_eq!(why_no_shot(&ok, false), None);
    }

    #[test]
    fn a_protected_previews_secret_never_enters_the_script_and_goes_only_to_its_host() {
        use crate::preview_access::Kind;
        for kind in [Kind::Header, Kind::Query, Kind::Basic] {
            let js = capture_script("https://pr-1.preview.example.com", &s(&["/"]), kind);
            assert!(js.contains(&format!("const kind = \"{}\"", kind.as_str())));
            // Read once from the one-time secret, then removed from the
            // environment, and blanked out of what is written.
            assert!(js.contains("delete process.env.GH_TOKEN"));
            assert!(js.contains("const hide ="));
        }
        let js = capture_script("https://pr-1.preview.example.com", &s(&["/"]), Kind::Header);
        assert!(js.contains("if (h === host) route.continue({ headers:"));
        assert!(js.contains("origin: 'https://' + host"));
        // A page still warming up is tried again, a bounded number of times.
        assert!(js.contains("[404, 502, 503, 504]"));
        assert!(js.contains(&format!("t <= {WARM_TRIES}")));
    }

    #[test]
    fn how_to_check_names_the_changed_specs_and_tests_never_a_bare_suite() {
        let suite = Suite {
            dir: "apps/web/e2e".into(),
            runner: "playwright".into(),
        };
        let mut c = changed();
        c.push("apps/web/e2e/tests/signup.spec.ts".into());
        let how = how_to_check(&c, Some(&suite), "npx vitest run");
        assert!(
            how.contains("cd apps/web/e2e && npx playwright test tests/signup.spec.ts"),
            "{how}"
        );
        assert!(how.contains("npx vitest run apps/web/src/test/lib/validation-utils.test.ts"));
        assert_ne!(how.trim(), "npm test");
    }

    #[test]
    fn the_unit_runner_is_read_from_the_tree() {
        assert_eq!(
            unit_cmd(&s(&["apps/web/vitest.config.ts"])),
            "npx vitest run"
        );
        assert_eq!(unit_cmd(&s(&["jest.config.js"])), "npx jest");
        assert_eq!(unit_cmd(&s(&["README.md"])), "");
    }

    #[test]
    fn globs_read_as_github_reads_them() {
        assert!(glob("apps/web/**", "apps/web/src/a.ts"));
        assert!(glob("**/*.md", "docs/a/b.md"));
        assert!(glob("**/*.md", "b.md"));
        assert!(!glob("apps/*", "apps/web/src/a.ts"));
        assert!(glob("main", "main"));
        assert!(glob("release/*", "release/1"));
    }
}
