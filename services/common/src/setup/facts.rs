//! What a repo shows, and the detectors that read settings from it.
//!
//! The worker fills [`RepoFacts`] from GitHub (bounded reads); everything
//! here is pure, so each detector is tested on fixture repos. The readers
//! are the existing ones: `e2e::ci_setup` (test runner and test job),
//! `e2e::workflow_gate` (labels workflows run on, deploy labels),
//! `e2e::check_gate` (required checks), `deploy_stats` (deploy workflows)
//! and `stale_prs` (what the stale cleanup would do).

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::Evidence;
use crate::e2e::{ci_setup, workflow_gate};
use crate::stale_prs::{self as stale, Exemption, Facts as StaleFacts, Plan, StaleConfig};

/// Files read whole when the repo has them (each cut to [`MAX_FILE_CHARS`]).
pub const READ_FILES: &[&str] = &[
    "package.json",
    "Cargo.toml",
    "pyproject.toml",
    "go.mod",
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    ".github/CONTRIBUTING.md",
    "CODE_REVIEW_GUIDELINES.md",
    "docs/CODE_REVIEW_GUIDELINES.md",
    ".github/pull_request_template.md",
    ".github/PULL_REQUEST_TEMPLATE.md",
    "pull_request_template.md",
    "docs/pull_request_template.md",
    ".github/CODEOWNERS",
    "CODEOWNERS",
    "docs/CODEOWNERS",
];

pub const MAX_FILE_CHARS: usize = 20_000;
/// Workflow files read.
pub const MAX_WORKFLOWS: usize = 25;
/// Merged pull requests summarized.
pub const MAX_MERGED: usize = 50;
/// Open pull requests whose timelines are read for the stale effect.
pub const MAX_STALE_READS: usize = 40;
/// The window the stale cleanup's effect is shown over.
pub const STALE_WINDOW_DAYS: i64 = 14;

/// One merged pull request, as the AI read and the detectors see it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PrDigest {
    pub number: u64,
    pub title: String,
    pub url: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub labels: Vec<String>,
    /// Files it touched (read for the newest ones only).
    #[serde(default)]
    pub files: Vec<String>,
    /// Its files were read.
    #[serde(default)]
    pub files_read: bool,
    /// Review comments by people (the newest ones only).
    #[serde(default)]
    pub review_comments: Vec<String>,
    /// Hours from opened to merged.
    #[serde(default)]
    pub hours_to_merge: Option<f64>,
    /// When it merged (RFC 3339).
    #[serde(default)]
    pub merged_at: Option<String>,
}

impl PrDigest {
    /// It added or changed tests.
    pub fn tests_changed(&self) -> bool {
        self.files.iter().any(|f| is_test_path(f))
    }
}

/// Everything Setup read from one repo.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RepoFacts {
    pub repo: String,
    pub default_branch: String,
    /// `https://github.com/<owner>/<name>`.
    pub html_url: String,
    /// Every path on the default branch (cut at a few thousand).
    pub paths: Vec<String>,
    /// [`READ_FILES`] the repo has, by path.
    pub files: BTreeMap<String, String>,
    /// `.github/workflows/*` as (path, text).
    pub workflows: Vec<(String, String)>,
    pub labels: Vec<String>,
    /// Checks branch protection or rules require on the default branch.
    pub required_checks: Vec<String>,
    /// Branch protection could be read (an app without administration read
    /// sees none).
    pub protection_read: bool,
    pub allow_squash: Option<bool>,
    pub allow_merge: Option<bool>,
    pub allow_rebase: Option<bool>,
    /// The newest tags, newest first.
    pub tags: Vec<String>,
    /// The last merged pull requests, newest first.
    pub merged: Vec<PrDigest>,
    /// Open pull requests and their timelines (oldest first), for the stale
    /// cleanup's effect. Not sent to the AI read.
    #[serde(default, skip_serializing)]
    pub open: Vec<(Value, Vec<Value>)>,
    /// The open pull requests whose timelines weren't read.
    #[serde(default)]
    pub open_not_read: usize,
    /// Log groups the Logs page saved this repo for (their code lives here).
    /// Not sent to the AI read.
    #[serde(default, skip_serializing)]
    pub log_groups: Vec<String>,
}

impl RepoFacts {
    pub fn file(&self, path: &str) -> Option<&str> {
        self.files.get(path).map(String::as_str)
    }

    pub fn has_path(&self, path: &str) -> bool {
        self.paths.iter().any(|p| p == path) || self.files.contains_key(path)
    }

    fn first_file<'a>(&self, paths: &[&'a str]) -> Option<&'a str> {
        paths.iter().copied().find(|p| self.has_path(p))
    }

    fn blob(&self, path: &str) -> Option<String> {
        (!self.html_url.is_empty()).then(|| {
            format!(
                "{}/blob/{}/{path}",
                self.html_url,
                if self.default_branch.is_empty() {
                    "HEAD"
                } else {
                    &self.default_branch
                }
            )
        })
    }

    fn evidence_file(&self, text: String, path: &str) -> Evidence {
        Evidence {
            text,
            file: Some(path.to_string()),
            line: None,
            url: self.blob(path),
        }
    }

    /// How many of the summarized merged pull requests carried `label`.
    pub fn label_uses(&self, label: &str) -> usize {
        self.merged
            .iter()
            .filter(|p| p.labels.iter().any(|l| l.eq_ignore_ascii_case(label)))
            .count()
    }

    /// The summarized pull requests merged in the last `days`.
    pub fn merged_within(&self, now: DateTime<Utc>, days: i64) -> usize {
        self.merged
            .iter()
            .filter_map(|p| p.merged_at.as_deref())
            .filter_map(|t| DateTime::parse_from_rfc3339(t).ok())
            .filter(|t| now - t.with_timezone(&Utc) <= Duration::days(days))
            .count()
    }

    /// The median hours to merge, when enough pull requests say.
    pub fn median_hours_to_merge(&self) -> Option<f64> {
        let mut h: Vec<f64> = self
            .merged
            .iter()
            .filter_map(|p| p.hours_to_merge)
            .collect();
        if h.len() < 3 {
            return None;
        }
        h.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(h[h.len() / 2])
    }

    fn has_label(&self, name: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|l| l.eq_ignore_ascii_case(name))
            .map(String::as_str)
    }
}

pub fn is_test_path(p: &str) -> bool {
    let l = p.to_ascii_lowercase();
    l.contains("__tests__")
        || l.starts_with("test/")
        || l.starts_with("tests/")
        || l.contains("/test/")
        || l.contains("/tests/")
        || l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("_test.")
        || l.ends_with("_test.go")
        || l.contains("/e2e/")
        || l.starts_with("e2e/")
        || l.rsplit('/').next().is_some_and(|f| f.starts_with("test_"))
}

/// A detected value and why.
#[derive(Debug, Clone, PartialEq)]
pub struct Found {
    pub value: Value,
    pub evidence: Vec<Evidence>,
}

fn found(value: Value, evidence: Vec<Evidence>) -> Found {
    Found { value, evidence }
}

const LOCKFILES: &[(&str, &str)] = &[
    ("package-lock.json", "npm lockfile"),
    ("yarn.lock", "Yarn lockfile"),
    ("pnpm-lock.yaml", "pnpm lockfile"),
    ("Cargo.lock", "Cargo lockfile"),
    ("go.sum", "Go module checksums"),
    ("poetry.lock", "Poetry lockfile"),
    ("uv.lock", "uv lockfile"),
    ("Pipfile.lock", "Pipenv lockfile"),
    ("requirements.txt", "pip requirements"),
    ("Gemfile.lock", "Bundler lockfile"),
    ("composer.lock", "Composer lockfile"),
    ("pom.xml", "Maven project"),
    ("build.gradle", "Gradle build"),
    ("build.gradle.kts", "Gradle build"),
];

/// The test runner the repo uses, and the file that says so.
fn test_runner(f: &RepoFacts) -> Option<(String, String)> {
    if let Some(pkg) = f.file("package.json") {
        if let Some(r) = ci_setup::runner_from_package(pkg) {
            return Some((
                format!("package.json depends on {}", runner_name(r)),
                "package.json".into(),
            ));
        }
        let scripts = ci_setup::package_scripts(Some(pkg));
        if let Some(t) = scripts.get("test").and_then(|v| v.as_str()) {
            if !t.contains("no test specified") {
                return Some((
                    format!("package.json runs tests with `{t}`"),
                    "package.json".into(),
                ));
            }
        }
    }
    if f.has_path("Cargo.toml")
        && f.paths
            .iter()
            .any(|p| p.starts_with("tests/") || p.contains("/tests/"))
    {
        return Some((
            "Rust tests in a tests folder, run by cargo test".into(),
            "Cargo.toml".into(),
        ));
    }
    if let Some(p) = f.file("pyproject.toml") {
        if p.contains("pytest") {
            return Some((
                "pyproject.toml configures pytest".into(),
                "pyproject.toml".into(),
            ));
        }
    }
    if f.has_path("go.mod") {
        if let Some(t) = f.paths.iter().find(|p| p.ends_with("_test.go")) {
            return Some((format!("Go tests such as {t}"), t.clone()));
        }
    }
    None
}

fn runner_name(r: ci_setup::Runner) -> &'static str {
    match r {
        ci_setup::Runner::Playwright => "Playwright",
        ci_setup::Runner::Jest => "Jest",
        ci_setup::Runner::Vitest => "Vitest",
        ci_setup::Runner::Cypress => "Cypress",
        ci_setup::Runner::Junit => "a JUnit-style runner",
    }
}

/// The labels workflows run on that the reviewer may add: they exist in the
/// repo and none deploys (`workflow_gate::is_deploy_label`). With the
/// workflow that asks for each.
pub fn ci_labels(f: &RepoFacts) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for (path, text) in &f.workflows {
        let gate = workflow_gate::read(text);
        for pattern in gate.labels() {
            let Some(label) = workflow_gate::resolve(&pattern, &f.labels, &[]) else {
                continue;
            };
            if workflow_gate::is_deploy_label(&label)
                || out.iter().any(|(l, _)| l.eq_ignore_ascii_case(&label))
            {
                continue;
            }
            out.push((label, path.clone()));
        }
    }
    out
}

/// Deploy workflows (by name, `deploy_stats::says_deploy`), for evidence.
pub fn deploy_workflows(f: &RepoFacts) -> Vec<String> {
    f.workflows
        .iter()
        .filter(|(p, _)| {
            let name = p.rsplit('/').next().unwrap_or(p);
            crate::deploy_stats::says_deploy(name)
        })
        .map(|(p, _)| p.clone())
        .collect()
}

/// The tag style the repo's own tags use, when most of them agree.
pub fn tag_style(tags: &[String]) -> Option<(&'static str, String, String)> {
    let semver = regex::Regex::new(r"^([A-Za-z\-_/]*)\d+\.\d+\.\d+").ok()?;
    let date = regex::Regex::new(r"^([A-Za-z\-_/]*)\d{4}[.\-]\d{2}[.\-]\d{2}").ok()?;
    let recent: Vec<&String> = tags.iter().take(20).collect();
    if recent.len() < 3 {
        return None;
    }
    let mut counts: HashMap<(&'static str, String), usize> = HashMap::new();
    for t in &recent {
        if let Some(c) = date.captures(t) {
            *counts.entry(("date", c[1].to_string())).or_default() += 1;
        } else if let Some(c) = semver.captures(t) {
            *counts.entry(("semver", c[1].to_string())).or_default() += 1;
        }
    }
    let ((mode, prefix), n) = counts.into_iter().max_by_key(|(k, n)| (*n, k.1.len()))?;
    if n * 10 < recent.len() * 6 {
        return None;
    }
    let example = recent
        .iter()
        .find(|t| t.starts_with(&prefix))
        .map(|t| t.to_string())
        .unwrap_or_default();
    Some((
        mode,
        prefix,
        format!("{n} of the last {} tags look like {example}", recent.len()),
    ))
}

/// What the detectors read, by setting id.
pub fn detect(f: &RepoFacts) -> HashMap<&'static str, Found> {
    let mut out: HashMap<&'static str, Found> = HashMap::new();
    let total = f.merged.len();

    // Code review: the review label, and the review flow the repo has.
    if let Some(l) = f.has_label(crate::review_config::DEFAULT_LABEL) {
        let used = f.label_uses(l);
        let ev = Evidence::text(format!(
            "Label {l} exists, used on {used} of the last {total} merged pull requests"
        ));
        out.insert("reviewer.label", found(json!(l), vec![ev.clone()]));
        out.insert("reviewer.enabled", found(json!(true), vec![ev]));
    } else if let Some(p) = f.first_file(&[".github/CODEOWNERS", "CODEOWNERS", "docs/CODEOWNERS"]) {
        out.insert(
            "reviewer.enabled",
            found(
                json!(true),
                vec![f.evidence_file("CODEOWNERS asks for reviews on this repo".into(), p)],
            ),
        );
    } else if let Some(p) = f.first_file(&[
        ".github/pull_request_template.md",
        ".github/PULL_REQUEST_TEMPLATE.md",
        "pull_request_template.md",
        "docs/pull_request_template.md",
    ]) {
        out.insert(
            "reviewer.enabled",
            found(
                json!(true),
                vec![f.evidence_file(
                    "A pull request template sets what a review checks".into(),
                    p,
                )],
            ),
        );
    }

    // Tests: the runner, and the workflow that runs them.
    if let Some((text, file)) = test_runner(f) {
        out.insert(
            "tests.verify_tests",
            found(json!(true), vec![f.evidence_file(text, &file)]),
        );
    }
    if !f.workflows.is_empty() {
        let scripts = ci_setup::package_scripts(f.file("package.json"));
        match ci_setup::pick_workflow(&f.workflows, &scripts) {
            Some((path, wf)) => {
                let job = wf.job.job_name.clone().unwrap_or(wf.job.job_id.clone());
                out.insert(
                    "tests.e2e",
                    found(
                        json!(true),
                        vec![f.evidence_file(
                            format!(
                                "Workflow {path} runs {} tests in job {job}",
                                runner_name(wf.job.runner)
                            ),
                            &path,
                        )],
                    ),
                );
            }
            None => {
                out.insert(
                    "tests.e2e",
                    found(
                        json!(false),
                        vec![Evidence::text(format!(
                            "None of the {} workflows runs a test job",
                            f.workflows.len()
                        ))],
                    ),
                );
            }
        }
    }

    // CI labels the workflows run on.
    let labels = ci_labels(f);
    if !labels.is_empty() {
        let ev: Vec<Evidence> = labels
            .iter()
            .take(5)
            .map(|(l, p)| f.evidence_file(format!("Workflow {p} runs on label {l}"), p))
            .collect();
        out.insert("ci_labels.enabled", found(json!(true), ev.clone()));
        out.insert(
            "ci_labels.allow",
            found(
                json!(labels
                    .iter()
                    .map(|(l, _)| l.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")),
                ev,
            ),
        );
    }

    // Auto-merge: the merge method the repo allows, and whether branch
    // protection requires checks (auto-merge needs them).
    let allowed: Vec<&str> = [
        ("squash", f.allow_squash),
        ("merge", f.allow_merge),
        ("rebase", f.allow_rebase),
    ]
    .iter()
    .filter(|(_, a)| *a == Some(true))
    .map(|(m, _)| *m)
    .collect();
    if let Some(m) = allowed.first() {
        out.insert(
            "auto_merge.merge_method",
            found(
                json!(m),
                vec![Evidence::text(format!(
                    "This repo allows {} merges",
                    allowed.join(", ")
                ))],
            ),
        );
    }
    if f.protection_read && f.required_checks.is_empty() {
        out.insert(
            "auto_merge.enabled",
            found(
                json!(false),
                vec![Evidence::text(format!(
                    "Branch protection on {} requires no checks, so nothing would hold a merge back",
                    f.default_branch
                ))],
            ),
        );
    }

    // Release tags in the repo's own style.
    if let Some((mode, prefix, why)) = tag_style(&f.tags) {
        out.insert(
            "tags.tag_mode",
            found(json!(mode), vec![Evidence::text(why.clone())]),
        );
        out.insert(
            "tags.tag_prefix",
            found(json!(prefix), vec![Evidence::text(why)]),
        );
    }

    // Security fixes need a dependency manifest.
    let locks: Vec<Evidence> = LOCKFILES
        .iter()
        .filter(|(p, _)| f.has_path(p))
        .map(|(p, what)| f.evidence_file(format!("{what} {p}"), p))
        .collect();
    if !locks.is_empty() {
        out.insert("security.enabled", found(json!(true), locks));
    } else if !f.paths.is_empty() {
        out.insert(
            "security.enabled",
            found(
                json!(false),
                vec![Evidence::text("No dependency lockfile or manifest found")],
            ),
        );
    }
    out
}

/// What the stale cleanup would have done over the last `days`, with `cfg`:
/// (marked, closed). Each open pull request's quiet time comes from
/// `stale_prs::last_activity`, its exemptions from `stale_prs::Exemption`
/// and each step from `stale_prs::decide`, as the sweep does.
pub fn stale_effect(
    cfg: &StaleConfig,
    open: &[(Value, Vec<Value>)],
    now: DateTime<Utc>,
    days: i64,
) -> (usize, usize) {
    let start = now - Duration::days(days);
    let (mut marked, mut closed) = (0, 0);
    for (pr, timeline) in open {
        let labels: Vec<String> = pr["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| l["name"].as_str().map(String::from))
            .collect();
        let exempt = Exemption {
            cfg,
            draft: pr["draft"].as_bool().unwrap_or(false),
            labels: &labels,
            author: pr["user"]["login"].as_str().unwrap_or(""),
            kept_open_by: None,
            held: None,
        }
        .reason();
        if exempt.is_some() {
            continue;
        }
        let last = stale::last_activity(pr, timeline, &cfg.label);
        let mark_at = last.at + Duration::days(cfg.stale_days as i64);
        if mark_at > now {
            continue;
        }
        let at_mark = StaleFacts {
            now: mark_at,
            last: last.clone(),
            marked: None,
            exempt: None,
        };
        if stale::decide(cfg, &at_mark) != Plan::Mark {
            continue;
        }
        if mark_at >= start {
            marked += 1;
        }
        let close_at = mark_at + Duration::days(cfg.close_days as i64);
        let at_close = StaleFacts {
            now: close_at.min(now),
            last,
            marked: Some((mark_at, true)),
            exempt: None,
        };
        if close_at >= start && stale::decide(cfg, &at_close) == Plan::Close {
            closed += 1;
        }
    }
    (marked, closed)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A web app: Playwright, an E2E workflow gated on a label, a deploy
    /// label, branch protection with checks, squash only, v-tags.
    pub fn web_app() -> RepoFacts {
        let workflow = r#"
name: e2e
on:
  pull_request:
    types: [opened, synchronize, labeled]
jobs:
  e2e:
    if: contains(github.event.pull_request.labels.*.name, 'E2E:IOS')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npx playwright test
"#;
        let deploy = r#"
name: deploy
on:
  pull_request:
    types: [labeled]
jobs:
  deploy:
    if: contains(github.event.pull_request.labels.*.name, 'deploy-staging')
    runs-on: ubuntu-latest
    steps:
      - run: ./deploy.sh
"#;
        RepoFacts {
            repo: "acme/web".into(),
            default_branch: "main".into(),
            html_url: "https://github.com/acme/web".into(),
            paths: vec![
                "package.json".into(),
                "package-lock.json".into(),
                ".github/CODEOWNERS".into(),
                ".github/workflows/e2e.yml".into(),
                ".github/workflows/deploy.yml".into(),
                "src/__tests__/app.test.ts".into(),
            ],
            files: BTreeMap::from([
                (
                    "package.json".into(),
                    r#"{"scripts":{"test":"playwright test"},"devDependencies":{"@playwright/test":"1.0.0"}}"#.into(),
                ),
                (".github/CODEOWNERS".into(), "* @acme/web".into()),
            ]),
            workflows: vec![
                (".github/workflows/e2e.yml".into(), workflow.into()),
                (".github/workflows/deploy.yml".into(), deploy.into()),
            ],
            labels: vec!["ch-review".into(), "E2E:IOS".into(), "deploy-staging".into()],
            required_checks: vec!["build".into(), "e2e".into()],
            protection_read: true,
            allow_squash: Some(true),
            allow_merge: Some(false),
            allow_rebase: Some(false),
            tags: vec!["v1.4.0".into(), "v1.3.2".into(), "v1.3.1".into(), "v1.3.0".into()],
            merged: (1..=6)
                .map(|n| PrDigest {
                    number: n,
                    title: format!("Change {n}"),
                    url: format!("https://github.com/acme/web/pull/{n}"),
                    labels: if n % 2 == 0 { vec!["ch-review".into()] } else { vec![] },
                    files: vec!["src/app.ts".into(), "src/__tests__/app.test.ts".into()],
                    files_read: true,
                    hours_to_merge: Some(n as f64 * 10.0),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// A Rust service: cargo tests, no workflows read, no branch protection
    /// checks, date tags, no review label.
    pub fn rust_service() -> RepoFacts {
        RepoFacts {
            repo: "acme/api".into(),
            default_branch: "main".into(),
            html_url: "https://github.com/acme/api".into(),
            paths: vec![
                "Cargo.toml".into(),
                "Cargo.lock".into(),
                "src/main.rs".into(),
                "tests/api.rs".into(),
            ],
            files: BTreeMap::from([("Cargo.toml".into(), "[package]\nname = \"api\"".into())]),
            protection_read: true,
            allow_squash: Some(true),
            allow_merge: Some(true),
            allow_rebase: Some(true),
            tags: vec![
                "2026.11.01".into(),
                "2026.10.20".into(),
                "2026.10.01".into(),
            ],
            ..Default::default()
        }
    }

    /// A docs repo: nothing to detect.
    pub fn docs_only() -> RepoFacts {
        RepoFacts {
            repo: "acme/docs".into(),
            default_branch: "main".into(),
            paths: vec!["README.md".into(), "guide/intro.md".into()],
            ..Default::default()
        }
    }

    #[test]
    fn the_web_app_detectors_read_its_files() {
        let d = detect(&web_app());
        assert_eq!(d["reviewer.label"].value, "ch-review");
        assert!(d["reviewer.enabled"].evidence[0]
            .text
            .contains("used on 3 of the last 6"));
        assert_eq!(d["tests.verify_tests"].value, true);
        assert!(d["tests.verify_tests"].evidence[0]
            .text
            .contains("Playwright"));
        assert_eq!(d["tests.e2e"].value, true);
        assert_eq!(
            d["tests.e2e"].evidence[0].file.as_deref(),
            Some(".github/workflows/e2e.yml")
        );
        // The E2E label is allowed; the deploy label never is.
        assert_eq!(d["ci_labels.allow"].value, "E2E:IOS");
        assert_eq!(d["ci_labels.enabled"].value, true);
        assert_eq!(d["auto_merge.merge_method"].value, "squash");
        // Checks are required, so auto-merge is left to the AI read.
        assert!(!d.contains_key("auto_merge.enabled"));
        assert_eq!(d["tags.tag_mode"].value, "semver");
        assert_eq!(d["tags.tag_prefix"].value, "v");
        assert_eq!(d["security.enabled"].value, true);
        assert!(d["security.enabled"].evidence[0]
            .text
            .contains("npm lockfile"));
        assert_eq!(
            d["security.enabled"].evidence[0].url.as_deref(),
            Some("https://github.com/acme/web/blob/main/package-lock.json")
        );
        assert_eq!(
            deploy_workflows(&web_app()),
            vec![".github/workflows/deploy.yml"]
        );
    }

    #[test]
    fn the_rust_service_detectors_read_its_files() {
        let d = detect(&rust_service());
        assert!(!d.contains_key("reviewer.label"));
        assert!(d["tests.verify_tests"].evidence[0]
            .text
            .contains("cargo test"));
        // No checks required: auto-merge is not recommended, and says why.
        assert_eq!(d["auto_merge.enabled"].value, false);
        assert!(d["auto_merge.enabled"].evidence[0]
            .text
            .contains("requires no checks"));
        assert_eq!(d["auto_merge.merge_method"].value, "squash");
        assert_eq!(d["tags.tag_mode"].value, "date");
        assert_eq!(d["tags.tag_prefix"].value, "");
        assert!(d["security.enabled"].evidence[0]
            .text
            .contains("Cargo lockfile"));
    }

    #[test]
    fn the_docs_repo_detects_almost_nothing() {
        let d = detect(&docs_only());
        assert_eq!(
            d.keys().copied().collect::<Vec<_>>(),
            vec!["security.enabled"]
        );
        assert_eq!(d["security.enabled"].value, false);
    }

    #[test]
    fn tag_styles_need_most_tags_to_agree() {
        let mixed: Vec<String> = ["v1.0.0", "release-a", "foo", "bar"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(tag_style(&mixed), None);
        assert_eq!(tag_style(&["v1.0.0".to_string()]), None);
    }

    fn pr(
        n: u64,
        created_days_ago: i64,
        now: DateTime<Utc>,
        labels: &[&str],
        draft: bool,
    ) -> Value {
        json!({
            "number": n,
            "created_at": (now - Duration::days(created_days_ago)).to_rfc3339(),
            "draft": draft,
            "user": {"login": "dev"},
            "labels": labels.iter().map(|l| json!({"name": l})).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn the_stale_effect_counts_marks_and_closes_in_the_window() {
        let now = Utc::now();
        let cfg = StaleConfig {
            stale_days: 21,
            ..StaleConfig::default()
        }; // 21 days, then 7
        let open = vec![
            // Quiet 25 days: marked 4 days ago, not closed yet.
            (pr(1, 25, now, &[], false), vec![]),
            // Quiet 30 days: marked 9 days ago, closed 2 days ago.
            (pr(2, 30, now, &[], false), vec![]),
            // Quiet 60 days: marked and closed long before the window.
            (pr(3, 60, now, &[], false), vec![]),
            // Active.
            (pr(4, 3, now, &[], false), vec![]),
            // Exempt: a draft, and a kept label.
            (pr(5, 40, now, &[], true), vec![]),
            (pr(6, 40, now, &["pinned"], false), vec![]),
            // A comment 2 days ago makes it active.
            (
                pr(7, 40, now, &[], false),
                vec![
                    json!({"event": "commented", "actor": {"login": "ana", "type": "User"},
                            "user": {"login": "ana", "type": "User"},
                            "created_at": (now - Duration::days(2)).to_rfc3339()}),
                ],
            ),
        ];
        assert_eq!(stale_effect(&cfg, &open, now, 14), (2, 1));
    }
}
