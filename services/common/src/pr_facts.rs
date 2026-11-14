//! What is recorded about a pull request at one head commit.
//!
//! A reviewer of a dependency pull request found its code sound and its
//! description wrong in five places: a package said to be "not moved on the
//! head" that the lockfile resolves at the patched version, an open item
//! saying the lockfile was out of sync when a frozen install passes, a fixed
//! advisory listed as not fixed, a wrong count of high advisories left, and
//! a package still listed as open. A sentence said "contract unchanged" with
//! nothing behind it, and no major version jump said why it was safe.
//!
//! A description is a view of these facts and of nothing else:
//!
//! - [`Facts`] is what the pipeline recorded for the head: what changed (the
//!   diff), why (the ticket or finding), what was verified and how (check
//!   results with their links), what was not and why, the behaviour changes
//!   known, the risk and the way back, what is open and who can close it,
//!   and for a dependency update [`DependencyFacts`].
//! - [`lead`] renders what a reviewer reads first from them, in a fixed
//!   order: one standalone line, what is to be decided, the feedback wanted,
//!   and the anomalies, each with its evidence.
//! - `pr_claims` checks every sentence somebody wrote against them, and
//!   `pr_layout` puts the routine detail after them, collapsed.
//!
//! Everything here is pure. The worker collects the facts
//! (`worker::passes::description`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The lead's markers: CoderHelm's own section, rebuilt on every build.
pub const LEAD_START: &str = "<!-- coderhelm:lead -->";
pub const LEAD_END: &str = "<!-- /coderhelm:lead -->";

/// One file of the diff.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChange {
    pub path: String,
    /// `added`, `modified`, `removed`, `renamed`.
    pub status: String,
    /// The path it had before a rename.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub previous: String,
    /// Lines added and removed.
    #[serde(default)]
    pub changed: usize,
}

/// How a recorded check ended on the head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Passed,
    Failed,
    Running,
    /// Skipped, neutral or cancelled: it reported and proved nothing.
    DidNotRun,
}

/// One recorded check result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckFact {
    pub name: String,
    pub outcome: Outcome,
    /// Where the recorded result is (empty when it has no page).
    #[serde(default)]
    pub url: String,
}

/// What a check verifies, read from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    Tests,
    Lint,
    Types,
    Build,
    Install,
}

impl Family {
    pub const ALL: [Family; 5] = [
        Family::Tests,
        Family::Lint,
        Family::Types,
        Family::Build,
        Family::Install,
    ];

    /// The words that name it, in a check's name or a sentence.
    pub fn words(self) -> &'static [&'static str] {
        match self {
            Family::Tests => &[
                "test",
                "tests",
                "suite",
                "spec",
                "specs",
                "jest",
                "pytest",
                "vitest",
                "e2e",
                "rspec",
                "unit",
                "integration",
            ],
            Family::Lint => &[
                "lint", "linter", "linting", "eslint", "clippy", "ruff", "rubocop", "fmt",
                "format", "prettier",
            ],
            Family::Types => &["typecheck", "types", "tsc", "mypy", "pyright", "typescript"],
            Family::Build => &["build", "compile", "bundle", "webpack", "docker"],
            Family::Install => &["install", "lockfile", "frozen", "dependencies"],
        }
    }

    /// The families a name or phrase names.
    pub fn of(text: &str) -> Vec<Family> {
        let lower = text.to_lowercase();
        let lower = lower
            .replace("type check", "typecheck")
            .replace("type-check", "typecheck");
        let words: Vec<&str> = lower
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect();
        Family::ALL
            .into_iter()
            .filter(|f| f.words().iter().any(|w| words.contains(w)))
            .collect()
    }
}

/// Something nothing verified, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unverified {
    pub what: String,
    pub why: String,
}

/// A statement with what it rests on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidenced {
    pub what: String,
    /// The record it rests on: a check and its result, a probe's before and
    /// after, a file and line.
    pub evidence: String,
}

/// Who can close an open item.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Closer {
    /// The pull request's CI, when it passes on the head.
    Ci,
    /// CoderHelm, on its next step.
    CoderHelm,
    /// A person.
    #[default]
    Person,
}

impl Closer {
    pub fn words(self) -> &'static str {
        match self {
            Closer::Ci => "CI settles it",
            Closer::CoderHelm => "CoderHelm settles it",
            Closer::Person => "needs a person",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenItem {
    pub text: String,
    pub closer: Closer,
}

/// Why the change exists.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Why {
    /// The ticket or finding as people write it.
    pub reference: String,
    pub title: String,
    #[serde(default)]
    pub url: String,
}

/// The risk the diff makes, and where a mistake costs most.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Risk {
    /// `Low`, `Medium`, `High`.
    pub level: String,
    pub why: String,
    /// The file to read first, and why.
    #[serde(default)]
    pub look_first: Option<(String, String)>,
    /// How to take the change back, and what a revert does not undo.
    #[serde(default)]
    pub rollback: String,
}

// ── Dependency updates ─────────────────────────────────────────────────

/// One package and version line, as the lockfile on the head resolves it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepLine {
    pub package: String,
    pub from: String,
    /// What the head resolves (empty when the line could not be read).
    pub to: String,
    /// The head resolves another version than the base.
    pub moved: bool,
    /// The move crosses a major version.
    pub major: bool,
    /// Advisories on this line the head still has.
    #[serde(default)]
    pub remaining: Vec<String>,
    /// Advisories on this line the head fixes.
    #[serde(default)]
    pub fixed: Vec<String>,
}

/// Packages that are released together and move together.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FamilyMove {
    pub name: String,
    pub members: Vec<String>,
    /// Every member is on the same line on the head.
    pub together: bool,
}

/// Why a major version jump is safe here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MajorEvidence {
    pub package: String,
    pub from: String,
    pub to: String,
    /// Each a recorded reason: the breaking changes read and where the code
    /// uses them, the checks that ran on the new version, a probe.
    pub evidence: Vec<String>,
}

/// One behaviour measured before and after.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Probe {
    pub what: String,
    pub before: String,
    pub after: String,
}

impl Probe {
    pub fn same(&self) -> bool {
        self.before == self.after
    }
}

/// The advisories a dependency update is about, reconciled with the head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Advisories {
    pub fixed: Vec<String>,
    pub remaining: Vec<String>,
    /// Advisories the new versions bring that the base did not have.
    #[serde(default)]
    pub introduced: Vec<String>,
    /// How many remain per severity word (`critical`, `high`, ...).
    #[serde(default)]
    pub remaining_by_severity: BTreeMap<String, usize>,
}

/// What a dependency update's description is rendered from. The fields past
/// `lines` and `advisories` come from the dependency modules when they are
/// there (families, a verification set of its own, safety evidence per
/// major, probes); each is empty or `None` when nothing recorded it, and is
/// then said as not known, never guessed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyFacts {
    pub lines: Vec<DepLine>,
    pub advisories: Advisories,
    #[serde(default)]
    pub families: Vec<FamilyMove>,
    #[serde(default)]
    pub majors: Vec<MajorEvidence>,
    #[serde(default)]
    pub probes: Vec<Probe>,
    /// Whether a frozen install of the head's lockfile passes, when a check
    /// recorded it: `(passes, the check that says so)`.
    #[serde(default)]
    pub frozen_install: Option<(bool, String)>,
    /// Lines whose resolution on the head could not be read, and why.
    #[serde(default)]
    pub unread: Vec<Unverified>,
    /// The package manager's ecosystem (`npm`), when one names them all.
    #[serde(default)]
    pub ecosystem: String,
}

impl DependencyFacts {
    pub fn line(&self, package: &str) -> Option<&DepLine> {
        self.lines
            .iter()
            .find(|l| l.package.eq_ignore_ascii_case(package))
    }

    pub fn moved(&self) -> usize {
        self.lines.iter().filter(|l| l.moved).count()
    }

    pub fn major_lines(&self) -> Vec<&DepLine> {
        self.lines.iter().filter(|l| l.moved && l.major).collect()
    }

    /// The recorded evidence for a line's major jump.
    pub fn evidence_for(&self, line: &DepLine) -> Option<&MajorEvidence> {
        self.majors
            .iter()
            .find(|m| m.package.eq_ignore_ascii_case(&line.package) && !m.evidence.is_empty())
    }
}

// ── The facts of one head ──────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facts {
    /// The head commit these are of.
    pub head: String,
    /// One standalone line: what the pull request does.
    pub summary: String,
    pub why: Option<Why>,
    /// The diff's files. `files_complete` is false when GitHub cut the list.
    pub files: Vec<FileChange>,
    pub files_complete: bool,
    /// Recorded check results on the head (CI and CoderHelm's own runs).
    pub checks: Vec<CheckFact>,
    /// Whether the head's checks could be read at all.
    pub checks_read: bool,
    pub unverified: Vec<Unverified>,
    /// Behaviour the change is known to alter, each with its evidence.
    pub behaviour: Vec<Evidenced>,
    /// Things recorded as the same before and after, each with its evidence.
    pub unchanged: Vec<Evidenced>,
    pub risk: Option<Risk>,
    pub open: Vec<OpenItem>,
    pub dependency: Option<DependencyFacts>,
    /// The run's page, for what a description leaves out.
    #[serde(default)]
    pub run_url: String,
    /// Things said once on the pull request (a key that could not be made).
    #[serde(default)]
    pub notes: Vec<String>,
    /// The one command or step that checks the change, when one is known.
    #[serde(default)]
    pub how_to_check: String,
    /// The work outside the diff, each step with one owner and one state.
    #[serde(default)]
    pub steps: Vec<crate::pr_steps::Step>,
    /// Checks CoderHelm made itself, each with its result.
    #[serde(default)]
    pub checked: Vec<crate::pr_steps::Checked>,
    /// Raw evidence (queries, ids, responses): shown collapsed, never in
    /// the part a reader sees first.
    #[serde(default)]
    pub evidence: Vec<String>,
    /// The repository's address (`https://github.com/org/repo`), for links
    /// to files at the head.
    #[serde(default)]
    pub repo_url: String,
    /// What a person must do, from the gate that decides what reaches one.
    /// Empty: derived from the steps and open items.
    #[serde(default)]
    pub needed: Vec<crate::pr_body::HumanItem>,
    /// Where a UI change can be seen.
    #[serde(default)]
    pub preview_url: String,
    /// A UI change's screenshots, or why there are none.
    #[serde(default)]
    pub screens: Option<crate::pr_body::Screens>,
    /// The feedback wanted, when something more specific than the records
    /// says it.
    #[serde(default)]
    pub review_question: String,
}

impl Facts {
    pub fn short_head(&self) -> &str {
        self.head.get(..7).unwrap_or(&self.head)
    }

    /// Whether `path` (as somebody wrote it) is one of the diff's files.
    pub fn has_file(&self, path: &str) -> bool {
        let p = path.trim().trim_matches('/');
        if p.is_empty() {
            return false;
        }
        self.files.iter().any(|f| {
            [f.path.as_str(), f.previous.as_str()]
                .into_iter()
                .filter(|x| !x.is_empty())
                .any(|x| {
                    x == p
                        || x.ends_with(&format!("/{p}"))
                        || p.ends_with(&format!("/{x}"))
                        || x.starts_with(&format!("{p}/"))
                        || (!p.contains('/') && x.rsplit('/').next() == Some(p))
                })
        })
    }

    /// The families a passed check on the head covers.
    pub fn passed_families(&self) -> Vec<Family> {
        let mut out: Vec<Family> = self
            .checks
            .iter()
            .filter(|c| c.outcome == Outcome::Passed)
            .flat_map(|c| Family::of(&c.name))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Every check that reported on the head passed, and at least one did.
    pub fn all_checks_passed(&self) -> bool {
        let reported: Vec<&CheckFact> = self
            .checks
            .iter()
            .filter(|c| c.outcome != Outcome::DidNotRun)
            .collect();
        !reported.is_empty() && reported.iter().all(|c| c.outcome == Outcome::Passed)
    }
}

/// One thing a reviewer should look at before the routine detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anomaly {
    pub what: String,
    pub evidence: String,
}

fn linked(c: &CheckFact) -> String {
    let name = c.name.replace('`', "'");
    if c.url.starts_with("https://") {
        format!("[`{name}`]({})", c.url)
    } else {
        format!("`{name}`")
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Everything that crosses a major version, changes behaviour, was not
/// verified or needs a person, each with its evidence. In a fixed order.
pub fn anomalies(f: &Facts) -> Vec<Anomaly> {
    let mut out = Vec::new();
    if let Some(d) = &f.dependency {
        for l in d.major_lines() {
            let evidence = match d.evidence_for(l) {
                Some(m) => m
                    .evidence
                    .iter()
                    .map(|e| one_line(e))
                    .collect::<Vec<_>>()
                    .join("; "),
                None => "no safety evidence is recorded for this jump: read its upgrade notes before merging".to_string(),
            };
            out.push(Anomaly {
                what: format!(
                    "`{}` {} to {} crosses a major version",
                    l.package, l.from, l.to
                ),
                evidence,
            });
        }
        for fam in d.families.iter().filter(|x| !x.together) {
            out.push(Anomaly {
                what: format!("the {} packages are not on one line", fam.name),
                evidence: fam
                    .members
                    .iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        if !d.advisories.introduced.is_empty() {
            out.push(Anomaly {
                what: format!(
                    "the new versions bring {}",
                    plural(d.advisories.introduced.len(), "advisory", "advisories")
                ),
                evidence: d.advisories.introduced.join(", "),
            });
        }
        if !d.advisories.remaining.is_empty() {
            let by: Vec<String> = d
                .advisories
                .remaining_by_severity
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(s, n)| format!("{n} {s}"))
                .collect();
            out.push(Anomaly {
                what: format!(
                    "{} not fixed by this pull request{}",
                    plural(
                        d.advisories.remaining.len(),
                        "advisory is",
                        "advisories are"
                    ),
                    if by.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", by.join(", "))
                    }
                ),
                evidence: listed(&d.advisories.remaining, 6),
            });
        }
        for p in d.probes.iter().filter(|p| !p.same()) {
            out.push(Anomaly {
                what: format!("{} changed", one_line(&p.what)),
                evidence: format!(
                    "before `{}`, after `{}`",
                    one_line(&p.before),
                    one_line(&p.after)
                ),
            });
        }
        if let Some((false, by)) = &d.frozen_install {
            out.push(Anomaly {
                what: "a frozen install of the lockfile fails".to_string(),
                evidence: one_line(by),
            });
        }
        for u in &d.unread {
            out.push(Anomaly {
                what: format!("{} could not be read on the head", one_line(&u.what)),
                evidence: one_line(&u.why),
            });
        }
    }
    for b in &f.behaviour {
        out.push(Anomaly {
            what: format!("behaviour changes: {}", one_line(&b.what)),
            evidence: one_line(&b.evidence),
        });
    }
    for (outcome, word) in [
        (Outcome::Failed, "failed"),
        (Outcome::Running, "is still running"),
        (Outcome::DidNotRun, "did not run"),
    ] {
        for c in f.checks.iter().filter(|c| c.outcome == outcome) {
            out.push(Anomaly {
                what: format!("{} {word} on `{}`", linked(c), f.short_head()),
                evidence: "what it checks is not verified".to_string(),
            });
        }
    }
    if f.checks.is_empty() {
        out.push(Anomaly {
            what: format!("no check has reported on `{}`", f.short_head()),
            evidence: if f.checks_read {
                "nothing recorded verifies this head yet".to_string()
            } else {
                "the head's checks could not be read".to_string()
            },
        });
    }
    for u in &f.unverified {
        out.push(Anomaly {
            what: format!("not verified: {}", one_line(&u.what)),
            evidence: one_line(&u.why),
        });
    }
    out
}

fn listed(ids: &[String], max: usize) -> String {
    let shown: Vec<&str> = ids.iter().take(max).map(String::as_str).collect();
    let more = ids.len().saturating_sub(max);
    if more > 0 {
        format!("{} and {more} more", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

/// The most anomalies a lead lists; the rest are counted, with where the
/// full list is.
pub const MAX_ANOMALIES: usize = 8;

/// What the reviewer is asked: the kind of feedback wanted, as a question.
fn review_question(f: &Facts) -> String {
    if !f.review_question.trim().is_empty() {
        return one_line(&f.review_question);
    }
    if let Some(d) = &f.dependency {
        let majors = d.major_lines().len();
        if majors == 1 {
            return "Approach: is the major upgrade safe for how this code uses it (evidence in the table)?".to_string();
        }
        if majors > 1 {
            return format!(
                "Approach: are the {majors} major upgrades safe for how this code uses them (evidence in the table)?"
            );
        }
        if !d.advisories.remaining.is_empty() {
            return format!(
                "Can the {} left wait for a release that fixes {}?",
                plural(d.advisories.remaining.len(), "advisory", "advisories"),
                if d.advisories.remaining.len() == 1 {
                    "it"
                } else {
                    "them"
                }
            );
        }
        return "Approach: is it right to take these updates together in one PR?".to_string();
    }
    if let Some(b) = f.behaviour.first() {
        return format!(
            "Approach: is the behaviour change ({}) the one wanted?",
            one_line(&b.what).trim_end_matches('.')
        );
    }
    match &f.why {
        Some(w) if !w.reference.is_empty() => {
            format!("Approach: does this do what {} asks?", w.reference)
        }
        _ => "Correctness: is this right to merge as it is?".to_string(),
    }
}

/// Lockfiles, snapshots and other files a tool writes.
pub fn is_generated(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let name = p.rsplit('/').next().unwrap_or(&p);
    [
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "cargo.lock",
        "poetry.lock",
        "uv.lock",
        "gemfile.lock",
        "composer.lock",
        "go.sum",
        "bun.lockb",
    ]
    .contains(&name)
        || name.ends_with(".snap")
        || p.contains("/__snapshots__/")
        || p.contains("/generated/")
        || name.ends_with(".min.js")
}

fn is_test_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.contains("/test/")
        || p.contains("/tests/")
        || p.contains("__tests__")
        || p.contains(".test.")
        || p.contains(".spec.")
        || p.contains("_test.")
}

/// A file whose change shows in a UI: components, styles, templates, an
/// editor's schema.
pub fn is_ui(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    if is_test_path(&p) || is_generated(&p) {
        return false;
    }
    let ext = p.rsplit('.').next().unwrap_or("");
    [
        "tsx", "jsx", "vue", "svelte", "css", "scss", "sass", "less", "html", "hbs", "erb",
        "liquid", "astro",
    ]
    .contains(&ext)
        || [
            "/components/",
            "/styles/",
            "/templates/",
            "schematypes/",
            "/schemas/",
        ]
        .iter()
        .any(|d| format!("/{p}").contains(d))
}

/// The family a check's name belongs to, in words, for a grouped row.
fn family_label(name: &str) -> &'static str {
    match Family::of(name).first() {
        Some(Family::Tests) => "Tests",
        Some(Family::Lint) => "Lint",
        Some(Family::Types) => "Type check",
        Some(Family::Build) => "Build",
        Some(Family::Install) => "Install",
        None => "Other checks",
    }
}

fn check_name(name: &str) -> String {
    one_line(&name.replace('`', "'"))
}

/// The Verification rows: CI on the head, then CoderHelm's own checks and
/// probes, then what nothing verified. A skipped check is not a row: its
/// workflow's conditions left this pull request out, and it is listed in
/// the full evidence only.
pub fn rows(f: &Facts) -> Vec<crate::pr_body::Row> {
    use crate::pr_body::{Outcome as O, Row};
    let mut ci: Vec<Row> = Vec::new();
    for c in &f.checks {
        let check = format!("{} (CI)", check_name(&c.name));
        let log = if c.url.starts_with("https://") {
            format!("[log]({})", c.url)
        } else {
            String::new()
        };
        match c.outcome {
            Outcome::Passed => ci.push(Row {
                check,
                outcome: O::Passed,
                detail: String::new(),
            }),
            Outcome::Failed => ci.push(Row {
                check,
                outcome: O::Failed,
                detail: log,
            }),
            Outcome::Running => ci.push(Row {
                check,
                outcome: O::NotRun,
                detail: "still running when this was written".into(),
            }),
            Outcome::DidNotRun => {}
        }
    }
    let mut own: Vec<Row> = Vec::new();
    for c in &f.checked {
        let what = one_line(&c.what);
        let mut name = crate::pr_body::fit_words(what.trim_end_matches('.'), 14)
            .trim_end_matches('.')
            .to_string();
        // A cut inside parentheses ends before them.
        if name.matches('(').count() > name.matches(')').count() {
            if let Some(i) = name.rfind('(') {
                name = name[..i].trim_end().to_string();
            }
        }
        own.push(Row {
            check: format!("{name} (CoderHelm)"),
            outcome: O::Passed,
            detail: one_line(&c.result),
        });
    }
    if let Some(d) = &f.dependency {
        for p in &d.probes {
            own.push(Row {
                check: format!("{} (CoderHelm)", one_line(&p.what)),
                outcome: if p.same() { O::Passed } else { O::Failed },
                detail: if p.same() {
                    format!("same before and after: `{}`", one_line(&p.after))
                } else {
                    format!(
                        "before `{}`, after `{}`",
                        one_line(&p.before),
                        one_line(&p.after)
                    )
                },
            });
        }
        if let Some((false, by)) = &d.frozen_install {
            if !f.checks.iter().any(|c| by.contains(&c.name)) {
                own.push(Row {
                    check: "Install from the lockfile".into(),
                    outcome: O::Failed,
                    detail: one_line(by),
                });
            }
        }
        for u in &d.unread {
            own.push(Row {
                check: format!("{} on the head", one_line(&u.what)),
                outcome: O::NotRun,
                detail: format!("could not be read: {}", one_line(&u.why)),
            });
        }
    }
    for u in &f.unverified {
        let mut what = one_line(&u.what).trim_end_matches('.').to_string();
        for tail in [
            " was not run",
            " has not run yet",
            " has not run",
            " not run",
        ] {
            if what.ends_with(tail) {
                what.truncate(what.len() - tail.len());
            }
        }
        own.push(Row {
            check: crate::pr_body::fit_words(&what, 12)
                .trim_end_matches('.')
                .to_string(),
            outcome: O::NotRun,
            detail: one_line(&u.why),
        });
    }
    if f.checks.iter().all(|c| c.outcome == Outcome::DidNotRun) {
        ci.insert(
            0,
            Row {
                check: "CI".into(),
                outcome: O::NotRun,
                detail: if f.checks_read {
                    "no check has reported on this head yet".into()
                } else {
                    "the head's checks could not be read".into()
                },
            },
        );
    }
    // Too many to list: passed CI checks grouped by what they check.
    if ci.len() + own.len() > crate::pr_body::MAX_ROWS {
        let mut grouped: Vec<Row> = ci
            .iter()
            .filter(|r| r.outcome != O::Passed)
            .cloned()
            .collect();
        let mut groups: Vec<(&'static str, Vec<&str>)> = Vec::new();
        for c in f.checks.iter().filter(|c| c.outcome == Outcome::Passed) {
            let l = family_label(&c.name);
            match groups.iter_mut().find(|(g, _)| *g == l) {
                Some(g) => g.1.push(&c.name),
                None => groups.push((l, vec![&c.name])),
            }
        }
        for (l, names) in groups {
            grouped.push(match names.as_slice() {
                [one] => Row {
                    check: format!("{} (CI)", check_name(one)),
                    outcome: O::Passed,
                    detail: String::new(),
                },
                _ => Row {
                    check: format!("{l} (CI): {} jobs", names.len()),
                    outcome: O::Passed,
                    detail: "all passed".into(),
                },
            });
        }
        ci = grouped;
    }
    ci.extend(own);
    ci
}

fn semver(v: &str) -> Vec<u64> {
    v.trim_start_matches(['v', '^', '~', '='])
        .split(['.', '-', '+'])
        .take(3)
        .map(|p| p.parse().unwrap_or(0))
        .collect()
}

/// `major`, `minor`, `patch` or what else the move is.
pub fn bump_type(l: &DepLine) -> &'static str {
    if !l.moved {
        return "not moved";
    }
    if l.to.is_empty() || l.to.contains(' ') {
        return "removed";
    }
    if l.major {
        return "major";
    }
    let (a, b) = (semver(&l.from), semver(&l.to));
    if a.first() != b.first() {
        "major"
    } else if a.get(1) != b.get(1) {
        "minor"
    } else {
        "patch"
    }
}

/// A dependency update's summary, from what the lockfile on the head
/// resolves.
fn dependency_summary(d: &DependencyFacts) -> String {
    let moved: Vec<&DepLine> = d.lines.iter().filter(|l| l.moved).collect();
    let eco = if d.ecosystem.trim().is_empty() {
        String::new()
    } else {
        format!("{} ", d.ecosystem.trim())
    };
    let mut kinds: Vec<String> = Vec::new();
    for k in ["major", "minor", "patch"] {
        let n = moved.iter().filter(|l| bump_type(l) == k).count();
        if n > 0 {
            kinds.push(format!("{n} {k}"));
        }
    }
    let mut s = format!(
        "Updates {}{}",
        plural(
            moved.len(),
            &format!("{eco}package"),
            &format!("{eco}packages")
        ),
        if kinds.is_empty() {
            ".".to_string()
        } else {
            format!(": {}.", kinds.join(", "))
        }
    );
    let fixed = d.advisories.fixed.len();
    let left = d.advisories.remaining.len();
    if fixed > 0 || left > 0 {
        let by: Vec<String> = d
            .advisories
            .remaining_by_severity
            .iter()
            .filter(|(_, n)| **n > 0)
            .map(|(s, n)| format!("{n} {s}"))
            .collect();
        s.push_str(&format!(
            " Fixes {}",
            plural(fixed, "security advisory", "security advisories")
        ));
        if left > 0 {
            s.push_str(&format!(
                "; {left} {} open{}",
                if left == 1 { "stays" } else { "stay" },
                if by.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", by.join(", "))
                }
            ));
        }
        s.push('.');
    }
    let still = d.lines.iter().filter(|l| !l.moved).count();
    if still > 0 {
        s.push_str(&format!(
            " {} the plan named did not move on the head.",
            plural(still, "line", "lines")
        ));
    }
    s
}

/// The visible table of a dependency update: majors, security fixes and
/// lines that need a look, ten at most.
fn dependency_rows(d: &DependencyFacts) -> Vec<crate::pr_body::DepRow> {
    let mut rows: Vec<(u8, crate::pr_body::DepRow)> = Vec::new();
    for l in &d.lines {
        let mut why: Vec<String> = Vec::new();
        let mut rank = 9u8;
        if l.moved && l.major {
            rank = 0;
            why.push(match d.evidence_for(l) {
                Some(m) => {
                    let e = m
                        .evidence
                        .iter()
                        .map(|e| one_line(e))
                        .collect::<Vec<_>>()
                        .join("; ");
                    let mut c = e.chars();
                    let e: String = match c.next() {
                        Some(f) => f.to_uppercase().chain(c).collect(),
                        None => e,
                    };
                    format!("Major. {e}")
                }
                None => "Major; no safety evidence is recorded: read its upgrade notes".into(),
            });
        }
        if !l.remaining.is_empty() {
            rank = rank.min(1);
            why.push(format!("Security: {} still open", l.remaining.join(", ")));
        }
        if !l.fixed.is_empty() {
            rank = rank.min(2);
            why.push(format!("Security: {}", l.fixed.join(", ")));
        }
        if !l.moved {
            rank = rank.min(3);
            why.push("Not moved on the head".into());
        }
        if rank == 9 {
            continue;
        }
        let change = if l.moved {
            format!(
                "{} to {}",
                l.from,
                if l.to.is_empty() { "?" } else { &l.to }
            )
        } else {
            l.from.clone()
        };
        rows.push((
            rank,
            crate::pr_body::DepRow {
                package: l.package.clone(),
                change,
                why: why.join(". "),
            },
        ));
    }
    rows.sort_by_key(|(r, _)| *r);
    rows.into_iter()
        .take(crate::pr_body::MAX_DEP_ROWS)
        .map(|(_, r)| r)
        .collect()
}

/// Every update, for the first fold.
fn all_updates(d: &DependencyFacts) -> String {
    let mut s = String::from("| Package | From | To | Type |\n|---|---|---|---|\n");
    for l in &d.lines {
        s.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            crate::pr_body::cell(&l.package),
            crate::pr_body::cell(&l.from),
            crate::pr_body::cell(if l.moved { &l.to } else { &l.from }),
            bump_type(l)
        ));
    }
    s.trim_end().to_string()
}

fn sentence_part(s: &str) -> String {
    one_line(s)
        .trim_end_matches(['.', ';', ','])
        .trim()
        .to_string()
}

/// A step a person has to act on, as an item of "Needed from you".
fn item_of(s: &crate::pr_steps::Step) -> crate::pr_body::HumanItem {
    use crate::pr_body::{HumanItem, ItemKind, Merge};
    use crate::pr_steps::{State, When};
    let what = sentence_part(&crate::pr_steps::plain(&s.what));
    let merge = if s.when == When::AfterMerge {
        Merge::After
    } else {
        Merge::Before
    };
    let command = if crate::pr_steps::is_command(&s.how) {
        s.how.trim().trim_matches('`').to_string()
    } else {
        String::new()
    };
    let owner = if s.owner.eq_ignore_ascii_case("coderhelm") || s.owner.trim().is_empty() {
        "Repository admin".to_string()
    } else {
        s.owner.clone()
    };
    let (action, why, owner) = match &s.state {
        State::Failed { cause, .. } => (what.clone(), sentence_part(cause), owner),
        State::Person { why, .. } => (
            what.clone(),
            sentence_part(&crate::pr_steps::plain(why)),
            owner,
        ),
        State::Approval { who, place } => (
            format!("Approve \"{what}\" {}", sentence_part(place)),
            "CoderHelm does it once it is approved".to_string(),
            who.clone(),
        ),
        _ => (what.clone(), String::new(), owner),
    };
    HumanItem {
        id: crate::pr_body::slug(&what),
        kind: ItemKind::Run,
        owner,
        merge,
        action,
        why,
        command,
        done: matches!(s.state, State::Done { .. }),
        ..Default::default()
    }
}

fn is_look(s: &crate::pr_steps::Step) -> bool {
    s.what.trim_start().starts_with("Look at") || s.owner.eq_ignore_ascii_case("the reviewer")
}

/// What a person has to do, as "Needed from you" lists it: the gate's list
/// when there is one, else the steps that need a person and the questions
/// only a person can answer. Looking at the change is review work: it is in
/// Review focus, never here.
pub fn needed_items(f: &Facts) -> Vec<crate::pr_body::HumanItem> {
    use crate::pr_body::{HumanItem, ItemKind, Merge};
    if !f.needed.is_empty() {
        return f.needed.clone();
    }
    let mut out: Vec<HumanItem> = f
        .steps
        .iter()
        .filter(|s| s.needs_person() && !is_look(s))
        .map(item_of)
        .collect();
    for o in f.open.iter().filter(|o| o.closer == Closer::Person) {
        let plain = crate::pr_steps::plain(&o.text);
        let question = plain.trim_start().starts_with("Answer:") || plain.trim_end().ends_with('?');
        let text = plain.trim_start_matches("Answer:").trim();
        out.push(if question {
            HumanItem {
                id: crate::pr_body::slug(text),
                kind: ItemKind::Decide,
                owner: "Ticket owner".into(),
                merge: Merge::Before,
                action: format!("Decide: {}", text.trim_end_matches('.')),
                why: "a product decision the ticket leaves open".into(),
                ..Default::default()
            }
        } else {
            HumanItem {
                id: crate::pr_body::slug(text),
                kind: ItemKind::Run,
                owner: "Repository admin".into(),
                merge: Merge::Before,
                action: text.trim_end_matches('.').to_string(),
                why: "the run recorded it as something only a person can close".into(),
                ..Default::default()
            }
        });
    }
    // A person's step that is now done stays, ticked, while there is room.
    for s in f.steps.iter().filter(|s| {
        matches!(s.state, crate::pr_steps::State::Done { .. })
            && !s.owner.eq_ignore_ascii_case("coderhelm")
            && !is_look(s)
    }) {
        if out.len() < crate::pr_body::MAX_ITEMS {
            out.push(item_of(s));
        }
    }
    out
}

/// The body every description shows, from the facts of its head. `ticked`:
/// the item ids a person ticked in the body this one replaces.
pub fn body_of(f: &Facts, ticked: &[String]) -> crate::pr_body::Body {
    use crate::pr_body::{Body, Focus, Level, ReadFirst, Screens, Ticket};
    let head = f.short_head().to_string();
    let ticket = f.why.as_ref().filter(|w| !w.reference.is_empty()).map(|w| {
        let url = if !w.url.is_empty() {
            w.url.clone()
        } else if let (Some(n), true) = (
            w.reference.strip_prefix('#'),
            f.repo_url.starts_with("https://"),
        ) {
            format!("{}/issues/{n}", f.repo_url)
        } else {
            String::new()
        };
        Ticket {
            key: w.reference.clone(),
            url,
        }
    });
    let summary = match &f.dependency {
        Some(d) => dependency_summary(d),
        None => one_line(&f.summary),
    };
    let done: Vec<String> = f
        .steps
        .iter()
        .filter(|s| {
            matches!(s.state, crate::pr_steps::State::Done { .. })
                && s.owner.eq_ignore_ascii_case("coderhelm")
        })
        .take(2)
        .map(|s| sentence_part(&crate::pr_steps::plain(&s.what)))
        .collect();
    let outside = if done.is_empty() {
        String::new()
    } else {
        let mut first = done.join("; ");
        if let Some(c) = first.get(..1) {
            first = format!("{}{}", c.to_lowercase(), &first[1..]);
        }
        format!("Done outside the diff: {first}.")
    };
    // Where to see a UI change.
    let look = f
        .steps
        .iter()
        .find(|s| is_look(s))
        .map(|s| {
            sentence_part(
                s.what
                    .trim_start()
                    .trim_start_matches("Look at")
                    .trim_start(),
            )
        })
        .unwrap_or_default();
    let see_it = match (f.preview_url.starts_with("https://"), look.is_empty()) {
        (true, true) => format!("[the preview]({})", f.preview_url),
        (true, false) => format!("[the preview]({}), {look}", f.preview_url),
        (false, false) => look,
        (false, true) => String::new(),
    };
    let generated: Vec<&FileChange> = f.files.iter().filter(|x| is_generated(&x.path)).collect();
    let read_first = f
        .risk
        .as_ref()
        .and_then(|r| r.look_first.as_ref())
        .map(|(path, why)| {
            let url = if f.repo_url.starts_with("https://") && !head.is_empty() {
                format!("{}/blob/{head}/{path}", f.repo_url.trim_end_matches('/'))
            } else {
                String::new()
            };
            let others = f
                .files
                .iter()
                .filter(|x| x.path != *path && !is_generated(&x.path))
                .count();
            let mut skim: Vec<String> = Vec::new();
            if others > 0 {
                skim.push(format!(
                    "then the other {}",
                    plural(others, "file", "files")
                ));
            }
            if let Some(g) = generated.first() {
                skim.push(format!(
                    "skip `{}`{}, which {} generated",
                    g.path,
                    if generated.len() > 1 {
                        format!(" and {} more", generated.len() - 1)
                    } else {
                        String::new()
                    },
                    if generated.len() > 1 { "are" } else { "is" }
                ));
            }
            ReadFirst {
                path: path.clone(),
                url,
                // Its own parentheses would nest inside the bullet's.
                why: one_line(why).replace(" (", ", ").replace(')', ""),
                skim: skim.join("; "),
            }
        });
    let lines: usize = f
        .files
        .iter()
        .filter(|x| !is_generated(&x.path))
        .map(|x| x.changed)
        .sum();
    let large = if lines > crate::pr_body::LARGE_CHANGE {
        let mut order: Vec<&FileChange> = f
            .files
            .iter()
            .filter(|x| !is_generated(&x.path) && x.changed > 0)
            .collect();
        order.sort_by_key(|x| (is_test_path(&x.path), std::cmp::Reverse(x.changed)));
        format!(
            "Large change: {lines} lines; suggested order: {}",
            order
                .iter()
                .take(3)
                .map(|x| format!("`{}`", x.path.rsplit('/').next().unwrap_or(&x.path)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };
    let (level, mut risk, rollback) = match &f.risk {
        Some(r) => (Level::of(&r.level), one_line(&r.why), one_line(&r.rollback)),
        None => (
            Level::Medium,
            "the diff could not be read, so nothing was judged against it".to_string(),
            String::new(),
        ),
    };
    if risk.chars().count() < 10 && !f.files.is_empty() {
        risk = format!(
            "{} in the diff, none on a path the risk check marks as shared or sensitive",
            plural(f.files.len(), "file", "files")
        );
    }
    if let Some(d) = &f.dependency {
        let mut extra: Vec<String> = Vec::new();
        if !d.advisories.introduced.is_empty() {
            extra.push(format!(
                "the new versions bring {}",
                d.advisories.introduced.join(", ")
            ));
        }
        for fam in d.families.iter().filter(|x| !x.together) {
            extra.push(format!("the {} packages are not on one line", fam.name));
        }
        if !extra.is_empty() {
            risk = if risk.is_empty() {
                extra.join("; ")
            } else {
                format!("{}; {}", risk.trim_end_matches('.'), extra.join("; "))
            };
        }
    }
    let screens = match &f.screens {
        Some(s) => Some(s.clone()),
        None if f.files.iter().any(|x| is_ui(&x.path)) => Some(Screens::Missing(
            "CoderHelm had no runnable preview to capture for this run".into(),
        )),
        None => None,
    };
    // The full list of checks, and how to check by hand.
    let mut evidence: Vec<String> = Vec::new();
    let how = one_line(&f.how_to_check);
    if !how.is_empty() {
        evidence.push(format!("How to check: {how}."));
    }
    if !f.checks.is_empty() {
        let mut s = String::from("Every check on this head:\n");
        for c in &f.checks {
            s.push_str(&format!(
                "- {}: {}\n",
                linked(c),
                match c.outcome {
                    Outcome::Passed => "passed",
                    Outcome::Failed => "failed",
                    Outcome::Running => "still running",
                    Outcome::DidNotRun => "skipped (its conditions left this PR out)",
                }
            ));
        }
        evidence.push(s.trim_end().to_string());
    }
    let items = needed_items(f);
    let mut built: Vec<String> = Vec::new();
    for l in crate::pr_steps::others(&f.steps)
        .into_iter()
        .filter(|l| !l.starts_with("Done:"))
    {
        built.push(format!("- {l}"));
    }
    if items.len() > crate::pr_body::MAX_ITEMS {
        built.push(format!(
            "- Also needed from a person: {}",
            items[crate::pr_body::MAX_ITEMS..]
                .iter()
                .map(|i| sentence_part(&i.action))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    let mut built = built.join("\n");
    for n in &f.notes {
        built.push_str(&format!("\n\n{}", one_line(n)));
    }
    let (notes_label, notes_top) = match &f.dependency {
        Some(d) if !d.lines.is_empty() => {
            (format!("All {} updates", d.lines.len()), all_updates(d))
        }
        _ => (String::new(), String::new()),
    };
    Body {
        ticket,
        head,
        results_of: String::new(),
        alert: String::new(),
        summary,
        outside,
        needed: items,
        ticked: ticked.to_vec(),
        focus: Focus {
            question: review_question(f),
            read_first,
            see_it,
            large,
        },
        dep_rows: f
            .dependency
            .as_ref()
            .map(dependency_rows)
            .unwrap_or_default(),
        level,
        risk,
        rollback,
        rows: rows(f),
        screens,
        notes_label,
        notes_top,
        evidence: evidence.join("\n\n"),
        built: built.trim().to_string(),
        run_url: f.run_url.clone(),
    }
}

/// What a person has to do, as lines (for logs and counts). Empty when
/// nothing is needed.
pub fn needed_lines(f: &Facts) -> Vec<String> {
    needed_items(f)
        .iter()
        .filter(|i| !i.done)
        .map(|i| i.action.clone())
        .collect()
}

/// The visible part of a description: what a reader sees first, rendered
/// from the facts and from nothing else, between the lead's markers.
pub fn lead(f: &Facts) -> String {
    lead_ticked(f, &[])
}

/// [`lead`], keeping the ticks a person made.
pub fn lead_ticked(f: &Facts, ticked: &[String]) -> String {
    let r = crate::pr_body::render(&body_of(f, ticked));
    format!("{LEAD_START}\n{}\n\n{LEAD_END}", r.visible)
}

/// The summary as [`lead`] writes it (what `pr_layout` takes out of a
/// section that says it again).
pub fn lead_line(f: &Facts) -> String {
    one_line(&f.summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn check(name: &str, outcome: Outcome) -> CheckFact {
        CheckFact {
            name: name.into(),
            outcome,
            url: format!("https://ci.example.com/{}", name.replace(' ', "-")),
        }
    }

    fn ticket_facts() -> Facts {
        Facts {
            head: "abc1234def".into(),
            summary: "Retries the export poller three times before it gives up.".into(),
            why: Some(Why {
                reference: "OPS-41".into(),
                title: "Export poller gives up on the first timeout".into(),
                url: String::new(),
            }),
            files: vec![FileChange {
                path: "src/export/poller.rs".into(),
                status: "modified".into(),
                previous: String::new(),
                changed: 0,
            }],
            files_complete: true,
            checks: vec![
                check("unit tests", Outcome::Passed),
                check("lint", Outcome::Passed),
            ],
            checks_read: true,
            risk: Some(Risk {
                level: "Low".into(),
                why: "one module".into(),
                look_first: Some((
                    "src/export/poller.rs".into(),
                    "the retry loop decides when a poll is abandoned".into(),
                )),
                rollback: "revert this pull request.".into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn the_lead_says_the_change_the_focus_the_risk_and_the_checks() {
        let mut f = ticket_facts();
        f.how_to_check = "`cargo test -p export`".into();
        f.repo_url = "https://github.com/acme/site".into();
        let out = lead(&f);
        assert_eq!(
            out,
            "<!-- coderhelm:lead -->\n\
             <!-- coderhelm:body v1 ticket=OPS-41 head=abc1234 -->\n\
             Retries the export poller three times before it gives up (OPS-41).<!-- ch:summary-end -->\n\n\
             ### Review focus\n\n\
             - Approach: does this do what OPS-41 asks?\n\
             - Read first: [`src/export/poller.rs`](https://github.com/acme/site/blob/abc1234/src/export/poller.rs) (the retry loop decides when a poll is abandoned).\n\n\
             **Risk:** Low. One module.\n\n\
             **Rollback:** Revert this pull request.\n\n\
             ### Verification\n\n\
             On abc1234 (head of this PR):\n\n\
             | Check | Result |\n\
             |---|---|\n\
             | unit tests (CI) | Passed |\n\
             | lint (CI) | Passed |\n\n\
             <!-- /coderhelm:lead -->"
        );
        assert_eq!(
            crate::pr_body::defects(&out, "abc1234"),
            vec!["no footer".to_string()]
        );
    }

    #[test]
    fn nothing_is_called_checked_that_no_record_backs() {
        let mut f = ticket_facts();
        f.checks = vec![];
        let out = lead(&f);
        assert!(
            out.contains("| CI | Not run: no check has reported on this head yet |"),
            "{out}"
        );
        f.checks = vec![
            check("lint", Outcome::Passed),
            check("unit tests", Outcome::Failed),
            check("Deploy to Production", Outcome::DidNotRun),
        ];
        f.open = vec![OpenItem {
            text: "Set the staging secret.".into(),
            closer: Closer::Person,
        }];
        f.unverified = vec![Unverified {
            what: "the export against the live bucket".into(),
            why: "the sandbox has no credentials for it".into(),
        }];
        let out = lead(&f);
        // Failed first, with its log; a skipped job is not a row.
        let failed = out
            .find("| unit tests (CI) | Failed: [log](https://ci.example.com/unit-tests) |")
            .unwrap();
        assert!(
            failed < out.find("| lint (CI) | Passed |").unwrap(),
            "{out}"
        );
        assert!(!out.contains("Deploy to Production"), "{out}");
        assert!(
            out.contains("| the export against the live bucket | Not run: the sandbox has no credentials for it |"),
            "{out}"
        );
        // What a person must do comes right after the Summary.
        assert!(
            out.contains("<!-- ch:summary-end -->\n\n### Needed from you\n\n- [ ] <!-- ch:set-the-staging-secret --> **Repository admin**, before merge: Set the staging secret. Why a person: the run recorded it as something only a person can close."),
            "{out}"
        );
        assert!(!out.contains("(a person)"), "{out}");
    }

    #[test]
    fn a_major_jump_states_its_evidence_or_that_there_is_none() {
        let mut f = ticket_facts();
        f.dependency = Some(DependencyFacts {
            lines: vec![
                DepLine {
                    package: "glob-walk".into(),
                    from: "2.1.0".into(),
                    to: "3.0.1".into(),
                    moved: true,
                    major: true,
                    ..Default::default()
                },
                DepLine {
                    package: "tiny-yaml".into(),
                    from: "1.2.0".into(),
                    to: "2.0.0".into(),
                    moved: true,
                    major: true,
                    ..Default::default()
                },
            ],
            majors: vec![MajorEvidence {
                package: "glob-walk".into(),
                from: "2.1.0".into(),
                to: "3.0.1".into(),
                evidence: vec![
                    "3.0 drops the sync API; this code calls only `walk()` (src/scan.rs:14)".into(),
                ],
            }],
            ..Default::default()
        });
        let out = lead(&f);
        assert!(
            out.contains("| `glob-walk` | 2.1.0 to 3.0.1 | Major. 3.0 drops the sync API"),
            "{out}"
        );
        assert!(
            out.contains("| `tiny-yaml` | 1.2.0 to 2.0.0 | Major; no safety evidence is recorded: read its upgrade notes |"),
            "{out}"
        );
        assert!(
            out.contains("Approach: are the 2 major upgrades safe"),
            "{out}"
        );
        assert!(
            out.contains("Updates 2 packages: 2 major (OPS-41)."),
            "{out}"
        );
    }

    #[test]
    fn many_checks_are_grouped_into_eight_rows() {
        let mut f = ticket_facts();
        f.checks = (0..12)
            .map(|i| check(&format!("unit tests ({i}/12)"), Outcome::Passed))
            .chain([
                check("lint", Outcome::Passed),
                check("build", Outcome::Failed),
            ])
            .collect();
        f.unverified = (0..3)
            .map(|i| Unverified {
                what: format!("thing {i}"),
                why: "not run".into(),
            })
            .collect();
        let out = lead(&f);
        assert!(
            out.contains("| Tests (CI): 12 jobs | Passed; all passed |"),
            "{out}"
        );
        assert!(out.contains("| build (CI) | Failed:"), "{out}");
        let rows = out
            .lines()
            .filter(|l| l.starts_with("| ") && !l.starts_with("| Check"))
            .count();
        assert!(rows <= crate::pr_body::MAX_ROWS, "{out}");
    }

    #[test]
    fn a_file_is_found_however_it_was_written() {
        let f = ticket_facts();
        assert!(f.has_file("src/export/poller.rs"));
        assert!(f.has_file("poller.rs"));
        assert!(f.has_file("export/poller.rs"));
        assert!(f.has_file("src/export"));
        assert!(!f.has_file("src/export/writer.rs"));
        assert!(!f.has_file(""));
    }

    #[test]
    fn check_families_come_from_names() {
        assert_eq!(Family::of("Rust (tests)"), vec![Family::Tests]);
        assert_eq!(Family::of("Type check"), vec![Family::Types]);
        assert_eq!(
            Family::of("npm ci --frozen-lockfile"),
            vec![Family::Install]
        );
        assert!(Family::of("Workflow security").is_empty());
        let f = ticket_facts();
        assert_eq!(f.passed_families(), vec![Family::Tests, Family::Lint]);
        assert!(f.all_checks_passed());
    }
}
