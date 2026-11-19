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
//!   order: one standalone line, what is to be decided with CoderHelm's own
//!   conclusion, and the anomalies, each with its evidence.
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
    /// Sign-offs that belong to the reviewer's merge decision (a page to
    /// look at) and decisions that matter for it, from `person_gate`:
    /// carried in Review focus, never listed as chores.
    #[serde(default)]
    pub review_notes: Vec<String>,
    /// Where a UI change can be seen.
    #[serde(default)]
    pub preview_url: String,
    /// A UI change's screenshots, or why there are none.
    #[serde(default)]
    pub screens: Option<crate::pr_body::Screens>,
    /// What the pull request waits on, one line (`crate::waiting_line`).
    #[serde(default)]
    pub waiting: String,
    /// Where the preview of the head stands (`crate::preview`): its
    /// checks, address and captured pages. A preview or end-to-end check
    /// that covers the change and was skipped is a gap, never evidence.
    #[serde(default)]
    pub preview: Option<crate::preview::State>,
    /// The feedback wanted, when something more specific than the records
    /// says it.
    #[serde(default)]
    pub review_question: String,
    /// What the change leaves out, each `item (why)`: the run's scope
    /// ledger and the written out-of-scope lines, without what the diff
    /// brings back (`crate::scope_ledger`).
    #[serde(default)]
    pub out_of_scope: Vec<String>,
    /// The ledger's out-of-scope items the diff brings back: never said as
    /// out of scope; each is not verified until it is taken out or moved in.
    #[serde(default)]
    pub scope_breaches: Vec<crate::scope_ledger::Breach>,
    /// Each acceptance criterion traced to its code, tests and proof on
    /// this head (`crate::trace`): shown as a table with the checks.
    #[serde(default)]
    pub trace: Option<crate::trace::Trace>,
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

/// What the reviewer is asked to decide: the merge decision only, as a
/// question that names the kind of feedback. Never CoderHelm's analysis
/// (`person_gate`): [`conclusion`] states that, with its evidence.
fn review_question(f: &Facts) -> String {
    if !f.review_question.trim().is_empty() {
        return one_line(&f.review_question);
    }
    match (&f.dependency, &f.why) {
        (Some(d), _) => format!(
            "Approach: decide whether to merge {}.",
            plural(d.moved(), "dependency update", "dependency updates")
        ),
        (None, Some(w)) if !w.reference.is_empty() => {
            format!(
                "Approach: decide whether this does what {} asks.",
                w.reference
            )
        }
        _ => "Correctness: decide whether this is right to merge.".to_string(),
    }
}

/// What CoderHelm concludes from the facts, with what each part rests on.
/// The head is named once, in Verification, so it is "this head" here.
pub fn conclusion(f: &Facts) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(d) = &f.dependency {
        let majors = d.major_lines();
        if !majors.is_empty() {
            let bare: Vec<String> = majors
                .iter()
                .filter(|l| d.evidence_for(l).is_none())
                .map(|l| format!("`{}` {} to {}", l.package, l.from, l.to))
                .collect();
            let covered = majors.len() - bare.len();
            if bare.is_empty() {
                parts.push(format!(
                    "{} safe for this code, each by the usages, checks and probes recorded beside it",
                    if majors.len() == 1 {
                        "the major version jump is".to_string()
                    } else {
                        format!("all {} major version jumps are", majors.len())
                    }
                ));
            } else {
                parts.push(format!(
                    "{covered} of {} major version jumps are shown safe for this code; {} {} no recorded evidence, so {} the risk in this merge",
                    majors.len(),
                    listed(&bare, 4),
                    if bare.len() == 1 { "has" } else { "have" },
                    if bare.len() == 1 { "it is" } else { "they are" },
                ));
            }
        }
        if !d.advisories.remaining.is_empty() {
            parts.push(format!(
                "{} left open, listed below with why",
                plural(
                    d.advisories.remaining.len(),
                    "advisory is",
                    "advisories are"
                )
            ));
        }
        if !d.advisories.introduced.is_empty() {
            parts.push(format!(
                "the new versions bring {}",
                plural(d.advisories.introduced.len(), "advisory", "advisories")
            ));
        }
    }
    if let Some(b) = f.behaviour.first() {
        parts.push(format!(
            "the behaviour changes as recorded ({}; {})",
            one_line(&b.what).trim_end_matches('.'),
            one_line(&b.evidence).trim_end_matches('.')
        ));
    }
    let failing = f
        .checks
        .iter()
        .filter(|c| c.outcome == Outcome::Failed)
        .count();
    if failing > 0 {
        parts.push(format!(
            "{} on this head, so it is not ready",
            plural(failing, "check fails", "checks fail")
        ));
    } else if f.all_checks_passed() {
        parts.push("all checks on this head passed".to_string());
    } else if f.checks.is_empty() {
        parts.push("no check has reported on this head yet".to_string());
    } else {
        let running = f
            .checks
            .iter()
            .filter(|c| c.outcome == Outcome::Running)
            .count();
        if running > 0 {
            parts.push(format!(
                "{} still running on this head; this is updated when {}",
                plural(running, "check is", "checks are"),
                if running == 1 { "it ends" } else { "they end" }
            ));
        }
    }
    // Nothing to conclude is said by saying nothing: a sentence that
    // carries no fact is filler (`pr_review`).
    parts.join("; ")
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
    let deploy = deploy_rows(f);
    let shown = |n: &str| {
        let row = format!("{} (deploy label)", check_name(n));
        deploy.iter().any(|r| r.check == row)
    };
    let mut ci: Vec<Row> = Vec::new();
    for c in f.checks.iter().filter(|c| !shown(&c.name)) {
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
            // Running is never a result: it is said as running, and the
            // description is rebuilt when it ends (`description::with_stamp`).
            Outcome::Running => ci.push(Row {
                check,
                outcome: O::Running,
                detail: String::new(),
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
    if deploy.len() + ci.len() + own.len() > crate::pr_body::MAX_ROWS {
        let mut grouped: Vec<Row> = ci
            .iter()
            .filter(|r| r.outcome != O::Passed)
            .cloned()
            .collect();
        let mut groups: Vec<(&'static str, Vec<&str>)> = Vec::new();
        for c in f
            .checks
            .iter()
            .filter(|c| c.outcome == Outcome::Passed && !shown(&c.name))
        {
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
    let mut out = deploy;
    out.extend(ci);
    out.extend(own);
    out
}

/// The deploy label's checks on the head, as the head's preview read them
/// (`crate::deploy_label`): first in Verification, never grouped away, each
/// with its run. A failure a later skipped run hides from the newest check
/// of its name still reads as failed.
fn deploy_rows(f: &Facts) -> Vec<crate::pr_body::Row> {
    use crate::pr_body::{Outcome as O, Row};
    use crate::preview::Outcome as P;
    let Some(p) = &f.preview else {
        return Vec::new();
    };
    let covers: Vec<crate::preview::Covering> = p
        .covers
        .iter()
        .filter(|c| !c.label.is_empty())
        .cloned()
        .collect();
    let mut out = Vec::new();
    for r in p
        .checks
        .iter()
        .filter(|r| crate::preview::is_covering(&covers, &r.name))
    {
        let link = if r.url.starts_with("https://") {
            format!("[run]({})", r.url)
        } else {
            String::new()
        };
        let with = |w: &str| match (w.is_empty(), link.is_empty()) {
            (true, _) => link.clone(),
            (false, true) => w.to_string(),
            (false, false) => format!("{w}; {link}"),
        };
        let (outcome, detail) = match r.outcome {
            P::Passed => (O::Passed, with("")),
            P::Failed if r.no_jobs => (O::Failed, with("it failed to start")),
            P::Failed => (O::Failed, with("")),
            P::Running => (O::Running, with("")),
            P::Waiting if r.environment.is_empty() => {
                (O::NotRun, with("waiting for an environment's approval"))
            }
            P::Waiting => (
                O::NotRun,
                with(&format!(
                    "waiting for approval to deploy to `{}`",
                    r.environment.replace('`', "'")
                )),
            ),
            P::Skipped => continue,
        };
        out.push(Row {
            check: format!("{} (deploy label)", check_name(&r.name)),
            outcome,
            detail,
        });
    }
    out
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

/// The step that asks the reviewer to look at the change: a sign-off the
/// gate folds into the merge decision, shown under Review focus.
fn is_look(s: &crate::pr_steps::Step) -> bool {
    s.for_review().is_some()
}

/// What a person has to do, as "Needed from you" lists it. The gate
/// (`person_gate`) decides every item: a step a person was to do, a
/// question put to a person and an open item only a person can close reach
/// the list only as the gate's own item, for a listed reason. Looking at
/// the change is review work: it is in Review focus, never here.
pub fn needed_items(f: &Facts) -> Vec<crate::pr_body::Needed> {
    use crate::person_gate::{self as gate, Outcome as G};
    use crate::pr_body::{Merge, Needed};
    let mut out: Vec<Needed> = Vec::new();
    for s in f.steps.iter().filter(|s| s.needs_person()) {
        if let G::Human(item) = gate::decide(&s.ask(), &gate::Context::default()) {
            out.push(Needed {
                id: crate::pr_body::slug(&crate::pr_steps::plain(&s.what)),
                merge: if s.when == crate::pr_steps::When::AfterMerge {
                    Merge::After
                } else {
                    Merge::Before
                },
                item,
            });
        }
    }
    for o in f.open.iter().filter(|o| o.closer == Closer::Person) {
        let plain = crate::pr_steps::plain(&o.text);
        // A question keeps its kind, so a hard-to-reverse choice is a
        // yes/no on CoderHelm's recommendation.
        let outcome = match plain.trim_start().strip_prefix("Answer:") {
            Some(q) => crate::person_gate::line_outcome(gate::Kind::Question, q.trim()),
            None => crate::person_gate::line_outcome(gate::Kind::OpenItem, &plain),
        };
        if let G::Human(item) = outcome {
            out.push(Needed {
                id: crate::pr_body::slug(plain.trim_start_matches("Answer:").trim()),
                merge: Merge::Before,
                item,
            });
        }
    }
    let mut seen: Vec<String> = Vec::new();
    out.retain(|n| {
        let k = n.key();
        let fresh = !seen.contains(&k);
        seen.push(k);
        fresh
    });
    out
}

/// [`conclusion`], and the sign-offs the merge decision also carries: a
/// look at the change is shown as "See it", so only the others are named.
fn conclusion_with_signoffs(f: &Facts) -> String {
    let shown: Vec<String> = f.steps.iter().filter_map(|s| s.for_review()).collect();
    let notes: Vec<String> = f
        .review_notes
        .iter()
        .map(|n| one_line(n).trim_end_matches('.').to_string())
        .filter(|n| !n.is_empty() && !shown.iter().any(|s| one_line(s).trim_end_matches('.') == n))
        .collect();
    let c = conclusion(f);
    if notes.is_empty() {
        c
    } else if c.is_empty() {
        format!("your merge decision also signs off: {}", notes.join("; "))
    } else {
        format!(
            "{c}. Your merge decision also signs off: {}",
            notes.join("; ")
        )
    }
}

/// The body every description shows, from the facts of its head. `ticked`:
/// the item ids a person ticked in the body this one replaces.
/// How a skipped check that covers the change reads: a gap CoderHelm
/// closes, never "nothing to do".
pub const GAP_WORDS: &str =
    "not run yet: it covers this change, so CoderHelm starts it on the preview and reads its result";

/// The preview's captures, or the one line saying why there are none.
fn screens_of(p: &crate::preview::State, visible: bool) -> Option<crate::pr_body::Screens> {
    use crate::pr_body::Screens;
    use crate::preview::Stage;
    if !p.shots.is_empty() {
        let mut pages: Vec<(String, String, String)> = Vec::new();
        for s in &p.shots {
            let at = match pages.iter().position(|x| x.0 == s.page) {
                Some(i) => i,
                None => {
                    pages.push((s.page.clone(), String::new(), String::new()));
                    pages.len() - 1
                }
            };
            if s.width == "phone" {
                pages[at].2 = s.src.clone();
            } else {
                pages[at].1 = s.src.clone();
            }
        }
        return Some(Screens::Pages(pages));
    }
    let why = match p.stage {
        Stage::Triggered | Stage::Deploying => {
            "the preview is still deploying; its pages are captured once it is up".to_string()
        }
        Stage::Ready => "the preview is up and its pages are being captured".to_string(),
        Stage::Failed => {
            "the preview checks failed; its pages are captured once they pass".to_string()
        }
        Stage::Approval => {
            "the preview waits for an environment's approval; its pages are captured once it is up"
                .to_string()
        }
        _ if !p.why.trim().is_empty() => p.why.clone(),
        _ if !visible => "the change has no page a person sees".to_string(),
        _ => return None,
    };
    Some(Screens::Missing(why))
}

/// The checks that cover the change and haven't run on the head.
pub fn preview_gaps(f: &Facts) -> Vec<String> {
    let visible = f
        .files
        .iter()
        .any(|x| crate::preview::is_user_visible(&x.path));
    let covers = f
        .preview
        .as_ref()
        .map(|p| p.covers.as_slice())
        .unwrap_or(&[]);
    f.checks
        .iter()
        .filter(|c| {
            c.outcome == Outcome::DidNotRun
                && (crate::preview::is_covering(covers, &c.name)
                    || crate::preview::skipped_is_gap(&c.name, visible))
        })
        .map(|c| c.name.clone())
        .collect()
}

/// The facts without what the head already settles: an open item or an
/// unverified claim about a kind of check (the lockfile, the tests, the
/// build) whose checks of that kind all passed on the head, and an item
/// the pull request's CI closes once every check on the head passed. The
/// evidence decides, never the item's wording: a fixed "lockfile out of
/// sync" goes because the install check passed on this head.
pub fn settled(f: &Facts) -> Facts {
    let passed_family = |fam: Family| {
        let of: Vec<&CheckFact> = f
            .checks
            .iter()
            .filter(|c| Family::of(&c.name).contains(&fam))
            .collect();
        !of.is_empty() && of.iter().all(|c| c.outcome == Outcome::Passed)
    };
    let settled_by_checks = |text: &str| {
        let fams = Family::of(text);
        !fams.is_empty() && fams.into_iter().all(passed_family)
    };
    let all_passed = f.checks_read
        && !f.checks.is_empty()
        && f.checks
            .iter()
            .all(|c| matches!(c.outcome, Outcome::Passed | Outcome::DidNotRun))
        && f.checks.iter().any(|c| c.outcome == Outcome::Passed);
    let mut out = f.clone();
    out.open
        .retain(|o| !(o.closer == Closer::Ci && all_passed) && !settled_by_checks(&o.text));
    out.unverified.retain(|u| !settled_by_checks(&u.what));
    out
}

pub fn body_of(f: &Facts, ticked: &[String]) -> crate::pr_body::Body {
    use crate::pr_body::{Body, Focus, Level, ReadFirst, Screens, Ticket};
    let settled_facts = settled(f);
    let f = &settled_facts;
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
            // A page is named by its name; its raw id stays in the evidence.
            sentence_part(&crate::pr_review::without_raw_ids(
                s.what
                    .trim_start()
                    .trim_start_matches("Look at")
                    .trim_start(),
            ))
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
    let visible = f
        .files
        .iter()
        .any(|x| crate::preview::is_user_visible(&x.path));
    let screens = match (&f.screens, &f.preview) {
        (Some(s), _) => Some(s.clone()),
        (None, Some(p)) => screens_of(p, visible),
        (None, None) if f.files.iter().any(|x| is_ui(&x.path)) => Some(Screens::Missing(
            "CoderHelm had no runnable preview to capture for this run".into(),
        )),
        (None, None) => None,
    };
    // The full list of checks, and how to check by hand.
    let mut evidence: Vec<String> = Vec::new();
    if let Some(p) = &f.preview {
        if p.url.starts_with("https://") {
            evidence.push(format!("Preview: {}", p.url));
        }
    }
    if !f.checks.is_empty() {
        let covers = f
            .preview
            .as_ref()
            .map(|p| p.covers.as_slice())
            .unwrap_or(&[]);
        let mut s = String::from("Every check on this head:\n");
        for c in &f.checks {
            let gap = c.outcome == Outcome::DidNotRun
                && (crate::preview::is_covering(covers, &c.name)
                    || crate::preview::skipped_is_gap(&c.name, visible));
            s.push_str(&format!(
                "- {}: {}\n",
                linked(c),
                match c.outcome {
                    Outcome::Passed => "passed",
                    Outcome::Failed => "failed",
                    Outcome::Running => "still running",
                    Outcome::DidNotRun if gap => GAP_WORDS,
                    Outcome::DidNotRun => "skipped (its conditions left this PR out)",
                }
            ));
        }
        evidence.push(s.trim_end().to_string());
    }
    if let Some(p) = &f.preview {
        for k in &p.kept_off {
            evidence.push(format!("{}.", k.line()));
        }
    }
    let items = needed_items(f);
    let mut built: Vec<String> = Vec::new();
    // A look at the change is under Review focus, not here.
    for l in f
        .steps
        .iter()
        .filter(|s| !s.needs_person() && !is_look(s))
        .map(crate::pr_steps::Step::line)
        .filter(|l| !l.starts_with("Done:"))
    {
        built.push(format!("- {l}"));
    }
    if items.len() > crate::pr_body::MAX_ITEMS {
        built.push(format!(
            "- Also needed from a person: {}",
            items[crate::pr_body::MAX_ITEMS..]
                .iter()
                .map(|n| sentence_part(n.item.action()))
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
    // Why: the ticket's or finding's own words; for a dependency update
    // without one, the advisories it is about.
    let problem = match (f.why.as_ref().map(|w| one_line(&w.title)), &f.dependency) {
        (Some(t), _) if !t.is_empty() => t,
        (_, Some(d)) => {
            let n = d.advisories.fixed.len() + d.advisories.remaining.len();
            if n > 0 {
                format!(
                    "{} in the dependency lines this updates",
                    plural(n, "known security advisory", "known security advisories")
                )
            } else {
                "Dependency lines behind their current releases".to_string()
            }
        }
        _ => String::new(),
    };
    Body {
        ticket,
        problem,
        how_to_check: one_line(&f.how_to_check),
        head,
        results_of: String::new(),
        alert: String::new(),
        waiting: f.waiting.clone(),
        summary,
        outside,
        needed: items,
        ticked: ticked.to_vec(),
        focus: Focus {
            question: review_question(f),
            conclusion: conclusion_with_signoffs(f),
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
        out_of_scope: f.out_of_scope.clone(),
    }
}

/// What a person has to do, as lines (for logs and counts). Empty when
/// nothing is needed.
pub fn needed_lines(f: &Facts) -> Vec<String> {
    needed_items(f).iter().map(|n| n.item.line()).collect()
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
    fn the_deploy_labels_checks_are_quoted_once_with_their_run() {
        use crate::preview::{CheckResult, Covering, Outcome as P, State};
        let run = |name: &str, outcome: P| CheckResult {
            name: name.into(),
            outcome,
            url: "https://github.com/acme/site/actions/runs/7/job/3".into(),
            environment: String::new(),
            no_jobs: false,
        };
        let preview = |checks: Vec<CheckResult>| State {
            covers: vec![Covering {
                check: "Deploy app".into(),
                workflow: ".github/workflows/deploy.yml".into(),
                label: "CI:DEPLOY_PREVIEW".into(),
                ..Default::default()
            }],
            checks,
            ..Default::default()
        };
        let mut f = ticket_facts();
        f.checks.push(check("Deploy app", Outcome::Passed));
        f.preview = Some(preview(vec![run("Deploy app", P::Passed)]));
        let out = lead(&f);
        // Shown once, as the deploy label's, with its run; not again as CI.
        assert!(
            out.contains("| Deploy app (deploy label) | Passed; [run](https://github.com/acme/site/actions/runs/7/job/3) |"),
            "{out}"
        );
        assert!(!out.contains("| Deploy app (CI)"), "{out}");
        // A later skipped run hid the failure from CI's newest check: the
        // row says failed.
        f.checks.pop();
        f.checks.push(check("Deploy app", Outcome::DidNotRun));
        f.preview = Some(preview(vec![run("Deploy app", P::Failed)]));
        assert!(lead(&f).contains(
            "| Deploy app (deploy label) | Failed: [run](https://github.com/acme/site/actions/runs/7/job/3) |"
        ));
        let mut started = run("Deploy app", P::Failed);
        started.no_jobs = true;
        f.preview = Some(preview(vec![started]));
        assert!(
            lead(&f).contains("| Deploy app (deploy label) | Failed: it failed to start; [run](")
        );
        let mut held = run("Deploy app", P::Waiting);
        held.environment = "preview".into();
        f.preview = Some(preview(vec![held]));
        assert!(lead(&f).contains(
            "| Deploy app (deploy label) | Not run: waiting for approval to deploy to `preview`; [run]("
        ));
        // Kept in sight when the CI checks are too many to list one by one.
        f.preview = Some(preview(vec![run("Deploy app", P::Passed)]));
        f.checks = (0..12)
            .map(|i| check(&format!("unit tests {i}"), Outcome::Passed))
            .collect();
        assert!(rows(&f)
            .iter()
            .any(|r| r.check == "Deploy app (deploy label)"));
        // A label left off is said in the full evidence.
        let mut p = preview(vec![]);
        p.kept_off = vec![crate::deploy_label::KeptOff {
            label: "CI:DEPLOY_PREVIEW".into(),
            by: crate::deploy_label::RemovedBy::Person,
        }];
        f.preview = Some(p);
        assert!(body_of(&f, &[]).evidence.contains(
            "`CI:DEPLOY_PREVIEW` was taken off by a person, so CoderHelm left it off this head."
        ));
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
             ### Problem\n\n\
             Export poller gives up on the first timeout (OPS-41).\n\n\
             ### Changes\n\n\
             Retries the export poller three times before it gives up.<!-- ch:summary-end -->\n\
             - Approach: decide whether this does what OPS-41 asks. CoderHelm's conclusion: all checks on this head passed.\n\
             - Read first: [`src/export/poller.rs`](https://github.com/acme/site/blob/abc1234/src/export/poller.rs) (the retry loop decides when a poll is abandoned).\n\n\
             ### Risk\n\n\
             **Risk:** Low. One module.\n\n\
             **Rollback:** Revert this pull request.\n\n\
             ### Verification\n\n\
             On abc1234 (head of this PR):\n\n\
             | Check | Result |\n\
             |---|---|\n\
             | unit tests (CI) | Passed |\n\
             | lint (CI) | Passed |\n\n\
             How to check: `cargo test -p export`.\n\n\
             <!-- /coderhelm:lead -->"
        );
        assert_eq!(
            crate::pr_body::defects(&out, "abc1234"),
            vec!["no footer".to_string()]
        );
    }

    /// What the head's checks settle leaves the description: a fixed
    /// "lockfile out of sync" goes once the install check passes on the
    /// head, whatever the item's wording; one the head doesn't settle stays.
    #[test]
    fn items_the_head_settles_are_removed_from_the_evidence() {
        let mut f = ticket_facts();
        f.open = vec![
            OpenItem {
                text: "The lockfile is out of sync with the manifest".into(),
                closer: Closer::CoderHelm,
            },
            OpenItem {
                text: "Wait for CI to pass on this head".into(),
                closer: Closer::Ci,
            },
            OpenItem {
                text: "Confirm the vendor's rate limit".into(),
                closer: Closer::Person,
            },
        ];
        f.unverified = vec![Unverified {
            what: "the build was not run".into(),
            why: "no runner".into(),
        }];
        // Before the fix: the install check failed on the head.
        f.checks = vec![
            check("install (frozen lockfile)", Outcome::Failed),
            check("unit tests", Outcome::Passed),
        ];
        let before = settled(&f);
        assert_eq!(before.open.len(), 3, "{:?}", before.open);
        assert_eq!(before.unverified.len(), 1);
        // On the next head both passed, and a build ran.
        f.checks = vec![
            check("install (frozen lockfile)", Outcome::Passed),
            check("unit tests", Outcome::Passed),
            check("build", Outcome::Passed),
        ];
        let after = settled(&f);
        let left: Vec<&str> = after.open.iter().map(|o| o.text.as_str()).collect();
        assert_eq!(left, vec!["Confirm the vendor's rate limit"]);
        assert!(after.unverified.is_empty(), "{:?}", after.unverified);
        // The rendered description no longer shows them.
        let out = lead(&f);
        assert!(!out.contains("out of sync"), "{out}");
        assert!(!out.contains("build was not run"), "{out}");
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
        // What a person must do comes right after Changes, before Risk.
        assert!(
            out.find("### Changes") < out.find("### Needed from you")
                && out.find("### Needed from you") < out.find("### Risk"),
            "{out}"
        );
        assert!(
            out.contains("\n\n### Needed from you\n\n- [ ] <!-- ch:set-the-staging-secret --> **Person who holds the credential**, before merge: Set the staging secret. Why a person: it needs a credential only a person can provide."),
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
        // The reviewer decides the merge; CoderHelm states its own
        // conclusion on the evidence, never asks for that analysis.
        assert!(
            out.contains("Approach: decide whether to merge 2 dependency updates."),
            "{out}"
        );
        assert!(
            out.contains("CoderHelm's conclusion: 1 of 2 major version jumps are shown safe for this code; `tiny-yaml` 1.2.0 to 2.0.0 has no recorded evidence, so it is the risk in this merge"),
            "{out}"
        );
        assert!(!out.contains("Feedback wanted"), "{out}");
        assert!(out.contains("Updates 2 packages: 2 major."), "{out}");
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
