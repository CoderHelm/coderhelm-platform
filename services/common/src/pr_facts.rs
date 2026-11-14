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
//!   conclusion,
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
    /// Sign-offs that belong to the reviewer's merge decision (a page to
    /// look at) and decisions that matter for it, from `person_gate`:
    /// carried in the one review line, never listed as chores.
    #[serde(default)]
    pub review_notes: Vec<String>,
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

/// `s` with its first letter upper case: the evidence is its own sentence.
fn sentence_start(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
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

/// What the reviewer is asked to decide, with CoderHelm's own conclusion
/// and the evidence it rests on. The reviewer is asked for the merge
/// decision only, never for CoderHelm's analysis (`person_gate`).
fn review_line(f: &Facts) -> String {
    let decide = match (&f.dependency, &f.why) {
        (Some(d), _) => format!(
            "whether to merge {}",
            plural(d.moved(), "dependency update", "dependency updates")
        ),
        (None, Some(w)) if !w.reference.is_empty() => {
            format!("whether this does what {} asks", w.reference)
        }
        _ => "whether this is right to merge".to_string(),
    };
    let mut s = format!(
        "decide {decide}. CoderHelm's conclusion: {}.",
        conclusion(f)
    );
    if !f.review_notes.is_empty() {
        s.push_str(&format!(
            " Your merge decision also signs off: {}.",
            f.review_notes
                .iter()
                .map(|n| one_line(n).trim_end_matches('.').to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    s
}

/// What CoderHelm concludes from the facts, with what each part rests on.
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
            "{} on `{}`, so it is not ready",
            plural(failing, "check fails", "checks fail"),
            f.short_head()
        ));
    } else if f.all_checks_passed() {
        parts.push(format!(
            "every recorded check passed on `{}`",
            f.short_head()
        ));
    } else if f.checks.is_empty() {
        parts.push(format!("no check has reported on `{}` yet", f.short_head()));
    }
    if parts.is_empty() {
        return match f.risk.as_ref().and_then(|r| r.look_first.as_ref()) {
            Some((path, why)) => format!(
                "nothing is out of the ordinary; the change that matters most is in `{path}` ({})",
                one_line(why).trim_end_matches('.')
            ),
            None => "nothing is out of the ordinary".to_string(),
        };
    }
    parts.join("; ")
}

/// How to check the change, and what the records say of it.
fn check_line(f: &Facts) -> String {
    let passed = f
        .checks
        .iter()
        .filter(|c| c.outcome == Outcome::Passed)
        .count();
    let record = if f.checks.is_empty() {
        format!("no check has reported on `{}` yet", f.short_head())
    } else if f.all_checks_passed() {
        format!(
            "{} passed on `{}`",
            plural(passed, "recorded check", "recorded checks"),
            f.short_head()
        )
    } else {
        format!(
            "{passed} of {} recorded checks passed on `{}`",
            f.checks.len(),
            f.short_head()
        )
    };
    let how = one_line(&f.how_to_check);
    if how.is_empty() {
        format!("{record}.")
    } else {
        format!("{}; {record}.", how.trim_end_matches('.'))
    }
}

/// What a person has to do: the steps that need one, then the open items
/// only a person can close. Empty when nothing is needed.
pub fn needed_lines(f: &Facts) -> Vec<String> {
    let mut out = crate::pr_steps::needed(&f.steps);
    // Every item through the gate: only what a person alone can do.
    for o in f.open.iter().filter(|o| {
        o.closer == Closer::Person
            && matches!(
                crate::person_gate::line_outcome(crate::person_gate::Kind::OpenItem, &o.text),
                crate::person_gate::Outcome::Human(_)
            )
    }) {
        out.push(format!(
            "{} (a person)",
            crate::pr_steps::plain(&o.text).trim_end_matches('.')
        ));
    }
    out
}

/// How many lines the part a reader sees first may take: about ten for an
/// ordinary change, a line more for each thing that is out of the ordinary
/// or needs a person.
pub fn visible_budget(f: &Facts) -> usize {
    10 + anomalies(f).len().min(MAX_ANOMALIES + 1)
        + needed_lines(f).len().min(crate::pr_steps::MAX_NEEDED + 1)
}

/// What a reader sees first, rendered from the facts in this order and from
/// nothing else: one standalone line saying the change, how to check it,
/// the risk, what the reader has to do (or that nothing is needed), what is
/// asked of the reviewer, and the anomalies with their evidence.
pub fn lead(f: &Facts) -> String {
    let all = anomalies(f);
    let mut s = String::new();
    s.push_str(LEAD_START);
    s.push('\n');
    let summary = one_line(&f.summary);
    if !summary.is_empty() {
        s.push_str(&summary);
        s.push_str("\n\n");
    }
    s.push_str(&format!("**Check:** {}\n", check_line(f)));
    if let Some(r) = f.risk.as_ref().filter(|r| !r.level.trim().is_empty()) {
        let why = one_line(&r.why);
        s.push_str(&format!(
            "**Risk:** **{}**{}\n",
            r.level.trim(),
            if why.is_empty() {
                ".".to_string()
            } else {
                format!(": {}.", why.trim_end_matches('.'))
            }
        ));
    }
    let needed = needed_lines(f);
    if needed.is_empty() {
        s.push_str(&format!(
            "**Needed from you:** {}\n",
            crate::pr_steps::NOTHING_NEEDED
        ));
    } else {
        s.push_str("**Needed from you:**\n");
        for (i, l) in needed.iter().take(crate::pr_steps::MAX_NEEDED).enumerate() {
            s.push_str(&format!("{}. {l}\n", i + 1));
        }
        if needed.len() > crate::pr_steps::MAX_NEEDED {
            s.push_str(&format!(
                "{}. and {} more, under Open items below.\n",
                crate::pr_steps::MAX_NEEDED + 1,
                needed.len() - crate::pr_steps::MAX_NEEDED
            ));
        }
    }
    s.push_str(&format!("**Review:** {}\n", review_line(f)));
    if !all.is_empty() {
        s.push_str("**Look at first:**\n");
        for a in all.iter().take(MAX_ANOMALIES) {
            s.push_str(&format!(
                "- {}. {}.\n",
                a.what,
                sentence_start(a.evidence.trim_end_matches('.'))
            ));
        }
        if all.len() > MAX_ANOMALIES {
            let more = all.len() - MAX_ANOMALIES;
            if f.run_url.is_empty() {
                s.push_str(&format!("- and {more} more, in the sections below.\n"));
            } else {
                s.push_str(&format!(
                    "- and {more} more: [the full list is on the run page]({}).\n",
                    f.run_url
                ));
            }
        }
    }
    for n in &f.notes {
        s.push_str(&format!("\n{}\n", one_line(n)));
    }
    s.push_str(LEAD_END);
    s
}

/// The lead's first line as [`lead`] writes it (what `pr_layout` takes out
/// of a section that says it again).
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
    fn the_lead_says_the_change_the_check_the_risk_and_what_is_needed() {
        let mut f = ticket_facts();
        f.how_to_check = "`cargo test -p export`".into();
        let out = lead(&f);
        assert_eq!(
            out,
            "<!-- coderhelm:lead -->\n\
             Retries the export poller three times before it gives up.\n\n\
             **Check:** `cargo test -p export`; 2 recorded checks passed on `abc1234`.\n\
             **Risk:** **Low**: one module.\n\
             **Needed from you:** Nothing.\n\
             **Review:** decide whether this does what OPS-41 asks. CoderHelm's conclusion: every recorded check passed on `abc1234`.\n\
             <!-- /coderhelm:lead -->"
        );
        assert!(out.lines().count() <= visible_budget(&f));
    }

    #[test]
    fn nothing_is_called_checked_that_no_record_backs() {
        let mut f = ticket_facts();
        f.checks = vec![];
        let out = lead(&f);
        assert!(
            out.contains("**Check:** no check has reported on `abc1234` yet."),
            "{out}"
        );
        assert!(
            out.contains("- no check has reported on `abc1234`"),
            "{out}"
        );
        f.checks = vec![
            check("unit tests", Outcome::Failed),
            check("lint", Outcome::Passed),
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
        assert!(out.contains("1 of 2 recorded checks passed"), "{out}");
        assert!(out.contains("failed on `abc1234`"), "{out}");
        assert!(out.contains("https://ci.example.com/unit-tests"), "{out}");
        assert!(
            out.contains("**Needed from you:**\n1. Set the staging secret (a person)\n"),
            "{out}"
        );
        assert!(
            out.contains(
                "not verified: the export against the live bucket. The sandbox has no credentials"
            ),
            "{out}"
        );
        // What a person must do is in the first five lines.
        let first: Vec<&str> = out.lines().filter(|l| !l.is_empty()).take(6).collect();
        assert!(
            first.iter().any(|l| l.starts_with("**Needed from you:**")),
            "{first:?}"
        );
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
                    remaining: vec![],
                },
                DepLine {
                    package: "tiny-yaml".into(),
                    from: "1.2.0".into(),
                    to: "2.0.0".into(),
                    moved: true,
                    major: true,
                    remaining: vec![],
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
            out.contains(
                "`glob-walk` 2.1.0 to 3.0.1 crosses a major version. 3.0 drops the sync API"
            ),
            "{out}"
        );
        assert!(
            out.contains("`tiny-yaml` 1.2.0 to 2.0.0 crosses a major version. No safety evidence is recorded"),
            "{out}"
        );
        assert!(
            out.contains("CoderHelm's conclusion: 1 of 2 major version jumps are shown safe for this code; `tiny-yaml` 1.2.0 to 2.0.0 has no recorded evidence, so it is the risk in this merge"),
            "{out}"
        );
        assert!(!out.contains("Feedback wanted"), "{out}");
        assert!(
            out.contains("whether to merge 2 dependency updates"),
            "{out}"
        );
    }

    #[test]
    fn a_long_list_of_anomalies_points_at_the_run_page() {
        let mut f = ticket_facts();
        f.run_url = "https://app.example.com/runs/detail?id=run-1".into();
        f.unverified = (0..20)
            .map(|i| Unverified {
                what: format!("thing {i}"),
                why: "not run".into(),
            })
            .collect();
        let out = lead(&f);
        assert_eq!(out.matches("\n- ").count(), MAX_ANOMALIES + 1);
        assert!(
            out.contains("and 12 more: [the full list is on the run page](https://app.example.com/runs/detail?id=run-1)"),
            "{out}"
        );
        assert!(out.lines().count() <= visible_budget(&f), "{out}");
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
