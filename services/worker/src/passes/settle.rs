//! A run settles its own open items before it finishes.
//!
//! A dependency update finished "completed" with five Open items on its pull
//! request. Two said the same thing: the package manager's audit never ran,
//! so the advisory counts were unknown. One asked for the build and test
//! results of the packages an override touched. One quoted a README line
//! that a later round had already rewritten. One asked for an end-to-end
//! checklist on a change with nothing to deploy. Every one of them was
//! CoderHelm's to run or to drop; none needed anything only a person has.
//! The full build had been refused twice in the coder's step for lack of
//! time, and nothing carried it anywhere.
//!
//! The rules:
//!
//! - Every open item is classified ([`classify`]): CoderHelm runs it in its
//!   sandbox (a check of the repository's own, the package manager's audit),
//!   the repository's CI settles it, it does not apply to this run, only a
//!   person has what it needs (a reason on `common::handback`), or it is
//!   CoderHelm's own work left (a fix round's, a deferral's).
//! - What CoderHelm can run, it runs, in a step with a full sandbox window:
//!   when this step has too little time left, the run goes on in a fresh
//!   step at the checks and takes it from there ([`next`]). The real output
//!   is the proof. A passing result closes the item and is quoted on the
//!   pull request; a failing one is acted on: a dependency fix takes out its
//!   newest verified set of bumps and defers them with the cause, any other
//!   run gets a fix round with the failing output.
//! - A change to the dependencies is always audited by its package manager
//!   after the bumps (`common::dep_audit`), whether an item asked or not,
//!   and the counts left are quoted on the pull request.
//! - A check the coder's step was refused for lack of time is owed: it runs
//!   here whether or not anything listed it.
//! - A change to the dependencies is verified with the set a senior
//!   engineer would run, found in the project and not in its CI
//!   (`common::upgrade_quality::verify`): a typed project is type-checked
//!   though no workflow does it. A dependency run records that set against
//!   its head, and this stage cites the record instead of building again
//!   ([`Record::adopt_verified`]); any other change to the dependencies
//!   gets the set's checks added to the repository's own ([`with_min_set`]).
//!   So "check not run" never stays open for a check of the set: it runs.
//! - An item that does not apply to the run is never raised, and items that
//!   name the same missing fact are one item ([`dedup`]).
//! - Open items are derived again whenever the head moves. Each line names
//!   the head it was written on ([`stamp`]); a run about to finish with
//!   lines written on an earlier head checks again first ([`stale`]).
//! - What stays on the pull request says why: an item only a person can
//!   settle names the reason, and an item CoderHelm ran and could not
//!   settle names what it ran and what came back.

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use common::dep_audit;
use common::handback::{human_only_in, HumanOnly};
use common::upgrade_quality::verify as vq;

use super::checks::{CheckKind, Step};
use super::proof::{self, Finding, RunEvidence, SandboxRecord};
use super::triage::TicketIntent;
use crate::models::TokenUsage;

/// Fix rounds a failing settle check gets in one run.
pub const MAX_FIX_ROUNDS: u32 = 2;
/// Fresh steps one run takes to find a full sandbox window.
pub const MAX_STEPS: u32 = 4;
/// Times a run about to finish goes back to derive its open items again.
pub const MAX_REDRIVES: u32 = 3;

// ── Classifying ─────────────────────────────────────────────────────────

/// Something CoderHelm runs in its sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    /// A check of the repository's own, by kind.
    Check(CheckKind),
    /// The package manager's audit of the lockfiles the change touches.
    Audit,
    /// The repository's checks, which install from the lockfile: an item
    /// that says a lockfile is out of sync with its manifest is settled by
    /// them on the head as it is, never carried from an earlier head.
    LockSync,
}

/// What settles an open item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Class {
    /// CoderHelm runs these and reads the result.
    Run(Vec<Action>),
    /// The repository's CI runs it on the pull request.
    Ci,
    /// It does not apply to this run: never raised.
    NotApplicable(&'static str),
    /// Only a person has what it needs.
    Human(HumanOnly),
    /// CoderHelm's own work left: a fix round's gap, a check the change did
    /// not pass, a security item, a deferral with its reason.
    Own,
}

/// What a run is, for classifying its items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Facts {
    pub intent: TicketIntent,
    /// The plan was written by rules (a dependency fix run): no planner
    /// chose where the fix goes or what else production needs.
    pub planned_by_rules: bool,
    /// The change touches a lockfile or a manifest one reads.
    pub dep_change: bool,
    /// The coder's step was refused a check for lack of time.
    pub checks_owed: bool,
}

impl Facts {
    /// Upkeep with no new behaviour: nothing to take to production beyond
    /// the pull request itself.
    fn upkeep(&self) -> bool {
        self.intent == TicketIntent::Chore || self.planned_by_rules
    }
}

/// Words that say a check has no result yet.
const NOT_RUN: &[&str] = &[
    "no check",
    "never ran",
    "not run",
    "did not run",
    "didn't run",
    "hasn't run",
    "has not run",
    "have not run",
    "haven't run",
    "nothing shows",
    "no result",
];

/// The repository's own checks a sentence names, by word. Format, plan and
/// security are left out: as words they mean other things.
fn check_words(text: &str) -> Vec<CheckKind> {
    let mut kinds = Vec::new();
    let words: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    for (i, w) in words.iter().enumerate() {
        let kind = match w.as_str() {
            "build" | "builds" | "compile" | "compiles" => Some(CheckKind::Build),
            "tests" | "test" => Some(CheckKind::Tests),
            "lint" | "linter" => Some(CheckKind::Lint),
            "typecheck" | "type-check" | "typechecks" => Some(CheckKind::Typecheck),
            "type" if words.get(i + 1).is_some_and(|n| n.starts_with("check")) => {
                Some(CheckKind::Typecheck)
            }
            _ => None,
        };
        if let Some(k) = kind {
            if !kinds.contains(&k) {
                kinds.push(k);
            }
        }
    }
    kinds
}

/// Whether a sentence asks for the package manager's audit result: it names
/// an audit or advisories, and a count or a severity of them.
pub fn names_lock_sync(text: &str) -> bool {
    let t = text.to_lowercase();
    (t.contains("lockfile") || t.contains("lock file"))
        && [
            "out of sync",
            "out-of-sync",
            "not in sync",
            "not regenerated",
            "needs regenerating",
            "needs to be regenerated",
            "frozen install",
            "frozen-lockfile",
        ]
        .iter()
        .any(|m| t.contains(m))
}

pub fn names_audit(text: &str) -> bool {
    let t = text.to_lowercase();
    if t.contains("audit log") || t.contains("audit trail") {
        return false;
    }
    let any = |marks: &[&str]| marks.iter().any(|m| t.contains(m));
    any(&["audit", "advisor", "vulnerabilit"])
        && any(&[
            "count",
            "critical",
            "high",
            "severity",
            "remaining",
            "how many",
            "never ran",
            "not run",
        ])
}

fn text_of(f: &Finding) -> String {
    format!("{} {}", f.detail, f.settle)
}

/// What settles `f` in a run with these facts.
pub fn classify(f: &Finding, facts: &Facts) -> Class {
    let point = f.point.trim();
    let text = text_of(f);
    // Whatever wrote it, on whatever head: a lockfile out of sync is a
    // fact about the branch, and CoderHelm checks it on this head.
    if names_lock_sync(&text) {
        return Class::Run(vec![Action::LockSync]);
    }
    match point {
        "ci_pending" => return Class::Ci,
        "manual_check" => return Class::Human(HumanOnly::PersonAction),
        "check_blocked" | "rule_failing" | "security" | "security_unchecked" | "optional_task"
        | "planned_work" => return Class::Own,
        // A step outside the pull request the plan names without an owner
        // is the team's to assign. A checklist nobody wrote, or a step the
        // judge thinks is missing, says nothing for upkeep.
        "end_to_end" => {
            return if facts.upkeep() && !f.detail.starts_with("No owner is named for:") {
                Class::NotApplicable(
                    "upkeep with no new behaviour has nothing to take to production beyond its \
                     pull request",
                )
            } else {
                Class::Human(HumanOnly::ProductDecision)
            };
        }
        p if p == proof::JUDGE_ASSUMPTION => {
            return Class::Human(human_only_in(&text).unwrap_or(HumanOnly::ProductDecision));
        }
        _ => {}
    }
    let kind = CheckKind::parse(&f.check);
    if kind == Some(CheckKind::Security) || names_audit(&text) {
        return if facts.dep_change {
            Class::Run(vec![Action::Audit])
        } else {
            Class::Own
        };
    }
    if point == "unrun_check" {
        return match kind {
            // A plan or preview needs credentials the sandbox never has.
            Some(CheckKind::Plan) => Class::Human(HumanOnly::Credential),
            Some(k) => Class::Run(vec![Action::Check(k)]),
            None => Class::Own,
        };
    }
    if let Some(why) = human_only_in(&text) {
        return Class::Human(why);
    }
    // A gap whose own words say the repository's checks settle it.
    if matches!(point, "assumption" | "external_fact" | "team_rule") {
        let mut kinds = check_words(&f.settle);
        let lower = f.detail.to_lowercase();
        if NOT_RUN.iter().any(|m| lower.contains(m)) {
            for k in check_words(&f.detail) {
                if !kinds.contains(&k) {
                    kinds.push(k);
                }
            }
        }
        if !kinds.is_empty() {
            kinds.sort();
            return Class::Run(kinds.into_iter().map(Action::Check).collect());
        }
    }
    Class::Own
}

/// The words of a sentence worth comparing: lowercase, longer than three
/// letters, without the ones every item shares.
fn content_words(text: &str) -> Vec<String> {
    const COMMON: &[&str] = &[
        "that", "this", "with", "from", "because", "there", "their", "which", "never", "nothing",
        "unknown", "have", "been", "were", "what", "when", "into", "them", "they", "does",
    ];
    let mut out: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|w| w.len() > 3 && !COMMON.contains(&w.as_str()))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Whether two items name the same missing fact: the same finding, the
/// audit, the same check that did not run, or sentences that share most of
/// their words.
fn same_fact(a: &(Finding, Class), b: &(Finding, Class)) -> bool {
    if a.0 == b.0 {
        return true;
    }
    let audit = |c: &Class| matches!(c, Class::Run(r) if r.as_slice() == [Action::Audit]);
    if audit(&a.1) && audit(&b.1) {
        return true;
    }
    let unrun = |x: &(Finding, Class)| {
        matches!(x.0.point.trim(), "unrun_check" | "ci_pending")
            .then(|| CheckKind::parse(&x.0.check))
            .flatten()
    };
    if let (Some(x), Some(y)) = (unrun(a), unrun(b)) {
        return x == y;
    }
    if a.0.point.trim() != b.0.point.trim() {
        return false;
    }
    let (x, y) = (content_words(&a.0.detail), content_words(&b.0.detail));
    if x.len() < 3 || y.len() < 3 {
        return false;
    }
    let shared = x.iter().filter(|w| y.contains(w)).count();
    shared * 10 >= x.len().min(y.len()) * 8
}

/// The items with each missing fact named once: the first one that names
/// it stays.
pub fn dedup(items: Vec<(Finding, Class)>) -> Vec<(Finding, Class)> {
    let mut out: Vec<(Finding, Class)> = Vec::new();
    for item in items {
        if !out.iter().any(|kept| same_fact(kept, &item)) {
            out.push(item);
        }
    }
    out
}

// ── What ran, kept across steps ─────────────────────────────────────────

/// What the settle stage ran and came to, for the whole run. The results
/// are of one head; the counts are the run's.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Record {
    /// The head the results below are of.
    pub head: String,
    /// Each check step's result line (`checks::result_line`).
    pub checks: Vec<String>,
    /// The kinds of check that reported, and those that passed, by name.
    pub ran: Vec<String>,
    pub passed: Vec<String>,
    /// The end of the output of a run with a failing check.
    pub failing: String,
    /// The checks ran on this head (whatever they came to).
    pub checked: bool,
    /// Why the sandbox gave no result for the checks, when it gave none.
    pub no_result: String,
    /// Each lockfile's audit line, as the pull request quotes it.
    pub audits: Vec<String>,
    /// The audit ran on this head and every lockfile reported.
    pub audited: bool,
    /// Why the audit gave no counts, when it gave none.
    pub audit_failed: String,
    /// Fix rounds taken on a failing check.
    pub rounds: u32,
    /// Sets of bumps taken out on a failing check.
    pub reverts: u32,
    /// Fresh steps taken for a sandbox window.
    pub steps: u32,
    /// The kinds that fail on this head and on the base branch too: not
    /// caused by the change, so never fixed or taken out for.
    pub base_fails: Vec<String>,
}

impl Record {
    /// The results are of `head`: another head's are set aside. The run's
    /// counts stay.
    pub fn for_head(&mut self, head: &str) {
        if self.head == head {
            return;
        }
        *self = Record {
            head: head.to_string(),
            rounds: self.rounds,
            reverts: self.reverts,
            steps: self.steps,
            ..Default::default()
        };
    }

    fn passed(&self, k: CheckKind) -> bool {
        self.passed.iter().any(|p| p == k.name())
    }

    fn ran(&self, k: CheckKind) -> bool {
        self.ran.iter().any(|p| p == k.name())
    }

    /// The kinds that ran and failed, lint and format aside: those are
    /// reported, and never hold a run (`checks::Step::advisory`).
    fn failed(&self) -> Vec<CheckKind> {
        [CheckKind::Build, CheckKind::Typecheck, CheckKind::Tests]
            .into_iter()
            .filter(|k| self.ran(*k) && !self.passed(*k))
            .filter(|k| !self.base_fails.iter().any(|b| b == k.name()))
            .collect()
    }

    /// The verification set a dependency run recorded on this head stands
    /// as this stage's check run: its results are cited with the commands
    /// that ran, and the same build is never run twice. A record of
    /// another commit, or one with no result, is not taken. `base`: the
    /// set's results on the base branch, when the run needed them.
    pub fn adopt_verified(&mut self, v: &vq::Verified, base: Option<&vq::Verified>) {
        if self.checked || !v.ran || v.sha != self.head {
            return;
        }
        let kind_of = |k: vq::Kind| match k {
            vq::Kind::Typecheck => Some(CheckKind::Typecheck),
            vq::Kind::Build => Some(CheckKind::Build),
            vq::Kind::Lint => Some(CheckKind::Lint),
            vq::Kind::Tests => Some(CheckKind::Tests),
            _ => None,
        };
        let commit: String = v.sha.chars().take(7).collect();
        // The audit has its own lines here, with its counts.
        for o in v.outcomes.iter().filter(|o| o.kind != vq::Kind::Audit) {
            let name = kind_of(o.kind).map_or(o.kind.name(), CheckKind::name);
            let on_base = base.and_then(|b| b.outcome(o.kind));
            let verdict = match &o.status {
                vq::Status::Passed => "PASS".to_string(),
                vq::Status::Failed { exit } if on_base.is_some_and(|b| b.ran() && !b.passed()) => {
                    format!(
                        "FAIL (exit {exit}) on the base branch too, so not caused by this change"
                    )
                }
                vq::Status::Failed { exit } => format!("FAIL (exit {exit})"),
                vq::Status::TimedOut { after } => {
                    format!("no result (stopped at its time limit of {after} seconds)")
                }
                vq::Status::NotRun => format!("not run ({})", o.why),
            };
            self.checks.push(format!(
                "- {name}: {verdict}, `{}` (the dependency run's verification set, on commit `{commit}`)",
                common::truncate_str(&o.shown, 300)
            ));
            let Some(k) = kind_of(o.kind) else {
                continue;
            };
            if o.ran() && !matches!(o.status, vq::Status::TimedOut { .. }) {
                self.ran.push(k.name().to_string());
                if o.passed() {
                    self.passed.push(k.name().to_string());
                } else if on_base.is_some_and(|b| b.ran() && !b.passed()) {
                    self.base_fails.push(k.name().to_string());
                }
            }
        }
        self.checked = true;
        self.no_result.clear();
    }

    /// A check run's steps and exit codes, recorded.
    pub fn record_checks(&mut self, steps: &[Step], codes: &[Option<i32>], output: &str) {
        let rec = SandboxRecord::from_steps(steps, codes);
        self.checks = steps
            .iter()
            .zip(codes)
            .map(|(s, c)| super::checks::result_line(s, *c))
            .collect();
        self.ran = rec.ran.iter().map(|k| k.name().to_string()).collect();
        self.passed = rec.passed.iter().map(|k| k.name().to_string()).collect();
        self.checked = true;
        self.no_result.clear();
        self.failing = if self.failed().is_empty() {
            String::new()
        } else {
            common::tail_str(output.trim(), 3000).to_string()
        };
    }

    /// An audit run's results, recorded.
    pub fn record_audits(&mut self, read: &[dep_audit::Audited]) {
        self.audits = read.iter().map(dep_audit::Audited::line).collect();
        self.audited = !read.is_empty() && read.iter().all(dep_audit::Audited::ran);
        self.audit_failed = if self.audited {
            String::new()
        } else {
            read.iter()
                .filter(|a| !a.ran())
                .map(dep_audit::Audited::line)
                .collect::<Vec<_>>()
                .join("; ")
        };
    }

    /// The record as the sandbox's own for the proof check and the pull
    /// request: the latest check run is what the branch stands on.
    pub fn write_evidence(&self, evidence: &mut RunEvidence) {
        if self.checked {
            evidence.checks = self.checks.clone();
            let kinds = |names: &[String]| -> Vec<CheckKind> {
                names.iter().filter_map(|n| CheckKind::parse(n)).collect()
            };
            evidence.sandbox = SandboxRecord {
                ran: kinds(&self.ran),
                passed: kinds(&self.passed),
                failure: None,
                refused_for_time: false,
            };
        }
        evidence.audits = self.audits.clone();
    }
}

// ── Deciding ────────────────────────────────────────────────────────────

/// What the settle stage runs for a run's items.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// The repository's checks run (all of its steps: one build).
    pub checks: bool,
    /// The kinds the items ask for.
    pub kinds: Vec<CheckKind>,
    pub audit: bool,
}

/// What to run for `items`: the checks when an item names one or the coder
/// was refused one, the audit when an item names it or the change touches
/// the dependencies.
pub fn plan(items: &[Finding], facts: &Facts) -> Plan {
    // A change to the dependencies is always verified: the set a senior
    // engineer would run, whether or not an item asked for it.
    let mut p = Plan {
        checks: facts.checks_owed || facts.dep_change,
        audit: facts.dep_change,
        ..Default::default()
    };
    for f in items {
        if let Class::Run(actions) = classify(f, facts) {
            for a in actions {
                match a {
                    Action::Audit => p.audit = true,
                    Action::LockSync => p.checks = true,
                    Action::Check(k) => {
                        p.checks = true;
                        if !p.kinds.contains(&k) {
                            p.kinds.push(k);
                        }
                    }
                }
            }
        }
    }
    p
}

/// What the stage does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Run what is planned and has no result on this head.
    Build,
    /// A check fails and the run is a dependency fix with a set to spare:
    /// take out its newest verified set of bumps.
    TakeOutSet,
    /// A check fails: one fix round with the failing output.
    FixRound,
    /// This step has too little time left: go on in a fresh one.
    FreshStep(&'static str),
    /// Nothing more to run: write what is left.
    Done,
}

/// What the step can still do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Room {
    /// A sandbox build fits before the step's deadline.
    pub build: bool,
    /// A fix round fits.
    pub round: bool,
    /// A sandbox exists, and has not given up on this run.
    pub sandbox: bool,
    /// Verified sets of bumps on the branch (a dependency fix run's).
    pub dep_sets: usize,
}

/// Decides the next move from what is planned, what ran and what the step
/// has room for. A check that needs a sandbox build this step has no time
/// for is carried into a fresh step, never left unverified; only a run that
/// has spent its steps writes it down as not run.
pub fn next(plan: &Plan, rec: &Record, room: &Room) -> Next {
    let wants_checks = plan.checks && !rec.checked && rec.no_result.is_empty();
    let wants_audit = plan.audit && !rec.audited && rec.audit_failed.is_empty();
    if (wants_checks || wants_audit) && room.sandbox {
        return if room.build {
            Next::Build
        } else if rec.steps < MAX_STEPS {
            Next::FreshStep("Settling the open items needs a sandbox build with a full time window")
        } else {
            Next::Done
        };
    }
    if plan.checks && rec.checked && !rec.failed().is_empty() {
        if room.dep_sets >= 2 {
            return Next::TakeOutSet;
        }
        if rec.rounds < MAX_FIX_ROUNDS {
            return if room.round {
                Next::FixRound
            } else if rec.steps < MAX_STEPS {
                Next::FreshStep("Fixing the failing check needs more time than this step has left")
            } else {
                Next::Done
            };
        }
    }
    Next::Done
}

/// What the stage came to for the pull request.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Settled {
    /// What stays under Open items, each saying why.
    pub open: Vec<Finding>,
    /// Items a result closed, each with the result that closed it.
    pub closed: Vec<(Finding, String)>,
    /// Items that do not apply to this run, each with why.
    pub dropped: Vec<(Finding, &'static str)>,
}

/// The result that closes an item CoderHelm ran, when every action it
/// needs has one; else why it stays open.
fn result_of(actions: &[Action], rec: &Record) -> Result<String, String> {
    let mut proof = Vec::new();
    for a in actions {
        match a {
            Action::Audit if rec.audited => proof.push(rec.audits.join("; ")),
            Action::Audit if !rec.audit_failed.is_empty() => {
                return Err(format!("CoderHelm ran the audit: {}", rec.audit_failed))
            }
            Action::Audit => {
                return Err(
                    "CoderHelm had no step with a full sandbox window left to run the audit".into(),
                )
            }
            Action::LockSync if rec.checked && !rec.passed.is_empty() => {
                proof.push(format!(
                    "the repository's checks installed from the lockfile and passed on this head ({})",
                    rec.passed.join(", ")
                ));
            }
            Action::LockSync if rec.checked => {
                return Err(
                    "CoderHelm ran the repository's checks on this head and none passed, so \
                     nothing shows the lockfile in sync"
                        .into(),
                )
            }
            Action::LockSync if !rec.no_result.is_empty() => {
                return Err(format!(
                    "CoderHelm's sandbox gave no result: {}",
                    rec.no_result
                ))
            }
            Action::LockSync => {
                return Err(
                    "CoderHelm had no step with a full sandbox window left to install from the \
                     lockfile"
                        .into(),
                )
            }
            Action::Check(k) if rec.passed(*k) => {
                let line = rec
                    .checks
                    .iter()
                    .find(|l| l.contains(k.name()))
                    .cloned()
                    .unwrap_or_else(|| format!("- {}: PASS", k.name()));
                proof.push(line.trim_start_matches("- ").to_string());
            }
            Action::Check(k) if rec.ran(*k) && rec.base_fails.iter().any(|b| b == k.name()) => {
                return Err(format!(
                    "CoderHelm ran {} in its sandbox: it fails on this commit and on the base \
                     branch too, so this change did not cause it",
                    k.name()
                ))
            }
            Action::Check(k) if rec.ran(*k) => {
                return Err(format!(
                    "CoderHelm ran {} in its sandbox and it fails; its fix rounds did not turn it \
                     green",
                    k.name()
                ))
            }
            Action::Check(k) if rec.checked => {
                return Err(format!(
                    "CoderHelm ran the repository's checks in its sandbox and they have no {} step",
                    k.name()
                ))
            }
            Action::Check(_) if !rec.no_result.is_empty() => {
                return Err(format!(
                    "CoderHelm's sandbox gave no result: {}",
                    rec.no_result
                ))
            }
            Action::Check(k) => {
                return Err(format!(
                    "CoderHelm had no step with a full sandbox window left to run {}",
                    k.name()
                ))
            }
        }
    }
    proof.dedup();
    Ok(proof.join("; "))
}

/// The items as they go on the pull request: what does not apply is
/// dropped, what a result settled is closed with that result, the same
/// missing fact is named once, and what stays says why.
pub fn apply(items: Vec<Finding>, facts: &Facts, rec: &Record) -> Settled {
    let mut out = Settled::default();
    let classed: Vec<(Finding, Class)> = items
        .into_iter()
        .map(|f| {
            let c = classify(&f, facts);
            (f, c)
        })
        .collect();
    for (mut f, class) in dedup(classed) {
        match class {
            Class::NotApplicable(why) => out.dropped.push((f, why)),
            Class::Run(actions) => match result_of(&actions, rec) {
                Ok(proof) => out.closed.push((f, proof)),
                Err(why) => {
                    f.needs = why;
                    out.open.push(f);
                }
            },
            Class::Human(why) => {
                f.needs = format!("Only a person can settle this: {}", why.label());
                out.open.push(f);
            }
            // A check a passing sandbox run already answers needs no CI.
            Class::Ci
                if CheckKind::parse(&f.check).is_some_and(|k| {
                    rec.passed(k) || (k == CheckKind::Security && rec.audited)
                }) =>
            {
                let k = CheckKind::parse(&f.check).unwrap_or(CheckKind::Build);
                let action = if k == CheckKind::Security {
                    Action::Audit
                } else {
                    Action::Check(k)
                };
                let proof = result_of(&[action], rec).unwrap_or_default();
                out.closed.push((f, proof));
            }
            Class::Ci | Class::Own => out.open.push(f),
        }
    }
    out
}

/// What the stage settled, for the pull request: each result quoted once.
/// Empty when it ran nothing.
pub fn settled_section(settled: &Settled, rec: &Record) -> String {
    let mut lines: Vec<String> = Vec::new();
    if rec.checked {
        lines.extend(rec.checks.iter().cloned());
    }
    lines.extend(rec.audits.iter().map(|a| format!("- {a}")));
    if lines.is_empty() {
        return String::new();
    }
    let closed = settled.closed.len();
    let commit: String = rec.head.chars().take(7).collect();
    let lead = if closed == 0 {
        format!("CoderHelm ran these in its sandbox on commit `{commit}`:")
    } else {
        format!(
            "CoderHelm ran these in its sandbox on commit `{commit}`; they settle {closed} \
             item{} that would have been open:",
            if closed == 1 { "" } else { "s" }
        )
    };
    format!(
        "{SETTLED_START}\n## Checked by CoderHelm\n{lead}\n{}\n{SETTLED_END}",
        lines.join("\n")
    )
}

pub const SETTLED_START: &str = "<!-- coderhelm:settled -->";
pub const SETTLED_END: &str = "<!-- /coderhelm:settled -->";

// ── Derived again when the head moves ───────────────────────────────────

const ON_HEAD_START: &str = "<!-- coderhelm:on-head ";
const ON_HEAD_END: &str = " -->";

fn is_item(line: &str) -> bool {
    let l = line.trim_start_matches('>').trim_start();
    l.starts_with("- ") || l.starts_with("* ")
}

/// The Open items section with each item naming the head it was written on.
pub fn stamp(section: &str, head: &str) -> String {
    if head.is_empty() {
        return section.to_string();
    }
    section
        .lines()
        .map(|l| {
            if is_item(l) && !l.contains(ON_HEAD_START) {
                format!("{l} {ON_HEAD_START}{head}{ON_HEAD_END}")
            } else {
                l.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The head an item line was written on.
fn head_of(line: &str) -> Option<&str> {
    let at = line.find(ON_HEAD_START)? + ON_HEAD_START.len();
    let rest = &line[at..];
    Some(rest[..rest.find(ON_HEAD_END)?].trim()).filter(|h| !h.is_empty())
}

/// Whether the description's Open items were written on a head other than
/// `head`: a later round may have settled one, so they are derived again.
/// Items written before lines carried a head are left as they are.
pub fn stale(body: &str, head: &str) -> bool {
    if head.is_empty() {
        return false;
    }
    let (owned, _) = super::completeness::take_owned(body);
    owned
        .open_items
        .lines()
        .filter(|l| is_item(l))
        .filter_map(head_of)
        .any(|h| h != head)
}

/// Whether a run about to finish goes back to its checks first: its open
/// items are of another head, and it has gone back fewer than
/// [`MAX_REDRIVES`] times and not for this head.
pub fn redrive_due(body: &str, head: &str, redrives: u32, last_head: &str) -> bool {
    redrives < MAX_REDRIVES && last_head != head && stale(body, head)
}

// ── Running ─────────────────────────────────────────────────────────────

/// How the stage ended in this step.
// One value per step, moved once: the record stays inline so the callers
// that take it apart are unchanged.
#[allow(clippy::large_enum_variant)]
pub enum Ran {
    /// Settled: what goes on the pull request.
    Settled(Box<Settled>, Record),
    /// The run goes on in a fresh step at the checks, with why.
    Again(String),
}

/// The feedback of a fix round on a failing settle check.
pub fn fix_feedback(rec: &Record, asked: &[Finding]) -> String {
    let failed: Vec<&str> = rec.failed().iter().map(|k| k.name()).collect();
    let items: Vec<String> = asked
        .iter()
        .map(|f| format!("- {}", f.detail.trim()))
        .collect();
    format!(
        "Before the run finishes, CoderHelm ran the repository's checks in its sandbox on this \
         branch and {} failed. Fix the cause in the code or the manifests (never by weakening a \
         check), then call run_checks. The results:\n{}\n\nThe end of the output:\n```\n{}\n```{}",
        failed.join(", "),
        rec.checks.join("\n"),
        rec.failing.trim(),
        if items.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nThese open items wait on that result; make what they say true, or correct \
                 the text they quote from the real results:\n{}",
                items.join("\n")
            )
        }
    )
}

/// The facts of the run on `w`'s branch: what its change touches, read from
/// the tree and the diff's paths.
pub async fn facts(
    w: &proof::Work<'_>,
    intent: TicketIntent,
    evidence: &RunEvidence,
) -> (Facts, Vec<dep_audit::Audit>) {
    // A dependency fix run's plan is written by rules.
    let planned_by_rules = common::security_fixes::is_ticket(&w.msg.ticket_id)
        && super::dep_fix::load(w.state, w.msg, w.run_id)
            .await
            .is_some();
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let tree: Vec<String> = match w.github.get_tree(owner, name, w.branch).await {
        Ok(t) => t
            .into_iter()
            .filter(|e| e.entry_type == "blob")
            .map(|e| e.path)
            .collect(),
        Err(_) => Vec::new(),
    };
    let changed =
        super::plan_coverage::changed_files(w.github, owner, name, &w.msg.base_branch, w.branch)
            .await
            .unwrap_or_default();
    let audits = dep_audit::audits_for(&tree, &changed);
    (
        Facts {
            intent,
            planned_by_rules,
            dep_change: !audits.is_empty(),
            checks_owed: evidence.sandbox.refused_for_time,
        },
        audits,
    )
}

fn sandbox<'a>(w: &proof::Work<'a>) -> crate::clients::sandbox::SandboxClient<'a> {
    crate::clients::sandbox::SandboxClient::new(
        &w.state.codebuild,
        &w.state.s3,
        &w.state.logs,
        &w.state.config.sandbox_bucket_name,
        &w.state.config.sandbox_project_name,
    )
}

/// The steps the agent permissions let run, in order.
async fn permitted(w: &proof::Work<'_>, steps: Vec<Step>) -> Vec<Step> {
    let gate = crate::agent::policy::gate(
        w.state,
        w.provider.scope.ctx(common::agent_policy::Agent::Coder),
        w.deadline,
    );
    let mut kept = Vec::new();
    for s in steps {
        if let common::agent_policy::Outcome::Run { .. } = gate
            .check_command(&s.cmd, "Run the repository's checks")
            .await
        {
            kept.push(s);
        }
    }
    kept
}

/// The repository's check steps with the verification set's added: each
/// type check, build, lint and test command `found` names for a kind the
/// steps do not already run. With no step at all, the set's own install
/// comes first. A typed project is then type-checked whatever its CI runs.
pub fn with_min_set(mut steps: Vec<Step>, found: &[vq::Plan]) -> Vec<Step> {
    for plan in found {
        let cd = if plan.dir.is_empty() {
            String::new()
        } else {
            format!("cd '{}' && ", plan.dir)
        };
        if steps.is_empty() {
            if let Some(install) = plan.checks.iter().find(|c| c.kind == vq::Kind::Install) {
                steps.push(Step {
                    kinds: vec![],
                    cmd: format!("{cd}{}", install.cmd),
                    scope: None,
                });
            }
        }
        for c in &plan.checks {
            let kind = match c.kind {
                vq::Kind::Typecheck => CheckKind::Typecheck,
                vq::Kind::Build => CheckKind::Build,
                vq::Kind::Lint => CheckKind::Lint,
                vq::Kind::Tests => CheckKind::Tests,
                _ => continue,
            };
            if steps.iter().any(|s| s.kinds.contains(&kind)) {
                continue;
            }
            steps.push(Step {
                kinds: vec![kind],
                cmd: format!("{cd}{}", c.cmd),
                scope: Some(format!(
                    "found in the project of `{}`, not in CI: {}",
                    plan.lockfile, c.found
                )),
            });
        }
    }
    steps
}

/// Projects whose verification set one settle build adds.
const MAX_MIN_SET_PROJECTS: usize = 2;

/// The verification set of each project whose lockfile the change touches,
/// found in the project's own files on the branch.
async fn min_set(w: &proof::Work<'_>, audits: &[dep_audit::Audit]) -> Vec<vq::Plan> {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let tree: Vec<String> = match w.github.get_tree(owner, name, w.branch).await {
        Ok(t) => t
            .into_iter()
            .filter(|e| e.entry_type == "blob")
            .map(|e| e.path)
            .collect(),
        Err(_) => return vec![],
    };
    let mut out = vec![];
    for a in audits.iter().take(MAX_MIN_SET_PROJECTS) {
        let mut files = vec![];
        for path in vq::files_to_read(&a.lockfile, &tree) {
            if let Ok(Some(text)) = w
                .github
                .read_file_if_exists(owner, name, &path, w.branch)
                .await
            {
                files.push((path, text));
            }
        }
        out.push(vq::discover(&a.lockfile, &tree, &files, None));
    }
    out
}

/// Runs what the plan still lacks on `head`, in the sandbox, and records
/// it. Every path records something, so the stage never asks for the same
/// build twice.
async fn build(
    w: &proof::Work<'_>,
    plan: &Plan,
    audits: &[dep_audit::Audit],
    head: &str,
    rec: &mut Record,
) {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let egress = crate::agent::policy::sandbox_egress(w.state, &w.msg.team_id).await;
    let node = super::detect_node_version(w.github, owner, name, w.branch).await;
    if plan.checks && !rec.checked && rec.no_result.is_empty() {
        let sb = sandbox(w).with_egress(egress.clone());
        let mut steps = super::dep_fix::probe_steps(w.github, w.msg).await;
        if !audits.is_empty() {
            steps = with_min_set(steps, &min_set(w, audits).await);
        }
        let steps = permitted(w, steps).await;
        if steps.is_empty() {
            rec.no_result =
                "the repository has no check command CoderHelm may run in its sandbox".into();
        } else if !sb.has_time(w.deadline) {
            // Decided again by `next`: a fresh step, never a guess.
            return;
        } else {
            let source = sb
                .ensure_source(w.run_id, head, || {
                    w.github.download_tarball(owner, name, head)
                })
                .await;
            match source {
                Err(e) => rec.no_result = format!("the branch couldn't be snapshotted: {e}"),
                Ok(source) => match sb
                    .run_checks(
                        &source,
                        &super::checks::script(&steps),
                        node.as_deref(),
                        None,
                        w.deadline,
                    )
                    .await
                {
                    Ok(o) if o.ran => {
                        crate::clients::sandbox::NoResults::clear(w.run_id);
                        let codes = super::checks::step_results(&o.output, steps.len());
                        if codes.iter().all(Option::is_none) {
                            rec.no_result = "the build ended before a step reported".into();
                        } else {
                            rec.record_checks(&steps, &codes, &o.output);
                        }
                    }
                    Ok(o) => {
                        if o.broken {
                            crate::clients::sandbox::NoResults::record(w.run_id, &o.reason);
                            rec.no_result = o.reason;
                        }
                        // Not started for lack of time: `next` decides.
                    }
                    Err(e) => rec.no_result = format!("the sandbox could not be driven ({e})"),
                },
            }
        }
        info!(
            run_id = w.run_id,
            checked = rec.checked,
            passed = ?rec.passed,
            no_result = %rec.no_result,
            "Settle: the repository's checks"
        );
    }
    if plan.audit && !rec.audited && rec.audit_failed.is_empty() {
        if audits.is_empty() {
            rec.audit_failed = "the change touches no lockfile CoderHelm has an audit for".into();
            return;
        }
        let cmd = dep_audit::command(audits);
        let gate = crate::agent::policy::gate(
            w.state,
            w.provider.scope.ctx(common::agent_policy::Agent::Coder),
            w.deadline,
        );
        if !matches!(
            gate.check_command(&cmd, "Audit the dependencies with their package manager")
                .await,
            common::agent_policy::Outcome::Run { .. }
        ) {
            rec.audit_failed = "the team's agent permissions hold the audit command".into();
            return;
        }
        let mut hosts: Vec<String> = super::generators::lockfile_hosts();
        for a in audits {
            for h in dep_audit::hosts(a.manager) {
                if !hosts.iter().any(|x| x == h) {
                    hosts.push(h.to_string());
                }
            }
        }
        let sb = sandbox(w)
            .with_egress(egress)
            .with_generator_hosts(hosts, vec![cmd.clone()]);
        if !sb.has_time(w.deadline) {
            return;
        }
        let source = sb
            .ensure_source(w.run_id, head, || {
                w.github.download_tarball(owner, name, head)
            })
            .await;
        match source {
            Err(e) => rec.audit_failed = format!("the branch couldn't be snapshotted: {e}"),
            Ok(source) => match sb
                .run_checks(&source, &cmd, node.as_deref(), None, w.deadline)
                .await
            {
                Ok(o) if o.ran => rec.record_audits(&dep_audit::read(audits, &o.output)),
                Ok(o) if o.broken => rec.audit_failed = o.reason,
                Ok(_) => {}
                Err(e) => rec.audit_failed = format!("the sandbox could not be driven ({e})"),
            },
        }
        info!(
            run_id = w.run_id,
            audited = rec.audited,
            audits = ?rec.audits,
            "Settle: the dependency audit"
        );
    }
}

/// The settle stage: classifies `items`, runs what CoderHelm can run, acts
/// on what comes back, and returns what goes on the pull request, or says
/// the run goes on in a fresh step. `evidence` gains what ran.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    w: &proof::Work<'_>,
    items: &[Finding],
    intent: TicketIntent,
    pr_number: u64,
    evidence: &mut RunEvidence,
    usage: &mut TokenUsage,
    has_time: impl Fn() -> bool,
) -> Ran {
    let head = w.head().await;
    let (facts, audits) = facts(w, intent, evidence).await;
    let plan = plan(items, &facts);
    let mut rec = w.ledger.update(|l| {
        l.settle.for_head(&head);
        l.settle.clone()
    });
    let sandbox_on =
        sandbox(w).is_enabled() && crate::clients::sandbox::NoResults::given_up(w.run_id).is_none();
    let save = |rec: &Record| {
        w.ledger.update(|l| l.settle = rec.clone());
    };
    // The verification set a dependency run recorded on this head is this
    // stage's check run: cited, never built again.
    if !rec.checked && common::security_fixes::is_ticket(&w.msg.ticket_id) {
        if let Some(run) = super::dep_fix::load(w.state, w.msg, w.run_id).await {
            if let Some(v) = &run.verify.head {
                rec.adopt_verified(v, run.verify.base.as_ref());
                if rec.checked {
                    save(&rec);
                }
            }
        }
    }
    loop {
        let dep_sets = match common::security_fixes::is_ticket(&w.msg.ticket_id) {
            true => super::dep_fix::load(w.state, w.msg, w.run_id)
                .await
                .map_or(0, |r| r.commits.len()),
            false => 0,
        };
        let room = Room {
            build: sandbox(w).has_time(w.deadline),
            round: has_time(),
            sandbox: sandbox_on,
            dep_sets,
        };
        match next(&plan, &rec, &room) {
            Next::Build => {
                let before = rec.clone();
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    "Settling the open items: running the checks they wait on in the sandbox",
                )
                .await;
                build(w, &plan, &audits, &head, &mut rec).await;
                save(&rec);
                w.save_ledger().await;
                if rec == before {
                    // The window closed between the decision and the build.
                    rec.steps += 1;
                    save(&rec);
                    w.save_ledger().await;
                    if rec.steps <= MAX_STEPS {
                        return Ran::Again(
                            "Settling the open items needs a sandbox build with a full time window"
                                .into(),
                        );
                    }
                    break;
                }
            }
            Next::TakeOutSet => {
                let why = format!(
                    "the checks CoderHelm ran in its sandbox failed with it applied ({}); taken \
                     out so the bumps before it ship",
                    common::truncate_str(
                        &rec.checks
                            .iter()
                            .filter(|l| l.contains("FAIL"))
                            .cloned()
                            .collect::<Vec<_>>()
                            .join("; "),
                        300
                    )
                );
                match super::resume::take_out_dep_set(
                    w.state, w.msg, w.run_id, w.github, w.branch, pr_number, &why,
                )
                .await
                {
                    Some(note) => {
                        rec.reverts += 1;
                        save(&rec);
                        w.save_ledger().await;
                        super::add_progress_note(w.state, &w.msg.team_id, w.run_id, &note).await;
                        return Ran::Again(
                            "A set of bumps came out after a failing check: the checks run again \
                             on what is left"
                                .into(),
                        );
                    }
                    None => {
                        warn!(
                            run_id = w.run_id,
                            "Settle: the newest set of bumps couldn't be taken out"
                        );
                        break;
                    }
                }
            }
            Next::FixRound => {
                rec.rounds += 1;
                save(&rec);
                w.save_ledger().await;
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    "A check the open items wait on fails in the sandbox: fixing it with the real output",
                )
                .await;
                let asked: Vec<Finding> = items
                    .iter()
                    .filter(|f| matches!(classify(f, &facts), Class::Run(_)))
                    .cloned()
                    .collect();
                let fixed = super::implement::run(
                    w.state,
                    w.msg,
                    w.github,
                    w.plan,
                    w.branch,
                    w.rules,
                    w.repo_instructions,
                    Some(&fix_feedback(&rec, &asked)),
                    "medium",
                    w.provider,
                    usage,
                    w.file_cache,
                    Some(w.run_id),
                    w.deadline,
                    super::write_guard::RoundFor::Settle,
                )
                .await;
                match fixed {
                    Ok(r) => evidence.merge(r.evidence),
                    Err(e) => {
                        warn!(run_id = w.run_id, error = %e, "Settle: the fix round failed");
                        break;
                    }
                }
                if w.head().await == head {
                    // The round changed nothing: what failed still fails.
                    break;
                }
                return Ran::Again(
                    "A fix round changed the branch: the checks run again on the new head".into(),
                );
            }
            Next::FreshStep(why) => {
                rec.steps += 1;
                save(&rec);
                w.save_ledger().await;
                return Ran::Again(why.to_string());
            }
            Next::Done => break,
        }
    }
    Ran::Settled(
        Box::new(write_down(w, items, &facts, &rec, evidence).await),
        rec,
    )
}

/// What goes on the pull request from what has run so far, with nothing
/// more run: for a run that can't go on in a fresh step.
pub async fn finish(
    w: &proof::Work<'_>,
    items: &[Finding],
    intent: TicketIntent,
    evidence: &mut RunEvidence,
) -> (Box<Settled>, Record) {
    let head = w.head().await;
    let (facts, _) = facts(w, intent, evidence).await;
    let rec = w.ledger.update(|l| {
        l.settle.for_head(&head);
        l.settle.clone()
    });
    let settled = write_down(w, items, &facts, &rec, evidence).await;
    (Box::new(settled), rec)
}

async fn write_down(
    w: &proof::Work<'_>,
    items: &[Finding],
    facts: &Facts,
    rec: &Record,
    evidence: &mut RunEvidence,
) -> Settled {
    rec.write_evidence(evidence);
    let settled = apply(items.to_vec(), facts, rec);
    info!(
        run_id = w.run_id,
        open = settled.open.len(),
        closed = settled.closed.len(),
        dropped = settled.dropped.len(),
        "Settle: the open items"
    );
    if !settled.closed.is_empty() || !settled.dropped.is_empty() {
        super::add_progress_note(
            w.state,
            &w.msg.team_id,
            w.run_id,
            &format!(
                "Open items: {} settled by a check CoderHelm ran, {} left out as not applying to this run, {} left",
                settled.closed.len(),
                settled.dropped.len(),
                settled.open.len()
            ),
        )
        .await;
    }
    settled
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(point: &str, detail: &str, settle: &str) -> Finding {
        Finding {
            point: point.into(),
            detail: detail.into(),
            settle: settle.into(),
            ..Default::default()
        }
    }

    fn chore() -> Facts {
        Facts {
            intent: TicketIntent::Chore,
            dep_change: true,
            ..Default::default()
        }
    }

    #[test]
    fn an_item_is_run_dropped_or_named_as_a_persons() {
        let facts = chore();
        // A check that did not run is run.
        let mut unrun = f("unrun_check", "Tests never ran before the PR.", "");
        unrun.check = "tests".into();
        assert_eq!(
            classify(&unrun, &facts),
            Class::Run(vec![Action::Check(CheckKind::Tests)])
        );
        // A plan needs credentials the sandbox never has.
        unrun.check = "plan".into();
        assert_eq!(
            classify(&unrun, &facts),
            Class::Human(HumanOnly::Credential)
        );
        // A gap whose own words say the checks settle it.
        let asks = f(
            "assumption",
            "The override moves two parents to a major they never declared.",
            "Confirm with the web build and the tests.",
        );
        assert_eq!(
            classify(&asks, &facts),
            Class::Run(vec![
                Action::Check(CheckKind::Build),
                Action::Check(CheckKind::Tests)
            ])
        );
        // A gap about another system is a fix round's, never a build's.
        let vendor = f(
            "external_fact",
            "The vendor's build id field is assumed to be a string.",
            "Read one payload and quote it.",
        );
        assert_eq!(classify(&vendor, &facts), Class::Own);
        // A source no connection reaches is a person's to grant.
        let access = f(
            "external_fact",
            "The billing table's column is unconfirmed: no connection reaches the warehouse.",
            "",
        );
        assert_eq!(
            classify(&access, &facts),
            Class::Human(HumanOnly::MissingAccess)
        );
        // CI's are CI's.
        assert_eq!(classify(&f("ci_pending", "Lint.", ""), &facts), Class::Ci);
    }

    #[test]
    fn the_audit_is_run_only_for_a_change_to_the_dependencies() {
        let item = f(
            "external_fact",
            "The remaining critical and high advisory counts are unknown.",
            "",
        );
        assert_eq!(classify(&item, &chore()), Class::Run(vec![Action::Audit]));
        let other = Facts::default();
        assert_eq!(classify(&item, &other), Class::Own);
        assert!(!names_audit(
            "The vendor's audit log count is not confirmed."
        ));
        // And it runs after the bumps whether or not an item asks.
        assert!(plan(&[], &chore()).audit);
        assert!(!plan(&[], &other).audit && !plan(&[], &other).checks);
    }

    #[test]
    fn an_end_to_end_checklist_is_not_raised_for_upkeep() {
        let none = f(
            "end_to_end",
            "The plan has no end-to-end checklist.",
            "List every step.",
        );
        assert!(matches!(classify(&none, &chore()), Class::NotApplicable(_)));
        let by_rules = Facts {
            planned_by_rules: true,
            ..Default::default()
        };
        assert!(matches!(
            classify(&none, &by_rules),
            Class::NotApplicable(_)
        ));
        // A feature's missing checklist is the team's to answer.
        let feature = Facts {
            intent: TicketIntent::Feature,
            ..Default::default()
        };
        assert_eq!(
            classify(&none, &feature),
            Class::Human(HumanOnly::ProductDecision)
        );
        // A step the plan itself names without an owner stays, even for upkeep.
        let unowned = f("end_to_end", "No owner is named for: rotate the key", "");
        assert_eq!(
            classify(&unowned, &chore()),
            Class::Human(HumanOnly::ProductDecision)
        );
    }

    #[test]
    fn the_same_missing_fact_is_one_item() {
        let facts = chore();
        let mut a = f(
            "unrun_check",
            "The remaining critical and high advisory counts are unknown because the audit never ran.",
            "Run it and quote the counts.",
        );
        a.check = "security".into();
        let b = f(
            "external_fact",
            "The critical and high counts from the audit are unknown.",
            "",
        );
        let c = f(
            "assumption",
            "The override moves two parents to a major they never declared.",
            "Confirm with the build and the tests.",
        );
        let d = f(
            "assumption",
            "The README says build and tests pass, but no check has run.",
            "Rewrite that line.",
        );
        let classed = |v: Vec<Finding>| -> Vec<(Finding, Class)> {
            v.into_iter()
                .map(|x| {
                    let cl = classify(&x, &facts);
                    (x, cl)
                })
                .collect()
        };
        let out = dedup(classed(vec![a.clone(), b, c.clone(), d.clone()]));
        let kept: Vec<&Finding> = out.iter().map(|(x, _)| x).collect();
        // The two audit items are one; two different facts that both wait
        // on the build stay two.
        assert_eq!(kept, vec![&a, &c, &d]);
        // The same check listed twice is one.
        let mut t1 = f("unrun_check", "Tests never ran.", "");
        t1.check = "tests".into();
        let mut t2 = f(
            "ci_pending",
            "The test suite did not run before the PR.",
            "",
        );
        t2.check = "tests".into();
        assert_eq!(dedup(classed(vec![t1, t2])).len(), 1);
        // Near-identical sentences of one point are one.
        let e1 = f(
            "external_fact",
            "The vendor sends the amount in cents on every order payload.",
            "",
        );
        let e2 = f(
            "external_fact",
            "Unconfirmed: the vendor sends the amount in cents on every order payload",
            "",
        );
        assert_eq!(dedup(classed(vec![e1, e2])).len(), 1);
    }

    fn step(kind: CheckKind, cmd: &str) -> Step {
        Step {
            kinds: vec![kind],
            cmd: cmd.into(),
            scope: None,
        }
    }

    fn green() -> Record {
        let mut r = Record {
            head: "h1".into(),
            ..Default::default()
        };
        r.record_checks(
            &[
                step(CheckKind::Build, "pnpm -r build"),
                step(CheckKind::Tests, "pnpm test"),
            ],
            &[Some(0), Some(0)],
            "ok",
        );
        r
    }

    #[test]
    fn a_passing_result_closes_the_item_with_the_result_as_proof() {
        let asks = f(
            "assumption",
            "The override moves two parents to a major they never declared.",
            "Confirm with the build and the tests.",
        );
        let out = apply(vec![asks.clone()], &chore(), &green());
        assert!(out.open.is_empty());
        assert_eq!(out.closed.len(), 1);
        assert!(out.closed[0].1.contains("build: PASS, `pnpm -r build`"));
        assert!(out.closed[0].1.contains("tests: PASS, `pnpm test`"));
        // Without the result it stays, and says what CoderHelm did.
        let none = Record {
            head: "h1".into(),
            no_result: "the sandbox build ended FAILED and printed no result".into(),
            ..Default::default()
        };
        let out = apply(vec![asks], &chore(), &none);
        assert!(out.closed.is_empty());
        assert!(
            out.open[0].needs.contains("gave no result"),
            "{}",
            out.open[0].needs
        );
    }

    #[test]
    fn items_are_derived_again_from_the_results_of_the_head() {
        let readme = f(
            "assumption",
            "The README says build and tests pass, but no check has run on this branch.",
            "Rewrite that line from the actual results.",
        );
        // On the head the item was raised on, nothing had run.
        let mut rec = Record::default();
        rec.for_head("h1");
        assert_eq!(apply(vec![readme.clone()], &chore(), &rec).open.len(), 1);
        // A later round moved the head and the checks ran there: gone.
        let mut rec = green();
        rec.rounds = 1;
        assert!(apply(vec![readme.clone()], &chore(), &rec).open.is_empty());
        // The head moves again: the old results are set aside, the counts stay.
        rec.for_head("h2");
        assert!(!rec.checked && rec.passed.is_empty());
        assert_eq!(rec.rounds, 1);
        assert_eq!(apply(vec![readme], &chore(), &rec).open.len(), 1);
    }

    #[test]
    fn what_stays_names_why() {
        let facts = Facts {
            intent: TicketIntent::Feature,
            ..Default::default()
        };
        let out = apply(
            vec![
                f("end_to_end", "No owner is named for: rotate the key", ""),
                f("logging", "An INFO log fires per record.", "Lower it."),
            ],
            &facts,
            &Record::default(),
        );
        assert_eq!(out.open.len(), 2);
        assert_eq!(
            out.open[0].needs,
            "Only a person can settle this: a product decision"
        );
        // CoderHelm's own work left is not dressed up as a person's.
        assert!(out.open[1].needs.is_empty());
        let section = proof::gaps_section(&out.open);
        assert!(section.contains("Only a person can settle this: a product decision"));
    }

    fn room() -> Room {
        Room {
            build: true,
            round: true,
            sandbox: true,
            dep_sets: 0,
        }
    }

    #[test]
    fn a_check_without_a_window_goes_to_a_fresh_step_never_unverified() {
        let plan = Plan {
            checks: true,
            kinds: vec![CheckKind::Build],
            audit: true,
        };
        let mut rec = Record::default();
        assert_eq!(next(&plan, &rec, &room()), Next::Build);
        let short = Room {
            build: false,
            ..room()
        };
        assert!(matches!(next(&plan, &rec, &short), Next::FreshStep(_)));
        // Bounded: a run that never finds a window writes it down.
        rec.steps = MAX_STEPS;
        assert_eq!(next(&plan, &rec, &short), Next::Done);
        let out = apply(
            vec![{
                let mut x = f("unrun_check", "Build never ran.", "");
                x.check = "build".into();
                x
            }],
            &chore(),
            &rec,
        );
        assert!(out.open[0]
            .needs
            .contains("no step with a full sandbox window"));
        // No sandbox at all: nothing to wait for.
        let none = Room {
            sandbox: false,
            ..room()
        };
        assert_eq!(next(&plan, &Record::default(), &none), Next::Done);
        // The coder's refused check is owed with no item at all.
        let owed = super::plan(
            &[],
            &Facts {
                checks_owed: true,
                ..Default::default()
            },
        );
        assert!(owed.checks);
        assert_eq!(next(&owed, &Record::default(), &room()), Next::Build);
    }

    #[test]
    fn a_failing_check_is_acted_on() {
        let plan = Plan {
            checks: true,
            kinds: vec![CheckKind::Build],
            audit: false,
        };
        let mut rec = Record::default();
        rec.record_checks(
            &[
                step(CheckKind::Build, "pnpm -r build"),
                step(CheckKind::Lint, "pnpm lint"),
            ],
            &[Some(2), Some(1)],
            "error TS2305: Module has no exported member",
        );
        assert_eq!(
            rec.failed(),
            vec![CheckKind::Build],
            "lint never holds a run"
        );
        // A dependency fix with a set to spare takes its newest set out.
        let dep = Room {
            dep_sets: 3,
            ..room()
        };
        assert_eq!(next(&plan, &rec, &dep), Next::TakeOutSet);
        // Its last set is never taken out: a fix round instead.
        let last = Room {
            dep_sets: 1,
            ..room()
        };
        assert_eq!(next(&plan, &rec, &last), Next::FixRound);
        // Any other run: a fix round with the real output.
        assert_eq!(next(&plan, &rec, &room()), Next::FixRound);
        let feedback = fix_feedback(&rec, &[]);
        assert!(feedback.contains("build: FAIL (exit 2), `pnpm -r build`"));
        assert!(feedback.contains("error TS2305"));
        // No time for the round: a fresh step. Rounds spent: written down.
        let short = Room {
            round: false,
            ..room()
        };
        assert!(matches!(next(&plan, &rec, &short), Next::FreshStep(_)));
        rec.rounds = MAX_FIX_ROUNDS;
        assert_eq!(next(&plan, &rec, &room()), Next::Done);
        let mut item = f("unrun_check", "Build never ran.", "");
        item.check = "build".into();
        let out = apply(vec![item], &Facts::default(), &rec);
        assert!(
            out.open[0].needs.contains("it fails"),
            "{}",
            out.open[0].needs
        );
        // A lint failure alone starts nothing.
        let mut lint = Record::default();
        lint.record_checks(&[step(CheckKind::Lint, "pnpm lint")], &[Some(1)], "x");
        assert_eq!(next(&plan, &lint, &room()), Next::Done);
    }

    /// "The lockfile is out of sync" stayed listed after the lockfile was
    /// regenerated: it was CoderHelm's own item, so nothing checked it
    /// again. It is checked on the head, whoever wrote it.
    #[test]
    fn a_lockfile_out_of_sync_item_is_settled_on_the_head_never_carried() {
        let items = vec![
            f(
                "check_blocked",
                "The lockfile is out of sync with the manifests this pull request changes (pnpm: specifiers in the lockfile don't match).",
                "",
            ),
            f("external_fact", "The vendor's rate limit is not confirmed.", ""),
        ];
        assert_eq!(
            classify(&items[0], &chore()),
            Class::Run(vec![Action::LockSync])
        );
        assert!(plan(&items, &chore()).checks, "the checks run for it");
        // The checks installed and passed on this head: the item closes.
        let mut rec = Record::default();
        rec.for_head("h2");
        rec.checked = true;
        rec.ran = vec!["build".into(), "tests".into()];
        rec.passed = vec!["build".into(), "tests".into()];
        let settled = apply(items.clone(), &chore(), &rec);
        assert_eq!(settled.closed.len(), 1);
        assert!(settled.closed[0]
            .1
            .contains("installed from the lockfile and passed on this head"));
        assert!(settled
            .open
            .iter()
            .all(|o| !o.detail.contains("out of sync")));
        // Nothing ran on this head: it stays, saying why, never as a fact
        // of the earlier head.
        let mut none = Record::default();
        none.for_head("h2");
        let settled = apply(items, &chore(), &none);
        let open = settled
            .open
            .iter()
            .find(|o| o.detail.contains("out of sync"))
            .unwrap();
        assert!(
            open.needs.contains("no step with a full sandbox window"),
            "{}",
            open.needs
        );
        assert!(!names_lock_sync("The lockfile was regenerated by pnpm."));
        assert!(!names_lock_sync(
            "The cache is out of sync with the database."
        ));
    }

    #[test]
    fn items_name_their_head_and_another_head_makes_them_stale() {
        let section = proof::gaps_section(&[f("logging", "An INFO log fires per record.", "")]);
        let on = stamp(&section, "abc123");
        assert!(on.contains("<!-- coderhelm:on-head abc123 -->"));
        assert_eq!(stamp(&on, "def456"), on, "a line is stamped once");
        let body = format!("## Summary\nx\n\n{on}");
        assert!(!stale(&body, "abc123"));
        assert!(stale(&body, "def456"));
        // Items written before lines carried a head are left alone.
        assert!(!stale(&format!("## Summary\nx\n\n{section}"), "def456"));
        assert!(!stale("## Summary\nx", "def456"));
        // Once per head, and bounded.
        assert!(redrive_due(&body, "def456", 0, ""));
        assert!(!redrive_due(&body, "def456", 1, "def456"));
        assert!(!redrive_due(&body, "def456", MAX_REDRIVES, ""));
        assert!(!redrive_due(&body, "abc123", 0, ""));
        // The marks survive the description being put together again.
        let (owned, rest) = crate::passes::completeness::take_owned(&body);
        let again = crate::passes::completeness::assemble(&rest, &owned);
        assert!(stale(&again, "def456"));
    }

    #[test]
    fn the_results_are_quoted_on_the_pull_request() {
        let mut rec = green();
        rec.record_audits(&[dep_audit::Audited {
            manager: "pnpm".into(),
            lockfile: "pnpm-lock.yaml".into(),
            shown: "pnpm audit --json".into(),
            outcome: dep_audit::Outcome::Counts(dep_audit::Counts {
                high: 1,
                other: 2,
                ..Default::default()
            }),
        }]);
        let mut audit = f("unrun_check", "The audit never ran.", "");
        audit.check = "security".into();
        let settled = apply(vec![audit], &chore(), &rec);
        assert!(settled.open.is_empty());
        let section = settled_section(&settled, &rec);
        assert!(section.contains("## Checked by CoderHelm"));
        assert!(section.contains("- build: PASS, `pnpm -r build`"));
        assert!(section.contains(
            "- `pnpm audit --json` on `pnpm-lock.yaml`: 0 critical, 1 high and 2 lower advisories left"
        ));
        assert!(section.contains("they settle 1 item that"));
        assert!(settled_section(&Settled::default(), &Record::default()).is_empty());
        // An audit that gave no report is never read as clean.
        let mut bad = Record::default();
        bad.record_audits(&[dep_audit::Audited {
            manager: "pnpm".into(),
            lockfile: "pnpm-lock.yaml".into(),
            shown: "pnpm audit --json".into(),
            outcome: dep_audit::Outcome::Failed("pnpm exited 1 with no summary".into()),
        }]);
        assert!(!bad.audited);
        let mut audit = f("unrun_check", "The audit never ran.", "");
        audit.check = "security".into();
        let out = apply(vec![audit], &chore(), &bad);
        assert!(out.open[0].needs.contains("gave no report"));
    }

    fn typed_set() -> vq::Plan {
        let tree: Vec<String> = ["package.json", "pnpm-lock.yaml", "tsconfig.json"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        vq::discover(
            "pnpm-lock.yaml",
            &tree,
            &[(
                "package.json".to_string(),
                r#"{"scripts":{"build":"tsc -p .","test":"vitest run"}}"#.to_string(),
            )],
            None,
        )
    }

    #[test]
    fn a_dependency_change_is_always_checked_and_a_recorded_set_is_cited_not_rebuilt() {
        // No item asks: the set runs all the same.
        let p = plan(&[], &chore());
        assert!(p.checks && p.audit);
        let set = typed_set();
        let all: Vec<usize> = (0..set.checks.len()).collect();
        // Install, type check, build pass; the tests fail here and on the
        // base branch.
        let out = "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 1 0\nCODERHELM_VERIFY 2 0\nCODERHELM_VERIFY 3 1\n";
        let head = vq::read(&set, "abc1234def", &all, &[], out);
        let base = vq::read(&set, "base", &all, &[], out);
        let mut rec = Record {
            head: "abc1234def".into(),
            audited: true,
            ..Default::default()
        };
        rec.adopt_verified(&head, Some(&base));
        assert!(rec.checked);
        assert_eq!(rec.passed, vec!["typecheck", "build"]);
        assert_eq!(rec.base_fails, vec!["tests"]);
        // A failure the base branch has too starts no fix round and takes
        // out no set.
        let room = Room {
            build: true,
            round: true,
            sandbox: true,
            dep_sets: 3,
        };
        assert_eq!(next(&p, &rec, &room), Next::Done);
        let mut unrun = f("unrun_check", "The type check never ran.", "");
        unrun.check = "typecheck".into();
        let mut tests_item = f("unrun_check", "Tests never ran.", "");
        tests_item.check = "tests".into();
        let settled = apply(vec![unrun, tests_item], &chore(), &rec);
        assert_eq!(settled.closed.len(), 1);
        assert!(settled.closed[0].1.contains(
            "typecheck: PASS, `npx --no-install tsc --noEmit` (the dependency run's verification set, on commit `abc1234`)"
        ));
        // What stays says what ran and what came back.
        assert_eq!(settled.open.len(), 1);
        assert!(settled.open[0].needs.contains(
            "it fails on this commit and on the base branch too, so this change did not cause it"
        ));
        // The same failure without the base branch failing is the change's.
        let green_base = vq::read(
            &set,
            "base",
            &all,
            &[],
            "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 1 0\nCODERHELM_VERIFY 2 0\nCODERHELM_VERIFY 3 0\n",
        );
        let mut rec = Record {
            head: "abc1234def".into(),
            audited: true,
            ..Default::default()
        };
        rec.adopt_verified(&head, Some(&green_base));
        assert_eq!(next(&p, &rec, &room), Next::TakeOutSet);
        // A record with no result, or of another commit, is never taken.
        let mut rec = Record {
            head: "abc1234def".into(),
            ..Default::default()
        };
        rec.adopt_verified(&vq::no_result(&set, "abc1234def", "timed out"), None);
        assert!(!rec.checked);
    }

    #[test]
    fn the_sets_checks_join_the_repositorys_own_for_the_kinds_it_lacks() {
        let set = typed_set();
        // The repository's steps run its tests; the set adds the type check
        // and the build, and leaves the tests to the repository's command.
        let own = vec![
            Step {
                kinds: vec![],
                cmd: "pnpm install --frozen-lockfile".into(),
                scope: None,
            },
            Step {
                kinds: vec![CheckKind::Tests],
                cmd: "pnpm test -- --ci".into(),
                scope: None,
            },
        ];
        let steps = with_min_set(own, std::slice::from_ref(&set));
        let cmds: Vec<&str> = steps.iter().map(|s| s.cmd.as_str()).collect();
        assert_eq!(
            cmds,
            vec![
                "pnpm install --frozen-lockfile",
                "pnpm test -- --ci",
                "npx --no-install tsc --noEmit",
                "pnpm run build"
            ]
        );
        assert_eq!(
            steps[2].scope.as_deref(),
            Some("found in the project of `pnpm-lock.yaml`, not in CI: `tsconfig.json` is there and no script runs it")
        );
        // With no step of its own, the set's install comes first.
        let steps = with_min_set(vec![], &[set]);
        assert!(steps[0].kinds.is_empty());
        assert!(steps[0].cmd.ends_with("pnpm install --frozen-lockfile"));
        assert_eq!(steps.len(), 4);
    }
}
