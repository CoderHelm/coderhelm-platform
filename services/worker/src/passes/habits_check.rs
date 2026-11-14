//! After the build, the change is held to the plan's habits (see `habits`).
//!
//! - **Tests that mean something.** Each new or changed test file is run in
//!   the sandbox twice: on the branch, and on the base with only the test
//!   files added (the fix taken out). A test that passes both ways proves
//!   nothing about the change; one that fails before any assertion runs
//!   doesn't show a bug's reported failure. Both go back to the coder, never
//!   to a person. For a bug the two results are the proof the pull request
//!   carries: failing before, passing after. When no test can show it, the
//!   plan's reason and its next best proof are recorded instead.
//! - **Stay inside the plan.** A changed file that is no task of the plan,
//!   no test, lockfile or generated output, and that the coder gave no
//!   reason for, goes back to be restored or justified.
//! - **Dependents.** A dependent the plan said it updates, and the branch
//!   doesn't touch, goes back too.
//!
//! The sandbox runs take up to its full wait, so the check asks for a step
//! of its own when this one has too little time left
//! ([`GateStop::NeedsTime`]), once; a run that still has none records the
//! tests as not run, with why. Like every check on the open pull request it
//! runs once on a head, keeps its rounds in the run's ledger, and what it
//! leaves goes under Open items.

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::habits::{self, is_test_path};
use super::plan::PlanResult;
use super::proof::Finding;
use common::e2e::test_package::{self, Step};

pub const MAX_FIX_ROUNDS: u32 = 2;
/// Test files run on their own; the rest are named as not run.
pub const MAX_TEST_FILES: usize = 8;
/// What the two sandbox runs need of a step: the sandbox's longest wait and
/// room to record the result.
pub const SANDBOX_STEP_SECS: u64 = 760;

pub const START: &str = "<!-- coderhelm:habits -->";
pub const END: &str = "<!-- /coderhelm:habits -->";

/// What running a test with and without the change showed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Fails without the change, passes with it.
    Proven,
    /// Passes with and without the change.
    PassesEitherWay,
    /// Fails without the change before any assertion runs (it can't build
    /// or import there): it shows the code is new, not that the bug existed.
    FailsBeforeAsserting,
    /// Fails with the change too.
    FailsWithChange,
    /// Couldn't be run here.
    NotRun,
}

/// One test file's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResult {
    pub path: String,
    pub verdict: Verdict,
    /// The failure it printed without the change, or why it didn't run.
    #[serde(default)]
    pub detail: String,
}

/// A changed file that is no part of the plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutsidePlan {
    pub path: String,
    /// The coder's reason; empty when it gave none.
    #[serde(default)]
    pub why: String,
}

/// What the check found, for the pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrFacts {
    #[serde(default)]
    pub bug: bool,
    #[serde(default)]
    pub tests: Vec<TestResult>,
    /// Why no test was run at all, when none was.
    #[serde(default)]
    pub no_tests: String,
    #[serde(default)]
    pub outside_plan: Vec<OutsidePlan>,
    /// Dependents the plan updates that the branch doesn't touch.
    #[serde(default)]
    pub not_updated: Vec<String>,
}

/// Why the check didn't give a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateStop {
    /// The sandbox runs need a step of their own.
    NeedsTime,
    /// The check couldn't read the change.
    Why(String),
}

/// One test file's run: its exit code and, when it failed, what it printed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileRun {
    pub code: Option<i32>,
    pub output: String,
}

/// Both runs of the changed test files, in the files' order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Runs {
    pub with: Vec<FileRun>,
    pub without: Vec<FileRun>,
}

/// Where the test files are run with and without the change.
#[async_trait::async_trait]
pub trait Bench: Send + Sync {
    /// Runs each file on its own on the branch and on the base with only
    /// these files added. `Err` says why nothing ran.
    async fn run(&self, files: &[String]) -> Result<Runs, String>;
}

const NOT_FOUND_MARKS: &[&str] = &[
    "no tests found",
    "no test files found",
    "no tests ran",
    "no test suite found",
    "no tests to run",
    "[no test files]",
    "collected 0 items",
];

const BEFORE_ASSERT_MARKS: &[&str] = &[
    "cannot find module",
    "failed to resolve import",
    "does not provide an export named",
    "is not exported",
    "modulenotfounderror",
    "importerror",
    "cannot import name",
    "syntaxerror",
    "error ts",
    "undefined:",
    "has no member named",
    "cannot find symbol",
    "unresolved import",
    "build failed",
];

fn has_any(text: &str, marks: &[&str]) -> bool {
    let low = text.to_ascii_lowercase();
    marks.iter().any(|m| low.contains(m))
}

/// The first line of a failing run that says what failed.
fn excerpt(output: &str) -> String {
    let lines: Vec<&str> = output
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("---"))
        .collect();
    let with_any = |words: &[&str]| {
        lines.iter().find(|l| {
            let low = l.to_ascii_lowercase();
            words.iter().any(|w| low.contains(w))
        })
    };
    let line = with_any(&["expected", "assert", "error", "panic", "received"])
        .or_else(|| with_any(&["fail"]))
        .or(lines.first())
        .copied()
        .unwrap_or("");
    common::truncate_str(line, 200).replace('`', "'")
}

/// What one file's two runs show.
pub fn verdict(with: &FileRun, without: &FileRun) -> (Verdict, String) {
    match (with.code, without.code) {
        (None, _) | (_, None) => (
            Verdict::NotRun,
            "the sandbox gave no result for it".to_string(),
        ),
        (Some(w), _) if w != 0 && has_any(&with.output, NOT_FOUND_MARKS) => (
            Verdict::NotRun,
            "the test command, scoped to this file, found no tests in it".to_string(),
        ),
        (Some(w), _) if w != 0 => (Verdict::FailsWithChange, excerpt(&with.output)),
        (Some(_), Some(0)) => (Verdict::PassesEitherWay, String::new()),
        (Some(_), Some(_)) if has_any(&without.output, NOT_FOUND_MARKS) => (
            Verdict::NotRun,
            "the test command found no tests in it without the change".to_string(),
        ),
        (Some(_), Some(_)) if has_any(&without.output, BEFORE_ASSERT_MARKS) => {
            (Verdict::FailsBeforeAsserting, excerpt(&without.output))
        }
        (Some(_), Some(_)) => (Verdict::Proven, excerpt(&without.output)),
    }
}

/// A repository's unit test command narrowed to one test file, or `None`
/// when CoderHelm can't narrow that runner. Never passes with no tests: a
/// filter that matches nothing must not read as a test that passes.
pub fn scoped_cmd(base_cmd: &str, file: &str) -> Option<String> {
    let safe = !file.is_empty()
        && file
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/'))
        && !file.contains("..");
    if !safe {
        return None;
    }
    let low = base_cmd.to_ascii_lowercase();
    if low.contains("pytest") {
        return Some(format!("{base_cmd} {file}"));
    }
    if low.trim_start().starts_with("go test") {
        let dir = file.rsplit_once('/').map_or(".", |(d, _)| d);
        return Some(format!("go test ./{dir}/"));
    }
    super::build_scoped_test_cmd(base_cmd, &[file.to_string()])
        .map(|c| c.replace(" --passWithNoTests", ""))
}

/// The output of each failing step of a sandbox script, in step order.
pub fn failing_outputs(output: &str, kind: &str) -> Vec<String> {
    let mark = format!("--- {kind} failed (exit ");
    output
        .split(&mark)
        .skip(1)
        .map(|block| {
            let body = block.split_once('\n').map_or("", |(_, b)| b);
            let end = body
                .find(&format!("--- {kind} failed"))
                .or_else(|| body.find(test_package::STEP_MARK))
                .unwrap_or(body.len());
            body[..end].trim().to_string()
        })
        .collect()
}

/// Each step's run from a sandbox script's codes and output: the failing
/// steps' outputs are printed in order.
pub fn file_runs(codes: &[Option<i32>], output: &str, kind: &str) -> Vec<FileRun> {
    let mut failures = failing_outputs(output, kind).into_iter();
    codes
        .iter()
        .map(|c| FileRun {
            code: *c,
            output: match c {
                Some(n) if *n != 0 => failures.next().unwrap_or_default(),
                _ => String::new(),
            },
        })
        .collect()
}

// ── What the diff shows ────────────────────────────────────────────────────

/// The change as the check reads it: every changed path and its status.
pub type Diff = [(String, String, String)];

/// The new and changed test files (never a removed one, never a snapshot).
pub fn changed_tests(diff: &Diff) -> Vec<String> {
    diff.iter()
        .filter(|(p, status, _)| {
            status != "removed" && is_test_path(p) && !p.contains("__snapshots__/")
        })
        .map(|(p, _, _)| p.clone())
        .collect()
}

fn changes_code(diff: &Diff) -> bool {
    diff.iter().any(|(p, _, _)| !is_test_path(p))
}

const OUTSIDE_LINE: &str = "outside the plan:";

/// The coder's reasons for files outside the plan, from its notes
/// (`Outside the plan: <path>: <why>`).
pub fn reasons(notes: &str) -> Vec<(String, String)> {
    notes
        .lines()
        .filter_map(|l| {
            let l = l.trim().trim_start_matches(['-', '*']).trim();
            let at = l.to_ascii_lowercase().find(OUTSIDE_LINE)?;
            let rest = l[at + OUTSIDE_LINE.len()..].trim();
            let (path, why) = rest.split_once(": ")?;
            let path = path.trim().trim_matches('`').trim();
            let why = why.trim();
            (!path.is_empty() && !why.is_empty()).then(|| (path.to_string(), why.to_string()))
        })
        .collect()
}

fn norm(p: &str) -> String {
    super::generators::normalize_path(p)
}

/// The changed files that are no part of the plan: not a task's file (or
/// under a task's directory), not a test, lockfile or settled generated
/// output, not a dependent the plan updates. Each carries the coder's
/// reason when its notes give one.
pub fn outside_plan(
    changed: &[String],
    plan: &PlanResult,
    generated: &[String],
    notes: &str,
) -> Vec<OutsidePlan> {
    let planned: Vec<String> = super::feature::plan_files::planned_files(&plan.tasks)
        .into_iter()
        .map(|f| norm(&f.path))
        .collect();
    // A plan that names no files states no scope to hold the change to.
    if planned.is_empty() {
        return Vec::new();
    }
    let updates: Vec<String> = plan
        .fix
        .as_ref()
        .map(|d| {
            d.habits
                .affected
                .iter()
                .filter(|a| a.updates())
                .map(|a| norm(a.path()))
                .collect()
        })
        .unwrap_or_default();
    let why = reasons(notes);
    changed
        .iter()
        .filter(|c| {
            let c = norm(c);
            let in_plan = planned
                .iter()
                .any(|p| *p == c || (p.ends_with('/') && c.starts_with(p.as_str())));
            !(in_plan
                || updates.contains(&c)
                || is_test_path(&c)
                || common::lockfile_sync::lock_manager(&c).is_some()
                || generated
                    .iter()
                    .any(|g| super::generators::glob_match(g, &c)))
        })
        .map(|c| OutsidePlan {
            path: c.clone(),
            why: why
                .iter()
                .find(|(p, _)| norm(p) == norm(c))
                .map(|(_, w)| w.clone())
                .unwrap_or_default(),
        })
        .collect()
}

/// The dependents of this repository the plan updates that the branch
/// doesn't touch.
pub fn not_updated(changed: &[String], plan: &PlanResult, own_repo: &str) -> Vec<String> {
    let Some(d) = plan.fix.as_ref() else {
        return Vec::new();
    };
    d.habits
        .affected
        .iter()
        .filter(|a| a.updates() && a.repo().is_none_or(|r| r.eq_ignore_ascii_case(own_repo)))
        .map(|a| a.path().to_string())
        .filter(|p| !p.is_empty() && !changed.iter().any(|c| norm(c) == norm(p)))
        .collect()
}

/// Reads the change against the plan and runs the changed tests with and
/// without it.
pub async fn check_once(
    bench: &dyn Bench,
    diff: &Diff,
    plan: &PlanResult,
    own_repo: &str,
    generated: &[String],
    notes: &str,
) -> PrFacts {
    let changed: Vec<String> = diff.iter().map(|(p, _, _)| p.clone()).collect();
    let fix = plan.fix.as_ref();
    let mut facts = PrFacts {
        bug: fix.is_some_and(|d| d.intent.is_bug()),
        outside_plan: outside_plan(&changed, plan, generated, notes),
        not_updated: not_updated(&changed, plan, own_repo),
        ..Default::default()
    };
    let tests = changed_tests(diff);
    if tests.is_empty() {
        facts.no_tests = "the change adds or changes no test file".to_string();
        return facts;
    }
    if !changes_code(diff) {
        facts.no_tests = "the change is only tests, so there is no fix to take out".to_string();
        return facts;
    }
    let (run, extra) = tests.split_at(tests.len().min(MAX_TEST_FILES));
    match bench.run(run).await {
        Ok(runs) => {
            let none = FileRun::default();
            for (i, path) in run.iter().enumerate() {
                let (verdict, detail) = verdict(
                    runs.with.get(i).unwrap_or(&none),
                    runs.without.get(i).unwrap_or(&none),
                );
                facts.tests.push(TestResult {
                    path: path.clone(),
                    verdict,
                    detail,
                });
            }
        }
        Err(why) => {
            for path in run {
                facts.tests.push(TestResult {
                    path: path.clone(),
                    verdict: Verdict::NotRun,
                    detail: why.clone(),
                });
            }
        }
    }
    for path in extra {
        facts.tests.push(TestResult {
            path: path.clone(),
            verdict: Verdict::NotRun,
            detail: format!("only the first {MAX_TEST_FILES} test files are run on their own"),
        });
    }
    facts
}

/// Whether a bug's plan says no failing test can be written.
fn repro_waived(plan: &PlanResult) -> bool {
    plan.fix
        .as_ref()
        .is_some_and(|d| !d.habits.repro.not_feasible.trim().is_empty())
}

/// What the coder must fix, one line each. Empty when the change holds.
pub fn blocking(f: &PrFacts, plan: &PlanResult) -> Vec<String> {
    let mut out = Vec::new();
    let repro = plan
        .fix
        .as_ref()
        .map(|d| d.habits.repro.test.trim().to_string())
        .unwrap_or_default();
    for t in &f.tests {
        match t.verdict {
            Verdict::PassesEitherWay => out.push(format!(
                "`{}` passes with the change taken out, so it doesn't test the change. Make it assert the behaviour the change adds or fixes (it must fail on the base code), through code that exists on the base.",
                t.path
            )),
            Verdict::FailsWithChange => out.push(format!(
                "`{}` fails with the change in place ({}). Fix the code or the test until it passes.",
                t.path, t.detail
            )),
            Verdict::FailsBeforeAsserting if f.bug && (repro.is_empty() || norm(&repro) == norm(&t.path)) => out.push(format!(
                "`{}` fails on the base code before any assertion runs ({}), so it doesn't show the reported failure. Test through an entry point that exists on the base, so the test builds there and fails on its assertion.",
                t.path, t.detail
            )),
            _ => {}
        }
    }
    if f.bug && f.tests.is_empty() && !repro_waived(plan) && f.no_tests.contains("no test file") {
        out.push("This is a bug fix and the change has no test that fails without it. Add the test that reproduces the reported failure (it must fail on the base code and pass with the fix).".to_string());
    }
    for o in f.outside_plan.iter().filter(|o| o.why.is_empty()) {
        out.push(format!(
            "`{}` was changed and is no task of the plan. Put it back with `restore_file` (omit `from_ref`), or, when the change truly needs it, say why in your Evidence on a line `Outside the plan: {}: <why>`.",
            o.path, o.path
        ));
    }
    for p in &f.not_updated {
        out.push(format!(
            "The plan says `{p}` depends on this change and is updated with it, but the branch doesn't touch it. Update it."
        ));
    }
    out
}

/// The fix round's instructions.
pub fn fix_feedback(lines: &[String]) -> String {
    format!(
        "The check after the build ran each new or changed test with and without your change, and compared the changed files with the plan. Fix every item:\n{}\nDo not delete or skip a test to settle an item.",
        lines
            .iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// What is still open after the fix rounds, for Open items.
pub fn open_findings(f: &PrFacts, plan: &PlanResult) -> Vec<Finding> {
    blocking(f, plan)
        .into_iter()
        .map(|l| {
            let (point, settle) = if l.contains("no task of the plan") {
                (
                    "Changed outside the plan",
                    "CoderHelm restores it or records why the change needs it in its next round on this pull request.",
                )
            } else if l.contains("depends on this change") {
                (
                    "Dependent not updated",
                    "CoderHelm updates it in its next round on this pull request.",
                )
            } else {
                (
                    "Test doesn't prove the change",
                    "CoderHelm rewrites the test in its next round on this pull request; CI's result alone doesn't settle it.",
                )
            };
            Finding {
                point: point.to_string(),
                detail: l.split(". ").next().unwrap_or(&l).trim_end_matches('.').to_string() + ".",
                settle: settle.to_string(),
                ..Default::default()
            }
        })
        .collect()
}

fn test_line(t: &TestResult) -> String {
    let detail = |d: &str| {
        if d.is_empty() {
            String::new()
        } else {
            format!(": `{d}`")
        }
    };
    match t.verdict {
        Verdict::Proven => format!(
            "- `{}`: fails without the change{}; passes with it.",
            t.path,
            detail(&t.detail)
        ),
        Verdict::FailsBeforeAsserting => format!(
            "- `{}`: can't build or load without the change{}; passes with it. It covers the new code, it doesn't show a failure on the old.",
            t.path,
            detail(&t.detail)
        ),
        Verdict::PassesEitherWay => format!(
            "- `{}`: passes with and without the change, so it doesn't test it yet.",
            t.path
        ),
        Verdict::FailsWithChange => format!(
            "- `{}`: fails with the change{}.",
            t.path,
            detail(&t.detail)
        ),
        Verdict::NotRun => format!("- `{}`: not run both ways here ({}).", t.path, t.detail),
    }
}

/// The pull request's section: the proof the tests give, what the files'
/// history showed, who the change affects, and what changed outside the
/// plan. Empty when there is nothing to say.
pub fn section(plan: &PlanResult, f: &PrFacts) -> String {
    let mut out = String::new();
    let h = plan.fix.as_ref().map(|d| &d.habits);
    let repro = h.map(|h| &h.repro);
    if !f.tests.is_empty() {
        out.push_str(if f.bug {
            "**Reproduced, then fixed** (each new or changed test run on the base code with only the tests added, then on this branch)\n"
        } else {
            "**The tests fail without the change** (each new or changed test run on the base code with only the tests added, then on this branch)\n"
        });
        for t in &f.tests {
            out.push_str(&test_line(t));
            out.push('\n');
        }
        out.push('\n');
    } else if let Some(r) = repro.filter(|r| f.bug && !r.not_feasible.trim().is_empty()) {
        out.push_str(&format!(
            "**No failing test for this fix:** {}. Proof instead: {}.\n\n",
            r.not_feasible.trim().trim_end_matches('.'),
            r.next_best.trim().trim_end_matches('.')
        ));
    }
    if let Some(h) = h {
        let hist: Vec<&habits::AreaHistory> = h
            .area_history
            .iter()
            .filter(|a| !a.found.trim().is_empty())
            .collect();
        if !hist.is_empty() {
            out.push_str("**History of the touched files**\n");
            for a in hist {
                let refs = if a.refs.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", a.refs.join(", "))
                };
                out.push_str(&format!("- `{}`{refs}: {}", a.path.trim(), a.found.trim()));
                if !a.bearing.trim().is_empty() {
                    out.push_str(&format!(" For this change: {}", a.bearing.trim()));
                }
                out.push('\n');
            }
            out.push('\n');
        }
        let affected: Vec<&habits::Affected> = h
            .affected
            .iter()
            .filter(|a| !a.where_.trim().is_empty())
            .collect();
        if !affected.is_empty() {
            out.push_str("**Who this affects**\n");
            for a in affected {
                let uses = if a.uses.trim().is_empty() {
                    String::new()
                } else {
                    format!(" (uses {})", a.uses.trim())
                };
                let state = if a.action.trim() == habits::UNCHECKED {
                    "found by a search of the team's repositories; the plan didn't settle whether it must change".to_string()
                } else if !a.updates() {
                    format!("safe: {}", a.why.trim())
                } else if f.not_updated.iter().any(|p| norm(p) == norm(a.path())) {
                    "planned to change with this, not changed yet".to_string()
                } else if a.repo().is_some() {
                    format!("changes in its own pull request: {}", a.why.trim())
                } else {
                    "updated in this pull request".to_string()
                };
                out.push_str(&format!("- `{}`{uses}: {state}\n", a.where_.trim()));
            }
            out.push('\n');
        } else if !h.no_outside_use.trim().is_empty() {
            out.push_str(&format!(
                "**Who this affects:** nothing outside this repository ({}).\n\n",
                h.no_outside_use.trim().trim_end_matches('.')
            ));
        }
    }
    if !f.outside_plan.is_empty() {
        out.push_str("**Changed outside the plan's files**\n");
        for o in &f.outside_plan {
            out.push_str(&format!(
                "- `{}`: {}\n",
                o.path,
                if o.why.is_empty() {
                    "no reason recorded yet"
                } else {
                    o.why.as_str()
                }
            ));
        }
        out.push('\n');
    }
    if out.is_empty() {
        return out;
    }
    format!(
        "{START}\n### Before and after the code\n{}\n{END}",
        out.trim_end()
    )
}

/// The section as `body` has it, markers included; empty when it has none.
pub fn current_section(body: &str) -> String {
    match (body.find(START), body.find(END)) {
        (Some(s), Some(e)) if e > s => body[s..e + END.len()].to_string(),
        _ => String::new(),
    }
}

/// The description with the section in place: replaced where it stands,
/// added at the end the first time, taken out when there is none.
pub fn shape_body(body: String, section: &str) -> String {
    let stripped = match (body.find(START), body.find(END)) {
        (Some(s), Some(e)) if e > s => {
            format!("{}{}", &body[..s], &body[e + END.len()..])
        }
        _ => body.clone(),
    };
    if section.is_empty() {
        return if stripped == body {
            body
        } else {
            format!("{}\n", stripped.trim_end())
        };
    }
    match (body.find(START), body.find(END)) {
        (Some(s), Some(e)) if e > s => {
            format!("{}{section}{}", &body[..s], &body[e + END.len()..])
        }
        _ => format!("{}\n\n{section}\n", body.trim_end()),
    }
}

const OUTSIDE_HEADING: &str = "**Changed outside the plan's files**";

/// The description with these files added to the section's "Changed
/// outside the plan's files" list, each with why: the record of the edits a
/// round on the pull request made outside the plan (`write_guard`). A file
/// already listed takes the new reason. `body` unchanged when there is
/// nothing to add.
pub fn with_outside(body: &str, entries: &[(String, String)]) -> String {
    if entries.is_empty() {
        return body.to_string();
    }
    let current = current_section(body);
    let mut inner: Vec<String> = if current.is_empty() {
        vec![
            START.to_string(),
            "### Before and after the code".to_string(),
        ]
    } else {
        current
            .trim_end_matches(END)
            .trim_end()
            .lines()
            .map(str::to_string)
            .collect()
    };
    let heading = match inner.iter().position(|l| l.trim() == OUTSIDE_HEADING) {
        Some(i) => i,
        None => {
            if inner.len() > 2 {
                inner.push(String::new());
            }
            inner.push(OUTSIDE_HEADING.to_string());
            inner.len() - 1
        }
    };
    for (path, why) in entries {
        let line = format!("- `{path}`: {why}");
        let start = format!("- `{path}`:");
        match inner
            .iter()
            .skip(heading)
            .position(|l| l.starts_with(&start))
        {
            Some(i) => inner[heading + i] = line,
            None => inner.push(line),
        }
    }
    inner.push(END.to_string());
    shape_body(body.to_string(), &inner.join("\n"))
}

/// Writes the files a round edited outside the plan's, with why, into the
/// pull request's description ([`with_outside`]). A description that can't
/// be read or saved is left as it is: the reasons are in the run's notes.
pub async fn record_outside(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    entries: &[(String, String)],
) {
    if entries.is_empty() {
        return;
    }
    let body = match github.get_pull_request(owner, repo, number).await {
        Ok(p) => p["body"].as_str().unwrap_or("").to_string(),
        Err(e) => {
            warn!(error = %e, "Could not read the PR to record the files changed outside the plan");
            return;
        }
    };
    let updated = with_outside(&body, entries);
    if updated != body {
        if let Err(e) = super::description::save(github, owner, repo, number, &updated).await {
            warn!(error = %e, "Could not record the files changed outside the plan on the PR");
        }
    }
}

// ── The sandbox ────────────────────────────────────────────────────────────

/// Runs the test files in the sandbox on the branch and on a temporary
/// branch holding the base with only those files.
pub struct SandboxBench<'a> {
    pub runner: super::test_bug::Runner<'a>,
    pub base_branch: &'a str,
    pub branch: &'a str,
}

const KIND: &str = "unit";

#[async_trait::async_trait]
impl Bench for SandboxBench<'_> {
    async fn run(&self, files: &[String]) -> Result<Runs, String> {
        let r = &self.runner;
        let base_cmd = super::detect_ci_test(r.github, r.owner, r.repo, self.base_branch)
            .await
            .ok_or("the repository's CI runs no unit test command CoderHelm can read")?;
        let mut steps: Vec<Step> = Vec::new();
        let tree: Vec<String> = r
            .github
            .get_tree(r.owner, r.repo, self.branch)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|e| e.path)
            .collect();
        let install: Vec<Step> = files
            .first()
            .and_then(|f| test_package::package_of(&tree, f, &|_| None))
            .and_then(|p| test_package::check_steps(&p))
            .map(|s| s.into_iter().filter(|s| s.kind == "install").collect())
            .unwrap_or_default();
        let installs = install.len();
        steps.extend(install);
        for f in files {
            let cmd = scoped_cmd(&base_cmd, f).ok_or_else(|| {
                format!("the test command `{base_cmd}` can't be narrowed to one file")
            })?;
            steps.push(Step { kind: KIND, cmd });
        }
        let head = r
            .github
            .get_ref(r.owner, r.repo, self.branch)
            .await
            .map_err(|e| e.to_string())?;
        let base_sha = r
            .github
            .get_ref(r.owner, r.repo, self.base_branch)
            .await
            .map_err(|e| e.to_string())?;
        let temp = format!("{}-without-change", self.branch);
        // One left by a step that was cut off is in the way of a new one.
        let _ = r.github.delete_branch(r.owner, r.repo, &temp).await;
        r.github
            .create_branch(r.owner, r.repo, &temp, &base_sha)
            .await
            .map_err(|e| format!("a branch for the check couldn't be made ({e})"))?;
        let both = async {
            for f in files {
                let content = r
                    .github
                    .read_file(r.owner, r.repo, f, self.branch)
                    .await
                    .map_err(|e| e.to_string())?;
                let sha = r.github.get_file_sha(r.owner, r.repo, f, &temp).await.ok();
                r.github
                    .write_file(
                        r.owner,
                        r.repo,
                        f,
                        &content,
                        &temp,
                        "test: the changed tests on the code before the change",
                        sha.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let before_sha = r
                .github
                .get_ref(r.owner, r.repo, &temp)
                .await
                .map_err(|e| e.to_string())?;
            // Side by side: each waits on its own sandbox build.
            let (with, without) = tokio::join!(
                r.run_at(&head, &steps, None),
                r.run_at(&before_sha, &steps, None)
            );
            Ok::<_, String>((with?, without?))
        }
        .await;
        if let Err(e) = r.github.delete_branch(r.owner, r.repo, &temp).await {
            warn!(error = %e, branch = %temp, "Could not delete the branch of the test check");
        }
        let ((with_codes, with_out), (without_codes, without_out)) = both?;
        let per_file = |codes: &[Option<i32>], out: &str| {
            let failed_installs = codes.iter().take(installs).any(|c| *c != Some(0));
            let runs = file_runs(&codes[installs.min(codes.len())..], out, KIND);
            (failed_installs, runs)
        };
        let (with_broken, with) = per_file(&with_codes, &with_out);
        let (without_broken, without) = per_file(&without_codes, &without_out);
        if with_broken || without_broken {
            return Err("the package's install failed in the sandbox".to_string());
        }
        Ok(Runs { with, without })
    }
}

/// The check on the open pull request. `Ok(None)` when the plan plans no
/// code. A problem gets a fix round; what is still open after the rounds is
/// in the facts ([`open_findings`]).
pub async fn gate(
    w: &super::proof::Work<'_>,
    evidence: &mut super::proof::RunEvidence,
    usage: &mut crate::models::TokenUsage,
    has_time: impl Fn() -> bool,
    secs_left: impl Fn() -> u64,
) -> Result<Option<PrFacts>, GateStop> {
    let head = w.head().await;
    let standing = w.ledger.update(|l| {
        if !l.habits.unchanged(&head) {
            return None;
        }
        l.habits.reused();
        l.habits_outcome.clone()
    });
    if let Some(standing) = standing {
        return standing.map_err(GateStop::Why);
    }
    let outcome = check_and_fix(w, evidence, usage, has_time, secs_left).await;
    if outcome == Err(GateStop::NeedsTime) {
        return outcome;
    }
    let head = w.head().await;
    let kept = outcome.clone().map_err(|e| match e {
        GateStop::Why(why) => why,
        GateStop::NeedsTime => String::new(),
    });
    w.ledger.update(|l| {
        l.habits.head = head;
        l.habits_outcome = Some(kept);
    });
    w.save_ledger().await;
    outcome
}

/// A bench for a step with no time for the sandbox: nothing runs, and each
/// test says so.
struct NoTime;

#[async_trait::async_trait]
impl Bench for NoTime {
    async fn run(&self, _files: &[String]) -> Result<Runs, String> {
        Err("no step of the run had the time for the two sandbox runs; the repository's CI runs the tests on this pull request".to_string())
    }
}

async fn check_and_fix(
    w: &super::proof::Work<'_>,
    evidence: &mut super::proof::RunEvidence,
    usage: &mut crate::models::TokenUsage,
    has_time: impl Fn() -> bool,
    secs_left: impl Fn() -> u64,
) -> Result<Option<PrFacts>, GateStop> {
    let own_repo = format!("{}/{}", w.msg.repo_owner, w.msg.repo_name);
    if super::feature::plan_files::planned_files(&w.plan.tasks).is_empty() {
        return Ok(None);
    }
    loop {
        let diff = w
            .github
            .compare_files_with_patches(
                &w.msg.repo_owner,
                &w.msg.repo_name,
                &w.msg.base_branch,
                w.branch,
            )
            .await
            .map_err(|e| GateStop::Why(format!("the diff couldn't be read ({e})")))?;
        let paths: Vec<String> = diff.iter().map(|(p, _, _)| p.clone()).collect();
        let work = super::run_end::work_files(&paths, &w.msg.ticket_id);
        let diff: Vec<(String, String, String)> = diff
            .into_iter()
            .filter(|(p, _, _)| work.contains(p))
            .collect();
        let needs_sandbox = !changed_tests(&diff).is_empty() && changes_code(&diff);
        let short = needs_sandbox && secs_left() < SANDBOX_STEP_SECS;
        if short
            && !w
                .ledger
                .update(|l| std::mem::replace(&mut l.habits_waited, true))
        {
            w.save_ledger().await;
            return Err(GateStop::NeedsTime);
        }
        let sandbox = SandboxBench {
            runner: super::test_bug::Runner {
                state: w.state,
                github: w.github,
                owner: &w.msg.repo_owner,
                repo: &w.msg.repo_name,
                team_id: &w.msg.team_id,
                run_id: w.run_id,
                provider: w.provider,
            },
            base_branch: &w.msg.base_branch,
            branch: w.branch,
        };
        let bench: &dyn Bench = if short { &NoTime } else { &sandbox };
        let facts = check_once(
            bench,
            &diff,
            w.plan,
            &own_repo,
            w.generated,
            &evidence.notes,
        )
        .await;
        let blocking = blocking(&facts, w.plan);
        info!(
            run_id = w.run_id,
            tests = facts.tests.len(),
            outside_plan = facts.outside_plan.len(),
            blocking = blocking.len(),
            rounds = w.ledger.update(|l| l.habits.rounds),
            "Check of the change against the plan's habits complete"
        );
        let proven = facts
            .tests
            .iter()
            .filter(|t| t.verdict == Verdict::Proven)
            .count();
        if blocking.is_empty() {
            super::add_progress_note(
                w.state,
                &w.msg.team_id,
                w.run_id,
                &format!(
                    "Tests and scope check passed: {proven} of {} changed test file(s) fail without the change and pass with it; no file changed outside the plan without a reason",
                    facts.tests.len()
                ),
            )
            .await;
            return Ok(Some(facts));
        }
        w.report_findings("habits", blocking.len()).await;
        let head = w.head().await;
        w.ledger.update(|l| l.habits.checked(&head, blocking.len()));
        let licence = if w.ledger.update(|l| l.habits.may_fix(MAX_FIX_ROUNDS)) && has_time() {
            super::stop_judge::cycle_licence(
                w.state,
                w.msg,
                w.run_id,
                "habits_check",
                w.ledger.update(|l| l.habits.rounds),
                MAX_FIX_ROUNDS,
                &super::stop_judge::evidence_lines("tests and scope check", &blocking),
            )
            .await
        } else {
            None
        };
        let Some(licence) = licence else {
            w.save_ledger().await;
            return Ok(Some(facts));
        };
        w.ledger.update(|l| l.habits.start_fix(licence));
        w.save_ledger().await;
        super::add_progress_note(
            w.state,
            &w.msg.team_id,
            w.run_id,
            &format!(
                "Tests and scope check found {} item(s), fixing: {}",
                blocking.len(),
                common::truncate_str(&blocking.join(" "), 600)
            ),
        )
        .await;
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
        let fixed = super::implement::run(
            w.state,
            w.msg,
            w.github,
            w.plan,
            w.branch,
            w.rules,
            w.repo_instructions,
            Some(&fix_feedback(&blocking)),
            "medium",
            w.provider,
            usage,
            w.file_cache,
            Some(w.run_id),
            w.deadline,
            // The plan's files and the dependents it updates are this round's:
            // "update it" names a file the branch doesn't touch.
            super::write_guard::RoundFor::Habits,
        )
        .await;
        w.ledger.update(|l| l.habits.end_fix());
        w.save_ledger().await;
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, super::PASS_CHECKS).await;
        match fixed {
            Ok(r) => evidence.merge(r.evidence),
            Err(e) => {
                warn!(run_id = w.run_id, error = %e, "The fix round of the tests and scope check failed");
                return Ok(Some(facts));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::fix_location::FixDecision;
    use crate::passes::habits::{Affected, AreaHistory, Habits, Repro};
    use crate::passes::triage::TicketIntent;

    fn run(code: i32, output: &str) -> FileRun {
        FileRun {
            code: Some(code),
            output: output.into(),
        }
    }

    pub(crate) struct Fake(pub Result<Runs, String>);

    #[async_trait::async_trait]
    impl Bench for Fake {
        async fn run(&self, _files: &[String]) -> Result<Runs, String> {
            self.0.clone()
        }
    }

    fn plan(tasks: &str, intent: TicketIntent, habits: Habits) -> PlanResult {
        PlanResult {
            proposal: String::new(),
            design: String::new(),
            tasks: tasks.into(),
            spec: String::new(),
            repo_tasks: Vec::new(),
            fix: Some(FixDecision {
                intent,
                habits,
                ..Default::default()
            }),
        }
    }

    fn diff(paths: &[&str]) -> Vec<(String, String, String)> {
        paths
            .iter()
            .map(|p| (p.to_string(), "modified".to_string(), String::new()))
            .collect()
    }

    const TASKS: &str = "- [ ] src/cart/total.test.ts: [modify] Three lines of 0.335\n- [ ] src/cart/total.ts: [modify] Round per line\n";

    #[test]
    fn two_runs_of_one_test_read_as_one_verdict() {
        let pass = run(0, "");
        let assertion = run(
            1,
            "FAIL src/cart/total.test.ts\nAssertionError: expected 1 to be 1.01",
        );
        assert_eq!(
            verdict(&pass, &assertion),
            (
                Verdict::Proven,
                "AssertionError: expected 1 to be 1.01".to_string()
            )
        );
        assert_eq!(verdict(&pass, &pass).0, Verdict::PassesEitherWay);
        let import = run(
            1,
            "Error: Failed to resolve import \"./perLine\" from src/cart/total.test.ts",
        );
        assert_eq!(verdict(&pass, &import).0, Verdict::FailsBeforeAsserting);
        assert_eq!(verdict(&assertion, &assertion).0, Verdict::FailsWithChange);
        // A filter that matched nothing is never a test that failed or passed.
        let none = run(1, "No test files found, exiting with code 1");
        assert_eq!(verdict(&none, &none).0, Verdict::NotRun);
        assert_eq!(verdict(&pass, &none).0, Verdict::NotRun);
        assert_eq!(verdict(&FileRun::default(), &pass).0, Verdict::NotRun);
    }

    #[test]
    fn a_test_command_is_narrowed_to_one_file_and_never_passes_on_nothing() {
        let c = scoped_cmd("pnpm exec vitest run", "src/cart/total.test.ts").unwrap();
        assert_eq!(c, "pnpm exec vitest run total.test.ts");
        assert!(!scoped_cmd("pnpm test", "src/a.test.ts")
            .unwrap()
            .contains("passWithNoTests"));
        assert_eq!(
            scoped_cmd("python -m pytest -q", "tests/test_total.py").as_deref(),
            Some("python -m pytest -q tests/test_total.py")
        );
        assert_eq!(
            scoped_cmd("go test ./...", "pkg/cart/total_test.go").as_deref(),
            Some("go test ./pkg/cart/")
        );
        assert_eq!(scoped_cmd("cargo test", "src/total.rs"), None);
        assert_eq!(scoped_cmd("pytest", "tests/$(id).py"), None);
        assert_eq!(scoped_cmd("pytest", "../outside.py"), None);
    }

    #[test]
    fn each_failing_step_gets_its_own_output() {
        let steps = vec![
            Step {
                kind: KIND,
                cmd: "a".into(),
            },
            Step {
                kind: KIND,
                cmd: "b".into(),
            },
            Step {
                kind: KIND,
                cmd: "c".into(),
            },
        ];
        // The script's own shape: failing steps' tails, then one mark per step.
        assert!(test_package::sandbox_script(&steps, None)
            .contains("--- unit failed (exit $ch_t1) ---"));
        let out = "--- unit failed (exit 1) ---\nexpected 1 to be 1.01\n\n--- unit failed (exit 2) ---\nNo test files found\n\nCODERHELM_TEST_STEP 1 1\nCODERHELM_TEST_STEP 2 0\nCODERHELM_TEST_STEP 3 2\n";
        let codes = test_package::step_codes(out, 3);
        let runs = file_runs(&codes, out, KIND);
        assert_eq!(runs[0], run(1, "expected 1 to be 1.01"));
        assert_eq!(runs[1], run(0, ""));
        assert_eq!(runs[2], run(2, "No test files found"));
    }

    #[tokio::test]
    async fn a_test_that_passes_either_way_goes_back_to_the_coder() {
        let p = plan(TASKS, TicketIntent::Bug, Habits::default());
        let bench = Fake(Ok(Runs {
            with: vec![run(0, "")],
            without: vec![run(0, "")],
        }));
        let d = diff(&["src/cart/total.ts", "src/cart/total.test.ts"]);
        let f = check_once(&bench, &d, &p, "acme/shop-api", &[], "").await;
        assert_eq!(f.tests[0].verdict, Verdict::PassesEitherWay);
        let b = blocking(&f, &p);
        assert_eq!(b.len(), 1);
        assert!(b[0].contains("passes with the change taken out"));
        assert!(fix_feedback(&b).contains("Do not delete or skip a test"));
        let open = open_findings(&f, &p);
        assert_eq!(open[0].point, "Test doesn't prove the change");
        assert!(section(&p, &f).contains("passes with and without the change"));
    }

    #[tokio::test]
    async fn a_bug_fix_carries_the_failing_then_passing_result() {
        let p = plan(
            TASKS,
            TicketIntent::Bug,
            Habits {
                repro: Repro {
                    test: "src/cart/total.test.ts".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let bench = Fake(Ok(Runs {
            with: vec![run(0, "")],
            without: vec![run(1, "AssertionError: expected 1 to be 1.01")],
        }));
        let d = diff(&["src/cart/total.ts", "src/cart/total.test.ts"]);
        let f = check_once(&bench, &d, &p, "acme/shop-api", &[], "").await;
        assert!(blocking(&f, &p).is_empty());
        let s = section(&p, &f);
        assert!(s.starts_with(START) && s.ends_with(END));
        assert!(s.contains("**Reproduced, then fixed**"));
        assert!(s.contains("- `src/cart/total.test.ts`: fails without the change: `AssertionError: expected 1 to be 1.01`; passes with it."));
    }

    #[tokio::test]
    async fn a_bug_test_that_cannot_build_on_the_base_does_not_reproduce() {
        let p = plan(TASKS, TicketIntent::Bug, Habits::default());
        let bench = Fake(Ok(Runs {
            with: vec![run(0, "")],
            without: vec![run(1, "Error: Cannot find module './perLine'")],
        }));
        let d = diff(&["src/cart/total.ts", "src/cart/total.test.ts"]);
        let f = check_once(&bench, &d, &p, "acme/shop-api", &[], "").await;
        assert!(blocking(&f, &p)[0].contains("before any assertion runs"));
        // For new work the same result is a test of new code: noted, not blocking.
        let feature = plan(TASKS, TicketIntent::Feature, Habits::default());
        let f = check_once(&bench, &d, &feature, "acme/shop-api", &[], "").await;
        assert!(blocking(&f, &feature).is_empty());
        assert!(section(&feature, &f).contains("It covers the new code"));
    }

    #[tokio::test]
    async fn a_bug_without_a_test_needs_the_plans_reason_and_next_best_proof() {
        let d = diff(&["config/limits.yml"]);
        let tasks = "- [ ] config/limits.yml: [modify] Raise the limit\n";
        let bare = plan(tasks, TicketIntent::Bug, Habits::default());
        let f = check_once(
            &Fake(Err("unused".into())),
            &d,
            &bare,
            "acme/shop-api",
            &[],
            "",
        )
        .await;
        assert!(blocking(&f, &bare)[0].contains("no test that fails without it"));
        let waived = plan(
            tasks,
            TicketIntent::Bug,
            Habits {
                repro: Repro {
                    not_feasible: "a value in a YAML file no test loads".into(),
                    next_best: "GET /limits returns 50 before and 100 after".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let f = check_once(
            &Fake(Err("unused".into())),
            &d,
            &waived,
            "acme/shop-api",
            &[],
            "",
        )
        .await;
        assert!(blocking(&f, &waived).is_empty());
        let s = section(&waived, &f);
        assert!(s.contains("**No failing test for this fix:** a value in a YAML file no test loads. Proof instead: GET /limits returns 50 before and 100 after."));
    }

    #[tokio::test]
    async fn tests_that_could_not_run_are_said_so_and_block_nothing() {
        let p = plan(TASKS, TicketIntent::Feature, Habits::default());
        let d = diff(&["src/cart/total.ts", "src/cart/total.test.ts"]);
        let f = check_once(&NoTime, &d, &p, "acme/shop-api", &[], "").await;
        assert_eq!(f.tests[0].verdict, Verdict::NotRun);
        assert!(blocking(&f, &p).is_empty());
        assert!(section(&p, &f).contains("not run both ways here (no step of the run had the time"));
        // More files than are run on their own: the rest are named.
        let many: Vec<String> = (0..10).map(|i| format!("src/t{i}.test.ts")).collect();
        let mut paths: Vec<&str> = many.iter().map(String::as_str).collect();
        paths.push("src/cart/total.ts");
        let runs = Runs {
            with: vec![run(0, ""); MAX_TEST_FILES],
            without: vec![run(1, "expected"); MAX_TEST_FILES],
        };
        let f = check_once(&Fake(Ok(runs)), &diff(&paths), &p, "acme/shop-api", &[], "").await;
        assert_eq!(f.tests.len(), 10);
        assert_eq!(
            f.tests
                .iter()
                .filter(|t| t.verdict == Verdict::NotRun)
                .count(),
            2
        );
    }

    #[test]
    fn a_file_outside_the_plan_is_restored_or_carries_its_reason() {
        let p = plan(
            "- [ ] src/cart/total.ts: [modify] Round per line\n- [ ] docs/: [modify] The rounding note\n",
            TicketIntent::Feature,
            Habits {
                affected: vec![Affected { where_: "src/invoice/pdf.ts".into(), action: "update".into(), ..Default::default() }],
                ..Default::default()
            },
        );
        let changed: Vec<String> = [
            "src/cart/total.ts",
            "src/cart/total.test.ts",
            "docs/rounding.md",
            "pnpm-lock.yaml",
            "src/generated/schema.ts",
            "src/invoice/pdf.ts",
            "src/utils/format.ts",
            "src/money.ts",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let notes = "Evidence: none needed\n- Outside the plan: `src/money.ts`: total.ts imports the new roundLine helper from it";
        let out = outside_plan(&changed, &p, &["src/generated/**".to_string()], notes);
        assert_eq!(
            out,
            vec![
                OutsidePlan {
                    path: "src/utils/format.ts".into(),
                    why: String::new()
                },
                OutsidePlan {
                    path: "src/money.ts".into(),
                    why: "total.ts imports the new roundLine helper from it".into()
                },
            ]
        );
        let f = PrFacts {
            outside_plan: out,
            ..Default::default()
        };
        let b = blocking(&f, &p);
        assert_eq!(b.len(), 1);
        assert!(b[0].contains("`src/utils/format.ts` was changed and is no task of the plan"));
        assert_eq!(open_findings(&f, &p)[0].point, "Changed outside the plan");
        let s = section(&p, &f);
        assert!(s.contains("- `src/money.ts`: total.ts imports the new roundLine helper from it"));
        assert!(s.contains("- `src/utils/format.ts`: no reason recorded yet"));
        // A plan that names no files states no scope.
        let open = plan("Do the thing", TicketIntent::Feature, Habits::default());
        assert!(outside_plan(&changed, &open, &[], "").is_empty());
    }

    #[test]
    fn a_dependent_the_plan_updates_must_be_in_the_branch() {
        let p = plan(
            "- [ ] src/cart/total.ts: [modify] x\n- [ ] src/invoice/pdf.ts: [modify] y\n",
            TicketIntent::Feature,
            Habits {
                affected: vec![
                    Affected {
                        where_: "src/invoice/pdf.ts".into(),
                        uses: "cartTotal".into(),
                        action: "update".into(),
                        ..Default::default()
                    },
                    Affected {
                        where_: "acme/storefront:src/checkout.ts".into(),
                        action: "update".into(),
                        why: "reads the per-line total".into(),
                        ..Default::default()
                    },
                    Affected {
                        where_: "src/report.ts".into(),
                        action: "safe".into(),
                        why: "reads `amount`, unchanged".into(),
                        ..Default::default()
                    },
                ],
                area_history: vec![AreaHistory {
                    path: "src/cart/total.ts".into(),
                    found: "Per-line rounding was reverted in #74.".into(),
                    refs: vec!["#74".into()],
                    bearing: "The export changes with it this time.".into(),
                }],
                ..Default::default()
            },
        );
        let changed = vec!["src/cart/total.ts".to_string()];
        let missing = not_updated(&changed, &p, "acme/shop-api");
        assert_eq!(missing, vec!["src/invoice/pdf.ts"]);
        let f = PrFacts {
            not_updated: missing,
            ..Default::default()
        };
        assert!(blocking(&f, &p)[0].contains("the branch doesn't touch it"));
        let s = section(&p, &f);
        assert!(s.contains(
            "- `src/invoice/pdf.ts` (uses cartTotal): planned to change with this, not changed yet"
        ));
        assert!(s.contains("- `acme/storefront:src/checkout.ts`: changes in its own pull request: reads the per-line total"));
        assert!(s.contains("- `src/report.ts`: safe: reads `amount`, unchanged"));
        assert!(s.contains("- `src/cart/total.ts` (#74): Per-line rounding was reverted in #74. For this change: The export changes with it this time."));
        let done = PrFacts::default();
        assert!(section(&p, &done)
            .contains("- `src/invoice/pdf.ts` (uses cartTotal): updated in this pull request"));
    }

    /// The edits a round on the pull request made outside the plan's files
    /// are listed on the pull request with why, and the description around
    /// the section is left as it is.
    #[test]
    fn a_rounds_edits_outside_the_plan_are_listed_on_the_pull_request() {
        let entry = |p: &str, w: &str| (p.to_string(), w.to_string());
        // No section yet: one is added with the list.
        let body = "## Summary\nAdds the gift route.\n";
        let first = with_outside(
            body,
            &[entry(
                "src/lib/metadata.ts",
                "the gift route's metadata is built here",
            )],
        );
        assert!(first.starts_with("## Summary\nAdds the gift route."));
        assert!(first.contains(START) && first.contains(END));
        assert!(first.contains(
            "**Changed outside the plan's files**\n- `src/lib/metadata.ts`: the gift route's metadata is built here"
        ));
        // A section with a list: a new file is added, a listed one takes
        // the new reason, nothing is listed twice.
        let second = with_outside(
            &first,
            &[
                entry("src/nav/links.ts", "named by the review findings"),
                entry("src/lib/metadata.ts", "the title helper moved here"),
            ],
        );
        assert_eq!(
            second
                .matches("**Changed outside the plan's files**")
                .count(),
            1
        );
        assert_eq!(second.matches("`src/lib/metadata.ts`").count(), 1);
        assert!(second.contains("- `src/lib/metadata.ts`: the title helper moved here"));
        assert!(second.contains("- `src/nav/links.ts`: named by the review findings"));
        assert_eq!(second.matches(START).count(), 1);
        // A section that says other things keeps them.
        let with_tests = format!(
            "## Summary\nx\n\n{START}\n### Before and after the code\n**The tests fail without the change**\n- `a.test.ts`: fails without, passes with\n{END}\n"
        );
        let third = with_outside(&with_tests, &[entry("src/a.ts", "the test imports it")]);
        assert!(third.contains("- `a.test.ts`: fails without, passes with"));
        assert!(third
            .contains("**Changed outside the plan's files**\n- `src/a.ts`: the test imports it"));
        // Nothing to add: unchanged.
        assert_eq!(with_outside(body, &[]), body);
    }

    #[test]
    fn the_section_is_replaced_in_place_and_removed_when_empty() {
        let body = "## Summary\nx\n".to_string();
        let one = shape_body(body.clone(), &format!("{START}\none\n{END}"));
        assert!(one.contains("one"));
        let two = shape_body(one.clone(), &format!("{START}\ntwo\n{END}"));
        assert!(two.contains("two") && !two.contains("one"));
        assert_eq!(two.matches(START).count(), 1);
        assert_eq!(
            shape_body(two.clone(), &format!("{START}\ntwo\n{END}")),
            two
        );
        assert_eq!(shape_body(two, ""), body);
        assert_eq!(shape_body(body.clone(), ""), body);
    }
}
