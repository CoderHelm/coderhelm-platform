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
//! - **Scope decided once.** What the run's scope ledger put out of scope
//!   (`common::scope_ledger`) and the change brings back, in code or in a
//!   test file (tests are exempt from "inside the plan", never from the
//!   ledger), goes back to be taken out, unless a `Scope change:` line
//!   cites an acceptance criterion or a person's comment.
//!
//! The sandbox runs take up to its full wait. They have a budget of their
//! own, never the leftovers of the coding step: when this step has too
//! little time left the check asks for a step of its own
//! ([`GateStop::NeedsTime`]) and records the runs as owed; the next step
//! runs them first, before any other check ([`run_owed`]), and the check
//! reads their recorded result ([`RecordedRuns`]). A run asks at most
//! [`MAX_PROOF_STEPS`] times; only past that are the tests recorded as not
//! run, with why. Seven runs had recorded "no step of the run had the time"
//! for every changed test: the fresh step ran every other check first and
//! came to these runs short again, and the one request it had was spent.
//!
//! The runs install the way the repository's CI does: in the CI job that
//! runs the files' tests (`common::test_env`, cached per manifest blob), or,
//! when CI installs nothing CoderHelm can read, each test file's package
//! (a workspace member from the workspace's root). Each test file runs with
//! the command CI's test step runs, narrowed to it, else its own package's
//! test script (`common::test_env::narrow`). Why a test didn't run is kept
//! with it ([`NotRunWhy`]) and counted on the run ([`TestsRan`]).
//!
//! Like every check on the open pull request it runs once on a head, keeps
//! its rounds in the run's ledger, and what it leaves goes under Open items.

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
/// How many steps of its own the sandbox runs may ask for in a run: one per
/// head a fix round leaves, and room for a step that was cut short.
pub const MAX_PROOF_STEPS: u32 = 4;

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
    /// Why it didn't run, as a kind to count (`None` when it ran).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<NotRunWhy>,
}

/// Why a changed test wasn't run with and without the change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotRunWhy {
    /// The dependencies' install failed in the sandbox.
    InstallFailed,
    /// No step had the time for the two sandbox runs.
    NoTime,
    /// No command CoderHelm can read runs the file on its own.
    NoCommand,
    /// An end-to-end spec: it runs against a preview, not in the sandbox.
    EndToEnd,
    /// The runner found no test in the file.
    NoTestsFound,
    /// More test files than are run on their own.
    TooMany,
    /// The sandbox gave no result, or something else stopped it.
    Other,
}

/// Why nothing ran: the install failed in the sandbox.
pub const INSTALL_FAILED: &str = "the package's install failed in the sandbox";
/// Why nothing ran: past [`MAX_PROOF_STEPS`] steps, still no time.
pub const NO_TIME: &str = "no step of the run had the time for the two sandbox runs";

impl NotRunWhy {
    /// The kind of a not-run detail line.
    pub fn of(detail: &str) -> NotRunWhy {
        let d = detail.to_ascii_lowercase();
        if d.contains("install") && d.contains("fail") {
            NotRunWhy::InstallFailed
        } else if d.contains("had the time") {
            NotRunWhy::NoTime
        } else if d.contains("end-to-end spec") {
            NotRunWhy::EndToEnd
        } else if d.contains("found no tests") {
            NotRunWhy::NoTestsFound
        } else if d.contains("only the first") {
            NotRunWhy::TooMany
        } else if d.contains("no unit test command")
            || d.contains("can't be narrowed")
            || d.contains("doesn't run `")
        {
            NotRunWhy::NoCommand
        } else {
            NotRunWhy::Other
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            NotRunWhy::InstallFailed => "install failed",
            NotRunWhy::NoTime => "no time",
            NotRunWhy::NoCommand => "no test command",
            NotRunWhy::EndToEnd => "end-to-end spec",
            NotRunWhy::NoTestsFound => "no tests found",
            NotRunWhy::TooMany => "past the file limit",
            NotRunWhy::Other => "other",
        }
    }
}

/// Whether the changed tests ran with and without the change, and why not:
/// the run's record of it (the ledger keeps the check's facts).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestsRan {
    /// Changed test files.
    pub files: usize,
    /// Of them, run both ways.
    pub ran: usize,
    /// The rest, by why.
    #[serde(default)]
    pub not_run: Vec<(NotRunWhy, usize)>,
}

impl TestsRan {
    pub fn of(tests: &[TestResult]) -> TestsRan {
        let mut not_run: Vec<(NotRunWhy, usize)> = Vec::new();
        for t in tests.iter().filter(|t| t.verdict == Verdict::NotRun) {
            let why = t.why.unwrap_or_else(|| NotRunWhy::of(&t.detail));
            match not_run.iter_mut().find(|(w, _)| *w == why) {
                Some((_, n)) => *n += 1,
                None => not_run.push((why, 1)),
            }
        }
        let n: usize = not_run.iter().map(|(_, n)| n).sum();
        TestsRan {
            files: tests.len(),
            ran: tests.len() - n,
            not_run,
        }
    }

    /// One line: "Tests run with and without the change: 2 of 3 (not run:
    /// 1 install failed)". Empty with no test files.
    pub fn line(&self) -> String {
        if self.files == 0 {
            return String::new();
        }
        let why = if self.not_run.is_empty() {
            String::new()
        } else {
            format!(
                " (not run: {})",
                self.not_run
                    .iter()
                    .map(|(w, n)| format!("{n} {}", w.label()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        format!(
            "Tests run with and without the change: {} of {}{why}",
            self.ran, self.files
        )
    }
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
    /// The change alters what a person sees in a repository with an
    /// end-to-end suite and adds or updates no spec in it: the line the
    /// coder acts on (`common::preview::E2eNeed::instruction`). Empty when
    /// no spec is needed or one is there.
    #[serde(default)]
    pub e2e_missing: String,
    /// Where the change is wider than its ticket and the coder gave no
    /// reason it may keep (`minimal_change`): a parallel structure,
    /// deleting or overwriting where the code only added, a log level,
    /// reporting nobody asked for, tests out of proportion, a payload
    /// nobody observed.
    #[serde(default)]
    pub wider: Vec<super::minimal_change::Widening>,
    /// What the change brings back that the run's scope ledger decided is
    /// out of scope (`common::scope_ledger`), in code or in tests.
    #[serde(default)]
    pub out_of_scope: Vec<common::scope_ledger::Breach>,
    /// What the change's end-to-end files rest on and what its diff
    /// carries (`live_guard`): the probes of outside pages, selectors no
    /// probe saw, each test branch on a page's state and whether a run
    /// took it, files only reformatted.
    #[serde(default)]
    pub live: super::live_guard::LiveFacts,
    /// Whether the changed tests ran both ways, and why not.
    #[serde(default)]
    pub tests_ran: TestsRan,
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
/// No code: it didn't run, and the output says why when it is known.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRun {
    pub code: Option<i32>,
    pub output: String,
}

/// Both runs of the changed test files, in the files' order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
        (None, _) if !with.output.is_empty() => (Verdict::NotRun, with.output.clone()),
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
    // Runners that take a path or a package: one place narrows them.
    if !low.contains("vitest") && !low.contains("jest") {
        if let Some(c) = common::test_env::narrow(base_cmd, file) {
            return Some(c);
        }
    }
    super::build_scoped_test_cmd(base_cmd, &[file.to_string()])
        .map(|c| c.replace(" --passWithNoTests", ""))
}

/// The command that runs one test file on its own: the CI test command
/// CoderHelm knows, narrowed to the file, else the file run the way the CI
/// job's test step runs it (`TestEnv::file_command`). `Err` says why none
/// can, and the test is then recorded as not run, never as passing.
pub fn file_cmd(
    ci_cmd: Option<&str>,
    env: Option<&common::test_env::TestEnv>,
    file: &str,
) -> Result<String, String> {
    if let Some(c) = ci_cmd.and_then(|c| scoped_cmd(c, file)) {
        return Ok(c);
    }
    if let Some(c) = env.and_then(|e| e.file_command(file)) {
        return Ok(c);
    }
    Err(match (ci_cmd, env) {
        (Some(c), _) => format!("the test command `{c}` can't be narrowed to `{file}`"),
        (None, Some(e)) => format!(
            "the CI test step ({}) doesn't run `{file}` on its own",
            e.source
        ),
        (None, None) => {
            "the repository's CI runs no unit test command CoderHelm can read".to_string()
        }
    })
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

/// Test data, not a test: fixtures, snapshots, recorded responses. Two
/// runs listed a fixture's JSON as a test CoderHelm couldn't run.
fn is_test_data(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let data_dir = [
        "/fixtures/",
        "/__fixtures__/",
        "/testdata/",
        "/test-data/",
        "/__snapshots__/",
        "/cassettes/",
    ]
    .iter()
    .any(|d| format!("/{p}").contains(d));
    let ext = p.rsplit_once('.').map_or("", |(_, e)| e);
    let data_ext = matches!(
        ext,
        "json"
            | "yaml"
            | "yml"
            | "csv"
            | "txt"
            | "snap"
            | "md"
            | "html"
            | "xml"
            | "sql"
            | "png"
            | "jpg"
            | "svg"
    );
    data_dir || data_ext
}

/// The changed test files a runner can run: not test data.
pub fn bench_files(diff: &Diff) -> Vec<String> {
    let mut tests: Vec<String> = changed_tests(diff)
        .into_iter()
        .filter(|p| !is_test_data(p))
        .collect();
    tests.truncate(MAX_TEST_FILES);
    tests
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
    let all = changed_tests(diff);
    let tests: Vec<String> = all.iter().filter(|p| !is_test_data(p)).cloned().collect();
    if tests.is_empty() {
        facts.no_tests = if all.is_empty() {
            "the change adds or changes no test file".to_string()
        } else {
            "the change adds or changes only test data, no test file to run".to_string()
        };
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
                let why = (verdict == Verdict::NotRun).then(|| NotRunWhy::of(&detail));
                facts.tests.push(TestResult {
                    path: path.clone(),
                    verdict,
                    detail,
                    why,
                });
            }
        }
        Err(why) => {
            for path in run {
                facts.tests.push(TestResult {
                    path: path.clone(),
                    verdict: Verdict::NotRun,
                    detail: why.clone(),
                    why: Some(NotRunWhy::of(&why)),
                });
            }
        }
    }
    for path in extra {
        facts.tests.push(TestResult {
            path: path.clone(),
            verdict: Verdict::NotRun,
            detail: format!("only the first {MAX_TEST_FILES} test files are run on their own"),
            why: Some(NotRunWhy::TooMany),
        });
    }
    facts.tests_ran = TestsRan::of(&facts.tests);
    facts
}

/// Whether a bug's plan says no failing test can be written.
fn repro_waived(plan: &PlanResult) -> bool {
    plan.fix
        .as_ref()
        .is_some_and(|d| !d.habits.repro.not_feasible.trim().is_empty())
}

/// The pull request's line for a bug fix that no test was shown to fail on
/// the base for.
pub const UNVERIFIED_FIX: &str = "Unverified: no test of this fix was shown to fail on the base code, so the reported failure is not reproduced here.";

/// The line for a change no changed test proves.
pub const UNPROVEN: &str = "No changed test fails without the change, so nothing proves it. Write a test that fails on the base code and passes with the change (a unit test, or an end-to-end spec when the behaviour is what a person sees in the browser), through code that exists on the base.";

/// Whether the change's tests prove nothing: test files were changed with
/// the code, and none fails without the change. A new feature's test that
/// can't build without the new code covers it; a bug fix's must show the
/// failure. Not when the plan says no failing test can be written.
pub fn unproven(f: &PrFacts, plan: &PlanResult) -> bool {
    if f.tests.is_empty() || repro_waived(plan) {
        return false;
    }
    let proven = f.tests.iter().any(|t| {
        t.verdict == Verdict::Proven || (!f.bug && t.verdict == Verdict::FailsBeforeAsserting)
    });
    !proven
}

/// The progress note for the check's result: "passed" only when a changed
/// test proves the change (or none needs to) and nothing blocks.
pub fn verdict_note(f: &PrFacts, plan: &PlanResult, blocking: &[String]) -> String {
    let proven = f
        .tests
        .iter()
        .filter(|t| t.verdict == Verdict::Proven)
        .count();
    let counts = format!(
        "{proven} of {} changed test file(s) fail without the change and pass with it",
        f.tests.len()
    );
    if unproven(f, plan) {
        let why = if f.tests.iter().all(|t| t.verdict == Verdict::NotRun) {
            "no test could be run both ways here"
        } else {
            "nothing proves the change"
        };
        return format!("Tests and scope check not passed: {counts}; {why}. Listed under Open items until a test that fails without the change is added.");
    }
    if !blocking.is_empty() {
        return format!(
            "Tests and scope check not passed: {counts}; {} item(s) open",
            blocking.len()
        );
    }
    format!(
        "Tests and scope check passed: {counts}; no file changed outside the plan without a reason"
    )
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
    // Nothing proves the change: changed tests ran both ways and not one
    // fails without it. A passing count of zero is never a pass.
    if unproven(f, plan) && f.tests.iter().any(|t| t.verdict != Verdict::NotRun) {
        out.push(UNPROVEN.to_string());
    }
    if !f.e2e_missing.is_empty() {
        out.push(f.e2e_missing.clone());
    }
    // A test that never ran proves nothing, whatever CI says later.
    for t in f
        .tests
        .iter()
        .filter(|t| t.verdict == Verdict::NotRun && t.detail.contains("found no tests"))
    {
        out.push(format!(
            "`{}`: {}. Make it run under the repository's test command, run it, and quote its output in your Evidence.",
            t.path, t.detail
        ));
    }
    // The change is held to the ticket: smaller, not wider.
    for w in &f.wider {
        out.push(w.instruction());
    }
    // Selectors from a probe, branches run when they can run, no reformat
    // of a file the fix doesn't change.
    out.extend(f.live.blocking());
    if f.bug && f.tests.is_empty() && !repro_waived(plan) && f.no_tests.contains("no test file") {
        out.push("This is a bug fix and the change has no test that fails without it. Add the test that reproduces the reported failure (it must fail on the base code and pass with the fix).".to_string());
    }
    // Scope was decided once: what it left out stays out, tests included.
    for b in &f.out_of_scope {
        out.push(b.instruction());
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
        "The check after the build ran each new or changed test with and without your change, compared the changed files with the plan, and compared the change with the ticket. Fix every item; where the change is wider than the ticket, make it smaller:\n{}\nDo not delete or skip a test to settle an item.",
        lines
            .iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// What is still open after the fix rounds, for Open items.
pub fn open_findings(f: &PrFacts, plan: &PlanResult) -> Vec<Finding> {
    let mut lines = blocking(f, plan);
    if unproven(f, plan) && !lines.iter().any(|l| l == UNPROVEN) {
        lines.push(UNPROVEN.to_string());
    }
    lines
        .into_iter()
        .map(|l| {
            let wider = f.wider.iter().find(|w| w.instruction() == l);
            let (point, settle) = if let Some(w) = wider {
                (
                    w.point(),
                    "CoderHelm makes the change smaller in its next round on this pull request.",
                )
            } else if l.contains("what a person asked for is never reverted") {
                (
                    "A person's ask taken out",
                    "CoderHelm puts it back in its next round on this pull request.",
                )
            } else if l.contains("decided is out of scope") {
                (
                    "Out of scope brought back",
                    "CoderHelm takes it out, code and tests, in its next round on this pull request, or records the criterion or comment that moved it in.",
                )
            } else if let Some(p) = f.live.point_of(&l) {
                p
            } else if l.contains("no task of the plan") {
                (
                    "Changed outside the plan",
                    "CoderHelm restores it or records why the change needs it in its next round on this pull request.",
                )
            } else if l.contains("depends on this change") {
                (
                    "Dependent not updated",
                    "CoderHelm updates it in its next round on this pull request.",
                )
            } else if l.contains("no end-to-end spec there covers it") {
                (
                    "No end-to-end test for a visible change",
                    "CoderHelm writes the spec in its next round on this pull request and runs it on the preview.",
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
        // How many ran both ways, and why the rest didn't, counted.
        let ran = TestsRan::of(&f.tests);
        if ran.ran < ran.files {
            out.push_str(&format!("{}.\n", ran.line()));
        }
        // A bug fix no test was shown to fail on the base for says so, in
        // so many words: it is unverified, never quietly proven by CI.
        if f.bug && !f.tests.iter().any(|t| t.verdict == Verdict::Proven) {
            out.push_str(UNVERIFIED_FIX);
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
        // Only uses a search found: one that found nothing tells a reviewer
        // nothing, so it is not written.
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
    out.push_str(&f.live.section());
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

/// The end-to-end spec's line: it runs against a preview, never in the
/// unit sandbox (CI and the end-to-end proof run it there).
pub const E2E_SPEC: &str = "an end-to-end spec: it runs against a preview, not in the unit sandbox";

/// Whether `file` is an end-to-end spec: its package's runner drives a
/// browser, or it sits in the repository's end-to-end suite.
fn is_e2e_spec(tree: &[String], file: &str, texts: &dyn Fn(&str) -> Option<String>) -> bool {
    use test_package::Runner as R;
    let runner = test_package::package_of(tree, file, texts).and_then(|p| p.runner);
    matches!(runner, Some(R::Playwright | R::Cypress | R::Wdio))
        || common::preview::e2e_suite(tree)
            .is_some_and(|s| file.starts_with(&format!("{}/", s.dir.trim_end_matches('/'))))
}

/// How the sandbox runs the files: the installs (none when CI's test job
/// installs, through its environment), one command per file that has one,
/// and why the others don't run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchPlan {
    pub install: Vec<Step>,
    /// Per file, in order: its command, or why it doesn't run.
    pub per_file: Vec<Result<String, String>>,
}

/// The sandbox plan for `files`, from the CI test command CoderHelm reads
/// (`ci_cmd`), CI's test environment for these files (`env`, its package
/// scripts already resolved) and the tree's manifests (`texts`). A file
/// with no command is recorded as not run with why; the others still run.
pub fn bench_plan(
    files: &[String],
    tree: &[String],
    ci_cmd: Option<&str>,
    env: Option<&common::test_env::TestEnv>,
    texts: &dyn Fn(&str) -> Option<String>,
) -> BenchPlan {
    let mut plan = BenchPlan::default();
    // CI's own install, in its order, at its versions, in its environment
    // (cached per manifest blob); else each file's package's.
    let ci_installs = env.is_some_and(|e| !e.install.is_empty());
    let mut installed: Vec<String> = Vec::new();
    for f in files {
        if is_e2e_spec(tree, f, texts) {
            plan.per_file.push(Err(E2E_SPEC.to_string()));
            continue;
        }
        let cmd = file_cmd(ci_cmd, env, f)
            .or_else(|why| common::test_env::package_test_command(f, tree, texts).ok_or(why));
        if cmd.is_ok() && !ci_installs {
            let steps = test_package::package_of(tree, f, texts)
                .and_then(|p| test_package::check_steps(&p))
                .unwrap_or_default();
            for s in steps.into_iter().filter(|s| s.kind == "install") {
                if !installed.contains(&s.cmd) {
                    installed.push(s.cmd.clone());
                    plan.install.push(s);
                }
            }
        }
        plan.per_file.push(cmd);
    }
    plan
}

/// The runs of the planned commands, per file: a file with no command did
/// not run, and says why.
fn spread(plan: &BenchPlan, runs: Vec<FileRun>) -> Vec<FileRun> {
    let mut runs = runs.into_iter();
    plan.per_file
        .iter()
        .map(|c| match c {
            Ok(_) => runs.next().unwrap_or_default(),
            Err(why) => FileRun {
                code: None,
                output: why.clone(),
            },
        })
        .collect()
}

/// Why the install failed, from a sandbox script's output: CI's install in
/// the test environment, or an install step; the error's last lines.
pub fn install_failure(output: &str, codes: &[Option<i32>], installs: usize) -> Option<String> {
    let tail = |t: &str| -> String {
        let lines: Vec<&str> = t.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
        let pick: Vec<&str> = lines
            .iter()
            .filter(|l| {
                let low = l.to_ascii_lowercase();
                low.contains("err") || low.contains("error") || low.contains("not found")
            })
            .copied()
            .collect();
        let pick = if pick.is_empty() {
            lines[lines.len().saturating_sub(2)..].to_vec()
        } else {
            pick[pick.len().saturating_sub(2)..].to_vec()
        };
        common::truncate_str(&pick.join(" "), 300).replace('`', "'")
    };
    if let Some(at) = output.find(common::test_env::INSTALL_FAILED) {
        let rest = &output[at + common::test_env::INSTALL_FAILED.len()..];
        let body = rest.split(test_package::STEP_MARK).next().unwrap_or(rest);
        return Some(format!("{INSTALL_FAILED} (CI's install: {})", tail(body)));
    }
    if codes.iter().take(installs).any(|c| *c != Some(0)) {
        let out = failing_outputs(output, "install").join("\n");
        return Some(if out.is_empty() {
            INSTALL_FAILED.to_string()
        } else {
            format!("{INSTALL_FAILED} ({})", tail(&out))
        });
    }
    None
}

#[async_trait::async_trait]
impl Bench for SandboxBench<'_> {
    async fn run(&self, files: &[String]) -> Result<Runs, String> {
        let r = &self.runner;
        let ci_cmd = super::detect_ci_test(r.github, r.owner, r.repo, self.base_branch).await;
        let tree_full = r
            .github
            .get_tree(r.owner, r.repo, self.branch)
            .await
            .map_err(|e| e.to_string())?;
        let tree: Vec<String> = tree_full.iter().map(|e| e.path.clone()).collect();
        // CI's job for these files: its install wraps every command, and
        // its test steps (package scripts read through) run each file.
        let env = super::test_setup::for_run(
            r.state,
            r.github,
            r.team_id,
            r.owner,
            r.repo,
            self.base_branch,
            self.branch,
            r.run_id,
            files,
        )
        .await;
        let mut texts: std::collections::HashMap<String, String> = Default::default();
        for p in common::test_env::script_files(env.as_ref().map(|(e, _)| e), &tree, files) {
            if let Ok(t) = r.github.read_file(r.owner, r.repo, &p, self.branch).await {
                texts.insert(p, t);
            }
        }
        let read = |p: &str| texts.get(p).cloned();
        let resolved = env.as_ref().map(|(e, _)| {
            let mut e = e.clone();
            e.resolve_scripts(&tree, &read);
            e
        });
        let plan = bench_plan(files, &tree, ci_cmd.as_deref(), resolved.as_ref(), &read);
        let commands: Vec<&String> = plan
            .per_file
            .iter()
            .filter_map(|c| c.as_ref().ok())
            .collect();
        if commands.is_empty() {
            // Nothing to run: each file says why.
            let none = spread(&plan, Vec::new());
            return Ok(Runs {
                with: none.clone(),
                without: none,
            });
        }
        let installs = plan.install.len();
        let mut steps: Vec<Step> = plan.install.clone();
        steps.extend(commands.iter().map(|c| Step {
            kind: KIND,
            cmd: (*c).clone(),
        }));
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
                r.run_at_in(&head, &steps, env.clone()),
                r.run_at_in(&before_sha, &steps, env.clone())
            );
            Ok::<_, String>((with?, without?))
        }
        .await;
        if let Err(e) = r.github.delete_branch(r.owner, r.repo, &temp).await {
            warn!(error = %e, branch = %temp, "Could not delete the branch of the test check");
        }
        let ((with_codes, with_out), (without_codes, without_out)) = both?;
        for (codes, out) in [(&with_codes, &with_out), (&without_codes, &without_out)] {
            if let Some(why) = install_failure(out, codes, installs) {
                warn!(run_id = r.run_id, why = %why, "The changed tests' install failed in the sandbox");
                return Err(why);
            }
        }
        let per_file = |codes: &[Option<i32>], out: &str| {
            spread(
                &plan,
                file_runs(&codes[installs.min(codes.len())..], out, KIND),
            )
        };
        Ok(Runs {
            with: per_file(&with_codes, &with_out),
            without: per_file(&without_codes, &without_out),
        })
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

/// The missing end-to-end spec's line for a change a person sees in the
/// browser, read from the changed paths and the repository's tree (its
/// e2e suite). Empty when none is needed, one is there, or the tree
/// can't be read.
async fn e2e_missing(w: &super::proof::Work<'_>, paths: &[String]) -> String {
    if !paths.iter().any(|p| common::preview::is_user_visible(p)) {
        return String::new();
    }
    let tree: Vec<String> = match w
        .github
        .get_tree(&w.msg.repo_owner, &w.msg.repo_name, w.branch)
        .await
    {
        Ok(t) => t.into_iter().map(|e| e.path).collect(),
        Err(e) => {
            warn!(run_id = w.run_id, error = %e, "Could not read the tree for the e2e suite");
            return String::new();
        }
    };
    let suite = common::preview::e2e_suite(&tree);
    common::preview::e2e_need(paths, suite.as_ref())
        .instruction()
        .unwrap_or_default()
}

/// Where the change is wider than its ticket (`minimal_change`), read
/// against the base text of the changed code files and the ticket's own
/// words, less what the coder's notes keep with a reason.
async fn wider(
    w: &super::proof::Work<'_>,
    diff: &Diff,
    notes: &str,
) -> Vec<super::minimal_change::Widening> {
    let (owner, repo) = (&w.msg.repo_owner, &w.msg.repo_name);
    let mut base = Vec::new();
    for (path, _, _) in diff
        .iter()
        .filter(|(p, s, _)| s == "modified" && !is_test_path(p))
        .take(12)
    {
        if let Ok(text) = w
            .github
            .read_file(owner, repo, path, &w.msg.base_branch)
            .await
        {
            base.push((path.clone(), text));
        }
    }
    let has_probe = match w.github.get_tree(owner, repo, w.branch).await {
        Ok(t) => t.iter().any(|e| common::e2e::probe::is_record(&e.path)),
        Err(_) => false,
    };
    let ticket = format!(
        "{}\n{}{}",
        w.msg.title,
        w.msg.body,
        common::run_questions::answers_note(&w.msg.answers)
    );
    let found = super::minimal_change::check(diff, &base, &ticket, has_probe);
    super::minimal_change::standing(&found, notes)
        .into_iter()
        .cloned()
        .collect()
}

/// A bench for a run whose steps never had the time for the sandbox, past
/// [`MAX_PROOF_STEPS`] asks: nothing runs, and each test says so.
struct NoTime;

#[async_trait::async_trait]
impl Bench for NoTime {
    async fn run(&self, _files: &[String]) -> Result<Runs, String> {
        Err(format!("{NO_TIME} in {MAX_PROOF_STEPS} steps of their own; the repository's CI runs the tests on this pull request"))
    }
}

/// The two sandbox runs of a head's changed test files, kept on the run's
/// ledger: a step that runs them first ([`run_owed`]) leaves them for the
/// check, which reads them instead of running them again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRuns {
    pub head: String,
    pub files: Vec<String>,
    pub result: Result<Runs, String>,
}

impl RecordedRuns {
    /// Whether these are the runs of `files` on `head`.
    pub fn of(&self, head: &str, files: &[String]) -> bool {
        !head.is_empty() && self.head == head && self.files == files
    }
}

/// A bench that answers with recorded runs.
struct Recorded(Result<Runs, String>);

#[async_trait::async_trait]
impl Bench for Recorded {
    async fn run(&self, _files: &[String]) -> Result<Runs, String> {
        self.0.clone()
    }
}

/// A bench that keeps what another one gave, for the ledger.
struct Recording<'b> {
    inner: &'b dyn Bench,
    got: std::sync::Mutex<Option<Result<Runs, String>>>,
}

#[async_trait::async_trait]
impl Bench for Recording<'_> {
    async fn run(&self, files: &[String]) -> Result<Runs, String> {
        let r = self.inner.run(files).await;
        *self.got.lock().unwrap() = Some(r.clone());
        r
    }
}

/// What the step does about the sandbox runs for a head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofTime {
    /// They ran for this head and files: read them.
    Recorded(Result<Runs, String>),
    /// This step has their time: run them now.
    Run,
    /// Owed to a fresh step, which runs them first.
    Owe,
    /// No step had the time, past the asks a run has.
    NoTime,
}

/// What a step does about the changed tests' sandbox runs: read the
/// recorded ones, run them, owe them to a fresh step (at most
/// [`MAX_PROOF_STEPS`] times a run, never on a path that has no step to
/// give), or, only past that, record them as not run.
pub fn proof_time(
    recorded: Option<&RecordedRuns>,
    head: &str,
    files: &[String],
    secs_left: u64,
    steps_asked: u32,
    can_hand_off: bool,
) -> ProofTime {
    if let Some(r) = recorded.filter(|r| r.of(head, files)) {
        return ProofTime::Recorded(r.result.clone());
    }
    if secs_left >= SANDBOX_STEP_SECS {
        return ProofTime::Run;
    }
    if can_hand_off && steps_asked < MAX_PROOF_STEPS {
        return ProofTime::Owe;
    }
    ProofTime::NoTime
}

/// The diff the check reads: the run's work files, with their status.
async fn work_diff(w: &super::proof::Work<'_>) -> Result<Vec<(String, String, String)>, String> {
    let diff = w
        .github
        .compare_files_with_patches(
            &w.msg.repo_owner,
            &w.msg.repo_name,
            &w.msg.base_branch,
            w.branch,
        )
        .await
        .map_err(|e| format!("the diff couldn't be read ({e})"))?;
    let paths: Vec<String> = diff.iter().map(|(p, _, _)| p.clone()).collect();
    let work = super::run_end::work_files(&paths, &w.msg.ticket_id);
    Ok(diff
        .into_iter()
        .filter(|(p, _, _)| work.contains(p))
        .collect())
}

fn sandbox_bench<'a>(w: &'a super::proof::Work<'a>) -> SandboxBench<'a> {
    SandboxBench {
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
    }
}

/// The sandbox runs a step owed this one, run first, before any other
/// check, with the step's whole budget; their result is recorded for the
/// check ([`RecordedRuns`]). Nothing when none are owed, or the head moved
/// since (the check asks again for the new one).
pub async fn run_owed(w: &super::proof::Work<'_>, secs_left: impl Fn() -> u64) {
    let owed = w.ledger.update(|l| l.proof_owed.clone());
    if owed.is_empty() {
        return;
    }
    let head = w.head().await;
    if head != owed || secs_left() < SANDBOX_STEP_SECS {
        if head != owed {
            w.ledger.update(|l| l.proof_owed.clear());
            w.save_ledger().await;
        }
        return;
    }
    let files = match work_diff(w).await {
        Ok(d) if changes_code(&d) => bench_files(&d),
        Ok(_) => Vec::new(),
        Err(e) => {
            warn!(run_id = w.run_id, error = %e, "The owed sandbox runs couldn't read the diff; the check runs them");
            return;
        }
    };
    if files.is_empty() {
        w.ledger.update(|l| l.proof_owed.clear());
        w.save_ledger().await;
        return;
    }
    super::add_progress_note(
        w.state,
        &w.msg.team_id,
        w.run_id,
        &format!(
            "Running the {} changed test file(s) with and without the change first in this step, with its whole time",
            files.len()
        ),
    )
    .await;
    let result = sandbox_bench(w).run(&files).await;
    info!(
        run_id = w.run_id,
        ok = result.is_ok(),
        files = files.len(),
        "The owed sandbox runs ran first in the step"
    );
    w.ledger.update(|l| {
        l.proof_owed.clear();
        l.proof_runs = Some(RecordedRuns {
            head: head.clone(),
            files: files.clone(),
            result: result.clone(),
        });
    });
    w.save_ledger().await;
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
        let diff = work_diff(w).await.map_err(GateStop::Why)?;
        let paths: Vec<String> = diff.iter().map(|(p, _, _)| p.clone()).collect();
        let files = bench_files(&diff);
        let needs_sandbox = !files.is_empty() && changes_code(&diff);
        let head = w.head().await;
        // The proof runs have a budget of their own: read the recorded
        // runs, run them now, or owe them to a fresh step that runs them
        // first; only past the asks are they recorded as not run.
        let time = if needs_sandbox {
            let (recorded, asked, can) = w
                .ledger
                .update(|l| (l.proof_runs.clone(), l.proof_steps, !l.no_proof_step));
            proof_time(recorded.as_ref(), &head, &files, secs_left(), asked, can)
        } else {
            ProofTime::Run
        };
        if time == ProofTime::Owe {
            w.ledger.update(|l| {
                l.proof_steps += 1;
                l.proof_owed = head.clone();
            });
            w.save_ledger().await;
            return Err(GateStop::NeedsTime);
        }
        let sandbox = sandbox_bench(w);
        let recording = Recording {
            inner: &sandbox,
            got: Default::default(),
        };
        let recorded;
        let bench: &dyn Bench = match &time {
            ProofTime::Recorded(r) => {
                recorded = Recorded(r.clone());
                &recorded
            }
            ProofTime::NoTime => &NoTime,
            _ => &recording,
        };
        let mut facts = check_once(
            bench,
            &diff,
            w.plan,
            &own_repo,
            w.generated,
            &evidence.notes,
        )
        .await;
        if let Some(result) = recording.got.lock().unwrap().take() {
            w.ledger.update(|l| {
                l.proof_runs = Some(RecordedRuns {
                    head: head.clone(),
                    files: files.clone(),
                    result,
                })
            });
        }
        if !facts.tests_ran.line().is_empty() {
            super::add_progress_note(w.state, &w.msg.team_id, w.run_id, &facts.tests_ran.line())
                .await;
        }
        facts.e2e_missing = e2e_missing(w, &paths).await;
        facts.wider = wider(w, &diff, &evidence.notes).await;
        // Fixture ids invented while the run looked up the real ones.
        let looked: Vec<String> = evidence.lookups.iter().map(|l| l.line.clone()).collect();
        facts
            .wider
            .extend(super::minimal_change::made_up_ids(&diff, &looked));
        // A page whose state a test branch reads is probed again while
        // there is time: an arm that couldn't run before is re-checked.
        facts.live = super::live_guard::facts_for(
            w,
            &diff,
            &evidence.notes,
            secs_left() >= SANDBOX_STEP_SECS,
        )
        .await;
        let comments = super::scope_ledger::comments(w.github, w.msg, w.branch).await;
        let people = super::scope_ledger::people(w.msg, &comments);
        let mut ledger = super::scope_ledger::load_or_seed(
            w.state,
            &w.msg.team_id,
            w.run_id,
            w.msg,
            w.plan,
            &people,
        )
        .await;
        facts.out_of_scope = super::scope_ledger::held(
            w.state,
            &w.msg.team_id,
            w.run_id,
            &mut ledger,
            &evidence.notes,
            &super::scope_ledger::sources(w.msg, &comments),
            &people,
            &diff,
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
        if blocking.is_empty() {
            super::add_progress_note(
                w.state,
                &w.msg.team_id,
                w.run_id,
                &verdict_note(&facts, w.plan, &blocking),
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

    /// A run's check once read "passed: 0 of 6 changed test file(s) fail
    /// without the change": six tests, none proving anything, called a
    /// pass. Zero is never a pass.
    #[tokio::test]
    async fn six_changed_tests_none_failing_without_the_change_is_not_passed() {
        let p = plan(TASKS, TicketIntent::Bug, Habits::default());
        let six: Vec<String> = (1..=6).map(|i| format!("src/cart/t{i}.test.ts")).collect();
        let mut paths: Vec<&str> = six.iter().map(String::as_str).collect();
        paths.push("src/cart/total.ts");
        let bench = Fake(Ok(Runs {
            with: vec![run(0, ""); 6],
            without: vec![run(0, ""); 6],
        }));
        let f = check_once(&bench, &diff(&paths), &p, "acme/shop-api", &[], "").await;
        assert_eq!(f.tests.len(), 6);
        assert!(unproven(&f, &p));
        let b = blocking(&f, &p);
        assert!(b.contains(&UNPROVEN.to_string()), "{b:?}");
        let note = verdict_note(&f, &p, &b);
        assert!(
            note.starts_with("Tests and scope check not passed: 0 of 6"),
            "{note}"
        );
        assert!(!note.contains("check passed"));
        // A feature's own tests too: the gap is closed by writing one that
        // fails without the change.
        let feature = plan(TASKS, TicketIntent::Feature, Habits::default());
        assert!(blocking(&f, &feature).contains(&UNPROVEN.to_string()));
    }

    #[tokio::test]
    async fn tests_that_could_not_run_both_ways_are_not_a_pass_either() {
        let p = plan(TASKS, TicketIntent::Bug, Habits::default());
        let bench = Fake(Err("the sandbox gave no result".into()));
        let d = diff(&["src/cart/total.ts", "src/cart/total.test.ts"]);
        let f = check_once(&bench, &d, &p, "acme/shop-api", &[], "").await;
        // No fix round can make the sandbox run: open, never "passed".
        assert!(!blocking(&f, &p).contains(&UNPROVEN.to_string()));
        assert!(verdict_note(&f, &p, &[]).starts_with("Tests and scope check not passed"));
        assert!(open_findings(&f, &p)
            .iter()
            .any(|o| o.point == "Test doesn't prove the change"));
    }

    #[tokio::test]
    async fn a_visible_change_with_no_e2e_spec_goes_back_to_the_coder() {
        let p = plan(TASKS, TicketIntent::Feature, Habits::default());
        let mut f = PrFacts::default();
        let suite = common::preview::Suite {
            dir: "e2e".into(),
            runner: "playwright".into(),
        };
        f.e2e_missing = common::preview::e2e_need(
            &["web/src/components/cart/total.tsx".to_string()],
            Some(&suite),
        )
        .instruction()
        .unwrap();
        let b = blocking(&f, &p);
        assert_eq!(b.len(), 1);
        assert!(b[0].contains("no end-to-end spec there covers it"));
        assert_eq!(
            open_findings(&f, &p)[0].point,
            "No end-to-end test for a visible change"
        );
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
        assert_eq!(b.len(), 2);
        assert!(b[0].contains("passes with the change taken out"));
        assert_eq!(b[1], UNPROVEN);
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
