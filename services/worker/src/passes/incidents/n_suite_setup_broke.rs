//! n. A review fix round on a Python repository finished every requested
//! change, then ran the full test suite in the sandbox. The suite was read
//! from the CI workflow, and its `uses: astral-sh/setup-uv` step was put in
//! the setup as words: "install uv with Python 3.11 and activate a venv
//! (astral-sh/setup-uv)". The sandbox's `/bin/sh` failed on the "(" (`eval:
//! Syntax error: "(" unexpected`, exit 2, no counts), and the round was held
//! as "Full test suite FAILED". The requester had named the test loop to
//! use; it never ran.
//!
//! Guarantee: a `uses:` step becomes real shell or is left out, never
//! words, and the suite runs under bash after `bash -n`. A command
//! CoderHelm built that breaks before any test runs (not valid bash, a
//! missing tool, a failed setup, exit 2 within seconds with no counts) is
//! CoderHelm's own failure: the round tries the requester's command, then
//! the repository's instructions, then the simplest test command, and when
//! none runs it lands the fix and says plainly that the tests didn't run in
//! the sandbox and why. Tests that ran and failed still hold the fix.

use super::fixture::*;
use crate::passes::review_fix::suite::{self, Origin, Outcome};
use crate::passes::review_fix::suite_shell::{self, Run};
use crate::passes::review_fix::{self, Check};

const OLD: &str = "Full test suite FAILED";

const WORKFLOW: &str = "name: tests\non: [pull_request]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: sudo apt-get update -qq && sudo apt-get install -yqq libpq-dev pkg-config\n      - uses: astral-sh/setup-uv@v5\n        with:\n          python-version: \"3.11\"\n          activate-environment: true\n      - run: uv pip install -r requirements.txt\n      - name: Run tests\n        run: for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || exit 1; done\n";

const REQUESTER: &str =
    "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || break; done";

/// What the sandbox printed for the derived command, in production.
const PROD_OUTPUT: &str = "===CODERHELM_CHECKS_START===\nv20.20.2\n/codebuild/output/tmp/script.sh: 1: eval: Syntax error: \"(\" unexpected\n===CODERHELM_CHECKS_END exit=2===\n";

fn files() -> Vec<(String, String)> {
    vec![
        (".github/workflows/tests.yml".into(), WORKFLOW.into()),
        (
            "pyproject.toml".into(),
            "[project]\nname = \"app\"\n".into(),
        ),
    ]
}

/// The discovery's answer as the model gave it: the setup-uv step in words.
fn the_discovery_answer() -> suite::Suite {
    suite::parse(&serde_json::json!({
        "found": true,
        "setup": "sudo apt-get update -qq && sudo apt-get install -yqq libpq-dev pkg-config; install uv with Python 3.11 and activate a venv (astral-sh/setup-uv); uv pip install -r requirements.txt",
        "command": "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || exit 1; done",
        "source": ".github/workflows/tests.yml, 'Run tests' step (preceded by the install and compile steps)",
        "left_out": [],
        "instructions_command": "",
        "simple_command": "python -m unittest discover",
        "reason": ""
    }))
    .unwrap()
}

fn end_of(check: &Check) -> Ending {
    let mut end = Ending::default();
    for l in check.evidence.iter().chain(check.hold.iter()) {
        end.said(l.clone());
    }
    if let Some(s) = &check.suite {
        end.said(s.line());
    }
    end
}

fn finish(outcome: Outcome) -> Check {
    let mut out = Check::default();
    review_fix::suite_step(outcome, true, &mut out);
    if let Some(s) = &out.suite {
        out.evidence.push(s.line());
    }
    out
}

#[test]
fn a_setup_uv_step_becomes_valid_bash_never_words() {
    let raw = the_discovery_answer();
    assert!(suite_shell::bash_syntax(&raw.setup, &raw.command).is_err());
    let s = raw.sanitized(&files());
    assert!(!s.setup.contains("activate a venv"), "{}", s.setup);
    assert!(
        s.setup.contains("uv venv -q --python 3.11 .venv"),
        "{}",
        s.setup
    );
    assert_eq!(suite_shell::bash_syntax(&s.setup, &s.command), Ok(()));
    // The discovery is told the shell for each step, and that words are
    // never a step.
    let prompt = suite::prompt(&files());
    assert!(prompt.contains("`astral-sh/setup-uv`: `python3 -m pip install -q uv"));
    assert!(suite::SYSTEM.contains("never describe a step in words"));
    // The sandbox runs it under bash, after `bash -n`.
    let wrapped = suite_shell::sandbox_command(&s.setup, &s.command);
    assert!(
        wrapped.contains("if bash -n /tmp/coderhelm_suite.sh; then bash /tmp/coderhelm_suite.sh")
    );
}

#[test]
fn the_requesters_command_runs_first_and_is_quoted() {
    let s = the_discovery_answer().sanitized(&files());
    let review = format!("Please fix the parser. Run the tests with `{REQUESTER}` before pushing.");
    let named = suite_shell::requester_command(&[&review, ""]).unwrap();
    assert_eq!(named, REQUESTER);
    let tries = s.candidates(Some(&named));
    assert_eq!(tries[0].command, REQUESTER);
    assert_eq!(tries[0].origin, Origin::Requester);
    assert!(tries.iter().all(|c| c.origin != Origin::Derived));
    let passed = finish(Outcome::Passed {
        command: tries[0].command.clone(),
        source: tries[0].source.clone(),
        counts: suite::counts("Ran 12 tests in 0.4s\n\nOK\n"),
        broke: vec![],
    });
    assert!(passed.hold.is_empty());
    assert!(end_of(&passed).says(&format!("`{REQUESTER}`")));
    assert!(end_of(&passed).says("from the command the requester named"));
}

#[test]
fn a_broken_derived_command_falls_back_and_never_holds_the_fix() {
    // The production output: a shell syntax error, exit 2, no counts.
    let why = match suite_shell::read_run(PROD_OUTPUT, Some(2)) {
        Run::Broken { why, .. } => why,
        other => panic!("{other:?}"),
    };
    assert!(why.contains("printed no counts"), "{why}");
    // Nothing else ran: the fix lands and the description says so.
    let derived = "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || exit 1; done";
    let landed = finish(Outcome::Broken(vec![(derived.into(), why.clone())]));
    assert!(landed.hold.is_empty(), "{:?}", landed.hold);
    assert!(landed.findings.is_empty());
    let end = end_of(&landed);
    assert!(
        end.says("The tests could not run in the sandbox"),
        "{:?}",
        end.words
    );
    assert!(end.says("CoderHelm's own test setup broke, not the tests"));
    end.never_says(OLD);
    // The requester's command ran after it and passed.
    let passed = finish(Outcome::Passed {
        command: REQUESTER.into(),
        source: "the command the requester named".into(),
        counts: suite::counts("Ran 12 tests in 0.4s\n\nOK\n"),
        broke: vec![(derived.into(), why)],
    });
    assert!(passed.hold.is_empty());
    let end = end_of(&passed);
    assert!(end.says("Full test suite passed"));
    assert!(end.says("after CoderHelm's own command broke before any test ran"));
    end.never_says(OLD);
}

#[test]
fn tests_that_ran_and_failed_still_hold_the_fix() {
    let out = format!(
        "{}0===\nRan 12 tests in 0.4s\n\nFAILED (failures=2)\n{}1 secs=31===\n",
        suite_shell::SETUP_MARKER,
        suite_shell::RAN_MARKER
    );
    let (counts, exit) = match suite_shell::read_run(&out, Some(1)) {
        Run::Failed { counts, exit } => (counts, exit),
        other => panic!("{other:?}"),
    };
    let held = finish(Outcome::Failed {
        command: REQUESTER.into(),
        source: "the command the requester named".into(),
        counts,
        exit,
        tail: out,
        broke: vec![],
    });
    assert_eq!(held.hold.len(), 1);
    assert!(
        held.hold[0].contains("Full test suite FAILED (exit 1)"),
        "{:?}",
        held.hold
    );
    assert!(
        held.hold[0].contains("10 passed, 2 failed"),
        "{:?}",
        held.hold
    );
}

#[test]
fn the_round_runs_each_command_through_the_bash_checks_and_the_fallbacks() {
    let src = include_str!("../review_fix/mod.rs");
    let run = fn_src(src, "async fn run_suite(");
    for call in [
        "round.requester_command()",
        "s.candidates(",
        "suite_shell::bash_syntax(",
        "suite_shell::sandbox_command(",
        "suite_shell::read_run(",
        "suite::Outcome::Broken(",
    ] {
        assert!(run.contains(call), "run_suite no longer calls {call}");
    }
    let check = fn_src(src, "pub async fn check(");
    assert!(check.contains("suite_step(outcome, last, &mut out)"));
    let discover = fn_src(src, "async fn discover_suite(");
    assert!(discover.contains(".sanitized(&files)"));
}
