//! tq. Across a batch of recent runs, many had new or changed tests to run
//! with and without the change, and not one ran both ways:
//!
//! - several recorded "the package's install failed in the sandbox", all in
//!   one pnpm workspace. The test files sat in a member (`apps/web`) whose
//!   lockfile is the workspace root's, and the sandbox installed the member
//!   on its own: `cd 'apps/web' && pnpm install --ignore-workspace
//!   --frozen-lockfile`. With no lockfile in that folder pnpm stops with
//!   `ERR_PNPM_NO_LOCKFILE` before any test, on every run. The run's own CI
//!   install (`pnpm install --frozen-lockfile` at the root, cached per
//!   manifest blob) was never used for these runs, and the failure said
//!   nothing of why. One of them also listed a Playwright spec as a unit
//!   test.
//! - several recorded "no step of the run had the time for the two sandbox
//!   runs". The check asked once for a step of its own; the fresh step ran
//!   every other check first (the proof, the link checks, the new-feature
//!   rules, live probes, the scope ledger) and came to the runs with less
//!   than their 760 seconds left, and the one request was spent: one run
//!   resumed for the runs and only reached them minutes later.
//! - a few recorded "the repository's CI runs no unit test command CoderHelm
//!   can read" for a data pipeline whose CI loops `python "$t"` over its
//!   tests, and listed fixture JSON files as tests too; one file without a
//!   command stopped every file from running.
//! - a few had no test to run (a change of only tests, or no test file).
//!
//! Guarantee: the runs install the way CI does (its job for the files, its
//! install lines in order, its Node and pnpm versions, through the cached
//! environment); without one a workspace member installs from the
//! workspace root, falling back to the member and what it depends on. A
//! failed install says why (its error lines). An end-to-end spec and test
//! data are never run as unit tests. The runs have a budget of their own:
//! owed to a fresh step, which runs them first, before any other check,
//! and records them for the head; a run asks up to `MAX_PROOF_STEPS`
//! times. Each file runs with CI's test step narrowed to it (one
//! `common::test_env::narrow` for pytest, unittest, Vitest, Jest, go test,
//! cargo test; package scripts and make targets read through), else its
//! own package's test script; a file with none doesn't stop the others.
//! Whether the tests ran, and why not, is counted on the run and in the
//! pull request.

use crate::passes::habits_check::{
    self, bench_files, bench_plan, install_failure, proof_time, FileRun, NotRunWhy, ProofTime,
    RecordedRuns, Runs, TestResult, TestsRan, Verdict,
};
use common::e2e::test_package::{self, STEP_MARK};
use common::test_env::{self, TestEnv};

fn tree(paths: &[&str]) -> Vec<String> {
    paths.iter().map(|p| p.to_string()).collect()
}

/// A web workspace like those runs': `apps/web` a member, the lockfile at
/// the root, an end-to-end package with its own lockfile.
fn workspace() -> Vec<String> {
    tree(&[
        "package.json",
        "pnpm-lock.yaml",
        "pnpm-workspace.yaml",
        "apps/web/package.json",
        "apps/web/src/components/drawer.tsx",
        "apps/web/src/test/components/drawer-reopen.test.tsx",
        "e2e/package.json",
        "e2e/pnpm-lock.yaml",
        "e2e/tests/browse/booking.spec.ts",
    ])
}

fn manifests(p: &str) -> Option<String> {
    match p {
        "package.json" => Some(r#"{"name":"acme","packageManager":"pnpm@10.28.2"}"#.into()),
        "apps/web/package.json" => Some(
            r#"{"name":"web","scripts":{"test":"vitest","test:run":"vitest run"},"devDependencies":{"vitest":"3.0.0"}}"#
                .into(),
        ),
        "e2e/package.json" => Some(r#"{"devDependencies":{"@playwright/test":"1.48.0"}}"#.into()),
        _ => None,
    }
}

#[test]
fn a_workspace_member_installs_from_the_root_and_says_why_when_it_fails() {
    let t = workspace();
    let test = "apps/web/src/test/components/drawer-reopen.test.tsx".to_string();
    // What those runs ran: the member on its own, where pnpm finds no
    // lockfile.
    let web = test_package::package_of(&t, &test, &manifests).unwrap();
    let install = &test_package::check_steps(&web).unwrap()[0].cmd;
    assert!(!install.contains("cd 'apps/web' &&"), "{install}");
    assert!(!install.contains("--ignore-workspace"), "{install}");
    assert!(install.starts_with("cd '.' && "), "{install}");
    // With CI's install for these files read, CI's install runs (in the
    // cached environment) and nothing else.
    let ci = TestEnv {
        kind: test_env::Kind::Node,
        install: vec!["pnpm install --frozen-lockfile".into()],
        test: vec!["pnpm --filter web run test:run".into()],
        ..Default::default()
    };
    let mut resolved = ci.clone();
    resolved.resolve_scripts(&t, &manifests);
    let plan = bench_plan(
        std::slice::from_ref(&test),
        &t,
        None,
        Some(&resolved),
        &manifests,
    );
    assert!(plan.install.is_empty());
    assert_eq!(
        plan.per_file,
        vec![Ok(
            "(cd apps/web && npx --no-install vitest run src/test/components/drawer-reopen.test.tsx)"
                .to_string()
        )]
    );
    // Without CI's, the member's package installs from the root, once.
    let plan = bench_plan(&[test.clone(), test.clone()], &t, None, None, &manifests);
    assert_eq!(plan.install.len(), 1);
    assert!(plan.install[0]
        .cmd
        .contains("pnpm install --frozen-lockfile"));
    // The Playwright spec one run listed is never a unit test.
    let spec = "e2e/tests/browse/booking.spec.ts".to_string();
    let plan = bench_plan(&[spec], &t, None, None, &manifests);
    assert!(matches!(&plan.per_file[0], Err(w) if w == habits_check::E2E_SPEC));
    assert_eq!(NotRunWhy::of(habits_check::E2E_SPEC), NotRunWhy::EndToEnd);
    // A failed install says why, from its own lines.
    let out = format!(
        "--- install failed (exit 1) ---\n ERR_PNPM_NO_LOCKFILE  Cannot install with \"frozen-lockfile\" because pnpm-lock.yaml is absent\n{STEP_MARK} 1 1\n{STEP_MARK} 2 1\n"
    );
    let why = install_failure(&out, &[Some(1), Some(1)], 1).unwrap();
    assert!(why.starts_with(habits_check::INSTALL_FAILED), "{why}");
    assert!(why.contains("ERR_PNPM_NO_LOCKFILE"), "{why}");
    let ci_out = format!(
        "{} 1\nERR_PNPM_FETCH_401 GET https://npm.pkg.example/@acme%2fui: Unauthorized\n{STEP_MARK} 1 1\n",
        test_env::INSTALL_FAILED
    );
    let why = install_failure(&ci_out, &[Some(1)], 0).unwrap();
    assert!(
        why.contains("CI's install") && why.contains("ERR_PNPM_FETCH_401"),
        "{why}"
    );
    assert_eq!(NotRunWhy::of(&why), NotRunWhy::InstallFailed);
    assert_eq!(install_failure("ok", &[Some(0), Some(0)], 1), None);
    // CI's wrapper prints that marker when its install fails.
    assert!(ci.wrap("true").contains(test_env::INSTALL_FAILED));
}

#[test]
fn the_runs_have_a_step_of_their_own_and_run_first_in_it() {
    let files = vec!["apps/web/src/test/a.test.ts".to_string()];
    // The coding step ends with 300 seconds left: the runs are owed.
    assert_eq!(proof_time(None, "h1", &files, 300, 0, true), ProofTime::Owe);
    // What those runs did next: the fresh step reached them after its
    // other checks, short again, with its one request spent. Now the owed
    // runs go first, with the step's whole time, and are recorded.
    assert_eq!(proof_time(None, "h1", &files, 880, 1, true), ProofTime::Run);
    let done = RecordedRuns {
        head: "h1".into(),
        files: files.clone(),
        result: Ok(Runs {
            with: vec![FileRun {
                code: Some(0),
                output: String::new(),
            }],
            without: vec![FileRun {
                code: Some(1),
                output: "AssertionError: expected 'pagehide'".into(),
            }],
        }),
    };
    // The check, however late in the step, reads them.
    assert!(matches!(
        proof_time(Some(&done), "h1", &files, 40, 1, true),
        ProofTime::Recorded(Ok(_))
    ));
    // A fix round's new head needs its own runs: owed again, up to the cap.
    assert_eq!(
        proof_time(Some(&done), "h2", &files, 40, 1, true),
        ProofTime::Owe
    );
    assert_eq!(
        proof_time(
            Some(&done),
            "h2",
            &files,
            40,
            habits_check::MAX_PROOF_STEPS,
            true
        ),
        ProofTime::NoTime
    );
    // A path with no step to give records them as not run.
    assert_eq!(
        proof_time(None, "h1", &files, 40, 0, false),
        ProofTime::NoTime
    );
    // The step runs what it owes before every other check.
    let src = include_str!("../mod.rs");
    let owed = src
        .find("habits_check::run_owed(&work, || remaining_secs(start)).await;")
        .expect("the owed runs run in the step");
    let first_check = src
        .find("hand_off_unless_time_for!(\n        proof,")
        .expect("the proof check");
    assert!(owed < first_check);
    let check = include_str!("../habits_check.rs");
    assert!(check.contains("l.proof_owed = head.clone();"));
    assert!(!check.contains("std::mem::replace(&mut l.habits_waited, true)"));
}

#[test]
fn a_loop_ci_runs_the_test_and_its_fixtures_are_not_tests() {
    // The data pipeline's CI: a loop runs each test file with `python`.
    let ci = TestEnv {
        kind: test_env::Kind::Python,
        install: vec!["uv pip install -r requirement.txt".into()],
        test: vec![
            "for t in tests/test_*.py; do".into(),
            "python \"$t\" || failed=1".into(),
            "done".into(),
        ],
        ..Default::default()
    };
    let diff: Vec<(String, String, String)> = [
        "dags/utils/booking_common.py",
        "tests/fixtures/tour_stops.json",
        "tests/fixtures/tour_live_names.json",
        "tests/test_tour_stop.py",
    ]
    .iter()
    .map(|p| (p.to_string(), "modified".to_string(), String::new()))
    .collect();
    let files = bench_files(&diff);
    assert_eq!(files, vec!["tests/test_tour_stop.py"]);
    let t = tree(&["tests/test_tour_stop.py", "requirement.txt"]);
    let plan = bench_plan(&files, &t, None, Some(&ci), &|_| None);
    assert_eq!(
        plan.per_file,
        vec![Ok("python tests/test_tour_stop.py".to_string())]
    );
    // A file no command runs is not run, with why; the others still run.
    let plan = bench_plan(
        &["tests/test_tour_stop.py".into(), "scripts/check.sh".into()],
        &t,
        None,
        Some(&ci),
        &|_| None,
    );
    assert!(plan.per_file[0].is_ok());
    assert!(plan.per_file[1].is_err());
    // `make test` and package scripts are read to their runner.
    let read =
        |p: &str| (p == "Makefile").then(|| "test:\n\tpython -m pytest -q tests\n".to_string());
    let mut make = TestEnv {
        test: vec!["make test".into()],
        ..Default::default()
    };
    make.resolve_scripts(&tree(&["Makefile"]), &read);
    assert_eq!(
        make.file_command("tests/test_tour_stop.py").as_deref(),
        Some("python -m pytest tests/test_tour_stop.py")
    );
}

#[test]
fn whether_the_tests_ran_is_counted_with_why() {
    let t = |path: &str, verdict: Verdict, detail: &str| TestResult {
        path: path.into(),
        verdict,
        detail: detail.into(),
        why: None,
    };
    let tests = vec![
        t("a.test.ts", Verdict::Proven, "expected 1"),
        t("b.test.ts", Verdict::NotRun, habits_check::INSTALL_FAILED),
        t("c.spec.ts", Verdict::NotRun, habits_check::E2E_SPEC),
    ];
    let ran = TestsRan::of(&tests);
    assert_eq!((ran.files, ran.ran), (3, 1));
    assert_eq!(
        ran.line(),
        "Tests run with and without the change: 1 of 3 (not run: 1 install failed, 1 end-to-end spec)"
    );
    assert_eq!(TestsRan::of(&[]).line(), "");
}
