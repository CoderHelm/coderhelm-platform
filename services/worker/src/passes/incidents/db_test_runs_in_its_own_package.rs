//! db. A web change added a unit test (`apps/web/...test.tsx`) and an
//! end-to-end spec (`e2e/tests/...spec.ts`). The check that runs each changed
//! test with and without the change picked its sandbox's install from the
//! end-to-end job, chosen because one of the files was its spec, and that job
//! installs only its own folder. The unit test then printed
//! `sh: 1: vitest: not found` both ways, and the check read that as "fails
//! with the change in place" and "no changed test fails without the change".
//! The run spent a fix round rewriting a test that had never run, and listed
//! two findings that weren't true; the run's own coder ran the same test in
//! its shell and it passed.
//!
//! Guarantee: the sandbox's install is chosen only by the files it runs (an
//! end-to-end spec runs against a preview, never there), and a job is kept
//! only when its folder covers every file (`TestEnv::misses`). A test whose
//! runner isn't installed didn't run: it is recorded as not run, never as a
//! failure of the change nor as a proof of it. A test that did run and fails
//! with the change, or proves nothing, holds the pull request as a check the
//! change didn't pass, so the run can't be done with it open.

use crate::passes::habits_check::{self, Bench, FileRun, Runs, Verdict};
use crate::passes::plan::PlanResult;
use common::test_env::TestEnv;

const UNIT: &str = "apps/web/src/test/components/block.test.tsx";
const SPEC: &str = "e2e/tests/site/block.spec.ts";

/// What the sandbox printed for the unit test, both ways: the runner missing.
const NO_RUNNER: &str = "> web@0.1.0 test /tmp/src/repo/apps/web\n\
     > vitest block.test.tsx\n\n\
     sh: 1: vitest: not found\n\
     /tmp/src/repo/apps/web:\n\
     ERR_PNPM_RECURSIVE_RUN_FIRST_FAIL  web@0.1.0 test: `vitest block.test.tsx`\n\
     spawn ENOENT\n\
     WARN  Local package.json exists, but node_modules missing, did you mean to install?";

struct Recorded(Runs);

#[async_trait::async_trait]
impl Bench for Recorded {
    async fn run(&self, _files: &[String]) -> Result<Runs, String> {
        Ok(self.0.clone())
    }
}

fn tree() -> Vec<String> {
    [
        "package.json",
        "pnpm-workspace.yaml",
        "apps/web/package.json",
        "apps/web/src/components/blocks/block.tsx",
        UNIT,
        "e2e/package.json",
        "e2e/playwright.config.ts",
        SPEC,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn plan() -> PlanResult {
    PlanResult {
        proposal: String::new(),
        tasks: String::new(),
        spec: String::new(),
        design: String::new(),
        repo_tasks: vec![],
        fix: None,
    }
}

fn diff() -> Vec<(String, String, String)> {
    ["apps/web/src/components/blocks/block.tsx", UNIT, SPEC]
        .iter()
        .map(|p| (p.to_string(), "modified".to_string(), String::new()))
        .collect()
}

fn missing() -> FileRun {
    FileRun {
        code: Some(1),
        output: NO_RUNNER.into(),
    }
}

fn e2e() -> FileRun {
    FileRun {
        code: None,
        output: habits_check::E2E_SPEC.into(),
    }
}

/// The recorded runs: the unit test's runner missing both ways, the spec not
/// run in the sandbox. Nothing ran, so nothing failed and nothing is proven.
#[tokio::test]
async fn a_test_whose_runner_is_missing_did_not_run() {
    let bench = Recorded(Runs {
        with: vec![missing(), e2e()],
        without: vec![missing(), e2e()],
    });
    let f = habits_check::check_once(&bench, &diff(), &plan(), "acme/site", &[], "").await;
    assert_eq!(f.tests[0].verdict, Verdict::NotRun, "{:?}", f.tests[0]);
    assert!(
        f.tests[0].detail.contains("install failed"),
        "{}",
        f.tests[0].detail
    );
    let blocking = habits_check::blocking(&f, &plan());
    assert!(
        !blocking
            .iter()
            .any(|l| l.contains("fails with the change in place")),
        "{blocking:?}"
    );
    assert!(!blocking.contains(&habits_check::UNPROVEN.to_string()));
    assert!(f.tests_ran.line().contains("install failed"));
}

/// The sandbox's job is chosen by the unit test alone: the end-to-end spec
/// never picks it.
#[test]
fn the_install_is_chosen_by_the_files_the_sandbox_runs() {
    let focus = habits_check::unit_focus(&tree(), &[UNIT.to_string(), SPEC.to_string()]);
    assert_eq!(focus, vec![UNIT.to_string()]);
}

/// A job whose folder doesn't cover every file is another package's: the
/// end-to-end job kept from an earlier choice is worked out again for the
/// unit test, even when one of the files is its own.
#[test]
fn a_job_is_kept_only_when_it_covers_every_file() {
    let e2e_job = TestEnv {
        dir: "e2e".into(),
        install: vec!["pnpm install --frozen-lockfile --ignore-workspace".into()],
        test: vec!["pnpm exec playwright test".into()],
        ..Default::default()
    };
    assert!(e2e_job.misses(&[UNIT.to_string(), SPEC.to_string()]));
    assert!(e2e_job.misses(&[UNIT.to_string()]));
    assert!(!e2e_job.misses(&[SPEC.to_string()]));
    assert!(!e2e_job.misses(&[]));
}

/// A test that ran and fails with the change is a check the change didn't
/// pass: it holds the draft, so the run is never done with it open.
#[tokio::test]
async fn a_test_that_ran_and_fails_holds_the_pull_request() {
    let bench = Recorded(Runs {
        with: vec![
            FileRun {
                code: Some(1),
                output: "AssertionError: expected 'closed' to be 'open'".into(),
            },
            e2e(),
        ],
        without: vec![
            FileRun {
                code: Some(1),
                output: "AssertionError: expected 'closed' to be 'open'".into(),
            },
            e2e(),
        ],
    });
    let f = habits_check::check_once(&bench, &diff(), &plan(), "acme/site", &[], "").await;
    assert_eq!(f.tests[0].verdict, Verdict::FailsWithChange);
    let open = habits_check::open_findings(&f, &plan());
    assert!(
        open.iter()
            .any(|o| o.point == "check_blocked" && o.detail.contains(UNIT)),
        "{open:?}"
    );
    // Written as the pull request writes it, it holds the draft even with
    // CI green: CI passing doesn't make the test prove the change.
    let body = crate::passes::proof::gaps_section(&open);
    assert!(
        crate::passes::proof::holds_blocked_on(&body, true),
        "{body}"
    );
}
