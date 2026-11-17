//! cm. A web ticket in a monorepo (a Next.js app under `apps/web`, a
//! studio, two small lambdas) was told its test environment came from the
//! image-resizer lambda's deploy workflow: install `(cd lambdas/image-resizer
//! && npm ci)`, tests `(cd lambdas/image-resizer && npm test)`. That
//! workflow sorts first and runs only for `lambdas/image-resizer/**`; the
//! run read the first job with a test step and stopped. Every sandbox
//! command of the run went through the lambda's install, and the coder was
//! told to run the lambda's tests "directly" for a change to the web app.
//! In the week before, the coder's own test commands for that app ended
//! 127 or 254 (no pnpm) or were refused in 36 of about 170 tries.
//!
//! Guarantee: the CI job chosen is the one whose tests run for the files
//! the run plans to change: a workflow filtered to other paths, or a job
//! that runs in a folder none of them is in, is taken only when nothing
//! fits better, and with no files a job that runs on every change from the
//! root comes first. A run's record made for another package's job is
//! worked out again (`test_env::from_workflows_for`, `TestEnv::misses`,
//! `test_setup::for_run`).

use common::test_env::{from_workflows, from_workflows_for, Kind};

const RESIZER: &str = r#"
name: Deploy Image Resizer Lambda
on:
  push:
    branches: [main]
    paths:
      - 'lambdas/image-resizer/**'
      - '.github/workflows/deploy-lambda-image-resizer.yml'
  pull_request:
    types: [opened, synchronize, reopened, closed, labeled]
    paths:
      - 'lambdas/image-resizer/**'
jobs:
  test:
    name: Run unit tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Install dependencies
        working-directory: lambdas/image-resizer
        run: npm ci
      - name: Run unit tests
        working-directory: lambdas/image-resizer
        run: npm test
"#;

const SUITE: &str = r#"
name: Test Suite
on:
  pull_request:
    branches: [main]
  push:
    branches: [main]
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '24'
      - name: Install dependencies
        run: pnpm install --frozen-lockfile
      - name: Run studio unit tests
        run: pnpm --filter studio test
      - name: Run unit tests
        run: pnpm --filter web test -- --run
"#;

fn workflows() -> Vec<(String, String)> {
    // The order the repository lists them in: the lambda's sorts first.
    vec![
        (
            ".github/workflows/deploy-lambda-image-resizer.yml".into(),
            RESIZER.into(),
        ),
        (".github/workflows/test.yml".into(), SUITE.into()),
    ]
}

fn tree() -> Vec<String> {
    vec![
        "pnpm-lock.yaml".into(),
        "package.json".into(),
        "lambdas/image-resizer/package-lock.json".into(),
    ]
}

#[test]
fn a_web_change_gets_the_web_suite_not_the_lambdas() {
    let focus = vec![
        "apps/web/src/components/blocks/portal-embed.tsx".to_string(),
        "apps/web/src/test/components/portal-embed.test.tsx".to_string(),
    ];
    let env = from_workflows_for(&workflows(), &tree(), &focus).expect("a test job");
    assert!(env.source.contains("test.yml"), "{}", env.source);
    assert_eq!(env.kind, Kind::Node);
    assert!(env.dir.is_empty());
    assert_eq!(env.install, vec!["pnpm install --frozen-lockfile"]);
    assert!(env.test.iter().any(|t| t.contains("--filter web")));
    assert!(!env.summary().contains("image-resizer"), "{}", env.summary());
    assert!(!env.misses(&focus));
}

#[test]
fn with_no_files_the_root_suite_comes_before_a_filtered_package() {
    let env = from_workflows(&workflows(), &tree()).expect("a test job");
    assert!(env.source.contains("test.yml"), "{}", env.source);
}

#[test]
fn a_change_to_the_lambda_gets_the_lambdas_job() {
    let focus = vec!["lambdas/image-resizer/src/handler.ts".to_string()];
    let env = from_workflows_for(&workflows(), &tree(), &focus).expect("a test job");
    assert!(env.source.contains("image-resizer"), "{}", env.source);
    assert!(env.test.iter().all(|t| t.contains("lambdas/image-resizer")));
    assert!(!env.misses(&focus));
}

/// The record the run kept from the earlier version names the lambda's
/// job: for a web change it is worked out again.
#[test]
fn a_record_for_another_package_is_worked_out_again() {
    let old = from_workflows_for(
        &workflows(),
        &tree(),
        &["lambdas/image-resizer/src/handler.ts".to_string()],
    )
    .unwrap();
    assert!(old.misses(&["apps/web/src/app/page.tsx".to_string()]));
    // No files to judge by: the record stands.
    assert!(!old.misses(&[]));
}

/// Only another package's job exists: it is still used, nothing better
/// fits (the sandbox needs some install).
#[test]
fn the_only_job_is_used_when_nothing_fits_better() {
    let env = from_workflows_for(
        &workflows()[..1],
        &tree(),
        &["apps/web/src/app/page.tsx".to_string()],
    )
    .expect("the lambda's job, for want of another");
    assert!(env.source.contains("image-resizer"));
}
