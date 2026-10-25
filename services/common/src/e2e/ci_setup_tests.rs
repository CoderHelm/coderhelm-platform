use super::*;

const E2E: &str = r#"# Runs the browser tests on every push.
name: E2E

on:
  push:
    branches: [main]
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npm run lint

  e2e:
    name: Browser tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # Install once, cached.
      - name: Install
        run: npm ci
      - name: Run E2E tests
        run: npx playwright test --project=chromium   # all browsers nightly
      # Keep the HTML report for people.
      - name: Report
        if: always()
        run: echo done
"#;

fn scripts() -> Map<String, Json> {
    Map::new()
}

fn upload_only() -> Options {
    Options {
        upload: true,
        quarantine: false,
        repair: false,
    }
}

fn both() -> Options {
    Options {
        upload: true,
        quarantine: true,
        repair: false,
    }
}

#[test]
fn finds_the_playwright_job_and_step() {
    let job = find_test_job(E2E, &scripts()).unwrap();
    assert_eq!(job.job_id, "e2e");
    assert_eq!(job.job_name.as_deref(), Some("Browser tests"));
    assert_eq!(job.step_index, 2);
    assert_eq!(job.runner, Runner::Playwright);
    assert!(!job.matrix);
}

#[test]
fn follows_npm_scripts_to_the_runner() {
    let pkg = r#"{"scripts":{"test:e2e":"playwright test","test":"vitest run"},"devDependencies":{"@playwright/test":"1"}}"#;
    let s = package_scripts(Some(pkg));
    assert_eq!(
        runner_of_command("npm run test:e2e", &s),
        Some(Runner::Playwright)
    );
    assert_eq!(
        runner_of_command("pnpm test:e2e", &s),
        Some(Runner::Playwright)
    );
    assert_eq!(runner_of_command("yarn test", &s), Some(Runner::Vitest));
    assert_eq!(runner_of_command("npm ci", &s), None);
    assert_eq!(runner_from_package(pkg), Some(Runner::Playwright));
    // "jest" inside another word is not jest.
    assert_eq!(runner_of_command("echo majestic", &s), None);
}

#[test]
fn a_repo_without_test_steps_has_nothing_to_set_up() {
    let yaml = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npm run build\n";
    assert!(inspect(yaml, &scripts()).is_none());
    assert!(pick_workflow(&[("build.yml".into(), yaml.into())], &scripts()).is_none());
}

#[test]
fn picks_the_e2e_workflow_among_several() {
    let unit = "on: push\njobs:\n  unit:\n    runs-on: x\n    steps:\n      - run: npx jest\n";
    let files = vec![
        (".github/workflows/unit.yml".to_string(), unit.to_string()),
        (".github/workflows/e2e.yml".to_string(), E2E.to_string()),
    ];
    let (path, facts) = pick_workflow(&files, &scripts()).unwrap();
    assert_eq!(path, ".github/workflows/e2e.yml");
    assert_eq!(facts.upload, UploadState::Missing);
    assert!(!facts.quarantine);
}

#[test]
fn inserts_the_upload_step_after_the_test_step_keeping_comments_and_order() {
    let p = plan(E2E, &scripts(), upload_only()).unwrap();
    assert_eq!(p.added, vec!["upload"]);
    let out = &p.yaml;
    // Comments and the trailing comment on the test command are kept.
    for kept in [
        "# Runs the browser tests on every push.",
        "      # Install once, cached.",
        "        run: npx playwright test --project=chromium   # all browsers nightly",
        "      # Keep the HTML report for people.",
    ] {
        assert!(out.contains(kept), "lost: {kept}\n{out}");
    }
    // The new step sits between the test step and the comment of the next.
    let test = out.find("name: Run E2E tests").unwrap();
    let upload = out.find(UPLOAD_STEP_NAME).unwrap();
    let report_comment = out.find("# Keep the HTML report").unwrap();
    assert!(test < upload && upload < report_comment, "{out}");
    assert!(out.contains("      - name: Upload E2E results for Coderhelm\n        # Keeps"));
    assert!(out.contains("        if: ${{ !cancelled() }}"));
    assert!(out
        .contains("          path: |\n            playwright-report/\n            test-results/"));
    // The lint job is byte for byte the same.
    assert!(out.starts_with(&E2E[..E2E.find("  e2e:").unwrap()]));
    let facts = inspect(out, &scripts()).unwrap();
    assert_eq!(facts.upload, UploadState::Found);
    assert!(facts.ours);
}

#[test]
fn planning_twice_changes_nothing() {
    let once = plan(E2E, &scripts(), both()).unwrap();
    assert!(once.changed());
    let twice = plan(&once.yaml, &scripts(), both()).unwrap();
    assert!(!twice.changed(), "{:?}", twice.added);
    assert_eq!(twice.yaml, once.yaml);
}

#[test]
fn a_repo_that_already_uploads_is_left_alone() {
    let yaml = E2E.replace(
        "      # Keep the HTML report for people.",
        "      - uses: actions/upload-artifact@v4\n        if: ${{ !cancelled() }}\n        with:\n          name: report\n          path: playwright-report/\n      # Keep the HTML report for people.",
    );
    assert_eq!(
        inspect(&yaml, &scripts()).unwrap().upload,
        UploadState::Found
    );
    let p = plan(&yaml, &scripts(), upload_only()).unwrap();
    assert!(!p.changed());
    assert_eq!(p.yaml, yaml);
}

#[test]
fn the_quarantine_step_goes_before_the_test_step_and_wires_the_command() {
    let p = plan(E2E, &scripts(), both()).unwrap();
    assert_eq!(p.added, vec!["upload", "quarantine", "permissions"]);
    let out = &p.yaml;
    let q = out.find(QUARANTINE_STEP_NAME).unwrap();
    let test = out.find("name: Run E2E tests").unwrap();
    let install = out.find("name: Install").unwrap();
    assert!(install < q && q < test, "{out}");
    // The flag goes before the trailing comment, not into it.
    assert!(
        out.contains("run: npx playwright test --project=chromium ${QUARANTINED:+--grep-invert \"$QUARANTINED\"}   # all browsers nightly"),
        "{out}"
    );
    // env lands on the test step, before the upload step.
    let env = out
        .find("QUARANTINED: ${{ steps.coderhelm_quarantine.outputs.grep }}")
        .unwrap();
    let upload = out.find(UPLOAD_STEP_NAME).unwrap();
    assert!(test < env && env < upload, "{out}");
    // The job may ask for an OIDC token.
    assert!(out.contains("    permissions:\n      contents: read\n      id-token: write"));
    let doc: Yaml = serde_yaml::from_str(out).unwrap();
    let step = &doc["jobs"]["e2e"]["steps"][3];
    assert_eq!(step["name"].as_str(), Some("Run E2E tests"));
    assert!(step["env"]["QUARANTINED"].as_str().is_some());
    assert!(inspect(out, &scripts()).unwrap().quarantine);
}

#[test]
fn existing_permissions_and_env_are_extended_not_replaced() {
    let yaml = r#"on: push
permissions:
  contents: read
  pull-requests: write
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Test
        env:
          CI: "true"
        run: |
          npm ci
          npm run e2e
"#;
    let pkg = r#"{"scripts":{"e2e":"playwright test"}}"#;
    let s = package_scripts(Some(pkg));
    let p = plan(yaml, &s, both()).unwrap();
    let out = &p.yaml;
    assert!(
        out.contains("permissions:\n  id-token: write\n  contents: read\n  pull-requests: write"),
        "{out}"
    );
    assert!(
        out.contains("        env:\n          QUARANTINED:"),
        "{out}"
    );
    assert!(out.contains("          CI: \"true\""));
    // npm needs `--` before arguments for the script.
    assert!(
        out.contains("          npm run e2e -- ${QUARANTINED:+--grep-invert \"$QUARANTINED\"}"),
        "{out}"
    );
    assert!(!out.contains("npm ci -- "));
    assert!(!plan(out, &s, both()).unwrap().changed());
}

#[test]
fn matrix_jobs_get_one_artifact_per_copy() {
    let yaml = "on: push\njobs:\n  e2e:\n    strategy:\n      matrix:\n        shard: [1, 2]\n    runs-on: x\n    steps:\n      - run: npx playwright test --shard=${{ matrix.shard }}/2\n";
    let p = plan(yaml, &scripts(), upload_only()).unwrap();
    assert!(
        p.yaml
            .contains("name: coderhelm-e2e-results-${{ strategy.job-index }}"),
        "{}",
        p.yaml
    );
}

#[test]
fn the_last_step_gets_the_upload_after_it() {
    let yaml = "on: push\njobs:\n  e2e:\n    runs-on: x\n    steps:\n    - uses: actions/checkout@v4\n    - run: npx playwright test\n\n# trailing comment\n";
    let p = plan(yaml, &scripts(), upload_only()).unwrap();
    assert!(p.yaml.contains("    - run: npx playwright test\n    - name: Upload E2E results for Coderhelm\n      # Keeps"), "{}", p.yaml);
    assert!(p.yaml.ends_with("\n\n# trailing comment\n"));
}

#[test]
fn repair_runs_an_upload_step_on_failure_too() {
    let yaml = "on: push\njobs:\n  e2e:\n    runs-on: x\n    steps:\n      - run: npx playwright test\n      - uses: actions/upload-artifact@v4\n        with:\n          path: playwright-report/\n";
    assert_eq!(
        inspect(yaml, &scripts()).unwrap().upload,
        UploadState::OnlyOnSuccess
    );
    // A plain setup doesn't touch a step that is there.
    assert!(!plan(yaml, &scripts(), upload_only()).unwrap().changed());
    let p = plan(
        yaml,
        &scripts(),
        Options {
            upload: true,
            quarantine: false,
            repair: true,
        },
    )
    .unwrap();
    assert_eq!(p.added, vec!["upload_if"]);
    assert!(
        p.yaml
            .ends_with("          path: playwright-report/\n        if: ${{ !cancelled() }}\n"),
        "{}",
        p.yaml
    );
    assert_eq!(
        inspect(&p.yaml, &scripts()).unwrap().upload,
        UploadState::Found
    );
}

#[test]
fn repair_adds_our_step_when_the_paths_dont_match() {
    let yaml = "on: push\njobs:\n  e2e:\n    runs-on: x\n    steps:\n      - run: npx playwright test\n      - uses: actions/upload-artifact@v4\n        if: always()\n        with:\n          path: coverage/\n";
    assert_eq!(
        inspect(yaml, &scripts()).unwrap().upload,
        UploadState::OtherFiles
    );
    let p = plan(
        yaml,
        &scripts(),
        Options {
            upload: true,
            quarantine: false,
            repair: true,
        },
    )
    .unwrap();
    assert_eq!(p.added, vec!["upload"]);
    assert_eq!(
        inspect(&p.yaml, &scripts()).unwrap().upload,
        UploadState::Found
    );
}

#[test]
fn quarantine_is_refused_for_runners_that_cant_skip_tests() {
    let yaml = "on: push\njobs:\n  test:\n    runs-on: x\n    steps:\n      - run: npx jest\n";
    let e = plan(yaml, &scripts(), both()).unwrap_err();
    assert!(e.contains("Playwright"), "{e}");
    let p = plan(yaml, &scripts(), upload_only()).unwrap();
    assert!(p.yaml.contains("junit.xml"));
}

#[test]
fn invalid_yaml_is_refused_not_mangled() {
    assert!(plan("jobs: [", &scripts(), upload_only()).is_err());
}

#[test]
fn the_diff_shows_only_the_added_lines() {
    let p = plan(E2E, &scripts(), upload_only()).unwrap();
    let d = unified_diff(".github/workflows/e2e.yml", E2E, &p.yaml);
    assert!(d.starts_with("--- a/.github/workflows/e2e.yml\n+++ b/.github/workflows/e2e.yml\n@@ "));
    assert!(d.contains("+      - name: Upload E2E results for Coderhelm\n"));
    assert!(!d
        .lines()
        .any(|l| l.starts_with('-') && !l.starts_with("---")));
    assert_eq!(unified_diff("x", E2E, E2E), "");
}

#[test]
fn reporters_that_write_results_are_recognised() {
    assert_eq!(
        reporter_writes_results(Runner::Playwright, Some("reporter: [['html']],"), None),
        Some(true)
    );
    assert_eq!(
        reporter_writes_results(Runner::Playwright, Some("reporter: 'list',"), None),
        Some(false)
    );
    assert_eq!(
        reporter_writes_results(Runner::Playwright, None, None),
        None
    );
    assert_eq!(
        reporter_writes_results(
            Runner::Jest,
            None,
            Some(r#"{"devDependencies":{"jest-junit":"1"}}"#)
        ),
        Some(true)
    );
}

#[test]
fn snippets_carry_the_repos_command() {
    let s = quarantine_snippet(Some("npm run e2e"));
    assert!(s.contains("run: npm run e2e -- ${QUARANTINED:+--grep-invert"));
    assert!(upload_snippet(Runner::Playwright, false)
        .starts_with("- name: Upload E2E results for Coderhelm"));
    let job = find_test_job(E2E, &scripts()).unwrap();
    assert_eq!(
        test_command(E2E, &job).as_deref(),
        Some("npx playwright test --project=chromium")
    );
}

fn facts_with(upload: UploadState) -> WorkflowFacts {
    WorkflowFacts {
        job: find_test_job(E2E, &scripts()).unwrap(),
        upload,
        upload_paths: vec!["coverage/".into()],
        quarantine: false,
        ours: false,
    }
}

#[test]
fn runs_without_results_are_counted_until_results_arrive() {
    let mut ci = CiState::default();
    ci.note_run("e2e.yml", "u1", "2026-10-01T00:00:00Z", "failure", None);
    ci.note_run("e2e.yml", "u2", "2026-10-02T00:00:00Z", "success", None);
    assert_eq!(ci.workflows["e2e.yml"].empty_runs, 2);
    assert!(ci.newest_results().is_none());
    ci.note_run("e2e.yml", "u3", "2026-10-03T00:00:00Z", "success", Some(42));
    let w = &ci.workflows["e2e.yml"];
    assert_eq!((w.empty_runs, w.last_tests), (0, 42));
    assert_eq!(
        ci.newest_results().unwrap().last_results_url.as_deref(),
        Some("u3")
    );
    ci.note_run("", "", "2026-10-04T00:00:00Z", "completed", Some(3));
    assert!(ci.workflows.contains_key("upload"));
}

#[test]
fn the_status_turns_green_only_on_real_results() {
    let found = facts_with(UploadState::Found);
    let missing = facts_with(UploadState::Missing);
    let seen = |empty| WorkflowSeen {
        empty_runs: empty,
        ..Default::default()
    };
    assert_eq!(status_of(None, None, false, false), SetupStatus::NoTests);
    assert_eq!(
        status_of(Some(&missing), None, false, false),
        SetupStatus::NeedsSetup
    );
    assert_eq!(
        status_of(Some(&missing), None, false, true),
        SetupStatus::InProgress
    );
    // YAML in place is not enough.
    assert_eq!(
        status_of(Some(&found), Some(&seen(1)), false, false),
        SetupStatus::Waiting
    );
    assert_eq!(
        status_of(Some(&found), Some(&seen(2)), false, false),
        SetupStatus::NothingReceived
    );
    assert_eq!(
        status_of(Some(&found), Some(&seen(0)), true, false),
        SetupStatus::SetUp
    );
    // Results from elsewhere count even without the step.
    assert_eq!(
        status_of(Some(&missing), None, true, false),
        SetupStatus::SetUp
    );
}

#[test]
fn the_likely_cause_names_what_went_wrong() {
    let seen = WorkflowSeen {
        empty_runs: 2,
        ..Default::default()
    };
    let (why, fixable) = likely_cause(&facts_with(UploadState::OtherFiles), &seen, Some(true));
    assert!(why.contains("coverage/") && fixable, "{why}");
    let (why, fixable) = likely_cause(&facts_with(UploadState::OnlyOnSuccess), &seen, Some(true));
    assert!(why.contains("only when every test passes") && fixable);
    let skipped = WorkflowSeen {
        last_empty_conclusion: "skipped".into(),
        ..seen.clone()
    };
    let (why, fixable) = likely_cause(&facts_with(UploadState::Found), &skipped, Some(true));
    assert!(why.contains("skipped") && !fixable);
    let (why, _) = likely_cause(&facts_with(UploadState::Found), &seen, Some(false));
    assert!(why.contains("playwright.config.ts"), "{why}");
}

#[test]
fn the_pull_request_says_what_it_changes() {
    let facts = inspect(E2E, &scripts()).unwrap();
    let p = plan(E2E, &scripts(), both()).unwrap();
    let r = pr_request(".github/workflows/e2e.yml", "main", &facts, &p, false);
    assert_eq!(r.branch, SETUP_BRANCH);
    assert_eq!(r.base, "main");
    assert_eq!(
        r.title,
        "Send E2E results to Coderhelm and skip quarantined tests"
    );
    assert!(r.body.starts_with("## Summary\n"));
    assert!(r.body.contains("## Changes\n") && r.body.contains("## Verification\n"));
    assert!(r.body.contains("**Browser tests** job"));
    assert!(r.body.contains("id-token: write"));
    for arrow in ['\u{2192}', '\u{21D2}', '\u{279C}'] {
        assert!(!r.body.contains(arrow));
    }
    let up = plan(E2E, &scripts(), upload_only()).unwrap();
    assert_eq!(
        pr_request("x", "main", &facts, &up, false).title,
        "Send E2E results to Coderhelm"
    );
    assert_eq!(
        pr_request("x", "main", &facts, &up, true).title,
        "Fix the E2E results upload for Coderhelm"
    );
}

#[test]
fn the_quarantine_step_asks_this_installs_api() {
    let p = plan(E2E, &scripts(), both()).unwrap();
    assert!(p.yaml.contains(&format!("{} | jq", quarantine_url())));
    assert!(quarantine_url().ends_with("/webhooks/ci/tests/quarantine"));
}
