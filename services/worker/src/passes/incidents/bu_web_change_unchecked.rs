//! bu. A run opened a pull request on a web app and called it ready with
//! nothing looked at in a browser. The repository's preview deploy and its
//! preview end-to-end checks skip drafts and run on a label; the
//! description was written while the pull request was a draft, so every
//! deploy and end-to-end check on the head read "skipped (its conditions
//! left this PR out)", next to passing unit tests. It said "No screenshot:
//! CoderHelm had no runnable preview", gave `npm test` as the way to check
//! it, and the tests check passed on "0 of 6 changed test files fail
//! without the change". The run page only said `needs_person_approval`.
//!
//! Guarantee, all read from the repository and the run's records:
//! - the preview and end-to-end jobs that cover the change, and the labels
//!   that start them, come from the workflow files (`common::preview`);
//!   a draft is marked ready for the checks that skip drafts and is ready
//!   only once they pass (`ready::decide`);
//! - a skipped check that covers the change is a gap CoderHelm closes,
//!   never evidence (`pr_facts`);
//! - the changed pages are captured at desktop and phone width once the
//!   preview is up (`preview::capture`), and "How to check" names the
//!   changed specs and tests;
//! - zero tests failing without the change is never a pass
//!   (`habits_check::unproven`);
//! - the run page and the description say in one line what the pull
//!   request waits on (`common::waiting_line`).

use common::pr_facts::{CheckFact, Facts, FileChange, Outcome};
use common::preview::{self as pv, Stage};

const WORKFLOW: &str = r#"
name: Deploy Preview
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review, labeled, closed]
    branches: [main]
jobs:
  deploy_preview:
    name: Deploy Preview App
    if: >-
      github.event.action != 'closed' &&
      github.event.pull_request.draft != true &&
      (github.event.action != 'labeled' ||
       github.event.label.name == 'CI:PREVIEW' ||
       github.event.label.name == 'CI:E2E' ||
       startsWith(github.event.label.name, 'E2E:'))
    runs-on: ubuntu-latest
    steps: [{run: deploy}]
  e2e_preview:
    name: E2E on preview
    needs: deploy_preview
    if: >-
      (contains(github.event.pull_request.labels.*.name, 'CI:E2E') ||
       contains(join(github.event.pull_request.labels.*.name, ','), 'E2E:')) &&
      !contains(github.event.pull_request.labels.*.name, 'CI:STAGING')
    runs-on: ubuntu-latest
    steps: [{run: e2e}]
  cleanup:
    name: Cleanup PR Preview
    if: github.event.action == 'closed'
    runs-on: ubuntu-latest
    steps: [{run: bye}]
"#;

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn changed() -> Vec<String> {
    s(&[
        "apps/web/src/components/signup/use-signup-form.ts",
        "apps/web/src/lib/validation.ts",
        "apps/web/src/test/lib/validation.test.ts",
    ])
}

#[test]
fn the_workflows_say_which_labels_start_the_preview_and_its_e2e() {
    let w = vec![(
        ".github/workflows/preview.yml".to_string(),
        WORKFLOW.to_string(),
    )];
    let c = changed();
    let labels = s(&[
        "CI:PREVIEW",
        "CI:E2E",
        "CI:STAGING",
        "E2E:signup",
        "E2E:billing",
    ]);
    let p = pv::plan(&pv::PlanInput {
        workflows: &w,
        changed: &c,
        base: "main",
        repo_labels: &labels,
        on_pr: &[],
        preferred: &[],
        areas: &[],
    });
    assert_eq!(
        p.add,
        s(&["E2E:signup"]),
        "the change's own area, never staging"
    );
    assert_eq!(p.draft_gated, s(&["Deploy Preview App", "E2E on preview"]));
    // The test map names the area when the repository keeps one.
    let areas = s(&["signup"]);
    let mapped = pv::plan(&pv::PlanInput {
        workflows: &w,
        changed: &c,
        base: "main",
        repo_labels: &s(&["CI:E2E"]),
        on_pr: &[],
        preferred: &[],
        areas: &areas,
    });
    assert_eq!(mapped.add, s(&["E2E:signup"]));
}

fn facts(preview: Option<pv::State>) -> Facts {
    Facts {
        head: "abc1234def".into(),
        files: changed()
            .into_iter()
            .map(|path| FileChange {
                path,
                status: "modified".into(),
                previous: String::new(),
                changed: 4,
            })
            .collect(),
        files_complete: true,
        checks_read: true,
        checks: vec![
            CheckFact {
                name: "Unit tests".into(),
                outcome: Outcome::Passed,
                url: String::new(),
            },
            CheckFact {
                name: "Deploy Preview App".into(),
                outcome: Outcome::DidNotRun,
                url: String::new(),
            },
            CheckFact {
                name: "E2E on preview".into(),
                outcome: Outcome::DidNotRun,
                url: String::new(),
            },
            CheckFact {
                name: "Deploy to Staging".into(),
                outcome: Outcome::DidNotRun,
                url: String::new(),
            },
        ],
        preview,
        ..Default::default()
    }
}

#[test]
fn a_skipped_preview_check_is_a_gap_never_evidence() {
    let f = facts(None);
    assert_eq!(
        common::pr_facts::preview_gaps(&f),
        s(&["Deploy Preview App", "E2E on preview"])
    );
    let b = common::pr_facts::body_of(&f, &[]);
    assert!(b.evidence.contains(&format!(
        "`Deploy Preview App`: {}",
        common::pr_facts::GAP_WORDS
    )));
    assert!(!b
        .evidence
        .contains("`E2E on preview`: skipped (its conditions left this PR out)"));
    // A staging deploy is not this pull request's to start.
    assert!(b
        .evidence
        .contains("`Deploy to Staging`: skipped (its conditions left this PR out)"));
}

#[test]
fn the_pages_are_shown_at_both_widths_once_the_preview_is_checked() {
    let st = pv::State {
        head: "abc1234def".into(),
        stage: Stage::Checked,
        url: "https://pr-1.preview-web.example.com".into(),
        shots: vec![
            pv::Shot {
                page: "/signup".into(),
                width: "desktop".into(),
                src: "https://github.com/org/web/blob/c1/pr-1/abc/signup-desktop.png?raw=true"
                    .into(),
            },
            pv::Shot {
                page: "/signup".into(),
                width: "phone".into(),
                src: "https://github.com/org/web/blob/c1/pr-1/abc/signup-phone.png?raw=true".into(),
            },
        ],
        ..Default::default()
    };
    let b = common::pr_facts::body_of(&facts(Some(st)), &[]);
    match b.screens {
        Some(common::pr_body::Screens::Pages(p)) => {
            assert_eq!(p.len(), 1);
            assert!(p[0].1.ends_with("signup-desktop.png?raw=true"));
            assert!(p[0].2.ends_with("signup-phone.png?raw=true"));
        }
        other => panic!("pages expected, got {other:?}"),
    }
    assert!(b
        .evidence
        .contains("Preview: https://pr-1.preview-web.example.com"));
    // Still deploying: it says so, never "no runnable preview".
    let deploying = pv::State {
        head: "abc1234def".into(),
        stage: Stage::Deploying,
        ..Default::default()
    };
    let b = common::pr_facts::body_of(&facts(Some(deploying)), &[]);
    let Some(common::pr_body::Screens::Missing(why)) = b.screens else {
        panic!("a line expected");
    };
    assert!(why.contains("still deploying"), "{why}");
}

#[test]
fn how_to_check_is_the_changed_specs_and_tests() {
    let suite = pv::Suite {
        dir: "apps/web/e2e".into(),
        runner: "playwright".into(),
    };
    let mut c = changed();
    c.push("apps/web/e2e/tests/signup.spec.ts".into());
    let how = pv::how_to_check(&c, Some(&suite), "npx vitest run");
    assert!(
        how.contains("npx playwright test tests/signup.spec.ts"),
        "{how}"
    );
    assert!(how.contains("apps/web/src/test/lib/validation.test.ts"));
}

#[test]
fn the_run_page_says_what_it_waits_on_in_one_line() {
    let standing = common::merge_standing::decide(
        "org/web",
        &common::merge_standing::Facts {
            head: "abc1234def".into(),
            reviewer_on: true,
            own_verdict: Some(("APPROVE".into(), "abc1234def".into())),
            auto_merge: true,
            require_human_approval: true,
            ..Default::default()
        },
    );
    let preview = pv::State {
        stage: Stage::Checked,
        ..Default::default()
    };
    let line = common::waiting_line::line(&common::waiting_line::Inputs {
        pr: 42,
        standing: Some(&standing),
        hold: None,
        preview: Some(&preview),
        checks_passed: true,
    });
    assert_eq!(
        line,
        "Waiting on: an approving review from a person on #42 (this repo requires one before CoderHelm merges). Ready for review: checks passed, preview checked."
    );
}
