//! y. A failing-test fix opened its pull request as a draft. CI passed and
//! CoderHelm's review approved, and the ready check kept the draft until the
//! checks that test the fix were green on its head. In that repository the
//! preview and staging deploys and the end-to-end tests run only on a pull
//! request that isn't a draft (`github.event.pull_request.draft == false`),
//! so on the draft every one of them was skipped: draft, so skipped, so
//! still a draft. CoderHelm tried to start them again, which a rerun can't
//! do (it replays the same draft event, and the app had no Actions write),
//! and then waited, deciding every ten minutes for six hours and then
//! giving up. Each recheck was decided twice, about fifteen seconds apart:
//! the run's own decision and the one its review verdict asked for each
//! scheduled its own next recheck, so two chains ran side by side and spent
//! the twelve-hour budget in six.
//!
//! Guarantees:
//! - Why a check was skipped is read from its workflow file (draft
//!   conditions, a trigger only on `ready_for_review`, the labels its
//!   conditions ask for), kept per repository.
//! - A draft never waits on checks that skip drafts. When the proof the
//!   decision waits on (the failing-test fix's checks, an end-to-end fix's
//!   repeat runs) skips drafts, the pull request is marked ready so it runs,
//!   the description says it is still being proven, and its result decides:
//!   ready once it passes, a hold with the reason when it fails.
//! - A check skipped for a label CoderHelm may not add (deploy and staging
//!   labels never are), or for a reason the workflow doesn't show, is a hold
//!   with one plain action, never a wait.
//! - Every wait is bounded: unchanged on the same head for two hours, it is
//!   a hold that names what didn't happen.
//! - One recheck is pending per pull request: a decision schedules the next
//!   one only when none is pending.

use common::draft_hold::{DraftHold, Kind, Reason};
use common::e2e::check_gate::{self, Standing};
use common::e2e::workflow_gate::{self, Skipped};

use super::fixture::fn_src;
use crate::passes::ready::{self, Ci, Decision, Facts};
use crate::passes::test_fix::{self, Hold};

/// What the ready decision said every ten minutes, twice.
const OLD_WAIT: &str = "skipped on this commit: `Deploy to Staging`, `E2E after staging deploy`";

/// The repository's staging workflow, as its conditions read.
const STAGING: &str = r#"
name: Staging
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review, labeled]
jobs:
  deploy:
    name: Deploy to Staging
    if: github.event.pull_request.draft == false && contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')
    runs-on: ubuntu-latest
    steps: []
  e2e:
    name: E2E after staging deploy
    needs: deploy
    runs-on: ubuntu-latest
    steps: []
"#;

/// The end-to-end tests on the preview: run on every pull request that
/// isn't a draft.
const PREVIEW: &str = r#"
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review]
jobs:
  e2e:
    name: E2E on preview
    if: ${{ !github.event.pull_request.draft }}
    runs-on: ubuntu-latest
"#;

fn head_checks() -> Vec<check_gate::Check> {
    let c = |name: &str, conclusion: &str, run: u64| check_gate::Check {
        name: name.into(),
        status: "completed".into(),
        conclusion: conclusion.into(),
        run_id: Some(run),
    };
    vec![
        c("Test", "success", 1),
        c("Deploy Web App", "skipped", 2),
        c("E2E on preview", "skipped", 2),
        c("Deploy to Staging", "skipped", 3),
        c("E2E after staging deploy", "skipped", 3),
    ]
}

fn gates() -> Vec<(String, Option<workflow_gate::WorkflowGate>)> {
    let (preview, staging) = (workflow_gate::read(PREVIEW), workflow_gate::read(STAGING));
    vec![
        ("E2E on preview".into(), Some(preview)),
        ("Deploy to Staging".into(), Some(staging.clone())),
        ("E2E after staging deploy".into(), Some(staging)),
    ]
}

fn facts(test_fix: Hold, draft: bool, body: &str) -> Facts {
    Facts {
        draft,
        body: body.into(),
        title: "fix(e2e): settle the venue menu before the click".into(),
        test_fix: Some(test_fix),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        approved: true,
        head: "bcf845d".into(),
        ..Default::default()
    }
}

#[test]
fn the_draft_is_marked_ready_so_the_checks_that_skip_drafts_run() {
    let standing = check_gate::standing(&head_checks(), &[]);
    // `Deploy Web App` deploys for people, not for tests: not gated.
    assert_eq!(
        standing.skipped,
        vec![
            "E2E on preview",
            "Deploy to Staging",
            "E2E after staging deploy"
        ]
    );
    // The old decision: a wait that could never end.
    assert!(standing
        .why_not()
        .contains(&OLD_WAIT[..OLD_WAIT.find(": ").unwrap()]));
    // The staging label is on the pull request: only the draft keeps all
    // three off, and the preview tests and staging both start on ready.
    let on_pr = vec!["ch-review".to_string(), "CI:DEPLOY_STAGING".into()];
    let skipped = workflow_gate::sort(&gates(), &on_pr);
    assert!(skipped.all_draft_gated());
    let hold = test_fix::decide(&[], &standing, &skipped, true, Some(true));
    assert_eq!(
        hold,
        Hold::RunsWhenReady(vec![
            "E2E on preview".into(),
            "Deploy to Staging".into(),
            "E2E after staging deploy".into()
        ])
    );
    let d = ready::decide(&facts(hold, true, ""));
    let Decision::Proving { why, reasons } = &d else {
        panic!("{d:?}")
    };
    assert!(reasons.is_empty());
    assert!(why.contains("run only on a pull request that isn't a draft"));
    let body = ready::body_for("Fixes the menu test.", &d).unwrap();
    assert!(body.contains(ready::PROVING_MARK));
    assert!(body.contains("it is still being proven"));
    assert!(body.contains("Don't merge it yet"));
    assert!(!body.contains("stays a draft"));

    // Marked ready: the checks start, and the decision keeps watching.
    let started = test_fix::decide(&[], &standing, &skipped, false, Some(true));
    assert!(matches!(&started, Hold::Wait(w) if w.contains("start now that it is marked ready")));
    let d = ready::decide(&facts(started, false, &body));
    assert!(matches!(&d, Decision::Proving { reasons, .. } if reasons.is_empty()));
    // They pass: ready, and the note goes.
    let d = ready::decide(&facts(Hold::Go, false, &body));
    assert_eq!(d, Decision::Ready { open: vec![] });
    let clean = ready::body_for(&body, &d).unwrap();
    assert!(!clean.contains(ready::PROVING_MARK));
    // They fail: a hold with the reason, on the ready pull request.
    let failed = Standing {
        failed: vec!["E2E after staging deploy".into()],
        ..Default::default()
    };
    let red = test_fix::decide(&[], &failed, &Skipped::default(), false, Some(true));
    assert!(matches!(&red, Hold::Failed(w) if w.contains("E2E after staging deploy")));
    let d = ready::decide(&facts(red, false, &body));
    assert!(
        matches!(&d, Decision::Proving { reasons, .. } if reasons[0].what.contains("isn't proven"))
    );
    let held = ready::body_for(&body, &d).unwrap();
    assert!(held.contains("it isn't proven") && held.contains("E2E after staging deploy"));
}

#[test]
fn a_check_skipped_for_a_label_coderhelm_may_not_add_is_one_action_never_a_wait() {
    let standing = check_gate::standing(&head_checks(), &[]);
    // No staging label: marking it ready alone wouldn't run the staging
    // checks. The preview tests still start on ready, so it is marked ready
    // first.
    let skipped = workflow_gate::sort(&gates(), &["E2E:nav".into()]);
    assert_eq!(skipped.draft_gated, vec!["E2E on preview"]);
    let hold = test_fix::decide(&[], &standing, &skipped, true, Some(true));
    assert_eq!(hold, Hold::RunsWhenReady(vec!["E2E on preview".into()]));
    // Once ready the staging checks are still skipped: a hold with one
    // plain action, on the ready pull request.
    let after = workflow_gate::sort(&gates()[1..], &["E2E:nav".into()]);
    let staging_only = Standing {
        skipped: vec![
            "Deploy to Staging".into(),
            "E2E after staging deploy".into(),
        ],
        ..Default::default()
    };
    let hold = test_fix::decide(&[], &staging_only, &after, false, Some(true));
    let Hold::Untriggered { todo, .. } = &hold else {
        panic!("{hold:?}")
    };
    assert_eq!(
        todo,
        "Add the `CI:DEPLOY_STAGING` label to run `Deploy to Staging`, `E2E after staging \
         deploy`, or mark it ready yourself if they aren't needed."
    );
    let proving_body = format!("{}\nx", ready::PROVING_MARK);
    let d = ready::decide(&facts(hold.clone(), false, &proving_body));
    assert!(
        matches!(&d, Decision::Proving { reasons, .. } if reasons.len() == 1 && reasons[0].todo.contains("CI:DEPLOY_STAGING"))
    );
    // On a draft with nothing that starts on ready: held, never a wait.
    let d = ready::decide(&facts(hold, true, ""));
    assert!(
        matches!(&d, Decision::Held { reasons } if reasons[0].what.contains("can't be proven yet"))
    );
    // CoderHelm never adds a deploy label just because a workflow asks.
    let src = include_str!("../review_pr.rs");
    let may = fn_src(src, "pub(crate) async fn may_add_label(");
    assert!(may.contains("is_deploy_label(label)"));
    assert!(common::e2e::workflow_gate::is_deploy_label(
        "CI:DEPLOY_STAGING"
    ));
}

#[test]
fn an_end_to_end_proof_that_skips_drafts_is_run_on_a_ready_pull_request() {
    let f = Facts {
        draft: true,
        title: format!(
            "{} settle the venue menu",
            crate::passes::e2e::FIX_TICKET_PREFIX
        ),
        runs_when_ready: vec!["E2E after staging deploy".into()],
        ..Default::default()
    };
    assert!(matches!(ready::decide(&f), Decision::Proving { reasons, .. } if reasons.is_empty()));
    // Without draft-gated checks it still waits for its repeat runs.
    let f = Facts {
        runs_when_ready: vec![],
        ..f
    };
    assert!(matches!(ready::decide(&f), Decision::Wait { .. }));
}

#[test]
fn a_wait_that_does_not_change_for_two_hours_is_a_hold() {
    let why = "this failing-test fix waits: the repeat run of the affected test hasn't passed yet";
    let t = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    };
    let mut prior = DraftHold::new(Kind::Waiting, vec![Reason::new(why, "")], 1508, "").unwrap();
    prior.since = "2026-11-10T06:35:47Z".into();
    prior.head = "bcf845d".into();
    let mut f = facts(
        Hold::Wait("the repeat run of the affected test hasn't passed yet".into()),
        true,
        "",
    );
    f.prior = Some(prior.clone());
    f.now = Some(t("2026-11-10T07:35:00Z"));
    assert!(matches!(ready::decide(&f), Decision::Wait { .. }));
    f.now = Some(t("2026-11-10T08:36:00Z"));
    let d = ready::decide(&f);
    let Decision::Held { reasons } = &d else {
        panic!("{d:?}")
    };
    assert!(reasons[0]
        .what
        .contains("hasn't changed in 2 hours on this head"));
    assert!(reasons[0].what.starts_with(why));
    // The hold it became stays one on that head, and a push starts over.
    let mut held = DraftHold::new(Kind::Held, reasons.clone(), 1508, "").unwrap();
    held.head = "bcf845d".into();
    f.prior = Some(held);
    assert!(matches!(ready::decide(&f), Decision::Held { .. }));
    f.head = "0a1b2c3".into();
    assert!(matches!(ready::decide(&f), Decision::Wait { .. }));
}

#[test]
fn one_recheck_is_pending_per_pull_request() {
    let now = 1_700_000_000;
    assert!(test_fix::may_schedule(None, now));
    // The chain's own recheck arrives at its due time (or a little early).
    assert!(test_fix::may_schedule(Some(now), now));
    assert!(test_fix::may_schedule(Some(now + 30), now));
    // A second decision mid-way (the review's verdict) adds no chain.
    assert!(!test_fix::may_schedule(Some(now + 585), now));
    let src = include_str!("../test_fix.rs");
    let gate = fn_src(src, "pub async fn ready_gate(");
    assert!(gate.contains("schedule_recheck("));
    assert!(
        !gate.contains("recheck_later("),
        "every recheck goes through the claim"
    );
    // Saving the gate keeps the claim: an update, never a put.
    let save = fn_src(src, "async fn save_gate(");
    assert!(save.contains(".update_item()") && !save.contains(".put_item()"));
    // A rerun replays the same event: only checks skipped for a reason the
    // workflow doesn't show are started again.
    assert!(gate.contains("skipped.unknown.contains"));
}
