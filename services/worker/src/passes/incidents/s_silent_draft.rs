//! s. A new-feature run checked its rules on the pull request: one checklist
//! item failing, then after a fix round none, but four of CoderHelm's own
//! sandbox checks failing on the new head. The repository's CI passed, the
//! review ran and approved, and the run logged "Review passed — completing
//! run", "Run completed" and "Run handed its PR over — review queued". No
//! "PR marked ready" and no "stays a draft" line was ever logged: the
//! mark-ready step had returned on the sandbox failures without a word. The
//! pull request stayed a draft, the run page said completed, and nobody was
//! told why. The same day, mark-ready messages that hit GitHub's rate limit
//! were logged as "Mark PR ready failed" and dropped, leaving five more pull
//! requests drafts.
//!
//! Guarantee: one decision (`ready::decide`) settles every pull request a
//! finished run leaves, on every path. With the repository's CI green and
//! CoderHelm's review approving, CoderHelm's own sandbox failures and
//! checklist items never hold the draft: the pull request is marked ready
//! with them under Open items. A hold that remains names each reason and
//! what to do, at the top of the description, at warn in the log, and on the
//! run, which is `needs_input` (never plain completed) while a person must
//! act. A mark-ready GitHub refuses is sent again after its reset, a bounded
//! number of times, and the last refusal is recorded on the run. A sweep
//! decides again for finished runs whose pull request is still a draft with
//! no recorded reason, so earlier casualties heal by themselves.

use super::fixture::*;
use crate::models::{MarkReadyMessage, WorkerMessage};
use crate::passes::feature::FeatureHold;
use crate::passes::ready::{self, Ci, Decision, Facts};
use common::draft_hold::{DraftHold, Kind, Reason};

/// Nothing was said: the old failure is the absence of any line.
const OLD: &str = "Mark PR ready failed";

fn the_four_sandbox_failures() -> FeatureHold {
    FeatureHold {
        sandbox_failed: vec![
            "`pnpm test --filter api`: 3 failed".into(),
            "`pnpm lint`: 2 errors".into(),
            "`pnpm typecheck`: 1 error".into(),
            "`pnpm test:e2e card`: 1 failed".into(),
        ],
        checklist_fails: vec![],
    }
}

fn the_pull_request_at_completion() -> Facts {
    Facts {
        draft: true,
        title: "GH-41: Add the card block".into(),
        body: "## Summary\nAdds the card block.\n\n## Verification\n- CI.".into(),
        feature: Some(the_four_sandbox_failures()),
        // The repository's CI on the head, CoderHelm's own review check left out.
        ci: ready::ci_standing(
            &[
                serde_json::json!({"name": "build", "status": "completed", "conclusion": "success",
                    "started_at": "2026-11-06T22:15:00Z", "app": {"slug": "github-actions"}}),
                serde_json::json!({"name": "test", "status": "completed", "conclusion": "success",
                    "started_at": "2026-11-06T22:15:00Z", "app": {"slug": "github-actions"}}),
                serde_json::json!({"name": common::review_check::CHECK_NAME, "status": "completed",
                    "conclusion": "success", "started_at": "2026-11-06T22:17:00Z",
                    "app": {"slug": "coderhelm"}}),
            ],
            &[],
        ),
        approved: true,
        ..Default::default()
    }
}

#[test]
fn a_green_approved_pr_is_marked_ready_with_the_sandbox_failures_listed() {
    let mut end = Ending {
        status: "completed".into(),
        pr: Some(21),
        draft: true,
        ..Default::default()
    };
    let f = the_pull_request_at_completion();
    assert_eq!(
        f.ci,
        Ci {
            green: true,
            why: String::new()
        }
    );
    let d = ready::decide(&f);
    let open = match &d {
        Decision::Ready { open } => open.clone(),
        other => panic!("held on CoderHelm's own sandbox: {other:?}"),
    };
    assert_eq!(open.len(), 4);
    end.draft = false;
    let body = ready::body_for(&f.body, &d).expect("the description lists them");
    assert!(body.contains("## Open items"));
    for o in &open {
        assert!(body.contains(o.as_str()), "{o} in {body}");
    }
    assert!(body.contains("this doesn't keep the draft: look at it before merging"));
    assert!(!body.contains(ready::STATUS_START));
    end.said(body);
    assert!(!end.draft);
    assert_eq!(end.status, "completed");
    end.never_says(OLD);
}

#[test]
fn a_hold_that_remains_is_named_everywhere_and_the_run_waits_on_a_person() {
    let mut end = Ending {
        status: "completed".into(),
        pr: Some(21),
        draft: true,
        ..Default::default()
    };
    // The same pull request with CI red: the sandbox failures hold, by name.
    let f = Facts {
        ci: Ci {
            green: false,
            why: "CI fails on this head: `test`".into(),
        },
        ..the_pull_request_at_completion()
    };
    let d = ready::decide(&f);
    let Decision::Held { reasons } = &d else {
        panic!("{d:?}")
    };
    // The description says it at the top, with what to do.
    let body = ready::body_for(&f.body, &d).unwrap();
    assert!(body.starts_with(ready::STATUS_START), "{body}");
    assert!(body.contains("**This pull request stays a draft.**"));
    assert!(body.contains("`pnpm lint`: 2 errors"));
    assert!(body.contains("mark it ready yourself"));
    end.said(body);
    // The run says it, and waits on a person instead of reading completed.
    let hold = DraftHold::new(
        Kind::Held,
        reasons.clone(),
        21,
        "https://github.com/acme/site/pull/21",
    )
    .unwrap();
    assert!(hold.needs_person());
    let summary = hold.summary();
    assert!(summary.starts_with("Finished, but pull request #21 stays a draft: "));
    assert!(summary.contains("CI fails on this head: `test`"));
    end.said(summary);
    let wait = hold.waiting_on();
    assert_eq!(wait.action, "Deal with what keeps pull request #21 a draft");
    end.status = common::run_status::NEEDS_INPUT.into();
    end.said(wait.line());
    assert_eq!(end.status, "needs_input");
    assert!(end.draft);
    end.never_says(OLD);
}

#[test]
fn every_path_goes_through_the_one_decision_and_no_hold_returns_silently() {
    // The run's own completion and the mark-ready message.
    let resume = include_str!("../resume.rs");
    let mark = fn_src(resume, "pub(crate) async fn mark_pr_ready(");
    assert!(mark.contains("super::ready::settle(&ctx)"));
    assert!(!mark.contains("return;"), "no early silent return");
    assert!(fn_src(resume, "pub(crate) async fn ready_or_prove(").contains("ready::settle(&ctx)"));
    assert!(
        fn_src(resume, "pub(crate) async fn mark_e2e_pr_ready(").contains("ready::settle(&ctx)")
    );
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::ready::on_message(&state, msg)"));
    assert!(!main.contains("\nasync fn mark_pr_ready("));
    // Each outcome is said: a hold at warn, on the description and the run.
    let src = include_str!("../ready.rs");
    let settle = fn_src(src, "pub async fn settle(");
    let held = &settle[settle.find("Decision::Held { reasons } => {").unwrap()..];
    let held = &held[..held.find("Decision::Wait").unwrap()];
    assert!(held.contains("warn!("));
    assert!(held.contains("write_body("));
    assert!(held.contains("record_hold("));
    // A run that finishes after its hold was recorded is needs_input.
    let end_run = fn_src(resume, "\nasync fn end_run(");
    assert!(end_run.contains("ready::after_completion(state, &msg.team_id, run_id)"));
    // The sweep runs with the stuck-review sweep.
    assert!(main.contains("passes::ready::sweep(&state)"));
}

#[test]
fn a_refused_mark_ready_is_tried_again_and_the_last_refusal_is_recorded() {
    let mut end = Ending {
        status: "completed".into(),
        pr: Some(1387),
        draft: true,
        ..Default::default()
    };
    // The message that was dropped, as the webhook sent it: it still parses.
    let old = r#"{"type":"mark_ready","team_id":"TEAM#1","installation_id":41,"repo_owner":"acme","repo_name":"web","pr_number":1387}"#;
    let WorkerMessage::MarkReady(m) = serde_json::from_str::<WorkerMessage>(old).unwrap() else {
        panic!()
    };
    assert_eq!(m.attempt, 0);
    // GitHub's limit: the next try waits for the reset, a bounded number of
    // times.
    let src = include_str!("../ready.rs");
    let retry = fn_src(src, "async fn retry_or_give_up(");
    assert!(retry.contains("attempt <= common::github_wait::MAX_WAITS"));
    assert!(retry.contains("not_before: common::github_wait::until(now, r.wait_secs)"));
    assert!(retry.contains("Kind::Failed"));
    let next = MarkReadyMessage {
        attempt: m.attempt + 1,
        ..m
    };
    assert_eq!(next.attempt, 1);
    // An unreadable timeline is a try again, never "leave the draft as is".
    let gather = fn_src(src, "async fn gather(");
    assert!(gather.contains("the pull request's timeline couldn't be read"));
    assert!(!src.contains("leaving the draft as is"));
    // After every try: recorded on the run like a hold, with what to do.
    let failed = DraftHold::new(
        Kind::Failed,
        vec![Reason::new(
            "GitHub didn't mark it ready (GitHub's rate limit) on each of 8 tries",
            "Mark it ready for review on GitHub yourself; CoderHelm also tries again within the \
             hour.",
        )],
        1387,
        "",
    )
    .unwrap();
    assert!(failed.needs_person());
    end.status = common::run_status::NEEDS_INPUT.into();
    end.said(failed.summary());
    assert!(end.says("couldn't mark it: GitHub didn't mark it ready (GitHub's rate limit)"));
    end.never_says(OLD);
}

#[test]
fn the_sweep_heals_a_finished_run_left_a_draft_with_no_reason() {
    let now = chrono::Utc::now();
    let casualty = ready::Candidate {
        team_id: "TEAM#1".into(),
        run_id: "01RUNDRAFT".into(),
        status: "completed".into(),
        owner: OWNER.into(),
        repo: REPO.into(),
        pr: 21,
        installation_id: 41,
        hold: None,
    };
    assert_eq!(ready::sweep_step(&casualty, now), ready::SweepStep::Decide);
    // A person's draft and a closed pull request are respected by the
    // decision it makes.
    let theirs = Facts {
        put_back_by: Some("dana".into()),
        ..the_pull_request_at_completion()
    };
    assert!(matches!(ready::decide(&theirs), Decision::Held { reasons }
        if reasons[0].what == "`dana` put it back to draft"));
    let closed = Facts {
        closed: true,
        ..the_pull_request_at_completion()
    };
    assert_eq!(ready::decide(&closed), Decision::Closed);
}
