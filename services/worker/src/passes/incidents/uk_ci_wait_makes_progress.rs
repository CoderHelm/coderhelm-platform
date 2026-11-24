//! uk. A run finished its work and waited for its pull request's checks.
//! One workflow passed; another never left `queued`. Every two minutes the
//! run asked GitHub again and armed another check, and webhook resumes in
//! between armed a second chain, so it checked about once a minute. After
//! eight hours and almost 500 checks nothing had changed, and nothing said
//! which check it was waiting on. The 24-hour cap measured from the run's
//! last write, which every check refreshed.
//!
//! Guarantee: the wait keeps the pending set and when it last changed
//! (`common::ci_watch`). An hour with no progress re-runs the stuck checks'
//! workflows once; a second hour stops the wait, names the stuck checks,
//! and finishes the run with its pull request open for the ready decision.
//! A run has one wait chain: a delayed check is armed only when none is
//! due.

use super::fixture::fn_src;
use common::ci_watch::{self, Step, STALL_SECS};
use serde_json::json;

#[test]
fn a_wait_with_no_progress_reruns_once_then_stops_and_names_the_check() {
    let checks = json!({"total_count": 4, "check_runs": [
        {"name": "Web App Lint", "status": "completed", "conclusion": "success"},
        {"name": "Unit Tests", "status": "completed", "conclusion": "success"},
        {"name": "E2E on preview", "status": "queued",
         "details_url": "https://github.com/o/web/actions/runs/9001/job/3"},
        {"name": "preview/deploy", "status": "in_progress", "details_url": "https://hosting.example/d/1"}
    ]});
    let pending = ci_watch::pending_of(&checks);
    assert_eq!(ci_watch::pending_run_ids(&checks), vec![9001]);
    // The real run: a check every ~60 s for eight hours, nothing moving.
    let mut w = None;
    let mut steps = Vec::new();
    for i in 0..480i64 {
        let (next, s) = ci_watch::step(w.as_ref(), "f00d123", &pending, i * 60);
        if s != Step::Wait {
            steps.push((i, s));
        }
        w = Some(next);
        if s == Step::Stop {
            break;
        }
    }
    assert_eq!(steps[0], (STALL_SECS / 60, Step::Rerun));
    assert_eq!(steps[1], (2 * STALL_SECS / 60, Step::Stop));
    let note = ci_watch::stop_note(w.as_ref().unwrap(), 2 * STALL_SECS);
    assert!(note.contains("E2E on preview (queued)"), "{note}");
    assert!(note.contains("preview/deploy (in_progress)"));
}

#[test]
fn the_wait_is_watched_and_has_one_chain() {
    let src = include_str!("../resume.rs");
    let pending = src
        .find("\"A workflow passed but others are still running — waiting for full CI\"")
        .unwrap();
    assert!(src[pending..pending + 900].contains("ci_wait_watchdog("));
    let watch = fn_src(src, "\nasync fn ci_wait_watchdog(");
    assert!(watch.contains("rerun_workflow_run(") && watch.contains("set_run_complete("));
    assert!(watch.contains("mark_pr_ready("));
    let send = fn_src(src, "\nasync fn send_delayed_resume(");
    let arm = send.find("arm_delayed_check(").expect("one chain");
    let count = send.find("may_poll_again(").unwrap();
    assert!(arm < count, "only the armed chain counts a poll");
    assert!(!ci_watch::may_arm(Some(1_000), 800));
}
