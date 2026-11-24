//! bu. A release fixed how a finished run's held draft is finished (a round
//! that answered a review finding with proof is read by a review of the
//! same head). The run it was for never got there. Its hold had been
//! decided shortly before the release, and the draft sweep re-decides a
//! held draft only once its hold is an hour old: the first sweep after the
//! release came seconds early and only peeked. The next one, a while
//! later, found less than half of GitHub's hourly budget left and
//! skipped the installation, logging "it looks again in about 1 min"; the
//! budget refilled seconds later, but nothing ran until the next
//! scheduled sweep. The pull request watch, which also finishes held
//! drafts, looked at none of its candidates for several sweeps: its hourly
//! cap went to merged pull requests looked at less recently. A person reset
//! the run over an hour after the release.
//!
//! Guarantee: a hold carries the version of the code that decided it, and
//! the sweep decides a hold other code wrote on its first pass after a
//! release, decisions before peeks. A sweep GitHub's budget skipped runs
//! again once the budget refills, as its log line says. The pull request
//! watch looks at held drafts first.

use crate::passes::{pr_watch, ready};
use common::draft_hold::{DraftHold, Kind, Reason};

const OLD: &str = "older-code";
const NEW: &str = "this-code";

fn held_by(version: &str, checked_secs_ago: i64) -> DraftHold {
    let mut h = DraftHold::new(
        Kind::Held,
        vec![Reason::new(
            "1 finding from CoderHelm's review is still open",
            "",
        )],
        7,
        "",
    )
    .unwrap();
    h.version = version.into();
    h.checked_at = (chrono::Utc::now() - chrono::Duration::seconds(checked_secs_ago)).to_rfc3339();
    h
}

fn candidate(status: &str, hold: Option<DraftHold>) -> ready::Candidate {
    ready::Candidate {
        team_id: "TEAM#1".into(),
        run_id: "01RUNHELD".into(),
        status: status.into(),
        owner: "acme".into(),
        repo: "api".into(),
        pr: 7,
        installation_id: 41,
        hold,
    }
}

#[test]
fn a_hold_older_code_decided_is_decided_again_on_the_first_sweep() {
    let now = chrono::Utc::now();
    // Decided 59 minutes 50 seconds ago, by the code before the release.
    let stuck = candidate("completed", Some(held_by(OLD, 3590)));
    assert_eq!(
        ready::sweep_step(&stuck, now, NEW),
        ready::SweepStep::Decide
    );
    // The same hold decided by this code is only peeked at until it is old.
    let current = candidate("completed", Some(held_by(NEW, 3590)));
    assert_eq!(
        ready::sweep_step(&current, now, NEW),
        ready::SweepStep::Peek
    );
    // A run waiting on a person for its draft is decided again too.
    let waiting = candidate("needs_input", Some(held_by(OLD, 60)));
    assert_eq!(
        ready::sweep_step(&waiting, now, NEW),
        ready::SweepStep::Decide
    );
    // A run still working is left to finish.
    let working = candidate("running", Some(held_by(OLD, 60)));
    assert_eq!(
        ready::sweep_step(&working, now, NEW),
        ready::SweepStep::Skip
    );
    // A hold stored before holds carried a version is other code's.
    let stored: DraftHold = serde_json::from_value(serde_json::json!({
        "kind": "held",
        "reasons": [{"what": "1 finding from CoderHelm's review is still open"}],
        "pr_number": 7,
        "checked_at": now.to_rfc3339()
    }))
    .unwrap();
    assert!(stored.other_version(NEW));
    // Every hold the ready decision records names this code.
    let src = include_str!("../ready.rs");
    let record = super::fixture::fn_src(src, "\nasync fn record_hold(");
    assert!(record.contains("hold.version = super::proof::checks_version()"));
}

#[test]
fn decisions_come_before_peeks_so_a_short_budget_reaches_them() {
    let now = chrono::Utc::now();
    let peek = |n: &str| ready::Candidate {
        run_id: n.into(),
        ..candidate("completed", Some(held_by(NEW, 60)))
    };
    let stuck = ready::Candidate {
        run_id: "stuck".into(),
        ..candidate("completed", Some(held_by(OLD, 60)))
    };
    let order = ready::sweep_order(vec![peek("a"), peek("b"), stuck, peek("c")], now, NEW);
    let ids: Vec<&str> = order.iter().map(|c| c.run_id.as_str()).collect();
    assert_eq!(ids, vec!["stuck", "a", "b", "c"]);
}

#[test]
fn a_sweep_the_budget_skipped_runs_again_once_it_refills() {
    use crate::clients::github_budget::SweepGate;
    use common::github_budget::{caller as who, Budget};
    let id = 990_041;
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    // Less than half the hour's budget left, refilling in ten seconds.
    crate::clients::github::record_budget(
        id,
        Budget {
            remaining: 2_100,
            limit: 5_000,
            reset: now + 10,
        },
    );
    let mut gate = SweepGate::new(who::SWEEP_DRAFTS);
    assert!(!gate.check(id, 15, now));
    let wait = gate.retry_in().expect("the skip names its wait");
    assert!(wait <= 60, "{wait}");
    // The sweeps run again in about that long, not at the next schedule.
    let delay = ready::follow_up_delay(&[Some(wait), None]).unwrap();
    assert!((30..=60).contains(&delay), "{delay}");
    assert_eq!(ready::follow_up_delay(&[None, None]), None);
    // A wait past the next scheduled sweep is that sweep's.
    assert_eq!(ready::follow_up_delay(&[Some(2_000)]), None);
    assert_eq!(ready::follow_up_delay(&[Some(400), Some(90)]), Some(90));
    // Wired after the sweeps, with both sweeps' waits.
    let main = include_str!("../../main.rs");
    assert!(
        main.contains("passes::ready::sweep_follow_up(&state, &[drafts.retry_in, watch.retry_in])")
    );
}

#[test]
fn the_pull_request_watch_looks_at_held_drafts_first() {
    let c = |run: &str, at: &str, held: bool| pr_watch::Candidate {
        run_id: run.into(),
        looked_at: at.into(),
        draft_held: held,
        ..Default::default()
    };
    let picked = pr_watch::order(
        vec![
            c("merged-1", "", false),
            c("merged-2", "2026-01-01T00:00:00Z", false),
            c("held", "2026-01-02T00:00:00Z", true),
        ],
        2,
    );
    let ids: Vec<&str> = picked.iter().map(|c| c.run_id.as_str()).collect();
    assert_eq!(ids, vec!["held", "merged-1"]);
}
