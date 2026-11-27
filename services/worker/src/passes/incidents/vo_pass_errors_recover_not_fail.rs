//! vo. Any error a pass returned that the run had no special case for
//! ended the run as failed, usually with the raw HTTP error as its reason:
//! a 5xx in the middle of the coder's work, a branch that wasn't there, a
//! base branch someone deleted. The run tried the same thing again in a
//! fresh step without changing anything until its ceilings ran out.
//!
//! Guarantee: every pass error takes one recovery path before anything is
//! marked failed: transient errors are tried again after a backoff, states
//! CoderHelm repairs are repaired and the same pass resumes from its
//! checkpoint (no second plan, no repeated ticket or pull request), the
//! same error again with no progress takes the next strategy, and only a
//! person's step stops a run, as "needs you" in plain words. An error
//! nobody classified is tried once more, then stops as "needs you", never
//! as failed. Failed runs CoderHelm now gets past are resumed by the
//! self-heal sweep, on deploy and every 15 minutes, once per run per
//! deploy.

use super::fixture::*;
use crate::passes::recover::replay::{Fault, World};
use common::run_recovery::{self as rr, Kind};

#[tokio::test]
async fn a_5xx_in_the_middle_of_implement_resumes_and_finishes() {
    let mut w = World::new("acme", "site", &["site", "api"], "tk-2");
    w.faults
        .push_back(Fault::ServerErrorAfterWrite("site".into()));
    assert!(w.run_to_end(6).await);
    w.finished_once();
    // The work written before the 5xx is kept: nothing is written twice.
    assert!(w.writes.values().all(|n| *n == 1), "{:?}", w.writes);
    let notes = w.run.notes.lock().unwrap().join("\n");
    assert!(notes.contains("GitHub didn't answer for a moment (502 Bad Gateway)"));
    assert!(notes.contains("Trying the same pass again from its checkpoint in 30 seconds"));
}

#[tokio::test]
async fn a_404_on_a_missing_branch_is_repaired_and_finishes() {
    let mut w = World::new("acme", "site", &["site"], "tk-3");
    w.faults
        .push_back(Fault::BranchDeletedAfterWrite("site".into()));
    assert!(w.run_to_end(6).await);
    w.finished_once();
    let notes = w.run.notes.lock().unwrap().join("\n");
    assert!(notes.contains("Repaired:"), "{notes}");
    assert!(!notes.contains("api.github.com"));
}

#[tokio::test]
async fn a_deleted_base_branch_moves_to_the_default_and_finishes() {
    let mut w = World::new("acme", "site", &["site"], "tk-4");
    w.host.remove("acme/site", "main");
    w.host.add("acme/site", "trunk");
    w.host
        .default
        .lock()
        .unwrap()
        .insert("acme/site".into(), "trunk".into());
    assert!(w.run_to_end(6).await);
    w.finished_once();
    assert_eq!(w.base, "trunk");
    assert!(w.prs.values().all(|base| base == "trunk"));
}

#[tokio::test]
async fn an_unclassified_error_is_tried_once_then_needs_you_never_failed() {
    let odd = "The frobnicator returned an answer nobody expected";
    let mut w = World::new("acme", "site", &["site"], "tk-5");
    w.faults.push_back(Fault::Unclassified(odd.into()));
    w.faults.push_back(Fault::Unclassified(odd.into()));
    assert!(!w.run_to_end(6).await);
    let n = w.stopped.clone().expect("stopped for a person");
    assert_eq!(n.why, common::handback::HumanOnly::Unrepaired);
    assert!(n.action.contains("press Retry"));
    assert!(n.detail.contains("tried again once"));
    assert_eq!(rr::classify(odd).kind, Kind::Unknown);
    // The one failed-status write never writes it as failed.
    assert_ne!(
        common::run_fail::status_for(odd),
        common::run_status::FAILED
    );
    // Tried once more first: one retry, then the stop.
    assert_eq!(w.steps, 2);
    // Once it gets past it, the next error starts its own count.
    let mut w = World::new("acme", "site", &["site"], "tk-6");
    w.faults.push_back(Fault::Unclassified(odd.into()));
    assert!(w.run_to_end(6).await);
    w.finished_once();
}

#[test]
fn the_orchestrator_recovers_before_anything_is_marked_failed() {
    let src = include_str!("../mod.rs");
    let arm = &src[src.find("Ok(Err(e)) => {").expect("the error arm")..];
    let recover = arm
        .find("recover::after_error(")
        .expect("the recovery path");
    let fail = arm
        .find("fail_run(state, &msg, &run_id, &sanitized")
        .expect("the one failed write");
    let person = arm.find("recover::stop_for_person(").expect("needs you");
    assert!(recover < fail && recover < person);
    // Every failed write reads the reason first: an unclassified one waits
    // on a person.
    let fail_fn = fn_src(src, "pub(crate) async fn fail_run(");
    assert!(fail_fn.contains("common::run_fail::status_for(error_msg)"));
    let funnel = include_str!("../../../../common/src/run_fail.rs");
    assert!(funnel.contains("rr::ending(&raw)"));
    // The sweep runs every 15 minutes and on each release's first start.
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::recover::on_new_release(&state).await;"));
    assert!(main.contains("passes::recover::self_heal_sweep(&state).await;"));
    let infra = include_str!("../../../../../infra/lib/api-stack.ts");
    let rule = &infra[infra
        .find("\"SelfHealSweepSchedule\"")
        .expect("the schedule")..];
    assert!(rule[..600].contains("Duration.minutes(15)"));
    assert!(rule[..600].contains("type: \"self_heal_sweep\""));
}
