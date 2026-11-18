//! da. Two runs on one ticket finished every planned task, opened their draft
//! pull requests, and were then stopped with "CoderHelm stopped this run and
//! handed it to you: its tasks were done and it had continued 4 more times
//! without finishing". The four continuations were: two steps that ran out of
//! time inside the proof check's and the rules' fix rounds (each round closed
//! a finding), one step the run asked for by design so the changed tests could
//! run with and without the change, and the step whose fix round that check
//! started. A fixed count of continuations after the work stopped a run that
//! was still closing things. The stop left `guard_open` on the run, so when
//! the pull request's `Deploy Preview` check went red with a readable error,
//! the CI fix round's first pass hit the stop and the run was failed instead
//! of fixed.
//!
//! Guarantee: there is no count of continuations after the work. Each
//! continuation names what it is for, the checks not yet run and the items
//! they left open (`run_guard::Target`), and the run goes on while each one
//! closes something. A step the run asks for by design is never counted. Once
//! continuing closes nothing (or nothing is left), the run ships: the pull
//! request carries what is open, the stop is taken back at once
//! (`run_guard::lift_shipped_stop`), and a red check with its error or a
//! comment starts the next round. Finished work is never handed back and
//! never failed for a stop.

use super::fixture::fn_src;
use crate::passes::run_guard::*;

/// The checks a run keeps, at the start of the checks after the work: none
/// has given a result.
fn ledger() -> Ledger {
    Ledger::default()
}

fn finding(point: &str, detail: &str) -> crate::passes::proof::Finding {
    crate::passes::proof::Finding {
        point: point.into(),
        detail: detail.into(),
        ..Default::default()
    }
}

/// The recorded run's steps after its tasks were done, each continuation's
/// target read from the checks' ledger as that step left it.
fn replay() -> (GuardState, Vec<Option<Trip>>) {
    let mut g = GuardState::default();
    g.apply(&Progress::TasksDone(6));
    g.apply(&Progress::WorkDone);
    g.apply(&Progress::PrOpen);
    let mut trips = Vec::new();
    // Step 3: the test and security audit need a fresh step; no check ran.
    let mut l = ledger();
    g.on_continuation_to(0.7, &Target::of(&l));
    trips.push(g.trip());
    // Step 4: the proof check ran and its fix round closed some of its gaps;
    // time ran out in the round.
    l.proof.head = "h1".into();
    l.proof_findings = Some(vec![
        finding("ci_pending", "the build isn't run"),
        finding("ci_pending", "the new spec hasn't run"),
        finding("team_rule", "blocks are tested on the components page"),
    ]);
    g.on_continuation_to(1.2, &Target::of(&l));
    trips.push(g.trip());
    // Step 5: the rules ran and their round fixed what they found.
    l.rules_outcome = Some(Ok(None));
    l.links_outcome = Some(Ok(String::new()));
    l.test_outcome = Some(Ok(None));
    g.on_continuation_to(1.4, &Target::of(&l));
    trips.push(g.trip());
    // Step 6: the changed tests need a step of their own, by design.
    g.on_continuation_to(
        0.1,
        &Target {
            owed: true,
            ..Default::default()
        },
    );
    trips.push(g.trip());
    // Step 7: the tests and scope check ran; its fix round worked on what it
    // found, and the step ran out of time.
    l.habits_outcome = Some(Ok(None));
    g.on_continuation_to(1.1, &Target::of(&l));
    trips.push(g.trip());
    (g, trips)
}

#[test]
fn a_run_that_keeps_closing_things_is_never_stopped() {
    let (g, trips) = replay();
    assert!(trips.iter().all(Option::is_none), "{trips:?}");
    assert_eq!(g.stale, 0);
    // Only the owed step went uncounted.
    assert_eq!(g.after_work, 4);
}

#[test]
fn a_run_that_stops_closing_things_ships_and_is_never_handed_back() {
    let (mut g, _) = replay();
    let l = {
        let mut l = ledger();
        l.proof.head = "h2".into();
        l.proof_findings = Some(vec![finding("ci_pending", "the build isn't run")]);
        l.rules_outcome = Some(Ok(None));
        l.links_outcome = Some(Ok(String::new()));
        l.test_outcome = Some(Ok(None));
        l.habits_outcome = Some(Ok(None));
        l
    };
    // The same open item, step after step: no check left to run and nothing
    // closed.
    g.on_continuation_to(1.0, &Target::of(&l));
    g.on_continuation_to(1.0, &Target::of(&l));
    assert_eq!(g.trip(), None, "one stale step is allowed");
    g.on_continuation_to(1.0, &Target::of(&l));
    let trip = g.trip().expect("it stops continuing");
    assert_eq!(trip, Trip::Converged);
    assert!(trip.ships());
    let why = trip.reason(&g);
    assert!(!why.contains("handed it to you"), "{why}");
    assert!(why.contains("Open items"), "{why}");
    // Older runs stopped by the count of continuations ship the same way.
    assert!(ships_code(trip.code()));
    assert!(ships_code("continuations_after_work"));
    assert!(!ships_code("spend"));
    assert!(!ships_code("ceiling_spend"));
    // Shipping starts convergence over: the rounds CI or a comment starts
    // aren't stopped by the steps before them.
    g.shipped();
    assert_eq!(g.trip(), None);
}

/// Every continuation names its target from the run's checks, the changed
/// tests' own step is owed, the draft's stop is taken back once the work is
/// on the pull request, and a red check's fix round takes it back before its
/// first pass.
#[test]
fn the_pipeline_targets_each_continuation_and_takes_back_a_shipping_stop() {
    let src = include_str!("../mod.rs");
    let cont = fn_src(src, "\nasync fn try_auto_continue(");
    assert!(
        cont.contains("run_guard::on_continuation(state, &msg.team_id, run_id, &scope, owed)"),
        "{cont}"
    );
    // The hand-off in the checks (the last mention: a test lists it first).
    let owed = src
        .rfind("\"Running the changed tests with and without the change needs a step of its own\",")
        .expect("the changed tests' hand-off");
    assert!(src[owed..owed + 200].contains("true,"));
    let guard = include_str!("../run_guard.rs");
    let end = fn_src(guard, "\npub async fn end_if_stopped(");
    assert!(end.contains("lift_shipped_stop("), "{end}");
    let settle = fn_src(guard, "\npub async fn settle_open_pr(");
    assert!(
        settle.find("lift_shipped_stop(").unwrap() < settle.find("stop_judge").unwrap(),
        "a shipping stop is never judged as a hand-back"
    );
    let resume = include_str!("../resume.rs");
    let lift = resume
        .find("super::run_guard::lift_shipped_stop(state, &msg.team_id, &msg.run_id).await;")
        .expect("the CI fix round takes back a shipping stop");
    let pass = resume[lift..]
        .find("super::update_pass(state, &msg.team_id, &msg.run_id, \"implement\")")
        .expect("then starts its pass");
    assert!(pass < 200);
}
