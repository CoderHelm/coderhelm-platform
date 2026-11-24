//! dq. A run on a large ticket finished part of its planned tasks and then
//! spent several steps reading the same files: each step searched, read, ran
//! the checks and wrote nothing, because the files it was told were missing
//! were already in their planned form. The guard ended it with "CoderHelm
//! stopped this run and handed it to you: it continued N times without
//! finishing another task ... Push to the branch or comment on the pull
//! request". A later run on another ticket did the same on one README line:
//! every step re-read the file, thought to the output limit and wrote
//! nothing, until the run stopped with the work on a held draft.
//!
//! The measuring and the cut replies are fixed on their own (`u`, the cut
//! retry in `agent::anthropic`), and a stuck task now climbs the
//! supervisor's ladder: one task, fresh context with edits in place, a
//! re-split, the strongest model, then the task deferred. But the guard's
//! count of continuations without a finished task still ended the run one
//! step after the ladder deferred the first stuck task, with the old words
//! that hand the work to a person, and its stop stayed on the run: the draft
//! it shipped could not take a CI fix round, because the round's first pass
//! hit the stop and the run ended failed instead of fixed.
//!
//! Guarantee: a run whose planned tasks stop moving changes approach first
//! (the ladder runs before the guard's count is spent), and when the count
//! is spent it ships what is done with the rest deferred, in words that say
//! so. It is a shipping stop: it is taken back once the work is on its pull
//! request, so CI's errors and comments start the next round. A CoderHelm
//! limit never hands unfinished work to a person.

use super::fixture::fn_src;
use crate::passes::run_guard::*;
use crate::passes::steps;
use crate::passes::supervisor::{self, Rung};

/// What a recorded implement step cost, in USD (the README run's steps
/// spent a few dollars each).
const STEP_USD: f64 = 2.6;

/// The recorded shape: the last task finished in the step before the first
/// hand-off; from then on each step writes nothing and the branch's head
/// stays where it is. Each hand-off is counted by the guard, then the next
/// step starts and the supervisor reads its stall from the step marks. The
/// ladder's rungs in order, and the guard's stop when it fired.
fn replay() -> (Vec<Rung>, Option<Trip>, GuardState) {
    let at = "implement_started";
    let head = "a1b2c3d";
    let mut g = GuardState::default();
    assert!(g.apply(&Progress::TasksDone(17)));
    // The mark the last productive step started at: its head moved since.
    let mut prev = Some(steps::progress_mark("d3c2b1a", at));
    let mut stalled = 0;
    let mut rungs = Vec::new();
    for _ in 0..20 {
        // The step before hands off: no task finished.
        g.on_continuation_to(STEP_USD, &Target::default());
        if let Some(trip) = g.trip() {
            return (rungs, Some(trip), g);
        }
        // The next step starts and compares where the run stands.
        let now = steps::progress_mark(head, at);
        stalled = steps::stalled_after(prev.as_deref(), &now, stalled);
        let rung = supervisor::next_rung(stalled, true);
        rungs.push(rung);
        match rung {
            // A deferred task is settled: the next step counts from clean.
            Rung::Defer => stalled = 0,
            Rung::Ship => return (rungs, None, g),
            _ => {}
        }
        prev = Some(now);
        // Its plan check: the same 17 done, the next stuck task first.
        assert!(!g.apply(&Progress::TasksDone(17)));
    }
    (rungs, None, g)
}

#[test]
fn a_stuck_task_changes_approach_before_the_guard_stops_the_run() {
    let (rungs, _, _) = replay();
    for r in [
        Rung::Narrow,
        Rung::SwitchTools,
        Rung::Resplit,
        Rung::Escalate,
        Rung::Defer,
    ] {
        assert!(
            rungs.contains(&r),
            "the ladder never reached {r:?}: {rungs:?}"
        );
    }
    // Never the same ask twice in a row after a step that changed nothing.
    for w in rungs.windows(2) {
        assert!(w[0] == Rung::Continue || w[0] != w[1], "{rungs:?}");
    }
}

#[test]
fn a_spent_count_with_tasks_open_ships_and_is_never_handed_back() {
    let (_, trip, mut g) = replay();
    let trip = trip.expect("the guard's count is spent with tasks still open");
    assert_eq!(trip, Trip::Continuations);
    assert!(trip.ships(), "a CoderHelm limit ships what is done");
    assert!(
        ships_code(trip.code()),
        "the stop is taken back once the work is on its pull request"
    );
    let why = trip.reason(&g);
    assert!(!why.contains("handed it to you"), "{why}");
    assert!(!why.contains("without finishing another task"), "{why}");
    assert!(why.contains("shipped"), "{why}");
    assert!(why.contains("deferred"), "{why}");
    assert!(why.contains("fixes what CI reports"), "{why}");
    // Shipping starts the counts over: the rounds CI or a comment starts
    // aren't stopped by the steps before them.
    g.shipped();
    assert_eq!(g.trip(), None);
    // The hard stops stay hard.
    assert!(!ships_code(Trip::Spend.code()));
    assert!(!ships_code(Trip::Transitions.code()));
    assert!(!ships_code("ceiling_continuations"));
}

/// A guard stop in a ticket step goes to the shipping draft, and that draft
/// takes back a shipping stop; a run with its pull request takes it back
/// before any hand-back is judged.
#[test]
fn the_pipeline_ships_and_takes_back_the_guard_stop() {
    let guard = include_str!("../run_guard.rs");
    let end = fn_src(guard, "\npub async fn end_if_stopped(");
    assert!(end.contains("stop_pr::open_with("), "{end}");
    assert!(end.contains("lift_shipped_stop("), "{end}");
    let settle = fn_src(guard, "\npub async fn settle_open_pr(");
    assert!(
        settle.find("ships_code(").unwrap() < settle.find("stop_judge").unwrap(),
        "a shipping stop is never judged as a hand-back"
    );
}
