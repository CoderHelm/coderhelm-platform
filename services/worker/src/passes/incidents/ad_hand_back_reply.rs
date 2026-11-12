//! ad. A coder ended its step with "I couldn't finish, please complete the
//! remaining tasks manually." Nothing it lacked was a credential, access, a
//! product decision, an ambiguous ticket or an approval: the words alone
//! handed the work to a person.
//!
//! Guarantee: a model reply that hands the work back for a reason not on
//! the human-only list is not an ending. The coder is told to change its
//! approach in the same pass; a reply that still changes nothing is a step
//! without progress, which climbs the supervisor's ladder (one task, other
//! tools, a re-split, the strongest model, then the stuck task deferred
//! with its reason) and the run finishes with what is done. Only a reply
//! naming something on the list can make a hand-back.

use crate::passes::{ownership, steps, supervisor};
use common::handback::{human_only_in, HandBack, HumanOnly};

const REPLY: &str = "I couldn't finish: tasks 3 and 4 are left. Please complete the remaining \
                     tasks manually, or re-trigger the run.";

/// The ending a run with `done` of `total` planned tasks reaches after the
/// coder's `reply`, every attempt from here changing nothing (the worst
/// case): what the supervisor decides from the reply's words and the steps'
/// progress marks.
fn ending_after(reply: &str, done: u32, total: u32) -> (Vec<supervisor::Rung>, supervisor::Ending) {
    let open: Vec<String> = (done + 1..=total).map(|i| format!("task {i}")).collect();
    // The reply's words are a blocker only when they name a listed reason.
    let blocker = human_only_in(reply).and_then(|why| HandBack::new(why, reply));
    let mut facts = supervisor::EndFacts {
        open_tasks: open.clone(),
        has_work: done > 0,
        blocker,
        ..Default::default()
    };
    let mut rungs = Vec::new();
    if facts.blocker.is_none() {
        // The branch didn't move: each step after the reply is a stall.
        let mark = steps::progress_mark("a8c8814", "implement");
        let mut prev = Some(mark.clone());
        let mut stalled = 0;
        for _ in 0..12 {
            stalled = steps::stalled_after(prev.as_deref(), &mark, stalled);
            prev = Some(mark.clone());
            let rung = supervisor::next_rung(stalled, true);
            rungs.push(rung);
            let e = supervisor::decide_end(&facts);
            assert_eq!(
                e,
                supervisor::Ending::KeepGoing,
                "ended before the ladder: {rungs:?}"
            );
            if matches!(rung, supervisor::Rung::Defer | supervisor::Rung::Ship) {
                break;
            }
        }
        // The stuck tasks are deferred with their reason; the ladder is spent.
        facts.deferred = facts
            .open_tasks
            .drain(..)
            .map(|t| supervisor::defer_reason(stalled, &t, true))
            .collect();
        facts.ladder_spent = true;
    }
    (rungs, supervisor::end(&facts))
}

#[test]
fn a_reply_that_hands_back_is_no_progress_and_climbs_the_ladder() {
    assert!(ownership::hands_back_without_reason(REPLY));
    // The same pass tells the coder to change its approach.
    let note = ownership::escalation_note(REPLY).expect("a keep-going note");
    assert!(note.contains("different approach") && note.contains("run_checks"));
    for phrase in ownership::HAND_OFF_PHRASES {
        assert!(
            !note.to_lowercase().contains(phrase),
            "the note itself says {phrase:?}"
        );
    }

    let (rungs, end) = ending_after(REPLY, 2, 4);
    // Every rung changes the approach before anything is deferred.
    assert_eq!(rungs[0], supervisor::Rung::Narrow, "{rungs:?}");
    for r in [
        supervisor::Rung::SwitchTools,
        supervisor::Rung::Resplit,
        supervisor::Rung::Escalate,
    ] {
        assert!(rungs.contains(&r), "{r:?} missing from {rungs:?}");
    }
    // The run finishes with what is done, never as a wait on a person.
    match &end {
        supervisor::Ending::Finished { deferred } => assert_eq!(deferred.len(), 2, "{deferred:?}"),
        other => panic!("ended as {other:?}"),
    }
    assert_ne!(end.status(), common::run_status::NEEDS_INPUT);
}

#[test]
fn only_a_reply_naming_a_human_only_reason_hands_back() {
    let reply = "I couldn't finish: the payments client needs the API key for the payments \
                 sandbox, which isn't in the repository's secrets.";
    assert!(!ownership::hands_back_without_reason(reply));
    assert!(ownership::escalation_note(reply).is_none());
    let (rungs, end) = ending_after(reply, 2, 4);
    assert!(rungs.is_empty());
    match end {
        supervisor::Ending::HandBack(h) => assert_eq!(h.why(), HumanOnly::Credential),
        other => panic!("ended as {other:?}"),
    }
}

#[test]
fn the_implement_pass_answers_a_hand_back_reply_before_it_returns() {
    let src = include_str!("../implement.rs");
    let gate = src
        .find("super::ownership::escalation_note(&closing)")
        .expect("the implement pass reads its closing reply for a hand-back");
    let returns = src[gate..]
        .find("Ok(ImplementResult {")
        .expect("the pass returns after the gate");
    // The note goes to the coder and the coder runs again, before the
    // result (and its notes) leave the pass.
    let between = &src[gate..gate + returns];
    assert!(between.contains("messages.push("));
    assert!(between.contains("provider::converse("));
}
