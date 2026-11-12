//! ae. Two loops of fix rounds still stopped on their own budget and waited
//! on a person after #635 moved every other ending onto the supervisor:
//!
//! - the CI-only fix rounds on a finished run's pull request: at eight
//!   automatic pushes CoderHelm marked the pull request ready, completed the
//!   run with its checks red, and said it "stops starting rounds on its own
//!   here ... a comment naming a change starts the next round right away",
//!   even when every round had turned another red check green;
//! - the self-review rounds on CoderHelm's own pull request: at two rounds
//!   it posted "a comment naming a change starts the next round right away"
//!   and left the findings holding the draft, even when each round closed
//!   one.
//!
//! Guarantee: both loops follow the supervisor's rule
//! (`supervisor::next_round`). Rounds go on while they make progress
//! (checks that were red turn green, findings close), past the cap up to
//! twice it and below the run's spend ceiling. A round without progress
//! changes approach (one item, a fresh context with small edits, a
//! re-split, the strongest model). When the rounds end, what is left is
//! deferred with the reason (the findings move to "Deferred" and stop
//! holding the draft; what broke the checks is reverted to the last green
//! head and deferred), or, when nothing can come out and checks are red,
//! the run fails with exactly what fails. Nothing waits on a comment.

use super::fixture::fn_src;
use crate::passes::supervisor::{self, Ending, Revert, RoundStep, Rung};
use crate::passes::{feedback, own_review, ownership, resume, review_pr, MAX_AUTO_FIX_PUSHES};

const OLD_CI: &str = "a comment naming a change starts the next round right away";

fn checks(range: std::ops::RangeInclusive<u32>) -> Vec<String> {
    range.map(|i| format!("check-{i}")).collect()
}

/// Drives the CI-only rounds over the failing checks each round leaves
/// (`after[i]` fails once round `i` ran): what each round's claim gave, and
/// how the loop ended (`None` while checks were still red and a round was
/// given for each).
fn ci_rounds(after: &[Vec<String>], first: Vec<String>) -> (Vec<RoundStep>, Vec<String>) {
    let (mut rounds, mut before, mut stalled) = (0u32, Vec::<String>::new(), 0u32);
    let mut failing = first;
    let mut steps = Vec::new();
    for next in after {
        if failing.is_empty() {
            break;
        }
        let step = crate::passes::auto_fix_step(rounds, &before, &failing, stalled, 0.0);
        steps.push(step.clone());
        let RoundStep::Round { stalled: s, .. } = step else {
            break;
        };
        rounds += 1;
        stalled = s;
        before = failing;
        failing = next.clone();
    }
    (steps, failing)
}

/// The old rule: a round only while fewer than eight pushes were taken.
fn old_ci_rounds(needed: usize) -> usize {
    needed.min(MAX_AUTO_FIX_PUSHES as usize)
}

#[test]
fn ci_only_rounds_past_the_cap_go_on_while_checks_turn_green_and_finish() {
    // Twelve red checks; every round turns one green (a lint stage, then
    // types, then each test suite).
    let after: Vec<Vec<String>> = (2..=13).map(|i| checks(i..=12)).collect();
    let (steps, left) = ci_rounds(&after, checks(1..=12));
    assert!(left.is_empty(), "the checks end green: {left:?}");
    assert_eq!(steps.len(), 12);
    assert!(steps.len() > MAX_AUTO_FIX_PUSHES as usize);
    assert!(steps.len() <= supervisor::ceiling(MAX_AUTO_FIX_PUSHES) as usize);
    for s in &steps {
        assert_eq!(
            s,
            &RoundStep::Round {
                rung: Rung::Continue,
                stalled: 0
            }
        );
    }
    // Green: the run finishes, nothing deferred, nothing failing.
    assert_eq!(
        supervisor::end_of_rounds(&[], "", &[]),
        Ending::Finished { deferred: vec![] }
    );
    // The old rule stopped after eight rounds with four checks still red
    // and asked for a comment.
    assert_eq!(old_ci_rounds(12), 8);
    assert_eq!(after[old_ci_rounds(12) - 1].len(), 4);
}

#[test]
fn ci_only_rounds_without_progress_change_approach_then_end_with_the_cause() {
    let red = checks(1..=2);
    let after: Vec<Vec<String>> = (0..20).map(|_| red.clone()).collect();
    let (steps, left) = ci_rounds(&after, red.clone());
    let rungs: Vec<Rung> = steps
        .iter()
        .filter_map(|s| match s {
            RoundStep::Round { rung, .. } => Some(*rung),
            RoundStep::End { .. } => None,
        })
        .collect();
    assert_eq!(
        rungs,
        vec![
            Rung::Continue,
            Rung::Narrow,
            Rung::SwitchTools,
            Rung::Resplit,
            Rung::Escalate
        ]
    );
    for r in &rungs[1..] {
        assert!(!supervisor::round_instruction(*r, "the failing checks").is_empty());
    }
    assert!(supervisor::round_instruction(Rung::Escalate, "x").contains(supervisor::ESCALATED));
    let Some(RoundStep::End { why }) = steps.last() else {
        panic!("{steps:?}")
    };
    assert!(why.contains("made no progress"), "{why}");
    // What broke the checks comes out first, deferred with the reason, and
    // CI decides again.
    let heads = vec![("c9".to_string(), false), ("c8".to_string(), true)];
    assert_eq!(
        supervisor::revert_for(&heads, &[], 0),
        Revert::ToHead("c8".into())
    );
    // Nothing can come out: the run fails with what fails and why.
    assert_eq!(
        supervisor::end_of_rounds(&[], why, &left),
        Ending::Failed,
        "red checks never finish"
    );
    let tried = feedback::auto_fix_rounds_tried(why);
    let cause = format!(
        "{}. {}",
        resume::ci_failing_reason(&left, "error[E0425]: cannot find value", tried.len()),
        tried.join("; ")
    );
    assert!(cause.contains("check-1, check-2"), "{cause}");
    assert!(cause.contains(why.as_str()), "{cause}");
    assert!(!ownership::hands_back_without_reason(&cause), "{cause}");
    assert!(!cause.contains("comment"), "{cause}");

    // Past the cap, a round that made no progress ends the rounds too.
    let at_cap = crate::passes::auto_fix_step(MAX_AUTO_FIX_PUSHES, &red, &red, 0, 0.0);
    assert!(matches!(at_cap, RoundStep::End { .. }), "{at_cap:?}");
    // And near the spend ceiling, whatever the progress.
    let spent = crate::passes::auto_fix_step(1, &checks(1..=3), &red, 0, 190.0);
    assert!(matches!(spent, RoundStep::End { .. }), "{spent:?}");
}

/// Drives the self-review rounds over the findings each review leaves open.
fn self_rounds(open: &[usize]) -> Vec<RoundStep> {
    let (mut rounds, mut before, mut stalled) = (0u32, None, 0u32);
    let mut steps = Vec::new();
    for &now in open {
        if now == 0 {
            break;
        }
        let step = review_pr::self_review_step(rounds, before, now, stalled, 0.0);
        steps.push(step.clone());
        let RoundStep::Round { stalled: s, .. } = step else {
            break;
        };
        rounds += 1;
        stalled = s;
        before = Some(now);
    }
    steps
}

#[test]
fn self_review_rounds_past_the_cap_go_on_while_findings_close() {
    // Each review finds one fewer: four rounds, past the cap of two, and the
    // fourth review approves.
    let steps = self_rounds(&[4, 3, 2, 1, 0]);
    assert_eq!(steps.len(), 4);
    assert!(steps.iter().all(|s| matches!(
        s,
        RoundStep::Round {
            rung: Rung::Continue,
            ..
        }
    )));
    assert!(steps.len() > own_review::MAX_FIX_ROUNDS as usize);
    // The old cap stopped after two with two findings open and asked for a
    // comment.
    assert_eq!(own_review::MAX_FIX_ROUNDS, 2);
    // The ceiling holds even while they close: twice the cap.
    let long = self_rounds(&[9, 8, 7, 6, 5, 4, 3]);
    assert!(matches!(long.last(), Some(RoundStep::End { why }) if why.contains("twice")));
    assert_eq!(
        long.len() as u32,
        supervisor::ceiling(own_review::MAX_FIX_ROUNDS) + 1
    );
}

#[test]
fn self_review_rounds_without_progress_defer_the_findings_and_finish() {
    let steps = self_rounds(&[2, 2, 2, 2]);
    assert!(matches!(
        steps[1],
        RoundStep::Round {
            rung: Rung::Narrow,
            stalled: 1
        }
    ));
    let Some(RoundStep::End { why }) = steps.last() else {
        panic!("{steps:?}")
    };
    let open = vec![
        "Retry loop never backs off (`src/sync.rs:88`)".to_string(),
        "Cursor skips tied rows (`src/cursor.rs:12`)".to_string(),
    ];
    let body = format!(
        "Closes #12\n\n{}\n\n## Summary\nx",
        own_review::section(&open)
    );
    assert!(own_review::hold(&body).is_some());
    let now = own_review::defer_findings(&body, &open, why);
    // Deferred, named with the reason, and no longer holding the draft: the
    // ready decision holds it only while required checks are red.
    assert!(own_review::hold(&now).is_none(), "{now}");
    assert!(now.contains("## Deferred\n- Retry loop never backs off"));
    assert!(now.contains(why.as_str()));
    let note = review_pr::self_review_end_note(&open, why);
    assert!(note.contains("deferred"), "{note}");
    assert!(note.contains("- Cursor skips tied rows"), "{note}");
    assert!(!ownership::hands_back_without_reason(&note), "{note}");
    assert!(!note.contains(OLD_CI), "{note}");
}

#[test]
fn neither_loop_ends_by_waiting_on_a_comment() {
    // The CI-only rounds end through the supervisor's red-CI ending
    // (revert to green, else failed with the cause), never by completing
    // the run with the checks red.
    let fb = include_str!("../feedback.rs");
    let end = fn_src(fb, "\nasync fn end_auto_fix_rounds(");
    assert!(end.contains("resume::end_red_ci_rounds("));
    assert!(!end.contains("\"completed\""));
    let gate = fn_src(fb, "\npub async fn run(");
    assert!(gate.contains("super::claim_auto_fix_round("));
    assert!(!gate.contains("claim_auto_fix_slot("));
    let rs = include_str!("../resume.rs");
    let red = fn_src(rs, "\npub(crate) async fn end_red_ci_rounds(");
    let (revert, fail) = (
        red.find("revert_to_green(").unwrap(),
        red.find("super::fail_run(").unwrap(),
    );
    assert!(revert < fail);
    // The own-review rounds defer what is open and finish.
    let fin = fn_src(rs, "\nasync fn finish_on_own_findings(");
    assert!(fin.contains("own_review::defer_findings("));
    assert!(fin.contains("wait_for_review("));
    assert!(!rs.contains("stop_on_own_findings("));
    // The self-review rounds too.
    let pr = include_str!("../review_pr.rs");
    assert!(pr.contains("SelfRound::End(why) => {"));
    let ended = fn_src(pr, "\nasync fn end_self_review_rounds(");
    assert!(ended.contains("own_review::defer_findings("));
    assert!(ended.contains("ready::request("));
    // The ready decision defers a finished run's findings once its rounds
    // are over, instead of holding them for a person.
    let ready = include_str!("../ready.rs");
    assert!(fn_src(ready, "\nasync fn fix_own_findings(").contains("OwnRound::Defer(why)"));
    // The words the old endings used are on the hand-off list, so the
    // source scan (`ownership`) fails if one comes back.
    for p in [
        "naming a change starts",
        "stops starting rounds",
        "comment to continue",
    ] {
        assert!(ownership::HAND_OFF_PHRASES.contains(&p), "{p}");
    }
}
