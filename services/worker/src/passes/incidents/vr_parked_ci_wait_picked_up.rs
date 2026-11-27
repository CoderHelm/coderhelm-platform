//! vr. A run waited on its pull request's checks while an outside step
//! waited on a workflow that never started. The CI wait reached its poll
//! cap and handed off: it marked the run done, and the review that picks a
//! finished run back up sent it to waiting on CI again. That wait never
//! looked again:
//!
//! - `send_delayed_resume` armed the next check's due time before it met
//!   the cap, and the hand-off left it in place. The review's wait asked to
//!   arm its own check a minute later, found one "already due", and sent
//!   nothing. No message was queued for the run, and it sat waiting on CI.
//! - The release that taught hand-offs to end the outside waits and reset
//!   the poll count came after. Its code runs only on a run's next resume,
//!   and nothing sends one to a run whose chain is gone: the self-heal
//!   sweep read only failed runs, and the stuck-run sweep would only close
//!   the run a day later, skipping the review and the outside steps.
//!
//! Guarantees: a hand-off clears the due check it armed; a run waiting on
//! CI with no check due and no write for an hour is picked back up by the
//! self-heal sweep (on each release and every 15 minutes) with one resume,
//! claimed by one conditional write, its poll count started again, at most
//! a few times per run; a live chain, a wait for GitHub's limit, or a run
//! an invocation holds is never picked up.

use super::fixture::fn_src;
use common::ci_watch::{self, WaitState};

#[test]
fn a_hand_off_clears_the_check_it_armed() {
    let src = include_str!("../resume.rs");
    let hand_off = fn_src(src, "\nasync fn hand_off_after_ci_wait(");
    assert!(hand_off.contains("REMOVE ci_resume_polls, #d"));
    assert!(hand_off.contains("ci_watch::DUE_FIELD"));
    // The check is armed before the cap is met, so the hand-off is what
    // has to clear it.
    let send = fn_src(src, "\nasync fn send_delayed_resume(");
    let arm = send.find("arm_delayed_check(").unwrap();
    let cap = send.find("may_poll_again(").unwrap();
    assert!(arm < cap && send[cap..].contains("hand_off_after_ci_wait("));
}

/// The run as it was left: polls past the cap, its due check two minutes
/// after the hand-off, nothing written for hours.
#[test]
fn the_parked_wait_is_picked_back_up_and_a_live_one_never() {
    let handed_off = 50_000;
    let left = WaitState {
        due: Some(handed_off + 120),
        updated: handed_off,
        ..Default::default()
    };
    assert!(!ci_watch::parked(&left, handed_off + 600));
    let later = handed_off + 5 * 3600;
    assert!(ci_watch::parked(&left, later));
    // A chain still polling arms a check every two minutes.
    let live = WaitState {
        due: Some(later + 120),
        updated: later - 30,
        ..Default::default()
    };
    assert!(!ci_watch::parked(&live, later));
    // Picked up a few times at most: the stuck-run sweep's cap ends it.
    let often = WaitState {
        redrives: ci_watch::MAX_PARKED_REDRIVES,
        ..left
    };
    assert!(!ci_watch::parked(&often, later));
}

#[test]
fn the_self_heal_sweep_sends_the_resume_once() {
    let recover = include_str!("../recover.rs");
    let sweep = fn_src(recover, "\npub async fn self_heal_sweep(");
    assert!(sweep.contains("resume::redrive_parked_waits(state)"));
    let src = include_str!("../resume.rs");
    let redrive = fn_src(src, "\npub(crate) async fn redrive_parked_waits(");
    assert!(redrive.contains("ci_watch::parked("));
    // One claim: only a run still waiting, with its due check stale, under
    // its cap; the poll count starts again with the new chain.
    assert!(redrive.contains("#s = :aw AND (attribute_not_exists(#d) OR #d <= :stale)"));
    assert!(redrive.contains("#r < :max"));
    assert!(redrive.contains("REMOVE ci_resume_polls"));
    let claim = redrive.find("let claimed").unwrap();
    let send = redrive.find("send_resume_message(").unwrap();
    assert!(claim < send);
    // The due check is set to now, never later: the resume arms its own
    // check two minutes on (`may_arm` lets it), so the new chain goes on.
    assert!(redrive.contains("SET #d = :now"));
    assert!(ci_watch::may_arm(Some(1_000), 1_000));
}
