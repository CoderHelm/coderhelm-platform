//! bv. Four paths still ended a run's pull request "for a person" over
//! CoderHelm's own limits. A security fix whose CI stayed red after its
//! fix rounds stayed a draft with "Fix the failing checks on this branch",
//! and the run waited on a person or failed. A wait unchanged for two hours
//! on one head (a review, a fix round, checks still running) became "Look
//! at why it didn't happen ... or mark it ready yourself". Lines in the
//! description (security items under Open items, planned work under Not
//! finished, a check the change didn't pass, review findings) each held the
//! draft with a person's to-do, and a GitHub refusal to mark a pull request
//! ready counted as a person's.
//!
//! Guarantee: only a reason on the human-only list (`common::handback`)
//! keeps a draft for a person, and it is made only by `Reason::person`.
//! Every other reason is CoderHelm's: once its own attempts are spent it is
//! listed as CoderHelm's note and the pull request goes ready. A stalled
//! wait is CoderHelm's next action (its checks run again on the head),
//! twice, then its note. A security fix whose CI stays red after every set
//! of bumps that could come out did is completed, ready, with the failing
//! checks as its note. The run guard waits on a person only with a
//! human-only reason.

use super::fixture::fn_src;
use crate::passes::ready::{self, Ci, Decision, Facts};
use common::draft_hold::{DraftHold, Kind, Reason};
use common::handback::HumanOnly;

const HEAD: &str = "abc9876d7a44";

fn draft(body: &str) -> Facts {
    Facts {
        draft: true,
        body: body.into(),
        head: HEAD.into(),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        ..Default::default()
    }
}

fn notes(d: &Decision) -> Vec<String> {
    match d {
        Decision::Ready { open } => open
            .iter()
            .filter(|o| o.starts_with(ready::NOTE_PREFIX))
            .cloned()
            .collect(),
        other => panic!("ready with notes, not {other:?}"),
    }
}

#[test]
fn a_security_fix_with_red_ci_goes_ready_with_the_failing_checks_as_its_note() {
    let f = Facts {
        security: true,
        ci: Ci {
            green: false,
            why: "CI fails on this head: `unit-tests`".into(),
        },
        ..draft("## Summary\nBumps.")
    };
    let d = ready::decide(&f);
    let n = notes(&d);
    assert!(
        n[0].contains("doesn't pass CI") && n[0].contains("unit-tests"),
        "{n:?}"
    );
    // The run ends completed with the note: never a person's wait.
    use crate::passes::resume::{red_ci_ending, RedCiEnding};
    assert_eq!(red_ci_ending("SEC-1234ABCD"), RedCiEnding::NoteAndReady);
    assert_eq!(red_ci_ending("PROJ-12"), RedCiEnding::Failed);
    let resume = include_str!("../resume.rs");
    let hand_off = fn_src(resume, "\nasync fn hand_off_red_ci(");
    let ending = hand_off
        .find("finish_security_red(")
        .expect("the security ending");
    // Every set that can come out comes out first; the ending comes after.
    assert!(hand_off.find("take_out_failing_bumps(").unwrap() < ending);
    assert!(ending < hand_off.find("wait_for_review(").unwrap());
    let rounds = fn_src(resume, "\npub(crate) async fn end_red_ci_rounds(");
    assert!(rounds.find("finish_security_red(").unwrap() < rounds.find("stop_red(").unwrap());
    let finish = fn_src(resume, "\nasync fn finish_security_red(");
    assert!(finish.contains("mark_pr_ready(") && finish.contains("COMPLETED"));
    assert!(!finish.contains("needs_input") && !finish.contains("wait_for_review("));
}

#[test]
fn description_lines_are_coderhelm_notes_and_the_pull_request_goes_ready() {
    let body = format!(
        "## Summary\nx\n\n## Open items\n- Audit still reports 2 advisories {}\n\n{}\n- Task 3: deferred\n",
        crate::passes::proof::SECURITY_MARK,
        crate::passes::proof::UNFINISHED_START,
    );
    let d = ready::decide(&draft(&body));
    let n = notes(&d);
    assert!(
        n.iter().any(|l| l.contains("Security items are open")),
        "{n:?}"
    );
    assert!(
        n.iter().any(|l| l.contains("Planned work is not done yet")),
        "{n:?}"
    );
    // What only a person can settle still holds, alone.
    let stacked = format!(
        "{body}\n<!-- coderhelm:stack -->\n<!-- coderhelm:stack-hold -->\n<!-- /coderhelm:stack -->"
    );
    match ready::decide(&draft(&stacked)) {
        Decision::Held { reasons } => {
            assert_eq!(reasons.len(), 1, "{reasons:?}");
            assert_eq!(reasons[0].human_only(), Some(HumanOnly::HeldByAnother));
        }
        other => panic!("{other:?}"),
    }
    let put_back = Facts {
        put_back_by: Some("a-teammate".into()),
        ..draft(&body)
    };
    assert!(
        matches!(ready::decide(&put_back), Decision::Held { reasons }
        if reasons[0].human_only() == Some(HumanOnly::PersonAction))
    );
}

#[test]
fn only_a_human_only_reason_makes_a_hold_a_persons() {
    let own = DraftHold::new(
        Kind::Held,
        vec![Reason::new(
            "A check the change didn't pass is listed",
            "Fix it",
        )],
        3,
        "",
    )
    .unwrap();
    assert!(!own.needs_person());
    let person = DraftHold::new(
        Kind::Held,
        vec![Reason::person(
            HumanOnly::Credential,
            "The deploy needs a token only a person has",
            "Add it",
        )],
        3,
        "",
    )
    .unwrap();
    assert!(person.needs_person());
    // GitHub refusing to mark it ready is CoderHelm's to try again.
    let refused =
        DraftHold::new(Kind::Failed, vec![Reason::new("GitHub refused", "")], 3, "").unwrap();
    assert!(!refused.needs_person());
    // A hold stored before reasons named their kind is not a person's.
    let stored: DraftHold = serde_json::from_value(serde_json::json!({
        "kind": "held",
        "reasons": [{"what": "1 finding from CoderHelm's review is still open", "todo": "Fix them"}],
        "pr_number": 3
    }))
    .unwrap();
    assert!(!stored.needs_person());
}

#[test]
fn a_stalled_wait_is_coderhelms_next_action_then_its_note() {
    let why = "CoderHelm is fixing 1 finding from its review";
    let now = chrono::Utc::now();
    let waited = |retried: u32| {
        let mut h = DraftHold::new(Kind::Waiting, vec![Reason::new(why, "")], 3, "").unwrap();
        h.head = HEAD.into();
        h.since = (now - chrono::Duration::hours(3)).to_rfc3339();
        h.retried = retried;
        h
    };
    let body = crate::passes::own_review::section(&["A finding (`a.rs:1`)".to_string()]);
    let at = |retried: u32| Facts {
        own_fixing: true,
        prior: Some(waited(retried)),
        now: Some(now),
        ..draft(&body)
    };
    assert_eq!(
        ready::decide(&at(0)),
        Decision::Retry {
            why: why.into(),
            attempt: 1
        }
    );
    assert!(matches!(
        ready::decide(&at(1)),
        Decision::Retry { attempt: 2, .. }
    ));
    let spent = ready::decide(&at(ready::MAX_STALL_RETRIES));
    let n = notes(&spent);
    assert!(
        n[0].contains(why) && n[0].contains("ran its checks again 2 times"),
        "{n:?}"
    );
    // The attempts carry across waits on the same head, never to a new one.
    assert_eq!(ready::carried_retries(Some(&waited(1)), HEAD), 1);
    assert_eq!(ready::carried_retries(Some(&waited(1)), "another"), 0);
    // The retry is CoderHelm's action: its checks run on the head again.
    let src = include_str!("../ready.rs");
    let settle = fn_src(src, "\npub async fn settle(");
    let retry = settle.find("Decision::Retry { why, attempt } =>").unwrap();
    assert!(settle[retry..].contains("super::recheck::start("));
    // Nothing in the decision names a person's to-do for a stall.
    assert!(!src.contains("or mark it ready yourself; CoderHelm decides again"));
}

#[test]
fn the_run_guard_waits_on_a_person_only_with_a_human_only_reason() {
    let guard = include_str!("../run_guard.rs");
    let wait = fn_src(guard, "\nasync fn wait_on_open_pr(");
    let gate = wait
        .find("super::supervisor::hand_back_for(judged, reason) else")
        .expect("the hand-back is gated");
    // The status write, named without its call so this file isn't one.
    let write = concat!(".needs", "_input(");
    assert!(gate < wait.find(write).unwrap());
    assert!(wait[..gate].find(write).is_none());
}
