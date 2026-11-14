//! be. A Jira ticket collected CoderHelm comments across retries. The first
//! run said "Already being fixed in #41 by coderhelm[bot]. CoderHelm didn't
//! build a second fix: ... If this ticket needs a separate change, say so on
//! the ticket and run it again." A retry with nothing new on the ticket asked
//! the open pull request question of the model again, got a different
//! answer (#41 lacks something), built on #41's branch, and stopped when its
//! pull request was about to open: a red "Run failed" panel saying the branch
//! repeats 4 commit(s) of #41 under other hashes (the hashes listed),
//! "Dropping them didn't work (too many commits to replay), and neither did
//! merging #41's branch in and rebuilding this chang" (cut mid-word), then
//! "Add more detail ... and comment to retry." and "View run" with an arrow. An older
//! failure panel sat below it. Several comments, each restating, one
//! contradicting the other.
//!
//! Guarantee: a retry with nothing new keeps the covered decision, made
//! before anything is built, and its ticket comment is the earlier one,
//! edited ("Retried at ...; same result"). Every comment goes through the
//! conversation layer: one live status comment per ticket, what changed is
//! named when the status moves on, people's comments are answered in reply
//! to them, the failure advice fits its cause, nothing is cut mid-word, no
//! arrow glyph and no raw hash shows.

use common::conversation::{
    decide, failure_comment, has_arrow, visible, Decision, Meta, Place, Said, Say, View,
};
use common::stop_rules::covering::{on_retry, retried_note, ticket_note, Outcome, Record, Retry};

const OLD_ADVICE: &str = "Add more detail";
const T0: i64 = 1_790_000_000_000;

fn covered() -> Record {
    Record {
        repo: "acme/web".into(),
        number: 41,
        url: "https://github.com/acme/web/pull/41".into(),
        author: "coderhelm[bot]".into(),
        reason: "It already changes the card list the ticket asks about".into(),
        own: true,
        outcome: Outcome::Complete.as_str().into(),
        ..Default::default()
    }
}

#[test]
fn a_retry_with_nothing_new_keeps_the_covered_decision_and_edits_the_comment() {
    let rec = covered();
    // Decided before building: the same answer, no model asked again.
    let Retry::Same(again) = on_retry(Some(&rec), true, false) else {
        panic!("a retry with nothing new decided afresh")
    };
    assert!(retried_note(&again).contains("nothing was built"));

    // The ticket shows one comment: the earlier one, edited.
    let note = ticket_note(&again, false);
    let earlier = Say::status("run", note.clone());
    let view = View {
        said: vec![Said {
            id: "10001".into(),
            at_ms: T0,
            meta: Meta {
                slot: "run".into(),
                fp: common::conversation::fingerprint(&note),
                gist: "#41 already has everything this ticket asks for.".into(),
                first_ms: T0,
                ..Default::default()
            },
            body: note.clone(),
        }],
        heard: vec![],
    };
    match decide(&view, &earlier, &Place::jira("ABC-12"), T0 + 3_600_000) {
        Decision::Edit { id, body, .. } => {
            assert_eq!(id, "10001");
            assert!(body.contains("same result"), "{body}");
        }
        other => panic!("a retry posted again: {other:?}"),
    }

    // A person asking for more decides afresh.
    assert_eq!(on_retry(Some(&rec), true, true), Retry::Decide);
}

#[test]
fn the_pipeline_decides_before_building() {
    let src = include_str!("../mod.rs");
    let check = src
        .find("covered_retry(state, msg, &github, &history)")
        .unwrap();
    let existing = src.find("stop_gate::existing_work(").unwrap();
    let plan = src.find("// --- Pass 2: Plan ---").unwrap();
    assert!(check < existing && existing < plan);
}

#[test]
fn the_failure_says_what_happened_and_the_step_for_its_cause() {
    let reason = "Stopped before opening a pull request: the branch repeats 4 commit(s) of open pull \
                  request #41 (`coderhelm/abc-12`) under other hashes (c1aaaaa, c2bbbbb, d3ccccc, \
                  e4ddddd). Dropping them didn't work (too many commits to replay), and neither did \
                  merging #41's branch in and rebuilding this change on it.";
    for markdown in [false, true] {
        let c = failure_comment(reason, "https://app.example/runs/detail?id=r1", markdown);
        let shown =
            common::conversation::shape(&c, if markdown { 60_000 } else { 6_000 }, markdown);
        assert!(!shown.contains(OLD_ADVICE), "{shown}");
        assert!(!has_arrow(&shown));
        assert!(!visible(&shown).contains("c1aaaaa"), "{shown}");
        assert!(shown.contains("rebuilding this change on it."), "{shown}");
        assert!(shown.contains("the open pull request named above carries this work"));
    }
}
