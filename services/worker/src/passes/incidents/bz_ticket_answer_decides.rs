//! bz. A run asked on its ticket for a page's heading and intro copy, with
//! a proposed default. The product owner answered on the ticket to skip
//! both and use the page title. That run took the answer as a round on
//! its own pull request. Later a new run on the same ticket opened a new
//! pull request: the comment was already marked handled, so the new run
//! never read it, built the intro anyway and listed it under "Decisions pending product input".
//!
//! Guarantee: every answer and instruction people wrote on the ticket is
//! a decision for every run on it, whichever run handled it: merged into
//! the run's answers before triage and planning, read again at each
//! boundary, and given to every coder round (the build and each fix) and
//! every feedback and resume round. Wherever a description is written
//! (`description::save`), an item the answers settle is taken out of the
//! pending lists and listed under "Decided on the ticket"; an answer that
//! goes against an acceptance criterion gets one line saying so.

use common::run_questions::Answer;
use common::ticket_comments::{Class, Comment, State};
use common::ticket_decisions::{
    from_comments, remember, settle, settle_known, Known, DECIDED_HEADING,
};

const QUESTION: &str = "Final heading and intro copy per marina? Proposed default: '{Marina} \
    Slip Booking' plus one portal sentence. I am building the default now; a different \
    answer changes it in src/utils/slip-portal-copy.ts.";

/// The answer as it was kept: handled by the earlier run.
fn handled_answer() -> Comment {
    Comment {
        ticket_id: "ABC-76".into(),
        comment_id: "10001".into(),
        author: "a-product-owner".into(),
        text:
            "Skip the H1 and intro copy; the page title alone is fine\n Book a Slip | Example Site"
                .into(),
        at: "2030-03-14T16:16:36Z".into(),
        run_id: "an-earlier-run".into(),
        state: State::Handled,
        class: Some(Class::Answer),
        question: QUESTION.into(),
        outcome: "applied as a round on the earlier pull request".into(),
        handled_by: "an-earlier-run".into(),
        ..Default::default()
    }
}

const DESCRIPTION: &str = "Ticket: [ABC-76](https://example.atlassian.net/browse/ABC-76)

## Summary
Embeds the booking portal on the four slip booking pages.

### Decisions pending product input
- Final H1 and intro copy per marina: built with the default '{Marina} Slip Booking' and one portal sentence until product confirms.
- Whether the portal opens in a new tab on phones

## Testing
- Unit tests for the route and the embed
";

#[test]
fn an_answer_an_earlier_run_handled_is_still_the_answer() {
    let answers = from_comments(&[handled_answer()]);
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].question, QUESTION);
    assert!(answers[0].answer.starts_with("Skip the H1"));
    // The planner reads it as a resolved decision.
    let note = common::run_questions::answers_note(&answers);
    assert!(note.contains("Skip the H1"), "{note}");
}

#[test]
fn the_description_never_lists_an_answered_item_as_pending() {
    let answers: Vec<Answer> = from_comments(&[handled_answer()]);
    let criteria = vec!["Each page shows an H1 and intro copy above the portal".to_string()];
    let s = settle(DESCRIPTION, &answers, &criteria);
    assert!(!s.body.contains("until product confirms"), "{}", s.body);
    // The unanswered item stays pending.
    assert!(s.body.contains("opens in a new tab on phones"));
    assert!(s.body.contains(DECIDED_HEADING));
    assert!(s.body.contains("Skip the H1 and intro copy"));
    // The answer against the criterion is said in one line.
    let conflict = s
        .body
        .lines()
        .find(|l| l.contains("goes against the acceptance criterion"))
        .expect("one line on the conflict");
    assert!(conflict.contains("H1 and intro copy above the portal"));
}

#[test]
fn every_description_write_settles_with_what_the_run_knows() {
    remember(
        7,
        "acme/site",
        "ABC-76",
        Known {
            answers: from_comments(&[handled_answer()]),
            criteria: Vec::new(),
        },
    );
    let s = settle_known(7, "acme/site", DESCRIPTION);
    assert_eq!(s.removed.len(), 1);
    // Another repository's description is not this ticket's.
    assert!(settle_known(7, "acme/other", DESCRIPTION)
        .removed
        .is_empty());
    // Nor is another installation's (another team's).
    assert!(settle_known(8, "acme/site", DESCRIPTION).removed.is_empty());

    let save = include_str!("../description.rs");
    let at = save.find("pub async fn save(").unwrap();
    assert!(save[at..at + 900].contains("ticket_decisions::settle_known("));
    // Before triage and planning, at each boundary, and in the rounds that
    // run outside the first step.
    let passes = include_str!("../mod.rs");
    assert!(
        passes.contains("ticket_comments::earlier_answers(state, &msg.team_id, &msg.ticket_id)")
    );
    assert!(passes.contains("msg.answers = ticket_comments::know_decisions("));
    assert!(include_str!("../resume.rs").contains("super::ticket_comments::know_decisions("));
    assert!(include_str!("../feedback.rs").contains("super::ticket_comments::know_decisions("));
    assert!(include_str!("../implement.rs").contains("answers_note(&msg.answers)"));
}
