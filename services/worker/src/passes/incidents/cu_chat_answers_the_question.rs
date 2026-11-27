//! cu. A person opened chat for the first time, named their role, and asked
//! what could be useful to someone in it. Chat answered a different
//! question: it led with "what's waiting on you" (a long list, one ticket's
//! details first), in the fixed shape that list's lookup imposes on every
//! answer, and said Jira wasn't connected, a claim it hadn't checked. Their
//! next message had to question that claim. Earlier, a
//! question that linked a pull request comment got the canned "found
//! nothing" line, because any answer that cited nothing was replaced by it.
//!
//! Guarantee: a question is read before it is answered
//! (`common::chat_intent::read`): an orientation, what Chat can do, what
//! needs me, a status, a how-to, a lookup, a judgement, a correction of
//! Chat itself, or small talk, and the role the person says they hold. The
//! list's fixed shape belongs only to "what needs me"
//! (`Intent::fixed_shape`); a correction names what it questions in the
//! last answer and must check it again (`Reading::disputed`); and an uncited
//! reply is kept as written only for small talk
//! (`common::knowledge::answer::finalize_reply`).

use common::chat_intent::{read, Intent, Signals};
use common::knowledge::answer::{finalize_reply, Registry, NO_SOURCE_ANSWER};

/// A made-up first question: a senior engineering leader, new to
/// Coderhelm, asks for an orientation.
const FIRST: &str = "Hello! I just started using Coderhelm and I'm the engineering director here. Which features would help someone like me most?";

/// The answer as it was given (shortened): the list first, then a claim
/// no check made in that answer supported.
const GIVEN: &str = "Welcome. Here are the things I think will matter most to you, starting with something already waiting on you.\n## 1. Questions only you can answer\nI checked what's waiting on your GitHub account: 9 items: 5 need you now, 3 today, 1 when you can. Jira isn't connected to Coderhelm, so your Jira tickets aren't in this list.";

const NEXT: &str = "You said Jira isn't connected, but the integrations page lists Atlassian as connected. Which is right?";

#[test]
fn the_first_question_is_an_orientation_for_a_senior_leader_and_never_the_list() {
    let r = read(FIRST, None, Signals::default());
    assert_eq!(r.intent, Intent::Orientation);
    assert_eq!(r.role.as_deref(), Some("engineering director"));
    assert!(r.senior);
    assert!(r.newcomer);
    // The list's fixed shape is not this question's.
    assert!(!r.intent.fixed_shape());
    let note = r.note();
    assert!(note.contains("coderhelm_setup"), "{note}");
    assert!(note.contains("Don't list their to-do items"), "{note}");
    // Only the list's own question gets the list's shape.
    let list = read(
        "What needs my answer?",
        None,
        Signals {
            needs_me: true,
            ..Default::default()
        },
    );
    assert!(list.intent.fixed_shape());
}

#[test]
fn the_follow_up_is_a_correction_that_names_the_unchecked_claim() {
    let r = read(NEXT, Some(GIVEN), Signals::default());
    assert_eq!(r.intent, Intent::Correction);
    assert_eq!(
        r.disputed,
        ["Jira isn't connected to Coderhelm, so your Jira tickets aren't in this list"]
    );
    let note = r.note();
    assert!(
        note.contains("Check the disputed point again now"),
        "{note}"
    );
    assert!(note.contains("whether you were wrong"), "{note}");
}

#[test]
fn only_small_talk_goes_uncited_and_every_other_answer_rests_on_a_source() {
    let thanks = read("thanks!", Some(GIVEN), Signals::default());
    assert_eq!(thanks.intent, Intent::SmallTalk);
    let kept = finalize_reply(
        "Glad to help.",
        &Registry::default(),
        thanks.intent.uncited_ok(),
    );
    assert_eq!(kept.text, "Glad to help.");
    // A judgement on a linked comment is not small talk: uncited, it is
    // never shown as if it were checked.
    let judged = read(
        "is this comment accurate https://github.com/acme/web/pull/3#issuecomment-1",
        None,
        Signals {
            names_work: true,
            ..Default::default()
        },
    );
    assert_eq!(judged.intent, Intent::Judgement);
    let f = finalize_reply(
        "Yes, it's accurate.",
        &Registry::default(),
        judged.intent.uncited_ok(),
    );
    assert_eq!(f.text, NO_SOURCE_ANSWER);
}
