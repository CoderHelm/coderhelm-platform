//! Chat understands a question before it answers: what it names
//! (`chat_hints`: a ticket, a pull request, "what needs me") and what the
//! person wants (`common::chat_intent`: an orientation, a status, a how-to,
//! a judgement, a correction of Chat itself, small talk, ...), with the
//! earlier answer in the thread. From that reading, and only from it:
//!
//! - the tools are ordered (the one that holds the answer first);
//! - the model is told how the question reads and how to size the answer;
//! - "what needs me" comes back in its list's shape only when that is the
//!   question (`chat_work::Ctx::needs_shape`);
//! - an orientation looks the team's setup up, and a correction checks the
//!   disputed point again, before either may answer ([`Understood::must_look`]);
//! - an uncited reply is kept only for small talk.
//!
//! The same reading runs in production (`brain::answer`) and in the evals
//! (`chat_eval`), so what is measured is what ships.

use common::chat_intent::{self, Intent, Reading, Signals};
use serde_json::Value;

use super::brain::Message;
use super::chat_agent::MustLook;
use super::chat_hints::{self, Hints};
use super::chat_setup::SETUP;

/// A question as read.
#[derive(Debug, Clone, PartialEq)]
pub struct Understood {
    pub hints: Hints,
    pub reading: Reading,
}

/// Words that put a disputed point on Coderhelm's own setup.
const SETUP_WORDS: &[&str] = &[
    "connect",
    "integration",
    "installed",
    "set up",
    "setup",
    "access",
    "jira",
    "github",
    "atlassian",
    "confluence",
    "slack",
    "linked",
    "sign in",
    "signed in",
    "permission",
];

/// Reads a question with the thread before it.
pub fn read(question: &str, history: &[Message]) -> Understood {
    let hints = chat_hints::read(question);
    let last = history
        .iter()
        .rev()
        .find(|m| m.role == "assistant")
        .map(|m| m.content.as_str());
    read_with(question, last, hints)
}

/// [`read`] from the last answer's text.
pub fn read_with(question: &str, last_answer: Option<&str>, hints: Hints) -> Understood {
    let signals = Signals {
        needs_me: hints.needs_me,
        status: hints.status,
        names_work: !hints.tickets.is_empty()
            || !hints.prs.is_empty()
            || !hints.quoted.is_empty()
            || !hints.advisories.is_empty(),
    };
    Understood {
        reading: chat_intent::read(question, last_answer, signals),
        hints,
    }
}

impl Understood {
    pub fn intent(&self) -> Intent {
        self.reading.intent
    }

    /// "What needs me" is written in its list's shape.
    pub fn needs_shape(&self) -> bool {
        self.intent().fixed_shape()
    }

    pub fn uncited_ok(&self) -> bool {
        self.intent().uncited_ok()
    }

    /// The disputed point is about Coderhelm's own setup (a connection,
    /// an integration, access).
    fn disputes_setup(&self, question: &str) -> bool {
        let text = format!("{question} {}", self.reading.disputed.join(" ")).to_lowercase();
        SETUP_WORDS.iter().any(|w| text.contains(w))
    }

    /// Puts the tool that holds the answer first.
    pub fn order(&self, tools: &mut [Value], question: &str) {
        super::chat_work::put_first(tools, &self.hints);
        let setup_first = match self.intent() {
            Intent::Orientation | Intent::Capability => true,
            Intent::Correction => self.disputes_setup(question),
            _ => false,
        };
        if setup_first {
            tools.sort_by_key(|t| usize::from(t["name"] != SETUP));
        }
    }

    /// The notes before the question: where the work lives (when it names
    /// work), then how the question reads and how to answer it.
    pub fn notes(&self, offered: &[&str]) -> String {
        let mut out = super::chat_work::hint_note(&self.hints, offered);
        out.push_str(&self.reading.note());
        out
    }

    /// Where the answer must look before it may be given.
    pub fn must_look(&self, offered: &[&str], question: &str) -> Option<MustLook> {
        let has = |t: &str| offered.contains(&t);
        match self.intent() {
            Intent::Orientation if has(SETUP) => Some(MustLook {
                tools: vec![SETUP.to_string()],
                what: "what Coderhelm is set up to do for this team, checked now".into(),
                always: true,
            }),
            Intent::Correction => {
                let tools: Vec<String> = if has(SETUP) && self.disputes_setup(question) {
                    vec![SETUP.to_string()]
                } else {
                    offered
                        .iter()
                        .filter(|t| **t != super::chat_policy::SIGN_IN_TOOL)
                        .map(|t| t.to_string())
                        .collect()
                };
                (!tools.is_empty()).then(|| MustLook {
                    tools,
                    what: "the point they question, checked again in this answer (never from the earlier answer's words)".into(),
                    always: true,
                })
            }
            _ => super::chat_work::must_look(&self.hints, offered),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::chat_work::{self as work, NEEDS_ME, RUNS, TICKETS};
    use serde_json::json;

    fn msg(role: &str, content: &str) -> Message {
        Message {
            role: role.into(),
            content: content.into(),
            ..Default::default()
        }
    }

    const OFFERED: &[&str] = &[SETUP, NEEDS_ME, TICKETS, work::PULL_REQUESTS, RUNS];

    /// The first question of the report: an orientation, never the list.
    #[test]
    fn a_newcomer_gets_an_orientation_from_the_setup_not_the_to_do_list() {
        let q = "this is my first time using coderhelm. i'm the cto of the company. are are a few things that you think could be useful to me";
        let u = read(q, &[]);
        assert_eq!(u.intent(), Intent::Orientation);
        assert!(!u.needs_shape());
        let m = u.must_look(OFFERED, q).unwrap();
        assert!(m.always && m.tools == [SETUP], "{m:?}");
        let notes = u.notes(OFFERED);
        assert!(notes.contains("an orientation"), "{notes}");
        assert!(notes.contains("the cto"), "{notes}");
        assert!(!notes.contains("Call work_needs_me first"), "{notes}");
        let mut tools = vec![
            json!({"name": NEEDS_ME}),
            json!({"name": RUNS}),
            json!({"name": SETUP}),
        ];
        u.order(&mut tools, q);
        assert_eq!(tools[0]["name"], SETUP);
    }

    /// The second: a correction about a connection checks the setup again.
    #[test]
    fn a_correction_about_a_connection_must_check_the_setup_again() {
        let history = vec![
            msg("user", "first time here, what can you do for me?"),
            msg("assistant", "Welcome. 24 items need you. Jira isn't connected to Coderhelm, so your Jira tickets aren't in this list."),
        ];
        let q = "what do you mean that jira isn't connected? i see atlassian in the integrations";
        let u = read(q, &history);
        assert_eq!(u.intent(), Intent::Correction);
        let m = u.must_look(OFFERED, q).unwrap();
        assert!(m.always && m.tools == [SETUP], "{m:?}");
        let mut tools = vec![json!({"name": TICKETS}), json!({"name": SETUP})];
        u.order(&mut tools, q);
        assert_eq!(tools[0]["name"], SETUP);
        // A correction about anything else must look somewhere, anywhere.
        let history = vec![msg("assistant", "The refund window is 24 hours [S1].")];
        let q = "that's wrong, the refund window is 48 hours";
        let u = read(q, &history);
        assert_eq!(u.intent(), Intent::Correction);
        let m = u.must_look(&[TICKETS, "github__search_code"], q).unwrap();
        assert!(m.always && m.tools.len() == 2, "{m:?}");
    }

    #[test]
    fn what_needs_me_keeps_its_list_and_its_tool() {
        let u = read("What needs my answer?", &[]);
        assert_eq!(u.intent(), Intent::NeedsMe);
        assert!(u.needs_shape());
        let m = u.must_look(OFFERED, "What needs my answer?").unwrap();
        assert!(m.always && m.tools == [NEEDS_ME]);
        assert!(u.notes(OFFERED).contains("Call work_needs_me first"));
    }

    #[test]
    fn small_talk_may_go_uncited_and_nothing_else_may() {
        assert!(read("thanks!", &[]).uncited_ok());
        for q in [
            "what can you do?",
            "How can I cancel a reservation?",
            "is this comment accurate https://github.com/acme/web/pull/3#issuecomment-9",
            "what caused the disk full error?",
        ] {
            assert!(!read(q, &[]).uncited_ok(), "{q}");
        }
    }

    #[test]
    fn a_linked_pull_request_comment_is_looked_up_by_number_and_repository() {
        let q = "is this comment accurate https://github.com/acme/web/pull/1450#issuecomment-6450165666";
        let u = read(q, &[]);
        assert_eq!(u.intent(), Intent::Judgement);
        let notes = u.notes(OFFERED);
        assert!(
            notes.contains("work_pull_requests with number 1450 and repo \"acme/web\""),
            "{notes}"
        );
        assert!(notes.contains("not its comments"), "{notes}");
        assert!(notes.contains("verdict first"), "{notes}");
        let m = u.must_look(OFFERED, q).unwrap();
        assert!(!m.always && m.tools.contains(&work::PULL_REQUESTS.to_string()));
    }
}
