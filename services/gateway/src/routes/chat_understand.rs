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
//! - an uncited reply is kept only for small talk;
//! - a question about how the app fits together reaches for the app map
//!   first (`common::app_map`), then the code for exact lines; with no map
//!   built, the answer says so in one line and reads the code.
//!
//! The same reading runs in production (`brain::answer`) and in the evals
//! (`chat_eval`), so what is measured is what ships.

use common::app_map as am;
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
    /// Asked from the App map page: the map's lookups go first.
    pub from_map: bool,
}

/// The note for a map question when the team's map is built.
pub const MAP_FIRST: &str = "start from the app map (map_find or map_overview, then map_part, map_screen or map_file) and answer from it in plain words, as numbered steps in the order things happen when it asks how something happens, citing the map. When it asks what a change would reach or break (shared code, an endpoint), call map_impact on the file, the endpoint (METHOD /path) or the part. Then read the code with your code tools only for the exact lines a step needs.";

/// The note for a map question when the team has no map yet.
pub const NO_MAP: &str = "the team has no app map yet. Say so in one line first (it can be built on the App map page, /app-map), then answer from the code with your code tools.";

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
        from_map: false,
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

    /// It asks how the app fits together (or was asked from the App map).
    pub fn about_app(&self) -> bool {
        self.reading.about_app || self.from_map
    }

    /// Puts the tool that holds the answer first.
    pub fn order(&self, tools: &mut [Value], question: &str) {
        super::chat_work::put_first(tools, &self.hints);
        if self.about_app() {
            tools.sort_by_key(|t| usize::from(!am::is_tool(t["name"].as_str().unwrap_or(""))));
        }
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
        out.push_str(&self.map_note(offered));
        out
    }

    /// For a question about how the app fits together: the app map first,
    /// or, when the team has none, a one-line note and the code.
    pub fn map_note(&self, offered: &[&str]) -> String {
        if !self.about_app() {
            return String::new();
        }
        if offered.iter().any(|t| am::is_tool(t)) {
            return format!("- It asks how the app fits together: {MAP_FIRST}\n");
        }
        if offered.iter().any(|t| super::chat_code::is_code_tool(t)) {
            return format!("- It asks how the app fits together: {NO_MAP}\n");
        }
        String::new()
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
            _ => super::chat_work::must_look(&self.hints, offered).or_else(|| {
                // A map question that says it found nothing looks at the map.
                let map: Vec<String> = offered
                    .iter()
                    .filter(|t| am::is_tool(t))
                    .map(|t| t.to_string())
                    .collect();
                (self.about_app() && !map.is_empty()).then(|| MustLook {
                    tools: map,
                    what: "the app map (its parts, screens and the lines between them)".into(),
                    always: false,
                })
            }),
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

    /// "How does X work" questions reach for the map first and read the
    /// code for exact lines; with no map, a one-line note and the code.
    #[test]
    fn a_question_about_how_the_app_fits_together_reaches_for_the_map_first() {
        let q = "What happens when someone books a slot?";
        let u = read(q, &[]);
        assert!(u.about_app());
        let mut tools = vec![
            json!({"name": "github__search_code"}),
            json!({"name": TICKETS}),
            json!({"name": "map_overview"}),
            json!({"name": "map_find"}),
        ];
        u.order(&mut tools, q);
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert_eq!(
            names,
            ["map_overview", "map_find", "github__search_code", TICKETS]
        );
        let with_map = ["map_overview", "map_find", "github__search_code"];
        let notes = u.notes(&with_map);
        assert!(notes.contains(MAP_FIRST), "{notes}");
        assert!(!notes.contains(NO_MAP));
        let m = u.must_look(&with_map, q).unwrap();
        assert!(
            !m.always && m.tools == ["map_overview", "map_find"],
            "{m:?}"
        );
        // No map built: say so in one line, then the code.
        let notes = u.notes(&["github__search_code"]);
        assert!(notes.contains(NO_MAP), "{notes}");
        assert!(u.must_look(&["github__search_code"], q).is_none());
        // Other questions are left as they were.
        let other = read("what caused the enospc error?", &[]);
        assert!(!other.about_app());
        assert!(!other.notes(&with_map).contains("app map"));
        let mut tools = vec![
            json!({"name": "github__search_code"}),
            json!({"name": "map_find"}),
        ];
        other.order(&mut tools, "what caused the enospc error?");
        assert_eq!(tools[0]["name"], "github__search_code");
        // Asked from the App map page: the map goes first whatever the words.
        let mut from_map = read("tell me about the booking service", &[]);
        assert!(!from_map.about_app());
        from_map.from_map = true;
        assert!(from_map.about_app());
        assert!(from_map.notes(&with_map).contains(MAP_FIRST));
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
