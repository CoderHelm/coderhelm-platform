//! Questions a plan may not ask: ones a person already settled, and ones
//! CoderHelm decides itself.
//!
//! The run carries every settled question on the ticket (answers given in
//! CoderHelm, and choices people made on its notices, see
//! `ticket_state::decision_answer`). Before a plan's questions reach a person,
//! the model reads each against them (structured output) and says whether a
//! settled answer covers it, in any wording, or whether it is something the
//! system decides (closing or replacing a pull request someone else opened is
//! never CoderHelm's call: once told to open its own, it leaves the other one
//! open). Those are dropped, or the plan is sent back once with the answers.
//! A model failure falls back to comparing the words.

use crate::agent::provider::ModelProvider;
use crate::models::TokenUsage;
use crate::WorkerState;
use common::run_questions::{self, Answer, Question};
use serde_json::{json, Value};
use tracing::{info, warn};

/// What may happen to one question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nobody has settled it: it may be asked.
    Open,
    /// A settled answer covers it (its index, when the model named one).
    Settled(Option<usize>),
    /// CoderHelm decides it itself; a person is never asked.
    SystemDecides,
}

impl Verdict {
    pub fn may_ask(self) -> bool {
        self == Verdict::Open
    }
}

pub const SYSTEM: &str = "An automated coding agent (CoderHelm) wants to ask a person some \
     questions about a ticket. Some questions on the ticket are already settled: people \
     answered them, or chose on CoderHelm's notices. For each new question, say whether a \
     settled answer already covers it, even in other words or from another angle (covered, \
     with the settled answer's number), or whether it is something CoderHelm must decide \
     itself and never ask a person (system_decides): whether to close, replace or take over a \
     pull request someone else opened when a person already said to open CoderHelm's own, or \
     anything the settled answers plus ordinary engineering judgement decide. The ticket's own \
     text and people's comments on the ticket are listed as settled entries too: when the ticket \
     already states the answer (for example that this work replaces an earlier pull request or \
     attempt), or a person's comment answers it, the question is covered. Otherwise it is \
     open. Judge meaning, not wording.";

/// The question a settled entry for the ticket's own text carries.
pub const TICKET_TEXT_QUESTION: &str = "What the ticket itself says (title and description)";
/// The question a settled entry for a person's ticket comment carries.
pub const TICKET_COMMENT_QUESTION: &str = "A person's comment on the ticket";
/// Most characters of the ticket's text read as settled.
const MAX_TICKET_CHARS: usize = 6_000;
/// Most ticket comments read as settled, newest kept.
const MAX_TICKET_COMMENTS: usize = 10;

/// Everything that settles a question on this ticket: the answers given in
/// CoderHelm and choices on its notices (`answers`), the ticket's own text
/// (a ticket that says it replaces earlier work answers a question about
/// that work), and what people wrote on the ticket since (status-only
/// comments left out). A question any of these covers is never asked.
pub fn settled_on_ticket(
    answers: &[Answer],
    ticket_text: &str,
    notes: &[common::ticket_state::Note],
) -> Vec<Answer> {
    let mut out = answers.to_vec();
    if !ticket_text.trim().is_empty() {
        out.push(Answer {
            question_id: "ticket".to_string(),
            question: TICKET_TEXT_QUESTION.to_string(),
            answer: common::truncate_str(ticket_text.trim(), MAX_TICKET_CHARS).to_string(),
            author: "the ticket".to_string(),
            ..Default::default()
        });
    }
    let people: Vec<&common::ticket_state::Note> = notes
        .iter()
        .filter(|n| n.intent != common::ticket_state::Intent::StatusOnly)
        .filter(|n| !n.text.trim().is_empty())
        .collect();
    let skip = people.len().saturating_sub(MAX_TICKET_COMMENTS);
    for (i, n) in people.into_iter().skip(skip).enumerate() {
        out.push(Answer {
            question_id: format!("ticket-comment-{}", i + 1),
            question: TICKET_COMMENT_QUESTION.to_string(),
            answer: common::truncate_str(n.text.trim(), 1_500).to_string(),
            author: n.author.clone(),
            answered_at: n.at.clone(),
            ..Default::default()
        });
    }
    out
}

/// `questions` without the ones `verdicts` says may not be asked.
pub fn open_only(questions: Vec<Question>, verdicts: &[Verdict]) -> Vec<Question> {
    questions
        .into_iter()
        .zip(
            verdicts
                .iter()
                .copied()
                .chain(std::iter::repeat(Verdict::Open)),
        )
        .filter(|(_, v)| v.may_ask())
        .map(|(q, _)| q)
        .collect()
}

pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "verdicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "question": {"type": "integer"},
                        "verdict": {"type": "string", "enum": ["open", "covered", "system_decides"]},
                        "settled_by": {"type": "integer"},
                        "reason": {"type": "string"}
                    },
                    "required": ["question", "verdict", "settled_by", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["verdicts"],
        "additionalProperties": false
    })
}

pub fn prompt(questions: &[Question], settled: &[Answer]) -> String {
    let mut out = String::from("## Settled on this ticket\n");
    for (i, a) in settled.iter().enumerate() {
        out.push_str(&format!(
            "{}. Question: {}\n   Answer: {}\n",
            i + 1,
            one_line(&a.question),
            one_line(&a.answer)
        ));
    }
    out.push_str("\n## Questions CoderHelm wants to ask now\n");
    for (i, q) in questions.iter().enumerate() {
        out.push_str(&format!("{}. {}\n", i + 1, one_line(&q.question)));
    }
    out.push_str(
        "\nGive one verdict per question (numbered as above); settled_by is the settled \
         answer's number, or 0 when none.",
    );
    out
}

/// The model's verdicts, one per question (1-based numbers in the reply);
/// a question it left out stays open.
pub fn apply(reply: &Value, questions: usize, settled: usize) -> Vec<Verdict> {
    let mut out = vec![Verdict::Open; questions];
    for v in reply["verdicts"].as_array().into_iter().flatten() {
        let Some(i) = v["question"]
            .as_u64()
            .and_then(|n| (n as usize).checked_sub(1))
            .filter(|i| *i < questions)
        else {
            continue;
        };
        let by = v["settled_by"]
            .as_u64()
            .and_then(|n| (n as usize).checked_sub(1))
            .filter(|n| *n < settled);
        out[i] = match v["verdict"].as_str().unwrap_or("") {
            "covered" => Verdict::Settled(by),
            "system_decides" => Verdict::SystemDecides,
            _ => Verdict::Open,
        };
    }
    out
}

/// The words-only reading, when the model can't be asked.
pub fn by_words(questions: &[Question], settled: &[Answer]) -> Vec<Verdict> {
    questions
        .iter()
        .map(|q| {
            match settled
                .iter()
                .position(|a| run_questions::already_answered(&q.question, std::slice::from_ref(a)))
            {
                Some(i) => Verdict::Settled(Some(i)),
                None => Verdict::Open,
            }
        })
        .collect()
}

/// One verdict per question. Nothing settled: every question is open.
pub async fn judge(
    state: &WorkerState,
    provider: &ModelProvider,
    questions: &[Question],
    settled: &[Answer],
    usage: &mut TokenUsage,
) -> Vec<Verdict> {
    if questions.is_empty() || settled.is_empty() {
        return vec![Verdict::Open; questions.len()];
    }
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &prompt(questions, settled),
        &schema(),
        true,
        "low",
        2048,
        usage,
    )
    .await
    {
        Ok(v) => {
            let out = apply(&v, questions.len(), settled.len());
            info!(
                asked = questions.len(),
                dropped = out.iter().filter(|v| !v.may_ask()).count(),
                "Checked the plan's questions against what is settled"
            );
            // The words still catch a plain repeat the model let through.
            out.into_iter()
                .zip(by_words(questions, settled))
                .map(|(m, w)| if m.may_ask() { w } else { m })
                .collect()
        }
        Err(e) => {
            warn!(error = %e, "Could not check the plan's questions; comparing words");
            by_words(questions, settled)
        }
    }
}

/// What the planner is told when its plan asks settled questions again.
pub fn send_back(asked: &[(Question, Verdict)], settled: &[Answer]) -> String {
    let mut out = String::from(
        "Your plan asks what is already settled on this ticket, or what CoderHelm decides \
         itself. Don't ask these:\n",
    );
    for (q, v) in asked {
        out.push_str(&format!("- You asked: {}\n", one_line(&q.question)));
        match v {
            Verdict::Settled(Some(i)) => {
                out.push_str(&format!("  Settled: {}\n", one_line(&settled[*i].answer)))
            }
            Verdict::Settled(None) => out.push_str("  Settled by the answers above.\n"),
            Verdict::SystemDecides => out.push_str(
                "  This is CoderHelm's own call: decide it (never close or replace a pull \
                 request someone else opened; leave it open).\n",
            ),
            Verdict::Open => {}
        }
    }
    out.push_str(
        "Plan with what is settled now. Use CLARIFICATION_NEEDED only for something still \
         open, asking about that alone. Output the COMPLETE plan again in exactly the same \
         format as before (every file block, the fix decision included).",
    );
    out
}

/// A plan's `CLARIFICATION_NEEDED:` text with only the questions that may
/// be asked; when none may, one question says what is settled didn't
/// settle the plan.
pub fn keep_open(questions: &[Question], verdicts: &[Verdict]) -> String {
    let open: Vec<&Question> = questions
        .iter()
        .zip(verdicts)
        .filter(|(_, v)| v.may_ask())
        .map(|(q, _)| q)
        .collect();
    let mut out = String::from("CLARIFICATION_NEEDED:\n");
    if open.is_empty() {
        out.push_str(
            "- question: The answers so far didn't settle it. What else should I know to make \
             this change?\n",
        );
        return out;
    }
    for q in open {
        out.push_str(&format!("- question: {}\n", one_line(&q.question)));
        if !q.why.is_empty() {
            out.push_str(&format!("  why: {}\n", one_line(&q.why)));
        }
        if !q.suggestions.is_empty() {
            out.push_str(&format!("  options: {}\n", q.suggestions.join(" | ")));
        }
    }
    out
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ticket_text_and_people_comments_settle_questions() {
        let notes = vec![
            common::ticket_state::Note {
                author: "Sam".into(),
                text: "Moved to QA".into(),
                intent: common::ticket_state::Intent::StatusOnly,
                at: String::new(),
            },
            common::ticket_state::Note {
                author: "Ana".into(),
                text: "Yes, close the old pull request.".into(),
                intent: common::ticket_state::Intent::Other,
                at: "2026-10-27T10:00:00Z".into(),
            },
        ];
        let s = settled_on_ticket(
            &[],
            "Build the gallery block. This replaces the earlier attempt in #12.",
            &notes,
        );
        assert_eq!(s.len(), 2, "{s:?}");
        assert_eq!(s[0].question, TICKET_TEXT_QUESTION);
        assert!(s[0].answer.contains("replaces the earlier attempt"));
        assert_eq!(s[1].author, "Ana");
        // Nothing settled when there is nothing to read.
        assert!(settled_on_ticket(&[], "  ", &[]).is_empty());
        // A covered question is dropped; one the model left out stays.
        let qs = run_questions::from_lines(&["Does this replace #12?", "Who sees it?"]);
        let kept = open_only(qs, &[Verdict::Settled(Some(0))]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].question, "Who sees it?");
    }
    use common::ticket_state::{decision_answer, Decision, CHOICE_OWN, KIND_OPEN_PR};

    fn q(text: &str) -> Question {
        Question {
            question: text.into(),
            ..Default::default()
        }
    }

    fn own() -> Answer {
        decision_answer(&Decision {
            kind: KIND_OPEN_PR.into(),
            subject: "acme/web#12".into(),
            choice: CHOICE_OWN.into(),
            said: "Go ahead and open your own".into(),
            ..Default::default()
        })
    }

    #[test]
    fn the_settled_decisions_and_the_new_questions_go_to_the_model() {
        let p = prompt(
            &[q(
                "Should the work continue on #12, or should #12 be closed and replaced?",
            )],
            &[own()],
        );
        assert!(p.contains("1. Question: Pull request acme/web#12 is already open"));
        assert!(p.contains("Answer: Open CoderHelm's own pull request and leave acme/web#12 open"));
        assert!(p.contains("## Questions CoderHelm wants to ask now\n1. Should the work continue"));
        assert_eq!(
            schema()["properties"]["verdicts"]["items"]["properties"]["verdict"]["enum"],
            json!(["open", "covered", "system_decides"])
        );
    }

    /// The question from the field: the reply already said to open its own,
    /// and closing the other PR is not CoderHelm's call. Both readings drop
    /// it; an unrelated question stays.
    #[test]
    fn a_question_a_recorded_decision_covers_is_dropped() {
        let asked = [
            q(
                "The ticket comment says to open your own while #12 is still open. Should the \
               work continue as new commits on #12, or should #12 be closed and replaced by a \
               new PR?",
            ),
            q("Which error message should the form show?"),
        ];
        let reply = json!({"verdicts": [
            {"question": 1, "verdict": "covered", "settled_by": 1, "reason": "said open own"},
            {"question": 2, "verdict": "open", "settled_by": 0, "reason": "new"}
        ]});
        let v = apply(&reply, asked.len(), 1);
        assert_eq!(v, vec![Verdict::Settled(Some(0)), Verdict::Open]);
        let tasks = keep_open(&asked, &v);
        assert!(!tasks.contains("#12"));
        assert!(tasks.contains("- question: Which error message should the form show?"));

        let reply = json!({"verdicts": [
            {"question": 1, "verdict": "system_decides", "settled_by": 0, "reason": "not ours"}
        ]});
        assert_eq!(apply(&reply, 1, 1), vec![Verdict::SystemDecides]);
        let back = send_back(&[(asked[0].clone(), Verdict::Settled(Some(0)))], &[own()]);
        assert!(back.contains("Settled: Open CoderHelm's own pull request"));
    }

    #[test]
    fn a_bad_or_partial_reply_leaves_questions_open() {
        assert_eq!(apply(&json!({}), 2, 1), vec![Verdict::Open, Verdict::Open]);
        let reply = json!({"verdicts": [{"question": 9, "verdict": "covered", "settled_by": 7}]});
        assert_eq!(apply(&reply, 1, 1), vec![Verdict::Open]);
        assert_eq!(
            keep_open(&[q("A?")], &[Verdict::SystemDecides]),
            "CLARIFICATION_NEEDED:\n- question: The answers so far didn't settle it. What else \
             should I know to make this change?\n"
        );
    }

    #[test]
    fn the_word_reading_catches_a_plain_repeat() {
        let settled = [Answer {
            question: "Which environments get the new rate limit?".into(),
            answer: "Production".into(),
            ..Default::default()
        }];
        assert_eq!(
            by_words(
                &[
                    q("Which environments should get the new rate limit?"),
                    q("What color?")
                ],
                &settled
            ),
            vec![Verdict::Settled(Some(0)), Verdict::Open]
        );
    }
}
