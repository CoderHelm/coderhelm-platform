//! The questions a run asks when it stops as `needs_input`, and the answers
//! people give to them in CoderHelm.
//!
//! The worker stores what it asked on the run (`questions`, see [`Asked`])
//! next to the comment it posted on the ticket. The gateway takes answers
//! for those questions, posts them as a reply on the ticket, keeps them on
//! the run (`answers`) and starts the ticket again with them on the message
//! ([`crate::messages::TicketMessage::answers`]). The next run's triage and
//! planner read them word for word ([`answers_note`]), and a plan that asks
//! an answered question again is sent back ([`repeated`]).

use serde::{Deserialize, Serialize};

/// Run attribute holding the [`Asked`] JSON.
pub const FIELD_QUESTIONS: &str = "questions";
/// Run attribute holding the JSON list of [`Answer`]s given on it.
pub const FIELD_ANSWERS: &str = "answers";

/// The most questions kept for one stop.
pub const MAX_QUESTIONS: usize = 8;
/// The longest answer taken, in characters.
pub const MAX_ANSWER_CHARS: usize = 4_000;

const MAX_QUESTION_CHARS: usize = 600;
const MAX_WHY_CHARS: usize = 600;
const MAX_SUGGESTIONS: usize = 4;
const MAX_SUGGESTION_CHARS: usize = 200;

/// One thing a run needs a person to tell it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    /// Stable within the run: `q1`, `q2`, ...
    pub id: String,
    /// The question, in plain words.
    pub question: String,
    /// Why it matters: what it blocks. Empty when the run didn't say.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub why: String,
    /// Candidate answers the run found in the code or the data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
    /// Set when the question is a choice CoderHelm records on the ticket
    /// (see `ticket_state::Decision`), e.g. `open_pr:acme/web#12`; its
    /// `suggestions` are then the choices.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decision: String,
    /// The evidence the question cites (`path:line`, a lookup id): what in
    /// the code or the data makes it a question (see `stop_rules::screen`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<String>,
}

/// What a run asked when it stopped, and where it asked it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Asked {
    pub questions: Vec<Question>,
    /// When it asked (RFC 3339).
    #[serde(default)]
    pub asked_at: String,
    /// The comment with the questions on the ticket or issue, when posting
    /// it worked and its address is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment_url: Option<String>,
}

/// An answer a person gave in CoderHelm.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    pub question_id: String,
    /// The question as it was asked, kept with the answer so the next run
    /// reads both even when the asking run is gone.
    pub question: String,
    pub answer: String,
    /// Who answered, as the ticket shows them (their name or `@login`).
    pub author: String,
    /// When (RFC 3339).
    #[serde(default)]
    pub answered_at: String,
    /// The question's `decision` key, when it was a recorded choice.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decision: String,
}

/// Why answers were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnswerError {
    /// Every answer was blank.
    Empty,
    /// An answer names a question this run didn't ask.
    UnknownQuestion(String),
    /// An answer is longer than [`MAX_ANSWER_CHARS`].
    TooLong(String),
    /// The same question was answered twice in one request.
    Duplicate(String),
}

impl AnswerError {
    /// For a person to read.
    pub fn message(&self) -> String {
        match self {
            AnswerError::Empty => "Write an answer to at least one question.".to_string(),
            AnswerError::UnknownQuestion(id) => {
                format!("This run didn't ask a question with id \"{id}\".")
            }
            AnswerError::TooLong(id) => format!(
                "The answer to \"{id}\" is longer than {MAX_ANSWER_CHARS} characters. Shorten it or \
                 add the detail to the ticket."
            ),
            AnswerError::Duplicate(id) => format!("\"{id}\" is answered twice."),
        }
    }
}

/// The answers to keep, in the order the questions were asked: blank ones
/// dropped, each trimmed. At least one must be left.
pub fn validate(
    questions: &[Question],
    given: &[(String, String)],
    author: &str,
    now: &str,
) -> Result<Vec<Answer>, AnswerError> {
    let mut seen: Vec<&str> = Vec::new();
    for (id, text) in given {
        if !questions.iter().any(|q| q.id == *id) {
            return Err(AnswerError::UnknownQuestion(id.clone()));
        }
        if seen.contains(&id.as_str()) {
            return Err(AnswerError::Duplicate(id.clone()));
        }
        seen.push(id);
        if text.trim().chars().count() > MAX_ANSWER_CHARS {
            return Err(AnswerError::TooLong(id.clone()));
        }
    }
    let out: Vec<Answer> = questions
        .iter()
        .filter_map(|q| {
            let text = given.iter().find(|(id, _)| *id == q.id)?.1.trim();
            (!text.is_empty()).then(|| Answer {
                question_id: q.id.clone(),
                question: q.question.clone(),
                answer: text.to_string(),
                author: author.to_string(),
                answered_at: now.to_string(),
                decision: q.decision.clone(),
            })
        })
        .collect();
    if out.is_empty() {
        return Err(AnswerError::Empty);
    }
    Ok(out)
}

/// The questions in a planner's `CLARIFICATION_NEEDED:` text.
///
/// The planner is asked for one block per question:
///
/// ```text
/// - question: Which environments get the new limit?
///   why: Each environment has its own value in config/limits.yml.
///   options: Production only | Every environment
/// ```
///
/// Older or looser answers still read: bullet or numbered lines become one
/// question each, and prose becomes a single question.
pub fn parse(detail: &str) -> Vec<Question> {
    let detail = detail.trim();
    if detail.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<Question> = Vec::new();
    let mut structured = false;
    for raw in detail.lines() {
        let line = raw.trim();
        let item = strip_bullet(line);
        if let Some(q) = field(item, "question") {
            structured = true;
            out.push(Question {
                question: q.to_string(),
                ..Default::default()
            });
        } else if let (Some(why), Some(last)) = (field(item, "why"), out.last_mut()) {
            last.why = why.to_string();
        } else if let (Some(ev), Some(last)) = (field(item, "evidence"), out.last_mut()) {
            last.evidence = ev
                .split('|')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        } else if let (Some(opts), Some(last)) = (
            field(item, "options").or_else(|| field(item, "suggestions")),
            out.last_mut(),
        ) {
            last.suggestions = opts
                .split('|')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
    }
    if !structured {
        let bullets: Vec<&str> = detail
            .lines()
            .map(str::trim)
            .filter(|l| strip_bullet(l).len() < l.len())
            .map(strip_bullet)
            .filter(|l| !l.is_empty())
            .collect();
        out = if bullets.len() >= 2 {
            bullets
                .into_iter()
                .map(|q| Question {
                    question: q.to_string(),
                    ..Default::default()
                })
                .collect()
        } else {
            vec![Question {
                question: detail.to_string(),
                ..Default::default()
            }]
        };
    }
    tidy(out)
}

/// Questions from a list of plain lines (a plan decision's `questions`).
pub fn from_lines<S: AsRef<str>>(lines: &[S]) -> Vec<Question> {
    tidy(
        lines
            .iter()
            .map(|l| strip_bullet(l.as_ref().trim()).to_string())
            .filter(|l| !l.is_empty())
            .map(|question| Question {
                question,
                ..Default::default()
            })
            .collect(),
    )
}

/// Numbered ids, clipped text, at most [`MAX_QUESTIONS`].
fn tidy(questions: Vec<Question>) -> Vec<Question> {
    questions
        .into_iter()
        .filter(|q| !q.question.trim().is_empty())
        .take(MAX_QUESTIONS)
        .enumerate()
        .map(|(i, q)| Question {
            id: format!("q{}", i + 1),
            question: clip(q.question.trim(), MAX_QUESTION_CHARS),
            why: clip(q.why.trim(), MAX_WHY_CHARS),
            suggestions: q
                .suggestions
                .iter()
                .map(|s| clip(s.trim(), MAX_SUGGESTION_CHARS))
                .filter(|s| !s.is_empty())
                .take(MAX_SUGGESTIONS)
                .collect(),
            decision: q.decision,
            evidence: q
                .evidence
                .iter()
                .map(|s| clip(s.trim(), MAX_SUGGESTION_CHARS))
                .filter(|s| !s.is_empty())
                .take(MAX_SUGGESTIONS)
                .collect(),
        })
        .collect()
}

/// The questions as the ticket comment lists them (Markdown).
pub fn markdown(questions: &[Question]) -> String {
    let mut out = String::new();
    for (i, q) in questions.iter().enumerate() {
        out.push_str(&format!("{}. {}\n", i + 1, q.question));
        if !q.why.is_empty() {
            out.push_str(&format!("   _Why: {}_\n", q.why));
        }
        if !q.suggestions.is_empty() {
            out.push_str(&format!(
                "   Possible answers: {}\n",
                q.suggestions
                    .iter()
                    .map(|s| format!("`{s}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !q.evidence.is_empty() {
            out.push_str(&format!(
                "   Evidence: {}\n",
                q.evidence
                    .iter()
                    .map(|s| format!("`{s}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    out.trim_end().to_string()
}

/// The questions as plain text (a Jira comment, where Markdown shows as is).
pub fn plain(questions: &[Question]) -> String {
    questions
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let mut s = format!("{}. {}", i + 1, q.question);
            if !q.why.is_empty() {
                s.push_str(&format!(" (Why: {})", q.why));
            }
            if !q.suggestions.is_empty() {
                s.push_str(&format!(" Possible answers: {}.", q.suggestions.join("; ")));
            }
            if !q.evidence.is_empty() {
                s.push_str(&format!(" Evidence: {}.", q.evidence.join("; ")));
            }
            s
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The reply posted on the ticket for answers given in CoderHelm. `markdown`
/// for GitHub; plain paragraphs for Jira. `asked_at` links to the comment
/// with the questions, when known.
pub fn reply(author: &str, answers: &[Answer], asked_at: Option<&str>, markdown: bool) -> String {
    let who = if author.trim().is_empty() {
        "Someone on the team"
    } else {
        author.trim()
    };
    let mut out = if markdown {
        format!("**{who} answered in CoderHelm:**\n\n")
    } else {
        format!("{who} answered in CoderHelm:\n\n")
    };
    for a in answers {
        if markdown {
            let quoted = a
                .question
                .lines()
                .map(|l| format!("> {l}"))
                .collect::<Vec<_>>()
                .join("\n");
            out.push_str(&format!("{quoted}\n\n{}\n\n", a.answer));
        } else {
            out.push_str(&format!(
                "Q: {}\n\nA: {}\n\n",
                one_line(&a.question),
                a.answer
            ));
        }
    }
    if let Some(url) = asked_at.filter(|u| !u.is_empty()) {
        if markdown {
            out.push_str(&format!(
                "_In reply to [CoderHelm's questions]({url})._\n\n"
            ));
        } else {
            out.push_str(&format!("In reply to CoderHelm's questions: {url}\n\n"));
        }
    }
    out.push_str("CoderHelm is continuing the work with these answers.");
    out
}

/// The section the triage and planner prompts get: every answer word for
/// word, and that these questions are settled. Empty with no answers.
pub fn answers_note(answers: &[Answer]) -> String {
    if answers.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "\n\n## Resolved decisions (people's answers to CoderHelm's earlier questions and \
         notices, word for word)\n\
         An earlier run stopped to ask about this ticket, or posted a notice on it, and people \
         answered. Treat each answer as part of the ticket. These are settled: plan with them and \
         never ask them again, in any wording. Ask only about something they leave open, and say \
         what is still missing. Never ask a person to settle what CoderHelm decides itself: \
         closing or replacing a pull request someone else opened is never CoderHelm's call; once \
         told to open its own, it leaves the other one open and opens its own.\n",
    );
    for a in answers {
        out.push_str(&format!(
            "- Question: {}\n  Answer from {}: {}\n",
            one_line(&a.question),
            if a.author.is_empty() {
                "a teammate"
            } else {
                &a.author
            },
            a.answer.replace('\n', "\n  ")
        ));
    }
    out
}

/// Adds `more` to `into`, skipping any already there (same question and
/// answer), in order.
pub fn merge(into: &mut Vec<Answer>, more: &[Answer]) {
    for a in more {
        let same = |b: &Answer| {
            b.question.trim() == a.question.trim() && b.answer.trim() == a.answer.trim()
        };
        if !into.iter().any(same) {
            into.push(a.clone());
        }
    }
}

/// A stable digest of the answers (part of the plan cache key).
pub fn digest(answers: &[Answer]) -> String {
    if answers.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = answers
        .iter()
        .map(|a| format!("{}|{}", a.question, a.answer))
        .collect();
    let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
    crate::sha256_hex(&refs)
}

/// Whether `question` asks again what one of `answers` already answered.
pub fn already_answered(question: &str, answers: &[Answer]) -> bool {
    let asked = words(question);
    if asked.is_empty() {
        return false;
    }
    answers.iter().any(|a| {
        let before = words(&a.question);
        if before.is_empty() {
            return false;
        }
        let common = asked.iter().filter(|w| before.contains(w)).count();
        let smaller = asked.len().min(before.len());
        let union = asked.len() + before.len() - common;
        // Same question reworded: most of the shorter one's words, or a
        // large overlap overall.
        common * 10 >= smaller * 8 || common * 10 >= union * 6
    })
}

/// The questions in a plan's `CLARIFICATION_NEEDED:` text that were
/// answered already. Empty when the plan isn't asking or nothing repeats.
pub fn repeated(tasks: &str, answers: &[Answer]) -> Vec<Question> {
    let Some(detail) = tasks.trim_start().strip_prefix("CLARIFICATION_NEEDED:") else {
        return Vec::new();
    };
    if answers.is_empty() {
        return Vec::new();
    }
    parse(detail)
        .into_iter()
        .filter(|q| already_answered(&q.question, answers))
        .collect()
}

/// Of `questions`, the ones not answered already (ids kept).
pub fn unanswered(questions: Vec<Question>, answers: &[Answer]) -> Vec<Question> {
    questions
        .into_iter()
        .filter(|q| !already_answered(&q.question, answers))
        .collect()
}

/// What the planner is told when its plan asks an answered question again.
pub fn send_back(repeats: &[Question], answers: &[Answer]) -> String {
    let mut out = String::from(
        "Your plan asks again what people already answered in CoderHelm. Those questions are \
         settled:\n",
    );
    for q in repeats {
        out.push_str(&format!("- You asked: {}\n", one_line(&q.question)));
        if let Some(a) = answers
            .iter()
            .find(|a| already_answered(&q.question, std::slice::from_ref(a)))
        {
            out.push_str(&format!("  They answered: {}\n", one_line(&a.answer)));
        }
    }
    out.push_str(
        "Plan with those answers now. Use CLARIFICATION_NEEDED only for something the answers \
         leave open, asking about that alone. Output the COMPLETE plan again in exactly the same \
         format as before (every file block, the fix decision included).",
    );
    out
}

/// The words that carry a question's meaning: lowercase, three letters or
/// more, common filler dropped.
fn words(s: &str) -> Vec<String> {
    const FILLER: &[&str] = &[
        "the", "and", "for", "should", "which", "what", "when", "where", "this", "that", "with",
        "you", "your", "are", "does", "use", "from", "into", "will", "would", "can", "there",
        "they", "them", "its", "how", "who", "any", "all", "need", "needs", "please",
    ];
    let mut out: Vec<String> = s
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|w| w.chars().count() >= 3 && !FILLER.contains(&w.as_str()))
        .collect();
    out.sort();
    out.dedup();
    out
}

fn strip_bullet(line: &str) -> &str {
    let l = line.trim_start();
    for b in ["- ", "* ", "• "] {
        if let Some(rest) = l.strip_prefix(b) {
            return rest.trim();
        }
    }
    let digits = l.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits > 0 {
        let rest = &l[digits..];
        if let Some(r) = rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") ")) {
            return r.trim();
        }
    }
    l
}

/// `key: value` (any case), the value trimmed.
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let (k, v) = line.split_once(':')?;
    k.trim()
        .eq_ignore_ascii_case(key)
        .then(|| v.trim())
        .filter(|v| !v.is_empty())
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn clip(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let kept: String = s.chars().take(max_chars).collect();
    format!("{}…", kept.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(q: &str, a: &str) -> Answer {
        Answer {
            question_id: "q1".into(),
            question: q.into(),
            answer: a.into(),
            author: "Pat Doe".into(),
            answered_at: "2026-10-27T10:00:00Z".into(),
            decision: String::new(),
        }
    }

    #[test]
    fn reads_the_structured_blocks() {
        let qs = parse(
            "\n- question: Which environments get the new limit?\n  why: Each environment has its \
             own value.\n  options: Production only | Every environment\n- question: What is the \
             new limit?\n",
        );
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[0].id, "q1");
        assert_eq!(qs[0].question, "Which environments get the new limit?");
        assert_eq!(qs[0].why, "Each environment has its own value.");
        assert_eq!(
            qs[0].suggestions,
            vec!["Production only", "Every environment"]
        );
        assert_eq!(qs[1].id, "q2");
        assert!(qs[1].why.is_empty() && qs[1].suggestions.is_empty());
    }

    #[test]
    fn reads_bullets_and_prose() {
        let qs = parse("- Which file holds the copy?\n- What should it say?");
        assert_eq!(
            qs.iter().map(|q| q.question.as_str()).collect::<Vec<_>>(),
            vec!["Which file holds the copy?", "What should it say?"]
        );
        let qs = parse("The new timeout value is not in the ticket.");
        assert_eq!(qs.len(), 1);
        assert_eq!(
            qs[0].question,
            "The new timeout value is not in the ticket."
        );
        assert!(parse("   ").is_empty());
    }

    #[test]
    fn keeps_at_most_eight_and_clips() {
        let many: String = (0..12).map(|i| format!("- question: Q{i}?\n")).collect();
        assert_eq!(parse(&many).len(), MAX_QUESTIONS);
        let long = "x".repeat(2_000);
        assert!(parse(&long)[0].question.chars().count() <= MAX_QUESTION_CHARS + 1);
    }

    #[test]
    fn validate_keeps_the_written_answers_in_order() {
        let qs = parse("- question: First?\n- question: Second?");
        let got = validate(
            &qs,
            &[("q2".into(), "  two  ".into()), ("q1".into(), "".into())],
            "Pat Doe",
            "now",
        )
        .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].question_id, "q2");
        assert_eq!(got[0].question, "Second?");
        assert_eq!(got[0].answer, "two");
        assert_eq!(got[0].author, "Pat Doe");
    }

    #[test]
    fn validate_refuses_blank_unknown_long_and_twice() {
        let qs = parse("- question: First?");
        assert_eq!(
            validate(&qs, &[("q1".into(), "  ".into())], "a", "n"),
            Err(AnswerError::Empty)
        );
        assert_eq!(validate(&qs, &[], "a", "n"), Err(AnswerError::Empty));
        assert_eq!(
            validate(&qs, &[("q9".into(), "x".into())], "a", "n"),
            Err(AnswerError::UnknownQuestion("q9".into()))
        );
        assert_eq!(
            validate(
                &qs,
                &[("q1".into(), "x".repeat(MAX_ANSWER_CHARS + 1))],
                "a",
                "n"
            ),
            Err(AnswerError::TooLong("q1".into()))
        );
        assert_eq!(
            validate(
                &qs,
                &[("q1".into(), "a".into()), ("q1".into(), "b".into())],
                "a",
                "n"
            ),
            Err(AnswerError::Duplicate("q1".into()))
        );
    }

    #[test]
    fn the_planner_reads_answers_word_for_word() {
        let note = answers_note(&[answer(
            "Which environments get the new limit?",
            "Production only.\nStaging keeps 100.",
        )]);
        assert!(note.contains("Which environments get the new limit?"));
        assert!(note.contains("Answer from Pat Doe: Production only.\n  Staging keeps 100."));
        assert!(note.contains("never ask them again"));
        assert!(note.contains("Resolved decisions"));
        assert!(answers_note(&[]).is_empty());
    }

    #[test]
    fn an_answered_question_is_recognized_when_reworded() {
        let answers = [answer(
            "Which environments get the new rate limit?",
            "Production",
        )];
        assert!(already_answered(
            "Which environments should get the new rate limit?",
            &answers
        ));
        assert!(already_answered(
            "Which environments get the new rate limit?",
            &answers
        ));
        assert!(!already_answered(
            "What should the error message say?",
            &answers
        ));
        assert!(!already_answered("", &answers));
    }

    #[test]
    fn repeated_finds_only_answered_questions_in_a_clarification() {
        let answers = [answer(
            "Which environments get the new rate limit?",
            "Production",
        )];
        let tasks =
            "CLARIFICATION_NEEDED:\n- question: Which environments should get the new rate \
                     limit?\n- question: What should the error message say?";
        let r = repeated(tasks, &answers);
        assert_eq!(r.len(), 1);
        assert!(r[0].question.contains("environments"));
        assert!(repeated("- [ ] src/a.rs: do it", &answers).is_empty());
        assert!(repeated(tasks, &[]).is_empty());
        let left = unanswered(
            parse(tasks.strip_prefix("CLARIFICATION_NEEDED:").unwrap()),
            &answers,
        );
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].question, "What should the error message say?");
        let back = send_back(&r, &answers);
        assert!(back.contains("They answered: Production"));
    }

    #[test]
    fn the_reply_names_who_answered() {
        let a = [answer("Which page?", "The pricing page.")];
        let md = reply("Pat Doe", &a, Some("https://example.com/c/1"), true);
        assert!(md.starts_with("**Pat Doe answered in CoderHelm:**"));
        assert!(md.contains("> Which page?\n\nThe pricing page."));
        assert!(md.contains("(https://example.com/c/1)"));
        let plain = reply("", &a, None, false);
        assert!(plain.starts_with("Someone on the team answered in CoderHelm:"));
        assert!(plain.contains("Q: Which page?\n\nA: The pricing page."));
        assert!(!plain.contains("**"));
    }

    #[test]
    fn comment_renderings_list_why_and_options() {
        let qs =
            parse("- question: Which page?\n  why: Two pages share it.\n  options: Home | Pricing");
        let md = markdown(&qs);
        assert!(md.contains("1. Which page?"));
        assert!(md.contains("_Why: Two pages share it._"));
        assert!(md.contains("`Home`, `Pricing`"));
        let p = plain(&qs);
        assert!(p.contains(
            "1. Which page? (Why: Two pages share it.) Possible answers: Home; Pricing."
        ));
    }

    #[test]
    fn merging_skips_answers_already_there() {
        let mut have = vec![answer("A?", "1")];
        merge(&mut have, &[answer("A?", "1"), answer("B?", "2")]);
        assert_eq!(have.len(), 2);
        assert_eq!(have[1].question, "B?");
    }

    #[test]
    fn the_digest_changes_with_the_answers() {
        assert!(digest(&[]).is_empty());
        assert_ne!(digest(&[answer("A?", "1")]), digest(&[answer("A?", "2")]));
    }
}
