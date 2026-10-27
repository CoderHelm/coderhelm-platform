//! What CoderHelm knows about a ticket beyond its runs: the comments people
//! wrote on it (read and classified by the worker) and whether CoderHelm's
//! work on it is done.
//!
//! One record per ticket on the settings table (`pk` = team, `sk` =
//! [`sk`]). The worker writes it when it reads a ticket comment; the gateway
//! reads it before a ticket event may start a run, and the planner reads the
//! comments as part of the ticket's history.

/// The settings-table sort key of a ticket's record.
pub fn sk(ticket_id: &str) -> String {
    format!("TICKETSTATE#{}", ticket_id.to_ascii_uppercase())
}

/// Field: `"done"` once CoderHelm's work on the ticket is finished (a person
/// said the fix was made elsewhere, or that it is resolved).
pub const FIELD_STATE: &str = "state";
/// Field: why the ticket is done, in one line.
pub const FIELD_DONE_REASON: &str = "done_reason";
/// Field: the content hash of the ticket when it was marked done. An edit to
/// the ticket's text after that is new work.
pub const FIELD_DONE_HASH: &str = "done_context_hash";
/// Field: the comments read so far, JSON (`Vec<Note>`), newest last.
pub const FIELD_NOTES: &str = "notes";

pub const STATE_DONE: &str = "done";

/// Most comments kept on the record.
pub const MAX_NOTES: usize = 20;

/// What a comment on a ticket is, as the worker's model read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    /// Asks for new or different work in the code.
    NewRequirement,
    /// Says the fix was made, or belongs, somewhere else (a setting in
    /// another system, a data change), or that the issue is resolved.
    FixElsewhereInfo,
    /// Asks a question.
    Question,
    /// Only reports status ("looking into it", "moved to QA").
    StatusOnly,
    Other,
}

impl Intent {
    pub fn parse(s: &str) -> Intent {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "new_requirement" => Intent::NewRequirement,
            "fix_elsewhere_info" | "fix_elsewhere" | "informational" => Intent::FixElsewhereInfo,
            "question" => Intent::Question,
            "status_only" | "status" => Intent::StatusOnly,
            _ => Intent::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Intent::NewRequirement => "new_requirement",
            Intent::FixElsewhereInfo => "fix_elsewhere_info",
            Intent::Question => "question",
            Intent::StatusOnly => "status_only",
            Intent::Other => "other",
        }
    }
}

/// One comment a person wrote on the ticket.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Note {
    pub author: String,
    pub text: String,
    pub intent: Intent,
    pub at: String,
}

/// The notes on a record, oldest first; empty when unreadable.
pub fn parse_notes(json: &str) -> Vec<Note> {
    serde_json::from_str(json).unwrap_or_default()
}

/// Field: decisions people made on CoderHelm's notices and questions, JSON
/// (`Vec<Decision>`), newest last. Every later run honours them.
pub const FIELD_DECISIONS: &str = "decisions";
/// Field: the notices CoderHelm posted on the ticket, JSON (`Vec<Notice>`),
/// newest last, so the same one isn't posted twice.
pub const FIELD_NOTICES: &str = "notices";
/// Most notices kept on the record.
pub const MAX_NOTICES: usize = 30;

/// Decision kind: what to do about an open pull request someone else
/// opened for the ticket. `subject` is `owner/name#number`.
pub const KIND_OPEN_PR: &str = "open_pr";
/// Open CoderHelm's own pull request, leaving the other one as it is.
pub const CHOICE_OWN: &str = "own";
/// Work on the other pull request instead.
pub const CHOICE_EXISTING: &str = "existing";

/// A choice a person made on one of CoderHelm's notices.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Decision {
    /// What it decides, e.g. [`KIND_OPEN_PR`].
    pub kind: String,
    /// What it is about, e.g. `acme/web#12`.
    pub subject: String,
    /// The choice, e.g. [`CHOICE_OWN`].
    pub choice: String,
    /// What the person wrote, word for word (trimmed).
    #[serde(default)]
    pub said: String,
    /// Who said it, as the ticket shows them.
    #[serde(default)]
    pub by: String,
    /// When it was recorded (RFC 3339).
    #[serde(default)]
    pub at: String,
}

impl Decision {
    /// The key a question carries for this decision (`kind:subject`).
    pub fn key(&self) -> String {
        decision_key(&self.kind, &self.subject)
    }
}

/// `kind:subject`, as a question's `decision` field holds it.
pub fn decision_key(kind: &str, subject: &str) -> String {
    format!("{kind}:{subject}")
}

/// A notice CoderHelm posted on the ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Notice {
    /// What it is about: a decision key, or `questions` for a list of
    /// questions.
    pub key: String,
    /// A hash of its text: the same text is never posted twice.
    pub text_hash: String,
    /// The comment's address, when known.
    #[serde(default)]
    pub url: String,
    /// When it was posted (RFC 3339).
    #[serde(default)]
    pub at: String,
}

/// The decisions on a record, oldest first; empty when unreadable.
pub fn parse_decisions(json: &str) -> Vec<Decision> {
    serde_json::from_str(json).unwrap_or_default()
}

/// The notices on a record, oldest first; empty when unreadable.
pub fn parse_notices(json: &str) -> Vec<Notice> {
    serde_json::from_str(json).unwrap_or_default()
}

/// The newest decision on `key` (`kind:subject`).
pub fn decision_on<'a>(decisions: &'a [Decision], key: &str) -> Option<&'a Decision> {
    decisions.iter().rev().find(|d| d.key() == key)
}

/// `decisions` with `new` in place of any earlier one on the same key.
pub fn with_decision(mut decisions: Vec<Decision>, new: Decision) -> Vec<Decision> {
    decisions.retain(|d| d.key() != new.key());
    decisions.push(new);
    decisions
}

/// The notice already posted with this exact text, or for this key and not
/// decided since: posting it again only repeats it.
pub fn posted_before<'a>(
    notices: &'a [Notice],
    key: &str,
    text_hash: &str,
    decided: bool,
) -> Option<&'a Notice> {
    notices
        .iter()
        .rev()
        .find(|n| n.text_hash == text_hash || (!decided && n.key == key && !key.is_empty()))
}

/// `notices` with `new` added, the oldest dropped past [`MAX_NOTICES`].
pub fn with_notice(mut notices: Vec<Notice>, new: Notice) -> Vec<Notice> {
    notices.push(new);
    if notices.len() > MAX_NOTICES {
        let drop = notices.len() - MAX_NOTICES;
        notices.drain(..drop);
    }
    notices
}

/// A decision as a settled question and answer, for the run's prompts
/// (see `run_questions::answers_note`).
pub fn decision_answer(d: &Decision) -> crate::run_questions::Answer {
    let (question, answer) = match (d.kind.as_str(), d.choice.as_str()) {
        (KIND_OPEN_PR, CHOICE_OWN) => (
            format!(
                "Pull request {} is already open for this ticket: should CoderHelm work on it, \
                 open its own, or close or replace it?",
                d.subject
            ),
            format!(
                "Open CoderHelm's own pull request and leave {} open as it is. (They wrote: \"{}\")",
                d.subject, d.said
            ),
        ),
        (KIND_OPEN_PR, CHOICE_EXISTING) => (
            format!(
                "Pull request {} is already open for this ticket: should CoderHelm work on it, \
                 or open its own?",
                d.subject
            ),
            format!(
                "Work on {} instead of opening another pull request. (They wrote: \"{}\")",
                d.subject, d.said
            ),
        ),
        _ => (
            format!("{} ({})", d.kind, d.subject),
            format!("{} (They wrote: \"{}\")", d.choice, d.said),
        ),
    };
    crate::run_questions::Answer {
        question_id: d.key(),
        question,
        answer,
        author: d.by.clone(),
        answered_at: d.at.clone(),
        decision: d.key(),
    }
}

/// Whether a ticket event may start a new run, given the ticket's record:
/// a done ticket stays done until its text changes.
pub fn done_blocks_run(
    state: Option<&str>,
    done_hash: Option<&str>,
    new_hash: Option<&str>,
) -> bool {
    if state != Some(STATE_DONE) {
        return false;
    }
    match (done_hash, new_hash) {
        (Some(d), Some(n)) if !d.is_empty() => d == n,
        // Unknown either way: a done ticket stays done.
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_done_ticket_stays_done_until_its_text_changes() {
        assert!(done_blocks_run(Some("done"), Some("h1"), Some("h1")));
        assert!(!done_blocks_run(Some("done"), Some("h1"), Some("h2")));
        assert!(done_blocks_run(Some("done"), None, Some("h2")));
        assert!(!done_blocks_run(None, None, Some("h2")));
    }

    #[test]
    fn a_later_decision_replaces_an_earlier_one_on_the_same_pr() {
        let d = |choice: &str| Decision {
            kind: KIND_OPEN_PR.into(),
            subject: "acme/web#12".into(),
            choice: choice.into(),
            said: "go ahead".into(),
            ..Default::default()
        };
        let all = with_decision(vec![d(CHOICE_EXISTING)], d(CHOICE_OWN));
        assert_eq!(all.len(), 1);
        assert_eq!(
            decision_on(&all, "open_pr:acme/web#12").unwrap().choice,
            CHOICE_OWN
        );
        assert!(decision_on(&all, "open_pr:acme/web#13").is_none());
        let a = decision_answer(&all[0]);
        assert!(a.answer.contains("leave acme/web#12 open"));
        assert!(a.answer.contains("\"go ahead\""));
        assert_eq!(a.decision, "open_pr:acme/web#12");
    }

    #[test]
    fn a_notice_is_posted_once() {
        let n = Notice {
            key: "open_pr:acme/web#12".into(),
            text_hash: "h1".into(),
            url: "https://example.com/c/1".into(),
            at: "2026-10-27T10:00:00Z".into(),
        };
        let all = with_notice(Vec::new(), n);
        // Same text: never again.
        assert!(posted_before(&all, "other", "h1", true).is_some());
        // Same subject, not decided yet: link to the earlier one.
        assert!(posted_before(&all, "open_pr:acme/web#12", "h2", false).is_some());
        // Decided since, new text: a new notice may go out.
        assert!(posted_before(&all, "open_pr:acme/web#12", "h2", true).is_none());
        let many = (0..40).fold(Vec::new(), |acc, i| {
            with_notice(
                acc,
                Notice {
                    key: format!("k{i}"),
                    ..Default::default()
                },
            )
        });
        assert_eq!(many.len(), MAX_NOTICES);
        assert_eq!(many[0].key, "k10");
    }

    #[test]
    fn intents_read_loosely() {
        assert_eq!(
            Intent::parse("fix-elsewhere-info"),
            Intent::FixElsewhereInfo
        );
        assert_eq!(Intent::parse("Status only"), Intent::StatusOnly);
        assert_eq!(Intent::parse("???"), Intent::Other);
        assert_eq!(sk("abc-1"), "TICKETSTATE#ABC-1");
    }
}
