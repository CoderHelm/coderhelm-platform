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
