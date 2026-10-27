//! A run's status: the words, which ones a person set, and what closing and
//! reopening a run do.
//!
//! A run ends one of two ways a person chose. `cancelled` stops a run that is
//! working (the dashboard's Cancel, or its PR closed by hand). `closed` says a
//! person is done with the run on purpose ("not needed", "duplicate", "done by
//! hand"): it stops any work, frees the ticket, stops the CI fixes and the
//! auto-merge gate, and stays closed until a person reopens it. No worker
//! write may bring either back, so every conditional write that refuses to
//! overwrite a cancel refuses a close too ([`not_ended`]).
//!
//! The tests below read the services' sources: a status list that names
//! `cancelled` beside other run statuses must name `closed` as well, and no
//! write may guard against a cancel alone.

use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;

pub const PENDING: &str = "pending";
pub const QUEUED: &str = "queued";
pub const RUNNING: &str = "running";
pub const AWAITING_CI: &str = "awaiting_ci";
pub const NEEDS_INPUT: &str = "needs_input";
pub const NEEDS_OUTSIDE_CHANGE: &str = "needs_outside_change";
pub const COMPLETED: &str = "completed";
/// Older runs finished with this word before `completed`.
pub const SUCCESS: &str = "success";
pub const MERGED: &str = "merged";
pub const FAILED: &str = "failed";
pub const CANCELLED: &str = "cancelled";
/// A person closed the run on purpose.
pub const CLOSED: &str = "closed";
/// Set on the old run when it is retried or started over.
pub const RETRIED: &str = "retried";
/// Hidden from the runs list unless asked for by status.
pub const ARCHIVED: &str = "archived";

/// Every status a run record carries.
pub const ALL: [&str; 14] = [
    PENDING,
    QUEUED,
    RUNNING,
    AWAITING_CI,
    NEEDS_INPUT,
    NEEDS_OUTSIDE_CHANGE,
    COMPLETED,
    SUCCESS,
    MERGED,
    FAILED,
    CANCELLED,
    CLOSED,
    RETRIED,
    ARCHIVED,
];

/// Statuses a person set to end the run. No worker write overwrites them.
pub const ENDED_BY_PERSON: [&str; 2] = [CANCELLED, CLOSED];

/// A person ended the run: work on it stops and nothing resumes it.
pub fn ended_by_person(status: &str) -> bool {
    ENDED_BY_PERSON.contains(&status)
}

/// The lesson trigger for a person's answer to "why was this closed?" on
/// CoderHelm's PR (the gateway sends it, the worker's lessons read it).
pub const CLOSE_REASON_TRIGGER: &str = "close_reason";

/// The condition that refuses to overwrite a run a person ended, for the
/// status attribute behind `status_name` (`#s`, `#status`). Add its values
/// with [`EndedValues::ended_values`].
pub fn not_ended(status_name: &str) -> String {
    format!("NOT ({status_name} IN (:ended_cancelled, :ended_closed))")
}

/// Adds the values [`not_ended`] names to an update, in its chain:
/// `.condition_expression(not_ended("#s")).ended_values()`.
pub trait EndedValues {
    fn ended_values(self) -> Self;
}

impl EndedValues for UpdateItemFluentBuilder {
    fn ended_values(self) -> Self {
        self.expression_attribute_values(":ended_cancelled", AttributeValue::S(CANCELLED.into()))
            .expression_attribute_values(":ended_closed", AttributeValue::S(CLOSED.into()))
    }
}

/// Working right now (or about to): closing stops the work.
pub const IN_FLIGHT: [&str; 4] = [PENDING, QUEUED, RUNNING, AWAITING_CI];

/// Over already: there is nothing left to close.
pub const OVER: [&str; 4] = [CANCELLED, MERGED, RETRIED, ARCHIVED];

/// Whether a run in `status` may be closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseCheck {
    /// Close it; reopening later shows it as [`reopen_status`] of this.
    Close,
    /// Closed already: closing again changes nothing.
    AlreadyClosed,
    /// Over already, with the words to say so.
    Refused(String),
}

/// Whether a run in `status` may be closed. In-flight runs, runs waiting on a
/// person and finished runs whose PR is still someone's to deal with may be
/// closed; a run that is already over (cancelled, merged, started again,
/// archived) may not.
pub fn close_check(status: &str) -> CloseCheck {
    match status {
        CLOSED => CloseCheck::AlreadyClosed,
        CANCELLED => CloseCheck::Refused("It was cancelled already.".into()),
        MERGED => CloseCheck::Refused("Its pull request merged already.".into()),
        RETRIED => CloseCheck::Refused("It was started again; close the newer run instead.".into()),
        ARCHIVED => CloseCheck::Refused("It is archived.".into()),
        "" => CloseCheck::Refused("It has no status.".into()),
        _ => CloseCheck::Close,
    }
}

/// The status a reopened run shows: the one it had when it was closed, or
/// `needs_input` when it was working then (reopening never restarts work;
/// Retry does that).
pub fn reopen_status(closed_from: &str) -> &'static str {
    if IN_FLIGHT.contains(&closed_from) {
        return NEEDS_INPUT;
    }
    ALL.iter()
        .copied()
        .find(|s| *s == closed_from && !ENDED_BY_PERSON.contains(s) && *s != ARCHIVED)
        .unwrap_or(NEEDS_INPUT)
}

/// Longest close reason kept.
pub const MAX_REASON_CHARS: usize = 200;

/// A close reason as kept: trimmed, one line, bounded; `None` when empty.
pub fn close_reason(text: &str) -> Option<String> {
    let one_line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = one_line.chars().take(MAX_REASON_CHARS).collect();
    let cut = cut.trim().to_string();
    (!cut.is_empty()).then_some(cut)
}

/// The note on the run's ticket: "Closed by X: reason".
pub fn ticket_note(by: &str, reason: Option<&str>) -> String {
    let by = if by.trim().is_empty() {
        "a team member"
    } else {
        by.trim()
    };
    let head = match reason.map(str::trim).filter(|r| !r.is_empty()) {
        Some(r) => format!("Closed by {by}: {r}"),
        None => format!("Closed by {by}."),
    };
    format!(
        "{head}\n\nCoderHelm stopped its work on this ticket. Editing or reopening the ticket starts a new run."
    )
}

/// The comment left on the run's PR when the person also closes it.
pub fn pr_close_comment(by: &str, reason: Option<&str>) -> String {
    let by = if by.trim().is_empty() {
        "a team member"
    } else {
        by.trim()
    };
    match reason.map(str::trim).filter(|r| !r.is_empty()) {
        Some(r) => format!("Closed by {by} in CoderHelm: {r}"),
        None => format!("Closed by {by} in CoderHelm."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_is_a_status_a_person_ends_a_run_with() {
        assert!(ALL.contains(&CLOSED));
        assert!(ended_by_person(CLOSED));
        assert!(ended_by_person(CANCELLED));
        for s in ALL.iter().filter(|s| !ENDED_BY_PERSON.contains(s)) {
            assert!(!ended_by_person(s), "{s}");
        }
        assert_eq!(
            not_ended("#s"),
            "NOT (#s IN (:ended_cancelled, :ended_closed))"
        );
    }

    #[test]
    fn what_may_be_closed() {
        for s in [
            PENDING,
            QUEUED,
            RUNNING,
            AWAITING_CI,
            NEEDS_INPUT,
            NEEDS_OUTSIDE_CHANGE,
            COMPLETED,
            SUCCESS,
            FAILED,
        ] {
            assert_eq!(close_check(s), CloseCheck::Close, "{s}");
        }
        assert_eq!(close_check(CLOSED), CloseCheck::AlreadyClosed);
        for s in OVER {
            assert!(matches!(close_check(s), CloseCheck::Refused(_)), "{s}");
        }
        assert!(matches!(close_check(""), CloseCheck::Refused(_)));
        // Every status is decided; none falls through by accident.
        for s in ALL {
            let over = OVER.contains(&s);
            assert_eq!(
                matches!(close_check(s), CloseCheck::Refused(_)),
                over,
                "{s}"
            );
        }
    }

    #[test]
    fn reopening_restores_the_status_or_waits_on_a_person() {
        assert_eq!(reopen_status(COMPLETED), COMPLETED);
        assert_eq!(reopen_status(FAILED), FAILED);
        assert_eq!(reopen_status(NEEDS_OUTSIDE_CHANGE), NEEDS_OUTSIDE_CHANGE);
        assert_eq!(reopen_status(NEEDS_INPUT), NEEDS_INPUT);
        for s in IN_FLIGHT {
            assert_eq!(reopen_status(s), NEEDS_INPUT, "{s}");
        }
        for s in ["", "closed", "cancelled", "archived", "bogus"] {
            assert_eq!(reopen_status(s), NEEDS_INPUT, "{s}");
        }
    }

    #[test]
    fn reasons_are_one_bounded_line() {
        assert_eq!(close_reason("  "), None);
        assert_eq!(
            close_reason(" Duplicate\n of ABC-2 "),
            Some("Duplicate of ABC-2".into())
        );
        assert_eq!(
            close_reason(&"x".repeat(500)).unwrap().chars().count(),
            MAX_REASON_CHARS
        );
    }

    #[test]
    fn the_ticket_note_names_who_and_why() {
        let note = ticket_note("Sam Lee", Some("Not needed"));
        assert!(note.starts_with("Closed by Sam Lee: Not needed\n"));
        assert!(note.contains("starts a new run"));
        assert!(ticket_note("", None).starts_with("Closed by a team member."));
        assert_eq!(
            pr_close_comment("@sam", Some("Duplicate")),
            "Closed by @sam in CoderHelm: Duplicate"
        );
    }

    fn services_sources() -> Vec<(std::path::PathBuf, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let mut out = Vec::new();
        let mut stack = vec![root.join("services"), root.join("lambda")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let p = entry.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if p.is_dir() {
                    if name != "target" && !name.starts_with('.') && name != "node_modules" {
                        stack.push(p);
                    }
                    continue;
                }
                if !(name.ends_with(".rs") || name.ends_with(".py")) {
                    continue;
                }
                if p.ends_with("common/src/run_status.rs") {
                    continue;
                }
                out.push((p.clone(), std::fs::read_to_string(&p).unwrap_or_default()));
            }
        }
        out
    }

    /// The bracketed list (`[...]`, `(...)`, `{...}`) around byte `at`.
    fn enclosing_list(text: &str, at: usize) -> &str {
        let bytes = text.as_bytes();
        let (mut depth, mut start) = (0i32, None);
        for i in (0..at).rev() {
            match bytes[i] {
                b')' | b']' | b'}' => depth += 1,
                b'(' | b'[' | b'{' if depth > 0 => depth -= 1,
                b'(' | b'[' | b'{' => {
                    start = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(start) = start else { return "" };
        let mut depth = 0i32;
        for (i, b) in bytes.iter().enumerate().skip(start) {
            match b {
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &text[start..=i];
                    }
                }
                _ => {}
            }
        }
        ""
    }

    /// A list of run statuses that names "cancelled" also names "closed".
    /// (Lists of CI conclusions, which have a "cancelled" of their own, name
    /// no other run status and are left alone.)
    #[test]
    fn every_status_list_with_cancelled_names_closed() {
        let others = [
            "\"completed\"",
            "\"failed\"",
            "\"needs_input\"",
            "\"merged\"",
            "\"awaiting_ci\"",
            "\"retried\"",
            "\"done\"",
        ];
        let mut offenders = Vec::new();
        for (path, text) in services_sources() {
            for (at, _) in text.match_indices("\"cancelled\"") {
                let list = enclosing_list(&text, at);
                // A big block (a function body, a whole test) is not a list.
                if list.is_empty() || list.len() > 600 {
                    continue;
                }
                let run_statuses = others.iter().filter(|o| list.contains(*o)).count();
                if run_statuses >= 2 && !list.contains("\"closed\"") && !list.contains("CLOSED") {
                    let line = text[..at].lines().count();
                    offenders.push(format!("{}:{line}", path.display()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "status lists naming \"cancelled\" without \"closed\": {offenders:#?}"
        );
    }

    /// A write that refuses to overwrite a cancelled run refuses a closed one
    /// too, through `not_ended`, never `<> :cancel...` alone; a check that
    /// stops work on a cancelled run stops it on a closed one, through
    /// `ended_by_person`, never `== "cancelled"` alone.
    #[test]
    fn no_write_guards_against_a_cancel_alone() {
        let mut offenders = Vec::new();
        for (path, text) in services_sources() {
            for (i, line) in text.lines().enumerate() {
                let l = line.replace(' ', "");
                // A CI conclusion has its own "cancelled"; a line marked
                // "cancel only:" means the cancel and nothing else.
                if l.contains("conclusion") || line.contains("cancel only:") {
                    continue;
                }
                if l.contains("<>:cancel") || l.contains("==\"cancelled\"") {
                    offenders.push(format!("{}:{}", path.display(), i + 1));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "use run_status::not_ended / ended_by_person: {offenders:#?}"
        );
    }
}
