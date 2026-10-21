//! The auto-merge gate's per-PR record, shared by the worker (which runs the
//! gate) and the gateway (which re-arms it on GitHub events).
//!
//! One record per PR (`pk` = team id, `sk` = [`record_sk`]) holds the gate's
//! state, the head it is bound to, the current arming (`chain_id`), the id of
//! the PR's one status comment, and when deploy labels were added. The
//! gateway reads it to decide whether an event should wake the gate; only the
//! worker writes it.

/// Sort-key prefix of every gate record.
pub const SK_PREFIX: &str = "MERGEGATE#";

/// Settings-table sort key of a PR's gate record (pk = team id).
pub fn record_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!("{SK_PREFIX}{owner}/{repo}#{pr:06}")
}

/// Both keys are not in yet (CoderHelm's approval, a person's approval).
/// Waits for a review event; never polls.
pub const STATE_WAITING_APPROVAL: &str = "waiting_approval";
/// Both keys are in; CI is still running. Polls (bounded) and wakes on a
/// completed check suite.
pub const STATE_WAITING_CI: &str = "waiting_ci";
/// CI finished with failures. Wakes when a check suite completes again (a
/// re-run) or on a new commit.
pub const STATE_BLOCKED_CI: &str = "blocked_ci";
/// GitHub refused the merge (branch protection, a conflict).
pub const STATE_DECLINED: &str = "declined";
/// Stopped polling after its window, or after errors kept failing. Any CI or
/// review event re-arms it.
pub const STATE_PAUSED: &str = "paused";
pub const STATE_MERGED: &str = "merged";
pub const STATE_CLOSED: &str = "closed";
/// A new commit arrived; the review of that commit re-arms the gate.
pub const STATE_HEAD_MOVED: &str = "head_moved";

/// Should a completed check suite on the gate's head re-arm it? Only while
/// the gate is waiting on (or was stopped by) CI or GitHub's merge rules.
pub fn wakes_on_ci(state: &str) -> bool {
    matches!(
        state,
        STATE_WAITING_CI | STATE_BLOCKED_CI | STATE_DECLINED | STATE_PAUSED
    )
}

/// Should a review event (an approval, a dismissal, a new review) on the
/// gate's head re-arm it? Any state that is not final: an approval can
/// complete the keys, and a dismissal or a request for changes must withdraw
/// one at once (the gate then shows it and never merges on it).
pub fn wakes_on_review(state: &str) -> bool {
    !matches!(state, STATE_MERGED | STATE_CLOSED | STATE_HEAD_MOVED)
}

/// Icons of the status comment's checklist lines (done, waiting, blocked).
pub const ICON_DONE: &str = "✅";
pub const ICON_WAITING: &str = "⏳";
pub const ICON_BLOCKED: &str = "🔴";

/// One line of the gate's checklist: `mark` is "done", "waiting" or
/// "blocked".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecklistLine {
    pub mark: &'static str,
    pub text: String,
}

/// Pure: the checklist lines of a stored status comment body (`- ✅ CI
/// green`), in order. Markdown code ticks are dropped so the dashboard can
/// show the text as is.
pub fn checklist(status_body: &str) -> Vec<ChecklistLine> {
    status_body
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- "))
        .filter_map(|l| {
            let (mark, rest) = [
                ("done", ICON_DONE),
                ("waiting", ICON_WAITING),
                ("blocked", ICON_BLOCKED),
            ]
            .into_iter()
            .find_map(|(m, icon)| l.strip_prefix(icon).map(|r| (m, r)))?;
            Some(ChecklistLine {
                mark,
                text: rest.trim().replace('`', ""),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checklist_reads_marked_lines_only() {
        let body = "<!-- marker -->\n**Auto-merge** · waiting\n\n- ✅ CoderHelm approved\n\
                    - ⏳ Waiting for a person's approval\n- 🔴 CI failing: `build`, `lint`\n\
                    - plain bullet\n\nNot merging.";
        assert_eq!(
            checklist(body),
            vec![
                ChecklistLine {
                    mark: "done",
                    text: "CoderHelm approved".into()
                },
                ChecklistLine {
                    mark: "waiting",
                    text: "Waiting for a person's approval".into()
                },
                ChecklistLine {
                    mark: "blocked",
                    text: "CI failing: build, lint".into()
                },
            ]
        );
        assert!(checklist("").is_empty());
    }

    #[test]
    fn record_key_is_zero_padded() {
        assert_eq!(record_sk("o", "r", 42), "MERGEGATE#o/r#000042");
    }

    #[test]
    fn ci_completion_wakes_only_ci_bound_states() {
        for s in [
            STATE_WAITING_CI,
            STATE_BLOCKED_CI,
            STATE_DECLINED,
            STATE_PAUSED,
        ] {
            assert!(wakes_on_ci(s), "{s}");
        }
        for s in [
            STATE_WAITING_APPROVAL,
            STATE_MERGED,
            STATE_CLOSED,
            STATE_HEAD_MOVED,
            "",
        ] {
            assert!(!wakes_on_ci(s), "{s}");
        }
    }

    #[test]
    fn review_events_wake_every_open_state() {
        for s in [
            STATE_WAITING_APPROVAL,
            STATE_WAITING_CI,
            STATE_BLOCKED_CI,
            STATE_DECLINED,
            STATE_PAUSED,
        ] {
            assert!(wakes_on_review(s), "{s}");
        }
        for s in [STATE_MERGED, STATE_CLOSED, STATE_HEAD_MOVED] {
            assert!(!wakes_on_review(s), "{s}");
        }
    }
}
