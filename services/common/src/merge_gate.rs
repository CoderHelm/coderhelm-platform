//! The auto-merge gate's per-PR record, shared by the worker (which runs the
//! gate) and the gateway (which re-arms it on GitHub events).

/// Settings-table sort key of a PR's gate record (pk = team id).
pub fn record_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!("MERGEGATE#{owner}/{repo}#{pr:06}")
}

pub const STATE_WAITING_APPROVAL: &str = "waiting_approval";
pub const STATE_WAITING_CI: &str = "waiting_ci";
pub const STATE_BLOCKED_CI: &str = "blocked_ci";
pub const STATE_DECLINED: &str = "declined";
pub const STATE_PAUSED: &str = "paused";
pub const STATE_MERGED: &str = "merged";
pub const STATE_CLOSED: &str = "closed";
pub const STATE_HEAD_MOVED: &str = "head_moved";

/// Should a completed check suite on the gate's head re-arm it? Only while
/// the gate is waiting on (or stopped by) CI or on GitHub's merge rules.
pub fn wakes_on_ci(state: &str) -> bool {
    matches!(
        state,
        STATE_WAITING_CI | STATE_BLOCKED_CI | STATE_DECLINED | STATE_PAUSED
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
