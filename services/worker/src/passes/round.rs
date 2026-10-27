//! The worker's side of fix rounds (see `common::run_round`): each round on
//! an open pull request runs inside its own budget, is written on the run
//! record while it works, continues in a fresh invocation when it runs out
//! of time, and when it can't finish leaves the run completed with the pull
//! request open and a plain account of what the round pushed.

use std::time::{Duration, Instant};

use common::run_round::{self, Incomplete, Kind, Round};
use common::run_status::EndedValues;
use tracing::{info, warn};

use super::attr_s;
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// When a round that starts in an invocation begun at `invocation_start`
/// must wrap up: its own budget, never the run's age.
pub(crate) fn deadline(invocation_start: Instant) -> Instant {
    invocation_start + Duration::from_secs(run_round::BUDGET_SECS)
}

/// Seconds left before that deadline.
pub(crate) fn secs_left(invocation_start: Instant, now: Instant) -> u64 {
    deadline(invocation_start)
        .saturating_duration_since(now)
        .as_secs()
}

/// Whether a round may start now, or should be parked for a fresh
/// invocation with its whole budget.
pub(crate) fn may_start(invocation_start: Instant, now: Instant) -> bool {
    secs_left(invocation_start, now) >= run_round::MIN_START_SECS
}

/// The branch head, for what a round pushes after it ("" when unknown).
pub(crate) async fn branch_head(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
) -> String {
    github
        .get_ref(owner, repo, branch)
        .await
        .unwrap_or_default()
}

/// Writes the round on the run record. A new round also clears an earlier
/// round's "didn't finish": it is being worked again.
pub(crate) async fn record(state: &WorkerState, team_id: &str, run_id: &str, round: &Round) {
    let first = round.attempt <= 1;
    let expr = if first {
        "SET #round = :r, updated_at = :t REMOVE #inc"
    } else {
        "SET #round = :r, updated_at = :t"
    };
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(expr)
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_values(":r", round.to_attr())
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .ended_values();
    if first {
        req = req.expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD);
    }
    if let Err(e) = req.send().await {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record the fix round");
        }
    }
}

/// Removes the round from the run record: it finished.
pub(crate) async fn clear(state: &WorkerState, team_id: &str, run_id: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("REMOVE #round")
        .expression_attribute_names("#round", run_round::FIELD)
        .send()
        .await;
}

/// What the round pushed since it started, read from the branch.
async fn pushed(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base_sha: &str,
    branch: &str,
) -> (Vec<run_round::Pushed>, Vec<String>) {
    if base_sha.is_empty() {
        return (Vec::new(), Vec::new());
    }
    match github.get_diff(owner, repo, base_sha, branch).await {
        Ok(compare) => run_round::pushed_from_compare(&compare),
        Err(e) => {
            warn!(error = %e, "Could not read what the round pushed");
            (Vec::new(), Vec::new())
        }
    }
}

/// Where a round's pull request is.
pub(crate) struct Pr<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
}

/// Leaves the run completed with its pull request open, saying the round
/// didn't finish, why, and the commits it pushed. Never a failed run: the
/// pull request is there and Resume runs the round again.
pub(crate) async fn finish_incomplete(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pr: &Pr<'_>,
    round: &Round,
    reason: &str,
) -> Incomplete {
    let now = chrono::Utc::now().to_rfc3339();
    let mut inc = Incomplete::of(round, reason, &now);
    let (commits, files) = pushed(pr.github, pr.owner, pr.repo, &round.base_sha, pr.branch).await;
    inc.pushed = commits;
    inc.files = files;
    // The caller completes the run (with its usage) and says so.
    write_incomplete(state, team_id, run_id, &inc).await;
    super::add_progress_note(
        state,
        team_id,
        run_id,
        &format!(
            "{} {}",
            inc.status_line(),
            run_round::pushed_note(&inc.pushed, &inc.files)
        ),
    )
    .await;
    warn!(
        run_id,
        reason, "Fix round didn't finish; the pull request stays open"
    );
    inc
}

/// The write: completed, with the round that didn't finish, never over a
/// run a person ended.
async fn write_incomplete(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    inc: &Incomplete,
) -> bool {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, #inc = :inc, current_pass = :cp, updated_at = :t \
             REMOVE #round, error_message, #err",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", attr_s(common::run_status::COMPLETED))
        .expression_attribute_values(
            ":sri",
            attr_s(&format!("{}#{run_id}", common::run_status::COMPLETED)),
        )
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":t", attr_s(&inc.at))
        .ended_values()
        .send()
        .await;
    match res {
        Ok(_) => true,
        Err(e) if common::run_fail::is_condition_failed(&e) => {
            info!(run_id, "Run was ended by a person — leaving it");
            false
        }
        Err(e) => {
            warn!(run_id, error = %e, "Could not record the unfinished round");
            false
        }
    }
}

/// After a round's worker failed outright (an error the pass returned):
/// when the run has an open pull request, it is left completed with the
/// round named as not finished instead of failed. Returns false when the
/// run has no pull request (a real failure, for the caller to record) or
/// was ended already.
pub(crate) async fn end_unfinished(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    kind: Kind,
    error: &str,
    only_running: bool,
) -> bool {
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .consistent_read(true)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .send()
        .await
    {
        Ok(r) => r.item.unwrap_or_default(),
        Err(_) => return false,
    };
    let num = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if num("pr_number") == 0
        || (only_running && status != common::run_status::RUNNING)
        || matches!(
            status.as_str(),
            "merged" | "cancelled" | "closed" | "retried" | "archived" | "failed"
        )
    {
        return false;
    }
    let round = Round::from_item(&item)
        .unwrap_or_else(|| Round::start(kind, 0, "", "", true, chrono::Utc::now()));
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::of(&round, &run_round::errored(round.kind, error), &now);
    if write_incomplete(state, team_id, run_id, &inc).await {
        super::webhooks::emit_run(state, team_id, run_id, "completed").await;
    }
    super::add_progress_note(state, team_id, run_id, &inc.status_line()).await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_round_starts_only_with_enough_of_its_budget_left() {
        let start = Instant::now();
        assert!(may_start(start, start));
        assert_eq!(secs_left(start, start), run_round::BUDGET_SECS);
        // After the review used most of the invocation, the fix round is
        // parked for a fresh one instead of being killed half way.
        let late = start + Duration::from_secs(run_round::BUDGET_SECS - 60);
        assert!(!may_start(start, late));
        let edge = start + Duration::from_secs(run_round::BUDGET_SECS - run_round::MIN_START_SECS);
        assert!(may_start(start, edge));
        // Past the deadline: nothing left, never a wrap-around.
        assert_eq!(secs_left(start, start + Duration::from_secs(5_000)), 0);
    }
}
