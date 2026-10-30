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
use crate::clients::github::{FileOp, GitHubClient};
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
    // What it saved is on its WIP branch; the pull request is untouched.
    let wip = if round.wip_branch.is_empty() {
        pr.branch
    } else {
        round.wip_branch.as_str()
    };
    let (commits, files) = pushed(pr.github, pr.owner, pr.repo, &round.base_sha, wip).await;
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
            run_round::pushed_note(&inc.pushed, &inc.files, wip)
        ),
    )
    .await;
    warn!(
        run_id,
        reason, "Fix round didn't finish; the pull request stays open"
    );
    inc
}

/// A round whose work is done but held from landing by CoderHelm's checks:
/// on the run record (`round_incomplete`, held), with its WIP branch, where
/// it started and what it was asked, so a mention on the pull request or
/// Resume picks it up first (common::held_work). The caller completes the
/// run as usual; the record stays until the work lands.
pub(crate) async fn hold(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    round: &Round,
    reason: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::held(round, reason, &now);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #inc = :inc, updated_at = :t")
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .ended_values()
        .send()
        .await;
    if let Err(e) = res {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record the held round");
        }
    }
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

/// Planned-work rounds stopped without progress: the run keeps its status
/// (`needs_input`, set by the caller) and says which round stopped and why;
/// the round leaves the record.
pub(crate) async fn stopped(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    round: &Round,
    reason: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::of(round, reason, &now);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #inc = :inc, updated_at = :t REMOVE #round")
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .ended_values()
        .send()
        .await;
    if let Err(e) = res {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record why the planned work stopped");
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

/// The round's WIP branch, ready to write to: the one it already has when
/// it continues (`reuse`), else a fresh one at the pull request's head.
/// Returns its name.
pub(crate) async fn open_wip(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_branch: &str,
    reuse: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if !reuse.is_empty() && github.get_ref(owner, repo, reuse).await.is_ok() {
        return Ok(reuse.to_string());
    }
    let wip = run_round::wip_branch(pr_branch);
    // A leftover WIP branch has no pull request, so this resets it to the
    // pull request's head.
    github.create_branch(owner, repo, &wip, pr_branch).await?;
    Ok(wip)
}

/// One file the round changed, from a compare of its start and its WIP head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Changed {
    pub path: String,
    pub removed: bool,
    /// The old path of a renamed file.
    pub previous: Option<String>,
}

/// The files a compare (`base...wip`) says the round changed.
pub(crate) fn changed_from_compare(compare: &serde_json::Value) -> Vec<Changed> {
    compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| {
                    let path = f["filename"].as_str()?.to_string();
                    let status = f["status"].as_str().unwrap_or("");
                    Some(Changed {
                        path,
                        removed: status == "removed",
                        previous: (status == "renamed")
                            .then(|| f["previous_filename"].as_str().map(String::from))
                            .flatten(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Files the pull request changed since the round started (someone else's
/// push) that the round changed too: landing would overwrite them.
pub(crate) fn overlap(ours: &[Changed], theirs: &[String]) -> Vec<String> {
    ours.iter()
        .flat_map(|c| std::iter::once(&c.path).chain(c.previous.iter()))
        .filter(|p| theirs.contains(p))
        .cloned()
        .collect()
}

/// How landing a round went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Landed {
    /// One commit on the pull request carries the round's changes.
    Pushed { files: usize },
    /// The round changed nothing.
    Nothing,
    /// Not landed (the WIP branch is kept): why.
    Refused(String),
}

/// Lands a finished round on the pull request: everything it changed on its
/// WIP branch since `base_sha`, as ONE commit with `message`, in one push.
/// Refuses (keeping the WIP branch) when the pull request changed the same
/// files meanwhile. The WIP branch is deleted once landed.
pub(crate) async fn land(pr: &Pr<'_>, wip: &str, base_sha: &str, message: &str) -> Landed {
    let (github, owner, repo) = (pr.github, pr.owner, pr.repo);
    if wip.is_empty() || wip == pr.branch || base_sha.is_empty() {
        return Landed::Refused("the round has no WIP branch to land".into());
    }
    let ours = match github.get_diff(owner, repo, base_sha, wip).await {
        Ok(c) => changed_from_compare(&c),
        Err(e) => return Landed::Refused(format!("its changes couldn't be read ({e})")),
    };
    if ours.is_empty() {
        let _ = github.delete_branch(owner, repo, wip).await;
        return Landed::Nothing;
    }
    match github.get_ref(owner, repo, pr.branch).await {
        Ok(head) if head != base_sha => {
            let theirs: Vec<String> = github
                .get_diff(owner, repo, base_sha, &head)
                .await
                .map(|c| {
                    changed_from_compare(&c)
                        .into_iter()
                        .flat_map(|c| std::iter::once(c.path).chain(c.previous))
                        .collect()
                })
                .unwrap_or_default();
            let clash = overlap(&ours, &theirs);
            if !clash.is_empty() {
                return Landed::Refused(format!(
                    "the pull request changed the same files meanwhile ({})",
                    clash.join(", ")
                ));
            }
        }
        Ok(_) => {}
        Err(e) => {
            return Landed::Refused(format!("the pull request's head couldn't be read ({e})"))
        }
    }
    let mut ops = Vec::new();
    for c in &ours {
        if let Some(prev) = &c.previous {
            ops.push(FileOp::Delete { path: prev.clone() });
        }
        if c.removed {
            ops.push(FileOp::Delete {
                path: c.path.clone(),
            });
            continue;
        }
        match github.read_file_live(owner, repo, &c.path, wip).await {
            Ok(content) => ops.push(FileOp::Write {
                path: c.path.clone(),
                content,
            }),
            Err(e) => {
                return Landed::Refused(format!(
                    "`{}` couldn't be read from the WIP branch ({e})",
                    c.path
                ))
            }
        }
    }
    match github
        .batch_write(owner, repo, pr.branch, message, &ops)
        .await
    {
        Ok(_) => {
            let _ = github.delete_branch(owner, repo, wip).await;
            Landed::Pushed { files: ours.len() }
        }
        Err(e) => Landed::Refused(format!("the one commit couldn't be pushed ({e})")),
    }
}

/// The one commit message a round lands with, in the team's commit style.
pub(crate) async fn round_message(
    state: &WorkerState,
    team_id: &str,
    ticket: &crate::models::TicketMessage,
    round: &Round,
) -> String {
    let summary = match round.kind {
        Kind::CiFix => "Fix the failing checks",
        Kind::ReviewFix => "Address the review",
        Kind::Feedback => "Address the review comments",
        Kind::SecurityFix => "Fix the security issues the check found",
        Kind::Conflict => "Merge the base branch and resolve the conflicts",
        Kind::PlannedWork => "Finish the planned work still not done",
    };
    let conventions = super::load_git_conventions(state, team_id, &ticket.ticket_id).await;
    conventions.commit_message(&super::ticket_tokens(ticket), summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rounds_changes_are_read_from_its_wip_compare() {
        let compare = serde_json::json!({"files": [
            {"filename": "src/tabs.tsx", "status": "added"},
            {"filename": "src/old.ts", "status": "removed"},
            {"filename": "src/new.ts", "status": "renamed", "previous_filename": "src/was.ts"},
            {"filename": "src/a.ts", "status": "modified"}
        ]});
        let c = changed_from_compare(&compare);
        assert_eq!(c.len(), 4);
        assert!(c[1].removed);
        assert_eq!(c[2].previous.as_deref(), Some("src/was.ts"));
        // Someone pushed to the PR meanwhile: only the same files block.
        assert!(overlap(&c, &["README.md".to_string()]).is_empty());
        assert_eq!(overlap(&c, &["src/a.ts".to_string()]), vec!["src/a.ts"]);
        assert_eq!(overlap(&c, &["src/was.ts".to_string()]), vec!["src/was.ts"]);
    }

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
