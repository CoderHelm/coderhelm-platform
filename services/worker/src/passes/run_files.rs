//! The run's `files_modified`: the files its branch changes against its base
//! (the run's real work, without the plan files CoderHelm commits beside
//! it), written wherever the run records its pull request and status, so the
//! run page's Changes tab and Files changed card read what the pull request
//! holds after every round.

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::TicketMessage;
use crate::WorkerState;

/// At most this many paths on the run record: a generated or vendored tree
/// can touch thousands, and the item has a size limit.
pub const MAX_FILES: usize = 500;

/// One pull request's branch and the base its diff is read against.
pub struct Branch<'a> {
    pub owner: &'a str,
    pub repo: &'a str,
    pub base: &'a str,
    pub branch: &'a str,
}

/// The list the run record holds: each branch's work files in order, with
/// the repository named before the path when the run has more than one
/// pull request; no duplicates, at most [`MAX_FILES`].
pub fn files_value(changed: &[(String, Vec<String>)], ticket_id: &str) -> Vec<String> {
    let multi = changed.len() > 1;
    let mut out: Vec<String> = Vec::new();
    for (repo, files) in changed {
        for f in super::run_end::work_files(files, ticket_id) {
            let entry = if multi { format!("{repo}/{f}") } else { f };
            if !out.contains(&entry) {
                out.push(entry);
            }
            if out.len() >= MAX_FILES {
                return out;
            }
        }
    }
    out
}

/// Reads each branch's diff and writes `files_modified` on the run. A diff
/// GitHub can't give leaves the field as it was: a stale list beats an
/// empty one.
pub async fn record(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    run_id: &str,
    ticket_id: &str,
    branches: &[Branch<'_>],
) {
    let mut changed = Vec::new();
    for b in branches {
        if b.branch.is_empty() || b.base.is_empty() {
            return;
        }
        let Some(files) =
            super::plan_coverage::changed_files(github, b.owner, b.repo, b.base, b.branch).await
        else {
            warn!(
                run_id,
                branch = b.branch,
                "Could not read the branch's diff for files_modified"
            );
            return;
        };
        changed.push((format!("{}/{}", b.owner, b.repo), files));
    }
    let files = files_value(&changed, ticket_id);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression("SET files_modified = :fm")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(
            ":fm",
            AttributeValue::L(files.iter().cloned().map(AttributeValue::S).collect()),
        )
        .send()
        .await;
    match res {
        Ok(_) => info!(run_id, files = files.len(), "Recorded files_modified"),
        Err(e) => warn!(run_id, error = %e, "Failed to record files_modified"),
    }
}

/// [`record`] for a run with one pull request in the message's repository.
pub async fn record_for(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
) {
    record(
        state,
        github,
        &msg.team_id,
        run_id,
        &msg.ticket_id,
        &[Branch {
            owner: &msg.repo_owner,
            repo: &msg.repo_name,
            base: &msg.base_branch,
            branch,
        }],
    )
    .await
}

/// [`record_for`] where the caller holds no GitHub client.
pub async fn record_with_app(state: &WorkerState, msg: &TicketMessage, run_id: &str, branch: &str) {
    match GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        Ok(github) => record_for(state, &github, msg, run_id, branch).await,
        Err(e) => warn!(run_id, error = %e, "No GitHub client for files_modified"),
    }
}
