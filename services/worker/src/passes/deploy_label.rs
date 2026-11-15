//! The repository's deploy label on CoderHelm's own pull request, read and
//! kept for the ready decision and the description (`common::deploy_label`
//! has the rules).
//!
//! - [`runs_on_head`] finds the workflows the deploy label starts (their
//!   jobs ask for it, read from the workflow files with the cache the
//!   skipped-check reading keeps) and their latest run on the head.
//! - [`before_ready`] puts a deploy label that came off back on a new head,
//!   once per head, and says whether the pull request waits for its
//!   workflow before it leaves draft.

use common::deploy_label::{self, DeployRun, Standing};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The repository's deploy labels; empty when it has none, or the settings
/// can't be read.
pub async fn labels(state: &WorkerState, team_id: &str, owner: &str, repo: &str) -> Vec<String> {
    let cfg = super::review_actions::OnApproveConfig::load(state, team_id, owner, repo).await;
    deploy_label::configured(&cfg.deploy_label)
}

/// The latest run on `head` of each workflow `labels` start. Empty when there
/// are no labels, or the runs can't be read.
#[allow(clippy::too_many_arguments)]
pub async fn runs_on_head(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    head: &str,
    head_ref: &str,
    labels: &[String],
) -> Vec<DeployRun> {
    if labels.is_empty() || head.is_empty() {
        return Vec::new();
    }
    let runs = match gh.list_workflow_runs_for_sha(owner, repo, head).await {
        Ok(r) => r,
        Err(e) => {
            warn!(owner, repo, head, error = %e, "Could not read the head's workflow runs");
            return Vec::new();
        }
    };
    let mut paths: Vec<String> = Vec::new();
    for r in &runs {
        if !matches!(
            r["event"].as_str(),
            Some("pull_request" | "pull_request_target")
        ) {
            continue;
        }
        let p = r["path"].as_str().unwrap_or("");
        if !p.is_empty() && !paths.iter().any(|x| x == p) {
            paths.push(p.to_string());
        }
    }
    let gates =
        super::skipped_checks::gates_of_paths(state, gh, team_id, (owner, repo), head_ref, &paths)
            .await;
    let deploy_paths: Vec<String> = gates
        .into_iter()
        .filter(|(_, g)| g.as_ref().is_some_and(|g| deploy_label::starts(g, labels)))
        .map(|(p, _)| p)
        .collect();
    deploy_label::latest_per_workflow(&runs, &deploy_paths)
}

/// Records that CoderHelm put the deploy labels on the pull request for
/// `head`, so no path adds them again on that head.
pub async fn record_added(
    state: &WorkerState,
    team_id: &str,
    (owner, repo): (&str, &str),
    number: u64,
    head: &str,
    now: i64,
) -> bool {
    match common::merge_gate::record_labels_added(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        owner,
        repo,
        number,
        head,
        now,
    )
    .await
    {
        Ok(()) => true,
        Err(e) => {
            warn!(pr = number, error = %e, "Could not record the deploy labels added for this head");
            false
        }
    }
}

/// Before a draft leaves draft: puts back a deploy label its workflow took
/// off, once per head, and says where the deploy label's workflows stand on
/// the head. A failed read never adds a label.
pub async fn before_ready(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    number: u64,
    pr: &serde_json::Value,
) -> Standing {
    let want = labels(state, team_id, owner, repo).await;
    if want.is_empty() {
        return Standing::None;
    }
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    let head_ref = pr["head"]["ref"].as_str().unwrap_or("");
    let runs = runs_on_head(state, gh, team_id, (owner, repo), head, head_ref, &want).await;
    let now = chrono::Utc::now().timestamp();
    let added = match common::merge_gate::labels_added(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        owner,
        repo,
        number,
    )
    .await
    {
        Ok(a) => a.unwrap_or_default(),
        Err(e) => {
            warn!(pr = number, error = %e, "Could not read when the deploy labels were added");
            return deploy_label::standing(&want, &runs, None, now);
        }
    };
    let mut added_at = (added.0 == head).then_some(added.1);
    let on_pr = super::skipped_checks::labels_of(pr);
    let add = deploy_label::to_add(&want, &on_pr, &added.0, head, &runs);
    // Recorded first: whatever happens next, this head never gets them twice.
    if !add.is_empty() && record_added(state, team_id, (owner, repo), number, head, now).await {
        match gh.add_labels(owner, repo, number, &add).await {
            Ok(_) => {
                info!(pr = number, head, labels = ?add, "Put the deploy labels back on the new head");
                added_at = Some(now);
            }
            Err(e) => {
                warn!(pr = number, head, labels = ?add, error = %e, "Could not put the deploy labels back on the new head");
            }
        }
    }
    deploy_label::standing(&want, &runs, added_at, now)
}
