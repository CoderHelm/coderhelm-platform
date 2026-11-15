//! The repository's deploy label on CoderHelm's own pull request, read and
//! kept for the ready decision and the description (`common::deploy_label`
//! has the rules).
//!
//! - [`runs_on_head`] finds the workflows the deploy label starts (their
//!   jobs ask for it, read from the workflow files with the cache the
//!   skipped-check reading keeps) and their latest run on the head.
//! - [`before_ready`] puts a deploy label a workflow took off back on a new
//!   head, once per head, and says whether the pull request waits for its
//!   workflow before it leaves draft.
//! - [`kept_off`] names the deploy labels a person, another app or someone
//!   CoderHelm couldn't identify took off, which CoderHelm leaves off, for the
//!   description.

use common::deploy_label::{self, DeployRun, KeptOff, RemovedBy, Standing};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The repository's deploy labels; empty when it has none, or the settings
/// can't be read.
pub async fn labels(state: &WorkerState, team_id: &str, owner: &str, repo: &str) -> Vec<String> {
    let cfg = super::review_actions::OnApproveConfig::load(state, team_id, owner, repo).await;
    deploy_label::configured(&cfg.deploy_label)
}

/// The latest run on `head` of each workflow `labels` start, and whether
/// the read was complete (the head's runs and every one of their workflow
/// files were read). Empty when there are no labels, or the runs can't be
/// read.
#[allow(clippy::too_many_arguments)]
pub async fn runs_on_head(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    head: &str,
    head_ref: &str,
    labels: &[String],
) -> (Vec<DeployRun>, bool) {
    if labels.is_empty() || head.is_empty() {
        return (Vec::new(), true);
    }
    let runs = match gh.list_workflow_runs_for_sha(owner, repo, head).await {
        Ok(r) => r,
        Err(e) => {
            warn!(owner, repo, head, error = %e, "Could not read the head's workflow runs");
            return (Vec::new(), false);
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
    let complete = gates.iter().all(|(_, g)| g.is_some());
    let deploy_paths: Vec<String> = gates
        .into_iter()
        .filter(|(_, g)| g.as_ref().is_some_and(|g| deploy_label::starts(g, labels)))
        .map(|(p, _)| p)
        .collect();
    let mut found = deploy_label::latest_per_workflow(&runs, &deploy_paths);
    for r in &mut found {
        if r.state() != deploy_label::RunState::Approval || r.id == 0 {
            continue;
        }
        match gh.pending_deployments(owner, repo, r.id).await {
            Ok(d) => {
                r.environment = d
                    .iter()
                    .filter_map(|d| d["environment"]["name"].as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            Err(e) => {
                warn!(owner, repo, run = r.id, error = %e, "Could not read the environment a run waits for");
            }
        }
    }
    (found, complete)
}

/// The deploy label's failed runs on `head` whose jobs ran: failing CI the
/// fix loop takes, even when a later skipped run of the same `labeled`
/// workflow (started by another label) hides its failing jobs from the
/// latest check of each name. Empty when anything can't be read.
pub async fn fixable_failures(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    head: &str,
    head_ref: &str,
) -> Vec<DeployRun> {
    let want = labels(state, team_id, owner, repo).await;
    let (runs, _) = runs_on_head(state, gh, team_id, (owner, repo), head, head_ref, &want).await;
    runs.into_iter().filter(DeployRun::fixable).collect()
}

/// Splits deploy labels to put back by who took them off last: those a
/// workflow (`github-actions[bot]`) or CoderHelm's own app took off, and
/// those a person, another app, or someone CoderHelm can't identify took off.
async fn by_remover(
    gh: &GitHubClient,
    (owner, repo): (&str, &str),
    number: u64,
    labels: Vec<String>,
) -> (Vec<String>, Vec<KeptOff>) {
    if labels.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let events = match gh.list_issue_events(owner, repo, number).await {
        Ok(e) => Some(e),
        Err(e) => {
            warn!(pr = number, error = %e, "Could not read who took the deploy labels off");
            None
        }
    };
    let app = gh.bot_login().await.unwrap_or_default();
    let mut bots = vec!["github-actions[bot]"];
    if !app.is_empty() {
        bots.push(&app);
    }
    let mut add = Vec::new();
    let mut kept = Vec::new();
    for label in labels {
        match deploy_label::removed_by(events.as_ref(), &label, &bots) {
            RemovedBy::Workflow => add.push(label),
            by => kept.push(KeptOff { label, by }),
        }
    }
    (add, kept)
}

/// The deploy labels CoderHelm leaves off this head because a person,
/// another app, or someone CoderHelm can't identify took them off: what the description
/// says. Empty when `complete` is false or anything can't be read.
#[allow(clippy::too_many_arguments)]
pub async fn kept_off(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    number: u64,
    pr: &serde_json::Value,
    want: &[String],
    (runs, complete): (&[DeployRun], bool),
) -> Vec<KeptOff> {
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    if want.is_empty() || !complete {
        return Vec::new();
    }
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
            return Vec::new();
        }
    };
    let on_pr = super::skipped_checks::labels_of(pr);
    let missing = deploy_label::to_add(want, &on_pr, &added.0, head, runs);
    by_remover(gh, (owner, repo), number, missing).await.1
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

/// Before a draft leaves draft: puts back a deploy label a workflow took
/// off, once per head, and says where the deploy label's workflows stand on
/// the head, and whether a label a workflow didn't take off was left off. A failed read never adds a label.
pub async fn before_ready(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    number: u64,
    pr: &serde_json::Value,
) -> (Standing, bool) {
    let want = labels(state, team_id, owner, repo).await;
    if want.is_empty() {
        return (Standing::None, false);
    }
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    let head_ref = pr["head"]["ref"].as_str().unwrap_or("");
    let (runs, complete) =
        runs_on_head(state, gh, team_id, (owner, repo), head, head_ref, &want).await;
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
            return (deploy_label::standing(&want, &runs, None, now), false);
        }
    };
    let mut added_at = (added.0 == head).then_some(added.1);
    let on_pr = super::skipped_checks::labels_of(pr);
    let missing = if complete {
        deploy_label::to_add(&want, &on_pr, &added.0, head, &runs)
    } else {
        Vec::new()
    };
    let (add, kept) = by_remover(gh, (owner, repo), number, missing).await;
    for k in &kept {
        info!(pr = number, head, label = %k.label, by = ?k.by, "Left off a deploy label a workflow didn't take off");
    }
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
    (deploy_label::standing(&want, &runs, added_at, now), !kept.is_empty())
}
