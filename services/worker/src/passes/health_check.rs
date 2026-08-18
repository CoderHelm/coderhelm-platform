//! Async, baseline-aware post-merge health check. Enqueued (with SQS delay) by
//! the on-approve auto-merge; it does NOT block the review. Behavior matches the
//! three requirements:
//!   - no user-set watch window — it self-reschedules (bounded) until the base
//!     branch's deploy checks COMPLETE, so the window is adaptive.
//!   - only-after-deploy-success — it classifies only once checks finish.
//!   - not-a-previous-error — it alerts only on check failures that are NEW vs.
//!     the pre-merge baseline; pre-existing failures are ignored.
//!
//! It reports (a PR comment); it never rolls back a shared branch.

use crate::clients::github::GitHubClient;
use crate::models::HealthCheckMessage;
use crate::WorkerState;
use std::collections::HashSet;
use tracing::{info, warn};

/// Max re-enqueues while checks are still running: 15 × 90s ≈ 22 min, enough for
/// most deploys without watching forever.
const MAX_ATTEMPTS: u32 = 15;
const RETRY_DELAY_SECS: i32 = 90;
/// A green result does NOT end the check — it STARTS the watch window. A deploy
/// that looks fine one minute after checks pass can start failing ten minutes
/// in; 💚 posts only after the base branch stays clean for this long. A new
/// failure at ANY tick alerts immediately.
const WATCH_WINDOW_SECS: i64 = 15 * 60;
/// Hard ceiling on total re-enqueues (adaptive wait + watch window ticks).
const TOTAL_MAX_ATTEMPTS: u32 =
    MAX_ATTEMPTS + (WATCH_WINDOW_SECS as u32 / RETRY_DELAY_SECS as u32) + 2;

/// Names of base-branch check runs currently in a failing conclusion.
pub async fn failing_checks(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<String> {
    let Ok(v) = github.list_check_runs_for_ref(owner, repo, git_ref).await else {
        return vec![];
    };
    v["check_runs"]
        .as_array()
        .map(|runs| {
            runs.iter()
                .filter(|r| {
                    r["status"].as_str() == Some("completed")
                        && matches!(
                            r["conclusion"].as_str().unwrap_or(""),
                            "failure" | "timed_out" | "cancelled" | "startup_failure"
                        )
                })
                .filter_map(|r| r["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

pub async fn run(
    state: &WorkerState,
    msg: HealthCheckMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    let v = github
        .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await?;
    let runs = v["check_runs"].as_array().cloned().unwrap_or_default();

    let any_running = runs
        .iter()
        .any(|r| r["status"].as_str() != Some("completed"));

    // Adaptive wait: re-enqueue (bounded) until checks complete.
    if any_running && !runs.is_empty() && msg.attempts < MAX_ATTEMPTS {
        reenqueue(state, &msg).await;
        return Ok(());
    }

    let baseline: HashSet<&str> = msg.baseline_failed.iter().map(|s| s.as_str()).collect();
    let now_failing: Vec<String> = runs
        .iter()
        .filter(|r| {
            r["status"].as_str() == Some("completed")
                && matches!(
                    r["conclusion"].as_str().unwrap_or(""),
                    "failure" | "timed_out" | "cancelled" | "startup_failure"
                )
        })
        .filter_map(|r| r["name"].as_str().map(String::from))
        .collect();

    // New failures = failing now but NOT failing before the merge.
    let new_failures: Vec<String> = now_failing
        .into_iter()
        .filter(|n| !baseline.contains(n.as_str()))
        .collect();

    let short = &msg.merge_sha[..msg.merge_sha.len().min(7)];
    let body = if runs.is_empty() {
        // Nothing to observe — stay quiet rather than cry wolf.
        info!(
            pr = msg.pr_number,
            "Health check: no checks on base — skipping"
        );
        return Ok(());
    } else if any_running {
        format!(
            "🟡 **Post-merge health check inconclusive** — deploy checks for `{short}` were still \
             running after the watch window. Keep an eye on the deploy."
        )
    } else if !new_failures.is_empty() {
        format!(
            "🔴 **Post-merge health check FAILED** — the merge of `{short}` introduced NEW check \
             failure(s): {}. (Pre-existing failures were ignored.) A human should decide on a \
             rollback.",
            new_failures.join(", ")
        )
    } else {
        // Green — but one look proves nothing. Start (or continue) the watch
        // window and only declare success once the base branch stayed clean for
        // the full window. Any new failure on a later tick hits the 🔴 branch
        // above immediately.
        let now = chrono::Utc::now().timestamp();
        let since = msg.green_since.unwrap_or(now);
        if now - since < WATCH_WINDOW_SECS && msg.attempts < TOTAL_MAX_ATTEMPTS {
            let mut next = msg.clone();
            next.green_since = Some(since);
            info!(
                pr = msg.pr_number,
                watched_secs = now - since,
                "Health check green — continuing watch window"
            );
            reenqueue(state, &next).await;
            return Ok(());
        }
        format!(
            "💚 **Post-merge health check passed** — deploy checks for `{short}` stayed green for \
             the full {}-minute watch window (no new failures vs. baseline).",
            WATCH_WINDOW_SECS / 60
        )
    };

    let _ = github
        .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &body)
        .await;
    info!(
        pr = msg.pr_number,
        new_failures = new_failures.len(),
        "Health check reported"
    );
    Ok(())
}

/// Schedule the first health check after a merge (SQS-delayed). Captures nothing
/// itself — the caller passes the pre-merge `baseline_failed`. Returns false if
/// the queue isn't configured.
#[allow(clippy::too_many_arguments)]
pub async fn schedule(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    base_branch: &str,
    merge_sha: &str,
    baseline_failed: Vec<String>,
) -> bool {
    let msg = HealthCheckMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        base_branch: base_branch.to_string(),
        merge_sha: merge_sha.to_string(),
        baseline_failed,
        attempts: 0,
        green_since: None,
    };
    reenqueue(state, &msg).await
}

/// Self-reschedule with an SQS delay (adaptive wait for the deploy to finish).
/// `msg.attempts` is bumped before send. Returns true on a successful enqueue.
async fn reenqueue(state: &WorkerState, msg: &HealthCheckMessage) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut next = msg.clone();
    next.attempts += 1;
    let body = match serde_json::to_value(&next) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), serde_json::json!("health_check"));
            }
            v.to_string()
        }
        Err(e) => {
            warn!(error = %e, "Health check: failed to serialize re-enqueue");
            return false;
        }
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(RETRY_DELAY_SECS)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Health check: re-enqueue failed");
            false
        }
    }
}
