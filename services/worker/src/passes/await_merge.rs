//! Armed auto-merge gate. Merges a PR only once BOTH keys are present (bot
//! APPROVE already established when this was armed + a human approval) AND every
//! CI check is green. It waits (self-scheduling, bounded) for pending checks —
//! e.g. a Terraform staging-apply — and NEVER merges on failing or still-running
//! CI. Bound to the reviewed head: a later commit aborts (the fresh review
//! re-arms). Reuses the two-key + post-merge (tag/health) logic from review_actions.

use crate::clients::github::GitHubClient;
use crate::models::AwaitMergeMessage;
use crate::passes::review_actions::{
    human_approval_present, post_merge_actions, should_merge, OnApproveConfig,
};
use crate::WorkerState;
use tracing::{info, warn};

/// Bounded wait: 40 × 90s ≈ 60 min — enough for a slow staging deploy /
/// Lambda@Edge replication without arming forever.
const MAX_ATTEMPTS: u32 = 40;
const RETRY_DELAY_SECS: i32 = 90;
const SETTLE_DELAY_SECS: i32 = 45;

enum Ci {
    Green,
    Pending,
    Failing(String),
}

/// Combine GitHub Actions check-runs + legacy commit statuses into one verdict.
/// No checks at all ⇒ Green (nothing to gate on).
async fn ci_state(github: &GitHubClient, owner: &str, repo: &str, sha: &str) -> Ci {
    let mut pending = false;
    let mut failing: Vec<String> = vec![];

    if let Ok(v) = github.list_check_runs_for_ref(owner, repo, sha).await {
        for r in v["check_runs"].as_array().cloned().unwrap_or_default() {
            if r["status"].as_str() != Some("completed") {
                pending = true;
                continue;
            }
            match r["conclusion"].as_str().unwrap_or("") {
                "failure" | "timed_out" | "cancelled" | "startup_failure" | "action_required" => {
                    failing.push(r["name"].as_str().unwrap_or("check").to_string())
                }
                _ => {}
            }
        }
    }
    // Legacy commit statuses (external CI that posts a status, e.g. staging apply).
    // Only meaningful when there's at least one status — an empty set reports
    // state "pending", which would otherwise wedge repos that use only check-runs.
    if let Ok(v) = github.get_commit_status(owner, repo, sha).await {
        let statuses = v["statuses"].as_array().cloned().unwrap_or_default();
        if !statuses.is_empty() {
            match v["state"].as_str().unwrap_or("") {
                "failure" | "error" => failing.push("commit status".to_string()),
                "pending" => pending = true,
                _ => {}
            }
        }
    }

    if !failing.is_empty() {
        Ci::Failing(failing.join(", "))
    } else if pending {
        Ci::Pending
    } else {
        Ci::Green
    }
}

pub async fn run(
    state: &WorkerState,
    msg: AwaitMergeMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;
    let cfg = OnApproveConfig::load(state, &msg.team_id, owner, repo).await;
    if !cfg.auto_merge {
        info!(
            pr = msg.pr_number,
            "auto_merge disabled — dropping await-merge"
        );
        return Ok(());
    }
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    let pr = github.get_pull_request(owner, repo, msg.pr_number).await?;
    if pr["state"].as_str() != Some("open") {
        info!(pr = msg.pr_number, "PR not open — stop await-merge");
        return Ok(());
    }
    // Bound to the reviewed head: a new commit invalidates the review, so abort
    // and let the fresh review re-arm.
    let cur_head = pr["head"]["sha"].as_str().unwrap_or("");
    if cur_head != msg.head_sha {
        info!(
            pr = msg.pr_number,
            "head moved — abort await-merge (fresh review re-arms)"
        );
        return Ok(());
    }

    // A self-authored (bot) PR forces the human key on regardless of config.
    let require_human = cfg.require_human_approval || msg.self_authored;
    let human_present = if require_human {
        human_approval_present(&github, owner, repo, msg.pr_number).await
    } else {
        false // ignored by should_merge when require_human is false
    };
    // The gate is only ever armed after a bot APPROVE verdict, so verdict is
    // approved by construction; should_merge folds in the two-key rule.
    let gate_ok = should_merge(cfg.auto_merge, require_human, true, human_present);
    let ci = ci_state(&github, owner, repo, &msg.head_sha).await;

    match ci {
        Ci::Failing(names) => {
            let _ = github
                .create_issue_comment(
                    owner,
                    repo,
                    msg.pr_number,
                    &format!(
                        "🔴 **Auto-merge aborted** — CI is failing ({names}). Not merging; push a \
                         fix and it will re-arm."
                    ),
                )
                .await;
            info!(pr = msg.pr_number, "await-merge: CI failing, aborted");
        }
        Ci::Green if gate_ok => {
            match github
                .merge_pull_request(owner, repo, msg.pr_number, &msg.head_sha, &cfg.merge_method)
                .await
            {
                Ok(true) => {
                    let lines = post_merge_actions(
                        state,
                        &github,
                        &cfg,
                        &msg.team_id,
                        msg.installation_id,
                        owner,
                        repo,
                        msg.pr_number,
                        &msg.head_sha,
                        &msg.base_branch,
                    )
                    .await;
                    let _ = github
                        .create_issue_comment(
                            owner,
                            repo,
                            msg.pr_number,
                            &format!("### 🚀 Auto-merged\n\n{}", lines.join("\n")),
                        )
                        .await;
                    info!(pr = msg.pr_number, "Armed auto-merge: merged");
                }
                Ok(false) => {
                    let _ = github
                        .create_issue_comment(
                            owner,
                            repo,
                            msg.pr_number,
                            "⏸️ Auto-merge: GitHub declined the merge (branch protection unmet or a \
                             conflict). Merge manually once resolved.",
                        )
                        .await;
                }
                Err(e) => {
                    warn!(pr = msg.pr_number, error = %e, "Auto-merge failed");
                    let _ = github
                        .create_issue_comment(
                            owner,
                            repo,
                            msg.pr_number,
                            &format!("⚠️ Auto-merge failed: {e}"),
                        )
                        .await;
                }
            }
        }
        // Pending CI or missing human approval → keep waiting (bounded).
        _ => {
            if msg.attempts < MAX_ATTEMPTS {
                send(state, &msg, msg.attempts + 1, RETRY_DELAY_SECS).await;
            } else {
                info!(
                    pr = msg.pr_number,
                    "await-merge: gave up after cap (still waiting on CI/approval)"
                );
            }
        }
    }
    Ok(())
}

/// Arm the gate for a PR (called at review time and on a human approval). Returns
/// false if the queue isn't configured.
#[allow(clippy::too_many_arguments)]
pub async fn arm(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    base_branch: &str,
    self_authored: bool,
) -> bool {
    let msg = AwaitMergeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        head_sha: head_sha.to_string(),
        base_branch: base_branch.to_string(),
        self_authored,
        attempts: 0,
    };
    send(state, &msg, 0, SETTLE_DELAY_SECS).await
}

/// Enqueue an AwaitMerge tick with `attempts` and an SQS delay.
async fn send(state: &WorkerState, msg: &AwaitMergeMessage, attempts: u32, delay: i32) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut next = msg.clone();
    next.attempts = attempts;
    let body = match serde_json::to_value(&next) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), serde_json::json!("await_merge"));
            }
            v.to_string()
        }
        Err(e) => {
            warn!(error = %e, "await-merge: serialize failed");
            return false;
        }
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "await-merge: enqueue failed");
            false
        }
    }
}
