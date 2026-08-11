//! Armed auto-merge gate. Merges a PR only once BOTH keys are present — the
//! bot's OWN latest verdict for this head is APPROVE (verified here, not assumed,
//! since the gateway also arms on any human approval) + a human approval — AND
//! every CI check is green. If the repo configures an (operator-set, never
//! hardcoded) deploy label, CoderHelm's own PRs carry it from PR creation (the PR
//! maker adds it) so the repo's deploy/preview CI runs from the start; human PRs
//! get it added here once approved. Either way the gate waits for that deploy's CI
//! before merging. It waits (self-scheduling, bounded) for pending checks —
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

/// Pure classification of a ref's GitHub check-runs → (any_pending, failing_names).
/// GitHub returns EVERY check-run for a ref, including superseded ones: a re-run or
/// a concurrency-cancel leaves the stale earlier run behind, so one check name can
/// appear several times (e.g. a `cancelled` plus a later `success`). We keep only
/// the LATEST run per name so a stale duplicate can't veto the current result, and
/// `cancelled` is treated as NEUTRAL — GitHub cancels superseded / concurrency-
/// grouped runs and fail-fast matrix siblings as a matter of course, and a genuine
/// failure always surfaces as `failure`/`timed_out` on its own check. Counting
/// `cancelled` as failing was producing false "CI is failing" aborts on PRs whose
/// checks had actually passed.
fn classify_check_runs(runs: &[serde_json::Value]) -> (bool, Vec<String>) {
    use std::collections::HashMap;
    let mut latest: HashMap<&str, &serde_json::Value> = HashMap::new();
    for r in runs {
        let name = r["name"].as_str().unwrap_or("check");
        let ts = r["started_at"].as_str().unwrap_or("");
        let newer = latest
            .get(name)
            .and_then(|e| e["started_at"].as_str())
            .map(|prev| prev <= ts)
            .unwrap_or(true);
        if newer {
            latest.insert(name, r);
        }
    }
    let mut pending = false;
    let mut failing: Vec<String> = vec![];
    for r in latest.values() {
        if r["status"].as_str() != Some("completed") {
            pending = true;
            continue;
        }
        match r["conclusion"].as_str().unwrap_or("") {
            "failure" | "timed_out" | "startup_failure" | "action_required" => {
                failing.push(r["name"].as_str().unwrap_or("check").to_string())
            }
            _ => {}
        }
    }
    (pending, failing)
}

/// Combine GitHub Actions check-runs + legacy commit statuses into one verdict.
/// No checks at all ⇒ Green (nothing to gate on).
async fn ci_state(github: &GitHubClient, owner: &str, repo: &str, sha: &str) -> Ci {
    let mut pending = false;
    let mut failing: Vec<String> = vec![];

    if let Ok(v) = github.list_check_runs_for_ref(owner, repo, sha).await {
        let runs = v["check_runs"].as_array().cloned().unwrap_or_default();
        let (p, f) = classify_check_runs(&runs);
        pending |= p;
        failing.extend(f);
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

    // Wait for CI to FULLY settle before ever declaring a failure: while anything
    // is still running the verdict isn't final, so keep waiting rather than
    // shouting "CI is failing" mid-run (and re-posting it on every re-check). Only
    // once every check is terminal do we decide fail vs green.
    if pending {
        Ci::Pending
    } else if !failing.is_empty() {
        Ci::Failing(failing.join(", "))
    } else {
        Ci::Green
    }
}

/// Claim the right to post the "auto-merge aborted" notice for this exact head,
/// so it is posted at most once. The gate re-arms on many CI events; without this
/// a failing head would collect a fresh abort comment on every re-check. First
/// caller for a (repo, pr, head) wins; later ones skip. Fails OPEN — only a
/// definitive "already posted" (ConditionalCheckFailed) suppresses; a transient
/// DynamoDB error still posts, so a real first notice is never lost.
async fn claim_abort_notice(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    head: &str,
) -> bool {
    let sk = format!("AWAITABORT#{owner}/{repo}#{pr:06}#{head}");
    let ttl = chrono::Utc::now().timestamp() as u64 + 7 * 86_400;
    match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(team_id))
        .item("sk", super::attr_s(&sk))
        .item("ttl", super::attr_n(ttl))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => !e
            .as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false),
    }
}

/// Was CoderHelm's OWN latest verdict for this head an APPROVE? This is the bot
/// key of the two-key rule, read from the persisted review record — uniform
/// across human PRs (where the bot posts a real APPROVE review) and bot-authored
/// PRs (where GitHub forbids self-approve, so the verdict lives only in the
/// record). Records are keyed `REVIEW#{owner}/{repo}#{pr:06}#{rfc3339}`, so a
/// descending scan yields newest-first; we take the newest real verdict for this
/// exact head. Missing/unreadable/other-head ⇒ false (fail-closed: the gate then
/// waits rather than merges).
async fn bot_approved_at_head(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    head_sha: &str,
) -> bool {
    let prefix = format!("REVIEW#{owner}/{repo}#{pr:0>6}#");
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
        .expression_attribute_values(":pk", super::attr_s(team_id))
        .expression_attribute_values(":sk", super::attr_s(&prefix))
        .scan_index_forward(false) // newest review first
        .limit(15)
        .send()
        .await;
    let resp = match out {
        Ok(o) => o,
        Err(e) => {
            warn!(pr, error = %e, "await-merge: could not read review records — bot key treated as absent");
            return false;
        }
    };
    for it in resp.items() {
        let verdict = it
            .get("verdict")
            .and_then(|a| a.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");
        // Only real verdicts count; skip QUESTION (reply-answer) records.
        if verdict != "APPROVE" && verdict != "REQUEST_CHANGES" {
            continue;
        }
        let rec_head = it
            .get("head_sha")
            .and_then(|a| a.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");
        if rec_head == head_sha {
            return verdict == "APPROVE";
        }
    }
    false
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
    // The bot key of the two-key rule: CoderHelm's OWN latest verdict for this
    // head must be APPROVE. The run_on_approve arming path only fires on a bot
    // APPROVE, but the gateway also arms on ANY human approval — so a human
    // approving a PR the bot flagged (REQUEST_CHANGES) would otherwise merge.
    // Verify it explicitly instead of assuming; fail-closed (no/other verdict →
    // not approved → the gate waits, never merges).
    let bot_approved = bot_approved_at_head(
        state,
        &msg.team_id,
        owner,
        repo,
        msg.pr_number,
        &msg.head_sha,
    )
    .await;
    // Gate 1 — approvals. Until BOTH keys are in (bot APPROVE + human approval),
    // just keep waiting: never touch CI or add a deploy label on an unapproved PR.
    if !should_merge(cfg.auto_merge, require_human, bot_approved, human_present) {
        wait_more(state, &msg).await;
        return Ok(());
    }

    // Gate 2 — optional deploy label(s). CoderHelm's OWN PRs already carry these
    // from PR creation (the PR maker adds them alongside the review label), so this
    // is a no-op for them and we fall straight through to the CI check. It still
    // matters for HUMAN-authored PRs: add the operator-configured label(s) now that
    // the PR is cleared to merge so the repo's OWN CI runs (deploy / staging /
    // preview / E2E suites), then wait a tick for that CI to register before we
    // judge it. `deploy_label` is a comma-separated list (e.g. `E2E:IOS,E2E:ANDROID`).
    // Idempotent — only the MISSING labels are added; once all are present we fall
    // through to the CI check. Never merges as a side effect here.
    let want_labels: Vec<String> = cfg
        .deploy_label
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if !want_labels.is_empty() {
        let present: std::collections::HashSet<String> = pr["labels"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|l| l["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let missing: Vec<String> = want_labels
            .iter()
            .filter(|l| !present.contains(*l))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let joined = missing
                .iter()
                .map(|l| format!("`{l}`"))
                .collect::<Vec<_>>()
                .join(", ");
            match github
                .add_labels(owner, repo, msg.pr_number, &missing)
                .await
            {
                Ok(_) => {
                    let _ = github
                        .create_issue_comment(
                            owner,
                            repo,
                            msg.pr_number,
                            &format!(
                                "🏷️ Cleared to merge — added {joined} to kick off CI. I'll merge \
                                 once all of it passes."
                            ),
                        )
                        .await;
                    info!(pr = msg.pr_number, labels = %joined, "await-merge: added deploy label(s)");
                }
                Err(e) => {
                    warn!(pr = msg.pr_number, error = %e, "await-merge: failed to add deploy label(s)");
                    let _ = github
                        .create_issue_comment(
                            owner,
                            repo,
                            msg.pr_number,
                            &format!("⚠️ Couldn't add the deploy label(s) {joined}: {e}"),
                        )
                        .await;
                }
            }
            // Give the label-triggered CI time to register before judging CI, so
            // we don't see the pre-CI checks as green and merge early.
            wait_more(state, &msg).await;
            return Ok(());
        }
    }

    // Gate 3 — all CI green (now includes the deploy CI the label triggered).
    match ci_state(&github, owner, repo, &msg.head_sha).await {
        Ci::Failing(names) => {
            // Post the abort notice at most once per head (the gate re-arms on many
            // events). CI is only reported failing once every check is terminal, so
            // this fires on a genuine, settled failure — not mid-run.
            if claim_abort_notice(
                state,
                &msg.team_id,
                owner,
                repo,
                msg.pr_number,
                &msg.head_sha,
            )
            .await
            {
                let _ = github
                    .create_issue_comment(
                        owner,
                        repo,
                        msg.pr_number,
                        &format!(
                            "🔴 **Auto-merge aborted** — CI is failing ({names}). Not merging; \
                             push a fix and it will re-arm."
                        ),
                    )
                    .await;
            }
            info!(pr = msg.pr_number, "await-merge: CI failing, aborted");
        }
        Ci::Green => {
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
        // CI still running → keep waiting (bounded).
        Ci::Pending => {
            wait_more(state, &msg).await;
        }
    }
    Ok(())
}

/// Re-enqueue the gate for another tick, or give up once the attempt bound is hit.
async fn wait_more(state: &WorkerState, msg: &AwaitMergeMessage) {
    if msg.attempts < MAX_ATTEMPTS {
        send(state, msg, msg.attempts + 1, RETRY_DELAY_SECS).await;
    } else {
        info!(
            pr = msg.pr_number,
            "await-merge: gave up after cap (still waiting on approval/deploy/CI)"
        );
    }
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

#[cfg(test)]
mod ci_tests {
    use super::classify_check_runs;
    use serde_json::json;

    fn cr(name: &str, status: &str, conclusion: &str, started_at: &str) -> serde_json::Value {
        json!({"name": name, "status": status, "conclusion": conclusion, "started_at": started_at})
    }

    #[test]
    fn cancelled_duplicate_does_not_fail() {
        // The incident: a superseded `cancelled` run alongside a later `success`
        // for the same check must NOT be reported as failing.
        let runs = vec![
            cr(
                "Web App Lint",
                "completed",
                "success",
                "2026-08-11T10:00:00Z",
            ),
            cr(
                "Web App Lint",
                "completed",
                "cancelled",
                "2026-08-11T09:00:00Z",
            ),
        ];
        let (pending, failing) = classify_check_runs(&runs);
        assert!(!pending);
        assert!(
            failing.is_empty(),
            "cancelled dup must not fail: {failing:?}"
        );
    }

    #[test]
    fn cancelled_only_is_neutral() {
        let runs = vec![cr(
            "Deploy",
            "completed",
            "cancelled",
            "2026-08-11T10:00:00Z",
        )];
        let (pending, failing) = classify_check_runs(&runs);
        assert!(!pending);
        assert!(failing.is_empty());
    }

    #[test]
    fn real_failure_is_reported() {
        let runs = vec![cr("Lint", "completed", "failure", "2026-08-11T10:00:00Z")];
        let (_, failing) = classify_check_runs(&runs);
        assert_eq!(failing, vec!["Lint".to_string()]);
    }

    #[test]
    fn in_progress_is_pending() {
        let runs = vec![cr(
            "Deploy Preview",
            "in_progress",
            "",
            "2026-08-11T10:00:00Z",
        )];
        let (pending, failing) = classify_check_runs(&runs);
        assert!(pending);
        assert!(failing.is_empty());
    }

    #[test]
    fn latest_run_per_name_wins() {
        // Older success then newer failure for one name => failing (latest wins).
        let runs = vec![
            cr("Lint", "completed", "success", "2026-08-11T09:00:00Z"),
            cr("Lint", "completed", "failure", "2026-08-11T10:00:00Z"),
        ];
        let (_, failing) = classify_check_runs(&runs);
        assert_eq!(failing, vec!["Lint".to_string()]);
    }
}
