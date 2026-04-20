use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;
use tracing::{error, info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{ResumeMessage, TicketMessage, TicketSource, TokenUsage};
use crate::passes::{
    attr_n, attr_s, implement, plan, review, save_checkpoint, write_pass_trace, FileCache,
};
use crate::WorkerState;

const MAX_CI_FIX_CYCLES: usize = 10;
const MAX_LOG_CHARS: usize = 15_000;

#[derive(Debug)]
struct RunEvent {
    sk: String,
    event_type: String,
    payload: Value,
}

/// Resume a run after receiving a webhook event (CI pass/fail, PR comment, etc).
pub async fn run(
    state: &WorkerState,
    msg: ResumeMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = std::time::Instant::now();

    // Load run record
    info!(run_id = %msg.run_id, table = %state.config.runs_table_name, "Loading run record");
    let run_record = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .send()
        .await
    {
        Ok(resp) => resp.item.ok_or("Run record not found")?,
        Err(e) => {
            error!(run_id = %msg.run_id, "Failed to load run record: {e:?}");
            return Err(format!("{e:?}").into());
        }
    };

    let status = run_record
        .get("status")
        .and_then(|v| v.as_s().ok())
        .unwrap_or(&String::new())
        .clone();

    // Only resume runs that are awaiting_ci
    if status != "awaiting_ci" {
        if status == "running" {
            // Run is still being processed by another invocation.
            // Return error so SQS retries after visibility timeout.
            warn!(
                run_id = msg.run_id,
                status,
                "Run is currently running — will retry after visibility timeout"
            );
            return Err("Run is running, retry later".into());
        }
        info!(
            run_id = msg.run_id,
            status,
            "Run not in awaiting_ci status, skipping resume"
        );
        return Ok(());
    }

    // Load all unprocessed events
    info!(run_id = %msg.run_id, events_table = %state.config.events_table_name, "Loading events");
    let events = match load_unprocessed_events(state, &msg.run_id).await {
        Ok(evts) => evts,
        Err(e) => {
            error!(run_id = %msg.run_id, "Failed to load events: {e:?}");
            return Err(e);
        }
    };
    if events.is_empty() {
        // No events — webhook may have been missed or repo has no CI.
        // Check GitHub PR check status directly.
        info!(run_id = msg.run_id, "No unprocessed events — checking GitHub PR status");

        let repo_owner = run_record
            .get("repo")
            .and_then(|v| v.as_s().ok())
            .and_then(|r| r.split('/').next())
            .unwrap_or("")
            .to_string();
        let repo_name = run_record
            .get("repo")
            .and_then(|v| v.as_s().ok())
            .and_then(|r| r.split('/').nth(1))
            .unwrap_or("")
            .to_string();
        let branch = run_record
            .get("branch")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let pr_number = run_record
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);

        if repo_owner.is_empty() || repo_name.is_empty() || branch.is_empty() {
            info!(run_id = msg.run_id, "Missing repo/branch info, can't check CI");
            return Ok(());
        }

        let github = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        )?;

        let checks = github
            .list_check_runs_for_ref(&repo_owner, &repo_name, &branch)
            .await
            .unwrap_or(serde_json::json!({"total_count": 0, "check_runs": []}));

        let check_runs: Option<&Vec<Value>> = checks["check_runs"].as_array();
        let total = checks["total_count"].as_u64().unwrap_or(0);

        if total == 0 {
            // No CI workflows at all — mark PR ready and complete
            info!(run_id = msg.run_id, "No CI checks found — marking PR ready and completing");
            mark_pr_ready(&github, &repo_owner, &repo_name, pr_number).await;
            set_run_complete(state, &msg.team_id, &msg.run_id).await;
            return Ok(());
        }

        let any_in_progress = check_runs
            .map(|runs: &Vec<Value>| {
                runs.iter().any(|r| {
                    let status = r["status"].as_str().unwrap_or("");
                    status == "in_progress" || status == "queued"
                })
            })
            .unwrap_or(false);

        if any_in_progress {
            // CI still running — send another delayed resume to check again later
            info!(run_id = msg.run_id, "CI still in progress — scheduling re-check");
            send_delayed_resume(state, &msg, 120).await;
            return Ok(());
        }

        let any_failed = check_runs
            .map(|runs: &Vec<Value>| {
                runs.iter().any(|r| {
                    let conclusion = r["conclusion"].as_str().unwrap_or("");
                    conclusion == "failure" || conclusion == "timed_out"
                })
            })
            .unwrap_or(false);

        if any_failed {
            // CI failed but webhook was missed — write a synthetic ci_failed event and re-trigger
            info!(run_id = msg.run_id, "CI failed (missed webhook) — writing event and re-triggering");
            let now = chrono::Utc::now();
            let event_sk = format!("EVENT#{}#ci_failed", now.format("%Y%m%dT%H%M%S%.3fZ"));
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.events_table_name)
                .item("pk", attr_s(&format!("RUN#{}", msg.run_id)))
                .item("sk", attr_s(&event_sk))
                .item("event_type", attr_s("ci_failed"))
                .item("payload", attr_s("{}"))
                .item("processed", AttributeValue::Bool(false))
                .item("created_at", attr_s(&now.to_rfc3339()))
                .send()
                .await;
            // Re-send resume immediately to process the new event
            send_delayed_resume(state, &msg, 0).await;
            return Ok(());
        }

        // All checks passed but webhook was missed — mark ready and complete
        info!(run_id = msg.run_id, "CI passed (missed webhook) — marking PR ready and completing");
        mark_pr_ready(&github, &repo_owner, &repo_name, pr_number).await;
        set_run_complete(state, &msg.team_id, &msg.run_id).await;
        return Ok(());
    }

    // Determine what happened
    let has_ci_pass = events.iter().any(|e| e.event_type == "ci_passed");
    let has_ci_fail = events.iter().any(|e| e.event_type == "ci_failed");
    let has_review_feedback = events
        .iter()
        .any(|e| e.event_type == "pr_review" || e.event_type == "pr_comment");

    info!(
        run_id = msg.run_id,
        event_count = events.len(),
        has_ci_pass,
        has_ci_fail,
        has_review_feedback,
        "Processing resume events"
    );

    // Extract run context from record
    let branch = run_record
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .ok_or("Missing branch on run record")?
        .clone();
    let repo_owner = run_record
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    let repo_name = run_record
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .and_then(|r| r.split('/').nth(1))
        .unwrap_or("")
        .to_string();
    let pr_number = run_record
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let pr_url = run_record
        .get("pr_url")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let ticket_id = run_record
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let title = run_record
        .get("title")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let issue_number = run_record
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let base_branch = run_record
        .get("base_branch")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "main".to_string());

    // Atomically claim this run: transition awaiting_ci → running.
    // If another Resume Lambda already claimed it, this fails and we exit.
    let claim_result = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression(
            "SET #s = :new_s, status_run_id = :sri, current_pass = :cp, updated_at = :t",
        )
        .condition_expression("#s = :expected")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":expected", attr_s("awaiting_ci"))
        .expression_attribute_values(":new_s", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{}", msg.run_id)))
        .expression_attribute_values(":cp", attr_s("test"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;

    if claim_result.is_err() {
        info!(
            run_id = msg.run_id,
            "Could not claim run (already picked up by another Resume), skipping"
        );
        return Ok(());
    }

    // Build a minimal TicketMessage for the implement/review passes
    let ticket_msg = TicketMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        source: TicketSource::Github,
        ticket_id,
        title: title.clone(),
        body: String::new(),
        repo_owner: repo_owner.clone(),
        repo_name: repo_name.clone(),
        issue_number,
        sender: String::new(),
        base_branch,
        image_attachments: vec![],
    };

    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    let mut usage = load_usage_from_checkpoint(state, &msg.team_id, &msg.run_id).await;

    if has_ci_fail && !has_ci_pass {
        // CI failed — try to fix
        info!(run_id = msg.run_id, "CI failed — attempting fix");

        // Collect failure logs from events
        let failure_logs = collect_ci_failure_info(&events);

        // Download actual workflow logs via GitHub API
        let workflow_run_id = events
            .iter()
            .filter(|e| e.event_type == "ci_failed")
            .filter_map(|e| e.payload["workflow_run_id"].as_u64())
            .last()
            .unwrap_or(0);

        let logs = if workflow_run_id > 0 {
            match github
                .get_workflow_run_logs(&repo_owner, &repo_name, workflow_run_id)
                .await
            {
                Ok(l) => {
                    if l.len() > MAX_LOG_CHARS {
                        format!("... (truncated)\n{}", &l[l.len() - MAX_LOG_CHARS..])
                    } else {
                        l
                    }
                }
                Err(e) => {
                    warn!("Failed to download workflow logs: {e}");
                    failure_logs.clone()
                }
            }
        } else {
            failure_logs.clone()
        };

        // Load checkpoint for review cycle count
        let cycle = load_cycle_from_checkpoint(state, &msg.team_id, &msg.run_id).await;
        if cycle >= MAX_CI_FIX_CYCLES {
            warn!(
                run_id = msg.run_id,
                cycle, "Max CI fix cycles reached, completing with failure"
            );
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                start.elapsed().as_secs(),
            )
            .await?;

            // Still mark PR ready — let human review
            mark_pr_ready(&github, &repo_owner, &repo_name, pr_number).await;
            return Ok(());
        }

        // Implement fix
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        super::update_pass(state, &msg.team_id, &msg.run_id, "implement").await?;

        let plan_result = plan::PlanResult {
            proposal: String::new(),
            tasks: String::new(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
        };
        let file_cache = FileCache::default();

        let feedback = format!(
            "CI workflow failed on PR #{pr_number} (branch: {branch}). Fix the failures.\n\n\
             Rules:\n\
             - Only fix what CI is complaining about. Don't refactor or add features.\n\
             - If the failure is in a test, fix the code (not the test) unless the test itself is wrong.\n\
             - If tests need updating because the feature changed behavior intentionally, update the tests.\n\
             - You may create new test files or edit existing ones if needed.\n\n\
             Failure summary:\n{failure_logs}\n\n\
             Full logs:\n{logs}"
        );

        match implement::run(
            state,
            &ticket_msg,
            &github,
            &plan_result,
            &branch,
            &[],
            "",
            Some(&feedback),
            "medium",
            &provider,
            &mut usage,
            &file_cache,
            Some(&msg.run_id),
        )
        .await
        {
            Ok(_) => {
                info!(run_id = msg.run_id, "CI fix implemented, pushing");
                write_pass_trace(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &format!("ci_fix:{}", cycle + 1),
                    pass_start,
                    &usage_before,
                    &usage,
                    None,
                )
                .await;
            }
            Err(e) => {
                error!(run_id = msg.run_id, error = %e, "CI fix implementation failed");
                set_run_status(state, &msg.team_id, &msg.run_id, "failed", "implement")
                    .await;
                return Err(e);
            }
        }

        // Save checkpoint with incremented cycle, go back to awaiting_ci
        save_checkpoint(
            state,
            &msg.team_id,
            &msg.run_id,
            "pr",
            &branch,
            (cycle + 1) as u8,
            &usage,
        )
        .await;

        set_run_awaiting_ci(
            state,
            &ticket_msg,
            &msg.run_id,
            &pr_url,
            pr_number,
            &branch,
            &usage,
            start.elapsed().as_secs(),
        )
        .await?;

        // Schedule a safety-net resume in case the CI webhook is missed
        send_delayed_resume(state, &msg, 120).await;

        info!(
            run_id = msg.run_id,
            "Fix pushed, back to awaiting_ci for next CI run"
        );
    } else if has_ci_pass {
        // CI passed — run review (include any PR review/comment feedback)
        let review_feedback = collect_review_feedback(&events);
        let repo_instructions = if review_feedback.is_empty() {
            String::new()
        } else {
            format!(
                "## Human Review Feedback\nThe following feedback was left on the PR. Address these comments:\n\n{}",
                review_feedback
            )
        };
        info!(
            run_id = msg.run_id,
            feedback_len = review_feedback.len(),
            "CI passed — running review"
        );
        super::update_pass(state, &msg.team_id, &msg.run_id, "review").await?;

        let plan_result = plan::PlanResult {
            proposal: String::new(),
            tasks: String::new(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
        };
        let file_cache = FileCache::default();

        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let review_result = match review::run(
            state,
            &ticket_msg,
            &github,
            &plan_result,
            &branch,
            &[],
            &repo_instructions,
            &provider,
            &mut usage,
            &file_cache,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(run_id = msg.run_id, error = %e, "Review pass errored, treating as pass");
                review::ReviewResult {
                    passed: true,
                    summary: format!("Review error: {e}"),
                }
            }
        };

        write_pass_trace(
            state,
            &msg.team_id,
            &msg.run_id,
            "review:resume",
            pass_start,
            &usage_before,
            &usage,
            None,
        )
        .await;

        if review_result.passed {
            info!(run_id = msg.run_id, "Review passed — completing run");
            mark_pr_ready(&github, &repo_owner, &repo_name, pr_number).await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                start.elapsed().as_secs(),
            )
            .await?;
        } else {
            // Review found issues — implement fix and go back to awaiting_ci
            info!(
                run_id = msg.run_id,
                "Review found issues — implementing fix"
            );
            super::update_pass(state, &msg.team_id, &msg.run_id, "implement").await?;
            let pass_start = std::time::Instant::now();
            let usage_before = usage.clone();

            match implement::run(
                state,
                &ticket_msg,
                &github,
                &plan_result,
                &branch,
                &[],
                "",
                Some(&review_result.summary),
                "medium",
                &provider,
                &mut usage,
                &file_cache,
                Some(&msg.run_id),
            )
            .await
            {
                Ok(_) => {
                    write_pass_trace(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        "implement:review_fix",
                        pass_start,
                        &usage_before,
                        &usage,
                        None,
                    )
                    .await;
                }
                Err(e) => {
                    error!(run_id = msg.run_id, error = %e, "Review fix implementation failed");
                    // Still mark PR ready, let human review
                    mark_pr_ready(&github, &repo_owner, &repo_name, pr_number).await;
                    complete_run_with_status(
                        state,
                        &ticket_msg,
                        &msg.run_id,
                        "completed",
                        &pr_url,
                        pr_number,
                        &branch,
                        &usage,
                        start.elapsed().as_secs(),
                    )
                    .await?;
                    return Ok(());
                }
            }

            // Back to awaiting_ci for the next CI run
            save_checkpoint(state, &msg.team_id, &msg.run_id, "pr", &branch, 0, &usage)
                .await;
            set_run_awaiting_ci(
                state,
                &ticket_msg,
                &msg.run_id,
                &pr_url,
                pr_number,
                &branch,
                &usage,
                start.elapsed().as_secs(),
            )
            .await?;

            // Schedule a safety-net resume in case the CI webhook is missed
            send_delayed_resume(state, &msg, 120).await;
        }
    }

    // Mark events processed only after all work completes successfully
    mark_events_processed(state, &msg.run_id, &events).await;

    Ok(())
}

async fn load_unprocessed_events(
    state: &WorkerState,
    run_id: &str,
) -> Result<Vec<RunEvent>, Box<dyn std::error::Error + Send + Sync>> {
    if state.config.events_table_name.is_empty() {
        return Ok(vec![]);
    }
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.events_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .filter_expression("#p = :false")
        .expression_attribute_names("#p", "processed")
        .expression_attribute_values(":pk", attr_s(&format!("RUN#{run_id}")))
        .expression_attribute_values(":prefix", attr_s("EVENT#"))
        .expression_attribute_values(":false", AttributeValue::Bool(false))
        .send()
        .await?;

    let events = result
        .items()
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?.clone();
            let event_type = item.get("event_type")?.as_s().ok()?.clone();
            let payload_str = item
                .get("payload")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "{}".to_string());
            let payload: Value = serde_json::from_str(&payload_str).unwrap_or_default();
            Some(RunEvent {
                sk,
                event_type,
                payload,
            })
        })
        .collect();

    Ok(events)
}

async fn mark_events_processed(state: &WorkerState, run_id: &str, events: &[RunEvent]) {
    if state.config.events_table_name.is_empty() {
        return;
    }
    for event in events {
        if let Err(e) = state
            .dynamo
            .update_item()
            .table_name(&state.config.events_table_name)
            .key("pk", attr_s(&format!("RUN#{run_id}")))
            .key("sk", attr_s(&event.sk))
            .update_expression("SET #p = :true")
            .expression_attribute_names("#p", "processed")
            .expression_attribute_values(":true", AttributeValue::Bool(true))
            .send()
            .await
        {
            warn!(error = %e, "Failed to mark event processed: {}", event.sk);
        }
    }
}

fn collect_ci_failure_info(events: &[RunEvent]) -> String {
    events
        .iter()
        .filter(|e| e.event_type == "ci_failed")
        .map(|e| {
            format!(
                "Workflow '{}' failed (run_id: {})",
                e.payload["workflow_name"].as_str().unwrap_or("unknown"),
                e.payload["workflow_run_id"].as_u64().unwrap_or(0),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn collect_review_feedback(events: &[RunEvent]) -> String {
    events
        .iter()
        .filter(|e| e.event_type == "pr_review" || e.event_type == "pr_comment")
        .map(|e| {
            if e.event_type == "pr_review" {
                let reviewer = e.payload["reviewer"].as_str().unwrap_or("unknown");
                let state = e.payload["review_state"].as_str().unwrap_or("");
                let body = e.payload["review_body"].as_str().unwrap_or("");
                format!("@{reviewer} ({state}):\n{body}")
            } else {
                let commenter = e.payload["commenter"].as_str().unwrap_or("unknown");
                let body = e.payload["body"].as_str().unwrap_or("");
                format!("@{commenter}:\n{body}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

async fn load_usage_from_checkpoint(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> TokenUsage {
    if state.config.checkpoints_table_name.is_empty() {
        return TokenUsage::default();
    }
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&format!("RUN#{run_id}")))
        .send()
        .await;

    match result {
        Ok(output) => {
            if let Some(item) = output.item() {
                let get_n = |key: &str| -> u64 {
                    item.get(key)
                        .and_then(|v| v.as_n().ok())
                        .and_then(|n| n.parse().ok())
                        .unwrap_or(0)
                };
                TokenUsage {
                    input_tokens: get_n("tokens_in"),
                    output_tokens: get_n("tokens_out"),
                    cache_read_tokens: get_n("cache_read"),
                    cache_write_tokens: get_n("cache_write"),
                    tool_calls: 0,
                    tool_names: vec![],
                }
            } else {
                TokenUsage::default()
            }
        }
        Err(_) => TokenUsage::default(),
    }
}

async fn load_cycle_from_checkpoint(state: &WorkerState, team_id: &str, run_id: &str) -> usize {
    if state.config.checkpoints_table_name.is_empty() {
        return 0;
    }
    state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&format!("RUN#{run_id}")))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|item| {
            item.get("review_cycle")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}

async fn set_run_status(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    status: &str,
    current_pass: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, current_pass = :cp, updated_at = :t",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(":cp", attr_s(current_pass))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
}

async fn set_run_awaiting_ci(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr_url: &str,
    pr_number: u64,
    branch: &str,
    usage: &TokenUsage,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
             cost_usd = :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("awaiting_ci"))
        .expression_attribute_values(":pr", attr_s(pr_url))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("awaiting_ci"))
        .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
        .send()
        .await?;

    Ok(())
}

async fn mark_pr_ready(
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    pr_number: u64,
) {
    if pr_number == 0 {
        return;
    }
    match github
        .get_pull_request(repo_owner, repo_name, pr_number)
        .await
    {
        Ok(pr) => {
            if let Some(node_id) = pr.get("node_id").and_then(|v| v.as_str()) {
                match github.mark_pr_ready(node_id).await {
                    Ok(_) => info!(pr_number, "PR marked ready for review"),
                    Err(e) => warn!(pr_number, error = %e, "Failed to mark PR ready"),
                }
            }
        }
        Err(e) => warn!(pr_number, error = %e, "Failed to fetch PR for marking ready"),
    }
}

async fn complete_run_with_status(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    status: &str,
    pr_url: &str,
    pr_number: u64,
    branch: &str,
    usage: &TokenUsage,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
             cost_usd = :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":pr", attr_s(pr_url))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .send()
        .await?;

    info!(run_id, status, "Run completed");
    Ok(())
}

/// Send a delayed resume message to the CI fix queue.
/// `delay_seconds` can be 0 (immediate) or up to 900 (15 min, SQS max).
async fn send_delayed_resume(state: &WorkerState, msg: &ResumeMessage, delay_seconds: i32) {
    if state.config.ci_fix_queue_url.is_empty() {
        warn!("CI_FIX_QUEUE_URL not configured — cannot send delayed resume");
        return;
    }
    let body = serde_json::json!({
        "type": "resume",
        "team_id": msg.team_id,
        "run_id": msg.run_id,
        "installation_id": msg.installation_id,
    });
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ci_fix_queue_url)
        .message_body(body.to_string())
        .delay_seconds(delay_seconds)
        .send()
        .await
    {
        Ok(_) => info!(run_id = msg.run_id, delay_seconds, "Delayed resume scheduled"),
        Err(e) => error!(run_id = msg.run_id, error = %e, "Failed to send delayed resume"),
    }
}

/// Mark a run as completed (success) without needing full token usage context.
async fn set_run_complete(state: &WorkerState, team_id: &str, run_id: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #s = :s, updated_at = :t, current_pass = :cp, status_run_id = :sri")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("success"))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("success#{run_id}")))
        .send()
        .await;
    info!(run_id, "Run marked complete (no CI / CI passed)");
}
