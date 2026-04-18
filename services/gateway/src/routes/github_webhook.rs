use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::verify::verify_github_signature;
use crate::models::{
    FeedbackMessage, MarkReadyMessage, OnboardMessage, OnboardRepo,
    PlanTaskContinueMessage, ResumeMessage, TicketMessage, TicketSource, WorkerMessage,
};
use crate::AppState;

/// Look up team_id by GitHub installation_id using the teams table GSI.
/// Returns None if no team has linked this installation yet.
pub async fn resolve_team_by_installation(
    state: &AppState,
    installation_id: u64,
) -> Option<String> {
    let result = match state
        .dynamo
        .query()
        .table_name(&state.config.teams_table_name)
        .index_name("github-installation-index")
        .key_condition_expression("github_installation_id = :iid")
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .limit(10)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(installation_id, error = %e, "Failed to query teams table for installation");
            return None;
        }
    };

    let items = result.items();

    // If multiple teams have this installation, prefer the one with the most users
    // (the real team, not orphan auto-created ones)
    if items.len() > 1 {
        tracing::warn!(
            installation_id,
            team_count = items.len(),
            "Multiple teams found for installation — returning first"
        );
    }

    items
        .first()
        .and_then(|item| item.get("team_id").and_then(|v| v.as_s().ok()).cloned())
}

/// Look up ALL team_ids linked to a GitHub installation_id.
pub async fn resolve_all_teams_by_installation(
    state: &AppState,
    installation_id: u64,
) -> Vec<String> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.teams_table_name)
        .index_name("github-installation-index")
        .key_condition_expression("github_installation_id = :iid")
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .send()
        .await;

    match result {
        Ok(r) => r
            .items()
            .iter()
            .filter_map(|item| item.get("team_id").and_then(|v| v.as_s().ok()).cloned())
            .collect(),
        Err(e) => {
            tracing::error!(installation_id, error = %e, "Failed to query teams for uninstall");
            vec![]
        }
    }
}

pub async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    // Verify signature
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !verify_github_signature(&state.secrets.github_webhook_secret, &body, signature) {
        warn!("Invalid GitHub webhook signature");
        return Err(StatusCode::UNAUTHORIZED);
    }

    let event_type = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let payload: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse webhook body: {e}");
        StatusCode::BAD_REQUEST
    })?;

    let installation_id = payload["installation"]["id"]
        .as_u64()
        .ok_or(StatusCode::BAD_REQUEST)?;

    info!(event_type, installation_id, "GitHub webhook received");

    // Installation events handle their own team resolution/creation
    if event_type == "installation" {
        return handle_installation(&state, &payload, installation_id).await;
    }
    if event_type == "installation_repositories" {
        return handle_installation_repos(&state, &payload, installation_id).await;
    }

    // For all other events, resolve team_id from the installation
    let team_id = match resolve_team_by_installation(&state, installation_id).await {
        Some(tid) => tid,
        None => {
            warn!(
                installation_id,
                event_type, "No team linked to this GitHub installation"
            );
            return Ok(StatusCode::OK);
        }
    };

    match event_type {
        "issues" => handle_issue_event(&state, &payload, installation_id, &team_id).await,
        "issue_comment" => handle_issue_comment(&state, &payload, installation_id, &team_id).await,
        "pull_request" => handle_pull_request(&state, &payload, installation_id, &team_id).await,
        "pull_request_review" => {
            handle_pr_review(&state, &payload, installation_id, &team_id).await
        }
        "pull_request_review_comment" | "pull_request_review_thread" => {
            info!(
                event_type,
                "Review comment event — handled via pull_request_review"
            );
            Ok(StatusCode::OK)
        }
        "check_run" => handle_check_run(&state, &payload, installation_id, &team_id).await,
        "check_suite" => handle_check_suite(&state, &payload, installation_id, &team_id).await,
        "repository" => handle_repository_event(&state, &payload, installation_id, &team_id).await,
        "workflow_run" => handle_workflow_run(&state, &payload, installation_id, &team_id).await,
        // Log-only events — acknowledge but no action needed
        "workflow_dispatch" | "workflow_job" => {
            info!(event_type, "Workflow event received — logged");
            Ok(StatusCode::OK)
        }
        "sub_issues" => {
            info!(event_type, "Sub-issue event received — logged");
            Ok(StatusCode::OK)
        }
        "meta" => {
            info!("GitHub App webhook deleted (meta event)");
            Ok(StatusCode::OK)
        }
        "security_advisory" => {
            info!("Security advisory event received — logged");
            Ok(StatusCode::OK)
        }
        _ => {
            info!(event_type, "Ignoring unhandled event type");
            Ok(StatusCode::OK)
        }
    }
}

async fn handle_issue_event(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    // Trigger on: issue assigned to coderhelm[bot], or labeled "coderhelm"
    let is_assigned_to_bot = action == "assigned"
        && payload["assignee"]["login"]
            .as_str()
            .map(|l| l.contains("coderhelm"))
            .unwrap_or(false);

    let is_labeled = action == "labeled"
        && payload["label"]["name"]
            .as_str()
            .map(|l| l.eq_ignore_ascii_case("coderhelm"))
            .unwrap_or(false);

    if !is_assigned_to_bot && !is_labeled {
        return Ok(StatusCode::OK);
    }
    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Github,
        ticket_id: format!("GH-{}", issue["number"].as_u64().unwrap_or(0)),
        title: issue["title"].as_str().unwrap_or("").to_string(),
        body: issue["body"].as_str().unwrap_or("").to_string(),
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        issue_number: issue["number"].as_u64().unwrap_or(0),
        sender: payload["sender"]["login"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // Dedup: skip if this ticket already has a run
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    let existing = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":ticket", attr_s(&ticket_id_str))
        .limit(5)
        .send()
        .await;

    if let Ok(result) = existing {
        if !result.items().is_empty() {
            info!(ticket_id = %ticket_id_str, "Skipping — ticket already has a run");
            return Ok(StatusCode::OK);
        }
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

async fn handle_issue_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "created" {
        return Ok(StatusCode::OK);
    }

    let body = payload["comment"]["body"].as_str().unwrap_or("");
    let commenter = payload["comment"]["user"]["login"].as_str().unwrap_or("");

    // If this comment is on a PR opened by coderhelm, treat as feedback
    if payload["issue"]["pull_request"].is_object() {
        let pr_user = payload["issue"]["user"]["login"].as_str().unwrap_or("");
        if pr_user.contains("coderhelm") && !commenter.contains("coderhelm") {
            let repo = &payload["repository"];
            let owner = repo["owner"]["login"].as_str().unwrap_or("");
            let name = repo["name"].as_str().unwrap_or("");
            let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);

            let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
            if run_id.is_empty() {
                warn!(pr_number, "No run found for PR comment — skipping");
                return Ok(StatusCode::OK);
            }

            info!(pr_number, commenter, "PR comment → feedback queue");
            let message = WorkerMessage::Feedback(FeedbackMessage {
                team_id: team_id.to_string(),
                installation_id,
                run_id,
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                review_id: 0,
                review_body: body.to_string(),
                comments: vec![],
            });
            return send_to_queue(state, &state.config.feedback_queue_url, &message).await;
        }
    }

    // Trigger on `/coderhelm` slash command or @coderhelm mention (issues or non-bot PRs)
    let is_slash = body.starts_with("/coderhelm");
    let is_mention = body.contains("@coderhelm");
    if !is_slash && !is_mention {
        return Ok(StatusCode::OK);
    }

    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Github,
        ticket_id: format!("GH-{}", issue["number"].as_u64().unwrap_or(0)),
        title: issue["title"].as_str().unwrap_or("").to_string(),
        body: issue["body"].as_str().unwrap_or("").to_string(),
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        issue_number: issue["number"].as_u64().unwrap_or(0),
        sender: payload["sender"]["login"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// Track PR merges for Coderhelm branches — updates run status to "merged".
async fn handle_pull_request(
    state: &AppState,
    payload: &Value,
    _installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    let merged = payload["pull_request"]["merged"].as_bool().unwrap_or(false);

    if action != "closed" || !merged {
        return Ok(StatusCode::OK);
    }

    // Only track our PRs
    let pr_user = payload["pull_request"]["user"]["login"]
        .as_str()
        .unwrap_or("");
    if !pr_user.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);
    if pr_number == 0 {
        return Ok(StatusCode::OK);
    }

    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let team_repo = format!("{team_id}#{owner}/{name}");

    // Query repo-index GSI to find the run with this PR number
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(1)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query run for merged PR: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(item) = result.items().first() {
        let run_id = item
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        if !run_id.is_empty() {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(team_id))
                .key("run_id", attr_s(&run_id))
                .update_expression("SET #status = :s, status_run_id = :sri, updated_at = :t")
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("merged"))
                .expression_attribute_values(":sri", attr_s(&format!("merged#{run_id}")))
                .expression_attribute_values(":t", attr_s(&now))
                .send()
                .await;

            info!(team_id, run_id, pr_number, "PR merged — run status updated");

            // ── Plan task dependency continuation ──
            // If this run's issue belongs to a plan task, check for waiting dependents.
            if let Some(issue_num) = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
            {
                if let Err(e) = trigger_plan_dependents(state, team_id, issue_num).await {
                    warn!(
                        team_id,
                        issue_num,
                        error = %e,
                        "Failed to check plan task dependents"
                    );
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

async fn handle_pr_review(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "submitted" {
        return Ok(StatusCode::OK);
    }

    // Act on "changes_requested" or "commented" reviews (covers both formal
    // change-request reviews and standalone single-line comments / thread replies)
    let review_state = payload["review"]["state"].as_str().unwrap_or("");
    if review_state != "changes_requested" && review_state != "commented" {
        return Ok(StatusCode::OK);
    }

    // Only process reviews on our PRs
    let pr_user = payload["pull_request"]["user"]["login"]
        .as_str()
        .unwrap_or("");
    if !pr_user.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    // Ignore reviews submitted by the bot itself
    let reviewer = payload["review"]["user"]["login"].as_str().unwrap_or("");
    if reviewer.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);

    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(team_id, "PR review skipped — token limit reached");
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
    if run_id.is_empty() {
        warn!(pr_number, "No run found for PR — skipping feedback");
        return Ok(StatusCode::OK);
    }

    // The review body is the top-level comment; individual line comments
    // will be fetched by the worker using the review_id via GitHub API.
    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        review_id: payload["review"]["id"].as_u64().unwrap_or(0),
        review_body: payload["review"]["body"].as_str().unwrap_or("").to_string(),
        comments: vec![],
    });

    send_to_queue(state, &state.config.feedback_queue_url, &message).await
}

async fn handle_check_run(
    _state: &AppState,
    payload: &Value,
    _installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let conclusion = payload["check_run"]["conclusion"].as_str().unwrap_or("");
    if conclusion != "failure" {
        return Ok(StatusCode::OK);
    }

    let branch = payload["check_run"]["check_suite"]["head_branch"]
        .as_str()
        .unwrap_or("");
    if !branch.starts_with("coderhelm/") {
        return Ok(StatusCode::OK);
    }

    // Individual check_run failures are logged but NOT acted on directly.
    // The workflow_run completion event handles the aggregate CI result.
    let check_name = payload["check_run"]["name"].as_str().unwrap_or("unknown");
    info!(
        team_id,
        branch,
        check_name,
        "check_run failed — will be handled by workflow_run completion"
    );

    Ok(StatusCode::OK)
}

async fn handle_installation(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    match action {
        "created" => {
            let org = payload["installation"]["account"]["login"]
                .as_str()
                .unwrap_or("unknown");
            info!(installation_id, org, "GitHub App installed");

            // Check if a team is already linked (reinstall scenario)
            let team_id = if let Some(tid) =
                resolve_team_by_installation(state, installation_id).await
            {
                Some(tid)
            } else {
                // Try to auto-link: look up the installing user's github_id in the users table
                let sender_id = payload["sender"]["id"].as_u64();
                let found_by_github = if let Some(github_id) = sender_id {
                    state
                        .dynamo
                        .query()
                        .table_name(&state.config.users_table_name)
                        .index_name("gsi1")
                        .key_condition_expression("gsi1pk = :gpk")
                        .expression_attribute_values(":gpk", attr_s(&format!("GHUSER#{github_id}")))
                        .limit(1)
                        .send()
                        .await
                        .ok()
                        .and_then(|r| r.items().first().cloned())
                        .and_then(|item| {
                            item.get("pk").and_then(|v| v.as_s().ok()).map(|s| s.to_string())
                        })
                } else {
                    None
                };

                // Fallback: find a team that has repos from this org
                let tid = if let Some(tid) = found_by_github {
                    Some(tid)
                } else {
                    // Scan repos table for any team that has a repo from this org
                    let repo_prefix = format!("REPO#{org}/");
                    state
                        .dynamo
                        .scan()
                        .table_name(&state.config.repos_table_name)
                        .filter_expression("begins_with(sk, :prefix)")
                        .expression_attribute_values(":prefix", attr_s(&repo_prefix))
                        .limit(1)
                        .send()
                        .await
                        .ok()
                        .and_then(|r| r.items().first().cloned())
                        .and_then(|item| {
                            item.get("pk").and_then(|v| v.as_s().ok()).map(|s| s.to_string())
                        })
                };

                if let Some(ref tid) = tid {
                    let now = chrono::Utc::now().to_rfc3339();
                    // Link in teams table
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.teams_table_name)
                        .key("team_id", attr_s(tid))
                        .key("sk", attr_s("META"))
                        .update_expression(
                            "SET github_installation_id = :iid, github_org = :org, updated_at = :now",
                        )
                        .expression_attribute_values(
                            ":iid",
                            aws_sdk_dynamodb::types::AttributeValue::N(
                                installation_id.to_string(),
                            ),
                        )
                        .expression_attribute_values(":org", attr_s(org))
                        .expression_attribute_values(":now", attr_s(&now))
                        .send()
                        .await;

                    // Link in main table
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.table_name)
                        .key("pk", attr_s(tid))
                        .key("sk", attr_s("META"))
                        .update_expression(
                            "SET github_install_id = :iid, github_org = :org, updated_at = :now",
                        )
                        .expression_attribute_values(
                            ":iid",
                            aws_sdk_dynamodb::types::AttributeValue::N(
                                installation_id.to_string(),
                            ),
                        )
                        .expression_attribute_values(":org", attr_s(org))
                        .expression_attribute_values(":now", attr_s(&now))
                        .send()
                        .await;

                    info!(
                        installation_id,
                        org,
                        team_id = tid.as_str(),
                        "Auto-linked GitHub installation via webhook"
                    );
                }
                tid
            };

            // Sync repos if we have a linked team
            if let Some(team_id) = team_id {
                let mut repos = extract_repos_from_installation(payload);
                if repos.is_empty() {
                    repos = fetch_installation_repos(state, installation_id).await;
                }

                let now = chrono::Utc::now().to_rfc3339();
                for repo in &repos {
                    let full = format!("{}/{}", repo.owner, repo.name);
                    let _ = state
                        .dynamo
                        .put_item()
                        .table_name(&state.config.repos_table_name)
                        .item("pk", attr_s(&team_id))
                        .item("sk", attr_s(&format!("REPO#{full}")))
                        .item("repo_name", attr_s(&full))
                        .item(
                            "enabled",
                            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
                        )
                        .item("ticket_source", attr_s("github"))
                        .item("created_at", attr_s(&now))
                        .send()
                        .await;
                }

                if !repos.is_empty() {
                    let onboard = WorkerMessage::Onboard(OnboardMessage {
                        team_id,
                        installation_id,
                        repos,
                    });
                    let _ = send_to_queue(state, &state.config.ticket_queue_url, &onboard).await;
                }
            }

            Ok(StatusCode::CREATED)
        }
        "deleted" => {
            info!(
                installation_id,
                "GitHub App uninstalled — removing installation link"
            );

            // Find ALL teams linked to this installation and remove the link
            let team_ids =
                resolve_all_teams_by_installation(state, installation_id).await;
            let now = chrono::Utc::now().to_rfc3339();
            for team_id in &team_ids {
                // Remove from teams table
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.teams_table_name)
                    .key("team_id", attr_s(team_id))
                    .key("sk", attr_s("META"))
                    .update_expression(
                        "REMOVE github_installation_id, github_org SET updated_at = :t",
                    )
                    .expression_attribute_values(":t", attr_s(&now))
                    .send()
                    .await;

                // Remove from main table
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(team_id))
                    .key("sk", attr_s("META"))
                    .update_expression("REMOVE github_install_id, github_org SET updated_at = :t")
                    .expression_attribute_values(":t", attr_s(&now))
                    .send()
                    .await;

                info!(
                    team_id,
                    "GitHub installation unlinked from team (both tables)"
                );
            }
            if team_ids.is_empty() {
                info!(installation_id, "No teams found for uninstalled installation");
            }

            Ok(StatusCode::OK)
        }
        _ => Ok(StatusCode::OK),
    }
}

async fn handle_installation_repos(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "added" {
        return Ok(StatusCode::OK);
    }

    let team_id = match resolve_team_by_installation(state, installation_id).await {
        Some(tid) => tid,
        None => {
            warn!(
                installation_id,
                "installation_repositories event but no team linked"
            );
            return Ok(StatusCode::OK);
        }
    };

    let repos: Vec<OnboardRepo> = payload["repositories_added"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|r| {
            let full_name = r["full_name"].as_str()?;
            let parts: Vec<&str> = full_name.splitn(2, '/').collect();
            if parts.len() != 2 {
                return None;
            }
            Some(OnboardRepo {
                owner: parts[0].to_string(),
                name: parts[1].to_string(),
                default_branch: r["default_branch"].as_str().unwrap_or("main").to_string(),
            })
        })
        .collect();

    if repos.is_empty() {
        return Ok(StatusCode::OK);
    }

    // Write REPO# items so the dashboard can list them
    let now = chrono::Utc::now().to_rfc3339();
    for repo in &repos {
        let full = format!("{}/{}", repo.owner, repo.name);
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.repos_table_name)
            .item("pk", attr_s(&team_id))
            .item("sk", attr_s(&format!("REPO#{full}")))
            .item("repo_name", attr_s(&full))
            .item(
                "enabled",
                aws_sdk_dynamodb::types::AttributeValue::Bool(false),
            )
            .item("ticket_source", attr_s("github"))
            .item("created_at", attr_s(&now))
            .send()
            .await;
    }

    let onboard = WorkerMessage::Onboard(OnboardMessage {
        team_id,
        installation_id,
        repos,
    });

    send_to_queue(state, &state.config.ticket_queue_url, &onboard).await
}

/// Handle check_suite events — delegated to workflow_run handler.
async fn handle_check_suite(
    _state: &AppState,
    payload: &Value,
    _installation_id: u64,
    _team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let branch = payload["check_suite"]["head_branch"].as_str().unwrap_or("");
    if !branch.starts_with("coderhelm/") {
        return Ok(StatusCode::OK);
    }

    let conclusion = payload["check_suite"]["conclusion"].as_str().unwrap_or("");
    // All CI result handling is done by the workflow_run event handler.
    // check_suite just logs for observability.
    info!(branch, conclusion, "check_suite completed — workflow_run handler will process");
    Ok(StatusCode::OK)
}

/// Handle workflow_run events — write CI events and trigger resume for coderhelm branches.
async fn handle_workflow_run(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let wf = &payload["workflow_run"];
    let branch = wf["head_branch"].as_str().unwrap_or("");
    if !branch.starts_with("coderhelm/") {
        return Ok(StatusCode::OK);
    }

    let conclusion = wf["conclusion"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let repo_owner = repo["owner"]["login"].as_str().unwrap_or("");
    let repo_name = repo["name"].as_str().unwrap_or("");
    let workflow_name = wf["name"].as_str().unwrap_or("unknown");

    // Find the run_id for this branch
    let run_id = match find_run_by_branch(state, team_id, repo_owner, repo_name, branch).await {
        Some(id) => id,
        None => {
            // Fallback: still handle success via MarkReady for backward compat
            if conclusion == "success" {
                let prs = wf["pull_requests"].as_array().cloned().unwrap_or_default();
                for pr in &prs {
                    let pr_number = pr["number"].as_u64().unwrap_or(0);
                    if pr_number == 0 { continue; }
                    info!(branch, pr_number, "CI passed (no run found) — marking PR ready directly");
                    let message = WorkerMessage::MarkReady(MarkReadyMessage {
                        team_id: team_id.to_string(),
                        installation_id,
                        repo_owner: repo_owner.to_string(),
                        repo_name: repo_name.to_string(),
                        pr_number,
                    });
                    let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
                }
            }
            return Ok(StatusCode::OK);
        }
    };

    let now = chrono::Utc::now();
    let event_type = if conclusion == "success" { "ci_passed" } else { "ci_failed" };

    // Write event to events table
    let event_sk = format!(
        "EVENT#{}#{}",
        now.format("%Y%m%dT%H%M%S%.3fZ"),
        event_type,
    );

    let mut logs_url = String::new();
    if let Some(url) = wf.get("logs_url").and_then(|v| v.as_str()) {
        logs_url = url.to_string();
    }

    let event_payload = serde_json::json!({
        "conclusion": conclusion,
        "workflow_name": workflow_name,
        "branch": branch,
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "logs_url": logs_url,
        "workflow_run_id": wf["id"].as_u64().unwrap_or(0),
    });

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("RUN#{run_id}")))
        .item("sk", attr_s(&event_sk))
        .item("event_type", attr_s(event_type))
        .item("payload", attr_s(&event_payload.to_string()))
        .item("processed", aws_sdk_dynamodb::types::AttributeValue::Bool(false))
        .item("created_at", attr_s(&now.to_rfc3339()))
        .item("expires_at", attr_n(now.timestamp() as u64 + 30 * 86400))
        .send()
        .await
    {
        error!("Failed to write CI event: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    info!(
        run_id,
        branch,
        event_type,
        workflow_name,
        "CI event recorded — sending Resume"
    );

    // Send Resume message to kick off the worker
    let message = WorkerMessage::Resume(ResumeMessage {
        team_id: team_id.to_string(),
        run_id: run_id.clone(),
        installation_id,
    });
    let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;

    Ok(StatusCode::OK)
}

/// Look up a run by branch name. Queries repo-index GSI and filters for matching branch
/// with status "awaiting_ci" or "running".
async fn find_run_by_branch(
    state: &AppState,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
    branch: &str,
) -> Option<String> {
    let team_repo = format!("{team_id}#{repo_owner}/{repo_name}");
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("branch = :b AND (#s = :s1 OR #s = :s2)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":s1", attr_s("awaiting_ci"))
        .expression_attribute_values(":s2", attr_s("running"))
        .scan_index_forward(false) // newest first
        .limit(50) // filter is post-query, so fetch enough to find a match
        .send()
        .await
        .ok()?;

    let items = result.items();
    items
        .first()
        .and_then(|item| item.get("run_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
}

/// Handle repository events — track renames, deletions, visibility changes.
async fn handle_repository_event(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let repo_name = repo["full_name"].as_str().unwrap_or("unknown");

    match action {
        "deleted" | "archived" => {
            info!(
                action,
                repo_name, installation_id, "Repository removed — deactivating repo record"
            );
            let parts: Vec<&str> = repo_name.splitn(2, '/').collect();
            if parts.len() == 2 {
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.repos_table_name)
                    .key("pk", attr_s(team_id))
                    .key("sk", attr_s(&format!("REPO#{}", parts[1])))
                    .update_expression("SET #status = :s, updated_at = :t")
                    .expression_attribute_names("#status", "status")
                    .expression_attribute_values(":s", attr_s("inactive"))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
            }
            Ok(StatusCode::OK)
        }
        "renamed" => {
            info!(
                action,
                repo_name, installation_id, "Repository renamed — logged"
            );
            Ok(StatusCode::OK)
        }
        "unarchived" => {
            info!(
                action,
                repo_name, installation_id, "Repository unarchived — logged"
            );
            Ok(StatusCode::OK)
        }
        _ => {
            info!(action, repo_name, "Repository event — no action needed");
            Ok(StatusCode::OK)
        }
    }
}

/// Fetch repos for an installation via GitHub API (used when "All repositories" is selected
/// and the webhook payload doesn't include the repo list).
pub async fn fetch_installation_repos(state: &AppState, installation_id: u64) -> Vec<OnboardRepo> {
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to get installation token for repo fetch: {e}");
            return vec![];
        }
    };

    let mut repos = Vec::new();
    let mut page = 1u32;
    loop {
        let url =
            format!("https://api.github.com/installation/repositories?per_page=100&page={page}");
        let resp = state
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await;

        let body: Value = match resp {
            Ok(r) => match r.error_for_status() {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to parse installation repos response: {e}");
                        break;
                    }
                },
                Err(e) => {
                    error!("GitHub API error fetching installation repos: {e}");
                    break;
                }
            },
            Err(e) => {
                error!("HTTP error fetching installation repos: {e}");
                break;
            }
        };

        let page_repos = body["repositories"].as_array();
        if let Some(arr) = page_repos {
            for r in arr {
                if let Some(full_name) = r["full_name"].as_str() {
                    let parts: Vec<&str> = full_name.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        repos.push(OnboardRepo {
                            owner: parts[0].to_string(),
                            name: parts[1].to_string(),
                            default_branch: r["default_branch"]
                                .as_str()
                                .unwrap_or("main")
                                .to_string(),
                        });
                    }
                }
            }
            // Stop when we got fewer than a full page
            if arr.len() < 100 {
                break;
            }
        } else {
            break;
        }
        page += 1;
    }

    info!(count = repos.len(), "Fetched repos from GitHub API");
    repos
}

fn extract_repos_from_installation(payload: &Value) -> Vec<OnboardRepo> {
    payload["repositories"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|r| {
            let full_name = r["full_name"].as_str()?;
            let parts: Vec<&str> = full_name.splitn(2, '/').collect();
            if parts.len() != 2 {
                return None;
            }
            Some(OnboardRepo {
                owner: parts[0].to_string(),
                name: parts[1].to_string(),
                default_branch: r["default_branch"].as_str().unwrap_or("main").to_string(),
            })
        })
        .collect()
}

async fn send_to_queue(
    state: &AppState,
    queue_url: &str,
    message: &WorkerMessage,
) -> Result<StatusCode, StatusCode> {
    let body = serde_json::to_string(message).map_err(|e| {
        error!("Failed to serialize message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send SQS message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!("Dispatched to SQS");
    Ok(StatusCode::ACCEPTED)
}

/// After a PR merges, check if the merged run's issue belongs to a plan task.
/// If so, find waiting tasks that depend on it and dispatch them to the worker.
async fn trigger_plan_dependents(
    state: &AppState,
    team_id: &str,
    issue_number: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use aws_sdk_dynamodb::types::AttributeValue;

    // Query all items in the plans table for this team (plans + tasks share pk)
    let mut exclusive_start_key = None;
    let mut matched_plan_id = String::new();
    let mut matched_task_id = String::new();

    'outer: loop {
        let mut query = state
            .dynamo
            .query()
            .table_name(&state.config.plans_table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()));

        if let Some(key) = exclusive_start_key.take() {
            query = query.set_exclusive_start_key(Some(key));
        }

        let result = query.send().await?;

        for item in result.items() {
            // Only look at task items (sk contains #TASK#)
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if !sk.contains("#TASK#") {
                continue;
            }

            let item_issue = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            if item_issue == issue_number {
                // Parse plan_id and task_id from sk: PLAN#<plan_id>#TASK#<task_id>
                let parts: Vec<&str> = sk.split('#').collect();
                if parts.len() >= 4 {
                    matched_plan_id = parts[1].to_string();
                    matched_task_id = parts[3].to_string();
                }
                break 'outer;
            }
        }

        match result.last_evaluated_key() {
            Some(key) => exclusive_start_key = Some(key.clone()),
            None => break,
        }
    }

    if matched_plan_id.is_empty() || matched_task_id.is_empty() {
        return Ok(()); // Not a plan task — nothing to do
    }

    info!(
        team_id,
        plan_id = %matched_plan_id,
        task_id = %matched_task_id,
        "Merged PR belongs to plan task — checking for waiting dependents"
    );

    // Query all tasks in this plan to find those waiting on the matched task
    let tasks_result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk_prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(
            ":sk_prefix",
            AttributeValue::S(format!("PLAN#{}#TASK#", matched_plan_id)),
        )
        .send()
        .await?;

    let mut tasks_to_continue: Vec<String> = Vec::new();
    for item in tasks_result.items() {
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");
        let depends_on = item
            .get("depends_on")
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");

        if status == "waiting" && depends_on == matched_task_id {
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                .unwrap_or("");
            let parts: Vec<&str> = sk.split('#').collect();
            if parts.len() >= 4 {
                tasks_to_continue.push(parts[3].to_string());
            }
        }
    }

    if tasks_to_continue.is_empty() {
        return Ok(());
    }

    info!(
        team_id,
        plan_id = %matched_plan_id,
        tasks = tasks_to_continue.len(),
        "Triggering waiting plan tasks"
    );

    let message = WorkerMessage::PlanTaskContinue(PlanTaskContinueMessage {
        team_id: team_id.to_string(),
        plan_id: matched_plan_id,
        tasks: tasks_to_continue,
    });

    let body = serde_json::to_string(&message)?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await?;

    Ok(())
}

/// Look up run_id from the runs table by PR number using the repo-index GSI.
async fn lookup_run_by_pr(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> String {
    let team_repo = format!("{team_id}#{owner}/{name}");
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(1)
        .send()
        .await;

    match result {
        Ok(r) => r
            .items()
            .first()
            .and_then(|item| item.get("run_id").and_then(|v| v.as_s().ok()).cloned())
            .unwrap_or_default(),
        Err(e) => {
            error!("Failed to query run by PR number: {e}");
            String::new()
        }
    }
}

// DynamoDB attribute helpers
fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Check whether this team has budget remaining. Returns Some(reason) if blocked.
pub async fn check_run_budget(state: &AppState, team_id: &str) -> Option<String> {
    // 1. Read configured token limit from settings
    let limit = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("SETTINGS#TOKEN_LIMIT"))
        .send()
        .await
        .ok()?;

    let max_tokens: u64 = limit
        .item()
        .and_then(|i| i.get("max_tokens"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    // 0 = unlimited
    if max_tokens == 0 {
        return None;
    }

    // 2. Read current month's token usage from analytics
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let analytics = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("team_id", attr_s(team_id))
        .key("period", attr_s(&month))
        .send()
        .await
        .ok()?;

    let tokens_in: u64 = analytics
        .item()
        .and_then(|i| i.get("total_tokens_in"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let tokens_out: u64 = analytics
        .item()
        .and_then(|i| i.get("total_tokens_out"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let total_tokens = tokens_in + tokens_out;

    if total_tokens >= max_tokens {
        let limit_label = if max_tokens >= 1_000_000 {
            format!("{}M", max_tokens / 1_000_000)
        } else {
            format!("{}K", max_tokens / 1_000)
        };
        return Some(format!(
            "You've used all **{limit_label}** tokens this month. \
             Increase your token limit in [Settings → Token Limit](https://app.coderhelm.com/settings/budget) to continue.",
        ));
    }

    None
}

/// Post a comment on a GitHub issue explaining why the run was skipped.
async fn post_limit_comment(
    state: &AppState,
    installation_id: u64,
    owner: &str,
    repo: &str,
    issue_number: u64,
    reason: &str,
) {
    if owner.is_empty() || repo.is_empty() || issue_number == 0 {
        return;
    }

    // Get installation access token
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get installation token for limit comment: {e}");
            return;
        }
    };

    let url = format!("https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments");
    let body = serde_json::json!({
        "body": format!("⚠️ **Coderhelm — run skipped**\n\n{reason}")
    });

    if let Err(e) = state
        .http
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(&body)
        .send()
        .await
    {
        warn!("Failed to post limit comment: {e}");
    }
}
