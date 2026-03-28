use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::billing::{FREE_TIER_TOKENS, INCLUDED_TOKENS, OVERAGE_PER_1K_TOKENS_CENTS};
use crate::auth::verify::verify_github_signature;
use crate::models::{
    CiFixMessage, FeedbackMessage, MarkReadyMessage, OnboardMessage, OnboardRepo, TicketMessage,
    TicketSource, WorkerMessage,
};
use crate::AppState;

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

    match event_type {
        "issues" => handle_issue_event(&state, &payload, installation_id).await,
        "issue_comment" => handle_issue_comment(&state, &payload, installation_id).await,
        "pull_request_review" => handle_pr_review(&state, &payload, installation_id).await,
        "pull_request_review_comment" | "pull_request_review_thread" => {
            handle_pr_review_comment(&state, &payload, installation_id).await
        }
        "check_run" => handle_check_run(&state, &payload, installation_id).await,
        "check_suite" => handle_check_suite(&state, &payload, installation_id).await,
        "installation" => handle_installation(&state, &payload, installation_id).await,
        "installation_repositories" => {
            handle_installation_repos(&state, &payload, installation_id).await
        }
        "repository" => handle_repository_event(&state, &payload, installation_id).await,
        // Log-only events — acknowledge but no action needed
        "workflow_dispatch" | "workflow_job" | "workflow_run" => {
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
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    // Trigger on: issue assigned to d3ftly[bot], or labeled "d3ftly"
    let is_assigned_to_bot = action == "assigned"
        && payload["assignee"]["login"]
            .as_str()
            .map(|l| l.contains("d3ftly"))
            .unwrap_or(false);

    let is_labeled = action == "labeled"
        && payload["label"]["name"]
            .as_str()
            .map(|l| l == "d3ftly")
            .unwrap_or(false);

    if !is_assigned_to_bot && !is_labeled {
        return Ok(StatusCode::OK);
    }

    let tenant_id = format!("TENANT#{installation_id}");
    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        tenant_id: tenant_id.clone(),
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
    if let Some(reason) = check_run_budget(state, &tenant_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

async fn handle_issue_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "created" {
        return Ok(StatusCode::OK);
    }

    let body = payload["comment"]["body"].as_str().unwrap_or("");

    // Trigger on `/d3ftly` slash command
    if !body.starts_with("/d3ftly") {
        return Ok(StatusCode::OK);
    }

    let tenant_id = format!("TENANT#{installation_id}");
    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        tenant_id: tenant_id.clone(),
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
    if let Some(reason) = check_run_budget(state, &tenant_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

async fn handle_pr_review(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "submitted" {
        return Ok(StatusCode::OK);
    }

    // Only process reviews on our PRs
    let pr_user = payload["pull_request"]["user"]["login"]
        .as_str()
        .unwrap_or("");
    if !pr_user.contains("d3ftly") {
        return Ok(StatusCode::OK);
    }

    let tenant_id = format!("TENANT#{installation_id}");
    let repo = &payload["repository"];

    let message = WorkerMessage::Feedback(FeedbackMessage {
        tenant_id,
        installation_id,
        run_id: String::new(), // TODO: look up from DynamoDB by PR number
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        pr_number: payload["pull_request"]["number"].as_u64().unwrap_or(0),
        review_id: payload["review"]["id"].as_u64().unwrap_or(0),
        review_body: payload["review"]["body"].as_str().unwrap_or("").to_string(),
        comments: vec![], // TODO: fetch review comments via API
    });

    send_to_queue(state, &state.config.feedback_queue_url, &message).await
}

async fn handle_check_run(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let conclusion = payload["check_run"]["conclusion"].as_str().unwrap_or("");
    if conclusion != "failure" {
        return Ok(StatusCode::OK);
    }

    // Only fix CI on our branches
    let branch = payload["check_run"]["check_suite"]["head_branch"]
        .as_str()
        .unwrap_or("");
    if !branch.starts_with("d3ftly/") {
        return Ok(StatusCode::OK);
    }

    let tenant_id = format!("TENANT#{installation_id}");
    let repo = &payload["repository"];

    let message = WorkerMessage::CiFix(CiFixMessage {
        tenant_id,
        installation_id,
        run_id: String::new(), // TODO: look up from DynamoDB by branch
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        branch: branch.to_string(),
        pr_number: 0, // TODO: look up
        check_run_id: payload["check_run"]["id"].as_u64().unwrap_or(0),
        attempt: 1,
    });

    send_to_queue(state, &state.config.ci_fix_queue_url, &message).await
}

async fn handle_installation(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    match action {
        "created" => {
            // New tenant! Provision in DynamoDB.
            let org = payload["installation"]["account"]["login"]
                .as_str()
                .unwrap_or("unknown");
            info!(
                installation_id,
                org, "New GitHub App installation — provisioning tenant"
            );

            let now = chrono::Utc::now().to_rfc3339();
            state
                .dynamo
                .put_item()
                .table_name(&state.config.table_name)
                .item("pk", attr_s(&format!("TENANT#{installation_id}")))
                .item("sk", attr_s("META"))
                .item("github_install_id", attr_n(installation_id))
                .item("github_org", attr_s(org))
                .item("plan", attr_s("free"))
                .item("status", attr_s("active"))
                .item("run_count_mtd", attr_n(0))
                .item("created_at", attr_s(&now))
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to create tenant: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            // Write REPO# items so the dashboard can list them
            let repos = extract_repos_from_installation(payload);
            let tenant_id = format!("TENANT#{installation_id}");
            let now_str = now.clone();
            for repo in &repos {
                let full = format!("{}/{}", repo.owner, repo.name);
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.table_name)
                    .item("pk", attr_s(&tenant_id))
                    .item("sk", attr_s(&format!("REPO#{full}")))
                    .item("repo_name", attr_s(&full))
                    .item(
                        "enabled",
                        aws_sdk_dynamodb::types::AttributeValue::Bool(true),
                    )
                    .item("ticket_source", attr_s("github"))
                    .item("created_at", attr_s(&now_str))
                    .send()
                    .await;
            }

            // Enqueue onboard for all repos in the installation
            if !repos.is_empty() {
                let onboard = WorkerMessage::Onboard(OnboardMessage {
                    tenant_id,
                    installation_id,
                    repos,
                });
                let _ = send_to_queue(state, &state.config.ticket_queue_url, &onboard).await;
            }

            Ok(StatusCode::CREATED)
        }
        "deleted" => {
            info!(
                installation_id,
                "GitHub App uninstalled — deactivating tenant"
            );
            state
                .dynamo
                .update_item()
                .table_name(&state.config.table_name)
                .key("pk", attr_s(&format!("TENANT#{installation_id}")))
                .key("sk", attr_s("META"))
                .update_expression("SET #status = :s")
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("deactivated"))
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to deactivate tenant: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
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
    let tenant_id = format!("TENANT#{installation_id}");
    let now = chrono::Utc::now().to_rfc3339();
    for repo in &repos {
        let full = format!("{}/{}", repo.owner, repo.name);
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.table_name)
            .item("pk", attr_s(&tenant_id))
            .item("sk", attr_s(&format!("REPO#{full}")))
            .item("repo_name", attr_s(&full))
            .item(
                "enabled",
                aws_sdk_dynamodb::types::AttributeValue::Bool(true),
            )
            .item("ticket_source", attr_s("github"))
            .item("created_at", attr_s(&now))
            .send()
            .await;
    }

    let onboard = WorkerMessage::Onboard(OnboardMessage {
        tenant_id,
        installation_id,
        repos,
    });

    send_to_queue(state, &state.config.ticket_queue_url, &onboard).await
}

/// Handle inline PR review comments and review threads.
/// These fire when someone leaves a line-level comment on our PR.
async fn handle_pr_review_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "created" {
        return Ok(StatusCode::OK);
    }

    // Only process comments on our PRs
    let pr_user = payload["pull_request"]["user"]["login"]
        .as_str()
        .unwrap_or("");
    if !pr_user.contains("d3ftly") {
        return Ok(StatusCode::OK);
    }

    let tenant_id = format!("TENANT#{installation_id}");
    let repo = &payload["repository"];
    let comment = &payload["comment"];

    let message = WorkerMessage::Feedback(FeedbackMessage {
        tenant_id,
        installation_id,
        run_id: String::new(),
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        pr_number: payload["pull_request"]["number"].as_u64().unwrap_or(0),
        review_id: comment["pull_request_review_id"].as_u64().unwrap_or(0),
        review_body: comment["body"].as_str().unwrap_or("").to_string(),
        comments: vec![],
    });

    send_to_queue(state, &state.config.feedback_queue_url, &message).await
}

/// Handle check_suite events — mark PR ready on success, log failures.
async fn handle_check_suite(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let branch = payload["check_suite"]["head_branch"].as_str().unwrap_or("");
    if !branch.starts_with("d3ftly/") {
        return Ok(StatusCode::OK);
    }

    let conclusion = payload["check_suite"]["conclusion"].as_str().unwrap_or("");
    let repo = &payload["repository"];

    match conclusion {
        "success" => {
            // All checks passed — find the open draft PR for this branch and mark it ready
            let prs = payload["check_suite"]["pull_requests"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for pr in &prs {
                let pr_number = pr["number"].as_u64().unwrap_or(0);
                if pr_number == 0 {
                    continue;
                }
                info!(branch, pr_number, "CI passed — marking PR ready");
                let message = WorkerMessage::MarkReady(MarkReadyMessage {
                    tenant_id: format!("TENANT#{installation_id}"),
                    installation_id,
                    repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
                    repo_name: repo["name"].as_str().unwrap_or("").to_string(),
                    pr_number,
                });
                let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            }
            Ok(StatusCode::OK)
        }
        "failure" => {
            info!(
                branch,
                "Check suite failed on d3ftly branch — delegating to check_run handler"
            );
            // The individual check_run events will handle CI fixes
            Ok(StatusCode::OK)
        }
        _ => Ok(StatusCode::OK),
    }
}

/// Handle repository events — track renames, deletions, visibility changes.
async fn handle_repository_event(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
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
            let tenant_id = format!("TENANT#{installation_id}");
            let parts: Vec<&str> = repo_name.splitn(2, '/').collect();
            if parts.len() == 2 {
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(&tenant_id))
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

// DynamoDB attribute helpers
fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Check whether this tenant has budget remaining. Returns Some(reason) if blocked.
async fn check_run_budget(state: &AppState, tenant_id: &str) -> Option<String> {
    // 1. Read current month's token usage from analytics
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let analytics = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("tenant_id", attr_s(tenant_id))
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

    // 2. Read billing record (subscription status)
    let billing = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
        .ok()?;

    let sub_status = billing
        .item()
        .and_then(|i| i.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("none");

    let is_pro = sub_status == "active";

    // Free tier: hard limit at 500K tokens
    if !is_pro && total_tokens >= FREE_TIER_TOKENS {
        return Some(format!(
            "You've used all **{}K free tokens** this month. \
             [Upgrade to Pro](https://app.d3ftly.com/billing) for 5M tokens/month.",
            FREE_TIER_TOKENS / 1000,
        ));
    }

    // Pro tier: check budget cap
    if is_pro {
        let budget = state
            .dynamo
            .get_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(tenant_id))
            .key("sk", attr_s("SETTINGS#BUDGET"))
            .send()
            .await
            .ok()?;

        let max_budget_cents = budget
            .item()
            .and_then(|i| i.get("max_budget_cents"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);

        if max_budget_cents > 0 {
            // Calculate current spend: base $199 + token overages
            let overage_tokens = total_tokens.saturating_sub(INCLUDED_TOKENS);
            let overage_1k = overage_tokens / 1000;
            let current_spend = 19900 + overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;
            if current_spend >= max_budget_cents {
                return Some(format!(
                    "Monthly budget cap of **${:.2}** reached (current spend: **${:.2}**). \
                     Adjust your budget in [Settings](https://app.d3ftly.com/settings/budget).",
                    max_budget_cents as f64 / 100.0,
                    current_spend as f64 / 100.0,
                ));
            }
        }
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
        "body": format!("⚠️ **d3ftly — run skipped**\n\n{reason}")
    });

    if let Err(e) = state
        .http
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "d3ftly-bot")
        .json(&body)
        .send()
        .await
    {
        warn!("Failed to post limit comment: {e}");
    }
}
