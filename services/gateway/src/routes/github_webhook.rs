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
        "pull_request" => handle_pull_request(&state, &payload, installation_id).await,
        "pull_request_review" => handle_pr_review(&state, &payload, installation_id).await,
        // Review-comment events are handled by handle_pr_review (fetches comments by review_id)
        // to avoid duplicate SQS dispatches when a review is submitted with inline comments.
        "pull_request_review_comment" | "pull_request_review_thread" => {
            info!(
                event_type,
                "Review comment event — handled via pull_request_review"
            );
            Ok(StatusCode::OK)
        }
        "check_run" => handle_check_run(&state, &payload, installation_id).await,
        "check_suite" => handle_check_suite(&state, &payload, installation_id).await,
        "installation" => handle_installation(&state, &payload, installation_id).await,
        "installation_repositories" => {
            handle_installation_repos(&state, &payload, installation_id).await
        }
        "repository" => handle_repository_event(&state, &payload, installation_id).await,
        "workflow_run" => handle_workflow_run(&state, &payload, installation_id).await,
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

    // Dedup: skip if this ticket already has a run
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    let existing = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .filter_expression("ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(&tenant_id))
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
            let tenant_id = format!("TENANT#{installation_id}");
            let repo = &payload["repository"];
            let owner = repo["owner"]["login"].as_str().unwrap_or("");
            let name = repo["name"].as_str().unwrap_or("");
            let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);

            let run_id = lookup_run_by_pr(state, &tenant_id, owner, name, pr_number).await;
            if run_id.is_empty() {
                warn!(pr_number, "No run found for PR comment — skipping");
                return Ok(StatusCode::OK);
            }

            info!(pr_number, commenter, "PR comment → feedback queue");
            let message = WorkerMessage::Feedback(FeedbackMessage {
                tenant_id,
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

/// Track PR merges for Coderhelm branches — updates run status to "merged".
async fn handle_pull_request(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
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

    let tenant_id = format!("TENANT#{installation_id}");
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let tenant_repo = format!("{tenant_id}#{owner}/{name}");

    // Query repo-index GSI to find the run with this PR number
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("tenant_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&tenant_repo))
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
                .key("tenant_id", attr_s(&tenant_id))
                .key("run_id", attr_s(&run_id))
                .update_expression("SET #status = :s, status_run_id = :sri, updated_at = :t")
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("merged"))
                .expression_attribute_values(":sri", attr_s(&format!("merged#{run_id}")))
                .expression_attribute_values(":t", attr_s(&now))
                .send()
                .await;

            info!(
                tenant_id,
                run_id, pr_number, "PR merged — run status updated"
            );
        }
    }

    Ok(StatusCode::OK)
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

    let tenant_id = format!("TENANT#{installation_id}");
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);

    let run_id = lookup_run_by_pr(state, &tenant_id, owner, name, pr_number).await;
    if run_id.is_empty() {
        warn!(pr_number, "No run found for PR — skipping feedback");
        return Ok(StatusCode::OK);
    }

    // The review body is the top-level comment; individual line comments
    // will be fetched by the worker using the review_id via GitHub API.
    let message = WorkerMessage::Feedback(FeedbackMessage {
        tenant_id,
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
    if !branch.starts_with("coderhelm/") {
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
            let org = payload["installation"]["account"]["login"]
                .as_str()
                .unwrap_or("unknown");
            info!(
                installation_id,
                org, "GitHub App installation — provisioning/reactivating tenant"
            );

            let tenant_pk = format!("TENANT#{installation_id}");
            let now = chrono::Utc::now().to_rfc3339();

            // Check if tenant already exists (reinstallation case)
            let existing = state
                .dynamo
                .get_item()
                .table_name(&state.config.table_name)
                .key("pk", attr_s(&tenant_pk))
                .key("sk", attr_s("META"))
                .projection_expression("#s")
                .expression_attribute_names("#s", "status")
                .send()
                .await
                .ok()
                .and_then(|r| r.item);

            if existing.is_some() {
                // Reactivate existing tenant — preserve plan, billing, etc.
                state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(&tenant_pk))
                    .key("sk", attr_s("META"))
                    .update_expression(
                        "SET #status = :s, github_install_id = :iid, github_org = :org, reactivated_at = :t",
                    )
                    .expression_attribute_names("#status", "status")
                    .expression_attribute_values(":s", attr_s("active"))
                    .expression_attribute_values(":iid", attr_n(installation_id))
                    .expression_attribute_values(":org", attr_s(org))
                    .expression_attribute_values(":t", attr_s(&now))
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Failed to reactivate tenant: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;
                info!(installation_id, "Reactivated existing tenant");
            } else {
                // Brand new tenant
                state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.table_name)
                    .item("pk", attr_s(&tenant_pk))
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
            }

            // Write REPO# items so the dashboard can list them
            let mut repos = extract_repos_from_installation(payload);

            // When installed with "All repositories", GitHub sends an empty array.
            // Fetch repos from the API in that case.
            if repos.is_empty() {
                info!(
                    installation_id,
                    "No repos in webhook payload — fetching from GitHub API"
                );
                repos = fetch_installation_repos(state, installation_id).await;
            }

            let now_str = now.clone();
            for repo in &repos {
                let full = format!("{}/{}", repo.owner, repo.name);
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.repos_table_name)
                    .item("pk", attr_s(&tenant_pk))
                    .item("sk", attr_s(&format!("REPO#{full}")))
                    .item("repo_name", attr_s(&full))
                    .item(
                        "enabled",
                        aws_sdk_dynamodb::types::AttributeValue::Bool(false),
                    )
                    .item("ticket_source", attr_s("github"))
                    .item("created_at", attr_s(&now_str))
                    .send()
                    .await;
            }

            // Enqueue onboard for all repos in the installation
            if !repos.is_empty() {
                let onboard = WorkerMessage::Onboard(OnboardMessage {
                    tenant_id: tenant_pk,
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
            let tenant_id = format!("TENANT#{installation_id}");
            state
                .dynamo
                .update_item()
                .table_name(&state.config.table_name)
                .key("pk", attr_s(&tenant_id))
                .key("sk", attr_s("META"))
                .update_expression("SET #status = :s, deactivated_at = :t")
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("deactivated"))
                .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to deactivate tenant: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            // Notify all users in the tenant
            let users = state
                .dynamo
                .query()
                .table_name(&state.config.users_table_name)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", attr_s(&tenant_id))
                .projection_expression("email")
                .send()
                .await
                .ok();

            if let Some(users_result) = users {
                for item in users_result.items() {
                    if let Some(email) = item
                        .get("email")
                        .and_then(|v| v.as_s().ok())
                        .filter(|e| !e.is_empty())
                    {
                        let subject = aws_sdk_sesv2::types::Content::builder()
                            .data("Coderhelm GitHub App uninstalled")
                            .build();
                        let body_text =
                            aws_sdk_sesv2::types::Content::builder()
                                .data("The Coderhelm GitHub App has been uninstalled from your organization. Coderhelm will no longer process any runs or webhooks for this account. To restore access, reinstall the app from https://app.coderhelm.com.")
                                .build();
                        if let (Ok(subj), Ok(txt)) = (subject, body_text) {
                            let _ = state
                                .ses
                                .send_email()
                                .from_email_address(&state.config.ses_from_address)
                                .destination(
                                    aws_sdk_sesv2::types::Destination::builder()
                                        .to_addresses(email)
                                        .build(),
                                )
                                .content(
                                    aws_sdk_sesv2::types::EmailContent::builder()
                                        .simple(
                                            aws_sdk_sesv2::types::Message::builder()
                                                .subject(subj)
                                                .body(
                                                    aws_sdk_sesv2::types::Body::builder()
                                                        .text(txt)
                                                        .build(),
                                                )
                                                .build(),
                                        )
                                        .build(),
                                )
                                .send()
                                .await;
                        }
                    }
                }
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
            .table_name(&state.config.repos_table_name)
            .item("pk", attr_s(&tenant_id))
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
        tenant_id,
        installation_id,
        repos,
    });

    send_to_queue(state, &state.config.ticket_queue_url, &onboard).await
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
    if !branch.starts_with("coderhelm/") {
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
                "Check suite failed on coderhelm branch — delegating to check_run handler"
            );
            // The individual check_run events will handle CI fixes
            Ok(StatusCode::OK)
        }
        _ => Ok(StatusCode::OK),
    }
}

/// Handle workflow_run events — mark PR ready when CI passes on coderhelm branches.
async fn handle_workflow_run(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
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

    match conclusion {
        "success" => {
            let prs = wf["pull_requests"].as_array().cloned().unwrap_or_default();
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
                "Workflow failed on coderhelm branch — check_run handler will process"
            );
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
                    .table_name(&state.config.repos_table_name)
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

/// Fetch repos for an installation via GitHub API (used when "All repositories" is selected
/// and the webhook payload doesn't include the repo list).
async fn fetch_installation_repos(state: &AppState, installation_id: u64) -> Vec<OnboardRepo> {
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

/// Look up run_id from the runs table by PR number using the repo-index GSI.
async fn lookup_run_by_pr(
    state: &AppState,
    tenant_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> String {
    let tenant_repo = format!("{tenant_id}#{owner}/{name}");
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("tenant_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&tenant_repo))
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
        .table_name(&state.config.billing_table_name)
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
             [Upgrade to Pro](https://app.coderhelm.com/billing) for 5M tokens/month.",
            FREE_TIER_TOKENS / 1000,
        ));
    }

    // Pro tier: check budget cap
    if is_pro {
        let budget = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
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
            // Calculate current overage spend (excludes base subscription)
            let overage_tokens = total_tokens.saturating_sub(INCLUDED_TOKENS);
            let overage_1k = overage_tokens / 1000;
            let overage_spend = overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;
            if overage_spend >= max_budget_cents {
                return Some(format!(
                    "Monthly overage budget of **${:.2}** reached (current overage: **${:.2}**). \
                     Adjust your budget in [Settings](https://app.coderhelm.com/settings/budget).",
                    max_budget_cents as f64 / 100.0,
                    overage_spend as f64 / 100.0,
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
