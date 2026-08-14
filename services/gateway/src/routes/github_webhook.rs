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
    AwaitMergeMessage, FeedbackMessage, GraphIndexMessage, MarkReadyMessage, OnboardMessage,
    OnboardRepo, PlanTaskContinueMessage, ResumeMessage, ReviewMessage, TicketMessage,
    TicketSource, WorkerMessage,
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

    // If multiple teams share this installation, prefer the one with the
    // most members — orphan auto-created teams have 0-1 users. Returning
    // whichever DynamoDB listed first routed webhooks to the wrong tenant.
    if items.len() > 1 {
        tracing::warn!(
            installation_id,
            team_count = items.len(),
            "Multiple teams found for installation — selecting by member count"
        );
        let mut best: Option<(String, i32)> = None;
        for item in items {
            let Some(team_id) = item.get("team_id").and_then(|v| v.as_s().ok()).cloned() else {
                continue;
            };
            let count = state
                .dynamo
                .query()
                .table_name(&state.config.users_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
                .expression_attribute_values(":pk", attr_s(&team_id))
                .expression_attribute_values(":u", attr_s("USER#"))
                .select(aws_sdk_dynamodb::types::Select::Count)
                .send()
                .await
                .map(|r| r.count)
                .unwrap_or(0);
            if best.as_ref().is_none_or(|(_, c)| count > *c) {
                best = Some((team_id, count));
            }
        }
        return best.map(|(t, _)| t);
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
        "push" => handle_push(&state, &payload, installation_id, &team_id).await,
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
        image_attachments: vec![],
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // Trigger gate: block while a run is in flight; on a completed latest
    // run, re-trigger only if the issue content changed (re-labeling an
    // edited issue reworks it; re-labeling an unchanged one is a no-op).
    // Failed/needs_input runs no longer block — the old any-run-exists check
    // made GitHub tickets unretriable by re-label.
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    let context_hash = common::ticket_context_hash(
        issue["title"].as_str().unwrap_or(""),
        issue["body"].as_str().unwrap_or(""),
        &[],
    );
    match super::trigger_gate::gate_ticket_trigger(
        state,
        team_id,
        &ticket_id_str,
        Some(&context_hash),
        false,
    )
    .await
    {
        super::trigger_gate::TicketGate::Enqueue => {}
        _ => {
            info!(ticket_id = %ticket_id_str, "Skipping — trigger gate blocked");
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

            // Write event for resume flow
            write_run_event(
                state,
                &run_id,
                "pr_comment",
                &serde_json::json!({
                    "commenter": commenter,
                    "body": body,
                    "pr_number": pr_number,
                }),
            )
            .await;

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
            // Budget gate — the review-feedback path checks this, but the comment
            // path didn't, letting over-limit teams keep spending via PR comments.
            if check_run_budget(state, team_id).await.is_some() {
                info!(team_id, "PR comment feedback skipped — token limit reached");
                return Ok(StatusCode::OK);
            }
            return send_to_queue(state, &state.config.feedback_queue_url, &message).await;
        }

        // Reviewer reply-handling: on a PR the reviewer covers, a human addressing
        // the bot ("@coderhelm re-review" or "@coderhelm <question>") re-runs the
        // review or answers the question — instead of opening a brand-new ticket.
        // Requires an explicit mention/command so unrelated PR chatter never fires.
        if !commenter.contains("coderhelm")
            && (body.contains("@coderhelm") || body.trim_start().starts_with("/coderhelm"))
        {
            let repo = &payload["repository"];
            let owner = repo["owner"]["login"].as_str().unwrap_or("");
            let name = repo["name"].as_str().unwrap_or("");
            let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);
            let cfg = load_review_config(state, team_id, owner, name).await;
            if cfg.enabled && !cfg.killed && pr_number != 0 {
                if let Some(reason) = check_run_budget(state, team_id).await {
                    post_limit_comment(state, installation_id, owner, name, pr_number, &reason)
                        .await;
                    return Ok(StatusCode::OK);
                }
                // Strip the address token to recover the human's actual message.
                let cleaned = body
                    .replace("@coderhelm", " ")
                    .trim()
                    .trim_start_matches("/coderhelm")
                    .trim()
                    .to_string();
                let lower = cleaned.to_lowercase();
                let is_rereview = lower.contains("re-review")
                    || lower.contains("rereview")
                    || lower.contains("review again")
                    || lower == "review";
                // Re-review → fresh verdict; anything else → a question the reviewer
                // answers. Empty head_sha makes the worker resolve the PR's current
                // head, so a reply always targets the latest code.
                let question = if is_rereview || cleaned.is_empty() {
                    None
                } else {
                    Some(cleaned)
                };
                info!(
                    owner,
                    name, pr_number, is_rereview, "Reviewer: reply → review job"
                );
                // Idempotency key = this comment's id. A webhook GitHub delivers
                // more than once (at-least-once) carries the SAME comment id, so
                // the worker runs the review exactly once; a genuinely new
                // `@coderhelm re-review` comment has a new id and re-runs.
                let comment_id = payload["comment"]["id"].as_u64().unwrap_or(0);
                let message = WorkerMessage::Review(ReviewMessage {
                    team_id: team_id.to_string(),
                    installation_id,
                    repo_owner: owner.to_string(),
                    repo_name: name.to_string(),
                    pr_number,
                    head_sha: String::new(),
                    label: cfg.label,
                    question,
                    trigger: "reply".to_string(),
                    dedup_key: format!("reply#{comment_id}"),
                });
                return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            }
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
        image_attachments: vec![],
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // Explicit human command: skip the content-hash check but still block
    // while a run is in flight — a repeated "/coderhelm" comment used to
    // enqueue a duplicate run with no dedup at all.
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    if matches!(
        super::trigger_gate::gate_ticket_trigger(state, team_id, &ticket_id_str, None, true).await,
        super::trigger_gate::TicketGate::SkipInFlight
    ) {
        info!(ticket_id = %ticket_id_str, "Skipping slash command — run already in flight");
        return Ok(StatusCode::OK);
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// Track PR merges for Coderhelm branches — updates run status to "merged".
async fn handle_pull_request(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    // Reviewer agent triggers (opt-in, off by default per repo):
    //  - human PRs: the review label added, or a new commit on a labeled PR.
    //  - CoderHelm's own PRs: auto-reviewed on open/reopen/push with no label
    //    needed (it can't self-approve, so review_pr posts a COMMENT review).
    // Native "Re-request review" button. GitHub fires `pull_request/review_requested`
    // with the reviewer being re-requested. Treat it like an explicit "@coderhelm
    // re-review": if CoderHelm is the requested reviewer and the repo is enabled,
    // enqueue a fresh review of the CURRENT head (empty head_sha → the worker
    // resolves it), bypassing the label/draft/dedup gates since it's a deliberate ask.
    if action == "review_requested" {
        let reviewer = payload["requested_reviewer"]["login"]
            .as_str()
            .unwrap_or("");
        if !reviewer.contains("coderhelm") {
            return Ok(StatusCode::OK);
        }
        let repo = &payload["repository"];
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);
        let cfg = load_review_config(state, team_id, owner, name).await;
        if !cfg.enabled || cfg.killed || pr_number == 0 {
            return Ok(StatusCode::OK);
        }
        if let Some(reason) = check_run_budget(state, team_id).await {
            post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
            return Ok(StatusCode::OK);
        }
        info!(
            owner,
            name, pr_number, "Reviewer: native re-request review → review job"
        );
        // Dedup a redelivered `review_requested` by the head it targets (the
        // payload carries it even though the worker resolves head fresh).
        let req_head = payload["pull_request"]["head"]["sha"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("pr{pr_number}"));
        let message = WorkerMessage::Review(ReviewMessage {
            team_id: team_id.to_string(),
            installation_id,
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            pr_number,
            head_sha: String::new(),
            label: cfg.label,
            question: None,
            trigger: "rerequest".to_string(),
            dedup_key: format!("rerequest#{req_head}"),
        });
        return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    }

    if action == "labeled"
        || action == "synchronize"
        || action == "opened"
        || action == "reopened"
        || action == "ready_for_review"
    {
        return handle_review_trigger(state, payload, installation_id, team_id, action).await;
    }

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
        // Limit applies BEFORE the filter — limit(1) only ever examined the
        // repo's newest run, dropping feedback/merge events for older PRs.
        .limit(50)
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

    let review_state = payload["review"]["state"].as_str().unwrap_or("");

    // Ignore reviews submitted by the bot itself — its APPROVE verdict already
    // armed the gate at review time (review_pr → run_on_approve).
    let reviewer = payload["review"]["user"]["login"].as_str().unwrap_or("");
    if reviewer.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);

    // A human APPROVED review is the second key for armed auto-merge. Re-arm the
    // gate so an approval that arrives AFTER the bot review still triggers the
    // merge — the review-time arming only had the bot's own key. This applies to
    // human PRs too, not just bot PRs. The worker's await_merge gate remains the
    // sole authority on whether auto_merge is on, both keys are present, and every
    // CI check is green before it merges (it waits for pending checks like a
    // staging deploy and never merges on failing CI). Here we only gate on the
    // repo having review enabled, so approvals on non-review repos are ignored.
    if review_state == "approved" {
        let cfg = load_review_config(state, team_id, owner, name).await;
        let head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        let base_branch = pr["base"]["ref"].as_str().unwrap_or("").to_string();
        if cfg.enabled && !cfg.killed && pr_number != 0 && !head_sha.is_empty() {
            let self_authored = pr["user"]["login"]
                .as_str()
                .unwrap_or("")
                .contains("coderhelm");
            info!(
                owner,
                name, pr_number, "Human approval — arming auto-merge gate"
            );
            let message = WorkerMessage::AwaitMerge(AwaitMergeMessage {
                team_id: team_id.to_string(),
                installation_id,
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                head_sha,
                base_branch,
                self_authored,
                attempts: 0,
            });
            let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
        }
        return Ok(StatusCode::OK);
    }

    // Feedback loop: act on "changes_requested" or "commented" reviews (covers
    // formal change-request reviews and standalone single-line comments / thread
    // replies). This path is bot-PR only.
    if review_state != "changes_requested" && review_state != "commented" {
        return Ok(StatusCode::OK);
    }
    let pr_user = pr["user"]["login"].as_str().unwrap_or("");
    if !pr_user.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

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

    // Write event to events table for the resume flow
    let review_body = payload["review"]["body"].as_str().unwrap_or("").to_string();
    let review_id = payload["review"]["id"].as_u64().unwrap_or(0);
    write_run_event(
        state,
        &run_id,
        "pr_review",
        &serde_json::json!({
            "review_state": review_state,
            "reviewer": reviewer,
            "review_id": review_id,
            "review_body": review_body,
            "pr_number": pr_number,
        }),
    )
    .await;

    // Also send to feedback queue for immediate processing
    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        review_id,
        review_body,
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
        branch, check_name, "check_run failed — will be handled by workflow_run completion"
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
                            item.get("pk")
                                .and_then(|v| v.as_s().ok())
                                .map(|s| s.to_string())
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
                            item.get("pk")
                                .and_then(|v| v.as_s().ok())
                                .map(|s| s.to_string())
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
                            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
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
            let team_ids = resolve_all_teams_by_installation(state, installation_id).await;
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
                info!(
                    installation_id,
                    "No teams found for uninstalled installation"
                );
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

    if action == "removed" {
        let removed: Vec<String> = payload["repositories_removed"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|r| r["full_name"].as_str().map(|s| s.to_string()))
            .collect();

        for full_name in &removed {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.repos_table_name)
                .key("pk", attr_s(&team_id))
                .key("sk", attr_s(&format!("REPO#{full_name}")))
                .send()
                .await;
            info!(repo = %full_name, "Removed repo (access revoked)");
        }

        return Ok(StatusCode::OK);
    }

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
    info!(
        branch,
        conclusion, "check_suite completed — workflow_run handler will process"
    );
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
    let workflow_name = wf["name"].as_str().unwrap_or("unknown");

    // Skip "skipped" workflows — they're not failures, just conditional workflows
    // that didn't apply (e.g., deploy only runs on main). Writing them as ci_failed
    // pollutes the events and confuses the CI fix loop.
    if conclusion == "skipped"
        || conclusion == "cancelled"
        || conclusion == "neutral"
        || conclusion == "stale"
    {
        info!(
            branch,
            conclusion, workflow_name, "Ignoring non-actionable workflow conclusion"
        );
        return Ok(StatusCode::OK);
    }

    let repo = &payload["repository"];
    let repo_owner = repo["owner"]["login"].as_str().unwrap_or("");
    let repo_name = repo["name"].as_str().unwrap_or("");

    // Find the run_id for this branch
    let run_id = match find_run_by_branch(state, team_id, repo_owner, repo_name, branch).await {
        Some(id) => id,
        None => {
            // Fallback: still handle success via MarkReady for backward compat
            if conclusion == "success" {
                let prs = wf["pull_requests"].as_array().cloned().unwrap_or_default();
                for pr in &prs {
                    let pr_number = pr["number"].as_u64().unwrap_or(0);
                    if pr_number == 0 {
                        continue;
                    }
                    info!(
                        branch,
                        pr_number, "CI passed (no run found) — marking PR ready directly"
                    );
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
    let event_type = if conclusion == "success" {
        "ci_passed"
    } else {
        "ci_failed"
    };

    // Write event to events table
    let event_sk = format!("EVENT#{}#{}", now.format("%Y%m%dT%H%M%S%.3fZ"), event_type,);

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
        .item(
            "processed",
            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
        )
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
        branch, event_type, workflow_name, "CI event recorded — sending Resume"
    );

    // Send Resume message with a delay — CI repos often have multiple workflows
    // (lint, test, build, deploy) that finish within seconds of each other.
    // The 60s delay lets them all land as events before the worker picks up.
    let message = WorkerMessage::Resume(ResumeMessage {
        team_id: team_id.to_string(),
        run_id: run_id.clone(),
        installation_id,
    });
    let body = serde_json::to_string(&message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The self-healing CI loop hinges on THIS one message — a discarded send
    // strands the run in awaiting_ci with no re-drive. Surface the failure so
    // GitHub retries the webhook (and the awaiting_ci reaper is the backstop).
    if let Err(e) = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .delay_seconds(60)
        .send()
        .await
    {
        error!(run_id, error = %e, "Failed to enqueue CI resume — returning 500 so GitHub retries");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

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
    // Fast path: query repo-index GSI (works for primary repo)
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
        .scan_index_forward(false)
        .limit(50)
        .send()
        .await
        .ok()?;

    if let Some(run_id) = result
        .items()
        .first()
        .and_then(|item| item.get("run_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
    {
        return Some(run_id);
    }

    // Fallback: multi-repo runs store secondary branches in `branches` list.
    // Query team partition and check if this branch is in the list.
    let fallback = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("contains(branches, :b) AND (#s = :s1 OR #s = :s2)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":s1", attr_s("awaiting_ci"))
        .expression_attribute_values(":s2", attr_s("running"))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await
        .ok()?;

    fallback
        .items()
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
                    .key("sk", attr_s(&format!("REPO#{}", repo_name)))
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

/// Per-repo reviewer-agent config. OFF by default — the bulletproof default so
/// the reviewer never touches a repo until a human explicitly opts it in.
struct ReviewConfig {
    enabled: bool,
    /// PR label that triggers a review.
    label: String,
    /// Hard kill switch — blocks ALL reviewer action regardless of `enabled`.
    killed: bool,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            label: "ch-review".to_string(),
            killed: false,
        }
    }
}

async fn load_review_config(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> ReviewConfig {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned());
    let Some(item) = item else {
        return ReviewConfig::default();
    };
    let d = ReviewConfig::default();
    ReviewConfig {
        enabled: item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(d.enabled),
        label: item
            .get("label")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or(d.label),
        killed: item
            .get("killed")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(d.killed),
    }
}

/// Push to a branch: if the repo's code graph is enabled and the push is to the
/// DEFAULT branch, enqueue an INCREMENTAL graph index for exactly the files the
/// push touched (added/modified/removed across its commits). This is what keeps
/// the graph permanently fresh without scheduled rebuilds.
async fn handle_push(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"]
        .as_str()
        .or_else(|| repo["owner"]["name"].as_str())
        .unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let default_branch = repo["default_branch"].as_str().unwrap_or("main");
    let git_ref = payload["ref"].as_str().unwrap_or("");
    if owner.is_empty() || name.is_empty() {
        return Ok(StatusCode::OK);
    }
    // Only the branch the graph tracks.
    if git_ref != format!("refs/heads/{default_branch}") {
        return Ok(StatusCode::OK);
    }
    // Graph must be explicitly enabled for the repo.
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let graph_enabled = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|it| {
            it.get("graph_enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
        })
        .unwrap_or(false);
    if !graph_enabled {
        return Ok(StatusCode::OK);
    }
    // Every file any commit in this push touched (deduped, bounded — a huge
    // push falls back to a FULL index rather than a 1000-file incremental).
    let mut files: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for c in payload["commits"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        for key in ["added", "modified", "removed"] {
            for f in c[key].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
                if let Some(p) = f.as_str() {
                    if seen.insert(p.to_string()) {
                        files.push(p.to_string());
                    }
                }
            }
        }
    }
    if files.is_empty() {
        return Ok(StatusCode::OK);
    }
    let changed_files = if files.len() > 300 { None } else { Some(files) };
    info!(
        owner,
        name,
        full = changed_files.is_none(),
        "Push on default branch → code-graph index"
    );
    let message = WorkerMessage::GraphIndex(GraphIndexMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        branch: default_branch.to_string(),
        changed_files,
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// Reviewer trigger: enqueue a code-review job when the repo's review label is
/// added to an open PR, or a new commit lands on an already-labeled PR.
/// Idempotency (one review per head SHA) is enforced worker-side by the review
/// run claim, so a re-label or duplicate webhook is harmless.
async fn handle_review_trigger(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    action: &str,
) -> Result<StatusCode, StatusCode> {
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    if pr_number == 0 || head_sha.is_empty() || pr["state"].as_str() != Some("open") {
        return Ok(StatusCode::OK);
    }
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");

    let cfg = load_review_config(state, team_id, owner, name).await;
    if !cfg.enabled || cfg.killed {
        return Ok(StatusCode::OK);
    }

    // Review is gated on the repo's trigger label for EVERY PR, including
    // CoderHelm's own (bot-authored) PRs — nothing is reviewed until the label is
    // present. `labeled`: the just-added label must BE the trigger label.
    // `opened`/`reopened`/`ready_for_review`/`synchronize`: the PR must already
    // carry the label (covers a PR opened with it, and re-review on a new commit).
    // Draft PRs are NEVER reviewed here (see the draft gate below): CoderHelm opens
    // its own PRs as draft and self-labels them at once, and a human may label a
    // draft to queue it. Either way the review waits for the PR to actually be
    // ready — GitHub re-fires this path as `ready_for_review` (draft=false) once the
    // author clicks "Ready for review" or the pipeline marks it ready.
    let is_bot_pr = pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm");
    let carries_label = pr["labels"]
        .as_array()
        .map(|ls| {
            ls.iter()
                .any(|l| l["name"].as_str() == Some(cfg.label.as_str()))
        })
        .unwrap_or(false);
    let triggered = match action {
        "labeled" => payload["label"]["name"].as_str() == Some(cfg.label.as_str()),
        "synchronize" | "opened" | "reopened" | "ready_for_review" => carries_label,
        _ => false,
    };
    if !triggered {
        return Ok(StatusCode::OK);
    }

    // Draft gate — never review a PR that is still a draft. This is the difference
    // between "reviewed the instant CoderHelm self-labeled its own half-built draft"
    // and "reviewed once the PR is genuinely ready". A draft carrying the label is a
    // queued request, not a live one: when it flips to ready GitHub fires
    // `ready_for_review` (draft=false) and we land back here with the label present.
    // An explicit `@coderhelm review` comment still works on a draft — that path is
    // separate (handle_issue_comment) — for a human who deliberately wants an early
    // look.
    if pr["draft"].as_bool().unwrap_or(false) {
        info!(
            owner,
            name,
            pr_number,
            action,
            "Reviewer skipped — PR is a draft; will review on ready_for_review"
        );
        return Ok(StatusCode::OK);
    }

    // Same-head dedup — collapse a burst of events for ONE commit into ONE review.
    // GitHub fires several `pull_request` events (opened, labeled, synchronize,
    // ready_for_review) for the same head within a second — CoderHelm's own
    // self-label on PR creation is a frequent trigger. Each event used to enqueue
    // its own review, so one commit was reviewed N times (observed: 4 identical
    // reviews of the same head in 1.3s). The claim is per (repo, pr, head): only the
    // first event through wins. A genuinely NEW commit has a different head → a
    // different key → reviews normally; an explicit `@coderhelm review` comment
    // bypasses this path entirely (handle_issue_comment).
    if !claim_review_slot(state, team_id, owner, name, pr_number, &head_sha).await {
        info!(
            owner,
            name,
            pr_number,
            action,
            "Reviewer skipped — this head was already claimed by a sibling event (dedup)"
        );
        return Ok(StatusCode::OK);
    }

    // Budget gate — same as every other enqueue path, so an over-limit team
    // can't keep spending tokens via label-triggered reviews.
    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(team_id, pr_number, "Reviewer skipped — token limit reached");
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    info!(
        owner,
        name, pr_number, is_bot_pr, action, "Reviewer: trigger matched — enqueuing review job"
    );
    let dedup_key = format!("head#{head_sha}");
    let message = WorkerMessage::Review(ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha,
        label: cfg.label,
        question: None,
        trigger: action.to_string(),
        dedup_key,
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// How long a review claim blocks re-review of the SAME head. Sized to comfortably
/// outlast the burst of webhook events GitHub fires for one commit (seconds), while
/// still self-expiring so a stuck/failed review can be re-driven later (push a new
/// commit, or `@coderhelm review`). The claim is keyed by head_sha, so a new commit
/// is never blocked regardless of this window.
const REVIEW_DEDUP_TTL_SECS: u64 = 3600;

/// Atomically claim the review slot for (team, repo, pr, head). Returns `true` if
/// THIS caller won the claim (should proceed to enqueue), `false` if a sibling
/// event already claimed this exact head (skip — it is already being reviewed).
/// Uses a conditional write so concurrent webhook deliveries race safely: exactly
/// one wins. Fails OPEN on any non-conditional error so a transient DynamoDB blip
/// never silently drops a legitimate review.
async fn claim_review_slot(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
    head_sha: &str,
) -> bool {
    let now = chrono::Utc::now();
    let sk = format!("REVIEWCLAIM#{owner}/{name}#{pr_number:06}#{head_sha}");
    match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sk))
        .item(
            "ttl",
            attr_n(now.timestamp() as u64 + REVIEW_DEDUP_TTL_SECS),
        )
        .item("created_at", attr_s(&now.to_rfc3339()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            // ConditionalCheckFailed => a sibling event already claimed this head.
            // Any other error (network/throttle/etc.) fails OPEN so a transient blip
            // never silently drops a real review. `as_service_error` (not
            // `into_service_error`) so a non-service error can't panic here.
            let already_claimed = e
                .as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false);
            if already_claimed {
                false
            } else {
                warn!(
                    owner,
                    name, pr_number, "Review dedup claim errored — allowing review (fail-open)"
                );
                true
            }
        }
    }
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

/// Write a run event to the events table for the resume flow.
async fn write_run_event(
    state: &AppState,
    run_id: &str,
    event_type: &str,
    payload: &serde_json::Value,
) {
    if state.config.events_table_name.is_empty() {
        return;
    }
    let now = chrono::Utc::now();
    let event_sk = format!("EVENT#{}#{}", now.format("%Y%m%dT%H%M%S%.3fZ"), event_type);

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("RUN#{run_id}")))
        .item("sk", attr_s(&event_sk))
        .item("event_type", attr_s(event_type))
        .item("payload", attr_s(&payload.to_string()))
        .item(
            "processed",
            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
        )
        .item("created_at", attr_s(&now.to_rfc3339()))
        .item("expires_at", attr_n(now.timestamp() as u64 + 30 * 86400))
        .send()
        .await
    {
        error!("Failed to write run event: {e}");
    }
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
    // First try the repo-index GSI (fast path — works for primary repo)
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
        // Limit applies BEFORE the filter — limit(1) only ever examined the
        // repo's newest run, dropping feedback/merge events for older PRs.
        .limit(50)
        .send()
        .await;

    if let Ok(r) = &result {
        if let Some(run_id) = r
            .items()
            .first()
            .and_then(|item| item.get("run_id").and_then(|v| v.as_s().ok()).cloned())
        {
            return run_id;
        }
    }

    // Fallback: multi-repo runs store secondary PRs in pr_numbers list.
    // Query the team partition and check the list attribute.
    let fallback = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("contains(pr_numbers, :pn)")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await;

    match fallback {
        Ok(r) => r
            .items()
            .first()
            .and_then(|item| item.get("run_id").and_then(|v| v.as_s().ok()).cloned())
            .unwrap_or_default(),
        Err(e) => {
            error!("Failed to query run by PR number (fallback): {e}");
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
