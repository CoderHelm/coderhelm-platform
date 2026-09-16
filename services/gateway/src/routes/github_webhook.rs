use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::coderhelm_command::{self, Command};
use crate::auth::verify::verify_github_signature;
use crate::models::{
    AwaitMergeMessage, FeedbackMessage, GraphIndexMessage, MarkReadyMessage, OnboardMessage,
    OnboardRepo, PlanTaskContinueMessage, ResumeMessage, ReviewMessage, TicketMessage,
    TicketSource, WorkerMessage,
};
use crate::AppState;
use common::merge_gate;

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
    // most members — orphan auto-created teams have 0-1 users. Ties go to the
    // lowest team id so the choice never depends on query order, and a failed
    // count routes nowhere (the delivery fails visibly) rather than to a team
    // that merely looked biggest because the real one could not be counted.
    if items.len() > 1 {
        let mut counted: Vec<(String, i32)> = Vec::with_capacity(items.len());
        for item in items {
            let Some(team_id) = item.get("team_id").and_then(|v| v.as_s().ok()).cloned() else {
                continue;
            };
            let count = match state
                .dynamo
                .query()
                .table_name(&state.config.users_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
                .expression_attribute_values(":pk", attr_s(&team_id))
                .expression_attribute_values(":u", attr_s("USER#"))
                .select(aws_sdk_dynamodb::types::Select::Count)
                .send()
                .await
            {
                Ok(r) => r.count,
                Err(e) => {
                    tracing::error!(installation_id, team_id, error = %e, "Could not count team members — not routing this delivery");
                    return None;
                }
            };
            counted.push((team_id, count));
        }
        let chosen = pick_team(counted);
        tracing::info!(
            installation_id,
            team_count = items.len(),
            team_id = chosen.as_deref().unwrap_or(""),
            "Multiple teams share this installation — routed by member count"
        );
        return chosen;
    }

    items
        .first()
        .and_then(|item| item.get("team_id").and_then(|v| v.as_s().ok()).cloned())
}

/// Pure: the team with the most members; ties go to the lowest team id.
fn pick_team(counted: Vec<(String, i32)>) -> Option<String> {
    counted
        .into_iter()
        .max_by(|(a_id, a_n), (b_id, b_n)| a_n.cmp(b_n).then_with(|| b_id.cmp(a_id)))
        .map(|(id, _)| id)
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

    // Unique per event and kept by GitHub's redeliveries, so it identifies one
    // deliberate action (e.g. a re-added label) across retries.
    let delivery = headers
        .get("x-github-delivery")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    info!(
        event_type,
        installation_id,
        delivery = %delivery,
        action = payload["action"].as_str().unwrap_or(""),
        repo = payload["repository"]["full_name"].as_str().unwrap_or(""),
        "GitHub webhook received"
    );

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
        "pull_request" => {
            handle_pull_request(&state, &payload, installation_id, &team_id, &delivery).await
        }
        "pull_request_review" => {
            handle_pr_review(&state, &payload, installation_id, &team_id).await
        }
        // Inline review comments: on CoderHelm's own PRs they reach the fix loop
        // through the `pull_request_review` event; on other PRs an inline
        // `@coderhelm …` is answered in its thread.
        "pull_request_review_comment" => {
            handle_review_comment(&state, &payload, installation_id, &team_id).await
        }
        "pull_request_review_thread" => {
            info!(event_type, "Review thread event — no action");
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
    let body = payload["comment"]["body"].as_str().unwrap_or("");
    let comment_id = payload["comment"]["id"].as_u64().unwrap_or(0);
    let commenter = payload["comment"]["user"]["login"].as_str().unwrap_or("");

    // CoderHelm's own comments (and other bots') are never instructions — its
    // answers even carry an "@coderhelm re-review" hint in their footer.
    if is_bot_user(&payload["comment"]["user"]) {
        return Ok(StatusCode::OK);
    }

    // A new comment, or an edit that adds a command the earlier text lacked
    // (e.g. someone fixing "@CoderHelm" after posting). The edit gets its own
    // dedup key so it runs exactly once.
    let dedup_key = match action {
        "created" => format!("reply#{comment_id}"),
        "edited" => {
            let before = payload["changes"]["body"]["from"].as_str().unwrap_or("");
            if coderhelm_command::parse(before).is_some()
                || coderhelm_command::parse(body).is_none()
            {
                return Ok(StatusCode::OK);
            }
            format!(
                "reply#{comment_id}#edit#{}",
                payload["comment"]["updated_at"].as_str().unwrap_or("")
            )
        }
        _ => return Ok(StatusCode::OK),
    };
    let command = coderhelm_command::parse(body);

    if payload["issue"]["pull_request"].is_object() {
        let repo = &payload["repository"];
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);
        let is_bot_pr = payload["issue"]["user"]["login"]
            .as_str()
            .unwrap_or("")
            .contains("coderhelm");

        // An explicit re-review always gets a fresh verdict. Other asks go to the
        // reviewer on people's PRs; on CoderHelm's own PRs the fix loop handles
        // them, because it can change the code it wrote.
        let review_job = match &command {
            Some(Command::Rereview) => Some(None),
            Some(Command::Ask(q)) if !is_bot_pr => Some(Some(q.clone())),
            _ => None,
        };
        if let Some(question) = review_job {
            let cfg = load_review_config(state, team_id, owner, name).await?;
            if cfg.enabled && !cfg.killed && pr_number != 0 {
                if let Some(reason) = check_run_budget(state, team_id).await {
                    post_limit_comment(state, installation_id, owner, name, pr_number, &reason)
                        .await;
                    return Ok(StatusCode::OK);
                }
                info!(
                    owner,
                    name,
                    pr_number,
                    commenter,
                    rereview = question.is_none(),
                    "Reviewer: comment → review job"
                );
                // Empty head_sha: the worker reviews the PR's current head. The
                // dedup key is the comment (or the edit), so a redelivered
                // webhook runs once and every new ask runs again.
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
                    dedup_key,
                    attempt: 0,
                    reply_to_comment_id: None,
                });
                return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            }
            info!(
                owner,
                name, pr_number, "Mention on a PR in a repo with review off — not a review job"
            );
        }

        if is_bot_pr {
            let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
            if run_id.is_empty() {
                warn!(
                    owner,
                    name, pr_number, "No run found for PR comment — skipping"
                );
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

            info!(
                owner,
                name, pr_number, commenter, "PR comment → feedback queue"
            );
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
                // Reply on the PR only to people who addressed CoderHelm; other
                // conversation is still applied, without a reply to each remark.
                trigger_author: if command.is_some() {
                    commenter.to_string()
                } else {
                    String::new()
                },
            });
            // Budget gate — same as every other enqueue path.
            if let Some(reason) = check_run_budget(state, team_id).await {
                info!(team_id, "PR comment feedback skipped — token limit reached");
                post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
                return Ok(StatusCode::OK);
            }
            return send_to_queue(state, &state.config.feedback_queue_url, &message).await;
        }
    }

    // `/coderhelm` or an @coderhelm mention on an issue (or on a person's PR in a
    // repo without review) starts a ticket run.
    if command.is_none() {
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

/// Is this GitHub user a bot (an App, or a `[bot]`/CoderHelm login)?
fn is_bot_user(user: &Value) -> bool {
    let login = user["login"].as_str().unwrap_or("");
    user["type"].as_str() == Some("Bot") || login.ends_with("[bot]") || login.contains("coderhelm")
}

/// Track PR merges for Coderhelm branches — updates run status to "merged".
async fn handle_pull_request(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    delivery: &str,
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
        let cfg = load_review_config(state, team_id, owner, name).await?;
        if !cfg.enabled || cfg.killed || pr_number == 0 {
            info!(
                owner,
                name, pr_number, "Re-request ignored — review is off for this repo"
            );
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
            // Every click is a deliberate ask; a redelivery of the same click
            // keeps its delivery id.
            dedup_key: format!("rerequest#{req_head}#{delivery}"),
            attempt: 0,
            reply_to_comment_id: None,
        });
        return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    }

    if action == "labeled"
        || action == "synchronize"
        || action == "opened"
        || action == "reopened"
        || action == "ready_for_review"
    {
        return handle_review_trigger(state, payload, installation_id, team_id, action, delivery)
            .await;
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
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);

    // A dismissal (of CoderHelm's review or of a person's approval) changes
    // what the merge gate may do: re-evaluate an existing gate right away so it
    // never merges on a withdrawn key and its status comment says so.
    if action == "dismissed" {
        rearm_gate(
            state,
            team_id,
            installation_id,
            owner,
            name,
            pr_number,
            pr["head"]["sha"].as_str().unwrap_or(""),
            |gate_state| {
                gate_state != merge_gate::STATE_MERGED && gate_state != merge_gate::STATE_CLOSED
            },
            "review dismissed",
        )
        .await?;
        return Ok(StatusCode::OK);
    }
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

    // A human APPROVED review is the second key for armed auto-merge. Arm the
    // gate so an approval that arrives AFTER the bot review still triggers the
    // merge. The worker's gate remains the sole authority on whether auto_merge
    // is on, both keys are present, and every CI check is green; here we only
    // gate on the repo having review enabled.
    if review_state == "approved" {
        let cfg = load_review_config(state, team_id, owner, name).await?;
        let head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        let base_branch = pr["base"]["ref"].as_str().unwrap_or("").to_string();
        if !cfg.enabled || cfg.killed || pr_number == 0 || head_sha.is_empty() {
            info!(
                owner,
                name, pr_number, "Human approval ignored — review is off for this repo"
            );
            return Ok(StatusCode::OK);
        }
        let self_authored = pr["user"]["login"]
            .as_str()
            .unwrap_or("")
            .contains("coderhelm");
        info!(
            owner,
            name, pr_number, reviewer, "Human approval — arming auto-merge gate"
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
            chain_id: String::new(),
        });
        // A lost arming is a lost merge: surface the failure to GitHub.
        return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
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
        // A review with a written body gets an answer on the PR when it has no
        // inline threads to answer in.
        trigger_author: if payload["review"]["body"]
            .as_str()
            .is_some_and(|b| !b.trim().is_empty())
        {
            reviewer.to_string()
        } else {
            String::new()
        },
    });

    send_to_queue(state, &state.config.feedback_queue_url, &message).await
}

/// Re-arm an existing merge gate for this PR from its stored record, when the
/// record is for `head_sha` and its state passes `wake`. No record, another
/// head, or a state `wake` rejects: nothing to do. A failed read or enqueue is
/// returned as 500 so GitHub shows the failed delivery.
#[allow(clippy::too_many_arguments)]
async fn rearm_gate(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
    head_sha: &str,
    wake: impl Fn(&str) -> bool,
    reason: &str,
) -> Result<(), StatusCode> {
    if pr_number == 0 || head_sha.is_empty() {
        return Ok(());
    }
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&merge_gate::record_sk(owner, name, pr_number)))
        .send()
        .await
        .map_err(|e| {
            error!(owner, name, pr_number, error = %e, "Could not read merge gate record");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(());
    };
    let get_s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let gate_state = get_s("state");
    if get_s("head_sha") != head_sha || !wake(&gate_state) {
        return Ok(());
    }
    info!(owner, name, pr_number, gate_state = %gate_state, reason, "Re-arming merge gate");
    let message = WorkerMessage::AwaitMerge(AwaitMergeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha: head_sha.to_string(),
        base_branch: get_s("base_branch"),
        self_authored: item
            .get("self_authored")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        attempts: 0,
        chain_id: String::new(),
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message)
        .await
        .map(|_| ())
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
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }
    let suite = &payload["check_suite"];
    let branch = suite["head_branch"].as_str().unwrap_or("");
    let conclusion = suite["conclusion"].as_str().unwrap_or("");
    let head_sha = suite["head_sha"].as_str().unwrap_or("");

    // CI finished on a PR's head: wake that PR's merge gate if it is waiting on
    // (or was stopped by) CI, on any branch. The gate re-reads every check
    // itself, so a flaky job re-run to green, or a deploy slower than the
    // gate's poll window, still merges.
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    for pr in suite["pull_requests"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        let pr_number = pr["number"].as_u64().unwrap_or(0);
        rearm_gate(
            state,
            team_id,
            installation_id,
            owner,
            name,
            pr_number,
            head_sha,
            merge_gate::wakes_on_ci,
            "check suite completed",
        )
        .await?;
    }

    if branch.starts_with("coderhelm/") {
        // CI result handling for CoderHelm's own runs is done by the
        // workflow_run handler; this is for observability.
        info!(
            branch,
            conclusion, "check_suite completed — workflow_run handler will process"
        );
    }
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

/// The repo's reviewer config. A missing item is the defaults (review off); a
/// failed read is logged and returned as 500, so it is never mistaken for
/// "review off" and GitHub shows the failed delivery.
async fn load_review_config(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<ReviewConfig, StatusCode> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!(team_id, owner, name, error = %e, "Could not read review config");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(ReviewConfig::default());
    };
    let d = ReviewConfig::default();
    Ok(ReviewConfig {
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
    })
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
    delivery: &str,
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

    let cfg = load_review_config(state, team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed {
        return Ok(StatusCode::OK);
    }

    // Review is gated on the repo's trigger label for EVERY PR, including
    // CoderHelm's own (bot-authored) PRs — nothing is reviewed until the label is
    // present. `labeled`: the just-added label must BE the trigger label.
    // `opened`/`reopened`/`ready_for_review`/`synchronize`: the PR must already
    // carry the label (covers a PR opened with it, and re-review on a new commit).
    // Label names are case-insensitive on GitHub, so the match is too.
    let is_bot_pr = pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm");
    let is_trigger_label = |n: &str| n.eq_ignore_ascii_case(&cfg.label);
    let carries_label = pr["labels"]
        .as_array()
        .map(|ls| {
            ls.iter()
                .any(|l| l["name"].as_str().is_some_and(is_trigger_label))
        })
        .unwrap_or(false);
    let triggered = match action {
        "labeled" => payload["label"]["name"]
            .as_str()
            .is_some_and(is_trigger_label),
        "synchronize" | "opened" | "reopened" | "ready_for_review" => carries_label,
        _ => false,
    };
    if !triggered {
        return Ok(StatusCode::OK);
    }

    // Draft gate — never review a PR that is still a draft. A draft carrying the
    // label is a queued request: when it flips to ready GitHub fires
    // `ready_for_review` (draft=false) and we land back here with the label
    // present. An explicit `@coderhelm review` comment still works on a draft.
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

    // Budget before any claim, so a refused trigger never uses up the commit.
    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(team_id, pr_number, "Reviewer skipped — token limit reached");
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // A person re-adding the label is a deliberate ask for a fresh review of
    // this commit: it gets its own key. Everything else collapses into one
    // review per commit — GitHub fires several events (opened, labeled,
    // synchronize, ready_for_review) for the same head within seconds.
    let deliberate = is_deliberate_relabel(
        action,
        is_bot_user(&payload["sender"]),
        pr["created_at"].as_str(),
        chrono::Utc::now(),
    ) && !delivery.is_empty();
    let dedup_key = if deliberate {
        format!("label#{delivery}")
    } else {
        let claim_sk = review_claim_sk(owner, name, pr_number, &head_sha);
        match common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &claim_sk,
            REVIEW_BURST_WINDOW_SECS,
        )
        .await
        {
            common::claim::Claim::Won => {}
            common::claim::Claim::Held => {
                info!(
                    owner,
                    name,
                    pr_number,
                    action,
                    "Reviewer skipped — this head was already claimed by a sibling event (dedup)"
                );
                return Ok(StatusCode::OK);
            }
            common::claim::Claim::Failed(e) => {
                // A transient DynamoDB error must never silently drop a review.
                warn!(owner, name, pr_number, error = %e, "Review dedup claim errored — allowing review (fail-open)");
            }
        }
        format!("head#{head_sha}")
    };

    info!(
        owner,
        name,
        pr_number,
        is_bot_pr,
        action,
        deliberate,
        "Reviewer: trigger matched — enqueuing review job"
    );
    let message = WorkerMessage::Review(ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha: head_sha.clone(),
        label: cfg.label,
        question: None,
        trigger: action.to_string(),
        dedup_key,
        attempt: 0,
        reply_to_comment_id: None,
    });
    let sent = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    if sent.is_err() && !deliberate {
        // Not queued: free the commit so a redelivery or re-label can retry.
        common::claim::release(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &review_claim_sk(owner, name, pr_number, &head_sha),
        )
        .await;
    }
    sent
}

/// How long one commit's review claim collapses sibling events. GitHub sends
/// the burst for one commit within seconds; after this window a same-commit
/// trigger (reopen, ready again) is treated as new.
const REVIEW_BURST_WINDOW_SECS: u64 = 10 * 60;

fn review_claim_sk(owner: &str, name: &str, pr_number: u64, head_sha: &str) -> String {
    format!("REVIEWCLAIM#{owner}/{name}#{pr_number:06}#{head_sha}")
}

/// Pure: is this `labeled` event a person re-adding the review label to an
/// existing PR, rather than part of the event burst of a PR opened with it?
fn is_deliberate_relabel(
    action: &str,
    sender_is_bot: bool,
    pr_created_at: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    const OPEN_BURST_SECS: i64 = 60;
    action == "labeled"
        && !sender_is_bot
        && pr_created_at
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .is_some_and(|created| {
                (now - created.with_timezone(&chrono::Utc)).num_seconds() > OPEN_BURST_SECS
            })
}

/// Inline review comment. On a person's PR in a repo with review on, an
/// `@coderhelm …` in a thread is answered in that thread (or re-reviews).
/// CoderHelm's own PRs route thread comments to the fix loop through the
/// `pull_request_review` event instead.
async fn handle_review_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    if payload["action"].as_str() != Some("created") || is_bot_user(&payload["comment"]["user"]) {
        return Ok(StatusCode::OK);
    }
    let pr = &payload["pull_request"];
    if pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm")
    {
        return Ok(StatusCode::OK);
    }
    let Some(command) = coderhelm_command::parse(payload["comment"]["body"].as_str().unwrap_or(""))
    else {
        return Ok(StatusCode::OK);
    };
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let comment_id = payload["comment"]["id"].as_u64().unwrap_or(0);
    let cfg = load_review_config(state, team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed || pr_number == 0 || comment_id == 0 {
        info!(
            owner,
            name, pr_number, "Inline mention ignored — review is off for this repo"
        );
        return Ok(StatusCode::OK);
    }
    if let Some(reason) = check_run_budget(state, team_id).await {
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }
    let (question, reply_to) = match command {
        Command::Rereview => (None, None),
        Command::Ask(q) => (Some(q), Some(comment_id)),
    };
    info!(
        owner,
        name,
        pr_number,
        comment_id,
        rereview = question.is_none(),
        "Reviewer: inline comment → review job"
    );
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
        dedup_key: format!("reviewcomment#{comment_id}"),
        attempt: 0,
        reply_to_comment_id: reply_to,
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
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

#[cfg(test)]
mod trigger_tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn relabel_by_a_person_on_an_existing_pr_is_deliberate() {
        let created = at(0).to_rfc3339();
        assert!(is_deliberate_relabel(
            "labeled",
            false,
            Some(&created),
            at(3600)
        ));
    }

    #[test]
    fn opening_burst_bots_and_other_actions_are_not() {
        let created = at(0).to_rfc3339();
        // PR opened with the label: the labeled event arrives with the open.
        assert!(!is_deliberate_relabel(
            "labeled",
            false,
            Some(&created),
            at(5)
        ));
        // CoderHelm labeling its own PR.
        assert!(!is_deliberate_relabel(
            "labeled",
            true,
            Some(&created),
            at(3600)
        ));
        assert!(!is_deliberate_relabel(
            "synchronize",
            false,
            Some(&created),
            at(3600)
        ));
        assert!(!is_deliberate_relabel("labeled", false, None, at(3600)));
    }

    #[test]
    fn team_pick_is_largest_then_lowest_id() {
        let teams = vec![
            ("TEAM#b".to_string(), 1),
            ("TEAM#real".to_string(), 10),
            ("TEAM#a".to_string(), 1),
        ];
        assert_eq!(pick_team(teams).as_deref(), Some("TEAM#real"));
        let tie = vec![("TEAM#b".to_string(), 3), ("TEAM#a".to_string(), 3)];
        assert_eq!(pick_team(tie).as_deref(), Some("TEAM#a"));
        assert_eq!(pick_team(vec![]), None);
    }

    #[test]
    fn bot_users_are_recognized() {
        assert!(is_bot_user(
            &serde_json::json!({"login": "coderhelm[bot]", "type": "Bot"})
        ));
        assert!(is_bot_user(&serde_json::json!({"login": "sentry[bot]"})));
        assert!(is_bot_user(
            &serde_json::json!({"login": "renovate", "type": "Bot"})
        ));
        assert!(!is_bot_user(
            &serde_json::json!({"login": "carolaQuintana", "type": "User"})
        ));
    }
}
