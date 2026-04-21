use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tracing::{error, info, warn};

use crate::models::{ImageAttachment, TicketMessage, TicketSource, WorkerMessage};
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

/// Try to acquire a ticket-level lock to prevent duplicate concurrent runs.
/// Returns true if acquired, false if already held by another run.
/// Fails open on unexpected DDB errors (logs warning and allows through).
async fn acquire_ticket_lock(state: &AppState, team_id: &str, ticket_id: &str) -> bool {
    use aws_sdk_dynamodb::types::AttributeValue;

    let sk = format!("TICKET_LOCK#{ticket_id}");
    let now = chrono::Utc::now();
    let locked_until = (now + chrono::Duration::minutes(15)).to_rfc3339();
    let now_str = now.to_rfc3339();

    let result = state
        .dynamo
        .put_item()
        .table_name(&state.config.teams_table_name)
        .item("team_id", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(sk))
        .item("locked_by", AttributeValue::S("gateway".to_string()))
        .item("locked_until", AttributeValue::S(locked_until))
        .item("created_at", AttributeValue::S(now_str.clone()))
        .condition_expression("attribute_not_exists(sk) OR locked_until < :now")
        .expression_attribute_values(":now", AttributeValue::S(now_str))
        .send()
        .await;

    match result {
        Ok(_) => true,
        Err(e) => {
            if e.to_string().contains("ConditionalCheckFailed") {
                info!(team_id, ticket_id, "Ticket lock held — skipping duplicate enqueue");
                false
            } else {
                warn!(team_id, ticket_id, error = %e, "Ticket lock check failed — allowing through");
                true
            }
        }
    }
}

/// Jira webhook handler — `/webhooks/jira/:token`
///
/// The token is an opaque random string that maps to a team (stored in the jira-tokens table).
/// Additionally, if the team has a webhook_secret configured, we verify the `X-Hub-Signature`
/// header (HMAC-SHA256) for defense-in-depth.
pub async fn handle(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(token): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    // Look up team by webhook token from jira-tokens table
    let token_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_tokens_table_name)
        .key("token", attr_s(&token))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to look up Jira webhook token: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item
        .ok_or_else(|| {
            warn!("Jira webhook: invalid token");
            StatusCode::UNAUTHORIZED
        })?;

    let team_id = token_item
        .get("team_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .to_string();

    let installation_id = token_item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // Verify HMAC signature if webhook_secret is configured
    let webhook_secret = token_item
        .get("webhook_secret")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty());

    if let Some(secret) = webhook_secret {
        let sig_header = headers
            .get("x-hub-signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let expected_sig = {
            let mut mac =
                Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC key length");
            mac.update(&body);
            let result = mac.finalize().into_bytes();
            format!("sha256={}", hex::encode(result))
        };

        if sig_header.len() != expected_sig.len()
            || sig_header
                .as_bytes()
                .ct_eq(expected_sig.as_bytes())
                .unwrap_u8()
                != 1
        {
            warn!(team_id, "Jira webhook: invalid HMAC signature");
            log_jira_event(
                &state,
                &team_id,
                "signature_rejected",
                "",
                "",
                "rejected",
                None,
            )
            .await;
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // Parse payload
    let payload: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse Jira webhook body: {e}");
        StatusCode::BAD_REQUEST
    })?;

    process_jira_payload(&state, &team_id, installation_id, &payload).await
}

/// Shared payload processing for both token-based and Forge webhook paths.
async fn process_jira_payload(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    payload: &Value,
) -> Result<StatusCode, StatusCode> {
    let event_type = payload
        .get("webhookEvent")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let issue = payload.get("issue").ok_or(StatusCode::BAD_REQUEST)?;

    // ── Handle comment events ────────────────────────────────────────────────
    if event_type.contains("comment") {
        return handle_jira_comment(state, team_id, installation_id, payload, issue).await;
    }

    // Repo mapping — now optional. When absent, triage will auto-pick the repo.
    let repo_owner = payload
        .get("repo_owner")
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("coderhelm")
                .and_then(|d| d.get("repo_owner"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
        .to_string();

    let repo_name = payload
        .get("repo_name")
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("coderhelm")
                .and_then(|d| d.get("repo_name"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
        .to_string();

    // ── Project + label filtering ────────────────────────────────────────────
    let jira_config = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("JIRA#config".to_string()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    // Check trigger label
    let trigger_label = jira_config
        .as_ref()
        .and_then(|item| item.get("trigger_label").and_then(|v| v.as_s().ok()))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "coderhelm".to_string());

    // Jira labels can be plain strings or objects with a "name" field
    let raw_labels = issue
        .get("fields")
        .and_then(|f| f.get("labels"))
        .and_then(|v| v.as_array());

    let issue_labels: Vec<String> = raw_labels
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    v.as_str().map(|s| s.to_string()).or_else(|| {
                        v.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let has_label = issue_labels.iter().any(|l| {
        l.eq_ignore_ascii_case(&trigger_label)
            || l.to_ascii_lowercase()
                .starts_with(&format!("{}:", trigger_label.to_ascii_lowercase()))
    });

    // Check assignee — trigger if assigned to "coderhelm" (case-insensitive)
    let assignee_name = issue
        .get("fields")
        .and_then(|f| f.get("assignee"))
        .and_then(|a| {
            a.get("displayName")
                .and_then(|v| v.as_str())
                .or_else(|| a.get("name").and_then(|v| v.as_str()))
        })
        .unwrap_or("");
    let is_assigned = assignee_name.to_ascii_lowercase().contains("coderhelm");

    info!(
        labels = ?issue_labels,
        raw_labels = ?issue.get("fields").and_then(|f| f.get("labels")),
        assignee = %assignee_name,
        trigger_label = %trigger_label,
        has_label,
        is_assigned,
        "Jira trigger check"
    );

    if !has_label && !is_assigned {
        let tk = issue.get("key").and_then(|v| v.as_str()).unwrap_or("?");
        info!(ticket_key = %tk, label = %trigger_label, "Skipping — no trigger label or assignee match");
        log_jira_event(state, team_id, event_type, tk, "", "filtered", None).await;
        return Ok(StatusCode::OK);
    }

    // Check project is enabled (if any projects are configured)
    let project_key = issue
        .get("fields")
        .and_then(|f| f.get("project"))
        .and_then(|p| p.get("key"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if !project_key.is_empty() {
        let project_item = state
            .dynamo
            .get_item()
            .table_name(&state.config.jira_config_table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S(format!("JIRA#PROJECT#{project_key}")),
            )
            .send()
            .await
            .ok()
            .and_then(|r| r.item);

        // If the project item exists and is disabled, skip
        if let Some(ref item) = project_item {
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            if !enabled {
                info!(project_key, "Skipping — Jira project not enabled");
                let tk = issue.get("key").and_then(|v| v.as_str()).unwrap_or("?");
                log_jira_event(state, team_id, event_type, tk, "", "filtered", None).await;
                return Ok(StatusCode::OK);
            }
        }
        // If no project item exists at all, allow (no project filtering configured)
    }

    let title = issue
        .get("fields")
        .and_then(|f| f.get("summary"))
        .and_then(|v| v.as_str())
        .or_else(|| issue.get("title").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    let raw_description = issue
        .get("fields")
        .and_then(|f| f.get("description"))
        .or_else(|| issue.get("description"));

    let desc_type = match raw_description {
        Some(Value::String(_)) => "string",
        Some(Value::Object(_)) => "object(ADF)",
        Some(Value::Null) => "null",
        Some(_) => "other",
        None => "none",
    };

    info!(
        team_id,
        ticket_key = issue.get("key").and_then(|v| v.as_str()).unwrap_or("?"),
        has_fields = issue.get("fields").is_some(),
        has_description = raw_description.is_some(),
        description_type = desc_type,
        "Jira ticket description debug"
    );

    let body_text = match raw_description {
        Some(Value::String(s)) => s.clone(),
        Some(other) => extract_adf_text(other),
        None => String::new(),
    };

    info!(
        team_id,
        body_len = body_text.len(),
        body_preview = &body_text[..body_text.len().min(200)],
        "Jira ticket body extracted"
    );

    let ticket_key = issue
        .get("key")
        .and_then(|v| v.as_str())
        .or_else(|| issue.get("id").and_then(|v| v.as_str()))
        .unwrap_or("JIRA-UNKNOWN");

    let sender = payload
        .get("user")
        .and_then(|u| u.get("displayName"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("user")
                .and_then(|u| u.get("name"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("jira")
        .to_string();

    info!(
        team_id,
        installation_id, ticket_key, repo_owner, repo_name, "Jira webhook received"
    );

    // Dedup: skip if this ticket already has a run (running or completed)
    // Allow re-trigger if all existing runs are failed or needs_input
    let existing_runs = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":ticket", attr_s(ticket_key))
        .limit(5)
        .send()
        .await;

    if let Ok(result) = existing_runs {
        let has_blocking_run = result.items().iter().any(|item| {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            matches!(status, "running" | "completed" | "queued")
        });
        if has_blocking_run {
            info!(ticket_key, "Skipping — ticket already has a run");
            log_jira_event(
                state,
                team_id,
                event_type,
                ticket_key,
                &title,
                "duplicate",
                None,
            )
            .await;
            return Ok(StatusCode::OK);
        }
    }

    // Check token budget before processing
    if let Some(_reason) = super::github_webhook::check_run_budget(state, team_id).await {
        info!(team_id, "Jira webhook skipped — token limit reached");
        log_jira_event(
            state,
            team_id,
            event_type,
            ticket_key,
            &title,
            "token_limit",
            None,
        )
        .await;
        return Ok(StatusCode::OK);
    }

    // Atomic dedup: write a short-lived lock to prevent duplicate SQS sends
    // when Forge fires the same event twice in quick succession.
    let dedup_key = format!("DEDUP#jira#{ticket_key}");
    let dedup_ttl = (chrono::Utc::now() + chrono::Duration::minutes(5))
        .timestamp()
        .to_string();
    let dedup_result = state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_events_table_name)
        .item("team_id", attr_s(team_id))
        .item("event_id", attr_s(&dedup_key))
        .item(
            "expires_at",
            aws_sdk_dynamodb::types::AttributeValue::N(dedup_ttl),
        )
        .condition_expression("attribute_not_exists(team_id)")
        .send()
        .await;

    if dedup_result.is_err() {
        info!(
            ticket_key,
            "Skipping — duplicate Forge event (dedup lock exists)"
        );
        log_jira_event(
            state,
            team_id,
            event_type,
            ticket_key,
            &title,
            "duplicate",
            None,
        )
        .await;
        return Ok(StatusCode::OK);
    }

    // Upload image attachments to S3 (sent by Forge as base64)
    let image_attachments = upload_image_attachments(
        state,
        team_id,
        ticket_key,
        payload,
    )
    .await;

    // Acquire ticket lock to prevent duplicate concurrent runs
    if !acquire_ticket_lock(state, team_id, ticket_key).await {
        log_jira_event(state, team_id, event_type, ticket_key, &title, "lock_held", None).await;
        return Ok(StatusCode::OK);
    }

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Jira,
        ticket_id: ticket_key.to_string(),
        title: title.clone(),
        body: body_text,
        repo_owner: repo_owner.clone(),
        repo_name: repo_name.clone(),
        issue_number: 0,
        sender,
        image_attachments,
    });

    let result = send_to_queue(state, &state.config.ticket_queue_url, &message).await;

    let status_str = if result.is_ok() { "processed" } else { "error" };
    let repo_display = if repo_owner.is_empty() && repo_name.is_empty() {
        String::new()
    } else {
        format!("{repo_owner}/{repo_name}")
    };
    log_jira_event(
        state,
        team_id,
        event_type,
        ticket_key,
        &title,
        status_str,
        Some(&repo_display),
    )
    .await;

    result
}

/// Forge app handler — `/webhooks/jira` (no token).
///
/// Authenticates via `coderhelm.installation_id` in the JSON body. The Forge
/// app always includes this field because it reads it from Forge storage.
pub async fn handle_forge(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let payload: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse Forge Jira webhook body: {e}");
        StatusCode::BAD_REQUEST
    })?;

    let installation_id = payload
        .pointer("/coderhelm/installation_id")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // team_id is now the primary identifier, set by the Forge app
    let team_id = payload
        .pointer("/coderhelm/team_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let team_id = match team_id {
        Some(tid) if !tid.is_empty() => {
            if tid.starts_with("TEAM#") {
                tid
            } else {
                format!("TEAM#{tid}")
            }
        }
        _ => {
            // Fallback: resolve team via GitHub installation GSI
            if installation_id == 0 {
                warn!("Forge Jira webhook: missing both team_id and installation_id");
                return Err(StatusCode::BAD_REQUEST);
            }
            match super::github_webhook::resolve_team_by_installation(&state, installation_id).await
            {
                Some(tid) => tid,
                None => {
                    warn!(
                        installation_id,
                        "Forge Jira webhook: no team linked to installation"
                    );
                    return Err(StatusCode::UNAUTHORIZED);
                }
            }
        }
    };

    // Verify this team actually exists by checking jira config
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    if config_item.is_none() {
        warn!(team_id, "Forge Jira webhook: team not configured");
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Verify forge_secret from the payload matches stored secret
    let request_secret = payload
        .pointer("/coderhelm/forge_secret")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let stored_secret = config_item
        .as_ref()
        .and_then(|item| item.get("forge_secret"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    if stored_secret.is_empty() || request_secret != stored_secret {
        warn!(
            team_id,
            "Forge Jira webhook: invalid or missing forge_secret"
        );
        return Err(StatusCode::FORBIDDEN);
    }

    // Resolve GitHub installation_id from team record if not provided by Forge
    let installation_id = if installation_id == 0 {
        state
            .dynamo
            .get_item()
            .table_name(&state.config.teams_table_name)
            .key("team_id", attr_s(&team_id))
            .key("sk", attr_s("META"))
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned())
            .and_then(|item| {
                item.get("github_installation_id")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .unwrap_or(0)
    } else {
        installation_id
    };

    info!(team_id, installation_id, payload = %payload, "Forge Jira webhook received");

    process_jira_payload(&state, &team_id, installation_id, &payload).await
}

/// Handle a Jira comment event — retry failed runs or send feedback for completed runs with PRs.
async fn handle_jira_comment(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    payload: &Value,
    issue: &Value,
) -> Result<StatusCode, StatusCode> {
    let ticket_key = issue.get("key").and_then(|v| v.as_str()).unwrap_or("");

    if ticket_key.is_empty() {
        return Ok(StatusCode::OK);
    }

    let comment_body = payload
        .get("comment")
        .and_then(|c| c.get("body"))
        .and_then(|b| b.as_str())
        .unwrap_or("");

    let comment_author = payload
        .get("comment")
        .and_then(|c| c.get("author"))
        .and_then(|a| a.get("displayName"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Ignore comments by the bot itself
    if comment_author.to_ascii_lowercase().contains("coderhelm") {
        info!(ticket_key, "Skipping own comment");
        return Ok(StatusCode::OK);
    }

    info!(team_id, ticket_key, comment_author, "Jira comment received");

    // Upload image attachments from the Forge payload (if any)
    let fresh_images = upload_image_attachments(state, team_id, ticket_key, payload).await;

    // Find the most recent run for this ticket.
    // Paginate because DynamoDB limit applies before filter_expression.
    let mut run_item_found: Option<std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> = None;
    let mut exclusive_start_key: Option<std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> = None;
    for _ in 0..10 {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :tid")
            .filter_expression("ticket_id = :ticket")
            .expression_attribute_values(":tid", attr_s(team_id))
            .expression_attribute_values(":ticket", attr_s(ticket_key))
            .scan_index_forward(false)
            .limit(100);
        if let Some(ref start) = exclusive_start_key {
            for (k, v) in start {
                q = q.exclusive_start_key(k.clone(), v.clone());
            }
        }
        let result = q.send().await.map_err(|e| {
            error!("Failed to query runs for Jira comment: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        if let Some(item) = result.items().first() {
            run_item_found = Some(item.clone());
            break;
        }
        match result.last_evaluated_key() {
            Some(lek) => exclusive_start_key = Some(lek.clone()),
            None => break,
        }
    }

    let Some(run_item) = run_item_found.as_ref() else {
        info!(ticket_key, "No runs found for Jira comment — ignoring");
        return Ok(StatusCode::OK);
    };

    let run_status = run_item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    let run_id = run_item
        .get("run_id")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    let pr_number = run_item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let repo = run_item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    let (repo_owner, repo_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("", "")
    };

    let title = run_item
        .get("title")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    match run_status {
        // Failed or needs_input run — retry by re-queuing the ticket
        "failed" | "needs_input" => {
            // Use the current ticket description from the payload (re-fetched by Forge)
            // instead of the stale body from the previous run
            let raw_description = issue
                .get("fields")
                .and_then(|f| f.get("description"))
                .or_else(|| issue.get("description"));
            let fresh_body = match raw_description {
                Some(Value::String(s)) => s.clone(),
                Some(other) => extract_adf_text(other),
                None => run_item
                    .get("body")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            };

            let extended_body = if comment_body.is_empty() {
                fresh_body
            } else {
                format!("{fresh_body}\n\n---\n\n**Additional context (from Jira comment by {comment_author}):**\n\n{comment_body}")
            };

            let message = WorkerMessage::Ticket(TicketMessage {
                team_id: team_id.to_string(),
                installation_id,
                source: TicketSource::Jira,
                ticket_id: ticket_key.to_string(),
                title: title.to_string(),
                body: extended_body,
                repo_owner: String::new(), // let worker auto-detect via select_repo
                repo_name: String::new(),
                issue_number: 0,
                sender: comment_author.to_string(),
                image_attachments: if !fresh_images.is_empty() {
                    fresh_images.clone()
                } else {
                    run_item
                        .get("image_attachments")
                        .and_then(|v| v.as_s().ok())
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or_default()
                },
            });

            // Acquire ticket lock to prevent duplicate concurrent runs
            if !acquire_ticket_lock(state, team_id, ticket_key).await {
                let repo_display = format!("{repo_owner}/{repo_name}");
                log_jira_event(state, team_id, "comment_retry", ticket_key, title, "lock_held", Some(&repo_display)).await;
                return Ok(StatusCode::OK);
            }

            info!(
                team_id,
                ticket_key, run_id, run_status, "Retrying Jira run after comment"
            );
            let result = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            let repo_display = format!("{repo_owner}/{repo_name}");
            log_jira_event(
                state,
                team_id,
                "comment_retry",
                ticket_key,
                title,
                if result.is_ok() {
                    "reprocessing"
                } else {
                    "error"
                },
                Some(&repo_display),
            )
            .await;
            result
        }
        // Completed run without PR — treat like needs_input, re-trigger
        "completed" if pr_number == 0 => {
            let raw_description = issue
                .get("fields")
                .and_then(|f| f.get("description"))
                .or_else(|| issue.get("description"));
            let fresh_body = match raw_description {
                Some(Value::String(s)) => s.clone(),
                Some(other) => extract_adf_text(other),
                None => run_item
                    .get("body")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            };

            let extended_body = if comment_body.is_empty() {
                fresh_body
            } else {
                format!("{fresh_body}\n\n---\n\n**Additional context (from Jira comment by {comment_author}):**\n\n{comment_body}")
            };

            let message = WorkerMessage::Ticket(TicketMessage {
                team_id: team_id.to_string(),
                installation_id,
                source: TicketSource::Jira,
                ticket_id: ticket_key.to_string(),
                title: title.to_string(),
                body: extended_body,
                repo_owner: String::new(), // let worker auto-detect via select_repo
                repo_name: String::new(),
                issue_number: 0,
                sender: comment_author.to_string(),
                image_attachments: if !fresh_images.is_empty() {
                    fresh_images.clone()
                } else {
                    run_item
                        .get("image_attachments")
                        .and_then(|v| v.as_s().ok())
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or_default()
                },
            });

            if !acquire_ticket_lock(state, team_id, ticket_key).await {
                let repo_display = format!("{repo_owner}/{repo_name}");
                log_jira_event(state, team_id, "comment_retry", ticket_key, title, "lock_held", Some(&repo_display)).await;
                return Ok(StatusCode::OK);
            }

            info!(
                team_id,
                ticket_key, run_id, "Retrying completed run (no PR) after comment"
            );
            let result = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            let repo_display = format!("{repo_owner}/{repo_name}");
            log_jira_event(state, team_id, "comment_retry", ticket_key, title, if result.is_ok() { "reprocessing" } else { "error" }, Some(&repo_display)).await;
            result
        }
        // Completed run with PR — send feedback
        "completed" if pr_number > 0 => {
            let message = WorkerMessage::Feedback(crate::models::FeedbackMessage {
                team_id: team_id.to_string(),
                installation_id,
                run_id: run_id.to_string(),
                repo_owner: repo_owner.to_string(),
                repo_name: repo_name.to_string(),
                pr_number,
                review_id: 0,
                review_body: format!("Jira comment by {comment_author}: {comment_body}"),
                comments: vec![],
            });

            info!(
                team_id,
                ticket_key, run_id, "Sending feedback for Jira comment on completed run"
            );
            let result = send_to_queue(state, &state.config.feedback_queue_url, &message).await;
            let repo_display = format!("{repo_owner}/{repo_name}");
            log_jira_event(
                state,
                team_id,
                "comment_feedback",
                ticket_key,
                title,
                if result.is_ok() { "feedback" } else { "error" },
                Some(&repo_display),
            )
            .await;
            result
        }
        // Running/queued/awaiting_ci — don't reprocess
        "running" | "queued" | "awaiting_ci" => {
            info!(
                ticket_key,
                run_status, "Ignoring Jira comment — run already in progress"
            );
            log_jira_event(
                state,
                team_id,
                "comment_skipped",
                ticket_key,
                title,
                "skipped",
                None,
            )
            .await;
            Ok(StatusCode::OK)
        }
        _ => {
            info!(
                ticket_key,
                run_status, "Ignoring Jira comment — run status not actionable"
            );
            Ok(StatusCode::OK)
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
            error!("Failed to send Jira message to SQS: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::ACCEPTED)
}

/// Log a Jira webhook event to the jira-events table.
async fn log_jira_event(
    state: &AppState,
    team_id: &str,
    event_type: &str,
    ticket_key: &str,
    title: &str,
    status: &str,
    repo: Option<&str>,
) {
    let event_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    // TTL: 1 day
    let expires_at = (chrono::Utc::now() + chrono::Duration::days(1))
        .timestamp()
        .to_string();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_events_table_name)
        .item("team_id", attr_s(team_id))
        .item("event_id", attr_s(&event_id))
        .item("event_type", attr_s(event_type))
        .item("ticket_key", attr_s(ticket_key))
        .item("title", attr_s(title))
        .item("status", attr_s(status))
        .item("created_at", attr_s(&now))
        .item(
            "expires_at",
            aws_sdk_dynamodb::types::AttributeValue::N(expires_at),
        );

    if let Some(r) = repo {
        put = put.item("repo", attr_s(r));
    }

    if let Err(e) = put.send().await {
        error!("Failed to log Jira event: {e}");
    }
}

/// Query Jira events for a team (used by the Events tab API).
pub async fn list_jira_events(
    state: &AppState,
    team_id: &str,
    limit: i32,
) -> Result<Vec<serde_json::Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(team_id))
        .scan_index_forward(false)
        .limit(limit)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query Jira events: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let events: Vec<serde_json::Value> = result
        .items()
        .iter()
        .filter(|item| {
            // Skip dedup lock records (DEDUP#jira#...)
            let eid = item
                .get("event_id")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            !eid.starts_with("DEDUP#")
        })
        .map(|item| {
            let s = |key: &str| -> &str {
                item.get(key)
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s.as_str())
                    .unwrap_or("")
            };
            serde_json::json!({
                "event_id": s("event_id"),
                "event_type": s("event_type"),
                "ticket_key": s("ticket_key"),
                "title": s("title"),
                "status": s("status"),
                "repo": s("repo"),
                "created_at": s("created_at"),
            })
        })
        .collect();

    Ok(events)
}

/// Upload base64-encoded image attachments from the Forge payload to S3.
/// Returns a Vec of ImageAttachment with S3 keys for inclusion in TicketMessage.
async fn upload_image_attachments(
    state: &AppState,
    team_id: &str,
    ticket_key: &str,
    payload: &Value,
) -> Vec<ImageAttachment> {
    let attachments = match payload.get("image_attachments").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return vec![],
    };

    let mut results = Vec::new();
    for att in attachments {
        let filename = att["filename"].as_str().unwrap_or("image.png");
        let media_type = att["media_type"].as_str().unwrap_or("image/png");
        let data_b64 = match att["data_base64"].as_str() {
            Some(d) => d,
            None => continue,
        };

        let data = match base64_decode(data_b64) {
            Some(d) => d,
            None => {
                warn!(team_id, ticket_key, filename, "Failed to decode base64 image attachment");
                continue;
            }
        };

        let safe_team = team_id.replace('#', "_");
        let s3_key = format!("attachments/{safe_team}/{ticket_key}/{filename}");

        match state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(&s3_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .content_type(media_type)
            .send()
            .await
        {
            Ok(_) => {
                info!(team_id, ticket_key, filename, s3_key = %s3_key, "Uploaded image attachment to S3");
                results.push(ImageAttachment {
                    s3_key: s3_key.clone(),
                    media_type: media_type.to_string(),
                    filename: filename.to_string(),
                });
            }
            Err(e) => {
                warn!(team_id, ticket_key, filename, error = %e, "Failed to upload image attachment to S3");
            }
        }
    }
    results
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(input).ok()
}

/// Extract plain text from Jira Atlassian Document Format (ADF).
/// ADF is a nested JSON tree with `content` arrays and `text` leaf nodes.
/// Falls back to raw JSON if the structure is unexpected.
fn extract_adf_text(value: &Value) -> String {
    let mut parts = Vec::new();
    extract_adf_text_inner(value, &mut parts);
    let result = parts.join("");
    if result.trim().is_empty() {
        // Fallback: serialize as JSON so we don't lose data entirely
        serde_json::to_string(value).unwrap_or_default()
    } else {
        result.trim().to_string()
    }
}

fn extract_adf_text_inner(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            // Text leaf node
            if let Some(Value::String(text)) = map.get("text") {
                parts.push(text.clone());
                return;
            }
            // Block nodes that should produce line breaks
            let node_type = map.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let is_block = matches!(
                node_type,
                "paragraph"
                    | "heading"
                    | "bulletList"
                    | "orderedList"
                    | "listItem"
                    | "blockquote"
                    | "codeBlock"
                    | "rule"
                    | "table"
                    | "tableRow"
                    | "tableCell"
            );
            // Hard break inline node
            if node_type == "hardBreak" {
                parts.push("\n".to_string());
                return;
            }
            // Media nodes (images/attachments embedded in the description)
            if node_type == "media" || node_type == "mediaSingle" || node_type == "mediaInline" {
                let filename = map
                    .get("attrs")
                    .and_then(|a| a.get("alt"))
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        map.get("attrs")
                            .and_then(|a| a.get("id"))
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or("image");
                parts.push(format!("[Attached image: {filename}]"));
                // Recurse into children (mediaSingle wraps a media node)
                if let Some(Value::Array(children)) = map.get("content") {
                    for child in children {
                        extract_adf_text_inner(child, parts);
                    }
                }
                return;
            }
            // Inline card (pasted URLs like Notion links)
            if node_type == "inlineCard" || node_type == "blockCard" {
                if let Some(url) = map
                    .get("attrs")
                    .and_then(|a| a.get("url"))
                    .and_then(|v| v.as_str())
                {
                    parts.push(url.to_string());
                }
                return;
            }
            if let Some(Value::Array(children)) = map.get("content") {
                for child in children {
                    extract_adf_text_inner(child, parts);
                }
            }
            if is_block && !parts.last().is_none_or(|s| s.ends_with('\n')) {
                parts.push("\n".to_string());
            }
        }
        Value::Array(arr) => {
            for item in arr {
                extract_adf_text_inner(item, parts);
            }
        }
        _ => {}
    }
}
