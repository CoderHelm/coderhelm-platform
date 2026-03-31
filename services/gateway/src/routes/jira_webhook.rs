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

use crate::models::{TicketMessage, TicketSource, WorkerMessage};
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

/// Jira webhook handler — `/webhooks/jira/:token`
///
/// The token is an opaque random string that maps to a tenant (stored in the jira-tokens table).
/// Additionally, if the tenant has a webhook_secret configured, we verify the `X-Hub-Signature`
/// header (HMAC-SHA256) for defense-in-depth.
pub async fn handle(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(token): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    // Look up tenant by webhook token from jira-tokens table
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

    let tenant_id = token_item
        .get("tenant_id")
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
            warn!(tenant_id, "Jira webhook: invalid HMAC signature");
            log_jira_event(
                &state,
                &tenant_id,
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

    let event_type = payload
        .get("webhookEvent")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let issue = payload.get("issue").ok_or(StatusCode::BAD_REQUEST)?;

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
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.clone()),
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

    let issue_labels: Vec<&str> = issue
        .get("fields")
        .and_then(|f| f.get("labels"))
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
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

    if !has_label && !is_assigned {
        let tk = issue.get("key").and_then(|v| v.as_str()).unwrap_or("?");
        info!(ticket_key = %tk, label = %trigger_label, "Skipping — no trigger label or assignee match");
        log_jira_event(&state, &tenant_id, event_type, tk, "", "filtered", None).await;
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
                aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.clone()),
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
                log_jira_event(&state, &tenant_id, event_type, tk, "", "filtered", None).await;
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

    let body_text = match issue
        .get("fields")
        .and_then(|f| f.get("description"))
        .or_else(|| issue.get("description"))
    {
        Some(Value::String(s)) => s.clone(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
        None => String::new(),
    };

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
        tenant_id,
        installation_id, ticket_key, repo_owner, repo_name, "Jira webhook received"
    );

    let message = WorkerMessage::Ticket(TicketMessage {
        tenant_id: tenant_id.clone(),
        installation_id,
        source: TicketSource::Jira,
        ticket_id: ticket_key.to_string(),
        title: title.clone(),
        body: body_text,
        repo_owner: repo_owner.clone(),
        repo_name: repo_name.clone(),
        issue_number: 0,
        sender,
    });

    let result = send_to_queue(&state, &state.config.ticket_queue_url, &message).await;

    let status_str = if result.is_ok() { "processed" } else { "error" };
    let repo_display = if repo_owner.is_empty() && repo_name.is_empty() {
        String::new()
    } else {
        format!("{repo_owner}/{repo_name}")
    };
    log_jira_event(
        &state,
        &tenant_id,
        event_type,
        ticket_key,
        &title,
        status_str,
        Some(&repo_display),
    )
    .await;

    result
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
    tenant_id: &str,
    event_type: &str,
    ticket_key: &str,
    title: &str,
    status: &str,
    repo: Option<&str>,
) {
    let event_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    // TTL: 90 days
    let expires_at = (chrono::Utc::now() + chrono::Duration::days(90))
        .timestamp()
        .to_string();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_events_table_name)
        .item("tenant_id", attr_s(tenant_id))
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

/// Query Jira events for a tenant (used by the Events tab API).
pub async fn list_jira_events(
    state: &AppState,
    tenant_id: &str,
    limit: i32,
) -> Result<Vec<serde_json::Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("tenant_id = :tid")
        .expression_attribute_values(":tid", attr_s(tenant_id))
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
