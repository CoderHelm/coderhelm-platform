use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::verify::verify_jira_signature;
use crate::models::{TicketMessage, TicketSource, WorkerMessage};
use crate::AppState;

/// Jira webhook handler — per-tenant URL: `/webhooks/jira/:tenant_id?secret=<token>`
///
/// Tenant is resolved from the URL path. Authentication is mandatory via either:
/// - `?secret=<token>` query parameter (for native Jira webhooks), or
/// - `x-hub-signature-256` HMAC header (for Jira Automation rules).
pub async fn handle(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(path_tenant): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let tenant_id = if path_tenant.starts_with("TENANT%23") || path_tenant.starts_with("TENANT#") {
        path_tenant.replace("%23", "#")
    } else {
        format!("TENANT#{path_tenant}")
    };

    // Resolve tenant
    let meta = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.clone()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".to_string()),
        )
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .item
        .ok_or_else(|| {
            warn!("Jira webhook: tenant not found: {tenant_id}");
            StatusCode::NOT_FOUND
        })?;

    let installation_id = meta
        .get("github_install_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // Load secret — mandatory
    let tenant_secret = super::api::load_jira_secret(&state, &tenant_id).await;
    let effective_secret = tenant_secret
        .as_deref()
        .or(state.secrets.jira_webhook_secret.as_deref());

    let stored_secret = effective_secret.ok_or_else(|| {
        warn!("Jira webhook rejected: no secret configured for {tenant_id}");
        StatusCode::UNAUTHORIZED
    })?;

    // Authenticate: URL secret param (native Jira webhooks) or HMAC header (Automation)
    let url_secret = query.get("secret").map(|s| s.as_str());
    let hmac_header = headers
        .get("x-hub-signature-256")
        .or_else(|| headers.get("x-hub-signature"))
        .and_then(|v| v.to_str().ok());

    let authenticated = if let Some(provided_secret) = url_secret {
        use subtle::ConstantTimeEq;
        provided_secret
            .as_bytes()
            .ct_eq(stored_secret.as_bytes())
            .into()
    } else if let Some(signature) = hmac_header {
        verify_jira_signature(stored_secret, &body, signature)
    } else {
        false
    };

    if !authenticated {
        warn!("Jira webhook auth failed for {tenant_id}");
        return Err(StatusCode::UNAUTHORIZED);
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
        .table_name(&state.config.table_name)
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

    let has_label = issue_labels
        .iter()
        .any(|l| *l == trigger_label || l.starts_with(&format!("{trigger_label}:")));

    if !has_label {
        info!(ticket_key = %issue.get("key").and_then(|v| v.as_str()).unwrap_or("?"), label = %trigger_label, "Skipping — trigger label not present");
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
            .table_name(&state.config.table_name)
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
        event_type,
        tenant_id, installation_id, ticket_key, repo_owner, repo_name, "Jira webhook received"
    );

    let message = WorkerMessage::Ticket(TicketMessage {
        tenant_id,
        installation_id,
        source: TicketSource::Jira,
        ticket_id: ticket_key.to_string(),
        title,
        body: body_text,
        repo_owner,
        repo_name,
        issue_number: 0,
        sender,
    });

    send_to_queue(&state, &state.config.ticket_queue_url, &message).await
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
