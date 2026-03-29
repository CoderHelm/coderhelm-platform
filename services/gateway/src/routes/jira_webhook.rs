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

/// Jira webhook handler.
///
/// Easiest integration path:
/// - Configure Jira Automation/Webhook to POST issue events to `/webhooks/jira`
/// - Include either `installation_id` or `tenant_id`, plus `repo_owner` + `repo_name`
/// - Optional HMAC header: `x-hub-signature-256: sha256=<hex>` when secret is configured
pub async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    // Parse payload first so we can resolve tenant for per-tenant secret lookup.
    let payload: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse Jira webhook body: {e}");
        StatusCode::BAD_REQUEST
    })?;

    let event_type = payload
        .get("webhookEvent")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let issue = payload.get("issue").ok_or(StatusCode::BAD_REQUEST)?;

    // Required repo mapping (from automation payload or custom fields).
    let repo_owner = payload
        .get("repo_owner")
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("coderhelm")
                .and_then(|d| d.get("repo_owner"))
                .and_then(|v| v.as_str())
        })
        .ok_or(StatusCode::BAD_REQUEST)?
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
        .ok_or(StatusCode::BAD_REQUEST)?
        .to_string();

    // Tenant/install resolution: installation_id (preferred) or tenant_id.
    let installation_id_from_payload = payload
        .get("installation_id")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            payload
                .get("coderhelm")
                .and_then(|d| d.get("installation_id"))
                .and_then(|v| v.as_u64())
        });

    let tenant_id_from_payload = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("coderhelm")
                .and_then(|d| d.get("tenant_id"))
                .and_then(|v| v.as_str())
        })
        .map(|s| {
            if s.starts_with("TENANT#") {
                s.to_string()
            } else {
                format!("TENANT#{s}")
            }
        });

    let (tenant_id, installation_id) = match (tenant_id_from_payload, installation_id_from_payload)
    {
        (Some(tenant_id), Some(installation_id)) => (tenant_id, installation_id),
        (None, Some(installation_id)) => (format!("TENANT#{installation_id}"), installation_id),
        (Some(tenant_id), None) => {
            // Resolve install id from tenant metadata.
            let item = state
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
                .ok_or(StatusCode::BAD_REQUEST)?;

            let install_id = item
                .get("github_install_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(StatusCode::BAD_REQUEST)?;

            (tenant_id, install_id)
        }
        (None, None) => return Err(StatusCode::BAD_REQUEST),
    };

    // Verify signature: per-tenant secret (DynamoDB) > global secret (Secrets Manager) > skip.
    let tenant_secret = super::api::load_jira_secret(&state, &tenant_id).await;
    let effective_secret = tenant_secret
        .as_deref()
        .or(state.secrets.jira_webhook_secret.as_deref());

    if let Some(secret) = effective_secret {
        let signature = headers
            .get("x-hub-signature-256")
            .or_else(|| headers.get("x-hub-signature"))
            .and_then(|v| v.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;

        if !verify_jira_signature(secret, &body, signature) {
            warn!("Invalid Jira webhook signature");
            return Err(StatusCode::UNAUTHORIZED);
        }
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
