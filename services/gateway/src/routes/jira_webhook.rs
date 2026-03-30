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

/// Jira webhook handler — per-tenant URL: `/webhooks/jira/:tenant_id`
pub async fn handle_with_tenant(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(path_tenant): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let tenant_id = if path_tenant.starts_with("TENANT%23") || path_tenant.starts_with("TENANT#") {
        path_tenant.replace("%23", "#")
    } else {
        format!("TENANT#{path_tenant}")
    };
    handle_inner(state, headers, body, Some(tenant_id)).await
}

/// Jira webhook handler — legacy shared URL (fallback).
pub async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    handle_inner(state, headers, body, None).await
}

async fn handle_inner(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    path_tenant_id: Option<String>,
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

    let (tenant_id, installation_id) = if let Some(tid) = path_tenant_id {
        // Tenant resolved from URL path — look up installation_id from META
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(tid.clone()),
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
                warn!("Jira webhook: tenant not found from path: {tid}");
                StatusCode::NOT_FOUND
            })?;

        let install_id = item
            .get("github_install_id")
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(StatusCode::BAD_REQUEST)?;

        (tid, install_id)
    } else {
        // Legacy: resolve tenant from payload fields
        match (tenant_id_from_payload, installation_id_from_payload) {
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
        }
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
