use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, response::Json, Extension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

use crate::models::{Claims, WorkerMessage};
use crate::AppState;

// ── Types ──────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Finding {
    pub severity: String, // "error" | "warning" | "info"
    pub category: String, // "security" | "performance" | "cost" | "reliability"
    pub title: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfraAnalysis {
    pub status: String, // "pending" | "ready" | "no_infra" | "failed"
    pub has_infra: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagram: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagram_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub findings: Option<Vec<Finding>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_repos: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ── GET /api/infrastructure ────────────────────────────────────────────────────

pub async fn get_infrastructure(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let sk = "INFRA#analysis".to_string();

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.infra_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(sk))
        .send()
        .await
        .map_err(|e| {
            error!("DynamoDB get_item failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let analysis = match result.item() {
        None => {
            // No analysis yet — return the default no_infra state
            InfraAnalysis {
                status: "no_infra".to_string(),
                has_infra: false,
                diagram: None,
                diagram_title: None,
                findings: None,
                suggested_prompt: Some(default_infra_prompt()),
                cached_at: None,
                scanned_repos: None,
                error: None,
            }
        }
        Some(item) => {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "pending".to_string());
            let has_infra = item
                .get("has_infra")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let diagram = item.get("diagram").and_then(|v| v.as_s().ok()).cloned();
            let diagram_title = item
                .get("diagram_title")
                .and_then(|v| v.as_s().ok())
                .cloned();
            let cached_at = item.get("cached_at").and_then(|v| v.as_s().ok()).cloned();

            let findings: Option<Vec<Finding>> = item
                .get("findings")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| serde_json::from_str(s).ok());

            let scanned_repos: Option<Vec<String>> = item
                .get("scanned_repos")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| serde_json::from_str(s).ok());

            let error_msg = item.get("error").and_then(|v| v.as_s().ok()).cloned();

            let suggested_prompt = if has_infra {
                None
            } else {
                Some(default_infra_prompt())
            };

            InfraAnalysis {
                status,
                has_infra,
                diagram,
                diagram_title,
                findings,
                suggested_prompt,
                cached_at,
                scanned_repos,
                error: error_msg,
            }
        }
    };

    Ok(Json(json!(analysis)))
}

// ── POST /api/infrastructure/refresh ─────────────────────────────────────────

pub async fn refresh_infrastructure(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now().to_rfc3339();
    let sk = "INFRA#analysis".to_string();

    // Set status to pending in DynamoDB
    state
        .dynamo
        .put_item()
        .table_name(&state.config.infra_table_name)
        .item("pk", AttributeValue::S(claims.team_id.clone()))
        .item("sk", AttributeValue::S(sk))
        .item("status", AttributeValue::S("pending".to_string()))
        .item("has_infra", AttributeValue::Bool(false))
        .item("updated_at", AttributeValue::S(now))
        .send()
        .await
        .map_err(|e| {
            error!("DynamoDB put_item failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Send SQS message to the tickets queue (worker handles infra_analyze type)
    let msg = WorkerMessage::InfraAnalyze(crate::models::InfraAnalyzeMessage {
        team_id: claims.team_id.clone(),
        triggered_by: claims.display_name(),
        repo: None,
    });

    let body = serde_json::to_string(&msg).map_err(|e| {
        error!("Failed to serialize infra message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
        .map_err(|e| {
            error!("SQS send_message failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "status": "pending" })))
}

// ── GET /api/infrastructure/repo/:owner/:name ────────────────────────────────

pub async fn get_repo_infrastructure(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let sk = format!("INFRA#REPO#{owner}/{name}");

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.infra_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(sk))
        .send()
        .await
        .map_err(|e| {
            error!("DynamoDB get_item failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let analysis = match result.item() {
        None => InfraAnalysis {
            status: "no_infra".to_string(),
            has_infra: false,
            diagram: None,
            diagram_title: None,
            findings: None,
            suggested_prompt: None,
            cached_at: None,
            scanned_repos: None,
            error: None,
        },
        Some(item) => {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "pending".to_string());
            let has_infra = item
                .get("has_infra")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            InfraAnalysis {
                status,
                has_infra,
                diagram: item.get("diagram").and_then(|v| v.as_s().ok()).cloned(),
                diagram_title: item
                    .get("diagram_title")
                    .and_then(|v| v.as_s().ok())
                    .cloned(),
                findings: item
                    .get("findings")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| serde_json::from_str(s).ok()),
                suggested_prompt: None,
                cached_at: item.get("cached_at").and_then(|v| v.as_s().ok()).cloned(),
                scanned_repos: item
                    .get("scanned_repos")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| serde_json::from_str(s).ok()),
                error: item.get("error").and_then(|v| v.as_s().ok()).cloned(),
            }
        }
    };

    Ok(Json(json!(analysis)))
}

// ── POST /api/infrastructure/repo/:owner/:name/refresh ───────────────────────

pub async fn refresh_repo_infrastructure(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo_full = format!("{owner}/{name}");
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("INFRA#REPO#{repo_full}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.infra_table_name)
        .item("pk", AttributeValue::S(claims.team_id.clone()))
        .item("sk", AttributeValue::S(sk))
        .item("status", AttributeValue::S("pending".to_string()))
        .item("has_infra", AttributeValue::Bool(false))
        .item("updated_at", AttributeValue::S(now))
        .send()
        .await
        .map_err(|e| {
            error!("DynamoDB put_item failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let msg = WorkerMessage::InfraAnalyze(crate::models::InfraAnalyzeMessage {
        team_id: claims.team_id.clone(),
        triggered_by: claims.display_name(),
        repo: Some(repo_full),
    });

    let body = serde_json::to_string(&msg).map_err(|e| {
        error!("Failed to serialize infra message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
        .map_err(|e| {
            error!("SQS send_message failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "status": "pending" })))
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn default_infra_prompt() -> String {
    "Create a production-ready AWS CDK TypeScript stack for a SaaS backend with the following components:\n\n\
    - API Gateway (HTTP API) + Lambda function (ARM64, Rust)\n\
    - DynamoDB single-table design with GSIs for team lookup\n\
    - SQS queues with dead-letter queues for async job processing\n\
    - S3 bucket for file storage with lifecycle policies\n\
    - Secrets Manager for API keys\n\
    - CloudWatch alarms for errors and DLQ depth\n\
    - WAF with rate limiting and managed rule sets\n\
    - CloudFront CDN with custom domain\n\
    - KMS encryption for all data at rest\n\
    - PITR enabled on DynamoDB\n\n\
    Follow AWS CDK best practices: use separate stacks per concern, avoid hardcoded account IDs, use context variables for stage."
        .to_string()
}
