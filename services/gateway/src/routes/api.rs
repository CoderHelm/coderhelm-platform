use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::{
    Claims, InfraAnalyzeMessage, NotificationPrefs, OnboardMessage, OnboardRepo, TicketMessage,
    TicketSource, WorkerMessage,
};
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;

/// GET /api/me — return current user info.
pub async fn me(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(json!({
        "user_id": claims.sub,
        "tenant_id": claims.tenant_id,
        "github_login": claims.github_login,
        "email": item.get("email").and_then(|v| v.as_s().ok()),
        "avatar_url": item.get("avatar_url").and_then(|v| v.as_s().ok()),
        "role": item.get("role").and_then(|v| v.as_s().ok()),
    })))
}

/// GET /api/runs — list runs for the tenant (from runs table).
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.tenant_id))
        .scan_index_forward(false) // newest first (ULID sorts lexicographically)
        .limit(50)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let runs: Vec<Value> = result
        .items()
        .iter()
        .map(|item| {
            json!({
                "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                "status": item.get("status").and_then(|v| v.as_s().ok()),
                "ticket_id": item.get("ticket_id").and_then(|v| v.as_s().ok()),
                "title": item.get("title").and_then(|v| v.as_s().ok()),
                "repo": item.get("repo").and_then(|v| v.as_s().ok()),
                "pr_url": item.get("pr_url").and_then(|v| v.as_s().ok()),
                "cost_usd": item.get("cost_usd").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()),
                "tokens_in": item.get("tokens_in").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "tokens_out": item.get("tokens_out").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "duration_s": item.get("duration_s").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({ "runs": runs })))
}

/// GET /api/runs/:run_id — get single run detail (from runs table).
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(&claims.tenant_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(json!({
        "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
        "status": item.get("status").and_then(|v| v.as_s().ok()),
        "ticket_source": item.get("ticket_source").and_then(|v| v.as_s().ok()),
        "ticket_id": item.get("ticket_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "repo": item.get("repo").and_then(|v| v.as_s().ok()),
        "branch": item.get("branch").and_then(|v| v.as_s().ok()),
        "pr_url": item.get("pr_url").and_then(|v| v.as_s().ok()),
        "pr_number": item.get("pr_number").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "current_pass": item.get("current_pass").and_then(|v| v.as_s().ok()),
        "tokens_in": item.get("tokens_in").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "tokens_out": item.get("tokens_out").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "cost_usd": item.get("cost_usd").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()),
        "files_modified": item.get("files_modified").and_then(|v| v.as_l().ok()).map(|list| list.iter().filter_map(|v| v.as_s().ok().map(|s| s.as_str())).collect::<Vec<_>>()),
        "duration_s": item.get("duration_s").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "error": item.get("error_message").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
    })))
}

/// GET /api/runs/:run_id/openspec — fetch the four openspec files from S3.
pub async fn get_run_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // First verify the run belongs to this tenant
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(&claims.tenant_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("ticket_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for openspec: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?;

    let prefix = format!(
        "tenants/{}/runs/{}/openspec",
        claims.tenant_id,
        ticket_id.to_lowercase()
    );

    let mut files = serde_json::Map::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        if let Ok(output) = state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            if let Ok(bytes) = output.body.collect().await {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    files.insert(name.replace(".md", ""), Value::String(text));
                }
            }
        }
    }

    Ok(Json(Value::Object(files)))
}

/// POST /api/runs/:run_id/retry — re-enqueue a failed run to the ticket queue.
pub async fn retry_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Fetch the failed run
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(&claims.tenant_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    if status != "failed" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Prefer stored installation_id; fall back to extracting from tenant_id (TENANT#<id>)
    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = claims
            .tenant_id
            .strip_prefix("TENANT#")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
    }
    let issue_number = item
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    if installation_id == 0 {
        error!("Cannot retry run {run_id}: unable to determine installation_id");
        return Err(StatusCode::BAD_REQUEST);
    }

    let ticket_source = item
        .get("ticket_source")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("github");
    let source = match ticket_source {
        "jira" => TicketSource::Jira,
        _ => TicketSource::Github,
    };

    let message = WorkerMessage::Ticket(TicketMessage {
        tenant_id: claims.tenant_id.clone(),
        installation_id,
        source,
        ticket_id: item
            .get("ticket_id")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        title: item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        body: String::new(), // body is re-fetched from GitHub by the worker
        repo_owner: parts[0].to_string(),
        repo_name: parts[1].to_string(),
        issue_number,
        sender: claims.github_login.clone(),
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize retry message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(run_id, "Run retried — dispatched to SQS");
    Ok(Json(json!({ "status": "retrying" })))
}

/// GET /api/repos — list repos configured for this tenant.
pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repos: Vec<Value> = result
        .items()
        .iter()
        .map(|item| {
            json!({
                "name": item.get("repo_name").and_then(|v| v.as_s().ok()),
                "enabled": item.get("enabled").and_then(|v| v.as_bool().ok()),
                "ticket_source": item.get("ticket_source").and_then(|v| v.as_s().ok()),
                "onboard_status": item.get("onboard_status").and_then(|v| v.as_s().ok()),
                "onboard_error": item.get("onboard_error").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({ "repos": repos })))
}

/// GET /api/integrations/jira/check — quick Jira integration readiness check.
pub async fn get_jira_integration_check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let repos_result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repo_items = repos_result.items();
    let repo_count = repo_items.len();
    let enabled_repos: Vec<_> = repo_items
        .iter()
        .filter(|item| {
            item.get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true)
        })
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?;
            Some(sk.strip_prefix("REPO#")?.to_string())
        })
        .collect();
    let enabled_repo_count = enabled_repos.len();

    let runs_result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.tenant_id))
        .scan_index_forward(false)
        .limit(25)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let jira_seen = runs_result.items().iter().any(|item| {
        item.get("ticket_source")
            .and_then(|v| v.as_s().ok())
            .map(|v| v == "jira")
            .unwrap_or(false)
    });

    // Load per-tenant JIRA secret from DynamoDB
    let tenant_secret = load_jira_secret(&state, &claims.tenant_id).await;
    let secret_configured = tenant_secret.is_some() || state.secrets.jira_webhook_secret.is_some();
    let ready = secret_configured && enabled_repo_count > 0;

    // Load installation_id from tenant META
    let meta_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    let installation_id = meta_result
        .as_ref()
        .and_then(|item| item.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(Json(json!({
        "ready": ready,
        "secret_configured": secret_configured,
        "configured_repos": {
            "total": repo_count,
            "enabled": enabled_repo_count
        },
        "enabled_repos": enabled_repos,
        "jira_events_seen": jira_seen,
        "installation_id": installation_id,
        "tenant_id": claims.tenant_id,
        "webhook_url": "https://api.coderhelm.com/webhooks/jira",
        "checklist": [
            "Generate a webhook secret below",
            "Create a Jira Automation rule with a Send web request action",
            "Paste the webhook URL and payload template",
            "Add the secret as x-hub-signature-256 header in Jira",
            "Create a test issue to verify"
        ],
    })))
}

/// POST /api/integrations/jira/secret — generate a JIRA webhook secret for this tenant.
pub async fn generate_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    use rand::Rng;
    let secret: String = rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s("JIRA_SECRET"))
        .item("secret", attr_s(&secret))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to store JIRA secret: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(tenant_id = %claims.tenant_id, "Generated JIRA webhook secret");
    Ok(Json(json!({ "secret": secret })))
}

/// DELETE /api/integrations/jira/secret — remove the JIRA webhook secret.
pub async fn delete_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("JIRA_SECRET"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete JIRA secret: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(tenant_id = %claims.tenant_id, "Deleted JIRA webhook secret");
    Ok(StatusCode::NO_CONTENT)
}

/// Load per-tenant JIRA secret from DynamoDB.
pub async fn load_jira_secret(state: &AppState, tenant_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("JIRA_SECRET".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| item.get("secret").and_then(|v| v.as_s().ok()).cloned())
}

/// POST /api/integrations/jira/check — validate a candidate Jira payload before wiring automation.
pub async fn validate_jira_integration_payload(
    Extension(_claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let top = |key: &str| body.get(key).and_then(|v| v.as_str()).map(str::to_string);
    let nested = |path1: &str, path2: &str| {
        body.get(path1)
            .and_then(|v| v.get(path2))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };

    let repo_owner = top("repo_owner").or_else(|| nested("coderhelm", "repo_owner"));
    let repo_name = top("repo_name").or_else(|| nested("coderhelm", "repo_name"));

    let installation_id = body
        .get("installation_id")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            body.get("coderhelm")
                .and_then(|v| v.get("installation_id"))
                .and_then(|v| v.as_u64())
        });
    let tenant_id = top("tenant_id").or_else(|| nested("coderhelm", "tenant_id"));

    let issue_key = body
        .get("issue")
        .and_then(|v| v.get("key"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let issue_summary = body
        .get("issue")
        .and_then(|v| v.get("fields"))
        .and_then(|v| v.get("summary"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("title"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let mut missing = Vec::new();
    if repo_owner.is_none() {
        missing.push("repo_owner (or coderhelm.repo_owner)");
    }
    if repo_name.is_none() {
        missing.push("repo_name (or coderhelm.repo_name)");
    }
    if installation_id.is_none() && tenant_id.is_none() {
        missing.push("installation_id (or tenant_id)");
    }
    if issue_key.is_none() {
        missing.push("issue.key (or issue.id)");
    }
    if issue_summary.is_none() {
        missing.push("issue.fields.summary (or issue.title)");
    }

    let valid = missing.is_empty();

    Ok(Json(json!({
        "valid": valid,
        "missing": missing,
        "normalized_preview": {
            "repo_owner": repo_owner,
            "repo_name": repo_name,
            "installation_id": installation_id,
            "tenant_id": tenant_id,
            "ticket_id": issue_key,
            "title": issue_summary,
            "event": body.get("webhookEvent").and_then(|v| v.as_str()).unwrap_or("unknown")
        },
        "next_step": if valid {
            "Payload shape looks good. Point Jira automation/webhook to /webhooks/jira."
        } else {
            "Fix missing fields above and run this check again."
        }
    })))
}

/// POST /api/repos/:owner/:name — update repo config.
pub async fn update_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let enabled = body["enabled"].as_bool().unwrap_or(true);

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s(&format!("REPO#{repo}")))
        .item("repo_name", attr_s(&repo))
        .item(
            "enabled",
            aws_sdk_dynamodb::types::AttributeValue::Bool(enabled),
        )
        .item("ticket_source", attr_s("github"));

    if enabled {
        put = put.item("onboard_status", attr_s("pending"));
    }

    put.send().await.map_err(|e| {
        error!("Failed to update repo: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // When enabling a repo, trigger onboarding to generate voice, instructions, etc.
    if enabled {
        let installation_id = claims
            .tenant_id
            .strip_prefix("TENANT#")
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);

        let message = WorkerMessage::Onboard(OnboardMessage {
            tenant_id: claims.tenant_id.clone(),
            installation_id,
            repos: vec![OnboardRepo {
                owner: owner.clone(),
                name: name.clone(),
                default_branch: "main".to_string(),
            }],
        });

        if let Ok(body) = serde_json::to_string(&message) {
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched onboarding for newly enabled repo");
        }

        // Trigger per-repo infrastructure analysis
        let per_repo_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            tenant_id: claims.tenant_id.clone(),
            triggered_by: claims.github_login.clone(),
            repo: Some(repo.clone()),
        });
        if let Ok(body) = serde_json::to_string(&per_repo_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let sk = format!("INFRA#REPO#{repo}");
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.table_name)
                .item("pk", attr_s(&claims.tenant_id))
                .item("sk", attr_s(&sk))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched per-repo infra analysis");
        }

        // Also trigger tenant-wide infrastructure analysis
        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            tenant_id: claims.tenant_id.clone(),
            triggered_by: claims.github_login.clone(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.table_name)
                .item("pk", attr_s(&claims.tenant_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched tenant-wide infra analysis");
        }
    } else {
        // When disabling a repo, remove its per-repo infra and refresh tenant-wide
        let sk = format!("INFRA#REPO#{repo}");
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s(&sk))
            .send()
            .await;

        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            tenant_id: claims.tenant_id.clone(),
            triggered_by: claims.github_login.clone(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.table_name)
                .item("pk", attr_s(&claims.tenant_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched infra refresh after repo disable");
        }
    }

    Ok(StatusCode::OK)
}

/// DELETE /api/repos/:owner/:name — remove a repo from this tenant.
pub async fn delete_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&format!("REPO#{repo}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete repo: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/stats — pre-computed analytics (O(1) read from analytics table).
pub async fn get_stats(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Read current month + all-time aggregates in parallel
    let now = chrono::Utc::now();
    let month_period = now.format("%Y-%m").to_string();

    let (month_result, alltime_result) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&claims.tenant_id))
            .key("period", attr_s(&month_period))
            .send(),
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&claims.tenant_id))
            .key("period", attr_s("ALL_TIME"))
            .send()
    );

    let month = month_result.map_err(|e| {
        error!("Failed to read monthly stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let alltime = alltime_result.map_err(|e| {
        error!("Failed to read all-time stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let extract = |item: Option<
        &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    >| {
        let total: u64 = item
            .and_then(|i| i.get("total_runs"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let completed: u64 = item
            .and_then(|i| i.get("completed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let failed: u64 = item
            .and_then(|i| i.get("failed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let cost: f64 = item
            .and_then(|i| i.get("total_cost_usd"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0.0);
        let tokens_in: u64 = item
            .and_then(|i| i.get("total_tokens_in"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let tokens_out: u64 = item
            .and_then(|i| i.get("total_tokens_out"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        json!({
            "total_runs": total,
            "completed": completed,
            "failed": failed,
            "in_progress": total.saturating_sub(completed + failed),
            "total_cost_usd": cost,
            "total_tokens_in": tokens_in,
            "total_tokens_out": tokens_out,
            "merge_rate": if total > 0 { completed as f64 / total as f64 } else { 0.0 },
        })
    };

    Ok(Json(json!({
        "month": extract(month.item()),
        "all_time": extract(alltime.item()),
    })))
}

/// GET /api/stats/history — last 6 months of analytics for charts.
pub async fn get_stats_history(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now();
    // Build period keys for the last 6 months
    let periods: Vec<String> = (0..6)
        .map(|i| {
            let d = now - chrono::Duration::days(i * 30);
            d.format("%Y-%m").to_string()
        })
        .collect();

    // Batch get all 6 months
    let mut months = Vec::new();
    for period in &periods {
        let result = state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&claims.tenant_id))
            .key("period", attr_s(period))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to read stats for {period}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let item = result.item();
        let get_n = |key: &str| -> u64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0)
        };
        let get_f = |key: &str| -> f64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0.0)
        };
        months.push(json!({
            "period": period,
            "total_runs": get_n("total_runs"),
            "completed": get_n("completed"),
            "failed": get_n("failed"),
            "total_cost_usd": get_f("total_cost_usd"),
            "total_tokens_in": get_n("total_tokens_in"),
            "total_tokens_out": get_n("total_tokens_out"),
        }));
    }

    // Reverse so oldest is first (for chart x-axis)
    months.reverse();

    Ok(Json(json!({ "months": months })))
}

// ─── Notification preferences ───────────────────────────────────────

/// GET /api/notifications — get notification preferences for calling user.
pub async fn get_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prefs = match result.item() {
        Some(item) => NotificationPrefs {
            email_run_complete: item
                .get("email_run_complete")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            email_run_failed: item
                .get("email_run_failed")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            email_weekly_summary: item
                .get("email_weekly_summary")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
        },
        None => NotificationPrefs::default(),
    };

    Ok(Json(json!({
        "email_run_complete": prefs.email_run_complete,
        "email_run_failed": prefs.email_run_failed,
        "email_weekly_summary": prefs.email_weekly_summary,
    })))
}

/// PUT /api/notifications — update notification preferences.
pub async fn update_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s(&sk))
        .item(
            "email_run_complete",
            attr_bool(body["email_run_complete"].as_bool().unwrap_or(true)),
        )
        .item(
            "email_run_failed",
            attr_bool(body["email_run_failed"].as_bool().unwrap_or(true)),
        )
        .item(
            "email_weekly_summary",
            attr_bool(body["email_weekly_summary"].as_bool().unwrap_or(true)),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Instructions ───────────────────────────────────────────────────

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_bool(val: bool) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::Bool(val)
}

const MAX_INSTRUCTIONS_BYTES: usize = 10_240; // 10KB limit

/// GET /api/instructions/global — get global custom instructions.
pub async fn get_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.tenant_id, "INSTRUCTIONS#GLOBAL").await
}

/// PUT /api/instructions/global — update global custom instructions.
pub async fn update_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.tenant_id, "INSTRUCTIONS#GLOBAL", content).await
}

/// GET /api/instructions/repo/:owner/:name — get per-repo custom instructions.
pub async fn get_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/instructions/repo/:owner/:name — update per-repo custom instructions.
pub async fn update_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    update_instructions_inner(&state, &claims.tenant_id, &sk, content).await
}

/// Validate repo path parameter to prevent DynamoDB key injection.
fn validate_repo_name(repo: &str) -> Result<(), StatusCode> {
    if repo.is_empty()
        || repo.len() > 200
        || repo.contains("..")
        || repo.contains('\0')
        || !repo
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

async fn get_instructions_inner(
    state: &AppState,
    tenant_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let content = result
        .item()
        .and_then(|item| item.get("content"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();

    Ok(Json(json!({ "content": content })))
}

async fn update_instructions_inner(
    state: &AppState,
    tenant_id: &str,
    sk: &str,
    content: &str,
) -> Result<StatusCode, StatusCode> {
    if content.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(tenant_id))
        .item("sk", attr_s(sk))
        .item("content", attr_s(content))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Must-rules ─────────────────────────────────────────────────────

const MAX_RULES_BYTES: usize = 10_240; // 10KB limit

/// GET /api/rules/global — get global must-rules.
pub async fn get_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_rules_inner(&state, &claims.tenant_id, "RULES#GLOBAL").await
}

/// PUT /api/rules/global — update global must-rules.
pub async fn update_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    update_rules_inner(&state, &claims.tenant_id, "RULES#GLOBAL", rules).await
}

/// GET /api/rules/repo/:owner/:name — get per-repo must-rules.
pub async fn get_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("RULES#REPO#{repo}");
    get_rules_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/rules/repo/:owner/:name — update per-repo must-rules.
pub async fn update_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    let sk = format!("RULES#REPO#{repo}");
    update_rules_inner(&state, &claims.tenant_id, &sk, rules).await
}

async fn get_rules_inner(
    state: &AppState,
    tenant_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rules: Vec<String> = result
        .item()
        .and_then(|item| item.get("rules"))
        .and_then(|v| v.as_l().ok())
        .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();

    Ok(Json(json!({ "rules": rules })))
}

async fn update_rules_inner(
    state: &AppState,
    tenant_id: &str,
    sk: &str,
    rules: &[Value],
) -> Result<StatusCode, StatusCode> {
    let rule_strings: Vec<String> = rules
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let serialized = serde_json::to_string(&rule_strings).unwrap_or_default();
    if serialized.len() > MAX_RULES_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let rule_attrs: Vec<aws_sdk_dynamodb::types::AttributeValue> = rule_strings
        .iter()
        .map(|s| aws_sdk_dynamodb::types::AttributeValue::S(s.clone()))
        .collect();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(tenant_id))
        .item("sk", attr_s(sk))
        .item(
            "rules",
            aws_sdk_dynamodb::types::AttributeValue::L(rule_attrs),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Voice settings ─────────────────────────────────────────────────

/// GET /api/voice/global — get global voice settings.
pub async fn get_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.tenant_id, "VOICE#GLOBAL").await
}

/// PUT /api/voice/global — update global voice settings.
pub async fn update_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.tenant_id, "VOICE#GLOBAL", content).await
}

/// GET /api/voice/repo/:owner/:name — get voice/tone settings for a repo.
pub async fn get_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("VOICE#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/voice/repo/:owner/:name — update voice/tone settings for a repo.
pub async fn update_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("VOICE#REPO#{repo}");
    update_instructions_inner(&state, &claims.tenant_id, &sk, content).await
}

// ─── Agents context ─────────────────────────────────────────────────

/// GET /api/agents/global — get global agents context.
pub async fn get_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.tenant_id, "AGENTS#GLOBAL").await
}

/// PUT /api/agents/global — update global agents context.
pub async fn update_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.tenant_id, "AGENTS#GLOBAL", content).await
}

/// GET /api/agents/repo/:owner/:name — get agents context for a repo.
pub async fn get_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("AGENTS#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/agents/repo/:owner/:name — update agents context for a repo.
pub async fn update_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("AGENTS#REPO#{repo}");
    update_instructions_inner(&state, &claims.tenant_id, &sk, content).await
}

/// POST /api/repos/:owner/:name/regenerate — re-run onboard (voice + agents) for a repo.
pub async fn regenerate_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.tenant_id).await?;
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let installation_id = claims
        .tenant_id
        .strip_prefix("TENANT#")
        .unwrap_or("0")
        .parse::<u64>()
        .unwrap_or(0);

    let message = WorkerMessage::Onboard(OnboardMessage {
        tenant_id: claims.tenant_id.clone(),
        installation_id,
        repos: vec![OnboardRepo {
            owner: parts[0].to_string(),
            name: parts[1].to_string(),
            default_branch: "main".to_string(),
        }],
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize onboard message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send regenerate message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(repo = %repo, "Dispatched regeneration to SQS");
    Ok(Json(json!({ "status": "regenerating" })))
}

/// GET /api/settings/budget — get monthly budget cap.
pub async fn get_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("SETTINGS#BUDGET"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch budget: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let max_budget_cents = result
        .item()
        .and_then(|i| i.get("max_budget_cents"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0); // 0 = no cap

    Ok(Json(json!({ "max_budget_cents": max_budget_cents })))
}

/// PUT /api/settings/budget — set monthly budget cap.
pub async fn update_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let max_budget_cents = body["max_budget_cents"].as_u64().unwrap_or(0);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s("SETTINGS#BUDGET"))
        .item(
            "max_budget_cents",
            aws_sdk_dynamodb::types::AttributeValue::N(max_budget_cents.to_string()),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update budget: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/settings/workflow — get workflow preferences.
pub async fn get_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("SETTINGS#WORKFLOW"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let commit_openspec = result
        .item()
        .and_then(|i| i.get("commit_openspec"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false); // default: off

    Ok(Json(json!({ "commit_openspec": commit_openspec })))
}

/// PUT /api/settings/workflow — update workflow preferences.
pub async fn update_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let commit_openspec = body["commit_openspec"].as_bool().unwrap_or(true);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s("SETTINGS#WORKFLOW"))
        .item(
            "commit_openspec",
            aws_sdk_dynamodb::types::AttributeValue::Bool(commit_openspec),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/health — system health check (stale runs, DLQ depth, queue depths).
pub async fn health(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now();
    let mut checks: Vec<Value> = Vec::new();
    let mut status = "healthy";

    // 1. Crashed runs — "running" for over 10 minutes (worker likely died)
    let crash_cutoff = (now - chrono::Duration::minutes(10)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .filter_expression("#s = :running AND created_at < :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.tenant_id))
        .expression_attribute_values(":running", attr_s("running"))
        .expression_attribute_values(":cutoff", attr_s(&crash_cutoff))
        .limit(20)
        .send()
        .await
    {
        let crashed: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = crashed.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "crashed_runs",
            "status": if count == 0 { "ok" } else { "critical" },
            "count": count,
            "items": crashed,
        }));
    }

    // 2. Stale queued — sitting in queue for over 30 minutes
    let stale_cutoff = (now - chrono::Duration::minutes(30)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .filter_expression("#s = :queued AND created_at < :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.tenant_id))
        .expression_attribute_values(":queued", attr_s("queued"))
        .expression_attribute_values(":cutoff", attr_s(&stale_cutoff))
        .limit(20)
        .send()
        .await
    {
        let stale_runs: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = stale_runs.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "stale_queued",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": stale_runs,
        }));
    }

    // 3. DLQ depth
    if !state.config.dlq_url.is_empty() {
        if let Ok(attrs) = state
            .sqs
            .get_queue_attributes()
            .queue_url(&state.config.dlq_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
        {
            let depth: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            if depth > 0 {
                status = "degraded";
            }
            checks.push(json!({
                "name": "dlq",
                "status": if depth == 0 { "ok" } else { "critical" },
                "depth": depth,
            }));
        }
    }

    // 4. Queue depths (tickets, ci-fix, feedback)
    for (name, url) in [
        ("tickets", &state.config.ticket_queue_url),
        ("ci_fix", &state.config.ci_fix_queue_url),
        ("feedback", &state.config.feedback_queue_url),
    ] {
        if let Ok(attrs) = state
            .sqs
            .get_queue_attributes()
            .queue_url(url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(
                aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .send()
            .await
        {
            let visible: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let in_flight: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(
                        &aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
                    )
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            checks.push(json!({
                "name": name,
                "status": "ok",
                "visible": visible,
                "in_flight": in_flight,
            }));
        }
    }

    // 5. Recent failed runs (last 24h)
    let error_cutoff = (now - chrono::Duration::hours(24)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("tenant_id = :tid")
        .filter_expression("#s = :failed AND created_at > :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.tenant_id))
        .expression_attribute_values(":failed", attr_s("failed"))
        .expression_attribute_values(":cutoff", attr_s(&error_cutoff))
        .limit(50)
        .send()
        .await
    {
        let failed_runs: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "error": item.get("error").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = failed_runs.len();
        checks.push(json!({
            "name": "failed_runs_24h",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": failed_runs,
        }));
    }

    Ok(Json(json!({
        "status": status,
        "checked_at": now.to_rfc3339(),
        "checks": checks,
    })))
}
