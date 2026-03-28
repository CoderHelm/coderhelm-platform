use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

use crate::models::{Claims, NotificationPrefs};
use crate::AppState;

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
        "files_modified": item.get("files_modified").and_then(|v| v.as_l().ok()),
        "duration_s": item.get("duration_s").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "error": item.get("error").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
    })))
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
            })
        })
        .collect();

    Ok(Json(json!({ "repos": repos })))
}

/// POST /api/repos/:repo — update repo config.
pub async fn update_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    validate_repo_name(&repo)?;
    let enabled = body["enabled"].as_bool().unwrap_or(true);

    state
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
        .item("ticket_source", attr_s("github"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update repo: {e}");
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

    let extract = |item: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>| {
        let total: u64 = item.and_then(|i| i.get("total_runs")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0);
        let completed: u64 = item.and_then(|i| i.get("completed")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0);
        let failed: u64 = item.and_then(|i| i.get("failed")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0);
        let cost: f64 = item.and_then(|i| i.get("total_cost_usd")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0.0);
        let tokens_in: u64 = item.and_then(|i| i.get("total_tokens_in")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0);
        let tokens_out: u64 = item.and_then(|i| i.get("total_tokens_out")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse().ok()).unwrap_or(0);
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
                .unwrap_or(true),
            email_run_failed: item
                .get("email_run_failed")
                .and_then(|v| v.as_bool().ok())
                .unwrap_or(true),
            email_weekly_summary: item
                .get("email_weekly_summary")
                .and_then(|v| v.as_bool().ok())
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

/// GET /api/instructions/repo/:repo — get per-repo custom instructions.
pub async fn get_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo_name(&repo)?;
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/instructions/repo/:repo — update per-repo custom instructions.
pub async fn update_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
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
        || !repo.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
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
        .item(
            "updated_at",
            attr_s(&chrono::Utc::now().to_rfc3339()),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}
