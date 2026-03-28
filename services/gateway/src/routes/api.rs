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
        "files_modified": item.get("files_modified").and_then(|v| v.as_l().ok()).map(|list| list.iter().filter_map(|v| v.as_s().ok().map(|s| s.as_str())).collect::<Vec<_>>()),
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
    let enabled_repo_count = repo_items
        .iter()
        .filter(|item| {
            item.get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true)
        })
        .count();

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

    let secret_configured = state.secrets.jira_webhook_secret.is_some();
    let ready = secret_configured && enabled_repo_count > 0;

    Ok(Json(json!({
        "ready": ready,
        "secret_configured": secret_configured,
        "configured_repos": {
            "total": repo_count,
            "enabled": enabled_repo_count
        },
        "jira_events_seen": jira_seen,
        "recommended_flow": "Jira Automation rule -> Send web request -> /webhooks/jira",
        "checklist": [
            "Use issue events: jira:issue_created and jira:issue_updated",
            "Keep webhook body included (do not exclude body)",
            "Configure secret in Jira webhook and d3ftly secrets",
            "Map repo_owner and repo_name in the payload",
            "Return 2xx quickly and process async (already handled by queueing)"
        ],
        "payload_template": {
            "repo_owner": "your-org",
            "repo_name": "your-repo",
            "installation_id": 123456,
            "issue": {
                "key": "PROJ-123",
                "fields": {
                    "summary": "Implement feature",
                    "description": "Acceptance criteria"
                }
            }
        },
        "check_endpoint": "/api/integrations/jira/check",
        "intake_endpoint": "/webhooks/jira"
    })))
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

    let repo_owner = top("repo_owner").or_else(|| nested("d3ftly", "repo_owner"));
    let repo_name = top("repo_name").or_else(|| nested("d3ftly", "repo_name"));

    let installation_id = body
        .get("installation_id")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            body.get("d3ftly")
                .and_then(|v| v.get("installation_id"))
                .and_then(|v| v.as_u64())
        });
    let tenant_id = top("tenant_id").or_else(|| nested("d3ftly", "tenant_id"));

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
        missing.push("repo_owner (or d3ftly.repo_owner)");
    }
    if repo_name.is_none() {
        missing.push("repo_name (or d3ftly.repo_name)");
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

/// GET /api/rules/repo/:repo — get per-repo must-rules.
pub async fn get_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo_name(&repo)?;
    let sk = format!("RULES#REPO#{repo}");
    get_rules_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/rules/repo/:repo — update per-repo must-rules.
pub async fn update_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
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

/// GET /api/voice/repo/:repo — get voice/tone settings for a repo.
pub async fn get_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo_name(&repo)?;
    let sk = format!("VOICE#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/voice/repo/:repo — update voice/tone settings for a repo.
pub async fn update_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("VOICE#REPO#{repo}");
    update_instructions_inner(&state, &claims.tenant_id, &sk, content).await
}

// ─── Agents context ─────────────────────────────────────────────────

/// GET /api/agents/repo/:repo — get agents context for a repo.
pub async fn get_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo_name(&repo)?;
    let sk = format!("AGENTS#REPO#{repo}");
    get_instructions_inner(&state, &claims.tenant_id, &sk).await
}

/// PUT /api/agents/repo/:repo — update agents context for a repo.
pub async fn update_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(repo): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("AGENTS#REPO#{repo}");
    update_instructions_inner(&state, &claims.tenant_id, &sk, content).await
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
