use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

use crate::models::{Claims, PlanExecuteMessage, WorkerMessage};
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl ToString) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// GET /api/plans — list all plans for the tenant.
pub async fn list_plans(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("PLAN#"))
        .filter_expression("attribute_exists(plan_id)")
        .scan_index_forward(false)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plans: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let plans: Vec<Value> = result
        .items()
        .iter()
        .filter(|item| {
            // Only plan items (not task items which have task_id)
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            !sk.contains("#TASK#")
        })
        .map(plan_from_item)
        .collect();

    Ok(Json(json!({ "plans": plans })))
}

/// POST /api/plans — create a new plan.
pub async fn create_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.tenant_id).await?;
    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    let description = body["description"].as_str().unwrap_or("");
    let repo = body["repo"].as_str().unwrap_or("");

    if title.is_empty() || title.len() > 500 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let plan_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("PLAN#{plan_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s(&sk))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("repo", attr_s(repo))
        .item("status", attr_s("draft"))
        .item("task_count", attr_n(0))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Create tasks if provided inline
    if let Some(tasks) = body["tasks"].as_array() {
        for (i, task) in tasks.iter().enumerate() {
            let task_title = task["title"].as_str().unwrap_or("Untitled task");
            let task_desc = task["description"].as_str().unwrap_or("");
            let task_criteria = task["acceptance_criteria"].as_str().unwrap_or("");
            let task_repo = task["repo"].as_str().unwrap_or("");
            let task_id = ulid::Ulid::new().to_string();
            let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

            let mut put = state
                .dynamo
                .put_item()
                .table_name(&state.config.table_name)
                .item("pk", attr_s(&claims.tenant_id))
                .item("sk", attr_s(&task_sk))
                .item("plan_id", attr_s(&plan_id))
                .item("task_id", attr_s(&task_id))
                .item("title", attr_s(task_title))
                .item("description", attr_s(task_desc))
                .item("acceptance_criteria", attr_s(task_criteria))
                .item("status", attr_s("draft"))
                .item("task_order", attr_n(i))
                .item("created_at", attr_s(&now));

            if !task_repo.is_empty() {
                put = put.item("repo", attr_s(task_repo));
            }

            put.send().await.map_err(|e| {
                error!("Failed to create task: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }

        // Update task count
        state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s(&sk))
            .update_expression("SET task_count = :c")
            .expression_attribute_values(":c", attr_n(tasks.len()))
            .send()
            .await
            .ok();
    }

    // Track plan usage and report overage to Stripe
    track_plan_usage(&state, &claims.tenant_id).await;

    Ok(Json(json!({ "plan_id": plan_id })))
}

/// Increment total_plans in analytics (plans are unlimited, no overage billing).
async fn track_plan_usage(state: &Arc<AppState>, tenant_id: &str) {
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(tenant_id))
            .key("period", attr_s(period))
            .update_expression("ADD total_plans :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }
}

/// GET /api/plans/:plan_id — get plan with all tasks.
pub async fn get_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Separate plan from tasks
    let mut plan_data: Option<Value> = None;
    let mut tasks: Vec<Value> = Vec::new();

    for item in items {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            tasks.push(task_from_item(item));
        } else {
            plan_data = Some(plan_from_item(item));
        }
    }

    let mut plan = plan_data.ok_or(StatusCode::NOT_FOUND)?;
    // Sort tasks by order
    tasks.sort_by_key(|t| t["order"].as_u64().unwrap_or(0));
    plan["tasks"] = json!(tasks);

    Ok(Json(plan))
}

/// PUT /api/plans/:plan_id — update plan metadata.
pub async fn update_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    validate_plan_id(&plan_id)?;
    let sk = format!("PLAN#{plan_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let mut update_parts = vec!["updated_at = :now".to_string()];
    let mut expr_values = vec![(":now".to_string(), attr_s(&now))];

    if let Some(title) = body["title"].as_str() {
        update_parts.push("title = :t".to_string());
        expr_values.push((":t".to_string(), attr_s(title)));
    }
    if let Some(desc) = body["description"].as_str() {
        update_parts.push("description = :d".to_string());
        expr_values.push((":d".to_string(), attr_s(desc)));
    }
    if let Some(status) = body["status"].as_str() {
        update_parts.push("#st = :s".to_string());
        expr_values.push((":s".to_string(), attr_s(status)));
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&sk))
        .update_expression(&update_expr)
        .condition_expression("attribute_exists(pk)");

    // "status" is a reserved word in DynamoDB
    if body["status"].is_string() {
        req = req.expression_attribute_names("#st", "status");
    }

    for (name, val) in expr_values {
        req = req.expression_attribute_values(&name, val);
    }

    req.send().await.map_err(|e| {
        error!("Failed to update plan: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}

/// DELETE /api/plans/:plan_id — delete plan and all tasks.
pub async fn delete_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<StatusCode, StatusCode> {
    validate_plan_id(&plan_id)?;

    // Query all items for this plan (plan + tasks)
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .projection_expression("pk, sk")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan for deletion: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Delete each item
    for item in result.items() {
        let pk = item
            .get("pk")
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let sk = item
            .get("sk")
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        state
            .dynamo
            .delete_item()
            .table_name(&state.config.table_name)
            .key("pk", pk)
            .key("sk", sk)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to delete plan item: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/tasks — add a task to a plan.
pub async fn add_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    let description = body["description"].as_str().unwrap_or("");
    let acceptance_criteria = body["acceptance_criteria"].as_str().unwrap_or("");
    let task_repo = body["repo"].as_str().unwrap_or("");
    let order = body["order"].as_u64().unwrap_or(0);

    let task_id = ulid::Ulid::new().to_string();
    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&claims.tenant_id))
        .item("sk", attr_s(&task_sk))
        .item("plan_id", attr_s(&plan_id))
        .item("task_id", attr_s(&task_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("acceptance_criteria", attr_s(acceptance_criteria))
        .item("status", attr_s("draft"))
        .item("task_order", attr_n(order))
        .item("created_at", attr_s(&now));

    if !task_repo.is_empty() {
        put = put.item("repo", attr_s(task_repo));
    }

    put.send().await.map_err(|e| {
        error!("Failed to create task: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Increment task count
    let plan_sk = format!("PLAN#{plan_id}");
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression(
            "SET task_count = if_not_exists(task_count, :zero) + :one, updated_at = :now",
        )
        .expression_attribute_values(":zero", attr_n(0))
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .ok();

    Ok(Json(json!({ "task_id": task_id })))
}

/// PUT /api/plans/:plan_id/tasks/:task_id — update a task.
pub async fn update_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

    let mut update_parts = Vec::new();
    let mut expr_values = Vec::new();

    if let Some(title) = body["title"].as_str() {
        update_parts.push("title = :t");
        expr_values.push((":t", attr_s(title)));
    }
    if let Some(desc) = body["description"].as_str() {
        update_parts.push("description = :d");
        expr_values.push((":d", attr_s(desc)));
    }
    if let Some(criteria) = body["acceptance_criteria"].as_str() {
        update_parts.push("acceptance_criteria = :ac");
        expr_values.push((":ac", attr_s(criteria)));
    }
    if let Some(repo) = body["repo"].as_str() {
        update_parts.push("repo = :r");
        expr_values.push((":r", attr_s(repo)));
    }
    if let Some(order) = body["order"].as_u64() {
        update_parts.push("task_order = :o");
        expr_values.push((":o", attr_n(order)));
    }
    if let Some(status) = body["status"].as_str() {
        update_parts.push("#st = :s");
        expr_values.push((":s", attr_s(status)));
    }

    if update_parts.is_empty() {
        return Ok(StatusCode::OK);
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&task_sk))
        .update_expression(&update_expr)
        .condition_expression("attribute_exists(pk)");

    if body["status"].is_string() {
        req = req.expression_attribute_names("#st", "status");
    }

    for (name, val) in expr_values {
        req = req.expression_attribute_values(name, val);
    }

    req.send().await.map_err(|e| {
        error!("Failed to update task: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}

/// DELETE /api/plans/:plan_id/tasks/:task_id — remove a task.
pub async fn delete_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&task_sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Decrement task count
    let plan_sk = format!("PLAN#{plan_id}");
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression(
            "SET task_count = if_not_exists(task_count, :one) - :one, updated_at = :now",
        )
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .ok();

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/tasks/:task_id/approve — approve a task for execution.
pub async fn approve_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.tenant_id).await?;
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, approved_at = :now, approved_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("approved"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.github_login))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to approve task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/tasks/:task_id/reject — reject a task.
pub async fn reject_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, rejected_at = :now, rejected_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("rejected"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.github_login))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to reject task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/execute — execute the plan (creates issues, queues tasks).
pub async fn execute_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.tenant_id).await?;
    validate_plan_id(&plan_id)?;

    // Get plan + all tasks
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Collect approved task IDs (worker reads full task data from DynamoDB)
    let mut approved_task_ids: Vec<(String, u64)> = Vec::new(); // (task_id, order)

    for item in result.items() {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            if status == "approved" {
                let tid = item
                    .get("task_id")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default();
                let order = item
                    .get("task_order")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0u64);
                approved_task_ids.push((tid, order));
            }
        }
    }

    if approved_task_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST); // No approved tasks
    }

    // Sort by order
    approved_task_ids.sort_by_key(|t| t.1);

    // Mark plan as executing
    let plan_sk = format!("PLAN#{plan_id}");
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression("SET #st = :s, executed_at = :now, executed_by = :by, updated_at = :now")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("executing"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.github_login))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update plan status: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Send SQS message for plan execution (worker creates issues + queues runs)
    let plan_msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        tenant_id: claims.tenant_id.clone(),
        plan_id: plan_id.clone(),
        triggered_by: claims.github_login.clone(),
        tasks: approved_task_ids
            .iter()
            .map(|(tid, _)| tid.clone())
            .collect(),
    });

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(serde_json::to_string(&plan_msg).map_err(|e| {
            error!("Failed to serialize plan message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send plan execute message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Mark each approved task as "queued"
    for (task_id, _) in &approved_task_ids {
        let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
        state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s(&task_sk))
            .update_expression("SET #st = :s")
            .expression_attribute_names("#st", "status")
            .expression_attribute_values(":s", attr_s("queued"))
            .send()
            .await
            .ok();
    }

    Ok(Json(json!({
        "status": "executing",
        "tasks_queued": approved_task_ids.len(),
    })))
}

// ─── Helpers ────────────────────────────────────────────────────────

fn validate_plan_id(id: &str) -> Result<(), StatusCode> {
    if id.is_empty() || id.len() > 30 || !id.chars().all(|c| c.is_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn plan_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    json!({
        "plan_id": item.get("plan_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "repo": item.get("repo").and_then(|v| v.as_s().ok()),
        "status": item.get("status").and_then(|v| v.as_s().ok()),
        "task_count": item.get("task_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
        "executed_at": item.get("executed_at").and_then(|v| v.as_s().ok()),
        "executed_by": item.get("executed_by").and_then(|v| v.as_s().ok()),
    })
}

fn task_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    json!({
        "task_id": item.get("task_id").and_then(|v| v.as_s().ok()),
        "plan_id": item.get("plan_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "acceptance_criteria": item.get("acceptance_criteria").and_then(|v| v.as_s().ok()),
        "status": item.get("status").and_then(|v| v.as_s().ok()),
        "repo": item.get("repo").and_then(|v| v.as_s().ok()),
        "order": item.get("task_order").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "issue_number": item.get("issue_number").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "issue_url": item.get("issue_url").and_then(|v| v.as_s().ok()),
        "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
        "approved_at": item.get("approved_at").and_then(|v| v.as_s().ok()),
        "approved_by": item.get("approved_by").and_then(|v| v.as_s().ok()),
        "rejected_at": item.get("rejected_at").and_then(|v| v.as_s().ok()),
        "rejected_by": item.get("rejected_by").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
    })
}

// ─── Plan Chat ────────────────────────────────────────────────────────

const PLAN_CHAT_SYSTEM: &str = r#"You are a planning assistant for Coderhelm, an autonomous AI coding agent.
Your job is to help the user break down a feature, epic, or large piece of work into a structured plan.

When the user describes what they want to build, you should:
1. If the user already specified the repo, tech constraints, and scope — go straight to generating the plan.
2. Only ask clarifying questions when truly critical info is missing (e.g. no repo specified at all).
3. Never ask more than 2 clarifying questions before generating a plan.

When generating the final plan, output it in this EXACT JSON format inside a code fence:

```json
{
  "title": "Short epic title",
  "description": "1-2 sentence overview",
  "repo": "owner/repo",
  "tasks": [
    {
      "title": "Concise task title",
      "description": "What to build and why. Be specific about files, APIs, UI components.",
      "acceptance_criteria": "- Bullet list\n- Of verifiable criteria",
      "order": 0
    }
  ]
}
```

Rules:
- Tasks should be independently implementable (one PR each)
- Order matters — Coderhelm works on them sequentially
- Each task title should be a GitHub issue title (imperative, max 60 chars)
- Acceptance criteria should be machine-verifiable where possible
- 3-10 tasks is ideal
- If the user mentions a specific repo, USE IT — don't ask again
- Be direct and action-oriented, not verbose"#;

/// POST /api/plans/chat — AI-powered plan generation chat.
pub async fn plan_chat(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let messages_input = body["messages"].as_array().ok_or(StatusCode::BAD_REQUEST)?;

    if messages_input.is_empty() || messages_input.len() > 20 {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Load org context to give the AI awareness of available repos
    let org_context = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("AGENTS#GLOBAL"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("content").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    // Build system prompt with org context
    let system_prompt = if org_context.is_empty() {
        PLAN_CHAT_SYSTEM.to_string()
    } else {
        format!("{PLAN_CHAT_SYSTEM}\n\nThe user's organization context:\n{org_context}")
    };

    // Convert messages to Bedrock format
    let mut bedrock_messages = Vec::new();
    for msg in messages_input {
        let role_str = msg["role"].as_str().unwrap_or("user");
        let content = msg["content"].as_str().unwrap_or("");
        if content.is_empty() {
            continue;
        }
        let role = match role_str {
            "assistant" => aws_sdk_bedrockruntime::types::ConversationRole::Assistant,
            _ => aws_sdk_bedrockruntime::types::ConversationRole::User,
        };
        let message = aws_sdk_bedrockruntime::types::Message::builder()
            .role(role)
            .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                content.to_string(),
            ))
            .build()
            .map_err(|e| {
                error!("Failed to build Bedrock message: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        bedrock_messages.push(message);
    }

    let response = state
        .bedrock
        .converse()
        .model_id(&state.config.model_id)
        .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            system_prompt,
        ))
        .set_messages(Some(bedrock_messages))
        .send()
        .await
        .map_err(|e| {
            error!("Bedrock converse failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let text = match response.output() {
        Some(aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg)) => msg
            .content()
            .iter()
            .find_map(|block| {
                if let aws_sdk_bedrockruntime::types::ContentBlock::Text(t) = block {
                    Some(t.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default(),
        _ => String::new(),
    };

    Ok(Json(json!({ "content": text })))
}
