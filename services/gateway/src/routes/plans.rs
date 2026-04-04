use aws_smithy_types::Document;
use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::{Claims, PlanExecuteMessage, PlanTaskContinueMessage, WorkerMessage};
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl ToString) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// GET /api/plans — list plans for the team (paginated).
pub async fn list_plans(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let limit: i32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .min(100);

    let mut query = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("PLAN#"))
        .filter_expression("attribute_exists(plan_id)")
        .scan_index_forward(false)
        .limit(limit);

    // Resume from cursor
    if let Some(cursor) = params.get("cursor").filter(|c| !c.is_empty()) {
        let lek = std::collections::HashMap::from([
            ("pk".to_string(), attr_s(&claims.team_id)),
            ("sk".to_string(), attr_s(cursor)),
        ]);
        query = query.set_exclusive_start_key(Some(lek));
    }

    let result = query.send().await.map_err(|e| {
        error!("Failed to query plans: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let plans: Vec<Value> = result
        .items()
        .iter()
        .filter(|item| {
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            !sk.contains("#TASK#")
        })
        .map(plan_from_item)
        .collect();

    let next_cursor = result
        .last_evaluated_key()
        .and_then(|k| k.get("sk"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());

    Ok(Json(json!({
        "plans": plans,
        "next_cursor": next_cursor,
    })))
}

/// POST /api/plans — create a new plan.
pub async fn create_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    let description = body["description"].as_str().unwrap_or("");
    let repo = body["repo"].as_str().unwrap_or("");
    let destination = match body["destination"].as_str() {
        Some("jira") => "jira",
        _ => "github",
    };

    if title.is_empty() || title.len() > 500 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let plan_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("PLAN#{plan_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("repo", attr_s(repo))
        .item("destination", attr_s(destination))
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
                .table_name(&state.config.plans_table_name)
                .item("pk", attr_s(&claims.team_id))
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
            if let Some(dest) = task["destination"].as_str().or(if destination.is_empty() {
                None
            } else {
                Some(destination)
            }) {
                put = put.item("destination", attr_s(dest));
            }
            if let Some(jp) = task["jira_project"].as_str() {
                put = put.item("jira_project", attr_s(jp));
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
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .update_expression("SET task_count = :c")
            .expression_attribute_values(":c", attr_n(tasks.len()))
            .send()
            .await
            .ok();
    }

    // Track plan usage and report overage to Stripe
    track_plan_usage(&state, &claims.team_id).await;

    Ok(Json(json!({ "plan_id": plan_id })))
}

/// Increment total_plans in analytics (plans are unlimited, no overage billing).
async fn track_plan_usage(state: &Arc<AppState>, team_id: &str) {
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(team_id))
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
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
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
    if let Some(dest) = body["destination"].as_str() {
        if dest == "github" || dest == "jira" {
            update_parts.push("destination = :dest".to_string());
            expr_values.push((":dest".to_string(), attr_s(dest)));
        }
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
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

    // Propagate destination change to all tasks
    if let Some(dest) = body["destination"].as_str() {
        if dest == "github" || dest == "jira" {
            let task_prefix = format!("PLAN#{plan_id}#TASK#");
            let result = state
                .dynamo
                .query()
                .table_name(&state.config.plans_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
                .expression_attribute_values(":pk", attr_s(&claims.team_id))
                .expression_attribute_values(":prefix", attr_s(&task_prefix))
                .projection_expression("pk, sk")
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to query tasks for destination update: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            for item in result.items() {
                if let Some(task_sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.plans_table_name)
                        .key("pk", attr_s(&claims.team_id))
                        .key("sk", attr_s(task_sk))
                        .update_expression("SET destination = :dest")
                        .expression_attribute_values(":dest", attr_s(dest))
                        .send()
                        .await;
                }
            }
        }
    }

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
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
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
            .table_name(&state.config.plans_table_name)
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
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
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
    if let Some(dest) = body["destination"].as_str() {
        put = put.item("destination", attr_s(dest));
    }
    if let Some(jp) = body["jira_project"].as_str() {
        put = put.item("jira_project", attr_s(jp));
    }
    if let Some(dep) = body["depends_on"].as_str() {
        if !dep.is_empty() {
            put = put.item("depends_on", attr_s(dep));
        }
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
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
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
    if let Some(dest) = body["destination"].as_str() {
        update_parts.push("destination = :dest");
        expr_values.push((":dest", attr_s(dest)));
    }
    if let Some(jp) = body["jira_project"].as_str() {
        update_parts.push("jira_project = :jp");
        expr_values.push((":jp", attr_s(jp)));
    }
    if let Some(dep) = body["depends_on"].as_str() {
        if dep.is_empty() {
            update_parts.push("depends_on = :dep");
            expr_values.push((":dep", attr_s("")));
        } else {
            update_parts.push("depends_on = :dep");
            expr_values.push((":dep", attr_s(dep)));
        }
    }

    if update_parts.is_empty() {
        return Ok(StatusCode::OK);
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
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
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
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
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
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
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, approved_at = :now, approved_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("approved"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
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
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, rejected_at = :now, rejected_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("rejected"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to reject task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/approve-and-execute — approve all draft tasks and execute.
pub async fn approve_all_and_execute(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    validate_plan_id(&plan_id)?;

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let now = chrono::Utc::now().to_rfc3339();
    let mut task_ids: Vec<(String, u64)> = Vec::new();

    for item in result.items() {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
        if !sk.contains("#TASK#") {
            continue;
        }
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
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

        // Approve draft tasks, include already-approved tasks
        if status == "draft" {
            let task_sk = format!("PLAN#{plan_id}#TASK#{tid}");
            state
                .dynamo
                .update_item()
                .table_name(&state.config.plans_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s(&task_sk))
                .update_expression("SET #st = :s, approved_at = :now, approved_by = :by")
                .expression_attribute_names("#st", "status")
                .expression_attribute_values(":s", attr_s("approved"))
                .expression_attribute_values(":now", attr_s(&now))
                .expression_attribute_values(":by", attr_s(&claims.display_name()))
                .send()
                .await
                .ok();
            task_ids.push((tid, order));
        } else if status == "approved" {
            task_ids.push((tid, order));
        }
    }

    if task_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    task_ids.sort_by_key(|t| t.1);

    // Mark plan as executing
    let plan_sk = format!("PLAN#{plan_id}");
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression("SET #st = :s, executed_at = :now, executed_by = :by, updated_at = :now")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("executing"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update plan status: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Send SQS message
    let plan_msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        triggered_by: claims.display_name(),
        tasks: task_ids.iter().map(|(tid, _)| tid.clone()).collect(),
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

    // Mark tasks as queued
    for (task_id, _) in &task_ids {
        let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
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
        "tasks_queued": task_ids.len(),
    })))
}

/// POST /api/plans/:plan_id/tasks/:task_id/force-run — force-run a waiting task.
pub async fn force_run_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    // Verify task is in "waiting" status
    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let status = item
        .item()
        .and_then(|i| i.get("status"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    if status != "waiting" {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Send PlanTaskContinue message to the worker
    let msg = WorkerMessage::PlanTaskContinue(PlanTaskContinueMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        tasks: vec![task_id.clone()],
    });

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(serde_json::to_string(&msg).map_err(|e| {
            error!("Failed to serialize force-run message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send force-run message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "status": "running" })))
}

/// POST /api/plans/:plan_id/execute — execute the plan (creates issues, queues tasks).
pub async fn execute_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    validate_plan_id(&plan_id)?;

    // Get plan + all tasks
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
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
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression("SET #st = :s, executed_at = :now, executed_by = :by, updated_at = :now")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("executing"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update plan status: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Send SQS message for plan execution (worker creates issues + queues runs)
    let plan_msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        triggered_by: claims.display_name(),
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
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
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
        "tokens_in": item.get("total_tokens_in").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "tokens_out": item.get("total_tokens_out").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "destination": item.get("destination").and_then(|v| v.as_s().ok()),
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
        "destination": item.get("destination").and_then(|v| v.as_s().ok()),
        "jira_project": item.get("jira_project").and_then(|v| v.as_s().ok()),
        "jira_ticket_key": item.get("jira_ticket_key").and_then(|v| v.as_s().ok()),
        "jira_ticket_url": item.get("jira_ticket_url").and_then(|v| v.as_s().ok()),
        "depends_on": item.get("depends_on").and_then(|v| v.as_s().ok()),
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

CRITICAL — Conversation continuity:
- When the user adds new requirements or follow-up messages, ALWAYS incorporate ALL previous requests from the entire conversation into the updated plan.
- If the user said "update X on dashboard" and then "update Y on landing page", the next plan MUST include BOTH tasks.
- Never forget or drop earlier requests. The plan should be the complete, cumulative result of everything discussed.
- When regenerating a plan, re-read the full conversation and include every distinct request as a task.

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
      "repo": "owner/repo",
      "order": 0
    }
  ]
}
```

Rules:
- A plan can span MULTIPLE repositories. The top-level "repo" is the primary repo.
- Each task has its own optional "repo" field — use it when a task targets a different repo than the top-level one.
- Tasks should be independently implementable (one PR each)
- Order matters — Coderhelm works on them sequentially
- Each task title should be a GitHub issue title (imperative, max 60 chars)
- Acceptance criteria should be machine-verifiable where possible
- 2-5 tasks is ideal. Fewer, bigger tasks are better than many small ones.
- Combine related work (e.g. code changes + README update + tests) into a single task
- Only split into separate tasks when there are truly independent streams of work
- If the user mentions a specific repo, USE IT — don't ask again
- Be direct and action-oriented, not verbose

Note: Each task will go through Coderhelm's full pipeline: OpenSpec generation
(proposal, design, tasks, spec documents), then implementation. Plan accordingly —
each task becomes a full engineering effort with its own spec and PR."#;

/// POST /api/plans/chat/token — Issue a short-lived JWT for the streaming endpoint.
/// The frontend calls this before opening the SSE stream to stream.coderhelm.com.
pub async fn stream_token(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Mint a short-lived token (5 minutes) scoped for streaming
    let token = crate::auth::jwt::create_token(
        &claims.sub,
        &claims.team_id,
        &claims.email,
        &claims.role,
        claims.github_login.as_deref(),
        &state.secrets.jwt_secret,
        300, // 5 minutes
    )
    .map_err(|e| {
        error!("Failed to create stream token: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(json!({ "token": token })))
}

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

    // Load org context, repo list, log analyzer flag, and enabled plugins in parallel
    let (org_context_result, repo_list_result, allow_plan_log_analyzer, enabled_plugins) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s("AGENTS#GLOBAL"))
            .send(),
        state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(&claims.team_id))
            .expression_attribute_values(":prefix", attr_s("REPO#"))
            .send(),
        load_allow_plan_log_analyzer(&state, &claims.team_id),
        load_enabled_plugins(&state, &claims.team_id),
    );

    let org_context = org_context_result
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("content").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    let repo_list: Vec<String> = repo_list_result
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| {
                    item.get("repo_name")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    // Conditionally load log analyzer context (depends on allow_plan_log_analyzer result)
    let log_analyzer_context = if allow_plan_log_analyzer {
        Some(load_log_analyzer_context(&state, &claims.team_id).await)
    } else {
        None
    };

    // Load MCP tool definitions from S3 cache for enabled plugins with credentials
    let mut mcp_tools: Vec<McpToolDef> = Vec::new();
    let mcp_proxy_fn = &state.config.mcp_proxy_function_name;
    if !mcp_proxy_fn.is_empty() {
        let cache_futures: Vec<_> = enabled_plugins
            .iter()
            .filter(|(_, has_creds, _)| *has_creds)
            .map(|(server_id, _, _)| {
                let s3 = &state.s3;
                let bucket = &state.config.bucket_name;
                async move {
                    let cache = load_mcp_tool_cache(s3, bucket, server_id).await;
                    (server_id.clone(), cache)
                }
            })
            .collect();
        let caches = futures::future::join_all(cache_futures).await;
        for (server_id, cache) in caches {
            if let Some(tools) = cache {
                for tool in tools {
                    mcp_tools.push(McpToolDef {
                        server_id: server_id.clone(),
                        name: tool["name"].as_str().unwrap_or("").to_string(),
                        description: tool["description"].as_str().unwrap_or("").to_string(),
                        input_schema: tool["inputSchema"].clone(),
                    });
                }
            }
        }
    }

    // Build system prompt with org context and repo list
    let mut system_prompt = PLAN_CHAT_SYSTEM.to_string();
    if !org_context.is_empty() {
        system_prompt.push_str(&format!(
            "\n\nThe user's organization context:\n{org_context}"
        ));
    }
    if !repo_list.is_empty() {
        // Limit to 50 repos to keep prompt size reasonable
        let repos_for_prompt: Vec<_> = repo_list.iter().take(50).collect();
        system_prompt.push_str(&format!(
            "\n\nAvailable repositories (use these exact names for the \"repo\" field):\n{}",
            repos_for_prompt
                .iter()
                .map(|r| format!("- {r}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    // Only add MCP instructions if there are tools available
    if !mcp_tools.is_empty() {
        let plugin_lines: Vec<String> = enabled_plugins
            .iter()
            .filter(|(_, has_creds, _)| *has_creds)
            .map(|(id, _, custom_prompt)| {
                let mut line = format!("- {id} (connected)");
                if let Some(prompt) = custom_prompt {
                    line.push_str(&format!(" — {prompt}"));
                }
                line
            })
            .collect();
        system_prompt.push_str(&format!(
            "\n\nYou have tool-call access to the following MCP servers right now. \
             You can invoke their tools directly during this conversation to look up \
             information, search documents, read pages, or query external services.\n\
             \n\
             IMPORTANT — Proactive tool use:\n\
             - BEFORE generating a plan, search connected tools for relevant context. \
             If Notion is connected, search for docs related to the user's request \
             (specs, config values, API keys references, design docs, requirements).\n\
             - If the user mentions anything that might be documented (IDs, credentials, \
             config, specs, designs, tickets), search the connected tools first.\n\
             - Don't ask the user for information that might already exist in their \
             connected tools — look it up yourself.\n\
             - When you find relevant context, incorporate it into the plan tasks \
             (e.g. specific IDs, endpoints, file paths, design references).\n\
             \n\
             Connected servers:\n{}",
            plugin_lines.join("\n")
        ));
    }
    if let Some(context) = log_analyzer_context.filter(|c| !c.is_empty()) {
        system_prompt.push_str(&format!(
            "\n\nAWS Log Analyzer context (enabled in workflow settings):\n{context}"
        ));
    }

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

    // Build Bedrock tool config from MCP tools
    let tool_config = if !mcp_tools.is_empty() {
        let specs: Vec<aws_sdk_bedrockruntime::types::Tool> = mcp_tools
            .iter()
            .map(|t| {
                aws_sdk_bedrockruntime::types::Tool::ToolSpec(
                    aws_sdk_bedrockruntime::types::ToolSpecification::builder()
                        .name(format!("{}__{}", t.server_id, t.name))
                        .description(&t.description)
                        .input_schema(aws_sdk_bedrockruntime::types::ToolInputSchema::Json(
                            json_to_document(&t.input_schema),
                        ))
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        Some(
            aws_sdk_bedrockruntime::types::ToolConfiguration::builder()
                .set_tools(Some(specs))
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    // Agentic loop — up to 5 tool-use turns for plan chat (gateway has 30s timeout)
    let max_turns = 5;
    let mut total_input_tokens: u64 = 0;
    let mut total_output_tokens: u64 = 0;
    let mut mcp_servers_used: std::collections::HashSet<String> = std::collections::HashSet::new();

    for turn in 0..max_turns {
        let mut req = state
            .bedrock
            .converse()
            .model_id(&state.config.model_id)
            .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
                system_prompt.clone(),
            ))
            .set_messages(Some(bedrock_messages.clone()));

        if let Some(ref tc) = tool_config {
            req = req.tool_config(tc.clone());
        }

        let response = req.send().await.map_err(|e| {
            error!("Bedrock converse failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

        // Track token usage
        if let Some(usage) = response.usage() {
            total_input_tokens += usage.input_tokens() as u64;
            total_output_tokens += usage.output_tokens() as u64;
        }

        let output_message = match response.output() {
            Some(aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg)) => msg.clone(),
            _ => {
                let state_clone = state.clone();
                let team_id_clone = claims.team_id.clone();
                tokio::spawn(async move {
                    track_chat_tokens(
                        &state_clone,
                        &team_id_clone,
                        total_input_tokens,
                        total_output_tokens,
                    )
                    .await;
                });
                return Ok(Json(json!({ "content": "" })));
            }
        };

        bedrock_messages.push(output_message.clone());

        // Check for tool use
        let tool_uses: Vec<_> = output_message
            .content()
            .iter()
            .filter_map(|block| {
                if let aws_sdk_bedrockruntime::types::ContentBlock::ToolUse(tu) = block {
                    Some(tu.clone())
                } else {
                    None
                }
            })
            .collect();

        if tool_uses.is_empty() {
            // No tool use — extract final text and return
            let text = output_message
                .content()
                .iter()
                .find_map(|block| {
                    if let aws_sdk_bedrockruntime::types::ContentBlock::Text(t) = block {
                        Some(t.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();

            {
                let state_clone = state.clone();
                let team_id_clone = claims.team_id.clone();
                tokio::spawn(async move {
                    track_chat_tokens(
                        &state_clone,
                        &team_id_clone,
                        total_input_tokens,
                        total_output_tokens,
                    )
                    .await;
                });
            }
            let servers: Vec<&str> = mcp_servers_used.iter().map(|s| s.as_str()).collect();
            return Ok(Json(json!({ "content": text, "mcp_servers": servers })));
        }

        // Execute MCP tool calls via proxy Lambda (in parallel)
        let tool_futures: Vec<_> = tool_uses.iter().map(|tool_use| {
            let full_name = tool_use.name().to_string();
            let input: Value = document_to_json(tool_use.input());
            let tool_use_id = tool_use.tool_use_id().to_string();
            let state_ref = &state;
            let team_id_ref = &claims.team_id;
            async move {
                let result = if let Some((server_id, tool_name)) = full_name.split_once("__") {
                    match invoke_mcp_tool(state_ref, team_id_ref, server_id, tool_name, &input).await {
                        Ok(result) => (Some(server_id.to_string()), result),
                        Err(e) => {
                            warn!(tool = full_name.as_str(), error = %e, "MCP tool call failed");
                            (Some(server_id.to_string()), json!(format!("Error: {e}")))
                        }
                    }
                } else {
                    (None, json!(format!("Unknown tool: {full_name}")))
                };
                (tool_use_id, result.0, result.1)
            }
        }).collect();
        let tool_results_raw = futures::future::join_all(tool_futures).await;

        let mut tool_results = Vec::new();
        for (tool_use_id, server_id, result) in tool_results_raw {
            if let Some(sid) = server_id {
                mcp_servers_used.insert(sid);
            }
            tool_results.push(aws_sdk_bedrockruntime::types::ContentBlock::ToolResult(
                aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                    .tool_use_id(tool_use_id)
                    .content(aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                        serde_json::to_string(&result).unwrap_or_default(),
                    ))
                    .build()
                    .map_err(|e| {
                        error!("Failed to build tool result: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?,
            ));
        }

        bedrock_messages.push(
            aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                .set_content(Some(tool_results))
                .build()
                .map_err(|e| {
                    error!("Failed to build tool result message: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
        );

        info!(turn = turn + 1, "Plan chat tool use turn complete");
    }

    // Hit max turns — extract last text
    let text = bedrock_messages
        .last()
        .and_then(|msg| {
            msg.content().iter().find_map(|block| {
                if let aws_sdk_bedrockruntime::types::ContentBlock::Text(t) = block {
                    Some(t.clone())
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    {
        let state_clone = state.clone();
        let team_id_clone = claims.team_id.clone();
        tokio::spawn(async move {
            track_chat_tokens(
                &state_clone,
                &team_id_clone,
                total_input_tokens,
                total_output_tokens,
            )
            .await;
        });
    }
    let servers: Vec<&str> = mcp_servers_used.iter().map(|s| s.as_str()).collect();
    Ok(Json(json!({ "content": text, "mcp_servers": servers })))
}

// ---------------------------------------------------------------------------
// Streaming variant: POST /api/plans/chat/stream → SSE
// ---------------------------------------------------------------------------

/// Send a single SSE event formatted as `event: {type}\ndata: {json}\n\n`.
fn sse_event(
    event_type: &str,
    data: Value,
) -> Result<axum::response::sse::Event, std::convert::Infallible> {
    Ok(axum::response::sse::Event::default()
        .event(event_type)
        .data(serde_json::to_string(&data).unwrap_or_default()))
}

pub async fn plan_chat_stream(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Result<
    axum::response::sse::Sse<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<axum::response::sse::Event, std::convert::Infallible>,
                    > + Send,
            >,
        >,
    >,
    StatusCode,
> {
    // Validate Bearer token (issued by /api/plans/chat/token)
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let claims = crate::auth::jwt::validate_token(token, &state.secrets.jwt_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let messages_input = body["messages"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    if messages_input.is_empty() || messages_input.len() > 20 {
        return Err(StatusCode::BAD_REQUEST);
    }

    // --- Same context loading as plan_chat (parallelized) ---
    let (org_context_result, repo_list_result, allow_plan_log_analyzer, enabled_plugins) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s("AGENTS#GLOBAL"))
            .send(),
        state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(&claims.team_id))
            .expression_attribute_values(":prefix", attr_s("REPO#"))
            .send(),
        load_allow_plan_log_analyzer(&state, &claims.team_id),
        load_enabled_plugins(&state, &claims.team_id),
    );

    let org_context = org_context_result
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("content").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    let repo_list: Vec<String> = repo_list_result
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| {
                    item.get("repo_name")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    let log_analyzer_context = if allow_plan_log_analyzer {
        Some(load_log_analyzer_context(&state, &claims.team_id).await)
    } else {
        None
    };

    // Load MCP tool definitions (before building system prompt so we can check if tools exist)
    let mut mcp_tools: Vec<McpToolDef> = Vec::new();
    let mcp_proxy_fn = &state.config.mcp_proxy_function_name;
    if !mcp_proxy_fn.is_empty() {
        let cache_futures: Vec<_> = enabled_plugins
            .iter()
            .filter(|(_, has_creds, _)| *has_creds)
            .map(|(server_id, _, _)| {
                let s3 = &state.s3;
                let bucket = &state.config.bucket_name;
                async move {
                    let cache = load_mcp_tool_cache(s3, bucket, server_id).await;
                    (server_id.clone(), cache)
                }
            })
            .collect();
        let caches = futures::future::join_all(cache_futures).await;
        for (server_id, cache) in caches {
            if let Some(tools) = cache {
                for tool in tools {
                    mcp_tools.push(McpToolDef {
                        server_id: server_id.clone(),
                        name: tool["name"].as_str().unwrap_or("").to_string(),
                        description: tool["description"].as_str().unwrap_or("").to_string(),
                        input_schema: tool["inputSchema"].clone(),
                    });
                }
            }
        }
    }

    let mut system_prompt = PLAN_CHAT_SYSTEM.to_string();
    if !org_context.is_empty() {
        system_prompt.push_str(&format!(
            "\n\nThe user's organization context:\n{org_context}"
        ));
    }
    if !repo_list.is_empty() {
        // Limit to 50 repos to keep prompt size reasonable
        let repos_for_prompt: Vec<_> = repo_list.iter().take(50).collect();
        system_prompt.push_str(&format!(
            "\n\nAvailable repositories (use these exact names for the \"repo\" field):\n{}",
            repos_for_prompt
                .iter()
                .map(|r| format!("- {r}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    // Only add MCP instructions if there are tools available
    if !mcp_tools.is_empty() {
        let plugin_lines: Vec<String> = enabled_plugins
            .iter()
            .filter(|(_, has_creds, _)| *has_creds)
            .map(|(id, _, custom_prompt)| {
                let mut line = format!("- {id} (connected)");
                if let Some(prompt) = custom_prompt {
                    line.push_str(&format!(" — {prompt}"));
                }
                line
            })
            .collect();
        system_prompt.push_str(&format!(
            "\n\nYou have tool-call access to the following MCP servers right now. \
             You can invoke their tools directly during this conversation to look up \
             information, search documents, read pages, or query external services.\n\
             \n\
             IMPORTANT — Proactive tool use:\n\
             - BEFORE generating a plan, search connected tools for relevant context. \
             If Notion is connected, search for docs related to the user's request \
             (specs, config values, API keys references, design docs, requirements).\n\
             - If the user mentions anything that might be documented (IDs, credentials, \
             config, specs, designs, tickets), search the connected tools first.\n\
             - Don't ask the user for information that might already exist in their \
             connected tools — look it up yourself.\n\
             - When you find relevant context, incorporate it into the plan tasks \
             (e.g. specific IDs, endpoints, file paths, design references).\n\
             \n\
             Connected servers:\n{}",
            plugin_lines.join("\n")
        ));
    }
    if let Some(context) = log_analyzer_context.filter(|c| !c.is_empty()) {
        system_prompt.push_str(&format!("\n\nAWS Log Analyzer context:\n{context}"));
    }

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

    let tool_config = if !mcp_tools.is_empty() {
        let specs: Vec<aws_sdk_bedrockruntime::types::Tool> = mcp_tools
            .iter()
            .map(|t| {
                aws_sdk_bedrockruntime::types::Tool::ToolSpec(
                    aws_sdk_bedrockruntime::types::ToolSpecification::builder()
                        .name(format!("{}__{}", t.server_id, t.name))
                        .description(&t.description)
                        .input_schema(aws_sdk_bedrockruntime::types::ToolInputSchema::Json(
                            json_to_document(&t.input_schema),
                        ))
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        Some(
            aws_sdk_bedrockruntime::types::ToolConfiguration::builder()
                .set_tools(Some(specs))
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    // Move everything into the streaming task
    let team_id = claims.team_id.clone();
    let model_id = state.config.model_id.clone();

    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<axum::response::sse::Event, std::convert::Infallible>,
    >(64);

    // Spawn the agentic streaming loop
    tokio::spawn(async move {
        let max_turns = 5;
        let mut total_input_tokens: u64 = 0;
        let mut total_output_tokens: u64 = 0;
        let mut mcp_servers_used: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for turn in 0..max_turns {
            // Build request — use converse_stream for streaming
            let mut req = state
                .bedrock
                .converse_stream()
                .model_id(&model_id)
                .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
                    system_prompt.clone(),
                ))
                .set_messages(Some(bedrock_messages.clone()));

            if let Some(ref tc) = tool_config {
                req = req.tool_config(tc.clone());
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    error!("Bedrock converse_stream failed: {e}");
                    let _ = tx
                        .send(sse_event(
                            "error",
                            json!({ "message": format!("AI service error: {e}") }),
                        ))
                        .await;
                    return;
                }
            };

            let mut event_stream = response.stream;

            // Track content blocks for tool use detection
            let mut current_tool_uses: Vec<(String, String, String)> = Vec::new(); // (tool_use_id, name, input_json)
            let mut active_tool_id = String::new();
            let mut active_tool_name = String::new();
            let mut active_tool_input = String::new();
            let mut stop_reason_is_tool_use = false;

            // Process stream events
            loop {
                match event_stream.recv().await {
                    Ok(Some(event)) => match event {
                        aws_sdk_bedrockruntime::types::ConverseStreamOutput::ContentBlockStart(
                            e,
                        ) => {
                            if let Some(start) = e.start() {
                                match start {
                                    aws_sdk_bedrockruntime::types::ContentBlockStart::ToolUse(
                                        tu,
                                    ) => {
                                        active_tool_id = tu.tool_use_id().to_string();
                                        active_tool_name = tu.name().to_string();
                                        active_tool_input.clear();

                                        let (server, display_name) =
                                            if active_tool_name.contains("__") {
                                                let parts: Vec<&str> =
                                                    active_tool_name.splitn(2, "__").collect();
                                                (Some(parts[0].to_string()), parts[1].to_string())
                                            } else {
                                                (None, active_tool_name.clone())
                                            };

                                        let mut data = json!({
                                            "id": active_tool_id,
                                            "name": display_name,
                                        });
                                        if let Some(s) = &server {
                                            data["server"] = json!(s);
                                        }
                                        let _ = tx.send(sse_event("tool_start", data)).await;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        aws_sdk_bedrockruntime::types::ConverseStreamOutput::ContentBlockDelta(
                            e,
                        ) => {
                            if let Some(delta) = e.delta() {
                                match delta {
                                    aws_sdk_bedrockruntime::types::ContentBlockDelta::Text(
                                        text,
                                    ) => {
                                        let _ = tx
                                            .send(sse_event("text_delta", json!({ "text": text })))
                                            .await;
                                    }
                                    aws_sdk_bedrockruntime::types::ContentBlockDelta::ToolUse(
                                        tu,
                                    ) => {
                                        let input = tu.input();
                                        active_tool_input.push_str(input);
                                        let _ = tx
                                            .send(sse_event(
                                                "tool_input_delta",
                                                json!({
                                                    "id": active_tool_id,
                                                    "partial_json": input,
                                                }),
                                            ))
                                            .await;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        aws_sdk_bedrockruntime::types::ConverseStreamOutput::ContentBlockStop(
                            _,
                        ) => {
                            if !active_tool_id.is_empty() {
                                current_tool_uses.push((
                                    active_tool_id.clone(),
                                    active_tool_name.clone(),
                                    active_tool_input.clone(),
                                ));
                                active_tool_id.clear();
                                active_tool_name.clear();
                                active_tool_input.clear();
                            }
                        }
                        aws_sdk_bedrockruntime::types::ConverseStreamOutput::MessageStop(e) => {
                            let reason = e.stop_reason();
                            stop_reason_is_tool_use = matches!(
                                reason,
                                aws_sdk_bedrockruntime::types::StopReason::ToolUse
                            );
                        }
                        aws_sdk_bedrockruntime::types::ConverseStreamOutput::Metadata(e) => {
                            if let Some(usage) = e.usage() {
                                total_input_tokens += usage.input_tokens() as u64;
                                total_output_tokens += usage.output_tokens() as u64;
                            }
                        }
                        _ => {}
                    },
                    Ok(None) => break,
                    Err(e) => {
                        error!("Stream error: {e}");
                        let _ = tx
                            .send(sse_event(
                                "error",
                                json!({ "message": format!("Stream error: {e}") }),
                            ))
                            .await;
                        return;
                    }
                }
            }

            // If no tool use, we're done
            if !stop_reason_is_tool_use || current_tool_uses.is_empty() {
                let _ = tx
                    .send(sse_event(
                        "usage",
                        json!({
                            "input_tokens": total_input_tokens,
                            "output_tokens": total_output_tokens,
                            "turns": turn + 1,
                        }),
                    ))
                    .await;
                let _ = tx.send(sse_event("done", json!({}))).await;

                let state_clone = state.clone();
                let team_id_clone = team_id.clone();
                tokio::spawn(async move {
                    track_chat_tokens(
                        &state_clone,
                        &team_id_clone,
                        total_input_tokens,
                        total_output_tokens,
                    )
                    .await;
                });
                return;
            }

            // Build assistant message with text + tool use blocks for conversation history
            let mut assistant_blocks: Vec<aws_sdk_bedrockruntime::types::ContentBlock> = Vec::new();
            // We don't have the text from streaming readily, but Bedrock needs the assistant message
            // in the history. We reconstruct it from the tool uses.
            for (tu_id, tu_name, tu_input) in &current_tool_uses {
                let input_doc: Document = match serde_json::from_str::<Value>(tu_input) {
                    Ok(v) => json_to_document(&v),
                    Err(_) => Document::Object(Default::default()),
                };
                assistant_blocks.push(aws_sdk_bedrockruntime::types::ContentBlock::ToolUse(
                    aws_sdk_bedrockruntime::types::ToolUseBlock::builder()
                        .tool_use_id(tu_id)
                        .name(tu_name)
                        .input(input_doc)
                        .build()
                        .unwrap(),
                ));
            }

            let assistant_msg = aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::Assistant)
                .set_content(Some(assistant_blocks))
                .build()
                .unwrap();
            bedrock_messages.push(assistant_msg);

            // Execute MCP tool calls in parallel and build tool results
            let tool_futures: Vec<_> = current_tool_uses.iter().map(|(tu_id, tu_name, tu_input)| {
                let input: Value = serde_json::from_str(tu_input).unwrap_or(json!({}));
                let state_ref = &state;
                let team_id_ref = &team_id;
                let tu_id = tu_id.clone();
                let tu_name = tu_name.clone();
                async move {
                    let (server_id_opt, result) = if let Some((server_id, tool_name)) = tu_name.split_once("__") {
                        match invoke_mcp_tool(state_ref, team_id_ref, server_id, tool_name, &input).await {
                            Ok(result) => (Some(server_id.to_string()), result),
                            Err(e) => {
                                warn!(tool = tu_name.as_str(), error = %e, "MCP tool call failed");
                                (Some(server_id.to_string()), json!(format!("Error: {e}")))
                            }
                        }
                    } else {
                        (None, json!(format!("Unknown tool: {tu_name}")))
                    };
                    (tu_id, tu_name, server_id_opt, result)
                }
            }).collect();
            let tool_results_raw = futures::future::join_all(tool_futures).await;

            let mut tool_results = Vec::new();
            for (tu_id, tu_name, server_id_opt, result) in tool_results_raw {
                if let Some(sid) = server_id_opt {
                    mcp_servers_used.insert(sid);
                    let summary = summarize_tool_result(&result);
                    let _ = tx
                        .send(sse_event(
                            "tool_result",
                            json!({
                                "id": tu_id,
                                "status": "success",
                                "summary": summary,
                            }),
                        ))
                        .await;
                } else {
                    let _ = tx
                        .send(sse_event(
                            "tool_result",
                            json!({
                                "id": tu_id,
                                "status": "error",
                                "summary": format!("Unknown tool: {tu_name}"),
                            }),
                        ))
                        .await;
                }

                tool_results.push(aws_sdk_bedrockruntime::types::ContentBlock::ToolResult(
                    aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                        .tool_use_id(tu_id.as_str())
                        .content(aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                            serde_json::to_string(&result).unwrap_or_default(),
                        ))
                        .build()
                        .unwrap(),
                ));
            }

            bedrock_messages.push(
                aws_sdk_bedrockruntime::types::Message::builder()
                    .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                    .set_content(Some(tool_results))
                    .build()
                    .unwrap(),
            );

            info!(
                turn = turn + 1,
                "Streaming plan chat tool use turn complete"
            );
        }

        // Hit max turns
        let _ = tx
            .send(sse_event(
                "usage",
                json!({
                    "input_tokens": total_input_tokens,
                    "output_tokens": total_output_tokens,
                    "turns": max_turns,
                }),
            ))
            .await;
        let _ = tx.send(sse_event("done", json!({}))).await;
        let state_clone = state.clone();
        let team_id_clone = team_id.clone();
        tokio::spawn(async move {
            track_chat_tokens(
                &state_clone,
                &team_id_clone,
                total_input_tokens,
                total_output_tokens,
            )
            .await;
        });
    });

    let stream: std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>
                + Send,
        >,
    > = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    Ok(axum::response::sse::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text(""),
    ))
}

/// Produce a short summary of a tool result for the UI.
fn summarize_tool_result(result: &Value) -> String {
    let s = result.to_string();
    if s.len() <= 80 {
        s
    } else {
        format!("{}…", &s[..77])
    }
}
async fn track_chat_tokens(state: &AppState, team_id: &str, input_tokens: u64, output_tokens: u64) {
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(team_id))
            .key("period", attr_s(period))
            .update_expression("ADD total_tokens_in :tin, total_tokens_out :tout")
            .expression_attribute_values(":tin", attr_n(input_tokens))
            .expression_attribute_values(":tout", attr_n(output_tokens))
            .send()
            .await;
    }
}

async fn load_allow_plan_log_analyzer(state: &AppState, team_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("SETTINGS#WORKFLOW"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| {
            i.get("allow_plan_log_analyzer")
                .and_then(|v| v.as_bool().ok())
                .copied()
        })
        .unwrap_or(false)
}

async fn load_log_analyzer_context(state: &AppState, team_id: &str) -> String {
    let result = match state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":prefix", attr_s("REC#"))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await
    {
        Ok(v) => v,
        Err(_) => return String::new(),
    };

    let mut lines: Vec<String> = Vec::new();
    for item in result.items() {
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
        if status == "dismissed" {
            continue;
        }

        let title = item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("Untitled recommendation");
        let severity = item
            .get("severity")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("unknown");
        let summary = item
            .get("summary")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        lines.push(format!("- [{severity}] {title}: {summary}"));
    }

    lines.join("\n")
}

// ---------------------------------------------------------------------------
// MCP tool helpers for plan_chat
// ---------------------------------------------------------------------------

struct McpToolDef {
    server_id: String,
    name: String,
    description: String,
    input_schema: Value,
}

fn json_to_document(val: &Value) -> Document {
    match val {
        Value::Null => Document::Null,
        Value::Bool(b) => Document::Bool(*b),
        Value::Number(n) => {
            Document::Number(aws_smithy_types::Number::Float(n.as_f64().unwrap_or(0.0)))
        }
        Value::String(s) => Document::String(s.clone()),
        Value::Array(arr) => Document::Array(arr.iter().map(json_to_document).collect()),
        Value::Object(obj) => Document::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_document(v)))
                .collect(),
        ),
    }
}

fn document_to_json(doc: &Document) -> Value {
    match doc {
        Document::Null => Value::Null,
        Document::Bool(b) => Value::Bool(*b),
        Document::Number(n) => json!(n.to_f64_lossy()),
        Document::String(s) => Value::String(s.clone()),
        Document::Array(arr) => Value::Array(arr.iter().map(document_to_json).collect()),
        Document::Object(obj) => {
            let map: serde_json::Map<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect();
            Value::Object(map)
        }
    }
}

/// Return the MCP configs table name, falling back to settings table for migration compat.
fn mcp_table(state: &AppState) -> &str {
    let t = &state.config.mcp_configs_table_name;
    if t.is_empty() {
        &state.config.settings_table_name
    } else {
        t
    }
}

/// Load enabled plugins for the team, returning (server_id, has_credentials, custom_prompt).
async fn load_enabled_plugins(
    state: &AppState,
    team_id: &str,
) -> Vec<(String, bool, Option<String>)> {
    state
        .dynamo
        .query()
        .table_name(mcp_table(state))
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":prefix", attr_s("PLUGIN#"))
        .send()
        .await
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| {
                    let sk = item.get("sk")?.as_s().ok()?;
                    sk.strip_prefix("PLUGIN#").map(|id| {
                        let has_creds = item
                            .get("has_credentials")
                            .and_then(|v| v.as_bool().ok())
                            .copied()
                            .unwrap_or(false);
                        let custom_prompt = item
                            .get("custom_prompt")
                            .and_then(|v| v.as_s().ok())
                            .map(|s| s.to_string());
                        (id.to_string(), has_creds, custom_prompt)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Load cached MCP tool schemas from S3 for a given server.
async fn load_mcp_tool_cache(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    server_id: &str,
) -> Option<Vec<Value>> {
    let key = format!("config/mcp-tools/{server_id}.json");
    let resp = s3.get_object().bucket(bucket).key(&key).send().await.ok()?;
    let bytes = resp.body.collect().await.ok()?.into_bytes();
    let parsed: Value = serde_json::from_slice(&bytes).ok()?;
    parsed["tools"].as_array().cloned()
}

/// Gateway plugin catalog — maps server_id to (npx_package, env_mapping).
/// env_mapping: &[(cred_field_key, env_var_name)]
#[allow(clippy::type_complexity)]
const GATEWAY_MCP_CATALOG: &[(&str, &str, &[(&str, &str)])] = &[
    (
        "figma",
        "figma-developer-mcp",
        &[("api_token", "FIGMA_API_KEY")],
    ),
    (
        "sentry",
        "@sentry/mcp-server",
        &[
            ("auth_token", "SENTRY_ACCESS_TOKEN"),
            ("org_slug", "SENTRY_ORG"),
        ],
    ),
    (
        "linear",
        "@linear/mcp-server",
        &[("api_key", "LINEAR_API_KEY")],
    ),
    (
        "notion",
        "@notionhq/notion-mcp-server",
        &[("api_key", "OPENAPI_MCP_HEADERS")],
    ),
    (
        "vercel",
        "@vercel/mcp-adapter",
        &[("api_token", "VERCEL_TOKEN")],
    ),
    ("stripe", "@stripe/mcp", &[("api_key", "STRIPE_SECRET_KEY")]),
    // Cloudflare removed — their MCP server moved to remote OAuth-only
    (
        "posthog",
        "@nicobailon/posthog-mcp",
        &[
            ("api_key", "POSTHOG_API_KEY"),
            ("project_id", "POSTHOG_PROJECT_ID"),
            ("host", "POSTHOG_HOST"),
        ],
    ),
    (
        "gitlab",
        "@nicobailon/gitlab-mcp",
        &[
            ("personal_token", "GITLAB_TOKEN"),
            ("gitlab_url", "GITLAB_URL"),
        ],
    ),
    (
        "neon",
        "@nicobailon/neon-mcp",
        &[("api_key", "NEON_API_KEY")],
    ),
    (
        "turso",
        "@nicobailon/turso-mcp",
        &[("api_token", "TURSO_AUTH_TOKEN"), ("org_name", "TURSO_ORG")],
    ),
    (
        "snyk",
        "@nicobailon/snyk-mcp",
        &[("api_token", "SNYK_TOKEN"), ("org_id", "SNYK_ORG_ID")],
    ),
    (
        "launchdarkly",
        "@nicobailon/launchdarkly-mcp",
        &[("api_key", "LD_API_KEY"), ("project_key", "LD_PROJECT_KEY")],
    ),
    (
        "mongodb",
        "@nicobailon/mongodb-mcp",
        &[("connection_string", "MDB_MCP_CONNECTION_STRING")],
    ),
    (
        "grafana",
        "@nicobailon/grafana-mcp",
        &[("api_token", "GRAFANA_TOKEN"), ("url", "GRAFANA_URL")],
    ),
    ("redis", "@nicobailon/redis-mcp", &[("url", "REDIS_URL")]),
    (
        "upstash",
        "@nicobailon/upstash-mcp",
        &[
            ("api_key", "UPSTASH_EMAIL"),
            ("api_token", "UPSTASH_API_KEY"),
        ],
    ),
];

/// Invoke an MCP tool via the MCP proxy Lambda.
async fn invoke_mcp_tool(
    state: &AppState,
    team_id: &str,
    server_id: &str,
    tool_name: &str,
    tool_input: &Value,
) -> Result<Value, String> {
    // Find catalog entry for this server
    let (_, npx_package, env_mapping) = GATEWAY_MCP_CATALOG
        .iter()
        .find(|(id, _, _)| *id == server_id)
        .ok_or_else(|| format!("Unknown MCP server: {server_id}"))?;

    // Load credentials from DynamoDB
    let creds_item = state
        .dynamo
        .get_item()
        .table_name(mcp_table(state))
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("PLUGIN#{server_id}")))
        .send()
        .await
        .map_err(|e| format!("Failed to load plugin credentials: {e}"))?
        .item()
        .cloned();

    let creds_json: Value = creds_item
        .as_ref()
        .and_then(|item| {
            item.get("credentials")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| serde_json::from_str(s).ok())
        })
        .unwrap_or(json!({}));

    // Map credential fields to env vars
    let mut env_vars = serde_json::Map::new();
    for (cred_key, env_var) in *env_mapping {
        if let Some(val) = creds_json.get(*cred_key).and_then(|v| v.as_str()) {
            env_vars.insert(env_var.to_string(), json!(val));
        }
    }

    // Build proxy Lambda payload
    let payload = json!({
        "action": "call_tool",
        "server_id": server_id,
        "npx_package": npx_package,
        "env_vars": env_vars,
        "tool_name": tool_name,
        "tool_input": tool_input,
    });

    let resp = state
        .lambda
        .invoke()
        .function_name(&state.config.mcp_proxy_function_name)
        .payload(aws_sdk_lambda::primitives::Blob::new(
            serde_json::to_vec(&payload).map_err(|e| format!("serialize: {e}"))?,
        ))
        .send()
        .await
        .map_err(|e| format!("Lambda invoke failed: {e}"))?;

    let response_payload = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();

    let response: Value =
        serde_json::from_slice(&response_payload).map_err(|e| format!("parse response: {e}"))?;

    if let Some(err) = response.get("error").and_then(|v| v.as_str()) {
        return Err(err.to_string());
    }

    Ok(response.get("result").cloned().unwrap_or(json!(null)))
}
