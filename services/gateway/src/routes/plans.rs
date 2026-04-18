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

    let mut put = state
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
        .item("updated_at", attr_s(&now));

    // Store which MCP servers were used during chat planning
    if let Some(servers) = body["mcp_servers"].as_array() {
        let server_list: Vec<aws_sdk_dynamodb::types::AttributeValue> = servers
            .iter()
            .filter_map(|s| s.as_str())
            .map(attr_s)
            .collect();
        if !server_list.is_empty() {
            put = put.item(
                "mcp_servers",
                aws_sdk_dynamodb::types::AttributeValue::L(server_list),
            );
        }
    }

    put.send().await.map_err(|e| {
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

/// POST /api/plans/:plan_id/tasks/:task_id/approve — approve a task and immediately dispatch it.
pub async fn approve_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    // 1. Mark task as approved
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

    // 2. Mark plan as executing (idempotent — may already be executing)
    let plan_sk = format!("PLAN#{plan_id}");
    let _ = state
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
        .await;

    // 3. Immediately dispatch this single task to the worker
    let plan_msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        triggered_by: claims.display_name(),
        tasks: vec![task_id.clone()],
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
            error!("Failed to send task to queue: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(plan_id, task_id, "Task approved and dispatched to worker");

    Ok(Json(json!({ "dispatched": true, "task_id": task_id })))
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

    if status != "waiting" && status != "failed" {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Reset task status to "waiting" so the worker picks it up
    if status == "failed" {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&task_sk))
            .update_expression("SET #s = :s")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr_s("waiting"))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to reset task status: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
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
    let mcp_servers: Vec<&str> = item
        .get("mcp_servers")
        .and_then(|v| v.as_l().ok())
        .map(|list| {
            list.iter()
                .filter_map(|v| v.as_s().ok().map(|s| s.as_str()))
                .collect()
        })
        .unwrap_or_default();

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
        "mcp_servers": if mcp_servers.is_empty() { Value::Null } else { json!(mcp_servers) },
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

// ─── Plan Openspec ────────────────────────────────────────────────────

/// POST /api/plans/:plan_id/openspec/generate — generate openspec markdown files from plan data.
pub async fn generate_plan_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    // Fetch plan + tasks (same query as get_plan)
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
            error!("Failed to query plan for openspec: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

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

    let plan = plan_data.ok_or(StatusCode::NOT_FOUND)?;
    tasks.sort_by_key(|t| t["order"].as_u64().unwrap_or(0));

    let title = plan["title"].as_str().unwrap_or("Untitled Plan");
    let description = plan["description"].as_str().unwrap_or("");
    let repo = plan["repo"].as_str().unwrap_or("");
    let destination = plan["destination"].as_str().unwrap_or("github");
    let task_count = tasks.len();

    // Generate proposal.md
    let mut proposal = format!(
        "# Proposal: {title}\n\n## Summary\n{description}\n\n## Scope\n- Repository: {repo}\n- Tasks: {task_count}\n- Destination: {destination}\n\n## Task Breakdown\n"
    );
    for t in &tasks {
        let t_title = t["title"].as_str().unwrap_or("");
        let t_desc = t["description"].as_str().unwrap_or("");
        proposal.push_str(&format!("### {t_title}\n{t_desc}\n\n"));
    }

    // Generate design.md
    let mut design = format!(
        "# Technical Design: {title}\n\n## Overview\n{description}\n\n## Changes by Repository\n"
    );
    let mut repos_map: std::collections::BTreeMap<String, Vec<&Value>> =
        std::collections::BTreeMap::new();
    for t in &tasks {
        let t_repo = t["repo"].as_str().unwrap_or(repo).to_string();
        repos_map.entry(t_repo).or_default().push(t);
    }
    for (r, repo_tasks) in &repos_map {
        let r_label = if r.is_empty() {
            "(default)"
        } else {
            r.as_str()
        };
        design.push_str(&format!("### {r_label}\n"));
        for t in repo_tasks {
            let t_title = t["title"].as_str().unwrap_or("");
            design.push_str(&format!("- {t_title}\n"));
        }
        design.push('\n');
    }
    design.push_str("## Task Dependencies\n");
    let mut has_deps = false;
    for t in &tasks {
        if let Some(dep) = t["depends_on"].as_str().filter(|s| !s.is_empty()) {
            let t_title = t["title"].as_str().unwrap_or("");
            let dep_title = tasks
                .iter()
                .find(|d| d["task_id"].as_str() == Some(dep))
                .and_then(|d| d["title"].as_str())
                .unwrap_or(dep);
            design.push_str(&format!("- **{t_title}** depends on **{dep_title}**\n"));
            has_deps = true;
        }
    }
    if !has_deps {
        design.push_str("No explicit task dependencies.\n");
    }

    // Generate tasks.md
    let mut tasks_md = format!("# Task Checklist: {title}\n\n");
    for t in &tasks {
        let t_title = t["title"].as_str().unwrap_or("");
        let t_repo = t["repo"].as_str().unwrap_or(repo);
        let t_desc = t["description"].as_str().unwrap_or("");
        let t_ac = t["acceptance_criteria"].as_str().unwrap_or("");
        tasks_md.push_str(&format!("- [ ] **{t_title}** ({t_repo})\n"));
        if !t_desc.is_empty() {
            tasks_md.push_str(&format!("  {t_desc}\n"));
        }
        if !t_ac.is_empty() {
            tasks_md.push_str(&format!("  Acceptance: {t_ac}\n"));
        }
        tasks_md.push('\n');
    }

    // Generate spec.md
    let mut spec = format!("# Acceptance Specification: {title}\n\n");
    for t in &tasks {
        let t_ac = t["acceptance_criteria"].as_str().unwrap_or("");
        if !t_ac.is_empty() {
            let t_title = t["title"].as_str().unwrap_or("");
            spec.push_str(&format!("## {t_title}\n\n### Criteria\n{t_ac}\n\n"));
        }
    }

    // Store in S3
    let prefix = format!("teams/{}/plans/{plan_id}/openspec", claims.team_id);
    let bucket = &state.config.bucket_name;
    let files = [
        ("proposal.md", proposal),
        ("design.md", design),
        ("tasks.md", tasks_md),
        ("spec.md", spec),
    ];

    for (name, content) in &files {
        let key = format!("{prefix}/{name}");
        state
            .s3
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(content.clone().into_bytes().into())
            .content_type("text/markdown")
            .send()
            .await
            .map_err(|e| {
                error!("Failed to upload openspec {name}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    // Update plan with openspec_generated_at timestamp
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("PLAN#{plan_id}")))
        .update_expression("SET openspec_generated_at = :ts")
        .expression_attribute_values(":ts", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            warn!("Failed to update openspec_generated_at: {e}");
        });

    info!(plan_id = %plan_id, team_id = %claims.team_id, "Openspec generated for plan");

    Ok(Json(json!({
        "files": ["proposal.md", "design.md", "tasks.md", "spec.md"]
    })))
}

/// GET /api/plans/:plan_id/openspec — fetch the four openspec files from S3.
pub async fn get_plan_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    let prefix = format!("teams/{}/plans/{plan_id}/openspec", claims.team_id);
    let bucket = &state.config.bucket_name;

    let mut files = serde_json::Map::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        if let Ok(output) = state.s3.get_object().bucket(bucket).key(&key).send().await {
            if let Ok(bytes) = output.body.collect().await {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    files.insert(name.replace(".md", ""), Value::String(text));
                }
            }
        }
    }

    if files.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(Value::Object(files)))
}

// ─── Plan Templates ───────────────────────────────────────────────────

fn validate_template_id(id: &str) -> Result<(), StatusCode> {
    if id.is_empty() || id.len() > 30 || !id.chars().all(|c| c.is_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn template_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    let tags: Value = item
        .get("tags")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    let task_count = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Vec<Value>>(s).ok())
        .map(|v| v.len())
        .unwrap_or(0);

    json!({
        "template_id": item.get("template_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "category": item.get("category").and_then(|v| v.as_s().ok()),
        "tags": tags,
        "task_count": task_count,
        "usage_count": item.get("usage_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "created_by": item.get("created_by").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
    })
}

fn template_full_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    let tags: Value = item
        .get("tags")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    let task_templates: Value = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    json!({
        "template_id": item.get("template_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "category": item.get("category").and_then(|v| v.as_s().ok()),
        "tags": tags,
        "task_templates": task_templates,
        "usage_count": item.get("usage_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "created_by": item.get("created_by").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
    })
}

/// GET /api/plans/templates — list templates for the team (paginated).
pub async fn list_templates(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let limit: i32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .min(100);

    let mut query = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("TEMPLATE#"))
        .scan_index_forward(false)
        .limit(limit);

    if let Some(cursor) = params.get("cursor").filter(|c| !c.is_empty()) {
        let lek = std::collections::HashMap::from([
            ("pk".to_string(), attr_s(&claims.team_id)),
            ("sk".to_string(), attr_s(cursor)),
        ]);
        query = query.set_exclusive_start_key(Some(lek));
    }

    let result = query.send().await.map_err(|e| {
        error!("Failed to query templates: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let templates: Vec<Value> = result.items().iter().map(template_from_item).collect();

    let next_cursor = result
        .last_evaluated_key()
        .and_then(|k| k.get("sk"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());

    Ok(Json(json!({
        "templates": templates,
        "next_cursor": next_cursor,
    })))
}

/// POST /api/plans/templates — create a new template.
pub async fn create_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    if title.is_empty() || title.len() > 200 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let description = body["description"].as_str().unwrap_or("");
    let category = body["category"].as_str().unwrap_or("");

    let tags_json = body["tags"]
        .as_array()
        .map(|arr| {
            let strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            serde_json::to_string(&strs).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|| "[]".to_string());

    let task_templates_json = body["task_templates"]
        .as_array()
        .map(|arr| serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string()))
        .unwrap_or_else(|| "[]".to_string());

    let template_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("TEMPLATE#{template_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("template_id", attr_s(&template_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("category", attr_s(category))
        .item("tags", attr_s(&tags_json))
        .item("task_templates", attr_s(&task_templates_json))
        .item("usage_count", attr_n(0))
        .item("created_by", attr_s(&claims.display_name()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "template_id": template_id })))
}

/// POST /api/plans/templates/from-plan/:plan_id — create a template from an existing plan.
pub async fn create_template_from_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    // Fetch the plan + tasks
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
            error!("Failed to query plan for template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let mut plan_title = String::new();
    let mut plan_description = String::new();
    let mut task_templates: Vec<Value> = Vec::new();

    for item in items {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            let task = json!({
                "title": item.get("title").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "description": item.get("description").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "acceptance_criteria": item.get("acceptance_criteria").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "repo": item.get("repo").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "order": item.get("task_order").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
            });
            task_templates.push(task);
        } else {
            plan_title = item
                .get("title")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
            plan_description = item
                .get("description")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
        }
    }

    // Sort tasks by order
    task_templates.sort_by_key(|t| t["order"].as_u64().unwrap_or(0));

    // Allow body overrides
    let title = body["title"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or(plan_title);
    if title.is_empty() || title.len() > 200 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let description = body["description"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or(plan_description);
    let category = body["category"].as_str().unwrap_or("");

    let tags_json = body["tags"]
        .as_array()
        .map(|arr| {
            let strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            serde_json::to_string(&strs).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|| "[]".to_string());

    let task_templates_json =
        serde_json::to_string(&task_templates).unwrap_or_else(|_| "[]".to_string());

    let template_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("TEMPLATE#{template_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("template_id", attr_s(&template_id))
        .item("title", attr_s(&title))
        .item("description", attr_s(&description))
        .item("category", attr_s(category))
        .item("tags", attr_s(&tags_json))
        .item("task_templates", attr_s(&task_templates_json))
        .item("usage_count", attr_n(0))
        .item("created_by", attr_s(&claims.display_name()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create template from plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "template_id": template_id })))
}

/// GET /api/plans/templates/:template_id — get full template including task_templates.
pub async fn get_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_template_id(&template_id)?;

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(template_full_from_item(item)))
}

/// DELETE /api/plans/templates/:template_id — delete a template.
pub async fn delete_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
) -> Result<StatusCode, StatusCode> {
    validate_template_id(&template_id)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// POST /api/plans/templates/:template_id/use — create a plan from a template.
pub async fn use_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    validate_template_id(&template_id)?;

    // Fetch the template
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get template for use: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let tmpl_title = item
        .get("title")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let tmpl_description = item
        .get("description")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();

    let task_templates: Vec<Value> = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    // Allow body overrides
    let title = body["title"].as_str().unwrap_or(&tmpl_title);
    let description = body["description"].as_str().unwrap_or(&tmpl_description);
    let repo = body["repo"].as_str().unwrap_or("");
    let destination = match body["destination"].as_str() {
        Some("jira") => "jira",
        _ => "github",
    };

    // Create the plan
    let plan_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let plan_sk = format!("PLAN#{plan_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&plan_sk))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("repo", attr_s(repo))
        .item("destination", attr_s(destination))
        .item("status", attr_s("draft"))
        .item("task_count", attr_n(task_templates.len()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create plan from template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Create tasks from task_templates
    for (i, task) in task_templates.iter().enumerate() {
        let task_title = task["title"].as_str().unwrap_or("Untitled task");
        let task_desc = task["description"].as_str().unwrap_or("");
        let task_criteria = task["acceptance_criteria"].as_str().unwrap_or("");
        let task_repo = task["repo"].as_str().unwrap_or(repo);
        let order = task["order"].as_u64().unwrap_or(i as u64);
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
            .item("task_order", attr_n(order))
            .item("created_at", attr_s(&now));

        if !task_repo.is_empty() {
            put = put.item("repo", attr_s(task_repo));
        }
        if !destination.is_empty() {
            put = put.item("destination", attr_s(destination));
        }

        put.send().await.map_err(|e| {
            error!("Failed to create task from template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    // Increment usage_count on the template
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .update_expression("ADD usage_count :one")
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await
        .ok();

    // Track plan usage
    track_plan_usage(&state, &claims.team_id).await;

    Ok(Json(json!({ "plan_id": plan_id })))
}

// ─── Plan Chat ────────────────────────────────────────────────────────

const PLAN_CHAT_SYSTEM: &str = r#"You are a planning assistant for Coderhelm, an autonomous AI coding agent.
Your job is to help the user break down a feature, epic, or large piece of work into a structured plan.

When the user describes what they want to build, you should:
1. If the user already specified the repo, tech constraints, and scope — go straight to generating the plan.
2. Only ask clarifying questions when truly critical info is missing (e.g. no repo specified at all).
3. Never ask more than 2 clarifying questions before generating a plan.

CRITICAL — Conversation continuity:
- When the user adds new requirements or follow-up messages, incorporate ALL previous requests into the updated plan.
- Never forget or drop earlier requests. The plan should be the complete, cumulative result of everything discussed.

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
- A plan can span MULTIPLE repositories. The top-level "repo" is the primary repo. Each task has an optional "repo" field for targeting a different repo.
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

/// Strip ```json...``` code blocks from a chat message, keeping surrounding text.
/// Used to trim stale plan JSONs from older assistant messages.
fn strip_plan_json(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    let mut pos = 0;
    while let Some(start) = content[pos..].find("```json") {
        let abs_start = pos + start;
        result.push_str(&content[pos..abs_start]);
        if let Some(end) = content[abs_start + 7..].find("```") {
            let abs_end = abs_start + 7 + end + 3;
            result.push_str("[plan updated]");
            pos = abs_end;
        } else {
            // Unclosed fence — keep as-is
            result.push_str(&content[abs_start..]);
            return result;
        }
    }
    result.push_str(&content[pos..]);
    result
}

/// Shared context for plan chat — loaded once, used by both streaming and non-streaming.
struct PlanChatContext {
    system_prompt: String,
    messages: Vec<Value>,
    tools: Vec<Value>,
}

/// Load all context needed for plan chat (DynamoDB, S3, prompt assembly, message conversion).
/// Shared between `plan_chat` and `plan_chat_stream` to eliminate duplication.
async fn load_plan_chat_context(
    state: &AppState,
    team_id: &str,
    messages_input: &[Value],
) -> Result<PlanChatContext, StatusCode> {

    // Load org context, repo list, log analyzer flag, enabled plugins, and templates in parallel
    let (
        org_context_result,
        repo_list_result,
        allow_plan_log_analyzer,
        enabled_plugins,
        tmpl_result,
    ) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s("AGENTS#GLOBAL"))
            .send(),
        state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("REPO#"))
            .limit(50)
            .send(),
        load_allow_plan_log_analyzer(state, team_id),
        load_enabled_plugins(state, team_id),
        state
            .dynamo
            .query()
            .table_name(&state.config.plans_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("TEMPLATE#"))
            .limit(10)
            .send(),
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

    let template_lines: Vec<String> = tmpl_result
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter_map(|item| {
                    let title = item.get("title")?.as_s().ok()?;
                    let desc = item
                        .get("description")
                        .and_then(|v| v.as_s().ok())
                        .unwrap_or(&String::new())
                        .clone();
                    Some(format!("- {title}: {desc}"))
                })
                .collect()
        })
        .unwrap_or_default();

    // Conditionally load log analyzer context
    let log_analyzer_context = if allow_plan_log_analyzer {
        Some(load_log_analyzer_context(state, team_id).await)
    } else {
        None
    };

    // Load MCP tool definitions from S3 cache (always load — let the model decide when to use them)
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

    // Build system prompt
    let mut system_prompt = PLAN_CHAT_SYSTEM.to_string();
    if !org_context.is_empty() {
        // Cap org_context to avoid prompt bloat
        let capped = if org_context.len() > 2000 {
            &org_context[..org_context[..2000].rfind('\n').unwrap_or(2000)]
        } else {
            &org_context
        };
        system_prompt.push_str(&format!("\n\nThe user's organization context:\n{capped}"));
    }
    if !repo_list.is_empty() {
        system_prompt.push_str(&format!(
            "\n\nAvailable repositories (use these exact names for the \"repo\" field):\n{}\n\nFor cross-repo changes, set the \"repo\" field on each task to the appropriate repository.",
            repo_list
                .iter()
                .map(|r| format!("- {r}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
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
            "\n\nYou have tool-call access to the following MCP servers.\n\
             IMPORTANT tool-use rules:\n\
             - Only call a tool when the user's request genuinely requires external data \
             (e.g. \"pull the tasks from Notion\", \"check Sentry for errors\").\n\
             - NEVER call tools on greetings, general planning questions, or when the user \
             has not referenced an external system.\n\
             - If a tool returns no results, do NOT retry — tell the user and continue planning.\n\
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
    if !template_lines.is_empty() {
        system_prompt.push_str("\n\nThe team has these plan templates available:\n");
        system_prompt.push_str(&template_lines.join("\n"));
        system_prompt.push_str("\nIf the user's request matches a template pattern, mention it and ask if they'd like to use it as a starting point.");
    }

    // Convert messages to Anthropic format with history trimming:
    // Strip plan JSON blocks from older assistant messages to reduce token waste.
    let mut api_messages: Vec<Value> = Vec::new();
    let msg_count = messages_input.len();
    for (i, msg) in messages_input.iter().enumerate() {
        let role_str = msg["role"].as_str().unwrap_or("user");
        let raw_content = msg["content"].as_str().unwrap_or("");
        if raw_content.is_empty() {
            continue;
        }
        // Trim plan JSON from older assistant messages (keep the latest intact)
        let content = if role_str == "assistant" && i < msg_count.saturating_sub(1) {
            strip_plan_json(raw_content)
        } else {
            raw_content.to_string()
        };
        api_messages.push(json!({
            "role": role_str,
            "content": [{"type": "text", "text": content}]
        }));
    }

    // Build Anthropic tools from MCP tools (cache_control on last for prompt caching)
    let tool_count = mcp_tools.len();
    let tools: Vec<Value> = mcp_tools
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let mut tool = json!({
                "name": format!("{}__{}", t.server_id, t.name),
                "description": t.description,
                "input_schema": t.input_schema
            });
            if i == tool_count - 1 {
                tool["cache_control"] = json!({"type": "ephemeral"});
            }
            tool
        })
        .collect();

    info!(
        team_id = team_id,
        tools_count = tools.len(),
        mcp_plugins = enabled_plugins.len(),
        messages_count = api_messages.len(),
        "load_plan_chat_context: context loaded"
    );

    Ok(PlanChatContext {
        system_prompt,
        messages: api_messages,
        tools,
    })
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

    let ctx = load_plan_chat_context(&state, &claims.team_id, messages_input).await?;
    let mut api_messages = ctx.messages;

    // Load team's Anthropic API key
    let provider_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let api_key = provider_result
        .as_ref()
        .and_then(|item| item.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .ok_or_else(|| {
            error!("No Anthropic API key configured for team");
            StatusCode::BAD_REQUEST
        })?;

    let model_id = provider_result
        .as_ref()
        .and_then(|item| item.get("primary_model"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-4-6".to_string());

    // Agentic loop — up to 5 tool-use turns for plan chat (gateway has 30s timeout)
    let max_turns = 5;
    let mut total_input_tokens: u64 = 0;
    let mut total_output_tokens: u64 = 0;
    let mut cache_read_tokens: u64 = 0;
    let mut cache_write_tokens: u64 = 0;
    let mut mcp_servers_used: std::collections::HashSet<String> = std::collections::HashSet::new();
    let http = reqwest::Client::new();

    for turn in 0..max_turns {
        let mut body = json!({
            "model": model_id,
            "max_tokens": 4096,
            "system": [{"type": "text", "text": ctx.system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": api_messages,
            "temperature": 0.7
        });
        if !ctx.tools.is_empty() {
            body["tools"] = json!(ctx.tools);
        }

        let resp = http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &api_key)
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "prompt-caching-2024-07-31")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                error!("Anthropic request failed: {e}");
                StatusCode::BAD_GATEWAY
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!("Anthropic API error ({status}): {body}");
            return Err(StatusCode::BAD_GATEWAY);
        }

        let response: Value = resp.json().await.map_err(|e| {
            error!("Failed to parse Anthropic response: {e}");
            StatusCode::BAD_GATEWAY
        })?;

        // Track token usage
        if let Some(usage) = response.get("usage") {
            total_input_tokens += usage["input_tokens"].as_u64().unwrap_or(0);
            total_output_tokens += usage["output_tokens"].as_u64().unwrap_or(0);
            cache_read_tokens += usage["cache_read_input_tokens"].as_u64().unwrap_or(0);
            cache_write_tokens += usage["cache_creation_input_tokens"].as_u64().unwrap_or(0);
        }

        let content = response["content"].as_array().cloned().unwrap_or_default();
        let stop_reason = response["stop_reason"].as_str().unwrap_or("");

        // Add assistant message to history
        api_messages.push(json!({"role": "assistant", "content": content}));

        // Extract tool uses
        let tool_uses: Vec<_> = content
            .iter()
            .filter(|b| b["type"].as_str() == Some("tool_use"))
            .cloned()
            .collect();

        if tool_uses.is_empty() || stop_reason == "end_turn" {
            // Extract final text and return
            let text = content
                .iter()
                .filter_map(|b| {
                    if b["type"].as_str() == Some("text") {
                        b["text"].as_str().map(String::from)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("");

            {
                let state_clone = state.clone();
                let team_id_clone = claims.team_id.clone();
                tokio::spawn(async move {
                    track_chat_tokens(
                        &state_clone,
                        &team_id_clone,
                        total_input_tokens,
                        total_output_tokens,
                        cache_read_tokens,
                        cache_write_tokens,
                    )
                    .await;
                });
            }
            let servers: Vec<&str> = mcp_servers_used.iter().map(|s| s.as_str()).collect();
            return Ok(Json(json!({ "content": text, "mcp_servers": servers })));
        }

        // Execute MCP tool calls via proxy Lambda (in parallel)
        let tool_futures: Vec<_> = tool_uses.iter().map(|tu| {
            let full_name = tu["name"].as_str().unwrap_or("").to_string();
            let input = tu["input"].clone();
            let tool_use_id = tu["id"].as_str().unwrap_or("").to_string();
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

        let mut tool_results: Vec<Value> = Vec::new();
        for (tool_use_id, server_id, result) in tool_results_raw {
            if let Some(sid) = server_id {
                mcp_servers_used.insert(sid);
            }
            let model_result = if is_empty_tool_result(&result) {
                "No results found.".to_string()
            } else {
                compact_tool_result_for_model(&result)
            };
            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": model_result
            }));
        }

        api_messages.push(json!({"role": "user", "content": tool_results}));

        info!(turn = turn + 1, "Plan chat tool use turn complete");
    }

    // Hit max turns — extract last text
    let text = api_messages
        .last()
        .and_then(|msg| msg["content"].as_array())
        .and_then(|content| {
            content.iter().find_map(|b| {
                if b["type"].as_str() == Some("text") {
                    b["text"].as_str().map(String::from)
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
                0,
                0,
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

/// Anthropic SSE streaming for plan chat.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::collapsible_match)]
async fn run_anthropic_stream(
    state: &Arc<AppState>,
    tx: &tokio::sync::mpsc::Sender<Result<axum::response::sse::Event, std::convert::Infallible>>,
    api_key: &str,
    model_id: &str,
    system_prompt: &str,
    tools: &[Value],
    messages: &mut Vec<Value>,
    team_id: &str,
) {
    use futures::StreamExt;

    let http = reqwest::Client::new();
    let max_turns = 5;
    let mut total_input_tokens: u64 = 0;
    let mut total_output_tokens: u64 = 0;
    let mut cache_read_tokens: u64 = 0;
    let mut cache_write_tokens: u64 = 0;

    info!(
        model = model_id,
        tools = tools.len(),
        messages = messages.len(),
        "run_anthropic_stream: starting agentic loop"
    );

    for turn in 0..max_turns {
        let mut body = json!({
            "model": model_id,
            "max_tokens": 4096,
            "stream": true,
            "system": [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
            "temperature": 0.7
        });
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }

        let resp = match http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "prompt-caching-2024-07-31")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                info!(turn = turn, "run_anthropic_stream: Anthropic returned 200");
                r
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                error!("Anthropic stream error ({status}): {body}");
                let _ = tx
                    .send(sse_event(
                        "error",
                        json!({"message": format!("Anthropic API error: {status}")}),
                    ))
                    .await;
                return;
            }
            Err(e) => {
                error!("Anthropic stream request failed: {e}");
                let _ = tx
                    .send(sse_event(
                        "error",
                        json!({"message": format!("AI service error: {e}")}),
                    ))
                    .await;
                return;
            }
        };

        // Process SSE stream from Anthropic
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        let mut assistant_text = String::new();
        let mut tool_uses: Vec<(String, String, String)> = Vec::new(); // (id, name, input_json)
        let mut active_tool_id = String::new();
        let mut active_tool_name = String::new();
        let mut active_tool_input = String::new();
        let mut stop_reason = String::new();

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => break,
            };
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(line_end) = buffer.find('\n') {
                let line = buffer[..line_end].trim().to_string();
                buffer = buffer[line_end + 1..].to_string();

                if !line.starts_with("data: ") {
                    continue;
                }
                let data = &line[6..];
                if data == "[DONE]" {
                    break;
                }

                let event: Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                match event["type"].as_str() {
                    Some("content_block_start") => {
                        if let Some(cb) = event.get("content_block") {
                            if cb["type"].as_str() == Some("tool_use") {
                                active_tool_id = cb["id"].as_str().unwrap_or("").to_string();
                                active_tool_name = cb["name"].as_str().unwrap_or("").to_string();
                                active_tool_input.clear();
                                let server = active_tool_name.split("__").next().unwrap_or("").to_string();
                                let _ = tx
                                    .send(sse_event(
                                        "tool_start",
                                        json!({"id": &active_tool_id, "name": &active_tool_name, "server": server}),
                                    ))
                                    .await;
                            }
                        }
                    }
                    Some("content_block_delta") => {
                        if let Some(delta) = event.get("delta") {
                            match delta["type"].as_str() {
                                Some("text_delta") => {
                                    if let Some(text) = delta["text"].as_str() {
                                        assistant_text.push_str(text);
                                        let _ =
                                            tx.send(sse_event("text_delta", json!({"text": text}))).await;
                                    }
                                }
                                Some("input_json_delta") => {
                                    if let Some(partial) = delta["partial_json"].as_str() {
                                        active_tool_input.push_str(partial);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Some("content_block_stop") => {
                        if !active_tool_id.is_empty() {
                            tool_uses.push((
                                active_tool_id.clone(),
                                active_tool_name.clone(),
                                active_tool_input.clone(),
                            ));
                            active_tool_id.clear();
                            active_tool_name.clear();
                            active_tool_input.clear();
                        }
                    }
                    Some("message_delta") => {
                        if let Some(sr) = event["delta"]["stop_reason"].as_str() {
                            stop_reason = sr.to_string();
                        }
                        if let Some(u) = event.get("usage") {
                            total_output_tokens += u["output_tokens"].as_u64().unwrap_or(0);
                        }
                    }
                    Some("message_start") => {
                        if let Some(u) = event.get("message").and_then(|m| m.get("usage")) {
                            total_input_tokens += u["input_tokens"].as_u64().unwrap_or(0);
                            cache_read_tokens += u["cache_read_input_tokens"].as_u64().unwrap_or(0);
                            cache_write_tokens +=
                                u["cache_creation_input_tokens"].as_u64().unwrap_or(0);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Build assistant message content for conversation state
        let mut assistant_content: Vec<Value> = Vec::new();
        if !assistant_text.is_empty() {
            assistant_content.push(json!({"type": "text", "text": assistant_text}));
        }
        for (tu_id, tu_name, tu_input) in &tool_uses {
            let input: Value = serde_json::from_str(tu_input).unwrap_or(json!({}));
            assistant_content.push(json!({
                "type": "tool_use",
                "id": tu_id,
                "name": tu_name,
                "input": input
            }));
        }
        if !assistant_content.is_empty() {
            messages.push(json!({"role": "assistant", "content": assistant_content}));
        }

        if stop_reason != "tool_use" || tool_uses.is_empty() {
            info!(
                turn = turn + 1,
                stop_reason = %stop_reason,
                text_len = assistant_text.len(),
                input_tokens = total_input_tokens,
                output_tokens = total_output_tokens,
                "run_anthropic_stream: finished"
            );
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
            let state_c = state.clone();
            let tid = team_id.to_string();
            tokio::spawn(async move {
                track_chat_tokens(
                    &state_c,
                    &tid,
                    total_input_tokens,
                    total_output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                )
                .await;
            });
            return;
        }

        // Execute tools
        let mut tool_results: Vec<Value> = Vec::new();
        for (tu_id, tu_name, tu_input_str) in &tool_uses {
            let input: Value = serde_json::from_str(tu_input_str).unwrap_or(json!({}));
            let _ = tx
                .send(sse_event(
                    "tool_call",
                    json!({"id": tu_id, "name": tu_name, "input": input}),
                ))
                .await;

            let result = if let Some((server_id, tool_name)) = tu_name.split_once("__") {
                match invoke_mcp_tool(state, team_id, server_id, tool_name, &input).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(tool = tu_name.as_str(), error = %e, "MCP tool call failed");
                        json!(format!("Error: {e}"))
                    }
                }
            } else {
                json!(format!("Unknown tool: {tu_name}"))
            };
            let is_error = result.as_str().map(|s| s.starts_with("Error:")).unwrap_or(false);
            let is_empty = is_empty_tool_result(&result);
            let result_str = if is_empty {
                "No results found.".to_string()
            } else {
                compact_tool_result_for_model(&result)
            };
            let summary = if is_empty {
                "No results found".to_string()
            } else {
                summarize_tool_result(&result)
            };
            let status = if is_error { "error" } else { "success" };
            let _ = tx
                .send(sse_event(
                    "tool_result",
                    json!({"id": tu_id, "name": tu_name, "status": status, "summary": summary}),
                ))
                .await;

            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": tu_id,
                "content": result_str
            }));
        }

        messages.push(json!({"role": "user", "content": tool_results}));
        info!(
            turn = turn + 1,
            "Anthropic streaming tool use turn complete"
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
    let state_c = state.clone();
    let tid = team_id.to_string();
    tokio::spawn(async move {
        track_chat_tokens(
            &state_c,
            &tid,
            total_input_tokens,
            total_output_tokens,
            cache_read_tokens,
            cache_write_tokens,
        )
        .await;
    });
}

#[allow(clippy::collapsible_match)]
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
        .ok_or_else(|| {
            warn!("plan_chat_stream: missing or invalid Authorization header");
            StatusCode::UNAUTHORIZED
        })?;

    let claims = crate::auth::jwt::validate_token(token, &state.secrets.jwt_secret)
        .map_err(|e| {
            warn!("plan_chat_stream: JWT validation failed: {e}");
            StatusCode::UNAUTHORIZED
        })?;

    let messages_input = body["messages"].as_array().ok_or_else(|| {
        warn!("plan_chat_stream: missing messages array in body");
        StatusCode::BAD_REQUEST
    })?;
    if messages_input.is_empty() || messages_input.len() > 20 {
        warn!(
            count = messages_input.len(),
            "plan_chat_stream: invalid message count"
        );
        return Err(StatusCode::BAD_REQUEST);
    }

    info!(
        team_id = %claims.team_id,
        message_count = messages_input.len(),
        "plan_chat_stream: starting"
    );

    let ctx = load_plan_chat_context(&state, &claims.team_id, messages_input).await.map_err(|s| {
        error!(status = %s, team_id = %claims.team_id, "plan_chat_stream: load_plan_chat_context failed");
        s
    })?;
    let mut api_messages = ctx.messages;

    // Load team's Anthropic API key (required)
    let provider_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let anthropic_api_key = provider_result
        .as_ref()
        .and_then(|item| item.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned();

    let model_id = provider_result
        .as_ref()
        .and_then(|item| item.get("primary_model"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-4-6".to_string());

    // Move everything into the streaming task
    let team_id = claims.team_id.clone();
    let system_prompt = ctx.system_prompt;
    let tools = ctx.tools;

    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<axum::response::sse::Event, std::convert::Infallible>,
    >(64);

    info!(
        team_id = %team_id,
        model = %model_id,
        tools_count = tools.len(),
        has_api_key = anthropic_api_key.is_some(),
        "plan_chat_stream: spawning streaming task"
    );

    // Spawn the agentic streaming loop
    tokio::spawn(async move {
        if let Some(ref api_key) = anthropic_api_key {
            run_anthropic_stream(
                &state,
                &tx,
                api_key,
                &model_id,
                &system_prompt,
                &tools,
                &mut api_messages,
                &team_id,
            )
            .await;
        } else {
            warn!(team_id = %team_id, "plan_chat_stream: no Anthropic API key configured");
            let _ = tx
                .send(sse_event(
                    "error",
                    json!({ "message": "Anthropic API key not configured. Please set up your API key in Settings → Model Provider." }),
                ))
                .await;
        }
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

/// Check if a tool result is effectively empty (no meaningful data returned).
fn is_empty_tool_result(result: &Value) -> bool {
    // Notion-style: {"object":"list","results":[]}
    if let Some(arr) = result.get("results").and_then(|v| v.as_array()) {
        if arr.is_empty() {
            return true;
        }
    }
    // Generic empty array/object or null
    match result {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        Value::String(s) => s.is_empty() || s == "null" || s == "[]" || s == "{}",
        _ => false,
    }
}

/// Produce a short summary of a tool result for the UI.
fn summarize_tool_result(result: &Value) -> String {
    let s = result.to_string();
    if s.len() <= 120 {
        s
    } else {
        format!("{}…", s.chars().take(117).collect::<String>())
    }
}

/// Keep tool results compact before feeding back into the model to avoid runaway context growth.
fn compact_tool_result_for_model(result: &Value) -> String {
    let s = result.to_string();
    let max_chars = 3000usize;
    if s.chars().count() <= max_chars {
        return s;
    }
    let truncated: String = s.chars().take(max_chars).collect();
    format!("{truncated}… [truncated]")
}
async fn track_chat_tokens(
    state: &AppState,
    team_id: &str,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
) {
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(team_id))
            .key("period", attr_s(period))
            .update_expression(
                "ADD total_tokens_in :tin, total_tokens_out :tout, cache_read_tokens :cr, cache_write_tokens :cw",
            )
            .expression_attribute_values(":tin", attr_n(input_tokens))
            .expression_attribute_values(":tout", attr_n(output_tokens))
            .expression_attribute_values(":cr", attr_n(cache_read_tokens))
            .expression_attribute_values(":cw", attr_n(cache_write_tokens))
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
