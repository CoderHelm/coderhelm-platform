use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info};

use crate::models::PlanExecuteMessage;
use crate::WorkerState;

/// Execute a plan: iterate through approved tasks in order, creating GitHub issues.
pub async fn run(
    state: &WorkerState,
    msg: PlanExecuteMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, tasks = msg.tasks.len(), "Starting plan execution");

    // Get tenant install_id
    let install_id = get_install_id(state, &msg.tenant_id).await?;
    let Some(install_id) = install_id else {
        error!(tenant_id = %msg.tenant_id, "Tenant not found for plan execute");
        return Ok(());
    };

    // Get the plan to find the repo
    let plan_repo = get_plan_repo(state, &msg.tenant_id, &msg.plan_id).await?;

    let github = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        install_id,
        &state.http,
    )?;

    // Execute tasks in order
    for task_id in &msg.tasks {
        let task = get_task(state, &msg.tenant_id, &msg.plan_id, task_id).await?;
        let Some(task) = task else {
            error!(task_id, "Task not found");
            continue;
        };

        // Update task status to "running"
        set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "running").await?;

        // Use task-level repo if set, otherwise fall back to plan repo
        let task_repo = task
            .repo
            .as_deref()
            .filter(|r| !r.is_empty())
            .unwrap_or(&plan_repo);
        if task_repo.is_empty() {
            error!(task_id, "No repo configured for task or plan");
            set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
            continue;
        }

        let (owner, repo) = split_repo(task_repo);
        let issue_body = format_issue_body(&task.description, &task.acceptance_criteria);

        match github
            .create_issue(owner, repo, &task.title, &issue_body)
            .await
        {
            Ok((issue_number, issue_url)) => {
                // Update task with issue info and mark done
                update_task_with_issue(
                    state,
                    &msg.tenant_id,
                    &msg.plan_id,
                    task_id,
                    issue_number,
                    &issue_url,
                )
                .await?;
                info!(task_id, issue_number, "Created GitHub issue for task");
            }
            Err(e) => {
                error!(task_id, error = %e, "Failed to create GitHub issue");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
            }
        }
    }

    // Mark plan as done
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(msg.tenant_id.clone()))
        .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
        .update_expression("SET #s = :s, executed_at = :ea, executed_by = :eb")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
        .expression_attribute_values(":ea", AttributeValue::S(now))
        .expression_attribute_values(":eb", AttributeValue::S(msg.triggered_by.clone()))
        .send()
        .await?;

    info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, "Plan execution complete");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn get_install_id(
    state: &WorkerState,
    tenant_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("META".to_string()))
        .send()
        .await?;

    Ok(result.item().and_then(|item| {
        item.get("github_install_id")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
    }))
}

async fn get_plan_repo(
    state: &WorkerState,
    tenant_id: &str,
    plan_id: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S(format!("PLAN#{plan_id}")))
        .send()
        .await?;

    Ok(result
        .item()
        .and_then(|item| item.get("repo").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default())
}

struct TaskData {
    title: String,
    description: String,
    acceptance_criteria: String,
    repo: Option<String>,
}

async fn get_task(
    state: &WorkerState,
    tenant_id: &str,
    plan_id: &str,
    task_id: &str,
) -> Result<Option<TaskData>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(format!("PLAN#{plan_id}#TASK#{task_id}")),
        )
        .send()
        .await?;

    Ok(result.item().map(|item| TaskData {
        title: item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        description: item
            .get("description")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        acceptance_criteria: item
            .get("acceptance_criteria")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        repo: item.get("repo").and_then(|v| v.as_s().ok()).cloned(),
    }))
}

async fn set_task_status(
    state: &WorkerState,
    tenant_id: &str,
    plan_id: &str,
    task_id: &str,
    status: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(format!("PLAN#{plan_id}#TASK#{task_id}")),
        )
        .update_expression("SET #s = :s")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", AttributeValue::S(status.to_string()))
        .send()
        .await?;
    Ok(())
}

async fn update_task_with_issue(
    state: &WorkerState,
    tenant_id: &str,
    plan_id: &str,
    task_id: &str,
    issue_number: u64,
    issue_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(format!("PLAN#{plan_id}#TASK#{task_id}")),
        )
        .update_expression("SET #s = :s, issue_number = :in, issue_url = :iu, completed_at = :ca")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
        .expression_attribute_values(":in", AttributeValue::N(issue_number.to_string()))
        .expression_attribute_values(":iu", AttributeValue::S(issue_url.to_string()))
        .expression_attribute_values(":ca", AttributeValue::S(now))
        .send()
        .await?;
    Ok(())
}

fn format_issue_body(description: &str, acceptance_criteria: &str) -> String {
    let mut body = description.to_string();
    if !acceptance_criteria.is_empty() {
        body.push_str("\n\n## Acceptance Criteria\n\n");
        body.push_str(acceptance_criteria);
    }
    body
}

fn split_repo(repo_full: &str) -> (&str, &str) {
    let mut parts = repo_full.splitn(2, '/');
    let owner = parts.next().unwrap_or("");
    let repo = parts.next().unwrap_or("");
    (owner, repo)
}
