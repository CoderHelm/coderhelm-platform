use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info};

use crate::models::{PlanExecuteMessage, PlanTaskContinueMessage};
use crate::WorkerState;

/// Execute a plan: iterate through approved tasks in order, creating GitHub issues or Jira tickets.
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

    // Load Jira config for create-ticket URL (only needed if any task targets Jira)
    let jira_create_url = load_jira_create_url(state, &msg.tenant_id).await;
    let jira_default_project = load_jira_default_project(state, &msg.tenant_id).await;

    // Ensure the "coderhelm" label exists in each repo we'll use (GitHub only)
    let mut ensured_repos = std::collections::HashSet::new();
    for task_id in &msg.tasks {
        if let Ok(Some(task)) = get_task(state, &msg.tenant_id, &msg.plan_id, task_id).await {
            if task.destination.as_deref() == Some("jira") {
                continue;
            }
            let r = task
                .repo
                .as_deref()
                .filter(|r| !r.is_empty())
                .unwrap_or(&plan_repo);
            if !r.is_empty() && ensured_repos.insert(r.to_string()) {
                let (owner, repo) = split_repo(r);
                if let Err(e) = github.ensure_label(owner, repo, "coderhelm").await {
                    error!(repo = r, error = %e, "Failed to ensure coderhelm label");
                }
            }
        }
    }

    // Execute tasks in order
    for task_id in &msg.tasks {
        let task = get_task(state, &msg.tenant_id, &msg.plan_id, task_id).await?;
        let Some(task) = task else {
            error!(task_id, "Task not found");
            continue;
        };

        // If task has a dependency, mark as waiting instead of processing
        if let Some(ref dep) = task.depends_on {
            if !dep.is_empty() {
                info!(task_id, depends_on = %dep, "Task has dependency — marking as waiting");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "waiting").await?;
                continue;
            }
        }

        // Update task status to "running"
        set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "running").await?;

        if task.destination.as_deref() == Some("jira") {
            // ── Create Jira ticket via Forge web trigger ──
            let project_key = task
                .jira_project
                .as_deref()
                .filter(|s| !s.is_empty())
                .or(jira_default_project.as_deref())
                .unwrap_or("");

            if project_key.is_empty() {
                error!(task_id, "No Jira project configured for task");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let Some(ref url) = jira_create_url else {
                error!(task_id, "No Jira create-ticket URL configured");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            };

            let description = format_issue_body(&task.description, &task.acceptance_criteria);
            let payload = serde_json::json!({
                "projectKey": project_key,
                "summary": task.title,
                "description": description,
                "labels": ["coderhelm"],
            });

            match state
                .http
                .post(url)
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let ticket_key = body["key"].as_str().unwrap_or("UNKNOWN");
                    let site_url = body["self"]
                        .as_str()
                        .and_then(|s| s.find("/rest/").map(|i| &s[..i]))
                        .unwrap_or("https://jira.atlassian.net");
                    let ticket_url = format!("{}/browse/{}", site_url, ticket_key);

                    update_task_with_jira(
                        state,
                        &msg.tenant_id,
                        &msg.plan_id,
                        task_id,
                        ticket_key,
                        &ticket_url,
                    )
                    .await?;

                    info!(task_id, ticket_key, "Created Jira ticket for task");
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    error!(task_id, status, body = %body, "Failed to create Jira ticket");
                    set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                }
                Err(e) => {
                    error!(task_id, error = %e, "Failed to call Forge create-ticket");
                    set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                }
            }
        } else {
            // ── Create GitHub issue (default) ──
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
                    // Label the issue FIRST to trigger the ticket pipeline
                    if let Err(e) = github
                        .add_label(owner, repo, issue_number, "coderhelm")
                        .await
                    {
                        error!(task_id, issue_number, error = %e, "Failed to add coderhelm label — issue won't auto-run");
                        set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed")
                            .await?;
                        continue;
                    }

                    // Only mark done after label is confirmed
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
    }

    // Query all tasks in this plan to see if any are waiting
    let all_tasks_result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk_prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(msg.tenant_id.clone()))
        .expression_attribute_values(
            ":sk_prefix",
            AttributeValue::S(format!("PLAN#{}#TASK#", msg.plan_id)),
        )
        .send()
        .await?;

    let any_waiting = all_tasks_result.items().iter().any(|item| {
        item.get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s == "waiting")
            .unwrap_or(false)
    });

    if any_waiting {
        // Mark plan as "waiting" instead of "done" — tasks are blocked on dependencies
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", AttributeValue::S(msg.tenant_id.clone()))
            .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
            .update_expression("SET #s = :s")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", AttributeValue::S("waiting".to_string()))
            .send()
            .await?;
        info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, "Plan has waiting tasks — not marking done");
    } else {
        // Mark plan as done
        let now = chrono::Utc::now().to_rfc3339();
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
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
    }
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
        .table_name(&state.config.plans_table_name)
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
    destination: Option<String>,
    jira_project: Option<String>,
    depends_on: Option<String>,
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
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(format!("PLAN#{plan_id}#TASK#{task_id}")),
        )
        .send()
        .await?;

    let str_field = |item: &std::collections::HashMap<String, AttributeValue>, key: &str| {
        item.get(key).and_then(|v| v.as_s().ok()).cloned()
    };

    Ok(result.item().map(|item| TaskData {
        title: str_field(item, "title").unwrap_or_default(),
        description: str_field(item, "description").unwrap_or_default(),
        acceptance_criteria: str_field(item, "acceptance_criteria").unwrap_or_default(),
        repo: str_field(item, "repo"),
        destination: str_field(item, "destination"),
        jira_project: str_field(item, "jira_project"),
        depends_on: str_field(item, "depends_on"),
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
        .table_name(&state.config.plans_table_name)
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
        .table_name(&state.config.plans_table_name)
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

async fn load_jira_create_url(state: &WorkerState, tenant_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("JIRA#config".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| {
            item.get("create_ticket_url")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
}

async fn load_jira_default_project(state: &WorkerState, tenant_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("JIRA#config".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| {
            item.get("default_project")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
}

async fn update_task_with_jira(
    state: &WorkerState,
    tenant_id: &str,
    plan_id: &str,
    task_id: &str,
    ticket_key: &str,
    ticket_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key(
            "sk",
            AttributeValue::S(format!("PLAN#{plan_id}#TASK#{task_id}")),
        )
        .update_expression(
            "SET #s = :s, jira_ticket_key = :jk, jira_ticket_url = :ju, completed_at = :ca",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
        .expression_attribute_values(":jk", AttributeValue::S(ticket_key.to_string()))
        .expression_attribute_values(":ju", AttributeValue::S(ticket_url.to_string()))
        .expression_attribute_values(":ca", AttributeValue::S(now))
        .send()
        .await?;
    Ok(())
}

/// Process waiting tasks whose dependencies have been met (triggered by PR merge).
pub async fn continue_tasks(
    state: &WorkerState,
    msg: PlanTaskContinueMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, tasks = msg.tasks.len(), "Continuing waiting plan tasks");

    let install_id = get_install_id(state, &msg.tenant_id).await?;
    let Some(install_id) = install_id else {
        error!(tenant_id = %msg.tenant_id, "Tenant not found for plan task continue");
        return Ok(());
    };

    let plan_repo = get_plan_repo(state, &msg.tenant_id, &msg.plan_id).await?;

    let github = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        install_id,
        &state.http,
    )?;

    let jira_create_url = load_jira_create_url(state, &msg.tenant_id).await;
    let jira_default_project = load_jira_default_project(state, &msg.tenant_id).await;

    // Ensure labels
    let mut ensured_repos = std::collections::HashSet::new();
    for task_id in &msg.tasks {
        if let Ok(Some(task)) = get_task(state, &msg.tenant_id, &msg.plan_id, task_id).await {
            if task.destination.as_deref() == Some("jira") {
                continue;
            }
            let r = task
                .repo
                .as_deref()
                .filter(|r| !r.is_empty())
                .unwrap_or(&plan_repo);
            if !r.is_empty() && ensured_repos.insert(r.to_string()) {
                let (owner, repo) = split_repo(r);
                let _ = github.ensure_label(owner, repo, "coderhelm").await;
            }
        }
    }

    for task_id in &msg.tasks {
        let task = get_task(state, &msg.tenant_id, &msg.plan_id, task_id).await?;
        let Some(task) = task else {
            error!(task_id, "Task not found");
            continue;
        };

        set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "running").await?;

        if task.destination.as_deref() == Some("jira") {
            let project_key = task
                .jira_project
                .as_deref()
                .filter(|s| !s.is_empty())
                .or(jira_default_project.as_deref())
                .unwrap_or("");

            if project_key.is_empty() {
                error!(task_id, "No Jira project configured for task");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let Some(ref url) = jira_create_url else {
                error!(task_id, "No Jira create-ticket URL configured");
                set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            };

            let description = format_issue_body(&task.description, &task.acceptance_criteria);
            let payload = serde_json::json!({
                "projectKey": project_key,
                "summary": task.title,
                "description": description,
                "labels": ["coderhelm"],
            });

            match state
                .http
                .post(url)
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let ticket_key = body["key"].as_str().unwrap_or("UNKNOWN");
                    let site_url = body["self"]
                        .as_str()
                        .and_then(|s| s.find("/rest/").map(|i| &s[..i]))
                        .unwrap_or("https://jira.atlassian.net");
                    let ticket_url = format!("{}/browse/{}", site_url, ticket_key);
                    update_task_with_jira(
                        state,
                        &msg.tenant_id,
                        &msg.plan_id,
                        task_id,
                        ticket_key,
                        &ticket_url,
                    )
                    .await?;
                    info!(task_id, ticket_key, "Created Jira ticket for waiting task");
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    error!(task_id, status, body = %body, "Failed to create Jira ticket for waiting task");
                    set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                }
                Err(e) => {
                    error!(task_id, error = %e, "Failed to call Forge create-ticket for waiting task");
                    set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                }
            }
        } else {
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
                    if let Err(e) = github
                        .add_label(owner, repo, issue_number, "coderhelm")
                        .await
                    {
                        error!(task_id, issue_number, error = %e, "Failed to add coderhelm label");
                        set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed")
                            .await?;
                        continue;
                    }
                    update_task_with_issue(
                        state,
                        &msg.tenant_id,
                        &msg.plan_id,
                        task_id,
                        issue_number,
                        &issue_url,
                    )
                    .await?;
                    info!(
                        task_id,
                        issue_number, "Created GitHub issue for waiting task"
                    );
                }
                Err(e) => {
                    error!(task_id, error = %e, "Failed to create GitHub issue for waiting task");
                    set_task_status(state, &msg.tenant_id, &msg.plan_id, task_id, "failed").await?;
                }
            }
        }
    }

    // Check if all plan tasks are now complete (no more waiting)
    let all_tasks_result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk_prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(msg.tenant_id.clone()))
        .expression_attribute_values(
            ":sk_prefix",
            AttributeValue::S(format!("PLAN#{}#TASK#", msg.plan_id)),
        )
        .send()
        .await?;

    let any_still_waiting = all_tasks_result.items().iter().any(|item| {
        item.get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s == "waiting")
            .unwrap_or(false)
    });

    if !any_still_waiting {
        let now = chrono::Utc::now().to_rfc3339();
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", AttributeValue::S(msg.tenant_id.clone()))
            .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
            .update_expression("SET #s = :s, executed_at = :ea")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
            .expression_attribute_values(":ea", AttributeValue::S(now))
            .send()
            .await?;
        info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, "All plan tasks complete — plan marked done");
    }

    Ok(())
}
