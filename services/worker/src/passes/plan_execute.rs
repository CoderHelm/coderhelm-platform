use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info};

use crate::models::{PlanExecuteMessage, PlanTaskContinueMessage};
use crate::WorkerState;

/// Execute a plan: iterate through approved tasks in order, creating GitHub issues or Jira tickets.
pub async fn run(
    state: &WorkerState,
    msg: PlanExecuteMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(team_id = %msg.team_id, plan_id = %msg.plan_id, tasks = msg.tasks.len(), "Starting plan execution");

    // Get team install_id
    let install_id = get_install_id(state, &msg.team_id).await?;
    let Some(install_id) = install_id else {
        error!(team_id = %msg.team_id, "Team not found for plan execute");
        return Ok(());
    };

    // Get the plan to find the repo
    let plan_repo = get_plan_repo(state, &msg.team_id, &msg.plan_id).await?;

    let github = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        install_id,
        &state.http,
    )?;

    // Load Jira config for create-ticket URL (only needed if any task targets Jira)
    let jira_create_url = load_jira_create_url(state, &msg.team_id).await;
    let jira_default_project = load_jira_default_project(state, &msg.team_id).await;
    let jira_site_url = load_jira_site_url(state, &msg.team_id).await;

    // Ensure the "coderhelm" label exists in each repo we'll use (GitHub only)
    let mut ensured_repos = std::collections::HashSet::new();
    for task_id in &msg.tasks {
        if let Ok(Some(task)) = get_task(state, &msg.team_id, &msg.plan_id, task_id).await {
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
        let task = get_task(state, &msg.team_id, &msg.plan_id, task_id).await?;
        let Some(task) = task else {
            error!(task_id, "Task not found");
            continue;
        };

        // Idempotency: SQS is at-least-once and a mid-batch crash redelivers
        // the whole PlanExecute message — without this, already-completed
        // tasks were re-run, creating duplicate GitHub issues/Jira tickets
        // (each duplicate then triggering a full duplicate agent run).
        if matches!(task.status.as_str(), "running" | "done" | "issue_created") {
            info!(task_id, status = %task.status, "Task already processed — skipping (redelivery)");
            continue;
        }

        // If task has a dependency, mark as waiting instead of processing
        if let Some(ref dep) = task.depends_on {
            if !dep.is_empty() {
                info!(task_id, depends_on = %dep, "Task has dependency — marking as waiting");
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "waiting").await?;
                continue;
            }
        }

        // Update task status to "running"
        set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "running").await?;

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
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let Some(ref url) = jira_create_url else {
                error!(task_id, "No Jira create-ticket URL configured");
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            };

            let description = format_issue_body(&task.description, &task.acceptance_criteria);
            let jira_repo = task
                .repo
                .as_deref()
                .filter(|r| !r.is_empty())
                .unwrap_or(&plan_repo);
            let mut jira_labels = vec!["coderhelm".to_string()];
            if !jira_repo.is_empty() {
                jira_labels.push(format!("coderhelm:{jira_repo}"));
            }
            let payload = serde_json::json!({
                "projectKey": project_key,
                "summary": task.title,
                "description": description,
                "labels": jira_labels,
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
                    link_ticket_to_plan(
                        state,
                        &msg.team_id,
                        &msg.plan_id,
                        task_id,
                        PlanTicket::Jira(ticket_key),
                    )
                    .await;
                    let ticket_url = if let Some(ref site) = jira_site_url {
                        format!("{}/browse/{}", site.trim_end_matches('/'), ticket_key)
                    } else {
                        // No site URL stored — just use the ticket key without a link
                        String::new()
                    };

                    update_task_with_jira(
                        state,
                        &msg.team_id,
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
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                }
                Err(e) => {
                    error!(task_id, error = %e, "Failed to call Forge create-ticket");
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
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
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let (owner, repo) = split_repo(task_repo);
            let issue_body = format_issue_body(&task.description, &task.acceptance_criteria);

            match github
                .create_issue(owner, repo, &task.title, &issue_body)
                .await
            {
                Ok((issue_number, issue_url)) => {
                    // Before the label: the label starts the run, and the
                    // run reads this to say it came from the plan.
                    link_ticket_to_plan(
                        state,
                        &msg.team_id,
                        &msg.plan_id,
                        task_id,
                        PlanTicket::Github {
                            repo: task_repo,
                            issue_number,
                        },
                    )
                    .await;
                    // Label the issue FIRST to trigger the ticket pipeline
                    if let Err(e) = github
                        .add_label(owner, repo, issue_number, "coderhelm")
                        .await
                    {
                        error!(task_id, issue_number, error = %e, "Failed to add coderhelm label — issue won't auto-run");
                        set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed")
                            .await?;
                        continue;
                    }

                    // Only mark done after label is confirmed
                    update_task_with_issue(
                        state,
                        &msg.team_id,
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
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
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
        .expression_attribute_values(":pk", AttributeValue::S(msg.team_id.clone()))
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
            .key("pk", AttributeValue::S(msg.team_id.clone()))
            .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
            .update_expression("SET #s = :s")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", AttributeValue::S("waiting".to_string()))
            .send()
            .await?;
        info!(team_id = %msg.team_id, plan_id = %msg.plan_id, "Plan has waiting tasks — not marking done");
    } else {
        // Mark plan as done
        let now = chrono::Utc::now().to_rfc3339();
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", AttributeValue::S(msg.team_id.clone()))
            .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
            .update_expression("SET #s = :s, executed_at = :ea, executed_by = :eb")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
            .expression_attribute_values(":ea", AttributeValue::S(now))
            .expression_attribute_values(":eb", AttributeValue::S(msg.triggered_by.clone()))
            .send()
            .await?;
        info!(team_id = %msg.team_id, plan_id = %msg.plan_id, "Plan execution complete");
    }
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// The ticket a plan's task opened.
pub(crate) enum PlanTicket<'a> {
    /// A GitHub issue in `owner/name`.
    Github { repo: &'a str, issue_number: u64 },
    /// A Jira ticket key.
    Jira(&'a str),
}

impl PlanTicket<'_> {
    /// Settings-table sort key of the link from the ticket to its plan task.
    pub(crate) fn sk(&self) -> String {
        match self {
            PlanTicket::Github { repo, issue_number } => {
                format!("PLAN_TICKET#github#{}#{issue_number}", repo.to_lowercase())
            }
            PlanTicket::Jira(key) => format!("PLAN_TICKET#jira#{}", key.to_uppercase()),
        }
    }

    /// The ticket id runs of this ticket carry.
    fn ticket_id(&self) -> String {
        match self {
            PlanTicket::Github { issue_number, .. } => format!("GH-{issue_number}"),
            PlanTicket::Jira(key) => key.to_string(),
        }
    }

    /// The repo a run must be in to be this ticket's (GitHub numbers repeat
    /// across repos; a Jira ticket's run can be in any repo).
    fn repo(&self) -> Option<String> {
        match self {
            PlanTicket::Github { repo, .. } => Some(repo.to_lowercase()),
            PlanTicket::Jira(_) => None,
        }
    }
}

/// The plan a run's ticket came from: `(plan_id, plan_title, plan_by)`.
pub(crate) async fn plan_of_ticket(
    state: &WorkerState,
    team_id: &str,
    ticket: &PlanTicket<'_>,
) -> Option<(String, String, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(ticket.sk()))
        .send()
        .await
        .ok()?
        .item?;
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let plan_id = s("plan_id");
    (!plan_id.is_empty()).then(|| (plan_id, s("plan_title"), s("plan_by")))
}

/// Records that a ticket came from a plan's task, so the runs it starts say
/// "Plan · <title>" instead of naming the bot that opened the issue. Also
/// marks a run that already started for it (a Jira webhook can beat this).
/// Best effort: a failure leaves the run labelled by its ticket's source.
async fn link_ticket_to_plan(
    state: &WorkerState,
    team_id: &str,
    plan_id: &str,
    task_id: &str,
    ticket: PlanTicket<'_>,
) {
    if matches!(ticket, PlanTicket::Jira(k) if k.is_empty() || k == "UNKNOWN") {
        return;
    }
    let plan = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(format!("PLAN#{plan_id}")))
        .projection_expression("title, executed_by")
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    let field = |k: &str| {
        plan.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .map(|s| common::truncate_str(s, 300).to_string())
            .unwrap_or_default()
    };
    let (title, by) = (field("title"), field("executed_by"));
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(ticket.sk()))
        .item("plan_id", AttributeValue::S(plan_id.to_string()))
        .item("task_id", AttributeValue::S(task_id.to_string()));
    if !title.is_empty() {
        put = put.item("plan_title", AttributeValue::S(title.clone()));
    }
    if !by.is_empty() {
        put = put.item("plan_by", AttributeValue::S(by.clone()));
    }
    if let Err(e) = put.send().await {
        tracing::warn!(team_id, plan_id, task_id, error = %e, "Could not link the ticket to its plan");
        return;
    }
    // A run that started before the link was written.
    let Ok(found) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":ticket", AttributeValue::S(ticket.ticket_id()))
        .send()
        .await
    else {
        return;
    };
    let want_repo = ticket.repo();
    for item in found.items() {
        let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).map(|s| s.as_str());
        if want_repo
            .as_deref()
            .is_some_and(|r| s("repo").map(str::to_lowercase).as_deref() != Some(r))
        {
            continue;
        }
        let Some(run_id) = s("run_id") else { continue };
        let mut update = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", AttributeValue::S(team_id.to_string()))
            .key("run_id", AttributeValue::S(run_id.to_string()))
            .condition_expression("attribute_exists(run_id)")
            .expression_attribute_values(":p", AttributeValue::S(plan_id.to_string()));
        let mut set = vec!["plan_id = :p"];
        if !title.is_empty() {
            set.push("plan_title = :pt");
            update = update.expression_attribute_values(":pt", AttributeValue::S(title.clone()));
        }
        if !by.is_empty() {
            set.push("plan_by = :pb");
            update = update.expression_attribute_values(":pb", AttributeValue::S(by.clone()));
        }
        if let Err(e) = update
            .update_expression(format!("SET {}", set.join(", ")))
            .send()
            .await
        {
            tracing::warn!(team_id, run_id, error = %e, "Could not mark the run as the plan's");
        }
    }
}

async fn get_install_id(
    state: &WorkerState,
    team_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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
    team_id: &str,
    plan_id: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(format!("PLAN#{plan_id}")))
        .send()
        .await?;

    Ok(result
        .item()
        .and_then(|item| item.get("repo").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default())
}

struct TaskData {
    status: String,
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
    team_id: &str,
    plan_id: &str,
    task_id: &str,
) -> Result<Option<TaskData>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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
        status: str_field(item, "status").unwrap_or_default(),
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
    team_id: &str,
    plan_id: &str,
    task_id: &str,
    status: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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
    team_id: &str,
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
        .key("pk", AttributeValue::S(team_id.to_string()))
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

async fn load_jira_create_url(state: &WorkerState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

async fn load_jira_default_project(state: &WorkerState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

async fn load_jira_site_url(state: &WorkerState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("JIRA#config".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| {
            item.get("site_url")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
}

async fn update_task_with_jira(
    state: &WorkerState,
    team_id: &str,
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
        .key("pk", AttributeValue::S(team_id.to_string()))
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
    info!(team_id = %msg.team_id, plan_id = %msg.plan_id, tasks = msg.tasks.len(), "Continuing waiting plan tasks");

    let install_id = get_install_id(state, &msg.team_id).await?;
    let Some(install_id) = install_id else {
        error!(team_id = %msg.team_id, "Team not found for plan task continue");
        return Ok(());
    };

    let plan_repo = get_plan_repo(state, &msg.team_id, &msg.plan_id).await?;

    let github = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        install_id,
        &state.http,
    )?;

    let jira_create_url = load_jira_create_url(state, &msg.team_id).await;
    let jira_default_project = load_jira_default_project(state, &msg.team_id).await;
    let jira_site_url = load_jira_site_url(state, &msg.team_id).await;

    // Ensure labels
    let mut ensured_repos = std::collections::HashSet::new();
    for task_id in &msg.tasks {
        if let Ok(Some(task)) = get_task(state, &msg.team_id, &msg.plan_id, task_id).await {
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
        let task = get_task(state, &msg.team_id, &msg.plan_id, task_id).await?;
        let Some(task) = task else {
            error!(task_id, "Task not found");
            continue;
        };

        set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "running").await?;

        if task.destination.as_deref() == Some("jira") {
            let project_key = task
                .jira_project
                .as_deref()
                .filter(|s| !s.is_empty())
                .or(jira_default_project.as_deref())
                .unwrap_or("");

            if project_key.is_empty() {
                error!(task_id, "No Jira project configured for task");
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let Some(ref url) = jira_create_url else {
                error!(task_id, "No Jira create-ticket URL configured");
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            };

            let description = format_issue_body(&task.description, &task.acceptance_criteria);
            let jira_repo = task
                .repo
                .as_deref()
                .filter(|r| !r.is_empty())
                .unwrap_or(&plan_repo);
            let mut jira_labels = vec!["coderhelm".to_string()];
            if !jira_repo.is_empty() {
                jira_labels.push(format!("coderhelm:{jira_repo}"));
            }
            let payload = serde_json::json!({
                "projectKey": project_key,
                "summary": task.title,
                "description": description,
                "labels": jira_labels,
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
                    link_ticket_to_plan(
                        state,
                        &msg.team_id,
                        &msg.plan_id,
                        task_id,
                        PlanTicket::Jira(ticket_key),
                    )
                    .await;
                    let ticket_url = if let Some(ref site) = jira_site_url {
                        format!("{}/browse/{}", site.trim_end_matches('/'), ticket_key)
                    } else {
                        String::new()
                    };
                    update_task_with_jira(
                        state,
                        &msg.team_id,
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
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                }
                Err(e) => {
                    error!(task_id, error = %e, "Failed to call Forge create-ticket for waiting task");
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
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
                set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
                continue;
            }

            let (owner, repo) = split_repo(task_repo);
            let issue_body = format_issue_body(&task.description, &task.acceptance_criteria);

            match github
                .create_issue(owner, repo, &task.title, &issue_body)
                .await
            {
                Ok((issue_number, issue_url)) => {
                    // Before the label: the label starts the run, and the
                    // run reads this to say it came from the plan.
                    link_ticket_to_plan(
                        state,
                        &msg.team_id,
                        &msg.plan_id,
                        task_id,
                        PlanTicket::Github {
                            repo: task_repo,
                            issue_number,
                        },
                    )
                    .await;
                    if let Err(e) = github
                        .add_label(owner, repo, issue_number, "coderhelm")
                        .await
                    {
                        error!(task_id, issue_number, error = %e, "Failed to add coderhelm label");
                        set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed")
                            .await?;
                        continue;
                    }
                    update_task_with_issue(
                        state,
                        &msg.team_id,
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
                    set_task_status(state, &msg.team_id, &msg.plan_id, task_id, "failed").await?;
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
        .expression_attribute_values(":pk", AttributeValue::S(msg.team_id.clone()))
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
            .key("pk", AttributeValue::S(msg.team_id.clone()))
            .key("sk", AttributeValue::S(format!("PLAN#{}", msg.plan_id)))
            .update_expression("SET #s = :s, executed_at = :ea")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", AttributeValue::S("done".to_string()))
            .expression_attribute_values(":ea", AttributeValue::S(now))
            .send()
            .await?;
        info!(team_id = %msg.team_id, plan_id = %msg.plan_id, "All plan tasks complete — plan marked done");
    }

    Ok(())
}
