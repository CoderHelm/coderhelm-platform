use crate::clients::email::{self, EmailEvent};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

pub mod ci_fix;
pub mod feedback;
mod implement;
pub mod infra_analyze;
pub mod onboard;
mod plan;
pub mod plan_execute;
mod pr;
mod review;
mod triage;

/// Main orchestration: run all passes for a new ticket.
pub async fn orchestrate_ticket(
    state: &WorkerState,
    msg: TicketMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = ulid::Ulid::new().to_string();
    let mut usage = TokenUsage::default();
    let start = std::time::Instant::now();

    info!(run_id, ticket_id = %msg.ticket_id, "Orchestration started");

    // Create run record
    create_run_record(state, &msg, &run_id).await?;

    match run_passes(state, &msg, &run_id, &mut usage, &start).await {
        Ok(_) => {
            let duration = start.elapsed().as_secs();
            info!(run_id, duration, "Orchestration complete");
        }
        Err(e) => {
            error!(run_id, error = %e, "Orchestration failed");
            let duration = start.elapsed().as_secs();
            fail_run(state, &msg, &run_id, &e.to_string(), &usage, duration).await;
            return Err(e);
        }
    }

    Ok(())
}

async fn run_passes(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &mut TokenUsage,
    start: &std::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize GitHub client for this tenant
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Load must-rules (global + repo-specific)
    let rules = load_rules(state, msg).await;

    // Load voice instructions from DynamoDB
    let voice = load_content(
        state,
        &msg.tenant_id,
        &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
    )
    .await;

    // Post "working on it" comment only for GitHub-sourced tickets.
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format!(
                    "🔄 **d3ftly is working on this**\n\n| Phase | Status |\n|-------|--------|\n| Triage | 🔄 In progress |\n| Plan | ⏳ Pending |\n| Implement | ⏳ Pending |\n| Review | ⏳ Pending |\n| PR | ⏳ Pending |\n\n[View run →](https://app.d3ftly.com/dashboard/runs/{})",
                    run_id,
                ),
            )
            .await?;
    }

    // --- Pass 1: Triage ---
    update_pass(state, &msg.tenant_id, run_id, "triage").await?;
    let triage_result = triage::run(state, msg, &github, usage).await?;
    info!(run_id, "Triage complete");

    // --- Pass 2: Plan ---
    update_pass(state, &msg.tenant_id, run_id, "plan").await?;
    let plan_result = plan::run(state, msg, &github, &triage_result, usage).await?;
    info!(run_id, "Plan complete");

    // --- Pass 3: Implement ---
    update_pass(state, &msg.tenant_id, run_id, "implement").await?;
    let branch_name = format!("d3ftly/{}", msg.ticket_id.to_lowercase());
    let impl_result = implement::run(
        state,
        msg,
        &github,
        &plan_result,
        &branch_name,
        &rules,
        usage,
    )
    .await?;
    info!(
        run_id,
        files = impl_result.files_modified.len(),
        "Implement complete"
    );

    // --- Pass 4: Review ---
    update_pass(state, &msg.tenant_id, run_id, "review").await?;
    review::run(state, msg, &github, &branch_name, &rules, usage).await?;
    info!(run_id, "Review complete");

    // --- Pass 5: Create PR ---
    update_pass(state, &msg.tenant_id, run_id, "pr").await?;
    let pr_result = pr::run(
        state,
        msg,
        &github,
        &branch_name,
        &plan_result,
        &voice,
        usage,
    )
    .await?;
    info!(run_id, pr_url = %pr_result.pr_url, "PR created");

    // Update run record with final state
    let duration = start.elapsed().as_secs();
    complete_run(
        state,
        msg,
        run_id,
        &pr_result,
        &impl_result,
        usage,
        duration,
    )
    .await?;

    // Post success comment only for GitHub-sourced tickets.
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format!(
                    "✅ **d3ftly completed this ticket**\n\n**PR**: {}\n**Files**: {} modified\n**Cost**: ${:.2}\n\n[View run →](https://app.d3ftly.com/dashboard/runs/{})",
                    pr_result.pr_url,
                    impl_result.files_modified.len(),
                    usage.estimated_cost(),
                    run_id,
                ),
            )
            .await?;
    }

    Ok(())
}

async fn create_run_record(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    state
        .dynamo
        .put_item()
        .table_name(&state.config.runs_table_name)
        .item("tenant_id", attr_s(&msg.tenant_id))
        .item("run_id", attr_s(run_id))
        .item("status", attr_s("running"))
        // Composite SK for status-index GSI: "running#<run_id>" for efficient status queries
        .item("status_run_id", attr_s(&format!("running#{run_id}")))
        // Composite key for repo-index GSI
        .item(
            "tenant_repo",
            attr_s(&format!("{}#{}", msg.tenant_id, repo)),
        )
        .item(
            "ticket_source",
            attr_s(match msg.source {
                TicketSource::Github => "github",
                TicketSource::Jira => "jira",
            }),
        )
        .item("ticket_id", attr_s(&msg.ticket_id))
        .item("title", attr_s(&msg.title))
        .item("repo", attr_s(&repo))
        .item("tokens_in", attr_n(0))
        .item("tokens_out", attr_n(0))
        .item("cost_usd", attr_n(0))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await?;

    // Increment analytics counters (current month + all-time)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&msg.tenant_id))
            .key("period", attr_s(period))
            .update_expression("ADD total_runs :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await?;
    }

    Ok(())
}

async fn update_pass(
    state: &WorkerState,
    tenant_id: &str,
    run_id: &str,
    pass: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(tenant_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET current_pass = :p, updated_at = :t")
        .expression_attribute_values(":p", attr_s(pass))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await?;
    Ok(())
}

async fn complete_run(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr: &pr::PrResult,
    impl_result: &implement::ImplementResult,
    usage: &TokenUsage,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    // Update run record in runs table
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(&msg.tenant_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("completed"))
        .expression_attribute_values(":pr", attr_s(&pr.pr_url))
        .expression_attribute_values(":pn", attr_n(pr.pr_number))
        .expression_attribute_values(":b", attr_s(&pr.branch))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
        .send()
        .await?;

    // Update analytics counters (current month + all-time)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&msg.tenant_id))
            .key("period", attr_s(period))
            .update_expression(
                "ADD completed :one, total_cost_usd :cost, \
                 total_tokens_in :ti, total_tokens_out :to, \
                 cache_read_tokens :cr, cache_write_tokens :cw",
            )
            .expression_attribute_values(":one", attr_n(1))
            .expression_attribute_values(":cost", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":cr", attr_n(usage.cache_read_tokens))
            .expression_attribute_values(":cw", attr_n(usage.cache_write_tokens))
            .send()
            .await?;
    }

    // Increment tenant's monthly run count in main table
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&msg.tenant_id))
        .key("sk", attr_s("META"))
        .update_expression("ADD run_count_mtd :one")
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await?;

    // Send run-complete notification
    let duration_str = format!("{}m {}s", duration / 60, duration % 60);
    if let Err(e) = email::send_notification(
        state,
        &msg.tenant_id,
        EmailEvent::RunComplete {
            run_id: run_id.to_string(),
            title: msg.title.clone(),
            repo: format!("{}/{}", msg.repo_owner, msg.repo_name),
            pr_url: pr.pr_url.clone(),
            files_modified: impl_result.files_modified.len(),
            duration: duration_str,
            cost: format!("{:.2}", cost),
        },
    )
    .await
    {
        error!("Failed to send run-complete email: {e}");
    }

    Ok(())
}

async fn fail_run(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    error_msg: &str,
    usage: &TokenUsage,
    duration: u64,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    // Update run record to failed
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(&msg.tenant_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, error_message = :err, tokens_in = :ti, \
             tokens_out = :to, cost_usd = :c, duration_s = :d, \
             updated_at = :t, status_run_id = :sri",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("failed"))
        .expression_attribute_values(":err", attr_s(&error_msg[..error_msg.len().min(500)]))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":sri", attr_s(&format!("failed#{run_id}")))
        .send()
        .await;

    // Update analytics counters
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("tenant_id", attr_s(&msg.tenant_id))
            .key("period", attr_s(period))
            .update_expression("ADD failed :one, total_cost_usd :cost")
            .expression_attribute_values(":one", attr_n(1))
            .expression_attribute_values(":cost", attr_n(format!("{:.4}", cost)))
            .send()
            .await;
    }

    // Send run-failed notification
    if let Err(e) = email::send_notification(
        state,
        &msg.tenant_id,
        EmailEvent::RunFailed {
            run_id: run_id.to_string(),
            title: msg.title.clone(),
            repo: format!("{}/{}", msg.repo_owner, msg.repo_name),
            error: error_msg[..error_msg.len().min(200)].to_string(),
        },
    )
    .await
    {
        error!("Failed to send run-failed email: {e}");
    }
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Load must-rules (global + repo-specific) from DynamoDB and merge them.
async fn load_rules(state: &WorkerState, msg: &TicketMessage) -> Vec<String> {
    // Hardcoded safety rules that always apply
    let mut rules = vec![
        "Never push directly to the default/main branch. Always create a feature branch."
            .to_string(),
    ];

    // Load global rules
    if let Some(global) = load_rule_list(state, &msg.tenant_id, "RULES#GLOBAL").await {
        rules.extend(global);
    }

    // Load repo-specific rules
    let repo_sk = format!("RULES#REPO#{}/{}", msg.repo_owner, msg.repo_name);
    if let Some(repo_rules) = load_rule_list(state, &msg.tenant_id, &repo_sk).await {
        rules.extend(repo_rules);
    }

    info!(count = rules.len(), "Loaded must-rules");
    rules
}

async fn load_rule_list(state: &WorkerState, tenant_id: &str, sk: &str) -> Option<Vec<String>> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get("rules"))
            .and_then(|v| v.as_l().ok())
            .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect()),
        Err(e) => {
            warn!(error = %e, "Failed to load rules from {sk}");
            None
        }
    }
}

/// Format must-rules as a string block for injection into system prompts.
pub fn format_rules_block(rules: &[String]) -> String {
    if rules.is_empty() {
        return String::new();
    }
    let mut block =
        String::from("\n\n## Must-Rules (MANDATORY — violating any of these is a failure)\n");
    for rule in rules {
        block.push_str(&format!("- {rule}\n"));
    }
    block
}

/// Load a text content field from single-table DynamoDB (voice, agents, etc.).
async fn load_content(state: &WorkerState, tenant_id: &str, sk: &str) -> String {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get("content"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        Err(e) => {
            warn!(error = %e, "Failed to load content from {sk}");
            String::new()
        }
    }
}
