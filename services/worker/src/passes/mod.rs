use crate::clients::email::{self, EmailEvent};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

/// Strip internal model IDs, Bedrock details, and service names from error messages
/// so they are safe to display to users.
fn sanitize_error(msg: &str) -> String {
    let mut s = msg.to_string();
    // Remove (model=us.anthropic.claude-...) patterns
    while let Some(start) = s.find("(model=") {
        if let Some(end) = s[start..].find(')') {
            s.replace_range(start..start + end + 1, "");
        } else {
            break;
        }
    }
    // Remove model IDs
    let patterns = [
        "us.anthropic.claude-opus-4-6-v1",
        "us.anthropic.claude-sonnet-4-6",
        "us.anthropic.claude-sonnet-4-20250514",
    ];
    for p in &patterns {
        s = s.replace(p, "");
    }
    s = s
        .replace(
            "Bedrock converse error",
            "An error occurred during processing",
        )
        .replace("service error", "service temporarily unavailable");
    // Collapse extra whitespace
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

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

    // Block processing if subscription is past_due (unpaid invoices)
    if !is_subscription_allowed(state, &msg.tenant_id).await {
        warn!(run_id, tenant_id = %msg.tenant_id, "Skipping ticket: subscription not active");
        return Ok(());
    }

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
            let sanitized = sanitize_error(&e.to_string());
            fail_run(state, &msg, &run_id, &sanitized, &usage, duration).await;
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

    // Load voice instructions from DynamoDB (repo-specific falls back to global)
    let voice = {
        let repo_voice = load_content(
            state,
            &msg.tenant_id,
            &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await;
        if repo_voice.is_empty() {
            load_content(state, &msg.tenant_id, "VOICE#GLOBAL").await
        } else {
            repo_voice
        }
    };

    // Post "working on it" comment only for GitHub-sourced tickets.
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format!(
                    "🔄 **Coderhelm is working on this**\n\n| Phase | Status |\n|-------|--------|\n| Triage | 🔄 In progress |\n| Plan | ⏳ Pending |\n| Implement | ⏳ Pending |\n| Review | ⏳ Pending |\n| PR | ⏳ Pending |\n\n[View run →](https://app.coderhelm.com/runs/detail?id={})",
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
    let branch_name = format!("coderhelm/{}", msg.ticket_id.to_lowercase());

    // Create working branch first
    github
        .create_branch(&msg.repo_owner, &msg.repo_name, &branch_name, "main")
        .await?;
    info!(run_id, branch = %branch_name, "Created working branch");

    // Commit openspec to the repo branch if enabled (default: on)
    let commit_openspec = load_workflow_setting(state, &msg.tenant_id, "commit_openspec").await;
    if commit_openspec {
        let ticket_slug = msg.ticket_id.to_lowercase();
        let spec_files: Vec<crate::clients::github::FileOp> = [
            ("proposal.md", &plan_result.proposal),
            ("design.md", &plan_result.design),
            ("tasks.md", &plan_result.tasks),
            ("spec.md", &plan_result.spec),
        ]
        .iter()
        .filter(|(_, content)| !content.is_empty())
        .map(|(name, content)| crate::clients::github::FileOp::Write {
            path: format!("openspec/specs/{ticket_slug}/{name}"),
            content: content.to_string(),
        })
        .collect();

        if !spec_files.is_empty() {
            if let Err(e) = github
                .batch_write(
                    &msg.repo_owner,
                    &msg.repo_name,
                    &branch_name,
                    &format!("Add openspec for {}", msg.ticket_id),
                    &spec_files,
                )
                .await
            {
                warn!(run_id, error = %e, "Failed to commit openspec to branch");
            }
        }
    }

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

    // Mark all tasks as done in S3 openspec so retries/dashboard see progress
    let tasks_key = format!(
        "tenants/{}/runs/{}/openspec/tasks.md",
        msg.tenant_id,
        msg.ticket_id.to_lowercase()
    );
    let checked = plan_result.tasks.replace("- [ ]", "- [x]");
    if let Err(e) = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(&tasks_key)
        .body(checked.as_bytes().to_vec().into())
        .content_type("text/markdown")
        .send()
        .await
    {
        warn!(run_id, error = %e, "Failed to update tasks.md with checked items");
    }

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
                    "✅ **Coderhelm completed this ticket**\n\n**PR**: {}\n**Files**: {} modified\n\n[View run →](https://app.coderhelm.com/runs/detail?id={})",
                    pr_result.pr_url,
                    impl_result.files_modified.len(),
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
        .item("installation_id", attr_n(msg.installation_id))
        .item("issue_number", attr_n(msg.issue_number))
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
    let entry = aws_sdk_dynamodb::types::AttributeValue::M(std::collections::HashMap::from([
        ("pass".to_string(), attr_s(pass)),
        ("started_at".to_string(), attr_s(&now)),
    ]));
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("tenant_id", attr_s(tenant_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET current_pass = :p, updated_at = :t, pass_history = list_append(if_not_exists(pass_history, :empty), :entry)",
        )
        .expression_attribute_values(":p", attr_s(pass))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":entry", aws_sdk_dynamodb::types::AttributeValue::L(vec![entry]))
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
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
             status_run_id = :sri, files_modified = :fm",
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
        .expression_attribute_values(
            ":fm",
            AttributeValue::L(
                impl_result
                    .files_modified
                    .iter()
                    .map(|f| attr_s(f))
                    .collect(),
            ),
        )
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

    // Report token overage to Stripe (after analytics are updated)
    let total_tokens = usage.input_tokens + usage.output_tokens;
    crate::clients::billing::report_token_overage(state, &msg.tenant_id, total_tokens).await;

    // Send run-complete notification
    let duration_str = format!("{}m {}s", duration / 60, duration % 60);
    let tokens_str = if total_tokens >= 1_000_000 {
        format!("{:.1}M", total_tokens as f64 / 1_000_000.0)
    } else if total_tokens >= 1_000 {
        format!("{:.1}k", total_tokens as f64 / 1_000.0)
    } else {
        total_tokens.to_string()
    };
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
            tokens: tokens_str,
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
        "Never commit secrets, credentials, API keys, tokens, or sensitive information. Use environment variables or secret managers instead."
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

/// Truncate file contents to limit token usage.
/// Keeps first ~32KB (~8K tokens) and appends a truncation notice.
pub fn truncate_content(content: &str, path: &str) -> String {
    const MAX_BYTES: usize = 32_000;
    if content.len() <= MAX_BYTES {
        return content.to_string();
    }
    let cut = content[..MAX_BYTES].rfind('\n').unwrap_or(MAX_BYTES);
    format!(
        "{}\n\n... (truncated — {path} is {} bytes, showing first {cut} bytes)",
        &content[..cut],
        content.len(),
    )
}

/// Truncate file tree output to limit token usage on large repos.
/// Keeps first 2000 entries.
pub fn truncate_tree(paths: &[&str]) -> String {
    const MAX_ENTRIES: usize = 2000;
    if paths.len() <= MAX_ENTRIES {
        return paths.join("\n");
    }
    let mut out = paths[..MAX_ENTRIES].join("\n");
    out.push_str(&format!(
        "\n\n... ({} more files not shown, {} total)",
        paths.len() - MAX_ENTRIES,
        paths.len(),
    ));
    out
}

/// Load a text content field from single-table DynamoDB (voice, agents, etc.).
pub(crate) async fn load_content(state: &WorkerState, tenant_id: &str, sk: &str) -> String {
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

/// Load a boolean workflow setting from SETTINGS#WORKFLOW. Returns default (true) if not found.
async fn load_workflow_setting(state: &WorkerState, tenant_id: &str, key: &str) -> bool {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("SETTINGS#WORKFLOW".to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get(key))
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Check if tenant subscription allows processing (active or free).
async fn is_subscription_allowed(state: &WorkerState, tenant_id: &str) -> bool {
    let status = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("BILLING".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|item| {
            item.get("subscription_status")
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .unwrap_or_default();

    matches!(status.as_str(), "active" | "free" | "trialing")
}
