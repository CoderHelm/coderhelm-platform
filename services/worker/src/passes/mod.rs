use crate::agent::mcp;
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
pub mod formatter;
mod implement;
pub mod infra_analyze;
pub mod onboard;
mod plan;
pub mod plan_execute;
mod pr;
mod resolve;
mod review;
mod security;
mod test;
mod triage;

/// Minimal MCP plugin catalog: (server_id, npx_package, env_mapping).
/// Kept in sync with the full catalog in gateway/routes/plugins.rs.
#[allow(clippy::type_complexity)]
pub const MCP_CATALOG: [(&str, &str, &[(&str, &str)]); 17] = [
    (
        "figma",
        "figma-developer-mcp",
        &[("api_token", "FIGMA_API_KEY")],
    ),
    (
        "sentry",
        "@sentry/mcp-server",
        &[
            ("auth_token", "SENTRY_AUTH_TOKEN"),
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
        "@vercel/mcp",
        &[("api_token", "VERCEL_API_TOKEN")],
    ),
    ("stripe", "@stripe/mcp", &[("api_key", "STRIPE_SECRET_KEY")]),
    (
        "cloudflare",
        "@cloudflare/mcp-server-cloudflare",
        &[
            ("api_token", "CLOUDFLARE_API_TOKEN"),
            ("account_id", "CLOUDFLARE_ACCOUNT_ID"),
        ],
    ),
    (
        "posthog",
        "@nicholasoxford/posthog-mcp",
        &[("api_key", "POSTHOG_API_KEY"), ("host", "POSTHOG_HOST")],
    ),
    (
        "gitlab",
        "@anthropic-ai/gitlab-mcp-server",
        &[("api_token", "GITLAB_TOKEN"), ("base_url", "GITLAB_URL")],
    ),
    (
        "neon",
        "@neondatabase/mcp-server-neon",
        &[("api_key", "NEON_API_KEY")],
    ),
    (
        "turso",
        "@tursodatabase/turso-mcp",
        &[("api_token", "TURSO_API_TOKEN"), ("org_name", "TURSO_ORG")],
    ),
    ("snyk", "snyk-mcp-server", &[("api_token", "SNYK_TOKEN")]),
    (
        "launchdarkly",
        "@launchdarkly/mcp-server",
        &[("api_key", "LAUNCHDARKLY_ACCESS_TOKEN")],
    ),
    (
        "mongodb",
        "mongodb-mcp-server",
        &[
            ("public_key", "MDB_MCP_API_PUBLIC_KEY"),
            ("private_key", "MDB_MCP_API_PRIVATE_KEY"),
            ("group_id", "MDB_MCP_API_GROUP_ID"),
        ],
    ),
    (
        "grafana",
        "mcp-grafana",
        &[("api_key", "GRAFANA_API_KEY"), ("base_url", "GRAFANA_URL")],
    ),
    (
        "redis",
        "@redis/mcp-server",
        &[("connection_url", "REDIS_URL")],
    ),
    (
        "upstash",
        "@anthropic-ai/upstash-mcp-server",
        &[("email", "UPSTASH_EMAIL"), ("api_key", "UPSTASH_API_KEY")],
    ),
];

/// Main orchestration: run all passes for a new ticket.
pub async fn orchestrate_ticket(
    state: &WorkerState,
    mut msg: TicketMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = ulid::Ulid::new().to_string();
    let mut usage = TokenUsage::default();
    let start = std::time::Instant::now();

    info!(run_id, ticket_id = %msg.ticket_id, "Orchestration started");

    // Block processing if subscription is past_due (unpaid invoices)
    if !is_subscription_allowed(state, &msg.team_id).await {
        warn!(run_id, team_id = %msg.team_id, "Skipping ticket: subscription not active");
        return Ok(());
    }

    // Dedup: skip if another run for this ticket is already in progress
    if is_ticket_already_running(state, &msg).await {
        warn!(
            run_id,
            ticket_id = %msg.ticket_id,
            "Skipping duplicate: another run for this ticket is already active"
        );
        return Ok(());
    }

    // Create run record
    create_run_record(state, &msg, &run_id).await?;

    match run_passes(state, &mut msg, &run_id, &mut usage, &start).await {
        Ok(_) => {
            let duration = start.elapsed().as_secs();
            info!(run_id, duration, "Orchestration complete");
        }
        Err(e) => {
            let duration = start.elapsed().as_secs();
            let err_msg = e.to_string();
            if err_msg.contains("cancelled by user") {
                info!(run_id, duration, "Run cancelled — preserving tokens");
                // Update token counts but keep status as cancelled
                let cost = usage.estimated_cost();
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.runs_table_name)
                    .key("team_id", attr_s(&msg.team_id))
                    .key("run_id", attr_s(&run_id))
                    .update_expression("SET tokens_in = :ti, tokens_out = :to, cost_usd = :c, duration_s = :d, updated_at = :t")
                    .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                    .expression_attribute_values(":to", attr_n(usage.output_tokens))
                    .expression_attribute_values(":c", attr_s(&format!("{cost:.6}")))
                    .expression_attribute_values(":d", attr_n(duration))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
                return Ok(());
            }
            error!(run_id, error = %e, "Orchestration failed");
            let sanitized = sanitize_error(&err_msg);
            fail_run(state, &msg, &run_id, &sanitized, &usage, duration).await;
            return Err(e);
        }
    }

    Ok(())
}

async fn run_passes(
    state: &WorkerState,
    msg: &mut TicketMessage,
    run_id: &str,
    usage: &mut TokenUsage,
    start: &std::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Resolve installation_id: if 0 (e.g. Jira-sourced tickets), look it up from team META
    if msg.installation_id == 0 {
        msg.installation_id = lookup_team_installation_id(state, &msg.team_id).await?;
        info!(
            run_id,
            installation_id = msg.installation_id,
            "Resolved installation_id from team META"
        );
    }

    // Initialize GitHub client for this team
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
            &msg.team_id,
            &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await;
        if repo_voice.is_empty() {
            load_content(state, &msg.team_id, "VOICE#GLOBAL").await
        } else {
            repo_voice
        }
    };

    // Load repo-level instruction files (AGENTS.md, CLAUDE.md, copilot-instructions, etc.)
    let repo_instructions = load_repo_instructions(&github, &msg.repo_owner, &msg.repo_name).await;

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

    // --- Auto-resolve repo for Jira tickets with bare "coderhelm" label ---
    if msg.repo_owner.is_empty() || msg.repo_name.is_empty() {
        if matches!(msg.source, TicketSource::Jira) {
            let repos = plan::fetch_team_repos(state, &msg.team_id).await;
            if repos.is_empty() {
                return Err("No enabled repos found for team — cannot auto-pick repo".into());
            }
            if repos.len() == 1 {
                // Single repo — no need to triage
                let (owner, name) = repos[0].split_once('/').unwrap_or(("", ""));
                msg.repo_owner = owner.to_string();
                msg.repo_name = name.to_string();
                info!(run_id, repo = %repos[0], "Auto-selected single repo");
            } else {
                // Multiple repos — ask triage to pick
                let selected = triage::select_repo(state, msg, &repos, usage).await?;
                let (owner, name) = selected.split_once('/').unwrap_or(("", ""));
                msg.repo_owner = owner.to_string();
                msg.repo_name = name.to_string();
                info!(run_id, repo = %selected, "Triage selected repo");
            }
            // Update run record with resolved repo
            let resolved_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
            if let Err(e) = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression("SET repo = :r, team_repo = :tr")
                .expression_attribute_values(":r", attr_s(&resolved_repo))
                .expression_attribute_values(
                    ":tr",
                    attr_s(&format!("{}#{}", msg.team_id, resolved_repo)),
                )
                .send()
                .await
            {
                error!(run_id, error = %e, "Failed to update run with resolved repo");
            }
        } else {
            return Err("repo_owner and repo_name are required for GitHub tickets".into());
        }
    }

    // Load enabled MCP plugins once for this run
    let mcp_table = if state.config.mcp_configs_table_name.is_empty() {
        &state.config.settings_table_name
    } else {
        &state.config.mcp_configs_table_name
    };
    let mcp_plugins =
        mcp::load_team_plugins(&state.dynamo, mcp_table, &msg.team_id, &MCP_CATALOG).await;
    let mcp_server_ids: Vec<String> = mcp_plugins.iter().map(|p| p.server_id.clone()).collect();
    if !mcp_server_ids.is_empty() {
        info!(run_id, servers = ?mcp_server_ids, "MCP servers active for run");
    }

    // --- Checkpoint Resume: skip expensive passes if we've already completed them ---
    let checkpoint = load_checkpoint(state, &msg.team_id, run_id).await;
    let can_skip_implement = checkpoint.as_ref().is_some_and(|(last_pass, branch, _)| {
        !branch.is_empty()
            && matches!(
                last_pass.as_str(),
                "implement" | "test" | "review" | "security"
            )
    });

    if let Some((ref last_pass, ref branch, ref cycle)) = checkpoint {
        info!(run_id, last_pass, branch, cycle, "Found checkpoint for run");
    }

    // --- Pass 1: Triage ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "triage").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let triage_result = triage::run(state, msg, &github, usage).await?;
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "triage",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    save_checkpoint(state, &msg.team_id, run_id, "triage", "", 0, usage).await;
    info!(run_id, "Triage complete");

    // --- Resolve external references via MCP tools ---
    let mut triage_result = triage_result;
    if !mcp_plugins.is_empty() && !state.config.mcp_proxy_function_name.is_empty() {
        let external_context = resolve::run(state, msg, &mcp_plugins, usage).await;
        if !external_context.is_empty() {
            triage_result.summary.push_str(&external_context);
            info!(run_id, "Resolved external references via MCP");
        }
    }

    // --- Pass 2: Plan ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "plan").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let plan_result = plan::run(state, msg, &github, &triage_result, usage).await?;
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "plan",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    save_checkpoint(state, &msg.team_id, run_id, "plan", "", 0, usage).await;
    info!(run_id, "Plan complete");

    // --- Check: already done? ---
    if plan_result.tasks.starts_with("NO_CHANGES_NEEDED:") {
        let reason = plan_result
            .tasks
            .strip_prefix("NO_CHANGES_NEEDED:")
            .unwrap_or("")
            .trim();
        info!(run_id, reason, "Plan determined no changes needed");

        let raw_github = format!(
            "✅ **No changes needed**\n\n{reason}\n\nThe codebase already satisfies what this issue asks for."
        );
        let github_comment = formatter::format_with_voice(state, &voice, &raw_github, usage).await;
        let raw_jira =
            format!("{reason}\n\nThe codebase already satisfies what this issue asks for.");
        let jira_comment = formatter::format_with_voice(state, &voice, &raw_jira, usage).await;

        if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &github_comment,
                )
                .await;
        } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
            let _ = post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &jira_comment,
                "no_changes",
                "No changes needed",
            )
            .await;
        }

        // Complete the run with no PR
        let duration = start.elapsed().as_secs();
        let cost = usage.estimated_cost();
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(run_id))
            .update_expression(
                "SET #status = :s, tokens_in = :ti, tokens_out = :to, cost_usd = :c, \
                 duration_s = :d, updated_at = :t, current_pass = :cp, \
                 status_run_id = :sri, mcp_servers = :mcp",
            )
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":s", attr_s("completed"))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":d", attr_n(duration))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":cp", attr_s("done"))
            .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
            .expression_attribute_values(
                ":mcp",
                AttributeValue::L(mcp_server_ids.iter().map(|s| attr_s(s)).collect()),
            )
            .send()
            .await;

        // Update analytics counters
        let analytics_month = chrono::Utc::now().format("%Y-%m").to_string();
        for period in &[analytics_month.as_str(), "ALL_TIME"] {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.analytics_table_name)
                .key("team_id", attr_s(&msg.team_id))
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
                .await;
        }

        // Report token usage for billing
        let total_tokens = usage.input_tokens + usage.output_tokens;
        crate::clients::billing::report_token_overage(state, &msg.team_id, total_tokens).await;
        return Ok(());
    }

    // --- Check: clarification needed? ---
    if plan_result.tasks.starts_with("CLARIFICATION_NEEDED:") {
        let detail = plan_result
            .tasks
            .strip_prefix("CLARIFICATION_NEEDED:")
            .unwrap_or("")
            .trim();
        info!(
            run_id,
            detail, "Plan needs clarification from the ticket author"
        );

        let raw_github = format!(
            "❓ **Clarification needed**\n\n{detail}\n\nPlease update the issue with the missing details and re-trigger the run."
        );
        let github_comment = formatter::format_with_voice(state, &voice, &raw_github, usage).await;
        let raw_jira = format!(
            "{detail}\n\nPlease update the issue with the missing details and comment to re-trigger."
        );
        let jira_comment = formatter::format_with_voice(state, &voice, &raw_jira, usage).await;

        if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &github_comment,
                )
                .await;
        } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
            let _ = post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &jira_comment,
                "clarification",
                "Clarification needed",
            )
            .await;
        }

        // Mark the run as needs_input so a comment can re-trigger it
        let duration = start.elapsed().as_secs();
        let cost = usage.estimated_cost();
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(run_id))
            .update_expression(
                "SET #status = :s, tokens_in = :ti, tokens_out = :to, cost_usd = :c, \
                 duration_s = :d, updated_at = :t, current_pass = :cp, \
                 status_run_id = :sri, mcp_servers = :mcp",
            )
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":s", attr_s("needs_input"))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":d", attr_n(duration))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":cp", attr_s("done"))
            .expression_attribute_values(":sri", attr_s(&format!("needs_input#{run_id}")))
            .expression_attribute_values(
                ":mcp",
                AttributeValue::L(mcp_server_ids.iter().map(|s| attr_s(s)).collect()),
            )
            .send()
            .await;

        // Update analytics counters
        let analytics_month = chrono::Utc::now().format("%Y-%m").to_string();
        for period in &[analytics_month.as_str(), "ALL_TIME"] {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.analytics_table_name)
                .key("team_id", attr_s(&msg.team_id))
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
                .await;
        }

        // Report token usage for billing
        let total_tokens = usage.input_tokens + usage.output_tokens;
        crate::clients::billing::report_token_overage(state, &msg.team_id, total_tokens).await;
        return Ok(());
    }

    // --- Pass 3: Implement ---
    let branch_name = if can_skip_implement {
        checkpoint.as_ref().unwrap().1.clone()
    } else {
        format!("coderhelm/{}", msg.ticket_id.to_lowercase())
    };

    let impl_result = if can_skip_implement {
        // Resume: reconstruct impl_result from the existing branch diff
        info!(run_id, branch = %branch_name, "Checkpoint resume: skipping implement pass");
        update_pass(state, &msg.team_id, run_id, "implement").await?;

        let diff_json = github
            .get_diff(&msg.repo_owner, &msg.repo_name, "main", &branch_name)
            .await?;
        let files_modified: Vec<String> = diff_json["files"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        info!(
            run_id,
            files = files_modified.len(),
            "Reconstructed files_modified from branch diff"
        );
        implement::ImplementResult { files_modified }
    } else {
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "implement").await?;

        // Create working branch first
        github
            .create_branch(&msg.repo_owner, &msg.repo_name, &branch_name, "main")
            .await?;
        info!(run_id, branch = %branch_name, "Created working branch");

        // Commit openspec to the repo branch if enabled (default: on)
        let commit_openspec = load_workflow_setting(state, &msg.team_id, "commit_openspec").await;
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

        // --- Plan Validation (deterministic, no LLM) ---
        let plan_warnings = validate_plan(
            &plan_result,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            state,
            &msg.team_id,
        )
        .await;
        if plan_warnings.blocked {
            let warning_text = plan_warnings.warnings.join("\n");
            let comment = format!(
                "⚠️ **Plan validation failed**\n\n{warning_text}\n\nPlease narrow the scope and re-trigger."
            );
            if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
                let _ = github
                    .create_issue_comment(
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.issue_number,
                        &comment,
                    )
                    .await;
            } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
                let _ = post_jira_comment(
                    state,
                    &msg.team_id,
                    &msg.ticket_id,
                    &warning_text,
                    "scope_too_large",
                    "Plan scope too large",
                )
                .await;
            }
            return Err(format!("Plan validation blocked: {warning_text}").into());
        }
        if !plan_warnings.warnings.is_empty() {
            for w in &plan_warnings.warnings {
                warn!(run_id, warning = %w, "Plan validation warning");
            }
        }

        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let result = implement::run(
            state,
            msg,
            &github,
            &plan_result,
            &branch_name,
            &rules,
            &repo_instructions,
            None,
            usage,
        )
        .await?;
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            "implement",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        save_checkpoint(
            state,
            &msg.team_id,
            run_id,
            "implement",
            &branch_name,
            0,
            usage,
        )
        .await;
        info!(
            run_id,
            files = result.files_modified.len(),
            "Implement complete"
        );

        // If no files were changed, comment on the issue and bail — don't open an empty PR
        if result.files_modified.is_empty() {
            warn!(run_id, "Implement pass produced zero file changes");
            let raw_clarification = "I explored the codebase but couldn't determine what changes to make for this issue.\n\n\
                 This usually means the issue needs more detail — for example:\n\
                 - Which file(s) or component(s) should be modified?\n\
                 - What is the expected behavior vs. current behavior?\n\
                 - Any relevant code snippets or error messages?\n\n\
                 Please add more context and I'll try again.";
            let clarification =
                formatter::format_with_voice(state, &voice, raw_clarification, usage).await;
            if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
                if let Err(e) = github
                    .create_issue_comment(
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.issue_number,
                        &clarification,
                    )
                    .await
                {
                    warn!(run_id, error = %e, "Failed to comment on issue about empty implementation");
                }
                return Err("Could not determine what changes to make — commented on issue asking for clarification".into());
            } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
                if let Err(e) = post_jira_comment(
                    state,
                    &msg.team_id,
                    &msg.ticket_id,
                    &clarification,
                    "clarification",
                    "More detail needed",
                )
                .await
                {
                    warn!(run_id, error = %e, "Failed to comment on Jira ticket about empty implementation");
                }
                return Err("Could not determine what changes to make — commented on ticket asking for clarification".into());
            }
            return Err("Could not determine what changes to make — please add more detail to the ticket (which files to change, expected behavior, relevant code snippets)".into());
        }

        // Mark all tasks as done in S3 openspec so retries/dashboard see progress
        let tasks_key = format!(
            "teams/{}/runs/{}/openspec/tasks.md",
            msg.team_id,
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

        result
    };

    // --- Pass 4: Test + Review loop (max 3 cycles) ---
    let review_loop_enabled = load_workflow_setting(state, &msg.team_id, "review_loop").await;
    let max_review_cycles: usize = if review_loop_enabled { 3 } else { 1 };

    for cycle in 1..=max_review_cycles {
        // Test gate: wait for CI if configured
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "test").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let test_result = match test::run(state, msg, &github, &branch_name).await {
            Ok(r) => r,
            Err(e) => {
                warn!(run_id, error = %e, "Test pass errored, proceeding without test results");
                test::TestResult { passed: true, output: Some(format!("Test pass error: {e}")) }
            }
        };
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            &format!("test:{cycle}"),
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        if !test_result.passed {
            info!(run_id, cycle, "CI failed, feeding back to implement");
            if cycle < max_review_cycles {
                check_cancelled(state, &msg.team_id, run_id).await?;
                update_pass(state, &msg.team_id, run_id, "implement").await?;
                let test_feedback = test_result.output.unwrap_or_default();
                implement::run(
                    state,
                    msg,
                    &github,
                    &plan_result,
                    &branch_name,
                    &rules,
                    &repo_instructions,
                    Some(&format!(
                        "CI tests failed. Fix the failures:\n\n{test_feedback}"
                    )),
                    usage,
                )
                .await?;
                continue; // Re-test on next cycle
            }
            warn!(
                run_id,
                "CI still failing after {max_review_cycles} cycles, proceeding"
            );
        }

        // Review
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "review").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let review_result = match review::run(
            state,
            msg,
            &github,
            &branch_name,
            &rules,
            &repo_instructions,
            usage,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(run_id, error = %e, "Review pass errored, proceeding");
                review::ReviewResult { passed: true, summary: format!("Review error: {e}") }
            }
        };
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            &format!("review:{cycle}"),
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        save_checkpoint(
            state,
            &msg.team_id,
            run_id,
            "review",
            &branch_name,
            cycle as u8,
            usage,
        )
        .await;
        info!(
            run_id,
            cycle,
            passed = review_result.passed,
            "Review cycle complete"
        );

        if review_result.passed || cycle == max_review_cycles {
            break;
        }

        // Feed review issues back into implement
        info!(run_id, cycle, "Re-implementing based on review feedback");
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "implement").await?;
        implement::run(
            state,
            msg,
            &github,
            &plan_result,
            &branch_name,
            &rules,
            &repo_instructions,
            Some(&review_result.summary),
            usage,
        )
        .await?;
    }

    // --- Security Audit (after review loop, before PR) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "security").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let security_result = match security::run(state, msg, &github, &branch_name, usage).await {
        Ok(r) => r,
        Err(e) => {
            warn!(run_id, error = %e, "Security pass errored, proceeding");
            security::SecurityResult { passed: true, summary: format!("Security error: {e}") }
        }
    };
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "security",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    save_checkpoint(
        state,
        &msg.team_id,
        run_id,
        "security",
        &branch_name,
        0,
        usage,
    )
    .await;
    info!(
        run_id,
        passed = security_result.passed,
        "Security audit complete"
    );

    if !security_result.passed {
        // One remediation cycle: implement fixes, then re-audit
        info!(run_id, "Security issues found, running remediation cycle");
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "implement").await?;
        implement::run(
            state,
            msg,
            &github,
            &plan_result,
            &branch_name,
            &rules,
            &repo_instructions,
            Some(&format!(
                "Security audit found vulnerabilities. Fix ALL of the following:\n\n{}",
                security_result.summary
            )),
            usage,
        )
        .await?;

        // Re-audit once
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "security").await?;
        let retry = match security::run(state, msg, &github, &branch_name, usage).await {
            Ok(r) => r,
            Err(e) => {
                warn!(run_id, error = %e, "Security retry errored, proceeding");
                security::SecurityResult { passed: true, summary: format!("Security error: {e}") }
            }
        };
        if !retry.passed {
            warn!(
                run_id,
                "Security issues remain after remediation, proceeding with PR"
            );
        }
    }

    // --- Resolve conflicts with main ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    match pr::resolve_conflicts(state, msg, &github, &branch_name, usage).await {
        Ok(true) => info!(run_id, "Resolved merge conflicts with main"),
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Conflict resolution failed, proceeding with PR"),
    }

    // --- Pass 6: Create PR ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "pr").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
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
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "pr",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    info!(run_id, pr_url = %pr_result.pr_url, "PR created");

    // Update run record with final state
    let duration = start.elapsed().as_secs();
    complete_run(
        state,
        msg,
        run_id,
        &pr_result,
        &impl_result,
        &mcp_server_ids,
        usage,
        duration,
    )
    .await?;

    // Post success comment only for GitHub-sourced tickets.
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let raw_comment = format!(
            "✅ **Coderhelm completed this ticket**\n\n**PR**: {}\n**Files**: {} modified\n\n[View run →](https://app.coderhelm.com/runs/detail?id={})",
            pr_result.pr_url,
            impl_result.files_modified.len(),
            run_id,
        );
        let comment = formatter::format_with_voice(state, &voice, &raw_comment, usage).await;
        github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.issue_number, &comment)
            .await?;
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        let raw_comment = format!(
            "PR: {}\nFiles: {} modified\n\nView run → https://app.coderhelm.com/runs/detail?id={}",
            pr_result.pr_url,
            impl_result.files_modified.len(),
            run_id,
        );
        let comment = formatter::format_with_voice(state, &voice, &raw_comment, usage).await;
        let _ = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &comment,
            "success",
            "Completed",
        )
        .await;
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
        .item("team_id", attr_s(&msg.team_id))
        .item("run_id", attr_s(run_id))
        .item("status", attr_s("running"))
        // Composite SK for status-index GSI: "running#<run_id>" for efficient status queries
        .item("status_run_id", attr_s(&format!("running#{run_id}")))
        // Composite key for repo-index GSI
        .item("team_repo", attr_s(&format!("{}#{}", msg.team_id, repo)))
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
            .key("team_id", attr_s(&msg.team_id))
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
    team_id: &str,
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
        .key("team_id", attr_s(team_id))
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

/// Check if a run has been cancelled by the user via the dashboard.
async fn check_cancelled(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await?;

    if let Some(item) = result.item() {
        if let Some(status) = item.get("status").and_then(|v| v.as_s().ok()) {
            if status == "cancelled" {
                return Err("Run cancelled by user".into());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn complete_run(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr: &pr::PrResult,
    impl_result: &implement::ImplementResult,
    mcp_server_ids: &[String],
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
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, files_modified = :fm, \
             repo = :repo, team_repo = :tr, mcp_servers = :mcp",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("completed"))
        .expression_attribute_values(":pr", attr_s(&pr.pr_url))
        .expression_attribute_values(":pn", attr_n(pr.pr_number))
        .expression_attribute_values(
            ":repo",
            attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)),
        )
        .expression_attribute_values(
            ":tr",
            attr_s(&format!(
                "{}#{}/{}",
                msg.team_id, msg.repo_owner, msg.repo_name
            )),
        )
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
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_server_ids.iter().map(|s| attr_s(s)).collect()),
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
            .key("team_id", attr_s(&msg.team_id))
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

    // Increment team's monthly run count in main table
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s("META"))
        .update_expression("ADD run_count_mtd :one")
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await?;

    // Report token overage to Stripe (after analytics are updated)
    let total_tokens = usage.input_tokens + usage.output_tokens;
    crate::clients::billing::report_token_overage(state, &msg.team_id, total_tokens).await;

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
        &msg.team_id,
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
        .key("team_id", attr_s(&msg.team_id))
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

    // Update analytics counters (include tokens so billing stays accurate)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression(
                "ADD failed :one, total_cost_usd :cost, \
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
            .await;
    }

    // Send run-failed notification
    if let Err(e) = email::send_notification(
        state,
        &msg.team_id,
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

    // Comment on the GitHub issue so the user knows the run failed
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        if let Ok(gh) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            let comment = format!(
                "⚠️ **Coderhelm couldn't complete this issue**\n\n\
                 {}\n\n\
                 If you can, add more detail to the issue description — for example which files to change, \
                 expected behavior, or relevant code snippets — and I'll try again.\n\n\
                 [View run →](https://app.coderhelm.com/runs/detail?id={})",
                &error_msg[..error_msg.len().min(300)],
                run_id,
            );
            if let Err(e) = gh
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.issue_number, &comment)
                .await
            {
                error!("Failed to comment on issue about run failure: {e}");
            }
        }
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        let comment = format!(
            "{}\n\n\
             Add more detail — which files to change, expected behavior, or relevant code snippets — and comment to retry.\n\n\
             View run → https://app.coderhelm.com/runs/detail?id={}",
            &error_msg[..error_msg.len().min(300)],
            run_id,
        );
        if let Err(e) = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &comment,
            "error",
            "Run failed",
        )
        .await
        {
            error!("Failed to comment on Jira ticket about run failure: {e}");
        }
    }
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Look up the GitHub App installation_id from the team META record.
async fn lookup_team_installation_id(
    state: &WorkerState,
    team_id: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .projection_expression("github_install_id")
        .send()
        .await?;

    result
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&id| id > 0)
        .ok_or_else(|| format!("No github_install_id found for team {team_id}").into())
}

/// Post a comment on a Jira ticket via the Forge web trigger.
async fn post_jira_comment(
    state: &WorkerState,
    team_id: &str,
    issue_key: &str,
    comment: &str,
    comment_type: &str,
    title: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("JIRA#config".to_string()),
        )
        .send()
        .await?
        .item
        .and_then(|item| {
            item.get("add_comment_url")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
        .ok_or("No Jira add_comment_url configured")?;

    let payload = serde_json::json!({
        "issueKey": issue_key,
        "comment": comment,
        "commentType": comment_type,
        "title": title,
    });

    let resp = state
        .http
        .post(&url)
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Jira comment failed ({status}): {body}").into());
    }

    Ok(())
}

/// Check if there is already an active (running) run for this ticket.
/// Prevents duplicate processing from SQS re-deliveries after Lambda timeouts.
async fn is_ticket_already_running(state: &WorkerState, msg: &TicketMessage) -> bool {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("status-index")
        .key_condition_expression("team_id = :tid AND begins_with(status_run_id, :prefix)")
        .filter_expression("ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(&msg.team_id))
        .expression_attribute_values(":prefix", attr_s("running#"))
        .expression_attribute_values(":ticket", attr_s(&msg.ticket_id))
        .limit(1)
        .send()
        .await;

    match result {
        Ok(out) => !out.items().is_empty(),
        Err(e) => {
            warn!("Dedup check failed, proceeding: {e}");
            false
        }
    }
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
    if let Some(global) = load_rule_list(state, &msg.team_id, "RULES#GLOBAL").await {
        rules.extend(global);
    }

    // Load repo-specific rules
    let repo_sk = format!("RULES#REPO#{}/{}", msg.repo_owner, msg.repo_name);
    if let Some(repo_rules) = load_rule_list(state, &msg.team_id, &repo_sk).await {
        rules.extend(repo_rules);
    }

    info!(count = rules.len(), "Loaded must-rules");
    rules
}

async fn load_rule_list(state: &WorkerState, team_id: &str, sk: &str) -> Option<Vec<String>> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

/// Format repo-level instruction files as a system prompt block.
pub fn format_instructions_block(instructions: &str) -> String {
    if instructions.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Repository Instructions (from the repo's instruction files — follow these conventions)\n\n{}",
        instructions
    )
}

/// Well-known instruction files that coding agents/IDEs use.
const INSTRUCTION_FILES: &[&str] = &[
    "AGENTS.md",
    ".github/AGENTS.md",
    "CLAUDE.md",
    ".claude/CLAUDE.md",
    "COPILOT.md",
    ".github/copilot-instructions.md",
    "copilot-instructions.md",
    ".cursorrules",
    ".github/instructions.md",
];

/// Load repo-level instruction files (AGENTS.md, CLAUDE.md, copilot-instructions, etc.)
/// from the GitHub repo. Returns combined content, capped at 16KB total.
pub async fn load_repo_instructions(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
) -> String {
    const MAX_TOTAL_BYTES: usize = 16_000;
    let mut combined = String::new();

    for path in INSTRUCTION_FILES {
        if combined.len() >= MAX_TOTAL_BYTES {
            break;
        }
        match github.read_file(owner, repo, path, "HEAD").await {
            Ok(content) if !content.trim().is_empty() => {
                let remaining = MAX_TOTAL_BYTES.saturating_sub(combined.len());
                let truncated = if content.len() > remaining {
                    &content[..content[..remaining].rfind('\n').unwrap_or(remaining)]
                } else {
                    &content
                };
                if !combined.is_empty() {
                    combined.push_str("\n\n---\n\n");
                }
                combined.push_str(&format!("### {path}\n\n{truncated}"));
                info!(
                    path,
                    bytes = truncated.len(),
                    "Loaded repo instruction file"
                );
            }
            _ => {} // File doesn't exist or can't be read — skip silently
        }
    }

    combined
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
pub(crate) async fn load_content(state: &WorkerState, team_id: &str, sk: &str) -> String {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

struct PlanValidation {
    warnings: Vec<String>,
    blocked: bool,
}

/// Deterministic plan validation — catches bad plans before the expensive Implement pass.
async fn validate_plan(
    plan: &plan::PlanResult,
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    state: &WorkerState,
    team_id: &str,
) -> PlanValidation {
    let mut warnings = Vec::new();
    let mut blocked = false;

    // Extract file paths from plan text (design + tasks)
    let plan_text = format!("{}\n{}", plan.design, plan.tasks);
    let file_re =
        regex::Regex::new(r"(?:^|[\s`\(])([a-zA-Z0-9_./\-]+\.\w{1,5})(?:[\s`\)\:]|$)").unwrap();
    let known_extensions = [
        "rs", "ts", "tsx", "js", "jsx", "py", "go", "java", "rb", "html", "css", "scss", "json",
        "yaml", "yml", "toml", "md", "sql", "sh", "tf", "vue", "svelte",
    ];
    let mut mentioned_files: Vec<String> = file_re
        .captures_iter(&plan_text)
        .filter_map(|cap| {
            let path = cap.get(1)?.as_str().to_string();
            let ext = path.rsplit('.').next().unwrap_or("");
            if known_extensions.contains(&ext) && path.contains('/') {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    mentioned_files.sort();
    mentioned_files.dedup();

    // 1. Scope check
    let max_plan_files = load_workflow_setting_num(state, team_id, "max_plan_files")
        .await
        .unwrap_or(15)
        .min(30) as usize;

    if mentioned_files.len() > max_plan_files + 10 {
        warnings.push(format!(
            "This plan modifies {} files — exceeds the hard cap of {}. Please break into smaller tickets.",
            mentioned_files.len(),
            max_plan_files + 10,
        ));
        blocked = true;
    } else if mentioned_files.len() > max_plan_files {
        warnings.push(format!(
            "This plan modifies {} files (limit: {}). Consider breaking into smaller tickets.",
            mentioned_files.len(),
            max_plan_files,
        ));
    }

    // 2. File existence check (sample up to 10 files to avoid API spam)
    if !mentioned_files.is_empty() {
        let sample: Vec<&String> = mentioned_files.iter().take(10).collect();
        let mut missing = Vec::new();
        for path in &sample {
            match github.read_file(repo_owner, repo_name, path, "main").await {
                Ok(_) => {}
                Err(_) => missing.push(path.to_string()),
            }
        }
        let missing_pct = if sample.is_empty() {
            0.0
        } else {
            missing.len() as f64 / sample.len() as f64
        };
        if missing_pct > 0.3 && missing.len() >= 2 {
            warnings.push(format!(
                "Warning: Plan references {} files that don't exist: {}. The plan may be hallucinating.",
                missing.len(),
                missing.join(", "),
            ));
        }
    }

    PlanValidation { warnings, blocked }
}

/// Load a numeric workflow setting from SETTINGS#WORKFLOW.
async fn load_workflow_setting_num(state: &WorkerState, team_id: &str, key: &str) -> Option<u64> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("SETTINGS#WORKFLOW".to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get(key))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok()),
        Err(_) => None,
    }
}

/// Load a boolean workflow setting from SETTINGS#WORKFLOW. Returns default (true) if not found.
async fn load_workflow_setting(state: &WorkerState, team_id: &str, key: &str) -> bool {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

/// Check if team subscription allows processing (active or free).
async fn is_subscription_allowed(state: &WorkerState, team_id: &str) -> bool {
    let status = state
        .dynamo
        .get_item()
        .table_name(&state.config.billing_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
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

    matches!(
        status.as_str(),
        "" | "none" | "active" | "free" | "trialing"
    )
}

/// Write a per-pass trace record to the traces table.
#[allow(clippy::too_many_arguments)]
async fn write_pass_trace(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass_name: &str,
    start_time: std::time::Instant,
    usage_before: &TokenUsage,
    usage_after: &TokenUsage,
    error: Option<&str>,
) {
    if state.config.traces_table_name.is_empty() {
        return;
    }
    let duration_ms = start_time.elapsed().as_millis() as u64;
    let input_tokens = usage_after
        .input_tokens
        .saturating_sub(usage_before.input_tokens);
    let output_tokens = usage_after
        .output_tokens
        .saturating_sub(usage_before.output_tokens);
    let cache_read = usage_after
        .cache_read_tokens
        .saturating_sub(usage_before.cache_read_tokens);
    let cache_write = usage_after
        .cache_write_tokens
        .saturating_sub(usage_before.cache_write_tokens);
    let tool_calls = usage_after
        .tool_calls
        .saturating_sub(usage_before.tool_calls);
    // Collect tool names used in this pass (names in after but not in before)
    let tool_names: Vec<String> = usage_after
        .tool_names
        .iter()
        .filter(|n| !usage_before.tool_names.contains(n))
        .cloned()
        .collect();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("RUN#{run_id}#PASS#{pass_name}");

    let mut req = state
        .dynamo
        .put_item()
        .table_name(&state.config.traces_table_name)
        .item("team_id", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(sk))
        .item("pass", AttributeValue::S(pass_name.to_string()))
        .item("duration_ms", attr_n(duration_ms))
        .item("input_tokens", attr_n(input_tokens))
        .item("output_tokens", attr_n(output_tokens))
        .item("cache_read_tokens", attr_n(cache_read))
        .item("cache_write_tokens", attr_n(cache_write))
        .item("tool_calls", attr_n(tool_calls))
        .item("timestamp", AttributeValue::S(now))
        .item(
            "ttl",
            attr_n(chrono::Utc::now().timestamp() as u64 + 30 * 86400),
        );

    if !tool_names.is_empty() {
        req = req.item(
            "tool_names",
            AttributeValue::L(
                tool_names
                    .iter()
                    .map(|s| AttributeValue::S(s.clone()))
                    .collect(),
            ),
        );
    }

    if let Some(err) = error {
        req = req.item("error", AttributeValue::S(err.to_string()));
    }

    if let Err(e) = req.send().await {
        warn!(error = %e, "Failed to write pass trace for {pass_name}");
    }
}

/// Save a checkpoint after a pass completes so runs can resume on Lambda timeout.
async fn save_checkpoint(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    last_pass: &str,
    branch: &str,
    review_cycle: u8,
    usage: &TokenUsage,
) {
    if state.config.checkpoints_table_name.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp() as u64;
    let ttl = now + 7 * 86400; // 7 day TTL

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.checkpoints_table_name)
        .item("team_id", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(format!("RUN#{run_id}")))
        .item("last_pass", AttributeValue::S(last_pass.to_string()))
        .item("branch", AttributeValue::S(branch.to_string()))
        .item("review_cycle", attr_n(review_cycle as u64))
        .item("tokens_in", attr_n(usage.input_tokens))
        .item("tokens_out", attr_n(usage.output_tokens))
        .item("cache_read", attr_n(usage.cache_read_tokens))
        .item("cache_write", attr_n(usage.cache_write_tokens))
        .item("updated_at", attr_n(now))
        .item("ttl", attr_n(ttl))
        .send()
        .await
    {
        warn!(error = %e, "Failed to save checkpoint after {last_pass}");
    }
}

/// Load an existing checkpoint for a run (if any).
async fn load_checkpoint(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Option<(String, String, u8)> {
    if state.config.checkpoints_table_name.is_empty() {
        return None;
    }
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(format!("RUN#{run_id}")))
        .send()
        .await
        .ok()?;
    let item = result.item()?;
    let last_pass = item.get("last_pass")?.as_s().ok()?.clone();
    let branch = item.get("branch")?.as_s().ok()?.clone();
    let review_cycle = item
        .get("review_cycle")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u8>().ok())
        .unwrap_or(0);
    Some((last_pass, branch, review_cycle))
}
