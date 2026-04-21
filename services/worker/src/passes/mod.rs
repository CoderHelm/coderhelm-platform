use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Maximum Lambda execution time (15 min). We use this to skip expensive
/// remediation cycles when we're running low on time.
const LAMBDA_TIMEOUT_SECS: u64 = 900;
/// Minimum seconds remaining before we'll start a new implement cycle.
const MIN_TIME_FOR_REMEDIATION_SECS: u64 = 240; // 4 minutes

use crate::agent::mcp;
use crate::agent::{llm, provider};
use crate::clients::email;
use crate::clients::github::GitHubClient;
use crate::memory::AgentMemory;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

/// Download an image from S3 and return as base64 string.
pub(crate) async fn download_image_as_base64(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Option<String> {
    use base64::Engine;
    match s3.get_object().bucket(bucket).key(key).send().await {
        Ok(resp) => match resp.body.collect().await {
            Ok(bytes) => {
                let data = bytes.into_bytes();
                Some(base64::engine::general_purpose::STANDARD.encode(&data))
            }
            Err(e) => {
                warn!(key, error = %e, "Failed to read S3 image body");
                None
            }
        },
        Err(e) => {
            warn!(key, error = %e, "Failed to download image from S3");
            None
        }
    }
}

/// In-memory file read cache shared across passes within a single run.
/// Avoids re-fetching the same files from GitHub when review/security
/// re-reads files that implement already loaded.
#[derive(Clone, Default)]
pub struct FileCache {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

/// Result of a per-repo implement→security→PR cycle (multi-repo support).
struct RepoPrResult {
    branch: String,
    pr_number: u64,
    pr_url: String,
}

impl FileCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.inner.read().await.get(key).cloned()
    }

    pub async fn insert(&self, key: String, value: String) {
        self.inner.write().await.insert(key, value);
    }

    pub async fn remove(&self, key: &str) {
        self.inner.write().await.remove(key);
    }
}

/// Strip internal model IDs and service names from error messages
/// so they are safe to display to users.
fn sanitize_error(msg: &str) -> String {
    let mut s = msg.to_string();
    // Remove (model=claude-...) patterns
    while let Some(start) = s.find("(model=") {
        if let Some(end) = s[start..].find(')') {
            s.replace_range(start..start + end + 1, "");
        } else {
            break;
        }
    }
    // Remove model IDs
    let patterns = [
        "claude-opus-4-20250514",
        "claude-sonnet-4-20250514",
    ];
    for p in &patterns {
        s = s.replace(p, "");
    }
    s = s
        .replace(
            "Anthropic API error",
            "An error occurred during processing",
        )
;
    // Collapse extra whitespace
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Returns seconds remaining before the Lambda timeout.
fn remaining_secs(start: &Instant) -> u64 {
    LAMBDA_TIMEOUT_SECS.saturating_sub(start.elapsed().as_secs())
}

/// Whether there's enough time left for an expensive remediation cycle.
fn has_time_for_remediation(start: &Instant) -> bool {
    remaining_secs(start) >= MIN_TIME_FOR_REMEDIATION_SECS
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
#[allow(dead_code)]
mod test;
mod triage;
pub mod resume;

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

    match tokio::time::timeout(
        std::time::Duration::from_secs(LAMBDA_TIMEOUT_SECS - 30), // 30s buffer for cleanup
        run_passes(state, &mut msg, &run_id, &mut usage, &start),
    )
    .await
    {
        Ok(Ok(_)) => {
            let duration = start.elapsed().as_secs();
            info!(run_id, duration, "Orchestration complete");
        }
        Ok(Err(e)) => {
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
                    .update_expression("SET tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, updated_at = :t")
                    .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                    .expression_attribute_values(":to", attr_n(usage.output_tokens))
                    .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                    .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                    .expression_attribute_values(":c", attr_n(format!("{cost:.4}")))
                    .expression_attribute_values(":d", attr_n(duration))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
                // Also update analytics for cancelled runs
                let month = chrono::Utc::now().format("%Y-%m").to_string();
                for period in &[month.as_str(), "ALL_TIME"] {
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.analytics_table_name)
                        .key("team_id", attr_s(&msg.team_id))
                        .key("period", attr_s(period))
                        .update_expression("ADD total_tokens_in :ti, total_tokens_out :to, cache_read_tokens :crt, cache_write_tokens :cwt")
                        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                        .expression_attribute_values(":to", attr_n(usage.output_tokens))
                        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                        .send()
                        .await;
                }
            } else {
                error!(run_id, error = %e, "Orchestration failed");
                let sanitized = sanitize_error(&err_msg);
                fail_run(state, &msg, &run_id, &sanitized, &usage, duration).await;
            }
        }
        Err(_elapsed) => {
            let duration = start.elapsed().as_secs();
            warn!(run_id, duration, "Orchestration timed out — Lambda limit approaching");
            fail_run(
                state,
                &msg,
                &run_id,
                "Run timed out (exceeded execution time limit). The issue may need a narrower scope.",
                &usage,
                duration,
            )
            .await;
        }
    }

    // Release or extend the ticket lock based on final run status.
    // If the run went to awaiting_ci, extend the lock (CI may take a while).
    // Otherwise, release it so new runs can proceed.
    let is_awaiting_ci = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
    {
        Ok(out) => out
            .item()
            .and_then(|i| i.get("status").and_then(|v| v.as_s().ok().cloned()))
            .map(|s| s == "awaiting_ci")
            .unwrap_or(false),
        Err(_) => false,
    };

    if is_awaiting_ci {
        info!(run_id, ticket_id = %msg.ticket_id, "Extending ticket lock for CI wait");
        extend_ticket_lock(state, &msg.team_id, &msg.ticket_id, 30).await;
    } else {
        release_ticket_lock(state, &msg.team_id, &msg.ticket_id).await;
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

    // Load team's Anthropic API key (required)
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;

    // File read cache shared across passes to avoid redundant GitHub fetches
    let file_cache = FileCache::new();

    // Pre-run budget gate: reject if monthly token limit exceeded
    if let Some(reason) = check_budget_exceeded(state, &msg.team_id).await {
        warn!(run_id, team_id = %msg.team_id, %reason, "Run rejected: budget exceeded");
        let duration = start.elapsed().as_secs();
        fail_run(state, msg, run_id, &reason, usage, duration).await;
        return Err(reason.into());
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
    let mut progress_comment_id: Option<u64> = None;
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let resp = github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format_progress("triage", run_id),
            )
            .await?;
        progress_comment_id = resp["id"].as_u64();
    }

    // --- Resolve repo for Jira tickets ---
    // For Jira: project mapping > LLM select_repo > single repo fallback.
    // validate_plan is the final safety net (tech-stack + plan-text checks).
    if matches!(msg.source, TicketSource::Jira) {
        let repos = plan::fetch_team_repos(state, &msg.team_id).await;
        if repos.is_empty() {
            return Err("No enabled repos found for team — cannot auto-pick repo".into());
        }

        // Check for explicit Jira project → repo mapping
        let project_key = msg.ticket_id.split('-').next().unwrap_or("");
        let mapped_repo = if !project_key.is_empty() {
            lookup_jira_project_repo(state, &msg.team_id, project_key).await
        } else {
            None
        };

        if let Some(ref repo) = mapped_repo {
            if repos.iter().any(|r| r == repo) {
                let (owner, name) = repo.split_once('/').unwrap_or(("", ""));
                msg.repo_owner = owner.to_string();
                msg.repo_name = name.to_string();
                info!(run_id, repo = %repo, project_key, "Repo from Jira project mapping");
            }
        }

        // If no mapping (or mapped repo not enabled), use LLM to pick the best repo
        if msg.repo_owner.is_empty() || !repos.iter().any(|r| r == &format!("{}/{}", msg.repo_owner, msg.repo_name)) {
            if repos.len() == 1 {
                // Only one repo — no need for LLM
                let (owner, name) = repos[0].split_once('/').unwrap_or(("", ""));
                msg.repo_owner = owner.to_string();
                msg.repo_name = name.to_string();
                info!(run_id, repo = %repos[0], "Single repo available");
            } else {
                // Multiple repos — ask LLM to pick based on ticket content
                match triage::select_repo(state, &msg, &repos, &github, &provider, usage).await {
                    Ok(selected) => {
                        let (owner, name) = selected.split_once('/').unwrap_or(("", ""));
                        msg.repo_owner = owner.to_string();
                        msg.repo_name = name.to_string();
                        info!(run_id, repo = %selected, "LLM selected repo for Jira ticket");
                    }
                    Err(e) => {
                        // Fall back to first repo if LLM fails
                        let (owner, name) = repos[0].split_once('/').unwrap_or(("", ""));
                        msg.repo_owner = owner.to_string();
                        msg.repo_name = name.to_string();
                        warn!(run_id, error = %e, repo = %repos[0], "LLM repo selection failed, using first repo");
                    }
                }
            }
        }
    } else if msg.repo_owner.is_empty() || msg.repo_name.is_empty() {
        return Err("repo_owner and repo_name are required for GitHub tickets".into());
    }

    // Look up the repo's default branch (main, master, develop, etc.)
    let default_branch = match github
        .get_default_branch(&msg.repo_owner, &msg.repo_name)
        .await
    {
        Ok(branch) => {
            info!(run_id, branch = %branch, "Resolved default branch");
            branch
        }
        Err(e) => {
            // Check the repos table for a cached default_branch before giving up
            let cached = state
                .dynamo
                .get_item()
                .table_name(&state.config.repos_table_name)
                .key("pk", attr_s(&msg.team_id))
                .key("sk", attr_s(&format!("REPO#{}/{}", msg.repo_owner, msg.repo_name)))
                .projection_expression("default_branch")
                .send()
                .await
                .ok()
                .and_then(|r| r.item)
                .and_then(|item| item.get("default_branch").and_then(|v| v.as_s().ok()).cloned())
                .filter(|s| !s.is_empty());
            match cached {
                Some(branch) => {
                    warn!(run_id, error = %e, branch = %branch, "GitHub API failed, using cached default_branch");
                    branch
                }
                None => {
                    return Err(format!(
                        "Cannot resolve default branch for {}/{}: {e}. Check GitHub installation permissions.",
                        msg.repo_owner, msg.repo_name
                    ).into());
                }
            }
        }
    };
    msg.base_branch = default_branch;

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
                "implement" | "security" | "pr" | "test" | "review"
            )
    });

    if let Some((ref last_pass, ref branch, ref cycle)) = checkpoint {
        info!(run_id, last_pass, branch, cycle, "Found checkpoint for run");
    }

    // --- Open agent memory (non-blocking: falls back to stateless if unavailable) ---
    let mut agent_memory =
        AgentMemory::open(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let memory_context = match agent_memory.as_mut() {
        Some(mem) => {
            mem.recall_context(&format!("{} {} {}", msg.title, msg.body, msg.repo_name), 5)
                .await
        }
        None => String::new(),
    };

    // --- Pass 1: Triage ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "triage").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let triage_result = triage::run(state, msg, &github, &provider, usage).await?;
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
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Triaged as {} complexity", triage_result.complexity),
    )
    .await;
    info!(run_id, "Triage complete");
    update_progress_comment(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        progress_comment_id,
        "plan",
        run_id,
    )
    .await;

    // --- Resolve external references via MCP tools ---
    let mut triage_result = triage_result;
    let mut used_mcp_ids: Vec<String> = Vec::new();
    if !mcp_plugins.is_empty() && !state.config.mcp_proxy_function_name.is_empty() {
        let external_context = resolve::run(state, msg, &mcp_plugins, &provider, usage).await;
        if !external_context.is_empty() {
            triage_result.summary.push_str(&external_context);
            used_mcp_ids = mcp_server_ids.clone();
            info!(run_id, "Resolved external references via MCP");
        }
    }

    // Inject agent memory context into triage summary
    if !memory_context.is_empty() {
        triage_result.summary.push_str("\n\n");
        triage_result.summary.push_str(&memory_context);
        info!(run_id, "Injected agent memory context");
    }

    // --- Pass 2: Plan ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "plan").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();

    // Compute context hash to detect ticket changes (body, images)
    let context_hash = plan::compute_ticket_context_hash(msg);

    // Try to reuse existing plan from S3 (same ticket, no changes)
    let existing_plan = load_existing_plan(state, &msg.team_id, &msg.ticket_id, &context_hash).await;
    let mut context_changed = existing_plan.is_none();
    let plan_result = if let Some(plan) = existing_plan {
        // Reuse if plan has content AND has unchecked tasks (not a completed plan from a previous run)
        let has_unchecked = plan.tasks.contains("- [ ]");
        if !plan.tasks.is_empty() && !plan.design.is_empty() && has_unchecked {
            context_changed = false;
            info!(run_id, "Reusing existing plan from S3 — ticket unchanged");
            add_progress_note(state, &msg.team_id, run_id, "Reused existing plan").await;
            plan
        } else {
            context_changed = true;
            plan::run(
                state, msg, &github, &triage_result, &repo_instructions, &provider, usage,
            )
            .await?
        }
    } else {
        plan::run(
            state, msg, &github, &triage_result, &repo_instructions, &provider, usage,
        )
        .await?
    };
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
    add_progress_note(state, &msg.team_id, run_id, "Plan generated").await;
    info!(run_id, "Plan complete");
    update_progress_comment(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        progress_comment_id,
        "implement",
        run_id,
    )
    .await;

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
        let github_comment =
            formatter::format_with_voice(state, &voice, &raw_github, &provider, usage).await;
        let raw_jira =
            format!("{reason}\n\nThe codebase already satisfies what this issue asks for.");
        let jira_comment =
            formatter::format_with_voice(state, &voice, &raw_jira, &provider, usage).await;

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
                "SET #status = :s, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
                 duration_s = :d, updated_at = :t, current_pass = :cp, \
                 status_run_id = :sri, mcp_servers = :mcp",
            )
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":s", attr_s("completed"))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
            .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":d", attr_n(duration))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":cp", attr_s("done"))
            .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
            .expression_attribute_values(
                ":mcp",
                AttributeValue::L(used_mcp_ids.iter().map(|s| attr_s(s)).collect()),
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
        let github_comment =
            formatter::format_with_voice(state, &voice, &raw_github, &provider, usage).await;
        let raw_jira = format!(
            "{detail}\n\nPlease update the issue with the missing details and comment to re-trigger."
        );
        let jira_comment =
            formatter::format_with_voice(state, &voice, &raw_jira, &provider, usage).await;

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
                "SET #status = :s, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
                 duration_s = :d, updated_at = :t, current_pass = :cp, \
                 status_run_id = :sri, mcp_servers = :mcp",
            )
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":s", attr_s("needs_input"))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
            .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":d", attr_n(duration))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":cp", attr_s("done"))
            .expression_attribute_values(":sri", attr_s(&format!("needs_input#{run_id}")))
            .expression_attribute_values(
                ":mcp",
                AttributeValue::L(used_mcp_ids.iter().map(|s| attr_s(s)).collect()),
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
            .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, &branch_name)
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
        implement::ImplementResult { files_modified, conversation_log: vec![] }
    } else {
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "implement").await?;

        // --- Re-run detection: check if branch already has an open PR ---
        // If an open PR exists, we always try to fix it rather than starting fresh.
        // Previous retries may have force-reset the branch to base, so we also check
        // for prior runs of the same ticket that were awaiting_ci (evidence of CI failures).
        let existing_pr = match github
            .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, &branch_name)
            .await
        {
            Ok(pr) => {
                info!(run_id, branch = %branch_name, found = pr.is_some(), "Re-run detection: checked for open PR");
                pr
            }
            Err(e) => {
                warn!(run_id, branch = %branch_name, error = %e, "Re-run detection: failed to check for open PR");
                None
            }
        };

        if let Some(ref pr) = existing_pr {
            let pr_number = pr["number"].as_u64().unwrap_or(0);
            let pr_url = pr["html_url"].as_str().unwrap_or("").to_string();
            let pr_head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
            let base_sha = pr["base"]["sha"].as_str().unwrap_or("").to_string();
            let check_ref = if pr_head_sha.is_empty() { branch_name.clone() } else { pr_head_sha.clone() };

            // Detect if the branch was force-reset to base (no diff from base = previous retry wiped it)
            let branch_is_reset = !pr_head_sha.is_empty() && !base_sha.is_empty() && pr_head_sha == base_sha;

            // Check CI status on the PR's head commit
            let checks = github
                .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &check_ref)
                .await
                .unwrap_or_default();
            let failed_conclusions = ["failure", "action_required", "timed_out", "startup_failure"];
            let has_ci_failure = checks["check_runs"]
                .as_array()
                .map(|runs| runs.iter().any(|r| {
                    r["conclusion"].as_str().map_or(false, |c| failed_conclusions.contains(&c))
                }))
                .unwrap_or(false);

            // Check for pending review comments on the PR
            let review_comments = github
                .get_review_comments(&msg.repo_owner, &msg.repo_name, pr_number)
                .await
                .unwrap_or_default();
            let pending_reviews: Vec<&serde_json::Value> = review_comments
                .iter()
                .filter(|c| {
                    let author = c["user"]["login"].as_str().unwrap_or("");
                    !author.contains("[bot]") && !author.contains("coderhelm")
                })
                .collect();
            let has_review_feedback = !pending_reviews.is_empty();

            info!(
                run_id, pr_number, has_ci_failure, has_review_feedback,
                review_count = pending_reviews.len(),
                check_ref = %check_ref,
                branch_is_reset,
                "Re-run detection: PR status"
            );

            if (has_ci_failure || has_review_feedback) && !context_changed {
                // Build combined feedback from CI failures and review comments
                let mut feedback_parts: Vec<String> = Vec::new();

                if has_ci_failure {
                    add_progress_note(
                        state,
                        &msg.team_id,
                        run_id,
                        &format!("Existing PR #{pr_number} has CI failures — fixing"),
                    )
                    .await;

                    // Get the failed workflow run logs
                    let failed_run_id = checks["check_runs"]
                        .as_array()
                        .and_then(|runs| {
                            runs.iter()
                                .filter(|r| r["conclusion"].as_str().map_or(false, |c| failed_conclusions.contains(&c)))
                                .filter_map(|r| {
                                    r["details_url"]
                                        .as_str()
                                        .and_then(|u| u.split("/runs/").nth(1))
                                        .and_then(|s| s.split('/').next())
                                        .and_then(|s| s.parse::<u64>().ok())
                                })
                                .last()
                        })
                        .unwrap_or(0);

                    let logs = if failed_run_id > 0 {
                        github
                            .get_workflow_run_logs(&msg.repo_owner, &msg.repo_name, failed_run_id)
                            .await
                            .unwrap_or_else(|_| "(failed to download logs)".to_string())
                    } else {
                        let job_id = checks["check_runs"]
                            .as_array()
                            .and_then(|runs| {
                                runs.iter()
                                    .find(|r| r["conclusion"].as_str().map_or(false, |c| failed_conclusions.contains(&c)))
                                    .and_then(|r| r["id"].as_u64())
                            })
                            .unwrap_or(0);
                        if job_id > 0 {
                            github
                                .get_check_run_logs(&msg.repo_owner, &msg.repo_name, job_id)
                                .await
                                .unwrap_or_else(|_| "(failed to download logs)".to_string())
                        } else {
                            "(no failed check runs found)".to_string()
                        }
                    };

                    let max_log = 15_000;
                    let trimmed_logs = if logs.len() > max_log {
                        format!("... (truncated)\n{}", &logs[logs.len() - max_log..])
                    } else {
                        logs
                    };

                    feedback_parts.push(format!(
                        "CI workflow failed on PR #{pr_number} (branch: {branch_name}). Fix the failures.\n\n\
                         Rules:\n\
                         - Only fix what CI is complaining about. Don't refactor or add features.\n\
                         - If the failure is in a test, fix the code (not the test) unless the test itself is wrong.\n\
                         - If tests need updating because the feature changed behavior intentionally, update the tests.\n\
                         - You may create new test files or edit existing ones if needed.\n\n\
                         Full logs:\n{trimmed_logs}"
                    ));
                }

                if has_review_feedback {
                    add_progress_note(
                        state,
                        &msg.team_id,
                        run_id,
                        &format!("Existing PR #{pr_number} has {} review comment(s) — addressing", pending_reviews.len()),
                    )
                    .await;

                    let mut review_text = format!(
                        "PR #{pr_number} has review comments that need to be addressed. \
                         Fix each comment by making the requested changes.\n\n"
                    );
                    for (i, comment) in pending_reviews.iter().enumerate().take(20) {
                        let path = comment["path"].as_str().unwrap_or("(unknown file)");
                        let line = comment["line"].as_u64().unwrap_or(0);
                        let body = comment["body"].as_str().unwrap_or("");
                        let author = comment["user"]["login"].as_str().unwrap_or("reviewer");
                        review_text.push_str(&format!(
                            "Comment {} by @{author} on `{path}`:{line}:\n{body}\n\n",
                            i + 1
                        ));
                    }
                    feedback_parts.push(review_text);
                }

                let feedback = feedback_parts.join("\n---\n\n");

                info!(
                    run_id,
                    pr_number,
                    has_ci_failure,
                    has_review_feedback,
                    "Re-run detected: existing PR needs fixes — entering fix mode"
                );

                let file_cache = FileCache::default();
                let pass_start = std::time::Instant::now();
                let usage_before = usage.clone();

                match implement::run(
                    state, msg, &github, &plan_result, &branch_name, &[], "",
                    Some(&feedback), "medium", &provider, usage, &file_cache, Some(run_id),
                )
                .await
                {
                    Ok(result) => {
                        write_pass_trace(state, &msg.team_id, run_id, "ci_fix", pass_start, &usage_before, usage, None).await;
                        write_conversation_log(state, &msg.team_id, run_id, "ci_fix", &result.conversation_log).await;
                        info!(run_id, files = result.files_modified.len(), "Fix implemented on re-run");

                        // Store PR info on run record and go to awaiting_ci
                        let now = chrono::Utc::now().to_rfc3339();
                        let cost = usage.estimated_cost();
                        let duration = start.elapsed().as_secs();
                        let _ = state.dynamo.update_item()
                            .table_name(&state.config.runs_table_name)
                            .key("team_id", attr_s(&msg.team_id))
                            .key("run_id", attr_s(run_id))
                            .update_expression(
                                "SET #s = :s, status_run_id = :sri, current_pass = :cp, \
                                 pr_number = :pn, pr_url = :pu, branch = :b, \
                                 repo = :repo, team_repo = :tr, \
                                 tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
                                 cost_usd = :cost, duration_s = :d, updated_at = :t"
                            )
                            .expression_attribute_names("#s", "status")
                            .expression_attribute_values(":s", attr_s("awaiting_ci"))
                            .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
                            .expression_attribute_values(":cp", attr_s("awaiting_ci"))
                            .expression_attribute_values(":pn", attr_n(pr_number))
                            .expression_attribute_values(":pu", attr_s(&pr_url))
                            .expression_attribute_values(":b", attr_s(&branch_name))
                            .expression_attribute_values(":repo", attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)))
                            .expression_attribute_values(":tr", attr_s(&format!("{}#{}/{}", msg.team_id, msg.repo_owner, msg.repo_name)))
                            .expression_attribute_values(":t", attr_s(&now))
                            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                            .expression_attribute_values(":to", attr_n(usage.output_tokens))
                            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                            .expression_attribute_values(":cost", attr_n(format!("{:.4}", cost)))
                            .expression_attribute_values(":d", attr_n(duration))
                            .send()
                            .await;

                        save_checkpoint(state, &msg.team_id, run_id, "pr", &branch_name, 1, usage).await;
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(run_id, error = %e, "Fix on re-run failed, falling through to fresh start");
                        // Fall through to reset branch and start fresh
                    }
                }
            }
        }

        // Track whether to skip branch creation (PR exists, just needs fresh implementation)
        let skip_branch_reset = if context_changed && existing_pr.is_some() {
            // Context changed (new description, images, etc.) — force reset branch for clean implementation
            let pr_number = existing_pr.as_ref().and_then(|p| p["number"].as_u64()).unwrap_or(0);
            info!(run_id, pr_number, "Context changed — forcing branch reset for fresh implementation");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Ticket updated — resetting PR #{pr_number} branch for fresh implementation"),
            )
            .await;
            false
        } else {
            existing_pr.as_ref().map_or(false, |pr| {
                let pr_head = pr["head"]["sha"].as_str().unwrap_or("");
                let base = pr["base"]["sha"].as_str().unwrap_or("");
                // Branch was reset to base by a previous retry — don't reset again
                !pr_head.is_empty() && !base.is_empty() && pr_head == base
            })
        };

        if skip_branch_reset {
            let pr_number = existing_pr.as_ref().and_then(|p| p["number"].as_u64()).unwrap_or(0);
            info!(run_id, pr_number, branch = %branch_name, "Skipping branch reset — PR exists, branch already at base");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("PR #{pr_number} exists — re-implementing on existing branch"),
            )
            .await;
        } else {
            // Create working branch (or reset existing one to base)
            github
                .create_branch(&msg.repo_owner, &msg.repo_name, &branch_name, &msg.base_branch)
                .await?;
            info!(run_id, branch = %branch_name, "Created working branch");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Created branch {}", branch_name),
            )
            .await;
        }

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
        let team_repos = plan::fetch_team_repos(state, &msg.team_id).await;
        let plan_warnings = validate_plan(
            &plan_result,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &msg.base_branch,
            state,
            &msg.team_id,
            &team_repos,
            &provider,
            usage,
        )
        .await;

        // If plan files exist in a different repo, switch before implement
        if let Some(ref better_repo) = plan_warnings.switch_repo {
            let (new_owner, new_name) = better_repo.split_once('/').unwrap_or(("", ""));
            if !new_owner.is_empty() && !new_name.is_empty() {
                warn!(
                    run_id,
                    from = %format!("{}/{}", msg.repo_owner, msg.repo_name),
                    to = %better_repo,
                    "Switching repo — plan files found in different repo"
                );
                msg.repo_owner = new_owner.to_string();
                msg.repo_name = new_name.to_string();

                // Re-resolve default branch for new repo
                msg.base_branch = github
                    .get_default_branch(&msg.repo_owner, &msg.repo_name)
                    .await
                    .unwrap_or_else(|_| "main".to_string());

                // Create (or reset) the working branch in the new repo
                github
                    .create_branch(&msg.repo_owner, &msg.repo_name, &branch_name, &msg.base_branch)
                    .await
                    .map_err(|e| {
                        format!("Failed to create branch {} in {}/{} after repo switch: {e}",
                            branch_name, msg.repo_owner, msg.repo_name)
                    })?;

                // Update run record
                let resolved_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
                let _ = state
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
                    .await;

                add_progress_note(
                    state,
                    &msg.team_id,
                    run_id,
                    &format!("Switched to {} (plan files matched)", resolved_repo),
                )
                .await;
            }
        }

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

        // --- Multi-repo path ---
        if !plan_result.repo_tasks.is_empty() {
            info!(
                run_id,
                repo_count = plan_result.repo_tasks.len(),
                "Multi-repo plan detected, running per-repo pipeline"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Multi-repo plan: {} repos", plan_result.repo_tasks.len()),
            )
            .await;

            let mut pr_results: Vec<RepoPrResult> = Vec::new();

            for repo_task in &plan_result.repo_tasks {
                let repo_branch = format!(
                    "coderhelm/{}-{}",
                    msg.ticket_id.to_lowercase(),
                    repo_task.name.to_lowercase()
                );

                // Override msg repo for this iteration
                let mut repo_msg = msg.clone();
                repo_msg.repo_owner = repo_task.owner.clone();
                repo_msg.repo_name = repo_task.name.clone();

                // Resolve base branch for this repo
                repo_msg.base_branch = github
                    .get_default_branch(&repo_task.owner, &repo_task.name)
                    .await
                    .unwrap_or_else(|_| "main".to_string());

                // Load repo-specific instructions
                let repo_instr = load_repo_instructions(
                    &github, &repo_task.owner, &repo_task.name,
                ).await;

                match run_repo_pipeline(
                    state,
                    &repo_msg,
                    &github,
                    &plan_result,
                    &repo_task.tasks,
                    &repo_branch,
                    &repo_msg.base_branch,
                    &rules,
                    &repo_instr,
                    &triage_result.complexity,
                    &voice,
                    &provider,
                    usage,
                    &file_cache,
                    &start,
                    run_id,
                    &mut agent_memory,
                )
                .await
                {
                    Ok(pr) => pr_results.push(pr),
                    Err(e) => {
                        warn!(
                            run_id,
                            repo = %format!("{}/{}", repo_task.owner, repo_task.name),
                            error = %e,
                            "Repo pipeline failed, continuing with other repos"
                        );
                        add_progress_note(
                            state,
                            &msg.team_id,
                            run_id,
                            &format!("Failed for {}/{}: {}", repo_task.owner, repo_task.name, e),
                        )
                        .await;
                    }
                }
            }

            if pr_results.is_empty() {
                return Err("Multi-repo pipeline produced no PRs".into());
            }

            // Save checkpoint + set awaiting_ci with all PRs
            save_checkpoint(state, &msg.team_id, run_id, "pr", &pr_results[0].branch, 0, usage).await;

            let duration = start.elapsed().as_secs();
            let now = chrono::Utc::now().to_rfc3339();
            let cost = usage.estimated_cost();

            // Build PR list attributes
            let pr_urls: Vec<AttributeValue> = pr_results.iter().map(|p| attr_s(&p.pr_url)).collect();
            let pr_numbers: Vec<AttributeValue> = pr_results.iter().map(|p| attr_n(p.pr_number)).collect();
            let branches: Vec<AttributeValue> = pr_results.iter().map(|p| attr_s(&p.branch)).collect();

            // Store first PR as primary for backward compat, plus full lists
            state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression(
                    "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
                     pr_urls = :prs, pr_numbers = :pns, branches = :bs, \
                     repo = :repo, team_repo = :tr, \
                     tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
                     cost_usd = :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
                     status_run_id = :sri, mcp_servers = :mcp",
                )
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("awaiting_ci"))
                .expression_attribute_values(":pr", attr_s(&pr_results[0].pr_url))
                .expression_attribute_values(":pn", attr_n(pr_results[0].pr_number))
                .expression_attribute_values(":b", attr_s(&pr_results[0].branch))
                .expression_attribute_values(":prs", AttributeValue::L(pr_urls))
                .expression_attribute_values(":pns", AttributeValue::L(pr_numbers))
                .expression_attribute_values(":bs", AttributeValue::L(branches))
                .expression_attribute_values(":repo", attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)))
                .expression_attribute_values(":tr", attr_s(&format!("{}#{}/{}", msg.team_id, msg.repo_owner, msg.repo_name)))
                .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                .expression_attribute_values(":to", attr_n(usage.output_tokens))
                .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
                .expression_attribute_values(":d", attr_n(duration))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":cp", attr_s("awaiting_ci"))
                .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
                .expression_attribute_values(
                    ":mcp",
                    AttributeValue::L(used_mcp_ids.iter().map(|id| attr_s(id)).collect()),
                )
                .send()
                .await?;

            info!(
                run_id,
                pr_count = pr_results.len(),
                "Multi-repo run set to awaiting_ci"
            );

            // Schedule a safety-net resume
            if !state.config.ci_fix_queue_url.is_empty() {
                let resume_body = serde_json::json!({
                    "type": "resume",
                    "team_id": msg.team_id,
                    "run_id": run_id,
                    "installation_id": msg.installation_id,
                });
                let _ = state
                    .sqs
                    .send_message()
                    .queue_url(&state.config.ci_fix_queue_url)
                    .message_body(resume_body.to_string())
                    .delay_seconds(120)
                    .send()
                    .await;
            }

            if let Some(mem) = agent_memory {
                if let Err(e) = mem.close_and_upload(state).await {
                    warn!(run_id, error = %e, "Failed to persist agent memory");
                }
            }

            return Ok(());
        }

        // --- Single-repo path (existing) ---

        // Write final repo to run record (after validate_plan may have switched it)
        let final_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(run_id))
            .update_expression("SET repo = :r, team_repo = :tr")
            .expression_attribute_values(":r", attr_s(&final_repo))
            .expression_attribute_values(
                ":tr",
                attr_s(&format!("{}#{}", msg.team_id, final_repo)),
            )
            .send()
            .await;

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
            &triage_result.complexity,
            &provider,
            usage,
            &file_cache,
            Some(run_id),
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

        // Save conversation log to S3
        write_conversation_log(state, &msg.team_id, run_id, "implement", &result.conversation_log).await;

        add_progress_note(
            state,
            &msg.team_id,
            run_id,
            &format!(
                "Implemented changes across {} file(s)",
                result.files_modified.len()
            ),
        )
        .await;

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
                formatter::format_with_voice(state, &voice, raw_clarification, &provider, usage)
                    .await;
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
                return Err(raw_clarification.into());
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
                return Err(raw_clarification.into());
            }
            return Err(raw_clarification.into());
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

    // --- Adversarial Test (multi-repo path) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    if has_time_for_remediation(start) {
        update_pass(state, &msg.team_id, run_id, "test").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let test_issues = adversarial_test(
            state, msg, &github, &plan_result, &branch_name, &provider, usage, &file_cache,
            &impl_result.files_modified, run_id,
        ).await;
        write_pass_trace(state, &msg.team_id, run_id, "test", pass_start, &usage_before, usage, None).await;

        if !test_issues.is_empty() {
            warn!(run_id, "Adversarial test found issues — running auto-fix");
            add_progress_note(
                state, &msg.team_id, run_id, "Test found issues — auto-fixing",
            ).await;

            let fix_plan = plan::PlanResult {
                proposal: String::new(), tasks: String::new(),
                spec: String::new(), design: String::new(), repo_tasks: vec![],
            };
            let fix_cache = FileCache::default();
            if let Err(e) = implement::run(
                state, msg, &github, &fix_plan, &branch_name, &[],
                "", Some(&test_issues), "low", &provider, usage, &fix_cache, Some(run_id),
            ).await {
                warn!(run_id, error = %e, "Test auto-fix failed, proceeding anyway");
            }
        }
    }

    // --- Security Audit (before PR — ensures only security-clean code is published) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "security").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let security_result = match security::run(
        state,
        msg,
        &github,
        &plan_result,
        &branch_name,
        &repo_instructions,
        &provider,
        usage,
        &file_cache,
        Some(run_id),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(run_id, error = %e, "Security pass errored, proceeding");
            security::SecurityResult {
                passed: true,
                summary: format!("Security error: {e}"),
            }
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
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        if security_result.passed {
            "Security audit passed"
        } else {
            "Security issues found, fixing"
        },
    )
    .await;

    if !security_result.passed {
        if !has_time_for_remediation(&start) {
            warn!(
                run_id,
                remaining_secs = remaining_secs(&start),
                "Skipping security remediation — not enough time remaining"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                "Security issues found but skipping remediation (low time remaining)",
            )
            .await;
        } else {
            info!(run_id, "Security issues found, running remediation cycle");
            check_cancelled(state, &msg.team_id, run_id).await?;
            update_pass(state, &msg.team_id, run_id, "implement").await?;
            let sec_fix_result = implement::run(
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
                &triage_result.complexity,
                &provider,
                usage,
                &file_cache,
                Some(run_id),
            )
            .await?;
            write_conversation_log(state, &msg.team_id, run_id, "security_fix", &sec_fix_result.conversation_log).await;

            check_cancelled(state, &msg.team_id, run_id).await?;
            update_pass(state, &msg.team_id, run_id, "security").await?;
            let retry = match security::run(
                state,
                msg,
                &github,
                &plan_result,
                &branch_name,
                &repo_instructions,
                &provider,
                usage,
                &file_cache,
                Some(run_id),
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(run_id, error = %e, "Security retry errored, proceeding");
                    security::SecurityResult {
                        passed: true,
                        summary: format!("Security error: {e}"),
                    }
                }
            };
            if !retry.passed {
                warn!(
                    run_id,
                    "Security issues remain after remediation, proceeding with PR"
                );
            }
        }
    }

    // Security findings are already posted as PR review comments and stored in the
    // run record — no need to duplicate the full audit text into long-term memory.

    // --- Create Draft PR (triggers CI) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "pr").await?;

    // Resolve conflicts with main before PR
    match pr::resolve_conflicts(state, msg, &github, &branch_name, &provider, usage).await {
        Ok(true) => info!(run_id, "Resolved merge conflicts with main"),
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Conflict resolution failed, proceeding"),
    }

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let pr_result = pr::run(
        state,
        msg,
        &github,
        &branch_name,
        &plan_result,
        &voice,
        &provider,
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
    info!(run_id, pr_url = %pr_result.pr_url, "Draft PR created");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Draft PR created: #{}", pr_result.pr_number),
    )
    .await;

    // Save checkpoint and set awaiting_ci — Lambda returns here.
    // The webhook-driven resume flow handles CI results and review.
    save_checkpoint(
        state,
        &msg.team_id,
        run_id,
        "pr",
        &branch_name,
        0,
        usage,
    )
    .await;

    // Update run record with PR info + awaiting_ci status
    let duration = start.elapsed().as_secs();
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, \
             repo = :repo, team_repo = :tr, mcp_servers = :mcp",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("awaiting_ci"))
        .expression_attribute_values(":pr", attr_s(&pr_result.pr_url))
        .expression_attribute_values(":pn", attr_n(pr_result.pr_number))
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
        .expression_attribute_values(":b", attr_s(&branch_name))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("awaiting_ci"))
        .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(
                used_mcp_ids
                    .iter()
                    .map(|id| attr_s(id))
                    .collect(),
            ),
        )
        .send()
        .await?;

    info!(
        run_id,
        pr_number = pr_result.pr_number,
        "Run set to awaiting_ci — Lambda returning, webhook will trigger resume"
    );

    // Schedule a safety-net resume in case CI webhook is missed or repo has no CI
    if !state.config.ci_fix_queue_url.is_empty() {
        let resume_body = serde_json::json!({
            "type": "resume",
            "team_id": msg.team_id,
            "run_id": run_id,
            "installation_id": msg.installation_id,
        });
        let _ = state
            .sqs
            .send_message()
            .queue_url(&state.config.ci_fix_queue_url)
            .message_body(resume_body.to_string())
            .delay_seconds(120)
            .send()
            .await;
    }

    // Persist agent memory (extract learnings first)
    if let Some(mut mem) = agent_memory {
        mem.extract_learnings_from_conversation(
            &impl_result.conversation_log,
            provider.api_key(),
        ).await;
        if let Err(e) = mem.close_and_upload(state).await {
            warn!(run_id, error = %e, "Failed to persist agent memory");
        }
    }

    Ok(())
}

/// Run implement → security → PR for a single repo.
/// Returns a RepoPrResult with the created PR info.
#[allow(clippy::too_many_arguments)]
async fn run_repo_pipeline(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    plan_result: &plan::PlanResult,
    repo_tasks: &str,
    branch_name: &str,
    base_branch: &str,
    rules: &[String],
    repo_instructions: &str,
    complexity: &str,
    voice: &str,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    start: &Instant,
    run_id: &str,
    _agent_memory: &mut Option<AgentMemory>,
) -> Result<RepoPrResult, Box<dyn std::error::Error + Send + Sync>> {
    let repo_owner = &msg.repo_owner;
    let repo_name = &msg.repo_name;

    // Create working branch
    github
        .create_branch(repo_owner, repo_name, branch_name, base_branch)
        .await?;
    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), branch = %branch_name, "Created working branch");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Created branch {} on {}/{}", branch_name, repo_owner, repo_name),
    )
    .await;

    // Build a per-repo plan with overridden tasks
    let repo_plan = plan::PlanResult {
        proposal: plan_result.proposal.clone(),
        design: plan_result.design.clone(),
        tasks: repo_tasks.to_string(),
        spec: plan_result.spec.clone(),
        repo_tasks: vec![],
    };

    // --- Implement ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "implement").await?;

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let impl_result = implement::run(
        state,
        msg,
        github,
        &repo_plan,
        branch_name,
        rules,
        repo_instructions,
        None,
        complexity,
        provider,
        usage,
        file_cache,
        Some(run_id),
    )
    .await?;
    write_pass_trace(state, &msg.team_id, run_id, "implement", pass_start, &usage_before, usage, None).await;
    write_conversation_log(state, &msg.team_id, run_id, &format!("implement_{repo_owner}_{repo_name}"), &impl_result.conversation_log).await;
    save_checkpoint(state, &msg.team_id, run_id, "implement", branch_name, 0, usage).await;

    info!(
        run_id,
        repo = %format!("{repo_owner}/{repo_name}"),
        files = impl_result.files_modified.len(),
        "Implement complete"
    );
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Implemented {} file(s) in {}/{}", impl_result.files_modified.len(), repo_owner, repo_name),
    )
    .await;

    if impl_result.files_modified.is_empty() {
        return Err(format!(
            "No files modified in {}/{}. The issue may need more detail.",
            repo_owner, repo_name
        ).into());
    }

    // --- Adversarial Test (catches bugs before security/PR) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    if has_time_for_remediation(start) {
        update_pass(state, &msg.team_id, run_id, "test").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let test_issues = adversarial_test(
            state, msg, github, &repo_plan, branch_name, provider, usage, file_cache,
            &impl_result.files_modified, run_id,
        ).await;
        write_pass_trace(state, &msg.team_id, run_id, "test", pass_start, &usage_before, usage, None).await;

        if !test_issues.is_empty() {
            warn!(run_id, "Adversarial test found issues — running auto-fix");
            add_progress_note(
                state, &msg.team_id, run_id, "Test found issues — auto-fixing",
            ).await;

            let fix_plan = plan::PlanResult {
                proposal: String::new(), tasks: String::new(),
                spec: String::new(), design: String::new(), repo_tasks: vec![],
            };
            let fix_cache = FileCache::default();
            if let Err(e) = implement::run(
                state, msg, github, &fix_plan, branch_name, &[],
                "", Some(&test_issues), "low", provider, usage, &fix_cache, Some(run_id),
            ).await {
                warn!(run_id, error = %e, "Test auto-fix failed, proceeding anyway");
            }
        }
    }

    // --- Security Audit ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "security").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let security_result = match security::run(
        state, msg, github, &repo_plan, branch_name, repo_instructions, provider, usage, file_cache, Some(run_id),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(run_id, error = %e, "Security pass errored, proceeding");
            security::SecurityResult { passed: true, summary: format!("Security error: {e}") }
        }
    };
    write_pass_trace(state, &msg.team_id, run_id, "security", pass_start, &usage_before, usage, None).await;
    save_checkpoint(state, &msg.team_id, run_id, "security", branch_name, 0, usage).await;

    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), passed = security_result.passed, "Security audit complete");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        if security_result.passed { "Security audit passed" } else { "Security issues found, fixing" },
    )
    .await;

    // Security remediation
    if !security_result.passed {
        if has_time_for_remediation(start) {
            info!(run_id, "Running security remediation");
            check_cancelled(state, &msg.team_id, run_id).await?;
            let sec_fix = implement::run(
                state, msg, github, &repo_plan, branch_name, rules, repo_instructions,
                Some(&format!("Security audit found vulnerabilities. Fix ALL of the following:\n\n{}", security_result.summary)),
                complexity, provider, usage, file_cache, Some(run_id),
            )
            .await?;
            write_conversation_log(state, &msg.team_id, run_id, &format!("security_fix_{}", branch_name), &sec_fix.conversation_log).await;
        } else {
            warn!(run_id, "Skipping security remediation — not enough time");
        }
    }

    // Security findings are already posted as PR review comments and stored in the
    // run record — no need to duplicate the full audit text into long-term memory.

    // --- Create Draft PR ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "pr").await?;

    match pr::resolve_conflicts(state, msg, github, branch_name, provider, usage).await {
        Ok(true) => info!(run_id, "Resolved merge conflicts"),
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Conflict resolution failed, proceeding"),
    }

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let pr_result = pr::run(state, msg, github, branch_name, &repo_plan, voice, provider, usage).await?;
    write_pass_trace(state, &msg.team_id, run_id, "pr", pass_start, &usage_before, usage, None).await;

    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), pr_url = %pr_result.pr_url, "Draft PR created");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Draft PR #{} created for {}/{}", pr_result.pr_number, repo_owner, repo_name),
    )
    .await;

    Ok(RepoPrResult {
        branch: branch_name.to_string(),
        pr_number: pr_result.pr_number,
        pr_url: pr_result.pr_url,
    })
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
        .item("base_branch", attr_s(&msg.base_branch))
        // Store body for Jira tickets so retry can re-use it (GitHub re-fetches from API)
        .item("ticket_body", attr_s(if matches!(msg.source, TicketSource::Jira) { &msg.body } else { "" }))
        .item(
            "image_attachments",
            AttributeValue::S(
                serde_json::to_string(&msg.image_attachments).unwrap_or_default(),
            ),
        )
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

pub(crate) async fn update_pass(
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

/// Append a timestamped progress note to the run record for live activity display.
async fn add_progress_note(state: &WorkerState, team_id: &str, run_id: &str, message: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let entry = aws_sdk_dynamodb::types::AttributeValue::M(std::collections::HashMap::from([
        ("message".to_string(), attr_s(message)),
        ("timestamp".to_string(), attr_s(&now)),
    ]));
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET progress_notes = list_append(if_not_exists(progress_notes, :empty), :entry), updated_at = :t",
        )
        .expression_attribute_values(":entry", aws_sdk_dynamodb::types::AttributeValue::L(vec![entry]))
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
}

/// Write a conversation log to S3 for the agent log viewer.
async fn write_conversation_log(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass_name: &str,
    log: &[serde_json::Value],
) {
    if log.is_empty() {
        return;
    }
    let key = format!("runs/{team_id}/{run_id}/conversation/{pass_name}.json");
    match serde_json::to_vec(log) {
        Ok(body) => {
            if let Err(e) = state
                .s3
                .put_object()
                .bucket(&state.config.bucket_name)
                .key(&key)
                .body(body.into())
                .content_type("application/json")
                .send()
                .await
            {
                warn!(run_id, pass_name, error = %e, "Failed to write conversation log to S3");
            } else {
                info!(run_id, pass_name, entries = log.len(), "Conversation log saved to S3");
            }
        }
        Err(e) => {
            warn!(run_id, pass_name, error = %e, "Failed to serialize conversation log");
        }
    }
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

#[allow(clippy::too_many_arguments, dead_code)]
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
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
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
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
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

    // Check token usage against limit and send warning if needed
    check_token_usage_warning(state, &msg.team_id, usage).await;

    Ok(())
}

/// Check if the team has exceeded their configured monthly token limit.
/// Returns Some(reason) if exceeded, None if within budget or no limit set.
async fn check_budget_exceeded(state: &WorkerState, team_id: &str) -> Option<String> {
    // Load token limit
    let limit = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#TOKEN_LIMIT".to_string()),
        )
        .send()
        .await
        .ok()?
        .item()
        .and_then(|item| item.get("max_tokens").cloned())
        .and_then(|v| v.as_n().ok().cloned())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    if limit == 0 {
        return None; // No limit configured
    }

    // Load current month usage
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let total_used = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key("period", aws_sdk_dynamodb::types::AttributeValue::S(month))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .map(|item| {
            let ti = item
                .get("total_tokens_in")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let to = item
                .get("total_tokens_out")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            ti + to
        })
        .unwrap_or(0);

    if total_used >= limit {
        Some(format!(
            "Monthly token limit exceeded ({} / {} tokens). Increase your limit in Settings → AI Models.",
            total_used, limit
        ))
    } else {
        None
    }
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
             tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, \
             updated_at = :t, status_run_id = :sri",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("failed"))
        .expression_attribute_values(":err", attr_s(&error_msg[..error_msg.len().min(500)]))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
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

    // Check token usage against limit and send warning if needed
    check_token_usage_warning(state, &msg.team_id, usage).await;

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

pub(crate) fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

pub(crate) fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Release a ticket-level lock after a run completes (any terminal state).
async fn release_ticket_lock(state: &WorkerState, team_id: &str, ticket_id: &str) {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await;
}

/// Extend a ticket lock (e.g. when waiting for CI, extend to 30 min).
async fn extend_ticket_lock(state: &WorkerState, team_id: &str, ticket_id: &str, minutes: i64) {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let locked_until = (chrono::Utc::now() + chrono::Duration::minutes(minutes)).to_rfc3339();
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET locked_until = :lu, updated_at = :t")
        .expression_attribute_values(":lu", attr_s(&locked_until))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
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
        .ok_or_else(|| {
            format!(
                "GitHub App is not installed for this team. Please install it at https://github.com/apps/coderhelm/installations/new"
            )
            .into()
        })
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
pub(crate) async fn load_rules(state: &WorkerState, msg: &TicketMessage) -> Vec<String> {
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

/// Format the full OpenSpec as a single context block for downstream passes.
/// Compact OpenSpec summary for review/security passes.
/// Only includes proposal and acceptance criteria (skips design/tasks detail).
pub fn format_openspec_summary(plan: &plan::PlanResult) -> String {
    let mut block = String::from("\n\n## OpenSpec Summary\n");
    if !plan.proposal.is_empty() {
        block.push_str("### Proposal\n");
        // Cap proposal at 2KB
        if plan.proposal.len() > 2000 {
            block.push_str(&plan.proposal[..plan.proposal[..2000].rfind('\n').unwrap_or(2000)]);
            block.push_str("\n... (truncated)\n");
        } else {
            block.push_str(&plan.proposal);
            block.push('\n');
        }
    }
    if !plan.spec.is_empty() {
        block.push_str("\n### Acceptance Criteria\n");
        block.push_str(&plan.spec);
        block.push('\n');
    }
    block
}

pub fn format_openspec_block(plan: &plan::PlanResult) -> String {
    let mut block = String::from("\n\n## OpenSpec\n");
    if !plan.proposal.is_empty() {
        block.push_str("### Proposal\n");
        block.push_str(&plan.proposal);
        block.push('\n');
    }
    if !plan.design.is_empty() {
        block.push_str("\n### Design\n");
        block.push_str(&plan.design);
        block.push('\n');
    }
    if !plan.tasks.is_empty() {
        block.push_str("\n### Tasks\n");
        block.push_str(&plan.tasks);
        block.push('\n');
    }
    if !plan.spec.is_empty() {
        block.push_str("\n### Acceptance Criteria\n");
        block.push_str(&plan.spec);
        block.push('\n');
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
/// Keeps first 500 entries.
pub fn truncate_tree(paths: &[&str]) -> String {
    const MAX_ENTRIES: usize = 500;
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
    /// If triage picked the wrong repo, this suggests the correct one (owner/name).
    switch_repo: Option<String>,
}

/// Plan validation — catches bad plans before the expensive Implement pass.
/// Uses deterministic checks (scope, file existence, plan-text mentions) plus
/// an LLM repo-fit check as a final safety net.
async fn validate_plan(
    plan: &plan::PlanResult,
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    base_branch: &str,
    state: &WorkerState,
    team_id: &str,
    team_repos: &[String],
    provider: &provider::ModelProvider,
    usage: &mut TokenUsage,
) -> PlanValidation {
    let mut warnings = Vec::new();
    let mut blocked = false;
    let mut switch_repo: Option<String> = None;

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
            match github.read_file(repo_owner, repo_name, path, base_branch).await {
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

            // Check if these files exist in another enabled repo (triage may have picked wrong)
            let current_repo = format!("{repo_owner}/{repo_name}");
            let other_repos: Vec<&String> = team_repos
                .iter()
                .filter(|r| *r != &current_repo)
                .collect();

            for candidate in &other_repos {
                let parts: Vec<&str> = candidate.splitn(2, '/').collect();
                if parts.len() != 2 {
                    continue;
                }
                let (c_owner, c_name) = (parts[0], parts[1]);
                let mut found = 0usize;
                for path in &missing {
                    if github.read_file(c_owner, c_name, path, "HEAD").await.is_ok() {
                        found += 1;
                    }
                }
                if found > missing.len() / 2 {
                    info!(
                        current = %current_repo,
                        candidate = %candidate,
                        found,
                        missing = missing.len(),
                        "Plan files found in different repo — suggesting switch"
                    );
                    switch_repo = Some(candidate.to_string());
                    break;
                }
            }
        }
    }

    // 3. Plan-text repo mention check — if the plan explicitly names a different team repo
    //    more than the current one, the planner probably explored and found the right repo
    //    but triage started in the wrong one (common for Jira tickets).
    if switch_repo.is_none() {
        let current_repo = format!("{repo_owner}/{repo_name}");
        let full_plan = format!("{}\n{}\n{}\n{}", plan.proposal, plan.design, plan.tasks, plan.spec);
        let full_plan_lower = full_plan.to_lowercase();

        // Count mentions of each team repo using word-boundary regex to avoid
        // substring false positives (e.g. "api" matching inside "gangway-api")
        let mut best_candidate: Option<(String, usize)> = None;
        let current_name_lower = repo_name.to_lowercase();
        let current_re = regex::Regex::new(&format!(r"(?i)\b{}\b", regex::escape(&current_name_lower)))
            .unwrap_or_else(|_| regex::Regex::new("^$").unwrap());
        let current_mentions = current_re.find_iter(&full_plan_lower).count();

        for candidate in team_repos {
            if *candidate == current_repo {
                continue;
            }
            if let Some((_owner, cand_name)) = candidate.split_once('/') {
                let cand_lower = cand_name.to_lowercase();
                // First try full owner/name match, then word-bounded name match
                let full_match_count = full_plan_lower.matches(&candidate.to_lowercase()).count();
                let name_re = regex::Regex::new(&format!(r"(?i)\b{}\b", regex::escape(&cand_lower)))
                    .unwrap_or_else(|_| regex::Regex::new("^$").unwrap());
                let cand_mentions = full_match_count + name_re.find_iter(&full_plan_lower).count();

                // Candidate repo must be mentioned at least 2 times and more than current
                if cand_mentions >= 2 && cand_mentions > current_mentions {
                    if best_candidate.as_ref().map_or(true, |(_, best)| cand_mentions > *best) {
                        best_candidate = Some((candidate.clone(), cand_mentions));
                    }
                }
            }
        }

        if let Some((candidate, mentions)) = best_candidate {
            info!(
                current = %current_repo,
                candidate = %candidate,
                mentions,
                current_mentions,
                "Plan text references different repo more often — suggesting switch"
            );
            switch_repo = Some(candidate);
        }
    }

    // 4. LLM repo-fit check — ask the LLM if this plan actually belongs in this repo.
    //    Uses AGENTS#GLOBAL context, repo description, directory listing, and plan summary
    //    to detect mismatches that deterministic checks miss (e.g. new files, same language).
    if switch_repo.is_none() && team_repos.len() > 1 {
        let current_repo = format!("{repo_owner}/{repo_name}");

        // Gather context: AGENTS#GLOBAL + repo info + directory listing
        let global_agents = load_content(state, team_id, "AGENTS#GLOBAL").await;

        let (current_lang, current_desc) = github.get_repo_info(repo_owner, repo_name).await
            .unwrap_or((None, None));
        let current_dirs = match github.list_directory(repo_owner, repo_name, "", base_branch).await {
            Ok(entries) => entries.iter().take(20).map(|e| e.name.clone()).collect::<Vec<_>>().join(", "),
            Err(_) => String::new(),
        };

        // Build candidate repo summaries
        let mut other_repo_info = Vec::new();
        for candidate in team_repos {
            if *candidate == current_repo { continue; }
            let parts: Vec<&str> = candidate.splitn(2, '/').collect();
            if parts.len() != 2 { continue; }
            let (cand_lang, cand_desc) = github.get_repo_info(parts[0], parts[1]).await
                .unwrap_or((None, None));
            let cand_dirs = match github.list_directory(parts[0], parts[1], "", "HEAD").await {
                Ok(entries) => entries.iter().take(15).map(|e| e.name.clone()).collect::<Vec<_>>().join(", "),
                Err(_) => String::new(),
            };
            let mut info = format!("  {candidate}");
            if let Some(l) = cand_lang { info.push_str(&format!(" (lang: {l})")); }
            if let Some(d) = cand_desc { info.push_str(&format!(" — {d}")); }
            if !cand_dirs.is_empty() { info.push_str(&format!("\n    files: [{cand_dirs}]")); }
            other_repo_info.push(info);
        }

        let plan_summary = format!(
            "Proposal: {}\nDesign: {}\nTasks: {}",
            &plan.proposal[..plan.proposal.len().min(500)],
            &plan.design[..plan.design.len().min(500)],
            &plan.tasks[..plan.tasks.len().min(500)],
        );

        let prompt = format!(
            r#"You are validating whether an implementation plan belongs in the correct repository.

## Current repository: {current_repo}
Language: {lang}
Description: {desc}
Top-level files: [{dirs}]

## Other available repositories:
{others}
{agents_section}
## Plan summary:
{plan_summary}

## Question
Does this plan belong in {current_repo}, or should it be in one of the other repositories?

Rules:
- Consider what each repo is FOR based on its name, description, directory structure, and AGENTS context.
- Data/ETL/pipeline repos are for data work only, NOT application features.
- If the plan mentions a specific service or API (e.g. "gangway", "adyen webhook"), match it to the repo that OWNS that code.
- If the plan clearly belongs in the current repo, respond: CORRECT
- If it belongs in a different repo, respond: SWITCH owner/name

Respond with ONLY one line: either "CORRECT" or "SWITCH owner/name"."#,
            lang = current_lang.as_deref().unwrap_or("unknown"),
            desc = current_desc.as_deref().unwrap_or("none"),
            dirs = current_dirs,
            others = other_repo_info.join("\n"),
            agents_section = if global_agents.is_empty() {
                String::new()
            } else {
                format!("\n## Repository context (AGENTS#GLOBAL):\n{global_agents}\n")
            },
        );

        let system = "You are a repository routing validator. Be precise — only suggest a switch \
                      if you are confident the plan does NOT belong in the current repo.";

        let mut messages = vec![("user".to_string(), vec![serde_json::json!({"type": "text", "text": prompt})])];

        match provider::converse(
            state,
            provider,
            provider.primary_model_id(),
            system,
            &mut messages,
            &[],
            &triage::NoOpExecutor,
            usage,
            llm::ConverseOptions { max_turns: 1, max_tokens: 128 },
            None,
            None,
        ).await {
            Ok(response) => {
                let trimmed: &str = response.trim();
                if let Some(rest) = trimmed.strip_prefix("SWITCH") {
                    let clean = rest.trim().trim_matches('`').trim().to_string();
                    if team_repos.iter().any(|r| r == &clean) {
                        info!(
                            current = %current_repo,
                            suggested = %clean,
                            "LLM repo-fit check suggests switching repo"
                        );
                        switch_repo = Some(clean);
                    } else {
                        warn!(
                            current = %current_repo,
                            suggested = %clean,
                            "LLM suggested repo not in team repos, ignoring"
                        );
                    }
                } else {
                    info!(current = %current_repo, "LLM repo-fit check: CORRECT");
                }
            }
            Err(e) => {
                warn!(error = %e, "LLM repo-fit check failed, proceeding with current repo");
            }
        }
    }

    PlanValidation { warnings, blocked, switch_repo }
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

/// Write a per-pass trace record to the traces table.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_pass_trace(
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
pub(crate) async fn save_checkpoint(
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

/// Build the progress comment body for the GitHub issue.
fn format_progress(current_phase: &str, run_id: &str) -> String {
    let phases = ["triage", "plan", "implement", "test", "security", "pr"];
    let labels = ["Triage", "Plan", "Implement", "Test", "Security", "PR"];
    let current_idx = phases.iter().position(|p| *p == current_phase);

    let mut rows = String::new();
    for (i, label) in labels.iter().enumerate() {
        let status = match current_idx {
            Some(ci) if i < ci => "✅ Done",
            Some(ci) if i == ci => "🔄 In progress",
            _ => "⏳ Pending",
        };
        rows.push_str(&format!("| {label} | {status} |\n"));
    }

    format!(
        "🔄 **Coderhelm is working on this**\n\n| Phase | Status |\n|-------|--------|\n{rows}\n[View run →](https://app.coderhelm.com/runs/detail?id={run_id})"
    )
}

/// Update the progress comment on the GitHub issue.
async fn update_progress_comment(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    comment_id: Option<u64>,
    phase: &str,
    run_id: &str,
) {
    if let Some(cid) = comment_id {
        let body = format_progress(phase, run_id);
        if let Err(e) = github.edit_issue_comment(owner, repo, cid, &body).await {
            tracing::warn!(error = %e, "Failed to update progress comment");
        }
    }
}

async fn check_token_usage_warning(state: &WorkerState, team_id: &str, _usage: &TokenUsage) {
    use crate::clients::email::EmailEvent;

    // Get team's token limit
    let limit = match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#TOKEN_LIMIT".to_string()),
        )
        .send()
        .await
    {
        Ok(r) => r
            .item()
            .and_then(|item| item.get("max_tokens"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0),
        Err(_) => return,
    };

    if limit == 0 {
        return;
    }

    // Get current month usage from analytics
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let total_used = match state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "period",
            aws_sdk_dynamodb::types::AttributeValue::S(month.clone()),
        )
        .send()
        .await
    {
        Ok(r) => {
            let item = r.item();
            let ti = item
                .and_then(|i| i.get("total_tokens_in"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let to = item
                .and_then(|i| i.get("total_tokens_out"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            ti + to
        }
        Err(_) => return,
    };

    let usage_pct = ((total_used as f64 / limit as f64) * 100.0) as u8;

    // Check if we already sent a warning this month (prevent spamming)
    let warning_sk = format!(
        "TOKEN_WARNING#{}#{}",
        month,
        if usage_pct >= 100 { "100" } else { "80" }
    );
    let already_sent = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(warning_sk.clone()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .is_some();

    if already_sent {
        return;
    }

    fn format_tokens(n: u64) -> String {
        if n >= 1_000_000 {
            format!("{:.1}M", n as f64 / 1_000_000.0)
        } else if n >= 1_000 {
            format!("{:.1}K", n as f64 / 1_000.0)
        } else {
            n.to_string()
        }
    }

    let event = if usage_pct >= 100 {
        Some(EmailEvent::TokenLimitReached {
            month: month.clone(),
            used_tokens: format_tokens(total_used),
            limit_tokens: format_tokens(limit),
        })
    } else if usage_pct >= 80 {
        Some(EmailEvent::TokenWarning80 {
            month: month.clone(),
            used_tokens: format_tokens(total_used),
            limit_tokens: format_tokens(limit),
            usage_pct,
        })
    } else {
        None
    };

    if let Some(event) = event {
        // Mark warning as sent
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .item("sk", aws_sdk_dynamodb::types::AttributeValue::S(warning_sk))
            .item(
                "sent_at",
                aws_sdk_dynamodb::types::AttributeValue::S(chrono::Utc::now().to_rfc3339()),
            )
            .send()
            .await;

        if let Err(e) = email::send_notification(state, team_id, event).await {
            error!("Failed to send token warning email: {e}");
        }
    }
}

/// Look up Jira project → repo mapping from DynamoDB.
async fn lookup_jira_project_repo(
    state: &WorkerState,
    team_id: &str,
    project_key: &str,
) -> Option<String> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("JIRA#PROJECT#{project_key}")))
        .send()
        .await
        .ok()?
        .item?;

    result
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .cloned()
}

/// Load an existing plan from S3 if one was already generated for this ticket.
/// Returns None if no plan exists or any file is missing.
async fn load_existing_plan(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
    context_hash: &str,
) -> Option<plan::PlanResult> {
    let prefix = format!(
        "teams/{}/runs/{}/openspec",
        team_id,
        ticket_id.to_lowercase()
    );

    // Check if ticket context has changed (body, images, etc.)
    let hash_key = format!("{prefix}/context_hash.txt");
    if let Ok(output) = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(&hash_key)
        .send()
        .await
    {
        if let Ok(bytes) = output.body.collect().await {
            let stored_hash = String::from_utf8(bytes.to_vec()).unwrap_or_default();
            if stored_hash.trim() != context_hash {
                info!("Ticket context changed (hash mismatch) — forcing re-plan");
                return None;
            }
        }
    }

    let mut files = std::collections::HashMap::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        match state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = output.body.collect().await.ok()?;
                let text = String::from_utf8(bytes.to_vec()).ok()?;
                if text.trim().is_empty() {
                    return None;
                }
                files.insert(*name, text);
            }
            Err(_) => return None,
        }
    }

    Some(plan::PlanResult {
        proposal: files.remove("proposal.md").unwrap_or_default(),
        design: files.remove("design.md").unwrap_or_default(),
        tasks: files.remove("tasks.md").unwrap_or_default(),
        spec: files.remove("spec.md").unwrap_or_default(),
        repo_tasks: vec![],
    })
}

/// Adversarial test pass — an LLM reads the diff and modified files looking for
/// bugs, broken syntax, missing error handling, and destructive deletions.
/// Returns empty string if clean, or a description of issues for the implement agent to fix.
#[allow(clippy::too_many_arguments)]
async fn adversarial_test(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &crate::clients::github::GitHubClient,
    plan: &plan::PlanResult,
    branch: &str,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    files_modified: &[String],
    run_id: &str,
) -> String {
    use crate::agent::llm;
    use crate::agent::provider;

    let files_list = files_modified.join(", ");
    let openspec_block = format_openspec_summary(plan);

    let system = format!(
        "You are an adversarial code tester for {owner}/{repo}. \
         Your job is to find REAL bugs that would break the build or cause runtime errors. \
         You are READ-ONLY — you cannot modify files. You report issues for another agent to fix.",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"An implementation agent just modified these files: {files_list}
{openspec}
Your job: find bugs that WILL break this code. Use `get_diff` to see all changes, then `read_file` on each modified file to verify the FULL file is correct.

Check for:
1. **Syntax errors** — unbalanced braces/brackets/parens, orphaned catch/else blocks, unclosed strings or template literals
2. **Broken imports** — importing symbols that don't exist or were renamed
3. **Type errors** — wrong argument types, missing required properties, calling methods that don't exist on the type
4. **Destructive deletions** — code that was REMOVED but not replaced with equivalent functionality (e.g. exported functions, API endpoints, event handlers that vanished)
5. **Unreachable code** — code after return/throw statements
6. **Missing error handling** — try without catch, async without await, unhandled promise rejections

Do NOT report:
- Style/formatting issues
- Performance suggestions
- Missing tests
- Anything that's a preference not a bug

Read EVERY modified file in full. Do not skip any.

If you find issues, respond with "ISSUES_FOUND:" followed by a detailed list with file paths and line numbers.
If everything is clean, respond with "CLEAN" and nothing else."#,
        files_list = files_list,
        openspec = openspec_block,
    );

    let tools = review::review_tools();
    let executor = review::ReviewToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch,
        base_branch: &msg.base_branch,
        file_cache,
    };

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let response = match provider::converse(
        state,
        provider,
        model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 15,
            max_tokens: 8192,
        },
        None,
        None,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(run_id, error = %e, "Adversarial test errored — skipping");
            return String::new();
        }
    };

    tracing::info!(run_id, "Adversarial test result: {}", &response[..response.len().min(200)]);

    if response.starts_with("CLEAN") || !response.contains("ISSUES_FOUND:") {
        String::new()
    } else {
        format!(
            "CRITICAL: An adversarial tester found the following bugs in your implementation. \
             Read each file mentioned, verify the issue, and fix it. \
             Do NOT remove existing functionality — only fix what's broken.\n\n{}",
            response
        )
    }
}
