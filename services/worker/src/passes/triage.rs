use serde::{Deserialize, Serialize};
use tracing::info;

use crate::agent::provider::ModelProvider;
use crate::agent::{llm, provider};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TriageResult {
    pub complexity: String,
    pub summary: String,
    pub branch_slug: String,
    pub clarity: String,
    pub questions: Vec<String>,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    _github: &GitHubClient,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<TriageResult, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You are a ticket triage agent for a GitHub issue. \
                  Analyze the issue and classify it. Return only valid JSON.";

    let prompt = format!(
        r#"Analyze this GitHub issue and return a JSON classification.

## Issue
Repository: {owner}/{repo}
Number: #{number}
Title: {title}

Body:
{body}

## Instructions
Return a JSON object with these fields:
- "complexity": "simple" (1-3 files), "medium" (4-10 files), "complex" (10+ files)
- "summary": one-line summary of what needs to be implemented
- "branch_slug": short kebab-case slug for the branch suffix (e.g., "add-retry-logic")
- "clarity": "clear" if you have enough info to implement, "needs_clarification" if not
- "questions": list of specific questions if clarity is "needs_clarification" (empty list if clear)

Rules:
- If the issue is vague, ambiguous, or missing acceptance criteria, set clarity to "needs_clarification".
- Return ONLY the JSON object, no other text."#,
        owner = msg.repo_owner,
        repo = msg.repo_name,
        number = msg.issue_number,
        title = msg.title,
        body = msg.body,
    );

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let response = provider::converse(
        state,
        provider,
        model_id,
        system,
        &mut messages,
        &[],
        &NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            max_tokens: 16384,
        },
        None,
    )
    .await?;

    // Parse JSON from response (strip markdown fences if present)
    let raw = response.trim();
    let json_str = if raw.starts_with("```") {
        let inner = raw.split('\n').skip(1).collect::<Vec<_>>().join("\n");
        inner.trim_end_matches("```").trim().to_string()
    } else {
        raw.to_string()
    };

    let result: TriageResult = serde_json::from_str(&json_str)?;
    info!(complexity = %result.complexity, clarity = %result.clarity, "Triage complete");
    Ok(result)
}

/// No-op tool executor for passes that don't need tools.
pub(crate) struct NoOpExecutor;

#[async_trait::async_trait]
impl llm::ToolExecutor for NoOpExecutor {
    async fn execute(
        &self,
        name: &str,
        _input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("No tools available in triage pass (called: {name})").into())
    }
}

/// Select the best repo for a Jira ticket when no explicit repo mapping is provided.
/// NOTE: Currently unused — repo selection is handled by project mapping + validate_plan.
/// Kept for potential future use as a fallback.
#[allow(dead_code)]
pub async fn select_repo(
    state: &WorkerState,
    msg: &TicketMessage,
    repos: &[String],
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You are a repo-selection agent. Given a Jira ticket and a list of repositories \
                  with descriptions, pick the single best repository for implementation. \
                  Rules: \
                  1. Read the repository descriptions carefully — match the ticket to the repo that OWNS that feature area. \
                  2. Data/analytics/ETL/pipeline repos (e.g. airflow, dbt) are ONLY for data pipeline work, NOT for application features or API integrations. \
                  3. If the ticket mentions specific services, APIs, SDKs, or integrations (e.g. Adyen, Exerp API, billing), pick the repo that implements or calls those services — typically an API/backend repo, not a data repo. \
                  4. Terraform/infrastructure repos are ONLY for infra changes (IAM, networking, deployment config). \
                  5. When unsure, prefer application/API repos over data or infrastructure repos. \
                  Think step by step: (1) what is this ticket about, (2) which repo owns that area, (3) return ONLY the repo in owner/name format.";

    let repo_list = repos
        .iter()
        .map(|r| format!("- {r}"))
        .collect::<Vec<_>>()
        .join("\n");

    let global_agents = super::load_content(state, &msg.team_id, "AGENTS#GLOBAL").await;

    let context_section = if global_agents.is_empty() {
        String::new()
    } else {
        format!("\n## Repository context\n{global_agents}\n")
    };

    let prompt = format!(
        r#"Pick the best repository for this Jira ticket.

## Ticket
Key: {ticket_id}
Title: {title}

Description:
{body}
{context_section}
## Available repositories
{repo_list}

Think step by step:
1. What is this ticket about? (one sentence)
2. Which repo owns that feature area based on the descriptions above?
3. Why is it NOT any of the other repos?

Then on the LAST line, return ONLY the repository in `owner/name` format."#,
        ticket_id = msg.ticket_id,
        title = msg.title,
        body = msg.body,
    );

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let response = provider::converse(
        state,
        provider,
        model_id,
        system,
        &mut messages,
        &[],
        &NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            max_tokens: 512,
        },
        None,
    )
    .await?;

    // Extract repo from the last non-empty line (reasoning comes before it)
    let last_line = response
        .trim()
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .trim_matches('`')
        .trim()
        .to_string();

    info!(response = %response.trim(), parsed = %last_line, "Triage repo selection reasoning");

    let selected = last_line;

    // Validate the selection is in our list
    if repos.contains(&selected) {
        info!(repo = %selected, "Repo selected for Jira ticket");
        Ok(selected)
    } else {
        // Fuzzy match — LLM might format slightly differently
        for repo in repos {
            if repo.eq_ignore_ascii_case(&selected) {
                info!(repo = %repo, "Repo selected (case-insensitive match)");
                return Ok(repo.clone());
            }
        }
        // Fall back to first repo
        info!(selected = %selected, fallback = %repos[0], "LLM pick not in list, falling back");
        Ok(repos[0].clone())
    }
}
