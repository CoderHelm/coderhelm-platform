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
            max_turns: 40,
            max_tokens: 16384,
        },
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
pub async fn select_repo(
    state: &WorkerState,
    msg: &TicketMessage,
    repos: &[String],
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You are a repo-selection agent. Given a Jira ticket, repository descriptions, \
                  and a list of repositories, pick the single best repository for this work. \
                  Return ONLY the repo in owner/name format.";

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

Return ONLY the repository in `owner/name` format. No explanation."#,
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
            max_turns: 40,
            max_tokens: 16384,
        },
    )
    .await?;

    let selected = response.trim().trim_matches('`').trim().to_string();

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
