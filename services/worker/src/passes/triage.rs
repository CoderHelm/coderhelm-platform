use serde::{Deserialize, Serialize};
use tracing::info;

use crate::agent::llm;
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

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let response = llm::converse(state, system, &mut messages, &[], &NoOpExecutor, usage).await?;

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
