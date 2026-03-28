use serde_json::json;
use tracing::info;

use crate::agent::llm;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::WorkerState;

pub struct PrResult {
    pub pr_number: u64,
    pub pr_url: String,
    pub branch: String,
    pub draft: bool,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    usage: &mut TokenUsage,
) -> Result<PrResult, Box<dyn std::error::Error + Send + Sync>> {
    // Get diff for body summary
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, "main", branch)
        .await?;
    let diff_summary = format_diff_summary(&diff);

    // Generate PR body via LLM
    let system =
        "You are writing a pull request description. Be concise and technical. Return only markdown.";

    let prompt = format!(
        r#"Write a concise pull request description for issue #{number}: {title}

## Summary
{summary}

## Files Changed
{diff_summary}

## Instructions
Write a PR description following this structure:

1. **Problem** — One sentence: what the issue asks for.
2. **Changes** — Bolded per-area headers with bullet details. Keep it tight.
3. **Risk** — State risk level in bold (**Low**, **Medium**), then why it's safe.
4. **Verification** — Numbered steps to verify the change.

Rules:
- Keep it short. Don't pad short changes with long descriptions.
- No filler phrases, no hedging, no emojis.
- Backticks for file paths, function names, env vars, CLI commands.
- Bold for emphasis on key concepts.

Return ONLY the markdown body text."#,
        number = msg.issue_number,
        title = msg.title,
        summary = plan.proposal,
        diff_summary = diff_summary,
    );

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let body_text = llm::converse(
        state,
        system,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
    )
    .await?;

    // Add issue link at the top
    let full_body = format!(
        "Closes #{number}\n\n{body_text}",
        number = msg.issue_number,
        body_text = body_text.trim(),
    );

    // Create PR title
    let mut title = format!("#{}: {}", msg.issue_number, msg.title);
    if title.len() > 72 {
        title.truncate(69);
        title.push_str("...");
    }

    // Create draft PR
    let pr_data = github
        .create_pull_request(
            &msg.repo_owner,
            &msg.repo_name,
            &title,
            &full_body,
            branch,
            "main",
            true,
        )
        .await?;

    let pr_number = pr_data
        .get("number")
        .and_then(|v| v.as_u64())
        .ok_or("Missing PR number")?;
    let pr_url = pr_data
        .get("html_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    info!(pr_number, pr_url = %pr_url, "Draft PR created");

    // Check if repo has CI — if not, mark ready immediately
    let has_ci = github
        .check_path_exists(
            &msg.repo_owner,
            &msg.repo_name,
            ".github/workflows",
            "main",
        )
        .await;

    let draft = if has_ci {
        true
    } else {
        info!("No CI workflows found — marking PR ready");
        github
            .mark_pr_ready(&msg.repo_owner, &msg.repo_name, pr_number)
            .await?;
        false
    };

    Ok(PrResult {
        pr_number,
        pr_url,
        branch: branch.to_string(),
        draft,
    })
}

fn format_diff_summary(diff: &serde_json::Value) -> String {
    let files = match diff.get("files").and_then(|v| v.as_array()) {
        Some(f) => f,
        None => return "(no changes)".to_string(),
    };

    files
        .iter()
        .map(|f| {
            let path = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
            let status = f.get("status").and_then(|v| v.as_str()).unwrap_or("modified");
            let adds = f.get("additions").and_then(|v| v.as_u64()).unwrap_or(0);
            let dels = f.get("deletions").and_then(|v| v.as_u64()).unwrap_or(0);
            format!("- {path} ({status}: +{adds}/-{dels})")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
