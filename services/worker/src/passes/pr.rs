use tracing::{info, warn};

use crate::agent::llm;
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::WorkerState;

#[allow(dead_code)]
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
    voice: &str,
    usage: &mut TokenUsage,
) -> Result<PrResult, Box<dyn std::error::Error + Send + Sync>> {
    // Get diff for body summary
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, "main", branch)
        .await?;
    let diff_summary = format_diff_summary(&diff);

    // Generate PR body via LLM
    let voice_block = if voice.is_empty() {
        String::new()
    } else {
        format!("\n\nIMPORTANT — Match the team's voice and tone as described below:\n{voice}")
    };
    let system = format!(
        "You are writing a pull request description. Be concise and technical. Return only markdown.{voice_block}"
    );

    let ticket_ref = match msg.source {
        TicketSource::Github => format!("#{}", msg.issue_number),
        TicketSource::Jira => msg.ticket_id.clone(),
    };

    let prompt = format!(
        r#"Write a concise pull request description for ticket {ticket_ref}: {title}

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
- Use asterisks (*) for bullet lists, never dashes (-).

Return ONLY the markdown body text."#,
        ticket_ref = ticket_ref,
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
        &state.config.light_model_id,
        &system,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
    )
    .await?;

    // Add issue link for GitHub tickets.
    // Strip any "Closes #N" the model may have included to avoid duplication.
    let clean_body: String = body_text
        .lines()
        .filter(|line| {
            let trimmed = line.trim().to_lowercase();
            !(trimmed.starts_with("closes #")
                || trimmed.starts_with("fixes #")
                || trimmed.starts_with("resolves #"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let clean_body = clean_body.trim();

    let full_body = if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        format!(
            "Closes #{number}\n\n{clean_body}",
            number = msg.issue_number,
        )
    } else {
        format!("Source ticket: {}\n\n{clean_body}", msg.ticket_id)
    };

    // Create PR title
    let mut title = match msg.source {
        TicketSource::Github => format!("#{}: {}", msg.issue_number, msg.title),
        TicketSource::Jira => format!("{}: {}", msg.ticket_id, msg.title),
    };
    if title.len() > 72 {
        title.truncate(69);
        title.push_str("...");
    }

    // Create draft PR
    // Create PR (not draft — ready for review immediately)
    let pr_data = github
        .create_pull_request(
            &msg.repo_owner,
            &msg.repo_name,
            &title,
            &full_body,
            branch,
            "main",
            false,
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

    info!(pr_number, pr_url = %pr_url, "PR created");

    Ok(PrResult {
        pr_number,
        pr_url,
        branch: branch.to_string(),
        draft: false,
    })
}

/// Attempt to merge main into the feature branch before creating the PR.
/// If there are conflicts, resolve them with the LLM and commit the resolution.
/// Returns Ok(true) if conflicts were found and resolved, Ok(false) if no conflicts.
pub async fn resolve_conflicts(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    usage: &mut TokenUsage,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Try to merge main into the feature branch
    let merged = github
        .merge_branch(&msg.repo_owner, &msg.repo_name, branch, "main")
        .await?;

    if merged {
        info!(branch, "Branch is up-to-date with main (no conflicts)");
        return Ok(false);
    }

    // Conflicts detected — find which files conflict
    info!(branch, "Merge conflicts detected, resolving with LLM");

    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, "main", branch)
        .await?;

    let files = diff
        .get("files")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // For each modified file that exists in both branches, read both versions and resolve
    let mut resolved_files: Vec<FileOp> = Vec::new();

    for file in &files {
        let path = match file.get("filename").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => continue,
        };
        let status = file
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("modified");

        // Only resolve files that exist on both sides (modified or changed)
        if status == "added" || status == "removed" {
            continue;
        }

        // Read the file from both branches
        let main_content = match github
            .read_file(&msg.repo_owner, &msg.repo_name, path, "main")
            .await
        {
            Ok(c) => c,
            Err(_) => continue, // file doesn't exist on main, skip
        };
        let branch_content = match github
            .read_file(&msg.repo_owner, &msg.repo_name, path, branch)
            .await
        {
            Ok(c) => c,
            Err(_) => continue,
        };

        if main_content == branch_content {
            continue;
        }

        // Use LLM to resolve the conflict
        let system = "You are a merge conflict resolver. Given two versions of a file (main and branch), produce the merged file that incorporates both sets of changes. Return ONLY the merged file content, no explanations or markdown fences.".to_string();

        let prompt = format!(
            "Merge these two versions of `{path}`.\n\n## main version\n```\n{main_content}\n```\n\n## branch version (our changes — prefer these)\n```\n{branch_content}\n```\n\nReturn the merged file content. Prefer the branch version when changes conflict directly.",
        );

        let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
            .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
            .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
            .build()?];

        let merged_content = llm::converse(
            state,
            &state.config.light_model_id,
            &system,
            &mut messages,
            &[],
            &super::triage::NoOpExecutor,
            usage,
        )
        .await?;

        resolved_files.push(FileOp::Write {
            path: path.to_string(),
            content: merged_content,
        });
    }

    if resolved_files.is_empty() {
        warn!(
            branch,
            "Conflict detected but no files to resolve — retrying merge"
        );
        return Err("Merge conflict detected but could not identify conflicting files".into());
    }

    info!(
        branch,
        files = resolved_files.len(),
        "Resolved conflicts, committing"
    );

    // Commit the resolved files
    github
        .batch_write(
            &msg.repo_owner,
            &msg.repo_name,
            branch,
            "Resolve merge conflicts with main",
            &resolved_files,
        )
        .await?;

    // Verify the merge now succeeds
    let retry = github
        .merge_branch(&msg.repo_owner, &msg.repo_name, branch, "main")
        .await?;

    if !retry {
        warn!(branch, "Conflicts remain after resolution attempt");
    }

    Ok(true)
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
            let status = f
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("modified");
            let adds = f.get("additions").and_then(|v| v.as_u64()).unwrap_or(0);
            let dels = f.get("deletions").and_then(|v| v.as_u64()).unwrap_or(0);
            format!("- {path} ({status}: +{adds}/-{dels})")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
