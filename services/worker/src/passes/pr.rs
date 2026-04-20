use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::agent::{llm, provider};
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
    pub node_id: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    voice: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<PrResult, Box<dyn std::error::Error + Send + Sync>> {
    // Get diff for body summary
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;
    let diff_summary = format_diff_summary(&diff);

    // Check for PR template in the repo
    let pr_template = fetch_pr_template(github, &msg.repo_owner, &msg.repo_name, &msg.base_branch).await;

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

    let template_block = if let Some(ref tmpl) = pr_template {
        format!(
            "\n\n## PR Template\nThe repository has a pull request template. Follow its structure and fill in the sections:\n\n```\n{tmpl}\n```\n\nFill in ALL sections from the template. Replace placeholder text with actual content."
        )
    } else {
        String::new()
    };

    let instructions = if pr_template.is_some() {
        "Follow the PR template above. Fill in every section with relevant content from this change. Remove any placeholder/instruction text."
    } else {
        "Write a PR description following this structure:\n\n\
         1. **Problem** — One sentence: what the issue asks for.\n\
         2. **Changes** — Bolded per-area headers with bullet details. Keep it tight.\n\
         3. **Risk** — State risk level in bold (**Low**, **Medium**), then why it's safe.\n\
         4. **Verification** — Numbered steps to verify the change."
    };

    let prompt = format!(
        r#"Write a concise pull request description for ticket {ticket_ref}: {title}

## Summary
{summary}

## Files Changed
{diff_summary}{template_block}

## Instructions
{instructions}

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
        template_block = template_block,
        instructions = instructions,
    );

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let body_text = provider::converse(
        state,
        provider,
        model_id,
        &system,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            max_tokens: 4096,
        },
        None,
        None,
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
        // Avoid duplicating "Source ticket:" if it's already in the body
        if clean_body.contains(&format!("Source ticket: {}", msg.ticket_id)) {
            clean_body.to_string()
        } else {
            format!("Source ticket: {}\n\n{clean_body}", msg.ticket_id)
        }
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

    // Check if a PR already exists for this branch (e.g. from a re-run or retry)
    let existing_pr = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;

    let (pr_number, pr_url, node_id) = if let Some(pr_data) = existing_pr {
        let number = pr_data.get("number").and_then(|v| v.as_u64()).ok_or("Missing PR number")?;
        let url = pr_data.get("html_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let nid = pr_data.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        info!(pr_number = number, pr_url = %url, "Using existing PR");
        (number, url, nid)
    } else {
        // Create draft PR — CI triggers on PR creation, and we'll mark it ready after tests/review pass
        let pr_data = github
            .create_pull_request(
                &msg.repo_owner,
                &msg.repo_name,
                &title,
                &full_body,
                branch,
                &msg.base_branch,
                true,
            )
            .await?;

        let number = pr_data.get("number").and_then(|v| v.as_u64()).ok_or("Missing PR number")?;
        let url = pr_data.get("html_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let nid = pr_data.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        info!(pr_number = number, pr_url = %url, "PR created");
        (number, url, nid)
    };

    Ok(PrResult {
        pr_number,
        pr_url,
        branch: branch.to_string(),
        draft: true,
        node_id,
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
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Try to merge main into the feature branch
    let merged = github
        .merge_branch(&msg.repo_owner, &msg.repo_name, branch, &msg.base_branch)
        .await?;

    if merged {
        info!(branch, "Branch is up-to-date with base (no conflicts)");
        return Ok(false);
    }

    // Conflicts detected — find which files conflict
    info!(branch, "Merge conflicts detected, resolving with LLM");

    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
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
            .read_file(&msg.repo_owner, &msg.repo_name, path, &msg.base_branch)
            .await
        {
            Ok(c) => c,
            Err(_) => continue, // file doesn't exist on base branch, skip
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

        // Cap file contents at 16KB to control token usage
        let cap = |s: &str, label: &str| -> String {
            if s.len() > 16_000 {
                let cut = s[..16_000].rfind('\n').unwrap_or(16_000);
                format!(
                    "{}... (truncated — {} is {} bytes)",
                    &s[..cut],
                    label,
                    s.len()
                )
            } else {
                s.to_string()
            }
        };
        let main_capped = cap(&main_content, &format!("main:{path}"));
        let branch_capped = cap(&branch_content, &format!("branch:{path}"));

        // Use LLM to resolve the conflict
        let system = "You are a merge conflict resolver. Given two versions of a file (main and branch), produce the merged file that incorporates both sets of changes. Return ONLY the merged file content, no explanations or markdown fences.".to_string();

        let prompt = format!(
            "Merge these two versions of `{path}`.\n\n## main version\n```\n{main_capped}\n```\n\n## branch version (our changes — prefer these)\n```\n{branch_capped}\n```\n\nReturn the merged file content. Prefer the branch version when changes conflict directly.",
        );

        let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

        let model_id = provider.primary_model_id();
        let merged_content = provider::converse(
            state,
            provider,
            model_id,
            &system,
            &mut messages,
            &[],
            &super::triage::NoOpExecutor,
            usage,
            llm::ConverseOptions {
                max_turns: 40,
                max_tokens: 16384,
            },
            None,
        None,
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
        .merge_branch(&msg.repo_owner, &msg.repo_name, branch, &msg.base_branch)
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

/// Try to fetch a PR template from common locations in the repo.
async fn fetch_pr_template(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base_branch: &str,
) -> Option<String> {
    let paths = [
        ".github/pull_request_template.md",
        ".github/PULL_REQUEST_TEMPLATE.md",
        "pull_request_template.md",
        "PULL_REQUEST_TEMPLATE.md",
        "docs/pull_request_template.md",
    ];
    for path in &paths {
        if let Ok(content) = github.read_file(owner, repo, path, base_branch).await {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                info!(owner, repo, path, "Found PR template");
                return Some(trimmed.to_string());
            }
        }
    }
    None
}
