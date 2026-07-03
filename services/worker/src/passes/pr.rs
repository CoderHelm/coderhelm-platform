use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::agent::{llm, provider};
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::syntax_check;
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
    let pr_template =
        fetch_pr_template(github, &msg.repo_owner, &msg.repo_name, &msg.base_branch).await;

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

    let diff_patches = format_diff_patches(&diff);
    let prompt = format!(
        r#"Write a concise pull request description for ticket {ticket_ref}: {title}

## Plan (context — what was INTENDED)
{summary}

## Files Changed
{diff_summary}

## Actual diff excerpts (the source of truth — describe THIS)
{diff_patches}{template_block}

## Instructions
Describe what the DIFF actually does. Where the diff differs from the plan, the diff wins.
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
            deadline: None,
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
        // char-boundary-safe: String::truncate panics mid-codepoint, and
        // ticket titles routinely contain em-dashes/emoji.
        title = format!("{}...", common::truncate_str(&title, 69));
    }

    // Check if a PR already exists for this branch (e.g. from a re-run or retry)
    let existing_pr = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;

    let (pr_number, pr_url, node_id) = if let Some(pr_data) = existing_pr {
        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info!(pr_number = number, pr_url = %url, "Using existing PR");
        (number, url, nid)
    } else if let Some(pr_data) = reopen_prior_pr(github, msg, branch).await {
        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info!(pr_number = number, pr_url = %url, "Reopened prior PR for branch");
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

        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
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

/// Reopen the branch's prior closed-unmerged PR, if any. A ticket-update
/// re-run force-resets the branch to base, which makes GitHub auto-close the
/// PR ("all commits removed"); once the fresh implementation has pushed new
/// commits, reopening keeps the PR number and its review history instead of
/// creating a new PR every re-run. Failures fall back to PR creation.
async fn reopen_prior_pr(
    github: &GitHubClient,
    msg: &TicketMessage,
    branch: &str,
) -> Option<serde_json::Value> {
    let prior = github
        .find_reopenable_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await
        .ok()
        .flatten()?;
    let number = prior.get("number").and_then(|v| v.as_u64())?;
    match github
        .reopen_pull_request(&msg.repo_owner, &msg.repo_name, number)
        .await
    {
        Ok(()) => Some(prior),
        Err(e) => {
            warn!(pr_number = number, error = %e, "Could not reopen prior PR; creating a new one");
            None
        }
    }
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

    info!(branch, "Merge conflicts detected, resolving with LLM");

    // Get SHAs for the merge commit parents
    let branch_sha = github
        .get_ref(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let base_sha = github
        .get_ref(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await?;

    // Use the compare API to find the merge base and files changed on each side
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;

    let merge_base_sha = compare
        .pointer("/merge_base_commit/sha")
        .and_then(|v| v.as_str())
        .unwrap_or(&base_sha);

    // Get files changed on the BASE side (merge_base → base_branch)
    let base_diff = github
        .get_diff(
            &msg.repo_owner,
            &msg.repo_name,
            merge_base_sha,
            &msg.base_branch,
        )
        .await?;
    let base_changed: std::collections::HashSet<String> = base_diff
        .get("files")
        .and_then(|v| v.as_array())
        .map(|files| {
            files
                .iter()
                .filter_map(|f| f.get("filename").and_then(|v| v.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Get files changed on the BRANCH side (merge_base → branch)
    let branch_diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, merge_base_sha, branch)
        .await?;
    let branch_files = branch_diff
        .get("files")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Only files changed on BOTH sides are actual conflicts
    let conflicting: Vec<&serde_json::Value> = branch_files
        .iter()
        .filter(|f| {
            f.get("filename")
                .and_then(|v| v.as_str())
                .map(|name| base_changed.contains(name))
                .unwrap_or(false)
        })
        .collect();

    info!(
        branch,
        total_branch_files = branch_files.len(),
        base_changed = base_changed.len(),
        conflicting = conflicting.len(),
        "Identified conflicting files (changed on both sides)"
    );

    let mut resolved_files: Vec<FileOp> = Vec::new();

    // For files only changed on base (not touched by branch), take the base version
    for base_file in &base_changed {
        let touched_by_branch = branch_files
            .iter()
            .any(|f| f.get("filename").and_then(|v| v.as_str()) == Some(base_file));
        if !touched_by_branch {
            match github
                .read_file(&msg.repo_owner, &msg.repo_name, base_file, &msg.base_branch)
                .await
            {
                Ok(content) => {
                    resolved_files.push(FileOp::Write {
                        path: base_file.clone(),
                        content,
                    });
                }
                Err(_) => {
                    // File was deleted on base
                    resolved_files.push(FileOp::Delete {
                        path: base_file.clone(),
                    });
                }
            }
        }
    }

    // For files changed on BOTH sides, do 3-way merge with LLM
    for file in &conflicting {
        let path = match file.get("filename").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => continue,
        };

        // Read all three versions: merge base, base branch (main), feature branch
        let ancestor = github
            .read_file(&msg.repo_owner, &msg.repo_name, path, merge_base_sha)
            .await
            .ok();
        let main_content = github
            .read_file(&msg.repo_owner, &msg.repo_name, path, &msg.base_branch)
            .await
            .ok();
        let branch_content = github
            .read_file(&msg.repo_owner, &msg.repo_name, path, branch)
            .await
            .ok();

        // If either side deleted the file, prefer branch's decision
        let (main_content, branch_content) = match (main_content, branch_content) {
            (Some(m), Some(b)) if m == b => continue, // no actual conflict
            (Some(m), Some(b)) => (m, b),
            (None, Some(b)) => {
                // Main deleted, branch still has it — keep branch version
                resolved_files.push(FileOp::Write {
                    path: path.to_string(),
                    content: b,
                });
                continue;
            }
            (Some(_), None) => {
                // Branch deleted, main still has it — keep branch's deletion
                resolved_files.push(FileOp::Delete {
                    path: path.to_string(),
                });
                continue;
            }
            (None, None) => continue,
        };

        let cap = |s: &str, limit: usize| -> String {
            if s.len() > limit {
                let head = common::truncate_str(s, limit);
                let cut = head.rfind('\n').unwrap_or(head.len());
                format!("{}... (truncated, {} bytes total)", &head[..cut], s.len())
            } else {
                s.to_string()
            }
        };

        // Build a 3-way merge prompt
        let system = "You are a precise merge conflict resolver. You will be given three versions of a file: the common ancestor, the main branch version, and the feature branch version. Produce the correctly merged file that incorporates changes from BOTH sides. When changes conflict directly, prefer the feature branch version. Return ONLY the file content — no explanations, no markdown fences, no commentary.".to_string();

        let mut prompt = format!("Merge these versions of `{path}`.\n\n");

        if let Some(ref anc) = ancestor {
            prompt.push_str(&format!(
                "## Common ancestor\n```\n{}\n```\n\n",
                cap(anc, 12_000)
            ));
        }

        prompt.push_str(&format!(
            "## main branch version (their changes)\n```\n{}\n```\n\n",
            cap(&main_content, 16_000)
        ));
        prompt.push_str(&format!(
            "## feature branch version (our changes — prefer when conflicting)\n```\n{}\n```\n\n",
            cap(&branch_content, 16_000)
        ));

        if ancestor.is_some() {
            prompt.push_str(
                "Use the common ancestor to understand what each side changed. \
                 Include changes from BOTH sides. Only prefer the feature branch \
                 when both sides modified the exact same lines.",
            );
        } else {
            prompt.push_str(
                "Include changes from BOTH sides. Only prefer the feature branch \
                 when both sides modified the exact same lines.",
            );
        }

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
                deadline: None,
            },
            None,
            None,
        )
        .await?;

        // Validate the model's merge before it becomes a commit — the output
        // used to be committed verbatim (dropped hunks, stray fences,
        // truncation all landed as file corruption). Any invalid file aborts
        // the whole resolution: an unresolved conflict GitHub can display is
        // strictly safer than a silently corrupted merge.
        let merged_content = strip_code_fences(&merged_content);
        let min_side = branch_content.len().min(main_content.len());
        if merged_content.trim().is_empty() && min_side > 0 {
            warn!(
                path,
                "LLM merge produced empty output — aborting conflict resolution"
            );
            return Ok(false);
        }
        if min_side > 200 && merged_content.len() < min_side * 3 / 10 {
            warn!(
                path,
                merged = merged_content.len(),
                min_side,
                "LLM merge dropped most of the file — aborting conflict resolution"
            );
            return Ok(false);
        }
        if let Err(problem) =
            syntax_check::validate_change(path, Some(&branch_content), &merged_content)
        {
            warn!(
                path,
                problem, "LLM merge is syntactically broken — aborting conflict resolution"
            );
            return Ok(false);
        }

        resolved_files.push(FileOp::Write {
            path: path.to_string(),
            content: merged_content,
        });
    }

    if resolved_files.is_empty() && !conflicting.is_empty() {
        warn!(
            branch,
            "Conflict detected but no files to resolve — all files matched"
        );
        // Try the merge one more time in case it was transient
        let retry = github
            .merge_branch(&msg.repo_owner, &msg.repo_name, branch, &msg.base_branch)
            .await?;
        return Ok(retry);
    }

    info!(
        branch,
        resolved = resolved_files.len(),
        llm_resolved = conflicting.len(),
        auto_resolved = resolved_files.len().saturating_sub(conflicting.len()),
        "Creating merge commit"
    );

    // Create a proper merge commit with TWO parents — this tells Git the merge is done
    github
        .create_merge_commit(
            &msg.repo_owner,
            &msg.repo_name,
            branch,
            &branch_sha,
            &base_sha,
            &format!("Merge {} into {}", &msg.base_branch, branch),
            &resolved_files,
        )
        .await?;

    Ok(true)
}

/// Strip a wrapping markdown code fence the model may have added despite
/// instructions ("```lang\n…\n```"). Inner fences are left untouched.
fn strip_code_fences(s: &str) -> String {
    let trimmed = s.trim();
    // Only strip a WRAPPING fence: starts with ``` AND ends with ```.
    // A file that legitimately begins with a fence (markdown docs) but has
    // content after its last fence must pass through untouched.
    if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            let after_lang = inner.find('\n').map(|i| i + 1).unwrap_or(0);
            return inner[after_lang..].trim_end().to_string() + "\n";
        }
    }
    s.to_string()
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

/// Real patch excerpts for the PR body writer. It previously saw only the
/// plan and a filename list — when implement deviated from plan, the PR
/// description confidently described a change that wasn't in the diff.
/// Caps: per-file and total, char-boundary-safe.
fn format_diff_patches(diff: &serde_json::Value) -> String {
    const PER_FILE: usize = 1200;
    const TOTAL: usize = 12_000;
    let files = match diff.get("files").and_then(|v| v.as_array()) {
        Some(f) => f,
        None => return String::new(),
    };
    let mut out = String::new();
    for f in files {
        if out.len() >= TOTAL {
            out.push_str("\n[… more files omitted …]\n");
            break;
        }
        let path = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let Some(patch) = f.get("patch").and_then(|v| v.as_str()) else {
            continue; // binary or too-large-for-API files have no patch
        };
        let excerpt = common::truncate_str(patch, PER_FILE);
        let marker = if excerpt.len() < patch.len() {
            "\n[… truncated …]"
        } else {
            ""
        };
        out.push_str(&format!("\n### {path}\n```diff\n{excerpt}{marker}\n```\n"));
    }
    out
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
