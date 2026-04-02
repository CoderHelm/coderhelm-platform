use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{FeedbackMessage, ReviewComment, TokenUsage};
use crate::WorkerState;

pub async fn run(
    state: &WorkerState,
    msg: FeedbackMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut usage = TokenUsage::default();

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // If the gateway didn't send inline comments (PR review submitted event),
    // fetch them from the GitHub API using the review_id.
    // When review_id == 0 (re-review), fetch ALL non-bot comments and let
    // filter_unanswered determine which still need a reply.
    let comments = if msg.comments.is_empty() && msg.review_id > 0 {
        fetch_review_comments(&github, &msg).await
    } else if msg.comments.is_empty() && msg.review_id == 0 {
        fetch_all_human_comments(&github, &msg).await
    } else {
        msg.comments.clone()
    };

    // Filter out comments the bot already replied to
    let comments = filter_unanswered(&github, &msg, comments).await;
    if comments.is_empty() && msg.review_body.is_empty() {
        info!(run_id = %msg.run_id, "All comments already answered — skipping feedback");

        // Reset status back to completed so the run doesn't stay stuck at "running"
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key(
                "run_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
            )
            .update_expression("SET #s = :s, status_run_id = :sri, updated_at = :t")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(
                ":s",
                aws_sdk_dynamodb::types::AttributeValue::S("completed".to_string()),
            )
            .expression_attribute_values(
                ":sri",
                aws_sdk_dynamodb::types::AttributeValue::S(format!("completed#{}", msg.run_id)),
            )
            .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
            .send()
            .await;

        return Ok(());
    }

    let formatted = format_review_comments(&msg.review_body, &comments);

    // Load voice instructions (repo-specific falls back to global)
    let voice = {
        let repo_voice = super::load_content(
            state,
            &msg.team_id,
            &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await;
        if repo_voice.is_empty() {
            super::load_content(state, &msg.team_id, "VOICE#GLOBAL").await
        } else {
            repo_voice
        }
    };
    let voice_block = if voice.is_empty() {
        String::new()
    } else {
        format!(" Match the team's voice and tone as described below:\n{voice}")
    };

    let system = format!(
        "You are a feedback agent for the {owner}/{repo} repository. \
         You respond to reviewer comments on pull requests using tools to read and write files \
         directly to the PR branch. \
         Treat each review thread as a conversation — the reviewer may ask follow-up questions \
         or request additional changes after your reply. Be conversational and collaborative. \
         If a comment asks a question, read the relevant code and answer it clearly. \
         If a comment requests a code change, you MUST use write_file or batch_write \
         to actually commit the change — do NOT describe what changes should be made without making them. \
         After pushing the change, write a short confirmation as your reply \
         (e.g. \"Done — hardcoded the measurement ID.\"). \
         Your final text output will be posted directly as a GitHub comment — \
         write it as a natural reply to the reviewer. \
         Never include meta-commentary like 'Response to Review Comments', \
         'Comment #1', 'Now I have the full context', or any internal reasoning. \
         Just answer directly as if you are talking to the reviewer.{voice_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"A reviewer left comments on PR #{pr_number}.

## Review Comments
{comments}

## Instructions
For each comment, decide whether it is:
- **A question** (e.g. "why did you do X?", "what does this do?", "could this cause Y?") — answer it with a clear, concise explanation. Read the relevant code first if needed.
- **A change request** (e.g. "use X instead", "add error handling", "this should be Y") — read the relevant file, fix the code using write_file or batch_write, and push the change. NEVER describe a code change in text without actually making it first.

Some comments may be prefixed with `[username]:` — these are earlier messages in the same thread provided for context. Do not reply to context messages; only reply to the reviewer's latest comment in each thread. Use the context to understand what was discussed previously.

Your output will be posted directly as GitHub comments — one reply per review comment thread. Write concise, direct replies. Do NOT include:
- Headings like "Response to Review Comments" or "Comment #1"
- Meta-commentary like "Now I have the full context" or "Let me explain"
- Numbered comment references — just answer naturally
- Internal reasoning or deliberation — only post the final answer

Use proper GitHub markdown in replies: backticks for code, triple-backtick blocks for multi-line code, and keep replies short (1-3 sentences for confirmations).

If there are multiple NEW comments to reply to (not counting context messages), write a separate reply for each one in the same order they appear above. \
Separate each reply with exactly this marker on its own line:
---SPLIT---
Each section will be posted as a separate reply to the corresponding comment thread.
If there is only one comment to reply to, do NOT include ---SPLIT--- at all.

Rules:
- Address every comment. Don't skip any.
- For code changes, follow the reviewer's suggestions exactly unless they conflict with the codebase conventions.
- Don't make unrelated changes beyond what the reviewer asked for.
- When you make a code change (create, edit, or delete a file), briefly confirm what you did (e.g. "Done — deleted `SETUP.md`." or "Updated — added error handling to `handler.rs`.").
- If the reviewer asks about the spec, design, or proposal, check for openspec files under `openspec/` and update them if needed.
- Keep answers concise but helpful."#,
        pr_number = msg.pr_number,
        comments = formatted,
    );

    // Determine the PR branch — we'll use the run's known branch
    // Try to extract branch from repo metadata. For now, use a convention.
    let branch = get_pr_branch(state, &msg).await?;

    let tools = feedback_tools();
    let executor = FeedbackToolExecutor {
        github: &github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch: &branch,
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let response = llm::converse(
        state,
        &state.config.light_model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        &mut usage,
    )
    .await?;

    info!(
        run_id = %msg.run_id,
        "Feedback complete: {}",
        &response[..response.len().min(200)]
    );

    // Reply to each review comment in its own thread.
    // Split the response by ---SPLIT--- markers to get per-comment replies.
    let sections: Vec<&str> = response
        .split("---SPLIT---")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    // Only match against actionable (non-context) comments for reply posting
    let actionable_comments: Vec<&ReviewComment> = comments
        .iter()
        .filter(|c| c.comment_id.is_some() && !c.is_context)
        .collect();

    if sections.len() == actionable_comments.len() {
        // Matched — post each section to its corresponding comment thread
        for (section, comment) in sections.iter().zip(actionable_comments.iter()) {
            let reply = if section.len() > 65000 {
                format!("{}\n\n*(truncated)*", &section[..65000])
            } else {
                section.to_string()
            };
            github
                .reply_to_review_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    comment.comment_id.unwrap(),
                    &reply,
                )
                .await?;
        }
    } else {
        // Fallback — post first non-empty section (strip ---SPLIT--- markers)
        let reply_text = sections
            .first()
            .map(|s| s.to_string())
            .unwrap_or(response.clone());
        let reply = if reply_text.len() > 65000 {
            format!("{}\n\n*(truncated)*", &reply_text[..65000])
        } else {
            reply_text
        };
        let first_comment_id = actionable_comments.first().and_then(|c| c.comment_id);
        if let Some(comment_id) = first_comment_id {
            github
                .reply_to_review_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    comment_id,
                    &reply,
                )
                .await?;
        } else {
            github
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &reply)
                .await?;
        }
    }

    // Note: We intentionally do NOT auto-resolve review threads.
    // The reviewer should resolve threads themselves once they're satisfied.
    // Auto-resolving kills the conversation when the reviewer wants to follow up.
    if false {
        let node_ids: Vec<&str> = actionable_comments
            .iter()
            .filter_map(|c| c.node_id.as_deref())
            .collect();
        if !node_ids.is_empty() {
            match github
                .get_review_thread_ids(&msg.repo_owner, &msg.repo_name, msg.pr_number, &node_ids)
                .await
            {
                Ok(thread_map) => {
                    let mut resolved = std::collections::HashSet::new();
                    for thread_id in thread_map.values() {
                        if resolved.insert(thread_id.clone()) {
                            if let Err(e) = github.resolve_review_thread(thread_id).await {
                                tracing::warn!(thread_id, error = %e, "Failed to resolve thread");
                            }
                        }
                    }
                    info!(
                        run_id = %msg.run_id,
                        resolved = resolved.len(),
                        "Resolved review threads after replying"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to fetch thread IDs for resolution");
                }
            }
        }
    }

    // Update run record in runs table (including status_run_id for GSI)
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression(
            "SET tokens_in = tokens_in + :ti, tokens_out = tokens_out + :to, \
             updated_at = :t, #s = :s, current_pass = :p, status_run_id = :sri, \
             pass_history = list_append(if_not_exists(pass_history, :empty), :entry)",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(
            ":ti",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.input_tokens.to_string()),
        )
        .expression_attribute_values(
            ":to",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.output_tokens.to_string()),
        )
        .expression_attribute_values(
            ":t",
            aws_sdk_dynamodb::types::AttributeValue::S(now.clone()),
        )
        .expression_attribute_values(
            ":s",
            aws_sdk_dynamodb::types::AttributeValue::S("completed".to_string()),
        )
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S("feedback".to_string()),
        )
        .expression_attribute_values(
            ":sri",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("completed#{}", msg.run_id)),
        )
        .expression_attribute_values(
            ":entry",
            aws_sdk_dynamodb::types::AttributeValue::L(vec![
                aws_sdk_dynamodb::types::AttributeValue::M(
                    [
                        (
                            "pass".to_string(),
                            aws_sdk_dynamodb::types::AttributeValue::S("feedback".to_string()),
                        ),
                        (
                            "started_at".to_string(),
                            aws_sdk_dynamodb::types::AttributeValue::S(now),
                        ),
                    ]
                    .into(),
                ),
            ]),
        )
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
        .send()
        .await?;

    // Update analytics counters (current month + all-time)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key(
                "period",
                aws_sdk_dynamodb::types::AttributeValue::S(period.to_string()),
            )
            .update_expression("ADD total_tokens_in :ti, total_tokens_out :to")
            .expression_attribute_values(
                ":ti",
                aws_sdk_dynamodb::types::AttributeValue::N(usage.input_tokens.to_string()),
            )
            .expression_attribute_values(
                ":to",
                aws_sdk_dynamodb::types::AttributeValue::N(usage.output_tokens.to_string()),
            )
            .send()
            .await?;
    }

    // Report token overage to Stripe
    let total_tokens = usage.input_tokens + usage.output_tokens;
    crate::clients::billing::report_token_overage(state, &msg.team_id, total_tokens).await;

    Ok(())
}

/// Look up the branch name from the run record in DynamoDB.
async fn get_pr_branch(
    state: &WorkerState,
    msg: &FeedbackMessage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .send()
        .await?;

    let item = result.item().ok_or("Run record not found")?;
    let branch = item
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .ok_or("Branch not found in run record")?;
    Ok(branch.clone())
}

fn format_review_comments(review_body: &str, comments: &[ReviewComment]) -> String {
    let mut parts = Vec::new();

    if !review_body.is_empty() {
        parts.push(format!("### Review Summary\n{review_body}\n"));
    }

    if comments.is_empty() && review_body.is_empty() {
        return "(no comments)".to_string();
    }

    for (i, c) in comments.iter().enumerate() {
        let line_info = c.line.map(|l| format!(" line {l}")).unwrap_or_default();
        parts.push(format!(
            "### Comment #{} \nFile: `{}`{}\n{}\n",
            i + 1,
            c.path,
            line_info,
            c.body,
        ));
    }

    parts.join("\n")
}

/// Fetch review comments from GitHub API for a given review.
/// Also collects prior thread context (earlier comments on the same file+line)
/// so the LLM sees the full conversation, not just the latest comment.
async fn fetch_review_comments(github: &GitHubClient, msg: &FeedbackMessage) -> Vec<ReviewComment> {
    match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(api_comments) => {
            // Identify the new comments from this review
            let review_comments: Vec<&serde_json::Value> = api_comments
                .iter()
                .filter(|c| {
                    c.get("pull_request_review_id")
                        .and_then(|v| v.as_u64())
                        .map(|id| id == msg.review_id)
                        .unwrap_or(false)
                })
                .collect();

            // Collect thread keys (path + line) for the new comments
            let thread_keys: std::collections::HashSet<(String, Option<u64>)> = review_comments
                .iter()
                .map(|c| {
                    let path = c["path"].as_str().unwrap_or("").to_string();
                    let line = c["line"].as_u64();
                    (path, line)
                })
                .collect();

            // Collect new comment IDs to avoid duplicating them in context
            let new_ids: std::collections::HashSet<u64> = review_comments
                .iter()
                .filter_map(|c| c["id"].as_u64())
                .collect();

            // Find earlier comments in the same threads (context)
            let mut context: Vec<ReviewComment> = api_comments
                .iter()
                .filter(|c| {
                    let path = c["path"].as_str().unwrap_or("").to_string();
                    let line = c["line"].as_u64();
                    let id = c["id"].as_u64().unwrap_or(0);
                    thread_keys.contains(&(path, line)) && !new_ids.contains(&id)
                })
                .map(|c| {
                    let author = c["user"]["login"].as_str().unwrap_or("unknown");
                    let body = c["body"].as_str().unwrap_or("");
                    ReviewComment {
                        path: c["path"].as_str().unwrap_or("").to_string(),
                        line: c["line"].as_u64(),
                        body: format!("[{author}]: {body}"),
                        comment_id: c["id"].as_u64(),
                        node_id: c["node_id"].as_str().map(|s| s.to_string()),
                        is_context: true,
                    }
                })
                .collect();

            // Append the new review comments (these are the ones to address)
            let mut new: Vec<ReviewComment> = review_comments
                .iter()
                .map(|c| ReviewComment {
                    path: c["path"].as_str().unwrap_or("").to_string(),
                    line: c["line"].as_u64(),
                    body: c["body"].as_str().unwrap_or("").to_string(),
                    comment_id: c["id"].as_u64(),
                    node_id: c["node_id"].as_str().map(|s| s.to_string()),
                    is_context: false,
                })
                .collect();

            context.append(&mut new);
            context
        }
        Err(e) => {
            tracing::warn!("Failed to fetch review comments: {e}");
            vec![]
        }
    }
}

/// Fetch ALL non-bot review comments from a PR (used by re-review path).
/// Returns every human-authored comment; filter_unanswered will then remove
/// ones the bot has already replied to.
async fn fetch_all_human_comments(
    github: &GitHubClient,
    msg: &FeedbackMessage,
) -> Vec<ReviewComment> {
    match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(api_comments) => api_comments
            .iter()
            .filter(|c| {
                // Skip bot-authored comments
                !c["user"]["login"]
                    .as_str()
                    .map(|l| l.contains("coderhelm"))
                    .unwrap_or(false)
            })
            .map(|c| ReviewComment {
                path: c["path"].as_str().unwrap_or("").to_string(),
                line: c["line"].as_u64(),
                body: c["body"].as_str().unwrap_or("").to_string(),
                comment_id: c["id"].as_u64(),
                node_id: c["node_id"].as_str().map(|s| s.to_string()),
                is_context: false,
            })
            .collect(),
        Err(e) => {
            tracing::warn!("Failed to fetch all human comments: {e}");
            vec![]
        }
    }
}

/// Filter out comments that the bot has already replied to.
async fn filter_unanswered(
    github: &GitHubClient,
    msg: &FeedbackMessage,
    comments: Vec<ReviewComment>,
) -> Vec<ReviewComment> {
    if comments.is_empty() {
        return comments;
    }

    // Fetch all PR comments to check for existing bot replies
    let all_comments = match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(c) => c,
        Err(_) => return comments, // can't check — process all
    };

    // Collect comment IDs that the bot already replied to
    let answered_ids: std::collections::HashSet<u64> = all_comments
        .iter()
        .filter(|c| {
            c["user"]["login"]
                .as_str()
                .map(|l| l.contains("coderhelm"))
                .unwrap_or(false)
        })
        .filter_map(|c| c["in_reply_to_id"].as_u64())
        .collect();

    let before = comments.len();
    let filtered: Vec<ReviewComment> = comments
        .into_iter()
        .filter(|c| {
            c.comment_id
                .map(|id| !answered_ids.contains(&id))
                .unwrap_or(true)
        })
        .collect();

    if filtered.len() < before {
        info!(
            run_id = %msg.run_id,
            skipped = before - filtered.len(),
            remaining = filtered.len(),
            "Skipped already-answered comments"
        );
    }

    filtered
}

fn feedback_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file. Prefer read_file_lines for targeted reads.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"}
                },
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific lines from a file (1-indexed, inclusive).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "start_line": {"type": "integer"},
                    "end_line": {"type": "integer"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code by keyword. Returns matching file paths and fragments."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"}
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "read_tree".to_string(),
            description: "Get the full recursive file tree.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "write_file".to_string(),
            description: "Create or update a single file.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                    "message": {"type": "string"},
                    "sha": {"type": "string"}
                },
                "required": ["path", "content", "message"]
            }),
        },
        ToolDefinition {
            name: "batch_write".to_string(),
            description: "Atomically write multiple files in a single commit.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "files": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string"},
                                "content": {"type": "string"},
                                "action": {"type": "string", "enum": ["write", "delete"]}
                            },
                            "required": ["path"]
                        }
                    }
                },
                "required": ["message", "files"]
            }),
        },
    ]
}

struct FeedbackToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for FeedbackToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        match name {
            "read_tree" => {
                let tree = self
                    .github
                    .get_tree(self.owner, self.repo, self.branch)
                    .await?;
                let paths: Vec<&str> = tree
                    .iter()
                    .filter(|e| e.entry_type == "blob")
                    .map(|e| e.path.as_str())
                    .collect();
                Ok(json!(super::truncate_tree(&paths)))
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let content = self
                    .github
                    .read_file(self.owner, self.repo, path, self.branch)
                    .await?;
                Ok(json!(super::truncate_content(&content, path)))
            }
            "read_file_lines" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let start = input
                    .get("start_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                let end = input
                    .get("end_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(start as u64 + 100) as usize;
                let content = self
                    .github
                    .read_file_lines(self.owner, self.repo, path, self.branch, start, end)
                    .await?;
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self
                    .github
                    .search_code(self.owner, self.repo, query)
                    .await?;
                let lines: Vec<String> = results
                    .iter()
                    .map(|r| {
                        if r.matches.is_empty() {
                            r.path.clone()
                        } else {
                            format!("{}\n{}", r.path, r.matches.join("\n"))
                        }
                    })
                    .collect();
                Ok(json!(lines.join("\n---\n")))
            }
            "write_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let content = input
                    .get("content")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing content")?;
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;
                let sha = input.get("sha").and_then(|v| v.as_str());
                self.github
                    .write_file(
                        self.owner,
                        self.repo,
                        path,
                        content,
                        self.branch,
                        message,
                        sha,
                    )
                    .await?;
                Ok(json!(format!("Wrote {path}")))
            }
            "batch_write" => {
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;
                let files_arr = input
                    .get("files")
                    .and_then(|v| v.as_array())
                    .ok_or("Missing files")?;
                let mut ops = Vec::new();
                for f in files_arr {
                    let path = f
                        .get("path")
                        .and_then(|v| v.as_str())
                        .ok_or("Missing file path")?;
                    let action = f.get("action").and_then(|v| v.as_str()).unwrap_or("write");
                    if action == "delete" {
                        ops.push(crate::clients::github::FileOp::Delete {
                            path: path.to_string(),
                        });
                    } else {
                        let content = f.get("content").and_then(|v| v.as_str()).unwrap_or("");
                        ops.push(crate::clients::github::FileOp::Write {
                            path: path.to_string(),
                            content: content.to_string(),
                        });
                    }
                }
                let sha = self
                    .github
                    .batch_write(self.owner, self.repo, self.branch, message, &ops)
                    .await?;
                Ok(json!(format!(
                    "Batch commit {} — {} files",
                    &sha[..8],
                    ops.len()
                )))
            }
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
