use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

/// Structured result from the review pass.
pub struct ReviewResult {
    pub passed: bool,
    pub summary: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    plan: &PlanResult,
    branch: &str,
    rules: &[String],
    repo_instructions: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
) -> Result<ReviewResult, Box<dyn std::error::Error + Send + Sync>> {
    let rules_block = super::format_rules_block(rules);
    let instructions_block = super::format_instructions_block(repo_instructions);
    let system = format!(
        "You are a code review agent for the {owner}/{repo} repository. \
         Review the diff against the OpenSpec to verify correctness and completeness. \
         You are READ-ONLY — you cannot modify files. Report issues for the implementation agent to fix.{rules_block}{instructions_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let openspec_block = super::format_openspec_summary(plan);
    let prompt = format!(
        r#"Review the implementation for issue #{number}: {title}
{openspec}
Use the `get_diff` tool to see all changes compared to main. Then review for:

1. **Task completeness** — Verify every task in the Tasks section is addressed by the diff. Flag any missed tasks.
2. **Design adherence** — Does the implementation follow the patterns and approach described in the Design section?
3. **Acceptance criteria** — Are the Given/When/Then scenarios in the Acceptance Criteria satisfied?
4. **Scope compliance** — Does the diff stay within the Proposal's scope boundaries? Flag anything that goes beyond scope.
5. **Correctness** — Any logic errors, null checks, off-by-one, missing error handling, typos?
6. **Convention compliance** — Does it follow the repo's patterns (naming, imports, structure)?
7. **Must-rules** — If must-rules are listed in the system prompt, verify every rule is respected.
8. **No placeholders** — Flag any TODO, FIXME, REPLACE_ME, stub implementations, or incomplete code left behind.
9. **No destructive deletions** — If large blocks of existing code were REMOVED without being replaced by equivalent functionality, flag it. The agent should modify code, not gut it.
10. **Syntax integrity** — For every modified file, verify that braces/brackets are balanced and the code would parse. If a file has orphaned catch blocks, dangling braces, or unreachable code after return/throw, it MUST be flagged.

After using `get_diff`, also use `read_file` on any file that had significant changes (>20 lines modified) to verify the full file is syntactically correct. Do NOT skip this step.

If you find issues, start with "ISSUES_FOUND:" followed by a list with file path, problem, and suggested fix.
If everything looks good, start with "LGTM" followed by a brief summary."#,
        number = msg.issue_number,
        title = msg.title,
        openspec = openspec_block,
    );

    // Read-only tool set — no write_file or batch_write
    let tools = review_tools();
    let executor = ReviewToolExecutor {
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
    let response = provider::converse(
        state,
        provider,
        model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 25,
            max_tokens: 8192,
            deadline: None,
        },
        None,
        None,
    )
    .await?;
    info!("Review result: {}", &response[..response.len().min(200)]);

    let passed = response.starts_with("LGTM") || !response.starts_with("ISSUES_FOUND:");
    Ok(ReviewResult {
        passed,
        summary: response,
    })
}

pub fn review_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "get_diff".to_string(),
            description: "Compare the current branch to main.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from the repository. Prefer read_file_lines for targeted reads.".to_string(),
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
            description: "Read specific lines from a file (1-indexed, inclusive). Much cheaper than reading the whole file.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line (inclusive)"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "list_directory".to_string(),
            description: "List contents of a directory.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path relative to repo root"}
                },
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code in the repository by keyword or symbol. Returns matching file paths and fragments.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["query"]
            }),
        },
    ]
}

pub struct ReviewToolExecutor<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
    pub base_branch: &'a str,
    pub file_cache: &'a FileCache,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for ReviewToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        match name {
            "get_diff" => {
                let diff = self
                    .github
                    .get_diff(self.owner, self.repo, self.base_branch, self.branch)
                    .await?;
                let files = diff.get("files").and_then(|v| v.as_array());
                if let Some(files) = files {
                    let mut lines = Vec::new();
                    for f in files {
                        let filename = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
                        let status = f.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let adds = f.get("additions").and_then(|v| v.as_u64()).unwrap_or(0);
                        let dels = f.get("deletions").and_then(|v| v.as_u64()).unwrap_or(0);
                        lines.push(format!("{filename} ({status}, +{adds}/-{dels})"));
                        if let Some(patch) = f.get("patch").and_then(|v| v.as_str()) {
                            let truncated = if patch.len() > 2000 {
                                format!("{}... (truncated)", &patch[..2000])
                            } else {
                                patch.to_string()
                            };
                            lines.push(truncated);
                        }
                    }
                    Ok(json!(lines.join("\n")))
                } else {
                    Ok(json!("No changes compared to main."))
                }
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let cache_key = format!("{}:{}", self.branch, path);
                let content = if let Some(cached) = self.file_cache.get(&cache_key).await {
                    cached
                } else {
                    let fetched = self
                        .github
                        .read_file(self.owner, self.repo, path, self.branch)
                        .await?;
                    self.file_cache.insert(cache_key, fetched.clone()).await;
                    fetched
                };
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
            "list_directory" => {
                let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
                let tree = self
                    .github
                    .get_tree(self.owner, self.repo, self.branch)
                    .await?;
                let entries: Vec<&str> = tree
                    .iter()
                    .filter(|e| {
                        let p = e.path.as_str();
                        if path.is_empty() {
                            !p.contains('/')
                        } else {
                            p.starts_with(&format!("{path}/")) && !p[path.len() + 1..].contains('/')
                        }
                    })
                    .map(|e| e.path.as_str())
                    .collect();
                Ok(json!(entries))
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
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
