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

pub struct SecurityResult {
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
    repo_instructions: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    _run_id: Option<&str>,
) -> Result<SecurityResult, Box<dyn std::error::Error + Send + Sync>> {
    // Trim repo instructions for the security pass (it only needs high-level context)
    let trimmed = if repo_instructions.len() > 2000 {
        &repo_instructions[..repo_instructions[..2000].rfind('\n').unwrap_or(2000)]
    } else {
        repo_instructions
    };
    let instructions_block = super::format_instructions_block(trimmed);
    let system = format!(
        "You are a security audit agent for the {owner}/{repo} repository. \
         You are READ-ONLY. Review the diff for OWASP Top 10 vulnerabilities, \
         supply chain risks (typosquatting, unpinned deps), and language-specific issues \
         (unsafe blocks, eval, prototype pollution, hardcoded secrets).{instructions_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let openspec_block = super::format_openspec_summary(plan);
    let prompt = format!(
        r#"Review the diff for security vulnerabilities.
{openspec}
Use `get_diff` to see all changes, then read specific functions if you need surrounding context.

Focus your analysis on the attack surfaces introduced by the changes described in the OpenSpec above.
Check that the implementation doesn't introduce vulnerabilities that conflict with the intended scope.

Output format:
- If no security issues: start with `SECURITY_PASS`
- If issues found: start with `SECURITY_FAIL` followed by:

### [CRITICAL/HIGH/MEDIUM/LOW] — Issue Title
- **File:** path/to/file:line
- **Category:** OWASP category
- **Remediation:** How to fix it"#,
        openspec = openspec_block,
    );

    let tools = security_tools();
    let executor = SecurityToolExecutor {
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
    let final_text = provider::converse(
        state,
        provider,
        model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 10,
            max_tokens: 4096,
        },
        None,
        None,
    )
    .await?;

    let passed = final_text.contains("SECURITY_PASS");

    info!(passed, "Security audit complete");

    Ok(SecurityResult {
        passed,
        summary: final_text,
    })
}

fn security_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "get_diff".to_string(),
            description: "Compare the current branch to main.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from the repository.".to_string(),
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
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line (inclusive)"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code in the repository by keyword or symbol.".to_string(),
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

struct SecurityToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
    base_branch: &'a str,
    file_cache: &'a FileCache,
}

#[async_trait::async_trait]
impl ToolExecutor for SecurityToolExecutor<'_> {
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
                            let truncated = if patch.len() > 4000 {
                                format!(
                                    "{}... (truncated, use read_file for full content)",
                                    &patch[..4000]
                                )
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
                // Try slicing from cached full file first
                let cache_key = format!("{}:{}", self.branch, path);
                let content = if let Some(cached) = self.file_cache.get(&cache_key).await {
                    cached
                        .lines()
                        .skip(start.saturating_sub(1))
                        .take(end.saturating_sub(start.saturating_sub(1)))
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    self.github
                        .read_file_lines(self.owner, self.repo, path, self.branch, start, end)
                        .await?
                };
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
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
