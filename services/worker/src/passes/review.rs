use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    rules: &[String],
    usage: &mut TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rules_block = super::format_rules_block(rules);
    let system = format!(
        "You are a code review agent for the {owner}/{repo} repository. \
         Review the diff for correctness, completeness, conventions, bugs, and security.{rules_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"Review the implementation for issue #{number}: {title}

Use the `get_diff` tool to see all changes compared to main. Then review for:

1. **Correctness** — Does the code do what the issue asks? Any logic errors?
2. **Completeness** — Are all tasks implemented? Any missing pieces?
3. **Convention compliance** — Does it follow the repo's patterns (naming, imports, structure)?
4. **Obvious bugs** — Null checks, off-by-one, missing error handling, typos?
5. **Security** — Any injection risks, exposed secrets, unsafe operations?
6. **Must-rules** — If must-rules are listed in the system prompt, verify every rule is respected.

If you find issues:
- Fix them using `write_file` or `batch_write`

If everything looks good, say "LGTM — no issues found."

Output a brief summary of what you reviewed and any fixes applied."#,
        number = msg.issue_number,
        title = msg.title,
    );

    // Reuse the same tool set as implement (read + write + diff)
    let tools = review_tools();
    let executor = ReviewToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch,
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let response = llm::converse(
        state,
        &state.config.model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
    )
    .await?;
    info!("Review result: {}", &response[..response.len().min(200)]);
    Ok(())
}

fn review_tools() -> Vec<ToolDefinition> {
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

struct ReviewToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
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
                    .get_diff(self.owner, self.repo, "main", self.branch)
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
