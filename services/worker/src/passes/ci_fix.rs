use serde_json::json;
use tracing::{info, warn};

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{CiFixMessage, TokenUsage};
use crate::WorkerState;

const MAX_LOG_CHARS: usize = 30_000;

pub async fn run(
    state: &WorkerState,
    msg: CiFixMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut usage = TokenUsage::default();

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Download check run logs
    let logs = match github
        .get_check_run_logs(&msg.repo_owner, &msg.repo_name, msg.check_run_id)
        .await
    {
        Ok(l) => {
            if l.len() > MAX_LOG_CHARS {
                format!("... (truncated)\n{}", &l[l.len() - MAX_LOG_CHARS..])
            } else {
                l
            }
        }
        Err(e) => {
            warn!("Failed to download CI logs: {e}");
            "(failed to download logs)".to_string()
        }
    };

    let system = format!(
        "You are a CI fix agent for the {owner}/{repo} repository. \
         Diagnose and fix CI failures. Only fix what CI is complaining about.",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"A CI check failed on PR #{pr_number} (branch: {branch}).

## CI Logs
```
{logs}
```

## Instructions
1. Read the CI logs carefully and identify the failure.
2. Read the relevant source files to understand the issue.
3. Fix the failing code — this is usually a lint error, type error, test failure, or build error.
4. Use `batch_write` if multiple files need changes.
5. Output a brief summary of what failed and what you fixed.

Rules:
- Only fix what the CI is complaining about. Don't refactor or add features.
- If the failure is in a test, fix the code (not the test) unless the test itself is wrong."#,
        pr_number = msg.pr_number,
        branch = msg.branch,
        logs = logs,
    );

    let tools = ci_fix_tools();
    let executor = CiFixToolExecutor {
        github: &github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch: &msg.branch,
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let response =
        llm::converse(state, &system, &mut messages, &tools, &executor, &mut usage).await?;

    info!(
        run_id = %msg.run_id,
        attempt = msg.attempt,
        "CI fix complete: {}",
        &response[..response.len().min(200)]
    );

    // Update run record in runs table with usage
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "tenant_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.tenant_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression(
            "SET ci_fix_attempt = :a, tokens_in = tokens_in + :ti, \
             tokens_out = tokens_out + :to, updated_at = :t",
        )
        .expression_attribute_values(
            ":a",
            aws_sdk_dynamodb::types::AttributeValue::N(msg.attempt.to_string()),
        )
        .expression_attribute_values(
            ":ti",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.input_tokens.to_string()),
        )
        .expression_attribute_values(
            ":to",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.output_tokens.to_string()),
        )
        .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
        .send()
        .await?;

    Ok(())
}

fn ci_fix_tools() -> Vec<ToolDefinition> {
    vec![
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

struct CiFixToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for CiFixToolExecutor<'a> {
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
