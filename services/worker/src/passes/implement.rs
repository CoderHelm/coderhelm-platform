use serde_json::json;
use std::collections::HashSet;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::WorkerState;

pub struct ImplementResult {
    pub files_modified: Vec<String>,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    plan: &PlanResult,
    branch: &str,
    usage: &mut TokenUsage,
) -> Result<ImplementResult, Box<dyn std::error::Error + Send + Sync>> {
    // Create the working branch from main
    github
        .create_branch(&msg.repo_owner, &msg.repo_name, branch, "main")
        .await?;
    info!(branch, "Created working branch");

    let system = format!(
        "You are an implementation agent for the {owner}/{repo} repository. \
         Implement each task from the checklist. Follow existing code patterns exactly.",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"Implement the following tasks for issue #{number}: {title}

## Tasks
{tasks}

## Design
{design}

## Instructions
- Implement each unchecked task (`- [ ]`) one at a time, in order.
- For each task: read the relevant files, understand the pattern, write the code.
- Use `batch_write` for atomic multi-file changes when a task touches multiple files.
- Follow existing code patterns exactly (imports, naming, structure, test style).
- After implementing all tasks, output a summary of what was done.
- Only implement the listed tasks. Do not add extras."#,
        number = msg.issue_number,
        title = msg.title,
        tasks = plan.tasks,
        design = plan.design,
    );

    let tools = all_tools();
    let executor = WriteToolExecutor {
        github,
        owner: msg.repo_owner.clone(),
        repo: msg.repo_name.clone(),
        branch: branch.to_string(),
        files_modified: std::sync::Mutex::new(HashSet::new()),
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    llm::converse(state, &system, &mut messages, &tools, &executor, usage).await?;

    let files = executor
        .files_modified
        .lock()
        .unwrap()
        .iter()
        .cloned()
        .collect();

    Ok(ImplementResult {
        files_modified: files,
    })
}

// ─── Full tool set for implement pass ───────────────────────

fn all_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_tree".to_string(),
            description: "Get the full recursive file tree.".to_string(),
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
            name: "write_file".to_string(),
            description: "Create or update a single file. Include sha for updates.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string", "description": "Full file content"},
                    "message": {"type": "string", "description": "Commit message"},
                    "sha": {"type": "string", "description": "SHA of existing file (required for updates)"}
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
                    "message": {"type": "string", "description": "Commit message"},
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
        ToolDefinition {
            name: "get_diff".to_string(),
            description: "Compare the current branch to main.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
    ]
}

struct WriteToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: String,
    repo: String,
    branch: String,
    files_modified: std::sync::Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for WriteToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        match name {
            "read_tree" => {
                let tree = self
                    .github
                    .get_tree(&self.owner, &self.repo, &self.branch)
                    .await?;
                let paths: Vec<&str> = tree
                    .iter()
                    .filter(|e| e.entry_type == "blob")
                    .map(|e| e.path.as_str())
                    .collect();
                Ok(json!(paths.join("\n")))
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let content = self
                    .github
                    .read_file(&self.owner, &self.repo, path, &self.branch)
                    .await?;
                Ok(json!(content))
            }
            "list_directory" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let entries = self
                    .github
                    .list_directory(&self.owner, &self.repo, path, &self.branch)
                    .await?;
                let lines: Vec<String> = entries
                    .iter()
                    .map(|e| format!("{}: {}", e.entry_type, e.name))
                    .collect();
                Ok(json!(lines.join("\n")))
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
                        &self.owner,
                        &self.repo,
                        path,
                        content,
                        &self.branch,
                        message,
                        sha,
                    )
                    .await?;
                self.files_modified.lock().unwrap().insert(path.to_string());
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
                        ops.push(FileOp::Delete {
                            path: path.to_string(),
                        });
                    } else {
                        let content = f.get("content").and_then(|v| v.as_str()).unwrap_or("");
                        ops.push(FileOp::Write {
                            path: path.to_string(),
                            content: content.to_string(),
                        });
                    }
                    self.files_modified.lock().unwrap().insert(path.to_string());
                }
                let sha = self
                    .github
                    .batch_write(&self.owner, &self.repo, &self.branch, message, &ops)
                    .await?;
                Ok(json!(format!(
                    "Batch commit {} — {} files",
                    &sha[..8],
                    ops.len()
                )))
            }
            "get_diff" => {
                let diff = self
                    .github
                    .get_diff(&self.owner, &self.repo, "main", &self.branch)
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
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
