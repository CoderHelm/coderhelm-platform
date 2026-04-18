use serde_json::json;
use std::collections::HashSet;
use tracing::warn;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::mcp;
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

pub struct ImplementResult {
    pub files_modified: Vec<String>,
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
    review_feedback: Option<&str>,
    complexity: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
) -> Result<ImplementResult, Box<dyn std::error::Error + Send + Sync>> {
    let rules_block = super::format_rules_block(rules);
    // Trim repo instructions for simple issues to reduce per-turn token cost
    let trimmed_instructions = if complexity == "simple" && repo_instructions.len() > 2000 {
        &repo_instructions[..repo_instructions[..2000].rfind('\n').unwrap_or(2000)]
    } else {
        repo_instructions
    };
    let instructions_block = super::format_instructions_block(trimmed_instructions);
    let system = format!(
        "You are an implementation agent for the {owner}/{repo} repository. \
         Implement each task from the checklist. Follow existing code patterns exactly.{rules_block}{instructions_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let feedback_section = match review_feedback {
        Some(fb) => format!(
            "\n\n## Review Feedback (from previous cycle)\nThe reviewer found issues with the previous implementation. Fix ALL of the following:\n{fb}"
        ),
        None => String::new(),
    };

    // Extract file paths mentioned in backticks from plan tasks
    let file_paths = extract_file_paths(&plan.tasks);
    let files_hint = if !file_paths.is_empty() && complexity == "simple" {
        format!(
            "\n\n## Target Files\nThe plan references these files — go directly to them:\n{}",
            file_paths
                .iter()
                .map(|p| format!("- `{p}`"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    } else {
        String::new()
    };

    // For simple issues, only send tasks (not full proposal/design/spec)
    let openspec_block = if complexity == "simple" {
        let mut block = String::from("\n\n## Tasks\n");
        block.push_str(&plan.tasks);
        block
    } else {
        super::format_openspec_block(plan)
    };

    let prompt = if complexity == "simple" {
        format!(
            r#"Implement for issue #{number}: {title}
{openspec}{files_hint}{feedback}

## Instructions — SIMPLE CHANGE
Go DIRECTLY to the target files listed in the OpenSpec.
- Use `read_file_lines` on the exact section, make the change with `edit_file`, done.
- Use `edit_file` for modifying existing files (search/replace). Only use `write_file` for new files.
- You should need 2-5 tool calls total.
- Only implement the listed tasks. Do not add extras.
- After implementing, output a one-line summary."#,
            number = msg.issue_number,
            title = msg.title,
            openspec = openspec_block,
            files_hint = files_hint,
            feedback = feedback_section,
        )
    } else {
        format!(
            r#"Implement the following tasks for issue #{number}: {title}
{openspec}{feedback}

## Instructions
- Use `search_code` to find exact files and lines before reading. Prefer `read_file_lines` over `read_file`.
- Use `edit_file` for modifying existing files (search/replace edits). Only use `write_file` for creating new files.
- Implement each unchecked task (`- [ ]`) in the Tasks section, one at a time, in order.
- Refer to the Design section for which files to modify and patterns to follow.
- Validate your changes against the Acceptance Criteria.
- Use `batch_write` for atomic multi-file changes (new files or when edit_file won't work).
- Follow existing code patterns exactly (imports, naming, structure, test style).
- After implementing all tasks, output a summary.
- Only implement the listed tasks. Do not add extras."#,
            number = msg.issue_number,
            title = msg.title,
            openspec = openspec_block,
            feedback = feedback_section,
        )
    };

    let mut tools = if complexity == "simple" {
        // Simple issues: no read_tree or list_directory — plan already says which files to edit
        all_tools()
            .into_iter()
            .filter(|t| t.name != "read_tree" && t.name != "list_directory")
            .collect()
    } else {
        all_tools()
    };

    // Only load MCP plugins if the issue body contains URLs or external references
    let has_external_refs = msg.body.contains("http://")
        || msg.body.contains("https://")
        || msg.body.contains("notion.so")
        || msg.body.contains("jira")
        || msg.body.contains("confluence");

    // Load MCP plugins and their cached tool schemas
    let mut loaded_mcp_plugins = Vec::new();
    if has_external_refs {
        let mcp_table = if state.config.mcp_configs_table_name.is_empty() {
            &state.config.settings_table_name
        } else {
            &state.config.mcp_configs_table_name
        };
        let mcp_plugins =
            mcp::load_team_plugins(&state.dynamo, mcp_table, &msg.team_id, &super::MCP_CATALOG).await;

        for plugin in &mcp_plugins {
            let schemas = match mcp::load_tool_cache(
                &state.s3,
                &state.config.bucket_name,
                &plugin.server_id,
            )
            .await
            {
                Some(cache) => cache.tools,
                None if !state.config.mcp_proxy_function_name.is_empty() => {
                    match mcp::list_tools(&state.lambda, &state.config.mcp_proxy_function_name, plugin)
                        .await
                    {
                        Ok(schemas) => schemas,
                        Err(e) => {
                            tracing::warn!(server_id = %plugin.server_id, error = %e, "Failed to list MCP tools, skipping");
                            continue;
                        }
                    }
                }
                _ => continue,
            };

            let mcp_tool_defs = mcp::to_tool_definitions(&plugin.server_id, &schemas);
            tools.extend(mcp_tool_defs);
            loaded_mcp_plugins.push(plugin.clone());
        }
    }

    // Add MCP context to system prompt
    let mut full_system = system.clone();
    if !loaded_mcp_plugins.is_empty() {
        let plugin_lines: Vec<String> = loaded_mcp_plugins
            .iter()
            .map(|p| format!("- {}", p.server_id))
            .collect();
        full_system.push_str(&format!(
            "\n\nYou have tool-call access to these MCP servers. \
             Only use them if the issue or plan explicitly references URLs or external resources:\n{}",
            plugin_lines.join("\n")
        ));
    }
    for plugin in &loaded_mcp_plugins {
        if let Some(ref prompt) = plugin.custom_prompt {
            full_system.push_str(&format!(
                "\n\n## MCP Server: {}\n{}",
                plugin.server_id, prompt
            ));
        }
    }

    let tasks_key = format!(
        "teams/{}/runs/{}/openspec/tasks.md",
        msg.team_id,
        msg.ticket_id.to_lowercase()
    );
    let task_tracker = TaskTracker::new(
        &state.s3,
        &state.config.bucket_name,
        &tasks_key,
        &plan.tasks,
    );
    let executor = CombinedWriteToolExecutor {
        write_executor: WriteToolExecutor {
            github,
            owner: msg.repo_owner.clone(),
            repo: msg.repo_name.clone(),
            branch: branch.to_string(),
            base_branch: msg.base_branch.clone(),
            files_modified: std::sync::Mutex::new(HashSet::new()),
            task_tracker: &task_tracker,
            file_cache,
        },
        mcp_plugins: &loaded_mcp_plugins,
        lambda: &state.lambda,
        mcp_proxy_function_name: &state.config.mcp_proxy_function_name,
    };

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    // Route simple issues to primary (Sonnet), medium/complex to heavy (Opus)
    let model_id = match complexity {
        "simple" => provider.primary_model_id(),
        _ => provider.heavy_model_id(),
    };
    let opts = llm::ConverseOptions {
        max_turns: match complexity {
            "simple" => 25,
            "medium" => 35,
            _ => 50,
        },
        max_tokens: 16384,
    };

    provider::converse(
        state,
        provider,
        model_id,
        &full_system,
        &mut messages,
        &tools,
        &executor,
        usage,
        opts,
    )
    .await?;

    let files = executor
        .write_executor
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
            description: "Get the full recursive file tree. Returns all file paths. Call once, then use list_directory for subdirs.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from the repository. Large files are truncated to ~32KB. Prefer read_file_lines for targeted reads.".to_string(),
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
            description: "Read specific lines from a file (1-indexed, inclusive). Use this instead of read_file when you know which lines you need — much cheaper.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line to read (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line to read (inclusive)"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code in the repository by keyword or symbol name. Returns matching file paths and text fragments. Use this to find where functions, types, or patterns are defined instead of reading many files.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (e.g. function name, error message, import path)"}
                },
                "required": ["query"]
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
            description: "Create a NEW file or fully rewrite a small file. For modifying existing files, prefer edit_file (much cheaper).".to_string(),
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
            name: "edit_file".to_string(),
            description: "Make targeted edits to an existing file using search/replace. Sends only the changed parts — use this instead of write_file for modifications. Each edit replaces one occurrence of old_text with new_text.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "edits": {
                        "type": "array",
                        "description": "Array of search/replace edits to apply in order",
                        "items": {
                            "type": "object",
                            "properties": {
                                "old_text": {"type": "string", "description": "Exact text to find (must match uniquely)"},
                                "new_text": {"type": "string", "description": "Replacement text"}
                            },
                            "required": ["old_text", "new_text"]
                        }
                    },
                    "message": {"type": "string", "description": "Commit message"}
                },
                "required": ["path", "edits", "message"]
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

/// Tracks task completion in S3 by matching written file paths against checklist items.
struct TaskTracker {
    s3: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    tasks_md: std::sync::Mutex<String>,
}

impl TaskTracker {
    fn new(s3: &aws_sdk_s3::Client, bucket: &str, key: &str, tasks_md: &str) -> Self {
        Self {
            s3: s3.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            tasks_md: std::sync::Mutex::new(tasks_md.to_string()),
        }
    }

    /// Check off tasks whose descriptions mention any of the given file paths, then write to S3.
    async fn mark_files_done(&self, paths: &[&str]) {
        let updated = {
            let mut md = self.tasks_md.lock().unwrap();
            let mut changed = false;
            let mut lines: Vec<String> = md.lines().map(|l| l.to_string()).collect();
            for line in &mut lines {
                if !line.contains("- [ ]") {
                    continue;
                }
                let matches = paths.iter().any(|p| {
                    let filename = p.rsplit('/').next().unwrap_or(p);
                    line.contains(p) || line.contains(filename)
                });
                if matches {
                    *line = line.replacen("- [ ]", "- [x]", 1);
                    changed = true;
                }
            }
            if !changed {
                return;
            }
            let new_md = lines.join("\n");
            *md = new_md.clone();
            new_md
        };
        if let Err(e) = self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(updated.as_bytes().to_vec().into())
            .content_type("text/markdown")
            .send()
            .await
        {
            warn!(key = %self.key, error = %e, "Failed to update tasks.md incrementally");
        }
    }
}

/// Paths the bot must never write to (require elevated GitHub App permissions).
fn is_protected_path(path: &str) -> bool {
    let normalized = path.trim_start_matches('/');
    normalized.starts_with(".github/workflows/") || normalized.starts_with(".github/actions/")
}

struct WriteToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: String,
    repo: String,
    branch: String,
    base_branch: String,
    files_modified: std::sync::Mutex<HashSet<String>>,
    task_tracker: &'a TaskTracker,
    file_cache: &'a FileCache,
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
                Ok(json!(super::truncate_tree(&paths)))
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
                        .read_file(&self.owner, &self.repo, path, &self.branch)
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
                    .read_file_lines(&self.owner, &self.repo, path, &self.branch, start, end)
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
                    .search_code(&self.owner, &self.repo, query)
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
                if is_protected_path(path) {
                    return Ok(json!(format!(
                        "Cannot modify {path}: CI/CD workflow files are protected. \
                         Skip this file and continue with other changes."
                    )));
                }
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
                self.file_cache
                    .remove(&format!("{}:{}", self.branch, path))
                    .await;
                self.task_tracker.mark_files_done(&[path]).await;
                Ok(json!(format!("Wrote {path}")))
            }
            "edit_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                if is_protected_path(path) {
                    return Ok(json!(format!(
                        "Cannot modify {path}: CI/CD workflow files are protected."
                    )));
                }
                let edits = input
                    .get("edits")
                    .and_then(|v| v.as_array())
                    .ok_or("Missing edits array")?;
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;

                // Fetch current file content
                let cache_key = format!("{}:{}", self.branch, path);
                let mut content = if let Some(cached) = self.file_cache.get(&cache_key).await {
                    cached
                } else {
                    self.github
                        .read_file(&self.owner, &self.repo, path, &self.branch)
                        .await?
                };

                // Apply edits sequentially
                let mut applied = 0;
                let mut errors = Vec::new();
                for edit in edits {
                    let old_text = edit.get("old_text").and_then(|v| v.as_str()).unwrap_or("");
                    let new_text = edit.get("new_text").and_then(|v| v.as_str()).unwrap_or("");
                    if old_text.is_empty() {
                        errors.push("Empty old_text in edit".to_string());
                        continue;
                    }
                    let count = content.matches(old_text).count();
                    if count == 0 {
                        errors.push(format!("old_text not found: {}…", &old_text[..old_text.len().min(60)]));
                    } else if count > 1 {
                        errors.push(format!("old_text matched {} times (must be unique): {}…", count, &old_text[..old_text.len().min(60)]));
                    } else {
                        content = content.replacen(old_text, new_text, 1);
                        applied += 1;
                    }
                }

                if applied == 0 {
                    return Ok(json!(format!("No edits applied to {path}: {}", errors.join("; "))));
                }

                // Write the modified file back
                // Get sha for update
                let sha = self.github
                    .get_file_sha(&self.owner, &self.repo, path, &self.branch)
                    .await
                    .ok();
                self.github
                    .write_file(
                        &self.owner,
                        &self.repo,
                        path,
                        &content,
                        &self.branch,
                        message,
                        sha.as_deref(),
                    )
                    .await?;
                self.files_modified.lock().unwrap().insert(path.to_string());
                self.file_cache
                    .insert(cache_key, content)
                    .await;
                self.task_tracker.mark_files_done(&[path]).await;

                let mut result = format!("Applied {applied} edit(s) to {path}");
                if !errors.is_empty() {
                    result.push_str(&format!(". Warnings: {}", errors.join("; ")));
                }
                Ok(json!(result))
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
                let mut skipped = Vec::new();
                for f in files_arr {
                    let path = f
                        .get("path")
                        .and_then(|v| v.as_str())
                        .ok_or("Missing file path")?;
                    if is_protected_path(path) {
                        skipped.push(path.to_string());
                        continue;
                    }
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
                    self.file_cache
                        .remove(&format!("{}:{}", self.branch, path))
                        .await;
                }
                if ops.is_empty() {
                    return Ok(json!(format!(
                        "All files skipped (protected CI/CD paths): {}",
                        skipped.join(", ")
                    )));
                }
                let sha = self
                    .github
                    .batch_write(&self.owner, &self.repo, &self.branch, message, &ops)
                    .await?;
                let written_paths: Vec<&str> = ops
                    .iter()
                    .filter_map(|op| match op {
                        FileOp::Write { path, .. } => Some(path.as_str()),
                        FileOp::Delete { .. } => None,
                    })
                    .collect();
                self.task_tracker.mark_files_done(&written_paths).await;
                let mut result = format!("Batch commit {} — {} files", &sha[..8], ops.len());
                if !skipped.is_empty() {
                    result.push_str(&format!(
                        ". Skipped protected files: {}",
                        skipped.join(", ")
                    ));
                }
                Ok(json!(result))
            }
            "get_diff" => {
                let diff = self
                    .github
                    .get_diff(&self.owner, &self.repo, &self.base_branch, &self.branch)
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

// ─── Combined tool executor (write + MCP) ───────────────────

struct CombinedWriteToolExecutor<'a> {
    write_executor: WriteToolExecutor<'a>,
    mcp_plugins: &'a [mcp::McpPlugin],
    lambda: &'a aws_sdk_lambda::Client,
    mcp_proxy_function_name: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for CombinedWriteToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        // Check if this is an MCP tool (prefixed with server_id__)
        if let Some((server_id, tool_name)) = name.split_once("__") {
            let plugin = self
                .mcp_plugins
                .iter()
                .find(|p| p.server_id == server_id)
                .ok_or_else(|| format!("No MCP plugin found for server: {server_id}"))?;

            return mcp::call_tool(
                self.lambda,
                self.mcp_proxy_function_name,
                plugin,
                tool_name,
                input,
            )
            .await;
        }

        // Fall through to write tools
        self.write_executor.execute(name, input).await
    }
}

/// Extract file paths from backtick-quoted strings in plan tasks.
/// Matches patterns like `path/to/file.ext` that look like file paths.
fn extract_file_paths(tasks: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in tasks.split('`').enumerate() {
        // Odd indices are inside backticks
        if cap.0 % 2 == 1 {
            let candidate = cap.1.trim();
            // Must contain a slash or dot-extension, and no spaces
            if !candidate.is_empty()
                && !candidate.contains(' ')
                && (candidate.contains('/') || candidate.contains('.'))
                && candidate.contains('.')
                && !candidate.starts_with('-')
                && !candidate.starts_with("http")
                && seen.insert(candidate.to_string())
            {
                paths.push(candidate.to_string());
            }
        }
    }
    paths
}
