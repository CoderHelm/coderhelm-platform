use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// Result of the plan pass: the four openspec files.
pub struct PlanResult {
    pub proposal: String,
    pub design: String,
    pub tasks: String,
    pub spec: String,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    triage: &super::triage::TriageResult,
    usage: &mut TokenUsage,
) -> Result<PlanResult, Box<dyn std::error::Error + Send + Sync>> {
    let system = format!(
        "You are a planning agent for the {owner}/{repo} repository. \
         Research the codebase using the provided tools, then generate an implementation plan.",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"Generate an implementation plan (openspec) for this issue.

## Issue
Title: {title}
Summary: {summary}

## Instructions

Research the codebase using the `read_tree`, `read_file`, and `list_directory` tools to
understand the existing patterns. Then generate four openspec files:

1. **proposal.md** — Problem statement, proposed approach, scope boundaries, risks
2. **design.md** — Technical design: which files to modify/create, patterns to follow, data flow
3. **tasks.md** — Step-by-step implementation checklist. Each task is `- [ ] description`. Tasks should be atomic and ordered.
4. **spec.md** — Acceptance criteria as Given/When/Then scenarios

After researching, output the four files using this exact format:

```proposal.md
(content)
```

```design.md
(content)
```

```tasks.md
(content)
```

```spec.md
(content)
```"#,
        title = msg.title,
        summary = triage.summary,
    );

    let tools = read_only_tools();
    let executor = ReadOnlyToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch: "main",
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
        usage,
    )
    .await?;

    let files = parse_openspec_files(&response);

    // Write openspec to S3
    let prefix = format!(
        "tenants/{}/runs/{}/openspec",
        msg.tenant_id,
        msg.ticket_id.to_lowercase()
    );
    for (name, content) in [
        ("proposal.md", &files.proposal),
        ("design.md", &files.design),
        ("tasks.md", &files.tasks),
        ("spec.md", &files.spec),
    ] {
        let key = format!("{prefix}/{name}");
        state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .body(content.as_bytes().to_vec().into())
            .content_type("text/markdown")
            .send()
            .await?;
    }

    info!("Plan openspec written to S3");
    Ok(files)
}

fn parse_openspec_files(response: &str) -> PlanResult {
    let filenames = ["proposal.md", "design.md", "tasks.md", "spec.md"];
    let mut contents: Vec<String> = Vec::new();

    for filename in &filenames {
        let marker = format!("```{filename}");
        if let Some(start) = response.find(&marker) {
            let content_start = response[start..]
                .find('\n')
                .map(|i| start + i + 1)
                .unwrap_or(start);
            let end = response[content_start..]
                .find("```")
                .map(|i| content_start + i)
                .unwrap_or(response.len());
            contents.push(response[content_start..end].trim().to_string());
        } else {
            contents.push(String::new());
        }
    }

    PlanResult {
        proposal: contents.remove(0),
        design: contents.remove(0),
        tasks: contents.remove(0),
        spec: contents.remove(0),
    }
}

// ─── Read-only tools for plan pass ──────────────────────────

fn read_only_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_tree".to_string(),
            description: "Get the full recursive file tree. Call once, then use list_directory for subdirs.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
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
            description: "Read specific lines from a file (1-indexed, inclusive). Much cheaper than reading the whole file.".to_string(),
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
            description: "Search for code in the repository by keyword or symbol. Returns matching file paths and text fragments. Use to find definitions instead of reading files.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (e.g. function name, type, import)"}
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
    ]
}

struct ReadOnlyToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for ReadOnlyToolExecutor<'a> {
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
            "list_directory" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let entries = self
                    .github
                    .list_directory(self.owner, self.repo, path, self.branch)
                    .await?;
                let lines: Vec<String> = entries
                    .iter()
                    .map(|e| format!("{}: {}", e.entry_type, e.name))
                    .collect();
                Ok(json!(lines.join("\n")))
            }
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
