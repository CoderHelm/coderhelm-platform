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
    // Fetch all enabled repos for this tenant so the planner can explore cross-repo
    let tenant_repos = fetch_tenant_repos(state, &msg.tenant_id).await;
    let default_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let repos_list = if tenant_repos.is_empty() {
        format!("- {default_repo} (this repo)")
    } else {
        tenant_repos
            .iter()
            .map(|r| {
                if r == &default_repo {
                    format!("- {r} (this repo — default)")
                } else {
                    format!("- {r}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let system = format!(
        "You are a planning agent for the {owner}/{repo} repository. \
         Research the codebase using the provided tools, then generate an implementation plan.\n\n\
         You have access to these repos:\n{repos_list}\n\n\
         All tools default to {owner}/{repo} when the `repo` parameter is omitted. \
         To explore another repo, pass `repo` as `owner/name` (e.g. `\"{default_repo}\"`).",
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
3. **tasks.md** — Step-by-step implementation checklist. Each task is `- [ ] description`. Tasks should be atomic and ordered. \
Only include tasks that can be accomplished by writing code (creating files, editing files, updating config). \
Do NOT include manual verification tasks, post-deploy checks, browser testing, or anything requiring human interaction.
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
        default_owner: &msg.repo_owner,
        default_repo: &msg.repo_name,
        allowed_repos: &tenant_repos,
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
    let repo_prop = json!({"type": "string", "description": "Repository as owner/name. Omit to use the default (issue) repo."});
    vec![
        ToolDefinition {
            name: "read_tree".to_string(),
            description: "Get the full recursive file tree. Call once, then use list_directory for subdirs.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo": repo_prop
                }
            }),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from a repository. Large files are truncated to ~32KB. Prefer read_file_lines for targeted reads.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "repo": repo_prop
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
                    "end_line": {"type": "integer", "description": "Last line to read (inclusive)"},
                    "repo": repo_prop
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code in a repository by keyword or symbol. Returns matching file paths and text fragments. Use to find definitions instead of reading files.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (e.g. function name, type, import)"},
                    "repo": repo_prop
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
                    "path": {"type": "string", "description": "Directory path relative to repo root"},
                    "repo": repo_prop
                },
                "required": ["path"]
            }),
        },
    ]
}

struct ReadOnlyToolExecutor<'a> {
    github: &'a GitHubClient,
    default_owner: &'a str,
    default_repo: &'a str,
    allowed_repos: &'a [String],
}

impl<'a> ReadOnlyToolExecutor<'a> {
    /// Resolve owner/repo from the optional `repo` param in tool input.
    /// Falls back to default (issue) repo. Returns an error if the repo isn't in the allowed list.
    fn resolve_repo(
        &self,
        input: &serde_json::Value,
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let repo_param = input.get("repo").and_then(|v| v.as_str());
        match repo_param {
            Some(full) => {
                let parts: Vec<&str> = full.splitn(2, '/').collect();
                if parts.len() != 2 {
                    return Err(format!("Invalid repo format: {full}. Use owner/name.").into());
                }
                // If we have an allowed list, check it; otherwise allow any (fallback for empty list)
                if !self.allowed_repos.is_empty() && !self.allowed_repos.iter().any(|r| r == full) {
                    return Err(format!("Repo {full} is not in the connected repos list.").into());
                }
                Ok((parts[0].to_string(), parts[1].to_string()))
            }
            None => Ok((
                self.default_owner.to_string(),
                self.default_repo.to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for ReadOnlyToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let (owner, repo) = self.resolve_repo(input)?;
        let branch = "main";
        match name {
            "read_tree" => {
                let tree = self.github.get_tree(&owner, &repo, branch).await?;
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
                let content = self.github.read_file(&owner, &repo, path, branch).await?;
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
                    .read_file_lines(&owner, &repo, path, branch, start, end)
                    .await?;
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self.github.search_code(&owner, &repo, query).await?;
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
                    .list_directory(&owner, &repo, path, branch)
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

/// Fetch enabled repos for a tenant from DynamoDB.
async fn fetch_tenant_repos(state: &WorkerState, tenant_id: &str) -> Vec<String> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.to_string()),
        )
        .expression_attribute_values(
            ":prefix",
            aws_sdk_dynamodb::types::AttributeValue::S("REPO#".to_string()),
        )
        .send()
        .await;

    match result {
        Ok(output) => output
            .items()
            .iter()
            .filter(|item| {
                item.get("enabled")
                    .and_then(|v| v.as_bool().ok())
                    .copied()
                    .unwrap_or(false)
            })
            .filter_map(|item| item.get("repo_name").and_then(|v| v.as_s().ok()).cloned())
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch tenant repos for planner");
            Vec::new()
        }
    }
}
