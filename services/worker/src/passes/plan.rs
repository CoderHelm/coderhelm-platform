use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::mcp;
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// Result of the plan pass: the four openspec files.
pub struct PlanResult {
    pub proposal: String,
    pub design: String,
    pub tasks: String,
    pub spec: String,
    /// Per-repo task breakdown. Empty = single-repo (use msg.repo_owner/repo_name).
    pub repo_tasks: Vec<RepoTasks>,
}

/// Tasks scoped to a specific repo for multi-repo orchestration.
#[derive(Clone, Debug)]
pub struct RepoTasks {
    pub owner: String,
    pub name: String,
    pub tasks: String,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    triage: &super::triage::TriageResult,
    repo_instructions: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<PlanResult, Box<dyn std::error::Error + Send + Sync>> {
    // Fetch all enabled repos for this team so the planner can explore cross-repo
    let team_repos = fetch_team_repos(state, &msg.team_id).await;
    let default_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let repos_list = if team_repos.is_empty() {
        format!("- {default_repo} (this repo)")
    } else {
        team_repos
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

    let instructions_block = if repo_instructions.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Repository Context (AGENTS.md)\nUse this to understand the repo structure and conventions. \
             This should reduce or eliminate the need to browse the file tree:\n\n{repo_instructions}"
        )
    };

    let system = format!(
        "You are a planning agent for the {owner}/{repo} repository. \
         Research the codebase using the provided tools, then generate an implementation plan.\n\n\
         You have access to these repos:\n{repos_list}\n\n\
         All tools default to {owner}/{repo} when the `repo` parameter is omitted. \
         To explore another repo, pass `repo` as `owner/name` (e.g. `\"{default_repo}\"`).{instructions_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let complexity_guidance = match triage.complexity.as_str() {
        "simple" => {
            r#"## Complexity: SIMPLE (1-3 files)

This is a simple change. Be extremely efficient:
- Use 1-2 `search_code` calls to locate the relevant file(s) and line numbers
- Use `read_file_lines` on the specific section (max 100 lines), NOT whole files
- You should need 2-4 tool calls total, then output the plan
- The plan should have 1-2 tasks maximum"#
        }
        "medium" => {
            r#"## Complexity: MEDIUM (4-10 files)

This is a medium-sized change. Be efficient:
- Start with `search_code` to find relevant files, symbols, and line numbers
- Use `read_file_lines` for targeted reads (max 100 lines each)
- You should need 4-8 tool calls total
- The plan should have 2-4 tasks"#
        }
        _ => {
            r#"## Complexity: COMPLEX (10+ files)

This is a complex change. Research thoroughly but stay focused:
- Start with `search_code` to find relevant symbols and entry points
- Use `list_directory` only if you need to understand a module's structure
- Use `read_file_lines` for targeted reads (max 100 lines each)
- Map out the affected code paths before planning
- The plan should have 3-6 tasks"#
        }
    };

    let has_images = !msg.image_attachments.is_empty();
    let image_hint = if has_images {
        "\n\n**Note:** This issue includes image attachments (screenshots, mockups, or designs) below the text. \
         Study them carefully — they may show UI designs to replicate, error messages to fix, \
         or visual specifications that define what to build. Use the images as primary context alongside the text."
    } else {
        ""
    };

    let prompt = format!(
        r#"Generate an implementation plan (openspec) for this issue.

## Issue
Title: {title}
Summary: {summary}

## Original Issue Body
{body}{image_hint}

{complexity_guidance}

## Research Instructions

**Start with `search_code`** to find relevant files, symbols, and line numbers.
Then use `read_file_lines` for targeted reads (max 100 lines per call).
Do NOT read entire files. Do NOT browse the file tree unless you need module structure.
Be surgical — search, read the exact lines you need, then output the plan.

**Verify before planning:** Check if the requested change is ALREADY in place. If it is,
set tasks.md to EXACTLY `NO_CHANGES_NEEDED: <reason>`.

**Ambiguous values:** If the issue asks to change a value but does NOT include the new value:
1. Check if the issue body contains URLs. If so, call the appropriate MCP tool to fetch them.
2. Do NOT proactively search external tools unless a URL is present.
3. If you cannot determine the value, set tasks.md to `CLARIFICATION_NEEDED: <what is missing>`.

Otherwise, generate four openspec files:

1. **proposal.md** — Problem statement, proposed approach, scope boundaries, risks
2. **design.md** — Technical design: which files to modify/create (use full paths from repo root e.g. `src/services/adyen.ts`), patterns to follow, data flow
3. **tasks.md** — Step-by-step implementation checklist. Each task MUST follow this format:
   `- [ ] <file_path>: <description>` (e.g. `- [ ] src/resolvers/join.ts: Add optional billingAddress param to OnlineJoin mutation`)
   Tasks should be atomic and ordered. Include the full file path from the repo root in every task. \
Only include tasks that can be accomplished by writing code (creating files, editing files, updating config). \
Do NOT include manual verification tasks, post-deploy checks, browser testing, or anything requiring human interaction.

   **Multi-repo:** If the issue requires changes in MORE THAN ONE repo, group tasks under `## owner/repo` headers:
   ```
   ## owner/repo-a
   - [ ] src/file.rs: Change something
   ## owner/repo-b
   - [ ] src/other.ts: Change something else
   ```
   Only use multi-repo headers when changes span 2+ repos. For single-repo issues, omit headers.
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
        body = msg.body,
        complexity_guidance = complexity_guidance,
    );

    let mut tools = read_only_tools();

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
            // Try S3 cache first, then invoke proxy to list tools
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

        // Filter MCP tools for plan pass — only keep read-only tools
        tools.retain(|t| {
            // Keep all built-in tools (no server_id__ prefix)
            if !t.name.contains("__") {
                return true;
            }
            // For MCP tools, only keep read-like operations
            let tool_name = t.name.split("__").last().unwrap_or(&t.name);
            tool_name.starts_with("get_")
                || tool_name.starts_with("list_")
                || tool_name.starts_with("search_")
                || tool_name.starts_with("read_")
                || tool_name.starts_with("query_")
                || tool_name.starts_with("fetch_")
                || tool_name.starts_with("find_")
                || tool_name.starts_with("describe_")
        });
    }

    // Add MCP context to system prompt if we have active plugins
    let mut full_system = system.clone();
    if !loaded_mcp_plugins.is_empty() {
        let plugin_lines: Vec<String> = loaded_mcp_plugins
            .iter()
            .map(|p| format!("- {}", p.server_id))
            .collect();
        full_system.push_str(&format!(
            "\n\nYou have tool-call access to these MCP servers. \
             Only use them if the issue description contains URLs or explicit references \
             to external sources:\n{}",
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

    let executor = CombinedToolExecutor {
        read_only: ReadOnlyToolExecutor {
            github,
            default_owner: &msg.repo_owner,
            default_repo: &msg.repo_name,
            base_branch: &msg.base_branch,
            allowed_repos: &team_repos,
        },
        mcp_plugins: &loaded_mcp_plugins,
        lambda: &state.lambda,
        mcp_proxy_function_name: &state.config.mcp_proxy_function_name,
    };

    let mut content_blocks = vec![serde_json::json!({"type": "text", "text": prompt})];
    for img in &msg.image_attachments {
        if let Some(b64) = super::download_image_as_base64(&state.s3, &state.config.bucket_name, &img.s3_key).await {
            content_blocks.push(serde_json::json!({
                "type": "image",
                "source": { "type": "base64", "media_type": img.media_type, "data": b64 }
            }));
        }
    }

    let mut messages = vec![("user".to_string(), content_blocks)];

    let plan_opts = llm::ConverseOptions {
        max_turns: match triage.complexity.as_str() {
            "simple" => 5,
            "medium" => 8,
            _ => 12,
        },
        max_tokens: 16384,
        deadline: None,
    };

    let model_id = provider.primary_model_id();
    let response = provider::converse(
        state,
        provider,
        model_id,
        &full_system,
        &mut messages,
        &tools,
        &executor,
        usage,
        plan_opts,
        None,
        None,
    )
    .await?;

    let files = parse_openspec_files(&response);

    // Write openspec to S3
    let prefix = format!(
        "teams/{}/runs/{}/openspec",
        msg.team_id,
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

    // Store context hash so we can detect when ticket content changes
    let context_hash = compute_ticket_context_hash(msg);
    let hash_key = format!("{prefix}/context_hash.txt");
    let _ = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(&hash_key)
        .body(context_hash.as_bytes().to_vec().into())
        .content_type("text/plain")
        .send()
        .await;

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

    let tasks_content = contents[2].clone();
    let repo_tasks = parse_repo_tasks(&tasks_content);

    PlanResult {
        proposal: contents.remove(0),
        design: contents.remove(0),
        tasks: contents.remove(0),
        spec: contents.remove(0),
        repo_tasks,
    }
}

/// Parse `## owner/repo` headers in tasks.md for multi-repo support.
/// Returns empty vec if no repo headers found (single-repo mode).
fn parse_repo_tasks(tasks: &str) -> Vec<RepoTasks> {
    let mut result = Vec::new();
    let mut current_owner = String::new();
    let mut current_name = String::new();
    let mut current_tasks = String::new();

    for line in tasks.lines() {
        if let Some(repo_str) = line.strip_prefix("## ") {
            // Save previous repo if any
            if !current_owner.is_empty() {
                result.push(RepoTasks {
                    owner: current_owner.clone(),
                    name: current_name.clone(),
                    tasks: current_tasks.trim().to_string(),
                });
            }
            // Parse owner/name
            let repo_str = repo_str.trim();
            if let Some((owner, name)) = repo_str.split_once('/') {
                current_owner = owner.to_string();
                current_name = name.to_string();
                current_tasks = String::new();
            }
        } else if !current_owner.is_empty() {
            current_tasks.push_str(line);
            current_tasks.push('\n');
        }
    }

    // Save last repo
    if !current_owner.is_empty() && !current_tasks.trim().is_empty() {
        result.push(RepoTasks {
            owner: current_owner,
            name: current_name,
            tasks: current_tasks.trim().to_string(),
        });
    }

    // Only return multi-repo if we found 2+ repos
    if result.len() < 2 {
        return vec![];
    }

    result
}

// ─── Read-only tools for plan pass ──────────────────────────

fn read_only_tools() -> Vec<ToolDefinition> {
    let repo_prop = json!({"type": "string", "description": "Repository as owner/name. Omit to use the default (issue) repo."});
    vec![
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code by keyword, function name, type, or symbol. Returns matching file paths with line numbers and code fragments. Always start here.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (e.g. function name, type, import, error message)"},
                    "repo": repo_prop
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific lines from a file (1-indexed, inclusive). Use search_code first to find the right file and line numbers.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line to read (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line to read (inclusive, max 100 lines per call)"},
                    "repo": repo_prop
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "list_directory".to_string(),
            description: "List contents of a directory. Use to understand module structure.".to_string(),
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
    base_branch: &'a str,
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
        let branch = &self.base_branch;
        match name {
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
                // Cap at 100 lines per call to limit token usage
                let end = end.min(start + 100);
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

/// Fetch enabled repos for a team from DynamoDB.
pub async fn fetch_team_repos(state: &WorkerState, team_id: &str) -> Vec<String> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
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
            tracing::warn!(error = %e, "Failed to fetch team repos for planner");
            Vec::new()
        }
    }
}

// ─── Combined tool executor (read-only + MCP) ──────────────

struct CombinedToolExecutor<'a> {
    read_only: ReadOnlyToolExecutor<'a>,
    mcp_plugins: &'a [mcp::McpPlugin],
    lambda: &'a aws_sdk_lambda::Client,
    mcp_proxy_function_name: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for CombinedToolExecutor<'a> {
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

        // Fall through to read-only tools
        self.read_only.execute(name, input).await
    }
}

/// Compute a hash of ticket content (body + image attachment keys) for change detection.
pub fn compute_ticket_context_hash(msg: &TicketMessage) -> String {
    let mut hasher = DefaultHasher::new();
    msg.title.hash(&mut hasher);
    msg.body.hash(&mut hasher);
    for img in &msg.image_attachments {
        img.s3_key.hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}
