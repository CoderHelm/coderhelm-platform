use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::mcp;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// Run the resolve pass: a lightweight agent that fetches content from external
/// references in the ticket (Notion pages, Figma designs, Sentry errors, etc.)
/// using available MCP tools.
///
/// Returns the resolved context as text to inject into the triage summary,
/// or empty string if nothing was resolved.
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    mcp_plugins: &[mcp::McpPlugin],
    usage: &mut TokenUsage,
) -> String {
    // Load MCP tool schemas
    let mut tools: Vec<ToolDefinition> = Vec::new();
    let mut loaded_plugins: Vec<mcp::McpPlugin> = Vec::new();

    for plugin in mcp_plugins {
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
                        tracing::warn!(server_id = %plugin.server_id, error = %e, "Failed to list MCP tools for resolve");
                        continue;
                    }
                }
            }
            _ => continue,
        };

        let mcp_tool_defs = mcp::to_tool_definitions(&plugin.server_id, &schemas);
        tools.extend(mcp_tool_defs);
        loaded_plugins.push(plugin.clone());
    }

    if tools.is_empty() {
        return String::new();
    }

    let plugin_lines: Vec<String> = loaded_plugins
        .iter()
        .map(|p| {
            let desc = p.custom_prompt.as_deref().unwrap_or("(no description)");
            format!("- **{}**: {}", p.server_id, desc)
        })
        .collect();

    let system = format!(
        "You are a context-gathering agent. Your ONLY job is to fetch content from external \
         references (URLs, service names, identifiers) found in a ticket description.\n\n\
         You have access to these MCP servers:\n{plugins}\n\n\
         Rules:\n\
         - Look for URLs, page references, ticket IDs, or mentions of external services in the issue.\n\
         - Use the available MCP tools to fetch the referenced content.\n\
         - If there are no external references, or none match your available tools, respond with \
           EXACTLY `NO_EXTERNAL_REFS`.\n\
         - After fetching, output a clean summary of what you found. Format each source as:\n\
           `## <source description>\\n<content>`\n\
         - Do NOT plan, implement, or analyze. Only fetch and summarize external content.\n\
         - Be concise. Strip boilerplate. Keep only the information relevant to the ticket.",
        plugins = plugin_lines.join("\n"),
    );

    let prompt = format!(
        "Fetch any external references from this ticket and return their content.\n\n\
         **Title:** {title}\n\
         **Description:** {body}",
        title = msg.title,
        body = msg.body,
    );

    let executor = McpOnlyExecutor {
        plugins: &loaded_plugins,
        lambda: &state.lambda,
        mcp_proxy_function_name: &state.config.mcp_proxy_function_name,
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()
        .unwrap()];

    let response = match llm::converse(
        state,
        &state.config.light_model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "Resolve pass failed, continuing without external context");
            return String::new();
        }
    };

    if response.trim() == "NO_EXTERNAL_REFS" || response.trim().is_empty() {
        info!("No external references to resolve");
        return String::new();
    }

    info!(chars = response.len(), "Resolved external references");
    format!(
        "\n\n## External context (auto-resolved)\n\n{}",
        response.trim()
    )
}

/// Tool executor that only handles MCP tools — no file/repo access.
struct McpOnlyExecutor<'a> {
    plugins: &'a [mcp::McpPlugin],
    lambda: &'a aws_sdk_lambda::Client,
    mcp_proxy_function_name: &'a str,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for McpOnlyExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let (server_id, tool_name) = name
            .split_once("__")
            .ok_or_else(|| format!("Resolve pass only supports MCP tools, got: {name}"))?;

        let plugin = self
            .plugins
            .iter()
            .find(|p| p.server_id == server_id)
            .ok_or_else(|| format!("No MCP plugin found for server: {server_id}"))?;

        mcp::call_tool(
            self.lambda,
            self.mcp_proxy_function_name,
            plugin,
            tool_name,
            input,
        )
        .await
    }
}
