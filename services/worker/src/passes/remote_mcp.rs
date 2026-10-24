//! Remote MCP servers for agents that build their own tool list (the
//! reviewer). The planner, coder and resolver get them through the same
//! `agent::mcp` plugins as the catalog servers; this wraps those plugins for
//! the reviewer's executor.
//!
//! The reviewer only reads: it gets the tools a server marks read-only (or,
//! without a hint, ones named like reads), from servers the team shares. Its
//! calls are audited like every other agent's.

use crate::agent::llm::ToolDefinition;
use crate::agent::mcp;
use crate::WorkerState;

/// Most text one tool result gives the model.
const MAX_RESULT_CHARS: usize = 24_000;

pub struct RemoteMcp {
    plugins: Vec<mcp::McpPlugin>,
    tools: Vec<ToolDefinition>,
}

impl RemoteMcp {
    /// The team's usable remote servers, read-only tools only. `None` when
    /// there are none.
    pub async fn load(
        state: &WorkerState,
        team_id: &str,
        user: Option<&str>,
        actor: String,
    ) -> Option<Self> {
        let table = if state.config.mcp_configs_table_name.is_empty() {
            &state.config.settings_table_name
        } else {
            &state.config.mcp_configs_table_name
        };
        let plugins = mcp::load_remote_plugins(
            &state.dynamo,
            &state.kms,
            team_id,
            &mcp::RemoteScope {
                table,
                key_id: &state.config.secrets_key_id,
                user,
                actor,
                read_only: true,
            },
        )
        .await;
        let mut tools = Vec::new();
        for p in &plugins {
            if let Some(s) =
                mcp::schemas_for(&state.s3, &state.config.bucket_name, &state.lambda, "", p).await
            {
                tools.extend(mcp::to_tool_definitions(&p.server_id, &s));
            }
        }
        (!tools.is_empty()).then_some(Self { plugins, tools })
    }

    pub fn tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|t| ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.input_schema.clone(),
            })
            .collect()
    }

    pub fn prompt_section(&self) -> String {
        let mut out = String::from(
            "\n\n## Connected MCP servers\nRead-only tools from these servers are available. \
             Use them only when the change or the question refers to that system:\n",
        );
        for p in &self.plugins {
            out.push_str(&format!("- {}\n", p.server_id));
            if let Some(prompt) = &p.custom_prompt {
                out.push_str(&format!("  {}\n", prompt.trim()));
            }
        }
        out
    }

    /// Runs `name` when it is one of these tools.
    pub async fn handle(
        &self,
        state: &WorkerState,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<String> {
        if !self.tools.iter().any(|t| t.name == name) {
            return None;
        }
        let (server_id, tool) = name.split_once("__")?;
        let plugin = self.plugins.iter().find(|p| p.server_id == server_id)?;
        Some(
            match mcp::call_tool(&state.lambda, "", plugin, tool, input).await {
                Ok(v) => {
                    let text = v
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| v.to_string());
                    common::head_tail_str(&text, MAX_RESULT_CHARS).to_string()
                }
                Err(e) => format!("Error: {e}"),
            },
        )
    }
}
