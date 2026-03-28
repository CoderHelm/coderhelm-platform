use aws_sdk_bedrockruntime::types::{
    CachePointBlock, ContentBlock, ConversationRole, Message, SystemContentBlock, Tool,
    ToolConfiguration, ToolInputSchema, ToolSpecification,
};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::TokenUsage;
use crate::WorkerState;

/// Call the LLM via Bedrock Converse API with tool use loop.
/// Uses prompt caching: a CachePoint is placed after the system prompt
/// so Bedrock caches it across calls within the same session/model.
pub async fn converse(
    state: &WorkerState,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let tool_config = build_tool_config(tools);

    loop {
        let mut request = state
            .bedrock
            .converse()
            .model_id(&state.config.model_id)
            // System prompt text
            .system(SystemContentBlock::Text(system_prompt.to_string()))
            // Cache point: tells Bedrock to cache everything above this marker.
            // On subsequent calls with the same system prompt prefix, Bedrock
            // serves cached tokens at 0.1x input price instead of full price.
            .system(SystemContentBlock::CachePoint(
                CachePointBlock::builder().build().unwrap(),
            ));

        for msg in messages.iter() {
            request = request.messages(msg.clone());
        }

        if !tools.is_empty() {
            request = request.tool_config(tool_config.clone());
        }

        let response = request.send().await?;

        // Track token usage (including cache metrics)
        if let Some(u) = response.usage() {
            usage.add(
                u.input_tokens() as u64,
                u.output_tokens() as u64,
                u.cache_read_input_token_count().unwrap_or(0) as u64,
                u.cache_write_input_token_count().unwrap_or(0) as u64,
            );
        }

        let output = response.output().ok_or("No output from Bedrock")?;
        let output_message = match output {
            aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg) => msg.clone(),
            _ => return Err("Unexpected output type".into()),
        };

        messages.push(output_message.clone());

        // Check if model wants to use tools
        let tool_uses: Vec<_> = output_message
            .content()
            .iter()
            .filter_map(|block| {
                if let ContentBlock::ToolUse(tool_use) = block {
                    Some(tool_use.clone())
                } else {
                    None
                }
            })
            .collect();

        if tool_uses.is_empty() {
            // No tool use — extract final text response
            let text = output_message
                .content()
                .iter()
                .filter_map(|block| {
                    if let ContentBlock::Text(t) = block {
                        Some(t.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Ok(text);
        }

        // Execute tools and build tool result message
        let mut tool_results = Vec::new();
        for tool_use in &tool_uses {
            info!(tool = tool_use.name(), "Executing tool");
            let input: Value = serde_json::from_str(
                &tool_use
                    .input()
                    .as_ref()
                    .map(|d| d.to_string())
                    .unwrap_or_default(),
            )
            .unwrap_or(json!({}));

            match tool_executor.execute(tool_use.name(), &input).await {
                Ok(result) => {
                    tool_results.push(ContentBlock::ToolResult(
                        aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                            .tool_use_id(tool_use.tool_use_id())
                            .content(aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                                serde_json::to_string(&result)?,
                            ))
                            .build()?,
                    ));
                }
                Err(e) => {
                    warn!(tool = tool_use.name(), error = %e, "Tool execution failed");
                    tool_results.push(ContentBlock::ToolResult(
                        aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                            .tool_use_id(tool_use.tool_use_id())
                            .content(aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                                format!("Error: {e}"),
                            ))
                            .status(aws_sdk_bedrockruntime::types::ToolResultStatus::Error)
                            .build()?,
                    ));
                }
            }
        }

        // Add tool results as user message
        messages.push(
            Message::builder()
                .role(ConversationRole::User)
                .set_content(Some(tool_results))
                .build()?,
        );
    }
}

pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

fn build_tool_config(tools: &[ToolDefinition]) -> ToolConfiguration {
    let tool_specs: Vec<Tool> = tools
        .iter()
        .map(|t| {
            Tool::ToolSpec(
                ToolSpecification::builder()
                    .name(&t.name)
                    .description(&t.description)
                    .input_schema(ToolInputSchema::Json(
                        aws_sdk_bedrockruntime::types::Document::try_from(
                            serde_json::to_value(&t.input_schema).unwrap(),
                        )
                        .unwrap(),
                    ))
                    .build()
                    .unwrap(),
            )
        })
        .collect();

    ToolConfiguration::builder()
        .set_tools(Some(tool_specs))
        .build()
        .unwrap()
}

/// Trait for executing tools. Implemented by the orchestrator.
#[async_trait::async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}
