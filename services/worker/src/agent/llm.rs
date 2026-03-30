use aws_sdk_bedrockruntime::types::{
    CachePointBlock, ContentBlock, ConversationRole, Message, SystemContentBlock, Tool,
    ToolConfiguration, ToolInputSchema, ToolSpecification,
};
use aws_smithy_types::Document;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::TokenUsage;
use crate::WorkerState;

fn json_to_document(val: &Value) -> Document {
    match val {
        Value::Null => Document::Null,
        Value::Bool(b) => Document::Bool(*b),
        Value::Number(n) => {
            Document::Number(aws_smithy_types::Number::Float(n.as_f64().unwrap_or(0.0)))
        }
        Value::String(s) => Document::String(s.clone()),
        Value::Array(arr) => Document::Array(arr.iter().map(json_to_document).collect()),
        Value::Object(obj) => Document::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_document(v)))
                .collect(),
        ),
    }
}

fn document_to_json(doc: &Document) -> Value {
    match doc {
        Document::Null => Value::Null,
        Document::Bool(b) => Value::Bool(*b),
        Document::Number(n) => {
            json!(n.to_f64_lossy())
        }
        Document::String(s) => Value::String(s.clone()),
        Document::Array(arr) => Value::Array(arr.iter().map(document_to_json).collect()),
        Document::Object(obj) => {
            let map: serde_json::Map<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect();
            Value::Object(map)
        }
    }
}

/// Call the LLM via Bedrock Converse API with tool use loop.
/// Uses prompt caching: a CachePoint is placed after the system prompt
/// so Bedrock caches it across calls within the same session/model.
/// Maximum tool-use turns per converse() call to prevent runaway token spend.
const MAX_TURNS: usize = 40;

pub async fn converse(
    state: &WorkerState,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let tool_config = build_tool_config(tools);
    let mut turns: usize = 0;

    // Build the cache point block once — reused every turn.
    let cache_point = CachePointBlock::builder()
        .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
        .build()
        .unwrap();

    loop {
        turns += 1;
        if turns > MAX_TURNS {
            warn!("Hit max turn limit ({MAX_TURNS}), forcing completion");
            return Ok(
                "[Turn limit reached — stopping to avoid excessive token usage]".to_string(),
            );
        }
        let mut request = state
            .bedrock
            .converse()
            .model_id(&state.config.model_id)
            // System prompt text
            .system(SystemContentBlock::Text(system_prompt.to_string()))
            // Cache point after system: caches system prompt + tool config prefix.
            .system(SystemContentBlock::CachePoint(cache_point.clone()));

        // Add all messages. On turn 2+, insert a cache point at the end of the
        // second-to-last message so Bedrock caches the conversation prefix and
        // only processes the new tool results as fresh input tokens.
        let msg_count = messages.len();
        for (i, msg) in messages.iter().enumerate() {
            request = request.messages(msg.clone());
            // Place cache point on the last user message (tool results) before
            // the most recent assistant reply, so the full prefix is cached.
            if turns > 1 && msg_count >= 2 && i == msg_count - 2 {
                // Bedrock supports cache points as content blocks within messages.
                // However the Converse API only supports cache points in system blocks,
                // not in individual messages — so we rely on the system-level cache.
                // Future: if Bedrock adds message-level cache points, insert here.
            }
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
                u.cache_read_input_tokens().unwrap_or(0) as u64,
                u.cache_write_input_tokens().unwrap_or(0) as u64,
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
            let input: Value = document_to_json(tool_use.input());

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
                    .input_schema(ToolInputSchema::Json(json_to_document(&t.input_schema)))
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
