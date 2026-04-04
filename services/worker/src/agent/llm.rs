use aws_sdk_bedrockruntime::types::{
    CachePointBlock, ContentBlock, ConversationRole, InferenceConfiguration, Message,
    SystemContentBlock, Tool, ToolConfiguration, ToolInputSchema, ToolSpecification,
};
use aws_smithy_types::Document;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::TokenUsage;
use crate::WorkerState;

/// Send a simple (non-agentic) Bedrock converse request with retry on transient errors.
/// Used by onboard, infra_analyze, and other one-shot Bedrock calls.
pub async fn converse_with_retry(
    bedrock: &aws_sdk_bedrockruntime::Client,
    model_id: &str,
    system: Vec<SystemContentBlock>,
    messages: Vec<Message>,
) -> Result<
    aws_sdk_bedrockruntime::operation::converse::ConverseOutput,
    Box<dyn std::error::Error + Send + Sync>,
> {
    for attempt in 0..3u32 {
        let mut req = bedrock.converse().model_id(model_id);
        for s in &system {
            req = req.system(s.clone());
        }
        req = req.set_messages(Some(messages.clone()));
        match req.send().await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                let err_str = format!("{e:#}");
                let transient = err_str.contains("ThrottlingException")
                    || err_str.contains("ServiceUnavailable")
                    || err_str.contains("InternalServer")
                    || err_str.contains("service error");
                if attempt < 2 && transient {
                    let delay_secs = 2u64.pow(attempt + 1);
                    warn!(
                        model_id,
                        attempt = attempt + 1,
                        "Bedrock transient error, retrying in {delay_secs}s: {err_str}"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                    continue;
                }
                return Err(format!("Bedrock converse error (model={model_id}): {e:#}").into());
            }
        }
    }
    unreachable!()
}

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
const DEFAULT_MAX_TURNS: usize = 40;

/// Options for controlling the converse loop.
pub struct ConverseOptions {
    pub max_turns: usize,
    pub max_tokens: i32,
}

impl Default for ConverseOptions {
    fn default() -> Self {
        Self {
            max_turns: DEFAULT_MAX_TURNS,
            max_tokens: 16384,
        }
    }
}

pub async fn converse(
    state: &WorkerState,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    converse_with_opts(state, model_id, system_prompt, messages, tools, tool_executor, usage, ConverseOptions::default()).await
}

pub async fn converse_with_opts(
    state: &WorkerState,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
    opts: ConverseOptions,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let tool_config = build_tool_config(tools);
    let mut turns: usize = 0;
    let max_turns = opts.max_turns;

    let inference_config = InferenceConfiguration::builder()
        .max_tokens(opts.max_tokens)
        .build();

    // Build the cache point block once — reused every turn.
    let cache_point = CachePointBlock::builder()
        .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
        .build()
        .unwrap();

    loop {
        turns += 1;
        if turns > max_turns {
            warn!("Hit max turn limit ({max_turns}), forcing completion");
            return Err(
                format!("Reached the maximum number of steps ({max_turns}) without finishing. The issue may need more detail or a narrower scope.").into()
            );
        }

        // Progress note every 10 turns to keep the LLM focused
        if turns > 1 && turns.is_multiple_of(10) {
            info!(turns, "Injecting progress note at turn {turns}");
            let remaining = max_turns - turns;
            let note = format!(
                "[SYSTEM NOTE] You have used {turns} of {max_turns} tool-use turns. {remaining} turns remaining. \
                 Focus on completing the task efficiently. If you are stuck, summarize what you have done and provide your best answer."
            );
            messages.push(
                Message::builder()
                    .role(ConversationRole::User)
                    .content(ContentBlock::Text(note))
                    .build()
                    .unwrap(),
            );
        }

        // Send with automatic retry for transient Bedrock errors (5xx, throttling).
        let response = 'send: {
            for attempt in 0..3u32 {
                let mut req = state
                    .bedrock
                    .converse()
                    .model_id(model_id)
                    .system(SystemContentBlock::Text(system_prompt.to_string()))
                    .system(SystemContentBlock::CachePoint(cache_point.clone()))
                    .inference_config(inference_config.clone());

                for msg in messages.iter() {
                    req = req.messages(msg.clone());
                }

                if !tools.is_empty() {
                    req = req.tool_config(tool_config.clone());
                }

                match req.send().await {
                    Ok(resp) => break 'send resp,
                    Err(e) => {
                        let err_str = format!("{e:#}");
                        let transient = err_str.contains("ThrottlingException")
                            || err_str.contains("ServiceUnavailable")
                            || err_str.contains("InternalServer")
                            || err_str.contains("service error");
                        if attempt < 2 && transient {
                            let delay_secs = 2u64.pow(attempt + 1);
                            warn!(
                                model_id,
                                attempt = attempt + 1,
                                "Bedrock transient error, retrying in {delay_secs}s: {err_str}"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                            continue;
                        }
                        let msg = format!("Bedrock converse error (model={model_id}): {e:#}");
                        error!("{msg}");
                        return Err(msg.into());
                    }
                }
            }
            unreachable!()
        };

        // Track token usage (including cache metrics)
        if let Some(u) = response.usage() {
            usage.add(
                u.input_tokens() as u64,
                u.output_tokens() as u64,
                u.cache_read_input_tokens().unwrap_or(0) as u64,
                u.cache_write_input_tokens().unwrap_or(0) as u64,
            );

            // Context intelligence: tiered compaction based on actual input tokens
            let input_tokens = u.input_tokens() as u64;
            let model_limit: u64 = 200_000;
            let context_pct = input_tokens as f64 / model_limit as f64;

            if context_pct > 0.90 {
                // Tier 3: Emergency — clear all tool results except last 3 turns
                info!(
                    "Context at {:.0}%, emergency compaction (clearing all but last 3 turns)",
                    context_pct * 100.0
                );
                clear_old_tool_results(messages, 3);
            } else if context_pct > 0.75 {
                // Tier 2: Aggressive — clear tool results older than last 5 turns
                info!(
                    "Context at {:.0}%, clearing old tool results (keeping last 5 turns)",
                    context_pct * 100.0
                );
                clear_old_tool_results(messages, 5);
            } else if context_pct > 0.60 {
                // Tier 1: Gentle — clear tool results older than last 8 turns
                clear_old_tool_results(messages, 8);
            }
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
            let tool_start = std::time::Instant::now();
            info!(tool = tool_use.name(), "Executing tool");
            let input: Value = document_to_json(tool_use.input());

            match tool_executor.execute(tool_use.name(), &input).await {
                Ok(result) => {
                    let duration_ms = tool_start.elapsed().as_millis() as u64;
                    usage.record_tool_call(tool_use.name(), duration_ms);
                    info!(tool = tool_use.name(), duration_ms, "Tool completed");
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
                    let duration_ms = tool_start.elapsed().as_millis() as u64;
                    usage.record_tool_call(tool_use.name(), duration_ms);
                    warn!(tool = tool_use.name(), error = %e, duration_ms, "Tool execution failed");
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

/// Replace tool result content with a placeholder for messages older than `keep_last` turns.
/// Preserves tool use blocks (names + inputs) but drops the full output text to reclaim context.
fn clear_old_tool_results(messages: &mut [Message], keep_last: usize) {
    let total = messages.len();
    if total <= keep_last * 2 {
        return; // Not enough messages to compact
    }
    let cutoff = total.saturating_sub(keep_last * 2); // Each tool turn = 2 messages (assistant + user)

    for msg in messages[..cutoff].iter_mut() {
        if msg.role() != &ConversationRole::User {
            continue;
        }
        let mut new_content = Vec::new();
        let mut modified = false;
        for block in msg.content() {
            match block {
                ContentBlock::ToolResult(tr) => {
                    // Replace large tool results with placeholder
                    let text_len: usize = tr
                        .content()
                        .iter()
                        .map(|c| match c {
                            aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(t) => {
                                t.len()
                            }
                            _ => 0,
                        })
                        .sum();
                    if text_len > 200 {
                        let placeholder = aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                            .tool_use_id(tr.tool_use_id())
                            .content(aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                                "[Tool result cleared — see recent context]".to_string(),
                            ))
                            .build()
                            .unwrap();
                        new_content.push(ContentBlock::ToolResult(placeholder));
                        modified = true;
                    } else {
                        new_content.push(block.clone());
                    }
                }
                _ => new_content.push(block.clone()),
            }
        }
        if modified {
            *msg = Message::builder()
                .role(ConversationRole::User)
                .set_content(Some(new_content))
                .build()
                .unwrap();
        }
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
