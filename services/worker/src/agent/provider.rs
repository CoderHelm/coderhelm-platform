//! Model provider dispatch — routes LLM calls to Bedrock or Anthropic based on team config.

use aws_sdk_bedrockruntime::types::{ContentBlock, ConversationRole, Message};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::models::TokenUsage;

use super::anthropic::{self, AnthropicClient};
use super::llm::{self, ToolDefinition, ToolExecutor};

/// Which model provider a team uses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "provider")]
pub enum ModelProvider {
    #[default]
    #[serde(rename = "bedrock")]
    Bedrock,
    #[serde(rename = "anthropic")]
    Anthropic {
        api_key: String,
        primary_model: String,
        heavy_model: String,
    },
}

impl ModelProvider {
    /// Load team's model provider settings from DynamoDB.
    pub async fn load_for_team(
        dynamo: &aws_sdk_dynamodb::Client,
        settings_table: &str,
        team_id: &str,
    ) -> Self {
        let result = dynamo
            .get_item()
            .table_name(settings_table)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#MODEL_PROVIDER".to_string()),
            )
            .send()
            .await;

        match result {
            Ok(output) => {
                if let Some(item) = output.item() {
                    let provider = item
                        .get("provider")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| s.as_str())
                        .unwrap_or("bedrock");

                    if provider == "anthropic" {
                        let api_key = item
                            .get("api_key")
                            .and_then(|v| v.as_s().ok())
                            .cloned()
                            .unwrap_or_default();
                        let primary_model = item
                            .get("primary_model")
                            .and_then(|v| v.as_s().ok())
                            .cloned()
                            .unwrap_or_else(|| "claude-sonnet-4-20250514".to_string());
                        let heavy_model = item
                            .get("heavy_model")
                            .and_then(|v| v.as_s().ok())
                            .cloned()
                            .unwrap_or_else(|| "claude-opus-4-20250514".to_string());

                        if api_key.is_empty() {
                            tracing::warn!(team_id, "Anthropic provider configured but no API key, falling back to Bedrock");
                            return Self::Bedrock;
                        }

                        info!(team_id, "Using Anthropic provider");
                        return Self::Anthropic {
                            api_key,
                            primary_model,
                            heavy_model,
                        };
                    }
                }
                Self::Bedrock
            }
            Err(e) => {
                tracing::warn!(team_id, error = %e, "Failed to load model provider settings, using Bedrock");
                Self::Bedrock
            }
        }
    }

    /// Get the model ID for "light" tasks (analysis, feedback, etc.)
    pub fn primary_model_id(&self, bedrock_default: &str) -> String {
        match self {
            Self::Bedrock => bedrock_default.to_string(),
            Self::Anthropic { primary_model, .. } => primary_model.clone(),
        }
    }

    /// Get the model ID for "heavy" tasks (implementation)
    pub fn heavy_model_id(&self, bedrock_default: &str) -> String {
        match self {
            Self::Bedrock => bedrock_default.to_string(),
            Self::Anthropic { heavy_model, .. } => heavy_model.clone(),
        }
    }

    /// Get the Anthropic API key (if configured).
    pub fn api_key(&self) -> Option<&str> {
        match self {
            Self::Bedrock => None,
            Self::Anthropic { api_key, .. } => Some(api_key.as_str()),
        }
    }

    #[allow(dead_code)]
    pub fn is_anthropic(&self) -> bool {
        matches!(self, Self::Anthropic { .. })
    }
}

/// Unified converse call that dispatches to Bedrock or Anthropic.
/// For Bedrock, delegates to existing `llm::converse_with_opts`.
/// For Anthropic, uses the Messages API client.
#[allow(clippy::too_many_arguments)]
pub async fn converse(
    state: &crate::WorkerState,
    provider: &ModelProvider,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
    opts: llm::ConverseOptions,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match provider {
        ModelProvider::Bedrock => {
            llm::converse_with_opts(
                state,
                model_id,
                system_prompt,
                messages,
                tools,
                tool_executor,
                usage,
                opts,
            )
            .await
        }
        ModelProvider::Anthropic { api_key, .. } => {
            let client = AnthropicClient::new(api_key.clone());

            // Convert Bedrock Messages to Anthropic format
            let mut api_messages = bedrock_to_anthropic_messages(messages);

            let result = anthropic::converse_tool_loop(
                &client,
                model_id,
                system_prompt,
                &mut api_messages,
                tools,
                tool_executor,
                usage,
                opts.max_turns,
                opts.max_tokens,
            )
            .await?;

            // Convert Anthropic messages back to Bedrock format for state consistency
            *messages = anthropic_to_bedrock_messages(&api_messages);

            Ok(result)
        }
    }
}

/// Simple converse (no tools) that dispatches to Bedrock or Anthropic.
pub async fn converse_simple(
    state: &crate::WorkerState,
    provider: &ModelProvider,
    model_id: &str,
    system_prompt: &str,
    user_message: &str,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match provider {
        ModelProvider::Bedrock => {
            let system_blocks = vec![
                aws_sdk_bedrockruntime::types::SystemContentBlock::Text(system_prompt.to_string()),
                aws_sdk_bedrockruntime::types::SystemContentBlock::CachePoint(
                    aws_sdk_bedrockruntime::types::CachePointBlock::builder()
                        .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
                        .build()
                        .unwrap(),
                ),
            ];
            let messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
                .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
                .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                    user_message.to_string(),
                ))
                .build()
                .map_err(|e| format!("Failed to build message: {e}"))?];
            let response =
                llm::converse_with_retry(&state.bedrock, model_id, system_blocks, messages).await?;
            // Track usage
            if let Some(u) = response.usage() {
                usage.add(
                    u.input_tokens() as u64,
                    u.output_tokens() as u64,
                    u.cache_read_input_tokens().unwrap_or(0) as u64,
                    u.cache_write_input_tokens().unwrap_or(0) as u64,
                );
            }
            // Extract text
            response
                .output()
                .and_then(|o| o.as_message().ok())
                .and_then(|m| m.content().first())
                .and_then(|c| c.as_text().ok())
                .map(|t| t.to_string())
                .ok_or_else(|| "No text in Bedrock response".into())
        }
        ModelProvider::Anthropic { api_key, .. } => {
            let client = AnthropicClient::new(api_key.clone());
            anthropic::converse_simple(&client, model_id, system_prompt, user_message, usage).await
        }
    }
}

// --- Format conversion helpers ---

fn bedrock_to_anthropic_messages(messages: &[Message]) -> Vec<(String, Vec<serde_json::Value>)> {
    messages
        .iter()
        .map(|msg| {
            let role = match msg.role() {
                ConversationRole::User => "user",
                ConversationRole::Assistant => "assistant",
                _ => "user",
            };
            let content: Vec<serde_json::Value> = msg
                .content()
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text(text) => {
                        Some(serde_json::json!({"type": "text", "text": text}))
                    }
                    ContentBlock::ToolUse(tu) => Some(serde_json::json!({
                        "type": "tool_use",
                        "id": tu.tool_use_id(),
                        "name": tu.name(),
                        "input": super::llm::document_to_json(tu.input())
                    })),
                    ContentBlock::ToolResult(tr) => {
                        let content_text = tr
                            .content()
                            .iter()
                            .filter_map(|c| {
                                if let aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                                    t,
                                ) = c
                                {
                                    Some(t.as_str())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        let is_error = tr.status()
                            == Some(&aws_sdk_bedrockruntime::types::ToolResultStatus::Error);
                        let mut obj = serde_json::json!({
                            "type": "tool_result",
                            "tool_use_id": tr.tool_use_id(),
                            "content": content_text
                        });
                        if is_error {
                            obj["is_error"] = serde_json::json!(true);
                        }
                        Some(obj)
                    }
                    _ => None,
                })
                .collect();
            (role.to_string(), content)
        })
        .collect()
}

fn anthropic_to_bedrock_messages(messages: &[(String, Vec<serde_json::Value>)]) -> Vec<Message> {
    messages
        .iter()
        .filter_map(|(role, content)| {
            let conv_role = match role.as_str() {
                "assistant" => ConversationRole::Assistant,
                _ => ConversationRole::User,
            };
            let blocks: Vec<ContentBlock> = content
                .iter()
                .filter_map(|block| {
                    let block_type = block.get("type")?.as_str()?;
                    match block_type {
                        "text" => {
                            let text = block.get("text")?.as_str()?;
                            Some(ContentBlock::Text(text.to_string()))
                        }
                        "tool_use" => {
                            let id = block.get("id")?.as_str()?;
                            let name = block.get("name")?.as_str()?;
                            let input = block.get("input")?;
                            Some(ContentBlock::ToolUse(
                                aws_sdk_bedrockruntime::types::ToolUseBlock::builder()
                                    .tool_use_id(id)
                                    .name(name)
                                    .input(super::llm::json_to_document(input))
                                    .build()
                                    .ok()?,
                            ))
                        }
                        "tool_result" => {
                            let tool_use_id = block.get("tool_use_id")?.as_str()?;
                            let content_str = block.get("content")?.as_str().unwrap_or("");
                            let mut builder =
                                aws_sdk_bedrockruntime::types::ToolResultBlock::builder()
                                    .tool_use_id(tool_use_id)
                                    .content(
                                        aws_sdk_bedrockruntime::types::ToolResultContentBlock::Text(
                                            content_str.to_string(),
                                        ),
                                    );
                            if block.get("is_error").and_then(|v| v.as_bool()) == Some(true) {
                                builder = builder
                                    .status(aws_sdk_bedrockruntime::types::ToolResultStatus::Error);
                            }
                            Some(ContentBlock::ToolResult(builder.build().ok()?))
                        }
                        _ => None,
                    }
                })
                .collect();
            if blocks.is_empty() {
                return None;
            }
            Message::builder()
                .role(conv_role)
                .set_content(Some(blocks))
                .build()
                .ok()
        })
        .collect()
}
