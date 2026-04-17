//! Model provider — Anthropic direct API is the only supported LLM provider.
//! Teams must configure their own Anthropic API key.

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::models::TokenUsage;

use super::anthropic::{self, AnthropicClient};
use super::llm::{ConverseOptions, ToolDefinition, ToolExecutor};

/// Team's Anthropic model provider settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelProvider {
    pub api_key: String,
    pub primary_model: String,
    pub heavy_model: String,
}

impl ModelProvider {
    /// Load team's Anthropic API key and model settings from DynamoDB.
    /// Returns an error if no API key is configured — teams must set up their key first.
    pub async fn load_for_team(
        dynamo: &aws_sdk_dynamodb::Client,
        settings_table: &str,
        team_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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
            .await
            .map_err(|e| format!("Failed to load model provider settings: {e}"))?;

        let item = result
            .item()
            .ok_or("No model provider configured. Please set up your Anthropic API key in Settings → Model Provider.")?;

        let api_key = item
            .get("api_key")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        if api_key.is_empty() {
            return Err("Anthropic API key is not configured. Please add your API key in Settings → Model Provider.".into());
        }

        let primary_model = item
            .get("primary_model")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "claude-sonnet-4-6".to_string());
        let heavy_model = item
            .get("heavy_model")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "claude-opus-4-7".to_string());

        info!(team_id, "Loaded Anthropic provider");
        Ok(Self {
            api_key,
            primary_model,
            heavy_model,
        })
    }

    /// Get the model ID for "light" tasks (analysis, feedback, etc.)
    pub fn primary_model_id(&self) -> &str {
        &self.primary_model
    }

    /// Get the model ID for "heavy" tasks (implementation)
    pub fn heavy_model_id(&self) -> &str {
        &self.heavy_model
    }

    /// Get the Anthropic API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    fn client(&self) -> AnthropicClient {
        AnthropicClient::new(self.api_key.clone())
    }
}

/// Converse with tool use loop via Anthropic Messages API.
#[allow(clippy::too_many_arguments)]
pub async fn converse(
    _state: &crate::WorkerState,
    provider: &ModelProvider,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<(String, Vec<serde_json::Value>)>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
    opts: ConverseOptions,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = provider.client();
    anthropic::converse_tool_loop(
        &client,
        model_id,
        system_prompt,
        messages,
        tools,
        tool_executor,
        usage,
        opts.max_turns,
        opts.max_tokens,
    )
    .await
}

/// Simple one-shot converse (no tools) via Anthropic Messages API.
pub async fn converse_simple(
    _state: &crate::WorkerState,
    provider: &ModelProvider,
    model_id: &str,
    system_prompt: &str,
    user_message: &str,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = provider.client();
    anthropic::converse_simple(&client, model_id, system_prompt, user_message, usage).await
}
