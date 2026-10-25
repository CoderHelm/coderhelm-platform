//! Model provider: the Anthropic API with the team's own key, or, when the
//! deployment uses Amazon Bedrock (`common::model_provider`), Bedrock with
//! the deployment's own AWS role and no key.

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
    /// Counts this team's model calls (set by `load_for_team`).
    #[serde(skip)]
    pub meter: Option<common::usage::UsageMeter>,
    /// The team, run, repository and person every loop on this provider is
    /// gated for (set by `load_for_team` and [`ModelProvider::for_run`]).
    #[serde(skip)]
    pub scope: super::policy::PolicyScope,
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

        let bedrock = common::model_provider::is_bedrock();
        let empty = std::collections::HashMap::new();
        let item = match result.item() {
            Some(item) => item,
            // On Bedrock a team needs no settings: the defaults apply.
            None if bedrock => &empty,
            None => return Err("No model provider configured. Please set up your Anthropic API key in Settings → Model Provider.".into()),
        };

        let api_key = item
            .get("api_key")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        if api_key.is_empty() && !bedrock {
            return Err("Anthropic API key is not configured. Please add your API key in Settings → Model Provider.".into());
        }

        // A retired saved id runs as its replacement (Anthropic refuses it).
        let primary_model = item
            .get("primary_model")
            .and_then(|v| v.as_s().ok())
            .map(|m| common::model_ids::current(m).to_string())
            .unwrap_or_else(|| "claude-sonnet-5-5".to_string());
        let heavy_model = item
            .get("heavy_model")
            .and_then(|v| v.as_s().ok())
            .map(|m| common::model_ids::current(m).to_string())
            .unwrap_or_else(|| "claude-opus-5-5".to_string());

        info!(team_id, "Loaded Anthropic provider");
        let analytics_table = std::env::var("ANALYTICS_TABLE_NAME").unwrap_or_default();
        if analytics_table.is_empty() {
            tracing::warn!(
                team_id,
                "ANALYTICS_TABLE_NAME unset: model usage won't be counted"
            );
        }
        Ok(Self {
            api_key,
            primary_model,
            heavy_model,
            meter: Some(common::usage::UsageMeter::dynamo(
                dynamo.clone(),
                &analytics_table,
                team_id,
            )),
            scope: super::policy::PolicyScope {
                team: team_id.to_string(),
                ..Default::default()
            },
        })
    }

    /// The run this provider works for: its id, repository (`owner/name`),
    /// the person who asked (when one did) and what the run is about. Every
    /// loop's decisions and approval cards are tied to it.
    pub fn for_run(mut self, run_id: &str, repo: &str, person: &str, task: &str) -> Self {
        self.scope.run_id = run_id.to_string();
        self.scope.repo = repo.to_string();
        self.scope.person = person.to_string();
        self.scope.task = task.to_string();
        self
    }

    /// Work that isn't a run (a pull request review), by a stable name.
    pub fn in_thread(mut self, thread: &str) -> Self {
        self.scope.thread = thread.to_string();
        self
    }

    /// The agent a whole run belongs to (a fix started by an alert or a log
    /// finding), so switching that agent off stops its runs.
    pub fn with_origin(mut self, agent: Option<common::agent_policy::Agent>) -> Self {
        self.scope.origin = agent;
        self
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

    /// A client whose every response is counted for this team.
    pub fn client(&self) -> AnthropicClient {
        let meter = self.meter.clone().unwrap_or_else(|| {
            tracing::warn!("Model provider without a usage meter; usage stays uncounted");
            common::usage::UsageMeter::memory("").0
        });
        match common::model_provider::bedrock_client() {
            Some(bedrock) => AnthropicClient::bedrock(bedrock.clone(), meter),
            None => AnthropicClient::new(self.api_key.clone(), meter),
        }
    }
}

/// Converse with tool use loop via Anthropic Messages API.
#[allow(clippy::too_many_arguments)]
pub async fn converse(
    state: &crate::WorkerState,
    provider: &ModelProvider,
    agent: common::agent_policy::Agent,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<(String, Vec<serde_json::Value>)>,
    tools: &[ToolDefinition],
    tool_executor: &dyn ToolExecutor,
    usage: &mut TokenUsage,
    opts: ConverseOptions,
    on_tool_call: Option<&super::llm::OnToolCall>,
    conversation_log: Option<&mut Vec<serde_json::Value>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = provider.client();
    // Every call this loop makes is asked of the policy gate first.
    let gate = super::policy::gate(state, provider.scope.ctx(agent), opts.deadline)
        .offering(tools.iter().map(|t| t.name.clone()));
    anthropic::converse_tool_loop(
        &client,
        model_id,
        system_prompt,
        messages,
        tools,
        tool_executor,
        &gate,
        usage,
        opts.max_turns,
        opts.max_tokens,
        opts.deadline,
        opts.effort,
        on_tool_call,
        conversation_log,
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
