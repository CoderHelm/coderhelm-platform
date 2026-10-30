//! Model provider: the Anthropic API with the team's own key, or, when the
//! deployment uses Amazon Bedrock (`common::model_provider`), Bedrock with
//! the deployment's own AWS role and no key.

use tracing::info;

use crate::models::TokenUsage;

use super::anthropic::{self, AnthropicClient};
use super::llm::{ConverseOptions, ToolDefinition, ToolExecutor};

/// Team's Anthropic model provider settings.
#[derive(Debug, Clone)]
pub struct ModelProvider {
    pub api_key: String,
    pub primary_model: String,
    pub heavy_model: String,
    /// The team chose its heavy model in Settings. Then every heavy route
    /// runs on it; otherwise the routes measured to hold their quality on a
    /// lighter model (`MEASURED_LIGHT_MODEL`) run there.
    pub heavy_model_chosen: bool,
    /// Counts this team's model calls (set by `load_for_team`). Required:
    /// there is no provider, so no client, whose calls go uncounted.
    pub meter: common::usage::UsageMeter,
    /// The team, run, repository and person every loop on this provider is
    /// gated for (set by `load_for_team` and [`ModelProvider::for_run`]).
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
        // The install's provider as it is now (Settings, System, Models).
        common::model_provider::refresh(dynamo, settings_table).await;
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
        let heavy_saved = item.get("heavy_model").and_then(|v| v.as_s().ok());
        let heavy_model_chosen = heavy_saved.is_some_and(|m| !m.trim().is_empty());
        let heavy_model = heavy_saved
            .filter(|m| !m.trim().is_empty())
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
            heavy_model_chosen,
            meter: common::usage::UsageMeter::dynamo(dynamo.clone(), &analytics_table, team_id),
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

    /// The reviewer's reading and writing of a review (`generate_review`).
    /// The critic, which decides which findings are kept and so what blocks,
    /// stays on [`Self::heavy_model_id`].
    pub fn review_model_id(&self) -> &str {
        self.heavy_route(Route::ReviewWriting)
    }

    /// The coder's model for a ticket of this complexity: simple tickets on
    /// the primary model, medium on the measured route, complex on heavy.
    pub fn implement_model_id(&self, complexity: &str) -> &str {
        match complexity {
            "simple" => self.primary_model_id(),
            "medium" => self.heavy_route(Route::MediumImplement),
            _ => self.heavy_model_id(),
        }
    }

    /// A heavy route's model: the team's own heavy model when it chose one,
    /// else the lighter model the route was measured on.
    fn heavy_route(&self, route: Route) -> &str {
        if self.heavy_model_chosen {
            return self.heavy_model_id();
        }
        match route {
            Route::ReviewWriting | Route::MediumImplement => MEASURED_LIGHT_MODEL,
        }
    }

    /// Get the Anthropic API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// A client whose every response is counted for this team, on the
    /// install's provider as it is now. A run that started on Bedrock with
    /// no team key loaded keeps Bedrock after a switch to Anthropic keys, so
    /// a switch never breaks a run under way.
    pub fn client(&self) -> AnthropicClient {
        let meter = self.meter.clone();
        let bedrock = common::model_provider::any_bedrock_client();
        match pick_client(
            common::model_provider::is_bedrock(),
            bedrock.is_some(),
            !self.api_key.is_empty(),
        ) {
            ClientKind::Bedrock => match bedrock {
                Some(b) => AnthropicClient::bedrock(b.clone(), meter),
                None => AnthropicClient::new(self.api_key.clone(), meter),
            },
            ClientKind::Anthropic => AnthropicClient::new(self.api_key.clone(), meter),
        }
    }
}

/// The model the routes below were measured on and held their quality
/// (`passes/model_route_eval.rs`, `reviewer-evals.yml` route suites):
/// review writing with the heavy critic kept 40/40 planted defects and
/// blocked 55/56 cases right against 40/40 and 53/56 on the heavy model;
/// medium tickets passed 20/20 on both.
pub const MEASURED_LIGHT_MODEL: &str = "claude-sonnet-5-5";

/// A heavy route that may run on [`MEASURED_LIGHT_MODEL`].
#[derive(Debug, Clone, Copy)]
enum Route {
    ReviewWriting,
    MediumImplement,
}

#[derive(Debug, PartialEq, Eq)]
enum ClientKind {
    Bedrock,
    Anthropic,
}

/// Which client a call gets: the provider in force, except that a run with
/// no team key stays on Bedrock (it started there before a switch).
fn pick_client(bedrock_now: bool, have_bedrock: bool, have_key: bool) -> ClientKind {
    if have_bedrock && (bedrock_now || !have_key) {
        ClientKind::Bedrock
    } else {
        ClientKind::Anthropic
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
    // A switch in Settings, System, Models reaches the next loop.
    common::model_provider::refresh(&state.dynamo, &state.config.settings_table_name).await;
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
    state: &crate::WorkerState,
    provider: &ModelProvider,
    model_id: &str,
    system_prompt: &str,
    user_message: &str,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    common::model_provider::refresh(&state.dynamo, &state.config.settings_table_name).await;
    let client = provider.client();
    anthropic::converse_simple(&client, model_id, system_prompt, user_message, usage).await
}

#[cfg(test)]
mod client_tests {
    use super::*;

    #[test]
    fn a_switch_never_strands_a_run() {
        // Bedrock now: Bedrock.
        assert_eq!(pick_client(true, true, false), ClientKind::Bedrock);
        assert_eq!(pick_client(true, true, true), ClientKind::Bedrock);
        // Switched to Anthropic keys: the team's key when it has one...
        assert_eq!(pick_client(false, true, true), ClientKind::Anthropic);
        // ...and a run that started on Bedrock without a key finishes there.
        assert_eq!(pick_client(false, true, false), ClientKind::Bedrock);
        // No Bedrock client at all: the Anthropic API.
        assert_eq!(pick_client(false, false, true), ClientKind::Anthropic);
    }
    fn provider(heavy: &str, chosen: bool) -> ModelProvider {
        ModelProvider {
            api_key: String::new(),
            primary_model: "claude-haiku-4-5".into(),
            heavy_model: heavy.into(),
            heavy_model_chosen: chosen,
            meter: common::usage::UsageMeter::memory("t").0,
            scope: Default::default(),
        }
    }

    #[test]
    fn measured_routes_run_light_unless_the_team_chose_its_heavy_model() {
        let default = provider("claude-opus-5-5", false);
        assert_eq!(default.review_model_id(), MEASURED_LIGHT_MODEL);
        assert_eq!(default.implement_model_id("medium"), MEASURED_LIGHT_MODEL);
        // Unmeasured routes keep their models: the critic and complex
        // tickets on heavy, simple tickets on primary.
        assert_eq!(default.heavy_model_id(), "claude-opus-5-5");
        assert_eq!(default.implement_model_id("complex"), "claude-opus-5-5");
        assert_eq!(default.implement_model_id("simple"), "claude-haiku-4-5");

        // A team that set its heavy model keeps it everywhere.
        let chosen = provider("claude-opus-4-8", true);
        assert_eq!(chosen.review_model_id(), "claude-opus-4-8");
        assert_eq!(chosen.implement_model_id("medium"), "claude-opus-4-8");
        assert_eq!(chosen.implement_model_id("complex"), "claude-opus-4-8");
        assert_eq!(chosen.implement_model_id("simple"), "claude-haiku-4-5");
        // The measured model is one this release calls.
        assert!(common::model_ids::is_selectable(MEASURED_LIGHT_MODEL));
    }
}
