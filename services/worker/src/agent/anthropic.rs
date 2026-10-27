//! Anthropic Messages API client for Claude calls.
//! Teams call the Anthropic API with their own key. The same Messages
//! requests can also go through Amazon Bedrock with the caller's AWS
//! credentials (no API key): the nightly reviewer evals use that, so CI holds
//! no long-lived model key.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::TokenUsage;

static ANTHROPIC_VERSION: &str = "2023-06-01";

/// Reusable Anthropic client.
pub struct AnthropicClient {
    http: Client,
    api_key: String,
    /// Set when requests go through Amazon Bedrock instead of the Anthropic
    /// API (the model id is then a Bedrock model or inference profile id).
    bedrock: Option<aws_sdk_bedrockruntime::Client>,
    /// Counts every response into the team's analytics counters. Required:
    /// a client can't be built without saying where its usage goes.
    meter: common::usage::UsageMeter,
    /// Another Messages endpoint (tests: a local fake server).
    url: Option<String>,
}

impl AnthropicClient {
    pub fn new(api_key: String, meter: common::usage::UsageMeter) -> Self {
        Self {
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| Client::new()),
            api_key,
            bedrock: None,
            meter,
            url: None,
        }
    }

    /// The same client against another Messages endpoint (tests).
    #[cfg(test)]
    fn at(mut self, url: &str) -> Self {
        self.url = Some(url.to_string());
        self
    }

    /// The same Messages requests, sent through Amazon Bedrock with the AWS
    /// credentials of `client` (no API key).
    pub fn bedrock(
        client: aws_sdk_bedrockruntime::Client,
        meter: common::usage::UsageMeter,
    ) -> Self {
        Self {
            bedrock: Some(client),
            ..Self::new(String::new(), meter)
        }
    }
}

// --- Request/Response types ---

#[derive(Serialize)]
struct MessagesRequest {
    model: String,
    max_tokens: i32,
    system: Vec<SystemBlock>,
    messages: Vec<ApiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ApiTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<CacheControl>,
    /// Always-thinking models only (see `ModelFamily`): adaptive thinking with
    /// an explicit preserved-thinking binding behavior.
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_config: Option<Value>,
    /// Server-side context editing — replaces client-side history edits on
    /// always-thinking models, where editing history invalidates thinking.
    #[serde(skip_serializing_if = "Option::is_none")]
    context_management: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fallbacks: Option<Value>,
    /// `{"type":"none"}` on the one answer-now turn after the step budget is
    /// used: tools stay declared (the history has tool calls) but can't be called.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<Value>,
    /// Sent only where the model takes it: always-thinking models refuse any
    /// sampling value but the default (see [`deterministic_temperature`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

/// Temperature 0 for a call that should answer the same way every time, on
/// models that accept it. Always-thinking models (Opus 5.x, Sonnet 5.x,
/// Fable) return a 400 for any non-default sampling value, so they get none
/// and their answer is held steady by the checks after it instead.
fn deterministic_temperature(model_id: &str) -> Option<f32> {
    match ModelFamily::of(model_id) {
        ModelFamily::Legacy => Some(0.0),
        ModelFamily::AlwaysThinking => None,
    }
}

/// How a model must be called. `AlwaysThinking` models (Opus 5.x, Sonnet 5.x,
/// Fable) think on every request and reject `budget_tokens`, disabled thinking
/// and edited history, so they get adaptive thinking, an explicit effort and
/// server-side context editing. Everything else keeps the legacy request shape.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ModelFamily {
    Legacy,
    AlwaysThinking,
}

impl ModelFamily {
    fn of(model_id: &str) -> Self {
        if common::is_always_thinking_model(model_id) {
            ModelFamily::AlwaysThinking
        } else {
            ModelFamily::Legacy
        }
    }
}

/// Models that accept the server-side `fallbacks: "default"` refusal fallback.
fn supports_default_fallback(model_id: &str) -> bool {
    matches!(
        model_id,
        "claude-opus-5-5" | "claude-opus-5" | "claude-fable-5-1" | "claude-sonnet-5-5"
    )
}

const BETA_PROMPT_CACHING: &str = "prompt-caching-2024-07-31";
const BETA_THINKING_BINDING: &str = "thinking-binding-controls-2026-08-01";
const BETA_CONTEXT_MANAGEMENT: &str = "context-management-2025-06-27";
const BETA_FALLBACK_DEFAULT: &str = "server-side-fallback-2026-07-01";

/// Tokens a server-side clear must remove before it happens at all. A clear
/// rewrites the cached conversation from the first cleared tool result on, so
/// with no minimum (the API default) every turn past the trigger cleared one
/// old result near the START of the history and re-wrote nearly the whole
/// context to the cache (1.25x input) instead of reading it (0.05x-0.1x).
const CLEAR_AT_LEAST_TOKENS: u64 = 50_000;
/// Input size at which old tool results start being cleared.
const CLEAR_TRIGGER_TOKENS: u64 = 120_000;
/// Most recent tool results always kept.
const CLEAR_KEEP_TOOL_USES: u64 = 4;

/// Legacy models trim history client-side; they do it once every this many turns.
const LEGACY_COMPACT_EVERY_TURNS: usize = 6;

fn clear_tool_uses_edit() -> Value {
    json!({"edits": [{
        "type": "clear_tool_uses_20250919",
        "trigger": {"type": "input_tokens", "value": CLEAR_TRIGGER_TOKENS},
        "keep": {"type": "tool_uses", "value": CLEAR_KEEP_TOOL_USES},
        "clear_at_least": {"type": "input_tokens", "value": CLEAR_AT_LEAST_TOKENS}
    }]})
}

/// The top-level (automatic) breakpoint for a tool loop turn: only when
/// another turn may read it back.
fn tail_cache(another_turn_may_follow: bool) -> Option<CacheControl> {
    another_turn_may_follow.then(|| CacheControl {
        r#type: "ephemeral".to_string(),
    })
}

/// Fill the model-family fields of a request. `effort` None → "high".
fn apply_model_family(request: &mut MessagesRequest, effort: Option<&str>, tool_loop: bool) {
    if ModelFamily::of(&request.model) != ModelFamily::AlwaysThinking {
        return;
    }
    // drop_block: a history edit degrades (the API drops the stale thinking
    // blocks) instead of failing the request with a 400.
    request.thinking = Some(json!({
        "type": "adaptive",
        "block_binding": {"prefix_mismatch_behavior": "drop_block"}
    }));
    request.output_config = Some(json!({"effort": effort.unwrap_or("high")}));
    if tool_loop {
        request.context_management = Some(clear_tool_uses_edit());
    }
    if supports_default_fallback(&request.model) {
        request.fallbacks = Some(json!("default"));
    }
}

fn beta_header(request: &MessagesRequest) -> String {
    let mut betas = vec![BETA_PROMPT_CACHING];
    if request.thinking.is_some() {
        betas.push(BETA_THINKING_BINDING);
    }
    if request.context_management.is_some() {
        betas.push(BETA_CONTEXT_MANAGEMENT);
    }
    if request.fallbacks.is_some() {
        betas.push(BETA_FALLBACK_DEFAULT);
    }
    betas.join(",")
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum SystemBlock {
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
}

#[derive(Serialize, Clone)]
struct CacheControl {
    r#type: String,
}

#[derive(Serialize, Clone)]
struct ApiMessage {
    role: String,
    /// Raw blocks, sent back exactly as received: thinking/fallback blocks must
    /// be echoed unchanged, so history is never round-tripped through a typed enum.
    content: Vec<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image { source: ImageSource },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
enum ImageSource {
    #[serde(rename = "base64")]
    Base64 { media_type: String, data: String },
}

#[derive(Clone, Serialize)]
struct ApiTool {
    name: String,
    description: String,
    input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<CacheControl>,
}

#[derive(Deserialize, Debug)]
struct MessagesResponse {
    /// Raw blocks — includes `thinking` / `fallback` blocks the typed
    /// `ContentBlock` doesn't model; see `blocks()` for the typed view.
    content: Vec<Value>,
    stop_reason: Option<String>,
    #[serde(default)]
    stop_details: Option<Value>,
    usage: ApiUsage,
}

impl MessagesResponse {
    /// Typed view of the blocks the loop acts on (text, tool_use); other block
    /// types are skipped here but kept verbatim in `content`.
    fn blocks(&self) -> Vec<ContentBlock> {
        self.content
            .iter()
            .filter_map(|b| serde_json::from_value(b.clone()).ok())
            .collect()
    }

    /// A safety-classifier decline (HTTP 200, `stop_reason: "refusal"`).
    fn refusal(&self) -> Option<String> {
        if self.stop_reason.as_deref() != Some("refusal") {
            return None;
        }
        let category = self
            .stop_details
            .as_ref()
            .and_then(|d| d.get("category"))
            .and_then(|c| c.as_str())
            .unwrap_or("unspecified");
        Some(format!(
            "The model declined this request (refusal, category: {category})."
        ))
    }
}

/// The usage a response carries, for logs. What is counted (the team's
/// counters and the run's totals) comes from the stream, per attempt
/// (`common::model_stream`), never from here.
#[derive(Deserialize, Debug)]
struct ApiUsage {
    /// Uncached input only.
    input_tokens: u64,
    /// Thinking included (billed as output).
    output_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
}

// --- Public API ---

use super::llm::ToolDefinition;

/// One-shot converse call (no tool loop). Equivalent to `converse_with_retry` for Bedrock.
pub async fn converse_simple(
    client: &AnthropicClient,
    model_id: &str,
    system: &str,
    user_message: &str,
    usage: &mut crate::models::TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut request = MessagesRequest {
        model: model_id.to_string(),
        max_tokens: 16384,
        system: vec![SystemBlock::Text {
            text: system.to_string(),
            cache_control: Some(CacheControl {
                r#type: "ephemeral".to_string(),
            }),
        }],
        messages: vec![ApiMessage {
            role: "user".to_string(),
            content: vec![json!({
                "type": "text",
                "text": user_message,
            })],
        }],
        tools: None,
        cache_control: None,
        thinking: None,
        output_config: None,
        context_management: None,
        fallbacks: None,
        tool_choice: None,
        temperature: None,
    };
    apply_model_family(&mut request, None, false);

    let resp = send_request(client, &request, usage).await?;
    if let Some(refusal) = resp.refusal() {
        return Err(refusal.into());
    }
    extract_text(&resp.blocks())
}

/// The model check's test call: one token out, recorded on the client's
/// meter like any call. `Ok` when the model answered (a reply cut at the
/// one-token limit is an answer).
pub async fn probe(
    client: &AnthropicClient,
    model_id: &str,
    usage: &mut crate::models::TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = MessagesRequest {
        model: model_id.to_string(),
        max_tokens: 1,
        system: vec![],
        messages: vec![ApiMessage {
            role: "user".to_string(),
            content: vec![json!({ "type": "text", "text": "Reply with OK." })],
        }],
        tools: None,
        cache_control: None,
        thinking: None,
        output_config: None,
        context_management: None,
        fallbacks: None,
        tool_choice: None,
        temperature: None,
    };
    send_request(client, &request, usage).await.map(|_| ())
}

/// One call whose answer is a JSON object matching `schema`, parsed. With
/// `constrained`, the API holds the answer to the schema (structured
/// outputs); without, the schema is in the instructions and the answer's
/// JSON object is parsed from its text. No tools. Counted like every other
/// call.
#[allow(clippy::too_many_arguments)]
pub async fn converse_json(
    client: &AnthropicClient,
    model_id: &str,
    system: &str,
    user_message: &str,
    schema: &Value,
    constrained: bool,
    effort: &str,
    max_tokens: i32,
    usage: &mut crate::models::TokenUsage,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    converse_json_shared(
        client,
        model_id,
        system,
        "",
        user_message,
        schema,
        constrained,
        effort,
        max_tokens,
        usage,
    )
    .await
}

/// [`converse_json`] for one of many calls that open their user message the
/// same way (`shared`: an inventory or catalogue that is the same for every
/// call of a batch). `shared` goes first with its own cache breakpoint and
/// `user_message`, this call's own text, after it, so every call after the
/// first reads the system prompt, the schema and `shared` from the cache.
#[allow(clippy::too_many_arguments)]
pub async fn converse_json_shared(
    client: &AnthropicClient,
    model_id: &str,
    system: &str,
    shared: &str,
    user_message: &str,
    schema: &Value,
    constrained: bool,
    effort: &str,
    max_tokens: i32,
    usage: &mut crate::models::TokenUsage,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let request = json_request(
        model_id,
        system,
        shared,
        user_message,
        schema,
        constrained,
        effort,
        max_tokens,
    );
    let resp = send_request(client, &request, usage).await?;
    let kinds: Vec<&str> = resp
        .content
        .iter()
        .filter_map(|b| b.get("type").and_then(|t| t.as_str()))
        .collect();
    info!(
        model = model_id,
        stop_reason = resp.stop_reason.as_deref().unwrap_or(""),
        blocks = ?kinds,
        output_tokens = resp.usage.output_tokens,
        "Structured answer received"
    );
    use common::model_retry::{ModelCallError, ModelErrorKind};
    if let Some(refusal) = resp.refusal() {
        return Err(Box::new(ModelCallError::new(
            ModelErrorKind::Refusal,
            refusal,
        )));
    }
    if resp.stop_reason.as_deref() == Some("max_tokens") {
        return Err(Box::new(ModelCallError::new(
            ModelErrorKind::Truncated,
            "the answer was cut off before it finished",
        )));
    }
    let text = extract_text(&resp.blocks())?;
    serde_json::from_str(json_object(&text)).map_err(|e| {
        Box::new(ModelCallError::new(
            ModelErrorKind::BadOutput,
            format!("the answer wasn't valid JSON: {e}"),
        )) as Box<dyn std::error::Error + Send + Sync>
    })
}

/// The request [`converse_json_shared`] sends. Everything up to the second
/// cache breakpoint (the system prompt with the schema, then `shared`) is
/// the same bytes for every call with the same `system`, `shared`, schema
/// and settings; only `user` differs, after it. With no `shared`, the user
/// message is one block and only the system prompt is cached.
#[allow(clippy::too_many_arguments)]
fn json_request(
    model_id: &str,
    system: &str,
    shared: &str,
    user: &str,
    schema: &Value,
    constrained: bool,
    effort: &str,
    max_tokens: i32,
) -> MessagesRequest {
    let mut content = Vec::new();
    if !shared.is_empty() {
        content.push(json!({
            "type": "text",
            "text": shared,
            "cache_control": { "type": "ephemeral" },
        }));
    }
    content.push(json!({ "type": "text", "text": user }));
    let system = if constrained {
        system.to_string()
    } else {
        format!(
            "{system}\n\nAnswer with only one JSON object that matches this JSON schema, \
             and nothing before or after it:\n{schema}"
        )
    };
    let mut request = MessagesRequest {
        model: model_id.to_string(),
        max_tokens,
        system: vec![SystemBlock::Text {
            text: system,
            cache_control: Some(CacheControl {
                r#type: "ephemeral".to_string(),
            }),
        }],
        messages: vec![ApiMessage {
            role: "user".to_string(),
            content,
        }],
        tools: None,
        cache_control: None,
        thinking: None,
        output_config: None,
        context_management: None,
        fallbacks: None,
        tool_choice: None,
        temperature: None,
    };
    apply_model_family(&mut request, Some(effort), false);
    request.temperature = deterministic_temperature(model_id);
    if constrained {
        request.output_config = Some(json_output_config(request.output_config.take(), schema));
    }
    // A decline is an error to see, not an answer from another model.
    request.fallbacks = None;
    request
}

/// The body [`converse_json_shared`] sends, byte for byte, for tests of
/// what is cached.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn json_request_body(
    model_id: &str,
    system: &str,
    shared: &str,
    user: &str,
    schema: &Value,
    constrained: bool,
    effort: &str,
    max_tokens: i32,
) -> String {
    serde_json::to_string(&json_request(
        model_id,
        system,
        shared,
        user,
        schema,
        constrained,
        effort,
        max_tokens,
    ))
    .expect("a request serializes")
}

/// The JSON object in an answer: from its first `{` to its last `}`, so a
/// code fence or a stray line around it doesn't matter.
fn json_object(text: &str) -> &str {
    match (text.find('{'), text.rfind('}')) {
        (Some(a), Some(b)) if b > a => &text[a..=b],
        _ => text.trim(),
    }
}

/// `output_config` with the JSON schema format added to whatever the model
/// family set (its effort).
fn json_output_config(base: Option<Value>, schema: &Value) -> Value {
    let mut cfg = base.unwrap_or_else(|| json!({}));
    cfg["format"] = json!({ "type": "json_schema", "schema": schema });
    cfg
}

/// Agentic tool-use loop. Equivalent to `converse_with_opts` for Bedrock.
#[allow(clippy::too_many_arguments)]
pub async fn converse_tool_loop(
    client: &AnthropicClient,
    model_id: &str,
    system_prompt: &str,
    messages: &mut Vec<(String, Vec<Value>)>, // (role, content blocks as JSON)
    tools: &[ToolDefinition],
    tool_executor: &dyn super::llm::ToolExecutor,
    gate: &common::agent_policy::Gate,
    usage: &mut TokenUsage,
    max_turns: usize,
    max_tokens: i32,
    deadline: Option<std::time::Instant>,
    effort: Option<&str>,
    on_tool_call: Option<&super::llm::OnToolCall>,
    mut conversation_log: Option<&mut Vec<Value>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let family = ModelFamily::of(model_id);
    let api_tools: Vec<ApiTool> = tools
        .iter()
        .enumerate()
        .map(|(i, t)| ApiTool {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
            // Cache breakpoint on the last tool — caches system + all tool defs
            cache_control: if i == tools.len() - 1 {
                Some(CacheControl {
                    r#type: "ephemeral".to_string(),
                })
            } else {
                None
            },
        })
        .collect();

    let mut turns: usize = 0;
    let mut deadline_warned = false;
    // Set once the step budget is used: one more turn, with tools off, in
    // which the agent must answer from what it already learned. Without it,
    // everything a planner read was thrown away the moment it ran out.
    let mut answer_now = false;

    loop {
        turns += 1;
        if turns > max_turns {
            if answer_now || tools.is_empty() {
                warn!("Hit max turn limit ({max_turns}) and the final answer turn, giving up");
                return Err(format!(
                    "Reached the maximum number of steps ({max_turns}) without finishing. \
                     The issue may need more detail or a narrower scope."
                )
                .into());
            }
            answer_now = true;
            warn!("Hit max turn limit ({max_turns}), asking for the final answer with tools off");
            messages.push((
                "user".to_string(),
                vec![json!({"type": "text", "text":
                    "[SYSTEM NOTE] You have used every tool step. Tools are now off. Write your \
                     final answer NOW, in exactly the format you were asked for, using what you \
                     have already learned. Where something is still unknown, say so in the \
                     answer instead of guessing."
                })],
            ));
        }

        // Deadline check — if <120s remain, tell the LLM to wrap up
        if let Some(dl) = deadline {
            let now = std::time::Instant::now();
            let remaining = if dl > now { (dl - now).as_secs() } else { 0 };
            if remaining < 30 {
                // Hard stop — not enough time even for one more API call
                warn!("Deadline imminent (<30s), forcing exit");
                return Ok("(timed out — partial implementation)".to_string());
            } else if remaining < 120 && !deadline_warned {
                deadline_warned = true;
                warn!(
                    remaining,
                    "Approaching deadline — injecting wrap-up message"
                );
                messages.push((
                    "user".to_string(),
                    vec![json!({"type": "text", "text":
                        "[SYSTEM NOTE — DEADLINE] You are almost out of time. Your completed \
                         edits are ALREADY committed (each edit committed as you made it), so \
                         do NOT dump remaining work now. Above all, do NOT write a stub, \
                         placeholder, or half-finished file to 'save progress' — a partial file \
                         breaks the build and is worse than leaving it unfinished. Stop, and \
                         output a short summary of what you finished and what still needs doing. \
                         This is your LAST turn."
                    })],
                ));
            }
        }

        // Progress note every 10 turns
        if turns > 1 && (turns.is_multiple_of(5) || turns == max_turns - 2) {
            info!(turns, "Injecting progress note at turn {turns}");
            let remaining = max_turns - turns;
            let note = format!(
                "[SYSTEM NOTE] You have used {turns} of {max_turns} tool-use turns. \
                 {remaining} turns remaining. Focus on completing the task efficiently."
            );
            messages.push((
                "user".to_string(),
                vec![json!({"type": "text", "text": note})],
            ));
        }

        // Build API messages from our internal format
        let api_messages: Vec<ApiMessage> = messages
            .iter()
            .map(|(role, content)| ApiMessage {
                role: role.clone(),
                content: content.clone(),
            })
            .collect();

        let mut request = MessagesRequest {
            model: model_id.to_string(),
            max_tokens,
            system: vec![SystemBlock::Text {
                text: system_prompt.to_string(),
                cache_control: Some(CacheControl {
                    r#type: "ephemeral".to_string(),
                }),
            }],
            messages: api_messages,
            tools: if api_tools.is_empty() {
                None
            } else {
                Some(api_tools.clone())
            },
            // Auto-cache: caches the entire prefix (tools + system + messages)
            // up to the last cacheable block, so turn N+1 reads turns 1..N.
            // Only when a turn N+1 can follow: on a call that is the last
            // (no tools, or the answer-now turn) the tail would be written
            // at 1.25x input and never read. The system and tool breakpoints
            // still cache what other calls share.
            cache_control: tail_cache(!api_tools.is_empty() && !answer_now),
            thinking: None,
            output_config: None,
            context_management: None,
            fallbacks: None,
            tool_choice: answer_now.then(|| json!({"type": "none"})),
            temperature: None,
        };
        apply_model_family(&mut request, effort, true);

        let response = send_request(client, &request, usage).await?;

        if let Some(refusal) = response.refusal() {
            return Err(refusal.into());
        }

        // Context compaction — aggressively clear old tool results every turn.
        // The model has already consumed them; keeping them in history is pure token waste.
        // Keep the last `keep_recent` turn-pairs uncompacted so the model has recent context.
        // Always-thinking models: history must stay append-only (an edit
        // invalidates every later thinking block), so the server clears old
        // tool results instead (`context_management` in apply_model_family).
        // Batched: each compaction edits history and so re-writes the cache
        // from the first edited block on. Compacting every turn re-wrote the
        // last ~8 messages on every turn; every 6th turn writes them once.
        let keep_recent = 4; // keep last 4 turn-pairs (~8 messages)
        if family == ModelFamily::Legacy && turns.is_multiple_of(LEGACY_COMPACT_EVERY_TURNS) {
            compact_messages(messages, keep_recent);
        }

        // Emergency compaction: if context is huge despite per-turn compaction, drop more
        let input_tokens = response.usage.input_tokens;
        let model_limit: u64 = 200_000;
        let context_pct = input_tokens as f64 / model_limit as f64;
        if family == ModelFamily::AlwaysThinking {
            // server-side context editing owns trimming
        } else if context_pct > 0.60 {
            info!(
                "Context at {:.0}%, emergency compaction",
                context_pct * 100.0
            );
            compact_messages(messages, 2);
        } else if context_pct > 0.40 {
            info!(
                "Context at {:.0}%, aggressive compaction",
                context_pct * 100.0
            );
            compact_messages(messages, 3);
        }

        // Keep the response blocks verbatim (thinking signatures must round-trip).
        messages.push(("assistant".to_string(), response.content.clone()));

        // Extract tool uses
        let blocks = response.blocks();
        let tool_uses: Vec<_> = blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolUse { id, name, input } => {
                    Some((id.clone(), name.clone(), input.clone()))
                }
                _ => None,
            })
            .collect();

        if tool_uses.is_empty() || response.stop_reason.as_deref() == Some("end_turn") {
            // Log the final assistant response
            if let Some(ref mut log) = conversation_log {
                log.push(json!({
                    "turn": turns,
                    "role": "assistant",
                    "content": response.content,
                    "usage": {
                        "input_tokens": response.usage.input_tokens,
                        "output_tokens": response.usage.output_tokens,
                        "cache_read": response.usage.cache_read_input_tokens,
                        "cache_write": response.usage.cache_creation_input_tokens
                    },
                    "stop_reason": response.stop_reason
                }));
            }
            return extract_text(&blocks);
        }

        // What the agent said before its calls: the "why" on an approval card.
        let why: String = blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        // Execute tools, each only after the policy gate allows it.
        let mut tool_results: Vec<Value> = Vec::new();
        for (tool_id, tool_name, tool_input) in &tool_uses {
            let tool_start = std::time::Instant::now();
            info!(tool = %tool_name, "Executing tool");

            let ran = match super::policy::gated_execute(
                gate,
                tool_executor,
                tool_name,
                tool_input,
                common::truncate_str(why.trim(), 500),
            )
            .await
            {
                super::policy::Gated::Ran(r) => r,
                super::policy::Gated::Refused(message) => {
                    if let Some(cb) = on_tool_call {
                        cb(tool_name, 0, &truncate_input_summary(tool_input), true);
                    }
                    info!(tool = %tool_name, "Tool not run: the policy gate said no");
                    tool_results.push(json!({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": message,
                        "is_error": true
                    }));
                    continue;
                }
                super::policy::Gated::Stop(stop) => {
                    warn!(tool = %tool_name, "Agent loop stopped by the policy gate");
                    return Err(Box::new(common::agent_policy::AgentStopped(stop)));
                }
            };
            match ran {
                Ok(result) => {
                    let duration_ms = tool_start.elapsed().as_millis() as u64;
                    usage.record_tool_call(tool_name, duration_ms);
                    if let Some(cb) = on_tool_call {
                        let input_summary = truncate_input_summary(tool_input);
                        cb(tool_name, duration_ms, &input_summary, false);
                    }
                    info!(tool = %tool_name, duration_ms, "Tool completed");
                    tool_results.push(json!({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": serde_json::to_string(&result)?
                    }));
                }
                Err(e) => {
                    let duration_ms = tool_start.elapsed().as_millis() as u64;
                    usage.record_tool_call(tool_name, duration_ms);
                    if let Some(cb) = on_tool_call {
                        let input_summary = truncate_input_summary(tool_input);
                        cb(tool_name, duration_ms, &input_summary, true);
                    }
                    warn!(tool = %tool_name, error = %e, duration_ms, "Tool execution failed");
                    tool_results.push(json!({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": format!("Error: {e}"),
                        "is_error": true
                    }));
                }
            }
        }

        // Log this turn to the conversation log (before compaction loses it)
        if let Some(ref mut log) = conversation_log {
            // Log assistant response with tool calls
            log.push(json!({
                "turn": turns,
                "role": "assistant",
                "content": response.content,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                    "cache_read": response.usage.cache_read_input_tokens,
                    "cache_write": response.usage.cache_creation_input_tokens
                }
            }));
            // Log tool results (truncate large outputs to 100KB)
            let truncated_results: Vec<Value> = tool_results
                .iter()
                .map(|r| {
                    let mut entry = r.clone();
                    if let Some(content) = entry.get("content").and_then(|c| c.as_str()) {
                        if content.len() > 102_400 {
                            entry["content"] = Value::String(format!(
                                "{}... [truncated, {} bytes total]",
                                common::truncate_str(content, 102_400),
                                content.len()
                            ));
                        }
                    }
                    entry
                })
                .collect();
            log.push(json!({
                "turn": turns,
                "role": "tool_results",
                "content": truncated_results
            }));
        }

        messages.push(("user".to_string(), tool_results));
    }
}

// --- Internal helpers ---

/// Produce a short summary of tool input for live event display.
fn truncate_input_summary(input: &Value) -> String {
    // For common tools, extract the most relevant field
    let summary = if let Some(path) = input.get("path").and_then(|v| v.as_str()) {
        path.to_string()
    } else if let Some(pattern) = input.get("pattern").and_then(|v| v.as_str()) {
        format!("pattern: {pattern}")
    } else if let Some(command) = input.get("command").and_then(|v| v.as_str()) {
        command.chars().take(200).collect()
    } else {
        let s = input.to_string();
        if s.len() > 200 {
            format!("{}…", common::truncate_str(&s, 200))
        } else {
            s
        }
    };
    summary
}

/// Sends one Messages request, streamed, and counts it: every attempt into
/// the team's counters as it happens (`common::model_stream`, a reply that
/// broke off and was retried included), and into the run's own totals
/// (`usage`). Transient failures (any 5xx including the Cloudflare 520-524
/// errors, 408, 429, 529 overloaded, requests that got no response, and
/// replies cut off mid-stream) are retried with backoff by status, not by
/// error text; the error returned after that is final for this call.
async fn send_request(
    client: &AnthropicClient,
    request: &MessagesRequest,
    usage: &mut TokenUsage,
) -> Result<MessagesResponse, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(bedrock) = &client.bedrock {
        return send_bedrock(client, bedrock, request, usage).await;
    }
    let mut body = serde_json::to_value(request)?;
    body["stream"] = json!(true);
    let body = serde_json::to_vec(&body)?;
    let url = client
        .url
        .as_deref()
        .unwrap_or(common::model_stream::MESSAGES_URL);
    let streamed = common::model_stream::send_metered(
        || {
            client
                .http
                .post(url)
                .header("x-api-key", &client.api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .header("anthropic-beta", beta_header(request))
                .header("content-type", "application/json")
                .body(body.clone())
        },
        &common::model_retry::WORKER_BACKOFF_SECS,
        &request.model,
        &client.meter,
        &request.model,
        body.len() as u64,
    )
    .await
    .map_err(model_err)?;
    for broken in &streamed.retried {
        usage.add_call(&request.model, broken);
    }
    usage.add_call(&request.model, &streamed.usage);
    read_message(streamed.message)
}

/// The Bedrock body for a Messages request: the same fields, with the model
/// in the URL instead of the body, Bedrock's version marker, and the betas
/// Bedrock needs (prompt caching needs none there).
fn bedrock_body(request: &MessagesRequest) -> Result<Value, serde_json::Error> {
    let mut body = serde_json::to_value(request)?;
    if let Some(o) = body.as_object_mut() {
        o.remove("model");
        o.insert("anthropic_version".into(), json!("bedrock-2023-05-31"));
        let header = beta_header(request);
        let betas: Vec<&str> = header
            .split(',')
            .filter(|b| !b.is_empty() && *b != BETA_PROMPT_CACHING)
            .collect();
        if !betas.is_empty() {
            o.insert("anthropic_beta".into(), json!(betas));
        }
    }
    Ok(body)
}

/// One Messages request through Amazon Bedrock with the deployment's own
/// role: streamed and metered per attempt exactly like the Anthropic API
/// path (`common::model_stream::send_bedrock_metered`), the limit asked
/// before each attempt. Every Bedrock install's runs go through here.
async fn send_bedrock(
    client: &AnthropicClient,
    bedrock: &aws_sdk_bedrockruntime::Client,
    request: &MessagesRequest,
    usage: &mut TokenUsage,
) -> Result<MessagesResponse, Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::to_vec(&bedrock_body(request)?)?;
    let streamed = common::model_stream::send_bedrock_metered(
        bedrock,
        &common::model_provider::bedrock_model_id(&request.model),
        &body,
        &common::model_retry::WORKER_BACKOFF_SECS,
        &client.meter,
        &request.model,
    )
    .await
    .map_err(model_err)?;
    for broken in &streamed.retried {
        usage.add_call(&request.model, broken);
    }
    usage.add_call(&request.model, &streamed.usage);
    read_message(streamed.message)
}

/// A model call failure as the boxed error callers get, the failure itself
/// in the box (`.map_err(Box::new)?` boxed it twice, so
/// `model_retry::model_error` could not find its kind: a transient 5xx read
/// as final, a refused key as unreachable).
fn model_err(e: common::model_retry::ModelCallError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}

fn read_message(
    message: Value,
) -> Result<MessagesResponse, Box<dyn std::error::Error + Send + Sync>> {
    serde_json::from_value(message).map_err(|e| {
        Box::new(common::model_retry::ModelCallError::new(
            common::model_retry::ModelErrorKind::Network,
            format!("the response couldn't be read: {e}"),
        )) as Box<dyn std::error::Error + Send + Sync>
    })
}

fn extract_text(
    content: &[ContentBlock],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let text = content
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(text)
}

/// Compact old tool results to reclaim context space.
fn compact_messages(messages: &mut [(String, Vec<Value>)], keep_last: usize) {
    let total = messages.len();
    if total <= keep_last * 2 {
        return;
    }
    let cutoff = total.saturating_sub(keep_last * 2);

    for (role, content) in messages[..cutoff].iter_mut() {
        if role != "user" {
            continue;
        }
        for block in content.iter_mut() {
            if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                if let Some(c) = block.get("content").and_then(|c| c.as_str()) {
                    if c.len() > 150 {
                        // Preserve a brief hint of what was in the result
                        let hint = common::truncate_str(c, 60);
                        block["content"] = json!(format!(
                            "[Cleared — {len} chars: {hint}…]",
                            len = c.len(),
                            hint = hint
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn bedrock_body_moves_the_model_out_and_marks_the_version() {
        let request = MessagesRequest {
            model: "us.anthropic.claude-haiku-4-5-20251001-v1:0".into(),
            max_tokens: 64,
            system: vec![],
            messages: vec![],
            tools: None,
            cache_control: None,
            thinking: None,
            output_config: Some(json!({"format": {"type": "json_schema", "schema": {}}})),
            context_management: None,
            fallbacks: None,
            tool_choice: None,
            temperature: Some(0.0),
        };
        let body = super::bedrock_body(&request).unwrap();
        assert!(body.get("model").is_none());
        assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        // Prompt caching needs no beta on Bedrock.
        assert!(body.get("anthropic_beta").is_none());
    }

    use super::*;

    #[test]
    fn the_json_object_is_found_inside_a_fence() {
        assert_eq!(
            json_object("```json\n{\"a\": {\"b\": 1}}\n```"),
            "{\"a\": {\"b\": 1}}"
        );
        assert_eq!(json_object(" {} "), "{}");
        assert_eq!(json_object(" nothing "), "nothing");
        let cfg = json_output_config(
            Some(json!({"effort": "medium"})),
            &json!({"type": "object"}),
        );
        assert_eq!(cfg["effort"], "medium");
        assert_eq!(cfg["format"]["type"], "json_schema");
    }

    fn request(model: &str) -> MessagesRequest {
        MessagesRequest {
            model: model.to_string(),
            max_tokens: 1024,
            system: vec![],
            messages: vec![],
            tools: None,
            cache_control: None,
            thinking: None,
            output_config: None,
            context_management: None,
            fallbacks: None,
            tool_choice: None,
            temperature: None,
        }
    }

    #[test]
    fn structured_answers_ask_for_temperature_zero_only_where_the_model_takes_it() {
        assert_eq!(deterministic_temperature("claude-haiku-4-5"), Some(0.0));
        assert_eq!(deterministic_temperature("claude-sonnet-4-6"), Some(0.0));
        assert_eq!(deterministic_temperature("claude-sonnet-5-5"), None);
        assert_eq!(deterministic_temperature("claude-opus-5-5"), None);
        // Bedrock ids of the same models (the live evals' 400).
        assert_eq!(
            deterministic_temperature("us.anthropic.claude-opus-5-5"),
            None
        );
        assert_eq!(
            deterministic_temperature("global.anthropic.claude-sonnet-5-5"),
            None
        );
        assert_eq!(
            deterministic_temperature("us.anthropic.claude-haiku-4-5-20251001-v1:0"),
            Some(0.0)
        );
        let mut r = request("claude-haiku-4-5");
        r.temperature = deterministic_temperature(&r.model);
        assert_eq!(serde_json::to_value(&r).unwrap()["temperature"], 0.0);
        let mut r = request("claude-sonnet-5-5");
        r.temperature = deterministic_temperature(&r.model);
        assert!(serde_json::to_value(&r)
            .unwrap()
            .get("temperature")
            .is_none());
    }

    #[test]
    fn the_answer_now_turn_keeps_tools_declared_but_off() {
        let mut r = request("claude-opus-5-5");
        let plain = serde_json::to_value(&r).unwrap();
        assert!(plain.get("tool_choice").is_none());
        r.tool_choice = Some(json!({"type": "none"}));
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["tool_choice"], json!({"type": "none"}));
    }

    #[test]
    fn model_family_by_id() {
        for m in [
            "claude-opus-5-5",
            "claude-sonnet-5-5",
            "claude-fable-5-1",
            "claude-opus-5",
        ] {
            assert_eq!(ModelFamily::of(m), ModelFamily::AlwaysThinking, "{m}");
        }
        for m in ["claude-opus-4-8", "claude-sonnet-4-6", "claude-haiku-4-5"] {
            assert_eq!(ModelFamily::of(m), ModelFamily::Legacy, "{m}");
        }
    }

    #[test]
    fn legacy_models_keep_the_old_request_shape() {
        let mut r = request("claude-opus-4-8");
        apply_model_family(&mut r, None, true);
        let v = serde_json::to_value(&r).unwrap();
        for k in [
            "thinking",
            "output_config",
            "context_management",
            "fallbacks",
        ] {
            assert!(v.get(k).is_none(), "{k} must not be sent to a legacy model");
        }
        assert_eq!(beta_header(&r), BETA_PROMPT_CACHING);
    }

    #[test]
    fn thinking_models_get_adaptive_binding_effort_and_server_trimming() {
        let mut r = request("claude-opus-5-5");
        apply_model_family(&mut r, None, true);
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["thinking"]["type"], "adaptive");
        assert_eq!(
            v["thinking"]["block_binding"]["prefix_mismatch_behavior"],
            "drop_block"
        );
        assert_eq!(v["output_config"]["effort"], "high");
        let edit = &v["context_management"]["edits"][0];
        assert_eq!(edit["type"], "clear_tool_uses_20250919");
        // Clears happen in large batches, so the cache isn't re-written every turn.
        assert_eq!(edit["clear_at_least"]["type"], "input_tokens");
        assert!(edit["clear_at_least"]["value"].as_u64().unwrap() >= 20_000);
        assert!(edit["trigger"]["value"].as_u64().unwrap() >= 100_000);
        assert_eq!(v["fallbacks"], "default");
        assert!(v.get("temperature").is_none());
        let betas = beta_header(&r);
        for b in [
            BETA_THINKING_BINDING,
            BETA_CONTEXT_MANAGEMENT,
            BETA_FALLBACK_DEFAULT,
        ] {
            assert!(betas.contains(b), "missing beta {b}");
        }
    }

    #[test]
    fn one_shot_calls_skip_context_editing_and_honor_effort() {
        let mut r = request("claude-sonnet-5-5");
        apply_model_family(&mut r, Some("low"), false);
        assert!(r.context_management.is_none());
        assert_eq!(r.output_config.unwrap()["effort"], "low");
    }

    #[test]
    fn response_keeps_thinking_blocks_verbatim() {
        let raw = json!({
            "content": [
                {"type": "thinking", "thinking": "", "signature": "sig=="},
                {"type": "text", "text": "hi"},
                {"type": "tool_use", "id": "t1", "name": "read_file", "input": {"path": "a"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        });
        let resp: MessagesResponse = serde_json::from_value(raw.clone()).unwrap();
        // History echo is byte-identical, thinking signature included.
        assert_eq!(Value::Array(resp.content.clone()), raw["content"]);
        let blocks = resp.blocks();
        assert_eq!(blocks.len(), 2);
        assert!(matches!(blocks[1], ContentBlock::ToolUse { .. }));
        assert_eq!(extract_text(&blocks).unwrap(), "hi");
        assert!(resp.refusal().is_none());
    }

    /// A whole streamed reply: "hello", with every usage part.
    fn whole_reply() -> String {
        [
            json!({"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,
                "usage":{"input_tokens":7,"output_tokens":1,"cache_read_input_tokens":9000,"cache_creation_input_tokens":1200,
                "cache_creation":{"ephemeral_5m_input_tokens":1000,"ephemeral_1h_input_tokens":200}}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":300}}),
            json!({"type":"message_stop"}),
        ]
        .iter()
        .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
        .collect()
    }

    /// A fake Messages endpoint: each request gets the next streamed body
    /// (the last repeats), and the connection closes after it.
    async fn sse_server(bodies: Vec<String>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut n = 0;
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let body = bodies.get(n).or(bodies.last()).unwrap().clone();
                n += 1;
                let mut buf = vec![0u8; 1 << 16];
                let _ = sock.read(&mut buf).await;
                let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://{addr}/v1/messages")
    }

    #[tokio::test]
    async fn a_broken_reply_retried_counts_both_attempts_in_the_run_and_the_team() {
        let broken = whole_reply()
            .split("event: content_block_stop")
            .next()
            .unwrap()
            .to_string();
        let url = sse_server(vec![broken, whole_reply()]).await;
        let (meter, calls) = common::usage::UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let mut usage = TokenUsage::default();
        let mut request = request("claude-opus-5-5");
        request.max_tokens = 100;
        // The worker's backoff waits 2s before the retry.
        let resp = send_request(&client, &request, &mut usage).await.unwrap();
        assert_eq!(resp.stop_reason.as_deref(), Some("end_turn"));
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 2);
        assert!(got[0].estimated);
        // "hello" is 5 bytes: 2 tokens estimated.
        assert_eq!(got[0].usage.output, 2);
        let team_total: u64 = got.iter().map(|c| c.usage.total()).sum();
        assert_eq!(team_total, usage.total_tokens());
    }

    #[tokio::test]
    async fn each_response_reaches_the_run_and_the_team_counters_exactly_once() {
        let url = sse_server(vec![whole_reply()]).await;
        let (meter, calls) = common::usage::UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let mut usage = TokenUsage::default();
        common::usage::with_feature(common::usage::feature::RUN, async {
            for model in ["claude-opus-5-5", "claude-sonnet-5-5"] {
                let text = converse_simple(&client, model, "sys", "hi", &mut usage)
                    .await
                    .unwrap();
                assert_eq!(text, "hello");
            }
        })
        .await;
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 2, "one team record per response");
        let team_total: u64 = got.iter().map(|c| c.usage.total()).sum();
        assert_eq!(team_total, usage.total_tokens());
        assert_eq!(usage.total_tokens(), 2 * (7 + 300 + 9000 + 1200));
        assert_eq!(usage.cache_write_1h_tokens, 400);
        let team_cost: f64 = got.iter().map(|c| c.usage.cost_usd(&c.model)).sum();
        assert!((team_cost - usage.cost_usd).abs() < 1e-12);
        assert!(got
            .iter()
            .all(|c| c.feature == "run" && c.team_id == "team-1"));
    }

    #[test]
    fn refusal_is_an_error_not_empty_text() {
        let resp: MessagesResponse = serde_json::from_value(json!({
            "content": [],
            "stop_reason": "refusal",
            "stop_details": {"type": "refusal", "category": "cyber"},
            "usage": {"input_tokens": 1, "output_tokens": 0}
        }))
        .unwrap();
        assert!(resp.refusal().unwrap().contains("cyber"));
    }
}

/// What a team is billed and what CoderHelm counts, reconciled end to end
/// on a fake Messages endpoint: a tool loop's every attempt (a broken one
/// retried included), parallel calls, the limit checked before each call,
/// and where the cache breakpoints go.
#[cfg(test)]
mod accounting_tests {
    use super::*;
    use common::usage::{
        record_expression, CallRecord, LimitCheck, RecordedCall, UsageMeter, UsageTotals,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn sse(events: &[Value]) -> String {
        events
            .iter()
            .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
            .collect()
    }

    fn start(input: u64, read: u64, w5: u64, w1: u64) -> Value {
        json!({"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,
            "usage":{"input_tokens":input,"output_tokens":1,"cache_read_input_tokens":read,"cache_creation_input_tokens":w5 + w1,
            "cache_creation":{"ephemeral_5m_input_tokens":w5,"ephemeral_1h_input_tokens":w1}}}})
    }

    /// A turn that calls `read_file` (stop reason `tool_use`).
    fn tool_turn(read: u64, write: u64) -> String {
        sse(&[
            start(5, read, write, 0),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":format!("tu{read}"),"name":"read_file","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"a.rs\"}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":40}}),
            json!({"type":"message_stop"}),
        ])
    }

    /// The final answer.
    fn answer_turn(read: u64, write: u64) -> String {
        sse(&[
            start(3, read, write, 0),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":120}}),
            json!({"type":"message_stop"}),
        ])
    }

    /// A fake endpoint that keeps every request body; each request gets the
    /// next reply (the last repeats).
    async fn server(replies: Vec<String>) -> (String, Arc<Mutex<Vec<Value>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let seen = bodies.clone();
        tokio::spawn(async move {
            let mut n = 0;
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let reply = replies.get(n).or(replies.last()).unwrap().clone();
                n += 1;
                // Read the head, then the body by its length.
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 1 << 16];
                while let Ok(k) = sock.read(&mut chunk).await {
                    if k == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..k]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some(h) = text.find("\r\n\r\n") {
                        let len = text[..h]
                            .lines()
                            .find_map(|l| {
                                let l = l.to_ascii_lowercase();
                                l.strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if buf.len() >= h + 4 + len {
                            if let Ok(v) = serde_json::from_slice::<Value>(&buf[h + 4..h + 4 + len])
                            {
                                seen.lock().unwrap().push(v);
                            }
                            break;
                        }
                    }
                }
                let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(reply.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        (format!("http://{addr}/v1/messages"), bodies)
    }

    struct Files;

    #[async_trait::async_trait]
    impl super::super::llm::ToolExecutor for Files {
        async fn execute(
            &self,
            _name: &str,
            _input: &Value,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(json!("fn main() {}"))
        }
    }

    async fn run_loop(
        client: &AnthropicClient,
        tools: &[ToolDefinition],
        usage: &mut TokenUsage,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let gate = common::agent_policy::Gate::new(
            Arc::new(common::agent_policy::store::MemStore::new()),
            common::agent_policy::GateCtx::new("t", common::agent_policy::Agent::Reviewer),
        )
        .offering(tools.iter().map(|t| t.name.clone()));
        let mut messages = vec![(
            "user".to_string(),
            vec![json!({"type":"text","text":"review"})],
        )];
        converse_tool_loop(
            client,
            "claude-opus-5-5",
            "You review code.",
            &mut messages,
            tools,
            &Files,
            &gate,
            usage,
            10,
            1024,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// The analytics month item after these calls: each record's ADD
    /// expression applied, as DynamoDB would.
    fn month_item(
        recs: &[RecordedCall],
    ) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
        let mut item: HashMap<String, f64> = HashMap::new();
        for r in recs {
            let rec = CallRecord {
                call_id: r.call_id.clone(),
                team_id: r.team_id.clone(),
                at_ms: 0,
                feature: r.feature.clone(),
                model: r.model.clone(),
                billed_to: r.billed_to,
                estimated: r.estimated,
                usage: r.usage,
                tag: String::new(),
            };
            let (expr, names, values) = record_expression(&rec, false);
            for part in expr.trim_start_matches("ADD ").split(", ") {
                let (n, v) = part.split_once(' ').unwrap();
                let v: f64 = values[v].as_n().unwrap().parse().unwrap();
                *item.entry(names[n].clone()).or_default() += v;
            }
        }
        item.into_iter()
            .map(|(k, v)| (k, aws_sdk_dynamodb::types::AttributeValue::N(v.to_string())))
            .collect()
    }

    #[tokio::test]
    async fn a_runs_totals_equal_the_teams_counters_call_for_call() {
        // Turn 1: a tool call; turn 2 breaks off mid-reply and is sent
        // again (both attempts billed); turn 3 answers.
        let broken = tool_turn(20_000, 2_000)
            .split("event: content_block_stop")
            .next()
            .unwrap()
            .to_string();
        let (url, _) = server(vec![
            tool_turn(0, 12_000),
            broken,
            tool_turn(12_000, 900),
            answer_turn(12_900, 300),
        ])
        .await;
        let (meter, calls) = UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let mut run = TokenUsage::default();
        let tools = crate::passes::review_agent::review_tools();
        let text = common::usage::with_feature(
            common::usage::feature::REVIEW,
            run_loop(&client, &tools, &mut run),
        )
        .await
        .unwrap();
        assert_eq!(text, "done");
        let recs = calls.lock().unwrap().clone();
        assert_eq!(recs.len(), 4, "every attempt is a recorded call");
        assert_eq!(recs.iter().filter(|r| r.estimated).count(), 1);
        // The run's own totals (what the run item stores) ...
        let sum = |f: fn(&RecordedCall) -> u64| recs.iter().map(f).sum::<u64>();
        assert_eq!(run.input_tokens, sum(|r| r.usage.uncached_input));
        assert_eq!(run.cache_read_tokens, sum(|r| r.usage.cache_read));
        assert_eq!(run.cache_write_tokens, sum(|r| r.usage.cache_write()));
        assert_eq!(run.output_tokens, sum(|r| r.usage.output));
        // ... equal the team's month counters, part by part, and the
        // feature and model breakdowns add up to the same totals.
        let item = month_item(&recs);
        let month = UsageTotals::from_item(Some(&item));
        assert_eq!(month.tokens_in_uncached, run.input_tokens);
        assert_eq!(month.cache_read, run.cache_read_tokens);
        assert_eq!(month.cache_write, run.cache_write_tokens);
        assert_eq!(month.tokens_out, run.output_tokens);
        let by_feature = common::usage::feature_totals(Some(&item));
        assert_eq!(by_feature.len(), 1);
        assert_eq!(by_feature[0].0, "review");
        assert_eq!(by_feature[0].1, month);
        let n = |k: &str| item[k].as_n().unwrap().parse::<f64>().unwrap();
        assert!((n("total_cost_usd") - run.cost_usd).abs() < 1e-6);
        assert!((n("feature#review#cost_usd") - run.cost_usd).abs() < 1e-6);
        assert!((n("model#claude-opus-5-5#cost_usd") - run.cost_usd).abs() < 1e-6);
        assert_eq!(n("model_calls") as usize, recs.len());
        assert_eq!(
            n("feature#review#cache_write_5m") + n("feature#review#cache_write_1h"),
            n("feature#review#cache_write")
        );
        // Cache reads are never input and writes never reads: each part at
        // its own Opus 5.5 rate, once.
        let want = (run.input_tokens as f64 * 4.0
            + run.cache_read_tokens as f64 * 0.20
            + run.cache_write_tokens as f64 * 5.0
            + run.output_tokens as f64 * 20.0)
            / 1e6;
        assert!(
            (run.cost_usd - want).abs() < 1e-9,
            "{} vs {want}",
            run.cost_usd
        );
    }

    #[tokio::test]
    async fn an_api_error_keeps_its_kind_for_the_caller() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 1 << 16];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"type":"error","error":{"type":"permission_error","message":"no"}}"#;
                let head = format!(
                    "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        let (meter, calls) = UsageMeter::memory("team-1");
        let client =
            AnthropicClient::new("k".into(), meter).at(&format!("http://{addr}/v1/messages"));
        let err = converse_simple(
            &client,
            "claude-opus-5-5",
            "s",
            "u",
            &mut TokenUsage::default(),
        )
        .await
        .unwrap_err();
        // Found by the callers that decide retry vs give up by kind.
        let m = err
            .downcast_ref::<common::model_retry::ModelCallError>()
            .expect("the failure itself, boxed once");
        assert_eq!(m.kind, common::model_retry::ModelErrorKind::Status(403));
        assert!(
            calls.lock().unwrap().is_empty(),
            "a refused request is not billed"
        );
    }

    #[tokio::test]
    async fn parallel_calls_are_each_counted_once() {
        let (url, _) = server(vec![answer_turn(0, 1_500)]).await;
        let (meter, calls) = UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let (mut a, mut b, mut c) = Default::default();
        let (ra, rb, rc) = tokio::join!(
            converse_simple(&client, "claude-sonnet-5-5", "s", "a", &mut a),
            converse_simple(&client, "claude-sonnet-5-5", "s", "b", &mut b),
            converse_simple(&client, "claude-sonnet-5-5", "s", "c", &mut c),
        );
        assert!(ra.is_ok() && rb.is_ok() && rc.is_ok());
        let recs = calls.lock().unwrap().clone();
        assert_eq!(recs.len(), 3);
        let team: u64 = recs.iter().map(|r| r.usage.total()).sum();
        let runs: TokenUsage = [a, b, c].iter().fold(TokenUsage::default(), |mut t, u| {
            t.merge(u);
            t
        });
        assert_eq!(team, runs.total_tokens());
    }

    /// A limit on everything the team has spent so far in this meter.
    struct Spent {
        calls: Arc<Mutex<Vec<RecordedCall>>>,
        max: u64,
    }

    #[async_trait::async_trait]
    impl LimitCheck for Spent {
        async fn reached(&self, _team: &str) -> Option<String> {
            let used: u64 = self
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.usage.total())
                .sum();
            (used >= self.max)
                .then(|| format!("Monthly token limit reached ({used} of {})", self.max))
        }
    }

    #[tokio::test]
    async fn a_runaway_loop_stops_at_the_limit_one_call_past_it_at_most() {
        // A loop that would call tools forever (10 turns).
        let (url, bodies) = server(vec![tool_turn(10_000, 500)]).await;
        let (meter, calls) = UsageMeter::memory("team-1");
        // Each turn is 10_000 + 500 + 5 + 40 = 10_545 tokens.
        let meter = meter.with_limits(Arc::new(Spent {
            calls: calls.clone(),
            max: 15_000,
        }));
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let mut run = TokenUsage::default();
        let tools = crate::passes::review_agent::review_tools();
        let err = run_loop(&client, &tools, &mut run).await.unwrap_err();
        let e = common::model_retry::model_error(err.as_ref())
            .unwrap_or_else(|| panic!("a model call error: {err}"));
        assert_eq!(e.kind, common::model_retry::ModelErrorKind::LimitReached);
        assert!(!e.is_transient(), "never retried");
        // Call 1 (10.5K) was under the limit, call 2 crossed it, call 3 was
        // never sent.
        assert_eq!(bodies.lock().unwrap().len(), 2);
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn the_conversation_tail_is_cached_only_when_another_turn_can_read_it() {
        let (url, bodies) = server(vec![tool_turn(0, 4_000), answer_turn(4_000, 100)]).await;
        let (meter, _) = UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        let tools = crate::passes::review_agent::review_tools();
        run_loop(&client, &tools, &mut TokenUsage::default())
            .await
            .unwrap();
        // A loop with tools: the tail breakpoint on every turn, after the
        // tools and system ones, which are the same bytes on every turn.
        let b = bodies.lock().unwrap().clone();
        assert_eq!(b.len(), 2);
        for body in &b {
            assert_eq!(body["cache_control"]["type"], "ephemeral");
            assert_eq!(body["system"][0]["cache_control"]["type"], "ephemeral");
            let tools = body["tools"].as_array().unwrap();
            assert!(tools.last().unwrap().get("cache_control").is_some());
            assert_eq!(
                tools
                    .iter()
                    .filter(|t| t.get("cache_control").is_some())
                    .count(),
                1
            );
        }
        assert_eq!(
            b[0]["tools"], b[1]["tools"],
            "tool order is the same on every turn"
        );
        assert_eq!(b[0]["system"], b[1]["system"]);
        // A one-shot loop (no tools): its only call is its last, so its
        // unique tail is not written to the cache; the system prompt still is.
        let (url, bodies) = server(vec![answer_turn(0, 100)]).await;
        let (meter, _) = UsageMeter::memory("team-1");
        let client = AnthropicClient::new("k".into(), meter).at(&url);
        run_loop(&client, &[], &mut TokenUsage::default())
            .await
            .unwrap();
        let b = bodies.lock().unwrap().clone();
        assert_eq!(b.len(), 1);
        assert!(b[0].get("cache_control").is_none(), "{}", b[0]);
        assert_eq!(b[0]["system"][0]["cache_control"]["type"], "ephemeral");
    }
}
