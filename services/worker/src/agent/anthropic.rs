//! Anthropic Messages API client for direct Claude API calls.
//! Used when a team provides their own Anthropic API key instead of using Bedrock.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::TokenUsage;

static ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
static ANTHROPIC_VERSION: &str = "2023-06-01";

/// Reusable Anthropic HTTP client.
pub struct AnthropicClient {
    http: Client,
    api_key: String,
}

impl AnthropicClient {
    pub fn new(api_key: String) -> Self {
        Self {
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_else(|_| Client::new()),
            api_key,
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
        request.context_management = Some(json!({"edits": [{"type": "clear_tool_uses_20250919"}]}));
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

#[derive(Deserialize, Debug)]
struct ApiUsage {
    input_tokens: u64,
    output_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
}

#[derive(Deserialize, Debug)]
struct ApiError {
    error: ApiErrorDetail,
}

#[derive(Deserialize, Debug)]
struct ApiErrorDetail {
    message: String,
    #[serde(default)]
    r#type: String,
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
    };
    apply_model_family(&mut request, None, false);

    let resp = send_request(client, &request).await?;
    usage.add(
        resp.usage.input_tokens,
        resp.usage.output_tokens,
        resp.usage.cache_read_input_tokens,
        resp.usage.cache_creation_input_tokens,
    );
    if let Some(refusal) = resp.refusal() {
        return Err(refusal.into());
    }
    extract_text(&resp.blocks())
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

    loop {
        turns += 1;
        if turns > max_turns {
            warn!("Hit max turn limit ({max_turns}), forcing completion");
            return Err(format!(
                "Reached the maximum number of steps ({max_turns}) without finishing. \
                 The issue may need more detail or a narrower scope."
            )
            .into());
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
            // up to the last cacheable block. On turn N, all prior turns are cached.
            cache_control: Some(CacheControl {
                r#type: "ephemeral".to_string(),
            }),
            thinking: None,
            output_config: None,
            context_management: None,
            fallbacks: None,
        };
        apply_model_family(&mut request, effort, true);

        let response = send_with_retry(client, &request, model_id).await?;

        // Track usage
        usage.add(
            response.usage.input_tokens,
            response.usage.output_tokens,
            response.usage.cache_read_input_tokens,
            response.usage.cache_creation_input_tokens,
        );

        if let Some(refusal) = response.refusal() {
            return Err(refusal.into());
        }

        // Context compaction — aggressively clear old tool results every turn.
        // The model has already consumed them; keeping them in history is pure token waste.
        // Keep the last `keep_recent` turn-pairs uncompacted so the model has recent context.
        // Always-thinking models: history must stay append-only (an edit
        // invalidates every later thinking block), so the server clears old
        // tool results instead (`context_management` in apply_model_family).
        let keep_recent = 4; // keep last 4 turn-pairs (~8 messages)
        if family == ModelFamily::Legacy {
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

        // Execute tools
        let mut tool_results: Vec<Value> = Vec::new();
        for (tool_id, tool_name, tool_input) in &tool_uses {
            let tool_start = std::time::Instant::now();
            info!(tool = %tool_name, "Executing tool");

            match tool_executor.execute(tool_name, tool_input).await {
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

async fn send_request(
    client: &AnthropicClient,
    request: &MessagesRequest,
) -> Result<MessagesResponse, Box<dyn std::error::Error + Send + Sync>> {
    let resp = client
        .http
        .post(ANTHROPIC_API_URL)
        .header("x-api-key", &client.api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("anthropic-beta", beta_header(request))
        .json(request)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        if let Ok(api_err) = serde_json::from_str::<ApiError>(&body) {
            return Err(format!(
                "Anthropic API error {status} ({}): {}",
                api_err.error.r#type, api_err.error.message
            )
            .into());
        }
        return Err(format!("Anthropic API error ({status}): {body}").into());
    }

    Ok(resp.json().await?)
}

async fn send_with_retry(
    client: &AnthropicClient,
    request: &MessagesRequest,
    model_id: &str,
) -> Result<MessagesResponse, Box<dyn std::error::Error + Send + Sync>> {
    for attempt in 0..3u32 {
        match send_request(client, request).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                let err_str = format!("{e}");
                let transient = err_str.contains("overloaded")
                    || err_str.contains("rate_limit")
                    || err_str.contains("529")
                    || err_str.contains("500")
                    || err_str.contains("502")
                    || err_str.contains("503")
                    || err_str.contains("504")
                    || err_str.contains("api_error")
                    || err_str.contains("timed out")
                    || err_str.contains("connection")
                    || err_str.contains("error sending request");
                if attempt < 2 && transient {
                    let delay_secs = 2u64.pow(attempt + 1);
                    warn!(
                        model_id,
                        attempt = attempt + 1,
                        "Anthropic transient error, retrying in {delay_secs}s: {err_str}"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
    unreachable!()
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
    use super::*;

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
        }
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
        assert_eq!(
            v["context_management"]["edits"][0]["type"],
            "clear_tool_uses_20250919"
        );
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
