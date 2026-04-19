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
            http: Client::new(),
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
    content: Vec<ContentBlock>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
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
    content: Vec<ContentBlock>,
    stop_reason: Option<String>,
    usage: ApiUsage,
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
    let request = MessagesRequest {
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
            content: vec![ContentBlock::Text {
                text: user_message.to_string(),
            }],
        }],
        tools: None,
        cache_control: None,
    };

    let resp = send_request(client, &request).await?;
    usage.add(
        resp.usage.input_tokens,
        resp.usage.output_tokens,
        resp.usage.cache_read_input_tokens,
        resp.usage.cache_creation_input_tokens,
    );
    extract_text(&resp.content)
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
    on_tool_call: Option<&(dyn Fn(&str, u64, &str, bool) + Send + Sync)>,
    mut conversation_log: Option<&mut Vec<Value>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
                content: content
                    .iter()
                    .filter_map(|c| serde_json::from_value(c.clone()).ok())
                    .collect(),
            })
            .collect();

        let request = MessagesRequest {
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
        };

        let response = send_with_retry(client, &request, model_id).await?;

        // Track usage
        usage.add(
            response.usage.input_tokens,
            response.usage.output_tokens,
            response.usage.cache_read_input_tokens,
            response.usage.cache_creation_input_tokens,
        );

        // Context compaction — aggressively clear old tool results every turn.
        // The model has already consumed them; keeping them in history is pure token waste.
        // Keep the last `keep_recent` turn-pairs uncompacted so the model has recent context.
        let keep_recent = 4; // keep last 4 turn-pairs (~8 messages)
        compact_messages(messages, keep_recent);

        // Emergency compaction: if context is huge despite per-turn compaction, drop more
        let input_tokens = response.usage.input_tokens;
        let model_limit: u64 = 200_000;
        let context_pct = input_tokens as f64 / model_limit as f64;
        if context_pct > 0.60 {
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

        // Serialize response content blocks
        let response_blocks: Vec<Value> = response
            .content
            .iter()
            .map(|b| serde_json::to_value(b).unwrap_or(Value::Null))
            .collect();

        messages.push(("assistant".to_string(), response_blocks));

        // Extract tool uses
        let tool_uses: Vec<_> = response
            .content
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
                    "content": response.content.iter().map(|b| serde_json::to_value(b).unwrap_or(Value::Null)).collect::<Vec<_>>(),
                    "usage": {
                        "input_tokens": response.usage.input_tokens,
                        "output_tokens": response.usage.output_tokens,
                        "cache_read": response.usage.cache_read_input_tokens,
                        "cache_write": response.usage.cache_creation_input_tokens
                    },
                    "stop_reason": response.stop_reason
                }));
            }
            return extract_text(&response.content);
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
                "content": response.content.iter().map(|b| serde_json::to_value(b).unwrap_or(Value::Null)).collect::<Vec<_>>(),
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                    "cache_read": response.usage.cache_read_input_tokens,
                    "cache_write": response.usage.cache_creation_input_tokens
                }
            }));
            // Log tool results (truncate large outputs to 100KB)
            let truncated_results: Vec<Value> = tool_results.iter().map(|r| {
                let mut entry = r.clone();
                if let Some(content) = entry.get("content").and_then(|c| c.as_str()) {
                    if content.len() > 102_400 {
                        entry["content"] = Value::String(format!(
                            "{}... [truncated, {} bytes total]",
                            &content[..102_400],
                            content.len()
                        ));
                    }
                }
                entry
            }).collect();
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
        if s.len() > 200 { format!("{}…", &s[..200]) } else { s }
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
        .header("anthropic-beta", "prompt-caching-2024-07-31")
        .json(request)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        if let Ok(api_err) = serde_json::from_str::<ApiError>(&body) {
            return Err(format!(
                "Anthropic API error ({}): {}",
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
                    || err_str.contains("503");
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
                        let hint = &c[..c.len().min(60)];
                        block["content"] =
                            json!(format!("[Cleared — {len} chars: {hint}…]", len = c.len(), hint = hint));
                    }
                }
            }
        }
    }
}
