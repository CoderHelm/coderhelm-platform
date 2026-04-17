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
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let request = MessagesRequest {
        model: model_id.to_string(),
        max_tokens: 16384,
        system: vec![SystemBlock::Text {
            text: system.to_string(),
            cache_control: None,
        }],
        messages: vec![ApiMessage {
            role: "user".to_string(),
            content: vec![ContentBlock::Text {
                text: user_message.to_string(),
            }],
        }],
        tools: None,
    };

    let resp = send_request(client, &request).await?;
    extract_text(&resp.content)
}

/// Agentic tool-use loop. Equivalent to `converse_with_opts` for Bedrock.
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
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let api_tools: Vec<ApiTool> = tools
        .iter()
        .map(|t| ApiTool {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
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
        if turns > 1 && turns % 10 == 0 {
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
        };

        let response = send_with_retry(client, &request, model_id).await?;

        // Track usage
        usage.add(
            response.usage.input_tokens,
            response.usage.output_tokens,
            response.usage.cache_read_input_tokens,
            response.usage.cache_creation_input_tokens,
        );

        // Context compaction based on input tokens
        let input_tokens = response.usage.input_tokens;
        let model_limit: u64 = 200_000;
        let context_pct = input_tokens as f64 / model_limit as f64;
        if context_pct > 0.90 {
            info!(
                "Context at {:.0}%, emergency compaction",
                context_pct * 100.0
            );
            compact_messages(messages, 3);
        } else if context_pct > 0.75 {
            info!(
                "Context at {:.0}%, aggressive compaction",
                context_pct * 100.0
            );
            compact_messages(messages, 5);
        } else if context_pct > 0.60 {
            compact_messages(messages, 8);
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

        messages.push(("user".to_string(), tool_results));
    }
}

// --- Internal helpers ---

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
fn compact_messages(messages: &mut Vec<(String, Vec<Value>)>, keep_last: usize) {
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
                    if c.len() > 200 {
                        block["content"] = json!("[Tool result cleared — see recent context]");
                    }
                }
            }
        }
    }
}
