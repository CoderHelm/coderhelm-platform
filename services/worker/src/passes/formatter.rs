use aws_sdk_bedrockruntime::types::{ContentBlock, ConversationRole, Message, SystemContentBlock};
use tracing::warn;

use crate::agent::llm;
use crate::models::TokenUsage;
use crate::WorkerState;

/// Rewrite text to match the team's voice/tone using the light model.
/// If voice is empty, returns the input unchanged.
pub async fn format_with_voice(
    state: &WorkerState,
    voice: &str,
    raw_text: &str,
    usage: &mut TokenUsage,
) -> String {
    if voice.is_empty() || raw_text.is_empty() {
        return raw_text.to_string();
    }

    let system = format!(
        "You are a formatter. Rewrite the following text to match this team's voice and tone.\n\
         Do not change technical content, code references, file paths, or meaning.\n\
         Only adjust style, phrasing, and tone.\n\n\
         Team voice instructions:\n{voice}"
    );

    let messages = vec![Message::builder()
        .role(ConversationRole::User)
        .content(ContentBlock::Text(format!(
            "Rewrite this text:\n\n{raw_text}"
        )))
        .build()
        .unwrap()];

    match llm::converse_with_retry(
        &state.bedrock,
        &state.config.light_model_id,
        vec![SystemContentBlock::Text(system)],
        messages,
    )
    .await
    {
        Ok(response) => {
            // Track usage
            if let Some(u) = response.usage() {
                usage.add(
                    u.input_tokens() as u64,
                    u.output_tokens() as u64,
                    0,
                    0,
                );
            }
            // Extract text from response
            response
                .output()
                .and_then(|o| o.as_message().ok())
                .and_then(|m| m.content().first())
                .and_then(|c| c.as_text().ok())
                .map(|t| t.to_string())
                .unwrap_or_else(|| raw_text.to_string())
        }
        Err(e) => {
            warn!(error = %e, "Formatter failed, using raw text");
            raw_text.to_string()
        }
    }
}
