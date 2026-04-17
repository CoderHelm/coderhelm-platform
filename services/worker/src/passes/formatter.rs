use tracing::warn;

use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::models::TokenUsage;
use crate::WorkerState;

/// Rewrite text to match the team's voice/tone using the light model.
/// If voice is empty, returns the input unchanged.
pub async fn format_with_voice(
    state: &WorkerState,
    voice: &str,
    raw_text: &str,
    provider: &ModelProvider,
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

    let user_msg = format!("Rewrite this text:\n\n{raw_text}");
    let model_id = provider.primary_model_id();

    match provider::converse_simple(state, provider, &model_id, &system, &user_msg, usage).await {
        Ok(text) => text,
        Err(e) => {
            warn!(error = %e, "Formatter failed, using raw text");
            raw_text.to_string()
        }
    }
}
