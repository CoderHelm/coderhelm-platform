//! Which service the deployment calls Claude through.
//!
//! The default is the Anthropic API with each team's own key (stored in
//! Settings → Model Provider). A self-hosted deployment can instead use
//! Amazon Bedrock in its own AWS account (`MODEL_PROVIDER=bedrock`, set by the
//! deploy config): no key is needed, the services' IAM roles call Bedrock,
//! and the model names a team picks (`claude-opus-5-5`) are mapped to Bedrock
//! ids here.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::{json, Value};

/// True when the deployment calls Claude through Amazon Bedrock.
pub fn is_bedrock() -> bool {
    static B: OnceLock<bool> = OnceLock::new();
    *B.get_or_init(|| provider_is_bedrock(std::env::var("MODEL_PROVIDER").ok().as_deref()))
}

/// [`is_bedrock`]'s rule on a given value.
pub fn provider_is_bedrock(v: Option<&str>) -> bool {
    v.is_some_and(|p| p.trim().eq_ignore_ascii_case("bedrock"))
}

static CLIENT: OnceLock<aws_sdk_bedrockruntime::Client> = OnceLock::new();

/// Keep the Bedrock client the services call Claude with (set at start).
pub fn set_bedrock_client(client: aws_sdk_bedrockruntime::Client) {
    let _ = CLIENT.set(client);
}

/// The Bedrock client, when the deployment uses Bedrock and it was set.
pub fn bedrock_client() -> Option<&'static aws_sdk_bedrockruntime::Client> {
    if is_bedrock() {
        CLIENT.get()
    } else {
        None
    }
}

/// The Bedrock id for a model name, from BEDROCK_MODEL_IDS (JSON object)
/// and BEDROCK_INFERENCE_PREFIX (`us.` by default).
pub fn bedrock_model_id(model: &str) -> String {
    static MAP: OnceLock<(String, HashMap<String, String>)> = OnceLock::new();
    let (prefix, map) = MAP.get_or_init(|| {
        let prefix = std::env::var("BEDROCK_INFERENCE_PREFIX").unwrap_or_else(|_| "us.".into());
        let map = std::env::var("BEDROCK_MODEL_IDS")
            .ok()
            .and_then(|s| serde_json::from_str::<HashMap<String, String>>(&s).ok())
            .unwrap_or_default();
        (prefix, map)
    });
    map_model_id(model, prefix, map)
}

/// [`bedrock_model_id`]'s rule: an explicit mapping wins; an id that is
/// already a Bedrock id (`anthropic.` in it, or an ARN) is used as given;
/// anything else becomes `<prefix>anthropic.<model>`.
pub fn map_model_id(model: &str, prefix: &str, map: &HashMap<String, String>) -> String {
    if let Some(id) = map.get(model) {
        return id.clone();
    }
    if model.contains("anthropic.") || model.starts_with("arn:") {
        return model.to_string();
    }
    let name = BEDROCK_NAMES
        .iter()
        .find(|(n, _)| *n == model)
        .map_or(model, |(_, id)| *id);
    format!("{prefix}anthropic.{name}")
}

/// Models whose Bedrock id isn't just their name (Bedrock has no undated
/// Haiku 4.5 id). Others can be added per deployment in BEDROCK_MODEL_IDS.
const BEDROCK_NAMES: &[(&str, &str)] = &[
    ("claude-haiku-4-5", "claude-haiku-4-5-20251001-v1:0"),
    (
        "claude-haiku-4-5-20251001",
        "claude-haiku-4-5-20251001-v1:0",
    ),
];

/// A Messages API request body as Bedrock takes it: no model or stream
/// fields (the model is in the call, streaming is the call), Bedrock's
/// version marker added. Everything else, tools and cache_control
/// included, is sent unchanged.
pub fn bedrock_body(body: &Value) -> Value {
    let mut b = body.clone();
    if let Some(o) = b.as_object_mut() {
        o.remove("model");
        o.remove("stream");
        o.insert("anthropic_version".into(), json!("bedrock-2023-05-31"));
    }
    b
}

/// One Bedrock streaming event, framed as the Anthropic API's SSE text so
/// the same reader handles both (`event: <type>\ndata: <json>\n\n`).
pub fn sse_frame(event_json: &[u8]) -> Vec<u8> {
    let ty = serde_json::from_slice::<Value>(event_json)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(String::from))
        .unwrap_or_else(|| "message".into());
    let mut out = format!("event: {ty}\ndata: ").into_bytes();
    out.extend_from_slice(event_json);
    out.extend_from_slice(b"\n\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_switch() {
        assert!(provider_is_bedrock(Some("bedrock")));
        assert!(provider_is_bedrock(Some(" Bedrock ")));
        assert!(!provider_is_bedrock(Some("anthropic")));
        assert!(!provider_is_bedrock(None));
    }

    #[test]
    fn model_ids_map_to_bedrock() {
        let mut map = HashMap::new();
        map.insert(
            "claude-haiku-4-5".to_string(),
            "us.anthropic.claude-haiku-4-5-20251001-v1:0".to_string(),
        );
        assert_eq!(
            map_model_id("claude-opus-5-5", "us.", &map),
            "us.anthropic.claude-opus-5-5"
        );
        assert_eq!(
            map_model_id("claude-opus-5-5", "", &map),
            "anthropic.claude-opus-5-5"
        );
        assert_eq!(
            map_model_id("claude-haiku-4-5", "eu.", &HashMap::new()),
            "eu.anthropic.claude-haiku-4-5-20251001-v1:0"
        );
        assert_eq!(
            map_model_id("claude-haiku-4-5", "eu.", &map),
            "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        );
        assert_eq!(
            map_model_id("eu.anthropic.claude-sonnet-5-5", "us.", &map),
            "eu.anthropic.claude-sonnet-5-5"
        );
        let arn = "arn:aws:bedrock:us-east-1:111122223333:application-inference-profile/abc";
        assert_eq!(map_model_id(arn, "us.", &map), arn);
    }

    #[test]
    fn body_drops_model_and_stream_and_marks_the_version() {
        let b = bedrock_body(&json!({
            "model": "claude-opus-5-5", "stream": true, "max_tokens": 10,
            "messages": [], "tools": [{"name": "t"}]
        }));
        assert_eq!(b.get("model"), None);
        assert_eq!(b.get("stream"), None);
        assert_eq!(b["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(b["max_tokens"], 10);
        assert_eq!(b["tools"][0]["name"], "t");
    }

    #[test]
    fn events_are_framed_as_sse() {
        let f = sse_frame(br#"{"type":"content_block_delta","index":0}"#);
        assert_eq!(
            String::from_utf8(f).unwrap(),
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0}\n\n"
        );
    }
}
