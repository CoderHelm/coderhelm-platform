//! The gateway's model calls (plan chat and the brain chat), every one metered.
//!
//! The only gateway file allowed to call a model (a test in
//! `common::model_call_sites` holds it to that). Each call is streamed and
//! recorded per attempt through `common::model_stream`, under feature `chat`.

use common::model_retry::ModelCallError;
use common::model_stream::{send_metered, InFlight};
use common::usage::{feature, with_feature, UsageMeter};
use serde_json::{json, Value};

/// The team's meter for its plan chat calls.
pub fn chat_meter(state: &crate::AppState, team_id: &str) -> UsageMeter {
    UsageMeter::dynamo(
        state.dynamo.clone(),
        &state.config.analytics_table_name,
        team_id,
    )
}

/// One plan chat turn, read whole. Streamed under the hood so a reply that
/// breaks off is still recorded; retried on transient failures (a person is
/// waiting, so the waits are short).
pub async fn chat_turn(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<Value, ModelCallError> {
    let mut body = body.clone();
    body["stream"] = json!(true);
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let call = send_metered(
        || {
            http.post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .header("anthropic-beta", "prompt-caching-2024-07-31")
                .header("content-type", "application/json")
                .body(bytes.clone())
        },
        &common::model_retry::INTERACTIVE_BACKOFF_SECS,
        "plan_chat",
        meter,
        model_id,
        bytes.len() as u64,
    );
    Ok(with_feature(feature::CHAT, call).await?.message)
}

/// Open a plan chat stream for the caller to relay event by event. The
/// returned [`InFlight`] must see every event (`flight.acc.feed`) and is
/// recorded by `flight.finish().await`, or on drop if the caller stops.
/// Retried by status until a stream starts; a stream already flowing is
/// never resent.
pub async fn open_chat_stream(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<(reqwest::Response, InFlight), ModelCallError> {
    open_stream(http, api_key, body, meter, model_id, feature::CHAT).await
}

/// [`open_chat_stream`] for the brain chat: recorded under feature `brain`.
pub async fn open_brain_stream(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<(reqwest::Response, InFlight), ModelCallError> {
    open_stream(http, api_key, body, meter, model_id, feature::BRAIN).await
}

async fn open_stream(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<(reqwest::Response, InFlight), ModelCallError> {
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    let resp = common::model_retry::send_streaming(
        || {
            http.post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .header("anthropic-beta", "prompt-caching-2024-07-31")
                .header("content-type", "application/json")
                .body(bytes.clone())
        },
        &common::model_retry::INTERACTIVE_BACKOFF_SECS,
        "plan_chat_stream",
    )
    .await?;
    let request_id = resp
        .headers()
        .get("request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let flight = with_feature(feature_tag, async {
        InFlight::new(meter, model_id, request_id, bytes.len() as u64)
    })
    .await;
    Ok((resp, flight))
}
