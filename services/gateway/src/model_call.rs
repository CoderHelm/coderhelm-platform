//! The gateway's model calls (plan chat and the brain chat), every one metered.
//!
//! The only gateway file allowed to call a model (a test in
//! `common::model_call_sites` holds it to that). Each call is streamed and
//! recorded per attempt through `common::model_stream`, under feature `chat`.
//! When the deployment uses Amazon Bedrock (`common::model_provider`), the
//! same requests go to Bedrock and its events are read as the same SSE.

use common::model_retry::{ModelCallError, ModelErrorKind};
use common::model_stream::{send_metered, InFlight, MESSAGES_URL};
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
    whole_turn(http, api_key, body, meter, model_id, feature::CHAT).await
}

/// [`chat_turn`] for Chat's own small calls (the answer check): recorded
/// under feature `brain`.
pub async fn brain_turn(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<Value, ModelCallError> {
    whole_turn(http, api_key, body, meter, model_id, feature::BRAIN).await
}

/// The repo resolver's one call (which repo a log group's code lives in):
/// recorded under feature `log_analysis`.
pub async fn log_repo_turn(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<Value, ModelCallError> {
    whole_turn(http, api_key, body, meter, model_id, feature::LOG_ANALYSIS).await
}

/// Where one call goes: Bedrock (with the id to call) or the Anthropic API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Via {
    Bedrock(String),
    Anthropic,
}

impl Via {
    /// The install's provider as it is now (Settings, System, Models).
    pub fn now(model_id: &str) -> Self {
        if common::model_provider::is_bedrock() {
            Via::Bedrock(common::model_provider::bedrock_model_id(model_id))
        } else {
            Via::Anthropic
        }
    }
}

async fn whole_turn(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<Value, ModelCallError> {
    whole_turn_via(
        Via::now(model_id),
        http,
        api_key,
        body,
        meter,
        model_id,
        feature_tag,
    )
    .await
}

async fn whole_turn_via(
    via: Via,
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<Value, ModelCallError> {
    if let Via::Bedrock(bedrock_id) = via {
        let client = bedrock_client()?;
        let bytes =
            serde_json::to_vec(&common::model_provider::bedrock_body(body)).unwrap_or_default();
        let call = common::model_stream::send_bedrock_metered(
            client,
            &bedrock_id,
            &bytes,
            &common::model_retry::INTERACTIVE_BACKOFF_SECS,
            meter,
            model_id,
        );
        return Ok(with_feature(feature_tag, call).await?.message);
    }
    let mut body = body.clone();
    body["stream"] = json!(true);
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let call = send_metered(
        || {
            http.post(MESSAGES_URL)
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
    Ok(with_feature(feature_tag, call).await?.message)
}

/// The setup checklist's test call: one turn, at most one token out,
/// metered like any chat turn. Tells whether the deployment can call
/// `model` with this key (or through Bedrock).
pub async fn probe(
    http: &reqwest::Client,
    api_key: &str,
    meter: &UsageMeter,
    model: &str,
) -> crate::routes::setup_checks::ModelProbe {
    probe_via(Via::now(model), http, api_key, meter, model).await
}

/// [`probe`] on a given provider, whatever the install uses now: a
/// provider switch checks the provider it would switch to.
pub async fn probe_via(
    via: Via,
    http: &reqwest::Client,
    api_key: &str,
    meter: &UsageMeter,
    model: &str,
) -> crate::routes::setup_checks::ModelProbe {
    use crate::routes::setup_checks::ModelProbe;
    let body = json!({
        "model": model,
        "max_tokens": 1,
        "messages": [{ "role": "user", "content": "Reply with OK." }],
    });
    match whole_turn_via(via, http, api_key, &body, meter, model, feature::CHAT).await {
        Ok(_) => ModelProbe::Ok,
        // The token limit is the point of a one-token call.
        Err(e) if e.kind == ModelErrorKind::Truncated => ModelProbe::Ok,
        Err(e) => match e.kind {
            ModelErrorKind::Status(code) => ModelProbe::Refused(code, e.message),
            _ => ModelProbe::Unreachable(e.message),
        },
    }
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
) -> Result<(ChatBytes, InFlight), ModelCallError> {
    open_stream(http, api_key, body, meter, model_id, feature::CHAT).await
}

/// [`open_chat_stream`] for the brain chat: recorded under feature `brain`.
pub async fn open_brain_stream(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
) -> Result<(ChatBytes, InFlight), ModelCallError> {
    open_stream(http, api_key, body, meter, model_id, feature::BRAIN).await
}

async fn open_stream(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<(ChatBytes, InFlight), ModelCallError> {
    if let Via::Bedrock(bedrock_id) = Via::now(model_id) {
        return open_bedrock_stream(&bedrock_id, body, meter, model_id, feature_tag).await;
    }
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    let open = common::model_stream::open_metered(
        || {
            http.post(MESSAGES_URL)
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .header("anthropic-beta", "prompt-caching-2024-07-31")
                .header("content-type", "application/json")
                .body(bytes.clone())
        },
        &common::model_retry::INTERACTIVE_BACKOFF_SECS,
        "plan_chat_stream",
        meter,
        model_id,
        bytes.len() as u64,
    );
    with_feature(feature_tag, open).await
}

/// A plan chat reply's bytes as Anthropic SSE text.
pub type ChatBytes = common::model_stream::ReplyBytes;

fn bedrock_client() -> Result<&'static aws_sdk_bedrockruntime::Client, ModelCallError> {
    common::model_provider::any_bedrock_client().ok_or_else(|| {
        ModelCallError::new(
            ModelErrorKind::Network,
            "Bedrock is the model provider but no client was set up",
        )
    })
}

/// The same request through Bedrock's streaming call, retried by status
/// until a stream starts (`common::model_stream::open_bedrock_metered`).
/// Bedrock's events are framed as SSE, so callers read them exactly like
/// the Anthropic API's.
async fn open_bedrock_stream(
    bedrock_id: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<(ChatBytes, InFlight), ModelCallError> {
    let client = bedrock_client()?;
    let bytes = serde_json::to_vec(&common::model_provider::bedrock_body(body)).unwrap_or_default();
    let open = common::model_stream::open_bedrock_metered(
        client,
        bedrock_id,
        &bytes,
        &common::model_retry::INTERACTIVE_BACKOFF_SECS,
        meter,
        model_id,
    );
    with_feature(feature_tag, open).await
}
