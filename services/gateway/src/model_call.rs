//! The gateway's model calls (plan chat and the brain chat), every one metered.
//!
//! The only gateway file allowed to call a model (a test in
//! `common::model_call_sites` holds it to that). Each call is streamed and
//! recorded per attempt through `common::model_stream`, under feature `chat`.
//! When the deployment uses Amazon Bedrock (`common::model_provider`), the
//! same requests go to Bedrock and its events are read as the same SSE.

use futures::StreamExt;

use common::model_retry::{ModelCallError, ModelErrorKind};
use common::model_stream::{send_metered, InFlight, SseParser};
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

async fn whole_turn(
    http: &reqwest::Client,
    api_key: &str,
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<Value, ModelCallError> {
    if common::model_provider::is_bedrock() {
        let (mut stream, mut flight) =
            open_bedrock_stream(body, meter, model_id, feature_tag).await?;
        let mut parser = SseParser::default();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(c) => {
                    for ev in parser.push(&c) {
                        flight.acc.feed(&ev);
                    }
                }
                Err(_) => break,
            }
        }
        flight.finish().await;
        let acc = std::mem::take(&mut flight.acc);
        if let Some(err) = acc.stream_error() {
            return Err(ModelCallError::new(
                ModelErrorKind::Network,
                err.to_string(),
            ));
        }
        if !acc.finished() {
            return Err(ModelCallError::new(
                ModelErrorKind::Network,
                "the Bedrock reply broke off",
            ));
        }
        return Ok(acc.into_message());
    }
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
    use crate::routes::setup_checks::ModelProbe;
    let body = json!({
        "model": model,
        "max_tokens": 1,
        "messages": [{ "role": "user", "content": "Reply with OK." }],
    });
    match chat_turn(http, api_key, &body, meter, model).await {
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
    if common::model_provider::is_bedrock() {
        return open_bedrock_stream(body, meter, model_id, feature_tag).await;
    }
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
    let stream = resp
        .bytes_stream()
        .map(|r| r.map(|b| b.to_vec()).map_err(|e| e.to_string()));
    Ok((Box::pin(stream), flight))
}

/// A plan chat reply's bytes as Anthropic SSE text.
pub type ChatBytes = std::pin::Pin<Box<dyn futures::Stream<Item = Result<Vec<u8>, String>> + Send>>;

/// The same request through Bedrock's streaming call, retried by status
/// until a stream starts. Bedrock's events are framed as SSE, so callers
/// read them exactly like the Anthropic API's.
async fn open_bedrock_stream(
    body: &Value,
    meter: &UsageMeter,
    model_id: &str,
    feature_tag: &'static str,
) -> Result<(ChatBytes, InFlight), ModelCallError> {
    use aws_sdk_bedrockruntime::types::ResponseStream;
    use common::model_retry::{backoff, is_transient_status, INTERACTIVE_BACKOFF_SECS};
    let Some(client) = common::model_provider::bedrock_client() else {
        return Err(ModelCallError::new(
            ModelErrorKind::Network,
            "Bedrock is the model provider but no client was set up",
        ));
    };
    let bytes = serde_json::to_vec(&common::model_provider::bedrock_body(body)).unwrap_or_default();
    let bedrock_id = common::model_provider::bedrock_model_id(model_id);
    let mut attempt = 0;
    let out = loop {
        let result = client
            .invoke_model_with_response_stream()
            .model_id(&bedrock_id)
            .content_type("application/json")
            .accept("application/json")
            .body(aws_smithy_types::Blob::new(bytes.clone()))
            .send()
            .await;
        match result {
            Ok(out) => break out,
            Err(e) => {
                let status = e.raw_response().map(|r| r.status().as_u16());
                let text = aws_sdk_bedrockruntime::error::DisplayErrorContext(&e).to_string();
                if !status.is_none_or(is_transient_status)
                    || attempt >= INTERACTIVE_BACKOFF_SECS.len()
                {
                    return Err(ModelCallError::new(
                        status.map_or(ModelErrorKind::Network, ModelErrorKind::Status),
                        text,
                    ));
                }
                tracing::warn!(model = %bedrock_id, attempt, status = ?status, "Bedrock stream failed to start — retrying");
                tokio::time::sleep(backoff(&INTERACTIVE_BACKOFF_SECS, attempt, None)).await;
                attempt += 1;
            }
        }
    };
    let flight = with_feature(feature_tag, async {
        InFlight::new(meter, model_id, None, bytes.len() as u64)
    })
    .await;
    let events = futures::stream::unfold(Some(out.body), |rx| async move {
        let mut rx = rx?;
        match rx.recv().await {
            Ok(Some(ResponseStream::Chunk(part))) => {
                let raw = part.bytes.map(|b| b.into_inner()).unwrap_or_default();
                Some((Ok(common::model_provider::sse_frame(&raw)), Some(rx)))
            }
            Ok(Some(_)) => Some((Ok(Vec::new()), Some(rx))),
            Ok(None) => None,
            Err(e) => Some((
                Err(aws_sdk_bedrockruntime::error::DisplayErrorContext(&e).to_string()),
                None,
            )),
        }
    });
    Ok((Box::pin(events), flight))
}
