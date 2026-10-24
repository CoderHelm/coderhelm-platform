//! Titan text embeddings on Amazon Bedrock, metered.
//!
//! Memory (the worker's learnings and the gateway's memory pages) embeds text
//! with Amazon Titan Text Embeddings V2 in CoderHelm's own AWS account. Each
//! call is recorded as feature `embedding`, billed to CoderHelm
//! ([`crate::usage::BilledTo::Coderhelm`]): Titan's response carries
//! `inputTextTokenCount`, which is the input; there is no output.

use serde_json::{json, Value};

use crate::usage::{feature, BilledTo, CallRecord, CallUsage, UsageMeter};

/// The embedding model memory uses.
pub const TITAN_V2: &str = "amazon.titan-embed-text-v2:0";

/// Dimensions of every stored embedding.
pub const DIMENSIONS: usize = 1024;

/// The call record for one Titan response. Without `inputTextTokenCount`
/// the input is estimated from the text (about 4 bytes per token) and the
/// record is marked estimated.
pub fn titan_record(team_id: &str, text: &str, body: &Value) -> CallRecord {
    let counted = body["inputTextTokenCount"].as_u64();
    let input = counted.unwrap_or_else(|| (text.len() as u64).div_ceil(4).max(1));
    let mut rec = CallRecord::now(
        team_id,
        feature::EMBEDDING,
        TITAN_V2,
        CallUsage {
            uncached_input: input,
            ..Default::default()
        },
    );
    rec.billed_to = BilledTo::Coderhelm;
    rec.estimated = counted.is_none();
    rec
}

/// The embedding of `text`, recorded on `meter`.
pub async fn titan_embed(
    client: &aws_sdk_bedrockruntime::Client,
    meter: &UsageMeter,
    text: &str,
) -> Result<Vec<f32>, String> {
    let payload = json!({ "inputText": text, "dimensions": DIMENSIONS, "normalize": true });
    let out = client
        .invoke_model()
        .model_id(TITAN_V2)
        .content_type("application/json")
        .accept("application/json")
        .body(aws_smithy_types::Blob::new(
            serde_json::to_vec(&payload).unwrap_or_default(),
        ))
        .send()
        .await
        .map_err(|e| {
            format!(
                "Bedrock embedding error: {}",
                aws_sdk_bedrockruntime::error::DisplayErrorContext(&e)
            )
        })?;
    let body: Value = serde_json::from_slice(out.body().as_ref())
        .map_err(|e| format!("Embedding response: {e}"))?;
    meter
        .record_call(titan_record(&meter.team_id, text, &body))
        .await;
    let v: Vec<f32> = body["embedding"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_f64().map(|f| f as f32))
                .collect()
        })
        .unwrap_or_default();
    if v.is_empty() {
        return Err("The embedding came back empty".into());
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn titan_counts_its_input_tokens_as_coderhelms_cost() {
        let body = json!({"embedding": [0.1, 0.2], "inputTextTokenCount": 37});
        let rec = titan_record("t1", "some text", &body);
        assert_eq!(rec.usage.uncached_input, 37);
        assert_eq!(rec.usage.output, 0);
        assert_eq!(rec.billed_to, BilledTo::Coderhelm);
        assert_eq!(rec.feature, "embedding");
        assert_eq!(rec.model, TITAN_V2);
        assert!(!rec.estimated);
        // $0.02 per million input tokens.
        let cost = crate::usage::embedding_cost_usd(TITAN_V2, 1_000_000);
        assert!((cost - 0.02).abs() < 1e-12, "{cost}");
    }

    #[test]
    fn a_missing_count_is_estimated_never_zero() {
        let rec = titan_record("t1", "abcdefgh", &json!({"embedding": [0.1]}));
        assert_eq!(rec.usage.uncached_input, 2);
        assert!(rec.estimated);
    }

    #[tokio::test]
    async fn titan_calls_never_touch_the_teams_counters() {
        let body = json!({"inputTextTokenCount": 37});
        let rec = titan_record("t1", "x", &body);
        let (expr, names, _) = crate::usage::record_expression(&rec, false);
        assert!(expr.starts_with("ADD "));
        for attr in names.values() {
            assert!(attr.starts_with("coderhelm#"), "{attr} is a team counter");
        }
        let (meter, calls) = UsageMeter::memory("t1");
        meter.record_call(rec).await;
        assert_eq!(calls.lock().unwrap()[0].billed_to, BilledTo::Coderhelm);
    }
}
