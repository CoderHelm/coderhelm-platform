//! Events: the envelope, [`emit_event`], and the kept copies that retries,
//! Resend and replay send again.
//!
//! [`emit_event`] only puts the event on the webhooks queue, so the work
//! that emits never waits on a team's endpoints. The worker then keeps the
//! event (only for teams with an endpoint), finds the endpoints that want it
//! and sends.
//!
//! A kept event is the exact body that was signed, so every retry and every
//! endpoint gets the same bytes. Kept copies expire with the delivery
//! records ([`crate::deliveries::RETENTION_DAYS`]).

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::catalog;

pub const SK_PREFIX: &str = "WEBHOOK_EVENT#";
pub const ID_PREFIX: &str = "evt_";

/// One event, as it is sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub created_at: String,
    pub team_id: String,
    pub api_version: String,
    pub data: Value,
}

impl Event {
    pub fn new(team_id: &str, event_type: &str, data: Value) -> Self {
        Self {
            id: format!("{ID_PREFIX}{}", ulid::Ulid::new()),
            event_type: event_type.into(),
            created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            team_id: team_id.into(),
            api_version: catalog::API_VERSION.into(),
            data,
        }
    }

    /// The body every endpoint gets for this event.
    pub fn body(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// The ULID inside an event id (sortable by time).
pub fn ulid_of(event_id: &str) -> &str {
    event_id.strip_prefix(ID_PREFIX).unwrap_or(event_id)
}

pub fn sk(event_id: &str) -> String {
    format!("{SK_PREFIX}{}", ulid_of(event_id))
}

/// Puts an event on the webhooks queue. Never fails the caller: a failure
/// is logged. Returns the event id when it was queued. Does nothing when the
/// queue isn't configured (local runs, older installs).
pub async fn emit_event(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    team_id: &str,
    event_type: &str,
    data: Value,
) -> Option<String> {
    emit_event_keyed(sqs, queue_url, team_id, event_type, data, None).await
}

/// [`emit_event`] with the event's ULID given, for an event whose source
/// can arrive twice (an SNS redelivery): the second copy has the same id, so
/// it is sent once.
pub async fn emit_event_keyed(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    team_id: &str,
    event_type: &str,
    data: Value,
    ulid: Option<&str>,
) -> Option<String> {
    if queue_url.is_empty() || team_id.is_empty() {
        return None;
    }
    debug_assert!(
        catalog::find(event_type).is_some(),
        "{event_type} is not in the event catalog"
    );
    let mut event = Event::new(team_id, event_type, data);
    if let Some(u) = ulid.and_then(|u| ulid::Ulid::from_string(u).ok()) {
        event.id = format!("{ID_PREFIX}{u}");
    }
    let body = serde_json::to_string(&crate::messages::WorkerMessage::WebhookEmit(
        crate::messages::WebhookEmitMessage {
            team_id: event.team_id.clone(),
            event: event.clone(),
        },
    ))
    .ok()?;
    let send = sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .send();
    match tokio::time::timeout(Duration::from_secs(3), send).await {
        Ok(Ok(_)) => Some(event.id),
        Ok(Err(e)) => {
            tracing::warn!(event_type, team_id, error = %aws_sdk_sqs::error::DisplayErrorContext(&e), "Could not queue a webhook event");
            None
        }
        Err(_) => {
            tracing::warn!(event_type, team_id, "Queueing a webhook event timed out");
            None
        }
    }
}

/// The item for a kept event.
pub fn to_item(e: &Event, body: &str, now_epoch: i64) -> HashMap<String, AttributeValue> {
    let s = |v: &str| AttributeValue::S(v.to_string());
    HashMap::from([
        ("pk".to_string(), s(&e.team_id)),
        ("sk".to_string(), s(&sk(&e.id))),
        ("event_type".to_string(), s(&e.event_type)),
        ("created_at".to_string(), s(&e.created_at)),
        ("body".to_string(), s(body)),
        (
            "ttl".to_string(),
            AttributeValue::N(crate::deliveries::ttl_from(now_epoch).to_string()),
        ),
    ])
}

/// A kept event: the parsed event and the exact body.
#[derive(Debug, Clone, PartialEq)]
pub struct Kept {
    pub event: Event,
    pub body: String,
}

pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<Kept> {
    let body = item.get("body")?.as_s().ok()?.clone();
    let event = serde_json::from_str(&body).ok()?;
    Some(Kept { event, body })
}

/// Keeps an event. False when it was already kept (a second copy of the
/// same queue message), so it is never sent twice by that.
pub async fn keep(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    e: &Event,
    body: &str,
) -> Result<bool, String> {
    let r = dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(to_item(e, body, chrono::Utc::now().timestamp())))
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await;
    match r {
        Ok(_) => Ok(true),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            Ok(false)
        }
        Err(e) => Err(aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string()),
    }
}

pub async fn get(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    event_id: &str,
) -> Result<Option<Kept>, String> {
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team_id.into()))
        .key("sk", AttributeValue::S(sk(event_id)))
        .send()
        .await
        .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())?;
    Ok(r.item().and_then(from_item))
}

/// The `sk` bounds of the events made between two times (inclusive).
pub fn range_keys(
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
) -> (String, String) {
    let ms = |t: chrono::DateTime<chrono::Utc>| t.timestamp_millis().max(0) as u64;
    let lo = ulid::Ulid::from_parts(ms(since), 0);
    let hi = ulid::Ulid::from_parts(ms(until), u128::MAX);
    (format!("{SK_PREFIX}{lo}"), format!("{SK_PREFIX}{hi}"))
}

/// Kept events made between two times, oldest first, at most `limit`.
pub async fn list_range(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
    limit: usize,
) -> Result<Vec<Kept>, String> {
    let (lo, hi) = range_keys(since, until);
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.into()))
            .expression_attribute_values(":lo", AttributeValue::S(lo.clone()))
            .expression_attribute_values(":hi", AttributeValue::S(hi.clone()))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())?;
        out.extend(r.items().iter().filter_map(from_item));
        start = r.last_evaluated_key().cloned();
        if start.is_none() || out.len() >= limit {
            break;
        }
    }
    out.truncate(limit);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_event_has_the_documented_envelope() {
        let e = Event::new("TEAM#a", "run.failed", json!({"run_id": "r1"}));
        assert!(e.id.starts_with("evt_") && e.id.len() == 4 + 26);
        assert_eq!(e.api_version, catalog::API_VERSION);
        let v: Value = serde_json::from_str(&e.body()).unwrap();
        assert_eq!(v["type"], "run.failed");
        assert_eq!(v["team_id"], "TEAM#a");
        assert_eq!(v["data"]["run_id"], "r1");
        // The body lists the fields in the documented order.
        let body = e.body();
        let at = |k: &str| body.find(&format!("\"{k}\":")).unwrap();
        let order = ["id", "type", "created_at", "team_id", "api_version", "data"].map(at);
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{body}");
    }

    #[test]
    fn a_kept_event_keeps_the_exact_body() {
        let e = Event::new("TEAM#a", "run.created", json!({"b": 1, "a": 2}));
        let body = e.body();
        let item = to_item(&e, &body, 1_800_000_000);
        assert_eq!(
            item["sk"].as_s().unwrap(),
            &format!("WEBHOOK_EVENT#{}", ulid_of(&e.id))
        );
        let k = from_item(&item).unwrap();
        assert_eq!(k.body, body);
        assert_eq!(k.event, e);
    }

    #[test]
    fn a_time_range_covers_exactly_the_events_made_in_it() {
        let t = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let (lo, hi) = range_keys(t("2026-10-26T10:00:00Z"), t("2026-10-26T11:00:00Z"));
        let at = |s: &str| {
            sk(&format!(
                "evt_{}",
                ulid::Ulid::from_parts(t(s).timestamp_millis() as u64, 12345)
            ))
        };
        let inside = at("2026-10-26T10:30:00Z");
        assert!(lo <= inside && inside <= hi);
        let edge = at("2026-10-26T11:00:00Z");
        assert!(edge <= hi);
        assert!(at("2026-10-26T09:59:59Z") < lo);
        assert!(at("2026-10-26T11:00:01Z") > hi);
    }

    #[tokio::test]
    async fn without_a_queue_nothing_is_emitted() {
        let conf = aws_sdk_sqs::Config::builder()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new("us-east-1"))
            .build();
        let sqs = aws_sdk_sqs::Client::from_conf(conf);
        assert_eq!(
            emit_event(&sqs, "", "TEAM#a", "run.failed", json!({})).await,
            None
        );
        assert_eq!(
            emit_event(&sqs, "https://q", "", "run.failed", json!({})).await,
            None
        );
    }
}
