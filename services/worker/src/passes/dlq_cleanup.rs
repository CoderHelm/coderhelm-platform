//! The DLQ cleanup (daily, EventBridge → SQS `dlq_cleanup`).
//!
//! A message the worker can't parse never succeeds on retry; it only sits in
//! the shared DLQ until SQS drops it at 14 days, with no record of what it
//! was. Once such a message is `common::dlq::CLEANUP_AFTER_DAYS` old, this
//! copies it to S3 (`dlq-archive/<sent day>/<message id>.json` in the data
//! bucket) and only then deletes it from the DLQ.
//!
//! "Can't parse" is judged by this worker's own `WorkerMessage`, the same
//! parse that failed three times before SQS moved the message here.
//! Messages the worker can read are never touched, at any age: they are a
//! team's work and a person decides whether to retry or delete them.
//!
//! Every message this run receives but keeps is given back at once.

use std::collections::HashSet;

use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, Message, MessageSystemAttributeName,
};
use serde_json::json;
use tracing::{info, warn};

use crate::models::WorkerMessage;
use crate::WorkerState;

/// Most receive calls one run makes (10 messages each).
const ROUNDS: usize = 30;
/// Seconds a received message stays hidden while this run looks at it.
const HOLD_SECS: i32 = 120;

/// What one run did.
#[derive(Debug, Default)]
pub struct Report {
    /// Distinct messages read.
    pub read: usize,
    /// Archived to S3 and deleted from the DLQ.
    pub archived: usize,
    /// Left in the DLQ (readable, too young, or archiving failed).
    pub kept: usize,
    /// False when the DLQ held more than one run reads.
    pub complete: bool,
}

/// Why a message is cleaned up, or `None` to keep it. The worker must fail
/// to parse it, and it must be old enough. A message with no send time is
/// kept.
pub fn cleanup_reason(body: &str, sent_at_ms: Option<i64>, now_ms: i64) -> Option<String> {
    let reads = serde_json::from_str::<WorkerMessage>(body).is_ok();
    let age = common::dlq::age_secs(sent_at_ms?, now_ms);
    if !common::dlq::cleanup_due(reads, age) {
        return None;
    }
    let class = common::dlq::classify(body);
    Some(match class.problem {
        Some(p) => p.describe(),
        // A known type with fields the worker can't read.
        None => match serde_json::from_str::<WorkerMessage>(body) {
            Err(e) => format!("the worker can't parse it: {e}"),
            Ok(_) => "the worker can't parse it".to_string(),
        },
    })
}

/// The archived record: the body exactly as stored, and why it was removed.
pub fn archive_record(
    m: &Message,
    sent_at_ms: i64,
    reason: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> serde_json::Value {
    let body = m.body().unwrap_or("");
    let class = common::dlq::classify(body);
    let attr = |n: MessageSystemAttributeName| m.attributes().and_then(|a| a.get(&n)).cloned();
    json!({
        "message_id": m.message_id(),
        "sent_at": chrono::DateTime::from_timestamp_millis(sent_at_ms).map(|d| d.to_rfc3339()),
        "archived_at": now.to_rfc3339(),
        "reason": reason,
        "team_id": class.team_id,
        "type": class.message_type,
        "source_queue_arn": attr(MessageSystemAttributeName::DeadLetterQueueSourceArn),
        "receive_count": attr(MessageSystemAttributeName::ApproximateReceiveCount),
        "body": body,
    })
}

pub async fn run(state: &WorkerState) -> Result<Report, String> {
    let url = &state.config.dlq_url;
    let mut report = Report::default();
    if url.is_empty() {
        info!("DLQ cleanup skipped: no DLQ_URL");
        report.complete = true;
        return Ok(report);
    }
    let now = chrono::Utc::now();
    let mut seen: HashSet<String> = HashSet::new();
    let mut give_back: Vec<String> = Vec::new();

    for _ in 0..ROUNDS {
        let resp = state
            .sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .visibility_timeout(HOLD_SECS)
            .wait_time_seconds(1)
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .send()
            .await
            .map_err(|e| format!("receive from the DLQ: {e}"))?;
        let msgs = resp.messages();
        if msgs.is_empty() {
            report.complete = true;
            break;
        }
        for m in msgs {
            let Some(handle) = m.receipt_handle() else {
                continue;
            };
            let id = m.message_id().unwrap_or("").to_string();
            if !seen.insert(id.clone()) {
                give_back.push(handle.to_string());
                continue;
            }
            report.read += 1;
            let sent_at_ms = m
                .attributes()
                .and_then(|a| a.get(&MessageSystemAttributeName::SentTimestamp))
                .and_then(|v| v.parse::<i64>().ok());
            let Some(reason) =
                cleanup_reason(m.body().unwrap_or(""), sent_at_ms, now.timestamp_millis())
            else {
                report.kept += 1;
                give_back.push(handle.to_string());
                continue;
            };
            let sent_at_ms = sent_at_ms.unwrap_or_default();
            match archive_then_delete(state, m, handle, sent_at_ms, &reason, now).await {
                Ok(key) => {
                    report.archived += 1;
                    let class = common::dlq::classify(m.body().unwrap_or(""));
                    warn!(
                        message_id = %id,
                        team_id = ?class.team_id,
                        message_type = ?class.message_type,
                        reason = %reason,
                        archive_key = %key,
                        "DLQ cleanup: archived and deleted a message the worker can't read"
                    );
                }
                Err(e) => {
                    report.kept += 1;
                    warn!(message_id = %id, error = %e, "DLQ cleanup: kept a message it could not archive");
                    give_back.push(handle.to_string());
                }
            }
        }
    }
    give_back_now(state, &give_back).await;
    Ok(report)
}

/// Write the archive record, and delete from the DLQ only once S3 has it.
async fn archive_then_delete(
    state: &WorkerState,
    m: &Message,
    handle: &str,
    sent_at_ms: i64,
    reason: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<String, String> {
    let key = common::dlq::archive_key(sent_at_ms, m.message_id().unwrap_or("unknown"));
    let record = archive_record(m, sent_at_ms, reason, now);
    let bytes = serde_json::to_vec_pretty(&record).map_err(|e| e.to_string())?;
    state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .content_type("application/json")
        .body(bytes.into())
        .send()
        .await
        .map_err(|e| format!("archive to s3://{}/{key}: {e}", state.config.bucket_name))?;
    state
        .sqs
        .delete_message()
        .queue_url(&state.config.dlq_url)
        .receipt_handle(handle)
        .send()
        .await
        .map_err(|e| format!("delete from the DLQ (archived at {key}): {e}"))?;
    Ok(key)
}

/// Make kept messages visible again right away (10 per call).
async fn give_back_now(state: &WorkerState, handles: &[String]) {
    for chunk in handles.chunks(10) {
        let entries: Vec<ChangeMessageVisibilityBatchRequestEntry> = chunk
            .iter()
            .enumerate()
            .filter_map(|(i, h)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(h)
                    .visibility_timeout(0)
                    .build()
                    .ok()
            })
            .collect();
        if let Err(e) = state
            .sqs
            .change_message_visibility_batch()
            .queue_url(&state.config.dlq_url)
            .set_entries(Some(entries))
            .send()
            .await
        {
            warn!("DLQ cleanup: could not give messages back: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY_MS: i64 = 86_400_000;
    const NOW: i64 = 1_800_000_000_000;

    /// A hand-made message with the PascalCase tag the worker refuses.
    const PASCAL: &str =
        r#"{"type":"SkillSync","team_id":"TEAM#team-a","repo":"acme/widgets","recheck":true}"#;

    #[test]
    fn an_old_message_with_an_unknown_type_is_cleaned_up() {
        let old = Some(NOW - 7 * DAY_MS);
        assert_eq!(
            cleanup_reason(PASCAL, old, NOW).as_deref(),
            Some("unknown message type `SkillSync`")
        );
        assert_eq!(
            cleanup_reason("not json", old, NOW).as_deref(),
            Some("the message is not JSON")
        );
    }

    #[test]
    fn young_messages_and_messages_with_no_time_are_kept() {
        assert_eq!(
            cleanup_reason(PASCAL, Some(NOW - 7 * DAY_MS + 1_000), NOW),
            None
        );
        assert_eq!(cleanup_reason(PASCAL, None, NOW), None);
    }

    #[test]
    fn readable_messages_are_never_cleaned_up() {
        let ancient = Some(NOW - 13 * DAY_MS);
        for body in [
            r#"{"type":"skill_sync","team_id":"TEAM#team-a","repo":"acme/widgets","recheck":true}"#,
            r#"{"type":"review_sweep"}"#,
            r#"{"type":"dlq_cleanup"}"#,
        ] {
            assert_eq!(cleanup_reason(body, ancient, NOW), None, "{body}");
        }
    }

    #[test]
    fn a_known_type_the_worker_cant_parse_is_cleaned_up_with_serdes_reason() {
        // `skill_sync` needs a repo.
        let r = cleanup_reason(
            r#"{"type":"skill_sync","team_id":"TEAM#team-a"}"#,
            Some(NOW - 8 * DAY_MS),
            NOW,
        )
        .unwrap();
        assert!(r.starts_with("the worker can't parse it: "), "{r}");
        assert!(r.contains("repo"), "{r}");
    }

    #[test]
    fn the_archive_keeps_the_body_exactly() {
        let m = Message::builder()
            .message_id("mid-1")
            .body(PASCAL)
            .attributes(
                MessageSystemAttributeName::DeadLetterQueueSourceArn,
                "arn:aws:sqs:us-east-1:000000000000:coderhelm-test-tickets",
            )
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "4")
            .build();
        let now = chrono::DateTime::from_timestamp_millis(NOW).unwrap();
        let r = archive_record(
            &m,
            NOW - 8 * DAY_MS,
            "unknown message type `SkillSync`",
            now,
        );
        assert_eq!(r["body"], PASCAL);
        assert_eq!(r["message_id"], "mid-1");
        assert_eq!(r["team_id"], "TEAM#team-a");
        assert_eq!(r["type"], "SkillSync");
        assert_eq!(r["receive_count"], "4");
        assert_eq!(r["reason"], "unknown message type `SkillSync`");
        assert!(r["source_queue_arn"]
            .as_str()
            .unwrap()
            .ends_with(":coderhelm-test-tickets"));
    }
}
