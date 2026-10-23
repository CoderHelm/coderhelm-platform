//! The shared dead-letter queue, as each team and the platform admin see it.
//!
//! One SQS DLQ holds every team's failed work. A team's health counts only
//! its own messages, computed from the same team-filtered list the dashboard
//! shows; the queue's total depth is never a team's status.
//!
//! Listing never hides anything: [`scan`] receives with a visibility timeout
//! of 0 and long polling (which asks every SQS server, where a short poll
//! samples a few and can answer empty while messages exist), so every reader
//! sees the same queue. Only redrive, delete and purge hold a message, for
//! [`HOLD_SECS`], and they give every other message they pass back at once.
//!
//! What counts for whom is [`common::dlq::classify`]: a team's Problem is
//! only its own messages the worker can read. Unreadable messages and global
//! jobs are shown to platform admins in the shared-queue view.

use std::collections::BTreeMap;
use std::sync::Arc;

use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, Message, MessageSystemAttributeName,
    QueueAttributeName,
};
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::dlq::{classify, Classified};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

/// Most receive calls one listing makes (10 messages each).
const SCAN_ROUNDS: usize = 10;
/// Seconds a long poll waits for a message. Long polling asks every SQS
/// server; a short poll samples a few and can answer empty while messages
/// exist.
const SCAN_WAIT_SECS: i32 = 1;
/// Seconds a message found for redrive, delete or purge stays hidden from
/// other readers while it is acted on.
const HOLD_SECS: i32 = 30;
/// Most receive calls a redrive, delete or purge makes looking for messages.
const HOLD_ROUNDS: usize = 20;

/// One message read from the DLQ.
#[derive(Debug, Clone)]
pub(crate) struct DlqEntry {
    pub message_id: String,
    pub body: String,
    /// SQS `SentTimestamp`: when the message was first sent (ms).
    pub sent_at_ms: Option<i64>,
    /// The queue it failed on, by name.
    pub source_queue: String,
    pub class: Classified,
}

impl DlqEntry {
    fn from_message(m: &Message) -> Option<Self> {
        let message_id = m.message_id()?.to_string();
        let body = m.body().unwrap_or("").to_string();
        let attrs = m.attributes();
        let sent_at_ms = attrs
            .and_then(|a| a.get(&MessageSystemAttributeName::SentTimestamp))
            .and_then(|v| v.parse::<i64>().ok());
        let source_queue = attrs
            .and_then(|a| a.get(&MessageSystemAttributeName::DeadLetterQueueSourceArn))
            .map(|arn| arn.rsplit(':').next().unwrap_or("").to_string())
            .unwrap_or_default();
        let class = classify(&body);
        Some(Self {
            message_id,
            body,
            sent_at_ms,
            source_queue,
            class,
        })
    }

    /// The entry as the dashboard reads it. `with_team` adds the team (the
    /// platform admin's view only).
    fn to_json(&self, now_ms: i64, with_team: bool) -> Value {
        let body: Value =
            serde_json::from_str(&self.body).unwrap_or_else(|_| Value::String(self.body.clone()));
        let mut v = json!({
            "message_id": self.message_id,
            "type": self.class.message_type,
            "sent_at": self.sent_at_ms.and_then(chrono::DateTime::from_timestamp_millis).map(|d| d.to_rfc3339()),
            "age_secs": self.sent_at_ms.map(|s| common::dlq::age_secs(s, now_ms)),
            "source_queue": self.source_queue,
            "retryable": self.class.retryable(),
            "problem": self.class.problem.as_ref().map(|p| p.describe()),
            "problem_kind": self.class.problem,
            "body": body,
        });
        if with_team {
            v["team_id"] = json!(self.class.team_id);
            v["global"] = json!(self.class.global());
            v["shared_only"] = json!(self.class.shared_only());
        }
        v
    }
}

/// A listing of the DLQ. `complete` is false when the queue held more than
/// one listing reads.
#[derive(Debug, Default)]
pub(crate) struct Scan {
    pub entries: Vec<DlqEntry>,
    pub complete: bool,
}

/// One team's part of a listing.
#[derive(Debug)]
pub(crate) struct TeamView<'a> {
    /// Its failed work the worker can read: these, and only these, are the
    /// team's Problem.
    pub failed: Vec<&'a DlqEntry>,
    /// Its messages that can never succeed (unknown type): shown so the team
    /// can delete them, never counted as a Problem.
    pub unretryable: Vec<&'a DlqEntry>,
}

pub(crate) fn team_view<'a>(entries: &'a [DlqEntry], team_id: &str) -> TeamView<'a> {
    let mut failed = Vec::new();
    let mut unretryable = Vec::new();
    for e in entries {
        if e.class.counts_for_team() == Some(team_id) {
            failed.push(e);
        } else if e.class.visible_to_team(team_id) {
            unretryable.push(e);
        }
    }
    let by_age = |a: &&DlqEntry, b: &&DlqEntry| a.sent_at_ms.cmp(&b.sent_at_ms);
    failed.sort_by(by_age);
    unretryable.sort_by(by_age);
    TeamView {
        failed,
        unretryable,
    }
}

/// The health page's "Failed messages" check for one team. A Problem only
/// when the team has failed work of its own; the age of the oldest is shown.
pub(crate) fn health_check_json(view: &TeamView, complete: bool, now_ms: i64) -> Value {
    let count = view.failed.len();
    let oldest = view.failed.first().and_then(|e| e.sent_at_ms);
    json!({
        "name": "dlq",
        "status": if count == 0 { "ok" } else { "critical" },
        "count": count,
        "oldest_sent_at": oldest.and_then(chrono::DateTime::from_timestamp_millis).map(|d| d.to_rfc3339()),
        "oldest_age_secs": oldest.map(|s| common::dlq::age_secs(s, now_ms)),
        "unretryable": view.unretryable.len(),
        "complete": complete,
    })
}

/// Read the whole DLQ without hiding anything.
///
/// Receives with a visibility timeout of 0, so other readers (another
/// dashboard, the next health check) see exactly what this one saw. With a
/// timeout of 0 the same messages can come back each round, so the listing
/// stops when it has as many as the queue reports, or after two rounds with
/// nothing new.
pub(crate) async fn scan(state: &AppState) -> Result<Scan, StatusCode> {
    let url = &state.config.dlq_url;
    if url.is_empty() {
        return Ok(Scan {
            entries: vec![],
            complete: true,
        });
    }
    let attrs = state
        .sqs
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to read DLQ attributes: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let attr = |name: QueueAttributeName| -> u64 {
        attrs
            .attributes()
            .and_then(|a| a.get(&name))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    let visible = attr(QueueAttributeName::ApproximateNumberOfMessages);
    let held = attr(QueueAttributeName::ApproximateNumberOfMessagesNotVisible);
    if visible == 0 && held == 0 {
        return Ok(Scan {
            entries: vec![],
            complete: true,
        });
    }

    let mut seen: BTreeMap<String, DlqEntry> = BTreeMap::new();
    let mut quiet_rounds = 0;
    let mut complete = false;
    for _ in 0..SCAN_ROUNDS {
        let resp = state
            .sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .visibility_timeout(0)
            .wait_time_seconds(SCAN_WAIT_SECS)
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to receive DLQ messages: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        let msgs = resp.messages();
        if msgs.is_empty() {
            complete = true;
            break;
        }
        let before = seen.len();
        for m in msgs {
            if let Some(e) = DlqEntry::from_message(m) {
                seen.entry(e.message_id.clone()).or_insert(e);
            }
        }
        if seen.len() as u64 >= visible {
            complete = true;
            break;
        }
        if seen.len() == before {
            quiet_rounds += 1;
            if quiet_rounds >= 2 {
                complete = true;
                break;
            }
        } else {
            quiet_rounds = 0;
        }
    }
    let mut entries: Vec<DlqEntry> = seen.into_values().collect();
    entries.sort_by_key(|e| e.sent_at_ms);
    Ok(Scan { entries, complete })
}

/// The "Failed messages" health check for a team, or `None` when the DLQ
/// can't be read (the check is left out, as before).
pub(crate) async fn team_health_check(state: &AppState, team_id: &str) -> Option<Value> {
    if state.config.dlq_url.is_empty() {
        return None;
    }
    let scan = scan(state).await.ok()?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    Some(health_check_json(
        &team_view(&scan.entries, team_id),
        scan.complete,
        now_ms,
    ))
}

// ---------------------------------------------------------------------------
// Team routes (owner or admin of the team)
// ---------------------------------------------------------------------------

/// GET /api/dlq — the team's failed messages, oldest first.
pub async fn list_dlq_messages(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" && claims.role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }
    let scan = scan(&state).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let view = team_view(&scan.entries, &claims.team_id);
    let messages: Vec<Value> = view
        .failed
        .iter()
        .chain(view.unretryable.iter())
        .map(|e| e.to_json(now_ms, false))
        .collect();
    Ok(Json(json!({
        "messages": messages,
        "count": view.failed.len(),
        "unretryable": view.unretryable.len(),
        "complete": scan.complete,
    })))
}

/// POST /api/dlq/redrive — send one of the team's failed messages back to
/// the queue it failed on.
///
/// The request names the message by its SQS `message_id` only. The message
/// is read again from the DLQ here, its team is checked on the stored body,
/// and the stored bytes are what is sent: a body posted by the client is
/// never sent anywhere (from #224). A message the worker can't read is
/// refused with 409: sending it again can only fail again.
pub async fn redrive_dlq_message(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    if claims.role != "owner" && claims.role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }
    if state.config.dlq_url.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    let message_id = requested_id(&body)?;
    let team_id = claims.team_id.clone();
    let found = hold_message(&state, message_id, |c| c.visible_to_team(&team_id))
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;

    if !found.entry.class.retryable() {
        warn!(
            team_id = %claims.team_id,
            message_id,
            problem = ?found.entry.class.problem,
            "DLQ redrive refused: the worker can't read this message"
        );
        release(&state, std::slice::from_ref(&found.receipt_handle)).await;
        return Err(StatusCode::CONFLICT);
    }
    let target_queue = redrive_target(&state, &found.entry);
    if target_queue.is_empty() {
        error!("Cannot redrive — target queue URL is empty");
        release(&state, std::slice::from_ref(&found.receipt_handle)).await;
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // The stored body, byte for byte.
    state
        .sqs
        .send_message()
        .queue_url(target_queue)
        .message_body(&found.entry.body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to redrive message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    delete_held(&state, &found.receipt_handle).await?;
    info!(team_id = %claims.team_id, message_id, "DLQ message redriven");
    Ok(StatusCode::OK)
}

/// POST /api/dlq/delete — delete one of the team's messages, readable or
/// not. Found by `message_id`; its team is read from the stored body.
pub async fn delete_dlq_message(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    if claims.role != "owner" && claims.role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }
    if state.config.dlq_url.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    let message_id = requested_id(&body)?;
    let team_id = claims.team_id.clone();
    let found = hold_message(&state, message_id, |c| c.visible_to_team(&team_id))
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
    delete_held(&state, &found.receipt_handle).await?;
    info!(
        team_id = %claims.team_id,
        message_id,
        message_type = ?found.entry.class.message_type,
        "DLQ message deleted"
    );
    Ok(StatusCode::OK)
}

/// POST /api/dlq/purge — delete every message of the team's. Other teams'
/// messages it passes are given back at once, so their health is unchanged.
pub async fn purge_dlq(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    if claims.role != "owner" && claims.role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }
    if state.config.dlq_url.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    let mut passed: Vec<String> = Vec::new();
    let mut deleted = 0u32;
    for _ in 0..HOLD_ROUNDS {
        let msgs = receive_held(&state).await?;
        if msgs.is_empty() {
            break;
        }
        for m in &msgs {
            let Some(handle) = m.receipt_handle() else {
                continue;
            };
            let mine = classify(m.body().unwrap_or("")).visible_to_team(&claims.team_id);
            if mine && delete_held(&state, handle).await.is_ok() {
                deleted += 1;
            } else {
                passed.push(handle.to_string());
            }
        }
    }
    release(&state, &passed).await;
    warn!(team_id = %claims.team_id, deleted, "DLQ purged (team-scoped)");
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// The platform admin's shared-queue view (called from routes::admin, which
// checks for the platform admin first)
// ---------------------------------------------------------------------------

/// Every message in the shared queue, with its team, for the platform admin.
pub(crate) async fn admin_list(state: &AppState) -> Result<Value, StatusCode> {
    let scan = scan(state).await?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let shared = scan
        .entries
        .iter()
        .filter(|e| e.class.shared_only())
        .count();
    let unreadable = scan.entries.iter().filter(|e| !e.class.readable()).count();
    let messages: Vec<Value> = scan
        .entries
        .iter()
        .map(|e| e.to_json(now_ms, true))
        .collect();
    Ok(json!({
        "messages": messages,
        "count": scan.entries.len(),
        "shared": shared,
        "unreadable": unreadable,
        "complete": scan.complete,
        "cleanup_after_days": common::dlq::CLEANUP_AFTER_DAYS,
        "retention_days": common::dlq::DLQ_RETENTION_DAYS,
    }))
}

/// Delete any one message, for the platform admin.
pub(crate) async fn admin_delete(
    state: &AppState,
    admin_email: &str,
    body: &Value,
) -> Result<StatusCode, StatusCode> {
    if state.config.dlq_url.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    let message_id = requested_id(body)?;
    let found = hold_message(state, message_id, |_| true)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
    delete_held(state, &found.receipt_handle).await?;
    warn!(
        admin = %admin_email,
        message_id,
        team_id = ?found.entry.class.team_id,
        message_type = ?found.entry.class.message_type,
        problem = ?found.entry.class.problem,
        "DLQ message deleted by the platform admin"
    );
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// Holding one message to act on it
// ---------------------------------------------------------------------------

/// A message received with a hold, so its receipt handle is the current one.
struct Held {
    entry: DlqEntry,
    receipt_handle: String,
}

fn requested_id(body: &Value) -> Result<&str, StatusCode> {
    body["message_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)
}

async fn receive_held(state: &AppState) -> Result<Vec<Message>, StatusCode> {
    let resp = state
        .sqs
        .receive_message()
        .queue_url(&state.config.dlq_url)
        .max_number_of_messages(10)
        .visibility_timeout(HOLD_SECS)
        .wait_time_seconds(SCAN_WAIT_SECS)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to receive DLQ messages: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(resp.messages().to_vec())
}

/// Find `message_id` and hold it when `allow` accepts what its stored body
/// is. Every other message received on the way is given back at once, so
/// looking for one never hides other teams' work. A message `allow` refuses
/// is answered as not found, never named.
async fn hold_message(
    state: &AppState,
    message_id: &str,
    allow: impl Fn(&Classified) -> bool,
) -> Result<Option<Held>, StatusCode> {
    let mut passed: Vec<String> = Vec::new();
    let mut found = None;
    for _ in 0..HOLD_ROUNDS {
        let msgs = receive_held(state).await?;
        if msgs.is_empty() {
            break;
        }
        for m in &msgs {
            let Some(handle) = m.receipt_handle() else {
                continue;
            };
            if found.is_none() && m.message_id() == Some(message_id) {
                if let Some(entry) = DlqEntry::from_message(m).filter(|e| allow(&e.class)) {
                    found = Some(Held {
                        entry,
                        receipt_handle: handle.to_string(),
                    });
                    continue;
                }
                warn!(message_id, "DLQ message is not the caller's");
            }
            passed.push(handle.to_string());
        }
        if found.is_some() {
            break;
        }
    }
    release(state, &passed).await;
    Ok(found)
}

/// Make received messages visible again right away (10 per call).
async fn release(state: &AppState, handles: &[String]) {
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
            warn!("Could not give DLQ messages back: {e}");
        }
    }
}

async fn delete_held(state: &AppState, receipt_handle: &str) -> Result<(), StatusCode> {
    state
        .sqs
        .delete_message()
        .queue_url(&state.config.dlq_url)
        .receipt_handle(receipt_handle)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| {
            error!("Failed to delete DLQ message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// The queue a redriven message goes back to: the one it failed on (SQS
/// records it), else by its type.
fn redrive_target<'a>(state: &'a AppState, entry: &DlqEntry) -> &'a str {
    let queues = [
        state.config.ticket_queue_url.as_str(),
        state.config.ci_fix_queue_url.as_str(),
        state.config.feedback_queue_url.as_str(),
    ];
    match pick_queue(
        &queues,
        &entry.source_queue,
        entry.class.message_type.as_deref(),
    ) {
        Some(i) => queues[i],
        None => "",
    }
}

/// Index into `[ticket, ci_fix, feedback]`: the queue whose URL ends with
/// the source queue's name, else by type (`resume` to CI fix, `feedback` to
/// feedback, everything else to tickets).
fn pick_queue(queues: &[&str; 3], source_queue: &str, message_type: Option<&str>) -> Option<usize> {
    if !source_queue.is_empty() {
        if let Some(i) = queues
            .iter()
            .position(|u| !u.is_empty() && u.rsplit('/').next() == Some(source_queue))
        {
            return Some(i);
        }
    }
    match message_type {
        Some("resume") => Some(1),
        Some("feedback") => Some(2),
        Some(_) => Some(0),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "TEAM#team-a";
    const B: &str = "TEAM#team-b";

    fn entry(id: &str, body: &str, sent_at_ms: i64) -> DlqEntry {
        DlqEntry {
            message_id: id.into(),
            body: body.into(),
            sent_at_ms: Some(sent_at_ms),
            source_queue: "coderhelm-test-tickets".into(),
            class: classify(body),
        }
    }

    /// Two PascalCase `SkillSync` messages for one team, another team's
    /// failed ticket, a failed global job and a body that isn't JSON.
    fn queue() -> Vec<DlqEntry> {
        vec![
            entry(
                "m1",
                r#"{"type":"SkillSync","team_id":"TEAM#team-a","repo":"o/r","recheck":true}"#,
                1_000,
            ),
            entry(
                "m2",
                r#"{"type":"SkillSync","team_id":"TEAM#team-a","repo":"o/r","recheck":true}"#,
                2_000,
            ),
            entry("m3", r#"{"type":"ticket","team_id":"TEAM#team-b"}"#, 3_000),
            entry("m4", r#"{"type":"weekly_summary"}"#, 4_000),
            entry("m5", "garbage", 5_000),
        ]
    }

    #[test]
    fn unreadable_messages_are_never_a_teams_problem() {
        let q = queue();
        let a = team_view(&q, A);
        assert!(a.failed.is_empty());
        assert_eq!(
            a.unretryable
                .iter()
                .map(|e| &e.message_id[..])
                .collect::<Vec<_>>(),
            ["m1", "m2"]
        );
        let check = health_check_json(&a, true, 10_000);
        assert_eq!(check["status"], "ok");
        assert_eq!(check["count"], 0);
        assert_eq!(check["unretryable"], 2);
        assert!(check["oldest_age_secs"].is_null());
    }

    #[test]
    fn a_team_counts_only_its_own_failed_work() {
        let q = queue();
        let b = team_view(&q, B);
        assert_eq!(b.failed.len(), 1);
        assert_eq!(b.failed[0].message_id, "m3");
        assert!(b.unretryable.is_empty());
        let check = health_check_json(&b, true, 63_000);
        assert_eq!(check["status"], "critical");
        assert_eq!(check["count"], 1);
        assert_eq!(check["oldest_age_secs"], 60);

        // A team with nothing in the queue is OK however full the queue is.
        let c = team_view(&q, "TEAM#team-c");
        assert_eq!(health_check_json(&c, true, 0)["status"], "ok");
    }

    #[test]
    fn the_status_is_the_same_for_the_same_queue_in_any_order() {
        let q = queue();
        let mut r = queue();
        r.reverse();
        for team in [A, B] {
            assert_eq!(
                health_check_json(&team_view(&q, team), true, 9_000),
                health_check_json(&team_view(&r, team), true, 9_000),
            );
        }
    }

    #[test]
    fn team_entries_hide_the_team_and_show_why_they_cant_be_retried() {
        let q = queue();
        let v = q[0].to_json(1_000 + 86_400_000, false);
        assert_eq!(v["retryable"], false);
        assert_eq!(v["problem"], "unknown message type `SkillSync`");
        assert_eq!(v["problem_kind"]["kind"], "unknown_type");
        assert_eq!(v["type"], "SkillSync");
        assert_eq!(v["age_secs"], 86_400);
        assert!(v.get("team_id").is_none());
        assert!(v.get("receipt_handle").is_none());

        let admin = q[4].to_json(0, true);
        assert_eq!(admin["team_id"], Value::Null);
        assert_eq!(admin["shared_only"], true);
        assert_eq!(admin["body"], "garbage");
        assert_eq!(q[2].to_json(0, true)["team_id"], B);
    }

    #[test]
    fn redrive_goes_back_to_the_queue_it_failed_on() {
        let qs = [
            "https://sqs.test/1/coderhelm-test-tickets",
            "https://sqs.test/1/coderhelm-test-ci-fix",
            "https://sqs.test/1/coderhelm-test-feedback",
        ];
        assert_eq!(
            pick_queue(&qs, "coderhelm-test-ci-fix", Some("ticket")),
            Some(1)
        );
        assert_eq!(
            pick_queue(&qs, "coderhelm-test-feedback", Some("review")),
            Some(2)
        );
        // No recorded source: by type.
        assert_eq!(pick_queue(&qs, "", Some("resume")), Some(1));
        assert_eq!(pick_queue(&qs, "", Some("feedback")), Some(2));
        assert_eq!(pick_queue(&qs, "some-other-queue", Some("review")), Some(0));
        assert_eq!(pick_queue(&qs, "", None), None);
    }
}
