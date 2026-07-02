//! Central gate for "should this ticket event start a new run?".
//!
//! Both webhook sources route through here so the dedup semantics are
//! uniform (they used to differ per path, and the underlying queries were
//! broken — DynamoDB applies Limit before FilterExpression, so the old
//! team-partition scans only ever inspected the 5 oldest runs):
//!
//! - a run in flight (running / queued / awaiting_ci) always blocks — a new
//!   run would force-reset the branch out from under the active one
//! - a completed latest run blocks unless the ticket CONTENT actually
//!   changed (context hash), in which case rework is allowed; the worker
//!   resets the branch and reopens the existing PR
//! - a failed / needs_input / retried / cancelled latest run never blocks
//! - explicit human triggers (slash commands, comment retries) skip the
//!   content check but still respect in-flight blocking

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::AppState;

pub enum TicketGate {
    Enqueue,
    SkipInFlight,
    SkipUnchanged,
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// Decide whether a ticket trigger should enqueue a new run.
///
/// `new_context_hash`: `common::ticket_context_hash` of the incoming ticket
/// content; pass `None` when the caller cannot compute it (treated as
/// unchanged for completed tickets). `explicit` marks direct human commands,
/// which bypass the content check.
///
/// Fails open on query errors — the worker's own active-run guard is the
/// backstop, and dropping webhooks during a DynamoDB blip is worse than a
/// rare duplicate.
pub async fn gate_ticket_trigger(
    state: &AppState,
    team_id: &str,
    ticket_id: &str,
    new_context_hash: Option<&str>,
    explicit: bool,
) -> TicketGate {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":ticket", attr_s(ticket_id))
        .send()
        .await;

    let items = match result {
        Ok(out) => out.items.unwrap_or_default(),
        Err(e) => {
            warn!(team_id, ticket_id, error = %e, "Trigger gate query failed — allowing run");
            return TicketGate::Enqueue;
        }
    };

    let status_of = |item: &std::collections::HashMap<String, AttributeValue>| {
        item.get("status")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };

    if items
        .iter()
        .any(|i| matches!(status_of(i).as_str(), "running" | "queued" | "awaiting_ci"))
    {
        info!(team_id, ticket_id, "Trigger gate: run in flight — skipping");
        return TicketGate::SkipInFlight;
    }

    if explicit {
        return TicketGate::Enqueue;
    }

    // Latest run by run_id (ULIDs sort lexicographically by creation time).
    let latest = items.iter().max_by(|a, b| {
        let ra = a
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let rb = b
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        ra.cmp(&rb)
    });
    let Some(latest) = latest else {
        return TicketGate::Enqueue;
    };

    // "success" is a legacy terminal status from an old resume path;
    // "merged" means the PR landed — rework only on real content change.
    if matches!(
        status_of(latest).as_str(),
        "completed" | "success" | "merged"
    ) {
        let stored_hash = latest.get("context_hash").and_then(|v| v.as_s().ok());
        match (new_context_hash, stored_hash) {
            (Some(new), Some(stored)) if new != stored => {
                info!(
                    team_id,
                    ticket_id, "Trigger gate: content changed — allowing rework"
                );
                TicketGate::Enqueue
            }
            // Unchanged content, unknown content, or legacy record without a
            // stored hash: a completed ticket stays done. Humans can still
            // force a run via comment, slash command, or the dashboard retry.
            _ => {
                info!(
                    team_id,
                    ticket_id, "Trigger gate: completed and unchanged — skipping"
                );
                TicketGate::SkipUnchanged
            }
        }
    } else {
        // failed / needs_input / retried / cancelled — allow retry
        TicketGate::Enqueue
    }
}

/// Compute the S3 keys the Jira attachment upload will produce, WITHOUT
/// uploading. Must mirror `upload_image_attachments` exactly — the worker
/// hashes the uploaded keys and stores that on the run record, so the gate's
/// hash has to match.
pub fn expected_attachment_keys(
    team_id: &str,
    ticket_key: &str,
    payload: &serde_json::Value,
) -> Vec<String> {
    let safe_team = team_id.replace('#', "_");
    payload
        .get("image_attachments")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|att| att["data_base64"].as_str().is_some())
                .map(|att| {
                    let filename = att["filename"].as_str().unwrap_or("image.png");
                    format!("attachments/{safe_team}/{ticket_key}/{filename}")
                })
                .collect()
        })
        .unwrap_or_default()
}
