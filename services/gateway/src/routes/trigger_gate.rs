//! Central gate for "should this ticket event start a new run?".
//!
//! Both webhook sources route through here so the dedup semantics are
//! uniform (they used to differ per path, and the underlying queries were
//! broken — DynamoDB applies Limit before FilterExpression, so the old
//! team-partition scans only ever inspected the 5 oldest runs):
//!
//! - a run in flight (running / queued / awaiting_ci) always blocks — a new
//!   run would force-reset the branch out from under the active one
//! - a completed latest run (its PR still open) blocks unless the ticket
//!   CONTENT actually changed (context hash), in which case rework is
//!   allowed; the worker resets the branch and updates that same PR
//! - a merged latest run blocks on any ticket edit: the work shipped, and a
//!   fresh run would rebuild it from scratch as a second PR (people closed
//!   five of those in one afternoon). A person asks explicitly for follow-up
//!   work, which still goes through
//! - a ticket CoderHelm already handled (its latest run completed, asked a
//!   question, reported a change outside the code, or was cancelled — its PR
//!   closed without merging included) starts a new run only when the ticket
//!   CONTENT changed: a status or field change alone never starts one. The
//!   run on a cancelled ticket used to re-run on any edit and open the same
//!   PR again
//! - a ticket a person marked done on the ticket itself ("fixed in the
//!   booking system") stays done until its content changes
//! - a failed / retried latest run never blocks
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

    // In-flight only counts while plausibly ALIVE: a run stuck in "running"
    // (worker crashed pre-terminal) or an abandoned awaiting_ci would
    // otherwise block the ticket forever. Bounds: running/queued can't
    // outlive a 15-min Lambda by much (2h is generous across retries);
    // awaiting_ci is finalized by the 24h wall cap (26h adds slack).
    let now = chrono::Utc::now();
    let age_hours = |i: &std::collections::HashMap<String, AttributeValue>| {
        i.get("updated_at")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|t| now.signed_duration_since(t).num_hours())
            .unwrap_or(i64::MAX)
    };
    if items.iter().any(|i| {
        let h = age_hours(i);
        match status_of(i).as_str() {
            "running" | "queued" => h < 2,
            "awaiting_ci" => h < 26,
            _ => false,
        }
    }) {
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

    // A person marked the ticket done on the ticket itself.
    if let Some(item) = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&common::ticket_state::sk(ticket_id)))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
    {
        let field = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).map(|s| s.as_str());
        if common::ticket_state::done_blocks_run(
            field(common::ticket_state::FIELD_STATE),
            field(common::ticket_state::FIELD_DONE_HASH),
            new_context_hash,
        ) {
            info!(
                team_id,
                ticket_id, "Trigger gate: ticket marked done and unchanged — skipping"
            );
            return TicketGate::SkipUnchanged;
        }
    }

    // "success" is a legacy terminal status from an old resume path;
    // "merged" means the PR landed — rework only on real content change.
    let latest_status = status_of(latest);
    if handled(&latest_status) {
        let stored_hash = latest.get("context_hash").and_then(|v| v.as_s().ok());
        match (new_context_hash, stored_hash) {
            _ if !rework_on_edit(&latest_status) => {
                info!(
                    team_id,
                    ticket_id,
                    "Trigger gate: ticket's PR already merged — an edit doesn't start a new run"
                );
                TicketGate::SkipUnchanged
            }
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

/// The latest run left the ticket handled: only a change to the ticket's
/// content starts another run. "needs_outside_change": the fix is a change in
/// another system. "needs_input": CoderHelm asked; people answer by comment
/// or by editing the ticket. "cancelled": a person stopped it, or closed its
/// PR without merging. Only a failed (or retried) run re-runs on any event.
fn handled(status: &str) -> bool {
    matches!(
        status,
        "completed" | "success" | "merged" | "needs_outside_change" | "needs_input" | "cancelled"
    )
}

/// Whether editing a ticket may start a new run after its latest run ended
/// with `status`. Only while its PR is still open: the run then updates that
/// PR. After a merge, follow-up work is asked for explicitly.
fn rework_on_edit(status: &str) -> bool {
    status != "merged"
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

#[cfg(test)]
mod tests {
    use super::{handled, rework_on_edit};

    #[test]
    fn a_handled_ticket_needs_a_content_change_to_run_again() {
        // The cancelled run whose PR a person closed: a status change on the
        // ticket started the same PR again.
        assert!(handled("cancelled"));
        assert!(handled("needs_outside_change"));
        assert!(handled("needs_input"));
        assert!(handled("completed"));
        assert!(!handled("failed"));
        assert!(!handled("retried"));
    }

    #[test]
    fn an_edit_reworks_an_open_pr_but_never_a_merged_one() {
        assert!(rework_on_edit("completed"));
        assert!(rework_on_edit("success"));
        assert!(!rework_on_edit("merged"));
    }
}
