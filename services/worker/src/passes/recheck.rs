//! A hold CoderHelm's own checks wrote is checked again when their code
//! changes.
//!
//! A run finished and its pull request stayed a draft: a line under Open
//! items said a check the change didn't pass, written by a rule that was
//! wrong. The rule was fixed, but the line was text in the description and
//! nothing ran the check again, so the pull request stayed held until a
//! person edited the description.
//!
//! The rules:
//!
//! - Every blocked line names the version of the code that wrote it
//!   (`proof::checked_by`). A line with no version was written before lines
//!   carried one.
//! - When `ready::settle` finds the pull request held on such a line written
//!   by other code than the code running now, it queues a fresh step of the
//!   run that resumes at the checks on the open pull request
//!   (`recheck_at_checks`). The checks whose failure is a hold run again on
//!   the head even if it didn't move (`run_guard::Ledger::for_checks_version`),
//!   the Open items are written from what they find, and the run finishes as
//!   any run does: `settle` marks the pull request ready or records the holds
//!   that are still true.
//! - Once per run and code version, recorded on the run (`recheck_version`)
//!   in the same write that claims it, so two deciders can't both start it
//!   and a check that still fails on new code never loops.
//! - Not for a run the run guard stopped (a person has it), a pull request a
//!   person put back to draft, or a closed one.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use super::{attr_s, steps};
use crate::models::{TicketMessage, TicketSource};
use crate::WorkerState;

/// The run's last step's message, rebuilt from its record, with the branch
/// the run works on. `None` when the record lacks what a step needs.
pub fn message_of(item: &HashMap<String, AttributeValue>) -> Option<(TicketMessage, String)> {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let (owner, name) = s("repo")
        .split_once('/')
        .map(|(a, b)| (a.to_string(), b.to_string()))?;
    let branch = s("branch");
    let installation_id = n("installation_id");
    if owner.is_empty() || name.is_empty() || branch.is_empty() || installation_id == 0 {
        return None;
    }
    let source = match s("ticket_source").as_str() {
        "jira" => TicketSource::Jira,
        "alert" => TicketSource::Alert,
        _ => TicketSource::Github,
    };
    let requester = Some(s("requester_key"))
        .filter(|k| !k.is_empty())
        .map(|key| common::requester::Requester {
            key,
            name: s("requester_name"),
            email: s("requester_email"),
        });
    let base_branch = Some(s("base_branch")).filter(|b| !b.is_empty());
    // The step after the run's last one (`run_step`, from 1).
    let last_step = n("run_step").max(1) as u32;
    let mut msg = TicketMessage {
        team_id: s("team_id"),
        installation_id,
        source,
        ticket_id: s("ticket_id"),
        title: s("title"),
        body: s("ticket_body"),
        repo_owner: owner,
        repo_name: name,
        issue_number: n("issue_number"),
        sender: s("sender"),
        requester,
        image_attachments: item
            .get("image_attachments")
            .and_then(|v| v.as_s().ok())
            .and_then(|j| serde_json::from_str(j).ok())
            .unwrap_or_default(),
        continuation: last_step - 1,
        continuation_run_id: Some(s("run_id")).filter(|r| !r.is_empty()),
        ..Default::default()
    };
    if let Some(b) = base_branch {
        msg.base_branch = b;
    }
    (!msg.team_id.is_empty() && msg.continuation_run_id.is_some()).then_some((msg, branch))
}

/// What the stages the step skips carried: a description with no security
/// item had its security audit pass (each finding, or an audit that didn't
/// run, is an item the pull request stays a draft on). With one, the audit
/// runs again in the step.
pub fn carry_of(body: &str) -> Option<steps::Carry> {
    (!super::proof::holds_security_items(body)).then(|| steps::Carry {
        security: super::security::Verdict::Passed,
        security_round: None,
        generated: Vec::new(),
    })
}

/// Claims the check again for `version`: true for the one decider that may
/// start it. Never for a run the guard stopped or one not finished.
async fn claim(state: &WorkerState, team_id: &str, run_id: &str, version: &str) -> bool {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET recheck_version = :v, recheck_at = :t")
        .condition_expression(
            "attribute_exists(run_id) AND attribute_not_exists(guard_open) \
             AND #s IN (:done, :old_done, :held) \
             AND (attribute_not_exists(recheck_version) OR recheck_version <> :v)",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":v", attr_s(version))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
        .expression_attribute_values(":old_done", attr_s(common::run_status::SUCCESS))
        // A condition value only, never a status written here.
        .expression_attribute_values(
            ":held",
            AttributeValue::S(common::run_status::NEEDS_INPUT.to_string()),
        )
        .send()
        .await;
    match res {
        Ok(_) => true,
        Err(e) => {
            if !format!("{e:?}").contains("ConditionalCheckFailed") {
                warn!(run_id, error = %e, "Could not claim checking the pull request again");
            }
            false
        }
    }
}

/// The claim given back when the step couldn't be queued: the next decision
/// tries again.
async fn release(state: &WorkerState, team_id: &str, run_id: &str, version: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("REMOVE recheck_version, recheck_at")
        .condition_expression("recheck_version = :v")
        .expression_attribute_values(":v", attr_s(version))
        .send()
        .await;
}

/// Starts checking a held pull request again with the code running now
/// (see the module's rules). True when a step is queued: the pull request
/// waits for it instead of staying held on the old line.
pub async fn start(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pr: u64,
    body: &str,
    version: &str,
) -> bool {
    if run_id.is_empty() || version.is_empty() {
        return false;
    }
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .consistent_read(true)
        .send()
        .await
    {
        Ok(o) => o.item.unwrap_or_default(),
        Err(e) => {
            warn!(run_id, error = %e, "Could not read the run to check its pull request again");
            return false;
        }
    };
    let Some((msg, branch)) = message_of(&item) else {
        warn!(
            run_id,
            pr, "The run's record lacks what a fresh step needs: its hold stays as written"
        );
        return false;
    };
    if !claim(state, team_id, run_id, version).await {
        return false;
    }
    let why = "The check that holds this pull request was written by an earlier version of \
               CoderHelm's checks";
    match super::recheck_at_checks(state, &msg, run_id, &branch, carry_of(body).as_ref(), why).await
    {
        super::Handoff::Continued => {
            info!(
                run_id,
                pr, version, "Checking the held pull request again with the checks' new code"
            );
            true
        }
        super::Handoff::Stopped(reason) => {
            warn!(run_id, pr, reason = %reason, "Could not check the held pull request again");
            release(state, team_id, run_id, version).await;
            false
        }
    }
}
