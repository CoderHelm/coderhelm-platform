//! A run waiting for the install's setup item "AWS role reads"
//! (`awaiting_setup`, `common::role_wait`): the re-checks of the role, and
//! the run going on by itself once the role reads.
//!
//! [`arm`] records the wait and sends the first re-check; [`check`] reads
//! the reads the run waited for again (for an AWS WAF alarm, the alarm's
//! own evidence; for metrics, a ListMetrics; otherwise the role's
//! permission check) and, when none is refused, starts the run again from
//! its ticket: the new run reads the data and opens its pull request.
//! [`wake`] sends a re-check of every waiting run of a team at once: the
//! daily role check, and a read that worked again in another run.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::messages::{RoleReadsWaitMessage, WorkerMessage};
use common::role_wait::{self, Waiting, FAST_POLLS, POLL_SECS};
use common::run_status::{AWAITING_SETUP, CLOSED, RETRIED};
use tracing::{info, warn};

use super::attr_s;
use crate::WorkerState;

type Item = HashMap<String, AttributeValue>;

fn text(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Sends a re-check, after `delay` seconds.
async fn send(state: &WorkerState, m: RoleReadsWaitMessage, delay: i32) {
    let Ok(body) = serde_json::to_string(&WorkerMessage::RoleReadsWait(m.clone())) else {
        return;
    };
    let res = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay.clamp(0, 900))
        .send()
        .await;
    if let Err(e) = res {
        warn!(run_id = %m.run_id, error = %e, "Could not send the AWS role re-check");
    }
}

/// Records the run's wait and sends its first re-check.
pub async fn arm(state: &WorkerState, team_id: &str, w: &Waiting) {
    role_wait::record(&state.dynamo, &state.config.settings_table_name, team_id, w).await;
    send(
        state,
        RoleReadsWaitMessage {
            team_id: team_id.to_string(),
            run_id: w.run_id.clone(),
            polls: 0,
            once: false,
        },
        POLL_SECS,
    )
    .await;
}

/// Sends a re-check of every run of the team waiting for reads in
/// `account` (every account when `None`), now.
pub async fn wake(state: &WorkerState, team_id: &str, account: Option<&str>) {
    let waits = role_wait::list(&state.dynamo, &state.config.settings_table_name, team_id).await;
    for w in waits
        .iter()
        .filter(|w| account.is_none_or(|a| w.accounts.iter().any(|x| x == a)))
    {
        send(
            state,
            RoleReadsWaitMessage {
                team_id: team_id.to_string(),
                run_id: w.run_id.clone(),
                polls: 0,
                once: true,
            },
            0,
        )
        .await;
    }
}

/// The reads the role is still refused in `account`, of those the run
/// waits for. `None` when the role couldn't be taken on (still waiting).
async fn still_refused(
    state: &WorkerState,
    team_id: &str,
    account: &str,
    item: &Item,
    w: &Waiting,
) -> Option<Vec<String>> {
    let session = common::log_findings::session_for(
        &state.dynamo,
        &state.sts,
        &state.config.aws_insights_table_name,
        team_id,
        account,
    )
    .await
    .ok()
    .flatten()?;
    let mut refused: Vec<String> = Vec::new();
    // An AWS WAF alarm: its own evidence, read again.
    if let Some(alarm) = common::alarm_evidence::waf_alarm(&text(item, "ticket_body")) {
        let got = common::alarm_evidence::gather(&session, &alarm, chrono::Utc::now()).await;
        refused.extend(got.refused);
    }
    // Metrics: one ListMetrics.
    if w.actions.iter().any(|a| a.starts_with("cloudwatch:"))
        && !matches!(common::live::metrics::probe(&session).await, Ok(true))
    {
        refused.extend(
            w.actions
                .iter()
                .filter(|a| a.starts_with("cloudwatch:"))
                .cloned(),
        );
    }
    // Anything else: the role's permission check, for the actions it covers.
    let other: Vec<&String> = w
        .actions
        .iter()
        .filter(|a| !a.starts_with("cloudwatch:"))
        .filter(|a| !refused.contains(a))
        .collect();
    let covered_by_alarm = common::alarm_evidence::waf_alarm(&text(item, "ticket_body")).is_some();
    if !other.is_empty() && !covered_by_alarm {
        let accounts = common::live::aws_account::list(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
        )
        .await
        .unwrap_or_default();
        let target = accounts.iter().find(|a| a.account == account)?.target();
        let c = common::live::permissions::check(
            &state.sts,
            &target,
            "role-wait",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await;
        if !c.works {
            return None;
        }
        refused.extend(other.into_iter().filter(|a| c.missing.contains(a)).cloned());
    }
    Some(refused)
}

/// One re-check of a waiting run.
pub async fn check(state: &WorkerState, m: &RoleReadsWaitMessage) {
    let table = &state.config.settings_table_name;
    let Some(w) = role_wait::get(&state.dynamo, table, &m.team_id, &m.run_id).await else {
        return;
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&m.team_id))
        .key("run_id", attr_s(&m.run_id))
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let Some(item) = item else {
        role_wait::remove(&state.dynamo, table, &m.team_id, &m.run_id).await;
        return;
    };
    let status = text(&item, "status");
    if status != AWAITING_SETUP {
        // A closed run may be reopened and wait again: the daily check
        // finds it then. Anything else ended the wait.
        if status != CLOSED {
            role_wait::remove(&state.dynamo, table, &m.team_id, &m.run_id).await;
        }
        return;
    }
    let mut refused: Vec<String> = Vec::new();
    let mut reachable = true;
    for account in &w.accounts {
        match still_refused(state, &m.team_id, account, &item, &w).await {
            Some(r) => refused.extend(r),
            None => reachable = false,
        }
    }
    if reachable && role_wait::ready(&w, &refused) {
        go_on(state, &m.team_id, &m.run_id, &item, &w).await;
        return;
    }
    info!(run_id = %m.run_id, refused = ?refused, polls = m.polls, "The AWS role still lacks reads the run waits for");
    if !m.once && m.polls + 1 < FAST_POLLS {
        send(
            state,
            RoleReadsWaitMessage {
                polls: m.polls + 1,
                ..m.clone()
            },
            POLL_SECS,
        )
        .await;
    }
}

/// The role reads now: the run starts again from its ticket (a new run that
/// reads the data and opens the pull request), once.
async fn go_on(state: &WorkerState, team_id: &str, run_id: &str, item: &Item, w: &Waiting) {
    let Some(mut ticket) = super::stop_judge::restart_message(item, Vec::new()) else {
        warn!(run_id, "The waiting run's ticket couldn't be rebuilt");
        return;
    };
    ticket.team_id = team_id.to_string();
    // The same sender: an alert's run reads the alarm's evidence again.
    let sender = text(item, "sender");
    if !sender.is_empty() {
        ticket.sender = sender;
    }
    ticket.requester = Some(common::requester::Requester::coderhelm("role reads"));
    let new_run = ticket.new_run_id.clone().unwrap_or_default();
    let now = chrono::Utc::now().to_rfc3339();
    // Only while it still waits: one re-check goes on, never two.
    let claimed = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #s = :r, status_run_id = :sri, retried_as = :n, updated_at = :t")
        .condition_expression("#s = :w")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":r", attr_s(RETRIED))
        .expression_attribute_values(":sri", attr_s(&format!("{RETRIED}#{run_id}")))
        .expression_attribute_values(":n", attr_s(&new_run))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":w", attr_s(AWAITING_SETUP))
        .send()
        .await
        .is_ok();
    if !claimed {
        return;
    }
    let table = &state.config.settings_table_name;
    role_wait::remove(&state.dynamo, table, team_id, run_id).await;
    for a in &w.accounts {
        common::aws_role_reads::clear(&state.dynamo, table, team_id, a).await;
    }
    let Ok(body) = serde_json::to_string(&WorkerMessage::Ticket(ticket)) else {
        return;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => {
            super::add_progress_note(
                state,
                team_id,
                run_id,
                &format!(
                    "The AWS role reads {} now: going on in run {new_run}, which reads the data \
                     and opens the pull request.",
                    w.actions.join(", ")
                ),
            )
            .await;
            info!(
                run_id,
                new_run, "AWS role reads arrived: the waiting run goes on"
            );
        }
        Err(e) => warn!(run_id, error = %e, "Could not queue the waiting run's ticket"),
    }
}
