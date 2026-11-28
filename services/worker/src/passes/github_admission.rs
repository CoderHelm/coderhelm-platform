//! The worker's side of `common::github_budget::admission`: a run's next
//! GitHub-heavy step (a ticket step, a CI round) is admitted against the
//! installation's live budget at the one place the worker picks up a message,
//! before the step starts and before its time counts as work.
//!
//! - The installation's line and the steps holding a share live on the
//!   analytics table beside the budget (`GITHUB#<installation>` / `ADMIT`),
//!   written only when nobody wrote in between (`adm_ver`).
//! - What a step spends is learned there too: each handled message adds its
//!   counted requests by caller (`flush` in `clients::github_budget`).
//! - A step that waits is sent again on its own queue with a delay until its
//!   expected start (one SQS delay at most, then it asks again), and its run
//!   page says when it starts, in the line the GitHub wait already uses
//!   (`note_github_wait`). Its place in line is kept.
//! - Anything that can't be read or written lets the step start: the
//!   per-request rules (`common::github_budget::decide`) and the wait for a
//!   spent limit (`common::github_wait`) still hold behind it.

use aws_sdk_dynamodb::types::AttributeValue;
use common::github_budget::{
    self as rules,
    admission::{self, Admit, Ledger},
};
use common::messages::WorkerMessage;
use common::run_status::EndedValues;
use tracing::{info, warn};

use crate::WorkerState;

/// The sort key of an installation's admissions.
pub const PERIOD: &str = "ADMIT";

/// Writes of the ledger tried before the step starts unadmitted.
const WRITES: u32 = 8;

/// Which queue a waiting step goes back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Queue {
    Tickets,
    CiFix,
}

/// A message that is a run's GitHub-heavy step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub installation_id: u64,
    pub team_id: String,
    /// The line's key: the run, or the ticket before its run has an id.
    pub key: String,
    /// The run whose page says it waits, when it has one.
    pub run_id: Option<String>,
    /// Whose requests it makes (`common::github_budget::caller`).
    pub caller: &'static str,
    pub queue: Queue,
}

fn key_safe(s: &str) -> String {
    s.chars()
        .map(|c| if c == ';' || c == '=' { '_' } else { c })
        .collect()
}

/// The step a message is, if it is one: a ticket step or a CI round of a
/// run. A message hopping through a wait for GitHub isn't a step yet (the
/// hop touches nothing), and nothing else is admitted here.
pub fn step_of(message: &WorkerMessage, now: u64) -> Option<Step> {
    use rules::caller as who;
    match message {
        WorkerMessage::Ticket(t) => {
            if t.installation_id == 0
                || common::github_wait::hop(t.github_wait.until, now).is_some()
            {
                return None;
            }
            let run = t
                .continuation_run_id
                .clone()
                .filter(|r| !r.is_empty() && t.same_run())
                .or_else(|| t.new_run_id.clone().filter(|r| !r.is_empty()));
            let key = match &run {
                Some(r) => key_safe(r),
                None => key_safe(&format!("ticket:{}:{}", t.team_id, t.ticket_id)),
            };
            Some(Step {
                installation_id: t.installation_id,
                team_id: t.team_id.clone(),
                key,
                run_id: run,
                caller: who::RUN,
                queue: Queue::Tickets,
            })
        }
        WorkerMessage::Resume(m) => {
            if m.installation_id == 0
                || m.run_id.is_empty()
                || common::github_wait::hop(m.github_wait.until, now).is_some()
            {
                return None;
            }
            Some(Step {
                installation_id: m.installation_id,
                team_id: m.team_id.clone(),
                key: key_safe(&m.run_id),
                run_id: Some(m.run_id.clone()),
                caller: who::CI,
                queue: Queue::CiFix,
            })
        }
        _ => None,
    }
}

/// The SQS delay of a step that waits until `start_at`: at least a minute,
/// at most one delay, then it asks again.
pub fn delay_for(start_at: u64, now: u64) -> u64 {
    start_at
        .saturating_sub(now)
        .clamp(60, common::github_wait::MAX_HOP_SECS)
}

/// The record attribute a caller's learned step cost is kept under.
pub fn cost_attrs(caller: &str) -> (String, String) {
    let base = rules::count_attr(caller);
    (format!("cost_sum_{base}"), format!("cost_n_{base}"))
}

fn n(v: u64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn num(item: &std::collections::HashMap<String, AttributeValue>, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn now_epoch() -> (u64, u64) {
    let t = chrono::Utc::now();
    (
        t.timestamp().max(0) as u64,
        t.timestamp_millis().max(0) as u64,
    )
}

/// What admission decided for a message.
#[derive(Debug)]
pub struct Admitted {
    /// Whether the message runs now. False: it was sent again to wait its
    /// turn.
    pub go: bool,
    /// The step holding a share, released when it ends.
    pub step: Option<Step>,
}

/// Reads the installation's ledger, applies `f`, and writes it back if
/// nobody wrote in between. `f` also gets the caller's learned cost.
async fn update<R>(
    state: &WorkerState,
    step: &Step,
    f: impl Fn(&mut Ledger, u64) -> R,
) -> Option<(R, Ledger)> {
    let table = &state.config.analytics_table_name;
    if table.is_empty() {
        return None;
    }
    let pk = rules::partition(step.installation_id);
    let (sum_attr, n_attr) = cost_attrs(step.caller);
    let limit = crate::clients::github::budget_of(step.installation_id)
        .map_or(rules::SMALLEST_LIMIT, |b| b.limit);
    for _ in 0..WRITES {
        let item = match state
            .dynamo
            .get_item()
            .table_name(table)
            .key("team_id", s(&pk))
            .key("period", s(PERIOD))
            .consistent_read(true)
            .send()
            .await
        {
            Ok(o) => o.item.unwrap_or_default(),
            Err(e) => {
                warn!(installation = step.installation_id, error = %e, "GitHub admission: could not read the line");
                return None;
            }
        };
        let text = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let ver = num(&item, "adm_ver");
        let mut ledger = Ledger::from_attrs(&text("adm_held"), &text("adm_line"));
        let need = admission::cost(num(&item, &sum_attr), num(&item, &n_attr), limit);
        let out = f(&mut ledger, need);
        let (held, line) = ledger.to_attrs();
        let cond = if ver == 0 {
            "attribute_not_exists(adm_ver)"
        } else {
            "adm_ver = :v0"
        };
        let mut req = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("team_id", s(&pk))
            .key("period", s(PERIOD))
            .update_expression("SET adm_held = :h, adm_line = :l, adm_ver = :v1")
            .condition_expression(cond)
            .expression_attribute_values(":h", s(&held))
            .expression_attribute_values(":l", s(&line))
            .expression_attribute_values(":v1", n(ver + 1));
        if ver > 0 {
            req = req.expression_attribute_values(":v0", n(ver));
        }
        match req.send().await {
            Ok(_) => return Some((out, ledger)),
            Err(e) if common::run_fail::is_condition_failed(&e) => continue,
            Err(e) => {
                warn!(installation = step.installation_id, error = %e, "GitHub admission: could not write the line");
                return None;
            }
        }
    }
    None
}

/// Whether `message` runs now, against the installation's live budget. A
/// step that doesn't fit is sent again to wait its turn, and its run page
/// says when it starts.
pub async fn admit(state: &WorkerState, message: &WorkerMessage, raw: &str) -> Admitted {
    let (now, now_ms) = now_epoch();
    let Some(step) = step_of(message, now) else {
        return Admitted {
            go: true,
            step: None,
        };
    };
    // GitHub's last word on the installation, from any worker.
    crate::clients::github_budget::refresh(state, step.installation_id).await;
    let budget = crate::clients::github::budget_of(step.installation_id);
    let limited = crate::clients::github_budget::limited_until(step.installation_id, now);
    let decided = update(state, &step, |l, need| {
        let was_in_line = l.line.iter().any(|w| w.key == step.key);
        (
            l.admit(&step.key, need, budget, limited, now, now_ms),
            was_in_line,
        )
    })
    .await;
    let Some(((decision, was_in_line), _)) = decided else {
        return Admitted {
            go: true,
            step: None,
        };
    };
    match decision {
        Admit::Go => {
            if was_in_line {
                if let Some(run) = &step.run_id {
                    super::clear_github_wait(state, &step.team_id, run).await;
                }
            }
            Admitted {
                go: true,
                step: Some(step),
            }
        }
        Admit::Wait { ahead, start_at } => {
            let delay = delay_for(start_at, now);
            if !send_again(state, &step, raw, delay).await {
                // Never dropped: it runs now, under the per-request rules.
                release(state, &step).await;
                return Admitted {
                    go: true,
                    step: None,
                };
            }
            let text = admission::waiting_text(ahead, start_at);
            info!(
                installation = step.installation_id,
                key = %step.key,
                ahead,
                start_at,
                delay,
                "github_admission: the step waits its turn for the installation's budget"
            );
            if let Some(run) = &step.run_id {
                if !was_in_line && claim_note(state, &step.team_id, run, now).await {
                    super::note_github_wait(state, &step.team_id, run, &text).await;
                } else {
                    set_wait_line(state, &step.team_id, run, &text).await;
                }
            }
            Admitted {
                go: false,
                step: None,
            }
        }
    }
}

/// The step ended: its share is free for the runs in line.
pub async fn release(state: &WorkerState, step: &Step) {
    let _ = update(state, step, |l, _| l.release(&step.key)).await;
}

/// Whether a run that last said it waits its turn in hour `last` (`now /
/// 3600`) says it again at `now`: once an hour at most, however many of its
/// steps wait.
pub fn notes_now(last: Option<u64>, now: u64) -> bool {
    last.is_none_or(|h| h < now / 3600)
}

/// Claims the run's one note of this hour (`adm_noted_hour`), so a run whose
/// steps wait one after another says so once ([`notes_now`]). Only the
/// writer that saw the last hour as it stands gets it.
async fn claim_note(state: &WorkerState, team_id: &str, run_id: &str, now: u64) -> bool {
    let last = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", s(team_id))
        .key("run_id", s(run_id))
        .projection_expression("adm_noted_hour")
        .consistent_read(true)
        .send()
        .await
    {
        Ok(o) => o
            .item
            .as_ref()
            .and_then(|i| i.get("adm_noted_hour"))
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok()),
        Err(_) => return false,
    };
    if !notes_now(last, now) {
        return false;
    }
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", s(team_id))
        .key("run_id", s(run_id))
        .update_expression("SET adm_noted_hour = :h")
        .expression_attribute_values(":h", n(now / 3600));
    req = match last {
        Some(h) => req
            .condition_expression("attribute_exists(run_id) AND adm_noted_hour = :last")
            .expression_attribute_values(":last", n(h)),
        None => req.condition_expression(
            "attribute_exists(run_id) AND attribute_not_exists(adm_noted_hour)",
        ),
    };
    req.send().await.is_ok()
}

/// The run page's waiting line, updated without another note.
async fn set_wait_line(state: &WorkerState, team_id: &str, run_id: &str, text: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", s(team_id))
        .key("run_id", s(run_id))
        .update_expression("SET github_wait = :w")
        .condition_expression(format!(
            "attribute_exists(run_id) AND {}",
            common::run_status::not_ended("#s")
        ))
        .expression_attribute_names("#s", "status")
        .ended_values()
        .expression_attribute_values(":w", s(text))
        .send()
        .await;
}

async fn send_again(state: &WorkerState, step: &Step, raw: &str, delay: u64) -> bool {
    let queue = match step.queue {
        Queue::Tickets => &state.config.ticket_queue_url,
        Queue::CiFix => &state.config.ci_fix_queue_url,
    };
    if queue.is_empty() {
        return false;
    }
    state
        .sqs
        .send_message()
        .queue_url(queue)
        .message_body(raw)
        .delay_seconds(delay as i32)
        .send()
        .await
        .map_err(|e| warn!(key = %step.key, error = %e, "GitHub admission: could not send the step to wait"))
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ResumeMessage, TicketMessage};

    const NOW: u64 = 1_794_000_000;

    #[test]
    fn a_ticket_step_and_a_ci_round_are_admitted_and_nothing_else() {
        let fresh = WorkerMessage::Ticket(TicketMessage {
            team_id: "T".into(),
            ticket_id: "GH-7".into(),
            installation_id: 9,
            ..Default::default()
        });
        let s = step_of(&fresh, NOW).unwrap();
        assert_eq!(s.key, "ticket:T:GH-7");
        assert_eq!(
            (s.caller, s.queue, s.run_id),
            (rules::caller::RUN, Queue::Tickets, None)
        );
        let cont = WorkerMessage::Ticket(TicketMessage {
            team_id: "T".into(),
            ticket_id: "GH-7".into(),
            installation_id: 9,
            continuation: 2,
            continuation_run_id: Some("R1".into()),
            ..Default::default()
        });
        assert_eq!(step_of(&cont, NOW).unwrap().run_id.as_deref(), Some("R1"));
        let resume = WorkerMessage::Resume(ResumeMessage {
            team_id: "T".into(),
            run_id: "R1".into(),
            installation_id: 9,
            ..Default::default()
        });
        let s = step_of(&resume, NOW).unwrap();
        assert_eq!((s.caller, s.queue), (rules::caller::CI, Queue::CiFix));
        // A hop through a wait for GitHub isn't a step.
        let mut hopping = ResumeMessage {
            team_id: "T".into(),
            run_id: "R1".into(),
            installation_id: 9,
            ..Default::default()
        };
        hopping.github_wait.until = NOW + 600;
        assert_eq!(step_of(&WorkerMessage::Resume(hopping), NOW), None);
        assert_eq!(step_of(&WorkerMessage::ReviewSweep, NOW), None);
    }

    #[test]
    fn a_waiting_step_asks_again_within_one_delay() {
        assert_eq!(delay_for(NOW + 5, NOW), 60);
        assert_eq!(delay_for(NOW + 300, NOW), 300);
        assert_eq!(delay_for(NOW + 3_000, NOW), 900);
        assert_eq!(
            cost_attrs(rules::caller::RUN),
            ("cost_sum_req_run".to_string(), "cost_n_req_run".to_string())
        );
    }
}
