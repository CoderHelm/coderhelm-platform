//! A run waiting for another change to merge (`awaiting_change`,
//! `common::change_wait`): the wait itself, its one note, and what the run
//! does when the change moves (a merge webhook, the 15-minute sweep).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::change_wait::{self as cw, Dependency, OtherWork, Settled};
use common::run_status::AWAITING_CHANGE;
use tracing::{info, warn};

use super::{add_progress_note, attr_s, RunUsageValues};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use common::run_status::EndedValues;

/// The team's latest run for each ticket key, as [`OtherWork`].
async fn other_work(state: &WorkerState, team_id: &str, keys: &[String]) -> Vec<OtherWork> {
    let mut out = Vec::new();
    for key in keys.iter().take(8) {
        let Ok(page) = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .index_name("ticket-index")
            .key_condition_expression("team_id = :t AND ticket_id = :k")
            .expression_attribute_values(":t", attr_s(team_id))
            .expression_attribute_values(":k", attr_s(key))
            .send()
            .await
        else {
            continue;
        };
        let mut items: Vec<&HashMap<String, AttributeValue>> = page.items().iter().collect();
        items.sort_by_key(|i| std::cmp::Reverse(s(i, "run_id").to_string()));
        if let Some(i) = items.first() {
            out.push(OtherWork {
                ticket: key.clone(),
                run_id: s(i, "run_id").to_string(),
                repo: s(i, "repo").to_string(),
                pr: n(i, "pr_number"),
                status: s(i, "status").to_string(),
            });
        }
    }
    out
}

fn s<'a>(i: &'a HashMap<String, AttributeValue>, k: &str) -> &'a str {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|x| x.as_str())
        .unwrap_or("")
}

fn n(i: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|x| x.parse().ok())
        .unwrap_or(0)
}

/// Ticket keys the run's record links (`related_criteria`'s `from`).
fn linked_keys(related: &str) -> Vec<String> {
    serde_json::from_str::<Vec<serde_json::Value>>(related)
        .unwrap_or_default()
        .iter()
        .filter_map(|v| v["from"].as_str().map(str::to_string))
        .collect()
}

/// The change this run waits for, from the planner's `cause`: `None` when
/// the cause doesn't say the target is missing or names no other change.
pub async fn dependency_for(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    cause: &str,
) -> Option<Dependency> {
    if !cw::target_missing(cause) {
        return None;
    }
    let record = record(state, &msg.team_id, run_id)
        .await
        .unwrap_or_default();
    let linked = linked_keys(s(&record, "related_criteria"));
    let text = format!("{}\n{}", msg.title, msg.body);
    let mut keys = cw::ticket_keys(cause);
    keys.extend(linked.iter().cloned());
    keys.extend(cw::ticket_keys(&text));
    keys.retain(|k| !k.eq_ignore_ascii_case(&msg.ticket_id));
    keys.dedup();
    let work = other_work(state, &msg.team_id, &keys).await;
    let now = chrono::Utc::now().to_rfc3339();
    cw::find(cause, &text, &linked, &msg.ticket_id, &work, &now)
}

async fn record(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Option<HashMap<String, AttributeValue>> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .send()
        .await
        .ok()?
        .item()
        .cloned()
}

/// The run waits for `dep`: kept on the run (its page shows it), the
/// ticket told once, the continuation kept for when it merges. Never a
/// failure and never a plan-check failure.
pub async fn begin(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    mut dep: Dependency,
    usage: &TokenUsage,
) {
    let earlier = record(state, &msg.team_id, run_id)
        .await
        .and_then(|r| Dependency::from_json(s(&r, cw::FIELD)));
    let told = !cw::should_note(earlier.as_ref(), &dep);
    dep.noted = true;
    super::recover::keep_resume_message(state, msg, run_id).await;
    let line = cw::note(&dep);
    let delta = usage.unrecorded();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, #w = :w, error_message = :em, current_pass = :cp, \
             updated_at = :t, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c \
             REMOVE failed_pass, error_detail, error_kind",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#w", cw::FIELD)
        .ended_values()
        .expression_attribute_values(":s", attr_s(AWAITING_CHANGE))
        .expression_attribute_values(":sri", common::run_ending::working(common::run_ending::Working::AwaitingChange, run_id).sri_attr())
        .expression_attribute_values(":w", attr_s(&dep.to_json()))
        .expression_attribute_values(":em", attr_s(&line))
        .expression_attribute_values(":cp", attr_s("waiting"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .usage_values(&delta)
        .send()
        .await;
    if let Err(e) = res {
        warn!(run_id, error = %e, "Could not set the run waiting for another change");
        return;
    }
    usage.mark_recorded(&delta);
    info!(run_id, dependency = %dep.label(), "Waiting for another change to merge");
    if !told {
        add_progress_note(state, &msg.team_id, run_id, &line).await;
        tell_ticket(state, msg, &line, "Waiting for another change").await;
    }
}

async fn tell_ticket(state: &WorkerState, msg: &TicketMessage, line: &str, title: &str) {
    let say = common::conversation::Say::status("run", line);
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let Ok(gh) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) else {
            return;
        };
        if let Err(e) =
            super::conversation::github(&gh, &msg.repo_owner, &msg.repo_name, msg.issue_number, say)
                .await
        {
            warn!(error = %e, "Could not tell the issue about the wait");
        }
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        if let Err(e) =
            super::conversation::jira(state, &msg.team_id, &msg.ticket_id, say, "status", title)
                .await
        {
            warn!(error = %e, "Could not tell the ticket about the wait");
        }
    }
}

/// What [`check_all`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Checked {
    pub waiting: usize,
    pub resumed: usize,
    pub stopped: usize,
}

/// Every waiting run (of `team`, or all): its dependency read again, and
/// the run resumed (merged: planned again against the new base), stopped
/// as "needs you" (closed without merging), or left waiting with its
/// page saying why (the dependency's run stopped).
pub async fn check_all(state: &WorkerState, team: Option<&str>) -> Checked {
    let mut out = Checked::default();
    // Read to the last page either way: one team's waiting runs by the
    // status index, or every team's runs by key (a capped scan of the runs
    // table stopped before the later runs).
    let read = match team {
        Some(t) => {
            super::team_runs::read_all(|start| async move {
                state
                    .dynamo
                    .query()
                    .table_name(&state.config.runs_table_name)
                    .index_name("status-index")
                    .key_condition_expression("team_id = :t AND begins_with(status_run_id, :p)")
                    .expression_attribute_values(":t", attr_s(t))
                    .expression_attribute_values(":p", attr_s(&format!("{AWAITING_CHANGE}#")))
                    .set_exclusive_start_key(start)
                    .send()
                    .await
                    .map(|p| (p.items().to_vec(), p.last_evaluated_key().cloned()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
        None => {
            let filter = super::team_runs::Filter {
                expr: "#s = :w",
                names: vec![("#s", "status")],
                values: vec![(":w", attr_s(AWAITING_CHANGE))],
                projection: None,
            };
            Ok(super::team_runs::matching(state, "Waiting runs", &filter).await)
        }
    };
    let items: Vec<HashMap<String, AttributeValue>> = match read {
        Ok(items) => items,
        Err(e) => {
            warn!(error = %e, "Waiting runs couldn't be listed");
            Vec::new()
        }
    }
    .into_iter()
    .filter(|i| s(i, "status") == AWAITING_CHANGE)
    .collect();
    out.waiting = items.len();
    for item in items {
        match check_one(state, &item).await {
            Some(true) => out.resumed += 1,
            Some(false) => out.stopped += 1,
            None => {}
        }
    }
    info!(
        waiting = out.waiting,
        resumed = out.resumed,
        stopped = out.stopped,
        "Waiting runs checked"
    );
    out
}

/// `Some(true)`: resumed; `Some(false)`: stopped for a person; `None`:
/// still waiting.
async fn check_one(state: &WorkerState, item: &HashMap<String, AttributeValue>) -> Option<bool> {
    let team_id = s(item, "team_id");
    let run_id = s(item, "run_id");
    let mut dep = Dependency::from_json(s(item, cw::FIELD))?;
    // A pull request that wasn't open when the wait began: its run's now.
    if dep.pr == 0 && !dep.ticket.is_empty() {
        if let Some(w) = other_work(state, team_id, std::slice::from_ref(&dep.ticket))
            .await
            .into_iter()
            .next()
        {
            dep.repo = w.repo;
            dep.pr = w.pr;
            dep.run_id = w.run_id;
        }
    }
    let gh = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        n(item, "installation_id"),
        &state.http,
    )
    .ok()?;
    let pr = match dep.repo.split_once('/') {
        Some((o, r)) if dep.pr > 0 => gh.get_pull_request(o, r, dep.pr).await.ok(),
        _ => None,
    };
    let pr_state = pr.as_ref().map(|p| {
        (
            p["state"].as_str().unwrap_or("open").to_string(),
            p["merged"].as_bool().unwrap_or(false),
        )
    });
    let dep_status = if dep.run_id.is_empty() {
        None
    } else {
        record(state, team_id, &dep.run_id)
            .await
            .map(|r| s(&r, "status").to_string())
    };
    let settled = cw::settle(
        pr_state.as_ref().map(|(st, m)| (st.as_str(), *m)),
        dep_status.as_deref(),
    );
    let resend = super::recover::resend_for(item)?;
    let super::recover::Resend::Ticket(mut next) = resend else {
        return None;
    };
    match settled {
        Settled::Waiting => None,
        Settled::DependencyStuck(why) => {
            let line = cw::stuck_line(&dep, &why);
            if s(item, "error_message") != line {
                set_line(state, team_id, run_id, &line).await;
                add_progress_note(state, team_id, run_id, &line).await;
            }
            None
        }
        Settled::ClosedUnmerged => {
            let reason = cw::closed_reason(&dep);
            add_progress_note(state, team_id, run_id, &reason).await;
            tell_ticket(
                state,
                &next,
                &reason,
                "The change this run waited for closed",
            )
            .await;
            let needs = common::run_recovery::NeedsYou {
                kind: common::run_recovery::Kind::NeedsPerson,
                why: common::handback::HumanOnly::ProductDecision,
                action: format!(
                    "Reopen and merge {}, or change this ticket, then press Retry",
                    dep.label()
                ),
                detail: reason,
            };
            super::recover::stop_for_person(
                state,
                &next,
                run_id,
                &needs,
                &TokenUsage::default(),
                0,
            )
            .await;
            Some(false)
        }
        Settled::Merged => {
            // Planned again against the new base: the plan made before the
            // merge said there was nothing to change yet.
            if let Some((o, r)) = s(item, "repo").split_once('/') {
                if let Ok(b) = gh.get_default_branch(o, r).await {
                    next.base_branch = b;
                }
            }
            super::plan::unmark_plan_run(state, &next).await;
            super::checkpoint_back_to_plan(state, team_id, run_id).await;
            let line = format!(
                "{} merged: planning again against the new base and going on",
                dep.label()
            );
            let body = serde_json::to_string(&crate::models::WorkerMessage::Ticket(*next)).ok()?;
            let sent = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(body)
                .delay_seconds(5)
                .send()
                .await
                .is_ok();
            if sent {
                add_progress_note(state, team_id, run_id, &line).await;
            }
            Some(sent)
        }
    }
}

async fn set_line(state: &WorkerState, team_id: &str, run_id: &str, line: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET error_message = :em, updated_at = :t")
        .condition_expression("#s = :w")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":w", attr_s(AWAITING_CHANGE))
        .expression_attribute_values(":em", attr_s(line))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

/// A run left failed because its plan "waits for nothing" while it waits
/// for another change (before this release): the self-heal sweep sets it
/// waiting, with no new plan before that change merges.
pub async fn wait_failed_run(state: &WorkerState, item: &HashMap<String, AttributeValue>) -> bool {
    let Some(super::recover::Resend::Ticket(msg)) = super::recover::resend_for(item) else {
        return false;
    };
    let cause = s(item, "error_message");
    let Some(dep) = dependency_for(state, &msg, s(item, "run_id"), cause).await else {
        return false;
    };
    // `begin` writes only over a run that isn't ended; a failed run is not.
    begin(state, &msg, s(item, "run_id"), dep, &TokenUsage::default()).await;
    true
}
