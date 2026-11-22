//! The claim every fresh run of a ticket takes before it writes anything
//! (`common::one_run`): one run per ticket, whatever carried it in.

use aws_sdk_dynamodb::types::AttributeValue;
use common::one_run::{self, Claim, Holder};
use tracing::{info, warn};

use crate::models::TicketMessage;
use crate::WorkerState;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The ticket a GitHub issue was opened for, from the stored link from the
/// issue to the plan task that opened it (`plan_execute`).
async fn linked_ticket(state: &WorkerState, msg: &TicketMessage) -> Option<String> {
    if msg.issue_number == 0 || state.config.settings_table_name.is_empty() {
        return None;
    }
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&msg.team_id))
        .key(
            "sk",
            s(&common::plan_dependents::github_link_sk(
                &repo,
                msg.issue_number,
            )),
        )
        .projection_expression("source_ticket")
        .send()
        .await
        .ok()?
        .item?;
    item.get("source_ticket")
        .and_then(|v| v.as_s().ok())
        .filter(|k| !k.is_empty())
        .cloned()
}

/// The canonical key of the ticket `msg` is for.
pub async fn ticket_key(state: &WorkerState, msg: &TicketMessage) -> String {
    let linked = match msg.source {
        common::messages::TicketSource::Github => linked_ticket(state, msg).await,
        _ => None,
    };
    one_run::canonical(
        &msg.source,
        &msg.ticket_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        &msg.body,
        linked.as_deref(),
    )
}

async fn holder(state: &WorkerState, team_id: &str, sk: &str) -> Option<Holder> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", s(team_id))
        .key("sk", s(sk))
        .send()
        .await
        .ok()?
        .item?;
    let get = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let run_id = get(&item, "run_id");
    let now = chrono::Utc::now();
    let age = |t: &str| {
        chrono::DateTime::parse_from_rfc3339(t)
            .ok()
            .map(|t| now.signed_duration_since(t))
    };
    let claim_secs = age(&get(&item, "claimed_at")).map_or(i64::MAX, |d| d.num_seconds());
    let run = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", s(team_id))
        .key("run_id", s(&run_id))
        .projection_expression("#s, updated_at")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    Some(Holder {
        status: run.as_ref().map(|r| get(r, "status")),
        idle_hours: run
            .as_ref()
            .and_then(|r| age(&get(r, "updated_at")))
            .map_or(i64::MAX, |d| d.num_hours()),
        claim_secs,
        run_id,
    })
}

/// Writes the claim: new, or over `from` (a run that ended). False when
/// another writer got there first.
async fn write(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
    run_id: &str,
    from: Option<&str>,
) -> Result<bool, String> {
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.teams_table_name)
        .item("team_id", s(team_id))
        .item("sk", s(sk))
        .item("run_id", s(run_id))
        .item("claimed_at", s(&chrono::Utc::now().to_rfc3339()));
    let put = match from {
        None => put.condition_expression("attribute_not_exists(sk) OR run_id = :me"),
        Some(old) => put
            .condition_expression("run_id = :old OR run_id = :me")
            .expression_attribute_values(":old", s(old)),
    }
    .expression_attribute_values(":me", s(run_id));
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e)
            if e.as_service_error()
                .is_some_and(|x| x.is_conditional_check_failed_exception()) =>
        {
            Ok(false)
        }
        Err(e) => Err(aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string()),
    }
}

/// Takes the ticket's claim for fresh run `run_id`. `Err(run)` when a live
/// run holds the ticket: this trigger starts nothing, and that run's page
/// says the ticket arrived again. Fails open on an unexpected error, like
/// the ticket lock.
pub async fn claim(state: &WorkerState, msg: &TicketMessage, run_id: &str) -> Result<(), String> {
    if state.config.teams_table_name.is_empty() {
        return Ok(());
    }
    let key = ticket_key(state, msg).await;
    let sk = one_run::claim_sk(&key);
    let mut from: Option<String> = None;
    // A lost race is read again once; past that the live holder decides.
    for _ in 0..3 {
        match write(state, &msg.team_id, &sk, run_id, from.as_deref()).await {
            Ok(true) => {
                info!(run_id, ticket = %key, "The run holds its ticket");
                end_earlier_waits(state, msg, run_id).await;
                return Ok(());
            }
            Ok(false) => {}
            Err(e) => {
                warn!(run_id, ticket = %key, error = %e, "Ticket claim failed: allowing the run");
                return Ok(());
            }
        }
        let h = holder(state, &msg.team_id, &sk).await;
        match one_run::decide(run_id, h.as_ref()) {
            Claim::Take => from = None,
            Claim::TakeOver(old) => from = Some(old),
            Claim::Held(live) => {
                warn!(run_id, live = %live, ticket = %key, ticket_id = %msg.ticket_id, "A live run holds this ticket: no second run");
                super::add_progress_note(
                    state,
                    &msg.team_id,
                    &live,
                    &one_run::held_note(&msg.ticket_id, &msg.source),
                )
                .await;
                return Err(live);
            }
        }
    }
    warn!(run_id, ticket = %key, "Ticket claim kept changing hands: allowing the run");
    Ok(())
}

/// The ticket's earlier runs still waiting on a person stop waiting once
/// fresh run `run_id` holds the ticket (`one_run::earlier_waits`): each
/// becomes `retried`, replaced by this run, so one ticket has one current
/// run and no old question goes on being reminded. Only while each still
/// waits; best effort, like the claim.
async fn end_earlier_waits(state: &WorkerState, msg: &TicketMessage, run_id: &str) {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .projection_expression("run_id, #s, repo")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", s(&msg.team_id))
        .expression_attribute_values(":ticket", s(&msg.ticket_id))
        .send()
        .await;
    let items = match out {
        Ok(o) => o.items.unwrap_or_default(),
        Err(e) => {
            warn!(run_id, ticket_id = %msg.ticket_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not read the ticket's earlier runs");
            return;
        }
    };
    let get = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let runs: Vec<(String, String, String)> = items
        .iter()
        .map(|i| (get(i, "run_id"), get(i, "status"), get(i, "repo")))
        .collect();
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let now = chrono::Utc::now().to_rfc3339();
    for old in one_run::earlier_waits(run_id, &repo, &runs) {
        let ended = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", s(&msg.team_id))
            .key("run_id", s(old))
            .update_expression(
                "SET #s = :r, status_run_id = :sri, retried_as = :n, updated_at = :t",
            )
            .condition_expression("#s = :ni")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":r", s(common::run_status::RETRIED))
            .expression_attribute_values(
                ":sri",
                s(&format!("{}#{old}", common::run_status::RETRIED)),
            )
            .expression_attribute_values(":n", s(run_id))
            .expression_attribute_values(":t", s(&now))
            .expression_attribute_values(":ni", s(common::run_status::NEEDS_INPUT))
            .send()
            .await
            .is_ok();
        if ended {
            info!(run_id, old, ticket_id = %msg.ticket_id, "An earlier waiting run of the ticket no longer waits");
            super::add_progress_note(state, &msg.team_id, old, &one_run::ended_wait_note(run_id))
                .await;
        }
    }
}

/// The live run that holds the ticket named `key` (seen from `repo`), if any:
/// its claim, or for a run that started before claims were kept, a live run
/// carrying that ticket id.
pub async fn live_run(state: &WorkerState, team_id: &str, key: &str, repo: &str) -> Option<String> {
    let canonical = one_run::canonical(
        &common::messages::TicketSource::Jira,
        key,
        repo,
        "",
        Some(key),
    );
    if let Some(h) = holder(state, team_id, &one_run::claim_sk(&canonical)).await {
        if h.live() {
            return Some(h.run_id);
        }
    }
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", s(team_id))
        .expression_attribute_values(":ticket", s(&key.to_ascii_uppercase()))
        .send()
        .await
        .ok()?;
    let now = chrono::Utc::now();
    out.items().iter().find_map(|item| {
        let get = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
        let idle_hours = get("updated_at")
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t).ok())
            .map_or(i64::MAX, |t| now.signed_duration_since(t).num_hours());
        let h = Holder {
            run_id: get("run_id")?,
            status: get("status"),
            idle_hours,
            claim_secs: i64::MAX,
        };
        h.live().then_some(h.run_id)
    })
}
