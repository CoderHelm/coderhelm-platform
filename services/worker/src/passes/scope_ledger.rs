//! The run's scope ledger (`common::scope_ledger`, whose module page has
//! the rules), kept on the run.
//!
//! - [`load_or_seed`]: the ledger, decided from the ticket's acceptance
//!   criteria (`stop_judge::acceptance`) and the plan when the plan is
//!   generated, before any code; a check that finds none writes it then.
//!   Every later round reads the same decisions.
//! - [`sources`]: what a scope change may cite: the ticket's acceptance
//!   criteria, the answers people gave and the people's comments on the
//!   pull request.
//! - [`held`]: the round's check against the ledger: `Scope change:` lines
//!   that cite a source move their item in (and are saved), and what the
//!   diff brings back of the rest is returned as breaches.
//! - [`drop_covered`]: planned work an out-of-scope entry names is not
//!   built again by a planned-work round.

use aws_sdk_dynamodb::types::AttributeValue;
use common::scope_ledger::{self, Breach, Ledger};
use tracing::{info, warn};

use super::plan::PlanResult;
use crate::models::TicketMessage;
use crate::WorkerState;

/// The ledger on the run, `None` when none was written.
pub(crate) async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Option<Ledger> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .projection_expression("#f")
        .expression_attribute_names("#f", scope_ledger::FIELD)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| {
            i.get(scope_ledger::FIELD)
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .map(|s| Ledger::from_json(&s))
}

pub(crate) async fn save(state: &WorkerState, team_id: &str, run_id: &str, ledger: &Ledger) {
    let saved = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", scope_ledger::FIELD)
        .expression_attribute_values(":v", AttributeValue::S(ledger.to_json()))
        .send()
        .await;
    if let Err(e) = saved {
        warn!(run_id, error = %e, "Could not keep the scope ledger on the run");
    }
}

/// The ledger on the run, decided from the ticket's acceptance criteria
/// and `plan` when there is none yet. Written once, on the first run's plan
/// before any code; a plan revision, a review round, a CI fix or a
/// planned-work round never writes it again, they are held to it.
pub(crate) async fn load_or_seed(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    msg: &TicketMessage,
    plan: &PlanResult,
    people: &[(String, String)],
) -> Ledger {
    if let Some(l) = load(state, team_id, run_id).await {
        return l;
    }
    let mut l = Ledger::from_ticket_and_plan(
        &super::stop_judge::acceptance(&msg.body),
        &plan.proposal,
        &plan.design,
        &plan.tasks,
    );
    // What the ticket names, and what people already answered or
    // commented, is in scope from the start: the plan never writes the
    // ticket's own ask or a person's as out of scope.
    let mut moved = l.named_by_ticket(&format!("{}\n{}", msg.title, msg.body));
    moved.extend(l.take_people(people));
    save(state, team_id, run_id, &l).await;
    if !moved.is_empty() {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("In scope because a person asked: {}", moved.join(" ")),
        )
        .await;
    }
    if !l.is_empty() {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!(
                "Scope decided at plan time, held in every round: out of scope {}",
                l.outs().map(|e| e.line()).collect::<Vec<_>>().join("; ")
            ),
        )
        .await;
    }
    l
}

/// The ledger on the run with the people's answers and the comments on
/// pull request `number` taken in (`Ledger::take_people`), saved when one
/// moved an item in. A planned-work round reads scope through this, so a
/// person's answer since the last check is never built around.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_with_people(
    state: &WorkerState,
    github: &crate::clients::github::GitHubClient,
    msg: &TicketMessage,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
) -> Option<Ledger> {
    let mut l = load(state, team_id, run_id).await?;
    let comments = match github.list_issue_comments(owner, repo, number).await {
        Ok(c) => super::review_pr::human_comments(&c, 30),
        Err(_) => Vec::new(),
    };
    let moved = l.take_people(&people(msg, &comments));
    if !moved.is_empty() {
        save(state, team_id, run_id, &l).await;
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("Scope changed: {}", moved.join(" ")),
        )
        .await;
    }
    Some(l)
}

/// What a scope change may cite: the ticket's acceptance criteria, the
/// answers people gave, and the people's `comments` (bodies).
pub(crate) fn sources(msg: &TicketMessage, comments: &[(String, String)]) -> Vec<String> {
    let mut out = super::stop_judge::acceptance(&msg.body);
    out.extend(
        msg.answers
            .iter()
            .map(|a| format!("{} {}", a.question, a.answer)),
    );
    out.extend(comments.iter().map(|(_, b)| b.clone()));
    out
}

/// What people said that decides scope, as (who, words): the answers
/// they gave the run's questions (an answer that says no is left out, its
/// question's words are not an ask) and their `comments`.
pub(crate) fn people(msg: &TicketMessage, comments: &[(String, String)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = msg
        .answers
        .iter()
        .filter(|a| {
            let l = a.answer.trim().to_lowercase();
            !(l == "no" || l.starts_with("no,") || l.starts_with("no.") || l.starts_with("no "))
        })
        .map(|a| (a.author.clone(), format!("{} {}", a.question, a.answer)))
        .collect();
    out.extend(comments.iter().cloned());
    out
}

/// The people's comments on the run's pull request, when it has one, as
/// (author, body).
pub(crate) async fn comments(
    github: &crate::clients::github::GitHubClient,
    msg: &TicketMessage,
    branch: &str,
) -> Vec<(String, String)> {
    let Ok(Some(pr)) = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await
    else {
        return Vec::new();
    };
    let Some(number) = pr["number"].as_u64() else {
        return Vec::new();
    };
    match github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, number)
        .await
    {
        Ok(c) => super::review_pr::human_comments(&c, 30),
        Err(_) => Vec::new(),
    }
}

/// The round's check against the ledger: the coder's `Scope change:` lines
/// that cite a source move their item in (the ledger is saved with who
/// asked), and what the diff brings back of the rest comes back as
/// breaches. A refused change is said on the run page.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn held(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    ledger: &mut Ledger,
    notes: &str,
    sources: &[String],
    people: &[(String, String)],
    diff: &[(String, String, String)],
) -> Vec<Breach> {
    // A person's answer or comment moves its item in on its own, every
    // round; the coder's `Scope change:` lines come after.
    let mut accepted = ledger.take_people(people);
    let (by_coder, refused) = ledger.apply_changes(notes, sources);
    accepted.extend(by_coder);
    if !accepted.is_empty() {
        save(state, team_id, run_id, ledger).await;
        info!(run_id, moved = ?accepted, "Scope changed with a cited reason; the ledger records it");
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("Scope changed: {}", accepted.join(" ")),
        )
        .await;
    }
    if !refused.is_empty() {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("Scope change refused: {}", refused.join(" ")),
        )
        .await;
    }
    scope_ledger::breaches(ledger, diff)
}

/// `items` without the planned work an out-of-scope entry names, and the
/// items left out with the entry that covers them.
pub(crate) fn drop_covered(
    ledger: &Ledger,
    items: Vec<String>,
) -> (Vec<String>, Vec<(String, String)>) {
    let mut keep = Vec::new();
    let mut gone = Vec::new();
    for i in items {
        match ledger.covers(&i) {
            Some(e) => gone.push((i, e.line())),
            None => keep.push(i),
        }
    }
    (keep, gone)
}

/// The run page's line for planned work left out because the plan decided
/// it is out of scope.
pub(crate) fn dropped_note(gone: &[(String, String)]) -> String {
    format!(
        "Not built again, decided out of scope at plan time: {}",
        gone.iter()
            .map(|(i, why)| format!("{} ({why})", common::truncate_str(i, 120)))
            .collect::<Vec<_>>()
            .join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_work_the_ledger_leaves_out_is_not_built() {
        let l = Ledger::from_plan(
            "",
            "## Out of scope\n- `/tours/<wing>` path pages (`selectedWing`): decided with the default: one id, as today\n",
            "",
        );
        let (keep, gone) = drop_covered(
            &l,
            vec![
                "apps/web/src/services/tourService.ts: prefer the id list over `selectedWing`"
                    .into(),
                "apps/web/src/utils/filterMaps.ts: add slugToIds".into(),
            ],
        );
        assert_eq!(keep.len(), 1);
        assert_eq!(gone.len(), 1);
        assert!(dropped_note(&gone).contains("decided out of scope at plan time"));
    }
}
