//! One writer per pull request branch, across runs of a ticket.
//!
//! A person pressed Retry on a stopped run of a ticket whose earlier run had
//! an open pull request. The new run took the same branch (the pull
//! request's), built on it for forty minutes, while the earlier run's
//! feedback pass worked the same pull request: two runs writing one branch.
//! Each run's writer slot only guarded the run itself.
//!
//! The rules here:
//!
//! - A run that is about to work on an open pull request an earlier run of
//!   the ticket opened finds that run ([`holder_runs`]) and reads it fresh.
//!   While it is still working ([`busy`]: queued or running, waiting for CI
//!   and fixing what fails, writing to the branch now, or a fix round under
//!   way or parked), the new run stops before it writes anything and says so
//!   on the run page, naming the pull request and the run
//!   ([`busy_note`], [`stop_busy`]).
//! - When the earlier run is done, the new run continues that pull request
//!   as its update ([`continue_note`]) and holds the earlier run's writer
//!   slot while it works ([`claim`]): a feedback or CI pass of the earlier
//!   run waits for it instead of writing the branch at the same time.

use common::run_round::{Round, State};
use common::run_status::{self, EndedValues};
use tracing::{info, warn};

use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

use super::ticket_history::TicketHistory;
use super::{attr_n, attr_s, RunUsageValues, RunWriterGuard};

/// Run statuses that mean the run is still working on its pull request.
const WORKING: [&str; 4] = [
    run_status::PENDING,
    run_status::QUEUED,
    run_status::RUNNING,
    run_status::AWAITING_CI,
];

/// An earlier run that opened the pull request, as its record says now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Holder {
    pub run_id: String,
    pub status: String,
    /// Unix seconds its writer slot is held until (0: free).
    pub writer_until: i64,
    /// What holds the slot (`feedback#…`, `resume#…`).
    pub writer_holder: String,
    pub round: Option<Round>,
}

impl Holder {
    pub fn from_item(
        run_id: &str,
        item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    ) -> Holder {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        Holder {
            run_id: run_id.to_string(),
            status: s("status"),
            writer_until: item
                .get("writer_until")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            writer_holder: s("writer_holder"),
            round: Round::from_item(item),
        }
    }
}

/// Why the earlier run is still working on its pull request, in a person's
/// words; `None` when it is done with it. `holder`: this run's own name on
/// a writer slot (its own hold is not someone else's work).
pub fn busy(h: &Holder, now: chrono::DateTime<chrono::Utc>, holder: &str) -> Option<String> {
    if run_status::ended_by_person(&h.status) {
        return None;
    }
    if WORKING.contains(&h.status.as_str()) {
        return Some(match h.status.as_str() {
            run_status::AWAITING_CI => {
                "it is waiting for CI on the pull request and fixes what fails".to_string()
            }
            run_status::RUNNING => "it is still running".to_string(),
            _ => "it is queued to start".to_string(),
        });
    }
    if h.writer_until > now.timestamp() && h.writer_holder != holder {
        let pass = h
            .writer_holder
            .split('#')
            .next()
            .filter(|p| !p.is_empty())
            .unwrap_or("a");
        return Some(format!("it is writing to the branch now (its {pass} pass)"));
    }
    if let Some(r) = &h.round {
        let deadline = chrono::DateTime::parse_from_rfc3339(&r.deadline_at)
            .map(|d| d.with_timezone(&chrono::Utc))
            .ok();
        let grace = chrono::Duration::seconds(common::run_round::SWEEP_GRACE_SECS as i64);
        // A round past its deadline and the sweep's grace is gone, not
        // working: the sweep closes it.
        let live = r.state == State::Pending || deadline.is_none_or(|d| d + grace > now);
        if live {
            return Some(match r.state {
                State::Pending => format!("its {} is waiting to run", r.kind.label()),
                State::Running => format!("its {} is under way", r.kind.label()),
            });
        }
    }
    None
}

/// The earlier runs of the ticket that opened `repo#number`.
pub fn holder_runs(history: &TicketHistory, repo: &str, number: u64) -> Vec<String> {
    history
        .runs
        .iter()
        .filter(|r| {
            r.pr_list
                .iter()
                .any(|p| p.pr_number == number && p.repo.eq_ignore_ascii_case(repo))
                || (r.pr_number == number
                    && (r.repo.is_empty() || r.repo.eq_ignore_ascii_case(repo)))
        })
        .map(|r| r.run_id.clone())
        .collect()
}

/// This run's name on an earlier run's writer slot.
pub fn holder_name(run_id: &str) -> String {
    format!("ticket_run#{run_id}")
}

/// The run page's note when the run continues the earlier run's pull
/// request.
pub fn continue_note(number: u64, earlier: &[String]) -> String {
    format!(
        "This ticket already has CoderHelm's open pull request #{number}{}: this run continues it \
         as its update, on its branch, and opens no other. The earlier run doesn't write to the \
         branch while this one works.",
        match earlier.last() {
            Some(r) => format!(" (from run {r})"),
            None => String::new(),
        }
    )
}

/// The run page's note (and the run's status) when the earlier run is
/// still working on the pull request.
pub fn busy_note(number: u64, holder_run: &str, why: &str) -> String {
    format!(
        "Stopped before writing anything: this ticket's open pull request #{number} belongs to run \
         {holder_run}, and {why}. Two runs never write the same branch. Retry once that run is \
         done: the retry then continues #{number} as its update."
    )
}

/// What [`claim`] found.
pub enum Claim {
    /// The earlier runs are done; this run holds their writer slots.
    Free(Vec<RunWriterGuard>),
    /// An earlier run is still working on the pull request.
    Busy { run_id: String, why: String },
}

/// Reads each earlier run fresh and, when none is working on the pull
/// request, takes their writer slots for this invocation.
pub async fn claim(state: &WorkerState, team_id: &str, run_id: &str, earlier: &[String]) -> Claim {
    let me = holder_name(run_id);
    let mut guards = Vec::new();
    for other in earlier {
        let item = match state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(team_id))
            .key("run_id", attr_s(other))
            .send()
            .await
        {
            Ok(out) => out.item,
            Err(e) => {
                warn!(run_id, other, error = %e, "Could not read the pull request's earlier run");
                return Claim::Busy {
                    run_id: other.clone(),
                    why: "its record couldn't be read to make sure it isn't working on it"
                        .to_string(),
                };
            }
        };
        let Some(item) = item else { continue };
        let h = Holder::from_item(other, &item);
        if let Some(why) = busy(&h, chrono::Utc::now(), &me) {
            return Claim::Busy {
                run_id: other.clone(),
                why,
            };
        }
        match super::acquire_run_writer_guard(state, team_id, other, &me).await {
            Some(g) => guards.push(g),
            None => {
                return Claim::Busy {
                    run_id: other.clone(),
                    why: "it is writing to the branch now".to_string(),
                }
            }
        }
    }
    Claim::Free(guards)
}

/// Ends the run without writing: the pull request's earlier run is still
/// working on it. The run reads as covered by that pull request, and Retry
/// continues it once the earlier run is done.
#[allow(clippy::too_many_arguments)]
pub async fn stop_busy(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &TokenUsage,
    start: &std::time::Instant,
    pr: (&str, u64, &str, &str),
    holder_run: &str,
    why: &str,
) {
    let (repo, number, url, title) = pr;
    let note = busy_note(number, holder_run, why);
    info!(
        run_id,
        pr = number,
        holder_run,
        why,
        "Pull request busy with an earlier run"
    );
    super::add_progress_note(state, &msg.team_id, run_id, &note).await;
    let record = serde_json::json!({
        "repo": repo,
        "number": number,
        "url": url,
        "title": title,
        "author": "CoderHelm",
        "reason": format!("run {holder_run} is still working on it"),
        "signals": ["it is CoderHelm's own pull request for this ticket"],
    });
    let status = run_status::COVERED_BY_PR;
    let delta = usage.unrecorded();
    let written = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, covered_by = :cb, error_message = :em, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, \
             cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp",
        )
        .condition_expression(run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(":cb", attr_s(&record.to_string()))
        .expression_attribute_values(":em", attr_s(&note))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .send()
        .await;
    if let Err(e) = written {
        warn!(run_id, error = %e, "Could not record the stop for a busy pull request");
    }
    super::webhooks::emit_run(state, &msg.team_id, run_id, status).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::ticket_history::PriorRun;
    use common::run_round::Kind;

    fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn holder(status: &str) -> Holder {
        Holder {
            run_id: "R0".into(),
            status: status.into(),
            ..Default::default()
        }
    }

    /// The observed case: the earlier run read `completed` while its
    /// feedback pass held its writer slot and worked the pull request.
    #[test]
    fn a_completed_run_writing_its_branch_is_busy() {
        let now = at("2026-01-10T01:55:00Z");
        let mut h = holder("completed");
        h.writer_until = now.timestamp() + 600;
        h.writer_holder = "feedback#1793152075223200704".into();
        let why = busy(&h, now, &holder_name("R1")).unwrap();
        assert_eq!(why, "it is writing to the branch now (its feedback pass)");
        // Its slot expired: done.
        h.writer_until = now.timestamp() - 1;
        assert_eq!(busy(&h, now, &holder_name("R1")), None);
        // This run's own hold (a continuation of it) is not someone else's.
        h.writer_until = now.timestamp() + 600;
        h.writer_holder = holder_name("R1");
        assert_eq!(busy(&h, now, &holder_name("R1")), None);
    }

    #[test]
    fn working_statuses_are_busy_and_ended_ones_are_not() {
        let now = at("2026-01-10T01:55:00Z");
        for s in ["pending", "queued", "running", "awaiting_ci"] {
            assert!(busy(&holder(s), now, "x").is_some(), "{s}");
        }
        assert!(busy(&holder("awaiting_ci"), now, "x")
            .unwrap()
            .contains("waiting for CI"));
        for s in [
            "completed",
            "needs_input",
            "failed",
            "covered_by_pr",
            "cancelled",
            "closed",
        ] {
            assert_eq!(busy(&holder(s), now, "x"), None, "{s}");
        }
    }

    #[test]
    fn a_round_under_way_or_parked_is_busy_until_the_sweep_would_close_it() {
        let now = at("2026-01-10T02:00:00Z");
        let mut round = Round::start(Kind::Feedback, 1, "", "", false, at("2026-01-10T01:50:00Z"));
        round.deadline_at = "2026-01-10T02:02:00Z".into();
        let mut h = holder("completed");
        h.round = Some(round.clone());
        assert!(busy(&h, now, "x").unwrap().contains("under way"));
        // Long past its deadline and the grace: gone, not working.
        round.deadline_at = "2026-01-10T01:00:00Z".into();
        h.round = Some(round.clone());
        assert_eq!(busy(&h, now, "x"), None);
        // Parked: waits for a fresh invocation, still the run's.
        round.state = State::Pending;
        h.round = Some(round);
        assert!(busy(&h, now, "x").unwrap().contains("waiting to run"));
    }

    #[test]
    fn the_runs_that_opened_the_pull_request_are_found() {
        let mut history = TicketHistory::default();
        history.runs.push(PriorRun {
            run_id: "R0".into(),
            repo: "acme/web".into(),
            pr_number: 41,
            pr_list: vec![common::run_prs::RunPr {
                repo: "acme/web".into(),
                pr_number: 41,
                ..Default::default()
            }],
            ..Default::default()
        });
        history.runs.push(PriorRun {
            run_id: "R1".into(),
            repo: "acme/web".into(),
            ..Default::default()
        });
        assert_eq!(
            holder_runs(&history, "Acme/Web", 41),
            vec!["R0".to_string()]
        );
        assert!(holder_runs(&history, "acme/api", 41).is_empty());
        assert!(holder_runs(&history, "acme/web", 7).is_empty());
    }

    #[test]
    fn the_notes_name_the_pull_request_and_the_run() {
        let busy = busy_note(
            41,
            "R0",
            "it is writing to the branch now (its feedback pass)",
        );
        assert!(busy.contains("#41"));
        assert!(busy.contains("run R0"));
        assert!(busy.contains("Two runs never write the same branch"));
        assert!(busy.contains("Retry once that run is done"));
        let cont = continue_note(41, &["R0".to_string()]);
        assert!(cont.contains("open pull request #41 (from run R0)"));
        assert!(cont.contains("continues it as its update"));
    }
}
