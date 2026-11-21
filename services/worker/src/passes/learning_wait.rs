//! Learning from a closed pull request waits for GitHub; it is never lost.
//!
//! Four of CoderHelm's pull requests once closed within ten seconds of each
//! other. Each close reached its run and queued its scoring. The
//! installation's GitHub budget was below the share kept for runs, so
//! CoderHelm's own budget gate held the scoring's first request ("not
//! sent ... the work waits for the reset"); the handler logged "Reward
//! outcome failed" and the message was gone. The lesson job for the same
//! closes lost its question the same way. Nothing tried again, and the
//! learning page said to check the GitHub App's pull request events, which
//! had arrived.
//!
//! The rules:
//!
//! - A held or refused GitHub request is a wait, never a failure, for this
//!   background work: the scoring (`reward_outcome`) and the lesson
//!   (`lesson`) messages are sent again, as themselves, to go on after
//!   GitHub's reset, with the waits they took counted and bounded
//!   (`common::github_wait`). Scoring starts only when the learning share
//!   of the budget has room for all of it ([`budget_wait`]), so it is not
//!   cut off halfway.
//! - A scoring that waits notes on its run when the next try is due; one
//!   that fails notes why ([`settle_outcome`]). Neither is consumed
//!   silently.
//! - The sweep, on the review-reminder tick, finds the team's closed pull
//!   requests with no score by the same rule the learning page uses
//!   (`common::rewards::unscored`) and sends their scoring again: a few per
//!   tick, only when the budget allows, never while a try is in flight, and
//!   at most `unscored::MAX_TRIES` times, then it says why it stopped.
//! - Scoring twice adds nothing: each signal is kept once
//!   (`common::rewards::store::add_all_once`).

use crate::clients::github_budget;
use crate::models::{LessonMessage, WorkerMessage};
use crate::passes::attr_s;
use crate::passes::rewards::OutcomeError;
use crate::WorkerState;
use common::github_budget::Spend;
use common::github_wait::{self as gw, Cause, GitHubWait};
use common::rewards::health::ClosedPr;
use common::rewards::unscored::{self, Next};
use common::rewards::RewardOutcomeMessage;
use tracing::{error, info, warn};

/// GitHub requests a lesson job makes at most (the pull request, its
/// comments, the issue's, the thread before asking, the question).
pub const LESSON_REQUESTS: u64 = 6;
/// Pull requests the sweep sends again per tick.
pub const SWEEP_PER_TICK: usize = 5;
/// Pages of one team's runs the sweep reads per tick.
const SWEEP_PAGES: usize = 10;

fn now_epoch() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// A learning message that can wait for GitHub.
pub trait Waits: Clone {
    fn github_wait(&self) -> &GitHubWait;
    /// This message, carrying `w`, as it is sent again.
    fn waiting(&self, w: GitHubWait) -> Self;
    fn message(self) -> WorkerMessage;
}

impl Waits for RewardOutcomeMessage {
    fn github_wait(&self) -> &GitHubWait {
        &self.github_wait
    }
    fn waiting(&self, w: GitHubWait) -> Self {
        Self {
            github_wait: w,
            // What follows a close (its deploy watch) ran on the first
            // delivery; this one only scores.
            rescore: true,
            ..self.clone()
        }
    }
    fn message(self) -> WorkerMessage {
        WorkerMessage::RewardOutcome(self)
    }
}

impl Waits for LessonMessage {
    fn github_wait(&self) -> &GitHubWait {
        &self.github_wait
    }
    fn waiting(&self, w: GitHubWait) -> Self {
        Self {
            github_wait: w,
            ..self.clone()
        }
    }
    fn message(self) -> WorkerMessage {
        WorkerMessage::Lesson(self)
    }
}

/// What becomes of a learning message GitHub held or refused.
#[derive(Debug, Clone)]
pub enum Again<M> {
    /// Sent again as itself, `delay` seconds from now (one hop of the wait).
    Send { msg: M, delay: u64 },
    /// It waited as often as it may: why it stops.
    Stop(String),
}

/// Pure: the message to send again after GitHub held or refused it for
/// `secs` at `now`.
pub fn again<M: Waits>(msg: &M, cause: Cause, secs: u64, now: u64) -> Again<M> {
    match gw::next_wait(cause, secs, msg.github_wait(), now) {
        Some(w) => Again::Send {
            delay: gw::first_hop(w.until, now),
            msg: msg.waiting(w),
        },
        None => Again::Stop(format!(
            "GitHub held or refused its requests through {} waits",
            msg.github_wait().waits
        )),
    }
}

/// Pure: the next hop's delay while a message's wait isn't over at `now`.
pub fn still_waiting<M: Waits>(msg: &M, now: u64) -> Option<u64> {
    gw::hop(msg.github_wait().until, now)
}

/// Pure: whether `need` more requests by a background caller fit the
/// budget now, from its standing and allowance: None when they fit, else
/// how long to wait. Short of room (not held), it waits for the hour's
/// reset (`reset`, epoch seconds, when known), at least a minute.
pub fn hold_of(
    standing: Spend,
    allowance: u64,
    need: u64,
    reset: Option<u64>,
    now: u64,
) -> Option<(Cause, u64)> {
    match standing {
        Spend::Hold { wait_secs, .. } => Some((Cause::RateLimit, wait_secs.max(1))),
        Spend::Go if allowance < need.max(1) => {
            let next_hour = (now / 3600 + 1) * 3600;
            let at = reset.filter(|r| *r > now).unwrap_or(next_hour);
            Some((Cause::RateLimit, at.saturating_sub(now).max(60)))
        }
        Spend::Go => None,
    }
}

/// Whether the current task may make `need` GitHub requests for the
/// installation now (after reading what other workers spent): None when it
/// may, else the wait. Asked before a learning job's first request, so the
/// job is never cut off halfway by the budget.
pub async fn budget_wait(
    state: &WorkerState,
    installation_id: u64,
    need: u64,
) -> Option<(Cause, u64)> {
    if installation_id == 0 {
        return None;
    }
    github_budget::refresh(state, installation_id).await;
    let now = now_epoch();
    let caller = github_budget::caller();
    hold_of(
        github_budget::standing(installation_id, caller, now),
        github_budget::allowance(installation_id, caller, now),
        need,
        crate::clients::github::budget_of(installation_id).map(|b| b.reset),
        now,
    )
}

/// Sends a worker message to the ticket queue after `delay` seconds (at
/// most one SQS delay).
async fn send(state: &WorkerState, msg: WorkerMessage, delay: u64) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let Ok(body) = serde_json::to_string(&msg) else {
        return false;
    };
    let mut req = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body);
    if delay > 0 {
        req = req.delay_seconds(delay.min(gw::MAX_HOP_SECS) as i32);
    }
    match req.send().await {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Could not send the learning job again");
            false
        }
    }
}

/// Notes on the run how its scoring goes (`unscored`): `retry_at` when the
/// next try is due, `error` why the last one ended.
async fn note(
    state: &WorkerState,
    msg: &RewardOutcomeMessage,
    retry_at: Option<u64>,
    error: Option<&str>,
) {
    if msg.run_id.is_empty() || msg.review_only || (retry_at.is_none() && error.is_none()) {
        return;
    }
    let mut sets = Vec::new();
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .condition_expression("attribute_exists(run_id)");
    if let Some(at) = retry_at {
        let at = chrono::DateTime::from_timestamp(at as i64, 0)
            .unwrap_or_else(chrono::Utc::now)
            .to_rfc3339();
        sets.push(format!("{} = :at", unscored::RETRY_AT));
        req = req.expression_attribute_values(":at", attr_s(&at));
    }
    if let Some(e) = error {
        sets.push(format!("{} = :e", unscored::ERROR));
        req = req.expression_attribute_values(":e", attr_s(common::truncate_str(e, 500)));
    }
    if let Err(e) = req
        .update_expression(format!("SET {}", sets.join(", ")))
        .send()
        .await
    {
        warn!(run_id = %msg.run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not note the scoring's next try on the run");
    }
}

/// A `reward_outcome` message: goes on waiting while its wait isn't over,
/// then scores, and settles what the scoring came to.
pub async fn on_outcome(state: &WorkerState, msg: RewardOutcomeMessage) {
    if let Some(hop) = still_waiting(&msg, now_epoch()) {
        if send(state, msg.clone().message(), hop).await {
            return;
        }
    }
    let result = super::rewards::score_outcome(state, msg.clone()).await;
    settle_outcome(state, &msg, result).await;
}

/// What a scoring came to: a wait sends the message again for after
/// GitHub's reset; a failure is noted for the sweep. Never dropped.
pub async fn settle_outcome(
    state: &WorkerState,
    msg: &RewardOutcomeMessage,
    result: Result<(), OutcomeError>,
) {
    let pr = msg.pr_number;
    match result {
        Ok(()) => {}
        Err(OutcomeError::Wait { cause, secs }) => match again(msg, cause, secs, now_epoch()) {
            Again::Send { msg: next, delay } => {
                let until = next.github_wait.until;
                if send(state, next.message(), delay).await {
                    warn!(team_id = %msg.team_id, run_id = %msg.run_id, pr, until, "Reward outcome waits for GitHub: scored again at {}", gw::clock(until));
                    note(state, msg, Some(until), None).await;
                } else {
                    error!(team_id = %msg.team_id, run_id = %msg.run_id, pr, "Reward outcome waits for GitHub and could not be sent again; the sweep sends it");
                    note(
                        state,
                        msg,
                        None,
                        Some("could not be sent again after a wait for GitHub"),
                    )
                    .await;
                }
            }
            Again::Stop(why) => {
                error!(team_id = %msg.team_id, run_id = %msg.run_id, pr, why, "Reward outcome stopped waiting for GitHub; the sweep tries it again");
                note(state, msg, None, Some(&why)).await;
            }
        },
        Err(OutcomeError::Failed(e)) => {
            error!(team_id = %msg.team_id, run_id = %msg.run_id, pr, error = %e, "Reward outcome failed; the sweep tries it again");
            note(state, msg, None, Some(&e)).await;
        }
    }
}

/// A `lesson` message: goes on waiting while its wait isn't over, then
/// runs; GitHub holding or refusing it sends it again for after the reset.
pub async fn on_lesson(state: &WorkerState, msg: LessonMessage) {
    if let Some(hop) = still_waiting(&msg, now_epoch()) {
        if send(state, msg.clone().message(), hop).await {
            return;
        }
    }
    let Err(e) = super::lessons::run(state, msg.clone()).await else {
        return;
    };
    let Some((cause, secs)) = crate::clients::github::outside_wait(&*e) else {
        error!("Lesson failed: {e:?}");
        return;
    };
    match again(&msg, cause, secs, now_epoch()) {
        Again::Send { msg: next, delay } => {
            let until = next.github_wait.until;
            if send(state, next.message(), delay).await {
                warn!(run_id = %msg.run_id, pr = msg.pr_number, until, "Lesson waits for GitHub: goes on at {}", gw::clock(until));
            } else {
                error!(run_id = %msg.run_id, pr = msg.pr_number, "Lesson waits for GitHub and could not be sent again");
            }
        }
        Again::Stop(why) => {
            error!(run_id = %msg.run_id, pr = msg.pr_number, why, "Lesson stopped waiting for GitHub");
        }
    }
}

// ── The sweep ─────────────────────────────────────────────────────────

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    pub teams: usize,
    pub missing: usize,
    pub sent: Vec<String>,
    pub in_flight: usize,
    pub held: usize,
    pub stopped: usize,
}

/// What the sweep does with one team's missing pull requests, in order.
#[derive(Debug, Clone)]
pub enum Step {
    /// Send this scoring again.
    Send(RewardOutcomeMessage),
    /// Stop trying this run's scoring, with why.
    Stop { run_id: String, why: String },
}

/// Pure: the sweep's steps for one team's missing pull requests, at most
/// `room` sends; `allows` says whether the installation's budget has room
/// for one scoring. Counts what it leaves into `report`.
pub fn plan(
    team_id: &str,
    missing: &[&ClosedPr],
    now: chrono::DateTime<chrono::Utc>,
    mut room: usize,
    mut allows: impl FnMut(u64) -> bool,
    report: &mut SweepReport,
) -> Vec<Step> {
    let mut out = Vec::new();
    for p in missing {
        match unscored::next(p, now) {
            Next::Stopped => report.stopped += 1,
            Next::InFlight => report.in_flight += 1,
            Next::GiveUp(why) => out.push(Step::Stop {
                run_id: p.run_id.clone(),
                why,
            }),
            Next::Send => {
                let Some(msg) = unscored::rescore_message(team_id, p) else {
                    continue;
                };
                if room == 0 {
                    continue;
                }
                if !allows(p.installation_id) {
                    report.held += 1;
                    continue;
                }
                room -= 1;
                out.push(Step::Send(msg));
            }
        }
    }
    out
}

/// The teams of this install (their `META` records).
async fn teams(state: &WorkerState) -> Vec<String> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let page = state
            .dynamo
            .scan()
            .table_name(&state.config.teams_table_name)
            .filter_expression("sk = :m")
            .expression_attribute_values(":m", AttributeValue::S("META".into()))
            .projection_expression("team_id")
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else {
            warn!("Scoring sweep: could not read the teams");
            break;
        };
        out.extend(
            page.items()
                .iter()
                .filter_map(|i| i.get("team_id").and_then(|v| v.as_s().ok()).cloned()),
        );
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// Finds every team's closed pull requests with no score yet (the learning
/// page's rule) and sends their scoring again, a few per tick, when the
/// budget allows and no try is in flight; stops after the bound and says
/// why on the run.
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let now = chrono::Utc::now();
    let since = (now - chrono::Duration::days(common::rewards::health::WINDOW_DAYS)).to_rfc3339();
    let store =
        common::rewards::store::Store::new(&state.dynamo, &state.config.settings_table_name);
    let mut gate = github_budget::SweepGate::new(common::github_budget::caller::SWEEP_LEARNING);
    for team in teams(state).await {
        let room = SWEEP_PER_TICK.saturating_sub(report.sent.len());
        let runs = match unscored::closed_runs(
            &state.dynamo,
            &state.config.runs_table_name,
            &team,
            &since,
            SWEEP_PAGES,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(team_id = %team, error = %e, "Scoring sweep: could not read the closed pull requests");
                continue;
            }
        };
        let closed = unscored::closed_prs_of(&runs);
        if closed.is_empty() {
            continue;
        }
        report.teams += 1;
        let events = match store.events_since(&team, &since).await {
            Ok(e) => e,
            Err(e) => {
                warn!(team_id = %team, error = %e, "Scoring sweep: could not read the scores");
                continue;
            }
        };
        let missing = unscored::missing(&closed, &events, now);
        report.missing += missing.len();
        // The budget is read once per installation (a skip stays skipped
        // for this sweep); each send asks for one scoring's requests.
        let mut budget: std::collections::HashMap<u64, bool> = Default::default();
        let mut ids: Vec<u64> = missing.iter().map(|p| p.installation_id).collect();
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            let ok = gate
                .allows(state, id, super::rewards::SCORING_REQUESTS)
                .await;
            budget.insert(id, ok);
        }
        let steps = plan(
            &team,
            &missing,
            now,
            room,
            |id| budget.get(&id).copied().unwrap_or(false),
            &mut report,
        );
        for step in steps {
            match step {
                Step::Send(msg) => {
                    let run_id = msg.run_id.clone();
                    let name = format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number);
                    if !send(state, msg.message(), 0).await {
                        continue;
                    }
                    mark_sent(state, &team, &run_id, now).await;
                    report.sent.push(name);
                }
                Step::Stop { run_id, why } => {
                    mark_stopped(state, &team, &run_id, &why).await;
                    report.stopped += 1;
                }
            }
        }
    }
    if !report.sent.is_empty() || report.missing > 0 {
        info!(
            teams = report.teams,
            missing = report.missing,
            sent = ?report.sent,
            in_flight = report.in_flight,
            held = report.held,
            stopped = report.stopped,
            "Scoring sweep: closed pull requests with no score sent again"
        );
    }
    report
}

/// The sweep sent a run's scoring again: one more try, due now.
async fn mark_sent(
    state: &WorkerState,
    team: &str,
    run_id: &str,
    now: chrono::DateTime<chrono::Utc>,
) {
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team))
        .key("run_id", attr_s(run_id))
        .condition_expression("attribute_exists(run_id)")
        .update_expression(format!(
            "SET {t} = if_not_exists({t}, :zero) + :one, {a} = :at",
            t = unscored::TRIES,
            a = unscored::RETRY_AT
        ))
        .expression_attribute_values(":zero", crate::passes::attr_n(0))
        .expression_attribute_values(":one", crate::passes::attr_n(1))
        .expression_attribute_values(":at", attr_s(&now.to_rfc3339()))
        .send()
        .await;
    if let Err(e) = r {
        warn!(run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Scoring sweep: could not count the try on the run");
    }
}

/// The sweep stopped trying a run's scoring: why, on the run, for the page.
async fn mark_stopped(state: &WorkerState, team: &str, run_id: &str, why: &str) {
    error!(
        run_id,
        why, "Scoring sweep: stopped trying to score a closed pull request"
    );
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team))
        .key("run_id", attr_s(run_id))
        .condition_expression("attribute_exists(run_id)")
        .update_expression(format!("SET {} = :w", unscored::GAVE_UP))
        .expression_attribute_values(":w", attr_s(why))
        .send()
        .await;
    if let Err(e) = r {
        warn!(run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Scoring sweep: could not note why it stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::github_budget::Hold;

    #[test]
    fn a_held_budget_is_a_wait_until_it_frees() {
        let now = 1_794_000_000;
        let held = Spend::Hold {
            why: Hold::ReservedForRuns,
            wait_secs: 1_500,
        };
        assert_eq!(
            hold_of(held, 0, 12, None, now),
            Some((Cause::RateLimit, 1_500))
        );
        // Room for less than a whole scoring: wait for the reset.
        assert_eq!(
            hold_of(Spend::Go, 5, 12, Some(now + 900), now),
            Some((Cause::RateLimit, 900))
        );
        // No reset known: the next hour, never under a minute.
        let at_hour_end = (now / 3600 + 1) * 3600 - 10;
        assert_eq!(
            hold_of(Spend::Go, 0, 12, None, at_hour_end),
            Some((Cause::RateLimit, 60))
        );
        assert_eq!(hold_of(Spend::Go, 12, 12, None, now), None);
    }

    #[test]
    fn a_lesson_waits_as_itself() {
        let msg = LessonMessage {
            team_id: "TEAM#a".into(),
            run_id: "R1".into(),
            trigger: "pr_closed".into(),
            ..Default::default()
        };
        let now = 1_794_000_000;
        let Again::Send { msg: next, delay } = again(&msg, Cause::RateLimit, 1_500, now) else {
            panic!("should wait");
        };
        assert_eq!(delay, gw::MAX_HOP_SECS);
        assert_eq!(
            (next.github_wait.waits, next.trigger.as_str()),
            (1, "pr_closed")
        );
        assert_eq!(still_waiting(&next, now), Some(gw::MAX_HOP_SECS));
        assert_eq!(still_waiting(&next, next.github_wait.until), None);
        // It round-trips on the queue with its wait.
        let raw = serde_json::to_value(next.clone().message()).unwrap();
        assert_eq!(raw["type"], "lesson");
        let back: WorkerMessage = serde_json::from_value(raw).unwrap();
        let WorkerMessage::Lesson(back) = back else {
            panic!("a lesson");
        };
        assert_eq!(back.github_wait, next.github_wait);
    }
}
