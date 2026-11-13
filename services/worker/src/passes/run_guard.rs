//! One progress guard for a whole run, and the limits it keeps.
//!
//! A small ticket once finished all its tasks and then spent 2.5 hours and
//! $41.96 without a pull request. Each 15-minute step resumed at the checks
//! before the pull request, re-ran them from nothing, and started fresh fix
//! rounds, because every round counter was a local variable of that step. Every
//! fix was a commit, so the stall detector saw progress, and the pull request
//! came last, with whatever time was left.
//!
//! The order is the first fix: once the work is built the draft pull request
//! opens, and the checks run on it. They can't hold it back, because it is
//! already there.
//!
//! The class of bug is a loop that only looks busy, so it is closed in one
//! place rather than in each pass:
//!
//! - Every pass goes through `update_pass`, every continuation through
//!   `try_auto_continue`, and every code-writing round through
//!   `implement::run`. Those three call this module, so a pass added later is
//!   covered without knowing the guard exists.
//! - The guard counts pass transitions, continuations and spend since the last
//!   real progress: a task completing, the pull request opening, CI going
//!   green, or fewer open findings than a check has ever reported. A commit is
//!   not progress. Each of these only moves one way (or is capped elsewhere),
//!   so a checker that keeps finding something can't keep resetting the count.
//! - When the guard fires a run ends one way, always: a draft pull request
//!   with what exists and its Open items, status `needs_input`, a plain reason
//!   on the run page, one notification, and the reason recorded for analytics.
//!
//! [`Ledger`] is the other half: the pull request the run opened, what each
//! check found on which head, and how many fix rounds it took, kept across
//! steps. It is what makes "at most 2 rounds per run" and "no check runs twice on
//! the same head" true whichever step the run is in.
//!
//! Every limit is here. How they were sized is in [`worst_normal_run`], which a
//! test holds against them.

use std::collections::BTreeMap;
use std::sync::Mutex;

use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use common::run_status::{self, EndedValues};
use common::run_wait::NeedsInput;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::proof::{self, Finding};
use super::run_ceiling::Totals;
use super::{add_progress_note, attr_n, attr_s, feature, test_fix};
use crate::models::TicketMessage;
use crate::WorkerState;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ── Limits ─────────────────────────────────────────────────────────────────

/// Proof-check fix rounds in one run, across all its steps.
pub const MAX_PROOF_FIX_ROUNDS: u32 = 2;
/// New-feature-rule fix rounds in one run, across all its steps.
pub const MAX_RULES_FIX_ROUNDS: u32 = 2;
/// Link and redirect fix rounds in one run, across all its steps.
pub const MAX_LINK_FIX_ROUNDS: u32 = 1;
/// Failing-test fix rounds in one run, across all its steps.
pub const MAX_TEST_FIX_ROUNDS: u32 = 2;
/// Continuations a run may take once its tasks are done. Findings going down
/// don't reset this: opening the pull request and checking it take one step,
/// and three more fresh 15-minute steps are far more than finished work needs.
pub const MAX_CONTINUATIONS_AFTER_WORK: u32 = 3;
/// Continuations without progress while the tasks are still being built: one
/// task spread over six full steps (90 minutes) isn't happening.
pub const MAX_CONTINUATIONS_WITHOUT_PROGRESS: u32 = 6;
/// Pass transitions without progress. See [`worst_normal_run`].
pub const MAX_TRANSITIONS_WITHOUT_PROGRESS: u32 = 40;
/// Spend without progress, in USD, whichever pass or step spent it. Only real
/// progress lowers it. A full 15-minute step costs about $4 (the run that
/// prompted this guard spent $41.96 in eleven), so this is five full steps with
/// nothing to show, about half of that run.
pub const MAX_SPEND_WITHOUT_PROGRESS_USD: f64 = 20.0;
/// Times the plan check's count may fall and its recovery still count as
/// progress; past that, only a new best does.
pub const MAX_TASK_DROPS: u32 = 2;

/// The most pass transitions a normal large ticket makes between two progress
/// events, with every fix round every check is allowed. A pass count is bounded
/// by the caps themselves, so this is the worst a run that isn't looping can
/// reach, and the guard must sit above both stretches.
///
/// - From the last task done to the pull request: test, its fix, security, its
///   fix, the re-audit, a generator fix, and the pull request.
/// - From the pull request to CI's first green: the checks, every check's
///   rounds at two transitions a round (to `implement` and back), every
///   automatic push the run is allowed (`MAX_AUTO_FIX_PUSHES`) and the own-review
///   rounds, each `implement` then `review`.
pub const fn worst_normal_run() -> u32 {
    let per_round = 2;
    let before_pr = 7;
    let after_pr = 1 + per_round
        * (MAX_PROOF_FIX_ROUNDS
            + MAX_RULES_FIX_ROUNDS
            + MAX_LINK_FIX_ROUNDS
            + MAX_TEST_FIX_ROUNDS
            + super::MAX_AUTO_FIX_PUSHES
            + super::own_review::MAX_FIX_ROUNDS);
    if before_pr > after_pr {
        before_pr
    } else {
        after_pr
    }
}

const _: () = assert!(MAX_TRANSITIONS_WITHOUT_PROGRESS >= worst_normal_run());

// ── The guard's state ──────────────────────────────────────────────────────

/// Why the guard ended a run. The stored code is what analytics counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trip {
    /// Finished work kept continuing without a pull request.
    ContinuationsAfterWork,
    /// Continuations without progress, tasks still open.
    Continuations,
    /// Pass transitions without progress.
    Transitions,
    /// Spend without progress.
    Spend,
}

impl Trip {
    pub fn code(self) -> &'static str {
        match self {
            Trip::ContinuationsAfterWork => "continuations_after_work",
            Trip::Continuations => "continuations",
            Trip::Transitions => "pass_transitions",
            Trip::Spend => "spend",
        }
    }

    /// What the run page says, in plain words.
    pub fn reason(self, g: &GuardState) -> String {
        let what = match self {
            Trip::ContinuationsAfterWork => format!(
                "its tasks were done and it had continued {} more times without finishing",
                g.after_work
            ),
            Trip::Continuations => format!(
                "it continued {} times without finishing another task",
                g.continuations
            ),
            Trip::Transitions => format!(
                "it moved between steps {} times with no task finished, no fewer open findings \
                 and no green CI",
                g.transitions
            ),
            Trip::Spend => format!(
                "it spent ${:.2} with no task finished, no fewer open findings and no green CI",
                g.spend_usd
            ),
        };
        format!(
            "CoderHelm stopped this run and handed it to you: {what}. Everything it did is on the \
             draft pull request, and what is still open is listed there under Open items. Push to \
             the branch or comment on the pull request and CoderHelm picks it up from there."
        )
    }
}

/// Real progress: what resets the guard's counts.
#[derive(Debug, Clone, PartialEq)]
pub enum Progress {
    /// This many of the plan's tasks are done.
    TasksDone(usize),
    /// Every required task is done.
    WorkDone,
    /// The pull request is open.
    PrOpen,
    /// CI came back green.
    CiGreen,
    /// A check reports this many open findings.
    Findings(&'static str, usize),
}

/// The guard's counts, kept on the run record (`run_guard`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GuardState {
    /// Pass transitions since the last progress.
    #[serde(default)]
    pub transitions: u32,
    /// Continuations since the last progress.
    #[serde(default)]
    pub continuations: u32,
    /// Continuations since the tasks were done (never reset by findings).
    #[serde(default)]
    pub after_work: u32,
    /// Spend since the last progress, from every model call of every step.
    #[serde(default)]
    pub spend_usd: f64,
    /// The most tasks done so far.
    #[serde(default)]
    pub tasks_done: usize,
    /// The last plan check's count (`None` before the first one).
    #[serde(default)]
    pub tasks_last: Option<usize>,
    /// How many times the count fell below the one before it.
    #[serde(default)]
    pub task_drops: u32,
    /// The run's coder wrote code since the count last fell. Only then is a
    /// recovery the run's own work done again: a count that falls and comes
    /// back with nothing written moved because of what CoderHelm did to the
    /// branch or measured it against (a rebuild, a rebase, another base), not
    /// because the run did anything.
    #[serde(default)]
    pub wrote_since_fall: bool,
    #[serde(default)]
    pub work_done: bool,
    #[serde(default)]
    pub pr_open: bool,
    /// Each check's lowest open-finding count so far. Only a new low counts:
    /// a checker reporting 2, 4, 3, 2 has made no progress at all.
    #[serde(default)]
    pub low: BTreeMap<String, usize>,
}

impl GuardState {
    /// One pass started, with what was spent since the guard last looked.
    pub fn on_pass(&mut self, spent: f64) {
        self.transitions = self.transitions.saturating_add(1);
        self.spend_usd += spent.max(0.0);
    }

    /// One continuation, with what was spent since the guard last looked.
    pub fn on_continuation(&mut self, spent: f64) {
        self.continuations = self.continuations.saturating_add(1);
        self.spend_usd += spent.max(0.0);
        if self.work_done {
            self.after_work = self.after_work.saturating_add(1);
        }
    }

    /// Applies a report. True (and the counts reset) when it is real progress.
    /// This is the only place the spend comes down, and each kind of progress
    /// moves one way, so nothing can reset it twice with the same news.
    pub fn apply(&mut self, p: &Progress) -> bool {
        let real = match p {
            Progress::TasksDone(n) => {
                let n = *n;
                let prev = self.tasks_last.unwrap_or(self.tasks_done);
                if n < prev {
                    self.task_drops = self.task_drops.saturating_add(1);
                    self.wrote_since_fall = false;
                }
                self.tasks_last = Some(n);
                let more = n > self.tasks_done;
                // Work the count lost (someone else moved the base, a revert)
                // done again by the run is progress, not a repeat of old
                // news: measured from the last count, only when the run wrote
                // code since the fall (CoderHelm's own rebuild or rebase of
                // the branch, undone by measuring again, is nothing the run
                // did), and a bounded number of times so a count that keeps
                // falling and rising still ends.
                let recovered = !more
                    && n > prev
                    && self.wrote_since_fall
                    && self.task_drops > 0
                    && self.task_drops <= MAX_TASK_DROPS;
                self.tasks_done = self.tasks_done.max(n);
                more || recovered
            }
            Progress::WorkDone => !std::mem::replace(&mut self.work_done, true),
            Progress::PrOpen => !std::mem::replace(&mut self.pr_open, true),
            Progress::CiGreen => true,
            Progress::Findings(check, n) => match self.low.get(*check).copied() {
                // The first report is where the check starts, not progress.
                None => {
                    self.low.insert((*check).to_string(), *n);
                    false
                }
                Some(low) if *n < low => {
                    self.low.insert((*check).to_string(), *n);
                    true
                }
                Some(_) => false,
            },
        };
        if real {
            self.reset();
        }
        real
    }

    /// The run's coder wrote code (`implement::run` changed files). Not
    /// progress by itself: a commit never is.
    pub fn on_own_code(&mut self) {
        self.wrote_since_fall = true;
    }

    fn reset(&mut self) {
        self.transitions = 0;
        self.continuations = 0;
        self.spend_usd = 0.0;
    }

    /// A person acted on the run: everything starts again from here, and what
    /// the checks have reported stands.
    pub fn person_acted(&mut self) {
        self.reset();
        self.after_work = 0;
    }

    /// Whether the run has gone too far without progress.
    pub fn trip(&self) -> Option<Trip> {
        if self.after_work > MAX_CONTINUATIONS_AFTER_WORK {
            return Some(Trip::ContinuationsAfterWork);
        }
        if self.continuations > MAX_CONTINUATIONS_WITHOUT_PROGRESS {
            return Some(Trip::Continuations);
        }
        if self.transitions > MAX_TRANSITIONS_WITHOUT_PROGRESS {
            return Some(Trip::Transitions);
        }
        if self.spend_usd > MAX_SPEND_WITHOUT_PROGRESS_USD {
            return Some(Trip::Spend);
        }
        None
    }
}

/// What this message's model calls cost since the guard last looked: live,
/// because a step's usage reaches the run record only at its end, and a step
/// cut off by the time limit never writes it at all.
fn take_spend() -> f64 {
    common::usage::take_run_spend()
}

/// The error a pass stops with once the guard fired. `orchestrate_ticket` and
/// the worker's resume and feedback handlers end the run on it.
#[derive(Debug)]
pub struct LoopStopped(pub String);

impl std::fmt::Display for LoopStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for LoopStopped {}

// ── The guard on the run record ────────────────────────────────────────────

struct Loaded {
    guard: GuardState,
    /// The run's counts for its whole life (see `run_ceiling`).
    totals: Totals,
    /// The reason, once the guard fired and no person has acted since.
    stopped: Option<String>,
}

/// Fails open: a read that doesn't answer never stops a run.
async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Option<Loaded> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression(
            "run_guard, guard_open, loop_stop_detail, ceil_passes, ceil_continuations, \
             ceil_worked_secs, ceil_spend_usd",
        )
        .send()
        .await;
    let out = match out {
        Ok(o) => o,
        Err(e) => {
            warn!(run_id, error = %e, "Run guard: could not read the run");
            return None;
        }
    };
    let item = out.item()?;
    Some(Loaded {
        totals: Totals::from_item(item),
        guard: item
            .get("run_guard")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default(),
        stopped: item.get("guard_open").map(|_| {
            item.get("loop_stop_detail")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "CoderHelm stopped this run and handed it to you.".to_string())
        }),
    })
}

/// Saves the guard's counts and adds to the run's lifetime ones. Those are only
/// ever added to, and this is the one place that does for passes, continuations
/// and spend.
async fn save(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    guard: &GuardState,
    passes: u32,
    continuations: u32,
    spent: f64,
) {
    let Ok(json) = serde_json::to_string(guard) else {
        return;
    };
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET run_guard = :g ADD ceil_passes :p, ceil_continuations :c, ceil_spend_usd :s",
        )
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(":g", attr_s(&json))
        .expression_attribute_values(":p", attr_n(passes))
        .expression_attribute_values(":c", attr_n(continuations))
        .expression_attribute_values(":s", attr_n(format!("{spent:.6}")))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Run guard: could not save its counts");
    }
}

/// Records that a run was stopped, once. Only the first writer logs the metric
/// and the note. Also closes this message's model allowance.
pub(crate) async fn record_stop(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    code: &str,
    reason: &str,
) {
    common::usage::close_run_allowance(reason);
    let first = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET guard_open = :yes, loop_stop_reason = :code, loop_stop_detail = :why, \
             loop_stopped_at = :now",
        )
        .condition_expression("attribute_exists(run_id) AND attribute_not_exists(guard_open)")
        .expression_attribute_values(":yes", AttributeValue::Bool(true))
        .expression_attribute_values(":code", attr_s(code))
        .expression_attribute_values(":why", attr_s(reason))
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .is_ok();
    if first {
        // The metric filter in the monitoring stack counts this text.
        warn!(
            run_id,
            team_id,
            stop_reason = code,
            "run_loop_stopped: the run is handed to a person"
        );
        add_progress_note(state, team_id, run_id, reason).await;
    }
}

async fn fire(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    trip: Trip,
    guard: &GuardState,
) -> String {
    let reason = trip.reason(guard);
    info!(
        run_id,
        transitions = guard.transitions,
        continuations = guard.continuations,
        after_work = guard.after_work,
        spend_usd = guard.spend_usd,
        "Run guard: no progress"
    );
    record_stop(state, team_id, run_id, trip.code(), &reason).await;
    reason
}

/// What the counts say now: the run's ceiling first, it is the harder stop.
async fn verdict(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    guard: &GuardState,
    totals: &Totals,
) -> Option<String> {
    if let Some(over) = totals.over() {
        let why = over.reason();
        record_stop(state, team_id, run_id, over.code(), &why).await;
        return Some(why);
    }
    match guard.trip() {
        Some(trip) => Some(fire(state, team_id, run_id, trip, guard).await),
        None => None,
    }
}

/// A pass started (from `update_pass`, so every pass). `Err` once the guard
/// has fired: the run must stop.
pub async fn on_pass(state: &WorkerState, team_id: &str, run_id: &str) -> Result<(), BoxError> {
    let Some(mut l) = load(state, team_id, run_id).await else {
        return Ok(());
    };
    if let Some(why) = l.stopped {
        return Err(Box::new(LoopStopped(why)));
    }
    let spent = take_spend();
    l.guard.on_pass(spent);
    l.totals.passes = l.totals.passes.saturating_add(1);
    l.totals.spend_usd += spent;
    save(state, team_id, run_id, &l.guard, 1, 0, spent).await;
    match verdict(state, team_id, run_id, &l.guard, &l.totals).await {
        Some(why) => Err(Box::new(LoopStopped(why))),
        None => Ok(()),
    }
}

/// The pass each run goes on with after a wait for GitHub, in this process:
/// entering it again is not a pass transition (`common::github_wait`).
static REENTRY: std::sync::LazyLock<Mutex<std::collections::HashMap<String, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// The run goes on with `pass` after a wait for GitHub: its next entry into
/// that pass is not counted as a new pass.
pub fn forgive_reentry(run_id: &str, pass: &str) {
    if let Ok(mut m) = REENTRY.lock() {
        m.insert(run_id.to_string(), pass.to_string());
    }
}

/// Whether entering `pass` now is the run going on after a wait for GitHub
/// (once: the entry after it counts as usual).
pub fn take_reentry(run_id: &str, pass: &str) -> bool {
    let Ok(mut m) = REENTRY.lock() else {
        return false;
    };
    match m.get(run_id) {
        Some(p) if p == pass => {
            m.remove(run_id);
            true
        }
        Some(_) => {
            // The run went on elsewhere: whatever it enters counts.
            m.remove(run_id);
            false
        }
        None => false,
    }
}

/// A step is about to hand off (from `try_auto_continue`, so every
/// continuation). `Some(reason)` when the next step must not start.
pub async fn on_continuation(state: &WorkerState, team_id: &str, run_id: &str) -> Option<String> {
    let mut l = load(state, team_id, run_id).await?;
    if l.stopped.is_some() {
        return l.stopped;
    }
    let spent = take_spend();
    l.guard.on_continuation(spent);
    l.totals.continuations = l.totals.continuations.saturating_add(1);
    l.totals.spend_usd += spent;
    save(state, team_id, run_id, &l.guard, 0, 1, spent).await;
    verdict(state, team_id, run_id, &l.guard, &l.totals).await
}

/// Before a code-writing round (from `implement::run`): `Err` once the guard
/// has fired. This holds the rounds whose callers can't stop on a pass error.
pub async fn check(state: &WorkerState, team_id: &str, run_id: &str) -> Result<(), BoxError> {
    match load(state, team_id, run_id).await.and_then(|l| l.stopped) {
        Some(why) => Err(Box::new(LoopStopped(why))),
        None => Ok(()),
    }
}

/// Reports what may be real progress.
pub async fn progress(state: &WorkerState, team_id: &str, run_id: &str, p: Progress) {
    let Some(mut l) = load(state, team_id, run_id).await else {
        return;
    };
    let spent = take_spend();
    // What was spent before this news counts against the time before it: real
    // progress clears the guard's count, never the run's lifetime one.
    l.guard.spend_usd += spent;
    let drops = l.guard.task_drops;
    let before = l.guard.tasks_last.unwrap_or(l.guard.tasks_done);
    if l.guard.apply(&p) {
        info!(run_id, progress = ?p, "Run guard: progress");
    }
    if l.guard.task_drops > drops {
        // Never silent: a fall in finished tasks is work to find, not a run
        // that merely stopped moving.
        warn!(
            run_id,
            before,
            now = l.guard.tasks_last.unwrap_or(0),
            best = l.guard.tasks_done,
            "Run guard: the plan check finds fewer tasks done than before"
        );
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!(
                "The plan check now finds {} planned task(s) done, down from {before}: the \
                 branch or what it is measured against changed. The run goes on from here.",
                l.guard.tasks_last.unwrap_or(0)
            ),
        )
        .await;
    }
    save(state, team_id, run_id, &l.guard, 0, 0, spent).await;
}

/// The run's coder wrote code: a fall in the plan check's count after this
/// and its recovery can be the run's own work (see [`GuardState::apply`]).
pub async fn wrote_code(state: &WorkerState, team_id: &str, run_id: &str) {
    let Some(mut l) = load(state, team_id, run_id).await else {
        return;
    };
    if l.guard.wrote_since_fall {
        return;
    }
    l.guard.on_own_code();
    save(state, team_id, run_id, &l.guard, 0, 0, 0.0).await;
}

/// A person acted on the run (a comment, a review): the counts start over and
/// an earlier stop no longer holds it. `loop_stop_reason` stays, for analytics.
pub async fn person_acted(state: &WorkerState, team_id: &str, run_id: &str) {
    let Some(mut l) = load(state, team_id, run_id).await else {
        return;
    };
    l.guard.person_acted();
    let Ok(json) = serde_json::to_string(&l.guard) else {
        return;
    };
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET run_guard = :g REMOVE guard_open, loop_notified_at")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(":g", attr_s(&json))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Run guard: could not start its counts over");
    }
}

/// A message is ending, perhaps because a model call was refused: what it spent
/// is counted, and a run that has gone over its ceiling is marked as stopped.
async fn flush(state: &WorkerState, team_id: &str, run_id: &str) {
    let Some(mut l) = load(state, team_id, run_id).await else {
        return;
    };
    if l.stopped.is_some() {
        return;
    }
    let spent = take_spend();
    if spent > 0.0 {
        l.guard.spend_usd += spent;
        l.totals.spend_usd += spent;
        save(state, team_id, run_id, &l.guard, 0, 0, spent).await;
    }
    let _ = verdict(state, team_id, run_id, &l.guard, &l.totals).await;
}

/// Claims the one notification a stop sends.
async fn claim_notification(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET loop_notified_at = :now")
        .condition_expression("attribute_not_exists(loop_notified_at)")
        .expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        .return_values(ReturnValue::None)
        .send()
        .await
        .is_ok()
}

/// A run stopped in a ticket step: when the guard fired, opens the draft (or
/// finds it) with what the checks left under Open items, and waits on a person. True when the guard had fired, so the caller doesn't
/// also salvage or fail the run the ordinary way.
pub async fn end_if_stopped(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: Option<&str>,
) -> bool {
    flush(state, &msg.team_id, run_id).await;
    let Some(reason) = load(state, &msg.team_id, run_id)
        .await
        .and_then(|l| l.stopped)
    else {
        return false;
    };
    let ledger = LedgerCell::load(
        state,
        &msg.team_id,
        run_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
    )
    .await;
    let mut items = ledger.update(|l| l.open_findings());
    items.push(proof::blocked_item("CoderHelm stopped this run", &reason));
    // The guard's limit is hard; what the end says is the stop judge's:
    // a named blocker for a person, or the draft with its assumption.
    let mut usage = crate::models::TokenUsage::default();
    let mut picture = super::stop_judge::handoff_picture(msg, "run_guard", &reason, &items);
    if let Some(l) = load(state, &msg.team_id, run_id).await {
        picture.budget = super::stop_judge::Budget {
            spend_usd: l.totals.spend_usd,
            spend_cap_usd: super::run_ceiling::MAX_RUN_SPEND_USD,
            continuations: l.totals.continuations,
            continuation_cap: super::run_ceiling::MAX_RUN_CONTINUATIONS,
            ..Default::default()
        };
    }
    let judged = super::stop_judge::decide_for_team(
        state,
        &mut usage,
        &msg.team_id,
        &super::stop_judge::run_subject(run_id),
        "",
        picture,
    )
    .await;
    let notify = claim_notification(state, &msg.team_id, run_id).await;
    let opened = match branch.filter(|b| !b.is_empty()) {
        Some(branch) => {
            super::stop_pr::open_with(state, msg, run_id, branch, &reason, &items, notify, &judged)
                .await
        }
        None => Err("no branch to open a pull request from".into()),
    };
    if let Err(e) = opened {
        // Nothing to open (no work yet): still never silent.
        warn!(run_id, error = %e, "Run guard: no draft pull request for the stopped run");
        super::mark_run_stopped_by_guard(state, msg, run_id, &reason, &judged).await;
    }
    true
}

/// A run that already has its pull request (a resume or feedback step): when
/// the guard fired, it waits on a person with the reason on the run page. True
/// when it had fired.
pub async fn settle_open_pr(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    flush(state, team_id, run_id).await;
    let Some(reason) = load(state, team_id, run_id).await.and_then(|l| l.stopped) else {
        return false;
    };
    let mut usage = crate::models::TokenUsage::default();
    let mut picture =
        super::stop_judge::Picture::new(super::stop_judge::Kind::Handoff, "run_guard", &reason);
    picture.ticket_title = format!("run {run_id}");
    let judged = super::stop_judge::decide_for_team(
        state,
        &mut usage,
        team_id,
        &super::stop_judge::run_subject(run_id),
        "",
        picture,
    )
    .await;
    wait_on_open_pr(state, team_id, run_id, &reason, &judged).await;
    true
}

/// The run waits on its pull request's review, with the guard's reason and
/// the judge's blocker or assumption.
async fn wait_on_open_pr(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    reason: &str,
    judged: &super::stop_judge::Judged,
) {
    // The supervisor's rule: only a need on the human-only list waits on a
    // person. A guard stop on a run that has its pull request otherwise
    // finishes it: completed with the reason on the run page (the pull
    // request lists what is open), or failed when its checks are red.
    let Some(hand_back) = super::supervisor::hand_back_for(judged, reason) else {
        finish_open_pr(state, team_id, run_id, reason).await;
        return;
    };
    let reason = match (judged.hold_words(), judged.assumption()) {
        (Some(h), _) => format!("{reason} Blocked on: {h}"),
        (None, Some(a)) => format!("{reason} Assumed: {a}"),
        _ => reason.to_string(),
    };
    let reason = &reason;
    let now = chrono::Utc::now().to_rfc3339();
    // It has its pull request: it waits for that pull request's review.
    let (number, url) = run_pr(state, team_id, run_id).await;
    let wait = super::stop_pr::review_wait(number, &url, reason);
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, waiting_on = :wo, error_message = :em, \
             updated_at = :t, current_pass = :cp",
        )
        .condition_expression(run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .needs_input(run_id, &wait, &hand_back)
        .expression_attribute_values(":em", attr_s(reason))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Run guard: could not hand the run to a person");
    }
    if claim_notification(state, team_id, run_id).await {
        super::webhooks::emit_run(state, team_id, run_id, run_status::NEEDS_INPUT).await;
    }
}

/// A guard stop on a run with its pull request and nothing a person must
/// supply: `failed` with the reason when the run's checks are red by its
/// record (`common::run_fail`), else `completed` with the reason noted.
async fn finish_open_pr(state: &WorkerState, team_id: &str, run_id: &str, reason: &str) {
    if let Some(red) = super::resume::red_ci_blocks_completion(state, team_id, run_id).await {
        let why = format!("{}; {red}", reason.trim().trim_end_matches('.'));
        let failure = common::run_fail::RunFailure {
            reason: &why,
            pass: None,
            only_from: None,
            extra: Vec::new(),
            add: Vec::new(),
        };
        match common::run_fail::fail_run(
            &state.dynamo,
            &state.config.runs_table_name,
            team_id,
            run_id,
            &failure,
        )
        .await
        {
            Ok(true) => super::webhooks::emit_run(state, team_id, run_id, "failed").await,
            Ok(false) => {}
            Err(e) => warn!(run_id, error = %e, "Run guard: could not end the run"),
        }
        return;
    }
    add_progress_note(
        state,
        team_id,
        run_id,
        &format!("Finished on its pull request; left open there: {reason}"),
    )
    .await;
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, deferred_reason = :dr, updated_at = :t, \
             current_pass = :cp",
        )
        .condition_expression(run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(run_status::COMPLETED))
        .expression_attribute_values(
            ":sri",
            attr_s(&format!("{}#{run_id}", run_status::COMPLETED)),
        )
        .expression_attribute_values(":dr", attr_s(common::truncate_str(reason, 500)))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .send()
        .await;
    match res {
        Ok(_) => {
            super::webhooks::emit_run(state, team_id, run_id, run_status::COMPLETED).await;
            super::ready::after_completion(state, team_id, run_id).await;
        }
        Err(e) => warn!(run_id, error = %e, "Run guard: could not finish the run"),
    }
}

/// The run's pull request (number, address), from its record.
async fn run_pr(state: &WorkerState, team_id: &str, run_id: &str) -> (u64, String) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("pr_number, pr_url")
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .unwrap_or_default();
    let number = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let url = item
        .get("pr_url")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    (number, url)
}

// ── What the checks found, across steps ────────────────────────────────────

/// One check's fix rounds and last result, for the whole run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CheckRecord {
    /// Fix rounds started in this run.
    #[serde(default)]
    pub rounds: u32,
    /// The branch head the last check ran on.
    #[serde(default)]
    pub head: String,
    /// Open findings at the last check.
    #[serde(default)]
    pub open: usize,
    /// Open findings when the last fix round started, until the check after it.
    #[serde(default)]
    pub open_at_fix: Option<usize>,
    /// A fix round is running. Still set when a step loads the record: that
    /// round was cut off.
    #[serde(default)]
    pub fixing: bool,
    /// No more fixing: a round made no progress, or time ran out in one.
    #[serde(default)]
    pub stopped: bool,
}

impl CheckRecord {
    /// A check on this head has already run: its result stands.
    pub fn unchanged(&self, head: &str) -> bool {
        !head.is_empty() && self.head == head
    }

    /// A check ran. Same or more findings than before the fix round ends the
    /// fixing at once.
    pub fn checked(&mut self, head: &str, open: usize) {
        if self.open_at_fix.take().is_some_and(|before| open >= before) {
            self.stopped = true;
        }
        self.head = head.to_string();
        self.open = open;
    }

    /// No check ran because the code didn't change. If a fix round just ran,
    /// it changed nothing, which is no progress.
    pub fn reused(&mut self) {
        if self.open_at_fix.take().is_some() {
            self.stopped = true;
        }
    }

    pub fn may_fix(&self, max_rounds: u32) -> bool {
        !self.stopped && self.rounds < max_rounds
    }

    /// A fix round starts. The first needs no judgment; any later one only
    /// with the stop judge's licence (`passes::stop_judge::CycleLicence`).
    pub fn start_fix(&mut self, _licence: super::stop_judge::CycleLicence) {
        self.rounds = self.rounds.saturating_add(1);
        self.open_at_fix = Some(self.open);
        self.fixing = true;
    }

    pub fn end_fix(&mut self) {
        self.fixing = false;
    }
}

/// The pull request the run opened: a step resuming after it never opens it
/// again, nor pays to describe it again.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenedPr {
    pub number: u64,
    pub url: String,
    #[serde(default)]
    pub node_id: String,
}

/// The run's checks, kept across steps on the checkpoint table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Ledger {
    #[serde(default)]
    pub proof: CheckRecord,
    #[serde(default)]
    pub rules: CheckRecord,
    #[serde(default)]
    pub links: CheckRecord,
    #[serde(default)]
    pub test_fix: CheckRecord,
    /// The tests and scope check (`habits_check`).
    #[serde(default)]
    pub habits: CheckRecord,
    /// What the tests and scope check last came to.
    #[serde(default)]
    pub habits_outcome: Option<Result<Option<super::habits_check::PrFacts>, String>>,
    /// That check has asked once for a step of its own for its sandbox
    /// runs: it never asks twice.
    #[serde(default)]
    pub habits_waited: bool,
    /// What the proof check last found (`None`: the judge couldn't answer).
    #[serde(default)]
    pub proof_findings: Option<Vec<Finding>>,
    /// What the new-feature rules last came to.
    #[serde(default)]
    pub rules_outcome: Option<Result<Option<feature::PrFacts>, String>>,
    /// What the link and redirect checks last came to.
    #[serde(default)]
    pub links_outcome: Option<Result<String, String>>,
    /// What the failing-test rules last came to.
    #[serde(default)]
    pub test_outcome: Option<Result<Option<test_fix::PrFacts>, String>>,
    /// The head the adversarial test last ran on.
    #[serde(default)]
    pub tested_head: String,
    /// The head the search for someone else's pull request last ran on.
    #[serde(default)]
    pub existing_work_head: String,
    /// Set once the pull request is open.
    #[serde(default)]
    pub opened: Option<OpenedPr>,
    /// CoderHelm's own open pull request that covers the ticket and lacks
    /// part of it: the run builds on it and carries its work there
    /// (`covering`).
    #[serde(default)]
    pub covering: Option<super::covering::Remembered>,
    /// The version of the code whose checks wrote the results above
    /// (`proof::checks_version`); empty for a ledger written before it was
    /// kept.
    #[serde(default)]
    pub checks_version: String,
}

/// A record larger than this drops its longest text: DynamoDB's item limit is
/// 400 KB, and the checks' results are worth more than their evidence prose.
const MAX_LEDGER_BYTES: usize = 300 * 1024;

impl Ledger {
    /// A new step picks the ledger up. A round still marked as running was cut
    /// off by the time limit, so nothing fixes any more: the checks run once if
    /// the code moved, and the pull request opens with what they find.
    pub fn on_step_start(&mut self) {
        let records = [
            &mut self.proof,
            &mut self.rules,
            &mut self.links,
            &mut self.test_fix,
            &mut self.habits,
        ];
        if records.iter().any(|r| r.fixing) {
            for r in records {
                r.fixing = false;
                r.stopped = true;
            }
        }
    }

    /// The checks run by code at `version`. A result another version wrote
    /// doesn't stand on an unchanged head for the checks whose failure is a
    /// hold (the link and redirect check, the failing-test rules, the
    /// new-feature rules): the rule that failed may be fixed now, so they run
    /// again. Their fix rounds still count. The proof check's result stands:
    /// what it leaves is never a check the change didn't pass. True when the
    /// results were set aside.
    pub fn for_checks_version(&mut self, version: &str) -> bool {
        if self.checks_version == version {
            return false;
        }
        self.checks_version = version.to_string();
        let mut set_aside = false;
        for r in [&mut self.rules, &mut self.links, &mut self.test_fix] {
            set_aside |= !r.head.is_empty();
            r.head.clear();
        }
        set_aside
    }

    /// What is still open, for a run the guard stopped. Honest about both:
    /// what the checks found, and what they didn't get to.
    pub fn open_findings(&self) -> Vec<Finding> {
        let mut open: Vec<Finding> = Vec::new();
        match &self.proof_findings {
            Some(f) => open.extend(f.iter().cloned()),
            None if self.proof.head.is_empty() => {
                open.push(proof::blocked_item(
                    "The proof check",
                    "it didn't finish, so what the change rests on isn't confirmed",
                ));
            }
            None => {}
        }
        match &self.links_outcome {
            Some(Err(why)) => open.push(proof::check_item("The link and redirect check", why)),
            Some(Ok(_)) => {}
            None => {}
        }
        match &self.test_outcome {
            Some(Err(why)) => open.push(proof::check_item("The failing-test rules", why)),
            Some(Ok(_)) => {}
            None => {}
        }
        match &self.rules_outcome {
            Some(Err(why)) => open.push(proof::check_item("The new-feature rules", why)),
            Some(Ok(Some(facts))) => open.extend(feature::open_findings(facts)),
            Some(Ok(None)) => {}
            None => open.push(proof::blocked_item(
                "The new-feature rules",
                "they didn't finish",
            )),
        }
        if let Some(Err(why)) = &self.habits_outcome {
            open.push(proof::check_item("The tests and scope check", why));
        }
        open
    }

    /// As stored, within the item limit.
    pub fn to_json_bounded(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        if json.len() <= MAX_LEDGER_BYTES {
            return json;
        }
        // The counts, the heads and the pull request always survive: they are
        // what keep the limits. What is dropped is checked again.
        let mut smaller = self.clone();
        smaller.proof_findings = None;
        smaller.proof.head = String::new();
        smaller.rules_outcome = None;
        smaller.rules.head = String::new();
        smaller.links_outcome = None;
        smaller.links.head = String::new();
        smaller.test_outcome = None;
        smaller.test_fix.head = String::new();
        smaller.habits_outcome = None;
        smaller.habits.head = String::new();
        serde_json::to_string(&smaller).unwrap_or_default()
    }
}

/// A run's ledger for one repository, shared by the checks of a step.
#[derive(Debug, Default)]
pub struct LedgerCell {
    ledger: Mutex<Ledger>,
    scope: String,
}

fn ledger_sk(run_id: &str, scope: &str) -> String {
    format!("RUN#{run_id}#CHECKS#{scope}")
}

impl LedgerCell {
    pub fn new(scope: &str, ledger: Ledger) -> Self {
        Self {
            ledger: Mutex::new(ledger),
            scope: scope.to_string(),
        }
    }

    /// Reads, or changes, the ledger.
    pub fn update<R>(&self, f: impl FnOnce(&mut Ledger) -> R) -> R {
        let mut guard = self.ledger.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    pub async fn load(state: &WorkerState, team_id: &str, run_id: &str, scope: &str) -> Self {
        let mut ledger = Ledger::default();
        if !state.config.checkpoints_table_name.is_empty() {
            if let Ok(out) = state
                .dynamo
                .get_item()
                .table_name(&state.config.checkpoints_table_name)
                .key("team_id", attr_s(team_id))
                .key("sk", attr_s(&ledger_sk(run_id, scope)))
                .send()
                .await
            {
                if let Some(saved) = out
                    .item()
                    .and_then(|i| i.get("data"))
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| serde_json::from_str(s).ok())
                {
                    ledger = saved;
                }
            }
        }
        Self::new(scope, ledger)
    }

    /// Saved at every change that must survive the step being cut off.
    pub async fn save(&self, state: &WorkerState, team_id: &str, run_id: &str) {
        if state.config.checkpoints_table_name.is_empty() {
            return;
        }
        let json = self.update(|l| l.to_json_bounded());
        let ttl = chrono::Utc::now().timestamp() as u64 + 7 * 86400;
        if let Err(e) = state
            .dynamo
            .put_item()
            .table_name(&state.config.checkpoints_table_name)
            .item("team_id", attr_s(team_id))
            .item("sk", attr_s(&ledger_sk(run_id, &self.scope)))
            .item("data", attr_s(&json))
            .item("ttl", attr_n(ttl))
            .send()
            .await
        {
            warn!(run_id, error = %e, "Could not save what the checks found");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_limit_is_close_enough_to_the_worst_normal_run_to_matter() {
        const { assert!(MAX_TRANSITIONS_WITHOUT_PROGRESS <= 2 * worst_normal_run()) };
    }

    #[test]
    fn a_commit_is_not_progress_only_these_are() {
        let mut g = GuardState::default();
        for _ in 0..10 {
            g.on_pass(0.0);
        }
        // A check's first report sets where it starts.
        assert!(!g.apply(&Progress::Findings("proof", 2)));
        assert_eq!(g.transitions, 10);
        // Same or more is nothing, whatever was committed in between.
        assert!(!g.apply(&Progress::Findings("proof", 2)));
        assert!(!g.apply(&Progress::Findings("proof", 4)));
        assert!(!g.apply(&Progress::Findings("proof", 3)));
        assert_eq!(g.transitions, 10);
        // A new low counts, once; going back up and down to it doesn't.
        assert!(g.apply(&Progress::Findings("proof", 1)));
        assert_eq!(g.transitions, 0);
        assert!(!g.apply(&Progress::Findings("proof", 3)));
        assert!(!g.apply(&Progress::Findings("proof", 1)));
        // Another check's count is its own.
        assert!(!g.apply(&Progress::Findings("rules", 1)));
        // Tasks, the pull request and CI.
        assert!(g.apply(&Progress::TasksDone(3)));
        assert!(!g.apply(&Progress::TasksDone(3)));
        assert!(g.apply(&Progress::WorkDone));
        assert!(!g.apply(&Progress::WorkDone));
        assert!(g.apply(&Progress::PrOpen));
        assert!(!g.apply(&Progress::PrOpen));
        assert!(g.apply(&Progress::CiGreen));
    }

    /// No pass and no continuation lowers the spend: only real progress does,
    /// and the same news twice is not progress.
    #[test]
    fn nothing_but_real_progress_resets_the_spend() {
        let mut g = GuardState::default();
        g.apply(&Progress::TasksDone(5));
        g.apply(&Progress::WorkDone);
        g.apply(&Progress::PrOpen);
        g.apply(&Progress::Findings("proof", 2));
        let mut spent = 0.0;
        for i in 0..200 {
            g.on_pass(0.5);
            spent += 0.5;
            assert!((g.spend_usd - spent).abs() < 1e-9, "pass {i}");
            if i % 3 == 0 {
                g.on_continuation(0.0);
            }
            assert!(!g.apply(&Progress::TasksDone(5)));
            assert!(!g.apply(&Progress::WorkDone));
            assert!(!g.apply(&Progress::PrOpen));
            assert!(!g.apply(&Progress::Findings("proof", 2 + (i % 3))));
            assert!((g.spend_usd - spent).abs() < 1e-9, "pass {i}");
        }
        assert!(g.apply(&Progress::CiGreen));
        assert_eq!(g.spend_usd, 0.0);
    }

    #[tokio::test]
    async fn spend_is_metered_as_it_happens_within_a_step() {
        // Outside a metered step nothing is counted, and nothing breaks.
        common::usage::note_run_spend(5.0);
        assert_eq!(take_spend(), 0.0);
        let allowance = std::sync::Arc::new(common::usage::RunAllowance::new(100.0));
        common::usage::with_run_allowance(allowance, async {
            let mut usage = crate::models::TokenUsage::default();
            usage.add_call(
                "claude-opus-5-5",
                &common::usage::CallUsage {
                    uncached_input: 1_000_000,
                    ..Default::default()
                },
            );
            let cost = usage.cost_usd;
            assert!(cost > 0.0);
            assert!((take_spend() - cost).abs() < 1e-3);
            assert_eq!(take_spend(), 0.0, "each dollar counted once");
        })
        .await;
    }

    #[test]
    fn fewer_findings_never_buy_finished_work_more_continuations() {
        let mut g = GuardState::default();
        g.apply(&Progress::WorkDone);
        g.apply(&Progress::Findings("proof", 9));
        for (i, open) in [8, 7, 6].into_iter().enumerate() {
            g.on_continuation(1.0);
            assert!(g.apply(&Progress::Findings("proof", open)));
            assert_eq!(g.trip(), None, "continuation {i}");
        }
        g.on_continuation(1.0);
        assert_eq!(g.trip(), Some(Trip::ContinuationsAfterWork));
    }

    #[test]
    fn each_limit_fires_on_its_own() {
        let mut g = GuardState::default();
        for _ in 0..MAX_TRANSITIONS_WITHOUT_PROGRESS {
            g.on_pass(0.0);
        }
        assert_eq!(g.trip(), None);
        g.on_pass(0.0);
        assert_eq!(g.trip(), Some(Trip::Transitions));

        let mut g = GuardState::default();
        for _ in 0..MAX_CONTINUATIONS_WITHOUT_PROGRESS {
            g.on_continuation(0.0);
        }
        assert_eq!(g.trip(), None);
        g.on_continuation(0.0);
        assert_eq!(g.trip(), Some(Trip::Continuations));

        let mut g = GuardState::default();
        g.on_pass(MAX_SPEND_WITHOUT_PROGRESS_USD);
        assert_eq!(g.trip(), None);
        g.on_pass(0.01);
        assert_eq!(g.trip(), Some(Trip::Spend));
        g.apply(&Progress::CiGreen);
        assert_eq!(g.trip(), None);
    }

    #[test]
    fn a_person_acting_starts_the_counts_over_and_keeps_the_lows() {
        let mut g = GuardState {
            after_work: 9,
            transitions: 99,
            continuations: 9,
            spend_usd: 99.0,
            ..Default::default()
        };
        g.apply(&Progress::Findings("proof", 3));
        assert!(g.trip().is_some());
        g.person_acted();
        assert_eq!(g.trip(), None);
        assert!(!g.apply(&Progress::Findings("proof", 3)));
    }

    #[test]
    fn the_reason_is_plain_and_says_what_to_do() {
        let g = GuardState {
            after_work: 4,
            ..Default::default()
        };
        let why = Trip::ContinuationsAfterWork.reason(&g);
        assert!(why.contains("tasks were done"), "{why}");
        assert!(why.contains("continued 4 more times"), "{why}");
        assert!(why.contains("Open items"), "{why}");
        assert!(why.contains("Push to the branch or comment"), "{why}");
        let g = GuardState {
            spend_usd: 20.5,
            ..Default::default()
        };
        assert!(Trip::Spend.reason(&g).contains("$20.50"));
    }

    #[test]
    fn stop_codes_are_stable_for_analytics() {
        assert_eq!(
            [
                Trip::ContinuationsAfterWork,
                Trip::Continuations,
                Trip::Transitions,
                Trip::Spend
            ]
            .map(Trip::code),
            [
                "continuations_after_work",
                "continuations",
                "pass_transitions",
                "spend"
            ]
        );
    }

    #[test]
    fn a_round_with_no_fewer_findings_ends_the_fixing() {
        let mut r = CheckRecord::default();
        r.checked("a", 2);
        assert!(r.may_fix(2));
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        r.checked("b", 2);
        assert!(!r.may_fix(2), "same findings: no second round");

        let mut r = CheckRecord::default();
        r.checked("a", 2);
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        r.checked("b", 4);
        assert!(!r.may_fix(2), "more findings: no second round");

        let mut r = CheckRecord::default();
        r.checked("a", 3);
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        r.checked("b", 1);
        assert!(r.may_fix(2), "fewer findings: a second round");
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        r.checked("c", 0);
        assert!(!r.may_fix(2), "two rounds is the limit");
    }

    #[test]
    fn a_round_that_changed_nothing_ends_the_fixing() {
        let mut r = CheckRecord::default();
        r.checked("a", 2);
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        assert!(r.unchanged("a"), "no commit: no re-check");
        r.reused();
        assert!(!r.may_fix(2));
    }

    #[test]
    fn reusing_a_result_between_steps_is_not_a_failed_round() {
        let mut r = CheckRecord::default();
        r.checked("a", 3);
        r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        r.end_fix();
        r.checked("b", 1);
        assert!(r.unchanged("b"));
        r.reused();
        assert!(r.may_fix(2));
    }

    #[test]
    fn an_unknown_head_is_never_called_unchanged() {
        let r = CheckRecord::default();
        assert!(!r.unchanged(""));
        assert!(!r.unchanged("a"));
    }

    #[test]
    fn time_running_out_in_a_round_ends_every_checks_fixing() {
        let mut l = Ledger::default();
        l.rules.checked("a", 1);
        l.rules
            .start_fix(crate::passes::stop_judge::CycleLicence::for_test());
        // The step is cut off here; the next one picks the ledger up.
        let mut next: Ledger = serde_json::from_str(&l.to_json_bounded()).unwrap();
        next.on_step_start();
        assert!(!next.rules.may_fix(MAX_RULES_FIX_ROUNDS));
        assert!(!next.proof.may_fix(MAX_PROOF_FIX_ROUNDS));
        assert!(!next.links.may_fix(MAX_LINK_FIX_ROUNDS));
        assert!(!next.test_fix.may_fix(MAX_TEST_FIX_ROUNDS));
        // Without a round cut off, a new step changes nothing.
        let mut calm = Ledger::default();
        calm.proof.checked("a", 1);
        calm.on_step_start();
        assert!(calm.proof.may_fix(MAX_PROOF_FIX_ROUNDS));
    }

    #[test]
    fn the_ledger_round_trips_every_checks_result() {
        let l = Ledger {
            proof: CheckRecord {
                rounds: 2,
                head: "abc".into(),
                open: 3,
                ..Default::default()
            },
            proof_findings: Some(vec![Finding {
                point: "assumption".into(),
                detail: "the vendor sends cents".into(),
                ..Default::default()
            }]),
            rules_outcome: Some(Ok(Some(feature::PrFacts {
                duplicates: vec!["a helper".into()],
                ..Default::default()
            }))),
            links_outcome: Some(Err("a redirect 404s".into())),
            test_outcome: Some(Ok(None)),
            opened: Some(OpenedPr {
                number: 7,
                url: "https://example.test/7".into(),
                node_id: "n".into(),
            }),
            ..Default::default()
        };
        let back: Ledger = serde_json::from_str(&l.to_json_bounded()).unwrap();
        assert_eq!(back, l);
        // An older record without these fields still reads.
        let old: Ledger = serde_json::from_str("{}").unwrap();
        assert_eq!(old, Ledger::default());
    }

    #[test]
    fn a_huge_ledger_keeps_its_counts_and_its_pull_request() {
        let l = Ledger {
            proof: CheckRecord {
                rounds: 2,
                head: "abc".into(),
                ..Default::default()
            },
            rules_outcome: Some(Err("x".repeat(MAX_LEDGER_BYTES + 10))),
            opened: Some(OpenedPr {
                number: 7,
                ..Default::default()
            }),
            ..Default::default()
        };
        let json = l.to_json_bounded();
        assert!(json.len() <= MAX_LEDGER_BYTES);
        let back: Ledger = serde_json::from_str(&json).unwrap();
        assert_eq!(back.proof.rounds, 2, "the counts always survive");
        assert_eq!(back.opened.unwrap().number, 7);
        // What was dropped isn't called unchanged, so it is checked again.
        assert!(!back.proof.unchanged("abc"));
    }

    #[test]
    fn open_items_report_every_finding_and_what_never_ran() {
        let l = Ledger {
            proof: CheckRecord {
                head: "abc".into(),
                ..Default::default()
            },
            proof_findings: Some(vec![Finding {
                point: "assumption".into(),
                detail: "the vendor sends cents".into(),
                ..Default::default()
            }]),
            rules_outcome: Some(Err("new items aren't registered".into())),
            links_outcome: Some(Err("a redirect 404s".into())),
            test_outcome: Some(Err("the change loosens tests".into())),
            ..Default::default()
        };
        let text = proof::gaps_section(&l.open_findings());
        for said in [
            "the vendor sends cents",
            "new items aren't registered",
            "a redirect 404s",
            "the change loosens tests",
        ] {
            assert!(text.contains(said), "{said} in {text}");
        }
        assert!(proof::holds_blocked(&text), "these keep it a draft");
        // A check that never finished says so rather than saying nothing.
        let text = proof::gaps_section(&Ledger::default().open_findings());
        assert!(text.contains("proof check"), "{text}");
        assert!(text.contains("didn't finish"), "{text}");
        assert!(text.contains("new-feature rules"), "{text}");
    }

    #[test]
    fn rules_left_failing_go_under_open_items_without_a_second_hold() {
        let facts = feature::PrFacts {
            checklist: vec![feature::checklist::Item::new(
                "Tests",
                feature::checklist::Status::Fail,
                "no spec for the new page",
            )],
            sandbox_failed: vec!["The unit tests fail".into(), "Output: boom".into()],
            ..Default::default()
        };
        let open = feature::open_findings(&facts);
        assert_eq!(open.len(), 2);
        let text = proof::gaps_section(&open);
        assert!(text.contains("no spec for the new page"), "{text}");
        assert!(text.contains("The unit tests fail"), "{text}");
        assert!(!text.contains("boom"), "{text}");
        // The checklist's own ready check holds the draft and clears itself.
        assert!(!proof::holds_blocked(&text));
    }

    #[test]
    fn a_blocked_item_holds_the_draft_until_it_is_gone() {
        let text = proof::gaps_section(&[proof::blocked_item(
            "The new-feature rules",
            "new items aren't registered",
        )]);
        assert!(crate::passes::resume::draft_hold(&text, "t", true).is_some());
        assert!(crate::passes::resume::draft_hold("nothing open", "t", true).is_none());
    }
}
