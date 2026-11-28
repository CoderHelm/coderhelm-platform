//! The absolute ceiling of a run: what makes "a run cannot loop without end"
//! true whatever the code above it does.
//!
//! `run_guard` stops a run that makes no progress, and real progress resets it.
//! This doesn't reset. Each run record counts passes, continuations, spend and
//! worked time for the run's whole life (`ceil_*`), and only adds to them. A
//! person's Retry starts a new run, with a new record: that is the one new
//! allowance. The layers hold independently:
//!
//! 1. The counts, on the run record, added to by every pass and continuation
//!    (`run_guard`) and every message's time ([`worked`]).
//! 2. [`admit`], at the one place the worker picks up a message, before any
//!    dispatch: a message of a run over its ceiling doesn't run, whatever sent
//!    it. `WorkerMessage::run_ref` has no catch-all, so a new message kind has to
//!    say whether it belongs to a run or it doesn't compile.
//! 3. Model calls: `UsageMeter::admit`, which every model call must go through
//!    (`common::model_call_sites` fails the build otherwise), refuses once the
//!    run's allowance is spent or closed.
//! 4. The stuck-run sweep (`lambda/stuck-run-cleanup/handler.py`) reads the same
//!    counts outside the worker and hands off any run over them.
//!
//! Worked time is the time the worker spent on the run, not the calendar: a
//! pull request can rightly wait days for CI or a person, and that costs nothing.
//! It is the union of the run's invocations ([`Clock`]), never their sum: two
//! messages of one run that overlap (a CI resume beside a review event, a wait
//! sent on while a step runs) count their shared minutes once. A step that was
//! killed never says when it ended; it is known killed only once its marker is
//! older than the longest a step can live ([`STEP_SECS`] and
//! [`KILLED_AFTER_SLACK_SECS`]), and only then counts for all of its time,
//! recorded on the run (`ceil_killed`). A marker of an invocation still running
//! is never a kill.
//!
//! Sized for the largest real work the caps allow, far above any loop's stop by
//! `run_guard`: the existing 50-step ceiling at about $4 and 15 minutes a step,
//! plus the rounds on the pull request.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use common::messages::WorkerMessage;
use common::usage::RunAllowance;
use tracing::warn;

use super::{attr_n, attr_s};
use crate::WorkerState;

/// Passes in a run's life. The record keeps at most 200 in its history
/// (`run_lists::MAX_PASSES`), and a 50-step run with its fix rounds and a long
/// conversation on its pull request stays under this.
pub const MAX_RUN_PASSES: u32 = 300;
/// Continuations in a run's life: the step ceiling that was already there.
pub const MAX_RUN_CONTINUATIONS: u32 = super::steps::MAX_RUN_STEPS;
/// Hours the worker spends on a run: 50 steps of 15 minutes, 12.5 hours, and
/// room for the fix rounds on the pull request.
pub const MAX_RUN_WORKED_SECS: u64 = 16 * 3600;
/// USD in a run's life: 50 steps at about $4.
pub const MAX_RUN_SPEND_USD: f64 = 200.0;

/// What a step that never reported its end counts for: all of its time, the
/// most one invocation lives (the worker's Lambda timeout).
pub const STEP_SECS: u64 = 900;

/// Past [`STEP_SECS`], how long before a marker is taken for a killed step:
/// the function's start and its first write are not the same instant.
pub const KILLED_AFTER_SLACK_SECS: u64 = 60;

/// The run's worked time as intervals: the invocations running now (by
/// invocation, when each started) and the counted time that could still
/// overlap them. Everything here is epoch seconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clock {
    /// Invocations that said they started and not yet that they ended.
    pub live: std::collections::BTreeMap<String, u64>,
    /// Counted time, merged, sorted: only what ends after the oldest live
    /// start is kept, since nothing counted later can start before it.
    pub spans: Vec<(u64, u64)>,
}

/// What an invocation's start found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Entered {
    /// Markers of steps that were killed (older than any step lives).
    pub killed: u32,
    /// Seconds those kills add that weren't counted already.
    pub charged: u64,
}

impl Clock {
    /// Adds `[from, to)` to the counted time; returns the seconds of it that
    /// weren't counted yet.
    pub fn count(&mut self, from: u64, to: u64) -> u64 {
        if to <= from {
            return 0;
        }
        let covered: u64 = self
            .spans
            .iter()
            .map(|&(s, e)| e.min(to).saturating_sub(s.max(from)))
            .sum();
        let (mut lo, mut hi) = (from, to);
        let mut kept: Vec<(u64, u64)> = Vec::with_capacity(self.spans.len() + 1);
        for &(s, e) in &self.spans {
            if e < lo || s > hi {
                kept.push((s, e));
            } else {
                lo = lo.min(s);
                hi = hi.max(e);
            }
        }
        kept.push((lo, hi));
        kept.sort_unstable();
        self.spans = kept;
        (to - from) - covered
    }

    /// An invocation `id` starts at `now`. A marker older than any step
    /// lives was a killed step: it is taken off and counts for all of its
    /// time. A marker of a step that may still be running counts nothing.
    pub fn enter(&mut self, id: &str, now: u64) -> Entered {
        let dead: Vec<(String, u64)> = self
            .live
            .iter()
            .filter(|(_, &at)| at + STEP_SECS + KILLED_AFTER_SLACK_SECS <= now)
            .map(|(k, &at)| (k.clone(), at))
            .collect();
        let mut out = Entered::default();
        for (k, at) in dead {
            self.live.remove(&k);
            out.killed += 1;
            out.charged += self.count(at, at + STEP_SECS);
        }
        self.live.insert(id.to_string(), now);
        self.prune(now);
        out
    }

    /// Invocation `id`, which started at `started`, ends at `now`: the
    /// seconds it adds are the part of its time nothing else counted.
    pub fn leave(&mut self, id: &str, started: u64, now: u64) -> u64 {
        // Its own marker says when it started; a marker already taken for
        // a kill (it outlived the limit) still counts only what is new.
        let from = self.live.remove(id).unwrap_or(started).min(started);
        let added = self.count(from, now.max(from));
        self.prune(now);
        added
    }

    /// Drops counted time nothing to come can overlap: whatever ends before
    /// the oldest live start (or `now`, with nothing live).
    fn prune(&mut self, now: u64) {
        let floor = self.live.values().copied().min().unwrap_or(now).min(now);
        self.spans.retain(|&(_, e)| e > floor);
    }

    /// As kept on the run: `id=start;id=start` and `start-end,start-end`.
    pub fn to_attrs(&self) -> (String, String) {
        let live = self
            .live
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(";");
        let spans = self
            .spans
            .iter()
            .map(|(s, e)| format!("{s}-{e}"))
            .collect::<Vec<_>>()
            .join(",");
        (live, spans)
    }

    pub fn from_attrs(live: &str, spans: &str) -> Self {
        let live = live
            .split(';')
            .filter_map(|p| {
                let (k, v) = p.split_once('=')?;
                Some((k.to_string(), v.parse().ok()?))
            })
            .collect();
        let spans = spans
            .split(',')
            .filter_map(|p| {
                let (s, e) = p.split_once('-')?;
                Some((s.parse().ok()?, e.parse().ok()?))
            })
            .collect();
        Self { live, spans }
    }
}

/// A run's counts, for its whole life.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Totals {
    pub passes: u32,
    pub continuations: u32,
    pub worked_secs: u64,
    pub spend_usd: f64,
}

/// Which ceiling a run is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Over {
    Passes,
    Continuations,
    WorkedTime,
    Spend,
}

impl Over {
    /// What analytics counts (`loop_stop_reason`). The sweep writes the same.
    pub fn code(self) -> &'static str {
        match self {
            Over::Passes => "ceiling_passes",
            Over::Continuations => "ceiling_continuations",
            Over::WorkedTime => "ceiling_worked_time",
            Over::Spend => "ceiling_spend",
        }
    }

    pub fn reason(self) -> String {
        let what = match self {
            Over::Passes => format!("it reached the limit of {MAX_RUN_PASSES} steps of work"),
            Over::Continuations => {
                format!("it reached the limit of {MAX_RUN_CONTINUATIONS} continuations")
            }
            Over::WorkedTime => format!(
                "it reached the limit of {} hours of work",
                MAX_RUN_WORKED_SECS / 3600
            ),
            Over::Spend => format!("it reached the limit of ${MAX_RUN_SPEND_USD:.0} of model use"),
        };
        format!(
            "CoderHelm ended this run: {what}, and nothing further runs on it. Everything it did \
             is on its branch and draft pull request. Retry starts a new run from there."
        )
    }
}

impl Totals {
    /// Whether this run is over its ceiling.
    pub fn over(&self) -> Option<Over> {
        if self.spend_usd > MAX_RUN_SPEND_USD {
            return Some(Over::Spend);
        }
        if self.worked_secs > MAX_RUN_WORKED_SECS {
            return Some(Over::WorkedTime);
        }
        if self.passes > MAX_RUN_PASSES {
            return Some(Over::Passes);
        }
        if self.continuations > MAX_RUN_CONTINUATIONS {
            return Some(Over::Continuations);
        }
        None
    }

    pub fn from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Self {
        let n = |k: &str| -> f64 {
            item.get(k)
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0.0)
        };
        Self {
            passes: n("ceil_passes") as u32,
            continuations: n("ceil_continuations") as u32,
            worked_secs: n("ceil_worked_secs") as u64,
            spend_usd: n("ceil_spend_usd"),
        }
    }

    /// What is left to spend, never below nothing.
    pub fn spend_left(&self) -> f64 {
        (MAX_RUN_SPEND_USD - self.spend_usd).max(0.0)
    }
}

/// What the one entry point needs from the outside world.
#[allow(async_fn_in_trait)]
pub trait EntryGate {
    /// The run's counts. `None` when they can't be read: the entry fails open,
    /// and the model and sweep layers still hold.
    async fn totals(&self, team_id: &str, run_id: &str) -> Option<Totals>;
    /// Ends a run over its ceiling, once, without running the message.
    async fn end_over(&self, message: &WorkerMessage, over: Over);
}

/// The entry guard: whether this message may run. Comes before any dispatch.
pub async fn admit<G: EntryGate>(gate: &G, message: &WorkerMessage) -> bool {
    let Some((team_id, run_id)) = message.run_ref() else {
        return true;
    };
    let Some(totals) = gate.totals(team_id, run_id).await else {
        return true;
    };
    match totals.over() {
        None => true,
        Some(over) => {
            warn!(
                team_id,
                run_id,
                stop_reason = over.code(),
                ?totals,
                "Run over its ceiling: the message doesn't run"
            );
            gate.end_over(message, over).await;
            false
        }
    }
}

/// The real thing, on the run record.
pub struct DynamoGate<'a> {
    pub state: &'a WorkerState,
}

impl EntryGate for DynamoGate<'_> {
    async fn totals(&self, team_id: &str, run_id: &str) -> Option<Totals> {
        load(self.state, team_id, run_id).await
    }

    async fn end_over(&self, message: &WorkerMessage, over: Over) {
        let Some((team_id, run_id)) = message.run_ref() else {
            return;
        };
        let state = self.state;
        super::run_guard::record_stop(state, team_id, run_id, over.code(), &over.reason()).await;
        // Fresh ticket steps have what a draft takes; the rest have their PR.
        if let WorkerMessage::Ticket(msg) = message {
            let checkpoint = super::load_checkpoint(state, team_id, run_id).await;
            super::run_guard::end_if_stopped(
                state,
                msg,
                run_id,
                checkpoint.as_ref().map(|c| c.1.as_str()),
            )
            .await;
        } else {
            super::run_guard::settle_open_pr(state, team_id, run_id).await;
        }
    }
}

pub(crate) async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Option<Totals> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("ceil_passes, ceil_continuations, ceil_worked_secs, ceil_spend_usd")
        .send()
        .await
    {
        Ok(out) => out.item().map(Totals::from_item),
        Err(e) => {
            warn!(run_id, error = %e, "Run ceiling: could not read the run");
            None
        }
    }
}

/// Runs a message with its model calls metered against what the run has left,
/// and adds its time to the run's. A run with no record yet (a fresh ticket) has
/// its whole allowance.
pub async fn worked<F: std::future::Future>(
    state: &WorkerState,
    message: &WorkerMessage,
    fut: F,
) -> F::Output {
    let run = message
        .run_ref()
        .map(|(t, r)| (t.to_string(), r.to_string()));
    let left = match &run {
        Some((t, r)) => load(state, t, r)
            .await
            .map_or(MAX_RUN_SPEND_USD, |t| t.spend_left()),
        None => MAX_RUN_SPEND_USD,
    };
    let id = ulid::Ulid::new().to_string();
    let started = now_epoch();
    if let Some((t, r)) = &run {
        mark_entered(state, t, r, &id, started).await;
    }
    let out = common::usage::with_run_allowance(Arc::new(RunAllowance::new(left)), fut).await;
    if let Some((t, r)) = &run {
        mark_left(state, t, r, &id, started).await;
    }
    out
}

fn now_epoch() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// Writes of the clock tried before the step's time is left uncounted (each
/// lost write is another invocation's, which is read again).
const CLOCK_WRITES: u32 = 8;

/// An invocation starts: its marker is set, and any marker of a killed step
/// is charged once.
async fn mark_entered(state: &WorkerState, team_id: &str, run_id: &str, id: &str, now: u64) {
    let killed = update_clock(state, team_id, run_id, |c| {
        let e = c.enter(id, now);
        (e.charged, e.killed)
    })
    .await;
    if let Some((charged, killed)) = killed.filter(|(_, k)| *k > 0) {
        warn!(
            run_id,
            killed,
            charged,
            "Run ceiling: a step of this run was killed and counts for all its time"
        );
    }
}

/// The invocation ends: it adds what of its time nothing else counted.
async fn mark_left(state: &WorkerState, team_id: &str, run_id: &str, id: &str, started: u64) {
    let now = now_epoch();
    update_clock(state, team_id, run_id, |c| (c.leave(id, started, now), 0)).await;
}

/// Reads the run's clock, applies `f` (seconds to add, kills found), and
/// writes it back if nobody wrote in between; read again otherwise. The
/// worked time itself is only ever added to.
async fn update_clock(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    f: impl Fn(&mut Clock) -> (u64, u32),
) -> Option<(u64, u32)> {
    for _ in 0..CLOCK_WRITES {
        let item = match state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(team_id))
            .key("run_id", attr_s(run_id))
            .projection_expression("run_id, ceil_live, ceil_spans, ceil_ver")
            .consistent_read(true)
            .send()
            .await
        {
            Ok(o) => o.item?,
            Err(e) => {
                warn!(run_id, error = %e, "Run ceiling: could not read the run's clock");
                return None;
            }
        };
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let ver: u64 = item
            .get("ceil_ver")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let mut clock = Clock::from_attrs(&s("ceil_live"), &s("ceil_spans"));
        let (secs, killed) = f(&mut clock);
        let (live, spans) = clock.to_attrs();
        let cond = if ver == 0 {
            "attribute_exists(run_id) AND attribute_not_exists(ceil_ver)"
        } else {
            "attribute_exists(run_id) AND ceil_ver = :v0"
        };
        let mut req = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(team_id))
            .key("run_id", attr_s(run_id))
            .update_expression(
                "SET ceil_live = :live, ceil_spans = :spans, ceil_ver = :v1 \
                 ADD ceil_worked_secs :s, ceil_killed :k REMOVE ceil_entered_at",
            )
            .condition_expression(cond)
            .expression_attribute_values(":live", attr_s(&live))
            .expression_attribute_values(":spans", attr_s(&spans))
            .expression_attribute_values(":v1", attr_n(ver + 1))
            .expression_attribute_values(":s", attr_n(secs))
            .expression_attribute_values(":k", attr_n(killed));
        if ver > 0 {
            req = req.expression_attribute_values(":v0", attr_n(ver));
        }
        match req.send().await {
            Ok(_) => return Some((secs, killed)),
            Err(e) if common::run_fail::is_condition_failed(&e) => continue,
            Err(e) => {
                warn!(run_id, error = %e, "Run ceiling: could not add the step's time");
                return None;
            }
        }
    }
    warn!(
        run_id,
        "Run ceiling: the run's clock kept changing; this step's time is not added"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_ceiling_fires_on_its_own() {
        assert_eq!(Totals::default().over(), None);
        let at = Totals {
            passes: MAX_RUN_PASSES,
            continuations: MAX_RUN_CONTINUATIONS,
            worked_secs: MAX_RUN_WORKED_SECS,
            spend_usd: MAX_RUN_SPEND_USD,
        };
        assert_eq!(at.over(), None, "at the ceiling is still under it");
        let over = |f: fn(&mut Totals)| {
            let mut t = Totals::default();
            f(&mut t);
            t.over()
        };
        assert_eq!(over(|t| t.passes = MAX_RUN_PASSES + 1), Some(Over::Passes));
        assert_eq!(
            over(|t| t.continuations = MAX_RUN_CONTINUATIONS + 1),
            Some(Over::Continuations)
        );
        assert_eq!(
            over(|t| t.worked_secs = MAX_RUN_WORKED_SECS + 1),
            Some(Over::WorkedTime)
        );
        assert_eq!(
            over(|t| t.spend_usd = MAX_RUN_SPEND_USD + 0.01),
            Some(Over::Spend)
        );
    }

    /// The numbers are big enough for what the existing caps allow, and the
    /// no-progress guard always speaks first in a loop.
    #[test]
    fn the_ceiling_clears_the_largest_real_work_and_sits_above_the_guard() {
        use super::super::run_guard as g;
        const { assert!(MAX_RUN_PASSES >= 3 * super::super::steps::MAX_RUN_STEPS) };
        const { assert!(MAX_RUN_WORKED_SECS >= 50 * 900) };
        const { assert!(MAX_RUN_PASSES > g::MAX_TRANSITIONS_WITHOUT_PROGRESS) };
        const { assert!(MAX_RUN_CONTINUATIONS > g::MAX_CONTINUATIONS_WITHOUT_PROGRESS) };
        const { assert!(MAX_RUN_SPEND_USD >= 50.0 * 4.0) };
        const { assert!(MAX_RUN_SPEND_USD > g::MAX_SPEND_WITHOUT_PROGRESS_USD) };
    }

    #[test]
    fn the_sweep_reads_the_same_numbers_and_the_same_names() {
        let src = include_str!("../../../../lambda/stuck-run-cleanup/handler.py");
        assert!(src.contains(&format!("MAX_RUN_PASSES = {MAX_RUN_PASSES}\n")));
        assert!(src.contains(&format!(
            "MAX_RUN_CONTINUATIONS = {MAX_RUN_CONTINUATIONS}\n"
        )));
        assert!(src.contains(&format!("MAX_RUN_WORKED_SECS = {MAX_RUN_WORKED_SECS}\n")));
        assert!(src.contains(&format!("MAX_RUN_SPEND_USD = {MAX_RUN_SPEND_USD:.1}\n")));
        for name in [
            "ceil_passes",
            "ceil_continuations",
            "ceil_worked_secs",
            "ceil_spend_usd",
            "guard_open",
            "loop_stop_reason",
            "loop_stop_detail",
        ] {
            assert!(src.contains(name), "{name}");
        }
        for over in [
            Over::Passes,
            Over::Continuations,
            Over::WorkedTime,
            Over::Spend,
        ] {
            assert!(src.contains(over.code()), "{}", over.code());
        }
    }

    #[test]
    fn the_reason_says_what_to_do() {
        let why = Over::Spend.reason();
        assert!(why.contains("$200"), "{why}");
        assert!(why.contains("Retry starts a new run"), "{why}");
    }
}
