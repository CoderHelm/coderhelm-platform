//! A ticket run works in steps, and a step's time is never a reason to fail.
//!
//! One worker invocation (a step) has at most 15 minutes. A new-feature run
//! once triaged, planned (with ~6 minutes of plan review), built eight files,
//! and reached the generator check with 30 seconds left: the check declined
//! to start a build and the run failed "not enough time left", with the work
//! done and the generator never run.
//!
//! Now every place that lacks time hands off instead: the run continues in a
//! fresh step (a NEW queue message with its own receive count and its own 15
//! minutes) that resumes exactly where this one stopped: the implement work,
//! the generator check ([`Gate::Generators`]) or the checks before the pull
//! request ([`Gate::Checks`]), with the branch as it is.
//!
//! A run keeps going as long as its steps make progress ([`stalled_after`]):
//! the branch head moved, or the run reached a later gate. It stops when two
//! steps in a row make none (with what was stuck), when the team's usage
//! limit stops it, or when a person cancels it. [`MAX_RUN_STEPS`] is only a
//! runaway ceiling, never reached by real work.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, warn};

use crate::models::TicketMessage;
use crate::passes::security;
use crate::WorkerState;

/// A runaway ceiling only: a run whose steps keep making progress is never
/// stopped by a count, but no loop can run forever. Logged loudly when hit.
pub const MAX_RUN_STEPS: u32 = 50;

/// Steps in a row that make no progress before the run stops.
pub const MAX_STALLED_STEPS: u32 = 2;

/// Seconds between steps: the next one starts right away (SQS allows up to
/// 900).
pub const STEP_DELAY_SECS: i32 = 5;

/// The step a message runs: the first invocation is step 1.
pub fn step_number(continuation: u32) -> u32 {
    continuation.saturating_add(1)
}

/// May the step after `continuation` start? Only the runaway ceiling says
/// no; a stalled run is stopped by [`stalled_after`].
pub fn under_ceiling(continuation: u32) -> bool {
    let next = step_number(continuation).saturating_add(1);
    if next > MAX_RUN_STEPS {
        error!(
            next_step = next,
            ceiling = MAX_RUN_STEPS,
            "RUNAWAY CEILING HIT: a ticket run reached the step ceiling; this never happens in normal work"
        );
        return false;
    }
    true
}

/// The next step's message: a new message (never a retry of this one), the
/// same run, one step on. SQS counts receives per message, so every step
/// starts at a receive count of 1, however many steps the run takes.
pub fn next_message(msg: &TicketMessage, run_id: &str, first_attempt_ms: u64) -> TicketMessage {
    let mut next = msg.clone();
    next.continuation = msg.continuation.saturating_add(1);
    next.continuation_run_id = Some(run_id.to_string());
    next.first_attempt_ms = first_attempt_ms;
    next
}

// ── Progress ────────────────────────────────────────────────────────────────

/// Where a run stands at the start of a step: its branch head and the point
/// it resumes at. A step made progress when the next step's mark differs:
/// a commit (implement work, a fix round, regenerated files) moves the head,
/// and a gate passed moves the resume point.
pub fn progress_mark(head: &str, at: &str) -> String {
    format!("{head}|{at}")
}

/// Steps in a row without progress, after the step that just ended: zero
/// when its mark moved (or there is no earlier mark), else one more.
pub fn stalled_after(prev: Option<&str>, now: &str, stalled: u32) -> u32 {
    match prev {
        Some(p) if !p.is_empty() && p == now => stalled.saturating_add(1),
        _ => 0,
    }
}

/// Whether a run with this many stalled steps stops.
pub fn stops_for_stall(stalled: u32) -> bool {
    stalled >= MAX_STALLED_STEPS
}

/// Why a stalled run stops, in a sentence.
pub fn stall_reason(step: u32, head: &str, at: &str, kept: &str) -> String {
    let short: String = head.chars().take(7).collect();
    format!(
        "Stopped: steps {} and {} made no progress. The branch stayed at {short} and the run was \
         stuck at {}. The work so far is kept on branch `{kept}`.",
        step.saturating_sub(2).max(1),
        step.saturating_sub(1).max(1),
        where_label(at)
    )
}

fn where_label(at: &str) -> &str {
    match at {
        "verify" => "the test and security audit after the implement work",
        "generators" => "the generator check (regenerating generated files)",
        "checks" => "the checks before the pull request",
        "implement_started" | "implement" => "the implement work (planned tasks)",
        "security" => "the security audit",
        other => other,
    }
}

// ── Resume points ──────────────────────────────────────────────────────────

/// A point after implement where a fresh step can pick the run up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gate {
    /// Everything after the implement work and its plan check: the
    /// generators run with the work, the adversarial test, the security
    /// audit, then the gates below.
    Verify,
    /// The generator check: regenerate, verify, one fix round.
    Generators,
    /// The proof, link, test-fix and new-feature checks, then the pull
    /// request.
    Checks,
}

impl Gate {
    pub fn as_str(self) -> &'static str {
        match self {
            Gate::Verify => "verify",
            Gate::Generators => "generators",
            Gate::Checks => "checks",
        }
    }

    pub fn parse(s: &str) -> Option<Gate> {
        match s {
            "verify" => Some(Gate::Verify),
            "generators" => Some(Gate::Generators),
            "checks" => Some(Gate::Checks),
            _ => None,
        }
    }

    /// Whether a run resuming at `resume` runs the stage at `self`. Stages
    /// before every gate (plan check, test, security) run only without one.
    pub fn runs_from(self, resume: Option<Gate>) -> bool {
        resume.is_none_or(|r| self >= r)
    }
}

/// What a run resuming at a gate needs from the stages it skips: the
/// security verdict (its open items go in the pull request) and the
/// security round parked for after the pull request opens.
#[derive(Debug, Clone, PartialEq)]
pub struct Carry {
    pub security: security::Verdict,
    pub security_round: Option<String>,
}

impl Carry {
    pub fn to_json(&self) -> String {
        let (kind, text) = match &self.security {
            security::Verdict::Passed => ("passed", ""),
            security::Verdict::Findings(t) => ("findings", t.as_str()),
            security::Verdict::NotChecked(t) => ("not_checked", t.as_str()),
        };
        serde_json::json!({
            "security": kind,
            "text": text,
            "round": self.security_round,
        })
        .to_string()
    }

    pub fn from_json(s: &str) -> Option<Carry> {
        let v: serde_json::Value = serde_json::from_str(s).ok()?;
        let text = v["text"].as_str().unwrap_or("").to_string();
        let security = match v["security"].as_str()? {
            "passed" => security::Verdict::Passed,
            "findings" => security::Verdict::Findings(text),
            "not_checked" => security::Verdict::NotChecked(text),
            _ => return None,
        };
        Some(Carry {
            security,
            security_round: v["round"].as_str().map(str::to_string),
        })
    }
}

/// What the run's checkpoint says about steps: the gate to resume at (with
/// what it carries), and the progress mark and stalled count of the last
/// step.
#[derive(Debug, Clone, Default)]
pub struct StepState {
    pub gate: Option<Gate>,
    pub carry: Option<Carry>,
    pub mark: Option<String>,
    /// The step that saved `mark`.
    pub mark_step: u32,
    pub stalled: u32,
}

fn key(run_id: &str) -> AttributeValue {
    AttributeValue::S(format!("RUN#{run_id}"))
}

pub async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> StepState {
    if state.config.checkpoints_table_name.is_empty() {
        return StepState::default();
    }
    let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", key(run_id))
        .send()
        .await
    else {
        return StepState::default();
    };
    let Some(item) = out.item() else {
        return StepState::default();
    };
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    StepState {
        gate: s("gate").as_deref().and_then(Gate::parse),
        carry: s("gate_carry").as_deref().and_then(Carry::from_json),
        mark: s("step_mark"),
        mark_step: item
            .get("step_mark_step")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
        stalled: item
            .get("stalled_steps")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
    }
}

/// The run resumes at `gate` in its next step (or after a redelivery).
pub async fn save_gate(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    gate: Gate,
    carry: Option<&Carry>,
) {
    if state.config.checkpoints_table_name.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", key(run_id))
        .update_expression("SET gate = :g, gate_carry = :c")
        .expression_attribute_values(":g", AttributeValue::S(gate.as_str().to_string()))
        .expression_attribute_values(
            ":c",
            AttributeValue::S(carry.map(Carry::to_json).unwrap_or_default()),
        )
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not save the gate the run resumes at");
    }
}

/// This step's progress mark and the stalled count so far.
pub async fn save_mark(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    mark: &str,
    stalled: u32,
    step: u32,
) {
    if state.config.checkpoints_table_name.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", key(run_id))
        .update_expression("SET step_mark = :m, stalled_steps = :n, step_mark_step = :s")
        .expression_attribute_values(":m", AttributeValue::S(mark.to_string()))
        .expression_attribute_values(":s", AttributeValue::N(step.to_string()))
        .expression_attribute_values(":n", AttributeValue::N(stalled.to_string()))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not save the step's progress mark");
    }
}

/// On the run record: step `step` is queued. The stuck-run sweep leaves a
/// run alone while a step is queued and hasn't started (a full queue can
/// hold it past the sweep's cutoff); the run page shows the step.
pub async fn mark_queued(state: &WorkerState, team_id: &str, run_id: &str, step: u32) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression("SET run_step = :s, step_queued_at = :t")
        .expression_attribute_values(":s", AttributeValue::N(step.to_string()))
        .expression_attribute_values(":t", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not record the queued step on the run");
    }
}

// ── Time ─────────────────────────────────────────────────────────────────

/// The "is there time for a fix round" answer the checks get, remembering
/// when it said no: a check that then blocks the pull request blocked it
/// for want of time, and the run continues in a fresh step instead.
pub struct TimeBudget<F: Fn() -> bool> {
    has_time: F,
    denied: AtomicBool,
}

impl<F: Fn() -> bool> TimeBudget<F> {
    pub fn new(has_time: F) -> Self {
        Self {
            has_time,
            denied: AtomicBool::new(false),
        }
    }

    pub fn has_time(&self) -> bool {
        let ok = (self.has_time)();
        if !ok {
            self.denied.store(true, Ordering::SeqCst);
        }
        ok
    }

    /// Whether a round was refused for time since the last call.
    pub fn take_denied(&self) -> bool {
        self.denied.swap(false, Ordering::SeqCst)
    }
}

/// A time budget over this step's start.
pub fn step_budget(start: Instant) -> TimeBudget<impl Fn() -> bool> {
    TimeBudget::new(move || super::has_time_for_remediation(&start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_keeps_a_run_going_past_the_old_bounds() {
        // The old bounds stopped at 3 continuations or 45 minutes. A run
        // whose every step moves its branch keeps going at step 20.
        let mut stalled = 0;
        let mut prev: Option<String> = None;
        for step in 1..=20u32 {
            let mark = progress_mark(&format!("sha{step}"), "implement_started");
            stalled = stalled_after(prev.as_deref(), &mark, stalled);
            assert!(!stops_for_stall(stalled), "step {step} stopped");
            assert!(under_ceiling(step - 1), "ceiling at step {step}");
            prev = Some(mark);
        }
        // A step that only passes a gate (no commit) is progress too.
        let a = progress_mark("same", "generators");
        let b = progress_mark("same", "checks");
        assert_eq!(stalled_after(Some(&a), &b, 1), 0);
    }

    #[test]
    fn two_stalled_steps_stop_with_what_was_stuck() {
        let mark = progress_mark("abcdef1234", "generators");
        let s1 = stalled_after(Some(&mark), &mark, 0);
        assert_eq!(s1, 1);
        assert!(!stops_for_stall(s1), "one stalled step keeps going");
        let s2 = stalled_after(Some(&mark), &mark, s1);
        assert!(stops_for_stall(s2));
        let why = stall_reason(7, "abcdef1234", "generators", "coderhelm-wip/x");
        assert!(why.contains("steps 5 and 6 made no progress"), "{why}");
        assert!(why.contains("abcdef1"), "{why}");
        assert!(why.contains("generator check"), "{why}");
        assert!(why.contains("coderhelm-wip/x"), "{why}");
    }

    #[test]
    fn the_first_step_and_a_missing_mark_are_progress() {
        assert_eq!(stalled_after(None, "a|b", 5), 0);
        assert_eq!(stalled_after(Some(""), "a|b", 5), 0);
    }

    #[test]
    fn the_ceiling_is_a_runaway_guard_far_above_real_work() {
        assert!(under_ceiling(0));
        assert!(under_ceiling(MAX_RUN_STEPS - 2));
        assert!(!under_ceiling(MAX_RUN_STEPS - 1));
        const { assert!(MAX_RUN_STEPS >= 50) };
    }

    #[test]
    fn gates_order_the_resume() {
        assert!(Gate::Generators.runs_from(None));
        assert!(Gate::Checks.runs_from(Some(Gate::Generators)));
        assert!(!Gate::Generators.runs_from(Some(Gate::Checks)));
        assert!(Gate::Verify.runs_from(Some(Gate::Verify)));
        assert!(!Gate::Verify.runs_from(Some(Gate::Generators)));
        for g in [Gate::Verify, Gate::Generators, Gate::Checks] {
            assert_eq!(Gate::parse(g.as_str()), Some(g));
        }
    }

    #[test]
    fn carry_round_trips() {
        for c in [
            Carry {
                security: security::Verdict::Passed,
                security_round: None,
            },
            Carry {
                security: security::Verdict::Findings("### SQLi\nFile: a.rs".into()),
                security_round: Some("fix them".into()),
            },
            Carry {
                security: security::Verdict::NotChecked("api down".into()),
                security_round: Some("recheck".into()),
            },
        ] {
            assert_eq!(Carry::from_json(&c.to_json()), Some(c));
        }
    }

    #[test]
    fn a_denied_round_is_remembered_once() {
        let b = TimeBudget::new(|| false);
        assert!(!b.take_denied());
        assert!(!b.has_time());
        assert!(b.take_denied());
        assert!(!b.take_denied());
        let ok = TimeBudget::new(|| true);
        assert!(ok.has_time());
        assert!(!ok.take_denied());
    }

    /// SQS: visibility 960 s, maxReceiveCount 3 to the DLQ. A run of ten
    /// steps sends ten new messages; each is received once and deleted (the
    /// handler never errors a ticket message back to the queue), so no
    /// message comes near the redrive count however long the run.
    #[test]
    fn a_ten_step_run_never_reuses_a_message() {
        const MAX_RECEIVE_COUNT: u32 = 3;
        struct Queued {
            body: String,
            receives: u32,
        }
        let mut queue: Vec<Queued> = Vec::new();
        let mut seen: Vec<String> = Vec::new();
        let first = TicketMessage {
            team_id: "TEAM#1".into(),
            ticket_id: "GH-1".into(),
            ..Default::default()
        };
        let body = |m: &TicketMessage| {
            serde_json::to_string(&crate::models::WorkerMessage::Ticket(m.clone())).unwrap()
        };
        queue.push(Queued {
            body: body(&first),
            receives: 0,
        });
        let mut steps = 0;
        while let Some(mut q) = queue.pop() {
            q.receives += 1;
            assert!(q.receives < MAX_RECEIVE_COUNT);
            assert!(!seen.contains(&q.body), "a message was re-used");
            seen.push(q.body.clone());
            let msg = match serde_json::from_str(&q.body).unwrap() {
                crate::models::WorkerMessage::Ticket(m) => m,
                _ => unreachable!(),
            };
            steps += 1;
            assert_eq!(step_number(msg.continuation), steps);
            if steps > 1 {
                assert_eq!(msg.continuation_run_id.as_deref(), Some("RUN1"));
            }
            // The step ends: the next step is a new message; this one is
            // deleted (dropped here), never made visible again.
            if steps < 10 && under_ceiling(msg.continuation) {
                let next = next_message(&msg, "RUN1", 1);
                queue.push(Queued {
                    body: body(&next),
                    receives: 0,
                });
            }
        }
        assert_eq!(steps, 10);
        assert_eq!(seen.len(), 10);
    }

    /// Continuations are sent as new messages, never by leaving this one to
    /// time out and come back.
    #[test]
    fn continuations_are_sent_not_redelivered() {
        let src = include_str!("mod.rs");
        let start = src.find("\nasync fn try_auto_continue(").unwrap();
        let body = &src[start..start + src[start..].find("\n}\n").unwrap()];
        assert!(body.contains(".send_message()"));
        assert!(body.contains("steps::next_message("));
        assert!(!body.contains("change_message_visibility"));
    }
}
