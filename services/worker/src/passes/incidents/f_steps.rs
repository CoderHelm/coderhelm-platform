//! f. A new-feature run triaged, planned, built eight files and reached the
//! generator check with 30 seconds of its 15-minute step left. The check
//! declined to start a build and the run failed "not enough time left",
//! with the work done. Other runs stopped at a fixed step count while still
//! making progress.
//!
//! Guarantee: a gate that lacks time (the generator check, the checks, the
//! pull request) continues the run in a fresh step at that gate, as a new
//! queue message; a run whose steps make progress keeps going (20 steps and
//! more); two steps in a row without progress stop it, saying where.

use super::fixture::*;
use crate::clients::sandbox::CodegenOutcome;
use crate::models::TicketMessage;
use crate::passes::generators::{self, GateStop, Pass, Read};
use crate::passes::steps::{self, Gate};

const OLD: &str = "not enough time left";

#[test]
fn a_generator_build_without_time_needs_a_step_not_a_failure() {
    // The sandbox declined the build at the step's deadline.
    let o = CodegenOutcome {
        ran: false,
        passed: false,
        changed: Vec::new(),
        dropped: Vec::new(),
        output: "codegen not run: not enough time left in this step for a sandbox build".into(),
    };
    let pass = match generators::read_outcome(o, "pnpm run typegen", || false) {
        Read::OutOfTime => Pass::OutOfTime,
        other => panic!("{other:?}"),
    };
    assert!(matches!(
        generators::stop_of(pass),
        Err(GateStop::NeedsTime)
    ));
}

/// One run's steps, as the queue and the run record carry them: each step
/// is a new message; each starts by comparing its progress mark with the
/// last step's. `heads[i]` is the branch head step `i + 1` starts at.
fn run_steps(heads: &[&str], at: Gate) -> (Ending, Vec<TicketMessage>) {
    let mut end = Ending::default();
    let first = TicketMessage {
        team_id: "TEAM#1".into(),
        ticket_id: TICKET.into(),
        ..Default::default()
    };
    let mut sent = vec![first.clone()];
    let mut msg = first;
    let mut mark: Option<String> = None;
    let mut stalled = 0;
    for head in heads {
        let step = steps::step_number(msg.continuation);
        let now = steps::progress_mark(head, at.as_str());
        if msg.continuation > 0 {
            stalled = steps::stalled_after(mark.as_deref(), &now, stalled);
            if steps::stops_for_stall(stalled) {
                end.said(steps::stall_reason(step, head, at.as_str(), BRANCH));
                end.status = common::run_status::FAILED.into();
                return (end, sent);
            }
        }
        mark = Some(now);
        // The step runs out of time at the gate: the next step is queued.
        assert!(steps::under_ceiling(msg.continuation), "step {step}");
        msg = steps::next_message(&msg, "RUN1", 1);
        sent.push(msg.clone());
        end.said(format!(
            "The checks need more time than this step has left: continuing in step {} with its own time budget",
            steps::step_number(msg.continuation)
        ));
    }
    end.status = common::run_status::RUNNING.into();
    (end, sent)
}

#[test]
fn a_run_making_progress_continues_for_twenty_steps_each_a_new_message() {
    let heads: Vec<String> = (0..20).map(|i| format!("{i:07x}aa")).collect();
    let heads: Vec<&str> = heads.iter().map(String::as_str).collect();
    let (end, sent) = run_steps(&heads, Gate::Checks);
    assert_eq!(end.status, common::run_status::RUNNING, "{:?}", end.words);
    assert_eq!(sent.len(), 21);
    // Every step is its own message (its own receive count), one run.
    for (i, m) in sent.iter().enumerate() {
        assert_eq!(m.continuation as usize, i);
        if i > 0 {
            assert_eq!(m.continuation_run_id.as_deref(), Some("RUN1"));
        }
    }
    let bodies: std::collections::HashSet<String> = sent
        .iter()
        .map(|m| serde_json::to_string(m).unwrap())
        .collect();
    assert_eq!(bodies.len(), sent.len(), "no message is sent twice");
    end.never_says(OLD);
}

#[test]
fn two_steps_without_progress_stop_the_run_saying_where() {
    let (end, _) = run_steps(
        &["aaa1111", "bbb2222", "bbb2222", "bbb2222"],
        Gate::Generators,
    );
    assert_eq!(end.status, common::run_status::FAILED);
    assert!(end.says("made no progress"), "{:?}", end.words);
    assert!(end.says("the generator check"));
    assert!(end.says(&format!("kept on branch `{BRANCH}`")));
    end.never_says(OLD);
}

#[test]
fn every_gate_that_lacks_time_hands_off_at_that_gate() {
    let rp = run_passes_src();
    let needs = rp
        .find("Err(generators::GateStop::NeedsTime) => {")
        .unwrap();
    let after = &rp[needs..needs + 600];
    assert!(after.contains("hand_off_at("));
    assert!(after.contains("steps::Gate::Generators"));
    let pr = rp
        .find("opening the pull request needs more time than this step has left")
        .unwrap();
    assert!(rp[pr - 400..pr].contains("steps::Gate::Checks"));
    assert!(!rp.contains(OLD));
    // A hand-off is a new message on the queue, never a retry of this one.
    let src = include_str!("../mod.rs");
    let hand_off = fn_src(src, "\nasync fn hand_off_at(");
    assert!(hand_off.contains("try_auto_continue("));
    let cont = fn_src(src, "\nasync fn try_auto_continue(");
    assert!(cont.contains("steps::next_message("));
    assert!(cont.contains(".send_message()"));
}
