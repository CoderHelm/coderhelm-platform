//! r. A fix round after red sandbox checks hit the installation's hourly
//! GitHub limit around 20:05 UTC: fifteen `batch_write` and fourteen
//! `edit_file` calls came back "403 ... API rate limit exceeded", the diff
//! and the plan check's compare failed the same way, and the run ended
//! "Orchestration failed": "Stopped without a pull request: the coder wrote
//! no file (and GitHub couldn't show the branch's diff) ... That is
//! CoderHelm's miss".
//!
//! Guarantee: GitHub refusing for a while never ends a run and is never
//! blamed on the coder. The first refused call tells the model the run
//! pauses (not the raw 403), the pass ends, and the run is sent again as
//! itself at GitHub's reset, in hops of at most one SQS delay. Its page
//! says when it goes on. The wait adds no step and no pass, and the run
//! goes on from its checkpoint, plan and branch.

use super::fixture::*;
use crate::clients::github::{self, GitHubPaused};
use crate::models::TicketMessage;
use crate::passes::plan_coverage;
use crate::passes::run_end::{self, Empty, Writes};
use common::github_wait::{self as gw, Cause};

const OLD: &str = "That is CoderHelm's miss";
const RUN: &str = "01RUNLIMIT";

/// The reset the 403s named: 2026-11-06 21:05:00 UTC.
const RESET: u64 = 1_793_999_100;
/// When the round hit the limit: 20:05:00.
const HIT: u64 = RESET - 3_600;

fn refused_write() -> Box<dyn std::error::Error + Send + Sync> {
    "HTTP status client error (403 Forbidden) for url \
     (https://api.github.com/repos/acme/site/git/trees): API rate limit exceeded for \
     installation ID 41."
        .into()
}

#[test]
fn a_rate_limited_round_waits_for_the_reset_and_goes_on_as_itself() {
    let mut end = Ending {
        status: "running".into(),
        ..Default::default()
    };
    // The model's view of the first refused write: the pause, never the 403.
    let (cause, _) = github::outside_wait(&*refused_write()).expect("GitHub's limit");
    assert_eq!(cause, Cause::RateLimit);
    let told = gw::tool_result(cause, RESET);
    assert!(told.contains("Do not retry"));
    assert!(!told.contains("403"));

    // Had the pass gone on to the empty-step check anyway: 29 refused
    // writes and an unreadable diff are a wait, not "no file".
    let mut writes = Writes::default();
    for _ in 0..29 {
        writes.refuse(Cause::RateLimit, RESET);
    }
    let tasks = plan_coverage::plan_tasks(&plan_tasks_md());
    let verdict = run_end::judge(None, &tasks, &[], true, &writes, None);
    assert_eq!(verdict, Empty::Wait(Cause::RateLimit, RESET));

    // The run waits: the same run, the same step, one wait, the pass it
    // was in.
    let first = TicketMessage {
        team_id: "TEAM#1".into(),
        ticket_id: TICKET.into(),
        continuation: 2,
        continuation_run_id: Some(RUN.into()),
        ..Default::default()
    };
    let paused = GitHubPaused {
        cause,
        until: RESET,
    };
    let (next, hop) =
        crate::passes::github_wait_message(&first, RUN, &paused, "implement", HIT).unwrap();
    assert_eq!(next.continuation, 2, "a wait is not a step");
    assert_eq!(next.model_retries, 0, "nor a model retry");
    assert_eq!(next.continuation_run_id.as_deref(), Some(RUN));
    assert_eq!(next.github_wait.waits, 1);
    assert_eq!(next.github_wait.pass, "implement");
    assert!(next.same_run());
    end.said(gw::waiting_text(cause, RESET));

    // Sent on in hops of at most one SQS delay until the reset.
    let mut t = HIT + hop;
    let mut hops = 1;
    while let Some(h) = gw::hop(next.github_wait.until, t) {
        assert!(h <= gw::MAX_HOP_SECS);
        t += h;
        hops += 1;
    }
    assert_eq!(hops, 4, "an hour is four hops");
    assert!(t >= RESET - gw::RESET_SLACK_SECS);

    // It goes on as itself: the entry guard knows its run, and entering the
    // pass it was in is not a new pass.
    let as_worker = common::messages::WorkerMessage::Ticket(next.clone());
    assert_eq!(as_worker.run_ref(), Some(("TEAM#1", RUN)));
    crate::passes::run_guard::forgive_reentry(RUN, "implement");
    assert!(crate::passes::run_guard::take_reentry(RUN, "implement"));
    assert!(
        !crate::passes::run_guard::take_reentry(RUN, "implement"),
        "only once"
    );

    assert!(
        end.says("Waiting for GitHub's hourly limit to reset at 21:05 UTC; continues by itself")
    );
    end.never_says(OLD);
    end.never_says("the coder wrote no file");
    end.never_says("Orchestration failed");
    assert_eq!(end.status, "running");
}

/// The pipeline's own places: the tool loop ends on a refusal, the rounds
/// that swallow errors pass the pause on, the empty-step check waits, and a
/// run's error is a wait before it is a failure.
#[test]
fn the_pipeline_pauses_at_each_place_github_can_refuse() {
    let anthropic = include_str!("../../agent/anthropic.rs");
    let lp = fn_src(anthropic, "pub async fn converse_tool_loop(");
    assert!(lp.contains("crate::clients::github::outside_wait(&*e)"));
    assert!(lp.contains("return Err(Box::new(p));"));

    let implement = include_str!("../implement.rs");
    assert_eq!(
        implement
            .matches("if crate::clients::github::outside_wait(&*e).is_some() {")
            .count(),
        3,
        "the remediation and keep-going rounds don't swallow a pause"
    );

    let rp = run_passes_src();
    let judge = rp.find("let mut verdict = run_end::judge(").unwrap();
    assert!(rp[..judge].contains("let outside = github.outside()"));
    assert!(rp.contains("run_end::Empty::Wait(cause, until) => {"));

    let src = include_str!("../mod.rs");
    let orch = &src[src.find("\npub async fn orchestrate_ticket(").unwrap()..];
    let orch = &orch[..orch.find("\nfn run_origin_agent(").unwrap()];
    let pause = orch
        .find("github_pause_of(&*e, msg.installation_id)")
        .unwrap();
    let wait = orch
        .find("wait_for_github(state, &msg, &run_id, p, &mut usage)")
        .unwrap();
    let fail = orch
        .find("fail_run(state, &msg, &run_id, &sanitized")
        .unwrap();
    assert!(
        pause < wait && wait < fail,
        "a wait comes before any failure"
    );
    // The wait is no step: it never goes through the continuation.
    let w = fn_src(src, "async fn wait_for_github(");
    assert!(!w.contains("try_auto_continue("));
    assert!(!w.contains("next_message("));
    assert!(w.contains("steps::mark_queued("));
    assert!(w.contains("steps::step_number(msg.continuation)"));
}
