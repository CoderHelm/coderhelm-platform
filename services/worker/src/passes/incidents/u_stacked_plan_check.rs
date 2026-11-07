//! u. A ticket whose earlier part was still an open pull request ran on
//! that pull request's branch. Its plan had 38 tasks. In step 2, measured
//! against the default branch, 37 were done. From step 3 on, every step
//! started again from the default branch, read the branch (already built on
//! the other pull request) as copying its commits, rebuilt it on that branch
//! and force-pushed it, and measured the plan against that branch instead:
//! the 13 files the other pull request already changed into their planned
//! form dropped out of the diff. The plan check read 23 of 38, the coder
//! found those files already done and wrote nothing, and after seven
//! continuations the guard stopped the run: "it continued 7 times without
//! finishing another task".
//!
//! Guarantee: a later step of a run keeps the base an earlier step stacked
//! it on (no rebuild, no force push, one base for the plan check); a planned
//! file the stacked-on branch already changes is done there and the note
//! says so; and a fall in the done count is reported and, a bounded number
//! of times, its recovery counts as progress.

use super::fixture::*;
use crate::passes::plan_coverage::{self, Next, PlanChanges};
use crate::passes::run_guard::{GuardState, Progress, Trip, MAX_TASK_DROPS};
use crate::passes::stack;

const DEP: &str = "coderhelm/gh-40-title-size";

fn plan(n: usize) -> String {
    (0..n)
        .map(|i| format!("- [ ] apps/web/src/blocks/b{i}.tsx: [modify] Read the title size"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn files(range: std::ops::Range<usize>) -> Vec<String> {
    range
        .map(|i| format!("apps/web/src/blocks/b{i}.tsx"))
        .collect()
}

#[test]
fn files_the_stacked_on_branch_already_changed_are_done_there() {
    let tasks = plan_coverage::plan_tasks(&plan(38));
    // The branch's own diff against the stacked-on branch: 24 files (one
    // planned task is genuinely left). The other branch changes 13 more.
    let own = files(0..24);
    let changes = PlanChanges {
        own: own.clone(),
        on_base: files(24..37),
    };
    let cov = changes.coverage(&tasks);
    assert_eq!(cov.done.len(), 37);
    assert_eq!(cov.on_base.len(), 13);
    assert_eq!(cov.required_missing().len(), 1);
    let note = plan_coverage::progress_note(&cov);
    assert!(note.contains("37 of 38"));
    assert!(note.contains("b30.tsx (changed by the branch this one is stacked on)"));
    assert!(!note.contains("b3.tsx (changed by"));
    // The round is told only the one task left, not the 13.
    let told = plan_coverage::round_feedback(&cov);
    assert!(told.contains("b37.tsx"));
    assert!(!told.contains("- [ ] `apps/web/src/blocks/b30.tsx`"));

    // The old check, the branch's own diff alone, read 23 of 38 forever.
    let old = plan_coverage::coverage(&tasks, &own);
    assert_eq!(old.required_missing().len(), 14);

    // With the last file written, the plan is complete.
    let all = PlanChanges {
        own: files(0..24).into_iter().chain(files(37..38)).collect(),
        on_base: files(24..37),
    };
    assert_eq!(
        plan_coverage::next(&all.coverage(&tasks), 0, true),
        Next::Complete
    );
}

#[test]
fn only_a_stacked_base_adds_its_changes() {
    assert!(plan_coverage::stacked_base(DEP, BASE));
    assert!(!plan_coverage::stacked_base(BASE, BASE));
    assert!(!plan_coverage::stacked_base(DEP, ""));
}

#[test]
fn a_later_step_keeps_the_base_an_earlier_step_stacked_it_on() {
    assert!(stack::keeps_base(true, DEP, BASE));
    // A new run, or a message that names the default branch, starts there.
    assert!(!stack::keeps_base(false, DEP, BASE));
    assert!(!stack::keeps_base(true, BASE, BASE));
    assert!(!stack::keeps_base(true, "", BASE));

    // The pipeline asks before it sets the default branch for good, and a
    // kept base is what the run stacks on when the ticket names none.
    let src = include_str!("../mod.rs");
    let rp = fn_src(src, "\nasync fn run_passes(");
    let reset = rp
        .find("std::mem::replace(&mut msg.base_branch, default_branch)")
        .expect("the default branch replaces the incoming base, which is kept");
    assert!(rp[reset..reset + 600].contains("stack::keeps_base(msg.same_run()"));
    let stacked = rp.find("stack::for_ticket(&github, msg, &own)").unwrap();
    assert!(rp[stacked..stacked + 300].contains("kept_stack"));
    assert!(!rp.contains("msg.base_branch = default_branch;"));
}

#[test]
fn every_plan_check_reads_the_stacked_base() {
    let pc = include_str!("../plan_coverage.rs");
    let gaps = fn_src(pc, "\npub async fn close_gaps(");
    assert!(gaps.contains("plan_changes("));
    assert!(!gaps.contains("coverage(&tasks, &changed)"));
    let cov = include_str!("../covering.rs");
    assert!(cov.contains("plan_coverage::plan_changes("));
}

#[test]
fn a_fall_in_the_count_is_news_and_its_recovery_is_progress() {
    // The incident's counts: 1, 37, then 23 (the base moved), 17, 23, ...
    let mut g = GuardState::default();
    assert!(g.apply(&Progress::TasksDone(1)));
    assert!(g.apply(&Progress::TasksDone(37)));
    g.on_continuation(1.0);
    assert!(!g.apply(&Progress::TasksDone(23)), "a fall is not progress");
    assert_eq!(g.task_drops, 1);
    g.on_continuation(1.0);
    assert!(!g.apply(&Progress::TasksDone(17)));
    assert_eq!(g.task_drops, 2);
    g.on_continuation(1.0);
    assert!(
        g.apply(&Progress::TasksDone(23)),
        "lost work done again is progress"
    );
    assert_eq!(g.continuations, 0);
    assert_eq!(g.tasks_done, 37, "the best stays the best");

    // A count that keeps falling and rising still ends: past the bound,
    // only a new best counts.
    for _ in 0..4 {
        g.on_continuation(1.0);
        g.apply(&Progress::TasksDone(17));
        g.on_continuation(1.0);
        g.apply(&Progress::TasksDone(23));
    }
    assert!(g.task_drops > MAX_TASK_DROPS);
    assert_eq!(g.trip(), Some(Trip::Continuations));
}

#[test]
fn a_record_from_before_counts_from_its_best() {
    // Guard state saved before this change has no last count.
    let g: GuardState = serde_json::from_str(r#"{"tasks_done":37}"#).unwrap();
    let mut g = g;
    assert!(!g.apply(&Progress::TasksDone(37)));
    assert!(!g.apply(&Progress::TasksDone(23)));
    assert_eq!(g.task_drops, 1);
}

#[test]
fn the_guard_reports_a_fall_on_the_run_page() {
    let src = include_str!("../run_guard.rs");
    let p = fn_src(src, "\npub async fn progress(");
    assert!(p.contains("add_progress_note("));
    assert!(p.contains("fewer tasks done than before"));
}
