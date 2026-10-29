//! g. A mechanical ticket needed 40 files. The planner was capped, the plan
//! was narrowed to fewer files with a "split the ticket" note, and the run
//! built only part of what the ticket asked.
//!
//! Guarantee: a plan holds every file the ticket needs, however many; no
//! check caps, trims or narrows it by count, and every later round is told
//! every task.

use super::fixture::*;
use crate::passes::plan_coverage;

fn forty() -> String {
    (0..40)
        .map(|i| format!("- [ ] apps/web/src/blocks/block_{i:02}.tsx: [modify] Read the new `variant` field\n"))
        .collect()
}

#[test]
fn a_forty_file_plan_is_kept_whole_through_every_check() {
    let md = forty();
    assert_eq!(super::super::plan_task_files(&md).len(), 40);
    let tasks = plan_coverage::plan_tasks(&md);
    assert_eq!(tasks.len(), 40);
    // The first step built 39 of them: the plan check names the last one.
    let mut gh = FakeGitHub::new();
    let built: Vec<(String, &str)> = tasks[..39].iter().map(|t| (t.path.clone(), "x")).collect();
    let refs: Vec<(&str, &str)> = built.iter().map(|(p, c)| (p.as_str(), *c)).collect();
    gh.commit("step-1", BRANCH, &refs);
    let cov = plan_coverage::coverage(&tasks, &gh.changed(BRANCH));
    let mut end = Ending::default();
    end.said(plan_coverage::progress_note(&cov));
    assert!(end.says("Plan check: 39 of 40 planned tasks done"));
    let feedback = plan_coverage::round_feedback(&cov);
    assert!(feedback.contains("block_39.tsx"), "{feedback}");
    // A build round told every task lists all 40.
    let all = crate::passes::run_end::build_round_feedback(&tasks);
    assert_eq!(all.matches("- [ ] `apps/web/src/blocks/block_").count(), 40);
    // A planned-work round on the pull request lists every task left.
    let items: Vec<String> = tasks
        .iter()
        .map(|t| format!("`{}` is not changed.", t.path))
        .collect();
    let round = crate::passes::finish_planned::round_feedback(&items);
    assert_eq!(
        round.matches("- [ ] `apps/web/src/blocks/block_").count(),
        40
    );
    for old in [
        "smaller tickets",
        "Plan scope too large",
        "split the ticket",
    ] {
        end.never_says(old);
        assert!(!feedback.contains(old) && !all.contains(old));
    }
}

#[test]
fn nothing_in_the_pipeline_caps_a_plan_by_count() {
    let src = include_str!("../mod.rs");
    for gone in [
        ["max_plan", "_files"].concat(),
        ["narrow_scope", "_note"].concat(),
        ["Plan scope", " too large"].concat(),
    ] {
        assert!(!src.contains(&gone), "{gone} is back");
    }
    assert!(!include_str!("../plan.rs").contains(&["tasks", " maximum"].concat()));
}
