//! z. Two facts a run page shows were wrong for every run that opened a
//! pull request. The plan check found 3 of 3 tasks done and saved its
//! checklist. A later fix round wrote 1 file, and the coder's task tracker,
//! built from the plan's unchecked copy, saved its own checklist over the
//! plan check's: the run page read 1 of 3. And the run's `files_modified`
//! (the Changes tab and the Files changed card) was written only by a
//! completion step nothing called, so it was empty for every run with a
//! pull request.
//!
//! Guarantee: a round's coder starts from the checklist as saved and only
//! adds ticks; only the plan check unchecks a task (its file was reverted).
//! `files_modified` is the branch's work against its base (without the plan
//! files CoderHelm commits), written when the pull request opens and again
//! whenever the run records its status after a round, capped in size.

use super::fixture::*;
use crate::passes::implement::{carry_ticks, tick_files};
use crate::passes::plan_coverage::{self, PlanChanges};
use crate::passes::run_files::{self, MAX_FILES};

const A: &str = "apps/web/src/blocks/card.tsx";
const B: &str = "apps/web/src/blocks/card.test.tsx";
const C: &str = "apps/studio/schemaTypes/blocks/card.ts";

fn plan() -> String {
    [
        format!("- [ ] {A}: [create] The card block"),
        format!("- [ ] {B}: [create] Tests for the card block"),
        format!("- [ ] {C}: [create] The card's schema"),
    ]
    .join("\n")
}

fn checked(md: &str) -> usize {
    md.lines()
        .filter(|l| l.trim_start().starts_with("- [x]"))
        .count()
}

fn plan_check(tasks_md: &str, done: &[&str]) -> String {
    let tasks = plan_coverage::plan_tasks(tasks_md);
    let cov = PlanChanges {
        own: done.iter().map(|s| s.to_string()).collect(),
        on_base: vec![],
    }
    .coverage(&tasks);
    plan_coverage::checked_tasks_md(tasks_md, &cov)
}

#[test]
fn a_fix_round_after_the_plan_check_never_unchecks_a_task() {
    let plan = plan();
    // The plan check: 3 of 3.
    let saved = plan_check(&plan, &[A, B, C]);
    assert_eq!(checked(&saved), 3);

    // The old tracker started from the plan's unchecked copy: the fix
    // round's one file saved 1 of 3 over the plan check's 3 of 3.
    let old = tick_files(&plan, &[B]).unwrap();
    assert_eq!(checked(&old), 1, "the incident's checklist");

    // The tracker now starts from the saved checklist.
    let seeded = carry_ticks(&plan, Some(&saved));
    assert_eq!(checked(&seeded), 3);
    // The round's file is already checked: nothing to write.
    assert_eq!(tick_files(&seeded, &[B]), None);

    // A round after a plan check of 2 of 3 adds the third, keeps the two.
    let saved = plan_check(&plan, &[A, C]);
    assert_eq!(checked(&saved), 2);
    let after = tick_files(&carry_ticks(&plan, Some(&saved)), &[B]).unwrap();
    assert_eq!(checked(&after), 3);
}

#[test]
fn only_the_plan_check_unchecks_a_reverted_task() {
    let plan = plan();
    let all = plan_check(&plan, &[A, B, C]);
    // A's change was reverted: the next plan check reads 2 of 3, also when
    // the checklist it starts from (a retry's, read back) is all checked.
    let again = plan_check(&all, &[B, C]);
    assert_eq!(checked(&again), 2);
    assert!(again.contains(&format!("- [ ] {A}")));
    // A line that names no file stays as written.
    let note = format!("{all}\n- [x] Tell the team");
    assert!(plan_check(&note, &[B, C]).contains("- [x] Tell the team"));
}

#[test]
fn a_saved_checklist_from_another_plan_adds_nothing() {
    let other = "- [x] apps/web/src/other.ts: [modify] Something else";
    assert_eq!(carry_ticks(&plan(), Some(other)), plan());
    assert_eq!(carry_ticks(&plan(), None), plan());
    assert_eq!(carry_ticks(&plan(), Some("")), plan());
}

#[test]
fn every_round_seeds_its_tracker_from_the_saved_checklist() {
    let src = include_str!("../implement.rs");
    let run = fn_src(src, "\npub async fn run(");
    assert!(run.contains("read_s3_text(state, &tasks_key)"));
    assert!(run.contains("carry_ticks(&plan.tasks, saved_tasks.as_deref())"));
    let mark = &src[src.find("async fn mark_files_done(").unwrap()..];
    assert!(mark[..600].contains("tick_files(&md, paths)"));
}

#[test]
fn files_modified_is_the_pull_requests_work_when_it_opens() {
    let spec = format!("openspec/specs/{}/tasks.md", TICKET.to_lowercase());
    let opened = vec![(
        format!("{OWNER}/{REPO}"),
        vec![A.to_string(), B.to_string(), C.to_string(), spec.clone()],
    )];
    assert_eq!(
        run_files::files_value(&opened, TICKET),
        vec![A.to_string(), B.to_string(), C.to_string()]
    );

    // A fix round pushed one more file: the next status write reads it.
    let fixed = vec![(
        format!("{OWNER}/{REPO}"),
        vec![
            A.to_string(),
            B.to_string(),
            C.to_string(),
            TYPES.to_string(),
            spec,
        ],
    )];
    assert_eq!(run_files::files_value(&fixed, TICKET).len(), 4);
    assert!(run_files::files_value(&fixed, TICKET).contains(&TYPES.to_string()));
}

#[test]
fn files_modified_names_the_repository_for_several_and_is_capped() {
    let two = vec![
        ("acme/site".to_string(), vec![A.to_string()]),
        ("acme/api".to_string(), vec!["src/card.rs".to_string()]),
    ];
    assert_eq!(
        run_files::files_value(&two, TICKET),
        vec![format!("acme/site/{A}"), "acme/api/src/card.rs".to_string()]
    );
    let many = vec![(
        format!("{OWNER}/{REPO}"),
        (0..MAX_FILES + 100)
            .map(|i| format!("gen/f{i}.ts"))
            .collect(),
    )];
    assert_eq!(run_files::files_value(&many, TICKET).len(), MAX_FILES);
}

#[test]
fn every_status_write_with_a_pull_request_records_its_files() {
    let src = include_str!("../mod.rs");
    assert!(
        !src.contains("async fn complete_run("),
        "the uncalled completion step is gone"
    );
    let rp = fn_src(src, "\nasync fn run_passes(");
    // The CI fix cycle, several repositories, and one repository.
    for marker in [
        "\"Fix implemented on re-run\"",
        "\"Multi-repo run set to awaiting_ci\"",
        "\"Run set to awaiting_ci — Lambda returning, webhook will trigger resume\"",
    ] {
        let at = rp
            .find(marker)
            .unwrap_or_else(|| panic!("{marker} is gone"));
        let window = if marker.contains("Fix implemented") {
            &rp[at..at + 4000]
        } else {
            &rp[at.saturating_sub(1500)..at]
        };
        assert!(window.contains("run_files::record"), "{marker}");
    }
    let resume = include_str!("../resume.rs");
    for head in ["\nasync fn set_run_awaiting_ci(", "\nasync fn end_run("] {
        assert!(
            fn_src(resume, head).contains("run_files::record_with_app(state, msg, run_id, branch)"),
            "{head}"
        );
    }
}
