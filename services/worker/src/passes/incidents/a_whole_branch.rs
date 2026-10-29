//! a. A run built most of its plan over several steps; the plan check said
//! 5 of 7 tasks done; its last step found nothing left to write and the run
//! ended "I ran but produced no file changes" with generic questions,
//! although the branch held all the work.
//!
//! Guarantee: a step that writes nothing is judged on the whole branch (its
//! diff against the base, every step summed). With work on the branch the
//! run goes on to the checks and the pull request.

use super::fixture::*;
use crate::passes::plan_coverage;
use crate::passes::run_end::{self, Empty};

const OLD: &str = "I ran but produced no file changes";

/// The pipeline's ending for a step whose implement wrote nothing, as
/// `run_passes` reads it: the branch's work files, the plan's tasks, the
/// coder's own questions, whether the ticket names a file.
fn last_step_writes_nothing(gh: &FakeGitHub, coder_notes: &str, ticket: &str) -> Ending {
    let mut end = Ending::default();
    let tasks = plan_coverage::plan_tasks(&plan_tasks_md());
    let changed = gh.changed(BRANCH);
    let cov = plan_coverage::coverage(&tasks, &changed);
    end.said(plan_coverage::progress_note(&cov));
    let work = run_end::work_files(&changed, TICKET);
    match run_end::judge(
        Some(&work),
        &tasks,
        &run_end::coder_questions(coder_notes),
        run_end::ticket_names_files(ticket),
    ) {
        Empty::Carry(files) => {
            end.said(format!(
                "This step wrote no new file; the branch holds the run's work on {} file(s): checking it against the plan",
                files.len()
            ));
            // The run goes on: the plan check, the generator check, the
            // proof check and the pull request.
            end.status = "checks".into();
        }
        Empty::Ask(qs) => {
            end.said(qs.join("\n"));
            end.status = common::run_status::NEEDS_INPUT.into();
        }
        Empty::Unbuilt(reason) => {
            end.said(reason);
            end.status = common::run_status::FAILED.into();
        }
        Empty::Generic => {
            end.said(format!("{OLD} for this issue."));
            end.status = common::run_status::NEEDS_INPUT.into();
        }
    }
    end
}

#[test]
fn a_last_step_that_writes_nothing_is_judged_on_the_whole_branch() {
    let mut gh = FakeGitHub::new();
    // Steps 1 to 3 built five of the planned files; the plan files sit
    // beside them.
    gh.commit(
        "step-1",
        BRANCH,
        &[
            (
                "apps/studio/schemaTypes/blocks/card.ts",
                "export const card = {}",
            ),
            (
                "apps/studio/schemaTypes/index.ts",
                "export * from './blocks/card'",
            ),
            ("openspec/specs/gh-41/tasks.md", "- [x] ..."),
        ],
    );
    gh.commit(
        "step-2",
        BRANCH,
        &[
            ("apps/web/src/blocks/card.tsx", "export function Card() {}"),
            ("apps/web/src/blocks/index.ts", "card: Card"),
        ],
    );
    gh.commit(
        "step-3",
        BRANCH,
        &[("apps/web/src/lib/sanity/queries.ts", "card { title }")],
    );
    let end = last_step_writes_nothing(
        &gh,
        "Everything in the plan is on the branch.",
        "Add a card block",
    );
    assert!(
        end.says("Plan check: 5 of 7 planned tasks done"),
        "{:?}",
        end.words
    );
    assert_eq!(end.status, "checks", "{:?}", end.words);
    assert!(end.says("the branch holds the run's work on 5 file(s)"));
    end.never_says(OLD);
    end.never_says("Which files or components should change?");
}

#[test]
fn a_branch_with_only_the_plan_files_is_unbuilt_work_named_not_a_generic_question() {
    let mut gh = FakeGitHub::new();
    gh.commit(
        "step-1",
        BRANCH,
        &[("openspec/specs/gh-41/tasks.md", "- [ ] ...")],
    );
    let end = last_step_writes_nothing(&gh, "", "Add a card block");
    assert_eq!(end.status, common::run_status::FAILED);
    assert!(end.says("apps/web/src/blocks/card.tsx"), "{:?}", end.words);
    assert!(end.says("That is CoderHelm's miss"));
    end.never_says(OLD);
}

#[test]
fn the_pipeline_reads_the_branch_before_it_judges_an_empty_step() {
    let rp = run_passes_src();
    let judge = rp.find("let mut verdict = run_end::judge(").unwrap();
    let work = rp.find("run_end::work_files(&c, &msg.ticket_id)").unwrap();
    assert!(
        work < judge,
        "the branch's work is read before the judgment"
    );
    let changed = &rp[..work];
    assert!(changed.rfind("plan_coverage::changed_files(").is_some());
    // The generic ending is only for a verdict that found nothing anywhere.
    assert!(rp.contains("run_end::Empty::Generic => {}"));
}
