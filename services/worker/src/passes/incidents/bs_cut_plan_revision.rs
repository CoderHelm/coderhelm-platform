//! bs. A ticket with a long history (earlier runs, pull requests closed
//! unmerged, review comments) was planned. The plan check left two items
//! open (a review comment on an earlier pull request the plan never
//! answered, and an outside-steps note) and sent the plan back. The
//! revision reached the output limit partway through its last block, before
//! any tool call. The loop told the planner "make a change in small
//! edit_file calls", the coder's note; the planner has no such tool and
//! wrote only its decision block again ("the four files above are
//! unchanged"). The worker read only that last turn: no tasks, no design.
//! The run failed "Plan generation produced an empty or malformed plan ...
//! retry the run", throwing the first plan away.
//!
//! Guarantee: a planner's cut answer is continued where it stopped and read
//! with what came before the cut, file by file; the planner is told to
//! continue its document, never the coder's edit_file note; a revision that
//! comes back empty, cut off or malformed never replaces a valid file; files
//! still missing are asked for alone; a plan still unusable is planned again
//! in a fresh step and only then reported, as CoderHelm's own failure, and
//! no run ever tells a person to retry it.

use super::fixture::*;
use crate::agent::anthropic::replay_cut_turns;
use crate::agent::llm::{AnswerForm, ToolDefinition};
use crate::passes::plan_sections::{self, State};

const OLD: &str = "retry the run";

fn tools(names: &[&str]) -> Vec<ToolDefinition> {
    names
        .iter()
        .map(|n| ToolDefinition {
            name: n.to_string(),
            description: String::new(),
            input_schema: serde_json::json!({}),
        })
        .collect()
}

/// The first plan, written in full: the plan the check sent back.
fn first_plan() -> String {
    format!(
        "```proposal.md\nAdd the card block to the page builder.\n```\n\
         ```design.md\napps/web/src/blocks/Card.tsx renders the block; the schema adds it.\n```\n\
         ```tasks.md\n{}\n```\n\
         ```spec.md\nGiven a page with a card block, when it renders, then the card shows.\n```\n\
         ```fix_location.json\n{{\"fix_location\":\"code\",\"cause\":\"New block\"}}\n```",
        plan_tasks_md()
    )
}

/// The revision's two turns, as the incident had them: the four files and
/// the start of the decision, cut at the output limit; then only the
/// decision written again from its start.
fn revision_turns() -> (String, String) {
    let cut = format!(
        "Revised for the two open items.\n\n\
         ```proposal.md\nAdd the card block; the earlier review's metadata case is handled.\n```\n\
         ```design.md\nCard.tsx renders the block; metadata keeps the earlier special case.\n```\n\
         ```tasks.md\n{}\n- [ ] apps/web/src/lib/metadata.ts: [modify] Keep the special case\n```\n\
         ```spec.md\nGiven a card page, when its metadata renders, then the special case holds.\n```\n\
         ```fix_location.json\n{{\"fix_location\":\"code\",\"cause\":\"New wo",
        plan_tasks_md()
    );
    let next = "My last answer was cut off partway through the `fix_location.json` block, so \
                here it is again in full. The four openspec files in that answer were complete \
                and are unchanged.\n\n```fix_location.json\n{\"fix_location\":\"code\",\"cause\":\"New block\"}\n```"
        .to_string();
    (cut, next)
}

fn plan_of(text: &str) -> crate::passes::plan::PlanResult {
    let a = plan_sections::read(text);
    crate::passes::plan::PlanResult {
        proposal: a.complete("proposal.md").into(),
        design: a.complete("design.md").into(),
        tasks: a.complete("tasks.md").into(),
        spec: a.complete("spec.md").into(),
        repo_tasks: vec![],
        fix: None,
    }
}

#[test]
fn a_cut_revision_keeps_its_files_and_the_planner_reads_the_planner_note() {
    let planner = tools(&["read_file_lines", "search_code", "query_content"]);
    let (cut, next) = revision_turns();

    // Before: the loop read only the last turn, and the planner read the
    // coder's note. That last turn holds no plan.
    let before = plan_of(&next);
    assert!(plan_sections::unusable(&before).is_some());
    let (notes, _) = replay_cut_turns(
        AnswerForm::Auto,
        &tools(&["read_file", "edit_file"]),
        &[(&cut, "max_tokens"), (&next, "end_turn")],
    );
    assert!(
        notes[0].contains("small edit_file calls"),
        "the coder still gets its note"
    );

    // After: the planner's loop continues the document and returns the cut
    // text with its continuation.
    let (notes, answer) = replay_cut_turns(
        AnswerForm::Document,
        &planner,
        &[(&cut, "max_tokens"), (&next, "end_turn")],
    );
    assert_eq!(notes.len(), 1);
    assert!(!notes[0].contains("edit_file"), "{}", notes[0]);
    assert!(notes[0].contains("Continue it exactly where it stopped"));
    let a = plan_sections::read(&answer);
    for f in plan_sections::FILES {
        assert_eq!(a.state(f), State::Complete, "{f}");
    }
    assert_eq!(a.state(plan_sections::DECISION), State::Complete);
    let revised = plan_of(&answer);
    assert!(plan_sections::unusable(&revised).is_none());
    assert!(
        revised.tasks.contains("metadata.ts"),
        "the revision's own tasks"
    );

    // A planner that only thought to the limit is told to write, not edit.
    let (notes, _) = replay_cut_turns(
        AnswerForm::Document,
        &planner,
        &[("", "max_tokens"), (&first_plan(), "end_turn")],
    );
    assert!(!notes[0].contains("edit_file"));
    assert!(notes[0].contains("write the document"));
    // Any other loop without edit tools is told to answer, not edit.
    let (notes, _) = replay_cut_turns(
        AnswerForm::Auto,
        &planner,
        &[("", "max_tokens"), ("done", "end_turn")],
    );
    assert!(!notes[0].contains("edit_file"));
}

#[test]
fn a_revision_that_comes_back_empty_never_replaces_the_valid_plan() {
    let valid = plan_of(&first_plan());
    assert!(plan_sections::unusable(&valid).is_none());
    // The revision as the old loop returned it: the decision alone.
    let (_, next) = revision_turns();
    let answer = plan_sections::read(&next);
    let (kept, lost) = plan_sections::merge(valid.clone(), plan_of(&next), &answer);
    assert!(lost.is_empty());
    assert_eq!(kept.tasks, valid.tasks);
    assert_eq!(kept.design, valid.design);
    assert!(plan_sections::unusable(&kept).is_none(), "the run goes on");

    // A revision cut inside its tasks: the earlier tasks stand, named lost.
    let cut = "```design.md\nD2\n```\n```tasks.md\n- [ ] apps/web/src/blocks/Card.tsx: [mod";
    let answer = plan_sections::read(cut);
    let (kept, lost) = plan_sections::merge(valid.clone(), plan_of(cut), &answer);
    assert_eq!(lost, vec!["tasks.md"]);
    assert_eq!(kept.tasks, valid.tasks);
    assert_eq!(
        kept.design, "D2",
        "a file written in full is the revision's"
    );

    let mut end = Ending {
        status: "running".into(),
        ..Default::default()
    };
    end.said("Plan check: 2 left open: [history_used] ... [outside_steps] ...");
    end.never_says(OLD);
    end.never_says("empty or malformed plan");
}

#[test]
fn a_plan_with_no_valid_first_round_is_repaired_file_by_file() {
    // No valid plan to fall back on: the files missing are asked for alone.
    let cut = "```proposal.md\nP\n```\n```design.md\nD cut";
    let p = plan_of(cut);
    assert_eq!(
        plan_sections::to_repair(&p),
        vec!["design.md", "tasks.md", "spec.md"]
    );
    let ask =
        plan_sections::repair_prompt(&plan_sections::to_repair(&p), &plan_sections::read(cut));
    assert!(ask.contains("`design.md` was cut off"));
    assert!(ask.contains("`tasks.md` is missing"));
    assert!(!ask.contains("edit_file"));
    // The repair's answer fills them in; the proposal stays.
    let reply = format!(
        "```design.md\nD\n```\n```tasks.md\n{}\n```\n```spec.md\nS\n```",
        plan_tasks_md()
    );
    let (out, _) = plan_sections::merge(p, plan_of(&reply), &plan_sections::read(&reply));
    assert!(plan_sections::unusable(&out).is_none());
    assert_eq!(out.proposal, "P");
}

#[test]
fn a_plan_still_unusable_plans_again_in_a_fresh_step_then_says_why() {
    let mut ps = crate::passes::steps::PlanState::new(OWNER, REPO, BASE);
    assert!(ps.may_plan_again());
    let before = ps.mark();
    ps.plan_retries += 1;
    assert_ne!(ps.mark(), before, "planning again counts as progress");
    assert!(!ps.may_plan_again(), "bounded");
    let why = plan_sections::unusable(&plan_of("")).unwrap();
    let failure = crate::passes::plan_failure(&why, None);
    assert!(failure.contains("the plan has no usable `proposal.md`, `design.md`, `tasks.md`"));
    assert!(failure.contains("CoderHelm's own failure"));
    assert!(!failure.to_lowercase().contains(OLD));

    // The pipeline's own places: the plan pass writes through the document
    // loop, revisions are merged, the repair runs, and the guard plans
    // again in a fresh step instead of failing with a retry instruction.
    let plan = include_str!("../plan.rs");
    let run = fn_src(plan, "pub async fn run(");
    assert!(
        !run.contains("provider::converse("),
        "the planner's calls are document loops"
    );
    assert!(run.matches("provider::converse_document(").count() >= 5);
    assert!(run.contains("super::plan_sections::repair_prompt("));
    assert!(run.matches("revise(").count() >= 4);
    let rp = run_passes_src();
    assert!(rp.contains("plan_sections::unusable(&plan_result)"));
    assert!(rp.contains("plan_again_or_stop("));
    for src in [plan, include_str!("../mod.rs")] {
        assert!(!src.contains("Nothing was asked of a person; retry the run"));
        assert!(!src.contains("transient model formatting issue"));
    }
    for src in [
        include_str!("../fix_location.rs"),
        include_str!("../resolved.rs"),
        include_str!("../../../../common/src/run_questions.rs"),
        include_str!("../../../../common/src/stop_rules/screen.rs"),
    ] {
        assert!(
            !src.contains("COMPLETE plan again"),
            "a send-back asks for the changed files only"
        );
    }
}
