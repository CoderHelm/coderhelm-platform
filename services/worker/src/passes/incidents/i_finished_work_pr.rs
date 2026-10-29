//! i. A run finished 32 of 33 planned tasks, the check before the pull
//! request still found one not done after its fix round, and the run ended
//! with no pull request: the work stayed on a branch nobody looked at. Once
//! it opened a draft instead, the planned work was left for a person.
//!
//! Guarantee: finished work always ends with a pull request. Planned work
//! still not done opens it as a draft with a "Not finished" section at the
//! top; CoderHelm then keeps working those tasks in rounds on the pull
//! request while each round makes progress, rewrites the section after each,
//! and marks the pull request ready when it is empty. Only two rounds in a
//! row without progress stop it, naming the tasks and why, with the run's
//! status `needs_input`, never `completed`.

use super::fixture::*;
use crate::passes::completeness::{self, Section};
use crate::passes::finish_planned::{self, Next};
use crate::passes::proof;
use crate::passes::resume::draft_hold;
use common::run_round::{Kind, Round};

const OLD: &str = "The work so far is kept on branch";

const CARD: &str =
    "`apps/web/src/blocks/card.tsx` is not written: the card block has no component.";
const MAP: &str = "The card block is not mapped in `apps/web/src/blocks/index.ts`.";

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-10-28T16:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// The run reaches the pull request: the proof judge (a recorded reply)
/// still finds two planned tasks not done after its fix round. Returns the
/// draft's description and the round parked for after it opens.
fn open_the_pull_request(gh: &mut FakeGitHub, end: &mut Ending) -> Option<Round> {
    let reply = judge_reply(&[
        ("planned_work", CARD),
        ("planned_work", MAP),
        ("ci_pending", "The site's build runs in CI."),
    ]);
    let gaps = proof::parse_findings(&reply).unwrap();
    let unfinished = proof::unfinished_section(&gaps);
    assert!(!unfinished.is_empty());
    let body = completeness::with_section(
        "## Summary\nAdds the card block.\n\n## Changes\n- The schema and query.\n",
        Section::Unfinished,
        unfinished.clone(),
    );
    gh.prs.push(FakePr {
        number: 21,
        branch: BRANCH.into(),
        body: body.clone(),
        draft: true,
    });
    end.pr = Some(21);
    end.draft = true;
    end.said(body);
    end.status = common::run_status::AWAITING_CI.into();
    finish_planned::first_round(&unfinished, now())
}

/// One planned-work round as the resume runs it: the round's commit lands
/// (`pushed` files), the judge (a recorded reply) reads the branch again,
/// the section is rewritten, and the next step is decided.
fn planned_round(
    gh: &mut FakeGitHub,
    end: &mut Ending,
    round: &Round,
    pushed: &[(&str, &str)],
    still_not_done: &[&str],
) -> Option<Round> {
    assert_eq!(round.kind, Kind::PlannedWork);
    assert!(!round.scoped, "planned work may add files");
    if !pushed.is_empty() {
        gh.commit("planned_work", BRANCH, pushed);
    }
    let body = gh.pr(21).body.clone();
    let before = finish_planned::listed(&body);
    for item in &before {
        assert!(
            round.feedback.contains(item.as_str()),
            "the round is told every task"
        );
    }
    let reply = judge_reply(
        &still_not_done
            .iter()
            .map(|d| ("planned_work", *d))
            .collect::<Vec<_>>(),
    );
    let section = proof::unfinished_section(&proof::parse_findings(&reply).unwrap());
    let after = finish_planned::listed(&section);
    gh.pr_mut(21).body = completeness::with_section(&body, Section::Unfinished, section);
    let next = finish_planned::next(&before, Some(&after), pushed.len(), round);
    end.status = finish_planned::run_status(&next, pushed.len()).into();
    match next {
        Next::Ready => {
            if draft_hold(&gh.pr(21).body, "Add the card block", false).is_none() {
                gh.pr_mut(21).draft = false;
                end.draft = false;
            }
            None
        }
        Next::Again { stalled } => Some(finish_planned::next_round(round, &after, stalled, now())),
        Next::Stop(why) => {
            end.said(why);
            None
        }
    }
}

#[test]
fn planned_work_left_opens_a_draft_then_coderhelm_finishes_it() {
    let mut gh = FakeGitHub::new();
    let mut end = Ending::default();
    let first = open_the_pull_request(&mut gh, &mut end).expect("a planned-work round is parked");
    assert_eq!(
        end.pr,
        Some(21),
        "finished work always ends with a pull request"
    );
    assert!(gh.pr(21).draft);
    assert_eq!(
        draft_hold(&gh.pr(21).body, "Add the card block", false),
        Some("planned work not done is listed in the description")
    );
    assert!(gh
        .pr(21)
        .body
        .contains("CoderHelm keeps working on it here"));
    assert_eq!(finish_planned::listed(&gh.pr(21).body).len(), 2);

    // Round 1 writes the component: one task left.
    let second = planned_round(
        &mut gh,
        &mut end,
        &first,
        &[("apps/web/src/blocks/card.tsx", "export function Card() {}")],
        &[MAP],
    )
    .expect("progress: another round");
    assert_eq!(second.number, 2);
    assert_eq!(second.stalled, 0);
    assert_eq!(end.status, common::run_status::AWAITING_CI);
    assert_eq!(finish_planned::listed(&gh.pr(21).body).len(), 1);
    assert!(gh.pr(21).draft);

    // Round 2 maps it: nothing left, the section goes, the PR is ready
    // once CI passes on the new commit.
    let none = planned_round(
        &mut gh,
        &mut end,
        &second,
        &[("apps/web/src/blocks/index.ts", "card: Card")],
        &[],
    );
    assert!(none.is_none());
    assert!(!proof::holds_unfinished(&gh.pr(21).body));
    assert!(!gh.pr(21).draft, "marked ready when nothing is left");
    assert_eq!(end.status, common::run_status::AWAITING_CI);
    end.never_says(OLD);
    end.never_says("Stopped with planned work not done");
}

#[test]
fn two_rounds_without_progress_stop_with_the_tasks_and_never_complete() {
    let mut gh = FakeGitHub::new();
    let mut end = Ending::default();
    let first = open_the_pull_request(&mut gh, &mut end).unwrap();
    let second = planned_round(&mut gh, &mut end, &first, &[], &[CARD, MAP])
        .expect("one stalled round is not a stop");
    assert_eq!(second.stalled, 1);
    let none = planned_round(&mut gh, &mut end, &second, &[], &[CARD, MAP]);
    assert!(none.is_none());
    assert_eq!(end.status, common::run_status::NEEDS_INPUT);
    assert_ne!(end.status, common::run_status::COMPLETED);
    assert!(
        end.says("planned-work rounds 1 and 2 made no progress"),
        "{:?}",
        end.words
    );
    assert!(end.says("apps/web/src/blocks/card.tsx"));
    assert!(end.says("apps/web/src/blocks/index.ts"));
    assert!(gh.pr(21).draft, "the pull request stays a draft");
    assert!(proof::holds_unfinished(&gh.pr(21).body));
}

#[test]
fn a_round_whose_check_cannot_answer_claims_nothing_done() {
    let mut gh = FakeGitHub::new();
    let mut end = Ending::default();
    let round = open_the_pull_request(&mut gh, &mut end).unwrap();
    let before = finish_planned::listed(&gh.pr(21).body);
    assert_eq!(
        finish_planned::next(&before, None, 0, &round),
        Next::Again { stalled: 1 }
    );
}

#[test]
fn the_pipeline_parks_the_round_and_the_resume_runs_it() {
    let src = include_str!("../mod.rs");
    let rp = run_passes_src();
    assert!(rp.contains("let unfinished = proof::unfinished_section(&gaps);"));
    assert!(rp.contains("finish_planned::first_round(&unfinished, chrono::Utc::now())"));
    assert!(rp.contains("round::record(state, &msg.team_id, run_id, round).await;"));
    // The multi-repository pipeline opens its drafts the same way.
    let multi = fn_src(src, "\nasync fn run_repo_pipeline(");
    assert!(multi.contains("let unfinished = proof::unfinished_section(&gaps);"));
    // The resume runs a parked planned-work round, judges again, and stops
    // with the status `finish_planned` gives (never a literal `completed`
    // for stopped work).
    let resume = include_str!("../resume.rs");
    let pending = fn_src(resume, "\nasync fn run_pending_round(");
    assert!(pending.contains("after_planned_round("));
    let after = fn_src(resume, "\nasync fn after_planned_round(");
    assert!(after.contains("recheck_planned_work("));
    assert!(after.contains("finish_planned::run_status(&next, files)"));
    assert!(after.contains("Section::Unfinished"));
    assert!(after.contains("mark_pr_ready("));
    let stop = &after[after.find("Next::Stop(reason) => {").unwrap()..];
    assert!(!stop.contains("\"completed\""));
}
