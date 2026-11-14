//! at. A feature run opened its draft pull request, and the proof check
//! found three gaps. The fix round searched for the function that builds a
//! page's metadata and went to edit that existing file, which the pull
//! request had not touched yet: two edits came back "rejected: outside the
//! ticket's files" and a third failed. The gaps stayed open, a planned task
//! was "still not done after the fix round", and planned-work rounds went
//! on for about an hour on a pull request whose checks passed. One of them
//! then hit GitHub's hourly limit: it was read as "it met an error", its
//! work branch was deleted during the limit (a 403 on the branch ref), an
//! attempt was spent, and the next try started five seconds later.
//!
//! Guarantee: a round may edit what it was started for. Its files are the
//! pull request's, the plan's task files (done or not) and listed
//! dependents, and the files its gap, finding or failing check names. Any
//! other existing file is refused every time, with one message that says
//! how to justify the edit; a justified edit is made and recorded on the
//! pull request. A round that ends with an edit refused and no task
//! finished made no progress whatever it pushed: the next round is told
//! what was refused, and a second one stops and says which file, once.
//! GitHub's limit is a wait: the round goes on as itself after the reset,
//! with its work branch and its attempt.

use super::fixture::*;
use crate::passes::finish_planned::{self, Next};
use crate::passes::plan::PlanResult;
use crate::passes::proof::{self, Finding};
use crate::passes::round::{self, Fault};
use crate::passes::write_guard::{Refusal, RoundFor, ScopeGuard, REASON_INPUT};
use crate::passes::{habits_check, supervisor};
use common::github_wait::{self as gw, Cause};
use common::run_round::Kind;

const OLD_REFUSAL: &str = "rejected: outside the ticket's files";
const OLD_FAULT: &str = "it met an error (Waiting for GitHub";

/// The plan task the branch had not done yet: an existing file.
const REGISTRY: &str = "apps/web/src/blocks/index.ts";
/// The existing file a gap names that is no task of the plan.
const METADATA: &str = "apps/web/src/lib/metadata.ts";
/// An existing file nothing names: a linter warns about it.
const UNRELATED: &str = "apps/web/src/lib/dates.ts";

fn plan() -> PlanResult {
    PlanResult {
        proposal: String::new(),
        tasks: plan_tasks_md(),
        spec: String::new(),
        design: String::new(),
        repo_tasks: vec![],
        fix: None,
    }
}

fn gap(point: &str, detail: &str) -> Finding {
    Finding {
        point: point.into(),
        detail: detail.into(),
        ..Default::default()
    }
}

/// The branch as the draft pull request had it: the new files only.
fn branch_with_the_new_files() -> FakeGitHub {
    let mut gh = FakeGitHub::new();
    gh.commit(
        "implement",
        BRANCH,
        &[
            (
                "apps/studio/schemaTypes/blocks/card.ts",
                "export const card = {}\n",
            ),
            (
                "apps/web/src/blocks/card.tsx",
                "export function Card() {}\n",
            ),
        ],
    );
    gh
}

fn three_gaps() -> Vec<Finding> {
    vec![
        gap(
            "assumption",
            &format!("The card page's metadata is assumed to come from `{METADATA}`, which has no entry for it."),
        ),
        gap(
            "team_rule",
            "The team's instructions ask for a test of every new block.",
        ),
        gap(
            "planned_work",
            &format!("The card block is not mapped to its component in `{REGISTRY}`."),
        ),
    ]
}

#[test]
fn the_proof_fix_round_edits_the_files_its_gaps_are_about() {
    let gh = branch_with_the_new_files();
    let pr_files = gh.changed(BRANCH);
    assert!(!pr_files.iter().any(|f| f == REGISTRY || f == METADATA));
    let mut end = Ending {
        status: "running".into(),
        pr: Some(21),
        draft: true,
        ..Default::default()
    };

    // The round's guard, as `implement::run` builds it for the proof fix.
    let reason = proof::fix_feedback(&three_gaps());
    let guard = ScopeGuard::for_round(
        RoundFor::ProofGap,
        &pr_files,
        Some(&plan()),
        &format!("{OWNER}/{REPO}"),
        &reason,
    );
    assert!(guard.bounded());

    // The plan's task file the branch doesn't touch yet, and the file the
    // gap names: both edited at the first try. They were the two refused
    // edits.
    for needed in [REGISTRY, METADATA] {
        match guard.check(needed, None) {
            Ok(()) => end.said(format!("edited {needed}")),
            Err(refusal) => end.said(refusal),
        }
    }
    assert!(end.says(&format!("edited {REGISTRY}")));
    assert!(end.says(&format!("edited {METADATA}")));
    assert!(guard.refused().is_empty());

    // The drive-by edit the guard exists for is refused, the third time as
    // the first: it never gives way to repetition.
    let first = guard.check(UNRELATED, None).unwrap_err();
    for _ in 0..3 {
        assert_eq!(guard.check(UNRELATED, None).unwrap_err(), first);
    }
    end.said(first.clone());
    assert!(first.contains(REASON_INPUT));
    assert!(first.contains(&format!("Outside the plan: {UNRELATED}: <why>")));
    assert_eq!(guard.refused().len(), 1);

    // With a reason it is made, and the reason is the round's record.
    let why = "the card page formats its date with the helper in this file";
    assert!(guard.check(UNRELATED, Some(why)).is_ok());
    assert!(guard.refused().is_empty());
    let outside = guard.outside();
    assert_eq!(
        outside,
        vec![
            (
                METADATA.to_string(),
                "named by the gaps the proof check listed".to_string()
            ),
            (UNRELATED.to_string(), why.to_string()),
        ],
        "the plan's own task file needs no reason"
    );

    // The record reaches the habits check as the coder's evidence line, so
    // the files are never "changed with no reason" and put back, and the
    // pull request lists them.
    let notes = ScopeGuard::evidence_lines(&outside);
    assert_eq!(habits_check::reasons(&notes), outside);
    let body = habits_check::with_outside("## Summary\nAdds the card block.\n", &outside);
    assert!(body.contains(&format!("- `{UNRELATED}`: {why}")));
    assert!(body.contains(&format!("- `{METADATA}`: named by the gaps")));

    end.never_says(OLD_REFUSAL);
}

/// Every round CoderHelm starts by itself after the build says what it is
/// for, at the place it is started; none passes "the pull request's files
/// only", and no count of tries opens the guard.
#[test]
fn every_round_says_what_it_is_for_and_no_count_opens_the_guard() {
    let at = |src: &str, head: &str, what: &str| {
        assert!(src.contains(what), "{head}: {what} is gone");
    };
    at(
        include_str!("../proof.rs"),
        "the proof fix",
        "super::write_guard::RoundFor::ProofGap,",
    );
    at(
        include_str!("../habits_check.rs"),
        "the tests and scope fix",
        "super::write_guard::RoundFor::Habits,",
    );
    at(
        include_str!("../plan_coverage.rs"),
        "the plan round",
        "super::write_guard::RoundFor::PlannedWork,",
    );
    at(
        include_str!("../settle.rs"),
        "the settle fix",
        "super::write_guard::RoundFor::Settle,",
    );
    at(
        include_str!("../link_checks.rs"),
        "the link fix",
        "super::write_guard::RoundFor::LinkFix,",
    );
    let resume = include_str!("../resume.rs");
    at(
        resume,
        "a round on the pull request",
        "super::write_guard::RoundFor::of_round(round.kind, round.scoped),",
    );
    at(
        resume,
        "the CI fix",
        ".unless_person(!review_feedback.is_empty()),",
    );
    // Planned work on the pull request is bounded by the plan, and a
    // review fix by its findings.
    assert_eq!(
        RoundFor::of_round(Kind::PlannedWork, false),
        RoundFor::PlannedWork
    );
    assert!(RoundFor::of_round(Kind::PlannedWork, false).bounded());
    assert_eq!(
        RoundFor::of_round(Kind::ReviewFix, true),
        RoundFor::OwnReview
    );

    // The guard is asked with the reason the call carries, and nothing
    // counts refusals.
    let implement = include_str!("../implement.rs");
    let feedback = include_str!("../feedback.rs");
    let guard = include_str!("../write_guard.rs");
    let guard = &guard[..guard.find("#[cfg(test)]").unwrap()];
    for (name, src) in [
        ("implement", implement),
        ("feedback", feedback),
        ("write_guard", guard),
    ] {
        for gone in ["should_block(", "MAX_SCOPE_REJECTS", "scope_block_decision"] {
            assert!(!src.contains(gone), "{name}.rs still has {gone}");
        }
    }
    assert_eq!(
        implement
            .matches(".check(path, super::write_guard::reason_input(")
            .count(),
        4,
        "write_file, edit_file, batch_write and restore_file ask the guard"
    );
    assert!(feedback.contains("async fn scope_refusal("));
    assert_eq!(feedback.matches(".scope_refusal(path, ").count(), 4);
    // A bounded round's plan comes from the run's cached plan when the
    // round carries none (every round on the pull request).
    let run = fn_src(implement, "pub async fn run(");
    assert!(run.contains("super::run_plan_files(state, msg).await"));
    assert!(run.contains("super::write_guard::ScopeGuard::for_round("));
}

/// The planned-work rounds on the pull request: a round whose edit was
/// refused and that finished no task made no progress although it pushed
/// files; the next round is told what was refused; a second such round
/// stops and names the file once. With the edit made, the task is finished
/// in one round.
#[test]
fn a_planned_work_round_that_is_refused_its_file_says_so_once_and_stops() {
    let mut end = Ending {
        status: "awaiting_ci".into(),
        pr: Some(21),
        draft: true,
        ..Default::default()
    };
    let unfinished = proof::unfinished_section(&three_gaps());
    let before = finish_planned::listed(&unfinished);
    assert_eq!(before.len(), 1);
    let first = finish_planned::first_round(&unfinished, chrono::Utc::now()).unwrap();
    let refused = vec![Refusal {
        path: ".github/workflows/ci.yml".into(),
        why: crate::passes::write_guard::PROTECTED_WHY.into(),
    }];

    // Round 1 pushed two files and was refused one edit: no task finished.
    assert!(!supervisor::progressed(false, 2, refused.len()));
    let Next::Again { stalled } = finish_planned::next(&before, Some(&before), 2, &refused, &first)
    else {
        panic!("round 1 goes on once, with a changed approach");
    };
    assert_eq!(stalled, 1);
    let second = finish_planned::next_round(&first, &before, stalled, &refused, chrono::Utc::now());
    assert!(second.feedback.contains("Edits the last round was refused"));
    assert!(second.feedback.contains("`.github/workflows/ci.yml`"));

    // Round 2 is refused the same edit: the work stops, and says which.
    let next = finish_planned::next(&before, Some(&before), 1, &refused, &second);
    end.status = finish_planned::run_status(&next, 1).into();
    match next {
        Next::Stop(why) => {
            assert!(why.contains("made no progress"), "{why}");
            assert!(why.contains("`.github/workflows/ci.yml`: CI/CD workflow files are protected"));
            end.said(why);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(end.status, common::run_status::NEEDS_INPUT);
    assert!(end.draft);
    assert_eq!(end.words.len(), 1, "said once, not round after round");

    // The same rounds with nothing refused go on as before (pushed files
    // are progress), and a finished task is progress whatever was refused.
    assert_eq!(
        finish_planned::next(&before, Some(&before), 2, &[], &first),
        Next::Again { stalled: 0 }
    );
    assert_eq!(
        finish_planned::next(&before, Some(&[]), 1, &refused, &first),
        Next::Ready
    );

    // The pipeline hands the round's refused edits to that decision.
    let resume = include_str!("../resume.rs");
    let after = fn_src(resume, "async fn after_planned_round(");
    assert!(
        after.contains("finish_planned::next(&before, after.as_deref(), files, refused, round)")
    );
    assert!(after.contains("finish_planned::next_round(round, &items, stalled, refused,"));
    let round_fn = fn_src(resume, "async fn run_fix_round(");
    assert!(round_fn.contains("*refused = std::mem::take(&mut r.refused);"));
    assert!(round_fn.contains("super::habits_check::record_outside("));
}

/// GitHub's hourly limit inside a round on the pull request: a wait for the
/// reset as the same round, never a fault that deletes the work branch and
/// spends an attempt.
#[test]
fn a_round_that_hits_githubs_limit_waits_and_goes_on_as_itself() {
    let mut end = Ending {
        status: "awaiting_ci".into(),
        pr: Some(21),
        draft: true,
        ..Default::default()
    };
    /// The reset the limit named, and when the round hit it.
    const RESET: u64 = 1_793_999_100;
    const HIT: u64 = RESET - 2_400;
    // The error the round's pass ended with, as the run words the pause.
    let paused = gw::waiting_text(Cause::RateLimit, RESET);

    // It is GitHub being busy, and the wait is what this process knows of
    // the limit.
    assert_eq!(round::fault_of(&paused), Fault::GitHubBusy);
    let (cause, wait) =
        round::github_wait_of(&paused, Some((Cause::RateLimit, RESET - HIT))).expect("a wait");
    assert_eq!((cause, wait), (Cause::RateLimit, 2_400));
    assert!(round::may_wait_for_github(0));
    end.said(gw::waiting_text(cause, gw::until(HIT, wait)));

    // The round is parked as it stands: the same attempt, its work branch.
    let mut working = finish_planned::first_round(
        &proof::unfinished_section(&three_gaps()),
        chrono::Utc::now(),
    )
    .unwrap()
    .resumed(chrono::Utc::now());
    working.wip_branch = common::run_round::wip_branch(BRANCH);
    let waiting = working.parked(false);
    assert_eq!(waiting.attempt, working.attempt, "a wait is not an attempt");
    assert_eq!(waiting.wip_branch, working.wip_branch, "its work is kept");
    assert_eq!(waiting.state, common::run_round::State::Pending);

    // Even past its waits the fault keeps the work branch; it used to be
    // "an error", which started clean.
    let plan = round::plan_for_fault(Kind::PlannedWork, &paused, round::after_fault_parked(true));
    assert!(!plan.clean);
    end.said(plan.note.clone());

    // The resume is sent on in hops until the reset, then runs the round.
    let mut t = HIT;
    let mut hops = 0;
    while let Some(h) = gw::hop(gw::until(HIT, wait), t) {
        assert!(h <= gw::MAX_HOP_SECS);
        t += h;
        hops += 1;
    }
    assert_eq!(hops, 3, "forty minutes is three hops");
    assert!(t >= RESET);

    // The pipeline: a round's fault is asked for GitHub's wait before it
    // is classified, and the wait parks the round without a new attempt.
    let resume = include_str!("../resume.rs");
    let fault = fn_src(resume, "async fn round_fault(");
    let waits = fault
        .find("super::round::github_wait_of(why, github_known)")
        .expect("the fault asks for GitHub's wait");
    let classified = fault.find("super::round::plan_for_fault(").unwrap();
    assert!(waits < classified);
    let wait_fn = fn_src(resume, "async fn wait_round_for_github(");
    assert!(wait_fn.contains("&round.parked(false)"));
    assert!(wait_fn.contains("send_github_wait(state, msg, cause, wait)"));
    assert!(!wait_fn.contains("delete_branch"));
    assert!(!wait_fn.contains("start_clean"));
    let round_fn = fn_src(resume, "async fn run_fix_round(");
    assert!(round_fn.contains("crate::clients::github::outside_wait(err)"));

    assert!(
        end.says("Waiting for GitHub's hourly limit to reset at 21:05 UTC; continues by itself")
    );
    end.never_says(OLD_FAULT);
    assert_eq!(end.status, "awaiting_ci");
}
