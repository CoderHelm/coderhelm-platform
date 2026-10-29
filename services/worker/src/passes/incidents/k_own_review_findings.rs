//! k. CoderHelm's own pull request got CoderHelm's own review: three
//! findings, all posted as notes, and the verdict APPROVE (posted as a
//! comment, since GitHub refuses a verdict on the bot's own PR). Three
//! seconds later the PR was marked ready for review and the merge armed,
//! waiting on approvals: a person got a PR with three known open issues,
//! none fixed and no thread resolved.
//!
//! Guarantee: on its own PR, CoderHelm fixes every finding of its own
//! review that isn't strictly optional (an inferred preference, or an
//! optional improvement to correct code) before the PR is marked ready or
//! the merge armed. The review lists them at the top of the description and
//! requests changes; the run takes them in review-fix rounds; the next
//! review resolves the threads the new commit settled. It goes on while the
//! rounds make progress, and two rounds without progress stop it with the
//! PR still a draft, the findings listed, and the run `needs_input`. A
//! verdict is never APPROVE while a finding holds the merge.

use super::fixture::*;
use crate::passes::completeness::{self, Section};
use crate::passes::own_review::{self, Next};
use crate::passes::resume::draft_hold;
use crate::passes::review_agent::{self, Finding};
use crate::passes::review_pr::RunReview;
use crate::passes::review_severity;

const OLD: &str = "Approved with notes";

/// The three findings the review posted, as the model answered them.
fn the_three_findings() -> Vec<Finding> {
    serde_json::from_value(serde_json::json!([
        {"file": "jobs/common/upsert.py", "line": 1204, "severity": "medium", "category": "correctness",
         "evidence": "unverified-concern",
         "title": "Kept updated_at hides the row from both incremental cursors",
         "body": "`jobs/common/upsert.py:1204` keeps updated_at while other columns change; the watermark in `jobs/sync_utils.py:357` reads MAX(updated_at), so the change never reaches the index until the nightly rebuild."},
        {"file": "jobs/common/upsert.py", "line": 1163, "severity": "low", "category": "correctness",
         "evidence": "verified-defect", "basis": "data",
         "title": "Ties on the sort key still flip between runs",
         "body": "A query on the source table found 9 groups with tied start dates. Most look old, so the impact is likely small."},
        {"file": "jobs/sync_pipeline.py", "line": 1328, "severity": "medium", "category": "convention",
         "evidence": "verified-defect",
         "title": "Skipped records get no WARNING and no count",
         "body": "The team rules ask for a WARNING with the record id and a count in the run summary; `if not start: continue` skips silently."}
    ]))
    .unwrap()
}

#[test]
fn the_own_review_requests_changes_and_lists_every_non_optional_finding() {
    let mut end = Ending::default();
    let mut findings = the_three_findings();
    review_severity::ground(&mut findings, "");
    review_severity::finalize(&mut findings);
    // Every one blocks under the rubric, so the verdict can't be APPROVE.
    assert!(findings.iter().all(Finding::holds_merge));
    let verdict = review_agent::decide_verdict(&findings, 0, &[], false);
    assert_eq!(verdict, "REQUEST_CHANGES");
    assert_eq!(
        review_severity::consistent_verdict("APPROVE", &findings),
        "REQUEST_CHANGES"
    );
    // On its own PR they are the run's to fix, listed at the top.
    let open = own_review::required(&findings, &[]);
    assert_eq!(open.len(), 3);
    let body = completeness::with_section(
        "## Summary\nKeeps the text.\n\n## Changes\n- The upsert.\n",
        Section::OpenFindings,
        own_review::section(&open),
    );
    assert!(body.starts_with(own_review::START));
    end.said(body.clone());
    end.said(review_agent::verdict_heading(
        verdict,
        &review_agent::HeadingCounts {
            defects: 3,
            own_open: 3,
            ..Default::default()
        },
    ));
    // While they are listed the PR stays a draft: neither mark-ready path
    // goes on.
    assert!(draft_hold(&body, "Keep the text", true).is_some());
    let main = include_str!("../../main.rs");
    let mark = fn_src(main, "\nasync fn mark_pr_ready(");
    let gate = mark
        .find("own_review::hold")
        .expect("mark_pr_ready checks own_review::hold");
    assert!(gate < mark.find("github.mark_pr_ready(").unwrap());
    // The run gets them as a fix round, not a completion.
    let rr = RunReview {
        verdict: "APPROVE",
        body: String::new(),
        override_finding: None,
        open_findings: open,
    };
    assert!(rr.blocks());
    end.draft = true;
    end.never_says(OLD);
}

#[test]
fn the_review_posts_never_approve_with_a_blocker_and_arms_nothing_on_open_findings() {
    let src = include_str!("../review_pr.rs");
    let run = fn_src(src, "\nasync fn run_once(");
    let consistent = run
        .find("review_severity::consistent_verdict(")
        .expect("run_once makes the verdict consistent with its findings");
    let own = run
        .find("own_review::required(")
        .expect("run_once lists the own PR's open findings");
    // Both come before the reviewed head's merge is armed on an approval
    // (the last arming call; an earlier one carries an approval forward).
    let arm = run.rfind("review_actions::run_on_approve(").unwrap();
    assert!(consistent < arm && own < arm);
    // Threads the new commit settled are resolved by the next review.
    assert!(run.contains("for t in &fixed_threads"));
    assert!(run.contains("resolve_review_thread(&t.id)"));
}

#[test]
fn the_run_fixes_them_in_rounds_and_stops_after_two_without_progress() {
    let resume = include_str!("../resume.rs");
    // A review with open own findings fails the run's review: a fix round
    // follows, never ready_or_prove.
    assert!(resume.contains("own_review::fix_feedback(&rr.open_findings"));
    let open = vec!["A (`a.py:1`)".to_string(), "B (`b.py:2`)".to_string()];
    let mut end = Ending::default();
    // Round 1 fixed one of three: progress, another round.
    assert_eq!(
        own_review::next(Some(3), &open, 0, 0),
        Next::Fix { stalled: 0 }
    );
    // Two rounds in a row with the same two open: stop.
    assert_eq!(
        own_review::next(Some(2), &open, 0, 0),
        Next::Fix { stalled: 1 }
    );
    match own_review::next(Some(2), &open, 0, 1) {
        Next::Stop(reason) => {
            end.status = common::run_status::NEEDS_INPUT.into();
            end.draft = true;
            end.said(reason);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(end.status, "needs_input");
    assert!(end.draft);
    assert!(end.says("- A (`a.py:1`)"));
    assert!(end.says("stays a draft"));
    // Nothing open: ready.
    assert_eq!(own_review::next(Some(2), &[], 1, 1), Next::Ready);
    end.never_says(OLD);
}
