//! ox. On the same pull request as `ow`, a review three rounds in raised
//! "A parallel structure next to an existing one" and "A test out of
//! proportion to the change" on code that hadn't changed since the first
//! review, and said they "should have been raised then". On CoderHelm's own
//! pull request each late note was still chained into the work that holds
//! the merge, so each one cost a push and a review. Why they were late:
//! the checks that find them (the smallest-change checks, with no model)
//! started running after the earlier rounds were reviewed, so the review
//! blamed itself for a miss it couldn't have made; and three more ways a
//! first review leaves findings for later were open in the code: the
//! checks with no model ran only when the head's tree could be read, a diff
//! over its room lost the files in its middle, and a review that ran out of
//! tool steps answered with the tools off as if it were done.
//!
//! Guarantee: every check runs on every review, the first included, with
//! or without the tree; the diff the reviewer gets names every changed
//! file and lists the ones it cut; a review that runs out of steps goes on
//! in a second pass over what it didn't reach, in the same review. A
//! finding a later review raises on lines unchanged since the previous one
//! never blocks: it is a note that says why (that review missed it, or the
//! check that found it started after it). On CoderHelm's own pull request
//! it is still sent to the coder in this round's work, apart from what
//! holds. A miss is recorded as one (`rewards::record_missed`, the review
//! lessons' raise-early lesson); a new check's finding is not.

use crate::passes::minimal_change::{Kind, Widening};
use crate::passes::review_agent::{self, Finding};
use crate::passes::review_one_verdict as one;
use crate::passes::review_policy::{hold_late_blockers, Round, UpFront, LATE_NOTE, NEW_CHECK_NOTE};
use crate::passes::review_rules::wider_findings;
use std::collections::{HashMap, HashSet};

const FILTERS: &str = "apps/web/src/utils/filterMaps.ts";
const TEST: &str = "apps/web/src/test/components/filter-nav-dock-grid.test.tsx";

fn the_two() -> Vec<Finding> {
    wider_findings(&[
        Widening {
            kind: Kind::Parallel,
            path: FILTERS.into(),
            what: "`dedupeDockOptionsBySlug` next to the existing `dedupeDocksBySlug`".into(),
        },
        Widening {
            kind: Kind::TestSize,
            path: TEST.into(),
            what: "240 added lines against 40 lines of changed code".into(),
        },
    ])
}

/// Round 4 changed only `FilterNav.tsx`; the two findings sit in files the
/// rounds since the previous review never touched.
fn changed_since_previous() -> HashMap<String, HashSet<u64>> {
    [(
        "apps/web/src/components/blocks/FilterNav.tsx".to_string(),
        HashSet::from([88, 89, 90]),
    )]
    .into()
}

fn round4() -> Round {
    Round {
        number: 4,
        previous_head: Some("ddd4444".into()),
    }
}

/// The previous review ran before the checks did: the findings are notes
/// saying so, and no miss is recorded.
#[test]
fn a_check_newer_than_the_previous_review_is_not_a_miss_and_never_blocks() {
    let mut findings = the_two();
    assert!(findings.iter().all(Finding::holds_merge));
    let changed = changed_since_previous();
    let r = round4();
    let late = hold_late_blockers(
        &mut findings,
        &UpFront {
            round: &r,
            changed_since_previous: Some(&changed),
            earlier_threads: &[],
            person_spoke: false,
            previous_review_at: "2026-11-16T23:21:58Z",
        },
    );
    assert!(findings.is_empty());
    assert_eq!(late.held.len(), 2);
    assert!(late.held.iter().all(|f| !f.holds_merge()));
    assert!(late
        .held
        .iter()
        .all(|f| f.note_only.as_deref() == Some(NEW_CHECK_NOTE)));
    assert!(late.missed.is_empty());
    assert!(!late.self_lesson);
    let md = one::late_markdown(&late.held);
    assert!(md.contains("found by a check that started running after it"));
    assert!(!md.contains("should have been raised"));
}

/// The previous review ran with the checks: a finding raised now on lines
/// it read is its miss, a note here, recorded, and still the coder's work on
/// CoderHelm's own pull request.
#[test]
fn a_finding_the_previous_review_missed_is_a_recorded_note_and_this_rounds_work() {
    let mut findings = the_two();
    let changed = changed_since_previous();
    let r = round4();
    let late = hold_late_blockers(
        &mut findings,
        &UpFront {
            round: &r,
            changed_since_previous: Some(&changed),
            earlier_threads: &[],
            person_spoke: false,
            previous_review_at: "2026-11-18T10:00:00Z",
        },
    );
    assert_eq!(late.missed.len(), 2);
    assert!(late.self_lesson);
    assert!(late
        .held
        .iter()
        .all(|f| f.note_only.as_deref() == Some(LATE_NOTE) && !f.holds_merge()));
    let work = one::late_lines(&late.held);
    assert_eq!(work.len(), 2);
    assert!(work.iter().all(|w| w.contains(one::LATE_MARK)));
    // The note says the previous review missed it, never that it holds.
    let md = one::late_markdown(&late.held);
    assert!(md.contains("which missed them"));
    // A miss weighs on the reviewer's score.
    assert!(
        common::rewards::score::default_weight(common::rewards::RewardKind::FindingMissed) < 0.0
    );
}

/// Round 1 has no previous review: nothing is held back, everything the
/// checks find blocks there.
#[test]
fn the_first_review_holds_back_nothing() {
    let mut findings = the_two();
    let r = Round {
        number: 1,
        previous_head: None,
    };
    let late = hold_late_blockers(
        &mut findings,
        &UpFront {
            round: &r,
            changed_since_previous: None,
            earlier_threads: &[],
            person_spoke: false,
            previous_review_at: "",
        },
    );
    assert!(late.held.is_empty());
    assert_eq!(findings.len(), 2);
}

/// A diff over its room keeps every file, and names those it cut.
#[test]
fn a_long_diff_keeps_every_file() {
    let big = "+x\n".repeat(30_000);
    let files: Vec<serde_json::Value> = (0..5)
        .map(|i| {
            serde_json::json!({
                "filename": format!("src/f{i}.ts"),
                "status": "modified",
                "additions": 1,
                "deletions": 0,
                "patch": if i == 2 { "+small change".to_string() } else { big.clone() },
            })
        })
        .collect();
    let compare = serde_json::json!({ "files": files });
    let d = review_agent::format_diff(&compare, 40_000);
    assert!(d.len() <= 44_000, "{}", d.len());
    for i in 0..5 {
        assert!(d.contains(&format!("### src/f{i}.ts")), "f{i} dropped");
    }
    assert!(d.contains("+small change"));
    assert!(d.contains("Read each of these at the PR head with read_file"));
    assert!(d.contains("- `src/f0.ts`") && !d.contains("- `src/f2.ts`"));
    // A diff that fits is unchanged.
    let small = serde_json::json!({"files": [{"filename": "a.ts", "status": "added", "additions": 1, "deletions": 0, "patch": "+a"}]});
    assert_eq!(
        review_agent::format_diff(&small, 40_000),
        "\n### a.ts (added, +1/-0)\n+a\n"
    );
}

/// A review that ran out of tool steps goes on over what it didn't read,
/// and the two passes are one answer.
#[test]
fn a_review_cut_by_its_steps_goes_on_in_a_second_pass() {
    let messages = vec![
        (
            "assistant".to_string(),
            vec![
                serde_json::json!({"type": "tool_use", "name": "read_file", "input": {"path": FILTERS}}),
            ],
        ),
        (
            "user".to_string(),
            vec![
                serde_json::json!({"type": "text", "text": crate::agent::anthropic::TOOLS_OFF_NOTE}),
            ],
        ),
    ];
    assert!(review_agent::ran_out_of_steps(&messages));
    assert!(!review_agent::ran_out_of_steps(&messages[..1]));
    assert_eq!(review_agent::opened_files(&messages), vec![FILTERS]);
    let first: review_agent::ReviewOutput = serde_json::from_value(serde_json::json!({
        "summary": "First pass.",
        "findings": [{"file": FILTERS, "line": 3, "title": "Dropdown lists one dock twice", "body": "b"}]
    }))
    .unwrap();
    let prompt = review_agent::continue_prompt("PR: x", &first, &[FILTERS.to_string()]);
    assert!(prompt.contains("ran out of tool"));
    assert!(prompt.contains("Dropdown lists one dock twice"));
    let second: review_agent::ReviewOutput = serde_json::from_value(serde_json::json!({
        "summary": "Second pass.",
        "findings": [
            {"file": FILTERS, "line": 3, "title": "Dropdown lists one dock twice", "body": "b"},
            {"file": TEST, "line": 9, "title": "Test asserts the old order", "body": "b"}
        ]
    }))
    .unwrap();
    let merged = review_agent::merge_review(first, second);
    assert_eq!(merged.findings.len(), 2);
    assert_eq!(merged.summary, "First pass. Second pass.");
}

/// The checks with no model run on every review, whether or not the
/// tree could be read, and a miss is recorded where it is found.
#[test]
fn every_check_runs_on_every_review_and_a_miss_is_recorded() {
    let src = include_str!("../review_pr.rs");
    assert!(
        !src.contains("if let Ok(tree) = github\n        .get_tree("),
        "the checks with no model are skipped when the tree can't be read"
    );
    let tree = src.find("Could not read the head's tree").expect("tree");
    let smallest = src[tree..]
        .find("smallest_change_gaps(")
        .expect("smallest-change checks run after the tree read");
    assert!(smallest > 0);
    // Nothing gates them on the round.
    let around = &src[tree.saturating_sub(800)..tree + smallest];
    assert!(!around.contains("review_round.number"));
    assert!(!around.contains("since_review"));
    assert!(src.contains("super::rewards::record_missed("));
    assert!(src.contains("previous_review_at: &previous_review_at"));
    let agent = include_str!("../review_agent.rs");
    assert!(agent.contains("if !ran_out_of_steps(&messages)"));
}
