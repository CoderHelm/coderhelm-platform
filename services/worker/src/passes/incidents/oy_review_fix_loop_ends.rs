//! oy. A data-pipeline ticket's run fixed the sport a few camps land on.
//! After a merge of the base branch, each round of CoderHelm's own review
//! raised something new: round 2 a model finding, round 3 "A test out of
//! proportion to the change", round 4 "A parallel structure next to an
//! existing one" in the shared helpers, which named as new a function the
//! main branch had had for months (`get_sport_name_to_id_map`: the branch
//! only added a parameter to it; the real new builder was
//! `build_sport_name_map`). Every one held the merge, so the run went
//! review, fix, review every ten minutes, nine fix rounds and past its
//! spend ceiling. Meanwhile the review on the pull request stayed on an
//! old head: rounds driven by the run's own review pass never asked the PR
//! reviewer, and a review of CoderHelm's own push with the same verdict and
//! no new finding posted nothing. A person had to type "re-review".
//!
//! Guarantees:
//! - The parallel-structure check reads both sides against the base: a
//!   definition the base already has is never new (a changed signature
//!   shows as removed and added), and "existing" means on the base.
//! - After the first review, the smallest-change checks a reason may keep
//!   (parallel structure, test size, log level, reporting) are notes, and a
//!   finding on lines unchanged since the previous review is a note.
//! - Late notes ride along with a round that has work that holds; on their
//!   own they start no round, so the run ends once only notes are left.
//! - Every head the run pushes gets the PR reviewer's review posted, news
//!   or not.

use crate::passes::minimal_change::{parallel_structures, Kind, Widening};
use crate::passes::review_one_verdict as one;
use crate::passes::review_policy::{hold_late_blockers, Round, UpFront, ADVISORY_LATE_NOTE};
use crate::passes::review_rules::wider_findings;
use std::collections::{HashMap, HashSet};

const COMMON: &str = "dags/utils/schedule_common.py";

/// The helpers file on the base branch.
const BASE: &str = "def get_sport_id_map(conn):\n    return {}\n\n\
def get_sport_name_to_id_map(conn):\n    return {}\n\n\
def extract_sport_from_class_name(name):\n    return None\n";

/// The branch: a parameter added to the existing lookup, and a new builder.
const PATCH: &str = "@@ -4,5 +4,12 @@\n\
-def get_sport_name_to_id_map(conn):\n\
+def get_sport_name_to_id_map(conn, include_inactive=False):\n\
     return {}\n\
+\n\
+def build_sport_name_map(rows):\n\
+    return {r[0]: r[1] for r in rows}\n";

#[test]
fn a_function_on_the_base_is_never_called_new() {
    let found = parallel_structures(
        &[(COMMON.into(), "modified".into(), PATCH.into())],
        &[(COMMON.into(), BASE.into())],
    );
    assert!(
        !found
            .iter()
            .any(|w| w.what.starts_with("`get_sport_name_to_id_map` next to")),
        "{found:?}"
    );
    // What the branch really added is still read.
    assert!(
        found
            .iter()
            .all(|w| w.what.starts_with("`build_sport_name_map`")),
        "{found:?}"
    );
}

fn changed() -> HashMap<String, HashSet<u64>> {
    // The rounds since the previous review touched both files.
    [
        (COMMON.to_string(), HashSet::from([4u64, 9])),
        (
            "tests/test_exerp_camp_sport.py".to_string(),
            HashSet::from([12u64]),
        ),
    ]
    .into()
}

#[test]
fn smallest_change_advice_after_the_first_review_never_holds_and_starts_no_round() {
    for (n, kind, path) in [
        (3usize, Kind::TestSize, "tests/test_exerp_camp_sport.py"),
        (4, Kind::Parallel, COMMON),
    ] {
        let mut findings = wider_findings(&[Widening {
            kind,
            path: path.into(),
            what: "x".into(),
        }]);
        assert!(findings[0].holds_merge());
        let c = changed();
        let r = Round {
            number: n,
            previous_head: Some("aaaaaaa".into()),
        };
        let late = hold_late_blockers(
            &mut findings,
            &UpFront {
                round: &r,
                changed_since_previous: Some(&c),
                earlier_threads: &[],
                person_spoke: false,
                previous_review_at: "2026-11-17T17:58:00Z",
            },
        );
        assert!(findings.is_empty(), "round {n}");
        assert_eq!(late.held.len(), 1);
        assert!(!late.held[0].holds_merge());
        assert_eq!(late.held[0].note_only.as_deref(), Some(ADVISORY_LATE_NOTE));
        // Advice, not a miss of the previous review.
        assert!(late.missed.is_empty());
        assert!(one::late_markdown(&late.held).contains("Advice from the smallest-change checks"));
    }
    // The first review: they block, so the coder takes them in round one.
    let mut findings = wider_findings(&[Widening {
        kind: Kind::TestSize,
        path: "tests/test_exerp_camp_sport.py".into(),
        what: "x".into(),
    }]);
    let r1 = Round {
        number: 1,
        previous_head: None,
    };
    let late = hold_late_blockers(
        &mut findings,
        &UpFront {
            round: &r1,
            changed_since_previous: None,
            earlier_threads: &[],
            person_spoke: false,
            previous_review_at: "",
        },
    );
    assert!(late.held.is_empty() && findings[0].holds_merge());
    // A replacing write blocks whenever it is raised: only advice turns
    // into notes.
    let replacing = &wider_findings(&[Widening {
        kind: Kind::Replacing,
        path: COMMON.into(),
        what: "`DELETE FROM`".into(),
    }])[0];
    assert!(!crate::passes::review_rules::advisory_check(replacing));
}

#[test]
fn every_pushed_head_gets_its_review_posted() {
    use crate::passes::review_pr::own_head_is_new;
    assert!(own_head_is_new(Some("aaaaaaa"), "bbbbbbb"));
    assert!(own_head_is_new(None, "bbbbbbb"));
    assert!(!own_head_is_new(Some("bbbbbbb"), "bbbbbbb"));
    let review = include_str!("../review_pr.rs");
    assert!(review.contains("has_news: has_news || new_human_commit || new_own_head,"));
    // The run asks the PR reviewer on every head, not only when its own
    // review pass passed.
    let resume = include_str!("../resume.rs");
    let ask = resume
        .find("super::review_pr::review_for_run(")
        .expect("asked");
    let before = &resume[ask.saturating_sub(1200)..ask];
    assert!(before.contains("let own_pass_passed = review_result.passed;"));
    assert!(!before.trim_end().ends_with("if review_result.passed {"));
    assert!(resume.contains("if own_pass_passed && rr.blocks() {"));
    // Late notes alone write no work list, so no round starts on them.
    assert!(review.contains("let own_open: Vec<String> = if own_open.is_empty() {"));
}
