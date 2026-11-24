//! ul. Review and fix rounds didn't converge. On one run the review raised
//! "A parallel structure next to an existing one" round after round: each fix
//! round moved the duplicate (a flag, then a merged helper, then a moved
//! function), the next review found it in the new place, and the ledger
//! counted it as a new finding each time. The rounds ran out only when the
//! pull request merged, after a costly string of rounds. On another run two rounds undid each
//! other: a review fix moved a listener to a capture listener on the window, a
//! later pass put the document listener back, and the review raised the
//! same finding again.
//!
//! Guarantee: a style-level finding (a parallel structure, near-identical
//! helpers) is the same ledger item wherever the code moved, and after two
//! rounds on it it is CoderHelm's note for the reviewer, demoted so it never
//! blocks. A round whose change takes back what an earlier review fix did
//! lands only when its account says why (`Undoes <sha>: <why>`); otherwise
//! it is refused and told to keep that fix.

use super::fixture::fn_src;
use crate::passes::own_review_ledger::{self, Ledger, Settled};
use crate::passes::round_agree;

const P1: &str = "A parallel structure next to an existing one (`dags/utils/booking_common.py`)";
const P2: &str = "A parallel structure next to an existing one (`dags/pipeline.py`)";
const P3: &str = "A parallel structure next to an existing one (`dags/utils/activity_map.py`)";

#[test]
fn a_style_finding_that_moves_is_one_item_and_then_a_note() {
    assert!(own_review_ledger::same_finding(P1, P2));
    let mut l = Ledger::default();
    l.observe(&[P1.to_string()], "");
    for (i, now) in [P2, P3].iter().enumerate() {
        l.start_round(&[l.findings[0].item.clone()]);
        l.end_round(vec![], "Folded the helper into the existing mapping.");
        l.observe(&[now.to_string()], "");
        assert_eq!(l.findings.len(), 1, "round {i}: the same item");
    }
    let t = &l.findings[0];
    assert_eq!(t.attempts, own_review_ledger::MAX_STYLE_ATTEMPTS);
    assert!(matches!(&t.settled, Some(Settled::Noted { why }) if why.contains("not a blocker")));
    assert!(l.settled_item(P3).is_some());
    // A finding about behaviour still needs the same file.
    assert!(!own_review_ledger::same_finding(
        "Live impact on charters is not measured (`dags/pipeline.py:1046`)",
        "Live impact on charters is not measured (`dags/other.py:10`)"
    ));
    // The review demotes the noted style finding: never a block.
    let src = include_str!("../review_pr.rs");
    let settle = fn_src(src, "\npub(crate) fn settle_own_findings(");
    assert!(settle.contains("own_review_ledger::is_style(") && settle.contains("demote("));
}

#[test]
fn a_later_round_never_undoes_a_review_fix_without_saying_why() {
    let fix = vec![(
        "abcdef1000".to_string(),
        vec![(
            "src/drawer-client.tsx".to_string(),
            "@@\n-document.addEventListener('visibilitychange', onHide);\n-return () => document.removeEventListener('visibilitychange', onHide);\n-sendDrawerLeft(state);\n+window.addEventListener('visibilitychange', onHide, true);\n+return () => window.removeEventListener('visibilitychange', onHide, true);\n+queueDrawerLeft(state);\n".to_string(),
        )],
    )];
    let later = vec![(
        "src/drawer-client.tsx".to_string(),
        "@@\n-window.addEventListener('visibilitychange', onHide, true);\n-return () => window.removeEventListener('visibilitychange', onHide, true);\n-queueDrawerLeft(state);\n+document.addEventListener('visibilitychange', onHide);\n+return () => document.removeEventListener('visibilitychange', onHide);\n+sendDrawerLeft(state);\n".to_string(),
    )];
    let u = round_agree::undoes(&fix, &later);
    assert_eq!(u.len(), 1);
    assert!(round_agree::undo_refusal(&u).contains(round_agree::UNDO_MARK));
    assert!(!round_agree::says_why("Followed the ticket.", &u[0].sha));
    // The landing every run round takes checks it, and the next step is told.
    let round = include_str!("../round.rs");
    let land = fn_src(round, "\npub(crate) async fn land_agreeing(");
    assert!(land.contains("round_agree::undoes(") && land.contains("says_why("));
    let resume = include_str!("../resume.rs");
    assert_eq!(resume.matches("round::land_agreeing(").count(), 2);
    let fault = fn_src(resume, "\nasync fn round_fault(");
    assert!(fault.contains("round_agree::UNDO_MARK"));
}
