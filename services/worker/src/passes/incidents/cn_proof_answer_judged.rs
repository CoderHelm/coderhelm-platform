//! cn. A ticket asked to show slot descriptions without literal HTML
//! entities. The proof check before the pull request listed gaps that asked
//! to confirm things: which stored descriptions hold entities, and whether
//! the live page shows them. The fix round did that work: four database
//! queries, three page checks and a run of the checks, at a real cost. It wrote
//! no code, because the gaps asked for answers, not changes. The check
//! then saw the same branch head, said "the code hasn't changed since the
//! last check, so that result stands", reused the result from before the
//! round, counted the round as no progress and listed the same gaps under
//! Open items. The answers never reached the judge.
//!
//! Across many runs, most proof fix rounds wrote no code (and most of
//! their cost), and the note "the code hasn't changed ... result stands"
//! followed again and again.
//!
//! Guarantee: a fix round that made a lookup or ran a check is judged
//! again on the same head, with what it brought (`proof::round_answered`,
//! `CheckRecord::answered`). Fewer open gaps lets fixing go on; the same or
//! more ends it, as before, so an answer can never loop.

use crate::passes::proof::{self, Lookup, LookupOutcome, RunEvidence};
use crate::passes::run_guard::CheckRecord;
use crate::passes::stop_judge::CycleLicence;

const MAX_ROUNDS: u32 = 2;

/// One pass of the proof loop as `proof::prove` runs it: judged (true)
/// or the earlier result reused (false).
fn check(r: &mut CheckRecord, head: &str, open_if_judged: usize) -> bool {
    if r.unchanged(head) {
        r.reused();
        false
    } else {
        r.checked(head, open_if_judged);
        true
    }
}

fn answers() -> RunEvidence {
    RunEvidence {
        lookups: vec![Lookup::new(
            "L1".into(),
            "Production database",
            LookupOutcome::Answered,
            "SELECT count(*) FROM tour WHERE description ~ '&[a-z]+;'",
            "48 rows",
        )],
        checks: vec!["test: vitest run src/utils/html-text.test.ts: PASS".into()],
        ..Default::default()
    }
}

#[test]
fn a_round_that_looked_things_up_is_judged_again_on_the_same_head() {
    let mut r = CheckRecord::default();
    assert!(check(&mut r, "e5a1", 4));
    r.start_fix(CycleLicence::for_test());
    r.end_fix();
    let round = answers();
    assert!(proof::round_answered(&round));
    r.answered();
    // Same head: judged again, not reused.
    assert!(check(&mut r, "e5a1", 1), "the answers reach the judge");
    assert_eq!(r.open, 1);
    assert!(r.may_fix(MAX_ROUNDS), "fewer gaps: fixing may go on");
}

#[test]
fn an_answer_that_closes_nothing_ends_the_fixing() {
    let mut r = CheckRecord::default();
    check(&mut r, "e5a1", 4);
    r.start_fix(CycleLicence::for_test());
    r.end_fix();
    r.answered();
    assert!(check(&mut r, "e5a1", 4));
    assert!(!r.may_fix(MAX_ROUNDS), "no fewer gaps: stop, never loop");
}

/// The run as it was: a round with no code and no answers reuses the
/// result and ends the fixing.
#[test]
fn a_round_that_brought_nothing_still_reuses_the_result() {
    let mut r = CheckRecord::default();
    check(&mut r, "e5a1", 4);
    r.start_fix(CycleLicence::for_test());
    r.end_fix();
    let round = RunEvidence {
        notes: "Looked at the code; nothing to change.".into(),
        ..Default::default()
    };
    assert!(!proof::round_answered(&round));
    assert!(!check(&mut r, "e5a1", 0), "reused");
    assert!(!r.may_fix(MAX_ROUNDS));
}
