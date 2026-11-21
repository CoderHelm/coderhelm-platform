//! dj. A made-up marina's booking ticket waited on a person's answer about
//! which booking page to embed. Ten minutes later an edit to the ticket
//! started a fresh run, which built the page and opened its pull request;
//! the first run went on waiting, reminded once a day, its question long
//! moot. One alarm did the same: each firing started a run beside the one
//! still asking, and two of them were still waiting days after a third had
//! finished the fix.
//!
//! Guarantee: when a fresh run of a ticket takes its claim, every earlier
//! run of that ticket in the same repository still waiting on a person
//! stops waiting: it becomes `retried`, replaced by the new run, and its
//! page says where the work went. A run that ended any other way is left as
//! it is, and a run of the same ticket in another repository is its own.

use common::one_run;
use common::run_status::{FAILED, NEEDS_INPUT};

const ASKED: &str = "01JRUNASKED000000000000000";
const ASKED_AGAIN: &str = "01JRUNASKEDAGAIN0000000000";
const FAILED_RUN: &str = "01JRUNFAILED00000000000000";
const OTHER_REPO: &str = "01JRUNOTHERREPO00000000000";
const FRESH: &str = "01JRUNFRESH000000000000000";
const LATER: &str = "01JRUNZLATER00000000000000";

fn runs() -> Vec<(String, String, String)> {
    let r =
        |id: &str, status: &str, repo: &str| (id.to_string(), status.to_string(), repo.to_string());
    vec![
        r(ASKED, NEEDS_INPUT, "harbor/booking"),
        r(ASKED_AGAIN, NEEDS_INPUT, "Harbor/Booking"),
        r(FAILED_RUN, FAILED, "harbor/booking"),
        r(OTHER_REPO, NEEDS_INPUT, "harbor/docks"),
        r(LATER, NEEDS_INPUT, "harbor/booking"),
    ]
}

#[test]
fn a_fresh_run_ends_the_tickets_earlier_waits() {
    let runs = runs();
    let ended = one_run::earlier_waits(FRESH, "harbor/booking", &runs);
    assert_eq!(
        ended,
        vec![ASKED, ASKED_AGAIN],
        "both waits on the ticket end"
    );
    // Never itself, never a newer run, never a run in another repository,
    // never a run that ended some other way.
    assert!(one_run::earlier_waits(ASKED, "harbor/booking", &runs).is_empty());
    let note = one_run::ended_wait_note(FRESH);
    assert!(
        note.contains(FRESH) && note.contains("no longer waits"),
        "{note}"
    );
}

#[test]
fn the_claim_ends_them_once_the_run_holds_the_ticket() {
    let src = include_str!("../one_run.rs");
    let claim = &src[src.find("pub async fn claim(").unwrap()..];
    let held = claim
        .find("\"The run holds its ticket\"")
        .expect("the claim taken");
    let after = &claim[held..];
    assert!(
        after[..after.find("return Ok(());").unwrap()]
            .contains("end_earlier_waits(state, msg, run_id)"),
        "taking the claim ends the earlier waits"
    );
    let f = &src[src.find("async fn end_earlier_waits(").unwrap()..];
    assert!(f.contains("one_run::earlier_waits("));
    // Only while it still waits: an answer or a Retry that came first wins.
    assert!(f.contains(".condition_expression(\"#s = :ni\")"));
    assert!(f.contains("retried_as = :n"));
}
