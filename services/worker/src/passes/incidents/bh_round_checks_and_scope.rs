//! bh. A fix round edited many files and ended with "Ran the
//! checks · not run: not enough time left": it had kept editing until the
//! step had no room for a sandbox build, and nothing ran the checks after
//! it. Two of its edits read "Didn't apply: Edited · refused: outside the
//! round's files", one a test of a file the round changed, with no file
//! named on the run page. The live log drew "Edited · 1 change" and
//! "Rewrote · +12 −8" with no file, and a group of reads with an empty
//! name ("Read 2 files and searched once · , scrollWidth ...").
//!
//! Guarantee: once the branch has unchecked edits and the step has room
//! for one more build but not two, the coder is told to run the checks
//! now; a pass that ends with edits no check ran on owes its checks, and
//! the settle stage runs them before the run finishes (a round is never
//! done on unchecked edits). The tests of a file the round changes, and
//! the module file that exports it, are the round's own files (recorded
//! with why); any refusal names its file. The live log's summary of every
//! call names its file or query as `key: value`.

use std::time::Duration;

use crate::agent::llm::input_summary;
use crate::clients::sandbox::in_check_window;
use crate::passes::implement::{check_now_text, checks_owed};
use crate::passes::write_guard::{refused_summary, RoundFor, ScopeGuard};
use serde_json::json;

#[test]
fn a_round_with_unchecked_edits_runs_its_checks_before_it_is_done() {
    let build = Duration::from_secs(180);
    // Room for one build and its buffer, not two: the moment to check.
    assert!(in_check_window(Duration::from_secs(330), build));
    assert!(!in_check_window(Duration::from_secs(900), build));
    // Too late for any build: the checks are owed, not dropped.
    assert!(!in_check_window(Duration::from_secs(120), build));
    let told = check_now_text(Duration::from_secs(330), build);
    assert!(
        told.contains("Call run_checks now") && told.contains("about 6 min"),
        "{told}"
    );
    // Many edited files, no check ran after them: owed to the settle stage.
    assert!(checks_owed(true, true));
    assert!(!checks_owed(true, false));
    assert!(
        !checks_owed(false, true),
        "no checks in the sandbox: nothing to owe"
    );
}

#[test]
fn the_rounds_tests_are_its_own_and_a_refusal_names_its_file() {
    let g = ScopeGuard::for_round(
        RoundFor::OwnReview,
        &[
            "apps/shop/src/cart/total.ts".to_string(),
            "apps/shop/src/cart/coupon.ts".to_string(),
        ],
        None,
        "acme/shop",
        "",
    );
    assert_eq!(g.check("apps/shop/src/cart/total.test.ts", None), Ok(()));
    assert_eq!(g.check("apps/shop/src/cart/index.ts", None), Ok(()));
    assert!(g
        .outside()
        .iter()
        .any(|(p, why)| p == "apps/shop/src/cart/total.test.ts" && why.contains("the test of")));
    // A file nothing ties to the round is still refused, by name.
    assert!(g.check("apps/shop/src/theme/colors.ts", None).is_err());
    let summary = refused_summary("apps/shop/src/theme/colors.ts");
    assert!(
        summary.contains("apps/shop/src/theme/colors.ts"),
        "{summary}"
    );
}

#[test]
fn every_live_row_names_its_file_or_query() {
    assert_eq!(
        input_summary(
            &json!({"path": "apps/shop/src/cart/total.ts", "edits": "[]", "message": "m"})
        ),
        "path: apps/shop/src/cart/total.ts"
    );
    assert_eq!(
        input_summary(&json!({"query": "scrollWidth <= window.innerWidth"})),
        "query: scrollWidth <= window.innerWidth"
    );
}
