//! um. Runs handed people reads CoderHelm does itself. One asked a person
//! to "read the Deploy Preview e2e result" because its app lacked Actions
//! write; reading a run needs no write. Another stopped with six
//! requirements "that need access CoderHelm doesn't have", the first being
//! to confirm the preview's e2e ran and passed. A third deferred a review
//! finding to a person: "check DevTools on the preview for the analytics
//! request when the tab is hidden".
//!
//! Guarantee: reading an end-to-end or CI result (the workflow run, its
//! log, report or artifacts) or what a page or the preview shows or
//! requests is CoderHelm's own work (`github_history`, `check_url`,
//! `probe_page`). The person gate keeps it as CoderHelm's whatever
//! permission the writer blamed, the plan check makes such a step
//! `by: coderhelm`, and a review round that says only a person can read it
//! is not a person's claim.

use common::person_gate::{decide, Ask, Context, Kind, Outcome, Work};
use common::stop_rules::tooling::names_own_read;

#[test]
fn reading_results_and_pages_is_coderhelms_own() {
    let asks = [
        Ask {
            kind: Kind::Attempt,
            text: "Read the Deploy Preview e2e result for the register pages".into(),
            why_not: "the GitHub App lacks Actions: write".into(),
            ..Default::default()
        },
        Ask {
            kind: Kind::Step,
            text: "Confirm 'Deploy Preview' and 'E2E on preview' ran (not skipped) and the spec passed".into(),
            ..Default::default()
        },
        Ask {
            kind: Kind::Step,
            text: "Check DevTools on the pr-41 preview for a request to the analytics host when the tab is hidden".into(),
            ..Default::default()
        },
    ];
    for a in &asks {
        assert!(
            matches!(decide(a, &Context::default()), Outcome::Do(Work::Own(_))),
            "{}",
            a.text
        );
    }
    // Real access gaps still reach a person.
    assert!(!names_own_read(
        "Grant the read-only role access to the WAF request logs"
    ));
    assert!(!names_own_read(
        "Do a real-device paid checkout on the preview"
    ));
}

#[test]
fn the_plan_and_the_review_ledger_keep_reads_as_coderhelms() {
    let plan = include_str!("../outside_work.rs");
    assert!(plan.contains("names_own_read(&t)"));
    let ledger = include_str!("../own_review_ledger.rs");
    assert!(ledger.contains("tooling::names_own_read(&what)"));
}
