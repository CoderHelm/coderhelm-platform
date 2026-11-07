//! t. A run opened its pull request and ran its checks on it. The
//! failing-test rules needed the end-to-end package's dependencies, and
//! CoderHelm's sandbox had no `pnpm`: the install failed twice with
//! "pnpm: command not found", and the gate returned that tooling failure
//! instead of a result. It went under Open items as "a check the change
//! didn't pass", marked to hold the draft. The repository's CI then passed on
//! the head, but the line stayed, so the pull request stayed a draft: first
//! when CI passed ("The PR stays a draft: a check the change didn't pass is
//! listed in the description"), and again when the ready sweep decided
//! a day and a half later. Nothing about the change had failed; the hold's
//! reason was CoderHelm's own environment.
//!
//! Guarantee: a check CoderHelm's own tools couldn't run is never a hold. Its
//! gate's tooling failure becomes an Open item CI settles
//! (`proof::check_item`), removed when CI passes. A line written the old way
//! is settled once the repository's CI is green on the head: the decision
//! doesn't hold on it and the description loses it when the pull request is
//! marked ready. A check the change really didn't pass still holds.

use crate::passes::proof;
use crate::passes::ready::{self, Ci, Decision, Facts};

/// The gate's reason, as the run recorded it.
const TOOLING: &str = "Tooling failure: `cd 'e2e' && (pnpm install --ignore-workspace \
     --frozen-lockfile || pnpm install --ignore-workspace)` failed: bash: line 1: pnpm: command \
     not found Tried first: `cd 'e2e' && pnpm install` failed (exit 127).";

/// The old reason, never to be the hold again for a tooling failure.
const OLD: &str = "A check the change didn't pass is listed under Open items";

/// The description as the run left it: proof gaps CI settles, and the
/// failing-test rules' tooling failure written as a blocked line.
fn the_description_as_written() -> String {
    let gaps = proof::gaps_section(&[
        proof::Finding {
            point: "ci_pending".into(),
            detail: "Most of the web unit tests this diff changes did not run in the sandbox."
                .into(),
            settle: "CI runs them.".into(),
            ..Default::default()
        },
        proof::blocked_item("The failing-test rules", TOOLING),
    ]);
    format!("## Summary\nSwitches block titles to the heading component.\n\n{gaps}")
}

fn facts(body: &str, ci_green: bool) -> Facts {
    Facts {
        draft: true,
        title: "Switch every remaining block title to the heading component".into(),
        body: body.to_string(),
        ci: Ci {
            green: ci_green,
            why: if ci_green {
                String::new()
            } else {
                "`build` is still running".into()
            },
        },
        approved: true,
        ..Default::default()
    }
}

#[test]
fn a_tooling_failure_written_as_a_blocked_line_stops_holding_once_ci_is_green() {
    let body = the_description_as_written();
    // While CI hasn't passed, the check it couldn't run is still unsettled.
    match ready::decide(&facts(&body, false)) {
        Decision::Held { reasons } => assert!(reasons.iter().any(|r| r.what.contains(OLD))),
        d => panic!("held while CI isn't green, got {d:?}"),
    }
    // CI green on the head: nothing true holds it any more.
    let d = ready::decide(&facts(&body, true));
    assert_eq!(d, Decision::Ready { open: vec![] });
    // And the description no longer says it holds.
    let after = ready::body_for(&body, &d).expect("the line goes");
    assert!(!after.contains(proof::BLOCKED_MARK), "{after}");
    assert!(!after.contains("pnpm: command not found"), "{after}");
    assert!(after.contains("Switches block titles"), "{after}");
    assert!(ready::body_holds_on(&after, false).is_empty());
    // Decided again (the sweep), nothing changes.
    assert_eq!(ready::body_for(&after, &d), None);
}

#[test]
fn a_gate_tooling_failure_is_an_item_ci_settles_never_a_hold() {
    let item = proof::check_item("The failing-test rules", TOOLING);
    assert_eq!(item.settler(), proof::Settler::Ci);
    let section = proof::gaps_section(&[item]);
    assert!(
        section.contains("couldn't run in CoderHelm's sandbox"),
        "{section}"
    );
    assert!(!proof::holds_blocked(&section), "{section}");
    assert!(ready::body_holds(&section).is_empty());
    // CI passing removes it like any item CI settles.
    let body = format!("## Summary\nx\n\n{section}");
    let settled = proof::settle_by_ci(&body).expect("CI settles it");
    assert!(!settled.contains("pnpm"), "{settled}");
}

#[test]
fn a_check_the_change_really_failed_still_holds_with_ci_green() {
    let gaps = proof::gaps_section(&[proof::check_item(
        "The new-feature rules",
        "two new blocks aren't registered in the schema index",
    )]);
    let body = format!("## Summary\nx\n\n{gaps}");
    match ready::decide(&facts(&body, true)) {
        Decision::Held { reasons } => assert!(reasons.iter().any(|r| r.what.contains(OLD))),
        d => panic!("a real failed check holds, got {d:?}"),
    }
    assert_eq!(proof::without_settled_blocked(&body, true), None);
}

#[test]
fn the_pipeline_writes_gate_errors_through_check_item() {
    let src = include_str!("../mod.rs");
    for check in [
        "The link and redirect check",
        "The failing-test rules",
        "The new-feature rules",
    ] {
        assert!(
            src.contains(&format!("proof::check_item(\"{check}\", &why)")),
            "{check}"
        );
        assert!(
            !src.contains(&format!("proof::blocked_item(\"{check}\", &why)")),
            "{check}"
        );
    }
    let ready = include_str!("../ready.rs");
    assert!(super::fixture::fn_src(ready, "pub fn decide(")
        .contains("body_holds_on(&f.body, f.ci.green)"));
}
