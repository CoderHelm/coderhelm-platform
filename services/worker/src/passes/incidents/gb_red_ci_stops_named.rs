//! gb. A pull request that removed a link from a camp registration page had
//! its end-to-end check fail on "selecting a camp adds it to the cart while
//! registration is open" with `Subtotal$0`. Three fix attempts later the
//! supervisor rolled seven of the pull request's files back to an older
//! green head; five runs of the last 53 rolled back that way. The test failed the same way on the result: the vendor portal
//! had opened registration, so it was red on every branch. Rolling back
//! fixed nothing and threw the change away.
//!
//! Guarantee: red checks never roll a ticket run's branch back to an older
//! head or take a planned task out (`resume::take_out_failing_bumps` only
//! takes out a dependency run's newest set of bumps). A failing test gets
//! at most two pushed fixes (`common::push_gate::FixLedger`, on the run as
//! `ci_test_fixes`); then the rounds stop, nothing more is pushed, and the
//! pull request and the run name the test and its error. A CI log that
//! can't be read is reproduced with the owning package's own commands in
//! the sandbox, and a sentence that blames the unreadable log is replaced.

use super::fixture::fn_src;
use crate::passes::push_checks;
use crate::passes::supervisor::{self, Ending, ReplayStop};
use common::push_gate::{self, FixLedger};

const TEST: &str = "tests/browse/camp-registration.spec.ts:141:7 › Camp registration › selecting a camp adds it to the cart while registration is open";

fn log() -> String {
    format!(
        "2026-11-17T04:37:00.1Z   ✘  1 [chromium] › {TEST} (12.4s)\n\
         2026-11-17T04:37:00.2Z     Error: Choosing \"Acme Mini-Camp (AM session)\" did not add it to the portal's Order Summary\n\
         2026-11-17T04:37:00.3Z     Received: Subtotal$0"
    )
}

#[test]
fn the_failing_test_is_named_and_the_rounds_stop_after_two_fixes() {
    let keys = push_checks::failing_keys(&[], &log(), &["E2E on preview".into()]);
    assert_eq!(keys, vec![TEST.to_string()]);
    let mut ledger = FixLedger::default();
    for _ in 0..push_gate::MAX_FIXES_PER_TEST {
        assert_eq!(ledger.stop(&keys), None);
        ledger.record_push(&keys);
    }
    let stop = ledger.stop(&keys).expect("stop after two fixes");
    let text = stop.text("Received: Subtotal$0");
    assert!(
        text.contains("selecting a camp adds it to the cart"),
        "{text}"
    );
    assert!(text.contains("Subtotal$0"));
    assert!(text.contains("No more fixes are pushed and nothing was rolled back"));
    // The ledger is kept on the run between invocations.
    assert_eq!(FixLedger::from_json(&ledger.to_json()), ledger);
    assert_eq!(push_checks::LEDGER_FIELD, "ci_test_fixes");
}

#[test]
fn red_checks_never_roll_the_branch_back() {
    let src = include_str!("../resume.rs");
    let out = fn_src(src, "async fn take_out_failing_bumps(");
    assert!(out.contains("revert_dep_commit("));
    for rollback in [
        "list_pr_commits(",
        "compare_changed_files(",
        "revert_for(",
        "plan_coverage::",
    ] {
        assert!(!out.contains(rollback), "{rollback}");
    }
    // The stop, with the test named on the pull request and the run, is the
    // ending: an eight-task run with red checks fails with its work kept,
    // never finishes on a rolled-back branch, never waits on a person.
    let hand_off = fn_src(src, "async fn hand_off_red_ci(");
    assert!(hand_off.contains("ci_failing_section("));
    assert_eq!(supervisor::replay(ReplayStop::RedCi, 8, 8), Ending::Failed);
}

#[test]
fn an_unreadable_log_is_reproduced_never_the_reason() {
    let brief = push_gate::logs_unread_brief(
        &["E2E on preview".into()],
        &["cd apps/web && CI=true pnpm exec vitest run".into()],
    );
    assert!(brief.contains("reproduce the failure in the sandbox first"));
    assert!(brief.contains("`cd apps/web && CI=true pnpm exec vitest run`"));
    let held = crate::passes::resume::held_tried(
        &["I could not read the CI logs, so the cause is unknown.".to_string()],
        &[],
        "eb32df1",
        "",
    );
    assert!(!held[0].contains("could not read"), "{}", held[0]);
    assert!(held[0].contains("reproduced"));
    // The fix brief carries it when no job log was read.
    let src = include_str!("../resume.rs");
    assert!(src.contains("{repro_brief}{logs_brief}"));
}
