//! cr. A pull request that removed a link from a slot booking page had its
//! end-to-end check fail on a test it had edited: "selecting a slot adds it
//! to the cart while booking is open". The booking vendor's portal the
//! page embeds had just opened booking, and a visitor who isn't signed
//! in gets the vendor's sign-in drawer instead of a cart line, so the test
//! was red on every branch, the base branch's staging deployment included.
//! The failure comment on the pull request named the test, its error
//! (`Total$0`) and had a screenshot of the drawer.
//!
//! The run never compared the test with the base branch. A fix attempt
//! wrote that a different spec was "broken on main"; with the check still
//! red, the supervisor reverted four of the pull request's own files to an
//! older head whose checks had passed, and the check failed the same way on
//! the result. The pull request said "CI is failing" with a lint warning
//! and a line of the workflow script as the error, "Deferred: reverted 4
//! file(s)" after the work had come back, and the run wrote that it
//! couldn't confirm what the test saw. Several other runs reverted
//! their own work the same way with no comparison, one of them for a cloud
//! account's load balancer limit in the preview deploy.
//!
//! Guarantee: before a CI fix round the failure is read from its own report
//! (the failing test, its error, the screenshot, the report link), the
//! failing test is run with the head's test code against the base branch's
//! deployment and against the pull request's preview, and both results are
//! kept on the run (`common::ci_repro`); each failing check's result on the
//! base branch's head is kept next to this head's. The fixer gets all of it,
//! the screenshot as an image. A revert of CoderHelm's own work needs a
//! record on the head that shows the change makes a check fail; a failure
//! red on the base too is fixed on the branch (the test or the code it
//! checks) and said, never reverted. No sentence CoderHelm writes calls a
//! failure the base branch's, flaky or not this change's without that
//! record, and none says a failure couldn't be confirmed while its report
//! was read. The "CI is failing" section names its head and goes once the
//! head moves or its checks show no failure; a revert's "Deferred" line goes
//! once its files are back.

use super::fixture::fn_src;
use crate::passes::{ci_repro, resume};
use common::ci_lines::{self, HeadCi};
use common::ci_repro::{self as repro, Leg, Record, RevertGate, Verdict};

const HEAD: &str = "e1e2e3f4a5b6c7d8";
const TEST: &str = "tests/browse/slot-booking.spec.ts › selecting a slot adds it to the cart while booking is open";

/// The e2e reporter's comment on the pull request, as it was posted.
fn failure_comment() -> String {
    [
        "## Playwright E2E results for e1e2e3f",
        "",
        "1 failed, 52 passed",
        "",
        "  1) [chromium] › tests/browse/slot-booking.spec.ts:141:7 › Slot booking › selecting a slot adds it to the cart while booking is open",
        "",
        "    Error: Choosing \"Acme Guest Slot (morning)\" did not add it to the portal's Booking total",
        "    expect(locator).not.toHaveText(expected) failed",
        "    Received: Total$0",
        "",
        "![failure](https://github.com/user-attachments/assets/0b7a-slot-drawer.png)",
        "",
        "[Full report](https://e2e-reports.example.com/pr-12/index.html)",
    ]
    .join("\n")
}

/// What the sandbox printed for each side: red on staging and on the
/// preview alike.
fn compared() -> Record {
    let red = "Running 1 test using 1 worker\n  1) [chromium] › tests/browse/slot-booking.spec.ts:141:7 › selecting a slot\n    Error: expect(locator).not.toHaveText(expected) failed\n    Received: Total$0\n  1 failed\n";
    Record {
        head: HEAD.into(),
        check: "Deploy Preview".into(),
        tests: vec![TEST.into()],
        base: repro::leg_of("https://staging.harbor.example.com", red),
        change: repro::leg_of("https://pr-12.preview.example.com", red),
        source: "sandbox".into(),
        at: String::new(),
    }
}

#[test]
fn the_failure_is_read_from_its_report_and_compared_with_the_base() {
    let ev = ci_repro::evidence_of("Deploy Preview", &failure_comment(), "").expect("read");
    assert_eq!(ev.tests, vec![TEST.to_string()]);
    assert!(
        ev.error
            .contains("did not add it to the portal's Booking total"),
        "{ev:?}"
    );
    assert_eq!(
        ev.shots,
        vec!["https://github.com/user-attachments/assets/0b7a-slot-drawer.png".to_string()]
    );
    assert!(ci_repro::image_host_allowed(
        "github.com",
        "/user-attachments/assets/0b7a-slot-drawer.png",
        &[]
    ));
    // The test runs against both addresses with the head's test code.
    let (spec, title) = repro::spec_and_title(TEST);
    let cmd = repro::leg_cmd("e2e", &spec, &title, "https://staging.harbor.example.com").unwrap();
    assert!(
        cmd.contains("--grep 'selecting a slot adds it to the cart while booking is open'"),
        "{cmd}"
    );
    assert!(cmd.contains("--retries=0"));

    let r = compared();
    assert!(r.base.red() && r.change.red());
    assert!(matches!(r.verdict(), Verdict::NotThisChange(_)));

    // The fixer reads the error, sees the screenshot and is told what a
    // failure red on the base means for the fix.
    let brief = repro::brief(Some(&ev), std::slice::from_ref(&r), HEAD);
    assert!(
        brief.contains("Total$0") || brief.contains("Booking total"),
        "{brief}"
    );
    assert!(brief.contains("0b7a-slot-drawer.png"), "{brief}");
    assert!(brief.contains("attached to this message as an image"));
    assert!(
        brief.contains("Fix the test, or the code it checks, on this branch"),
        "{brief}"
    );
    assert!(brief.contains("Never revert this pull request's work for it"));
}

#[test]
fn nothing_is_reverted_without_a_comparison_that_shows_the_change_caused_it() {
    // Red on the base too: the four files stay.
    match repro::may_revert(&[compared()], HEAD) {
        RevertGate::Refused(why) => assert!(why.contains("fails on the base branch too"), "{why}"),
        g => panic!("reverted: {g:?}"),
    }
    // No comparison at all (the check that failed doesn't run on the base,
    // as a preview deploy that hit the cloud account's limit): nothing comes
    // out either.
    let infra = repro::from_checks(HEAD, "a0a1a2a3", "Deploy Preview", None);
    assert!(matches!(
        repro::may_revert(&[infra], HEAD),
        RevertGate::Refused(_)
    ));
    // A comparison on an older head proves nothing about this one.
    let ours = Record {
        base: Leg {
            target: "s".into(),
            ran: true,
            passed: 1,
            failed: 0,
            note: String::new(),
        },
        ..compared()
    };
    assert!(matches!(
        repro::may_revert(std::slice::from_ref(&ours), HEAD),
        RevertGate::Allowed(_)
    ));
    assert!(matches!(
        repro::may_revert(&[ours], "b0b1b2b3b4b5"),
        RevertGate::Refused(_)
    ));

    // The pipeline: a ticket run's own work never comes out for a red
    // check at all now (`push_gate::FixLedger` stops the rounds with the
    // test named instead); only a dependency run's bumps do.
    let src = include_str!("../resume.rs");
    let revert = fn_src(src, "async fn take_out_failing_bumps(");
    assert!(revert.contains("revert_dep_commit("));
    assert!(!revert.contains("list_pr_commits("));
}

#[test]
fn blame_and_unconfirmed_need_the_record_and_the_report() {
    // The first attempt's account, before anything was compared.
    let said =
        "The `slot pricing` spec is broken on main since #310; nothing in this diff touches it.";
    let held = resume::held_tried(&[said.to_string()], &[], HEAD, "");
    assert!(!held[0].contains("broken on main"), "{held:?}");
    assert!(held[0].contains(repro::BLAME_HELD));
    // With the comparison on record, the line that says so stays.
    let shown = "Fails on the base branch too: the test fails on staging as well.";
    assert_eq!(
        resume::held_tried(&[shown.to_string()], &[compared()], HEAD, ""),
        vec![shown.to_string()]
    );

    // "Couldn't confirm" while the report was read is replaced by it.
    let ev = ci_repro::evidence_of("Deploy Preview", &failure_comment(), "").unwrap();
    let unsure = "I couldn't confirm what the e2e test saw on the portal.";
    let held = resume::held_tried(&[unsure.to_string()], &[compared()], HEAD, &ev.line());
    assert!(held[0].starts_with("The failure's report says: `Deploy Preview` failed in `tests/browse/slot-booking.spec.ts"), "{held:?}");
    assert!(held[0]
        .contains("screenshot: https://github.com/user-attachments/assets/0b7a-slot-drawer.png"));

    // The pipeline: every account goes through the hold before the pull
    // request, and the fixer's message carries the brief and the shot.
    let src = include_str!("../resume.rs");
    let hand = fn_src(src, "async fn hand_off_red_ci(");
    assert!(hand.find("held_tried(").unwrap() < hand.find("ci_failing_section(").unwrap());
    assert!(src.contains("Failure summary:\\n{failure_logs}{repro_brief}"));
    assert!(src.contains("super::ci_repro::compare("));
    assert!(src.contains("super::ci_repro::screenshot("));
    assert!(
        src.contains("let e2e_on_base = on_base.iter().any(|c| super::e2e_reads::is_e2e_name(c));")
    );
}

#[test]
fn the_error_quoted_is_the_tests_never_the_workflow_script() {
    let logs = [
        "2030-03-17T04:36:58.1Z ✖ 1 problem (0 errors, 1 warning)",
        "2030-03-17T04:36:58.2Z 0 errors and 1 warning potentially fixable with the `--fix` option.",
        "2030-03-17T04:37:00.7683325Z \u{1b}[36;1m# With a build ID, also wait until the page is served by that build\u{1b}[0m",
        "2030-03-17T15:22:44.6320006Z Error: Choosing \"Acme Guest Slot (morning)\" did not add it to the portal's Booking total",
        "2030-03-17T15:22:44.6320874Z expect(locator).not.toHaveText(expected) failed",
    ]
    .join("\n");
    let e = resume::failure_excerpt(&logs);
    assert!(e.starts_with("Error: Choosing"), "{e}");
}

#[test]
fn the_ci_lines_are_true_for_the_head() {
    let section = resume::ci_failing_section(
        "c4c5c6c7c8c9",
        &["Deploy Preview".to_string()],
        "Error: Choosing a slot did not add it",
        &[],
        "CoderHelm fixes the test on this branch.",
    );
    let body = format!(
        "Source ticket: HARBOR-12\n\n{section}\n\nRemoves the new-tab link.\n\n## Deferred\n{}",
        ci_lines::revert_line(
            "CI stayed red: reverted 4 file(s) to b0b1b2b.",
            "b0b1b2b3b4b5b6b7",
            &["apps/web/src/components/blocks/portal-embed.tsx".to_string()],
        )
    );
    // The work came back on a later head and its checks are running: both
    // lines go.
    let later = HeadCi {
        head: HEAD.into(),
        read: true,
        failing: vec![],
    };
    let out = ci_lines::reconcile(&body, &later, &|m| {
        m.paths.iter().any(|p| p.ends_with("portal-embed.tsx"))
    });
    assert!(!out.contains("CI is failing"), "{out}");
    assert!(!out.contains("reverted 4 file(s)"), "{out}");
    assert!(!out.contains("## Deferred"), "{out}");
    assert!(out.contains("Removes the new-tab link."));
    // Every build runs it, and a failing section is never "current".
    let src = include_str!("../description.rs");
    let build = fn_src(src, "async fn build(");
    assert!(build.find("ci_lines_for_head(").unwrap() < build.find("gather(").unwrap());
    assert!(!crate::passes::description::is_current(
        &body,
        "c4c5c6c7c8c9"
    ));
}
