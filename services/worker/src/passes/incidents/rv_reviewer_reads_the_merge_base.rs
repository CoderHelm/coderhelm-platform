//! rv. A vendor-portal pull request, and a CI script's, as the reviewer saw
//! them.
//!
//! - After the base branch was merged into the portal pull request, the
//!   review read the diff from the commit it had approved. That diff
//!   carried a drawer component another pull request had already merged,
//!   and the review called it "an unrelated change". Against the merge
//!   base the pull request changed a handful of files and the drawer
//!   wasn't one.
//! - The preview's end-to-end run exercised the vendor's test ids against
//!   the live portal and passed. The review still blocked on "selectors no
//!   probe observed", because it only read committed probe records.
//! - The end-to-end check was red on the pull request and on the base
//!   branch. The review was told only the check's name, and wrote that an
//!   unrelated late-fee test was "broken on main". The failing test was
//!   the slot-selection test in the spec the pull request changed, red
//!   because the vendor had opened next season's booking.
//! - On the CI script's pull request the review asked for a test of the
//!   script and the run handed it back, though a stub of the cloud command
//!   on PATH and a fixture of its answer were all it needed.
//!
//! Guarantee: a re-review covers only the pull request's own change since
//! the reviewed commit (`review_scope::own_files_moved`, `own_part_of`), and
//! a finding calling a file outside the merge-base diff unrelated is dropped
//! (`drop_outside_claims`). A passing end-to-end run whose report shows the
//! ids is a probe record (`e2e_reads::passing_run`,
//! `review_probe::from_report`), and what is still unobserved the reviewer
//! probes itself and decides from the record (`review_probe::decide`). A red
//! check is read test by test (`review_ci::CiCompare`), and a blame or
//! revert sentence it doesn't back is held (`review_ci::hold`). On
//! CoderHelm's own pull request a request for a test is the coder's work,
//! with how to test a script (`own_review::required`, `test_how`).

use serde_json::{json, Value};

use crate::passes::review_agent::Finding;
use crate::passes::{e2e_reads, own_review, review_ci, review_probe, review_scope};

const PORTAL: &str = "https://portal.example-vendor.io/org/7/portal/42";

fn file(name: &str, patch: &str) -> Value {
    json!({"filename": name, "status": "modified", "patch": patch})
}

/// The pull request's own change: the embed, its test and the portal spec.
fn own(embed_patch: &str) -> Vec<Value> {
    vec![
        file(
            "apps/web/src/components/blocks/portal-embed.tsx",
            embed_patch,
        ),
        file(
            "apps/web/src/test/components/portal-embed.test.tsx",
            "@@ -3,1 +3,1 @@\n-expect(link).toBeVisible();\n+expect(link).toBeNull();",
        ),
        file(
            "e2e/tests/browse/portal.spec.ts",
            "@@ -20,1 +20,1 @@\n-await expect(newTab).toBeVisible();\n+await expect(newTab).toHaveCount(0);",
        ),
    ]
}

const EMBED_AT_APPROVAL: &str =
    "@@ -10,2 +10,1 @@\n-<a href={url}>{linkLabel}</a>\n-<Frame h=\"svh\" />\n+<Frame />";
const EMBED_NOW: &str = "@@ -10,1 +10,0 @@\n-<a href={url}>{linkLabel}</a>";

#[test]
fn a_re_review_after_a_base_merge_covers_only_the_prs_own_change() {
    let at_approval = json!({"status": "ahead", "files": own(EMBED_AT_APPROVAL)});
    let now = json!({"status": "ahead", "files": own(EMBED_NOW)});
    // From the approved commit to the head: the PR's own fix, and the
    // drawer the base branch brought in with the merge.
    let since = json!({"status": "ahead", "files": [
        file("apps/web/src/components/blocks/portal-embed.tsx", "@@ -10,1 +10,1 @@\n-<Frame h=\"svh\" />\n+<Frame />"),
        file("apps/web/src/components/ui/drawer.tsx", "@@ -60,1 +60,2 @@\n <Title>\n+<span className=\"sr-only\">Drawer</span>"),
    ]});
    assert!(!review_scope::same_own_change(&at_approval, &now));
    let moved = review_scope::own_files_moved(&at_approval, &now);
    assert_eq!(
        moved,
        vec!["apps/web/src/components/blocks/portal-embed.tsx"]
    );
    let reviewed = review_scope::own_part_of(&since, &moved);
    let paths = review_scope::compare_paths(&reviewed);
    assert_eq!(
        paths,
        vec!["apps/web/src/components/blocks/portal-embed.tsx"]
    );
    assert!(!paths.iter().any(|p| p.ends_with("drawer.tsx")));
    assert_eq!(reviewed["status"], "ahead");

    // And a finding that calls the drawer this PR's unrelated change is
    // dropped; one about a caller the change breaks is kept.
    let pr_files = review_scope::compare_paths(&now);
    let mut findings = vec![
        Finding {
            file: "apps/web/src/components/ui/drawer.tsx".into(),
            title: "Unrelated change to the drawer's title".into(),
            body: "This file is out of scope for the ticket; revert it.".into(),
            severity: "high".into(),
            ..Default::default()
        },
        Finding {
            file: "apps/web/src/lib/routing/content-renderers.tsx".into(),
            title: "Still passes the removed linkLabel prop".into(),
            body: "The renderer passes `linkLabel`, which the embed no longer takes.".into(),
            severity: "high".into(),
            ..Default::default()
        },
    ];
    let dropped = review_scope::drop_outside_claims(&mut findings, &pr_files);
    assert_eq!(dropped.len(), 1);
    assert_eq!(findings.len(), 1);
    assert!(findings[0].file.ends_with("content-renderers.tsx"));
}

#[test]
fn the_reviewer_wires_the_merge_base_scope_in_both_re_review_paths() {
    let src = include_str!("../review_pr.rs");
    assert_eq!(
        src.matches("review_scope::own_part_of(&since, &moved)")
            .count(),
        2
    );
    assert!(src.contains("review_scope::drop_outside_claims(&mut findings, &changed_list)"));
}

const SPEC: &str = "import { test, expect } from '@playwright/test';\nconst PORTAL = 'https://portal.example-vendor.io/org/7/portal/42';\ntest('choosing a slot adds it to the booking', async ({ page }) => {\n  const frame = page.frameLocator('iframe');\n  await frame.getByTestId('slot-option').first().click();\n  await expect(frame.getByTestId('booking-total-preview')).not.toHaveText('$0');\n});\n";

fn gaps() -> Vec<common::e2e::probe::Unprobed> {
    let files = vec![(
        "e2e/tests/browse/portal.spec.ts".to_string(),
        SPEC.to_string(),
        SPEC.to_string(),
    )];
    common::e2e::probe::unprobed(&files, &[], &["acme.example".into()])
}

#[test]
fn a_passing_end_to_end_run_on_the_head_is_a_record_of_the_ids_it_exercised() {
    let g = gaps();
    assert_eq!(g.len(), 1, "no committed probe: the gate finds the ids");
    let head = "c0ffee1d2e3f405162738495a6b7c8d9e0f1a2b3";
    let checks = vec![
        json!({"name": "Lint", "status": "completed", "conclusion": "success"}),
        json!({"name": "E2E:slots / Playwright", "status": "completed", "conclusion": "success"}),
    ];
    let comments = vec![
        json!({"body": "## Playwright E2E results for c0ffee1\n\n12 passed, 0 failed\n\n[Full report](https://reports.acme.example/pr-1/index.html)"}),
    ];
    let run = e2e_reads::passing_run(&checks, &comments, head).expect("a passing run");
    assert_eq!(
        run.report.as_deref(),
        Some("https://reports.acme.example/pr-1/index.html")
    );
    // The report's text shows both ids (`report_shows`): they are observed.
    let found = vec![
        "booking-total-preview".to_string(),
        "slot-option".to_string(),
    ];
    let record =
        review_probe::from_report(&g[0].origin, run.report.as_deref().unwrap(), head, &found);
    let files = vec![(
        "e2e/tests/browse/portal.spec.ts".to_string(),
        SPEC.to_string(),
        SPEC.to_string(),
    )];
    assert!(common::e2e::probe::unprobed(&files, &[record], &["acme.example".into()]).is_empty());
    // A red or running run is no record.
    let red = vec![
        json!({"name": "E2E:slots / Playwright", "status": "completed", "conclusion": "failure"}),
    ];
    assert_eq!(e2e_reads::passing_run(&red, &comments, head), None);
}

#[test]
fn what_is_still_unobserved_the_reviewer_probes_and_decides_from_the_record() {
    let g = &gaps()[0];
    assert_eq!(
        review_probe::page_url(SPEC, &g.origin).as_deref(),
        Some(PORTAL)
    );
    // The page has the ids: no finding.
    let seen = common::e2e::probe::ProbeRecord {
        origin: g.origin.clone(),
        url: PORTAL.into(),
        testids: vec!["slot-option".into(), "booking-total-preview".into()],
        ..Default::default()
    };
    assert!(review_probe::decide(g, SPEC, SPEC, &review_probe::Probed::Ran(seen)).is_none());
    // The page lacks one: a verified, required finding quoting the record.
    let partial = common::e2e::probe::ProbeRecord {
        origin: g.origin.clone(),
        url: PORTAL.into(),
        testids: vec!["slot-option".into()],
        ..Default::default()
    };
    let f = review_probe::decide(g, SPEC, SPEC, &review_probe::Probed::Ran(partial)).unwrap();
    assert!(f.required.is_some() && f.body.contains("`booking-total-preview`"));
    // The probe couldn't run: a note, never a block on the reviewer's own
    // missing look; on CoderHelm's own PR it is still the coder's work.
    let f = review_probe::decide(
        g,
        SPEC,
        SPEC,
        &review_probe::Probed::NotRun("the sandbox couldn't run the probe".into()),
    )
    .unwrap();
    assert!(f.required.is_none() && !f.holds_merge());
    assert_eq!(own_review::required(&[f], &[]).len(), 1);
}

#[test]
fn the_reviewer_settles_probe_gaps_before_they_are_findings() {
    let src = include_str!("../review_pr.rs");
    let gate = src.find("async fn probe_gaps(").unwrap();
    let end = src.find("async fn smallest_change_gaps(").unwrap();
    let body = &src[gate..end];
    assert!(body.contains("e2e_reads::passing_run("));
    assert!(body.contains("super::web_check::PROBE_TOOL"));
    assert!(body.contains("review_probe::decide("));
    assert!(!body.contains("review_rules::probe_findings("));
}

const HEAD_NOTES: &str = "\n--- E2E on preview / Playwright e2e (all tests) ---\nfailure: e2e/tests/browse/portal.spec.ts:120: 1) [chromium] › browse/portal.spec.ts:120:7 › Portal › selecting a slot adds it to the booking while booking is open ──\n\n    Error: Choosing \"Guest Slot (AM)\" did not add it to the booking total\n    expect(locator).not.toHaveText(expected) failed\n    Received: \"Total$0\"\n";

#[test]
fn a_red_check_is_read_test_by_test_and_a_blame_it_doesnt_back_is_held() {
    // Red on both heads, the same slot test (the vendor opened booking).
    let base_notes = HEAD_NOTES.replace("portal.spec.ts:120", "portal.spec.ts:118");
    let base_runs = json!({"check_runs": [
        {"name": "E2E on preview / Playwright e2e (all tests)", "status": "completed", "conclusion": "failure"}
    ]});
    let cmp = review_ci::CiCompare::read("c0ffee1", "f0e1d2c", HEAD_NOTES, &base_notes, &base_runs)
        .touching(vec!["e2e/tests/browse/portal.spec.ts".into()]);
    let t = &cmp.head_failing[0];
    assert!(t.error.contains("did not add it to the booking total"));
    assert!(cmp.on_base_too(t) && cmp.in_touched_spec(t));
    // The check is never the base's by its name: only the test is.
    assert_eq!(
        cmp.base_failing(),
        vec!["Portal › selecting a slot adds it to the booking while booking is open"]
    );
    let ctx = cmp.context();
    assert!(ctx.contains("(a spec this change touches)"));
    assert!(ctx.contains("did not add it to the booking total"));

    let mut summary =
        "The late fee test is broken on main, so the red e2e check isn't this change's. \
                       Revert the portal spec to the last green head to get CI green."
            .to_string();
    let mut findings = vec![Finding {
        file: "e2e/tests/browse/portal.spec.ts".into(),
        title: "Slot selection test fails".into(),
        body: "selecting a slot adds it to the booking while booking is open fails, and it's pre-existing on main.".into(),
        caused_by_diff: Some(false),
        ..Default::default()
    }];
    let held = review_ci::hold(&mut findings, &mut summary, &cmp);
    assert_eq!(held, 3);
    assert!(summary.starts_with(review_ci::BLAME_HELD));
    assert!(summary.contains(review_ci::REVERT_HELD));
    assert!(!summary.contains("late fee"));
    // The slot test fails on the base too, but it is in the spec this PR
    // changes: its fix belongs on this branch, never "pre-existing".
    assert!(findings[0].body.contains(review_ci::TOUCHED_HELD));
    assert_eq!(findings[0].caused_by_diff, None);
}

#[test]
fn the_reviewer_reads_red_checks_through_the_comparison() {
    let src = include_str!("../review_pr.rs");
    assert!(src.contains("review_ci::CiCompare::read("));
    assert!(src.contains("let base_failing: Vec<String> = ci_compare.base_failing();"));
    assert!(src.contains("review_ci::hold(&mut findings, &mut output.summary, &ci_compare)"));
}

#[test]
fn a_request_for_a_test_of_a_ci_script_is_the_coders_work_with_how() {
    let tracked = Finding {
        file: ".github/scripts/check-waf-rules.sh".into(),
        title: "No test covers the new rule-count check".into(),
        body: "The script now fails when describe-rules returns no rules, but no test covers it."
            .into(),
        category: "tests".into(),
        severity: "medium".into(),
        tracked: Some(true),
        note_only: Some("A note, not a block: the PR already tracks this as a follow-up.".into()),
        ..Default::default()
    };
    let items = own_review::required(&[tracked], &[]);
    assert_eq!(items.len(), 1, "a tracked test ask still reaches the coder");
    assert!(items[0].contains("stub commands first on PATH"));
    assert!(items[0].contains("fixture"));
}
