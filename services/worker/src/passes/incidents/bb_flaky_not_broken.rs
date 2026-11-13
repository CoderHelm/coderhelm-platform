//! bb. A pull request's end-to-end workflow failed on a timeout in a spec
//! the change never touched. The same workflow had failed and then passed
//! on one commit of two unrelated pull requests that week. The run spent
//! its fix rounds on it: a model edited the spec's timeout and a retry
//! helper, in files the ticket had nothing to do with, and the reviewer
//! asked why. On an installation whose GitHub App may not write Actions the
//! re-run CoderHelm tried first was refused (403), which was logged and
//! followed by the same fix round.
//!
//! Guarantee: before a CI fix round, the failure is judged
//! (`common::flaky_checks`). A failure naming a file the pull request
//! changed is the change's. A failing check with a record of failing and
//! then passing on one commit of other pull requests, which names no
//! changed file, is re-run once and no code changes for it; the pull
//! request says which check, the evidence, and what was done. When GitHub
//! refuses the re-run for a missing permission, the pull request says that
//! with the permission by name, and still no code changes. A second failure
//! on a head already re-run is read by a fix round. The record is kept per
//! repository, written only from a commit that failed and then passed with
//! nothing changed.

use super::fixture::*;
use crate::passes::resume::{self, NotOursNext};
use chrono::{Duration, TimeZone, Utc};
use common::flaky_checks::{self as fc, Kind, NotOurs, Observation, Record, Rerun, Touch};

const OLD_FIX: &str = "CI failed — attempting fix";
const WORKFLOW: &str = "E2E";

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 2, 12, 9, 0, 0).unwrap()
}

/// What the repository saw that week: two other pull requests whose E2E
/// failed and passed on one commit.
fn record() -> Record {
    let mut r = Record::default();
    for (pr, sha, days) in [(52, "aa11bb22cc33", 3), (57, "dd44ee55ff66", 1)] {
        assert!(r.observe(
            WORKFLOW,
            Observation {
                kind: Kind::PassedOnRerun,
                at: (now() - Duration::days(days)).to_rfc3339(),
                pr,
                sha: sha.to_string(),
                line: String::new(),
            },
        ));
    }
    r
}

/// The failing job's output: a timeout in a spec the change doesn't touch.
fn logs() -> String {
    [
        "Running 41 tests using 4 workers",
        "  1) [chromium] › e2e/checkout.spec.ts:88:5 › pays with a saved card",
        "    Test timeout of 30000ms exceeded.",
        "    at /home/runner/work/site/site/e2e/checkout.spec.ts:102:22",
        "  1 failed, 40 passed",
    ]
    .join("\n")
}

fn changed() -> Vec<String> {
    vec![
        "apps/web/src/components/card-block.tsx".to_string(),
        "apps/studio/schemas/card.ts".to_string(),
    ]
}

#[test]
fn the_flaky_check_is_rerun_and_no_code_changes_for_it() {
    let failing = vec![WORKFLOW.to_string()];
    let touch = fc::touch(&logs(), &changed());
    assert_eq!(
        touch,
        Touch::Elsewhere(vec!["e2e/checkout.spec.ts".to_string()])
    );
    let j = fc::judge(&failing, &[], &touch, None, &record(), 61, now());
    assert!(j.none_ours());
    assert!(matches!(&j.not_ours[0].1, NotOurs::Flaky(seen) if seen.len() == 2));

    // The re-run started: the run waits for it, no fix round.
    assert_eq!(
        resume::after_rerun_try(Some(&Rerun::Started), true),
        NotOursNext::Wait
    );
    let note = fc::section(&j, BASE, &Rerun::Started, &touch);
    assert!(note.contains("`E2E` has failed and then passed on the same commit of other pull requests (#52 at `aa11bb2`, #57 at `dd44ee5`)"));
    assert!(note.contains("`e2e/checkout.spec.ts`, which this pull request does not change"));
    assert!(note.contains("changed no code"));
    assert!(!note.contains(OLD_FIX));
}

#[test]
fn a_refused_rerun_is_said_with_the_permission_and_still_changes_no_code() {
    let refused =
        "GitHub API 403 Forbidden: {\"message\":\"Resource not accessible by integration\"}";
    assert!(fc::rerun_refused_for_permission(refused));
    assert_eq!(resume::rerun_result(false, true), Rerun::NoPermission);
    assert_eq!(resume::rerun_result(false, false), Rerun::NotActions);
    assert_eq!(resume::rerun_result(true, true), Rerun::Started);
    // With the record behind it the run finishes without a fix round.
    assert_eq!(
        resume::after_rerun_try(Some(&Rerun::NoPermission), true),
        NotOursNext::Finish
    );
    let touch = fc::touch(&logs(), &changed());
    let j = fc::judge(
        &[WORKFLOW.to_string()],
        &[],
        &touch,
        None,
        &record(),
        61,
        now(),
    );
    let note = fc::section(&j, BASE, &Rerun::NoPermission, &touch);
    assert!(note.contains("no Actions write permission"));
    assert!(note.contains("changed no code"));
    let low = note.to_lowercase();
    for phrase in crate::passes::ownership::HAND_OFF_PHRASES {
        assert!(!low.contains(phrase), "{phrase}");
    }
    // An infrastructure failure nothing could start again has no record
    // behind it: a fix round reads it, as before.
    assert_eq!(
        resume::after_rerun_try(Some(&Rerun::NoPermission), false),
        NotOursNext::Fix
    );
}

#[test]
fn broken_is_never_waved_through_as_flaky() {
    let failing = vec![WORKFLOW.to_string()];
    // The failure names a file the pull request changed: the change's,
    // whatever the record says.
    let own = "  1) e2e/cards.spec.ts:12:3\n    at apps/web/src/components/card-block.tsx:40:9";
    let touch = fc::touch(own, &changed());
    assert!(matches!(touch, Touch::Changed(_)));
    let j = fc::judge(&failing, &[], &touch, None, &record(), 61, now());
    assert_eq!(j.ours, failing);
    assert!(!j.none_ours());
    // No record on other pull requests: a failure that names no changed
    // file can still be caused by one.
    let touch = fc::touch(&logs(), &changed());
    let j = fc::judge(&failing, &[], &touch, None, &Record::default(), 61, now());
    assert_eq!(j.ours, failing);
    // This pull request's own flakes are not evidence about itself.
    let mut own_only = Record::default();
    for sha in ["s1", "s2", "s3"] {
        own_only.observe(
            WORKFLOW,
            Observation {
                kind: Kind::PassedOnRerun,
                at: now().to_rfc3339(),
                pr: 61,
                sha: sha.into(),
                line: String::new(),
            },
        );
    }
    assert!(!own_only.known_flaky(WORKFLOW, 61, now()));
    // It failed again on a head already re-run: a fix round reads it, told
    // what the record says and never to weaken the test.
    assert_eq!(resume::after_rerun_try(None, true), NotOursNext::Fix);
    let j = fc::judge(&failing, &[], &touch, None, &record(), 61, now());
    let told = fc::fixer_note(&j);
    assert!(told.contains("failed here again after a re-run"));
    assert!(told.contains("never weaken, skip or delete the test"));
}

#[test]
fn the_fix_path_judges_before_it_changes_code_and_records_what_it_learns() {
    let src = include_str!("../resume.rs");
    let run = fn_src(src, "\npub async fn run(");
    let judged = run
        .find("super::flaky::judge(")
        .expect("the failure is judged");
    let fix = run
        .find("lock_repair_on_pr(")
        .expect("the first thing that changes code for a failure");
    assert!(judged < fix, "the judgement comes before any change");
    let after = &run[judged..fix];
    assert!(after.contains("after_rerun_try(rerun.as_ref(), known_flaky)"));
    assert!(after.contains("super::flaky::note_on_pr("));
    // A re-run that started puts the run back to waiting for CI.
    assert!(after.contains("set_run_awaiting_ci("));
    // The record learns only from a head that failed and then passed.
    let learn = fn_src(src, "\nasync fn record_flaky_pass(");
    assert!(learn.contains("CiOutcome::PassedAfterRerun"));
    assert!(learn.contains("super::flaky::passed_after_rerun("));
    let rerun = fn_src(src, "\nasync fn try_rerun_once(");
    assert!(rerun.contains("super::flaky::remember_rerun("));
    assert!(rerun.contains("rerun_refused_for_permission("));
}
