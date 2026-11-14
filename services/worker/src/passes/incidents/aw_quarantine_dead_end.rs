//! aw. Sixteen end-to-end tests of one team sat in quarantine, thirteen for
//! more than two days and some for over three weeks. Each row had one
//! button, "Release from quarantine", and one row a link to a fix pull
//! request. Nothing was happening to the rest and nothing could be done
//! from the page:
//!
//! - the week's two automatic fixes went to whichever flaky test a
//!   default-branch run listed first, and only to tests that had cost CI
//!   time that week, which a quarantined test never does;
//! - a test whose fix had failed once was never looked at again;
//! - a person's "Fix this test" was marked by the gateway and then dropped
//!   by the worker as a duplicate of that mark, and was refused outright for
//!   a quarantined test whose health had gone back to healthy;
//! - one test had failed three times and then passed about thirty-five runs
//!   in a row on several pull requests and branches. It stayed quarantined,
//!   under "Likely cause: Environment ... passed on retry in 0 of the last
//!   30 runs", a sentence its history had disproved hours before.
//!
//! Guarantee: a quarantined test always shows where it stands and what
//! happens next (`common::e2e::quarantine::standing`): the fix pull request
//! with its state, the run that is working, or the actions (fix, remove)
//! with the one reason Coderhelm has not fixed it by itself. A test that
//! proved healthy leaves by itself with the evidence, and its ticket is
//! closed. A cause the runs disprove is marked out of date. Automatic fixes
//! go to the longest quarantined first, a fix that did not hold is tried
//! again after a week, and the weekly number is said with the day it starts
//! again. A person's request is never counted and never dropped. A removal
//! is a pull request for the team: Coderhelm proposes one only under its
//! rule and never merges it.

use chrono::{DateTime, Duration, Utc};
use common::e2e::detect::apply_run;
use common::e2e::model::{
    Cause, Framework, HistoryEntry, Outcome, PrSeen, Removal, State, TestRecord,
};
use common::e2e::quarantine::{self, Budget, Peer};
use common::e2e::stats::{stats, summary};
use serde_json::Value;
use std::collections::HashMap;

use super::fixture::{BASE, OWNER, REPO};
use crate::passes::{e2e, quarantine_work};

const OLD_REFUSAL: &str = "Only flaky tests get a fix PR";
const OLD_DROP: &str = "Not starting another";

/// Thursday morning, the day the page was looked at.
fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-12T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn repo() -> String {
    format!("{OWNER}/{REPO}")
}

fn quarantined(id: &str, title: &str, days: i64) -> TestRecord {
    let since = now() - Duration::days(days);
    TestRecord {
        id: id.into(),
        repo: repo(),
        framework: Framework::Playwright,
        file: "e2e/cards.spec.ts".into(),
        title: title.into(),
        project: Some("chromium".into()),
        tags: vec!["@cards".into()],
        health: State::Flaky,
        quarantined: true,
        quarantined_since: Some(since.to_rfc3339()),
        quarantined_by: Some("auto".into()),
        never_quarantine: false,
        owner: None,
        cause: None,
        last_flip_at: Some(since.to_rfc3339()),
        runs_since_flip: 0,
        fix_merged_at: None,
        ticket_url: None,
        fix_pr_url: None,
        fix_status: None,
        fix_branch: None,
        fix_phase_at: None,
        workflow_path: Some(".github/workflows/e2e.yml".into()),
        care: Default::default(),
        last_seen: now().to_rfc3339(),
        history: vec![],
        events: vec![],
    }
}

fn run(n: u32, at: DateTime<Utc>, outcome: Outcome, pr: Option<u64>) -> HistoryEntry {
    HistoryEntry {
        run_key: format!("{n}-1"),
        sha: format!("c0ffee{n:02}"),
        branch: pr.map_or(BASE.to_string(), |p| format!("feature/{p}")),
        pr,
        at: at.to_rfc3339(),
        outcome,
        bad_run: false,
        attempts: vec![],
    }
}

/// A test that still fails now and then: a failure and a retry-pass in the
/// last two days.
fn still_flaky(id: &str, title: &str, days: i64) -> TestRecord {
    let mut r = quarantined(id, title, days);
    r.history = vec![
        run(1, now() - Duration::hours(40), Outcome::Failed, Some(71)),
        run(2, now() - Duration::hours(30), Outcome::Passed, None),
        run(3, now() - Duration::hours(20), Outcome::Flaky, Some(72)),
        run(4, now() - Duration::hours(3), Outcome::Passed, None),
    ];
    r.last_seen = r.history.last().unwrap().at.clone();
    r
}

fn rows(records: &[&TestRecord]) -> Vec<Value> {
    records.iter().map(|r| summary(r, now())).collect()
}

#[test]
fn the_test_that_went_back_to_passing_leaves_by_itself_and_its_cause_is_withdrawn() {
    let mut r = quarantined("t-healthy", "cards › opens the card editor", 1);
    r.ticket_url = Some(format!("https://github.com/{OWNER}/{REPO}/issues/88"));
    r.cause = Some(Cause {
        label: "environment".into(),
        evidence: "The failure is deterministic and tied to the preview setup: passed on retry in 0 of the last 30 runs.".into(),
        depends_on: None,
        independent: None,
        at: Some((now() - Duration::hours(23)).to_rfc3339()),
        out_of_date: None,
    });
    let start = now() - Duration::hours(7);
    for i in 0..3 {
        r.history.push(run(
            i,
            start - Duration::minutes(30 - i as i64),
            Outcome::Failed,
            Some(60),
        ));
    }
    // Thirty-five passes over the next hours, recorded one run at a time,
    // on four pull requests and the default branch.
    let mut released_on = None;
    let mut stale_on = None;
    for i in 0..35u32 {
        let at = start + Duration::minutes(10 * i as i64);
        let pr = match i % 5 {
            0 => None,
            k => Some(60 + k as u64),
        };
        let applied = apply_run(
            &mut r,
            run(10 + i, at, Outcome::Passed, pr),
            at + Duration::minutes(4),
            BASE,
            true,
        );
        if applied.cause_went_stale {
            stale_on = Some(i + 1);
        }
        if applied.released.is_some() {
            released_on = Some(i + 1);
        }
    }
    // Ten clean passes disprove a cause that fails every run while it
    // lasts; twenty over more than two hours release the test.
    assert_eq!(stale_on, Some(10));
    assert_eq!(released_on, Some(20));
    assert!(!r.quarantined);
    assert_eq!(r.health, State::Healthy);
    let rel = r.care.release.clone().unwrap();
    assert!(rel.reason.starts_with(
        "Left quarantine by itself: 20 passes in a row with no failure and no retry, on 5 pull requests and branches, over 3 hours, since it last failed on 12 November 2026."
    ));
    let cause = r.cause.as_ref().unwrap();
    assert!(cause.out_of_date.as_deref().unwrap().starts_with(
        "Out of date: this was worked out while the test was failing. Since its last failure on 12 November 2026 it has passed 10 runs in a row on 5 pull requests and branches"
    ));
    // The history says why, and the fifteen passes after it change nothing.
    let left: Vec<_> = r
        .events
        .iter()
        .filter(|e| e.from == "quarantined" && e.to == "healthy")
        .collect();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].reason, rel.reason);
    // The ticket opened for it is this repository's, so it is closed.
    assert_eq!(
        quarantine_work::own_issue(r.ticket_url.as_deref().unwrap(), OWNER, REPO),
        Some(88)
    );
    // The row is gone from the quarantine list, and the page shows why.
    let s = summary(&r, now());
    assert_eq!(s["state"], "healthy");
    assert!(s["quarantine"].is_null());
    assert_eq!(s["release"]["passes"], 20);
}

#[test]
fn one_lucky_branch_releases_nothing() {
    let mut r = quarantined("t-branch", "cards › saves a draft", 1);
    let start = now() - Duration::hours(8);
    for i in 0..40u32 {
        let at = start + Duration::minutes(10 * i as i64);
        let applied = apply_run(
            &mut r,
            run(i, at, Outcome::Passed, Some(64)),
            at + Duration::minutes(4),
            BASE,
            true,
        );
        assert!(applied.released.is_none());
    }
    assert!(r.quarantined);
    let st = quarantine::standing(&r, &stats(&r, now()), now()).unwrap();
    assert_eq!(
        st.progress.line,
        "20 of 20 passes needed, across 1 pull request or branch; a second pull request or branch is needed"
    );
}

#[test]
fn a_persons_fix_request_starts_and_is_never_counted() {
    // Quarantined three weeks, health back to healthy after a quiet week:
    // the page used to refuse the fix, and the worker dropped the rest.
    let mut r = still_flaky("t-asked", "cards › deletes a card", 23);
    r.health = State::Healthy;
    assert_eq!(quarantine::fix_blocked(&r, now()), None);
    // What the gateway writes before it queues the request.
    r.fix_status = Some("reproducing".into());
    r.fix_phase_at = None;
    r.care.fix_requested_at = Some(now().to_rfc3339());
    // The worker reads that mark as the request, not as a fix under way.
    assert_eq!(
        e2e::under_way(r.fix_status.as_deref(), r.fix_phase_at.as_deref(), now()),
        None
    );
    // A second click meanwhile is told it is in progress.
    assert_eq!(
        quarantine::fix_blocked(&r, now() + Duration::minutes(2)).unwrap(),
        "A fix for this test is already in progress."
    );
    let gateway = include_str!("../../../../gateway/src/routes/tests.rs");
    assert!(!gateway.contains(OLD_REFUSAL));
    assert!(gateway.contains("r.fix_phase_at = None;"));
    // The week's number is only ever taken by the automatic paths.
    let e2e_src = include_str!("../e2e.rs");
    let code = &e2e_src[..e2e_src.find("#[cfg(test)]").unwrap()];
    assert!(!code.contains("claim_auto_fix("));
    assert!(code.contains(OLD_DROP), "a real duplicate is still dropped");
}

#[test]
fn automatic_fixes_go_to_the_longest_quarantined_and_the_cap_is_said() {
    // Three weeks with one fix that did not hold, twenty days ago.
    let mut oldest = still_flaky("t-old", "cards › reorders cards", 24);
    oldest.fix_pr_url = Some(format!("https://github.com/{OWNER}/{REPO}/pull/31"));
    let e = quarantine::fix_failed(
        &mut oldest,
        now() - Duration::days(20),
        "Fix didn't hold: failed 3 of 20 repeat runs.",
    );
    oldest.events.push(e);
    let newer = still_flaky("t-new", "cards › filters by tag", 3);
    let newest = still_flaky("t-newest", "cards › searches", 1);
    let list = rows(&[&newest, &oldest, &newer]);
    let eligible: Vec<Value> = list
        .iter()
        .filter(|v| v["auto_fix"] == "eligible")
        .cloned()
        .collect();
    let order: Vec<&str> = quarantine::auto_fix_order(&eligible)
        .iter()
        .map(|v| v["id"].as_str().unwrap())
        .collect();
    assert_eq!(order, vec!["t-old", "t-new", "t-newest"]);
    // The row with the fix that did not hold says so, links to it, and
    // offers the actions again.
    let st = quarantine::standing(&oldest, &stats(&oldest, now()), now()).unwrap();
    assert_eq!(st.state, "fix_failed");
    assert_eq!(
        st.attempt.as_deref(),
        Some("Fix didn't hold: failed 3 of 20 repeat runs.")
    );
    assert!(st.attempt_link.unwrap().target.ends_with("/pull/31"));
    assert_eq!(st.actions, vec!["fix", "remove", "release"]);

    // The week's two are used: every waiting row says the number and the
    // day, not nothing.
    let mut list = list;
    let spent = Budget {
        repo: repo(),
        on: true,
        cap: 2,
        used: 2,
        resumes_at: quarantine::week_resets(now()).to_rfc3339(),
    };
    quarantine::overlay(&mut list, &HashMap::from([(repo(), spent)]));
    for row in &list {
        assert_eq!(
            row["quarantine"]["why"],
            "Automatic fixes are at this week's limit of 2 for this repository. They resume on Monday 16 November."
        );
    }
    assert_eq!(quarantine::week_key(now()), "2026-W46");
}

#[test]
fn no_row_is_a_dead_end() {
    let url = format!("https://github.com/{OWNER}/{REPO}/pull/31");
    for fix in [
        None,
        Some("reproducing"),
        Some("fixing"),
        Some("burn_in"),
        Some("opened"),
        Some("merged"),
        Some("failed"),
    ] {
        for removal in [
            None,
            Some("running"),
            Some("opened"),
            Some("declined"),
            Some("failed"),
        ] {
            let mut r = still_flaky("t-any", "cards › any", 9);
            r.fix_status = fix.map(str::to_string);
            if fix.is_some() {
                r.fix_pr_url = Some(url.clone());
                r.fix_phase_at = Some(now().to_rfc3339());
                r.care.fix_run_id = Some("01JQRUNFIX".into());
            }
            r.care.removal = removal.map(|status| Removal {
                status: status.into(),
                by: "dana@example.com".into(),
                at: now().to_rfc3339(),
                run_id: Some("01JQRUNREMOVE".into()),
                pr: (status != "running").then(|| PrSeen {
                    url: format!("https://github.com/{OWNER}/{REPO}/pull/44"),
                    state: "open".into(),
                    ..Default::default()
                }),
                ..Default::default()
            });
            let st = quarantine::standing(&r, &stats(&r, now()), now()).unwrap();
            let case = format!("fix {fix:?}, removal {removal:?}: {st:?}");
            assert!(!st.headline.is_empty(), "{case}");
            assert!(st.actions.contains(&"release".to_string()), "{case}");
            let acts = st.actions.contains(&"fix".to_string())
                && st.actions.contains(&"remove".to_string());
            // Work in flight is linked; anything else can be acted on,
            // with the reason nothing happened by itself.
            assert!(st.link.is_some() || acts, "{case}");
            if acts {
                assert!(
                    !st.why.is_empty() || st.hold == "eligible",
                    "an idle row with no reason: {case}"
                );
            }
        }
    }
}

#[test]
fn a_removal_is_proposed_only_under_the_rule_and_never_merged() {
    let peers = vec![Peer {
        title: "cards › opens the card editor".into(),
        file: "e2e/cards.spec.ts".into(),
        tags: vec!["@cards".into()],
        healthy: true,
    }];
    let mut r = still_flaky("t-remove", "cards › drags a card between lists", 34);
    assert_eq!(
        quarantine::may_propose_removal(&r, now(), &peers).unwrap_err(),
        "0 fixes did not hold, under the 2 needed"
    );
    for days in [25, 11] {
        let e = quarantine::fix_failed(
            &mut r,
            now() - Duration::days(days),
            "Fix didn't hold: failed 2 of 20 repeat runs.",
        );
        r.events.push(e);
    }
    assert_eq!(quarantine::may_propose_removal(&r, now(), &peers), Ok(()));
    // With nothing else covering it, Coderhelm does not propose it; a
    // person may still ask, and the description says what is lost.
    assert!(quarantine::may_propose_removal(&r, now(), &[]).is_err());
    assert_eq!(quarantine::removal_blocked(&r, now()), None);
    let evidence = quarantine::removal_evidence(&r, &stats(&r, now()), now(), &[]);
    assert!(evidence.last().unwrap().starts_with(
        "No healthy test shares its file or tags: removing it loses the only end-to-end check of"
    ));

    let evidence = quarantine::removal_evidence(&r, &stats(&r, now()), now(), &peers);
    let ticket = quarantine::removal_ticket(&r, &evidence, "coderhelm");
    assert!(ticket.contains("Delete this one test case only."));
    assert!(ticket.contains("Do not merge this pull request."));
    assert!(ticket.contains("- Quarantined since 9 October 2026 (34 days), automatically."));
    assert!(ticket.contains(
        "- 2 fix attempts did not hold; the last: Fix didn't hold: failed 2 of 20 repeat runs."
    ));
    assert!(ticket.contains(
        "- Still covering this area: `cards › opens the card editor` (same tag @cards)."
    ));
    // The removal's run is not held to the fix rules, which refuse a
    // deleted test; its branch is told apart from a fix's.
    assert_eq!(
        quarantine_work::removal_test_id("coderhelm/e2e-rm-t-remove"),
        Some("t-remove")
    );
    let fix_rules = include_str!("../test_fix.rs");
    assert!(fix_rules.contains("starts_with(common::e2e::quarantine::REMOVE_TICKET_PREFIX)"));
    // Nothing in the pass merges, and the merge gate is told to leave the
    // pull request before its description is written.
    let pass = include_str!("../quarantine_work.rs");
    let code = &pass[..pass.find("#[cfg(test)]").unwrap()];
    assert!(!code.contains("merge_pull_request("));
    assert!(code.contains("c.no_merge = true;"));

    // Merged by the team: the row goes, and the record names who approved.
    r.care.removal = Some(Removal {
        status: "opened".into(),
        by: "coderhelm".into(),
        at: now().to_rfc3339(),
        pr: Some(PrSeen {
            url: format!("https://github.com/{OWNER}/{REPO}/pull/52"),
            state: "open".into(),
            ..Default::default()
        }),
        evidence,
        ..Default::default()
    });
    let reviews = serde_json::json!([
        { "user": { "login": "robin-lead" }, "state": "APPROVED" },
        { "user": { "login": "sam-reviewer" }, "state": "COMMENTED" }
    ]);
    quarantine::removed(
        &mut r,
        now(),
        quarantine_work::approvers(&reviews),
        "robin-lead",
    );
    let s = summary(&r, now());
    assert_eq!(s["removed"], true);
    assert!(s["quarantine"].is_null());
    assert_eq!(
        s["removal"]["approved_by"],
        serde_json::json!(["robin-lead"])
    );
    assert_eq!(
        r.events.last().unwrap().reason,
        format!("Removed by https://github.com/{OWNER}/{REPO}/pull/52, approved by robin-lead, merged by robin-lead.")
    );
}
