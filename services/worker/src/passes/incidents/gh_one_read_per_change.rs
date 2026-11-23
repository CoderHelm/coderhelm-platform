//! gh. A busy afternoon of runs on one installation spent GitHub's whole
//! hourly budget twice in two hours (20:49 and 21:48 UTC): 6,033 and 6,066
//! counted requests in those clock hours against a limit of 5,000. The
//! runs' own steps made 70% of them (passes about 235 requests each, CI
//! fix rounds about 190), mostly the same reads asked again by code that
//! didn't know another part of the step had just asked: 78 places read a
//! pull request, 74 a branch's ref, 45 a diff. A run waiting on CI then
//! wrote "Waiting for GitHub's hourly limit; continues by itself at 22:02
//! UTC" four times in two minutes, and a review or a person's answer that
//! came in during that hour waited for the reset like everything else.
//!
//! Guarantee: a read the worker made before is asked again conditionally
//! (304, which GitHub doesn't count, while nothing changed) and a read
//! named by a full commit SHA isn't asked again at all; every counted
//! request is logged by caller and route, so the next busy hour says where
//! it went. A run's own steps (its passes, its CI wait, merge steps, a
//! rebuilt description) stop at the last tenth of the hour and wait for the
//! reset, which leaves that tenth to reviews and people's answers; sweeps
//! still stop at half. The same progress note written again within a
//! quarter of an hour stays one note, with the repeat counted.

use crate::clients::github_budget::{self as ledger};
use crate::clients::github_cache as cache;
use crate::passes::run_lists;
use common::github_budget::{self as budget, caller as who, Budget, Hold, Spend};

const SHA: &str = "224d831d874e7bca15314e10a6f75ac5a00c5812";
const BASE: &str = "f4395bfe0aa5748918dd3d0c1b2a3e4f5a6b7c8d";

/// The reads of a step that never change are kept for good; the ones
/// named by a branch, a number or a ref are asked conditionally.
#[test]
fn the_reads_of_a_step_are_asked_once_per_change() {
    let repo = "https://api.github.com/repos/acme/site";
    for never_changes in [
        format!("{repo}/commits/{SHA}"),
        format!("{repo}/compare/{BASE}...{SHA}"),
        format!("{repo}/git/trees/{SHA}?recursive=1"),
        format!("{repo}/contents/apps/web/src/app/sitemap.ts?ref={SHA}"),
    ] {
        assert!(cache::immutable(&never_changes), "{never_changes}");
    }
    for may_change in [
        format!("{repo}/pulls/1573"),
        format!("{repo}/git/ref/heads/coderhelm/cpm-8988"),
        format!("{repo}/commits/{SHA}/check-runs"),
        format!("{repo}/commits/{SHA}/status"),
        format!("{repo}/compare/main...coderhelm/cpm-8988"),
    ] {
        assert!(!cache::immutable(&may_change), "{may_change}");
    }
    // Kept per installation and media type: a diff is not the JSON.
    let pr = format!("{repo}/pulls/1573");
    assert_ne!(
        cache::key(124_828_479, &pr, "application/vnd.github+json"),
        cache::key(124_828_479, &pr, "application/vnd.github.diff")
    );
    // And the log line names what was asked, so a hot read shows itself.
    assert_eq!(
        ledger::route_of(
            "GET",
            &format!("{repo}/commits/{SHA}/check-runs?per_page=100")
        ),
        "GET /repos/:o/:r/commits/:sha/check-runs"
    );
}

/// The hour of the incident, replayed on the decision every request takes:
/// with under a tenth left, the run's steps wait for the reset while a
/// review and a person's answer still go; sweeps stopped long before.
#[test]
fn the_last_of_the_hour_is_kept_for_people() {
    let reset = 1_795_471_369;
    let now = reset - 600;
    let at = |remaining| {
        Some(Budget {
            remaining,
            limit: 5_000,
            reset,
        })
    };
    // 681 left (20:00 to 20:30 UTC): runs still go, sweeps don't.
    for step in [who::RUN, who::CI, who::MERGE, who::DESCRIPTION] {
        assert_eq!(budget::decide(step, at(681), None, 0, now), Spend::Go);
    }
    assert!(matches!(
        budget::decide(who::SWEEP_PR_WATCH, at(681), None, 0, now),
        Spend::Hold {
            why: Hold::ReservedForRuns,
            ..
        }
    ));
    // 480 left: the run's steps wait for the reset, people's work goes on.
    for step in [who::RUN, who::CI, who::MERGE, who::DESCRIPTION, who::TESTS] {
        assert_eq!(
            budget::decide(step, at(480), None, 0, now),
            Spend::Hold {
                why: Hold::ReservedForPeople,
                wait_secs: 600
            },
            "{step}"
        );
    }
    for person in [who::REVIEW, who::FIX_ROUND, who::CHAT, who::MCP, who::READY] {
        assert_eq!(
            budget::decide(person, at(1), None, 0, now),
            Spend::Go,
            "{person}"
        );
    }
    // The wait reads as GitHub's limit, so the run pauses and goes on
    // by itself at the reset.
    assert!(Hold::ReservedForPeople
        .as_str()
        .contains("kept for reviews and people's answers"));
    // Spent is spent: nobody sends until the reset.
    for anyone in [who::REVIEW, who::RUN] {
        assert!(matches!(
            budget::decide(anyone, at(0), Some(reset), 0, now),
            Spend::Hold {
                why: Hold::LimitSpent,
                ..
            }
        ));
    }
}

/// "Waiting for GitHub's hourly limit" written at 21:52:39, 21:53:35,
/// 21:53:54 and 21:54:18: the last three are repeats of the first.
#[test]
fn the_same_wait_note_is_one_note() {
    use chrono::TimeZone;
    let first = "2026-11-23T21:52:39.512391044+00:00";
    for (h, m, s) in [(21, 53, 35), (21, 53, 54), (21, 54, 18)] {
        let at = chrono::Utc.with_ymd_and_hms(2026, 11, 23, h, m, s).unwrap();
        assert!(first >= run_lists::repeat_cutoff(at).as_str());
    }
    // A quarter of an hour on, the same words are news again.
    let later = chrono::Utc
        .with_ymd_and_hms(2026, 11, 23, 22, 8, 0)
        .unwrap();
    assert!(first < run_lists::repeat_cutoff(later).as_str());
}
