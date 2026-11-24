//! gh. A busy afternoon of runs on one installation spent GitHub's whole
//! hourly budget more than once, with more counted requests in those clock
//! hours than the limit allows. The runs' own steps made most of them
//! (each pass and each CI fix round made hundreds), mostly the same reads
//! asked again by code that didn't know another part of the step had just
//! asked: dozens of places read a pull request, a branch's ref or a diff.
//! A run waiting on CI then wrote "Waiting for GitHub's hourly limit;
//! continues by itself at ..." several times in two minutes, and a review
//! or a person's answer that came in during that hour waited for the reset
//! like everything else.
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

const SHA: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
const BASE: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0aabbccdd";

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
        format!("{repo}/pulls/42"),
        format!("{repo}/git/ref/heads/coderhelm/abc-123"),
        format!("{repo}/commits/{SHA}/check-runs"),
        format!("{repo}/commits/{SHA}/status"),
        format!("{repo}/compare/main...coderhelm/abc-123"),
    ] {
        assert!(!cache::immutable(&may_change), "{may_change}");
    }
    // Kept per installation and media type: a diff is not the JSON.
    let pr = format!("{repo}/pulls/42");
    assert_ne!(
        cache::key(1_000_001, &pr, "application/vnd.github+json"),
        cache::key(1_000_001, &pr, "application/vnd.github.diff")
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
    let reset = 1_900_000_000;
    let now = reset - 600;
    let at = |remaining| {
        Some(Budget {
            remaining,
            limit: 5_000,
            reset,
        })
    };
    // 700 left: runs still go, sweeps don't.
    for step in [who::RUN, who::CI, who::MERGE, who::DESCRIPTION] {
        assert_eq!(budget::decide(step, at(700), None, 0, now), Spend::Go);
    }
    assert!(matches!(
        budget::decide(who::SWEEP_PR_WATCH, at(700), None, 0, now),
        Spend::Hold {
            why: Hold::ReservedForRuns,
            ..
        }
    ));
    // 450 left: the run's steps wait for the reset, people's work goes on.
    for step in [who::RUN, who::CI, who::MERGE, who::DESCRIPTION, who::TESTS] {
        assert_eq!(
            budget::decide(step, at(450), None, 0, now),
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

/// "Waiting for GitHub's hourly limit" written at 14:52:39, 14:53:35,
/// 14:53:54 and 14:54:18: the last three are repeats of the first.
#[test]
fn the_same_wait_note_is_one_note() {
    use chrono::TimeZone;
    let first = "2030-03-12T14:52:39.512391044+00:00";
    for (h, m, s) in [(14, 53, 35), (14, 53, 54), (14, 54, 18)] {
        let at = chrono::Utc.with_ymd_and_hms(2030, 3, 12, h, m, s).unwrap();
        assert!(first >= run_lists::repeat_cutoff(at).as_str());
    }
    // A quarter of an hour on, the same words are news again.
    let later = chrono::Utc.with_ymd_and_hms(2030, 3, 12, 15, 8, 0).unwrap();
    assert!(first < run_lists::repeat_cutoff(later).as_str());
}
