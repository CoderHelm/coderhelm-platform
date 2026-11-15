//! bj. Two repositories' Security pages read hundreds of findings "in a
//! pull request" for two days, through two releases meant to give them
//! back, and they were taken for stuck. The logs could not say otherwise:
//! the sweep that compares each finding with GitHub wrote its line only
//! when it changed something, so a sweep that read every pull request and
//! found each one still open left no trace, and neither did one that never
//! ran. And in the hours GitHub's budget ran low, the sweep did stop: the
//! budget held it before its first read, three sweeps in a row read
//! nothing, and a pull request closed in those hours would have kept its
//! findings until the budget came back. What GitHub said in the end: both
//! pull requests were open (one repository's findings had moved from a
//! closed pull request to a new one, which is why its count never moved).
//!
//! Guarantee: every sweep leaves one line with what it read and what
//! GitHub said (open, merged, closed, unanswered), what it changed and
//! what it left. A held budget slows the release, never stops it: a few
//! reads each sweep go on a small allowance of their own, in a turn that
//! reaches every pull request within a few sweeps. A pull request still
//! open keeps its findings; a closed one gives them back; a merged one
//! fixes them.

use chrono::{DateTime, Utc};
use common::github_budget::{self as budget, caller as who};
use common::security_fixes::{Finding, FindingState, Status};
use common::security_release::{self as rel, PrNow};
use std::sync::atomic::Ordering;

use crate::passes::security_fixes::release::{fake::*, reconcile, Scope};

const WEB: &str = "acme/web";
const API: &str = "acme/api";

fn now() -> DateTime<Utc> {
    at("2026-11-14T18:18:52Z")
}

/// The two repositories as they stood: one bundle pull request each, with
/// a few hundred findings in it.
fn the_findings() -> Vec<(Finding, FindingState)> {
    let web = (0..300).map(|i| in_pr(&format!("w{i:03}"), WEB, 41, "SEC-AAAAAAAAAAAA"));
    let api = (0..200).map(|i| in_pr(&format!("a{i:03}"), API, 42, "SEC-BBBBBBBBBBBB"));
    web.chain(api).collect()
}

fn in_pr_count(all: &[(Finding, FindingState)], repo: &str) -> usize {
    all.iter()
        .filter(|(f, _)| f.repo == repo && f.status == Status::PrOpen)
        .count()
}

#[tokio::test]
async fn open_pull_requests_keep_their_findings_and_the_sweep_says_so() {
    let mut all = the_findings();
    let github = Fake::default();
    github.pr(WEB, 41, PrNow::Open);
    github.pr(API, 42, PrNow::Open);
    let r = reconcile(&github, &mut all, &Scope::All, now()).await;
    // Two reads, both open, nothing to change: the counts stand, and the
    // line the sweep leaves says why.
    assert_eq!((r.prs_read, r.prs_open, r.changed(), r.left), (2, 2, 0, 0));
    assert_eq!((in_pr_count(&all, WEB), in_pr_count(&all, API)), (300, 200));
    assert_eq!(github.saves(), 0);
}

#[tokio::test]
async fn a_held_budget_still_gives_back_a_closed_pull_requests_findings() {
    let mut all = the_findings();
    let github = Fake::default();
    // The hour's budget is below the runs' share: the sweep is held.
    github.held.store(true, Ordering::Relaxed);
    github.pr(WEB, 41, closed_by("octo-admin"));
    github.pr(API, 42, PrNow::Merged);
    let r = reconcile(&github, &mut all, &Scope::All, now()).await;
    assert_eq!((r.prs_read, r.floor_reads, r.left), (2, 2, 0));
    assert_eq!((r.prs_closed, r.prs_merged), (1, 1));
    assert_eq!((r.released, r.fixed), (300, 200));
    assert!(all
        .iter()
        .filter(|(f, _)| f.repo == WEB)
        .all(|(f, st)| f.status == Status::Open && st.ticket.is_none()));
    assert!(all
        .iter()
        .filter(|(f, _)| f.repo == API)
        .all(|(f, _)| f.status == Status::Fixed));
}

#[test]
fn the_floor_spends_inside_the_runs_share_on_a_small_cap_of_its_own() {
    let t = 1_794_680_332;
    let low = Some(budget::Budget {
        remaining: 2_350,
        limit: 5_000,
        reset: t + 1_800,
    });
    // What the logs showed: the sweep held for the runs' share...
    assert!(matches!(
        budget::decide(who::SWEEP_SECURITY, low, None, 0, t),
        budget::Spend::Hold {
            why: budget::Hold::ReservedForRuns,
            ..
        }
    ));
    // ...and the floor still sent, a few an hour.
    assert_eq!(
        budget::decide(who::SWEEP_SECURITY_FLOOR, low, None, 0, t),
        budget::Spend::Go
    );
    let cap = budget::hourly_cap(who::SWEEP_SECURITY_FLOOR).unwrap();
    assert!(cap as usize >= rel::FLOOR_READS_PER_SWEEP * 3 * 3, "{cap}");
}

#[test]
fn the_release_reads_on_the_floor_and_every_sweep_leaves_its_line() {
    let src = include_str!("../security_release.rs");
    let code = &src[..src.find("#[cfg(test)]").unwrap()];
    // The held check no longer ends the sweep before its first read.
    assert!(!code.contains("|| self.cx.gh.held()"));
    assert!(code.contains("on_floor(&self.cx.gh, reads.pr_now("));
    assert!(code.contains("on_floor(&self.cx.gh, reads.open_pr("));
    assert!(code.contains("*scope == Scope::All || report.changed() > 0"));
    let infra = include_str!("../infra_findings.rs");
    assert!(infra.contains("rel::on_floor(self.reads.gh, self.reads.pr_now("));
}
