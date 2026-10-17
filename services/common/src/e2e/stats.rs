//! Numbers derived from a test's history, for the dashboard and for ranking.

use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};

use super::detect::{flips, FLIP_WINDOW_DAYS};
use super::model::{Outcome, TestRecord};

fn at(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    pub runs_30d: u32,
    pub fail_rate_30d: f64,
    pub flake_rate_30d: f64,
    pub flips_14d: u32,
    pub prs_blocked_7d: u32,
    pub minutes_lost_7d: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
}

pub fn stats(r: &TestRecord, now: DateTime<Utc>) -> Stats {
    let since30 = now - Duration::days(30);
    let since7 = now - Duration::days(7);
    let recent: Vec<_> = r
        .history
        .iter()
        .filter(|e| {
            !e.bad_run && e.outcome != Outcome::Skipped && at(&e.at).is_some_and(|t| t >= since30)
        })
        .collect();
    let runs = recent.len() as u32;
    let failed = recent
        .iter()
        .filter(|e| e.outcome == Outcome::Failed)
        .count() as f64;
    let flaky = recent
        .iter()
        .filter(|e| e.outcome == Outcome::Flaky)
        .count() as f64;
    let mut durations: Vec<u64> = recent
        .iter()
        .flat_map(|e| e.attempts.iter())
        .filter(|a| a.status == super::model::AttemptStatus::Passed)
        .map(|a| a.duration_ms)
        .collect();
    durations.sort_unstable();
    let week: Vec<_> = recent
        .iter()
        .filter(|e| at(&e.at).is_some_and(|t| t >= since7))
        .collect();
    let mut prs: Vec<u64> = week
        .iter()
        .filter(|e| e.outcome == Outcome::Failed)
        .filter_map(|e| e.pr)
        .collect();
    prs.sort_unstable();
    prs.dedup();
    let lost_ms: u64 = week
        .iter()
        .flat_map(|e| e.attempts.iter())
        .filter(|a| a.status.is_failure())
        .map(|a| a.duration_ms)
        .sum();
    let rate = |n: f64| {
        if runs == 0 {
            0.0
        } else {
            (n / runs as f64 * 1000.0).round() / 1000.0
        }
    };
    Stats {
        runs_30d: runs,
        fail_rate_30d: rate(failed),
        flake_rate_30d: rate(flaky),
        flips_14d: flips(&r.history, now - Duration::days(FLIP_WINDOW_DAYS)) as u32,
        prs_blocked_7d: prs.len() as u32,
        minutes_lost_7d: (lost_ms as f64 / 60_000.0 * 10.0).round() / 10.0,
        p50_ms: percentile(&durations, 0.5),
        p95_ms: percentile(&durations, 0.95),
    }
}

/// The dashboard's `Test` shape.
pub fn summary(r: &TestRecord, now: DateTime<Utc>) -> Value {
    let s = stats(r, now);
    json!({
        "id": r.id,
        "repo": r.repo,
        "framework": r.framework.as_str(),
        "file": r.file,
        "title": r.title,
        "project": r.project,
        "tags": r.tags,
        "state": r.state().as_str(),
        "flags": r.flags(),
        "owner": r.owner,
        "cause": r.cause,
        "runs_30d": s.runs_30d,
        "fail_rate_30d": s.fail_rate_30d,
        "flake_rate_30d": s.flake_rate_30d,
        "flips_14d": s.flips_14d,
        "prs_blocked_7d": s.prs_blocked_7d,
        "minutes_lost_7d": s.minutes_lost_7d,
        "p50_ms": s.p50_ms,
        "p95_ms": s.p95_ms,
        "last_seen": r.last_seen,
        "quarantined_since": r.quarantined_since,
        "never_quarantine": r.never_quarantine,
        "ticket_url": r.ticket_url,
        "fix_pr_url": r.fix_pr_url,
        "fix_status": r.fix_status,
    })
}

/// Ranks tests by what they cost: PRs blocked, then minutes lost, then how
/// often they flip.
pub fn impact(v: &Value) -> (u64, u64, u64) {
    let n = |k: &str| v[k].as_f64().unwrap_or(0.0);
    (
        n("prs_blocked_7d") as u64,
        (n("minutes_lost_7d") * 10.0) as u64,
        n("flips_14d") as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::{Attempt, AttemptStatus, Framework, HistoryEntry, State};

    fn att(status: AttemptStatus, ms: u64) -> Attempt {
        Attempt {
            retry: 0,
            status,
            duration_ms: ms,
            error: None,
            trace_url: None,
            video_url: None,
            screenshot_url: None,
        }
    }

    #[test]
    fn stats_count_the_right_window() {
        let now = DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let e = |d: i64, outcome: Outcome, pr: Option<u64>, attempts: Vec<Attempt>| HistoryEntry {
            run_key: format!("r{d}{outcome:?}"),
            sha: format!("s{d}"),
            branch: "main".into(),
            pr,
            at: (now - Duration::days(d)).to_rfc3339(),
            outcome,
            bad_run: false,
            attempts,
        };
        let r = TestRecord {
            id: "x".into(),
            repo: "acme/web".into(),
            framework: Framework::Playwright,
            file: "a".into(),
            title: "t".into(),
            project: None,
            tags: vec![],
            health: State::Flaky,
            quarantined: false,
            quarantined_since: None,
            quarantined_by: None,
            never_quarantine: false,
            owner: None,
            cause: None,
            last_flip_at: None,
            runs_since_flip: 0,
            fix_merged_at: None,
            ticket_url: None,
            fix_pr_url: None,
            fix_status: None,
            fix_branch: None,
            fix_phase_at: None,
            workflow_path: None,
            last_seen: String::new(),
            history: vec![
                e(40, Outcome::Failed, Some(1), vec![]),
                e(
                    10,
                    Outcome::Passed,
                    None,
                    vec![att(AttemptStatus::Passed, 1000)],
                ),
                e(
                    3,
                    Outcome::Failed,
                    Some(7),
                    vec![
                        att(AttemptStatus::Failed, 60_000),
                        att(AttemptStatus::Failed, 30_000),
                    ],
                ),
                e(
                    2,
                    Outcome::Flaky,
                    Some(8),
                    vec![
                        att(AttemptStatus::Failed, 6_000),
                        att(AttemptStatus::Passed, 3000),
                    ],
                ),
                e(1, Outcome::Failed, Some(7), vec![]),
            ],
            events: vec![],
        };
        let s = stats(&r, now);
        assert_eq!(s.runs_30d, 4);
        assert_eq!(s.fail_rate_30d, 0.5);
        assert_eq!(s.flake_rate_30d, 0.25);
        assert_eq!(s.prs_blocked_7d, 1, "PR 7 twice, PR 8 was only flaky");
        assert_eq!(s.minutes_lost_7d, 1.6);
        assert_eq!(s.p50_ms, 3000);
        let v = summary(&r, now);
        assert_eq!(v["state"], "flaky");
        assert_eq!(v["runs_30d"], 4);
        assert!(impact(&v) > (0, 0, 0));
    }
}
