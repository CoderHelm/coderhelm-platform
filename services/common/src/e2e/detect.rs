//! Decides whether a test is healthy, flaky, broken or fixed. Pure functions:
//! everything they need is passed in, including the time.
//!
//! The rules, with their starting values:
//!
//! | State | Rule |
//! |---|---|
//! | Flaky | Passed and failed on the same commit twice in 14 days (a retry, a re-run of the job, or the same commit on several branches), or a failure rate on the default branch that is likely above 2% (at least 20 runs, 90% confidence) |
//! | Broken | Fails in at least 8 of its last 10 runs on the default branch, across at least 2 commits, with no pass on any of those commits |
//! | Bad run | More than 20% of a job's tests fail together (at least 3), or the job died. Those failures count against the run, not the tests |
//! | Back to healthy | No flip for 7 days or 100 runs |
//! | Fixed | 30 days with no flake after a fix merged |
//!
//! Broken outranks flaky, and a broken test is never quarantined: it is a real
//! regression, not noise.

use chrono::{DateTime, Duration, Utc};

use super::model::{HistoryEntry, Outcome, State, StateEvent, TestRecord};

pub const FLIP_WINDOW_DAYS: i64 = 14;
pub const FLIPS_FOR_FLAKY: usize = 2;
pub const RATE_THRESHOLD: f64 = 0.02;
pub const RATE_CONFIDENCE: f64 = 0.9;
pub const RATE_MIN_RUNS: usize = 20;
pub const RATE_WINDOW_DAYS: i64 = 30;
pub const BROKEN_WINDOW: usize = 10;
pub const BROKEN_FAILS: usize = 8;
pub const BROKEN_MIN_SHAS: usize = 2;
pub const RESOLVE_DAYS: i64 = 7;
pub const RESOLVE_RUNS: u32 = 100;
pub const FIXED_DAYS: i64 = 30;
/// History kept per test; only the most recent [`DETAILED_HISTORY`] keep their
/// attempts.
pub const MAX_HISTORY: usize = 120;
pub const DETAILED_HISTORY: usize = 50;

fn at(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// Whether a run broke as a whole, so its failures say nothing about tests.
pub fn is_bad_run(ran: usize, failed: usize, job_died: bool) -> bool {
    job_died || (failed >= 3 && failed * 5 > ran)
}

fn counts(e: &HistoryEntry) -> bool {
    !e.bad_run && e.outcome != Outcome::Skipped
}

/// Commits in the window on which the test both passed and failed.
pub fn flips(history: &[HistoryEntry], since: DateTime<Utc>) -> usize {
    use std::collections::HashMap;
    let mut by_sha: HashMap<&str, (bool, bool)> = HashMap::new();
    for e in history.iter().filter(|e| counts(e)) {
        if at(&e.at).is_some_and(|t| t < since) {
            continue;
        }
        let entry = by_sha.entry(e.sha.as_str()).or_default();
        match e.outcome {
            Outcome::Flaky => *entry = (true, true),
            Outcome::Passed => entry.0 = true,
            Outcome::Failed => entry.1 = true,
            Outcome::Skipped => {}
        }
    }
    by_sha.values().filter(|(p, f)| *p && *f).count()
}

/// Whether the newest entry is itself a flip: flaky on its own, or the
/// opposite of an earlier result for the same commit.
pub fn is_flip(history: &[HistoryEntry], newest: &HistoryEntry) -> bool {
    if !counts(newest) {
        return false;
    }
    if newest.outcome == Outcome::Flaky {
        return true;
    }
    history
        .iter()
        .filter(|e| counts(e) && e.sha == newest.sha && e.run_key != newest.run_key)
        .any(|e| {
            matches!(
                (e.outcome, newest.outcome),
                (Outcome::Passed, Outcome::Failed)
                    | (Outcome::Failed, Outcome::Passed)
                    | (Outcome::Flaky, _)
            )
        })
}

fn ln_gamma(x: f64) -> f64 {
    // Lanczos approximation (g = 7, n = 9).
    const C: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        let pi = std::f64::consts::PI;
        return (pi / (pi * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut a = C[0];
    let t = x + 7.5;
    for (i, c) in C.iter().enumerate().skip(1) {
        a += c / (x + i as f64);
    }
    0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
}

fn beta_cf(a: f64, b: f64, x: f64) -> f64 {
    let (fpmin, eps) = (1e-300, 1e-12);
    let (qab, qap, qam) = (a + b, a + 1.0, a - 1.0);
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < fpmin {
        d = fpmin;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..300 {
        let m = m as f64;
        let m2 = 2.0 * m;
        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < fpmin {
            d = fpmin;
        }
        c = 1.0 + aa / c;
        if c.abs() < fpmin {
            c = fpmin;
        }
        d = 1.0 / d;
        h *= d * c;
        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < fpmin {
            d = fpmin;
        }
        c = 1.0 + aa / c;
        if c.abs() < fpmin {
            c = fpmin;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < eps {
            break;
        }
    }
    h
}

/// The regularized incomplete beta function I_x(a, b).
fn inc_beta(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let bt = (ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln()).exp();
    if x < (a + 1.0) / (a + b + 2.0) {
        bt * beta_cf(a, b, x) / a
    } else {
        1.0 - bt * beta_cf(b, a, 1.0 - x) / b
    }
}

/// How likely the true failure rate is above `threshold`, given `fails` out of
/// `runs`, starting from a flat prior (Beta(1, 1)).
pub fn prob_rate_above(fails: usize, runs: usize, threshold: f64) -> f64 {
    let a = 1.0 + fails as f64;
    let b = 1.0 + (runs.saturating_sub(fails)) as f64;
    1.0 - inc_beta(a, b, threshold)
}

/// Whether the test fails steadily on the default branch.
pub fn is_broken(history: &[HistoryEntry], default_branch: &str) -> bool {
    let main: Vec<&HistoryEntry> = history
        .iter()
        .rev()
        .filter(|e| counts(e) && e.branch == default_branch)
        .take(BROKEN_WINDOW)
        .collect();
    if main.len() < BROKEN_WINDOW.min(BROKEN_FAILS) {
        return false;
    }
    let failing: Vec<&&HistoryEntry> = main
        .iter()
        .filter(|e| e.outcome == Outcome::Failed)
        .collect();
    if failing.len() < BROKEN_FAILS.min(main.len()) {
        return false;
    }
    let mut shas: Vec<&str> = failing.iter().map(|e| e.sha.as_str()).collect();
    shas.sort();
    shas.dedup();
    if shas.len() < BROKEN_MIN_SHAS {
        return false;
    }
    // A pass on any of those commits means it isn't steadily broken.
    !history
        .iter()
        .filter(|e| counts(e) && shas.contains(&e.sha.as_str()))
        .any(|e| matches!(e.outcome, Outcome::Passed | Outcome::Flaky))
}

/// Failure rate on the default branch in the window: (runs, failures).
pub fn main_rate(
    history: &[HistoryEntry],
    default_branch: &str,
    since: DateTime<Utc>,
) -> (usize, usize) {
    let main = history.iter().filter(|e| {
        counts(e) && e.branch == default_branch && at(&e.at).is_some_and(|t| t >= since)
    });
    let (mut runs, mut fails) = (0, 0);
    for e in main {
        runs += 1;
        if matches!(e.outcome, Outcome::Failed | Outcome::Flaky) {
            fails += 1;
        }
    }
    (runs, fails)
}

/// The health the history supports, and why. `None` keeps the current one.
pub fn decide(r: &TestRecord, now: DateTime<Utc>, default_branch: &str) -> Option<(State, String)> {
    if is_broken(&r.history, default_branch) {
        return (r.health != State::Broken).then(|| {
            (
                State::Broken,
                format!("Failed at least {BROKEN_FAILS} of the last {BROKEN_WINDOW} runs on {default_branch}, on more than one commit, with no pass."),
            )
        });
    }
    let recent_flips = flips(&r.history, now - Duration::days(FLIP_WINDOW_DAYS));
    let (runs, fails) = main_rate(
        &r.history,
        default_branch,
        now - Duration::days(RATE_WINDOW_DAYS),
    );
    let rate_flaky = runs >= RATE_MIN_RUNS
        && fails > 0
        && prob_rate_above(fails, runs, RATE_THRESHOLD) >= RATE_CONFIDENCE;
    let flaky_reason = if recent_flips >= FLIPS_FOR_FLAKY {
        Some(format!(
            "Passed and failed on the same commit {recent_flips} times in {FLIP_WINDOW_DAYS} days."
        ))
    } else if rate_flaky {
        Some(format!(
            "Failed {fails} of {runs} runs on {default_branch} in {RATE_WINDOW_DAYS} days, likely above {}%.",
            (RATE_THRESHOLD * 100.0) as u32
        ))
    } else {
        None
    };

    match r.health {
        State::Broken => Some(match flaky_reason {
            Some(why) => (State::Flaky, why),
            None => (
                State::Healthy,
                format!("Passing again on {default_branch}."),
            ),
        }),
        State::Flaky => {
            let quiet_days = r
                .last_flip_at
                .as_deref()
                .and_then(at)
                .map(|t| now - t >= Duration::days(RESOLVE_DAYS))
                .unwrap_or(true);
            let quiet = (quiet_days || r.runs_since_flip >= RESOLVE_RUNS) && !rate_flaky;
            quiet.then(|| {
                (
                    State::Healthy,
                    format!("No flip for {RESOLVE_DAYS} days or {RESOLVE_RUNS} runs."),
                )
            })
        }
        State::Healthy | State::Fixed => {
            if let Some(why) = flaky_reason {
                return Some((State::Flaky, why));
            }
            if r.health == State::Healthy {
                if let Some(merged) = r.fix_merged_at.as_deref().and_then(at) {
                    let clean = r
                        .last_flip_at
                        .as_deref()
                        .and_then(at)
                        .is_none_or(|f| f < merged);
                    if clean && now - merged >= Duration::days(FIXED_DAYS) {
                        return Some((
                            State::Fixed,
                            format!("No flake in the {FIXED_DAYS} days since the fix merged."),
                        ));
                    }
                }
            }
            None
        }
        // Quarantine is a flag, never a health.
        State::Quarantined => None,
    }
}

/// What applying one run did to a test.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub events: Vec<StateEvent>,
    pub became_flaky: bool,
    pub became_broken: bool,
}

/// Adds a run to a test's history and updates its health and quarantine.
/// `auto_quarantine` quarantines a test that just turned flaky.
pub fn apply_run(
    r: &mut TestRecord,
    entry: HistoryEntry,
    now: DateTime<Utc>,
    default_branch: &str,
    auto_quarantine: bool,
) -> Applied {
    let now_s = now.to_rfc3339();
    let flip = is_flip(&r.history, &entry);
    r.last_seen = entry.at.clone();
    r.history.push(entry);
    if r.history.len() > MAX_HISTORY {
        let drop = r.history.len() - MAX_HISTORY;
        r.history.drain(..drop);
    }
    let detailed_from = r.history.len().saturating_sub(DETAILED_HISTORY);
    for e in &mut r.history[..detailed_from] {
        e.attempts.clear();
    }
    if flip {
        r.last_flip_at = Some(now_s.clone());
        r.runs_since_flip = 0;
    } else {
        r.runs_since_flip = r.runs_since_flip.saturating_add(1);
    }

    let mut out = Applied::default();
    if let Some((to, reason)) = decide(r, now, default_branch) {
        out.events.push(StateEvent {
            at: now_s.clone(),
            from: r.health.as_str().to_string(),
            to: to.as_str().to_string(),
            reason,
            actor: "coderhelm".to_string(),
        });
        out.became_flaky = to == State::Flaky;
        out.became_broken = to == State::Broken;
        r.health = to;
    }
    if r.health == State::Broken && r.quarantined {
        r.quarantined = false;
        r.quarantined_since = None;
        r.quarantined_by = None;
        out.events.push(StateEvent {
            at: now_s.clone(),
            from: "quarantined".into(),
            to: "broken".into(),
            reason: "Broken tests are real regressions, so they are never quarantined.".into(),
            actor: "coderhelm".into(),
        });
    }
    if out.became_flaky && auto_quarantine && !r.quarantined && !r.never_quarantine {
        r.quarantined = true;
        r.quarantined_since = Some(now_s.clone());
        r.quarantined_by = Some("auto".into());
        out.events.push(StateEvent {
            at: now_s,
            from: "flaky".into(),
            to: "quarantined".into(),
            reason: "Quarantined automatically: it still runs, but can't fail the build.".into(),
            actor: "coderhelm".into(),
        });
    }
    r.events.extend(out.events.iter().cloned());
    if r.events.len() > 100 {
        let drop = r.events.len() - 100;
        r.events.drain(..drop);
    }
    out
}

/// Whether the test can be quarantined now, and why not when it can't.
pub fn can_quarantine(r: &TestRecord) -> Result<(), &'static str> {
    if r.health == State::Broken {
        return Err("This test is broken, not flaky: it fails every time, so it is a real regression and can't be quarantined.");
    }
    if r.never_quarantine {
        return Err("This test is pinned as never quarantine. Unpin it first.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::Framework;

    fn record() -> TestRecord {
        TestRecord {
            id: "t1".into(),
            repo: "acme/web".into(),
            framework: Framework::Playwright,
            file: "a.spec.ts".into(),
            title: "t".into(),
            project: None,
            tags: vec![],
            health: State::Healthy,
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
            history: vec![],
            events: vec![],
        }
    }

    fn day(d: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + Duration::days(d)
    }

    fn entry(n: u32, sha: &str, branch: &str, outcome: Outcome, d: i64) -> HistoryEntry {
        HistoryEntry {
            run_key: format!("r{n}"),
            sha: sha.into(),
            branch: branch.into(),
            pr: None,
            at: day(d).to_rfc3339(),
            outcome,
            bad_run: false,
            attempts: vec![],
        }
    }

    fn run(r: &mut TestRecord, e: HistoryEntry, d: i64, auto: bool) -> Applied {
        apply_run(r, e, day(d), "main", auto)
    }

    #[test]
    fn bad_runs_need_a_real_share_of_failures() {
        assert!(!is_bad_run(100, 2, false));
        assert!(
            !is_bad_run(10, 2, false),
            "2 of 10 is not more than 20% with at least 3"
        );
        assert!(is_bad_run(10, 3, false));
        assert!(!is_bad_run(100, 20, false));
        assert!(is_bad_run(100, 21, false));
        assert!(is_bad_run(5, 0, true));
    }

    #[test]
    fn one_flip_is_noted_two_make_it_flaky() {
        let mut r = record();
        let a = run(&mut r, entry(1, "s1", "pr-1", Outcome::Flaky, 0), 0, false);
        assert!(!a.became_flaky);
        assert_eq!(r.health, State::Healthy);
        assert_eq!(r.runs_since_flip, 0);
        // the same commit failing on one branch and passing on another
        run(&mut r, entry(2, "s2", "main", Outcome::Failed, 1), 1, false);
        let a = run(&mut r, entry(3, "s2", "pr-2", Outcome::Passed, 1), 1, false);
        assert!(a.became_flaky);
        assert_eq!(r.health, State::Flaky);
        assert!(r
            .events
            .last()
            .unwrap()
            .reason
            .contains("same commit 2 times"));
    }

    #[test]
    fn flips_outside_the_window_do_not_count() {
        let mut r = record();
        run(&mut r, entry(1, "s1", "main", Outcome::Flaky, 0), 0, false);
        let a = run(
            &mut r,
            entry(2, "s2", "main", Outcome::Flaky, 20),
            20,
            false,
        );
        assert!(!a.became_flaky);
    }

    #[test]
    fn a_bad_run_never_makes_a_test_flaky_or_broken() {
        let mut r = record();
        for i in 0..12 {
            let mut e = entry(i, &format!("s{i}"), "main", Outcome::Failed, i as i64 / 4);
            e.bad_run = true;
            run(&mut r, e, i as i64 / 4, true);
        }
        assert_eq!(r.health, State::Healthy);
        assert!(!r.quarantined);
    }

    #[test]
    fn steady_failures_on_main_are_broken_and_never_quarantined() {
        let mut r = record();
        r.health = State::Flaky;
        r.quarantined = true;
        r.quarantined_since = Some(day(0).to_rfc3339());
        r.last_flip_at = Some(day(0).to_rfc3339());
        for i in 0..10 {
            run(
                &mut r,
                entry(i, &format!("s{}", i % 3), "main", Outcome::Failed, 1),
                1,
                true,
            );
        }
        assert_eq!(r.health, State::Broken);
        assert!(!r.quarantined, "broken tests are taken out of quarantine");
        assert_eq!(r.state(), State::Broken);
        assert!(can_quarantine(&r).is_err());
        assert!(r
            .events
            .iter()
            .any(|e| e.to == "broken" && e.from == "quarantined"));
    }

    #[test]
    fn failures_on_one_commit_only_are_not_broken() {
        let mut r = record();
        for i in 0..10 {
            run(
                &mut r,
                entry(i, "same", "main", Outcome::Failed, 1),
                1,
                false,
            );
        }
        assert_ne!(r.health, State::Broken);
    }

    #[test]
    fn a_pass_on_a_failing_commit_means_not_broken() {
        let mut r = record();
        for i in 0..10 {
            run(
                &mut r,
                entry(i, &format!("s{}", i % 2), "main", Outcome::Failed, 1),
                1,
                false,
            );
        }
        assert_eq!(r.health, State::Broken);
        run(
            &mut r,
            entry(20, "s0", "pr-9", Outcome::Passed, 2),
            2,
            false,
        );
        assert_ne!(r.health, State::Broken);
    }

    #[test]
    fn a_steady_low_failure_rate_on_main_is_flaky() {
        // 3 failures in 40 runs, each on its own commit, no same-commit flips.
        let mut r = record();
        for i in 0..40 {
            let o = if i % 13 == 5 {
                Outcome::Failed
            } else {
                Outcome::Passed
            };
            run(
                &mut r,
                entry(i, &format!("s{i}"), "main", o, i as i64 / 3),
                i as i64 / 3,
                false,
            );
        }
        assert_eq!(r.health, State::Flaky);
        assert!(r
            .events
            .iter()
            .any(|e| e.reason.contains("likely above 2%")));
    }

    #[test]
    fn the_posterior_is_sane() {
        assert!(prob_rate_above(0, 100, 0.02) < 0.2);
        assert!(prob_rate_above(10, 100, 0.02) > 0.99);
        assert!(prob_rate_above(1, 20, 0.02) > 0.5);
        let p = prob_rate_above(0, 0, 0.02);
        assert!((p - 0.98).abs() < 1e-6, "flat prior: {p}");
    }

    #[test]
    fn flaky_resolves_after_a_quiet_week() {
        let mut r = record();
        run(&mut r, entry(1, "s1", "main", Outcome::Flaky, 0), 0, false);
        run(&mut r, entry(2, "s2", "main", Outcome::Flaky, 0), 0, false);
        assert_eq!(r.health, State::Flaky);
        run(&mut r, entry(3, "s3", "main", Outcome::Passed, 3), 3, false);
        assert_eq!(r.health, State::Flaky);
        run(&mut r, entry(4, "s4", "main", Outcome::Passed, 8), 8, false);
        assert_eq!(r.health, State::Healthy);
    }

    #[test]
    fn auto_quarantine_only_when_allowed() {
        let mut r = record();
        run(&mut r, entry(1, "s1", "main", Outcome::Flaky, 0), 0, true);
        run(&mut r, entry(2, "s2", "main", Outcome::Flaky, 0), 0, true);
        assert!(r.quarantined);
        assert_eq!(r.quarantined_by.as_deref(), Some("auto"));
        assert_eq!(r.state(), State::Quarantined);

        let mut pinned = record();
        pinned.never_quarantine = true;
        run(
            &mut pinned,
            entry(1, "s1", "main", Outcome::Flaky, 0),
            0,
            true,
        );
        run(
            &mut pinned,
            entry(2, "s2", "main", Outcome::Flaky, 0),
            0,
            true,
        );
        assert_eq!(pinned.health, State::Flaky);
        assert!(!pinned.quarantined);

        let mut off = record();
        run(
            &mut off,
            entry(1, "s1", "main", Outcome::Flaky, 0),
            0,
            false,
        );
        run(
            &mut off,
            entry(2, "s2", "main", Outcome::Flaky, 0),
            0,
            false,
        );
        assert!(!off.quarantined);
    }

    #[test]
    fn fixed_after_thirty_clean_days_from_the_fix() {
        let mut r = record();
        r.health = State::Healthy;
        r.last_flip_at = Some(day(-5).to_rfc3339());
        r.fix_merged_at = Some(day(0).to_rfc3339());
        run(
            &mut r,
            entry(1, "s1", "main", Outcome::Passed, 10),
            10,
            false,
        );
        assert_eq!(r.health, State::Healthy);
        run(
            &mut r,
            entry(2, "s2", "main", Outcome::Passed, 31),
            31,
            false,
        );
        assert_eq!(r.health, State::Fixed);
        run(
            &mut r,
            entry(3, "s3", "main", Outcome::Flaky, 32),
            32,
            false,
        );
        run(
            &mut r,
            entry(4, "s4", "main", Outcome::Flaky, 32),
            32,
            false,
        );
        assert_eq!(r.health, State::Flaky);
    }

    #[test]
    fn history_is_capped_and_old_attempts_dropped() {
        let mut r = record();
        for i in 0..(MAX_HISTORY as u32 + 10) {
            let mut e = entry(i, &format!("s{i}"), "main", Outcome::Passed, 0);
            e.attempts = vec![crate::e2e::model::Attempt {
                retry: 0,
                status: crate::e2e::model::AttemptStatus::Passed,
                duration_ms: 1,
                error: None,
                trace_url: None,
                video_url: None,
                screenshot_url: None,
            }];
            run(&mut r, e, 0, false);
        }
        assert_eq!(r.history.len(), MAX_HISTORY);
        assert!(r.history[0].attempts.is_empty());
        assert_eq!(r.history.last().unwrap().attempts.len(), 1);
        assert_eq!(
            r.history.iter().filter(|e| !e.attempts.is_empty()).count(),
            DETAILED_HISTORY
        );
    }
}
