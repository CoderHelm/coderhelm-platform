//! Quarantine is a place a test passes through, never one it is left in.
//!
//! Sixteen tests sat quarantined on one team, thirteen for more than two
//! days and some for over three weeks, with one button on each row (release)
//! and nothing happening: the weekly automatic fixes went to whichever
//! flaky test a default-branch run listed first, a test whose fix had failed
//! once was never looked at again, a person's "Fix this test" was dropped by
//! the worker as a duplicate of its own request, and a test that had gone
//! back to passing stayed quarantined until someone released it by hand,
//! under a cause sentence its history had already disproved.
//!
//! Everything here is a pure function of a test's record and the time, so
//! the worker (as each run is recorded, and in its periodic sweep), the
//! gateway (what a row says) and the tests agree.
//!
//! | Rule | Value | Basis |
//! |---|---|---|
//! | Leaves by itself | [`RELEASE_PASSES`] passes in a row with no failure and no pass on a retry, counted from runs after the last failure, the quarantine and a merged fix | the runs the flaky rule needs before it judges a rate (`detect::RATE_MIN_RUNS`), and the repeat runs a fix must hold for |
//! | ... on more than one branch | [`RELEASE_MIN_SOURCES`] pull requests or branches | the broken rule's "more than one commit" (`detect::BROKEN_MIN_SHAS`): one lucky branch proves nothing |
//! | ... over time | [`RELEASE_MIN_HOURS`] hours from the first of those passes to the last | new here: longer than one push's CI cycle, short enough that a test healthy all afternoon leaves the same day |
//! | Unstable | quarantined again [`UNSTABLE_BOUNCES`] times within [`UNSTABLE_WINDOW_DAYS`] days of leaving by itself; it then also needs [`UNSTABLE_QUIET_DAYS`] days without a failure | the flaky rule's two flips, its 30-day rate window and its 7 quiet days |
//! | Cause out of date | [`CAUSE_STALE_OUTSIDE`] clean passes in a row when the cause is outside the test (environment, network), [`RELEASE_PASSES`] otherwise, on more than one branch | a cause outside the test fails every run while it lasts, so the broken rule's window (`detect::BROKEN_WINDOW`) of passes disproves it |
//! | Automatic fix again | [`AUTO_FIX_RETRY_DAYS`] days after one that did not hold, [`AUTO_FIX_MAX_ATTEMPTS`] in all | the 7 quiet days; two, like the weekly default |
//! | Removal proposed | quarantined [`REMOVAL_MIN_DAYS`] days, [`REMOVAL_MIN_FAILED_FIXES`] fixes that did not hold, and another healthy test covers the same file or tag | the 30 days of the fixed rule |

use std::collections::HashMap;

use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::detect::{
    BROKEN_MIN_SHAS, BROKEN_WINDOW, FIXED_DAYS, FLIPS_FOR_FLAKY, RATE_MIN_RUNS, RATE_WINDOW_DAYS,
    RESOLVE_DAYS,
};
use super::model::{HistoryEntry, Outcome, Release, Removal, State, StateEvent, TestRecord};
use super::stats::Stats;

pub const RELEASE_PASSES: u32 = RATE_MIN_RUNS as u32;
pub const RELEASE_MIN_SOURCES: u32 = BROKEN_MIN_SHAS as u32;
pub const RELEASE_MIN_HOURS: i64 = 2;
pub const UNSTABLE_BOUNCES: usize = FLIPS_FOR_FLAKY;
pub const UNSTABLE_WINDOW_DAYS: i64 = RATE_WINDOW_DAYS;
pub const UNSTABLE_QUIET_DAYS: i64 = RESOLVE_DAYS;
pub const CAUSE_STALE_OUTSIDE: u32 = BROKEN_WINDOW as u32;
pub const AUTO_FIX_RETRY_DAYS: i64 = RESOLVE_DAYS;
pub const AUTO_FIX_MAX_ATTEMPTS: u32 = 2;
/// A quarantined test that has not run for this long has nothing to be
/// fixed from and cannot prove itself.
pub const NOT_RUNNING_DAYS: i64 = RESOLVE_DAYS;
pub const REMOVAL_MIN_DAYS: i64 = FIXED_DAYS;
pub const REMOVAL_MIN_FAILED_FIXES: u32 = 2;
/// A fix or removal marked as started this long ago with nothing to show
/// for it has ended.
pub const STALLED_HOURS: i64 = 24;

pub const REMOVE_TICKET_PREFIX: &str = "E2E-RM-";

fn at(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn day(t: DateTime<Utc>) -> String {
    t.format("%-d %B %Y").to_string()
}

fn day_of(s: &str) -> String {
    at(s).map(day).unwrap_or_else(|| s.to_string())
}

fn counts(e: &HistoryEntry) -> bool {
    !e.bad_run && e.outcome != Outcome::Skipped
}

fn sources_words(n: u32) -> String {
    if n == 1 {
        "1 pull request or branch".to_string()
    } else {
        format!("{n} pull requests and branches")
    }
}

fn span_words(minutes: u32) -> String {
    match minutes {
        0..=89 => format!("{minutes} minutes"),
        m if m < 48 * 60 => format!("{} hours", (m + 30) / 60),
        m => format!("{} days", (m + 12 * 60) / (24 * 60)),
    }
}

// ── the passes since the last failure ─────────────────────────────────────

/// The clean passes at the end of a test's history.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Streak {
    passes: u32,
    sources: u32,
    minutes: u32,
    /// The run that ended the streak going back: a failure or a pass on a
    /// retry.
    last_failure_at: Option<String>,
}

/// Passes in a row, newest first, back to the last run that failed or passed
/// only on a retry, and no further back than `floor`.
fn streak(r: &TestRecord, floor: Option<DateTime<Utc>>) -> Streak {
    let mut runs: Vec<(DateTime<Utc>, &HistoryEntry)> = r
        .history
        .iter()
        .filter(|e| counts(e))
        .filter_map(|e| at(&e.at).map(|t| (t, e)))
        .collect();
    runs.sort_by_key(|(t, _)| *t);
    let mut out = Streak::default();
    let mut seen: Vec<String> = Vec::new();
    let (mut first, mut last) = (None, None);
    for (t, e) in runs.iter().rev() {
        if e.outcome != Outcome::Passed {
            out.last_failure_at = Some(e.at.clone());
            break;
        }
        if floor.is_some_and(|f| *t < f) {
            break;
        }
        out.passes += 1;
        let source = match e.pr {
            Some(n) => format!("#{n}"),
            None => e.branch.clone(),
        };
        if !seen.contains(&source) {
            seen.push(source);
        }
        last = last.or(Some(*t));
        first = Some(*t);
    }
    if out.last_failure_at.is_none() {
        out.last_failure_at = runs
            .iter()
            .rev()
            .find(|(_, e)| e.outcome != Outcome::Passed)
            .map(|(_, e)| e.at.clone());
    }
    out.sources = seen.len() as u32;
    out.minutes = match (first, last) {
        (Some(a), Some(b)) => (b - a).num_minutes().max(0) as u32,
        _ => 0,
    };
    out
}

/// How far a quarantined test is from leaving by itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub passes: u32,
    pub needed: u32,
    /// Pull requests and branches the passes ran on.
    pub sources: u32,
    pub sources_needed: u32,
    /// From the first pass counted to the last.
    pub minutes: u32,
    pub minutes_needed: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<String>,
    /// An unstable test leaves no earlier than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<String>,
    pub ready: bool,
    /// "12 of 20 passes needed, across 3 pull requests and branches".
    pub line: String,
}

/// The floor passes are counted from: the quarantine, and a fix that merged
/// since (the test proves the fix, not the code before it).
fn floor(r: &TestRecord) -> Option<DateTime<Utc>> {
    [r.quarantined_since.as_deref(), r.fix_merged_at.as_deref()]
        .into_iter()
        .flatten()
        .filter_map(at)
        .max()
}

pub fn progress(r: &TestRecord, now: DateTime<Utc>) -> Progress {
    let s = streak(r, floor(r));
    let minutes_needed = (RELEASE_MIN_HOURS * 60) as u32;
    let not_before = unstable(r, now).and_then(|_| {
        let from = [
            s.last_failure_at.as_deref().and_then(at),
            r.quarantined_since.as_deref().and_then(at),
        ]
        .into_iter()
        .flatten()
        .max()?;
        Some(from + Duration::days(UNSTABLE_QUIET_DAYS))
    });
    let enough = s.passes >= RELEASE_PASSES
        && s.sources >= RELEASE_MIN_SOURCES
        && s.minutes >= minutes_needed;
    let ready = enough && not_before.is_none_or(|t| now >= t);
    let line = if s.passes == 0 {
        match &s.last_failure_at {
            Some(f) => format!(
                "No pass yet since it last failed on {}; {RELEASE_PASSES} in a row are needed",
                day_of(f)
            ),
            None => format!(
                "No run since it was quarantined; {RELEASE_PASSES} passes in a row are needed"
            ),
        }
    } else {
        let mut l = format!(
            "{} of {RELEASE_PASSES} passes needed, across {}",
            s.passes.min(RELEASE_PASSES),
            sources_words(s.sources)
        );
        if s.sources < RELEASE_MIN_SOURCES {
            l.push_str("; a second pull request or branch is needed");
        } else if s.passes >= RELEASE_PASSES && s.minutes < minutes_needed {
            l.push_str(&format!(
                "; they span {}, and {RELEASE_MIN_HOURS} hours are needed",
                span_words(s.minutes)
            ));
        } else if let Some(t) = not_before.filter(|t| enough && now < *t) {
            l.push_str(&format!(
                "; it is unstable, so it leaves on {} if it keeps passing",
                day(t)
            ));
        }
        l
    };
    Progress {
        passes: s.passes,
        needed: RELEASE_PASSES,
        sources: s.sources,
        sources_needed: RELEASE_MIN_SOURCES,
        minutes: s.minutes,
        minutes_needed,
        last_failure_at: s.last_failure_at,
        not_before: not_before.map(|t| t.to_rfc3339()),
        ready,
        line,
    }
}

/// Why an unstable test is called that, or `None`.
pub fn unstable(r: &TestRecord, now: DateTime<Utc>) -> Option<String> {
    let since = now - Duration::days(UNSTABLE_WINDOW_DAYS);
    let n = r
        .care
        .bounces
        .iter()
        .filter(|b| at(b).is_some_and(|t| t >= since))
        .count();
    (n >= UNSTABLE_BOUNCES).then(|| {
        format!(
            "Unstable: it left quarantine by itself and was quarantined again {n} times in {UNSTABLE_WINDOW_DAYS} days. It now also needs {UNSTABLE_QUIET_DAYS} days without a failure before it leaves by itself."
        )
    })
}

/// Puts a test in quarantine. One that left by itself within the window is
/// bouncing, and that is counted.
pub fn enter(r: &mut TestRecord, now: DateTime<Utc>, by: &str) {
    r.quarantined = true;
    r.quarantined_since = Some(now.to_rfc3339());
    r.quarantined_by = Some(by.to_string());
    let since = now - Duration::days(UNSTABLE_WINDOW_DAYS);
    r.care.bounces.retain(|b| at(b).is_some_and(|t| t >= since));
    if r.care
        .release
        .as_ref()
        .and_then(|rel| at(&rel.at))
        .is_some_and(|t| t >= since)
    {
        r.care.bounces.push(now.to_rfc3339());
    }
}

/// The release a quarantined test has earned, if it has.
pub fn release_due(r: &TestRecord, now: DateTime<Utc>) -> Option<Release> {
    if !r.quarantined || r.health == State::Broken {
        return None;
    }
    let p = progress(r, now);
    if !p.ready {
        return None;
    }
    let since = match &p.last_failure_at {
        Some(f) => format!("since it last failed on {}", day_of(f)),
        None => "since it was quarantined".to_string(),
    };
    Some(Release {
        at: now.to_rfc3339(),
        reason: format!(
            "Left quarantine by itself: {} passes in a row with no failure and no retry, on {}, over {}, {since}.",
            p.passes,
            sources_words(p.sources),
            span_words(p.minutes)
        ),
        passes: p.passes,
        sources: p.sources,
        hours: p.minutes / 60,
    })
}

/// Takes the test out of quarantine with the evidence, and returns the
/// event for its history. A flaky test becomes healthy: the passes are the
/// evidence, and a new flake is judged from here on (`detect::decide`).
pub fn release(r: &mut TestRecord, rel: Release) -> StateEvent {
    let to = match r.health {
        State::Flaky => State::Healthy,
        other => other,
    };
    r.health = to;
    r.quarantined = false;
    r.quarantined_since = None;
    r.quarantined_by = None;
    // A fix that did not hold no longer matters; one that merged stays on
    // the record.
    if r.fix_status.as_deref() == Some("failed") {
        r.fix_status = None;
    }
    r.care.fix_failure = None;
    r.care.fix_failed_at = None;
    r.care.fix_failures = 0;
    r.care.fix_requested_at = None;
    let event = StateEvent {
        at: rel.at.clone(),
        from: "quarantined".into(),
        to: to.as_str().into(),
        reason: rel.reason.clone(),
        actor: "coderhelm".into(),
    };
    r.care.release = Some(rel);
    event
}

// ── a fix that did not hold ───────────────────────────────────────────────

/// Records that the current fix ended without fixing the test, with why.
pub fn fix_failed(r: &mut TestRecord, now: DateTime<Utc>, why: &str) -> StateEvent {
    r.fix_status = Some("failed".into());
    r.care.fix_failure = Some(why.to_string());
    r.care.fix_failed_at = Some(now.to_rfc3339());
    r.care.fix_failures = r.care.fix_failures.saturating_add(1);
    r.care.fix_requested_at = None;
    let state = if r.quarantined {
        "quarantined"
    } else {
        r.health.as_str()
    };
    StateEvent {
        at: now.to_rfc3339(),
        from: state.into(),
        to: state.into(),
        reason: why.to_string(),
        actor: "coderhelm".into(),
    }
}

/// A fix merged and the test failed again afterwards: the fix did not hold.
fn merged_fix_failed(r: &TestRecord) -> Option<String> {
    if r.fix_status.as_deref() != Some("merged") {
        return None;
    }
    let merged = r.fix_merged_at.as_deref().and_then(at)?;
    r.history
        .iter()
        .filter(|e| counts(e) && e.outcome != Outcome::Passed)
        .filter_map(|e| at(&e.at))
        .filter(|t| *t > merged)
        .max()
        .map(|t| {
            format!(
                "The fix merged on {}, and the test failed again on {}.",
                day(merged),
                day(t)
            )
        })
}

// ── a cause the history disproves ─────────────────────────────────────────

fn outside_the_test(label: &str) -> bool {
    matches!(label, "environment" | "network")
}

/// What the history now says against the recorded cause, or `None` while
/// nothing contradicts it.
pub fn cause_contradicted(r: &TestRecord) -> Option<String> {
    let c = r.cause.as_ref()?;
    if c.out_of_date.is_some() {
        return None;
    }
    let s = streak(r, c.at.as_deref().and_then(at));
    let needed = if outside_the_test(&c.label) {
        CAUSE_STALE_OUTSIDE
    } else {
        RELEASE_PASSES
    };
    if s.passes < needed || s.sources < RELEASE_MIN_SOURCES {
        return None;
    }
    let since = match &s.last_failure_at {
        Some(f) => format!("Since its last failure on {}", day_of(f)),
        None => "Since then".to_string(),
    };
    Some(format!(
        "Out of date: this was worked out while the test was failing. {since} it has passed {} runs in a row on {}, with no retry.",
        s.passes,
        sources_words(s.sources)
    ))
}

/// What changed, as far as the repository shows: commits to the test's file
/// since its last failure (`(short sha, first line)`), newest first.
pub fn what_changed(commits: &[(String, String)]) -> String {
    match commits.first() {
        Some((sha, title)) => format!(
            "The test's file changed since then ({sha} \"{}\"{}).",
            crate::truncate_str(title, 80),
            if commits.len() > 1 {
                format!(" and {} more", commits.len() - 1)
            } else {
                String::new()
            }
        ),
        None => "The test's file has not changed since then, so what changed is outside the test: the environment it runs against or the application.".to_string(),
    }
}

// ── settling a record ─────────────────────────────────────────────────────

/// What [`settle`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Settled {
    pub events: Vec<StateEvent>,
    pub released: Option<Release>,
    /// The cause was marked out of date just now.
    pub cause_went_stale: bool,
}

/// Brings a record up to date with its own history: a merged fix that
/// failed again, a cause the runs disprove, and a quarantine the test has
/// earned its way out of. Running it twice changes nothing the second time.
pub fn settle(r: &mut TestRecord, now: DateTime<Utc>) -> Settled {
    let mut out = Settled::default();
    if let Some(why) = merged_fix_failed(r) {
        out.events.push(fix_failed(r, now, &why));
    }
    if let Some(why) = cause_contradicted(r) {
        if let Some(c) = r.cause.as_mut() {
            c.out_of_date = Some(why);
            out.cause_went_stale = true;
        }
    }
    if let Some(rel) = release_due(r, now) {
        out.events.push(release(r, rel.clone()));
        out.released = Some(rel);
    }
    out
}

// ── which tests Coderhelm fixes by itself ─────────────────────────────────

/// Why Coderhelm is not starting a fix for a test by itself right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hold {
    /// A real regression: a person's call.
    Broken,
    /// A fix is being reproduced, written, proven or reviewed.
    InFlight,
    /// A pull request that removes it is being written or is open.
    Removal,
    /// It is passing its way out of quarantine.
    Passing,
    /// It has not run in CI for [`NOT_RUNNING_DAYS`] days.
    NotRunning(String),
    /// No failure in the window to fix from.
    NothingToFixFrom,
    /// [`AUTO_FIX_MAX_ATTEMPTS`] fixes did not hold.
    Tried,
    /// The last fix did not hold; the next automatic one waits until then.
    CoolingOff(String),
    /// Flaky, not quarantined, and too rare or too cheap to spend a run on.
    TooRare,
}

impl Hold {
    pub fn code(&self) -> &'static str {
        match self {
            Hold::Broken => "broken",
            Hold::InFlight => "in_flight",
            Hold::Removal => "removal",
            Hold::Passing => "passing",
            Hold::NotRunning(_) => "not_running",
            Hold::NothingToFixFrom => "nothing_to_fix_from",
            Hold::Tried => "tried",
            Hold::CoolingOff(_) => "cooling_off",
            Hold::TooRare => "too_rare",
        }
    }
}

fn fix_in_flight(r: &TestRecord) -> bool {
    matches!(
        r.fix_status.as_deref(),
        Some("reproducing" | "fixing" | "burn_in" | "opened" | "merged")
    )
}

fn removal_in_flight(r: &TestRecord) -> bool {
    r.care
        .removal
        .as_ref()
        .is_some_and(|m| matches!(m.status.as_str(), "running" | "opened"))
}

/// A flaky test that is not quarantined is fixed by itself only when it
/// fails this often on recent runs, has enough runs to judge, and cost CI
/// time this week.
const UNQUARANTINED_MIN_RATE: f64 = 0.05;

/// `None` when Coderhelm may start a fix for this test by itself.
pub fn auto_hold(r: &TestRecord, s: &Stats, now: DateTime<Utc>) -> Option<Hold> {
    if r.health == State::Broken {
        return Some(Hold::Broken);
    }
    if fix_in_flight(r) {
        return Some(Hold::InFlight);
    }
    if removal_in_flight(r) {
        return Some(Hold::Removal);
    }
    let rate = s.fail_rate_30d + s.flake_rate_30d;
    if !r.quarantined {
        let wanted = r.health == State::Flaky
            && r.fix_status.is_none()
            && r.fix_pr_url.is_none()
            && s.runs_30d >= RATE_MIN_RUNS as u32
            && rate >= UNQUARANTINED_MIN_RATE
            && s.minutes_lost_7d > 0.0;
        return (!wanted).then_some(Hold::TooRare);
    }
    let p = progress(r, now);
    if at(&r.last_seen).is_none_or(|t| now - t >= Duration::days(NOT_RUNNING_DAYS)) {
        return Some(Hold::NotRunning(r.last_seen.clone()));
    }
    if p.passes >= RELEASE_PASSES / 2 {
        return Some(Hold::Passing);
    }
    if rate <= 0.0 {
        return Some(if p.passes > 0 {
            Hold::Passing
        } else {
            Hold::NothingToFixFrom
        });
    }
    if r.care.fix_failures >= AUTO_FIX_MAX_ATTEMPTS {
        return Some(Hold::Tried);
    }
    if let Some(failed) = r.care.fix_failed_at.as_deref().and_then(at) {
        let until = failed + Duration::days(AUTO_FIX_RETRY_DAYS);
        if now < until {
            return Some(Hold::CoolingOff(until.to_rfc3339()));
        }
    }
    None
}

/// The order automatic fixes are started in: quarantined tests first, the
/// longest quarantined first, then the worst (how often it fails, then the
/// CI time it lost this week). `rows` are test summaries.
pub fn auto_fix_order(rows: &[Value]) -> Vec<&Value> {
    let mut out: Vec<&Value> = rows.iter().collect();
    let since = |v: &Value| v["quarantined_since"].as_str().and_then(at);
    let rate = |v: &Value| {
        v["fail_rate_30d"].as_f64().unwrap_or(0.0) + v["flake_rate_30d"].as_f64().unwrap_or(0.0)
    };
    let lost = |v: &Value| v["minutes_lost_7d"].as_f64().unwrap_or(0.0);
    out.sort_by(|a, b| {
        match (since(a), since(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then(
            rate(b)
                .partial_cmp(&rate(a))
                .unwrap_or(std::cmp::Ordering::Equal),
        )
        .then(
            lost(b)
                .partial_cmp(&lost(a))
                .unwrap_or(std::cmp::Ordering::Equal),
        )
        .then(a["id"].as_str().cmp(&b["id"].as_str()))
    });
    out
}

// ── what a person may ask for ─────────────────────────────────────────────

/// A fix that was asked for and not yet started counts as in flight for
/// this long; after it, the request was lost and can be made again.
const REQUEST_MINUTES: i64 = 60;

/// Whether a fix is being reproduced or written right now: started within
/// [`STALLED_HOURS`], or asked for within the hour.
fn fix_being_written(r: &TestRecord, now: DateTime<Utc>) -> bool {
    if !matches!(r.fix_status.as_deref(), Some("reproducing" | "fixing")) {
        return false;
    }
    match r.fix_phase_at.as_deref().and_then(at) {
        Some(t) => now - t < Duration::hours(STALLED_HOURS),
        None => r
            .care
            .fix_requested_at
            .as_deref()
            .and_then(at)
            .is_some_and(|t| now - t < Duration::minutes(REQUEST_MINUTES)),
    }
}

fn with_url(text: &str, url: Option<&str>) -> String {
    match url.filter(|u| !u.is_empty()) {
        Some(u) => format!("{text}: {u}."),
        None => format!("{text}."),
    }
}

/// Why a person's "Fix this test" can't start now, or `None` when it can.
/// A fix that did not hold, or whose pull request was closed, never stands
/// in the way.
pub fn fix_blocked(r: &TestRecord, now: DateTime<Utc>) -> Option<String> {
    if r.health == State::Broken {
        return Some("Only flaky tests get a fix PR. A broken test is a real regression: fix the code it tests.".into());
    }
    if r.health != State::Flaky && !r.quarantined {
        return Some("Only a flaky or quarantined test gets a fix PR.".into());
    }
    if is_removed(r) {
        return Some("This test was removed.".into());
    }
    if removal_in_flight(r) {
        return Some(
            "A pull request that removes this test is in flight. Close it first to ask for a fix."
                .into(),
        );
    }
    let url = r.fix_pr_url.as_deref();
    match r.fix_status.as_deref() {
        Some("reproducing" | "fixing") if fix_being_written(r, now) => {
            Some("A fix for this test is already in progress.".into())
        }
        Some("burn_in") => Some(with_url(
            "A fix for this test is proving itself on its pull request",
            url,
        )),
        Some("opened") => Some(with_url("A fix pull request is already open", url)),
        Some("merged") => Some(
            "A fix merged and the test is proving itself. If it fails again, a new fix can be asked for.".into(),
        ),
        _ => None,
    }
}

/// Why a person's "Remove this test" can't start now, or `None` when it
/// can.
pub fn removal_blocked(r: &TestRecord, now: DateTime<Utc>) -> Option<String> {
    if is_removed(r) {
        return Some("This test was already removed.".into());
    }
    if !r.quarantined {
        return Some("Only a quarantined test can be removed from here.".into());
    }
    if let Some(m) = r.care.removal.as_ref().filter(|_| removal_in_flight(r)) {
        return Some(with_url(
            if m.status == "opened" {
                "A pull request that removes this test is already open"
            } else {
                "A pull request that removes this test is already being written"
            },
            m.pr.as_ref().map(|p| p.url.as_str()),
        ));
    }
    let url = r.fix_pr_url.as_deref();
    match r.fix_status.as_deref() {
        Some("reproducing" | "fixing") if fix_being_written(r, now) => {
            Some("A fix for this test is in progress. Wait for it before removing the test.".into())
        }
        Some("burn_in" | "opened") => Some(with_url(
            "A fix pull request is open. Close it first to remove the test",
            url,
        )),
        Some("merged") => Some("A fix merged and the test is proving itself.".into()),
        _ => None,
    }
}

// ── the weekly spend guard ────────────────────────────────────────────────

/// The week a count of automatic runs belongs to (ISO week, UTC).
pub fn week_key(now: DateTime<Utc>) -> String {
    now.format("%G-W%V").to_string()
}

/// When the week's count starts again: the next Monday, 00:00 UTC.
pub fn week_resets(now: DateTime<Utc>) -> DateTime<Utc> {
    let days = 7 - now.weekday().num_days_from_monday() as i64;
    (now.date_naive() + Duration::days(days))
        .and_hms_opt(0, 0, 0)
        .map(|d| d.and_utc())
        .unwrap_or(now)
}

/// A repository's automatic fix runs this week.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub repo: String,
    /// Whether the team has fix pull requests turned on.
    pub on: bool,
    pub cap: u32,
    pub used: u32,
    pub resumes_at: String,
}

impl Budget {
    pub fn spent(&self) -> bool {
        self.used >= self.cap
    }

    /// The cap in one line, with why there is one.
    pub fn line(&self) -> String {
        if !self.on {
            return "Automatic fix pull requests are turned off for this team, so Coderhelm fixes a test only when someone asks.".to_string();
        }
        if self.spent() {
            format!(
                "Automatic fixes are at this week's limit of {} for this repository. They resume on {}.",
                self.cap,
                at(&self.resumes_at)
                    .map(|t| t.format("%A %-d %B").to_string())
                    .unwrap_or_else(|| "Monday".to_string())
            )
        } else {
            format!(
                "Coderhelm has started {} of this week's {} automatic fixes for this repository.",
                self.used, self.cap
            )
        }
    }
}

/// Why there is a cap at all, for the page.
pub const CAP_REASON: &str = "Each automatic fix is a full coding run, so the limit bounds what they cost. A fix you ask for is never counted.";

// ── what a row says ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    /// `pr` | `run`
    pub kind: String,
    pub label: String,
    /// A pull request's address, or a run's id.
    pub target: String,
}

/// Where a quarantined test stands and what happens next.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Standing {
    /// `idle` | `fix_running` | `fix_pr` | `fix_merged` | `fix_failed` |
    /// `removal_running` | `removal_pr`
    pub state: String,
    pub headline: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<Link>,
    /// The attempt before this one that did not hold: what happened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_link: Option<Link>,
    /// Why Coderhelm has not fixed it by itself, in one line. Empty while
    /// work is in flight.
    pub why: String,
    /// `eligible`, or a [`Hold::code`].
    pub hold: String,
    /// `fix` | `remove` | `release`
    pub actions: Vec<String>,
    pub progress: Progress,
}

fn pr_link(url: &str, label: &str) -> Option<Link> {
    (!url.is_empty()).then(|| Link {
        kind: "pr".into(),
        label: label.into(),
        target: url.to_string(),
    })
}

fn run_link(id: Option<&str>, label: &str) -> Option<Link> {
    id.filter(|i| !i.is_empty()).map(|i| Link {
        kind: "run".into(),
        label: label.into(),
        target: i.to_string(),
    })
}

fn checks_words(checks: Option<&str>) -> &'static str {
    match checks {
        Some("passing") => ", checks passing",
        Some("failing") => ", checks failing",
        Some("running") => ", checks running",
        _ => "",
    }
}

fn why_line(hold: Option<&Hold>, p: &Progress, r: &TestRecord) -> String {
    match hold {
        None => String::new(),
        Some(Hold::Broken) | Some(Hold::InFlight) | Some(Hold::Removal) | Some(Hold::TooRare) => {
            String::new()
        }
        Some(Hold::Passing) => format!(
            "It is passing again and leaves quarantine by itself: {}.",
            p.line
        ),
        Some(Hold::NotRunning(since)) => format!(
            "It has not run in CI since {}, so there is no failure to fix from and it cannot prove itself. If the workflow skips quarantined tests, it stays here until it is fixed or removed.",
            day_of(since)
        ),
        Some(Hold::NothingToFixFrom) => format!(
            "It has no failure in the last {RATE_WINDOW_DAYS} days to fix from."
        ),
        Some(Hold::Tried) => format!(
            "{} fixes did not hold, so Coderhelm does not start another by itself. Ask for a fix, or remove the test.",
            r.care.fix_failures
        ),
        Some(Hold::CoolingOff(until)) => format!(
            "The last fix did not hold. Coderhelm tries again by itself after {}.",
            day_of(until)
        ),
    }
}

/// Where a quarantined test stands. `None` for a test that is not
/// quarantined.
pub fn standing(r: &TestRecord, s: &Stats, now: DateTime<Utc>) -> Option<Standing> {
    if !r.quarantined {
        return None;
    }
    let p = progress(r, now);
    let hold = auto_hold(r, s, now);
    let fix_url = r.fix_pr_url.as_deref().unwrap_or("");
    let seen = r.care.fix_pr.as_ref();
    let mut st = Standing {
        hold: hold
            .as_ref()
            .map(|h| h.code())
            .unwrap_or("eligible")
            .to_string(),
        why: why_line(hold.as_ref(), &p, r),
        actions: vec!["release".into()],
        ..Default::default()
    };
    let removal = r.care.removal.as_ref();
    match (removal.map(|m| m.status.as_str()), r.fix_status.as_deref()) {
        (Some("running"), _) => {
            st.state = "removal_running".into();
            st.headline = "Coderhelm is writing a pull request that removes this test".into();
            st.link = run_link(removal.and_then(|m| m.run_id.as_deref()), "Open the run");
        }
        (Some("opened"), _) => {
            let pr = removal.and_then(|m| m.pr.as_ref());
            st.state = "removal_pr".into();
            st.headline = format!(
                "Removal pull request open{}: it is your team's to approve and merge",
                checks_words(pr.and_then(|p| p.checks.as_deref()))
            );
            st.link = pr.and_then(|p| pr_link(&p.url, "Removal PR"));
        }
        (_, Some("reproducing")) => {
            st.state = "fix_running".into();
            st.headline = format!("Reproducing the failure with {RELEASE_PASSES} repeat runs");
            st.link = run_link(r.care.fix_run_id.as_deref(), "Open the run");
        }
        (_, Some("fixing")) => {
            st.state = "fix_running".into();
            st.headline = "Coderhelm is writing a fix".into();
            st.link = pr_link(fix_url, "Fix PR")
                .or_else(|| run_link(r.care.fix_run_id.as_deref(), "Open the run"));
        }
        (_, Some("burn_in")) => {
            st.state = "fix_pr".into();
            st.headline = format!(
                "Fix pull request is a draft: proving itself over {RELEASE_PASSES} repeat runs"
            );
            st.link = pr_link(fix_url, "Fix PR");
        }
        (_, Some("opened")) => {
            st.state = "fix_pr".into();
            st.headline = format!(
                "Fix pull request open{}",
                checks_words(seen.and_then(|p| p.checks.as_deref()))
            );
            st.link = pr_link(fix_url, "Fix PR");
        }
        (_, Some("merged")) => {
            st.state = "fix_merged".into();
            st.headline = format!("Fix merged, proving itself: {}", p.line);
            st.link = pr_link(fix_url, "Fix PR");
        }
        (removal_status, fix) => {
            let failed_fix = fix == Some("failed");
            let ended_removal = matches!(removal_status, Some("declined" | "failed"));
            st.state = if failed_fix { "fix_failed" } else { "idle" }.into();
            st.headline = if failed_fix {
                "The last fix did not hold".to_string()
            } else {
                "Nothing in flight".to_string()
            };
            if failed_fix {
                st.attempt = Some(r.care.fix_failure.clone().unwrap_or_else(|| {
                    "The fix ended without a pull request that held.".to_string()
                }));
                st.attempt_link = pr_link(fix_url, "The fix PR");
            } else if ended_removal {
                let m = removal.expect("checked above");
                st.attempt = Some(m.reason.clone().unwrap_or_else(|| {
                    "The removal ended without a merged pull request.".to_string()
                }));
                st.attempt_link =
                    m.pr.as_ref()
                        .and_then(|p| pr_link(&p.url, "The removal PR"));
            }
            st.actions = vec!["fix".into(), "remove".into(), "release".into()];
        }
    }
    st.progress = p;
    Some(st)
}

/// Fills in what only the whole list knows: the team's switch, the week's
/// count, and how many tests are ahead of each one. `rows` are test
/// summaries of any repositories.
pub fn overlay(rows: &mut [Value], budgets: &HashMap<String, Budget>) {
    let mut ahead: HashMap<String, usize> = HashMap::new();
    {
        let eligible: Vec<Value> = rows
            .iter()
            .filter(|v| v["quarantine"]["hold"].as_str() == Some("eligible"))
            .cloned()
            .collect();
        let mut seen: HashMap<String, usize> = HashMap::new();
        for v in auto_fix_order(&eligible) {
            let repo = v["repo"].as_str().unwrap_or("").to_string();
            let n = seen.entry(repo).or_default();
            ahead.insert(v["id"].as_str().unwrap_or("").to_string(), *n);
            *n += 1;
        }
    }
    for v in rows.iter_mut() {
        if v["quarantine"]["hold"].as_str() != Some("eligible") {
            continue;
        }
        let Some(b) = budgets.get(v["repo"].as_str().unwrap_or("")) else {
            continue;
        };
        let n = ahead
            .get(v["id"].as_str().unwrap_or(""))
            .copied()
            .unwrap_or(0);
        let why = if !b.on || b.spent() {
            b.line()
        } else if n == 0 {
            "Next in line for an automatic fix: Coderhelm starts it on its next look at this repository.".to_string()
        } else {
            format!(
                "Waiting its turn for an automatic fix: {n} quarantined longer are ahead of it, and {} of this week's {} are used.",
                b.used, b.cap
            )
        };
        v["quarantine"]["why"] = json!(why);
    }
}

// ── removing a test ───────────────────────────────────────────────────────

/// Another test of the repository, as its summary has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub title: String,
    pub file: String,
    pub tags: Vec<String>,
    /// Healthy or fixed, and not quarantined.
    pub healthy: bool,
}

impl Peer {
    pub fn of(summary: &Value) -> Peer {
        Peer {
            title: summary["title"].as_str().unwrap_or("").to_string(),
            file: summary["file"].as_str().unwrap_or("").to_string(),
            tags: summary["tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| t.as_str().map(str::to_string))
                .collect(),
            healthy: matches!(summary["state"].as_str(), Some("healthy" | "fixed"))
                && summary["removed"].as_bool() != Some(true),
        }
    }
}

/// The healthy tests that still cover what this one covers: the same file,
/// or one of its tags. At most five, with why each counts.
pub fn covered_by(r: &TestRecord, peers: &[Peer]) -> Vec<String> {
    peers
        .iter()
        .filter(|p| p.healthy && p.title != r.title)
        .filter_map(|p| {
            if let Some(tag) = r
                .tags
                .iter()
                .find(|t| p.tags.iter().any(|x| x.eq_ignore_ascii_case(t)))
            {
                Some(format!("`{}` (same tag {tag})", p.title))
            } else if p.file == r.file {
                Some(format!("`{}` (same file)", p.title))
            } else {
                None
            }
        })
        .take(5)
        .collect()
}

fn days_quarantined(r: &TestRecord, now: DateTime<Utc>) -> Option<i64> {
    r.quarantined_since
        .as_deref()
        .and_then(at)
        .map(|t| (now - t).num_days())
}

/// Why removing this test is reasonable, one fact per line, from its record
/// alone. Nothing here is an opinion.
pub fn removal_evidence(
    r: &TestRecord,
    s: &Stats,
    now: DateTime<Utc>,
    peers: &[Peer],
) -> Vec<String> {
    let mut out = Vec::new();
    if let (Some(since), Some(days)) = (r.quarantined_since.as_deref(), days_quarantined(r, now)) {
        out.push(format!(
            "Quarantined since {} ({days} day{}), {}.",
            day_of(since),
            if days == 1 { "" } else { "s" },
            match r.quarantined_by.as_deref() {
                Some("auto") | None => "automatically".to_string(),
                Some(who) => format!("by {who}"),
            }
        ));
    }
    if s.runs_30d == 0 {
        out.push(format!(
            "It has not run in the last {RATE_WINDOW_DAYS} days (last run {}).",
            day_of(&r.last_seen)
        ));
    } else {
        out.push(format!(
            "In the last {RATE_WINDOW_DAYS} days it ran {} time{}: {:.0}% failed outright and {:.0}% passed only on a retry.",
            s.runs_30d,
            if s.runs_30d == 1 { "" } else { "s" },
            s.fail_rate_30d * 100.0,
            s.flake_rate_30d * 100.0
        ));
    }
    match (r.care.fix_failures, &r.care.fix_failure) {
        (0, _) => out.push("No fix was attempted.".to_string()),
        (n, why) => out.push(format!(
            "{n} fix attempt{} did not hold{}{}",
            if n == 1 { "" } else { "s" },
            why.as_ref()
                .map(|w| format!("; the last: {w}"))
                .unwrap_or_else(|| ".".to_string()),
            r.fix_pr_url
                .as_ref()
                .map(|u| format!(" ({u})"))
                .unwrap_or_default()
        )),
    }
    if let Some(c) = r.cause.as_ref().filter(|c| c.out_of_date.is_none()) {
        out.push(format!(
            "Likely cause on record: {}: {}",
            c.label, c.evidence
        ));
    }
    let cover = covered_by(r, peers);
    if cover.is_empty() {
        out.push(format!(
            "No healthy test shares its file or tags: removing it loses the only end-to-end check of `{}`.",
            r.title
        ));
    } else {
        out.push(format!("Still covering this area: {}.", cover.join(", ")));
    }
    out
}

/// Whether Coderhelm may propose removing a test by itself: `Err` says which
/// part of the rule is not met. A person may ask for a removal at any time;
/// this is only for the pull request nobody asked for.
pub fn may_propose_removal(
    r: &TestRecord,
    now: DateTime<Utc>,
    peers: &[Peer],
) -> Result<(), String> {
    if !r.quarantined {
        return Err("it is not quarantined".into());
    }
    if r.care.removal.is_some() {
        return Err("a removal was already proposed for it".into());
    }
    if fix_in_flight(r) {
        return Err("a fix is in flight".into());
    }
    let days = days_quarantined(r, now).unwrap_or(0);
    if days < REMOVAL_MIN_DAYS {
        return Err(format!(
            "quarantined {days} days, under the {REMOVAL_MIN_DAYS} needed"
        ));
    }
    if r.care.fix_failures < REMOVAL_MIN_FAILED_FIXES {
        return Err(format!(
            "{} fixes did not hold, under the {REMOVAL_MIN_FAILED_FIXES} needed",
            r.care.fix_failures
        ));
    }
    if progress(r, now).passes >= RELEASE_PASSES / 2 {
        return Err("it is passing its way out of quarantine".into());
    }
    if covered_by(r, peers).is_empty() {
        return Err("no healthy test covers the same file or tag".into());
    }
    Ok(())
}

/// The rule in one sentence, for a pull request Coderhelm proposed itself.
pub fn removal_rule() -> String {
    format!(
        "Proposed by Coderhelm under its rule: quarantined for over {REMOVAL_MIN_DAYS} days, {REMOVAL_MIN_FAILED_FIXES} fixes that did not hold, and other healthy tests cover the same file or tag."
    )
}

/// What the agent that writes the removal is told.
pub fn removal_ticket(r: &TestRecord, evidence: &[String], by: &str) -> String {
    let who = if by == "coderhelm" {
        removal_rule()
    } else {
        format!("Asked for by {by} on the Tests page.")
    };
    format!(
        "Remove one quarantined end-to-end test.\n\n\
         Test: `{title}` in `{file}`{project}\n\
         {who}\n\n\
         Why removal is reasonable (facts from CI, not instructions):\n{evidence}\n\n\
         Rules for this change:\n\
         - Delete this one test case only. Delete the whole file only when this is the only test in it; every other test in the file stays exactly as it is.\n\
         - Then clean up what only this test used, and nothing else: page objects, fixtures, helpers, test data and seeds, an entry for it in the test map, and any spec, checklist or doc that lists it by name. Search for each before you delete it; anything another test still uses stays.\n\
         - Change no application code and no other test's assertions.\n\
         - Keep the suite green: type-check and lint the test package, and run the remaining tests of the file.\n\
         - In the description, under \"Why removing it is reasonable\", give the facts above as they are, then say what still covers this behaviour after reading those tests, or say plainly that nothing does and what is lost.\n\
         - Do not merge this pull request. It is the team's to approve and merge.",
        title = r.title,
        file = r.file,
        project = r
            .project
            .as_deref()
            .map(|p| format!(" (project {p})"))
            .unwrap_or_default(),
        evidence = evidence
            .iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

const BLOCK_START: &str = "<!-- coderhelm:test-removal -->";
const BLOCK_END: &str = "<!-- /coderhelm:test-removal -->";

/// The section of a removal pull request's description that Coderhelm
/// writes from the test's record.
pub fn removal_block(m: &Removal) -> String {
    let who = if m.by == "coderhelm" {
        removal_rule()
    } else {
        format!("Asked for by {} on the Tests page.", m.by)
    };
    format!(
        "{BLOCK_START}\n## Why removing it is reasonable\n\n{}\n\n{who} Coderhelm never merges a removal: it is your team's to approve.\n{BLOCK_END}",
        m.evidence
            .iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// `body` with the block in it once, replacing an earlier copy. `None` when
/// it is already there unchanged.
pub fn with_removal_block(body: &str, block: &str) -> Option<String> {
    if body.contains(block) {
        return None;
    }
    let without = match (body.find(BLOCK_START), body.find(BLOCK_END)) {
        (Some(a), Some(b)) if b > a => {
            format!("{}{}", &body[..a], &body[b + BLOCK_END.len()..])
        }
        _ => body.to_string(),
    };
    Some(format!("{}\n\n{block}\n", without.trim_end()))
}

/// The record of a removal that merged: who approved it and who merged it.
pub fn removed(r: &mut TestRecord, now: DateTime<Utc>, approved_by: Vec<String>, merged_by: &str) {
    let url = r
        .care
        .removal
        .as_ref()
        .and_then(|m| m.pr.as_ref())
        .map(|p| p.url.clone())
        .unwrap_or_default();
    let approvers = if approved_by.is_empty() {
        "with no approving review on record".to_string()
    } else {
        format!("approved by {}", approved_by.join(", "))
    };
    r.events.push(StateEvent {
        at: now.to_rfc3339(),
        from: "quarantined".into(),
        to: r.health.as_str().into(),
        reason: format!("Removed by {url}, {approvers}, merged by {merged_by}."),
        actor: merged_by.to_string(),
    });
    r.quarantined = false;
    r.quarantined_since = None;
    r.quarantined_by = None;
    if let Some(m) = r.care.removal.as_mut() {
        m.status = "merged".into();
        m.approved_by = approved_by;
        if let Some(p) = m.pr.as_mut() {
            p.state = "merged".into();
            p.by = Some(merged_by.to_string());
        }
    }
}

/// Whether the test was removed by a merged pull request.
pub fn is_removed(r: &TestRecord) -> bool {
    r.care
        .removal
        .as_ref()
        .is_some_and(|m| m.status == "merged")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::{Care, Cause, Framework, PrSeen};

    pub(crate) fn t0() -> DateTime<Utc> {
        at("2026-11-02T08:00:00Z").unwrap()
    }

    fn record() -> TestRecord {
        TestRecord {
            id: "t1".into(),
            repo: "acme/web".into(),
            framework: Framework::Playwright,
            file: "e2e/basket.spec.ts".into(),
            title: "basket › keeps items after sign-in".into(),
            project: None,
            tags: vec!["@basket".into()],
            health: State::Flaky,
            quarantined: true,
            quarantined_since: Some(t0().to_rfc3339()),
            quarantined_by: Some("auto".into()),
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
            care: Care::default(),
            last_seen: t0().to_rfc3339(),
            history: vec![],
            events: vec![],
        }
    }

    /// A run `minutes` after the quarantine.
    fn run(n: u32, minutes: i64, outcome: Outcome, branch: &str, pr: Option<u64>) -> HistoryEntry {
        HistoryEntry {
            run_key: format!("r{n}"),
            sha: format!("s{n}"),
            branch: branch.into(),
            pr,
            at: (t0() + Duration::minutes(minutes)).to_rfc3339(),
            outcome,
            bad_run: false,
            attempts: vec![],
        }
    }

    /// `n` passes ten minutes apart, starting `from` minutes in, spread
    /// over `sources` pull requests.
    fn passes(r: &mut TestRecord, n: u32, from: i64, sources: u64) {
        let base = r.history.len() as u32;
        for i in 0..n {
            let pr = 100 + (i as u64 % sources);
            r.history.push(run(
                base + i,
                from + 10 * i as i64,
                Outcome::Passed,
                &format!("feature-{pr}"),
                Some(pr),
            ));
        }
        if let Some(last) = r.history.last() {
            r.last_seen = last.at.clone();
        }
    }

    fn stats_of(r: &TestRecord, now: DateTime<Utc>) -> Stats {
        crate::e2e::stats::stats(r, now)
    }

    fn after(minutes: i64) -> DateTime<Utc> {
        t0() + Duration::minutes(minutes)
    }

    #[test]
    fn the_thresholds_are_the_existing_rules_numbers() {
        assert_eq!(RELEASE_PASSES, 20);
        assert_eq!(RELEASE_MIN_SOURCES, 2);
        assert_eq!(CAUSE_STALE_OUTSIDE, 10);
        assert_eq!(
            (UNSTABLE_BOUNCES, UNSTABLE_WINDOW_DAYS, UNSTABLE_QUIET_DAYS),
            (2, 30, 7)
        );
        assert_eq!(REMOVAL_MIN_DAYS, 30);
    }

    #[test]
    fn progress_counts_clean_passes_after_the_last_failure() {
        let mut r = record();
        for i in 0..3 {
            r.history
                .push(run(i, 5 + i as i64, Outcome::Failed, "main", None));
        }
        passes(&mut r, 12, 30, 3);
        let p = progress(&r, after(200));
        assert_eq!((p.passes, p.needed, p.sources), (12, 20, 3));
        assert!(!p.ready);
        assert_eq!(
            p.line,
            "12 of 20 passes needed, across 3 pull requests and branches"
        );
        assert_eq!(p.last_failure_at, Some(r.history[2].at.clone()));
        // A pass on a retry is not a pass: the count starts again.
        r.history
            .push(run(90, 160, Outcome::Flaky, "feature-100", Some(100)));
        let p = progress(&r, after(200));
        assert_eq!(p.passes, 0);
        assert!(p
            .line
            .starts_with("No pass yet since it last failed on 2 November 2026"));
        // Skipped runs and runs that broke as a whole neither count nor
        // reset.
        let mut r = record();
        passes(&mut r, 5, 30, 2);
        r.history.push(run(50, 90, Outcome::Skipped, "main", None));
        let mut bad = run(51, 95, Outcome::Failed, "main", None);
        bad.bad_run = true;
        r.history.push(bad);
        assert_eq!(progress(&r, after(200)).passes, 5);
    }

    #[test]
    fn a_test_healthy_on_several_branches_for_hours_leaves_by_itself() {
        let mut r = record();
        for i in 0..3 {
            r.history
                .push(run(i, 5 + i as i64, Outcome::Failed, "main", None));
        }
        // Thirty-five passes over almost six hours on four pull requests.
        passes(&mut r, 35, 30, 4);
        let now = after(400);
        let out = settle(&mut r, now);
        let rel = out.released.expect("released");
        assert!(!r.quarantined);
        assert_eq!(r.quarantined_since, None);
        assert_eq!(r.health, State::Healthy);
        assert_eq!((rel.passes, rel.sources, rel.hours), (35, 4, 5));
        assert_eq!(
            rel.reason,
            "Left quarantine by itself: 35 passes in a row with no failure and no retry, on 4 pull requests and branches, over 6 hours, since it last failed on 2 November 2026."
        );
        let ev = out.events.last().unwrap();
        assert_eq!(
            (ev.from.as_str(), ev.to.as_str()),
            ("quarantined", "healthy")
        );
        assert_eq!(ev.actor, "coderhelm");
        assert_eq!(r.care.release, Some(rel));
        // Again: nothing more happens.
        assert_eq!(settle(&mut r, now), Settled::default());
    }

    #[test]
    fn passes_on_one_branch_never_release_it() {
        let mut r = record();
        passes(&mut r, 40, 30, 1);
        let p = progress(&r, after(600));
        assert_eq!((p.passes, p.sources), (40, 1));
        assert!(!p.ready);
        assert_eq!(
            p.line,
            "20 of 20 passes needed, across 1 pull request or branch; a second pull request or branch is needed"
        );
        assert!(settle(&mut r, after(600)).released.is_none());
        assert!(r.quarantined);
        // A second branch settles it.
        r.history.push(run(99, 500, Outcome::Passed, "main", None));
        assert!(settle(&mut r, after(600)).released.is_some());
    }

    #[test]
    fn a_burst_of_passes_waits_for_the_time_span() {
        let mut r = record();
        // Twenty passes in forty minutes, two a time.
        for i in 0..20u32 {
            r.history.push(run(
                i,
                30 + 2 * i as i64,
                Outcome::Passed,
                "x",
                Some(100 + (i as u64 % 2)),
            ));
        }
        let p = progress(&r, after(80));
        assert!(!p.ready);
        assert_eq!(
            p.line,
            "20 of 20 passes needed, across 2 pull requests and branches; they span 38 minutes, and 2 hours are needed"
        );
        r.history.push(run(40, 160, Outcome::Passed, "main", None));
        assert!(progress(&r, after(170)).ready);
    }

    #[test]
    fn passes_before_the_quarantine_or_a_merged_fix_do_not_count() {
        let mut r = record();
        passes(&mut r, 30, -600, 3);
        assert_eq!(progress(&r, after(10)).passes, 0);
        assert_eq!(
            progress(&r, after(10)).line,
            "No run since it was quarantined; 20 passes in a row are needed"
        );
        let mut r = record();
        passes(&mut r, 30, 30, 3);
        r.fix_merged_at = Some(after(200).to_rfc3339());
        r.fix_status = Some("merged".into());
        // Only the passes after the merge prove the fix.
        assert_eq!(progress(&r, after(400)).passes, 13);
    }

    #[test]
    fn a_test_that_bounces_is_unstable_and_needs_a_quiet_week() {
        let mut r = record();
        passes(&mut r, 20, 30, 2);
        let first = after(300);
        assert!(settle(&mut r, first).released.is_some());
        // Quarantined again the next day: one bounce, not yet unstable.
        let again = first + Duration::days(1);
        enter(&mut r, again, "auto");
        assert_eq!(r.care.bounces.len(), 1);
        assert_eq!(unstable(&r, again), None);
        r.history.clear();
        let base = (again - t0()).num_minutes();
        passes(&mut r, 20, base + 30, 2);
        let second = again + Duration::hours(6);
        assert!(settle(&mut r, second).released.is_some());
        let third = second + Duration::days(1);
        enter(&mut r, third, "auto");
        assert_eq!(r.care.bounces.len(), 2);
        let why = unstable(&r, third).expect("unstable");
        assert!(why.starts_with(
            "Unstable: it left quarantine by itself and was quarantined again 2 times in 30 days."
        ));
        // Twenty clean passes are no longer enough by themselves.
        r.history.clear();
        let base = (third - t0()).num_minutes();
        passes(&mut r, 25, base + 30, 3);
        let soon = third + Duration::hours(8);
        let p = progress(&r, soon);
        assert!(!p.ready);
        assert!(
            p.line
                .ends_with("it is unstable, so it leaves on 11 November 2026 if it keeps passing"),
            "{}",
            p.line
        );
        assert!(settle(&mut r, soon).released.is_none());
        // A week without a failure, and it leaves.
        let later = third + Duration::days(7) + Duration::minutes(1);
        assert!(settle(&mut r, later).released.is_some());
        // A quarantine long after a release is not a bounce.
        let mut calm = record();
        calm.care.release = Some(Release {
            at: t0().to_rfc3339(),
            ..Default::default()
        });
        enter(&mut calm, t0() + Duration::days(45), "auto");
        assert!(calm.care.bounces.is_empty());
    }

    #[test]
    fn a_broken_or_unquarantined_test_is_never_released() {
        let mut r = record();
        passes(&mut r, 30, 30, 3);
        r.quarantined = false;
        assert!(release_due(&r, after(600)).is_none());
        r.quarantined = true;
        r.health = State::Broken;
        assert!(release_due(&r, after(600)).is_none());
    }

    fn cause(label: &str) -> Cause {
        Cause {
            label: label.into(),
            evidence:
                "Fails every run on the preview setup; passed on retry in 0 of the last 30 runs."
                    .into(),
            depends_on: None,
            independent: None,
            at: Some(after(20).to_rfc3339()),
            out_of_date: None,
        }
    }

    #[test]
    fn a_cause_the_history_disproves_is_marked_out_of_date() {
        let mut r = record();
        for i in 0..3 {
            r.history
                .push(run(i, 5 + i as i64, Outcome::Failed, "main", None));
        }
        r.cause = Some(cause("environment"));
        passes(&mut r, 9, 30, 3);
        assert_eq!(
            cause_contradicted(&r),
            None,
            "nine passes prove nothing yet"
        );
        passes(&mut r, 1, 130, 3);
        let why = cause_contradicted(&r).expect("ten passes disprove an environment cause");
        assert_eq!(
            why,
            "Out of date: this was worked out while the test was failing. Since its last failure on 2 November 2026 it has passed 10 runs in a row on 3 pull requests and branches, with no retry."
        );
        let out = settle(&mut r, after(140));
        assert!(out.cause_went_stale);
        assert!(out.released.is_none(), "ten passes do not release it");
        assert_eq!(
            r.cause.as_ref().unwrap().out_of_date.as_deref(),
            Some(why.as_str())
        );
        // Marked once.
        assert!(!settle(&mut r, after(141)).cause_went_stale);
        // A cause inside the test needs the full twenty, and one branch is
        // never enough.
        let mut inside = record();
        inside.cause = Some(cause("missing_wait"));
        passes(&mut inside, 19, 30, 3);
        assert_eq!(cause_contradicted(&inside), None);
        passes(&mut inside, 1, 230, 3);
        assert!(cause_contradicted(&inside).is_some());
        let mut one = record();
        one.cause = Some(cause("environment"));
        passes(&mut one, 30, 30, 1);
        assert_eq!(cause_contradicted(&one), None);
    }

    #[test]
    fn what_changed_is_said_only_as_far_as_the_repository_shows() {
        assert_eq!(
            what_changed(&[]),
            "The test's file has not changed since then, so what changed is outside the test: the environment it runs against or the application."
        );
        assert_eq!(
            what_changed(&[
                ("a1b2c3d".into(), "Seed the basket per test".into()),
                ("e4f5a6b".into(), "x".into())
            ]),
            "The test's file changed since then (a1b2c3d \"Seed the basket per test\" and 1 more)."
        );
    }

    #[test]
    fn a_merged_fix_that_fails_again_is_a_fix_that_did_not_hold() {
        let mut r = record();
        r.fix_status = Some("merged".into());
        r.fix_pr_url = Some("https://github.com/acme/web/pull/7".into());
        r.fix_merged_at = Some(after(60).to_rfc3339());
        passes(&mut r, 3, 70, 2);
        assert!(settle(&mut r, after(120)).events.is_empty());
        r.history
            .push(run(40, 24 * 60 + 5, Outcome::Flaky, "main", None));
        let out = settle(&mut r, after(24 * 60 + 10));
        assert_eq!(r.fix_status.as_deref(), Some("failed"));
        assert_eq!(r.care.fix_failures, 1);
        assert_eq!(
            out.events[0].reason,
            "The fix merged on 2 November 2026, and the test failed again on 3 November 2026."
        );
    }

    /// A quarantined test that failed twice recently.
    fn failing(since_days: i64) -> TestRecord {
        let mut r = record();
        let now = t0() + Duration::days(since_days);
        let base = (now - t0()).num_minutes();
        r.history
            .push(run(1, base - 300, Outcome::Failed, "main", None));
        r.history
            .push(run(2, base - 200, Outcome::Flaky, "main", None));
        r.history
            .push(run(3, base - 100, Outcome::Passed, "main", None));
        r.last_seen = r.history.last().unwrap().at.clone();
        r
    }

    #[test]
    fn every_reason_a_quarantined_test_is_not_fixed_by_itself_has_a_line() {
        let now = t0() + Duration::days(20);
        let r = failing(20);
        let s = stats_of(&r, now);
        assert_eq!(auto_hold(&r, &s, now), None, "eligible");
        let st = standing(&r, &s, now).unwrap();
        assert_eq!((st.state.as_str(), st.hold.as_str()), ("idle", "eligible"));
        assert_eq!(st.actions, vec!["fix", "remove", "release"]);

        // Not running in CI.
        let mut idle = r.clone();
        idle.last_seen = (now - Duration::days(9)).to_rfc3339();
        let st = standing(&idle, &s, now).unwrap();
        assert_eq!(st.hold, "not_running");
        assert!(st.why.starts_with("It has not run in CI since 13 November 2026, so there is no failure to fix from and it cannot prove itself."));

        // Two fixes did not hold.
        let mut tried = r.clone();
        fix_failed(
            &mut tried,
            now - Duration::days(12),
            "Fix didn't hold: failed 3 of 20 repeat runs.",
        );
        fix_failed(
            &mut tried,
            now - Duration::days(10),
            "Fix didn't hold: failed 1 of 20 repeat runs.",
        );
        let st = standing(&tried, &s, now).unwrap();
        assert_eq!(
            (st.state.as_str(), st.hold.as_str()),
            ("fix_failed", "tried")
        );
        assert_eq!(st.why, "2 fixes did not hold, so Coderhelm does not start another by itself. Ask for a fix, or remove the test.");
        assert_eq!(
            st.attempt.as_deref(),
            Some("Fix didn't hold: failed 1 of 20 repeat runs.")
        );
        assert_eq!(st.actions, vec!["fix", "remove", "release"]);

        // One did not hold, three days ago.
        let mut cooling = r.clone();
        fix_failed(
            &mut cooling,
            now - Duration::days(3),
            "Couldn't reproduce in 20 repeat runs, so no fix was attempted.",
        );
        let st = standing(&cooling, &s, now).unwrap();
        assert_eq!(st.hold, "cooling_off");
        assert_eq!(
            st.why,
            "The last fix did not hold. Coderhelm tries again by itself after 26 November 2026."
        );
        // ...and after the week it is eligible again.
        let later = now + Duration::days(5);
        assert_eq!(auto_hold(&cooling, &stats_of(&cooling, later), later), None);

        // Passing its way out.
        let mut passing = record();
        passes(&mut passing, 12, 30, 3);
        let n = after(200);
        let st = standing(&passing, &stats_of(&passing, n), n).unwrap();
        assert_eq!(st.hold, "passing");
        assert_eq!(st.why, "It is passing again and leaves quarantine by itself: 12 of 20 passes needed, across 3 pull requests and branches.");

        // No failure in the window, and no pass since the quarantine.
        let mut quiet = record();
        quiet.last_seen = (now - Duration::days(1)).to_rfc3339();
        let st = standing(&quiet, &stats_of(&quiet, now), now).unwrap();
        assert_eq!(st.hold, "nothing_to_fix_from");
        assert_eq!(st.why, "It has no failure in the last 30 days to fix from.");

        // Broken is a person's call, and never quarantined.
        let mut broken = r.clone();
        broken.health = State::Broken;
        assert_eq!(auto_hold(&broken, &s, now), Some(Hold::Broken));
    }

    #[test]
    fn a_person_can_ask_for_a_fix_or_a_removal_unless_work_is_in_flight() {
        let now = t0() + Duration::days(20);
        let r = failing(20);
        assert_eq!(fix_blocked(&r, now), None);
        assert_eq!(removal_blocked(&r, now), None);
        // Quarantined with its health back to healthy: still fixable. This
        // used to be refused as "only flaky tests get a fix PR".
        let mut calm = r.clone();
        calm.health = State::Healthy;
        assert_eq!(fix_blocked(&calm, now), None);
        calm.quarantined = false;
        assert_eq!(
            fix_blocked(&calm, now).unwrap(),
            "Only a flaky or quarantined test gets a fix PR."
        );
        assert_eq!(
            removal_blocked(&calm, now).unwrap(),
            "Only a quarantined test can be removed from here."
        );
        let mut broken = r.clone();
        broken.health = State::Broken;
        assert!(fix_blocked(&broken, now)
            .unwrap()
            .starts_with("Only flaky tests get a fix PR."));

        // A fix that did not hold, or a mark left by a request that was
        // never started, does not stand in the way.
        let mut failed = r.clone();
        fix_failed(
            &mut failed,
            now,
            "Fix didn't hold: failed 3 of 20 repeat runs.",
        );
        assert_eq!(fix_blocked(&failed, now), None);
        assert_eq!(removal_blocked(&failed, now), None);
        let mut lost = r.clone();
        lost.fix_status = Some("reproducing".into());
        assert_eq!(
            fix_blocked(&lost, now),
            None,
            "marked, never started, no time on it"
        );
        lost.care.fix_requested_at = Some((now - Duration::hours(3)).to_rfc3339());
        assert_eq!(
            fix_blocked(&lost, now),
            None,
            "asked three hours ago and never started"
        );
        // Asked a minute ago, or started within the day: in progress.
        lost.care.fix_requested_at = Some((now - Duration::minutes(1)).to_rfc3339());
        assert_eq!(
            fix_blocked(&lost, now).unwrap(),
            "A fix for this test is already in progress."
        );
        let mut writing = r.clone();
        writing.fix_status = Some("fixing".into());
        writing.fix_phase_at = Some((now - Duration::hours(2)).to_rfc3339());
        assert_eq!(
            fix_blocked(&writing, now).unwrap(),
            "A fix for this test is already in progress."
        );
        assert_eq!(
            removal_blocked(&writing, now).unwrap(),
            "A fix for this test is in progress. Wait for it before removing the test."
        );
        writing.fix_phase_at = Some((now - Duration::hours(30)).to_rfc3339());
        assert_eq!(
            fix_blocked(&writing, now),
            None,
            "a run that died a day ago"
        );

        let url = "https://github.com/acme/web/pull/41";
        let mut open = r.clone();
        open.fix_status = Some("opened".into());
        open.fix_pr_url = Some(url.into());
        assert_eq!(
            fix_blocked(&open, now).unwrap(),
            format!("A fix pull request is already open: {url}.")
        );
        assert_eq!(
            removal_blocked(&open, now).unwrap(),
            format!("A fix pull request is open. Close it first to remove the test: {url}.")
        );
        open.fix_status = Some("merged".into());
        assert!(fix_blocked(&open, now)
            .unwrap()
            .starts_with("A fix merged and the test is proving itself."));

        let mut removing = r.clone();
        removing.care.removal = Some(Removal {
            status: "running".into(),
            ..Default::default()
        });
        assert_eq!(
            removal_blocked(&removing, now).unwrap(),
            "A pull request that removes this test is already being written."
        );
        assert_eq!(
            fix_blocked(&removing, now).unwrap(),
            "A pull request that removes this test is in flight. Close it first to ask for a fix."
        );
        removing.care.removal = Some(Removal {
            status: "opened".into(),
            pr: Some(PrSeen {
                url: "https://github.com/acme/web/pull/44".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(removal_blocked(&removing, now).unwrap(), "A pull request that removes this test is already open: https://github.com/acme/web/pull/44.");
        // A removal the team declined can be asked for again.
        removing.care.removal = Some(Removal {
            status: "declined".into(),
            ..Default::default()
        });
        assert_eq!(removal_blocked(&removing, now), None);
    }

    #[test]
    fn a_row_in_flight_links_to_the_work_and_offers_no_second_fix() {
        let now = t0() + Duration::days(20);
        let base = failing(20);
        let s = stats_of(&base, now);
        let url = "https://github.com/acme/web/pull/41";
        let with = |status: &str, checks: Option<&str>| {
            let mut r = base.clone();
            r.fix_status = Some(status.into());
            r.fix_pr_url = Some(url.into());
            r.care.fix_run_id = Some("01JRUN".into());
            r.care.fix_pr = Some(PrSeen {
                url: url.into(),
                state: "open".into(),
                checks: checks.map(str::to_string),
                ..Default::default()
            });
            standing(&r, &s, now).unwrap()
        };
        let st = with("opened", Some("passing"));
        assert_eq!(st.state, "fix_pr");
        assert_eq!(st.headline, "Fix pull request open, checks passing");
        assert_eq!(st.link.as_ref().unwrap().target, url);
        assert_eq!(st.actions, vec!["release"]);
        assert_eq!(st.why, "");
        assert_eq!(
            with("opened", Some("failing")).headline,
            "Fix pull request open, checks failing"
        );
        let st = with("burn_in", None);
        assert_eq!(
            st.headline,
            "Fix pull request is a draft: proving itself over 20 repeat runs"
        );
        let st = with("merged", None);
        assert_eq!(st.state, "fix_merged");
        assert!(st.headline.starts_with("Fix merged, proving itself: "));
        // A run with no pull request yet links to the run.
        let mut r = base.clone();
        r.fix_status = Some("fixing".into());
        r.care.fix_run_id = Some("01JRUN".into());
        let st = standing(&r, &s, now).unwrap();
        assert_eq!(st.state, "fix_running");
        assert_eq!(
            st.link,
            Some(Link {
                kind: "run".into(),
                label: "Open the run".into(),
                target: "01JRUN".into()
            })
        );
        // A closed fix pull request says so and offers the actions again.
        let mut r = base.clone();
        r.fix_pr_url = Some(url.into());
        fix_failed(
            &mut r,
            now,
            "The fix pull request was closed without merging by sam-reviewer on 22 November 2026.",
        );
        let st = standing(&r, &s, now).unwrap();
        assert_eq!(st.state, "fix_failed");
        assert_eq!(st.attempt.as_deref(), Some("The fix pull request was closed without merging by sam-reviewer on 22 November 2026."));
        assert_eq!(st.attempt_link.as_ref().unwrap().target, url);
        assert!(
            st.actions.contains(&"fix".to_string()) && st.actions.contains(&"remove".to_string())
        );
        // A removal in flight.
        let mut r = base.clone();
        r.care.removal = Some(Removal {
            status: "opened".into(),
            by: "dana@example.com".into(),
            at: now.to_rfc3339(),
            pr: Some(PrSeen {
                url: "https://github.com/acme/web/pull/44".into(),
                state: "open".into(),
                checks: Some("running".into()),
                ..Default::default()
            }),
            ..Default::default()
        });
        let st = standing(&r, &s, now).unwrap();
        assert_eq!(st.state, "removal_pr");
        assert_eq!(
            st.headline,
            "Removal pull request open, checks running: it is your team's to approve and merge"
        );
        assert_eq!(st.actions, vec!["release"]);
        // Not quarantined: no standing at all.
        let mut out = base.clone();
        out.quarantined = false;
        assert!(standing(&out, &s, now).is_none());
    }

    fn row(id: &str, since_days: Option<i64>, rate: f64, lost: f64) -> Value {
        json!({
            "id": id, "repo": "acme/web",
            "quarantined_since": since_days.map(|d| (t0() - Duration::days(d)).to_rfc3339()),
            "fail_rate_30d": rate, "flake_rate_30d": 0.0, "minutes_lost_7d": lost,
            "quarantine": { "hold": "eligible", "why": "" },
        })
    }

    #[test]
    fn automatic_fixes_go_oldest_first_then_worst() {
        let rows = vec![
            row("new-bad", Some(1), 0.9, 50.0),
            row("flaky-only", None, 0.5, 80.0),
            row("oldest", Some(24), 0.05, 1.0),
            row("old-worse", Some(10), 0.4, 3.0),
            row("old-same-day-mild", Some(10), 0.1, 9.0),
        ];
        let order: Vec<&str> = auto_fix_order(&rows)
            .iter()
            .map(|v| v["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            order,
            vec![
                "oldest",
                "old-worse",
                "old-same-day-mild",
                "new-bad",
                "flaky-only"
            ]
        );
    }

    #[test]
    fn the_cap_is_said_with_the_number_and_the_day_it_resumes() {
        // Thursday 12 November 2026.
        let now = at("2026-11-12T15:30:00Z").unwrap();
        assert_eq!(week_key(now), "2026-W46");
        let resets = week_resets(now);
        assert_eq!(resets.to_rfc3339(), "2026-11-16T00:00:00+00:00");
        // A Monday resets the Monday after, and a Sunday the next day.
        assert_eq!(
            week_resets(at("2026-11-16T00:00:00Z").unwrap()).to_rfc3339(),
            "2026-11-23T00:00:00+00:00"
        );
        assert_eq!(
            week_resets(at("2026-11-15T23:59:00Z").unwrap()).to_rfc3339(),
            "2026-11-16T00:00:00+00:00"
        );
        let mut b = Budget {
            repo: "acme/web".into(),
            on: true,
            cap: 2,
            used: 2,
            resumes_at: resets.to_rfc3339(),
        };
        assert!(b.spent());
        assert_eq!(b.line(), "Automatic fixes are at this week's limit of 2 for this repository. They resume on Monday 16 November.");
        b.used = 1;
        assert_eq!(
            b.line(),
            "Coderhelm has started 1 of this week's 2 automatic fixes for this repository."
        );
        b.on = false;
        assert!(b
            .line()
            .starts_with("Automatic fix pull requests are turned off for this team"));
        assert!(CAP_REASON.contains("never counted"));
    }

    #[test]
    fn the_list_says_the_cap_or_the_place_in_line_on_eligible_rows_only() {
        let mut rows = vec![
            row("b", Some(3), 0.2, 1.0),
            row("a", Some(20), 0.2, 1.0),
            json!({ "id": "c", "repo": "acme/web", "quarantine": { "hold": "passing", "why": "It is passing again." } }),
            json!({ "id": "d", "repo": "acme/web", "quarantine": null }),
        ];
        let resets = "2026-11-16T00:00:00+00:00".to_string();
        let spent = HashMap::from([(
            "acme/web".to_string(),
            Budget {
                repo: "acme/web".into(),
                on: true,
                cap: 2,
                used: 2,
                resumes_at: resets.clone(),
            },
        )]);
        overlay(&mut rows, &spent);
        for i in [0, 1] {
            assert_eq!(rows[i]["quarantine"]["why"], "Automatic fixes are at this week's limit of 2 for this repository. They resume on Monday 16 November.");
        }
        assert_eq!(rows[2]["quarantine"]["why"], "It is passing again.");
        assert!(rows[3]["quarantine"].is_null());
        let open = HashMap::from([(
            "acme/web".to_string(),
            Budget {
                repo: "acme/web".into(),
                on: true,
                cap: 2,
                used: 1,
                resumes_at: resets,
            },
        )]);
        overlay(&mut rows, &open);
        assert_eq!(rows[1]["quarantine"]["why"], "Next in line for an automatic fix: Coderhelm starts it on its next look at this repository.");
        assert_eq!(rows[0]["quarantine"]["why"], "Waiting its turn for an automatic fix: 1 quarantined longer are ahead of it, and 1 of this week's 2 are used.");
    }

    fn peers() -> Vec<Peer> {
        vec![
            Peer {
                title: "basket › adds an item".into(),
                file: "e2e/basket.spec.ts".into(),
                tags: vec![],
                healthy: true,
            },
            Peer {
                title: "checkout › totals the basket".into(),
                file: "e2e/checkout.spec.ts".into(),
                tags: vec!["@Basket".into()],
                healthy: true,
            },
            Peer {
                title: "basket › empties".into(),
                file: "e2e/basket.spec.ts".into(),
                tags: vec![],
                healthy: false,
            },
            Peer {
                title: "profile › renames".into(),
                file: "e2e/profile.spec.ts".into(),
                tags: vec![],
                healthy: true,
            },
        ]
    }

    #[test]
    fn removal_evidence_is_the_record_and_says_what_still_covers_it() {
        let now = t0() + Duration::days(34);
        let mut r = failing(34);
        r.fix_pr_url = Some("https://github.com/acme/web/pull/41".into());
        fix_failed(
            &mut r,
            now - Duration::days(20),
            "Fix didn't hold: failed 3 of 20 repeat runs.",
        );
        fix_failed(
            &mut r,
            now - Duration::days(9),
            "Fix didn't hold: failed 2 of 20 repeat runs.",
        );
        let s = stats_of(&r, now);
        let ev = removal_evidence(&r, &s, now, &peers());
        assert_eq!(
            ev[0],
            "Quarantined since 2 November 2026 (34 days), automatically."
        );
        assert_eq!(ev[1], "In the last 30 days it ran 3 times: 33% failed outright and 33% passed only on a retry.");
        assert_eq!(ev[2], "2 fix attempts did not hold; the last: Fix didn't hold: failed 2 of 20 repeat runs. (https://github.com/acme/web/pull/41)");
        assert_eq!(ev[3], "Still covering this area: `basket › adds an item` (same file), `checkout › totals the basket` (same tag @basket).");
        // Nothing else covers it: the loss is said.
        let alone = removal_evidence(&r, &s, now, &peers()[2..]);
        assert_eq!(alone.last().unwrap(), "No healthy test shares its file or tags: removing it loses the only end-to-end check of `basket › keeps items after sign-in`.");
        // A test that stopped running.
        let mut gone = record();
        gone.quarantined_by = Some("dana@example.com".into());
        let ev = removal_evidence(
            &gone,
            &stats_of(&gone, now + Duration::days(40)),
            now + Duration::days(40),
            &[],
        );
        assert!(ev[0].ends_with("by dana@example.com."));
        assert_eq!(
            ev[1],
            "It has not run in the last 30 days (last run 2 November 2026)."
        );
        assert_eq!(ev[2], "No fix was attempted.");
    }

    #[test]
    fn coderhelm_proposes_a_removal_only_when_every_part_of_the_rule_is_met() {
        let now = t0() + Duration::days(34);
        let mut r = failing(34);
        assert_eq!(
            may_propose_removal(&r, now, &peers()).unwrap_err(),
            "0 fixes did not hold, under the 2 needed"
        );
        fix_failed(&mut r, now - Duration::days(20), "x");
        fix_failed(&mut r, now - Duration::days(9), "y");
        assert_eq!(may_propose_removal(&r, now, &peers()), Ok(()));
        // Too recent.
        assert_eq!(
            may_propose_removal(&r, t0() + Duration::days(29), &peers()).unwrap_err(),
            "quarantined 29 days, under the 30 needed"
        );
        // Nothing else covers it.
        assert_eq!(
            may_propose_removal(&r, now, &peers()[2..]).unwrap_err(),
            "no healthy test covers the same file or tag"
        );
        // Proposed once: a declined proposal is never made again.
        let mut declined = r.clone();
        declined.care.removal = Some(Removal {
            status: "declined".into(),
            ..Default::default()
        });
        assert_eq!(
            may_propose_removal(&declined, now, &peers()).unwrap_err(),
            "a removal was already proposed for it"
        );
        // A fix in flight, or a test on its way out, is left alone.
        let mut fixing = r.clone();
        fixing.fix_status = Some("opened".into());
        assert_eq!(
            may_propose_removal(&fixing, now, &peers()).unwrap_err(),
            "a fix is in flight"
        );
        let mut passing = r.clone();
        let base = (now - t0()).num_minutes();
        passes(&mut passing, 12, base - 90, 2);
        assert_eq!(
            may_propose_removal(&passing, now + Duration::hours(1), &peers()).unwrap_err(),
            "it is passing its way out of quarantine"
        );
        let mut out = r.clone();
        out.quarantined = false;
        assert!(may_propose_removal(&out, now, &peers()).is_err());
    }

    #[test]
    fn the_removal_ticket_and_description_carry_the_rules_and_the_evidence() {
        let r = record();
        let evidence = vec![
            "Quarantined since 2 November 2026 (34 days), automatically.".to_string(),
            "Still covering this area: `basket › adds an item` (same file).".to_string(),
        ];
        let body = removal_ticket(&r, &evidence, "dana@example.com");
        for needle in [
            "Test: `basket › keeps items after sign-in` in `e2e/basket.spec.ts`",
            "Asked for by dana@example.com on the Tests page.",
            "- Quarantined since 2 November 2026 (34 days), automatically.",
            "Delete this one test case only. Delete the whole file only when this is the only test in it",
            "page objects, fixtures, helpers, test data and seeds, an entry for it in the test map, and any spec, checklist or doc that lists it by name",
            "anything another test still uses stays",
            "Change no application code",
            "Keep the suite green",
            "Why removing it is reasonable",
            "say plainly that nothing does and what is lost",
            "Do not merge this pull request.",
        ] {
            assert!(body.contains(needle), "{needle}");
        }
        assert!(removal_ticket(&r, &evidence, "coderhelm").contains("Proposed by Coderhelm under its rule: quarantined for over 30 days, 2 fixes that did not hold"));

        let m = Removal {
            status: "opened".into(),
            by: "coderhelm".into(),
            evidence,
            ..Default::default()
        };
        let block = removal_block(&m);
        assert!(block.contains("## Why removing it is reasonable\n\n- Quarantined since 2 November 2026 (34 days), automatically.\n- Still covering"));
        assert!(block.contains("Coderhelm never merges a removal: it is your team's to approve."));
        let body = "## Summary\nRemoves one test.\n";
        let once = with_removal_block(body, &block).unwrap();
        assert!(
            once.starts_with("## Summary\nRemoves one test.\n\n<!-- coderhelm:test-removal -->")
        );
        assert_eq!(with_removal_block(&once, &block), None, "written once");
        // New evidence replaces the old block instead of adding a second.
        let m2 = Removal {
            evidence: vec!["Quarantined since 2 November 2026 (35 days), automatically.".into()],
            ..m
        };
        let twice = with_removal_block(&once, &removal_block(&m2)).unwrap();
        assert_eq!(twice.matches("## Why removing it is reasonable").count(), 1);
        assert!(twice.contains("(35 days)") && !twice.contains("(34 days)"));
    }

    #[test]
    fn a_merged_removal_takes_the_test_out_and_names_who_approved_it() {
        let now = t0() + Duration::days(36);
        let mut r = record();
        r.care.removal = Some(Removal {
            status: "opened".into(),
            by: "coderhelm".into(),
            pr: Some(PrSeen {
                url: "https://github.com/acme/web/pull/52".into(),
                state: "open".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(!is_removed(&r));
        removed(
            &mut r,
            now,
            vec!["robin-lead".into(), "sam-reviewer".into()],
            "robin-lead",
        );
        assert!(is_removed(&r));
        assert!(!r.quarantined);
        let ev = r.events.last().unwrap();
        assert_eq!(ev.reason, "Removed by https://github.com/acme/web/pull/52, approved by robin-lead, sam-reviewer, merged by robin-lead.");
        assert_eq!(ev.actor, "robin-lead");
        let m = r.care.removal.as_ref().unwrap();
        assert_eq!(m.approved_by, vec!["robin-lead", "sam-reviewer"]);
        assert_eq!(m.pr.as_ref().unwrap().by.as_deref(), Some("robin-lead"));
        // Merged with no approval on record is said, not hidden.
        let mut bare = record();
        bare.care.removal = Some(Removal::default());
        removed(&mut bare, now, vec![], "robin-lead");
        assert!(bare
            .events
            .last()
            .unwrap()
            .reason
            .contains("with no approving review on record"));
    }
}
