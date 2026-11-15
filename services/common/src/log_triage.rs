//! What a log finding's latest sighting is worth: noise, watch or act, read
//! against the finding's own week, the way an engineer on call reads it.
//! The analyzer makes the same decision before it tells anyone
//! (`lambda/log-analyzer/triage.py`); both read the same cases
//! (`tests/fixtures/log_triage_cases.json`).
//!
//! - noise: it happens most days and this is its usual rate. Recorded,
//!   nobody is told, no fix starts.
//! - watch: new and not critical, back after a quiet week, or above its
//!   usual rate but still small.
//! - act: new and critical, or rising well above its usual rate.

use chrono::{Duration, NaiveDate};
use serde::Serialize;

use crate::log_findings::{Counted, Finding};

/// The days before today a sighting is compared with.
pub const BASELINE_DAYS: i64 = 7;
/// Seen on at least this many of those days: part of the usual day.
pub const USUAL_DAYS: usize = 2;
/// Rising: this many times its usual day, and at least this many lines.
pub const RISING_FACTOR: f64 = 3.0;
pub const RISING_MIN_LINES: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Noise,
    Watch,
    Act,
}

impl Decision {
    pub fn word(self) -> &'static str {
        match self {
            Decision::Noise => "Noise",
            Decision::Watch => "Watch",
            Decision::Act => "Act",
        }
    }
}

/// A decision and its one-sentence reason, with the numbers behind it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Triage {
    pub decision: Decision,
    pub reason: String,
    /// Lines a day on average over the days before.
    pub per_day: f64,
    /// Days with a line among them.
    pub days_seen: usize,
    pub lines: u64,
}

impl Triage {
    /// The card's first line: `Act: Rising: 400 lines against …`.
    pub fn line(&self) -> String {
        format!("{}: {}", self.decision.word(), self.reason)
    }
}

/// Pure: (lines a day on average, days seen) over the days before `today`.
pub fn baseline(history: &[Counted], today: NaiveDate) -> (f64, usize) {
    let mut total = 0u64;
    let mut seen = 0usize;
    for i in 1..=BASELINE_DAYS {
        let day = (today - Duration::days(i)).format("%Y-%m-%d").to_string();
        let n: u64 = history
            .iter()
            .filter(|c| c.name == day)
            .map(|c| c.lines)
            .sum();
        total += n;
        if n > 0 {
            seen += 1;
        }
    }
    (total as f64 / BASELINE_DAYS as f64, seen)
}

fn lines_word(n: u64) -> String {
    format!("{n} line{}", if n == 1 { "" } else { "s" })
}

fn per_day_words(avg: f64) -> String {
    if avg <= 0.0 {
        "none".into()
    } else if avg < 1.0 {
        "less than one a day".into()
    } else {
        format!("about {} a day", (avg + 0.5).floor() as u64)
    }
}

/// Pure: the decision for a sighting of `lines` lines, against `history`
/// (the days before it); `new` when the finding has no history at all.
pub fn decide(
    history: &[Counted],
    lines: u64,
    today: NaiveDate,
    new: bool,
    critical: bool,
) -> Triage {
    let (avg, seen) = baseline(history, today);
    let t = |decision, reason: String| Triage {
        decision,
        reason,
        per_day: avg,
        days_seen: seen,
        lines,
    };
    let n = lines_word(lines);
    if new || history.is_empty() {
        return if critical {
            t(Decision::Act, format!("New and critical: {n}."))
        } else {
            t(Decision::Watch, format!("New: {n}, not seen before."))
        };
    }
    if lines >= RISING_MIN_LINES && lines as f64 > RISING_FACTOR * avg.max(1.0) {
        return t(
            Decision::Act,
            format!(
                "Rising: {n} against {} over the last {BASELINE_DAYS} days.",
                per_day_words(avg)
            ),
        );
    }
    if seen >= USUAL_DAYS {
        return t(
            Decision::Noise,
            format!(
                "At its usual rate: {n}, {} on {seen} of the last {BASELINE_DAYS} days.",
                per_day_words(avg)
            ),
        );
    }
    if seen == 0 {
        return t(
            Decision::Watch,
            format!("Back after a quiet week: {n}, none in the last {BASELINE_DAYS} days."),
        );
    }
    t(
        Decision::Watch,
        format!("Seen again: {n}, on {seen} of the last {BASELINE_DAYS} days."),
    )
}

/// Pure: a finding's latest day against the days before it.
pub fn of_finding(f: &Finding, today: NaiveDate) -> Triage {
    let day = today.format("%Y-%m-%d").to_string();
    let lines: u64 = f
        .daily_lines
        .iter()
        .filter(|c| c.name == day)
        .map(|c| c.lines)
        .sum();
    let before: Vec<Counted> = f
        .daily_lines
        .iter()
        .filter(|c| c.name != day)
        .cloned()
        .collect();
    let new = before.is_empty() && f.reopen_count == 0;
    decide(
        &before,
        lines,
        today,
        new,
        f.severity == crate::log_findings::Severity::Critical,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shared_cases_decide_the_same_here() {
        let today = NaiveDate::from_ymd_opt(2026, 11, 14).unwrap();
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/log_triage_cases.json")).unwrap();
        for c in cases["cases"].as_array().unwrap() {
            let new = c["new"].as_bool().unwrap_or(false);
            let mut history: Vec<Counted> = c["history"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, n)| Counted {
                    name: (today - Duration::days(i as i64 + 1))
                        .format("%Y-%m-%d")
                        .to_string(),
                    lines: n.as_u64().unwrap(),
                })
                .collect();
            if history.is_empty() && !new {
                history.push(Counted {
                    name: "1999-01-01".into(),
                    lines: 1,
                });
            }
            let t = decide(
                &history,
                c["lines"].as_u64().unwrap(),
                today,
                new,
                c["severity"] == "critical",
            );
            let want = serde_json::to_value(t.decision).unwrap();
            assert_eq!(
                (want.as_str().unwrap(), t.reason.as_str()),
                (
                    c["decision"].as_str().unwrap(),
                    c["reason"].as_str().unwrap()
                ),
                "{}",
                c["name"]
            );
        }
    }
}
