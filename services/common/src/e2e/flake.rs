//! How often a test passed and failed across every report still kept, so a
//! fix for a flaky test says "36/36 passed in the last 25 runs" before it is
//! written and can be measured the same way after.

use serde::{Deserialize, Serialize};

use super::model::{Attempt, AttemptStatus, HistoryEntry, Outcome, ParsedTest};

/// One run of one test: when, which run, and its attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    /// `{run_id}-{attempt}`.
    pub run_key: String,
    pub at: String,
    pub outcome: Outcome,
    /// Attempts that ran (passed or failed); 0 when unknown.
    pub executions: u32,
    pub passes: u32,
}

impl RunResult {
    pub fn from_attempts(run_key: &str, at: &str, attempts: &[Attempt]) -> RunResult {
        let ran: Vec<&Attempt> = attempts
            .iter()
            .filter(|a| a.status == AttemptStatus::Passed || a.status.is_failure())
            .collect();
        RunResult {
            run_key: run_key.to_string(),
            at: at.to_string(),
            outcome: Outcome::of(attempts),
            executions: ran.len() as u32,
            passes: ran
                .iter()
                .filter(|a| a.status == AttemptStatus::Passed)
                .count() as u32,
        }
    }

    pub fn from_history(h: &HistoryEntry) -> RunResult {
        if h.attempts.is_empty() {
            let (executions, passes) = match h.outcome {
                Outcome::Passed => (1, 1),
                Outcome::Failed => (1, 0),
                Outcome::Flaky => (2, 1),
                Outcome::Skipped => (0, 0),
            };
            return RunResult {
                run_key: h.run_key.clone(),
                at: h.at.clone(),
                outcome: h.outcome,
                executions,
                passes,
            };
        }
        RunResult::from_attempts(&h.run_key, &h.at, &h.attempts)
    }

    pub fn from_parsed(run_key: &str, at: &str, t: &ParsedTest) -> RunResult {
        RunResult::from_attempts(run_key, at, &t.attempts)
    }
}

/// The counts for one test.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tally {
    pub test: String,
    /// Runs the test ran in (skipped runs left out).
    pub runs: u32,
    /// Attempts that ran, and how many passed.
    pub executions: u32,
    pub passes: u32,
    /// Runs that failed outright, and runs that passed only on a retry.
    pub failed_runs: u32,
    pub flaky_runs: u32,
    /// The window: the oldest and newest run counted.
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    /// The newest run where it failed at least once, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<String>,
    /// Whether the newest run failed at least once.
    #[serde(default)]
    pub failing_now: bool,
}

/// Counts the runs, newest last after sorting by time; a run counted twice
/// (two copies of a report) is counted once.
pub fn tally(test: &str, mut runs: Vec<RunResult>) -> Tally {
    runs.sort_by(|a, b| a.at.cmp(&b.at).then(a.run_key.cmp(&b.run_key)));
    runs.dedup_by(|a, b| a.run_key == b.run_key);
    let ran: Vec<&RunResult> = runs
        .iter()
        .filter(|r| r.outcome != Outcome::Skipped)
        .collect();
    let mut t = Tally {
        test: test.to_string(),
        runs: ran.len() as u32,
        ..Default::default()
    };
    for r in &ran {
        t.executions += r.executions;
        t.passes += r.passes;
        match r.outcome {
            Outcome::Failed => t.failed_runs += 1,
            Outcome::Flaky => t.flaky_runs += 1,
            _ => {}
        }
        if matches!(r.outcome, Outcome::Failed | Outcome::Flaky) {
            t.last_failure = Some(r.run_key.clone());
        }
    }
    t.from = ran.first().map(|r| r.at.clone()).unwrap_or_default();
    t.to = ran.last().map(|r| r.at.clone()).unwrap_or_default();
    t.failing_now = ran
        .last()
        .is_some_and(|r| matches!(r.outcome, Outcome::Failed | Outcome::Flaky));
    t
}

impl Tally {
    /// "36/36 passed in the last 25 runs", with the failures when any.
    pub fn line(&self) -> String {
        if self.runs == 0 {
            return format!("`{}`: no run of it in the reports kept", self.test);
        }
        let mut s = format!(
            "`{}`: {}/{} passed in the last {} run{}",
            self.test,
            self.passes,
            self.executions,
            self.runs,
            if self.runs == 1 { "" } else { "s" }
        );
        if self.failed_runs + self.flaky_runs > 0 {
            s.push_str(&format!(
                " ({} failed, {} passed only on a retry; last failure in run {})",
                self.failed_runs,
                self.flaky_runs,
                self.last_failure.as_deref().unwrap_or("?")
            ));
        }
        if !self.from.is_empty() {
            s.push_str(&format!(", {} to {}", day(&self.from), day(&self.to)));
        }
        s
    }

    /// No failure in the window: whatever made it fail may already be fixed.
    pub fn quiet(&self) -> bool {
        self.runs > 0 && self.failed_runs + self.flaky_runs == 0
    }
}

fn day(at: &str) -> &str {
    at.get(..10).unwrap_or(at)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn att(status: AttemptStatus) -> Attempt {
        Attempt {
            retry: 0,
            status,
            duration_ms: 1,
            error: None,
            trace_url: None,
            video_url: None,
            screenshot_url: None,
        }
    }

    fn run(key: &str, at: &str, s: &[AttemptStatus]) -> RunResult {
        let a: Vec<Attempt> = s.iter().map(|x| att(*x)).collect();
        RunResult::from_attempts(key, at, &a)
    }

    #[test]
    fn a_clean_window_reads_as_passes_over_executions_and_runs() {
        use AttemptStatus::*;
        let mut runs = Vec::new();
        for i in 0..25 {
            let n = if i < 11 { 2 } else { 1 };
            runs.push(run(
                &format!("{i}-1"),
                &format!("2026-10-{:02}T00:00:00Z", i + 1),
                &vec![Passed; n],
            ));
        }
        let t = tally("deletes an item", runs);
        assert_eq!(t.runs, 25);
        assert_eq!((t.passes, t.executions), (36, 36));
        assert!(t.quiet());
        assert!(!t.failing_now);
        assert_eq!(
            t.line(),
            "`deletes an item`: 36/36 passed in the last 25 runs, 2026-10-01 to 2026-10-25"
        );
    }

    #[test]
    fn failures_and_retries_are_counted_and_the_newest_decides_failing_now() {
        use AttemptStatus::*;
        let t = tally(
            "t",
            vec![
                run("3-1", "2026-10-03", &[Failed, Passed]),
                run("1-1", "2026-10-01", &[Passed]),
                run("2-1", "2026-10-02", &[Failed, Failed]),
                run("2-1", "2026-10-02", &[Failed, Failed]),
                run("4-1", "2026-10-04", &[Skipped]),
            ],
        );
        assert_eq!(t.runs, 3);
        assert_eq!((t.passes, t.executions), (2, 5));
        assert_eq!((t.failed_runs, t.flaky_runs), (1, 1));
        assert_eq!(t.last_failure.as_deref(), Some("3-1"));
        assert!(t.failing_now);
        assert!(!t.quiet());
        assert!(t.line().contains("1 failed, 1 passed only on a retry"));
    }

    #[test]
    fn history_without_attempts_still_counts() {
        let h = HistoryEntry {
            run_key: "9-1".into(),
            sha: "abc".into(),
            branch: "main".into(),
            pr: None,
            at: "2026-10-09".into(),
            outcome: Outcome::Flaky,
            bad_run: false,
            attempts: vec![],
        };
        let r = RunResult::from_history(&h);
        assert_eq!((r.passes, r.executions), (1, 2));
        assert_eq!(
            tally("x", vec![]).line(),
            "`x`: no run of it in the reports kept"
        );
    }
}
