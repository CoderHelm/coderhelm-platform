//! Quarantined tests don't fail Coderhelm's own CI decisions.
//!
//! A quarantined test still runs in the team's CI, and unless their workflow
//! skips it (the Tests page shows the step), its failure still turns GitHub's
//! check red. Coderhelm reads every workflow run's test report (`e2e::ingest`)
//! and keeps what it found with the run's claim. When every failing check on a
//! commit belongs to a workflow run whose only failures were quarantined tests,
//! the commit counts as passing for Coderhelm: the CI-fix loop doesn't start,
//! the run goes on to review and ready-for-review, and armed auto-merge isn't
//! held back.
//!
//! The decision is per workflow run: GitHub reports one check per job, the test
//! report covers the whole run. A failing check from anything that isn't a
//! GitHub Actions run, a failing commit status, or a run whose report shows a
//! real failure (or no report at all) keeps the commit failing.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::e2e::model::IngestOutcome;
use common::e2e::store::Store;
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// How long after a failing check completes Coderhelm waits for its test
/// report to be read before it treats the failure as real. Reading starts 20s
/// after the run ends and usually takes well under a minute.
pub const RESULTS_WAIT_SECS: i64 = 600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gate {
    /// Something failed for real, or Coderhelm can't tell: treat as failed.
    Real,
    /// Every failure so far may be quarantined, but a test report hasn't been
    /// read yet. Check again shortly.
    Waiting,
    /// Every failing check failed only on these quarantined tests.
    OnlyQuarantined(Vec<String>),
    /// Every failing end-to-end test fails for a reason outside the change
    /// and is fixed in a separate pull request: CI is blocked on that fix,
    /// not failing (see `e2e_attribution`). Carries the lines saying so.
    Unrelated(Vec<String>),
}

/// The failing checks on a commit, grouped by the workflow run they came from.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Failing {
    /// Workflow run ids of failing GitHub Actions checks.
    pub runs: Vec<u64>,
    /// A failing check that isn't a GitHub Actions job.
    pub other: bool,
    /// When the newest failing check completed.
    pub last_completed: Option<DateTime<Utc>>,
}

/// The workflow run id in a check's URL (`…/actions/runs/{id}/job/{job}`).
fn workflow_run_id(check: &Value) -> Option<u64> {
    ["details_url", "html_url"].iter().find_map(|k| {
        let url = check[*k].as_str()?;
        let rest = url.split("/actions/runs/").nth(1)?;
        rest.split(['/', '?', '#']).next()?.parse().ok()
    })
}

/// The failing checks in a `check-runs` listing. Only the newest run of each
/// check name counts, so a failure that a re-run fixed doesn't linger.
pub fn failing_checks(checks: &[Value]) -> Failing {
    let mut latest: HashMap<&str, &Value> = HashMap::new();
    for c in checks {
        let name = c["name"].as_str().unwrap_or("check");
        let ts = c["started_at"].as_str().unwrap_or("");
        let newer = latest
            .get(name)
            .and_then(|e| e["started_at"].as_str())
            .is_none_or(|prev| prev <= ts);
        if newer {
            latest.insert(name, c);
        }
    }
    let mut out = Failing::default();
    for c in latest.values() {
        if c["status"].as_str() != Some("completed") {
            continue;
        }
        let failed = matches!(
            c["conclusion"].as_str().unwrap_or(""),
            "failure" | "timed_out" | "startup_failure" | "action_required"
        );
        if !failed {
            continue;
        }
        match workflow_run_id(c) {
            Some(id) => {
                if !out.runs.contains(&id) {
                    out.runs.push(id);
                }
            }
            None => out.other = true,
        }
        let done = c["completed_at"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc));
        if done > out.last_completed {
            out.last_completed = done;
        }
    }
    out.runs.sort_unstable();
    out
}

/// The decision, from the failing checks and what reading each of their
/// workflow runs found (`None` = not claimed yet).
pub fn decide(
    failing: &Failing,
    outcomes: &[(u64, Option<IngestOutcome>)],
    now: DateTime<Utc>,
) -> Gate {
    if failing.other || failing.runs.is_empty() {
        return Gate::Real;
    }
    let mut names: Vec<String> = Vec::new();
    let mut waiting = false;
    for id in &failing.runs {
        match outcomes
            .iter()
            .find(|(r, _)| r == id)
            .and_then(|(_, o)| o.as_ref())
        {
            Some(IngestOutcome::Read {
                failed,
                rescued: true,
                quarantined,
            }) if *failed > 0 => names.extend(quarantined.iter().cloned()),
            // Not read yet: wait, within a bound.
            None | Some(IngestOutcome::Reading) => waiting = true,
            // No report, a real failure, or a run that failed with every test
            // passing (another step failed).
            Some(_) => return Gate::Real,
        }
    }
    if waiting {
        let waited = failing
            .last_completed
            .map(|t| (now - t).num_seconds())
            .unwrap_or(i64::MAX);
        return if waited < RESULTS_WAIT_SECS {
            Gate::Waiting
        } else {
            Gate::Real
        };
    }
    names.sort();
    names.dedup();
    Gate::OnlyQuarantined(names)
}

/// Whether `git_ref` (a commit sha or branch) failed only on quarantined tests.
/// Anything Coderhelm can't confirm is [`Gate::Real`], so this never turns a
/// real failure into a pass.
pub async fn gate(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Gate {
    let st = Store::new(&state.dynamo, &state.config.tests_table_name);
    if !st.enabled() {
        return Gate::Real;
    }
    let full = format!("{owner}/{repo}");
    match st.settings(team_id).await {
        Ok(s) if s.covers(&full) => {}
        _ => return Gate::Real,
    }
    let checks = match github.list_check_runs_for_ref(owner, repo, git_ref).await {
        Ok(v) => v["check_runs"].as_array().cloned().unwrap_or_default(),
        Err(e) => {
            warn!(repo = %full, error = %e, "Could not list checks for the quarantine check");
            return Gate::Real;
        }
    };
    let failing = failing_checks(&checks);
    if failing.other || failing.runs.is_empty() {
        return Gate::Real;
    }
    // A failing legacy commit status isn't a test run.
    if let Ok(v) = github.get_commit_status(owner, repo, git_ref).await {
        let any = v["statuses"].as_array().is_some_and(|s| !s.is_empty());
        if any && matches!(v["state"].as_str(), Some("failure" | "error")) {
            return Gate::Real;
        }
    }
    let mut outcomes = Vec::new();
    for id in &failing.runs {
        match st.latest_outcome(team_id, *id).await {
            Ok(o) => outcomes.push((*id, o)),
            Err(e) => {
                warn!(repo = %full, run = id, error = %e, "Could not read a run's test outcome");
                return Gate::Real;
            }
        }
    }
    let mut g = decide(&failing, &outcomes, Utc::now());
    if g == Gate::Real {
        let sha = checks
            .iter()
            .find_map(|c| c["head_sha"].as_str())
            .unwrap_or(git_ref);
        if let Some(lines) =
            super::e2e_attribution::blocked(state, team_id, &full, sha, &failing.runs).await
        {
            g = Gate::Unrelated(lines);
        }
    }
    info!(repo = %full, git_ref, runs = ?failing.runs, gate = ?g, "Quarantine check");
    g
}

/// The line Coderhelm records when it treats a commit as passing.
pub fn passing_line(tests: &[String]) -> String {
    let shown: Vec<String> = tests.iter().take(10).map(|t| format!("`{t}`")).collect();
    let more = if tests.len() > shown.len() {
        format!(" and {} more", tests.len() - shown.len())
    } else {
        String::new()
    };
    format!(
        "Only quarantined tests failed: {}{more}. Coderhelm treats this as passing.",
        shown.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn check(name: &str, conclusion: &str, run: u64, started: &str) -> Value {
        json!({
            "name": name,
            "status": "completed",
            "conclusion": conclusion,
            "started_at": started,
            "completed_at": "2026-10-19T10:05:00Z",
            "details_url": format!("https://github.com/acme/web/actions/runs/{run}/job/9{run}"),
        })
    }

    fn rescued(names: &[&str]) -> Option<IngestOutcome> {
        Some(IngestOutcome::Read {
            failed: names.len() as u32,
            rescued: true,
            quarantined: names.iter().map(|s| s.to_string()).collect(),
        })
    }

    #[test]
    fn failing_checks_group_by_workflow_run_and_skip_superseded_ones() {
        let checks = vec![
            check("e2e (1/2)", "failure", 11, "2026-10-19T10:00:00Z"),
            check("e2e (2/2)", "failure", 11, "2026-10-19T10:00:00Z"),
            check("lint", "failure", 12, "2026-10-19T09:00:00Z"),
            check("lint", "success", 13, "2026-10-19T09:30:00Z"),
            check("build", "success", 14, "2026-10-19T10:00:00Z"),
            check("deploy", "cancelled", 15, "2026-10-19T10:00:00Z"),
        ];
        let f = failing_checks(&checks);
        assert_eq!(f.runs, vec![11]);
        assert!(!f.other);
        assert_eq!(f.last_completed, Some(at("2026-10-19T10:05:00Z")));

        let external = json!({ "name": "ext", "status": "completed", "conclusion": "failure", "details_url": "https://ci.example.com/b/1" });
        assert!(failing_checks(&[external]).other);
    }

    #[test]
    fn only_quarantined_failures_pass() {
        let f = Failing {
            runs: vec![11, 12],
            other: false,
            last_completed: Some(at("2026-10-19T10:05:00Z")),
        };
        let now = at("2026-10-19T10:06:00Z");
        assert_eq!(
            decide(
                &f,
                &[(11, rescued(&["b", "a"])), (12, rescued(&["a"]))],
                now
            ),
            Gate::OnlyQuarantined(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn any_real_failure_or_missing_report_stays_failed() {
        let f = Failing {
            runs: vec![11, 12],
            other: false,
            last_completed: Some(at("2026-10-19T10:05:00Z")),
        };
        let now = at("2026-10-19T10:06:00Z");
        let real = Some(IngestOutcome::Read {
            failed: 2,
            rescued: false,
            quarantined: vec!["a".into()],
        });
        assert_eq!(
            decide(&f, &[(11, rescued(&["a"])), (12, real)], now),
            Gate::Real
        );
        assert_eq!(
            decide(
                &f,
                &[(11, rescued(&["a"])), (12, Some(IngestOutcome::NoTests))],
                now
            ),
            Gate::Real
        );
        // Every test passed, yet the run failed: another step broke.
        let green = Some(IngestOutcome::Read {
            failed: 0,
            rescued: false,
            quarantined: vec![],
        });
        assert_eq!(
            decide(&f, &[(11, rescued(&["a"])), (12, green)], now),
            Gate::Real
        );
        // A real failure wins even while another report is still being read.
        assert_eq!(
            decide(&f, &[(11, Some(IngestOutcome::NoTests)), (12, None)], now),
            Gate::Real
        );
        let other = Failing {
            other: true,
            ..Failing::default()
        };
        assert_eq!(decide(&other, &[], now), Gate::Real);
        assert_eq!(decide(&Failing::default(), &[], now), Gate::Real);
    }

    #[test]
    fn an_unread_report_is_waited_for_within_a_bound() {
        let f = Failing {
            runs: vec![11],
            other: false,
            last_completed: Some(at("2026-10-19T10:05:00Z")),
        };
        assert_eq!(
            decide(&f, &[(11, None)], at("2026-10-19T10:06:00Z")),
            Gate::Waiting
        );
        assert_eq!(
            decide(
                &f,
                &[(11, Some(IngestOutcome::Reading))],
                at("2026-10-19T10:14:00Z")
            ),
            Gate::Waiting
        );
        assert_eq!(
            decide(&f, &[(11, None)], at("2026-10-19T10:15:01Z")),
            Gate::Real
        );
    }

    #[test]
    fn the_passing_line_names_the_tests() {
        assert_eq!(
            passing_line(&["a".into(), "b".into()]),
            "Only quarantined tests failed: `a`, `b`. Coderhelm treats this as passing."
        );
    }
}
