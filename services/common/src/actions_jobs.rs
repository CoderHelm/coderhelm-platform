//! A workflow run's jobs across its attempts (`GET /actions/runs/{id}/jobs
//! ?filter=all`), read for a CI fix: which jobs fail on the newest attempt
//! (their logs are what the fix reads), the step each failed in, and which
//! jobs failed on an earlier attempt and passed when re-run (flaky, not
//! something the change broke).

use serde_json::Value;

/// Conclusions that mean a job failed.
const FAILED: &[&str] = &["failure", "timed_out", "startup_failure"];

/// One failing job on the newest attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedJob {
    pub id: u64,
    pub name: String,
    /// The first step that failed, when GitHub names one.
    pub step: Option<String>,
}

/// What a run's jobs say about its failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunJobs {
    pub attempt: u64,
    pub failed: Vec<FailedJob>,
    /// Jobs that failed on an earlier attempt and passed on a later one.
    pub flaky: Vec<String>,
}

fn attempt_of(j: &Value) -> u64 {
    j["run_attempt"].as_u64().unwrap_or(1)
}

/// Reads a jobs listing (any `filter`).
pub fn read(jobs: &Value) -> RunJobs {
    let all: Vec<&Value> = jobs["jobs"].as_array().into_iter().flatten().collect();
    let attempt = all.iter().map(|j| attempt_of(j)).max().unwrap_or(1);
    let failed_conclusion = |j: &Value| FAILED.contains(&j["conclusion"].as_str().unwrap_or(""));
    let failed = all
        .iter()
        .filter(|j| attempt_of(j) == attempt && failed_conclusion(j))
        .filter_map(|j| {
            Some(FailedJob {
                id: j["id"].as_u64()?,
                name: j["name"].as_str().unwrap_or("job").to_string(),
                step: j["steps"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|s| failed_conclusion(s))
                    .and_then(|s| s["name"].as_str())
                    .map(str::to_string),
            })
        })
        .collect();
    let mut flaky: Vec<String> = Vec::new();
    for j in &all {
        let name = j["name"].as_str().unwrap_or("");
        if name.is_empty() || !failed_conclusion(j) || flaky.iter().any(|f| f == name) {
            continue;
        }
        let passed_later = all.iter().any(|k| {
            k["name"].as_str() == Some(name)
                && attempt_of(k) > attempt_of(j)
                && k["conclusion"].as_str() == Some("success")
        });
        if passed_later {
            flaky.push(name.to_string());
        }
    }
    flaky.sort();
    RunJobs {
        attempt,
        failed,
        flaky,
    }
}

/// The header a job's log is given: its name and the step that failed.
pub fn log_header(job: &FailedJob) -> String {
    match &job.step {
        Some(step) => format!("Job `{}` failed at step `{step}`", job.name),
        None => format!("Job `{}` failed", job.name),
    }
}

/// A line for the fix about jobs that passed when re-run, if any.
pub fn flaky_line(jobs: &RunJobs) -> Option<String> {
    (!jobs.flaky.is_empty()).then(|| {
        format!(
            "Failed on an earlier attempt of this workflow run and passed when re-run (flaky, \
             not caused by the change): {}",
            jobs.flaky
                .iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(id: u64, name: &str, attempt: u64, conclusion: &str) -> Value {
        json!({"id": id, "name": name, "run_attempt": attempt, "conclusion": conclusion,
               "steps": [{"name": "Checkout", "conclusion": "success"},
                         {"name": "Run tests", "conclusion": conclusion}]})
    }

    #[test]
    fn the_newest_attempt_decides_what_fails() {
        let v = json!({"jobs": [
            job(1, "unit", 1, "failure"), job(2, "lint", 1, "failure"),
            job(3, "unit", 2, "failure"), job(4, "lint", 2, "success"),
        ]});
        let r = read(&v);
        assert_eq!(r.attempt, 2);
        assert_eq!(
            r.failed,
            vec![FailedJob {
                id: 3,
                name: "unit".into(),
                step: Some("Run tests".into())
            }]
        );
        assert_eq!(r.flaky, vec!["lint".to_string()]);
        assert_eq!(
            flaky_line(&r).unwrap(),
            "Failed on an earlier attempt of this workflow run and passed when re-run (flaky, \
             not caused by the change): `lint`"
        );
        assert_eq!(
            log_header(&r.failed[0]),
            "Job `unit` failed at step `Run tests`"
        );
    }

    #[test]
    fn a_single_attempt_has_no_flaky_jobs() {
        let r =
            read(&json!({"jobs": [job(1, "unit", 1, "timed_out"), job(2, "e2e", 1, "success")]}));
        assert_eq!(r.failed.len(), 1);
        assert!(r.flaky.is_empty());
        assert!(flaky_line(&r).is_none());
    }

    #[test]
    fn a_job_without_steps_or_attempts_still_reads() {
        let r = read(&json!({"jobs": [{"id": 9, "name": "build", "conclusion": "failure"}]}));
        assert_eq!(r.attempt, 1);
        assert_eq!(r.failed[0].step, None);
        assert_eq!(log_header(&r.failed[0]), "Job `build` failed");
        assert_eq!(
            read(&Value::Null),
            RunJobs {
                attempt: 1,
                ..Default::default()
            }
        );
    }
}
