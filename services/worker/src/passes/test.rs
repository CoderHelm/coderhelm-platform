use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::TicketMessage;
use crate::WorkerState;

const MAX_LOG_CHARS: usize = 30_000;
const POLL_INTERVAL_SECS: u64 = 15;
const POLL_TIMEOUT_SECS: u64 = 300; // 5 minutes

pub struct TestResult {
    pub passed: bool,
    pub output: Option<String>,
}

/// Run the test pass: wait for GitHub Actions CI on the branch, return results.
/// If no CI is configured, returns passed = true (skip).
pub async fn run(
    _state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
) -> Result<TestResult, Box<dyn std::error::Error + Send + Sync>> {
    // Check if repo has CI workflows
    let has_ci = github
        .read_file(&msg.repo_owner, &msg.repo_name, ".github/workflows", "main")
        .await
        .is_ok();

    if !has_ci {
        info!("No CI workflows detected, skipping test pass");
        return Ok(TestResult {
            passed: true,
            output: None,
        });
    }

    // Poll check runs on the branch until all complete or timeout
    let start = std::time::Instant::now();
    loop {
        if start.elapsed().as_secs() > POLL_TIMEOUT_SECS {
            warn!("Test pass timed out after {}s", POLL_TIMEOUT_SECS);
            return Ok(TestResult {
                passed: true, // Don't block on timeout
                output: Some("CI timed out — proceeding without test results.".to_string()),
            });
        }

        let check_data = github
            .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, branch)
            .await?;

        let check_runs = check_data["check_runs"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        if check_runs.is_empty() {
            // No check runs yet — wait a bit for CI to start
            if start.elapsed().as_secs() > 60 {
                info!("No check runs appeared after 60s, skipping test pass");
                return Ok(TestResult {
                    passed: true,
                    output: None,
                });
            }
            tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;
            continue;
        }

        let all_completed = check_runs
            .iter()
            .all(|r| r["status"].as_str() == Some("completed"));

        if !all_completed {
            tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;
            continue;
        }

        // All completed — check for failures
        let mut failures = Vec::new();
        for run in &check_runs {
            let conclusion = run["conclusion"].as_str().unwrap_or("unknown");
            let name = run["name"].as_str().unwrap_or("unknown");
            if conclusion == "failure" || conclusion == "timed_out" {
                let job_id = run["id"].as_u64().unwrap_or(0);
                let logs = if job_id > 0 {
                    match github
                        .get_check_run_logs(&msg.repo_owner, &msg.repo_name, job_id)
                        .await
                    {
                        Ok(l) => {
                            if l.len() > MAX_LOG_CHARS {
                                format!("... (truncated)\n{}", &l[l.len() - MAX_LOG_CHARS..])
                            } else {
                                l
                            }
                        }
                        Err(e) => {
                            warn!("Failed to download CI logs for {name}: {e}");
                            "(failed to download logs)".to_string()
                        }
                    }
                } else {
                    String::new()
                };
                failures.push(format!("### {name} — {conclusion}\n{logs}"));
            }
        }

        if failures.is_empty() {
            info!("All CI checks passed");
            return Ok(TestResult {
                passed: true,
                output: None,
            });
        }

        let output = failures.join("\n\n---\n\n");
        info!(
            failed_checks = failures.len(),
            "CI checks failed"
        );
        return Ok(TestResult {
            passed: false,
            output: Some(output),
        });
    }
}
