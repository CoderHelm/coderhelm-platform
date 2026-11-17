//! The repository's deploy label, read for the preview of CoderHelm's own
//! pull request (`preview::advance`) and the auto-merge gate. The rules are
//! `common::deploy_label`'s; this module reads the settings and GitHub.

use common::preview::{CheckResult, Outcome};
use tracing::warn;

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The repository's deploy labels; empty when it has none, or the settings
/// can't be read.
pub async fn labels(state: &WorkerState, team_id: &str, owner: &str, repo: &str) -> Vec<String> {
    let cfg = super::review_actions::OnApproveConfig::load(state, team_id, owner, repo).await;
    common::deploy_label::configured(&cfg.deploy_label)
}

/// The logins whose taking a label off counts as a workflow's: GitHub
/// Actions, and CoderHelm's own app when its login can be read.
pub async fn workflow_logins(gh: &GitHubClient) -> Vec<String> {
    let mut out = vec!["github-actions[bot]".to_string()];
    if let Some(app) = gh.bot_login().await.filter(|a| !a.is_empty()) {
        out.push(app);
    }
    out
}

/// The deploy workflows among `paths` whose newest run on `head` failed
/// before any job ran (GitHub's `startup_failure`): they report no check,
/// so each is a failed result of its own, with its run's link and nothing
/// on the branch for a fix to act on. Empty when the runs can't be read.
pub async fn unstarted(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    head: &str,
    paths: &[String],
) -> Vec<CheckResult> {
    let runs = match gh.list_workflow_runs_for_sha(owner, repo, head).await {
        Ok(r) => r,
        Err(e) => {
            warn!(owner, repo, head, error = %e, "Could not read the head's workflow runs");
            return Vec::new();
        }
    };
    unstarted_of(&runs, paths)
}

/// Pure half of [`unstarted`]: GitHub's runs listing, newest first.
pub fn unstarted_of(runs: &[serde_json::Value], paths: &[String]) -> Vec<CheckResult> {
    let mut out = Vec::new();
    for path in paths {
        if out.iter().any(|r: &(String, CheckResult)| &r.0 == path) {
            continue;
        }
        let newest = runs
            .iter()
            .filter(|r| r["path"].as_str() == Some(path.as_str()))
            .max_by(|a, b| {
                a["created_at"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["created_at"].as_str().unwrap_or(""))
            });
        if let Some(r) = newest.filter(|r| r["conclusion"].as_str() == Some("startup_failure")) {
            out.push((
                path.clone(),
                CheckResult {
                    name: r["name"].as_str().unwrap_or(path).to_string(),
                    outcome: Outcome::Failed,
                    url: r["html_url"].as_str().unwrap_or("").to_string(),
                    environment: String::new(),
                    no_jobs: true,
                },
            ));
        }
    }
    out.into_iter().map(|(_, r)| r).collect()
}

/// The environment a check held for an approval waits for, from its
/// workflow run's pending deployments; empty when GitHub doesn't say.
pub async fn environment_of(gh: &GitHubClient, owner: &str, repo: &str, check_url: &str) -> String {
    let Some(run_id) =
        common::commit_ci::actions_run_id(&serde_json::json!({ "html_url": check_url }))
    else {
        return String::new();
    };
    match gh.pending_deployments(owner, repo, run_id).await {
        Ok(d) => d
            .iter()
            .filter_map(|d| d["environment"]["name"].as_str())
            .collect::<Vec<_>>()
            .join(", "),
        Err(e) => {
            warn!(owner, repo, run = run_id, error = %e, "Could not read the environment a check waits for");
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_deploy_workflow_that_failed_to_start_is_a_failed_result_with_no_jobs() {
        let run = |path: &str, conclusion: &str, created: &str| {
            json!({"name": "Deploy app", "path": path, "conclusion": conclusion,
                   "created_at": created, "html_url": "https://github.com/o/r/actions/runs/7"})
        };
        let runs = vec![
            run(
                ".github/workflows/deploy.yml",
                "startup_failure",
                "2026-11-17T10:05:00Z",
            ),
            run(
                ".github/workflows/deploy.yml",
                "success",
                "2026-11-17T10:00:00Z",
            ),
            run(
                ".github/workflows/other.yml",
                "startup_failure",
                "2026-11-17T10:05:00Z",
            ),
        ];
        let got = unstarted_of(&runs, &[".github/workflows/deploy.yml".to_string()]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "Deploy app");
        assert_eq!(got[0].outcome, Outcome::Failed);
        assert!(got[0].no_jobs);
        assert_eq!(got[0].url, "https://github.com/o/r/actions/runs/7");
        // Its newest run started: nothing to say here.
        let later = vec![
            run(
                ".github/workflows/deploy.yml",
                "startup_failure",
                "2026-11-17T10:00:00Z",
            ),
            run(
                ".github/workflows/deploy.yml",
                "success",
                "2026-11-17T10:05:00Z",
            ),
        ];
        assert!(unstarted_of(&later, &[".github/workflows/deploy.yml".to_string()]).is_empty());
    }
}
