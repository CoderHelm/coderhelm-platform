//! The repository's deploy label, read for the preview of CoderHelm's own
//! pull request (`preview::advance`) and the auto-merge gate. The rules are
//! `common::deploy_label`'s; this module reads the settings and GitHub.

use common::preview::{CheckResult, Covering, Outcome};
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

/// The deploy label's covering checks among `covers` whose workflow's
/// newest run on `head` failed before any job ran (GitHub's
/// `startup_failure`): they report no check, so each is a failed result
/// under its covering check's name, with its run's link and nothing on the
/// branch for a fix to act on. Empty when the runs can't be read.
pub async fn unstarted(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    head: &str,
    covers: &[&Covering],
) -> Vec<CheckResult> {
    let runs = match gh.list_workflow_runs_for_sha(owner, repo, head).await {
        Ok(r) => r,
        Err(e) => {
            warn!(owner, repo, head, error = %e, "Could not read the head's workflow runs");
            return Vec::new();
        }
    };
    unstarted_of(&runs, covers)
}

/// Pure half of [`unstarted`]: GitHub's runs listing.
pub fn unstarted_of(runs: &[serde_json::Value], covers: &[&Covering]) -> Vec<CheckResult> {
    covers
        .iter()
        .filter_map(|c| {
            let newest = runs
                .iter()
                .filter(|r| r["path"].as_str() == Some(c.workflow.as_str()))
                .max_by(|a, b| {
                    a["created_at"]
                        .as_str()
                        .unwrap_or("")
                        .cmp(b["created_at"].as_str().unwrap_or(""))
                })?;
            (newest["conclusion"].as_str() == Some("startup_failure")).then(|| CheckResult {
                name: c.check.clone(),
                outcome: Outcome::Failed,
                url: newest["html_url"].as_str().unwrap_or("").to_string(),
                environment: String::new(),
                no_jobs: true,
            })
        })
        .collect()
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
            json!({"name": "Deploy preview", "path": path, "conclusion": conclusion,
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
        let cover = Covering {
            check: "Deploy app".into(),
            workflow: ".github/workflows/deploy.yml".into(),
            label: "CI:DEPLOY_PREVIEW".into(),
            ..Default::default()
        };
        // Named after the covering job, not the workflow's `name:`, so the
        // Verification table, which lists covering checks, quotes it.
        let got = unstarted_of(&runs, &[&cover]);
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
        assert!(unstarted_of(&later, &[&cover]).is_empty());
    }
}
