//! The run's test environment (`common::test_env`), found once and reused.
//!
//! The first round that has a sandbox reads the repository's CI workflows
//! and records how its tests install and run on the run, in the sandbox
//! bucket (`env-cache/runs/<run>.json`), with one line in the run's notes.
//! Every later round, continuation and proof step reads that record instead
//! of working it out again. Each one names the install by its fingerprint, so
//! every sandbox build restores the cached install, or saves it when it is
//! the first (`SandboxClient::with_test_env`).

use common::test_env::{self, TestEnv};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Where the run's record lives in the sandbox bucket.
pub fn record_key(run_id: &str) -> String {
    let safe: String = run_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    format!("env-cache/runs/{safe}.json")
}

async fn read_record(state: &WorkerState, run_id: &str) -> Option<TestEnv> {
    let obj = state
        .s3
        .get_object()
        .bucket(&state.config.sandbox_bucket_name)
        .key(record_key(run_id))
        .send()
        .await
        .ok()?;
    let bytes = obj.body.collect().await.ok()?.into_bytes();
    serde_json::from_slice(&bytes).ok()
}

async fn write_record(state: &WorkerState, run_id: &str, env: &TestEnv) {
    let Ok(body) = serde_json::to_vec(env) else {
        return;
    };
    if let Err(e) = state
        .s3
        .put_object()
        .bucket(&state.config.sandbox_bucket_name)
        .key(record_key(run_id))
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .content_type("application/json")
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not record the run's test environment");
    }
}

/// The run's test environment and the cache key of its install at
/// `git_ref`, or `None` without a sandbox or a CI job that runs tests.
/// `focus`: the files the run changes or plans to; the CI job chosen is the
/// one whose tests run for them (`test_env::from_workflows_for`).
#[allow(clippy::too_many_arguments)]
pub async fn for_run(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    base_branch: &str,
    git_ref: &str,
    run_id: &str,
    focus: &[String],
) -> Option<(TestEnv, String)> {
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        return None;
    }
    let tree: Vec<(String, String)> = github
        .get_tree(owner, repo, git_ref)
        .await
        .ok()?
        .into_iter()
        .filter(|e| e.entry_type == "blob")
        .map(|e| (e.path, e.sha))
        .collect();
    // A record made for another package's job (an earlier version chose the
    // first job of the first workflow) is worked out again for these files.
    let env = match read_record(state, run_id).await {
        Some(env) if !env.misses(focus) => env,
        _ => {
            let workflows = super::preview::workflows(github, owner, repo, base_branch).await;
            let paths: Vec<String> = tree.iter().map(|(p, _)| p.clone()).collect();
            let env = test_env::from_workflows_for(&workflows, &paths, focus)?;
            write_record(state, run_id, &env).await;
            super::add_progress_note(state, team_id, run_id, &env.summary()).await;
            env
        }
    };
    let fp = env.fingerprint(&tree);
    let key = test_env::cache_key(team_id, &format!("{owner}/{repo}"), &fp);
    info!(run_id, key = %key, "The run's test environment");
    Some((env, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_record_per_run() {
        assert_eq!(record_key("01ABC"), "env-cache/runs/01ABC.json");
        assert_eq!(record_key("../x"), "env-cache/runs/x.json");
    }
}
