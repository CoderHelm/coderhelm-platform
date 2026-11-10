//! Why the checks that test a pull request were skipped on its head, read
//! from their workflow files (`common::e2e::workflow_gate`) and kept per
//! repository, so each workflow file is read about once a day.
//!
//! The ready decision and the failing-test fix's ready check use this
//! before waiting on such a check: a check that runs only on a pull request
//! that isn't a draft is never waited on from a draft (it would wait
//! forever), and one that needs a label CoderHelm can't add is a hold with
//! that one action, never a wait.

use std::collections::HashMap;

use common::e2e::check_gate::{self, Check};
use common::e2e::workflow_gate::{self, Skipped, WorkflowGate};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::attr_s;
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// A workflow file's gate is read again after this long.
const CACHE_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Cached {
    gate: WorkflowGate,
    /// Epoch seconds it was read.
    at: i64,
}

fn cache_sk(owner: &str, repo: &str) -> String {
    format!("WORKFLOW_GATES#{owner}/{repo}")
}

async fn load_cache(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
) -> HashMap<String, Cached> {
    let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&cache_sk(owner, repo)))
        .send()
        .await
    else {
        return HashMap::new();
    };
    out.item
        .as_ref()
        .and_then(|i| i.get("data"))
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

async fn save_cache(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    cache: &HashMap<String, Cached>,
) {
    let ttl = (chrono::Utc::now() + chrono::Duration::days(30)).timestamp();
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&cache_sk(owner, repo)))
        .item(
            "data",
            attr_s(&serde_json::to_string(cache).unwrap_or_default()),
        )
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N(ttl.to_string()),
        )
        .send()
        .await
    {
        warn!(error = %e, "Could not keep the repository's workflow gates");
    }
}

/// The skipped checks among `names`, sorted by why they were skipped on
/// this head. `labels` are the labels on the pull request. A check whose workflow can't be read is unknown.
#[allow(clippy::too_many_arguments)]
pub async fn sort(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    head_ref: &str,
    labels: &[String],
    checks: &[Check],
    names: &[String],
) -> Skipped {
    if names.is_empty() {
        return Skipped::default();
    }
    let now = chrono::Utc::now().timestamp();
    let mut cache = load_cache(state, team_id, owner, repo).await;
    let mut changed = false;
    // Each workflow run's file, read once per call.
    let mut paths: HashMap<u64, Option<String>> = HashMap::new();
    let mut rows: Vec<(String, Option<WorkflowGate>)> = Vec::new();
    for name in names {
        let run = checks
            .iter()
            .find(|c| &c.name == name)
            .and_then(|c| c.run_id);
        let path = match run {
            Some(id) => match paths.get(&id) {
                Some(p) => p.clone(),
                None => {
                    let p = gh
                        .get_workflow_run(owner, repo, id)
                        .await
                        .ok()
                        .and_then(|r| r["path"].as_str().map(str::to_string))
                        .filter(|p| !p.is_empty());
                    paths.insert(id, p.clone());
                    p
                }
            },
            None => None,
        };
        let gate = match path {
            Some(p) => match cache.get(&p) {
                Some(c) if now - c.at < CACHE_SECS => Some(c.gate.clone()),
                _ => match gh.read_file(owner, repo, &p, head_ref).await {
                    Ok(text) => {
                        let gate = workflow_gate::read(&text);
                        cache.insert(
                            p.clone(),
                            Cached {
                                gate: gate.clone(),
                                at: now,
                            },
                        );
                        changed = true;
                        Some(gate)
                    }
                    Err(e) => {
                        warn!(path = %p, error = %e, "Could not read a skipped check's workflow");
                        None
                    }
                },
            },
            None => None,
        };
        rows.push((name.clone(), gate));
    }
    if changed {
        save_cache(state, team_id, owner, repo, &cache).await;
    }
    let s = workflow_gate::sort(&rows, labels);
    info!(
        repo = %format!("{owner}/{repo}"),
        draft_gated = ?s.draft_gated,
        draft_no_start = ?s.draft_no_start,
        label_gated = ?s.label_gated,
        unknown = ?s.unknown,
        "Read why the checks that test it were skipped"
    );
    s
}

/// The test-related checks skipped on a pull request's head that run only
/// once it is marked ready (empty for a pull request that isn't a draft).
/// For the ready decision's waits on proof other than the failing-test
/// fix's own check (an end-to-end fix's repeat runs).
pub async fn draft_gated_on_head(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    pr: &serde_json::Value,
) -> Vec<String> {
    if pr["draft"].as_bool() != Some(true) {
        return Vec::new();
    }
    let sha = pr["head"]["sha"].as_str().unwrap_or("");
    let head_ref = pr["head"]["ref"].as_str().unwrap_or("");
    if sha.is_empty() {
        return Vec::new();
    }
    let runs = gh
        .list_check_runs_for_ref(owner, repo, sha)
        .await
        .ok()
        .and_then(|v| v["check_runs"].as_array().cloned())
        .unwrap_or_default();
    let checks = check_gate::checks(&runs, &[]);
    let standing = check_gate::standing(&checks, &[]);
    sort(
        state,
        gh,
        team_id,
        (owner, repo),
        head_ref,
        &labels_of(pr),
        &checks,
        &standing.skipped,
    )
    .await
    .draft_gated
}

/// The labels on a pull request.
pub fn labels_of(pr: &serde_json::Value) -> Vec<String> {
    pr["labels"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| l["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}
