//! Jobs the gateway hands the worker for E2E results. Shared so both sides
//! agree on the shape.

use serde::{Deserialize, Serialize};

/// Read the test reports of one finished workflow run (or one upload).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestIngest {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    #[serde(default)]
    pub repo_id: u64,
    pub default_branch: String,
    pub workflow: String,
    /// The workflow's file, e.g. `.github/workflows/e2e.yml`.
    #[serde(default)]
    pub workflow_path: String,
    /// GitHub's run id; 0 for an upload.
    pub run_id: u64,
    #[serde(default = "one")]
    pub run_attempt: u32,
    pub head_sha: String,
    pub head_branch: String,
    pub event: String,
    #[serde(default)]
    pub pr_numbers: Vec<u64>,
    /// `success` | `failure` | `timed_out` | `cancelled` …
    pub conclusion: String,
    pub html_url: String,
    pub started_at: String,
    #[serde(default)]
    pub updated_at: String,
    /// Bucket key of an uploaded report (results that never reach GitHub).
    #[serde(default)]
    pub upload_key: Option<String>,
}

fn one() -> u32 {
    1
}

/// Something a person (or Coderhelm itself) asked to be done about a test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestAction {
    pub team_id: String,
    pub repo: String,
    /// `fix` | `generate` | `ticket`
    pub action: String,
    /// The test (fix, ticket) or recommendation (generate) it is about.
    pub target_id: String,
    pub actor: String,
}
