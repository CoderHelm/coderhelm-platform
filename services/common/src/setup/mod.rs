//! Setup: pick repos, review what CoderHelm recommends for every setting
//! and why, then apply to all of them at once.
//!
//! - [`registry`]: every setting Setup manages, read and written through
//!   the same validation as its settings page.
//! - [`facts`]: what a repo shows (files, workflows, labels, branch
//!   protection, tags, its last merged pull requests) and the detectors
//!   that read facts from it, reusing the E2E, workflow and deploy readers.
//! - [`ai`]: the one bounded AI read per repo: its prompt, its schema, and
//!   the check every answer goes through.
//! - [`plan`]: recommendations (detected, inferred, default) and the plan
//!   items Review shows: current value, custom, decision, diff.
//! - [`runs`]: a run, its per-repo plans and results, what Setup applied,
//!   what the team has reviewed, and the team's Setup settings.
//! - [`apply`]: applying a run (idempotent, per repo, one audit event per
//!   change, `before` values kept) and undoing it.
//!
//! Storage (settings table, `pk` = team): `SETUP_RUN#<id>`,
//! `SETUP_RUN_REPO#<id>#<repo>`, `SETUP_APPLIED#<repo>`, `SETUP_SEEN#TEAM`,
//! `SETUP_SETTINGS#TEAM`, and `AUDIT#<id>` events (`setup.*`).

pub mod ai;
pub mod apply;
pub mod facts;
pub mod plan;
pub mod registry;
pub mod runs;
pub mod store;

use serde::{Deserialize, Serialize};

/// One line of evidence behind a recommended value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl Evidence {
    pub fn text(t: impl Into<String>) -> Self {
        Self {
            text: t.into(),
            ..Default::default()
        }
    }

    pub fn file(t: impl Into<String>, file: &str) -> Self {
        Self {
            text: t.into(),
            file: Some(file.to_string()),
            ..Default::default()
        }
    }
}

/// Where a recommended value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Read straight from the repo by code.
    Detected,
    /// The AI read, citing what it is based on.
    Inferred,
    /// CoderHelm's own pick.
    Default,
}

/// Tokens one repo's AI read is expected to use (the prompt is capped at
/// [`ai::MAX_PROMPT_CHARS`], the answer at [`ai::MAX_OUTPUT_TOKENS`]).
pub const ESTIMATE_TOKENS_PER_REPO: u64 = 20_000;

/// Runs and their plans are kept this long.
pub const KEEP_DAYS: i64 = 180;
