//! Learning from outcomes: CoderHelm's reward system.
//!
//! Signals (a review comment acted on or dismissed, a thumbs up or down, a
//! pull request merged as written, edited, closed or reverted, CI's first-try
//! result, a plan task approved or rejected, time to merge, re-review asks,
//! an alert tied to a merged change) are each recorded as one
//! [`event::RewardEvent`], from the GitHub webhooks and dashboard actions
//! that already report them.
//!
//! A transparent weighted score ([`score`]) turns events into a reward per
//! run, review, finding category, agent, repository, skill version and
//! prompt version. The agents then seek reward in context, with no training:
//! good and bad past examples are put in their prompts ([`exemplars`]), the
//! reviewer is calibrated per repository and category ([`calibration`]),
//! prompt variants are picked by a capped bandit ([`bandit`]), and lessons
//! are written only from evidence ([`lessons`]). Every behavior change is
//! logged with its evidence and can be reverted per repository
//! ([`changes`]). All records are per team ([`store`]).

pub mod bandit;
pub mod calibration;
pub mod changes;
pub mod event;
pub mod example_text;
pub mod exemplars;
pub mod health;
pub mod lessons;
pub mod mapping;
pub mod score;
pub mod store;
pub mod summary;
pub mod unscored;

pub use event::{RewardEvent, RewardKind};

/// The outcome of a closed pull request to score (gateway to worker, on the
/// `pull_request.closed` webhook, and from the worker's reconcile sweep for a
/// close the webhook missed). For CoderHelm's own PR the worker reads the PR,
/// its commits and CoderHelm's review threads and records the coder's and
/// the reviewer's events; for a person's PR CoderHelm reviewed
/// (`review_only`) it records only what became of the review's comments.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct RewardOutcomeMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub run_id: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// A PR CoderHelm reviewed but didn't write: no coder outcome.
    #[serde(default)]
    pub review_only: bool,
    /// What happened, when it is not the pull request closing: empty for a
    /// close, [`TRIGGER_REVERTED`] when a merged CoderHelm change was
    /// reverted (`pr_number` is the reverted one), [`TRIGGER_GUIDE_CLOSED`]
    /// when the repository's guide pull request closed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trigger: String,
    /// People's words that came with it: why they reverted.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
    /// GitHub held or refused the scoring's requests: when it goes on, and
    /// how often it waited (`crate::github_wait`). A held request is a wait,
    /// never a lost score.
    #[serde(
        default,
        skip_serializing_if = "crate::github_wait::GitHubWait::is_idle"
    )]
    pub github_wait: crate::github_wait::GitHubWait,
    /// Sent again only to score it (after a wait for GitHub, or by the sweep
    /// for a close with no score yet): what else follows a close already ran.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rescore: bool,
}

pub const TRIGGER_REVERTED: &str = "reverted";
pub const TRIGGER_GUIDE_CLOSED: &str = "guide_closed";
