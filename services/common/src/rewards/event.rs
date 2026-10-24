//! One outcome signal, kept with what produced it.
//!
//! Every signal CoderHelm learns from (a review comment acted on, a merged
//! pull request, a thumbs down, a revert) is one [`RewardEvent`]. It carries
//! the time, the run or review it belongs to, the agent, the repository, the
//! model, the prompt and skill versions and the plugin, so the score can be
//! split along any of them later.
//!
//! Events live on the settings table under the team's own partition key
//! (`pk` = team id, `sk` = [`event_sk`]): one team's events are never read
//! when another team's agent decides anything.

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Sort-key prefix of every reward event.
pub const EVENT_PREFIX: &str = "REWARD#";
/// Events are kept long enough for a year of trends.
pub const KEEP_DAYS: i64 = 400;

/// What happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RewardKind {
    /// The author changed the code a review comment pointed at, or accepted it.
    FindingAccepted,
    /// A person argued with a review comment and the judge agreed the
    /// reviewer was wrong, or dismissed the review.
    FindingDismissed,
    /// A review comment was still open when the pull request closed.
    FindingIgnored,
    RatingUp,
    RatingDown,
    /// CoderHelm's pull request merged with no commits from people.
    PrMergedUnchanged,
    /// Merged after people pushed commits; `value` is the share of the diff
    /// they changed (0..1).
    PrMergedEdited,
    PrClosedUnmerged,
    /// A merged CoderHelm change was reverted later.
    PrReverted,
    CiFirstTry,
    /// CI needed fix rounds; `value` is how many.
    CiFixRounds,
    PlanTaskApproved,
    PlanTaskRejected,
    /// `value` is the hours from opening to merging.
    TimeToMerge,
    RereviewRequested,
    /// An alert named a merged CoderHelm change.
    IncidentLinked,
}

impl RewardKind {
    pub const ALL: &'static [RewardKind] = &[
        RewardKind::FindingAccepted,
        RewardKind::FindingDismissed,
        RewardKind::FindingIgnored,
        RewardKind::RatingUp,
        RewardKind::RatingDown,
        RewardKind::PrMergedUnchanged,
        RewardKind::PrMergedEdited,
        RewardKind::PrClosedUnmerged,
        RewardKind::PrReverted,
        RewardKind::CiFirstTry,
        RewardKind::CiFixRounds,
        RewardKind::PlanTaskApproved,
        RewardKind::PlanTaskRejected,
        RewardKind::TimeToMerge,
        RewardKind::RereviewRequested,
        RewardKind::IncidentLinked,
    ];

    pub fn key(self) -> &'static str {
        match self {
            RewardKind::FindingAccepted => "finding_accepted",
            RewardKind::FindingDismissed => "finding_dismissed",
            RewardKind::FindingIgnored => "finding_ignored",
            RewardKind::RatingUp => "rating_up",
            RewardKind::RatingDown => "rating_down",
            RewardKind::PrMergedUnchanged => "pr_merged_unchanged",
            RewardKind::PrMergedEdited => "pr_merged_edited",
            RewardKind::PrClosedUnmerged => "pr_closed_unmerged",
            RewardKind::PrReverted => "pr_reverted",
            RewardKind::CiFirstTry => "ci_first_try",
            RewardKind::CiFixRounds => "ci_fix_rounds",
            RewardKind::PlanTaskApproved => "plan_task_approved",
            RewardKind::PlanTaskRejected => "plan_task_rejected",
            RewardKind::TimeToMerge => "time_to_merge",
            RewardKind::RereviewRequested => "rereview_requested",
            RewardKind::IncidentLinked => "incident_linked",
        }
    }

    pub fn from_key(k: &str) -> Option<RewardKind> {
        RewardKind::ALL.iter().copied().find(|x| x.key() == k)
    }

    /// The name people see.
    pub fn label(self) -> &'static str {
        match self {
            RewardKind::FindingAccepted => "Review comment acted on",
            RewardKind::FindingDismissed => "Review comment dismissed or argued",
            RewardKind::FindingIgnored => "Review comment left open at close",
            RewardKind::RatingUp => "Thumbs up",
            RewardKind::RatingDown => "Thumbs down",
            RewardKind::PrMergedUnchanged => "PR merged as written",
            RewardKind::PrMergedEdited => "PR merged after people's edits",
            RewardKind::PrClosedUnmerged => "PR closed without merging",
            RewardKind::PrReverted => "Merged change reverted",
            RewardKind::CiFirstTry => "CI passed first try",
            RewardKind::CiFixRounds => "CI needed fix rounds",
            RewardKind::PlanTaskApproved => "Plan task approved",
            RewardKind::PlanTaskRejected => "Plan task rejected",
            RewardKind::TimeToMerge => "Time to merge",
            RewardKind::RereviewRequested => "Re-review requested",
            RewardKind::IncidentLinked => "Alert tied to a merged change",
        }
    }

    /// The finding signals: what the reviewer's calibration counts.
    pub fn is_finding(self) -> bool {
        matches!(
            self,
            RewardKind::FindingAccepted | RewardKind::FindingDismissed | RewardKind::FindingIgnored
        )
    }

    /// A signal strong enough on its own to corroborate a lesson (see
    /// `lessons::backed`). Weak signals need company.
    pub fn is_strong(self) -> bool {
        matches!(
            self,
            RewardKind::PrReverted
                | RewardKind::IncidentLinked
                | RewardKind::FindingDismissed
                | RewardKind::PrClosedUnmerged
                | RewardKind::PrMergedUnchanged
                | RewardKind::FindingAccepted
        )
    }
}

/// Which agent an event is about.
pub const AGENT_REVIEWER: &str = "reviewer";
pub const AGENT_CODER: &str = "coder";
pub const AGENT_PLANNER: &str = "planner";
pub const AGENTS: &[&str] = &[AGENT_REVIEWER, AGENT_CODER, AGENT_PLANNER];

/// One outcome signal.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct RewardEvent {
    pub team_id: String,
    pub kind: Option<RewardKind>,
    /// When it happened (RFC 3339), from the source when it says so.
    pub at: String,
    pub agent: String,
    /// `owner/name`; empty for a plan task with no repository yet.
    pub repo: String,
    pub run_id: String,
    /// The review record it belongs to (`REVIEW#...`), if any.
    pub review_sk: String,
    pub pr: u64,
    pub model: String,
    pub prompt_version: String,
    /// The prompt variant the bandit chose (see `bandit`).
    pub variant: String,
    /// `name@version` of each skill applied.
    pub skills: Vec<String>,
    pub plugin: String,
    /// Finding category (`tests`, `style`, `security`, ...).
    pub category: String,
    /// The part of the repository it touched (see [`file_area`]).
    pub area: String,
    /// Magnitude: edit share, CI rounds, hours to merge.
    pub value: f64,
    /// A short line people can read as evidence.
    pub detail: String,
    /// Where it came from (`webhook:pull_request`, `dashboard:rate`, ...).
    pub source: String,
    /// What makes two deliveries of the same signal one event.
    pub dedup: String,
}

impl RewardEvent {
    pub fn new(team_id: &str, kind: RewardKind, agent: &str, at: &str, dedup: &str) -> Self {
        Self {
            team_id: team_id.to_string(),
            kind: Some(kind),
            agent: agent.to_string(),
            at: at.to_string(),
            dedup: dedup.to_string(),
            ..Default::default()
        }
    }

    pub fn with_repo(mut self, repo: &str, pr: u64) -> Self {
        self.repo = repo.to_string();
        self.pr = pr;
        self
    }

    pub fn with_value(mut self, v: f64) -> Self {
        self.value = if v.is_finite() { v } else { 0.0 };
        self
    }

    pub fn with_detail(mut self, d: &str) -> Self {
        self.detail = crate::truncate_str(d.trim(), 400).to_string();
        self
    }

    pub fn with_source(mut self, s: &str) -> Self {
        self.source = s.to_string();
        self
    }
}

/// The time-ordered sort key of an event (the hash keeps two events of the
/// same second apart). Counting once is the [`seen_sk`] marker's job.
pub fn event_sk(at: &str, dedup: &str) -> String {
    let digest = Sha256::digest(dedup.as_bytes());
    let hash: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("{EVENT_PREFIX}{at}#{hash}")
}

/// Sort-key prefix of the marker that makes each signal count once.
pub const SEEN_PREFIX: &str = "REWARDSEEN#";

/// The once-only marker of a signal.
pub fn seen_sk(dedup: &str) -> String {
    let digest = Sha256::digest(dedup.as_bytes());
    let hash: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("{SEEN_PREFIX}{hash}")
}

/// The part of a repository a path belongs to: its first two directories
/// (`services/worker/src/x.rs` → `services/worker`), or the file's directory.
pub fn file_area(path: &str) -> String {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() <= 1 {
        return String::new();
    }
    let dirs = &parts[..parts.len() - 1];
    dirs.iter().take(2).copied().collect::<Vec<_>>().join("/")
}

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn get_s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn get_f(item: &Item, k: &str) -> f64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

impl RewardEvent {
    pub fn sk(&self) -> String {
        event_sk(&self.at, &self.dedup)
    }

    pub fn to_item(&self) -> Item {
        let ttl = chrono::DateTime::parse_from_rfc3339(&self.at)
            .map(|d| d.timestamp())
            .unwrap_or_else(|_| chrono::Utc::now().timestamp())
            + KEEP_DAYS * 86_400;
        let mut i = Item::from([
            ("pk".to_string(), s(&self.team_id)),
            ("sk".to_string(), s(&self.sk())),
            ("record_type".to_string(), s("reward_event")),
            (
                "kind".to_string(),
                s(self.kind.map(RewardKind::key).unwrap_or("")),
            ),
            ("at".to_string(), s(&self.at)),
            ("agent".to_string(), s(&self.agent)),
            (
                "value".to_string(),
                AttributeValue::N(self.value.to_string()),
            ),
            ("pr".to_string(), AttributeValue::N(self.pr.to_string())),
            ("ttl".to_string(), AttributeValue::N(ttl.to_string())),
        ]);
        for (k, v) in [
            ("repo", &self.repo),
            ("run_id", &self.run_id),
            ("review_sk", &self.review_sk),
            ("model", &self.model),
            ("prompt_version", &self.prompt_version),
            ("variant", &self.variant),
            ("plugin", &self.plugin),
            ("category", &self.category),
            ("area", &self.area),
            ("detail", &self.detail),
            ("source", &self.source),
            ("dedup", &self.dedup),
        ] {
            if !v.is_empty() {
                i.insert(k.to_string(), s(v));
            }
        }
        if !self.skills.is_empty() {
            i.insert(
                "skills".to_string(),
                AttributeValue::Ss(self.skills.clone()),
            );
        }
        i
    }

    pub fn from_item(item: &Item) -> Option<Self> {
        let sk = get_s(item, "sk");
        if !sk.starts_with(EVENT_PREFIX) {
            return None;
        }
        let kind = RewardKind::from_key(&get_s(item, "kind"))?;
        Some(Self {
            team_id: get_s(item, "pk"),
            kind: Some(kind),
            at: get_s(item, "at"),
            agent: get_s(item, "agent"),
            repo: get_s(item, "repo"),
            run_id: get_s(item, "run_id"),
            review_sk: get_s(item, "review_sk"),
            pr: get_f(item, "pr") as u64,
            model: get_s(item, "model"),
            prompt_version: get_s(item, "prompt_version"),
            variant: get_s(item, "variant"),
            skills: item
                .get("skills")
                .and_then(|v| v.as_ss().ok())
                .cloned()
                .unwrap_or_default(),
            plugin: get_s(item, "plugin"),
            category: get_s(item, "category"),
            area: get_s(item, "area"),
            value: get_f(item, "value"),
            detail: get_s(item, "detail"),
            source: get_s(item, "source"),
            dedup: get_s(item, "dedup"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_round_trip_by_key() {
        for k in RewardKind::ALL {
            assert_eq!(RewardKind::from_key(k.key()), Some(*k));
            assert!(!k.label().is_empty());
        }
        assert_eq!(RewardKind::from_key("nope"), None);
    }

    #[test]
    fn the_same_signal_twice_has_one_key() {
        let a = event_sk("2026-10-01T00:00:00Z", "merge#acme/web#12");
        let b = event_sk("2026-10-01T00:00:00Z", "merge#acme/web#12");
        let c = event_sk("2026-10-01T00:00:00Z", "merge#acme/web#13");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with("REWARD#2026-10-01T00:00:00Z#"));
        assert_eq!(seen_sk("x"), seen_sk("x"));
        assert_ne!(seen_sk("x"), seen_sk("y"));
        assert!(seen_sk("x").starts_with(SEEN_PREFIX));
    }

    #[test]
    fn areas_are_the_first_two_directories() {
        assert_eq!(file_area("services/worker/src/x.rs"), "services/worker");
        assert_eq!(file_area("src/a.ts"), "src");
        assert_eq!(file_area("README.md"), "");
    }

    #[test]
    fn events_round_trip_through_items_under_the_team_key() {
        let mut e = RewardEvent::new(
            "TEAM#a",
            RewardKind::PrMergedEdited,
            AGENT_CODER,
            "2026-10-01T00:00:00Z",
            "d",
        )
        .with_repo("acme/web", 12)
        .with_value(0.25)
        .with_detail("people changed a quarter");
        e.skills = vec!["tests@3".into()];
        e.variant = "standard".into();
        let item = e.to_item();
        assert_eq!(item["pk"], AttributeValue::S("TEAM#a".into()));
        let back = RewardEvent::from_item(&item).unwrap();
        assert_eq!(back, e);
        assert!(RewardEvent::from_item(&HashMap::new()).is_none());
    }
}
