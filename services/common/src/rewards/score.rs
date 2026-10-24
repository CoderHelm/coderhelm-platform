//! The reward score: a plain, weighted sum people can read.
//!
//! Each event scores `weight(kind) × shape(kind, value)`, clamped to
//! ±[`MAX_EVENT_SCORE`]. The shape is 1 for most kinds; for the measured ones
//! it turns the value into 0..1 (how much of the diff people left alone, how
//! many CI rounds, how fast it merged). Weights default to [`default_weight`]
//! and a team may tune them in settings ([`Weights`]).
//!
//! Anti-gaming: a group's reward is the MEAN score per event, never a sum, so
//! more comments, longer comments or more pull requests do not earn more.
//! Nothing here reads a comment's length or the number of findings. Review
//! comments left open when a pull request closes count against the review,
//! so a reviewer that raises many points and gets a few acted on scores
//! lower than one that raises only the few.

use super::event::{RewardEvent, RewardKind};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

/// The most one event can move a score, either way.
pub const MAX_EVENT_SCORE: f64 = 3.0;
/// The range a team may set a weight to.
pub const MAX_WEIGHT: f64 = 3.0;
/// Hours after which a merge earns no speed bonus.
pub const MERGE_BONUS_HOURS: f64 = 72.0;
/// CI rounds at which the penalty is full.
pub const CI_ROUNDS_FULL: f64 = 4.0;

/// The weight CoderHelm uses when the team set none.
pub fn default_weight(kind: RewardKind) -> f64 {
    match kind {
        RewardKind::FindingAccepted => 1.0,
        RewardKind::FindingDismissed => -1.0,
        RewardKind::FindingIgnored => -0.25,
        RewardKind::RatingUp => 1.0,
        RewardKind::RatingDown => -1.0,
        RewardKind::PrMergedUnchanged => 1.5,
        RewardKind::PrMergedEdited => 1.0,
        RewardKind::PrClosedUnmerged => -1.0,
        RewardKind::PrReverted => -2.0,
        RewardKind::CiFirstTry => 0.5,
        RewardKind::CiFixRounds => -0.75,
        RewardKind::PlanTaskApproved => 0.5,
        RewardKind::PlanTaskRejected => -0.75,
        RewardKind::TimeToMerge => 0.5,
        // A re-review ask is normal after a fix: recorded, not scored, unless
        // the team decides otherwise.
        RewardKind::RereviewRequested => 0.0,
        RewardKind::IncidentLinked => -2.5,
    }
}

/// The value turned into 0..1 for the measured kinds; 1 for the others.
pub fn shape(kind: RewardKind, value: f64) -> f64 {
    let v = if value.is_finite() { value } else { 0.0 };
    match kind {
        // What people left alone: a full rewrite earns nothing.
        RewardKind::PrMergedEdited => (1.0 - v).clamp(0.0, 1.0),
        RewardKind::CiFixRounds => (v / CI_ROUNDS_FULL).clamp(0.0, 1.0),
        // Fast merges earn a little; slow ones earn nothing, never a penalty.
        RewardKind::TimeToMerge => (1.0 - v / MERGE_BONUS_HOURS).clamp(0.0, 1.0),
        _ => 1.0,
    }
}

/// A team's weights: CoderHelm's defaults with the team's overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Weights {
    overrides: BTreeMap<String, f64>,
}

impl Weights {
    pub fn defaults() -> Self {
        Self::default()
    }

    /// Reads overrides, dropping unknown kinds and clamping each to ±MAX_WEIGHT.
    pub fn from_overrides(raw: &HashMap<String, f64>) -> Self {
        let mut overrides = BTreeMap::new();
        for (k, v) in raw {
            if RewardKind::from_key(k).is_some() && v.is_finite() {
                overrides.insert(k.clone(), v.clamp(-MAX_WEIGHT, MAX_WEIGHT));
            }
        }
        Self { overrides }
    }

    /// Checks a team's request: every key must be a signal and every value a
    /// number. Returns the clamped overrides, or the first bad key.
    pub fn validate(raw: &serde_json::Map<String, serde_json::Value>) -> Result<Self, String> {
        let mut m = HashMap::new();
        for (k, v) in raw {
            if RewardKind::from_key(k).is_none() {
                return Err(format!("unknown signal `{k}`"));
            }
            let Some(n) = v.as_f64().filter(|n| n.is_finite()) else {
                return Err(format!("`{k}` must be a number"));
            };
            m.insert(k.clone(), n);
        }
        Ok(Self::from_overrides(&m))
    }

    pub fn get(&self, kind: RewardKind) -> f64 {
        self.overrides
            .get(kind.key())
            .copied()
            .unwrap_or_else(|| default_weight(kind))
    }

    pub fn overrides(&self) -> &BTreeMap<String, f64> {
        &self.overrides
    }

    /// Every signal's effective weight, by key.
    pub fn effective(&self) -> BTreeMap<String, f64> {
        RewardKind::ALL
            .iter()
            .map(|k| (k.key().to_string(), self.get(*k)))
            .collect()
    }

    pub fn default_map() -> BTreeMap<String, f64> {
        Self::defaults().effective()
    }
}

/// One event's score.
pub fn score(event: &RewardEvent, w: &Weights) -> f64 {
    let Some(kind) = event.kind else {
        return 0.0;
    };
    (w.get(kind) * shape(kind, event.value)).clamp(-MAX_EVENT_SCORE, MAX_EVENT_SCORE)
}

/// A group's reward.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Reward {
    pub events: u64,
    pub total: f64,
    /// Mean score per event: what the dashboard shows and the bandit uses.
    pub mean: f64,
    pub positive: u64,
    pub negative: u64,
}

impl Reward {
    pub fn add(&mut self, s: f64) {
        self.events += 1;
        self.total += s;
        if s > 0.0 {
            self.positive += 1;
        } else if s < 0.0 {
            self.negative += 1;
        }
        self.mean = self.total / self.events as f64;
    }
}

/// What a score can be split by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dim {
    Run,
    Review,
    Category,
    Agent,
    Repo,
    SkillVersion,
    PromptVersion,
    Variant,
    Model,
    Plugin,
}

fn keys(e: &RewardEvent, dim: Dim) -> Vec<String> {
    let one = |v: &str| {
        if v.is_empty() {
            vec![]
        } else {
            vec![v.to_string()]
        }
    };
    match dim {
        Dim::Run => one(&e.run_id),
        Dim::Review => one(&e.review_sk),
        Dim::Category => one(&e.category),
        Dim::Agent => one(&e.agent),
        Dim::Repo => one(&e.repo),
        Dim::SkillVersion => e.skills.clone(),
        Dim::PromptVersion => one(&e.prompt_version),
        Dim::Variant => one(&e.variant),
        Dim::Model => one(&e.model),
        Dim::Plugin => one(&e.plugin),
    }
}

/// The reward of each group along `dim`, for one team's events only.
pub fn score_by(
    team_id: &str,
    events: &[RewardEvent],
    w: &Weights,
    dim: Dim,
) -> BTreeMap<String, Reward> {
    let mut out: BTreeMap<String, Reward> = BTreeMap::new();
    for e in events.iter().filter(|e| e.team_id == team_id) {
        let s = score(e, w);
        for k in keys(e, dim) {
            out.entry(k).or_default().add(s);
        }
    }
    out
}

/// The reward of all of one team's events.
pub fn total(team_id: &str, events: &[RewardEvent], w: &Weights) -> Reward {
    let mut r = Reward::default();
    for e in events.iter().filter(|e| e.team_id == team_id) {
        r.add(score(e, w));
    }
    r
}

/// Daily reward per (date, agent, repo), oldest first.
pub fn trend(
    team_id: &str,
    events: &[RewardEvent],
    w: &Weights,
) -> Vec<(String, String, String, Reward)> {
    let mut m: BTreeMap<(String, String, String), Reward> = BTreeMap::new();
    for e in events.iter().filter(|e| e.team_id == team_id) {
        let date = e.at.get(..10).unwrap_or("").to_string();
        if date.len() != 10 {
            continue;
        }
        m.entry((date, e.agent.clone(), e.repo.clone()))
            .or_default()
            .add(score(e, w));
    }
    m.into_iter().map(|((d, a, r), v)| (d, a, r, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewards::event::{AGENT_CODER, AGENT_REVIEWER};

    fn ev(team: &str, kind: RewardKind, value: f64) -> RewardEvent {
        RewardEvent::new(team, kind, AGENT_REVIEWER, "2026-10-01T00:00:00Z", "x")
            .with_repo("acme/web", 1)
            .with_value(value)
    }

    #[test]
    fn scores_follow_the_weights_and_shapes() {
        let w = Weights::defaults();
        assert_eq!(score(&ev("t", RewardKind::FindingAccepted, 0.0), &w), 1.0);
        assert_eq!(score(&ev("t", RewardKind::FindingDismissed, 0.0), &w), -1.0);
        assert_eq!(score(&ev("t", RewardKind::PrMergedUnchanged, 0.0), &w), 1.5);
        // A quarter of the diff changed by people keeps three quarters.
        assert_eq!(score(&ev("t", RewardKind::PrMergedEdited, 0.25), &w), 0.75);
        // A full rewrite earns nothing, and more than the diff is no penalty.
        assert_eq!(score(&ev("t", RewardKind::PrMergedEdited, 1.0), &w), 0.0);
        assert_eq!(score(&ev("t", RewardKind::PrMergedEdited, 9.0), &w), 0.0);
        assert_eq!(score(&ev("t", RewardKind::CiFixRounds, 2.0), &w), -0.375);
        assert_eq!(score(&ev("t", RewardKind::CiFixRounds, 40.0), &w), -0.75);
        assert_eq!(score(&ev("t", RewardKind::TimeToMerge, 0.0), &w), 0.5);
        assert_eq!(score(&ev("t", RewardKind::TimeToMerge, 500.0), &w), 0.0);
        assert_eq!(score(&ev("t", RewardKind::PrReverted, 0.0), &w), -2.0);
        assert_eq!(score(&ev("t", RewardKind::RereviewRequested, 0.0), &w), 0.0);
        assert_eq!(score(&ev("t", RewardKind::TimeToMerge, f64::NAN), &w), 0.5);
    }

    #[test]
    fn teams_tune_weights_within_bounds() {
        let w = Weights::from_overrides(&HashMap::from([
            ("finding_ignored".to_string(), -0.5),
            ("pr_reverted".to_string(), -50.0),
            ("not_a_signal".to_string(), 2.0),
        ]));
        assert_eq!(w.get(RewardKind::FindingIgnored), -0.5);
        assert_eq!(w.get(RewardKind::PrReverted), -MAX_WEIGHT);
        assert_eq!(w.get(RewardKind::RatingUp), 1.0);
        assert_eq!(w.overrides().len(), 2);
        assert_eq!(w.effective().len(), RewardKind::ALL.len());
        let bad = serde_json::json!({"nope": 1});
        assert!(Weights::validate(bad.as_object().unwrap()).is_err());
        let bad = serde_json::json!({"rating_up": "high"});
        assert!(Weights::validate(bad.as_object().unwrap()).is_err());
        let ok = serde_json::json!({"rating_up": 2});
        assert_eq!(
            Weights::validate(ok.as_object().unwrap())
                .unwrap()
                .get(RewardKind::RatingUp),
            2.0
        );
    }

    #[test]
    fn more_comments_never_earn_more() {
        let w = Weights::defaults();
        // Precise reviewer: two comments, both acted on.
        let precise: Vec<_> = (0..2)
            .map(|_| ev("t", RewardKind::FindingAccepted, 0.0))
            .collect();
        // Noisy reviewer: the same two acted on, plus eight left open.
        let mut noisy = precise.clone();
        noisy.extend((0..8).map(|_| ev("t", RewardKind::FindingIgnored, 0.0)));
        let p = total("t", &precise, &w);
        let n = total("t", &noisy, &w);
        assert!(p.mean > n.mean, "{} vs {}", p.mean, n.mean);
        // Twice the volume of the same quality is the same reward.
        let doubled: Vec<_> = precise.iter().chain(precise.iter()).cloned().collect();
        assert_eq!(total("t", &doubled, &w).mean, p.mean);
    }

    #[test]
    fn long_comments_score_the_same_as_short_ones() {
        let w = Weights::defaults();
        let short = ev("t", RewardKind::FindingAccepted, 0.0).with_detail("x");
        let long = ev("t", RewardKind::FindingAccepted, 0.0).with_detail(&"word ".repeat(500));
        assert_eq!(score(&short, &w), score(&long, &w));
    }

    #[test]
    fn dismissals_reverts_and_broken_ci_are_penalised() {
        let w = Weights::defaults();
        for k in [
            RewardKind::FindingDismissed,
            RewardKind::PrReverted,
            RewardKind::CiFixRounds,
            RewardKind::IncidentLinked,
        ] {
            assert!(score(&ev("t", k, 1.0), &w) < 0.0, "{k:?}");
        }
    }

    #[test]
    fn scores_split_by_dimension_and_never_mix_teams() {
        let w = Weights::defaults();
        let mut a = ev("TEAM#a", RewardKind::FindingAccepted, 0.0);
        a.category = "tests".into();
        a.skills = vec!["s@1".into(), "t@2".into()];
        let mut b = ev("TEAM#a", RewardKind::FindingDismissed, 0.0);
        b.category = "style".into();
        let mut other = ev("TEAM#b", RewardKind::FindingDismissed, 0.0);
        other.category = "tests".into();
        let mut c = ev("TEAM#a", RewardKind::PrMergedUnchanged, 0.0);
        c.agent = AGENT_CODER.into();
        let all = vec![a, b, other, c];
        let cats = score_by("TEAM#a", &all, &w, Dim::Category);
        assert_eq!(cats["tests"].events, 1);
        assert_eq!(cats["tests"].mean, 1.0);
        assert_eq!(cats["style"].mean, -1.0);
        let skills = score_by("TEAM#a", &all, &w, Dim::SkillVersion);
        assert_eq!(skills.len(), 2);
        let agents = score_by("TEAM#a", &all, &w, Dim::Agent);
        assert_eq!(agents["reviewer"].events, 2);
        assert_eq!(agents["coder"].events, 1);
        assert_eq!(total("TEAM#a", &all, &w).events, 3);
        assert_eq!(total("TEAM#b", &all, &w).events, 1);
        let t = trend("TEAM#a", &all, &w);
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].0, "2026-10-01");
    }
}
