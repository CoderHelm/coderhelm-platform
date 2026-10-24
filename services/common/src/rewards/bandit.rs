//! Picking between prompt variants by their reward (UCB1, capped).
//!
//! An agent with variants (today the reviewer's prompt) has a default arm.
//! The default is used until it and every other arm have enough samples;
//! under-sampled arms are tried only while the share of reviews spent off
//! the default stays under the exploration cap. Once arms have
//! [`MIN_PULLS`] samples, the arm with the highest upper confidence bound
//! (mean reward + √(2 ln N / n)) is picked. An admin may pin an arm, which
//! wins over everything.
//!
//! Reward per arm is the mean score per event of the reviews that used it,
//! mapped from −1..1 to 0..1.

use serde::Serialize;

/// Samples an arm needs before its mean is trusted.
pub const MIN_PULLS: u64 = 8;
/// Most of an agent's runs spent trying arms other than the default while
/// some are under-sampled.
pub const EXPLORATION_CAP: f64 = 0.1;

/// A variant of an agent's prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Arm {
    pub key: &'static str,
    pub label: &'static str,
    /// Added to the agent's system prompt (empty for the default).
    #[serde(skip)]
    pub addendum: &'static str,
}

/// The reviewer's prompt variants. The first is the default.
pub const REVIEWER_ARMS: &[Arm] = &[
    Arm {
        key: "standard",
        label: "Standard",
        addendum: "",
    },
    Arm {
        key: "evidence_first",
        label: "Evidence first",
        addendum: "\n\n## How to write findings\nLead every finding with the line of the diff or \
                   the tool result it rests on. Leave out a concern you cannot tie to a line you \
                   were shown.",
    },
    Arm {
        key: "focused",
        label: "Focused",
        addendum: "\n\n## What to raise\nRaise what a careful senior reviewer would hold the PR \
                   for: defects, missing tests, risky behaviour, broken contracts. Leave out \
                   matters of taste.",
    },
];

/// The prompt version of each agent's base prompt. Bump when the base prompt
/// changes in a way that should start a fresh comparison.
pub const REVIEWER_PROMPT_VERSION: &str = "review-2026-10";
pub const CODER_PROMPT_VERSION: &str = "implement-2026-10";

/// The arms of an agent; empty when it has no variants.
pub fn arms_for(agent: &str) -> &'static [Arm] {
    match agent {
        super::event::AGENT_REVIEWER => REVIEWER_ARMS,
        _ => &[],
    }
}

pub fn arm(agent: &str, key: &str) -> Option<&'static Arm> {
    arms_for(agent).iter().find(|a| a.key == key)
}

/// What is known about one arm.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ArmStats {
    pub key: String,
    /// Runs that used it.
    pub pulls: u64,
    /// Outcome events of those runs.
    pub events: u64,
    /// Mean score per event (−1..1, see `score`).
    pub mean: f64,
}

impl ArmStats {
    /// The mean mapped to 0..1.
    pub fn reward01(&self) -> f64 {
        (self.mean.clamp(-1.0, 1.0) + 1.0) / 2.0
    }

    /// Trusted once it has enough outcomes, not merely runs.
    pub fn sampled(&self, min: u64) -> bool {
        self.events >= min
    }
}

/// Why an arm was picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Why {
    Pinned,
    Default,
    Explore,
    Best,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Choice {
    pub arm: String,
    pub why: Why,
}

/// The upper confidence bound of `a` among `total` pulls.
pub fn ucb(a: &ArmStats, total: u64) -> f64 {
    if a.events == 0 {
        return f64::INFINITY;
    }
    let n = a.events as f64;
    let t = (total.max(1)) as f64;
    a.reward01() + (2.0 * t.ln().max(0.0) / n).sqrt()
}

/// Picks an arm. `stats` may miss arms (no data yet); `pinned` wins when it
/// names an arm.
pub fn choose(
    arms: &[Arm],
    stats: &[ArmStats],
    pinned: Option<&str>,
    min: u64,
    cap: f64,
) -> Choice {
    let Some(default) = arms.first() else {
        return Choice {
            arm: String::new(),
            why: Why::Default,
        };
    };
    if let Some(p) = pinned.filter(|p| arms.iter().any(|a| a.key == *p)) {
        return Choice {
            arm: p.to_string(),
            why: Why::Pinned,
        };
    }
    let get = |k: &str| {
        stats
            .iter()
            .find(|s| s.key == k)
            .cloned()
            .unwrap_or(ArmStats {
                key: k.to_string(),
                ..Default::default()
            })
    };
    let all: Vec<ArmStats> = arms.iter().map(|a| get(a.key)).collect();
    let total_pulls: u64 = all.iter().map(|a| a.pulls).sum();

    // Explore an under-sampled arm while the runs spent on under-sampled
    // arms stay within the cap.
    let under: Vec<&ArmStats> = all.iter().skip(1).filter(|a| !a.sampled(min)).collect();
    if !under.is_empty() {
        let exploring: u64 = under.iter().map(|a| a.pulls).sum();
        let share_after = (exploring + 1) as f64 / (total_pulls + 1) as f64;
        if share_after <= cap {
            let pick = under
                .iter()
                .min_by_key(|a| (a.pulls, a.key.clone()))
                .map(|a| a.key.clone())
                .unwrap_or_default();
            return Choice {
                arm: pick,
                why: Why::Explore,
            };
        }
    }
    // Exploit among the arms with enough evidence, the default among them.
    let eligible: Vec<&ArmStats> = all.iter().filter(|a| a.sampled(min)).collect();
    if !all[0].sampled(min) || eligible.len() < 2 {
        return Choice {
            arm: default.key.to_string(),
            why: Why::Default,
        };
    }
    let total_events: u64 = eligible.iter().map(|a| a.events).sum();
    let best = eligible
        .iter()
        .max_by(|a, b| {
            ucb(a, total_events)
                .partial_cmp(&ucb(b, total_events))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|a| a.key.clone())
        .unwrap_or_else(|| default.key.to_string());
    let why = if best == default.key {
        Why::Default
    } else {
        Why::Best
    };
    Choice { arm: best, why }
}

/// The arm with the best mean among sampled arms: what the dashboard calls
/// the leader, and what a change-log entry records when it moves.
pub fn leader(arms: &[Arm], stats: &[ArmStats], min: u64) -> Option<String> {
    arms.iter()
        .filter_map(|a| stats.iter().find(|s| s.key == a.key))
        .filter(|s| s.sampled(min))
        .max_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|s| s.key.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(key: &str, pulls: u64, events: u64, mean: f64) -> ArmStats {
        ArmStats {
            key: key.into(),
            pulls,
            events,
            mean,
        }
    }

    #[test]
    fn an_admin_pin_wins_and_unknown_pins_are_ignored() {
        let c = choose(
            REVIEWER_ARMS,
            &[],
            Some("focused"),
            MIN_PULLS,
            EXPLORATION_CAP,
        );
        assert_eq!(c.arm, "focused");
        assert_eq!(c.why, Why::Pinned);
        let c = choose(
            REVIEWER_ARMS,
            &[],
            Some("bogus"),
            MIN_PULLS,
            EXPLORATION_CAP,
        );
        assert_ne!(c.why, Why::Pinned);
    }

    #[test]
    fn with_no_data_the_default_runs_and_exploration_stays_under_the_cap() {
        // The first run: one try off default would be 100% of runs.
        let c = choose(REVIEWER_ARMS, &[], None, MIN_PULLS, EXPLORATION_CAP);
        assert_eq!(c.arm, "standard");
        assert_eq!(c.why, Why::Default);
        // After 20 default runs, one try is 1/21 < 10%.
        let stats = vec![st("standard", 20, 30, 0.2)];
        let c = choose(REVIEWER_ARMS, &stats, None, MIN_PULLS, EXPLORATION_CAP);
        assert_eq!(c.why, Why::Explore);
        assert_eq!(c.arm, "evidence_first");
        // Simulate many runs: the off-default share never passes the cap.
        let mut pulls = std::collections::HashMap::<String, u64>::new();
        pulls.insert("standard".into(), 20);
        for _ in 0..500 {
            let stats: Vec<ArmStats> = REVIEWER_ARMS
                .iter()
                .map(|a| {
                    let p = pulls.get(a.key).copied().unwrap_or(0);
                    // Outcomes trail runs; a quarter of runs report none yet.
                    st(a.key, p, p * 3 / 4, 0.1)
                })
                .collect();
            let c = choose(REVIEWER_ARMS, &stats, None, MIN_PULLS, EXPLORATION_CAP);
            *pulls.entry(c.arm).or_default() += 1;
            let total: u64 = pulls.values().sum();
            // Runs spent on arms still under-sampled never pass the cap.
            let exploring: u64 = REVIEWER_ARMS
                .iter()
                .skip(1)
                .map(|a| pulls.get(a.key).copied().unwrap_or(0))
                .filter(|p| p * 3 / 4 < MIN_PULLS)
                .sum();
            assert!(exploring as f64 / total as f64 <= EXPLORATION_CAP + 1e-9);
        }
    }

    #[test]
    fn once_sampled_the_better_arm_wins() {
        let stats = vec![
            st("standard", 100, 300, 0.1),
            st("evidence_first", 30, 120, 0.6),
            st("focused", 30, 120, -0.2),
        ];
        let c = choose(REVIEWER_ARMS, &stats, None, MIN_PULLS, EXPLORATION_CAP);
        assert_eq!(c.arm, "evidence_first");
        assert_eq!(c.why, Why::Best);
        assert_eq!(
            leader(REVIEWER_ARMS, &stats, MIN_PULLS).as_deref(),
            Some("evidence_first")
        );
        // A worse variant never displaces a better default.
        let stats = vec![
            st("standard", 100, 3000, 0.5),
            st("evidence_first", 30, 1200, -0.5),
            st("focused", 30, 1200, -0.4),
        ];
        let c = choose(REVIEWER_ARMS, &stats, None, MIN_PULLS, EXPLORATION_CAP);
        assert_eq!(c.arm, "standard");
    }

    #[test]
    fn agents_without_variants_have_no_arms() {
        assert!(arms_for("coder").is_empty());
        let c = choose(&[], &[], Some("x"), MIN_PULLS, EXPLORATION_CAP);
        assert_eq!(c.arm, "");
        assert!(arm("reviewer", "focused").is_some());
        assert!(arm("reviewer", "nope").is_none());
        assert!(ucb(&st("a", 0, 0, 0.0), 10).is_infinite());
    }
}
