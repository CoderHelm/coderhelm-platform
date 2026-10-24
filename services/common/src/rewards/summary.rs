//! What the dashboard's Learning page shows, built from one team's records.

use super::bandit::{self, ArmStats};
use super::calibration::{self, CategoryState};
use super::changes::Change;
use super::event::{RewardEvent, RewardKind};
use super::exemplars::Exemplar;
use super::score::{self, Dim, Weights};
use serde_json::{json, Value};

/// Everything the page is built from.
pub struct Inputs<'a> {
    pub team_id: &'a str,
    pub days: u32,
    /// Empty for every repository.
    pub repo: &'a str,
    pub can_manage: bool,
    pub weights: &'a Weights,
    pub events: &'a [RewardEvent],
    pub categories: &'a [(String, CategoryState)],
    pub changes: &'a [Change],
    pub exemplars: &'a [Exemplar],
    /// (agent, pinned arm, arm stats).
    pub arms: &'a [(String, Option<String>, Vec<ArmStats>)],
}

fn r3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

pub fn build(i: &Inputs<'_>) -> Value {
    let in_repo = |r: &str| i.repo.is_empty() || r == i.repo;
    let events: Vec<RewardEvent> = i
        .events
        .iter()
        .filter(|e| e.team_id == i.team_id && in_repo(&e.repo))
        .cloned()
        .collect();
    let w = i.weights;
    let total = score::total(i.team_id, &events, w);
    let trend: Vec<Value> = score::trend(i.team_id, &events, w)
        .into_iter()
        .map(|(date, agent, repo, r)| {
            json!({"date": date, "agent": agent, "repo": repo, "reward": r3(r.mean), "events": r.events})
        })
        .collect();
    let group = |dim: Dim, name: &str| -> Vec<Value> {
        let mut v: Vec<Value> = score::score_by(i.team_id, &events, w, dim)
            .into_iter()
            .map(|(k, r)| {
                json!({name: k, "reward": r3(r.mean), "events": r.events, "positive": r.positive, "negative": r.negative})
            })
            .collect();
        v.sort_by(|a, b| b["events"].as_u64().cmp(&a["events"].as_u64()));
        v
    };
    let signals: Vec<Value> = RewardKind::ALL
        .iter()
        .map(|k| {
            json!({
                "kind": k.key(),
                "label": k.label(),
                "weight": w.get(*k),
                "count": events.iter().filter(|e| e.kind == Some(*k)).count(),
            })
        })
        .collect();
    let mut categories: Vec<Value> = i
        .categories
        .iter()
        .filter(|(r, _)| in_repo(r))
        .map(|(repo, c)| {
            json!({
                "repo": repo,
                "category": c.category,
                "accepted": c.counts.accepted,
                "dismissed": c.counts.dismissed,
                "ignored": c.counts.ignored,
                "accept_rate": c.counts.accept_rate().map(r3),
                "adjustment": c.adjustment.key(),
                "exempt": c.exempt,
                "reverted": c.reverted,
                "reason": calibration::reason(&c.category, &c.counts, c.adjustment),
            })
        })
        .collect();
    categories.sort_by(|a, b| {
        let n = |v: &Value| {
            v["accepted"].as_u64().unwrap_or(0)
                + v["dismissed"].as_u64().unwrap_or(0)
                + v["ignored"].as_u64().unwrap_or(0)
        };
        n(b).cmp(&n(a))
    });
    let changes: Vec<Value> = i
        .changes
        .iter()
        .filter(|c| i.repo.is_empty() || c.repo == i.repo || c.repo == super::changes::TEAM_REPO)
        .take(200)
        .map(|c| serde_json::to_value(c).unwrap_or(Value::Null))
        .collect();
    let mut ex: Vec<&Exemplar> = i
        .exemplars
        .iter()
        .filter(|e| in_repo(&e.repo) && e.polarity().is_some())
        .collect();
    ex.sort_by(|a, b| {
        b.reward
            .abs()
            .partial_cmp(&a.reward.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let exemplars: Vec<Value> = ex
        .into_iter()
        .take(60)
        .map(|e| {
            json!({
                "id": e.id, "repo": e.repo, "kind": e.kind, "polarity": e.polarity(),
                "category": e.category, "area": e.area,
                "text": crate::truncate_str(&e.text, 400),
                "reward": r3(e.reward), "uses": e.uses, "created_at": e.created_at, "pr": e.pr,
            })
        })
        .collect();
    let bandits: Vec<Value> = i
        .arms
        .iter()
        .map(|(agent, pinned, stats)| {
            let arms = bandit::arms_for(agent);
            let total_events: u64 = stats.iter().map(|s| s.events).sum();
            json!({
                "agent": agent,
                "pinned": pinned.clone().unwrap_or_default(),
                "default_arm": arms.first().map(|a| a.key).unwrap_or(""),
                "exploration_cap": bandit::EXPLORATION_CAP,
                "min_pulls": bandit::MIN_PULLS,
                "leader": bandit::leader(arms, stats, bandit::MIN_PULLS),
                "arms": arms.iter().map(|a| {
                    let s = stats.iter().find(|s| s.key == a.key).cloned().unwrap_or_default();
                    let u = bandit::ucb(&s, total_events);
                    json!({
                        "key": a.key, "label": a.label, "pulls": s.pulls, "events": s.events,
                        "mean": r3(s.mean),
                        "ucb": if u.is_finite() { Some(r3(u)) } else { None },
                        "eligible": s.sampled(bandit::MIN_PULLS),
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({
        "days": i.days,
        "repo": i.repo,
        "min_samples": calibration::MIN_SAMPLES,
        "can_manage": i.can_manage,
        "weights": w.effective(),
        "default_weights": Weights::default_map(),
        "signals": signals,
        "totals": {"events": total.events, "reward": r3(total.mean)},
        "trend": trend,
        "agents": group(Dim::Agent, "agent"),
        "repos": group(Dim::Repo, "repo"),
        "categories": categories,
        "changes": changes,
        "exemplars": exemplars,
        "bandit": bandits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewards::calibration::{Adjustment, Counts};
    use crate::rewards::event::{AGENT_CODER, AGENT_REVIEWER};

    #[test]
    fn the_page_shows_only_this_teams_and_repos_data() {
        let w = Weights::defaults();
        let mine = RewardEvent::new(
            "TEAM#a",
            RewardKind::FindingAccepted,
            AGENT_REVIEWER,
            "2026-10-01T00:00:00Z",
            "1",
        )
        .with_repo("acme/web", 1);
        let other_repo = RewardEvent::new(
            "TEAM#a",
            RewardKind::PrReverted,
            AGENT_CODER,
            "2026-10-02T00:00:00Z",
            "2",
        )
        .with_repo("acme/api", 2);
        let other_team = RewardEvent::new(
            "TEAM#b",
            RewardKind::PrReverted,
            AGENT_CODER,
            "2026-10-02T00:00:00Z",
            "3",
        )
        .with_repo("acme/web", 3);
        let events = vec![mine, other_repo, other_team];
        let cats = vec![(
            "acme/web".to_string(),
            CategoryState {
                category: "style".into(),
                counts: Counts {
                    accepted: 1,
                    dismissed: 9,
                    ignored: 0,
                },
                adjustment: Adjustment::Suppress,
                exempt: false,
                reverted: false,
            },
        )];
        let arms = vec![(
            AGENT_REVIEWER.to_string(),
            Some("focused".to_string()),
            vec![],
        )];
        let v = build(&Inputs {
            team_id: "TEAM#a",
            days: 30,
            repo: "acme/web",
            can_manage: false,
            weights: &w,
            events: &events,
            categories: &cats,
            changes: &[],
            exemplars: &[],
            arms: &arms,
        });
        assert_eq!(v["totals"]["events"], 1);
        assert_eq!(v["totals"]["reward"], 1.0);
        assert_eq!(v["trend"].as_array().unwrap().len(), 1);
        assert_eq!(v["categories"][0]["adjustment"], "suppress");
        assert_eq!(v["categories"][0]["accept_rate"], 0.1);
        assert_eq!(v["bandit"][0]["pinned"], "focused");
        assert_eq!(v["bandit"][0]["arms"].as_array().unwrap().len(), 3);
        assert_eq!(
            v["signals"].as_array().unwrap().len(),
            RewardKind::ALL.len()
        );
        assert_eq!(v["can_manage"], false);
        let all = build(&Inputs {
            team_id: "TEAM#a",
            days: 30,
            repo: "",
            can_manage: true,
            weights: &w,
            events: &events,
            categories: &cats,
            changes: &[],
            exemplars: &[],
            arms: &[],
        });
        assert_eq!(all["totals"]["events"], 2);
        assert_eq!(all["repos"].as_array().unwrap().len(), 2);
    }
}
