//! What the dashboard's Learning page shows, built from one team's records.

use super::bandit::{self, ArmStats};
use super::calibration::{self, CategoryState};
use super::changes::Change;
use super::event::{RewardEvent, RewardKind};
use super::exemplars::Exemplar;
use super::score::{self, Dim, Weights};
use serde_json::{json, Value};

/// Most examples listed on the page.
pub const MAX_EXEMPLARS: usize = 200;

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
    // Most used first, then the biggest reward: the ones shaping prompts.
    ex.sort_by(|a, b| {
        b.uses.cmp(&a.uses).then_with(|| {
            b.reward
                .abs()
                .partial_cmp(&a.reward.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    let exemplars: Vec<Value> = ex
        .into_iter()
        .take(MAX_EXEMPLARS)
        .map(|e| {
            let d = e.display();
            json!({
                "id": e.id, "repo": e.repo, "kind": e.kind, "polarity": e.polarity(),
                "category": e.category, "area": e.area,
                "title": d.title, "summary": d.summary, "reason": d.reason,
                // Kept for older dashboards: one clean line, never raw markup.
                "text": super::example_text::cut_words(&d.line(), 240),
                "reward": r3(e.reward), "events": e.events, "uses": e.uses,
                "created_at": e.created_at, "pr": e.pr, "run_id": e.run_id,
                "last_used_at": e.last_used_at, "used_by": e.used_by,
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

    #[test]
    fn examples_read_clean_and_most_used_first() {
        use super::super::exemplars::{Exemplar, Use, KIND_FINDING, KIND_FIX};
        let old = Exemplar {
            id: "old".into(),
            repo: "acme/web".into(),
            kind: KIND_FINDING.into(),
            text: "Missing test: <!-- coderhelm:x -->The **retry** path has no test.".into(),
            reward: 2.5,
            uses: 1,
            ..Default::default()
        };
        let used = Exemplar {
            id: "used".into(),
            repo: "acme/web".into(),
            kind: KIND_FIX.into(),
            reward: 1.2,
            uses: 9,
            reason: "CI passed on the first try, merged without changes".into(),
            run_id: "R1".into(),
            last_used_at: "2026-10-02T00:00:00Z".into(),
            used_by: vec![Use {
                at: "2026-10-02T00:00:00Z".into(),
                agent: "coder".into(),
                run_id: "R2".into(),
                pr: 0,
            }],
            ..Default::default()
        }
        .with_text(
            "Retry payments",
            "Source ticket: OPS-1\n\n**Summary** Retry once.",
        );
        let neutral = Exemplar {
            id: "neutral".into(),
            repo: "acme/web".into(),
            reward: 0.2,
            uses: 50,
            ..Default::default()
        };
        let w = Weights::default();
        let v = build(&Inputs {
            team_id: "TEAM#a",
            days: 30,
            repo: "",
            can_manage: true,
            weights: &w,
            events: &[],
            categories: &[],
            changes: &[],
            exemplars: &[old, used, neutral],
            arms: &[],
        });
        let ex = v["exemplars"].as_array().unwrap();
        assert_eq!(ex.len(), 2, "no polarity, not listed");
        assert_eq!(ex[0]["id"], "used");
        assert_eq!(ex[0]["title"], "Retry payments");
        assert_eq!(ex[0]["summary"], "Retry once.");
        assert_eq!(
            ex[0]["reason"],
            "CI passed on the first try, merged without changes"
        );
        assert_eq!(ex[0]["run_id"], "R1");
        assert_eq!(ex[0]["used_by"][0]["run_id"], "R2");
        assert!(ex[0]["used_by"][0].get("pr").is_none());
        assert_eq!(ex[1]["title"], "Missing test");
        assert_eq!(ex[1]["summary"], "The **retry** path has no test.");
        assert_eq!(ex[1]["reason"], "The team acted on this comment");
        assert_eq!(ex[1]["text"], "Missing test: The retry path has no test.");
        assert!(!ex[1]["text"].as_str().unwrap().contains("<!--"));
    }
}
