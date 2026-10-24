//! The log of every behavior change the reward system makes.
//!
//! A change is written when a repository's calibration for a category moves
//! (none → downgrade, downgrade → suppress, ...), when the reviewer's leading
//! prompt variant moves or an admin pins one, and when a lesson is written.
//! Each change keeps why, with the evidence, and can be reverted for its
//! repository: a reverted calibration stays off (new evidence is still
//! counted but does not change behavior) until an admin restores it.

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use std::collections::HashMap;

pub const CHANGE_PREFIX: &str = "LEARNCHANGE#";
pub const STATE_PREFIX: &str = "LEARNSTATE#";

pub const KIND_CALIBRATION: &str = "calibration";
pub const KIND_VARIANT: &str = "variant";
pub const KIND_LESSON: &str = "lesson";

/// The repository of team-wide changes (variants).
pub const TEAM_REPO: &str = "*";

pub fn change_sk(id: &str) -> String {
    format!("{CHANGE_PREFIX}{id}")
}

pub fn state_sk(repo: &str, kind: &str, key: &str) -> String {
    format!("{STATE_PREFIX}{repo}#{kind}#{key}")
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Change {
    pub id: String,
    pub repo: String,
    pub kind: String,
    pub key: String,
    pub from: String,
    pub to: String,
    pub reason: String,
    /// Evidence lines (JSON objects as the dashboard shows them).
    pub evidence: Vec<serde_json::Value>,
    pub at: String,
    pub reverted: bool,
    pub reverted_by: String,
    pub reverted_at: String,
}

/// What is applied now for one (repo, kind, key).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct State {
    pub current: String,
    pub change_id: String,
    pub reverted: bool,
}

type Item = HashMap<String, AttributeValue>;

fn gs(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn gb(i: &Item, k: &str) -> bool {
    i.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}

impl State {
    pub fn from_item(i: &Item) -> Self {
        Self {
            current: gs(i, "current"),
            change_id: gs(i, "change_id"),
            reverted: gb(i, "reverted"),
        }
    }
}

impl Change {
    pub fn to_item(&self, team_id: &str) -> Item {
        let s = |v: &str| AttributeValue::S(v.to_string());
        Item::from([
            ("pk".to_string(), s(team_id)),
            ("sk".to_string(), s(&change_sk(&self.id))),
            ("record_type".to_string(), s("learning_change")),
            ("id".to_string(), s(&self.id)),
            ("repo".to_string(), s(&self.repo)),
            ("kind".to_string(), s(&self.kind)),
            ("key".to_string(), s(&self.key)),
            ("from".to_string(), s(&self.from)),
            ("to".to_string(), s(&self.to)),
            ("reason".to_string(), s(&self.reason)),
            (
                "evidence".to_string(),
                s(&serde_json::Value::Array(self.evidence.clone()).to_string()),
            ),
            ("at".to_string(), s(&self.at)),
            ("reverted".to_string(), AttributeValue::Bool(self.reverted)),
            ("reverted_by".to_string(), s(&self.reverted_by)),
            ("reverted_at".to_string(), s(&self.reverted_at)),
        ])
    }

    pub fn from_item(i: &Item) -> Option<Self> {
        if !gs(i, "sk").starts_with(CHANGE_PREFIX) {
            return None;
        }
        Some(Self {
            id: gs(i, "id"),
            repo: gs(i, "repo"),
            kind: gs(i, "kind"),
            key: gs(i, "key"),
            from: gs(i, "from"),
            to: gs(i, "to"),
            reason: gs(i, "reason"),
            evidence: serde_json::from_str::<Vec<serde_json::Value>>(&gs(i, "evidence"))
                .unwrap_or_default(),
            at: gs(i, "at"),
            reverted: gb(i, "reverted"),
            reverted_by: gs(i, "reverted_by"),
            reverted_at: gs(i, "reverted_at"),
        })
    }
}

/// Whether a newly decided value is a change to log: it differs from what is
/// applied, and an admin has not reverted this key for the repository.
pub fn is_change(state: Option<&State>, decided: &str) -> bool {
    match state {
        Some(s) if s.reverted => false,
        Some(s) => s.current != decided,
        // Nothing applied yet: only a real adjustment is a change.
        None => decided != "none" && !decided.is_empty(),
    }
}

/// The changes a "revert everything for this repo" turns off: the active
/// (not reverted) changes that are still the applied one for their key.
pub fn active_for_repo<'a>(
    changes: &'a [Change],
    states: &[(String, State)],
    repo: &str,
) -> Vec<&'a Change> {
    changes
        .iter()
        .filter(|c| c.repo == repo && !c.reverted)
        .filter(|c| {
            let sk = state_sk(&c.repo, &c.kind, &c.key);
            states
                .iter()
                .any(|(k, s)| *k == sk && s.change_id == c.id && !s.reverted)
                || c.kind == KIND_LESSON
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(cur: &str, id: &str, rev: bool) -> State {
        State {
            current: cur.into(),
            change_id: id.into(),
            reverted: rev,
        }
    }

    #[test]
    fn only_real_moves_are_logged_and_reverts_hold() {
        assert!(!is_change(None, "none"));
        assert!(is_change(None, "suppress"));
        assert!(!is_change(Some(&st("suppress", "c1", false)), "suppress"));
        assert!(is_change(Some(&st("downgrade", "c1", false)), "suppress"));
        assert!(is_change(Some(&st("suppress", "c1", false)), "none"));
        // An admin's revert holds against new evidence.
        assert!(!is_change(Some(&st("suppress", "c1", true)), "downgrade"));
    }

    #[test]
    fn changes_round_trip_and_repo_reverts_pick_the_live_ones() {
        let c = Change {
            id: "01A".into(),
            repo: "acme/web".into(),
            kind: KIND_CALIBRATION.into(),
            key: "style".into(),
            from: "none".into(),
            to: "suppress".into(),
            reason: "r".into(),
            evidence: vec![serde_json::json!({"kind":"finding_dismissed","pr":3})],
            at: "2026-10-01T00:00:00Z".into(),
            ..Default::default()
        };
        let back = Change::from_item(&c.to_item("TEAM#a")).unwrap();
        assert_eq!(back, c);
        let old = Change {
            id: "009".into(),
            to: "downgrade".into(),
            ..c.clone()
        };
        let other = Change {
            id: "01B".into(),
            repo: "acme/api".into(),
            ..c.clone()
        };
        let lesson = Change {
            id: "01C".into(),
            kind: KIND_LESSON.into(),
            key: "L1".into(),
            ..c.clone()
        };
        let all = vec![old, c.clone(), other, lesson];
        let states = vec![(
            state_sk("acme/web", KIND_CALIBRATION, "style"),
            st("suppress", "01A", false),
        )];
        let ids: Vec<&str> = active_for_repo(&all, &states, "acme/web")
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        assert_eq!(ids, vec!["01A", "01C"]);
    }
}
