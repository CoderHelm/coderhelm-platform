//! A sync's work list, kept between worker runs.
//!
//! One worker run has a time budget (a Lambda stops at 15 minutes), so a
//! big sync is done in steps: the first run lists the selection and plans
//! the sync ([`super::plan_sync`]), then fetches and indexes until its
//! budget is spent, and saves what is left here. The next run (queued at
//! once, and the scheduled sweep as a backstop) picks the list up instead
//! of listing again, until it is empty. Then the removals run and the
//! listing's cursor is committed, so a lost list never skips a change: the
//! next listing starts from the old cursor.
//!
//! The list is an object in the base's folder of the data bucket, not on
//! the source's record (a list of 50,000 items is far bigger than a
//! DynamoDB item may be). It belongs to the selection, settings and limit
//! it was planned for ([`plan_key`]): editing any of them plans again.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::connector::{ConnectorError, Listed};
use super::Source;

/// What is left of one sync.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    /// The plan's key ([`plan_key`]): a different one plans again.
    pub plan: String,
    /// Items still to fetch, in order.
    pub fetch: Vec<Listed>,
    /// External ids to remove once everything is fetched.
    #[serde(default)]
    pub remove: Vec<String>,
    /// The listing's cursor, committed when the list is done.
    #[serde(default)]
    pub cursor: String,
    /// Items this sync brings in, and how many are done.
    pub planned: u32,
    pub done: u32,
    /// What the listing found, for the page while the list runs.
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub total: Option<u64>,
    #[serde(default)]
    pub listed: u32,
    /// Part of the selection couldn't be listed (kind, message).
    #[serde(default)]
    pub problem: Option<(String, String)>,
    /// When it was planned (unix seconds).
    #[serde(default)]
    pub at: i64,
}

/// The key a plan belongs to: what is picked, the settings, and the limit.
pub fn plan_key(s: &Source) -> String {
    let mut h = Sha256::new();
    h.update(serde_json::to_vec(&s.selection).unwrap_or_default());
    h.update([0]);
    h.update(serde_json::to_vec(&s.options).unwrap_or_default());
    h.update([0]);
    h.update(s.limit().to_le_bytes());
    super::hex(&h.finalize())[..32].to_string()
}

/// Where a source's list is kept.
pub fn key(team: &str, kb_id: &str, source_id: &str) -> String {
    format!("{team}/knowledge/{kb_id}/_sync/{source_id}/pending.json")
}

/// A connector error as kept on the list.
pub fn keep_error(e: &ConnectorError) -> (String, String) {
    let kind = match e {
        ConnectorError::Reconnect(_) => "reconnect",
        ConnectorError::Forbidden(_) => "forbidden",
        ConnectorError::NotFound(_) => "not_found",
        ConnectorError::RateLimited(_) => "rate_limited",
        ConnectorError::Config(_) => "config",
        ConnectorError::Temporary(_) => "temporary",
    };
    (kind.to_string(), e.to_string())
}

/// A kept error as a connector error again.
pub fn read_error((kind, m): &(String, String)) -> ConnectorError {
    let m = m.clone();
    match kind.as_str() {
        "reconnect" => ConnectorError::Reconnect(m),
        "forbidden" => ConnectorError::Forbidden(m),
        "not_found" => ConnectorError::NotFound(m),
        "rate_limited" => ConnectorError::RateLimited(m),
        "config" => ConnectorError::Config(m),
        _ => ConnectorError::Temporary(m),
    }
}

impl Pending {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn decode(bytes: &[u8]) -> Option<Pending> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Selected;
    use super::*;

    #[test]
    fn a_plan_belongs_to_its_selection_settings_and_limit() {
        let mut s = Source {
            selection: vec![Selected {
                id: "a".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let k = plan_key(&s);
        assert_eq!(k, plan_key(&s.clone()));
        s.item_limit = Some(2_000);
        let k2 = plan_key(&s);
        assert_ne!(k, k2);
        s.options = serde_json::json!({"types": {}});
        assert_ne!(k2, plan_key(&s));
        assert!(crate::knowledge::owns_key(
            "TEAM#t",
            &key("TEAM#t", "kb", "s")
        ));
    }

    #[test]
    fn lists_and_errors_round_trip() {
        let p = Pending {
            plan: "k".into(),
            fetch: vec![Listed {
                external_id: "x".into(),
                ..Default::default()
            }],
            planned: 3,
            done: 2,
            problem: Some(keep_error(&ConnectorError::Forbidden("no".into()))),
            ..Default::default()
        };
        assert_eq!(Pending::decode(&p.encode()), Some(p.clone()));
        assert_eq!(
            read_error(p.problem.as_ref().unwrap()),
            ConnectorError::Forbidden("no".into())
        );
        assert_eq!(Pending::decode(b"nope"), None);
    }
}
