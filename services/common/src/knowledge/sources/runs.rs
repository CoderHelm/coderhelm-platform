//! The sync history: one record per sync of a source, with what started
//! it, how long it took, what it found, added, changed and removed, and the
//! items it couldn't read or left out (each with its fix). Kept 90 days, at
//! most [`MAX_RUNS_PER_SOURCE`] per source.
//!
//! Record: `pk` = team, `sk` = [`run_sk`] (newest last when sorted, so a
//! query read backwards is newest first), with a `ttl`.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use super::sync::Outcome;
use super::{Source, SourceError};

/// How long a run is kept.
pub const RUN_TTL_SECS: i64 = 90 * 24 * 3600;
/// Runs one source keeps at most (older ones go first).
pub const MAX_RUNS_PER_SOURCE: usize = 200;
/// Item problems one run keeps at most.
pub const MAX_RUN_PROBLEMS: usize = 50;

/// One item a run couldn't read, or left out.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemProblem {
    pub name: String,
    #[serde(default)]
    pub url: String,
    /// `failed` or `skipped`.
    pub kind: String,
    pub error: String,
    /// What fixes it, in one sentence.
    #[serde(default)]
    pub fix: String,
}

/// One sync of a source.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRun {
    pub kb_id: String,
    pub source_id: String,
    /// What started it ([`super::trigger`]).
    pub trigger: String,
    pub started_at: String,
    pub ended_at: String,
    pub took_secs: i64,
    /// `done`, `problems` (done, some items not read) or `stopped`.
    pub result: String,
    pub listed: u32,
    pub added: u32,
    pub updated: u32,
    /// Fetched again with the same text, or not fetched at all.
    pub unchanged: u32,
    pub removed: u32,
    pub failed: u32,
    pub skipped: u32,
    pub embedded: u32,
    pub reused: u32,
    /// How it connected (`oauth`, `saved_key`...), when it opened a connection.
    #[serde(default)]
    pub via: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<SourceError>,
    #[serde(default)]
    pub problems: Vec<ItemProblem>,
}

pub fn run_prefix(kb_id: &str, source_id: &str) -> String {
    format!("KBSYNC#{kb_id}#{source_id}#")
}

/// Sorts by when it started: zero-padded seconds.
pub fn run_sk(kb_id: &str, source_id: &str, started_secs: i64) -> String {
    format!(
        "{}{:012}",
        run_prefix(kb_id, source_id),
        started_secs.max(0)
    )
}

/// The fix for an item that couldn't be read, from what went wrong.
pub fn item_fix(error: &str, kind: &str) -> String {
    let e = error.to_lowercase();
    if kind == "skipped" && (e.contains("no text") || e.contains("empty")) {
        "Nothing to search in it. Add text to the page, or leave it out.".into()
    } else if e.contains("scanned") || e.contains("text recognition") {
        "Upload a version with selectable text (export it again, or run text recognition on it)."
            .into()
    } else if e.contains("larger than") || e.contains("too large") {
        "Split it into smaller files, or leave it out.".into()
    } else if e.contains("isn't supported") || e.contains("not supported") {
        "Save it in a format that is read (PDF, Word, Markdown, text, HTML, CSV, Excel).".into()
    } else if e.contains("no text") {
        "Add text to the page, or leave it out.".into()
    } else if e.contains("access") || e.contains("permission") || e.contains("forbidden") {
        "Give the connection access to it in the app, then check for changes.".into()
    } else {
        "It's tried again on the next check.".into()
    }
}

/// The record of one sync that ran, from its outcome.
pub fn from_outcome(
    src: &Source,
    trigger: &str,
    started: i64,
    ended: i64,
    out: &Outcome,
    via: &str,
) -> SyncRun {
    let ts = |s: i64| {
        chrono::DateTime::from_timestamp(s, 0)
            .unwrap_or_default()
            .to_rfc3339()
    };
    let n = |v: usize| u32::try_from(v).unwrap_or(u32::MAX);
    let touched = out.added + out.updated + out.failed + out.skipped;
    let result = if out.error.is_some() {
        "stopped"
    } else if out.failed > 0 {
        "problems"
    } else {
        "done"
    };
    SyncRun {
        kb_id: src.kb_id.clone(),
        source_id: src.id.clone(),
        trigger: super::trigger::clean(trigger).to_string(),
        started_at: ts(started),
        ended_at: ts(ended),
        took_secs: (ended - started).max(0),
        result: result.into(),
        listed: n(out.listed),
        added: n(out.added),
        updated: n(out.updated),
        unchanged: n(out.listed.saturating_sub(touched)),
        removed: n(out.removed),
        failed: n(out.failed),
        skipped: n(out.skipped),
        embedded: n(out.embedded_chunks),
        reused: n(out.reused_chunks),
        via: via.to_string(),
        error: out.error.as_ref().map(|e| super::source_error(src, e)),
        problems: out
            .problems
            .iter()
            .take(MAX_RUN_PROBLEMS)
            .cloned()
            .collect(),
    }
}

/// The record of a sync that couldn't start (its connection didn't open).
pub fn stopped(src: &Source, trigger: &str, at: i64, error: SourceError) -> SyncRun {
    let ts = chrono::DateTime::from_timestamp(at, 0)
        .unwrap_or_default()
        .to_rfc3339();
    SyncRun {
        kb_id: src.kb_id.clone(),
        source_id: src.id.clone(),
        trigger: super::trigger::clean(trigger).to_string(),
        started_at: ts.clone(),
        ended_at: ts,
        result: "stopped".into(),
        error: Some(error),
        ..Default::default()
    }
}

pub fn started_secs(run: &SyncRun) -> i64 {
    chrono::DateTime::parse_from_rfc3339(&run.started_at)
        .map(|t| t.timestamp())
        .unwrap_or(0)
}

/// The settings-table record of a run, expiring after [`RUN_TTL_SECS`].
pub fn run_record(team: &str, run: &SyncRun) -> HashMap<String, AttributeValue> {
    let start = started_secs(run);
    HashMap::from([
        ("pk".into(), AttributeValue::S(team.to_string())),
        (
            "sk".into(),
            AttributeValue::S(run_sk(&run.kb_id, &run.source_id, start)),
        ),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(run).unwrap_or_default()),
        ),
        (
            "ttl".into(),
            AttributeValue::N((start + RUN_TTL_SECS).to_string()),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_sort_by_when_they_started() {
        let a = run_sk("kb", "s", 999);
        let b = run_sk("kb", "s", 1_000);
        assert!(a < b);
        assert!(a.starts_with(&run_prefix("kb", "s")));
        let rec = run_record(
            "t",
            &SyncRun {
                kb_id: "kb".into(),
                source_id: "s".into(),
                started_at: "2026-11-11T10:00:00+00:00".into(),
                ..Default::default()
            },
        );
        let start = 1_794_391_200i64;
        assert_eq!(rec["sk"].as_s().unwrap(), &run_sk("kb", "s", start));
        assert_eq!(
            rec["ttl"].as_n().unwrap(),
            &(start + RUN_TTL_SECS).to_string()
        );
    }

    #[test]
    fn every_item_error_has_a_fix() {
        assert!(item_fix("It has no text to read.", "skipped").contains("Add text"));
        assert!(item_fix(
            "No text found in this PDF. A scanned PDF needs text recognition",
            "failed"
        )
        .contains("selectable text"));
        assert!(item_fix("The file is larger than 25 MB.", "skipped").contains("Split"));
        assert!(item_fix("This kind of file isn't supported.", "skipped").contains("format"));
        assert!(item_fix("no access to it", "failed").contains("access"));
        assert!(item_fix("timeout", "failed").contains("next check"));
    }

    #[test]
    fn a_run_says_how_it_went() {
        let src = Source {
            id: "s".into(),
            kb_id: "kb".into(),
            kind: Some(super::super::Kind::Notion),
            ..Default::default()
        };
        let out = Outcome {
            listed: 10,
            added: 2,
            updated: 1,
            failed: 1,
            ..Default::default()
        };
        let r = from_outcome(&src, "nonsense", 100, 160, &out, "oauth");
        assert_eq!(
            (
                r.result.as_str(),
                r.trigger.as_str(),
                r.took_secs,
                r.unchanged
            ),
            ("problems", "schedule", 60, 6)
        );
        let stopped = Outcome {
            error: Some(super::super::ConnectorError::RateLimited("busy".into())),
            ..Default::default()
        };
        assert_eq!(
            from_outcome(&src, "manual", 0, 1, &stopped, "").result,
            "stopped"
        );
        assert_eq!(
            from_outcome(&src, "change", 0, 1, &Outcome::default(), "").result,
            "done"
        );
    }
}
