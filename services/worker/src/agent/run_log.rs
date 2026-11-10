//! Writing a run's agent log (format: `common::agent_log`).
//!
//! A pass that keeps its own log (the build step, CI fixes, security
//! fixes) writes it with [`write`] under its own name. Every other tool loop
//! on a run is logged by `provider::converse` under the pass the run is in
//! ([`set_pass`], called by `update_pass` for every pass), so planning, plan
//! rounds, the test step, the security review, the PR description and
//! feedback rounds are logged too, each model call in its own file.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use tracing::{info, warn};

use crate::WorkerState;

/// The pass each run in this worker is in now (run id to pass name).
static CURRENT: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// More runs than one worker holds at once: past it the map starts over
/// (a lost name only makes a log fall back to its agent's name).
const MAX_RUNS: usize = 4096;

/// The run is in `pass` now.
pub fn set_pass(run_id: &str, pass: &str) {
    if run_id.is_empty() {
        return;
    }
    let mut m = CURRENT.lock().unwrap_or_else(|e| e.into_inner());
    if m.len() >= MAX_RUNS && !m.contains_key(run_id) {
        m.clear();
    }
    m.insert(run_id.to_string(), pass.to_string());
}

/// The pass the run is in now, if this worker set one.
pub fn current_pass(run_id: &str) -> Option<String> {
    CURRENT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(run_id)
        .cloned()
}

/// The stored form of one log: its key, and its body with secrets redacted
/// (`common::agent_log::redact_entry`). `now` is when it ended.
pub fn stored(
    team_id: &str,
    run_id: &str,
    pass: &str,
    step: Option<&str>,
    log: &[Value],
    now: chrono::DateTime<chrono::Utc>,
) -> (String, Value) {
    use common::agent_log as al;
    let ended_at = al::rfc3339(now);
    let started_at = al::first_started_at(log).unwrap_or_else(|| ended_at.clone());
    let started_ms = chrono::DateTime::parse_from_rfc3339(&started_at)
        .map(|t| t.timestamp_millis())
        .unwrap_or_else(|_| now.timestamp_millis());
    let entries: Vec<Value> = log
        .iter()
        .map(|e| {
            let mut e = e.clone();
            al::redact_entry(&mut e);
            e
        })
        .collect();
    (
        al::log_key(team_id, run_id, pass, started_ms),
        al::file_body(pass, step, None, &started_at, &ended_at, entries),
    )
}

/// Writes one log to S3 for the run page's agent log. Each call writes a
/// new file (keyed by when the log started), so a pass that runs again
/// never replaces an earlier round. Never fails the caller.
pub async fn write(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass: &str,
    step: Option<&str>,
    log: &[Value],
) {
    if log.is_empty() || team_id.is_empty() || run_id.is_empty() {
        return;
    }
    let (key, body) = stored(team_id, run_id, pass, step, log, chrono::Utc::now());
    match serde_json::to_vec(&body) {
        Ok(body) => {
            if let Err(e) = state
                .s3
                .put_object()
                .bucket(&state.config.bucket_name)
                .key(&key)
                .body(body.into())
                .content_type("application/json")
                .send()
                .await
            {
                warn!(run_id, pass, error = %e, "Failed to write conversation log to S3");
            } else {
                info!(
                    run_id,
                    pass,
                    entries = log.len(),
                    "Conversation log saved to S3"
                );
            }
        }
        Err(e) => warn!(run_id, pass, error = %e, "Failed to serialize conversation log"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_current_pass_is_per_run() {
        set_pass("run-log-a", "plan");
        set_pass("run-log-b", "implement");
        set_pass("run-log-a", "test");
        assert_eq!(current_pass("run-log-a").as_deref(), Some("test"));
        assert_eq!(current_pass("run-log-b").as_deref(), Some("implement"));
        assert_eq!(current_pass("run-log-none"), None);
        set_pass("", "plan");
        assert_eq!(current_pass(""), None);
    }

    #[test]
    fn a_stored_log_is_keyed_by_its_start_and_redacted() {
        let tok = format!("ghp_{}", "z".repeat(36));
        let log = vec![
            json!({"role": "call_started", "call": 1, "turn": 0,
                   "started_at": "2026-11-10T10:00:00.250Z"}),
            json!({"role": "system_note", "call": 1, "turn": 5, "kind": "turn_budget",
                   "text": format!("note {tok}"), "started_at": "2026-11-10T10:01:00.000Z"}),
        ];
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-10T10:02:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let (key, body) = stored("t1", "r1", "ci_fix", Some("coder"), &log, now);
        assert_eq!(key, "runs/t1/r1/conversation/ci_fix-1794304800250.json");
        assert_eq!(body["format"], 2);
        assert_eq!(body["pass"], "ci_fix");
        assert_eq!(body["step"], "coder");
        assert_eq!(body["started_at"], "2026-11-10T10:00:00.250Z");
        assert_eq!(body["ended_at"], "2026-11-10T10:02:00.000Z");
        assert_eq!(body["entries"].as_array().unwrap().len(), 2);
        assert!(!body.to_string().contains(&tok), "redacted");

        // Two rounds of one pass get two keys.
        let later = vec![json!({"role": "call_started", "started_at": "2026-11-10T10:30:00.000Z"})];
        let (key2, _) = stored("t1", "r1", "ci_fix", None, &later, now);
        assert_ne!(key, key2);
    }
}
