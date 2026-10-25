//! The shared dead-letter queue: what each failed message is, whose it is,
//! and whether sending it again could ever work.
//!
//! One DLQ holds every team's failed work, plus failed global jobs (the
//! scheduled sweeps carry no team). The gateway's health check, the team's
//! failed-message list, the platform admin's shared-queue view and the
//! worker's cleanup all read a message the same way, through [`classify`].
//!
//! The rules:
//! - A message belongs to a team only when its body is JSON with a string
//!   `team_id` and a `type` the worker reads. Only those count toward that
//!   team's health.
//! - A message the worker can't read (not JSON, no `type`, or a `type` the
//!   worker has no variant for) never succeeds on retry. It is shown as
//!   "can't be retried", is never a team's Problem, and counts only in the
//!   platform admin's shared-queue view.
//! - A message with no team (a global job) is the platform admin's alone.

use serde::Serialize;

/// Every `type` tag the worker's `WorkerMessage` accepts, exactly as it
/// deserializes it. The worker's contract test fails when this list and the
/// enum differ, so the gateway's "can it be read?" answer is the worker's.
pub const WORKER_MESSAGE_TYPES: &[&str] = &[
    "ticket",
    "tool_proposals",
    "hidden_suggest",
    "feedback",
    "onboard",
    "mark_ready",
    "plan_execute",
    "plan_task_continue",
    "infra_analyze",
    "resume",
    "review",
    "review_reminder",
    "review_sweep",
    "health_check",
    "await_merge",
    "tag_sweep",
    "release_notes",
    "graph_index",
    "test_ingest",
    "test_action",
    "skill_sync",
    // The one tag that is not snake_case. Gateway and worker have always
    // agreed on it, so it stays as it is on the wire.
    "SkillAsk",
    "weekly_summary",
    "weekly_summary_test",
    "usage_reconcile",
    "lesson",
    "review_lesson",
    "reward_outcome",
    "jira_comment",
    "dlq_cleanup",
    "log_autofix",
    // Resumes agent work kept while a team's agents were paused.
    "agent_resume_sweep",
    // A model call's usage, held because its counters couldn't be written.
    "usage_record",
];

/// Scheduled jobs that act on every team (EventBridge puts them on the ticket
/// queue). They carry no team, so no team may redrive or see them.
pub const GLOBAL_MESSAGE_TYPES: &[&str] = &[
    "review_reminder",
    "review_sweep",
    "weekly_summary",
    "usage_reconcile",
    "dlq_cleanup",
    "log_autofix",
    "agent_resume_sweep",
];

/// The DLQ keeps a message this long, counted from when it was first sent
/// (SQS keeps the original enqueue time when it moves a message to the DLQ).
/// Past it, SQS drops the message without a trace.
pub const DLQ_RETENTION_DAYS: i64 = 14;

/// An unreadable message this old is archived and deleted by the cleanup.
/// It must be under [`DLQ_RETENTION_DAYS`], or SQS drops the message first
/// and nothing is archived.
pub const CLEANUP_AFTER_DAYS: i64 = 7;

/// Why the worker can't read a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Problem {
    /// The body is not a JSON object.
    NotJson,
    /// A JSON object with no string `type`.
    NoType,
    /// A `type` the worker has no variant for.
    UnknownType { message_type: String },
}

impl Problem {
    /// The reason, in the words the dashboard shows.
    pub fn describe(&self) -> String {
        match self {
            Problem::NotJson => "the message is not JSON".to_string(),
            Problem::NoType => "the message has no type".to_string(),
            Problem::UnknownType { message_type } => {
                format!("unknown message type `{message_type}`")
            }
        }
    }
}

/// What a stored DLQ body is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    /// The body's string `team_id`, when it has one.
    pub team_id: Option<String>,
    /// The body's string `type`, when it has one.
    pub message_type: Option<String>,
    /// Set when the worker can't read the message.
    pub problem: Option<Problem>,
}

/// Read a stored DLQ body. Never fails: anything odd becomes a [`Problem`].
pub fn classify(body: &str) -> Classified {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(body) else {
        return Classified {
            team_id: None,
            message_type: None,
            problem: Some(Problem::NotJson),
        };
    };
    let team_id = map
        .get("team_id")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(String::from);
    let message_type = map.get("type").and_then(|t| t.as_str()).map(String::from);
    let problem = match message_type.as_deref() {
        None => Some(Problem::NoType),
        Some(t) if !WORKER_MESSAGE_TYPES.contains(&t) => Some(Problem::UnknownType {
            message_type: t.to_string(),
        }),
        Some(_) => None,
    };
    Classified {
        team_id,
        message_type,
        problem,
    }
}

impl Classified {
    /// Whether the worker can read it.
    pub fn readable(&self) -> bool {
        self.problem.is_none()
    }

    /// A global job: a readable message whose type acts on every team.
    pub fn global(&self) -> bool {
        self.message_type
            .as_deref()
            .is_some_and(|t| GLOBAL_MESSAGE_TYPES.contains(&t))
    }

    /// The team whose health this message counts toward: a readable,
    /// per-team message's `team_id`. Unreadable messages and global jobs
    /// count toward no team.
    pub fn counts_for_team(&self) -> Option<&str> {
        if !self.readable() || self.global() {
            return None;
        }
        self.team_id.as_deref()
    }

    /// Whether `team_id` may see and delete this message: its body names that
    /// team, readable or not. A team sees its own unreadable messages (marked
    /// "can't be retried") so it can clear them; they never count as its
    /// Problem.
    pub fn visible_to_team(&self, team_id: &str) -> bool {
        !self.global() && self.team_id.as_deref() == Some(team_id)
    }

    /// Whether sending it back to its queue could work: the worker reads it
    /// and it is one team's work.
    pub fn retryable(&self) -> bool {
        self.counts_for_team().is_some()
    }

    /// Shown in the platform admin's shared-queue view as needing them:
    /// unreadable, global, or carrying no team.
    pub fn shared_only(&self) -> bool {
        self.counts_for_team().is_none()
    }
}

/// Whether the cleanup archives and deletes a message: the worker can't
/// read it (judged by the worker's own enum) and it is at least
/// [`CLEANUP_AFTER_DAYS`] old. Readable messages are never cleaned up, at
/// any age: they are someone's work and a person decides.
pub fn cleanup_due(worker_reads_it: bool, age_secs: i64) -> bool {
    !worker_reads_it && age_secs >= CLEANUP_AFTER_DAYS * 86_400
}

/// Seconds since `sent_at_ms` (SQS `SentTimestamp`, epoch milliseconds),
/// never negative.
pub fn age_secs(sent_at_ms: i64, now_ms: i64) -> i64 {
    ((now_ms - sent_at_ms) / 1000).max(0)
}

/// The S3 key a cleaned-up message is archived under.
pub fn archive_key(sent_at_ms: i64, message_id: &str) -> String {
    let day = chrono::DateTime::from_timestamp_millis(sent_at_ms)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown-date".to_string());
    let safe: String = message_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("dlq-archive/{day}/{safe}.json")
}

/// The shared gateway → worker contract: one sample body per message type the
/// gateway sends (exactly as the gateway serializes it) and per scheduled job
/// (as EventBridge sends it). The gateway's test checks it serializes each
/// one to these exact bytes; the worker's test checks it parses each one.
pub const CONTRACT_JSON: &str = include_str!("../contract/worker_messages.json");

/// The variant tags an internally tagged serde enum accepts, read from the
/// error it gives for an unknown one ("unknown variant `x`, expected one of
/// `a`, `b`"). Lets a test list every variant without naming them.
pub fn accepted_tags(unknown_variant_error: &str) -> Vec<String> {
    let Some(rest) = unknown_variant_error.split("expected one of ").nth(1) else {
        return unknown_variant_error
            .split("expected `")
            .nth(1)
            .and_then(|r| r.split('`').next())
            .map(|t| vec![t.to_string()])
            .unwrap_or_default();
    };
    rest.split('`')
        .skip(1)
        .step_by(2)
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEAM: &str = "TEAM#team-a";

    #[test]
    fn a_readable_team_message_counts_for_its_team_only() {
        let c = classify(r#"{"type":"ticket","team_id":"TEAM#team-a","installation_id":1}"#);
        assert!(c.readable());
        assert_eq!(c.counts_for_team(), Some(TEAM));
        assert!(c.visible_to_team(TEAM));
        assert!(!c.visible_to_team("TEAM#team-b"));
        assert!(c.retryable());
        assert!(!c.shared_only());
    }

    #[test]
    fn a_pascal_case_type_is_unknown_and_never_a_team_problem() {
        let c = classify(r#"{"type":"SkillSync","team_id":"TEAM#team-a","repo":"o/r"}"#);
        assert_eq!(
            c.problem,
            Some(Problem::UnknownType {
                message_type: "SkillSync".into()
            })
        );
        assert_eq!(
            c.problem.as_ref().unwrap().describe(),
            "unknown message type `SkillSync`"
        );
        assert_eq!(c.counts_for_team(), None);
        assert!(!c.retryable());
        assert!(c.shared_only());
        // The team still sees it, to delete it.
        assert!(c.visible_to_team(TEAM));
    }

    #[test]
    fn unreadable_bodies_have_no_team() {
        for (body, problem) in [
            ("not json", Problem::NotJson),
            (r#""a string""#, Problem::NotJson),
            ("[1,2]", Problem::NotJson),
            (r#"{"team_id":"TEAM#team-a"}"#, Problem::NoType),
            (r#"{"type":7,"team_id":"TEAM#team-a"}"#, Problem::NoType),
        ] {
            let c = classify(body);
            assert_eq!(c.problem, Some(problem), "{body}");
            assert_eq!(c.counts_for_team(), None, "{body}");
            assert!(c.shared_only(), "{body}");
        }
        assert!(!classify("not json").visible_to_team(TEAM));
    }

    #[test]
    fn global_jobs_belong_to_no_team_even_if_they_name_one() {
        for t in GLOBAL_MESSAGE_TYPES {
            let c = classify(&format!(r#"{{"type":"{t}","team_id":"TEAM#team-a"}}"#));
            assert!(c.readable(), "{t}");
            assert!(c.global(), "{t}");
            assert_eq!(c.counts_for_team(), None, "{t}");
            assert!(!c.visible_to_team(TEAM), "{t}");
            assert!(!c.retryable(), "{t}");
        }
        let c = classify(r#"{"type":"weekly_summary"}"#);
        assert!(c.shared_only());
    }

    #[test]
    fn a_message_with_an_empty_team_has_no_team() {
        let c = classify(r#"{"type":"ticket","team_id":""}"#);
        assert_eq!(c.team_id, None);
        assert!(c.shared_only());
        assert!(!c.visible_to_team(""));
    }

    #[test]
    fn global_types_are_worker_types_and_tags_are_unique() {
        for t in GLOBAL_MESSAGE_TYPES {
            assert!(WORKER_MESSAGE_TYPES.contains(t), "{t}");
        }
        let mut seen = std::collections::BTreeSet::new();
        for t in WORKER_MESSAGE_TYPES {
            assert!(seen.insert(*t), "{t} listed twice");
        }
    }

    #[test]
    fn cleanup_takes_only_old_unreadable_messages() {
        let day = 86_400;
        assert!(!cleanup_due(false, CLEANUP_AFTER_DAYS * day - 1));
        assert!(cleanup_due(false, CLEANUP_AFTER_DAYS * day));
        assert!(cleanup_due(false, 13 * day));
        // Readable work is never cleaned up, however old.
        assert!(!cleanup_due(true, CLEANUP_AFTER_DAYS * day));
        assert!(!cleanup_due(true, 365 * day));
        // The cleanup must act before SQS's retention drops the message.
        const { assert!(CLEANUP_AFTER_DAYS < DLQ_RETENTION_DAYS) };
    }

    #[test]
    fn accepted_tags_reads_serdes_unknown_variant_error() {
        let e = "unknown variant `SkillSync`, expected one of `ticket`, `skill_sync`, `SkillAsk` at line 1 column 20";
        assert_eq!(accepted_tags(e), ["ticket", "skill_sync", "SkillAsk"]);
        assert_eq!(
            accepted_tags("unknown variant `x`, expected `only`"),
            ["only"]
        );
        assert!(accepted_tags("missing field `type`").is_empty());
    }

    #[test]
    fn the_contract_file_is_valid_json_with_both_sections() {
        let v: serde_json::Value = serde_json::from_str(CONTRACT_JSON).unwrap();
        for (section, msgs) in v.as_object().unwrap() {
            for (tag, body) in msgs.as_object().unwrap() {
                assert_eq!(body["type"], tag.as_str(), "{section}.{tag}");
                assert!(WORKER_MESSAGE_TYPES.contains(&tag.as_str()), "{tag}");
            }
        }
        for t in GLOBAL_MESSAGE_TYPES {
            assert!(v["scheduled"].get(*t).is_some(), "{t}");
        }
    }

    #[test]
    fn ages_and_archive_keys() {
        assert_eq!(age_secs(1_000, 61_000), 60);
        assert_eq!(age_secs(61_000, 1_000), 0);
        // 2026-10-19T13:26:27Z
        let sent = 1_792_416_387_000;
        assert_eq!(
            archive_key(sent, "0b8c-41f2/x"),
            "dlq-archive/2026-10-19/0b8c-41f2_x.json"
        );
    }
}
