//! The one way a run is marked failed.
//!
//! Every write that sets a run's status to `failed` goes through
//! [`fail_run`], so each failed run says why (`error_message`, a sentence a
//! person can read) and in which pass (`failed_pass`). A test below reads the
//! sources and refuses any other write of the failed status.
//!
//! Two failed runs without a reason led here: a feedback pass that lost a race
//! with a cancel wrote a raw "service error" under another field name over the
//! cancelled status, and a run whose start failed was left running until the
//! stuck-run sweep called it a timeout "during unknown pass".

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The run status this module writes.
pub const STATUS: &str = "failed";

/// The day runs started recording why they failed. The analytics page says
/// "Not recorded (before ...)" for a failed run older than this with no reason.
pub const REASONS_RECORDED_SINCE: &str = "2026-10-23";

/// `failed_pass` when the run failed before any pass started.
pub const BEFORE_FIRST_PASS: &str = "before its first pass";

/// Longest reason stored on the run record.
const MAX_REASON_CHARS: usize = 500;

/// Statuses a failure never overwrites: the run already ended another way.
const FINAL_STATUSES: [&str; 2] = ["cancelled", "merged"];

/// Why a run failed and where.
#[derive(Debug, Clone, Default)]
pub struct RunFailure<'a> {
    /// What went wrong, for a person to read.
    pub reason: &'a str,
    /// The pass the run was in. `None` keeps the pass the record names
    /// (`current_pass`), or [`BEFORE_FIRST_PASS`] when it names none.
    pub pass: Option<&'a str>,
    /// Fail the run only while it is still in this status: a sweep that finds
    /// a stuck run must not overwrite one that finished meanwhile.
    pub only_from: Option<&'a str>,
    /// More fields to set in the same write (tokens, cost, duration).
    pub extra: Vec<(&'static str, AttributeValue)>,
}

/// The status-index sort key of a failed run.
pub fn status_run_id(run_id: &str) -> String {
    format!("{STATUS}#{run_id}")
}

/// A reason a person can read: trimmed, never empty, bounded.
pub fn readable_reason(reason: &str) -> String {
    let reason = reason.trim();
    if reason.is_empty() {
        return "Failed without an error message".to_string();
    }
    crate::truncate_str(reason, MAX_REASON_CHARS).to_string()
}

/// An error with its causes. The AWS SDK's own text for a failed call is just
/// "service error"; the cause under it names what DynamoDB refused and why.
pub fn error_text(e: &(dyn std::error::Error + 'static)) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        let text = err.to_string();
        let text = text.trim();
        if !text.is_empty() && !parts.iter().any(|p| p.contains(text)) {
            parts.push(text.to_string());
        }
        cur = err.source();
    }
    parts.join(": ")
}

/// One DynamoDB update, built without a client so it can be checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub expression: String,
    pub condition: String,
    pub names: HashMap<String, String>,
    pub values: HashMap<String, AttributeValue>,
}

/// The write that marks `run_id` failed.
pub fn update(run_id: &str, f: &RunFailure<'_>, now: &str) -> Update {
    let s = |v: &str| AttributeValue::S(v.to_string());
    let mut names = HashMap::from([
        ("#s".to_string(), "status".to_string()),
        // Older writes put the reason under `error`; one field from now on.
        ("#legacy".to_string(), "error".to_string()),
    ]);
    let mut values = HashMap::from([
        (":failed".to_string(), s(STATUS)),
        (":sri".to_string(), s(&status_run_id(run_id))),
        (":err".to_string(), s(&readable_reason(f.reason))),
        (":t".to_string(), s(now)),
    ]);
    let pass = match f.pass.map(str::trim).filter(|p| !p.is_empty()) {
        Some(p) => {
            values.insert(":pass".into(), s(p));
            ":pass".to_string()
        }
        None => {
            values.insert(":pass".into(), s(BEFORE_FIRST_PASS));
            "if_not_exists(current_pass, :pass)".to_string()
        }
    };
    let mut expression = format!(
        "SET #s = :failed, status_run_id = :sri, error_message = :err, failed_pass = {pass}, updated_at = :t"
    );
    for (i, (field, value)) in f.extra.iter().enumerate() {
        let (n, v) = (format!("#x{i}"), format!(":x{i}"));
        expression.push_str(&format!(", {n} = {v}"));
        names.insert(n, (*field).to_string());
        values.insert(v, value.clone());
    }
    expression.push_str(" REMOVE #legacy");
    let condition = match f.only_from {
        Some(from) => {
            values.insert(":from".into(), s(from));
            "attribute_exists(run_id) AND #s = :from".to_string()
        }
        None => {
            let keys: Vec<String> = FINAL_STATUSES
                .iter()
                .enumerate()
                .map(|(i, st)| {
                    values.insert(format!(":final{i}"), s(st));
                    format!(":final{i}")
                })
                .collect();
            format!(
                "attribute_exists(run_id) AND NOT (#s IN ({}))",
                keys.join(", ")
            )
        }
    };
    Update {
        expression,
        condition,
        names,
        values,
    }
}

/// Marks the run failed with a reason and a pass. `Ok(false)`: the run had
/// already ended another way (cancelled, merged), left another status
/// `only_from` asked for, or has no record; nothing was written.
pub async fn fail_run(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    run_id: &str,
    f: &RunFailure<'_>,
) -> Result<bool, String> {
    let u = update(run_id, f, &chrono::Utc::now().to_rfc3339());
    let res = dynamo
        .update_item()
        .table_name(table)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression(u.expression)
        .condition_expression(u.condition)
        .set_expression_attribute_names(Some(u.names))
        .set_expression_attribute_values(Some(u.values))
        .send()
        .await;
    match res {
        Ok(_) => Ok(true),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            tracing::info!(
                run_id,
                "Run already ended another way; not marking it failed"
            );
            Ok(false)
        }
        Err(e) => Err(error_text(&e)),
    }
}

/// Whether a DynamoDB write failed only on its condition.
pub fn is_condition_failed<E, R>(e: &aws_sdk_dynamodb::error::SdkError<E, R>) -> bool
where
    E: aws_sdk_dynamodb::error::ProvideErrorMetadata,
{
    e.as_service_error().and_then(|s| s.code()) == Some("ConditionalCheckFailedException")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(u: &Update, k: &str) -> String {
        u.values[k].as_s().unwrap().clone()
    }

    #[test]
    fn a_failure_always_records_a_reason_and_a_pass() {
        let u = update(
            "R1",
            &RunFailure {
                reason: "  ",
                pass: Some("feedback"),
                ..Default::default()
            },
            "now",
        );
        assert!(u.expression.contains("error_message = :err"));
        assert!(u.expression.contains("failed_pass = :pass"));
        assert!(u.expression.contains("status_run_id = :sri"));
        assert!(u.expression.ends_with("REMOVE #legacy"));
        assert_eq!(sv(&u, ":err"), "Failed without an error message");
        assert_eq!(sv(&u, ":pass"), "feedback");
        assert_eq!(sv(&u, ":sri"), "failed#R1");
        assert_eq!(u.names["#legacy"], "error");
    }

    #[test]
    fn without_a_pass_the_records_own_pass_is_kept() {
        let u = update(
            "R1",
            &RunFailure {
                reason: "boom",
                ..Default::default()
            },
            "now",
        );
        assert!(u
            .expression
            .contains("failed_pass = if_not_exists(current_pass, :pass)"));
        assert_eq!(sv(&u, ":pass"), BEFORE_FIRST_PASS);
    }

    #[test]
    fn a_cancelled_or_merged_run_is_never_overwritten() {
        let u = update("R1", &RunFailure::default(), "now");
        assert!(u.condition.contains("NOT (#s IN (:final0, :final1))"));
        assert_eq!(sv(&u, ":final0"), "cancelled");
        assert_eq!(sv(&u, ":final1"), "merged");
        // No record, no write: a failure never creates a phantom run.
        assert!(u.condition.starts_with("attribute_exists(run_id)"));
    }

    #[test]
    fn a_sweep_fails_only_a_run_still_in_its_status() {
        let u = update(
            "R1",
            &RunFailure {
                reason: "timed out",
                only_from: Some("running"),
                ..Default::default()
            },
            "now",
        );
        assert_eq!(u.condition, "attribute_exists(run_id) AND #s = :from");
        assert_eq!(sv(&u, ":from"), "running");
    }

    #[test]
    fn extra_fields_ride_along() {
        let u = update(
            "R1",
            &RunFailure {
                reason: "x",
                extra: vec![("cost_usd", AttributeValue::N("1.5".into()))],
                ..Default::default()
            },
            "now",
        );
        assert!(u.expression.contains("#x0 = :x0"));
        assert_eq!(u.names["#x0"], "cost_usd");
    }

    #[derive(Debug)]
    struct Outer(Inner);
    #[derive(Debug)]
    struct Inner;
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("service error")
        }
    }
    impl std::fmt::Display for Inner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ConditionalCheckFailedException: The conditional request failed")
        }
    }
    impl std::error::Error for Inner {}
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn error_text_names_the_cause_not_just_service_error() {
        assert_eq!(
            error_text(&Outer(Inner)),
            "service error: ConditionalCheckFailedException: The conditional request failed"
        );
    }

    /// Every status write of `failed` goes through this module. A failed run's
    /// status-index key is `failed#<run_id>`, so any other write of the failed
    /// status has to spell `failed#`; this reads every service's sources and
    /// the stuck-run sweep for it.
    #[test]
    fn no_other_code_writes_the_failed_status() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let mut offenders = Vec::new();
        let mut stack = vec![root.join("services")];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = entry.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if p.is_dir() {
                    if name != "target" && !name.starts_with('.') {
                        stack.push(p);
                    }
                    continue;
                }
                if !name.ends_with(".rs") || p.ends_with("common/src/run_fail.rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&p).unwrap_or_default();
                for (i, line) in text.lines().enumerate() {
                    if line.contains("failed#") {
                        offenders.push(format!("{}:{}", p.display(), i + 1));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "write the failed status with common::run_fail::fail_run: {offenders:?}"
        );
        // The sweep is Python and can't call the helper: it must still write
        // the reason and the pass.
        let sweep =
            std::fs::read_to_string(root.join("lambda/stuck-run-cleanup/handler.py")).unwrap();
        for field in ["error_message", "failed_pass", "last_known_pass"] {
            assert!(sweep.contains(field), "stuck-run sweep must write {field}");
        }
    }
}
