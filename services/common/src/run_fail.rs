//! The one way a run is marked failed.
//!
//! Every write that sets a run's status to `failed` goes through
//! [`fail_run`], so each failed run says why (`error_message`, a sentence a
//! person can read) and in which pass (`failed_pass`). A test below reads the
//! sources and refuses any other write of the failed status.
//!
//! What ends the run is a [`Cause`], and every ending goes through the one
//! gate (`run_ending::gate`):
//!
//! - [`Cause::Error`]: an error a pass raised, read by
//!   `run_recovery::ending`. Only one CoderHelm gets past by itself
//!   (transient, or a state it repairs) is written as `failed`, in plain
//!   words with the raw error kept apart (`error_detail`): the self-heal
//!   sweep resumes it from its checkpoint. Anything else stops the run as
//!   "needs you" (`needs_input`, with what a person does and why), and no
//!   run shows a raw HTTP dump.
//! - [`Cause::Stopped`]: a stop the pass knew the reason for (red checks,
//!   its review still open, the run guard), typed. It is written as that
//!   reason and never read again by the error classifier: red CI was read
//!   as "an error it doesn't know how to repair, tried again once" with no
//!   retry made.
//! - [`Cause::NeedsYou`]: a wait only a person ends, decided by the
//!   recovery path or by the usage limit.
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

/// What ends the run: see the module comment.
#[derive(Debug, Clone)]
pub enum Cause<'a> {
    /// An error a pass raised, as it was raised (`error_text`).
    Error(&'a str),
    /// A stop the pass knew the reason for: `reason` is its sentence for a
    /// person, `stop` the typed reason and what was tried.
    Stopped {
        reason: &'a str,
        stop: crate::run_ending::Stopped,
    },
    /// A wait only a person ends: what they do and why (with the error's
    /// kind as it was classified when that was decided).
    NeedsYou {
        reason: &'a str,
        needs: &'a crate::run_recovery::NeedsYou,
    },
}

impl Default for Cause<'_> {
    fn default() -> Self {
        Cause::Error("")
    }
}

impl<'a> Cause<'a> {
    /// The words a person reads.
    pub fn reason(&self) -> &'a str {
        match self {
            Cause::Error(r)
            | Cause::Stopped { reason: r, .. }
            | Cause::NeedsYou { reason: r, .. } => r,
        }
    }
}

/// Why a run failed and where.
#[derive(Debug, Clone, Default)]
pub struct RunFailure<'a> {
    /// What ended it.
    pub cause: Cause<'a>,
    /// The pass the run was in. `None` keeps the pass the record names
    /// (`current_pass`), or [`BEFORE_FIRST_PASS`] when it names none.
    pub pass: Option<&'a str>,
    /// Fail the run only while it is still in this status: a sweep that finds
    /// a stuck run must not overwrite one that finished meanwhile.
    pub only_from: Option<&'a str>,
    /// More fields to set in the same write (duration).
    pub extra: Vec<(&'static str, AttributeValue)>,
    /// Numbers added to fields in the same write (tokens, cost): what this
    /// invocation used, on top of what earlier ones recorded.
    pub add: Vec<(&'static str, AttributeValue)>,
}

/// The status-index sort key of a failed run.
pub fn status_run_id(run_id: &str) -> String {
    crate::run_ending::restored(STATUS, run_id)
        .map(|w| w.sri().to_string())
        .unwrap_or_default()
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
    /// The status it writes.
    pub status: &'static str,
    /// The self-heal sweep resumes the run from its checkpoint: an error
    /// CoderHelm gets past by itself, parked as `failed`.
    pub resumable: bool,
    /// What the ticket hears.
    pub said: String,
}

/// The status the write for `f` sets.
pub fn status_of(f: &RunFailure<'_>) -> &'static str {
    update("", f, "").status
}

/// The write that ends `run_id` for a stop (see the module comment).
pub fn update(run_id: &str, f: &RunFailure<'_>, now: &str) -> Update {
    use crate::run_ending::{self as re, RunEnd};
    use crate::run_recovery::{self as rr, Ending};
    let s = |v: &str| AttributeValue::S(v.to_string());
    let raw = readable_reason(f.cause.reason());
    let mut names = HashMap::from([
        ("#s".to_string(), "status".to_string()),
        // Older writes put the reason under `error`; one field from now on.
        ("#legacy".to_string(), "error".to_string()),
        ("#detail".to_string(), rr::FIELD_ERROR_DETAIL.to_string()),
        ("#kind".to_string(), rr::FIELD_ERROR_KIND.to_string()),
    ]);
    // A wait on a person, through the gate with what they do.
    let needs_you = |needs: &rr::NeedsYou| {
        let wait = crate::run_wait::WaitingOn::action(&needs.action, &needs.detail, "")
            .unwrap_or_else(|| {
                crate::run_wait::WaitingOn::fixed(
                    "Look at what stopped the run, then press Retry",
                    &needs.detail,
                    "",
                )
            });
        let why = crate::handback::HandBack::new(needs.why, &needs.detail)
            .unwrap_or_else(|| crate::handback::HandBack::of(needs.why));
        crate::run_wait::needs_input_write(run_id, &wait, &why)
    };
    let (write, plain, kind, wait, resumable, said) = match &f.cause {
        // Only an error a pass raised is read by the classifier.
        Cause::Error(_) => match rr::ending(&raw) {
            Ending::Parked(c) => {
                let w = re::gate(
                    run_id,
                    &RunEnd::Stopped(re::Stopped {
                        reason: re::Stop::Error {
                            kind: c.kind.code().to_string(),
                            plain: c.plain.clone(),
                        },
                        tried: re::Tried::default(),
                    }),
                );
                let said = format!("{} CoderHelm resumes the run by itself.", c.plain);
                (
                    w,
                    c.plain.clone(),
                    c.kind.code().to_string(),
                    None,
                    true,
                    said,
                )
            }
            Ending::NeedsYou(c, needs) => {
                let (w, wo) = needs_you(&needs);
                let said = format!("{} {}.", needs.detail, needs.action);
                (
                    w,
                    needs.detail.clone(),
                    c.kind.code().to_string(),
                    Some(wo),
                    false,
                    said,
                )
            }
        },
        Cause::NeedsYou { needs, .. } => {
            let kind = needs.kind;
            let (w, wo) = needs_you(needs);
            let said = format!("{} {}.", needs.detail, needs.action);
            (
                w,
                needs.detail.clone(),
                kind.code().to_string(),
                Some(wo),
                false,
                said,
            )
        }
        // A typed stop is written as itself.
        Cause::Stopped { stop, .. } => {
            let w = re::gate(run_id, &RunEnd::Stopped(stop.clone()));
            let code = match w.ending() {
                Some(AttributeValue::M(m)) => m
                    .get("reason")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default(),
                _ => String::new(),
            };
            (w, raw.clone(), code, None, false, raw.clone())
        }
    };
    let status = write.status();
    let mut values = HashMap::from([
        (":err".to_string(), s(&readable_reason(&plain))),
        (":detail".to_string(), s(&raw)),
        (":kind".to_string(), s(&kind)),
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
        "SET #s = :failed, status_run_id = :sri, error_message = :err, failed_pass = {pass}, \
         #detail = :detail, #kind = :kind, updated_at = :t"
    );
    write.add_to(":failed", &mut expression, &mut names, &mut values);
    if let Some(wo) = wait {
        names.insert("#wo".into(), crate::run_wait::FIELD_WAITING_ON.to_string());
        values.insert(":wo".into(), wo);
        expression.push_str(", #wo = :wo");
    }
    for (i, (field, value)) in f.extra.iter().enumerate() {
        let (n, v) = (format!("#x{i}"), format!(":x{i}"));
        expression.push_str(&format!(", {n} = {v}"));
        names.insert(n, (*field).to_string());
        values.insert(v, value.clone());
    }
    if !f.add.is_empty() {
        values.insert(":zero".into(), AttributeValue::N("0".into()));
    }
    for (i, (field, value)) in f.add.iter().enumerate() {
        let (n, v) = (format!("#a{i}"), format!(":a{i}"));
        expression.push_str(&format!(", {n} = if_not_exists({n}, :zero) + {v}"));
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
        status,
        resumable,
        said,
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

    /// A reason the self-heal sweep resumes is parked as failed, in plain
    /// words, with the raw error kept apart.
    const PASSING: &str = "HTTP status server error (502 Bad Gateway) for url \
        (https://api.github.com/repos/o/r/pulls): {\"message\":\"Server Error\"}";

    #[test]
    fn a_failure_always_records_a_reason_and_a_pass() {
        let u = update(
            "R1",
            &RunFailure {
                cause: Cause::Error(PASSING),
                pass: Some("feedback"),
                ..Default::default()
            },
            "now",
        );
        assert!(u.expression.contains("error_message = :err"));
        assert!(u.expression.contains("failed_pass = :pass"));
        assert!(u.expression.contains("status_run_id = :sri"));
        assert!(u.expression.ends_with("REMOVE #legacy"));
        assert_eq!(sv(&u, ":failed"), STATUS);
        assert_eq!(
            sv(&u, ":err"),
            "GitHub didn't answer for a moment (502 Bad Gateway)."
        );
        assert_eq!(sv(&u, ":detail"), PASSING);
        assert_eq!(sv(&u, ":kind"), "transient");
        assert_eq!(sv(&u, ":pass"), "feedback");
        assert_eq!(sv(&u, ":sri"), "failed#R1");
        assert_eq!(u.names["#legacy"], "error");
    }

    /// An error nobody classified, or one only a person can get past, never
    /// ends a run as failed: it waits on a person, saying what to do.
    #[test]
    fn an_unclassified_error_never_produces_status_failed() {
        for reason in [
            "  ",
            "boom",
            "Multi-repo pipeline produced no PRs",
            "HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/o/r/pulls): {\"message\":\"Validation Failed\"}",
            "Monthly token limit exceeded (5 / 4 tokens). Increase your limit in Settings under Token Limit.",
        ] {
            let u = update("R1", &RunFailure { cause: Cause::Error(reason), ..Default::default() }, "now");
            assert_ne!(sv(&u, ":failed"), STATUS, "{reason}");
            assert_eq!(sv(&u, ":failed"), crate::run_status::NEEDS_INPUT, "{reason}");
            assert_eq!(sv(&u, ":sri"), "needs_input#R1");
            assert!(u.expression.contains("#wo = :wo"));
            assert_eq!(u.names["#wo"], crate::run_wait::FIELD_WAITING_ON);
            let wait: crate::run_wait::WaitingOn =
                serde_json::from_str(&sv(&u, ":wo")).unwrap();
            assert!(!wait.action.is_empty() && !wait.handback.is_empty());
            assert!(!sv(&u, ":err").contains("https://"), "{reason}");
            assert!(!sv(&u, ":err").contains('{'), "{reason}");
            assert_ne!(u.status, STATUS);
            assert!(!u.resumable);
        }
        let parked = update(
            "R1",
            &RunFailure {
                cause: Cause::Error(PASSING),
                ..Default::default()
            },
            "now",
        );
        assert_eq!(parked.status, STATUS);
        assert!(parked.resumable);
    }

    /// A red CI ending is a typed stop: written as itself, never read by the
    /// error classifier, never "an error it doesn't know how to repair",
    /// never "tried again" without a retry, and never resumed as a pause.
    #[test]
    fn a_red_ci_stop_is_never_read_as_an_unknown_error() {
        use crate::run_ending::{RedCi, Stop, Stopped, Tried};
        let red = RedCi {
            checks: vec!["Deploy Staging".into()],
            head: "0123456789abcdef0123456789abcdef01234567".into(),
            base: "main".into(),
            base_red: vec![],
            excerpt: "expect(received).toEqual(expected)".into(),
        };
        let why = format!(
            "{}. 2 CI fix rounds changed no code. The pull request stays a draft until CI passes",
            red.reason()
        );
        let u = update(
            "R1",
            &RunFailure {
                cause: Cause::Stopped {
                    reason: &why,
                    stop: Stopped {
                        reason: Stop::RedCi(red),
                        tried: Tried {
                            rounds: 2,
                            ..Default::default()
                        },
                    },
                },
                ..Default::default()
            },
            "now",
        );
        assert_eq!(sv(&u, ":failed"), STATUS);
        assert_eq!(sv(&u, ":sri"), "failed#R1");
        assert_eq!(sv(&u, ":kind"), "red_ci");
        assert_ne!(sv(&u, ":kind"), "unknown");
        assert!(!u.resumable, "a red check is no pause to resume");
        assert!(!u.expression.contains("#wo"), "nobody is waited on");
        for text in [sv(&u, ":err"), u.said.clone()] {
            assert!(!text.contains("doesn't know how to repair"), "{text}");
            assert!(!text.contains("tried again"), "{text}");
            assert!(text.contains("Deploy Staging"), "{text}");
        }
        let ending = match &u.values[":run_ending"] {
            AttributeValue::M(m) => m.clone(),
            v => panic!("{v:?}"),
        };
        assert_eq!(ending["kind"], AttributeValue::S("stopped".into()));
        assert!(ending["tried"].as_s().unwrap().contains("2 fix rounds"));
        // The same words as an error a pass raised would have been read as
        // unknown: the typed path is what keeps them from it.
        assert_eq!(
            crate::run_recovery::classify(&why).kind,
            crate::run_recovery::Kind::Unknown
        );
    }

    #[test]
    fn without_a_pass_the_records_own_pass_is_kept() {
        let u = update(
            "R1",
            &RunFailure {
                cause: Cause::Error("boom"),
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
                cause: Cause::Error("timed out"),
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
                cause: Cause::Error("x"),
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

    /// Tokens and cost are added to what the record holds, never set: a
    /// later invocation's failure must not wipe the earlier cost.
    #[test]
    fn a_failure_adds_its_usage_to_the_record() {
        let u = update(
            "R1",
            &RunFailure {
                cause: Cause::Error("x"),
                add: vec![("cost_usd", AttributeValue::N("0.25".into()))],
                ..Default::default()
            },
            "now",
        );
        assert!(u
            .expression
            .contains("#a0 = if_not_exists(#a0, :zero) + :a0"));
        assert_eq!(u.names["#a0"], "cost_usd");
        assert_eq!(u.values[":a0"], AttributeValue::N("0.25".into()));
        assert_eq!(u.values[":zero"], AttributeValue::N("0".into()));
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
