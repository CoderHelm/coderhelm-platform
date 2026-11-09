//! One review job, step by step: what the dashboard's session page shows and
//! where the PR's "CoderHelm review" check links (its Details link). One
//! record per job (`pk` = team id, `sk` = [`record_sk`] of the job id that
//! `review_live` records as `job`), so an older check keeps showing its own
//! review, never the latest one.
//!
//! The worker opens the record when the job starts real work, adds a step at
//! each phase, and closes it when the job ends. `steps` is a list that only
//! grows: a new phase or more detail is one more entry (a map with `at`,
//! `text` and an optional `detail`), never a new attribute. Writes are
//! best-effort: the record is a view, never a source of truth.
//!
//! The record expires with the per-PR status ([`RECORD_TTL_SECS`]).

use std::collections::HashMap;

use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use crate::review_live as rl;

/// Sort-key prefix of every record.
pub const SK_PREFIX: &str = "REVIEWSESSION#";

/// Settings-table sort key of a job's record (pk = team id).
pub fn record_sk(job: &str) -> String {
    format!("{SK_PREFIX}{job}")
}

/// Is this a job id the worker makes (a UUID)? Anything else is refused
/// before it reaches a key.
pub fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.chars().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// How long the record is kept after its last write: as long as the PR's
/// live status.
pub const RECORD_TTL_SECS: i64 = rl::RECORD_TTL_SECS;

/// The status words are the per-PR ones (`review_live`): reviewing, done,
/// skipped, retrying (this job didn't finish and was sent again; the retry
/// has its own session) and failed. Read side only: stalled.
pub use rl::{
    PHASE_DONE as STATUS_DONE, PHASE_FAILED as STATUS_FAILED, PHASE_RETRYING as STATUS_RETRYING,
    PHASE_REVIEWING as STATUS_REVIEWING, PHASE_SKIPPED as STATUS_SKIPPED,
    PHASE_STALLED as STATUS_STALLED,
};

/// The status as of `now`: a review past `expires_at` (a worker that died
/// mid-review) is stalled.
pub fn effective_status(status: &str, expires_at: Option<i64>, now: i64) -> &str {
    match expires_at {
        Some(t) if status == STATUS_REVIEWING && t < now => STATUS_STALLED,
        _ => status,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// Pure: one entry of `steps`.
pub fn step_value(at: i64, text: &str, detail: &str) -> AttributeValue {
    let mut m = HashMap::from([
        ("at".to_string(), n(at)),
        ("text".to_string(), s(crate::truncate_str(text, 300))),
    ]);
    if !detail.is_empty() {
        m.insert("detail".to_string(), s(crate::truncate_str(detail, 2_000)));
    }
    AttributeValue::M(m)
}

/// Appends `:step` (a one-entry list) to `steps`, creating it when missing.
pub const APPEND_STEP: &str = "#steps = list_append(if_not_exists(#steps, :none), :step)";

/// Every other write needs the record the job opened: a job that never
/// started real work leaves no session.
pub const EXISTS: &str = "attribute_exists(sk)";

/// Bind every `#name` placeholder `expr` uses to the attribute of that name
/// (several are DynamoDB reserved words, such as `status` and `ttl`).
fn named(mut req: UpdateItemFluentBuilder, expr: &str) -> UpdateItemFluentBuilder {
    for name in placeholders(expr) {
        req = req.expression_attribute_names(format!("#{name}"), name);
    }
    req
}

/// Pure: the distinct `#name` placeholders in `expr`, in order.
pub fn placeholders(expr: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for w in expr.split(|c: char| !(c == '#' || c == '_' || c.is_ascii_alphanumeric())) {
        if let Some(name) = w.strip_prefix('#') {
            if !name.is_empty() && !out.iter().any(|o| o == name) {
                out.push(name.to_string());
            }
        }
    }
    out
}

/// The job that started real work.
pub struct Open<'a> {
    pub job: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    pub head_sha: &'a str,
    pub title: &'a str,
    pub trigger: &'a str,
    pub is_question: bool,
    pub attempt: u32,
    /// How long the job may run before the session shows as stalled.
    pub budget_secs: i64,
}

/// Pure: the update that opens (or, for a job that runs a second pass,
/// re-opens) a session. `started_at` keeps the first start.
pub fn open_update(has_title: bool) -> String {
    let mut sets = vec![
        "#status = :st".to_string(),
        "#job = :j".to_string(),
        "#repo = :repo".to_string(),
        "#pr_number = :pr".to_string(),
        "#head_sha = :h".to_string(),
        "#trigger = :trig".to_string(),
        "#kind = :kind".to_string(),
        "#attempt = :att".to_string(),
        "#started_at = if_not_exists(#started_at, :now)".to_string(),
        "#expires_at = :exp".to_string(),
        "#updated_at = :at".to_string(),
        "#ttl = :ttl".to_string(),
        APPEND_STEP.to_string(),
    ];
    if has_title {
        sets.push("#pr_title = :title".to_string());
    }
    format!("SET {}", sets.join(", "))
}

/// Open the session of `o.job`, with its first step.
pub async fn open(dynamo: &DynamoClient, table: &str, team_id: &str, o: &Open<'_>, step: &str) {
    let now = chrono::Utc::now().timestamp();
    let update = open_update(!o.title.is_empty());
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(o.job)))
        .update_expression(&update)
        .expression_attribute_values(":st", s(STATUS_REVIEWING))
        .expression_attribute_values(":j", s(o.job))
        .expression_attribute_values(":repo", s(&format!("{}/{}", o.owner, o.repo)))
        .expression_attribute_values(":pr", n(o.pr as i64))
        .expression_attribute_values(":h", s(o.head_sha))
        .expression_attribute_values(":trig", s(o.trigger))
        .expression_attribute_values(":kind", s(rl::kind(o.is_question)))
        .expression_attribute_values(":att", n(i64::from(o.attempt)))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":exp", n(now + o.budget_secs.max(60)))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + RECORD_TTL_SECS))
        .expression_attribute_values(":none", AttributeValue::L(vec![]))
        .expression_attribute_values(":step", AttributeValue::L(vec![step_value(now, step, "")]));
    if !o.title.is_empty() {
        req = req.expression_attribute_values(":title", s(crate::truncate_str(o.title, 300)));
    }
    log_unless_missing(named(req, &update).send().await.err(), o.job, "open");
}

/// Pure: the update that adds one step.
pub fn step_update() -> String {
    format!("SET {APPEND_STEP}, #updated_at = :at")
}

/// Add a step to `job`'s session. `detail` may be empty.
pub async fn step(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    job: &str,
    text: &str,
    detail: &str,
) {
    let now = chrono::Utc::now().timestamp();
    let update = step_update();
    let req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(&update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":none", AttributeValue::L(vec![]))
        .expression_attribute_values(
            ":step",
            AttributeValue::L(vec![step_value(now, text, detail)]),
        )
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
    log_unless_missing(named(req, &update).send().await.err(), job, "step");
}

/// Record the GitHub check run that links to this session.
pub async fn set_check_run(dynamo: &DynamoClient, table: &str, team_id: &str, job: &str, id: u64) {
    let update = "SET #check_run_id = :c, #updated_at = :at";
    let req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":c", n(id as i64))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
    log_unless_missing(named(req, update).send().await.err(), job, "check run");
}

/// What the review decided.
pub struct Verdict<'a> {
    /// `APPROVE` or `REQUEST_CHANGES`.
    pub verdict: &'a str,
    pub findings: usize,
    /// The posted GitHub review; empty when nothing new was posted.
    pub review_url: &'a str,
}

/// Pure: the update that records the verdict.
pub fn verdict_update(has_url: bool) -> String {
    let mut sets = vec!["#verdict = :v", "#findings = :f", "#updated_at = :at"];
    if has_url {
        sets.push("#review_url = :u");
    }
    format!("SET {}", sets.join(", "))
}

/// Record the verdict of `job`'s review.
pub async fn note_verdict(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    job: &str,
    v: &Verdict<'_>,
) {
    let update = verdict_update(!v.review_url.is_empty());
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(&update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":v", s(v.verdict))
        .expression_attribute_values(":f", n(v.findings as i64))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
    if !v.review_url.is_empty() {
        req = req.expression_attribute_values(":u", s(v.review_url));
    }
    log_unless_missing(named(req, &update).send().await.err(), job, "verdict");
}

/// Pure: the update that closes a session.
pub fn close_update(has_reason: bool) -> String {
    let mut expr = format!(
        "SET #status = :st, #finished_at = :now, #updated_at = :at, #ttl = :ttl, {APPEND_STEP}"
    );
    if has_reason {
        expr.push_str(", #reason = :why REMOVE #expires_at");
    } else {
        expr.push_str(" REMOVE #expires_at, #reason");
    }
    expr
}

/// Close `job`'s session with `status` (one of the `STATUS_*` words), its
/// last step, and why when it didn't end with a review.
pub async fn close(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    job: &str,
    status: &str,
    reason: Option<&str>,
    step: &str,
) {
    let now = chrono::Utc::now().timestamp();
    let update = close_update(reason.is_some());
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(&update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":st", s(status))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + RECORD_TTL_SECS))
        .expression_attribute_values(":none", AttributeValue::L(vec![]))
        .expression_attribute_values(":step", AttributeValue::L(vec![step_value(now, step, "")]));
    if let Some(why) = reason {
        req = req.expression_attribute_values(":why", s(crate::truncate_str(why, 500)));
    }
    log_unless_missing(named(req, &update).send().await.err(), job, "close");
}

/// Pure: one entry of `activity` ([`crate::review_activity::Event`]).
pub fn activity_value(e: &crate::review_activity::Event) -> AttributeValue {
    let mut m = HashMap::from([
        ("at".to_string(), n(e.at_ms)),
        ("kind".to_string(), s(&e.kind)),
    ]);
    for (k, v) in [
        ("label", &e.label),
        ("heading", &e.heading),
        ("text", &e.text),
    ] {
        if !v.is_empty() {
            m.insert(k.to_string(), s(v));
        }
    }
    if e.failed {
        m.insert("failed".to_string(), AttributeValue::Bool(true));
    }
    AttributeValue::M(m)
}

/// Pure: the update that appends a batch of activity (`:ev`, a list) and
/// counts the events the review's caps dropped (`:d`).
pub fn activity_update() -> String {
    "SET #activity = list_append(if_not_exists(#activity, :none), :ev), #updated_at = :at \
     ADD #activity_dropped :d"
        .to_string()
}

/// Append a batch of what the reviewer did to `job`'s session. The events
/// are already capped and scrubbed ([`crate::review_activity::Recorder`]).
pub async fn append_activity(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    job: &str,
    events: &[crate::review_activity::Event],
    dropped: u64,
) {
    if events.is_empty() && dropped == 0 {
        return;
    }
    let update = activity_update();
    let req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(&update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":none", AttributeValue::L(vec![]))
        .expression_attribute_values(
            ":ev",
            AttributeValue::L(events.iter().map(activity_value).collect()),
        )
        .expression_attribute_values(":d", n(dropped as i64))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
    log_unless_missing(named(req, &update).send().await.err(), job, "activity");
}

/// Pure: the activity of a record, in the order it was appended, at most
/// [`crate::review_activity::MAX_EVENTS`].
pub fn activity_of(item: &HashMap<String, AttributeValue>) -> Vec<crate::review_activity::Event> {
    let Some(list) = item.get("activity").and_then(|v| v.as_l().ok()) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|v| v.as_m().ok())
        .map(|m| {
            let get = |k: &str| {
                m.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            crate::review_activity::Event {
                at_ms: m
                    .get("at")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0),
                kind: get("kind"),
                label: get("label"),
                heading: get("heading"),
                text: get("text"),
                failed: m
                    .get("failed")
                    .and_then(|v| v.as_bool().ok())
                    .copied()
                    .unwrap_or(false),
            }
        })
        .take(crate::review_activity::MAX_EVENTS)
        .collect()
}

/// Pure: how many events the review's caps left out.
pub fn activity_dropped(item: &HashMap<String, AttributeValue>) -> u64 {
    item.get("activity_dropped")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Record the review's "PR Overview" on `job`'s session.
pub async fn set_overview(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    job: &str,
    overview: &crate::review_overview::Overview,
) {
    if overview.is_empty() {
        return;
    }
    let update = "SET #overview = :o, #updated_at = :at";
    let req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(job)))
        .update_expression(update)
        .condition_expression(EXISTS)
        .expression_attribute_values(":o", crate::review_overview::to_attr(overview))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
    log_unless_missing(named(req, update).send().await.err(), job, "overview");
}

/// A condition miss is a job that never opened a session (it stopped before
/// real work); anything else is logged and dropped.
fn log_unless_missing<E, R>(
    err: Option<aws_sdk_dynamodb::error::SdkError<E, R>>,
    job: &str,
    what: &str,
) where
    E: aws_sdk_dynamodb::error::ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    let Some(e) = err else { return };
    if e.as_service_error().and_then(|se| se.code()) == Some("ConditionalCheckFailedException") {
        return;
    }
    tracing::warn!(job, what, error = ?e, "review session: could not record");
}

/// One step as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub at: i64,
    pub text: String,
    pub detail: String,
}

/// Pure: the steps of a record, oldest first (the order they were added).
pub fn steps_of(item: &HashMap<String, AttributeValue>) -> Vec<Step> {
    let Some(list) = item.get("steps").and_then(|v| v.as_l().ok()) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|v| v.as_m().ok())
        .map(|m| {
            let get = |k: &str| {
                m.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            Step {
                at: m
                    .get("at")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0),
                text: get("text"),
                detail: get("detail"),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_ids() {
        let id = "3f2c9a1e-5b7d-4c8e-9a0b-1c2d3e4f5a6b";
        assert_eq!(record_sk(id), format!("REVIEWSESSION#{id}"));
        assert!(valid_id(id));
        for bad in [
            "",
            "x",
            "../REVIEW#acme/web#1",
            &id.replace('-', "x"),
            &format!("{id}0"),
        ] {
            assert!(!valid_id(bad), "{bad}");
        }
    }

    #[test]
    fn a_review_past_its_budget_stalls() {
        assert_eq!(
            effective_status(STATUS_REVIEWING, Some(100), 99),
            STATUS_REVIEWING
        );
        assert_eq!(
            effective_status(STATUS_REVIEWING, Some(100), 101),
            STATUS_STALLED
        );
        assert_eq!(effective_status(STATUS_DONE, Some(100), 101), STATUS_DONE);
        assert_eq!(
            effective_status(STATUS_REVIEWING, None, 101),
            STATUS_REVIEWING
        );
    }

    #[test]
    fn every_write_appends_steps_and_never_replaces_them() {
        for expr in [
            open_update(true),
            open_update(false),
            step_update(),
            close_update(true),
            close_update(false),
        ] {
            assert!(expr.contains(APPEND_STEP), "{expr}");
            assert!(!expr.contains("#steps = :"), "{expr}");
        }
        assert!(APPEND_STEP.contains("list_append(if_not_exists(#steps, :none), :step)"));
        // The verdict and the check run never touch the steps.
        assert!(!verdict_update(true).contains("#steps"));
    }

    #[test]
    fn appended_steps_read_back_in_order() {
        // What DynamoDB holds after an open and two appends.
        let item = HashMap::from([(
            "steps".to_string(),
            AttributeValue::L(vec![
                step_value(10, "Started reviewing `abc1234`", ""),
                step_value(20, "Read the change", "4 files"),
                step_value(30, "Posted the review", ""),
            ]),
        )]);
        let steps = steps_of(&item);
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].text, "Started reviewing `abc1234`");
        assert_eq!(
            steps[1],
            Step {
                at: 20,
                text: "Read the change".into(),
                detail: "4 files".into()
            }
        );
        assert_eq!(steps[2].at, 30);
        assert!(steps_of(&HashMap::new()).is_empty());
        // A long text is cut on a char boundary.
        let long = "é".repeat(400);
        let AttributeValue::M(m) = step_value(1, &long, "") else {
            panic!()
        };
        assert!(m["text"].as_s().unwrap().len() <= 300);
        assert!(!m.contains_key("detail"));
    }

    #[test]
    fn close_keeps_or_clears_the_reason() {
        assert_eq!(
            close_update(false),
            format!(
                "SET #status = :st, #finished_at = :now, #updated_at = :at, #ttl = :ttl, \
                 {APPEND_STEP} REMOVE #expires_at, #reason"
            )
        );
        let with = close_update(true);
        assert!(with.contains("#reason = :why") && with.ends_with("REMOVE #expires_at"));
    }

    #[test]
    fn activity_appends_and_reads_back_in_order() {
        use crate::review_activity::{Event, MAX_EVENTS};
        let update = activity_update();
        assert!(update.contains("list_append(if_not_exists(#activity, :none), :ev)"));
        assert!(update.contains("ADD #activity_dropped :d"));
        assert_eq!(
            placeholders(&update),
            vec!["activity", "updated_at", "activity_dropped"]
        );
        let events = vec![
            Event::note(1, "**Checking callers**\nTwo places.").unwrap(),
            Event::tool(2, "read_file", &serde_json::json!({"path": "a.rs"}), true),
            Event::comment(3, "a.rs", 9, "Off by one"),
        ];
        let item = HashMap::from([
            (
                "activity".to_string(),
                AttributeValue::L(events.iter().map(activity_value).collect()),
            ),
            ("activity_dropped".to_string(), n(4)),
        ]);
        assert_eq!(activity_of(&item), events);
        assert_eq!(activity_dropped(&item), 4);
        // Empty fields aren't written.
        let AttributeValue::M(m) = activity_value(&events[1]) else {
            panic!()
        };
        assert!(!m.contains_key("text") && m.contains_key("failed"));
        // A record past the cap is read back capped.
        let many = HashMap::from([(
            "activity".to_string(),
            AttributeValue::L(vec![activity_value(&events[1]); MAX_EVENTS + 10]),
        )]);
        assert_eq!(activity_of(&many).len(), MAX_EVENTS);
        assert!(activity_of(&HashMap::new()).is_empty());
        assert_eq!(activity_dropped(&HashMap::new()), 0);
    }

    #[test]
    fn placeholders_are_distinct_and_complete() {
        let p = placeholders(&open_update(true));
        assert_eq!(p.iter().filter(|n| *n == "started_at").count(), 1);
        assert_eq!(p.iter().filter(|n| *n == "steps").count(), 1);
        for name in ["status", "ttl", "steps", "pr_title", "expires_at"] {
            assert!(p.iter().any(|n| n == name), "{name}");
        }
        assert_eq!(placeholders(&step_update()), vec!["steps", "updated_at"]);
    }
}
