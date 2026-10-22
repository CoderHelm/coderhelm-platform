//! Where the reviewer is with each PR right now: queued, reviewing, retrying
//! after a service error, or done. One record per PR (`pk` = team id, `sk` =
//! [`record_sk`]). The gateway marks a review queued when it sends the job; the
//! worker marks it reviewing when the job starts real work and done (or
//! retrying, or failed) when it ends.
//!
//! An in-flight phase carries `expires_at`: past it, the phase no longer
//! counts (a worker that crashed mid-review never leaves a PR "reviewing"
//! forever). The record itself expires after [`RECORD_TTL_SECS`].
//!
//! Together with the auto-merge gate's record (`merge_gate`), this gives the
//! dashboard one status per PR: [`pr_status`].

use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

/// Sort-key prefix of every record.
pub const SK_PREFIX: &str = "REVIEWLIVE#";

/// Settings-table sort key of a PR's record (pk = team id).
pub fn record_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!("{SK_PREFIX}{owner}/{repo}#{pr:06}")
}

/// The job is in the queue (a push waits a minute there to settle).
pub const PHASE_QUEUED: &str = "queued";
/// A worker is reading the PR and writing the review.
pub const PHASE_REVIEWING: &str = "reviewing";
/// The review hit a service error and is sent again after a delay.
pub const PHASE_RETRYING: &str = "retrying";
/// Every retry failed; the PR has a note saying so.
pub const PHASE_FAILED: &str = "failed";
/// The last job finished (with a review, an answer, or nothing to do).
pub const PHASE_DONE: &str = "done";
/// The last job looked at a new commit and didn't review it on purpose (it
/// changed no code or data since CoderHelm approved); `reason` says so.
pub const PHASE_SKIPPED: &str = "skipped";
/// Read side only: an in-flight phase past its `expires_at`.
pub const PHASE_STALLED: &str = "stalled";

/// How long a queued job counts as queued: a push waits a minute, a retry
/// hop up to 15, and the queue itself is quick.
pub const QUEUED_FOR_SECS: i64 = 30 * 60;
/// How long the record is kept after its last write.
pub const RECORD_TTL_SECS: i64 = 30 * 86_400;

/// Is this phase work that is still to happen or happening now?
pub fn in_flight(phase: &str) -> bool {
    matches!(phase, PHASE_QUEUED | PHASE_REVIEWING | PHASE_RETRYING)
}

/// The phase as of `now`: an in-flight phase past `expires_at` is stalled.
pub fn effective_phase(phase: &str, expires_at: Option<i64>, now: i64) -> &str {
    match expires_at {
        Some(t) if in_flight(phase) && t < now => PHASE_STALLED,
        _ => phase,
    }
}

/// One PR's status for the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrStatus {
    /// Short machine word the dashboard maps to a label.
    pub status: &'static str,
    /// [`GROUP_ACTIVE`], [`GROUP_WAITING`] or [`GROUP_DONE`].
    pub group: &'static str,
}

/// The reviewer is on it now (or it is queued).
pub const GROUP_ACTIVE: &str = "active";
/// Reviewed; waiting on CI, people, or a fix.
pub const GROUP_WAITING: &str = "waiting";
pub const GROUP_DONE: &str = "done";

/// Pure: the one status a PR shows, from the review phase (as of now, see
/// [`effective_phase`]) and the auto-merge gate's state (empty when no gate
/// is armed). A review in flight wins (it is what happens next); then the
/// gate's state; then how the last job ended.
pub fn pr_status(review_phase: &str, gate_state: &str) -> PrStatus {
    use crate::merge_gate as g;
    let st = |status, group| PrStatus { status, group };
    match review_phase {
        PHASE_REVIEWING => return st("reviewing", GROUP_ACTIVE),
        PHASE_QUEUED => return st("queued", GROUP_ACTIVE),
        PHASE_RETRYING => return st("retrying", GROUP_ACTIVE),
        _ => {}
    }
    match gate_state {
        g::STATE_MERGED => return st("merged", GROUP_DONE),
        g::STATE_CLOSED => return st("closed", GROUP_DONE),
        g::STATE_WAITING_CI => return st("waiting_ci", GROUP_WAITING),
        g::STATE_BLOCKED_CI => return st("ci_failing", GROUP_WAITING),
        g::STATE_WAITING_APPROVAL => return st("waiting_people", GROUP_WAITING),
        g::STATE_DECLINED => return st("merge_blocked", GROUP_WAITING),
        g::STATE_PAUSED => return st("paused", GROUP_WAITING),
        // Waits on a person to re-arm it.
        g::STATE_STOPPED => return st("stopped", GROUP_WAITING),
        _ => {}
    }
    match review_phase {
        PHASE_FAILED => st("failed", GROUP_WAITING),
        PHASE_STALLED => st("stalled", GROUP_WAITING),
        PHASE_SKIPPED => st("skipped", GROUP_DONE),
        // A new commit after the gate's review, with no review queued for
        // it: the gate re-arms when that commit is reviewed.
        _ if gate_state == g::STATE_HEAD_MOVED => st("new_commits", GROUP_DONE),
        _ => st("reviewed", GROUP_DONE),
    }
}

/// What a job is doing: a formal review, or answering a comment.
pub fn kind(is_question: bool) -> &'static str {
    if is_question {
        "question"
    } else {
        "review"
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// Every attribute the expressions here touch (all go through `#name`
/// placeholders: several are DynamoDB reserved words).
const ATTRS: &[&str] = &[
    "phase",
    "queued_at",
    "expires_at",
    "repo",
    "pr_number",
    "trigger",
    "kind",
    "updated_at",
    "ttl",
    "job",
    "started_at",
    "head_sha",
    "attempt",
    "pr_title",
    "reason",
    "finished_at",
];

/// Pure: the attributes whose `#name` placeholder appears in `expr`.
/// DynamoDB refuses a placeholder the expressions don't use.
pub fn placeholders(expr: &str) -> Vec<&'static str> {
    let used: Vec<&str> = expr
        .split(|c: char| !(c == '#' || c == '_' || c.is_ascii_alphanumeric()))
        .filter_map(|w| w.strip_prefix('#'))
        .collect();
    ATTRS.iter().copied().filter(|a| used.contains(a)).collect()
}

fn named(mut req: UpdateItemFluentBuilder, exprs: &str) -> UpdateItemFluentBuilder {
    for name in placeholders(exprs) {
        req = req.expression_attribute_names(format!("#{name}"), name);
    }
    req
}

/// The update that marks a job queued.
pub const QUEUED_UPDATE: &str = "SET #phase = :q, #queued_at = :now, #expires_at = :exp, \
     #repo = :repo, #pr_number = :pr, #trigger = :trig, #kind = :kind, #updated_at = :at, \
     #ttl = :ttl";
/// Queued never hides a review that is running now.
pub const QUEUED_CONDITION: &str = "attribute_not_exists(sk) OR #phase <> :r OR #expires_at < :now";

/// Mark a job queued. Never hides a review that is running now (that job
/// marks itself done when it ends; the queued one marks itself reviewing
/// when it starts). Best-effort: the dashboard status is never worth
/// failing the enqueue for.
#[allow(clippy::too_many_arguments)]
pub async fn mark_queued(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    trigger: &str,
    is_question: bool,
    delay_secs: i64,
) {
    let now = chrono::Utc::now().timestamp();
    let req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(owner, repo, pr)))
        .update_expression(QUEUED_UPDATE)
        .condition_expression(QUEUED_CONDITION)
        .expression_attribute_values(":q", s(PHASE_QUEUED))
        .expression_attribute_values(":r", s(PHASE_REVIEWING))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":exp", n(now + delay_secs.max(0) + QUEUED_FOR_SECS))
        .expression_attribute_values(":repo", s(&format!("{owner}/{repo}")))
        .expression_attribute_values(":pr", n(pr as i64))
        .expression_attribute_values(":trig", s(trigger))
        .expression_attribute_values(":kind", s(kind(is_question)))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + RECORD_TTL_SECS));
    let req = named(req, &format!("{QUEUED_UPDATE} {QUEUED_CONDITION}"));
    log_unless_condition(req.send().await.err(), pr, PHASE_QUEUED);
}

/// A job that started real work (after its dedup and superseded checks).
pub struct Reviewing<'a> {
    pub job: &'a str,
    pub head_sha: &'a str,
    pub title: &'a str,
    pub trigger: &'a str,
    pub is_question: bool,
    pub attempt: u32,
    /// How long the job may run before its phase stops counting.
    pub budget_secs: i64,
}

/// Pure: the update that marks a PR as being reviewed.
pub fn reviewing_update(has_title: bool) -> String {
    let mut sets = vec![
        "#phase = :p",
        "#job = :j",
        "#started_at = :now",
        "#expires_at = :exp",
        "#head_sha = :h",
        "#repo = :repo",
        "#pr_number = :pr",
        "#trigger = :trig",
        "#kind = :kind",
        "#attempt = :att",
        "#updated_at = :at",
        "#ttl = :ttl",
    ];
    if has_title {
        sets.push("#pr_title = :title");
    }
    format!("SET {} REMOVE #reason", sets.join(", "))
}

/// Mark a PR as being reviewed by `r.job` now.
pub async fn mark_reviewing(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    r: &Reviewing<'_>,
) {
    let now = chrono::Utc::now().timestamp();
    let update = reviewing_update(!r.title.is_empty());
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(owner, repo, pr)))
        .update_expression(&update)
        .expression_attribute_values(":p", s(PHASE_REVIEWING))
        .expression_attribute_values(":j", s(r.job))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":exp", n(now + r.budget_secs.max(60)))
        .expression_attribute_values(":h", s(r.head_sha))
        .expression_attribute_values(":repo", s(&format!("{owner}/{repo}")))
        .expression_attribute_values(":pr", n(pr as i64))
        .expression_attribute_values(":trig", s(r.trigger))
        .expression_attribute_values(":kind", s(kind(r.is_question)))
        .expression_attribute_values(":att", n(i64::from(r.attempt)))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + RECORD_TTL_SECS));
    if !r.title.is_empty() {
        req = req.expression_attribute_values(":title", s(crate::truncate_str(r.title, 300)));
    }
    let req = named(req, &update);
    log_unless_condition(req.send().await.err(), pr, PHASE_REVIEWING);
}

/// How a job ended.
pub enum Finished<'a> {
    /// Done (posted a review or answer, or had nothing to do).
    Done,
    /// Failed; sent again to run in `retry_in_secs`.
    Retrying {
        reason: &'a str,
        retry_in_secs: i64,
        budget_secs: i64,
    },
    /// Failed for good.
    Failed { reason: &'a str },
    /// Not reviewed on purpose: `reason` says why (a push after an approval
    /// that changed no code or data). The earlier verdict stands.
    Skipped { reason: &'a str },
}

/// Only the job that owns the record may end it. Not ended by this job: a
/// review another job is running now, or a job queued after this one
/// started.
pub const FINISH_CONDITION: &str = "attribute_exists(sk) AND NOT ((#phase = :r AND #job <> :j \
     AND #expires_at > :now) OR (#phase = :q AND #queued_at > :started))";

/// Pure: the update that records how a job ended.
pub fn finish_update(has_reason: bool, has_expiry: bool) -> String {
    let mut sets = vec![
        "#phase = :p",
        "#finished_at = :now",
        "#updated_at = :at",
        "#ttl = :ttl",
    ];
    let mut removes = vec![];
    if has_reason {
        sets.push("#reason = :why");
    } else {
        removes.push("#reason");
    }
    if has_expiry {
        sets.push("#expires_at = :exp");
    } else {
        removes.push("#expires_at");
    }
    let mut expr = format!("SET {}", sets.join(", "));
    if !removes.is_empty() {
        expr.push_str(&format!(" REMOVE {}", removes.join(", ")));
    }
    expr
}

/// Record how job `job` (which started at `started_at`, epoch seconds) ended.
#[allow(clippy::too_many_arguments)]
pub async fn mark_finished(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    job: &str,
    started_at: i64,
    how: Finished<'_>,
) {
    let now = chrono::Utc::now().timestamp();
    let (phase, reason, expires_at) = match how {
        Finished::Done => (PHASE_DONE, None, None),
        Finished::Retrying {
            reason,
            retry_in_secs,
            budget_secs,
        } => (
            PHASE_RETRYING,
            Some(reason),
            Some(now + retry_in_secs.max(0) + budget_secs.max(60) + QUEUED_FOR_SECS),
        ),
        Finished::Failed { reason } => (PHASE_FAILED, Some(reason), None),
        Finished::Skipped { reason } => (PHASE_SKIPPED, Some(reason), None),
    };
    let update = finish_update(reason.is_some(), expires_at.is_some());
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(owner, repo, pr)))
        .update_expression(&update)
        .condition_expression(FINISH_CONDITION)
        .expression_attribute_values(":p", s(phase))
        .expression_attribute_values(":r", s(PHASE_REVIEWING))
        .expression_attribute_values(":q", s(PHASE_QUEUED))
        .expression_attribute_values(":j", s(job))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":started", n(started_at))
        .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + RECORD_TTL_SECS));
    if let Some(why) = reason {
        req = req.expression_attribute_values(":why", s(crate::truncate_str(why, 500)));
    }
    if let Some(exp) = expires_at {
        req = req.expression_attribute_values(":exp", n(exp));
    }
    let req = named(req, &format!("{update} {FINISH_CONDITION}"));
    log_unless_condition(req.send().await.err(), pr, phase);
}

/// A condition miss is the expected "another job owns it"; anything else is
/// logged and dropped (the record is a view, never a source of truth).
fn log_unless_condition<E, R>(
    err: Option<aws_sdk_dynamodb::error::SdkError<E, R>>,
    pr: u64,
    phase: &str,
) where
    E: aws_sdk_dynamodb::error::ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    let Some(e) = err else { return };
    if e.as_service_error().and_then(|se| se.code()) == Some("ConditionalCheckFailedException") {
        return;
    }
    tracing::warn!(pr, phase, error = ?e, "review status: could not record");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_gate as g;

    #[test]
    fn record_key_is_zero_padded() {
        assert_eq!(record_sk("acme", "web", 7), "REVIEWLIVE#acme/web#000007");
    }

    #[test]
    fn in_flight_phases_stall_after_they_expire() {
        assert_eq!(
            effective_phase(PHASE_REVIEWING, Some(100), 99),
            PHASE_REVIEWING
        );
        assert_eq!(
            effective_phase(PHASE_REVIEWING, Some(100), 101),
            PHASE_STALLED
        );
        assert_eq!(effective_phase(PHASE_QUEUED, Some(100), 101), PHASE_STALLED);
        assert_eq!(
            effective_phase(PHASE_RETRYING, Some(100), 101),
            PHASE_STALLED
        );
        // Finished phases never stall, and a missing expiry keeps the phase.
        assert_eq!(effective_phase(PHASE_DONE, Some(100), 101), PHASE_DONE);
        assert_eq!(effective_phase(PHASE_FAILED, Some(100), 101), PHASE_FAILED);
        assert_eq!(effective_phase(PHASE_REVIEWING, None, 101), PHASE_REVIEWING);
    }

    #[test]
    fn a_review_in_flight_wins_over_the_gate() {
        for gate in [
            g::STATE_WAITING_CI,
            g::STATE_MERGED,
            g::STATE_HEAD_MOVED,
            "",
        ] {
            assert_eq!(pr_status(PHASE_REVIEWING, gate).status, "reviewing");
            assert_eq!(pr_status(PHASE_QUEUED, gate).status, "queued");
            assert_eq!(pr_status(PHASE_RETRYING, gate).group, GROUP_ACTIVE);
        }
    }

    #[test]
    fn gate_states_map_to_waiting_or_done() {
        let cases = [
            (g::STATE_WAITING_CI, "waiting_ci", GROUP_WAITING),
            (g::STATE_BLOCKED_CI, "ci_failing", GROUP_WAITING),
            (g::STATE_WAITING_APPROVAL, "waiting_people", GROUP_WAITING),
            (g::STATE_DECLINED, "merge_blocked", GROUP_WAITING),
            (g::STATE_PAUSED, "paused", GROUP_WAITING),
            (g::STATE_STOPPED, "stopped", GROUP_WAITING),
            (g::STATE_MERGED, "merged", GROUP_DONE),
            (g::STATE_CLOSED, "closed", GROUP_DONE),
            (g::STATE_HEAD_MOVED, "new_commits", GROUP_DONE),
        ];
        for (gate, status, group) in cases {
            assert_eq!(
                pr_status(PHASE_DONE, gate),
                PrStatus { status, group },
                "{gate}"
            );
        }
    }

    #[test]
    fn without_a_gate_the_last_job_decides() {
        assert_eq!(pr_status(PHASE_DONE, "").status, "reviewed");
        assert_eq!(pr_status("", "").status, "reviewed");
        assert_eq!(pr_status(PHASE_FAILED, "").status, "failed");
        assert_eq!(pr_status(PHASE_STALLED, "").status, "stalled");
        // A push skipped on purpose shows as skipped, and never stalls.
        assert_eq!(
            pr_status(PHASE_SKIPPED, ""),
            PrStatus {
                status: "skipped",
                group: GROUP_DONE
            }
        );
        assert_eq!(
            effective_phase(PHASE_SKIPPED, Some(100), 101),
            PHASE_SKIPPED
        );
        assert!(!in_flight(PHASE_SKIPPED));
        // The gate's state still outranks it once a gate is armed.
        assert_eq!(
            pr_status(PHASE_SKIPPED, g::STATE_WAITING_CI).status,
            "waiting_ci"
        );
        // A gate that merged outranks an older failure.
        assert_eq!(pr_status(PHASE_FAILED, g::STATE_MERGED).status, "merged");
    }

    #[test]
    fn finish_condition_protects_other_jobs() {
        for name in [":r", ":j", ":now", ":q", ":started"] {
            assert!(FINISH_CONDITION.contains(name), "{name}");
        }
        assert_eq!(
            placeholders(FINISH_CONDITION),
            vec!["phase", "queued_at", "expires_at", "job"]
        );
    }

    #[test]
    fn updates_only_name_what_they_use() {
        assert!(reviewing_update(true).contains("#pr_title = :title"));
        assert!(!reviewing_update(false).contains("pr_title"));
        assert_eq!(
            finish_update(false, false),
            "SET #phase = :p, #finished_at = :now, #updated_at = :at, #ttl = :ttl \
             REMOVE #reason, #expires_at"
        );
        let retry = finish_update(true, true);
        assert!(retry.contains("#reason = :why") && retry.contains("#expires_at = :exp"));
        assert!(!retry.contains("REMOVE"));
        // `#queued_at` must not also bind a shorter name; unknown names are
        // never bound.
        assert_eq!(placeholders("#queued_at #nope"), vec!["queued_at"]);
    }

    #[test]
    fn every_placeholder_in_an_expression_is_bound() {
        for expr in [
            reviewing_update(true),
            reviewing_update(false),
            finish_update(true, true),
            finish_update(false, false),
            format!("{QUEUED_UPDATE} {QUEUED_CONDITION}"),
            FINISH_CONDITION.to_string(),
        ] {
            let bound = placeholders(&expr);
            for word in expr.split(|c: char| !(c == '#' || c == '_' || c.is_ascii_alphanumeric())) {
                if let Some(name) = word.strip_prefix('#') {
                    assert!(bound.contains(&name), "{name} unbound in {expr}");
                }
            }
        }
    }
}
