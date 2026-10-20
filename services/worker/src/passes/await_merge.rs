//! Armed auto-merge gate. Merges a PR only once BOTH keys are present —
//! CoderHelm's own latest verdict for this head is APPROVE (verified here, not
//! assumed, and withdrawn when that review is dismissed) and a person approved
//! (when the repo requires it, always on CoderHelm's own PRs) — AND every CI
//! check is green. If the repo configures an (operator-set, never hardcoded)
//! deploy label, it is added once per commit after both keys are in and the
//! gate waits for that deploy's CI. It NEVER merges on failing, still-running
//! or unreadable CI. Bound to the reviewed head: a later commit stops the gate
//! (the review of the new commit re-arms it).
//!
//! Event-driven, with bounded polling only as a fallback:
//! - Waiting on people never polls. An approval, a dismissal or a new review
//!   on the PR re-arms the gate (the gateway reads the gate record).
//! - Waiting on CI polls for up to `WATCH_WINDOW_SECS`, and the gateway also
//!   re-arms the gate whenever a check suite on the PR's head completes, on
//!   any branch. A slow deploy, or a flaky job re-run to green, still merges.
//! - An error (GitHub, DynamoDB) is retried on the requeue schedule shared
//!   with model errors (`common::model_retry`), never ends the gate, and never
//!   counts as green.
//!
//! State lives in one record per PR (`common::merge_gate`), and the PR shows
//! ONE status comment, edited in place, saying exactly what the gate waits on.
//! Each arming starts a new chain; a tick from an older chain stops, so a PR
//! never has two polling loops.

use crate::clients::github::{GitHubClient, MergeOutcome};
use crate::models::AwaitMergeMessage;
use crate::passes::review_actions::{
    fetch_approvals, post_merge_actions, should_merge, Approvals, OnApproveConfig,
};
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::merge_gate::{
    record_sk as gate_sk, STATE_BLOCKED_CI, STATE_CLOSED, STATE_DECLINED, STATE_HEAD_MOVED,
    STATE_MERGED, STATE_PAUSED, STATE_WAITING_APPROVAL, STATE_WAITING_CI,
};
use std::collections::HashMap;
use tracing::{info, warn};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Delay between polls while CI runs.
const POLL_DELAY_SECS: i32 = 90;
/// Delay before the first evaluation after CoderHelm approves, so the checks
/// the reviewed push triggered have registered.
const SETTLE_DELAY_SECS: i32 = 45;
/// How long one arming keeps polling CI before it pauses. A completed check
/// suite re-arms a paused gate, so this bounds cost, not correctness.
const WATCH_WINDOW_SECS: i64 = 60 * 60;
/// Hard ceiling on CI polls per chain.
const MAX_POLLS: u32 = 60;
/// Errors in a row retried before the gate pauses (≈ a day at the longest
/// requeue delay). A pause still wakes on the next CI or review event.
const MAX_ERROR_RETRIES: u32 = 100;
/// After the gate adds deploy labels, how long a "green" CI result is not
/// trusted unless a check has started since the labels were added.
const LABEL_CI_GRACE_SECS: i64 = 180;
/// How long a gate record is kept after its last arming.
const RECORD_TTL_SECS: u64 = 30 * 86_400;

/// Hidden marker that identifies the gate's status comment.
pub const STATUS_MARKER: &str = "<!-- coderhelm:merge-status -->";

/// Starts the small print under the checklist (the commit and branch). Notes
/// are added above it.
const FOOTER_START: &str = "\n<sub>";

// ─── CI ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ci {
    Green,
    Pending(Vec<String>),
    Failing(Vec<String>),
}

/// Pure summary of a ref's checks.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct CheckSummary {
    pub pending: Vec<String>,
    pub failing: Vec<String>,
    /// Latest `started_at` across the kept runs (RFC 3339), if any.
    pub latest_start: Option<String>,
}

impl CheckSummary {
    /// The verdict: anything still running is not final yet, so a failure is
    /// only reported once every check has finished.
    pub(crate) fn ci(&self) -> Ci {
        if !self.pending.is_empty() {
            Ci::Pending(self.pending.clone())
        } else if !self.failing.is_empty() {
            Ci::Failing(self.failing.clone())
        } else {
            Ci::Green
        }
    }

    /// `latest_start` as epoch seconds.
    pub(crate) fn latest_start_secs(&self) -> Option<i64> {
        self.latest_start
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.timestamp())
    }
}

/// Pure classification of a ref's GitHub check-runs.
/// GitHub returns EVERY check-run for a ref, including superseded ones: a re-run or
/// a concurrency-cancel leaves the stale earlier run behind, so one check name can
/// appear several times (e.g. a `cancelled` plus a later `success`). We keep only
/// the LATEST run per name so a stale duplicate can't veto the current result (and
/// a re-run of a flaky job that passed replaces its failure), and `cancelled` is
/// treated as NEUTRAL — GitHub cancels superseded / concurrency-grouped runs and
/// fail-fast matrix siblings as a matter of course, and a genuine failure always
/// surfaces as `failure`/`timed_out` on its own check.
pub(crate) fn classify_check_runs(runs: &[serde_json::Value]) -> CheckSummary {
    let mut latest: HashMap<&str, &serde_json::Value> = HashMap::new();
    for r in runs {
        let name = r["name"].as_str().unwrap_or("check");
        let ts = r["started_at"].as_str().unwrap_or("");
        let newer = latest
            .get(name)
            .and_then(|e| e["started_at"].as_str())
            .map(|prev| prev <= ts)
            .unwrap_or(true);
        if newer {
            latest.insert(name, r);
        }
    }
    let mut out = CheckSummary::default();
    for (name, r) in &latest {
        if let Some(ts) = r["started_at"].as_str() {
            if out.latest_start.as_deref().is_none_or(|cur| cur < ts) {
                out.latest_start = Some(ts.to_string());
            }
        }
        if r["status"].as_str() != Some("completed") {
            out.pending.push(name.to_string());
            continue;
        }
        if matches!(
            r["conclusion"].as_str().unwrap_or(""),
            "failure" | "timed_out" | "startup_failure" | "action_required"
        ) {
            out.failing.push(name.to_string());
        }
    }
    out.pending.sort();
    out.failing.sort();
    out
}

/// Legacy commit statuses (external CI that posts a status, e.g. a staging
/// apply) folded into the summary, one entry per context.
fn add_commit_statuses(summary: &mut CheckSummary, status: &serde_json::Value) {
    for st in status["statuses"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        let ctx = st["context"]
            .as_str()
            .unwrap_or("commit status")
            .to_string();
        match st["state"].as_str().unwrap_or("") {
            "failure" | "error" => summary.failing.push(ctx),
            "pending" => summary.pending.push(ctx),
            _ => {}
        }
    }
}

/// What a quarantine check does to a failing verdict: only quarantined tests
/// failed → green; a test report still being read → pending (keep waiting).
fn with_quarantine(ci: Ci, gate: &crate::passes::quarantine::Gate) -> Ci {
    use crate::passes::quarantine::Gate;
    match (ci, gate) {
        (Ci::Failing(_), Gate::OnlyQuarantined(_)) => Ci::Green,
        (Ci::Failing(names), Gate::Waiting) => Ci::Pending(names),
        (ci, _) => ci,
    }
}

/// Every check-run and commit status on `sha`. No checks at all is green
/// (nothing to gate on). An API error is an error — never a green light.
async fn ci_summary(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<CheckSummary, BoxError> {
    let runs = github.list_check_runs_for_ref(owner, repo, sha).await?;
    let runs = runs["check_runs"].as_array().cloned().unwrap_or_default();
    let mut summary = classify_check_runs(&runs);
    let status = github.get_commit_status(owner, repo, sha).await?;
    add_commit_statuses(&mut summary, &status);
    summary.pending.sort();
    summary.failing.sort();
    Ok(summary)
}

/// Pure: may a green result be trusted yet, given when the gate added deploy
/// labels? Label-triggered workflows take a moment to register their checks;
/// until one has started (or the grace period passed) green means "not yet".
pub(crate) fn label_ci_settled(
    labels_added_at: Option<i64>,
    latest_start: Option<i64>,
    now: i64,
) -> bool {
    match labels_added_at {
        None => true,
        Some(added) => {
            now - added >= LABEL_CI_GRACE_SECS || latest_start.is_some_and(|s| s >= added)
        }
    }
}

// ─── The decision (pure) ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mark {
    Done,
    Waiting,
    Blocked,
}

impl Mark {
    fn icon(self) -> &'static str {
        match self {
            Mark::Done => "✅",
            Mark::Waiting => "⏳",
            Mark::Blocked => "🔴",
        }
    }
}

/// Everything the gate knows about the PR at one evaluation.
#[derive(Debug, Clone)]
pub(crate) struct GateView {
    pub draft: bool,
    pub require_human: bool,
    /// CoderHelm's own latest verdict recorded for this head.
    pub bot_verdict: Option<String>,
    pub approvals: Approvals,
    /// Deploy labels the repo wants on a PR cleared to merge.
    pub want_labels: Vec<String>,
    pub present_labels: Vec<String>,
    /// The gate already added the deploy labels for this head, and when.
    pub labels_added_at: Option<i64>,
    /// CI on the head, quarantine applied. None until both keys are in (CI is
    /// not read for a PR that can't merge yet).
    pub ci: Option<CheckSummary>,
    pub now: i64,
}

/// What the gate does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    /// Waiting on a person: record it and stop; a review event re-arms.
    WaitForPeople,
    /// Both keys are in; read CI and decide again.
    ReadCi,
    /// Add these deploy labels (once for this head), then poll.
    AddLabels(Vec<String>),
    /// CI still running: poll (bounded); a completed check suite also re-arms.
    PollCi,
    /// CI failed: stop; a completed check suite (a re-run) or a new commit
    /// re-arms.
    BlockedOnCi,
    /// Everything is green: merge.
    Merge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GateDecision {
    pub state: &'static str,
    pub lines: Vec<(Mark, String)>,
    pub note: Option<String>,
    pub action: Action,
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// Pure: the gate's decision from what it knows. The only place the two
/// keys, the deploy labels and CI are weighed, so the evals drive exactly
/// what production runs.
pub(crate) fn decide_gate(v: &GateView) -> GateDecision {
    let bot_dismissed = v.approvals.bot_dismissed_at_head;
    let bot_approved = v.bot_verdict.as_deref() == Some("APPROVE") && !bot_dismissed;
    let human_present = v.approvals.human_key();

    let mut lines: Vec<(Mark, String)> = Vec::new();
    lines.push(match (v.bot_verdict.as_deref(), bot_dismissed) {
        (Some("APPROVE"), false) => (Mark::Done, "CoderHelm approved".to_string()),
        (Some("APPROVE"), true) => (
            Mark::Blocked,
            "CoderHelm's approval was dismissed. Comment `@coderhelm re-review` to review \
             again"
                .to_string(),
        ),
        (Some(_), _) => (Mark::Blocked, "CoderHelm requested changes".to_string()),
        (None, _) => (Mark::Waiting, "Waiting for CoderHelm's review".to_string()),
    });
    if v.require_human {
        lines.push(if human_present {
            (
                Mark::Done,
                format!("Approved by {}", mention_list(&v.approvals.approved_by)),
            )
        } else if !v.approvals.blocking_by.is_empty() {
            (
                Mark::Blocked,
                format!(
                    "Changes requested by {}",
                    mention_list(&v.approvals.blocking_by)
                ),
            )
        } else {
            (Mark::Waiting, "Waiting for a person's approval".to_string())
        });
    }
    if v.draft {
        lines.push((
            Mark::Waiting,
            "Waiting for the PR to leave draft".to_string(),
        ));
    }

    // Gate 1: both keys (and not a draft). Waiting on people never polls.
    if v.draft || !should_merge(true, v.require_human, bot_approved, human_present) {
        return GateDecision {
            state: STATE_WAITING_APPROVAL,
            lines,
            note: None,
            action: Action::WaitForPeople,
        };
    }

    // Gate 2: deploy labels, added once per head.
    let joined = v
        .want_labels
        .iter()
        .map(|l| format!("`{l}`"))
        .collect::<Vec<_>>()
        .join(", ");
    if !v.want_labels.is_empty() {
        let missing: Vec<String> = v
            .want_labels
            .iter()
            .filter(|l| !v.present_labels.iter().any(|p| p.eq_ignore_ascii_case(l)))
            .cloned()
            .collect();
        if !missing.is_empty() && v.labels_added_at.is_none() {
            lines.push((
                Mark::Waiting,
                format!("Adding {joined}, then waiting for its CI to start"),
            ));
            return GateDecision {
                state: STATE_WAITING_CI,
                lines,
                note: None,
                action: Action::AddLabels(missing),
            };
        }
        // Present, or added earlier for this head and since removed by the
        // repo's own workflow: never re-added in a loop.
        lines.push((Mark::Done, format!("Deploy label {joined} applied")));
    }

    // Gate 3: CI.
    let Some(summary) = &v.ci else {
        return GateDecision {
            state: STATE_WAITING_CI,
            lines,
            note: None,
            action: Action::ReadCi,
        };
    };
    let ci = match summary.ci() {
        Ci::Green if !label_ci_settled(v.labels_added_at, summary.latest_start_secs(), v.now) => {
            Ci::Pending(vec!["label-triggered CI (not started yet)".to_string()])
        }
        other => other,
    };
    match ci {
        Ci::Pending(names) => {
            lines.push((Mark::Waiting, format!("CI running: {}", code_list(&names))));
            GateDecision {
                state: STATE_WAITING_CI,
                lines,
                note: None,
                action: Action::PollCi,
            }
        }
        Ci::Failing(names) => {
            lines.push((Mark::Blocked, format!("CI failing: {}", code_list(&names))));
            GateDecision {
                state: STATE_BLOCKED_CI,
                lines,
                note: Some(
                    "Not merging. Re-run the failed checks or push a fix; this picks up again \
                     when CI finishes."
                        .to_string(),
                ),
                action: Action::BlockedOnCi,
            }
        }
        Ci::Green => {
            lines.push((Mark::Done, "CI green".to_string()));
            GateDecision {
                state: STATE_WAITING_CI,
                lines,
                note: None,
                action: Action::Merge,
            }
        }
    }
}

fn mention_list(logins: &[String]) -> String {
    logins
        .iter()
        .map(|l| format!("@{l}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn code_list(names: &[String]) -> String {
    const SHOW: usize = 8;
    let mut out = names
        .iter()
        .take(SHOW)
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ");
    if names.len() > SHOW {
        out.push_str(&format!(" and {} more", names.len() - SHOW));
    }
    out
}

/// Pure: the status comment body: one bold line with the overall state, the
/// checklist, any note, then the commit and branch in small print. No
/// timestamps, so an unchanged state renders byte-identically and the comment
/// is not rewritten.
pub(crate) fn render_status(
    head: &str,
    base: &str,
    lines: &[(Mark, String)],
    note: Option<&str>,
) -> String {
    let state = if lines.iter().any(|(m, _)| *m == Mark::Blocked) {
        " · blocked"
    } else if lines.iter().any(|(m, _)| *m == Mark::Waiting) {
        " · waiting"
    } else {
        ""
    };
    let mut body = format!("{STATUS_MARKER}\n**Auto-merge**{state}\n\n");
    for (mark, line) in lines {
        body.push_str(&format!("- {} {line}\n", mark.icon()));
    }
    if let Some(note) = note {
        body.push_str(&format!("\n{note}\n"));
    }
    body.push_str(&format!(
        "{FOOTER_START}Commit {} into {base}. Merges on its own once every item is done.</sub>",
        short(head)
    ));
    body
}

/// Pure: the last status body with `note` added above the small print (used
/// when the gate pauses, so the checklist of what it was waiting on stays
/// visible).
fn with_note(last_body: &str, head: &str, base: &str, note: &str) -> String {
    match last_body.rfind(FOOTER_START) {
        Some(i) if last_body.starts_with(STATUS_MARKER) => {
            let (main, footer) = last_body.split_at(i);
            format!("{}\n\n{note}\n{footer}", main.trim_end())
        }
        _ => render_status(head, base, &[], Some(note)),
    }
}

/// The delay before error retry `n` (0-based): the shared requeue schedule,
/// then its longest step. None once the gate should pause instead.
pub(crate) fn error_retry_delay(n: u32) -> Option<i32> {
    if n >= MAX_ERROR_RETRIES {
        return None;
    }
    let longest = *common::model_retry::REQUEUE_DELAYS_SECS
        .last()
        .unwrap_or(&900);
    Some(
        common::model_retry::requeue_delay(n)
            .unwrap_or(longest)
            .min(900) as i32,
    )
}

// ─── Gate record ────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct GateRecord {
    chain_id: String,
    armed_at: i64,
    status_comment_id: Option<u64>,
    status_hash: String,
    status_body: String,
    labels_added_head: String,
    labels_added_at: Option<i64>,
}

fn num(item: &HashMap<String, AttributeValue>, k: &str) -> Option<i64> {
    item.get(k)?.as_n().ok()?.parse().ok()
}

fn text(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn record_key(msg: &AwaitMergeMessage) -> String {
    gate_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number)
}

async fn load_record(state: &WorkerState, msg: &AwaitMergeMessage) -> Result<GateRecord, BoxError> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&record_key(msg)))
        .consistent_read(true)
        .send()
        .await?;
    let Some(item) = out.item() else {
        return Ok(GateRecord::default());
    };
    Ok(GateRecord {
        chain_id: text(item, "chain_id"),
        armed_at: num(item, "armed_at").unwrap_or(0),
        status_comment_id: num(item, "status_comment_id").map(|n| n as u64),
        status_hash: text(item, "status_hash"),
        status_body: text(item, "status_body"),
        labels_added_head: text(item, "labels_added_head"),
        labels_added_at: num(item, "labels_added_at"),
    })
}

/// Start a new chain for this arming: it becomes the only current chain.
/// Keeps the status comment and the label bookkeeping.
async fn start_chain(
    state: &WorkerState,
    msg: &AwaitMergeMessage,
    now: i64,
) -> Result<(), BoxError> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&record_key(msg)))
        .update_expression(
            "SET chain_id = :c, head_sha = :h, base_branch = :b, self_authored = :sa, \
             installation_id = :i, armed_at = :now, #st = :st, #ttl = :ttl",
        )
        .expression_attribute_names("#st", "state")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":c", attr_s(&msg.chain_id))
        .expression_attribute_values(":h", attr_s(&msg.head_sha))
        .expression_attribute_values(":b", attr_s(&msg.base_branch))
        .expression_attribute_values(":sa", AttributeValue::Bool(msg.self_authored))
        .expression_attribute_values(":i", attr_n(msg.installation_id))
        .expression_attribute_values(":now", attr_n(now))
        .expression_attribute_values(":st", attr_s(STATE_WAITING_CI))
        .expression_attribute_values(":ttl", attr_n(now as u64 + RECORD_TTL_SECS))
        .send()
        .await?;
    Ok(())
}

/// Record the gate's state, only while this chain is still the current one.
async fn set_state(state: &WorkerState, msg: &AwaitMergeMessage, gate_state: &str, detail: &str) {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&record_key(msg)))
        .update_expression("SET #st = :st, detail = :d, updated_at = :t")
        .condition_expression("chain_id = :c")
        .expression_attribute_names("#st", "state")
        .expression_attribute_values(":st", attr_s(gate_state))
        .expression_attribute_values(":d", attr_s(common::truncate_str(detail, 1000)))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":c", attr_s(&msg.chain_id))
        .send()
        .await;
    if let Err(e) = res {
        let superseded = e
            .as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false);
        if !superseded {
            warn!(pr = msg.pr_number, error = %e, "await-merge: could not record gate state");
        }
    }
}

async fn record_labels_added(
    state: &WorkerState,
    msg: &AwaitMergeMessage,
    now: i64,
) -> Result<(), BoxError> {
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&record_key(msg)))
        .update_expression("SET labels_added_head = :h, labels_added_at = :t")
        .expression_attribute_values(":h", attr_s(&msg.head_sha))
        .expression_attribute_values(":t", attr_n(now))
        .send()
        .await?;
    Ok(())
}

// ─── Status comment ─────────────────────────────────────────────────────────

/// Create or update the PR's single status comment. Skips the write when the
/// rendered body is unchanged. Finds an existing status comment on the PR
/// when the record has none, so a lost record never doubles it.
async fn publish_status(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &AwaitMergeMessage,
    record: &mut GateRecord,
    body: &str,
) {
    let hash = common::content_hash(body);
    if record.status_hash == hash {
        return;
    }
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    if record.status_comment_id.is_none() {
        if let Ok(comments) = github.list_issue_comments(owner, repo, msg.pr_number).await {
            record.status_comment_id = comments
                .iter()
                .rev()
                .find(|c| {
                    c["body"]
                        .as_str()
                        .is_some_and(|b| b.starts_with(STATUS_MARKER))
                })
                .and_then(|c| c["id"].as_u64());
        }
    }
    let mut comment_id = None;
    if let Some(id) = record.status_comment_id {
        match github.edit_issue_comment(owner, repo, id, body).await {
            Ok(_) => comment_id = Some(id),
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "await-merge: status comment edit failed — posting a new one")
            }
        }
    }
    if comment_id.is_none() {
        // Two chains can briefly overlap; only one may create the comment.
        let create_sk = format!("MERGESTATUSCREATE#{owner}/{repo}#{:06}", msg.pr_number);
        let claim = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &create_sk,
            60,
        )
        .await;
        if !claim.won_or_failed_open() {
            return;
        }
        match github
            .create_issue_comment(owner, repo, msg.pr_number, body)
            .await
        {
            Ok(v) => comment_id = v["id"].as_u64(),
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "await-merge: status comment post failed");
                return;
            }
        }
    }
    let Some(id) = comment_id else { return };
    record.status_comment_id = Some(id);
    record.status_hash = hash.clone();
    record.status_body = body.to_string();
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&gate_sk(owner, repo, msg.pr_number)))
        .update_expression("SET status_comment_id = :id, status_hash = :h, status_body = :b")
        .expression_attribute_values(":id", attr_n(id))
        .expression_attribute_values(":h", attr_s(&hash))
        .expression_attribute_values(":b", attr_s(body))
        .send()
        .await
    {
        warn!(pr = msg.pr_number, error = %e, "await-merge: could not record the status comment");
    }
}

// ─── Bot key ────────────────────────────────────────────────────────────────

/// CoderHelm's own latest verdict for this head, from the persisted review
/// records (uniform across people's PRs, where the bot posts a real review,
/// and its own PRs, where GitHub forbids self-approval so the verdict lives
/// only in the record). Records are keyed `REVIEW#{owner}/{repo}#{pr:06}#{rfc3339}`,
/// so a descending query yields newest-first. None = no verdict for this head
/// yet. A failed read is an error (the gate retries; it never merges on it).
async fn bot_verdict_at_head(
    state: &WorkerState,
    msg: &AwaitMergeMessage,
) -> Result<Option<String>, BoxError> {
    let prefix = format!(
        "REVIEW#{}/{}#{:0>6}#",
        msg.repo_owner, msg.repo_name, msg.pr_number
    );
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    // Page through the newest records until one for this head is found; the
    // records of other heads and question answers are skipped.
    for _ in 0..5 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
            .expression_attribute_values(":pk", attr_s(&msg.team_id))
            .expression_attribute_values(":sk", attr_s(&prefix))
            .scan_index_forward(false)
            .limit(25)
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        for it in resp.items() {
            let verdict = text(it, "verdict");
            if verdict != "APPROVE" && verdict != "REQUEST_CHANGES" {
                continue;
            }
            if text(it, "head_sha") == msg.head_sha {
                return Ok(Some(verdict));
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(None)
}

// ─── Gate ───────────────────────────────────────────────────────────────────

enum Next {
    /// Evaluate again after a delay.
    Poll,
    /// Nothing to poll for; an event re-arms the gate if needed.
    Stop,
}

pub async fn run(state: &WorkerState, mut msg: AwaitMergeMessage) -> Result<(), BoxError> {
    let now = chrono::Utc::now().timestamp();
    let mut record = if msg.chain_id.is_empty() {
        msg.chain_id = ulid::Ulid::new().to_string();
        msg.attempts = 0;
        if let Err(e) = start_chain(state, &msg, now).await {
            // Without a chain the gate can't guard against a second loop;
            // retry the arming instead of dropping it.
            warn!(pr = msg.pr_number, error = %e, "await-merge: could not start chain — retrying");
            msg.chain_id.clear();
            retry_after_error(state, &msg, None, &e.to_string()).await;
            return Ok(());
        }
        match load_record(state, &msg).await {
            Ok(r) => r,
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "await-merge: could not read gate record — retrying");
                retry_after_error(state, &msg, None, &e.to_string()).await;
                return Ok(());
            }
        }
    } else {
        match load_record(state, &msg).await {
            Ok(r) if r.chain_id != msg.chain_id => {
                info!(
                    pr = msg.pr_number,
                    "await-merge: superseded by a newer arming — stopping"
                );
                return Ok(());
            }
            Ok(r) => r,
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "await-merge: could not read gate record — retrying");
                retry_after_error(state, &msg, None, &e.to_string()).await;
                return Ok(());
            }
        }
    };
    if record.armed_at == 0 {
        record.armed_at = now;
    }

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    match evaluate(state, &github, &msg, &mut record, now).await {
        Ok(Next::Stop) => {}
        Ok(Next::Poll) => {
            msg.error_retries = 0;
            if !poll_again(state, &msg, now, record.armed_at).await {
                pause(state, &github, &msg, &mut record, None).await;
            }
        }
        Err(e) => {
            warn!(pr = msg.pr_number, attempt = msg.error_retries, error = %e, "await-merge: evaluation failed — will retry");
            retry_after_error(state, &msg, Some((&github, &mut record)), &e.to_string()).await;
        }
    }
    Ok(())
}

/// Requeue after an error on the shared requeue schedule. Errors never end
/// the gate and never count as green; after `MAX_ERROR_RETRIES` in a row it
/// pauses (an event re-arms it) and says why on the PR.
async fn retry_after_error(
    state: &WorkerState,
    msg: &AwaitMergeMessage,
    pause_with: Option<(&GitHubClient, &mut GateRecord)>,
    error: &str,
) {
    if let Some(delay) = error_retry_delay(msg.error_retries) {
        let mut next = msg.clone();
        next.error_retries += 1;
        if send(state, &next, next.attempts, delay).await {
            return;
        }
    }
    match pause_with {
        Some((github, record)) => pause(state, github, msg, record, Some(error)).await,
        None => warn!(
            pr = msg.pr_number,
            error, "await-merge: could not requeue — the next PR event re-arms the gate"
        ),
    }
}

/// Queue the next CI poll unless the window or poll budget is spent. Returns
/// false when the gate should pause instead.
async fn poll_again(state: &WorkerState, msg: &AwaitMergeMessage, now: i64, armed_at: i64) -> bool {
    if msg.attempts >= MAX_POLLS || (armed_at > 0 && now - armed_at >= WATCH_WINDOW_SECS) {
        return false;
    }
    send(state, msg, msg.attempts + 1, POLL_DELAY_SECS).await
}

/// Stop polling and say so on the PR. A completed check suite, a review event
/// or a new commit re-arms the gate.
async fn pause(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &AwaitMergeMessage,
    record: &mut GateRecord,
    last_error: Option<&str>,
) {
    info!(
        pr = msg.pr_number,
        attempts = msg.attempts,
        "await-merge: pausing — waiting for the next CI or review event"
    );
    set_state(
        state,
        msg,
        STATE_PAUSED,
        last_error.unwrap_or("poll window elapsed"),
    )
    .await;
    let mut note = "Paused. This picks up again on its own when CI finishes on this commit, \
                    or on a new review or commit."
        .to_string();
    if let Some(err) = last_error {
        note.push_str(&format!(
            "\n\nLast check failed with: `{}`",
            common::truncate_str(err, 300).replace('`', "'")
        ));
    }
    let body = with_note(&record.status_body, &msg.head_sha, &msg.base_branch, &note);
    publish_status(state, github, msg, record, &body).await;
}

async fn evaluate(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &AwaitMergeMessage,
    record: &mut GateRecord,
    now: i64,
) -> Result<Next, BoxError> {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let cfg = OnApproveConfig::try_load(state, &msg.team_id, owner, repo).await?;
    if !cfg.auto_merge {
        info!(
            pr = msg.pr_number,
            "auto_merge disabled — dropping await-merge"
        );
        return Ok(Next::Stop);
    }

    let pr = github.get_pull_request(owner, repo, msg.pr_number).await?;
    let present_labels: Vec<String> = pr["labels"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| l["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    // People's PRs opted into review get the status comment; CoderHelm's own
    // PRs always do.
    let watched = msg.self_authored
        || present_labels
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&cfg.review_label));

    if pr["state"].as_str() != Some("open") {
        info!(pr = msg.pr_number, "PR not open — stop await-merge");
        set_state(state, msg, STATE_CLOSED, "").await;
        if record.status_comment_id.is_some() || !record.status_body.is_empty() {
            let note = if pr["merged"].as_bool() == Some(true) {
                "Merged by hand. Nothing left to do."
            } else {
                "PR closed. Nothing left to do."
            };
            let body = render_status(&msg.head_sha, &msg.base_branch, &[], Some(note));
            publish_status(state, github, msg, record, &body).await;
        }
        return Ok(Next::Stop);
    }

    // Bound to the reviewed head: a new commit invalidates the review.
    let cur_head = pr["head"]["sha"].as_str().unwrap_or("");
    if cur_head != msg.head_sha {
        info!(
            pr = msg.pr_number,
            armed = %msg.head_sha,
            current = cur_head,
            "head moved — stop await-merge (the review of the new commit re-arms)"
        );
        set_state(state, msg, STATE_HEAD_MOVED, cur_head).await;
        if watched && !record.status_body.is_empty() {
            let note = format!(
                "New commit {}. Waiting for CoderHelm to review it before merging.",
                short(cur_head)
            );
            let body = render_status(&msg.head_sha, &msg.base_branch, &[], Some(&note));
            publish_status(state, github, msg, record, &body).await;
        }
        return Ok(Next::Stop);
    }

    let approvals = fetch_approvals(github, owner, repo, msg.pr_number, &msg.head_sha).await?;
    let bot_verdict = bot_verdict_at_head(state, msg).await?;
    let mut view = GateView {
        draft: pr["draft"].as_bool().unwrap_or(false),
        require_human: cfg.require_human_approval || msg.self_authored,
        bot_verdict,
        approvals,
        want_labels: cfg
            .deploy_label
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        present_labels,
        labels_added_at: (record.labels_added_head == msg.head_sha)
            .then_some(record.labels_added_at)
            .flatten(),
        ci: None,
        now,
    };
    let mut decision = decide_gate(&view);
    let mut quarantined: Option<Vec<String>> = None;
    if decision.action == Action::ReadCi {
        let summary = ci_summary(github, owner, repo, &msg.head_sha).await?;
        let summary = match summary.ci() {
            Ci::Failing(names) => {
                let gate = crate::passes::quarantine::gate(
                    state,
                    github,
                    &msg.team_id,
                    owner,
                    repo,
                    &msg.head_sha,
                )
                .await;
                if let crate::passes::quarantine::Gate::OnlyQuarantined(tests) = &gate {
                    quarantined = Some(tests.clone());
                }
                match with_quarantine(Ci::Failing(names), &gate) {
                    Ci::Green => CheckSummary {
                        failing: vec![],
                        ..summary
                    },
                    Ci::Pending(p) => CheckSummary {
                        pending: p,
                        ..summary
                    },
                    Ci::Failing(_) => summary,
                }
            }
            _ => summary,
        };
        view.ci = Some(summary);
        decision = decide_gate(&view);
    }

    let detail = decision
        .lines
        .last()
        .map(|(_, l)| l.clone())
        .unwrap_or_default();
    let status = render_status(
        &msg.head_sha,
        &msg.base_branch,
        &decision.lines,
        decision.note.as_deref(),
    );
    match decision.action {
        Action::WaitForPeople => {
            info!(
                pr = msg.pr_number,
                "await-merge: waiting on approvals — stopping until the next review event"
            );
            set_state(state, msg, decision.state, &detail).await;
            if watched {
                publish_status(state, github, msg, record, &status).await;
            }
            Ok(Next::Stop)
        }
        Action::ReadCi => Err("the gate asked for CI after reading it".into()),
        Action::AddLabels(missing) => {
            github
                .add_labels(owner, repo, msg.pr_number, &missing)
                .await?;
            record_labels_added(state, msg, now).await?;
            record.labels_added_head = msg.head_sha.clone();
            record.labels_added_at = Some(now);
            info!(pr = msg.pr_number, labels = ?missing, "await-merge: added deploy label(s)");
            set_state(state, msg, decision.state, "deploy labels added").await;
            publish_status(state, github, msg, record, &status).await;
            Ok(Next::Poll)
        }
        Action::PollCi => {
            set_state(state, msg, decision.state, &detail).await;
            publish_status(state, github, msg, record, &status).await;
            Ok(Next::Poll)
        }
        Action::BlockedOnCi => {
            info!(pr = msg.pr_number, detail = %detail, "await-merge: CI failing — waiting for a re-run or a new commit");
            set_state(state, msg, decision.state, &detail).await;
            publish_status(state, github, msg, record, &status).await;
            Ok(Next::Stop)
        }
        Action::Merge => {
            merge(
                state,
                github,
                msg,
                record,
                &cfg,
                decision.lines,
                quarantined,
            )
            .await
        }
    }
}

async fn merge(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &AwaitMergeMessage,
    record: &mut GateRecord,
    cfg: &OnApproveConfig,
    mut lines: Vec<(Mark, String)>,
    quarantined: Option<Vec<String>>,
) -> Result<Next, BoxError> {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    match github
        .merge_pull_request(owner, repo, msg.pr_number, &msg.head_sha, &cfg.merge_method)
        .await?
    {
        MergeOutcome::Merged => {
            set_state(state, msg, STATE_MERGED, "").await;
            let mut merged_lines = post_merge_actions(
                state,
                github,
                cfg,
                &msg.team_id,
                msg.installation_id,
                owner,
                repo,
                msg.pr_number,
                &msg.head_sha,
                &msg.base_branch,
            )
            .await;
            if let Some(tests) = &quarantined {
                merged_lines.push(crate::passes::quarantine::passing_line(tests));
            }
            lines.push((Mark::Done, format!("Merged into `{}`", msg.base_branch)));
            let mut body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            if !merged_lines.is_empty() {
                // The merge report lives in the same comment: one comment per PR.
                body = with_note(
                    &body,
                    &msg.head_sha,
                    &msg.base_branch,
                    &format!("**After merging**\n\n{}", merged_lines.join("\n")),
                );
            }
            publish_status(state, github, msg, record, &body).await;
            info!(pr = msg.pr_number, "Armed auto-merge: merged");
            Ok(Next::Stop)
        }
        MergeOutcome::NotMergeable(reason) => {
            // Branch protection, a required check GitHub still counts as
            // missing, or a conflict. Keep polling: most of these clear on
            // their own, and the reason is on the PR for the ones that don't.
            warn!(pr = msg.pr_number, reason = %reason, "await-merge: GitHub declined the merge");
            lines.push((
                Mark::Waiting,
                format!(
                    "GitHub won't merge yet: {}",
                    common::truncate_str(&reason, 300)
                ),
            ));
            set_state(state, msg, STATE_DECLINED, &reason).await;
            let body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            publish_status(state, github, msg, record, &body).await;
            Ok(Next::Poll)
        }
        MergeOutcome::HeadMoved(reason) => {
            info!(pr = msg.pr_number, reason = %reason, "await-merge: head moved at merge time — stopping");
            set_state(state, msg, STATE_HEAD_MOVED, &reason).await;
            Ok(Next::Stop)
        }
    }
}

/// Arm the gate for a PR (called when CoderHelm approves; the gateway arms on
/// approvals, dismissals, new reviews and completed check suites). Every
/// arming starts a new chain. Returns false if the queue isn't configured.
#[allow(clippy::too_many_arguments)]
pub async fn arm(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    base_branch: &str,
    self_authored: bool,
) -> bool {
    let msg = AwaitMergeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        head_sha: head_sha.to_string(),
        base_branch: base_branch.to_string(),
        self_authored,
        attempts: 0,
        chain_id: String::new(),
        error_retries: 0,
    };
    send(state, &msg, 0, SETTLE_DELAY_SECS).await
}

/// Enqueue an AwaitMerge tick with `attempts` and an SQS delay.
async fn send(state: &WorkerState, msg: &AwaitMergeMessage, attempts: u32, delay: i32) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut next = msg.clone();
    next.attempts = attempts;
    let body = match serde_json::to_value(&next) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), serde_json::json!("await_merge"));
            }
            v.to_string()
        }
        Err(e) => {
            warn!(error = %e, "await-merge: serialize failed");
            return false;
        }
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay.clamp(0, 900))
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "await-merge: enqueue failed");
            false
        }
    }
}

#[cfg(test)]
mod ci_tests {
    use super::*;
    use crate::passes::quarantine::Gate;
    use serde_json::json;

    #[test]
    fn quarantined_only_failures_do_not_hold_back_the_merge() {
        let failing = || Ci::Failing(vec!["e2e".into()]);
        let only = Gate::OnlyQuarantined(vec!["a".into()]);
        assert_eq!(with_quarantine(failing(), &only), Ci::Green);
        assert!(matches!(
            with_quarantine(failing(), &Gate::Waiting),
            Ci::Pending(_)
        ));
        assert!(matches!(
            with_quarantine(failing(), &Gate::Real),
            Ci::Failing(_)
        ));
        assert!(matches!(
            with_quarantine(Ci::Pending(vec![]), &only),
            Ci::Pending(_)
        ));
    }

    fn cr(name: &str, status: &str, conclusion: &str, started_at: &str) -> serde_json::Value {
        json!({"name": name, "status": status, "conclusion": conclusion, "started_at": started_at})
    }

    #[test]
    fn cancelled_duplicate_does_not_fail() {
        // A superseded `cancelled` run alongside a later `success` for the same
        // check must NOT be reported as failing.
        let runs = vec![
            cr(
                "Web App Lint",
                "completed",
                "success",
                "2026-08-11T10:00:00Z",
            ),
            cr(
                "Web App Lint",
                "completed",
                "cancelled",
                "2026-08-11T09:00:00Z",
            ),
        ];
        let s = classify_check_runs(&runs);
        assert!(s.pending.is_empty());
        assert!(
            s.failing.is_empty(),
            "cancelled dup must not fail: {:?}",
            s.failing
        );
    }

    #[test]
    fn cancelled_only_is_neutral() {
        let runs = vec![cr(
            "Deploy",
            "completed",
            "cancelled",
            "2026-08-11T10:00:00Z",
        )];
        let s = classify_check_runs(&runs);
        assert_eq!(s.ci(), Ci::Green);
    }

    #[test]
    fn real_failure_is_reported() {
        let runs = vec![cr("Lint", "completed", "failure", "2026-08-11T10:00:00Z")];
        assert_eq!(classify_check_runs(&runs).failing, vec!["Lint".to_string()]);
    }

    #[test]
    fn in_progress_is_pending_by_name() {
        let runs = vec![
            cr("Deploy Preview", "in_progress", "", "2026-08-11T10:00:00Z"),
            cr("Lint", "completed", "success", "2026-08-11T09:00:00Z"),
        ];
        let s = classify_check_runs(&runs);
        assert_eq!(s.pending, vec!["Deploy Preview".to_string()]);
        assert!(s.failing.is_empty());
        assert_eq!(s.latest_start.as_deref(), Some("2026-08-11T10:00:00Z"));
    }

    #[test]
    fn latest_run_per_name_wins() {
        // Older success then newer failure for one name => failing (latest wins).
        let runs = vec![
            cr("Lint", "completed", "success", "2026-08-11T09:00:00Z"),
            cr("Lint", "completed", "failure", "2026-08-11T10:00:00Z"),
        ];
        assert_eq!(classify_check_runs(&runs).failing, vec!["Lint".to_string()]);
        // A flaky job re-run to green replaces its failure.
        let rerun = vec![
            cr("E2E", "completed", "failure", "2026-08-11T09:00:00Z"),
            cr("E2E", "completed", "success", "2026-08-11T10:00:00Z"),
        ];
        assert_eq!(classify_check_runs(&rerun).ci(), Ci::Green);
    }

    #[test]
    fn commit_statuses_are_named_by_context() {
        let mut s = CheckSummary::default();
        add_commit_statuses(
            &mut s,
            &json!({"statuses": [
                {"context": "staging-apply", "state": "pending"},
                {"context": "security", "state": "error"},
                {"context": "lint", "state": "success"}
            ]}),
        );
        assert_eq!(s.pending, vec!["staging-apply".to_string()]);
        assert_eq!(s.failing, vec!["security".to_string()]);
    }

    #[test]
    fn green_is_not_trusted_until_label_ci_starts() {
        let added = 1_000;
        assert!(!label_ci_settled(Some(added), Some(added - 50), added + 30));
        assert!(label_ci_settled(Some(added), Some(added + 10), added + 30));
        assert!(label_ci_settled(
            Some(added),
            None,
            added + LABEL_CI_GRACE_SECS
        ));
        assert!(label_ci_settled(None, None, added));
    }

    fn view() -> GateView {
        GateView {
            draft: false,
            require_human: true,
            bot_verdict: Some("APPROVE".into()),
            approvals: Approvals {
                approved_by: vec!["alice".into()],
                ..Default::default()
            },
            want_labels: vec![],
            present_labels: vec![],
            labels_added_at: None,
            ci: None,
            now: 10_000,
        }
    }

    fn green() -> CheckSummary {
        CheckSummary::default()
    }

    #[test]
    fn both_keys_then_ci_then_merge() {
        let mut v = view();
        assert_eq!(decide_gate(&v).action, Action::ReadCi);
        v.ci = Some(green());
        assert_eq!(decide_gate(&v).action, Action::Merge);
    }

    #[test]
    fn waiting_on_people_never_polls() {
        let mut v = view();
        v.approvals = Approvals::default();
        let d = decide_gate(&v);
        assert_eq!(d.action, Action::WaitForPeople);
        assert_eq!(d.state, STATE_WAITING_APPROVAL);
        assert!(d
            .lines
            .iter()
            .any(|(m, l)| *m == Mark::Waiting && l.contains("person's approval")));
        // A standing request for changes is named.
        v.approvals = Approvals {
            approved_by: vec!["alice".into()],
            blocking_by: vec!["bob".into()],
            ..Default::default()
        };
        let d = decide_gate(&v);
        assert_eq!(d.action, Action::WaitForPeople);
        assert!(d.lines.iter().any(|(_, l)| l.contains("@bob")));
    }

    #[test]
    fn a_dismissed_coderhelm_approval_disarms_the_gate() {
        let mut v = view();
        v.approvals.bot_dismissed_at_head = true;
        v.ci = Some(green());
        let d = decide_gate(&v);
        assert_eq!(d.action, Action::WaitForPeople);
        assert!(d
            .lines
            .iter()
            .any(|(m, l)| *m == Mark::Blocked && l.contains("dismissed")));
    }

    #[test]
    fn coderhelm_s_request_for_changes_is_never_merged_on() {
        let mut v = view();
        v.bot_verdict = Some("REQUEST_CHANGES".into());
        v.ci = Some(green());
        assert_eq!(decide_gate(&v).action, Action::WaitForPeople);
        v.bot_verdict = None;
        assert_eq!(decide_gate(&v).action, Action::WaitForPeople);
    }

    #[test]
    fn a_draft_is_never_merged() {
        let mut v = view();
        v.draft = true;
        v.ci = Some(green());
        assert_eq!(decide_gate(&v).action, Action::WaitForPeople);
    }

    #[test]
    fn the_person_key_is_optional_when_the_repo_says_so() {
        let mut v = view();
        v.require_human = false;
        v.approvals = Approvals::default();
        v.ci = Some(green());
        assert_eq!(decide_gate(&v).action, Action::Merge);
    }

    #[test]
    fn deploy_labels_are_added_once_per_head() {
        let mut v = view();
        v.want_labels = vec!["deploy:staging".into()];
        assert_eq!(
            decide_gate(&v).action,
            Action::AddLabels(vec!["deploy:staging".into()])
        );
        // Added for this head (and since removed by the repo's workflow): not again.
        v.labels_added_at = Some(v.now - 10);
        v.ci = Some(green());
        // Green right after adding is not trusted until label CI starts.
        assert_eq!(decide_gate(&v).action, Action::PollCi);
        v.now += LABEL_CI_GRACE_SECS;
        assert_eq!(decide_gate(&v).action, Action::Merge);
        // Present already (case-insensitive): nothing to add.
        let mut w = view();
        w.want_labels = vec!["Deploy:Staging".into()];
        w.present_labels = vec!["deploy:staging".into()];
        assert_eq!(decide_gate(&w).action, Action::ReadCi);
    }

    #[test]
    fn ci_running_polls_and_failing_stops() {
        let mut v = view();
        v.ci = Some(CheckSummary {
            pending: vec!["Deploy".into()],
            ..Default::default()
        });
        let d = decide_gate(&v);
        assert_eq!(d.action, Action::PollCi);
        assert_eq!(d.state, STATE_WAITING_CI);
        v.ci = Some(CheckSummary {
            failing: vec!["E2E".into()],
            ..Default::default()
        });
        let d = decide_gate(&v);
        assert_eq!(d.action, Action::BlockedOnCi);
        assert_eq!(d.state, STATE_BLOCKED_CI);
        assert!(common::merge_gate::wakes_on_ci(d.state));
    }

    #[test]
    fn error_retries_follow_the_shared_schedule_then_pause() {
        assert_eq!(error_retry_delay(0), Some(120));
        assert_eq!(error_retry_delay(1), Some(600));
        assert_eq!(error_retry_delay(2), Some(900));
        assert_eq!(error_retry_delay(50), Some(900));
        assert_eq!(error_retry_delay(MAX_ERROR_RETRIES), None);
    }

    #[test]
    fn status_renders_marks_and_is_stable() {
        let lines = vec![
            (Mark::Done, "CoderHelm approved".to_string()),
            (Mark::Waiting, "Waiting for a person's approval".to_string()),
        ];
        let a = render_status("abc1234def", "main", &lines, None);
        let b = render_status("abc1234def", "main", &lines, None);
        assert_eq!(a, b, "same state must render identically (no rewrite)");
        assert!(a.starts_with(STATUS_MARKER));
        assert!(a.contains("**Auto-merge** · waiting\n"));
        assert!(!a.contains('#'), "no heading: the status is one bold line");
        assert!(a.contains("Commit abc1234 into main."));
        assert!(a.contains("- ✅ CoderHelm approved"));
        assert!(a.contains("- ⏳ Waiting for a person's approval"));
    }

    #[test]
    fn pause_note_keeps_the_checklist() {
        let lines = vec![(Mark::Waiting, "CI running: `Deploy`".to_string())];
        let last = render_status("abc1234", "main", &lines, None);
        let paused = with_note(&last, "abc1234", "main", "Paused.");
        assert!(paused.contains("CI running: `Deploy`"));
        assert!(paused.find("Paused.") < paused.find("Commit abc1234 into main."));
        assert!(paused.ends_with("</sub>"));
        let fresh = with_note("", "abc1234", "main", "Paused.");
        assert!(fresh.starts_with(STATUS_MARKER) && fresh.contains("Paused."));
    }

    #[test]
    fn a_note_on_an_old_style_comment_keeps_its_checklist() {
        let old = format!(
            "{STATUS_MARKER}\n### 🚦 Auto-merge status — `abc1234` → `main`\n\n- ⏳ CI running: `Deploy`\n\n<sub>I merge automatically once every item is ✅. This comment updates in place.</sub>"
        );
        let paused = with_note(&old, "abc1234", "main", "Paused.");
        assert!(paused.contains("CI running: `Deploy`") && paused.contains("Paused."));
    }

    #[test]
    fn long_check_lists_are_capped() {
        let names: Vec<String> = (1..=10).map(|i| format!("c{i}")).collect();
        let out = code_list(&names);
        assert!(out.contains("`c8`"));
        assert!(!out.contains("`c9`"));
        assert!(out.ends_with("and 2 more"));
    }
}
