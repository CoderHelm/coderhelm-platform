//! Armed auto-merge gate. Merges a PR only once BOTH keys are present — the
//! bot's OWN latest verdict for this head is APPROVE (verified here, not assumed,
//! since the gateway also arms on any human approval) + a human approval — AND
//! every CI check is green. If the repo configures an (operator-set, never
//! hardcoded) deploy label, CoderHelm's own PRs carry it from PR creation (the PR
//! maker adds it) so the repo's deploy/preview CI runs from the start; human PRs
//! get it added here once approved. Either way the gate waits for that deploy's CI
//! before merging, and NEVER merges on failing, still-running or unreadable CI.
//! Bound to the reviewed head: a later commit stops the gate (the fresh review
//! re-arms).
//!
//! Event-driven with a bounded poll as the backstop:
//! - Waiting on a person (an approval) does not poll. The approval webhook, a
//!   new review verdict, or a dismissal re-arms the gate.
//! - Waiting on CI polls for up to `WATCH_WINDOW_SECS`, and the gateway also
//!   re-arms the gate whenever a check suite on the PR's head completes. So a
//!   slow deploy, or a flaky job re-run to green, still merges.
//! - Transient GitHub or DynamoDB errors re-poll instead of ending the gate.
//!
//! State lives in one record per PR (`MERGEGATE#…`), and the PR shows ONE
//! status comment that is edited in place with exactly what the gate is
//! waiting on. Each arming starts a new chain; a tick from an older chain
//! stops, so a PR never has two polling loops.

use crate::clients::github::{GitHubClient, MergeOutcome};
use crate::models::AwaitMergeMessage;
use crate::passes::review_actions::{
    fetch_approvals, post_merge_actions, should_merge, OnApproveConfig,
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
/// Delay before the first evaluation after a bot APPROVE, so checks triggered
/// by the reviewed push have registered.
const SETTLE_DELAY_SECS: i32 = 45;
/// How long one arming keeps polling CI before it pauses. CI completion
/// re-arms a paused gate, so this bounds cost, not correctness.
const WATCH_WINDOW_SECS: i64 = 60 * 60;
/// Hard ceiling on polls per chain (errors included).
const MAX_POLLS: u32 = 80;
/// After the gate adds deploy labels, how long a "green" CI result is not
/// trusted unless a check has started since the labels were added.
const LABEL_CI_GRACE_SECS: i64 = 180;
/// How long a gate record is kept after its last write.
const RECORD_TTL_SECS: u64 = 30 * 86_400;

/// Hidden marker that identifies the gate's status comment.
pub const STATUS_MARKER: &str = "<!-- coderhelm:merge-status -->";

// ─── CI ─────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
enum Ci {
    Green,
    Pending(Vec<String>),
    Failing(Vec<String>),
}

/// Pure summary of a ref's check-runs.
#[derive(Debug, Default, PartialEq, Eq)]
struct CheckSummary {
    pending: Vec<String>,
    failing: Vec<String>,
    /// Latest `started_at` across the kept runs (RFC 3339), if any.
    latest_start: Option<String>,
}

/// Pure classification of a ref's GitHub check-runs.
/// GitHub returns EVERY check-run for a ref, including superseded ones: a re-run or
/// a concurrency-cancel leaves the stale earlier run behind, so one check name can
/// appear several times (e.g. a `cancelled` plus a later `success`). We keep only
/// the LATEST run per name so a stale duplicate can't veto the current result, and
/// `cancelled` is treated as NEUTRAL — GitHub cancels superseded / concurrency-
/// grouped runs and fail-fast matrix siblings as a matter of course, and a genuine
/// failure always surfaces as `failure`/`timed_out` on its own check.
fn classify_check_runs(runs: &[serde_json::Value]) -> CheckSummary {
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

/// Combine GitHub Actions check-runs + legacy commit statuses into one verdict.
/// No checks at all ⇒ Green (nothing to gate on). An API error is an error —
/// never a green light.
async fn ci_state(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<(Ci, CheckSummary), BoxError> {
    let runs = github.list_check_runs_for_ref(owner, repo, sha).await?;
    let runs = runs["check_runs"].as_array().cloned().unwrap_or_default();
    let mut summary = classify_check_runs(&runs);

    // Legacy commit statuses (external CI that posts a status, e.g. staging apply).
    // Only meaningful when there's at least one status — an empty set reports
    // state "pending", which would otherwise wedge repos that use only check-runs.
    let status = github.get_commit_status(owner, repo, sha).await?;
    let statuses = status["statuses"].as_array().cloned().unwrap_or_default();
    for st in &statuses {
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

    // While anything is still running the verdict isn't final — keep waiting
    // rather than declaring a failure mid-run.
    let ci = if !summary.pending.is_empty() {
        Ci::Pending(summary.pending.clone())
    } else if !summary.failing.is_empty() {
        Ci::Failing(summary.failing.clone())
    } else {
        Ci::Green
    };
    Ok((ci, summary))
}

/// Pure: may a green result be trusted yet, given when the gate added deploy
/// labels? Label-triggered workflows take a moment to register their checks;
/// until one has started (or the grace period passed) green means "not yet".
fn label_ci_settled(labels_added_at: Option<i64>, latest_start: Option<i64>, now: i64) -> bool {
    match labels_added_at {
        None => true,
        Some(added) => {
            now - added >= LABEL_CI_GRACE_SECS || latest_start.is_some_and(|s| s >= added)
        }
    }
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

async fn load_record(state: &WorkerState, msg: &AwaitMergeMessage) -> Result<GateRecord, BoxError> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key(
            "sk",
            attr_s(&gate_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number)),
        )
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
/// Keeps the status comment and label bookkeeping.
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
        .key(
            "sk",
            attr_s(&gate_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number)),
        )
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
        .key(
            "sk",
            attr_s(&gate_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number)),
        )
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

async fn record_labels_added(state: &WorkerState, msg: &AwaitMergeMessage, now: i64) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key(
            "sk",
            attr_s(&gate_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number)),
        )
        .update_expression("SET labels_added_head = :h, labels_added_at = :t")
        .expression_attribute_values(":h", attr_s(&msg.head_sha))
        .expression_attribute_values(":t", attr_n(now))
        .send()
        .await;
}

// ─── Status comment ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
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

/// Pure: the status comment body. No timestamps, so an unchanged state renders
/// byte-identically and the comment is not rewritten.
fn render_status(head: &str, base: &str, lines: &[(Mark, String)], note: Option<&str>) -> String {
    let short = &head[..head.len().min(7)];
    let mut body = format!("{STATUS_MARKER}\n### 🚦 Auto-merge status — `{short}` → `{base}`\n\n");
    for (mark, line) in lines {
        body.push_str(&format!("- {} {line}\n", mark.icon()));
    }
    if let Some(note) = note {
        body.push_str(&format!("\n{note}\n"));
    }
    body.push_str(STATUS_FOOTER);
    body
}

const STATUS_FOOTER: &str =
    "\n<sub>I merge automatically once every item is ✅. This comment updates in place.</sub>";

/// Pure: the last status body with `note` added above the footer (used when
/// the gate pauses, so the checklist of what it was waiting on stays visible).
fn with_note(last_body: &str, head: &str, base: &str, note: &str) -> String {
    match last_body.strip_suffix(STATUS_FOOTER) {
        Some(main) if last_body.starts_with(STATUS_MARKER) => {
            format!("{}\n{note}\n{STATUS_FOOTER}", main.trim_end())
        }
        _ => render_status(head, base, &[], Some(note)),
    }
}

/// Create or update the PR's single status comment. Skips the write when the
/// rendered body is unchanged.
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
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
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
    let _ = state
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
        .await;
}

// ─── Bot key ────────────────────────────────────────────────────────────────

/// CoderHelm's own latest verdict for this head, from the persisted review
/// records (uniform across human PRs, where the bot posts a real review, and
/// bot PRs, where GitHub forbids self-approval so the verdict lives only in the
/// record). Records are keyed `REVIEW#{owner}/{repo}#{pr:06}#{rfc3339}`, so a
/// descending query yields newest-first. None = no verdict for this head yet.
async fn bot_verdict_at_head(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    head_sha: &str,
) -> Result<Option<String>, BoxError> {
    let prefix = format!("REVIEW#{owner}/{repo}#{pr:0>6}#");
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    // Page through the newest records until one for this head is found; the
    // records of other heads and question answers are skipped.
    for _ in 0..5 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
            .expression_attribute_values(":pk", attr_s(team_id))
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
            if text(it, "head_sha") == head_sha {
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
            // Without a chain record the gate cannot guard against a second
            // loop; retry the arming shortly instead of dropping it.
            warn!(pr = msg.pr_number, error = %e, "await-merge: could not start chain — retrying");
            msg.chain_id.clear();
            send(state, &msg, msg.attempts, POLL_DELAY_SECS).await;
            return Ok(());
        }
        let mut r = load_record(state, &msg).await.unwrap_or_default();
        r.armed_at = now;
        r.chain_id = msg.chain_id.clone();
        r
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
                poll_again(state, &msg, now, now).await;
                return Ok(());
            }
        }
    };

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    match evaluate(state, &github, &msg, &mut record, now).await {
        Ok(Next::Stop) => {}
        Ok(Next::Poll) => {
            if !poll_again(state, &msg, now, record.armed_at).await {
                pause(state, &github, &msg, &mut record, None).await;
            }
        }
        Err(e) => {
            warn!(pr = msg.pr_number, attempt = msg.attempts, error = %e, "await-merge: evaluation failed — will retry");
            let err = e.to_string();
            if !poll_again(state, &msg, now, record.armed_at).await {
                pause(state, &github, &msg, &mut record, Some(&err)).await;
            }
        }
    }
    Ok(())
}

/// Queue the next poll unless the window or poll budget is spent. Returns
/// false when the gate should pause instead.
async fn poll_again(state: &WorkerState, msg: &AwaitMergeMessage, now: i64, armed_at: i64) -> bool {
    if msg.attempts >= MAX_POLLS || (armed_at > 0 && now - armed_at >= WATCH_WINDOW_SECS) {
        return false;
    }
    send(state, msg, msg.attempts + 1, POLL_DELAY_SECS).await
}

/// Stop polling and say so on the PR. A completed check suite, an approval or
/// a new commit re-arms the gate.
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
    let mut note = format!(
        "⏸️ I stopped checking after {} minutes. I'll pick this up again automatically when CI \
         finishes on this commit, or on a new approval or commit.",
        WATCH_WINDOW_SECS / 60
    );
    if let Some(err) = last_error {
        note.push_str(&format!(
            "\n\nLast check failed with: `{}`",
            common::truncate_str(err, 300)
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
    let watched = msg.self_authored
        || pr["labels"].as_array().is_some_and(|ls| {
            ls.iter().any(|l| {
                l["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(&cfg.review_label))
            })
        });

    if pr["state"].as_str() != Some("open") {
        info!(pr = msg.pr_number, "PR not open — stop await-merge");
        set_state(state, msg, STATE_CLOSED, "").await;
        if record.status_comment_id.is_some() {
            let note = if pr["merged"].as_bool() == Some(true) {
                "✋ Merged outside the gate — nothing left to do."
            } else {
                "✋ PR closed — nothing left to do."
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
        if watched {
            let note = format!(
                "🔁 New commit `{}` — waiting for CoderHelm's review of it before merging.",
                &cur_head[..cur_head.len().min(7)]
            );
            let body = render_status(&msg.head_sha, &msg.base_branch, &[], Some(&note));
            publish_status(state, github, msg, record, &body).await;
        }
        return Ok(Next::Stop);
    }

    // ── Gate 1: both keys. Waiting on people never polls. ──
    let short = &msg.head_sha[..msg.head_sha.len().min(7)];
    let require_human = cfg.require_human_approval || msg.self_authored;
    let approvals = fetch_approvals(github, owner, repo, msg.pr_number, &msg.head_sha).await?;
    let verdict = bot_verdict_at_head(
        state,
        &msg.team_id,
        owner,
        repo,
        msg.pr_number,
        &msg.head_sha,
    )
    .await?;
    let bot_approved = verdict.as_deref() == Some("APPROVE") && !approvals.bot_dismissed_at_head;
    let human_present = approvals.human_key();

    let mut lines: Vec<(Mark, String)> = Vec::new();
    lines.push(match (verdict.as_deref(), approvals.bot_dismissed_at_head) {
        (Some("APPROVE"), false) => (Mark::Done, format!("CoderHelm approved `{short}`")),
        (Some("APPROVE"), true) => (
            Mark::Blocked,
            format!("CoderHelm's approval of `{short}` was dismissed — reply `@coderhelm re-review` to review again"),
        ),
        (Some(_), _) => (
            Mark::Blocked,
            format!("CoderHelm requested changes on `{short}`"),
        ),
        (None, _) => (
            Mark::Waiting,
            format!("Waiting for CoderHelm's review of `{short}`"),
        ),
    });
    if require_human {
        lines.push(if human_present {
            (
                Mark::Done,
                format!("Approved by {}", mention_list(&approvals.approved_by)),
            )
        } else if !approvals.blocking_by.is_empty() {
            (
                Mark::Blocked,
                format!(
                    "Changes requested by {}",
                    mention_list(&approvals.blocking_by)
                ),
            )
        } else {
            (Mark::Waiting, "Waiting for a human approval".to_string())
        });
    }

    if !should_merge(cfg.auto_merge, require_human, bot_approved, human_present) {
        info!(
            pr = msg.pr_number,
            bot_approved,
            human_present,
            "await-merge: waiting on approvals — stopping until the next review event"
        );
        set_state(state, msg, STATE_WAITING_APPROVAL, "").await;
        if watched {
            let body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            publish_status(state, github, msg, record, &body).await;
        }
        return Ok(Next::Stop);
    }

    // ── Gate 2: deploy label(s), added once per head. ──
    let want_labels: Vec<String> = cfg
        .deploy_label
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let mut labels_added_at = None;
    if !want_labels.is_empty() {
        let present: Vec<String> = pr["labels"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|l| l["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let missing: Vec<String> = want_labels
            .iter()
            .filter(|l| !present.iter().any(|p| p.eq_ignore_ascii_case(l)))
            .cloned()
            .collect();
        let joined = want_labels
            .iter()
            .map(|l| format!("`{l}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let added_for_this_head = record.labels_added_head == msg.head_sha;
        if added_for_this_head {
            labels_added_at = record.labels_added_at;
        }
        if !missing.is_empty() && !added_for_this_head {
            github
                .add_labels(owner, repo, msg.pr_number, &missing)
                .await?;
            record_labels_added(state, msg, now).await;
            record.labels_added_head = msg.head_sha.clone();
            record.labels_added_at = Some(now);
            info!(pr = msg.pr_number, labels = %joined, "await-merge: added deploy label(s)");
            lines.push((
                Mark::Waiting,
                format!("Added {joined} — waiting for its CI to start"),
            ));
            set_state(state, msg, STATE_WAITING_CI, "deploy labels added").await;
            let body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            publish_status(state, github, msg, record, &body).await;
            return Ok(Next::Poll);
        }
        // Present, or added earlier for this head and since removed by the
        // repo's own workflow — never re-add in a loop.
        lines.push((Mark::Done, format!("Deploy label {joined} applied")));
    }

    // ── Gate 3: CI. ──
    let (ci, summary) = ci_state(github, owner, repo, &msg.head_sha).await?;
    let latest_start = summary
        .latest_start
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp());
    let ci = match ci {
        Ci::Green if !label_ci_settled(labels_added_at, latest_start, now) => {
            Ci::Pending(vec!["label-triggered CI (not started yet)".to_string()])
        }
        other => other,
    };
    match ci {
        Ci::Pending(names) => {
            lines.push((Mark::Waiting, format!("CI running: {}", code_list(&names))));
            set_state(state, msg, STATE_WAITING_CI, &names.join(", ")).await;
            let body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            publish_status(state, github, msg, record, &body).await;
            Ok(Next::Poll)
        }
        Ci::Failing(names) => {
            info!(pr = msg.pr_number, failing = %names.join(", "), "await-merge: CI failing — waiting for a re-run or a new commit");
            lines.push((Mark::Blocked, format!("CI failing: {}", code_list(&names))));
            set_state(state, msg, STATE_BLOCKED_CI, &names.join(", ")).await;
            let body = render_status(
                &msg.head_sha,
                &msg.base_branch,
                &lines,
                Some("Not merging. Re-run the failed checks or push a fix — I pick it up again as soon as CI finishes."),
            );
            publish_status(state, github, msg, record, &body).await;
            Ok(Next::Stop)
        }
        Ci::Green => {
            lines.push((Mark::Done, "CI green".to_string()));
            merge(state, github, msg, record, &cfg, lines).await
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
) -> Result<Next, BoxError> {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    match github
        .merge_pull_request(owner, repo, msg.pr_number, &msg.head_sha, &cfg.merge_method)
        .await?
    {
        MergeOutcome::Merged => {
            set_state(state, msg, STATE_MERGED, "").await;
            let merged_lines = post_merge_actions(
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
            let _ = github
                .create_issue_comment(
                    owner,
                    repo,
                    msg.pr_number,
                    &format!("### 🚀 Auto-merged\n\n{}", merged_lines.join("\n")),
                )
                .await;
            lines.push((Mark::Done, format!("Merged into `{}`", msg.base_branch)));
            let body = render_status(&msg.head_sha, &msg.base_branch, &lines, None);
            publish_status(state, github, msg, record, &body).await;
            info!(pr = msg.pr_number, "Armed auto-merge: merged");
            Ok(Next::Stop)
        }
        MergeOutcome::NotMergeable(reason) => {
            // Branch protection, a required check GitHub still counts as
            // missing, or a conflict. Keep polling: most of these clear on their
            // own, and the reason is on the PR for the ones that don't.
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

/// Arm the gate for a PR (called at review time; the gateway arms on human
/// approvals, dismissals and completed check suites). Every arming starts a new
/// chain. Returns false if the queue isn't configured.
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
        .delay_seconds(delay)
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
    use serde_json::json;

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
        assert!(s.pending.is_empty());
        assert!(s.failing.is_empty());
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
    }

    #[test]
    fn green_is_not_trusted_until_label_ci_starts() {
        let added = 1_000;
        // Just added, nothing started since: not settled.
        assert!(!label_ci_settled(Some(added), Some(added - 50), added + 30));
        // A check started after the labels: settled.
        assert!(label_ci_settled(Some(added), Some(added + 10), added + 30));
        // Grace period over: settled even if the label triggered nothing.
        assert!(label_ci_settled(
            Some(added),
            None,
            added + LABEL_CI_GRACE_SECS
        ));
        // No labels added by the gate: nothing to wait for.
        assert!(label_ci_settled(None, None, added));
    }

    #[test]
    fn status_renders_marks_and_is_stable() {
        let lines = vec![
            (Mark::Done, "CoderHelm approved `abc1234`".to_string()),
            (Mark::Waiting, "Waiting for a human approval".to_string()),
        ];
        let a = render_status("abc1234def", "main", &lines, None);
        let b = render_status("abc1234def", "main", &lines, None);
        assert_eq!(a, b, "same state must render identically (no rewrite)");
        assert!(a.starts_with(STATUS_MARKER));
        assert!(a.contains("`abc1234` → `main`"));
        assert!(a.contains("- ✅ CoderHelm approved"));
        assert!(a.contains("- ⏳ Waiting for a human approval"));
    }

    #[test]
    fn pause_note_keeps_the_checklist() {
        let lines = vec![(Mark::Waiting, "CI running: `Deploy`".to_string())];
        let last = render_status("abc1234", "main", &lines, None);
        let paused = with_note(&last, "abc1234", "main", "⏸️ paused");
        assert!(paused.contains("CI running: `Deploy`"));
        assert!(paused.contains("⏸️ paused"));
        assert!(paused.ends_with(STATUS_FOOTER));
        // No previous body: the note alone.
        let fresh = with_note("", "abc1234", "main", "⏸️ paused");
        assert!(fresh.starts_with(STATUS_MARKER) && fresh.contains("⏸️ paused"));
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
