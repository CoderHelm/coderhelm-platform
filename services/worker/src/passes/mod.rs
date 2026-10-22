use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Maximum Lambda execution time (15 min). We use this to skip expensive
/// remediation cycles when we're running low on time.
const LAMBDA_TIMEOUT_SECS: u64 = 900;
/// Minimum seconds remaining before we'll start a new implement cycle.
const MIN_TIME_FOR_REMEDIATION_SECS: u64 = 240; // 4 minutes

/// Auto-continuation: a big ticket that times out mid-implement is re-enqueued
/// to continue from its partial (already-committed) branch state, rather than
/// stopping at a partial PR. Hard-bounded two ways so it can NEVER loop:
const MAX_CONTINUATIONS: u32 = 3;
/// ...and a wall-clock backstop across all continuations (survives a bad count).
const MAX_CONTINUATION_WALL_SECS: u64 = 45 * 60;

/// May a timed-out run be auto-continued? Pure + total so it's unit-tested.
/// False the moment EITHER bound is hit — defense in depth against a runaway.
fn should_auto_continue(continuation: u32, first_attempt_ms: u64, now_ms: u64) -> bool {
    if continuation >= MAX_CONTINUATIONS {
        return false;
    }
    if first_attempt_ms > 0 {
        let elapsed_secs = now_ms.saturating_sub(first_attempt_ms) / 1000;
        if elapsed_secs >= MAX_CONTINUATION_WALL_SECS {
            return false;
        }
    }
    true
}

/// Other repos of the same owner that an agent tried to read or edit, by the
/// paths it passed (`owner/other-repo/path/to/file`): a sign the ticket's
/// files live in a repo other than the one the run is in.
fn repos_reached_for(log: &[serde_json::Value], owner: &str, repo: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let prefix = format!("{owner}/");
    let mut visit = |v: &serde_json::Value| {
        if let Some(blocks) = v["content"].as_array() {
            for b in blocks {
                if b["type"] != "tool_use" {
                    continue;
                }
                let name = b["name"].as_str().unwrap_or("");
                if !matches!(
                    name,
                    "read_file" | "read_file_lines" | "edit_file" | "write_file"
                ) {
                    continue;
                }
                let Some(path) = b["input"]["path"].as_str() else {
                    continue;
                };
                let Some(rest) = path.strip_prefix(&prefix) else {
                    continue;
                };
                let Some((other, file)) = rest.split_once('/') else {
                    continue;
                };
                if other != repo && !other.is_empty() && !file.is_empty() {
                    let full = format!("{owner}/{other}");
                    if !out.contains(&full) {
                        out.push(full);
                    }
                }
            }
        }
    };
    log.iter().for_each(&mut visit);
    out
}

/// The agent-loop error emitted when the step budget is exhausted
/// (anthropic.rs: "Reached the maximum number of steps (N) without finishing").
/// This means "needs more turns", NOT a real failure — treated like the
/// wall-clock timeout and auto-continued rather than salvaged into a
/// half-finished PR. Substring match is the same error-classification idiom as
/// the `cancelled by user` check. Coupled to the message in
/// `agent::anthropic::converse_tool_loop`.
fn is_budget_exhausted(err_msg: &str) -> bool {
    err_msg.contains("maximum number of steps")
}

/// Sends a ticket run again after a transient model error, keeping its
/// run_id, per [`common::model_retry::requeue_delay`]. False once the policy
/// is spent or the queue is missing (the run then fails as before).
async fn requeue_after_model_error(state: &WorkerState, msg: &TicketMessage, run_id: &str) -> bool {
    let Some(delay) = common::model_retry::requeue_delay(msg.model_retries) else {
        return false;
    };
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut next = msg.clone();
    next.model_retries = msg.model_retries + 1;
    next.continuation_run_id = Some(run_id.to_string());
    let Ok(body) = serde_json::to_string(&crate::models::WorkerMessage::Ticket(next)) else {
        return false;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay as i32)
        .send()
        .await
    {
        Ok(_) => {
            warn!(
                run_id,
                retry = msg.model_retries + 1,
                delay,
                "model_error_requeued: run sent again after a transient model error"
            );
            true
        }
        Err(e) => {
            warn!(run_id, error = %e, "Could not requeue the run after a model error");
            false
        }
    }
}

/// Auto-continue a run that ran out of budget (wall-clock OR step limit) with
/// real partial work already committed to its branch: re-enqueue the ticket to
/// resume from that branch (reusing run_id). Hard-bounded by count AND
/// wall-clock via `should_auto_continue` so it can NEVER loop, and only when a
/// partial checkpoint + ticket queue exist. Returns true iff a continuation was
/// enqueued. Shared by both orchestration death paths (timeout + error arms) so
/// they can't drift.
async fn try_auto_continue(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    checkpoint: &Option<(String, String, u8)>,
    continuable_passes: &[&str],
) -> bool {
    // Gate on the checkpoint's pass IDENTITY, not just on there being a branch.
    // The wall-clock-timeout arm passes the broad set (any late pass with work
    // committed is worth resuming); the step-budget arm passes only
    // ["implement_started"] so a step-limit raised by some OTHER pass sharing the
    // agent loop (security/test) can never resume from a stale implement
    // checkpoint. Callers own this invariant; don't widen it here.
    let has_partial = matches!(checkpoint, Some((lp, br, _))
        if !br.is_empty() && continuable_passes.contains(&lp.as_str()));
    if !has_partial || state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let first_ms = if msg.first_attempt_ms > 0 {
        msg.first_attempt_ms
    } else {
        now_ms
    };
    if !should_auto_continue(msg.continuation, first_ms, now_ms) {
        return false;
    }
    let mut cont = msg.clone();
    cont.continuation = msg.continuation + 1;
    cont.continuation_run_id = Some(run_id.to_string());
    cont.first_attempt_ms = first_ms;
    match serde_json::to_string(&crate::models::WorkerMessage::Ticket(cont)) {
        Ok(body) => match state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body)
            .delay_seconds(5)
            .send()
            .await
        {
            Ok(_) => {
                info!(
                    run_id,
                    next_continuation = msg.continuation + 1,
                    "Ran out of budget with partial work — auto-continuing (bounded)"
                );
                true
            }
            Err(e) => {
                warn!(run_id, error = %e, "Continuation enqueue failed — falling back to salvage");
                false
            }
        },
        Err(e) => {
            warn!(run_id, error = %e, "Continuation serialize failed — falling back to salvage");
            false
        }
    }
}

/// Hard, signature-INDEPENDENT ceiling on how many times the automated post-PR
/// loops (CI-fix, review-fix, feedback) may push a fix to a PR before it is
/// handed to a human. This is the backstop for thrash that (a) mutating-error
/// stall detection can't catch (each cycle emits different errors, so the
/// signature never repeats) and (b) a dropped resume ("run already running")
/// lets bypass the per-cycle counter while the uncapped feedback loop keeps
/// pushing. Durable on the run record so it survives every Lambda invocation and
/// is shared across all three loops. High enough for a PR that legitimately
/// iterates across several distinct CI stages (lint → types → tests → a review
/// round), low enough to stop an hour-long churn fast (the real incident ran 16+
/// cycles). Human-driven feedback is NOT gated by this (see feedback.rs).
pub(crate) const MAX_AUTO_FIX_PUSHES: u32 = 8;

/// Atomically claim one automated-fix slot for the run: increment the durable
/// counter IFF it is still under MAX_AUTO_FIX_PUSHES — the check and the
/// increment are ONE conditional write, so concurrent invocations (feedback is
/// unserialized: several webhooks for one run can run at once) can never
/// overshoot the cap. Returns true if a slot was claimed (proceed with the fix),
/// false if the budget is spent (hand off to a human). Fails CLOSED: a
/// non-ConditionalCheckFailed error also returns false, because the feedback
/// path has NO other cap (no per-cycle counter, no wall-clock) and failing open
/// there would resurrect the exact unbounded churn this prevents. A transient
/// DynamoDB blip therefore causes an early, RECOVERABLE hand-off — strictly
/// safer than thrash.
pub(crate) async fn claim_auto_fix_slot(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    match state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("ADD auto_fix_pushes :one")
        .condition_expression("attribute_not_exists(auto_fix_pushes) OR auto_fix_pushes < :max")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":max", attr_n(u64::from(MAX_AUTO_FIX_PUSHES)))
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            if !format!("{e:?}").contains("ConditionalCheckFailed") {
                warn!(run_id, error = %e, "auto-fix slot claim errored — failing closed (hand off)");
            }
            false
        }
    }
}

/// TTL for the per-run WRITER slot, in seconds. Slightly past the 960s
/// stale-run reset so a crashed holder never blocks a post-reset retry for
/// more than ~1 minute after the reset fires.
pub(crate) const RUN_WRITER_TTL_SECS: i64 = 1020;

/// Acquire the run's exclusive WRITER slot. Feedback and resume both write the
/// same PR branch; resume serializes against other resumes via the status
/// claim, but feedback historically took NO claim at all — so a feedback pass
/// and a resume pass could interleave writes on one branch and clobber each
/// other (and force-squash under a broken single-writer assumption). This slot
/// makes "I am the only platform writer on this run's branch" explicit: one
/// conditional write, re-entrant for the same holder, self-expiring via TTL so
/// a crashed holder can't wedge the run. Returns true when the slot is held.
pub(crate) async fn acquire_run_writer_slot(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    holder: &str,
) -> bool {
    let now = chrono::Utc::now().timestamp();
    let until = now + RUN_WRITER_TTL_SECS;
    match state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET writer_until = :until, writer_holder = :h")
        .condition_expression(
            "attribute_not_exists(writer_until) OR writer_until < :now OR writer_holder = :h",
        )
        .expression_attribute_values(":until", attr_n(until as u64))
        .expression_attribute_values(":now", attr_n(now as u64))
        .expression_attribute_values(":h", attr_s(holder))
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            if !format!("{e:?}").contains("ConditionalCheckFailed") {
                warn!(run_id, error = %e, "writer-slot acquire errored — treating as busy");
            }
            false
        }
    }
}

/// RAII handle for the writer slot: releases on drop (any exit path,
/// including `?` errors), with the TTL as the backstop for hard crashes.
pub(crate) struct RunWriterGuard {
    dynamo: aws_sdk_dynamodb::Client,
    table: String,
    team_id: String,
    run_id: String,
    holder: String,
}

impl Drop for RunWriterGuard {
    fn drop(&mut self) {
        let dynamo = self.dynamo.clone();
        let table = self.table.clone();
        let team_id = self.team_id.clone();
        let run_id = self.run_id.clone();
        let holder = self.holder.clone();
        // Release only OUR slot (holder-guarded) so a late release can never
        // steal a successor's slot.
        let release = async move {
            let _ = dynamo
                .update_item()
                .table_name(&table)
                .key("team_id", attr_s(&team_id))
                .key("run_id", attr_s(&run_id))
                .update_expression("SET writer_until = :zero")
                .condition_expression("writer_holder = :h")
                .expression_attribute_values(":zero", attr_n(0))
                .expression_attribute_values(":h", attr_s(&holder))
                .send()
                .await;
        };
        // Drive the release to completion SYNCHRONOUSLY. A bare tokio::spawn
        // here is unreliable on Lambda: the execution environment freezes the
        // instant the handler future resolves, so a detached release task
        // typically never runs and the slot then sits until its TTL (~17 min),
        // stalling the next writer on the run. `#[tokio::main]` gives a
        // multi-thread runtime, so block_in_place lets us block this worker on
        // the release (without wedging the scheduler) so it lands before we
        // return. Falls back to spawn only if dropped outside a runtime
        // (best-effort; TTL still backstops).
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(move || handle.block_on(release)),
            Err(_) => {
                tokio::spawn(release);
            }
        }
    }
}

/// Acquire the writer slot and wrap it in a self-releasing guard.
pub(crate) async fn acquire_run_writer_guard(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    holder: &str,
) -> Option<RunWriterGuard> {
    if acquire_run_writer_slot(state, team_id, run_id, holder).await {
        Some(RunWriterGuard {
            dynamo: state.dynamo.clone(),
            table: state.config.runs_table_name.clone(),
            team_id: team_id.to_string(),
            run_id: run_id.to_string(),
            holder: holder.to_string(),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod continuation_tests {
    use super::{
        is_budget_exhausted, should_auto_continue, MAX_CONTINUATIONS, MAX_CONTINUATION_WALL_SECS,
    };

    #[test]
    fn budget_exhausted_classifies_step_limit() {
        // The exact message agent::anthropic emits when the step budget is hit.
        assert!(is_budget_exhausted(
            "Reached the maximum number of steps (60) without finishing. \
             The issue may need more detail or a narrower scope."
        ));
        // Genuine failures must NOT be treated as continuable.
        assert!(!is_budget_exhausted(
            "not_found_error: model does not exist"
        ));
        assert!(!is_budget_exhausted("cancelled by user"));
        assert!(!is_budget_exhausted("GitHub API 404: issue not found"));
    }

    #[test]
    fn stops_at_max_count() {
        assert!(should_auto_continue(0, 0, 0));
        assert!(should_auto_continue(MAX_CONTINUATIONS - 1, 0, 0));
        assert!(!should_auto_continue(MAX_CONTINUATIONS, 0, 0)); // cap hit
        assert!(!should_auto_continue(MAX_CONTINUATIONS + 5, 0, 0));
    }

    #[test]
    fn stops_past_wall_clock() {
        let start = 1_000_000_000u64;
        // within budget → allowed
        assert!(should_auto_continue(1, start, start + 10 * 60 * 1000));
        // past the wall-clock budget → stopped even though count is fine
        let past = start + (MAX_CONTINUATION_WALL_SECS + 60) * 1000;
        assert!(!should_auto_continue(1, start, past));
    }

    #[test]
    fn count_cap_beats_fresh_clock() {
        // Even with a fresh clock, the count cap still stops it.
        assert!(!should_auto_continue(
            MAX_CONTINUATIONS,
            1_000_000_000,
            1_000_000_000
        ));
    }

    #[test]
    fn zero_first_attempt_ignores_wall_clock() {
        // first_attempt_ms == 0 means "unset" — only the count bounds apply.
        assert!(should_auto_continue(0, 0, u64::MAX));
    }
}

use crate::agent::mcp;
use crate::agent::{llm, provider};
use crate::clients::email;
use crate::clients::github::GitHubClient;
use crate::memory::AgentMemory;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

/// Download an image from S3 and return as base64 string.
pub(crate) async fn download_image_as_base64(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Option<String> {
    use base64::Engine;
    match s3.get_object().bucket(bucket).key(key).send().await {
        Ok(resp) => match resp.body.collect().await {
            Ok(bytes) => {
                let data = bytes.into_bytes();
                Some(base64::engine::general_purpose::STANDARD.encode(&data))
            }
            Err(e) => {
                warn!(key, error = %e, "Failed to read S3 image body");
                None
            }
        },
        Err(e) => {
            warn!(key, error = %e, "Failed to download image from S3");
            None
        }
    }
}

/// In-memory file read cache shared across passes within a single run.
/// Avoids re-fetching the same files from GitHub when review/security
/// re-reads files that implement already loaded.
#[derive(Clone, Default)]
pub struct FileCache {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

/// Result of a per-repo implement→security→PR cycle (multi-repo support).
struct RepoPrResult {
    branch: String,
    pr_number: u64,
    pr_url: String,
}

impl FileCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.inner.read().await.get(key).cloned()
    }

    pub async fn insert(&self, key: String, value: String) {
        self.inner.write().await.insert(key, value);
    }

    pub async fn remove(&self, key: &str) {
        self.inner.write().await.remove(key);
    }
}

/// S3 prefix for a ticket's cached openspec plan, scoped to the repo the
/// plan was generated against. The old ticket-only key silently reused a
/// plan generated for repo A (its file paths and all) when the same ticket
/// re-ran in repo B.
pub(crate) fn openspec_prefix(team_id: &str, ticket_id: &str, owner: &str, repo: &str) -> String {
    format!(
        "teams/{team_id}/runs/{}/openspec/{owner}__{repo}",
        ticket_id.to_lowercase()
    )
}

/// Line diff of two ticket texts: unchanged lines prefixed "  ", removed
/// "- ", added "+ ". Plain LCS over lines; when the texts are too large for
/// the quadratic table it falls back to the full old and new text.
pub(crate) fn ticket_text_diff(old: &str, new: &str) -> String {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    if a.len().saturating_mul(b.len()) > 4_000_000 {
        return format!("### Previous ticket\n{old}\n\n### Updated ticket\n{new}");
    }
    // lcs[i][j] = LCS length of a[i..] and b[j..]
    let mut lcs = vec![vec![0u32; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len() + b.len());
    while i < a.len() || j < b.len() {
        if i < a.len() && j < b.len() && a[i] == b[j] {
            out.push(format!("  {}", a[i]));
            i += 1;
            j += 1;
        } else if j < b.len() && (i == a.len() || lcs[i][j + 1] >= lcs[i + 1][j]) {
            out.push(format!("+ {}", b[j]));
            j += 1;
        } else {
            out.push(format!("- {}", a[i]));
            i += 1;
        }
    }
    out.join("\n")
}

/// Render a GitHub compare response as a unified-style diff, bounded to
/// `max_bytes` (head and tail kept).
pub(crate) fn format_compare_diff(compare: &serde_json::Value, max_bytes: usize) -> String {
    let mut out = String::new();
    for f in compare["files"].as_array().into_iter().flatten() {
        let name = f["filename"].as_str().unwrap_or("(unknown)");
        let status = f["status"].as_str().unwrap_or("modified");
        out.push_str(&format!("--- {name} ({status})\n"));
        match f["patch"].as_str() {
            Some(p) => out.push_str(p),
            None => out.push_str("(no textual patch — binary or too large)"),
        }
        out.push_str("\n\n");
    }
    if out.len() > max_bytes {
        common::head_tail_str(&out, max_bytes)
    } else {
        out
    }
}

/// Instruction given to the implement agent when the ticket changed under an
/// open PR: update the existing implementation to match, don't start over.
pub(crate) fn ticket_update_note(
    pr_number: u64,
    previous_ticket: Option<&str>,
    current_ticket: &str,
    pr_diff: &str,
) -> String {
    let ticket_change = match previous_ticket {
        Some(old) => format!(
            "### What changed in the ticket (- old, + new)\n```diff\n{}\n```",
            common::head_tail_str(&ticket_text_diff(old, current_ticket), 16_000)
        ),
        None => "### What changed in the ticket\nThe previous ticket text was not saved; \
                 compare the updated ticket (below, under \"Original issue\") with the \
                 existing PR diff to see what is different."
            .to_string(),
    };
    let diff_block = if pr_diff.trim().is_empty() {
        "(the PR diff could not be loaded — read the changed files on this branch)".to_string()
    } else {
        format!("```diff\n{pr_diff}\n```")
    };
    format!(
        "\n## TICKET UPDATED — UPDATE THE EXISTING PR #{pr_number}\n\
         This branch already holds PR #{pr_number}, built from an EARLIER version of the ticket. \
         The ticket has since changed. Update the existing implementation so it matches the \
         UPDATED ticket: change what the edit changed, remove what it dropped, add what it added. \
         Keep the parts that still match. Do NOT start over and do NOT add a second, parallel \
         implementation next to the existing one.\n\
         If the branch already satisfies the updated ticket, make no edits and say \
         \"PR already matches the updated ticket\".\n\n\
         {ticket_change}\n\n\
         ### Current PR diff (against the base branch)\n{diff_block}\n"
    )
}

/// Persist and unlock agent memory at an early exit. Without this, the
/// no-changes / clarification / needs_input / blocked-plan returns leaked the
/// memory lock for its full 15-minute TTL, forcing subsequent runs on the
/// repo to start stateless.
/// Counts each recalled memory's use, so people can see which memories the
/// agents actually lean on. Best effort: a failed count never fails a run.
async fn record_memory_use(state: &WorkerState, msg: &TicketMessage, run_id: &str, ids: &[String]) {
    let now = chrono::Utc::now().to_rfc3339();
    for id in ids {
        let sk = common::memory_meta::usage_sk(&msg.repo_owner, &msg.repo_name, id);
        let res = state
            .dynamo
            .update_item()
            .table_name(&state.config.settings_table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key("sk", aws_sdk_dynamodb::types::AttributeValue::S(sk))
            .update_expression(
                "ADD uses :one SET last_used_at = :now, last_run_id = :run, last_title = :title",
            )
            .expression_attribute_values(
                ":one",
                aws_sdk_dynamodb::types::AttributeValue::N("1".into()),
            )
            .expression_attribute_values(
                ":now",
                aws_sdk_dynamodb::types::AttributeValue::S(now.clone()),
            )
            .expression_attribute_values(
                ":run",
                aws_sdk_dynamodb::types::AttributeValue::S(run_id.to_string()),
            )
            .expression_attribute_values(
                ":title",
                aws_sdk_dynamodb::types::AttributeValue::S(
                    common::truncate_str(&msg.title, 120).to_string(),
                ),
            )
            .send()
            .await;
        if let Err(e) = res {
            warn!(run_id, error = %e, "Could not count a memory's use");
        }
    }
}

async fn close_memory(state: &WorkerState, mem: Option<crate::memory::AgentMemory>) {
    if let Some(mem) = mem {
        if let Err(e) = mem.close_and_upload(state).await {
            warn!(error = %e, "Failed to persist agent memory at early exit");
        }
    }
}

/// Strip internal model IDs and service names from error messages
/// so they are safe to display to users.
fn sanitize_error(msg: &str) -> String {
    let mut s = msg.to_string();
    // Remove (model=claude-...) patterns
    while let Some(start) = s.find("(model=") {
        if let Some(end) = s[start..].find(')') {
            s.replace_range(start..start + end + 1, "");
        } else {
            break;
        }
    }
    // Remove model IDs — any `claude-…` token, so new models never leak,
    // including Bedrock ids (`us.anthropic.claude-…-v1:0`).
    s = s
        .split(' ')
        .filter(|w| {
            let w = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '-');
            !common::usage::normalize_model(w).starts_with("claude-")
        })
        .collect::<Vec<_>>()
        .join(" ");
    s = s.replace("Anthropic API error", "An error occurred during processing");
    // Collapse extra whitespace
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Returns seconds remaining before the Lambda timeout.
fn remaining_secs(start: &Instant) -> u64 {
    LAMBDA_TIMEOUT_SECS.saturating_sub(start.elapsed().as_secs())
}

/// Whether there's enough time left for an expensive remediation cycle.
fn has_time_for_remediation(start: &Instant) -> bool {
    remaining_secs(start) >= MIN_TIME_FOR_REMEDIATION_SECS
}

pub mod agent_tools;
pub mod await_merge;
mod checks;
pub mod code_graph;
pub mod e2e;
pub mod feedback;
pub mod fix_location;
pub mod formatter;
pub mod github_history;
pub mod health_check;
pub mod hidden_suggest;
mod implement;
pub mod infra_analyze;
pub mod jira_comment;
pub mod lessons;
pub mod live_sources;
pub mod onboard;
pub mod ops_tools;
mod plan;
pub mod plan_chat;
pub mod plan_execute;
mod pr;
pub mod pr_outcomes;
mod proof;
pub mod quarantine;
pub mod release_notes;
pub(crate) mod repo_pick;
mod resolve;
pub mod resume;
mod review;
pub mod review_actions;
pub mod review_agent;
#[cfg(test)]
mod review_eval;
pub mod review_labels;
pub mod review_lessons;
pub mod review_pr;
pub mod review_reminder;
pub mod review_risk;
pub mod review_scope;
pub mod review_sweep;
pub mod review_verdict;
mod security;
pub mod skill_compat;
pub mod skill_suggest;
pub mod skills;
pub mod syntax_check;
#[allow(dead_code)]
mod test;
pub mod ticket_history;
pub mod tool_proposals;
mod triage;
pub mod usage_reconcile;
pub mod web_check;
pub mod weekly_summary;
pub mod write_guard;

/// Minimal MCP plugin catalog: (server_id, npx_package, env_mapping).
/// Kept in sync with the full catalog in gateway/routes/plugins.rs.
#[allow(clippy::type_complexity)]
pub const MCP_CATALOG: [(&str, &str, &[(&str, &str)]); 17] = [
    (
        "figma",
        "figma-developer-mcp",
        &[("api_token", "FIGMA_API_KEY")],
    ),
    (
        "sentry",
        "@sentry/mcp-server",
        &[
            ("auth_token", "SENTRY_AUTH_TOKEN"),
            ("org_slug", "SENTRY_ORG"),
        ],
    ),
    (
        "linear",
        "@linear/mcp-server",
        &[("api_key", "LINEAR_API_KEY")],
    ),
    (
        "notion",
        "@notionhq/notion-mcp-server",
        &[("api_key", "OPENAPI_MCP_HEADERS")],
    ),
    (
        "vercel",
        "@vercel/mcp",
        &[("api_token", "VERCEL_API_TOKEN")],
    ),
    ("stripe", "@stripe/mcp", &[("api_key", "STRIPE_SECRET_KEY")]),
    (
        "cloudflare",
        "@cloudflare/mcp-server-cloudflare",
        &[
            ("api_token", "CLOUDFLARE_API_TOKEN"),
            ("account_id", "CLOUDFLARE_ACCOUNT_ID"),
        ],
    ),
    (
        "posthog",
        "@nicholasoxford/posthog-mcp",
        &[("api_key", "POSTHOG_API_KEY"), ("host", "POSTHOG_HOST")],
    ),
    (
        "gitlab",
        "@anthropic-ai/gitlab-mcp-server",
        &[("api_token", "GITLAB_TOKEN"), ("base_url", "GITLAB_URL")],
    ),
    (
        "neon",
        "@neondatabase/mcp-server-neon",
        &[("api_key", "NEON_API_KEY")],
    ),
    (
        "turso",
        "@tursodatabase/turso-mcp",
        &[("api_token", "TURSO_API_TOKEN"), ("org_name", "TURSO_ORG")],
    ),
    ("snyk", "snyk-mcp-server", &[("api_token", "SNYK_TOKEN")]),
    (
        "launchdarkly",
        "@launchdarkly/mcp-server",
        &[("api_key", "LAUNCHDARKLY_ACCESS_TOKEN")],
    ),
    (
        "mongodb",
        "mongodb-mcp-server",
        &[
            ("public_key", "MDB_MCP_API_PUBLIC_KEY"),
            ("private_key", "MDB_MCP_API_PRIVATE_KEY"),
            ("group_id", "MDB_MCP_API_GROUP_ID"),
        ],
    ),
    (
        "grafana",
        "mcp-grafana",
        &[("api_key", "GRAFANA_API_KEY"), ("base_url", "GRAFANA_URL")],
    ),
    (
        "redis",
        "@redis/mcp-server",
        &[("connection_url", "REDIS_URL")],
    ),
    (
        "upstash",
        "@anthropic-ai/upstash-mcp-server",
        &[("email", "UPSTASH_EMAIL"), ("api_key", "UPSTASH_API_KEY")],
    ),
];

/// Main orchestration: run all passes for a new ticket.
pub async fn orchestrate_ticket(
    state: &WorkerState,
    mut msg: TicketMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A continuation reuses its original run_id (so it's ONE run, not a
    // duplicate); a fresh ticket mints a new one.
    let is_continuation = (msg.continuation > 0 || msg.model_retries > 0)
        && msg
            .continuation_run_id
            .as_deref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
    let run_id = msg
        .continuation_run_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| ulid::Ulid::new().to_string());
    let mut usage = TokenUsage::default();
    let start = std::time::Instant::now();

    info!(run_id, ticket_id = %msg.ticket_id, continuation = msg.continuation, "Orchestration started");

    // Dedup only FRESH runs — a continuation legitimately reuses its own run_id
    // and would otherwise see itself as "already running" and drop.
    if !is_continuation && is_ticket_already_running(state, &msg).await {
        warn!(
            run_id,
            ticket_id = %msg.ticket_id,
            "Skipping duplicate: another run for this ticket is already active"
        );
        return Ok(());
    }

    if is_continuation {
        // Reuse the existing run record — but ONLY if it isn't cancelled. This
        // is the hard guard that prevents auto-continuation from ever
        // resurrecting a run the user cancelled.
        if !resume_run_to_running(state, &msg.team_id, &run_id).await {
            info!(
                run_id,
                "Continuation aborted — run is cancelled or no longer exists"
            );
            return Ok(());
        }
    } else {
        create_run_record(state, &msg, &run_id).await?;
    }

    match tokio::time::timeout(
        std::time::Duration::from_secs(LAMBDA_TIMEOUT_SECS - 30), // 30s buffer for cleanup
        run_passes(state, &mut msg, &run_id, &mut usage, &start),
    )
    .await
    {
        Ok(Ok(_)) => {
            let duration = start.elapsed().as_secs();
            info!(run_id, duration, "Orchestration complete");
        }
        Ok(Err(e)) => {
            let duration = start.elapsed().as_secs();
            let err_msg = e.to_string();
            if err_msg.contains("cancelled by user") {
                info!(run_id, duration, "Run cancelled — preserving tokens");
                // Update token counts but keep status as cancelled
                let cost = usage.estimated_cost();
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.runs_table_name)
                    .key("team_id", attr_s(&msg.team_id))
                    .key("run_id", attr_s(&run_id))
                    .update_expression("SET tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, updated_at = :t")
                    .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                    .expression_attribute_values(":to", attr_n(usage.output_tokens))
                    .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                    .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                    .expression_attribute_values(":c", attr_n(format!("{cost:.4}")))
                    .expression_attribute_values(":d", attr_n(duration))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
                // The team's counters already have these tokens: each model
                // call is counted as it happens (common::usage).
            } else {
                error!(run_id, error = %e, "Orchestration failed");
                let checkpoint = load_checkpoint(state, &msg.team_id, &run_id).await;
                // Budget exhausted (hit the step limit) with committed partial
                // work is "needs more turns", not a real failure — auto-continue
                // (bounded) from the branch instead of salvaging a half-finished
                // PR the user sees as an error. Identical treatment to the
                // wall-clock timeout arm; only THIS error class continues, other
                // failures still salvage/fail below.
                // Only a genuine mid-implement step-budget death continues (the
                // salvage arm below uses the same "implement_started" signal). This
                // stays correct even if a future non-implement pass propagates the
                // same step-limit message — it won't match this checkpoint.
                let continued = is_budget_exhausted(&err_msg)
                    && try_auto_continue(state, &msg, &run_id, &checkpoint, &["implement_started"])
                        .await;
                // A transient model failure (5xx incl. 520-524, 429, 529, no
                // response) that outlasted the client's own retries is not the
                // ticket's fault: the run is sent again later (same run_id), by
                // the one requeue policy every pass uses. Decided from the
                // error's kind, never its text.
                let continued = continued || {
                    let err: &(dyn std::error::Error + 'static) = &*e;
                    common::model_retry::is_transient_model_error(err)
                        && requeue_after_model_error(state, &msg, &run_id).await
                };
                // Salvage before failing: an implement that burned its turn
                // budget mid-task has already pushed real commits to the branch.
                // Only mid-implement deaths salvage: for later-pass errors a
                // salvage would mark the run completed and hide the real error
                // behind a misleading "partial work" message.
                let salvaged = if continued {
                    false
                } else if let Some((ref last_pass, ref branch, _)) = checkpoint {
                    if !branch.is_empty() && last_pass.as_str() == "implement_started" {
                        info!(
                            run_id,
                            branch,
                            "Error mid-implement — attempting to salvage partial work as draft PR"
                        );
                        match salvage_timeout_pr(state, &msg, &run_id, branch, &usage, duration)
                            .await
                        {
                            Ok(pr_url) => {
                                info!(run_id, pr_url, "Salvaged partial work into draft PR");
                                true
                            }
                            Err(salvage_err) => {
                                warn!(run_id, error = %salvage_err, "Failed to salvage partial work");
                                false
                            }
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };
                if !continued && !salvaged {
                    let sanitized = sanitize_error(&err_msg);
                    fail_run(state, &msg, &run_id, &sanitized, &usage, duration).await;
                }
                // `?` failure paths inside run_passes bypass close_memory —
                // release the repo's memory lock so the next run isn't
                // stateless for the lock's full 15-minute TTL.
                crate::memory::lock::release_lock(
                    &state.dynamo,
                    &state.config.table_name,
                    &msg.team_id,
                    &msg.repo_owner,
                    &msg.repo_name,
                    None, // tokenless: only clears an EXPIRED lock
                )
                .await;
            }
        }
        Err(_elapsed) => {
            let duration = start.elapsed().as_secs();
            warn!(
                run_id,
                duration, "Orchestration timed out — Lambda limit approaching"
            );

            let checkpoint = load_checkpoint(state, &msg.team_id, &run_id).await;

            // Prefer AUTO-CONTINUATION over a partial PR: re-enqueue to resume
            // from the already-committed partial work (bounded — see helper). A
            // wall-clock timeout can strand real work in any late pass, so the
            // full continuable set applies here.
            let continued = try_auto_continue(
                state,
                &msg,
                &run_id,
                &checkpoint,
                &["implement", "implement_started", "security"],
            )
            .await;

            // Fallback (bounds hit, no queue, or enqueue failed): salvage the
            // partial work as a draft PR, else fail.
            if !continued {
                let salvaged = if let Some((ref last_pass, ref branch, _)) = checkpoint {
                    if !branch.is_empty()
                        && matches!(
                            last_pass.as_str(),
                            "implement" | "implement_started" | "security"
                        )
                    {
                        info!(
                            run_id,
                            branch, "Timeout — attempting to salvage partial work as draft PR"
                        );
                        match salvage_timeout_pr(state, &msg, &run_id, branch, &usage, duration)
                            .await
                        {
                            Ok(pr_url) => {
                                info!(run_id, pr_url, "Salvaged partial work into draft PR");
                                true
                            }
                            Err(e) => {
                                warn!(run_id, error = %e, "Failed to salvage partial work");
                                false
                            }
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !salvaged {
                    fail_run(
                        state,
                        &msg,
                        &run_id,
                        "Run timed out (exceeded execution time limit). The issue may need a narrower scope.",
                        &usage,
                        duration,
                    )
                    .await;
                }
            }
        }
    }

    // Release or extend the ticket lock based on final run status.
    // If the run went to awaiting_ci, extend the lock (CI may take a while).
    // Otherwise, release it so new runs can proceed.
    let is_awaiting_ci = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
    {
        Ok(out) => out
            .item()
            .and_then(|i| i.get("status").and_then(|v| v.as_s().ok().cloned()))
            .map(|s| s == "awaiting_ci")
            .unwrap_or(false),
        Err(_) => false,
    };

    if is_awaiting_ci {
        info!(run_id, ticket_id = %msg.ticket_id, "Extending ticket lock for CI wait");
        extend_ticket_lock(state, &msg.team_id, &msg.ticket_id, 30).await;
    } else {
        release_ticket_lock(state, &msg.team_id, &msg.ticket_id).await;
    }

    Ok(())
}

async fn run_passes(
    state: &WorkerState,
    msg: &mut TicketMessage,
    run_id: &str,
    usage: &mut TokenUsage,
    start: &std::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Resolve installation_id: if 0 (e.g. Jira-sourced tickets), look it up from team META
    if msg.installation_id == 0 {
        msg.installation_id = lookup_team_installation_id(state, &msg.team_id).await?;
        info!(
            run_id,
            installation_id = msg.installation_id,
            "Resolved installation_id from team META"
        );
    }

    // Load team's Anthropic API key (required)
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;

    // File read cache shared across passes to avoid redundant GitHub fetches
    let file_cache = FileCache::new();

    // Deadline for the implement pass — leave 150s buffer for PR creation + cleanup
    let implement_deadline =
        Some(*start + std::time::Duration::from_secs(LAMBDA_TIMEOUT_SECS.saturating_sub(150)));

    // Pre-run budget gate: reject if monthly token limit exceeded
    if let Some(reason) = check_budget_exceeded(state, &msg.team_id).await {
        warn!(run_id, team_id = %msg.team_id, %reason, "Run rejected: budget exceeded");
        let duration = start.elapsed().as_secs();
        fail_run(state, msg, run_id, &reason, usage, duration).await;
        return Err(reason.into());
    }

    // Initialize GitHub client for this team
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Load must-rules (global + repo-specific)
    let rules = load_rules(state, msg).await;

    // Load voice instructions from DynamoDB (repo-specific falls back to global)
    let voice = {
        let repo_voice = load_content(
            state,
            &msg.team_id,
            &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await;
        if repo_voice.is_empty() {
            load_content(state, &msg.team_id, "VOICE#GLOBAL").await
        } else {
            repo_voice
        }
    };

    // Load repo-level instruction files (AGENTS.md, CLAUDE.md, copilot-instructions, etc.)
    let mut repo_instructions =
        load_repo_instructions(&github, &msg.repo_owner, &msg.repo_name).await;
    // The team's own instructions, loaded once: every agent in the run that
    // plans, writes, proves or describes the change gets them from `msg`.
    msg.team_instructions =
        load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;

    // Post "working on it" comment only for GitHub-sourced tickets.
    let mut progress_comment_id: Option<u64> = None;
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let resp = github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format_progress("triage", run_id),
            )
            .await?;
        progress_comment_id = resp["id"].as_u64();
    }

    // The ticket's history with CoderHelm: earlier runs, how their PRs
    // ended, lessons, what people wrote on the ticket. Read before the repo
    // is chosen (a ticket whose PR is already in a repo stays there), and
    // the planner is shown it and held to it below.
    let mut history = ticket_history::load(state, &msg.team_id, &msg.ticket_id, run_id).await;
    ticket_history::add_pr_comments(&mut history, &github).await;

    // --- Resolve repo for Jira tickets ---
    // For Jira: explicit pin on the message > the repo of the ticket's own
    // pull request (CoderHelm's, merged or open, or one that names the
    // ticket) > project mapping > LLM select_repo > single repo. A repo the
    // ticket names (pinned, or plainly
    // written in the ticket) that is connected but not turned on for runs is
    // never swapped for another repo: the run stops as needs_input and says
    // so. validate_plan is the final safety net (tech-stack + plan-text checks).
    // Set when Coderhelm chose the repo itself (repo, why), so a no-changes
    // result can say where it looked.
    let mut auto_pick: Option<(String, String)> = None;
    if matches!(msg.source, TicketSource::Jira) {
        let team_repos = plan::fetch_team_repo_list(state, &msg.team_id).await;
        let repos: Vec<String> = team_repos
            .iter()
            .filter(|r| r.enabled)
            .map(|r| r.full_name.clone())
            .collect();
        let disabled: Vec<String> = team_repos
            .iter()
            .filter(|r| !r.enabled)
            .map(|r| r.full_name.clone())
            .collect();

        let mut resolved: Option<String> = None;
        let mut stop: Option<String> = None;

        // A retry or an explicit pin names the repo outright.
        if !msg.repo_owner.is_empty() && !msg.repo_name.is_empty() {
            let pin = format!("{}/{}", msg.repo_owner, msg.repo_name);
            match repo_pick::pin_state(&pin, &team_repos) {
                repo_pick::PinState::Enabled(r) => {
                    info!(run_id, repo = %r, "Repo pinned on the ticket message");
                    resolved = Some(r);
                }
                repo_pick::PinState::Disabled(r) => {
                    warn!(run_id, repo = %r, "Pinned repo is connected but not turned on for runs");
                    stop = Some(repo_pick::not_enabled_message(&r));
                }
                repo_pick::PinState::NotListed => {
                    match github.get_repo_info(&msg.repo_owner, &msg.repo_name).await {
                        Ok(_) => {
                            warn!(run_id, repo = %pin, "Pinned repo is visible to the app but not set up for runs");
                            stop = Some(repo_pick::not_enabled_message(&pin));
                        }
                        Err(e) if e.to_string().contains("client error") => {
                            warn!(run_id, repo = %pin, error = %e, "Pinned repo is not visible to the app");
                            stop = Some(repo_pick::not_connected_message(&pin));
                        }
                        Err(e) => {
                            return Err(format!(
                                "Could not check whether {pin} is connected (GitHub error: {e}). Retry the run."
                            )
                            .into());
                        }
                    }
                }
            }
        }

        if resolved.is_none() && stop.is_none() {
            if repos.is_empty() {
                // Nothing is turned on; say so plainly when the ticket names a
                // connected repo, otherwise keep the existing failure.
                let text = format!("{}\n{}", msg.title, msg.body);
                match repo_pick::ticket_names_only_disabled(&text, &team_repos) {
                    Some(r) => stop = Some(repo_pick::not_enabled_message(&r)),
                    None => {
                        return Err("No enabled repos found for team — cannot auto-pick repo".into())
                    }
                }
            } else if let Some((repo, why)) =
                repo_of_ticket_pr(&history, &repos, &github, &msg.ticket_id).await
            {
                // The ticket's work already lives in a repo: its pull request
                // is there. Planning anywhere else only to switch back was
                // three wasted plans on one ticket.
                info!(run_id, repo = %repo, why = %why, "Repo from the ticket's own pull request");
                auto_pick = Some((repo.clone(), why));
                resolved = Some(repo);
            } else {
                // Check for explicit Jira project → repo mapping
                let project_key = msg.ticket_id.split('-').next().unwrap_or("");
                let mapped_repo = if !project_key.is_empty() {
                    lookup_jira_project_repo(state, &msg.team_id, project_key).await
                } else {
                    None
                };
                if let Some(repo) = mapped_repo.filter(|m| repos.iter().any(|r| r == m)) {
                    info!(run_id, repo = %repo, project_key, "Repo from Jira project mapping");
                    resolved = Some(repo);
                }
            }
        }

        if resolved.is_none() && stop.is_none() {
            let text = format!("{}\n{}", msg.title, msg.body);
            if let Some(r) = repo_pick::ticket_names_only_disabled(&text, &team_repos) {
                // Deterministic: the ticket names exactly one connected repo,
                // by its exact name, and it isn't turned on.
                warn!(run_id, repo = %r, "Ticket names a repo that isn't turned on for runs");
                stop = Some(repo_pick::not_enabled_message(&r));
            } else if repos.len() == 1 && !repo_pick::mentions_disabled_repo(&text, &disabled) {
                // Only one repo and nothing hints at another — no need for LLM
                info!(run_id, repo = %repos[0], "Single repo available");
                auto_pick = Some((
                    repos[0].clone(),
                    "it is the only repo turned on for runs.".to_string(),
                ));
                resolved = Some(repos[0].clone());
            } else {
                // Ask the LLM to pick based on ticket content; it also judges
                // whether the ticket belongs in a repo that isn't turned on.
                match triage::select_repo(state, msg, &repos, &disabled, &github, &provider, usage)
                    .await
                {
                    Ok(repo_pick::RepoPick::Repo { repo, why }) => {
                        info!(run_id, repo = %repo, "LLM selected repo for Jira ticket");
                        auto_pick = Some((repo.clone(), why));
                        resolved = Some(repo);
                    }
                    Ok(repo_pick::RepoPick::NotEnabled(r)) => {
                        warn!(run_id, repo = %r, "Model judged the ticket belongs in a repo that isn't turned on");
                        stop = Some(repo_pick::not_enabled_message(&r));
                    }
                    Err(e) => {
                        // Fall back to first repo if LLM fails
                        warn!(run_id, error = %e, repo = %repos[0], "LLM repo selection failed, using first repo");
                        auto_pick = Some((
                            repos[0].clone(),
                            "the repo picker failed, so the first enabled repo was used."
                                .to_string(),
                        ));
                        resolved = Some(repos[0].clone());
                    }
                }
            }
        }

        if let Some(message) = stop {
            stop_run_repo_not_enabled(state, msg, run_id, usage, start, &message).await;
            return Ok(());
        }
        let repo = resolved.ok_or("Could not resolve a repo for this Jira ticket")?;
        let (owner, name) = repo.split_once('/').unwrap_or(("", ""));
        let changed = msg.repo_owner != owner || msg.repo_name != name;
        msg.repo_owner = owner.to_string();
        msg.repo_name = name.to_string();
        if changed {
            // Loaded above before the repo was known: the repo's own
            // instructions join the team-wide ones now.
            repo_instructions =
                load_repo_instructions(&github, &msg.repo_owner, &msg.repo_name).await;
            msg.team_instructions =
                load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
        }
    } else if msg.repo_owner.is_empty() || msg.repo_name.is_empty() {
        return Err("repo_owner and repo_name are required for GitHub tickets".into());
    }

    // The run record was written before a Jira ticket's repo was known (as
    // "/"); record the repo now, so the run shows under it in Runs, by-repo
    // analytics and the repo index.
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #repo = :r, team_repo = :tr")
        .condition_expression(
            "attribute_exists(run_id) AND (attribute_not_exists(#repo) OR #repo <> :r)",
        )
        .expression_attribute_names("#repo", "repo")
        .expression_attribute_values(":r", attr_s(&repo))
        .expression_attribute_values(":tr", attr_s(&format!("{}#{}", msg.team_id, repo)))
        .send()
        .await
    {
        let unchanged = e
            .as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false);
        if !unchanged {
            warn!(run_id, error = %e, "Could not record the run's repo");
        }
    }

    // Look up the repo's default branch (main, master, develop, etc.)
    let default_branch = match github
        .get_default_branch(&msg.repo_owner, &msg.repo_name)
        .await
    {
        Ok(branch) => {
            info!(run_id, branch = %branch, "Resolved default branch");
            branch
        }
        Err(e) => {
            // Check the repos table for a cached default_branch before giving up
            let cached = state
                .dynamo
                .get_item()
                .table_name(&state.config.repos_table_name)
                .key("pk", attr_s(&msg.team_id))
                .key(
                    "sk",
                    attr_s(&format!("REPO#{}/{}", msg.repo_owner, msg.repo_name)),
                )
                .projection_expression("default_branch")
                .send()
                .await
                .ok()
                .and_then(|r| r.item)
                .and_then(|item| {
                    item.get("default_branch")
                        .and_then(|v| v.as_s().ok())
                        .cloned()
                })
                .filter(|s| !s.is_empty());
            match cached {
                Some(branch) => {
                    warn!(run_id, error = %e, branch = %branch, "GitHub API failed, using cached default_branch");
                    branch
                }
                None => {
                    return Err(format!(
                        "Cannot resolve default branch for {}/{}: {e}. Check GitHub installation permissions.",
                        msg.repo_owner, msg.repo_name
                    ).into());
                }
            }
        }
    };
    msg.base_branch = default_branch;

    // create_run_record() ran BEFORE run_passes and persisted the UNRESOLVED
    // incoming base_branch, which defaults to `main`. Now that the repo's real
    // default is resolved (e.g. `develop`), write it back so the stored record
    // matches the branch the PR will actually target. A stale `main` on the record
    // is precisely what let a `develop`-based PR's fix loop merge `main` and churn
    // out-of-scope files endlessly. Best-effort — the fix loop also resolves the
    // base from the live PR directly, so this is defence in depth, not the only
    // guard.
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET base_branch = :b")
        .expression_attribute_values(":b", attr_s(&msg.base_branch))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Failed to persist resolved base_branch to run record");
    }

    // Load enabled MCP plugins once for this run
    let mcp_table = if state.config.mcp_configs_table_name.is_empty() {
        &state.config.settings_table_name
    } else {
        &state.config.mcp_configs_table_name
    };
    let mcp_plugins = mcp::load_team_plugins(
        &state.dynamo,
        &state.kms,
        mcp_table,
        &msg.team_id,
        &MCP_CATALOG,
    )
    .await;
    let mcp_server_ids: Vec<String> = mcp_plugins.iter().map(|p| p.server_id.clone()).collect();
    if !mcp_server_ids.is_empty() {
        info!(run_id, servers = ?mcp_server_ids, "MCP servers active for run");
    }

    // --- Checkpoint Resume: skip expensive passes if we've already completed them ---
    let checkpoint = load_checkpoint(state, &msg.team_id, run_id).await;
    let can_skip_implement = checkpoint.as_ref().is_some_and(|(last_pass, branch, _)| {
        !branch.is_empty()
            && matches!(
                last_pass.as_str(),
                "implement" | "security" | "pr" | "test" | "review"
            )
    });

    if let Some((ref last_pass, ref branch, ref cycle)) = checkpoint {
        info!(run_id, last_pass, branch, cycle, "Found checkpoint for run");
    }

    // --- Open agent memory (non-blocking: falls back to stateless if unavailable) ---
    let mut agent_memory =
        AgentMemory::open(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let memory_context = match agent_memory.as_mut() {
        Some(mem) => {
            let ctx = mem
                .recall_context(&format!("{} {} {}", msg.title, msg.body, msg.repo_name), 5)
                .await;
            record_memory_use(state, msg, run_id, &mem.last_recalled).await;
            ctx
        }
        None => String::new(),
    };

    // --- A continuation of this same run (Lambda time limit, step budget,
    // a transient model error) picks up this run's own triage and plan: the
    // work on the branch was made against them. Triaging and planning again
    // gave a different plan (memory and history read differently a few
    // minutes later) for a branch built on the first one.
    let same_run = msg.continuation > 0 || msg.model_retries > 0;
    let own_plan = if same_run {
        load_run_plan(state, msg, run_id).await
    } else {
        None
    };
    let resumed_plan = own_plan.is_some();

    let mut used_mcp_ids: Vec<String> = Vec::new();
    let (mut triage_result, own_plan) = match own_plan {
        Some(saved) => {
            info!(
                run_id,
                "Continuation: reusing this run's own triage and plan"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                "Continued with this run's own plan",
            )
            .await;
            used_mcp_ids = saved.used_mcp_ids;
            (saved.triage, Some(saved.plan))
        }
        None => {
            // --- Pass 1: Triage ---
            check_cancelled(state, &msg.team_id, run_id).await?;
            update_pass(state, &msg.team_id, run_id, "triage").await?;
            let pass_start = std::time::Instant::now();
            let usage_before = usage.clone();
            let mut triage_result = triage::run(state, msg, &github, &provider, usage).await?;
            write_pass_trace(
                state,
                &msg.team_id,
                run_id,
                "triage",
                pass_start,
                &usage_before,
                usage,
                None,
            )
            .await;
            save_checkpoint(state, &msg.team_id, run_id, "triage", "", 0, usage).await;
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Triaged as {} complexity", triage_result.complexity),
            )
            .await;
            info!(run_id, "Triage complete");

            // --- Resolve external references via MCP tools ---
            if !mcp_plugins.is_empty() && !state.config.mcp_proxy_function_name.is_empty() {
                let external_context =
                    resolve::run(state, msg, &mcp_plugins, &provider, usage).await;
                if !external_context.is_empty() {
                    triage_result.summary.push_str(&external_context);
                    used_mcp_ids = mcp_server_ids.clone();
                    info!(run_id, "Resolved external references via MCP");
                }
            }

            // Inject agent memory context into triage summary
            if !memory_context.is_empty() {
                triage_result.summary.push_str("\n\n");
                triage_result.summary.push_str(&memory_context);
                info!(run_id, "Injected agent memory context");
            }
            save_run_triage(state, msg, run_id, &triage_result, &used_mcp_ids).await;
            (triage_result, None)
        }
    };
    // May be upgraded by the plan-size valve below — triage classifies blind.
    let mut effective_complexity = triage_result.complexity.clone();
    update_progress_comment(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        progress_comment_id,
        "plan",
        run_id,
    )
    .await;

    // --- Pass 2: Plan ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "plan").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();

    // Compute context hash to detect ticket changes (body, images)
    let context_hash = plan::compute_ticket_context_hash(msg);

    // The ticket's history (read before the repo was chosen) goes to the
    // planner.
    if !history.is_empty() {
        msg.ticket_history_note = Some(history.planner_note());
        info!(
            run_id,
            earlier_runs = history.runs.len(),
            closed_unmerged = history.closed_unmerged().len(),
            notes = history.notes.len(),
            "Ticket has history — the planner gets it"
        );
    }
    // The cached plan is reused only when nothing that can change it has
    // changed: the ticket, what memory recalled (lessons), the history, and
    // the planner itself.
    let plan_key = plan::plan_cache_key(
        &context_hash,
        &memory_context,
        &history.digest(),
        &msg.team_instructions,
    );
    msg.plan_cache_key = Some(plan_key.clone());

    // Try to reuse existing plan from S3 (same ticket, repo, and content).
    // A continuation already has its own plan and never looks it up by key.
    let lookup = if resumed_plan {
        PlanLookup {
            plan: None,
            hash_changed: false,
            previous_ticket_text: None,
        }
    } else {
        load_existing_plan(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &msg.repo_owner,
            &msg.repo_name,
            &context_hash,
            &plan_key,
        )
        .await
    };
    // The ticket changed ONLY when a stored hash exists and differs. A missing
    // plan (first run) or a finished plan (no unchecked tasks) is not a ticket
    // change; treating it as one sent untouched re-runs down the "ticket
    // updated" path.
    let context_changed = lookup.hash_changed;
    let previous_ticket_text = lookup.previous_ticket_text;
    let mut plan_was_reused = false;
    let mut plan_result = if let Some(plan) = own_plan {
        plan_was_reused = true;
        plan
    } else if let Some(plan) = lookup.plan {
        // Reuse if plan has content AND has unchecked tasks (not a completed plan from a previous run)
        let has_unchecked = plan.tasks.contains("- [ ]");
        if !plan.tasks.is_empty() && !plan.design.is_empty() && has_unchecked {
            plan_was_reused = true;
            info!(run_id, "Reusing existing plan from S3 — ticket unchanged");
            add_progress_note(state, &msg.team_id, run_id, "Reused existing plan").await;
            // A continuation of this run picks this plan up as its own.
            plan::mark_plan_run(state, msg, run_id).await;
            plan
        } else {
            plan::run(
                state,
                msg,
                &github,
                &triage_result,
                &repo_instructions,
                &provider,
                usage,
                run_id,
            )
            .await?
        }
    } else {
        plan::run(
            state,
            msg,
            &github,
            &triage_result,
            &repo_instructions,
            &provider,
            usage,
            run_id,
        )
        .await?
    };
    if !resumed_plan {
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            "plan",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        // A continuation keeps the checkpoint it resumed from (its branch):
        // overwriting it with "plan" would strand the work if it timed out
        // again before implement saved a new one.
        save_checkpoint(state, &msg.team_id, run_id, "plan", "", 0, usage).await;
        add_progress_note(state, &msg.team_id, run_id, "Plan generated").await;
    }
    info!(run_id, "Plan complete");
    update_progress_comment(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        progress_comment_id,
        "implement",
        run_id,
    )
    .await;

    // --- Repo check, before anything is decided on this plan ---
    // A plan written against the wrong repo (its files live in another
    // one) is planned again in the right repo before its decision counts or
    // a branch is made; a plan naming files that don't exist is planned
    // again once, and never implemented while they still don't.
    // A continuation's plan passed this check when the run first made it.
    let mut early_validation: Option<PlanValidation> = None;
    if !resumed_plan && !plan_result.tasks.trim().is_empty() {
        let team_repos = plan::fetch_team_repos(state, &msg.team_id).await;
        let mut v = validate_plan(
            &plan_result,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &msg.base_branch,
            state,
            &msg.team_id,
            &team_repos,
            &provider,
            usage,
        )
        .await;
        if let Some(better_repo) = v.switch_repo.take() {
            if switch_run_repo(state, msg, run_id, &github, &better_repo, &v.switch_reason).await {
                plan_was_reused = false;
                // The repo the run uses now is the one CoderHelm picked; a
                // note naming the repo it left would point people at the
                // wrong place.
                auto_pick = Some(repo_pick::switched_pick(&better_repo, &v.switch_reason));
                // The new repo's own instructions and memory (its lessons)
                // go into the plan made there.
                repo_instructions =
                    load_repo_instructions(&github, &msg.repo_owner, &msg.repo_name).await;
                msg.team_instructions =
                    load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name)
                        .await;
                close_memory(state, agent_memory.take()).await;
                agent_memory =
                    AgentMemory::open(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
                if let Some(mem) = agent_memory.as_mut() {
                    let ctx = mem
                        .recall_context(&format!("{} {} {}", msg.title, msg.body, msg.repo_name), 5)
                        .await;
                    record_memory_use(state, msg, run_id, &mem.last_recalled).await;
                    if !ctx.is_empty() {
                        triage_result.summary.push_str("\n\n");
                        triage_result.summary.push_str(&ctx);
                    }
                    msg.plan_cache_key = Some(plan::plan_cache_key(
                        &context_hash,
                        &format!("{memory_context}{ctx}"),
                        &history.digest(),
                        &msg.team_instructions,
                    ));
                }
                plan_result = plan::run(
                    state,
                    msg,
                    &github,
                    &triage_result,
                    &repo_instructions,
                    &provider,
                    usage,
                    run_id,
                )
                .await?;
                v = validate_plan(
                    &plan_result,
                    &github,
                    &msg.repo_owner,
                    &msg.repo_name,
                    &msg.base_branch,
                    state,
                    &msg.team_id,
                    &team_repos,
                    &provider,
                    usage,
                )
                .await;
                if let Some(again) = v.switch_repo.take() {
                    warn!(run_id, suggested = %again, "Repo check suggests switching again — staying");
                }
            }
        }
        let codes = !plan_result.tasks.starts_with("NO_CHANGES_NEEDED:")
            && !plan_result.tasks.starts_with("CLARIFICATION_NEEDED:")
            && matches!(
                fix_location::route_for_plan(plan_result.fix.as_ref(), &plan_result.tasks),
                fix_location::Route::Code | fix_location::Route::CodeAndOutside
            );
        if codes && !v.missing_files.is_empty() {
            info!(
                run_id,
                missing = ?v.missing_files,
                "Plan names files that don't exist — planning again"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!(
                    "Plan named {} files that don't exist in {}/{} — planning again",
                    v.missing_files.len(),
                    msg.repo_owner,
                    msg.repo_name
                ),
            )
            .await;
            let mut corrected = msg.clone();
            corrected.body.push_str(&missing_files_note(
                &v.missing_files,
                &format!("{}/{}", msg.repo_owner, msg.repo_name),
            ));
            plan_was_reused = false;
            plan_result = plan::run(
                state,
                &corrected,
                &github,
                &triage_result,
                &repo_instructions,
                &provider,
                usage,
                run_id,
            )
            .await?;
            restore_plan_hash(state, msg, &context_hash).await;
            v = validate_plan(
                &plan_result,
                &github,
                &msg.repo_owner,
                &msg.repo_name,
                &msg.base_branch,
                state,
                &msg.team_id,
                &team_repos,
                &provider,
                usage,
            )
            .await;
            v.switch_repo = None;
            if !v.missing_files.is_empty() {
                warn!(run_id, missing = ?v.missing_files, "Plan still names files that don't exist — not implementing");
                let decision = fix_location::FixDecision {
                    fix_location: fix_location::FixLocation::Unknown,
                    cause: format!(
                        "The plan for this ticket names files that don't exist in {}/{} ({}), \
                         even after planning again, so I stopped before changing any code. \
                         The ticket may belong in another repository.",
                        msg.repo_owner,
                        msg.repo_name,
                        v.missing_files.join(", ")
                    ),
                    ..Default::default()
                };
                finish_without_code(
                    state,
                    msg,
                    run_id,
                    &github,
                    &decision,
                    fix_location::Route::Ask,
                    usage,
                    start,
                    &used_mcp_ids,
                )
                .await;
                close_memory(state, agent_memory.take()).await;
                return Ok(());
            }
        }
        early_validation = Some(v);
    }

    // --- Guard: a malformed/truncated plan must not proceed to implement ---
    // parse_openspec_files returns empty strings for any fence the model
    // omitted; an empty task list used to flow straight into implement and
    // produce a guaranteed zero-change run.
    // Where the fix belongs, as the planner decided it from what it looked up.
    let mut fix_route = fix_location::route_for_plan(plan_result.fix.as_ref(), &plan_result.tasks);
    // The ticket's history holds the plan: no second pull request after
    // CoderHelm learned the fix is outside the code, or after its PR was
    // closed unmerged, unless the plan answers why (see ticket_history).
    if !plan_result.tasks.starts_with("NO_CHANGES_NEEDED:")
        && !plan_result.tasks.starts_with("CLARIFICATION_NEEDED:")
    {
        if let ticket_history::Guard::Stop {
            route,
            decision,
            why,
        } = history.guard(plan_result.fix.as_ref(), &context_hash)
        {
            info!(run_id, why = %why, "Ticket history holds the plan — no code change");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("No new pull request: {why}"),
            )
            .await;
            plan_result.fix = Some(*decision);
            fix_route = route;
        }
    }
    let plan_is_directive = plan_result.tasks.starts_with("NO_CHANGES_NEEDED:")
        || plan_result.tasks.starts_with("CLARIFICATION_NEEDED:")
        || matches!(
            fix_route,
            fix_location::Route::OutsideOnly | fix_location::Route::Ask
        );
    if !plan_is_directive
        && (plan_result.tasks.trim().is_empty() || plan_result.design.trim().is_empty())
    {
        warn!(
            run_id,
            "Plan generation produced empty tasks/design — failing run"
        );
        close_memory(state, agent_memory.take()).await;
        return Err(
            "Plan generation produced an empty or malformed plan (missing tasks/design). \
             This is usually a transient model formatting issue — retry the run."
                .into(),
        );
    }

    // --- Check: is the fix outside the code? ---
    // The code works as designed and the outcome comes from data or settings
    // in another system: report the change to make there, open no PR. Not
    // sure which: report what was found and ask, rather than code around it.
    if !plan_result.tasks.starts_with("NO_CHANGES_NEEDED:")
        && !plan_result.tasks.starts_with("CLARIFICATION_NEEDED:")
        && matches!(
            fix_route,
            fix_location::Route::OutsideOnly | fix_location::Route::Ask
        )
    {
        let mut decision = plan_result
            .fix
            .clone()
            .unwrap_or_else(fix_location::undecided);
        if decision.cause.trim().is_empty() {
            // A plan that codes nothing but wrote no decision: its one line
            // is what it found.
            decision.cause = plan_result
                .tasks
                .trim_start()
                .strip_prefix(fix_location::NO_CODE_CHANGE)
                .unwrap_or("")
                .trim()
                .to_string();
        }
        finish_without_code(
            state,
            msg,
            run_id,
            &github,
            &decision,
            fix_route,
            usage,
            start,
            &used_mcp_ids,
        )
        .await;
        close_memory(state, agent_memory.take()).await;
        return Ok(());
    }

    // A code change AND a change outside the code: the ticket hears about the
    // outside part now (the PR body carries it too), so a person can make it
    // while the code is written.
    if fix_route == fix_location::Route::CodeAndOutside {
        if let Some(decision) = plan_result.fix.as_ref() {
            report_also_outside(state, msg, run_id, &github, decision).await;
        }
    } else if fix_route == fix_location::Route::Code {
        // The reviewer reads what the planner found when it judges the PR.
        if let Some(decision) = plan_result.fix.as_ref() {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression("SET fix_location = :fl, fix_decision = :fd")
                .expression_attribute_values(":fl", attr_s(decision.effective_location().as_str()))
                .expression_attribute_values(
                    ":fd",
                    attr_s(&serde_json::to_string(decision).unwrap_or_default()),
                )
                .send()
                .await;
        }
    }

    // --- Check: already done? ---
    if plan_result.tasks.starts_with("NO_CHANGES_NEEDED:") {
        let reason = plan_result
            .tasks
            .strip_prefix("NO_CHANGES_NEEDED:")
            .unwrap_or("")
            .trim();
        info!(run_id, reason, "Plan determined no changes needed");

        let raw_github = format!(
            "✅ **No changes needed**\n\n{reason}\n\nThe codebase already satisfies what this issue asks for."
        );
        let github_comment =
            formatter::format_with_voice(state, &voice, &raw_github, &provider, usage).await;
        let raw_jira =
            format!("{reason}\n\nThe codebase already satisfies what this issue asks for.");
        let mut jira_comment =
            formatter::format_with_voice(state, &voice, &raw_jira, &provider, usage).await;
        let mut github_comment = github_comment;
        // When Coderhelm picked the repo itself, say which one it looked at and
        // why, so a wrong-repo pick is visible instead of reading as "done".
        // Appended after voice formatting so it can't be reworded away.
        if let Some((repo, why)) = auto_pick.as_ref() {
            let note = repo_pick::no_changes_repo_note(repo, why);
            add_progress_note(state, &msg.team_id, run_id, &note).await;
            github_comment = format!("{github_comment}\n\n{note}");
            jira_comment = format!("{jira_comment}\n\n{note}");
        }

        if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &github_comment,
                )
                .await;
        } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
            let _ = post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &jira_comment,
                "no_changes",
                "No changes needed",
            )
            .await;
        }

        // Complete the run with no PR
        let duration = start.elapsed().as_secs();
        let cost = usage.estimated_cost();
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(run_id))
            .update_expression(
                "SET #status = :s, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
                 duration_s = :d, updated_at = :t, current_pass = :cp, \
                 status_run_id = :sri, mcp_servers = :mcp",
            )
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":s", attr_s("completed"))
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
            .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
            .expression_attribute_values(":d", attr_n(duration))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":cp", attr_s("done"))
            .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
            .expression_attribute_values(
                ":mcp",
                AttributeValue::L(used_mcp_ids.iter().map(|s| attr_s(s)).collect()),
            )
            .send()
            .await;

        // Update analytics counters
        let analytics_month = chrono::Utc::now().format("%Y-%m").to_string();
        for period in &[analytics_month.as_str(), "ALL_TIME"] {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.analytics_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("period", attr_s(period))
                .update_expression("ADD completed :one")
                .expression_attribute_values(":one", attr_n(1))
                .send()
                .await;
        }

        close_memory(state, agent_memory.take()).await;
        return Ok(());
    }

    // --- Check: clarification needed? ---
    if plan_result.tasks.starts_with("CLARIFICATION_NEEDED:") {
        let detail = plan_result
            .tasks
            .strip_prefix("CLARIFICATION_NEEDED:")
            .unwrap_or("")
            .trim();
        info!(
            run_id,
            detail, "Plan needs clarification from the ticket author"
        );

        let raw_github = format!(
            "❓ **Clarification needed**\n\n{detail}\n\nPlease update the issue with the missing details and re-trigger the run."
        );
        let github_comment =
            formatter::format_with_voice(state, &voice, &raw_github, &provider, usage).await;
        let raw_jira = format!(
            "{detail}\n\nPlease update the issue with the missing details and comment to re-trigger."
        );
        let jira_comment =
            formatter::format_with_voice(state, &voice, &raw_jira, &provider, usage).await;

        if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &github_comment,
                )
                .await;
        } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
            let _ = post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &jira_comment,
                "clarification",
                "Clarification needed",
            )
            .await;
        }

        // Mark the run as needs_input so a comment can re-trigger it
        mark_run_needs_input(state, msg, run_id, usage, start, &used_mcp_ids).await;

        close_memory(state, agent_memory.take()).await;
        return Ok(());
    }

    // --- Pass 3: Implement ---
    // The team's branch/commit/PR title templates, read once for the run.
    let conventions = load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
    let tokens = ticket_tokens(msg);
    let branch_name = if can_skip_implement {
        checkpoint.as_ref().unwrap().1.clone()
    } else {
        resolve_run_branch(
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            &conventions,
            &tokens,
            "",
        )
        .await
    };

    let impl_result = if can_skip_implement {
        // Resume: reconstruct impl_result from the existing branch diff
        info!(run_id, branch = %branch_name, "Checkpoint resume: skipping implement pass");
        update_pass(state, &msg.team_id, run_id, "implement").await?;

        let diff_json = github
            .get_diff(
                &msg.repo_owner,
                &msg.repo_name,
                &msg.base_branch,
                &branch_name,
            )
            .await?;
        let files_modified: Vec<String> = diff_json["files"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        info!(
            run_id,
            files = files_modified.len(),
            "Reconstructed files_modified from branch diff"
        );
        implement::ImplementResult {
            files_modified,
            conversation_log: vec![],
            // The first attempt's lookups weren't kept; the proof check's
            // fix round can make them again.
            evidence: proof::RunEvidence {
                notes: "(This run resumed from a checkpoint: the first attempt's lookups and \
                        notes are not available.)"
                    .to_string(),
                ..Default::default()
            },
        }
    } else {
        check_cancelled(state, &msg.team_id, run_id).await?;
        update_pass(state, &msg.team_id, run_id, "implement").await?;

        // --- Re-run detection: check if branch already has an open PR ---
        // If an open PR exists, we always try to fix it rather than starting fresh.
        // Previous retries may have force-reset the branch to base, so we also check
        // for prior runs of the same ticket that were awaiting_ci (evidence of CI failures).
        // Track whether the PR lookup actually SUCCEEDED. A transient API
        // failure used to collapse to `None` (indistinguishable from "no PR"),
        // which then flipped skip_branch_reset=false → force-reset the branch
        // to base, wiping any real work on it. On a failed lookup we must NOT
        // reset.
        let mut pr_lookup_failed = false;
        let existing_pr = match github
            .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, &branch_name)
            .await
        {
            Ok(pr) => {
                info!(run_id, branch = %branch_name, found = pr.is_some(), "Re-run detection: checked for open PR");
                pr
            }
            Err(e) => {
                warn!(run_id, branch = %branch_name, error = %e, "Re-run detection: failed to check for open PR — will NOT reset branch");
                pr_lookup_failed = true;
                None
            }
        };
        // Someone else's open pull request for this ticket (a person's, or an
        // earlier run's on another branch): don't open a second one unless a
        // person said to go ahead.
        if existing_pr.is_none() && msg.person_direction.is_none() {
            if let Some(other) = open_pr_for_ticket(
                &github,
                &msg.repo_owner,
                &msg.repo_name,
                &msg.ticket_id,
                &branch_name,
            )
            .await
            {
                let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
                stop_for_open_pr(state, msg, &github, run_id, usage, start, &repo, &other).await;
                close_memory(state, agent_memory.take()).await;
                return Ok(());
            }
        }

        if let Some(ref pr) = existing_pr {
            let pr_number = pr["number"].as_u64().unwrap_or(0);
            let pr_url = pr["html_url"].as_str().unwrap_or("").to_string();
            let pr_head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
            let base_sha = pr["base"]["sha"].as_str().unwrap_or("").to_string();
            let check_ref = if pr_head_sha.is_empty() {
                branch_name.clone()
            } else {
                pr_head_sha.clone()
            };

            // Detect if the branch was force-reset to base (no diff from base = previous retry wiped it)
            let branch_is_reset =
                !pr_head_sha.is_empty() && !base_sha.is_empty() && pr_head_sha == base_sha;

            // Check CI status on the PR's head commit
            let checks = github
                .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &check_ref)
                .await
                .unwrap_or_default();
            let failed_conclusions = ["failure", "action_required", "timed_out", "startup_failure"];
            let has_ci_failure = checks["check_runs"]
                .as_array()
                .map(|runs| {
                    runs.iter().any(|r| {
                        r["conclusion"]
                            .as_str()
                            .is_some_and(|c| failed_conclusions.contains(&c))
                    })
                })
                .unwrap_or(false);

            // Check for pending review comments on the PR
            let review_comments = github
                .get_review_comments(&msg.repo_owner, &msg.repo_name, pr_number)
                .await
                .unwrap_or_default();
            let pending_reviews: Vec<&serde_json::Value> = review_comments
                .iter()
                .filter(|c| {
                    let author = c["user"]["login"].as_str().unwrap_or("");
                    !author.contains("[bot]") && !author.contains("coderhelm")
                })
                .collect();
            let has_review_feedback = !pending_reviews.is_empty();

            info!(
                run_id, pr_number, has_ci_failure, has_review_feedback,
                review_count = pending_reviews.len(),
                check_ref = %check_ref,
                branch_is_reset,
                "Re-run detection: PR status"
            );

            if (has_ci_failure || has_review_feedback) && !context_changed {
                // Build combined feedback from CI failures and review comments
                let mut feedback_parts: Vec<String> = Vec::new();

                if has_ci_failure {
                    add_progress_note(
                        state,
                        &msg.team_id,
                        run_id,
                        &format!("Existing PR #{pr_number} has CI failures — fixing"),
                    )
                    .await;

                    // Get the failed workflow run logs
                    let failed_run_id = checks["check_runs"]
                        .as_array()
                        .and_then(|runs| {
                            runs.iter()
                                .filter(|r| {
                                    r["conclusion"]
                                        .as_str()
                                        .is_some_and(|c| failed_conclusions.contains(&c))
                                })
                                .filter_map(|r| {
                                    r["details_url"]
                                        .as_str()
                                        .and_then(|u| u.split("/runs/").nth(1))
                                        .and_then(|s| s.split('/').next())
                                        .and_then(|s| s.parse::<u64>().ok())
                                })
                                .next_back()
                        })
                        .unwrap_or(0);

                    let logs = if failed_run_id > 0 {
                        match github
                            .get_workflow_run_logs(&msg.repo_owner, &msg.repo_name, failed_run_id)
                            .await
                        {
                            Ok(l) => l,
                            Err(_) => {
                                // Fallback: check run annotations (uses checks:read, not actions:read)
                                github
                                    .get_check_run_annotations(
                                        &msg.repo_owner,
                                        &msg.repo_name,
                                        &branch_name,
                                    )
                                    .await
                                    .unwrap_or_else(|_| "(failed to download logs)".to_string())
                            }
                        }
                    } else {
                        let job_id = checks["check_runs"]
                            .as_array()
                            .and_then(|runs| {
                                runs.iter()
                                    .find(|r| {
                                        r["conclusion"]
                                            .as_str()
                                            .is_some_and(|c| failed_conclusions.contains(&c))
                                    })
                                    .and_then(|r| r["id"].as_u64())
                            })
                            .unwrap_or(0);
                        if job_id > 0 {
                            github
                                .get_check_run_logs(&msg.repo_owner, &msg.repo_name, job_id)
                                .await
                                .unwrap_or_else(|_| "(failed to download logs)".to_string())
                        } else {
                            "(no failed check runs found)".to_string()
                        }
                    };

                    let max_log = 15_000;
                    let trimmed_logs = if logs.len() > max_log {
                        common::head_tail_str(&logs, max_log)
                    } else {
                        logs
                    };

                    feedback_parts.push(format!(
                        "CI workflow failed on PR #{pr_number} (branch: {branch_name}). Fix the failures.\n\n\
                         Rules:\n\
                         - Only fix what CI is complaining about. Don't refactor or add features.\n\
                         - If the failure is in a test, fix the code (not the test) unless the test itself is wrong.\n\
                         - If tests need updating because the feature changed behavior intentionally, update the tests.\n\
                         - You may create new test files or edit existing ones if needed.\n\n\
                         Full logs:\n{trimmed_logs}"
                    ));
                }

                if has_review_feedback {
                    add_progress_note(
                        state,
                        &msg.team_id,
                        run_id,
                        &format!(
                            "Existing PR #{pr_number} has {} review comment(s) — addressing",
                            pending_reviews.len()
                        ),
                    )
                    .await;

                    let mut review_text = format!(
                        "PR #{pr_number} has review comments that need to be addressed. \
                         Fix each comment by making the requested changes.\n\n"
                    );
                    for (i, comment) in pending_reviews.iter().enumerate().take(20) {
                        let path = comment["path"].as_str().unwrap_or("(unknown file)");
                        let line = comment["line"].as_u64().unwrap_or(0);
                        let body = comment["body"].as_str().unwrap_or("");
                        let author = comment["user"]["login"].as_str().unwrap_or("reviewer");
                        review_text.push_str(&format!(
                            "Comment {} by @{author} on `{path}`:{line}:\n{body}\n\n",
                            i + 1
                        ));
                    }
                    feedback_parts.push(review_text);
                }

                let feedback = feedback_parts.join("\n---\n\n");

                info!(
                    run_id,
                    pr_number,
                    has_ci_failure,
                    has_review_feedback,
                    "Re-run detected: existing PR needs fixes — entering fix mode"
                );

                let file_cache = FileCache::default();
                let pass_start = std::time::Instant::now();
                let usage_before = usage.clone();

                match implement::run(
                    state,
                    msg,
                    &github,
                    &plan_result,
                    &branch_name,
                    &[],
                    "",
                    Some(&feedback),
                    "medium",
                    &provider,
                    usage,
                    &file_cache,
                    Some(run_id),
                    None,
                    // Scoped only when purely CI-driven; human review comments
                    // in the feedback mean the human's ask wins (unscoped).
                    !has_review_feedback,
                )
                .await
                {
                    Ok(result) => {
                        write_pass_trace(
                            state,
                            &msg.team_id,
                            run_id,
                            "ci_fix",
                            pass_start,
                            &usage_before,
                            usage,
                            None,
                        )
                        .await;
                        write_conversation_log(
                            state,
                            &msg.team_id,
                            run_id,
                            "ci_fix",
                            &result.conversation_log,
                        )
                        .await;
                        info!(
                            run_id,
                            files = result.files_modified.len(),
                            "Fix implemented on re-run"
                        );

                        // Store PR info on run record and go to awaiting_ci
                        let now = chrono::Utc::now().to_rfc3339();
                        let cost = usage.estimated_cost();
                        let duration = start.elapsed().as_secs();
                        let _ = state.dynamo.update_item()
                            .table_name(&state.config.runs_table_name)
                            .key("team_id", attr_s(&msg.team_id))
                            .key("run_id", attr_s(run_id))
                            .update_expression(
                                "SET #s = :s, status_run_id = :sri, current_pass = :cp, \
                                 pr_number = :pn, pr_url = :pu, branch = :b, \
                                 repo = :repo, team_repo = :tr, \
                                 tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
                                 cost_usd = :cost, duration_s = :d, updated_at = :t"
                            )
                            .expression_attribute_names("#s", "status")
                            // Never resurrect a cancelled run: if the user cancelled
                            // mid-cycle, this terminal write must be a no-op rather
                            // than flip the run back to awaiting_ci and re-trigger the
                            // loop. (This was the resurrection hole — a re-run fix
                            // cycle finished after a cancel and clobbered it.)
                            .condition_expression("#s <> :cancelled")
                            .expression_attribute_values(":cancelled", attr_s("cancelled"))
                            .expression_attribute_values(":s", attr_s("awaiting_ci"))
                            .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
                            .expression_attribute_values(":cp", attr_s("awaiting_ci"))
                            .expression_attribute_values(":pn", attr_n(pr_number))
                            .expression_attribute_values(":pu", attr_s(&pr_url))
                            .expression_attribute_values(":b", attr_s(&branch_name))
                            .expression_attribute_values(":repo", attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)))
                            .expression_attribute_values(":tr", attr_s(&format!("{}#{}/{}", msg.team_id, msg.repo_owner, msg.repo_name)))
                            .expression_attribute_values(":t", attr_s(&now))
                            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                            .expression_attribute_values(":to", attr_n(usage.output_tokens))
                            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                            .expression_attribute_values(":cost", attr_n(format!("{:.4}", cost)))
                            .expression_attribute_values(":d", attr_n(duration))
                            .send()
                            .await;

                        save_checkpoint(state, &msg.team_id, run_id, "pr", &branch_name, 1, usage)
                            .await;
                        close_memory(state, agent_memory.take()).await;
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(run_id, error = %e, "Fix on re-run failed, falling through to fresh start");
                        // Fall through to reset branch and start fresh
                    }
                }
            }
        }

        // Set when the ticket changed under an open PR: this run updates that
        // PR in place instead of starting over.
        let mut ticket_update_pr: Option<u64> = None;
        // Track whether to skip branch creation (PR exists, just needs fresh implementation)
        let skip_branch_reset = if pr_lookup_failed {
            // Couldn't confirm PR state — never force-reset (would risk wiping
            // real work on the branch on a transient API error).
            info!(
                run_id,
                "PR lookup failed — skipping branch reset to protect existing work"
            );
            true
        } else if context_changed && existing_pr.is_some() {
            // The ticket changed while a PR is open. A branch with an open PR
            // is never reset to base (create_branch keeps its head so GitHub
            // doesn't auto-close the PR), so "resetting" here only pretended:
            // the agent then found the old work on the branch and made no
            // edits, or stacked a second implementation. Treat it as what it
            // is: update the existing PR to match the new ticket.
            let pr_number = existing_pr
                .as_ref()
                .and_then(|p| p["number"].as_u64())
                .unwrap_or(0);
            info!(
                run_id,
                pr_number, "Ticket changed with an open PR — updating the PR to match"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Ticket updated — updating PR #{pr_number} to match"),
            )
            .await;
            let pr_diff = match github
                .get_diff(
                    &msg.repo_owner,
                    &msg.repo_name,
                    &msg.base_branch,
                    &branch_name,
                )
                .await
            {
                Ok(compare) => format_compare_diff(&compare, 20_000),
                Err(e) => {
                    warn!(run_id, error = %e, "Could not load the open PR's diff for the ticket update");
                    String::new()
                }
            };
            msg.ticket_update_note = Some(ticket_update_note(
                pr_number,
                previous_ticket_text.as_deref(),
                &plan::ticket_text(&msg.title, &msg.body),
                &pr_diff,
            ));
            ticket_update_pr = Some(pr_number);
            true
        } else {
            existing_pr.as_ref().is_some_and(|pr| {
                let pr_head = pr["head"]["sha"].as_str().unwrap_or("");
                let base = pr["base"]["sha"].as_str().unwrap_or("");
                // Branch was reset to base by a previous retry — don't reset again
                !pr_head.is_empty() && !base.is_empty() && pr_head == base
            })
        };

        if ticket_update_pr.is_some() {
            // Progress note already posted; the branch keeps the PR's head.
        } else if skip_branch_reset {
            let pr_number = existing_pr
                .as_ref()
                .and_then(|p| p["number"].as_u64())
                .unwrap_or(0);
            info!(run_id, pr_number, branch = %branch_name, "Skipping branch reset — PR exists, branch already at base");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("PR #{pr_number} exists — re-implementing on existing branch"),
            )
            .await;
        } else {
            // Create working branch (or reset existing one to base)
            github
                .create_branch(
                    &msg.repo_owner,
                    &msg.repo_name,
                    &branch_name,
                    &msg.base_branch,
                )
                .await?;
            info!(run_id, branch = %branch_name, "Created working branch");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Created branch {}", branch_name),
            )
            .await;

            // The reset wiped any commits from a previous partial run, so a
            // reused plan's `- [x]` checkmarks no longer reflect reality —
            // implement is told to skip checked tasks, which silently dropped
            // the wiped work. Reset every task to unchecked.
            if plan_was_reused && plan_result.tasks.contains("- [x]") {
                plan_result.tasks = plan_result.tasks.replace("- [x]", "- [ ]");
                let tasks_key = format!(
                    "{}/tasks.md",
                    openspec_prefix(
                        &msg.team_id,
                        &msg.ticket_id,
                        &msg.repo_owner,
                        &msg.repo_name
                    )
                );
                if let Err(e) = state
                    .s3
                    .put_object()
                    .bucket(&state.config.bucket_name)
                    .key(&tasks_key)
                    .body(plan_result.tasks.as_bytes().to_vec().into())
                    .content_type("text/markdown")
                    .send()
                    .await
                {
                    warn!(run_id, error = %e, "Failed to reset task checkmarks after branch reset");
                }
                info!(
                    run_id,
                    "Branch reset — reused plan's task checkmarks cleared"
                );
            }
        }

        // Commit openspec to the repo branch if enabled (default: on)
        let commit_openspec = load_workflow_setting(state, &msg.team_id, "commit_openspec").await;
        if commit_openspec {
            let ticket_slug = msg.ticket_id.to_lowercase();
            let spec_files: Vec<crate::clients::github::FileOp> = [
                ("proposal.md", &plan_result.proposal),
                ("design.md", &plan_result.design),
                ("tasks.md", &plan_result.tasks),
                ("spec.md", &plan_result.spec),
            ]
            .iter()
            .filter(|(_, content)| !content.is_empty())
            .map(|(name, content)| crate::clients::github::FileOp::Write {
                path: format!("openspec/specs/{ticket_slug}/{name}"),
                content: content.to_string(),
            })
            .collect();

            if !spec_files.is_empty() {
                if let Err(e) = github
                    .batch_write(
                        &msg.repo_owner,
                        &msg.repo_name,
                        &branch_name,
                        &conventions.commit_message(
                            &tokens,
                            &format!("Add openspec for {}", msg.ticket_id),
                        ),
                        &spec_files,
                    )
                    .await
                {
                    warn!(run_id, error = %e, "Failed to commit openspec to branch");
                }
            }
        }

        // --- Plan Validation (deterministic, no LLM) ---
        let team_repos = plan::fetch_team_repos(state, &msg.team_id).await;
        // Already validated (and the repo settled) right after planning.
        let mut plan_warnings = match early_validation.take() {
            Some(v) => v,
            None => {
                validate_plan(
                    &plan_result,
                    &github,
                    &msg.repo_owner,
                    &msg.repo_name,
                    &msg.base_branch,
                    state,
                    &msg.team_id,
                    &team_repos,
                    &provider,
                    usage,
                )
                .await
            }
        };
        // Over the file cap: ask the planner ONCE to narrow the scope before
        // failing the run — most oversized plans pad the change set.
        if let Some((planned, cap)) = plan_warnings.over_cap {
            info!(
                run_id,
                planned, cap, "Plan over the file cap — re-planning once with a narrower scope"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!(
                    "Plan changed {planned} files (cap {cap}) — re-planning with a narrower scope"
                ),
            )
            .await;
            let mut narrowed = msg.clone();
            narrowed.body.push_str(&narrow_scope_note(planned, cap));
            match plan::run(
                state,
                &narrowed,
                &github,
                &triage_result,
                &repo_instructions,
                &provider,
                usage,
                run_id,
            )
            .await
            {
                Ok(p) => {
                    plan_result = p;
                    // The planner stored a hash of the NARROWED ticket; put the
                    // real ticket's hash back so the next run reuses this plan.
                    let prefix = openspec_prefix(
                        &msg.team_id,
                        &msg.ticket_id,
                        &msg.repo_owner,
                        &msg.repo_name,
                    );
                    let _ = state
                        .s3
                        .put_object()
                        .bucket(&state.config.bucket_name)
                        .key(format!("{prefix}/context_hash.txt"))
                        .body(context_hash.as_bytes().to_vec().into())
                        .content_type("text/plain")
                        .send()
                        .await;
                    // Keep the branch's openspec in step with the plan in use.
                    if commit_openspec {
                        let ticket_slug = msg.ticket_id.to_lowercase();
                        let spec_files: Vec<crate::clients::github::FileOp> = [
                            ("proposal.md", &plan_result.proposal),
                            ("design.md", &plan_result.design),
                            ("tasks.md", &plan_result.tasks),
                            ("spec.md", &plan_result.spec),
                        ]
                        .iter()
                        .filter(|(_, content)| !content.is_empty())
                        .map(|(name, content)| crate::clients::github::FileOp::Write {
                            path: format!("openspec/specs/{ticket_slug}/{name}"),
                            content: content.to_string(),
                        })
                        .collect();
                        if !spec_files.is_empty() {
                            if let Err(e) = github
                                .batch_write(
                                    &msg.repo_owner,
                                    &msg.repo_name,
                                    &branch_name,
                                    &conventions.commit_message(
                                        &tokens,
                                        &format!("Narrow openspec for {}", msg.ticket_id),
                                    ),
                                    &spec_files,
                                )
                                .await
                            {
                                warn!(run_id, error = %e, "Failed to commit the narrowed openspec");
                            }
                        }
                    }
                    plan_warnings = validate_plan(
                        &plan_result,
                        &github,
                        &msg.repo_owner,
                        &msg.repo_name,
                        &msg.base_branch,
                        state,
                        &msg.team_id,
                        &team_repos,
                        &provider,
                        usage,
                    )
                    .await;
                }
                Err(e) => {
                    warn!(run_id, error = %e, "Narrowed re-plan failed — keeping the first plan's verdict")
                }
            }
        }

        // Complexity upgrade valve: triage classifies with zero codebase
        // knowledge, and a wrong "simple" verdict silently strips the
        // implement agent's model tier, exploration tools, and design
        // context. If the plan itself references several files, the ticket
        // is not simple — upgrade before implement. (Never downgrades.)
        if triage_result.complexity == "simple" && plan_warnings.mentioned_file_count >= 4 {
            info!(
                run_id,
                files = plan_warnings.mentioned_file_count,
                "Plan touches multiple files — upgrading complexity simple → medium"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!(
                    "Upgraded to medium complexity (plan references {} files)",
                    plan_warnings.mentioned_file_count
                ),
            )
            .await;
            effective_complexity = "medium".to_string();
        }

        // If plan files exist in a different repo, switch before implement
        if let Some(ref better_repo) = plan_warnings.switch_repo {
            let (new_owner, new_name) = better_repo.split_once('/').unwrap_or(("", ""));
            if !new_owner.is_empty() && !new_name.is_empty() {
                warn!(
                    run_id,
                    from = %format!("{}/{}", msg.repo_owner, msg.repo_name),
                    to = %better_repo,
                    "Switching repo — plan files found in different repo"
                );
                // Keep the openspec cache coherent: copy it to the new
                // repo's prefix BEFORE mutating msg (computing the old prefix
                // after the mutation made this a self-copy no-op).
                let old_prefix = openspec_prefix(
                    &msg.team_id,
                    &msg.ticket_id,
                    &msg.repo_owner,
                    &msg.repo_name,
                );
                let new_prefix = openspec_prefix(&msg.team_id, &msg.ticket_id, new_owner, new_name);
                msg.repo_owner = new_owner.to_string();
                msg.repo_name = new_name.to_string();
                for name in [
                    "proposal.md",
                    "design.md",
                    "tasks.md",
                    "spec.md",
                    "context_hash.txt",
                    plan::PLAN_RUN_FILE,
                ] {
                    let _ = state
                        .s3
                        .copy_object()
                        .bucket(&state.config.bucket_name)
                        .copy_source(format!(
                            "{}/{}/{}",
                            state.config.bucket_name, old_prefix, name
                        ))
                        .key(format!("{new_prefix}/{name}"))
                        .send()
                        .await;
                }

                // Re-resolve default branch for new repo
                msg.base_branch = github
                    .get_default_branch(&msg.repo_owner, &msg.repo_name)
                    .await
                    .unwrap_or_else(|_| "main".to_string());

                // Create (or reset) the working branch in the new repo
                github
                    .create_branch(
                        &msg.repo_owner,
                        &msg.repo_name,
                        &branch_name,
                        &msg.base_branch,
                    )
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to create branch {} in {}/{} after repo switch: {e}",
                            branch_name, msg.repo_owner, msg.repo_name
                        )
                    })?;

                // Update run record
                let resolved_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.runs_table_name)
                    .key("team_id", attr_s(&msg.team_id))
                    .key("run_id", attr_s(run_id))
                    .update_expression("SET repo = :r, team_repo = :tr")
                    .expression_attribute_values(":r", attr_s(&resolved_repo))
                    .expression_attribute_values(
                        ":tr",
                        attr_s(&format!("{}#{}", msg.team_id, resolved_repo)),
                    )
                    .send()
                    .await;

                add_progress_note(
                    state,
                    &msg.team_id,
                    run_id,
                    &format!(
                        "Switched to {} ({})",
                        resolved_repo,
                        if plan_warnings.switch_reason.is_empty() {
                            "repo routing"
                        } else {
                            &plan_warnings.switch_reason
                        }
                    ),
                )
                .await;
            }
        }

        if plan_warnings.blocked {
            let warning_text = plan_warnings.warnings.join("\n");
            let comment = format!(
                "⚠️ **Plan validation failed**\n\n{warning_text}\n\nPlease narrow the scope and re-trigger."
            );
            if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
                let _ = github
                    .create_issue_comment(
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.issue_number,
                        &comment,
                    )
                    .await;
            } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
                let _ = post_jira_comment(
                    state,
                    &msg.team_id,
                    &msg.ticket_id,
                    &warning_text,
                    "scope_too_large",
                    "Plan scope too large",
                )
                .await;
            }
            close_memory(state, agent_memory.take()).await;
            return Err(format!("Plan validation blocked: {warning_text}").into());
        }
        if !plan_warnings.warnings.is_empty() {
            for w in &plan_warnings.warnings {
                warn!(run_id, warning = %w, "Plan validation warning");
            }
        }

        // --- Multi-repo path ---
        if !plan_result.repo_tasks.is_empty() {
            info!(
                run_id,
                repo_count = plan_result.repo_tasks.len(),
                "Multi-repo plan detected, running per-repo pipeline"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Multi-repo plan: {} repos", plan_result.repo_tasks.len()),
            )
            .await;

            let mut pr_results: Vec<RepoPrResult> = Vec::new();

            // Each repo's branch: the team's pattern plus `-{repo}`, or the
            // branch of the PR already open there for this ticket.
            let mut repo_branches: Vec<String> = Vec::new();
            for repo_task in &plan_result.repo_tasks {
                repo_branches.push(
                    resolve_run_branch(
                        &github,
                        &repo_task.owner,
                        &repo_task.name,
                        &msg.ticket_id,
                        &conventions,
                        &tokens,
                        &repo_task.name.to_lowercase(),
                    )
                    .await,
                );
            }

            // A pull request for this ticket already open in any of the repos
            // (a person's, or an earlier run's): none is opened twice.
            if msg.person_direction.is_none() {
                for (repo_task, own) in plan_result.repo_tasks.iter().zip(&repo_branches) {
                    if let Some(other) = open_pr_for_ticket(
                        &github,
                        &repo_task.owner,
                        &repo_task.name,
                        &msg.ticket_id,
                        own,
                    )
                    .await
                    {
                        let repo = format!("{}/{}", repo_task.owner, repo_task.name);
                        stop_for_open_pr(state, msg, &github, run_id, usage, start, &repo, &other)
                            .await;
                        close_memory(state, agent_memory.take()).await;
                        return Ok(());
                    }
                }
            }

            for (repo_task, repo_branch) in plan_result.repo_tasks.iter().zip(repo_branches) {
                // Override msg repo for this iteration
                let mut repo_msg = msg.clone();
                repo_msg.repo_owner = repo_task.owner.clone();
                repo_msg.repo_name = repo_task.name.clone();

                // Resolve base branch for this repo
                repo_msg.base_branch = github
                    .get_default_branch(&repo_task.owner, &repo_task.name)
                    .await
                    .unwrap_or_else(|_| "main".to_string());

                // Load repo-specific instructions
                let repo_instr =
                    load_repo_instructions(&github, &repo_task.owner, &repo_task.name).await;
                repo_msg.team_instructions = load_team_instructions(
                    state,
                    &repo_msg.team_id,
                    &repo_task.owner,
                    &repo_task.name,
                )
                .await;

                match run_repo_pipeline(
                    state,
                    &repo_msg,
                    &github,
                    &plan_result,
                    &repo_task.tasks,
                    &repo_branch,
                    &repo_msg.base_branch,
                    &rules,
                    &repo_instr,
                    &effective_complexity,
                    &voice,
                    &provider,
                    usage,
                    &file_cache,
                    start,
                    run_id,
                    &mut agent_memory,
                    implement_deadline,
                )
                .await
                {
                    Ok(pr) => pr_results.push(pr),
                    Err(e) => {
                        warn!(
                            run_id,
                            repo = %format!("{}/{}", repo_task.owner, repo_task.name),
                            error = %e,
                            "Repo pipeline failed, continuing with other repos"
                        );
                        add_progress_note(
                            state,
                            &msg.team_id,
                            run_id,
                            &format!("Failed for {}/{}: {}", repo_task.owner, repo_task.name, e),
                        )
                        .await;
                    }
                }
            }

            if pr_results.is_empty() {
                close_memory(state, agent_memory.take()).await;
                return Err("Multi-repo pipeline produced no PRs".into());
            }

            // Save checkpoint + set awaiting_ci with all PRs
            save_checkpoint(
                state,
                &msg.team_id,
                run_id,
                "pr",
                &pr_results[0].branch,
                0,
                usage,
            )
            .await;

            let duration = start.elapsed().as_secs();
            let now = chrono::Utc::now().to_rfc3339();
            let cost = usage.estimated_cost();

            // Build PR list attributes
            let pr_urls: Vec<AttributeValue> =
                pr_results.iter().map(|p| attr_s(&p.pr_url)).collect();
            let pr_numbers: Vec<AttributeValue> =
                pr_results.iter().map(|p| attr_n(p.pr_number)).collect();
            let branches: Vec<AttributeValue> =
                pr_results.iter().map(|p| attr_s(&p.branch)).collect();

            // Store first PR as primary for backward compat, plus full lists
            let multi_res = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression(
                    "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
                     pr_urls = :prs, pr_numbers = :pns, branches = :bs, \
                     repo = :repo, team_repo = :tr, \
                     tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
                     cost_usd = :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
                     status_run_id = :sri, mcp_servers = :mcp",
                )
                .expression_attribute_names("#status", "status")
                // Never resurrect a cancelled run (mirrors the single-repo guard).
                .condition_expression("#status <> :cancel")
                .expression_attribute_values(":cancel", attr_s("cancelled"))
                .expression_attribute_values(":s", attr_s("awaiting_ci"))
                .expression_attribute_values(":pr", attr_s(&pr_results[0].pr_url))
                .expression_attribute_values(":pn", attr_n(pr_results[0].pr_number))
                .expression_attribute_values(":b", attr_s(&pr_results[0].branch))
                .expression_attribute_values(":prs", AttributeValue::L(pr_urls))
                .expression_attribute_values(":pns", AttributeValue::L(pr_numbers))
                .expression_attribute_values(":bs", AttributeValue::L(branches))
                .expression_attribute_values(":repo", attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)))
                .expression_attribute_values(":tr", attr_s(&format!("{}#{}/{}", msg.team_id, msg.repo_owner, msg.repo_name)))
                .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                .expression_attribute_values(":to", attr_n(usage.output_tokens))
                .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
                .expression_attribute_values(":d", attr_n(duration))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":cp", attr_s("awaiting_ci"))
                .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
                .expression_attribute_values(
                    ":mcp",
                    AttributeValue::L(used_mcp_ids.iter().map(|id| attr_s(id)).collect()),
                )
                .send()
                .await;
            if let Err(e) = multi_res {
                if format!("{e:?}").contains("ConditionalCheckFailed") {
                    info!(
                        run_id,
                        "Multi-repo run cancelled during orchestration — not setting awaiting_ci"
                    );
                    return Ok(());
                }
                return Err(e.into());
            }

            info!(
                run_id,
                pr_count = pr_results.len(),
                "Multi-repo run set to awaiting_ci"
            );

            // Schedule a safety-net resume
            if !state.config.ci_fix_queue_url.is_empty() {
                let resume_body = serde_json::json!({
                    "type": "resume",
                    "team_id": msg.team_id,
                    "run_id": run_id,
                    "installation_id": msg.installation_id,
                });
                let _ = state
                    .sqs
                    .send_message()
                    .queue_url(&state.config.ci_fix_queue_url)
                    .message_body(resume_body.to_string())
                    .delay_seconds(120)
                    .send()
                    .await;
            }

            if let Some(mem) = agent_memory {
                if let Err(e) = mem.close_and_upload(state).await {
                    warn!(run_id, error = %e, "Failed to persist agent memory");
                }
            }

            return Ok(());
        }

        // --- Single-repo path (existing) ---

        // Write final repo to run record (after validate_plan may have switched it)
        let final_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("run_id", attr_s(run_id))
            .update_expression("SET repo = :r, team_repo = :tr")
            .expression_attribute_values(":r", attr_s(&final_repo))
            .expression_attribute_values(":tr", attr_s(&format!("{}#{}", msg.team_id, final_repo)))
            .send()
            .await;

        // Record the branch BEFORE implement runs: if implement dies mid-way
        // (turn exhaustion, API failure), salvage needs the branch name, and
        // the last successful checkpoint ("plan") doesn't carry one. The
        // distinct pass name keeps can_skip_implement from treating an
        // unfinished implement as done on SQS redelivery.
        save_checkpoint(
            state,
            &msg.team_id,
            run_id,
            "implement_started",
            &branch_name,
            0,
            usage,
        )
        .await;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let result = implement::run(
            state,
            msg,
            &github,
            &plan_result,
            &branch_name,
            &rules,
            &repo_instructions,
            None,
            &effective_complexity,
            &provider,
            usage,
            &file_cache,
            Some(run_id),
            implement_deadline,
            false, // initial implement — unscoped
        )
        .await?;
        // The ticket-update note is for the main implement only; later fix
        // passes (test/security) must not read "make no edits if it matches".
        msg.ticket_update_note = None;
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            "implement",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        save_checkpoint(
            state,
            &msg.team_id,
            run_id,
            "implement",
            &branch_name,
            0,
            usage,
        )
        .await;
        info!(
            run_id,
            files = result.files_modified.len(),
            "Implement complete"
        );

        // Save conversation log to S3
        write_conversation_log(
            state,
            &msg.team_id,
            run_id,
            "implement",
            &result.conversation_log,
        )
        .await;

        add_progress_note(
            state,
            &msg.team_id,
            run_id,
            &format!(
                "Implemented changes across {} file(s)",
                result.files_modified.len()
            ),
        )
        .await;

        // If no files were changed, comment on the issue and stop — don't open
        // an empty PR. This is a needs_input outcome (mirroring the plan-pass
        // clarification branch): the ticket lacks detail, a reply re-triggers.
        // It used to `return Err`, which routed through fail_run — marking the
        // run failed AND posting a second, generic failure comment on top of
        // the clarification comment.
        // Ticket update on an open PR where the agent found the branch already
        // matches the new ticket: that is a finished run, not a request for
        // more detail. Keep the PR and complete.
        if let (true, Some(pr_number)) = (result.files_modified.is_empty(), ticket_update_pr) {
            info!(
                run_id,
                pr_number, "Ticket update: PR already matches the updated ticket"
            );
            let note = format!("PR #{pr_number} already matches the updated ticket");
            add_progress_note(state, &msg.team_id, run_id, &note).await;
            let pr_url = existing_pr
                .as_ref()
                .and_then(|p| p["html_url"].as_str())
                .unwrap_or("")
                .to_string();
            let duration = start.elapsed().as_secs();
            let cost = usage.estimated_cost();
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression(
                    "SET #status = :s, status_run_id = :sri, pr_number = :pn, pr_url = :pu, \
                     branch = :b, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, \
                     cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, updated_at = :t, \
                     current_pass = :cp",
                )
                .expression_attribute_names("#status", "status")
                .condition_expression("#status <> :cancelled")
                .expression_attribute_values(":cancelled", attr_s("cancelled"))
                .expression_attribute_values(":s", attr_s("completed"))
                .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
                .expression_attribute_values(":pn", attr_n(pr_number))
                .expression_attribute_values(":pu", attr_s(&pr_url))
                .expression_attribute_values(":b", attr_s(&branch_name))
                .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                .expression_attribute_values(":to", attr_n(usage.output_tokens))
                .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
                .expression_attribute_values(":d", attr_n(duration))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":cp", attr_s("done"))
                .send()
                .await;
            close_memory(state, agent_memory.take()).await;
            return Ok(());
        }

        if result.files_modified.is_empty() {
            warn!(run_id, "Implement pass produced zero file changes");
            // The files it went for were in a different repo of the org (one
            // this team hasn't turned on), so it could look but not change.
            // Saying that is the useful answer; "add more detail" is not.
            let elsewhere =
                repos_reached_for(&result.conversation_log, &msg.repo_owner, &msg.repo_name);
            let elsewhere_note = (!elsewhere.is_empty()).then(|| {
                format!(
                    "The file(s) this ticket changes look like they're in {}, not {}/{}, so I could \
                     read them but not change them. Turn that repo on for Coderhelm (Repos page) and \
                     re-run, or move the ticket to it.",
                    elsewhere.iter().map(|r| format!("`{r}`")).collect::<Vec<_>>().join(", "),
                    msg.repo_owner,
                    msg.repo_name
                )
            });
            let raw_clarification = elsewhere_note.as_deref().unwrap_or("I ran but produced no file changes for this issue.\n\n\
                 If the issue already spells out the change (the exact file(s) and the values/content \
                 to write), this is usually a transient miss on my side — **re-run it** and it \
                 typically goes through.\n\n\
                 If details are actually missing, add them and re-trigger — for example:\n\
                 - Which file(s) or component(s) should change?\n\
                 - The expected vs. current behavior, or the exact values/content to use?");
            let clarification =
                formatter::format_with_voice(state, &voice, raw_clarification, &provider, usage)
                    .await;
            if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
                if let Err(e) = github
                    .create_issue_comment(
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.issue_number,
                        &clarification,
                    )
                    .await
                {
                    warn!(run_id, error = %e, "Failed to comment on issue about empty implementation");
                }
            } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
                if let Err(e) = post_jira_comment(
                    state,
                    &msg.team_id,
                    &msg.ticket_id,
                    &clarification,
                    "clarification",
                    "More detail needed",
                )
                .await
                {
                    warn!(run_id, error = %e, "Failed to comment on Jira ticket about empty implementation");
                }
            }

            let duration = start.elapsed().as_secs();
            let cost = usage.estimated_cost();
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("run_id", attr_s(run_id))
                .update_expression(
                    "SET #status = :s, status_run_id = :sri, error_message = :em, \
                     tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, \
                     cost_usd = :c, duration_s = :d, updated_at = :t, current_pass = :cp",
                )
                .expression_attribute_names("#status", "status")
                .expression_attribute_values(":s", attr_s("needs_input"))
                .expression_attribute_values(":sri", attr_s(&format!("needs_input#{run_id}")))
                .expression_attribute_values(":em", attr_s(raw_clarification))
                .expression_attribute_values(":ti", attr_n(usage.input_tokens))
                .expression_attribute_values(":to", attr_n(usage.output_tokens))
                .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
                .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
                .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
                .expression_attribute_values(":d", attr_n(duration))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":cp", attr_s("done"))
                .send()
                .await;
            close_memory(state, agent_memory.take()).await;
            return Ok(());
        }

        // Mark all tasks as done in S3 openspec so retries/dashboard see progress
        let tasks_key = format!(
            "{}/tasks.md",
            openspec_prefix(
                &msg.team_id,
                &msg.ticket_id,
                &msg.repo_owner,
                &msg.repo_name
            )
        );
        let checked = plan_result.tasks.replace("- [ ]", "- [x]");
        if let Err(e) = state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(&tasks_key)
            .body(checked.as_bytes().to_vec().into())
            .content_type("text/markdown")
            .send()
            .await
        {
            warn!(run_id, error = %e, "Failed to update tasks.md with checked items");
        }

        result
    };

    // --- Adversarial Test (multi-repo path) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    if has_time_for_remediation(start) {
        update_pass(state, &msg.team_id, run_id, "test").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let test_issues = adversarial_test(
            state,
            msg,
            &github,
            &plan_result,
            &branch_name,
            &provider,
            usage,
            &file_cache,
            &impl_result.files_modified,
            run_id,
        )
        .await;
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            "test",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;

        if !test_issues.is_empty() {
            warn!(run_id, "Adversarial test found issues — running auto-fix");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                "Test found issues — auto-fixing",
            )
            .await;

            let fix_plan = plan::PlanResult {
                proposal: String::new(),
                tasks: String::new(),
                spec: String::new(),
                design: String::new(),
                repo_tasks: vec![],
                fix: None,
            };
            let fix_cache = FileCache::default();
            // Fixer gets the same rules/conventions as the original implement
            // — it used to run with empty rules and instructions, free to
            // violate every must-rule while "fixing".
            if let Err(e) = implement::run(
                state,
                msg,
                &github,
                &fix_plan,
                &branch_name,
                &rules,
                &repo_instructions,
                Some(&test_issues),
                "simple",
                &provider,
                usage,
                &fix_cache,
                Some(run_id),
                None,
                true, // bot-driven test-fix — scoped to the PR
            )
            .await
            {
                warn!(run_id, error = %e, "Test auto-fix failed, proceeding anyway");
            }
        }
    }

    // --- Security Audit (before PR — ensures only security-clean code is published) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "security").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let security_result = match security::run(
        state,
        msg,
        &github,
        &plan_result,
        &branch_name,
        &repo_instructions,
        &provider,
        usage,
        &file_cache,
        Some(run_id),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(run_id, error = %e, "Security pass errored, proceeding");
            security::SecurityResult {
                passed: true,
                summary: format!("Security error: {e}"),
            }
        }
    };
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "security",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    save_checkpoint(
        state,
        &msg.team_id,
        run_id,
        "security",
        &branch_name,
        0,
        usage,
    )
    .await;
    info!(
        run_id,
        passed = security_result.passed,
        "Security audit complete"
    );
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        if security_result.passed {
            "Security audit passed"
        } else {
            "Security issues found, fixing"
        },
    )
    .await;

    if !security_result.passed {
        if !has_time_for_remediation(start) {
            warn!(
                run_id,
                remaining_secs = remaining_secs(start),
                "Skipping security remediation — not enough time remaining"
            );
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                "Security issues found but skipping remediation (low time remaining)",
            )
            .await;
        } else {
            info!(run_id, "Security issues found, running remediation cycle");
            check_cancelled(state, &msg.team_id, run_id).await?;
            update_pass(state, &msg.team_id, run_id, "implement").await?;
            let sec_fix_result = implement::run(
                state,
                msg,
                &github,
                &plan_result,
                &branch_name,
                &rules,
                &repo_instructions,
                Some(&format!(
                    "Security audit found vulnerabilities. Fix ALL of the following:\n\n{}",
                    security_result.summary
                )),
                &effective_complexity,
                &provider,
                usage,
                &file_cache,
                Some(run_id),
                None,
                true, // bot-driven security fix — scoped to the PR
            )
            .await?;
            write_conversation_log(
                state,
                &msg.team_id,
                run_id,
                "security_fix",
                &sec_fix_result.conversation_log,
            )
            .await;

            check_cancelled(state, &msg.team_id, run_id).await?;
            update_pass(state, &msg.team_id, run_id, "security").await?;
            let retry = match security::run(
                state,
                msg,
                &github,
                &plan_result,
                &branch_name,
                &repo_instructions,
                &provider,
                usage,
                &file_cache,
                Some(run_id),
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(run_id, error = %e, "Security retry errored, proceeding");
                    security::SecurityResult {
                        passed: true,
                        summary: format!("Security error: {e}"),
                    }
                }
            };
            if !retry.passed {
                warn!(
                    run_id,
                    "Security issues remain after remediation, proceeding with PR"
                );
            }
        }
    }

    // Security findings are already posted as PR review comments and stored in the
    // run record — no need to duplicate the full audit text into long-term memory.

    // --- Proof check (before PR — what the change rests on is proven) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "review").await?;
    let mut evidence = impl_result.evidence.clone();
    let gaps = proof::prove(
        &proof::Work {
            state,
            msg,
            github: &github,
            plan: &plan_result,
            branch: &branch_name,
            rules: &rules,
            repo_instructions: &repo_instructions,
            provider: &provider,
            file_cache: &file_cache,
            run_id,
        },
        &mut evidence,
        usage,
        || has_time_for_remediation(start),
    )
    .await;

    // --- Create Draft PR (triggers CI) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "pr").await?;

    // Resolve conflicts with main before PR
    match pr::resolve_conflicts(state, msg, &github, &branch_name, &provider, usage).await {
        Ok(true) => info!(run_id, "Resolved merge conflicts with main"),
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Conflict resolution failed, proceeding"),
    }

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let pr_result = pr::run(
        state,
        msg,
        &github,
        &branch_name,
        &plan_result,
        &voice,
        &pr::Proof {
            evidence: &evidence.for_pr(),
            gaps: &proof::gaps_section(&gaps),
            manual: &proof::manual_steps(&gaps),
        },
        &provider,
        usage,
        run_id,
    )
    .await?;
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "pr",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    info!(run_id, pr_url = %pr_result.pr_url, "Draft PR created");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Draft PR created: #{}", pr_result.pr_number),
    )
    .await;

    // Save checkpoint and set awaiting_ci — Lambda returns here.
    // The webhook-driven resume flow handles CI results and review.
    save_checkpoint(state, &msg.team_id, run_id, "pr", &branch_name, 0, usage).await;

    // Update run record with PR info + awaiting_ci status
    let duration = start.elapsed().as_secs();
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    let awaiting_res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, \
             repo = :repo, team_repo = :tr, mcp_servers = :mcp",
        )
        .expression_attribute_names("#status", "status")
        // Never flip a cancelled run back to awaiting_ci (would resume it).
        .condition_expression("#status <> :cancel")
        .expression_attribute_values(":cancel", attr_s("cancelled"))
        .expression_attribute_values(":s", attr_s("awaiting_ci"))
        .expression_attribute_values(":pr", attr_s(&pr_result.pr_url))
        .expression_attribute_values(":pn", attr_n(pr_result.pr_number))
        .expression_attribute_values(
            ":repo",
            attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)),
        )
        .expression_attribute_values(
            ":tr",
            attr_s(&format!(
                "{}#{}/{}",
                msg.team_id, msg.repo_owner, msg.repo_name
            )),
        )
        .expression_attribute_values(":b", attr_s(&branch_name))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("awaiting_ci"))
        .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(
                used_mcp_ids
                    .iter()
                    .map(|id| attr_s(id))
                    .collect(),
            ),
        )
        .send()
        .await;
    if let Err(e) = awaiting_res {
        if format!("{e:?}").contains("ConditionalCheckFailed") {
            info!(
                run_id,
                "Run cancelled during orchestration — not setting awaiting_ci"
            );
            return Ok(());
        }
        return Err(e.into());
    }

    info!(
        run_id,
        pr_number = pr_result.pr_number,
        "Run set to awaiting_ci — Lambda returning, webhook will trigger resume"
    );

    // Schedule a safety-net resume in case CI webhook is missed or repo has no CI
    if !state.config.ci_fix_queue_url.is_empty() {
        let resume_body = serde_json::json!({
            "type": "resume",
            "team_id": msg.team_id,
            "run_id": run_id,
            "installation_id": msg.installation_id,
        });
        let _ = state
            .sqs
            .send_message()
            .queue_url(&state.config.ci_fix_queue_url)
            .message_body(resume_body.to_string())
            .delay_seconds(120)
            .send()
            .await;
    }

    // Persist agent memory (extract learnings first)
    if let Some(mut mem) = agent_memory {
        mem.extract_learnings_from_conversation(
            &impl_result.conversation_log,
            &provider,
            &crate::memory::LearningOrigin {
                run_id: run_id.to_string(),
                ticket_id: msg.ticket_id.clone(),
                title: msg.title.clone(),
            },
        )
        .await;
        if let Err(e) = mem.close_and_upload(state).await {
            warn!(run_id, error = %e, "Failed to persist agent memory");
        }
    }

    Ok(())
}

/// Run implement → security → PR for a single repo.
/// Returns a RepoPrResult with the created PR info.
#[allow(clippy::too_many_arguments)]
async fn run_repo_pipeline(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    plan_result: &plan::PlanResult,
    repo_tasks: &str,
    branch_name: &str,
    base_branch: &str,
    rules: &[String],
    repo_instructions: &str,
    complexity: &str,
    voice: &str,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    start: &Instant,
    run_id: &str,
    _agent_memory: &mut Option<AgentMemory>,
    implement_deadline: Option<std::time::Instant>,
) -> Result<RepoPrResult, Box<dyn std::error::Error + Send + Sync>> {
    let repo_owner = &msg.repo_owner;
    let repo_name = &msg.repo_name;

    // Create working branch
    github
        .create_branch(repo_owner, repo_name, branch_name, base_branch)
        .await?;
    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), branch = %branch_name, "Created working branch");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Created branch {} on {}/{}",
            branch_name, repo_owner, repo_name
        ),
    )
    .await;

    // Build a per-repo plan with overridden tasks
    let repo_plan = plan::PlanResult {
        proposal: plan_result.proposal.clone(),
        design: plan_result.design.clone(),
        tasks: repo_tasks.to_string(),
        spec: plan_result.spec.clone(),
        repo_tasks: vec![],
        fix: plan_result.fix.clone(),
    };

    // --- Implement ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "implement").await?;

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let impl_result = implement::run(
        state,
        msg,
        github,
        &repo_plan,
        branch_name,
        rules,
        repo_instructions,
        None,
        complexity,
        provider,
        usage,
        file_cache,
        Some(run_id),
        implement_deadline,
        false, // initial implement — unscoped
    )
    .await?;
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "implement",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    write_conversation_log(
        state,
        &msg.team_id,
        run_id,
        &format!("implement_{repo_owner}_{repo_name}"),
        &impl_result.conversation_log,
    )
    .await;
    save_checkpoint(
        state,
        &msg.team_id,
        run_id,
        "implement",
        branch_name,
        0,
        usage,
    )
    .await;

    info!(
        run_id,
        repo = %format!("{repo_owner}/{repo_name}"),
        files = impl_result.files_modified.len(),
        "Implement complete"
    );
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Implemented {} file(s) in {}/{}",
            impl_result.files_modified.len(),
            repo_owner,
            repo_name
        ),
    )
    .await;

    if impl_result.files_modified.is_empty() {
        return Err(format!(
            "No files modified in {}/{}. The issue may need more detail.",
            repo_owner, repo_name
        )
        .into());
    }

    // --- Adversarial Test (catches bugs before security/PR) ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    if has_time_for_remediation(start) {
        update_pass(state, &msg.team_id, run_id, "test").await?;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let test_issues = adversarial_test(
            state,
            msg,
            github,
            &repo_plan,
            branch_name,
            provider,
            usage,
            file_cache,
            &impl_result.files_modified,
            run_id,
        )
        .await;
        write_pass_trace(
            state,
            &msg.team_id,
            run_id,
            "test",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;

        if !test_issues.is_empty() {
            warn!(run_id, "Adversarial test found issues — running auto-fix");
            add_progress_note(
                state,
                &msg.team_id,
                run_id,
                "Test found issues — auto-fixing",
            )
            .await;

            let fix_plan = plan::PlanResult {
                proposal: String::new(),
                tasks: String::new(),
                spec: String::new(),
                design: String::new(),
                repo_tasks: vec![],
                fix: None,
            };
            let fix_cache = FileCache::default();
            // Same rules/conventions as the original implement (see the
            // single-repo auto-fix above).
            if let Err(e) = implement::run(
                state,
                msg,
                github,
                &fix_plan,
                branch_name,
                rules,
                repo_instructions,
                Some(&test_issues),
                "simple",
                provider,
                usage,
                &fix_cache,
                Some(run_id),
                None,
                true, // bot-driven test-fix — scoped to the PR
            )
            .await
            {
                warn!(run_id, error = %e, "Test auto-fix failed, proceeding anyway");
            }
        }
    }

    // --- Security Audit ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "security").await?;
    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let security_result = match security::run(
        state,
        msg,
        github,
        &repo_plan,
        branch_name,
        repo_instructions,
        provider,
        usage,
        file_cache,
        Some(run_id),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(run_id, error = %e, "Security pass errored, proceeding");
            security::SecurityResult {
                passed: true,
                summary: format!("Security error: {e}"),
            }
        }
    };
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "security",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;
    save_checkpoint(
        state,
        &msg.team_id,
        run_id,
        "security",
        branch_name,
        0,
        usage,
    )
    .await;

    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), passed = security_result.passed, "Security audit complete");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        if security_result.passed {
            "Security audit passed"
        } else {
            "Security issues found, fixing"
        },
    )
    .await;

    // Security remediation
    if !security_result.passed {
        if has_time_for_remediation(start) {
            info!(run_id, "Running security remediation");
            check_cancelled(state, &msg.team_id, run_id).await?;
            let sec_fix = implement::run(
                state,
                msg,
                github,
                &repo_plan,
                branch_name,
                rules,
                repo_instructions,
                Some(&format!(
                    "Security audit found vulnerabilities. Fix ALL of the following:\n\n{}",
                    security_result.summary
                )),
                complexity,
                provider,
                usage,
                file_cache,
                Some(run_id),
                None,
                true, // bot-driven security fix — scoped to the PR
            )
            .await?;
            write_conversation_log(
                state,
                &msg.team_id,
                run_id,
                &format!("security_fix_{}", branch_name),
                &sec_fix.conversation_log,
            )
            .await;
        } else {
            warn!(run_id, "Skipping security remediation — not enough time");
        }
    }

    // Security findings are already posted as PR review comments and stored in the
    // run record — no need to duplicate the full audit text into long-term memory.

    // --- Proof check ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "review").await?;
    let mut evidence = impl_result.evidence.clone();
    let gaps = proof::prove(
        &proof::Work {
            state,
            msg,
            github,
            plan: &repo_plan,
            branch: branch_name,
            rules,
            repo_instructions,
            provider,
            file_cache,
            run_id,
        },
        &mut evidence,
        usage,
        || has_time_for_remediation(start),
    )
    .await;

    // --- Create Draft PR ---
    check_cancelled(state, &msg.team_id, run_id).await?;
    update_pass(state, &msg.team_id, run_id, "pr").await?;

    match pr::resolve_conflicts(state, msg, github, branch_name, provider, usage).await {
        Ok(true) => info!(run_id, "Resolved merge conflicts"),
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Conflict resolution failed, proceeding"),
    }

    let pass_start = std::time::Instant::now();
    let usage_before = usage.clone();
    let pr_result = pr::run(
        state,
        msg,
        github,
        branch_name,
        &repo_plan,
        voice,
        &pr::Proof {
            evidence: &evidence.for_pr(),
            gaps: &proof::gaps_section(&gaps),
            manual: &proof::manual_steps(&gaps),
        },
        provider,
        usage,
        run_id,
    )
    .await?;
    write_pass_trace(
        state,
        &msg.team_id,
        run_id,
        "pr",
        pass_start,
        &usage_before,
        usage,
        None,
    )
    .await;

    info!(run_id, repo = %format!("{repo_owner}/{repo_name}"), pr_url = %pr_result.pr_url, "Draft PR created");
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Draft PR #{} created for {}/{}",
            pr_result.pr_number, repo_owner, repo_name
        ),
    )
    .await;

    Ok(RepoPrResult {
        branch: branch_name.to_string(),
        pr_number: pr_result.pr_number,
        pr_url: pr_result.pr_url,
    })
}

/// Flip a continuation's existing run back to "running" — but ONLY if it still
/// exists and is NOT cancelled. Returns false (→ abort the continuation)
/// otherwise. This is the guard that stops auto-continuation from ever
/// resurrecting a run the user cancelled.
async fn resume_run_to_running(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #s = :r, status_run_id = :sri, updated_at = :t")
        .condition_expression("attribute_exists(run_id) AND #s <> :cancelled")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":r", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cancelled", attr_s("cancelled"))
        .send()
        .await
        .is_ok()
}

async fn create_run_record(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.runs_table_name)
        .item("team_id", attr_s(&msg.team_id))
        .item("run_id", attr_s(run_id))
        .item("status", attr_s("running"))
        // Composite SK for status-index GSI: "running#<run_id>" for efficient status queries
        .item("status_run_id", attr_s(&format!("running#{run_id}")))
        // Composite key for repo-index GSI
        .item("team_repo", attr_s(&format!("{}#{}", msg.team_id, repo)))
        .item(
            "ticket_source",
            attr_s(match msg.source {
                TicketSource::Github => "github",
                TicketSource::Jira => "jira",
                TicketSource::Alert => "alert",
            }),
        )
        .item("ticket_id", attr_s(&msg.ticket_id))
        // Who asked for the work, for per-person analytics.
        .item("sender", attr_s(&msg.sender))
        .item("title", attr_s(&msg.title))
        .item("repo", attr_s(&repo))
        .item("installation_id", attr_n(msg.installation_id))
        .item("issue_number", attr_n(msg.issue_number))
        .item("base_branch", attr_s(&msg.base_branch))
        // Store the body for Jira and alert tickets so a retry can re-use it
        // (GitHub re-fetches from the API)
        .item(
            "ticket_body",
            attr_s(
                if matches!(msg.source, TicketSource::Jira | TicketSource::Alert) {
                    &msg.body
                } else {
                    ""
                },
            ),
        )
        .item(
            "image_attachments",
            AttributeValue::S(serde_json::to_string(&msg.image_attachments).unwrap_or_default()),
        )
        .item("tokens_in", attr_n(0))
        .item("tokens_out", attr_n(0))
        .item("cost_usd", attr_n(0))
        // Content hash of title+body+images: the gateway's webhook gate
        // compares against this to skip re-triggers where nothing changed.
        .item(
            "context_hash",
            attr_s(&plan::compute_ticket_context_hash(msg)),
        )
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now));
    // The same person, however each source names them (see common::requester).
    if let Some(r) = &msg.requester {
        put = put.item("requester_key", attr_s(&r.key));
        if !r.name.is_empty() {
            put = put.item("requester_name", attr_s(&r.name));
        }
        if !r.email.is_empty() {
            put = put.item("requester_email", attr_s(&r.email));
        }
    }
    put.send().await?;

    // Increment analytics counters (current month + all-time)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD total_runs :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await?;
    }

    Ok(())
}

pub(crate) async fn update_pass(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let entry = aws_sdk_dynamodb::types::AttributeValue::M(std::collections::HashMap::from([
        ("pass".to_string(), attr_s(pass)),
        ("started_at".to_string(), attr_s(&now)),
    ]));
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET current_pass = :p, updated_at = :t, pass_history = list_append(if_not_exists(pass_history, :empty), :entry)",
        )
        .expression_attribute_values(":p", attr_s(pass))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":entry", aws_sdk_dynamodb::types::AttributeValue::L(vec![entry]))
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
        .send()
        .await?;
    Ok(())
}

/// A CI result worth counting on the run record, for analytics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CiOutcome {
    /// CI was red and Coderhelm started a fix cycle.
    FixAttempt,
    /// A fix cycle changed code and pushed it. Also keeps when, so a week's
    /// fixes can be counted whenever the run started.
    FixCommitted,
    /// CI came back green.
    Passed,
    /// Coderhelm could not get CI green and left the PR to a person; the
    /// first hand-off's time is kept.
    HandedOff,
}

impl CiOutcome {
    fn update_expression(self) -> &'static str {
        match self {
            CiOutcome::FixAttempt => "ADD ci_fix_attempts :one SET ci_last = :failed",
            CiOutcome::FixCommitted => {
                "ADD ci_fix_commits :one SET ci_fixes_at = list_append(if_not_exists(ci_fixes_at, :empty), :at)"
            }
            CiOutcome::Passed => "SET ci_last = :passed",
            CiOutcome::HandedOff => "SET handed_off_at = if_not_exists(handed_off_at, :now)",
        }
    }
}

/// Counts a CI result on the run record. Never fails the run: analytics are
/// best effort.
pub(crate) async fn record_ci_outcome(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    outcome: CiOutcome,
) {
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(outcome.update_expression());
    req = match outcome {
        CiOutcome::FixAttempt => req
            .expression_attribute_values(":one", attr_n(1))
            .expression_attribute_values(":failed", attr_s("failed")),
        CiOutcome::FixCommitted => {
            let now = chrono::Utc::now().to_rfc3339();
            req.expression_attribute_values(":one", attr_n(1))
                .expression_attribute_values(
                    ":at",
                    aws_sdk_dynamodb::types::AttributeValue::L(vec![attr_s(&now)]),
                )
                .expression_attribute_values(
                    ":empty",
                    aws_sdk_dynamodb::types::AttributeValue::L(vec![]),
                )
        }
        CiOutcome::Passed => req.expression_attribute_values(":passed", attr_s("passed")),
        CiOutcome::HandedOff => {
            req.expression_attribute_values(":now", attr_s(&chrono::Utc::now().to_rfc3339()))
        }
    };
    if let Err(e) = req.send().await {
        warn!(run_id, error = %e, "Failed to record CI outcome (non-fatal)");
    }
}

/// Ends a run whose fix is not (or not yet known to be) in the code: posts
/// the findings on the ticket the run came from and records them on the run,
/// with no branch and no pull request. A change outside the code ends the run
/// as `needs_outside_change`; not knowing where the fix belongs ends it as
/// `needs_input`, so a reply picks it up again.
#[allow(clippy::too_many_arguments)]
async fn finish_without_code(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    decision: &fix_location::FixDecision,
    route: fix_location::Route,
    usage: &TokenUsage,
    start: &std::time::Instant,
    mcp_ids: &[String],
) {
    let outside = route == fix_location::Route::OutsideOnly;
    let (report, jira_title, jira_type) = if outside {
        (
            fix_location::outside_report(decision, false),
            "Needs a change outside the code",
            "clarification",
        )
    } else {
        (
            fix_location::unknown_report(decision, false),
            "Not sure yet where the fix belongs",
            "clarification",
        )
    };
    let heading = format!("**{jira_title}**\n\n");
    info!(
        run_id,
        fix_location = decision.effective_location().as_str(),
        "Fix is not in the code — reporting instead of opening a PR"
    );
    add_progress_note(state, &msg.team_id, run_id, jira_title).await;

    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        if let Err(e) = github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format!("{heading}{report}"),
            )
            .await
        {
            warn!(run_id, error = %e, "Could not post the outside-the-code report on the issue");
        }
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        if let Err(e) = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &fix_location::jira_text(&report),
            jira_type,
            jira_title,
        )
        .await
        {
            warn!(run_id, error = %e, "Could not post the outside-the-code report on the ticket");
        }
    }

    if !outside {
        mark_run_needs_input(state, msg, run_id, usage, start, mcp_ids).await;
    }
    let status = if outside {
        fix_location::STATUS_OUTSIDE
    } else {
        "needs_input"
    };
    let duration = start.elapsed().as_secs();
    let now = chrono::Utc::now().to_rfc3339();
    let decision_json = serde_json::to_string(decision).unwrap_or_default();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, \
             cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, updated_at = :t, \
             current_pass = :cp, status_run_id = :sri, mcp_servers = :mcp, \
             fix_location = :fl, fix_decision = :fd, outside_change_report = :rep",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", usage.estimated_cost())))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_ids.iter().map(|s| attr_s(s)).collect()),
        )
        .expression_attribute_values(":fl", attr_s(decision.effective_location().as_str()))
        .expression_attribute_values(":fd", attr_s(&decision_json))
        .expression_attribute_values(":rep", attr_s(&format!("{heading}{report}")))
        .send()
        .await;

    if outside {
        let analytics_month = chrono::Utc::now().format("%Y-%m").to_string();
        for period in &[analytics_month.as_str(), "ALL_TIME"] {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.analytics_table_name)
                .key("team_id", attr_s(&msg.team_id))
                .key("period", attr_s(period))
                .update_expression("ADD completed :one, outside_code :one")
                .expression_attribute_values(":one", attr_n(1))
                .send()
                .await;
        }
    }
}

/// Tells the ticket about the part of the fix that has to be made outside the
/// code when the run also changes code, and records it on the run.
async fn report_also_outside(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    decision: &fix_location::FixDecision,
) {
    let section = fix_location::also_outside_section(Some(decision));
    if section.is_empty() {
        return;
    }
    let report = section
        .trim()
        .replacen("## Also needed outside the code", "", 1)
        .trim()
        .replace(
            "This pull request does not make that change",
            "The pull request for this ticket does not make that change",
        );
    let title = "Also needed outside the code";
    add_progress_note(state, &msg.team_id, run_id, title).await;
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let _ = github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.issue_number,
                &format!("**{title}**\n\n{report}"),
            )
            .await;
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        if let Err(e) = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &fix_location::jira_text(&report),
            "clarification",
            title,
        )
        .await
        {
            warn!(run_id, error = %e, "Could not post the outside part of the fix on the ticket");
        }
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET fix_location = :fl, fix_decision = :fd, outside_change_report = :rep",
        )
        .expression_attribute_values(":fl", attr_s(decision.effective_location().as_str()))
        .expression_attribute_values(
            ":fd",
            attr_s(&serde_json::to_string(decision).unwrap_or_default()),
        )
        .expression_attribute_values(":rep", attr_s(&format!("**{title}**\n\n{report}")))
        .send()
        .await;
}

/// Mark a run `needs_input` (a comment or re-run can pick it up again) and
/// count it in the analytics, the way every needs_input stop is recorded.
async fn mark_run_needs_input(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &TokenUsage,
    start: &std::time::Instant,
    mcp_ids: &[String],
) {
    let duration = start.elapsed().as_secs();
    let cost = usage.estimated_cost();
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, mcp_servers = :mcp",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("needs_input"))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("needs_input#{run_id}")))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_ids.iter().map(|s| attr_s(s)).collect()),
        )
        .send()
        .await;

    // Update analytics counters
    let analytics_month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[analytics_month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD completed :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }
}

/// Stop a Jira run whose ticket belongs in a repo that isn't connected or
/// isn't turned on for runs: tell the ticket (and the run notes) in plain
/// words, and leave the run needs_input instead of re-routing it.
async fn stop_run_repo_not_enabled(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &TokenUsage,
    start: &std::time::Instant,
    message: &str,
) {
    info!(
        run_id,
        message, "Run stopped: ticket's repo is not available for runs"
    );
    add_progress_note(state, &msg.team_id, run_id, message).await;
    if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        if let Err(e) = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            message,
            "clarification",
            "Repo not turned on",
        )
        .await
        {
            warn!(run_id, error = %e, "Could not post the repo-not-turned-on Jira comment");
        }
    }
    mark_run_needs_input(state, msg, run_id, usage, start, &[]).await;
}

/// Append a timestamped progress note to the run record for live activity display.
pub(crate) async fn add_progress_note(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    message: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let entry = aws_sdk_dynamodb::types::AttributeValue::M(std::collections::HashMap::from([
        ("message".to_string(), attr_s(message)),
        ("timestamp".to_string(), attr_s(&now)),
    ]));
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET progress_notes = list_append(if_not_exists(progress_notes, :empty), :entry), updated_at = :t",
        )
        .expression_attribute_values(":entry", aws_sdk_dynamodb::types::AttributeValue::L(vec![entry]))
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
}

/// Write a conversation log to S3 for the agent log viewer.
async fn write_conversation_log(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass_name: &str,
    log: &[serde_json::Value],
) {
    if log.is_empty() {
        return;
    }
    let key = format!("runs/{team_id}/{run_id}/conversation/{pass_name}.json");
    match serde_json::to_vec(log) {
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
                warn!(run_id, pass_name, error = %e, "Failed to write conversation log to S3");
            } else {
                info!(
                    run_id,
                    pass_name,
                    entries = log.len(),
                    "Conversation log saved to S3"
                );
            }
        }
        Err(e) => {
            warn!(run_id, pass_name, error = %e, "Failed to serialize conversation log");
        }
    }
}

/// Check if a run has been cancelled by the user via the dashboard.
async fn check_cancelled(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await?;

    if let Some(item) = result.item() {
        if let Some(status) = item.get("status").and_then(|v| v.as_s().ok()) {
            if status == "cancelled" {
                return Err("Run cancelled by user".into());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, dead_code)]
async fn complete_run(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr: &pr::PrResult,
    impl_result: &implement::ImplementResult,
    mcp_server_ids: &[String],
    usage: &TokenUsage,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    // Update run record in runs table
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, \
             duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri, files_modified = :fm, \
             repo = :repo, team_repo = :tr, mcp_servers = :mcp \
             REMOVE error_message, #err",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", attr_s("completed"))
        .expression_attribute_values(":pr", attr_s(&pr.pr_url))
        .expression_attribute_values(":pn", attr_n(pr.pr_number))
        .expression_attribute_values(
            ":repo",
            attr_s(&format!("{}/{}", msg.repo_owner, msg.repo_name)),
        )
        .expression_attribute_values(
            ":tr",
            attr_s(&format!(
                "{}#{}/{}",
                msg.team_id, msg.repo_owner, msg.repo_name
            )),
        )
        .expression_attribute_values(":b", attr_s(&pr.branch))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
        .expression_attribute_values(
            ":fm",
            AttributeValue::L(
                impl_result
                    .files_modified
                    .iter()
                    .map(|f| attr_s(f))
                    .collect(),
            ),
        )
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_server_ids.iter().map(|s| attr_s(s)).collect()),
        )
        .send()
        .await?;

    // Update analytics counters (current month + all-time)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD completed :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await?;
    }

    // Increment team's monthly run count in main table
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s("META"))
        .update_expression("ADD run_count_mtd :one")
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await?;

    // Check token usage against limit and send warning if needed
    check_token_usage_warning(state, &msg.team_id, usage).await;

    Ok(())
}

/// Check if the team has exceeded their configured monthly token limit.
/// Returns Some(reason) if exceeded, None if within budget or no limit set.
async fn check_budget_exceeded(state: &WorkerState, team_id: &str) -> Option<String> {
    // Load token limit
    let limit = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#TOKEN_LIMIT".to_string()),
        )
        .send()
        .await
        .ok()?
        .item()
        .and_then(|item| item.get("max_tokens").cloned())
        .and_then(|v| v.as_n().ok().cloned())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    if limit == 0 {
        return None; // No limit configured
    }

    // Load current month usage
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let total_used = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key("period", aws_sdk_dynamodb::types::AttributeValue::S(month))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .map(|item| {
            let ti = item
                .get("total_tokens_in")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let to = item
                .get("total_tokens_out")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            ti + to
        })
        .unwrap_or(0);

    if total_used >= limit {
        Some(format!(
            "Monthly token limit exceeded ({} / {} tokens). Increase your limit in Settings → AI Models.",
            total_used, limit
        ))
    } else {
        None
    }
}

async fn fail_run(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    error_msg: &str,
    usage: &TokenUsage,
    duration: u64,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();

    // Update run record to failed
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, error_message = :err, tokens_in = :ti, \
             tokens_out = :to, cache_read_tokens = :crt, cache_write_tokens = :cwt, cost_usd = :c, duration_s = :d, \
             updated_at = :t, status_run_id = :sri",
        )
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":s", attr_s("failed"))
        .expression_attribute_values(":err", attr_s(common::truncate_str(error_msg, 500)))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{:.4}", cost)))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":sri", attr_s(&format!("failed#{run_id}")))
        .send()
        .await;

    // Update analytics counters (include tokens so billing stays accurate)
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD failed :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }

    // Check token usage against limit and send warning if needed
    check_token_usage_warning(state, &msg.team_id, usage).await;

    // Comment on the GitHub issue so the user knows the run failed
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        if let Ok(gh) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            let comment = format!(
                "⚠️ **Coderhelm couldn't complete this issue**\n\n\
                 {}\n\n\
                 If you can, add more detail to the issue description — for example which files to change, \
                 expected behavior, or relevant code snippets — and I'll try again.\n\n\
                 [View run →](https://app.coderhelm.com/runs/detail?id={})",
                common::truncate_str(error_msg, 300),
                run_id,
            );
            if let Err(e) = gh
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.issue_number, &comment)
                .await
            {
                error!("Failed to comment on issue about run failure: {e}");
            }
        }
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        let comment = format!(
            "{}\n\n\
             Add more detail — which files to change, expected behavior, or relevant code snippets — and comment to retry.\n\n\
             View run → https://app.coderhelm.com/runs/detail?id={}",
            common::truncate_str(error_msg, 300),
            run_id,
        );
        if let Err(e) = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &comment,
            "error",
            "Run failed",
        )
        .await
        {
            error!("Failed to comment on Jira ticket about run failure: {e}");
        }
    }
}

/// Coding-agent instructions for repos with "Require tests" on: the agent writes
/// the tests the reviewer will look for, following the repo's own conventions.
pub(crate) const TESTS_REQUIRED_CODER_SECTION: &str = "\n\n## Tests are required in this repo\n\
Every change that adds or changes behavior must ship with tests in this same branch:\n\
- New behavior (a feature, endpoint, UI state, option, job): add tests that exercise it.\n\
- Changed or fixed behavior: update the existing tests that cover that code so they assert the \
new behavior. A bug fix gets a test that fails without the fix.\n\
- Follow the repo's own testing conventions exactly: framework, where tests live and how they're \
named, helpers and fixtures, how mocks are done, which command runs them. They are in AGENTS.md / \
CLAUDE.md and in nested ones next to the code (e.g. `e2e/CLAUDE.md`, a package's CLAUDE.md): read \
them with your tools BEFORE writing tests, then copy the pattern of the nearest existing test for \
the same code (search the test directories for the changed symbols or files).\n\
- If the repo documents end-to-end coverage rules (e.g. a test map or spec index that new flows \
must be added to), follow those too.\n\
- No tests needed for pure refactors with no behavior change, docs, comments, copy, styling, \
config values, dependency bumps or generated files.\n\
- Never weaken, skip, or delete an existing test to make it pass; fix the code, or update the \
test only when the behavior change is intended.\n";

pub(crate) fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

pub(crate) fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Release a ticket-level lock after a run completes (any terminal state).
/// Takes the ticket lock the gateway takes before it starts a run (same
/// record), so a run started from the worker can't run twice. Fails open on
/// an unexpected DynamoDB error, like the gateway.
pub(crate) async fn acquire_ticket_lock(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
) -> bool {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let now = chrono::Utc::now();
    let now_str = now.to_rfc3339();
    let result = state
        .dynamo
        .put_item()
        .table_name(&state.config.teams_table_name)
        .item("team_id", attr_s(team_id))
        .item("sk", attr_s(&sk))
        .item("locked_by", attr_s("worker"))
        .item(
            "locked_until",
            attr_s(&(now + chrono::Duration::minutes(15)).to_rfc3339()),
        )
        .item("created_at", attr_s(&now_str))
        .condition_expression("attribute_not_exists(sk) OR locked_until < :now")
        .expression_attribute_values(":now", attr_s(&now_str))
        .send()
        .await;
    match result {
        Ok(_) => true,
        Err(e) => {
            let held = e
                .as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception());
            if !held {
                warn!(team_id, ticket_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Ticket lock check failed — allowing through");
            }
            !held
        }
    }
}

async fn release_ticket_lock(state: &WorkerState, team_id: &str, ticket_id: &str) {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await;
}

/// Extend a ticket lock (e.g. when waiting for CI, extend to 30 min).
async fn extend_ticket_lock(state: &WorkerState, team_id: &str, ticket_id: &str, minutes: i64) {
    let sk = format!("TICKET_LOCK#{ticket_id}");
    let locked_until = (chrono::Utc::now() + chrono::Duration::minutes(minutes)).to_rfc3339();
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET locked_until = :lu, updated_at = :t")
        .expression_attribute_values(":lu", attr_s(&locked_until))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
}

/// Look up the GitHub App installation_id from the team META record.
async fn lookup_team_installation_id(
    state: &WorkerState,
    team_id: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .projection_expression("github_install_id")
        .send()
        .await?;

    result
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&id| id > 0)
        .ok_or_else(|| {
            "GitHub App is not installed for this team. Please install it at https://github.com/apps/coderhelm/installations/new".to_string()
            .into()
        })
}

/// Post a comment on a Jira ticket via the Forge web trigger.
/// Whether `text` names the ticket key `key` (case-insensitive, as a whole
/// token: `ABC-12` isn't `ABC-123`).
fn mentions_ticket(text: &str, key: &str) -> bool {
    if key.is_empty() {
        return false;
    }
    let t = text.to_ascii_lowercase();
    let k = key.to_ascii_lowercase();
    let mut from = 0;
    while let Some(i) = t[from..].find(&k) {
        let at = from + i;
        let before = t[..at].chars().next_back();
        let after = t[at + k.len()..].chars().next();
        let edge = |c: Option<char>| c.is_none_or(|c| !c.is_ascii_alphanumeric());
        if edge(before) && edge(after) {
            return true;
        }
        from = at + k.len();
    }
    false
}

/// The team's git conventions (branch, commit and PR title templates). The
/// defaults when the team set none, the record can't be read, or the ticket
/// is CoderHelm's own E2E work, whose branches and titles the E2E loop reads
/// back by their fixed shape.
pub async fn load_git_conventions(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
) -> common::git_conventions::GitConventions {
    use common::git_conventions as gc;
    if is_internal_ticket(ticket_id) {
        return gc::GitConventions::default();
    }
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(gc::SETTINGS_SK))
        .send()
        .await
        .ok()
        .and_then(|o| o.item.map(|i| gc::GitConventions::from_item(&i)))
        .unwrap_or_default()
}

/// CoderHelm's own E2E tickets (a fix for a failing test, a generated test).
fn is_internal_ticket(ticket_id: &str) -> bool {
    ticket_id.starts_with(e2e::FIX_TICKET_PREFIX) || ticket_id.starts_with(e2e::GEN_TICKET_PREFIX)
}

/// The template tokens for this run's ticket. `{ticket}` is written the way
/// the PR title always wrote it (`#42` for a GitHub issue).
pub fn ticket_tokens(msg: &TicketMessage) -> common::git_conventions::Tokens {
    let reference = match msg.source {
        TicketSource::Github if msg.issue_number > 0 => format!("#{}", msg.issue_number),
        _ => msg.ticket_id.clone(),
    };
    common::git_conventions::Tokens::new(&reference, &msg.ticket_id, &msg.title)
}

/// The branch this run works on in `owner/name`. A pull request already open
/// for the ticket keeps its branch whatever the pattern is now: first the
/// team's pattern and the default, then any open pull request CoderHelm
/// opened for the ticket (an earlier pattern, an earlier title's slug). Only
/// when there is none is a new branch named from the team's pattern.
/// `suffix` is a multi-repo run's repository name.
async fn resolve_run_branch(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    ticket_id: &str,
    conventions: &common::git_conventions::GitConventions,
    tokens: &common::git_conventions::Tokens,
    suffix: &str,
) -> String {
    let candidates = conventions.branch_candidates(tokens, suffix);
    for c in &candidates {
        if let Ok(Some(_)) = github.find_open_pr_for_branch(owner, name, c).await {
            return c.clone();
        }
    }
    if let Ok(prs) = github.list_open_pulls(owner, name).await {
        if let Some(b) = own_pr_branch_for_ticket(&prs, ticket_id) {
            return b;
        }
    }
    candidates[0].clone()
}

/// The branch of an open pull request CoderHelm opened (its author is the
/// app) that names the ticket in its branch or title.
fn own_pr_branch_for_ticket(prs: &serde_json::Value, ticket_id: &str) -> Option<String> {
    prs.as_array()?.iter().find_map(|p| {
        let head = p["head"]["ref"].as_str().unwrap_or("");
        let title = p["title"].as_str().unwrap_or("");
        let author = p["user"]["login"].as_str().unwrap_or("");
        (is_own_login(author)
            && !head.is_empty()
            && (mentions_ticket(head, ticket_id) || mentions_ticket(title, ticket_id)))
        .then(|| head.to_string())
    })
}

/// Whether a GitHub login is CoderHelm's app (it opens CoderHelm's PRs).
pub fn is_own_login(login: &str) -> bool {
    login.to_ascii_lowercase().contains("coderhelm")
}

/// An open pull request in `owner/name` that already names the ticket (in
/// its title, branch or description), other than the one on `own_branch`:
/// `(number, title, author)`. A failed lookup finds none.
async fn open_pr_for_ticket(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    ticket: &str,
    own_branch: &str,
) -> Option<(u64, String, String)> {
    let prs = github.list_open_pulls(owner, name).await.ok()?;
    prs.as_array()?.iter().find_map(|p| {
        let head = p["head"]["ref"].as_str().unwrap_or("");
        let title = p["title"].as_str().unwrap_or("");
        let body = p["body"].as_str().unwrap_or("");
        (head != own_branch
            && (mentions_ticket(title, ticket)
                || mentions_ticket(head, ticket)
                || mentions_ticket(body, ticket)))
        .then(|| {
            (
                p["number"].as_u64().unwrap_or(0),
                title.to_string(),
                p["user"]["login"].as_str().unwrap_or("").to_string(),
            )
        })
    })
}

/// The repo a Jira ticket's work already lives in, among the `enabled`
/// repos, with why: CoderHelm's own pull request for the ticket (merged or
/// open, from the ticket's history), else an open pull request in exactly
/// one enabled repo that names the ticket (a linked PR). None when neither
/// settles it; the usual pick runs then.
async fn repo_of_ticket_pr(
    history: &ticket_history::TicketHistory,
    enabled: &[String],
    github: &GitHubClient,
    ticket_id: &str,
) -> Option<(String, String)> {
    if let Some((repo, pr, state)) = repo_pick::repo_of_own_pr(&history.runs, enabled) {
        return Some((repo, repo_pick::ticket_pr_why(pr, state, true)));
    }
    // Bounded like the picker's code search: one list call per repo.
    if ticket_id.is_empty() || enabled.len() > 10 {
        return None;
    }
    let mut found: Vec<(String, u64)> = Vec::new();
    for r in enabled {
        let Some((owner, name)) = r.split_once('/') else {
            continue;
        };
        if let Some((number, _, _)) = open_pr_for_ticket(github, owner, name, ticket_id, "").await {
            found.push((r.clone(), number));
        }
    }
    match found.as_slice() {
        [(repo, number)] => Some((
            repo.clone(),
            repo_pick::ticket_pr_why(*number, "open", false),
        )),
        _ => None,
    }
}

/// Stops a run because a pull request for its ticket is already open: says
/// so on the ticket and the run, and waits for a person to say what to do.
#[allow(clippy::too_many_arguments)]
async fn stop_for_open_pr(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
    usage: &TokenUsage,
    start: &std::time::Instant,
    repo: &str,
    pr: &(u64, String, String),
) {
    let note = format!(
        "An open pull request for {} already exists in {repo}: #{} \"{}\" by {}. I didn't open another. Reply here to have me work on that pull request, or to go ahead with my own.",
        msg.ticket_id, pr.0, pr.1, pr.2
    );
    info!(
        run_id,
        repo,
        pr = pr.0,
        "Open pull request for the ticket exists — not opening another"
    );
    add_progress_note(state, &msg.team_id, run_id, &note).await;
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        let _ = github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.issue_number, &note)
            .await;
    } else if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        let _ = post_jira_comment(
            state,
            &msg.team_id,
            &msg.ticket_id,
            &note,
            "clarification",
            "A pull request is already open",
        )
        .await;
    }
    mark_run_needs_input(state, msg, run_id, usage, start, &[]).await;
}

async fn post_jira_comment(
    state: &WorkerState,
    team_id: &str,
    issue_key: &str,
    comment: &str,
    comment_type: &str,
    title: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("JIRA#config".to_string()),
        )
        .send()
        .await?
        .item
        .and_then(|item| {
            item.get("add_comment_url")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned()
        })
        .ok_or("No Jira add_comment_url configured")?;

    let payload = serde_json::json!({
        "issueKey": issue_key,
        "comment": comment,
        "commentType": comment_type,
        "title": title,
    });

    let resp = state
        .http
        .post(&url)
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Jira comment failed ({status}): {body}").into());
    }

    Ok(())
}

/// Check if there is already an active run for this ticket. "Active" means
/// running, queued, or awaiting_ci — an awaiting_ci run owns its branch/PR
/// (the resume loop is mid-flight on it), so a fresh run starting then would
/// force-reset that branch out from under it.
///
/// Uses the ticket-index GSI. The old status-index version applied Limit(1)
/// before its ticket filter, so any OTHER ticket's running run made this
/// ticket look idle — the guard silently never worked for busy teams.
async fn is_ticket_already_running(state: &WorkerState, msg: &TicketMessage) -> bool {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("ticket-index")
        .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
        .expression_attribute_values(":tid", attr_s(&msg.team_id))
        .expression_attribute_values(":ticket", attr_s(&msg.ticket_id))
        .send()
        .await;

    match result {
        Ok(out) => out.items().iter().any(|item| {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            // Same staleness bounds as the gateway trigger gate — a zombie
            // run must not block the ticket forever.
            let age_hours = item
                .get("updated_at")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| chrono::Utc::now().signed_duration_since(t).num_hours())
                .unwrap_or(i64::MAX);
            match status {
                "running" | "queued" => age_hours < 2,
                "awaiting_ci" => age_hours < 26,
                _ => false,
            }
        }),
        Err(e) => {
            warn!("Dedup check failed, proceeding: {e}");
            false
        }
    }
}

/// Load must-rules (global + repo-specific) from DynamoDB and merge them.
pub(crate) async fn load_rules(state: &WorkerState, msg: &TicketMessage) -> Vec<String> {
    // Built-in rules that ship for EVERY team (no per-team config needed).
    // First two are safety; the rest are the core code-quality guarantees that
    // apply universally. Teams can add their own on top via RULES#GLOBAL /
    // RULES#REPO below.
    let mut rules = vec![
        "Never push directly to the default/main branch. Always create a feature branch."
            .to_string(),
        "Never commit secrets, credentials, API keys, tokens, or sensitive information. Use environment variables or secret managers instead."
            .to_string(),
        "Reuse existing code before inventing: search the repo for an existing component, utility, or pattern and use it with its ACTUAL API. Never create a parallel version or guess a component's props, a function's signature, or a type's fields — read the definition (or the generated types) first."
            .to_string(),
        "When a run_checks tool is available, call it before finishing and make sure the repo's real build and type-check pass. Never finish with failing type errors."
            .to_string(),
        "Never leave stubs, placeholders, or TODOs — write complete, working code. If you break a file, restore it and fix it properly rather than shipping a partial version."
            .to_string(),
        "Never suppress or fake a check to make it pass: do not use `as any`/`as never`, `@ts-ignore`, `@ts-expect-error`, `eslint-disable`, or a non-null `!` to dodge a real error, and never loosen a type or delete/skip a test just to turn a check green. Fix the actual cause — a green check must mean the code is correct."
            .to_string(),
        "Ground every reference in the repo — don't guess. Before referencing any import path, exported symbol, type field, environment variable, or package script, confirm it exists by reading the file or the generated types. The installed versions in THIS repo are the source of truth, not how a library usually works."
            .to_string(),
        "If you add something a codegen step consumes (a Sanity block/schema, GraphQL type, etc.), update or regenerate the generated types so they include it — otherwise type-checks will fail."
            .to_string(),
        "Stay in ticket scope: change only what the ticket requires. Never fix pre-existing lint warnings, unused imports, style issues, or hook-deps in files your change doesn't touch — even when CI logs mention them. They pre-date this PR and belong in a separate cleanup; note them in your summary instead of fixing them."
            .to_string(),
        "UI work must look right on mobile AND desktop. When a design shows different structures per breakpoint (element order, a separate mobile header/trigger, stacked vs inline), build distinct per-breakpoint structures (e.g. `md:hidden` + `hidden md:flex`) — never one structure with only responsive class tweaks — and match per-breakpoint sizing (full-width buttons, taller tap targets on mobile)."
            .to_string(),
        "Use the right editing tool. edit_file needs old_text to match EXACTLY; a sloppy match silently drops lines (e.g. an array element). For JSON, template literals, or heavy special characters, or when an edit_file call fails more than once on a file, switch to write_file and rewrite the whole file with complete content. Very large generated files (e.g. sanity.types.ts) are not indexed by search_code — read them directly with read_file instead of concluding a type is missing."
            .to_string(),
        "In jsdom / Testing-Library component tests, never select elements with querySelector/querySelectorAll using Tailwind arbitrary-value classes (e.g. `.text-[32px]`, `.w-[50%]`, `.top-[-1px]`) — the square brackets are invalid CSS selector syntax and throw `SyntaxError: '…' is not a valid selector` in jsdom. Query by role, accessible name, text, label, or test id (getByRole/getByText/getByLabelText/getByTestId), or escape the class with CSS.escape. When FIXING such a failing test, change the QUERY approach — do NOT merely swap the tag name (e.g. h3 → h2) while keeping the bracketed class."
            .to_string(),
    ];

    // Load global rules
    if let Some(global) = load_rule_list(state, &msg.team_id, "RULES#GLOBAL").await {
        rules.extend(global);
    }

    // Load repo-specific rules
    let repo_sk = format!("RULES#REPO#{}/{}", msg.repo_owner, msg.repo_name);
    if let Some(repo_rules) = load_rule_list(state, &msg.team_id, &repo_sk).await {
        rules.extend(repo_rules);
    }

    info!(count = rules.len(), "Loaded must-rules");
    rules
}

async fn load_rule_list(state: &WorkerState, team_id: &str, sk: &str) -> Option<Vec<String>> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get("rules"))
            .and_then(|v| v.as_l().ok())
            .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect()),
        Err(e) => {
            warn!(error = %e, "Failed to load rules from {sk}");
            None
        }
    }
}

/// Format must-rules as a string block for injection into system prompts.
pub fn format_rules_block(rules: &[String]) -> String {
    if rules.is_empty() {
        return String::new();
    }
    let mut block =
        String::from("\n\n## Must-Rules (MANDATORY — violating any of these is a failure)\n");
    for rule in rules {
        block.push_str(&format!("- {rule}\n"));
    }
    block
}

/// Format the full OpenSpec as a single context block for downstream passes.
/// Compact OpenSpec summary for review/security passes: proposal, design,
/// and acceptance criteria. Design was previously dropped entirely, so
/// reviewers judged changes with no knowledge of the intended approach.
pub fn format_openspec_summary(plan: &plan::PlanResult) -> String {
    let mut block = String::from("\n\n## OpenSpec Summary\n");
    fn push_capped(block: &mut String, header: &str, text: &str) {
        block.push_str(header);
        if text.len() > 2000 {
            let head = common::truncate_str(text, 2000);
            block.push_str(&head[..head.rfind('\n').unwrap_or(head.len())]);
            block.push_str("\n... (truncated)\n");
        } else {
            block.push_str(text);
            block.push('\n');
        }
    }
    if !plan.proposal.is_empty() {
        push_capped(&mut block, "### Proposal\n", &plan.proposal);
    }
    if !plan.design.is_empty() {
        push_capped(&mut block, "\n### Design\n", &plan.design);
    }
    if !plan.spec.is_empty() {
        block.push_str("\n### Acceptance Criteria\n");
        block.push_str(&plan.spec);
        block.push('\n');
    }
    block
}

pub fn format_openspec_block(plan: &plan::PlanResult) -> String {
    let mut block = String::from("\n\n## OpenSpec\n");
    if !plan.proposal.is_empty() {
        block.push_str("### Proposal\n");
        block.push_str(&plan.proposal);
        block.push('\n');
    }
    if !plan.design.is_empty() {
        block.push_str("\n### Design\n");
        block.push_str(&plan.design);
        block.push('\n');
    }
    if !plan.tasks.is_empty() {
        block.push_str("\n### Tasks\n");
        block.push_str(&plan.tasks);
        block.push('\n');
    }
    if !plan.spec.is_empty() {
        block.push_str("\n### Acceptance Criteria\n");
        block.push_str(&plan.spec);
        block.push('\n');
    }
    block
}

/// Format repo-level instruction files as a system prompt block.
pub fn format_instructions_block(instructions: &str) -> String {
    if instructions.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Repository Instructions (from the repo's instruction files — follow these conventions)\n\n{}",
        instructions
    )
}

/// The team's own instructions for a repository, as people wrote them in
/// CoderHelm: the team-wide ones, then the repository's. Empty when none.
/// Load once per run and pass down (`TicketMessage::team_instructions`).
pub async fn load_team_instructions(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> String {
    let global = load_content(state, team_id, "INSTRUCTIONS#GLOBAL").await;
    let repo = load_content(state, team_id, &format!("INSTRUCTIONS#REPO#{owner}/{name}")).await;
    join_team_instructions(&global, &repo)
}

fn join_team_instructions(global: &str, repo: &str) -> String {
    [global.trim(), repo.trim()]
        .iter()
        .filter(|s| !s.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The section every agent that plans, writes, proves, describes or reviews
/// a change gets for the team's instructions. Empty when there are none.
pub fn team_instructions_block(text: &str) -> String {
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Your team's instructions\nThese are the team's working rules for this repository \
         (how to test, log, describe and commit changes, which conventions and helpers to use). \
         Follow them. Where they conflict with CoderHelm's default instructions, the team's win, \
         except safety rules and read-only data rules, which always hold.\n\n{text}"
    )
}

/// Derive the shell command the execution sandbox runs to verify the repo —
/// its real build / type-check / lint (and tests, last, so a broken build
/// short-circuits before the slow suite). Mirrors what the repo's own CI runs,
/// so "green in the sandbox" tracks "green in CI" — the durable generalization
/// of pinning a toolchain. Returns None when the stack isn't recognized, in
/// which case the sandbox stays off for the run (graceful, never worse).
pub(crate) async fn detect_check_commands(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    // ---- Node / TypeScript (the common case) ----
    if let Ok(pkg_raw) = github.read_file(owner, repo, "package.json", git_ref).await {
        if let Ok(pkg) = serde_json::from_str::<serde_json::Value>(&pkg_raw) {
            let scripts = pkg.get("scripts").and_then(|s| s.as_object());
            let has = |name: &str| scripts.map(|s| s.contains_key(name)).unwrap_or(false);

            // Package manager from whichever lockfile the repo commits.
            let (install, run) = if github
                .file_exists(owner, repo, "pnpm-lock.yaml", git_ref)
                .await
            {
                (
                    "corepack enable && pnpm install --frozen-lockfile",
                    "pnpm run",
                )
            } else if github.file_exists(owner, repo, "yarn.lock", git_ref).await {
                ("corepack enable && yarn install --frozen-lockfile", "yarn")
            } else {
                ("npm ci --no-audit --no-fund", "npm run")
            };

            // Faithful CI replica: prefer the exact type-check command the repo's
            // OWN workflow runs (e.g. `pnpm --filter web check-types`). Guessing
            // from root package.json scripts misses per-workspace commands that
            // are the real gate on monorepos, and running the full `build`
            // instead can OOM. A workspace `tsc --noEmit` is light and catches
            // the type errors CI catches.
            if let Some(ci_typecheck) = detect_ci_typecheck(github, owner, repo, git_ref).await {
                return Some(format!("{install} && {ci_typecheck}"));
            }

            let mut steps = vec![install.to_string()];
            // This command is typecheck + build only — the reliable,
            // self-contained correctness signals. Lint and tests are NOT
            // chained on here:
            //   - lint can hard-fail on a task-config quirk ("Could not find
            //     task lint") that has nothing to do with the change; with `&&`
            //     that would mask the build result. run_checks runs the CI's
            //     own lint command as a separate step instead (`checks::script`),
            //     so its result is reported without hiding this one.
            //   - a full app test suite needs services/env the sandbox lacks, so
            //     it false-REDs; that verification stays CI's job. run_checks
            //     runs only the unit tests the change touched, also as its own
            //     step.
            // typecheck first (fast) so a type error fails before the slower build.
            for script in ["typecheck", "type-check", "build"] {
                if has(script) {
                    steps.push(format!("{run} {script}"));
                }
            }
            // No build/typecheck script but TS is configured — check types anyway.
            let has_compile_step = steps.iter().any(|s| {
                s.contains("build") || s.contains("typecheck") || s.contains("type-check")
            });
            if !has_compile_step
                && github
                    .read_file(owner, repo, "tsconfig.json", git_ref)
                    .await
                    .is_ok()
            {
                steps.push("npx --no-install tsc --noEmit".to_string());
            }
            // Only the install step => still verifies deps resolve (low signal
            // but valid); more steps => the real checks.
            return Some(steps.join(" && "));
        }
    }

    // ---- Rust ----
    if github
        .read_file(owner, repo, "Cargo.toml", git_ref)
        .await
        .is_ok()
    {
        // Compile + compile-check tests (`--no-run`) but don't EXECUTE them —
        // running tests can need services/env the sandbox lacks (false RED),
        // same principle as dropping `test` for Node.
        return Some("cargo build --locked && cargo test --no-run --locked".to_string());
    }
    // ---- Go ----
    if github
        .read_file(owner, repo, "go.mod", git_ref)
        .await
        .is_ok()
    {
        return Some("go build ./... && go vet ./...".to_string());
    }
    // ---- Python ----
    if github
        .read_file(owner, repo, "requirements.txt", git_ref)
        .await
        .is_ok()
    {
        return Some("pip install -r requirements.txt && python -m compileall -q .".to_string());
    }
    if github
        .read_file(owner, repo, "pyproject.toml", git_ref)
        .await
        .is_ok()
    {
        return Some("pip install -e . && python -m compileall -q .".to_string());
    }
    None
}

/// Extract the type-check command the repo's CI actually runs (e.g.
/// `pnpm --filter web check-types`) by scanning `.github/workflows/*.yml`. This
/// is the faithful-CI-replica path: run exactly what CI gates on — it catches
/// per-workspace type errors the root scripts miss, and `tsc --noEmit` is light
/// so it won't OOM like a full monorepo build. Returns None if no workflow
/// type-check step is found.
async fn detect_ci_typecheck(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    let entries = github
        .list_directory(owner, repo, ".github/workflows", git_ref)
        .await
        .ok()?;
    // Collect EVERY distinct type-check command CI runs (a monorepo may check
    // several workspaces), not just the first — otherwise we could run a
    // workspace the change didn't touch and miss the one it did.
    let mut cmds: Vec<String> = Vec::new();
    for entry in entries {
        if !(entry.name.ends_with(".yml") || entry.name.ends_with(".yaml")) {
            continue;
        }
        let Ok(yaml) = github.read_file(owner, repo, &entry.path, git_ref).await else {
            continue;
        };
        // Workflows are line-oriented; a step's `run:` command (inline or under
        // `run: |`) sits on its own line. Scan each line, strip the common YAML
        // prefixes, and keep the ones that are type-check invocations.
        for raw in yaml.lines() {
            let line = raw.trim().trim_start_matches("- ").trim();
            let line = line.strip_prefix("run:").unwrap_or(line).trim();
            let line = line.trim_matches(|c| c == '"' || c == '\'').trim();
            if is_typecheck_cmd(line) && !cmds.iter().any(|c| c == line) {
                cmds.push(line.to_string());
            }
        }
    }
    if cmds.is_empty() {
        None
    } else {
        Some(cmds.join(" && "))
    }
}

/// A single shell line that runs a TypeScript type-check (not install/lint/build).
fn is_typecheck_cmd(line: &str) -> bool {
    let low = line.to_lowercase();
    if low.starts_with('#') {
        return false;
    }
    let looks_typecheck = low.contains("check-types")
        || low.contains("typecheck")
        || low.contains("type-check")
        || (low.contains("tsc") && low.contains("noemit"))
        || (low.contains("tsc") && low.contains("-p "));
    // Must be an actual package-manager/tsc invocation, not prose or a job name.
    let is_cmd = low.contains("pnpm")
        || low.contains("npm ")
        || low.contains("yarn")
        || low.contains("npx")
        || low.contains("turbo")
        || low.starts_with("tsc");
    looks_typecheck && is_cmd
}

/// Detect the repo's OWN unit-test command from its CI workflows — the faithful
/// replica, same approach as `detect_ci_typecheck`. The CI line already encodes
/// the correct workspace scoping/config (e.g. `pnpm --filter web test`), which
/// is why we mirror it rather than guess a runner. Returns the FIRST unit-test
/// invocation; e2e/integration/browser suites are excluded (they need real
/// services and would false-RED in the sandbox — the exact reason tests were
/// originally left out of the gate entirely). Callers scope it to the changed
/// test files via [`build_scoped_test_cmd`] so we never run a full suite.
async fn detect_ci_test(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    let entries = github
        .list_directory(owner, repo, ".github/workflows", git_ref)
        .await
        .ok()?;
    for entry in entries {
        if !(entry.name.ends_with(".yml") || entry.name.ends_with(".yaml")) {
            continue;
        }
        let Ok(yaml) = github.read_file(owner, repo, &entry.path, git_ref).await else {
            continue;
        };
        for raw in yaml.lines() {
            let line = raw.trim().trim_start_matches("- ").trim();
            let line = line.strip_prefix("run:").unwrap_or(line).trim();
            let line = line.trim_matches(|c| c == '"' || c == '\'').trim();
            if is_test_cmd(line) {
                return Some(line.to_string());
            }
        }
    }
    None
}

/// A single shell line that runs a UNIT test suite (vitest/jest or a `test`
/// script) — deliberately NOT e2e/integration/browser suites, which need real
/// services and would false-RED in the sandbox.
fn is_test_cmd(line: &str) -> bool {
    let low = line.to_lowercase();
    if low.starts_with('#') {
        return false;
    }
    // Suites that need a browser/DB/services — skip; these are CI-only by nature.
    if low.contains("e2e")
        || low.contains("playwright")
        || low.contains("cypress")
        || low.contains("integration")
        || low.contains("storybook")
    {
        return false;
    }
    let is_cmd = low.contains("pnpm")
        || low.contains("npm ")
        || low.contains("yarn")
        || low.contains("npx")
        || low.contains("turbo")
        || low.starts_with("vitest")
        || low.starts_with("jest");
    // A direct vitest/jest binary, or a package "test"/"test:unit" script.
    let looks_test = low.contains("vitest")
        || low.contains("jest")
        || line.split_whitespace().any(|t| {
            let t = t.trim_matches(|c| c == '"' || c == '\'');
            // `test:run`, `test:ci`: a package's own unit-test script (the
            // e2e/integration ones are already turned away above).
            t == "test" || t.starts_with("test:") || t == "unit" || t.ends_with(":unit")
        });
    looks_test && is_cmd
}

/// True when `path` is a unit/component test file (by convention). Used to scope
/// the sandbox test run to the tests a change actually touched.
pub(crate) fn is_test_file(path: &str) -> bool {
    let p = path.to_lowercase();
    let base = p.rsplit('/').next().unwrap_or(&p);
    base.contains(".test.") || base.contains(".spec.") || p.contains("__tests__/")
}

/// Scope a repo's unit-test command to specific changed test files by appending
/// their basenames as filename filters (vitest/jest match test paths by
/// substring). `--passWithNoTests` is the safety valve: if the filter matches
/// nothing in the chosen workspace (e.g. a monorepo where the change lives in a
/// different package than the first detected test command), the run passes
/// instead of false-REDing. Returns None when there are no changed test files.
pub(crate) fn build_scoped_test_cmd(base_cmd: &str, changed_files: &[String]) -> Option<String> {
    let mut names: Vec<String> = changed_files
        .iter()
        .filter(|f| is_test_file(f))
        .filter_map(|f| f.rsplit('/').next())
        // Drop anything odd so we never inject shell metacharacters.
        .filter(|n| {
            !n.is_empty()
                && n.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        })
        .map(|n| n.to_string())
        .collect();
    names.sort();
    names.dedup();
    if names.is_empty() {
        return None;
    }
    let args = names.join(" ");
    let low = base_cmd.to_lowercase();
    // A direct vitest/jest binary takes filename filters positionally; a package
    // `test` script needs `--` to forward the args to the underlying runner.
    let direct = low.contains("vitest") || low.contains("jest");
    Some(if direct {
        format!("{base_cmd} --passWithNoTests {args}")
    } else {
        format!("{base_cmd} -- --passWithNoTests {args}")
    })
}

#[cfg(test)]
mod team_instructions_tests {
    use super::{join_team_instructions, team_instructions_block};

    #[test]
    fn team_wide_then_repo_instructions_and_nothing_when_empty() {
        assert_eq!(
            join_team_instructions(" Use make test. ", "Branches: fix/<ticket>.\n"),
            "Use make test.\n\nBranches: fix/<ticket>."
        );
        assert_eq!(join_team_instructions("", " repo only "), "repo only");
        assert_eq!(join_team_instructions(" ", "\n"), "");
        assert_eq!(team_instructions_block(""), "");
        assert_eq!(team_instructions_block(" \n "), "");
        let b = team_instructions_block("Use make test.");
        assert!(b.starts_with("\n\n## Your team's instructions\n"));
        assert!(b.contains("Follow them."));
        assert!(b.contains(
            "the team's win, except safety rules and read-only data rules, which always hold"
        ));
        assert!(b.ends_with("\n\nUse make test."));
    }
}

#[cfg(test)]
mod test_gate_tests {
    use super::{build_scoped_test_cmd, is_test_cmd, is_test_file};

    #[test]
    fn recognizes_unit_test_commands() {
        assert!(is_test_cmd("pnpm --filter web test"));
        assert!(is_test_cmd("pnpm exec vitest run"));
        assert!(is_test_cmd("npx jest"));
        assert!(is_test_cmd("yarn test:unit"));
        assert!(is_test_cmd("pnpm test:run"));
    }

    #[test]
    fn skips_service_dependent_suites() {
        assert!(!is_test_cmd("pnpm test:e2e"));
        assert!(!is_test_cmd("pnpm exec playwright test"));
        assert!(!is_test_cmd("npx cypress run"));
        assert!(!is_test_cmd("pnpm test:integration"));
        // not a command at all
        assert!(!is_test_cmd("# run the tests"));
        assert!(!is_test_cmd("Run the latest build"));
    }

    #[test]
    fn identifies_test_files() {
        assert!(is_test_file(
            "apps/web/src/test/components/split-feature.test.tsx"
        ));
        assert!(is_test_file("src/foo.spec.ts"));
        assert!(is_test_file("src/__tests__/bar.ts"));
        assert!(!is_test_file("apps/web/src/components/split-feature.tsx"));
        assert!(!is_test_file("src/lib/util.ts"));
    }

    #[test]
    fn scopes_vitest_by_basename_positionally() {
        let cmd = build_scoped_test_cmd(
            "pnpm exec vitest run",
            &[
                "apps/web/src/components/split-feature.tsx".to_string(),
                "apps/web/src/test/components/split-feature.test.tsx".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(
            cmd,
            "pnpm exec vitest run --passWithNoTests split-feature.test.tsx"
        );
    }

    #[test]
    fn scopes_test_script_through_double_dash() {
        let cmd = build_scoped_test_cmd("pnpm --filter web test", &["src/a.test.ts".to_string()])
            .unwrap();
        assert_eq!(cmd, "pnpm --filter web test -- --passWithNoTests a.test.ts");
    }

    #[test]
    fn none_when_no_test_files_changed() {
        assert!(build_scoped_test_cmd("pnpm test", &["src/only-source.ts".to_string()]).is_none());
        assert!(build_scoped_test_cmd("pnpm test", &[]).is_none());
    }

    #[test]
    fn rejects_files_with_shell_metacharacters() {
        // A pathological name never reaches the shell.
        let cmd = build_scoped_test_cmd(
            "vitest run",
            &[
                "src/a.test.ts".to_string(),
                "src/$(rm -rf).test.ts".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(cmd, "vitest run --passWithNoTests a.test.ts");
    }
}

/// Detect the Node.js major version the repo expects (package.json `engines.node`
/// or `.nvmrc`) so the sandbox runs the repo's real toolchain. Returns None when
/// it's not a Node repo or the version is unspecified — the buildspec then
/// defaults to a modern Node. Only the major is used (e.g. ">=20.9.0" -> "20").
/// Without this a repo requiring Node >=20 fails `next build` on the image's
/// default Node 18 — a false RED.
pub(crate) async fn detect_node_version(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    fn first_major(s: &str) -> Option<String> {
        let major: String = s
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        (!major.is_empty()).then_some(major)
    }
    if let Ok(pkg_raw) = github.read_file(owner, repo, "package.json", git_ref).await {
        if let Ok(pkg) = serde_json::from_str::<serde_json::Value>(&pkg_raw) {
            if let Some(node) = pkg
                .get("engines")
                .and_then(|e| e.get("node"))
                .and_then(|v| v.as_str())
            {
                if let Some(m) = first_major(node) {
                    return Some(m);
                }
            }
        }
    }
    if let Ok(nvmrc) = github.read_file(owner, repo, ".nvmrc", git_ref).await {
        if let Some(m) = first_major(nvmrc.trim()) {
            return Some(m);
        }
    }
    None
}

/// Detect the repo's codegen command (Sanity typegen, GraphQL codegen, etc.) so
/// the agent can REGENERATE generated files — the thing it otherwise can't do,
/// which makes tickets like "add a Sanity block" (whose block key must land in
/// the generated types union) autonomously completable. Returns None if no
/// codegen script is found (the run_codegen tool then stays off).
pub(crate) async fn detect_codegen_command(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    let pkg_raw = github
        .read_file(owner, repo, "package.json", git_ref)
        .await
        .ok()?;
    let pkg = serde_json::from_str::<serde_json::Value>(&pkg_raw).ok()?;
    let scripts = pkg.get("scripts").and_then(|s| s.as_object());
    let has = |name: &str| scripts.map(|s| s.contains_key(name)).unwrap_or(false);

    let (install, run) = if github
        .file_exists(owner, repo, "pnpm-lock.yaml", git_ref)
        .await
    {
        (
            "corepack enable && pnpm install --frozen-lockfile",
            "pnpm run",
        )
    } else if github.file_exists(owner, repo, "yarn.lock", git_ref).await {
        ("corepack enable && yarn install --frozen-lockfile", "yarn")
    } else {
        ("npm ci --no-audit --no-fund", "npm run")
    };

    // A root codegen script wins.
    for name in [
        "typegen",
        "codegen",
        "sanity-typegen",
        "gen:types",
        "generate:types",
    ] {
        if has(name) {
            return Some(format!("{install} && {run} {name}"));
        }
    }
    // pnpm monorepo: codegen often lives in a workspace (e.g. apps/studio). Run
    // it recursively where present (`--if-present` = no-op elsewhere, exit 0).
    if run == "pnpm run" {
        return Some(format!(
            "{install} && pnpm -r --if-present run typegen && pnpm -r --if-present run codegen"
        ));
    }
    None
}

/// Load a repo's PUBLIC codegen environment (e.g. Sanity's project-id and
/// dataset vars) from team settings, so the sandbox can run codegen that needs
/// project config the secret-free tarball lacks (Sanity's `schema extract` does
/// a CORS check against the real project id — a dummy id fails, the real public
/// one succeeds without a token). Stored per-repo at
/// `SETTINGS#CODEGEN_ENV#<owner>/<repo>` as a String map under attribute `env`.
/// PUBLIC config ONLY — these values ship in the client bundle; a token/secret
/// must never be put here (it would cross the sandbox's secret-free boundary).
pub(crate) async fn load_codegen_env(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
) -> Vec<(String, String)> {
    let sk = format!("SETTINGS#CODEGEN_ENV#{owner}/{repo}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    let Some(item) = item else {
        return Vec::new();
    };
    item.get("env")
        .and_then(|v| v.as_m().ok())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_s().ok().map(|s| (k.clone(), s.clone())))
                .collect()
        })
        .unwrap_or_default()
}

/// Well-known instruction files that coding agents/IDEs use.
const INSTRUCTION_FILES: &[&str] = &[
    "AGENTS.md",
    ".github/AGENTS.md",
    "CLAUDE.md",
    ".claude/CLAUDE.md",
    "COPILOT.md",
    ".github/copilot-instructions.md",
    "copilot-instructions.md",
    ".cursorrules",
    ".github/instructions.md",
];

/// Load repo-level instruction files from the repo's default branch (HEAD).
pub async fn load_repo_instructions(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
) -> String {
    load_repo_instructions_at_ref(github, owner, repo, "HEAD").await
}

/// Load repo-level instruction files (AGENTS.md, CLAUDE.md, copilot-instructions, etc.)
/// at a specific git ref. The reviewer reads them at the PR head so a PR that
/// edits AGENTS.md is reviewed against its own new rules. Capped at 16KB total.
pub async fn load_repo_instructions_at_ref(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> String {
    const MAX_TOTAL_BYTES: usize = 16_000;
    let mut combined = String::new();

    for path in INSTRUCTION_FILES {
        if combined.len() >= MAX_TOTAL_BYTES {
            break;
        }
        match github.read_file(owner, repo, path, git_ref).await {
            Ok(content) if !content.trim().is_empty() => {
                let remaining = MAX_TOTAL_BYTES.saturating_sub(combined.len());
                let truncated: String = if content.len() > remaining {
                    let head = common::truncate_str(&content, remaining);
                    head[..head.rfind('\n').unwrap_or(head.len())].to_string()
                } else {
                    content.clone()
                };
                if !combined.is_empty() {
                    combined.push_str("\n\n---\n\n");
                }
                combined.push_str(&format!("### {path}\n\n{truncated}"));
                info!(
                    path,
                    bytes = truncated.len(),
                    "Loaded repo instruction file"
                );
            }
            _ => {} // File doesn't exist or can't be read — skip silently
        }
    }

    combined
}

/// The repo's instruction files (AGENTS.md, CLAUDE.md, …) at `git_ref`, in full
/// (bounded at 256KB). For scanning what the repo documents — e.g. which PR
/// labels it uses — not for prompts (see load_repo_instructions_at_ref).
pub async fn load_repo_docs_full(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> String {
    const MAX_TOTAL_BYTES: usize = 256_000;
    let mut combined = String::new();
    for path in INSTRUCTION_FILES {
        if combined.len() >= MAX_TOTAL_BYTES {
            break;
        }
        if let Ok(content) = github.read_file(owner, repo, path, git_ref).await {
            combined.push_str(common::truncate_str(
                &content,
                MAX_TOTAL_BYTES - combined.len(),
            ));
            combined.push('\n');
        }
    }
    combined
}

/// Truncate file contents to limit token usage.
/// Keeps first ~32KB (~8K tokens) and appends a truncation notice.
pub fn truncate_content(content: &str, path: &str) -> String {
    const MAX_BYTES: usize = 32_000;
    if content.len() <= MAX_BYTES {
        return content.to_string();
    }
    let head = common::truncate_str(content, MAX_BYTES);
    let cut = head.rfind('\n').unwrap_or(head.len());
    format!(
        "{}\n\n... (truncated — {path} is {} bytes, showing first {cut} bytes)",
        &head[..cut],
        content.len(),
    )
}

/// Truncate file tree output to limit token usage on large repos.
/// Keeps first 500 entries.
pub fn truncate_tree(paths: &[&str]) -> String {
    const MAX_ENTRIES: usize = 500;
    if paths.len() <= MAX_ENTRIES {
        return paths.join("\n");
    }
    let mut out = paths[..MAX_ENTRIES].join("\n");
    out.push_str(&format!(
        "\n\n... ({} more files not shown, {} total)",
        paths.len() - MAX_ENTRIES,
        paths.len(),
    ));
    out
}

/// Load a text content field from single-table DynamoDB (voice, agents, etc.).
pub(crate) async fn load_content(state: &WorkerState, team_id: &str, sk: &str) -> String {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get("content"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        Err(e) => {
            warn!(error = %e, "Failed to load content from {sk}");
            String::new()
        }
    }
}

struct PlanValidation {
    warnings: Vec<String>,
    blocked: bool,
    /// If triage picked the wrong repo, this suggests the correct one (owner/name).
    switch_repo: Option<String>,
    /// Human-readable reason for the switch, shown in the run's progress notes.
    switch_reason: String,
    /// How many distinct files the plan changes (its task checklist; the
    /// plan text when it has no checklist) — used to upgrade a misclassified
    /// "simple" ticket before implement.
    mentioned_file_count: usize,
    /// `(files the plan changes, hard cap)` when the plan is over the cap.
    over_cap: Option<(usize, usize)>,
    /// Files the plan changes that don't exist in the repo (and aren't
    /// created by a task), when enough are missing to warn about.
    missing_files: Vec<String>,
}

/// The note a re-plan gets when the plan named files that don't exist.
fn missing_files_note(missing: &[String], repo: &str) -> String {
    format!(
        "\n\n---\n\n**Planning note (from CoderHelm):** the previous plan for this ticket named \
         files that don't exist in {repo}: {}. Look the paths up with your tools and plan only \
         with files that exist (or tasks that create them, marked as new). If the change doesn't \
         belong in this repository, say so instead of planning code.",
        missing.join(", ")
    )
}

/// A file one plan task changes, read from the `- [ ] <path>: <description>`
/// checklist the planner is required to write.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanTaskFile {
    path: String,
    /// The task creates the file — it legitimately doesn't exist yet.
    is_new: bool,
}

/// Files the plan's task checklist actually changes, deduped, in order.
/// Empty when the tasks don't follow the checklist format.
fn plan_task_files(tasks: &str) -> Vec<PlanTaskFile> {
    let mut out: Vec<PlanTaskFile> = Vec::new();
    for line in tasks.lines() {
        let l = line.trim_start();
        let Some(rest) = l
            .strip_prefix("- [ ]")
            .or_else(|| l.strip_prefix("- [x]"))
            .or_else(|| l.strip_prefix("- [X]"))
        else {
            continue;
        };
        let Some((head, desc)) = rest.split_once(':') else {
            continue;
        };
        let path = head.trim().trim_matches(|c| c == '`' || c == '*').trim();
        let looks_like_path = !path.is_empty()
            && path.len() < 300
            && !path.chars().any(char::is_whitespace)
            && (path.contains('/') || path.contains('.'));
        if !looks_like_path {
            continue;
        }
        let path = path.trim_start_matches("./").to_string();
        let d = desc.to_lowercase();
        let is_new = d.contains("create")
            || d.contains("new file")
            || d.contains("(new)")
            || d.trim_start().starts_with("new ")
            || d.trim_start().starts_with("add new");
        match out.iter_mut().find(|f| f.path == path) {
            Some(f) => f.is_new |= is_new,
            None => out.push(PlanTaskFile { path, is_new }),
        }
    }
    out
}

/// Appended to the ticket for the one narrowed re-plan.
fn narrow_scope_note(planned: usize, cap: usize) -> String {
    format!(
        "\n\n---\nNOTE FROM CODERHELM: the previous plan for this ticket changed {planned} files, \
         over the limit of {cap}. Plan again and change at most {cap} files: only the files this \
         ticket strictly requires. Leave out refactors, renames, cleanups and optional follow-ups."
    )
}

/// Plan validation — catches bad plans before the expensive Implement pass.
/// Uses deterministic checks (scope, file existence, plan-text mentions) plus
/// an LLM repo-fit check as a final safety net.
#[allow(clippy::too_many_arguments)]
async fn validate_plan(
    plan: &plan::PlanResult,
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    base_branch: &str,
    state: &WorkerState,
    team_id: &str,
    team_repos: &[String],
    provider: &provider::ModelProvider,
    usage: &mut TokenUsage,
) -> PlanValidation {
    let mut warnings = Vec::new();
    let mut blocked = false;
    let mut switch_repo: Option<String> = None;
    let mut switch_reason = String::new();
    let mut missing_files: Vec<String> = Vec::new();

    // Extract file paths from plan text (design + tasks)
    let plan_text = format!("{}\n{}", plan.design, plan.tasks);
    let file_re =
        regex::Regex::new(r"(?:^|[\s`\(])([a-zA-Z0-9_./\-]+\.\w{1,5})(?:[\s`\)\:]|$)").unwrap();
    let known_extensions = [
        "rs", "ts", "tsx", "js", "jsx", "py", "go", "java", "rb", "html", "css", "scss", "json",
        "yaml", "yml", "toml", "md", "sql", "sh", "tf", "vue", "svelte",
    ];
    let mut mentioned_files: Vec<String> = file_re
        .captures_iter(&plan_text)
        .filter_map(|cap| {
            let path = cap.get(1)?.as_str().to_string();
            let ext = path.rsplit('.').next().unwrap_or("");
            if known_extensions.contains(&ext) && path.contains('/') {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    mentioned_files.sort();
    mentioned_files.dedup();

    // The files the plan CHANGES are its checklist's task files. Every path
    // the text merely mentions ("follow the pattern in X", "see Y") used to
    // count toward the cap and failed reasonable plans; the regex mentions
    // remain the fallback for a plan without the checklist format.
    let task_files = plan_task_files(&plan.tasks);
    let new_files: std::collections::HashSet<&str> = task_files
        .iter()
        .filter(|f| f.is_new)
        .map(|f| f.path.as_str())
        .collect();
    let changed_files: Vec<String> = if task_files.is_empty() {
        mentioned_files.clone()
    } else {
        task_files.iter().map(|f| f.path.clone()).collect()
    };

    // 1. Scope check
    let max_plan_files = load_workflow_setting_num(state, team_id, "max_plan_files")
        .await
        .unwrap_or(15)
        .min(30) as usize;

    let mut over_cap = None;
    if changed_files.len() > max_plan_files + 10 {
        warnings.push(format!(
            "This plan modifies {} files — exceeds the hard cap of {}. Please break into smaller tickets.",
            changed_files.len(),
            max_plan_files + 10,
        ));
        blocked = true;
        over_cap = Some((changed_files.len(), max_plan_files + 10));
    } else if changed_files.len() > max_plan_files {
        warnings.push(format!(
            "This plan modifies {} files (limit: {}). Consider breaking into smaller tickets.",
            changed_files.len(),
            max_plan_files,
        ));
    }

    // 2. File existence check (sample up to 10 files to avoid API spam).
    // Read at HEAD, same as the candidate-repo checks below — reading the
    // current repo at base_branch while candidates get HEAD meant a repo
    // whose default branch isn't the message's base_branch had ALL its
    // files "missing", which then triggered a bogus repo switch.
    // Files a task CREATES don't exist yet by design — never "hallucinated".
    let existing_expected: Vec<&String> = changed_files
        .iter()
        .filter(|p| !new_files.contains(p.as_str()))
        .collect();
    // The files the plan changes all exist here: that settles the repo. A
    // plan that explains its cause by naming another repo (the API it reads,
    // the pipeline that writes its data) still changes files here, so neither
    // mentions nor the model check below may move it.
    let mut files_here = false;
    if !existing_expected.is_empty() {
        let sample: Vec<&String> = existing_expected.iter().take(10).copied().collect();
        let mut missing = Vec::new();
        for path in &sample {
            match github.read_file(repo_owner, repo_name, path, "HEAD").await {
                Ok(_) => {}
                Err(_) => missing.push(path.to_string()),
            }
        }
        files_here = missing.is_empty();
        let missing_pct = if sample.is_empty() {
            0.0
        } else {
            missing.len() as f64 / sample.len() as f64
        };
        if missing_pct > 0.3 && missing.len() >= 2 {
            warnings.push(format!(
                "Warning: Plan references {} files that don't exist: {}. The plan may be hallucinating.",
                missing.len(),
                missing.join(", "),
            ));
            missing_files = missing.clone();

            // Check if these files exist in another enabled repo (triage may have picked wrong)
            let current_repo = format!("{repo_owner}/{repo_name}");
            let other_repos: Vec<&String> =
                team_repos.iter().filter(|r| *r != &current_repo).collect();

            for candidate in &other_repos {
                let parts: Vec<&str> = candidate.splitn(2, '/').collect();
                if parts.len() != 2 {
                    continue;
                }
                let (c_owner, c_name) = (parts[0], parts[1]);
                let mut found = 0usize;
                for path in &missing {
                    if github
                        .read_file(c_owner, c_name, path, "HEAD")
                        .await
                        .is_ok()
                    {
                        found += 1;
                    }
                }
                if found > missing.len() / 2 {
                    info!(
                        current = %current_repo,
                        candidate = %candidate,
                        found,
                        missing = missing.len(),
                        "Plan files found in different repo — suggesting switch"
                    );
                    switch_repo = Some(candidate.to_string());
                    switch_reason = format!(
                        "{found} of {} plan files exist there but not here",
                        missing.len()
                    );
                    break;
                }
            }
        }
    }

    // 3. Plan-text repo mention check. Only FULL "owner/name" references are
    //    strong enough to decide a switch — an unambiguous repo path in the
    //    plan means the planner found the right repo. Bare repo names are
    //    demoted to a HINT for the LLM check below: repos named after common
    //    words make bare-name counting actively wrong (a ticket about LCP
    //    "for mobile" — the device — routed web work into the repo
    //    named "mobile" because \bmobile\b matched twice).
    let mut mention_hint: Option<String> = None;
    if switch_repo.is_none() && !files_here {
        let current_repo = format!("{repo_owner}/{repo_name}");
        let full_plan = format!(
            "{}\n{}\n{}\n{}",
            plan.proposal, plan.design, plan.tasks, plan.spec
        );
        let full_plan_lower = full_plan.to_lowercase();

        let current_full_mentions = full_plan_lower
            .matches(&current_repo.to_lowercase())
            .count();

        let mut best_full: Option<(String, usize)> = None;
        let mut best_bare: Option<(String, usize)> = None;
        for candidate in team_repos {
            if *candidate == current_repo {
                continue;
            }
            if let Some((_owner, cand_name)) = candidate.split_once('/') {
                let full_count = full_plan_lower.matches(&candidate.to_lowercase()).count();
                if full_count > 0
                    && best_full
                        .as_ref()
                        .is_none_or(|(_, best)| full_count > *best)
                {
                    best_full = Some((candidate.clone(), full_count));
                }

                // Word-boundary bare-name count — hint only, never decides.
                // The pattern varies per candidate, so it can't be hoisted.
                #[allow(clippy::regex_creation_in_loops)]
                let name_re = regex::Regex::new(&format!(
                    r"(?i)\b{}\b",
                    regex::escape(&cand_name.to_lowercase())
                ))
                .unwrap_or_else(|_| regex::Regex::new("^$").unwrap());
                let bare_count = name_re.find_iter(&full_plan_lower).count();
                if bare_count >= 2
                    && best_bare
                        .as_ref()
                        .is_none_or(|(_, best)| bare_count > *best)
                {
                    best_bare = Some((candidate.clone(), bare_count));
                }
            }
        }

        if let Some((candidate, mentions)) = best_full {
            if mentions > current_full_mentions {
                info!(
                    current = %current_repo,
                    candidate = %candidate,
                    mentions,
                    "Plan explicitly references different repo path — suggesting switch"
                );
                switch_repo = Some(candidate);
                switch_reason = format!("plan explicitly references it {mentions}x");
            }
        }
        if switch_repo.is_none() {
            mention_hint = best_bare.map(|(candidate, _)| candidate);
        }
    }

    // 4. LLM repo-fit check — ask the LLM if this plan actually belongs in this repo.
    //    Uses AGENTS#GLOBAL context, repo description, directory listing, and plan summary
    //    to detect mismatches that deterministic checks miss (e.g. new files, same language).
    if switch_repo.is_none() && !files_here && team_repos.len() > 1 {
        let current_repo = format!("{repo_owner}/{repo_name}");

        // Gather context: AGENTS#GLOBAL + repo info + directory listing
        let global_agents = load_content(state, team_id, "AGENTS#GLOBAL").await;

        let (current_lang, current_desc) = github
            .get_repo_info(repo_owner, repo_name)
            .await
            .unwrap_or((None, None));
        let current_dirs = match github
            .list_directory(repo_owner, repo_name, "", base_branch)
            .await
        {
            Ok(entries) => entries
                .iter()
                .take(20)
                .map(|e| e.name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            Err(_) => String::new(),
        };

        // Build candidate repo summaries
        let mut other_repo_info = Vec::new();
        for candidate in team_repos {
            if *candidate == current_repo {
                continue;
            }
            let parts: Vec<&str> = candidate.splitn(2, '/').collect();
            if parts.len() != 2 {
                continue;
            }
            let (cand_lang, cand_desc) = github
                .get_repo_info(parts[0], parts[1])
                .await
                .unwrap_or((None, None));
            let cand_dirs = match github.list_directory(parts[0], parts[1], "", "HEAD").await {
                Ok(entries) => entries
                    .iter()
                    .take(15)
                    .map(|e| e.name.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                Err(_) => String::new(),
            };
            let mut info = format!("  {candidate}");
            if let Some(l) = cand_lang {
                info.push_str(&format!(" (lang: {l})"));
            }
            if let Some(d) = cand_desc {
                info.push_str(&format!(" — {d}"));
            }
            if !cand_dirs.is_empty() {
                info.push_str(&format!("\n    files: [{cand_dirs}]"));
            }
            other_repo_info.push(info);
        }

        let plan_summary = format!(
            "Proposal: {}\nDesign: {}\nTasks: {}",
            common::truncate_str(&plan.proposal, 500),
            common::truncate_str(&plan.design, 500),
            common::truncate_str(&plan.tasks, 500),
        );

        let hint_section = match &mention_hint {
            Some(candidate) => format!(
                "\n## Weak signal (verify, do not trust blindly):\n\
                 The plan text mentions the name of `{candidate}` several times. This may \
                 be a genuine reference to that repository, OR ordinary English usage of \
                 the same word (e.g. a repo named \"mobile\" vs. a ticket about mobile \
                 devices). Judge from the repo descriptions and directory structures.\n"
            ),
            None => String::new(),
        };

        let prompt = format!(
            r#"You are validating whether an implementation plan belongs in the correct repository.

## Current repository: {current_repo}
Language: {lang}
Description: {desc}
Top-level files: [{dirs}]

## Other available repositories:
{others}
{agents_section}{hint_section}
## Plan summary:
{plan_summary}

## Question
Does this plan belong in {current_repo}, or should it be in one of the other repositories?

Rules:
- Consider what each repo is FOR based on its name, description, directory structure, and AGENTS context.
- Data/ETL/pipeline repos are for data work only, NOT application features.
- If the plan mentions a specific service or API (e.g. "billing-api", "payments webhook"), match it to the repo that OWNS that code.
- A repo name appearing as an ordinary word in prose (e.g. "mobile", "web", "data") is NOT evidence the work belongs there.
- If the plan clearly belongs in the current repo, respond: CORRECT
- If it belongs in a different repo, respond: SWITCH owner/name

Respond with ONLY one line: either "CORRECT" or "SWITCH owner/name"."#,
            lang = current_lang.as_deref().unwrap_or("unknown"),
            desc = current_desc.as_deref().unwrap_or("none"),
            dirs = current_dirs,
            others = other_repo_info.join("\n"),
            agents_section = if global_agents.is_empty() {
                String::new()
            } else {
                format!("\n## Repository context (AGENTS#GLOBAL):\n{global_agents}\n")
            },
        );

        let system = "You are a repository routing validator. Be precise — only suggest a switch \
                      if you are confident the plan does NOT belong in the current repo.";

        let mut messages = vec![(
            "user".to_string(),
            vec![serde_json::json!({"type": "text", "text": prompt})],
        )];

        match provider::converse(
            state,
            provider,
            provider.primary_model_id(),
            system,
            &mut messages,
            &[],
            &triage::NoOpExecutor,
            usage,
            llm::ConverseOptions {
                max_turns: 1,
                max_tokens: 128,
                deadline: None,
                effort: Some("low"),
            },
            None,
            None,
        )
        .await
        {
            Ok(response) => {
                let trimmed: &str = response.trim();
                if let Some(rest) = trimmed.strip_prefix("SWITCH") {
                    let clean = rest.trim().trim_matches('`').trim().to_string();
                    if team_repos.iter().any(|r| r == &clean) {
                        info!(
                            current = %current_repo,
                            suggested = %clean,
                            "LLM repo-fit check suggests switching repo"
                        );
                        switch_repo = Some(clean);
                        switch_reason = "repo-fit check matched the plan to it".to_string();
                    } else {
                        warn!(
                            current = %current_repo,
                            suggested = %clean,
                            "LLM suggested repo not in team repos, ignoring"
                        );
                    }
                } else {
                    info!(current = %current_repo, "LLM repo-fit check: CORRECT");
                }
            }
            Err(e) => {
                warn!(error = %e, "LLM repo-fit check failed, proceeding with current repo");
            }
        }
    }

    PlanValidation {
        warnings,
        blocked,
        switch_repo,
        switch_reason,
        mentioned_file_count: changed_files.len(),
        over_cap,
        missing_files,
    }
}

/// Load a numeric workflow setting from SETTINGS#WORKFLOW.
async fn load_workflow_setting_num(state: &WorkerState, team_id: &str, key: &str) -> Option<u64> {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("SETTINGS#WORKFLOW".to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get(key))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok()),
        Err(_) => None,
    }
}

/// Load a boolean workflow setting from SETTINGS#WORKFLOW. Returns default (true) if not found.
async fn load_workflow_setting(state: &WorkerState, team_id: &str, key: &str) -> bool {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("SETTINGS#WORKFLOW".to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get(key))
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Write a per-pass trace record to the traces table.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_pass_trace(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pass_name: &str,
    start_time: std::time::Instant,
    usage_before: &TokenUsage,
    usage_after: &TokenUsage,
    error: Option<&str>,
) {
    if state.config.traces_table_name.is_empty() {
        return;
    }
    let duration_ms = start_time.elapsed().as_millis() as u64;
    let input_tokens = usage_after
        .input_tokens
        .saturating_sub(usage_before.input_tokens);
    let output_tokens = usage_after
        .output_tokens
        .saturating_sub(usage_before.output_tokens);
    let cache_read = usage_after
        .cache_read_tokens
        .saturating_sub(usage_before.cache_read_tokens);
    let cache_write = usage_after
        .cache_write_tokens
        .saturating_sub(usage_before.cache_write_tokens);
    let tool_calls = usage_after
        .tool_calls
        .saturating_sub(usage_before.tool_calls);
    // Collect tool names used in this pass (names in after but not in before)
    let tool_names: Vec<String> = usage_after
        .tool_names
        .iter()
        .filter(|n| !usage_before.tool_names.contains(n))
        .cloned()
        .collect();
    let now = chrono::Utc::now().to_rfc3339();

    // A pass that runs again in the same run (a second plan or implement,
    // a continuation's) gets its own record, numbered like the CI fix
    // passes ("plan:2"), instead of overwriting the first one.
    for repeat in 1..=MAX_TRACE_REPEATS {
        let pass = trace_pass_name(pass_name, repeat);
        let sk = format!("RUN#{run_id}#PASS#{pass}");
        let mut req = state
            .dynamo
            .put_item()
            .table_name(&state.config.traces_table_name)
            .condition_expression("attribute_not_exists(sk)")
            .item("team_id", AttributeValue::S(team_id.to_string()))
            .item("sk", AttributeValue::S(sk))
            .item("pass", AttributeValue::S(pass))
            .item("duration_ms", attr_n(duration_ms))
            .item("input_tokens", attr_n(input_tokens))
            .item("output_tokens", attr_n(output_tokens))
            .item("cache_read_tokens", attr_n(cache_read))
            .item("cache_write_tokens", attr_n(cache_write))
            .item(
                "cost_usd",
                attr_n(format!(
                    "{:.6}",
                    (usage_after.cost_usd - usage_before.cost_usd).max(0.0)
                )),
            )
            .item("tool_calls", attr_n(tool_calls))
            .item("timestamp", AttributeValue::S(now.clone()))
            .item(
                "ttl",
                attr_n(chrono::Utc::now().timestamp() as u64 + 30 * 86400),
            );

        if !tool_names.is_empty() {
            req = req.item(
                "tool_names",
                AttributeValue::L(
                    tool_names
                        .iter()
                        .map(|s| AttributeValue::S(s.clone()))
                        .collect(),
                ),
            );
        }

        if let Some(err) = error {
            req = req.item("error", AttributeValue::S(err.to_string()));
        }

        match req.send().await {
            Ok(_) => return,
            Err(e) => {
                let taken = e
                    .as_service_error()
                    .map(|se| se.is_conditional_check_failed_exception())
                    .unwrap_or(false);
                if !taken {
                    warn!(error = %e, "Failed to write pass trace for {pass_name}");
                    return;
                }
            }
        }
    }
    warn!(
        run_id,
        "Pass {pass_name} ran more than {MAX_TRACE_REPEATS} times — later traces not kept"
    );
}

/// How many records one pass name may have in one run.
const MAX_TRACE_REPEATS: u32 = 50;

/// The trace name of a pass's `repeat`-th run in a run: the first keeps its
/// name, later ones are numbered ("plan:2"), as CI fix passes are. A name
/// that already carries a number ("ci_fix:3") is numbered again only if it
/// repeats.
fn trace_pass_name(pass_name: &str, repeat: u32) -> String {
    if repeat <= 1 {
        pass_name.to_string()
    } else {
        format!("{pass_name}:{repeat}")
    }
}

#[cfg(test)]
mod trace_name_tests {
    use super::trace_pass_name;

    #[test]
    fn a_repeated_pass_gets_its_own_numbered_trace() {
        assert_eq!(trace_pass_name("plan", 1), "plan");
        assert_eq!(trace_pass_name("plan", 2), "plan:2");
        assert_eq!(trace_pass_name("implement", 3), "implement:3");
        // Readers group by the name before ':' (analytics, the run page).
        assert_eq!(trace_pass_name("plan", 2).split(':').next(), Some("plan"));
    }
}

/// Salvage partial work from a timed-out run by creating a draft PR.
/// Returns the PR URL on success.
async fn salvage_timeout_pr(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    usage: &TokenUsage,
    duration: u64,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Check if there's actually a diff on the branch
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;
    let files_changed = diff
        .get("files")
        .and_then(|f| f.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    if files_changed == 0 {
        return Err("No changes on branch to salvage".into());
    }

    // Check for existing PR
    let existing = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let (pr_number, pr_url) = if let Some(pr_data) = existing {
        let n = pr_data.get("number").and_then(|v| v.as_u64()).unwrap_or(0);
        let u = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (n, u)
    } else {
        let conventions = load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
        let title = format!(
            "⚠️ Partial: {}",
            pr::pr_title(&conventions, &ticket_tokens(msg), msg)
        );
        let body = format!(
            "Closes #{}\n\n\
             ⚠️ **This implementation is partial** — the agent ran out of time before finishing all tasks.\n\n\
             Please review what's been done and either:\n\
             - Complete the remaining work manually, or\n\
             - Re-trigger the issue for the agent to continue\n",
            msg.issue_number,
        );
        let pr_data = github
            .create_pull_request(
                &msg.repo_owner,
                &msg.repo_name,
                common::truncate_str(&title, 72),
                &body,
                branch,
                &msg.base_branch,
                true, // draft
            )
            .await?;
        let n = pr_data.get("number").and_then(|v| v.as_u64()).unwrap_or(0);
        let u = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (n, u)
    };

    // Mark run as completed (not failed) — it produced a PR
    let now = chrono::Utc::now().to_rfc3339();
    let cost = usage.estimated_cost();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, pr_url = :pu, pr_number = :pn, \
             branch = :b, tokens_in = :ti, tokens_out = :to, \
             cache_read_tokens = :crt, cache_write_tokens = :cwt, \
             cost_usd = :c, duration_s = :d, updated_at = :t, \
             error_message = :err",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("completed"))
        .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
        .expression_attribute_values(":pu", attr_s(&pr_url))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":ti", attr_n(usage.input_tokens))
        .expression_attribute_values(":to", attr_n(usage.output_tokens))
        .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
        .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
        .expression_attribute_values(":c", attr_n(format!("{cost:.4}")))
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":err", attr_s("Partial implementation — agent timed out"))
        .send()
        .await;

    Ok(pr_url)
}

/// Save a checkpoint after a pass completes so runs can resume on Lambda timeout.
pub(crate) async fn save_checkpoint(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    last_pass: &str,
    branch: &str,
    review_cycle: u8,
    usage: &TokenUsage,
) {
    if state.config.checkpoints_table_name.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp() as u64;
    let ttl = now + 7 * 86400; // 7 day TTL

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.checkpoints_table_name)
        .item("team_id", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(format!("RUN#{run_id}")))
        .item("last_pass", AttributeValue::S(last_pass.to_string()))
        .item("branch", AttributeValue::S(branch.to_string()))
        .item("review_cycle", attr_n(review_cycle as u64))
        .item("tokens_in", attr_n(usage.input_tokens))
        .item("tokens_out", attr_n(usage.output_tokens))
        .item("cache_read", attr_n(usage.cache_read_tokens))
        .item("cache_write", attr_n(usage.cache_write_tokens))
        .item("cost_usd", attr_n(format!("{:.6}", usage.estimated_cost())))
        .item("updated_at", attr_n(now))
        .item("ttl", attr_n(ttl))
        .send()
        .await
    {
        warn!(error = %e, "Failed to save checkpoint after {last_pass}");
    }
}

/// Load an existing checkpoint for a run (if any).
async fn load_checkpoint(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Option<(String, String, u8)> {
    if state.config.checkpoints_table_name.is_empty() {
        return None;
    }
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(format!("RUN#{run_id}")))
        .send()
        .await
        .ok()?;
    let item = result.item()?;
    let last_pass = item.get("last_pass")?.as_s().ok()?.clone();
    let branch = item.get("branch")?.as_s().ok()?.clone();
    let review_cycle = item
        .get("review_cycle")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u8>().ok())
        .unwrap_or(0);
    Some((last_pass, branch, review_cycle))
}

/// Build the progress comment body for the GitHub issue.
fn format_progress(current_phase: &str, run_id: &str) -> String {
    let phases = ["triage", "plan", "implement", "test", "security", "pr"];
    let labels = ["Triage", "Plan", "Implement", "Test", "Security", "PR"];
    let current_idx = phases.iter().position(|p| *p == current_phase);

    let mut rows = String::new();
    for (i, label) in labels.iter().enumerate() {
        let status = match current_idx {
            Some(ci) if i < ci => "✅ Done",
            Some(ci) if i == ci => "🔄 In progress",
            _ => "⏳ Pending",
        };
        rows.push_str(&format!("| {label} | {status} |\n"));
    }

    format!(
        "🔄 **Coderhelm is working on this**\n\n| Phase | Status |\n|-------|--------|\n{rows}\n[View run →](https://app.coderhelm.com/runs/detail?id={run_id})"
    )
}

/// Update the progress comment on the GitHub issue.
async fn update_progress_comment(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    comment_id: Option<u64>,
    phase: &str,
    run_id: &str,
) {
    if let Some(cid) = comment_id {
        let body = format_progress(phase, run_id);
        if let Err(e) = github.edit_issue_comment(owner, repo, cid, &body).await {
            tracing::warn!(error = %e, "Failed to update progress comment");
        }
    }
}

async fn check_token_usage_warning(state: &WorkerState, team_id: &str, _usage: &TokenUsage) {
    use crate::clients::email::EmailEvent;

    // Get team's token limit
    let limit = match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#TOKEN_LIMIT".to_string()),
        )
        .send()
        .await
    {
        Ok(r) => r
            .item()
            .and_then(|item| item.get("max_tokens"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0),
        Err(_) => return,
    };

    if limit == 0 {
        return;
    }

    // Get current month usage from analytics
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let total_used = match state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "period",
            aws_sdk_dynamodb::types::AttributeValue::S(month.clone()),
        )
        .send()
        .await
    {
        Ok(r) => {
            let item = r.item();
            let ti = item
                .and_then(|i| i.get("total_tokens_in"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            let to = item
                .and_then(|i| i.get("total_tokens_out"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            ti + to
        }
        Err(_) => return,
    };

    let usage_pct = ((total_used as f64 / limit as f64) * 100.0) as u8;

    // Check if we already sent a warning this month (prevent spamming)
    let warning_sk = format!(
        "TOKEN_WARNING#{}#{}",
        month,
        if usage_pct >= 100 { "100" } else { "80" }
    );
    let already_sent = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(warning_sk.clone()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .is_some();

    if already_sent {
        return;
    }

    fn format_tokens(n: u64) -> String {
        if n >= 1_000_000 {
            format!("{:.1}M", n as f64 / 1_000_000.0)
        } else if n >= 1_000 {
            format!("{:.1}K", n as f64 / 1_000.0)
        } else {
            n.to_string()
        }
    }

    let event = if usage_pct >= 100 {
        Some(EmailEvent::TokenLimitReached {
            month: month.clone(),
            used_tokens: format_tokens(total_used),
            limit_tokens: format_tokens(limit),
        })
    } else if usage_pct >= 80 {
        Some(EmailEvent::TokenWarning80 {
            month: month.clone(),
            used_tokens: format_tokens(total_used),
            limit_tokens: format_tokens(limit),
            usage_pct,
        })
    } else {
        None
    };

    if let Some(event) = event {
        // Mark warning as sent
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .item("sk", aws_sdk_dynamodb::types::AttributeValue::S(warning_sk))
            .item(
                "sent_at",
                aws_sdk_dynamodb::types::AttributeValue::S(chrono::Utc::now().to_rfc3339()),
            )
            .send()
            .await;

        if let Err(e) = email::send_notification(state, team_id, event).await {
            error!("Failed to send token warning email: {e}");
        }
    }
}

/// Look up Jira project → repo mapping from DynamoDB.
async fn lookup_jira_project_repo(
    state: &WorkerState,
    team_id: &str,
    project_key: &str,
) -> Option<String> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("JIRA#PROJECT#{project_key}")))
        .send()
        .await
        .ok()?
        .item?;

    result
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .cloned()
}

/// Result of looking up the cached plan for a ticket.
struct PlanLookup {
    /// The cached plan, when the ticket is unchanged and every file exists.
    plan: Option<plan::PlanResult>,
    /// True only when a stored context hash exists AND differs from the
    /// current ticket's hash: a real ticket edit.
    hash_changed: bool,
    /// The ticket text (title + body) the cached plan was built from, when
    /// the ticket changed and that text was saved.
    previous_ticket_text: Option<String>,
}

async fn read_s3_text(state: &WorkerState, key: &str) -> Option<String> {
    let output = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(key)
        .send()
        .await
        .ok()?;
    let bytes = output.body.collect().await.ok()?;
    String::from_utf8(bytes.to_vec()).ok()
}

/// Load an existing plan from S3 if one was already generated for this ticket.
/// `plan` is None if no plan exists, any file is missing, or the ticket changed.
async fn load_existing_plan(
    state: &WorkerState,
    team_id: &str,
    ticket_id: &str,
    repo_owner: &str,
    repo_name: &str,
    context_hash: &str,
    plan_key: &str,
) -> PlanLookup {
    let prefix = openspec_prefix(team_id, ticket_id, repo_owner, repo_name);

    // Check if ticket context has changed (body, images, etc.)
    let hash_key = format!("{prefix}/context_hash.txt");
    if let Some(stored_hash) = read_s3_text(state, &hash_key).await {
        if stored_hash.trim() != context_hash {
            info!("Ticket context changed (hash mismatch) — forcing re-plan");
            let previous_ticket_text =
                read_s3_text(state, &format!("{prefix}/{}", plan::TICKET_TEXT_FILE))
                    .await
                    .filter(|t| !t.trim().is_empty());
            return PlanLookup {
                plan: None,
                hash_changed: true,
                previous_ticket_text,
            };
        }
    }

    // Reused only when the plan was made under the same key (same ticket,
    // lessons, history and planner) AND carries the decision on where the
    // fix belongs that plans must now have.
    let stored_key = read_s3_text(state, &format!("{prefix}/{}", plan::PLAN_KEY_FILE)).await;
    let plan = match stored_key {
        Some(k) if k.trim() == plan_key => {
            load_cached_plan_files(state, &prefix).await.filter(|p| {
                let decided = p.fix.is_some();
                if !decided {
                    info!("Cached plan has no decision on where the fix belongs — re-planning");
                }
                decided
            })
        }
        Some(_) => {
            info!("Cached plan was made under a different key (lessons, history or planner changed) — re-planning");
            None
        }
        None => None,
    };
    PlanLookup {
        plan,
        hash_changed: false,
        previous_ticket_text: None,
    }
}

/// One run's triage, kept so a continuation of the run carries on with it.
#[derive(serde::Serialize, serde::Deserialize)]
struct RunTriage {
    triage: triage::TriageResult,
    #[serde(default)]
    used_mcp_ids: Vec<String>,
}

/// A run's own triage and plan, for its continuation.
struct OwnPlan {
    triage: triage::TriageResult,
    used_mcp_ids: Vec<String>,
    plan: plan::PlanResult,
}

/// Where a run's triage is kept: per run, so another run of the ticket
/// never reads it.
fn run_triage_key(team_id: &str, ticket_id: &str, run_id: &str) -> String {
    format!(
        "teams/{team_id}/runs/{}/run_state/{run_id}/triage.json",
        ticket_id.to_lowercase()
    )
}

async fn save_run_triage(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    triage: &triage::TriageResult,
    used_mcp_ids: &[String],
) {
    let Ok(json) = serde_json::to_string(&serde_json::json!({
        "triage": triage,
        "used_mcp_ids": used_mcp_ids,
    })) else {
        return;
    };
    if let Err(e) = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(run_triage_key(&msg.team_id, &msg.ticket_id, run_id))
        .body(json.into_bytes().into())
        .content_type("application/json")
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not keep the run's triage for a continuation");
    }
}

/// This run's own triage and plan, when the cached plan files in the run's
/// repo were written (or taken up) by this very run. None otherwise: the
/// continuation then triages and plans as a fresh run would.
async fn load_run_plan(state: &WorkerState, msg: &TicketMessage, run_id: &str) -> Option<OwnPlan> {
    let saved: RunTriage = serde_json::from_str(
        &read_s3_text(state, &run_triage_key(&msg.team_id, &msg.ticket_id, run_id)).await?,
    )
    .ok()?;
    let prefix = openspec_prefix(
        &msg.team_id,
        &msg.ticket_id,
        &msg.repo_owner,
        &msg.repo_name,
    );
    let owner = read_s3_text(state, &format!("{prefix}/{}", plan::PLAN_RUN_FILE)).await?;
    if owner.trim() != run_id {
        info!(run_id, plan_run = %owner.trim(), "Cached plan belongs to another run — planning again");
        return None;
    }
    let plan = load_cached_plan_files(state, &prefix).await?;
    if plan.tasks.trim().is_empty() {
        return None;
    }
    Some(OwnPlan {
        triage: saved.triage,
        used_mcp_ids: saved.used_mcp_ids,
        plan,
    })
}

/// Puts the real ticket's context hash back on the cached plan after a
/// re-plan whose ticket text carried a planning note.
async fn restore_plan_hash(state: &WorkerState, msg: &TicketMessage, context_hash: &str) {
    let prefix = openspec_prefix(
        &msg.team_id,
        &msg.ticket_id,
        &msg.repo_owner,
        &msg.repo_name,
    );
    let _ = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(format!("{prefix}/context_hash.txt"))
        .body(context_hash.as_bytes().to_vec().into())
        .content_type("text/plain")
        .send()
        .await;
}

/// Moves the run to `repo` before anything is built: the message, its
/// default branch and the run record. No branch is made here. False when
/// `repo` isn't `owner/name`.
async fn switch_run_repo(
    state: &WorkerState,
    msg: &mut TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    repo: &str,
    reason: &str,
) -> bool {
    let Some((owner, name)) = repo
        .split_once('/')
        .filter(|(o, n)| !o.is_empty() && !n.is_empty())
    else {
        return false;
    };
    warn!(
        run_id,
        from = %format!("{}/{}", msg.repo_owner, msg.repo_name),
        to = %repo,
        "Plan belongs in another repo — switching before planning is final"
    );
    msg.repo_owner = owner.to_string();
    msg.repo_name = name.to_string();
    msg.base_branch = github
        .get_default_branch(owner, name)
        .await
        .unwrap_or_else(|_| "main".to_string());
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET repo = :r, team_repo = :tr, base_branch = :b")
        .expression_attribute_values(":r", attr_s(repo))
        .expression_attribute_values(":tr", attr_s(&format!("{}#{repo}", msg.team_id)))
        .expression_attribute_values(":b", attr_s(&msg.base_branch))
        .send()
        .await;
    add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Switched to {repo} ({}) — planning again there",
            if reason.is_empty() {
                "repo routing"
            } else {
                reason
            }
        ),
    )
    .await;
    true
}

async fn load_cached_plan_files(state: &WorkerState, prefix: &str) -> Option<plan::PlanResult> {
    let mut files = std::collections::HashMap::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        match state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = output.body.collect().await.ok()?;
                let text = String::from_utf8(bytes.to_vec()).ok()?;
                if text.trim().is_empty() {
                    return None;
                }
                files.insert(*name, text);
            }
            Err(_) => return None,
        }
    }

    Some(plan::PlanResult {
        proposal: files.remove("proposal.md").unwrap_or_default(),
        design: files.remove("design.md").unwrap_or_default(),
        tasks: files.remove("tasks.md").unwrap_or_default(),
        spec: files.remove("spec.md").unwrap_or_default(),
        repo_tasks: vec![],
        fix: read_s3_text(state, &format!("{prefix}/{}", fix_location::FILE))
            .await
            .and_then(|t| serde_json::from_str(&t).ok()),
    })
}

/// Adversarial test pass — an LLM reads the diff and modified files looking for
/// bugs, broken syntax, missing error handling, and destructive deletions.
/// Returns empty string if clean, or a description of issues for the implement agent to fix.
#[allow(clippy::too_many_arguments)]
async fn adversarial_test(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &crate::clients::github::GitHubClient,
    plan: &plan::PlanResult,
    branch: &str,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    files_modified: &[String],
    run_id: &str,
) -> String {
    use crate::agent::llm;
    use crate::agent::provider;

    let files_list = files_modified.join(", ");
    let openspec_block = format_openspec_summary(plan);

    let system = format!(
        "You are an adversarial code tester for {owner}/{repo}. \
         Your job is to find REAL bugs that would break the build or cause runtime errors. \
         You are READ-ONLY — you cannot modify files. You report issues for another agent to fix.",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = format!(
        r#"An implementation agent just modified these files: {files_list}
{openspec}
Your job: find bugs that WILL break this code. Use `get_diff` to see all changes, then `read_file` on each modified file to verify the FULL file is correct.

Check for:
1. **Syntax errors** — unbalanced braces/brackets/parens, orphaned catch/else blocks, unclosed strings or template literals
2. **Broken imports** — importing symbols that don't exist or were renamed
3. **Type errors** — wrong argument types, missing required properties, calling methods that don't exist on the type
4. **Destructive deletions** — code that was REMOVED but not replaced with equivalent functionality (e.g. exported functions, API endpoints, event handlers that vanished)
5. **Unreachable code** — code after return/throw statements
6. **Missing error handling** — try without catch, async without await, unhandled promise rejections

Do NOT report:
- Style/formatting issues
- Performance suggestions
- Missing tests
- Anything that's a preference not a bug

Read EVERY modified file in full. Do not skip any.

If you find issues, respond with "ISSUES_FOUND:" followed by a detailed list with file paths and line numbers.
If everything is clean, respond with "CLEAN" and nothing else."#,
        files_list = files_list,
        openspec = openspec_block,
    );

    let tools = review::review_tools();
    let executor = review::ReviewToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch,
        base_branch: &msg.base_branch,
        file_cache,
    };

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let response = match provider::converse(
        state,
        provider,
        model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 15,
            max_tokens: 8192,
            deadline: None,
            effort: None,
        },
        None,
        None,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(run_id, error = %e, "Adversarial test errored — skipping");
            return String::new();
        }
    };

    tracing::info!(
        run_id,
        "Adversarial test result: {}",
        common::truncate_str(&response, 200)
    );

    // Fail-closed: unparseable output cannot vouch for the diff.
    if common::parse_verdict(&response, "CLEAN", "ISSUES_FOUND") == common::Verdict::Pass {
        String::new()
    } else {
        format!(
            "CRITICAL: An adversarial tester found the following bugs in your implementation. \
             Read each file mentioned, verify the issue, and fix it. \
             Do NOT remove existing functionality — only fix what's broken.\n\n{}",
            response
        )
    }
}

#[cfg(test)]
mod sanitize_error_tests {
    use super::sanitize_error;

    #[test]
    fn strips_any_model_id() {
        let out = sanitize_error("Anthropic API error 400 (model=claude-opus-5-5): bad model claude-fable-5-1, try claude-sonnet-5-5.");
        assert!(!out.contains("claude-"), "{out}");
        assert!(
            out.starts_with("An error occurred during processing"),
            "{out}"
        );
    }

    #[test]
    fn strips_bedrock_model_ids() {
        let out = sanitize_error(
            "Bedrock error on us.anthropic.claude-opus-5-5, fell back from \
             global.anthropic.claude-haiku-4-5-20251001-v1:0.",
        );
        assert!(!out.contains("claude-"), "{out}");
        assert!(!out.contains("anthropic."), "{out}");
    }
}

#[cfg(test)]
mod ci_outcome_tests {
    use super::CiOutcome;

    #[test]
    fn each_outcome_updates_only_its_own_fields() {
        assert_eq!(
            CiOutcome::FixAttempt.update_expression(),
            "ADD ci_fix_attempts :one SET ci_last = :failed"
        );
        assert_eq!(
            CiOutcome::FixCommitted.update_expression(),
            "ADD ci_fix_commits :one SET ci_fixes_at = list_append(if_not_exists(ci_fixes_at, :empty), :at)"
        );
        assert_eq!(
            CiOutcome::Passed.update_expression(),
            "SET ci_last = :passed"
        );
        assert_eq!(
            CiOutcome::HandedOff.update_expression(),
            "SET handed_off_at = if_not_exists(handed_off_at, :now)"
        );
    }
}

#[cfg(test)]
mod repos_reached_for_tests {
    use super::repos_reached_for;
    use serde_json::json;

    #[test]
    fn a_path_into_another_repo_of_the_org_is_noticed() {
        let log = vec![json!({"role": "assistant", "content": [
            {"type": "tool_use", "name": "read_file", "input": {"path": "acme/tools/skills/debug/SKILL.md"}},
            {"type": "tool_use", "name": "read_file", "input": {"path": "src/app.ts"}},
            {"type": "tool_use", "name": "search_code", "input": {"query": "acme/other/x"}},
            {"type": "tool_use", "name": "read_file", "input": {"path": "acme/api/src/main.rs"}}
        ]})];
        assert_eq!(
            repos_reached_for(&log, "acme", "api"),
            vec!["acme/tools".to_string()]
        );
        assert!(repos_reached_for(&[], "acme", "api").is_empty());
    }
}

#[cfg(test)]
mod ticket_update_tests {
    use super::{format_compare_diff, ticket_text_diff, ticket_update_note};
    use serde_json::json;

    #[test]
    fn diff_marks_removed_added_and_kept_lines() {
        let old = "# Title\n\nShow the badge in red\nKeep footer";
        let new = "# Title\n\nShow the badge in green\nKeep footer\nAdd tooltip";
        let d = ticket_text_diff(old, new);
        assert!(d.contains("- Show the badge in red"));
        assert!(d.contains("+ Show the badge in green"));
        assert!(d.contains("  Keep footer"));
        assert!(d.contains("+ Add tooltip"));
        assert!(!d.contains("- Keep footer"));
    }

    #[test]
    fn diff_of_identical_texts_has_no_changes() {
        assert_eq!(ticket_text_diff("a\nb", "a\nb"), "  a\n  b");
    }

    #[test]
    fn compare_diff_lists_each_file_and_is_bounded() {
        let compare = json!({"files": [
            {"filename": "src/a.rs", "status": "modified", "patch": "@@ -1 +1 @@\n-x\n+y"},
            {"filename": "img.png", "status": "added"}
        ]});
        let d = format_compare_diff(&compare, 10_000);
        assert!(d.contains("--- src/a.rs (modified)"));
        assert!(d.contains("+y"));
        assert!(d.contains("--- img.png (added)"));
        let big = json!({"files": [{"filename": "f", "patch": "x".repeat(50_000)}]});
        assert!(format_compare_diff(&big, 1_000).len() <= 1_100);
    }

    #[test]
    fn update_note_tells_agent_to_update_existing_pr() {
        let n = ticket_update_note(42, Some("# T\n\nold line"), "# T\n\nnew line", "+code");
        assert!(n.contains("UPDATE THE EXISTING PR #42"));
        assert!(n.contains("- old line"));
        assert!(n.contains("+ new line"));
        assert!(n.contains("+code"));
        assert!(n.contains("PR already matches the updated ticket"));
        // Missing previous text still yields a usable note.
        let n = ticket_update_note(7, None, "# T", "");
        assert!(n.contains("previous ticket text was not saved"));
        assert!(n.contains("could not be loaded"));
    }
}

#[cfg(test)]
mod plan_cap_tests {
    use super::*;

    #[test]
    fn counts_only_checklist_task_files() {
        let tasks = "- [ ] src/a.ts: Change the handler (see src/ref.ts for the pattern)\n\
                     - [x] `src/b.ts`: Update import\n\
                     - [ ] src/a.ts: Also add the export\n\
                     Some prose mentioning docs/guide.md\n\
                     - [ ] Verify: nothing";
        let files = plan_task_files(tasks);
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["src/a.ts", "src/b.ts"]);
    }

    #[test]
    fn created_files_are_marked_new() {
        let tasks = "- [ ] src/new_widget.tsx: Create the widget component\n\
                     - [ ] src/index.ts: Export the widget\n\
                     - [ ] test/widget.test.tsx: New test for the widget";
        let files = plan_task_files(tasks);
        assert!(files[0].is_new);
        assert!(!files[1].is_new);
        assert!(files[2].is_new);
    }

    #[test]
    fn plans_without_a_checklist_fall_back() {
        assert!(plan_task_files("Update the handler in src/a.ts and src/b.ts").is_empty());
        assert!(plan_task_files("NO_CHANGES_NEEDED: already done").is_empty());
    }

    #[test]
    fn narrow_note_names_the_cap() {
        let n = narrow_scope_note(31, 25);
        assert!(n.contains("31 files") && n.contains("at most 25 files"));
    }
}

#[cfg(test)]
mod git_conventions_tests {
    use super::{is_internal_ticket, is_own_login, own_pr_branch_for_ticket, ticket_tokens};
    use crate::models::{TicketMessage, TicketSource};
    use serde_json::json;

    fn pr(head: &str, title: &str, author: &str) -> serde_json::Value {
        json!({"head": {"ref": head}, "title": title, "user": {"login": author}})
    }

    /// A PR CoderHelm opened under the old default branch, or under an
    /// earlier template (another slug), is found and its branch kept; a
    /// person's PR naming the ticket is not taken for CoderHelm's.
    #[test]
    fn a_rerun_keeps_the_branch_of_its_open_pr_whatever_the_pattern_was() {
        let prs = json!([
            pr(
                "feature/ABC-12-old-title",
                "ABC-12: Old title",
                "coderhelm[bot]"
            ),
            pr("someone/abc-12", "ABC-12 by hand", "a-person"),
        ]);
        assert_eq!(
            own_pr_branch_for_ticket(&prs, "ABC-12").as_deref(),
            Some("feature/ABC-12-old-title")
        );
        let old_default = json!([pr("coderhelm/abc-12", "ABC-12: Fix", "coderhelm[bot]")]);
        assert_eq!(
            own_pr_branch_for_ticket(&old_default, "ABC-12").as_deref(),
            Some("coderhelm/abc-12")
        );
        let human_only = json!([pr("someone/abc-12", "ABC-12 by hand", "a-person")]);
        assert_eq!(own_pr_branch_for_ticket(&human_only, "ABC-12"), None);
        let other_ticket = json!([pr("fix/abc-13", "ABC-13: Other", "coderhelm[bot]")]);
        assert_eq!(own_pr_branch_for_ticket(&other_ticket, "ABC-12"), None);
    }

    #[test]
    fn coderhelm_is_known_by_its_app_login() {
        assert!(is_own_login("coderhelm[bot]"));
        assert!(is_own_login("CoderHelm-dev[bot]"));
        assert!(!is_own_login("a-person"));
        assert!(!is_own_login(""));
    }

    #[test]
    fn e2e_tickets_keep_the_default_shapes_the_e2e_loop_reads_back() {
        assert!(is_internal_ticket("E2E-FIX-abc"));
        assert!(is_internal_ticket("E2E-GEN-abc"));
        assert!(!is_internal_ticket("ABC-12"));
    }

    #[test]
    fn a_github_issue_is_written_as_hash_number() {
        let mut msg: TicketMessage = serde_json::from_value(json!({
            "team_id": "t", "installation_id": 1, "source": "github",
            "ticket_id": "GH-42", "title": "Fix the drawer", "body": "",
            "repo_owner": "acme", "repo_name": "web", "issue_number": 42,
            "sender": "", "base_branch": "main"
        }))
        .unwrap();
        let t = ticket_tokens(&msg);
        assert_eq!(
            (t.ticket.as_str(), t.ticket_lower.as_str()),
            ("#42", "gh-42")
        );
        msg.source = TicketSource::Jira;
        msg.ticket_id = "ABC-7".into();
        msg.issue_number = 0;
        assert_eq!(ticket_tokens(&msg).ticket, "ABC-7");
    }
}

#[cfg(test)]
mod open_pr_tests {
    use super::mentions_ticket;

    #[test]
    fn a_ticket_key_is_matched_as_a_whole_token() {
        assert!(mentions_ticket("ABC-12: fix the drawer", "ABC-12"));
        assert!(mentions_ticket("coderhelm/abc-12-api", "ABC-12"));
        assert!(mentions_ticket("Fixes [ABC-12]", "abc-12"));
        assert!(!mentions_ticket("ABC-123 something else", "ABC-12"));
        assert!(!mentions_ticket("XABC-12", "ABC-12"));
        assert!(!mentions_ticket("anything", ""));
    }
}
