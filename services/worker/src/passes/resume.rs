use aws_sdk_dynamodb::types::AttributeValue;
use common::run_status::EndedValues;
use serde_json::Value;
use tracing::{error, info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{ResumeMessage, TicketMessage, TicketSource, TokenUsage};
use crate::passes::quarantine::{self, Gate};
use crate::passes::RunUsageValues;
use crate::passes::{
    attr_n, attr_s, implement, plan, review, save_checkpoint, write_pass_trace, FileCache,
};
use crate::WorkerState;
use common::run_round;

const MAX_CI_FIX_CYCLES: usize = 10;
const MAX_LOG_CHARS: usize = 15_000;
/// Cap on how long a run may sit in awaiting_ci before being finalized.
const MAX_AWAITING_CI_HOURS: i64 = 24;
/// Times the CI wait loop may re-arm in one run (about a day of two-minute
/// checks). Counted on the run record, so writing the record on every check
/// can't reset it, as the wall-clock backstop's last-write age could.
const MAX_RESUME_POLLS: i64 = 720;
/// Halt the fix loop once the SAME failure has recurred this many times in a
/// row. This is the real "stop thrashing" signal: if the errors don't change,
/// the fixes are making no progress (noop commits, placeholder stubs, wrong
/// fixes), so churning further just mangles the PR — hand it to a human. Sits
/// UNDER the coarse MAX_CI_FIX_CYCLES ceiling and fires much sooner.
const MAX_STALL_CYCLES: u32 = 2;

/// Deterministic FNV-1a hash — stable across processes/deploys (unlike a
/// randomized hasher), so a stored signature compares correctly next invocation.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A stable signature of WHAT is failing, so "same failure again" (no progress)
/// is distinguishable from "different failure" (the fix changed something).
/// Extracts error lines, normalizes whitespace, sorts + dedupes, then hashes the
/// set — so log timestamps and ordering don't perturb it.
fn ci_failure_signature(logs: &str) -> String {
    // GitHub Actions prefixes EVERY log line with an ISO-8601 timestamp
    // ("2026-07-24T22:01:03.202Z  <content>"). Strip it per line so (a) the
    // anchored ESLint matcher below sees the real content and (b) the captured
    // lines don't drift by timestamp every cycle — that drift is exactly what
    // defeated stall detection and let the fix loop churn to the wall clock.
    let ts = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*").ok();
    // ESLint "stylish" output: a line like `12:5  error  '_' is unused`. Repos
    // that run `--max-warnings 0` fail CI on `warning` lines too. Without this,
    // lint failures matched none of the substring patterns below and fell
    // through to the drifting tail hash.
    let eslint = regex::Regex::new(r"^\d+:\d+\s+(error|warning)\b").ok();
    let mut errs: Vec<String> = logs
        .lines()
        .map(|l| {
            let l = l.trim();
            match &ts {
                Some(re) => re.replace(l, "").into_owned(),
                None => l.to_string(),
            }
        })
        .filter(|l| {
            let low = l.to_ascii_lowercase();
            low.contains("error ts")
                || low.contains(": error")
                || low.starts_with("error:")
                || low.contains("failed with exit")
                || eslint.as_ref().is_some_and(|re| re.is_match(&low))
        })
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    errs.sort_unstable();
    errs.dedup();
    if errs.is_empty() {
        // No recognizable error lines — fall back to a hash of the tail.
        return format!("{:016x}", stable_hash(common::tail_str(logs, 2000)));
    }
    format!("{:016x}", stable_hash(&errs.join("\n")))
}

/// Pure decision: given the previous failure signature + stall count and the
/// current signature, return the new count and whether to HALT. Halts when the
/// exact same failure set has recurred MAX_STALL_CYCLES times — i.e. the fixes
/// are making no progress.
///
/// `last_fix_noop`: the previous fix commit changed nothing (see
/// `fix_made_no_progress`). That cycle was already counted when the fix
/// landed, so a changed signature must not reset the count — the set of
/// failing workflows can shift between cycles while the tree stands still
/// (8 cycles once "restored" a lockfile to the same empty bytes this way).
fn stall_decision(
    prev_sig: &str,
    prev_stall: u32,
    current_sig: &str,
    last_fix_noop: bool,
) -> (u32, bool) {
    if last_fix_noop {
        (prev_stall, prev_stall >= MAX_STALL_CYCLES)
    } else if !prev_sig.is_empty() && prev_sig == current_sig {
        let n = prev_stall + 1;
        (n, n >= MAX_STALL_CYCLES)
    } else {
        (0, false)
    }
}

/// Did a fix cycle leave the code where it was? True when the tree after the
/// fix equals the tree before it (no commit, or files rewritten with
/// identical bytes) or equals a tree an earlier cycle already produced (a
/// change undone, back where CI already failed). Trees are content-addressed,
/// so this is exact. Unknown trees (a lookup failed) never count.
fn fix_made_no_progress(before: Option<&str>, after: Option<&str>, seen: &[String]) -> bool {
    let Some(after) = after.filter(|s| !s.is_empty()) else {
        return false;
    };
    before == Some(after) || seen.iter().any(|t| t == after)
}

/// Pure decision for a fix that just landed: a no-progress fix counts as a
/// stall cycle on its own, whatever CI says next. Returns the new count and
/// whether to HALT now.
fn noop_fix_decision(prev_stall: u32, noop: bool) -> (u32, bool) {
    if noop {
        let n = prev_stall + 1;
        (n, n >= MAX_STALL_CYCLES)
    } else {
        (prev_stall, false)
    }
}

#[cfg(test)]
mod final_squash_tests {
    use super::first_own_commit_message;
    use serde_json::json;

    #[test]
    fn the_final_squash_keeps_coderhelms_first_commit_message() {
        let commits = vec![
            json!({"committer": {"login": "web-flow"}, "author": {"login": "a-person"},
                   "commit": {"message": "wip"}}),
            json!({"committer": {"login": "coderhelm[bot]"}, "author": null,
                   "commit": {"message": "abc-12: fix the drawer"}}),
            json!({"committer": {"login": "coderhelm[bot]"},
                   "commit": {"message": "abc-12: fix the lint"}}),
        ];
        assert_eq!(
            first_own_commit_message(&commits).as_deref(),
            Some("abc-12: fix the drawer")
        );
        assert_eq!(first_own_commit_message(&commits[..1]), None);
    }
}

#[cfg(test)]
mod stall_tests {
    use super::{
        ci_failure_signature, fix_made_no_progress, noop_fix_decision, stall_decision,
        MAX_STALL_CYCLES,
    };

    #[test]
    fn signature_is_order_and_noise_independent() {
        let a = "src/x.tsx(13,50): error TS2344: bad\n2026 12:03:04 noise\nsrc/y.tsx(1,1): error TS2339: nope";
        let b = "99:99 other-timestamp\nsrc/y.tsx(1,1): error TS2339: nope\nsrc/x.tsx(13,50): error TS2344: bad";
        assert_eq!(ci_failure_signature(a), ci_failure_signature(b));
    }

    #[test]
    fn different_errors_produce_different_signatures() {
        let a = "src/x.tsx(1,1): error TS2344: bad";
        let b = "src/x.tsx(1,1): error TS2339: other";
        assert_ne!(ci_failure_signature(a), ci_failure_signature(b));
    }

    #[test]
    fn halts_after_the_same_failure_repeats() {
        let (s1, h1) = stall_decision("", 0, "SIG", false); // first sight
        assert_eq!((s1, h1), (0, false));
        let (s2, _) = stall_decision("SIG", 0, "SIG", false); // 1st repeat
        assert_eq!(s2, 1);
        let (s3, h3) = stall_decision("SIG", 1, "SIG", false); // 2nd repeat → cap
        assert_eq!(s3, MAX_STALL_CYCLES);
        assert!(h3);
    }

    #[test]
    fn progress_resets_the_counter() {
        // errors changed → not stalled, count resets even if it was high.
        assert_eq!(stall_decision("SIG_A", 5, "SIG_B", false), (0, false));
    }

    #[test]
    fn a_noop_fix_is_not_reset_by_a_shifting_signature() {
        // The prod loop: each cycle's failing workflow set differed, so the
        // signature never repeated, while the fix rewrote the same bytes.
        assert_eq!(stall_decision("SIG_A", 1, "SIG_B", true), (1, false));
        assert_eq!(
            stall_decision("SIG_A", MAX_STALL_CYCLES, "SIG_B", true),
            (MAX_STALL_CYCLES, true)
        );
        // Counted once at the fix; the same signature after it adds nothing.
        assert_eq!(stall_decision("SIG", 1, "SIG", true), (1, false));
    }

    #[test]
    fn an_unchanged_or_revisited_tree_is_no_progress() {
        let seen = vec!["t1".to_string(), "t2".to_string()];
        // Identical bytes rewritten (or nothing committed): same tree.
        assert!(fix_made_no_progress(Some("t3"), Some("t3"), &[]));
        // Back to a tree an earlier cycle already had.
        assert!(fix_made_no_progress(Some("t3"), Some("t1"), &seen));
        // A new tree is progress.
        assert!(!fix_made_no_progress(Some("t3"), Some("t4"), &seen));
        // A failed lookup proves nothing.
        assert!(!fix_made_no_progress(Some("t3"), None, &seen));
        assert!(!fix_made_no_progress(None, Some(""), &[String::new()]));
    }

    #[test]
    fn noop_fixes_halt_on_their_own() {
        let (s1, h1) = noop_fix_decision(0, true);
        assert_eq!((s1, h1), (1, false));
        let (s2, h2) = noop_fix_decision(s1, true);
        assert_eq!((s2, h2), (MAX_STALL_CYCLES, true));
        // A fix that changed the tree leaves the count to the signature check.
        assert_eq!(noop_fix_decision(1, false), (1, false));
    }

    #[test]
    fn eslint_failures_are_recognized_and_stable() {
        // A known failure mode: a repo's CI fails on pre-existing ESLint
        // warnings-as-errors. Real GitHub Actions logs prefix EVERY line with an
        // ISO timestamp (which differs every cycle) — the signature must strip it
        // so the same two lint findings hash identically across cycles and stall
        // detection fires instead of churning to the wall-clock timeout.
        let a = "2026-07-24T22:01:03.202Z ./src/services/schoolBreakCampService.ts\n\
                 2026-07-24T22:01:03.210Z   12:5  error  '_' is defined but never used  @typescript-eslint/no-unused-vars\n\
                 2026-07-24T22:01:03.400Z ::group::lint\n\
                 2026-07-24T22:01:03.999Z   8:10  warning  Unexpected ternary  no-ternary";
        let b = "2026-07-24T23:59:59.111Z ./src/services/schoolBreakCampService.ts\n\
                 2026-07-24T23:59:59.222Z   8:10  warning  Unexpected ternary  no-ternary\n\
                 2026-07-24T23:59:59.333Z   12:5  error  '_' is defined but never used  @typescript-eslint/no-unused-vars";
        let sig_a = ci_failure_signature(a);
        assert_eq!(
            sig_a,
            ci_failure_signature(b),
            "same lint failures (differing timestamps) must hash identically"
        );
        // And it must not collapse to the drifting tail-hash fallback: a log with
        // no recognized error/warning lines hashes differently.
        assert_ne!(
            sig_a,
            ci_failure_signature(
                "2026-07-24T22:01:03.202Z just some\n2026-07-24T22:01:03.500Z random output"
            )
        );
    }
}

/// Authoritative CI verdict for a branch head, derived from GitHub's aggregate
/// check-run list. CI *events* are only wake-ups: a repo with N workflows fires
/// N independent `workflow_run` completions, so "a ci_passed arrived" says
/// nothing about the other workflows. A pass on one workflow (e.g. `Lint`) must
/// never be read as "CI is green" while another (e.g. `Test Suite`) is still
/// running or has failed — that exact masking shipped a red PR (run 01KZ5AJW:
/// Lint passed, Test Suite failed, the old `has_ci_fail && !has_ci_pass` guard
/// skipped the fix path). This aggregate is the single source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CiVerdict {
    /// The repo runs no CI on this ref.
    NoChecks,
    /// At least one check is still queued/in_progress — or the check-runs API
    /// call itself failed, which is treated as pending so a transient error is
    /// never read as "green" (fail-closed; the 24h awaiting_ci cap is the
    /// ultimate backstop).
    Pending,
    /// At least one check concluded failure/timed_out.
    Failed,
    /// Every check concluded and none failed.
    Green,
}

/// Pure verdict from a GitHub `check-runs-for-ref` JSON body. Split from the API
/// call so it is unit-testable against crafted payloads. A failure anywhere wins
/// over any number of passes; anything still running keeps the verdict Pending.
fn ci_verdict_from_checks(checks: &Value) -> CiVerdict {
    if checks["total_count"].as_u64().unwrap_or(0) == 0 {
        return CiVerdict::NoChecks;
    }
    let runs = checks["check_runs"].as_array();
    let any_pending = runs
        .map(|rs| {
            rs.iter().any(|r| {
                let s = r["status"].as_str().unwrap_or("");
                s == "in_progress" || s == "queued"
            })
        })
        .unwrap_or(false);
    if any_pending {
        return CiVerdict::Pending;
    }
    let any_failed = runs
        .map(|rs| {
            rs.iter().any(|r| {
                let c = r["conclusion"].as_str().unwrap_or("");
                c == "failure" || c == "timed_out"
            })
        })
        .unwrap_or(false);
    if any_failed {
        CiVerdict::Failed
    } else {
        CiVerdict::Green
    }
}

/// Query GitHub for the aggregate check state on `branch`'s head. A failed API
/// call is treated as `Pending` (fail-closed: wait and re-check rather than
/// falsely concluding "no checks → complete").
async fn evaluate_ci_verdict(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    branch: &str,
) -> CiVerdict {
    match github.list_check_runs_for_ref(owner, name, branch).await {
        Ok(checks) => ci_verdict_from_checks(&checks),
        Err(e) => {
            warn!(owner, name, branch, error = %e, "check-runs query failed — treating CI as pending");
            CiVerdict::Pending
        }
    }
}

/// What a quarantine check does to a CI verdict: a failure made only of
/// quarantined tests counts as green, one whose test report isn't read yet
/// counts as still running. Anything else is left as is.
fn apply_quarantine(verdict: CiVerdict, gate: &Gate) -> CiVerdict {
    match (verdict, gate) {
        (CiVerdict::Failed, Gate::OnlyQuarantined(_)) => CiVerdict::Green,
        (CiVerdict::Failed, Gate::Waiting) => CiVerdict::Pending,
        (v, _) => v,
    }
}

/// [`evaluate_ci_verdict`], with failures made only of quarantined tests
/// treated as passing (see `passes::quarantine`). Returns the quarantined tests
/// when that happened.
#[allow(clippy::too_many_arguments)]
async fn evaluate_ci_verdict_with_quarantine(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    name: &str,
    branch: &str,
    head: Option<&str>,
) -> (CiVerdict, Option<Vec<String>>) {
    let verdict = evaluate_ci_verdict(github, owner, name, branch).await;
    if verdict != CiVerdict::Failed {
        return (verdict, None);
    }
    let gate = quarantine::gate(state, github, team_id, owner, name, head.unwrap_or(branch)).await;
    let names = match &gate {
        Gate::OnlyQuarantined(n) => Some(n.clone()),
        _ => None,
    };
    (apply_quarantine(verdict, &gate), names)
}

/// Where a resume goes once the quarantine check has looked at a CI failure.
#[derive(Debug, PartialEq, Eq)]
enum FailRoute {
    /// Start a fix attempt.
    Fix,
    /// A test report isn't read yet: look again shortly, change nothing.
    Wait,
    /// Only quarantined tests failed: carry on as if CI passed.
    TreatAsPassed(Vec<String>),
    /// Every failing end-to-end test fails for a reason outside the change
    /// and is fixed in a separate pull request: no fix round; the run is
    /// blocked on that fix.
    Blocked(Vec<String>),
}

fn fail_route(gate: Gate) -> FailRoute {
    match gate {
        Gate::Real => FailRoute::Fix,
        Gate::Waiting => FailRoute::Wait,
        Gate::OnlyQuarantined(names) => FailRoute::TreatAsPassed(names),
        Gate::Unrelated(lines) => FailRoute::Blocked(lines),
    }
}

/// The run branch's current head commit, or None when the run record lacks
/// repo/branch or GitHub can't be reached.
async fn current_branch_head(
    state: &WorkerState,
    msg: &ResumeMessage,
    run_record: &std::collections::HashMap<String, AttributeValue>,
) -> Option<String> {
    let repo = run_record.get("repo").and_then(|v| v.as_s().ok())?;
    let (owner, name) = repo.split_once('/')?;
    let branch = run_record
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .filter(|b| !b.is_empty())?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )
    .ok()?;
    match github.get_ref(owner, name, branch).await {
        Ok(sha) => Some(sha),
        Err(e) => {
            warn!(run_id = %msg.run_id, error = %e, "Could not read branch head — CI events filtered by recency only");
            None
        }
    }
}

/// Record a synthetic `ci_failed` event so a follow-up resume routes the run into
/// the fix path. Used when GitHub's aggregate says a check failed but no
/// `ci_failed` webhook event is present yet — a missed webhook, or a straggler
/// that failed after a `ci_passed` already landed. The empty payload carries no
/// `workflow_run_id`, so the fix path falls back to check-run annotations for the
/// failure detail (see the log-collection block in the fix path).
async fn record_synthetic_ci_failed(state: &WorkerState, run_id: &str, head_sha: Option<&str>) {
    let now = chrono::Utc::now();
    let event_sk = format!("EVENT#{}#ci_failed", now.format("%Y%m%dT%H%M%S%.3fZ"));
    let payload = serde_json::json!({ "head_sha": head_sha.unwrap_or("") });
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("RUN#{run_id}")))
        .item("sk", attr_s(&event_sk))
        .item("event_type", attr_s("ci_failed"))
        .item("payload", attr_s(&payload.to_string()))
        .item("processed", AttributeValue::Bool(false))
        .item("created_at", attr_s(&now.to_rfc3339()))
        // The events table's TTL attribute (infra database-stack).
        .item(
            "expires_at",
            attr_n((now.timestamp() as u64 + 30 * 86400).to_string()),
        )
        .send()
        .await
    {
        warn!(run_id, error = %e, "Failed to write synthetic ci_failed event");
    }
}

#[cfg(test)]
mod ci_verdict_tests {
    use super::{apply_quarantine, ci_verdict_from_checks, fail_route, CiVerdict, FailRoute, Gate};
    use serde_json::json;

    #[test]
    fn quarantined_only_failures_never_start_a_fix() {
        assert_eq!(
            fail_route(Gate::OnlyQuarantined(vec!["a".into()])),
            FailRoute::TreatAsPassed(vec!["a".into()])
        );
        assert_eq!(fail_route(Gate::Waiting), FailRoute::Wait);
        assert_eq!(fail_route(Gate::Real), FailRoute::Fix);
        assert_eq!(
            fail_route(Gate::Unrelated(vec!["Blocked on #9".into()])),
            FailRoute::Blocked(vec!["Blocked on #9".into()])
        );
    }

    #[test]
    fn quarantine_turns_only_a_failed_verdict() {
        let only = Gate::OnlyQuarantined(vec!["a".into()]);
        assert_eq!(apply_quarantine(CiVerdict::Failed, &only), CiVerdict::Green);
        assert_eq!(
            apply_quarantine(CiVerdict::Failed, &Gate::Waiting),
            CiVerdict::Pending
        );
        assert_eq!(
            apply_quarantine(CiVerdict::Failed, &Gate::Real),
            CiVerdict::Failed
        );
        assert_eq!(
            apply_quarantine(CiVerdict::Pending, &only),
            CiVerdict::Pending
        );
        assert_eq!(
            apply_quarantine(CiVerdict::NoChecks, &only),
            CiVerdict::NoChecks
        );
    }

    #[test]
    fn no_checks_when_total_zero() {
        assert_eq!(
            ci_verdict_from_checks(&json!({"total_count": 0, "check_runs": []})),
            CiVerdict::NoChecks
        );
    }

    #[test]
    fn green_when_all_completed_and_none_failed() {
        let checks = json!({"total_count": 2, "check_runs": [
            {"status": "completed", "conclusion": "success"},
            {"status": "completed", "conclusion": "success"},
        ]});
        assert_eq!(ci_verdict_from_checks(&checks), CiVerdict::Green);
    }

    #[test]
    fn pending_when_any_still_running() {
        // A "Lint passed, Test Suite still running" snapshot must NOT read green.
        let checks = json!({"total_count": 2, "check_runs": [
            {"status": "completed", "conclusion": "success", "name": "Lint"},
            {"status": "in_progress", "conclusion": null, "name": "Test Suite"},
        ]});
        assert_eq!(ci_verdict_from_checks(&checks), CiVerdict::Pending);
    }

    #[test]
    fn failed_masks_any_number_of_passes() {
        // The 01KZ5AJW shape: one workflow green, the failing one red. Verdict
        // must be Failed regardless of the pass.
        let checks = json!({"total_count": 2, "check_runs": [
            {"status": "completed", "conclusion": "success", "name": "Lint"},
            {"status": "completed", "conclusion": "failure", "name": "Test Suite"},
        ]});
        assert_eq!(ci_verdict_from_checks(&checks), CiVerdict::Failed);
    }

    #[test]
    fn pending_wins_over_a_failure_that_already_landed() {
        // If something failed but another check is still running, stay Pending so
        // the fix path waits for the full failure set before acting.
        let checks = json!({"total_count": 2, "check_runs": [
            {"status": "completed", "conclusion": "failure"},
            {"status": "queued", "conclusion": null},
        ]});
        assert_eq!(ci_verdict_from_checks(&checks), CiVerdict::Pending);
    }

    #[test]
    fn timed_out_counts_as_failed() {
        let checks = json!({"total_count": 1, "check_runs": [
            {"status": "completed", "conclusion": "timed_out"},
        ]});
        assert_eq!(ci_verdict_from_checks(&checks), CiVerdict::Failed);
    }
}

/// Wall-clock duration from run created_at, falling back to Lambda start time.
fn wall_clock_secs(
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    start: &std::time::Instant,
) -> u64 {
    created_at
        .map(|t| {
            chrono::Utc::now()
                .signed_duration_since(t)
                .num_seconds()
                .max(0) as u64
        })
        .unwrap_or_else(|| start.elapsed().as_secs())
}

#[derive(Debug, Clone)]
struct RunEvent {
    sk: String,
    event_type: String,
    payload: Value,
}

/// Split CI events into (current, stale). A ci_failed/ci_passed event is stale
/// when it names a head_sha that is no longer the branch head (a newer push
/// superseded it), or when a later event for the same workflow exists. Events
/// without a head_sha (written before it was recorded) are judged only by
/// recency. Non-CI events always stay current. `events` must be in sk order
/// (oldest first), which is how the events query returns them.
fn split_stale_ci_events(
    events: Vec<RunEvent>,
    branch_head: Option<&str>,
) -> (Vec<RunEvent>, Vec<RunEvent>) {
    let is_ci = |e: &RunEvent| e.event_type == "ci_failed" || e.event_type == "ci_passed";
    let workflow_of = |e: &RunEvent| {
        e.payload["workflow_name"]
            .as_str()
            .unwrap_or("")
            .to_string()
    };
    // Index of the newest CI event per workflow.
    let mut latest: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, e) in events.iter().enumerate() {
        if is_ci(e) {
            latest.insert(workflow_of(e), i);
        }
    }
    let mut current = Vec::new();
    let mut stale = Vec::new();
    for (i, e) in events.into_iter().enumerate() {
        if !is_ci(&e) {
            current.push(e);
            continue;
        }
        let sha = e.payload["head_sha"].as_str().unwrap_or("");
        let old_commit = match branch_head {
            Some(head) => !sha.is_empty() && !sha.eq_ignore_ascii_case(head),
            None => false,
        };
        let superseded = latest.get(&workflow_of(&e)) != Some(&i);
        if old_commit || superseded {
            stale.push(e);
        } else {
            current.push(e);
        }
    }
    (current, stale)
}

#[cfg(test)]
mod stale_ci_event_tests {
    use super::{split_stale_ci_events, RunEvent};
    use serde_json::json;

    fn ev(sk: &str, kind: &str, payload: serde_json::Value) -> RunEvent {
        RunEvent {
            sk: sk.into(),
            event_type: kind.into(),
            payload,
        }
    }

    fn sks(v: &[RunEvent]) -> Vec<&str> {
        v.iter().map(|e| e.sk.as_str()).collect()
    }

    const HEAD: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OLD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn failure_for_an_older_commit_is_stale() {
        let events = vec![
            ev(
                "1",
                "ci_failed",
                json!({"workflow_name": "Test", "head_sha": OLD}),
            ),
            ev(
                "2",
                "ci_failed",
                json!({"workflow_name": "Lint", "head_sha": HEAD}),
            ),
        ];
        let (cur, stale) = split_stale_ci_events(events, Some(HEAD));
        assert_eq!(sks(&cur), vec!["2"]);
        assert_eq!(sks(&stale), vec!["1"]);
    }

    #[test]
    fn only_the_latest_result_per_workflow_counts() {
        let events = vec![
            ev("1", "ci_failed", json!({"workflow_name": "Test"})),
            ev("2", "ci_failed", json!({"workflow_name": "Test"})),
            ev("3", "ci_passed", json!({"workflow_name": "Lint"})),
            ev("4", "ci_passed", json!({"workflow_name": "Test"})),
        ];
        let (cur, stale) = split_stale_ci_events(events, None);
        assert_eq!(sks(&cur), vec!["3", "4"]);
        assert_eq!(sks(&stale), vec!["1", "2"]);
    }

    #[test]
    fn events_without_head_sha_and_non_ci_events_are_kept() {
        let events = vec![
            ev("1", "pr_comment", json!({"body": "please rename"})),
            ev("2", "ci_failed", json!({})),
        ];
        let (cur, stale) = split_stale_ci_events(events, Some(HEAD));
        assert_eq!(sks(&cur), vec!["1", "2"]);
        assert!(stale.is_empty());
    }

    #[test]
    fn unknown_head_filters_by_recency_only() {
        let events = vec![ev(
            "1",
            "ci_failed",
            json!({"workflow_name": "Test", "head_sha": OLD}),
        )];
        let (cur, _) = split_stale_ci_events(events, None);
        assert_eq!(sks(&cur), vec!["1"]);
    }
}

/// Names of the checks that concluded failure or timed_out.
fn failing_check_names(checks: &Value) -> std::collections::BTreeSet<String> {
    checks["check_runs"]
        .as_array()
        .map(|rs| {
            rs.iter()
                .filter(|r| {
                    matches!(
                        r["conclusion"].as_str().unwrap_or(""),
                        "failure" | "timed_out"
                    )
                })
                .filter_map(|r| r["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The head's failing checks split into this PR's own and those that also
/// fail on the base branch's latest commit. A check failing on the base too
/// was red before this PR: not Coderhelm's to fix, and not counted against it.
fn split_base_failures(
    head: &std::collections::BTreeSet<String>,
    base: &std::collections::BTreeSet<String>,
) -> (Vec<String>, Vec<String>) {
    head.iter().cloned().partition(|n| !base.contains(n))
}

/// Re-run a failure once before spending a fix attempt on it? Only when this
/// head was not re-run already and a failing workflow passed earlier on this
/// PR: a check that was green and went red on a small change is the shape a
/// flaky test takes (a random id matching a pattern, a timing race). A check
/// that never passed is fixed straight away.
fn should_rerun(
    head: Option<&str>,
    rerun_heads: &[String],
    failing_workflows: &[String],
    passed_before: &std::collections::HashSet<String>,
) -> bool {
    let Some(head) = head.filter(|h| !h.is_empty()) else {
        return false;
    };
    !rerun_heads.iter().any(|h| h == head)
        && failing_workflows.iter().any(|w| passed_before.contains(w))
}

/// How long a failure waits for the rest of its head's checks to finish
/// before it is fixed anyway (a check can sit queued forever).
const MAX_FULL_CI_WAIT_MINUTES: i64 = 45;

/// When an event arrived, from its key (`EVENT#20261021T231915.657Z#...`).
fn event_time(sk: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let ts = sk.strip_prefix("EVENT#")?.split('#').next()?;
    chrono::NaiveDateTime::parse_from_str(ts, "%Y%m%dT%H%M%S%.3fZ")
        .ok()
        .map(|t| t.and_utc())
}

/// The oldest CI failure among `events` has waited longer than
/// `MAX_FULL_CI_WAIT_MINUTES` for the rest of the checks.
fn waited_long_for_full_ci(events: &[RunEvent], now: chrono::DateTime<chrono::Utc>) -> bool {
    events
        .iter()
        .filter(|e| e.event_type == "ci_failed")
        .filter_map(|e| event_time(&e.sk))
        .min()
        .is_some_and(|t| (now - t).num_minutes() >= MAX_FULL_CI_WAIT_MINUTES)
}

/// Lines kept before each error marker of a job log.
const ERROR_WINDOW_LINES: usize = 80;

/// The part of a failed job's log that explains the failure. GitHub marks
/// the failing step's errors with `##[error]`; the lines just before each
/// marker are the failing step's own output. A long job (an end-to-end
/// suite, a deploy) fails mid-log and then runs its upload and comment
/// steps, so a head-and-tail cut showed only those and the fixer read the
/// wrong part. Logs without markers keep the head and the tail.
fn focus_failing_log(log: &str, budget: usize) -> String {
    let lines: Vec<&str> = log.lines().collect();
    let marks: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.contains("##[error]"))
        .map(|(i, _)| i)
        .collect();
    if marks.is_empty() {
        return common::head_tail_str(log, budget);
    }
    let mut out: Vec<String> = Vec::new();
    let mut shown_to = 0usize;
    for &m in &marks {
        let from = m.saturating_sub(ERROR_WINDOW_LINES).max(shown_to);
        if from > shown_to {
            out.push(format!("… ({} lines not shown)", from - shown_to));
        }
        out.extend(lines[from..=m].iter().map(|l| l.to_string()));
        shown_to = m + 1;
    }
    common::head_tail_str(&out.join("\n"), budget)
}

#[cfg(test)]
mod ci_triage_tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeSet, HashSet};

    fn set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn failing_checks_are_named() {
        let checks = json!({"check_runs": [
            {"name": "Lint", "conclusion": "success"},
            {"name": "Test Suite", "conclusion": "failure"},
            {"name": "Deploy Preview", "conclusion": "timed_out"},
            {"name": "Skipped", "conclusion": "skipped"},
        ]});
        assert_eq!(
            failing_check_names(&checks),
            set(&["Deploy Preview", "Test Suite"])
        );
    }

    #[test]
    fn checks_red_on_base_are_not_this_prs() {
        let (own, base) = split_base_failures(
            &set(&["Lint", "Test Suite"]),
            &set(&["Test Suite", "Other"]),
        );
        assert_eq!(own, vec!["Lint"]);
        assert_eq!(base, vec!["Test Suite"]);
        // Everything red on base: nothing of this PR's own to fix.
        let (own, _) = split_base_failures(&set(&["Test Suite"]), &set(&["Test Suite"]));
        assert!(own.is_empty());
    }

    #[test]
    fn a_once_green_check_is_rerun_once_per_head() {
        let passed: HashSet<String> = ["Test Suite".to_string()].into();
        let failing = vec!["Test Suite".to_string()];
        assert!(should_rerun(Some("abc"), &[], &failing, &passed));
        // Already re-run on this head: spend the attempt.
        assert!(!should_rerun(
            Some("abc"),
            &["abc".to_string()],
            &failing,
            &passed
        ));
        // Never green on this PR: fix it, a re-run would only wait.
        assert!(!should_rerun(
            Some("abc"),
            &[],
            &["Lint".to_string()],
            &passed
        ));
        // Unknown head: can't track the one re-run.
        assert!(!should_rerun(None, &[], &failing, &passed));
    }

    #[test]
    fn the_failing_step_is_kept_not_the_steps_after_it() {
        let mut log: Vec<String> = (0..500).map(|i| format!("setup line {i}")).collect();
        log.push("  1) checkout › pays with a saved card".into());
        log.push("     Error: expected 200, got 500".into());
        log.push("##[error]Process completed with exit code 1.".into());
        log.extend((0..2000).map(|i| format!("upload report chunk {i}")));
        let focused = focus_failing_log(&log.join("\n"), 4000);
        assert!(
            focused.contains("Error: expected 200, got 500"),
            "{focused}"
        );
        assert!(focused.contains("##[error]Process completed"));
        assert!(!focused.contains("upload report chunk"));
        assert!(focused.starts_with("… ("));
    }

    #[test]
    fn a_failure_waits_for_the_full_check_set_but_not_forever() {
        let ev = |sk: &str| RunEvent {
            sk: sk.into(),
            event_type: "ci_failed".into(),
            payload: json!({}),
        };
        let t = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let events = vec![ev("EVENT#20261021T231915.657Z#ci_failed")];
        assert!(!waited_long_for_full_ci(&events, t("2026-10-21T23:30:00Z")));
        assert!(waited_long_for_full_ci(&events, t("2026-10-22T00:05:00Z")));
        // No parsable failure time: keep waiting (the day cap still holds).
        assert!(!waited_long_for_full_ci(
            &[ev("EVENT#garbage")],
            t("2026-10-22T00:05:00Z")
        ));
    }

    #[test]
    fn a_log_without_markers_keeps_head_and_tail() {
        let log: String = (0..3000).map(|i| format!("line {i}\n")).collect();
        let focused = focus_failing_log(&log, 2000);
        assert!(focused.contains("line 0"));
        assert!(focused.contains("line 2999"));
    }
}

/// Resume a run after receiving a webhook event (CI pass/fail, PR comment, etc).
pub async fn run(
    state: &WorkerState,
    msg: ResumeMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start = std::time::Instant::now();

    // Load run record
    info!(run_id = %msg.run_id, table = %state.config.runs_table_name, "Loading run record");
    let run_record = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .send()
        .await
    {
        Ok(resp) => resp.item.ok_or("Run record not found")?,
        Err(e) => {
            error!(run_id = %msg.run_id, "Failed to load run record: {e:?}");
            return Err(format!("{e:?}").into());
        }
    };

    let status = run_record
        .get("status")
        .and_then(|v| v.as_s().ok())
        .unwrap_or(&String::new())
        .clone();

    // Only resume runs that are awaiting_ci
    if status != "awaiting_ci" {
        if status == "running" {
            // Check if the run is stale (running but no progress for > 10 min)
            let updated_at = run_record
                .get("updated_at")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
            let stale = updated_at
                .map(|t| {
                    // Must exceed the worker Lambda's max lifetime (900s): a
                    // 600s threshold let a second resume claim a run whose
                    // 12-minute implement was still alive, double-writing the
                    // same branch.
                    chrono::Utc::now().signed_duration_since(t).num_seconds() > 960
                })
                .unwrap_or(false);

            if stale {
                warn!(
                    run_id = msg.run_id,
                    "Run stuck in running state for >10min — force-resetting to awaiting_ci"
                );
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.runs_table_name)
                    .key("team_id", attr_s(&msg.team_id))
                    .key("run_id", attr_s(&msg.run_id))
                    .update_expression("SET #s = :s, current_pass = :cp, status_run_id = :sri")
                    .condition_expression(common::run_status::not_ended("#s"))
                    .expression_attribute_names("#s", "status")
                    .expression_attribute_values(":s", attr_s("awaiting_ci"))
                    .expression_attribute_values(":cp", attr_s("awaiting_ci"))
                    .expression_attribute_values(
                        ":sri",
                        attr_s(&format!("awaiting_ci#{}", msg.run_id)),
                    )
                    // Never resurrect a cancelled run — the condition makes this
                    // force-reset a no-op once the run is cancelled.
                    .ended_values()
                    .send()
                    .await
                    .ok();
                // Fall through to normal resume logic below
            } else {
                // Run is still being processed by another invocation.
                // Don't re-queue — the safety-net resume (sent after awaiting_ci)
                // and any new webhook events will pick it up. Re-queuing here
                // causes an exponential cascade of bouncing messages.
                info!(
                    run_id = msg.run_id,
                    status, "Run is currently running — dropping resume (safety-net will cover)"
                );
                return Ok(());
            }
        } else {
            // A run handed off while red can still go green afterwards (a
            // person's push, a re-run, a flaky check passing). Record it, or
            // the run counts as "ended red" forever.
            if matches!(status.as_str(), "completed" | "merged") {
                record_late_green(state, &msg, &run_record).await;
            }
            info!(
                run_id = msg.run_id,
                status, "Run not in awaiting_ci status, skipping resume"
            );
            return Ok(());
        }
    }

    // ONE PLATFORM WRITER PER RUN: take the writer slot NOW — before ANY branch
    // write, including the squash / mark-ready fast-paths below (wall-clock cap,
    // missed-webhook CI pass) which call squash_branch, a force-push. Acquiring
    // it here (not just before the status claim) means a slot held by an
    // in-flight feedback pass defers this resume (delayed re-enqueue) instead of
    // squashing away its concurrent commit. Held to the end of the invocation;
    // the guard releases on every exit path (Drop), TTL as the crash backstop.
    let writer_holder = format!(
        "resume#{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut writer_guard = None;
    for attempt in 0..5u32 {
        writer_guard = crate::passes::acquire_run_writer_guard(
            state,
            &msg.team_id,
            &msg.run_id,
            &writer_holder,
        )
        .await;
        if writer_guard.is_some() {
            break;
        }
        info!(
            run_id = msg.run_id,
            attempt, "run writer slot busy — waiting"
        );
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    }
    let _writer_guard = match writer_guard {
        Some(g) => g,
        None => {
            // Another pass (an in-flight feedback or a concurrent resume) holds the
            // per-run writer slot. This is a TRANSIENT condition under the per-run
            // lock, NOT a failure — re-enqueue this resume with a short delay so it
            // retries once the holder releases the slot (or its TTL frees it).
            // Returning Err here would mark the run failed (the SQS handler acks the
            // message either way, so it is never actually redelivered on its own).
            info!(
                run_id = msg.run_id,
                "run writer slot busy — deferring resume via delayed re-enqueue"
            );
            send_delayed_resume(state, &msg, 120).await;
            return Ok(());
        }
    };

    // Parse created_at for wall-clock duration (spans multiple Lambda invocations)
    let created_at = run_record
        .get("created_at")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc));

    // A run that opened pull requests in several repositories waits on each
    // pull request's own CI: its record holds one repo and one branch, which
    // needn't belong together (see `resume_multi_repo`).
    let pr_urls: Vec<String> = run_record
        .get("pr_urls")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    if pr_urls.len() > 1 {
        return resume_multi_repo(state, &msg, &pr_urls, created_at).await;
    }

    // A fix round parked for a fresh invocation runs first, with its whole
    // budget: its own clock, never the run's.
    if let Some(pending) =
        run_round::Round::from_item(&run_record).filter(|r| r.state == run_round::State::Pending)
    {
        return run_pending_round(state, &msg, &run_record, pending, created_at, &start).await;
    }

    // Wall-clock backstop: the reschedule loop has no cycle cap of its own
    // (unlike CI fixes), so a check that never concludes would keep a run in
    // awaiting_ci forever. Past the cap, hand the PR to a human as-is.
    // Age = time since the last state write (updated_at), NOT run creation:
    // a re-review on a days-old run re-enters awaiting_ci with an ancient
    // created_at, and the creation-age version finalized it instantly —
    // skipping the CI result it had just been handed.
    let age_hours = run_record
        .get("updated_at")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| chrono::Utc::now().signed_duration_since(t).num_hours())
        .or_else(|| created_at.map(|t| chrono::Utc::now().signed_duration_since(t).num_hours()))
        .unwrap_or(0);
    if age_hours >= MAX_AWAITING_CI_HOURS {
        warn!(
            run_id = msg.run_id,
            age_hours, "Run exceeded awaiting_ci wall-clock cap — finalizing for human review"
        );
        let repo_full = run_record
            .get("repo")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let pr_number = run_record
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        if let (Some((owner, name)), true) = (repo_full.split_once('/'), pr_number > 0) {
            if let Ok(github) = GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                msg.installation_id,
                &state.http,
            ) {
                mark_pr_ready(state, &msg.team_id, &github, owner, name, pr_number).await;
            }
        }
        super::add_progress_note(
            state,
            &msg.team_id,
            &msg.run_id,
            "CI did not conclude within 24h — PR handed off for human review",
        )
        .await;
        set_run_complete(state, &msg.team_id, &msg.run_id, created_at).await;
        return Ok(());
    }

    // Load all unprocessed events
    info!(run_id = %msg.run_id, events_table = %state.config.events_table_name, "Loading events");
    let events = match load_unprocessed_events(state, &msg.run_id).await {
        Ok(evts) => evts,
        Err(e) => {
            error!(run_id = %msg.run_id, "Failed to load events: {e:?}");
            return Err(e);
        }
    };
    // The branch head right now: CI results for any older commit are stale and
    // must not be "fixed" again (that replayed failures a later push had
    // already fixed). A failed lookup filters by recency only.
    let branch_head = current_branch_head(state, &msg, &run_record).await;
    let (events, stale_events) = split_stale_ci_events(events, branch_head.as_deref());
    if !stale_events.is_empty() {
        info!(
            run_id = %msg.run_id,
            stale = stale_events.len(),
            head = branch_head.as_deref().unwrap_or(""),
            "Dropping stale CI events (older commit or superseded by a later result)"
        );
        mark_events_processed(state, &msg.run_id, &stale_events).await;
    }
    // The ticket's outside steps CoderHelm runs (a deploy, the end-to-end
    // run, a staging migration): an approval answered or a workflow ended
    // since the last look carries them on. A failed end-to-end run goes to
    // the CI fix round; while a step is under way the run doesn't finish
    // (a CI failure is still fixed meanwhile).
    match super::outside_exec::on_resume(state, &msg, &run_record).await {
        super::outside_exec::OnResume::Failed(f) => {
            info!(
                run_id = msg.run_id,
                workflow_run_id = f.workflow_run_id,
                "Started end-to-end run failed — sending it to the CI fix round"
            );
            super::outside_exec::record_failed(state, &msg.run_id, &f).await;
            send_delayed_resume(state, &msg, 0).await;
            return Ok(());
        }
        super::outside_exec::OnResume::Waiting
            if !events.iter().any(|e| e.event_type == "ci_failed") =>
        {
            info!(
                run_id = msg.run_id,
                "An outside step waits on an approval or a workflow run — checking again later"
            );
            send_delayed_resume(state, &msg, 120).await;
            return Ok(());
        }
        _ => {}
    }
    if events.is_empty() {
        // No events — webhook may have been missed or repo has no CI.
        // Only check GitHub if we've been in awaiting_ci for >90 seconds,
        // otherwise we'd see stale check results from the previous commit.
        let updated_at = run_record
            .get("updated_at")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
        let secs_since_update = updated_at
            .map(|t| chrono::Utc::now().signed_duration_since(t).num_seconds())
            .unwrap_or(0);

        if secs_since_update < 90 {
            info!(
                run_id = msg.run_id,
                secs_since_update,
                "No events but run just entered awaiting_ci — waiting for CI webhooks"
            );
            send_delayed_resume(state, &msg, 90).await;
            return Ok(());
        }

        info!(
            run_id = msg.run_id,
            "No unprocessed events — checking GitHub PR status"
        );

        let repo_owner = run_record
            .get("repo")
            .and_then(|v| v.as_s().ok())
            .and_then(|r| r.split('/').next())
            .unwrap_or("")
            .to_string();
        let repo_name = run_record
            .get("repo")
            .and_then(|v| v.as_s().ok())
            .and_then(|r| r.split('/').nth(1))
            .unwrap_or("")
            .to_string();
        let branch = run_record
            .get("branch")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let pr_number = run_record
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);

        if repo_owner.is_empty() || repo_name.is_empty() || branch.is_empty() {
            info!(
                run_id = msg.run_id,
                "Missing repo/branch info, can't check CI"
            );
            return Ok(());
        }

        let github = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        )?;

        // Aggregate check state is the source of truth (see CiVerdict). A failed
        // API call yields Pending here — never a false "no checks → complete".
        // A failure made only of quarantined tests counts as green.
        let (verdict, quarantined) = evaluate_ci_verdict_with_quarantine(
            state,
            &github,
            &msg.team_id,
            &repo_owner,
            &repo_name,
            &branch,
            branch_head.as_deref(),
        )
        .await;
        match verdict {
            CiVerdict::NoChecks => {
                // No CI workflows at all — mark PR ready and complete
                info!(
                    run_id = msg.run_id,
                    "No CI checks found — marking PR ready and completing"
                );
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    super::CiOutcome::Passed,
                )
                .await;
                ready_or_prove(
                    state,
                    &github,
                    &msg.team_id,
                    &repo_owner,
                    &repo_name,
                    pr_number,
                )
                .await;
                set_run_complete(state, &msg.team_id, &msg.run_id, created_at).await;
                return Ok(());
            }
            CiVerdict::Pending => {
                // CI still running — send another delayed resume to check again later
                info!(
                    run_id = msg.run_id,
                    "CI still in progress — scheduling re-check"
                );
                send_delayed_resume(state, &msg, 120).await;
                return Ok(());
            }
            CiVerdict::Failed => {
                // CI failed but the webhook was missed — record a synthetic event
                // and re-trigger so the next resume routes into the fix path.
                info!(
                    run_id = msg.run_id,
                    "CI failed (missed webhook) — writing event and re-triggering"
                );
                record_synthetic_ci_failed(state, &msg.run_id, branch_head.as_deref()).await;
                send_delayed_resume(state, &msg, 0).await;
                return Ok(());
            }
            CiVerdict::Green => {
                // All checks passed but webhook was missed — mark ready and complete
                info!(
                    run_id = msg.run_id,
                    "CI passed (missed webhook) — marking PR ready and completing"
                );
                // A green CI counts however Coderhelm learned of it: without
                // this, a fixed run that missed its webhook read "ended red".
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    passed_outcome(&run_record, branch_head.as_deref()),
                )
                .await;
                if let Some(names) = &quarantined {
                    super::add_progress_note(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        &quarantine::passing_line(names),
                    )
                    .await;
                }
                super::pr::settle_unverified_by_ci(&github, &repo_owner, &repo_name, pr_number)
                    .await;
                ready_or_prove(
                    state,
                    &github,
                    &msg.team_id,
                    &repo_owner,
                    &repo_name,
                    pr_number,
                )
                .await;
                set_run_complete(state, &msg.team_id, &msg.run_id, created_at).await;
                return Ok(());
            }
        }
    }

    // Determine what happened
    let mut has_ci_pass = events.iter().any(|e| e.event_type == "ci_passed");
    let mut has_ci_fail = events.iter().any(|e| e.event_type == "ci_failed");
    let has_review_feedback = events
        .iter()
        .any(|e| e.event_type == "pr_review" || e.event_type == "pr_comment");

    info!(
        run_id = msg.run_id,
        event_count = events.len(),
        has_ci_pass,
        has_ci_fail,
        has_review_feedback,
        "Processing resume events"
    );

    // Extract run context from record
    let branch = run_record
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .ok_or("Missing branch on run record")?
        .clone();
    let repo_owner = run_record
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    let repo_name = run_record
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .and_then(|r| r.split('/').nth(1))
        .unwrap_or("")
        .to_string();
    let pr_number = run_record
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let pr_url = run_record
        .get("pr_url")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let ticket_id = run_record
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let title = run_record
        .get("title")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let issue_number = run_record
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let base_branch = run_record
        .get("base_branch")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "main".to_string());

    // (writer slot already held — acquired right after the awaiting_ci gate so
    // it also covers the squash fast-paths above.)

    // Quarantined tests don't fail Coderhelm's decisions: when every failing
    // check failed only on quarantined tests, there's nothing to fix and the
    // run carries on as if CI passed. Decided before claiming the run, so a
    // wait for an unread test report leaves the run untouched.
    //
    // Before that, and before any fix attempt is spent, the whole check set on
    // the CURRENT head decides (events are only wake-ups):
    //  - still running → wait for the full set: fixing on the first failure
    //    to land fed the fixer a partial set, so the stall signature drifted
    //    every cycle and never caught a loop;
    //  - green → the failure belonged to an older commit (an event without a
    //    head sha can't be judged stale otherwise): nothing to fix;
    //  - failed → checks also failing on the base branch are not this PR's;
    //    and a check that passed earlier on this PR is re-run once first.
    let mut on_base: Vec<String> = Vec::new();
    let mut red_on_base_only = false;
    if has_ci_fail && !repo_owner.is_empty() && !repo_name.is_empty() {
        if let Ok(gh) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            let git_ref = branch_head.as_deref().unwrap_or(&branch).to_string();
            match gh
                .list_check_runs_for_ref(&repo_owner, &repo_name, &git_ref)
                .await
            {
                Ok(checks) => match ci_verdict_from_checks(&checks) {
                    CiVerdict::Pending if !waited_long_for_full_ci(&events, chrono::Utc::now()) => {
                        info!(
                            run_id = msg.run_id,
                            "A check failed but others still run on this head — waiting for the full set before fixing"
                        );
                        send_delayed_resume(state, &msg, 90).await;
                        return Ok(());
                    }
                    // A check stuck queued must not hold the fix for a day.
                    CiVerdict::Pending => {}
                    CiVerdict::Green => {
                        info!(
                            run_id = msg.run_id,
                            "The failure is not on the current head, which is green — treating CI as passed"
                        );
                        has_ci_fail = false;
                        has_ci_pass = true;
                    }
                    CiVerdict::NoChecks => {}
                    CiVerdict::Failed => {
                        let head_failing = failing_check_names(&checks);
                        let base_failing = match gh
                            .list_check_runs_for_ref(&repo_owner, &repo_name, &base_branch)
                            .await
                        {
                            Ok(b) => failing_check_names(&b),
                            Err(e) => {
                                warn!(run_id = msg.run_id, error = %e, "Could not read the base branch's checks");
                                Default::default()
                            }
                        };
                        let (own, base) = split_base_failures(&head_failing, &base_failing);
                        on_base = base;
                        red_on_base_only = own.is_empty() && !on_base.is_empty();
                        if !red_on_base_only
                            && try_rerun_once(
                                state,
                                &msg,
                                &gh,
                                &run_record,
                                &repo_owner,
                                &repo_name,
                                branch_head.as_deref(),
                                &events,
                            )
                            .await
                        {
                            return Ok(());
                        }
                    }
                },
                Err(e) => {
                    warn!(run_id = msg.run_id, error = %e, "Could not read the head's checks — fixing from the events");
                }
            }
        }
    }

    let mut quarantined_only: Option<Vec<String>> = None;
    let mut blocked_on: Option<Vec<String>> = None;
    if has_ci_fail && !red_on_base_only && !repo_owner.is_empty() && !repo_name.is_empty() {
        if let Ok(gh) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            let git_ref = branch_head.as_deref().unwrap_or(&branch);
            let gate =
                quarantine::gate(state, &gh, &msg.team_id, &repo_owner, &repo_name, git_ref).await;
            match fail_route(gate) {
                FailRoute::Fix => {}
                FailRoute::Wait => {
                    info!(
                        run_id = msg.run_id,
                        "CI failed; waiting for the test report to tell whether only quarantined tests failed"
                    );
                    send_delayed_resume(state, &msg, 60).await;
                    return Ok(());
                }
                FailRoute::TreatAsPassed(names) => {
                    info!(
                        run_id = msg.run_id,
                        tests = ?names,
                        "Only quarantined tests failed — treating CI as passed"
                    );
                    has_ci_fail = false;
                    has_ci_pass = true;
                    quarantined_only = Some(names);
                }
                FailRoute::Blocked(lines) => {
                    info!(
                        run_id = msg.run_id,
                        "Every E2E failure is outside this change and fixed separately; no fix round"
                    );
                    blocked_on = Some(lines);
                }
            }
        }
    }

    // Atomically claim this run: transition awaiting_ci → running.
    // If another Resume Lambda already claimed it, this fails and we exit.
    let claim_result = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression(
            "SET #s = :new_s, status_run_id = :sri, current_pass = :cp, updated_at = :t",
        )
        .condition_expression("#s = :expected")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":expected", attr_s("awaiting_ci"))
        .expression_attribute_values(":new_s", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{}", msg.run_id)))
        .expression_attribute_values(":cp", attr_s("test"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;

    if claim_result.is_err() {
        info!(
            run_id = msg.run_id,
            "Could not claim run (already picked up by another Resume), skipping"
        );
        return Ok(());
    }

    let FixContext {
        ticket_msg,
        provider,
        github,
        mut usage,
        rules,
        repo_instructions,
    } = fix_context(
        state,
        &msg,
        &run_record,
        &FixTarget {
            owner: &repo_owner,
            repo: &repo_name,
            base_branch: &base_branch,
            ticket_id: &ticket_id,
            title: &title,
            issue_number,
        },
    )
    .await?;

    // Collect any review feedback (comments on the PR) — useful for both CI fix and review paths
    let review_feedback = collect_review_feedback(&events);

    if has_ci_fail {
        // A workflow failed — try to fix. A failure takes precedence over any
        // pass: on a multi-workflow repo (e.g. Lint + Test Suite),
        // Lint passing must NOT mask Test Suite failing. The old guard
        // (`has_ci_fail && !has_ci_pass`) skipped this path whenever any workflow
        // passed, shipping red PRs as "completed" (run 01KZ5AJW).
        if !on_base.is_empty() {
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::RedOnBase(on_base.clone()),
            )
            .await;
        }
        if let Some(lines) = &blocked_on {
            // The failures aren't this change's: they are fixed in a
            // separate pull request, and this one waits for it as a draft.
            super::add_progress_note(state, &msg.team_id, &msg.run_id, &lines.join(" ")).await;
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::HandedOff(super::GaveUp::UnrelatedTest),
            )
            .await;
            mark_events_processed(state, &msg.run_id, &events).await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;
            return Ok(());
        }
        if red_on_base_only {
            // Every failing check fails on the base branch too: red before
            // this PR. Not a fix attempt, and not Coderhelm's to "fix".
            info!(
                run_id = msg.run_id,
                checks = ?on_base,
                "CI is red only on checks that fail on the base branch too — handing off"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &format!(
                    "CI is red only on checks that also fail on {base_branch} ({}) — not caused by this PR; handed off for human review.",
                    on_base.join(", ")
                ),
            )
            .await;
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::HandedOff(super::GaveUp::RedOnBase),
            )
            .await;
            mark_events_processed(state, &msg.run_id, &events).await;
            mark_pr_ready(
                state,
                &msg.team_id,
                &github,
                &repo_owner,
                &repo_name,
                pr_number,
            )
            .await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;
            return Ok(());
        }
        info!(run_id = msg.run_id, "CI failed — attempting fix");
        super::record_ci_outcome(
            state,
            &msg.team_id,
            &msg.run_id,
            super::CiOutcome::FixAttempt,
        )
        .await;

        // Collect failure logs from events
        let failure_logs = collect_ci_failure_info(&events);

        // Download actual workflow logs via GitHub API
        // Collect all genuinely failed workflow run IDs (skip "skipped" conclusions)
        let failed_run_ids: Vec<u64> = events
            .iter()
            .filter(|e| e.event_type == "ci_failed")
            .filter(|e| {
                let conclusion = e.payload["conclusion"].as_str().unwrap_or("");
                conclusion == "failure" || conclusion == "timed_out"
            })
            .filter_map(|e| e.payload["workflow_run_id"].as_u64())
            .collect();

        let mut all_logs = Vec::new();
        for wf_run_id in &failed_run_ids {
            match github
                .get_workflow_run_logs(&repo_owner, &repo_name, *wf_run_id)
                .await
            {
                Ok(l) => {
                    // The failing step's own output (see focus_failing_log);
                    // head+tail when the log has no error markers, since
                    // compile cascades put the root cause at the head.
                    let budget = MAX_LOG_CHARS / failed_run_ids.len().max(1);
                    all_logs.push(focus_failing_log(&l, budget));
                }
                Err(e) => {
                    warn!(
                        workflow_run_id = wf_run_id,
                        "Failed to download workflow logs: {e}"
                    );
                }
            }
        }

        let logs = if !all_logs.is_empty() {
            all_logs.join("\n\n--- Next workflow ---\n\n")
        } else {
            // Fallback: fetch annotations from failed check runs (uses checks:read, not actions:read)
            warn!("No workflow logs available — falling back to check run annotations");
            match github
                .get_check_run_annotations(&repo_owner, &repo_name, &branch)
                .await
            {
                Ok(annotations) if !annotations.is_empty() => {
                    info!(
                        run_id = msg.run_id,
                        len = annotations.len(),
                        "Got check run annotations as fallback"
                    );
                    format!("{failure_logs}\n\nCheck run annotations:\n{annotations}")
                }
                _ => failure_logs.clone(),
            }
        };

        // Load checkpoint for review cycle count
        let cycle = load_cycle_from_checkpoint(state, &msg.team_id, &msg.run_id).await;
        let over_cycle = cycle >= MAX_CI_FIX_CYCLES;
        // Atomically claim a shared auto-fix slot (skipped when the cycle cap has
        // already tripped, so we don't consume a slot on the way out).
        let claimed =
            !over_cycle && super::claim_auto_fix_slot(state, &msg.team_id, &msg.run_id).await;
        if over_cycle || !claimed {
            warn!(
                run_id = msg.run_id,
                cycle, "Fix budget reached — completing for human review"
            );
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::HandedOff(super::GaveUp::Budget),
            )
            .await;
            // Every terminal path consumes its events; left unprocessed, the
            // next resume (e.g. after a re-review) replayed these failures.
            mark_events_processed(state, &msg.run_id, &events).await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;

            // Still mark PR ready — let human review
            mark_pr_ready(
                state,
                &msg.team_id,
                &github,
                &repo_owner,
                &repo_name,
                pr_number,
            )
            .await;
            return Ok(());
        }

        // Halt-on-no-progress: if the SAME failure keeps recurring, the fixes
        // aren't working (noop commits, placeholder stubs, wrong fixes). Stop
        // churning the PR and hand it to a human — this fires long before the
        // coarse cycle cap above.
        if check_and_record_stall(state, &msg.team_id, &msg.run_id, &logs).await {
            warn!(
                run_id = msg.run_id,
                "CI fixes stalled on the same failure — handing PR to human review"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                "Automated fixes stalled on the same failure (no progress) — handed off for human review.",
            )
            .await;
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::HandedOff(super::GaveUp::Stalled),
            )
            .await;
            // mark_pr_ready squashes the branch (while the PR is still a draft)
            // before un-drafting, so the placeholder→revert→refix churn collapses
            // into one clean commit here — no separate squash needed. The churn
            // only ever leaked before because a stall-blind run timed out and
            // never reached this handoff.
            mark_events_processed(state, &msg.run_id, &events).await;
            mark_pr_ready(
                state,
                &msg.team_id,
                &github,
                &repo_owner,
                &repo_name,
                pr_number,
            )
            .await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;
            return Ok(());
        }

        // Implement fix
        let repo_full = format!("{repo_owner}/{repo_name}");
        let ci_fix = super::webhooks::CiFix {
            team_id: &msg.team_id,
            run_id: &msg.run_id,
            repo: &repo_full,
            pr_number,
            branch: &branch,
            attempt: cycle + 1,
        };
        let failing_checks: Vec<String> = events
            .iter()
            .filter(|e| e.event_type == "ci_failed")
            .filter_map(|e| e.payload["workflow_name"].as_str().map(str::to_string))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        super::webhooks::emit_ci_fix_started(state, &ci_fix, &failing_checks).await;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        super::update_pass(state, &msg.team_id, &msg.run_id, "implement").await?;

        let plan_result = plan::PlanResult {
            proposal: String::new(),
            tasks: String::new(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
            fix: None,
        };
        let file_cache = FileCache::default();

        // Include review feedback if any PR comments arrived alongside CI failure
        let review_context = if review_feedback.is_empty() {
            String::new()
        } else {
            format!("\n\nPR review comments (also address if relevant):\n{review_feedback}")
        };
        let base_note = if on_base.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nThese checks also fail on {base_branch}, so this PR did not cause them. \
                 Leave them alone: {}.",
                on_base.join(", ")
            )
        };

        let feedback = format!(
            "CI workflow failed on PR #{pr_number} (branch: {branch}). Fix the failures.\n\n\
             Rules:\n\
             - Only fix what CI is complaining about. Don't refactor or add features.\n\
             - Fix ONLY failures caused by this PR's own changes. NEVER fix pre-existing \
               lint warnings/unused imports/style issues in files this PR does not touch, \
               even when CI logs mention them — they pre-date this PR; note them in your \
               summary instead.\n\
             - If the failure is in a test, fix the code (not the test) unless the test itself is wrong.\n\
             - If tests need updating because the feature changed behavior intentionally, update the tests.\n\
             - For lint/formatting failures (Prettier, ESLint): read the existing file first, match its style exactly (trailing commas, line length, quote style, indentation). Write the COMPLETE file content.\n\
             - NEVER replace a real file with a stub or placeholder to make an error go away — write the complete, working implementation. If you broke a file, use restore_file to recover it, then fix it properly.\n\
             - Before you finish, call run_checks to verify your fix builds and type-checks against real output. Do not finish on a red result if run_checks is available.\n\
             - You may create new test files or edit existing ones if needed.\n\n\
             Failure summary:\n{failure_logs}\n\n\
             Full logs:\n{logs}{review_context}{base_note}"
        );

        // The CI fix is a round of its own (common::run_round): its own
        // budget, on the run record while it works, and parked for a fresh
        // invocation when this one has too little left to finish it.
        let round_base = match branch_head.as_deref() {
            Some(h) if !h.is_empty() => h.to_string(),
            _ => super::round::branch_head(&github, &repo_owner, &repo_name, &branch).await,
        };
        let mut ci_round = run_round::Round::start(
            run_round::Kind::CiFix,
            (cycle + 1).min(u32::MAX as usize) as u32,
            &round_base,
            &feedback,
            review_feedback.is_empty(),
            chrono::Utc::now(),
        );
        let pr_ref = PrRef {
            owner: &repo_owner,
            repo: &repo_name,
            branch: &branch,
            number: pr_number,
            url: &pr_url,
        };
        if !super::round::may_start(start, std::time::Instant::now()) {
            mark_events_processed(state, &msg.run_id, &events).await;
            park_round(
                state,
                &msg,
                (&ticket_msg, &usage),
                &pr_ref,
                &ci_round,
                false,
                created_at,
                &start,
            )
            .await?;
            return Ok(());
        }
        // The fix writes its checkpoints to its own WIP branch; the pull
        // request gets one commit when it's done.
        ci_round.wip_branch =
            super::round::open_wip(&github, &repo_owner, &repo_name, &branch, "").await?;
        super::round::record(state, &msg.team_id, &msg.run_id, &ci_round).await;

        // The tree before the fix: compared with the tree after it to tell a
        // fix that changed code from one that only rewrote the same bytes.
        let tree_before = github
            .get_tree_sha(&repo_owner, &repo_name, &branch)
            .await
            .ok();

        match implement::run(
            state,
            &ticket_msg,
            &github,
            &plan_result,
            &ci_round.wip_branch,
            &rules,
            &repo_instructions,
            Some(&feedback),
            "medium",
            &provider,
            &mut usage,
            &file_cache,
            Some(&msg.run_id),
            Some(super::round::deadline(start)),
            // Scope to the PR only when this cycle is purely CI-driven; if
            // human review comments ride along, the human's ask wins.
            review_feedback.is_empty(),
        )
        .await
        {
            Ok(result) if result.timed_out => {
                // Out of its budget with work left: what it wrote is
                // committed; the rest continues in a fresh step, or the run
                // says the round didn't finish.
                write_pass_trace(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &format!("ci_fix:{}", cycle + 1),
                    pass_start,
                    &usage_before,
                    &usage,
                    None,
                )
                .await;
                mark_events_processed(state, &msg.run_id, &events).await;
                if ci_round.may_continue() {
                    park_round(
                        state,
                        &msg,
                        (&ticket_msg, &usage),
                        &pr_ref,
                        &ci_round,
                        true,
                        created_at,
                        &start,
                    )
                    .await?;
                    return Ok(());
                }
                super::round::finish_incomplete(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &super::round::Pr {
                        github: &github,
                        owner: &repo_owner,
                        repo: &repo_name,
                        branch: &branch,
                    },
                    &ci_round,
                    &run_round::out_of_time(ci_round.kind, ci_round.attempt),
                )
                .await;
                complete_run_with_status(
                    state,
                    &ticket_msg,
                    &msg.run_id,
                    "completed",
                    &pr_url,
                    pr_number,
                    &branch,
                    &usage,
                    wall_clock_secs(created_at, &start),
                )
                .await?;
                return Ok(());
            }
            Ok(mut result) => {
                // One commit, one push: the fix lands on the pull request.
                let message =
                    super::round::round_message(state, &msg.team_id, &ticket_msg, &ci_round).await;
                let gh = super::round::Pr {
                    github: &github,
                    owner: &repo_owner,
                    repo: &repo_name,
                    branch: &branch,
                };
                match super::round::land(&gh, &ci_round.wip_branch, &ci_round.base_sha, &message)
                    .await
                {
                    super::round::Landed::Pushed { .. } => {}
                    super::round::Landed::Nothing => result.files_modified.clear(),
                    super::round::Landed::Refused(why) => {
                        mark_events_processed(state, &msg.run_id, &events).await;
                        super::round::finish_incomplete(
                            state,
                            &msg.team_id,
                            &msg.run_id,
                            &gh,
                            &ci_round,
                            &format!("its changes couldn't land on the pull request: {why}"),
                        )
                        .await;
                        complete_run_with_status(
                            state,
                            &ticket_msg,
                            &msg.run_id,
                            "completed",
                            &pr_url,
                            pr_number,
                            &branch,
                            &usage,
                            wall_clock_secs(created_at, &start),
                        )
                        .await?;
                        return Ok(());
                    }
                }
                super::round::clear(state, &msg.team_id, &msg.run_id).await;
                info!(
                    run_id = msg.run_id,
                    files = result.files_modified.len(),
                    "CI fix implemented"
                );
                write_pass_trace(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &format!("ci_fix:{}", cycle + 1),
                    pass_start,
                    &usage_before,
                    &usage,
                    None,
                )
                .await;

                if result.files_modified.is_empty() {
                    warn!(
                        run_id = msg.run_id,
                        "CI fix produced no changes — completing with PR for human review"
                    );
                    super::webhooks::emit_ci_fix_completed(state, &ci_fix, "no_changes", 0).await;
                    super::record_ci_outcome(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        super::CiOutcome::HandedOff(super::GaveUp::NoChange),
                    )
                    .await;
                    // Don't hard-fail — complete the run and mark PR ready for human review
                    mark_events_processed(state, &msg.run_id, &events).await;
                    complete_run_with_status(
                        state,
                        &ticket_msg,
                        &msg.run_id,
                        "completed",
                        &pr_url,
                        pr_number,
                        &branch,
                        &usage,
                        wall_clock_secs(created_at, &start),
                    )
                    .await?;
                    mark_pr_ready(
                        state,
                        &msg.team_id,
                        &github,
                        &repo_owner,
                        &repo_name,
                        pr_number,
                    )
                    .await;
                    return Ok(());
                }
                super::webhooks::emit_ci_fix_completed(
                    state,
                    &ci_fix,
                    "pushed",
                    result.files_modified.len(),
                )
                .await;
            }
            Err(e) => {
                if requeue_after_model_error(state, &msg, &*e).await {
                    return Ok(());
                }
                error!(run_id = msg.run_id, error = %e, "CI fix implementation failed — the PR stays open, the round is named as not finished");
                super::webhooks::emit_ci_fix_completed(state, &ci_fix, "failed", 0).await;
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    super::CiOutcome::HandedOff(super::GaveUp::FixError),
                )
                .await;
                mark_events_processed(state, &msg.run_id, &events).await;
                // Not "failed", and not handed over as if done: the run says
                // the round didn't finish and what it pushed; Resume runs it
                // again.
                super::round::finish_incomplete(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &super::round::Pr {
                        github: &github,
                        owner: &repo_owner,
                        repo: &repo_name,
                        branch: &branch,
                    },
                    &ci_round,
                    &run_round::errored(ci_round.kind, &common::run_fail::error_text(&*e)),
                )
                .await;
                complete_run_with_status(
                    state,
                    &ticket_msg,
                    &msg.run_id,
                    "completed",
                    &pr_url,
                    pr_number,
                    &branch,
                    &usage,
                    wall_clock_secs(created_at, &start),
                )
                .await?;
                return Ok(());
            }
        }

        // A fix that left the tree unchanged (or returned it to an earlier
        // cycle's state) is a stall cycle in its own right; enough of them
        // hand the PR to a human now rather than after the next CI run.
        let tree_after = github
            .get_tree_sha(&repo_owner, &repo_name, &branch)
            .await
            .ok();
        if record_fix_progress(
            state,
            &msg.team_id,
            &msg.run_id,
            tree_before.as_deref(),
            tree_after.as_deref(),
        )
        .await
        {
            warn!(
                run_id = msg.run_id,
                "CI fixes are not changing the code — handing PR to human review"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                "Automated fixes stopped changing the code (same files, same bytes) — handed off for human review.",
            )
            .await;
            super::record_ci_outcome(
                state,
                &msg.team_id,
                &msg.run_id,
                super::CiOutcome::HandedOff(super::GaveUp::NoProgress),
            )
            .await;
            mark_events_processed(state, &msg.run_id, &events).await;
            mark_pr_ready(
                state,
                &msg.team_id,
                &github,
                &repo_owner,
                &repo_name,
                pr_number,
            )
            .await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;
            return Ok(());
        }

        // The fix changed code: count it before going back to wait for CI.
        super::record_ci_outcome(
            state,
            &msg.team_id,
            &msg.run_id,
            super::CiOutcome::FixCommitted,
        )
        .await;

        // Save checkpoint with incremented cycle, go back to awaiting_ci
        save_checkpoint(
            state,
            &msg.team_id,
            &msg.run_id,
            "pr",
            &branch,
            (cycle + 1) as u8,
            &usage,
        )
        .await;

        // Mark events processed BEFORE setting awaiting_ci to prevent a race
        // where another resume grabs the run and re-processes stale events
        mark_events_processed(state, &msg.run_id, &events).await;

        let still_active = set_run_awaiting_ci(
            state,
            &ticket_msg,
            &msg.run_id,
            &pr_url,
            pr_number,
            &branch,
            &usage,
            wall_clock_secs(created_at, &start),
        )
        .await?;

        if !still_active {
            return Ok(()); // run cancelled — do not reschedule
        }

        // Schedule a safety-net resume in case the CI webhook is missed
        send_delayed_resume(state, &msg, 120).await;

        info!(
            run_id = msg.run_id,
            "Fix pushed, back to awaiting_ci for next CI run"
        );
    } else if has_ci_pass {
        // A ci_passed event arrived and no ci_failed did — but that reflects only
        // the workflows reported SO FAR. Confirm against GitHub that the whole
        // check set is complete and green before running review + completing: a
        // fast workflow (Lint) finishing ahead of a slow one (Test Suite) must
        // not conclude the run while tests are still running — or after a straggler
        // has already failed. Events are wake-ups; the aggregate is the source of
        // truth (see CiVerdict).
        let (verdict, quarantined) = evaluate_ci_verdict_with_quarantine(
            state,
            &github,
            &msg.team_id,
            &repo_owner,
            &repo_name,
            &branch,
            branch_head.as_deref(),
        )
        .await;
        if quarantined.is_some() {
            quarantined_only = quarantined;
        }
        match verdict {
            CiVerdict::Pending => {
                info!(
                    run_id = msg.run_id,
                    "A workflow passed but others are still running — waiting for full CI"
                );
                // Leave events unprocessed so the next resume re-evaluates with the
                // full set (and still sees any review feedback that rode along).
                let still_active = set_run_awaiting_ci(
                    state,
                    &ticket_msg,
                    &msg.run_id,
                    &pr_url,
                    pr_number,
                    &branch,
                    &usage,
                    wall_clock_secs(created_at, &start),
                )
                .await?;
                if still_active {
                    send_delayed_resume(state, &msg, 120).await;
                }
                return Ok(());
            }
            CiVerdict::Failed => {
                // A check failed even though a ci_passed landed first. Record a
                // synthetic ci_failed and re-drive so the next resume routes into
                // the fix path (the real ci_failed webhook, when it arrives, carries
                // the workflow_run_id for full logs).
                info!(
                    run_id = msg.run_id,
                    "A workflow passed but another check failed — routing to the fix path"
                );
                record_synthetic_ci_failed(state, &msg.run_id, branch_head.as_deref()).await;
                let still_active = set_run_awaiting_ci(
                    state,
                    &ticket_msg,
                    &msg.run_id,
                    &pr_url,
                    pr_number,
                    &branch,
                    &usage,
                    wall_clock_secs(created_at, &start),
                )
                .await?;
                if still_active {
                    send_delayed_resume(state, &msg, 0).await;
                }
                return Ok(());
            }
            // Green (or NoChecks — a pass with no checks now visible) → review.
            CiVerdict::Green | CiVerdict::NoChecks => {}
        }

        // CI passed — run review (include any PR review/comment feedback)
        let review_repo_instructions = if review_feedback.is_empty() {
            repo_instructions.clone()
        } else {
            format!(
                "{repo_instructions}\n\n## Human Review Feedback\nThe following feedback was left on the PR. Address these comments:\n\n{}",
                review_feedback
            )
        };
        info!(
            run_id = msg.run_id,
            feedback_len = review_feedback.len(),
            "CI passed — running review"
        );
        if let Some(names) = &quarantined_only {
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &quarantine::passing_line(names),
            )
            .await;
        }
        super::record_ci_outcome(
            state,
            &msg.team_id,
            &msg.run_id,
            passed_outcome(&run_record, branch_head.as_deref()),
        )
        .await;
        // CI's green result settles the checks the PR opened without.
        if verdict == CiVerdict::Green {
            super::pr::settle_unverified_by_ci(&github, &repo_owner, &repo_name, pr_number).await;
        }
        super::update_pass(state, &msg.team_id, &msg.run_id, "review").await?;

        let plan_result = plan::PlanResult {
            proposal: String::new(),
            tasks: String::new(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
            fix: None,
        };
        let file_cache = FileCache::default();

        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let review_result = match review::run(
            state,
            &ticket_msg,
            &github,
            &plan_result,
            &branch,
            &rules,
            &review_repo_instructions,
            &provider,
            &mut usage,
            &file_cache,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(run_id = msg.run_id, error = %e, "Review pass errored, treating as pass");
                review::ReviewResult {
                    passed: true,
                    summary: format!("Review error: {e}"),
                }
            }
        };

        write_pass_trace(
            state,
            &msg.team_id,
            &msg.run_id,
            "review:resume",
            pass_start,
            &usage_before,
            &usage,
            None,
        )
        .await;

        // One review authority: the run's own checks passing is not enough.
        // The PR reviewer gives its verdict on this head now, and the run
        // completes only when that verdict doesn't block.
        let mut review_result = review_result;
        if review_result.passed {
            let budget =
                std::time::Duration::from_secs(super::remaining_secs(&start).saturating_sub(150));
            if let Some(rr) = super::review_pr::review_for_run(
                state,
                &msg.team_id,
                msg.installation_id,
                &repo_owner,
                &repo_name,
                pr_number,
                branch_head.as_deref().unwrap_or(""),
                budget,
            )
            .await
            {
                if let Some(finding) = rr.override_finding.as_deref() {
                    // The diff overrides data another system owns: the fix
                    // belongs there, not in another commit on this PR.
                    let planned = run_record
                        .get("fix_decision")
                        .and_then(|v| v.as_s().ok())
                        .and_then(|j| {
                            serde_json::from_str::<super::fix_location::FixDecision>(j).ok()
                        });
                    let where_ = planned
                        .as_ref()
                        .and_then(|d| d.outside_change.as_ref())
                        .map(|c| c.system.clone())
                        .unwrap_or_default();
                    let cause = planned
                        .as_ref()
                        .map(|d| d.cause.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .unwrap_or_else(|| finding.to_string());
                    info!(
                        run_id = msg.run_id,
                        "PR reviewer found the diff overrides another system's data — closing own PR"
                    );
                    let closed = super::feedback::close_own_pr_outside(
                        state,
                        &github,
                        &super::feedback::OwnPr {
                            team_id: &msg.team_id,
                            run_id: &msg.run_id,
                            owner: &repo_owner,
                            repo: &repo_name,
                            pr_number,
                        },
                        &where_,
                        &cause,
                    )
                    .await;
                    if closed {
                        mark_events_processed(state, &msg.run_id, &events).await;
                        return Ok(());
                    }
                }
                if rr.blocks() {
                    info!(
                        run_id = msg.run_id,
                        "PR reviewer requested changes — the run doesn't complete as passed"
                    );
                    review_result = review::ReviewResult {
                        passed: false,
                        summary: format!("The PR reviewer requested changes:\n\n{}", rr.body),
                    };
                }
            }
        }

        if review_result.passed {
            info!(run_id = msg.run_id, "Review passed — completing run");
            ready_or_prove(
                state,
                &github,
                &msg.team_id,
                &repo_owner,
                &repo_name,
                pr_number,
            )
            .await;
            complete_run_with_status(
                state,
                &ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;
        } else {
            // Review found issues — implement fix and go back to awaiting_ci.
            // Draws on the SAME bounded budget as CI fixes: this branch used
            // to run uncapped AND reset the shared counter to zero, so an
            // oscillating review could loop until the 24h wall clock.
            let cycle = load_cycle_from_checkpoint(state, &msg.team_id, &msg.run_id).await;
            let over_cycle = cycle >= MAX_CI_FIX_CYCLES;
            let claimed =
                !over_cycle && super::claim_auto_fix_slot(state, &msg.team_id, &msg.run_id).await;
            if over_cycle || !claimed {
                warn!(
                    run_id = msg.run_id,
                    cycle, "Fix budget exhausted — handing PR to human review as-is"
                );
                super::add_progress_note(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    "Automated fix budget exhausted — PR handed off for human review",
                )
                .await;
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    super::CiOutcome::HandedOff(super::GaveUp::Budget),
                )
                .await;
                mark_pr_ready(
                    state,
                    &msg.team_id,
                    &github,
                    &repo_owner,
                    &repo_name,
                    pr_number,
                )
                .await;
                complete_run_with_status(
                    state,
                    &ticket_msg,
                    &msg.run_id,
                    "completed",
                    &pr_url,
                    pr_number,
                    &branch,
                    &usage,
                    wall_clock_secs(created_at, &start),
                )
                .await?;
                mark_events_processed(state, &msg.run_id, &events).await;
                return Ok(());
            }
            info!(
                run_id = msg.run_id,
                "Review found issues — implementing fix"
            );
            // The fix is a round of its own: its own budget, parked for a
            // fresh invocation when the review used most of this one, and
            // never a failed run when it can't finish.
            let base_sha =
                super::round::branch_head(&github, &repo_owner, &repo_name, &branch).await;
            let fix_round = run_round::Round::start(
                run_round::Kind::ReviewFix,
                (cycle + 1).min(u32::MAX as usize) as u32,
                &base_sha,
                &review_result.summary,
                // Bot self-review fix — scoped, unless human comments were
                // folded into the review context.
                review_feedback.is_empty(),
                chrono::Utc::now(),
            );
            let mut ctx = FixContext {
                ticket_msg,
                provider,
                github,
                usage,
                rules,
                repo_instructions,
            };
            let pr = PrRef {
                owner: &repo_owner,
                repo: &repo_name,
                branch: &branch,
                number: pr_number,
                url: &pr_url,
            };
            let end =
                run_fix_round(state, &msg, &mut ctx, &pr, fix_round, created_at, &start).await?;
            mark_events_processed(state, &msg.run_id, &events).await;
            match end {
                RoundEnd::Parked => return Ok(()),
                RoundEnd::Incomplete => {
                    complete_run_with_status(
                        state,
                        &ctx.ticket_msg,
                        &msg.run_id,
                        "completed",
                        &pr_url,
                        pr_number,
                        &branch,
                        &ctx.usage,
                        wall_clock_secs(created_at, &start),
                    )
                    .await?;
                    return Ok(());
                }
                RoundEnd::Done { files: 0 } => {
                    warn!(
                        run_id = msg.run_id,
                        "Review fix produced no changes — completing anyway"
                    );
                    mark_pr_ready(
                        state,
                        &msg.team_id,
                        &ctx.github,
                        &repo_owner,
                        &repo_name,
                        pr_number,
                    )
                    .await;
                    complete_run_with_status(
                        state,
                        &ctx.ticket_msg,
                        &msg.run_id,
                        "completed",
                        &pr_url,
                        pr_number,
                        &branch,
                        &ctx.usage,
                        wall_clock_secs(created_at, &start),
                    )
                    .await?;
                    return Ok(());
                }
                RoundEnd::Done { .. } => {}
            }

            // Back to awaiting_ci for the next CI run. Increment the shared
            // fix counter — writing 0 here reset the CI-fix budget each time.
            save_checkpoint(
                state,
                &msg.team_id,
                &msg.run_id,
                "pr",
                &branch,
                (cycle + 1).min(u8::MAX as usize) as u8,
                &ctx.usage,
            )
            .await;
            let still_active = set_run_awaiting_ci(
                state,
                &ctx.ticket_msg,
                &msg.run_id,
                &pr_url,
                pr_number,
                &branch,
                &ctx.usage,
                wall_clock_secs(created_at, &start),
            )
            .await?;

            // Schedule a safety-net resume in case the CI webhook is missed
            if still_active {
                send_delayed_resume(state, &msg, 120).await;
            }
            return Ok(());
        }
    }

    // Review/comment-only events (no CI signal): the two branches above both
    // skipped, but the run was already claimed to "running" — without this,
    // it wedged there and the reviewer's feedback was silently discarded.
    // Restore awaiting_ci and keep the safety net alive; comment feedback is
    // handled by the feedback pass once the run completes.
    if !has_ci_fail && !has_ci_pass {
        info!(
            run_id = msg.run_id,
            "Resume had no CI events (review/comments only) — restoring awaiting_ci"
        );
        let still_active = set_run_awaiting_ci(
            state,
            &ticket_msg,
            &msg.run_id,
            &pr_url,
            pr_number,
            &branch,
            &usage,
            wall_clock_secs(created_at, &start),
        )
        .await?;
        if still_active {
            send_delayed_resume(state, &msg, 120).await;
        }
    }

    // Mark events processed only after all work completes successfully
    mark_events_processed(state, &msg.run_id, &events).await;

    Ok(())
}

/// What a fix round on the run's pull request works with.
struct FixContext {
    ticket_msg: TicketMessage,
    provider: ModelProvider,
    github: GitHubClient,
    usage: TokenUsage,
    rules: Vec<String>,
    repo_instructions: String,
}

/// The pull request's repository and ticket, read from the run record.
struct FixTarget<'a> {
    owner: &'a str,
    repo: &'a str,
    base_branch: &'a str,
    ticket_id: &'a str,
    title: &'a str,
    issue_number: u64,
}

/// Loads what a fix round needs: the ticket (with its design mockups), the
/// team's model provider, GitHub, the run's usage so far, rules and the
/// repository's instructions.
async fn fix_context(
    state: &WorkerState,
    msg: &ResumeMessage,
    run_record: &std::collections::HashMap<String, AttributeValue>,
    t: &FixTarget<'_>,
) -> Result<FixContext, Box<dyn std::error::Error + Send + Sync>> {
    // Rehydrate the ticket's design mockups from the run record so the review
    // pass (and review-fix implement) can check the code AGAINST the design —
    // the empty vec here meant post-CI passes never saw the mockups.
    let image_attachments: Vec<crate::models::ImageAttachment> = run_record
        .get("image_attachments")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    // Build a minimal TicketMessage for the implement/review passes
    let ticket_msg = TicketMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        source: TicketSource::Github,
        ticket_id: t.ticket_id.to_string(),
        title: t.title.to_string(),
        body: String::new(),
        repo_owner: t.owner.to_string(),
        repo_name: t.repo.to_string(),
        issue_number: t.issue_number,
        sender: String::new(),
        requester: None,
        base_branch: t.base_branch.to_string(),
        image_attachments,
        continuation: 0,
        continuation_run_id: None,
        first_attempt_ms: 0,
        model_retries: 0,
        ticket_update_note: None,
        ticket_history_note: None,
        plan_cache_key: None,
        // Loaded once here; the fix and review rounds read it from the
        // message.
        team_instructions: super::load_team_instructions(state, &msg.team_id, t.owner, t.repo)
            .await,
        person_direction: None,
        answers: Vec::new(),
        new_run_id: None,
    };

    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // This invocation's calls only: every run write adds them to what the
    // record holds. (Restoring a running total from the checkpoint and SETting
    // it wiped the run's cost once that checkpoint's 7-day TTL expired.)
    let usage = TokenUsage::default();

    // Load rules and repo instructions so CI fix / review fix have full context
    let rules = super::load_rules(state, &ticket_msg).await;
    let repo_instructions = super::load_repo_instructions(&github, t.owner, t.repo).await;
    Ok(FixContext {
        ticket_msg,
        provider,
        github,
        usage,
        rules,
        repo_instructions,
    })
}

/// The run's pull request, as a round needs it.
struct PrRef<'a> {
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
    number: u64,
    url: &'a str,
}

/// How a round's invocation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundEnd {
    /// The round's work is done; `files` changed.
    Done { files: usize },
    /// Parked: it continues in a fresh invocation with its own budget.
    Parked,
    /// It can't finish: the run is completed with the round named as not
    /// finished, and Resume can run it again.
    Incomplete,
}

/// Parks a round for a fresh invocation: the run waits (awaiting_ci) with
/// the round on its record, and a resume picks it up first thing.
#[allow(clippy::too_many_arguments)]
async fn park_round(
    state: &WorkerState,
    msg: &ResumeMessage,
    ctx: (&TicketMessage, &TokenUsage),
    pr: &PrRef<'_>,
    round: &run_round::Round,
    next_attempt: bool,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    start: &std::time::Instant,
) -> Result<RoundEnd, Box<dyn std::error::Error + Send + Sync>> {
    let parked = round.parked(next_attempt);
    super::round::record(state, &msg.team_id, &msg.run_id, &parked).await;
    let note = if next_attempt {
        format!(
            "The {} (fix {}) used its time budget with work left; it continues in a fresh step with its own budget (step {} of {}).",
            round.kind.label(),
            round.number,
            parked.attempt,
            run_round::MAX_ATTEMPTS
        )
    } else {
        format!(
            "The {} (fix {}) starts in a fresh step so it gets its whole time budget.",
            round.kind.label(),
            round.number
        )
    };
    super::add_progress_note(state, &msg.team_id, &msg.run_id, &note).await;
    let still_active = set_run_awaiting_ci(
        state,
        ctx.0,
        &msg.run_id,
        pr.url,
        pr.number,
        pr.branch,
        ctx.1,
        wall_clock_secs(created_at, start),
    )
    .await?;
    if still_active {
        send_delayed_resume(state, msg, 5).await;
    }
    Ok(RoundEnd::Parked)
}

/// Runs one fix round's implement inside its own budget. A round that would
/// start with too little of this invocation left, or that uses its budget
/// with work left, is parked for a fresh invocation (bounded by
/// `run_round::MAX_ATTEMPTS`); a round that can't finish leaves the run
/// completed with the pull request open and says what it pushed.
#[allow(clippy::too_many_arguments)]
async fn run_fix_round(
    state: &WorkerState,
    msg: &ResumeMessage,
    ctx: &mut FixContext,
    pr: &PrRef<'_>,
    round: run_round::Round,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    start: &std::time::Instant,
) -> Result<RoundEnd, Box<dyn std::error::Error + Send + Sync>> {
    if !super::round::may_start(*start, std::time::Instant::now()) {
        return park_round(
            state,
            msg,
            (&ctx.ticket_msg, &ctx.usage),
            pr,
            &round,
            false,
            created_at,
            start,
        )
        .await;
    }
    // A round resumed by a person starts from the branch as it is now.
    let mut round = round;
    if round.base_sha.is_empty() {
        round.base_sha = super::round::branch_head(&ctx.github, pr.owner, pr.repo, pr.branch).await;
    }
    // The round writes to its own WIP branch, never the pull request's: a
    // continuing round picks up the one it has.
    let continuing = !round.wip_branch.is_empty();
    round.wip_branch =
        super::round::open_wip(&ctx.github, pr.owner, pr.repo, pr.branch, &round.wip_branch)
            .await?;
    super::round::record(state, &msg.team_id, &msg.run_id, &round).await;
    super::update_pass(state, &msg.team_id, &msg.run_id, "implement").await?;
    let feedback = if round.attempt > 1 || continuing {
        format!(
            "{}\n\n## CONTINUING THIS ROUND\nAn earlier step of this round stopped before it \
             finished; everything it wrote is already committed on this branch. Read the files you \
             need first, then finish only what is left.",
            round.feedback
        )
    } else {
        round.feedback.clone()
    };
    let plan_result = plan::PlanResult {
        proposal: String::new(),
        tasks: String::new(),
        spec: String::new(),
        design: String::new(),
        repo_tasks: vec![],
        fix: None,
    };
    let pass_start = std::time::Instant::now();
    let usage_before = ctx.usage.clone();
    let result = implement::run(
        state,
        &ctx.ticket_msg,
        &ctx.github,
        &plan_result,
        &round.wip_branch,
        &ctx.rules,
        &ctx.repo_instructions,
        Some(&feedback),
        "medium",
        &ctx.provider,
        &mut ctx.usage,
        &FileCache::default(),
        Some(&msg.run_id),
        Some(super::round::deadline(*start)),
        round.scoped,
    )
    .await;
    write_pass_trace(
        state,
        &msg.team_id,
        &msg.run_id,
        &format!("{}:{}", round.kind.as_str(), round.number),
        pass_start,
        &usage_before,
        &ctx.usage,
        None,
    )
    .await;
    let gh = super::round::Pr {
        github: &ctx.github,
        owner: pr.owner,
        repo: pr.repo,
        branch: pr.branch,
    };
    match result {
        Ok(r) if r.timed_out => {
            if round.may_continue() {
                park_round(
                    state,
                    msg,
                    (&ctx.ticket_msg, &ctx.usage),
                    pr,
                    &round,
                    true,
                    created_at,
                    start,
                )
                .await
            } else {
                super::round::finish_incomplete(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &gh,
                    &round,
                    &run_round::out_of_time(round.kind, round.attempt),
                )
                .await;
                Ok(RoundEnd::Incomplete)
            }
        }
        Ok(_) => {
            // Done: the round's changes land on the pull request as one
            // commit, one push.
            let message =
                super::round::round_message(state, &msg.team_id, &ctx.ticket_msg, &round).await;
            match super::round::land(&gh, &round.wip_branch, &round.base_sha, &message).await {
                super::round::Landed::Pushed { files } => {
                    super::round::clear(state, &msg.team_id, &msg.run_id).await;
                    // The pull request changed: its description is rewritten
                    // from the whole diff now, never left on the earlier
                    // version.
                    if files > 0 {
                        let _ = super::pr::refresh_description(
                            state,
                            &ctx.github,
                            (&msg.team_id, ctx.ticket_msg.installation_id),
                            pr.owner,
                            pr.repo,
                            pr.number,
                            &ctx.ticket_msg.team_instructions,
                            &[],
                            &ctx.provider,
                            &mut ctx.usage,
                        )
                        .await;
                    }
                    Ok(RoundEnd::Done { files })
                }
                super::round::Landed::Nothing => {
                    super::round::clear(state, &msg.team_id, &msg.run_id).await;
                    Ok(RoundEnd::Done { files: 0 })
                }
                super::round::Landed::Refused(why) => {
                    super::round::finish_incomplete(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        &gh,
                        &round,
                        &format!("its changes couldn't land on the pull request: {why}"),
                    )
                    .await;
                    Ok(RoundEnd::Incomplete)
                }
            }
        }
        Err(e) => {
            let err: &(dyn std::error::Error + 'static) = &*e;
            // A transient model failure is another step of the same round
            // (bounded by its attempts), not the round failing.
            if common::model_retry::is_transient_model_error(err) && round.may_continue() {
                warn!(run_id = msg.run_id, error = %e, "Fix round hit a transient model error — continuing in a fresh step");
                return park_round(
                    state,
                    msg,
                    (&ctx.ticket_msg, &ctx.usage),
                    pr,
                    &round,
                    true,
                    created_at,
                    start,
                )
                .await;
            }
            error!(run_id = msg.run_id, error = %e, "Fix round failed");
            super::round::finish_incomplete(
                state,
                &msg.team_id,
                &msg.run_id,
                &gh,
                &round,
                &run_round::errored(round.kind, &common::run_fail::error_text(err)),
            )
            .await;
            Ok(RoundEnd::Incomplete)
        }
    }
}

/// A parked round, picked up by a fresh invocation with its whole budget:
/// it runs before anything else the resume would do.
async fn run_pending_round(
    state: &WorkerState,
    msg: &ResumeMessage,
    run_record: &std::collections::HashMap<String, AttributeValue>,
    pending: run_round::Round,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    start: &std::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let text = |k: &str| {
        run_record
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let num = |k: &str| {
        run_record
            .get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let repo_full = text("repo");
    let (owner, repo) = repo_full.split_once('/').unwrap_or(("", ""));
    let branch = text("branch");
    let pr_number = num("pr_number");
    let pr_url = text("pr_url");
    if owner.is_empty() || branch.is_empty() || pr_number == 0 {
        warn!(
            run_id = msg.run_id,
            "Parked round on a run without its pull request — dropping it"
        );
        super::round::clear(state, &msg.team_id, &msg.run_id).await;
        return Ok(());
    }
    // Claim the run, as every resume that works does.
    let claimed = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression(
            "SET #s = :new_s, status_run_id = :sri, current_pass = :cp, updated_at = :t",
        )
        .condition_expression("#s = :expected")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":expected", attr_s("awaiting_ci"))
        .expression_attribute_values(":new_s", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{}", msg.run_id)))
        .expression_attribute_values(":cp", attr_s("implement"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .is_ok();
    if !claimed {
        info!(
            run_id = msg.run_id,
            "Could not claim the run for its parked round, skipping"
        );
        return Ok(());
    }
    let base_branch = run_record
        .get("base_branch")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "main".to_string());
    let mut ctx = fix_context(
        state,
        msg,
        run_record,
        &FixTarget {
            owner,
            repo,
            base_branch: &base_branch,
            ticket_id: &text("ticket_id"),
            title: &text("title"),
            issue_number: num("issue_number"),
        },
    )
    .await?;
    let pr = PrRef {
        owner,
        repo,
        branch: &branch,
        number: pr_number,
        url: &pr_url,
    };
    info!(
        run_id = msg.run_id,
        kind = pending.kind.as_str(),
        number = pending.number,
        attempt = pending.attempt,
        "Running a parked fix round with its own budget"
    );
    let round = pending.resumed(chrono::Utc::now());
    let kind = round.kind;
    let number = round.number;
    let end = if kind == run_round::Kind::Conflict {
        run_conflict_round(state, msg, &mut ctx, &pr, &round).await
    } else {
        run_fix_round(state, msg, &mut ctx, &pr, round, created_at, start).await?
    };
    match end {
        RoundEnd::Parked => {}
        RoundEnd::Incomplete => {
            complete_run_with_status(
                state,
                &ctx.ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &ctx.usage,
                wall_clock_secs(created_at, start),
            )
            .await?;
        }
        RoundEnd::Done { files: 0 } => {
            if kind == run_round::Kind::SecurityFix {
                recheck_security(state, msg, &mut ctx, &pr).await;
            }
            if kind == run_round::Kind::CiFix {
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    super::CiOutcome::HandedOff(super::GaveUp::NoChange),
                )
                .await;
            }
            mark_pr_ready(state, &msg.team_id, &ctx.github, owner, repo, pr_number).await;
            complete_run_with_status(
                state,
                &ctx.ticket_msg,
                &msg.run_id,
                "completed",
                &pr_url,
                pr_number,
                &branch,
                &ctx.usage,
                wall_clock_secs(created_at, start),
            )
            .await?;
        }
        RoundEnd::Done { .. } => {
            if kind == run_round::Kind::CiFix {
                super::record_ci_outcome(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    super::CiOutcome::FixCommitted,
                )
                .await;
            }
            if kind == run_round::Kind::SecurityFix {
                recheck_security(state, msg, &mut ctx, &pr).await;
            }
            save_checkpoint(
                state,
                &msg.team_id,
                &msg.run_id,
                "pr",
                &branch,
                number.min(u8::MAX as u32) as u8,
                &ctx.usage,
            )
            .await;
            let still_active = set_run_awaiting_ci(
                state,
                &ctx.ticket_msg,
                &msg.run_id,
                &pr_url,
                pr_number,
                &branch,
                &ctx.usage,
                wall_clock_secs(created_at, start),
            )
            .await?;
            if still_active {
                send_delayed_resume(state, msg, 120).await;
            }
        }
    }
    Ok(())
}

/// Starts the one conflict round on CoderHelm's own pull request after
/// auto-merge found it conflicting with its base: the run (completed or
/// waiting) gets a parked conflict round and a resume to run it. False when
/// there is no such run, or it is busy or ended: a person resolves it then.
pub(crate) async fn start_conflict_round(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
) -> bool {
    let Some(run_id) =
        super::review_pr::lookup_run_by_pr(state, team_id, owner, repo, pr_number).await
    else {
        return false;
    };
    let round = run_round::Round::start(
        run_round::Kind::Conflict,
        1,
        "",
        "Merge the base branch into this pull request's branch and resolve the conflicts.",
        true,
        chrono::Utc::now(),
    )
    .parked(false);
    let claimed = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression(
            "SET #s = :ac, status_run_id = :sri, #round = :r, current_pass = :ac, updated_at = :t \
             REMOVE #inc",
        )
        .condition_expression("#s IN (:completed, :ac) AND attribute_not_exists(#round)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_values(":ac", attr_s("awaiting_ci"))
        .expression_attribute_values(":completed", attr_s("completed"))
        .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
        .expression_attribute_values(":r", round.to_attr())
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
    if let Err(e) = claimed {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not start the conflict round");
        }
        return false;
    }
    if state.config.ci_fix_queue_url.is_empty() {
        return false;
    }
    let body = serde_json::json!({
        "type": "resume",
        "team_id": team_id,
        "run_id": run_id,
        "installation_id": installation_id,
    });
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ci_fix_queue_url)
        .message_body(body.to_string())
        .delay_seconds(5)
        .send()
        .await
        .is_ok()
}

/// The conflict round: the base branch merged into the pull request's
/// branch, the conflicting files resolved, one commit. A conflict that
/// can't be resolved that way is handed to a person with one comment.
async fn run_conflict_round(
    state: &WorkerState,
    msg: &ResumeMessage,
    ctx: &mut FixContext,
    pr: &PrRef<'_>,
    round: &run_round::Round,
) -> RoundEnd {
    let _ = super::update_pass(state, &msg.team_id, &msg.run_id, "conflict_fix").await;
    let base = ctx.ticket_msg.base_branch.clone();
    match super::pr::resolve_conflicts(
        state,
        &ctx.ticket_msg,
        &ctx.github,
        pr.branch,
        &ctx.provider,
        &mut ctx.usage,
    )
    .await
    {
        Ok(resolved) => {
            super::round::clear(state, &msg.team_id, &msg.run_id).await;
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &if resolved {
                    format!(
                        "Merged `{base}` into the branch and resolved the conflicts in one commit"
                    )
                } else {
                    format!("Merged `{base}` into the branch; nothing conflicted any more")
                },
            )
            .await;
            // A new head: CI runs, the review of that commit re-arms auto-merge.
            RoundEnd::Done { files: 1 }
        }
        Err(e) => {
            let why = common::run_fail::error_text(&*e);
            let gh = super::round::Pr {
                github: &ctx.github,
                owner: pr.owner,
                repo: pr.repo,
                branch: pr.branch,
            };
            super::round::finish_incomplete(
                state,
                &msg.team_id,
                &msg.run_id,
                &gh,
                round,
                &format!("the conflict with `{base}` couldn't be resolved automatically ({why})"),
            )
            .await;
            let comment = format!(
                "This pull request conflicts with `{base}` and CoderHelm couldn't resolve it on its \
                 own ({}). Please resolve the conflict on this branch; auto-merge starts again with \
                 the review of the new commit.",
                common::truncate_str(&why, 300)
            );
            if let Err(e) = ctx
                .github
                .create_issue_comment(pr.owner, pr.repo, pr.number, &comment)
                .await
            {
                warn!(run_id = msg.run_id, error = %e, "Could not hand the conflict off on the PR");
            }
            RoundEnd::Incomplete
        }
    }
}

/// After a security fix round: the security check runs again on the pull
/// request, and its Open items say what it found. Fixed, the items go and
/// the pull request may be marked ready; still failing, or a check that
/// didn't run, the items stay (with what it found now) and it stays a draft.
async fn recheck_security(
    state: &WorkerState,
    msg: &ResumeMessage,
    ctx: &mut FixContext,
    pr: &PrRef<'_>,
) {
    let _ = super::update_pass(state, &msg.team_id, &msg.run_id, "security").await;
    let plan = plan::PlanResult {
        proposal: String::new(),
        tasks: String::new(),
        spec: String::new(),
        design: String::new(),
        repo_tasks: vec![],
        fix: None,
    };
    let verdict = super::security::Verdict::of(
        super::security::run(
            state,
            &ctx.ticket_msg,
            &ctx.github,
            &plan,
            pr.branch,
            &ctx.repo_instructions,
            &ctx.provider,
            &mut ctx.usage,
            &FileCache::default(),
            Some(&msg.run_id),
        )
        .await,
    );
    let note = match &verdict {
        super::security::Verdict::Passed => "Security check passed after the security fix",
        super::security::Verdict::Findings(_) => {
            "Security issues remain after the security fix: they stay under Open items and the pull request stays a draft"
        }
        super::security::Verdict::NotChecked(_) => {
            "The security check couldn't run after the security fix: the pull request stays a draft"
        }
    };
    super::add_progress_note(state, &msg.team_id, &msg.run_id, note).await;
    let items = super::security::open_items(&verdict, false);
    let body = match ctx
        .github
        .get_pull_request(pr.owner, pr.repo, pr.number)
        .await
    {
        Ok(p) => p["body"].as_str().unwrap_or("").to_string(),
        Err(e) => {
            warn!(run_id = msg.run_id, error = %e, "Could not read the PR to update its security items");
            return;
        }
    };
    if let Some(updated) = super::proof::with_security_items(&body, &items) {
        if let Err(e) = ctx
            .github
            .update_pr_body(pr.owner, pr.repo, pr.number, &updated)
            .await
        {
            warn!(run_id = msg.run_id, error = %e, "Could not update the PR's security items");
        }
    }
}

async fn load_unprocessed_events(
    state: &WorkerState,
    run_id: &str,
) -> Result<Vec<RunEvent>, Box<dyn std::error::Error + Send + Sync>> {
    if state.config.events_table_name.is_empty() {
        return Ok(vec![]);
    }
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.events_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .filter_expression("#p = :false")
        .expression_attribute_names("#p", "processed")
        .expression_attribute_values(":pk", attr_s(&format!("RUN#{run_id}")))
        .expression_attribute_values(":prefix", attr_s("EVENT#"))
        .expression_attribute_values(":false", AttributeValue::Bool(false))
        .send()
        .await?;

    let events = result
        .items()
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?.clone();
            let event_type = item.get("event_type")?.as_s().ok()?.clone();
            let payload_str = item
                .get("payload")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "{}".to_string());
            let payload: Value = serde_json::from_str(&payload_str).unwrap_or_default();
            Some(RunEvent {
                sk,
                event_type,
                payload,
            })
        })
        .collect();

    Ok(events)
}

async fn mark_events_processed(state: &WorkerState, run_id: &str, events: &[RunEvent]) {
    if state.config.events_table_name.is_empty() {
        return;
    }
    for event in events {
        if let Err(e) = state
            .dynamo
            .update_item()
            .table_name(&state.config.events_table_name)
            .key("pk", attr_s(&format!("RUN#{run_id}")))
            .key("sk", attr_s(&event.sk))
            .update_expression("SET #p = :true")
            .expression_attribute_names("#p", "processed")
            .expression_attribute_values(":true", AttributeValue::Bool(true))
            .send()
            .await
        {
            warn!(error = %e, "Failed to mark event processed: {}", event.sk);
        }
    }
}

fn collect_ci_failure_info(events: &[RunEvent]) -> String {
    events
        .iter()
        .filter(|e| e.event_type == "ci_failed")
        .map(|e| {
            format!(
                "Workflow '{}' failed (run_id: {})",
                e.payload["workflow_name"].as_str().unwrap_or("unknown"),
                e.payload["workflow_run_id"].as_u64().unwrap_or(0),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

type RunRecord = std::collections::HashMap<String, AttributeValue>;

/// Heads whose failed jobs this run already re-ran once.
fn rerun_heads(run_record: &RunRecord) -> Vec<String> {
    run_record
        .get("ci_rerun_heads")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}

/// A green CI on `head`: flaky when this head failed and was re-run.
fn passed_outcome(run_record: &RunRecord, head: Option<&str>) -> super::CiOutcome {
    match head {
        Some(h) if rerun_heads(run_record).iter().any(|x| x == h) => {
            super::CiOutcome::PassedAfterRerun
        }
        _ => super::CiOutcome::Passed,
    }
}

/// Workflows that passed at least once on this run's PR, any commit.
async fn ci_passed_workflows(
    state: &WorkerState,
    run_id: &str,
) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    if state.config.events_table_name.is_empty() {
        return out;
    }
    let mut start = None;
    loop {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.events_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .filter_expression("event_type = :t")
            .expression_attribute_values(":pk", attr_s(&format!("RUN#{run_id}")))
            .expression_attribute_values(":prefix", attr_s("EVENT#"))
            .expression_attribute_values(":t", attr_s("ci_passed"))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(resp) = resp else {
            return out;
        };
        for item in resp.items() {
            let payload: Value = item
                .get("payload")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            if let Some(w) = payload["workflow_name"].as_str() {
                out.insert(w.to_string());
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => return out,
        }
    }
}

/// Re-runs this head's failed workflows once instead of spending a fix
/// attempt, when the failure looks flaky (see `should_rerun`). True when a
/// re-run started: the run stays in awaiting_ci and the re-run's result
/// wakes it again.
#[allow(clippy::too_many_arguments)]
async fn try_rerun_once(
    state: &WorkerState,
    msg: &ResumeMessage,
    gh: &GitHubClient,
    run_record: &RunRecord,
    owner: &str,
    name: &str,
    head: Option<&str>,
    events: &[RunEvent],
) -> bool {
    let failed: Vec<&RunEvent> = events
        .iter()
        .filter(|e| e.event_type == "ci_failed")
        .filter(|e| {
            matches!(
                e.payload["conclusion"].as_str().unwrap_or(""),
                "failure" | "timed_out"
            )
        })
        .collect();
    let workflows: Vec<String> = failed
        .iter()
        .filter_map(|e| e.payload["workflow_name"].as_str().map(str::to_string))
        .collect();
    let passed_before = ci_passed_workflows(state, &msg.run_id).await;
    if !should_rerun(head, &rerun_heads(run_record), &workflows, &passed_before) {
        return false;
    }
    let mut started = false;
    for e in &failed {
        let Some(wf_run) = e.payload["workflow_run_id"].as_u64() else {
            continue;
        };
        match gh.rerun_failed_jobs(owner, name, wf_run).await {
            Ok(()) => started = true,
            Err(err) => {
                warn!(run_id = msg.run_id, workflow_run_id = wf_run, error = %err, "Could not re-run failed jobs")
            }
        }
    }
    if !started {
        return false;
    }
    info!(run_id = msg.run_id, workflows = ?workflows, "CI failed on a check that passed earlier on this PR — re-running once before a fix");
    super::record_ci_outcome(
        state,
        &msg.team_id,
        &msg.run_id,
        super::CiOutcome::Rerun(head.unwrap_or_default().to_string()),
    )
    .await;
    super::add_progress_note(
        state,
        &msg.team_id,
        &msg.run_id,
        &format!(
            "CI failed on {} after passing earlier on this PR — re-running the failed jobs once before changing code.",
            workflows.join(", ")
        ),
    )
    .await;
    // The failures this re-run replaces; the re-run's own result arrives as
    // new events. Comments stay for the next resume.
    let consumed: Vec<RunEvent> = failed.into_iter().cloned().collect();
    mark_events_processed(state, &msg.run_id, &consumed).await;
    send_delayed_resume(state, msg, 120).await;
    true
}

/// After a run was handed off red, a green CI on its current head is
/// recorded, so the run counts as ended green. Only looks when the run's last
/// CI result was red and a CI pass arrived.
async fn record_late_green(state: &WorkerState, msg: &ResumeMessage, run_record: &RunRecord) {
    let last = run_record.get("ci_last").and_then(|v| v.as_s().ok());
    if last.map(String::as_str) != Some("failed") {
        return;
    }
    let Ok(events) = load_unprocessed_events(state, &msg.run_id).await else {
        return;
    };
    let ci: Vec<RunEvent> = events
        .into_iter()
        .filter(|e| e.event_type == "ci_passed" || e.event_type == "ci_failed")
        .collect();
    if !ci.iter().any(|e| e.event_type == "ci_passed") {
        return;
    }
    let Some(repo) = run_record.get("repo").and_then(|v| v.as_s().ok()) else {
        return;
    };
    let Some((owner, name)) = repo.split_once('/') else {
        return;
    };
    let Some(head) = current_branch_head(state, msg, run_record).await else {
        return;
    };
    let Ok(gh) = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) else {
        return;
    };
    if evaluate_ci_verdict(&gh, owner, name, &head).await == CiVerdict::Green {
        info!(
            run_id = msg.run_id,
            "CI went green after the hand-off — recording it"
        );
        super::record_ci_outcome(
            state,
            &msg.team_id,
            &msg.run_id,
            passed_outcome(run_record, Some(&head)),
        )
        .await;
        mark_events_processed(state, &msg.run_id, &ci).await;
    }
}

fn collect_review_feedback(events: &[RunEvent]) -> String {
    events
        .iter()
        .filter(|e| e.event_type == "pr_review" || e.event_type == "pr_comment")
        .map(|e| {
            if e.event_type == "pr_review" {
                let reviewer = e.payload["reviewer"].as_str().unwrap_or("unknown");
                let state = e.payload["review_state"].as_str().unwrap_or("");
                let body = e.payload["review_body"].as_str().unwrap_or("");
                format!("@{reviewer} ({state}):\n{body}")
            } else {
                let commenter = e.payload["commenter"].as_str().unwrap_or("unknown");
                let body = e.payload["body"].as_str().unwrap_or("");
                format!("@{commenter}:\n{body}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

/// Compare the current failure against the stored one, persist the update, and
/// return true if the run has STALLED (same failure repeating → halt). The pure
/// decision (`stall_decision` / `ci_failure_signature`) is unit-tested.
async fn check_and_record_stall(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    failure_text: &str,
) -> bool {
    let current_sig = ci_failure_signature(failure_text);
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("last_error_sig, stall_count, last_fix_noop")
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let prev_sig = item
        .as_ref()
        .and_then(|i| i.get("last_error_sig"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let prev_stall = item
        .as_ref()
        .and_then(|i| i.get("stall_count"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0u32);
    let last_fix_noop = item
        .as_ref()
        .and_then(|i| i.get("last_fix_noop"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let (new_stall, halt) = stall_decision(&prev_sig, prev_stall, &current_sig, last_fix_noop);
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET last_error_sig = :s, stall_count = :c")
        .expression_attribute_values(":s", attr_s(&current_sig))
        .expression_attribute_values(":c", attr_n(new_stall as u64))
        .send()
        .await;
    halt
}

/// After a fix lands: decide whether it moved the code at all (tree before
/// vs. after, and every tree earlier cycles produced), persist that, and
/// return true if the run has STALLED. Signature checks alone miss a fix
/// that rewrites the same bytes while the failing workflows shift. The pure
/// decisions (`fix_made_no_progress` / `noop_fix_decision`) are unit-tested.
async fn record_fix_progress(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    tree_before: Option<&str>,
    tree_after: Option<&str>,
) -> bool {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("stall_count, fix_trees")
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let prev_stall = item
        .as_ref()
        .and_then(|i| i.get("stall_count"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0u32);
    let seen: Vec<String> = item
        .as_ref()
        .and_then(|i| i.get("fix_trees"))
        .and_then(|v| v.as_ss().ok())
        .cloned()
        .unwrap_or_default();
    let noop = fix_made_no_progress(tree_before, tree_after, &seen);
    let (new_stall, halt) = noop_fix_decision(prev_stall, noop);
    if noop {
        warn!(
            run_id,
            tree = tree_after.unwrap_or(""),
            stall_count = new_stall,
            "CI fix left the tree unchanged or back at an earlier state — counted as a stall"
        );
    }
    let trees: Vec<String> = [tree_before, tree_after]
        .into_iter()
        .flatten()
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect();
    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .expression_attribute_values(":c", attr_n(new_stall as u64))
        .expression_attribute_values(":n", AttributeValue::Bool(noop));
    update = if trees.is_empty() {
        update.update_expression("SET stall_count = :c, last_fix_noop = :n")
    } else {
        update
            .update_expression("SET stall_count = :c, last_fix_noop = :n ADD fix_trees :t")
            .expression_attribute_values(":t", AttributeValue::Ss(trees))
    };
    let _ = update.send().await;
    halt
}

async fn load_cycle_from_checkpoint(state: &WorkerState, team_id: &str, run_id: &str) -> usize {
    if state.config.checkpoints_table_name.is_empty() {
        return 0;
    }
    state
        .dynamo
        .get_item()
        .table_name(&state.config.checkpoints_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s(&format!("RUN#{run_id}")))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|item| {
            item.get("review_cycle")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
async fn set_run_awaiting_ci(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr_url: &str,
    pr_number: u64,
    branch: &str,
    usage: &TokenUsage,
    duration: u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let delta = usage.unrecorded();

    // Conditional on the run NOT being cancelled: a user cancel must be
    // durable — a racing CI-fix/resume invocation used to blindly overwrite
    // "cancelled" with "awaiting_ci" and resurrect the loop. Returns
    // Ok(false) when the run was cancelled so the caller stops rescheduling.
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri \
             REMOVE error_message, #err, #inc",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#err", "error")
        // A round is being worked again: its earlier "didn't finish" is over.
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .ended_values()
        .expression_attribute_values(":s", attr_s("awaiting_ci"))
        .expression_attribute_values(":pr", attr_s(pr_url))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .expression_attribute_values(":b", attr_s(branch))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("awaiting_ci"))
        .expression_attribute_values(":sri", attr_s(&format!("awaiting_ci#{run_id}")))
        .send()
        .await;
    match res {
        Ok(_) => {
            usage.mark_recorded(&delta);
            Ok(true)
        }
        // Display for a service error is only "service error" — classify on
        // the typed error, or a cancelled run surfaced as a hard failure.
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            info!(
                run_id,
                "Run is cancelled — not restoring awaiting_ci; stopping loop"
            );
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn mark_pr_ready(
    state: &WorkerState,
    team_id: &str,
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    pr_number: u64,
) {
    // Asking people to merge: the PR's redirects and links are checked
    // again and the description's table updated; one that fails now keeps
    // the PR a draft, with the reason in that table.
    if let Some(r) = super::link_checks::refresh_pr(
        state, github, team_id, repo_owner, repo_name, pr_number, "", true,
    )
    .await
    .filter(|r| !r.blocking.is_empty())
    {
        warn!(pr_number, blocking = ?r.blocking, "Link and redirect checks fail now; the PR stays a draft");
        return;
    }
    // A failing-test fix stays a draft until the checks that test it are
    // green on its head and a repeat run passed.
    if let super::test_fix::Hold::Wait(_) =
        super::test_fix::ready_gate(state, github, team_id, repo_owner, repo_name, pr_number).await
    {
        return;
    }
    // A new feature stays a draft until its verification checklist passes.
    if super::feature::ready_hold(state, github, team_id, repo_owner, repo_name, pr_number)
        .await
        .is_some()
    {
        return;
    }
    // With a team commit template, the final squash keeps the message the
    // run's first commit was given from it; otherwise the PR title, as always.
    let keep_first = super::load_git_conventions(state, team_id, "")
        .await
        .commit
        .is_some();
    mark_ready(github, repo_owner, repo_name, pr_number, false, keep_first).await
}

/// Marks an E2E fix or generated-test PR ready once its repeat runs proved it.
/// Those PRs are skipped by [`mark_pr_ready`]: green CI alone isn't enough.
/// Marks a finished run's PR ready for review. A flaky-test fix PR starts its
/// proof first, and stays a draft while the proof runs or after the fix was
/// refused; when no proof can run it is marked ready with a note saying so.
pub(crate) async fn ready_or_prove(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
    pr_number: u64,
) {
    use super::e2e::Proof;
    match super::e2e::start_fix_proof(state, github, team_id, repo_owner, repo_name, pr_number)
        .await
    {
        Proof::Ready => mark_e2e_pr_ready(github, repo_owner, repo_name, pr_number).await,
        Proof::Running | Proof::Refused => info!(
            pr_number,
            "Flaky-test fix PR stays a draft while its proof runs or after it was refused"
        ),
        Proof::NotAFix => {
            mark_pr_ready(state, team_id, github, repo_owner, repo_name, pr_number).await
        }
    }
}

pub(crate) async fn mark_e2e_pr_ready(
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    pr_number: u64,
) {
    mark_ready(github, repo_owner, repo_name, pr_number, true, false).await
}

/// Whether a PR title belongs to an E2E fix or generated test, which waits for
/// its repeat runs instead of green CI.
pub(crate) fn is_e2e_proof_pr(title: &str) -> bool {
    let t = title.trim_start_matches("⚠️ Partial: ");
    t.starts_with(super::e2e::FIX_TICKET_PREFIX) || t.starts_with(super::e2e::GEN_TICKET_PREFIX)
}

/// The message of the PR's first commit CoderHelm made (the squash at PR
/// open, written from the team's commit template).
fn first_own_commit_message(commits: &[serde_json::Value]) -> Option<String> {
    commits.iter().find_map(|c| {
        let own = [&c["committer"]["login"], &c["author"]["login"]]
            .iter()
            .any(|l| super::is_own_login(l.as_str().unwrap_or("")));
        let message = c["commit"]["message"].as_str().unwrap_or("").trim();
        (own && !message.is_empty()).then(|| message.to_string())
    })
}

async fn mark_ready(
    github: &GitHubClient,
    repo_owner: &str,
    repo_name: &str,
    pr_number: u64,
    proven: bool,
    // The team has a commit template: squash under the first commit's
    // message (CoderHelm wrote it from the template) instead of the title.
    keep_first_message: bool,
) {
    if pr_number == 0 {
        return;
    }
    // Retry once on failure — transient GitHub API issues are common
    for attempt in 1..=2 {
        match github
            .get_pull_request(repo_owner, repo_name, pr_number)
            .await
        {
            Ok(pr) => {
                // If the PR was closed (e.g. a human closed it), never
                // reopen/mark it ready — respect the close.
                if pr.get("state").and_then(|v| v.as_str()) == Some("closed") {
                    info!(pr_number, "PR is closed — not marking ready");
                    return;
                }
                // Security issues not fixed yet (or a security check that
                // didn't run) are listed under Open items: the PR stays a
                // draft until they're gone.
                if super::proof::holds_security_items(
                    pr.get("body").and_then(|v| v.as_str()).unwrap_or(""),
                ) {
                    info!(
                        pr_number,
                        "Security items open in the description — the PR stays a draft"
                    );
                    return;
                }
                // Planned work not done is listed at the top: the PR stays
                // a draft until a person (or a later round) settles it.
                if super::proof::holds_unfinished(
                    pr.get("body").and_then(|v| v.as_str()).unwrap_or(""),
                ) {
                    info!(
                        pr_number,
                        "Planned work not done is listed in the description; the PR stays a draft"
                    );
                    return;
                }
                // E2E fix and generated-test PRs wait for their repeat runs.
                if !proven
                    && pr
                        .get("title")
                        .and_then(|v| v.as_str())
                        .is_some_and(is_e2e_proof_pr)
                {
                    info!(
                        pr_number,
                        "E2E proof PR — stays a draft until its repeat runs pass"
                    );
                    return;
                }
                // A person put this PR back into draft: that is their call, and
                // a later CI pass or run completion must not undo it. Only a
                // draft CoderHelm opened (or marked ready itself) is its to mark
                // ready. When the timeline can't be read, the draft is left alone
                // this time (fail closed: a draft stays a draft).
                if pr.get("draft").and_then(|v| v.as_bool()).unwrap_or(false) {
                    match github
                        .list_issue_timeline(repo_owner, repo_name, pr_number)
                        .await
                    {
                        Ok(timeline) => {
                            if let Some(who) =
                                crate::clients::github::human_converted_to_draft(&timeline)
                            {
                                info!(pr_number, converted_by = %who, "PR was put back into draft by a person — not marking ready");
                                return;
                            }
                        }
                        Err(e) => {
                            warn!(pr_number, error = %e, "Could not read the PR timeline — leaving the draft as is");
                            return;
                        }
                    }
                }
                // A PR reaching ready (CI + review passed) is no longer
                // "partial" — drop the salvage marker from the title.
                if let Some(title) = pr.get("title").and_then(|v| v.as_str()) {
                    if let Some(clean) = title.strip_prefix("⚠️ Partial: ") {
                        let _ = github
                            .update_pr_title(repo_owner, repo_name, pr_number, clean)
                            .await;
                    }
                }
                let is_draft = pr.get("draft").and_then(|v| v.as_bool()).unwrap_or(false);
                if !is_draft {
                    info!(pr_number, "PR already not a draft — skipping mark_pr_ready");
                    return;
                }
                // Final squash: collapse the run's whole history — the initial
                // implementation AND every CI-fix cycle (including any
                // restore/refix churn) — into ONE clean commit before the PR
                // goes up for review. Runs once, here at completion: the loop is
                // done (single writer), and only while still a draft, so a human
                // who has taken the PR over isn't clobbered. Best-effort — a
                // failure just leaves the noisier history.
                if let (Some(head), Some(base)) = (
                    pr.get("head")
                        .and_then(|h| h.get("ref"))
                        .and_then(|v| v.as_str()),
                    pr.get("base")
                        .and_then(|b| b.get("ref"))
                        .and_then(|v| v.as_str()),
                ) {
                    let title = pr
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Apply changes");
                    let mut squash_msg = title
                        .strip_prefix("⚠️ Partial: ")
                        .unwrap_or(title)
                        .to_string();
                    if keep_first_message {
                        if let Ok(commits) = github
                            .list_pr_commits(repo_owner, repo_name, pr_number)
                            .await
                        {
                            if let Some(m) = first_own_commit_message(&commits) {
                                squash_msg = m;
                            }
                        }
                    }
                    // A commit message an agent wrote may carry a stand-in
                    // key (`ABC-0`); the PR title carries the real one.
                    let squash_msg = common::git_conventions::normalize_message(
                        &squash_msg,
                        common::git_conventions::subject_key(title).as_deref(),
                    );
                    if let Err(e) = github
                        .squash_branch(repo_owner, repo_name, head, base, &squash_msg)
                        .await
                    {
                        warn!(pr_number, error = %e, "Final squash before mark-ready failed — proceeding with full history");
                    }
                }
                if let Some(node_id) = pr.get("node_id").and_then(|v| v.as_str()) {
                    match github.mark_pr_ready(node_id).await {
                        Ok(_) => {
                            info!(pr_number, "PR marked ready for review");
                            return;
                        }
                        Err(e) => {
                            warn!(pr_number, attempt, error = %e, "Failed to mark PR ready");
                        }
                    }
                } else {
                    warn!(pr_number, attempt, "PR response missing node_id");
                }
            }
            Err(e) => warn!(pr_number, attempt, error = %e, "Failed to fetch PR for marking ready"),
        }
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
    error!(pr_number, "Failed to mark PR ready after 2 attempts");
}

#[allow(clippy::too_many_arguments)]
async fn complete_run_with_status(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    status: &str,
    pr_url: &str,
    pr_number: u64,
    branch: &str,
    usage: &TokenUsage,
    duration: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    let delta = usage.unrecorded();

    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, pr_url = :pr, pr_number = :pn, branch = :b, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp, \
             status_run_id = :sri \
             REMOVE error_message, #err",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":pr", attr_s(pr_url))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .expression_attribute_values(":b", attr_s(branch))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(duration))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        // Cancel is terminal: never overwrite a cancelled run with a completion.
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .send()
        .await;
    match res {
        Ok(_) => {
            usage.mark_recorded(&delta);
            super::webhooks::emit_run(state, &msg.team_id, run_id, status).await
        }
        Err(e) if format!("{e:?}").contains("ConditionalCheckFailed") => {
            info!(run_id, "Run is cancelled — not overwriting with completion");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    }

    // The team's counters already have these tokens: each model call is
    // counted as it happens (common::usage).
    info!(run_id, status, "Run completed");
    Ok(())
}

/// A fix that failed on a transient model error is sent again later (the
/// one requeue policy, [`common::model_retry::requeue_delay`]) instead of
/// completing the run with the fix unapplied. False when the error isn't
/// transient or the policy is spent.
async fn requeue_after_model_error(
    state: &WorkerState,
    msg: &ResumeMessage,
    e: &(dyn std::error::Error + 'static),
) -> bool {
    if !common::model_retry::is_transient_model_error(e) {
        return false;
    }
    let Some(delay) = common::model_retry::requeue_delay(msg.model_retries) else {
        return false;
    };
    let mut next = msg.clone();
    next.model_retries = msg.model_retries + 1;
    warn!(run_id = %msg.run_id, retry = next.model_retries, delay, "model_error_requeued: fix sent again after a transient model error");
    send_delayed_resume(state, &next, delay as i32).await;
    true
}

/// Send a delayed resume message to the CI fix queue.
/// `delay_seconds` can be 0 (immediate) or up to 900 (15 min, SQS max).
/// CI for a run that opened pull requests in several repositories. Each
/// pull request's own head is checked: all green, every one is marked ready
/// and the run completes; any failure, the run is handed to a person (this
/// loop fixes CI in one repository, not across several); otherwise it checks
/// again later, within [`MAX_RESUME_POLLS`].
async fn resume_multi_repo(
    state: &WorkerState,
    msg: &ResumeMessage,
    pr_urls: &[String],
    created_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let mut open = Vec::new();
    let mut failed = Vec::new();
    let mut waiting = Vec::new();
    for url in pr_urls {
        let Some((owner, name, number)) = common::pr_outcome::parse_pr_url(url) else {
            continue;
        };
        let pr = match github.get_pull_request(&owner, &name, number).await {
            Ok(p) => p,
            Err(e) => {
                warn!(run_id = %msg.run_id, pr = %url, error = %e, "Could not read a pull request — checking again later");
                waiting.push(url.clone());
                continue;
            }
        };
        if pr["state"].as_str() == Some("closed") {
            continue;
        }
        let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        match evaluate_ci_verdict(&github, &owner, &name, &head).await {
            CiVerdict::Failed => failed.push(url.clone()),
            CiVerdict::Pending => waiting.push(url.clone()),
            // CI's green result settles the checks the PR opened without.
            CiVerdict::Green => {
                super::pr::settle_unverified_by_ci(&github, &owner, &name, number).await
            }
            CiVerdict::NoChecks => {}
        }
        open.push((owner, name, number));
    }
    let events = load_unprocessed_events(state, &msg.run_id)
        .await
        .unwrap_or_default();
    let n_open = open.len();
    let finish = |note: Option<String>| async move {
        if let Some(n) = note {
            super::add_progress_note(state, &msg.team_id, &msg.run_id, &n).await;
        }
        for (o, n, num) in &open {
            mark_pr_ready(state, &msg.team_id, &github, o, n, *num).await;
        }
        mark_events_processed(state, &msg.run_id, &events).await;
        set_run_complete(state, &msg.team_id, &msg.run_id, created_at).await;
    };
    if !failed.is_empty() {
        info!(run_id = %msg.run_id, failed = ?failed, "Multi-repo run: CI failed — handing off");
        finish(Some(format!(
            "CI failed on {}. Handed off for a person: CoderHelm fixes CI within one repository, not across several in one run.",
            failed.join(", ")
        )))
        .await;
        return Ok(());
    }
    if waiting.is_empty() {
        info!(run_id = %msg.run_id, prs = n_open, "Multi-repo run: CI green on every pull request — completing");
        finish(None).await;
        return Ok(());
    }
    info!(run_id = %msg.run_id, waiting = ?waiting, "Multi-repo run: CI still running — checking again later");
    send_delayed_resume(state, msg, 120).await;
    Ok(())
}

/// Counts one more re-arm of the CI wait loop and says whether it may go on.
async fn may_poll_again(state: &WorkerState, msg: &ResumeMessage) -> bool {
    let polls = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .update_expression("ADD ci_resume_polls :one")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await
        .ok()
        .and_then(|o| o.attributes)
        .and_then(|a| {
            a.get("ci_resume_polls")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<i64>().ok())
        })
        .unwrap_or(0);
    polls <= MAX_RESUME_POLLS
}

/// Hands a run whose CI never concluded to a person: its pull requests are
/// marked ready and the run completes with a note saying why.
async fn hand_off_after_ci_wait(state: &WorkerState, msg: &ResumeMessage) {
    let rec = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .unwrap_or_default();
    let mut urls: Vec<String> = rec
        .get("pr_urls")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    if let Some(u) = rec.get("pr_url").and_then(|v| v.as_s().ok()) {
        if !u.is_empty() && !urls.contains(u) {
            urls.push(u.clone());
        }
    }
    if let Ok(github) = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        for u in &urls {
            if let Some((o, n, num)) = common::pr_outcome::parse_pr_url(u) {
                mark_pr_ready(state, &msg.team_id, &github, &o, &n, num).await;
            }
        }
    }
    super::add_progress_note(
        state,
        &msg.team_id,
        &msg.run_id,
        "CI didn't conclude after about a day of checks — handed off for a person",
    )
    .await;
    let created_at = rec
        .get("created_at")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc));
    set_run_complete(state, &msg.team_id, &msg.run_id, created_at).await;
}

async fn send_delayed_resume(state: &WorkerState, msg: &ResumeMessage, delay_seconds: i32) {
    if state.config.ci_fix_queue_url.is_empty() {
        warn!("CI_FIX_QUEUE_URL not configured — cannot send delayed resume");
        return;
    }
    if !may_poll_again(state, msg).await {
        warn!(run_id = %msg.run_id, "CI wait loop reached its cap — handing off instead of checking again");
        hand_off_after_ci_wait(state, msg).await;
        return;
    }
    // Final backstop: never re-arm the resume loop for a run a person ended
    // (cancelled or closed).
    let cancelled = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .consistent_read(true)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&msg.run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|i| i.get("status").and_then(|v| v.as_s().ok()).cloned())
        .map(|st| common::run_status::ended_by_person(&st))
        .unwrap_or(false);
    if cancelled {
        info!(
            run_id = msg.run_id,
            "Run cancelled or closed — not scheduling delayed resume"
        );
        return;
    }
    let body = serde_json::json!({
        "type": "resume",
        "team_id": msg.team_id,
        "run_id": msg.run_id,
        "installation_id": msg.installation_id,
    });
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ci_fix_queue_url)
        .message_body(body.to_string())
        .delay_seconds(delay_seconds)
        .send()
        .await
    {
        Ok(_) => info!(
            run_id = msg.run_id,
            delay_seconds, "Delayed resume scheduled"
        ),
        Err(e) => error!(run_id = msg.run_id, error = %e, "Failed to send delayed resume"),
    }
}

/// Mark a run as completed (success) without needing full token usage context.
async fn set_run_complete(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
) {
    let now = chrono::Utc::now();
    let duration = created_at
        .map(|t| now.signed_duration_since(t).num_seconds().max(0) as u64)
        .unwrap_or(0);
    // A requirement the ticket needs that no connection let CoderHelm do
    // leaves the run open: it ends as needing that outside change, never as
    // completed, and the note says what is not done and the access to grant.
    let left = super::outside_exec::left_undone(state, team_id, run_id).await;
    let status = end_status(&left);
    if !left.is_empty() {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!(
                "Not done: {} requirement(s) still need access CoderHelm doesn't have; the pull request and the ticket list them at the top, with the access to grant",
                left.len()
            ),
        )
        .await;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, updated_at = :t, current_pass = :cp, status_run_id = :sri, duration_s = :d \
             REMOVE error_message, #err",
        )
        .expression_attribute_names("#err", "error")
        .expression_attribute_names("#s", "status")
        // "completed" — the single terminal-success status. This path used
        // to write "success", which every status filter in the gateway and
        // dashboard missed (dedup, comment routing, retry eligibility).
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(":d", attr_n(duration))
        .send()
        .await;
    super::webhooks::emit_run(state, team_id, run_id, status).await;
    info!(
        run_id,
        duration, status, "Run marked complete (no CI / CI passed)"
    );
}

/// How a run whose CI passed ends: completed, or (with requirements left
/// that need access CoderHelm lacks) needing that outside change.
fn end_status(left: &[String]) -> &'static str {
    if left.is_empty() {
        "completed"
    } else {
        super::fix_location::STATUS_OUTSIDE
    }
}

#[cfg(test)]
mod end_status_tests {
    use super::end_status;

    #[test]
    fn a_requirement_left_for_a_person_keeps_the_run_from_completing() {
        assert_eq!(end_status(&[]), "completed");
        assert_eq!(
            end_status(&["1. Replace the hero on production".to_string()]),
            crate::passes::fix_location::STATUS_OUTSIDE
        );
    }
}
