//! Analytics: is Coderhelm worth it, and where does it struggle?
//!
//! Everything here is computed on request from the records the product already
//! keeps (runs, review stats, data-source lookups, pass traces) for one period
//! and the period before it, so every number traces back to rows a person can
//! open. Nothing is estimated except where a number is labelled an estimate on
//! the page, with its formula.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Datelike, Duration, SecondsFormat, TimeZone, Utc};
use common::requester::{Known, Requester};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Most pages read from one table for one request; a guard, not a limit
/// anyone should reach (a page is up to 1 MB).
const MAX_PAGES: usize = 40;

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn n(item: &Item, k: &str) -> f64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}
fn opt_n(item: &Item, k: &str) -> Option<f64> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
}
fn b(item: &Item, k: &str) -> Option<bool> {
    item.get(k).and_then(|v| v.as_bool().ok()).copied()
}
fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn list_s(item: &Item, k: &str) -> Vec<String> {
    item.get(k)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}
/// The last pass a run's `pass_history` names.
fn last_history_pass(item: &Item) -> String {
    item.get("pass_history")
        .and_then(|v| v.as_l().ok())
        .and_then(|l| {
            l.iter().rev().find_map(|e| {
                e.as_m()
                    .ok()
                    .and_then(|m| m.get("pass"))
                    .and_then(|p| p.as_s().ok())
                    .filter(|p| !p.is_empty())
                    .cloned()
            })
        })
        .unwrap_or_default()
}

// ── Rows ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunRow {
    pub run_id: String,
    pub ticket_id: String,
    pub created_at: String,
    pub status: String,
    pub repo: String,
    pub source: String,
    pub sender: String,
    pub requester_key: String,
    pub requester_name: String,
    pub requester_email: String,
    pub title: String,
    pub cost_usd: f64,
    pub tokens_in: f64,
    pub tokens_out: f64,
    pub cache_read_tokens: f64,
    pub cache_write_tokens: f64,
    pub duration_s: f64,
    pub pr_number: u64,
    pub pr_url: String,
    pub error_message: String,
    pub ci_fix_attempts: u64,
    pub ci_fix_commits: u64,
    pub ci_last: String,
    pub pr_outcome: String,
    pub merged_at: String,
    pub pr_opened_at: String,
    pub human_commits: Option<u64>,
    /// The reason older failure writes saved under `error`.
    pub error_legacy: String,
    /// The pass a failed run was in (`common::run_fail`).
    pub failed_pass: String,
    pub current_pass: String,
    /// The last pass in `pass_history`.
    pub last_pass: String,
    /// Why Coderhelm left a red PR to a person (worker `GaveUp`).
    pub ci_end: String,
    /// Failing checks that also failed on the base branch.
    pub ci_red_on_base: Vec<String>,
    /// Times the failed jobs of a head were re-run instead of fixed.
    pub ci_reruns: u64,
    /// CI went green after such a re-run.
    pub ci_flaky: bool,
}

impl RunRow {
    /// All four parts: new input, cache read, cache write, output.
    fn tokens(&self) -> f64 {
        self.tokens_in + self.cache_read_tokens + self.cache_write_tokens + self.tokens_out
    }

    fn from_item(i: &Item) -> RunRow {
        RunRow {
            run_id: s(i, "run_id"),
            ticket_id: s(i, "ticket_id"),
            created_at: s(i, "created_at"),
            status: s(i, "status"),
            repo: s(i, "repo"),
            source: s(i, "ticket_source"),
            sender: s(i, "sender"),
            requester_key: s(i, "requester_key"),
            requester_name: s(i, "requester_name"),
            requester_email: s(i, "requester_email"),
            title: s(i, "title"),
            cost_usd: n(i, "cost_usd"),
            tokens_in: n(i, "tokens_in"),
            tokens_out: n(i, "tokens_out"),
            cache_read_tokens: n(i, "cache_read_tokens"),
            cache_write_tokens: n(i, "cache_write_tokens"),
            duration_s: n(i, "duration_s"),
            pr_number: n(i, "pr_number") as u64,
            pr_url: s(i, "pr_url"),
            error_message: s(i, "error_message"),
            ci_fix_attempts: n(i, "ci_fix_attempts") as u64,
            ci_fix_commits: n(i, "ci_fix_commits") as u64,
            ci_last: s(i, "ci_last"),
            pr_outcome: s(i, "pr_outcome"),
            merged_at: s(i, "merged_at"),
            pr_opened_at: s(i, "pr_opened_at"),
            human_commits: opt_n(i, "human_commits").map(|v| v as u64),
            error_legacy: s(i, "error"),
            failed_pass: s(i, "failed_pass"),
            current_pass: s(i, "current_pass"),
            last_pass: last_history_pass(i),
            ci_end: s(i, "ci_end"),
            ci_red_on_base: list_s(i, "ci_red_on_base"),
            ci_reruns: n(i, "ci_reruns") as u64,
            ci_flaky: b(i, "ci_flaky").unwrap_or(false),
        }
    }
    fn merged(&self) -> bool {
        self.status == "merged" || self.pr_outcome == "merged"
    }
    /// Who asked for the run; older runs only have the free-text sender.
    fn requester(&self) -> Option<Requester> {
        Requester::from_run(
            &self.requester_key,
            &self.requester_name,
            &self.requester_email,
            &self.sender,
        )
    }
    fn has_pr(&self) -> bool {
        self.pr_number > 0
    }
    /// The pull request a run opened or updated: reruns share it.
    fn pr_key(&self) -> (String, u64) {
        (self.repo.to_lowercase(), self.pr_number)
    }
    /// The ticket a run worked on; a run that recorded none is its own.
    fn ticket_key(&self) -> String {
        if self.ticket_id.is_empty() {
            format!("run:{}", self.run_id)
        } else {
            self.ticket_id.clone()
        }
    }
    /// Hours from the pull request opening to its merge, when both are known.
    fn hours_to_merge(&self) -> Option<f64> {
        let opened = DateTime::parse_from_rfc3339(&self.pr_opened_at).ok()?;
        let merged = DateTime::parse_from_rfc3339(&self.merged_at).ok()?;
        let h = (merged - opened).num_seconds() as f64 / 3600.0;
        (h >= 0.0).then_some(h)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReviewRow {
    pub created_at: String,
    pub repo: String,
    pub pr: u64,
    pub verdict: String,
    pub posted_as: String,
    pub findings: u64,
    pub prior_fixed: Option<u64>,
    pub prior_settled: Option<u64>,
    pub posted: bool,
    pub thumbs_up: u64,
    pub thumbs_down: u64,
}

impl ReviewRow {
    fn from_item(i: &Item) -> ReviewRow {
        ReviewRow {
            created_at: s(i, "created_at"),
            repo: s(i, "repo"),
            pr: n(i, "pr_number") as u64,
            verdict: s(i, "verdict"),
            posted_as: s(i, "posted_as"),
            findings: n(i, "findings") as u64,
            prior_fixed: opt_n(i, "prior_fixed").map(|v| v as u64),
            prior_settled: opt_n(i, "prior_settled_by_person").map(|v| v as u64),
            posted: b(i, "posted").unwrap_or(true),
            thumbs_up: n(i, "thumbs_up") as u64,
            thumbs_down: n(i, "thumbs_down") as u64,
        }
    }
    /// A review someone saw on the PR: not an answer to a question, and not
    /// a re-review that found nothing new and so posted nothing.
    fn is_posted_review(&self) -> bool {
        self.posted && self.verdict != "QUESTION"
    }
    /// What GitHub shows: the verdict can be an approval posted as a plain
    /// comment. Records from before `posted_as` was kept fall back to it.
    fn shown_as(&self) -> &str {
        if self.posted_as.is_empty() {
            &self.verdict
        } else {
            &self.posted_as
        }
    }
    fn pr_key(&self) -> (String, u64) {
        (self.repo.to_lowercase(), self.pr)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LookupRow {
    pub at: String,
    pub source_name: String,
    pub actor: String,
    pub outcome: String,
    pub repo: String,
}

impl LookupRow {
    fn from_item(i: &Item) -> LookupRow {
        LookupRow {
            at: s(i, "at"),
            source_name: s(i, "source_name"),
            actor: s(i, "actor"),
            outcome: s(i, "outcome"),
            repo: s(i, "repo"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TraceRow {
    /// From the key, `RUN#<run_id>#PASS#<pass>`.
    pub run_id: String,
    pub pass: String,
    pub timestamp: String,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub duration_ms: f64,
}

impl TraceRow {
    fn from_item(i: &Item) -> TraceRow {
        let sk = s(i, "sk");
        TraceRow {
            run_id: sk
                .strip_prefix("RUN#")
                .and_then(|r| r.split("#PASS#").next())
                .unwrap_or_default()
                .to_string(),
            pass: s(i, "pass"),
            timestamp: s(i, "timestamp"),
            input_tokens: n(i, "input_tokens"),
            output_tokens: n(i, "output_tokens"),
            duration_ms: n(i, "duration_ms"),
        }
    }
}

// ── Periods ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Window {
    pub fn last_days(end: DateTime<Utc>, days: i64) -> (Window, Window) {
        let start = end - Duration::days(days);
        let prev = Window {
            start: start - Duration::days(days),
            end: start,
        };
        (Window { start, end }, prev)
    }
    /// This month so far, the same stretch of last month, and all of last
    /// month. "The same stretch" is the same time elapsed since the first of
    /// the month (October 1 to 22, 15:00 against September 1 to 22, 15:00),
    /// cut at the end of last month when it is shorter (March 31 against
    /// all of February).
    pub fn month_to_date(now: DateTime<Utc>) -> (Window, Window, Window) {
        let start = Utc
            .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
            .single()
            .unwrap_or(now);
        let last = start - Duration::days(1);
        let prev_start = Utc
            .with_ymd_and_hms(last.year(), last.month(), 1, 0, 0, 0)
            .single()
            .unwrap_or(last);
        let prev_end = (prev_start + (now - start)).min(start);
        (
            Window { start, end: now },
            Window {
                start: prev_start,
                end: prev_end,
            },
            Window {
                start: prev_start,
                end: start,
            },
        )
    }
    fn contains(&self, ts: &str) -> bool {
        self.at(ts).is_some()
    }
    /// Whole days since the window started, when `ts` falls inside it.
    fn at(&self, ts: &str) -> Option<i64> {
        let t = DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&Utc);
        (t >= self.start && t < self.end).then(|| (t - self.start).num_days())
    }
    /// How many day slots the window spans (a part day counts).
    fn day_slots(&self) -> i64 {
        let secs = (self.end - self.start).num_seconds().max(0);
        (secs + 86_399) / 86_400
    }
}

// ── Aggregation (pure) ─────────────────────────────────────────────────────

fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let m = v.len() / 2;
    Some(if v.len().is_multiple_of(2) {
        (v[m - 1] + v[m]) / 2.0
    } else {
        v[m]
    })
}

fn ratio(num: f64, den: f64) -> Option<f64> {
    (den > 0.0).then(|| num / den)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// A failure message with ids, numbers and quoted values taken out, so the
/// same kind of failure groups together.
pub fn failure_reason(msg: &str) -> String {
    let msg = msg.trim();
    if msg.is_empty() {
        return "No reason recorded".to_string();
    }
    let first = msg.lines().next().unwrap_or(msg);
    let mut out = String::new();
    let mut in_quote: Option<char> = None;
    let mut last_hash = false;
    for c in first.chars() {
        if let Some(q) = in_quote {
            if c == q {
                in_quote = None;
            }
            continue;
        }
        if c == '"' || c == '`' {
            in_quote = Some(c);
            out.push('…');
            last_hash = false;
            continue;
        }
        if c.is_ascii_digit() {
            if !last_hash {
                out.push('#');
                last_hash = true;
            }
            continue;
        }
        last_hash = false;
        out.push(c);
    }
    let out: String = out.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&out, 120).to_string()
}

/// Values of `current_pass` that name a state, not a pass.
const NOT_A_PASS: [&str; 5] = ["cancelled", "closed", "done", "awaiting_ci", "unknown"];

/// Why a failed run failed, for grouping. Runs since
/// `common::run_fail::REASONS_RECORDED_SINCE` always record a reason and a
/// pass; for older ones the reason is derived from what they did record: the
/// field older writes used (`error`), the pass they reached (history, then
/// traces), a cancel a late write overwrote.
pub fn run_failure_reason(r: &RunRow, trace_pass: Option<&str>) -> String {
    let pass = [
        r.failed_pass.as_str(),
        r.last_pass.as_str(),
        trace_pass.unwrap_or(""),
    ]
    .into_iter()
    .map(|p| p.split(':').next().unwrap_or(p).trim())
    .find(|p| {
        !p.is_empty() && !NOT_A_PASS.contains(p) && *p != common::run_fail::BEFORE_FIRST_PASS
    });
    let msg = r.error_message.trim();
    if !msg.is_empty() {
        // The stuck-run sweep's message from before it looked up the pass.
        if msg.contains("during unknown pass") {
            return match pass {
                Some(p) => failure_reason(&msg.replace("unknown pass", &format!("{p} pass"))),
                None => "Run stopped before its first pass started (auto-cleanup)".to_string(),
            };
        }
        return failure_reason(msg);
    }
    // Cancelled (its pass says so), then a late write set "failed" over it.
    // A close leaves the pass alone, and no write sets failed over a close.
    if r.current_pass == common::run_status::CANCELLED {
        return "Cancelled, then marked failed by a late write".to_string();
    }
    if !r.error_legacy.trim().is_empty() {
        let base = failure_reason(&r.error_legacy);
        return match pass {
            Some(p) => format!("{base} ({p} pass)"),
            None => base,
        };
    }
    let since = common::run_fail::REASONS_RECORDED_SINCE;
    if r.created_at.as_str() < since {
        format!("Not recorded (before {since})")
    } else {
        "No reason recorded".to_string()
    }
}

/// How a run whose CI went red ended up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiEnd {
    /// Green after a fix Coderhelm pushed.
    GreenAfterFix,
    /// Green after a re-run of the failed jobs, with no code change.
    Flaky,
    /// Green without a Coderhelm fix (a person's push, a later run).
    GreenOtherwise,
    /// Red only on checks that also fail on the base branch.
    RedOnBase,
    /// The run is still working on it.
    Running,
    /// Coderhelm left it red to a person, for this reason.
    GaveUp(String),
}

/// The worker's fix attempt ceiling (resume.rs `MAX_CI_FIX_CYCLES`): an older
/// run that recorded no reason but reached it ran out of attempts.
const MAX_CI_FIX_ATTEMPTS: u64 = 10;

/// What a stored `ci_end` means, for a person.
fn gave_up_label(ci_end: &str, r: &RunRow) -> String {
    match ci_end {
        "budget" => "Fix attempts used up".into(),
        "stalled" => "Same failure after a fix".into(),
        "no_change" => "Fix found nothing to change".into(),
        "no_progress" => "Fixes stopped changing the code".into(),
        "fix_error" => "Fix attempt errored".into(),
        "" if r.ci_fix_attempts >= MAX_CI_FIX_ATTEMPTS => "Fix attempts used up".into(),
        "" => format!(
            "Not recorded (before {})",
            common::run_fail::REASONS_RECORDED_SINCE
        ),
        other => other.replace('_', " "),
    }
}

impl RunRow {
    /// CI went red on this run at least once.
    fn ci_went_red(&self) -> bool {
        self.ci_fix_attempts > 0 || self.ci_reruns > 0 || !self.ci_red_on_base.is_empty()
    }
    /// Red only on checks that already failed on the base branch: never its
    /// own failure to fix.
    fn red_only_on_base(&self) -> bool {
        !self.ci_red_on_base.is_empty() && self.ci_fix_attempts == 0 && self.ci_reruns == 0
    }
    fn ci_end_state(&self) -> CiEnd {
        if self.ci_last == "passed" {
            return if self.ci_flaky {
                CiEnd::Flaky
            } else if self.ci_fix_commits > 0 {
                CiEnd::GreenAfterFix
            } else {
                CiEnd::GreenOtherwise
            };
        }
        if matches!(
            self.status.as_str(),
            "running" | "awaiting_ci" | "queued" | "pending"
        ) {
            return CiEnd::Running;
        }
        if !self.ci_red_on_base.is_empty() && matches!(self.ci_end.as_str(), "" | "red_on_base") {
            return CiEnd::RedOnBase;
        }
        CiEnd::GaveUp(gave_up_label(&self.ci_end, self))
    }
}

/// CI self-fixes, told honestly: runs red only on checks that fail on the
/// base branch too are counted apart, and every red run is placed in one
/// outcome, with the reason when Coderhelm gave up.
pub fn ci_outcomes(runs: &[&RunRow]) -> Value {
    let red: Vec<&RunRow> = runs.iter().copied().filter(|r| r.ci_went_red()).collect();
    let own: Vec<&RunRow> = red
        .iter()
        .copied()
        .filter(|r| !r.red_only_on_base())
        .collect();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for r in &red {
        let key = match r.ci_end_state() {
            CiEnd::GreenAfterFix => "green_after_fix",
            CiEnd::Flaky => "flaky",
            CiEnd::GreenOtherwise => "green_without_fix",
            CiEnd::RedOnBase => "red_on_base",
            CiEnd::Running => "running",
            CiEnd::GaveUp(why) => {
                *reasons.entry(why).or_default() += 1;
                "gave_up"
            }
        };
        *counts.entry(key).or_default() += 1;
    }
    let c = |k: &str| counts.get(k).copied().unwrap_or(0);
    let mut gave_up_reasons: Vec<Value> = reasons
        .into_iter()
        .map(|(reason, count)| json!({"reason": reason, "count": count}))
        .collect();
    gave_up_reasons.sort_by(|a, b| b["count"].as_u64().cmp(&a["count"].as_u64()));
    let fixed = c("green_after_fix");
    // Runs still working on it have no outcome yet.
    let settled = own.len().saturating_sub(c("running"));
    json!({
        "runs_with_red_ci": own.len(),
        "red_on_base_runs": red.len() - own.len(),
        "fixed_by_coderhelm": fixed,
        "fix_rate": ratio(fixed as f64, settled as f64),
        "fix_attempts": own.iter().map(|r| r.ci_fix_attempts).sum::<u64>(),
        "fix_commits": own.iter().map(|r| r.ci_fix_commits).sum::<u64>(),
        "reruns": red.iter().map(|r| r.ci_reruns).sum::<u64>(),
        "outcomes": {
            "green_after_fix": fixed,
            "green_without_fix": c("green_without_fix"),
            "flaky": c("flaky"),
            "red_on_base": c("red_on_base"),
            "gave_up": c("gave_up"),
            "running": c("running"),
        },
        "gave_up_reasons": gave_up_reasons,
    })
}

/// One row per pull request among `runs`: a rerun that pushed to the same PR
/// does not open another. The row kept knows the most: a merged one first,
/// then the latest run.
fn distinct_prs<'a>(runs: &[&'a RunRow]) -> Vec<&'a RunRow> {
    let mut by: HashMap<(String, u64), &RunRow> = HashMap::new();
    for r in runs.iter().copied().filter(|r| r.has_pr()) {
        let e = by.entry(r.pr_key()).or_insert(r);
        if (r.merged(), &r.created_at) > (e.merged(), &e.created_at) {
            *e = r;
        }
    }
    let mut out: Vec<&RunRow> = by.into_values().collect();
    out.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then(a.run_id.cmp(&b.run_id))
    });
    out
}

/// The headline numbers for one period. Pull requests are counted once each,
/// however many runs worked on them.
pub fn summary(runs: &[&RunRow], reviews: &[&ReviewRow]) -> Value {
    let total = runs.len() as f64;
    let with_pr = distinct_prs(runs);
    let merged: Vec<&RunRow> = with_pr.iter().copied().filter(|r| r.merged()).collect();
    let closed = with_pr
        .iter()
        .filter(|r| !r.merged() && r.pr_outcome == "closed")
        .count();
    // A run a person closed on purpose is not a failure; it is counted on
    // its own.
    let failed = runs
        .iter()
        .filter(|r| r.status == common::run_status::FAILED)
        .count();
    let closed_by_person = runs
        .iter()
        .filter(|r| r.status == common::run_status::CLOSED)
        .count();
    let cost: f64 = runs.iter().map(|r| r.cost_usd).sum();
    let tokens: f64 = runs.iter().map(|r| r.tokens()).sum();
    let hours: Vec<f64> = merged.iter().filter_map(|r| r.hours_to_merge()).collect();
    let edits_known: Vec<&&RunRow> = merged
        .iter()
        .filter(|r| r.human_commits.is_some())
        .collect();
    let untouched = edits_known
        .iter()
        .filter(|r| r.human_commits == Some(0))
        .count();
    let durations: Vec<f64> = runs
        .iter()
        .filter(|r| r.status != "failed" && r.duration_s > 0.0)
        .map(|r| r.duration_s / 60.0)
        .collect();
    let posted = reviews.iter().filter(|r| r.is_posted_review()).count();
    json!({
        "runs": runs.len(),
        "prs_opened": with_pr.len(),
        "prs_merged": merged.len(),
        "prs_closed": closed,
        "prs_open": with_pr.len().saturating_sub(merged.len() + closed),
        "merge_rate": ratio(merged.len() as f64, with_pr.len() as f64),
        "failed_runs": failed,
        "failure_rate": ratio(failed as f64, total),
        "closed_runs": closed_by_person,
        "cost_usd": round2(cost),
        "cost_per_run": ratio(cost, total).map(round2),
        "cost_per_merged_pr": ratio(cost, merged.len() as f64).map(round2),
        "tokens": tokens,
        "tokens_per_run": ratio(tokens, total).map(f64::round),
        "tokens_per_merged_pr": ratio(tokens, merged.len() as f64).map(f64::round),
        "tokens_in": runs.iter().map(|r| r.tokens_in).sum::<f64>(),
        "tokens_out": runs.iter().map(|r| r.tokens_out).sum::<f64>(),
        "cache_read_tokens": runs.iter().map(|r| r.cache_read_tokens).sum::<f64>(),
        "cache_write_tokens": runs.iter().map(|r| r.cache_write_tokens).sum::<f64>(),
        "median_run_minutes": median(durations).map(round2),
        "median_hours_to_merge": median(hours.clone()).map(round2),
        "time_to_merge_known": hours.len(),
        "merged_untouched": untouched,
        "merged_with_edits": edits_known.len() - untouched,
        "edits_known": edits_known.len(),
        "reviews": posted,
    })
}

// ── Compared with the period before ───────────────────────────────────────

/// How much work there was in one period, each thing counted once: a ticket
/// worked on by three runs is one ticket, a PR reviewed five times is one PR
/// reviewed and five reviews.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Counts {
    /// Distinct tickets the runs worked on.
    pub tickets: usize,
    /// Every run, reruns included.
    pub runs: usize,
    /// Distinct pull requests CoderHelm's runs opened or pushed to.
    pub prs_opened: usize,
    /// Of those, merged so far.
    pub prs_merged: usize,
    /// Of those, neither merged nor closed yet.
    pub prs_open: usize,
    /// Distinct pull requests that got at least one posted review.
    pub prs_reviewed: usize,
    /// Reviews posted (answers to questions and silent re-checks left out).
    pub reviews_posted: usize,
    pub approvals: usize,
    pub changes_requested: usize,
    /// Reviews posted as a plain comment, without approving or asking for changes.
    pub comments: usize,
    /// Distinct repositories with a run or a posted review.
    pub repos: usize,
    /// Distinct people who asked for the runs.
    pub people: usize,
    /// Runs that recorded who (or what) asked for them.
    pub runs_with_requester: usize,
}

pub fn counts(runs: &[&RunRow], reviews: &[&ReviewRow], known: &[Known]) -> Counts {
    use std::collections::HashSet;
    let prs = distinct_prs(runs);
    let posted: Vec<&&ReviewRow> = reviews.iter().filter(|r| r.is_posted_review()).collect();
    let shown = |v: &str| posted.iter().filter(|r| r.shown_as() == v).count();
    let requesters: Vec<Option<Requester>> = runs.iter().map(|r| r.requester()).collect();
    let (groups, at) = common::requester::group(&requesters, known);
    let people: HashSet<usize> = at
        .iter()
        .copied()
        .filter(|&g| groups[g].kind == common::requester::Kind::Person)
        .collect();
    let repos: HashSet<String> = runs
        .iter()
        .map(|r| r.repo.to_lowercase())
        .chain(posted.iter().map(|r| r.repo.to_lowercase()))
        .filter(|r| !r.is_empty())
        .collect();
    Counts {
        tickets: runs
            .iter()
            .map(|r| r.ticket_key())
            .collect::<HashSet<_>>()
            .len(),
        runs: runs.len(),
        prs_opened: prs.len(),
        prs_merged: prs.iter().filter(|r| r.merged()).count(),
        prs_open: prs
            .iter()
            .filter(|r| !r.merged() && r.pr_outcome != "closed")
            .count(),
        prs_reviewed: posted
            .iter()
            .map(|r| r.pr_key())
            .collect::<HashSet<_>>()
            .len(),
        reviews_posted: posted.len(),
        approvals: shown("APPROVE"),
        changes_requested: shown("REQUEST_CHANGES"),
        comments: shown("COMMENT"),
        repos: repos.len(),
        people: people.len(),
        runs_with_requester: requesters.iter().filter(|r| r.is_some()).count(),
    }
}

/// Who asked is only comparable when both periods recorded it for nearly
/// every run; before requesters were saved, a jump from nobody is not real.
const REQUESTER_COVERAGE: f64 = 0.9;

fn requester_recorded(c: &Counts) -> bool {
    c.runs == 0 || c.runs_with_requester as f64 >= REQUESTER_COVERAGE * c.runs as f64
}

/// Running totals by day for one period: for each day slot, how many
/// distinct tickets, PRs opened and PRs reviewed so far.
fn cumulative(w: Window, runs: &[&RunRow], reviews: &[&ReviewRow]) -> Vec<[usize; 3]> {
    let slots = w.day_slots().max(0) as usize;
    let mut first: [HashMap<String, i64>; 3] = Default::default();
    let mut note = |m: usize, key: String, slot: i64| {
        let e = first[m].entry(key).or_insert(slot);
        *e = (*e).min(slot);
    };
    for r in runs {
        if let Some(slot) = w.at(&r.created_at) {
            note(0, r.ticket_key(), slot);
            if r.has_pr() {
                let (repo, pr) = r.pr_key();
                note(1, format!("{repo}#{pr}"), slot);
            }
        }
    }
    for r in reviews.iter().filter(|r| r.is_posted_review()) {
        if let Some(slot) = w.at(&r.created_at) {
            let (repo, pr) = r.pr_key();
            note(2, format!("{repo}#{pr}"), slot);
        }
    }
    let mut out = vec![[0usize; 3]; slots];
    for (m, seen) in first.iter().enumerate() {
        for &slot in seen.values() {
            if let Some(row) = out.get_mut(slot as usize) {
                row[m] += 1;
            }
        }
    }
    for i in 1..out.len() {
        let before = out[i - 1];
        for (m, v) in out[i].iter_mut().enumerate() {
            *v += before[m];
        }
    }
    out
}

/// This period against the one before it (and, month to date, all of last
/// month): the counts, and running totals by day for the chart.
pub fn comparison(
    window: Window,
    prev: Window,
    previous_full: Option<Window>,
    runs: &[&RunRow],
    reviews: &[&ReviewRow],
    known: &[Known],
) -> Value {
    let pick = |w: Window| {
        let rs: Vec<&RunRow> = runs
            .iter()
            .copied()
            .filter(|r| w.contains(&r.created_at))
            .collect();
        let rv: Vec<&ReviewRow> = reviews
            .iter()
            .copied()
            .filter(|r| w.contains(&r.created_at))
            .collect();
        (rs, rv)
    };
    let (cr, cv) = pick(window);
    let (pr, pv) = pick(prev);
    let current = counts(&cr, &cv, known);
    let previous = counts(&pr, &pv, known);
    let full = previous_full.map(|w| {
        let (r, v) = pick(w);
        counts(&r, &v, known)
    });
    let cur_days = cumulative(window, &cr, &cv);
    let prev_days = cumulative(prev, &pr, &pv);
    let daily: Vec<Value> = cur_days
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let p = prev_days.get(i);
            let shift = Duration::days(i as i64);
            json!({
                "offset": i,
                "day": (window.start + shift).format("%Y-%m-%d").to_string(),
                "previous_day": p.map(|_| (prev.start + shift).format("%Y-%m-%d").to_string()),
                "tickets": c[0],
                "prs_opened": c[1],
                "prs_reviewed": c[2],
                "previous_tickets": p.map(|p| p[0]),
                "previous_prs_opened": p.map(|p| p[1]),
                "previous_prs_reviewed": p.map(|p| p[2]),
            })
        })
        .collect();
    let people_comparable = requester_recorded(&current)
        && requester_recorded(&previous)
        && full.as_ref().is_none_or(requester_recorded);
    json!({
        "current": current,
        "previous": previous,
        "previous_full": full,
        "previous_full_period": previous_full.map(|w| json!({"start": w.start.to_rfc3339(), "end": w.end.to_rfc3339()})),
        "people_comparable": people_comparable,
        "daily": daily,
    })
}

fn day(ts: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&Utc).format("%Y-%m-%d").to_string())
}

/// Everything the page shows, for one period and the one before it.
#[allow(clippy::too_many_arguments)]
pub fn build(
    window: Window,
    prev: Window,
    previous_full: Option<Window>,
    runs: &[RunRow],
    reviews: &[ReviewRow],
    lookups: &[LookupRow],
    traces: &[TraceRow],
    known: &[Known],
    repo: Option<&str>,
) -> Value {
    let repo_ok = |r: &str| repo.is_none_or(|want| r.eq_ignore_ascii_case(want));
    let cur_runs: Vec<&RunRow> = runs
        .iter()
        .filter(|r| window.contains(&r.created_at) && repo_ok(&r.repo))
        .collect();
    let prev_runs: Vec<&RunRow> = runs
        .iter()
        .filter(|r| prev.contains(&r.created_at) && repo_ok(&r.repo))
        .collect();
    let cur_reviews: Vec<&ReviewRow> = reviews
        .iter()
        .filter(|r| window.contains(&r.created_at) && repo_ok(&r.repo))
        .collect();
    let prev_reviews: Vec<&ReviewRow> = reviews
        .iter()
        .filter(|r| prev.contains(&r.created_at) && repo_ok(&r.repo))
        .collect();
    let in_repo_runs: Vec<&RunRow> = runs.iter().filter(|r| repo_ok(&r.repo)).collect();
    let in_repo_reviews: Vec<&ReviewRow> = reviews.iter().filter(|r| repo_ok(&r.repo)).collect();
    let compared = comparison(
        window,
        prev,
        previous_full,
        &in_repo_runs,
        &in_repo_reviews,
        known,
    );
    let cur_lookups: Vec<&LookupRow> = lookups
        .iter()
        .filter(|l| window.contains(&l.at) && (repo.is_none() || repo_ok(&l.repo)))
        .collect();

    // Daily series across the whole window, zero-filled.
    let mut daily: BTreeMap<String, [f64; 7]> = BTreeMap::new();
    let mut d = window.start;
    while d < window.end {
        daily.insert(d.format("%Y-%m-%d").to_string(), [0.0; 7]);
        d += Duration::days(1);
    }
    // A pull request counts on the day of its first run in the window.
    let mut first_pr_run: HashMap<(String, u64), &str> = HashMap::new();
    for r in cur_runs.iter().filter(|r| r.has_pr()) {
        let e = first_pr_run.entry(r.pr_key()).or_insert(&r.created_at);
        if r.created_at.as_str() < *e {
            *e = &r.created_at;
        }
    }
    for ts in first_pr_run.values() {
        if let Some(e) = day(ts).and_then(|k| daily.get_mut(&k)) {
            e[1] += 1.0;
        }
    }
    for r in &cur_runs {
        if let Some(e) = day(&r.created_at).and_then(|k| daily.get_mut(&k)) {
            e[0] += 1.0;
            if r.status == "failed" {
                e[3] += 1.0;
            }
            e[4] += r.cost_usd;
            e[6] += r.tokens();
        }
    }
    // Merges land on the day they happened, when known; once per PR.
    let mut merged_seen = std::collections::HashSet::new();
    for r in runs.iter().filter(|r| r.merged() && repo_ok(&r.repo)) {
        if let Some(e) = day(&r.merged_at)
            .filter(|_| window.contains(&r.merged_at))
            .filter(|_| merged_seen.insert(r.pr_key()))
            .and_then(|k| daily.get_mut(&k))
        {
            e[2] += 1.0;
        }
    }
    for r in &cur_reviews {
        if r.is_posted_review() {
            if let Some(e) = day(&r.created_at).and_then(|k| daily.get_mut(&k)) {
                e[5] += 1.0;
            }
        }
    }
    let daily: Vec<Value> = daily
        .into_iter()
        .map(|(day, v)| json!({"day": day, "runs": v[0], "prs_opened": v[1], "merged": v[2], "failed": v[3], "cost_usd": round2(v[4]), "tokens": v[6], "reviews": v[5]}))
        .collect();

    // Breakdowns. PRs and merges count once per pull request.
    type PrSets = (
        std::collections::HashSet<(String, u64)>,
        std::collections::HashSet<(String, u64)>,
    );
    let mut source_prs: HashMap<String, PrSets> = HashMap::new();
    let mut repo_prs: HashMap<String, PrSets> = HashMap::new();
    let mut by_source: BTreeMap<String, [f64; 3]> = BTreeMap::new();
    let mut by_repo: BTreeMap<String, [f64; 6]> = BTreeMap::new();
    for r in &cur_runs {
        let src = if r.source.is_empty() {
            "other".to_string()
        } else {
            r.source.clone()
        };
        if r.has_pr() {
            for sets in [
                source_prs.entry(src.clone()).or_default(),
                repo_prs.entry(r.repo.clone()).or_default(),
            ] {
                sets.0.insert(r.pr_key());
                if r.merged() {
                    sets.1.insert(r.pr_key());
                }
            }
        }
        by_source.entry(src).or_default()[0] += 1.0;
        let e = by_repo.entry(r.repo.clone()).or_default();
        e[0] += 1.0;
        if r.status == "failed" {
            e[3] += 1.0;
        }
        e[4] += r.cost_usd;
        e[5] += r.tokens();
    }
    for (src, (prs, merged)) in &source_prs {
        if let Some(e) = by_source.get_mut(src) {
            e[1] = prs.len() as f64;
            e[2] = merged.len() as f64;
        }
    }
    for (repo, (prs, merged)) in &repo_prs {
        if let Some(e) = by_repo.get_mut(repo) {
            e[1] = prs.len() as f64;
            e[2] = merged.len() as f64;
        }
    }
    // Who asks: one row per person, however each source named them.
    let requesters: Vec<Option<Requester>> = cur_runs.iter().map(|r| r.requester()).collect();
    let (groups, at) = common::requester::group(&requesters, known);
    let mut by_person = vec![[0.0f64; 2]; groups.len()];
    for (r, &g) in cur_runs.iter().zip(&at) {
        by_person[g][0] += 1.0;
        if r.merged() {
            by_person[g][1] += 1.0;
        }
    }
    let mut reviews_by_repo: HashMap<String, usize> = HashMap::new();
    for r in cur_reviews.iter().filter(|r| r.is_posted_review()) {
        *reviews_by_repo.entry(r.repo.clone()).or_default() += 1;
    }
    for repo in reviews_by_repo.keys() {
        by_repo.entry(repo.clone()).or_default();
    }
    let mut repos: Vec<Value> = by_repo
        .iter()
        .map(|(repo, v)| {
            json!({"repo": repo, "runs": v[0], "prs": v[1], "merged": v[2], "failed": v[3],
                   "cost_usd": round2(v[4]), "tokens": v[5], "merge_rate": ratio(v[2], v[1]),
                   "reviews": reviews_by_repo.get(repo).copied().unwrap_or(0)})
        })
        .collect();
    repos.sort_by(|a, b| {
        b["runs"]
            .as_f64()
            .partial_cmp(&a["runs"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let sources: Vec<Value> = by_source
        .iter()
        .map(|(src, v)| json!({"source": src, "runs": v[0], "prs": v[1], "merged": v[2], "merge_rate": ratio(v[2], v[1])}))
        .collect();
    let mut people: Vec<Value> = groups
        .iter()
        .zip(&by_person)
        .map(|(g, v)| json!({"person": g.label, "kind": g.kind, "runs": v[0], "merged": v[1]}))
        .collect();
    people.sort_by(|a, b| {
        b["runs"]
            .as_f64()
            .partial_cmp(&a["runs"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a["person"].as_str().cmp(&b["person"].as_str()))
    });

    // The last pass each run's traces name (kept 30 days): a failed run with
    // no pass of its own on record gets its reason's pass from here.
    let mut trace_pass: HashMap<&str, (&str, &str)> = HashMap::new();
    for t in traces.iter().filter(|t| !t.run_id.is_empty()) {
        let e = trace_pass
            .entry(t.run_id.as_str())
            .or_insert((t.timestamp.as_str(), t.pass.as_str()));
        if t.timestamp.as_str() > e.0 {
            *e = (t.timestamp.as_str(), t.pass.as_str());
        }
    }
    let reason_of =
        |r: &RunRow| run_failure_reason(r, trace_pass.get(r.run_id.as_str()).map(|e| e.1));

    // Failures, grouped by reason.
    let mut failures: HashMap<String, (usize, &RunRow)> = HashMap::new();
    for r in cur_runs.iter().filter(|r| r.status == "failed") {
        let e = failures.entry(reason_of(r)).or_insert((0, r));
        e.0 += 1;
        if r.created_at > e.1.created_at {
            e.1 = r;
        }
    }
    let mut failures: Vec<Value> = failures
        .into_iter()
        .map(|(reason, (count, last))| json!({"reason": reason, "count": count, "last_run_id": last.run_id, "last_at": last.created_at}))
        .collect();
    failures.sort_by(|a, b| b["count"].as_u64().cmp(&a["count"].as_u64()));
    failures.truncate(8);

    // CI self-fixes, with how each red run ended.
    let ci = ci_outcomes(&cur_runs);

    // Reviews, split by what GitHub shows.
    let posted: Vec<&&ReviewRow> = cur_reviews
        .iter()
        .filter(|r| r.is_posted_review())
        .collect();
    let verdict = |v: &str| posted.iter().filter(|r| r.shown_as() == v).count();
    let known: Vec<&&&ReviewRow> = posted.iter().filter(|r| r.prior_fixed.is_some()).collect();
    let acted: u64 = known.iter().filter_map(|r| r.prior_fixed).sum();
    let settled: u64 = known.iter().filter_map(|r| r.prior_settled).sum();
    let reviews_json = json!({
        "posted": posted.len(),
        "approve": verdict("APPROVE"),
        "request_changes": verdict("REQUEST_CHANGES"),
        "comment": verdict("COMMENT"),
        "questions": cur_reviews.iter().filter(|r| r.verdict == "QUESTION").count(),
        "prs_reviewed": posted.iter().map(|r| r.pr_key()).collect::<std::collections::HashSet<_>>().len(),
        "findings": posted.iter().map(|r| r.findings).sum::<u64>(),
        "findings_fixed": acted,
        "findings_settled_by_person": settled,
        "acted_on_rate": ratio(acted as f64, (acted + settled) as f64),
        "outcomes_known_reviews": known.len(),
        "thumbs_up": posted.iter().map(|r| r.thumbs_up).sum::<u64>(),
        "thumbs_down": posted.iter().map(|r| r.thumbs_down).sum::<u64>(),
    });

    // Data-source lookups.
    let mut by_lookup_source: BTreeMap<String, [usize; 3]> = BTreeMap::new();
    let mut by_actor: BTreeMap<String, usize> = BTreeMap::new();
    for l in &cur_lookups {
        let e = by_lookup_source.entry(l.source_name.clone()).or_default();
        e[0] += 1;
        match l.outcome.as_str() {
            "refused" => e[1] += 1,
            "failed" => e[2] += 1,
            _ => {}
        }
        let actor = match l.actor.as_str() {
            "reviewer" | "planner" | "coder" => l.actor.clone(),
            _ => "people".to_string(),
        };
        *by_actor.entry(actor).or_default() += 1;
    }
    let lookups_json = json!({
        "total": cur_lookups.len(),
        "refused": cur_lookups.iter().filter(|l| l.outcome == "refused").count(),
        "failed": cur_lookups.iter().filter(|l| l.outcome == "failed").count(),
        "by_source": by_lookup_source.iter().map(|(k, v)| json!({"source": k, "total": v[0], "refused": v[1], "failed": v[2]})).collect::<Vec<_>>(),
        "by_actor": by_actor.iter().map(|(k, v)| json!({"actor": k, "total": v})).collect::<Vec<_>>(),
    });

    // Where the work goes, by step (pass traces are kept 30 days).
    let mut by_step: BTreeMap<String, (usize, f64, f64, f64)> = BTreeMap::new();
    for t in traces.iter().filter(|t| window.contains(&t.timestamp)) {
        let step = t.pass.split(':').next().unwrap_or("").to_string();
        if step.is_empty() {
            continue;
        }
        let e = by_step.entry(step).or_default();
        e.0 += 1;
        e.1 += t.input_tokens;
        e.2 += t.output_tokens;
        e.3 += t.duration_ms;
    }
    let mut steps: Vec<Value> = by_step
        .into_iter()
        .map(|(step, (count, ti, to, ms))| json!({"step": step, "count": count, "tokens_in": ti, "tokens_out": to, "avg_seconds": round2(ms / count as f64 / 1000.0)}))
        .collect();
    steps.sort_by(|a, b| {
        let t = |v: &Value| {
            v["tokens_in"].as_f64().unwrap_or(0.0) + v["tokens_out"].as_f64().unwrap_or(0.0)
        };
        t(b).partial_cmp(&t(a)).unwrap_or(std::cmp::Ordering::Equal)
    });

    // Drill-downs.
    let mut merged_recent: Vec<&&RunRow> = cur_runs.iter().filter(|r| r.merged()).collect();
    merged_recent.sort_by(|a, b| {
        b.merged_at
            .cmp(&a.merged_at)
            .then(b.created_at.cmp(&a.created_at))
    });
    let merged_recent: Vec<Value> = merged_recent
        .iter()
        .take(10)
        .map(|r| json!({"run_id": r.run_id, "title": r.title, "repo": r.repo, "pr_number": r.pr_number, "pr_url": r.pr_url,
                        "hours_to_merge": r.hours_to_merge().map(round2), "human_commits": r.human_commits}))
        .collect();
    let mut failed_recent: Vec<&&RunRow> =
        cur_runs.iter().filter(|r| r.status == "failed").collect();
    failed_recent.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let failed_recent: Vec<Value> = failed_recent
        .iter()
        .take(10)
        .map(|r| json!({"run_id": r.run_id, "title": r.title, "repo": r.repo, "reason": reason_of(r), "at": r.created_at}))
        .collect();

    let mut repo_names: Vec<String> = runs
        .iter()
        .map(|r| r.repo.clone())
        .chain(reviews.iter().map(|r| r.repo.clone()))
        .filter(|r| !r.is_empty())
        .collect();
    repo_names.sort();
    repo_names.dedup();

    json!({
        "period": {"start": window.start.to_rfc3339(), "end": window.end.to_rfc3339()},
        "previous_period": {"start": prev.start.to_rfc3339(), "end": prev.end.to_rfc3339()},
        "summary": summary(&cur_runs, &cur_reviews),
        "previous": summary(&prev_runs, &prev_reviews),
        "comparison": compared,
        "daily": daily,
        "sources": sources,
        "repos": repos,
        "people": people,
        "failures": failures,
        "ci": ci,
        "reviews": reviews_json,
        "lookups": lookups_json,
        "steps": steps,
        "steps_window_days": 30,
        "merged_recent": merged_recent,
        "failed_recent": failed_recent,
        "repo_names": repo_names,
    })
}

// ── Loading ────────────────────────────────────────────────────────────────

/// The smallest and largest run ids created in a window (run ids are ULIDs).
fn run_id_bounds(w: Window) -> (String, String) {
    let lo = ulid::Ulid::from_parts(w.start.timestamp_millis().max(0) as u64, 0);
    let hi = ulid::Ulid::from_parts(w.end.timestamp_millis().max(0) as u64, (1u128 << 80) - 1);
    (lo.to_string(), hi.to_string())
}

async fn load_runs(state: &AppState, team: &str, w: Window) -> Result<Vec<RunRow>, String> {
    let (lo, hi) = run_id_bounds(w);
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :t AND run_id BETWEEN :lo AND :hi")
            .expression_attribute_values(":t", av(team))
            .expression_attribute_values(":lo", av(&lo))
            .expression_attribute_values(":hi", av(&hi))
            .projection_expression(
                "run_id, created_at, #s, repo, ticket_source, sender, requester_key, requester_name, \
                 requester_email, title, ticket_id, cost_usd, tokens_in, tokens_out, \
                 cache_read_tokens, cache_write_tokens, \
                 duration_s, pr_number, pr_url, error_message, ci_fix_attempts, ci_fix_commits, ci_last, \
                 pr_outcome, merged_at, pr_opened_at, human_commits, #err, failed_pass, current_pass, \
                 pass_history, ci_end, ci_red_on_base, ci_reruns, ci_flaky",
            )
            .expression_attribute_names("#s", "status")
            .expression_attribute_names("#err", "error")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("query runs: {e}"))?;
        out.extend(resp.items().iter().map(RunRow::from_item));
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// Every item in the settings table under `team` with `lo <= sk <= hi`.
async fn load_settings_range(
    state: &AppState,
    team: &str,
    lo: &str,
    hi: &str,
) -> Result<Vec<Item>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":lo", av(lo))
            .expression_attribute_values(":hi", av(hi))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("query settings range: {e}"))?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// The people a run's requester can be matched to: the team's members, and
/// the Jira users the gateway has seen on webhooks. Best effort: without
/// them, rows are still grouped by what each run recorded.
async fn load_known(state: &AppState, team: &str) -> Vec<Known> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":u", av("USER#"))
            .projection_expression("email, #n, github_login")
            .expression_attribute_names("#n", "name")
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(resp) = resp else {
            warn!("Could not read team members for analytics");
            break;
        };
        out.extend(resp.items().iter().map(|i| Known {
            name: s(i, "name"),
            email: s(i, "email"),
            github_login: s(i, "github_login"),
            member: true,
            ..Default::default()
        }));
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let prefix = common::requester::JIRA_USER_PREFIX;
    match load_settings_range(state, team, prefix, &format!("{prefix}~")).await {
        Ok(items) => out.extend(items.iter().map(|i| Known {
            name: s(i, "name"),
            email: s(i, "email"),
            jira_id: s(i, "sk").trim_start_matches(prefix).to_string(),
            ..Default::default()
        })),
        Err(e) => warn!(error = %e, "Could not read Jira users for analytics"),
    }
    out
}

async fn load_traces(state: &AppState, team: &str) -> Vec<TraceRow> {
    if state.config.traces_table_name.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.traces_table_name)
            .key_condition_expression("team_id = :t AND begins_with(sk, :p)")
            .expression_attribute_values(":t", av(team))
            .expression_attribute_values(":p", av("RUN#"))
            .projection_expression("sk, pass, #ts, input_tokens, output_tokens, duration_ms")
            .expression_attribute_names("#ts", "timestamp")
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(resp) = resp else {
            warn!("Could not read pass traces for analytics");
            break;
        };
        out.extend(resp.items().iter().map(TraceRow::from_item));
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    out
}

/// One write that brings a team's review stats in line with its review
/// records.
#[derive(Debug, Clone, PartialEq)]
pub enum StatFix {
    /// The review has no stats record (written before they existed, or a
    /// copy DynamoDB throttled): copy it. Such reviews did not record whether
    /// they were posted, so they count as posted.
    Copy(Item),
    /// The stats record predates `posted_as`: add what GitHub shows.
    PostedAs { sk: String, posted_as: String },
}

/// Review records (`REVIEW#`, with bodies) against the stats records already
/// there (key → whether it has `posted_as`) → the writes that make every
/// review counted once, with what was posted.
pub fn stat_fixes(team: &str, reviews: &[Item], stats: &HashMap<String, bool>) -> Vec<StatFix> {
    let mut out = Vec::new();
    for i in reviews {
        let created_at = s(i, "created_at");
        let repo = s(i, "repo");
        if created_at.is_empty() || repo.is_empty() {
            continue;
        }
        let pr = n(i, "pr_number") as u64;
        let posted_as = s(i, "posted_as");
        let sk = common::review_stats::stat_sk(&created_at, &repo, pr);
        match stats.get(&sk) {
            Some(true) => {}
            Some(false) => {
                if !posted_as.is_empty() {
                    out.push(StatFix::PostedAs { sk, posted_as });
                }
            }
            None => {
                let counts = common::review_stats::ReviewCounts {
                    findings: common::review_stats::count_findings(&s(i, "body")),
                    posted: true,
                    ..Default::default()
                };
                let mut item = common::review_stats::stat_item(
                    team,
                    &created_at,
                    &repo,
                    pr,
                    &s(i, "verdict"),
                    &posted_as,
                    &s(i, "risk"),
                    &s(i, "trigger"),
                    &counts,
                );
                for k in ["thumbs_up", "thumbs_down"] {
                    item.insert(k.into(), AttributeValue::N((n(i, k) as u64).to_string()));
                }
                out.push(StatFix::Copy(item));
            }
        }
    }
    out
}

/// How long one page load may spend repairing stats. What is left is picked
/// up by the next load: every write is idempotent.
const REPAIR_BUDGET: std::time::Duration = std::time::Duration::from_secs(12);
/// Review records are read with their bodies; enough pages for any team.
const REPAIR_MAX_PAGES: usize = 400;

/// Makes every review record have a stats record carrying what was posted,
/// once per team. Writes only what is missing, never over a stats record the
/// worker wrote (it knows what became of earlier findings; a copy does not).
async fn repair_review_stats(state: &AppState, team: &str) -> Result<(), String> {
    use futures::stream::{self, StreamExt};
    let table = &state.config.settings_table_name;
    let marker = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", av(team))
        .key("sk", av(common::review_stats::REPAIR_MARKER_SK))
        .send()
        .await
        .map_err(|e| format!("read repair marker: {e}"))?;
    if marker.item().is_some() {
        return Ok(());
    }
    let deadline = std::time::Instant::now() + REPAIR_BUDGET;
    let prefix = common::review_stats::PREFIX;
    let stats: HashMap<String, bool> =
        load_settings_range(state, team, prefix, &format!("{prefix}~"))
            .await?
            .iter()
            .map(|i| (s(i, "sk"), i.contains_key("posted_as")))
            .collect();

    let mut fixes = Vec::new();
    let mut complete = false;
    let mut start: Option<Item> = None;
    for _ in 0..REPAIR_MAX_PAGES {
        if std::time::Instant::now() >= deadline {
            break;
        }
        let resp = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":p", av("REVIEW#"))
            .projection_expression(
                "repo, pr_number, verdict, posted_as, risk, #tr, created_at, body, thumbs_up, thumbs_down",
            )
            .expression_attribute_names("#tr", "trigger")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("read reviews for repair: {e}"))?;
        // Bodies are only needed to count findings; each page is dropped here.
        fixes.extend(stat_fixes(team, resp.items(), &stats));
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => {
                complete = true;
                break;
            }
        }
    }

    let (copies, stamps) = fixes.iter().fold((0, 0), |(c, p), f| match f {
        StatFix::Copy(_) => (c + 1, p),
        StatFix::PostedAs { .. } => (c, p + 1),
    });
    let results: Vec<Result<(), String>> = stream::iter(fixes)
        .map(|fix| async move {
            if std::time::Instant::now() >= deadline {
                return Err("out of time".to_string());
            }
            let res = match fix {
                StatFix::Copy(item) => state
                    .dynamo
                    .put_item()
                    .table_name(table)
                    .set_item(Some(item))
                    .condition_expression("attribute_not_exists(sk)")
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        e.as_service_error()
                            .is_some_and(|e| e.is_conditional_check_failed_exception())
                            .then_some(())
                            .ok_or_else(|| format!("copy review stat: {e}"))
                    }),
                StatFix::PostedAs { sk, posted_as } => state
                    .dynamo
                    .update_item()
                    .table_name(table)
                    .key("pk", av(team))
                    .key("sk", av(&sk))
                    .condition_expression(
                        "attribute_exists(sk) AND attribute_not_exists(posted_as)",
                    )
                    .update_expression("SET posted_as = :p")
                    .expression_attribute_values(":p", av(&posted_as))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        e.as_service_error()
                            .is_some_and(|e| e.is_conditional_check_failed_exception())
                            .then_some(())
                            .ok_or_else(|| format!("stamp review stat: {e}"))
                    }),
            };
            // A failed condition means the record is already right.
            match res {
                Ok(()) | Err(Ok(())) => Ok(()),
                Err(Err(e)) => Err(e),
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;
    let failed: Vec<&String> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    info!(
        team,
        copies,
        stamps,
        failed = failed.len(),
        complete,
        "Review stats repair pass"
    );
    if !complete || !failed.is_empty() {
        return Err(format!(
            "repair unfinished ({} of {} writes left or failed{}); the next load continues",
            failed.len(),
            copies + stamps,
            failed
                .first()
                .map(|e| format!(", first: {e}"))
                .unwrap_or_default()
        ));
    }
    state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", av(team))
        .item("sk", av(common::review_stats::REPAIR_MARKER_SK))
        .item("copied", AttributeValue::N(copies.to_string()))
        .item("stamped", AttributeValue::N(stamps.to_string()))
        .item("at", av(&Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| format!("write repair marker: {e}"))?;
    Ok(())
}

#[derive(serde::Deserialize)]
pub struct AnalyticsQuery {
    days: Option<i64>,
    /// `mtd`: this month so far against the same days of last month.
    period: Option<String>,
    repo: Option<String>,
}

/// GET /api/analytics?days=30&repo=owner/name
/// GET /api/analytics?period=mtd&repo=owner/name
pub async fn get_analytics(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let month_to_date = match q.period.as_deref() {
        None | Some("") | Some("days") => false,
        Some("mtd") => true,
        Some(_) => return Err(StatusCode::BAD_REQUEST),
    };
    let days = match q.days.unwrap_or(30) {
        d @ (7 | 30 | 90) => d,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let repo = q.repo.as_deref().map(str::trim).filter(|r| !r.is_empty());
    if let Some(r) = repo {
        if r.len() > 200 || !r.contains('/') || r.chars().any(|c| c.is_control()) {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    let team = claims.team_id.clone();
    let (window, prev, previous_full) = if month_to_date {
        let (w, p, full) = Window::month_to_date(Utc::now());
        (w, p, Some(full))
    } else {
        let (w, p) = Window::last_days(Utc::now(), days);
        (w, p, None)
    };
    let whole = Window {
        start: prev.start,
        end: window.end,
    };

    // A support view only reads: the repair waits for the team's own visit.
    if claims.support_view.is_some() {
    } else if let Err(e) = repair_review_stats(&state, &team).await {
        // The page still works; some older reviews just aren't counted yet.
        warn!(error = %e, "Review stats repair did not finish");
    }

    let lo = |prefix: &str| {
        format!(
            "{prefix}{}",
            whole.start.to_rfc3339_opts(SecondsFormat::Millis, true)
        )
    };
    let hi = |prefix: &str| {
        format!(
            "{prefix}{}~",
            whole.end.to_rfc3339_opts(SecondsFormat::Millis, true)
        )
    };
    let (stat_lo, stat_hi) = (
        lo(common::review_stats::PREFIX),
        hi(common::review_stats::PREFIX),
    );
    let (log_lo, log_hi) = (lo("DSLOG#"), hi("DSLOG#"));
    let (runs, reviews, lookups, traces, known) = tokio::join!(
        load_runs(&state, &team, whole),
        load_settings_range(&state, &team, &stat_lo, &stat_hi),
        load_settings_range(&state, &team, &log_lo, &log_hi),
        load_traces(&state, &team),
        load_known(&state, &team),
    );
    let runs = runs.map_err(|e| {
        error!(error = %e, "Analytics: runs");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let reviews: Vec<ReviewRow> = reviews
        .map_err(|e| {
            error!(error = %e, "Analytics: reviews");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .iter()
        .map(ReviewRow::from_item)
        .collect();
    let lookups: Vec<LookupRow> = lookups
        .unwrap_or_default()
        .iter()
        .map(LookupRow::from_item)
        .collect();

    let mut out = build(
        window,
        prev,
        previous_full,
        &runs,
        &reviews,
        &lookups,
        &traces,
        &known,
        repo,
    );
    out["days"] = json!(if month_to_date {
        window.day_slots()
    } else {
        days
    });
    out["basis"] = json!(if month_to_date {
        "month_to_date"
    } else {
        "days"
    });
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(days_ago: i64) -> String {
        (Utc::now() - Duration::days(days_ago)).to_rfc3339()
    }

    fn run(id: &str, days_ago: i64, status: &str) -> RunRow {
        RunRow {
            run_id: id.into(),
            created_at: at(days_ago),
            status: status.into(),
            repo: "acme/web".into(),
            source: "jira".into(),
            sender: "a-person".into(),
            title: format!("ticket {id}"),
            cost_usd: 2.0,
            duration_s: 600.0,
            ..Default::default()
        }
    }

    fn windows() -> (Window, Window) {
        Window::last_days(Utc::now(), 30)
    }

    #[test]
    fn a_runs_tokens_are_all_four_parts() {
        let mut r = run("1", 1, "completed");
        r.tokens_in = 10.0;
        r.cache_read_tokens = 1000.0;
        r.cache_write_tokens = 200.0;
        r.tokens_out = 50.0;
        assert_eq!(r.tokens(), 1260.0);
    }

    #[test]
    fn a_closed_run_is_counted_as_closed_by_a_person_not_failed() {
        let closed = run("1", 3, "closed");
        let failed = RunRow {
            error_message: "Timed out after 900s".into(),
            ..run("2", 4, "failed")
        };
        let done = run("3", 5, "completed");
        let (w, p) = windows();
        let out = build(
            w,
            p,
            None,
            &[closed, failed, done],
            &[],
            &[],
            &[],
            &[],
            None,
        );
        let sm = &out["summary"];
        assert_eq!(sm["runs"], 3);
        assert_eq!(sm["failed_runs"], 1);
        assert_eq!(sm["closed_runs"], 1);
        assert!((sm["failure_rate"].as_f64().unwrap() - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn merge_rate_is_merged_out_of_opened_and_never_counts_unknown_as_merged() {
        let mut merged = run("1", 3, "merged");
        merged.pr_number = 10;
        merged.pr_opened_at = at(3);
        merged.merged_at = at(2);
        merged.human_commits = Some(0);
        let mut closed = run("2", 4, "completed");
        closed.pr_number = 11;
        closed.pr_outcome = "closed".into();
        let mut open = run("3", 5, "completed");
        open.pr_number = 12;
        let failed = RunRow {
            error_message: "Timed out after 900s".into(),
            ..run("4", 6, "failed")
        };
        let (w, p) = windows();
        let out = build(
            w,
            p,
            None,
            &[merged, closed, open, failed],
            &[],
            &[],
            &[],
            &[],
            None,
        );
        let sm = &out["summary"];
        assert_eq!(sm["runs"], 4);
        assert_eq!(sm["prs_opened"], 3);
        assert_eq!(sm["prs_merged"], 1);
        assert_eq!(sm["prs_closed"], 1);
        assert_eq!(sm["prs_open"], 1);
        assert!((sm["merge_rate"].as_f64().unwrap() - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(sm["failed_runs"], 1);
        assert_eq!(sm["cost_per_merged_pr"], 8.0);
        assert_eq!(sm["cost_per_run"], 2.0);
        assert!(sm["tokens"].as_f64().is_some());
        assert!(sm.get("tokens_per_merged_pr").is_some());
        assert_eq!(sm["median_hours_to_merge"], 24.0);
        assert_eq!(sm["merged_untouched"], 1);
        assert_eq!(sm["edits_known"], 1);
        assert_eq!(out["failures"][0]["reason"], "Timed out after #s");
    }

    #[test]
    fn the_previous_period_is_separate_and_repo_filter_applies() {
        let cur = run("1", 3, "completed");
        let old = run("2", 40, "completed");
        let other = RunRow {
            repo: "acme/api".into(),
            ..run("3", 3, "completed")
        };
        let (w, p) = windows();
        let out = build(
            w,
            p,
            None,
            &[cur.clone(), old.clone(), other.clone()],
            &[],
            &[],
            &[],
            &[],
            None,
        );
        assert_eq!(out["summary"]["runs"], 2);
        assert_eq!(out["previous"]["runs"], 1);
        let only = build(
            w,
            p,
            None,
            &[cur, old, other],
            &[],
            &[],
            &[],
            &[],
            Some("ACME/web"),
        );
        assert_eq!(only["summary"]["runs"], 1);
        assert_eq!(only["repo_names"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn ci_self_fixes_count_runs_that_went_green_after_a_fix() {
        let fixed = RunRow {
            ci_fix_attempts: 2,
            ci_fix_commits: 1,
            ci_last: "passed".into(),
            ..run("1", 2, "completed")
        };
        let stuck = RunRow {
            ci_fix_attempts: 1,
            ci_last: "failed".into(),
            ..run("2", 2, "completed")
        };
        let clean = run("3", 2, "completed");
        let (w, p) = windows();
        let out = build(w, p, None, &[fixed, stuck, clean], &[], &[], &[], &[], None);
        assert_eq!(out["ci"]["runs_with_red_ci"], 2);
        assert_eq!(out["ci"]["fixed_by_coderhelm"], 1);
        assert_eq!(out["ci"]["fix_rate"], 0.5);
        assert_eq!(out["ci"]["fix_attempts"], 3);
    }

    #[test]
    fn review_outcomes_only_count_where_known() {
        let r = |verdict: &str, fixed: Option<u64>, settled: Option<u64>| ReviewRow {
            created_at: at(1),
            repo: "acme/web".into(),
            pr: 5,
            verdict: verdict.into(),
            posted_as: String::new(),
            findings: 2,
            prior_fixed: fixed,
            prior_settled: settled,
            posted: true,
            thumbs_up: 1,
            thumbs_down: 0,
        };
        let reviews = vec![
            r("REQUEST_CHANGES", None, None),
            r("APPROVE", Some(3), Some(1)),
            r("QUESTION", None, None),
            ReviewRow {
                posted: false,
                ..r("COMMENT", Some(9), Some(9))
            },
        ];
        let (w, p) = windows();
        let out = build(w, p, None, &[], &reviews, &[], &[], &[], None);
        let rv = &out["reviews"];
        assert_eq!(rv["posted"], 2);
        assert_eq!(rv["questions"], 1);
        assert_eq!(rv["findings"], 4);
        assert_eq!(rv["findings_fixed"], 3);
        assert_eq!(rv["acted_on_rate"], 0.75);
        assert_eq!(rv["outcomes_known_reviews"], 1);
        assert_eq!(rv["prs_reviewed"], 1);
        assert_eq!(out["summary"]["reviews"], 2);
    }

    #[test]
    fn lookups_group_by_source_and_agent() {
        let l = |src: &str, actor: &str, outcome: &str| LookupRow {
            at: at(1),
            source_name: src.into(),
            actor: actor.into(),
            outcome: outcome.into(),
            repo: String::new(),
        };
        let (w, p) = windows();
        let out = build(
            w,
            p,
            None,
            &[],
            &[],
            &[
                l("db", "planner", "ok"),
                l("db", "a@example.com", "refused"),
                l("logs", "coder", "failed"),
            ],
            &[],
            &[],
            None,
        );
        let lk = &out["lookups"];
        assert_eq!(lk["total"], 3);
        assert_eq!(lk["refused"], 1);
        assert_eq!(lk["by_source"][0]["source"], "db");
        assert_eq!(lk["by_source"][0]["total"], 2);
        let actors: Vec<&str> = lk["by_actor"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["actor"].as_str().unwrap())
            .collect();
        assert_eq!(actors, vec!["coder", "people", "planner"]);
    }

    fn old_failed(id: &str) -> RunRow {
        RunRow {
            created_at: "2026-10-12T13:49:26+00:00".into(),
            ..run(id, 1, "failed")
        }
    }

    #[test]
    fn older_failures_get_a_reason_derived_from_what_they_recorded() {
        // The sweep's old message, with the pass found in the run's history.
        let r = RunRow {
            error_message: "Run timed out during unknown pass (auto-cleanup)".into(),
            last_pass: "implement".into(),
            ..old_failed("1")
        };
        assert_eq!(
            run_failure_reason(&r, None),
            "Run timed out during implement pass (auto-cleanup)"
        );
        // ...or in its traces.
        let r = RunRow {
            last_pass: String::new(),
            ..r
        };
        assert_eq!(
            run_failure_reason(&r, Some("ci_fix:2")),
            "Run timed out during ci_fix pass (auto-cleanup)"
        );
        // ...or nowhere: it never started a pass.
        assert_eq!(
            run_failure_reason(&r, None),
            "Run stopped before its first pass started (auto-cleanup)"
        );
        // A cancel overwritten by a late write.
        let r = RunRow {
            current_pass: "cancelled".into(),
            error_legacy: "service error".into(),
            ..old_failed("2")
        };
        assert_eq!(
            run_failure_reason(&r, None),
            "Cancelled, then marked failed by a late write"
        );
        // The field older writes used, with the pass it failed in.
        let r = RunRow {
            error_legacy: "Model call failed".into(),
            last_pass: "feedback".into(),
            ..old_failed("3")
        };
        assert_eq!(
            run_failure_reason(&r, None),
            "Model call failed (feedback pass)"
        );
        // Nothing at all on an old run.
        assert_eq!(
            run_failure_reason(&old_failed("4"), None),
            format!(
                "Not recorded (before {})",
                common::run_fail::REASONS_RECORDED_SINCE
            )
        );
        // A recorded reason is used as is.
        let r = RunRow {
            error_message: "Feedback pass failed: boom".into(),
            failed_pass: "feedback".into(),
            ..old_failed("5")
        };
        assert_eq!(run_failure_reason(&r, None), "Feedback pass failed: boom");
    }

    #[test]
    fn ci_outcomes_place_every_red_run_and_keep_base_failures_apart() {
        let red = |id: &str, status: &str| RunRow {
            ci_fix_attempts: 1,
            ci_last: "failed".into(),
            ..run(id, 2, status)
        };
        let runs = [
            // Fixed by a Coderhelm push.
            RunRow {
                ci_fix_commits: 1,
                ci_last: "passed".into(),
                ..red("1", "completed")
            },
            // Green after a person's push, no Coderhelm fix.
            RunRow {
                ci_last: "passed".into(),
                ..red("2", "completed")
            },
            // Re-run turned it green.
            RunRow {
                ci_fix_attempts: 0,
                ci_reruns: 1,
                ci_flaky: true,
                ci_last: "passed".into(),
                ..run("3", 2, "completed")
            },
            // Red only on a check that fails on the base branch too.
            RunRow {
                ci_red_on_base: vec!["Deploy Preview".into()],
                ci_end: "red_on_base".into(),
                ci_last: "failed".into(),
                ..run("4", 2, "completed")
            },
            // Gave up: nothing to change.
            RunRow {
                ci_end: "no_change".into(),
                ..red("5", "completed")
            },
            // Older run that used every attempt, no reason stored.
            RunRow {
                ci_fix_attempts: 10,
                ci_fix_commits: 8,
                ..red("6", "completed")
            },
            // Still fixing.
            red("7", "running"),
            // Never red: not counted.
            run("8", 2, "completed"),
        ];
        let refs: Vec<&RunRow> = runs.iter().collect();
        let ci = ci_outcomes(&refs);
        assert_eq!(ci["runs_with_red_ci"], 6);
        assert_eq!(ci["red_on_base_runs"], 1);
        let o = &ci["outcomes"];
        assert_eq!(o["green_after_fix"], 1);
        assert_eq!(o["green_without_fix"], 1);
        assert_eq!(o["flaky"], 1);
        assert_eq!(o["red_on_base"], 1);
        assert_eq!(o["gave_up"], 2);
        assert_eq!(o["running"], 1);
        assert_eq!(ci["fixed_by_coderhelm"], 1);
        // Settled own red runs: 6 minus the one still running.
        assert!((ci["fix_rate"].as_f64().unwrap() - 0.2).abs() < 1e-9);
        assert_eq!(ci["fix_attempts"], 14);
        let reasons: Vec<&str> = ci["gave_up_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["reason"].as_str().unwrap())
            .collect();
        assert!(reasons.contains(&"Fix found nothing to change"));
        assert!(reasons.contains(&"Fix attempts used up"));
    }

    #[test]
    fn a_trace_key_names_its_run() {
        let item: Item = [
            ("sk".to_string(), av("RUN#01ABC#PASS#ci_fix:2")),
            ("pass".to_string(), av("ci_fix:2")),
        ]
        .into();
        assert_eq!(TraceRow::from_item(&item).run_id, "01ABC");
    }

    #[test]
    fn daily_series_is_zero_filled_and_steps_group_cycles() {
        let (w, p) = windows();
        let traces = vec![
            TraceRow {
                run_id: "1".into(),
                pass: "implement".into(),
                timestamp: at(1),
                input_tokens: 300.0,
                output_tokens: 10.0,
                duration_ms: 2000.0,
            },
            TraceRow {
                run_id: "1".into(),
                pass: "ci_fix:1".into(),
                timestamp: at(1),
                input_tokens: 50.0,
                output_tokens: 5.0,
                duration_ms: 1000.0,
            },
            TraceRow {
                run_id: "1".into(),
                pass: "ci_fix:2".into(),
                timestamp: at(1),
                input_tokens: 50.0,
                output_tokens: 5.0,
                duration_ms: 3000.0,
            },
        ];
        let out = build(
            w,
            p,
            None,
            &[run("1", 2, "completed")],
            &[],
            &[],
            &traces,
            &[],
            None,
        );
        assert_eq!(out["daily"].as_array().unwrap().len(), 30);
        let steps = out["steps"].as_array().unwrap();
        assert_eq!(steps[0]["step"], "implement");
        assert_eq!(steps[1]["step"], "ci_fix");
        assert_eq!(steps[1]["count"], 2);
        assert_eq!(steps[1]["avg_seconds"], 2.0);
    }

    #[test]
    fn people_are_one_row_each_and_coderhelm_work_is_labelled() {
        let by = |id: &str, sender: &str| RunRow {
            sender: sender.into(),
            ..run(id, 2, "completed")
        };
        let mut merged = by("1", "ada@example.com");
        merged.status = "merged".into();
        let recorded = RunRow {
            sender: "Ada L.".into(),
            requester_key: "jira:557058:0a1b2c3d-4e5f-6789-abcd-ef0123456789".into(),
            requester_name: "Ada L.".into(),
            ..run("2", 2, "completed")
        };
        let runs = vec![
            merged,
            by("3", "Ada Lovelace"),
            by("4", "557058:0a1b2c3d-4e5f-6789-abcd-ef0123456789"),
            recorded,
            by("5", "e2e"),
            by("6", ""),
        ];
        // A member, and the same person as a Jira user seen on a webhook.
        let known = vec![
            Known {
                name: "Ada Lovelace".into(),
                email: "ada@example.com".into(),
                member: true,
                ..Default::default()
            },
            Known {
                name: "Ada L.".into(),
                email: "ada@example.com".into(),
                jira_id: "557058:0a1b2c3d-4e5f-6789-abcd-ef0123456789".into(),
                ..Default::default()
            },
        ];
        let (w, p) = windows();
        let out = build(w, p, None, &runs, &[], &[], &[], &known, None);
        assert_eq!(
            out["people"],
            json!([
                {"person": "Ada Lovelace", "kind": "person", "runs": 4.0, "merged": 1.0},
                {"person": "", "kind": "not_recorded", "runs": 1.0, "merged": 0.0},
                {"person": "CoderHelm (E2E tests)", "kind": "coderhelm", "runs": 1.0, "merged": 0.0},
            ])
        );
    }

    #[test]
    fn failure_reasons_group_similar_messages() {
        assert_eq!(
            failure_reason("Timed out after 900s"),
            failure_reason("Timed out after 1200s")
        );
        assert_eq!(
            failure_reason("Branch `coderhelm/x-1` already exists"),
            "Branch … already exists"
        );
        assert_eq!(failure_reason(""), "No reason recorded");
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn run_at(id: &str, created_at: &str, ticket: &str, repo: &str, pr: u64) -> RunRow {
        RunRow {
            run_id: id.into(),
            ticket_id: ticket.into(),
            created_at: created_at.into(),
            status: "completed".into(),
            repo: repo.into(),
            pr_number: pr,
            ..Default::default()
        }
    }

    fn review_at(
        created_at: &str,
        repo: &str,
        pr: u64,
        verdict: &str,
        posted_as: &str,
    ) -> ReviewRow {
        ReviewRow {
            created_at: created_at.into(),
            repo: repo.into(),
            pr,
            verdict: verdict.into(),
            posted_as: posted_as.into(),
            posted: true,
            ..Default::default()
        }
    }

    #[test]
    fn month_to_date_compares_the_same_time_elapsed_last_month() {
        let (cur, prev, full) = Window::month_to_date(ts("2026-10-22T15:00:00Z"));
        assert_eq!(cur.start, ts("2026-10-01T00:00:00Z"));
        assert_eq!(cur.end, ts("2026-10-22T15:00:00Z"));
        assert_eq!(prev.start, ts("2026-09-01T00:00:00Z"));
        assert_eq!(prev.end, ts("2026-09-22T15:00:00Z"));
        assert_eq!(full.start, ts("2026-09-01T00:00:00Z"));
        assert_eq!(full.end, ts("2026-10-01T00:00:00Z"));
        assert_eq!(cur.day_slots(), 22);
        // A longer month against a shorter one stops at the end of the shorter.
        let (_, prev, full) = Window::month_to_date(ts("2026-03-31T12:00:00Z"));
        assert_eq!(prev.start, ts("2026-02-01T00:00:00Z"));
        assert_eq!(prev.end, ts("2026-03-01T00:00:00Z"));
        assert_eq!(full.end, prev.end);
        // January looks back at December of the year before.
        let (cur, prev, _) = Window::month_to_date(ts("2027-01-05T00:00:00Z"));
        assert_eq!(cur.start, ts("2027-01-01T00:00:00Z"));
        assert_eq!(prev.start, ts("2026-12-01T00:00:00Z"));
        assert_eq!(prev.end, ts("2026-12-05T00:00:00Z"));
    }

    #[test]
    fn comparison_puts_each_record_in_exactly_one_period() {
        let (cur, prev, full) = Window::month_to_date(ts("2026-10-22T15:00:00Z"));
        let runs = [
            // Previous stretch: its first instant counts, its end does not.
            run_at("1", "2026-09-01T00:00:00Z", "T-1", "acme/web", 0),
            run_at("2", "2026-09-22T14:59:59Z", "T-2", "acme/web", 0),
            // After the same time of day: all of September only.
            run_at("3", "2026-09-22T15:00:00Z", "T-3", "acme/web", 0),
            run_at("4", "2026-09-30T23:59:59Z", "T-4", "acme/web", 0),
            // This month: from the first instant, up to now.
            run_at("5", "2026-10-01T00:00:00Z", "T-5", "acme/web", 0),
            run_at("6", "2026-10-22T14:59:59Z", "T-6", "acme/web", 0),
            run_at("7", "2026-10-22T15:00:00Z", "T-7", "acme/web", 0),
            run_at("8", "2026-08-31T23:59:59Z", "T-8", "acme/web", 0),
        ];
        let refs: Vec<&RunRow> = runs.iter().collect();
        let out = comparison(cur, prev, Some(full), &refs, &[], &[]);
        assert_eq!(out["previous"]["runs"], 2);
        assert_eq!(out["previous_full"]["runs"], 4);
        assert_eq!(out["current"]["runs"], 2);
        assert_eq!(out["previous_full_period"]["end"], full.end.to_rfc3339());
        // Rolling windows meet without a gap or an overlap.
        let (w, p) = Window::last_days(ts("2026-10-22T15:00:00Z"), 7);
        let edge = run_at("9", &w.start.to_rfc3339(), "T-9", "acme/web", 0);
        let out = comparison(w, p, None, &[&edge], &[], &[]);
        assert_eq!(out["current"]["runs"], 1);
        assert_eq!(out["previous"]["runs"], 0);
        assert_eq!(out["previous_full"], Value::Null);
    }

    #[test]
    fn tickets_prs_and_reviewed_prs_are_counted_once_each() {
        let runs = [
            run_at("1", "2026-10-02T10:00:00Z", "T-1", "acme/web", 7),
            // A rerun on the same ticket pushes to the same PR (repo case differs).
            run_at("2", "2026-10-03T10:00:00Z", "T-1", "Acme/Web", 7),
            RunRow {
                status: "merged".into(),
                ..run_at("3", "2026-10-04T10:00:00Z", "T-2", "acme/api", 8)
            },
            // No ticket recorded: the run is its own.
            run_at("4", "2026-10-05T10:00:00Z", "", "acme/api", 0),
            run_at("5", "2026-10-05T11:00:00Z", "", "acme/api", 0),
        ];
        let reviews = [
            review_at(
                "2026-10-02T12:00:00Z",
                "acme/web",
                40,
                "REQUEST_CHANGES",
                "REQUEST_CHANGES",
            ),
            review_at("2026-10-03T12:00:00Z", "acme/web", 40, "APPROVE", "APPROVE"),
            review_at("2026-10-04T12:00:00Z", "ACME/web", 40, "APPROVE", "COMMENT"),
            // Older record without posted_as: its verdict is what was posted.
            review_at(
                "2026-10-04T13:00:00Z",
                "acme/docs",
                3,
                "REQUEST_CHANGES",
                "",
            ),
            // Not reviews: an answer to a question, and a silent re-check.
            review_at(
                "2026-10-04T14:00:00Z",
                "acme/web",
                41,
                "QUESTION",
                "COMMENT",
            ),
            ReviewRow {
                posted: false,
                ..review_at("2026-10-04T15:00:00Z", "acme/web", 42, "APPROVE", "APPROVE")
            },
        ];
        let rr: Vec<&RunRow> = runs.iter().collect();
        let rv: Vec<&ReviewRow> = reviews.iter().collect();
        let c = counts(&rr, &rv, &[]);
        assert_eq!(c.runs, 5);
        assert_eq!(c.tickets, 4);
        assert_eq!(c.prs_opened, 2);
        assert_eq!(c.prs_merged, 1);
        assert_eq!(c.prs_open, 1);
        assert_eq!(c.reviews_posted, 4);
        assert_eq!(c.prs_reviewed, 2);
        assert_eq!((c.approvals, c.changes_requested, c.comments), (1, 2, 1));
        assert_eq!(c.repos, 3);
        // The headline PR counts agree with the comparison.
        let sm = summary(&rr, &rv);
        assert_eq!(sm["prs_opened"], 2);
        assert_eq!(sm["prs_merged"], 1);
        assert_eq!(sm["reviews"], 4);
    }

    #[test]
    fn a_rerun_on_a_merged_pr_is_one_merged_pr_everywhere() {
        let (w, p) = windows();
        let mut first = run("1", 5, "completed");
        first.pr_number = 9;
        let mut second = run("2", 3, "merged");
        second.pr_number = 9;
        second.merged_at = at(2);
        let out = build(w, p, None, &[first, second], &[], &[], &[], &[], None);
        assert_eq!(out["summary"]["prs_opened"], 1);
        assert_eq!(out["summary"]["prs_merged"], 1);
        assert_eq!(out["summary"]["prs_open"], 0);
        assert_eq!(out["repos"][0]["prs"], 1.0);
        assert_eq!(out["repos"][0]["merged"], 1.0);
        let daily = out["daily"].as_array().unwrap();
        let sum = |k: &str| daily.iter().map(|d| d[k].as_f64().unwrap()).sum::<f64>();
        assert_eq!(sum("prs_opened"), 1.0);
        assert_eq!(sum("merged"), 1.0);
    }

    #[test]
    fn running_totals_line_up_by_day_and_stop_where_last_month_did() {
        let (cur, prev, full) = Window::month_to_date(ts("2026-03-31T12:00:00Z"));
        let runs = [
            run_at("1", "2026-03-01T09:00:00Z", "T-1", "acme/web", 1),
            run_at("2", "2026-03-02T09:00:00Z", "T-1", "acme/web", 1),
            run_at("3", "2026-03-02T10:00:00Z", "T-2", "acme/web", 2),
            run_at("4", "2026-02-01T09:00:00Z", "T-9", "acme/web", 0),
        ];
        let reviews = [
            review_at("2026-03-01T12:00:00Z", "acme/web", 50, "APPROVE", "APPROVE"),
            review_at("2026-03-03T12:00:00Z", "acme/web", 50, "APPROVE", "APPROVE"),
        ];
        let rr: Vec<&RunRow> = runs.iter().collect();
        let rv: Vec<&ReviewRow> = reviews.iter().collect();
        let out = comparison(cur, prev, Some(full), &rr, &rv, &[]);
        let d = out["daily"].as_array().unwrap();
        assert_eq!(d.len(), 31);
        assert_eq!(d[0]["day"], "2026-03-01");
        assert_eq!(d[0]["previous_day"], "2026-02-01");
        assert_eq!(
            (
                d[0]["tickets"].as_u64(),
                d[0]["prs_opened"].as_u64(),
                d[0]["prs_reviewed"].as_u64()
            ),
            (Some(1), Some(1), Some(1))
        );
        assert_eq!(
            (d[1]["tickets"].as_u64(), d[1]["prs_opened"].as_u64()),
            (Some(2), Some(2))
        );
        assert_eq!(d[2]["prs_reviewed"], 1);
        assert_eq!(d[30]["tickets"], 2);
        assert_eq!(d[0]["previous_tickets"], 1);
        assert_eq!(d[27]["previous_tickets"], 1);
        // February has 28 days: nothing to compare after them.
        assert_eq!(d[28]["previous_tickets"], Value::Null);
        assert_eq!(d[28]["previous_day"], Value::Null);
    }

    #[test]
    fn who_asked_is_not_compared_when_a_period_did_not_record_it() {
        let (cur, prev, _) = Window::month_to_date(ts("2026-10-22T15:00:00Z"));
        let asked = |id: &str, at: &str, who: &str| RunRow {
            sender: who.into(),
            ..run_at(id, at, id, "acme/web", 0)
        };
        let runs = [
            run_at("1", "2026-09-03T10:00:00Z", "T-1", "acme/web", 0),
            run_at("2", "2026-09-04T10:00:00Z", "T-2", "acme/web", 0),
            asked("3", "2026-10-03T10:00:00Z", "ada@example.com"),
            asked("4", "2026-10-04T10:00:00Z", "grace@example.com"),
        ];
        let rr: Vec<&RunRow> = runs.iter().collect();
        let out = comparison(cur, prev, None, &rr, &[], &[]);
        assert_eq!(out["current"]["people"], 2);
        assert_eq!(out["previous"]["people"], 0);
        assert_eq!(out["previous"]["runs_with_requester"], 0);
        assert_eq!(out["people_comparable"], false);
        let runs = [
            asked("1", "2026-09-03T10:00:00Z", "ada@example.com"),
            asked("3", "2026-10-03T10:00:00Z", "ada@example.com"),
        ];
        let rr: Vec<&RunRow> = runs.iter().collect();
        let out = comparison(cur, prev, None, &rr, &[], &[]);
        assert_eq!(out["people_comparable"], true);
    }

    #[test]
    fn repair_copies_missing_reviews_and_stamps_what_was_posted() {
        let review = |created_at: &str, pr: u64, verdict: &str, posted_as: &str| -> Item {
            HashMap::from([
                ("created_at".to_string(), av(created_at)),
                ("repo".to_string(), av("acme/web")),
                ("pr_number".to_string(), AttributeValue::N(pr.to_string())),
                ("verdict".to_string(), av(verdict)),
                ("posted_as".to_string(), av(posted_as)),
                ("body".to_string(), av("Summary.\n\n- [high] a.rs:1 — x")),
                ("thumbs_up".to_string(), AttributeValue::N("2".into())),
            ])
        };
        let a = "2026-10-01T10:00:00Z";
        let b = "2026-10-02T10:00:00Z";
        let c = "2026-10-03T10:00:00Z";
        let stats = HashMap::from([
            (common::review_stats::stat_sk(b, "acme/web", 2), false),
            (common::review_stats::stat_sk(c, "acme/web", 3), true),
        ]);
        let fixes = stat_fixes(
            "TEAM#t",
            &[
                review(a, 1, "APPROVE", "COMMENT"),
                review(b, 2, "REQUEST_CHANGES", "REQUEST_CHANGES"),
                review(c, 3, "APPROVE", "APPROVE"),
            ],
            &stats,
        );
        assert_eq!(fixes.len(), 2);
        let StatFix::Copy(item) = &fixes[0] else {
            panic!("expected a copy, got {:?}", fixes[0]);
        };
        let row = ReviewRow::from_item(item);
        assert_eq!(row.posted_as, "COMMENT");
        assert_eq!(row.verdict, "APPROVE");
        assert_eq!(row.findings, 1);
        assert_eq!(row.thumbs_up, 2);
        assert!(row.posted);
        assert_eq!(
            s(item, "sk"),
            common::review_stats::stat_sk(a, "acme/web", 1)
        );
        assert_eq!(
            fixes[1],
            StatFix::PostedAs {
                sk: common::review_stats::stat_sk(b, "acme/web", 2),
                posted_as: "REQUEST_CHANGES".into()
            }
        );
    }

    #[test]
    fn run_id_bounds_cover_the_window() {
        let (w, _) = windows();
        let (lo, hi) = run_id_bounds(w);
        let inside = ulid::Ulid::from_parts(
            (Utc::now() - Duration::days(1)).timestamp_millis() as u64,
            42,
        )
        .to_string();
        assert!(lo < inside && inside < hi);
    }
}
