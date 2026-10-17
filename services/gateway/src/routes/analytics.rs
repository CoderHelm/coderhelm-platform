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
use chrono::{DateTime, Duration, SecondsFormat, Utc};
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

// ── Rows ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunRow {
    pub run_id: String,
    pub created_at: String,
    pub status: String,
    pub repo: String,
    pub source: String,
    pub sender: String,
    pub title: String,
    pub cost_usd: f64,
    pub tokens_in: f64,
    pub tokens_out: f64,
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
}

impl RunRow {
    fn from_item(i: &Item) -> RunRow {
        RunRow {
            run_id: s(i, "run_id"),
            created_at: s(i, "created_at"),
            status: s(i, "status"),
            repo: s(i, "repo"),
            source: s(i, "ticket_source"),
            sender: s(i, "sender"),
            title: s(i, "title"),
            cost_usd: n(i, "cost_usd"),
            tokens_in: n(i, "tokens_in"),
            tokens_out: n(i, "tokens_out"),
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
        }
    }
    fn merged(&self) -> bool {
        self.status == "merged" || self.pr_outcome == "merged"
    }
    fn has_pr(&self) -> bool {
        self.pr_number > 0
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
            findings: n(i, "findings") as u64,
            prior_fixed: opt_n(i, "prior_fixed").map(|v| v as u64),
            prior_settled: opt_n(i, "prior_settled_by_person").map(|v| v as u64),
            posted: b(i, "posted").unwrap_or(true),
            thumbs_up: n(i, "thumbs_up") as u64,
            thumbs_down: n(i, "thumbs_down") as u64,
        }
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
    pub pass: String,
    pub timestamp: String,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub duration_ms: f64,
}

impl TraceRow {
    fn from_item(i: &Item) -> TraceRow {
        TraceRow {
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
    fn contains(&self, ts: &str) -> bool {
        DateTime::parse_from_rfc3339(ts)
            .map(|t| {
                let t = t.with_timezone(&Utc);
                t >= self.start && t < self.end
            })
            .unwrap_or(false)
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

/// The headline numbers for one period.
pub fn summary(runs: &[&RunRow], reviews: &[&ReviewRow]) -> Value {
    let total = runs.len() as f64;
    let with_pr: Vec<&&RunRow> = runs.iter().filter(|r| r.has_pr()).collect();
    let merged: Vec<&&RunRow> = with_pr.iter().copied().filter(|r| r.merged()).collect();
    let closed = with_pr.iter().filter(|r| r.pr_outcome == "closed").count();
    let failed = runs.iter().filter(|r| r.status == "failed").count();
    let cost: f64 = runs.iter().map(|r| r.cost_usd).sum();
    let tokens: f64 = runs.iter().map(|r| r.tokens_in + r.tokens_out).sum();
    let hours: Vec<f64> = merged.iter().filter_map(|r| r.hours_to_merge()).collect();
    let edits_known: Vec<&&&RunRow> = merged
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
    let posted: Vec<&&ReviewRow> = reviews
        .iter()
        .filter(|r| r.posted && r.verdict != "QUESTION")
        .collect();
    json!({
        "runs": runs.len(),
        "prs_opened": with_pr.len(),
        "prs_merged": merged.len(),
        "prs_closed": closed,
        "prs_open": with_pr.len().saturating_sub(merged.len() + closed),
        "merge_rate": ratio(merged.len() as f64, with_pr.len() as f64),
        "failed_runs": failed,
        "failure_rate": ratio(failed as f64, total),
        "cost_usd": round2(cost),
        "cost_per_run": ratio(cost, total).map(round2),
        "cost_per_merged_pr": ratio(cost, merged.len() as f64).map(round2),
        "tokens": tokens,
        "tokens_per_run": ratio(tokens, total).map(f64::round),
        "tokens_per_merged_pr": ratio(tokens, merged.len() as f64).map(f64::round),
        "tokens_in": runs.iter().map(|r| r.tokens_in).sum::<f64>(),
        "tokens_out": runs.iter().map(|r| r.tokens_out).sum::<f64>(),
        "median_run_minutes": median(durations).map(round2),
        "median_hours_to_merge": median(hours.clone()).map(round2),
        "time_to_merge_known": hours.len(),
        "merged_untouched": untouched,
        "merged_with_edits": edits_known.len() - untouched,
        "edits_known": edits_known.len(),
        "reviews": posted.len(),
    })
}

fn day(ts: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&Utc).format("%Y-%m-%d").to_string())
}

/// Everything the page shows, for one period and the one before it.
pub fn build(
    window: Window,
    prev: Window,
    runs: &[RunRow],
    reviews: &[ReviewRow],
    lookups: &[LookupRow],
    traces: &[TraceRow],
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
    for r in &cur_runs {
        if let Some(e) = day(&r.created_at).and_then(|k| daily.get_mut(&k)) {
            e[0] += 1.0;
            if r.has_pr() {
                e[1] += 1.0;
            }
            if r.status == "failed" {
                e[3] += 1.0;
            }
            e[4] += r.cost_usd;
            e[6] += r.tokens_in + r.tokens_out;
        }
    }
    // Merges land on the day they happened, when known.
    for r in runs.iter().filter(|r| r.merged() && repo_ok(&r.repo)) {
        if let Some(e) = day(&r.merged_at)
            .filter(|_| window.contains(&r.merged_at))
            .and_then(|k| daily.get_mut(&k))
        {
            e[2] += 1.0;
        }
    }
    for r in &cur_reviews {
        if r.posted && r.verdict != "QUESTION" {
            if let Some(e) = day(&r.created_at).and_then(|k| daily.get_mut(&k)) {
                e[5] += 1.0;
            }
        }
    }
    let daily: Vec<Value> = daily
        .into_iter()
        .map(|(day, v)| json!({"day": day, "runs": v[0], "prs_opened": v[1], "merged": v[2], "failed": v[3], "cost_usd": round2(v[4]), "tokens": v[6], "reviews": v[5]}))
        .collect();

    // Breakdowns.
    let mut by_source: BTreeMap<String, [f64; 3]> = BTreeMap::new();
    let mut by_repo: BTreeMap<String, [f64; 6]> = BTreeMap::new();
    let mut by_person: BTreeMap<String, [f64; 2]> = BTreeMap::new();
    for r in &cur_runs {
        let src = if r.source.is_empty() {
            "other".to_string()
        } else {
            r.source.clone()
        };
        let e = by_source.entry(src).or_default();
        e[0] += 1.0;
        if r.has_pr() {
            e[1] += 1.0;
        }
        if r.merged() {
            e[2] += 1.0;
        }
        let e = by_repo.entry(r.repo.clone()).or_default();
        e[0] += 1.0;
        if r.has_pr() {
            e[1] += 1.0;
        }
        if r.merged() {
            e[2] += 1.0;
        }
        if r.status == "failed" {
            e[3] += 1.0;
        }
        e[4] += r.cost_usd;
        e[5] += r.tokens_in + r.tokens_out;
        let who = if r.sender.is_empty() {
            String::new()
        } else {
            r.sender.clone()
        };
        let e = by_person.entry(who).or_default();
        e[0] += 1.0;
        if r.merged() {
            e[1] += 1.0;
        }
    }
    let mut reviews_by_repo: HashMap<String, usize> = HashMap::new();
    for r in cur_reviews
        .iter()
        .filter(|r| r.posted && r.verdict != "QUESTION")
    {
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
    let mut people: Vec<Value> = by_person
        .iter()
        .map(|(who, v)| json!({"person": who, "runs": v[0], "merged": v[1]}))
        .collect();
    people.sort_by(|a, b| {
        b["runs"]
            .as_f64()
            .partial_cmp(&a["runs"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Failures, grouped by reason.
    let mut failures: HashMap<String, (usize, &RunRow)> = HashMap::new();
    for r in cur_runs.iter().filter(|r| r.status == "failed") {
        let e = failures
            .entry(failure_reason(&r.error_message))
            .or_insert((0, r));
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

    // CI self-fixes.
    let red: Vec<&&RunRow> = cur_runs.iter().filter(|r| r.ci_fix_attempts > 0).collect();
    let fixed = red.iter().filter(|r| r.ci_last == "passed").count();
    let ci = json!({
        "runs_with_red_ci": red.len(),
        "fixed_by_coderhelm": fixed,
        "fix_rate": ratio(fixed as f64, red.len() as f64),
        "fix_attempts": red.iter().map(|r| r.ci_fix_attempts).sum::<u64>(),
        "fix_commits": red.iter().map(|r| r.ci_fix_commits).sum::<u64>(),
    });

    // Reviews.
    let posted: Vec<&&ReviewRow> = cur_reviews
        .iter()
        .filter(|r| r.posted && r.verdict != "QUESTION")
        .collect();
    let verdict = |v: &str| posted.iter().filter(|r| r.verdict == v).count();
    let known: Vec<&&&ReviewRow> = posted.iter().filter(|r| r.prior_fixed.is_some()).collect();
    let acted: u64 = known.iter().filter_map(|r| r.prior_fixed).sum();
    let settled: u64 = known.iter().filter_map(|r| r.prior_settled).sum();
    let reviews_json = json!({
        "posted": posted.len(),
        "approve": verdict("APPROVE"),
        "request_changes": verdict("REQUEST_CHANGES"),
        "comment": verdict("COMMENT"),
        "questions": cur_reviews.iter().filter(|r| r.verdict == "QUESTION").count(),
        "prs_reviewed": posted.iter().map(|r| (r.repo.clone(), r.pr)).collect::<std::collections::HashSet<_>>().len(),
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
        .map(|r| json!({"run_id": r.run_id, "title": r.title, "repo": r.repo, "reason": failure_reason(&r.error_message), "at": r.created_at}))
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
                "run_id, created_at, #s, repo, ticket_source, sender, title, cost_usd, tokens_in, tokens_out, \
                 duration_s, pr_number, pr_url, error_message, ci_fix_attempts, ci_fix_commits, ci_last, \
                 pr_outcome, merged_at, pr_opened_at, human_commits",
            )
            .expression_attribute_names("#s", "status")
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
            .projection_expression("pass, #ts, input_tokens, output_tokens, duration_ms")
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

/// Copies reviews written before stats records existed into stats records,
/// once per team. Safe to repeat: each copy lands on the same key.
async fn backfill_review_stats(state: &AppState, team: &str) -> Result<(), String> {
    let table = &state.config.settings_table_name;
    let marker = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", av(team))
        .key("sk", av(common::review_stats::BACKFILL_MARKER_SK))
        .send()
        .await
        .map_err(|e| format!("read backfill marker: {e}"))?;
    if marker.item().is_some() {
        return Ok(());
    }
    let mut copied = 0usize;
    let mut start: Option<Item> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", av(team))
            .expression_attribute_values(":p", av("REVIEW#"))
            .projection_expression(
                "sk, repo, pr_number, verdict, risk, #tr, created_at, body, thumbs_up, thumbs_down",
            )
            .expression_attribute_names("#tr", "trigger")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("read reviews for backfill: {e}"))?;
        let items: Vec<Item> = resp
            .items()
            .iter()
            .filter_map(|i| {
                let created_at = s(i, "created_at");
                let repo = s(i, "repo");
                if created_at.is_empty() || repo.is_empty() {
                    return None;
                }
                let counts = common::review_stats::ReviewCounts {
                    findings: common::review_stats::count_findings(&s(i, "body")),
                    posted: true,
                    ..Default::default()
                };
                let mut item = common::review_stats::stat_item(
                    team,
                    &created_at,
                    &repo,
                    n(i, "pr_number") as u64,
                    &s(i, "verdict"),
                    &s(i, "risk"),
                    &s(i, "trigger"),
                    &counts,
                );
                item.insert(
                    "thumbs_up".into(),
                    AttributeValue::N((n(i, "thumbs_up") as u64).to_string()),
                );
                item.insert(
                    "thumbs_down".into(),
                    AttributeValue::N((n(i, "thumbs_down") as u64).to_string()),
                );
                Some(item)
            })
            .collect();
        for chunk in items.chunks(25) {
            let writes = chunk
                .iter()
                .filter_map(|item| {
                    let put = aws_sdk_dynamodb::types::PutRequest::builder()
                        .set_item(Some(item.clone()))
                        .build()
                        .ok()?;
                    Some(
                        aws_sdk_dynamodb::types::WriteRequest::builder()
                            .put_request(put)
                            .build(),
                    )
                })
                .collect::<Vec<_>>();
            state
                .dynamo
                .batch_write_item()
                .request_items(table, writes)
                .send()
                .await
                .map_err(|e| format!("write review stats: {e}"))?;
            copied += chunk.len();
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", av(team))
        .item("sk", av(common::review_stats::BACKFILL_MARKER_SK))
        .item("copied", AttributeValue::N(copied.to_string()))
        .item("at", av(&Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| format!("write backfill marker: {e}"))?;
    info!(team, copied, "Review stats backfilled");
    Ok(())
}

#[derive(serde::Deserialize)]
pub struct AnalyticsQuery {
    days: Option<i64>,
    repo: Option<String>,
}

/// GET /api/analytics?days=30&repo=owner/name
pub async fn get_analytics(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, StatusCode> {
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
    let (window, prev) = Window::last_days(Utc::now(), days);
    let whole = Window {
        start: prev.start,
        end: window.end,
    };

    if let Err(e) = backfill_review_stats(&state, &team).await {
        // The page still works; older reviews just aren't counted yet.
        warn!(error = %e, "Review stats backfill did not finish");
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
    let (runs, reviews, lookups, traces) = tokio::join!(
        load_runs(&state, &team, whole),
        load_settings_range(&state, &team, &stat_lo, &stat_hi),
        load_settings_range(&state, &team, &log_lo, &log_hi),
        load_traces(&state, &team),
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

    let mut out = build(window, prev, &runs, &reviews, &lookups, &traces, repo);
    out["days"] = json!(days);
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
        let out = build(w, p, &[merged, closed, open, failed], &[], &[], &[], None);
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
            &[cur.clone(), old.clone(), other.clone()],
            &[],
            &[],
            &[],
            None,
        );
        assert_eq!(out["summary"]["runs"], 2);
        assert_eq!(out["previous"]["runs"], 1);
        let only = build(w, p, &[cur, old, other], &[], &[], &[], Some("ACME/web"));
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
        let out = build(w, p, &[fixed, stuck, clean], &[], &[], &[], None);
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
        let out = build(w, p, &[], &reviews, &[], &[], None);
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
            &[],
            &[],
            &[
                l("db", "planner", "ok"),
                l("db", "a@example.com", "refused"),
                l("logs", "coder", "failed"),
            ],
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

    #[test]
    fn daily_series_is_zero_filled_and_steps_group_cycles() {
        let (w, p) = windows();
        let traces = vec![
            TraceRow {
                pass: "implement".into(),
                timestamp: at(1),
                input_tokens: 300.0,
                output_tokens: 10.0,
                duration_ms: 2000.0,
            },
            TraceRow {
                pass: "ci_fix:1".into(),
                timestamp: at(1),
                input_tokens: 50.0,
                output_tokens: 5.0,
                duration_ms: 1000.0,
            },
            TraceRow {
                pass: "ci_fix:2".into(),
                timestamp: at(1),
                input_tokens: 50.0,
                output_tokens: 5.0,
                duration_ms: 3000.0,
            },
        ];
        let out = build(w, p, &[run("1", 2, "completed")], &[], &[], &traces, None);
        assert_eq!(out["daily"].as_array().unwrap().len(), 30);
        let steps = out["steps"].as_array().unwrap();
        assert_eq!(steps[0]["step"], "implement");
        assert_eq!(steps[1]["step"], "ci_fix");
        assert_eq!(steps[1]["count"], 2);
        assert_eq!(steps[1]["avg_seconds"], 2.0);
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
