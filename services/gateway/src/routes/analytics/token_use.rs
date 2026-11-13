//! Token use, explained: what a period's tokens were, what kind of work and
//! which model used them, and what a merged pull request took.
//!
//! Two records feed it, and they do not measure the same thing:
//!
//! * the per-call meter (`common::usage`, the analytics table's `DAY#`
//!   items): every model call the team was billed for, on the day it was
//!   made, with counters per feature and per model. This is the period's
//!   total and the two breakdowns. It is not kept per repository or per run.
//! * the run records: what each run used, follow-up work on its pull request
//!   included. These say which runs and pull requests were heavy, and what a
//!   merged pull request took. Reviews of people's pull requests, chat and
//!   log analysis are not runs and are not in them.
//!
//! A number is compared with the period before only when both periods were
//! recorded the same way ([`Since`]): a jump that comes from how tokens were
//! counted is not a change in use.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::NaiveDate;
use common::usage::{work_kind, UsageSlice};
use serde_json::{json, Value};

use super::{day, ratio, round2, window_days, MeteredDay, RunRow, Window};

/// The first whole day (UTC) from which a run record holds everything the
/// run used. Until the release of the day before, a resume, a CI fix or a
/// continuation SET the record's tokens to its own, dropping what earlier
/// invocations of the same run had recorded; since then every invocation
/// ADDs. Records from before it can be short by any amount, so numbers built
/// on them are not compared with numbers built on later ones. An install
/// that upgraded later than this is covered by the day its per-call counting
/// started (the same upgrade brought both).
pub const RUN_RECORDS_COMPLETE_FROM: &str = "2026-10-28";

/// Rows in each "heaviest" list.
const HEAVIEST: usize = 10;

/// From when each record can be trusted to hold everything.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Since {
    /// The first day (`YYYY-MM-DD`, UTC) the team's per-call meter has
    /// anything; `None` when it has nothing.
    pub counted_since: Option<String>,
}

impl Since {
    /// The first whole day of per-call counting: the day after it started
    /// (it started part-way through a day, with a deploy).
    fn metered_from(&self) -> Option<String> {
        let first = NaiveDate::parse_from_str(self.counted_since.as_deref()?, "%Y-%m-%d").ok()?;
        Some(first.succ_opt()?.format("%Y-%m-%d").to_string())
    }

    /// The first whole day run records hold everything a run used.
    fn runs_from(&self) -> String {
        self.metered_from()
            .filter(|d| d.as_str() > RUN_RECORDS_COMPLETE_FROM)
            .unwrap_or_else(|| RUN_RECORDS_COMPLETE_FROM.to_string())
    }

    /// Whether a period starting at `start` is wholly inside per-call counting.
    fn metered_covers(&self, start: &Window) -> bool {
        self.metered_from()
            .is_some_and(|d| d <= start.start.format("%Y-%m-%d").to_string())
    }

    /// Whether every run started in a period starting at `start` has a
    /// complete record.
    fn runs_cover(&self, start: &Window) -> bool {
        self.runs_from() <= start.start.format("%Y-%m-%d").to_string()
    }
}

fn round4(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

fn run_slice(r: &RunRow) -> UsageSlice {
    UsageSlice {
        tokens_in: r.tokens_in.max(0.0) as u64,
        tokens_out: r.tokens_out.max(0.0) as u64,
        cache_read: r.cache_read_tokens.max(0.0) as u64,
        cache_write: r.cache_write_tokens.max(0.0) as u64,
        calls: 0,
        cost_usd: r.cost_usd.max(0.0),
    }
}

/// The kind of work a run is, from its record: a security fix (its ticket
/// id says so), an alert's run, or a ticket run.
fn run_kind(r: &RunRow) -> &'static str {
    if common::security_fixes::is_ticket(&r.ticket_id) {
        work_kind::SECURITY
    } else if r.source == "alert" {
        work_kind::LOGS
    } else {
        work_kind::TICKET_RUNS
    }
}

/// The four parts, their sum and the cost, as the page reads them.
fn parts(s: &UsageSlice) -> Value {
    json!({
        "tokens": s.total(),
        "tokens_in": s.tokens_in,
        "tokens_out": s.tokens_out,
        "cache_read_tokens": s.cache_read,
        "cache_write_tokens": s.cache_write,
        "cost_usd": round4(s.cost_usd),
    })
}

fn with(mut v: Value, extra: Value) -> Value {
    if let (Some(a), Some(b)) = (v.as_object_mut(), extra.as_object()) {
        for (k, x) in b {
            a.insert(k.clone(), x.clone());
        }
    }
    v
}

/// One period's use, by day, feature and model.
#[derive(Debug, Clone, Default)]
struct Measured {
    total: UsageSlice,
    /// By kind of work, then by the feature (metered) inside it.
    by_kind: BTreeMap<&'static str, BTreeMap<String, UsageSlice>>,
    by_model: BTreeMap<String, UsageSlice>,
    daily: BTreeMap<String, UsageSlice>,
}

impl Measured {
    fn kind_total(&self, kind: &str) -> UsageSlice {
        let mut t = UsageSlice::default();
        for s in self.by_kind.get(kind).into_iter().flat_map(|m| m.values()) {
            t.add(s);
        }
        t
    }
}

/// What the meter counted on the days of `w`; `None` when it has none.
fn metered(w: Window, by_day: &HashMap<&str, &MeteredDay>) -> Option<Measured> {
    let mut m = Measured::default();
    let mut any = false;
    for d in window_days(w) {
        let Some(md) = by_day.get(d.as_str()) else {
            continue;
        };
        any = true;
        m.total.add(&md.total);
        m.daily.insert(d, md.total);
        for (f, s) in &md.by_feature {
            m.by_kind
                .entry(work_kind::of_feature(f))
                .or_default()
                .entry(f.clone())
                .or_default()
                .add(s);
        }
        for (model, s) in &md.by_model {
            m.by_model.entry(model.clone()).or_default().add(s);
        }
    }
    any.then_some(m)
}

/// What the records of the runs started in `w` add up to, on the day each
/// run started. No models: a run record doesn't keep them.
fn from_runs(runs: &[&RunRow]) -> Measured {
    let mut m = Measured::default();
    for r in runs {
        let s = run_slice(r);
        m.total.add(&s);
        m.by_kind
            .entry(run_kind(r))
            .or_default()
            .entry(String::new())
            .or_default()
            .add(&s);
        if let Some(d) = day(&r.created_at) {
            m.daily.entry(d).or_default().add(&s);
        }
    }
    m
}

/// What the pull requests merged from a period's runs took.
#[derive(Debug, Clone, Default, PartialEq)]
struct PerMergedPr {
    prs: usize,
    /// The runs that worked on those pull requests.
    runs: usize,
    used: UsageSlice,
    by_kind: BTreeMap<&'static str, (usize, usize, UsageSlice)>,
}

impl PerMergedPr {
    /// Runs started in the period whose pull request has merged: every run
    /// of the period on that pull request counts (a rerun pushed to it too),
    /// and nothing else does. Failed runs, runs whose pull request is still
    /// open or was closed, and runs that opened none are not in it, so the
    /// number moves with what a merged pull request took, not with how many
    /// merged.
    fn of(runs: &[&RunRow]) -> PerMergedPr {
        let mut kind_of: HashMap<(String, u64), &'static str> = HashMap::new();
        for r in runs.iter().filter(|r| r.has_pr() && r.merged()) {
            kind_of.entry(r.pr_key()).or_insert_with(|| run_kind(r));
        }
        let mut out = PerMergedPr {
            prs: kind_of.len(),
            ..Default::default()
        };
        for kind in kind_of.values() {
            out.by_kind.entry(*kind).or_default().0 += 1;
        }
        for r in runs.iter().filter(|r| r.has_pr()) {
            let Some(kind) = kind_of.get(&r.pr_key()) else {
                continue;
            };
            let s = run_slice(r);
            out.runs += 1;
            out.used.add(&s);
            let e = out.by_kind.entry(*kind).or_default();
            e.1 += 1;
            e.2.add(&s);
        }
        out
    }

    fn json(&self) -> Value {
        let per = |s: &UsageSlice, prs: usize| {
            json!({
                "tokens_per_pr": ratio(s.total() as f64, prs as f64).map(f64::round),
                "cost_per_pr": ratio(s.cost_usd, prs as f64).map(round2),
            })
        };
        let by_kind: Vec<Value> = work_kind::ALL
            .iter()
            .filter_map(|k| self.by_kind.get(k).map(|v| (k, v)))
            .map(|(k, (prs, runs, s))| {
                with(
                    with(parts(s), per(s, *prs)),
                    json!({"kind": k, "prs": prs, "runs": runs}),
                )
            })
            .collect();
        with(
            with(parts(&self.used), per(&self.used, self.prs)),
            json!({"prs": self.prs, "runs": self.runs, "by_kind": by_kind}),
        )
    }
}

/// How a run's work ended, each run in exactly one.
fn outcome(r: &RunRow, merged: &HashSet<(String, u64)>) -> &'static str {
    if r.has_pr() && merged.contains(&r.pr_key()) {
        "merged"
    } else if r.failed() {
        "failed"
    } else if r.has_pr() && r.pr_outcome == "closed" {
        "closed"
    } else if r.has_pr() {
        "open"
    } else {
        "no_pr"
    }
}

const OUTCOMES: [&str; 5] = ["merged", "open", "closed", "failed", "no_pr"];

/// Everything the page shows about token use for `window`, against `prev`.
///
/// `runs` are the runs of both periods (any repository; `repo` picks one),
/// `metered` the meter's days over both. With one repository picked the
/// meter can't answer (it isn't kept per repository), so the total and the
/// kinds come from the run records and there are no models.
pub fn token_use(
    window: Window,
    prev: Window,
    runs: &[RunRow],
    metered_days: &[MeteredDay],
    repo: Option<&str>,
    since: &Since,
) -> Value {
    let repo_ok = |r: &str| repo.is_none_or(|want| r.eq_ignore_ascii_case(want));
    let pick = |w: Window| -> Vec<&RunRow> {
        runs.iter()
            .filter(|r| w.contains(&r.created_at) && repo_ok(&r.repo))
            .collect()
    };
    let (cur_runs, prev_runs) = (pick(window), pick(prev));
    let by_day: HashMap<&str, &MeteredDay> =
        metered_days.iter().map(|m| (m.day.as_str(), m)).collect();

    // The total and its breakdowns.
    let cur_metered = metered(window, &by_day).filter(|_| repo.is_none());
    let (basis, cur, before, comparable) = match cur_metered {
        Some(cur) => (
            "metered",
            cur,
            // No meter day in a period that was being counted is a period
            // with no calls.
            metered(prev, &by_day).unwrap_or_default(),
            since.metered_covers(&prev),
        ),
        None => (
            "runs",
            from_runs(&cur_runs),
            from_runs(&prev_runs),
            since.runs_cover(&prev),
        ),
    };
    let not_comparable = (!comparable).then_some(if basis == "metered" {
        "counting_started_later"
    } else {
        "run_records_were_partial"
    });
    let before_of = |s: UsageSlice| {
        if comparable {
            json!({"previous_tokens": s.total(), "previous_cost_usd": round4(s.cost_usd)})
        } else {
            json!({"previous_tokens": null, "previous_cost_usd": null})
        }
    };

    let by_kind: Vec<Value> = work_kind::ALL
        .iter()
        .filter_map(|k| {
            let total = cur.kind_total(k);
            (total.total() > 0).then(|| {
                let mut features: Vec<(&String, &UsageSlice)> = cur
                    .by_kind
                    .get(k)
                    .into_iter()
                    .flatten()
                    .filter(|(f, _)| !f.is_empty())
                    .collect();
                features.sort_by(|a, b| b.1.total().cmp(&a.1.total()).then(a.0.cmp(b.0)));
                with(
                    with(parts(&total), before_of(before.kind_total(k))),
                    json!({
                        "kind": k,
                        "calls": total.calls,
                        "features": features
                            .into_iter()
                            .map(|(f, s)| with(parts(s), json!({"feature": f, "calls": s.calls})))
                            .collect::<Vec<_>>(),
                    }),
                )
            })
        })
        .collect();

    let mut models: Vec<(&String, &UsageSlice)> = cur.by_model.iter().collect();
    models.sort_by(|a, b| b.1.total().cmp(&a.1.total()).then(a.0.cmp(b.0)));
    let by_model: Vec<Value> = models
        .into_iter()
        .map(|(model, s)| {
            with(
                with(
                    parts(s),
                    before_of(before.by_model.get(model).copied().unwrap_or_default()),
                ),
                json!({
                    "model": model,
                    "calls": s.calls,
                    "cache_hit_share": s.cache_hit_share(),
                    "price_known": common::usage::price_known(model),
                }),
            )
        })
        .collect();

    let daily: Vec<Value> = window_days(window)
        .into_iter()
        .map(|d| {
            let s = cur.daily.get(&d).copied().unwrap_or_default();
            with(parts(&s), json!({"day": d}))
        })
        .collect();

    // What a merged pull request took, from the run records.
    let per_pr = PerMergedPr::of(&cur_runs);
    let per_pr_before = PerMergedPr::of(&prev_runs);
    let per_pr_comparable = since.runs_cover(&prev);
    let mut per_merged_pr = per_pr.json();
    per_merged_pr["comparable"] = json!(per_pr_comparable);
    per_merged_pr["not_comparable"] =
        json!((!per_pr_comparable).then_some("run_records_were_partial"));
    per_merged_pr["previous"] = if per_pr_comparable {
        let mut p = per_pr_before.json();
        if let Some(o) = p.as_object_mut() {
            o.remove("by_kind");
        }
        p
    } else {
        Value::Null
    };

    // Where the period's run tokens went, by how each run's work ended.
    let merged_keys: HashSet<(String, u64)> = cur_runs
        .iter()
        .filter(|r| r.has_pr() && r.merged())
        .map(|r| r.pr_key())
        .collect();
    let mut by_outcome: HashMap<&str, (usize, UsageSlice)> = HashMap::new();
    for r in &cur_runs {
        let e = by_outcome.entry(outcome(r, &merged_keys)).or_default();
        e.0 += 1;
        e.1.add(&run_slice(r));
    }
    let run_outcomes: Vec<Value> = OUTCOMES
        .iter()
        .filter_map(|o| by_outcome.get(o).map(|v| (o, v)))
        .map(|(o, (n, s))| with(parts(s), json!({"outcome": o, "runs": n})))
        .collect();

    // The heaviest runs, and the heaviest pull requests (all their runs).
    let mut heavy: Vec<&RunRow> = cur_runs
        .iter()
        .copied()
        .filter(|r| r.tokens() > 0.0)
        .collect();
    heavy.sort_by(|a, b| {
        b.tokens()
            .partial_cmp(&a.tokens())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.run_id.cmp(&b.run_id))
    });
    let heaviest_runs: Vec<Value> = heavy
        .iter()
        .take(HEAVIEST)
        .map(|r| {
            with(
                parts(&run_slice(r)),
                json!({
                    "run_id": r.run_id, "title": r.title, "repo": r.repo,
                    "pr_number": r.pr_number, "pr_url": r.pr_url,
                    "kind": run_kind(r), "outcome": outcome(r, &merged_keys),
                }),
            )
        })
        .collect();
    struct Pr<'a> {
        latest: &'a RunRow,
        runs: usize,
        used: UsageSlice,
    }
    let mut prs: HashMap<(String, u64), Pr> = HashMap::new();
    for r in heavy.iter().copied().filter(|r| r.has_pr()) {
        let e = prs.entry(r.pr_key()).or_insert(Pr {
            latest: r,
            runs: 0,
            used: UsageSlice::default(),
        });
        if r.created_at > e.latest.created_at {
            e.latest = r;
        }
        e.runs += 1;
        e.used.add(&run_slice(r));
    }
    let mut prs: Vec<((String, u64), Pr)> = prs.into_iter().collect();
    prs.sort_by(|a, b| b.1.used.total().cmp(&a.1.used.total()).then(a.0.cmp(&b.0)));
    let heaviest_prs: Vec<Value> = prs
        .iter()
        .take(HEAVIEST)
        .map(|(key, p)| {
            let ended = if merged_keys.contains(key) {
                "merged"
            } else {
                outcome(p.latest, &merged_keys)
            };
            with(
                parts(&p.used),
                json!({
                    "repo": p.latest.repo, "pr_number": p.latest.pr_number,
                    "pr_url": p.latest.pr_url, "title": p.latest.title,
                    "run_id": p.latest.run_id, "runs": p.runs,
                    "kind": run_kind(p.latest),
                    "outcome": ended,
                }),
            )
        })
        .collect();

    let run_total = from_runs(&cur_runs).total;
    json!({
        "basis": basis,
        "counted_since": since.counted_since,
        "metered_from": since.metered_from(),
        "run_records_from": since.runs_from(),
        "total": with(parts(&cur.total), json!({
            "calls": cur.total.calls,
            "cache_hit_share": cur.total.cache_hit_share(),
        })),
        "previous": if comparable { parts(&before.total) } else { Value::Null },
        "comparable": comparable,
        "not_comparable": not_comparable,
        "daily": daily,
        "by_kind": by_kind,
        "by_model": by_model,
        "per_merged_pr": per_merged_pr,
        "runs": with(parts(&run_total), json!({"runs": cur_runs.len(), "by_outcome": run_outcomes})),
        "heaviest_runs": heaviest_runs,
        "heaviest_prs": heaviest_prs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    /// 30 days ending 2026-12-31 against the 30 before: both wholly after
    /// run records became complete and after `counted()` started counting.
    fn windows() -> (Window, Window) {
        Window::last_days(Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap(), 30)
    }

    fn counted() -> Since {
        Since {
            counted_since: Some("2026-10-19".into()),
        }
    }

    fn run(id: &str, at: &str, pr: u64, tokens: [f64; 4], cost: f64) -> RunRow {
        RunRow {
            run_id: id.into(),
            created_at: format!("{at}T12:00:00Z"),
            status: "completed".into(),
            repo: "acme/web".into(),
            source: "jira".into(),
            ticket_id: format!("WEB-{id}"),
            title: format!("ticket {id}"),
            pr_number: pr,
            pr_url: if pr > 0 {
                format!("https://github.com/acme/web/pull/{pr}")
            } else {
                String::new()
            },
            tokens_in: tokens[0],
            tokens_out: tokens[1],
            cache_read_tokens: tokens[2],
            cache_write_tokens: tokens[3],
            cost_usd: cost,
            ..Default::default()
        }
    }

    fn merged(mut r: RunRow) -> RunRow {
        r.pr_outcome = "merged".into();
        r
    }

    fn slice(tokens: [u64; 4], calls: u64, cost: f64) -> UsageSlice {
        UsageSlice {
            tokens_in: tokens[0],
            tokens_out: tokens[1],
            cache_read: tokens[2],
            cache_write: tokens[3],
            calls,
            cost_usd: cost,
        }
    }

    fn meter(
        day: &str,
        features: &[(&str, UsageSlice)],
        models: &[(&str, UsageSlice)],
    ) -> MeteredDay {
        let mut total = UsageSlice::default();
        for (_, s) in features {
            total.add(s);
        }
        MeteredDay {
            day: day.into(),
            cost_usd: total.cost_usd,
            tokens: total.total() as f64,
            total,
            by_feature: features.iter().map(|(f, s)| (f.to_string(), *s)).collect(),
            by_model: models.iter().map(|(m, s)| (m.to_string(), *s)).collect(),
            ..Default::default()
        }
    }

    /// The tile divided every run's tokens by the merged pull requests, so
    /// two failed runs and one still open made a merged PR look four times
    /// as heavy. Only the runs behind the merged PRs count now.
    #[test]
    fn tokens_per_merged_pr_counts_only_the_runs_behind_merged_prs() {
        let (w, p) = windows();
        let mut failed = run("f", "2026-12-10", 0, [10.0, 10.0, 3000.0, 0.0], 3.0);
        failed.status = "failed".into();
        let mut closed = run("c", "2026-12-11", 9, [0.0, 0.0, 500.0, 0.0], 0.5);
        closed.pr_outcome = "closed".into();
        let runs = vec![
            merged(run("a", "2026-12-05", 1, [100.0, 50.0, 800.0, 50.0], 1.0)),
            // A rerun that pushed to the same pull request: same PR, more tokens.
            run("a2", "2026-12-06", 1, [0.0, 0.0, 1000.0, 0.0], 0.25),
            merged(run("b", "2026-12-07", 2, [0.0, 0.0, 2000.0, 0.0], 0.75)),
            run("open", "2026-12-08", 3, [0.0, 0.0, 4000.0, 0.0], 4.0),
            failed,
            closed,
        ];
        let v = token_use(w, p, &runs, &[], None, &counted());
        let per = &v["per_merged_pr"];
        assert_eq!(per["prs"], 2);
        assert_eq!(per["runs"], 3);
        assert_eq!(per["tokens"], 4000);
        assert_eq!(per["cache_read_tokens"], 3800);
        assert_eq!(per["tokens_per_pr"], 2000.0);
        assert_eq!(per["cost_per_pr"], 1.0);
        // The old number, for the same rows: every run's tokens over 2 PRs.
        assert_eq!(v["runs"]["tokens"], 11_520);
        let by: HashMap<&str, (u64, u64)> = v["runs"]["by_outcome"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                (
                    o["outcome"].as_str().unwrap(),
                    (o["runs"].as_u64().unwrap(), o["tokens"].as_u64().unwrap()),
                )
            })
            .collect();
        assert_eq!(by["merged"], (3, 4000));
        assert_eq!(by["open"], (1, 4000));
        assert_eq!(by["failed"], (1, 3020));
        assert_eq!(by["closed"], (1, 500));
        assert_eq!(by.values().map(|v| v.0).sum::<u64>(), runs.len() as u64);
    }

    #[test]
    fn fewer_merges_alone_do_not_move_tokens_per_merged_pr() {
        let (w, p) = windows();
        let pr = |id: &str, at: &str, n: u64| merged(run(id, at, n, [0.0, 0.0, 1000.0, 0.0], 1.0));
        let mut runs = vec![
            pr("p1", "2026-11-05", 1),
            pr("p2", "2026-11-06", 2),
            pr("p3", "2026-11-07", 3),
            pr("p4", "2026-11-08", 4),
            pr("c1", "2026-12-05", 5),
        ];
        // The current period also has runs that merged nothing.
        for i in 0..5 {
            runs.push(run(
                &format!("o{i}"),
                "2026-12-09",
                20 + i,
                [0.0, 0.0, 9000.0, 0.0],
                9.0,
            ));
        }
        let v = token_use(w, p, &runs, &[], None, &counted());
        let per = &v["per_merged_pr"];
        assert_eq!(per["tokens_per_pr"], 1000.0);
        assert_eq!(per["comparable"], true);
        assert_eq!(per["previous"]["tokens_per_pr"], 1000.0);
        assert_eq!(per["previous"]["prs"], 4);
    }

    #[test]
    fn merged_prs_split_by_kind_of_run() {
        let (w, p) = windows();
        let mut sec = merged(run("s", "2026-12-05", 7, [0.0, 0.0, 600.0, 0.0], 0.5));
        sec.ticket_id = "SEC-acme-web-1".into();
        let mut alert = merged(run("al", "2026-12-06", 8, [0.0, 0.0, 300.0, 0.0], 0.25));
        alert.source = "alert".into();
        let runs = vec![
            sec,
            alert,
            merged(run("t1", "2026-12-07", 1, [0.0, 0.0, 5000.0, 0.0], 2.0)),
            merged(run("t2", "2026-12-08", 2, [0.0, 0.0, 1000.0, 0.0], 1.0)),
        ];
        let v = token_use(w, p, &runs, &[], None, &counted());
        let kinds = v["per_merged_pr"]["by_kind"].as_array().unwrap();
        let row = |k: &str| kinds.iter().find(|r| r["kind"] == k).unwrap();
        assert_eq!(row("ticket_runs")["prs"], 2);
        assert_eq!(row("ticket_runs")["tokens_per_pr"], 3000.0);
        assert_eq!(row("security")["tokens_per_pr"], 600.0);
        assert_eq!(row("logs")["tokens_per_pr"], 300.0);
        assert_eq!(
            kinds
                .iter()
                .map(|r| r["prs"].as_u64().unwrap())
                .sum::<u64>(),
            v["per_merged_pr"]["prs"].as_u64().unwrap()
        );
    }

    /// Run records from before invocations ADDed their tokens can be short
    /// by any amount: a period on them is never compared with one after.
    #[test]
    fn periods_recorded_differently_are_not_compared() {
        let end = Utc.with_ymd_and_hms(2026, 11, 13, 0, 0, 0).unwrap();
        let (w, p) = Window::last_days(end, 30);
        let runs = vec![
            merged(run("old", "2026-10-01", 1, [0.0, 0.0, 100.0, 0.0], 0.1)),
            merged(run("new", "2026-11-05", 2, [0.0, 0.0, 656.0, 0.0], 0.6)),
        ];
        let v = token_use(w, p, &runs, &[], None, &counted());
        let per = &v["per_merged_pr"];
        assert_eq!(per["tokens_per_pr"], 656.0);
        assert_eq!(per["comparable"], false);
        assert_eq!(per["not_comparable"], "run_records_were_partial");
        assert_eq!(
            per["previous"],
            Value::Null,
            "no number to compute a change from"
        );
        assert_eq!(v["run_records_from"], RUN_RECORDS_COMPLETE_FROM);
        // The total falls back to the run records here (no meter days), and
        // is not compared either.
        assert_eq!(v["basis"], "runs");
        assert_eq!(v["comparable"], false);
        assert_eq!(v["previous"], Value::Null);
        assert_eq!(v["by_kind"][0]["previous_tokens"], Value::Null);

        // 7 days ending the same day: both periods start after the change.
        let (w, p) = Window::last_days(end, 7);
        let v = token_use(w, p, &runs, &[], None, &counted());
        assert_eq!(v["per_merged_pr"]["comparable"], true);
        assert_eq!(v["comparable"], true);
    }

    #[test]
    fn the_metered_total_is_compared_only_when_counting_covers_the_period_before() {
        let (w, p) = windows();
        let days = vec![
            meter(
                "2026-12-10",
                &[("run", slice([10, 20, 900, 70], 4, 2.0))],
                &[("claude-opus-5-5", slice([10, 20, 900, 70], 4, 2.0))],
            ),
            meter(
                "2026-11-10",
                &[("run", slice([5, 5, 80, 10], 1, 0.5))],
                &[("claude-opus-5-5", slice([5, 5, 80, 10], 1, 0.5))],
            ),
        ];
        let v = token_use(w, p, &[], &days, None, &counted());
        assert_eq!(v["basis"], "metered");
        assert_eq!(v["total"]["tokens"], 1000);
        assert_eq!(v["comparable"], true);
        assert_eq!(v["previous"]["tokens"], 100);
        assert_eq!(v["by_kind"][0]["previous_tokens"], 100);
        assert_eq!(v["by_model"][0]["previous_tokens"], 100);

        // Counting started inside the period before: its total is partial.
        let late = Since {
            counted_since: Some("2026-11-10".into()),
        };
        let v = token_use(w, p, &[], &days, None, &late);
        assert_eq!(v["total"]["tokens"], 1000);
        assert_eq!(v["comparable"], false);
        assert_eq!(v["not_comparable"], "counting_started_later");
        assert_eq!(v["previous"], Value::Null);
        assert_eq!(v["by_model"][0]["previous_tokens"], Value::Null);
        assert_eq!(v["metered_from"], "2026-11-11");
        // Run records are complete only from that upgrade too.
        assert_eq!(v["run_records_from"], "2026-11-11");
        assert_eq!(v["per_merged_pr"]["comparable"], false);

        // The day counting starts is a part day: a period starting on it is
        // not wholly counted, one starting the day after is.
        let (first, next) = (
            Since {
                counted_since: Some("2026-11-01".into()),
            },
            Since {
                counted_since: Some("2026-10-31".into()),
            },
        );
        assert_eq!(p.start.format("%Y-%m-%d").to_string(), "2026-11-01");
        assert_eq!(
            token_use(w, p, &[], &days, None, &first)["comparable"],
            false
        );
        assert_eq!(token_use(w, p, &[], &days, None, &next)["comparable"], true);

        // A period before with no calls at all, while counting: zero, compared.
        let v = token_use(w, p, &[], &days[..1], None, &counted());
        assert_eq!(v["comparable"], true);
        assert_eq!(v["previous"]["tokens"], 0);
    }

    #[test]
    fn kinds_models_and_days_come_from_the_meter_with_every_part() {
        let (w, p) = windows();
        let days = vec![
            meter(
                "2026-12-10",
                &[
                    ("run", slice([100, 200, 5000, 700], 10, 6.0)),
                    ("plan", slice([0, 0, 1000, 0], 1, 0.5)),
                    ("feedback", slice([10, 10, 480, 0], 2, 0.25)),
                    ("review", slice([50, 50, 2000, 400], 5, 1.5)),
                    ("security_fix", slice([0, 100, 0, 0], 1, 0.125)),
                    ("security", slice([0, 0, 200, 0], 1, 0.0625)),
                    ("chat", slice([1, 1, 0, 0], 1, 0.01)),
                    ("agent_ask", slice([1, 1, 0, 0], 1, 0.01)),
                    ("log_analysis", slice([20, 5, 0, 0], 3, 0.02)),
                    ("onboard", slice([3, 3, 0, 0], 1, 0.03)),
                ],
                &[
                    ("claude-opus-5-5", slice([150, 300, 8000, 1100], 20, 8.0)),
                    ("claude-haiku-5-5", slice([35, 70, 680, 0], 6, 0.5075)),
                ],
            ),
            meter(
                "2026-12-11",
                &[("review", slice([0, 0, 500, 0], 1, 0.25))],
                &[("claude-not-released-9", slice([0, 0, 500, 0], 1, 0.25))],
            ),
        ];
        let v = token_use(w, p, &[], &days, None, &counted());
        let kinds: Vec<(&str, u64)> = v["by_kind"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| (k["kind"].as_str().unwrap(), k["tokens"].as_u64().unwrap()))
            .collect();
        assert_eq!(
            kinds,
            [
                ("ticket_runs", 7000),
                ("security", 300),
                ("follow_ups", 500),
                ("reviews", 3000),
                ("chat", 4),
                ("logs", 25),
                ("other", 6),
            ]
        );
        // What is lumped is listed, heaviest first.
        let tickets = &v["by_kind"][0];
        assert_eq!(tickets["features"][0]["feature"], "run");
        assert_eq!(tickets["features"][1]["feature"], "plan");
        assert_eq!(tickets["cache_read_tokens"], 6000);
        assert_eq!(tickets["cache_write_tokens"], 700);
        assert_eq!(tickets["tokens_in"], 100);
        assert_eq!(tickets["tokens_out"], 200);
        assert_eq!(tickets["cost_usd"], 6.5);
        assert_eq!(tickets["calls"], 11);
        // The kinds add up to the total, part by part.
        for part in [
            "tokens",
            "tokens_in",
            "tokens_out",
            "cache_read_tokens",
            "cache_write_tokens",
        ] {
            let sum: u64 = v["by_kind"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| k[part].as_u64().unwrap())
                .sum();
            assert_eq!(sum, v["total"][part].as_u64().unwrap(), "{part}");
        }
        assert_eq!(v["total"]["tokens"], 10_835);
        assert_eq!(v["total"]["calls"], 27);

        let models = v["by_model"].as_array().unwrap();
        assert_eq!(models[0]["model"], "claude-opus-5-5");
        assert_eq!(models[0]["cache_hit_share"], 8000.0 / 9250.0);
        assert_eq!(models[0]["price_known"], true);
        assert_eq!(models[2]["model"], "claude-not-released-9");
        assert_eq!(models[2]["price_known"], false);
        assert_eq!(models[2]["cache_hit_share"], 1.0);

        let daily = v["daily"].as_array().unwrap();
        assert_eq!(daily.len(), 30);
        let d = daily.iter().find(|d| d["day"] == "2026-12-11").unwrap();
        assert_eq!(d["cache_read_tokens"], 500);
        assert_eq!(daily[0]["tokens"], 0);
    }

    #[test]
    fn one_repository_reads_the_run_records_and_has_no_models() {
        let (w, p) = windows();
        let mut other = merged(run("x", "2026-12-05", 4, [0.0, 0.0, 7000.0, 0.0], 7.0));
        other.repo = "acme/api".into();
        let mut sec = run("s", "2026-12-06", 0, [0.0, 0.0, 250.0, 0.0], 0.5);
        sec.ticket_id = "SEC-1".into();
        let runs = vec![
            merged(run("a", "2026-12-05", 1, [100.0, 50.0, 800.0, 50.0], 1.0)),
            sec,
            other,
        ];
        let days = vec![meter(
            "2026-12-10",
            &[("review", slice([0, 0, 99_000, 0], 1, 9.0))],
            &[("claude-opus-5-5", slice([0, 0, 99_000, 0], 1, 9.0))],
        )];
        let v = token_use(w, p, &runs, &days, Some("acme/web"), &counted());
        assert_eq!(v["basis"], "runs");
        assert_eq!(v["total"]["tokens"], 1250);
        assert_eq!(v["by_model"], json!([]));
        assert_eq!(v["by_kind"][0]["kind"], "ticket_runs");
        assert_eq!(v["by_kind"][0]["features"], json!([]));
        assert_eq!(v["by_kind"][1]["kind"], "security");
        assert_eq!(v["by_kind"][1]["tokens"], 250);
        assert_eq!(v["per_merged_pr"]["prs"], 1);
        let day = v["daily"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["day"] == "2026-12-05")
            .unwrap();
        assert_eq!(day["tokens"], 1000);
    }

    #[test]
    fn the_heaviest_runs_and_pull_requests_are_listed_with_links() {
        let (w, p) = windows();
        let mut runs: Vec<RunRow> = (0..14)
            .map(|i| {
                run(
                    &format!("r{i:02}"),
                    "2026-12-05",
                    100 + i,
                    [0.0, 0.0, 1000.0 * (i + 1) as f64, 0.0],
                    1.0,
                )
            })
            .collect();
        // Three runs on one pull request: none the heaviest run, together
        // the heaviest pull request.
        for i in 0..3 {
            runs.push(run(
                &format!("same{i}"),
                &format!("2026-12-0{}", 6 + i),
                7,
                [0.0, 0.0, 6000.0, 0.0],
                2.0,
            ));
        }
        runs.push(run("nothing", "2026-12-05", 0, [0.0; 4], 0.0));
        let v = token_use(w, p, &runs, &[], None, &counted());
        let heavy = v["heaviest_runs"].as_array().unwrap();
        assert_eq!(heavy.len(), 10);
        assert_eq!(heavy[0]["run_id"], "r13");
        assert_eq!(heavy[0]["tokens"], 14_000);
        assert_eq!(heavy[0]["pr_url"], "https://github.com/acme/web/pull/113");
        assert_eq!(heavy[0]["outcome"], "open");
        let prs = v["heaviest_prs"].as_array().unwrap();
        assert_eq!(prs.len(), 10);
        assert_eq!(prs[0]["pr_number"], 7);
        assert_eq!(prs[0]["runs"], 3);
        assert_eq!(prs[0]["tokens"], 18_000);
        assert_eq!(prs[0]["cost_usd"], 6.0);
        assert_eq!(
            prs[0]["run_id"], "same2",
            "the latest run of the pull request"
        );
        assert_eq!(prs[1]["tokens"], 14_000);
    }

    #[test]
    fn nothing_recorded_is_an_empty_answer_not_an_error() {
        let (w, p) = windows();
        let v = token_use(w, p, &[], &[], None, &Since::default());
        assert_eq!(v["basis"], "runs");
        assert_eq!(v["total"]["tokens"], 0);
        assert_eq!(v["total"]["cache_hit_share"], Value::Null);
        assert_eq!(v["counted_since"], Value::Null);
        assert_eq!(v["per_merged_pr"]["tokens_per_pr"], Value::Null);
        assert_eq!(v["by_kind"], json!([]));
        assert_eq!(v["heaviest_runs"], json!([]));
    }
}
