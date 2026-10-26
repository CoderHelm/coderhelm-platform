//! The Deliveries overview: how deliveries went over a period, against the
//! period before.
//!
//! `GET /api/deliveries/analytics?period=24h|7d|30d&source=&status=&endpoint=`
//! reads the hour or day rollups ([`common::deliveries_rollup`]) for the
//! period and the one before it: at most 60 items, however many deliveries
//! there were. Nothing scans the records themselves, which are kept 14 days;
//! rollups are kept 120.
//!
//! The first request for a team counts the records written before rollups
//! existed (deliveries, alert events and the Jira event log), each once, a
//! few seconds' worth at a time ([`ensure_backfilled`]); the reply says when
//! that is still going.
//!
//! On a hybrid install the hosted relay's rollups for it are added
//! (`POST /hybrid/deliveries/rollups`), the same way the list adds the relay's
//! records. A self-hosted install adds the deliveries that reached no team.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Duration, SecondsFormat, Timelike, Utc};
use common::deliveries::{self, Delivery};
use common::deliveries_rollup::{
    self as rollup, Counters, EndpointStat, MarkKey, Store, Unrolled, DAY_PREFIX, HOUR_PREFIX,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;
type Fail = (StatusCode, Json<Value>);

fn fail(code: StatusCode, c: &str, message: &str) -> Fail {
    (code, Json(json!({ "error": message, "code": c })))
}

// ── Periods ────────────────────────────────────────────────────────────

/// The periods the overview offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    /// The last 24 hours, by hour.
    Day,
    /// The last 7 days (today included), by day.
    Week,
    /// The last 30 days (today included), by day.
    Month,
}

impl Period {
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.unwrap_or("7d") {
            "24h" | "1d" => Some(Self::Day),
            "7d" | "" => Some(Self::Week),
            "30d" => Some(Self::Month),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
        }
    }

    fn slots(self) -> i64 {
        match self {
            Self::Day => 24,
            Self::Week => 7,
            Self::Month => 30,
        }
    }

    pub fn hourly(self) -> bool {
        self == Self::Day
    }
}

/// The buckets of a period and of the one before it, oldest first, each as
/// the start of its hour or day.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub hourly: bool,
    pub current: Vec<DateTime<Utc>>,
    pub previous: Vec<DateTime<Utc>>,
}

pub fn plan(period: Period, now: DateTime<Utc>) -> Plan {
    let hourly = period.hourly();
    let (last, step) = if hourly {
        let h = now
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(now);
        (h, Duration::hours(1))
    } else {
        let d = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .map(|d| d.and_utc())
            .unwrap_or(now);
        (d, Duration::days(1))
    };
    let n = period.slots();
    let at = |i: i64| last - step * (i as i32);
    let current = (0..n).rev().map(at).collect();
    let previous = (n..2 * n).rev().map(at).collect();
    Plan {
        hourly,
        current,
        previous,
    }
}

fn bucket_key(hourly: bool, t: DateTime<Utc>) -> String {
    if hourly {
        rollup::hour_key(t)
    } else {
        rollup::day_key(t)
    }
}

fn bucket_sk(hourly: bool, t: DateTime<Utc>) -> String {
    let prefix = if hourly { HOUR_PREFIX } else { DAY_PREFIX };
    format!("{prefix}{}", bucket_key(hourly, t))
}

// ── Tallies ────────────────────────────────────────────────────────────

/// A latency histogram: bucket name to count.
pub type Hist = BTreeMap<String, i64>;

/// The counters of one scope (a source and direction, or an endpoint).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Tally {
    pub handled: i64,
    pub ignored: i64,
    pub failed: i64,
    pub retried: i64,
    pub retried_ok: i64,
    pub retries: i64,
    pub lat_ok: Hist,
    pub lat_failed: Hist,
    pub reasons: BTreeMap<String, i64>,
    pub events: BTreeMap<String, i64>,
}

fn add_map(into: &mut BTreeMap<String, i64>, from: &BTreeMap<String, i64>) {
    for (k, v) in from {
        *into.entry(k.clone()).or_insert(0) += v;
    }
}

impl Tally {
    pub fn add(&mut self, o: &Tally) {
        self.handled += o.handled;
        self.ignored += o.ignored;
        self.failed += o.failed;
        self.retried += o.retried;
        self.retried_ok += o.retried_ok;
        self.retries += o.retries;
        add_map(&mut self.lat_ok, &o.lat_ok);
        add_map(&mut self.lat_failed, &o.lat_failed);
        add_map(&mut self.reasons, &o.reasons);
        add_map(&mut self.events, &o.events);
    }

    pub fn total(&self) -> i64 {
        self.handled + self.ignored + self.failed
    }

    /// Only what the status filter shows.
    pub fn only(&self, st: StatusSel) -> Tally {
        let ok = st != StatusSel::Failed;
        let failed = matches!(st, StatusSel::All | StatusSel::Failed);
        Tally {
            handled: if matches!(st, StatusSel::All | StatusSel::Ok | StatusSel::Handled) {
                self.handled
            } else {
                0
            },
            ignored: if matches!(st, StatusSel::All | StatusSel::Ok | StatusSel::Ignored) {
                self.ignored
            } else {
                0
            },
            failed: if failed { self.failed } else { 0 },
            retried: match st {
                StatusSel::All => self.retried,
                StatusSel::Failed => (self.retried - self.retried_ok).max(0),
                _ => self.retried_ok,
            },
            retried_ok: if st == StatusSel::Failed {
                0
            } else {
                self.retried_ok
            },
            retries: if st == StatusSel::All {
                self.retries
            } else {
                0
            },
            lat_ok: if ok { self.lat_ok.clone() } else { Hist::new() },
            lat_failed: if failed {
                self.lat_failed.clone()
            } else {
                Hist::new()
            },
            reasons: if failed {
                self.reasons.clone()
            } else {
                BTreeMap::new()
            },
            events: if failed {
                self.events.clone()
            } else {
                BTreeMap::new()
            },
        }
    }

    fn latency(&self) -> Hist {
        let mut h = self.lat_ok.clone();
        add_map(&mut h, &self.lat_failed);
        h
    }
}

/// The status filter, as the list offers it (`pending` has no rollup: a
/// retrying delivery counts once it finishes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusSel {
    All,
    Ok,
    Failed,
    Handled,
    Ignored,
}

impl StatusSel {
    pub fn parse(raw: Option<&str>) -> Option<Self> {
        Some(match raw.unwrap_or("all") {
            "" | "all" => Self::All,
            "ok" => Self::Ok,
            "failed" => Self::Failed,
            "handled" => Self::Handled,
            "ignored" => Self::Ignored,
            _ => return None,
        })
    }
}

/// One bucket's counters by scope: `s#<source>#<in|out>` or `ep#<id>`.
pub fn by_scope(c: &Counters) -> BTreeMap<String, Tally> {
    let mut out: BTreeMap<String, Tally> = BTreeMap::new();
    for (k, v) in c {
        let parts: Vec<&str> = k.split('#').collect();
        let (scope, field) = match parts.first() {
            Some(&"s") if parts.len() >= 4 => (parts[..3].join("#"), &parts[3..]),
            Some(&"ep") if parts.len() >= 3 => (parts[..2].join("#"), &parts[2..]),
            _ => continue,
        };
        let t = out.entry(scope).or_default();
        match field {
            ["handled"] => t.handled += v,
            ["ignored"] => t.ignored += v,
            ["failed"] => t.failed += v,
            ["retried"] => t.retried += v,
            ["retried_ok"] => t.retried_ok += v,
            ["retries"] => t.retries += v,
            ["lat", "ok", b] => *t.lat_ok.entry(b.to_string()).or_insert(0) += v,
            ["lat", "failed", b] => *t.lat_failed.entry(b.to_string()).or_insert(0) += v,
            ["r", code] => *t.reasons.entry(code.to_string()).or_insert(0) += v,
            ["e", event] => *t.events.entry(event.to_string()).or_insert(0) += v,
            _ => {}
        }
    }
    out
}

/// What the overview is narrowed to.
#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    pub source: Option<String>,
    pub status: StatusSel,
    pub endpoint: Option<String>,
}

impl Selection {
    /// A source scope's source and direction, when the selection takes it.
    fn source_of<'a>(&self, scope: &'a str) -> Option<(&'a str, &'a str)> {
        let mut p = scope.split('#');
        if p.next()? != "s" {
            return None;
        }
        let (src, dir) = (p.next()?, p.next()?);
        match &self.source {
            Some(s) if s != src => None,
            _ => Some((src, dir)),
        }
    }

    /// Whether the headline numbers take this scope: the endpoint's own
    /// scope when one is chosen, otherwise every source scope that matches.
    fn takes(&self, scope: &str) -> bool {
        match &self.endpoint {
            Some(ep) => scope == rollup::endpoint_scope(ep),
            None => self.source_of(scope).is_some(),
        }
    }

    /// Endpoints show unless another source than webhooks is chosen.
    fn shows_endpoints(&self) -> bool {
        self.source.as_deref().is_none_or(|s| s == "webhook")
    }
}

// ── The reply ──────────────────────────────────────────────────────────

/// Headline numbers for one period.
#[derive(Debug, Clone, Default, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Totals {
    /// Finished deliveries (handled, ignored or failed).
    #[schema(example = 1284)]
    pub deliveries: i64,
    /// Finished and handled.
    #[schema(example = 1190)]
    pub handled: i64,
    /// Finished and ignored on purpose (a duplicate, an event nothing needed).
    #[schema(example = 61)]
    pub ignored: i64,
    /// Finished and failed.
    #[schema(example = 33)]
    pub failed: i64,
    /// Handled or ignored, out of finished. Null with none.
    #[schema(example = 0.9743)]
    pub success_rate: Option<f64>,
    /// Deliveries that took more than one attempt.
    #[schema(example = 41)]
    pub retried: i64,
    /// Of those, the ones that went fine in the end.
    #[schema(example = 37)]
    pub retried_ok: i64,
    /// Attempts after the first, in all. Null when a status filter is on.
    #[schema(example = 58)]
    pub retries: Option<i64>,
    /// Latency percentiles in milliseconds (the last attempt's), from a
    /// histogram, so within a bucket's width. Null with no timed deliveries.
    #[schema(example = 180)]
    pub p50_ms: Option<u64>,
    /// 95th percentile latency, milliseconds.
    #[schema(example = 1240)]
    pub p95_ms: Option<u64>,
    /// 99th percentile latency, milliseconds.
    #[schema(example = 4800)]
    pub p99_ms: Option<u64>,
}

pub fn totals(t: &Tally, status: StatusSel) -> Totals {
    let n = t.total();
    let ok = t.handled + t.ignored;
    let h = t.latency();
    Totals {
        deliveries: n,
        handled: t.handled,
        ignored: t.ignored,
        failed: t.failed,
        success_rate: (n > 0).then(|| round4(ok as f64 / n as f64)),
        retried: t.retried,
        retried_ok: t.retried_ok,
        retries: (status == StatusSel::All).then_some(t.retries),
        p50_ms: rollup::percentile(&h, 0.50),
        p95_ms: rollup::percentile(&h, 0.95),
        p99_ms: rollup::percentile(&h, 0.99),
    }
}

fn round4(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

/// One hour or day of the period.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Point {
    /// The bucket's start (RFC 3339, UTC).
    #[schema(example = "2026-10-26T00:00:00Z")]
    pub start: String,
    /// The bucket's end, exclusive.
    #[schema(example = "2026-10-27T00:00:00Z")]
    pub end: String,
    /// Finished deliveries in the bucket.
    #[schema(example = 190)]
    pub deliveries: i64,
    /// Handled or ignored.
    #[schema(example = 186)]
    pub ok: i64,
    /// Failed.
    #[schema(example = 4)]
    pub failed: i64,
    /// Handled or ignored, out of finished. Null with none.
    #[schema(example = 0.9789)]
    pub success_rate: Option<f64>,
    /// 95th percentile latency, milliseconds.
    #[schema(example = 1100)]
    pub p95_ms: Option<u64>,
    /// Deliveries in the matching bucket of the period before.
    #[schema(example = 171)]
    pub previous_deliveries: i64,
}

/// One source and direction.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct SourceRow {
    /// `github`, `relay`, `webhook`, `alerts`, `jira`, `slack`, `teams`, `email`, `broker`, `knowledge`.
    #[schema(example = "github")]
    pub source: String,
    /// `inbound` or `outbound`.
    #[schema(example = "inbound")]
    pub direction: String,
    /// Finished deliveries in the period.
    #[schema(example = 912)]
    pub deliveries: i64,
    /// Handled or ignored.
    #[schema(example = 905)]
    pub ok: i64,
    /// Failed.
    #[schema(example = 7)]
    pub failed: i64,
    /// Handled or ignored, out of finished. Null with none.
    #[schema(example = 0.9923)]
    pub success_rate: Option<f64>,
    /// 95th percentile latency, milliseconds.
    #[schema(example = 640)]
    pub p95_ms: Option<u64>,
    /// Finished deliveries in the period before.
    #[schema(example = 870)]
    pub previous_deliveries: i64,
    /// The success rate in the period before.
    #[schema(example = 0.9897)]
    pub previous_success_rate: Option<f64>,
}

/// Failures with one reason.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ReasonRow {
    /// The reason code, as on each delivery.
    #[schema(example = "webhook_server_error")]
    pub code: String,
    /// What happened, in plain words.
    #[schema(example = "Your webhook endpoint answered with a server error (5xx).")]
    pub message: String,
    /// What to do about it.
    #[schema(
        example = "Check your endpoint's logs for this event id. CoderHelm retries for about a day."
    )]
    pub fix: String,
    /// Failures with this reason in the period.
    #[schema(example = 12)]
    pub count: i64,
    /// Failures with this reason in the period before.
    #[schema(example = 3)]
    pub previous_count: i64,
    /// The sources it happened on, most first.
    #[schema(example = json!(["webhook"]))]
    pub sources: Vec<String>,
}

/// An event type that failed.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct EventRow {
    /// The event or message type.
    #[schema(example = "run.failed")]
    pub event: String,
    /// Where it came from or went.
    #[schema(example = "webhook")]
    pub source: String,
    /// Failures of this type in the period.
    #[schema(example = 9)]
    pub failed: i64,
}

/// One outbound webhook endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct EndpointRow {
    /// The endpoint's id.
    #[schema(example = "ep_01JBENDPT4K2M8N3P5Q7R9S1T3")]
    pub id: String,
    /// The endpoint's address (empty once the endpoint is deleted).
    #[schema(example = "https://hooks.example.com/coderhelm")]
    pub url: String,
    /// The endpoint's description, as the team wrote it.
    #[schema(example = "Incident bot")]
    pub description: String,
    /// The endpoint no longer exists; its counts stay for the period.
    #[schema(example = false)]
    pub deleted: bool,
    /// Whether the endpoint is on.
    #[schema(example = true)]
    pub enabled: bool,
    /// Why it's off: `manual` or `failing`. Empty while on.
    #[schema(example = "")]
    pub disabled_reason: String,
    /// Finished deliveries to it in the period.
    #[schema(example = 214)]
    pub deliveries: i64,
    /// Delivered.
    #[schema(example = 202)]
    pub ok: i64,
    /// Failed.
    #[schema(example = 12)]
    pub failed: i64,
    /// Delivered, out of finished. Null with none.
    #[schema(example = 0.9439)]
    pub success_rate: Option<f64>,
    /// Median latency, milliseconds.
    #[schema(example = 320)]
    pub p50_ms: Option<u64>,
    /// 95th percentile latency, milliseconds.
    #[schema(example = 2900)]
    pub p95_ms: Option<u64>,
    /// Failures in a row since its last success.
    #[schema(example = 0)]
    pub streak: i64,
    /// When it last failed.
    #[schema(example = "2026-10-26T09:14:02Z")]
    pub last_failure_at: Option<String>,
    /// The last failure's reason code.
    #[schema(example = "webhook_server_error")]
    pub last_failure_reason: Option<String>,
    /// The last failure's reason in plain words.
    #[schema(example = "Your webhook endpoint answered with a server error (5xx).")]
    pub last_failure_message: Option<String>,
    /// The first failure since its last success, while it's failing.
    #[schema(example = json!(null))]
    pub failing_since: Option<String>,
    /// When it last took a delivery.
    #[schema(example = "2026-10-26T15:02:40Z")]
    pub last_success_at: Option<String>,
}

/// Whether the records from before rollups existed are all counted yet.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Backfill {
    /// False while older records are still being counted; numbers grow until it is true.
    #[schema(example = true)]
    pub complete: bool,
}

/// The Deliveries overview for one period.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "period": "7d",
    "granularity": "day",
    "from": "2026-10-20T00:00:00Z",
    "to": "2026-10-27T00:00:00Z",
    "previous_from": "2026-10-13T00:00:00Z",
    "previous_to": "2026-10-20T00:00:00Z",
    "summary": {"deliveries": 1284, "handled": 1190, "ignored": 61, "failed": 33, "success_rate": 0.9743, "retried": 41, "retried_ok": 37, "retries": 58, "p50_ms": 180, "p95_ms": 1240, "p99_ms": 4800},
    "previous": {"deliveries": 1120, "handled": 1040, "ignored": 52, "failed": 28, "success_rate": 0.975, "retried": 30, "retried_ok": 26, "retries": 44, "p50_ms": 170, "p95_ms": 1100, "p99_ms": 4100},
    "series": [{"start": "2026-10-26T00:00:00Z", "end": "2026-10-27T00:00:00Z", "deliveries": 190, "ok": 186, "failed": 4, "success_rate": 0.9789, "p95_ms": 1100, "previous_deliveries": 171}],
    "sources": [{"source": "github", "direction": "inbound", "deliveries": 912, "ok": 905, "failed": 7, "success_rate": 0.9923, "p95_ms": 640, "previous_deliveries": 870, "previous_success_rate": 0.9897}],
    "reasons": [{"code": "webhook_server_error", "message": "Your webhook endpoint answered with a server error (5xx).", "fix": "Check your endpoint's logs for this event id. CoderHelm retries for about a day.", "count": 12, "previous_count": 3, "sources": ["webhook"]}],
    "events": [{"event": "run.failed", "source": "webhook", "failed": 9}],
    "endpoints": [{"id": "ep_01JBENDPT4K2M8N3P5Q7R9S1T3", "url": "https://hooks.example.com/coderhelm", "description": "Incident bot", "deleted": false, "enabled": true, "disabled_reason": "", "deliveries": 214, "ok": 202, "failed": 12, "success_rate": 0.9439, "p50_ms": 320, "p95_ms": 2900, "streak": 0, "last_failure_at": "2026-10-26T09:14:02Z", "last_failure_reason": "webhook_server_error", "last_failure_message": "Your webhook endpoint answered with a server error (5xx).", "failing_since": null, "last_success_at": "2026-10-26T15:02:40Z"}],
    "seen_sources": ["github", "webhook", "slack", "email"],
    "backfill": {"complete": true},
    "kept_days": 120,
    "records_kept_days": 14
}))]
pub struct DeliveryAnalytics {
    /// `24h`, `7d` or `30d`.
    #[schema(example = "7d")]
    pub period: String,
    /// `hour` or `day`.
    #[schema(example = "day")]
    pub granularity: String,
    /// The period: from (inclusive) to (exclusive), RFC 3339.
    #[schema(example = "2026-10-20T00:00:00Z")]
    pub from: String,
    /// The end of the period, exclusive.
    #[schema(example = "2026-10-27T00:00:00Z")]
    pub to: String,
    /// The period before, as long as this one.
    #[schema(example = "2026-10-13T00:00:00Z")]
    pub previous_from: String,
    /// The end of the period before, exclusive.
    #[schema(example = "2026-10-20T00:00:00Z")]
    pub previous_to: String,
    pub summary: Totals,
    pub previous: Totals,
    /// One point per hour (24h) or day, oldest first.
    pub series: Vec<Point>,
    /// By source and direction, most deliveries first.
    pub sources: Vec<SourceRow>,
    /// Failures by reason, most first.
    pub reasons: Vec<ReasonRow>,
    /// The event types that failed most (up to 10).
    pub events: Vec<EventRow>,
    /// The team's outbound webhook endpoints, and any that had deliveries
    /// in the period since. Empty when another source is chosen.
    pub endpoints: Vec<EndpointRow>,
    /// Every source with deliveries in either period, for the filter.
    #[schema(example = json!(["github", "webhook", "slack", "email"]))]
    pub seen_sources: Vec<String>,
    pub backfill: Backfill,
    /// Days the counts are kept.
    #[schema(example = 120)]
    pub kept_days: i64,
    /// Days each delivery's record is kept (what a drill-in can list).
    #[schema(example = 14)]
    pub records_kept_days: i64,
}

/// An endpoint as the settings table keeps it (only what the overview
/// shows; the signing secret is never read).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EndpointMeta {
    pub id: String,
    pub url: String,
    pub description: String,
    pub enabled: bool,
    pub disabled_reason: String,
    pub failing_since: Option<String>,
    pub last_failure_at: Option<String>,
    pub last_success_at: Option<String>,
}

pub fn endpoint_meta(item: &Item) -> Option<EndpointMeta> {
    let g = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let opt = |k: &str| Some(g(k)).filter(|v| !v.is_empty());
    let id = g("id");
    if id.is_empty() || !g("sk").starts_with(ENDPOINT_SK) {
        return None;
    }
    Some(EndpointMeta {
        id,
        url: g("url"),
        description: g("description"),
        enabled: item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        disabled_reason: g("disabled_reason"),
        failing_since: opt("failing_since"),
        last_failure_at: opt("last_failure_at"),
        last_success_at: opt("last_success_at"),
    })
}

/// Where webhook endpoints are kept (`common::webhooks::endpoint`).
pub const ENDPOINT_SK: &str = "WEBHOOK_ENDPOINT#";

fn direction_word(dir: &str) -> &'static str {
    if dir == "in" {
        "inbound"
    } else {
        "outbound"
    }
}

/// The overview from the rollups (`rows`: sort key to counters), the
/// endpoints and their streaks. Pure: every number is tested.
pub fn build(
    period: Period,
    p: &Plan,
    rows: &BTreeMap<String, Counters>,
    sel: &Selection,
    metas: &[EndpointMeta],
    stats: &[EndpointStat],
) -> DeliveryAnalytics {
    let step = if p.hourly {
        Duration::hours(1)
    } else {
        Duration::days(1)
    };
    let scoped = |t: &DateTime<Utc>| {
        rows.get(&bucket_sk(p.hourly, *t))
            .map(by_scope)
            .unwrap_or_default()
    };
    let cur: Vec<BTreeMap<String, Tally>> = p.current.iter().map(scoped).collect();
    let prev: Vec<BTreeMap<String, Tally>> = p.previous.iter().map(scoped).collect();
    let st = sel.status;

    let pick = |b: &BTreeMap<String, Tally>| {
        let mut t = Tally::default();
        for (scope, x) in b {
            if sel.takes(scope) {
                t.add(&x.only(st));
            }
        }
        t
    };
    let sum = |bs: &[BTreeMap<String, Tally>]| {
        let mut t = Tally::default();
        for b in bs {
            t.add(&pick(b));
        }
        t
    };
    let now_t = sum(&cur);
    let prev_t = sum(&prev);

    let series = p
        .current
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let x = pick(&cur[i]);
            let tt = totals(&x, st);
            Point {
                start: t.to_rfc3339_opts(SecondsFormat::Secs, true),
                end: (*t + step).to_rfc3339_opts(SecondsFormat::Secs, true),
                deliveries: tt.deliveries,
                ok: tt.handled + tt.ignored,
                failed: tt.failed,
                success_rate: tt.success_rate,
                p95_ms: tt.p95_ms,
                previous_deliveries: prev.get(i).map(|b| pick(b).total()).unwrap_or(0),
            }
        })
        .collect();

    // By source and direction (the endpoint filter doesn't narrow these).
    let mut src_now: BTreeMap<(String, String), Tally> = BTreeMap::new();
    let mut src_prev: BTreeMap<(String, String), Tally> = BTreeMap::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (bs, into) in [(&cur, &mut src_now), (&prev, &mut src_prev)] {
        for b in bs.iter() {
            for (scope, x) in b {
                if let Some(src) = scope.strip_prefix("s#").and_then(|r| r.split('#').next()) {
                    if x.total() != 0 {
                        seen.insert(src.to_string());
                    }
                }
                if let Some((src, dir)) = sel.source_of(scope) {
                    into.entry((src.to_string(), dir.to_string()))
                        .or_default()
                        .add(&x.only(st));
                }
            }
        }
    }
    let mut sources: Vec<SourceRow> = src_now
        .iter()
        .filter(|(_, t)| t.total() != 0)
        .map(|((src, dir), t)| {
            let tt = totals(t, st);
            let pt = src_prev
                .get(&(src.clone(), dir.clone()))
                .map(|x| totals(x, st))
                .unwrap_or_default();
            SourceRow {
                source: src.clone(),
                direction: direction_word(dir).into(),
                deliveries: tt.deliveries,
                ok: tt.handled + tt.ignored,
                failed: tt.failed,
                success_rate: tt.success_rate,
                p95_ms: tt.p95_ms,
                previous_deliveries: pt.deliveries,
                previous_success_rate: pt.success_rate,
            }
        })
        .collect();
    sources.sort_by(|a, b| {
        b.deliveries
            .cmp(&a.deliveries)
            .then(a.source.cmp(&b.source))
    });

    // Reasons and events, with the sources they happened on.
    let mut reason_sources: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    let mut events: BTreeMap<(String, String), i64> = BTreeMap::new();
    for b in &cur {
        for (scope, x) in b {
            if !sel.takes(scope) {
                continue;
            }
            let x = x.only(st);
            let src = match scope.strip_prefix("s#") {
                Some(r) => r.split('#').next().unwrap_or("").to_string(),
                None => "webhook".to_string(),
            };
            for (code, n) in &x.reasons {
                *reason_sources
                    .entry(code.clone())
                    .or_default()
                    .entry(src.clone())
                    .or_insert(0) += n;
            }
            for (ev, n) in &x.events {
                *events.entry((src.clone(), ev.clone())).or_insert(0) += n;
            }
        }
    }
    let mut reasons: Vec<ReasonRow> = now_t
        .reasons
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(code, n)| {
            let r = deliveries::reason(code);
            let mut srcs: Vec<(String, i64)> = reason_sources
                .get(code)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
                .unwrap_or_default();
            srcs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            ReasonRow {
                code: code.clone(),
                message: r.message.into(),
                fix: r.fix.into(),
                count: *n,
                previous_count: prev_t.reasons.get(code).copied().unwrap_or(0),
                sources: srcs.into_iter().filter(|s| s.1 > 0).map(|s| s.0).collect(),
            }
        })
        .collect();
    reasons.sort_by(|a, b| b.count.cmp(&a.count).then(a.code.cmp(&b.code)));
    let mut events: Vec<EventRow> = events
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|((source, event), failed)| EventRow {
            event,
            source,
            failed,
        })
        .collect();
    events.sort_by(|a, b| b.failed.cmp(&a.failed).then(a.event.cmp(&b.event)));
    events.truncate(10);

    // Endpoints: every one the team has, plus any with deliveries since deleted.
    let mut endpoints = vec![];
    if sel.shows_endpoints() {
        let mut ep_t: BTreeMap<String, Tally> = BTreeMap::new();
        for b in &cur {
            for (scope, x) in b {
                if let Some(id) = scope.strip_prefix("ep#") {
                    ep_t.entry(id.to_string()).or_default().add(&x.only(st));
                }
            }
        }
        let mut ids: Vec<String> = metas.iter().map(|m| m.id.clone()).collect();
        for (id, t) in &ep_t {
            if t.total() != 0 && !metas.iter().any(|m| rollup::part(&m.id) == *id) {
                ids.push(id.clone());
            }
        }
        for id in ids {
            let meta = metas.iter().find(|m| m.id == id);
            let key = rollup::part(&id);
            let t = ep_t.get(&key).cloned().unwrap_or_default();
            let tt = totals(&t, st);
            let stat = stats.iter().find(|s| rollup::part(&s.endpoint_id) == key);
            let last_failure_at = stat
                .and_then(|s| s.last_failure_at.clone())
                .or_else(|| meta.and_then(|m| m.last_failure_at.clone()));
            let last_failure_reason = stat
                .and_then(|s| s.last_failure_reason.clone())
                .filter(|r| !r.is_empty());
            endpoints.push(EndpointRow {
                id: id.clone(),
                url: meta.map(|m| m.url.clone()).unwrap_or_default(),
                description: meta.map(|m| m.description.clone()).unwrap_or_default(),
                deleted: meta.is_none(),
                enabled: meta.is_some_and(|m| m.enabled),
                disabled_reason: meta.map(|m| m.disabled_reason.clone()).unwrap_or_default(),
                deliveries: tt.deliveries,
                ok: tt.handled + tt.ignored,
                failed: tt.failed,
                success_rate: tt.success_rate,
                p50_ms: tt.p50_ms,
                p95_ms: tt.p95_ms,
                streak: stat.map(|s| s.streak.max(0)).unwrap_or(0),
                last_failure_message: last_failure_reason
                    .as_deref()
                    .map(|r| deliveries::reason(r).message.to_string()),
                last_failure_reason,
                last_failure_at,
                failing_since: meta.and_then(|m| m.failing_since.clone()),
                last_success_at: stat
                    .and_then(|s| s.last_success_at.clone())
                    .or_else(|| meta.and_then(|m| m.last_success_at.clone())),
            });
        }
        endpoints.sort_by(|a, b| {
            b.streak
                .cmp(&a.streak)
                .then(b.failed.cmp(&a.failed))
                .then(b.deliveries.cmp(&a.deliveries))
                .then(a.id.cmp(&b.id))
        });
    }

    let first = |v: &[DateTime<Utc>]| {
        v.first()
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true))
            .unwrap_or_default()
    };
    let end = |v: &[DateTime<Utc>]| {
        v.last()
            .map(|t| (*t + step).to_rfc3339_opts(SecondsFormat::Secs, true))
            .unwrap_or_default()
    };
    DeliveryAnalytics {
        period: period.name().into(),
        granularity: if p.hourly { "hour" } else { "day" }.into(),
        from: first(&p.current),
        to: end(&p.current),
        previous_from: first(&p.previous),
        previous_to: end(&p.previous),
        summary: totals(&now_t, st),
        previous: totals(&prev_t, st),
        series,
        sources,
        reasons,
        events,
        endpoints,
        seen_sources: seen.into_iter().collect(),
        backfill: Backfill { complete: true },
        kept_days: rollup::DAY_RETENTION_DAYS,
        records_kept_days: deliveries::RETENTION_DAYS,
    }
}

/// Adds rows into `into` (counters of the same item add up).
pub fn merge_rows(into: &mut BTreeMap<String, Counters>, rows: Vec<(String, Counters)>) {
    for (sk, c) in rows {
        let e = into.entry(sk).or_default();
        for (k, v) in c {
            *e.entry(k).or_insert(0) += v;
        }
    }
}

/// Only the hosted relay's own counters (relay forwards and the broker):
/// what an install adds from CoderHelm's side.
pub fn relay_only(c: Counters) -> Counters {
    c.into_iter()
        .filter(|(k, _)| k.starts_with("s#relay#") || k.starts_with("s#broker#"))
        .collect()
}

// ── Backfill ───────────────────────────────────────────────────────────

fn store(state: &AppState) -> rollup::Dynamo<'_> {
    rollup::Dynamo {
        client: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

async fn query_unrolled(
    state: &AppState,
    table: &str,
    key_expr: &str,
    values: Vec<(&str, AttributeValue)>,
) -> Result<Vec<Item>, String> {
    let mut out = vec![];
    let mut start = None;
    loop {
        let mut q = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression(key_expr)
            .filter_expression("attribute_not_exists(#rolled)")
            .expression_attribute_names("#rolled", rollup::ROLLED)
            .set_exclusive_start_key(start);
        for (k, v) in &values {
            q = q.expression_attribute_values(*k, v.clone());
        }
        let r = q
            .send()
            .await
            .map_err(|e| format!("{e:?}").chars().take(300).collect::<String>())?;
        out.extend(r.items().iter().cloned());
        start = r.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    Ok(out)
}

fn key_of(item: &Item, names: &[&str]) -> HashMap<String, AttributeValue> {
    names
        .iter()
        .filter_map(|n| item.get(*n).map(|v| (n.to_string(), v.clone())))
        .collect()
}

/// A Jira event log item as a delivery (the gateway's list mapping).
pub fn jira_item_to_delivery(item: &Item) -> Option<Delivery> {
    let mut v = serde_json::Map::new();
    for (k, a) in item {
        if let Ok(s) = a.as_s() {
            v.insert(k.clone(), Value::String(s.clone()));
        }
    }
    super::deliveries::jira_to_delivery(&Value::Object(v))
}

/// The records of `pk` (and, for a team, its alert events and Jira event
/// log) that rollups haven't counted yet.
async fn unrolled(state: &AppState, pk: &str, team: bool) -> Result<Vec<Unrolled>, String> {
    let settings = state.config.settings_table_name.clone();
    let s = |v: &str| AttributeValue::S(v.to_string());
    let mut out = vec![];
    for item in query_unrolled(
        state,
        &settings,
        "pk = :pk AND begins_with(sk, :p)",
        vec![(":pk", s(pk)), (":p", s(deliveries::SK_PREFIX))],
    )
    .await?
    {
        if let Some(d) = deliveries::from_item(&item) {
            out.push(Unrolled {
                mark: MarkKey {
                    table: settings.clone(),
                    key: key_of(&item, &["pk", "sk"]),
                },
                delivery: d,
            });
        }
    }
    if !team {
        return Ok(out);
    }
    for item in query_unrolled(
        state,
        &settings,
        "pk = :pk AND begins_with(sk, :p)",
        vec![(":pk", s(pk)), (":p", s("ALERT_EVENT#"))],
    )
    .await?
    {
        if let Some(d) = super::deliveries::alert_to_delivery(&item) {
            out.push(Unrolled {
                mark: MarkKey {
                    table: settings.clone(),
                    key: key_of(&item, &["pk", "sk"]),
                },
                delivery: d,
            });
        }
    }
    let jira = state.config.jira_events_table_name.clone();
    if !jira.is_empty() {
        for item in query_unrolled(state, &jira, "team_id = :t", vec![(":t", s(pk))]).await? {
            let dedup = item
                .get("event_id")
                .and_then(|v| v.as_s().ok())
                .is_some_and(|e| e.starts_with("DEDUP#"));
            if dedup {
                continue;
            }
            if let Some(d) = jira_item_to_delivery(&item) {
                out.push(Unrolled {
                    mark: MarkKey {
                        table: jira.clone(),
                        key: key_of(&item, &["team_id", "event_id"]),
                    },
                    delivery: d,
                });
            }
        }
    }
    Ok(out)
}

/// Counts `pk`'s records from before rollups existed, once, until
/// `deadline`. True once every one is counted (and from then on, at the cost
/// of one read).
pub async fn ensure_backfilled(
    state: &AppState,
    pk: &str,
    team: bool,
    deadline: std::time::Instant,
) -> bool {
    let st = store(state);
    match st.backfilled(pk).await {
        Ok(true) => return true,
        Ok(false) => {}
        Err(e) => {
            warn!(error = %e, "Could not read the delivery rollup backfill marker");
            return false;
        }
    }
    let items = match unrolled(state, pk, team).await {
        Ok(i) => i,
        Err(e) => {
            warn!(error = %e, "Could not list records for the delivery rollup backfill");
            return false;
        }
    };
    let report = rollup::backfill(&st, pk, items, Utc::now(), deadline).await;
    tracing::info!(
        counted = report.counted,
        skipped = report.skipped,
        left = report.left,
        "Delivery rollup backfill"
    );
    if report.left > 0 {
        return false;
    }
    let at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    if let Err(e) = st.set_backfilled(pk, &at).await {
        warn!(error = %e, "Could not mark the delivery rollup backfill done");
    }
    true
}

/// The rollup rows of `pk` for a plan (the previous period's first bucket to
/// this one's last).
pub async fn read_rows(
    state: &AppState,
    pk: &str,
    p: &Plan,
) -> Result<Vec<(String, Counters)>, String> {
    let (Some(a), Some(b)) = (p.previous.first(), p.current.last()) else {
        return Ok(vec![]);
    };
    store(state)
        .read(pk, &bucket_sk(p.hourly, *a), &bucket_sk(p.hourly, *b))
        .await
}

async fn endpoint_metas(state: &AppState, team: &str) -> Vec<EndpointMeta> {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team.into()))
        .expression_attribute_values(":p", AttributeValue::S(ENDPOINT_SK.into()))
        .projection_expression(
            "pk, sk, id, #u, description, enabled, disabled_reason, failing_since, last_failure_at, last_success_at",
        )
        .expression_attribute_names("#u", "url")
        .send()
        .await;
    match r {
        Ok(r) => r.items().iter().filter_map(endpoint_meta).collect(),
        Err(e) => {
            warn!(error = %e, "Could not read webhook endpoints for the overview");
            vec![]
        }
    }
}

/// The hosted relay's rollups for this install (hybrid only).
async fn relayed_rows(state: &AppState, p: &Plan) -> Vec<(String, Counters)> {
    let Some(creds) = common::hybrid::client::credentials() else {
        return vec![];
    };
    let (Some(a), Some(b)) = (p.previous.first(), p.current.last()) else {
        return vec![];
    };
    let body = json!({ "from": bucket_sk(p.hourly, *a), "to": bucket_sk(p.hourly, *b) });
    match common::hybrid::client::call(
        &state.http,
        &creds,
        "POST",
        "/hybrid/deliveries/rollups",
        Some(&body),
    )
    .await
    {
        Ok(v) => parse_relayed(&v),
        Err(e) => {
            warn!(error = %e.message, "Could not read relay rollups from CoderHelm");
            vec![]
        }
    }
}

/// The hosted side's reply, as rows (relay counters only).
pub fn parse_relayed(v: &Value) -> Vec<(String, Counters)> {
    v["buckets"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|b| {
                    let sk = b["sk"].as_str()?;
                    if !sk.starts_with(DAY_PREFIX) && !sk.starts_with(HOUR_PREFIX) {
                        return None;
                    }
                    let c: Counters = b["counters"]
                        .as_object()?
                        .iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_i64()?)))
                        .collect();
                    Some((sk.to_string(), relay_only(c)))
                })
                .collect()
        })
        .unwrap_or_default()
}

// ── The route ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AnalyticsQuery {
    /// `24h` (by hour), `7d` or `30d` (by day). Default `7d`. Each is
    /// compared with the period before it, as long.
    #[param(example = "7d")]
    period: Option<String>,
    /// Only one source: `github`, `relay`, `webhook`, `alerts`, `jira`,
    /// `slack`, `teams`, `email`, `broker`, `knowledge`.
    #[param(example = "webhook")]
    source: Option<String>,
    /// `all` (default), `ok` (handled or ignored), `handled`, `ignored` or
    /// `failed`. Retrying deliveries count once they finish.
    #[param(example = "failed")]
    status: Option<String>,
    /// Only one outbound webhook endpoint (its id).
    #[param(example = "ep_01JBENDPT4K2M8N3P5Q7R9S1T3")]
    endpoint: Option<String>,
}

/// How long one request spends counting older records before it answers.
const BACKFILL_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// GET /api/deliveries/analytics
#[utoipa::path(
    get,
    path = "/api/deliveries/analytics",
    tag = "Deliveries",
    operation_id = "getDeliveryAnalytics",
    summary = "Get delivery analytics",
    description = "How the team's deliveries went over a period, against the period before: \
        webhooks CoderHelm received (GitHub, Jira, alerts), events it relayed, and what it sent \
        (webhook endpoints, Slack, Teams, email). Per hour for `24h`, per day otherwise: volume, \
        success rate, latency percentiles, retries and how many of them went fine, failures by \
        reason in plain words, the event types that failed most, and each webhook endpoint's \
        success rate, latency, failure streak and whether it is turned off. Counts are kept \
        120 days; each delivery's own record 14.",
    params(AnalyticsQuery),
    responses(
        (status = 200, description = "The overview.", body = DeliveryAnalytics),
        (status = 400, description = "An unknown `period` or `status`.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["deliveries"])),
)]
pub async fn analytics(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<DeliveryAnalytics>, Fail> {
    let allowed = claims.require_role(3);
    allowed.map_err(|c| fail(c, "forbidden", "Admins only."))?;
    let period = Period::parse(q.period.as_deref()).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "period is 24h, 7d or 30d.",
        )
    })?;
    let status = StatusSel::parse(q.status.as_deref()).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "status is all, ok, handled, ignored or failed.",
        )
    })?;
    let clean = |v: Option<String>| {
        v.map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && s != "all")
    };
    let sel = Selection {
        source: clean(q.source).map(|s| rollup::part(&s)),
        status,
        endpoint: clean(q.endpoint),
    };
    let team = claims.team_id.clone();
    let unrouted = super::setup_checks::self_hosted();
    let deadline = std::time::Instant::now() + BACKFILL_BUDGET;
    let (own_done, unrouted_done) =
        tokio::join!(ensure_backfilled(&state, &team, true, deadline), async {
            !unrouted
                || ensure_backfilled(&state, super::deliveries::UNROUTED, false, deadline).await
        });

    let p = plan(period, Utc::now());
    let st = store(&state);
    let (own, other, relayed, metas, stats) = tokio::join!(
        read_rows(&state, &team, &p),
        async {
            if unrouted {
                read_rows(&state, super::deliveries::UNROUTED, &p)
                    .await
                    .unwrap_or_default()
            } else {
                vec![]
            }
        },
        relayed_rows(&state, &p),
        endpoint_metas(&state, &team),
        st.endpoints(&team),
    );
    let own = own.map_err(|e| {
        warn!(error = %e, "Could not read delivery rollups");
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "The delivery counts couldn't be read. Try again in a minute.",
        )
    })?;
    let mut rows = BTreeMap::new();
    merge_rows(&mut rows, own);
    merge_rows(&mut rows, other);
    merge_rows(&mut rows, relayed);
    let mut out = build(period, &p, &rows, &sel, &metas, &stats.unwrap_or_default());
    out.backfill.complete = own_done && unrouted_done;
    Ok(Json(out))
}

/// The hosted relay's rollups for one install, between two rollup sort
/// keys: what `POST /hybrid/deliveries/rollups` answers. Counts the install's
/// older relay records first, a few seconds' worth.
pub async fn install_rollups(state: &AppState, pk: &str, from: &str, to: &str) -> Value {
    let ok_key = |k: &str| k.starts_with(DAY_PREFIX) || k.starts_with(HOUR_PREFIX);
    if !ok_key(from) || !ok_key(to) || from > to {
        return json!({ "buckets": [] });
    }
    ensure_backfilled(
        state,
        pk,
        false,
        std::time::Instant::now() + BACKFILL_BUDGET,
    )
    .await;
    let rows = store(state).read(pk, from, to).await.unwrap_or_default();
    json!({
        "buckets": rows
            .into_iter()
            .map(|(sk, c)| json!({ "sk": sk, "counters": relay_only(c) }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::deliveries::{Attempt, Direction, Status};

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn periods_cover_themselves_and_the_one_before() {
        let now = t("2026-10-26T15:42:10Z");
        let day = plan(Period::Day, now);
        assert!(day.hourly);
        assert_eq!(day.current.len(), 24);
        assert_eq!(day.current.last().unwrap(), &t("2026-10-26T15:00:00Z"));
        assert_eq!(day.current.first().unwrap(), &t("2026-10-25T16:00:00Z"));
        assert_eq!(day.previous.last().unwrap(), &t("2026-10-25T15:00:00Z"));
        assert_eq!(day.previous.first().unwrap(), &t("2026-10-24T16:00:00Z"));
        let week = plan(Period::Week, now);
        assert!(!week.hourly);
        assert_eq!(week.current.first().unwrap(), &t("2026-10-20T00:00:00Z"));
        assert_eq!(week.current.last().unwrap(), &t("2026-10-26T00:00:00Z"));
        assert_eq!(week.previous.first().unwrap(), &t("2026-10-13T00:00:00Z"));
        assert_eq!(week.previous.len(), 7);
        let month = plan(Period::Month, now);
        assert_eq!(month.current.len(), 30);
        assert_eq!(month.previous.first().unwrap(), &t("2026-08-28T00:00:00Z"));
        // 60 days of daily rollups fit in what is kept.
        const { assert!(60 < rollup::DAY_RETENTION_DAYS) };
        const { assert!(48 < rollup::HOUR_RETENTION_HOURS) };
        assert_eq!(Period::parse(None), Some(Period::Week));
        assert_eq!(Period::parse(Some("90d")), None);
        assert_eq!(StatusSel::parse(Some("pending")), None);
    }

    fn delivery(at: &str, source: &str, dir: Direction, status: Status, ms: u64) -> Delivery {
        let mut d = Delivery::new(dir, source, "pull_request.opened", "x");
        d.created_at = at.into();
        d.status = status;
        d.duration_ms = ms;
        if status == Status::Failed {
            d.reason = "handler_error".into();
        }
        d
    }

    /// Rows as the write path makes them, through the in-memory store.
    fn rows_of(pk: &str, ds: &[Delivery], now: DateTime<Utc>) -> BTreeMap<String, Counters> {
        let m = rollup::Memory::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            for d in ds {
                rollup::apply_at(&m, pk, None, d, now).await;
            }
            let mut rows = BTreeMap::new();
            merge_rows(
                &mut rows,
                m.read(pk, "DELIVERY_ROLLUP#", "DELIVERY_ROLLUP#~")
                    .await
                    .unwrap(),
            );
            rows
        })
    }

    fn all() -> Selection {
        Selection {
            source: None,
            status: StatusSel::All,
            endpoint: None,
        }
    }

    #[test]
    fn the_week_adds_up_and_compares_with_the_week_before() {
        let now = t("2026-10-26T15:00:00Z");
        let mut ds = vec![];
        for i in 0..18 {
            ds.push(delivery(
                "2026-10-26T09:00:00Z",
                "github",
                Direction::Inbound,
                Status::Handled,
                100 + i,
            ));
        }
        ds.push(delivery(
            "2026-10-26T09:00:00Z",
            "github",
            Direction::Inbound,
            Status::Ignored,
            0,
        ));
        ds.push(delivery(
            "2026-10-24T09:00:00Z",
            "github",
            Direction::Inbound,
            Status::Failed,
            900,
        ));
        for _ in 0..5 {
            ds.push(delivery(
                "2026-10-15T09:00:00Z",
                "slack",
                Direction::Outbound,
                Status::Handled,
                300,
            ));
        }
        ds.push(delivery(
            "2026-10-15T09:00:00Z",
            "slack",
            Direction::Outbound,
            Status::Failed,
            300,
        ));
        // Before the previous week: left out.
        ds.push(delivery(
            "2026-10-01T09:00:00Z",
            "slack",
            Direction::Outbound,
            Status::Failed,
            300,
        ));
        let rows = rows_of("TEAM#1", &ds, now);
        let p = plan(Period::Week, now);
        let a = build(Period::Week, &p, &rows, &all(), &[], &[]);
        assert_eq!(a.summary.deliveries, 20);
        assert_eq!(
            (a.summary.handled, a.summary.ignored, a.summary.failed),
            (18, 1, 1)
        );
        assert_eq!(a.summary.success_rate, Some(0.95));
        assert_eq!(a.previous.deliveries, 6);
        assert_eq!(a.previous.success_rate, Some(0.8333));
        assert_eq!(a.series.len(), 7);
        assert_eq!(a.series[6].deliveries, 19, "today");
        assert_eq!(a.series[4].failed, 1, "two days ago");
        assert_eq!(
            a.series[2].previous_deliveries, 6,
            "Oct 22 lines up with Oct 15"
        );
        assert_eq!(a.from, "2026-10-20T00:00:00Z");
        assert_eq!(a.to, "2026-10-27T00:00:00Z");
        assert_eq!(a.previous_to, "2026-10-20T00:00:00Z");
        assert_eq!(
            a.sources.len(),
            1,
            "only sources with deliveries in the period"
        );
        assert_eq!(a.sources[0].source, "github");
        assert_eq!(a.sources[0].direction, "inbound");
        assert_eq!(a.seen_sources, vec!["github", "slack"]);
        assert_eq!(a.reasons.len(), 1);
        assert_eq!(a.reasons[0].code, "handler_error");
        assert_eq!(a.reasons[0].count, 1);
        assert_eq!(a.reasons[0].previous_count, 1);
        assert_eq!(a.reasons[0].sources, vec!["github"]);
        assert!(a.reasons[0].message.ends_with('.'));
        assert_eq!(a.events[0].event, "pull_request.opened");
        // Latency: 18 in (100, 200], one in (750, 1000]; the ignored one wasn't timed.
        assert_eq!(a.summary.p50_ms, Some(150));
        assert!(a.summary.p95_ms.unwrap() > 750, "{:?}", a.summary.p95_ms);
        assert_eq!(a.summary.retries, Some(0));
    }

    #[test]
    fn filters_narrow_every_number() {
        let now = t("2026-10-26T15:00:00Z");
        let mut ok = delivery(
            "2026-10-26T10:00:00Z",
            "webhook",
            Direction::Outbound,
            Status::Handled,
            400,
        );
        ok.endpoint_id = "ep_a".into();
        ok.attempts = vec![Attempt::default(), Attempt::default(), Attempt::default()];
        let mut bad = delivery(
            "2026-10-26T10:00:00Z",
            "webhook",
            Direction::Outbound,
            Status::Failed,
            9_000,
        );
        bad.endpoint_id = "ep_b".into();
        bad.reason = "webhook_timeout".into();
        bad.attempts = vec![Attempt::default(), Attempt::default()];
        let gh = delivery(
            "2026-10-26T10:00:00Z",
            "github",
            Direction::Inbound,
            Status::Handled,
            50,
        );
        let rows = rows_of("TEAM#1", &[ok, bad, gh], now);
        let p = plan(Period::Day, now);

        let a = build(Period::Day, &p, &rows, &all(), &[], &[]);
        assert_eq!(a.summary.deliveries, 3);
        assert_eq!(
            (a.summary.retried, a.summary.retried_ok, a.summary.retries),
            (2, 1, Some(3))
        );
        assert_eq!(a.granularity, "hour");
        assert_eq!(a.series.len(), 24);
        assert_eq!(a.series.iter().map(|s| s.deliveries).sum::<i64>(), 3);

        let failed = Selection {
            status: StatusSel::Failed,
            ..all()
        };
        let f = build(Period::Day, &p, &rows, &failed, &[], &[]);
        assert_eq!((f.summary.deliveries, f.summary.failed), (1, 1));
        assert_eq!(
            (f.summary.retried, f.summary.retried_ok, f.summary.retries),
            (1, 0, None)
        );
        assert_eq!(
            f.summary.p50_ms,
            Some(10_000),
            "only the failed one's latency (its bucket's top)"
        );
        assert_eq!(f.summary.success_rate, Some(0.0));

        let okay = Selection {
            status: StatusSel::Ok,
            ..all()
        };
        let o = build(Period::Day, &p, &rows, &okay, &[], &[]);
        assert_eq!((o.summary.deliveries, o.summary.failed), (2, 0));
        assert!(o.reasons.is_empty());

        let gh_only = Selection {
            source: Some("github".into()),
            ..all()
        };
        let g = build(Period::Day, &p, &rows, &gh_only, &[], &[]);
        assert_eq!(g.summary.deliveries, 1);
        assert!(
            g.endpoints.is_empty(),
            "endpoints hide when another source is chosen"
        );

        let one = Selection {
            endpoint: Some("ep_b".into()),
            ..all()
        };
        let e = build(Period::Day, &p, &rows, &one, &[], &[]);
        assert_eq!((e.summary.deliveries, e.summary.failed), (1, 1));
        assert_eq!(e.reasons[0].code, "webhook_timeout");
        assert_eq!(e.reasons[0].sources, vec!["webhook"]);
    }

    #[test]
    fn endpoints_show_rates_streaks_and_whether_they_are_on() {
        let now = t("2026-10-26T15:00:00Z");
        let mut ds = vec![];
        for i in 0..4 {
            let mut d = delivery(
                "2026-10-26T10:00:00Z",
                "webhook",
                Direction::Outbound,
                if i == 0 {
                    Status::Failed
                } else {
                    Status::Handled
                },
                300,
            );
            d.endpoint_id = "ep_on".into();
            if i == 0 {
                d.reason = "webhook_server_error".into();
            }
            ds.push(d);
        }
        let mut gone = delivery(
            "2026-10-25T10:00:00Z",
            "webhook",
            Direction::Outbound,
            Status::Failed,
            0,
        );
        gone.endpoint_id = "ep_gone".into();
        ds.push(gone);
        let rows = rows_of("TEAM#1", &ds, now);
        let metas = vec![
            EndpointMeta {
                id: "ep_on".into(),
                url: "https://hooks.example.com/a".into(),
                enabled: true,
                ..Default::default()
            },
            EndpointMeta {
                id: "ep_off".into(),
                url: "https://hooks.example.com/b".into(),
                enabled: false,
                disabled_reason: "failing".into(),
                failing_since: Some("2026-10-20T00:00:00Z".into()),
                ..Default::default()
            },
        ];
        let stats = vec![EndpointStat {
            endpoint_id: "ep_off".into(),
            streak: 40,
            last_failure_at: Some("2026-10-26T01:00:00Z".into()),
            last_failure_reason: Some("webhook_unreachable".into()),
            ..Default::default()
        }];
        let a = build(
            Period::Week,
            &plan(Period::Week, now),
            &rows,
            &all(),
            &metas,
            &stats,
        );
        let ids: Vec<&str> = a.endpoints.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["ep_off", "ep_on", "ep_gone"],
            "the longest streak first, then failures, then volume"
        );
        let off = &a.endpoints[0];
        assert!(!off.enabled && off.disabled_reason == "failing" && !off.deleted);
        assert_eq!(off.deliveries, 0, "no traffic while off");
        assert_eq!(off.success_rate, None);
        assert_eq!(off.streak, 40);
        assert!(off.last_failure_message.is_some());
        assert_eq!(off.failing_since.as_deref(), Some("2026-10-20T00:00:00Z"));
        let gone = &a.endpoints[2];
        assert!(gone.deleted && gone.url.is_empty());
        let on = &a.endpoints[1];
        assert_eq!((on.deliveries, on.ok, on.failed), (4, 3, 1));
        assert_eq!(on.success_rate, Some(0.75));
        assert_eq!(on.p50_ms, Some(250));
    }

    #[test]
    fn empty_periods_read_as_nothing_not_zero_percent() {
        let now = t("2026-10-26T15:00:00Z");
        let a = build(
            Period::Month,
            &plan(Period::Month, now),
            &BTreeMap::new(),
            &all(),
            &[],
            &[],
        );
        assert_eq!(
            a.summary,
            Totals {
                retries: Some(0),
                ..Default::default()
            }
        );
        assert_eq!(a.summary.success_rate, None);
        assert_eq!(a.series.len(), 30);
        assert!(a.series.iter().all(|s| s.success_rate.is_none()));
        assert!(a.sources.is_empty() && a.reasons.is_empty() && a.endpoints.is_empty());
    }

    #[test]
    fn a_team_reads_only_its_own_rollups() {
        let now = t("2026-10-26T15:00:00Z");
        let a_rows = rows_of(
            "TEAM#a",
            &[delivery(
                "2026-10-26T10:00:00Z",
                "email",
                Direction::Outbound,
                Status::Failed,
                10,
            )],
            now,
        );
        let b_rows = rows_of("TEAM#b", &[], now);
        let p = plan(Period::Week, now);
        assert_eq!(
            build(Period::Week, &p, &a_rows, &all(), &[], &[])
                .summary
                .deliveries,
            1
        );
        assert_eq!(
            build(Period::Week, &p, &b_rows, &all(), &[], &[])
                .summary
                .deliveries,
            0
        );
    }

    #[test]
    fn an_install_adds_only_the_relays_counters_from_coderhelm() {
        let v = json!({"buckets": [
            {"sk": "DELIVERY_ROLLUP#D#2026-10-26", "counters": {"s#relay#out#handled": 4, "s#github#in#handled": 99, "s#broker#out#failed": 1}},
            {"sk": "TEAM#x", "counters": {"s#relay#out#handled": 1}},
            {"sk": "DELIVERY_ROLLUP#D#2026-10-25"}
        ]});
        let rows = parse_relayed(&v);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1,
            Counters::from([
                ("s#relay#out#handled".into(), 4),
                ("s#broker#out#failed".into(), 1)
            ])
        );
        let mut all_rows = BTreeMap::new();
        merge_rows(
            &mut all_rows,
            vec![(
                "DELIVERY_ROLLUP#D#2026-10-26".into(),
                Counters::from([("s#relay#out#handled".into(), 1)]),
            )],
        );
        merge_rows(&mut all_rows, rows);
        assert_eq!(
            all_rows["DELIVERY_ROLLUP#D#2026-10-26"]["s#relay#out#handled"],
            5
        );
    }

    #[test]
    fn endpoint_items_are_read_without_their_secrets() {
        let s = |v: &str| AttributeValue::S(v.into());
        let item = HashMap::from([
            ("sk".to_string(), s("WEBHOOK_ENDPOINT#ep_1")),
            ("id".to_string(), s("ep_1")),
            ("url".to_string(), s("https://hooks.example.com/x")),
            ("enabled".to_string(), AttributeValue::Bool(true)),
            ("secret_sealed".to_string(), s("sealed")),
            ("failing_since".to_string(), s("")),
        ]);
        let m = endpoint_meta(&item).unwrap();
        assert_eq!(
            (m.id.as_str(), m.enabled, m.failing_since.as_deref()),
            ("ep_1", true, None)
        );
        assert!(!format!("{m:?}").contains("sealed"));
        let mut other = item.clone();
        other.insert("sk".into(), s("DELIVERY#1"));
        assert!(endpoint_meta(&other).is_none());
    }

    #[test]
    fn jira_log_items_map_like_the_list() {
        let s = |v: &str| AttributeValue::S(v.into());
        let item = HashMap::from([
            ("team_id".to_string(), s("TEAM#1")),
            ("event_id".to_string(), s("01J")),
            ("event_type".to_string(), s("issue_updated")),
            ("status".to_string(), s("token_limit")),
            ("created_at".to_string(), s("2026-10-26T10:00:00Z")),
            ("expires_at".to_string(), AttributeValue::N("1".into())),
        ]);
        let d = jira_item_to_delivery(&item).unwrap();
        assert_eq!(
            (d.source.as_str(), d.status, d.reason.as_str()),
            ("jira", Status::Failed, "jira_limit")
        );
    }
}
