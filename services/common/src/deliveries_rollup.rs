//! Delivery rollups: counters per hour and per day, written when a delivery
//! record is written, so the Deliveries overview reads a handful of items
//! instead of scanning records (which expire after 14 days anyway).
//!
//! Every write of a record ([`crate::deliveries::record`]) replaces the item
//! and gets the one it replaced back. The rollups then take the difference
//! between what the new record counts for and what the old one counted for
//! ([`changes`]). The differences telescope: however many times a record is
//! rewritten (retried, redelivered, resent), and in whatever order two writers
//! race, the rollups end up counting its last state once.
//!
//! A record counts only once it is finished (handled, ignored or failed). It
//! counts under two scopes: its source and direction (`s#github#in`), and,
//! for an outbound webhook, its endpoint (`ep#ep_123`). Under each scope:
//! handled, ignored and failed counts; retried records, extra attempts and
//! retried records that went fine; a latency histogram ([`LATENCY_BOUNDS_MS`],
//! kept apart for records that went fine and those that failed);
//! and, for failures, the reason code and the event type.
//!
//! Items on the settings table, beside the records:
//! - `DELIVERY_ROLLUP#D#<yyyy-mm-dd>`: a UTC day, kept [`DAY_RETENTION_DAYS`].
//! - `DELIVERY_ROLLUP#H#<yyyy-mm-ddThh>`: a UTC hour, kept [`HOUR_RETENTION_HOURS`].
//! - `DELIVERY_ENDPOINT#<endpoint id>`: an endpoint's failure streak and its
//!   last failure and success.
//! - `DELIVERY_ROLLUP#BACKFILL`: written once the records that were written
//!   before rollups existed have been counted ([`backfill`]).
//!
//! A record counted by the rollups carries `rolled`. Records without it were
//! written before; [`backfill`] counts each of them once, in a transaction
//! that sets `rolled` only where it is still missing, so a record rewritten
//! meanwhile (the write path counts it) is never counted twice.

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::{AttributeValue, TransactWriteItem, Update};
use chrono::{DateTime, Duration, Utc};

use crate::deliveries::{Delivery, Direction, Status};

pub const DAY_PREFIX: &str = "DELIVERY_ROLLUP#D#";
pub const HOUR_PREFIX: &str = "DELIVERY_ROLLUP#H#";
pub const ENDPOINT_PREFIX: &str = "DELIVERY_ENDPOINT#";
pub const BACKFILL_SK: &str = "DELIVERY_ROLLUP#BACKFILL";
/// The attribute a counted record carries.
pub const ROLLED: &str = "rolled";
/// Days a day rollup is kept: 90 days and a comparison period's worth more.
pub const DAY_RETENTION_DAYS: i64 = 120;
/// Hours an hour rollup is kept: the last 24 hours, the 24 before, and slack.
pub const HOUR_RETENTION_HOURS: i64 = 72;

/// Upper bounds (inclusive) of the latency histogram's buckets, in
/// milliseconds. Anything slower falls in a last, open bucket (`inf`).
pub const LATENCY_BOUNDS_MS: &[u64] = &[
    25, 50, 100, 200, 300, 500, 750, 1_000, 1_500, 2_000, 3_000, 5_000, 7_500, 10_000, 15_000,
    30_000, 60_000,
];

/// Counter name to amount. Names are DynamoDB attribute names.
pub type Counters = BTreeMap<String, i64>;

/// A key part as it may appear in a counter name: lowercase letters, digits,
/// `.`, `_` and `-`, at most 64 characters (`#` separates parts).
pub fn part(raw: &str) -> String {
    let p: String = raw
        .trim()
        .chars()
        .take(64)
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if p.is_empty() {
        "none".into()
    } else {
        p
    }
}

/// The latency bucket's name: its upper bound, or `inf`.
pub fn latency_bucket(ms: u64) -> String {
    LATENCY_BOUNDS_MS
        .iter()
        .find(|b| ms <= **b)
        .map(|b| b.to_string())
        .unwrap_or_else(|| "inf".into())
}

/// How a finished record counts.
pub fn class(status: Status) -> Option<&'static str> {
    match status {
        Status::Handled => Some("handled"),
        Status::Ignored => Some("ignored"),
        Status::Failed => Some("failed"),
        Status::Pending | Status::Received => None,
    }
}

fn dir(d: Direction) -> &'static str {
    match d {
        Direction::Inbound => "in",
        Direction::Outbound => "out",
    }
}

/// The scope of a source and direction: `s#<source>#<in|out>`.
pub fn source_scope(source: &str, direction: Direction) -> String {
    format!("s#{}#{}", part(source), dir(direction))
}

/// The scope of a webhook endpoint: `ep#<endpoint id>`.
pub fn endpoint_scope(endpoint_id: &str) -> String {
    format!("ep#{}", part(endpoint_id))
}

/// What one record counts for (nothing until it is finished).
pub fn contribution(d: &Delivery) -> Counters {
    let mut c = Counters::new();
    let Some(class) = class(d.status) else {
        return c;
    };
    let mut scopes = vec![source_scope(&d.source, d.direction)];
    if !d.endpoint_id.is_empty() {
        scopes.push(endpoint_scope(&d.endpoint_id));
    }
    let attempts = d.attempts.len().max(1) as i64;
    for s in scopes {
        c.insert(format!("{s}#{class}"), 1);
        if attempts > 1 {
            c.insert(format!("{s}#retried"), 1);
            c.insert(format!("{s}#retries"), attempts - 1);
            if d.status.is_ok() {
                c.insert(format!("{s}#retried_ok"), 1);
            }
        }
        if d.duration_ms > 0 {
            let how = if d.status.is_ok() { "ok" } else { "failed" };
            c.insert(
                format!("{s}#lat#{how}#{}", latency_bucket(d.duration_ms)),
                1,
            );
        }
        if d.status == Status::Failed {
            let reason = if d.reason.is_empty() {
                "unknown"
            } else {
                &d.reason
            };
            c.insert(format!("{s}#r#{}", part(reason)), 1);
            c.insert(format!("{s}#e#{}", part(&d.event)), 1);
        }
    }
    c
}

/// The UTC day and hour a record counts in (by when it was created, which
/// never changes when it is rewritten).
pub fn bucket_time(d: &Delivery) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&d.created_at)
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

pub fn day_key(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

pub fn hour_key(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H").to_string()
}

/// One rollup item's change: its sort key, the counters to add, and the
/// item's expiry (epoch seconds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub sk: String,
    pub add: Counters,
    pub ttl: i64,
}

/// The rollup items a record at `t` counts in that are still kept at `now`.
fn rollup_items(t: DateTime<Utc>, now: DateTime<Utc>) -> Vec<(String, i64)> {
    let mut out = vec![];
    let day_end = t
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|d| d.and_utc() + Duration::days(1))
        .unwrap_or(t);
    let day_ttl = (day_end + Duration::days(DAY_RETENTION_DAYS)).timestamp();
    if day_ttl > now.timestamp() {
        out.push((format!("{DAY_PREFIX}{}", day_key(t)), day_ttl));
    }
    let hour_ttl = (t + Duration::hours(HOUR_RETENTION_HOURS + 1)).timestamp();
    if hour_ttl > now.timestamp() {
        out.push((format!("{HOUR_PREFIX}{}", hour_key(t)), hour_ttl));
    }
    out
}

fn merge_into(into: &mut Counters, from: &Counters, sign: i64) {
    for (k, v) in from {
        *into.entry(k.clone()).or_insert(0) += sign * v;
    }
}

/// What the rollups must add when `old` (the record as last counted, or
/// None) becomes `new`: the new record's counters in its buckets less the
/// old one's in its own. Unchanged counters and empty items are left out.
pub fn changes(old: Option<&Delivery>, new: &Delivery, now: DateTime<Utc>) -> Vec<Change> {
    let mut by_sk: BTreeMap<String, (Counters, i64)> = BTreeMap::new();
    let mut put = |d: &Delivery, sign: i64| {
        let c = contribution(d);
        if c.is_empty() {
            return;
        }
        for (sk, ttl) in rollup_items(bucket_time(d), now) {
            let e = by_sk.entry(sk).or_insert_with(|| (Counters::new(), ttl));
            e.1 = e.1.max(ttl);
            merge_into(&mut e.0, &c, sign);
        }
    };
    if let Some(o) = old {
        put(o, -1);
    }
    put(new, 1);
    by_sk
        .into_iter()
        .filter_map(|(sk, (mut add, ttl))| {
            add.retain(|_, v| *v != 0);
            (!add.is_empty()).then_some(Change { sk, add, ttl })
        })
        .collect()
}

/// What a record's write means for its endpoint's streak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointChange {
    /// One more failure in a row.
    Failed {
        at: String,
        reason: String,
        event: String,
    },
    /// A success: the streak is over.
    Succeeded { at: String },
}

/// The endpoint streak change when `old` becomes `new`, if any: only a record
/// that just finished (or just changed between failed and fine) counts.
pub fn endpoint_change(
    old: Option<&Delivery>,
    new: &Delivery,
    now: DateTime<Utc>,
) -> Option<(String, EndpointChange)> {
    if new.endpoint_id.is_empty() {
        return None;
    }
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let was = old.and_then(|o| class(o.status).map(|_| o.status.is_ok()));
    let change = match new.status {
        Status::Failed if was != Some(false) => EndpointChange::Failed {
            at,
            reason: new.reason.clone(),
            event: new.event.clone(),
        },
        s if s.is_ok() && was != Some(true) => EndpointChange::Succeeded { at },
        _ => return None,
    };
    Some((new.endpoint_id.clone(), change))
}

/// An endpoint's streak item, as read back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndpointStat {
    pub endpoint_id: String,
    /// Failures in a row since the last success.
    pub streak: i64,
    pub last_failure_at: Option<String>,
    pub last_failure_reason: Option<String>,
    pub last_failure_event: Option<String>,
    pub last_success_at: Option<String>,
}

/// An item to mark counted in a backfill: its table and key.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkKey {
    pub table: String,
    pub key: HashMap<String, AttributeValue>,
}

/// A record written before rollups existed, as found by a backfill.
#[derive(Debug, Clone)]
pub struct Unrolled {
    pub mark: MarkKey,
    pub delivery: Delivery,
}

/// Why a backfill transaction didn't go through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkError {
    /// These marks (indexes into the batch) were already counted: the
    /// record was rewritten (and counted) after it was read.
    AlreadyCounted(Vec<usize>),
    Other(String),
}

/// Where rollups are kept. DynamoDB in production ([`Dynamo`]); a map in tests.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Adds counters to one rollup item (creating it with `ttl`).
    async fn add(&self, pk: &str, change: &Change) -> Result<(), String>;
    /// Applies an endpoint streak change.
    async fn endpoint(&self, pk: &str, endpoint_id: &str, c: &EndpointChange)
        -> Result<(), String>;
    /// Rollup items with sort keys from `from` to `to` (inclusive).
    async fn read(&self, pk: &str, from: &str, to: &str)
        -> Result<Vec<(String, Counters)>, String>;
    /// Every endpoint streak item of `pk`.
    async fn endpoints(&self, pk: &str) -> Result<Vec<EndpointStat>, String>;
    /// In one transaction: marks each item counted (only where it isn't yet)
    /// and adds the counters.
    async fn mark_and_add(
        &self,
        pk: &str,
        marks: &[MarkKey],
        adds: &[Change],
    ) -> Result<(), MarkError>;
    /// Whether `pk`'s backfill finished.
    async fn backfilled(&self, pk: &str) -> Result<bool, String>;
    async fn set_backfilled(&self, pk: &str, at: &str) -> Result<(), String>;
}

/// Updates the rollups for one record write. Failures are logged: a record
/// is never refused because its counters couldn't be written.
pub async fn apply(store: &dyn Store, pk: &str, old: Option<&Delivery>, new: &Delivery) {
    let now = Utc::now();
    apply_at(store, pk, old, new, now).await
}

pub async fn apply_at(
    store: &dyn Store,
    pk: &str,
    old: Option<&Delivery>,
    new: &Delivery,
    now: DateTime<Utc>,
) {
    for c in changes(old, new, now) {
        let mut tries = 0;
        loop {
            tries += 1;
            match store.add(pk, &c).await {
                Ok(()) => break,
                Err(e) if tries < 3 => {
                    tracing::debug!(error = %e, sk = %c.sk, "Retrying a delivery rollup write");
                    tokio::time::sleep(std::time::Duration::from_millis(50 * tries)).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, sk = %c.sk, delivery = %new.id, "Could not update a delivery rollup");
                    break;
                }
            }
        }
    }
    if let Some((ep, change)) = endpoint_change(old, new, now) {
        if let Err(e) = store.endpoint(pk, &ep, &change).await {
            tracing::warn!(error = %e, endpoint = %ep, "Could not update an endpoint's delivery streak");
        }
    }
}

/// How a backfill went.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillReport {
    /// Records counted now.
    pub counted: usize,
    /// Records already counted by a write meanwhile.
    pub skipped: usize,
    /// Records left for the next backfill (a write failed).
    pub left: usize,
}

/// Records per backfill transaction: each record and each distinct rollup
/// item is one action, and a transaction takes at most 100.
pub const BACKFILL_BATCH: usize = 30;

/// Counts records written before rollups existed, each exactly once.
/// Stops starting batches at `deadline`; the rest are `left` for next time.
pub async fn backfill(
    store: &dyn Store,
    pk: &str,
    items: Vec<Unrolled>,
    now: DateTime<Utc>,
    deadline: std::time::Instant,
) -> BackfillReport {
    let mut report = BackfillReport::default();
    let mut rest = items.as_slice();
    while !rest.is_empty() {
        let take = rest.len().min(BACKFILL_BATCH);
        let (batch, tail) = rest.split_at(take);
        rest = tail;
        if std::time::Instant::now() >= deadline {
            report.left += batch.len();
            continue;
        }
        let mut live: Vec<&Unrolled> = batch.iter().collect();
        let mut tries = 0;
        loop {
            tries += 1;
            if live.is_empty() {
                break;
            }
            let mut adds: BTreeMap<String, Change> = BTreeMap::new();
            for u in &live {
                for c in changes(None, &u.delivery, now) {
                    let e = adds.entry(c.sk.clone()).or_insert_with(|| Change {
                        sk: c.sk.clone(),
                        add: Counters::new(),
                        ttl: c.ttl,
                    });
                    merge_into(&mut e.add, &c.add, 1);
                }
            }
            let marks: Vec<MarkKey> = live.iter().map(|u| u.mark.clone()).collect();
            let adds: Vec<Change> = adds.into_values().collect();
            match store.mark_and_add(pk, &marks, &adds).await {
                Ok(()) => {
                    report.counted += live.len();
                    break;
                }
                Err(MarkError::AlreadyCounted(idx)) if tries < 5 && !idx.is_empty() => {
                    report.skipped += idx.len();
                    live = live
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _)| !idx.contains(i))
                        .map(|(_, u)| u)
                        .collect();
                }
                Err(e) => {
                    tracing::warn!(error = ?e, pk, "A delivery rollup backfill batch failed; it is retried next time");
                    report.left += live.len();
                    break;
                }
            }
        }
    }
    report
}

// ── Reading ────────────────────────────────────────────────────────────

/// A value at quantile `q` (0..=1) of a latency histogram (bucket name to
/// count), in milliseconds, interpolated inside its bucket. None when empty.
/// A value in the open last bucket reads as its lower bound.
pub fn percentile(hist: &BTreeMap<String, i64>, q: f64) -> Option<u64> {
    let counts: Vec<(u64, u64, i64)> = {
        let mut lo = 0u64;
        let mut v = vec![];
        for b in LATENCY_BOUNDS_MS {
            v.push((
                lo,
                *b,
                hist.get(&b.to_string()).copied().unwrap_or(0).max(0),
            ));
            lo = *b;
        }
        v.push((lo, lo, hist.get("inf").copied().unwrap_or(0).max(0)));
        v
    };
    let n: i64 = counts.iter().map(|c| c.2).sum();
    if n <= 0 {
        return None;
    }
    let rank = (q.clamp(0.0, 1.0) * n as f64).max(1.0);
    let mut cum = 0f64;
    for (lo, hi, c) in counts {
        if c == 0 {
            continue;
        }
        let next = cum + c as f64;
        if next >= rank {
            let within = (rank - cum) / c as f64;
            return Some((lo as f64 + (hi - lo) as f64 * within).round() as u64);
        }
        cum = next;
    }
    None
}

// ── DynamoDB ───────────────────────────────────────────────────────────

/// Rollups on the settings table.
pub struct Dynamo<'a> {
    pub client: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// The update for one rollup change: `ADD` every counter, keep the first
/// expiry. Pure, so the exact expression is tested.
pub fn add_expression(
    c: &Change,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let mut names = HashMap::from([("#ttl".to_string(), "ttl".to_string())]);
    let mut values = HashMap::from([(":ttl".to_string(), n(c.ttl))]);
    let mut parts = vec![];
    for (i, (k, v)) in c.add.iter().enumerate() {
        names.insert(format!("#a{i}"), k.clone());
        values.insert(format!(":v{i}"), n(*v));
        parts.push(format!("#a{i} :v{i}"));
    }
    (
        format!(
            "SET #ttl = if_not_exists(#ttl, :ttl) ADD {}",
            parts.join(", ")
        ),
        names,
        values,
    )
}

/// Counters from a rollup item (every numeric attribute but the expiry).
pub fn counters_of(item: &HashMap<String, AttributeValue>) -> Counters {
    item.iter()
        .filter(|(k, _)| k.as_str() != "ttl")
        .filter_map(|(k, v)| {
            let n = v.as_n().ok()?.parse::<i64>().ok()?;
            Some((k.clone(), n))
        })
        .collect()
}

fn endpoint_stat(item: &HashMap<String, AttributeValue>) -> Option<EndpointStat> {
    let g = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let id = g("sk")?.strip_prefix(ENDPOINT_PREFIX)?.to_string();
    Some(EndpointStat {
        endpoint_id: id,
        streak: item
            .get("streak")
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        last_failure_at: g("last_failure_at"),
        last_failure_reason: g("last_failure_reason"),
        last_failure_event: g("last_failure_event"),
        last_success_at: g("last_success_at"),
    })
}

/// The indexes a cancelled transaction's reasons name as failed conditions.
pub fn failed_conditions<'a>(codes: impl IntoIterator<Item = Option<&'a str>>) -> Vec<usize> {
    codes
        .into_iter()
        .enumerate()
        .filter(|(_, c)| *c == Some("ConditionalCheckFailed"))
        .map(|(i, _)| i)
        .collect()
}

#[async_trait::async_trait]
impl Store for Dynamo<'_> {
    async fn add(&self, pk: &str, c: &Change) -> Result<(), String> {
        let (expr, names, values) = add_expression(c);
        self.client
            .update_item()
            .table_name(self.table)
            .key("pk", s(pk))
            .key("sk", s(&c.sk))
            .update_expression(expr)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}").chars().take(300).collect())
    }

    async fn endpoint(
        &self,
        pk: &str,
        endpoint_id: &str,
        c: &EndpointChange,
    ) -> Result<(), String> {
        let ttl = (Utc::now() + Duration::days(DAY_RETENTION_DAYS)).timestamp();
        let req = self
            .client
            .update_item()
            .table_name(self.table)
            .key("pk", s(pk))
            .key("sk", s(&format!("{ENDPOINT_PREFIX}{endpoint_id}")))
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":ttl", n(ttl));
        let req = match c {
            EndpointChange::Failed { at, reason, event } => req
                .update_expression(
                    "ADD streak :one SET last_failure_at = :at, last_failure_reason = :r, \
                     last_failure_event = :e, #ttl = :ttl",
                )
                .expression_attribute_values(":one", n(1))
                .expression_attribute_values(":at", s(at))
                .expression_attribute_values(":r", s(reason))
                .expression_attribute_values(":e", s(event)),
            EndpointChange::Succeeded { at } => req
                .update_expression("SET streak = :zero, last_success_at = :at, #ttl = :ttl")
                .expression_attribute_values(":zero", n(0))
                .expression_attribute_values(":at", s(at)),
        };
        req.send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}").chars().take(300).collect())
    }

    async fn read(
        &self,
        pk: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<(String, Counters)>, String> {
        let mut out = vec![];
        let mut start = None;
        loop {
            let r = self
                .client
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :pk AND sk BETWEEN :a AND :b")
                .expression_attribute_values(":pk", s(pk))
                .expression_attribute_values(":a", s(from))
                .expression_attribute_values(":b", s(to))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("{e:?}").chars().take(300).collect::<String>())?;
            for item in r.items() {
                if let Some(sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
                    out.push((sk.clone(), counters_of(item)));
                }
            }
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn endpoints(&self, pk: &str) -> Result<Vec<EndpointStat>, String> {
        let r = self
            .client
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(pk))
            .expression_attribute_values(":p", s(ENDPOINT_PREFIX))
            .send()
            .await
            .map_err(|e| format!("{e:?}").chars().take(300).collect::<String>())?;
        Ok(r.items().iter().filter_map(endpoint_stat).collect())
    }

    async fn mark_and_add(
        &self,
        pk: &str,
        marks: &[MarkKey],
        adds: &[Change],
    ) -> Result<(), MarkError> {
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
        let mut items = vec![];
        for m in marks {
            let u = Update::builder()
                .table_name(&m.table)
                .set_key(Some(m.key.clone()))
                .update_expression("SET #rolled = :one")
                .condition_expression("attribute_not_exists(#rolled)")
                .expression_attribute_names("#rolled", ROLLED)
                .expression_attribute_values(":one", n(1))
                .build()
                .map_err(|e| MarkError::Other(e.to_string()))?;
            items.push(TransactWriteItem::builder().update(u).build());
        }
        for c in adds {
            let (expr, names, values) = add_expression(c);
            let u = Update::builder()
                .table_name(self.table)
                .key("pk", s(pk))
                .key("sk", s(&c.sk))
                .update_expression(expr)
                .set_expression_attribute_names(Some(names))
                .set_expression_attribute_values(Some(values))
                .build()
                .map_err(|e| MarkError::Other(e.to_string()))?;
            items.push(TransactWriteItem::builder().update(u).build());
        }
        match self
            .client
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(TransactWriteItemsError::TransactionCanceledException(c)) =
                    e.as_service_error()
                {
                    let idx = failed_conditions(
                        c.cancellation_reasons()
                            .iter()
                            .take(marks.len())
                            .map(|r| r.code()),
                    );
                    if !idx.is_empty() {
                        return Err(MarkError::AlreadyCounted(idx));
                    }
                }
                Err(MarkError::Other(
                    format!("{e:?}").chars().take(300).collect(),
                ))
            }
        }
    }

    async fn backfilled(&self, pk: &str) -> Result<bool, String> {
        let r = self
            .client
            .get_item()
            .table_name(self.table)
            .key("pk", s(pk))
            .key("sk", s(BACKFILL_SK))
            .send()
            .await
            .map_err(|e| format!("{e:?}").chars().take(300).collect::<String>())?;
        Ok(r.item().is_some())
    }

    async fn set_backfilled(&self, pk: &str, at: &str) -> Result<(), String> {
        self.client
            .put_item()
            .table_name(self.table)
            .item("pk", s(pk))
            .item("sk", s(BACKFILL_SK))
            .item("done_at", s(at))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}").chars().take(300).collect())
    }
}

// ── A store in memory, for tests here and in the gateway ───────────────

/// Rollups in memory, with the same semantics as [`Dynamo`]: counters add,
/// marks are conditional and a transaction is all or nothing.
#[derive(Default)]
pub struct Memory {
    pub items: std::sync::Mutex<BTreeMap<(String, String), Counters>>,
    pub endpoints: std::sync::Mutex<BTreeMap<(String, String), EndpointStat>>,
    /// Marked records, by table and a printed key.
    pub marked: std::sync::Mutex<std::collections::BTreeSet<String>>,
    pub done: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// Makes every `add` fail (to test that a record is never refused).
    pub fail_adds: std::sync::atomic::AtomicBool,
}

impl Memory {
    /// The printed key a mark is kept under.
    pub fn mark_id(m: &MarkKey) -> String {
        let mut k: Vec<String> = m
            .key
            .iter()
            .map(|(k, v)| format!("{k}={}", v.as_s().map(String::as_str).unwrap_or("")))
            .collect();
        k.sort();
        format!("{}|{}", m.table, k.join(","))
    }

    pub fn get(&self, pk: &str, sk: &str) -> Counters {
        self.items
            .lock()
            .unwrap()
            .get(&(pk.to_string(), sk.to_string()))
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl Store for Memory {
    async fn add(&self, pk: &str, c: &Change) -> Result<(), String> {
        if self.fail_adds.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("unavailable".into());
        }
        let mut items = self.items.lock().unwrap();
        let e = items.entry((pk.to_string(), c.sk.clone())).or_default();
        merge_into(e, &c.add, 1);
        Ok(())
    }

    async fn endpoint(
        &self,
        pk: &str,
        endpoint_id: &str,
        c: &EndpointChange,
    ) -> Result<(), String> {
        let mut eps = self.endpoints.lock().unwrap();
        let e = eps
            .entry((pk.to_string(), endpoint_id.to_string()))
            .or_insert_with(|| EndpointStat {
                endpoint_id: endpoint_id.to_string(),
                ..Default::default()
            });
        match c {
            EndpointChange::Failed { at, reason, event } => {
                e.streak += 1;
                e.last_failure_at = Some(at.clone());
                e.last_failure_reason = Some(reason.clone());
                e.last_failure_event = Some(event.clone());
            }
            EndpointChange::Succeeded { at } => {
                e.streak = 0;
                e.last_success_at = Some(at.clone());
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        pk: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<(String, Counters)>, String> {
        Ok(self
            .items
            .lock()
            .unwrap()
            .iter()
            .filter(|((p, sk), _)| p == pk && sk.as_str() >= from && sk.as_str() <= to)
            .map(|((_, sk), c)| (sk.clone(), c.clone()))
            .collect())
    }

    async fn endpoints(&self, pk: &str) -> Result<Vec<EndpointStat>, String> {
        Ok(self
            .endpoints
            .lock()
            .unwrap()
            .iter()
            .filter(|((p, _), _)| p == pk)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn mark_and_add(
        &self,
        pk: &str,
        marks: &[MarkKey],
        adds: &[Change],
    ) -> Result<(), MarkError> {
        let mut marked = self.marked.lock().unwrap();
        let taken = failed_conditions(marks.iter().map(|m| {
            marked
                .contains(&Self::mark_id(m))
                .then_some("ConditionalCheckFailed")
        }));
        if !taken.is_empty() {
            return Err(MarkError::AlreadyCounted(taken));
        }
        for m in marks {
            marked.insert(Self::mark_id(m));
        }
        let mut items = self.items.lock().unwrap();
        for c in adds {
            let e = items.entry((pk.to_string(), c.sk.clone())).or_default();
            merge_into(e, &c.add, 1);
        }
        Ok(())
    }

    async fn backfilled(&self, pk: &str) -> Result<bool, String> {
        Ok(self.done.lock().unwrap().contains(pk))
    }

    async fn set_backfilled(&self, pk: &str, _at: &str) -> Result<(), String> {
        self.done.lock().unwrap().insert(pk.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deliveries::Attempt;

    fn at(t: &str, source: &str, status: Status) -> Delivery {
        let mut d = Delivery::new(Direction::Outbound, source, "run.failed", "x");
        d.created_at = t.into();
        d.status = status;
        d
    }

    fn attempt(ms: u64) -> Attempt {
        Attempt {
            at: "t".into(),
            code: Some(500),
            duration_ms: ms,
            error: String::new(),
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-26T15:30:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn parts_are_safe_attribute_names() {
        assert_eq!(part("pull_request.opened"), "pull_request.opened");
        assert_eq!(part("shared:GitHub"), "shared_github");
        assert_eq!(part("a#b c"), "a_b_c");
        assert_eq!(part(""), "none");
        assert_eq!(part(&"x".repeat(200)).len(), 64);
        assert_eq!(latency_bucket(0), "25");
        assert_eq!(latency_bucket(25), "25");
        assert_eq!(latency_bucket(26), "50");
        assert_eq!(latency_bucket(60_000), "60000");
        assert_eq!(latency_bucket(60_001), "inf");
    }

    #[test]
    fn a_record_counts_once_it_is_finished() {
        let mut d = at("2026-10-26T15:10:00Z", "webhook", Status::Pending);
        d.endpoint_id = "ep_1".into();
        assert!(
            contribution(&d).is_empty(),
            "a retrying record counts for nothing yet"
        );
        d.push_attempt(attempt(120));
        d.push_attempt(attempt(80));
        d.status = Status::Handled;
        let c = contribution(&d);
        assert_eq!(c["s#webhook#out#handled"], 1);
        assert_eq!(c["s#webhook#out#retried"], 1);
        assert_eq!(c["s#webhook#out#retries"], 1);
        assert_eq!(c["s#webhook#out#retried_ok"], 1);
        assert_eq!(c["s#webhook#out#lat#ok#100"], 1);
        assert_eq!(c["ep#ep_1#handled"], 1);
        assert_eq!(c["ep#ep_1#retried_ok"], 1);
        assert!(
            !c.keys().any(|k| k.contains("#r#")),
            "no reason when it went fine"
        );
        d.status = Status::Failed;
        d.reason = "webhook_server_error".into();
        let c = contribution(&d);
        assert_eq!(c["s#webhook#out#failed"], 1);
        assert_eq!(c["s#webhook#out#r#webhook_server_error"], 1);
        assert_eq!(c["s#webhook#out#e#run.failed"], 1);
        assert!(!c.contains_key("s#webhook#out#retried_ok"));
        let mut g = at("2026-10-26T15:10:00Z", "github", Status::Failed);
        g.direction = Direction::Inbound;
        assert_eq!(contribution(&g)["s#github#in#r#unknown"], 1);
    }

    #[test]
    fn rewrites_telescope_to_the_last_state() {
        let m = Memory::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut d = at("2026-10-26T15:10:00Z", "relay", Status::Pending);
            apply_at(&m, "TEAM#1", None, &d, now()).await;
            assert!(m.get("TEAM#1", "DELIVERY_ROLLUP#D#2026-10-26").is_empty());
            let first = d.clone();
            d.push_attempt(attempt(3000));
            d.status = Status::Failed;
            d.reason = "relay_unreachable".into();
            apply_at(&m, "TEAM#1", Some(&first), &d, now()).await;
            let failed = d.clone();
            d.push_attempt(attempt(200));
            d.status = Status::Handled;
            d.reason.clear();
            apply_at(&m, "TEAM#1", Some(&failed), &d, now()).await;
            // A second write of the same state changes nothing.
            apply_at(&m, "TEAM#1", Some(&d), &d, now()).await;
            for sk in [
                "DELIVERY_ROLLUP#D#2026-10-26",
                "DELIVERY_ROLLUP#H#2026-10-26T15",
            ] {
                let c = m.get("TEAM#1", sk);
                let live: Counters = c.into_iter().filter(|(_, v)| *v != 0).collect();
                assert_eq!(
                    live,
                    Counters::from([
                        ("s#relay#out#handled".into(), 1),
                        ("s#relay#out#retried".into(), 1),
                        ("s#relay#out#retries".into(), 1),
                        ("s#relay#out#retried_ok".into(), 1),
                        ("s#relay#out#lat#ok#200".into(), 1),
                    ]),
                    "{sk}"
                );
            }
        });
    }

    #[test]
    fn old_records_change_only_the_buckets_still_kept() {
        let d = at("2026-10-20T10:00:00Z", "email", Status::Failed);
        let ch = changes(None, &d, now());
        assert_eq!(
            ch.len(),
            1,
            "the hour is gone after 72 hours; the day stays"
        );
        assert_eq!(ch[0].sk, "DELIVERY_ROLLUP#D#2026-10-20");
        let day_end = DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z").unwrap();
        assert_eq!(ch[0].ttl, (day_end + Duration::days(120)).timestamp());
        let ancient = at("2026-01-01T10:00:00Z", "email", Status::Failed);
        assert!(changes(None, &ancient, now()).is_empty());
    }

    #[test]
    fn endpoint_streaks_follow_finished_records() {
        let mut d = at("2026-10-26T15:10:00Z", "webhook", Status::Pending);
        assert_eq!(endpoint_change(None, &d, now()), None, "no endpoint");
        d.endpoint_id = "ep_1".into();
        assert_eq!(endpoint_change(None, &d, now()), None, "still retrying");
        let pending = d.clone();
        d.status = Status::Failed;
        d.reason = "webhook_gave_up".into();
        assert!(matches!(
            endpoint_change(Some(&pending), &d, now()),
            Some((_, EndpointChange::Failed { .. }))
        ));
        assert_eq!(
            endpoint_change(Some(&d), &d, now()),
            None,
            "the same failure twice"
        );
        let failed = d.clone();
        d.status = Status::Handled;
        assert!(matches!(
            endpoint_change(Some(&failed), &d, now()),
            Some((_, EndpointChange::Succeeded { .. }))
        ));
    }

    #[test]
    fn percentiles_interpolate_inside_buckets() {
        let h = |pairs: &[(&str, i64)]| -> BTreeMap<String, i64> {
            pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
        };
        assert_eq!(percentile(&h(&[]), 0.5), None);
        // 100 values in (100, 200]: the median is halfway.
        assert_eq!(percentile(&h(&[("200", 100)]), 0.5), Some(150));
        // 90 fast, 10 slow: p50 is fast, p95 sits in the slow bucket.
        let mixed = h(&[("50", 90), ("1000", 10)]);
        assert_eq!(percentile(&mixed, 0.5), Some(39), "25 + 25 x 50/90");
        assert_eq!(percentile(&mixed, 0.95), Some(750 + 250 / 2));
        assert_eq!(percentile(&mixed, 0.99), Some(750 + 250 * 9 / 10));
        // The open bucket reads as its lower bound.
        assert_eq!(percentile(&h(&[("inf", 3)]), 0.99), Some(60_000));
        // A single value is its bucket's top.
        assert_eq!(percentile(&h(&[("25", 1)]), 0.5), Some(25));
    }

    #[test]
    fn the_rollup_update_adds_and_keeps_the_first_expiry() {
        let c = Change {
            sk: "DELIVERY_ROLLUP#D#2026-10-26".into(),
            add: Counters::from([
                ("s#github#in#handled".into(), 1),
                ("s#github#in#failed".into(), -1),
            ]),
            ttl: 99,
        };
        let (expr, names, values) = add_expression(&c);
        assert_eq!(
            expr,
            "SET #ttl = if_not_exists(#ttl, :ttl) ADD #a0 :v0, #a1 :v1"
        );
        assert_eq!(names["#a0"], "s#github#in#failed");
        assert_eq!(values[":v0"], AttributeValue::N("-1".into()));
        assert_eq!(values[":ttl"], AttributeValue::N("99".into()));
        let mut item = HashMap::from([
            ("pk".to_string(), s("TEAM#1")),
            ("sk".to_string(), s("DELIVERY_ROLLUP#D#2026-10-26")),
            ("ttl".to_string(), n(5)),
            ("s#github#in#handled".to_string(), n(4)),
        ]);
        assert_eq!(
            counters_of(&item),
            Counters::from([("s#github#in#handled".into(), 4)])
        );
        item.insert("sk".into(), s("DELIVERY_ENDPOINT#ep_9"));
        item.insert("streak".into(), n(3));
        assert_eq!(endpoint_stat(&item).unwrap().streak, 3);
        assert_eq!(
            failed_conditions([None, Some("ConditionalCheckFailed"), Some("None")]),
            vec![1]
        );
    }

    fn unrolled(id: &str, d: Delivery) -> Unrolled {
        Unrolled {
            mark: MarkKey {
                table: "settings".into(),
                key: HashMap::from([
                    ("pk".to_string(), s("TEAM#1")),
                    ("sk".to_string(), s(&format!("DELIVERY#{id}"))),
                ]),
            },
            delivery: d,
        }
    }

    #[test]
    fn a_backfill_counts_each_old_record_once() {
        let m = Memory::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let far = std::time::Instant::now() + std::time::Duration::from_secs(60);
            let mut items = vec![];
            for i in 0..70 {
                let st = if i % 7 == 0 {
                    Status::Failed
                } else {
                    Status::Handled
                };
                items.push(unrolled(
                    &i.to_string(),
                    at("2026-10-25T09:00:00Z", "github", st),
                ));
            }
            items.push(unrolled(
                "pending",
                at("2026-10-25T09:00:00Z", "relay", Status::Pending),
            ));
            // One record was rewritten (and counted by the write path) after the backfill read it.
            m.marked
                .lock()
                .unwrap()
                .insert(Memory::mark_id(&items[3].mark));
            let r = backfill(&m, "TEAM#1", items.clone(), now(), far).await;
            assert_eq!(
                r,
                BackfillReport {
                    counted: 70,
                    skipped: 1,
                    left: 0
                }
            );
            let day = m.get("TEAM#1", "DELIVERY_ROLLUP#D#2026-10-25");
            assert_eq!(day["s#github#out#failed"], 10);
            assert_eq!(
                day["s#github#out#handled"], 59,
                "the record counted elsewhere is left out"
            );
            // Running it again counts nothing more.
            let again = backfill(&m, "TEAM#1", items, now(), far).await;
            assert_eq!(again.counted, 0);
            assert_eq!(
                m.get("TEAM#1", "DELIVERY_ROLLUP#D#2026-10-25")["s#github#out#failed"],
                10
            );
            // Past the deadline nothing starts: it is all left for next time.
            let late = backfill(
                &m,
                "TEAM#1",
                vec![unrolled(
                    "z",
                    at("2026-10-25T09:00:00Z", "github", Status::Handled),
                )],
                now(),
                std::time::Instant::now(),
            )
            .await;
            assert_eq!(
                late,
                BackfillReport {
                    counted: 0,
                    skipped: 0,
                    left: 1
                }
            );
        });
    }

    #[test]
    fn teams_never_see_each_others_counters() {
        let m = Memory::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut d = at("2026-10-26T15:10:00Z", "webhook", Status::Failed);
            d.endpoint_id = "ep_1".into();
            apply_at(&m, "TEAM#a", None, &d, now()).await;
            let b = m
                .read("TEAM#b", DAY_PREFIX, &format!("{DAY_PREFIX}~"))
                .await
                .unwrap();
            assert!(b.is_empty());
            assert!(m.endpoints("TEAM#b").await.unwrap().is_empty());
            assert_eq!(m.endpoints("TEAM#a").await.unwrap()[0].streak, 1);
            let a = m
                .read("TEAM#a", DAY_PREFIX, &format!("{DAY_PREFIX}~"))
                .await
                .unwrap();
            assert_eq!(a.len(), 1);
        });
    }

    #[test]
    fn a_failed_counter_write_never_fails_the_record() {
        let m = Memory::default();
        m.fail_adds.store(true, std::sync::atomic::Ordering::SeqCst);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let d = at("2026-10-26T15:10:00Z", "email", Status::Handled);
            apply_at(&m, "TEAM#1", None, &d, now()).await;
        });
        assert!(m.get("TEAM#1", "DELIVERY_ROLLUP#D#2026-10-26").is_empty());
    }
}
