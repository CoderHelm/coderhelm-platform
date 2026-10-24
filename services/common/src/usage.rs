//! Model usage accounting: the one path every model call goes through.
//!
//! Every call is a [`CallRecord`] with an id (Anthropic's `request-id`, or a
//! generated one). [`UsageMeter::record_call`] adds it exactly once: the
//! counter updates and a once-only marker for the id are one transaction, a
//! failed write is retried, and a call that still can't be written is handed
//! to the fallback (a `usage_record` message on the tickets queue) and added
//! later, once. Calls CoderHelm pays for itself (Titan embeddings) are kept
//! under `coderhelm#` counters, apart from the team's ([`BilledTo`]).
//!
//! Each Anthropic response carries four token counts: new (uncached) input,
//! input read from the prompt cache, input written to the cache (5-minute or
//! 1-hour), and output (thinking included, it is billed as output). [`record`]
//! ADDs one call's counts to the team's analytics counters: the month, all
//! time, and the day (the day is what the daily reconciliation compares with
//! Anthropic's own report). It is the ONLY writer of token and cost counters
//! on the analytics table; runs keep their own per-run totals on the run item.
//!
//! Prices live in `model_prices.json` (USD per million tokens, from Anthropic's
//! pricing page). They are internal: cost is stored and logged, never shown.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use aws_sdk_dynamodb::types::{AttributeValue, TransactWriteItem, Update};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

/// Where a model call came from. Breakdowns are keyed by these names.
pub mod feature {
    /// Ticket runs: triage, plan, implement, self review, security, PR.
    pub const RUN: &str = "run";
    /// Follow-up work on a run's PR (review comments, CI fixes, conflicts).
    pub const FEEDBACK: &str = "feedback";
    /// PR reviews of people's pull requests.
    pub const REVIEW: &str = "review";
    /// A question asked of a team agent (skills).
    pub const AGENT_ASK: &str = "agent_ask";
    /// Syncing team agents' knowledge.
    pub const SKILL_SYNC: &str = "skill_sync";
    /// Alert triage and alert runs.
    pub const ALERT: &str = "alert";
    /// Repository onboarding.
    pub const ONBOARD: &str = "onboard";
    /// Infrastructure analysis.
    pub const INFRA: &str = "infra";
    /// Plan chat in the dashboard.
    pub const CHAT: &str = "chat";
    /// Executing an approved plan's tasks.
    pub const PLAN: &str = "plan";
    /// The scheduled log analyzer.
    pub const LOG_ANALYSIS: &str = "log_analysis";
    /// Release notes.
    pub const RELEASE_NOTES: &str = "release_notes";
    /// Test result classification.
    pub const TESTS: &str = "tests";
    /// Live source descriptions and tool proposals.
    pub const LIVE_SOURCES: &str = "live_sources";
    /// Reading a connected data source's tables to suggest hidden fields.
    pub const DATA_SOURCE: &str = "data_source";
    /// Learnings extracted into memory after a run.
    pub const MEMORY: &str = "memory";
    /// Titan embeddings for memory (billed to CoderHelm, not the team).
    pub const EMBEDDING: &str = "embedding";
    /// A call made outside any known scope (still counted in every total).
    pub const OTHER: &str = "other";
}

tokio::task_local! {
    static FEATURE: &'static str;
}

/// Run `fut` with every model call inside it attributed to `feature`.
pub async fn with_feature<F: std::future::Future>(feature: &'static str, fut: F) -> F::Output {
    FEATURE.scope(feature, fut).await
}

/// The feature of the current task, or [`feature::OTHER`].
pub fn current_feature() -> &'static str {
    FEATURE.try_with(|f| *f).unwrap_or(feature::OTHER)
}

// ── Prices ──────────────────────────────────────────────────────────────────

/// USD per million tokens for one model.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct Rates {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write_5m: f64,
    pub cache_write_1h: f64,
    /// Higher prices for a request whose prompt (all input, cache included)
    /// is over `over_tokens` (Haiku 5.5).
    #[serde(default)]
    pub long_prompt: Option<LongPrompt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct LongPrompt {
    pub over_tokens: u64,
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write_5m: f64,
    pub cache_write_1h: f64,
}

#[derive(Deserialize)]
struct PriceFile {
    models: HashMap<String, Rates>,
}

/// The price table, shipped with the code. The log analyzer lambda carries a
/// byte-identical copy (a test keeps them equal).
pub const PRICE_TABLE_JSON: &str = include_str!("../model_prices.json");

fn prices() -> &'static HashMap<String, Rates> {
    static P: OnceLock<HashMap<String, Rates>> = OnceLock::new();
    P.get_or_init(|| {
        serde_json::from_str::<PriceFile>(PRICE_TABLE_JSON)
            .map(|f| f.models)
            .unwrap_or_default()
    })
}

/// The canonical id of a model: lowercase, without a provider or region
/// prefix (`anthropic.`, `us.`), a context suffix (`[1m]`), a Bedrock version
/// (`-v1:0`) or a date (`-20250805`, `@20250805`).
pub fn normalize_model(model_id: &str) -> String {
    let mut m = model_id.trim().to_ascii_lowercase();
    if let Some(i) = m.find('[') {
        m.truncate(i);
    }
    if let Some(i) = m.find('@') {
        m.truncate(i);
    }
    for p in ["global.", "us.", "eu.", "apac.", "jp.", "au."] {
        if let Some(rest) = m.strip_prefix(p) {
            m = rest.to_string();
        }
    }
    if let Some(rest) = m.strip_prefix("anthropic.") {
        m = rest.to_string();
    }
    if let Some(i) = m.find("-v1") {
        m.truncate(i);
    }
    if let Some(i) = m.find(':') {
        m.truncate(i);
    }
    // A trailing 8-digit date.
    if let Some((head, tail)) = m.rsplit_once('-') {
        if tail.len() == 8 && tail.chars().all(|c| c.is_ascii_digit()) {
            m = head.to_string();
        }
    }
    m
}

/// The most expensive known rates (used for a model the table doesn't know,
/// so an unknown model is never counted as free).
pub fn most_expensive_rates() -> Rates {
    prices()
        .values()
        .copied()
        .max_by(|a, b| {
            (a.output + a.input)
                .partial_cmp(&(b.output + b.input))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(Rates {
            input: 15.0,
            output: 75.0,
            cache_read: 1.5,
            cache_write_5m: 18.75,
            cache_write_1h: 30.0,
            long_prompt: None,
        })
}

/// Rates for a model, and whether the table knew it. An unknown model is
/// logged once per process and priced at [`most_expensive_rates`].
pub fn rates_for(model_id: &str) -> (Rates, bool) {
    let m = normalize_model(model_id);
    if let Some(r) = prices().get(&m) {
        return (*r, true);
    }
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let first = WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .map(|mut s| s.insert(m.clone()))
        .unwrap_or(false);
    if first {
        warn!(
            model = %model_id,
            "Model missing from the price table; costing it at the most expensive known rate"
        );
    }
    (most_expensive_rates(), false)
}

// ── One call's usage ────────────────────────────────────────────────────────

/// The cache lifetime a request asked for. Used to split cache writes when a
/// response doesn't break them down itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheTtl {
    #[default]
    FiveMinutes,
    OneHour,
}

/// Token counts of one model call.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CallUsage {
    /// `usage.input_tokens`: only the input that was neither read from nor
    /// written to the cache.
    pub uncached_input: u64,
    pub cache_read: u64,
    pub cache_write_5m: u64,
    pub cache_write_1h: u64,
    /// `usage.output_tokens`, thinking included.
    pub output: u64,
}

impl CallUsage {
    /// Parse a Messages API `usage` object. Cache writes are split by
    /// `cache_creation.ephemeral_{5m,1h}_input_tokens` when present, otherwise
    /// all of `cache_creation_input_tokens` goes to `ttl`.
    pub fn from_api(usage: &Value, ttl: CacheTtl) -> Self {
        let n = |v: &Value, k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
        let write_total = n(usage, "cache_creation_input_tokens");
        let (w5, w1) = match usage.get("cache_creation").filter(|c| c.is_object()) {
            Some(c) => {
                let w5 = n(c, "ephemeral_5m_input_tokens");
                let w1 = n(c, "ephemeral_1h_input_tokens");
                // Trust the total if the breakdown misses part of it.
                let rest = write_total.saturating_sub(w5 + w1);
                match ttl {
                    CacheTtl::FiveMinutes => (w5 + rest, w1),
                    CacheTtl::OneHour => (w5, w1 + rest),
                }
            }
            None => match ttl {
                CacheTtl::FiveMinutes => (write_total, 0),
                CacheTtl::OneHour => (0, write_total),
            },
        };
        CallUsage {
            uncached_input: n(usage, "input_tokens"),
            cache_read: n(usage, "cache_read_input_tokens"),
            cache_write_5m: w5,
            cache_write_1h: w1,
            output: n(usage, "output_tokens"),
        }
    }

    pub fn cache_write(&self) -> u64 {
        self.cache_write_5m + self.cache_write_1h
    }

    /// Every input token of the request, cached or not.
    pub fn prompt_tokens(&self) -> u64 {
        self.uncached_input + self.cache_read + self.cache_write()
    }

    /// All four parts: what the token limit counts.
    pub fn total(&self) -> u64 {
        self.prompt_tokens() + self.output
    }

    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }

    /// USD for this call on `model_id`.
    pub fn cost_usd(&self, model_id: &str) -> f64 {
        let (r, _) = rates_for(model_id);
        let (input, output, read, w5, w1) = match r.long_prompt {
            Some(lp) if self.prompt_tokens() > lp.over_tokens => (
                lp.input,
                lp.output,
                lp.cache_read,
                lp.cache_write_5m,
                lp.cache_write_1h,
            ),
            _ => (
                r.input,
                r.output,
                r.cache_read,
                r.cache_write_5m,
                r.cache_write_1h,
            ),
        };
        (self.uncached_input as f64 * input
            + self.output as f64 * output
            + self.cache_read as f64 * read
            + self.cache_write_5m as f64 * w5
            + self.cache_write_1h as f64 * w1)
            / 1_000_000.0
    }
}

// ── Counters ────────────────────────────────────────────────────────────────

/// The analytics periods one call is added to.
pub fn periods(now: chrono::DateTime<chrono::Utc>) -> [String; 3] {
    [
        now.format("%Y-%m").to_string(),
        "ALL_TIME".to_string(),
        format!("DAY#{}", now.format("%Y-%m-%d")),
    ]
}

/// A breakdown key part: lowercase letters, digits, `-`, `_`, `.` only.
fn key_part(s: &str) -> String {
    let k: String = s
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .collect();
    if k.is_empty() {
        "unknown".into()
    } else {
        k
    }
}

/// Attribute name of a per-model counter, e.g. `model#claude-opus-5-5#cache_read`.
pub fn model_attr(model_id: &str, field: &str) -> String {
    format!("model#{}#{field}", key_part(&normalize_model(model_id)))
}

/// Attribute name of a per-feature counter, e.g. `feature#review#tokens_out`.
pub fn feature_attr(feature: &str, field: &str) -> String {
    format!("feature#{}#{field}", key_part(feature))
}

fn cost_n(v: f64) -> String {
    format!("{v:.9}")
}

/// The ADD expression for one call: every counter, its name map and values.
/// Pure, so the exact attributes and amounts are tested.
pub fn add_expression(
    feature: &str,
    model_id: &str,
    call: &CallUsage,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let cost = call.cost_usd(model_id);
    let mut adds: Vec<(String, String)> = vec![
        // Kept names (history and other readers): total_tokens_in is the
        // UNCACHED input, as Anthropic's `input_tokens` always was.
        ("total_tokens_in".into(), call.uncached_input.to_string()),
        ("tokens_in_uncached".into(), call.uncached_input.to_string()),
        ("cache_read_tokens".into(), call.cache_read.to_string()),
        ("cache_write_tokens".into(), call.cache_write().to_string()),
        (
            "cache_write_5m_tokens".into(),
            call.cache_write_5m.to_string(),
        ),
        (
            "cache_write_1h_tokens".into(),
            call.cache_write_1h.to_string(),
        ),
        ("total_tokens_out".into(), call.output.to_string()),
        ("total_tokens".into(), call.total().to_string()),
        ("total_cost_usd".into(), cost_n(cost)),
        ("model_calls".into(), "1".into()),
    ];
    for (name_of, key) in [
        (model_attr as fn(&str, &str) -> String, model_id),
        (feature_attr as fn(&str, &str) -> String, feature),
    ] {
        adds.push((name_of(key, "tokens_in"), call.uncached_input.to_string()));
        adds.push((name_of(key, "cache_read"), call.cache_read.to_string()));
        adds.push((name_of(key, "cache_write"), call.cache_write().to_string()));
        adds.push((name_of(key, "tokens_out"), call.output.to_string()));
        adds.push((name_of(key, "cost_usd"), cost_n(cost)));
        adds.push((name_of(key, "calls"), "1".into()));
    }
    let mut names = HashMap::new();
    let mut values = HashMap::new();
    let mut parts = Vec::new();
    for (i, (attr, n)) in adds.into_iter().enumerate() {
        names.insert(format!("#a{i}"), attr);
        values.insert(format!(":v{i}"), AttributeValue::N(n));
        parts.push(format!("#a{i} :v{i}"));
    }
    (format!("ADD {}", parts.join(", ")), names, values)
}

// ── Who pays for a call ─────────────────────────────────────────────────────

/// Who is billed for a model call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BilledTo {
    /// The team's own Anthropic key: the team's totals, its token limit and
    /// the reconciliation against Anthropic's report all count it.
    #[default]
    Team,
    /// CoderHelm's own AWS account (Titan embeddings on Bedrock). Kept on the
    /// same analytics items under `coderhelm#` counters only, so it never
    /// counts against a team's Anthropic spend or token limit, and never
    /// reads as drift against Anthropic's report.
    Coderhelm,
}

/// One model call as it is recorded: the unit of exactly-once accounting.
/// It is also the body of the `usage_record` worker message (a call whose
/// counters could not be written, added later), so its shape is a contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallRecord {
    /// Anthropic's `request-id` when there is one, otherwise a generated id.
    /// The same id is never counted twice.
    pub call_id: String,
    pub team_id: String,
    /// When the call was made (ms since the epoch): the periods it counts in.
    pub at_ms: i64,
    pub feature: String,
    pub model: String,
    #[serde(default)]
    pub billed_to: BilledTo,
    /// The output (or, rarely, the input) is an estimate: the reply broke off
    /// before its final usage arrived. See `model_stream`.
    #[serde(default)]
    pub estimated: bool,
    pub usage: CallUsage,
}

impl CallRecord {
    /// A call made now under `feature`, with a fresh id.
    pub fn now(team_id: &str, feature: &str, model: &str, usage: CallUsage) -> Self {
        Self {
            call_id: ulid::Ulid::new().to_string(),
            team_id: team_id.to_string(),
            at_ms: chrono::Utc::now().timestamp_millis(),
            feature: feature.to_string(),
            model: model.to_string(),
            billed_to: BilledTo::Team,
            estimated: false,
            usage,
        }
    }

    fn at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.at_ms).unwrap_or_else(chrono::Utc::now)
    }
}

/// USD per million input tokens of the embedding models CoderHelm pays for,
/// from the `embedding_models` section of the price table.
fn embedding_prices() -> &'static HashMap<String, f64> {
    #[derive(Deserialize)]
    struct EmbeddingRate {
        input: f64,
    }
    #[derive(Deserialize)]
    struct File {
        #[serde(default)]
        embedding_models: HashMap<String, EmbeddingRate>,
    }
    static P: OnceLock<HashMap<String, f64>> = OnceLock::new();
    P.get_or_init(|| {
        serde_json::from_str::<File>(PRICE_TABLE_JSON)
            .map(|f| {
                f.embedding_models
                    .into_iter()
                    .map(|(k, v)| (k, v.input))
                    .collect()
            })
            .unwrap_or_default()
    })
}

/// USD for an embedding call (input tokens only). An unknown embedding model
/// is priced at the most expensive known embedding rate, never free.
pub fn embedding_cost_usd(model_id: &str, input_tokens: u64) -> f64 {
    let p = embedding_prices();
    let rate = p
        .get(&normalize_model(model_id))
        .copied()
        .unwrap_or_else(|| p.values().copied().fold(0.0_f64, f64::max).max(0.02));
    input_tokens as f64 * rate / 1_000_000.0
}

/// The ADD expression for a call CoderHelm pays for: `coderhelm#` counters
/// only, never the team's totals.
pub fn own_add_expression(
    feature: &str,
    model_id: &str,
    call: &CallUsage,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let cost = embedding_cost_usd(model_id, call.prompt_tokens());
    let m = key_part(&normalize_model(model_id));
    let f = key_part(feature);
    let input = call.prompt_tokens().to_string();
    let out = call.output.to_string();
    let adds: Vec<(String, String)> = vec![
        ("coderhelm#tokens_in".into(), input.clone()),
        ("coderhelm#tokens_out".into(), out.clone()),
        ("coderhelm#cost_usd".into(), cost_n(cost)),
        ("coderhelm#calls".into(), "1".into()),
        (format!("coderhelm#model#{m}#tokens_in"), input.clone()),
        (format!("coderhelm#model#{m}#tokens_out"), out),
        (format!("coderhelm#model#{m}#cost_usd"), cost_n(cost)),
        (format!("coderhelm#model#{m}#calls"), "1".into()),
        (format!("coderhelm#feature#{f}#tokens_in"), input),
        (format!("coderhelm#feature#{f}#calls"), "1".into()),
    ];
    expression(adds)
}

fn expression(
    adds: Vec<(String, String)>,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let mut names = HashMap::new();
    let mut values = HashMap::new();
    let mut parts = Vec::new();
    for (i, (attr, n)) in adds.into_iter().enumerate() {
        names.insert(format!("#a{i}"), attr);
        values.insert(format!(":v{i}"), AttributeValue::N(n));
        parts.push(format!("#a{i} :v{i}"));
    }
    (format!("ADD {}", parts.join(", ")), names, values)
}

/// Calls per period whose usage is an estimate (the reply broke off).
pub const ESTIMATED_CALLS: &str = "estimated_calls";
/// Calls per period added later from the fallback queue.
pub const RECOVERED_CALLS: &str = "recovered_calls";

/// The full ADD expression for one recorded call: the team's counters (or
/// CoderHelm's own), plus the estimated and recovered call counts.
pub fn record_expression(
    rec: &CallRecord,
    recovered: bool,
) -> (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
) {
    let (expr, mut names, mut values) = match rec.billed_to {
        BilledTo::Team => add_expression(&rec.feature, &rec.model, &rec.usage),
        BilledTo::Coderhelm => own_add_expression(&rec.feature, &rec.model, &rec.usage),
    };
    let mut extra = Vec::new();
    if rec.estimated {
        extra.push(ESTIMATED_CALLS);
    }
    if recovered {
        extra.push(RECOVERED_CALLS);
    }
    let mut expr = expr;
    for (i, attr) in extra.into_iter().enumerate() {
        let (n, v) = (format!("#x{i}"), format!(":x{i}"));
        expr.push_str(&format!(", {n} {v}"));
        names.insert(n, attr.to_string());
        values.insert(v, AttributeValue::N("1".into()));
    }
    (expr, names, values)
}

/// How long the once-only marker of a call is kept: longer than the
/// fallback queue keeps a message (14 days), so a replay always finds it.
pub const CALL_MARKER_DAYS: i64 = 35;

/// The key of a call's once-only marker on the analytics table. Its own
/// partition, so no team's period reads ever see it.
pub fn call_marker_key(call_id: &str) -> (String, String) {
    (format!("USAGE_CALL#{call_id}"), "CALL".to_string())
}

// ── Where calls are written ─────────────────────────────────────────────────

/// Whether a call was added now or had been added before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Added {
    Counted,
    AlreadyCounted,
}

/// Adds a call to its team's counters, exactly once per `call_id`.
#[async_trait::async_trait]
pub trait UsageStore: Send + Sync {
    async fn add(&self, rec: &CallRecord, recovered: bool) -> Result<Added, String>;
}

/// The analytics table: one transaction adds the call to its month, all-time
/// and day items and writes the call's once-only marker; a marker that is
/// already there cancels the whole transaction, so nothing is added twice.
pub struct DynamoStore {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

#[async_trait::async_trait]
impl UsageStore for DynamoStore {
    async fn add(&self, rec: &CallRecord, recovered: bool) -> Result<Added, String> {
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
        use aws_sdk_dynamodb::types::Put;
        let (expr, names, values) = record_expression(rec, recovered);
        let (pk, sk) = call_marker_key(&rec.call_id);
        let expires = rec.at().timestamp() + CALL_MARKER_DAYS * 86_400;
        let marker = Put::builder()
            .table_name(&self.table)
            .item("team_id", AttributeValue::S(pk))
            .item("period", AttributeValue::S(sk))
            .item("for_team", AttributeValue::S(rec.team_id.clone()))
            .item("expires_at", AttributeValue::N(expires.to_string()))
            .condition_expression("attribute_not_exists(team_id)")
            .build()
            .map_err(|e| e.to_string())?;
        // The marker goes first: its cancellation reason is at index 0.
        let mut tx = self
            .dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(marker).build());
        for period in periods(rec.at()) {
            let update = Update::builder()
                .table_name(&self.table)
                .key("team_id", AttributeValue::S(rec.team_id.clone()))
                .key("period", AttributeValue::S(period))
                .update_expression(&expr)
                .set_expression_attribute_names(Some(names.clone()))
                .set_expression_attribute_values(Some(values.clone()))
                .build()
                .map_err(|e| e.to_string())?;
            tx = tx.transact_items(TransactWriteItem::builder().update(update).build());
        }
        match tx.send().await {
            Ok(_) => Ok(Added::Counted),
            Err(e) => {
                let already = matches!(
                    e.as_service_error(),
                    Some(TransactWriteItemsError::TransactionCanceledException(c))
                        if c.cancellation_reasons().first().and_then(|r| r.code())
                            == Some("ConditionalCheckFailed")
                );
                if already {
                    Ok(Added::AlreadyCounted)
                } else {
                    Err(format!(
                        "{}",
                        aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                    ))
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordedCall {
    pub call_id: String,
    pub team_id: String,
    pub feature: String,
    pub model: String,
    pub usage: CallUsage,
    pub billed_to: BilledTo,
    pub estimated: bool,
    pub recovered: bool,
}

/// An in-memory store (tests): once per `call_id`, and it can be told to
/// fail like an unreachable table.
#[derive(Default)]
pub struct MemoryStore {
    pub calls: Arc<Mutex<Vec<RecordedCall>>>,
    seen: Mutex<HashSet<String>>,
    /// Every write fails while set.
    pub failing: std::sync::atomic::AtomicBool,
    /// Writes attempted, failed ones included.
    pub attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl UsageStore for MemoryStore {
    async fn add(&self, rec: &CallRecord, recovered: bool) -> Result<Added, String> {
        use std::sync::atomic::Ordering;
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            return Err("the table is unreachable".into());
        }
        let mut seen = self.seen.lock().map_err(|e| e.to_string())?;
        if !seen.insert(rec.call_id.clone()) {
            return Ok(Added::AlreadyCounted);
        }
        self.calls
            .lock()
            .map_err(|e| e.to_string())?
            .push(RecordedCall {
                call_id: rec.call_id.clone(),
                team_id: rec.team_id.clone(),
                feature: rec.feature.clone(),
                model: rec.model.clone(),
                usage: rec.usage,
                billed_to: rec.billed_to,
                estimated: rec.estimated,
                recovered,
            });
        Ok(Added::Counted)
    }
}

// ── The fallback for a call that could not be written ──────────────────────

/// Holds a call whose counters could not be written, so it is added later
/// (exactly once: the call id is kept).
#[async_trait::async_trait]
pub trait UsageFallback: Send + Sync {
    async fn hold(&self, rec: &CallRecord) -> Result<(), String>;
}

/// The `usage_record` worker message: a [`CallRecord`] with its type tag.
pub fn usage_record_message(rec: &CallRecord) -> Value {
    let mut v = serde_json::to_value(rec).unwrap_or_default();
    v["type"] = Value::String("usage_record".into());
    v
}

/// Sends the call to the worker's tickets queue as a `usage_record` message;
/// the worker adds it (once) and the period's `recovered_calls` counts it.
pub struct SqsFallback {
    pub sqs: aws_sdk_sqs::Client,
    pub queue_url: String,
}

#[async_trait::async_trait]
impl UsageFallback for SqsFallback {
    async fn hold(&self, rec: &CallRecord) -> Result<(), String> {
        if self.queue_url.is_empty() || self.queue_url == "unused" {
            return Err("no fallback queue configured".into());
        }
        self.sqs
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(usage_record_message(rec).to_string())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_sqs::error::DisplayErrorContext(&e)))
    }
}

/// Collects held calls (tests).
#[derive(Default)]
pub struct MemoryFallback {
    pub held: Mutex<Vec<CallRecord>>,
}

#[async_trait::async_trait]
impl UsageFallback for MemoryFallback {
    async fn hold(&self, rec: &CallRecord) -> Result<(), String> {
        self.held
            .lock()
            .map_err(|e| e.to_string())?
            .push(rec.clone());
        Ok(())
    }
}

static FALLBACK: OnceLock<Arc<dyn UsageFallback>> = OnceLock::new();

/// The process's fallback (set once at startup by the worker and gateway).
pub fn set_fallback(f: Arc<dyn UsageFallback>) {
    let _ = FALLBACK.set(f);
}

/// Add a held call: the worker's `usage_record` handler. Counted once, ever:
/// replaying the same message again adds nothing.
pub async fn replay(store: &dyn UsageStore, rec: &CallRecord) -> Result<Added, String> {
    store.add(rec, true).await
}

// ── The meter ───────────────────────────────────────────────────────────────

/// Waits (ms) before each retry of a failed counter write.
const WRITE_RETRY_MS: [u64; 3] = [100, 400, 1_600];

/// A team's meter: what every model client holds so each call is counted.
#[derive(Clone)]
pub struct UsageMeter {
    pub team_id: String,
    store: Arc<dyn UsageStore>,
    fallback: Option<Arc<dyn UsageFallback>>,
    retry_ms: &'static [u64],
}

impl std::fmt::Debug for UsageMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageMeter")
            .field("team_id", &self.team_id)
            .finish_non_exhaustive()
    }
}

impl UsageMeter {
    pub fn dynamo(dynamo: aws_sdk_dynamodb::Client, table: &str, team_id: &str) -> Self {
        Self::with_store(
            team_id,
            Arc::new(DynamoStore {
                dynamo,
                table: table.to_string(),
            }),
        )
    }

    pub fn with_store(team_id: &str, store: Arc<dyn UsageStore>) -> Self {
        Self {
            team_id: team_id.to_string(),
            store,
            fallback: None,
            retry_ms: &WRITE_RETRY_MS,
        }
    }

    /// A meter that keeps calls in memory (tests), and what it kept.
    pub fn memory(team_id: &str) -> (Self, Arc<Mutex<Vec<RecordedCall>>>) {
        let store = Arc::new(MemoryStore::default());
        let calls = store.calls.clone();
        let mut m = Self::with_store(team_id, store);
        m.retry_ms = &[0, 0];
        (m, calls)
    }

    /// This meter with its own fallback instead of the process's (tests).
    pub fn with_fallback(mut self, f: Arc<dyn UsageFallback>) -> Self {
        self.fallback = Some(f);
        self
    }

    /// This meter with no waits between write retries (tests).
    pub fn without_waits(mut self) -> Self {
        self.retry_ms = &[0, 0];
        self
    }

    /// Count one call under the current task's feature. Never fails the
    /// caller.
    pub async fn record(&self, model_id: &str, call: &CallUsage) {
        self.record_as(current_feature(), model_id, call).await
    }

    pub async fn record_as(&self, feature: &str, model_id: &str, call: &CallUsage) {
        let rec = CallRecord::now(&self.team_id, feature, model_id, *call);
        self.record_call(rec).await
    }

    /// Count one call. A failed write is retried with backoff; if it still
    /// fails, the call is handed to the fallback (the tickets queue) to be
    /// added later, once. Only if that fails too is it lost, and then the
    /// whole record is logged as an error so it can be added by hand.
    pub async fn record_call(&self, rec: CallRecord) {
        if rec.usage.is_empty() || rec.team_id.is_empty() {
            return;
        }
        let mut last = String::new();
        for attempt in 0..=self.retry_ms.len() {
            match self.store.add(&rec, false).await {
                Ok(_) => return,
                Err(e) => last = e,
            }
            if let Some(ms) = self.retry_ms.get(attempt) {
                tokio::time::sleep(std::time::Duration::from_millis(*ms)).await;
            }
        }
        let fallback = self.fallback.clone().or_else(|| FALLBACK.get().cloned());
        let held = match &fallback {
            Some(f) => f.hold(&rec).await,
            None => Err("no fallback set".into()),
        };
        match held {
            Ok(()) => warn!(team_id = %rec.team_id, call_id = %rec.call_id, error = %last,
                "Couldn't record model usage; queued it to be added later"),
            Err(e) => tracing::error!(
                event = "usage_lost",
                record = %serde_json::to_string(&rec).unwrap_or_default(),
                write_error = %last,
                fallback_error = %e,
                "Couldn't record model usage or queue it"
            ),
        }
    }
}

// ── The check against Anthropic's report ───────────────────────────────────

/// Where the daily usage check (`usage_reconcile`) keeps its last outcome on
/// the analytics table: one item for the whole platform.
pub fn usage_check_key() -> (String, String) {
    ("__coderhelm__".to_string(), "USAGE_CHECK".to_string())
}

/// The outcome of one run of the usage check.
#[derive(Debug, Clone, PartialEq)]
pub enum UsageCheck {
    /// No Anthropic admin-key secret: nothing is compared.
    Off { secret: String },
    /// Compared `teams` teams for `day`; `flagged` models differed by more
    /// than the drift limit; `failed` teams' reports couldn't be read.
    Ran {
        day: String,
        teams: u64,
        flagged: u64,
        failed: u64,
    },
}

/// The analytics item for an outcome, stamped `checked_at`.
pub fn usage_check_item(check: &UsageCheck, checked_at: &str) -> HashMap<String, AttributeValue> {
    let (pk, sk) = usage_check_key();
    let s = |v: &str| AttributeValue::S(v.to_string());
    let n = |v: u64| AttributeValue::N(v.to_string());
    let mut item = HashMap::from([
        ("team_id".to_string(), s(&pk)),
        ("period".to_string(), s(&sk)),
        ("checked_at".to_string(), s(checked_at)),
    ]);
    match check {
        UsageCheck::Off { secret } => {
            item.insert("state".into(), s("off"));
            item.insert("secret".into(), s(secret));
        }
        UsageCheck::Ran {
            day,
            teams,
            flagged,
            failed,
        } => {
            item.insert("state".into(), s("ran"));
            item.insert("day".into(), s(day));
            item.insert("teams".into(), n(*teams));
            item.insert("flagged_models".into(), n(*flagged));
            item.insert("failed_teams".into(), n(*failed));
        }
    }
    item
}

/// The admin page's view of the check: `state` is `off`, `ran` or `never`
/// (the job hasn't run since this was deployed).
pub fn usage_check_json(item: Option<&HashMap<String, AttributeValue>>) -> Value {
    let Some(i) = item else {
        return serde_json::json!({ "state": "never" });
    };
    let s = |k: &str| i.get(k).and_then(|v| v.as_s().ok()).cloned();
    serde_json::json!({
        "state": s("state").unwrap_or_else(|| "never".into()),
        "checked_at": s("checked_at"),
        "secret": s("secret"),
        "day": s("day"),
        "teams": n_of(i, "teams"),
        "flagged_models": n_of(i, "flagged_models"),
        "failed_teams": n_of(i, "failed_teams"),
    })
}

// ── Reading counters back ───────────────────────────────────────────────────

/// The four parts of a period's usage, read from an analytics item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UsageTotals {
    pub tokens_in_uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub tokens_out: u64,
}

fn n_of(item: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0)
}

impl UsageTotals {
    pub fn from_item(item: Option<&HashMap<String, AttributeValue>>) -> Self {
        let Some(i) = item else {
            return Self::default();
        };
        Self {
            tokens_in_uncached: n_of(i, "total_tokens_in"),
            cache_read: n_of(i, "cache_read_tokens"),
            cache_write: n_of(i, "cache_write_tokens"),
            tokens_out: n_of(i, "total_tokens_out"),
        }
    }

    /// All four parts. Summed from the parts (not `total_tokens`) so periods
    /// counted before `total_tokens` existed still add up.
    pub fn total(&self) -> u64 {
        self.tokens_in_uncached + self.cache_read + self.cache_write + self.tokens_out
    }

    /// The old limit rule: new input + output only.
    pub fn uncached_total(&self) -> u64 {
        self.tokens_in_uncached + self.tokens_out
    }

    /// Cache read ÷ (cache read + cache write + new input); 0 with no input.
    pub fn cache_hit_rate(&self) -> f64 {
        let denom = self.cache_read + self.cache_write + self.tokens_in_uncached;
        if denom == 0 {
            0.0
        } else {
            self.cache_read as f64 / denom as f64
        }
    }

    /// What the token limit compares with the team's `max_tokens`.
    pub fn counted(&self, counts_cache: bool) -> u64 {
        if counts_cache {
            self.total()
        } else {
            self.uncached_total()
        }
    }
}

/// Per-feature totals (all four parts summed) from an analytics item,
/// largest first.
pub fn feature_totals(
    item: Option<&HashMap<String, AttributeValue>>,
) -> Vec<(String, UsageTotals)> {
    let Some(i) = item else {
        return vec![];
    };
    let mut by: HashMap<String, UsageTotals> = HashMap::new();
    for (k, v) in i {
        let Some(rest) = k.strip_prefix("feature#") else {
            continue;
        };
        let Some((f, field)) = rest.rsplit_once('#') else {
            continue;
        };
        let n = v
            .as_n()
            .ok()
            .and_then(|n| n.parse::<f64>().ok())
            .unwrap_or(0.0)
            .max(0.0) as u64;
        let e = by.entry(f.to_string()).or_default();
        match field {
            "tokens_in" => e.tokens_in_uncached += n,
            "cache_read" => e.cache_read += n,
            "cache_write" => e.cache_write += n,
            "tokens_out" => e.tokens_out += n,
            _ => {}
        }
    }
    let mut out: Vec<_> = by.into_iter().filter(|(_, t)| t.total() > 0).collect();
    out.sort_by(|a, b| b.1.total().cmp(&a.1.total()).then(a.0.cmp(&b.0)));
    out
}

/// Whether the token limit counts cache tokens for a team: the team's
/// `counts_cache` on `SETTINGS#TOKEN_LIMIT` wins; otherwise the
/// `TOKEN_LIMIT_COUNTS_CACHE` env default (off unless "true").
pub fn counts_cache_enabled(team_setting: Option<bool>, env_default: Option<&str>) -> bool {
    team_setting.unwrap_or_else(|| {
        env_default
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn price_table_parses_and_has_every_model_in_use() {
        for m in [
            "claude-opus-5-5",
            "claude-sonnet-5-5",
            "claude-fable-5-1",
            "claude-haiku-5-5",
            "claude-opus-4-8",
            "claude-sonnet-4-6",
            "claude-haiku-4-5",
        ] {
            assert!(rates_for(m).1, "{m} missing from the price table");
        }
        let (o, _) = rates_for("claude-opus-5-5");
        assert_eq!(
            (
                o.input,
                o.output,
                o.cache_read,
                o.cache_write_5m,
                o.cache_write_1h
            ),
            (4.0, 20.0, 0.20, 5.0, 8.0)
        );
        let (s, _) = rates_for("claude-sonnet-5-5");
        assert_eq!(
            (
                s.input,
                s.output,
                s.cache_read,
                s.cache_write_5m,
                s.cache_write_1h
            ),
            (2.0, 10.0, 0.20, 2.5, 4.0)
        );
        let (f, _) = rates_for("claude-fable-5-1");
        assert_eq!((f.input, f.output, f.cache_read), (10.0, 50.0, 0.25));
    }

    #[test]
    fn writes_cost_more_than_input_and_reads_less() {
        for (m, r) in prices() {
            assert!(r.cache_write_5m > r.input, "{m}");
            assert!(r.cache_write_1h > r.cache_write_5m, "{m}");
            assert!(r.cache_read < r.input, "{m}");
            assert!(r.output > r.input, "{m}");
        }
    }

    #[test]
    fn model_ids_normalize() {
        assert_eq!(normalize_model("claude-opus-5-5"), "claude-opus-5-5");
        assert_eq!(normalize_model("Claude-Opus-5-5[1m]"), "claude-opus-5-5");
        assert_eq!(
            normalize_model("us.anthropic.claude-sonnet-4-6-v1:0"),
            "claude-sonnet-4-6"
        );
        assert_eq!(
            normalize_model("claude-opus-4-1-20250805"),
            "claude-opus-4-1"
        );
        assert_eq!(
            normalize_model("claude-opus-4-5@20251101"),
            "claude-opus-4-5"
        );
        assert_eq!(
            normalize_model("anthropic.claude-haiku-4-5"),
            "claude-haiku-4-5"
        );
    }

    #[test]
    fn unknown_model_costs_the_most_expensive_rate_never_zero() {
        let (r, known) = rates_for("claude-imaginary-9");
        assert!(!known);
        assert_eq!(r, most_expensive_rates());
        let call = CallUsage {
            uncached_input: 1_000_000,
            output: 1_000_000,
            ..Default::default()
        };
        let unknown = call.cost_usd("claude-imaginary-9");
        assert!(unknown > 0.0);
        for m in prices().keys() {
            assert!(
                unknown >= call.cost_usd(m),
                "{m} costs more than the fallback"
            );
        }
    }

    #[test]
    fn cost_uses_each_part_at_its_own_rate() {
        let call = CallUsage {
            uncached_input: 1_000_000,
            cache_read: 2_000_000,
            cache_write_5m: 1_000_000,
            cache_write_1h: 1_000_000,
            output: 1_000_000,
        };
        // Opus 5.5: 4 + 2×0.20 + 5 + 8 + 20
        assert!((call.cost_usd("claude-opus-5-5") - 37.4).abs() < 1e-9);
        // Sonnet 5.5: 2 + 2×0.20 + 2.5 + 4 + 10 (cache reads are $0.20/MTok)
        assert!((call.cost_usd("claude-sonnet-5-5") - 18.9).abs() < 1e-9);
    }

    #[test]
    fn haiku_long_prompts_pay_the_higher_tier() {
        let short = CallUsage {
            uncached_input: 50_000,
            output: 1_000,
            ..Default::default()
        };
        let long = CallUsage {
            uncached_input: 50_000,
            cache_read: 60_000,
            output: 1_000,
            ..Default::default()
        };
        let s = short.cost_usd("claude-haiku-5-5");
        assert!((s - (50_000.0 * 0.10 + 1_000.0 * 0.50) / 1e6).abs() < 1e-12);
        let l = long.cost_usd("claude-haiku-5-5");
        assert!((l - (50_000.0 * 0.50 + 60_000.0 * 0.05 + 1_000.0 * 2.50) / 1e6).abs() < 1e-12);
    }

    #[test]
    fn api_usage_parses_all_parts_and_the_ttl_split() {
        let u = json!({
            "input_tokens": 12, "output_tokens": 300,
            "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 900,
            "cache_creation": {"ephemeral_5m_input_tokens": 600, "ephemeral_1h_input_tokens": 300}
        });
        let c = CallUsage::from_api(&u, CacheTtl::FiveMinutes);
        assert_eq!(
            c,
            CallUsage {
                uncached_input: 12,
                cache_read: 5000,
                cache_write_5m: 600,
                cache_write_1h: 300,
                output: 300
            }
        );
        assert_eq!(c.total(), 12 + 5000 + 900 + 300);
        // No breakdown: all writes take the request's TTL.
        let u = json!({"input_tokens": 1, "output_tokens": 2, "cache_creation_input_tokens": 70});
        assert_eq!(
            CallUsage::from_api(&u, CacheTtl::OneHour).cache_write_1h,
            70
        );
        assert_eq!(
            CallUsage::from_api(&u, CacheTtl::FiveMinutes).cache_write_5m,
            70
        );
    }

    fn adds(feature: &str, model: &str, c: &CallUsage) -> HashMap<String, String> {
        let (expr, names, values) = add_expression(feature, model, c);
        assert!(expr.starts_with("ADD "));
        names
            .iter()
            .map(|(ph, attr)| {
                let v = values[&ph.replace("#a", ":v")].as_n().unwrap().clone();
                (attr.clone(), v)
            })
            .collect()
    }

    #[test]
    fn one_call_adds_every_counter_once() {
        let c = CallUsage {
            uncached_input: 10,
            cache_read: 1000,
            cache_write_5m: 200,
            cache_write_1h: 0,
            output: 50,
        };
        let a = adds("review", "claude-sonnet-5-5", &c);
        assert_eq!(a["total_tokens_in"], "10");
        assert_eq!(a["tokens_in_uncached"], "10");
        assert_eq!(a["cache_read_tokens"], "1000");
        assert_eq!(a["cache_write_tokens"], "200");
        assert_eq!(a["total_tokens_out"], "50");
        assert_eq!(a["total_tokens"], "1260");
        assert_eq!(a["model_calls"], "1");
        assert_eq!(a["model#claude-sonnet-5-5#cache_read"], "1000");
        assert_eq!(a["feature#review#tokens_out"], "50");
        let cost: f64 = a["total_cost_usd"].parse().unwrap();
        assert!((cost - c.cost_usd("claude-sonnet-5-5")).abs() < 1e-9);
        // Every attribute appears exactly once in the expression.
        let (expr, names, _) = add_expression("review", "claude-sonnet-5-5", &c);
        assert_eq!(expr.matches("#a").count(), names.len());
    }

    #[tokio::test]
    async fn the_meter_records_under_the_scoped_feature() {
        let (meter, calls) = UsageMeter::memory("t1");
        let c = CallUsage {
            uncached_input: 1,
            output: 1,
            ..Default::default()
        };
        meter.record("claude-opus-5-5", &c).await;
        with_feature(feature::REVIEW, meter.record("claude-opus-5-5", &c)).await;
        meter.record("claude-opus-5-5", &CallUsage::default()).await; // empty: skipped
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].feature, feature::OTHER);
        assert_eq!(got[1].feature, feature::REVIEW);
    }

    #[test]
    fn limit_counts_all_four_parts_only_when_enabled() {
        let mut item = HashMap::new();
        for (k, v) in [
            ("total_tokens_in", "120000"),
            ("cache_read_tokens", "61400000"),
            ("cache_write_tokens", "8800000"),
            ("total_tokens_out", "1000000"),
        ] {
            item.insert(k.to_string(), AttributeValue::N(v.into()));
        }
        let t = UsageTotals::from_item(Some(&item));
        assert_eq!(t.total(), 120_000 + 61_400_000 + 8_800_000 + 1_000_000);
        assert_eq!(t.counted(true), t.total());
        assert_eq!(t.counted(false), 1_120_000);
        let rate = t.cache_hit_rate();
        assert!((rate - 61_400_000.0 / 70_320_000.0).abs() < 1e-12);
        assert_eq!(UsageTotals::from_item(None).total(), 0);
        assert_eq!(UsageTotals::default().cache_hit_rate(), 0.0);
    }

    #[test]
    fn the_flag_defaults_off_and_the_team_setting_wins() {
        assert!(!counts_cache_enabled(None, None));
        assert!(!counts_cache_enabled(None, Some("false")));
        assert!(counts_cache_enabled(None, Some("TRUE")));
        assert!(counts_cache_enabled(Some(true), Some("false")));
        assert!(!counts_cache_enabled(Some(false), Some("true")));
    }

    #[test]
    fn feature_totals_group_the_breakdown() {
        let (_, names, values) = add_expression(
            "review",
            "claude-sonnet-5-5",
            &CallUsage {
                uncached_input: 1,
                cache_read: 2,
                cache_write_5m: 3,
                cache_write_1h: 0,
                output: 4,
            },
        );
        let item: HashMap<String, AttributeValue> = names
            .iter()
            .map(|(ph, attr)| (attr.clone(), values[&ph.replace("#a", ":v")].clone()))
            .collect();
        let f = feature_totals(Some(&item));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].0, "review");
        assert_eq!(f[0].1.total(), 10);
    }

    /// Exactly once: no service adds tokens or cost to the analytics counters
    /// except through this module (runs keep per-run totals on the run item).
    #[test]
    fn nothing_else_adds_tokens_or_cost_to_the_team_counters() {
        const COUNTERS: &[&str] = &[
            "total_tokens_in",
            "total_tokens_out",
            "cache_read_tokens",
            "cache_write_tokens",
            "total_cost_usd",
            "review_cost_usd",
            "total_tokens",
        ];
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = vec![];
        for svc in ["worker/src", "gateway/src"] {
            walk(&root.join(svc), &mut files);
        }
        assert!(files.len() > 20, "found the service sources");
        let mut offenders = vec![];
        for f in files {
            let src = std::fs::read_to_string(&f).unwrap();
            for (i, _) in src.match_indices("\"ADD ") {
                let rest = &src[i + 1..];
                let lit = &rest[..rest.find('"').unwrap_or(rest.len())];
                let words: Vec<&str> = lit
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .collect();
                if COUNTERS.iter().any(|c| words.contains(c)) {
                    offenders.push(format!("{}: {lit}", f.display()));
                }
            }
        }
        assert!(offenders.is_empty(), "counted twice: {offenders:#?}");
    }

    fn names_of(rec: &CallRecord, recovered: bool) -> HashMap<String, String> {
        let (expr, names, values) = record_expression(rec, recovered);
        assert_eq!(expr.matches(':').count(), names.len());
        names
            .iter()
            .map(|(ph, attr)| {
                let v = values[&ph.replacen('#', ":", 1).replace(":a", ":v")].clone();
                (attr.clone(), v.as_n().unwrap().clone())
            })
            .collect()
    }

    #[test]
    fn estimated_and_recovered_calls_are_counted_per_period() {
        let mut rec = CallRecord::now(
            "t1",
            "run",
            "claude-opus-5-5",
            CallUsage {
                uncached_input: 3,
                output: 4,
                ..Default::default()
            },
        );
        let plain = names_of(&rec, false);
        assert!(!plain.contains_key(ESTIMATED_CALLS));
        assert!(!plain.contains_key(RECOVERED_CALLS));
        rec.estimated = true;
        let a = names_of(&rec, true);
        assert_eq!(a[ESTIMATED_CALLS], "1");
        assert_eq!(a[RECOVERED_CALLS], "1");
        assert_eq!(a["total_tokens_out"], "4");
    }

    #[test]
    fn the_fallback_message_is_the_contract_sample() {
        let contract: Value = serde_json::from_str(crate::dlq::CONTRACT_JSON).unwrap();
        let sample = contract["gateway"]["usage_record"].clone();
        let rec: CallRecord = serde_json::from_value(sample.clone()).unwrap();
        assert_eq!(usage_record_message(&rec), sample);
    }

    #[test]
    fn the_usage_check_reads_off_ran_or_never() {
        assert_eq!(usage_check_json(None)["state"], "never");
        let off = usage_check_item(
            &UsageCheck::Off {
                secret: "coderhelm/prod/anthropic-admin-keys".into(),
            },
            "2026-10-23T00:00:00Z",
        );
        let j = usage_check_json(Some(&off));
        assert_eq!(j["state"], "off");
        assert_eq!(j["secret"], "coderhelm/prod/anthropic-admin-keys");
        let ran = usage_check_item(
            &UsageCheck::Ran {
                day: "2026-10-22".into(),
                teams: 3,
                flagged: 1,
                failed: 0,
            },
            "2026-10-23T00:00:00Z",
        );
        let j = usage_check_json(Some(&ran));
        assert_eq!(j["state"], "ran");
        assert_eq!(j["teams"], 3);
        assert_eq!(j["flagged_models"], 1);
        assert_eq!(off["team_id"], AttributeValue::S("__coderhelm__".into()));
    }

    #[test]
    fn the_log_analyzer_carries_the_same_price_table() {
        assert_eq!(
            PRICE_TABLE_JSON,
            include_str!("../../../lambda/log-analyzer/model_prices.json"),
            "copy services/common/model_prices.json to lambda/log-analyzer/"
        );
    }
}
