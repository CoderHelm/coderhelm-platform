//! A team's monthly token limits, read from its `SETTINGS#TOKEN_LIMIT` item.
//!
//! A team can set three limits, each optional and each checked on its own:
//!
//! - `max_new_tokens`: new (uncached) input + output tokens.
//! - `max_cache_read_tokens`: input read from the prompt cache.
//! - `max_cache_write_tokens`: input written to the prompt cache.
//!
//! A missing field or 0 means no limit of that kind. Whichever limit is
//! reached first stops new work, and the reason names that limit.
//!
//! Teams set up before these fields existed have one `max_tokens` number and
//! an optional `counts_cache` flag. Until any of the three new fields is
//! written, that old pair is read exactly as before: one limit that counts new
//! input + output, or all four kinds of tokens when `counts_cache` is on (the
//! team's flag, else the `TOKEN_LIMIT_COUNTS_CACHE` env default). Saving the
//! new limits removes the old pair (see [`write_new_limits`]).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::usage::{counts_cache_enabled, UsageTotals};

type Item = HashMap<String, AttributeValue>;

/// The settings item's sort key.
pub const SETTINGS_SK: &str = "SETTINGS#TOKEN_LIMIT";

/// New (uncached) input + output tokens a month.
pub const MAX_NEW_TOKENS: &str = "max_new_tokens";
/// Cache read tokens a month.
pub const MAX_CACHE_READ_TOKENS: &str = "max_cache_read_tokens";
/// Cache write tokens a month.
pub const MAX_CACHE_WRITE_TOKENS: &str = "max_cache_write_tokens";
/// The old single limit (read only as a fallback).
pub const LEGACY_MAX_TOKENS: &str = "max_tokens";
/// The old flag: whether the single limit counts cache tokens too.
pub const LEGACY_COUNTS_CACHE: &str = "counts_cache";

/// Which limit a number belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LimitKind {
    /// New (uncached) input + output.
    NewTokens,
    CacheRead,
    CacheWrite,
    /// The old single limit; `counts_cache` = all four kinds, else new
    /// input + output.
    Legacy {
        counts_cache: bool,
    },
}

impl LimitKind {
    /// The three limits a team sets now, in display order.
    pub const ALL: [LimitKind; 3] = [
        LimitKind::NewTokens,
        LimitKind::CacheRead,
        LimitKind::CacheWrite,
    ];

    /// Stable id used in API bodies and warning keys. The old single limit
    /// counts what `new_tokens` counts unless it counts cache too.
    pub fn key(self) -> &'static str {
        match self {
            LimitKind::NewTokens => "new_tokens",
            LimitKind::CacheRead => "cache_read",
            LimitKind::CacheWrite => "cache_write",
            LimitKind::Legacy {
                counts_cache: false,
            } => "new_tokens",
            LimitKind::Legacy { counts_cache: true } => "all_tokens",
        }
    }

    /// The settings field that holds this limit.
    pub fn field(self) -> &'static str {
        match self {
            LimitKind::NewTokens => MAX_NEW_TOKENS,
            LimitKind::CacheRead => MAX_CACHE_READ_TOKENS,
            LimitKind::CacheWrite => MAX_CACHE_WRITE_TOKENS,
            LimitKind::Legacy { .. } => LEGACY_MAX_TOKENS,
        }
    }

    /// What the limit is called in messages ("the new tokens limit").
    pub fn label(self) -> &'static str {
        match self {
            LimitKind::NewTokens => "new tokens",
            LimitKind::CacheRead => "cache read tokens",
            LimitKind::CacheWrite => "cache write tokens",
            LimitKind::Legacy { .. } => "token",
        }
    }

    /// The month's tokens this limit counts.
    pub fn used(self, t: &UsageTotals) -> u64 {
        match self {
            LimitKind::NewTokens => t.uncached_total(),
            LimitKind::CacheRead => t.cache_read,
            LimitKind::CacheWrite => t.cache_write,
            LimitKind::Legacy { counts_cache } => t.counted(counts_cache),
        }
    }
}

/// One limit that is set (always > 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub kind: LimitKind,
    pub max: u64,
}

/// One limit compared with the month's usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitUse {
    pub kind: LimitKind,
    pub max: u64,
    pub used: u64,
}

impl LimitUse {
    pub fn reached(&self) -> bool {
        self.used >= self.max
    }

    /// Percent used, rounded down (as the warning thresholds compare it).
    pub fn pct(&self) -> u64 {
        if self.max == 0 {
            return 0;
        }
        ((self.used as u128 * 100) / self.max as u128) as u64
    }
}

/// A team's limits as read from its settings item.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenLimits {
    /// Only the limits that are set, in display order.
    pub limits: Vec<Limit>,
    /// True when read from the old `max_tokens` / `counts_cache` pair.
    pub legacy: bool,
}

fn n_field(item: &Item, k: &str) -> Option<u64> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
}

/// Whether the item has any of the three new fields (then the old pair is
/// not read, even if the new fields are all 0 = no limits).
pub fn has_new_fields(item: Option<&Item>) -> bool {
    item.is_some_and(|i| LimitKind::ALL.iter().any(|k| i.contains_key(k.field())))
}

impl TokenLimits {
    /// Reads the limits; `env_counts_cache` is the
    /// `TOKEN_LIMIT_COUNTS_CACHE` default for the old pair.
    pub fn from_item_with(item: Option<&Item>, env_counts_cache: Option<&str>) -> Self {
        let Some(i) = item else {
            return Self::default();
        };
        if has_new_fields(item) {
            let limits = LimitKind::ALL
                .iter()
                .filter_map(|&kind| {
                    n_field(i, kind.field())
                        .filter(|&max| max > 0)
                        .map(|max| Limit { kind, max })
                })
                .collect();
            return Self {
                limits,
                legacy: false,
            };
        }
        let max = n_field(i, LEGACY_MAX_TOKENS).unwrap_or(0);
        if max == 0 {
            return Self::default();
        }
        let team = i
            .get(LEGACY_COUNTS_CACHE)
            .and_then(|v| v.as_bool().ok())
            .copied();
        let counts_cache = counts_cache_enabled(team, env_counts_cache);
        Self {
            limits: vec![Limit {
                kind: LimitKind::Legacy { counts_cache },
                max,
            }],
            legacy: true,
        }
    }

    /// Reads the limits with the env default from the process environment.
    pub fn from_item(item: Option<&Item>) -> Self {
        Self::from_item_with(
            item,
            std::env::var("TOKEN_LIMIT_COUNTS_CACHE").ok().as_deref(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.limits.is_empty()
    }

    /// The set limit of one kind, if any.
    pub fn get(&self, kind: LimitKind) -> Option<u64> {
        self.limits.iter().find(|l| l.kind == kind).map(|l| l.max)
    }

    /// Every set limit compared with the month's usage.
    pub fn uses(&self, t: &UsageTotals) -> Vec<LimitUse> {
        self.limits
            .iter()
            .map(|l| LimitUse {
                kind: l.kind,
                max: l.max,
                used: l.kind.used(t),
            })
            .collect()
    }

    /// The first limit (in display order) the month's usage has reached.
    pub fn reached(&self, t: &UsageTotals) -> Option<LimitUse> {
        self.uses(t).into_iter().find(LimitUse::reached)
    }
}

/// Reads a team's limits from the settings table.
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    settings_table: &str,
    team_id: &str,
) -> Result<TokenLimits, aws_sdk_dynamodb::Error> {
    let out = dynamo
        .get_item()
        .table_name(settings_table)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(SETTINGS_SK.to_string()))
        .send()
        .await
        .map_err(aws_sdk_dynamodb::Error::from)?;
    Ok(TokenLimits::from_item(out.item()))
}

/// Reads a team's usage for a month (`YYYY-MM`) from the analytics table.
pub async fn load_month_usage(
    dynamo: &aws_sdk_dynamodb::Client,
    analytics_table: &str,
    team_id: &str,
    month: &str,
) -> Result<UsageTotals, aws_sdk_dynamodb::Error> {
    let out = dynamo
        .get_item()
        .table_name(analytics_table)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("period", AttributeValue::S(month.to_string()))
        .send()
        .await
        .map_err(aws_sdk_dynamodb::Error::from)?;
    Ok(UsageTotals::from_item(out.item()))
}

/// "1.5M", "250K", "900": short token counts for messages.
pub fn short_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        let m = n as f64 / 1_000_000.0;
        if n.is_multiple_of(1_000_000) {
            format!("{}M", n / 1_000_000)
        } else {
            format!("{m:.1}M")
        }
    } else if n >= 1_000 {
        if n.is_multiple_of(1_000) {
            format!("{}K", n / 1_000)
        } else {
            format!("{:.1}K", n as f64 / 1_000.0)
        }
    } else {
        n.to_string()
    }
}

/// The settings-item key for a warning already sent this month. The old
/// single limit keeps its old key, so a team that already had this month's
/// warning isn't sent it again.
pub fn warning_sk(month: &str, kind: LimitKind, level: u8) -> String {
    match kind {
        LimitKind::Legacy { .. } => format!("TOKEN_WARNING#{month}#{level}"),
        k => format!("TOKEN_WARNING#{month}#{}#{level}", k.key()),
    }
}

/// The warning level a limit has crossed: 100, 80, or none.
pub fn warning_level(u: &LimitUse) -> Option<u8> {
    match u.pct() {
        p if p >= 100 => Some(100),
        p if p >= 80 => Some(80),
        _ => None,
    }
}

/// Why a request to save limits was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadLimit(pub &'static str);

/// The new limits from a request body. `Ok(None)` when the body has none of
/// the three fields (an older client sending only `max_tokens`). Each field
/// is a whole number of tokens, or null / 0 for no limit.
pub fn parse_new_limits(body: &serde_json::Value) -> Result<Option<NewLimits>, BadLimit> {
    if !LimitKind::ALL.iter().any(|k| body.get(k.field()).is_some()) {
        return Ok(None);
    }
    let one = |kind: LimitKind| -> Result<Option<u64>, BadLimit> {
        match body.get(kind.field()) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(v) => v
                .as_u64()
                .map(|n| (n > 0).then_some(n))
                .ok_or(BadLimit(kind.field())),
        }
    };
    Ok(Some(NewLimits {
        new_tokens: one(LimitKind::NewTokens)?,
        cache_read: one(LimitKind::CacheRead)?,
        cache_write: one(LimitKind::CacheWrite)?,
    }))
}

/// The new limits a team saves; `None` or 0 = no limit of that kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NewLimits {
    pub new_tokens: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl NewLimits {
    fn of(&self, kind: LimitKind) -> u64 {
        match kind {
            LimitKind::NewTokens => self.new_tokens,
            LimitKind::CacheRead => self.cache_read,
            LimitKind::CacheWrite => self.cache_write,
            LimitKind::Legacy { .. } => None,
        }
        .unwrap_or(0)
    }
}

/// The update that saves the three new limits: each is written (0 = no
/// limit, so the item stays in the new form) and the old pair is removed.
/// Returns (expression, values).
pub fn new_limits_update(limits: &NewLimits) -> (String, HashMap<String, AttributeValue>) {
    let mut values = HashMap::new();
    let mut sets = Vec::new();
    for (i, kind) in LimitKind::ALL.iter().enumerate() {
        let placeholder = format!(":l{i}");
        sets.push(format!("{} = {placeholder}", kind.field()));
        values.insert(placeholder, AttributeValue::N(limits.of(*kind).to_string()));
    }
    let expr = format!(
        "SET {} REMOVE {LEGACY_MAX_TOKENS}, {LEGACY_COUNTS_CACHE}",
        sets.join(", ")
    );
    (expr, values)
}

/// Saves the three new limits on a team's settings item.
pub async fn write_new_limits(
    dynamo: &aws_sdk_dynamodb::Client,
    settings_table: &str,
    team_id: &str,
    limits: &NewLimits,
) -> Result<(), aws_sdk_dynamodb::Error> {
    let (expr, values) = new_limits_update(limits);
    dynamo
        .update_item()
        .table_name(settings_table)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(SETTINGS_SK.to_string()))
        .update_expression(expr)
        .set_expression_attribute_values(Some(values))
        .send()
        .await
        .map_err(aws_sdk_dynamodb::Error::from)?;
    Ok(())
}

// ── The check before every model call ──────────────────────────────────────

/// Why a call is refused when `hit` is reached: names the limit, the month's
/// count and the limit, and how to raise it.
pub fn call_refused_reason(hit: &LimitUse) -> String {
    format!(
        "Monthly {} limit reached ({} of {}). New model calls are stopped until \
         the month turns or an admin raises the limit in Settings, Token Limit.",
        hit.kind.label(),
        short_tokens(hit.used),
        short_tokens(hit.max)
    )
}

/// How long a team's limit settings are kept before they are read again.
/// The month's usage is read fresh on every call.
pub const LIMITS_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// The limit check every model call asks first ([`crate::usage::UsageMeter::admit`]):
/// the team's limits (kept [`LIMITS_TTL`]) against its month's counters on
/// the analytics table, read on each call. The counters are the ones every
/// call is added to as it finishes, so a long loop sees its own spend on its
/// next call. A failed read lets the call through (and logs it), as the
/// check at the start of a run always has: an outage of the settings or
/// analytics table must not stop every team's work.
pub struct DynamoLimitCheck {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub settings_table: String,
    pub analytics_table: String,
    cache: std::sync::Mutex<HashMap<String, (std::time::Instant, TokenLimits)>>,
}

impl DynamoLimitCheck {
    pub fn new(
        dynamo: aws_sdk_dynamodb::Client,
        settings_table: &str,
        analytics_table: &str,
    ) -> Self {
        Self {
            dynamo,
            settings_table: settings_table.to_string(),
            analytics_table: analytics_table.to_string(),
            cache: Default::default(),
        }
    }

    async fn limits(&self, team_id: &str) -> Option<TokenLimits> {
        if let Ok(c) = self.cache.lock() {
            if let Some((at, l)) = c.get(team_id) {
                if at.elapsed() < LIMITS_TTL {
                    return Some(l.clone());
                }
            }
        }
        match load(&self.dynamo, &self.settings_table, team_id).await {
            Ok(l) => {
                if let Ok(mut c) = self.cache.lock() {
                    c.insert(team_id.to_string(), (std::time::Instant::now(), l.clone()));
                }
                Some(l)
            }
            Err(e) => {
                tracing::warn!(team_id, error = %e, "Token limits not read; the call goes ahead");
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::usage::LimitCheck for DynamoLimitCheck {
    async fn reached(&self, team_id: &str) -> Option<String> {
        if self.settings_table.is_empty() || self.analytics_table.is_empty() {
            return None;
        }
        let limits = self.limits(team_id).await?;
        if limits.is_empty() {
            return None;
        }
        let month = chrono::Utc::now().format("%Y-%m").to_string();
        let totals = match load_month_usage(&self.dynamo, &self.analytics_table, team_id, &month)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(team_id, error = %e, "Month usage not read; the call goes ahead");
                return None;
            }
        };
        limits.reached(&totals).map(|hit| call_refused_reason(&hit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(fields: &[(&str, AttributeValue)]) -> Item {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn n(v: u64) -> AttributeValue {
        AttributeValue::N(v.to_string())
    }

    fn usage() -> UsageTotals {
        UsageTotals {
            tokens_in_uncached: 120_000,
            cache_read: 61_400_000,
            cache_write: 8_800_000,
            tokens_out: 1_000_000,
        }
    }

    #[test]
    fn no_item_or_empty_item_means_no_limits() {
        assert!(TokenLimits::from_item_with(None, None).is_empty());
        assert!(TokenLimits::from_item_with(Some(&item(&[])), None).is_empty());
        let zero = item(&[(LEGACY_MAX_TOKENS, n(0))]);
        assert!(TokenLimits::from_item_with(Some(&zero), Some("true")).is_empty());
    }

    #[test]
    fn old_max_tokens_alone_is_one_new_input_plus_output_limit() {
        let i = item(&[(LEGACY_MAX_TOKENS, n(1_000_000))]);
        let l = TokenLimits::from_item_with(Some(&i), None);
        assert!(l.legacy);
        assert_eq!(
            l.limits,
            vec![Limit {
                kind: LimitKind::Legacy {
                    counts_cache: false
                },
                max: 1_000_000
            }]
        );
        // Same number the old check compared: new input + output.
        let u = l.reached(&usage()).expect("1.12M >= 1M");
        assert_eq!(u.used, 1_120_000);
        assert_eq!(u.used, usage().counted(false));
        let under = item(&[(LEGACY_MAX_TOKENS, n(2_000_000))]);
        assert!(TokenLimits::from_item_with(Some(&under), None)
            .reached(&usage())
            .is_none());
    }

    #[test]
    fn old_counts_cache_flag_and_env_default_still_count_all_four_parts() {
        let flagged = item(&[
            (LEGACY_MAX_TOKENS, n(50_000_000)),
            (LEGACY_COUNTS_CACHE, AttributeValue::Bool(true)),
        ]);
        let l = TokenLimits::from_item_with(Some(&flagged), Some("false"));
        let u = l.reached(&usage()).expect("71.32M >= 50M");
        assert_eq!(u.used, usage().total());
        assert_eq!(u.kind, LimitKind::Legacy { counts_cache: true });

        // No team flag: the env default decides, as before.
        let plain = item(&[(LEGACY_MAX_TOKENS, n(50_000_000))]);
        assert!(TokenLimits::from_item_with(Some(&plain), Some("TRUE"))
            .reached(&usage())
            .is_some());
        assert!(TokenLimits::from_item_with(Some(&plain), None)
            .reached(&usage())
            .is_none());

        // The team's own `false` beats an env default of true.
        let off = item(&[
            (LEGACY_MAX_TOKENS, n(50_000_000)),
            (LEGACY_COUNTS_CACHE, AttributeValue::Bool(false)),
        ]);
        assert!(TokenLimits::from_item_with(Some(&off), Some("true"))
            .reached(&usage())
            .is_none());
    }

    #[test]
    fn new_fields_win_over_the_old_pair_even_when_all_zero() {
        let i = item(&[
            (LEGACY_MAX_TOKENS, n(1)),
            (LEGACY_COUNTS_CACHE, AttributeValue::Bool(true)),
            (MAX_NEW_TOKENS, n(0)),
        ]);
        let l = TokenLimits::from_item_with(Some(&i), Some("true"));
        assert!(!l.legacy);
        assert!(l.is_empty(), "0 = no limit; the old pair is not read");
    }

    #[test]
    fn each_new_limit_is_checked_on_its_own() {
        let i = item(&[
            (MAX_NEW_TOKENS, n(2_000_000)),
            (MAX_CACHE_READ_TOKENS, n(100_000_000)),
            (MAX_CACHE_WRITE_TOKENS, n(8_000_000)),
        ]);
        let l = TokenLimits::from_item_with(Some(&i), None);
        let uses = l.uses(&usage());
        assert_eq!(uses.len(), 3);
        assert_eq!(uses[0].used, 1_120_000);
        assert_eq!(uses[1].used, 61_400_000);
        assert_eq!(uses[2].used, 8_800_000);
        // Only cache write is over its own limit.
        let hit = l.reached(&usage()).expect("cache write over");
        assert_eq!(hit.kind, LimitKind::CacheWrite);
        assert_eq!(hit.kind.label(), "cache write tokens");

        // An unset kind never stops work, however large its usage.
        let only_new = item(&[(MAX_NEW_TOKENS, n(5_000_000))]);
        let l = TokenLimits::from_item_with(Some(&only_new), None);
        assert_eq!(l.limits.len(), 1);
        assert!(l.reached(&usage()).is_none());
        assert_eq!(l.get(LimitKind::NewTokens), Some(5_000_000));
        assert_eq!(l.get(LimitKind::CacheRead), None);
    }

    #[test]
    fn warning_levels_and_keys_are_per_limit() {
        let u = |used, max| LimitUse {
            kind: LimitKind::CacheRead,
            max,
            used,
        };
        assert_eq!(warning_level(&u(79, 100)), None);
        assert_eq!(warning_level(&u(80, 100)), Some(80));
        assert_eq!(warning_level(&u(99, 100)), Some(80));
        assert_eq!(warning_level(&u(100, 100)), Some(100));
        assert_eq!(warning_level(&u(500, 100)), Some(100));
        assert_eq!(
            warning_sk("2026-10", LimitKind::CacheRead, 80),
            "TOKEN_WARNING#2026-10#cache_read#80"
        );
        assert_ne!(
            warning_sk("2026-10", LimitKind::NewTokens, 80),
            warning_sk("2026-10", LimitKind::CacheWrite, 80)
        );
        // The old limit keeps the old key.
        assert_eq!(
            warning_sk(
                "2026-10",
                LimitKind::Legacy {
                    counts_cache: false
                },
                100
            ),
            "TOKEN_WARNING#2026-10#100"
        );
    }

    #[test]
    fn saving_writes_all_three_and_drops_the_old_pair() {
        let (expr, values) = new_limits_update(&NewLimits {
            new_tokens: Some(5_000_000),
            cache_read: None,
            cache_write: Some(0),
        });
        assert_eq!(
            expr,
            "SET max_new_tokens = :l0, max_cache_read_tokens = :l1, max_cache_write_tokens = :l2 \
             REMOVE max_tokens, counts_cache"
        );
        assert_eq!(values[":l0"], n(5_000_000));
        assert_eq!(values[":l1"], n(0));
        assert_eq!(values[":l2"], n(0));

        // Read back: only the one set limit, not legacy.
        let mut saved: Item = values
            .iter()
            .map(|(k, v)| {
                let field = match k.as_str() {
                    ":l0" => MAX_NEW_TOKENS,
                    ":l1" => MAX_CACHE_READ_TOKENS,
                    _ => MAX_CACHE_WRITE_TOKENS,
                };
                (field.to_string(), v.clone())
            })
            .collect();
        saved.insert("pk".into(), AttributeValue::S("t".into()));
        let l = TokenLimits::from_item_with(Some(&saved), Some("true"));
        assert!(!l.legacy);
        assert_eq!(
            l.limits,
            vec![Limit {
                kind: LimitKind::NewTokens,
                max: 5_000_000
            }]
        );
    }

    #[test]
    fn request_bodies_parse_per_limit() {
        use serde_json::json;
        assert_eq!(parse_new_limits(&json!({ "max_tokens": 5 })), Ok(None));
        assert_eq!(
            parse_new_limits(&json!({
                "max_new_tokens": 5_000_000,
                "max_cache_read_tokens": null,
                "max_cache_write_tokens": 0,
            })),
            Ok(Some(NewLimits {
                new_tokens: Some(5_000_000),
                cache_read: None,
                cache_write: None,
            }))
        );
        assert_eq!(
            parse_new_limits(&json!({ "max_cache_read_tokens": 7 })),
            Ok(Some(NewLimits {
                new_tokens: None,
                cache_read: Some(7),
                cache_write: None,
            }))
        );
        assert_eq!(
            parse_new_limits(&json!({ "max_new_tokens": -1 })),
            Err(BadLimit(MAX_NEW_TOKENS))
        );
        assert_eq!(
            parse_new_limits(&json!({ "max_cache_write_tokens": "lots" })),
            Err(BadLimit(MAX_CACHE_WRITE_TOKENS))
        );
        assert_eq!(
            parse_new_limits(&json!({ "max_cache_read_tokens": 1.5 })),
            Err(BadLimit(MAX_CACHE_READ_TOKENS))
        );
    }

    #[test]
    fn short_tokens_reads_well() {
        assert_eq!(short_tokens(900), "900");
        assert_eq!(short_tokens(250_000), "250K");
        assert_eq!(short_tokens(1_500), "1.5K");
        assert_eq!(short_tokens(2_000_000), "2M");
        assert_eq!(short_tokens(1_120_000), "1.1M");
    }

    #[test]
    fn a_refused_call_names_the_limit_and_the_numbers() {
        let l = TokenLimits {
            limits: vec![Limit {
                kind: LimitKind::CacheWrite,
                max: 8_000_000,
            }],
            legacy: false,
        };
        let hit = l.reached(&usage()).expect("8.8M of 8M cache writes");
        let r = call_refused_reason(&hit);
        assert!(r.contains("cache write tokens limit reached"), "{r}");
        assert!(r.contains("8.8M of 8M"), "{r}");
        assert!(!r.contains('\u{2192}'), "no arrow glyphs: {r}");
    }
}
