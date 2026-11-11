//! How many items a source keeps, and what embedding them costs.
//!
//! The per-source limit is a team setting (`pk` = team, `sk` =
//! [`SETTINGS_SK`]) admins change on the Knowledge page, between
//! [`MIN_ITEMS_PER_SOURCE`] and [`MAX_ITEMS_PER_SOURCE`];
//! [`DEFAULT_ITEMS_PER_SOURCE`] until they do. A source also never takes a
//! base past [`MAX_ITEMS_PER_BASE`] ([`effective`]).
//!
//! Embedding is what a sync costs: Titan Text Embeddings V2, priced from
//! the price table ([`crate::usage::embedding_cost_usd`]). A large first sync
//! shows its estimate before it starts ([`super::LARGE_SYNC_ITEMS`]), and
//! every sync's embeddings count against the team's monthly token limit
//! ([`token_room`]): when the limit is reached the sync stops and says so.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use super::{
    DEFAULT_ITEMS_PER_SOURCE, MAX_ITEMS_PER_BASE, MAX_ITEMS_PER_SOURCE, MIN_ITEMS_PER_SOURCE,
};

/// The settings record's sort key.
pub const SETTINGS_SK: &str = "KNOWLEDGE#SETTINGS";

/// Embedding tokens one synced item takes when nothing better is known
/// (two passages of about 400 tokens). Only for the worst case shown next
/// to the limit; a first sync's estimate samples the items themselves.
pub const TOKENS_PER_ITEM: u64 = 800;

/// The analytics counter Titan embeddings are added to (see
/// [`crate::usage::own_add_expression`]).
pub const EMBEDDING_TOKENS_ATTR: &str = "coderhelm#feature#embedding#tokens_in";

/// A team's knowledge settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    /// Items one source keeps (0: the default).
    #[serde(default)]
    pub items_per_source: u32,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub updated_by: String,
}

impl Settings {
    /// The per-source limit in force.
    pub fn items_per_source(&self) -> u32 {
        if self.items_per_source == 0 {
            DEFAULT_ITEMS_PER_SOURCE
        } else {
            self.items_per_source
                .clamp(MIN_ITEMS_PER_SOURCE, MAX_ITEMS_PER_SOURCE)
        }
    }
}

/// A per-source limit an admin typed, checked.
pub fn check_items(n: i64) -> Result<u32, String> {
    let (lo, hi) = (MIN_ITEMS_PER_SOURCE as i64, MAX_ITEMS_PER_SOURCE as i64);
    if (lo..=hi).contains(&n) {
        Ok(n as u32)
    } else {
        Err(format!(
            "Pick a number from {} to {}.",
            group(lo as u64),
            group(hi as u64)
        ))
    }
}

/// `50000` as `50,000`.
pub fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The most items a source keeps: the team's limit, or what the base has
/// left when its other sources already hold `others`. The reason names
/// which limit binds (`source` or `base`), for the page.
pub fn effective(team_limit: u32, others: u64) -> (u32, &'static str) {
    let left = (MAX_ITEMS_PER_BASE as u64).saturating_sub(others) as u32;
    if left < team_limit {
        (left, "base")
    } else {
        (team_limit, "source")
    }
}

/// Embedding tokens of one passage's input (about four bytes a token, the
/// way Titan's own count runs on English text).
pub fn tokens_of(text_len: usize) -> u64 {
    (text_len as u64).div_ceil(4).max(1)
}

/// USD for `tokens` embedding tokens.
pub fn usd(tokens: u64) -> f64 {
    crate::usage::embedding_cost_usd(crate::embed::TITAN_V2, tokens)
}

/// USD per million embedding tokens.
pub fn usd_per_million() -> f64 {
    usd(1_000_000)
}

/// The estimate for `items` items whose sampled ones took `sample` tokens
/// each (no sample: [`TOKENS_PER_ITEM`] each).
pub fn estimate(items: u64, sample: &[u64], token_left: Option<u64>, at: &str) -> super::Estimate {
    let per = if sample.is_empty() {
        TOKENS_PER_ITEM
    } else {
        sample.iter().sum::<u64>().div_ceil(sample.len() as u64)
    };
    let tokens = per.saturating_mul(items);
    super::Estimate {
        items,
        tokens,
        usd: usd(tokens),
        token_left,
        at: at.to_string(),
    }
}

/// Where a team stands against its monthly token limit, embeddings
/// included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRoom {
    pub limit: u64,
    pub used: u64,
    pub left: u64,
}

/// The team's room under its monthly limit on new tokens, given the month's
/// totals item: the team's own new tokens plus the embeddings CoderHelm
/// meters for it. `None` when the team has no such limit.
pub fn room_of(
    limits: &crate::token_limits::TokenLimits,
    month: Option<&HashMap<String, AttributeValue>>,
) -> Option<TokenRoom> {
    use crate::token_limits::LimitKind;
    let limit = limits
        .limits
        .iter()
        .find(|l| matches!(l.kind, LimitKind::NewTokens | LimitKind::Legacy { .. }))
        .copied()?;
    let totals = crate::usage::UsageTotals::from_item(month);
    let embedded = month
        .and_then(|m| m.get(EMBEDDING_TOKENS_ATTR))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0);
    let used = limit.kind.used(&totals).saturating_add(embedded);
    Some(TokenRoom {
        limit: limit.max,
        used,
        left: limit.max.saturating_sub(used),
    })
}

/// Reads the team's room under its monthly token limit (`None`: no limit,
/// or it couldn't be read; the sync then goes ahead, like model calls do).
pub async fn token_room(
    dynamo: &aws_sdk_dynamodb::Client,
    settings_table: &str,
    analytics_table: &str,
    team: &str,
) -> Option<TokenRoom> {
    if settings_table.is_empty() || analytics_table.is_empty() {
        return None;
    }
    let limits = crate::token_limits::load(dynamo, settings_table, team)
        .await
        .map_err(|e| tracing::warn!(error = %e, "Token limits not read for a knowledge sync"))
        .ok()?;
    if limits.is_empty() {
        return None;
    }
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let item = dynamo
        .get_item()
        .table_name(analytics_table)
        .key("team_id", AttributeValue::S(team.to_string()))
        .key("period", AttributeValue::S(month))
        .send()
        .await
        .map_err(|e| tracing::warn!(error = %e, "Month usage not read for a knowledge sync"))
        .ok()?;
    room_of(&limits, item.item())
}

/// Reads the team's knowledge settings (none saved: the defaults).
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
) -> Result<Settings, String> {
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(SETTINGS_SK.into()))
        .send()
        .await
        .map_err(|e| format!("knowledge settings: {e}"))?;
    Ok(r.item()
        .and_then(crate::knowledge::read_data::<Settings>)
        .unwrap_or_default())
}

pub async fn save(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    s: &Settings,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(team.to_string()))
        .item("sk", AttributeValue::S(SETTINGS_SK.into()))
        .item(
            "data",
            AttributeValue::S(serde_json::to_string(s).map_err(|e| e.to_string())?),
        )
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("knowledge settings save: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_limit_defaults_and_stays_in_range() {
        assert_eq!(Settings::default().items_per_source(), 50_000);
        let s = Settings {
            items_per_source: 7,
            ..Default::default()
        };
        assert_eq!(s.items_per_source(), MIN_ITEMS_PER_SOURCE);
        assert!(check_items(999).is_err());
        assert!(check_items(100_001).is_err());
        assert_eq!(check_items(20_000), Ok(20_000));
        assert_eq!(
            check_items(0).unwrap_err(),
            "Pick a number from 1,000 to 100,000."
        );
        assert_eq!(group(4_812), "4,812");
        assert_eq!(group(100), "100");
        assert_eq!(group(1_000_000), "1,000,000");
    }

    #[test]
    fn a_base_near_its_own_limit_binds_first() {
        assert_eq!(effective(50_000, 0), (50_000, "source"));
        assert_eq!(effective(50_000, 70_000), (30_000, "base"));
        assert_eq!(effective(50_000, 200_000), (0, "base"));
    }

    #[test]
    fn estimates_are_priced_from_the_sample() {
        let e = estimate(4_812, &[700, 900], Some(1_000_000), "now");
        assert_eq!(e.tokens, 800 * 4_812);
        assert!(e.usd > 0.0 && e.usd < 1.0, "{}", e.usd);
        assert_eq!(e.token_left, Some(1_000_000));
        // No sample: the per-item guess.
        assert_eq!(estimate(10, &[], None, "").tokens, 10 * TOKENS_PER_ITEM);
        assert!((usd_per_million() - usd(1_000_000)).abs() < 1e-12);
        assert_eq!(tokens_of(0), 1);
        assert_eq!(tokens_of(8), 2);
    }

    #[test]
    fn embeddings_count_against_the_new_token_limit() {
        let limits_item: HashMap<String, AttributeValue> = HashMap::from([(
            crate::token_limits::MAX_NEW_TOKENS.to_string(),
            AttributeValue::N("1000000".into()),
        )]);
        let limits = crate::token_limits::TokenLimits::from_item(Some(&limits_item));
        let month: HashMap<String, AttributeValue> = HashMap::from([
            (
                "total_tokens_in".to_string(),
                AttributeValue::N("100000".into()),
            ),
            (
                "total_tokens_out".to_string(),
                AttributeValue::N("50000".into()),
            ),
            (
                EMBEDDING_TOKENS_ATTR.to_string(),
                AttributeValue::N("250000".into()),
            ),
        ]);
        let r = room_of(&limits, Some(&month)).unwrap();
        assert_eq!((r.limit, r.used, r.left), (1_000_000, 400_000, 600_000));
        // No limit: nothing to count against.
        let none = crate::token_limits::TokenLimits::from_item(None);
        assert_eq!(room_of(&none, Some(&month)), None);
    }
}
