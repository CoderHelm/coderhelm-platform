//! The team's daily alert token budget (`pk = team, sk = ALERT_BUDGET`):
//! how many tokens alert runs may spend in 24 hours. The alerts route and
//! Setup both read and write it here.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

pub type Item = HashMap<String, AttributeValue>;

pub const SK: &str = "ALERT_BUDGET";
/// The budget when the team never set one.
pub const DEFAULT_DAILY_TOKENS: u64 = 500_000;
/// The highest budget a save accepts.
pub const MAX_DAILY_TOKENS: u64 = 100_000_000;

/// The saved budget, when there is one.
pub fn saved(item: Option<&Item>) -> Option<u64> {
    item.and_then(|i| i.get("daily_tokens"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
}

/// The budget above [`MAX_DAILY_TOKENS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooHigh;

/// The item a save writes. 0 stops every alert run.
pub fn item(team_id: &str, daily_tokens: u64, by: &str, now: &str) -> Result<Item, TooHigh> {
    if daily_tokens > MAX_DAILY_TOKENS {
        return Err(TooHigh);
    }
    let s = |v: &str| AttributeValue::S(v.to_string());
    Ok(HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(SK)),
        (
            "daily_tokens".to_string(),
            AttributeValue::N(daily_tokens.to_string()),
        ),
        ("updated_at".to_string(), s(now)),
        ("updated_by".to_string(), s(by)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saves_reads_back_and_caps() {
        let i = item("T", 1_000, "a@x.io", "now").unwrap();
        assert_eq!(saved(Some(&i)), Some(1_000));
        assert_eq!(saved(None), None);
        assert_eq!(item("T", MAX_DAILY_TOKENS + 1, "a", "now"), Err(TooHigh));
    }
}
