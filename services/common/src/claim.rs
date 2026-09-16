//! Expiring "do this once" claims on a DynamoDB item.
//!
//! A claim is an item whose `ttl` attribute (epoch seconds) is the moment it
//! stops blocking. DynamoDB's TTL sweeper deletes expired items lazily — often
//! hours or days late — so the conditional write must compare `ttl` against
//! the current time itself. A condition of only `attribute_not_exists(pk)`
//! keeps honoring an expired claim until the sweeper happens to run.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

/// Condition under which a claim can be taken: no live item holds it.
pub const CLAIM_CONDITION: &str = "attribute_not_exists(pk) OR #ttl < :now";

#[derive(Debug, PartialEq, Eq)]
pub enum Claim {
    /// This caller holds the claim until `ttl`.
    Won,
    /// A live claim already exists.
    Held,
    /// The write failed for another reason (throttle, network, permissions).
    Failed(String),
}

impl Claim {
    /// Treat an unknown outcome as won. For claims whose only purpose is to
    /// suppress duplicates, losing a real action is worse than a duplicate.
    pub fn won_or_failed_open(&self) -> bool {
        !matches!(self, Claim::Held)
    }
}

/// Take the claim `(pk, sk)` for `ttl_secs`. An expired claim is taken over.
pub async fn claim(dynamo: &DynamoClient, table: &str, pk: &str, sk: &str, ttl_secs: u64) -> Claim {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let result = dynamo
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(pk.to_string()))
        .item("sk", AttributeValue::S(sk.to_string()))
        .item("ttl", AttributeValue::N((now + ttl_secs).to_string()))
        .item(
            "claimed_at",
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        )
        .condition_expression(CLAIM_CONDITION)
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
        .send()
        .await;
    match result {
        Ok(_) => Claim::Won,
        Err(e) => {
            let held = e
                .as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false);
            if held {
                Claim::Held
            } else {
                Claim::Failed(e.to_string())
            }
        }
    }
}

/// Move an existing claim's expiry to `ttl_secs` from now (for example, turn a
/// short in-progress lease into a longer "done" marker). Best-effort.
pub async fn extend(dynamo: &DynamoClient, table: &str, pk: &str, sk: &str, ttl_secs: u64) {
    let until = chrono::Utc::now().timestamp().max(0) as u64 + ttl_secs;
    let _ = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .update_expression("SET #ttl = :until")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":until", AttributeValue::N(until.to_string()))
        .send()
        .await;
}

/// Drop a claim so the same action can run again. Best-effort.
pub async fn release(dynamo: &DynamoClient, table: &str, pk: &str, sk: &str) {
    let _ = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condition_takes_over_expired_claims() {
        assert!(CLAIM_CONDITION.contains("attribute_not_exists(pk)"));
        assert!(CLAIM_CONDITION.contains("#ttl < :now"));
    }

    #[test]
    fn only_held_blocks_fail_open_callers() {
        assert!(Claim::Won.won_or_failed_open());
        assert!(Claim::Failed("throttled".into()).won_or_failed_open());
        assert!(!Claim::Held.won_or_failed_open());
    }
}
