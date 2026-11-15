//! A run's events (CI results, live tool calls) in the events table, under
//! `RUN#<run_id>`, each row stamped with the team that owns the run.
//!
//! The key holds only the run id, so a run id that ever named runs of two
//! teams would share one partition. Every row therefore carries `team_id`,
//! and every read keeps only the rows of the team that asked
//! ([`owned_by`]). All access goes through this module: a source scan
//! (incident case `bl_team_isolation`) fails on a direct read or write of the
//! events table anywhere else.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The attribute that names the run's team on every row.
pub const TEAM_ATTR: &str = "team_id";

/// Rows written before every row carried a team are kept until this day;
/// the table's 30-day TTL has removed them all by then. After it, a row
/// with no team is never shown.
pub const UNSTAMPED_UNTIL: &str = "2026-12-20";

/// The partition of one run's events.
pub fn pk(run_id: &str) -> String {
    format!("RUN#{run_id}")
}

/// A write of one event row for `team`'s run, with its key and team set.
pub fn put(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    run_id: &str,
    sk: &str,
) -> aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder {
    client
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(pk(run_id)))
        .item("sk", AttributeValue::S(sk.to_string()))
        .item(TEAM_ATTR, AttributeValue::S(team_id.to_string()))
}

/// A query of one run's rows whose sort key starts with `prefix`. The rows
/// it returns must pass [`owned_by`] before they are used.
pub fn query(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    run_id: &str,
    prefix: &str,
) -> aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder {
    client
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(pk(run_id)))
        .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_string()))
}

/// An update of one row, by the key a read that passed [`owned_by`] gave.
pub fn update(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    run_id: &str,
    sk: &str,
) -> aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder {
    client
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk(run_id)))
        .key("sk", AttributeValue::S(sk.to_string()))
}

/// True when the row belongs to `team_id`. A row from before rows were
/// stamped counts as the asking team's only until [`UNSTAMPED_UNTIL`]
/// (`today` is `YYYY-MM-DD`); the caller has already checked that the run
/// itself is the team's.
pub fn owned_by(item: &HashMap<String, AttributeValue>, team_id: &str, today: &str) -> bool {
    match item.get(TEAM_ATTR).and_then(|v| v.as_s().ok()) {
        Some(t) => !team_id.is_empty() && t == team_id,
        None => today < UNSTAMPED_UNTIL,
    }
}

/// Today as `YYYY-MM-DD`, for [`owned_by`].
pub fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(team: Option<&str>) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::from([("pk".to_string(), AttributeValue::S(pk("01RUN")))]);
        if let Some(t) = team {
            m.insert(TEAM_ATTR.to_string(), AttributeValue::S(t.to_string()));
        }
        m
    }

    #[test]
    fn a_row_is_only_ever_its_teams() {
        let a = row(Some("TEAM#a"));
        assert!(owned_by(&a, "TEAM#a", "2026-11-15"));
        assert!(
            !owned_by(&a, "TEAM#b", "2026-11-15"),
            "B never sees A's row"
        );
        assert!(!owned_by(&a, "", "2026-11-15"));
    }

    #[test]
    fn unstamped_rows_expire_with_the_ttl() {
        let old = row(None);
        assert!(owned_by(&old, "TEAM#a", "2026-11-15"));
        assert!(!owned_by(&old, "TEAM#a", UNSTAMPED_UNTIL));
        assert!(!owned_by(&old, "TEAM#a", "2027-01-01"));
    }

    #[test]
    fn the_partition_is_the_run() {
        assert_eq!(pk("01RUN"), "RUN#01RUN");
    }
}
