//! A small index partition in the settings table that names every record of
//! one kind, so a sweep reads those records by key instead of scanning the
//! whole table.
//!
//! The settings table holds analytics, markers and every team's settings
//! together; on a busy install it is well over 100,000 items. A sweep that
//! found its records with a filtered scan read all of it on every tick, and a
//! scan with a page cap stopped before reaching them at all.
//!
//! Layout (settings table):
//! - `pk = INDEX#<name>`, `sk = <member>`: one item per member (a team id, a
//!   name). Writers add the member when they save the record.
//! - `pk = INDEX#<name>`, `sk = #BUILT`: when the index was last rebuilt
//!   from a full scan (`at`, RFC 3339).
//!
//! Readers rebuild the index from one full scan when it was never built or
//! the last rebuild is older than [`REBUILD_AFTER_HOURS`], so records saved
//! before the index existed, or by a writer that failed to add itself, are
//! found within a day. Readers check each member's record by key and drop
//! members whose record is gone.

use std::collections::{BTreeSet, HashMap};

use aws_sdk_dynamodb::types::{AttributeValue, PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client as DynamoClient;
use chrono::{DateTime, Duration, Utc};
use tracing::{info, warn};

pub type Item = HashMap<String, AttributeValue>;

/// How old a rebuild may be before the next reader rebuilds again.
pub const REBUILD_AFTER_HOURS: i64 = 24;
const BUILT_SK: &str = "#BUILT";

/// One index: its name and how a full scan recognises its members.
pub struct Spec<'a> {
    /// `INDEX#<name>`.
    pub name: &'a str,
    /// The scan filter that matches the indexed records.
    pub filter: &'a str,
    pub values: Vec<(&'a str, AttributeValue)>,
    /// The member a matching record stands for (`None` skips it).
    pub member_of: fn(&Item) -> Option<String>,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

pub fn partition(name: &str) -> String {
    format!("INDEX#{name}")
}

/// Pure: whether a reader must rebuild, from the last rebuild time.
pub fn needs_rebuild(built_at: Option<&str>, now: DateTime<Utc>) -> bool {
    match built_at.and_then(|t| DateTime::parse_from_rfc3339(t).ok()) {
        Some(t) => now - t.with_timezone(&Utc) >= Duration::hours(REBUILD_AFTER_HOURS),
        None => true,
    }
}

/// Adds `member` to the index (writers call this when they save a record).
pub async fn add(
    dynamo: &DynamoClient,
    table: &str,
    name: &str,
    member: &str,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(&partition(name)))
        .item("sk", s(member))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("index {name}: {e}"))
}

/// Drops `member` from the index (its record is gone).
pub async fn remove(dynamo: &DynamoClient, table: &str, name: &str, member: &str) {
    if let Err(e) = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", s(&partition(name)))
        .key("sk", s(member))
        .send()
        .await
    {
        warn!(index = name, member, error = %e, "Could not drop a stale index member");
    }
}

/// Every member, rebuilding the index from one full scan when it is due.
pub async fn members(
    dynamo: &DynamoClient,
    table: &str,
    spec: &Spec<'_>,
) -> Result<Vec<String>, String> {
    let pk = partition(spec.name);
    let built = dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(&pk))
        .key("sk", s(BUILT_SK))
        .send()
        .await
        .map_err(|e| format!("index {}: {e}", spec.name))?;
    let built_at = built
        .item()
        .and_then(|i| i.get("at"))
        .and_then(|v| v.as_s().ok())
        .cloned();
    if needs_rebuild(built_at.as_deref(), Utc::now()) {
        rebuild(dynamo, table, spec).await?;
    }
    let mut out = BTreeSet::new();
    let mut start: Option<Item> = None;
    loop {
        let page = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :p")
            .expression_attribute_values(":p", s(&pk))
            .projection_expression("sk")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("index {}: {e}", spec.name))?;
        for i in page.items() {
            if let Some(m) = i.get("sk").and_then(|v| v.as_s().ok()) {
                if m != BUILT_SK {
                    out.insert(m.clone());
                }
            }
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out.into_iter().collect())
}

/// One full scan for the indexed records, written as members, then the
/// rebuild time. Every page is read: a cap would leave members out.
async fn rebuild(dynamo: &DynamoClient, table: &str, spec: &Spec<'_>) -> Result<(), String> {
    let pk = partition(spec.name);
    let mut found = BTreeSet::new();
    let (mut start, mut pages): (Option<Item>, usize) = (None, 0);
    loop {
        let mut scan = dynamo
            .scan()
            .table_name(table)
            .filter_expression(spec.filter)
            .set_exclusive_start_key(start.take());
        for (k, v) in &spec.values {
            scan = scan.expression_attribute_values(*k, v.clone());
        }
        let page = scan
            .send()
            .await
            .map_err(|e| format!("index {} rebuild: {e}", spec.name))?;
        pages += 1;
        found.extend(page.items().iter().filter_map(spec.member_of));
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    let members: Vec<String> = found.into_iter().collect();
    for chunk in members.chunks(25) {
        let writes = chunk
            .iter()
            .filter_map(|m| {
                PutRequest::builder()
                    .item("pk", s(&pk))
                    .item("sk", s(m))
                    .build()
                    .ok()
                    .map(|p| WriteRequest::builder().put_request(p).build())
            })
            .collect::<Vec<_>>();
        let mut pending = Some(HashMap::from([(table.to_string(), writes)]));
        for _ in 0..5 {
            let Some(req) = pending.take().filter(|r| r.values().any(|w| !w.is_empty())) else {
                break;
            };
            let r = dynamo
                .batch_write_item()
                .set_request_items(Some(req))
                .send()
                .await
                .map_err(|e| format!("index {} rebuild write: {e}", spec.name))?;
            pending = r.unprocessed_items().cloned();
        }
        if pending.is_some_and(|r| r.values().any(|w| !w.is_empty())) {
            return Err(format!(
                "index {} rebuild: some members were not written",
                spec.name
            ));
        }
    }
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(&pk))
        .item("sk", s(BUILT_SK))
        .item("at", s(&Utc::now().to_rfc3339()))
        .item("members", AttributeValue::N(members.len().to_string()))
        .send()
        .await
        .map_err(|e| format!("index {} rebuild mark: {e}", spec.name))?;
    info!(
        index = spec.name,
        pages,
        members = members.len(),
        "Rebuilt a settings index from a full scan"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_never_built_index_is_rebuilt() {
        assert!(needs_rebuild(None, Utc::now()));
        assert!(needs_rebuild(Some("not a time"), Utc::now()));
    }

    #[test]
    fn a_recent_rebuild_is_kept_and_an_old_one_is_redone() {
        let now = DateTime::parse_from_rfc3339("2026-11-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!needs_rebuild(Some("2026-11-07T00:00:01Z"), now));
        assert!(needs_rebuild(Some("2026-11-06T12:00:00Z"), now));
    }

    #[test]
    fn the_index_lives_in_its_own_partition() {
        assert_eq!(partition("LOG_AUTOFIX"), "INDEX#LOG_AUTOFIX");
    }
}
