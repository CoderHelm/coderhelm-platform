//! Reading the global feed records (the worker writes them:
//! `worker::passes::infra_feeds`). Every row is `pk` = [`super::GLOBAL_PK`],
//! its JSON in `json`.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use super::feeds::{self, Advisory, Bulletin, FeedStatus, LatestImage, Runtime};

type Item = HashMap<String, AttributeValue>;

fn json_of(i: &Item) -> &str {
    i.get("json")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

/// One row as `T`.
pub async fn get<T: DeserializeOwned>(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    sk: &str,
) -> Option<T> {
    let o = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(super::GLOBAL_PK.into()))
        .key("sk", AttributeValue::S(sk.into()))
        .send()
        .await
        .ok()?;
    serde_json::from_str(json_of(o.item()?)).ok()
}

/// Every row whose sort key starts with `prefix`, as `T`.
pub async fn all<T: DeserializeOwned>(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    prefix: &str,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..200 {
        let Ok(r) = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(super::GLOBAL_PK.into()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        out.extend(
            r.items()
                .iter()
                .filter_map(|i| serde_json::from_str(json_of(i)).ok()),
        );
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    out
}

pub async fn advisories(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Vec<Advisory> {
    all(dynamo, table, super::ADVISORY_PREFIX).await
}

pub async fn bulletins(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Vec<Bulletin> {
    all(dynamo, table, super::BULLETIN_PREFIX).await
}

pub async fn runtimes(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Vec<Runtime> {
    get(dynamo, table, super::RUNTIMES_SK)
        .await
        .unwrap_or_default()
}

pub async fn latest_images(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Vec<LatestImage> {
    get(dynamo, table, super::LATEST_IMAGES_SK)
        .await
        .unwrap_or_default()
}

/// One feed's status (empty when never read).
pub async fn status(dynamo: &aws_sdk_dynamodb::Client, table: &str, feed: &str) -> FeedStatus {
    get(dynamo, table, &super::feed_status_sk(feed))
        .await
        .unwrap_or(FeedStatus {
            feed: feed.to_string(),
            ..Default::default()
        })
}

/// Every feed's status, in [`feeds::FEEDS`] order.
pub async fn statuses(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Vec<FeedStatus> {
    let mut out = Vec::new();
    for (f, _) in feeds::FEEDS {
        out.push(status(dynamo, table, f).await);
    }
    out
}

/// What the Sources panel shows: each feed's name, when it last read, how
/// many items, and its last error (a page that changed its format).
pub fn sources_view(statuses: &[FeedStatus], now: chrono::DateTime<chrono::Utc>) -> Value {
    let rows: Vec<Value> = feeds::FEEDS
        .iter()
        .map(|(key, name)| {
            let s = statuses.iter().find(|s| s.feed == *key);
            json!({
                "feed": key,
                "name": name,
                "read_at": s.map(|s| s.ok_at.as_str()).filter(|v| !v.is_empty()),
                "count": s.map(|s| s.count).unwrap_or(0),
                "fresh": s.is_some_and(|s| s.fresh(now)),
                "error": s.map(|s| s.error.as_str()).filter(|v| !v.is_empty()),
                "error_at": s.map(|s| s.error_at.as_str()).filter(|v| !v.is_empty()),
            })
        })
        .collect();
    json!({ "feeds": rows })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sources_view_lists_every_feed_and_its_error() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let st = vec![
            FeedStatus {
                feed: "alas2023".into(),
                ..Default::default()
            }
            .ok(3135, "2026-11-10T06:00:00Z"),
            FeedStatus {
                feed: "bulletins".into(),
                ..Default::default()
            }
            .ok(43, "2026-11-08T06:00:00Z")
            .failed(
                "none of 43 bulletin pages could be read",
                "2026-11-10T06:00:00Z",
            ),
        ];
        let v = sources_view(&st, now);
        let rows = v["feeds"].as_array().unwrap();
        assert_eq!(rows.len(), feeds::FEEDS.len());
        assert_eq!(rows[0]["fresh"], true);
        assert_eq!(rows[0]["count"], 3135);
        let b = rows.iter().find(|r| r["feed"] == "bulletins").unwrap();
        assert_eq!(b["fresh"], false);
        assert!(b["error"].as_str().unwrap().contains("bulletin pages"));
        let never = rows.iter().find(|r| r["feed"] == "bottlerocket").unwrap();
        assert!(never["read_at"].is_null());
    }
}
