//! Paginated DynamoDB lookups.
//!
//! A query's `Limit` applies BEFORE its filter expression, so one page of a
//! filtered query can come back empty while a later page holds the match.
//! These helpers follow `LastEvaluatedKey` until a match or the end.

use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use std::future::Future;

pub type Item = HashMap<String, AttributeValue>;

/// Walk pages from `fetch` (given the previous page's LastEvaluatedKey,
/// None for the first page) and return the first item `pick` accepts.
/// Stops at the first match or when a page has no LastEvaluatedKey.
pub async fn first_match_paged<T, E, F, Fut, P>(mut fetch: F, mut pick: P) -> Result<Option<T>, E>
where
    F: FnMut(Option<Item>) -> Fut,
    Fut: Future<Output = Result<(Vec<Item>, Option<Item>), E>>,
    P: FnMut(&Item) -> Option<T>,
{
    let mut start: Option<Item> = None;
    loop {
        let (items, next) = fetch(start.take()).await?;
        if let Some(found) = items.iter().find_map(&mut pick) {
            return Ok(Some(found));
        }
        match next {
            Some(key) if !key.is_empty() => start = Some(key),
            _ => return Ok(None),
        }
    }
}

/// Run `query` across every page and return the first item `pick` accepts.
pub async fn query_first_match<T, P>(
    query: QueryFluentBuilder,
    pick: P,
) -> Result<Option<T>, String>
where
    P: FnMut(&Item) -> Option<T>,
{
    first_match_paged(
        |start| {
            let q = query.clone().set_exclusive_start_key(start);
            async move {
                let out = q
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
                Ok((out.items.unwrap_or_default(), out.last_evaluated_key))
            }
        },
        pick,
    )
    .await
}

/// The `run_id` string attribute of an item, if present.
pub fn run_id_of(item: &Item) -> Option<String> {
    item.get("run_id").and_then(|v| v.as_s().ok()).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(run_id: &str) -> Item {
        HashMap::from([("run_id".to_string(), AttributeValue::S(run_id.into()))])
    }

    fn key(n: usize) -> Option<Item> {
        Some(HashMap::from([(
            "k".to_string(),
            AttributeValue::N(n.to_string()),
        )]))
    }

    /// Pages: [empty, empty, [match]] — the match sits past two filtered-out
    /// pages, which the old single-page query never reached.
    #[tokio::test]
    async fn follows_last_evaluated_key_past_empty_pages() {
        let pages = [
            (vec![], key(1)),
            (vec![], key(2)),
            (vec![item("run-3")], None),
        ];
        let mut calls = Vec::new();
        let found: Result<Option<String>, String> = first_match_paged(
            |start| {
                let i = start
                    .as_ref()
                    .and_then(|k| k.get("k"))
                    .and_then(|v| v.as_n().ok())
                    .map(|n| n.parse::<usize>().unwrap())
                    .unwrap_or(0);
                calls.push(i);
                let page = pages[i].clone();
                async move { Ok(page) }
            },
            run_id_of,
        )
        .await;
        assert_eq!(found.unwrap().as_deref(), Some("run-3"));
        assert_eq!(calls, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn stops_at_the_end_without_a_match() {
        let mut calls = 0;
        let found: Result<Option<String>, String> = first_match_paged(
            |_| {
                calls += 1;
                async { Ok((vec![HashMap::new()], None)) }
            },
            run_id_of,
        )
        .await;
        assert_eq!(found.unwrap(), None);
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn stops_at_first_match_without_reading_more_pages() {
        let mut calls = 0;
        let found: Result<Option<String>, String> = first_match_paged(
            |_| {
                calls += 1;
                async { Ok((vec![item("a"), item("b")], key(9))) }
            },
            run_id_of,
        )
        .await;
        assert_eq!(found.unwrap().as_deref(), Some("a"));
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn propagates_errors() {
        let found: Result<Option<String>, String> =
            first_match_paged(|_| async { Err("boom".to_string()) }, run_id_of).await;
        assert_eq!(found.unwrap_err(), "boom");
    }
}
