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

/// Every item from pages of `fetch` (given the previous page's
/// LastEvaluatedKey, None for the first page), following LastEvaluatedKey
/// until a page has none or `max_pages` pages were read. Returns the items
/// and whether more pages were left unread.
pub async fn all_pages<E, F, Fut>(mut fetch: F, max_pages: usize) -> Result<(Vec<Item>, bool), E>
where
    F: FnMut(Option<Item>) -> Fut,
    Fut: Future<Output = Result<(Vec<Item>, Option<Item>), E>>,
{
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..max_pages.max(1) {
        let (items, next) = fetch(start.take()).await?;
        out.extend(items);
        match next {
            Some(key) if !key.is_empty() => start = Some(key),
            _ => return Ok((out, false)),
        }
    }
    Ok((out, true))
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

    /// Pages: [a], [b, c], [d]: every item, in order, across three pages.
    #[tokio::test]
    async fn all_pages_reads_every_page_in_order() {
        let pages = [
            (vec![item("a")], key(1)),
            (vec![item("b"), item("c")], key(2)),
            (vec![item("d")], None),
        ];
        let (items, more): (Vec<Item>, bool) = all_pages(
            |start| {
                let i = start
                    .as_ref()
                    .and_then(|k| k.get("k"))
                    .and_then(|v| v.as_n().ok())
                    .map(|n| n.parse::<usize>().unwrap())
                    .unwrap_or(0);
                let page = pages[i].clone();
                async move { Ok::<_, String>(page) }
            },
            10,
        )
        .await
        .unwrap();
        let ids: Vec<String> = items.iter().filter_map(run_id_of).collect();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);
        assert!(!more);
    }

    #[tokio::test]
    async fn all_pages_stops_at_its_page_cap() {
        let mut calls = 0;
        let (items, more) = all_pages(
            |_| {
                calls += 1;
                async { Ok::<_, String>((vec![item("x")], key(1))) }
            },
            3,
        )
        .await
        .unwrap();
        assert_eq!(calls, 3);
        assert_eq!(items.len(), 3);
        assert!(more, "pages were left");
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
