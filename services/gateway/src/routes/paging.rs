//! Newest-first pagination for item families whose sort key isn't time-ordered
//! (reviews sort by repo/PR, releases by repo/tag).
//!
//! Reads only the keys and timestamps of the family (cheap, and complete — a
//! single Query stops at 1 MB), orders them by time, then fetches the full
//! items for one page. The cursor is the last item's `time|sk`, so pages stay
//! stable while new items arrive.

use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use axum::http::StatusCode;
use std::collections::HashMap;
use tracing::error;

use crate::AppState;

pub type Item = HashMap<String, AttributeValue>;

pub struct Page {
    pub items: Vec<Item>,
    pub next: Option<String>,
}

/// Pure: order `(time, sk)` keys newest first and pick the page after `cursor`.
/// Returns the page's sort keys and the cursor for the next page.
pub fn page_keys(
    mut keys: Vec<(String, String)>,
    cursor: Option<&str>,
    limit: usize,
) -> (Vec<String>, Option<String>) {
    keys.sort_by(|a, b| b.cmp(a));
    let start = match cursor.and_then(|c| c.split_once('|')) {
        Some((t, sk)) => {
            let at = (t.to_string(), sk.to_string());
            keys.iter().position(|k| *k < at).unwrap_or(keys.len())
        }
        None => 0,
    };
    let page: Vec<(String, String)> = keys.iter().skip(start).take(limit).cloned().collect();
    let next = (start + page.len() < keys.len())
        .then(|| page.last().map(|(t, sk)| format!("{t}|{sk}")))
        .flatten();
    (page.into_iter().map(|(_, sk)| sk).collect(), next)
}

/// One page of the `pk` items whose sort key starts with `prefix`, newest
/// first by the string attribute `time_attr`.
pub async fn newest_first(
    state: &AppState,
    table: &str,
    pk: &str,
    prefix: &str,
    time_attr: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Page, StatusCode> {
    let mut keys: Vec<(String, String)> = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(pk.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.to_string()))
            .projection_expression("sk, #t")
            .expression_attribute_names("#t", time_attr)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!(prefix, error = %e, "Could not list item keys");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for it in out.items() {
            let s = |k: &str| {
                it.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            keys.push((s(time_attr), s("sk")));
        }
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }

    let (sks, next) = page_keys(keys, cursor, limit);
    let mut by_sk: HashMap<String, Item> = HashMap::new();
    for chunk in sks.chunks(100) {
        let mut pending = Some(
            KeysAndAttributes::builder()
                .set_keys(Some(
                    chunk
                        .iter()
                        .map(|sk| {
                            HashMap::from([
                                ("pk".to_string(), AttributeValue::S(pk.to_string())),
                                ("sk".to_string(), AttributeValue::S(sk.clone())),
                            ])
                        })
                        .collect(),
                ))
                .build()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
        // BatchGetItem may return part of the request as unprocessed.
        for _ in 0..5 {
            let Some(req) = pending.take() else { break };
            let out = state
                .dynamo
                .batch_get_item()
                .request_items(table, req)
                .send()
                .await
                .map_err(|e| {
                    error!(prefix, error = %e, "Could not fetch page items");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            for it in out
                .responses()
                .and_then(|r| r.get(table))
                .into_iter()
                .flatten()
            {
                if let Some(sk) = it.get("sk").and_then(|v| v.as_s().ok()) {
                    by_sk.insert(sk.clone(), it.clone());
                }
            }
            pending = out
                .unprocessed_keys()
                .and_then(|u| u.get(table))
                .filter(|k| !k.keys().is_empty())
                .cloned();
        }
    }
    let items = sks.iter().filter_map(|sk| by_sk.remove(sk)).collect();
    Ok(Page { items, next })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(t: &str, sk: &str) -> (String, String) {
        (t.to_string(), sk.to_string())
    }

    #[test]
    fn pages_newest_first_and_resume_after_the_cursor() {
        let keys = vec![
            k("2026-01-01", "R#a#1"),
            k("2026-01-03", "R#b#1"),
            k("2026-01-02", "R#a#2"),
            k("2026-01-03", "R#a#3"),
            k("2026-01-04", "R#c#1"),
        ];
        let (p1, c1) = page_keys(keys.clone(), None, 2);
        assert_eq!(p1, vec!["R#c#1", "R#b#1"]);
        let (p2, c2) = page_keys(keys.clone(), c1.as_deref(), 2);
        assert_eq!(p2, vec!["R#a#3", "R#a#2"]);
        let (p3, c3) = page_keys(keys.clone(), c2.as_deref(), 2);
        assert_eq!(p3, vec!["R#a#1"]);
        assert_eq!(c3, None);
    }

    #[test]
    fn a_new_item_does_not_shift_later_pages() {
        let mut keys = vec![k("2026-01-02", "R#x"), k("2026-01-01", "R#y")];
        let (_, c1) = page_keys(keys.clone(), None, 1);
        keys.push(k("2026-01-05", "R#new"));
        let (p2, _) = page_keys(keys, c1.as_deref(), 1);
        assert_eq!(p2, vec!["R#y"]);
    }

    #[test]
    fn exact_last_page_has_no_cursor() {
        let keys = vec![k("2", "a"), k("1", "b")];
        assert_eq!(page_keys(keys, None, 2).1, None);
    }
}
