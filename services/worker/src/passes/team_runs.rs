//! Every team's runs that pass a filter, read team by team through the runs
//! table's key (`team_id`), every page to the end.
//!
//! The sweeps that look for runs in some state (finished with a pull
//! request, failed, a draft to settle, a description to rebuild) found them
//! with a filtered scan of the whole runs table capped at a number of pages.
//! On a large table the cap stopped before the later runs, and those were
//! never looked at. A Query per team has no cap: each team's partition is
//! read to its last page, so every matching run is found however large the
//! table grows.

use std::future::Future;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::warn;

use crate::WorkerState;

pub type Item = std::collections::HashMap<String, AttributeValue>;

/// The key placeholder; filters must not use it.
const TEAM: &str = ":qteam";

/// What the runs must pass, as the scan said it: its filter, the names and
/// values the filter uses, and the attributes read (all when `None`).
#[derive(Debug, Clone, Default)]
pub struct Filter<'a> {
    pub expr: &'a str,
    pub names: Vec<(&'a str, &'a str)>,
    pub values: Vec<(&'a str, AttributeValue)>,
    pub projection: Option<&'a str>,
}

/// Every item of a paged read: `page(start)` gives one page and where the
/// next starts (`None` at the end). Pages are read until the last one.
pub async fn read_all<F, Fut>(mut page: F) -> Result<Vec<Item>, String>
where
    F: FnMut(Option<Item>) -> Fut,
    Fut: Future<Output = Result<(Vec<Item>, Option<Item>), String>>,
{
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let (items, next) = page(start.take()).await?;
        out.extend(items);
        match next.filter(|k| !k.is_empty()) {
            Some(k) => start = Some(k),
            None => break,
        }
    }
    Ok(out)
}

/// One team's runs that pass `f`, every page.
pub async fn of_team(state: &WorkerState, team: &str, f: &Filter<'_>) -> Result<Vec<Item>, String> {
    debug_assert!(!f.expr.contains(TEAM));
    read_all(|start| async move {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression(format!("team_id = {TEAM}"))
            .expression_attribute_values(TEAM, AttributeValue::S(team.to_string()))
            .filter_expression(f.expr)
            .set_exclusive_start_key(start);
        for (k, v) in &f.names {
            q = q.expression_attribute_names(*k, *v);
        }
        for (k, v) in &f.values {
            q = q.expression_attribute_values(*k, v.clone());
        }
        if let Some(p) = f.projection {
            q = q.projection_expression(p);
        }
        let page = q
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
        Ok((page.items().to_vec(), page.last_evaluated_key().cloned()))
    })
    .await
}

/// Every team's runs that pass `f`. A team whose read fails is left for the
/// next sweep (`what` names the sweep in the warning); the others are read.
pub async fn matching(state: &WorkerState, what: &str, f: &Filter<'_>) -> Vec<Item> {
    let mut out = Vec::new();
    for team in super::learning_wait::teams(state).await {
        match of_team(state, &team, f).await {
            Ok(items) => out.extend(items),
            Err(e) => {
                warn!(team_id = %team, sweep = what, error = %e, "Could not read the team's runs")
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(n: usize) -> Item {
        Item::from([("run_id".to_string(), AttributeValue::S(format!("{n:05}")))])
    }

    /// A partition of `total` items, `size` a page.
    fn pages(total: usize, size: usize, start: Option<Item>) -> (Vec<Item>, Option<Item>) {
        let from = start
            .and_then(|k| k.get("run_id").and_then(|v| v.as_s().ok()).cloned())
            .map(|s| s.parse::<usize>().unwrap() + 1)
            .unwrap_or(0);
        let to = (from + size).min(total);
        let page: Vec<Item> = (from..to).map(item).collect();
        let next = (to < total).then(|| item(to - 1));
        (page, next)
    }

    #[tokio::test]
    async fn every_page_is_read_however_many_there_are() {
        // Far more pages than any cap the scans had.
        let mut reads = 0;
        let got = read_all(|start| {
            reads += 1;
            async move { Ok(pages(2_345, 10, start)) }
        })
        .await
        .unwrap();
        assert_eq!(got.len(), 2_345);
        assert_eq!(reads, 235);
        assert_eq!(got.last(), Some(&item(2_344)));
    }

    #[tokio::test]
    async fn an_empty_last_key_ends_the_read_and_an_error_stops_it() {
        let got = read_all(|_| async { Ok((vec![item(1)], Some(Item::new()))) })
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        let mut n = 0;
        let err = read_all(|start| {
            n += 1;
            let fail = n == 3;
            async move {
                if fail {
                    Err("throttled".to_string())
                } else {
                    Ok(pages(100, 10, start))
                }
            }
        })
        .await;
        assert_eq!(err, Err("throttled".to_string()));
    }

    #[test]
    fn the_sweeps_read_runs_by_team_never_by_a_capped_scan() {
        for (name, src) in [
            ("pr_watch", include_str!("pr_watch.rs")),
            ("ready", include_str!("ready.rs")),
            ("recover", include_str!("recover.rs")),
            ("change_wait", include_str!("change_wait.rs")),
        ] {
            let code = src.split("#[cfg(test)]\nmod tests").next().unwrap();
            assert!(
                !code.contains(".scan()"),
                "{name} still scans the runs table"
            );
            assert!(
                !code.contains("for _ in 0..200"),
                "{name} still caps its pages"
            );
        }
    }
}
