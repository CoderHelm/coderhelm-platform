//! Every repository's reviewer config, read by key.
//!
//! The configs live in the shared settings table under each team's partition
//! (`pk = <team>`, `sk = REVIEW_CONFIG#REPO#<owner/repo>`). That table also
//! holds analytics, rollups and markers, so a filtered scan of it walks every
//! item to find a handful of configs, and a page cap made the sweeps stop
//! before reaching them. Instead, the teams that have configs are kept in an
//! index partition (`INDEX#REVIEW_CONFIG`, see `common::review_config_index`)
//! and each team's configs are queried by key. Every page is read.

use crate::passes::attr_s;
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::review_config_index::{self as index, CONFIG_PREFIX};
use common::settings_index;
use std::collections::HashMap;
use std::future::Future;
use tracing::{info, warn};

pub type Item = HashMap<String, AttributeValue>;

/// One reviewer config: the team, the repository and the raw record.
pub struct RepoConfig {
    pub team_id: String,
    pub repo: String,
    pub item: Item,
}

/// Reads every page `fetch` returns, until it says there are no more.
/// `limit` only guards against a read that never ends; reaching it is logged.
pub async fn drain_pages<T, F, Fut>(what: &str, limit: usize, mut fetch: F) -> Option<Vec<T>>
where
    F: FnMut(Option<Item>) -> Fut,
    Fut: Future<Output = Result<(Vec<T>, Option<Item>), String>>,
{
    let mut out = vec![];
    let mut next: Option<Item> = None;
    for _ in 0..limit {
        match fetch(next.take()).await {
            Ok((items, more)) => {
                out.extend(items);
                match more {
                    Some(k) if !k.is_empty() => next = Some(k),
                    _ => return Some(out),
                }
            }
            Err(e) => {
                warn!(what, error = %e, "Reading reviewer configs failed");
                return None;
            }
        }
    }
    warn!(
        what,
        pages = limit,
        read = out.len(),
        "Reading reviewer configs stopped at its page guard"
    );
    Some(out)
}

/// Every config of the indexed teams. A team whose read fails is skipped
/// for this tick and counted; a team with no configs left is returned in
/// `empty` so its stale index entry can be dropped.
pub struct Collected {
    pub configs: Vec<RepoConfig>,
    pub failed: usize,
    pub empty: Vec<String>,
}

/// Reads each team's configs with `team_configs`. Which teams to read comes
/// only from the index, never from how teams are recorded elsewhere.
pub async fn collect<F, Fut>(teams: &[String], mut team_configs: F) -> Collected
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Option<Vec<RepoConfig>>>,
{
    let mut out = Collected {
        configs: vec![],
        failed: 0,
        empty: vec![],
    };
    for team in teams {
        match team_configs(team.clone()).await {
            Some(c) if c.is_empty() => out.empty.push(team.clone()),
            Some(c) => out.configs.extend(c),
            None => out.failed += 1,
        }
    }
    out
}

async fn team_configs(state: &WorkerState, team_id: String) -> Option<Vec<RepoConfig>> {
    let team = team_id.as_str();
    drain_pages("team configs", 100, |start| async move {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team))
            .expression_attribute_values(":pfx", attr_s(CONFIG_PREFIX))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())?;
        let configs = resp
            .items()
            .iter()
            .filter_map(|item| {
                let sk = item.get("sk").and_then(|v| v.as_s().ok())?;
                let repo = index::repo_of(sk)?.to_string();
                Some(RepoConfig {
                    team_id: team.to_string(),
                    repo,
                    item: item.clone(),
                })
            })
            .collect::<Vec<_>>();
        Ok((configs, resp.last_evaluated_key().cloned()))
    })
    .await
}

/// Every reviewer config of every team that has one.
pub async fn all(state: &WorkerState, sweep: &str) -> Vec<RepoConfig> {
    let table = &state.config.settings_table_name;
    let teams = match settings_index::members(&state.dynamo, table, &index::spec()).await {
        Ok(t) => t,
        Err(e) => {
            warn!(sweep, error = %e, "Could not read the reviewer config index");
            return vec![];
        }
    };
    let got = collect(&teams, |t| team_configs(state, t)).await;
    for gone in &got.empty {
        settings_index::remove(&state.dynamo, table, index::NAME, gone).await;
    }
    info!(
        sweep,
        teams = teams.len(),
        teams_failed = got.failed,
        teams_dropped = got.empty.len(),
        configs = got.configs.len(),
        "Reviewer configs read"
    );
    got.configs
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn key(n: usize) -> Item {
        HashMap::from([("pk".to_string(), AttributeValue::N(n.to_string()))])
    }
    fn page_of(k: &Option<Item>) -> usize {
        k.as_ref()
            .and_then(|m| m.get("pk"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
    }
    fn cfg(team: &str, repo: &str) -> RepoConfig {
        RepoConfig {
            team_id: team.to_string(),
            repo: repo.to_string(),
            item: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn every_page_is_read_however_many_there_are() {
        // 120 pages: more than the 50 the old scans stopped at.
        let pages = 120;
        let calls = RefCell::new(0);
        let got = drain_pages("t", 1000, |start| {
            *calls.borrow_mut() += 1;
            let p = page_of(&start);
            async move {
                let more = (p + 1 < pages).then(|| key(p + 1));
                Ok::<_, String>((vec![format!("repo-{p}")], more))
            }
        })
        .await
        .unwrap();
        assert_eq!(got.len(), pages);
        assert_eq!(got.last().unwrap(), "repo-119");
        assert_eq!(*calls.borrow(), pages);
    }

    #[tokio::test]
    async fn a_failed_read_returns_nothing_rather_than_a_partial_list() {
        let got = drain_pages("t", 10, |start| {
            let p = page_of(&start);
            async move {
                if p == 2 {
                    Err("throttled".to_string())
                } else {
                    Ok((vec![p], Some(key(p + 1))))
                }
            }
        })
        .await;
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn the_guard_only_stops_a_read_that_never_ends() {
        let got = drain_pages("t", 5, |start| {
            let p = page_of(&start);
            async move { Ok::<_, String>((vec![p], Some(key(p + 1)))) }
        })
        .await
        .unwrap();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn a_team_with_no_teams_table_record_is_still_swept() {
        // The index names the team from its config's own partition key, so a
        // team that exists only as members (no META record) is read like any
        // other.
        let teams = vec![
            "TEAM#with-meta".to_string(),
            "TEAM#members-only".to_string(),
        ];
        let got = collect(&teams, |t| async move {
            Some(vec![cfg(
                &t,
                &format!("{}/web", t.trim_start_matches("TEAM#")),
            )])
        })
        .await;
        let swept: Vec<_> = got.configs.iter().map(|c| c.team_id.as_str()).collect();
        assert_eq!(swept, vec!["TEAM#with-meta", "TEAM#members-only"]);
        assert_eq!(got.failed, 0);
    }

    #[tokio::test]
    async fn one_failed_team_never_hides_the_others_and_empty_teams_are_dropped() {
        let teams = vec![
            "TEAM#a".to_string(),
            "TEAM#b".to_string(),
            "TEAM#c".to_string(),
        ];
        let got = collect(&teams, |t| async move {
            match t.as_str() {
                "TEAM#a" => None,
                "TEAM#b" => Some(vec![]),
                _ => Some(vec![cfg(&t, "acme/web")]),
            }
        })
        .await;
        assert_eq!(got.failed, 1);
        assert_eq!(got.empty, vec!["TEAM#b".to_string()]);
        assert_eq!(got.configs.len(), 1);
        assert_eq!(got.configs[0].team_id, "TEAM#c");
    }
}
