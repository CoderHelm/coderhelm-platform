//! Fills in what happened to merged PRs that were recorded before the merge
//! webhook stored it (merge time, who merged, when it opened, people's
//! commits). Analytics place merges by `merged_at`, so without this every
//! older merge is missing from the per-day chart. Runs on the review-reminder
//! tick, a few runs at a time, and stops touching a run once it is filled or
//! GitHub can't say.

use crate::clients::github::GitHubClient;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::pr_outcome::{human_commit_count, outcome, parse_pr_url, Outcome};
use std::collections::HashMap;
use tracing::{info, warn};

/// Runs filled per tick: keeps GitHub calls and the tick's time small.
const PER_TICK: usize = 25;
/// Scan pages per tick, so a large table can't run the tick long.
const MAX_PAGES: usize = 20;

struct Pending {
    team_id: String,
    run_id: String,
    pr_url: String,
    installation_id: u64,
}

pub async fn backfill(state: &WorkerState) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pending = find_pending(state).await?;
    if pending.is_empty() {
        return Ok(());
    }
    let mut clients: HashMap<u64, GitHubClient> = HashMap::new();
    let (mut filled, mut unknown) = (0, 0);
    for p in pending {
        let Some((owner, repo, number)) = parse_pr_url(&p.pr_url) else {
            mark_unknown(state, &p, "no PR link").await;
            unknown += 1;
            continue;
        };
        let gh = match clients.entry(p.installation_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                p.installation_id,
                &state.http,
            )?),
        };
        let pr = match gh.get_pull_request(&owner, &repo, number).await {
            Ok(v) => v,
            Err(e) => {
                warn!(run_id = %p.run_id, error = %e, "Backfill: GitHub didn't return the PR");
                mark_unknown(state, &p, "GitHub didn't return the PR").await;
                unknown += 1;
                continue;
            }
        };
        let Outcome::Merged {
            merged_at,
            opened_at,
            merged_by,
        } = outcome(&pr)
        else {
            mark_unknown(state, &p, "GitHub doesn't show it merged").await;
            unknown += 1;
            continue;
        };
        let human = gh
            .list_pr_commits(&owner, &repo, number)
            .await
            .ok()
            .map(|c| human_commit_count(&c));
        let mut expr =
            "SET merged_at = :ma, pr_opened_at = :op, merged_by = :mb, pr_outcome = :o".to_string();
        let mut req = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&p.team_id))
            .key("run_id", attr_s(&p.run_id))
            // Never overwrite what the webhook recorded.
            .condition_expression("attribute_not_exists(merged_at)")
            .expression_attribute_values(":ma", attr_s(&merged_at))
            .expression_attribute_values(":op", attr_s(&opened_at))
            .expression_attribute_values(":mb", attr_s(&merged_by))
            .expression_attribute_values(":o", attr_s("merged"));
        if let Some(n) = human {
            expr.push_str(", human_commits = :hc");
            req = req.expression_attribute_values(":hc", attr_n(n));
        }
        match req.update_expression(expr).send().await {
            Ok(_) => filled += 1,
            Err(e) => warn!(run_id = %p.run_id, error = %e, "Backfill: could not save the merge"),
        }
    }
    info!(filled, unknown, "Backfilled merged PR outcomes");
    Ok(())
}

async fn find_pending(
    state: &WorkerState,
) -> Result<Vec<Pending>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    for _ in 0..MAX_PAGES {
        let resp = state
            .dynamo
            .scan()
            .table_name(&state.config.runs_table_name)
            .filter_expression(
                "#s = :merged AND attribute_not_exists(merged_at) AND attribute_not_exists(pr_backfill) AND attribute_exists(pr_url)",
            )
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":merged", attr_s("merged"))
            .projection_expression("team_id, run_id, pr_url, installation_id")
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        for i in resp.items() {
            let s = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            let installation_id = i
                .get("installation_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if installation_id == 0 {
                continue;
            }
            out.push(Pending {
                team_id: s("team_id"),
                run_id: s("run_id"),
                pr_url: s("pr_url"),
                installation_id,
            });
            if out.len() >= PER_TICK {
                return Ok(out);
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(out)
}

/// Stops retrying a run GitHub can't answer for.
async fn mark_unknown(state: &WorkerState, p: &Pending, why: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&p.team_id))
        .key("run_id", attr_s(&p.run_id))
        .update_expression("SET pr_backfill = :w")
        .expression_attribute_values(":w", attr_s(why))
        .send()
        .await
        .map_err(|e| warn!(run_id = %p.run_id, error = %e, "Backfill: could not mark the run"));
}
