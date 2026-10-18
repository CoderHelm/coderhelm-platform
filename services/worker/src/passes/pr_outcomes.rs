//! Fills in what happened to merged PRs that were recorded before the merge
//! webhook stored it (merge time, who merged, when it opened, people's
//! commits). Analytics place merges by `merged_at`, so without this every
//! older merge is missing from the per-day chart. Runs on the review-reminder
//! tick, a few runs at a time, and stops touching a run once it is filled or
//! GitHub can't say.
//!
//! Second sweep: runs that ended (completed / cancelled / retried) with a PR
//! but no `pr_outcome` — the merge/close webhook missed them, so merge rate
//! counted them as neither. GitHub is asked once per day per run until the PR
//! is merged or closed.

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
    reconcile_finished(state, &mut clients).await?;
    Ok(())
}

/// Run statuses that end a run while its PR may still merge or close later.
const FINISHED_STATUSES: [&str; 3] = ["completed", "cancelled", "retried"];
/// An open PR is asked about again after this long.
const RECHECK_OPEN_AFTER_HOURS: i64 = 24;

/// The run fields to record for a PR outcome; None while the PR is open.
fn outcome_fields(o: &Outcome) -> Option<Vec<(&'static str, &'static str, String)>> {
    match o {
        Outcome::Merged {
            merged_at,
            opened_at,
            merged_by,
        } => Some(vec![
            ("pr_outcome", ":o", "merged".to_string()),
            ("merged_at", ":ma", merged_at.clone()),
            ("pr_opened_at", ":op", opened_at.clone()),
            ("merged_by", ":mb", merged_by.clone()),
        ]),
        Outcome::Closed {
            closed_at,
            opened_at,
        } => Some(vec![
            ("pr_outcome", ":o", "closed".to_string()),
            ("pr_closed_at", ":c", closed_at.clone()),
            ("pr_opened_at", ":op", opened_at.clone()),
        ]),
        Outcome::Open => None,
    }
}

/// Fill `pr_outcome` on finished runs the webhook never reached.
async fn reconcile_finished(
    state: &WorkerState,
    clients: &mut HashMap<u64, GitHubClient>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now();
    let cutoff = (now - chrono::Duration::hours(RECHECK_OPEN_AFTER_HOURS)).to_rfc3339();
    let pending = find_runs(
        state,
        "#s IN (:s0, :s1, :s2) AND attribute_exists(pr_url) AND attribute_not_exists(pr_outcome) \
         AND attribute_not_exists(pr_backfill) \
         AND (attribute_not_exists(pr_outcome_checked_at) OR pr_outcome_checked_at < :cut)",
        vec![
            (":s0", attr_s(FINISHED_STATUSES[0])),
            (":s1", attr_s(FINISHED_STATUSES[1])),
            (":s2", attr_s(FINISHED_STATUSES[2])),
            (":cut", attr_s(&cutoff)),
        ],
    )
    .await?;
    if pending.is_empty() {
        return Ok(());
    }
    let (mut recorded, mut still_open, mut unknown) = (0, 0, 0);
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
                warn!(run_id = %p.run_id, error = %e, "Reconcile: GitHub didn't return the PR");
                mark_unknown(state, &p, "GitHub didn't return the PR").await;
                unknown += 1;
                continue;
            }
        };
        let Some(fields) = outcome_fields(&outcome(&pr)) else {
            // Still open: look again tomorrow.
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(&p.team_id))
                .key("run_id", attr_s(&p.run_id))
                .update_expression("SET pr_outcome_checked_at = :t")
                .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
                .send()
                .await;
            still_open += 1;
            continue;
        };
        let expr = format!(
            "SET {}",
            fields
                .iter()
                .map(|(k, v, _)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut req = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(&p.team_id))
            .key("run_id", attr_s(&p.run_id))
            // Never overwrite what the webhook recorded meanwhile.
            .condition_expression("attribute_not_exists(pr_outcome)")
            .update_expression(expr);
        for (_, placeholder, value) in &fields {
            req = req.expression_attribute_values(*placeholder, attr_s(value));
        }
        match req.send().await {
            Ok(_) => recorded += 1,
            Err(e) => {
                warn!(run_id = %p.run_id, error = %e, "Reconcile: could not save the PR outcome")
            }
        }
    }
    info!(
        recorded,
        still_open, unknown, "Reconciled PR outcomes on finished runs"
    );
    Ok(())
}

async fn find_pending(
    state: &WorkerState,
) -> Result<Vec<Pending>, Box<dyn std::error::Error + Send + Sync>> {
    find_runs(
        state,
        "#s = :merged AND attribute_not_exists(merged_at) AND attribute_not_exists(pr_backfill) AND attribute_exists(pr_url)",
        vec![(":merged", attr_s("merged"))],
    )
    .await
}

/// Scan for runs matching `filter` (`#s` = status), up to PER_TICK of them.
async fn find_runs(
    state: &WorkerState,
    filter: &str,
    values: Vec<(&str, AttributeValue)>,
) -> Result<Vec<Pending>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    for _ in 0..MAX_PAGES {
        let mut scan = state
            .dynamo
            .scan()
            .table_name(&state.config.runs_table_name)
            .filter_expression(filter)
            .expression_attribute_names("#s", "status");
        for (k, v) in &values {
            scan = scan.expression_attribute_values(*k, v.clone());
        }
        let resp = scan
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_and_closed_prs_map_to_outcome_fields() {
        let merged = Outcome::Merged {
            merged_at: "2026-10-01T00:00:00Z".into(),
            opened_at: "2026-09-30T00:00:00Z".into(),
            merged_by: "pat".into(),
        };
        let f = outcome_fields(&merged).unwrap();
        assert!(f.contains(&("pr_outcome", ":o", "merged".to_string())));
        assert!(f.iter().any(|(k, _, v)| *k == "merged_by" && v == "pat"));

        let closed = Outcome::Closed {
            closed_at: "2026-10-02T00:00:00Z".into(),
            opened_at: "2026-09-30T00:00:00Z".into(),
        };
        let f = outcome_fields(&closed).unwrap();
        assert!(f.contains(&("pr_outcome", ":o", "closed".to_string())));
        assert!(f.iter().any(|(k, _, _)| *k == "pr_closed_at"));
    }

    #[test]
    fn open_prs_record_nothing_yet() {
        assert!(outcome_fields(&Outcome::Open).is_none());
    }

    #[test]
    fn placeholders_are_unique_per_update() {
        for o in [
            Outcome::Merged {
                merged_at: "a".into(),
                opened_at: "b".into(),
                merged_by: "c".into(),
            },
            Outcome::Closed {
                closed_at: "a".into(),
                opened_at: "b".into(),
            },
        ] {
            let f = outcome_fields(&o).unwrap();
            let mut p: Vec<&str> = f.iter().map(|(_, p, _)| *p).collect();
            p.sort();
            p.dedup();
            assert_eq!(p.len(), f.len());
        }
    }

    #[test]
    fn finished_statuses_cover_completed_cancelled_retried() {
        assert_eq!(FINISHED_STATUSES, ["completed", "cancelled", "retried"]);
    }
}
