//! Deploy stats: one record per deploy CoderHelm starts by adding a label
//! (`common::deploy_stats`), and the one-time copy of earlier deploys from
//! GitHub's label history.

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::deploy_stats::{self as ds, BACKFILL_DAYS, BACKFILL_MARKER_SK, PREFIX};
use tracing::{info, warn};

use crate::WorkerState;

/// Deploys recorded live on one PR: (label, when).
type LiveDeploys = Vec<(String, chrono::DateTime<Utc>)>;

fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// Records each deploy label in `labels` CoderHelm just added to a PR.
/// Best effort: a failed write is logged, never fails the caller.
#[allow(clippy::too_many_arguments)]
pub async fn record(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    labels: &[String],
    head: &str,
    configured: &[String],
    source: &str,
) {
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let full = format!("{owner}/{repo}");
    for label in labels.iter().filter(|l| ds::is_deploy_label(l, configured)) {
        let item = ds::stat_item(team_id, &now, &full, pr, label, head, source);
        if let Err(e) = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .set_item(Some(item))
            .send()
            .await
        {
            warn!(pr, label = %label, error = %e, "Could not record a deploy");
        }
    }
}

/// The PRs a team touched in the backfill window: reviewed (review stats)
/// or opened by a run.
async fn recent_prs(
    state: &WorkerState,
    team_id: &str,
    since: chrono::DateTime<Utc>,
) -> Result<Vec<(String, u64)>, String> {
    let mut prs: HashSet<(String, u64)> = HashSet::new();
    let lo = format!(
        "{}{}",
        common::review_stats::PREFIX,
        since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    );
    let hi = format!("{}~", common::review_stats::PREFIX);
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av(team_id))
            .expression_attribute_values(":lo", av(&lo))
            .expression_attribute_values(":hi", av(&hi))
            .projection_expression("repo, pr_number")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("read review stats: {e}"))?;
        prs.extend(r.items().iter().filter_map(repo_pr));
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let lo = ulid::Ulid::from_parts(since.timestamp_millis().max(0) as u64, 0).to_string();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :t AND run_id >= :lo")
            .expression_attribute_values(":t", av(team_id))
            .expression_attribute_values(":lo", av(&lo))
            .projection_expression("repo, pr_number")
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("read runs: {e}"))?;
        prs.extend(r.items().iter().filter_map(repo_pr));
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let mut out: Vec<(String, u64)> = prs.into_iter().collect();
    out.sort();
    Ok(out)
}

fn repo_pr(i: &HashMap<String, AttributeValue>) -> Option<(String, u64)> {
    let repo = i.get("repo")?.as_s().ok()?.to_string();
    let pr: u64 = i.get("pr_number")?.as_n().ok()?.parse().ok()?;
    (repo.contains('/') && pr > 0).then_some((repo, pr))
}

/// Which copied events are new: in the window, a deploy label, and not
/// already recorded live (same PR and label within five minutes).
pub fn new_deploys(
    added: &[(String, String)],
    since: chrono::DateTime<Utc>,
    configured: &[String],
    live: &[(String, chrono::DateTime<Utc>)],
) -> Vec<(String, String)> {
    added
        .iter()
        .filter(|(label, _)| ds::is_deploy_label(label, configured))
        .filter_map(|(label, at)| {
            let t = chrono::DateTime::parse_from_rfc3339(at)
                .ok()?
                .with_timezone(&Utc);
            (t >= since).then_some((label, at, t))
        })
        .filter(|(label, _, t)| {
            !live.iter().any(|(l, lt)| {
                l.eq_ignore_ascii_case(label) && (*lt - *t).num_seconds().abs() <= 300
            })
        })
        .map(|(l, at, _)| (l.clone(), at.clone()))
        .collect()
}

/// Copies a team's deploys of the last `BACKFILL_DAYS` from GitHub, once.
/// Re-running is safe: each copy is written only when missing, and the
/// marker is written only when every PR was read.
pub async fn backfill(state: &WorkerState, team_id: &str) -> Result<(), String> {
    let table = &state.config.settings_table_name;
    let done = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", av(team_id))
        .key("sk", av(BACKFILL_MARKER_SK))
        .send()
        .await
        .map_err(|e| format!("read backfill marker: {e}"))?;
    if done.item().is_some() {
        return Ok(());
    }
    let Some(github) = super::plan_chat::github_for(state, team_id).await else {
        return Err("no GitHub installation for the team".into());
    };
    let since = Utc::now() - Duration::days(BACKFILL_DAYS);
    let prs = recent_prs(state, team_id, since).await?;
    // Deploys already recorded live, by PR, to skip their copies.
    let mut live: HashMap<(String, u64), LiveDeploys> = HashMap::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", av(team_id))
            .expression_attribute_values(":p", av(PREFIX))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("read deploy stats: {e}"))?;
        for i in r.items() {
            let g = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            if g("source") == "backfill" {
                continue;
            }
            if let (Some(k), Ok(t)) = (
                repo_pr(i),
                chrono::DateTime::parse_from_rfc3339(&g("created_at")),
            ) {
                live.entry(k)
                    .or_default()
                    .push((g("label"), t.with_timezone(&Utc)));
            }
        }
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let mut configured: HashMap<String, Vec<String>> = HashMap::new();
    let (mut copied, mut failed) = (0usize, 0usize);
    for (repo, pr) in &prs {
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        if !configured.contains_key(repo) {
            let cfg =
                super::review_actions::OnApproveConfig::load(state, team_id, owner, name).await;
            configured.insert(repo.clone(), ds::configured(&cfg.deploy_label));
        }
        let added = match github.labels_added_by_app(owner, name, *pr).await {
            Ok(a) => a,
            Err(e) => {
                warn!(repo = %repo, pr, error = %e, "Deploy backfill: could not read label history");
                failed += 1;
                continue;
            }
        };
        let empty = Vec::new();
        for (label, at) in new_deploys(
            &added,
            since,
            &configured[repo],
            live.get(&(repo.clone(), *pr)).unwrap_or(&empty),
        ) {
            let item = ds::stat_item(team_id, &at, repo, *pr, &label, "", "backfill");
            match state
                .dynamo
                .put_item()
                .table_name(table)
                .set_item(Some(item))
                .condition_expression("attribute_not_exists(sk)")
                .send()
                .await
            {
                Ok(_) => copied += 1,
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|e| e.is_conditional_check_failed_exception()) => {}
                Err(e) => {
                    warn!(error = %e, "Deploy backfill: write failed");
                    failed += 1;
                }
            }
        }
    }
    info!(
        team_id,
        prs = prs.len(),
        copied,
        failed,
        "Deploy stats backfill pass"
    );
    if failed > 0 {
        return Err(format!(
            "{failed} PRs or writes failed; the next request continues"
        ));
    }
    state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", av(team_id))
        .item("sk", av(BACKFILL_MARKER_SK))
        .item(
            "since",
            av(&since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        )
        .item("prs", AttributeValue::N(prs.len().to_string()))
        .item("copied", AttributeValue::N(copied.to_string()))
        .item("at", av(&Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| format!("write backfill marker: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copies_only_deploy_labels_in_the_window_not_recorded_live() {
        let since = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let added = vec![
            (
                "CI:DEPLOY_STAGING".to_string(),
                "2026-08-01T00:00:00Z".to_string(),
            ),
            (
                "CI:DEPLOY_STAGING".to_string(),
                "2026-09-10T10:00:00Z".to_string(),
            ),
            (
                "coderhelm-review".to_string(),
                "2026-09-10T10:00:00Z".to_string(),
            ),
            ("ship-it".to_string(), "2026-09-11T00:00:00Z".to_string()),
            (
                "CI:DEPLOY_STAGING".to_string(),
                "2026-10-01T12:00:00Z".to_string(),
            ),
        ];
        let live = vec![(
            "ci:deploy_staging".to_string(),
            chrono::DateTime::parse_from_rfc3339("2026-10-01T12:02:00Z")
                .unwrap()
                .with_timezone(&Utc),
        )];
        let got = new_deploys(&added, since, &["ship-it".to_string()], &live);
        assert_eq!(
            got,
            vec![
                (
                    "CI:DEPLOY_STAGING".to_string(),
                    "2026-09-10T10:00:00Z".to_string()
                ),
                ("ship-it".to_string(), "2026-09-11T00:00:00Z".to_string()),
            ]
        );
    }
}
