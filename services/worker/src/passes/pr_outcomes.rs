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
use common::rewards::RewardOutcomeMessage;
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
    let mut clients: HashMap<u64, GitHubClient> = HashMap::new();
    // The second sweep runs whether or not the first found anything: once
    // every older merge was filled, an early return here meant finished runs
    // never learned their PR closed or merged.
    if pending.is_empty() {
        return reconcile_finished(state, &mut clients).await;
    }
    let (mut filled, mut unknown) = (0, 0);
    let mut budget = crate::clients::github_budget::SweepGate::new(
        common::github_budget::caller::SWEEP_PR_OUTCOMES,
    );
    for p in pending {
        let Some((owner, repo, number)) = parse_pr_url(&p.pr_url) else {
            mark_unknown(state, &p, "no PR link").await;
            unknown += 1;
            continue;
        };
        if !budget.allows(state, p.installation_id, 2).await {
            continue;
        }
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
            // GitHub's limit (or the sweep's own budget) says nothing
            // about the pull request: it is read at a later sweep.
            Err(e) if crate::clients::github::outside_wait(&*e).is_some() => continue,
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
            Ok(_) => {
                filled += 1;
                score_missed(state, &p, &owner, &repo, number).await;
            }
            Err(e) => warn!(run_id = %p.run_id, error = %e, "Backfill: could not save the merge"),
        }
    }
    info!(filled, unknown, "Backfilled merged PR outcomes");
    reconcile_finished(state, &mut clients).await?;
    Ok(())
}

/// Run statuses that end a run while its PR may still merge or close later.
const FINISHED_STATUSES: [&str; 4] = ["completed", "cancelled", "closed", "retried"];
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
        "finished_outcomes",
        "#s IN (:s0, :s1, :s2, :s3) AND attribute_exists(pr_url) AND attribute_not_exists(pr_outcome) \
         AND attribute_not_exists(pr_backfill) \
         AND (attribute_not_exists(pr_outcome_checked_at) OR pr_outcome_checked_at < :cut)",
        vec![
            (":s0", attr_s(FINISHED_STATUSES[0])),
            (":s1", attr_s(FINISHED_STATUSES[1])),
            (":s2", attr_s(FINISHED_STATUSES[2])),
            (":s3", attr_s(FINISHED_STATUSES[3])),
            (":cut", attr_s(&cutoff)),
        ],
    )
    .await?;
    if pending.is_empty() {
        return Ok(());
    }
    let (mut recorded, mut still_open, mut unknown) = (0, 0, 0);
    let mut budget = crate::clients::github_budget::SweepGate::new(
        common::github_budget::caller::SWEEP_PR_OUTCOMES,
    );
    for p in pending {
        let Some((owner, repo, number)) = parse_pr_url(&p.pr_url) else {
            mark_unknown(state, &p, "no PR link").await;
            unknown += 1;
            continue;
        };
        if !budget.allows(state, p.installation_id, 2).await {
            continue;
        }
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
            Err(e) if crate::clients::github::outside_wait(&*e).is_some() => continue,
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
            Ok(_) => {
                recorded += 1;
                score_missed(state, &p, &owner, &repo, number).await;
            }
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
        "merged_backfill",
        "#s = :merged AND attribute_not_exists(merged_at) AND attribute_not_exists(pr_backfill) AND attribute_exists(pr_url)",
        vec![(":merged", attr_s("merged"))],
    )
    .await
}

type Key = HashMap<String, AttributeValue>;

/// The settings item holding where a sweep's scan stopped (`SYSTEM` /
/// `SCAN_CURSOR#<sweep>`).
fn cursor_sk(sweep: &str) -> String {
    format!("SCAN_CURSOR#{sweep}")
}

/// What to do with the saved cursor after a tick's scan.
#[derive(Debug, PartialEq)]
enum CursorStep {
    /// The tick filled up mid-scan: start from the same place next tick
    /// (what it found is settled meanwhile and drops out of the filter).
    Keep,
    /// The page cap ran out: carry on from here next tick.
    Save(Key),
    /// The scan reached the end of the table: start over next tick.
    Clear,
}

/// Pure: the cursor step, from how the scan ended.
fn cursor_step(filled: bool, pages_left: bool, last: Option<&Key>) -> CursorStep {
    match (filled, last.filter(|k| !k.is_empty())) {
        (true, _) => CursorStep::Keep,
        (false, Some(k)) if !pages_left => CursorStep::Save(k.clone()),
        _ => CursorStep::Clear,
    }
}

fn key_from(item: Option<&Key>) -> Option<Key> {
    let i = item?;
    let team = i.get("team_id")?.clone();
    let run = i.get("run_id")?.clone();
    Some(HashMap::from([
        ("team_id".to_string(), team),
        ("run_id".to_string(), run),
    ]))
}

async fn load_cursor(state: &WorkerState, sweep: &str) -> Option<Key> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s("SYSTEM"))
        .key("sk", attr_s(&cursor_sk(sweep)))
        .send()
        .await
        .ok()?;
    key_from(r.item())
}

async fn store_cursor(state: &WorkerState, sweep: &str, step: CursorStep) {
    let table = &state.config.settings_table_name;
    let r = match step {
        CursorStep::Keep => return,
        CursorStep::Clear => state
            .dynamo
            .delete_item()
            .table_name(table)
            .key("pk", attr_s("SYSTEM"))
            .key("sk", attr_s(&cursor_sk(sweep)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        CursorStep::Save(k) => {
            let mut req = state
                .dynamo
                .put_item()
                .table_name(table)
                .item("pk", attr_s("SYSTEM"))
                .item("sk", attr_s(&cursor_sk(sweep)))
                .item("at", attr_s(&chrono::Utc::now().to_rfc3339()));
            for (name, v) in k {
                req = req.item(name, v);
            }
            req.send().await.map(|_| ()).map_err(|e| e.to_string())
        }
    };
    if let Err(e) = r {
        warn!(sweep, error = %e, "Could not save where the runs scan stopped");
    }
}

/// Scan for runs matching `filter` (`#s` = status), up to PER_TICK of them.
/// Each tick reads at most MAX_PAGES pages, starting where the last tick's
/// page cap stopped, so every run is reached however large the table grows
/// (a cap that restarted from the first page never reached later runs).
async fn find_runs(
    state: &WorkerState,
    sweep: &str,
    filter: &str,
    values: Vec<(&str, AttributeValue)>,
) -> Result<Vec<Pending>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    let resumed = load_cursor(state, sweep).await;
    let from_middle = resumed.is_some();
    let mut start: Option<Key> = resumed;
    let mut pages = 0usize;
    let mut last: Option<Key> = None;
    let mut filled = false;
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
        pages += 1;
        last = resp.last_evaluated_key().cloned();
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
                filled = true;
                break;
            }
        }
        if filled {
            break;
        }
        match last.as_ref().filter(|k| !k.is_empty()) {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    let step = cursor_step(filled, pages < MAX_PAGES, last.as_ref());
    info!(
        sweep,
        pages,
        found = out.len(),
        from_middle,
        reached_end = step == CursorStep::Clear,
        "Runs scan"
    );
    store_cursor(state, sweep, step).await;
    Ok(out)
}

/// The reward message for a run whose close the webhook never delivered.
fn missed_outcome(p: &Pending, owner: &str, repo: &str, number: u64) -> RewardOutcomeMessage {
    RewardOutcomeMessage {
        team_id: p.team_id.clone(),
        installation_id: p.installation_id,
        run_id: p.run_id.clone(),
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number: number,
        ..Default::default()
    }
}

/// A close the webhook missed is learned from like any other: the run's
/// outcome was only written on the run before, so the coder's and the
/// reviewer's rewards never saw it. Scoring is idempotent per signal, so a
/// webhook that arrives late counts nothing twice.
async fn score_missed(state: &WorkerState, p: &Pending, owner: &str, repo: &str, number: u64) {
    if let Err(e) =
        super::rewards::score_outcome(state, missed_outcome(p, owner, repo, number)).await
    {
        warn!(run_id = %p.run_id, error = %e, "Reconcile: could not score the PR outcome");
    } else {
        info!(run_id = %p.run_id, pr = number, "Reconcile: scored a PR outcome the webhook missed");
    }
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

    fn key(run: &str) -> Key {
        HashMap::from([
            ("team_id".to_string(), attr_s("TEAM#a")),
            ("run_id".to_string(), attr_s(run)),
        ])
    }

    #[test]
    fn a_tick_that_runs_out_of_pages_carries_on_from_there() {
        assert_eq!(
            cursor_step(false, false, Some(&key("r20"))),
            CursorStep::Save(key("r20"))
        );
    }

    #[test]
    fn a_tick_that_reaches_the_end_starts_over_next_time() {
        assert_eq!(cursor_step(false, true, None), CursorStep::Clear);
        assert_eq!(cursor_step(false, false, None), CursorStep::Clear);
        assert_eq!(
            cursor_step(false, true, Some(&HashMap::new())),
            CursorStep::Clear
        );
    }

    #[test]
    fn a_tick_that_fills_up_keeps_its_starting_point() {
        assert_eq!(cursor_step(true, false, Some(&key("r9"))), CursorStep::Keep);
        assert_eq!(cursor_step(true, true, None), CursorStep::Keep);
    }

    /// Ticks over a table larger than the page cap: every page is reached,
    /// which the old restart-from-the-first-page scan never did.
    #[test]
    fn every_page_is_reached_across_ticks() {
        let total_pages = MAX_PAGES * 3 + 7;
        let mut cursor: Option<usize> = None;
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..10 {
            let from = cursor.unwrap_or(0);
            let mut pages = 0;
            let mut page = from;
            while pages < MAX_PAGES && page < total_pages {
                seen.insert(page);
                page += 1;
                pages += 1;
            }
            let last = (page < total_pages).then(|| key(&format!("r{page}")));
            match cursor_step(false, pages < MAX_PAGES, last.as_ref()) {
                CursorStep::Save(_) => cursor = Some(page),
                CursorStep::Clear => cursor = None,
                CursorStep::Keep => {}
            }
        }
        assert_eq!(seen.len(), total_pages);
    }

    #[test]
    fn a_saved_cursor_is_read_back_as_the_table_key() {
        let mut item = key("r5");
        item.insert("pk".into(), attr_s("SYSTEM"));
        item.insert("at".into(), attr_s("2026-11-07T00:00:00Z"));
        assert_eq!(key_from(Some(&item)), Some(key("r5")));
        assert_eq!(key_from(None), None);
    }

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

    /// "Reconciled PR outcomes on finished runs" wrote the outcome on the
    /// run and nothing else: a close the webhook missed never reached the
    /// rewards. The message it now scores names the run and the PR.
    #[test]
    fn a_missed_close_is_scored_for_its_run() {
        let p = Pending {
            team_id: "TEAM#a".into(),
            run_id: "R1".into(),
            pr_url: "https://github.com/acme/web/pull/7".into(),
            installation_id: 42,
        };
        let m = missed_outcome(&p, "acme", "web", 7);
        assert_eq!(
            (m.team_id.as_str(), m.run_id.as_str(), m.pr_number),
            ("TEAM#a", "R1", 7)
        );
        assert_eq!(
            (m.repo_owner.as_str(), m.repo_name.as_str()),
            ("acme", "web")
        );
        assert_eq!(m.installation_id, 42);
        assert!(!m.review_only);
    }

    #[test]
    fn finished_statuses_cover_completed_cancelled_closed_retried() {
        assert_eq!(
            FINISHED_STATUSES,
            ["completed", "cancelled", "closed", "retried"]
        );
    }
}
