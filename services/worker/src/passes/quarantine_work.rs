//! Working the quarantine: no test is left there with nothing happening.
//!
//! The rules are `common::e2e::quarantine` (pure, tested there). This is the
//! part that touches GitHub and the queue:
//!
//! - **As each run is recorded** (`e2e::ingest`): a test that left
//!   quarantine by itself has its ticket closed with the evidence
//!   ([`after_release`]), a cause the runs disproved says what changed as far
//!   as the repository shows ([`say_what_changed`]), and the repository's
//!   automatic fixes go to the tests quarantined longest ([`start_auto_fixes`]).
//! - **In the periodic sweep** ([`sweep`], hourly per team): the same for
//!   tests no run touched, plus what only GitHub knows: a fix pull request
//!   that merged, was closed, or whose checks went red; a fix or removal run
//!   that ended with nothing; a removal pull request that merged (the record
//!   says who approved it) or was closed; tickets that are due.
//! - **Removing a test** ([`start_removal`]): a pull request that deletes the
//!   one test case and what only it used, with why removal is reasonable in
//!   its description ([`removal_pr_seen`], written through `description`).
//!   Coderhelm never merges it: the merge gate is told so for that pull
//!   request, whatever the team's on-approve setting.
//!
//! Automatic work (a fix or a removal nobody asked for) is counted against
//! the team's weekly number per repository; a person's request never is.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::e2e::messages::{TestAction, TestIngest};
use common::e2e::model::{PrSeen, Release, Removal, Settings, TestRecord};
use common::e2e::quarantine::{self, Peer, REMOVE_TICKET_PREFIX};
use common::e2e::stats::stats;
use common::e2e::store::Store;
use serde_json::Value;
use tracing::{info, warn};

use super::e2e::{enqueue, event, file_ticket, github, internal_ticket, store, ticket_due};
use crate::clients::github::GitHubClient;
use crate::models::WorkerMessage;
use crate::WorkerState;

type Err = Box<dyn std::error::Error + Send + Sync>;

/// Tests looked at per team in one sweep.
const MAX_LOOKED_AT: usize = 200;
/// The words the merge gate shows when it declines to merge a removal.
pub(crate) const NEVER_MERGE: &str =
    "A pull request that removes a test is the team's to approve and merge";
/// Who an automatic action is recorded as.
pub(crate) const AUTOMATIC: &str = "Coderhelm (automatic)";

fn parse(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn day(s: &str) -> String {
    parse(s)
        .map(|t| t.format("%-d %B %Y").to_string())
        .unwrap_or_else(|| s.to_string())
}

fn removal_branch(id: &str) -> String {
    format!("coderhelm/{}{id}", REMOVE_TICKET_PREFIX.to_lowercase())
}

/// The test a removal branch is for.
pub(crate) fn removal_test_id(branch: &str) -> Option<&str> {
    branch
        .strip_prefix("coderhelm/")?
        .strip_prefix(&REMOVE_TICKET_PREFIX.to_lowercase())
        .filter(|id| !id.is_empty())
}

fn pr_number(url: &str) -> Option<u64> {
    url.rsplit('/').next()?.parse().ok()
}

// ── reading pull requests ─────────────────────────────────────────────────

/// A commit's checks in one word, or `None` when it has none.
pub(crate) fn checks_state(check_runs: &Value) -> Option<&'static str> {
    let runs = check_runs["check_runs"].as_array()?;
    if runs.is_empty() {
        return None;
    }
    if runs
        .iter()
        .any(|c| c["status"].as_str() != Some("completed"))
    {
        return Some("running");
    }
    let failing = runs.iter().any(|c| {
        matches!(
            c["conclusion"].as_str(),
            Some("failure" | "timed_out" | "cancelled" | "action_required" | "startup_failure")
        )
    });
    Some(if failing { "failing" } else { "passing" })
}

/// A pull request as GitHub has it now.
pub(crate) fn pr_seen(pr: &Value, checks: Option<&str>, now: DateTime<Utc>) -> PrSeen {
    let merged = pr["merged"].as_bool() == Some(true) || pr["merged_at"].is_string();
    let state = if merged {
        "merged"
    } else if pr["state"].as_str() == Some("closed") {
        "closed"
    } else if pr["draft"].as_bool() == Some(true) {
        "draft"
    } else {
        "open"
    };
    let open = matches!(state, "open" | "draft");
    PrSeen {
        url: pr["html_url"].as_str().unwrap_or("").to_string(),
        state: state.to_string(),
        checks: checks.filter(|_| open).map(str::to_string),
        seen_at: now.to_rfc3339(),
        by: if merged {
            pr["merged_by"]["login"].as_str().map(str::to_string)
        } else {
            None
        },
    }
}

/// Everyone whose latest review of the pull request approves it.
pub(crate) fn approvers(reviews: &Value) -> Vec<String> {
    let mut latest: Vec<(String, String)> = Vec::new();
    for r in reviews.as_array().into_iter().flatten() {
        let (Some(who), Some(state)) = (r["user"]["login"].as_str(), r["state"].as_str()) else {
            continue;
        };
        // A comment leaves an earlier approval standing.
        if state == "COMMENTED" || state == "PENDING" {
            continue;
        }
        match latest.iter_mut().find(|(w, _)| w == who) {
            Some(e) => e.1 = state.to_string(),
            None => latest.push((who.to_string(), state.to_string())),
        }
    }
    latest
        .into_iter()
        .filter(|(_, s)| s == "APPROVED")
        .map(|(w, _)| w)
        .collect()
}

async fn read_pr(
    gh: &GitHubClient,
    owner: &str,
    name: &str,
    number: u64,
    now: DateTime<Utc>,
) -> Option<(Value, PrSeen)> {
    let pr = gh.get_pull_request(owner, name, number).await.ok()?;
    let checks = if pr["state"].as_str() == Some("open") {
        match pr["head"]["sha"].as_str() {
            Some(sha) => gh
                .list_check_runs_for_ref(owner, name, sha)
                .await
                .ok()
                .and_then(|c| checks_state(&c)),
            None => None,
        }
    } else {
        None
    };
    let seen = pr_seen(&pr, checks, now);
    Some((pr, seen))
}

// ── fixes ─────────────────────────────────────────────────────────────────

/// Records that the fix pull request merged: from here the test proves the
/// fix before it leaves quarantine.
pub(crate) fn fix_merged(r: &mut TestRecord, pr: &Value, merged_at: &str) {
    if r.fix_status.as_deref() == Some("merged") {
        return;
    }
    r.fix_status = Some("merged".into());
    r.fix_merged_at = Some(merged_at.to_string());
    let by = pr["merged_by"]["login"].as_str().unwrap_or("someone");
    let state = if r.quarantined {
        "quarantined"
    } else {
        "flaky"
    };
    r.events.push(event(
        state,
        state,
        format!(
            "Fix merged by {by}. The test now proves the fix: {} passes in a row on more than one pull request or branch, then it leaves quarantine by itself.",
            quarantine::RELEASE_PASSES
        ),
    ));
}

/// What a fix pull request's state on GitHub means for the test. Pure:
/// `None` leaves the record as it is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FixNews {
    Merged,
    /// Closed without merging.
    Closed,
}

pub(crate) fn fix_news(r: &TestRecord, seen: &PrSeen) -> Option<FixNews> {
    let already = r.care.fix_pr.as_ref().map(|p| p.state.as_str());
    match seen.state.as_str() {
        "merged" if r.fix_status.as_deref() != Some("merged") => Some(FixNews::Merged),
        "closed" if already != Some("closed") => Some(FixNews::Closed),
        _ => None,
    }
}

/// Brings the test's fix up to date with GitHub and the clock. Returns
/// whether the record changed.
async fn refresh_fix(
    gh: &GitHubClient,
    owner: &str,
    name: &str,
    r: &mut TestRecord,
    now: DateTime<Utc>,
) -> bool {
    let before = r.clone();
    let status = r.fix_status.clone();
    // A request the worker never started: asked for again, not counted as
    // an attempt.
    if status.as_deref() == Some("reproducing") && r.fix_phase_at.is_none() {
        let waited = r
            .care
            .fix_requested_at
            .as_deref()
            .and_then(parse)
            .is_none_or(|t| now - t >= chrono::Duration::hours(1));
        if waited {
            r.fix_status = None;
            r.care.fix_requested_at = None;
            r.events.push(event(
                "quarantined",
                "quarantined",
                "A fix was asked for and never started. It can be asked for again.".into(),
            ));
        }
        return *r != before;
    }
    // The pull request, by its address or by the fix branch.
    let mut number = r.fix_pr_url.as_deref().and_then(pr_number);
    if number.is_none() && matches!(status.as_deref(), Some("fixing" | "failed")) {
        if let Some(branch) = r.fix_branch.clone() {
            if let Ok(Some(pr)) = gh.find_open_pr_for_branch(owner, name, &branch).await {
                number = pr["number"].as_u64();
                r.fix_pr_url = pr["html_url"].as_str().map(str::to_string);
            }
        }
    }
    if let Some(n) = number {
        if let Some((pr, seen)) = read_pr(gh, owner, name, n, now).await {
            match fix_news(r, &seen) {
                Some(FixNews::Merged) => {
                    let at = pr["merged_at"]
                        .as_str()
                        .unwrap_or(&seen.seen_at)
                        .to_string();
                    fix_merged(r, &pr, &at);
                }
                Some(FixNews::Closed) if status.as_deref() != Some("failed") => {
                    let by = gh
                        .get_pr_closed_by(owner, name, n)
                        .await
                        .unwrap_or_else(|| "someone".into());
                    let at = pr["closed_at"].as_str().unwrap_or(&seen.seen_at);
                    let e = quarantine::fix_failed(
                        r,
                        now,
                        &format!(
                            "The fix pull request was closed without merging by {by} on {}.",
                            day(at)
                        ),
                    );
                    r.events.push(e);
                }
                _ => {}
            }
            // Its checks change every hour; only its state is worth a write.
            let same = r
                .care
                .fix_pr
                .as_ref()
                .is_some_and(|p| p.state == seen.state && p.checks == seen.checks);
            if !same {
                r.care.fix_pr = Some(seen);
            }
        }
    } else if matches!(status.as_deref(), Some("reproducing" | "fixing")) {
        // Started over a day ago, and nothing to show for it.
        let started = r.fix_phase_at.as_deref().and_then(parse);
        if started.is_some_and(|t| now - t >= chrono::Duration::hours(quarantine::STALLED_HOURS)) {
            let on = day(r.fix_phase_at.as_deref().unwrap_or(""));
            let why = if status.as_deref() == Some("reproducing") {
                format!("The repeat runs that reproduce it started on {on} and never reported back, so no fix was written.")
            } else {
                format!("The fix run that started on {on} ended without a pull request.")
            };
            let e = quarantine::fix_failed(r, now, &why);
            r.events.push(e);
        }
    }
    *r != before
}

// ── leaving quarantine by itself ──────────────────────────────────────────

/// A ticket's issue number, when it is an issue of this repository.
pub(crate) fn own_issue(url: &str, owner: &str, name: &str) -> Option<u64> {
    url.strip_prefix(&format!("https://github.com/{owner}/{name}/issues/"))?
        .parse()
        .ok()
}

/// Closes the ticket filed for a test that is no longer quarantined, with
/// why, so nobody is left holding it.
async fn close_ticket(gh: &GitHubClient, owner: &str, name: &str, r: &mut TestRecord, why: &str) {
    let Some(url) = r.ticket_url.clone() else {
        return;
    };
    let Some(n) = own_issue(&url, owner, name) else {
        return;
    };
    if let Err(e) = gh
        .create_issue_comment(
            owner,
            name,
            n,
            &format!("Closing: `{}` is no longer quarantined. {why}", r.title),
        )
        .await
    {
        warn!(test = %r.id, error = %e, "Could not note why a test's ticket is closed");
    }
    match gh.close_issue(owner, name, n).await {
        Ok(_) => {
            r.ticket_url = None;
            let state = r.health.as_str().to_string();
            r.events
                .push(event(&state, &state, format!("Ticket closed: {url}.")));
        }
        Err(e) => warn!(test = %r.id, error = %e, "Could not close a test's ticket"),
    }
}

/// A test left quarantine by itself: its ticket is closed with the evidence.
pub(crate) async fn after_release(
    gh: &GitHubClient,
    owner: &str,
    name: &str,
    r: &mut TestRecord,
    rel: &Release,
) {
    close_ticket(gh, owner, name, r, &rel.reason).await;
}

/// A cause was just marked out of date: adds what changed, as far as the
/// repository shows it (commits to the test's file since its last failure).
/// Says nothing when the file can't be found in the repository.
pub(crate) async fn say_what_changed(
    gh: &GitHubClient,
    owner: &str,
    name: &str,
    r: &mut TestRecord,
) {
    let since = quarantine::progress(r, Utc::now()).last_failure_at;
    let Some(since) = since else {
        return;
    };
    // Reports name the file from the repository root or from the test
    // package; only a path with history is the file.
    let mut path = None;
    for candidate in [r.file.clone(), format!("e2e/{}", r.file)] {
        if gh
            .commits_for_path(owner, name, &candidate, None, 1)
            .await
            .is_ok_and(|c| !c.is_empty())
        {
            path = Some(candidate);
            break;
        }
    }
    let Some(path) = path else {
        return;
    };
    let Ok(commits) = gh
        .commits_for_path(owner, name, &path, Some(&since), 5)
        .await
    else {
        return;
    };
    let commits: Vec<(String, String)> = commits
        .iter()
        .map(|c| {
            (
                c["sha"].as_str().unwrap_or("").chars().take(7).collect(),
                c["commit"]["message"]
                    .as_str()
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect();
    if let Some(c) = r.cause.as_mut() {
        if let Some(stale) = c.out_of_date.as_mut() {
            stale.push(' ');
            stale.push_str(&quarantine::what_changed(&commits));
        }
    }
}

// ── automatic fixes ───────────────────────────────────────────────────────

/// Starts the repository's automatic fixes, for the tests quarantined
/// longest first, until the week's number is used. Returns how many started.
pub(crate) async fn start_auto_fixes(
    state: &WorkerState,
    st: &Store<'_>,
    team: &str,
    repo: &str,
    settings: &Settings,
    now: DateTime<Utc>,
) -> u32 {
    let Ok(rows) = st.summaries(team, Some(repo)).await else {
        return 0;
    };
    let eligible: Vec<Value> = rows
        .into_iter()
        .filter(|v| v["auto_fix"].as_str() == Some("eligible"))
        .collect();
    let mut started = 0;
    for row in quarantine::auto_fix_order(&eligible) {
        let Some(id) = row["id"].as_str() else {
            continue;
        };
        // The summary may be days old: the record decides.
        let Ok(Some(mut r)) = st.record(team, id).await else {
            continue;
        };
        if quarantine::auto_hold(&r, &stats(&r, now), now).is_some() {
            continue;
        }
        if !st
            .claim_auto_fix(team, repo, now, settings.auto_fixes_per_week)
            .await
        {
            info!(
                repo,
                cap = settings.auto_fixes_per_week,
                "Automatic fixes are at this week's limit for the repository"
            );
            break;
        }
        // Marked first, so another run looking at the same moment does not
        // pick it too.
        r.fix_status = Some("reproducing".into());
        r.fix_phase_at = None;
        r.care.fix_requested_at = Some(now.to_rfc3339());
        if let Err(e) = st.put_record(team, &r).await {
            warn!(test = id, error = %e, "Could not mark an automatic fix as asked for");
            continue;
        }
        info!(repo, test = id, quarantined_since = ?r.quarantined_since, "Starting an automatic fix");
        let action = WorkerMessage::TestAction(TestAction {
            team_id: team.to_string(),
            repo: repo.to_string(),
            action: "fix".into(),
            target_id: id.to_string(),
            actor: AUTOMATIC.into(),
            setup: None,
        });
        match enqueue(state, action).await {
            Ok(()) => started += 1,
            Err(e) => warn!(error = %e, "Could not start an automatic fix"),
        }
    }
    started
}

// ── removing a test ───────────────────────────────────────────────────────

fn peers_of(rows: &[Value], id: &str) -> Vec<Peer> {
    rows.iter()
        .filter(|v| v["id"].as_str() != Some(id))
        .map(Peer::of)
        .collect()
}

/// Starts the run that opens a pull request removing the test. Asked for by
/// a person it starts at once; asked for by Coderhelm it starts only when
/// the rule is met.
pub(crate) async fn start_removal(
    state: &WorkerState,
    st: &Store<'_>,
    msg: &TestIngest,
    a: &TestAction,
) -> Result<(), Err> {
    let Some(mut r) = st.record(&a.team_id, &a.target_id).await? else {
        return Ok(());
    };
    let now = Utc::now();
    let own = a.actor == AUTOMATIC;
    let by = if own { "coderhelm" } else { a.actor.as_str() };
    let rows = st.summaries(&a.team_id, Some(&a.repo)).await?;
    let peers = peers_of(&rows, &r.id);
    // The gateway marks a person's request as running before this starts.
    let asked = r
        .care
        .removal
        .as_ref()
        .is_some_and(|m| m.status == "running" && m.run_id.is_none());
    // Judged as the record stood before that mark.
    let mut unmarked = r.clone();
    if asked {
        unmarked.care.removal = None;
    }
    let refused = if own {
        quarantine::may_propose_removal(&unmarked, now, &peers).err()
    } else {
        quarantine::removal_blocked(&unmarked, now)
    };
    if let Some(why) = refused {
        info!(test = %r.id, why, "Not starting a removal");
        if asked {
            r.care.removal = None;
        }
        r.events.push(event(
            "quarantined",
            "quarantined",
            format!(
                "No removal was started (asked by {}): {}",
                a.actor,
                why.trim_end_matches('.')
            ),
        ));
        st.put_record(&a.team_id, &r).await?;
        return Ok(());
    }
    let evidence = quarantine::removal_evidence(&r, &stats(&r, now), now, &peers);
    let ticket_id = format!("{REMOVE_TICKET_PREFIX}{}", r.id);
    let run_id = ulid::Ulid::new().to_string();
    let ticket = internal_ticket(
        msg,
        &ticket_id,
        common::truncate_str(&format!("Remove quarantined test: {}", r.title), 200),
        quarantine::removal_ticket(&r, &evidence, by),
        &run_id,
    );
    enqueue(state, WorkerMessage::Ticket(ticket)).await?;
    r.care.removal = Some(Removal {
        status: "running".into(),
        by: by.to_string(),
        at: now.to_rfc3339(),
        run_id: Some(run_id),
        pr: None,
        evidence,
        reason: None,
        approved_by: Vec::new(),
    });
    r.events.push(event(
        "quarantined",
        "quarantined",
        if own {
            format!("Proposing its removal. {}", quarantine::removal_rule())
        } else {
            format!("Writing a pull request that removes it (asked by {by}).")
        },
    ));
    st.put_record(&a.team_id, &r).await?;
    Ok(())
}

/// Tells the merge gate never to merge this pull request, whatever the
/// team's on-approve setting (it reads this record right before merging).
async fn never_merge(state: &WorkerState, team: &str, owner: &str, name: &str, number: u64) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let mut c = super::review_fix::pr_constraints(state, team, owner, name, number).await;
    if c.no_merge {
        return;
    }
    c.no_merge = true;
    c.quotes.push(NEVER_MERGE.to_string());
    let Ok(json) = serde_json::to_string(&c) else {
        return;
    };
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", A::S(team.to_string()))
        .item(
            "sk",
            A::S(super::review_fix::constraints::pr_sk(owner, name, number)),
        )
        .item("constraints", A::S(json))
        .send()
        .await
    {
        warn!(pr = number, error = %e, "Could not record that a removal is never merged by Coderhelm");
    }
}

/// The removal's pull request exists: it is linked to the test, its
/// description gets the evidence, and the merge gate is told to leave it.
pub(crate) async fn removal_pr_seen(
    state: &WorkerState,
    gh: &GitHubClient,
    team: &str,
    owner: &str,
    name: &str,
    id: &str,
    pr: &Value,
) {
    let st = store(state);
    let Ok(Some(mut r)) = st.record(team, id).await else {
        return;
    };
    let Some(number) = pr["number"].as_u64() else {
        return;
    };
    never_merge(state, team, owner, name, number).await;
    let now = Utc::now();
    let seen = pr_seen(pr, None, now);
    let Some(m) = r.care.removal.as_mut() else {
        return;
    };
    let first = m.pr.is_none();
    if matches!(m.status.as_str(), "running" | "failed") {
        m.status = "opened".into();
        m.reason = None;
    }
    m.pr = Some(seen.clone());
    let block = quarantine::removal_block(m);
    if let Some(body) = quarantine::with_removal_block(pr["body"].as_str().unwrap_or(""), &block) {
        if let Err(e) = super::description::save(gh, owner, name, number, &body).await {
            warn!(pr = number, error = %e, "Could not add the removal's evidence to its description");
        }
    }
    if first {
        r.events.push(event(
            "quarantined",
            "quarantined",
            format!(
                "Removal pull request opened: {}. It is the team's to approve and merge.",
                seen.url
            ),
        ));
    }
    if let Err(e) = st.put_record(team, &r).await {
        warn!(test = id, error = %e, "Could not link the removal pull request to its test");
    }
}

/// Brings a removal up to date with GitHub and the clock. Returns whether
/// the record changed.
async fn refresh_removal(
    state: &WorkerState,
    gh: &GitHubClient,
    team: &str,
    owner: &str,
    name: &str,
    r: &mut TestRecord,
    now: DateTime<Utc>,
) -> bool {
    let before = r.clone();
    let Some(m) = r.care.removal.clone() else {
        return false;
    };
    if !matches!(m.status.as_str(), "running" | "opened") {
        return false;
    }
    let number = match m.pr.as_ref().and_then(|p| pr_number(&p.url)) {
        Some(n) => Some(n),
        None => gh
            .find_open_pr_for_branch(owner, name, &removal_branch(&r.id))
            .await
            .ok()
            .flatten()
            .and_then(|p| p["number"].as_u64()),
    };
    let Some(n) = number else {
        // Started over a day ago, and no pull request.
        if parse(&m.at)
            .is_some_and(|t| now - t >= chrono::Duration::hours(quarantine::STALLED_HOURS))
        {
            let why = format!(
                "The removal run that started on {} ended without a pull request.",
                day(&m.at)
            );
            if let Some(m) = r.care.removal.as_mut() {
                m.status = "failed".into();
                m.reason = Some(why.clone());
            }
            r.events.push(event("quarantined", "quarantined", why));
        }
        return *r != before;
    };
    let Some((pr, seen)) = read_pr(gh, owner, name, n, now).await else {
        return false;
    };
    match seen.state.as_str() {
        "merged" => {
            let approved = gh
                .list_pr_reviews(owner, name, n)
                .await
                .map(|v| approvers(&v))
                .unwrap_or_default();
            let by = seen.by.clone().unwrap_or_else(|| "someone".into());
            if let Some(m) = r.care.removal.as_mut() {
                m.pr = Some(seen);
            }
            quarantine::removed(r, now, approved, &by);
            close_ticket(
                gh,
                owner,
                name,
                r,
                "It was removed by a merged pull request.",
            )
            .await;
        }
        "closed" => {
            let by = gh
                .get_pr_closed_by(owner, name, n)
                .await
                .unwrap_or_else(|| "someone".into());
            let why = format!(
                "The removal pull request was closed without merging by {by} on {}. Coderhelm does not propose it again; it can still be asked for.",
                day(pr["closed_at"].as_str().unwrap_or(&seen.seen_at))
            );
            if let Some(m) = r.care.removal.as_mut() {
                m.status = "declined".into();
                m.reason = Some(why.clone());
                m.pr = Some(seen);
            }
            r.events.push(event("quarantined", "quarantined", why));
        }
        _ => {
            // Still open: linked, never merged by Coderhelm, and its
            // description keeps the evidence through any rewrite.
            if m.status == "running" || m.pr.is_none() {
                removal_pr_seen(state, gh, team, owner, name, &r.id, &pr).await;
                if let Ok(Some(fresh)) = store(state).record(team, &r.id).await {
                    *r = fresh;
                }
            } else {
                let block = quarantine::removal_block(&m);
                if let Some(body) =
                    quarantine::with_removal_block(pr["body"].as_str().unwrap_or(""), &block)
                {
                    let _ = super::description::save(gh, owner, name, n, &body).await;
                }
            }
            if let Some(m) = r.care.removal.as_mut() {
                let same =
                    m.pr.as_ref()
                        .is_some_and(|p| p.state == seen.state && p.checks == seen.checks);
                if !same {
                    m.pr = Some(seen);
                }
            }
        }
    }
    *r != before
}

// ── the sweep ─────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct SweepReport {
    pub teams: usize,
    pub looked_at: usize,
    pub released: usize,
    pub changed: usize,
    pub fixes_started: u32,
    pub removals_proposed: u32,
}

/// Whether the sweep has anything to do for a test, from its summary.
pub(crate) fn needs_look(row: &Value) -> bool {
    if row["removed"].as_bool() == Some(true) {
        return false;
    }
    row["state"].as_str() == Some("quarantined")
        || !row["quarantine"].is_null()
        || matches!(
            row["fix_status"].as_str(),
            Some("reproducing" | "fixing" | "burn_in" | "opened")
        )
        || matches!(
            row["removal"]["status"].as_str(),
            Some("running" | "opened")
        )
}

/// Looks at every quarantined test of every team with E2E results on, at
/// most once an hour per team.
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let st = store(state);
    if !st.enabled() {
        return report;
    }
    let teams = match st.teams().await {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "quarantine: the teams index could not be read");
            return report;
        }
    };
    let hour = Utc::now().format("%Y%m%d%H").to_string();
    for team in teams {
        let Ok(settings) = st.settings(&team).await else {
            continue;
        };
        if !settings.enabled {
            continue;
        }
        let claim = format!("E2E_QUARANTINE_SWEEP#{hour}");
        if !super::review_pr::claim_once_ttl(state, &team, &claim, 2 * 3600).await {
            continue;
        }
        report.teams += 1;
        if let Err(e) = sweep_team(state, &st, &team, &settings, &mut report).await {
            warn!(team = %team, error = %e, "quarantine: sweep failed for a team");
        }
    }
    report
}

async fn sweep_team(
    state: &WorkerState,
    st: &Store<'_>,
    team: &str,
    settings: &Settings,
    report: &mut SweepReport,
) -> Result<(), Err> {
    let rows = st.summaries(team, None).await?;
    let mut by_repo: HashMap<String, Vec<&Value>> = HashMap::new();
    for row in rows.iter().filter(|r| needs_look(r)) {
        by_repo
            .entry(row["repo"].as_str().unwrap_or("").to_string())
            .or_default()
            .push(row);
    }
    let mut looked_at = 0;
    for repo_rec in st.repos(team).await? {
        let Some(repo) = repo_rec["repo"].as_str() else {
            continue;
        };
        if !settings.covers(repo) {
            continue;
        }
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        let gh = match github(state, repo_rec["installation_id"].as_u64().unwrap_or(0)) {
            Ok(g) => g,
            Err(e) => {
                warn!(repo, error = %e, "quarantine: no GitHub client for the repository");
                continue;
            }
        };
        for row in by_repo.get(repo).map(Vec::as_slice).unwrap_or(&[]) {
            if looked_at >= MAX_LOOKED_AT {
                break;
            }
            let Some(id) = row["id"].as_str() else {
                continue;
            };
            let Ok(Some(mut r)) = st.record(team, id).await else {
                continue;
            };
            looked_at += 1;
            let now = Utc::now();
            let before = r.clone();
            let settled = quarantine::settle(&mut r, now);
            r.events.extend(settled.events.iter().cloned());
            if settled.cause_went_stale {
                say_what_changed(&gh, owner, name, &mut r).await;
            }
            if let Some(rel) = &settled.released {
                report.released += 1;
                info!(
                    test = id,
                    repo,
                    passes = rel.passes,
                    "Test left quarantine by itself (sweep)"
                );
                after_release(&gh, owner, name, &mut r, rel).await;
            }
            refresh_fix(&gh, owner, name, &mut r, now).await;
            refresh_removal(state, &gh, team, owner, name, &mut r, now).await;
            if ticket_due(&r, settings, now) {
                file_ticket(&gh, owner, name, settings, &mut r).await;
            }
            // What the row says can change with the date alone (a cool-off
            // that ended, a test that stopped running): the saved summary
            // is brought up to date even when the record is the same.
            let fresh = common::e2e::stats::summary(&r, now);
            let said =
                fresh["quarantine"] != row["quarantine"] || fresh["auto_fix"] != row["auto_fix"];
            if r != before || said {
                report.changed += 1;
                if r.events.len() > 100 {
                    let drop = r.events.len() - 100;
                    r.events.drain(..drop);
                }
                st.put_record(team, &r).await?;
            }
        }
        if !settings.fix_prs {
            continue;
        }
        let now = Utc::now();
        report.fixes_started += start_auto_fixes(state, st, team, repo, settings, now).await;
        report.removals_proposed += propose_removal(state, st, team, repo, settings, now).await;
    }
    report.looked_at += looked_at;
    Ok(())
}

/// Proposes at most one removal per repository per sweep, for the test
/// quarantined longest that meets the rule, counted against the same weekly
/// number as automatic fixes.
async fn propose_removal(
    state: &WorkerState,
    st: &Store<'_>,
    team: &str,
    repo: &str,
    settings: &Settings,
    now: DateTime<Utc>,
) -> u32 {
    let Ok(rows) = st.summaries(team, Some(repo)).await else {
        return 0;
    };
    let quarantined: Vec<Value> = rows
        .iter()
        .filter(|v| v["state"].as_str() == Some("quarantined") && v["removal"].is_null())
        .cloned()
        .collect();
    for row in quarantine::auto_fix_order(&quarantined) {
        let Some(id) = row["id"].as_str() else {
            continue;
        };
        let Ok(Some(r)) = st.record(team, id).await else {
            continue;
        };
        if quarantine::may_propose_removal(&r, now, &peers_of(&rows, id)).is_err() {
            continue;
        }
        if !st
            .claim_auto_fix(team, repo, now, settings.auto_fixes_per_week)
            .await
        {
            return 0;
        }
        info!(
            repo,
            test = id,
            "Proposing the removal of a long-quarantined test"
        );
        let action = WorkerMessage::TestAction(TestAction {
            team_id: team.to_string(),
            repo: repo.to_string(),
            action: "remove".into(),
            target_id: id.to_string(),
            actor: AUTOMATIC.into(),
            setup: None,
        });
        return match enqueue(state, action).await {
            Ok(()) => 1,
            Err(e) => {
                warn!(error = %e, "Could not propose a removal");
                0
            }
        };
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn now() -> DateTime<Utc> {
        parse("2026-11-12T09:00:00Z").unwrap()
    }

    #[test]
    fn checks_read_as_one_word() {
        assert_eq!(checks_state(&json!({ "check_runs": [] })), None);
        assert_eq!(checks_state(&json!({})), None);
        let runs = |v: Value| json!({ "check_runs": v });
        assert_eq!(
            checks_state(&runs(json!([
                { "status": "completed", "conclusion": "success" },
                { "status": "completed", "conclusion": "skipped" },
                { "status": "completed", "conclusion": "neutral" }
            ]))),
            Some("passing")
        );
        assert_eq!(
            checks_state(&runs(json!([
                { "status": "completed", "conclusion": "success" },
                { "status": "completed", "conclusion": "failure" }
            ]))),
            Some("failing")
        );
        assert_eq!(
            checks_state(&runs(json!([
                { "status": "completed", "conclusion": "failure" },
                { "status": "in_progress", "conclusion": null }
            ]))),
            Some("running"),
            "still running: not yet failed"
        );
    }

    #[test]
    fn a_pull_request_reads_as_open_draft_merged_or_closed() {
        let pr = |v: Value| {
            let mut p =
                json!({ "html_url": "https://github.com/acme/web/pull/41", "state": "open" });
            for (k, x) in v.as_object().unwrap() {
                p[k] = x.clone();
            }
            p
        };
        let seen = pr_seen(&pr(json!({})), Some("passing"), now());
        assert_eq!(
            seen,
            PrSeen {
                url: "https://github.com/acme/web/pull/41".into(),
                state: "open".into(),
                checks: Some("passing".into()),
                seen_at: now().to_rfc3339(),
                by: None,
            }
        );
        assert_eq!(
            pr_seen(&pr(json!({ "draft": true })), None, now()).state,
            "draft"
        );
        let merged = pr_seen(
            &pr(
                json!({ "state": "closed", "merged": true, "merged_by": { "login": "robin-lead" } }),
            ),
            Some("passing"),
            now(),
        );
        assert_eq!(
            (merged.state.as_str(), merged.by.as_deref(), merged.checks),
            ("merged", Some("robin-lead"), None)
        );
        assert_eq!(
            pr_seen(
                &pr(json!({ "state": "closed", "merged": false })),
                None,
                now()
            )
            .state,
            "closed"
        );
    }

    #[test]
    fn approvers_are_the_latest_review_of_each_person() {
        let reviews = json!([
            { "user": { "login": "robin-lead" }, "state": "CHANGES_REQUESTED" },
            { "user": { "login": "sam-reviewer" }, "state": "APPROVED" },
            { "user": { "login": "robin-lead" }, "state": "APPROVED" },
            { "user": { "login": "sam-reviewer" }, "state": "COMMENTED" },
            { "user": { "login": "kit-dev" }, "state": "APPROVED" },
            { "user": { "login": "kit-dev" }, "state": "DISMISSED" },
            { "state": "APPROVED" }
        ]);
        assert_eq!(approvers(&reviews), vec!["robin-lead", "sam-reviewer"]);
        assert!(approvers(&json!(null)).is_empty());
    }

    #[test]
    fn removal_branches_name_their_test_and_fix_branches_do_not() {
        assert_eq!(removal_branch("ab12"), "coderhelm/e2e-rm-ab12");
        assert_eq!(removal_test_id("coderhelm/e2e-rm-ab12"), Some("ab12"));
        assert_eq!(removal_test_id("coderhelm/e2e-fix-ab12"), None);
        assert_eq!(removal_test_id("coderhelm/e2e-rm-"), None);
        assert_eq!(removal_test_id("feature/e2e-rm-ab12"), None);
        assert_eq!(pr_number("https://github.com/acme/web/pull/41"), Some(41));
        assert_eq!(pr_number(""), None);
    }

    #[test]
    fn only_a_ticket_in_this_repository_is_closed() {
        assert_eq!(
            own_issue("https://github.com/acme/web/issues/77", "acme", "web"),
            Some(77)
        );
        assert_eq!(
            own_issue("https://github.com/acme/api/issues/77", "acme", "web"),
            None
        );
        assert_eq!(
            own_issue("https://example.atlassian.net/browse/WEB-7", "acme", "web"),
            None
        );
    }

    #[test]
    fn the_sweep_looks_at_quarantined_tests_and_work_in_flight_only() {
        assert!(needs_look(&json!({ "state": "quarantined" })));
        assert!(needs_look(
            &json!({ "state": "flaky", "fix_status": "opened" })
        ));
        assert!(needs_look(
            &json!({ "state": "flaky", "fix_status": "reproducing" })
        ));
        assert!(needs_look(
            &json!({ "state": "healthy", "removal": { "status": "opened" } })
        ));
        assert!(!needs_look(&json!({ "state": "healthy" })));
        assert!(!needs_look(
            &json!({ "state": "flaky", "fix_status": "merged" })
        ));
        assert!(!needs_look(
            &json!({ "state": "flaky", "fix_status": "failed" })
        ));
        assert!(!needs_look(
            &json!({ "state": "quarantined", "removed": true })
        ));
    }

    /// The pass's own code: no comment lines, nothing from its tests on.
    fn code() -> String {
        let src = include_str!("quarantine_work.rs");
        let end = src.find("#[cfg(test)]").unwrap();
        src[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_removal_is_never_merged_and_its_description_goes_through_the_one_writer() {
        let code = code();
        assert!(!code.contains("merge_pull_request("));
        assert!(!code.contains("update_pr_body("));
        assert!(code.contains("super::description::save("));
        // Linked, told to the merge gate, then described: in that order,
        // so a pull request is never open without the gate knowing.
        let seen = code.find("pub(crate) async fn removal_pr_seen").unwrap();
        let body = &code[seen..];
        assert!(body.find("never_merge(").unwrap() < body.find("description::save(").unwrap());
    }

    #[test]
    fn automatic_work_is_counted_and_a_persons_request_is_not() {
        let code = code();
        // Both automatic paths take one of the week's runs before they
        // enqueue anything.
        for f in [
            "pub(crate) async fn start_auto_fixes",
            "async fn propose_removal",
        ] {
            let at = code.find(f).unwrap_or_else(|| panic!("{f} is gone"));
            let body = &code[at..];
            let end = body[1..]
                .find("\nasync fn ")
                .or(body[1..].find("\npub(crate) async fn "))
                .map_or(body.len(), |i| i + 1);
            let body = &body[..end];
            assert!(
                body.find("claim_auto_fix(").unwrap() < body.find("enqueue(").unwrap(),
                "{f}"
            );
        }
        // The removal a person asks for never touches the count.
        let at = code.find("pub(crate) async fn start_removal").unwrap();
        let body = &code[at..];
        let end = body.find("\nasync fn never_merge").unwrap();
        assert!(!body[..end].contains("claim_auto_fix("));
        // ...and neither does the fix a person asks for.
        let e2e = include_str!("e2e.rs");
        assert!(!e2e[..e2e.find("#[cfg(test)]").unwrap()].contains("claim_auto_fix("));
    }

    #[test]
    fn a_merged_fix_starts_the_proof_once() {
        let mut r: TestRecord = serde_json::from_value(json!({
            "id": "x", "repo": "acme/web", "framework": "playwright", "file": "e2e/a.spec.ts",
            "title": "a › b", "health": "flaky", "quarantined": true,
            "quarantined_since": "2026-10-20T00:00:00Z", "last_seen": "2026-11-12T08:00:00Z",
            "fix_status": "opened", "fix_pr_url": "https://github.com/acme/web/pull/41"
        }))
        .unwrap();
        let seen = PrSeen {
            state: "merged".into(),
            ..Default::default()
        };
        assert_eq!(fix_news(&r, &seen), Some(FixNews::Merged));
        let pr = json!({ "merged_by": { "login": "robin-lead" } });
        fix_merged(&mut r, &pr, "2026-11-12T08:30:00Z");
        assert_eq!(r.fix_status.as_deref(), Some("merged"));
        assert_eq!(r.fix_merged_at.as_deref(), Some("2026-11-12T08:30:00Z"));
        assert!(
            r.quarantined,
            "merging is not leaving: it proves itself first"
        );
        assert_eq!(
            r.events.last().unwrap().reason,
            "Fix merged by robin-lead. The test now proves the fix: 20 passes in a row on more than one pull request or branch, then it leaves quarantine by itself."
        );
        assert_eq!(fix_news(&r, &seen), None);
        fix_merged(&mut r, &pr, "2026-11-12T09:30:00Z");
        assert_eq!(r.events.len(), 1);
        // A pull request closed without merging is news once.
        let closed = PrSeen {
            state: "closed".into(),
            ..Default::default()
        };
        r.fix_status = Some("opened".into());
        assert_eq!(fix_news(&r, &closed), Some(FixNews::Closed));
        r.care.fix_pr = Some(closed.clone());
        assert_eq!(fix_news(&r, &closed), None);
    }
}
