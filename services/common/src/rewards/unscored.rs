//! Which of CoderHelm's closed pull requests have no score yet, and what is
//! done about each: the one rule the learning health page and the worker's
//! retry sweep share, so the page can never call a pull request missing
//! that the sweep doesn't try again, or the other way round.
//!
//! Four pull requests once closed within ten seconds of each other. Each
//! close reached its run and queued its scoring, and GitHub's budget, held
//! back for the runs, refused the scoring's first request. The message was
//! consumed with "Reward outcome failed", nothing tried again, and the page
//! said to check the GitHub App's pull request events, which had arrived.
//!
//! The rules:
//!
//! - A closed pull request is due once [`health::SCORE_GRACE_MINUTES`] passed
//!   since it closed, inside the page's window, and missing while no coder
//!   outcome event names it ([`missing`]).
//! - Its run records how its scoring goes: when the next try is due
//!   ([`RETRY_AT`]), how often it was sent again ([`TRIES`]), the last error
//!   ([`ERROR`]), and why it stopped ([`GAVE_UP`]). A close the webhook never
//!   brought, found by CoderHelm's own check, says so ([`OUTCOME_FROM`]).
//! - The sweep sends a missing one again unless a try is in flight, at most
//!   [`MAX_TRIES`] times; then it stops and says why ([`next`]).

use super::health::{self, ClosedPr, Retry};
use super::{RewardEvent, RewardOutcomeMessage};
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Utc};
use std::collections::{BTreeSet, HashMap};

pub type Item = HashMap<String, AttributeValue>;

/// On the run: when the next scoring try is due (RFC 3339).
pub const RETRY_AT: &str = "reward_retry_at";
/// On the run: how often its scoring was sent again.
pub const TRIES: &str = "reward_tries";
/// On the run: the last error a scoring try ended with.
pub const ERROR: &str = "reward_error";
/// On the run: why CoderHelm stopped trying to score it.
pub const GAVE_UP: &str = "reward_gave_up";
/// On the run: who recorded the close; [`FROM_SWEEP`] when GitHub's event
/// never brought it and CoderHelm's own check found it.
pub const OUTCOME_FROM: &str = "pr_outcome_from";
pub const FROM_SWEEP: &str = "sweep";

/// Times the sweep sends one pull request's scoring again before it stops.
/// A wait for GitHub is not a try: a message waits on its own
/// (`crate::github_wait`), and the sweep leaves it alone meanwhile.
pub const MAX_TRIES: u32 = 8;
/// How long past its due time a try is still taken to be in flight.
pub const IN_FLIGHT_GRACE_MINUTES: i64 = 30;
/// Minutes between two sweeps (the review-reminder tick).
pub const SWEEP_EVERY_MINUTES: i64 = 20;

/// The run fields the page and the sweep read.
pub const CLOSED_RUN_FIELDS: &[&str] = &[
    "run_id",
    "pr_url",
    "merged_at",
    "pr_closed_at",
    "prompt_variant",
    "installation_id",
    RETRY_AT,
    TRIES,
    ERROR,
    GAVE_UP,
    OUTCOME_FROM,
];

fn item_s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn item_u(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Pure: the CoderHelm pull requests that closed, from run items
/// (`pr_url`, `merged_at` or `pr_closed_at`), with how their scoring goes.
pub fn closed_prs_of(runs: &[Item]) -> Vec<ClosedPr> {
    runs.iter()
        .filter_map(|r| {
            let (owner, name, pr) = crate::pr_outcome::parse_pr_url(&item_s(r, "pr_url"))?;
            let closed_at = match item_s(r, "merged_at") {
                m if !m.is_empty() => m,
                _ => item_s(r, "pr_closed_at"),
            };
            // The coder arm its run picked; a run from before the coder had
            // variants has none, and its PR can't be credited to one.
            let variant = match item_s(r, "prompt_variant") {
                v if super::bandit::arm(super::event::AGENT_CODER, &v).is_some() => v,
                _ => String::new(),
            };
            (!closed_at.is_empty()).then(|| ClosedPr {
                repo: format!("{owner}/{name}"),
                pr,
                closed_at,
                variant,
                run_id: item_s(r, "run_id"),
                installation_id: item_u(r, "installation_id"),
                retry: Retry {
                    tries: item_u(r, TRIES).min(u32::MAX as u64) as u32,
                    retry_at: item_s(r, RETRY_AT),
                    error: item_s(r, ERROR),
                    gave_up: item_s(r, GAVE_UP),
                    from_sweep: item_s(r, OUTCOME_FROM) == FROM_SWEEP,
                },
            })
        })
        .collect()
}

/// The team's runs whose pull request merged or closed since `since`, with
/// [`CLOSED_RUN_FIELDS`]; at most `max_pages` pages.
pub async fn closed_runs(
    dynamo: &aws_sdk_dynamodb::Client,
    runs_table: &str,
    team: &str,
    since: &str,
    max_pages: usize,
) -> Result<Vec<Item>, String> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    let names: Vec<String> = (0..CLOSED_RUN_FIELDS.len())
        .map(|i| format!("#f{i}"))
        .collect();
    for _ in 0..max_pages.max(1) {
        let mut q = dynamo
            .query()
            .table_name(runs_table)
            .key_condition_expression("team_id = :t")
            .filter_expression("merged_at >= :s OR pr_closed_at >= :s")
            .projection_expression(names.join(", "))
            .expression_attribute_values(":t", AttributeValue::S(team.into()))
            .expression_attribute_values(":s", AttributeValue::S(since.into()))
            .set_exclusive_start_key(start.take());
        for (alias, name) in names.iter().zip(CLOSED_RUN_FIELDS) {
            q = q.expression_attribute_names(alias, *name);
        }
        let page = q
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
        out.extend(page.items().iter().cloned());
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

fn parse(t: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(t)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

/// The pull requests with a coder outcome event in `events`.
pub fn scored<'e>(events: impl IntoIterator<Item = &'e RewardEvent>) -> BTreeSet<(String, u64)> {
    events
        .into_iter()
        .filter(|e| e.agent == super::event::AGENT_CODER)
        .filter(|e| {
            matches!(
                e.kind,
                Some(super::RewardKind::PrMergedUnchanged)
                    | Some(super::RewardKind::PrMergedEdited)
                    | Some(super::RewardKind::PrClosedUnmerged)
                    | Some(super::RewardKind::TimeToMerge)
            )
        })
        .map(|e| (e.repo.clone(), e.pr))
        .collect()
}

/// The closed pull requests that should be scored by `now`: closed inside
/// the window and longer ago than the grace. One per pull request.
pub fn due(closed: &[ClosedPr], now: DateTime<Utc>) -> Vec<&ClosedPr> {
    let since = now - chrono::Duration::days(health::WINDOW_DAYS);
    let grace = now - chrono::Duration::minutes(health::SCORE_GRACE_MINUTES);
    let mut seen = BTreeSet::new();
    closed
        .iter()
        .filter(|p| parse(&p.closed_at).is_some_and(|d| d >= since && d <= grace))
        .filter(|p| seen.insert((p.repo.clone(), p.pr)))
        .collect()
}

/// The due pull requests no coder outcome event names: what the page calls
/// missing and the sweep sends again.
pub fn missing<'a, 'e>(
    closed: &'a [ClosedPr],
    events: impl IntoIterator<Item = &'e RewardEvent>,
    now: DateTime<Utc>,
) -> Vec<&'a ClosedPr> {
    let scored = scored(events);
    due(closed, now)
        .into_iter()
        .filter(|p| !scored.contains(&(p.repo.clone(), p.pr)))
        .collect()
}

/// What the sweep does with one missing pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Send its scoring again.
    Send,
    /// A try is due or waiting for GitHub: leave it.
    InFlight,
    /// Tried as often as it may: stop, and say why on the run.
    GiveUp(String),
    /// Already stopped.
    Stopped,
}

/// Whether a try sent at `retry_at` is still in flight at `now`.
pub fn in_flight(retry: &Retry, now: DateTime<Utc>) -> bool {
    parse(&retry.retry_at)
        .is_some_and(|at| at + chrono::Duration::minutes(IN_FLIGHT_GRACE_MINUTES) > now)
}

/// The sweep's decision for one missing pull request.
pub fn next(p: &ClosedPr, now: DateTime<Utc>) -> Next {
    if !p.retry.gave_up.is_empty() {
        return Next::Stopped;
    }
    if in_flight(&p.retry, now) {
        return Next::InFlight;
    }
    if p.retry.tries >= MAX_TRIES {
        return Next::GiveUp(gave_up_reason(p.retry.tries, &p.retry.error));
    }
    Next::Send
}

/// Why scoring stopped, in plain words.
pub fn gave_up_reason(tries: u32, error: &str) -> String {
    let error = error.trim();
    if error.is_empty() {
        format!("Scoring did not finish after {tries} tries")
    } else {
        format!(
            "Scoring did not finish after {tries} tries; the last one ended: {}",
            crate::truncate_str(error, 300)
        )
    }
}

/// The scoring message the sweep sends again for a missing pull request,
/// or None when its run can't say where it is.
pub fn rescore_message(team_id: &str, p: &ClosedPr) -> Option<RewardOutcomeMessage> {
    let (owner, name) = p.repo.split_once('/')?;
    if p.installation_id == 0 || p.run_id.is_empty() || owner.is_empty() || name.is_empty() {
        return None;
    }
    Some(RewardOutcomeMessage {
        team_id: team_id.to_string(),
        installation_id: p.installation_id,
        run_id: p.run_id.clone(),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: p.pr,
        rescore: true,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewards::event::AGENT_CODER;
    use crate::rewards::RewardKind;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 21, 12, 0, 0).unwrap()
    }

    fn closed(pr: u64, at: &str) -> ClosedPr {
        ClosedPr {
            repo: "acme/web".into(),
            pr,
            closed_at: at.into(),
            run_id: format!("R{pr}"),
            installation_id: 7,
            ..Default::default()
        }
    }

    #[test]
    fn a_closed_pr_is_missing_until_a_coder_outcome_names_it() {
        let prs = vec![
            closed(1, "2026-11-16T16:55:00Z"),
            closed(2, "2026-11-16T16:55:04Z"),
            // Within the grace: not due yet.
            closed(3, "2026-11-21T11:30:00Z"),
            // Before the window.
            closed(4, "2026-11-01T00:00:00Z"),
            // The same pull request from a second run counts once.
            closed(1, "2026-11-16T16:55:00Z"),
        ];
        let got: Vec<u64> = missing(&prs, &[], now()).iter().map(|p| p.pr).collect();
        assert_eq!(got, vec![1, 2]);
        let events = vec![
            RewardEvent::new(
                "T",
                RewardKind::PrClosedUnmerged,
                AGENT_CODER,
                "2026-11-21T11:00:00Z",
                "closed#acme/web#1",
            )
            .with_repo("acme/web", 1),
            // A reviewer's event is not the coder's outcome.
            RewardEvent::new(
                "T",
                RewardKind::FindingIgnored,
                "reviewer",
                "2026-11-21T11:00:00Z",
                "x",
            )
            .with_repo("acme/web", 2),
        ];
        let got: Vec<u64> = missing(&prs, &events, now()).iter().map(|p| p.pr).collect();
        assert_eq!(got, vec![2]);
    }

    #[test]
    fn the_sweep_sends_again_leaves_a_try_in_flight_and_stops_at_the_bound() {
        let mut p = closed(1, "2026-11-16T16:55:00Z");
        assert_eq!(next(&p, now()), Next::Send);
        p.retry.retry_at = "2026-11-21T12:10:00Z".into();
        assert_eq!(next(&p, now()), Next::InFlight);
        // Due long ago and still missing: that try is over.
        p.retry.retry_at = "2026-11-21T11:00:00Z".into();
        assert_eq!(next(&p, now()), Next::Send);
        p.retry.tries = MAX_TRIES;
        p.retry.error = "HTTP status client error (404 Not Found)".into();
        let Next::GiveUp(why) = next(&p, now()) else {
            panic!("should stop");
        };
        assert!(
            why.contains("after 8 tries") && why.contains("404"),
            "{why}"
        );
        p.retry.gave_up = why;
        assert_eq!(next(&p, now()), Next::Stopped);
    }

    #[test]
    fn the_message_names_the_run_and_the_pull_request() {
        let m = rescore_message("TEAM#a", &closed(9, "2026-11-16T16:55:00Z")).unwrap();
        assert_eq!(
            (m.repo_owner.as_str(), m.repo_name.as_str(), m.pr_number),
            ("acme", "web", 9)
        );
        assert_eq!((m.run_id.as_str(), m.installation_id), ("R9", 7));
        assert!(m.rescore && m.github_wait.is_idle() && !m.review_only);
        let mut no_install = closed(9, "2026-11-16T16:55:00Z");
        no_install.installation_id = 0;
        assert!(rescore_message("TEAM#a", &no_install).is_none());
    }

    #[test]
    fn closed_prs_carry_how_their_scoring_goes() {
        let mut run = Item::new();
        let s = |v: &str| AttributeValue::S(v.into());
        run.insert("run_id".into(), s("R1"));
        run.insert("pr_url".into(), s("https://github.com/acme/web/pull/3"));
        run.insert("pr_closed_at".into(), s("2026-11-16T16:55:00Z"));
        run.insert("installation_id".into(), AttributeValue::N("41".into()));
        run.insert(TRIES.into(), AttributeValue::N("2".into()));
        run.insert(RETRY_AT.into(), s("2026-11-16T18:00:05Z"));
        run.insert(OUTCOME_FROM.into(), s(FROM_SWEEP));
        let got = closed_prs_of(&[run]);
        assert_eq!(got.len(), 1);
        let p = &got[0];
        assert_eq!((p.run_id.as_str(), p.installation_id), ("R1", 41));
        assert_eq!(p.retry.tries, 2);
        assert_eq!(p.retry.retry_at, "2026-11-16T18:00:05Z");
        assert!(p.retry.from_sweep);
    }
}
