//! lw. A person closed four of CoderHelm's pull requests within ten
//! seconds. Each close reached its run (`pr_outcome = closed`) and queued
//! its scoring. The installation had 2353 of 5000 GitHub requests left,
//! under the half kept for runs, so CoderHelm's own budget gate held the
//! scoring's first read: "Reward outcome failed: ... 403 ... not sent, what
//! is left of GitHub's hourly budget is kept for runs". The handler only
//! logged it and the message was consumed; the lesson job lost its
//! question the same way. Nothing tried again: the sweep only looks at
//! runs with no outcome, and these had one. For days the learning page
//! read "Not learning: 4 closed pull request(s) were never scored" and
//! told a person to check the GitHub App's pull request events, which had
//! arrived.
//!
//! Guarantee: a held or refused GitHub request is a wait for learning work,
//! never a failure. The scoring and the lesson are sent again as themselves
//! for after GitHub's reset, their waits counted and bounded, and scoring
//! starts only when the budget has room for all of it. A sweep finds every
//! closed pull request with no score by the learning page's own rule and
//! sends its scoring again, a few at a time, never while a try is in
//! flight, at most a bounded number of times. Scoring twice adds nothing.
//! While CoderHelm retries, the page says so and when; it asks a person to
//! check the GitHub App only for a close GitHub's event never brought.

use super::fixture::*;
use crate::clients::github;
use crate::clients::github_budget;
use crate::models::LessonMessage;
use crate::passes::learning_wait::{self as lw, Again, Step, SweepReport};
use crate::passes::rewards::{outcome_error, OutcomeError, SCORING_REQUESTS};
use common::github_budget::{caller as who, Budget, Spend};
use common::github_wait::{self as gw, Cause};
use common::rewards::event::AGENT_CODER;
use common::rewards::health::{self, ClosedPr, State};
use common::rewards::store::{add_all_once, MemoryLog};
use common::rewards::{mapping, unscored, RewardKind, RewardOutcomeMessage};
use serde_json::json;

const OLD: &str = "were never scored";
const OLD_CHECK: &str = "GitHub App";
const TEAM: &str = "TEAM#team-a";
const INSTALL: u64 = 940_017;
const PRS: [u64; 4] = [41, 42, 43, 44];

fn at(t: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(t)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

fn epoch(t: &str) -> u64 {
    at(t).timestamp() as u64
}

fn rfc(epoch: u64) -> String {
    chrono::DateTime::from_timestamp(epoch as i64, 0)
        .unwrap()
        .to_rfc3339()
}

/// The four closes as their runs recorded them.
fn closes() -> Vec<ClosedPr> {
    PRS.iter()
        .enumerate()
        .map(|(i, pr)| ClosedPr {
            repo: format!("{OWNER}/{REPO}"),
            pr: *pr,
            closed_at: format!("2026-11-16T16:55:0{}Z", i * 3),
            run_id: format!("01RUN{pr}"),
            installation_id: INSTALL,
            ..Default::default()
        })
        .collect()
}

/// What the gateway queued for each close.
fn queued(p: &ClosedPr) -> RewardOutcomeMessage {
    RewardOutcomeMessage {
        team_id: TEAM.into(),
        installation_id: INSTALL,
        run_id: p.run_id.clone(),
        repo_owner: OWNER.into(),
        repo_name: REPO.into(),
        pr_number: p.pr,
        ..Default::default()
    }
}

/// The error the budget gate gave every read, as the worker logged it.
fn held_read(pr: u64) -> Box<dyn std::error::Error + Send + Sync> {
    format!(
        "HTTP status client error (403 Forbidden) for url \
         (https://api.github.com/repos/{OWNER}/{REPO}/pulls/{pr}): not sent, what is left of \
         GitHub's hourly budget is kept for runs; API rate limit, the work waits for the reset"
    )
    .into()
}

/// The coder's events for a pull request closed unmerged.
fn closed_events(p: &ClosedPr) -> Vec<common::rewards::RewardEvent> {
    let ctx = mapping::RunContext {
        team_id: TEAM.into(),
        run_id: p.run_id.clone(),
        repo: p.repo.clone(),
        pr: p.pr,
        ..Default::default()
    };
    let pr = json!({
        "number": p.pr, "state": "closed", "merged_at": null,
        "closed_at": p.closed_at, "created_at": "2026-11-16T15:00:00Z",
    });
    mapping::pr_closed_events(&ctx, &pr, Some(0), None, Default::default())
}

#[tokio::test]
async fn held_scorings_wait_are_swept_and_score_once() {
    let held_at = epoch("2026-11-16T16:55:10Z");
    let reset = epoch("2026-11-16T18:25:00Z");
    github::record_budget(
        INSTALL,
        Budget {
            remaining: 2_353,
            limit: 5_000,
            reset,
        },
    );

    // 1. The budget holds every learning request: under the runs' share.
    let (standing, allowance) = github_budget::scoped(who::SWEEP_LEARNING, async {
        (
            github_budget::standing(INSTALL, github_budget::caller(), held_at),
            github_budget::allowance(INSTALL, github_budget::caller(), held_at),
        )
    })
    .await;
    assert!(matches!(standing, Spend::Hold { .. }), "{standing:?}");
    // Scoring doesn't start into a hold: it waits for the reset whole.
    let hold = lw::hold_of(standing, allowance, SCORING_REQUESTS, Some(reset), held_at);
    assert_eq!(hold, Some((Cause::RateLimit, reset - held_at)));

    // 2. A held read is a wait, never a failure.
    let closes = closes();
    let mut waiting = Vec::new();
    for p in &closes {
        let err = outcome_error(&*held_read(p.pr));
        assert!(
            matches!(
                err,
                OutcomeError::Wait {
                    cause: Cause::RateLimit,
                    ..
                }
            ),
            "{err:?}"
        );
        let (cause, secs) = hold.unwrap();
        // 3. The message is sent again as itself, never consumed.
        let Again::Send { msg, delay } = lw::again(&queued(p), cause, secs, held_at) else {
            panic!("PR {} was dropped", p.pr);
        };
        assert_eq!(
            (msg.pr_number, msg.run_id.as_str()),
            (p.pr, p.run_id.as_str())
        );
        assert!(msg.rescore, "only scoring runs again, not the deploy watch");
        assert_eq!(msg.github_wait.waits, 1);
        assert!(msg.github_wait.until >= reset, "waits past the reset");
        assert_eq!(delay, gw::MAX_HOP_SECS);
        // It hops until the reset, then scores.
        let mut t = held_at;
        let mut hops = 0;
        while let Some(h) = lw::still_waiting(&msg, t) {
            assert!(h <= gw::MAX_HOP_SECS);
            t += h;
            hops += 1;
        }
        assert!(hops >= 5 && t >= reset, "{hops} hops to {t}");
        // On the queue it is a `reward_outcome` with its wait.
        let raw =
            serde_json::to_value(crate::models::WorkerMessage::RewardOutcome(msg.clone())).unwrap();
        assert_eq!(raw["type"], "reward_outcome");
        waiting.push(msg);
    }
    // The lesson's question waits the same way.
    let lesson = LessonMessage {
        team_id: TEAM.into(),
        installation_id: INSTALL,
        run_id: closes[0].run_id.clone(),
        repo_owner: OWNER.into(),
        repo_name: REPO.into(),
        pr_number: closes[0].pr,
        trigger: "pr_closed".into(),
        ..Default::default()
    };
    let (cause, secs) = github::outside_wait(&*held_read(closes[0].pr)).unwrap();
    assert!(matches!(
        lw::again(&lesson, cause, secs, held_at),
        Again::Send { .. }
    ));
    // A wait that never ends stops, said, never silently.
    let mut spent = waiting[0].clone();
    spent.github_wait.waits = gw::MAX_LIMIT_WAITS;
    assert!(matches!(
        lw::again(&spent, Cause::RateLimit, 60, held_at),
        Again::Stop(_)
    ));

    // 4. The card while it waits: retrying, with when; nothing for a person.
    let mut recorded = closes.clone();
    for (p, m) in recorded.iter_mut().zip(&waiting) {
        p.retry.retry_at = rfc(m.github_wait.until);
    }
    let page_at = at("2026-11-16T18:00:00Z");
    let l = health::loops(
        &health::Inputs {
            closed_prs: &recorded,
            ..Default::default()
        },
        page_at,
    );
    let card = l.iter().find(|x| x.key == "pr_outcomes").unwrap();
    assert_eq!(card.state, State::Retrying, "{card:?}");
    assert_eq!((card.count, card.expected), (0, Some(4)));
    assert!(
        card.detail.contains("retrying the other 4 automatically"),
        "{}",
        card.detail
    );
    assert!(
        card.detail.contains("next try at 18:25 UTC"),
        "{}",
        card.detail
    );
    assert!(card
        .detail
        .contains(&format!("{OWNER}/{REPO} #41, #42, #43, #44")));
    assert!(
        !card.detail.contains(OLD) && !card.check.contains(OLD_CHECK),
        "{card:?}"
    );
    let page = health::build(
        &health::Inputs {
            closed_prs: &recorded,
            ..Default::default()
        },
        page_at,
    );
    assert_eq!(page["state"], "ok");
    assert_eq!(page["retrying"], json!(["pr_outcomes"]));

    // 5. Had the messages been lost anyway, the sweep finds all four by the
    // page's own rule, after deploy, inside the week.
    let after_deploy = at("2026-11-21T12:00:00Z");
    let missing = unscored::missing(&closes, std::iter::empty(), after_deploy);
    assert_eq!(missing.iter().map(|p| p.pr).collect::<Vec<_>>(), PRS);
    let mut report = SweepReport::default();
    let steps = lw::plan(
        TEAM,
        &missing,
        after_deploy,
        lw::SWEEP_PER_TICK,
        |_| true,
        &mut report,
    );
    let sent: Vec<u64> = steps
        .iter()
        .map(|s| match s {
            Step::Send(m) => {
                assert!(m.rescore && m.installation_id == INSTALL && m.team_id == TEAM);
                m.pr_number
            }
            Step::Stop { .. } => panic!("nothing stops on the first try"),
        })
        .collect();
    assert_eq!(sent, PRS);
    // A try in flight is left alone; a budget with no room sends nothing.
    let mut report = SweepReport::default();
    assert!(lw::plan(
        TEAM,
        &unscored::missing(&recorded, std::iter::empty(), page_at),
        page_at,
        5,
        |_| true,
        &mut report
    )
    .is_empty());
    assert_eq!(report.in_flight, 4);
    let mut report = SweepReport::default();
    assert!(lw::plan(TEAM, &missing, after_deploy, 5, |_| false, &mut report).is_empty());
    assert_eq!(report.held, 4);
    // A few per tick.
    let mut report = SweepReport::default();
    assert_eq!(
        lw::plan(TEAM, &missing, after_deploy, 2, |_| true, &mut report).len(),
        2
    );
    // Bounded: after the last try it stops and says why on the run.
    let mut worn = closes.clone();
    for p in &mut worn {
        p.retry.tries = unscored::MAX_TRIES;
        p.retry.error = "HTTP status client error (404 Not Found)".into();
    }
    let mut report = SweepReport::default();
    let steps = lw::plan(
        TEAM,
        &unscored::missing(&worn, std::iter::empty(), after_deploy),
        after_deploy,
        5,
        |_| true,
        &mut report,
    );
    assert!(steps
        .iter()
        .all(|s| matches!(s, Step::Stop { why, .. } if why.contains("404"))));
    assert_eq!(steps.len(), 4);

    // 6. Scoring: the sweep's message and a late one both score; each pull
    // request's close is kept exactly once. A write the table refused keeps
    // nothing, so the next try keeps it.
    let log = MemoryLog::default();
    log.failing.store(1, std::sync::atomic::Ordering::SeqCst);
    for p in &closes {
        let events = closed_events(p);
        let first = add_all_once(&log, &events).await;
        if p.pr == PRS[0] {
            assert!(first.is_err(), "the refused write is an error, not a score");
            assert!(add_all_once(&log, &events).await.unwrap() > 0);
        } else {
            assert!(first.unwrap() > 0);
        }
        assert_eq!(
            add_all_once(&log, &events).await.unwrap(),
            0,
            "a second score adds nothing"
        );
    }
    let kept = log.events();
    for pr in PRS {
        let closes_kept = kept
            .iter()
            .filter(|e| {
                e.pr == pr && e.agent == AGENT_CODER && e.kind == Some(RewardKind::PrClosedUnmerged)
            })
            .count();
        assert_eq!(closes_kept, 1, "PR {pr}");
    }

    // 7. Scored: the card is green and the sweep has nothing left.
    let l = health::loops(
        &health::Inputs {
            events: &kept,
            closed_prs: &recorded,
            ..Default::default()
        },
        after_deploy,
    );
    let card = l.iter().find(|x| x.key == "pr_outcomes").unwrap();
    assert_eq!(
        (card.state, card.count, card.expected),
        (State::Ok, 4, Some(4))
    );
    assert!(unscored::missing(&closes, &kept, after_deploy).is_empty());

    // A close GitHub's event never brought is the one case that asks a
    // person to check the GitHub App.
    let mut unheard = closes.clone();
    unheard[0].retry.from_sweep = true;
    let l = health::loops(
        &health::Inputs {
            closed_prs: &unheard,
            ..Default::default()
        },
        after_deploy,
    );
    let card = l.iter().find(|x| x.key == "pr_outcomes").unwrap();
    assert_eq!(card.state, State::Quiet);
    assert!(card.check.contains(OLD_CHECK), "{}", card.check);
    github_budget::drain_of(INSTALL);
}

/// The pipeline still takes these paths: the worker's handlers wait and
/// settle, the sweep runs on the tick, and scoring keeps each signal once.
#[test]
fn the_handlers_wait_and_the_sweep_runs_on_the_tick() {
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::learning_wait::on_outcome(&state, msg)"));
    assert!(main.contains("passes::learning_wait::on_lesson(&state, msg)"));
    assert!(main.contains("passes::learning_wait::sweep(&state)"));
    assert!(!main.contains("error!(\"Reward outcome failed: {e}\")"));
    let rewards = include_str!("../rewards.rs");
    let score = &rewards[rewards.find("pub async fn score_outcome(").unwrap()..];
    let score = &score[..score.find("\n}\n").unwrap()];
    assert!(score.contains("budget_wait(") && score.contains("add_all_once("));
    assert!(!score.contains("record_all("));
    let reconcile = include_str!("../pr_outcomes.rs");
    assert!(reconcile.contains("learning_wait::settle_outcome("));
    assert!(reconcile.contains("unscored::OUTCOME_FROM"));
    let lessons = include_str!("../lessons.rs");
    assert!(lessons.contains("learning_wait::budget_wait("));
    assert!(lessons.contains("REMOVE close_signal, close_asked_at"));
}
