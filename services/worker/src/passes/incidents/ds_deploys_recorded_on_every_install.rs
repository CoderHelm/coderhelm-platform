//! ds. A made-up marina's Insights line of deploys CoderHelm started went
//! flat and stayed flat for weeks. Its install reached
//! GitHub through CoderHelm's token broker, so its gateway had no GitHub App
//! key of its own: asking GitHub for the app's slug failed, and every
//! completed workflow run was dropped before it reached the worker, without
//! a line in any log. The one-time backfill had read the history up to the
//! day the line stopped and was marked done, so nothing ever read the hole back, and nothing
//! said the line had stopped.
//!
//! Guarantee: a gateway that can't name the app's bot still sends every
//! run a bot triggered, and the worker decides with its own bot. A team that
//! records deploys most days and records none for two days while its bots'
//! runs keep arriving is stalled: the Learning health page turns red and the
//! worker's log says `deploy_records_stalled` (an alarm). Any such hole is
//! read back from GitHub's run history once, from each repository's newest
//! record before it, without touching the team's backfill marker.

use chrono::{DateTime, Duration, TimeZone, Utc};
use common::deploy_stats::{self as ds, health};
use common::rewards::health::State;
use serde_json::json;

const BOT: &str = "coderhelm[bot]";

fn at(d: u32, h: u32, m: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 3, d, h, m, 0).unwrap()
}

/// A preview deploy of the bot's push, as the webhook brings it.
fn preview_run() -> serde_json::Value {
    json!({
        "id": 12345678901u64,
        "name": "Deploy Preview",
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "triggering_actor": {"login": BOT, "type": "Bot"},
        "actor": {"login": BOT, "type": "Bot"},
    })
}

/// Thirty to seventy deploys a day across the marina's repositories, every
/// day for the three weeks to the last record (the 6th at 21:26 in this
/// made-up month).
fn records() -> Vec<(DateTime<Utc>, String)> {
    let last = at(6, 21, 26);
    let mut out = Vec::new();
    let mut t = last - Duration::days(21);
    let mut i = 0usize;
    while t <= last {
        let repo = ["marina/web", "marina/api", "marina/docks"][i % 3];
        out.push((t, repo.to_string()));
        t += Duration::minutes(29);
        i += 1;
    }
    out.push((last, "marina/web".to_string()));
    out.sort();
    out
}

#[test]
fn a_gateway_without_the_apps_key_still_sends_coderhelms_runs() {
    let run = preview_run();
    // The broken path: the bot unknown, and the run dropped.
    assert!(ds::worth_reading(&run, None), "the run reaches the worker");
    assert!(ds::worth_reading(&run, Some(BOT)));
    assert!(ds::may_be_ours(&run, BOT), "the worker reads it as its own");
    // A person's run still never goes.
    let mut person = run.clone();
    person["triggering_actor"] = json!({"login": "harbormaster", "type": "User"});
    person["actor"] = person["triggering_actor"].clone();
    assert!(!ds::worth_reading(&person, None));
}

#[test]
fn the_gateway_asks_no_app_key_before_queuing_a_run() {
    let src = include_str!("../../../../gateway/src/routes/analytics.rs");
    let f = &src[src.find("pub async fn queue_deploy_run(").unwrap()..];
    let f = &f[..f.find("\n}\n").unwrap()];
    assert!(
        !f.contains("let Some(slug) = crate::auth::github_app::app_slug(state).await else"),
        "an unknown slug no longer drops the run"
    );
    assert!(f.contains("note_bot_run(state, team_id, run).await"));
    assert!(f.contains("deploy_run_worth_reading(run, bot.as_deref())"));
    let bot = &src[src.find("async fn app_bot_login(").unwrap()..];
    assert!(
        bot.contains("common::hybrid::client::bot_login()"),
        "the broker's bot when the gateway can't ask GitHub"
    );
}

#[test]
fn seventeen_quiet_days_read_as_stalled_and_the_hole_is_read_back() {
    let records = records();
    let times: Vec<DateTime<Utc>> = records.iter().map(|(t, _)| *t).collect();
    let now = at(23, 18, 0);
    // The bots kept working: dozens of runs since the last record.
    match health::read(&times, 220, now) {
        health::Reading::Stalled { since, .. } => assert_eq!(since, at(6, 21, 26)),
        r => panic!("not stalled: {r:?}"),
    }
    let l = health::loop_of(
        "stalled",
        "2030-03-06T21:26:00Z",
        "2030-03-23T18:00:00Z",
        220,
        None,
    );
    assert_eq!(l.state, State::Quiet, "the Learning health page is red");
    // The hole, from where recording stopped, read once.
    let gap = health::first_gap(&times, now, None).expect("a hole");
    assert_eq!(gap, at(6, 21, 26));
    assert_eq!(health::first_gap(&times, now, Some(gap)), None);
    // Each repository from its own newest record before the hole.
    let repos: Vec<String> = ["marina/web", "Marina/API", "marina/docks", "marina/new"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let starts = health::repo_starts(&records, &repos, gap, now - Duration::days(182));
    assert_eq!(starts["marina/web"], at(6, 21, 26));
    assert!(starts["marina/api"] <= gap && starts["marina/api"] > gap - Duration::hours(2));
    assert_eq!(starts["marina/new"], gap);
    // Recording came back after the fix: the hole is still found.
    let mut back = times.clone();
    back.push(at(24, 9, 0));
    assert_eq!(health::first_gap(&back, at(24, 10, 0), None), Some(gap));
}

#[test]
fn the_sweep_checks_health_and_a_gap_fill_keeps_the_marker() {
    let src = include_str!("../deploy_stats.rs");
    let sweep = &src[src.find("pub async fn sweep(").unwrap()..];
    assert!(sweep.contains("check_health(state, &team_id, &item).await"));
    let check = &src[src.find("async fn check_health(").unwrap()..];
    assert!(check.contains("\"deploy_records_stalled: "));
    assert!(check.contains("ds::health::first_gap(&times, now, filled_from)"));
    assert!(check.contains("open_gap_fill(state, team_id, gap, &records, now)"));
    // A finished backfill runs again only for a hole, and never rewrites
    // the marker (from when deploys are known).
    let backfill = &src[src.find("pub async fn backfill(").unwrap()..];
    assert!(backfill.contains("gap_open(read(ds::BACKFILL_PROGRESS_SK).await?.as_ref())"));
    let pass = &src[src.find("async fn backfill_pass(").unwrap()..];
    let gap_end = pass.find("if gap {").expect("the gap pass ends");
    let marker = pass
        .find(".item(\"sk\", av(ds::BACKFILL_MARKER_SK))")
        .unwrap();
    assert!(
        gap_end < marker,
        "a gap fill finishes before the marker write"
    );
    // The alarm on the worker's log.
    let infra = include_str!("../../../../../infra/lib/monitoring-stack.ts");
    assert!(infra.contains("FilterPattern.literal('\"deploy_records_stalled\"')"));
}
