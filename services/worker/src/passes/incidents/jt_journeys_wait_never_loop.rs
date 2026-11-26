//! jt. A release that changed the map's extractor retraced every journey,
//! and earlier rebuilds that day had already spent most of the day's
//! tracing budget. Each pass then found that not one entry fitted what was
//! left, traced nothing, cost nothing, and queued the next pass a minute
//! later, while the watchdog each pass also queued piled up beside it: the
//! step ran every half minute for minutes with no progress. Then GitHub
//! refused one repository's tarball once (a 403), and the whole trace gave
//! up and wrote every journey from the untraced candidates, so the
//! journeys through the new screens were mostly missing.
//!
//! Guarantee: a repository GitHub won't give is tried again with backoff
//! within the pass; if it still fails, only the journeys that need it wait
//! (named on the status in a plain line) and every other journey is
//! traced; the waiting ones are tried again on a later pass, after GitHub's
//! reset when it was a rate limit. A pass that can't go on (the budget
//! spent, or every entry waiting on a repository) schedules exactly one
//! resume, at the budget's reset or the retry time, and nothing sooner:
//! every other message is dropped. A release that changed only the
//! extractor keeps every journey and traces none (`flows.mjs` `prepare`,
//! measured in `flows.test.mjs`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::passes::app_map_trace as tr;
use serde_json::{json, Value};

use super::fixture::Ending;

/// The day the incident is replayed on (UTC), at 15:00.
const NOW: i64 = 1_800_000_000 / 86_400 * 86_400 + 15 * 3_600;

fn repo_of() -> BTreeMap<String, String> {
    [
        ("harbor-api", "harbor/harbor-api"),
        ("harbor-etl", "harbor/harbor-etl"),
        ("harbor-web", "harbor/harbor-web"),
    ]
    .into_iter()
    .map(|(f, r)| (f.to_string(), r.to_string()))
    .collect()
}

/// A todo of `web` entries through the site and the API, and `etl` entries
/// whose journeys also read the data jobs.
fn todo(web: usize, etl: usize) -> Value {
    let mut entries = Vec::new();
    for i in 0..web {
        entries.push(json!({"id": format!("S{i}"), "key": format!("screen:web:/w{i}"), "hash": "h", "why": "new",
            "files": [format!("harbor-web/app/w{i}/page.tsx"), "harbor-api/src/app.controller.ts"]}));
    }
    for i in 0..etl {
        entries.push(json!({"id": format!("E{i}"), "key": format!("screen:web:/e{i}"), "hash": "h", "why": "new",
            "files": [format!("harbor-web/app/e{i}/page.tsx"), "harbor-etl/jobs/export.py"]}));
    }
    json!({"entries": entries, "table": {}, "heads": {"harbor/harbor-api": "a1", "harbor/harbor-etl": "b1", "harbor/harbor-web": "c1"}})
}

fn forbidden() -> Box<dyn std::error::Error + Send + Sync> {
    "HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/harbor/harbor-etl/tarball/b1)".into()
}

/// The journeys step's scheduling, replayed on a fake queue and clock with
/// the step's own decisions (`take_message`, `reschedule`, `next_resume`):
/// the day's budget nearly spent, the old step's pile of messages still
/// queued, until the next day's reset. Returns the passes run (when) and
/// what was said.
fn replay_spent_budget(day_spent: f64, queued: &[(i64, u32)]) -> (Vec<i64>, Ending) {
    let mut queue: Vec<(i64, u32)> = queued.to_vec();
    let mut next = Value::Null;
    let mut passes = Vec::new();
    let mut end = Ending::default();
    let until = tr::budget_reset(NOW) + 60;
    while let Some(i) = (0..queue.len()).min_by_key(|i| queue[*i].0) {
        let (now, turn) = queue.remove(i);
        if now > until {
            break;
        }
        match tr::take_message(turn, &next, now) {
            tr::Take::Stale => continue,
            tr::Take::Hop(secs) => {
                queue.push((now + secs, turn));
                continue;
            }
            tr::Take::Run => {}
        }
        passes.push(now);
        // The pass: its watchdog first, then the budget.
        let watch = tr::reschedule(&next, now + tr::MAX_DELAY_SECS);
        queue.push((now + tr::MAX_DELAY_SECS, tr::turn_of(&watch)));
        let spent = if now >= tr::budget_reset(NOW) {
            0.0
        } else {
            day_spent
        };
        let budget = tr::pass_budget(spent);
        let progressed = tr::can_start(budget);
        if !progressed {
            end.said("App map: no journey can go on now; one resume is queued for when one can");
        }
        let p = tr::PassEnd {
            progressed,
            by_time: false,
            runnable: if progressed { 0 } else { 40 },
            retry_at: Vec::new(),
            day_spent: spent,
        };
        let at = tr::next_resume(&p, now);
        next = tr::reschedule(&watch, at.unwrap_or(i64::MAX));
        if let Some(at) = at {
            queue.push((
                now + (at - now).clamp(0, tr::MAX_DELAY_SECS),
                tr::turn_of(&next),
            ));
        }
    }
    (passes, end)
}

#[test]
fn a_spent_budget_queues_one_resume_at_the_reset_and_nothing_sooner() {
    // The day's budget has less left than one entry costs; the old step left
    // a pile of messages (a watchdog and a next pass per pass) behind.
    let pile: Vec<(i64, u32)> = (0..12).map(|i| (NOW + i * 25, 0)).collect();
    let (passes, end) = replay_spent_budget(tr::DAILY_BUDGET_USD - 0.1, &pile);
    let reset = tr::budget_reset(NOW);
    let before: Vec<&i64> = passes.iter().filter(|t| **t < reset).collect();
    assert_eq!(
        before.len(),
        1,
        "one pass before the reset, then it waits: {passes:?}"
    );
    assert!(
        passes.iter().any(|t| *t >= reset && *t < reset + 60),
        "it resumes at the reset: {passes:?}"
    );
    // No pass follows one that made no progress within the idle guard.
    for w in passes.windows(2) {
        assert!(w[1] - w[0] >= tr::IDLE_SECS, "{passes:?}");
    }
    assert!(end.says("one resume is queued"));
    end.never_says("another step is tracing the journeys");
    // The pass's own decision: nothing sooner than the reset.
    let p = tr::PassEnd {
        runnable: 40,
        day_spent: tr::DAILY_BUDGET_USD - 0.1,
        ..Default::default()
    };
    assert_eq!(tr::next_resume(&p, NOW), Some(reset));
    // The pipeline still decides it there.
    let src = include_str!("../app_map.rs");
    let pass = super::fixture::fn_src(src, "async fn flows_pass(");
    assert!(pass.contains("tr::next_resume(&end, now)"));
    assert!(pass.contains("tr::can_start(budget)"));
    let step = super::fixture::fn_src(src, "async fn flows_step(");
    assert!(step.contains("tr::take_message(turn, &next, now)"));
}

#[tokio::test]
async fn a_403_on_one_repository_traces_every_other_journey_and_retries_it_later() {
    let tried = AtomicUsize::new(0);
    let slept = std::sync::Mutex::new(Vec::new());
    // GitHub refuses the data jobs' tarball on every try in this pass.
    let read = tr::read_repo(
        || {
            tried.fetch_add(1, Ordering::Relaxed);
            async { Err::<Vec<u8>, _>(forbidden()) }
        },
        |s| {
            slept.lock().unwrap().push(s);
            async {}
        },
        &tr::READ_RETRY_SECS,
    )
    .await;
    assert_eq!(
        tried.load(Ordering::Relaxed),
        1 + tr::READ_RETRY_SECS.len(),
        "tried again with backoff within the pass"
    );
    assert_eq!(*slept.lock().unwrap(), tr::READ_RETRY_SECS.to_vec());
    let fail = read.unwrap_err();
    assert!(matches!(fail, tr::RepoFail::Failed { .. }));
    let mut unread = json!({});
    tr::note_unread(&mut unread, "harbor/harbor-etl", &fail, NOW);
    // Only the journeys through it wait; the other ten are traced.
    let t = todo(10, 4);
    let (ready, waiting) = tr::split_ready(&t, &repo_of(), &unread, NOW);
    assert_eq!(ready.len(), 10);
    assert_eq!(waiting.len(), 4);
    assert!(waiting
        .iter()
        .all(|(e, r)| r == "harbor/harbor-etl" && e["id"].as_str().unwrap().starts_with('E')));
    // The status names it in one plain line.
    let line = common::app_map::unread_line(&["harbor/harbor-etl".to_string()]);
    let mut end = Ending::default();
    end.said(line.clone());
    assert!(line.starts_with("harbor-etl couldn't be read"), "{line}");
    end.never_says("the journeys couldn't be traced; the candidates are used");
    // The ten traced, the four wait: one resume at the retry time.
    let until = unread["harbor/harbor-etl"]["until"].as_i64().unwrap();
    assert_eq!(until, NOW + tr::REPO_RETRY_SECS);
    let p = tr::PassEnd {
        progressed: true,
        runnable: 0,
        retry_at: vec![until],
        day_spent: 2.0,
        ..Default::default()
    };
    assert_eq!(tr::next_resume(&p, NOW), Some(until));
    // Before it, the repository isn't asked again; after it, it is, and once
    // it reads the four are traced and the status clears.
    assert!(!tr::readable_now(&unread, "harbor/harbor-etl", until - 1));
    assert!(tr::readable_now(&unread, "harbor/harbor-etl", until));
    let ok = tr::read_repo(
        || async { Ok::<_, Box<dyn std::error::Error + Send + Sync>>(vec![1u8]) },
        |_| async {},
        &tr::READ_RETRY_SECS,
    )
    .await;
    assert!(ok.is_ok());
    unread.as_object_mut().unwrap().remove("harbor/harbor-etl");
    let (ready, waiting) = tr::split_ready(&todo(0, 4), &repo_of(), &unread, until);
    assert_eq!((ready.len(), waiting.len()), (4, 0));
    assert_eq!(common::app_map::unread_line(&[]), "");
    // The pipeline reads each repository this way and traces what is ready.
    let src = include_str!("../app_map.rs");
    let reads = super::fixture::fn_src(src, "async fn trace_journeys(");
    assert!(reads.contains("tr::read_repo("));
    assert!(reads.contains("tr::split_ready(todo, &repo_of, &unread"));
    assert!(
        !reads.contains(".map_err(|e| format!(\"{repo} couldn't be read: {e}\"))?"),
        "one repository no longer fails the trace"
    );
}
