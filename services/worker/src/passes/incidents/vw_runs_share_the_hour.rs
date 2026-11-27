//! vw. Three failures, all in a run's own accounting:
//!
//! - **Worked time summed overlaps.** Two messages of one run overlapped (a
//!   CI resume beside a review event, a wait for GitHub sent on while a step
//!   ran). The second found the first's "working" marker, took it for a
//!   killed step and charged a whole step, and then both invocations' times
//!   were added. A run could be stopped at the worked-time ceiling long before
//!   it had worked that long, and runs showed more worked time than their
//!   whole life.
//! - **A burst of runs met GitHub's hour together.** Many tickets started
//!   close together. Nothing admitted runs against the installation's shared
//!   budget, so every run spent until the share kept for people, then every
//!   run waited for the reset, and pages said "Waiting for GitHub's hourly
//!   limit" over and over.
//! - **A 5xx while building a revert was final.** A merge's deploy failed,
//!   CoderHelm began the revert pull request, GitHub answered one call with a
//!   server error, and the revert was reported as impossible. Nothing tried
//!   again.
//!
//! Guarantees: worked time is the union of a run's invocations, and only a
//! step older than any step lives is charged as killed, once, and recorded;
//! a run's next GitHub-heavy step starts when its learned spend fits the
//! installation's live budget after the steps running and the runs ahead of
//! it, otherwise it waits its turn in arrival order and its page says when it
//! starts; every GitHub call on the revert path is tried again while the
//! worker's one classifier says its error passes by itself, and still failing
//! that way the deploy watch looks again and builds it then.

use std::collections::{HashMap, HashSet};

use common::github_budget::{
    self as rules,
    admission::{self, Admit, Ledger},
    Budget, Spend,
};

use super::fixture::fn_src;
use crate::passes::run_ceiling::{Clock, KILLED_AFTER_SLACK_SECS, STEP_SECS};

const T0: u64 = 1_794_000_000;

// ---- Worked time ----------------------------------------------------------

#[test]
fn two_overlapping_invocations_count_their_shared_minutes_once() {
    let mut c = Clock::default();
    // A step of 600 s, and a second message of the run 100 s into it, also
    // 600 s.
    let a = c.enter("a", T0);
    let b = c.enter("b", T0 + 100);
    assert_eq!((a.killed, b.killed), (0, 0), "a live marker is not a kill");
    assert_eq!((a.charged, b.charged), (0, 0));
    let total = c.leave("a", T0, T0 + 600) + c.leave("b", T0 + 100, T0 + 700);
    // The old way: 600 + 600 and a 900 s "killed step" for the overlap.
    assert_eq!(total, 700, "the union of [0,600) and [100,700)");
    // Ending in the other order, and one inside the other, is the union too.
    let mut c = Clock::default();
    c.enter("a", T0);
    c.enter("b", T0 + 100);
    let total = c.leave("b", T0 + 100, T0 + 300) + c.leave("a", T0, T0 + 600);
    assert_eq!(total, 600);
    assert!(
        c.live.is_empty() && c.spans.is_empty(),
        "nothing kept: {c:?}"
    );
}

#[test]
fn only_a_step_older_than_any_step_lives_is_charged_as_killed_once() {
    let mut c = Clock::default();
    c.enter("killed", T0);
    // Still possibly running: no charge.
    let e = c.enter("next", T0 + STEP_SECS);
    assert_eq!((e.killed, e.charged), (0, 0));
    c.leave("next", T0 + STEP_SECS, T0 + STEP_SECS + 30);
    // Past any step's life: charged for all of its time, once, and
    // recorded as a kill; the overlap with what was counted is not counted
    // again.
    let later = T0 + STEP_SECS + KILLED_AFTER_SLACK_SECS;
    let e = c.enter("third", later);
    assert_eq!(e.killed, 1);
    assert_eq!(e.charged, STEP_SECS);
    let e = c.enter("fourth", later + 1);
    assert_eq!(e.killed, 0, "a kill is charged once");
    // Kept on the run as text, read back the same.
    let (live, spans) = c.to_attrs();
    assert_eq!(Clock::from_attrs(&live, &spans), c);
}

/// A run's day of overlapping messages: steps, CI resumes beside them,
/// waits sent on every few minutes while a step runs. Its worked time stays
/// within its life, which the old sum broke by hours.
#[test]
fn a_busy_run_never_works_longer_than_it_lived() {
    let mut c = Clock::default();
    let life = 5 * 3600 + 1_200;
    let mut total = 0;
    let mut old_sum = 0;
    let mut t = 0;
    let mut n = 0;
    while t + 900 <= life {
        // A step of 14 minutes, a resume of 3 beside it, and three waits of
        // a few seconds sent on while it runs.
        let msgs = [
            (t, 840),
            (t + 120, 180),
            (t + 300, 5),
            (t + 420, 5),
            (t + 600, 5),
        ];
        for (live, (start, _)) in msgs.into_iter().enumerate() {
            let id = format!("m{n}-{start}");
            if live > 0 {
                old_sum += 900;
            }
            c.enter(&id, T0 + start);
        }
        let mut ends: Vec<(u64, u64)> = msgs.iter().map(|(s, d)| (s + d, *s)).collect();
        ends.sort();
        for (end, start) in ends {
            total += c.leave(&format!("m{n}-{start}"), T0 + start, T0 + end);
            old_sum += end - start;
        }
        t += 900;
        n += 1;
    }
    assert!(total <= life, "{total} > {life}");
    assert!(old_sum > life, "the old sum was over the life: {old_sum}");
}

#[test]
fn the_worker_counts_worked_time_with_the_clock() {
    let src = include_str!("../run_ceiling.rs");
    let worked = fn_src(src, "pub async fn worked<");
    assert!(worked.contains("mark_entered(state, t, r, &id, started)"));
    assert!(worked.contains("mark_left(state, t, r, &id, started)"));
    let entered = fn_src(src, "async fn mark_entered(");
    assert!(entered.contains("c.enter(id, now)"), "{entered}");
    assert!(
        !entered.contains("STEP_SECS"),
        "no flat charge for a marker"
    );
    let write = fn_src(src, "async fn update_clock(");
    assert!(write.contains("ADD ceil_worked_secs :s, ceil_killed :k"));
    assert!(write.contains("ceil_ver = :v0"), "one writer at a time");
}

// ---- A burst of runs on one budget ---------------------------------------

/// What each step of a run costs in counted requests: made-up numbers with
/// a wide spread, a step reading many files the heaviest.
const COSTS: [u64; 6] = [30, 70, 45, 55, 20, 90];
const STEP_LEN: u64 = 300;
const STEPS: usize = 8;
const RUNS: usize = 15;

enum State {
    Pending { next: u64 },
    Running { started: u64, sent: u64, cost: u64 },
    Done,
}

struct Outcome {
    /// Requests the per-request rules held (the people's share or a spent
    /// limit): each is a "Waiting for GitHub's hourly limit".
    held: u64,
    /// Runs that entered the line, by run and hour.
    notes: HashMap<(usize, u64), u32>,
    finished: usize,
}

/// 15 runs starting within 30 minutes on one 5,000-an-hour installation,
/// each step's requests sent over its five minutes, through the real
/// per-request decision. `admitted`: through the real admission ledger,
/// with the cost per step learned from the steps that ended.
fn burst(admitted: bool) -> Outcome {
    let limit = 5_000;
    let mut budget = Budget {
        remaining: limit,
        limit,
        reset: T0 + 3_600,
    };
    let mut ledger = Ledger::default();
    let (mut sum, mut steps_seen) = (0u64, 0u64);
    let mut runs: Vec<(State, usize)> = (0..RUNS)
        .map(|i| {
            (
                State::Pending {
                    next: T0 + 120 * i as u64,
                },
                0,
            )
        })
        .collect();
    let mut out = Outcome {
        held: 0,
        notes: HashMap::new(),
        finished: 0,
    };
    let mut in_line: HashSet<usize> = HashSet::new();
    let mut noted: HashMap<usize, u64> = HashMap::new();
    for t in T0..T0 + 4 * 3_600 {
        if t >= budget.reset {
            budget.remaining = limit;
            budget.reset += 3_600;
        }
        for (i, (state, step)) in runs.iter_mut().enumerate() {
            let key = format!("run{i}");
            if let State::Pending { next } = *state {
                if next > t {
                    continue;
                }
                let go = if admitted {
                    let need = admission::cost(sum, steps_seen, limit);
                    match ledger.admit(&key, need, Some(budget), None, t, t * 1_000 + i as u64) {
                        Admit::Go => {
                            in_line.remove(&i);
                            true
                        }
                        Admit::Wait { start_at, .. } => {
                            // The page's note, as `github_admission::admit`
                            // writes it: entering the line, once an hour.
                            if in_line.insert(i)
                                && crate::passes::github_admission::notes_now(
                                    noted.get(&i).copied(),
                                    t,
                                )
                            {
                                noted.insert(i, t / 3_600);
                                *out.notes.entry((i, t / 3_600)).or_default() += 1;
                            }
                            *state = State::Pending {
                                next: t + crate::passes::github_admission::delay_for(start_at, t),
                            };
                            false
                        }
                    }
                } else {
                    true
                };
                if go {
                    *state = State::Running {
                        started: t,
                        sent: 0,
                        cost: COSTS[(i + *step) % COSTS.len()],
                    };
                }
            }
            if let State::Running {
                started,
                sent,
                cost,
            } = state
            {
                let due = (*cost * (t - *started) / STEP_LEN).min(*cost);
                while *sent < due {
                    match rules::decide(rules::caller::RUN, Some(budget), None, 0, t) {
                        Spend::Go => {
                            budget.remaining = budget.remaining.saturating_sub(1);
                            *sent += 1;
                        }
                        Spend::Hold { .. } => {
                            // The run waits for the reset, as it does today.
                            out.held += 1;
                            break;
                        }
                    }
                }
                if *sent < due {
                    ledger.release(&key);
                    *state = State::Pending { next: budget.reset };
                    continue;
                }
                if *sent == *cost && t - *started >= STEP_LEN {
                    sum += *cost;
                    steps_seen += 1;
                    ledger.release(&key);
                    *step += 1;
                    *state = if *step == STEPS {
                        out.finished += 1;
                        State::Done
                    } else {
                        State::Pending { next: t + 1 }
                    };
                }
            }
        }
    }
    out
}

#[test]
fn a_burst_of_runs_shares_one_budget_and_no_run_meets_the_limit() {
    // More than an hour's share: some steps must wait for the reset.
    let need: u64 = (0..RUNS)
        .flat_map(|i| (0..STEPS).map(move |s| COSTS[(i + s) % COSTS.len()]))
        .sum();
    assert!(need > 4_500, "{need}");
    let with = burst(true);
    assert_eq!(with.held, 0, "no request met the limit");
    assert_eq!(with.finished, RUNS, "every run finished");
    // The page says it waits once per run and hour, never over and over,
    // and only some runs wait at all.
    assert!(with.notes.values().all(|n| *n <= 1), "{:?}", with.notes);
    // Without admission the same burst meets the limit: the test has teeth.
    let without = burst(false);
    assert!(without.held > 0, "the old way meets the limit");
}

#[test]
fn the_line_keeps_arrival_order_and_says_when_a_run_starts() {
    let limit = 5_000;
    // 600 left: 100 of the runs' share after the people's 500.
    let budget = Some(Budget {
        remaining: 600,
        limit,
        reset: T0 + 1_200,
    });
    let mut l = Ledger::default();
    assert_eq!(l.admit("a", 80, budget, None, T0, 1), Admit::Go);
    let b = l.admit("b", 80, budget, None, T0, 2);
    assert_eq!(
        b,
        Admit::Wait {
            ahead: 0,
            start_at: T0 + admission::HOLD_SECS
        },
        "it starts when a's share is free"
    );
    let c = l.admit("c", 80, budget, None, T0 + 1, 3);
    assert!(matches!(c, Admit::Wait { ahead: 1, .. }), "{c:?}");
    // a ended: b, first in line, goes; c, behind it, still waits even with
    // room for one.
    l.release("a");
    assert!(matches!(
        l.admit("c", 80, budget, None, T0 + 2, 4),
        Admit::Wait { ahead: 1, .. }
    ));
    assert_eq!(l.admit("b", 80, budget, None, T0 + 2, 5), Admit::Go);
    // GitHub said the hour is spent: nobody starts before its reset.
    let spent = l.admit("c", 80, budget, Some(T0 + 900), T0 + 3, 6);
    assert!(
        matches!(spent, Admit::Wait { start_at, .. } if start_at >= T0 + 900),
        "{spent:?}"
    );
    // The page's words.
    assert_eq!(
        admission::waiting_text(1, T0 + 1_200),
        format!(
            "Waiting its turn for GitHub's hourly budget (1 run ahead); starts by itself at about {}",
            common::github_wait::clock(T0 + 1_200)
        )
    );
    // A step's cost is learned from the steps seen; nothing seen is the
    // first guess, never a cap.
    assert_eq!(admission::cost(0, 0, 5_000), 100);
    assert_eq!(admission::cost(510, 10, 5_000), 51);
    // Kept on the record as text, read back the same.
    let (held, line) = l.to_attrs();
    assert_eq!(Ledger::from_attrs(&held, &line), l);
}

#[test]
fn every_run_step_is_admitted_before_it_runs_and_its_time_counts() {
    let main = include_str!("../../main.rs");
    let admit = main
        .find("github_admission::admit(")
        .expect("the admission");
    let worked = main.find("run_ceiling::worked(").unwrap();
    let release = main.find("github_admission::release(").unwrap();
    assert!(admit < worked && worked < release);
    // What a step spends is learned where the counts are written.
    let budget = include_str!("../../clients/github_budget.rs");
    assert!(fn_src(budget, "pub async fn flush(").contains("learn_step_costs("));
}

// ---- The revert ----------------------------------------------------------

#[tokio::test]
async fn a_500_while_building_the_revert_is_tried_again_and_the_revert_opens() {
    use crate::passes::deploy_revert::{self as dr, Outcome};
    let pulls_500 = "HTTP status server error (500 Internal Server Error) for url \
                     (https://api.github.com/repos/northwind/shop/pulls)";
    assert!(dr::transient(
        &*Box::<dyn std::error::Error + Send + Sync>::from(pulls_500)
    ));
    // The classifier is the worker's one: the revert has no list of its own.
    let src = include_str!("../deploy_revert.rs");
    assert!(fn_src(src, "pub fn transient(").contains("common::run_recovery::classify("));
    let build = fn_src(src, "async fn build<");
    for call in [
        "find_open_pr_for_branch",
        "get_pull_request",
        "get_ref",
        "put_tree",
        "put_commit",
        "create_branch",
        "create_pull_request",
    ] {
        assert!(build.contains(&format!(".{call}(")), "{call}");
    }
    assert_eq!(
        build.matches("step(pause_ms,").count(),
        7,
        "every call goes through the classified retry"
    );
    // Still failing that way, the outcome is a retry on the next look.
    let health = include_str!("../health_check.rs");
    assert!(health.contains("matches!(outcome, super::deploy_revert::Outcome::Retry(_))"));
    assert!(health.contains("if again && revert_looks_again(now, merged_at)"));
    assert!(crate::passes::health_check::revert_looks_again(
        T0 as i64 + 60,
        T0 as i64
    ));
    assert!(!dr::report_line(&Outcome::Retry("GitHub API 500".into())).contains("could not"));
}
