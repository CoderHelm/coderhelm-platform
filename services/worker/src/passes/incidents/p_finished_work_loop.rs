//! p. A small ticket finished all 11 tasks, then spent 2.5 hours and $41.96
//! with no pull request. Each 15-minute step resumed at the checks before the
//! pull request and re-ran them from nothing. Their round counters were locals,
//! so every step got new rounds; every fix was a commit, so the stall detector
//! saw progress; and the pull request came last, with whatever time was left.
//!
//! Guarantee: the draft pull request opens as soon as the work is built, before
//! the checks, which then can't hold it back. A check runs once on a head and
//! keeps its rounds for the whole run. And whatever any pass does, a run with no
//! real progress ends one way: a draft with its Open items, waiting on a person.

use crate::passes::proof::{self, Finding, Next};
use crate::passes::run_guard::*;
use crate::passes::steps::Gate;

/// What that run cost, and the steps it took.
const INCIDENT_USD: f64 = 41.96;
const INCIDENT_CONTINUATIONS: usize = 11;

fn gaps(n: usize) -> Vec<Finding> {
    (0..n)
        .map(|i| Finding {
            point: "assumption".into(),
            detail: format!("gap {i}"),
            ..Default::default()
        })
        .collect()
}

/// The run's recorded shape: every step `plan, implement, review, implement,
/// review`, every second one with `pr` after it, and the proof check's count in
/// each going 2, 4, 3, 1 and round again. The spend per step is the run's total
/// over its steps: only the total was recorded.
struct Replayed {
    /// Where the guard stopped it, if it did.
    trip: Option<Trip>,
    continuations: usize,
    passes: usize,
    usd: f64,
}

fn replay(work_done: bool) -> Replayed {
    let per_step = INCIDENT_USD / (INCIDENT_CONTINUATIONS as f64);
    let counts = [2, 4, 3, 1];
    let mut g = GuardState::default();
    if work_done {
        g.apply(&Progress::TasksDone(11));
        g.apply(&Progress::WorkDone);
    }
    let (mut passes, mut usd) = (0, 0.0);
    for step in 0..INCIDENT_CONTINUATIONS {
        let mut seq = vec!["plan", "implement", "review", "implement", "review"];
        if step % 2 == 1 {
            seq.push("pr");
        }
        let per_pass = per_step / seq.len() as f64;
        for pass in &seq {
            g.on_pass(per_pass);
            passes += 1;
            usd += per_pass;
            if *pass == "review" {
                g.apply(&Progress::Findings("proof", counts[step % counts.len()]));
            }
            if let Some(trip) = g.trip() {
                return Replayed {
                    trip: Some(trip),
                    continuations: step,
                    passes,
                    usd,
                };
            }
        }
        g.on_continuation(0.0);
        if let Some(trip) = g.trip() {
            return Replayed {
                trip: Some(trip),
                continuations: step + 1,
                passes,
                usd,
            };
        }
    }
    Replayed {
        trip: None,
        continuations: INCIDENT_CONTINUATIONS,
        passes,
        usd,
    }
}

/// With nothing else changed, the guard alone stops that run at its fourth
/// continuation: 22 passes and about $15.26, 36% of what it spent.
#[test]
fn the_recorded_run_stops_at_its_fourth_continuation() {
    let r = replay(true);
    assert_eq!(r.trip, Some(Trip::ContinuationsAfterWork));
    assert_eq!(r.continuations, 4);
    assert_eq!(r.passes, 22);
    assert!((r.usd - 15.26).abs() < 0.01, "{}", r.usd);
    assert!(r.usd < INCIDENT_USD / 2.0);
}

/// Even if the run never reported its tasks done, it doesn't go the distance.
#[test]
fn the_recorded_run_stops_without_any_word_that_its_tasks_are_done() {
    let r = replay(false);
    assert!(r.trip.is_some());
    // The count of 1 is a new low and counts once. The spend since then stops it
    // at $20, in the ninth step, at about $33.57 in all.
    assert_eq!(r.trip, Some(Trip::Spend));
    assert_eq!(r.continuations, 8);
    assert!((r.usd - 33.57).abs() < 0.01, "{}", r.usd);
    assert!(r.usd < INCIDENT_USD, "{}", r.usd);
}

/// The spend counts by itself: a run that burns money in few passes and never
/// continues stops at the limit, and no pass or continuation brings it down.
#[test]
fn spend_alone_stops_a_run_well_before_that_runs_total() {
    let mut g = GuardState::default();
    g.apply(&Progress::WorkDone);
    g.apply(&Progress::PrOpen);
    g.apply(&Progress::Findings("proof", 3));
    let mut usd = 0.0;
    while g.trip().is_none() {
        g.on_pass(1.0);
        usd += 1.0;
        assert!(!g.apply(&Progress::Findings("proof", 3)));
        assert!(!g.apply(&Progress::WorkDone));
        assert!(!g.apply(&Progress::PrOpen));
        assert!(usd < INCIDENT_USD);
    }
    assert_eq!(g.trip(), Some(Trip::Spend));
    assert!(usd <= MAX_SPEND_WITHOUT_PROGRESS_USD + 1.0);
}

/// Across every continuation of that shape the proof check takes its two
/// rounds at most, stored across steps as it really is, and a head it has seen
/// is never judged again.
#[test]
fn a_continuation_does_not_reset_the_rounds_or_recheck_a_head() {
    let mut stored = Ledger::default().to_json_bounded();
    let mut judged = 0;
    let mut rounds_run = 0;
    let counts = [2, 4, 3, 1];
    // A fix that changes nothing leaves the head where it is.
    let mut head = 1;
    for step in 0..INCIDENT_CONTINUATIONS {
        let mut l: Ledger = serde_json::from_str(&stored).unwrap();
        l.on_step_start();
        let at = format!("sha{head}");
        let open = counts[step % counts.len()];
        if l.proof.unchanged(&at) {
            l.proof.reused();
        } else {
            judged += 1;
            l.proof.checked(&at, open);
        }
        if l.proof.may_fix(MAX_PROOF_FIX_ROUNDS) && l.proof.open > 0 {
            l.proof
                .start_fix(crate::passes::stop_judge::CycleLicence::for_test());
            rounds_run += 1;
            head += 1;
            l.proof.end_fix();
        }
        stored = l.to_json_bounded();
    }
    let l: Ledger = serde_json::from_str(&stored).unwrap();
    assert!(rounds_run <= MAX_PROOF_FIX_ROUNDS);
    assert_eq!(l.proof.rounds, rounds_run);
    assert!(judged <= rounds_run as usize + 1, "judged {judged}");
}

/// The counts 2, 4: the round made things worse, and the fixing stops there.
#[test]
fn no_progress_between_rounds_stops_the_fixing_at_once() {
    let mut r = CheckRecord::default();
    r.checked("a", 2);
    assert!(r.may_fix(MAX_PROOF_FIX_ROUNDS));
    r.start_fix(crate::passes::stop_judge::CycleLicence::for_test());
    r.end_fix();
    r.checked("b", 4);
    assert!(!r.may_fix(MAX_PROOF_FIX_ROUNDS));
    // What was found is still reported: the gaps go under Open items.
    let found = gaps(4);
    assert_eq!(
        proof::next(Some(&found), r.rounds, r.may_fix(MAX_PROOF_FIX_ROUNDS)),
        Next::OpenWithGaps
    );
    let open = proof::gaps_section(&found);
    for i in 0..4 {
        assert!(open.contains(&format!("gap {i}")), "{open}");
    }
}

/// A step cut off in a fix round: the next one fixes nothing more.
#[test]
fn time_running_out_during_fixes_leaves_only_the_description_to_do() {
    let mut l = Ledger {
        opened: Some(OpenedPr {
            number: 7,
            ..Default::default()
        }),
        ..Default::default()
    };
    l.rules.checked("a", 1);
    l.rules
        .start_fix(crate::passes::stop_judge::CycleLicence::for_test());
    let mut next: Ledger = serde_json::from_str(&l.to_json_bounded()).unwrap();
    next.on_step_start();
    assert_eq!(next.opened.as_ref().unwrap().number, 7, "no second PR");
    assert!(!next.rules.may_fix(MAX_RULES_FIX_ROUNDS));
    assert!(!next.proof.may_fix(MAX_PROOF_FIX_ROUNDS));
    // The rounds don't start again in the step after that either.
    let mut after: Ledger = serde_json::from_str(&next.to_json_bounded()).unwrap();
    after.on_step_start();
    assert!(!after.rules.may_fix(MAX_RULES_FIX_ROUNDS));
}

fn fn_of<'a>(src: &'a str, start: &str, end: &str) -> &'a str {
    let at = src.find(start).unwrap_or_else(|| panic!("no {start}"));
    let rest = &src[at..];
    &rest[..rest.find(end).unwrap_or_else(|| panic!("no {end}"))]
}

/// The order: the pull request, then every check, on both paths. The checks'
/// errors become Open items, never the run's error.
#[test]
fn the_pull_request_opens_before_every_check() {
    let src = include_str!("../mod.rs");
    let single = fn_of(src, "\nasync fn run_passes(", "\nasync fn record_open_pr(");
    let multi = fn_of(
        src,
        "\nasync fn run_repo_pipeline(",
        "\nasync fn resume_run_to_running(",
    );
    for (name, body) in [("single", single), ("multi", multi)] {
        let pr = body
            .find("pr::run(\n")
            .unwrap_or_else(|| panic!("{name}: no pr::run"));
        for check in [
            "proof::prove(",
            "link_checks::gate(",
            "test_fix::gate(",
            "pr::apply_checks(",
        ] {
            let at = body
                .find(check)
                .unwrap_or_else(|| panic!("{name}: no {check}"));
            assert!(pr < at, "{name}: {check} runs before the PR");
        }
        assert!(!body.contains("blocked the pull request"), "{name}");
        assert!(
            body.contains("proof::check_item(\"The link and redirect check\""),
            "{name}"
        );
        assert!(
            body.contains("proof::check_item(\"The failing-test rules\""),
            "{name}"
        );
    }
    let rules = single.find("feature::gate(").unwrap();
    assert!(single.find("pr::run(\n").unwrap() < rules);
    assert!(single.contains("proof::check_item(\"The new-feature rules\""));
    // A resume after the pull request never opens or describes it again.
    assert!(single.contains("let pr_result = if let Some(o) = opened {"));
    assert!(multi.contains("let pr_result = if let Some(o) = already_open {"));
    // Gates only go forward: the PR, then the checks.
    assert!(Gate::Pr < Gate::Checks);
    assert!(Gate::Generators < Gate::Pr);
}

/// Every check keeps its result by head, and an open-ended fix round is gone.
#[test]
fn every_check_runs_once_on_a_head_and_stops_at_the_deadline() {
    for (name, src) in [
        ("links", include_str!("../link_checks.rs")),
        ("test_fix", include_str!("../test_fix.rs")),
        ("rules", include_str!("../feature/mod.rs")),
    ] {
        let gate = fn_of(src, "\npub async fn gate(", "\nasync fn check_and_fix(");
        assert!(gate.contains(".unchanged(&head)"), "{name}");
        assert!(gate.contains("return standing;"), "{name}");
        // A round beyond the first starts only with the stop judge's licence.
        assert!(src.contains("start_fix(licence)"), "{name}");
        assert!(src.contains("stop_judge::cycle_licence("), "{name}");
        assert!(src.contains("w.deadline,"), "{name}");
        assert!(src.contains("end_fix()"), "{name}");
    }
    let proof_src = include_str!("../proof.rs");
    let prove = fn_of(proof_src, "\npub async fn prove(", "\n#[cfg(test)]");
    assert!(prove.contains("l.proof.unchanged(&head)"));
    assert!(prove.contains("start_fix(lic)"));
    assert!(prove.contains("stop_judge::cycle_licence("));
    // Nothing counts rounds in a local any more.
    for (name, src) in [
        ("proof", proof_src),
        ("links", include_str!("../link_checks.rs")),
        ("test_fix", include_str!("../test_fix.rs")),
        ("rules", include_str!("../feature/mod.rs")),
    ] {
        assert!(!src.contains("let mut rounds = 0;"), "{name}");
    }
}

/// Everything goes through the guard: every pass, every continuation, every
/// code-writing round, and a queued step of a stopped run doesn't start.
#[test]
fn every_pass_continuation_and_round_goes_through_the_guard() {
    let src = include_str!("../mod.rs");
    let up = fn_of(
        src,
        "\npub(crate) async fn update_pass(",
        "\n/// Why Coderhelm stopped",
    );
    assert!(up.contains("run_guard::on_pass("));
    let cont = fn_of(
        src,
        "\nasync fn try_auto_continue(",
        "\npub(crate) const MAX_AUTO_FIX_PUSHES",
    );
    assert!(
        cont.find("run_guard::on_continuation(").unwrap() < cont.find(".send_message()").unwrap()
    );
    let imp = fn_of(
        include_str!("../implement.rs"),
        "\npub async fn run(",
        "let rules_block",
    );
    assert!(imp.contains("run_guard::check("));
    let back = fn_of(
        src,
        "\nasync fn resume_run_to_running(",
        "\n/// The reason a run that panicked",
    );
    assert!(back.contains("attribute_not_exists(guard_open)"));
    // Both ways a ticket step ends go to the one ending.
    let orch = fn_of(
        src,
        "\npub async fn orchestrate_ticket(",
        "\nfn run_origin_agent(",
    );
    assert_eq!(orch.matches("run_guard::end_if_stopped(").count(), 2);
    // So do a resume's and a feedback round's.
    let main = include_str!("../../main.rs");
    assert_eq!(main.matches("run_guard::settle_open_pr(").count(), 2);
    assert_eq!(main.matches("run_ceiling::worked(").count(), 1);
}

/// One row: a checker that finds something forever, in the passes it really
/// uses. `Rounds(n)` is held by the run ledger or an existing durable cap, and
/// the run ends when they are spent; otherwise only the guard stops it.
#[derive(Clone, Copy)]
enum Bound {
    Rounds(u32),
    GuardOnly,
}

struct Checker {
    name: &'static str,
    /// The file the loop lives in, and what proves it is covered there.
    source: &'static str,
    covered_by: &'static str,
    /// It runs before the pull request exists.
    before_pr: bool,
    passes: &'static [&'static str],
    usd_per_cycle: f64,
    /// A cycle ends with a continuation.
    continues: bool,
    bound: Bound,
}

fn checkers() -> Vec<Checker> {
    let fix = &["implement", "review"][..];
    vec![
        Checker {
            name: "proof check",
            source: include_str!("../proof.rs"),
            covered_by: "update_pass(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 4.0,
            continues: true,
            bound: Bound::Rounds(MAX_PROOF_FIX_ROUNDS),
        },
        Checker {
            name: "new-feature rules",
            source: include_str!("../feature/mod.rs"),
            covered_by: "update_pass(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 4.0,
            continues: true,
            bound: Bound::Rounds(MAX_RULES_FIX_ROUNDS),
        },
        Checker {
            name: "link and redirect check",
            source: include_str!("../link_checks.rs"),
            covered_by: "update_pass(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 3.0,
            continues: true,
            bound: Bound::Rounds(MAX_LINK_FIX_ROUNDS),
        },
        Checker {
            name: "failing-test rules",
            source: include_str!("../test_fix.rs"),
            covered_by: "update_pass(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 4.0,
            continues: true,
            bound: Bound::Rounds(MAX_TEST_FIX_ROUNDS),
        },
        Checker {
            name: "plan check, tasks never done",
            source: include_str!("../plan_coverage.rs"),
            covered_by: "update_pass(",
            before_pr: true,
            passes: &["implement"],
            usd_per_cycle: 4.0,
            continues: true,
            bound: Bound::GuardOnly,
        },
        Checker {
            name: "generator check",
            source: include_str!("../generators.rs"),
            covered_by: "update_pass(",
            before_pr: true,
            passes: &["implement"],
            usd_per_cycle: 2.0,
            continues: true,
            bound: Bound::GuardOnly,
        },
        Checker {
            name: "test and security audit",
            source: include_str!("../mod.rs"),
            covered_by: "update_pass(state, &msg.team_id, run_id, \"security\")",
            before_pr: true,
            passes: &["test", "implement", "security"],
            usd_per_cycle: 4.0,
            continues: true,
            bound: Bound::GuardOnly,
        },
        Checker {
            name: "CI fix loop",
            source: include_str!("../resume.rs"),
            covered_by: "claim_auto_fix_slot(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 2.0,
            continues: false,
            bound: Bound::Rounds(crate::passes::MAX_AUTO_FIX_PUSHES),
        },
        Checker {
            name: "feedback, CI only",
            source: include_str!("../feedback.rs"),
            covered_by: "run_guard::on_pass(",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 2.0,
            continues: false,
            bound: Bound::Rounds(crate::passes::MAX_AUTO_FIX_PUSHES),
        },
        Checker {
            name: "own review",
            source: include_str!("../own_review.rs"),
            covered_by: "MAX_FIX_ROUNDS",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 3.0,
            continues: false,
            bound: Bound::Rounds(crate::passes::own_review::MAX_FIX_ROUNDS),
        },
        Checker {
            name: "conflict round",
            source: include_str!("../resume.rs"),
            covered_by: "\"conflict_fix\"",
            before_pr: false,
            passes: &["conflict_fix", "review"],
            usd_per_cycle: 1.0,
            continues: false,
            bound: Bound::GuardOnly,
        },
        Checker {
            name: "planned work rounds",
            source: include_str!("../resume.rs"),
            covered_by: "update_pass(state, &msg.team_id, &msg.run_id, \"implement\")",
            before_pr: false,
            passes: fix,
            usd_per_cycle: 4.0,
            continues: false,
            bound: Bound::GuardOnly,
        },
        // What a pass nobody has written yet gets, only from `update_pass`.
        Checker {
            name: "a pass added later",
            source: include_str!("../mod.rs"),
            covered_by: "run_guard::on_pass(",
            before_pr: false,
            passes: &["some_new_pass"],
            usd_per_cycle: 0.5,
            continues: false,
            bound: Bound::GuardOnly,
        },
    ]
}

/// How a simulated run ended.
#[derive(Debug, Default)]
struct End {
    draft_prs: u32,
    waiting_on_a_person: bool,
    cycles: u32,
    usd: f64,
}

/// A run whose work is done (unless the checker holds it there) and whose
/// checker finds the same thing forever, driven through the real guard.
fn run_forever(c: &Checker) -> End {
    let mut g = GuardState::default();
    let mut end = End::default();
    let mut ledger = Ledger::default();
    // One draft, however the run reaches it: opening is idempotent per branch.
    let open = |end: &mut End, ledger: &mut Ledger| {
        if ledger.opened.is_none() {
            ledger.opened = Some(OpenedPr {
                number: 7,
                ..Default::default()
            });
            end.draft_prs += 1;
        }
    };
    if !c.before_pr {
        g.apply(&Progress::TasksDone(11));
        g.apply(&Progress::WorkDone);
        open(&mut end, &mut ledger);
        g.apply(&Progress::PrOpen);
    }
    g.apply(&Progress::Findings("checker", 5));
    for cycle in 0..1000 {
        if let Bound::Rounds(max) = c.bound {
            if cycle >= max {
                // Its cap ended the fixing: the run goes on with the draft open
                // and the findings listed. Nothing more is spent on this.
                open(&mut end, &mut ledger);
                return end;
            }
        }
        end.cycles += 1;
        for _ in c.passes {
            g.on_pass(c.usd_per_cycle / c.passes.len() as f64);
            assert!(!g.apply(&Progress::Findings("checker", 5)));
            if g.trip().is_some() {
                break;
            }
        }
        end.usd += c.usd_per_cycle;
        if g.trip().is_none() && c.continues {
            g.on_continuation(0.0);
        }
        if g.trip().is_some() {
            // The one ending.
            open(&mut end, &mut ledger);
            end.waiting_on_a_person = true;
            return end;
        }
    }
    panic!("{}: it never ended", c.name);
}

#[test]
fn a_checker_that_finds_something_forever_ends_with_one_draft_within_the_limits() {
    let table = checkers();
    assert!(table.len() >= 13);
    for c in &table {
        assert!(c.source.contains(c.covered_by), "{}: not covered", c.name);
        let end = run_forever(c);
        assert_eq!(end.draft_prs, 1, "{}: {end:?}", c.name);
        assert!(
            end.usd <= MAX_SPEND_WITHOUT_PROGRESS_USD + c.usd_per_cycle,
            "{}: spent {}",
            c.name,
            end.usd
        );
        assert!(end.usd < INCIDENT_USD, "{}", c.name);
        match c.bound {
            // Stopped by the guard: it says so and waits.
            Bound::GuardOnly => assert!(end.waiting_on_a_person, "{}: {end:?}", c.name),
            Bound::Rounds(max) => assert!(end.cycles <= max, "{}: {end:?}", c.name),
        }
    }
}

/// A big ticket's normal run, with every fix round allowed, doesn't trip it.
#[test]
fn a_normal_large_ticket_never_trips_the_guard() {
    let mut g = GuardState::default();
    let step = 3.5;
    // Triage and plan, then 30 tasks over eight steps of building.
    g.on_pass(1.0);
    g.on_pass(2.0);
    for s in 1..=8 {
        g.on_pass(step);
        assert!(g.apply(&Progress::TasksDone(s * 30 / 8)));
        assert_eq!(g.trip(), None, "building, step {s}");
        g.on_continuation(0.0);
        assert_eq!(g.trip(), None, "continuing, step {s}");
    }
    g.apply(&Progress::WorkDone);
    // Everything before the pull request, in a step that runs out of time.
    for _ in 0..worst_normal_run().min(7) {
        g.on_pass(0.5);
        assert_eq!(g.trip(), None);
    }
    g.on_continuation(0.0);
    assert_eq!(g.trip(), None);
    assert!(g.apply(&Progress::PrOpen));
    // Every round of every check, each one fixing something, over two steps.
    g.apply(&Progress::Findings("proof", 6));
    for open in [4, 2] {
        g.on_pass(1.5);
        g.on_pass(0.2);
        assert!(g.apply(&Progress::Findings("proof", open)));
    }
    g.on_continuation(0.0);
    assert_eq!(g.trip(), None);
    // Every automatic push CI is allowed, no green until the last.
    for push in 0..crate::passes::MAX_AUTO_FIX_PUSHES {
        g.on_pass(1.5);
        g.on_pass(0.2);
        assert_eq!(g.trip(), None, "push {push}");
    }
    assert!(g.apply(&Progress::CiGreen));
    // The own review's rounds, one fix after the other.
    for round in 0..crate::passes::own_review::MAX_FIX_ROUNDS {
        g.on_pass(2.0);
        g.on_pass(0.2);
        assert_eq!(g.trip(), None, "round {round}");
    }
}

#[test]
fn the_steps_are_ended_by_a_reason_a_person_can_act_on() {
    let r = replay(true);
    let g = GuardState {
        after_work: r.continuations as u32,
        ..Default::default()
    };
    let why = r.trip.unwrap().reason(&g);
    assert!(why.contains("handed it to you"), "{why}");
}
