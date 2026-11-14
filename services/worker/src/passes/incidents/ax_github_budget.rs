//! ax. An installation used up GitHub's hourly request budget. Sweeps on
//! timers had been added the same day (a look at every finished run's pull
//! request every 15 minutes, a read of every pull request a security
//! finding records, a description rebuilt after each push) and spent it
//! beside the runs. A review fix round that had worked for two hours then
//! met the limit: "The review fix round didn't land: GitHub didn't answer
//! (its changes couldn't be read (403 Forbidden) ... /compare/...)", the
//! next step read the branch, got the same 403 and logged `Resume failed:
//! RateLimited { wait_secs: 731 }`, the run read "Pull request open; the
//! last fix round didn't finish: the review fix round hit an error (403
//! Forbidden ... git/ref/heads/...)", and fourteen minutes later the sweep
//! deferred the review's findings to a person. The security reconcile, the
//! outcomes reconcile and the review reminders failed on the same limit,
//! one warning per item.
//!
//! Guarantee: a rate limit is a wait, everywhere. A resume that ends on it
//! is sent for the reset as the same run and the same round (no attempt
//! spent, nothing marked unfinished, no finding deferred), the run page
//! says "Waiting for GitHub's hourly limit; continues by itself at <time>",
//! and once the limit is met no caller sends another request until the
//! reset. Every request is counted under who made it; sweeps spend only
//! above the share kept for runs, each within its own hourly cap, and say a
//! skip once. A round a limit ended before this is run again by the sweep
//! once the budget allows.

use super::fixture::*;
use crate::clients::github::{self, RateLimited};
use crate::clients::github_budget::{self as ledger, SweepGate};
use crate::passes::pr_watch::{self, Act, Marks, Probe, Seen};
use crate::passes::round::{self, Fault};
use common::github_budget::{self as budget, caller as who, Hold, Spend};
use common::github_wait::{self as gw, Cause};
use common::run_round::{self, Kind};

const OLD_END: &str = "the last fix round didn't finish";
const OLD_DEFER: &str = "fix rounds on its own review's findings ended";
/// GitHub's reset, and when the round met the limit (731 seconds before).
const RESET: u64 = 1_793_999_100;
const HIT: u64 = RESET - 731;

/// The 403 as the client reports it for a read during the limit.
fn refused(path: &str) -> String {
    format!(
        "HTTP status client error (403 Forbidden) for url \
         (https://api.github.com/repos/acme/site/{path}): {{\"message\":\"API rate limit \
         exceeded for installation.\"}}"
    )
}

/// The exact sequence: the round can't read its changes for the limit, the
/// step after it can't read the branch, and the run waits for the reset as
/// the same round, then lands. Nothing is deferred.
#[test]
fn a_fix_round_that_meets_the_limit_waits_and_lands_after_the_reset() {
    let mut end = Ending {
        status: "awaiting_ci".into(),
        pr: Some(41),
        draft: false,
        ..Default::default()
    };
    let working = {
        let mut r = run_round::Round::start(
            Kind::ReviewFix,
            2,
            "base000",
            "Fix the two findings of CoderHelm's review",
            true,
            chrono::Utc::now(),
        );
        r.wip_branch = run_round::wip_branch("coderhelm/gh-41");
        r
    };

    // 1. The round's changes couldn't be read: GitHub's limit, so a wait
    //    (not "GitHub didn't answer ... runs it again in a fresh step").
    let unread = format!(
        "its changes couldn't be read ({})",
        refused("compare/base000...coderhelm-wip/coderhelm/gh-41")
    );
    assert_eq!(round::fault_of(&unread), Fault::GitHubBusy);
    let (cause, wait) =
        round::github_wait_of(&unread, Some((Cause::RateLimit, 731))).expect("a wait");
    assert_eq!((cause, wait), (Cause::RateLimit, 731));
    let parked = working.parked(false);
    assert_eq!(parked.attempt, working.attempt, "no attempt is spent");
    assert_eq!(parked.wip_branch, working.wip_branch, "its work is kept");

    // 2. The step after it reads the branch and gets the same 403, typed.
    //    It used to be `Resume failed: RateLimited` and "didn't finish".
    let err: Box<dyn std::error::Error + Send + Sync> = Box::new(RateLimited::for_test(731));
    let paused = crate::passes::github_pause_of(&*err, 0).expect("GitHub's limit is a pause");
    assert_eq!(paused.cause, Cause::RateLimit);
    // The same error with its type lost (a step that words its own error)
    // still reads as the limit.
    let worded: Box<dyn std::error::Error + Send + Sync> = format!(
        "the description couldn't be rebuilt: {}",
        refused("pulls/41")
    )
    .into();
    assert!(crate::passes::github_pause_of(&*worded, 0).is_some());
    assert!(gw::may_wait(Cause::RateLimit, 1));
    let until = gw::until(HIT, gw::paced(Cause::RateLimit, 731, 1));
    end.said(gw::waiting_text(Cause::RateLimit, until));
    assert_eq!(
        gw::waiting_text(Cause::RateLimit, RESET),
        "Waiting for GitHub's hourly limit; continues by itself at 21:05 UTC"
    );

    // 3. The resume rides to the reset in hops and runs the parked round.
    let (mut t, mut hops) = (HIT, 0);
    while let Some(h) = gw::hop(until, t) {
        assert!(h <= gw::MAX_HOP_SECS);
        t += h;
        hops += 1;
    }
    assert_eq!(hops, 1, "731 seconds is one delayed message");
    assert!(t >= RESET);
    let again = parked.resumed(chrono::Utc::now());
    assert_eq!(
        (again.kind, again.number, again.attempt),
        (Kind::ReviewFix, 2, 1)
    );
    assert_eq!(
        again.feedback, working.feedback,
        "the same findings, none deferred"
    );
    assert_eq!(again.wip_branch, working.wip_branch);

    // The pipeline: the worker asks for GitHub's wait before it calls a
    // resume failed or its round unfinished, and the wait parks the round
    // without an attempt and puts the run back to waiting.
    let main = include_str!("../../main.rs");
    let arm = &main[main
        .find("models::WorkerMessage::Resume(msg) => {")
        .unwrap()..];
    let waits = arm
        .find("passes::resume::wait_out_github(&state, &waiting, &*e).await")
        .expect("the resume arm waits for GitHub");
    assert!(waits < arm.find("error!(\"Resume failed").unwrap());
    assert!(waits < arm.find("passes::round::end_unfinished(").unwrap());
    let resume = include_str!("../resume.rs");
    let wait_fn = fn_src(resume, "pub(crate) async fn wait_out_github(");
    assert!(wait_fn.contains("super::github_pause_of(e, msg.installation_id)"));
    assert!(wait_fn.contains("send_github_wait(state, msg, paused.cause, wait)"));
    assert!(wait_fn.contains("release_for_wait("));
    let release = fn_src(resume, "async fn release_for_wait(");
    assert!(release.contains("&round.parked(false)"));
    assert!(release.contains("common::run_status::AWAITING_CI"));
    for gone in [
        "delete_branch",
        "finish_incomplete",
        "defer_findings",
        "end_unfinished",
    ] {
        assert!(!wait_fn.contains(gone) && !release.contains(gone), "{gone}");
    }
    // A limit already met (by this worker or another) is waited out before
    // the resume reads anything.
    let run = fn_src(resume, "pub async fn run(");
    let known = run
        .find("crate::clients::github_budget::limited_until(msg.installation_id, now)")
        .expect("the resume asks whether the limit is spent");
    assert!(
        known
            < run
                .find("load_unprocessed_events(state, &msg.run_id)")
                .unwrap()
    );
    assert!(
        known
            < run
                .find("run_pending_round(state, &msg, &run_record")
                .unwrap()
    );
    // The feedback round and the ticket run wait the same way.
    let feedback = include_str!("../feedback.rs");
    assert!(fn_src(feedback, "pub(crate) async fn wait_for_github(")
        .contains("super::github_pause_of(e, msg.installation_id)"));
    assert!(include_str!("../mod.rs")
        .contains("common::github_wait::may_wait(paused.cause, msg.github_wait.waits)"));

    assert!(end.says("Waiting for GitHub's hourly limit; continues by itself at 21:05 UTC"));
    end.never_says(OLD_END);
    end.never_says(OLD_DEFER);
    end.never_says("Resume");
    assert_eq!(end.status, "awaiting_ci");
}

/// Once the limit is met, nobody sends into it; before it is, the sweeps
/// stop at the share kept for runs and at their own caps.
#[tokio::test]
async fn one_budget_runs_first_sweeps_last() {
    let (id, now) = (930_001, 1_794_000_000);
    let left = |remaining| {
        github::record_budget(
            id,
            budget::Budget {
                remaining,
                limit: 5_000,
                reset: now + 1_800,
            },
        )
    };
    // 2,400 of 5,000 left: below the runs' share.
    left(2_400);
    for sweep in [
        who::SWEEP_PR_WATCH,
        who::SWEEP_SECURITY,
        who::SWEEP_PR_OUTCOMES,
        who::SWEEP_REVIEW_REMINDER,
        who::SWEEP_REVIEW,
        who::SWEEP_DRAFTS,
    ] {
        assert_eq!(
            ledger::standing(id, sweep, now),
            Spend::Hold {
                why: Hold::ReservedForRuns,
                wait_secs: 1_800
            },
            "{sweep}"
        );
        // One line for the installation, however many items it had.
        let mut gate = SweepGate::new(sweep);
        for _ in 0..40 {
            assert!(!gate.check(id, 1, now));
        }
        assert_eq!(gate.skipped(), 1);
    }
    for work in [who::RUN, who::FIX_ROUND, who::REVIEW] {
        assert_eq!(ledger::standing(id, work, now), Spend::Go, "{work}");
    }
    // With room, a sweep still stops at its own hour's cap: every sweep at
    // its cap leaves the runs half of the smallest budget.
    left(4_800);
    let cap = budget::hourly_cap(who::SWEEP_PR_WATCH).unwrap();
    ledger::scoped(who::SWEEP_PR_WATCH, async {
        for _ in 0..cap {
            assert_eq!(ledger::admit(id, now), Spend::Go);
            ledger::responded(id, 200, false, now);
        }
        assert!(matches!(
            ledger::admit(id, now),
            Spend::Hold {
                why: Hold::HourlyCap,
                ..
            }
        ));
    })
    .await;
    let all: u64 = who::BACKGROUND.iter().map(|(_, c)| c).sum();
    assert!(all <= budget::SMALLEST_LIMIT / 2);
    // The limit met: nothing leaves for anyone until the reset.
    ledger::limit_spent(id, now + 731, now);
    for anyone in [who::FIX_ROUND, who::RUN, who::SWEEP_PR_WATCH] {
        assert_eq!(
            ledger::standing(id, anyone, now),
            Spend::Hold {
                why: Hold::LimitSpent,
                wait_secs: 731
            }
        );
    }
    assert_eq!(ledger::standing(id, who::FIX_ROUND, now + 732), Spend::Go);
    ledger::drain_of(id);

    // A run waiting on CI polls every ten minutes, not two, while the
    // budget is below the runs' share; a resume due now is never delayed.
    let low = Some(budget::Budget {
        remaining: 2_400,
        limit: 5_000,
        reset: now + 1_800,
    });
    let poll = crate::passes::resume::poll_delay;
    assert_eq!(poll(120, low, now), 600);
    assert_eq!(poll(0, low, now), 0);
    assert_eq!(poll(900, low, now), 900);
    assert_eq!(poll(120, github::budget_of(id), now), 120, "room: as asked");
    assert_eq!(poll(120, None, now), 120);
    assert_eq!(poll(120, low, now + 1_801), 120, "past the reset");

    // Each sweep asks before each item, in its own loop.
    for (src, ask) in [
        (
            include_str!("../pr_watch.rs"),
            ".allows(state, c.installation_id, LOOK_REQUESTS)",
        ),
        (
            include_str!("../review_sweep.rs"),
            "!budget.allows(state, iid, 1).await",
        ),
        (
            include_str!("../review_reminder.rs"),
            "!budget.allows(state, iid, 1).await",
        ),
        (
            include_str!("../pr_outcomes.rs"),
            "!budget.allows(state, p.installation_id, 2).await",
        ),
        (
            include_str!("../ready.rs"),
            "!budget.allows(state, c.installation_id, need).await",
        ),
        (include_str!("../security_fixes.rs"), "|| self.gh.held()"),
        (
            include_str!("../security_release.rs"),
            "|| self.cx.gh.held()",
        ),
        (include_str!("../skills.rs"), "if !force && gh.held() {"),
    ] {
        assert!(src.contains(ask), "{ask}");
    }
    // A limit never writes a pull request off as unknown for good.
    let outcomes = include_str!("../pr_outcomes.rs");
    assert_eq!(
        outcomes
            .matches("Err(e) if crate::clients::github::outside_wait(&*e).is_some() => continue,")
            .count(),
        2
    );
    // Every message's requests are labelled, written and logged.
    let main = include_str!("../../main.rs");
    assert!(main.contains(
        "github_budget::scoped(\n        label,\n        Box::pin(handle_message_inner("
    ));
    assert!(main.contains("github_budget::flush(&state).await;"));
    for sweep in [
        "who::SWEEP_PR_WATCH",
        "who::SWEEP_SECURITY",
        "who::SWEEP_PR_OUTCOMES",
        "who::SWEEP_DRAFTS",
        "who::SWEEP_STALE_PRS",
    ] {
        let label = main.find(sweep).unwrap_or_else(|| panic!("{sweep}"));
        assert!(main[..label].trim_end().ends_with("as_sweep("), "{sweep}");
    }
}

/// A pull request nobody touched is looked at for free, and a round the
/// limit ended before is run again by the sweep, once.
#[test]
fn the_watch_costs_nothing_while_nothing_changed_and_picks_the_round_back_up() {
    let seen = Seen {
        open: true,
        head: "9f8e7d6".into(),
        base_sha: "bbbbbbb".into(),
        reviewer_on: true,
        own_verdict: Some(("REQUEST_CHANGES".into(), "9f8e7d6".into())),
        ..Default::default()
    };
    // What a look read is kept with each read's validator...
    let probe = Probe {
        validators: vec![(
            "https://api.github.com/repos/acme/site/pulls/41".into(),
            "\"abc\"".into(),
        )],
        seen: seen.clone(),
        human_approved: false,
    };
    let stored = probe.to_json();
    assert_eq!(Probe::parse(&stored), Some(probe));
    assert_eq!(Probe::parse(""), None);
    // ...and the next look asks only whether they changed.
    let look = fn_src(include_str!("../pr_watch.rs"), "pub async fn look(");
    let asked = look
        .find("github.unchanged(&p.validators).await")
        .expect("a conditional look");
    assert!(asked < look.find("read(state, github, c).await").unwrap());

    // The run the limit ended: "didn't finish", for a 403 on a read.
    let reason = run_round::errored(Kind::ReviewFix, &refused("git/ref/heads/coderhelm/gh-41"));
    assert!(round::ended_by_limit(&reason));
    let at = "2026-11-06T20:52:49+00:00";
    let none = Marks::default();
    assert_eq!(
        pr_watch::resume_after_limit("completed", &seen, &none, at),
        Some(Act::ResumeRound(at.into()))
    );
    // Before the findings on the head are taken or deferred again.
    assert_eq!(
        pr_watch::decide("completed", &seen, &none),
        Act::FixFindings
    );
    assert!(
        look.find("resume_after_limit(").unwrap()
            < look.find("decide(&c.status, &seen, &c.marks)").unwrap()
    );
    // Once per round: claimed against when that round stopped.
    let done = Marks {
        limit_at: at.into(),
        ..Default::default()
    };
    assert_eq!(
        pr_watch::resume_after_limit("completed", &seen, &done, at),
        None
    );
    assert_eq!(
        Act::ResumeRound(at.into()).claim(&seen),
        Some((Marks::LIMIT, at.to_string()))
    );
    // A round that stopped for anything else, a merged or closed pull
    // request, and a run still working are left alone.
    assert_eq!(
        pr_watch::resume_after_limit("completed", &seen, &none, ""),
        None
    );
    assert_eq!(
        pr_watch::resume_after_limit("running", &seen, &none, at),
        None
    );
    let closed = Seen {
        open: false,
        ..seen.clone()
    };
    assert_eq!(
        pr_watch::resume_after_limit("completed", &closed, &none, at),
        None
    );
    assert!(!round::ended_by_limit(&run_round::out_of_time(
        Kind::ReviewFix,
        3
    )));
    // The round is parked with what it kept and a resume is sent.
    let resume = include_str!("../resume.rs");
    let again = fn_src(resume, "pub(crate) async fn resume_limit_ended(");
    assert!(again.contains("i.resume_round(now)"));
    assert!(again.contains("send_resume_message(state, &msg, 0)"));
    assert!(again.contains("super::round::ended_by_limit(&inc.reason)"));
}
