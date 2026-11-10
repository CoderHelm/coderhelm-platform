//! x. A new-feature run finished and handed its draft pull request over for
//! review. The review job started ("Reviewing PR from label") and ended two
//! seconds later with no log line, no verdict, no review record and no live
//! status: a hand-off of a draft was skipped ("reviewed when it is marked
//! ready") on a path that said nothing, and its ending was dropped because
//! nothing had marked the job queued. The ready gate meanwhile kept the
//! draft until CoderHelm's review approved the head. Each waited on the
//! other, and the pull request was held on every sweep, naming nothing a
//! person could do, although the repository's CI was green.
//!
//! The four items that held it were CoderHelm's own sandbox failing: the
//! sandbox switched Node to the repository's version, that Node had no
//! pnpm, and the install, type-check, lint and unit steps each exited 127
//! with `pnpm: command not found`. Each was counted against the change, and
//! the run spent a fix round on it.
//!
//! A second pull request the same night lost its review the same way: a push
//! review CoderHelm skipped as its own push during the run kept the head's
//! claim and its dedup key, so the run's own review was dropped as a repeat
//! and the hand-off as "already being reviewed".
//!
//! Guarantees:
//! - Every review job ends in exactly one visible outcome: a verdict, a skip
//!   with its reason, a retry or a failure. Each ending is one
//!   `review_job_ended` log line and a live status, which is created when no
//!   earlier step wrote one; every early return names its reason.
//! - A run's hand-off reviews its pull request, draft or not. A head claim
//!   held by another review makes it wait for that review and look again,
//!   never skip; a push review skipped as CoderHelm's own push gives its
//!   claim and its dedup key back.
//! - When the draft decision needs CoderHelm's approval on the head and no
//!   review of the head exists, the gate queues one (once per head) and
//!   waits ("CoderHelm is reviewing this head") instead of holding; the
//!   verdict asks for the decision again. A reviewer that is off says so.
//! - A sandbox step that exited 127 or printed a tooling error, and every
//!   step after such an install, is CoderHelm's tooling, never a failure of
//!   the change. The sandbox makes the root's package manager available
//!   before the steps run.

use common::e2e::test_package::Step;

use super::fixture::fn_src;
use crate::passes::feature::{step_results, FeatureHold, StepResult};
use crate::passes::ready::{self, Ci, Decision, Facts};
use crate::passes::review_pr::{keeps_job_claim, Outcome};

/// What the gate said on every sweep.
const OLD_HOLD: &str = "CoderHelm's review hasn't approved this head";

/// The four sandbox lines the run recorded on the head.
fn sandbox_hold() -> FeatureHold {
    FeatureHold {
        sandbox_failed: vec![
            "install failed: `pnpm install --frozen-lockfile`".into(),
            "typecheck failed: `pnpm run check-types`".into(),
            "lint failed: `pnpm run lint`".into(),
            "unit failed: `pnpm run test`".into(),
        ],
        checklist_fails: vec![],
    }
}

/// The pull request as the sweep found it: a draft, the repository's CI
/// green, CoderHelm's own sandbox lines on the head, no review of the head.
fn held_facts() -> Facts {
    Facts {
        draft: true,
        title: "Add a size setting to the page blocks".into(),
        feature: Some(sandbox_hold()),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        approved: false,
        review_missing: true,
        head: "eeeb43e1913dfaef6930eaf02c6523af3ae4d26c".into(),
        ..Default::default()
    }
}

fn says(d: &Decision) -> String {
    match d {
        Decision::Held { reasons } | Decision::Proving { reasons, .. } => reasons
            .iter()
            .map(|r| r.line())
            .collect::<Vec<_>>()
            .join("\n"),
        Decision::Wait { why, .. } => why.clone(),
        Decision::Ready { open } => open.join("\n"),
        Decision::Closed => String::new(),
    }
}

#[test]
fn a_missing_review_is_queued_and_waited_for_never_held_on() {
    let f = held_facts();
    assert!(ready::wants_review(&f), "the gate asks for the review");
    let d = ready::decide(&f);
    assert!(
        matches!(&d, Decision::Wait { why, .. } if why == ready::REVIEWING_WHY),
        "{d:?}"
    );
    assert!(!says(&d).contains(OLD_HOLD));

    // The review approves: ready, with CoderHelm's own items listed.
    let approved = Facts {
        approved: true,
        review_missing: false,
        ..held_facts()
    };
    assert!(!ready::wants_review(&approved));
    assert!(matches!(ready::decide(&approved), Decision::Ready { open } if open.len() == 4));

    // CI not green yet: still held on CI, and the review is asked for now,
    // not after CI.
    let ci_red = Facts {
        ci: Ci {
            green: false,
            why: "the unit job failed".into(),
        },
        ..held_facts()
    };
    assert!(ready::wants_review(&ci_red));
    let d = ready::decide(&ci_red);
    assert!(matches!(d, Decision::Held { .. }));
    assert!(
        says(&d).contains("CoderHelm is reviewing this head"),
        "{}",
        says(&d)
    );
    assert!(!says(&d).contains(OLD_HOLD));
}

#[test]
fn a_review_that_never_comes_is_named() {
    // The reviewer is off: no review is queued, and the hold says why no
    // approval comes.
    let off = Facts {
        reviewer_off: true,
        ..held_facts()
    };
    assert!(!ready::wants_review(&off));
    let d = ready::decide(&off);
    assert!(matches!(d, Decision::Held { .. }));
    assert!(
        says(&d).contains("reviewer is off for this repository"),
        "{}",
        says(&d)
    );

    // A review that requested changes on the head is a hold, as before.
    let changes = Facts {
        review_missing: false,
        ..held_facts()
    };
    assert!(!ready::wants_review(&changes));
    assert!(says(&ready::decide(&changes)).contains(OLD_HOLD));

    // A person's draft and a closed pull request are never reviewed for it.
    let put_back = Facts {
        put_back_by: Some("dev".into()),
        ..held_facts()
    };
    assert!(!ready::wants_review(&put_back));
    let closed = Facts {
        closed: true,
        ..held_facts()
    };
    assert!(!ready::wants_review(&closed));
}

#[test]
fn the_hand_off_reviews_a_draft_and_waits_out_a_claim() {
    let src = include_str!("../review_pr.rs");
    let once = fn_src(src, "\nasync fn run_once(");
    assert!(!once.contains("NotReviewed::Draft"));
    assert!(!once.contains("reviewed when it is marked ready"));
    // A held head claim: the hand-off waits for that review, then looks
    // again; it is never skipped as "already being reviewed".
    let handoff = &once[once
        .find("HANDOFF_TRIGGER && !head_sha.is_empty()")
        .unwrap()..];
    let handoff = &handoff[..handoff.find("live_job.reviewing(").unwrap()];
    assert!(handoff.contains("deferred_ask(&msg)"));
    assert!(handoff.contains("enqueue_review(state, &later"));
    assert!(!handoff.contains("\"This commit is already being reviewed.\""));
    // A push skipped as CoderHelm's own gives the head's claim back.
    let own = &once[once.find("own_loop_hold(state").unwrap()..];
    let own = &own[..own.find("return Ok(Outcome::Skipped").unwrap()];
    assert!(own.contains("common::claim::release("));
    // The hand-off shows queued, and its verdict asks for the decision.
    let handoff_fn = fn_src(src, "\npub(crate) async fn review_on_handoff(");
    assert!(handoff_fn.contains("note_queued("));
    let run = fn_src(src, "\npub async fn run(");
    assert!(run.contains("super::ready::request(state, &msg)"));
    // The gate's ask goes through the same hand-off rules, once per head.
    let ask = fn_src(src, "\npub(crate) async fn review_for_ready(");
    assert!(ask.contains("HANDOFF_TRIGGER") && ask.contains("READYREVIEW#"));
    let settle = fn_src(include_str!("../ready.rs"), "\npub async fn settle(");
    assert!(settle.contains("ask_for_review(ctx, &mut facts)"));
}

#[test]
fn a_skipped_job_gives_its_dedup_key_back() {
    let skipped: Result<Outcome, ()> = Ok(Outcome::Skipped {
        reason: "Not reviewed now: CoderHelm pushed this while its run is still fixing it.".into(),
    });
    assert!(!keeps_job_claim(&skipped));
    let done: Result<Outcome, ()> = Ok(Outcome::Done {
        posted: Some("APPROVE"),
        run_review: None,
    });
    assert!(keeps_job_claim(&done));
    let failed: Result<Outcome, &str> = Err("boom");
    assert!(!keeps_job_claim(&failed));
    // The run's own review gives a skipped job's key back too.
    let src = include_str!("../review_pr.rs");
    let for_run = fn_src(src, "\npub async fn review_for_run(");
    let skip = &for_run[for_run.find("Ok(Ok(Outcome::Skipped").unwrap()..];
    assert!(skip[..200].contains("settle_job_claim(state, &msg, false)"));
}

/// The guard: a job can't end silently. Every early `return Ok(DONE)` in a
/// review job names why it posted nothing, and every ending goes through
/// `LiveJob::finished`, which logs it and records it.
#[test]
fn no_review_job_ends_silently() {
    let src = include_str!("../review_pr.rs");
    let once = fn_src(src, "\nasync fn run_once(");
    let mut checked = 0;
    let mut at = 0;
    while let Some(i) = once[at..].find("return Ok(DONE);") {
        let end = at + i;
        let start = once[..end].rfind("return ").map(|j| j + 7).unwrap_or(0);
        let path = &once[start..end];
        assert!(
            [".not_posted(", ".settle(", ".answered("]
                .iter()
                .any(|m| path.contains(m)),
            "a silent return in run_once: {}",
            &path[path.len().saturating_sub(400)..]
        );
        checked += 1;
        at = end + 1;
    }
    assert!(checked >= 6, "{checked}");
    // Skips always carry a reason.
    assert!(!once.contains("Outcome::Skipped {}"));
    // One line and one live status per ending, created when none exists.
    let finished = fn_src(src, "\n    async fn finished(");
    assert!(finished.contains("\"review_job_ended\""));
    assert!(finished.contains("mark_finished("));
    assert!(common::review_live::FINISH_CONDITION.starts_with("attribute_not_exists(sk) OR "));
    // Every arm of a job's run ends through it.
    let run = fn_src(src, "\npub async fn run(");
    assert!(run.matches("job.finished(").count() >= 3);
    assert!(run.contains("review_failed("));
}

/// The sandbox log of the run, as CodeBuild printed it.
const SANDBOX_LOG: &str = "===CODERHELM_CHECKS_START===\nv24.22.2\n\
--- install failed (exit 127) ---\nbash: line 1: pnpm: command not found\n\n\
--- typecheck failed (exit 127) ---\nbash: line 2: pnpm: command not found\n\n\
--- lint failed (exit 127) ---\nbash: line 3: pnpm: command not found\n\n\
--- unit failed (exit 127) ---\nbash: line 4: pnpm: command not found\n\n\
CODERHELM_TEST_STEP 1 127\nCODERHELM_TEST_STEP 2 127\nCODERHELM_TEST_STEP 3 127\n\
CODERHELM_TEST_STEP 4 127\n===CODERHELM_CHECKS_END exit=1===\n";

fn steps() -> Vec<Step> {
    [
        ("install", "pnpm install --frozen-lockfile"),
        ("typecheck", "pnpm run check-types"),
        ("lint", "pnpm run lint"),
        ("unit", "pnpm run test"),
    ]
    .into_iter()
    .map(|(kind, cmd)| Step {
        kind,
        cmd: cmd.into(),
    })
    .collect()
}

#[test]
fn coderhelms_own_sandbox_failing_never_counts_against_the_change() {
    let codes = common::e2e::test_package::step_codes(SANDBOX_LOG, 4);
    let results = step_results(&steps(), &codes, SANDBOX_LOG);
    assert_eq!(results.len(), 4);
    for r in &results {
        assert!(
            matches!(r, StepResult::Tooling(why) if why.starts_with(common::stop_rules::tooling::PREFIX)),
            "{r:?}"
        );
    }
    // After an install that couldn't run, a step that failed on its own
    // words is still CoderHelm's tooling: nothing it needs was installed.
    let log = SANDBOX_LOG.replace(
        "bash: line 2: pnpm: command not found",
        "src/a.ts(1,1): error TS2307",
    );
    let codes = vec![Some(127), Some(2), Some(0), Some(0)];
    let r = step_results(&steps(), &codes, &log);
    assert!(matches!(&r[1], StepResult::Tooling(why) if why.contains("install before it")));
    // A real failure after a good install still counts.
    let codes = vec![Some(0), Some(2), Some(0), Some(0)];
    let r = step_results(
        &steps(),
        &codes,
        "--- typecheck failed (exit 2) ---\nsrc/a.ts(1,1): error TS2322\n",
    );
    assert_eq!(r[1], StepResult::Fail(2));

    // The sandbox makes the root's manager available first.
    let sandbox = fn_src(
        include_str!("../feature/mod.rs"),
        "\nasync fn sandbox_items(",
    );
    assert!(sandbox.contains("test_package::ensure_manager(checklist::manager(tree))"));
    assert!(sandbox.contains("step_results(&kept, &codes, &o.output)"));
}
