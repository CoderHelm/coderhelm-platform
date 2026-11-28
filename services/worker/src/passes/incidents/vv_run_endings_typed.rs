//! vv. Three ways a run ended said something that wasn't so.
//!
//! - **Red CI read as an unknown error.** A run's checks stayed red after its
//!   fix rounds. The pass knew why it stopped (which checks, on which head)
//!   and wrote it as a sentence; the error classifier, built for HTTP
//!   errors, read the sentence back, found no shape it knew, and the run
//!   waited on a person as if after an unknown error it had already tried
//!   again. Nothing had tried again. The base branch's checks were read
//!   only when the round started: the base went red on the same deploy
//!   check during the round, and the run never saw it.
//! - **Rounds that never ran counted as stalls.** Planned-work rounds met
//!   the team's usage limit before their first model call. Each was counted
//!   as a round that made no progress, and enough of those stopped the work.
//! - **A content gap the change can't fill failed the run.** A test read
//!   preview content that wasn't there. The gap check matched a list of
//!   phrases the failure didn't use, and it ended the loop only when the
//!   change carried its own step to add the content. The round changed no
//!   code and the run failed.
//!
//! Guarantees:
//!
//! - Every status write goes through one gate (`common::run_ending`): done
//!   with what it delivered, waiting on someone named with the evidence, or
//!   stopped with a typed reason and what was tried. A test walks every
//!   status write in every service and refuses one that doesn't.
//! - A stop the pass knows the reason for is typed (`run_fail::Cause`) and
//!   never read by the error classifier, which reads only errors a pass
//!   raised. "Tried again" is written only from a recorded retry.
//! - Red CI at the ending names its checks, its head and the base, read
//!   again at the ending: red on the base too, the run finishes
//!   (`red_on_base`); else it stops `failed` with the checks named.
//! - A round the usage limit refused never counts as a stall: the run waits
//!   on the limit, the ending every run at the limit takes. A round that
//!   didn't finish runs again with the stalls it had.
//! - A round that changed no code and found the test's content missing ends
//!   the loop with that content named before merge, from the round's own
//!   verdict, whether or not the change has a step for it.

use super::fixture::fn_src;
use crate::passes::finish_planned::{self, Next, Ran};
use crate::passes::resume;
use common::run_ending::{RunEnd, Stop, Stopped, Tried};

fn set(names: &[&str]) -> std::collections::BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

const HEAD: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";

/// The base was green on the deploy check when the round started and red on
/// it at the ending: read again at the ending, the check is the base's and
/// the run finishes.
#[test]
fn red_ci_is_read_again_at_the_ending_and_red_on_the_base_finishes() {
    let head = set(&["Deploy Preview"]);
    let at_start = resume::red_of(HEAD, &head, "main", &set(&[]), &[], "BucketNotEmpty");
    assert!(
        !at_start.all_on_base(),
        "at the round's start it was this change's"
    );
    let at_end = resume::red_of(
        HEAD,
        &head,
        "main",
        &set(&["Deploy Preview", "Lint"]),
        &[],
        "BucketNotEmpty",
    );
    assert!(at_end.all_on_base());
    assert_eq!(at_end.base_red, vec!["Deploy Preview".to_string()]);
    // An end-to-end check red on the base names a check, not a test: never
    // called the base's here.
    let e2e = resume::red_of(
        HEAD,
        &set(&["E2E after deploy"]),
        "main",
        &set(&["E2E after deploy"]),
        &[],
        "",
    );
    assert!(!e2e.all_on_base());

    // Every red ending reads the base again before it stops, and finishes
    // when the base is red too.
    let src = include_str!("../resume.rs");
    for f in ["\nasync fn hand_off_red_ci(", "\nasync fn stop_red("] {
        let body = fn_src(src, f);
        let read = body
            .find("red_ci_now(")
            .expect("the base read at the ending");
        let done = body
            .find("finish_red_on_base(")
            .expect("red on the base finishes");
        assert!(read < done, "{f}");
    }
    let finish = fn_src(src, "\nasync fn finish_red_on_base(");
    assert!(finish.contains("GaveUp::RedOnBase"));
    assert!(finish.contains("common::run_status::COMPLETED"));
    // The red endings stop typed, never with prose for the classifier.
    for f in [
        "\nasync fn wait_on_person(",
        "\npub(crate) async fn end_red_ci_rounds(",
    ] {
        let body = fn_src(src, f);
        assert!(
            !body.contains("super::fail_run("),
            "{f} hands prose to the classifier"
        );
        assert!(body.contains("stop_red("), "{f}");
    }
    let stop = fn_src(src, "\nasync fn stop_red(");
    assert!(stop.contains("Stop::RedCi(red)"));
    assert!(!stop.contains("fail_run("));
}

/// A red-CI stop is written as itself: failed, its checks named, never
/// "doesn't know how to repair" and never "tried again".
#[test]
fn a_red_ci_stop_never_reads_as_an_unknown_error() {
    let red = resume::red_of(HEAD, &set(&["Deploy Preview"]), "main", &set(&[]), &[], "");
    let why = format!("{}. 2 CI fix rounds changed no code", red.reason());
    let f = common::run_fail::RunFailure {
        cause: common::run_fail::Cause::Stopped {
            reason: &why,
            stop: Stopped {
                reason: Stop::RedCi(red),
                tried: Tried {
                    rounds: 2,
                    ..Default::default()
                },
            },
        },
        ..Default::default()
    };
    let u = common::run_fail::update("R1", &f, "now");
    assert_eq!(u.status, common::run_status::FAILED);
    assert!(!u.resumable);
    let kind = u.values[":kind"].as_s().unwrap();
    assert_eq!(kind, "red_ci");
    let err = u.values[":err"].as_s().unwrap();
    assert!(
        err.contains("Deploy Preview") && err.contains("`a1b2c3d`"),
        "{err}"
    );
    assert!(err.contains("main is green on these checks"), "{err}");
    assert!(!err.contains("doesn't know how to repair"));
    assert!(!u.said.contains("tried again"));
    // Through the gate: the ending is on the record.
    assert!(u.expression.contains("#run_ending = :run_ending"));
}

/// Planned-work rounds the usage limit refused never stop the work as
/// rounds without progress: the run waits on the limit.
#[test]
fn rounds_the_usage_limit_refused_are_not_stalls() {
    let items = vec!["`apps/web/src/gallery.tsx` is not written.".to_string()];
    let section = crate::passes::proof::unfinished_section(&[crate::passes::proof::Finding {
        point: "planned_work".into(),
        detail: items[0].clone(),
        ..Default::default()
    }]);
    let now = chrono::Utc::now();
    let mut round = finish_planned::first_round(&section, now).unwrap();
    let limit = "Monthly cache read tokens limit reached (9.1M of 9M).".to_string();
    for _ in 0..2 {
        let next = finish_planned::next(
            &items,
            Some(&items),
            &Ran::Limited(limit.clone()),
            &[],
            &round,
        );
        assert_eq!(next, Next::WaitLimit(limit.clone()));
        assert!(!matches!(next, Next::Stop(_)));
        assert_eq!(
            finish_planned::next(&items, Some(&items), &Ran::Unfinished, &[], &round),
            Next::Again { stalled: 0 }
        );
        round = finish_planned::next_round(&round, &items, 0, &[], now);
    }
    // The limit ending is the one every run at the limit takes.
    let needs = common::run_recovery::usage_limit(&limit);
    assert_eq!(needs.why, common::handback::HumanOnly::UsageLimit);

    let src = include_str!("../resume.rs");
    let after = fn_src(src, "\nasync fn after_planned_round(");
    assert!(after.contains("RoundEnd::Parked | RoundEnd::Incomplete => Ran::Unfinished"));
    assert!(after.contains("RoundEnd::Limited(why) => Ran::Limited(why)"));
    assert!(after.contains("super::end_at_usage_limit("));
    // The limit is known from the refused call's kind, before the fault
    // decision runs the round again.
    let round_src = fn_src(src, "\nasync fn run_fix_round(");
    let limited = round_src
        .find("ModelErrorKind::LimitReached")
        .expect("the limit read from the call");
    let fault = round_src
        .find("Fix round met an error; taking the way around it")
        .expect("the fault decision");
    assert!(limited < fault);
}

/// A round that changed no code and found the content missing ends the
/// loop with the content named before merge, with no step of the change's
/// own and words no list holds.
#[test]
fn a_content_gap_with_no_step_ends_the_loop_from_the_rounds_verdict() {
    use common::content_gap::{self, Verdict};
    let failure =
        "Error: Absent preview content: /gallery needs a Tile block whose photo has a caption";
    let account = "I ran the test: the page renders the caption when the CMS has one.\n\
                   Cause: content: /gallery needs a Tile block whose photo has a caption in preview";
    match content_gap::after_round(account, failure, false, true, &[]) {
        Verdict::AwaitsStep { gap, step } => {
            assert!(gap.line.contains("Tile block whose photo has a caption"));
            assert!(step.contains("Add the content the test reads"), "{step}");
        }
        v => panic!("{v:?}"),
    }
    let src = include_str!("../resume.rs");
    let ci = &src[src
        .find("let mut ci_round = run_round::Round::start(")
        .unwrap()..];
    let gap = ci
        .find("content_after_round(")
        .expect("the round's verdict is read");
    let hand_off = ci
        .find("super::GaveUp::NoChange")
        .expect("the no-change hand-off");
    assert!(
        gap < hand_off,
        "the content verdict comes before the red hand-off"
    );
    assert!(src.contains("feedback.push_str(common::content_gap::CAUSE_ASK);"));
}

/// The gate refuses an ending without its evidence: never a done run
/// without its pull request.
#[test]
fn an_ending_without_evidence_is_never_written_as_claimed() {
    let w = common::run_ending::gate("R1", &common::run_ending::delivered("completed", 0, ""));
    assert_eq!(w.status(), common::run_status::FAILED);
    let w = common::run_ending::gate(
        "R1",
        &RunEnd::Stopped(Stopped {
            reason: Stop::Guard {
                why: "the run reached its ceiling".into(),
            },
            tried: Tried::default(),
        }),
    );
    assert_eq!(w.status(), common::run_status::FAILED);
    assert_eq!(w.sri(), format!("{}#R1", common::run_status::FAILED));
}
