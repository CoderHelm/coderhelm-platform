//! ag. After #646 three ways were left for a run to end badly over red
//! checks or a refused landing:
//!
//! - a dependency fix whose pull request's CI stayed red had its work taken
//!   apart by planned task (one task per lockfile: a lockfile put back
//!   without its manifest), and after three reverts the run ended failed
//!   with bumps still in the pull request that passed on their own;
//! - a run stopped after its work opened its pull request with the
//!   manifests it had changed and lockfiles nobody had written, and waited
//!   for CI to fail before the lockfiles were regenerated;
//! - a review fix or feedback round whose finished work GitHub refused to
//!   land was recorded as "didn't finish" at once, while a CI fix round
//!   with the same refusal ran again.
//!
//! Guarantee: red checks after every fix take out only what breaks them. A
//! dependency fix takes out its newest commit of verified bumps, defers
//! those bumps with what CI printed and keeps the commits before it, again
//! while CI stays red, never emptying the pull request. Any other run is
//! taken apart task by task down to the part that passes. What ships is
//! green with the rest under "Deferred". A stopped run's lockfiles are
//! written by their package managers before its pull request opens. Every
//! kind of fix round takes the same classified decision about a fault:
//! while it may continue it runs again, keeping its work when only the
//! landing failed.

use super::fixture::fn_src;
use crate::passes::generators::LockRepair;
use crate::passes::round::{self, AfterFault, Fault};
use crate::passes::supervisor::{self, Revert};
use crate::passes::{resume, stop_pr};
use common::dep_fix::{self, DepPlan, DepRun, Group, PlanBump};
use common::run_round::Kind;

fn bump(i: usize) -> PlanBump {
    PlanBump {
        package: format!("lib-{i:02}"),
        from: "2.1.0".into(),
        to: "2.1.5".into(),
        manifest: "pnpm-lock.yaml".into(),
        finding_ids: vec![format!("f{i}")],
        ..Default::default()
    }
}

/// A dependency run that committed three verified sets.
fn run_with_three_sets() -> DepRun {
    let plan = DepPlan {
        bumps: (0..6).map(bump).collect(),
        deferred: vec![],
    };
    let mut run = DepRun::new(plan, "base", 3.0);
    for (bumps, head) in [(vec![0, 1, 2], "h1"), (vec![3, 4], "h2"), (vec![5], "h3")] {
        run.groups = vec![Group {
            bumps,
            green: true,
            ..Default::default()
        }];
        run.on_commit(0, head);
    }
    run
}

#[test]
fn a_dependency_fix_with_red_ci_defers_only_the_set_that_breaks_it() {
    let mut run = run_with_three_sets();
    assert_eq!(run.applied.len(), 6);
    let failure = "npm error ETARGET No matching version found for lib-05@2.1.5";
    let why = dep_fix::ci_deferral_reason(failure);

    // CI is red after every fix: the newest set comes out, five bumps stay.
    let out = run.take_out_last_commit(&why).expect("a set to take out");
    assert_eq!(
        out.restore_to, "h2",
        "the branch goes back to the set before"
    );
    assert_eq!(run.applied, vec![0, 1, 2, 3, 4]);
    let note = resume::dep_revert_note(&out.lines, run.commits.len());
    assert!(
        note.contains("took out the newest verified set of 1 bump"),
        "{note}"
    );
    assert!(note.contains("kept the 2 sets before it"), "{note}");
    // The pull request names the bump and the exact failure.
    assert!(note.contains("`lib-05` 2.1.0 to 2.1.5 deferred"), "{note}");
    assert!(note.contains(failure), "{note}");
    // Its finding stays open and is held out of later bundles; the others
    // are fixed by the pull request.
    assert_eq!(run.fixed_ids(), vec!["f0", "f1", "f2", "f3", "f4"]);
    let held: Vec<&str> = run
        .deferred
        .iter()
        .filter(|d| !d.later)
        .flat_map(|d| d.finding_ids.iter().map(String::as_str))
        .collect();
    assert_eq!(held, vec!["f5"]);

    // Still red: the set before it. The last set is never taken out: the
    // pull request is not emptied.
    assert!(run.take_out_last_commit(&why).is_some());
    assert_eq!(run.applied, vec![0, 1, 2]);
    assert_eq!(run.take_out_last_commit(&why), None);
    assert_eq!(run.applied, vec![0, 1, 2]);

    // The pipeline: the revert asks the dependency run first, and only a
    // written revert moves the run's saved state.
    let src = include_str!("../resume.rs");
    let revert = fn_src(src, "async fn revert_to_green(");
    assert!(
        revert.find("revert_dep_commit(").unwrap() < revert.find("sup::revert_for(").unwrap(),
        "a dependency run is taken apart by task again"
    );
    let dep = fn_src(src, "async fn revert_dep_commit(");
    let write = dep.find("apply_revert(").unwrap();
    assert!(dep.find("take_out_last_commit(").unwrap() < write);
    assert!(write < dep.find("dep_fix::save(").unwrap());
    assert!(write < dep.find("dep_fix::settle(").unwrap());
    // And the driver records each commit it makes.
    let common_src = include_str!("../../../../common/src/dep_fix.rs");
    assert!(common_src.contains("self.commits.push(BatchCommit {"));
}

#[test]
fn a_plan_is_taken_apart_down_to_the_part_that_passes() {
    let done: Vec<(String, String)> = (1..=6)
        .map(|i| (format!("src/part{i}.ts"), format!("part {i}")))
        .collect();
    // No earlier green head: the last task comes out, then the one before,
    // past the three reverts that used to end the run failed.
    let mut left = done.clone();
    let mut reverts = 0u32;
    while let Revert::Task { path, .. } = supervisor::revert_for(&[], &left, reverts) {
        assert_eq!(path, left.last().unwrap().0);
        left.pop();
        reverts += 1;
    }
    assert_eq!(
        left.len(),
        1,
        "one task stays: the pull request is never emptied"
    );
    assert_eq!(reverts, 5);
    assert!(
        reverts > 3,
        "the old cap ended the run with three tasks that could still come out"
    );
    // A green earlier head is still taken first.
    let heads = vec![("c9".to_string(), false), ("c8".to_string(), true)];
    assert_eq!(
        supervisor::revert_for(&heads, &done, 4),
        Revert::ToHead("c8".into())
    );
    // The bound stays.
    assert_eq!(
        supervisor::revert_for(&heads, &done, supervisor::MAX_REVERTS),
        Revert::Nothing
    );
}

#[test]
fn a_stopped_runs_lockfiles_are_written_before_its_pull_request_opens() {
    let src = include_str!("../stop_pr.rs");
    let open = &src[src.find("pub async fn open_with(").unwrap()..];
    let locks = open
        .find("lockfiles_before_stop(")
        .expect("the lockfiles step");
    assert!(
        locks < open.find(".get_diff(").unwrap(),
        "the diff is read before the lockfiles are written"
    );
    // (Spelled in two parts: a scan counts the places that open pull
    // requests by this call's text.)
    let opens = format!(".create_pull{}", "_request(");
    assert!(locks < open.find(&opens).unwrap());
    let step = fn_src(src, "async fn lockfiles_before_stop(");
    assert!(step.contains("generators::repair_lockfiles_with("));

    // Said on the run only when there was something to write.
    assert_eq!(stop_pr::stop_lock_note(&LockRepair::default()), None);
    let wrote = LockRepair {
        written: vec!["pnpm-lock.yaml".into()],
        ..Default::default()
    };
    let note = stop_pr::stop_lock_note(&wrote).unwrap();
    assert!(
        note.starts_with("Before the pull request: CoderHelm regenerated"),
        "{note}"
    );
    // One that couldn't be written is named, and the stop still opens.
    let failed = LockRepair {
        failed: vec![("yarn.lock".into(), "yarn exited 1".into())],
        ..Default::default()
    };
    assert!(stop_pr::stop_lock_note(&failed)
        .unwrap()
        .contains("couldn't be written"));
    assert!(
        !step.contains("return Err("),
        "a lockfile failure must not stop the pull request"
    );
}

#[test]
fn every_kind_of_fix_round_takes_the_same_decision_about_a_fault() {
    const PUSH: &str = "the one commit couldn't be pushed (HTTP status client error (422 Unprocessable Entity): Update is not a fast forward)";
    const CLASH: &str = "the pull request changed the same files meanwhile (src/a.ts)";
    const OTHER: &str = "the model's reply could not be read";

    // A round with a step left runs again; one without is the round that
    // didn't finish. The same rule the CI fix round follows.
    assert_eq!(round::after_fault_parked(true), AfterFault::Redrive);
    assert_eq!(round::after_fault_parked(false), AfterFault::Supervisor);
    for kind in [
        Kind::ReviewFix,
        Kind::Feedback,
        Kind::SecurityFix,
        Kind::PlannedWork,
    ] {
        // Only the landing failed: the work is kept for the next try.
        let plan = round::plan_for_fault(kind, PUSH, round::after_fault_parked(true));
        assert_eq!(plan.class, Fault::PushRejected);
        assert_eq!(plan.next, AfterFault::Redrive);
        assert!(
            !plan.clean,
            "{kind:?}: finished work is thrown away over a refused push"
        );
        assert!(plan.note.contains("fresh step"), "{}", plan.note);
        assert!(!plan.note.contains("didn't finish"), "{}", plan.note);
        // The pull request moved under it, or anything else: from its head.
        for error in [CLASH, OTHER] {
            let plan = round::plan_for_fault(kind, error, round::after_fault_parked(true));
            assert!(plan.clean, "{kind:?}: {error}");
            assert_eq!(plan.next, AfterFault::Redrive);
        }
        let plan = round::plan_for_fault(kind, PUSH, round::after_fault_parked(false));
        assert_eq!(plan.next, AfterFault::Supervisor);
    }
    assert!(round::keeps_work(Fault::Unreadable) && round::keeps_work(Fault::GitHubBusy));
    assert!(!round::keeps_work(Fault::WipMissing) && !round::keeps_work(Fault::Sandbox));

    // One decision, three callers: none decides a refusal on its own.
    let resume_src = include_str!("../resume.rs");
    let feedback_src = include_str!("../feedback.rs");
    assert_eq!(
        resume_src.matches("super::round::plan_for_fault(").count(),
        2
    );
    assert_eq!(
        feedback_src
            .matches("super::round::plan_for_fault(")
            .count(),
        1
    );
    let fix_round = fn_src(resume_src, "async fn run_fix_round(");
    assert_eq!(
        fix_round.matches("round_fault(").count(),
        2,
        "a refusal and an error"
    );
    assert!(
        !fix_round.contains("couldn't land on the pull request"),
        "run_fix_round calls a refused landing unfinished at once"
    );
    let fault = fn_src(resume_src, "async fn round_fault(");
    let ended = fault.find("finish_incomplete(").unwrap();
    assert!(fault.find("AfterFault::Supervisor").unwrap() < ended);
    assert!(fault.contains("park_round("));
    let refused = &feedback_src[feedback_src.find("Landed::Refused(why) => {").unwrap()..];
    assert!(
        refused.find("send_delayed_feedback(").unwrap()
            < refused.find("finish_incomplete(").unwrap(),
        "a feedback round's refused landing is called unfinished before it is tried again"
    );
}
