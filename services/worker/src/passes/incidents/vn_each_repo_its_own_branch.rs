//! vn. A run planned work in two repositories. Planning used most of its
//! step and both coders started with a minute or two left, so neither
//! wrote anything. Each repository's pipeline still saved the run's own
//! checkpoint as implemented, with its own branch, so the second
//! repository's branch was the last one kept. The next step read that
//! checkpoint as the run's, skipped the coder and compared the second
//! repository's branch in the run's repository, where it was never made:
//! GitHub's 404 ended the whole run as failed, with the HTTP error as its
//! reason.
//!
//! Guarantee: each repository uses its own branch, made before anything
//! compares it, and keeps its own checkpoint; the run's checkpoint names a
//! branch of the run's repository only, and only once work is on it. A
//! checkpoint naming a branch the run's repository doesn't have goes back
//! to the plan (never planned again). Each coder starts with a step's full
//! time. A compare of a branch not pushed yet is no diff. A run left failed
//! this way is resumed by the self-heal sweep after the release and
//! finishes with one pull request per repository, one plan and one
//! follow-up.

use super::fixture::*;
use crate::clients::github::{compare_404, empty_compare, Compare404};
use crate::passes::recover::replay::World;
use crate::passes::recover::{resend_for, sweep_wants, Resend};
use aws_sdk_dynamodb::types::AttributeValue;
use common::run_recovery::{self as rr, Kind, Repair};
use std::collections::HashMap;

/// The run's repository, the other one, and the ticket: invented names.
const OWNER: &str = "acme";
const RUN_REPO: &str = "infra-live";
const OTHER: &str = "web-api";
const TICKET: &str = "tk-1";

/// The error the run ended on, as the record holds it.
fn incident_error() -> String {
    format!(
        "HTTP status client error (404 Not Found) for url \
         (https://api.github.com/repos/{OWNER}/{RUN_REPO}/compare/main...coderhelm/{TICKET}-{OTHER}): \
         {{\"message\":\"Not Found\",\"documentation_url\":\"https://docs.github.com/rest/commits/commits#compare-two-commits\",\"status\":\"404\"}}"
    )
}

/// The failed run's record, in the table's shape (names invented).
fn failed_record() -> HashMap<String, AttributeValue> {
    let pass = |p: &str| {
        AttributeValue::M(HashMap::from([
            ("pass".to_string(), s(p)),
            ("started_at".to_string(), s("2000-01-01T00:00:00Z")),
        ]))
    };
    let mut item = run_item(&[
        ("team_id", s("TEAM#t1")),
        ("run_id", s("R1")),
        ("status", s("failed")),
        ("current_pass", s("implement")),
        ("failed_pass", s("implement")),
        ("repo", s(&format!("{OWNER}/{RUN_REPO}"))),
        ("base_branch", s("main")),
        ("run_step", n(4)),
        ("installation_id", n(7)),
        ("issue_number", n(0)),
        ("ticket_source", s("jira")),
        ("ticket_id", s("TK-1")),
        ("title", s("Build the scheduled job")),
        ("error_message", s(&incident_error())),
        ("ceil_continuations", n(3)),
        ("ceil_passes", n(6)),
    ]);
    item.insert(
        "pass_history".into(),
        AttributeValue::L(
            [
                "triage",
                "plan",
                "plan",
                "implement",
                "implement",
                "implement",
            ]
            .iter()
            .map(|p| pass(p))
            .collect(),
        ),
    );
    item
}

/// Where the run stood after its second step: the plan made once, its
/// follow-up filed once, the run's branch and each repository's branch
/// made with nothing on them, and the run's checkpoint naming the other
/// repository's branch (what the old pipeline saved).
fn after_the_second_step(guard: bool) -> World {
    let mut w = World::new(OWNER, RUN_REPO, &[RUN_REPO, OTHER], TICKET);
    w.guard = guard;
    w.plan_saved = true;
    w.plans = 1;
    w.tickets.insert("follow-up: seed the mapping rows".into());
    w.ticket_writes = 1;
    let run_full = format!("{OWNER}/{RUN_REPO}");
    let other_full = format!("{OWNER}/{OTHER}");
    w.host.add(&run_full, &format!("coderhelm/{TICKET}"));
    w.host.add(&run_full, &w.branch_for(RUN_REPO));
    w.host.add(&other_full, &w.branch_for(OTHER));
    *w.run.checkpoint.lock().unwrap() = Some(("implement".into(), w.branch_for(OTHER)));
    w
}

#[test]
fn the_error_is_a_missing_branch_said_in_plain_words() {
    let c = rr::classify(&incident_error());
    assert_eq!(c.kind, Kind::Repairable);
    assert_eq!(
        c.repair,
        Some(Repair::MissingRef {
            owner: OWNER.into(),
            repo: RUN_REPO.into(),
            base: "main".into(),
            head: format!("coderhelm/{TICKET}-{OTHER}"),
        })
    );
    assert!(!c.plain.contains("api.github.com") && !c.plain.contains('{'));
    // Parked for the sweep, never "needs you" and never a raw dump.
    assert_eq!(
        common::run_fail::status_for(&incident_error()),
        common::run_status::FAILED
    );
    // A compare whose head isn't pushed yet is no diff.
    assert_eq!(compare_404(true, false), Compare404::NoDiffYet);
    assert_eq!(compare_404(false, true), Compare404::BaseGone);
    assert_eq!(compare_404(true, true), Compare404::AsIs);
    assert_eq!(empty_compare()["files"].as_array().map(Vec::len), Some(0));
}

/// The release that failed, replayed: its next step compares the other
/// repository's branch in the run's repository and meets the 404. The
/// recovery path repairs the checkpoint and the run finishes.
#[tokio::test]
async fn the_old_release_meets_the_404_and_recovery_finishes_the_run() {
    let mut w = after_the_second_step(false);
    let first = w.step().unwrap_err();
    assert!(first.starts_with(&incident_error()[..incident_error().find("): ").unwrap()]));
    assert_eq!(
        rr::classify(&first).repair,
        rr::classify(&incident_error()).repair
    );
    w.steps = 0;
    assert!(w.run_to_end(6).await, "{:?}", w.run.notes.lock().unwrap());
    w.finished_once();
    let notes = w.run.notes.lock().unwrap().join("\n");
    assert!(notes.contains("Repaired: the checkpoint (implement) named"));
    assert!(!notes.contains("api.github.com"));
}

/// The new release from the same state: the checkpoint guard sends the
/// pass back to the plan before any compare, and each repository builds on
/// its own branch in one step, with no error at all.
#[tokio::test]
async fn the_new_release_never_compares_another_repositorys_branch() {
    let mut w = after_the_second_step(true);
    w.step().expect("no error");
    w.finished_once();
    assert!(w.run.notes.lock().unwrap().is_empty(), "nothing to recover");
}

/// After the release, the self-heal sweep finds the failed run, resumes
/// the same run one step on from its checkpoint and own plan, and it
/// finishes.
#[tokio::test]
async fn the_sweep_resumes_the_failed_run_after_the_release() {
    let item = failed_record();
    assert!(sweep_wants(&item, "release-2"));
    let Some(Resend::Ticket(next)) = resend_for(&item) else {
        panic!("a ticket continuation")
    };
    assert!(next.same_run(), "the same run, never a new one");
    assert_eq!(next.continuation_run_id.as_deref(), Some("R1"));
    assert_eq!(next.continuation, 5);
    assert_eq!(next.repo_name, RUN_REPO);
    // The sweep's recovery, then the resumed step, on the new release.
    let mut w = after_the_second_step(true);
    let c = rr::classify(&incident_error());
    assert_eq!(c.kind, Kind::Repairable);
    assert!(w.run_to_end(4).await);
    w.finished_once();
}

#[test]
fn the_pipeline_keeps_each_repositorys_branch_and_checkpoint_its_own() {
    let src = include_str!("../mod.rs");
    // The run's resume checks its checkpoint's branch is in its repository
    // before it skips the coder.
    let guard = src.find("recover::branch_missing(").expect("the guard");
    let skip = src
        .find("let can_skip_implement = checkpoint")
        .expect("the skip");
    assert!(guard < skip);
    // A repository's pipeline keeps its own checkpoint, never the run's,
    // makes its branch before anything compares it, and saves its
    // checkpoint only after the empty-work judgement.
    let repo = fn_src(src, "async fn run_repo_pipeline(");
    assert!(
        !repo.contains("save_checkpoint("),
        "a repo pipeline writes the run's checkpoint"
    );
    assert!(repo.contains("save_repo_checkpoint("));
    let start = repo.find(".start_branch(").unwrap();
    let diff = repo.find(".get_diff(").unwrap();
    assert!(start < diff, "the branch is made before it is compared");
    let judged = repo.find("run_end::judge(").unwrap();
    let saved = repo.find("save_repo_checkpoint(").unwrap();
    assert!(judged < saved, "nothing written is no checkpoint");
    // Each repository's coder starts with a step's full time.
    let multi = src
        .find("for (repo_task, repo_branch) in plan_result.repo_tasks.iter().zip(repo_branches)")
        .unwrap();
    let rest = &src[multi..];
    let time = rest.find("MIN_TIME_FOR_IMPLEMENT_SECS").unwrap();
    let pipeline = rest.find("run_repo_pipeline(").unwrap();
    assert!(time < pipeline);
    // A compare of a branch not pushed yet is no diff.
    let gh = include_str!("../../clients/github.rs");
    let get_diff = fn_src(gh, "pub async fn get_diff(");
    assert!(get_diff.contains("compare_404("));
    assert!(get_diff.contains("empty_compare()"));
}
