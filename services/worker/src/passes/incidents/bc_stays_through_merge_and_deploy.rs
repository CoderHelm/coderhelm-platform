//! bc. Three pull requests CoderHelm opened were left as they were after
//! their runs finished. One conflicted with its base after another merge:
//! the conflict was found only if auto-merge tried to merge it, once, and
//! it had no approval yet. One went red a day later on a check that reports
//! through a commit status, which no event carries to a finished run. One
//! was merged by a person: its production deploy failed twenty minutes
//! later on a job that had passed before the merge, and nothing looked,
//! because the deploy was watched only for merges CoderHelm made itself
//! with the health check setting on.
//!
//! Guarantee: a finished run's pull request is looked at when the run
//! completes and by the sweep (`pr_watch`). A conflict starts the conflict
//! round, once per base commit. Red checks on the head that are not red on
//! the base go to the fix loop, once per head. A merge by anyone gets the
//! merge commit's deploy watched, once per merge, quietly unless it fails.
//! A deploy that fails on checks passing before the merge of CoderHelm's
//! own change gets a revert pull request with the failing jobs as evidence,
//! ready for the team's approval and never merged by CoderHelm; when later
//! commits changed the same files, no revert is built and the report says
//! which files.

use super::fixture::*;
use crate::clients::github::GitEntry;
use crate::passes::deploy_revert::{self, Cause, Outcome, Plan};
use crate::passes::health_check::{self, Job, Verdict};
use crate::passes::pr_watch::{self, Act, Marks, Seen};
use std::collections::HashMap;

const HEAD: &str = "5e4d3c2b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d";
const BASE_NOW: &str = "b1b2b3b4b5b6b7b8b9b0b1b2b3b4b5b6b7b8b9b0";
const MERGE: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";

fn approved() -> Seen {
    Seen {
        open: true,
        head: HEAD.into(),
        base_sha: BASE_NOW.into(),
        reviewer_on: true,
        own_verdict: Some(("APPROVE".into(), HEAD.into())),
        ..Default::default()
    }
}

fn entry(sha: &str) -> GitEntry {
    GitEntry {
        mode: "100644".into(),
        kind: "blob".into(),
        sha: sha.into(),
    }
}

fn tree(files: &[(&str, &str)]) -> HashMap<String, GitEntry> {
    files
        .iter()
        .map(|(p, s)| (p.to_string(), entry(s)))
        .collect()
}

#[test]
fn a_moved_base_and_late_red_checks_are_picked_up_without_a_retry() {
    // The base moved and the pull request conflicts.
    let mut seen = approved();
    seen.conflict = true;
    let act = pr_watch::decide("completed", &seen, &Marks::default());
    assert_eq!(act, Act::ResolveConflict);
    let (field, base) = act.claim(&seen).unwrap();
    assert_eq!((field, base.as_str()), (Marks::CONFLICT, BASE_NOW));
    // A second look at the same base starts no second round.
    let marks = Marks {
        conflict_base: BASE_NOW.into(),
        ..Default::default()
    };
    assert_eq!(pr_watch::decide("completed", &seen, &marks), Act::Nothing);

    // A commit status went red after the run finished.
    let mut seen = approved();
    seen.failing = vec!["ci/preview".into()];
    assert_eq!(
        pr_watch::decide("completed", &seen, &marks),
        Act::FixCi(vec!["ci/preview".into()])
    );
    // A run still working its pull request is left to its own loop.
    assert_eq!(pr_watch::decide("awaiting_ci", &seen, &marks), Act::Nothing);

    // Each act uses the mechanism that already exists for it.
    let perform = fn_src(include_str!("../pr_watch.rs"), "\nasync fn perform(");
    assert!(perform.contains("super::resume::start_conflict_round("));
    assert!(perform.contains("send_ci_fix(state, c)"));
    assert!(perform.contains("super::review_pr::review_for_ready("));
    assert!(perform.contains("super::resume::queue_own_review_round("));
    assert!(perform.contains("super::health_check::schedule_own("));
    // Checks red on the base too are not sent to the fix loop.
    let read = fn_src(include_str!("../pr_watch.rs"), "\nasync fn read(");
    assert!(read.contains("!on_base.contains(n)"));
}

#[test]
fn a_merge_by_a_person_gets_its_deploy_watched_once_and_quietly() {
    let seen = Seen {
        merged: true,
        merge_sha: MERGE.into(),
        ..Default::default()
    };
    let act = pr_watch::decide("merged", &seen, &Marks::default());
    assert_eq!(act, Act::WatchDeploy);
    assert_eq!(act.claim(&seen), Some((Marks::DEPLOY, MERGE.to_string())));
    let marks = Marks {
        deploy_sha: MERGE.into(),
        ..Default::default()
    };
    assert_eq!(pr_watch::decide("merged", &seen, &marks), Act::Nothing);
    // One watch per merge commit, shared with the team's own setting.
    assert_eq!(
        health_check::watch_claim_sk(OWNER, REPO, MERGE),
        format!("DEPLOYWATCH#{OWNER}/{REPO}#{MERGE}")
    );
    let own = fn_src(
        include_str!("../health_check.rs"),
        "\npub async fn schedule_own(",
    );
    assert!(own.contains("watch_claim_sk(owner, repo, merge_sha)"));
    assert!(own.contains("quiet: true"));
    // A quiet watch says nothing for a green or cancelled deploy, and
    // speaks for a failed one.
    assert!(!health_check::speaks(true, Some(&Verdict::Green)));
    assert!(!health_check::speaks(true, None));
    assert!(health_check::speaks(true, Some(&Verdict::Failed(vec![]))));
    assert!(health_check::speaks(false, Some(&Verdict::Green)));
}

#[test]
fn a_failed_deploy_gets_a_revert_ready_for_approval_with_the_evidence() {
    let job = Job {
        name: "deploy-production".into(),
        status: "completed".into(),
        conclusion: "failure".into(),
        url: format!("https://github.com/{OWNER}/{REPO}/actions/runs/88/job/3"),
        ..Default::default()
    };
    let verdict = Verdict::Failed(vec![job]);
    let comment = health_check::render(MERGE, &verdict, &[]);
    assert!(comment.contains("should `a1b2c3d` be rolled back?"));

    // Nothing merged since: every path the merge changed goes back.
    let before = tree(&[("apps/web/src/hours.tsx", "h1"), ("README.md", "r1")]);
    let merged = tree(&[
        ("apps/web/src/hours.tsx", "h2"),
        ("apps/web/src/hours.test.tsx", "t1"),
        ("README.md", "r1"),
    ]);
    let Plan::Clean(changes) = deploy_revert::plan(&before, &merged, &merged) else {
        panic!("a clean revert");
    };
    assert_eq!(changes.len(), 2);

    // The pull request: GitHub's revert shape, the evidence, the approval.
    let evidence = vec![format!(
        "- [**deploy-production**](https://github.com/{OWNER}/{REPO}/actions/runs/88/job/3) (failure)"
    )];
    let files: Vec<String> = changes.iter().map(|(p, _)| p.clone()).collect();
    let body = deploy_revert::body(41, MERGE, &Cause::DeployFailed, &evidence, &files, "");
    assert!(body.starts_with("Reverts #41\n"));
    assert!(body.contains("actions/runs/88/job/3) (failure)"));
    assert!(body.contains("does not merge it"));
    assert_eq!(
        deploy_revert::title("Show opening hours"),
        "Revert \"Show opening hours\""
    );
    // The existing record of reverts recognises it.
    assert_eq!(
        common::rewards::mapping::revert_target(
            &deploy_revert::title("Show opening hours"),
            &body,
            &format!("{OWNER}/{REPO}")
        ),
        Some(41)
    );

    // The comment on the merged pull request says what CoderHelm did; the
    // open question is gone.
    let line = deploy_revert::report_line(&Outcome::Opened {
        number: 44,
        url: String::new(),
    });
    let said = health_check::with_revert_line(&comment, &line);
    assert!(said.contains("CoderHelm opened #44 to revert this change"));
    assert!(said.contains("not merged without the team's approval"));
    assert!(!said.contains("be rolled back?"));
    assert!(said.ends_with(&health_check::marker(MERGE)));

    // The revert is opened ready, and nothing in it merges.
    let src = include_str!("../deploy_revert.rs");
    assert!(!src.contains("merge_pull_request("));
    let build = fn_src(src, "\nasync fn build(");
    assert!(build.contains("find_open_pr_for_branch("), "one per merge");
    let run = fn_src(include_str!("../health_check.rs"), "\npub async fn run(");
    assert!(run.contains("propose_revert(state, &github, &msg, jobs)"));
}

#[test]
fn a_revert_never_undoes_someone_elses_later_work() {
    let before = tree(&[("apps/web/src/hours.tsx", "h1")]);
    let merged = tree(&[("apps/web/src/hours.tsx", "h2")]);
    // A later commit changed the same file.
    let head = tree(&[("apps/web/src/hours.tsx", "h3")]);
    let Plan::Moved(files) = deploy_revert::plan(&before, &merged, &head) else {
        panic!("no clean revert");
    };
    let line = deploy_revert::report_line(&Outcome::Moved(files));
    assert!(line.contains("did not open a revert"));
    assert!(line.contains("`apps/web/src/hours.tsx`"));
    // Someone reverted it by hand already: nothing to do.
    assert_eq!(
        deploy_revert::plan(&before, &merged, &before),
        Plan::AlreadyReverted
    );
}
