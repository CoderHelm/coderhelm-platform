//! j. After an auto-merge, the post-merge health check gave up on a timer:
//! 15 looks 90 seconds apart (about 22 minutes), then it posted "Post-merge
//! health check inconclusive: deploy checks were still running after the
//! watch window". The deploy (staging, production, then e2e) took longer.
//! It also read the check runs of the base BRANCH, not the merge commit, so
//! later merges moving the branch head kept "something running" and could
//! put another commit's deploy in the verdict.
//!
//! Guarantee: the check judges the merge commit's own deploy and waits as
//! long as that deploy runs, each look a new queue message. It posts a real
//! verdict only (💚 after the watch window, which starts when the deploy
//! completes, or 🔴 naming each new failing job with its link), one comment
//! per merge. Other commits on the branch never change the verdict. A deploy
//! superseded by a later merge's run is followed to the run that includes the
//! commit, and the comment says so. It never says "inconclusive".
//!
//! Follow-up: a production deploy held for an environment approval is not
//! stuck. The one comment reports what finished and names the job waiting
//! for approval, in which environment, with a link; the check keeps watching
//! (the safety ceiling does not count the wait) and updates the same comment
//! once the approved deploy finishes. A deploy cancelled by hand with no
//! later run is reported 🟡 cancelled, never 🔴.

use super::fixture::*;
use crate::models::{HealthCheckMessage, WorkerMessage};
use crate::passes::health_check::{
    assemble, decide, decide_from, mark, marker, render, render_interim, successor_candidates,
    tick_message, Approval, Follow, Job, Next, Verdict, WfRun,
};
use serde_json::{json, Value};
use std::collections::HashSet;

const OLD: &str = "inconclusive";
const MERGE: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
const LATER: &str = "f9e8d7c6b5a40312";
const MERGED_AT: &str = "2026-10-28T15:00:00Z";

fn at(min: i64) -> String {
    let t =
        chrono::DateTime::parse_from_rfc3339(MERGED_AT).unwrap() + chrono::Duration::minutes(min);
    t.to_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn epoch(min: i64) -> i64 {
    chrono::DateTime::parse_from_rfc3339(&at(min))
        .unwrap()
        .timestamp()
}

/// A check run as GitHub returns it, done at `done` minutes (None = running).
fn check_run(name: &str, sha: &str, suite: u64, start: i64, done: Option<(i64, &str)>) -> Value {
    json!({
        "name": name,
        "head_sha": sha,
        "status": if done.is_some() { "completed" } else { "in_progress" },
        "conclusion": done.map(|d| d.1),
        "html_url": format!("https://github.com/{OWNER}/{REPO}/actions/runs/{suite}/job/{}", name.len()),
        "started_at": at(start),
        "completed_at": done.map(|d| at(d.0)),
        "check_suite": {"id": suite},
    })
}

/// A workflow run as GitHub returns it.
fn workflow_run(
    id: u64,
    wf: u64,
    name: &str,
    sha: &str,
    created: i64,
    state: Option<&str>,
) -> Value {
    json!({
        "id": id,
        "workflow_id": wf,
        "name": name,
        "head_sha": sha,
        "head_branch": BASE,
        "event": "push",
        "status": if state.is_some() { "completed" } else { "in_progress" },
        "conclusion": state,
        "html_url": format!("https://github.com/{OWNER}/{REPO}/actions/runs/{id}"),
        "created_at": at(created),
        "run_started_at": at(created),
        "updated_at": at(created),
        "check_suite_id": id * 10,
    })
}

/// The repo's deploy, minute by minute: CI, then staging, production and e2e
/// for the merge commit, 45 minutes end to end. Two later merges land on the
/// branch at minutes 20 and 40, each with its own long deploy.
fn branch_at(min: i64, e2e: &str) -> Vec<Value> {
    let done = |end: i64, c: &'static str| (min >= end).then_some((end, c));
    let e2e: &'static str = if e2e == "failure" {
        "failure"
    } else {
        "success"
    };
    let mut runs = vec![
        check_run("ci", MERGE, 10, 0, done(8, "success")),
        check_run("deploy-staging", MERGE, 20, 8, done(20, "success")),
        check_run("deploy-production", MERGE, 20, 20, done(35, "success")),
        check_run("e2e", MERGE, 20, 35, done(45, e2e)),
    ];
    if min >= 20 {
        runs.push(check_run("deploy-staging", "later1", 30, 20, None));
    }
    if min >= 40 {
        runs.push(check_run("e2e", "later2", 40, 40, done(42, "failure")));
    }
    runs
}

/// The check runs GitHub returns for `git_ref` at minute `min`.
fn check_runs_for(git_ref: &str, min: i64, e2e: &str) -> Vec<Job> {
    branch_at(min, e2e)
        .iter()
        .filter(|r| r["head_sha"] == git_ref)
        .map(Job::from_check_run)
        .collect()
}

/// Ticks the check the way the queue does: each look decides, and a wait is
/// a new message that the next look reads back.
/// Returns how it ended, the ticks sent, and the minute it ended.
fn watch(e2e: &str, baseline: &[&str]) -> (Ending, Vec<String>, i64) {
    let mut end = Ending::default();
    let mut msg = HealthCheckMessage {
        pr_number: 41,
        base_branch: BASE.into(),
        merge_sha: MERGE.into(),
        baseline_failed: baseline.iter().map(|s| s.to_string()).collect(),
        merged_at: Some(epoch(0)),
        ..Default::default()
    };
    let baseline: HashSet<String> = msg.baseline_failed.iter().cloned().collect();
    let mut sent = Vec::new();
    let mut now = epoch(0) + 60;
    for _ in 0..200 {
        let min = (now - epoch(0)) / 60;
        let jobs = assemble(MERGE, &check_runs_for(MERGE, min, e2e), &[], &[]);
        match decide(
            now,
            epoch(0),
            msg.attempts,
            msg.green_since,
            &jobs,
            &baseline,
        ) {
            Next::Wait { delay, green_since } => {
                msg.green_since = green_since;
                let (body, d) = tick_message(&msg, delay).unwrap();
                assert!(d <= 900);
                sent.push(body.clone());
                let WorkerMessage::HealthCheck(next) = serde_json::from_str(&body).unwrap() else {
                    panic!("a tick is a health_check message")
                };
                msg = next;
                now += d as i64;
            }
            Next::Post(v) => {
                end.said(render(MERGE, &v, &[]));
                end.status = "posted".into();
                return (end, sent, (now - epoch(0)) / 60);
            }
            Next::Quiet => {
                end.status = "quiet".into();
                return (end, sent, (now - epoch(0)) / 60);
            }
            Next::Interim { .. } => panic!("nothing in this deploy waits for approval"),
        }
    }
    panic!("the check never reached a verdict");
}

/// The one PR comment for the merge, as the marker keeps it: every post
/// after the first edits it.
#[derive(Default)]
struct Comment {
    created: usize,
    body: String,
    history: Vec<String>,
}

impl Comment {
    fn upsert(&mut self, body: String) {
        if self.body.contains(&marker(MERGE)) {
            if self.body == body {
                return;
            }
        } else {
            self.created += 1;
        }
        self.history.push(body.clone());
        self.body = body;
    }
}

/// A deploy whose production job is held for approval from minute 20 until
/// `approved` (minutes), then runs 15 minutes with conclusion `prod`.
fn approval_deploy(min: i64, approved: i64, prod: &'static str) -> (Vec<Job>, Vec<Approval>) {
    let done = |end: i64, c: &'static str| (min >= end).then_some((end, c));
    let mut runs = vec![
        check_run("ci", MERGE, 10, 0, done(8, "success")),
        check_run("deploy-staging", MERGE, 20, 8, done(20, "success")),
    ];
    let mut approvals = Vec::new();
    if min >= 20 && min < approved {
        let mut held = check_run("deploy-production", MERGE, 20, 20, None);
        held["status"] = json!("waiting");
        runs.push(held);
        approvals.push(Approval {
            suite_id: 20,
            environments: vec!["production".into()],
            run_url: format!("https://github.com/{OWNER}/{REPO}/actions/runs/2"),
        });
    } else if min >= approved {
        runs.push(check_run(
            "deploy-production",
            MERGE,
            20,
            approved,
            done(approved + 15, prod),
        ));
    }
    let jobs = runs.iter().map(Job::from_check_run).collect();
    (jobs, approvals)
}

/// Ticks the check through an approval wait the way `run` does.
fn watch_approval(approved: i64, prod: &'static str) -> (Ending, Comment, i64) {
    let mut end = Ending::default();
    let mut comment = Comment::default();
    let mut msg = HealthCheckMessage {
        pr_number: 41,
        merge_sha: MERGE.into(),
        merged_at: Some(epoch(0)),
        ..Default::default()
    };
    let none = HashSet::new();
    let mut now = epoch(0) + 60;
    for _ in 0..2000 {
        let min = (now - epoch(0)) / 60;
        let (jobs, approvals) = approval_deploy(min, approved, prod);
        let jobs = mark(assemble(MERGE, &jobs, &[], &[]), &approvals, &[]);
        if jobs.iter().any(Job::awaiting_approval) {
            msg.approval_seen_at = Some(now);
        }
        let from = msg.approval_seen_at.unwrap_or(epoch(0));
        let delay = match decide_from(
            now,
            epoch(0),
            from,
            msg.attempts,
            msg.green_since,
            &jobs,
            &none,
        ) {
            Next::Wait { delay, green_since } => {
                msg.green_since = green_since;
                delay
            }
            Next::Interim {
                finished,
                failed,
                waiting,
                delay,
            } => {
                msg.green_since = None;
                comment.upsert(render_interim(MERGE, finished, &failed, &waiting, &[]));
                delay
            }
            Next::Post(v) => {
                comment.upsert(render(MERGE, &v, &[]));
                for b in &comment.history {
                    end.said(b.clone());
                }
                end.status = "posted".into();
                return (end, comment, min);
            }
            Next::Quiet => panic!("the deploy exists"),
        };
        let (body, d) = tick_message(&msg, delay).unwrap();
        let WorkerMessage::HealthCheck(next) = serde_json::from_str(&body).unwrap() else {
            panic!()
        };
        msg = next;
        now += d as i64;
    }
    panic!("the check never reached a verdict");
}

#[test]
fn a_45_minute_deploy_gets_a_green_verdict_not_inconclusive() {
    let (end, sent, minute) = watch("success", &[]);
    assert_eq!(end.status, "posted");
    assert_eq!(end.words.len(), 1, "one comment");
    assert!(
        end.says("💚 **Post-merge health check passed**"),
        "{:?}",
        end.words
    );
    assert!(end.says(&marker(MERGE)));
    // It waited past the old 22-minute timer: through the 45-minute deploy
    // and the 15-minute window after it, each look a new message.
    assert!(minute >= 60, "posted at minute {minute}");
    assert!(minute <= 75, "posted at minute {minute}");
    assert!(!sent.is_empty());
    let distinct: HashSet<&String> = sent.iter().collect();
    assert_eq!(distinct.len(), sent.len(), "no message is sent twice");
    end.never_says(OLD);
}

#[test]
fn later_merges_on_the_branch_never_change_the_verdict() {
    // At minute 50 the branch head has a running deploy and a failed e2e of
    // its own; the merge commit's deploy is done and green.
    let jobs = assemble(MERGE, &check_runs_for(MERGE, 50, "success"), &[], &[]);
    assert!(jobs.iter().all(|j| j.head_sha == MERGE));
    // Even fed everything on the branch, only the merge commit counts.
    let everything: Vec<Job> = branch_at(50, "success")
        .iter()
        .map(Job::from_check_run)
        .collect();
    assert_eq!(assemble(MERGE, &everything, &[], &[]), jobs);
    let v = decide(epoch(61), epoch(0), 20, None, &jobs, &HashSet::new());
    assert_eq!(v, Next::Post(Verdict::Green));
    // And the check reads the merge commit, never the branch.
    let src = include_str!("../health_check.rs");
    let read = fn_src(src, "\nasync fn read_deploy(");
    assert!(read.contains("check_runs_of(github, msg, &msg.merge_sha)"));
    assert!(read.contains("runs_of(github, msg, &msg.merge_sha)"));
    assert!(
        !src.contains("list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &msg.base_branch)")
    );
}

#[test]
fn a_new_e2e_failure_is_named_with_its_link_and_old_ones_ignored() {
    let (end, _, _) = watch("failure", &["lint"]);
    assert!(
        end.says("🔴 **Post-merge health check FAILED**"),
        "{:?}",
        end.words
    );
    assert!(end.says(&format!(
        "[**e2e**](https://github.com/{OWNER}/{REPO}/actions/runs/20/job/3)"
    )));
    end.never_says(OLD);
    // The same failure before the merge is not news: green.
    let (end, _, _) = watch("failure", &["e2e"]);
    assert!(end.says("💚"), "{:?}", end.words);
}

#[test]
fn a_superseded_deploy_follows_the_run_that_includes_the_commit() {
    // The deploy workflow (id 7) cancelled the merge commit's run when the
    // next merge's run started; that run deploys both commits.
    let own = WfRun::from_json(&workflow_run(100, 7, "Deploy", MERGE, 0, Some("cancelled")));
    let ci = WfRun::from_json(&workflow_run(101, 8, "CI", MERGE, 0, Some("success")));
    let later = WfRun::from_json(&workflow_run(200, 7, "Deploy", LATER, 6, None));
    let unrelated = WfRun::from_json(&workflow_run(300, 9, "Docs", LATER, 6, None));
    let cands = successor_candidates(&own, &[later.clone(), unrelated]);
    assert_eq!(cands, vec![later.clone()]);

    let own_checks = vec![
        Job::from_check_run(&check_run("ci", MERGE, 1010, 0, Some((5, "success")))),
        Job::from_check_run(&check_run("deploy", MERGE, 1000, 0, Some((6, "cancelled")))),
    ];
    let follow = |done: Option<(i64, &str)>| Follow {
        superseded: own.clone(),
        successor: later.clone(),
        successor_checks: vec![
            Job::from_check_run(&check_run("deploy", LATER, 2000, 6, done)),
            // The later commit's CI is not this commit's deploy.
            Job::from_check_run(&check_run("ci", LATER, 2010, 6, Some((7, "failure")))),
        ],
    };
    let none = HashSet::new();

    // While the following run deploys, the check waits (the cancelled run is
    // not a failure).
    let f = follow(None);
    let jobs = assemble(MERGE, &own_checks, &[own.clone(), ci.clone()], &[f]);
    assert!(jobs.iter().all(|j| j.conclusion != "cancelled"));
    assert!(matches!(
        decide(epoch(30), epoch(0), 4, None, &jobs, &none),
        Next::Wait { .. }
    ));

    // It finishes green; after the window the verdict says what it followed.
    let f = follow(Some((40, "success")));
    let jobs = assemble(MERGE, &own_checks, &[own, ci], std::slice::from_ref(&f));
    let v = decide(epoch(56), epoch(0), 9, None, &jobs, &none);
    assert_eq!(v, Next::Post(Verdict::Green));
    let mut end = Ending::default();
    end.said(render(MERGE, &Verdict::Green, &[f]));
    assert!(end.says("superseded by a later merge"));
    assert!(end.says(&format!(
        "[**Deploy**](https://github.com/{OWNER}/{REPO}/actions/runs/200)"
    )));
    assert!(end.says(&format!("for `{}`", &LATER[..7])));
    end.never_says(OLD);
}

#[test]
fn one_comment_per_merge_and_no_timer_verdict_in_the_source() {
    let src = include_str!("../health_check.rs");
    let upsert = fn_src(src, "\nasync fn upsert_comment(");
    assert!(upsert.contains("marker(&msg.merge_sha)"));
    assert!(upsert.contains("edit_issue_comment("));
    let run = fn_src(src, "\npub async fn run(");
    assert!(run.contains("upsert_comment("));
    assert!(!run.contains("create_issue_comment("));
    assert!(!src.contains("MAX_ATTEMPTS"));
    assert!(!src.to_lowercase().contains("health check inconclusive"));
}

#[test]
fn a_deploy_waiting_ten_hours_for_approval_is_reported_then_judged_in_the_same_comment() {
    let (end, comment, minute) = watch_approval(600, "success");
    assert_eq!(comment.created, 1, "one comment per merge");
    // First what finished and what waits, with the link; then the verdict.
    assert!(end.says("⏸ **Post-merge health check: waiting for approval.**"));
    assert!(end.says(&format!(
        "[**deploy-production**](https://github.com/{OWNER}/{REPO}/actions/runs/2) is waiting for approval in **production**"
    )));
    assert!(comment.body.starts_with("💚"), "{}", comment.body);
    assert!(
        minute >= 630,
        "judged after approval and the window: {minute}"
    );
    // The wait was posted once, not on every look.
    assert_eq!(comment.history.len(), 2, "{:?}", comment.history);
    end.never_says("stuck");
    end.never_says("still running");
    end.never_says(OLD);
}

#[test]
fn an_approved_deploy_that_fails_updates_the_comment_to_red() {
    let (end, comment, _) = watch_approval(90, "failure");
    assert_eq!(comment.created, 1);
    assert!(end.says("waiting for approval in **production**"));
    assert!(comment.body.starts_with("🔴"), "{}", comment.body);
    assert!(comment.body.contains("deploy-production"));
    end.never_says("stuck");
}

#[test]
fn a_deploy_cancelled_by_hand_with_no_later_run_is_yellow_not_red() {
    let own = WfRun::from_json(&workflow_run(100, 7, "Deploy", MERGE, 0, Some("cancelled")));
    // No later run of the workflow contains the commit.
    assert!(successor_candidates(&own, &[]).is_empty());
    let checks = vec![
        Job::from_check_run(&check_run("ci", MERGE, 10, 0, Some((5, "success")))),
        Job::from_check_run(&check_run("deploy", MERGE, 1000, 0, Some((9, "cancelled")))),
    ];
    let jobs = mark(
        assemble(MERGE, &checks, std::slice::from_ref(&own), &[]),
        &[],
        &[own],
    );
    let v = decide(epoch(30), epoch(0), 5, None, &jobs, &HashSet::new());
    let Next::Post(verdict @ Verdict::Cancelled(_)) = v else {
        panic!("{v:?}")
    };
    let mut end = Ending::default();
    end.said(render(MERGE, &verdict, &[]));
    assert!(end.says(&format!(
        "the deploy for `{}` was cancelled; nothing later deployed it",
        &MERGE[..7]
    )));
    assert!(end.says("🟡"));
    assert!(end.says(&format!(
        "https://github.com/{OWNER}/{REPO}/actions/runs/1000/job/6"
    )));
    end.never_says("FAILED");
    end.never_says(OLD);
}
