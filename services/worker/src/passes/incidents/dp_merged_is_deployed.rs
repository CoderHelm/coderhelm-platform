//! dp. In a repository that deploys to production only on a version tag,
//! merged pull requests (CoderHelm's and people's) were never released, and
//! a person had to cut the tag by hand.
//!
//! Three things stopped the release. The pull request's release steps (the
//! tag deploy, armed when it opened) ticked while waiting for the merge; a
//! tick that met GitHub's budget held returned the error and sent no next
//! tick, so the chain ended before the merge. At the merge the release was
//! unchanged, and the follow assumed its chain was still running. The
//! repository's batched release tag fired after CoderHelm's merge, but its
//! tries met the same held budget, counted it as failures and gave up. And
//! merges by people never opened a release window: only CoderHelm's own
//! merge did.
//!
//! Guarantee: a push to the default branch, whoever merged, joins the
//! repository's release window when the team releases merged changes; one
//! tag covers everything merged so far, cut on a green head only (a running
//! head waits, a red head is reported and never tagged), and a release
//! freeze holds it. A held or spent GitHub budget is a wait until the reset,
//! never a failed try. A release step tick that fails is tried again, and a
//! merge arms an unchanged release again. The tag deploy of a team that
//! releases merged changes waits for that team release, with no card of its
//! own, follows the tag's deploy run to the end and says once on the run and
//! its ticket "Deployed to production in `vX.Y.Z` at HH:MM UTC", or that the
//! deploy failed.

use chrono::TimeZone;
use serde_json::json;

use crate::passes::health_check::Job;
use crate::passes::release_plan::{self, Kind, On, Status, Step};
use crate::passes::release_run::{self, Next};
use crate::passes::review_actions::{self, HeadCi};

const MERGE: &str = "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b";
const LATER: &str = "0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e";

const DEPLOY_PRODUCTION: &str = r#"
name: Build and Deploy Production
on:
  push:
    tags: ['v*.*.*']
jobs:
  deploy:
    environment: production
    runs-on: ubuntu-latest
    steps:
      - run: ./deploy.sh
"#;

fn tag_step() -> Step {
    let m = release_plan::mechanics(
        &[(
            ".github/workflows/deploy-production.yml".into(),
            DEPLOY_PRODUCTION.into(),
        )],
        None,
    );
    let steps = release_plan::follow_steps(&m);
    assert_eq!(steps.len(), 1, "{steps:?}");
    steps[0].clone()
}

fn job(name: &str, status: &str, conclusion: &str) -> Job {
    Job {
        name: name.into(),
        status: status.into(),
        conclusion: conclusion.into(),
        ..Default::default()
    }
}

#[test]
fn the_tag_deploy_is_a_step_after_the_merge() {
    let s = tag_step();
    assert_eq!(s.kind, Kind::Deploy);
    assert!(matches!(s.trigger, Some(On::Tag(_))));
    assert!(s.after_merge);
    // Before the merge it waits; after, it starts.
    assert_eq!(release_run::next(std::slice::from_ref(&s), 0, false), Next::Wait);
    assert_eq!(release_run::next(&[s], 0, true), Next::Start);
}

#[test]
fn a_merge_arms_an_unchanged_release_again() {
    // The record as it stood: the tag step pending, its chain gone.
    let steps = vec![tag_step()];
    assert!(release_run::refollow(&steps, &steps).is_none());
    assert!(release_run::rearm(&steps, true));
    // A release with nothing left to do is left alone.
    let mut done = steps.clone();
    done[0].status = Status::Done;
    assert!(!release_run::rearm(&done, true));
    // The source re-arms on an unchanged release rather than returning.
    let src = include_str!("../release_run.rs");
    let follow = &src[src.find("pub async fn follow_merge(").unwrap()..];
    assert!(follow.contains("None if rearm(&o.steps, true) =>"));
}

#[test]
fn a_failed_tick_is_tried_again_at_githubs_reset() {
    // GitHub's budget held for longer than a queue delay: the next tick waits as long as
    // a queue delay allows, then is held or goes.
    assert_eq!(release_run::retry_delay(Some(2000)), 900);
    assert_eq!(release_run::retry_delay(Some(10)), 60);
    assert_eq!(release_run::retry_delay(None), 300);
    let src = include_str!("../release_run.rs");
    let run =
        &src[src.find("pub async fn run(").unwrap()..src.find("pub fn retry_delay(").unwrap()];
    assert!(run.contains("send(state, &next_msg, d).await"));
}

#[test]
fn a_held_budget_is_a_wait_never_a_failed_try() {
    let src = include_str!("../review_actions.rs");
    let sweep = &src[src.find("pub async fn run_tag_sweep(").unwrap()
        ..src.find("async fn on_merged(").unwrap()];
    // Every GitHub call of the release goes through the budget-aware retry.
    assert!(!sweep.contains("retry_or_record("), "{sweep}");
    assert!(sweep.matches("retry_or_wait(").count() >= 4);
    let wait = &src[src.find("async fn retry_or_wait(").unwrap()..];
    assert!(wait.contains("rate_limited(e)"));
    // The tag list is read, never guessed empty (a seed tag).
    let cut = &src[src.find("pub(crate) async fn cut_tag_if_new(").unwrap()..];
    assert!(cut[..cut.find("if tags").unwrap()].contains(".await?"));
}

#[test]
fn a_persons_merge_joins_the_release() {
    // The gateway sends every push to the default branch; the worker joins
    // it to the window when the team releases merged changes.
    let gw = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    let push = &gw[gw.find("async fn handle_push(").unwrap()..];
    let branch_gate = push.find("refs/heads/{default_branch}").unwrap();
    let arm = push.find("release_on_push(payload").unwrap();
    assert!(arm > branch_gate, "only the default branch releases");
    let m: crate::models::TagSweepMessage = serde_json::from_value(json!({
        "team_id": "T", "installation_id": 1, "repo_owner": "o", "repo_name": "r",
        "base_branch": "main", "pr_number": 0, "merged_sha": LATER
    }))
    .unwrap();
    assert_eq!(m.merged_sha, LATER);
    let src = include_str!("../review_actions.rs");
    let on = &src[src.find("async fn on_merged(").unwrap()..];
    assert!(on.contains("if !cfg.auto_tag"));
    assert!(on.contains("schedule_tag_sweep("));
}

#[test]
fn never_a_red_head() {
    assert_eq!(
        review_actions::head_ci(&[job("lint", "completed", "success")]),
        HeadCi::Green
    );
    assert_eq!(review_actions::head_ci(&[]), HeadCi::Green);
    assert_eq!(
        review_actions::head_ci(&[
            job("lint", "completed", "success"),
            job("test", "in_progress", "")
        ]),
        HeadCi::Running(vec!["test".into()])
    );
    assert_eq!(
        review_actions::head_ci(&[
            job("test", "completed", "failure"),
            job("deploy-staging", "in_progress", "")
        ]),
        HeadCi::Red(vec!["test".into()])
    );
    // A deploy held for an environment's approval is not a check.
    let mut held = job("deploy-production", "waiting", "");
    held.environment = Some("production".into());
    assert_eq!(review_actions::head_ci(&[held]), HeadCi::Green);
}

#[test]
fn a_freeze_holds_the_release_until_it_ends() {
    let now = chrono::Utc
        .with_ymd_and_hms(2026, 11, 24, 16, 0, 0)
        .unwrap();
    assert_eq!(
        review_actions::freeze_wait(Some("2026-11-24T16:20:00Z"), now),
        1_205
    );
    assert_eq!(review_actions::freeze_wait(None, now), 900);
    assert_eq!(
        review_actions::freeze_wait(Some("2026-11-25T16:00:00Z"), now),
        3_600
    );
}

#[test]
fn one_tag_covers_every_merge_and_the_first_one_is_named() {
    let tags = json!([
        {"name": "v1.4.9", "commit": {"sha": "a"}},
        {"name": "v1.4.12", "commit": {"sha": LATER}},
        {"name": "v1.4.11", "commit": {"sha": "b"}},
        {"name": "v1.4.12-rc1", "commit": {"sha": "c"}},
        {"name": "nightly", "commit": {"sha": "d"}},
    ]);
    let order = release_run::releases_newest_first(&tags, "v");
    let names: Vec<&str> = order.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["v1.4.12", "v1.4.11", "v1.4.9"]);
    // The next one is the patch after the highest.
    let all: Vec<String> = ["v1.4.9", "v1.4.12", "v1.4.11"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(review_actions::next_semver_tag(&all, "v"), "v1.4.13");
}

#[test]
fn the_deploy_is_said_once_with_its_tag_and_time() {
    let mut s = tag_step();
    s.tag = "v1.4.12".into();
    s.tag_sha = LATER.into();
    s.run_id = 1001;
    s.status = Status::Done;
    let at = chrono::Utc
        .with_ymd_and_hms(2026, 11, 24, 16, 45, 0)
        .unwrap();
    let line = release_run::deploy_outcome_line(&s, Status::Running, "o", "r", at).unwrap();
    assert_eq!(
        line,
        "Deployed to production in `v1.4.12` at 16:45 UTC \
         (https://github.com/o/r/actions/runs/1001)."
    );
    // Said once: a later tick with the same status says nothing.
    assert!(release_run::deploy_outcome_line(&s, Status::Done, "o", "r", at).is_none());
    s.status = Status::Failed;
    s.detail = "the workflow run ended `failure`: https://x".into();
    let failed = release_run::deploy_outcome_line(&s, Status::Running, "o", "r", at).unwrap();
    assert!(failed.starts_with("Merged, but the production deploy in `v1.4.12` did not succeed"));
    let _ = MERGE;
}

#[test]
fn the_tag_deploy_follows_the_tags_own_commit() {
    let src = include_str!("../release_run.rs");
    let check = &src[src.find("async fn check(").unwrap()..];
    assert!(check.contains(
        "Some(On::Tag(_)) if !s.tag_sha.is_empty() => (s.tag.clone(), s.tag_sha.clone())"
    ));
    // The team's release goes before the card.
    let tick = &src[src.find("async fn tick(").unwrap()..];
    let team = tick.find("team_release(").unwrap();
    let gate = tick.find(".check(Request {").unwrap();
    assert!(team < gate);
}
