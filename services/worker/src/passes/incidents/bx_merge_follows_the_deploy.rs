//! bx. A person merged one of CoderHelm's pull requests and nothing
//! followed its deploy. The run had been closed by hand ("done by hand")
//! hours before the merge, so the merge kept it closed, and the pull
//! request watch looked only at finished runs: the merge commit's deploy
//! was never watched. The release steps armed when the pull request opened
//! (the plan said "release through the `v*` tag") did tick after the merge,
//! but waited on the wrong workflow: another service's deploy that has a
//! production job on merge to `main` and a path filter on that service's
//! folder, so it never runs for a web change. Staging's deploy was not a
//! step at all, and the `v*` tag that deploys production was never asked
//! for. The product owner saw no deploy.
//!
//! Guarantee: a merge is followed the same way whoever merged it and
//! whatever the run's status: CoderHelm's merge (run `merged`) and a
//! person's (run `completed`, or `closed` by hand) both take
//! `Act::WatchDeploy`, which watches the merge commit's deploy and follows
//! the release (`release_run::follow_merge`). A deploy is this change's only
//! when its workflow runs for the files the pull request changed (the push
//! trigger's path filters); a workflow named for its environment goes
//! before one service's deploy. The release waits for staging's deploy,
//! then prepares production's by the repository's own path (the `v*` tag,
//! under one approval when the team's settings ask for it, said once in
//! one line). A release already armed on the wrong deploy is pointed at the
//! right one, keeping every step that is done. The sweep reads merged runs
//! a person ended, so a merge already missed is picked up.

use crate::passes::pr_watch::{self, Act, Marks, Seen};
use crate::passes::release_plan::{self, Env, Kind, On, Status, Step};
use crate::passes::release_run::{self, approval_line, refollow, Next};

const MERGE: &str = "9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b";

/// Another service's deploy: a job per environment on merge to `main`,
/// limited to its folder.
const SERVICE_DEPLOY: &str = r#"
name: Deploy Thumbnail Service
on:
  push:
    branches: [main]
    paths:
      - 'services/thumbnail/**'
      - '.github/workflows/deploy-thumbnail.yml'
  workflow_dispatch:
jobs:
  deploy-staging:
    environment: staging
    runs-on: ubuntu-latest
    steps:
      - run: ./deploy.sh staging
  deploy-production:
    needs: deploy-staging
    environment: production
    runs-on: ubuntu-latest
    steps:
      - run: ./deploy.sh production
"#;

const STAGING: &str = r#"
name: Deploy Staging
on:
  push:
    branches: [main]
jobs:
  deploy:
    environment: staging
    runs-on: ubuntu-latest
    steps:
      - run: npm run deploy
"#;

const PRODUCTION: &str = r#"
name: Deploy Production
on:
  push:
    tags: ['v*']
jobs:
  deploy:
    environment: production
    runs-on: ubuntu-latest
    steps:
      - run: npm run deploy
"#;

const TESTS: &str = r#"
name: Test Suite
on:
  push:
    branches: [main, develop]
  workflow_dispatch:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: npm test
"#;

fn repo() -> release_plan::Mechanics {
    release_plan::mechanics(
        &[
            (
                ".github/workflows/deploy-thumbnail.yml".into(),
                SERVICE_DEPLOY.into(),
            ),
            (
                ".github/workflows/deploy-staging.yml".into(),
                STAGING.into(),
            ),
            (
                ".github/workflows/deploy-production.yml".into(),
                PRODUCTION.into(),
            ),
            (".github/workflows/test.yml".into(), TESTS.into()),
        ],
        None,
    )
}

/// The web change the pull request made.
fn changed() -> Vec<String> {
    vec![
        "apps/web/src/components/blocks/portal-embed.tsx".into(),
        "apps/web/src/lib/csp.ts".into(),
    ]
}

/// The release armed when the pull request opened, as it stood a moment
/// after the merge: production's deploy waiting on the service's workflow.
fn armed_before_the_fix() -> Vec<Step> {
    let json = r#"[{"id":"deploy_production","kind":"deploy","env":"production",
        "what":"Deploy the change to production",
        "how":"wait for `Deploy Thumbnail Service` (.github/workflows/deploy-thumbnail.yml), which runs on merge to `main`",
        "tool":"","after":[],"after_merge":true,"verify":"","status":"running",
        "detail":"waiting for `.github/workflows/deploy-thumbnail.yml` on the merge commit",
        "before":"","after_count":"","approval_id":"","run_id":0,"tag":"",
        "workflow":".github/workflows/deploy-thumbnail.yml","inputs":[],
        "trigger":{"on":"merge","value":"main"},"project_id":"p1","dataset":"production",
        "started_at":1794879648}]"#;
    serde_json::from_str(json).unwrap()
}

fn merged_seen() -> Seen {
    Seen {
        merged: true,
        merge_sha: MERGE.into(),
        ..Default::default()
    }
}

#[test]
fn coderhelm_merge_and_person_merge_take_the_same_follow() {
    let seen = merged_seen();
    // CoderHelm merged it: the webhook moved the run to `merged`.
    assert_eq!(
        pr_watch::decide("merged", &seen, &Marks::default()),
        Act::WatchDeploy
    );
    // A person merged it after the run finished.
    assert_eq!(
        pr_watch::decide("completed", &seen, &Marks::default()),
        Act::WatchDeploy
    );
    // A person merged it after closing the run by hand (this incident).
    assert_eq!(
        pr_watch::decide("closed", &seen, &Marks::default()),
        Act::WatchDeploy
    );
    // Each act claims the same mark against the same merge commit: one
    // follow per merge, whichever way it came.
    for status in ["merged", "completed", "closed"] {
        let act = pr_watch::decide(status, &seen, &Marks::default());
        assert_eq!(
            act.claim(&seen),
            Some((Marks::DEPLOY, MERGE.to_string())),
            "{status}"
        );
        let done = Marks {
            deploy_sha: MERGE.into(),
            ..Default::default()
        };
        assert_eq!(pr_watch::decide(status, &seen, &done), Act::Nothing);
    }
}

#[test]
fn a_closed_run_is_followed_only_for_its_merge() {
    // An open pull request of a run a person closed is left to them.
    let open = Seen {
        open: true,
        head: "abc".into(),
        conflict: true,
        base_sha: "def".into(),
        failing: vec!["build".into()],
        ..Default::default()
    };
    for status in ["closed", "cancelled", "failed"] {
        assert_eq!(
            pr_watch::decide(status, &open, &Marks::default()),
            Act::Nothing,
            "{status}"
        );
        assert!(pr_watch::follows_merge(status));
    }
    for status in ["running", "queued", "retried", "archived"] {
        assert!(!pr_watch::follows_merge(status), "{status}");
        assert_eq!(
            pr_watch::decide(status, &merged_seen(), &Marks::default()),
            Act::Nothing
        );
    }
}

#[test]
fn the_sweep_catches_up_merges_already_missed() {
    // The scan reads a merged run a person ended until its deploy is
    // followed, inside the watch window.
    let f = pr_watch::SWEEP_FILTER;
    assert!(f.contains("#s IN (:closed, :cancelled, :failed)"), "{f}");
    assert!(f.contains("pr_outcome = :merged AND attribute_not_exists(#wd)"));
    assert!(f.contains("updated_at >= :cut"));
}

#[test]
fn the_deploy_is_the_one_that_runs_for_the_change() {
    let m = repo();
    // Without the files (when the pull request opens), the workflow named
    // for production goes before a service's deploy with a path filter.
    let d = m.deploy(Env::Production).unwrap();
    assert_eq!(d.name, "Deploy Production");
    assert_eq!(d.on, On::Tag("v*".into()));
    assert_eq!(m.deploy(Env::Staging).unwrap().name, "Deploy Staging");

    // With the files: the service's deploy never runs for a web change.
    let web = m.for_change(&changed());
    assert!(!web
        .deploys
        .iter()
        .any(|d| d.file.ends_with("deploy-thumbnail.yml") && matches!(d.on, On::Merge(_))));
    assert_eq!(
        web.deploy(Env::Production).unwrap().name,
        "Deploy Production"
    );
    assert_eq!(web.deploy(Env::Staging).unwrap().name, "Deploy Staging");

    // A change to the service runs it: it stays a deploy of the change.
    let svc = m.for_change(&["services/thumbnail/src/index.ts".to_string()]);
    assert!(svc
        .deploys
        .iter()
        .any(|d| d.file.ends_with("deploy-thumbnail.yml") && matches!(d.on, On::Merge(_))));
}

#[test]
fn the_release_waits_for_staging_then_asks_for_the_tag() {
    let steps = release_plan::follow_steps(&repo().for_change(&changed()));
    let ids: Vec<&str> = steps.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["deploy_staging", "deploy_production"]);
    let (stg, prod) = (&steps[0], &steps[1]);
    // Staging deploys on merge: CoderHelm only waits for it, no gate.
    assert_eq!(stg.workflow, ".github/workflows/deploy-staging.yml");
    assert_eq!(stg.trigger, Some(On::Merge("main".into())));
    assert!(stg.tool.is_empty());
    // Production: the next `v*` tag, checked by the team's release settings
    // (an approval card unless the team allowed it), after staging's deploy.
    assert_eq!(prod.workflow, ".github/workflows/deploy-production.yml");
    assert_eq!(prod.trigger, Some(On::Tag("v*".into())));
    assert_eq!(prod.tool, "release_production_tag");
    assert_eq!(prod.after, vec!["deploy_staging".to_string()]);
    assert!(steps
        .iter()
        .all(|s| s.after_merge && s.kind == Kind::Deploy));

    // After the merge, staging is waited for first; production holds.
    assert_eq!(release_run::next(&steps, 0, true), Next::Start);
    assert_eq!(release_run::next(&steps, 1, true), Next::Wait);
}

#[test]
fn the_release_armed_on_the_wrong_deploy_is_pointed_at_the_right_one() {
    let old = armed_before_the_fix();
    let fresh = release_plan::follow_steps(&repo().for_change(&changed()));
    let now = refollow(&old, &fresh).expect("the release changes");
    let ids: Vec<&str> = now.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["deploy_staging", "deploy_production"]);
    assert_eq!(now[0].status, Status::Pending);
    let prod = &now[1];
    assert_eq!(prod.status, Status::Pending);
    assert_eq!(prod.workflow, ".github/workflows/deploy-production.yml");
    assert_eq!(prod.trigger, Some(On::Tag("v*".into())));
    assert_eq!(prod.after, vec!["deploy_staging".to_string()]);
    assert!(prod.detail.is_empty() && prod.started_at == 0);
    // A second look changes nothing: the running chain is left as it is.
    assert_eq!(refollow(&now, &fresh), None);
}

#[test]
fn a_step_already_done_is_kept() {
    let mut old = armed_before_the_fix();
    old[0].status = Status::Done;
    old[0].detail = "`Deploy Thumbnail Service` succeeded".into();
    let fresh = release_plan::follow_steps(&repo().for_change(&changed()));
    let now = refollow(&old, &fresh).unwrap();
    let prod = now.iter().find(|s| s.id == "deploy_production").unwrap();
    assert_eq!(prod.status, Status::Done);
    assert_eq!(prod.workflow, ".github/workflows/deploy-thumbnail.yml");
}

#[test]
fn a_merge_with_no_release_gets_its_deploys_followed() {
    // No release armed (a plain code change): the merge arms the deploys.
    let fresh = release_plan::follow_steps(&repo().for_change(&changed()));
    let now = refollow(&[], &fresh).unwrap();
    assert_eq!(now, fresh);
    // A repository whose workflows deploy nothing has nothing to follow:
    // no step names a grant for a deploy that happens elsewhere.
    let none = release_plan::follow_steps(&release_plan::mechanics(
        &[(".github/workflows/test.yml".into(), TESTS.into())],
        None,
    ));
    assert!(none.is_empty(), "{none:?}");
}

#[test]
fn the_approval_is_said_once_in_one_short_line() {
    let mut steps = release_plan::follow_steps(&repo().for_change(&changed()));
    assert_eq!(approval_line(&steps), None);
    steps[1].status = Status::Approval;
    let line = approval_line(&steps).unwrap();
    assert!(line.contains("`v*`"), "{line}");
    assert!(line.contains("approval"));
    assert!(!line.contains(".github/"), "{line}");
    assert!(!line.contains('\n'));
    let rendered = common::comment_style::render(&line, true);
    assert!(
        common::comment_style::lint(&rendered).is_empty(),
        "{rendered}"
    );
}
