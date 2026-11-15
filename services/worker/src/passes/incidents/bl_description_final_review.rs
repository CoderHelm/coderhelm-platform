//! bl. A ticket asked for a Studio warning when a hero subtitle runs past
//! 160 characters. CoderHelm made the change and opened the pull request,
//! and its description read:
//!
//! - six checks as "Not run: still running when this was written", in the
//!   results table, never changed when they finished;
//! - "Needed from you: give CoderHelm the deploy credentials for this
//!   environment or run `cd apps/studio && npm run deploy:staging` once ...
//!   Why a person: a credential only a person holds", in a repository that
//!   deploys staging from a pull request label CoderHelm adds itself, and
//!   with no attempt behind the credential claim;
//! - the file to read first twice ("Read first" and "Look first") and the
//!   way back twice ("Rollback" and "Roll back"), from its lead and an older
//!   "For the reviewer" block;
//! - "CoderHelm's conclusion: nothing is out of the ordinary";
//! - a Studio page by its raw document id beside its name;
//! - "Not verified on ...: ... (still running); ... (did not run)" as one
//!   line.
//!
//! The repository has a pull request template; the description's own
//! layout came first and the template's sections were folded away.
//!
//! What let it through: each writer was right alone and nothing read the
//! result as its reader would; the stamp that says a description is current
//! named only the head, so checks ending on that head never rebuilt it; the
//! gate made every deploy a person could not be shown to own a credential
//! item; the template only guided the model's first draft.
//!
//! Guarantee: every description passes a final review before it is posted
//! (`common::pr_review`, run in `description::save`): a running check reads
//! "Running" and is never a result, and the stamp keeps the description
//! rebuilt until the checks end; a deploy the repository lets CoderHelm
//! start is CoderHelm's (`outside_work::own_startable`); nothing is said
//! twice; no filler; no raw id beside a name; running and did-not-run are
//! said apart; the repository's template is the visible structure
//! (`common::pr_template`). And a run that needs something outside the code
//! still makes the change, opens the pull request and ends delivered, with
//! what only a person can do listed once under "Needed from you".

use common::pr_facts::Why;
use common::pr_review::{self, Kind};

use super::fixture::*;
use crate::passes::description;
use crate::passes::description_facts::{self, Inputs};
use crate::passes::fix_location::{self, FixDecision, FixLocation, OutsideChange, Route as Way};
use crate::passes::outside_work::{self as ow, Abilities, Confirm, NoteStep, Route, Trigger};
use crate::passes::pr_risk;
use crate::passes::reviewer_guide::{Check, State};

const HEAD: &str = "6de2414a0b1c2d3e4f5061728394a5b6c7d8e9f0";
const PAGE_ID: &str = "0f3c2a1b-9d8e-4c7b-a6f5-e4d3c2b1a0f9";
const DEPLOY: &str = "cd apps/studio && npm run deploy:staging";

/// The description as it was posted (names made up), lead and old block.
fn posted_before() -> String {
    format!(
        "Adds a Studio warning on the Hero block subtitle when it is longer than 160 characters (OPS-88).

### Needed from you

- [ ] <!-- ch:deploy-staging --> **Person on the team**, before merge: Give CoderHelm the deploy credentials for this environment or run the command below once. Why a person: a credential only a person holds.

  ```sh
  {DEPLOY}
  ```

### Review focus

- Approach: decide whether this does what OPS-88 asks. CoderHelm's conclusion: nothing is out of the ordinary.
- Read first: `apps/studio/schemaTypes/blocks/hero-block-validation.ts` (the largest change to code, 20 added lines); then the other 2 files.
- See it: Studio: Events page ({PAGE_ID}), hero Sub Title field.

**For the reviewer**
* **Look first:** `apps/studio/schemaTypes/blocks/hero-block-validation.ts`, the largest change to code (20 added lines).
* **Roll back:** revert this pull request; nothing outside the code changes with it.
* **Not verified on `6de2414`:** `Test Coverage Report` (still running); `E2E after staging deploy` (did not run).

**Risk:** Low. One schema file and its test.

**Rollback:** Revert this pull request; nothing outside the code changes with it.

### Verification

On 6de2414 (head of this PR):

| Check | Result |
|---|---|
| Test Coverage Report (CI) | Not run: still running when this was written |
| Lint (CI) | Not run: still running when this was written |
"
    )
}

/// What the old description was wrong about, as it said it.
const OLD: &[&str] = &[
    "still running when this was written",
    "nothing is out of the ordinary",
    PAGE_ID,
    "Give CoderHelm the deploy credentials",
    "(still running); ",
];

fn files() -> Vec<(String, String, String)> {
    let patch = |n: usize| format!("@@ -1 +1,{n} @@\n{}", "+line\n".repeat(n));
    vec![
        (
            "apps/studio/schemaTypes/blocks/hero-block-validation.ts".into(),
            "added".into(),
            patch(20),
        ),
        (
            "apps/studio/schemaTypes/blocks/hero-block.ts".into(),
            "modified".into(),
            patch(3),
        ),
        (
            "apps/studio/schemaTypes/blocks/hero-block-validation.test.ts".into(),
            "added".into(),
            patch(12),
        ),
    ]
}

fn checks(state: State) -> Vec<Check> {
    [
        "Test Coverage Report",
        "Lint",
        "Type check",
        "Unit tests",
        "Build",
        "Studio build",
    ]
    .iter()
    .map(|n| Check {
        name: n.to_string(),
        url: format!("https://ci.example.com/{}", n.replace(' ', "-")),
        state,
    })
    .collect()
}

/// The run's outside work, as the planner wrote it: the staging deploy
/// handed to a person, and the page to look at by its id.
fn the_note() -> OutsideChange {
    OutsideChange {
        system: "the Studio on staging".into(),
        change: "Deploy the Studio so the warning shows to editors".into(),
        steps: vec![NoteStep {
            step: "Deploy the Studio schema to staging".into(),
            by: "person".into(),
            owner: "a person on the team".into(),
            how: DEPLOY.into(),
            kind: "deploy".into(),
            target: "staging".into(),
            why_person: "the repository has no deploy workflow CoderHelm can start".into(),
            reason: "credential".into(),
            ..Default::default()
        }],
        confirm: Some(Confirm {
            query: "*[_type == \"heroBlock\"][0]{_type, \"len\": length(subTitle)}".into(),
            expect: "{_type: heroBlock, len: 392}".into(),
            page: format!("Studio: Events page ({PAGE_ID}), hero Sub Title field"),
            result: "[{\"_type\":\"heroBlock\",\"len\":392}]".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The repository: staging deploys when a label is on the pull request.
fn the_repo() -> Abilities {
    Abilities {
        deploys: vec![Route {
            name: "Deploy staging".into(),
            file: ".github/workflows/deploy-staging.yml".into(),
            command: String::new(),
            trigger: Trigger::Label("deploy-staging".into()),
        }],
        ..Default::default()
    }
}

const WRITTEN: &str = "## Description\n\nAdds a Studio warning on the Hero block subtitle past 160 characters.\n\n## Changes\n\n* `apps/studio/schemaTypes/blocks/hero-block-validation.ts`: the length rule.\n* `apps/studio/schemaTypes/blocks/hero-block.ts`: uses it.\n\n## Type of change\n\n- [x] New feature\n";

const TEMPLATE: &str = "## Description\n<!-- What and why -->\n\n## Type of change\n- [ ] Bug fix\n- [ ] New feature\n\n## How has this been tested?\n\n## Checklist\n- [ ] I have read the contributing guide\n";

/// The body as the run builds it now: the executor's pass over the steps,
/// the facts of the head, the shape, and the one save's review.
fn built(state: State, template: Option<&str>) -> (String, common::pr_facts::Facts) {
    let mut c = the_note();
    ow::classify_with(&mut c, &the_repo());
    let files = files();
    let floor = pr_risk::floor(&files, &std::collections::HashMap::new());
    let checks = checks(state);
    let f = description_facts::facts(&Inputs {
        head: HEAD,
        body: WRITTEN,
        files: &files,
        floor: &floor,
        checks: Some(&checks),
        why: Some(Why {
            reference: "OPS-88".into(),
            title: "Warn on long hero subtitles".into(),
            url: String::new(),
        }),
        dependency: None,
        outside: Some(&c),
        run_url: "https://app.example.com/runs/detail?id=run-88",
        notes: vec![],
        evidence: vec![],
        repo_url: "https://github.com/acme/site",
    });
    let shaped = description_facts::shape_for(
        WRITTEN,
        "OPS-88: Warn on long hero subtitles",
        f,
        None,
        template,
    );
    let body = description::with_stamp(&shaped.body, HEAD);
    (saved(&body), shaped.facts)
}

/// What `description::save` writes.
fn saved(body: &str) -> String {
    let reviewed = pr_review::review(&description::clean(body));
    let sunk = common::person_gate::sink(&reviewed.body);
    common::pr_layout::layout(&sunk.text)
}

/// 1. The posted text through the final review: each defect is found and
///    fixed.
#[test]
fn the_final_review_fixes_the_posted_description() {
    let r = pr_review::review(&posted_before());
    let kinds: Vec<Kind> = r.findings.iter().map(|f| f.kind).collect();
    for k in [
        Kind::RunningAsResult,
        Kind::Filler,
        Kind::RawId,
        Kind::Duplicate,
        Kind::Mixed,
    ] {
        assert!(kinds.contains(&k), "{k:?} not found: {:#?}", r.findings);
    }
    let end = Ending {
        status: "completed".into(),
        pr: Some(88),
        draft: false,
        words: vec![r.body.clone()],
    };
    for old in [OLD[0], OLD[1], OLD[2], OLD[4]] {
        end.never_says(old);
    }
    assert!(end.says("| Lint (CI) | Running; updated here when it ends |"));
    assert!(end.says("**Still running on `6de2414`:** `Test Coverage Report`"));
    assert_eq!(
        r.body
            .matches("revert this pull request; nothing outside")
            .count()
            + r.body
                .matches("Revert this pull request; nothing outside")
                .count(),
        1,
        "{}",
        r.body
    );
    assert!(end.says("See it: Studio: Events page, hero Sub Title field."));
}

/// 2. The run's own build: no running check as a result, rebuilt until the
///    checks end, the deploy CoderHelm's, no filler, no raw id, nothing
///    twice.
#[test]
fn the_description_is_built_with_nothing_frozen_doable_or_said_twice() {
    let (body, facts) = built(State::Running, None);
    let end = Ending {
        status: "awaiting_ci".into(),
        pr: Some(88),
        draft: false,
        words: vec![body.clone()],
    };
    for old in OLD {
        end.never_says(old);
    }
    end.never_says("### Needed from you");
    end.never_says("For the reviewer");
    assert!(
        end.says("| Lint (CI) | Running; updated here when it ends |"),
        "{body}"
    );
    assert!(
        end.says("6 checks are still running on this head"),
        "{body}"
    );
    // Built while checks ran: not current, so the next reconcile (the
    // checks step, the watch, the end of the run) builds it again.
    assert!(body.contains("checks-running"), "{body}");
    assert!(!description::is_current(&body, HEAD));
    // Once they pass, the rows are results and the description is current.
    let (done, _) = built(State::Passed, None);
    assert!(!done.contains("Running"), "{done}");
    assert!(description::is_current(&done, HEAD));
    // The deploy is CoderHelm's, on the label route; nothing for a person.
    assert!(common::pr_facts::needed_items(&facts).is_empty());
    let deploy = facts
        .steps
        .iter()
        .find(|s| s.what.contains("Deploy the Studio"))
        .expect("the deploy step");
    assert!(deploy.owner.eq_ignore_ascii_case("coderhelm"), "{deploy:?}");
    // The final review has nothing left to fix in it.
    assert!(
        pr_review::review(&body).findings.is_empty(),
        "{:#?}",
        pr_review::review(&body).findings
    );
    // And the save runs it, for every writer.
    let src = include_str!("../description.rs");
    assert!(fn_src(src, "\npub async fn save(").contains("pr_review::review("));
    assert!(fn_src(src, "\npub fn is_current(").contains("running"));
}

/// 3. The repository's template is the visible structure, filled with
///    CoderHelm's content, and nothing is said twice below it.
#[test]
fn the_template_is_the_structure() {
    let (body, _) = built(State::Passed, Some(TEMPLATE));
    assert!(
        pr_review::template_missing(&body, TEMPLATE).is_empty(),
        "{body}"
    );
    let seen = common::pr_body::in_sight(&body);
    let at = |h: &str| {
        seen.find(h)
            .unwrap_or_else(|| panic!("{h} not in sight:\n{seen}"))
    };
    assert!(at("## Description") < at("## Type of change"));
    assert!(at("## Type of change") < at("## How has this been tested?"));
    assert!(at("## How has this been tested?") < at("## Checklist"));
    // The summary under Description, the review focus after it, the checks
    // under the testing section, the writer's ticked box kept.
    assert!(at("## Description") < at("Adds a Studio warning"));
    assert!(at("### Review focus") < at("## Type of change"));
    assert!(at("## How has this been tested?") < at("| Check | Result |"));
    assert!(seen.contains("- [x] New feature"), "{seen}");
    assert!(
        seen.contains("- [ ] I have read the contributing guide"),
        "{seen}"
    );
    // The written Type of change is not said again in the fold.
    assert_eq!(body.matches("- [x] New feature").count(), 1, "{body}");
    // And the rebuild reads the template.
    let src = include_str!("../description.rs");
    assert!(fn_src(src, "\nasync fn build(").contains("fetch_pr_template("));
}

/// 4. Something outside the code never replaces the change: the plan's
///    code is implemented and its pull request opened, and the run ends
///    delivered with what only a person can do listed once.
#[test]
fn outside_work_never_stops_the_change_or_the_pull_request() {
    let outside = FixDecision {
        fix_location: FixLocation::OutsideCode,
        cause: "Editors need the warning in the Studio.".into(),
        outside_change: Some(the_note()),
        ..Default::default()
    };
    let tasks = "- [ ] apps/studio/schemaTypes/blocks/hero-block-validation.ts: the length rule";
    assert_eq!(
        fix_location::route_for_plan(Some(&outside), tasks),
        Way::CodeAndOutside,
        "the code is made and the pull request opened"
    );
    // A plan with nothing to change in the code still reports.
    assert_eq!(
        fix_location::route_for_plan(Some(&outside), "NO_CODE_CHANGE: data only"),
        Way::OutsideOnly
    );
    // A person-only step stays, listed once, and the run still completes.
    let mut c = the_note();
    c.steps.push(NoteStep {
        step: "Confirm the new limit with the vendor by phone".into(),
        by: "person".into(),
        owner: "the account manager".into(),
        kind: "other".into(),
        why_person: "a phone call is outside every system CoderHelm reaches".into(),
        reason: "outside_reach".into(),
        ..Default::default()
    });
    ow::classify_with(&mut c, &the_repo());
    let left = ow::left(&c);
    assert_eq!(left.human.len(), 1, "{left:?}");
    let resume = include_str!("../resume.rs");
    let end = fn_src(resume, "\nfn end_status(");
    assert!(!end.contains("STATUS_OUTSIDE"), "{end}");
    assert!(end.contains("COMPLETED"), "{end}");
    let done = fn_src(resume, "\nasync fn set_run_complete(");
    assert!(done.contains("Delivered: the pull request is open"));
}

/// The repository's files: a staging deploy job a pull request label
/// starts (no dispatch), the package script it runs, and a production
/// deploy that is dispatched.
fn label_repo() -> Abilities {
    let files: Vec<(String, String)> = vec![
        (
            ".github/workflows/deploy-staging.yml".into(),
            "name: Deploy staging\non:\n  pull_request:\n    types: [labeled, synchronize]\njobs:\n  deploy:\n    if: contains(github.event.pull_request.labels.*.name, 'deploy-staging')\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: npm ci\n      - run: cd apps/studio && npm run deploy:staging\n".into(),
        ),
        (
            ".github/workflows/deploy-production.yml".into(),
            "name: Deploy production\non:\n  workflow_dispatch:\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npm run deploy:production\n".into(),
        ),
        (
            "apps/studio/package.json".into(),
            "{\"scripts\": {\"deploy:staging\": \"node scripts/ship.js --env staging\", \"deploy:production\": \"node scripts/ship.js --env production\"}}".into(),
        ),
    ];
    let tree: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
    ow::abilities(&tree, &files, &[], false)
}

/// 5. A staging deploy defined by a label-triggered job is found, the gate
///    is told CoderHelm can deploy, and the step is CoderHelm's: never a
///    credential item while it has not tried.
#[test]
fn a_label_triggered_staging_deploy_job_is_coderhelms() {
    let a = label_repo();
    let staging = a
        .deploys
        .iter()
        .find(|r| r.file.ends_with("deploy-staging.yml"))
        .expect("the label-triggered staging deploy is found");
    assert_eq!(staging.trigger, Trigger::Label("deploy-staging".into()));
    assert!(staging.startable());
    assert!(a.can_deploy(ow::Target::Staging));
    // The gate, told what the repository lets CoderHelm do.
    let mut c = the_note();
    let ask = ow::step_now(&c, 0, true).unwrap().ask();
    let ctx = common::person_gate::Context {
        can_deploy: a.can_deploy(ow::Target::Staging),
        ..Default::default()
    };
    assert!(!matches!(
        common::person_gate::decide(&ask, &ctx),
        common::person_gate::Outcome::Human(_)
    ));
    // Without that knowledge it was a credential item: the old failure.
    assert!(matches!(
        common::person_gate::decide(&ask, &common::person_gate::Context::default()),
        common::person_gate::Outcome::Human(_)
    ));
    assert_eq!(ow::classify_with(&mut c, &a), 1);
    let s = &c.steps[0];
    assert_eq!(s.doer(), ow::Doer::CoderHelm);
    assert!(s.reason.is_empty() && s.why_person.is_empty());
    assert!(ow::left(&c).human.is_empty());
    // A failed attempt whose own cause is a credential still reaches a
    // person: the gate says what the attempt found, not a guess.
    let tried = common::person_gate::Ask {
        kind: common::person_gate::Kind::Attempt,
        text: "Deploy the Studio schema to staging".into(),
        how: DEPLOY.into(),
        why_not: "the deploy token is missing; provide the token in the sandbox secrets".into(),
        ..Default::default()
    };
    assert!(matches!(
        common::person_gate::decide(&tried, &ctx),
        common::person_gate::Outcome::Human(_)
    ));
}

/// 6. "CoderHelm does this on its next step" is true: the executor's next
///    action for the step adds the label that starts the job, the run it
///    starts is waited for, and its end is recorded on the step and the
///    run's decision.
#[test]
fn the_deploy_step_is_executed_not_only_reworded() {
    use crate::passes::outside_exec::{self, record_end, START_WORKFLOW};
    let a = label_repo();
    let mut c = the_note();
    ow::classify_with(&mut c, &a);
    let snapshot = c.clone();
    let action = outside_exec::next_action(&c.steps[0], &a, &snapshot).expect("it is done now");
    let route = match &action {
        ow::Action::Label(l, r) => {
            assert_eq!(l, "deploy-staging");
            r.clone()
        }
        other => panic!("not started: {other:?}"),
    };
    assert!(route.file.ends_with("deploy-staging.yml"));
    let (tool, args) = outside_exec::gate_args(&c.steps[0], &action, "acme/site", "feat").unwrap();
    assert_eq!(tool, START_WORKFLOW);
    assert_eq!(args["label"], "deploy-staging");
    // Started: the step waits on the job, and says so.
    c.steps[0].run = Some(ow::StepRun {
        state: outside_exec::RUNNING.into(),
        workflow: route.file.clone(),
        ..Default::default()
    });
    c.steps[0].result = format!(
        "added `deploy-staging` to #88, which starts {}",
        route.line()
    );
    let st = ow::step_now(&c, 0, true).unwrap();
    assert!(
        matches!(&st.state, common::pr_steps::State::Waiting { on } if on.contains("workflow it started ends")),
        "{st:?}"
    );
    assert!(
        outside_exec::next_action(&c.steps[0], &a, &c.clone()).is_some(),
        "polled again"
    );
    // Ended: recorded, done, nothing left.
    let run = serde_json::json!({"id": 4242, "html_url": "https://github.com/acme/site/actions/runs/4242", "head_sha": "6de2414"});
    assert!(record_end(&mut c.steps[0], &route, &run, "success", vec![]).is_none());
    let s = &c.steps[0];
    assert!(s.done);
    assert_eq!(s.run.as_ref().unwrap().workflow_run_id, 4242);
    assert!(s.result.contains("concluded `success`"), "{}", s.result);
    assert!(matches!(
        ow::step_now(&c, 0, true).unwrap().state,
        common::pr_steps::State::Done { .. }
    ));
    assert!(ow::left(&c).doable.is_empty() && ow::left(&c).human.is_empty());
    assert!(outside_exec::next_action(&c.steps[0], &a, &c.clone()).is_none());
    // The executor runs this path, and the resume saves it on the run.
    let src = include_str!("../outside_exec.rs");
    let exec = fn_src(src, "\npub async fn execute(");
    assert!(exec.contains("outside_work::classify_with(") && exec.contains("next_action("));
    let start = fn_src(src, "\nasync fn start(");
    assert!(start.contains(".add_labels(") && start.contains("poll(cx, s, r, until, p)"));
    assert!(fn_src(src, "\nasync fn finish(").contains("record_end("));
    let resume = fn_src(src, "\npub async fn on_resume(");
    assert!(resume.contains("save(state") && resume.contains("refresh_pr("));
}

/// 7. A person saying part of the fix is outside the code keeps the code
///    change and its open pull request, with the outside part attached.
#[test]
fn feedback_naming_outside_work_keeps_the_code_change() {
    let coded = FixDecision {
        fix_location: FixLocation::Code,
        cause: "Editors need the warning in the Studio.".into(),
        ..Default::default()
    };
    assert_eq!(
        fix_location::route_for_feedback(Some(&coded), 3),
        Way::CodeAndOutside
    );
    assert_eq!(
        fix_location::route_for_feedback(None, 3),
        Way::CodeAndOutside
    );
    // Nothing coded to keep: closed as before.
    assert_eq!(
        fix_location::route_for_feedback(Some(&coded), 0),
        Way::OutsideOnly
    );
    let d = fix_location::with_outside_part(
        Some(&coded),
        "the CMS",
        "Set the limit field on the hero type",
    );
    assert_eq!(d.fix_location, FixLocation::Both);
    assert_eq!(fix_location::route(Some(&d)), Way::CodeAndOutside);
    assert_eq!(d.outside_change.as_ref().unwrap().system, "the CMS");
    // The feedback pass keeps the code before it would close anything.
    let fb = include_str!("../feedback.rs");
    let keep = fb.find("keep_code_with_outside(state").expect("kept");
    let close = fb
        .find("let closed = close_own_pr_outside(")
        .expect("closed");
    assert!(keep < close);
    let body = fn_src(fb, "\nasync fn keep_code_with_outside(");
    assert!(body.contains("route_for_feedback(") && body.contains("refresh_pr("));
}
