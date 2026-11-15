//! bk. A ticket run embedded a vendor's booking portal on four pages. The
//! code was done and its pull request open; the run then ended "needs an
//! outside change" and the ticket's comment handed people a list of work
//! CoderHelm could do itself:
//!
//! - "Confirm 'Deploy Preview' and 'E2E on preview' ran ... CoderHelm could
//!   not: ran `a page check find [...]`: the read API returned 400 Bad
//!   Request (a person on the team)". The step only read workflow runs. The
//!   gate's deploy rule fired on the word "Deploy" in a workflow's name, and
//!   a failed attempt with no listed reason defaulted to "a credential".
//! - "Tag the v* production release under approval ... CoderHelm could not:
//!   the repository has no production deploy workflow CoderHelm can start
//!   (it deploys with `cd apps/studio && npm run deploy` ...)". The release
//!   runs on a pushed tag, which CoderHelm cuts after one approval; the
//!   studio scripts had nothing to do with it.
//! - Three more steps (a lint, a page check, the preview check) carried the
//!   same 400 as their reason: every check step ran the note's one confirm
//!   "query", a page check written as `check_url https://... find [...]`,
//!   against the CMS's GROQ read API, which answered 400 to each. The
//!   `check_url` tool itself was never broken.
//! - "Look at <page>. CoderHelm cannot: how a page looks is a judgement only
//!   a person can make" (a review sign-off), "... CoderHelm does this once
//!   step 7 is done" (a step number the ticket never showed), and a vendor
//!   setting needed only if a check failed, listed as a chore.
//! - The comment opened on the commenter's raw Jira account id: the site's
//!   older Jira app sends the id where the name belongs.
//! - The run recorded `handoff_human=0` with all of this handed over: the
//!   outside steps never reached the hand-off counts.
//!
//! Why today's gate missed it: the end of the run decided its status from
//! `outside_work::held`, which listed every person step and every failed
//! CoderHelm step without asking the gate; the guard test only checked that
//! each file called the gate somewhere, and `outside_work.rs` did (for the
//! "Needed from you" lines), so the second, ungated path in the same file
//! passed. The finish-the-work guard of the run's own review covered
//! `complete_run_with_status` and `wait_on_person`, not the CI resume's
//! `set_run_complete`, which wrote the status.
//!
//! Guarantee: one executor for outside steps; each step's action comes from
//! its own words (a GROQ query, a page check, a read of workflow runs, the
//! head's checks, the release steps), never another step's query; a failed
//! attempt reaches a person only for a permission or a credential in its own
//! cause; a tool failure is diagnosed once, its steps take another way, and
//! it is reported once as CoderHelm's own problem; the production release is
//! one approval; a run waits on a person only with a gate-approved item and
//! nothing it can still do; every text that reaches a person passes the
//! sink check; a reply never shows an account id; the counts are what was
//! handed over.

use common::person_gate::{self, Class, Kind, Outcome};
use common::pr_steps::State;
use serde_json::json;

use crate::passes::fix_location::OutsideChange;
use crate::passes::outside_exec::{self, checks_verdict, runs_verdict};
use crate::passes::outside_work::{
    self as ow, Abilities, Action, Confirm, ContentSource, NoteStep, Route, Trigger,
};
use crate::passes::release_plan::{self, Deploy, Env, Mechanics, On};

/// The page check the planner wrote where a GROQ query belongs.
const PAGE_CHECK_QUERY: &str = "check_url https://www.site.example/{north,south,east,west}/programmes/register find ['Programme registration','Open registration in a new tab']";

fn step(by: &str, kind: &str, target: &str, text: &str, how: &str) -> NoteStep {
    NoteStep {
        step: text.into(),
        by: by.into(),
        owner: if by == "person" {
            "a team"
        } else {
            "CoderHelm"
        }
        .into(),
        how: how.into(),
        kind: kind.into(),
        target: target.into(),
        ..Default::default()
    }
}

/// The run's outside steps as the planner wrote them (names made up).
fn the_steps() -> Vec<NoteStep> {
    vec![
        NoteStep {
            values: vec!["draft=false".into(), "labels += CI:E2E, E2E:programmes".into()],
            ..step("coderhelm", "e2e", "staging", "Open the PR ready for review, then add labels CI:E2E and E2E:programmes keeping existing labels", "GitHub PR and labels API")
        },
        step("coderhelm", "verify", "staging", "Run e2e eslint on the two new e2e files; record exit 0", "sandbox: cd e2e && pnpm install --ignore-workspace && npx eslint pages/register.ts tests/register.spec.ts"),
        step("coderhelm", "verify", "staging", "Confirm 'Deploy Preview' and 'E2E on preview' ran (not skipped) and the spec passed; read the framing result; if refused, flag it on the ticket and stop", "github_history workflow_runs and the PR e2e comment"),
        NoteStep {
            why_person: "Only read access to the vendor's programs API; no portal admin".into(),
            grant: "None via a connection".into(),
            ..step("person", "other", "production", "Only if framing is refused: allow the site and the preview hosts in frame-ancestors", "the vendor's portal settings")
        },
        step("coderhelm", "verify", "staging", "check_url the four paths on the preview, then on staging after merge, for 200 and the iframe title and link text", "check_url with find"),
        NoteStep {
            why_person: "No real devices or payment method".into(),
            grant: "None".into(),
            ..step("person", "verify", "staging", "One paid registration in a private window on Chrome, desktop Safari, iOS Safari and Android Chrome", "real devices on the preview or staging")
        },
        step("coderhelm", "deploy", "production", "Tag the v* production release under approval", "deploy-production.yml on v* tag"),
        step("coderhelm", "verify", "production", "opsctl cache purge-all --env prod --yes, then check_url the four production paths", "opsctl and check_url"),
    ]
}

fn the_note() -> OutsideChange {
    OutsideChange {
        system: "the vendor's portal; the preview end-to-end run; the site release".into(),
        change: "Confirm framing on the preview; real-device paid checkout; production release"
            .into(),
        steps: the_steps(),
        confirm: Some(Confirm {
            query: PAGE_CHECK_QUERY.into(),
            expect: "200 and all strings".into(),
            page: "/north/programmes/register".into(),
            e2e: "e2e/tests/register.spec.ts passes, not skipped".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// What the repository offers: a CMS it reads, a labelled end-to-end run,
/// the studio's deploy scripts, production deploys on a `v*` tag, a cache
/// purge workflow.
fn the_repo() -> Abilities {
    Abilities {
        source: Some(ContentSource {
            project_id: "proj1".into(),
            dataset: "production".into(),
            origin: "apps/studio/.env".into(),
        }),
        deploys: vec![
            Route {
                name: "deploy".into(),
                file: "apps/studio/package.json".into(),
                command: "cd apps/studio && npm run deploy".into(),
                trigger: Trigger::Script,
            },
            Route {
                name: "deploy:production".into(),
                file: "apps/studio/package.json".into(),
                command: "cd apps/studio && npm run deploy:production".into(),
                trigger: Trigger::Script,
            },
        ],
        e2e: vec![Route {
            name: "E2E on preview".into(),
            file: ".github/workflows/deploy-preview.yml".into(),
            command: String::new(),
            trigger: Trigger::Label("CI:E2E".into()),
        }],
        release: Mechanics {
            deploys: vec![Deploy {
                env: Env::Production,
                name: "Deploy Production".into(),
                file: ".github/workflows/deploy-production.yml".into(),
                on: On::Tag("v*".into()),
            }],
            purge: Some(release_plan::Runner {
                name: "Purge cache".into(),
                file: ".github/workflows/purge.yml".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The run's record as it ended: every check step's result the same 400.
fn the_old_results() -> OutsideChange {
    let mut c = the_note();
    let old = format!("not done: ran `{PAGE_CHECK_QUERY}`: the read API returned 400 Bad Request");
    for i in [1, 2, 4] {
        c.steps[i].result = old.clone();
    }
    c.steps[0].done = true;
    c.steps[0].result = "added `CI:E2E`, `E2E:programmes` to #12".into();
    c.steps[6].result = "not done: the repository has no production deploy workflow CoderHelm can start (it deploys with `cd apps/studio && npm run deploy` or `cd apps/studio && npm run deploy:production`); grant: add a deploy workflow with a manual start (`workflow_dispatch:` in its triggers)".into();
    c.confirm.as_mut().unwrap().result =
        "the query failed: the read API returned 400 Bad Request".into();
    c
}

/// The comment the ticket got, as it was posted (names made up).
const OLD_COMMENT: &str = "**Needed from you:**\n\
1. Confirm 'Deploy Preview' and 'E2E on preview' ran (not skipped) and the spec passed; read the framing result: github_history workflow_runs and the PR e2e comment. CoderHelm could not: ran `a page check find ['Programme registration']`: the read API returned 400 Bad Request. (a person on the team)\n\
2. Tag the v* production release under approval: deploy-production.yml on v* tag. CoderHelm could not: the repository has no production deploy workflow CoderHelm can start (it deploys with `cd apps/studio && npm run deploy`). To let it: add a deploy workflow with a manual start (`workflow_dispatch:` in its triggers). (a person on the team)\n\
\n\
**What CoderHelm does and checked**\n\
- Run e2e eslint on the two new e2e files; record exit 0: sandbox: cd e2e && npx eslint pages/register.ts. CoderHelm could not: ran `a page check find ['Programme registration']`: the read API returned 400 Bad Request.\n\
- After merging: a page check the four paths on the preview, for 200 and the iframe title. CoderHelm could not: ran `a page check find ['Programme registration']`: the read API returned 400 Bad Request.\n\
- cache purge-all --env prod --yes, then a page check the four production paths. CoderHelm does this once step 7 is done.\n\
- Look at /north/programmes/register. CoderHelm cannot: how a page looks is a judgement only a person can make.\n";

#[test]
fn every_step_gets_its_own_action_and_the_page_check_never_reaches_the_cms() {
    let a = the_repo();
    let c = the_note();
    let actions: Vec<Action> = c.steps.iter().map(|s| ow::action_for(s, &a, &c)).collect();
    assert!(matches!(&actions[0], Action::Label(l, _) if l.contains("CI:E2E")));
    assert!(
        matches!(&actions[1], Action::HeadChecks(cmd) if cmd.contains("eslint")),
        "{:?}",
        actions[1]
    );
    assert_eq!(
        actions[2],
        Action::ReadRuns(vec!["Deploy Preview".into(), "E2E on preview".into()])
    );
    // The preview's pages are read from the preview's end-to-end run.
    assert_eq!(actions[4], Action::ReadRuns(Vec::new()));
    // The release and what follows it are the release steps'.
    assert_eq!(actions[6], Action::Release);
    assert_eq!(actions[7], Action::Release);
    // The cause of the 400: the confirm is a page check, not GROQ, and is
    // never sent to the CMS.
    assert!(!ow::is_groq(PAGE_CHECK_QUERY));
    assert!(ow::is_groq("*[_type == \"settings\"]{_id, blankLayout}"));
    assert!(ow::is_groq("count(*[_type == \"page\"])"));
    // No two steps share one action that failed for all of them.
    let queries = actions
        .iter()
        .filter(|x| matches!(x, Action::Query(_)))
        .count();
    assert_eq!(queries, 0, "{actions:?}");
    // The confirm's pages, written out, for the release's own check.
    let checks = release_plan::checks_of(&c);
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].0.len(), 4);
    assert_eq!(
        checks[0].1,
        vec![
            "Programme registration".to_string(),
            "Open registration in a new tab".to_string()
        ]
    );
    // The release's steps: the next tag under one approval, the purge, the
    // production pages checked.
    let rel = release_plan::release_only_steps(&a.release, &checks);
    let kinds: Vec<_> = rel.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        vec![
            release_plan::Kind::Deploy,
            release_plan::Kind::Purge,
            release_plan::Kind::Check
        ]
    );
    assert_eq!(rel[0].tool, "release_production_tag");
    assert!(rel.iter().all(|s| s.after_merge));
}

/// What the executor records from what GitHub answers on the head.
fn as_executed() -> OutsideChange {
    let a = the_repo();
    let mut c = the_note();
    c.placed = Some(Default::default());
    assert_eq!(
        ow::classify(&mut c),
        0,
        "the gate keeps the two person steps"
    );
    let runs = vec![
        json!({"name": "E2E on preview", "status": "completed", "conclusion": "success", "id": 9}),
        json!({"name": "Deploy Preview", "status": "completed", "conclusion": "success", "id": 8}),
        json!({"name": "Lint", "status": "completed", "conclusion": "success", "id": 7}),
    ];
    let checks = vec![
        json!({"name": "lint", "status": "completed", "conclusion": "success"}),
        json!({"name": "unit", "status": "completed", "conclusion": "success"}),
    ];
    for i in 0..c.steps.len() {
        let snapshot = c.clone();
        let s = &mut c.steps[i];
        if s.doer() != ow::Doer::CoderHelm {
            continue;
        }
        match ow::action_for(s, &a, &snapshot) {
            Action::Label(..) => {
                s.done = true;
                s.result = "added `CI:E2E`, `E2E:programmes` to #12".into();
            }
            Action::HeadChecks(_) => {
                assert_eq!(checks_verdict(&checks), Some(Ok(2)));
                s.done = true;
                s.result = "the pull request's 2 checks passed on `abc1234`".into();
            }
            Action::ReadRuns(names) => {
                assert!(runs_verdict(&runs, &names).done(), "{names:?}");
                s.done = true;
                s.result = "passed on `abc1234`".into();
            }
            Action::Release => {
                s.run = Some(ow::StepRun {
                    state: ow::RELEASE.into(),
                    ..Default::default()
                });
                s.result = "the release steps do it after the merge".into();
            }
            other => panic!("step {i}: {other:?}"),
        }
    }
    c
}

#[test]
fn the_run_does_its_steps_and_asks_once_for_the_release_and_the_device_checkout() {
    let c = as_executed();
    let left = ow::left(&c);
    assert!(left.doable.is_empty(), "{:?}", left.doable);
    assert_eq!(left.human.len(), 2, "{:?}", left.human);
    assert!(
        left.human[0].contains("paid registration"),
        "{}",
        left.human[0]
    );
    assert!(
        left.human[1].starts_with("Approve \"Tag the v* production release"),
        "{}",
        left.human[1]
    );
    assert!(person_gate::may_wait_on_person(
        left.human.len(),
        left.doable.len()
    ));
    // The vendor setting is a question only if the check finds framing
    // refused: until then nobody acts.
    let vendor = ow::step_now(&c, 3, true).unwrap();
    assert!(
        matches!(&vendor.state, State::Waiting { on } if on.starts_with("only if framing is refused")),
        "{vendor:?}"
    );
    // The ticket's note: short, what was done, the asks once.
    let note = ow::ticket_note(&c);
    for never in [
        "CoderHelm could not",
        "a person on the team",
        "github_history",
        "400",
        "once step",
        "Look at",
        "npm run deploy",
        "judgement only a person",
    ] {
        assert!(!note.contains(never), "`{never}` in:\n{note}");
    }
    assert!(note.starts_with("Done outside the code: "), "{note}");
    assert!(note.contains("**Needed from you:**\n1. "), "{note}");
    assert_eq!(
        person_gate::sink(&note).sent_back,
        Vec::<String>::new(),
        "{note}"
    );
    // The counts are what was handed over.
    let k = ow::counts(&c);
    assert_eq!(k.human, 2);
    assert_eq!(k.review, 1, "the look is the reviewer's sign-off");
}

#[test]
fn the_old_record_hands_nothing_over_and_sends_the_run_back_to_its_steps() {
    let c = the_old_results();
    let left = ow::left(&c);
    // The 400 and the missing workflow are CoderHelm's to work around.
    assert!(
        left.human
            .iter()
            .all(|l| !l.contains("400") && !l.contains("deploy workflow")),
        "{:?}",
        left.human
    );
    assert!(left.doable.len() >= 3, "{:?}", left.doable);
    assert!(!person_gate::may_wait_on_person(
        left.human.len(),
        left.doable.len()
    ));
    // Retrying clears exactly CoderHelm's failed attempts.
    let mut again = c.clone();
    assert_eq!(ow::reopen_own(&mut again), 4);
    assert!(again.steps[1].result.is_empty());
    assert!(again.steps[0].done, "a done step stays done");
    // Each reason is its own: a failed attempt is read with its cause.
    for i in [1, 2, 4, 6] {
        let st = ow::step_now(&c, i, true).unwrap();
        assert!(!st.needs_person(), "step {i}: {}", st.line());
    }
}

#[test]
fn the_comment_as_posted_would_not_pass_the_sink() {
    let sunk = person_gate::sink(OLD_COMMENT);
    assert!(sunk.defect());
    assert!(sunk.human.is_empty(), "{:?}", sunk.human);
    // Every item it addressed to a person, and every "could not" and
    // "cannot" line, is taken out (the purge line was CoderHelm's own).
    assert_eq!(sunk.sent_back.len(), 5, "{:?}", sunk.sent_back);
    assert!(
        sunk.text.contains("**Needed from you:** Nothing."),
        "{}",
        sunk.text
    );
    assert!(!sunk.text.contains("CoderHelm could not"), "{}", sunk.text);
    // A genuine item passes it: a credential, the device checkout.
    let ok = "**Needed from you:**\n1. One paid registration on real devices. CoderHelm cannot: No real devices or payment method. (QA)\n";
    let s = person_gate::sink(ok);
    assert!(!s.defect(), "{s:?}");
    assert_eq!(s.text, ok);
}

#[test]
fn a_tool_failure_is_one_diagnosis_one_report_and_never_a_persons() {
    // The page check broken on this run's installation.
    let mut h = common::tool_health::Health::new(9_001_001);
    let mut ways = Vec::new();
    for _ in 0..3 {
        if outside_exec::use_page_check(&mut h) {
            // The first page check fails with the 400 the run saw.
            let d = h.failed("check_url", "the read API returned 400 Bad Request");
            assert_eq!(d, common::tool_health::Diagnosis::BadInput);
            ways.push("check_url, then the end-to-end run");
        } else {
            ways.push("the end-to-end run");
        }
    }
    assert_eq!(
        ways.iter().filter(|w| w.starts_with("check_url")).count(),
        1
    );
    let reports = h.take_reports();
    assert_eq!(reports.len(), 1, "reported once");
    assert!(reports[0].line().starts_with("CoderHelm's own problem"));
    assert!(h.take_reports().is_empty());
    // The steps that took the other way are done, and nobody is asked.
    let mut c = as_executed();
    c.steps[4].result =
        "passed on `abc1234` (read from the end-to-end run; the page check is down)".into();
    assert!(ow::left(&c).human.iter().all(|l| !l.contains("check")));
    // The planner hears of it.
    let p = common::tool_health::prompt_section(&common::tool_health::degraded_now(
        9_001_001,
        &["check_url", "query_content"],
    ));
    assert!(
        p.contains("`check_url`") && !p.contains("query_content"),
        "{p}"
    );
}

#[test]
fn a_failed_attempt_reaches_a_person_only_for_a_permission_or_a_credential() {
    let attempt = |why: &str| person_gate::Ask {
        kind: Kind::Attempt,
        text: "Confirm 'Deploy Preview' ran".into(),
        why_not: why.into(),
        ..Default::default()
    };
    let ctx = person_gate::Context::default();
    for why in [
        "ran `a page check`: the read API returned 400 Bad Request",
        "the repository has no production deploy workflow CoderHelm can start",
        "timed out",
    ] {
        assert_eq!(
            person_gate::decide(&attempt(why), &ctx).class(),
            Class::Do,
            "{why}"
        );
    }
    let refused = person_gate::decide(
        &attempt("CoderHelm's GitHub App is not allowed to start workflows in this repository"),
        &ctx,
    );
    assert!(matches!(refused, Outcome::Human(_)), "{refused:?}");
    // A step that names a workflow called "Deploy ..." is not a deploy.
    let read = person_gate::Ask {
        kind: Kind::Step,
        text: "Confirm 'Deploy Preview' and 'E2E on preview' ran".into(),
        ..Default::default()
    };
    assert_eq!(person_gate::decide(&read, &ctx).class(), Class::Do);
}

#[test]
fn a_reply_names_the_person_never_their_account_id() {
    use common::conversation::{reply_body, Heard, How};
    let to = Heard {
        id: "c1".into(),
        author: "557058:0f0e0000-aaaa-bbbb-cccc-000000000000".into(),
        author_id: "557058:0f0e0000-aaaa-bbbb-cccc-000000000000".into(),
        text: "Skip the H1 and intro copy. Just use the title.".into(),
        ..Default::default()
    };
    let b = reply_body(&to, &[], How::MentionQuote, "", "Done.");
    assert!(!b.contains("557058"), "{b}");
    assert!(
        b.starts_with("Replying to your comment (\"Skip the H1"),
        "{b}"
    );
    let named = Heard {
        author: "Sam Lee".into(),
        ..to.clone()
    };
    assert!(reply_body(&named, &[], How::MentionQuote, "", "Done.")
        .starts_with("Sam Lee, replying to your comment"));
    assert_eq!(common::conversation_io::jira_id_of(&to), to.author_id);
}

#[test]
fn the_recorded_plan_puts_every_step_on_coderhelm_but_the_approval_and_the_checkout() {
    // The planner's recorded outside steps for this ticket, replayed through
    // the plan check and the gate: what CoderHelm does, what waits on a
    // condition, and what a person is asked.
    let a = the_repo();
    let mut c = the_note();
    let gaps = ow::gaps(&c, &a, ow::PlanFacts::default()).join("\n");
    assert!(
        !gaps.contains("goes to a person without saying why"),
        "{gaps}"
    );
    ow::classify(&mut c);
    let person: Vec<&str> = c
        .steps
        .iter()
        .filter(|s| s.doer() == ow::Doer::Person)
        .map(|s| s.step.as_str())
        .collect();
    assert_eq!(person.len(), 2, "{person:?}");
    let asks = ow::left(&as_executed()).human;
    assert_eq!(asks.len(), 2);
    assert!(asks.iter().any(|a| a.contains("paid registration")));
    assert!(asks.iter().any(|a| a.contains("production release")));
    // A step written with no executor, or in other words, is CoderHelm's.
    assert_eq!(ow::Doer::parse(""), ow::Doer::CoderHelm);
    assert_eq!(ow::Doer::parse("sandbox"), ow::Doer::CoderHelm);
    assert_eq!(ow::Doer::parse("vendor"), ow::Doer::Person);
    // A person step for a CoderHelm tool's work goes back to the planner.
    let mut bad = the_note();
    bad.steps[2].by = "person".into();
    let g = ow::gaps(&bad, &a, ow::PlanFacts::default()).join("\n");
    assert!(g.contains("work CoderHelm's own tools do"), "{g}");
    assert!(g.contains("`reason`: one of credential"), "{g}");
}

// ── The whole history, replayed ─────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct History {
    steps: Vec<NoteStep>,
    questions: Vec<String>,
    lines: Vec<String>,
}

/// Words that mark an item as CoderHelm's own work whatever its wording.
const OWN_WORK: &[&str] = &[
    "check_url",
    "page check",
    "github_history",
    "workflow_runs",
    "add pr labels",
    "eslint",
    "400 bad request",
    "look at",
];

#[test]
fn zero_doable_items_reach_a_person_across_the_history() {
    // Every person item the team's runs produced (outside steps, the
    // outside reports' lines, the questions), names made up.
    let h: History =
        serde_json::from_str(include_str!("bk_history_person_items.json")).expect("fixture");
    let mut by_class: std::collections::BTreeMap<&str, usize> = Default::default();
    let mut human: Vec<(String, String)> = Vec::new();
    let mut tally = |o: &Outcome, text: &str| {
        let k = match o.class() {
            Class::Do => "doable",
            Class::Settled => "settled",
            Class::Decided => "decided",
            Class::Review => "review",
            Class::Human => "human",
        };
        *by_class.entry(k).or_default() += 1;
        if let Outcome::Human(h) = o {
            human.push((text.to_string(), h.reason().code().to_string()));
        }
    };
    // The outside steps as they would render now.
    for s in &h.steps {
        let mut c = OutsideChange {
            steps: vec![s.clone()],
            ..Default::default()
        };
        ow::classify(&mut c);
        let Some(st) = ow::step_now(&c, 0, true) else {
            continue;
        };
        let o = match st.state {
            State::Done { .. } | State::Waiting { .. } => {
                Outcome::Do(person_gate::Work::Own(st.what.clone()))
            }
            _ => person_gate::decide(&st.ask(), &Default::default()),
        };
        tally(&o, &st.line());
    }
    // The report lines exactly as people read them, through the sink.
    for l in &h.lines {
        tally(&person_gate::sink_outcome(l), l);
    }
    for q in &h.questions {
        let a = person_gate::Ask::new(Kind::Question, q);
        tally(&person_gate::decide(&a, &Default::default()), q);
    }
    let total = h.steps.len() + h.lines.len() + h.questions.len();
    eprintln!("history replay: {total} items, {by_class:?}");
    for (l, r) in &human {
        eprintln!("  human ({r}): {}", common::truncate_str(l, 160));
    }
    // The bar: nothing that is CoderHelm's own work is left for a person.
    // The action is read, not the reason a writer gave.
    for (l, _) in &human {
        let action = l.split("CoderHelm c").next().unwrap_or(l).to_lowercase();
        assert!(
            !OWN_WORK.iter().any(|w| action.contains(w)),
            "doable item reaches a person: {l}"
        );
    }
    assert_eq!(by_class.values().sum::<usize>(), total - skipped(&h));
}

/// History steps the pull request itself does: no item at all.
fn skipped(h: &History) -> usize {
    h.steps
        .iter()
        .filter(|s| s.doer() == ow::Doer::ThisPr)
        .count()
}
