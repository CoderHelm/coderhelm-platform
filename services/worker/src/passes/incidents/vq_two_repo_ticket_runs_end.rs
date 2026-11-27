//! vq. Two related tickets: one for a setting in an infrastructure
//! repo ("harbor-infra"), one for the web page that reads it ("harbor").
//! Both repos were connected.
//!
//! - The infra ticket said `Repo: harbor/harbor-infra`.
//!   Routing picked that repo, then the plan's repo check counted more
//!   mentions of the web repo in the plan and moved the run there. With
//!   nothing to change in the web repo, it built a large end-to-end test as
//!   stand-in work.
//! - CoderHelm's own review called the test out of proportion and a fix
//!   round cut it down. The scope judge called the removals out of scope,
//!   twice, so nothing was pushed; a review comment's quoted template line
//!   (`Wider than the ticket: <path>: <why>`) ran as the test command. Then
//!   the planned-work check asked for the plan's full test again, the round
//!   that tried was refused for undoing the review's fix, and after two
//!   rounds with no progress the run failed into a hand-off with an unknown
//!   error.
//! - The web ticket's run planned the infra change as a CoderHelm step "PR in
//!   harbor/harbor-infra", which no executor can do. An earlier step waited
//!   for a workflow its label never started, with no end, while
//!   the CI wait kept an old head's pending checks and the note
//!   said nothing was needed from anyone. Re-running failed jobs got a 403
//!   for the missing Actions permission, once per workflow run.
//!
//! Guarantees: the repo a ticket names is the run's repo and the repo check
//! never moves it; a change the plan puts in another connected repo is that
//! repo's linked pull request in the same run; a step asking for a pull
//! request elsewhere is never run here; removals a comment asks for stay;
//! a quoted template is never a command; what a review settled is not
//! planned work left; every outside wait has an end and says what it waits
//! on; the CI wait follows the head; the missing permission is one item.

use super::fixture::fn_src;
use crate::passes::fix_location::{EndToEndStep, FixDecision, FixLocation, OutsideChange};
use crate::passes::outside_work::{NoteStep, Placement, StepRun};
use crate::passes::repo_pick::{ticket_named_repo, TeamRepo};
use crate::passes::{finish_planned, infra_repo, outside_exec, outside_work, round_agree};

fn repos() -> Vec<TeamRepo> {
    vec![
        TeamRepo {
            full_name: "harbor/harbor".into(),
            enabled: true,
        },
        TeamRepo {
            full_name: "harbor/harbor-infra".into(),
            enabled: true,
        },
    ]
}

#[test]
fn the_repo_the_ticket_names_is_the_runs_repo() {
    let body = "Summary\nRepo: harbor/harbor-infra. Add \"theme\" to the feature list. \
                The page in harbor/harbor already reads it; harbor/harbor needs nothing.";
    assert_eq!(
        ticket_named_repo(
            "Add `theme` to the feature list (harbor-infra)",
            body,
            &repos()
        )
        .as_deref(),
        Some("harbor/harbor-infra")
    );
    // Routing reads it before the project default and the model's pick,
    // and the plan's repo check stays in it.
    let src = include_str!("../mod.rs");
    let named = src.find("Repo named on the ticket itself").unwrap();
    let ticket_pr = src.find("Repo from the ticket's own pull request").unwrap();
    let mapping = src.find("\"Repo from Jira project mapping\"").unwrap();
    assert!(named < ticket_pr && named < mapping);
    assert!(src.contains("but the ticket names this one — staying"));
}

#[test]
fn a_change_planned_for_another_connected_repo_is_its_linked_pull_request() {
    let info: Vec<infra_repo::RepoInfo> = repos()
        .into_iter()
        .map(|r| infra_repo::RepoInfo {
            full_name: r.full_name,
            enabled: r.enabled,
            ..Default::default()
        })
        .collect();
    let d = FixDecision {
        fix_location: FixLocation::Both,
        outside_change: Some(OutsideChange {
            system: "the CDN through harbor-infra".into(),
            change: "Add \"theme\" to the feature list".into(),
            where_: "PR in harbor/harbor-infra; CI applies it on merge".into(),
            ..Default::default()
        }),
        end_to_end: vec![EndToEndStep {
            step: "Add theme to the feature list".into(),
            by: "other_repo".into(),
            owner: String::new(),
            repo: "harbor/harbor-infra".into(),
        }],
        ..Default::default()
    };
    assert_eq!(
        infra_repo::named_other_repo(&d, "harbor/harbor", &info).as_deref(),
        Some("harbor/harbor-infra")
    );
    let src = include_str!("../mod.rs");
    assert!(src.contains("infra_repo::link_named_change("));
    // A step saved before this asks for that pull request: never run here.
    let c = OutsideChange {
        placed: Some(Placement {
            repo: "harbor/harbor".into(),
            branch: "coderhelm/theme".into(),
            pr_number: 12,
        }),
        ..Default::default()
    };
    let s = NoteStep {
        step: "Open the infra PR with the feature list entry; run its checks".into(),
        how: "PR in harbor/harbor-infra; CI lint and plan".into(),
        by: "coderhelm".into(),
        kind: "other".into(),
        ..Default::default()
    };
    assert!(matches!(
        outside_work::action_for(&s, &outside_work::Abilities::default(), &c),
        outside_work::Action::Cannot(_)
    ));
}

#[test]
fn the_review_fix_round_keeps_the_cut_and_runs_no_template() {
    use crate::passes::review_fix::{scope, suite_shell};
    let finding = "A test out of proportion to the change. If the ticket truly needs it, say \
                   why on a line `Wider than the ticket: e2e/tests/theme.spec.ts: <why>`.";
    assert_eq!(suite_shell::requester_command(&[finding]), None);
    assert!(scope::asks_for_less(
        "A test out of proportion to the change"
    ));
    let mod_src = include_str!("../review_fix/mod.rs");
    let step = fn_src(mod_src, "\nasync fn scope_step(");
    assert!(step.contains("scope::keep_asked_removals("));
}

#[test]
fn what_the_review_settled_is_not_planned_work_left() {
    let item = "The task for e2e/tests/theme.spec.ts is only partly done: not Test 2 \
                (`?theme=dark` turns on Dark) and not Test 3";
    let taken = round_agree::settled_by_review(&[
        "A test out of proportion to the change (`e2e/tests/theme.spec.ts:1`)".to_string(),
    ]);
    assert_eq!(
        round_agree::sort(&[item.to_string()], &taken, &[])
            .skipped
            .len(),
        1
    );
    // A round refused for undoing the review fix ends ready, never stalled.
    let refusal = round_agree::undo_refusal(&[round_agree::Undo {
        sha: "0f0f0f0aaaa".into(),
        path: "e2e/tests/theme.spec.ts".into(),
        lines: vec!["test('dark theme')".into(); 3],
    }]);
    let refused: Vec<crate::passes::write_guard::Refusal> = round_agree::undone_files(&refusal)
        .into_iter()
        .map(|t| crate::passes::write_guard::Refusal {
            path: t.path,
            why: format!("{} ({})", round_agree::UNDO_MARK, t.sha),
        })
        .collect();
    let (settled, other) = finish_planned::split_undone(&refused);
    let after = finish_planned::drop_settled(vec![item.to_string()], &settled);
    let r = finish_planned::first_round_of(&[item.to_string()], chrono::Utc::now()).unwrap();
    assert_eq!(
        finish_planned::next(&[item.to_string()], Some(&after), 0, &other, &r),
        finish_planned::Next::Ready
    );
    let src = include_str!("../resume.rs");
    let recheck = fn_src(src, "\nasync fn recheck_planned_work(");
    assert!(recheck.contains("review_settled("));
    let after_round = fn_src(src, "\nasync fn after_planned_round(");
    assert!(after_round.contains("finish_planned::split_undone("));
}

#[test]
fn the_outside_wait_ends_follows_the_head_and_says_what_it_waits_on() {
    let mut s = NoteStep {
        step: "Run the e2e on the preview".into(),
        by: "coderhelm".into(),
        kind: "e2e".into(),
        run: Some(StepRun {
            state: outside_exec::RUNNING.into(),
            workflow: ".github/workflows/deploy-staging.yml".into(),
            started_at: "2026-01-05T10:00:00Z".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let c = OutsideChange {
        steps: vec![s.clone()],
        ..Default::default()
    };
    assert!(outside_exec::waiting_line(&c)
        .unwrap()
        .contains("stops waiting at"));
    let later = chrono::DateTime::parse_from_rfc3339("2026-01-06T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert!(outside_exec::end_if_overdue(&mut s, later));
    assert!(!outside_work::under_way(&s));
    // The stale head's pending checks are dropped for the current head.
    let old = common::ci_watch::Wait {
        head: "4d4d4d4".into(),
        pending: vec!["E2E (in_progress)".into()],
        since: 1,
        reran: false,
    };
    let now = common::ci_watch::for_head(Some(&old), "b2b2b2b", 100);
    assert!(now.pending.is_empty() && now.head == "b2b2b2b");
    let src = include_str!("../resume.rs");
    let wait = src
        .find("\"An outside step waits on an approval or a workflow run — checking again later\"")
        .unwrap();
    assert!(src[wait..wait + 1400].contains("track_ci_for_head("));
    let hand_off = fn_src(src, "\nasync fn hand_off_after_ci_wait(");
    assert!(hand_off.contains("end_all_waits(") && hand_off.contains("REMOVE ci_resume_polls"));
    let exec = include_str!("../outside_exec.rs");
    let on_resume = fn_src(exec, "\npub async fn on_resume(");
    assert!(on_resume.contains("merged before it ended"));
}

#[test]
fn a_missing_actions_permission_is_one_status_and_no_loop() {
    let src = include_str!("../resume.rs");
    for f in [
        "\nasync fn try_rerun_once(",
        "\nasync fn rerun_unrelated(",
        "\nasync fn ci_wait_watchdog(",
    ] {
        let body = fn_src(src, f);
        assert!(body.contains("actions_write_allowed("), "{f}");
        assert!(body.contains("actions_write_missing("), "{f}");
    }
    assert!(outside_exec::actions_write_note().contains("Starting workflows"));
}
