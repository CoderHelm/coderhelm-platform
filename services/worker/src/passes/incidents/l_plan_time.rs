//! k. A ticket about a wrong class time (a schedule data fix) failed in the
//! plan pass after about 14 minutes: "The step reached its time limit and
//! the run couldn't continue in a fresh step: no work is committed to
//! continue from." It planned (with two reviewers and a revision), switched
//! repository and planned again there, was told by the release check that
//! "the change removes `updated_at`, which deployed code reads" and by the
//! plan check that production and staging were unproven, and was sent to
//! plan a third time for one file tag that didn't match. The third plan ran
//! out of time, and planning had nothing on a branch to continue from.
//!
//! Guarantee: planning that runs out of time continues in a fresh step at
//! the plan gate, from how far it got (the repository it settled on, its
//! last plan, its open gaps and its re-plans), and keeps going while each
//! step makes a new plan revision or leaves fewer open gaps. The checks
//! re-plan once in all: a repository switch plans once, with the file check
//! in that plan's one send-back, and every check's gaps share that one
//! send-back. A data fix that writes a column (`SET updated_at = now()`) is
//! not a removal, and an environment the plan's claims don't rest on owes
//! no environment-only proof.

use super::fixture::*;
use crate::models::TicketMessage;
use crate::passes::fix_location::{EnvCheck, FixDecision, FixEvidence};
use crate::passes::plan::PlanResult;
use crate::passes::release_plan::{self, FieldChange, Mechanics, Release};
use crate::passes::steps::{self, Gate, PlanState};
use crate::passes::{plan_judge, ticket_history::TicketHistory};

const OLD: &str = "no work is committed to continue from";

/// The schedule data fix the planner wrote: one migration that sets the
/// right start time and bumps `updated_at`, with the release block the old
/// check pushed it to write, and both environments listed as unchecked.
fn schedule_fix() -> PlanResult {
    PlanResult {
        proposal: "Correct the Saturday Speed and Agility class from 9am to 8am.".into(),
        design: "A data migration runs UPDATE class_schedule SET start_time = '08:00', \
                 updated_at = now() WHERE class_id = 42. No schema change."
            .into(),
        tasks: "- [ ] src/database/migrations/1783400000000-FixClassTime.ts: [create] Set the \
                class's start time to 08:00 and bump updated_at\n"
            .into(),
        spec: "Given the class page, the Saturday time reads 8am.".into(),
        repo_tasks: vec![],
        fix: Some(FixDecision {
            cause: "The class is stored with a 9am start; the ticket says 8am.".into(),
            evidence: FixEvidence {
                data_shows: "class_schedule row 42 has start_time 09:00".into(),
                environments: vec![
                    EnvCheck {
                        env: "production".into(),
                        proof: String::new(),
                        proven: false,
                    },
                    EnvCheck {
                        env: "staging".into(),
                        proof: String::new(),
                        proven: false,
                    },
                ],
                ..Default::default()
            },
            release: Some(Release {
                fields: vec![FieldChange {
                    old: vec!["updated_at".into()],
                    new: String::new(),
                    types: vec!["class_schedule".into()],
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}

/// One run's planning steps. `revisions[i]` and `gaps[i]` are how far
/// planning got when step `i + 1` ran out of time (its plan revision and
/// open gaps); the last step finishes planning.
fn plan_steps(revisions: &[u32], gaps: &[usize]) -> (Ending, Vec<TicketMessage>) {
    let mut end = Ending::default();
    let first = TicketMessage {
        team_id: "TEAM#1".into(),
        ticket_id: TICKET.into(),
        repo_owner: OWNER.into(),
        repo_name: "web".into(),
        ..Default::default()
    };
    let mut sent = vec![first.clone()];
    let mut msg = first;
    let mut mark: Option<String> = None;
    let mut stalled = 0;
    let mut ps = PlanState::new(OWNER, "web", BASE);
    for (i, (&rev, &g)) in revisions.iter().zip(gaps).enumerate() {
        let step = steps::step_number(msg.continuation);
        // The step starts: it compares how far planning got with the mark
        // of the step before (see `run_passes`).
        if msg.continuation > 0 {
            stalled = steps::plan_stalled_after(mark.as_deref(), &ps, stalled);
            if steps::stops_for_stall(stalled) {
                end.said(steps::plan_stall_reason(step, &ps));
                end.status = common::run_status::FAILED.into();
                return (end, sent);
            }
            mark = Some(ps.mark());
        }
        // Step 1 plans in the first repository and switches; the plan made
        // there is the switch's one re-plan.
        if i == 0 {
            ps.switched = true;
            ps.replans = 1;
            ps.repo = format!("{OWNER}/{REPO}");
        }
        ps.revision = rev;
        ps.open_gaps = (0..g).map(|n| format!("gap {n}")).collect();
        let last = i + 1 == revisions.len();
        if last {
            ps.checked = true;
            end.said("Plan generated");
            end.status = common::run_status::RUNNING.into();
            return (end, sent);
        }
        // Out of time while planning: the checkpoint holds the plan gate.
        let st = steps::StepState {
            gate: Some(Gate::Plan),
            plan: Some(ps.clone()),
            ..Default::default()
        };
        let planning = st.planning().is_some();
        assert!(planning, "step {step} hands off at the plan gate");
        assert!(steps::under_ceiling(msg.continuation));
        msg = steps::next_message(&msg, "RUN1", 1);
        // The next step resumes in the repository planning settled on.
        let resumed = steps::StepState {
            gate: Some(Gate::Plan),
            plan: Some(PlanState::from_json(&ps.to_json()).unwrap()),
            ..Default::default()
        }
        .planning()
        .unwrap();
        let (owner, name) = resumed.owner_name().unwrap();
        msg.repo_owner = owner.into();
        msg.repo_name = name.into();
        sent.push(msg.clone());
        end.said(format!(
            "This step reached its time limit while planning: continuing in step {} with its own \
             time budget",
            steps::step_number(msg.continuation)
        ));
    }
    (end, sent)
}

#[test]
fn planning_that_runs_out_of_time_continues_at_the_plan_gate() {
    // Step 1: two plans (first repository, then the switch) and out of
    // time; step 2: the file re-plan's plan, final.
    let (end, sent) = plan_steps(&[2, 3], &[2, 0]);
    assert_eq!(end.status, common::run_status::RUNNING, "{:?}", end.words);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].continuation, 1);
    assert_eq!(sent[1].continuation_run_id.as_deref(), Some("RUN1"));
    assert_eq!(
        sent[1].repo_name, REPO,
        "resumes in the repository it switched to"
    );
    assert!(end.says("while planning: continuing in step 2"));
    end.never_says(OLD);
    // Fewer open gaps with no new revision is progress too.
    let (end, _) = plan_steps(&[2, 2, 2], &[3, 1, 1]);
    assert_eq!(end.status, common::run_status::RUNNING, "{:?}", end.words);
}

#[test]
fn two_planning_steps_without_progress_stop_saying_where() {
    let (end, _) = plan_steps(&[2, 2, 2, 2], &[1, 1, 1, 1]);
    assert_eq!(end.status, common::run_status::FAILED);
    assert!(end.says("made no progress in planning"), "{:?}", end.words);
    assert!(end.says(&format!("{OWNER}/{REPO}")));
    end.never_says(OLD);
}

#[test]
fn the_pipeline_hands_planning_off_and_bounds_the_replans() {
    let src = include_str!("../mod.rs");
    // The timeout continues a run at the plan gate with nothing committed.
    let cont = fn_src(src, "\nasync fn try_auto_continue(");
    assert!(cont.contains("let has_partial = planning"));
    let orch = fn_src(src, "\npub async fn orchestrate_ticket(");
    let timeout = orch
        .find("Orchestration timed out — Lambda limit approaching")
        .unwrap();
    let arm = &orch[timeout..];
    assert!(arm.contains(".planning()"));
    assert!(arm.contains("This step reached its time limit while planning"));
    // Planning saves how far it got, and resumes from it.
    let rp = run_passes_src();
    assert!(rp.contains("steps::load(state, &msg.team_id, run_id).await"));
    assert!(rp.contains("steps::plan_stalled_after("));
    assert!(rp.matches("steps::save_plan(").count() >= 5);
    // A switch is refused once planning switched or used its re-plan; a
    // file mismatch re-plans only while one is left; both later plans
    // check their files in their one send-back.
    assert!(rp.contains(".filter(|_| ps.switched || !ps.may_replan())"));
    assert!(rp.contains("&& ps.may_replan()"));
    assert_eq!(steps::MAX_PLAN_REPLANS, 1);
    let mut p = PlanState::new(OWNER, REPO, BASE);
    p.switched = true;
    p.replans = 1;
    assert!(!p.may_replan(), "the switch used the one re-plan");
    let plan = include_str!("../plan.rs");
    let run = fn_src(plan, "\npub async fn run(");
    let files = run.find("super::plan_file_gaps(").unwrap();
    let send_back = run.find("super::fix_location::send_back(&back)").unwrap();
    assert!(files < send_back, "file gaps join the one send-back");
}

#[test]
fn a_schedule_data_fix_owes_no_release_split_and_no_environment_proof() {
    let plan = schedule_fix();
    let g = release_plan::plan_gaps(&plan, &Mechanics::default());
    assert!(
        !g.iter().any(|x| x.contains("removes `updated_at`")),
        "{g:?}"
    );
    assert!(
        !g.iter().any(|x| x.contains("Plan two migrations")),
        "{g:?}"
    );
    // No release at all: a data migration that removes nothing needs none.
    let mut bare = schedule_fix();
    bare.fix.as_mut().unwrap().release = None;
    assert!(release_plan::plan_gaps(&bare, &Mechanics::default()).is_empty());
    // Its claims are about one stored row, not an environment.
    let found = plan_judge::structural(&plan, &TicketHistory::default(), true);
    assert!(
        !found.iter().any(|f| f.point == "env_unproven"),
        "{found:?}"
    );
}
