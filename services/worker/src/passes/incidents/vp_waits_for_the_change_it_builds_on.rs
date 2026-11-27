//! vp. A ticket flipped a flag in a file another ticket's run was still
//! creating, and that run hadn't merged. The planner saw it correctly:
//! the file and what it configures didn't exist yet in any environment.
//! The plan check rejected the plan as "a change outside the code that
//! leaves nothing for a person and waits for nothing", planned again at a
//! cost, and ended the run failed. It did wait for something: the other
//! ticket's change.
//!
//! Guarantee: before the plan check rejects anything as waiting for
//! nothing, it looks for the change that makes the ticket's target. With
//! one, the run waits for it (never failed, never a plan-check failure):
//! the dependency is kept on the run and shown on its page, the ticket
//! hears one note, and a merge plans again against the new base and goes
//! on, with nothing planned before then and the note never repeated. A
//! dependency's stopped run is said on the page; one closed without
//! merging stops the run as "needs you". Runs left failed this way are
//! set waiting by the self-heal sweep.

use super::fixture::*;
use crate::passes::recover::waits_for_another_change;
use common::change_wait::{self as cw, Dependency, OtherWork, Settled};

/// The planner's cause, as it wrote it (names invented).
const CAUSE: &str = "The ticket flips dry_run in envs/prod/job.tf, but that file, the job \
    (TK-20) and the incidents table it writes don't exist yet in any environment, so neither \
    the flip nor its staging gate can be done.";
const TICKET: &str = "TK-21";
const BODY: &str = "In infra-live envs/prod/job.tf set dry_run = false once staging shows one \
    email. A follow-up of TK-20 (Build the scheduled job), filed by CoderHelm.";

/// The run waiting, step by step, with what it spends and says.
#[derive(Default)]
struct Waiter {
    plans: u32,
    ticket_notes: Vec<String>,
    page: Vec<String>,
    kept: Option<Dependency>,
    status: &'static str,
}

impl Waiter {
    /// The plan check: the target is missing, so it looks for the change
    /// that makes it before it rejects anything.
    fn plan_check(&mut self, work: &[OtherWork]) {
        self.plans += 1;
        match cw::find(CAUSE, BODY, &["TK-20".into()], TICKET, work, "t0") {
            Some(dep) => self.begin(dep),
            None => self.status = "failed",
        }
    }

    /// `change_wait::begin`.
    fn begin(&mut self, mut dep: Dependency) {
        let note = cw::should_note(self.kept.as_ref(), &dep);
        dep.noted = true;
        if note {
            self.ticket_notes.push(cw::note(&dep));
            self.page.push(cw::note(&dep));
        }
        self.kept = Some(dep);
        self.status = "awaiting_change";
    }

    /// `change_wait::check_one`.
    fn check(&mut self, pr: Option<(&str, bool)>, dep_run: Option<&str>) {
        let dep = self.kept.clone().unwrap();
        match cw::settle(pr, dep_run) {
            Settled::Waiting => {}
            Settled::DependencyStuck(why) => {
                let line = cw::stuck_line(&dep, &why);
                if self.page.last() != Some(&line) {
                    self.page.push(line);
                }
            }
            Settled::ClosedUnmerged => {
                self.ticket_notes.push(cw::closed_reason(&dep));
                self.status = "needs_input";
            }
            Settled::Merged => {
                // Planned again against the new base, then on.
                self.plans += 1;
                self.status = "running";
            }
        }
    }
}

fn building(pr: u64, status: &str) -> Vec<OtherWork> {
    vec![OtherWork {
        ticket: "TK-20".into(),
        run_id: "R20".into(),
        repo: "acme/infra-live".into(),
        pr,
        status: status.into(),
    }]
}

#[tokio::test]
async fn the_run_waits_for_the_change_then_goes_on_when_it_merges() {
    let mut w = Waiter::default();
    w.plan_check(&building(0, "running"));
    assert_eq!(w.status, "awaiting_change", "never failed");
    assert_eq!(
        w.ticket_notes,
        vec!["Waiting for TK-20 to merge; this run continues by itself after that."]
    );
    // While it waits: sweeps, a redelivered step, the dependency's run
    // stopping and coming back. No re-plan, no second note.
    w.check(None, Some("running"));
    w.begin(cw::find(CAUSE, BODY, &[], TICKET, &building(0, "running"), "t1").unwrap());
    w.check(None, Some("failed"));
    w.check(None, Some("failed"));
    assert!(w
        .page
        .last()
        .unwrap()
        .starts_with("Still waiting for TK-20: the run building it stopped"));
    w.check(Some(("open", false)), Some("awaiting_ci"));
    assert_eq!(w.plans, 1, "no plan spend before the merge");
    assert_eq!(w.ticket_notes.len(), 1, "one note");
    // The pull request merges: planned again, and the run goes on.
    w.check(Some(("closed", true)), Some("merged"));
    assert_eq!(w.status, "running");
    assert_eq!(w.plans, 2);
    assert_eq!(w.ticket_notes.len(), 1, "no duplicate note");
}

#[tokio::test]
async fn a_dependency_closed_without_merging_stops_for_a_person() {
    let mut w = Waiter::default();
    w.plan_check(&building(12, "awaiting_ci"));
    assert_eq!(
        w.kept.as_ref().unwrap().label(),
        "TK-20 (acme/infra-live#12)"
    );
    w.check(Some(("closed", false)), Some("closed"));
    assert_eq!(w.status, "needs_input");
    assert_eq!(w.ticket_notes.len(), 2);
    assert!(w.ticket_notes[1].contains("closed without merging"));
    assert_eq!(w.plans, 1);
}

#[test]
fn the_failed_record_is_set_waiting_by_the_sweep() {
    let error = format!(
        "CoderHelm couldn't write a usable plan for this ticket: the plan stopped on a change \
         outside the code that leaves nothing for a person and waits for nothing ({CAUSE}), \
         after asking the planner again."
    );
    let item = run_item(&[
        ("team_id", s("TEAM#t1")),
        ("run_id", s("R21")),
        ("status", s("failed")),
        ("current_pass", s("plan")),
        ("failed_pass", s("plan")),
        ("repo", s("acme/infra-live")),
        ("ticket_id", s(TICKET)),
        ("ticket_body", s(BODY)),
        ("run_step", n(2)),
        ("installation_id", n(7)),
        ("error_message", s(&error)),
    ]);
    assert!(waits_for_another_change(&item, "release-2"));
    // An unrelated plan failure is not.
    let mut other = item.clone();
    other.insert(
        "error_message".into(),
        s("the plan stopped on a change in the vendor's dashboard"),
    );
    assert!(!waits_for_another_change(&other, "release-2"));
}

#[test]
fn the_plan_check_looks_for_the_change_before_it_rejects() {
    let src = include_str!("../mod.rs");
    let finish = fn_src(src, "async fn finish_without_code(");
    let dep = finish
        .find("change_wait::dependency_for(")
        .expect("the dependency is looked for");
    let reject = finish.find("waits for nothing").expect("the rejection");
    assert!(dep < reject, "looked for before anything is rejected");
    assert!(finish[dep..reject].contains("change_wait::begin("));
    let wait = include_str!("../change_wait.rs");
    let begin = fn_src(wait, "pub async fn begin(");
    assert!(begin.contains("cw::should_note("), "one note per wait");
    assert!(begin.contains("keep_resume_message("));
    let check = fn_src(wait, "async fn check_one(");
    let merged = check.find("Settled::Merged =>").unwrap();
    assert!(
        check[merged..].contains("unmark_plan_run("),
        "planned again only after the merge"
    );
    assert!(!check[..merged].contains("unmark_plan_run("));
    // A merge or close asks the waiting runs; the sweep is the backstop.
    let gw = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    assert!(gw.contains("WorkerMessage::ChangeWaitCheck("));
    let rec = include_str!("../recover.rs");
    assert!(rec.contains("super::change_wait::check_all(state, None)"));
    assert!(rec.contains("waits_for_another_change(&item, version)"));
}
