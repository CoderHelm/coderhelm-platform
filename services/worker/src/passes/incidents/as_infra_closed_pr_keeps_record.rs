//! as. The same gap as `ap`, in the other store. An infrastructure finding
//! keeps its own record, and nothing released it either: a person closed
//! its fix pull request and the record stayed `pr_open`, so the
//! Infrastructure tab kept showing the closed pull request and never
//! offered "Open fix PR" again; a fix run that was cancelled or failed
//! left the record `fixing` for good (this store had no fallback at all);
//! and a reopened pull request was never noticed. Only a read of the AWS
//! account ever changed such a record.
//!
//! Guarantee: an infrastructure record is `fixing` only while its run
//! works and `pr_open` only while its pull request is open or merged. It
//! is the same reconciliation as for dependency findings, not a copy: one
//! `reconcile` over `common::security_release::Claim`, the same reads of
//! GitHub and the run records, the same events from the gateway, the same
//! sweep with its bounds, the same person-closed record and holds, the
//! same Retry. What is this store's own stays: a merged pull request
//! never closes a finding (only a read of the account does); a pull
//! request the replacement guard made a draft is not "closed" and is left
//! as it is; a finding that fell back to its runbook stays there until its
//! code or its target changes; and a pull request is recorded only after
//! the guard read it.

use chrono::{DateTime, Duration, Utc};
use common::infra_vuln::inventory::Location;
use common::infra_vuln::matching::{Fix, FixKind, InfraFinding};
use common::infra_vuln::records::{self, GuardStep, PrRef, Record, Status};
use common::security_release::{Claim, OpenPr, PrNow, RunNow};

use super::fixture::fn_src;
use crate::passes::infra_findings::newly_drafted;
use crate::passes::security_fixes::release::{fake::*, reconcile, Scope};

const REPO: &str = "acme/infra";
const TICKET: &str = "SEC-0A1B2C3D4E5F";
const PR: u64 = 58;
const REFUSED: &str =
    "db/main.tf, line 2: `aws_db_instance.orders` changes `identifier`, which replaces the resource";

fn now() -> DateTime<Utc> {
    at("2026-11-13T10:00:00Z")
}

/// The orders database's engine update, with a pull request to open.
fn finding() -> Record {
    Record::new(
        InfraFinding {
            id: "orders1".into(),
            name: "orders".into(),
            title: "orders: PostgreSQL 16.4 is behind 16.6".into(),
            fix: Fix {
                kind: Some(FixKind::Pr),
                to: "16.6".into(),
                location: Some(Location {
                    repo: REPO.into(),
                    file: "db/main.tf".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        },
        "2026-11-11T08:00:00Z",
    )
}

/// As it stood: the run's pull request open, the guard had nothing to say.
fn in_its_pull_request() -> Record {
    let mut r = finding();
    r.status = Status::PrOpen;
    r.ticket = TICKET.into();
    r.pr = Some(PrRef {
        number: PR,
        url: format!("https://github.com/{REPO}/pull/{PR}"),
        state: "open".into(),
        ..Default::default()
    });
    r.updated_at = "2026-11-12T09:00:00Z".into();
    r
}

/// What the page needs to offer the fix again: an open finding with a pull
/// request to make and no pull request standing in the way.
fn offers_the_fix(r: &Record) -> bool {
    r.status == Status::Open
        && r.view()["fix"]["kind"] == "pr"
        && r.pr.as_ref().is_none_or(|p| p.state == "closed")
}

#[tokio::test]
async fn a_closed_pull_request_gives_the_record_back_with_who_closed_it() {
    let mut recs = vec![in_its_pull_request()];
    assert!(!offers_the_fix(&recs[0]), "the button was hidden");
    let github = Fake::default();
    github.pr(REPO, PR, closed_by("octo-admin"));

    // The sweep, with no webhook: one read, the record released.
    let r = reconcile(&github, &mut recs, &Scope::All, now()).await;
    assert_eq!((r.prs_read, r.released), (1, 1));
    let rec = &recs[0];
    assert!(offers_the_fix(rec));
    let v = rec.view();
    assert_eq!(v["pr"]["state"], "closed");
    assert_eq!(v["pr"]["closed_by"], "octo-admin");
    assert_eq!(v["pr"]["closed_at"], "2026-11-13T09:30:00Z");
    assert!(github.event_texts()[0].contains("PR #58 was closed without merging by octo-admin"));
    // No run starts by itself for a week; a person asking never waits
    // (`start_fixes` skips the hold only for the finding asked for).
    assert!(rec.on_hold(now() + Duration::days(6)));
    assert!(!rec.on_hold(now() + Duration::days(8)));

    // Idempotent, and the webhook's event ends in the same place.
    let saves = github.saves();
    reconcile(&github, &mut recs, &Scope::All, now()).await;
    assert_eq!(github.saves(), saves);
    let mut by_event = vec![in_its_pull_request()];
    let scope = Scope::Pr {
        repo: REPO.into(),
        number: PR,
    };
    reconcile(&github, &mut by_event, &scope, now()).await;
    assert_eq!(by_event, recs);

    // Reopened by a person: the record is in it again.
    github.pr(REPO, PR, PrNow::Open);
    let r = reconcile(&github, &mut recs, &Scope::All, now()).await;
    assert_eq!(r.attached, 1);
    assert_eq!(recs[0].status, Status::PrOpen);
    assert!(!recs[0].on_hold(now()));
}

#[tokio::test]
async fn a_cancelled_or_failed_run_no_longer_keeps_the_record_fixing() {
    for (status, by, days) in [
        ("cancelled", Some("admin@example.com"), 7),
        ("failed", None, 1),
    ] {
        let mut r = finding();
        r.claim(TICKET, at("2026-11-13T09:00:00Z"));
        let mut recs = vec![r];
        let runs = Fake::default();
        runs.run(
            TICKET,
            RunNow::Ended {
                status: status.into(),
                by: by.map(String::from),
            },
        );
        let scope = Scope::Ticket(TICKET.into());
        let rep = reconcile(&runs, &mut recs, &scope, now()).await;
        assert_eq!(rep.released, 1, "{status}");
        assert!(offers_the_fix(&recs[0]), "{status}");
        assert!(recs[0].on_hold(now() + Duration::days(days) - Duration::hours(1)));
        assert!(!recs[0].on_hold(now() + Duration::days(days) + Duration::hours(1)));
    }
    // GitHub or the run records not answering releases nothing.
    let mut r = finding();
    r.claim(TICKET, at("2026-11-13T09:00:00Z"));
    let mut recs = vec![r];
    let down = Fake {
        branches_down: true,
        ..Default::default()
    };
    reconcile(&down, &mut recs, &Scope::All, now()).await;
    assert_eq!(recs[0].status, Status::Fixing);
}

#[tokio::test]
async fn the_guards_draft_is_not_closed_and_the_runbook_stays() {
    // The guard tripped twice: a draft with a hold, the finding on its
    // runbook, exactly as the guard left it.
    let mut r = in_its_pull_request();
    assert_eq!(r.guard_step(&[REFUSED.to_string()]), GuardStep::FixRound);
    assert_eq!(r.guard_step(&[REFUSED.to_string()]), GuardStep::Draft);
    r.status = Status::Open;
    r.pr.as_mut().unwrap().state = "draft".into();
    let mut recs = vec![r];
    let before = recs.clone();
    let github = Fake::default();
    // GitHub says a draft is open.
    github.pr(REPO, PR, PrNow::Open);
    let rep = reconcile(&github, &mut recs, &Scope::All, now()).await;
    assert_eq!((rep.prs_read, rep.changed(), github.saves()), (1, 0, 0));
    assert_eq!(recs, before, "a held draft is neither closed nor reopened");
    assert_eq!(recs[0].view()["fix"]["kind"], "runbook");
    assert!(!offers_the_fix(&recs[0]));

    // A person closes the draft: said, and the runbook still stands, with
    // no attempt by itself, until the code or the target changes.
    github.pr(REPO, PR, closed_by("octo-admin"));
    reconcile(&github, &mut recs, &Scope::All, now()).await;
    let r = &recs[0];
    assert_eq!(r.pr.as_ref().unwrap().state, "closed");
    assert!(r.drafted && r.guard_blocks());
    assert_eq!(r.view()["fix"]["kind"], "runbook");
    assert!(!offers_the_fix(r));
    let mut changed = r.clone();
    changed.finding.fix.to = "16.7".into();
    assert!(!changed.guard_blocks());
}

#[tokio::test]
async fn a_fix_round_that_dies_becomes_the_guards_second_step_never_a_ready_pull_request() {
    let mut r = in_its_pull_request();
    assert_eq!(r.guard_step(&[REFUSED.to_string()]), GuardStep::FixRound);
    r.status = Status::Fixing;
    let mut recs = vec![r];
    let github = Fake::default();
    github.pr(REPO, PR, PrNow::Open);
    github.branch_prs.lock().unwrap().insert(
        TICKET.into(),
        OpenPr {
            number: PR,
            url: "u".into(),
        },
    );
    github.run(
        TICKET,
        RunNow::Ended {
            status: "failed".into(),
            by: None,
        },
    );
    reconcile(&github, &mut recs, &Scope::All, now()).await;
    let r = &recs[0];
    assert_eq!(
        (r.status, r.pr.as_ref().unwrap().state.as_str()),
        (Status::Open, "draft")
    );
    assert!(r.drafted && r.guard_blocks());
    // The pull request is made a draft on GitHub too, never closed.
    assert_eq!(newly_drafted(r), Some((REPO.to_string(), PR)));
    let line = github.event_texts().pop().unwrap();
    assert!(
        line.contains("PR #58 is now a draft") && !line.contains("closed"),
        "{line}"
    );
}

#[test]
fn one_reconciliation_serves_both_stores() {
    // Both stores are a `Claim`; nothing of the rules is copied.
    fn is_claim<C: Claim>() {}
    is_claim::<Record>();
    is_claim::<(
        common::security_fixes::Finding,
        common::security_fixes::FindingState,
    )>();
    let infra = include_str!("../infra_findings.rs");
    let rel = include_str!("../security_release.rs");
    assert_eq!(rel.matches("async fn reconcile").count(), 1);
    assert!(!infra.contains("async fn reconcile"));
    assert!(fn_src(infra, "pub(crate) async fn release(").contains("rel::reconcile("));
    // The same reads of GitHub and the run records.
    assert!(infra.contains("self.reads.pr_now(") && infra.contains("self.reads.run_now("));

    // The sweep and the events reach this store through the same calls.
    let fixes = include_str!("../security_fixes.rs");
    let tick = fn_src(fixes, "async fn team_tick(");
    let at = tick.find("infra_findings::release(").expect("the sweep");
    assert!(at < tick.find("let Some(kev_map) = kev_map else").unwrap());
    let events = fn_src(fixes, "async fn event_for_team(");
    assert!(events.contains("infra_findings::release(state, team_id, &cx.gh, &scope)"));

    // A person's request (the button, and Retry) checks the record against
    // GitHub first, and only the finding asked for skips the hold.
    let on_event = fn_src(infra, "pub async fn on_event(");
    assert!(
        on_event.find("release(state, team, &gh, &scope)").unwrap()
            < on_event.find("start_fixes(").unwrap()
    );
    let start = fn_src(infra, "async fn start_fixes(");
    assert!(start.contains("if r.on_hold(Utc::now()) && !asked {"));
    let gateway = include_str!("../../../../gateway/src/routes/security.rs");
    assert!(fn_src(gateway, "pub(crate) async fn retry_event(")
        .contains("super::infra_security::retry_of("));

    // A pull request is recorded only through the guard.
    assert!(fn_src(infra, "    async fn opened(").contains("on_pr_opened("));
    let _ = records::GUARD_STOPPED;
}
