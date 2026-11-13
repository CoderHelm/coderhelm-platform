//! ap. An admin closed CoderHelm's dependency fix pull request for one
//! repository on GitHub to start again, and cancelled its fix run. On the
//! Security page, filtered to that repository, every checkbox was off,
//! the header's too, with nothing to say why: 274 of the repository's
//! findings still read "in a pull request" a day after it was closed (3
//! were open, with no fix yet), and a finding in a pull request cannot be
//! picked. Nothing told the findings: the pull request webhook recorded
//! the close on the run and stopped there, cancelling a run wrote only the
//! run, and the one place that compared a finding with GitHub was the last
//! step of the scheduled sweep, behind the time every team's sources and
//! triage had left, reading any failed request as "still open".
//!
//! Guarantee: a finding is `pr_open` only while its pull request is open
//! and `fixing` only while its run works. A pull request closed unmerged
//! gives its findings back with who closed it and when; a cancelled run
//! releases what it claimed. Both happen on the event, and the sweep makes
//! the same comparison first, on a clock of its own, for every team, so a
//! missed webhook and the records that were stale before are put right
//! within a sweep with no step by a person. A person who asks for a fix
//! always gets one; automatic fixes wait a week after a person said no. A
//! Retry on the run opens a new pull request from the findings as they are
//! now, never the closed one again. GitHub not answering is never read as
//! open, closed or merged.

use chrono::{DateTime, Duration, Utc};
use common::security_fixes::{
    self as sec, Finding, FindingState, FixUnit, SecurityConfig, Status, Tier, Verdict,
};
use common::security_release::{self as rel, RunNow};

use super::fixture::fn_src;
use crate::passes::security_fixes::release::{fake::*, reconcile, Scope};
use crate::passes::security_fixes::{mark_retry, release};

const REPO: &str = "acme/site";
const TICKET: &str = "SEC-0A1B2C3D4E5F";
const PR: u64 = 412;

fn day_after() -> DateTime<Utc> {
    at("2026-11-13T10:00:00Z")
}

fn cfg() -> SecurityConfig {
    SecurityConfig {
        enabled: true,
        ..Default::default()
    }
}

/// The repository as it stood: 274 findings in the bundle pull request,
/// last written when it opened the day before, and 3 open with no fix yet.
fn the_repository() -> Vec<(Finding, FindingState)> {
    let mut all: Vec<(Finding, FindingState)> = (0..274)
        .map(|i| {
            let (mut f, st) = in_pr(&format!("f{i:03}"), REPO, PR, TICKET);
            f.tier = Some(if i < 4 { Tier::P1 } else { Tier::P2 });
            f.verdict = Verdict::NotReachable;
            (f, st)
        })
        .collect();
    for i in 0..3 {
        let (mut f, mut st) = in_pr(&format!("n{i}"), REPO, PR, TICKET);
        f.status = Status::Open;
        f.pr = None;
        f.fixed_version = None;
        f.tier = Some(Tier::P2);
        f.verdict = Verdict::NotReachable;
        st.ticket = None;
        st.bundled = false;
        all.push((f, st));
    }
    all
}

fn ids(all: &[(Finding, FindingState)]) -> Vec<String> {
    all.iter().map(|(f, _)| f.id.clone()).collect()
}

/// What a person's request for every finding of the repository gets.
fn asked(all: &[(Finding, FindingState)], now: DateTime<Utc>) -> (usize, Vec<String>) {
    let (units, skipped) = sec::manual_units(all, &ids(all), &cfg(), now);
    (
        units.iter().map(|u| u.finding_ids.len()).sum(),
        skipped.into_iter().map(|s| s.reason).collect(),
    )
}

#[tokio::test]
async fn before_the_sweep_nothing_could_be_asked_for() {
    let all = the_repository();
    let (fixes, reasons) = asked(&all, day_after());
    assert_eq!(fixes, 0, "the page had no row to pick");
    assert_eq!(
        reasons
            .iter()
            .filter(|r| *r == "already has an open PR")
            .count(),
        274
    );
}

#[tokio::test]
async fn the_sweep_puts_yesterdays_stale_records_right_with_no_step_by_a_person() {
    let mut all = the_repository();
    let github = Fake::default();
    // The pull request as GitHub has it: closed by a person, not merged.
    github.pr(REPO, PR, closed_by("octo-admin"));
    let now = day_after();

    let r = reconcile(&github, &mut all, &Scope::All, now).await;
    assert_eq!((r.prs_read, r.released), (1, 274), "one read of GitHub");
    assert!(all.iter().all(|(f, _)| f.status != Status::PrOpen));

    // Each finding keeps who closed its pull request, when, and which.
    let (f, st) = &all[0];
    let pr = f.pr.as_ref().unwrap();
    assert_eq!(
        (
            pr.number,
            pr.state.as_str(),
            pr.closed_by.as_deref(),
            pr.closed_by_person
        ),
        (PR, "closed", Some("octo-admin"), true)
    );
    assert_eq!(pr.closed_at.as_deref(), Some("2026-11-13T09:30:00Z"));
    assert_eq!(st.ticket, None);
    let line = &github.event_texts()[0];
    assert!(
        line.contains("PR #412 was closed without merging by octo-admin"),
        "{line}"
    );

    // A person asks: every finding with a fix gets one. The 3 with no
    // fixed version are skipped for that reason, never for the pull request.
    let (fixes, reasons) = asked(&all, now);
    assert_eq!(fixes, 274);
    assert_eq!(reasons, vec!["no fixed version"; 3]);

    // Automatic fixes do not open the same bundle again against the
    // person's wishes: nothing is planned for a week, then it is.
    assert!(sec::plan_units(&all, &cfg(), now).is_empty());
    assert!(sec::plan_units(&all, &cfg(), now + Duration::days(6)).is_empty());
    let later = sec::plan_units(&all, &cfg(), now + Duration::days(8));
    assert_eq!(
        later.iter().map(|u| u.finding_ids.len()).sum::<usize>(),
        274
    );

    // The same sweep again: one read, nothing written.
    let saves = github.saves();
    let again = reconcile(&github, &mut all, &Scope::All, now + Duration::minutes(20)).await;
    assert_eq!((again.changed(), github.saves()), (0, saves));
}

#[tokio::test]
async fn the_webhook_and_the_cancel_release_at_once_and_agree_with_the_sweep() {
    // The close's webhook: the findings follow it now.
    let mut by_webhook = the_repository();
    let github = Fake::default();
    github.pr(REPO, PR, closed_by("octo-admin"));
    let scope = Scope::Pr {
        repo: REPO.into(),
        number: PR,
    };
    let r = reconcile(&github, &mut by_webhook, &scope, day_after()).await;
    assert_eq!(r.released, 274);
    // The sweep, had the webhook never arrived, ends in the same place.
    let mut by_sweep = the_repository();
    reconcile(&github, &mut by_sweep, &Scope::All, day_after()).await;
    assert_eq!(by_webhook, by_sweep);

    // A run cancelled before it opened a pull request releases its claim.
    let mut claimed: Vec<_> = (0..5)
        .map(|i| fixing(&format!("c{i}"), REPO, TICKET))
        .collect();
    let runs = Fake::default();
    runs.run(
        TICKET,
        RunNow::Ended {
            status: "cancelled".into(),
            by: Some("admin@example.com".into()),
        },
    );
    let r = reconcile(
        &runs,
        &mut claimed,
        &Scope::Ticket(TICKET.into()),
        day_after(),
    )
    .await;
    assert_eq!(r.released, 5);
    assert!(claimed
        .iter()
        .all(|(f, st)| f.status == Status::Open && st.ticket.is_none()));
    assert!(runs.event_texts()[0].contains("cancelled by admin@example.com"));
    assert_eq!(
        claimed[0].0.waiting.as_deref(),
        Some("The fix run was cancelled by admin@example.com. Ask for a fix to start a new one.")
    );
}

#[tokio::test]
async fn github_not_answering_releases_nothing_and_the_next_sweep_heals() {
    let mut all = the_repository();
    let before = all.clone();
    let down = Fake::default();
    let r = reconcile(&down, &mut all, &Scope::All, day_after()).await;
    assert_eq!((r.changed(), down.saves()), (0, 0));
    assert_eq!(
        all, before,
        "an unread pull request is never taken for closed"
    );
    down.pr(REPO, PR, closed_by("octo-admin"));
    let r = reconcile(
        &down,
        &mut all,
        &Scope::All,
        day_after() + Duration::minutes(20),
    )
    .await;
    assert_eq!(r.released, 274);
}

#[tokio::test]
async fn retry_on_the_run_is_a_new_request_from_the_findings_as_they_are_now() {
    let mut all = the_repository();
    let unit = FixUnit {
        repo: REPO.into(),
        bundle: true,
        ticket: TICKET.into(),
        finding_ids: ids(&all)[..274].to_vec(),
        ..Default::default()
    };
    let now = day_after();
    // While the pull request reads as open, Retry asks for nothing new.
    assert!(mark_retry(&mut all, &unit, "admin@example.com", now).is_empty());

    let github = Fake::default();
    github.pr(REPO, PR, closed_by("octo-admin"));
    reconcile(
        &github,
        &mut all,
        &Scope::Ids(unit.finding_ids.clone()),
        now,
    )
    .await;
    // One was fixed another way since the plan was made.
    all[7].0.status = Status::Fixed;
    let asked = mark_retry(&mut all, &unit, "admin@example.com", now);
    assert_eq!(asked.len(), 273);
    assert!(!asked.contains(&all[7].0.id));

    // The request starts as any a person made: one pull request for the
    // bundle, on a plan made now, whatever the hold says.
    let units = sec::requested_units(&all, &cfg(), now);
    assert_eq!(units.len(), 1);
    let new = &units[0];
    assert_eq!(new.finding_ids.len(), 273);
    assert!(new.bundle);
    assert_ne!(new.ticket, TICKET, "a new run, not the old plan");
    assert_ne!(new.branch, "", "on a branch of its own");
    // And the closed pull request is never the one it joins.
    assert_eq!(sec::open_bundle_pr(&all, new, true), None);
}

#[test]
fn a_reopened_pull_request_and_a_merge_are_followed_too() {
    let (mut f, mut st) = in_pr("a", REPO, PR, TICKET);
    rel::settle_pr(&mut f, &mut st, &closed_by("octo-admin"), day_after()).unwrap();
    rel::settle_pr(&mut f, &mut st, &rel::PrNow::Open, day_after()).unwrap();
    assert_eq!(f.status, Status::PrOpen);
    rel::settle_pr(&mut f, &mut st, &rel::PrNow::Merged, day_after()).unwrap();
    assert_eq!(f.status, Status::Fixed);
}

/// The places the pipeline calls the release: each was missing.
#[test]
fn every_path_that_ends_a_claim_tells_the_findings() {
    let fixes = include_str!("../security_fixes.rs");
    // The sweep: first for the team, before its sources, and for a team the
    // sweep's budget never reached.
    let tick = fn_src(fixes, "async fn team_tick(");
    let release_at = tick.find("release::run(").expect("the tick releases");
    let sync_at = tick.find("sync_repo(").expect("the tick syncs");
    assert!(release_at < sync_at, "the release comes before the sources");
    assert!(tick[release_at..sync_at].contains("let Some(kev_map) = kev_map else"));
    let sweep = fn_src(fixes, "async fn sweep_inner(");
    assert!(sweep.contains("TICK_BUDGET_SECS + RELEASE_BUDGET_SECS"));
    // Its own clock, not the sweep's.
    assert!(tick.contains("release::TEAM_BUDGET_SECS"));
    assert_eq!(release::TEAM_BUDGET_SECS, 45);
    // The events.
    let events = fn_src(fixes, "async fn event_for_team(");
    assert!(events.contains("\"pr\" | \"run\" =>"));
    assert!(events.contains("\"retry\" =>"));
    assert!(events.contains("retry_asked("));

    // The gateway: the pull request webhook, the cancel, the close, Retry.
    let webhook = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    let handler = fn_src(webhook, "async fn handle_pull_request(");
    let told = handler
        .find("super::security::on_pull_request(")
        .expect("the webhook tells the findings");
    // Before any early return of the handler (a reopen returns early).
    assert!(told < handler.find("return ").unwrap());
    let api = include_str!("../../../../gateway/src/routes/api.rs");
    assert!(fn_src(api, "pub async fn cancel_run(").contains("super::security::on_run_ended("));
    let retry = fn_src(api, "pub async fn retry_run(");
    let event_at = retry
        .find("super::security::retry_event(")
        .expect("Retry asks security");
    assert!(event_at < retry.find("retry_message(").unwrap());
    let close = include_str!("../../../../gateway/src/routes/run_close.rs");
    assert!(close.contains("super::security::on_run_ended("));

    // GitHub failing is never "open": the release reads it as unknown.
    let rel_src = include_str!("../security_release.rs");
    let pr_now = fn_src(rel_src, "impl ReleaseIo for Live<'_, '_> {");
    assert!(pr_now.contains("return PrNow::Unknown"));
    assert!(pr_now.contains("is_not_found"));
}
