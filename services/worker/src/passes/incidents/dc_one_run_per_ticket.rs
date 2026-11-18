//! dc. A person approved a plan written for a tracker ticket that already
//! had a run starting. The plan's task opened a GitHub issue for the same
//! work, labelled for CoderHelm; the issue's run carried `GH-<n>` while the
//! tracker's run carried the tracker key, so the guards keyed by ticket id
//! saw two tickets. One ticket got two runs and two pull requests, and both
//! runs spent their budgets on the same change.
//!
//! Guarantee: every fresh run takes one claim on the ticket's canonical key
//! before it writes anything, at the run start every trigger goes through.
//! An issue CoderHelm opens for another ticket names that ticket (a marker
//! in its body, and the stored link from the issue to its plan task), so it
//! resolves to the same key. A second trigger for a ticket a live run holds
//! starts nothing, whatever its source or order; and a plan task for a
//! ticket a live run already works opens no issue at all.

use std::collections::HashMap;

use common::messages::TicketSource;
use common::one_run::{self, Claim, Holder};

use super::fixture::*;
use crate::models::TicketMessage;

const KEY: &str = "WEB-77";
const TRACKER_RUN: &str = "01JRUNTRACKER0000000000000";
const ISSUE_RUN: &str = "01JRUNISSUE000000000000000";
const ISSUE: u64 = 41;

/// The tracker's ticket, as its webhook sends it.
fn tracker_ticket() -> TicketMessage {
    TicketMessage {
        source: TicketSource::Jira,
        ticket_id: KEY.into(),
        title: "Make the accordion block work without JavaScript".into(),
        body: "Every answer is in the server HTML.".into(),
        repo_owner: OWNER.into(),
        repo_name: REPO.into(),
        ..Default::default()
    }
}

/// The issue the plan task opens, as the GitHub webhook turns it into a
/// ticket: the plan is titled after the tracker ticket, the issue body is
/// what `plan_execute` writes, the sender is CoderHelm's own bot.
fn issue_ticket(body_edited_away: bool) -> TicketMessage {
    let title = format!("{KEY}: Make the accordion block work without JavaScript");
    let description = "Every answer is in the server HTML.";
    let named = one_run::named_ticket(&title, description);
    assert_eq!(named.as_deref(), Some(KEY), "the task names its ticket");
    let body =
        crate::passes::plan_execute::with_ticket_marker(description.to_string(), named.as_deref());
    TicketMessage {
        source: TicketSource::Github,
        ticket_id: format!("GH-{ISSUE}"),
        title,
        body: if body_edited_away {
            description.to_string()
        } else {
            body
        },
        repo_owner: OWNER.into(),
        repo_name: REPO.into(),
        issue_number: ISSUE,
        sender: "coderhelm[bot]".into(),
        ..Default::default()
    }
}

/// The claim table, with DynamoDB's conditional write: new, or the same
/// run, or over the run id the writer read.
#[derive(Default)]
struct Claims {
    rows: HashMap<String, String>,
}

impl Claims {
    fn write(&mut self, sk: &str, me: &str, from: Option<&str>) -> bool {
        let ok = match (self.rows.get(sk), from) {
            (None, None) => true,
            (Some(held), None) => held == me,
            (Some(held), Some(old)) => held == old || held == me,
            (None, Some(_)) => false,
        };
        if ok {
            self.rows.insert(sk.into(), me.into());
        }
        ok
    }

    /// The holder, with its run's status as the run table has it.
    fn holder(&self, sk: &str, runs: &HashMap<&str, &str>) -> Option<Holder> {
        self.rows.get(sk).map(|run| Holder {
            run_id: run.clone(),
            status: runs.get(run.as_str()).map(|s| s.to_string()),
            idle_hours: 0,
            claim_secs: 5,
        })
    }
}

/// `one_run::claim` as the worker runs it, on the fake table: `Ok` when
/// the run may start, `Err(live run)` when it must not.
fn start(
    claims: &mut Claims,
    runs: &HashMap<&str, &str>,
    msg: &TicketMessage,
    me: &str,
    linked: Option<&str>,
) -> Result<(), String> {
    let key = one_run::canonical(
        &msg.source,
        &msg.ticket_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        &msg.body,
        linked,
    );
    let sk = one_run::claim_sk(&key);
    let mut from: Option<String> = None;
    for _ in 0..3 {
        if claims.write(&sk, me, from.as_deref()) {
            return Ok(());
        }
        match one_run::decide(me, claims.holder(&sk, runs).as_ref()) {
            Claim::Take => from = None,
            Claim::TakeOver(old) => from = Some(old),
            Claim::Held(live) => return Err(live),
        }
    }
    Ok(())
}

#[test]
fn the_issue_coderhelm_opened_for_a_ticket_starts_no_second_run() {
    let mut claims = Claims::default();
    let runs: HashMap<&str, &str> = HashMap::from([(TRACKER_RUN, "running")]);
    // The tracker's run starts first and holds the ticket.
    assert_eq!(
        start(&mut claims, &runs, &tracker_ticket(), TRACKER_RUN, None),
        Ok(())
    );
    // The issue's run, a second later, resolves to the same ticket.
    let issue = issue_ticket(false);
    let end = start(&mut claims, &runs, &issue, ISSUE_RUN, None);
    assert_eq!(end, Err(TRACKER_RUN.to_string()), "one ticket, one run");
    // Its body edited by a person: the stored link still names the ticket.
    let edited = issue_ticket(true);
    assert_eq!(
        start(&mut claims, &runs, &edited, ISSUE_RUN, Some(KEY)),
        Err(TRACKER_RUN.to_string())
    );
    // The live run's page says the ticket arrived again; the old ending
    // (a second run and a second pull request) never happens.
    let note = one_run::held_note(&issue.ticket_id, &issue.source);
    assert!(note.contains("no second run or pull request"), "{note}");
}

#[test]
fn the_order_does_not_matter_and_an_ended_run_hands_the_ticket_on() {
    let mut claims = Claims::default();
    let mut runs: HashMap<&str, &str> = HashMap::from([(ISSUE_RUN, "awaiting_ci")]);
    // The issue's run wins the race this time: the tracker's is the second.
    assert_eq!(
        start(&mut claims, &runs, &issue_ticket(false), ISSUE_RUN, None),
        Ok(())
    );
    assert_eq!(
        start(&mut claims, &runs, &tracker_ticket(), TRACKER_RUN, None),
        Err(ISSUE_RUN.to_string())
    );
    // Once that run has ended, the ticket's next run takes the claim over.
    runs.insert(ISSUE_RUN, "failed");
    assert_eq!(
        start(&mut claims, &runs, &tracker_ticket(), TRACKER_RUN, None),
        Ok(())
    );
    // Another ticket is never held by this one.
    let mut other = tracker_ticket();
    other.ticket_id = "WEB-78".into();
    assert_eq!(start(&mut claims, &runs, &other, "01JOTHER", None), Ok(()));
}

#[test]
fn every_trigger_goes_through_the_claim_and_the_plan_opens_no_duplicate_issue() {
    // The run start: every fresh run claims its ticket before its record.
    let src = include_str!("../mod.rs");
    let f = &src[src.find("\npub async fn orchestrate_ticket(").unwrap()..];
    let f = &f[..f.find("\n}\n").unwrap()];
    let claim = f
        .find("one_run::claim(state, &msg, &run_id)")
        .expect("the run start takes the ticket's claim");
    let record = f.find("create_run_record(state, &msg, &run_id").unwrap();
    assert!(claim < record, "the claim comes before anything is written");
    // The claim resolves a CoderHelm issue through its stored link.
    let one = include_str!("../one_run.rs");
    assert!(one.contains("\"source_ticket\""));
    // The plan: each issue-opening path names its ticket and skips a ticket
    // a live run already works.
    let plan = include_str!("../plan_execute.rs");
    assert_eq!(plan.matches(".create_issue(").count(), 2);
    for at in plan.match_indices(".create_issue(").map(|(i, _)| i) {
        let before = &plan[..at];
        let near = &before[before.len().saturating_sub(1800)..];
        assert!(near.contains("one_run::named_ticket("));
        assert!(near.contains("one_run::live_run("));
        assert!(near.contains("with_ticket_marker("));
    }
    assert!(plan.contains("put.item(\"source_ticket\""));
}
