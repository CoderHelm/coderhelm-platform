//! bi. A ticket run's own review asked for changes on its pull request. Two
//! fix rounds ran; the loop then said "2 rounds, and the last one past the
//! first 2 made no progress", moved what was open to "Deferred" on the pull
//! request and marked the run completed. The rounds hadn't failed on the
//! code: their edits to the files the fix needed were refused as outside the
//! round's files, their checks were skipped for lack of time, and an edit
//! failed because its `edits` came as a string. What was deferred was a
//! one-line config omission named with its file and line, and a placeholder
//! ("the review requested changes on <sha>"). Its review said "changes
//! requested" on the final head, and the run page said "CoderHelm is still
//! working on it ... a fix round is taking them" for a run that had
//! finished. The finding itself asked to hide the site's header and footer,
//! which a comment on the ticket said to keep. One outside step asked a
//! person to add two labels to the pull request, which CoderHelm can do.
//!
//! Guarantee: each finding is tracked on its own. A round a tool or a guard
//! blocked is not a stall: the next round on the finding removes the block.
//! A stalled finding climbs its own ladder (one finding, a fresh context, a
//! re-split, the strongest model). A finding the ticket, its comments or the
//! plan rule out is settled by the ticket's words, never deferred. Only a
//! person-gate reason defers one. A run is never completed while its own
//! review's verdict on the head asks for changes, nor with a deferred item
//! without such a reason; the card of a finished run never says it is
//! working. Adding labels is CoderHelm's own step.

use super::fixture::*;
use crate::passes::own_review_ledger::{
    self as ledger, Approach, Blocker, Completion, Ledger, Plan, Settled, TICKET_CONFLICT,
};
use crate::passes::pr_watch::{self, Act, Performed};

const OLD: &str = "made no progress";

/// The finding the run deferred, as two reviews worded it.
const NAV: &str = "Site navbar and footer still render on the portal page; signup-portal isn't in settings.blankLayout (`apps/web/src/lib/routing/renderers.tsx:1632`)";
const NAV_AGAIN: &str = "The site navbar and footer still render on the portal pages because `signup-portal` isn't in settings.blankLayout (`apps/web/src/lib/routing/renderers.tsx:1636`)";
const LINT: &str = "e2e lint fails on the two new files (`e2e/tests/signup.spec.ts:121`)";
const H1: &str = "Test plan says one H1; the spec asserts none (`e2e/specs/browse.md:191`)";

/// What the round's log carried.
fn the_blocked_round_log() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"content": "refused: outside the round's files"}),
        serde_json::json!({"content": "not run: not enough time left"}),
        serde_json::json!({"content": "Error: edit_file not applied: `edits` was a string that isn't a JSON array."}),
    ]
}

fn round(l: &mut Ledger, focus: &[String], blocked: Vec<Blocker>, account: &str) {
    l.start_round(focus);
    l.end_round(blocked, account);
}

#[test]
fn rounds_the_tools_blocked_are_not_no_progress_and_the_omission_is_fixed() {
    let mut end = Ending::default();
    let mut l = Ledger::default();
    let first = vec![NAV.to_string(), LINT.to_string(), H1.to_string()];
    l.observe(&first, "");
    let Plan::Round {
        focus,
        approach: Approach::All,
        ..
    } = l.decide(&first, 0.0, 200.0)
    else {
        panic!("the first round takes every finding");
    };
    // Round 1 fixed two and was blocked on the third.
    let blocked = ledger::blockers_of(&[], &the_blocked_round_log());
    assert_eq!(
        blocked,
        vec![Blocker::Scope, Blocker::NoTime, Blocker::ToolInput]
    );
    round(&mut l, &focus, blocked, "Fixed the lint and the H1.");
    let open = vec![NAV_AGAIN.to_string()];
    l.observe(&open, "");
    // Round 2 (the cap of two once ended the loop here): not a stall.
    let t = &l.findings[0];
    assert_eq!((t.stalled, t.blocked), (0, 1), "{t:?}");
    match l.decide(&open, 0.0, 200.0) {
        Plan::Round {
            focus,
            approach,
            unscoped,
            fresh,
        } => {
            assert_eq!(focus, open);
            assert!(unscoped, "the refused edit's file is open to this round");
            assert!(fresh, "the round starts with its full time");
            let told = ledger::instruction(&approach, &focus);
            end.said(told.clone());
            assert!(told.contains("didn't fail on the code"));
            round(&mut l, &focus, vec![], "Added it to the list.");
        }
        other => panic!("{other:?}"),
    }
    // Round 3: the next review no longer raises it.
    l.observe(&[], "");
    assert_eq!(l.decide(&[], 0.0, 200.0), Plan::Ready);
    assert!(l.findings.iter().all(|t| t.settled == Some(Settled::Fixed)));
    assert_eq!(
        ledger::completion(Some(("APPROVE", "6470f1dabc")), "6470f1dabc", &l),
        Completion::Complete
    );
    end.status = "completed".into();
    end.never_says(OLD);
}

#[test]
fn a_finding_the_ticket_comment_rules_out_is_dropped_by_the_ticket_not_deferred() {
    let ticket = "### Ticket: Sign-up pages\nAdd the four register pages.\n\n### Comment on the ticket by a teammate\nKeep your site's header and footer. You do not need to remove the footer, although the portal's optimal experience is without a footer.";
    let mut l = Ledger::default();
    let open = vec![NAV.to_string()];
    l.observe(&open, ticket);
    round(
        &mut l,
        &open,
        vec![],
        &format!("{TICKET_CONFLICT} {NAV} :: \"Keep your site's header and footer\""),
    );
    l.observe(&[NAV_AGAIN.to_string()], ticket);
    let Plan::Settle {
        deferred,
        by_ticket,
    } = l.decide(&[NAV_AGAIN.to_string()], 0.0, 200.0)
    else {
        panic!("settled by the ticket");
    };
    assert!(deferred.is_empty());
    let lines = ledger::settled_lines(&deferred, &by_ticket);
    assert!(lines[0].contains("the ticket rules it out"), "{lines:?}");
    // The review of the head then names it as a note, never a hold.
    let mut findings: Vec<crate::passes::review_agent::Finding> = serde_json::from_value(serde_json::json!([
        {"file": "apps/web/src/lib/routing/renderers.tsx", "line": 1640, "severity": "medium", "category": "correctness",
         "evidence": "verified-defect",
         "title": "Site navbar and footer still render on the portal pages; signup-portal isn't in settings.blankLayout",
         "body": "NavbarServer and Footer read the hide list from settings.blankLayout."}
    ]))
    .unwrap();
    let mut own_open = vec![NAV_AGAIN.to_string()];
    let settled = crate::passes::review_pr::settle_own_findings(&l, &mut findings, &mut own_open);
    assert!(own_open.is_empty(), "{own_open:?}");
    assert!(!settled.is_empty());
    assert!(!findings[0].holds_merge());
    assert!(findings[0].body.contains("the ticket says"));
    // The reviewer is told the ticket and its comments.
    let section = crate::passes::review_pr::own_ticket_section(ticket);
    assert!(section.contains("Keep your site's header and footer"));
    assert!(section.contains("Never raise a finding that asks for something these rule out"));
}

#[test]
fn the_run_is_never_completed_while_its_review_asks_for_changes() {
    let l = Ledger::default();
    assert!(matches!(
        ledger::completion(Some(("REQUEST_CHANGES", "6470f1dabc")), "6470f1dabc", &l),
        Completion::Continue(_)
    ));
    // Every completion path asks the guard first.
    let resume = include_str!("../resume.rs");
    for head in [
        "\nasync fn complete_run_with_status(",
        "\nasync fn wait_on_person(",
    ] {
        assert!(
            fn_src(resume, head).contains("own_review_completion("),
            "{head} completes without the guard"
        );
    }
    // A finished run's rounds are bounded per finding, never by the shared
    // CI budget, and the round's blockers reach the ledger.
    assert!(fn_src(resume, "\nasync fn run_fix_round(").contains("record_round_end("));
    let queue = fn_src(resume, "\npub(crate) async fn queue_own_review_round(");
    assert!(queue.contains("own_review_ledger::of_item("));
    assert!(!queue.contains("rounds_over("));
    // A placeholder is never deferred as a finding.
    let watch = include_str!("../pr_watch.rs");
    assert!(!watch
        .contains("its review comments on the \\\n                     pull request say what"));
}

#[test]
fn a_finished_run_whose_findings_were_settled_is_never_shown_working() {
    // The look that settled the findings started nothing.
    assert!(!pr_watch::is_working(
        &Act::FixFindings,
        Performed::Settled,
        "completed"
    ));
    assert!(!pr_watch::is_working(
        &Act::FixFindings,
        Performed::NotStarted,
        "completed"
    ));
    // A round it queued is working; so is a run in flight.
    assert!(pr_watch::is_working(
        &Act::FixFindings,
        Performed::Started,
        "completed"
    ));
    assert!(pr_watch::is_working(
        &Act::Nothing,
        Performed::Settled,
        "awaiting_ci"
    ));
    let standing = common::merge_standing::decide(
        "your-team/web",
        &common::merge_standing::Facts {
            head: "6470f1dabc".into(),
            reviewer_on: true,
            own_verdict: Some(("REQUEST_CHANGES".into(), "6470f1dabc".into())),
            working: pr_watch::is_working(&Act::FixFindings, Performed::Settled, "completed"),
            auto_merge: true,
            ..Default::default()
        },
    );
    assert_eq!(standing.code, "review_changes_requested");
    assert!(!standing.text.contains("a fix round is taking them"));
}

#[test]
fn coderhelm_adds_the_pull_request_labels_itself() {
    use crate::passes::outside_work::{self, Abilities, Action, Doer, NoteStep};
    let s = NoteStep {
        step: "Add PR labels E2E:signup and CI:E2E, sending the existing labels too".into(),
        by: "person".into(),
        owner: "a repository admin".into(),
        how: "GitHub labels API".into(),
        why_person: "CoderHelm's GitHub App is not allowed to start workflows in this repository"
            .into(),
        grant: "give the app Actions: write".into(),
        values: vec!["labels += E2E:signup, CI:E2E".into()],
        kind: "e2e".into(),
        ..Default::default()
    };
    assert_eq!(
        s.labels_named(),
        vec!["E2E:signup".to_string(), "CI:E2E".to_string()]
    );
    assert!(s.adds_labels_only());
    assert_eq!(s.doer(), Doer::Person);
    match outside_work::action_for(&s, &Abilities::default(), &Default::default()) {
        Action::Label(l, r) => {
            assert_eq!(l, "E2E:signup,CI:E2E");
            assert!(r.file.is_empty(), "no workflow to wait for");
        }
        other => panic!("{other:?}"),
    }
    // The executor takes it over, whoever the plan named.
    let exec = include_str!("../outside_exec.rs");
    assert!(exec.contains("s.adds_labels_only()"));
}
