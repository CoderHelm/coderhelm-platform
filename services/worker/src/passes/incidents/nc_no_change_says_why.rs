//! nc. An alarm started a run on a ticket that already had CoderHelm's open
//! pull request, and the run said it would continue that pull request as
//! its update. Its plan found, with evidence, that the code already did
//! what the alarm asked. The run ended `completed` with no pull request, no
//! error and its notes stopping at the plan: the reason lived only in the
//! worker's log. Nothing was said about the open pull request, and the
//! alarm's channel heard nothing.
//!
//! Guarantee: a plan that finds no code change is needed ends the run as
//! its own outcome, `no_change`, with the plan's reason (in full, with what
//! it checked) as the run's summary note and its `no_change` record. The
//! open pull request the run was continuing is decided in plain words, on
//! the run page and on the pull request: still needed (it stays open, and
//! why) or no longer needed (closed with a note, its branch kept). A run an
//! alert started answers on the alert's channels with the reason.

use super::fixture::*;
use crate::passes::fix_location::FixDecision;
use crate::passes::no_change::{self, OpenPr, Origin, Verdict};
use crate::passes::pr_holder;

const EARLIER_RUN: &str = "01JRUNEARLIERALARM00000000";
const PR: u64 = 31;

/// The plan, as the planner wrote it for the alarm.
const TASKS: &str = "NO_CHANGES_NEEDED: The current rule already matches the crawler: 610 \
     requests blocked in the last hour, 90% on the expected fingerprint. The counting rule \
     stayed under its 400 an hour threshold for 48 hours (at most 310). The remaining counts \
     come from other crawlers.\n\nThe stream behind the open pull request is down to 3 requests.";

fn plan_fix() -> FixDecision {
    let mut f = FixDecision::default();
    f.evidence.looked_up = vec![
        "firewall logs, last hour: 610 blocked, 90% on the expected fingerprint".into(),
        "counting rule, last 48 hours: at most 310 an hour against a 400 threshold".into(),
    ];
    f
}

fn open_pr() -> OpenPr {
    OpenPr {
        repo: format!("{OWNER}/{REPO}"),
        number: PR,
        url: format!("https://github.com/{OWNER}/{REPO}/pull/{PR}"),
        title: "Block the scraper stream at the edge".into(),
        branch: BRANCH.into(),
    }
}

/// The run as `run_passes` ends it: it said it continues the open pull
/// request when it claimed it, then the plan said no change.
fn replay(verdict: Option<(Verdict, String)>, closed: bool) -> (Ending, no_change::Ending) {
    let mut end = Ending::default();
    end.said(pr_holder::continue_note(PR, &[EARLIER_RUN.to_string()]));
    let ending = no_change::decide(
        Origin::Alert,
        TASKS,
        Some(&plan_fix()),
        Some(&open_pr()),
        verdict,
        closed,
    );
    end.status = ending.status.to_string();
    end.said(ending.note.clone());
    if let Some((_, text, _)) = &ending.on_pr {
        end.said(text.clone());
    }
    if let Some((paragraph, line)) = &ending.alert {
        end.said(format!("{paragraph}\n{line}"));
    }
    (end, ending)
}

#[test]
fn a_no_change_ending_says_why_on_the_run_and_answers_the_alert() {
    let (end, ending) = replay(
        Some((
            Verdict::NotNeeded,
            "The stream it blocks is down to 3 requests, so the rule it adds has nothing left to stop.".into(),
        )),
        true,
    );
    assert!(end.says("continues it as its update, on its branch, and opens no other"));

    // Its own outcome, never a silent "completed".
    assert_eq!(end.status, common::run_status::NO_CHANGE);
    assert_ne!(end.status, common::run_status::COMPLETED);

    // The reason, in full with its evidence, is the run's summary note and
    // its record.
    assert!(ending
        .note
        .starts_with("No change needed. The current rule already matches"));
    assert!(ending.note.contains("at most 310"));
    assert!(ending.note.contains("down to 3 requests"));
    assert!(ending
        .note
        .contains("- firewall logs, last hour: 610 blocked"));
    assert_eq!(
        ending.record.reason,
        no_change::reason_of(TASKS),
        "the record keeps the whole reason"
    );
    assert_eq!(ending.record.checks.len(), 2);

    // The open pull request is decided, in plain words, on the run page and
    // on the pull request.
    let call = ending.record.open_pr.as_ref().expect("the PR decision");
    assert_eq!(call.number, PR);
    assert_eq!(call.verdict, Verdict::NotNeeded);
    assert!(call.closed);
    assert!(call
        .line
        .contains("is no longer needed, so CoderHelm closed it"));
    assert!(call
        .line
        .contains(&format!("Its branch `{BRANCH}` is kept.")));
    assert!(ending.note.contains(&call.line));
    let (n, on_pr, close) = ending.on_pr.clone().unwrap();
    assert_eq!((n, close), (PR, true));
    assert!(on_pr.contains("found that no code change is needed"));
    assert!(on_pr.contains("This pull request is closed"));

    // The alert hears back on its channels, with the reason's paragraph.
    let (paragraph, line) = ending.alert.clone().expect("the alert reply");
    assert!(paragraph.starts_with("The current rule already matches the crawler"));
    assert!(!paragraph.contains("down to 3 requests"), "one paragraph");
    assert_eq!(line, call.line);
    let card = common::alert_notify::no_change_message(
        "ALARM: crawler-rule-drift",
        &format!("{OWNER}/{REPO}"),
        &paragraph,
        &line,
        "https://app.example.com/runs/detail?id=r1",
        "https://app.example.com/alerts/detail?id=a1",
    );
    assert!(card.summary.contains("No change needed"));
    let teams = common::notify::teams::render(&card).to_string();
    assert!(
        teams.contains("The current rule already matches the crawler"),
        "{teams}"
    );
    assert!(teams.contains("View run"), "{teams}");

    // The alert is not a ticket: nothing is posted on one.
    assert!(ending.ticket.is_none());
}

#[test]
fn a_pull_request_still_needed_stays_open_and_says_why() {
    let (_, ending) = replay(
        Some((
            Verdict::Keep,
            "Its rule is not in the code yet and the stream still runs.".into(),
        )),
        true,
    );
    let call = ending.record.open_pr.unwrap();
    assert!(!call.closed);
    assert_eq!(
        call.line,
        format!("Pull request #{PR} stays open: Its rule is not in the code yet and the stream still runs.")
    );
    let (_, on_pr, close) = ending.on_pr.unwrap();
    assert!(!close);
    assert!(on_pr.contains("This pull request stays open"));

    // When closing fails, the run says so and recommends it.
    let (_, failed) = replay(
        Some((Verdict::NotNeeded, "Nothing left to block.".into())),
        false,
    );
    assert!(failed
        .record
        .open_pr
        .unwrap()
        .line
        .contains("closing it is recommended"));
}

#[test]
fn the_pipeline_ends_a_no_change_plan_through_this_ending() {
    let rp = run_passes_src();
    let at = rp
        .find("plan_result.tasks.starts_with(no_change::SENTINEL)")
        .expect("the no-change check");
    let rest = &rp[at..];
    let finish = rest
        .find("no_change::finish(")
        .expect("ends through no_change::finish");
    let next_return = rest.find("return Ok(());").unwrap();
    assert!(finish < next_return);
    // The old silent ending is gone from the pipeline.
    assert!(!rp.contains("\"Plan determined no changes needed\""));

    let src = include_str!("../no_change.rs");
    let body = &src[src.find("pub(crate) async fn finish(").unwrap()..];
    for needed in [
        "super::add_progress_note(state, &msg.team_id, run_id, &end.note)",
        "attr_s(end.status)",
        "#nc = :nc",
        "expression_attribute_names(\"#nc\", FIELD)",
        "notify_alert_no_change(",
        "close_pull_request(",
        "conversation::jira_judged(",
        "conversation::github_judged(",
        "emit_run(state, &msg.team_id, run_id, end.status)",
    ] {
        assert!(body.contains(needed), "finish must carry {needed}");
    }
}
