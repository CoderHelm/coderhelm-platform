//! o. A requester left a five-point change request on a pull request
//! CoderHelm opened, and asked for no comments on the pull request. The
//! feedback round did all five, then the sandbox test step broke on
//! CoderHelm's own setup, so the round was held: "Review fix held — not
//! landed; its WIP branch is kept". The run completed with nothing on its
//! record about the held round. The next day the owner commented a bare
//! `@coderhelm` on the pull request to get that work landed. The gateway
//! read a bare mention as the re-review command, the reviewer posted a
//! fresh verdict, and nothing landed.
//!
//! Guarantee: work a round left held or unfinished on CoderHelm's own pull
//! request comes first. A held round stays on the run record (its WIP
//! branch, where it started, what it was asked) until it lands, and a WIP
//! branch ahead of the pull request counts too, for rounds held before the
//! record kept them. A bare mention resumes that round: the checks run again
//! on the WIP with the current setup, and the work lands (or is fixed, then
//! lands). A mention with words is read by the model, told about the held
//! work: "do it", "redo", "continue" and fix requests run a fix round on the
//! held WIP; only an ask for a review is a review. The person is told what
//! CoderHelm is doing, on the run and the ticket when the requester asked
//! for no PR comments.

use super::fixture::*;
use crate::passes::review_agent::{self, AskKind, AskRoute};
use common::held_work::{self, MentionRoute, Outstanding};
use common::run_round::{self, Incomplete, Kind, Round};

/// What happened instead: a review of the head.
const OLD: &str = "Review";

const FIVE: &str = "1. Break ties by id\n2. Keep updated_at advancing\n3. Warn once per run\n\
                    4. Count skipped rows\n5. Test the tie";

fn wip() -> String {
    run_round::wip_branch(BRANCH)
}

/// The held round as the feedback pass now records it.
fn held_record() -> Incomplete {
    let mut r = Round::start(
        Kind::Feedback,
        0,
        "3cb5799",
        FIVE,
        false,
        chrono::Utc::now(),
    );
    r.wip_branch = wip();
    Incomplete::held(
        &r,
        "Full test suite FAILED (exit 2) in CoderHelm's own setup",
        "2026-10-29T21:34:45Z",
    )
}

fn ending_for(route: MentionRoute, work: Option<&Outstanding>, may_comment: bool) -> Ending {
    let mut end = Ending {
        status: "completed".into(),
        pr: Some(12),
        ..Default::default()
    };
    end.said(format!("{route:?}"));
    if let (MentionRoute::ResumeWork, Some(w)) = (route, work) {
        // Told through the channel the requester allowed.
        end.said(if may_comment {
            format!("on the PR: {}", w.pickup_line())
        } else {
            format!("on the run and the ticket: {}", w.pickup_line())
        });
    }
    end
}

#[test]
fn a_bare_mention_resumes_the_held_round_never_a_review() {
    let inc = held_record();
    let item = run_item(&[
        ("status", s("completed")),
        ("pr_number", n(12)),
        ("branch", s(BRANCH)),
        (run_round::INCOMPLETE_FIELD, inc.to_attr()),
    ]);
    // The record keeps the held round, and it can be resumed.
    let back = Incomplete::from_item(&item).unwrap();
    assert!(back.held);
    assert!(run_round::may_resume("completed", 12, true));
    assert!(back.status_line().contains("held, not landed"));
    let work = Outstanding::from_incomplete(&back).unwrap();
    assert_eq!(work.wip_branch, wip());
    assert_eq!(work.base_sha, "3cb5799");

    let route = held_work::mention_route(true, false, true);
    assert_eq!(route, MentionRoute::ResumeWork);
    let end = ending_for(route, Some(&work), false);
    assert!(end.says(
        "on the run and the ticket: Picking up the held changes from 2026-10-29 21:34 UTC: \
         re-running the checks with the current setup, then landing them."
    ));
    end.never_says(OLD);

    // The resumed round is asked the five points first, then the mention.
    let body = work.round_body("@coderhelm");
    assert!(body.contains(FIVE));
    assert!(body.contains("don't redo or revert them"));
}

#[test]
fn a_round_held_before_the_record_kept_it_is_found_on_its_wip_branch() {
    let mut gh = FakeGitHub::new();
    gh.commit(
        "feedback",
        &wip(),
        &[("apps/web/src/lib/cards.ts", "export const tie = 'id';\n")],
    );
    // GitHub's compare of the pull request's branch with the WIP.
    let mut compare = gh.compare(&wip());
    compare["ahead_by"] = 2.into();
    compare["merge_base_commit"] = serde_json::json!({"sha": "3cb5799"});
    compare["commits"] = serde_json::json!([
        {"commit": {"committer": {"date": "2026-10-29T21:34:40Z"}}}
    ]);
    let mut work = Outstanding::from_wip_compare(&wip(), &compare).unwrap();
    assert!(work.held);
    assert_eq!(work.base_sha, "3cb5799");
    // What it was asked: the person's last review before it stopped.
    let reviews = serde_json::json!([
        {"user": {"login": "lead", "type": "User"}, "body": FIVE, "submitted_at": "2026-10-29T21:26:30Z"},
        {"user": {"login": "coderhelm[bot]", "type": "Bot"}, "body": "APPROVE", "submitted_at": "2026-10-30T00:20:14Z"}
    ]);
    work.requests = held_work::requests_from_reviews(&reviews, &work.since);
    assert_eq!(work.requests, FIVE);
    assert_eq!(
        held_work::mention_route(true, false, true),
        MentionRoute::ResumeWork
    );
    // A branch with nothing the pull request lacks is not outstanding.
    let even = serde_json::json!({"ahead_by": 0, "files": [], "merge_base_commit": {"sha": "x"}});
    assert!(Outstanding::from_wip_compare(&wip(), &even).is_none());
}

#[test]
fn words_are_read_for_meaning_and_only_a_review_ask_reviews() {
    // "do it", "redo", "continue": the model reads continue_work, and on
    // CoderHelm's own PR that is the fix loop, which resumes the held WIP.
    for said in ["do it", "redo", "continue", "go ahead and apply them"] {
        assert_eq!(
            held_work::mention_route(false, false, true),
            MentionRoute::Ask,
            "{said}"
        );
    }
    let kind = review_agent::parse_ask(&serde_json::json!({"kind": "continue_work"}));
    assert_eq!(kind, AskKind::ContinueWork);
    assert_eq!(review_agent::route_ask(kind, true), AskRoute::FixLoop);
    // Instructions run a fix round.
    let fix = review_agent::parse_ask(&serde_json::json!({"kind": "fix_request"}));
    assert_eq!(review_agent::route_ask(fix, true), AskRoute::FixLoop);
    // The model is told about the held work.
    let work = Outstanding::from_incomplete(&held_record()).unwrap();
    let prompt = review_agent::ask_prompt("do it", "APPROVE on 3cb5799.", None, Some(&work.note()));
    assert!(prompt.contains("## Outstanding work on the PR"));
    assert!(prompt.contains("held the changes from landing"));
    // A review happens when asked for, or with no work outstanding.
    assert_eq!(
        held_work::mention_route(false, true, true),
        MentionRoute::Review
    );
    assert_eq!(
        held_work::mention_route(true, false, false),
        MentionRoute::Review
    );
}

#[test]
fn the_gateway_and_the_feedback_round_still_make_these_calls() {
    let gateway = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    let comment = fn_src(gateway, "async fn handle_issue_comment(");
    for call in [
        "outstanding_work(",
        "common::held_work::mention_route(",
        "resume_outstanding(",
        "outstanding_work: outstanding.as_ref().map(|(_, w)| w.note())",
    ] {
        assert!(
            comment.contains(call),
            "handle_issue_comment no longer calls {call}"
        );
    }
    let lookup = fn_src(gateway, "async fn outstanding_work(");
    assert!(lookup.contains("Outstanding::from_incomplete"));
    assert!(lookup.contains("Outstanding::from_wip_compare"));

    let feedback = include_str!("../feedback.rs");
    let run = fn_src(feedback, "pub async fn run(");
    for call in [
        "outstanding_work(state, &github, &msg)",
        "w.round_body(&msg.review_body)",
        "w.pickup_line()",
        "constraints.may_comment()",
        "super::round::hold(",
        "if wrote || held_work.is_some() {",
    ] {
        assert!(
            run.contains(call),
            "the feedback round no longer calls {call}"
        );
    }
    // A held round is never cleared off the record by the round's own end.
    assert!(run.contains("if held.is_empty() { \", #inc\" } else { \"\" }"));
}
