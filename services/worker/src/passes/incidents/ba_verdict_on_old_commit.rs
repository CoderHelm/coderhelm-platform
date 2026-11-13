//! ba. A run finished as Done with its pull request marked ready. Its run
//! page said "Needs a person" and that merging was the team's call, while
//! CoderHelm's own reviewer stood at "Changes requested" on a commit older
//! than the pull request's head. The run's last pass was a fix (implement)
//! with no review after it: the fix was pushed, the run waited for CI, and
//! the wake-up that found CI green with no event to read (a missed webhook,
//! or a repository with no checks) marked the pull request ready and
//! completed the run through an ending that skips the review step every
//! other ending goes through. Nobody was ever asked to review the new head.
//! The verdict shown was the true last one, on the commit before the fix.
//! The run's count of passes (12) ended nothing: the ceiling is 300.
//!
//! Guarantee: a run never rests as completed with its own reviewer's
//! verdict missing on the head or asking for changes. Every completion
//! looks at the pull request (`pr_watch::after_completion`), and so does
//! the sweep: no verdict on the head asks for the review of that commit,
//! once per head; changes requested on the head get their fix round, and
//! when the run's rounds are over what is open is deferred on the pull
//! request with the reason. Neither asks for a person. The run page names
//! the one true reason the pull request is where it is
//! (`common::merge_standing`): a person is named only for the team's own
//! rule, with where that rule is set.

use super::fixture::*;
use crate::passes::pr_watch::{self, Act, Marks, Seen};
use common::merge_standing::{self, Facts};

const OLD_CHIP: &str = "needs a person";
const OLD_NEXT: &str = "your team's call";
const HEAD: &str = "7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b";
const BEFORE_FIX: &str = "2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c";

/// The pull request as the run left it: ready, green, and reviewed only on
/// the commit before the last fix.
fn left_by_the_run() -> Seen {
    Seen {
        open: true,
        head: HEAD.into(),
        base_sha: "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0".into(),
        reviewer_on: true,
        own_verdict: Some(("REQUEST_CHANGES".into(), BEFORE_FIX.into())),
        ..Default::default()
    }
}

fn facts(seen: &Seen, working: bool) -> Facts {
    Facts {
        head: seen.head.clone(),
        reviewer_on: seen.reviewer_on,
        own_verdict: seen.own_verdict.clone(),
        working,
        ..Default::default()
    }
}

#[test]
fn the_completed_run_gets_its_head_reviewed_and_the_loop_goes_on_to_approval() {
    let repo = format!("{OWNER}/{REPO}");
    let mut seen = left_by_the_run();
    let mut marks = Marks::default();

    // 1. Completed with the verdict on an older commit: the head is
    //    reviewed. Asked once, however often the sweep looks.
    let act = pr_watch::decide("completed", &seen, &marks);
    assert_eq!(act, Act::ReviewHead);
    marks.review_head = act.claim(&seen).unwrap().1;
    assert_eq!(pr_watch::decide("completed", &seen, &marks), Act::Nothing);
    let page = merge_standing::decide(&repo, &facts(&seen, true));
    assert_eq!(page.code, "reviewing");
    assert!(!page.needs_person);
    // The old verdict is never shown as the verdict on this head.
    assert!(page.text.contains("`7a6b5c4`") && page.text.contains("`2b3c4d5`"));

    // 2. The review of the head asks for changes: a fix round takes them.
    seen.own_verdict = Some(("REQUEST_CHANGES".into(), HEAD.into()));
    let act = pr_watch::decide("completed", &seen, &marks);
    assert_eq!(act, Act::FixFindings);
    let page = merge_standing::decide(&repo, &facts(&seen, true));
    assert_eq!(page.code, "fixing_review");
    assert!(!page.needs_person);

    // 3. The round pushed a new head: that head is reviewed in turn. The
    //    marks are per commit, so the loop is never stopped by the last one.
    marks.findings_head = HEAD.into();
    let next_head = "9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c";
    seen.head = next_head.into();
    assert_eq!(
        pr_watch::decide("completed", &seen, &marks),
        Act::ReviewHead
    );

    // 4. Approved on the head: nothing is left for CoderHelm to do on it.
    seen.own_verdict = Some(("APPROVE".into(), next_head.into()));
    assert_eq!(pr_watch::decide("completed", &seen, &marks), Act::Nothing);
}

#[test]
fn the_page_names_the_one_true_reason_and_asks_a_person_only_for_the_teams_rule() {
    let repo = format!("{OWNER}/{REPO}");
    let approved = Facts {
        head: HEAD.into(),
        reviewer_on: true,
        own_verdict: Some(("APPROVE".into(), HEAD.into())),
        ..Default::default()
    };
    // Auto-merge off: said as that, with the setting.
    let off = merge_standing::decide(&repo, &approved);
    assert_eq!(off.code, "auto_merge_off");
    assert!(off.needs_person && off.text.starts_with("Auto-merge is off for this repo"));
    assert_eq!(off.link, "/reviewer/config");
    // The setting asks for a person's approval: said as that.
    let key = merge_standing::decide(
        &repo,
        &Facts {
            auto_merge: true,
            require_human_approval: true,
            ..approved.clone()
        },
    );
    assert_eq!(key.code, "needs_person_approval");
    assert!(key
        .text
        .contains("requires an approving review from a person"));
    // The repository's own branch rules: said as that, with GitHub's page.
    let rules = merge_standing::decide(
        &repo,
        &Facts {
            auto_merge: true,
            blocked_by_rules: true,
            ..approved.clone()
        },
    );
    assert_eq!(rules.code, "repo_requires_person_review");
    assert!(rules.link.ends_with("/settings/branches"));
    // Approved, green and allowed: CoderHelm merges and follows the deploy.
    let go = merge_standing::decide(
        &repo,
        &Facts {
            auto_merge: true,
            ..approved.clone()
        },
    );
    assert_eq!(go.code, "merging");
    assert!(!go.needs_person && go.next.contains("deploy"));
    for s in [&off, &key, &rules, &go] {
        let all = format!("{} {}", s.text, s.next).to_lowercase();
        assert!(!all.contains(OLD_CHIP) && !all.contains(OLD_NEXT), "{all}");
    }
}

#[test]
fn every_ending_looks_at_the_pull_request() {
    let src = include_str!("../resume.rs");
    // The ending that skipped the review step.
    let fast = fn_src(src, "\nasync fn set_run_complete(");
    assert!(fast.contains("super::pr_watch::after_completion(state, team_id, run_id)"));
    // The ending every other path takes still asks for the hand-off review.
    let end = fn_src(src, "\nasync fn end_run(");
    assert!(end.contains("review_on_handoff("));
    assert!(end.contains("super::pr_watch::after_completion("));
    // And the sweep covers a completion nothing looked at.
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::pr_watch::sweep(&state)"));
    // The look claims its act against the commit before doing it.
    let look = fn_src(include_str!("../pr_watch.rs"), "\npub async fn look(");
    assert!(look.contains("claim(state, c, field, &value)"));
    assert!(look.contains("release(state, c, field, &value)"));
    // A pass count never ended the run in the incident: the ceiling is far
    // above any run's passes.
    let totals = crate::passes::run_ceiling::Totals {
        passes: 12,
        ..Default::default()
    };
    assert_eq!(totals.over(), None);
}
