//! mz. Two pull requests on acme/web each passed CI and merged one after
//! the other. #11 added a required field, `currency`, to the shared
//! `Invoice` struct; #12's new refund code built `Invoice { amount }`
//! without it. Each head was green against the base it was tested on, and
//! the base branch's rules didn't ask for a branch to be current (an admin
//! merge skips that rule anyway), so #12 merged on checks that never saw
//! #11. Together the base didn't compile.
//!
//! Guarantee: CoderHelm never merges a head its base has moved past.
//! Right before every merge it compares the head with the base branch's
//! tip; when the base has commits the head lacks, it merges the base into
//! the branch, waits for CI on that new commit, and merges only when that
//! is green. This holds on every repository CoderHelm merges in, whatever
//! the branch's rules say, and an unreadable comparison never merges.

use super::fixture::fn_src;
use crate::passes::await_merge::{
    base_moved_line, decide_gate, Action, CheckSummary, GateView, Mark, ReviewRules,
};
use crate::passes::review_actions::approval_state;
use common::merge_gate::{base_standing, BaseStanding};
use serde_json::json;

/// #12's head, green against the base before #11 merged.
const GREEN_ALONE: &str = "c12aaaa1c0ffee00000000000000000000000000";
/// The commit CoderHelm makes by merging the base into #12's branch.
const MERGED_RESULT: &str = "c12bbbb2c0ffee00000000000000000000000000";

fn github_src() -> &'static str {
    include_str!("../../clients/github.rs")
}

fn gate_src() -> &'static str {
    include_str!("../await_merge.rs")
}

/// The body of `GitHubClient::merge_pull_request`.
fn merge_call() -> &'static str {
    let src = github_src();
    let start = src
        .find("pub async fn merge_pull_request(")
        .expect("merge_pull_request is gone");
    let end = start
        + src[start..]
            .find("pub async fn customer_guard_checks(")
            .expect("the function after merge_pull_request");
    &src[start..end]
}

/// The gate on `head`: CoderHelm approved it, no person key is required,
/// and CI on `head` reads as `ci`.
fn gate(head: &str, ci: CheckSummary) -> GateView {
    GateView {
        draft: false,
        require_human: false,
        bot_verdict: Some("APPROVE".into()),
        bot_holding: vec![],
        approvals: approval_state(
            &json!([{"user": {"login": "coderhelm[bot]"}, "state": "APPROVED", "commit_id": head}]),
            head,
        ),
        want_labels: vec![],
        present_labels: vec![],
        labels_added_at: None,
        taken_off: vec![],
        preview_ran: vec![],
        ci: Some(ci),
        now: 1_000,
        head: head.into(),
        base: "main".into(),
        review_decision: None,
        earlier_approval_holds: false,
        rules: ReviewRules::default(),
        requested: vec![],
        base_merge_at_head: head == MERGED_RESULT,
    }
}

#[test]
fn a_head_its_base_moved_past_is_never_merged_on_its_own_green_checks() {
    // #12 alone: every check green, the keys in. On its own the gate
    // would merge it.
    assert_eq!(
        decide_gate(&gate(GREEN_ALONE, CheckSummary::default())).action,
        Action::Merge
    );
    // But #11 merged meanwhile: GitHub compares main...c12aaaa as
    // "diverged" (or "behind"), so the head's checks never ran on the
    // merged result.
    assert_eq!(base_standing("diverged"), Some(BaseStanding::Moved));
    assert_eq!(base_standing("behind"), Some(BaseStanding::Moved));
    assert_eq!(base_standing("ahead"), Some(BaseStanding::Current));
    // An answer CoderHelm can't read is never "current".
    assert_eq!(base_standing(""), None);

    // The check is inside the one call every CoderHelm merge goes
    // through, before GitHub is asked to merge, and it doesn't depend on
    // the branch's "must be up to date" rule.
    let call = merge_call();
    let at = |needle: &str| call.find(needle).unwrap_or_else(|| panic!("{needle}"));
    assert!(at("self.compare_status(owner, repo, base, sha)") < at("/merge\""));
    assert!(call
        .contains("BaseStanding::Moved) => {\n                return Ok(MergeOutcome::BaseMoved("));
    assert!(call.contains("None => {\n                return Err("));
    assert!(!call.contains("mergeable_state"));
    assert!(!call.contains("required_status_checks"));
}

#[test]
fn coderhelm_brings_the_base_in_and_merges_only_once_ci_passes_on_that_commit() {
    // The gate answers a moved base by merging it into the branch, the same
    // base merge it uses for a conflict, and says why on the pull request.
    let merge = fn_src(gate_src(), "async fn merge(");
    let moved = &merge[merge
        .find("MergeOutcome::BaseMoved(base) =>")
        .expect("the gate handles a moved base")..];
    assert!(moved.contains("base_moved_line(&base)"));
    assert!(moved.contains("keep_current("));
    assert!(moved.contains("BaseNeed::Behind"));
    let (mark, line) = base_moved_line("main");
    assert_eq!(mark, Mark::Waiting);
    assert!(
        line.contains("its checks never ran on the merged result"),
        "{line}"
    );
    assert!(
        line.contains("merges once the checks pass on that commit"),
        "{line}"
    );
    // After the base merge the gate goes on on the new commit, where CI is
    // read again.
    let keep = fn_src(gate_src(), "async fn keep_current(");
    assert!(keep.contains("bring_base_in("));
    assert!(keep.contains("arm("));

    // On c12bbbb, the merged result: CI runs, and the gate waits.
    let running = CheckSummary {
        pending: vec!["build".into()],
        ..Default::default()
    };
    assert_eq!(
        decide_gate(&gate(MERGED_RESULT, running)).action,
        Action::PollCi
    );
    // The build fails: `Invoice { amount }` is missing `currency`. The gate
    // never merges it.
    let red = CheckSummary {
        failing: vec!["build".into()],
        ..Default::default()
    };
    let d = decide_gate(&gate(MERGED_RESULT, red));
    assert_eq!(d.action, Action::BlockedOnCi);
    assert_ne!(d.action, Action::Merge);
    // Fixed on the branch and green on the merged result: it merges.
    assert_eq!(
        decide_gate(&gate(MERGED_RESULT, CheckSummary::default())).action,
        Action::Merge
    );
}
