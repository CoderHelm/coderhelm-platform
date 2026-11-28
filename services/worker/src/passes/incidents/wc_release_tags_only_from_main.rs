//! wc. A repository whose release tags deploy production had CoderHelm's
//! release on. Pull requests that merged into a feature branch were
//! released like merges into main: the auto merge tagged the merge commit,
//! and the release steps and the batched release tagged the head of the
//! branch the pull request went into. Each tag deployed that feature
//! branch to production over what main held. Pushes were already read only
//! from the default branch; the merge paths never asked which branch they
//! merged into.
//!
//! Guarantees: every release tag CoderHelm cuts goes through
//! `cut_tag_if_new`, which tags a commit only once GitHub says the default
//! branch holds it (unreadable is never a release); a merge into another
//! branch arms no release and says so on the pull request and in its
//! release steps; and a batched release always tags the default branch's
//! head, whichever branch its window was opened from.

use crate::passes::review_actions::{default_branch_holds, not_released_line};

const ACTIONS: &str = include_str!("../review_actions.rs");
const RELEASE: &str = include_str!("../release_run.rs");

fn body(src: &'static str, sig: &str) -> &'static str {
    let start = src.find(sig).unwrap_or_else(|| panic!("{sig}"));
    let rest = &src[start..];
    &rest[..rest.find("\n}\n").map(|i| i + 3).unwrap_or(rest.len())]
}

#[test]
fn a_release_tag_needs_the_default_branch_to_hold_the_commit() {
    // compare/{sha}...{default}: the default branch is ahead of, or the same
    // as, a commit it holds; behind or diverged is a commit only another
    // branch has.
    assert!(default_branch_holds("ahead"));
    assert!(default_branch_holds("identical"));
    assert!(!default_branch_holds("behind"));
    assert!(!default_branch_holds("diverged"));
    assert!(!default_branch_holds(""));

    let cut = body(ACTIONS, "pub(crate) async fn cut_tag_if_new(");
    let check = cut.find("default_branch_holds(&status)").unwrap();
    let create = cut.find("create_tag_ref(").unwrap();
    assert!(
        check < create,
        "the branch is checked before any tag exists"
    );
    assert!(cut.contains(".compare_status(owner, repo, sha, &default_branch)"));
    assert!(cut.contains("Cut::NotOnDefaultBranch(default_branch)"));
    // The only place a tag is created.
    assert_eq!(ACTIONS.matches("create_tag_ref(").count(), 1);
    assert!(!RELEASE.contains("create_tag_ref("));
}

#[test]
fn a_merge_into_a_feature_branch_arms_no_release_and_says_so() {
    let post = body(ACTIONS, "pub(crate) async fn post_merge_actions(");
    assert!(post.contains("Ok(d) if d == base_branch => true"));
    assert!(post.contains("let batched_tag = released_branch && cfg.tag_batch_minutes > 0;"));
    assert!(post.contains("if released_branch {"));
    let line = not_released_line("feature-x", "main");
    assert!(line.contains("`feature-x`") && line.contains("only from `main`"));

    let team = body(RELEASE, "async fn team_release(");
    let guard = team.find("Ok(d) if d == base_branch => {}").unwrap();
    let arm = team.find("schedule_tag_sweep(").unwrap();
    assert!(guard < arm, "the branch is read before a release is armed");
    assert!(team.contains("s.status = Status::Skipped;"));
}

#[test]
fn a_batched_release_tags_the_default_branch() {
    let sweep = &ACTIONS[ACTIONS
        .find("// A release is always of the default branch")
        .unwrap()..];
    let read = sweep
        .find(".get_default_branch(&msg.repo_owner, &msg.repo_name)")
        .unwrap();
    let set = sweep.find("msg.base_branch = b").unwrap();
    let head = sweep
        .find(".get_ref(owner, repo, &msg.base_branch)")
        .unwrap();
    assert!(read < set && set < head);
    assert!(!sweep[..read].contains("if msg.base_branch.is_empty()"));
}
