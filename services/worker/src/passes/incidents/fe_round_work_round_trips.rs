//! fe. Fix rounds lost the work they had done, and ran out of time doing it
//! again.
//!
//! - A pull request touched a route folder Next.js spells `%5Fmarkdown` (a
//!   URL segment that starts with `_`). The round's file was read back for
//!   landing with the path pasted into the contents URL as it is; GitHub
//!   decoded `%5F` to `_`, answered 404 for a file that was there, and the
//!   404 was read as "its work branch wasn't there, so it had saved
//!   nothing": the branch holding the work was deleted, twice, and the
//!   coder tried `%255F` paths that wrote new files.
//! - A CI fix round and a review fix round each ran out of their time
//!   budget several steps in a row (each command a sandbox build of one to
//!   two minutes, the checks two to six). The run page said "Resume
//!   continues from there", but the next round reset the WIP branch to the
//!   pull request's head and did the same work again in the same budget:
//!   dozens of checkpoint commits thrown away on one run.
//!
//! Guarantee: every contents URL percent-encodes each path segment and the
//! ref, so a path holding `%`, `#`, `?`, a space or brackets reads and
//! writes the file it names. A file GitHub won't give back is not a missing
//! branch: the branch and its work are kept. A new round on a head that a
//! round left unfinished work on goes on from that WIP branch, told it is
//! continuing; a head that moved since starts fresh.

use serde_json::json;

use super::fixture::fn_src;
use crate::clients::github::contents_url;
use crate::passes::round::{self, fault_of, keeps_work, picks_up, Fault};

/// The segments of `url`'s path after `contents/`, decoded the way GitHub
/// decodes them, and its `ref`.
fn served(url: &str) -> (String, Option<String>) {
    let u = reqwest::Url::parse(url).expect("a URL");
    let segs: Vec<String> = u
        .path_segments()
        .expect("segments")
        .skip_while(|s| *s != "contents")
        .skip(1)
        .map(|s| urlencoding::decode(s).expect("utf-8").into_owned())
        .collect();
    let r = u
        .query_pairs()
        .find(|(k, _)| k == "ref")
        .map(|(_, v)| v.into_owned());
    (segs.join("/"), r)
}

#[test]
fn a_path_with_special_characters_is_the_file_github_serves() {
    for path in [
        "apps/web/src/app/%5Fmarkdown/[[...slug]]/route.ts",
        "apps/web/src/app/(site)/[library]/page.tsx",
        "docs/notes on #3?.md",
        "data/100%.json",
        "plain/path.ts",
    ] {
        for git_ref in [
            "coderhelm-wip/coderhelm/gh-41-card-block",
            "main",
            "a1b2c3d",
        ] {
            let url = contents_url("acme", "site", path, Some(git_ref));
            assert_eq!(
                served(&url),
                (path.to_string(), Some(git_ref.to_string())),
                "{url}"
            );
        }
        let put = contents_url("acme", "site", path, None);
        assert_eq!(served(&put), (path.to_string(), None), "{put}");
    }
}

#[test]
fn a_file_github_wont_give_back_is_not_a_missing_branch() {
    // The run's own error, names invented.
    let file_404 = "`apps/web/src/app/%5Fmarkdown/[[...slug]]/route.ts` couldn't be read from the WIP branch (HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/contents/apps/web/src/app/%5Fmarkdown/[[...slug]]/route.ts?ref=coderhelm-wip/coderhelm/gh-41-card-block))";
    assert_eq!(fault_of(file_404), Fault::Unreadable);
    assert!(
        keeps_work(fault_of(file_404)),
        "the branch holding the work is kept"
    );
    // A branch that really isn't there still reads as one.
    let ref_404 = "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/git/ref/heads/coderhelm-wip/coderhelm/gh-41-card-block)";
    assert_eq!(fault_of(ref_404), Fault::WipMissing);
}

#[test]
fn the_ci_fix_loop_keeps_the_branch_a_fault_left_whole() {
    let src = include_str!("../resume.rs");
    let at = src
        .find("let fault_plan = super::round::plan_for_fault(")
        .expect("the CI fix loop's fault decision");
    let block = &src[at..at + 4000];
    let clean = block
        .find("let clean = fault_plan.clean")
        .expect("clean is decided");
    let delete = block.find(".delete_branch(").expect("the branch delete");
    assert!(
        clean < delete,
        "the WIP branch is deleted before the fault is read"
    );
    assert!(block[clean..delete].contains("if clean"), "{block}");
}

#[test]
fn a_new_round_goes_on_from_unfinished_work_on_the_same_head() {
    // compare(pr branch...WIP branch): the head plus the round's checkpoints.
    let unfinished = json!({"status": "ahead", "ahead_by": 6, "behind_by": 0});
    assert!(picks_up(&unfinished));
    // The head moved since (a person's push, a landed round): fresh.
    for c in [
        json!({"status": "diverged", "ahead_by": 6, "behind_by": 1}),
        json!({"status": "behind", "ahead_by": 0, "behind_by": 2}),
        json!({"status": "identical", "ahead_by": 0, "behind_by": 0}),
        json!({"message": "Not Found"}),
    ] {
        assert!(!picks_up(&c), "{c}");
    }
    assert!(round::CONTINUING.contains("already committed on this branch"));

    let src = include_str!("../resume.rs");
    // The CI fix round opens the carried branch, never a reset one.
    let ci = fn_src(src, "\npub async fn run(");
    let carried = ci
        .find("let carried = super::round::carried(")
        .expect("the CI round reads the unfinished work");
    let open = ci
        .find("super::round::open_wip(&github, &repo_owner, &repo_name, &branch, &carried)")
        .expect("the CI round opens the carried branch");
    assert!(carried < open);
    assert!(!ci.contains("super::round::open_wip(&github, &repo_owner, &repo_name, &branch, \"\")"));
    // The review fix round too: run_fix_round reads a set WIP branch as a
    // continuing round.
    assert!(ci.contains(
        "fix_round.wip_branch =\n                super::round::carried(&github, &repo_owner, &repo_name, &branch).await;"
    ));
    let fix = fn_src(src, "async fn run_fix_round(");
    assert!(fix.contains("let continuing = !round.wip_branch.is_empty();"));
    assert!(fix.contains("super::round::CONTINUING"));
}
