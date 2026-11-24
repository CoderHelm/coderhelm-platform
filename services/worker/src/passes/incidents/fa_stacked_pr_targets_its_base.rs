//! fa. Two tickets asked for the same change: one for the work, and a
//! sibling titled "Merge and deploy" the first, whose body restated it. Two
//! runs built it from the default branch an hour apart, each with one
//! commit per file written. Both made the same small edits (the same query
//! line, the same import) and their code generator committed the same
//! generated files, so several commits of the two branches had the same
//! patch id. The first ticket's run had already opened its own pull request
//! (#41, onto the default branch). Every retry of that ticket continued #41
//! on its kept branch, whose old commits read as copies of the sibling's
//! open pull request (#40) "under new hashes", and stopped before the pull
//! request step: first "rebasing onto that branch didn't apply cleanly",
//! then "too many commits to replay", then "neither did merging #40's
//! branch in". Several runs stopped the same way, each on the
//! same kept branch with the same four commits, until a person stepped in.
//!
//! Today the branch's own change is rebuilt on #40's branch and the run
//! stacks on it. Two gaps were left. #41 already existed with the default
//! branch as its base, and a reused pull request never moved: its diff
//! showed all of #40 beside this change while its description said it was
//! stacked and showed only this change. And on the next attempt, whose base
//! starts again from the default branch, #40's own commits on the branch
//! (the same hashes) read as copies again: the change was rebuilt and force
//! pushed on every retry.
//!
//! Guarantee: a pull request a run reuses (its own open one, or a reopened
//! one) targets the branch the run is stacked on, so its diff is only this
//! change; a base that isn't another open pull request's branch (one a
//! person chose) is left alone. A branch that holds another open pull
//! request's own commits (the same hashes) is built on it: the run takes
//! that branch as its base and rebuilds nothing.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::fixture::*;
use crate::passes::pr::stack_base;
use crate::passes::stack::{self, Copy, Dependency};

const SIBLING: &str = "coderhelm/harbor-40";
const OWN: &str = "coderhelm/harbor-41";

fn open_pr(n: u64, head: &str, base: &str) -> Value {
    json!({
        "number": n,
        "title": format!("HARBOR-{n}: dock card shows the slip photo"),
        "head": {"ref": head, "repo": {"full_name": "harbor/web"}},
        "base": {"ref": base},
    })
}

#[test]
fn a_reused_pull_request_moves_onto_the_branch_the_run_is_stacked_on() {
    let open = vec![open_pr(40, SIBLING, "main"), open_pr(41, OWN, "main")];
    let own = &open[1];
    // The run rebuilt its change on #40's branch: its base is that branch.
    assert_eq!(
        stack_base(own, SIBLING, &open, "harbor", "web").as_deref(),
        Some(SIBLING),
        "#41 kept the default branch as its base while its branch was stacked on #40"
    );
    // Already there: nothing to move.
    let moved = open_pr(41, OWN, SIBLING);
    assert_eq!(stack_base(&moved, SIBLING, &open, "harbor", "web"), None);
    // A base a person chose (a long-lived branch, no open pull request) is
    // left alone, and so is the default branch.
    assert_eq!(stack_base(own, "release-2", &open, "harbor", "web"), None);
    let stacked = open_pr(41, OWN, SIBLING);
    assert_eq!(stack_base(&stacked, "main", &open, "harbor", "web"), None);
    // A fork's branch is never a base.
    let fork = vec![json!({
        "number": 39,
        "head": {"ref": "dock-photo", "repo": {"full_name": "someone/web"}},
        "base": {"ref": "main"},
    })];
    assert_eq!(stack_base(own, "dock-photo", &fork, "harbor", "web"), None);
}

fn copy(commits: &[&str], shared: &[&str]) -> Copy {
    Copy {
        dep: Dependency {
            number: 40,
            branch: SIBLING.into(),
            title: "HARBOR-40: merge and deploy HARBOR-41".into(),
            ..Default::default()
        },
        commits: commits.iter().map(|s| s.to_string()).collect(),
        shared: shared.iter().map(|s| s.to_string()).collect(),
        own: vec!["HARBOR-41: dock card shows the slip photo".into()],
        files: BTreeMap::new(),
    }
}

#[test]
fn a_branch_built_on_the_other_pull_request_is_stacked_without_a_rebuild() {
    // The retry's branch, already rebuilt on #40: #40's own commits (the
    // same hashes) and then this change. Nothing was copied.
    let their = ["a1a1a1a", "b2b2b2b", "c3c3c3c"].map(String::from);
    let matched = vec!["a1a1a1a".to_string(), "b2b2b2b".to_string()];
    let shared = stack::shared(&matched, &their);
    assert_eq!(shared, matched);
    let c = copy(&["a1a1a1a", "b2b2b2b"], &["a1a1a1a", "b2b2b2b"]);
    assert!(
        stack::built_on(&c),
        "the other pull request's own commits read as copies"
    );
    let why = stack::built_on_why(&c);
    assert!(
        why.contains("built on #40's branch `coderhelm/harbor-40`"),
        "{why}"
    );
    assert!(!why.contains("under other hashes"));
    assert!(!why.contains("re-created"));
    // The kept branch of the incident: same changes under new hashes. These
    // are copies, rebuilt as before.
    let old = copy(&["e5e5e5e", "f6f6f6f", "d7d7d7d", "c8c8c8c"], &[]);
    assert!(!stack::built_on(&old));
    // Some copied, some shared: still rebuilt.
    let mixed = copy(&["a1a1a1a", "e5e5e5e"], &["a1a1a1a"]);
    assert!(!stack::built_on(&mixed));
    assert!(!stack::built_on(&copy(&[], &[])));
}

#[test]
fn the_pipeline_moves_reused_pull_requests_and_skips_needless_rebuilds() {
    let pr = include_str!("../pr.rs");
    let run = fn_src(pr, "pub async fn run(");
    let reused = run
        .find("\"Using existing PR\"")
        .expect("the existing pull request path");
    let reopened = run
        .find("\"Reopened prior PR for branch\"")
        .expect("the reopened pull request path");
    for at in [reused, reopened] {
        assert!(
            run[at..at + 200].contains("retarget(github, msg, &pr_data)"),
            "a reused pull request keeps its old base"
        );
    }
    let stack_src = include_str!("../stack.rs");
    for head in ["pub async fn at_start(", "pub async fn before_pr("] {
        let f = fn_src(stack_src, head);
        let check = f
            .find("built_on(&c)")
            .unwrap_or_else(|| panic!("{head} rebuilds a stacked branch"));
        let rebuild = f.find("rebuild_on(").unwrap();
        assert!(
            check < rebuild,
            "{head} rebuilds before it asks whether the branch is built on it"
        );
    }
}
