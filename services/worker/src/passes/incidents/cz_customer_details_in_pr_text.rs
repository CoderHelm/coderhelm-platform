//! cz. Pull requests on CoderHelm's public repositories named details of an
//! install CoderHelm serves: an organization's name, ticket keys, a host. A
//! text guard in CI caught ids and emails, but it could not know names
//! without a list, and a list in a public repository (even hashed) gives
//! them away.
//!
//! Guarantee: CoderHelm checks every pull request of a public repository
//! against terms read from its own records, in memory only, and sets the
//! "Customer details guard" check run. A hit fails it with only the place;
//! fixing the text turns it green; a fork's pull request is skipped with
//! the same output whatever it says; and CoderHelm never merges while the
//! guard fails. Every name below is invented.

use crate::passes::customer_guard::{decide, sources};
use common::customer_guard::{
    self as guard, check_body, guard_state, merge_refusal, InstallRecords, TeamRecords,
};
use serde_json::json;

/// A synthetic install: a customer team and the repository's own team.
fn install() -> InstallRecords {
    InstallRecords {
        teams: vec![
            TeamRecords {
                orgs: vec!["Velmora-Freight".into()],
                repos: vec!["Velmora-Freight/velmora-dispatch".into()],
                jira_hosts: vec!["https://velmorafreight.atlassian.net".into()],
                project_keys: vec!["VLM".into()],
                aws_accounts: vec!["310987654327".into()],
                ..Default::default()
            },
            TeamRecords {
                orgs: vec!["example-oss".into()],
                ..Default::default()
            },
        ],
        install: TeamRecords::default(),
    }
}

fn pr(body: &str, head_repo: &str, assoc: &str) -> serde_json::Value {
    json!({
        "title": "Retry the dispatch sync once",
        "body": body,
        "state": "open",
        "author_association": assoc,
        "user": {"login": "dana", "type": "User"},
        "head": {"sha": "f00dbabe1234", "repo": {"full_name": head_repo}},
        "base": {"repo": {"full_name": "example-oss/helmkit", "private": false}},
    })
}

fn commits(message: &str) -> Vec<serde_json::Value> {
    vec![json!({"sha": "c0ffee99aa11", "commit": {"message": message}})]
}

#[test]
fn a_leaked_name_fails_with_only_its_place_and_the_fix_turns_it_green() {
    let terms = guard::derive(&install(), &["example-oss"]);
    let leaked = pr(
        "## Summary\nVelmora Freight's VLM-4410 runs failed twice.",
        "example-oss/helmkit",
        "MEMBER",
    );
    let msgs = commits("Retry once\n\nSeen in velmora-dispatch on 310987654327");
    let files = vec![json!({
        "filename": "docs/incidents/zz_retry.md",
        "status": "added",
        "changes": 1,
        "patch": "@@ -0,0 +1,1 @@\n+Hit velmorafreight.atlassian.net",
    })];
    let (v, hits) = decide(
        &guard::Origin::from_pr(&leaked),
        || sources(&leaked, &msgs, &files, &|_| None),
        &terms,
    );
    assert_eq!(v.conclusion, "failure");
    assert_eq!(hits.len(), 3);
    let out = check_body("f00dbabe1234", &v).to_string();
    assert!(out.contains("PR body line 2"));
    assert!(out.contains("commit c0ffee9 message line 3"));
    assert!(out.contains("docs/incidents/zz_retry.md:1"));
    let lower = out.to_lowercase();
    for needle in ["velmora", "vlm", "310987654327", "freight", "dispatch"] {
        assert!(!lower.contains(needle), "{needle} reached the check run");
    }
    for k in guard::Kind::ALL {
        assert!(!out.contains(k.as_str()), "the check run names a kind");
    }

    // The same pull request, its text fixed: green.
    let fixed = pr(
        "## Summary\nOne install's runs failed twice.",
        "example-oss/helmkit",
        "MEMBER",
    );
    let msgs = commits("Retry once");
    let (v, _) = decide(
        &guard::Origin::from_pr(&fixed),
        || sources(&fixed, &msgs, &[], &|_| None),
        &terms,
    );
    assert_eq!(v.conclusion, "success");
}

#[test]
fn a_fork_cannot_use_the_check_to_test_a_name() {
    let terms = guard::derive(&install(), &["example-oss"]);
    let probe = pr("Is Velmora Freight a customer?", "outsider/helmkit", "NONE");
    let plain = pr("Is anyone a customer?", "outsider/helmkit", "NONE");
    let a = decide(
        &guard::Origin::from_pr(&probe),
        || panic!("never read"),
        &terms,
    )
    .0;
    let b = decide(
        &guard::Origin::from_pr(&plain),
        || panic!("never read"),
        &terms,
    )
    .0;
    assert_eq!(
        check_body("f00dbabe1234", &a),
        check_body("f00dbabe1234", &b)
    );
    assert_eq!(a.conclusion, "neutral");
}

#[test]
fn coderhelm_never_merges_while_the_guard_fails() {
    let failing = vec![
        json!({"name": guard::CHECK_NAME, "id": 1, "started_at": "t", "status": "completed", "conclusion": "failure"}),
    ];
    assert!(merge_refusal(guard_state(&failing), true, Some(0)).is_some());
    // The one merge call checks the guard before it merges.
    let client = include_str!("../../clients/github.rs");
    let merge = client
        .split("pub async fn merge_pull_request(")
        .nth(1)
        .unwrap();
    let put = merge.find("self.http.put(").unwrap();
    let refusal = merge.find("customer_guard::merge_refusal(").unwrap();
    assert!(refusal < put);
}
