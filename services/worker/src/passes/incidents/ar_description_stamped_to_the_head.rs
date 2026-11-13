//! ar. A dependency run's pull request was described from a head, and then
//! the head moved on without it. A feedback round changed an override in
//! the manifest and the description was rebuilt on that commit, where the
//! lockfile still resolved the old version ("1.4.4-lts.1, not moved on the
//! head"). The lockfile was regenerated in the commits after it, the run's
//! checks ran again and the run completed. The description still said not
//! moved, still counted the advisory as not fixed, still listed the package
//! under open findings and "the lockfile is out of sync" under Open items.
//! CoderHelm's own review of the final head saw it and left it as a note.
//!
//! What let it through:
//!
//! - the checks step on an open pull request wrote its own sections and
//!   never the ones computed from the lockfile, and a regenerated lockfile
//!   was pushed with nothing after it;
//! - a review wrote its findings into the copy of the description it had
//!   read when it started, so a review that began before a rebuild and
//!   ended after it put the older description back, and a review of a head
//!   already replaced wrote that head's findings onto the new one;
//! - an item about the lockfile was CoderHelm's own and so was never
//!   checked again;
//! - a finding that the description was behind the code was a wording nit,
//!   and nits on CoderHelm's own pull request ended the loop.
//!
//! Guarantee: the description names the head it was built for, and
//! whenever that is not the pull request's head it is rebuilt: after every
//! message in which CoderHelm moved the branch (seen where every request
//! leaves the GitHub client), in the checks and settle step, before a
//! review of its own pull request, in the pull request watch, and at the
//! end of the run, which never completes while the two differ. A review
//! writes on the description as it is now and only for the head it
//! reviewed. A lockfile-out-of-sync item is checked on the head. A finding
//! that the description is behind is settled by the rebuild, and every
//! finding that is not pure style is CoderHelm's to act on.

use super::fixture::*;
use crate::clients::github::branch_moved_by;
use crate::passes::completeness::{self, Section};
use crate::passes::{description, own_review, security_fixes, settle};
use common::dep_head::{self, LockAt};
use common::security_fixes::{self as sec, Advisory, Bump, Finding, FindingState, FixUnit};

fn unit() -> FixUnit {
    FixUnit {
        repo: format!("{OWNER}/{REPO}"),
        ecosystem: "npm".into(),
        bundle: true,
        bumps: vec![Bump {
            package: "xml-kit".into(),
            from: "1.4.4-lts.1".into(),
            to: "2.0.3".into(),
            major: true,
            manifest: "pnpm-lock.yaml".into(),
        }],
        ..Default::default()
    }
}

fn finding() -> (Finding, FindingState) {
    (
        Finding {
            id: "f-1".into(),
            package: "xml-kit".into(),
            version: "1.4.4-lts.1".into(),
            fixed_version: Some("2.0.3".into()),
            manifest: "pnpm-lock.yaml".into(),
            advisory: Advisory {
                primary: "GHSA-xxxx-0009".into(),
                ids: vec!["GHSA-xxxx-0009".into()],
                ..Default::default()
            },
            ..Default::default()
        },
        FindingState::default(),
    )
}

fn at(head_version: &str) -> LockAt {
    let copies = |v: &str| Some(vec![("xml-kit".to_string(), v.to_string())]);
    LockAt {
        path: "pnpm-lock.yaml".into(),
        base: copies("1.4.4-lts.1"),
        head: copies(head_version),
        why: String::new(),
    }
}

/// The description as it was built on `sha`, stamped for it.
fn described(sha: &str, resolved: &str) -> String {
    let (unit, (f, st)) = (unit(), finding());
    let head = dep_head::resolve(&unit, sha, &[at(resolved)]);
    let body = sec::pr_body_at_head(
        &unit,
        &[(&f, &st)],
        "Bumped.",
        &common::upgrade_notes::WhatToCheck::default(),
        &sec::BodyLinks::default(),
        &head,
    );
    description::with_stamp(&body, sha)
}

/// The manifest's head and the lockfile's head: the description built on
/// the first is not current on the second, and the rebuild leaves nothing
/// of the first.
#[test]
fn a_description_of_an_earlier_head_is_rebuilt_for_the_head() {
    let (h1, h2) = ("1111111aaaa", "2222222bbbb");
    let on_manifest = described(h1, "1.4.4-lts.1");
    assert!(on_manifest.contains("1.4.4-lts.1, not moved on the head"));
    assert!(on_manifest.contains("Not fixed on the head: GHSA-xxxx-0009"));
    assert!(description::is_current(&on_manifest, h1));
    // The lockfile is regenerated: another head.
    assert!(!description::is_current(&on_manifest, h2), "stale: rebuilt");
    let fresh = described(h2, "2.0.3");
    let rebuilt = description::with_stamp(&security_fixes::rebuilt_body(&on_manifest, &fresh), h2);
    let mut end = Ending::default();
    end.said(rebuilt.clone());
    assert!(
        end.says("| `xml-kit` | 1.4.4-lts.1 to 2.0.3 (major) |"),
        "{rebuilt}"
    );
    assert!(end.says("Fixes 1 of 1 advisory"), "{rebuilt}");
    end.never_says("not moved on the head");
    end.never_says("Not fixed on the head");
    assert!(description::is_current(&rebuilt, h2));
    assert_eq!(rebuilt.matches(description::STAMP_START).count(), 1);
}

/// A review that started on the earlier description and ended after the
/// rebuild: written the old way it puts the earlier description back.
/// Written on the description as it is now, it adds only its section; and
/// for a head already replaced it writes nothing.
#[test]
fn a_review_never_writes_back_the_description_it_started_with() {
    let read_at_start = described("1111111aaaa", "1.4.4-lts.1");
    let now = described("2222222bbbb", "2.0.3");
    let section = own_review::section(&["Scope the override (`package.json:12`)".to_string()]);
    // The old way: the section added to the copy read at the start.
    let clobbered =
        completeness::with_section(&read_at_start, Section::OpenFindings, section.clone());
    assert!(
        clobbered.contains("not moved on the head"),
        "what the run shipped"
    );
    // On the description as it is now.
    let edited = completeness::with_section(&now, Section::OpenFindings, section);
    assert!(!edited.contains("not moved on the head"));
    assert!(edited.contains("1.4.4-lts.1 to 2.0.3 (major)"));
    assert!(edited.contains("Scope the override"));
    assert!(
        description::is_current(&edited, "2222222bbbb"),
        "the stamp survives a section write"
    );
    let review = include_str!("../review_pr.rs");
    let write = &review[review
        .find("let section = super::own_review::section(&own_open);")
        .unwrap()..];
    let write = &write[..1800];
    assert!(write.contains("description::edit("));
    assert!(
        write.contains("Some(&head_sha)"),
        "only for the head it reviewed"
    );
    assert!(
        !write.contains("pr_body"),
        "never the copy read when the review started"
    );
    let edit = include_str!("../description.rs");
    let edit = fn_src(edit, "pub async fn edit(");
    assert!(edit.find("get_pull_request(").unwrap() < edit.find("change(body)").unwrap());
    assert!(edit.contains("Edited::HeadMoved"));
}

/// Every push is seen where requests leave the client, and the message
/// that made it ends with the rebuild. The run cannot complete on another
/// head's description.
#[test]
fn every_push_is_followed_by_the_rebuild_and_no_run_completes_stale() {
    // The pushes of this run: a round's landing, a regenerated lockfile, a
    // take-out. Each is a ref update, a contents write or a merge.
    for (method, path, body) in [
        (
            "PATCH",
            "/repos/acme/site/git/refs/heads/coderhelm/security-1",
            r#"{"sha":"a","force":false}"#,
        ),
        (
            "POST",
            "/repos/acme/site/git/refs",
            r#"{"ref":"refs/heads/coderhelm/security-1","sha":"a"}"#,
        ),
        (
            "PUT",
            "/repos/acme/site/contents/pnpm-lock.yaml",
            r#"{"branch":"coderhelm/security-1"}"#,
        ),
        (
            "POST",
            "/repos/acme/site/merges",
            r#"{"base":"coderhelm/security-1","head":"main"}"#,
        ),
    ] {
        assert_eq!(
            branch_moved_by(method, path, body.as_bytes()),
            Some(("acme".into(), "site".into(), "coderhelm/security-1".into())),
            "{method} {path}"
        );
    }
    let client = include_str!("../../clients/github.rs");
    let tracked = fn_src(client, "async fn send_tracked(");
    assert!(tracked.contains("branch_moved_by(") && tracked.contains("note_branch_moved("));
    let main = include_str!("../../main.rs");
    let handle = fn_src(main, "async fn handle_message(");
    let inner = handle.find("handle_message_inner(").unwrap();
    assert!(handle[inner..].contains("description::reconcile_moved("));
    // The paths that pushed in this run and called nothing: all end in a
    // reconcile now, without knowing about it.
    let pr = include_str!("../pr.rs");
    assert!(fn_src(pr, "pub async fn apply_checks(").contains("description::reconcile("));
    let resume = include_str!("../resume.rs");
    let done = fn_src(resume, "async fn complete_run_with_status(");
    let gate = done.find("description::reconcile(").unwrap();
    assert!(gate < done.find("redrive_stale_items(").unwrap());
    assert!(done[gate..].contains("return Err("));
}

/// "The lockfile is out of sync" was true of the manifest's head. On the
/// head with the regenerated lockfile the checks install and pass, and the
/// item goes.
#[test]
fn an_item_about_the_earlier_head_does_not_survive_the_checks_of_this_one() {
    let item = crate::passes::proof::Finding {
        point: "check_blocked".into(),
        detail: "The lockfile is out of sync with the manifests this pull request changes.".into(),
        ..Default::default()
    };
    let facts = settle::Facts {
        dep_change: true,
        planned_by_rules: true,
        ..Default::default()
    };
    assert_eq!(
        settle::classify(&item, &facts),
        settle::Class::Run(vec![settle::Action::LockSync])
    );
    let mut rec = settle::Record::default();
    rec.for_head("2222222bbbb");
    rec.checked = true;
    rec.ran = vec!["build".into()];
    rec.passed = vec!["build".into()];
    let settled = settle::apply(vec![item], &facts, &rec);
    assert!(settled.open.is_empty(), "{:?}", settled.open);
    let mut end = Ending::default();
    end.said(crate::passes::proof::gaps_section(&settled.open));
    end.never_says("out of sync");
}

/// The reviewer's note on the final head: the table still says not moved.
/// It is never optional, the rebuild settles it, and an optional
/// improvement is acted on too; only pure style is left.
#[test]
fn a_finding_on_coderhelms_own_pull_request_is_acted_on() {
    let f = |v: serde_json::Value| -> crate::passes::review_agent::Finding {
        let mut base =
            serde_json::json!({"file": "package.json", "line": 12, "title": "t", "body": "b"});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    };
    let stale = f(serde_json::json!({
        "title": "The PR description still says xml-kit was not moved",
        "body": "pnpm-lock.yaml resolves xml-kit to 2.0.3; the description's table still lists `1.4.4-lts.1, not moved on the head` and counts its advisory as not fixed.",
        "category": "wording", "severity": "low", "optional": true
    }));
    let improvement = f(
        serde_json::json!({"title": "Drop the unused override", "category": "maintainability", "optional": true}),
    );
    let style = f(serde_json::json!({"title": "Rename the script", "category": "naming"}));
    assert!(own_review::about_description(&stale));
    let required = own_review::required(&[stale, improvement, style], &[]);
    assert_eq!(required.len(), 2, "{required:?}");
    assert!(required[0].starts_with("The PR description still says"));
    assert!(required[1].starts_with("Drop the unused override"));
    // The review rebuilds the description for such a finding before it
    // decides what is open, and drops the finding the rebuild settled.
    let review = include_str!("../review_pr.rs");
    let at = review
        .find("findings.iter().any(super::own_review::about_description)")
        .unwrap();
    let after = &review[at..at + 1600];
    assert!(after.contains("description::rebuild("));
    assert!(after.contains("findings.retain("));
    assert!(
        after.find("description::rebuild(").unwrap() < after.find("own_review::required(").unwrap()
    );
}
