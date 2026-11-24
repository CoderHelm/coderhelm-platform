//! ry. Two pull requests on a web app, read by a person who asked whether
//! CoderHelm's approvals and resolved comments could go away after a while,
//! because there was too much of them to read.
//!
//! On the first, CoderHelm's "Approved with notes" review from days
//! earlier still stood in full: the file table, the "Why approved" line, a
//! risk block with bars, an FYI list of unconfirmed items, Verification,
//! Tests, "Earlier comments" and the rating footer. Under it a person's
//! commit "Address the review", then the next review, as long again. The
//! earlier review only got out of the way when a new one was posted, and a
//! re-review with nothing new to say posts nothing.
//!
//! On the second, an inline thread of CoderHelm's from the day before was still
//! open: "Blocking (failing check): Lint & Type Check and Deploy Preview
//! are red on this head … I could not read the logs". It was about a head
//! two pushes back; the checks on the head had passed since.
//!
//! Guarantee: CoderHelm's own reviews and threads are tidied on a push, on
//! checks finishing, on a merge or a close and on the review sweep, not only
//! when a new review is posted (`review_supersede::tidy`, carried out by
//! `passes::review_tidy`, queued by the gateway's webhook handler). A body
//! of an older commit than the head becomes one line; on a merge every body
//! does and CoderHelm's resolved threads are hidden; a finding about failing
//! checks resolves once they pass, a finding whose code left the diff
//! resolves, and CoderHelm's comments in them are hidden. A new review keeps
//! only its verdict and what holds the merge in sight (`fold`). A person's
//! review, thread or unanswered reply is never touched, and nothing is
//! posted to announce it.

use common::review_supersede::{self as rs, PrState, Settled};
use serde_json::json;

const OLD: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
const HEAD: &str = "f0e1d2c3b4a5968778695a4b3c2d1e0f98765432";

/// The approval as it stood: every section in sight.
fn long_approval() -> String {
    [
        "## PR Overview",
        "Adds the slip booking status to the slip cards.",
        "| File | What |\n|---|---|\n| `apps/web/src/slips/card.tsx` | Shows the status |",
        "✅ **Approved with notes**: nothing blocks the merge; 2 notes below are for a look.",
        "_Why approved: nothing blocks (no required rule broken, no failing check)._",
        "#### Risk: low\n`██░░░░░░░░` 2/10",
        "#### FYI: facts worth confirming\n- The booking vendor's open date field.",
        "#### Verification\n- typecheck passed",
        "#### Tests\n- e2e covers the closed branch only",
        "#### Earlier comments\n1 settled and resolved · 0 still open",
        "---\n**Was this review right?** Rate it",
    ]
    .join("\n\n")
}

fn reviews(body: &str) -> Vec<rs::Review> {
    super::super::conversation::reviews_of(&json!([
        {"id": 11, "state": "APPROVED", "body": body, "commit_id": OLD,
         "user": {"login": "coderhelm[bot]", "type": "Bot"}},
        {"id": 12, "state": "COMMENTED", "body": "Looks fine to me.", "commit_id": OLD,
         "user": {"login": "sam", "type": "User"}},
    ]))
}

fn pr(state: PrState) -> rs::PrNow {
    let mut p = super::super::review_tidy::pr_now(
        "acme",
        "web",
        42,
        &json!({"state": "open", "head": {"sha": HEAD}}),
    );
    p.state = state;
    p
}

/// The first screenshot: after the person's push the days-old approval
/// is one line naming the commit it approved; the person's review is not
/// touched; a second run does nothing.
#[test]
fn an_approval_of_an_older_commit_becomes_one_line_on_the_push() {
    let r = reviews(&long_approval());
    let t = rs::tidy(&r, &[], &pr(PrState::Open));
    assert_eq!(t.collapse.len(), 1, "{t:?}");
    let (id, body) = &t.collapse[0];
    assert_eq!(*id, 11);
    let shown = body.lines().last().unwrap();
    assert_eq!(
        shown,
        format!("✅ Approved with notes [`a1b2c3d`](https://github.com/acme/web/pull/42/commits/{OLD}) (superseded by later commits)")
    );
    for gone in [
        "Risk",
        "FYI",
        "Verification",
        "Tests",
        "Earlier comments",
        "Rate it",
        "| File |",
    ] {
        assert!(!body.contains(gone), "{gone} still in {body}");
    }
    let after = reviews(body);
    assert!(rs::tidy(&after, &[], &pr(PrState::Open)).is_empty());
}

/// On the merge every body of CoderHelm's is one line, the approval of the
/// merged head too.
#[test]
fn the_merge_leaves_one_line_per_review() {
    let mut r = reviews(&long_approval());
    r[0].commit_id = HEAD.into();
    assert!(rs::tidy(&r, &[], &pr(PrState::Open)).collapse.is_empty());
    let t = rs::tidy(&r, &[], &pr(PrState::Merged));
    assert_eq!(t.collapse.len(), 1);
    assert!(t.collapse[0].1.ends_with("(merged)"));
}

/// A new review is short from the start: the verdict and why in sight, the
/// overview, file table, risk, FYI, verification, tests and earlier
/// comments folded under one collapsed block; the review posts the folded
/// body (and so does the re-review that brings the standing one up to date).
#[test]
fn a_new_review_keeps_only_its_verdict_in_sight() {
    let full = long_approval();
    let (head, rest): (Vec<&str>, Vec<&str>) = full
        .split("\n\n")
        .filter(|p| !p.starts_with("---"))
        .partition(|p| p.starts_with("✅") || p.starts_with("_Why"));
    let body = rs::fold(
        &head.join("\n\n"),
        &rest.join("\n\n"),
        "\n\n---\n**Was this review right?** Rate it",
    );
    let (shown, folded) = body.split_once("<details>").unwrap();
    assert!(shown.starts_with("✅ **Approved with notes**"));
    assert!(shown.contains("_Why approved"));
    for hidden in [
        "## PR Overview",
        "| File |",
        "#### Risk",
        "#### FYI",
        "#### Verification",
        "#### Tests",
        "#### Earlier comments",
    ] {
        assert!(!shown.contains(hidden), "{hidden} in sight");
        assert!(folded.contains(hidden), "{hidden} lost");
    }
    let src = include_str!("../review_pr.rs");
    let at = src
        .find("common::review_supersede::fold(")
        .expect("the review folds its body");
    assert!(src[..at].contains("let mut visible = verdict_line;"));
    // The overview lives only in the review body: no comment of its own is
    // posted (or posted again) anywhere.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![root.clone()];
    let mut callers = Vec::new();
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some("rs")
                && !p.to_string_lossy().contains("incidents")
            {
                let s = std::fs::read_to_string(&p).unwrap();
                if s.contains("review_overview::markdown(") {
                    callers.push(p.strip_prefix(&root).unwrap().to_string_lossy().to_string());
                }
            }
        }
    }
    assert_eq!(callers, vec!["passes/review_pr.rs".to_string()]);
}

fn failing_check_thread(person_reply: bool) -> Vec<rs::Thread> {
    let mut comments = vec![json!({
        "id": "C1", "isMinimized": false,
        "body": "🔴 **Blocking (failing check)**: Lint & Type Check and Deploy Preview are red on this head. I could not read the logs, so the cause is not confirmed.",
        "author": {"login": "coderhelm"}, "pullRequestReview": {"databaseId": 11}
    })];
    if person_reply {
        comments.push(json!({
            "id": "P1", "isMinimized": false, "body": "Isn't this the flaky deploy?",
            "author": {"login": "kim"}, "pullRequestReview": {"databaseId": 13}
        }));
    }
    super::super::conversation::threads_of(&[json!({
        "id": "T1", "isResolved": false, "isOutdated": false, "line": 12,
        "path": ".github/workflows/ci.yml",
        "comments": {"nodes": comments}
    })])
}

/// The second screenshot: once the head's checks pass, the failing-check
/// thread resolves and CoderHelm's comment in it is hidden, with no new
/// review. While the checks run, it stays.
#[test]
fn a_failing_check_thread_settles_when_the_head_goes_green() {
    let threads = failing_check_thread(false);
    let mut now = pr(PrState::Open);
    now.checks_pass = rs::checks_pass(&json!({"check_runs": [
        {"name": "Lint & Type Check", "status": "completed", "conclusion": "success", "app": {"slug": "github-actions"}},
        {"name": "Deploy Preview", "status": "in_progress", "conclusion": null, "app": {"slug": "vercel"}},
        {"name": "CoderHelm review", "status": "in_progress", "conclusion": null, "app": {"slug": "coderhelm"}}
    ]}));
    assert!(rs::tidy(&[], &threads, &now).is_empty());
    now.checks_pass = rs::checks_pass(&json!({"check_runs": [
        {"name": "Lint & Type Check", "status": "completed", "conclusion": "success", "app": {"slug": "github-actions"}},
        {"name": "Deploy Preview", "status": "completed", "conclusion": "success", "app": {"slug": "vercel"}},
        {"name": "CoderHelm review", "status": "in_progress", "conclusion": null, "app": {"slug": "coderhelm"}}
    ]}));
    let t = rs::tidy(&[], &threads, &now);
    assert_eq!(t.resolve, vec![("T1".to_string(), Settled::ChecksPass)]);
    assert_eq!(t.minimize, vec!["C1".to_string()]);
}

/// A person who answered in that thread keeps it in sight until CoderHelm
/// answers them.
#[test]
fn a_persons_unanswered_reply_keeps_the_thread() {
    let threads = failing_check_thread(true);
    let mut now = pr(PrState::Open);
    now.checks_pass = Some(true);
    assert!(rs::tidy(&[], &threads, &now).is_empty());
    now.state = PrState::Merged;
    assert!(rs::tidy(&[], &threads, &now).is_empty());
}

/// A finding on a file the pull request no longer changes resolves.
#[test]
fn a_finding_on_code_that_left_the_diff_settles() {
    let mut now = pr(PrState::Open);
    now.changed_paths = Some(vec!["apps/web/src/slips/card.tsx".into()]);
    let t = rs::tidy(&[], &failing_check_thread(false), &now);
    assert_eq!(t.resolve, vec![("T1".to_string(), Settled::FileGone)]);
}

/// Every trigger reaches the tidy: the worker runs it for its message and
/// on the sweep; the gateway queues it on a push, a merge or a close, and
/// checks finishing.
#[test]
fn every_trigger_reaches_the_tidy() {
    let main = include_str!("../../main.rs");
    assert!(main.contains("WorkerMessage::ReviewTidy(msg) =>"));
    assert!(main.contains("passes::review_tidy::run(&state, &msg)"));
    let sweep = include_str!("../review_sweep.rs");
    assert!(
        sweep.contains("super::review_tidy::pull_request(&github, owner, name, number, Some(pr))")
    );
    let hook = include_str!("../../../../gateway/src/routes/github_webhook.rs");
    for reason in ["\"push\"", "\"merged\"", "\"closed\"", "\"checks\""] {
        assert!(hook.contains(reason), "{reason}");
    }
    assert!(hook.matches("queue_review_tidy(state,").count() >= 3);
}
