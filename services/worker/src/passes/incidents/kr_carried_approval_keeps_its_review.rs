//! kr. A pull request on a web app, approved by CoderHelm a week earlier
//! after a full review. A person merged the base branch into it. CoderHelm
//! saw the PR's own change was the one it approved and posted a new approval
//! on the new head whose whole body was one line: "Approved: the new commits
//! only bring in the base branch; the PR's own change is the same as the
//! approved `abc1234`." plus the rating footer. Posting it rewrote every
//! earlier review of CoderHelm's to "Outdated: see CoderHelm's latest
//! review", and the tidy between reviews cuts any review of an older commit
//! to one line, so the full review the approval rested on was gone from
//! sight. The pull request's only visible approval had no review in it, and
//! a person pointed out there was no review left to check.
//!
//! Guarantee: a carried approval stands on its own. Its body is the verdict,
//! why the earlier review still holds for the new head ("the PR's own change
//! is unchanged since `<sha>`; the new commits only bring in `<base>`"), then
//! that review as it was posted, from its stored record (what was checked,
//! the criteria, the tests, the notes); a carry of a carry keeps the one
//! review, never nests. While a carried approval is CoderHelm's latest word,
//! the review it rests on is never rewritten, cut to one line or hidden, nor
//! its comments hidden or its threads resolved. With no stored review of the
//! approved commit to carry (or only an old one-line carry), the PR is
//! reviewed in full instead of approved with nothing in sight.

use super::super::review_pr::{carried_approval_body, carried_review, CarriedApproval};
use super::super::review_sweep::ReviewRecord;
use common::review_supersede::{self as rs, PrState};
use serde_json::json;

/// The approved commit (the full review) and the head after the person
/// merged the base branch in.
const A: &str = "abc12340c1f2e3d4a5b6c7d8e9f0a1b2c3d4e5f6";
const B: &str = "7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f";

/// The full review of `A` as it was posted, without the rating footer.
fn full_review() -> String {
    [
        "✅ **Approved with notes**: nothing blocks the merge; 1 note below is for a look.",
        "_Why approved: nothing blocks (no required rule broken, no failing check)._",
        "<details>\n<summary>Details: PR Overview · Risk · Criteria · Verification · Tests</summary>",
        "## PR Overview\n| File | What |\n|---|---|\n| `apps/web/src/signup/form.tsx` | Validates the date of birth |",
        "#### Risk: low",
        "#### Criteria\n- Ages under 18 are refused: proven by `form.test.tsx` \"refuses a minor\"",
        "#### Verification\n- typecheck passed\n- unit tests passed",
        "#### Tests\n- the under-18 branch is covered; the leap-day branch is not",
        "#### Additional findings\n- [low] apps/web/src/signup/form.tsx:41 — The error text is not translated",
        "</details>",
    ]
    .join("\n\n")
}

fn footer() -> String {
    super::super::review_pr::rating_footer("https://app.example", "acme", "web", 42)
}

/// CoderHelm's stored records, newest first: the full review of `A`.
fn records() -> Vec<ReviewRecord> {
    vec![ReviewRecord {
        verdict: "APPROVE".into(),
        posted_as: "APPROVE".into(),
        head_sha: A.into(),
        created_at: "2030-03-05T15:00:00Z".into(),
        body: "Validates the date of birth.".into(),
        posted_body: full_review(),
        ..Default::default()
    }]
}

/// The carried approval posted on `B`, as `carry_approval` builds it.
fn carried_on_b() -> String {
    let review = carried_review(&records(), A).expect("the stored review of A");
    carried_approval_body(
        &CarriedApproval {
            approved: A,
            only_base_merged: true,
            base_branch: "main",
            review: &review,
        },
        B,
    )
}

/// GitHub's reviews: the full one on `A`, a person's, then the carried one.
fn reviews(carried: &str) -> Vec<rs::Review> {
    super::super::conversation::reviews_of(&json!([
        {"id": 11, "state": "APPROVED", "body": format!("{}{}", full_review(), footer()),
         "commit_id": A, "user": {"login": "coderhelm[bot]", "type": "Bot"}},
        {"id": 12, "state": "COMMENTED", "body": "Thanks, merging main in.", "commit_id": A,
         "user": {"login": "sam", "type": "User"}},
        {"id": 13, "state": "APPROVED", "body": format!("{carried}{}", footer()),
         "commit_id": B, "user": {"login": "coderhelm[bot]", "type": "Bot"}},
    ]))
}

/// The full review's note, an inline thread of CoderHelm's.
fn note_thread() -> rs::Thread {
    rs::Thread {
        id: "T1".into(),
        path: "apps/web/src/signup/form.tsx".into(),
        first_body: "The error text is not translated.".into(),
        comments: vec![rs::ThreadComment {
            node_id: "C1".into(),
            own: true,
            review_id: 11,
            minimized: false,
        }],
        ..Default::default()
    }
}

fn open_pr() -> rs::PrNow {
    super::super::review_tidy::pr_now(
        "acme",
        "web",
        42,
        &json!({"state": "open", "head": {"sha": B}}),
    )
}

/// The latest visible review on `B` carries the full review's content,
/// stated against `B`, and names the commit it rests on.
#[test]
fn the_carried_approval_carries_the_review_it_rests_on() {
    let body = carried_on_b();
    assert!(
        body.contains(
            "✅ **Approved with notes** on `7e8f9a0`: the PR's own change is unchanged since \
             `abc1234`; the new commits only bring in `main`."
        ),
        "{body}"
    );
    for section in [
        "_Why approved: nothing blocks",
        "## PR Overview",
        "#### Criteria\n- Ages under 18 are refused",
        "#### Verification\n- typecheck passed",
        "#### Tests\n- the under-18 branch is covered",
        "The error text is not translated",
    ] {
        assert!(body.contains(section), "{section} missing from {body}");
    }
    // Not the one line the incident posted.
    assert!(body.lines().count() > 10, "{body}");
    assert_eq!(rs::carried_from(&body).as_deref(), Some(A));
}

/// Posting the carried approval rewrites nothing it rests on: the review of
/// `A` keeps its body, its note's comment stays in sight and its thread
/// open. A person's review is never touched either.
#[test]
fn posting_the_carry_keeps_the_review_it_rests_on() {
    let r = reviews(&carried_on_b());
    let p = rs::plan(&r, &[note_thread()], 13, &[]);
    assert!(!p.edit.contains(&11), "{p:?}");
    assert!(!p.minimize.contains(&"C1".to_string()), "{p:?}");
    assert!(!p.resolve.contains(&"T1".to_string()), "{p:?}");
    assert!(!p.edit.contains(&12), "{p:?}");
}

/// The tidy between reviews (a push, checks finishing, the sweep) never cuts
/// the review the latest carried approval rests on, nor the carried
/// approval; a second run does nothing.
#[test]
fn the_tidy_never_cuts_the_review_a_carry_rests_on() {
    let r = reviews(&carried_on_b());
    let t = rs::tidy(&r, &[note_thread()], &open_pr());
    assert!(t.collapse.is_empty(), "{t:?}");
    assert!(t.minimize.is_empty(), "{t:?}");
    assert_eq!(rs::kept_for_carry(&r), vec![11]);
}

/// The approval already posted the old way (one line, no review in it) is
/// still read as resting on `A`: the review of `A` is not cut while it
/// stands.
#[test]
fn an_old_one_line_carry_still_keeps_its_review() {
    let one_line = "✅ **Approved**: the new commits only bring in the base branch; the PR's \
                    own change is the same as the approved `abc1234`.";
    let r = reviews(one_line);
    assert!(rs::tidy(&r, &[], &open_pr()).collapse.is_empty());
    assert!(rs::plan(&r, &[], 13, &[]).edit.is_empty());
}

/// Once a real review follows (the PR's own change moved), it is the live
/// one: the review of `A` and the carried approval are both cut as before.
/// A merge cuts every body.
#[test]
fn a_later_review_supersedes_both() {
    let mut r = reviews(&carried_on_b());
    r.push(rs::Review {
        id: 14,
        own: true,
        state: "APPROVED".into(),
        body: "✅ **Approved**: the new validation reads well.".into(),
        commit_id: "0a1b2c3d4e5f60718293a4b5c6d7e8f901234567".into(),
    });
    assert_eq!(rs::plan(&r, &[], 14, &[]).edit, vec![11, 13]);
    let merged = rs::tidy(
        &reviews(&carried_on_b()),
        &[],
        &rs::PrNow {
            state: PrState::Merged,
            ..open_pr()
        },
    );
    let cut: Vec<u64> = merged.collapse.iter().map(|(id, _)| *id).collect();
    assert_eq!(cut, vec![11, 13]);
}

/// A carry of a carry (the base branch merged in again) carries the same
/// review once, from the carried approval's own record.
#[test]
fn a_second_merge_carries_the_same_review_once() {
    let first = carried_on_b();
    let stored = vec![ReviewRecord {
        verdict: "APPROVE".into(),
        posted_as: "APPROVE".into(),
        head_sha: B.into(),
        body: first.clone(),
        posted_body: first,
        ..Default::default()
    }];
    let review = carried_review(&stored, B).expect("the carried approval's record");
    let again = carried_approval_body(
        &CarriedApproval {
            approved: B,
            only_base_merged: true,
            base_branch: "main",
            review: &review,
        },
        "c3d4e5f60718293a4b5c6d7e8f90123456789abc",
    );
    assert!(again.contains("unchanged since `7e8f9a0`"), "{again}");
    assert!(again.contains("The review of `abc1234` below"), "{again}");
    assert_eq!(again.matches("#### Criteria").count(), 1, "{again}");
}

/// No stored review to carry, or only a one-line carry with no review in
/// it: nothing to carry, and the pipeline reviews the PR in full instead.
#[test]
fn nothing_to_carry_means_a_real_review() {
    assert_eq!(carried_review(&[], A), None);
    let one_line = ReviewRecord {
        verdict: "APPROVE".into(),
        posted_as: "APPROVE".into(),
        head_sha: A.into(),
        body: "✅ **Approved**: the new commits only bring in the base branch; the PR's own \
               change is the same as the approved `1234abc`."
            .into(),
        ..Default::default()
    };
    assert_eq!(carried_review(&[one_line], A), None);
    let mut asked_changes = records();
    asked_changes[0].verdict = "REQUEST_CHANGES".into();
    assert_eq!(carried_review(&asked_changes, A), None);
    // A record older than the posted body still carries its summary and
    // findings.
    let mut older = records();
    older[0].posted_body = String::new();
    assert_eq!(
        carried_review(&older, A).as_deref(),
        Some("Validates the date of birth.")
    );
    // The pipeline asks for the review before carrying, and reviews in full
    // without it.
    let src = include_str!("../review_pr.rs");
    let carry = src.find("carried_review(&records, &approved)").unwrap();
    let full = src[carry..]
        .find("if nothing_new && carried.is_none() {")
        .unwrap();
    assert!(src[carry + full..]
        .starts_with("if nothing_new && carried.is_none() {\n                    warn!"));
    assert!(src.contains("let posted_body = full_body.strip_suffix(&footer)"));
}
