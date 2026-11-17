//! ow. CoderHelm's own pull request fixed a sport filter that listed one
//! sport twice. A later review of it listed findings under "Holding the
//! merge" that the same review said no longer applied: the earlier thread
//! about the dropdown, whose code the fix rounds had replaced, and two
//! findings the review itself had kept as notes ("on code that hasn't
//! changed since my previous review, so these are notes and don't change
//! the verdict"). On CoderHelm's own pull request every late note was also
//! chained into its open work, and that work is named as holding the merge,
//! so each of those findings had two verdicts in one review.
//!
//! Guarantee: one state per finding before the body is written
//! (`review_one_verdict::settle`): a thread that is resolved, settled by
//! this review or about code no longer in the change, a finding raised
//! late, and an earlier hold the review now calls a note never hold the
//! merge; "Holding the merge" is built only from what still blocks at the
//! head. The finished body is checked before it is posted
//! (`review_one_verdict::enforce`): no finding listed as holding is also
//! called a note, settled or no longer applying.

use crate::passes::own_review;
use crate::passes::review_agent::{verdict_heading, Finding};
use crate::passes::review_one_verdict::{self as one, Key, State};
use crate::passes::review_policy::{self, demote, named_holding, LATE_NOTE};
use crate::passes::review_verdict::{HoldKind, Verdict};

const FILTERS: &str = "apps/web/src/utils/filterMaps.ts";
const TEST: &str = "apps/web/src/test/components/filter-nav-sport-grid.test.tsx";

/// The earlier thread: its code was replaced by the fix rounds.
fn earlier_thread_line() -> String {
    own_review::thread_line(
        FILTERS,
        632,
        "**Drop-ins sport dropdown can now list one sport twice**\n\nTwo sport ids share a slug.",
    )
}

/// The two findings the review raised late, as the checks wrote them.
fn late() -> Vec<Finding> {
    let mut out: Vec<Finding> = crate::passes::review_rules::wider_findings(&[
        crate::passes::minimal_change::Widening {
            kind: crate::passes::minimal_change::Kind::Parallel,
            path: FILTERS.into(),
            what: "`dedupeSportOptionsBySlug` next to the existing `dedupeSportsBySlug`".into(),
        },
        crate::passes::minimal_change::Widening {
            kind: crate::passes::minimal_change::Kind::TestSize,
            path: TEST.into(),
            what: "236 added lines against 41 lines of changed code".into(),
        },
    ]);
    for f in out.iter_mut() {
        demote(f, LATE_NOTE);
    }
    out
}

/// The model's summary, as it wrote it.
const SUMMARY: &str = "The fix keeps one option per sport slug. The earlier dropdown comment \
     on `apps/web/src/utils/filterMaps.ts` about listing one sport twice no longer applies: \
     the new helper replaced that code.";

/// The review's body from its parts, the way `review_pr` writes them.
fn body(verdict: &Verdict, notes: usize, summary: &str, held_late: &[Finding]) -> String {
    format!(
        "{}\n\n_{}_\n\n{summary}{}",
        verdict_heading(verdict, notes),
        verdict.why(0, notes),
        one::late_markdown(held_late)
    )
}

/// What the review did: late notes chained into CoderHelm's open work, the
/// earlier thread still open, and every one named as holding.
#[test]
fn the_old_assembly_gave_findings_two_verdicts_and_the_check_names_them() {
    let held = late();
    let own_open = own_review::required(&[], &held);
    let open = [(FILTERS.to_string(), 632u64, earlier_thread_line())];
    let open: Vec<(String, u64, String)> = open
        .iter()
        .map(|(p, l, t)| (p.clone(), *l, format!("**{t}**")))
        .collect();
    let verdict = Verdict::from_holding(named_holding(&[], &open, &[], false, &own_open, &[]));
    assert_eq!(verdict.holding().len(), 3);
    let b = body(&verdict, held.len(), SUMMARY, &held);
    let found = one::check(&b);
    assert_eq!(found.len(), 3, "{found:#?}\n{b}");
    assert!(found.iter().any(|c| c.state == State::NoLongerApplies));
    assert_eq!(
        found.iter().filter(|c| c.state == State::Note).count(),
        2,
        "{found:#?}"
    );
    // The last word before posting removes the words that contradict the
    // hold, so even this body would never go out with two verdicts.
    let (fixed, _) = one::enforce(&b);
    assert!(one::check(&fixed).is_empty(), "{fixed}");
}

/// What the review does now: one state per finding, then the body.
#[test]
fn every_finding_has_one_verdict() {
    let held = late();
    // Late findings are not part of the open work that holds the merge.
    let own_open = own_review::required(&[], &[]);
    assert!(own_open.is_empty());
    // On their own, late notes start no round: nothing holds, the draft
    // isn't held, and the run ends (`review_pr` writes the work list only
    // when something in it holds).
    let own_late = one::late_lines(&held);
    assert_eq!(own_late.len(), 2);
    assert!(own_late.iter().all(|l| l.contains(one::LATE_MARK)));
    let src = include_str!("../review_pr.rs");
    assert!(src.contains("let own_open: Vec<String> = if own_open.is_empty() {"));

    let open = vec![(FILTERS.to_string(), 632u64, earlier_thread_line())];
    let late_keys: Vec<Key> = held.iter().map(Key::of_finding).collect();
    let settled = one::settle(one::Inputs {
        holding: named_holding(&[], &open, &[], false, &own_open, &[]),
        late: &late_keys,
        prose: SUMMARY,
        ..Default::default()
    });
    assert!(settled.holding.is_empty(), "{:?}", settled.holding);
    assert_eq!(settled.entries[0].state, State::NoLongerApplies);
    let verdict = Verdict::from_holding(settled.holding);
    assert_eq!(verdict.event(), "APPROVE");
    let b = body(&verdict, held.len(), &settled.prose, &held);
    assert!(one::check(&b).is_empty(), "{b}");
    assert!(!b.contains(one::HOLDING_HEAD));
    assert!(b.contains("my previous review, which missed them"));
}

/// A thread a person resolved, or one about a file the pull request no
/// longer changes, never holds; one the review called gone settles even a
/// required rule.
#[test]
fn resolved_and_gone_threads_never_hold() {
    use crate::clients::github::BotReviewThread;
    let thread = |id: &str, path: &str, resolved: bool| BotReviewThread {
        id: id.into(),
        root_id: 1,
        path: path.into(),
        line: 632,
        outdated: false,
        body: "**Drop-ins sport dropdown can now list one sport twice**".into(),
        replies: vec![],
        resolved,
        resolved_by_person: resolved,
    };
    let threads = vec![
        thread("t1", FILTERS, true),
        thread("t2", "apps/web/src/old.ts", false),
    ];
    let changed = vec![FILTERS.to_string()];
    let closed = crate::passes::review_pr::closed_threads(&threads, &[], &[], &changed);
    assert_eq!(closed.len(), 2);
    assert_eq!(closed[0].1, State::Settled);
    assert_eq!(closed[1].1, State::NoLongerApplies);
    assert!(crate::passes::review_rules::settles(
        crate::passes::review_rules::REQUIRED_MARK,
        crate::passes::review_agent::GONE
    ));
    let settled = one::settle(one::Inputs {
        holding: vec![crate::passes::review_verdict::Holding::new(
            HoldKind::KeptFromEarlier,
            earlier_thread_line(),
        )],
        closed: &closed,
        ..Default::default()
    });
    assert!(settled.holding.is_empty());
}

/// The review settles before it writes, and checks the body before it
/// posts.
#[test]
fn the_review_settles_before_it_writes_and_checks_before_it_posts() {
    let src = include_str!("../review_pr.rs");
    let settle = src.find("review_one_verdict::settle(").expect("settle");
    let verdict = src
        .find("let mut final_verdict = super::review_verdict::Verdict::from_holding(one.holding)")
        .expect("the verdict comes from the settled holds");
    assert!(settle < verdict);
    let enforce = src
        .find("review_one_verdict::enforce(&full_body)")
        .expect("enforce");
    let post = src[enforce..]
        .find("review_verdict::post(")
        .expect("posted after the check");
    assert!(post > 0);
    // Late findings are never chained into the open work that holds.
    assert!(src.contains("super::own_review::required(&findings, &[])"));
    assert!(!src.contains("super::own_review::required(&findings, &held_late)"));
    let _ = review_policy::NEW_CHECK_NOTE;
}
