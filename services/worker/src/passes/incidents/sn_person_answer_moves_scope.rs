//! sn. The duplicated filter-chip ticket again (see `sl`). The plan left
//! the `/tours/<kiosk>` path pages out of scope, decided with a default.
//! A reviewer, sam, answered on the pull request that the path pages should
//! cover both rows. The run replied "Understood" and then reverted the
//! path-page work as out of scope: the scope ledger was seeded from the
//! ticket's criteria and the plan only, and a person's comment counted only
//! when the coder itself wrote a `Scope change:` line quoting it.
//!
//! Guarantee: what people say decides scope as they say it. At seed and on
//! every round's comments, an out-of-scope entry a person's answer or
//! comment asks for moves in, recording who asked
//! (`Ledger::take_people`, `scope_ledger::people`, `load_or_seed`, `held`,
//! `load_with_people`). From then on the path-page work is in scope, and a
//! diff that takes it out is a breach that goes back to be put back. A
//! comment that keeps the item out moves nothing.

use crate::passes::habits_check::{self, PrFacts};
use crate::passes::stop_judge::acceptance;
use common::scope_ledger::{self, Ledger};

const TICKET: &str = "Observed\nTwo Multi-Kiosk filters show\nAcceptance Criteria\nOnly one multi-kiosk filter to show";
const PROPOSAL: &str = "## Problem\nTwo Multi-Kiosk chips.\n";
const DESIGN: &str = "## Out of scope\n- Merging the two schedule API rows: a data change in another repository\n- `/tours/<kiosk>` path pages (`selectedKiosk`, `findNormalizedKiosk`): decided with the default: one id, as today\n";
const TASKS: &str = "- [ ] apps/web/src/utils/filterMaps.ts: [modify] Add slugToIds\n";

/// sam's answer on the pull request, in substance.
const ANSWER: &str = "The `/tours/<kiosk>` path pages should cover both rows too, so tours from either Multi-Kiosk row show there.";

fn ledger() -> Ledger {
    Ledger::from_ticket_and_plan(&acceptance(TICKET), PROPOSAL, DESIGN, TASKS)
}

fn said(text: &str) -> Vec<(String, String)> {
    vec![("sam".to_string(), text.to_string())]
}

/// The round that built the path pages after sam's answer.
fn path_pages_built() -> Vec<(String, String, String)> {
    vec![(
        "apps/web/src/services/tourService.ts".into(),
        "modified".into(),
        "+  // A /tours/<kiosk> path page filters by every row sharing the slug\n+  const ids = selectedKiosk ? slugToIds(all, selectedKiosk.slug) : [];".into(),
    )]
}

/// The later revert that took them out "as out of scope".
fn path_pages_reverted() -> Vec<(String, String, String)> {
    vec![(
        "apps/web/src/services/tourService.ts".into(),
        "modified".into(),
        "-  // A /tours/<kiosk> path page filters by every row sharing the slug\n-  const ids = selectedKiosk ? slugToIds(all, selectedKiosk.slug) : [];\n+  const ids = [];".into(),
    )]
}

#[test]
fn the_answer_moves_the_path_pages_in_and_a_revert_is_a_breach() {
    // Before the answer the path pages are out, and building them breaches.
    let l = ledger();
    assert_eq!(scope_ledger::breaches(&l, &path_pages_built()).len(), 1);

    // sam's answer moves them in on its own: no `Scope change:` line.
    let mut l = ledger();
    let moved = l.take_people(&said(ANSWER));
    assert_eq!(moved.len(), 1, "{moved:?}");
    let e = l
        .entries
        .iter()
        .find(|e| e.item.contains("/tours/<kiosk>"))
        .unwrap();
    assert!(!e.is_out() && e.moved_in.starts_with("sam asked for it"));
    // The other entry stays out.
    assert_eq!(l.outs().count(), 1);
    // Building them is no breach now.
    assert!(scope_ledger::breaches(&l, &path_pages_built()).is_empty());

    // Taking them out again is the breach, and it goes back.
    let b = scope_ledger::breaches(&l, &path_pages_reverted());
    assert_eq!(b.len(), 1, "{b:?}");
    assert!(b[0].reverted);
    let plan = crate::passes::plan::PlanResult {
        tasks: TASKS.into(),
        ..Default::default()
    };
    let f = PrFacts {
        out_of_scope: b,
        ..Default::default()
    };
    let lines = habits_check::blocking(&f, &plan);
    assert!(
        lines
            .iter()
            .any(|x| x.contains("takes out") && x.contains("sam asked for it")),
        "{lines:?}"
    );
    let open = habits_check::open_findings(&f, &plan);
    assert!(open.iter().any(|o| o.point == "A person's ask taken out"));
}

#[test]
fn the_answer_counts_at_seed_and_a_comment_keeping_it_out_moves_nothing() {
    // An answer the run already had when the plan was made: in scope from
    // the start, never written as out of scope.
    let mut l = ledger();
    l.take_people(&said(ANSWER));
    assert!(l.outs().all(|e| !e.item.contains("/tours/<kiosk>")));
    // A comment that keeps the path pages out leaves them out.
    let mut l = ledger();
    let moved = l.take_people(&said(
        "Leave the `/tours/<kiosk>` path pages for a separate ticket.",
    ));
    assert!(moved.is_empty());
    assert_eq!(l.outs().count(), 2);
}
