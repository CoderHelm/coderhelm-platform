//! sl. A ticket said a guest slips page showed two Multi-Berth filter
//! chips (two rows of the schedule API share one slug). The plan fixed the
//! chips and wrote, in its proposal, "Out: `/charters/<berth>` path pages
//! ... Both are open product questions", with an "Open product questions"
//! section that already answered each with "Meanwhile". Its task list
//! then said "switch any other slugToId caller ... to slugToIds". Days
//! later a round built the path pages (service files and their tests);
//! the tests and scope check took the code out again as "no task of the
//! plan" but left the tests, which that check exempts; the run's automatic
//! revert put the path-page tests back for code that was no longer there;
//! a person deleted them by hand. The description kept saying the path
//! pages were out of scope while the diff changed them, and asked the team
//! the questions the plan had answered. An audit of recent runs found many
//! recorded plans that left product questions open, and several branches
//! where a file went in, came out and came back.
//!
//! Guarantee: scope is decided once, from the ticket's acceptance criteria
//! (`stop_judge::acceptance`) and the plan, before any code
//! (`common::scope_ledger`). A plan without `## Out of scope`, with an open
//! question, an item without a reason, an item a criterion asks for, a
//! task naming an out-of-scope item or an open-ended task goes back. Every
//! round's check reads the diff, tests included, against the ledger; a
//! planned-work round never builds an out-of-scope item; a scope change
//! needs a `Scope change:` line citing a criterion or a person's comment,
//! and the ledger records it. The description says what the ledger says:
//! questions become defaults, and a sentence the diff refutes goes.

use crate::passes::habits_check::{self, PrFacts};
use crate::passes::stop_judge::acceptance;
use crate::passes::{description_facts, scope_ledger as store};
use common::pr_facts::{Facts, FileChange};
use common::scope_ledger::{self, Ledger};

const TICKET: &str = "Steps to reproduce\nGo to /harbor/guest-slips\nScroll to the berths filter\nObserved\nTwo Multi-Berth filters show\nAcceptance Criteria\nOnly one multi-berth filter to show";

/// The proposal as the planner wrote it.
const PROPOSAL: &str = "## Problem\nTwo Multi-Berth chips.\n\n## Scope\n- In: shared berth chips and `?berths=` resolution on every page using them.\n- Out:\n  - Merging the two schedule API rows.\n  - `/charters/<berth>` path pages, which resolve one berth through `findNormalizedBerth` and `selectedBerth`. Both are open product questions.\n\n## Open product questions\n- Should the schedule API merge the two Multi-Berth rows? Meanwhile the rows stay as they are.\n- Which spelling should the single chip use? Meanwhile it uses whichever row comes first.\n- Should `/charters/<berth>` path pages also cover every row sharing the slug? Meanwhile they keep current behaviour (one id).\n";

const TASKS: &str = "- [ ] apps/web/src/test/utils/filterMaps.test.ts: [modify] Add slugToIds cases\n- [ ] apps/web/src/utils/filterMaps.ts: [modify] Add slugToIds and dedupeBerthsBySlug; switch any other slugToId caller in apps/web/src that builds a berth-id variable to slugToIds\n- [ ] apps/web/src/components/blocks/BerthsFilter.tsx: [modify] One chip per slug\n";

/// The plan after the send-back: scope decided once.
const DESIGN: &str = "## Out of scope\n- Merging the two schedule API rows: a data change in another repository\n- `/charters/<berth>` path pages (`selectedBerth`, `findNormalizedBerth`): decided with the default: one id, as today\n- The chip's spelling: decided with the default: whichever row comes first\n";
const TASKS_AFTER: &str = "- [ ] apps/web/src/test/utils/filterMaps.test.ts: [modify] Add slugToIds cases\n- [ ] apps/web/src/utils/filterMaps.ts: [modify] Add slugToIds and dedupeBerthsBySlug\n- [ ] apps/web/src/components/blocks/BerthsFilter.tsx: [modify] One chip per slug\n";

fn patch(lines: &[&str]) -> String {
    lines
        .iter()
        .map(|l| format!("+{l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The round that built the path pages, days later.
fn path_page_round() -> Vec<(String, String, String)> {
    vec![
        (
            "apps/web/src/components/cms-blocks/charters-block.tsx".into(),
            "modified".into(),
            patch(&[
                "  // A /charters/<berth> path page filters by every berth sharing the path",
                "  const selectedBerthsIdList = selectedBerth",
            ]),
        ),
        (
            "apps/web/src/services/charterService.ts".into(),
            "modified".into(),
            patch(&["    : selectedBerth", "      ? [selectedBerth.id]"]),
        ),
        (
            "apps/web/src/test/components/charters-block-dedup.test.tsx".into(),
            "modified".into(),
            patch(&[
                "  it(\"sends every berth id sharing a /charters/<berth> path slug\", () => {",
                "    expect(args.selectedBerth?.id).toBe(\"s-b\");",
            ]),
        ),
    ]
}

/// The automatic revert: the code was out again, the tests came back.
fn revert_head() -> Vec<(String, String, String)> {
    vec![
        (
            "apps/web/src/utils/filterMaps.ts".into(),
            "modified".into(),
            patch(&["export const slugToIds = (all, slug) => [];"]),
        ),
        path_page_round().remove(2),
    ]
}

fn ledger() -> Ledger {
    Ledger::from_ticket_and_plan(&acceptance(TICKET), PROPOSAL, DESIGN, TASKS_AFTER)
}

#[test]
fn the_plan_decides_scope_once_and_asks_nothing() {
    let g = common::scope_ledger::plan_gaps(
        PROPOSAL,
        "## Reuse\n- `slugToId`\n",
        TASKS,
        &acceptance(TICKET),
    );
    let all = g.join("\n");
    assert!(all.contains("lacks `## Out of scope`"), "{all}");
    assert!(all.contains("3 product question(s) open"), "{all}");
    assert!(all.contains("reaches past its files"), "{all}");
    // The plan as the send-back asks for it passes, and its criteria are
    // the ledger's in-scope list.
    assert!(common::scope_ledger::plan_gaps(
        PROPOSAL.split("## Open product questions").next().unwrap(),
        DESIGN,
        TASKS_AFTER,
        &acceptance(TICKET)
    )
    .is_empty());
    let l = ledger();
    assert_eq!(
        l.criteria,
        vec!["Only one multi-berth filter to show".to_string()]
    );
    let path = l
        .outs()
        .find(|e| e.item.contains("/charters/<berth>"))
        .expect("path pages out");
    assert!(
        path.marks.contains(&"selectedBerth".to_string()),
        "{:?}",
        path.marks
    );
}

#[test]
fn every_round_is_held_to_it_tests_included() {
    let l = ledger();
    // The round that built the path pages: each file is a breach.
    let b = scope_ledger::breaches(&l, &path_page_round());
    assert_eq!(b.len(), 3, "{b:?}");
    // The revert that put the tests back with the code gone: the tests and
    // scope check exempts tests from "inside the plan", the ledger doesn't.
    let head = revert_head();
    let changed: Vec<String> = head.iter().map(|(p, _, _)| p.clone()).collect();
    let plan = crate::passes::plan::PlanResult {
        tasks: TASKS_AFTER.into(),
        ..Default::default()
    };
    assert!(habits_check::outside_plan(&changed, &plan, &[], "").is_empty());
    let breaches = scope_ledger::breaches(&l, &head);
    assert_eq!(breaches.len(), 1);
    let f = PrFacts {
        out_of_scope: breaches,
        ..Default::default()
    };
    let lines = habits_check::blocking(&f, &plan);
    assert!(
        lines
            .iter()
            .any(|x| x.contains("charters-block-dedup.test.tsx")
                && x.contains("decided is out of scope")),
        "{lines:?}"
    );
    let open = habits_check::open_findings(&f, &plan);
    assert!(open.iter().any(|o| o.point == "Out of scope brought back"));
    // A planned-work round never builds it.
    let (keep, gone) = store::drop_covered(
        &l,
        vec![
            "apps/web/src/services/charterService.ts: prefer the resolved id list over `selectedBerth`".into(),
            "apps/web/src/components/blocks/BerthsFilter.tsx: one chip per slug".into(),
        ],
    );
    assert_eq!((keep.len(), gone.len()), (1, 1));
}

#[test]
fn scope_changes_only_with_a_cited_reason() {
    let mut l = ledger();
    let sources = vec!["Please also make the /charters/<berth> path pages list charters from every row sharing the slug".to_string()];
    let (ok, no) = l.apply_changes(
        "Scope change: /charters/<berth> path pages: it seemed incomplete without them",
        &sources,
    );
    assert!(ok.is_empty() && no.len() == 1);
    assert_eq!(scope_ledger::breaches(&l, &path_page_round()).len(), 3);
    let (ok, _) = l.apply_changes(
        "Scope change: /charters/<berth> path pages: the comment asks that path pages list charters from every row sharing the slug",
        &sources,
    );
    assert_eq!(ok.len(), 1);
    assert!(scope_ledger::breaches(&l, &path_page_round()).is_empty());
}

#[test]
fn the_description_says_what_the_ledger_says() {
    let l = ledger();
    let files = path_page_round();
    let mut facts = Facts {
        head: "abc1234def5678901234567890abcdef12345678".into(),
        files: files
            .iter()
            .map(|(p, s, _)| FileChange {
                path: p.clone(),
                status: s.clone(),
                changed: 2,
                ..Default::default()
            })
            .collect(),
        files_complete: true,
        ..Default::default()
    };
    description_facts::with_scope(&mut facts, Some(&l), &files);
    // Brought back: not verified, never "out of scope".
    assert!(
        !facts
            .out_of_scope
            .iter()
            .any(|x| x.contains("/charters/<berth>")),
        "{:?}",
        facts.out_of_scope
    );
    assert!(facts
        .out_of_scope
        .iter()
        .any(|x| x.starts_with("Merging the two schedule API rows")));
    assert!(facts
        .unverified
        .iter()
        .any(|u| u.what.contains("decided out of scope at plan time")));
    let written = "## Changes\nOne chip per slug.\n\nRedirect helpers are untouched. Out of scope: merging the two rows, and `/charters/<berth>` pages, which still resolve one berth.\n\n## Open product questions\nThe ticket doesn't settle these. Please answer on the pull request:\n- Which spelling should the single chip use? Meanwhile it uses whichever row comes first.\n";
    let (text, corrections) = description_facts::hold_scope(written, &mut facts);
    assert!(!text.contains("still resolve one berth"), "{text}");
    assert!(text.contains("Redirect helpers are untouched."));
    assert!(!text.to_lowercase().contains("question"), "{text}");
    assert_eq!(corrections.len(), 1);
    assert!(
        facts
            .out_of_scope
            .iter()
            .any(|x| x.contains("decided with the default")),
        "{:?}",
        facts.out_of_scope
    );
    let shaped = description_facts::shape(written, "ABC-1: One Multi-Berth chip", facts, None);
    let low = shaped.body.to_lowercase();
    assert!(
        !low.contains("open product question") && !low.contains("please answer"),
        "{}",
        shaped.body
    );
    assert!(
        shaped.body.contains(common::pr_body::OUT_OF_SCOPE),
        "{}",
        shaped.body
    );
    assert!(!common::pr_body::defects(&shaped.body, "abc1234")
        .iter()
        .any(|d| d.contains("question")));
}
