//! de. A ticket asked for a page block to work without JavaScript and for
//! its structured data (`FAQPage`) to be in the server HTML. The plan's
//! `## Out of scope` held one item decided with its default, and under it
//! two indented lines recording what a lookup had shown ("Looked up: the
//! page returned 200 and `FAQPage` was not in the server HTML", "No lookup
//! settles which is better"). The plan check read each indented line as an
//! item left out of scope, so `FAQPage` became the mark of an out-of-scope
//! item called "Looked up", and every task line naming it (five of them)
//! added the same gap: "A task names `FAQPage`, which `## Out of scope`
//! leaves out (\"Looked up\")". The run note and the decision's open gaps
//! carried that line five times, and the run told itself to take the
//! ticket's own ask out of its tasks. A second plan for the
//! same ticket was told an item about one script per page was asked for
//! by the criterion "No layout shift when the page loads or when items
//! toggle": the two share only "when" (counted twice) and "page".
//!
//! Guarantee: an out-of-scope section's items are its top-level bullets;
//! an indented line under one, and a line that records a lookup, is that
//! item's evidence, never an item (`scope_ledger::out_bullets`). What the
//! ticket itself names (title, body) is in scope: no mark of an
//! out-of-scope item, and an item it says in full moves in
//! (`Ledger::named_by_ticket`, read by the plan check and by the ledger
//! kept on the run). A criterion asks for an item only on distinct words
//! that tell items apart. Every plan-check gap is said once, in the note
//! and on the decision (`scope_ledger::unique`, `FixDecision::add_gaps`).

use crate::passes::fix_location::FixDecision;
use crate::passes::plan::PlanResult;
use crate::passes::plan_shape;
use common::scope_ledger::Ledger;

const TICKET: &str = "Make the FAQ block work without JavaScript\n\nEvery question and answer of the FAQ block is in the server HTML, and the block's `FAQPage` structured data is rendered on the server.\n\nAcceptance criteria:\n- Answers are readable with JavaScript off\n- No layout shift when the page loads or when items toggle\n";

const DESIGN: &str = "## Out of scope\n- **Merging the structured data of several blocks on one page**: decided with the default: one script per block\n  - Looked up: `https://example.test/moorings?cb=1` returned 200 with two blocks, and `FAQPage` was not in the server HTML\n  - No lookup settles which is better for search: today's script is added only in the browser\n- `apps/web/src/components/ui/accordion.tsx`: other blocks use it\n- One FAQPage script per page when a page has two FAQ blocks: decided with the default: one per block\n";

const TASKS: &str = "- [ ] apps/web/src/components/blocks/faq.tsx: [modify] Render each answer in `<details>`, and the `FAQPage` script on the server\n- [ ] apps/web/src/test/components/faq.test.tsx: [create] The `FAQPage` script is in the server HTML\n- [ ] e2e/pages/faq.ts: [create] Read the `FAQPage` script from the raw HTML\n- [ ] e2e/tests/site/faq.spec.ts: [create] With JavaScript off, every answer and the `FAQPage` script are there\n- [ ] e2e/specs/site.md: [modify] Document the `FAQPage` check\n";

fn plan() -> PlanResult {
    PlanResult {
        proposal: "## Problem\nThe block needs JavaScript.\n".into(),
        tasks: TASKS.into(),
        spec: String::new(),
        design: DESIGN.into(),
        repo_tasks: vec![],
        fix: None,
    }
}

#[test]
fn the_plan_check_reads_items_not_evidence_and_says_each_gap_once() {
    let gaps = plan_shape::plan_gaps(&plan(), TICKET, true, &[]);
    let all = gaps.join("\n");
    // The ticket's own ask is never an out-of-scope mark, and a lookup's
    // record is never an item.
    assert!(!all.contains("A task names `FAQPage`"), "{all}");
    assert!(!all.contains("(\"Looked up\")"), "{all}");
    assert!(!all.contains("\"No lookup settles"), "{all}");
    // "when" and "page" tell no item apart from a criterion.
    assert!(!all.contains("acceptance criterion asks for it"), "{all}");
    // Each gap once.
    for g in &gaps {
        assert_eq!(gaps.iter().filter(|x| *x == g).count(), 1, "{g}");
    }
}

#[test]
fn the_ledger_kept_on_the_run_holds_the_items_and_the_ticket_names_stay_in() {
    let mut l = Ledger::from_ticket_and_plan(&[], "", DESIGN, TASKS);
    l.named_by_ticket(TICKET);
    let items: Vec<&str> = l.entries.iter().map(|e| e.item.as_str()).collect();
    assert!(
        !items.iter().any(|i| i.starts_with("Looked up")),
        "{items:?}"
    );
    assert!(
        !items.iter().any(|i| i.starts_with("No lookup")),
        "{items:?}"
    );
    assert!(!items.iter().any(|i| i.contains("**")), "{items:?}");
    assert!(
        items.contains(&"Merging the structured data of several blocks on one page"),
        "{items:?}"
    );
    for e in l.outs() {
        assert!(
            !e.marks.iter().any(|m| m.eq_ignore_ascii_case("FAQPage")),
            "{e:?}"
        );
    }
}

#[test]
fn a_gap_found_on_five_task_lines_is_one_open_gap() {
    let mut d = FixDecision::default();
    let line = "A task names `X`, which `## Out of scope` leaves out (\"Y\").".to_string();
    d.add_gaps(vec![line.clone(); 5]);
    d.add_gaps(vec![line.clone(), "Another gap.".to_string()]);
    assert_eq!(d.open_gaps, vec![line, "Another gap.".to_string()]);
}

/// The plan check's assembly takes its gaps through the one place that
/// says each once.
#[test]
fn the_plan_check_adds_its_gaps_through_the_one_place() {
    let src = include_str!("../plan.rs");
    assert!(
        !src.contains("d.open_gaps.extend("),
        "every gap goes through add_gaps"
    );
    assert!(src.contains("common::scope_ledger::unique(left)"));
    let habits = include_str!("../habits.rs");
    assert!(!habits.contains("d.open_gaps.extend("));
}
