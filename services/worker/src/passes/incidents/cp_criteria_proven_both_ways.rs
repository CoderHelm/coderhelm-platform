//! cp. A ticket said one thing: on a library's slots page, the wing
//! filter showed two Guest-Wing chips; it should show one. The run planned
//! and wrote hundreds of lines of unit tests over several test files and never
//! loaded the page the ticket names: no end-to-end test, nothing run on
//! staging or the preview. It added three helpers to one file in the same
//! change (`slugToIds` beside the existing `slugToId`, then
//! `dedupeWingsBySlug` and `dedupeWingOptionsBySlug`, two copies of the
//! same first-per-key filter) with no shared logic. A person later replaced
//! the two dedupe helpers with one, cut the unit tests and added the one
//! end-to-end test of the page. Across the runs audited with it, many pull
//! requests whose ticket named a page had no end-to-end spec, and most of
//! those that changed one never ran it on staging.
//!
//! Guarantee: the plan maps each criterion (the acceptance list, else the
//! Expected lines) that names a page or what a person sees to an
//! end-to-end test of that page, with the assertion, under `## Criteria
//! map`; a plan without it, or with a unit test in its place, is sent back
//! (`plan_shape::criteria_gaps`, `common::e2e_proof::map_gaps`). Before the
//! pull request is called done, each criterion's test runs on staging and on
//! the preview through the red-check runner (`ci_repro::two_way`), and both
//! results are kept on the run and written into the pull request: failing
//! on staging and passing on the preview is proven; passing on both doesn't
//! test the ticket and goes back to the coder, as does failing on the
//! preview or having no test; a side that couldn't run is labelled
//! unverified with its reason (`e2e_proof`, `ready::decide`). Near-identical
//! helpers added in one change are found against each other, not only
//! against the code that was there (`minimal_change::twin_helpers`), and the
//! coder's guard refuses the second one until the edit names what they share
//! (`explore_guard`, `shares`).

use super::fixture::fn_src;
use crate::passes::e2e_proof::{standing_of, Standing};
use crate::passes::explore_guard::ExploreGuard;
use crate::passes::minimal_change::{self, Kind};
use crate::passes::plan::PlanResult;
use crate::passes::plan_shape;
use common::ci_repro::{self as repro, Record};
use common::e2e_proof::{self as ep, Outcome, Proof};

const TITLE: &str = "Duplicate Guest-Wing filter showing on the Acme Library Slots page";
/// The ticket as it was written (plain text, no acceptance heading).
const BODY: &str = "Steps to reproduce\nGo to /acme-library/slots\nScroll to the wing filter\nObserved\nTwo Guest-Wing filters show\nExpected\nOnly one guest-wing filter to show";
const HEAD: &str = "c0ffee5a1b2d";

fn plan(design: &str) -> PlanResult {
    PlanResult {
        design: design.into(),
        tasks: "- [ ] apps/web/src/utils/filterMaps.ts: one wing per slug".into(),
        ..Default::default()
    }
}

/// The plan's proof as the run wrote it: unit tests only.
const PLAN_BEFORE: &str = "## Reuse\n- `slugToId` in apps/web/src/utils/filterMaps.ts\n\n## Proof\nUnit tests: filterMaps.test.ts for the slug helpers; wing-filter-dedupe.test.tsx renders one chip per slug; filter-nav-wing-grid.test.tsx renders one button per slug.\n";

/// The same plan with the criteria map the person's fix needed.
fn plan_after() -> String {
    format!(
        "{PLAN_BEFORE}\n## Criteria map\n- Only one guest-wing filter shows | page: /acme-library/slots | e2e: e2e/tests/browse/libraries.spec.ts › the slots page shows one Guest-Wing chip | asserts: exactly one chip named Guest-Wing in the wing filter\n"
    )
}

/// The helpers the change added to `filterMaps.ts`, as its patch had them.
fn helpers_patch() -> String {
    [
        "@@ -562,21 +562,77 @@",
        " export const slugToId = (allWings: WingItem[], slug: string) => {",
        "   const found = allWings.find((s) => normalizeWingName(s.label) === slug);",
        "   return found?.id || slug;",
        " };",
        " ",
        "+export const slugToIds = (allWings: WingItem[], slug: string): string[] =>",
        "+  Array.from(",
        "+    new Set(",
        "+      allWings",
        "+        .filter((s) => Boolean(s?.id) && normalizeWingName(s.label) === slug)",
        "+        .map((s) => s.id)",
        "+    )",
        "+  );",
        "+",
        "+export const dedupeWingsBySlug = (allWings: WingItem[]): WingItem[] => {",
        "+  const seen = new Set<string>();",
        "+  return allWings.filter((wing): wing is WingItem => {",
        "+    if (!wing?.id) return false;",
        "+    const slug = normalizeWingName(wing.label);",
        "+    if (seen.has(slug)) return false;",
        "+    seen.add(slug);",
        "+    return true;",
        "+  });",
        "+};",
        "+",
        "+export const dedupeWingOptionsBySlug = <T extends { value: string }>(",
        "+  options: T[]",
        "+): T[] => {",
        "+  const seen = new Set<string>();",
        "+  return options.filter((option) => {",
        "+    if (seen.has(option.value)) return false;",
        "+    seen.add(option.value);",
        "+    return true;",
        "+  });",
        "+};",
        "",
    ]
    .join("\n")
}

fn leg(target: &str, out: &str) -> repro::Leg {
    repro::leg_of(target, out)
}

fn proof(test: &str, base_out: &str, change_out: &str) -> Proof {
    Proof {
        criterion: "Only one guest-wing filter to show".into(),
        page: "/acme-library/slots".into(),
        test: test.into(),
        record: Record {
            head: HEAD.into(),
            check: "two-way proof".into(),
            tests: vec![test.into()],
            base: leg("https://staging.example.com", base_out),
            change: leg("https://pr-12.preview.example.com", change_out),
            source: "sandbox".into(),
            at: String::new(),
        },
        ..Default::default()
    }
}

#[test]
fn the_plan_maps_the_criterion_to_a_test_of_its_page() {
    let c = ep::criteria(TITLE, BODY, &[]);
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].text, "Only one guest-wing filter to show");
    assert_eq!(c[0].page.as_deref(), Some("/acme-library/slots"));
    // The run's plan: unit tests only, sent back.
    let g = plan_shape::criteria_gaps(&plan(PLAN_BEFORE), TITLE, BODY, true);
    assert_eq!(g.len(), 1, "{g:?}");
    assert!(g[0].contains("## Criteria map") && g[0].contains("/acme-library/slots"));
    assert!(g[0].contains("never replace it"));
    // A unit test named in the map is no end-to-end test.
    let unit = plan_after().replace(
        "e2e/tests/browse/libraries.spec.ts",
        "apps/web/src/test/components/wing-filter-dedupe.test.tsx",
    );
    assert!(
        plan_shape::criteria_gaps(&plan(&unit), TITLE, BODY, true)[0]
            .contains("isn't an end-to-end spec")
    );
    // The plan with the map passes; the planner is told to write it.
    assert!(plan_shape::criteria_gaps(&plan(&plan_after()), TITLE, BODY, true).is_empty());
    assert!(ep::PLANNER.contains("## Criteria map"));
    let plan_src = include_str!("../plan.rs");
    assert!(plan_src.contains("common::e2e_proof::PLANNER"));
    assert_eq!(
        plan_src
            .matches("super::plan_shape::criteria_gaps(")
            .count(),
        2,
        "the draft and the revised plan are both checked"
    );
}

#[test]
fn each_criterion_runs_both_ways_before_it_is_done() {
    let t = "e2e/tests/browse/libraries.spec.ts › the slots page shows one Guest-Wing chip";
    let red = "  1) [chromium] › libraries.spec.ts › the slots page shows one Guest-Wing chip\n    Error: expect(locator).toHaveCount(expected) failed\n    Expected: 1\n    Received: 2\n  1 failed\n";
    let green = "  1 passed (4.2s)\n";
    // The run's change had no end-to-end test of the page.
    let none = Proof {
        test: String::new(),
        record: Record {
            head: HEAD.into(),
            ..Default::default()
        },
        ..proof(t, "", "")
    };
    assert!(matches!(ep::outcome(&none), Outcome::Missing(_)));
    assert!(none
        .instruction()
        .unwrap()
        .contains("loads `/acme-library/slots`"));
    // A test that passes on staging too doesn't test the ticket.
    let both = proof(t, green, green);
    assert!(matches!(ep::outcome(&both), Outcome::NotTheTicket(_)));
    assert_eq!(standing_of(&[both], HEAD), Standing::Fixing(1));
    // Red on the preview is the coder's.
    assert!(matches!(
        ep::outcome(&proof(t, red, red)),
        Outcome::CoderWork(_)
    ));
    // No browser on one side: unverified, with why, never a pass.
    let no_browser = proof(
        t,
        "Error: browserType.launch: Executable doesn't exist at /ms-playwright/chromium",
        green,
    );
    match standing_of(std::slice::from_ref(&no_browser), HEAD) {
        Standing::Done(notes) => assert!(notes[0].contains("Executable doesn't exist")),
        s => panic!("{s:?}"),
    }
    // Red on staging, green on the preview: proven, with both results.
    let good = proof(t, red, green);
    match ep::outcome(&good) {
        Outcome::Proven(w) => {
            assert!(w.contains("staging") && w.contains("preview"), "{w}");
            assert!(w.contains("1 failed") && w.contains("1 passed"), "{w}");
        }
        o => panic!("{o:?}"),
    }
    assert_eq!(standing_of(&[good], HEAD), Standing::Done(vec![]));
}

#[test]
fn the_proof_is_wired_into_the_watch_the_ready_decision_and_the_pull_request() {
    let watch = fn_src(include_str!("../pr_watch.rs"), "async fn preview(");
    assert!(watch.contains("super::e2e_proof::prove("));
    assert!(watch.contains("queue_own_review_round("));
    let ready = include_str!("../ready.rs");
    assert!(fn_src(ready, "fn decide_raw(").contains("Standing::Fixing"));
    assert!(ready.contains("super::e2e_proof::standing("));
    assert!(include_str!("../description.rs").contains("with_two_way("));
    // The proof runs through the same runner as a red check's comparison.
    let proof_src = include_str!("../e2e_proof.rs");
    assert!(proof_src.contains("super::ci_repro::two_way("));
    assert!(fn_src(
        include_str!("../ci_repro.rs"),
        "pub(crate) async fn compare("
    )
    .contains("two_way("));
}

#[test]
fn twin_helpers_in_one_change_are_found_and_stopped() {
    let diff = [(
        "apps/web/src/utils/filterMaps.ts".to_string(),
        "modified".to_string(),
        helpers_patch(),
    )];
    // Before: compared only with the code that was there, the two dedupe
    // helpers passed.
    let parallel = minimal_change::parallel_structures(&diff, &[]);
    assert!(parallel
        .iter()
        .all(|w| !w.what.contains("dedupeWingOptionsBySlug")));
    assert!(parallel.iter().any(|w| w
        .what
        .contains("`slugToIds` next to the existing `slugToId`")));
    // Now: compared with each other.
    let found = minimal_change::check(&diff, &[], &format!("{TITLE}\n{BODY}"), false);
    let twins: Vec<_> = found.iter().filter(|w| w.kind == Kind::Twin).collect();
    assert_eq!(twins.len(), 1, "{found:?}");
    assert!(twins[0]
        .what
        .contains("`dedupeWingOptionsBySlug` next to `dedupeWingsBySlug`"));

    // The coder's guard: the second helper in the build names what it shares.
    let g = ExploreGuard::for_build();
    let base =
        "export const slugToId = (allWings: WingItem[], slug: string) => {\n  return slug;\n};\n";
    let new_side: String = helpers_patch()
        .lines()
        .skip(1)
        .filter(|l| !l.starts_with('-'))
        .map(|l| l.get(1..).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let (first, _) = new_side
        .split_once("export const dedupeWingOptionsBySlug")
        .unwrap();
    let first = format!("{first}\n");
    assert!(g
        .check_edit(
            "apps/web/src/utils/filterMaps.ts",
            base,
            &first,
            &serde_json::json!({"why_new": "every id behind a slug"})
        )
        .is_ok());
    let e = g
        .check_edit(
            "apps/web/src/utils/filterMaps.ts",
            &first,
            &new_side,
            &serde_json::json!({}),
        )
        .unwrap_err();
    assert!(
        e.contains("`dedupeWingOptionsBySlug` like `dedupeWingsBySlug`"),
        "{e}"
    );
}
