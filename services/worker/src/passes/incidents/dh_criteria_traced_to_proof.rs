//! dh. A ticket listed two acceptance criteria: one filter chip per sport
//! on a camps page, and the page view tracked there. The pull request
//! changed the chip code, added an end-to-end spec for the chip, and its
//! two-way proof passed. Its description listed the files and the checks,
//! and nothing tied either to the criteria: the tracking criterion had
//! changed code and no test at all, and no line of the description, the
//! run page or the API said so. A reader had to match files and tests to
//! the ticket by hand to find the criterion left unproven.
//!
//! Guarantee: every build of the description traces each criterion (the
//! ticket's, else the plan's criteria map) to the files and definitions
//! changed for it (the map line's `code:`, else its words), the tests that
//! cover it, and its proof on the head: the two-way end-to-end proof with
//! the served-page check, else its tests' runs with and without the change
//! (`common::trace`). The table is in the description's checks, rebuilt
//! from the facts on every push; a criterion with no traced code, no test
//! or no proof that holds is an open item; and the trace is kept on the run
//! (`traceability`) and served by the run API.

use crate::passes::description_facts;
use common::ci_repro::{Leg, Record};
use common::e2e_proof::{self as ep, Proof};
use common::pr_facts::{Facts, FileChange};
use common::trace::{self, Inputs, State};

const HEAD: &str = "c0ffee1234567890c0ffee1234567890c0ffee12";

const TICKET: &str = "On /club/summer-camps the sport filter shows two Multi-Sport chips.\n\nAcceptance Criteria\n- Only one multi-sport filter chip shows on /club/summer-camps\n- Page views of the camps page are tracked";

const DESIGN: &str = "## Criteria map\n- Only one multi-sport filter chip shows | page: /club/summer-camps | e2e: e2e/camps.spec.ts › shows one multi-sport filter chip | asserts: one Multi-Sport chip | code: filterMaps.ts, dedupeSportsBySlug\n- Page views of the camps page are tracked | page: /club/summer-camps | e2e: none: an analytics call | asserts: a page view event\n";

fn diff() -> Vec<(String, String, String)> {
    vec![
        (
            "apps/web/src/utils/filterMaps.ts".into(),
            "modified".into(),
            "@@ -1 +1,4 @@\n+export function dedupeSportsBySlug(rows: Row[]) {\n+  return uniqBy(rows, (r) => r.slug);\n+}\n".into(),
        ),
        (
            "apps/web/src/lib/analytics.ts".into(),
            "modified".into(),
            "@@ -1 +1,2 @@\n+export function trackPageView(page: string) {}\n".into(),
        ),
        (
            "e2e/camps.spec.ts".into(),
            "added".into(),
            "@@ -0,0 +1,3 @@\n+test('shows one multi-sport filter chip', async ({ page }) => {\n+  await page.goto('/club/summer-camps');\n+});\n".into(),
        ),
    ]
}

fn proven_chip() -> Proof {
    Proof {
        criterion: "Only one multi-sport filter chip shows on /club/summer-camps".into(),
        page: "/club/summer-camps".into(),
        test: "e2e/camps.spec.ts › shows one multi-sport filter chip".into(),
        record: Record {
            head: HEAD.into(),
            base: Leg {
                ran: true,
                failed: 1,
                target: "https://staging.example.test".into(),
                ..Default::default()
            },
            change: Leg {
                ran: true,
                passed: 1,
                target: "https://pr-41.example.test".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

fn the_trace() -> trace::Trace {
    let acceptance = crate::passes::stop_judge::acceptance(TICKET);
    let map = ep::parse_map(DESIGN);
    let criteria = ep::criteria_or_map("Duplicate chip", TICKET, &acceptance, &map);
    assert_eq!(criteria.len(), 2, "{criteria:?}");
    trace::build(&Inputs {
        head: HEAD,
        criteria: &criteria,
        from_plan: false,
        map: &map,
        files: &diff(),
        proofs: &[proven_chip()],
        test_runs: &[],
    })
}

#[test]
fn each_criterion_is_traced_and_the_unproven_one_is_open() {
    let t = the_trace();
    let chip = &t.rows[0];
    assert_eq!(chip.code[0].path, "apps/web/src/utils/filterMaps.ts");
    assert_eq!(chip.code[0].symbols, vec!["dedupeSportsBySlug"]);
    assert_eq!(chip.proof.state, State::Proven);
    let views = &t.rows[1];
    assert_eq!(views.code[0].path, "apps/web/src/lib/analytics.ts");
    assert!(views.tests.is_empty());
    assert_eq!(views.proof.state, State::Missing);

    let mut f = Facts {
        head: HEAD.into(),
        files: diff()
            .iter()
            .map(|(p, s, _)| FileChange {
                path: p.clone(),
                status: s.clone(),
                changed: 3,
                ..Default::default()
            })
            .collect(),
        files_complete: true,
        ..Default::default()
    };
    description_facts::with_two_way(&mut f, &[proven_chip()]);
    description_facts::with_trace(&mut f, t.clone());
    // The open item names the criterion and what is missing, once.
    let open: Vec<_> = f
        .unverified
        .iter()
        .filter(|u| u.what.contains("Page views"))
        .collect();
    assert_eq!(open.len(), 1, "{:?}", f.unverified);
    assert!(open[0].why.contains("no test"));

    // The table is in the description, in its checks, from the facts.
    let shaped = description_facts::shape("", "fix: one Multi-Sport chip", f, None);
    let body = &shaped.body;
    assert!(
        body.contains("| Criterion | Code | Tests | Proof |"),
        "{body}"
    );
    assert!(body.contains("proven both ways"));
    assert!(body.contains("| none | no test |"));
    let fold = body
        .find(common::pr_body::EVIDENCE)
        .expect("the checks fold");
    assert!(body.find("Acceptance criteria, traced").unwrap() > fold);
    // Built again from the same facts, the same table: nothing piles up.
    let again = description_facts::shape(
        body,
        "fix: one Multi-Sport chip",
        shaped.facts.clone(),
        None,
    );
    assert_eq!(again.body.matches("Acceptance criteria, traced").count(), 1);
}

#[test]
fn the_trace_is_kept_on_the_run_and_served_by_the_api() {
    let description = include_str!("../description.rs");
    assert!(description.contains("with_trace(&mut f, t)"));
    assert!(description.contains("save_trace(state, team_id"));
    let api = include_str!("../../../../gateway/src/routes/api.rs");
    assert!(api.contains("traceability: st(common::trace::FIELD)"));
    let t = the_trace();
    assert_eq!(trace::Trace::from_json(&t.to_json()), Some(t));
}
