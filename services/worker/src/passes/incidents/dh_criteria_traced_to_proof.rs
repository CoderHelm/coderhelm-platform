//! dh. A ticket listed two acceptance criteria: one filter chip per wing
//! on a slots page, and the page view tracked there. The pull request
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

const TICKET: &str = "On /club/slots the wing filter shows two Guest-Wing chips.\n\nAcceptance Criteria\n- Only one guest-wing filter chip shows on /club/slots\n- Page views of the slots page are tracked";

const DESIGN: &str = "## Criteria map\n- Only one guest-wing filter chip shows | page: /club/slots | e2e: e2e/slots.spec.ts › shows one guest-wing filter chip | asserts: one Guest-Wing chip | code: filterMaps.ts, dedupeWingsBySlug\n- Page views of the slots page are tracked | page: /club/slots | e2e: none: an analytics call | asserts: a page view event\n";

fn diff() -> Vec<(String, String, String)> {
    vec![
        (
            "apps/web/src/utils/filterMaps.ts".into(),
            "modified".into(),
            "@@ -1 +1,4 @@\n+export function dedupeWingsBySlug(rows: Row[]) {\n+  return uniqBy(rows, (r) => r.slug);\n+}\n".into(),
        ),
        (
            "apps/web/src/lib/analytics.ts".into(),
            "modified".into(),
            "@@ -1 +1,2 @@\n+export function trackPageView(page: string) {}\n".into(),
        ),
        (
            "e2e/slots.spec.ts".into(),
            "added".into(),
            "@@ -0,0 +1,3 @@\n+test('shows one guest-wing filter chip', async ({ page }) => {\n+  await page.goto('/club/slots');\n+});\n".into(),
        ),
    ]
}

fn proven_chip() -> Proof {
    Proof {
        criterion: "Only one guest-wing filter chip shows on /club/slots".into(),
        page: "/club/slots".into(),
        test: "e2e/slots.spec.ts › shows one guest-wing filter chip".into(),
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
    assert_eq!(chip.code[0].symbols, vec!["dedupeWingsBySlug"]);
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
    let shaped = description_facts::shape("", "fix: one Guest-Wing chip", f, None);
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
    let again =
        description_facts::shape(body, "fix: one Guest-Wing chip", shaped.facts.clone(), None);
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

#[test]
fn an_open_criterion_drives_a_fix_round_like_any_open_gap() {
    use crate::passes::run_guard::{Ledger, Target, TraceGaps};
    let t = the_trace();
    // The untested criterion is the coder's work; the proven one is not.
    let items = trace::coder_items(&t);
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(items[0].starts_with(trace::CODER_ITEM));
    assert!(items[0].contains("Page views") && items[0].contains("no test"));

    // A criterion whose two-way proof sends its own item is sent once.
    let mut both_green = proven_chip();
    both_green.record.base = Leg {
        ran: true,
        passed: 1,
        ..Default::default()
    };
    let acceptance = crate::passes::stop_judge::acceptance(TICKET);
    let map = ep::parse_map(DESIGN);
    let criteria = ep::criteria_or_map("Duplicate chip", TICKET, &acceptance, &map);
    let not_the_ticket = trace::build(&Inputs {
        head: HEAD,
        criteria: &criteria,
        from_plan: false,
        map: &map,
        files: &diff(),
        proofs: std::slice::from_ref(&both_green),
        test_runs: &[],
    });
    let proof_items = ep::coder_items(std::slice::from_ref(&both_green), HEAD);
    assert_eq!(proof_items.len(), 1);
    let trace_items = trace::coder_items(&not_the_ticket);
    assert!(
        !trace_items.iter().any(|i| i.contains("guest-wing")),
        "{trace_items:?}"
    );
    let merged =
        crate::passes::pr_watch::with_trace_items(proof_items.clone(), trace_items.clone());
    assert_eq!(merged.len(), 2, "{merged:?}");
    assert_eq!(
        crate::passes::pr_watch::with_trace_items(merged.clone(), trace_items).len(),
        2
    );

    // On the run's ledger the trace's items are open items: the run
    // converges on them and they hold the draft.
    let mut l = Ledger::default();
    let before = Target::of(&l).open.len();
    l.trace = TraceGaps {
        head: HEAD.into(),
        items: items.clone(),
    };
    let target = Target::of(&l);
    assert_eq!(target.open.len(), before + 1, "{:?}", target.open);
    assert!(target.open.iter().any(|o| o.contains("Page views")));
    assert!(l
        .open_findings()
        .iter()
        .any(|f| f.point == "check_blocked" && f.detail.contains("The criteria trace")));
    assert_eq!(l.trace.on(HEAD), items.as_slice());
    assert!(l.trace.on("0000000").is_empty());

    // Wired: the description records them, the watch sends them with the
    // proof's in one round, and without a preview the trace sends them.
    let description = include_str!("../description.rs");
    assert!(description.contains("record_trace_gaps(state, team_id"));
    assert!(description.contains("g.run.preview.is_none() && !gaps.is_empty()"));
    assert!(description.contains("queue_own_review_round("));
    let watch = include_str!("../pr_watch.rs");
    let at = watch
        .find("super::description::trace_items(")
        .expect("the watch reads them");
    let queue = watch[at..]
        .find("queue_own_review_round(")
        .expect("and queues them");
    assert!(watch[at..at + queue].contains("with_trace_items(items, trace)"));
}
