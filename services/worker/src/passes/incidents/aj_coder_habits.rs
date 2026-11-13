//! ah. A bug fix that was right on its face and wrong four ways. The ticket
//! said cart totals were a cent off. The plan named two files and nothing
//! about them. The run:
//!
//! - wrote the fix first and a test after it that passed with or without
//!   the fix, so nothing showed the bug had existed or was gone;
//! - rounded per line, the approach a pull request had shipped and another
//!   had reverted two months earlier, for a reason one commit list away;
//! - changed the shape of a response field another of the team's
//!   repositories read, and said nothing about that repository;
//! - listed "half up or half even?" as an open doubt at the bottom of the
//!   finished pull request, after building one of the two;
//! - reformatted a helper file no task named.
//!
//! Guarantee: before code, the plan of a bug names the test that fails on
//! the code as it is (or why none can, and the next best proof) and that
//! test is the first task; the worker reads the history of the files the
//! plan modifies and a revert must be answered by name; every use of a
//! changed name the worker finds in the team's other repositories is
//! updated by a task or called safe with the reason, and the pull request
//! names it either way; a doubt that changes what is built is asked at
//! planning time with the default being built. After the build each new or
//! changed test is run with and without the change: one that passes either
//! way goes back to the coder, and so does a changed file outside the plan
//! with no reason. The pull request carries the failing and the passing
//! result.

use super::fixture::{fn_src, run_passes_src};
use crate::passes::fix_location::{self, send_back};
use crate::passes::habits::{self, Dependents, Facts};
use crate::passes::habits_check::{self, Bench, FileRun, Runs, Verdict};
use crate::passes::plan::PlanResult;
use crate::passes::triage::TicketIntent;
use serde_json::json;

const OWN: &str = "acme/shop-api";

const TASKS: &str = "- [ ] src/cart/total.ts: [modify] Round each line before summing\n- [ ] src/cart/total.test.ts: [modify] A cart of three lines at 0.335\n";

fn plan(tasks: &str, block: &str) -> PlanResult {
    let reply = format!("```fix_location.json\n{block}\n```");
    let mut fix = fix_location::parse_for(&reply, TicketIntent::Bug).expect("a decision");
    habits::fold_questions(&mut fix);
    PlanResult {
        proposal: String::new(),
        design: "## Design\nRound each line in `cartTotal`.".into(),
        tasks: tasks.into(),
        spec: String::new(),
        repo_tasks: Vec::new(),
        fix: Some(fix),
    }
}

/// What the worker reads for the draft: the revert in the file's commits,
/// the storefront's use of the field, one caller in the repository.
fn facts() -> Facts {
    let commits = [
        json!({"sha": "9f8e7d6c5b4a", "commit": {"message": "Revert \"Round cart totals per line (#212)\" (#219)\n\nThis reverts commit 1a2b3c4.", "committer": {"date": "2026-01-20T10:00:00Z"}}}),
        json!({"sha": "1a2b3c4d5e6f", "commit": {"message": "Round cart totals per line (#212)", "committer": {"date": "2026-01-14T10:00:00Z"}}}),
    ];
    Facts {
        own_repo: OWN.into(),
        history: vec![habits::file_history("src/cart/total.ts", &commits)],
        dependents: Dependents {
            in_repo: vec![("src/invoice/pdf.ts".into(), "references `cartTotal`".into())],
            other_repos: vec![(
                "acme/storefront".into(),
                "lineTotals".into(),
                vec!["src/checkout/summary.tsx:88: cart.lineTotals.map((t) => t.amount)".into()],
            )],
            has_other_repos: true,
        },
    }
}

/// The plan as it used to be written: the files and nothing about them.
const BARE: &str = r#"{"fix_location": "code", "cause": "Totals are rounded once per cart.",
  "evidence": {"basis": "code", "producer": {"kind": "code", "where": "acme/shop-api:src/cart/total.ts cartTotal"}, "scope": "one function"},
  "habits": {"surfaces": ["lineTotals"]},
  "questions": ["Should rounding be half up or half even?"]}"#;

/// The plan after the one send-back.
const CAREFUL: &str = r##"{"fix_location": "code", "cause": "Totals are rounded once per cart.",
  "evidence": {"basis": "code", "producer": {"kind": "code", "where": "acme/shop-api:src/cart/total.ts cartTotal"}, "scope": "one function"},
  "habits": {
    "repro": {"test": "src/cart/total.test.ts", "fails_with": "three lines at 0.335 total 1.00, expected 1.02"},
    "area_history": [{"path": "src/cart/total.ts", "found": "Per-line rounding shipped in #212 and was reverted in #219: the ledger export still summed unrounded lines, so invoices stopped matching it.", "refs": ["#212", "#219"], "bearing": "This plan changes the export's sum in the same pull request."}],
    "surfaces": ["lineTotals"],
    "affected": [
      {"where": "acme/storefront:src/checkout/summary.tsx", "uses": "lineTotals[].amount", "action": "safe", "why": "it reads `amount`, which keeps its name and type; only its value is rounded"},
      {"where": "src/invoice/pdf.ts", "uses": "cartTotal", "action": "update", "why": "prints the per-line amounts"}
    ],
    "ask_early": [{"question": "Should rounding be half up or half even?", "default": "half up, as the invoice module already rounds", "depends_on": ["src/cart/total.ts"]}]
  }}"##;

const CAREFUL_TASKS: &str = "- [ ] src/cart/total.ts: [modify] Round each line before summing\n- [ ] src/invoice/pdf.ts: [modify] Print the rounded line amounts\n- [ ] src/cart/total.test.ts: [modify] A cart of three lines at 0.335\n";

#[test]
fn a_plan_of_files_alone_is_sent_back_with_what_the_worker_read() {
    let draft = plan(TASKS, BARE);
    let gaps = habits::plan_gaps(&draft, &facts());
    let back = send_back(&gaps);
    // The reproduction, the revert by commit and date, the other
    // repository's use by file and line, the caller in this one.
    assert!(back.contains("`habits.repro`"), "{back}");
    assert!(back.contains("REVERT 9f8e7d6 2026-01-20"), "{back}");
    assert!(back.contains("acme/storefront uses `lineTotals`"), "{back}");
    assert!(back.contains("src/checkout/summary.tsx:88"), "{back}");
    assert!(
        back.contains("`src/invoice/pdf.ts` (references `cartTotal`)"),
        "{back}"
    );
    assert_eq!(gaps.len(), 4, "{gaps:?}");

    // A draft that ignores the send-back keeps each gap on the decision,
    // and the other repository's use is named among those it affects.
    let mut ignored = plan(TASKS, BARE);
    let d = ignored.fix.as_mut().unwrap();
    habits::keep_open(d, &facts(), &gaps);
    assert_eq!(d.open_gaps.len(), 4);
    assert_eq!(
        d.habits.affected[0].where_,
        "acme/storefront:src/checkout/summary.tsx"
    );
    let section = habits_check::section(&ignored, &habits_check::PrFacts::default());
    assert!(section.contains("`acme/storefront:src/checkout/summary.tsx` (uses `lineTotals`): found by a search of the team's repositories"), "{section}");
}

#[test]
fn the_careful_plan_passes_and_the_test_comes_first() {
    let mut p = plan(CAREFUL_TASKS, CAREFUL);
    assert_eq!(habits::plan_gaps(&p, &facts()), Vec::<String>::new());

    // The doubt is asked now, with the default the run builds, once.
    let d = p.fix.as_ref().unwrap();
    assert_eq!(d.questions.len(), 1);
    assert!(d.questions[0]
        .starts_with("Should rounding be half up or half even? Proposed default: half up"));
    assert!(d.questions[0].contains("I am building the default now"));

    // The reproducing test is the first task, so it is the first commit.
    let test = d.habits.repro.test.clone();
    p.tasks = habits::put_repro_first(&p.tasks, &test).expect("the test task moves up");
    assert!(p.tasks.starts_with("- [ ] src/cart/total.test.ts:"));

    // The coder reads all of it in the design.
    let d = p.fix.as_ref().unwrap();
    let design = habits::with_design_section(&p.design, d, &p.tasks);
    assert!(design.contains("Write `src/cart/total.test.ts` before any other task"));
    assert!(design.contains("reverted in #219"));
    assert!(design.contains("**Update with it:** `src/invoice/pdf.ts`"));
    assert!(design.contains("Build: half up, as the invoice module already rounds"));
}

struct Sandbox(Runs);

#[async_trait::async_trait]
impl Bench for Sandbox {
    async fn run(&self, _files: &[String]) -> Result<Runs, String> {
        Ok(self.0.clone())
    }
}

fn ran(code: i32, output: &str) -> FileRun {
    FileRun {
        code: Some(code),
        output: output.into(),
    }
}

fn diff(paths: &[&str]) -> Vec<(String, String, String)> {
    paths
        .iter()
        .map(|p| (p.to_string(), "modified".to_string(), String::new()))
        .collect()
}

#[tokio::test]
async fn a_test_that_passes_without_the_fix_and_a_stray_file_go_back_to_the_coder() {
    let p = plan(CAREFUL_TASKS, CAREFUL);
    // The first build: the test asserts nothing the fix changes, the
    // dependent wasn't touched, and a helper was reformatted.
    let first = diff(&[
        "src/cart/total.ts",
        "src/cart/total.test.ts",
        "src/utils/format.ts",
    ]);
    let both_pass = Sandbox(Runs {
        with: vec![ran(0, "")],
        without: vec![ran(0, "")],
    });
    let f =
        habits_check::check_once(&both_pass, &first, &p, OWN, &[], "Evidence: none needed").await;
    let blocking = habits_check::blocking(&f, &p);
    assert_eq!(blocking.len(), 3, "{blocking:?}");
    let feedback = habits_check::fix_feedback(&blocking);
    assert!(feedback.contains("`src/cart/total.test.ts` passes with the change taken out"));
    assert!(feedback.contains("`src/utils/format.ts` was changed and is no task of the plan"));
    assert!(feedback.contains("`src/invoice/pdf.ts` depends on this change"));
    // It is the coder's to fix: never a line for a person.
    for phrase in crate::passes::ownership::HAND_OFF_PHRASES {
        assert!(!feedback.to_lowercase().contains(phrase), "{phrase}");
    }

    // After the fix round: the test fails on the base on its assertion, the
    // dependent is updated, the helper is back.
    let second = diff(&[
        "src/cart/total.ts",
        "src/cart/total.test.ts",
        "src/invoice/pdf.ts",
    ]);
    let proven = Sandbox(Runs {
        with: vec![ran(0, "")],
        without: vec![ran(
            1,
            "FAIL src/cart/total.test.ts\nAssertionError: expected 1 to be 1.02",
        )],
    });
    let f = habits_check::check_once(&proven, &second, &p, OWN, &[], "").await;
    assert_eq!(f.tests[0].verdict, Verdict::Proven);
    assert!(habits_check::blocking(&f, &p).is_empty());
    assert!(habits_check::open_findings(&f, &p).is_empty());

    // The pull request carries the failing and the passing result, the
    // history and who it affects; a second check rewrites it in place.
    let body = "## Summary\nRounds each cart line.\n".to_string();
    let section = habits_check::section(&p, &f);
    let out = habits_check::shape_body(body, &section);
    assert!(out.contains("**Reproduced, then fixed**"));
    assert!(out.contains(
        "fails without the change: `AssertionError: expected 1 to be 1.02`; passes with it."
    ));
    assert!(out.contains("`src/cart/total.ts` (#212, #219): Per-line rounding shipped in #212 and was reverted in #219"));
    assert!(out.contains("`acme/storefront:src/checkout/summary.tsx` (uses lineTotals[].amount): safe: it reads `amount`"));
    assert!(out.contains("`src/invoice/pdf.ts` (uses cartTotal): updated in this pull request"));
    assert_eq!(habits_check::shape_body(out.clone(), &section), out);
}

#[test]
fn the_pipeline_still_does_each_of_these_at_its_place() {
    // Planning: the worker reads the facts for the draft and its gaps go in
    // the one send-back; what is left is kept, the test moves first and the
    // coder's design carries the section.
    let plan_src = include_str!("../plan.rs");
    let facts_at = plan_src
        .find("habit_reader.facts(&draft).await")
        .expect("facts read");
    let send_at = plan_src
        .find("super::fix_location::send_back(&back)")
        .expect("send-back");
    assert!(
        facts_at < send_at,
        "the facts are read before the send-back"
    );
    assert!(plan_src[facts_at..send_at].contains("back.extend(habit_gaps.iter().cloned())"));
    for call in [
        "super::habits::keep_open(d, &facts, &left)",
        "super::habits::put_repro_first(&files.tasks, &test)",
        "super::habits::with_design_section(&files.design, d, &files.tasks)",
        "super::habits::fold_questions(&mut d)",
    ] {
        assert!(plan_src.contains(call), "{call}");
    }
    // The planner is told, for every kind of ticket.
    for intent in [TicketIntent::Bug, TicketIntent::Feature] {
        assert!(fix_location::planner_guidance(intent).contains("\"habits\""));
    }

    // After the build: the check runs on the open pull request, takes a
    // step of its own when the sandbox runs need one, and its result goes
    // into the description and the open items.
    let rp = run_passes_src();
    let gate = rp.find("habits_check::gate(").expect("the check runs");
    let after = &rp[gate..gate + 1400];
    assert!(after.contains("habits_check::GateStop::NeedsTime"));
    assert!(after.contains("steps::Gate::Checks"));
    assert!(after.contains("habits_check::open_findings(h, &plan_result)"));
    assert!(rp.contains("habits: habits_pr.as_ref()"));
    let pr_src = include_str!("../pr.rs");
    let with_checks = fn_src(pr_src, "fn with_checks(");
    assert!(with_checks.contains("super::habits_check::shape_body("));
    // It asks for a step once, and the sandbox wait fits a step.
    let check_src = include_str!("../habits_check.rs");
    assert!(check_src.contains("std::mem::replace(&mut l.habits_waited, true)"));
    const { assert!(habits_check::SANDBOX_STEP_SECS < 900) };
}
