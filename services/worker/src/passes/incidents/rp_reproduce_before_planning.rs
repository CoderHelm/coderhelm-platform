//! rp. A data-pipeline ticket named four camps of a vendor's booking
//! system filed under the wrong sport ("Squash Camp-Half Day" listed under
//! volleyball, "Total Skills Hockey Camp-Full Day" missing from hockey).
//! The pipeline already had a table for exactly this, `CAMP_SPORT_OVERRIDES`,
//! with entries for "squash camp" and "total skills hockey camp". Its lookup
//! matched exact names, and the vendor appends "-Half Day" / "-Full Day", so
//! the entries never matched: the lookup returned `None` for every one of
//! the ticket's names on the base. The run's own notes said so, then
//! planned "a keyword step after the override": +595/-186 across the
//! pipeline, a moved extractor, keyword matching, new logging, a fixture,
//! other programs moved to another lookup (its own reviewer flagged that),
//! and two questions on the pull request that one page load answered. The
//! new test was never run on the base: the repository's CI runs each test
//! file with a `python "$t"` loop, which CoderHelm couldn't narrow, so the
//! test was recorded as not run and the description said nothing of it.
//! The right change stripped the suffix in the lookup and added two
//! entries, +49/-1, mostly a test of the exact names stored on staging that
//! fails on the base. Across the 53 runs audited, 8 of 10 bug tickets that
//! wrote code ran nothing before their first edit, 6 diffs of 300+ lines
//! answered a ticket naming 1 to 10 records, and none of the 17 runs whose
//! tests were checked had a test shown to fail on the base.
//!
//! Guarantee: a bug plan reproduces before it plans (`## Reproduce`: the
//! ticket's exact inputs and their stored variants, a command this run ran
//! on the base with `reproduce_on_base` and its quoted output, the wrong
//! line), or says `Unverified:` after trying. The fix goes in the mechanism
//! the wrong line is in; a new resolution path beside it goes back. A
//! question a page load or a query answers is looked up before it is
//! decided. A diff that rewrites a path for a few named records goes back
//! to be narrowed (`minimal_change::whole_path`). Each new test runs on the
//! base and the head the way CI runs it, and a bug fix no test was shown to
//! fail for says `Unverified:` in its description.

use crate::passes::habits_check::{self, FileRun, PrFacts, TestResult, Verdict};
use crate::passes::minimal_change::{self, Kind};
use crate::passes::plan::PlanResult;
use crate::passes::reproduce;
use common::repro_first::{self, Attempt};
use common::test_env;

const TICKET: &str = "Update sport categorization for summer camps\nObserved\nThe hockey filter only catches the preschool hockey class, not the Total Skills Hockey Camp-Full Day (a0000001-0000-4000-8000-000000000001)\nWhen filtering for Volleyball, Squash Camp-Half Day (a0000002-0000-4000-8000-000000000002) shows\nExpected\nSquash Camp-Half Day (a0000002-0000-4000-8000-000000000002)\nSquash Camp- Full Day (a0000003-0000-4000-8000-000000000003)\nSquash High-Performance Camp (a0000004-0000-4000-8000-000000000004)";

/// The call that reproduces it on the base, and what it printed.
const CALL: &str = "cd dags && python -c \"from utils.schedule_common import get_camp_sport_override as g; [print(n, g(n)) for n in ['Squash Camp-Half Day', 'Squash Camp-Full Day', 'Squash Camp- Full Day', 'Total Skills Hockey Camp-Full Day']]\"";
const PRINTED: &str = "Squash Camp-Half Day None\nSquash Camp-Full Day None\nSquash Camp- Full Day None\nTotal Skills Hockey Camp-Full Day None";

fn ran() -> Vec<Attempt> {
    vec![Attempt {
        command: CALL.into(),
        exit: Some(0),
        output: PRINTED.into(),
        error: String::new(),
    }]
}

const REUSE: &str = "## Reuse\n- `CAMP_SPORT_OVERRIDES` and `get_camp_sport_override` in dags/utils/schedule_common.py\n\n## Must not change\nOther programs' sport lookups; logging.\n\n## Measure first\nNone: the change stays inside this repository.\n";

/// The plan as the run wrote it.
fn plan_as_run() -> PlanResult {
    PlanResult {
        proposal: "## Problem\nCamps sit on the wrong sport.\n\n## Approach\nExact keys miss suffixed names, so a keyword step is added after the override, and booking programs move to the active-only sport map.".into(),
        design: format!("{REUSE}\n## Smallest change\nAdd `resolve_camp_sport_id` and `build_sport_name_map`: the override first, then a keyword match on the camp name, for every camp the activities path loads.\n\n## Proof\n`tests/test_camp_sport.py` over 116 live camp names.\n\n## Out of scope\n- Inactive Squash camps: decided with the default: they show under the squash filter as today\n"),
        tasks: "- [ ] dags/utils/schedule_common.py: [modify] add resolve_camp_sport_id\n- [ ] dags/camp_pipeline.py: [modify] use it for every camp\n- [ ] tests/test_camp_sport.py: [create] the test\n".into(),
        ..Default::default()
    }
}

/// The plan the gates ask for.
fn plan_right() -> PlanResult {
    PlanResult {
        proposal: "## Problem\nCamps sit on the wrong sport.\n".into(),
        design: format!("{REUSE}\n## Reproduce\nInputs: a0000001-0000-4000-8000-000000000001, a0000002-0000-4000-8000-000000000002, a0000003-0000-4000-8000-000000000003, a0000004-0000-4000-8000-000000000004; staging lookup L2 (name LIKE '%squash camp%'): `Squash Camp- Full Day`, `Squash Camp-Full Day`, `Squash Camp-Half Day`, `Summer Squash Camp Plus`\nRan on base: `{CALL}`\nOutput:\n```\n{PRINTED}\n```\nWrong line: dags/utils/schedule_common.py:99 `return CAMP_SPORT_OVERRIDES.get(normalized)` matches the whole name, suffix included.\n\n## Smallest change\nStrip a trailing `-Half Day` / `-Full Day` before `CAMP_SPORT_OVERRIDES.get` in `get_camp_sport_override`; add 2 entries.\n\n## Proof\nOne test of the four stored names; it fails on the base.\n\n## Out of scope\n- Inactive Squash camps: decided with the default: they show as today. Looked up: the summer camps page already shows a squash chip\n"),
        tasks: "- [ ] tests/test_camp_sport.py: [create] the stored names map to squash\n- [ ] dags/utils/schedule_common.py: [modify] strip the suffix in get_camp_sport_override\n".into(),
        ..Default::default()
    }
}

#[test]
fn the_plan_reproduces_on_the_base_and_fixes_the_lookup_it_lands_in() {
    let g = reproduce::plan_gaps(&plan_as_run(), true, TICKET, &[], true, true);
    let all = g.join("\n");
    assert!(all.contains("lacks `## Reproduce`"), "{all}");
    // The question one page load answers is looked up first.
    assert!(
        all.contains("Inactive Squash camps") && all.contains("check_url"),
        "{all}"
    );

    // The plan the send-back asks for passes, with the run it quotes.
    let g = reproduce::plan_gaps(&plan_right(), true, TICKET, &ran(), true, true);
    assert!(g.is_empty(), "{g:?}");
    // The same section with nothing run behind it goes back.
    let g = reproduce::plan_gaps(&plan_right(), true, TICKET, &[], true, true);
    assert!(
        g.iter().any(|l| l.contains("not a command this run ran")),
        "{g:?}"
    );

    // A reproduction that lands in the overrides lookup, with the keyword
    // step planned beside it, goes back.
    let mut beside = plan_right();
    beside.design = beside.design.replace(
        "Strip a trailing `-Half Day` / `-Full Day` before `CAMP_SPORT_OVERRIDES.get` in `get_camp_sport_override`; add 2 entries.",
        "Add `resolve_camp_sport_id`: the override first, then a keyword match on the name.",
    );
    let g = reproduce::plan_gaps(&beside, true, TICKET, &ran(), true, true);
    assert!(
        g.iter()
            .any(|l| l.contains("`CAMP_SPORT_OVERRIDES`") && l.contains("doesn't change it")),
        "{g:?}"
    );
    assert!(g.iter().any(|l| l.contains("keyword match")), "{g:?}");

    // The plan check calls it, on the draft and on the revision.
    let src = include_str!("../plan.rs");
    assert_eq!(src.matches("super::reproduce::plan_gaps(").count(), 2);
    assert!(src.contains("super::reproduce::tool()"));
    // Its inputs are the ticket's own records.
    assert_eq!(repro_first::ticket_ids(TICKET).len(), 4);
}

fn patch(sign: char, lines: &[String]) -> String {
    lines
        .iter()
        .map(|l| format!("{sign}{l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_whole_path_rewrite_for_four_camps_goes_back_and_the_lookup_fix_does_not() {
    // The run's diff, in shape: the pipeline that loads every camp rewritten.
    let added: Vec<String> = (0..300)
        .map(|i| format!("    sport_id = resolve_camp_sport_id(row, names_{i}, sport_map)"))
        .collect();
    let removed: Vec<String> = (0..90)
        .map(|i| format!("    sport_id = group_sport.get(row['group_{i}'])"))
        .collect();
    let wide = vec![(
        "dags/camp_pipeline.py".to_string(),
        "modified".to_string(),
        format!("{}\n{}", patch('+', &added), patch('-', &removed)),
    )];
    let found = minimal_change::check(&wide, &[], TICKET, false);
    let w = found
        .iter()
        .find(|w| w.kind == Kind::WholePath)
        .expect("whole path");
    assert!(
        w.what.contains("390 changed code lines") && w.what.contains("4 record(s)"),
        "{}",
        w.what
    );
    assert!(w.instruction().contains("Narrow it"));

    // The right change: the suffix stripped in the lookup, two entries.
    let narrow = vec![
        (
            "dags/utils/schedule_common.py".to_string(),
            "modified".to_string(),
            "-    normalized = re.sub(r\"\\s+\", \" \", str(name).strip().lower())\n+    normalized = re.sub(r\"\\s*-\\s*(half|full) day$\", \"\", re.sub(r\"\\s+\", \" \", str(name).strip().lower()))\n+    'summer squash camp plus': 'b0000001-0000-4000-8000-00000000000a',\n+    'squash high-performance camp': 'b0000001-0000-4000-8000-00000000000a',".to_string(),
        ),
        (
            "tests/test_camp_sport.py".to_string(),
            "added".to_string(),
            patch('+', &(0..45).map(|i| format!("    assert_case({i})")).collect::<Vec<_>>()),
        ),
    ];
    let found = minimal_change::check(&narrow, &[], TICKET, false);
    assert!(found.is_empty(), "{found:?}");
    // A reason the narrower change can't work keeps a wide one.
    assert!(minimal_change::standing(
        &minimal_change::check(&wide, &[], TICKET, false),
        "Wider than the ticket: dags/camp_pipeline.py: the vendor gives no name for these camps"
    )
    .is_empty());
}

const CI: &str = r#"
name: Tests
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: uv pip install --quiet "apache-airflow[postgres]==2.10.3" -r requirement.txt
      - name: Run tests
        run: |
          failed=0
          for t in tests/test_*.py; do
            echo "::group::$t"
            python "$t" || failed=1
            echo "::endgroup::"
          done
          exit $failed
"#;

#[test]
fn the_new_test_runs_on_the_base_the_way_ci_runs_it() {
    let env = test_env::from_workflows(&[(".github/workflows/tests.yml".into(), CI.into())], &[])
        .expect("the test job");
    // CoderHelm's runner list knows no command here; the loop runs the file.
    assert_eq!(
        habits_check::file_cmd(None, Some(&env), "tests/test_camp_sport.py").as_deref(),
        Ok("python tests/test_camp_sport.py")
    );
    assert!(
        habits_check::file_cmd(None, None, "tests/test_camp_sport.py")
            .unwrap_err()
            .contains("no unit test command")
    );
    // Fails on the base with the reported names, passes on the head.
    let base = FileRun {
        code: Some(1),
        output: "AssertionError: 'Squash Camp-Half Day' maps to None, expected squash".into(),
    };
    let head = FileRun {
        code: Some(0),
        output: String::new(),
    };
    assert_eq!(habits_check::verdict(&head, &base).0, Verdict::Proven);
    // A test that passes on the base goes back to the coder.
    let plan = PlanResult {
        tasks: "- [ ] tests/test_camp_sport.py: [create] x\n".into(),
        ..Default::default()
    };
    let either = PrFacts {
        bug: true,
        tests: vec![TestResult {
            path: "tests/test_camp_sport.py".into(),
            verdict: habits_check::verdict(&head, &head).0,
            detail: String::new(),
            why: None,
        }],
        ..Default::default()
    };
    assert!(habits_check::blocking(&either, &plan)
        .iter()
        .any(|l| l.contains("passes with the change taken out")));
    // A bug fix whose test never ran both ways says so in its description.
    let not_run = PrFacts {
        bug: true,
        tests: vec![TestResult {
            path: "tests/test_camp_sport.py".into(),
            verdict: Verdict::NotRun,
            detail: "the sandbox gave no result for it".into(),
            why: None,
        }],
        ..Default::default()
    };
    assert!(habits_check::section(&plan, &not_run).contains(habits_check::UNVERIFIED_FIX));
    let proven = PrFacts {
        bug: true,
        tests: vec![TestResult {
            path: "tests/test_camp_sport.py".into(),
            verdict: Verdict::Proven,
            detail: "AssertionError".into(),
            why: None,
        }],
        ..Default::default()
    };
    assert!(!habits_check::section(&plan, &proven).contains("Unverified"));
}
