//! ga. A pull request that removed a duplicated filter chip changed a shared
//! slug helper and added hundreds of lines of unit tests for a few dozen
//! lines of code, with fixtures named `dock-a` and `dock-b` although the run had
//! queried the database and quoted the two real records' ids. CI went red on
//! a test outside the diff. Each CI fix round's sandbox reported "2 of 2
//! checks passed": its test step was the first workflow line with a test
//! command, another package's `npm test` with `--passWithNoTests`, so no
//! touched test ran. The coder's own `vitest run` of the touched files
//! exited 1 again and again (and 127 when `pnpm` was missing), and the fixes
//! were pushed anyway. After several attempts the supervisor put one source file
//! back to an older green head in a commit titled "Revert the change that
//! broke CI", leaving the tests that expected the reverted helper in place.
//!
//! Guarantee: the sandbox runs the owning package's own scripts: its whole
//! unit suite when a non-test file changed, only the touched test files
//! when nothing but tests changed, and its type check and lint
//! (`common::push_gate::package_steps`, `push_checks::merge_steps`). A CI
//! fix is pushed only when those ran on its last edit and passed; a sandbox
//! that couldn't run them pushes with `Unverified:` and what ran
//! (`push_checks::before_push`). A commit message that says it takes
//! something out while the diff adds test lines is rewritten from the diff
//! (`round::land`, `push_gate::true_message`). A commit that removes a
//! definition a test still names is held until the test goes with it. A
//! failing test is fixed at most twice; then the rounds stop with the test
//! and its error named, and nothing is rolled back. Fixture ids invented
//! while the run looked up real ones, and three or more test cases with the
//! same body, go back to the coder (`minimal_change`).

use super::fixture::fn_src;
use crate::passes::checks::{CheckKind, Step};
use crate::passes::minimal_change::{self, Kind};
use crate::passes::push_checks;
use common::push_gate::{self, FileChange, FixLedger, Verdict};

const WEB: &str =
    r#"{"scripts":{"test":"vitest","lint":"next lint","check-types":"tsc --noEmit"}}"#;
const LAMBDA: &str = r#"{"scripts":{"test":"jest"}}"#;

fn manifests() -> Vec<(String, String)> {
    vec![
        (
            String::new(),
            r#"{"scripts":{"build":"turbo build"}}"#.into(),
        ),
        ("apps/web".into(), WEB.into()),
        ("lambdas/resizer".into(), LAMBDA.into()),
    ]
}

fn touched() -> Vec<String> {
    [
        "apps/web/src/utils/filterMaps.ts",
        "apps/web/src/components/blocks/DocksFilter.tsx",
        "apps/web/src/test/utils/filterMaps.test.ts",
        "apps/web/src/test/components/dock-filter-dedupe.test.tsx",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[test]
fn the_owning_packages_whole_suite_replaces_another_packages_test_line() {
    // What run_checks ran: the derived type check, CI's lint, and the
    // first workflow's test line scoped to the touched test files.
    let before = vec![
        Step {
            kinds: vec![CheckKind::Typecheck],
            cmd: "pnpm check-types".into(),
            scope: None,
        },
        Step {
            kinds: vec![CheckKind::Tests],
            cmd: "(cd lambdas/resizer && npm test) -- --passWithNoTests filterMaps.test.ts".into(),
            scope: None,
        },
    ];
    let pkg = push_checks::to_steps(&push_gate::package_steps(
        &touched(),
        &manifests(),
        &["pnpm-lock.yaml".into()],
    ));
    let steps = push_checks::merge_steps(before, pkg);
    let cmds: Vec<&str> = steps.iter().map(|s| s.cmd.as_str()).collect();
    assert_eq!(
        cmds,
        vec![
            "pnpm check-types",
            // A shared helper changed: the web package's whole suite, so a
            // caller outside the diff is tested too.
            "cd apps/web && CI=true pnpm exec vitest run",
            "cd apps/web && pnpm run lint",
        ]
    );
    assert!(!cmds.iter().any(|c| c.contains("passWithNoTests")));
    assert!(steps[1]
        .scope
        .as_deref()
        .unwrap()
        .contains("whole unit suite"));
}

#[test]
fn a_fix_whose_tests_failed_or_never_ran_is_not_pushed() {
    use crate::passes::proof::SandboxRecord;
    let files = touched();
    // Types passed; the suite failed (the coder's `vitest run` exit 1).
    let red = SandboxRecord {
        ran: vec![CheckKind::Typecheck, CheckKind::Tests],
        passed: vec![CheckKind::Typecheck],
        ..Default::default()
    };
    assert!(matches!(
        push_gate::verdict(&push_checks::checked_of(&red, &files)),
        Verdict::Red(_)
    ));
    // "2 of 2 checks passed" with no test step: never a verified push.
    let no_tests = SandboxRecord {
        ran: vec![CheckKind::Typecheck, CheckKind::Lint],
        passed: vec![CheckKind::Typecheck, CheckKind::Lint],
        ..Default::default()
    };
    let Verdict::Unverified(why) = push_gate::verdict(&push_checks::checked_of(&no_tests, &files))
    else {
        panic!("a fix no suite ran on is unverified");
    };
    assert!(push_gate::unverified_line(&why).starts_with("Unverified: no unit suite"));
    // An edit after the last check run: the checks are owed first.
    let owed = SandboxRecord {
        refused_for_time: true,
        ..red
    };
    assert_eq!(
        push_gate::verdict(&push_checks::checked_of(&owed, &files)),
        Verdict::Owed
    );
    // The CI fix loop asks the gate before it lands anything, and records
    // the pushed fix against the failing tests.
    let src = include_str!("../resume.rs");
    let at = |n: &str| src.find(n).unwrap_or_else(|| panic!("{n}"));
    assert!(at("push_checks::before_push(") < at("match super::round::land_agreeing("));
    assert!(at("match super::round::land_agreeing(") < at("push_checks::record_push("));
}

#[test]
fn a_revert_message_that_adds_tests_back_is_rewritten_and_its_tests_must_go() {
    let patch = "@@ -560,40 +560,3 @@\n-export function dedupeDocksBySlug(docks) {\n-  return docks;\n-}\n export const slugToId = (s) => s;";
    let commit = vec![
        FileChange {
            path: "apps/web/src/utils/filterMaps.ts".into(),
            status: "modified".into(),
            additions: 0,
            deletions: 37,
            patch: patch.into(),
        },
        FileChange {
            path: "apps/web/src/test/components/charters-block-dedup.test.tsx".into(),
            status: "added".into(),
            additions: 64,
            deletions: 0,
            patch: String::new(),
        },
    ];
    let old = "Revert the change that broke CI; it is deferred on the pull request";
    let msg = push_gate::true_message(old, &commit);
    assert!(
        msg.starts_with("Change filterMaps.ts, charters-block-dedup.test.tsx (+64/-37)"),
        "{msg}"
    );
    assert!(msg.contains("adds test lines to"));
    // The helper it takes out is still imported by a test: held.
    let removed = push_gate::removed_symbols(&commit);
    assert_eq!(removed[0].0, "dedupeDocksBySlug");
    let tests = vec![(
        "apps/web/src/test/utils/filterMaps.test.ts".to_string(),
        "import { dedupeDocksBySlug, slugToId } from '../../utils/filterMaps';".to_string(),
    )];
    let stale = push_gate::still_expected(&removed, &tests);
    assert_eq!(stale.len(), 1);
    assert!(stale[0].line().contains("same commit"));
    // Every round lands through the message check.
    let round = include_str!("../round.rs");
    let land = fn_src(round, "pub(crate) async fn land(");
    assert!(land.find("true_message(").unwrap() < land.find(".batch_write(").unwrap());
    // And no commit is titled as a revert by the supervisor any more.
    assert!(!include_str!("../resume.rs").contains("\"Revert the change that broke CI"));
}

#[test]
fn a_test_red_after_two_pushed_fixes_stops_named_without_a_rollback() {
    let log = "2030-03-16T23:37:45.4879187Z  FAIL  src/test/app/global-error.test.tsx > global-error.tsx > reports the error via reportClientError";
    let keys = push_checks::failing_keys(&[], log, &["Test Suite".into()]);
    assert_eq!(
        keys,
        vec!["src/test/app/global-error.test.tsx > global-error.tsx > reports the error via reportClientError"]
    );
    let mut ledger = FixLedger::default();
    ledger.record_push(&keys);
    assert_eq!(ledger.stop(&keys), None, "one fix pushed: a second may go");
    ledger.record_push(&keys);
    let stop = ledger
        .stop(&keys)
        .expect("two pushed fixes, still red: stop");
    let text = stop.text("In HTML, <html> cannot be a child of <div>.");
    assert!(text.contains("global-error.test.tsx") && text.contains("<html> cannot be"));
    assert!(text.contains("nothing was rolled back"));
    // The run had four attempts and a rollback; the stop now comes first.
    let src = include_str!("../resume.rs");
    let stop_at = src.find("ledger.stop(&failing_keys)").unwrap();
    assert!(stop_at < src.find("// Implement fix").unwrap());
}

#[test]
fn invented_fixture_ids_and_repeated_cases_go_back_to_the_coder() {
    // The run looked up the two records behind the duplicate chip.
    let lookups = vec![
        "- [L1] Asked Production database: SELECT id, name FROM dock WHERE ... Came back: 1a2b3c4d… 'Guest-dock' (active); 5e6f7a8b… 'Guest-Dock' (inactive)".to_string(),
    ];
    let mut cases = String::new();
    for (i, name) in ["Guest-dock", "Guest-Dock", "guest dock", "GUEST-DOCK"]
        .iter()
        .enumerate()
    {
        cases.push_str(&format!(
            "+  it('keeps one chip for {name}', () => {{\n+    const out = dedupeDocksBySlug([{{ id: 'dock-{}', label: '{name}' }}]);\n+    expect(out).toHaveLength(1);\n+  }});\n",
            ["a", "b", "c", "d"][i]
        ));
    }
    let mut big = cases.clone();
    for i in 0..360 {
        big.push_str(&format!("+  expect(slugToId('s{i}')).toBe('s{i}');\n"));
    }
    let code: String = (0..50).map(|i| format!("+  const k{i} = {i};\n")).collect();
    let diff = vec![
        (
            "apps/web/src/test/utils/filterMaps.test.ts".to_string(),
            "modified".to_string(),
            big,
        ),
        (
            "apps/web/src/utils/filterMaps.ts".to_string(),
            "modified".to_string(),
            code,
        ),
    ];
    let ids = minimal_change::made_up_ids(&diff, &lookups);
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].kind, Kind::MadeUpIds);
    assert!(ids[0].what.contains("`dock-a`") && ids[0].what.contains("`dock-d`"));
    assert!(!ids[0].excusable());
    // Hundreds of test lines for 50 of code: the size check #742 added fires
    // on this diff today (the run predates it), and so does the new check
    // for cases that differ only in their values.
    let found = minimal_change::check(&diff, &[], "Only one guest-dock filter shows", false);
    let sizes: Vec<&str> = found
        .iter()
        .filter(|w| w.kind == Kind::TestSize)
        .map(|w| w.what.as_str())
        .collect();
    assert!(
        sizes.iter().any(|w| w.contains("added lines against 50")),
        "{sizes:?}"
    );
    assert!(
        sizes
            .iter()
            .any(|w| w.contains("4 added cases with the same body")),
        "{sizes:?}"
    );
    // The habits check of the change reads the lookups.
    let hc = include_str!("../habits_check.rs");
    assert!(hc.contains("minimal_change::made_up_ids(&diff, &looked)"));
}
