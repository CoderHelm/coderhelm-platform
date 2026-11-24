//! oz. The pull request of `ow` and `ox`, its whole review history. The
//! fix was right by its third commit; CI and the preview's end-to-end run
//! were green, and a person's product answers were built. The
//! review still requested changes for six more rounds on two generic
//! findings. "A parallel structure next to an existing one" never named the
//! function it duplicated across three rewrites of the file. "A test out of
//! proportion to the change" moved from one test file to the next as each
//! was trimmed, and landed on a test whose selected-chip and row-order
//! cases the review had asked to restore. CoderHelm's own fix rounds
//! deferred both as making no progress, and the review kept blocking. A
//! person had answered that the `/charters/<berth>` path pages should cover
//! both rows; the run reverted that work as out of scope, and a later
//! review called it missing.
//!
//! Guarantee (`review_discipline::judge`, run on every review before the
//! verdict): a blocking finding names its file, its line and what it
//! conflicts with or asks for; a generic finding doesn't move to another
//! file; a finding never asks the opposite of an earlier request; a
//! finding CoderHelm's rounds deferred holds no more unless new code gives
//! it new evidence; a person's answer decides scope; and once each ticket
//! criterion is proven both ways with CI green, what is left that isn't a
//! shown defect is a follow-up note on an approval.

use crate::passes::review_agent::{verdict_heading, Finding};
use crate::passes::review_discipline::{self as d, Earlier, Facts};
use crate::passes::review_one_verdict as one;
use crate::passes::review_policy::named_holding;
use crate::passes::review_verdict::Verdict;
use serde_json::json;

const MAPS: &str = "apps/web/src/utils/filterMaps.ts";
const MAPS_TEST: &str = "apps/web/src/test/utils/filterMaps.test.ts";
const GRID_TEST: &str = "apps/web/src/test/components/filter-nav-dock-grid.test.tsx";
const DEDUPE_TEST: &str = "apps/web/src/test/components/docks-filter-dedupe.test.tsx";
const VALIDATION_TEST: &str =
    "apps/web/src/test/components/charters-block-filter-validation.test.tsx";
const PARALLEL: &str = "A parallel structure next to an existing one";
const TEST_SIZE: &str = "A test out of proportion to the change";

/// The person's answer on the ticket, the day before.
const ANSWER: &str = "Yes: the /charters/<berth> path pages should cover both rows too, the \
     berth chip and the charter list.";

fn finding(v: serde_json::Value) -> Finding {
    let mut base = json!({"line": 1, "severity": "blocking", "evidence": "verified-defect",
        "category": "scope", "caused_by_diff": true, "body": "b"});
    for (k, val) in v.as_object().unwrap() {
        base[k] = val.clone();
    }
    serde_json::from_value(base).unwrap()
}

fn size(file: &str) -> Finding {
    finding(
        json!({"file": file, "line": 4, "category": "tests", "title": TEST_SIZE,
        "body": "The tests are 240 added lines against 40 lines of changed code for this change. Cut them to the cases the ticket names."}),
    )
}

fn facts<'a>(earlier: &'a [Earlier], deferred: &'a [String], people: &'a [String]) -> Facts<'a> {
    Facts {
        earlier,
        deferred,
        changed_since_previous: None,
        people,
        criteria_proven: false,
        ci_green: true,
        unobtainable: &[],
    }
}

#[test]
fn the_parallel_structure_that_names_nothing_never_blocks() {
    // As it was posted on every rewrite of the file: no line, no function.
    let mut fs = vec![finding(json!({"file": MAPS, "line": 0, "title": PARALLEL,
        "body": "Extend the existing one (add the entries or the parameter there) and remove the new one."}))];
    let made = d::judge(&mut fs, &facts(&[], &[], &[]));
    assert_eq!(made.len(), 1);
    assert!(!fs[0].holds_merge());
    // Named and anchored, the same check blocks in a first review.
    let mut named = vec![finding(
        json!({"file": MAPS, "line": 120, "title": PARALLEL,
        "body": "`apps/web/src/utils/filterMaps.ts` adds `dedupeDockOptionsBySlug` next to the existing `dedupeDocksBySlug`."}),
    )];
    assert!(d::judge(&mut named, &facts(&[], &[], &[])).is_empty());
    assert!(named[0].note_only.is_none());
    // The check anchors it on the new definition's line.
    let patch = "@@ -110,3 +110,9 @@ export const x = 1;\n context\n+export function dedupeDockOptionsBySlug(o) {\n+  return o;\n+}\n";
    assert_eq!(
        crate::passes::review_rules::anchor_line(patch, &named[0].body),
        111
    );
}

#[test]
fn the_test_size_finding_doesnt_move_and_never_undoes_a_request() {
    // Rounds 3 to 6: each test trimmed, the finding moved to the next one.
    let mut earlier = vec![Earlier::of_item(&format!("{TEST_SIZE} (`{MAPS_TEST}`)"))];
    for next in [GRID_TEST, DEDUPE_TEST, VALIDATION_TEST] {
        let mut fs = vec![size(next)];
        let made = d::judge(&mut fs, &facts(&earlier, &[], &[]));
        assert_eq!(made.len(), 1, "{next}");
        assert!(made[0].why.contains("raised before on"), "{}", made[0].why);
        assert!(!fs[0].holds_merge());
        earlier.push(Earlier::of_item(&format!("{TEST_SIZE} (`{next}`)")));
    }
    // The review's own earlier request on the dedupe test.
    let asked = vec![Earlier::of_thread(
        DEDUPE_TEST,
        "**Should fix: Restore the selected-chip and row-order cases**\n\nThe rewrite dropped them; put back both cases.",
    )];
    let mut fs = vec![size(DEDUPE_TEST)];
    let made = d::judge(&mut fs, &facts(&asked, &[], &[]));
    assert_eq!(made.len(), 1);
    assert!(made[0].why.contains("would undo my earlier request"));
    assert!(made[0].why.contains("Restore the selected-chip"));
}

#[test]
fn a_finding_the_rounds_deferred_holds_no_more_without_new_code() {
    let deferred = vec![
        format!("{PARALLEL} (`{MAPS}`)"),
        format!("{TEST_SIZE} (`{VALIDATION_TEST}`)"),
    ];
    let mut fs = vec![
        finding(json!({"file": MAPS, "line": 120, "title": PARALLEL,
            "body": "`x` next to the existing `y`."})),
        size(VALIDATION_TEST),
    ];
    let made = d::judge(&mut fs, &facts(&[], &deferred, &[]));
    assert_eq!(made.len(), 2);
    assert!(made.iter().all(|m| m.why.contains("deferred")));
    // New code on the line is new evidence: it may hold again.
    let changed: std::collections::HashMap<String, std::collections::HashSet<u64>> =
        [(MAPS.to_string(), std::collections::HashSet::from([121u64]))].into();
    let mut again = vec![finding(
        json!({"file": MAPS, "line": 120, "title": PARALLEL,
        "body": "`x` next to the existing `y`."}),
    )];
    let f = Facts {
        changed_since_previous: Some(&changed),
        ..facts(&[], &deferred, &[])
    };
    assert!(d::judge(&mut again, &f).is_empty());
    // The thread of a deferred finding holds the merge no more either.
    let src = include_str!("../review_pr.rs");
    let settle = &src[src.find("pub(crate) fn settle_own_findings(").unwrap()..];
    assert!(settle[..2400].contains("settled.push(line);\n                continue;"));
}

#[test]
fn a_persons_answer_decides_scope() {
    let people = vec![ANSWER.to_string()];
    let mut out = vec![finding(
        json!({"file": "apps/web/src/components/sanity-blocks/charters-block.tsx", "line": 30,
        "title": "Path-page changes are outside the ticket",
        "body": "The /charters/<berth> path pages changes are out of scope for a day slips chip fix; revert them."}),
    )];
    let made = d::judge(&mut out, &facts(&[], &[], &people));
    assert_eq!(made.len(), 1);
    assert!(made[0].why.contains("a person asked for this"));
    // Called missing after the revert: it says whose answer it was.
    let mut missing = vec![finding(
        json!({"file": "apps/web/src/components/sanity-blocks/charters-block.tsx", "line": 30,
        "category": "correctness", "defeats_goal": true,
        "title": "Path pages no longer cover both rows",
        "body": "The /charters/<berth> path pages are missing the berth chip dedupe."}),
    )];
    d::judge(&mut missing, &facts(&[], &[], &people));
    assert!(missing[0].body.contains("A person asked for this"));
    assert!(missing[0]
        .body
        .contains("path pages should cover both rows"));
}

#[test]
fn the_last_round_approves_with_follow_up_notes_once_the_criteria_pass() {
    let proofs = json!([
        {"criterion": "One Multi-Dock chip on the day slips page", "test": "e2e/slips.spec.ts › one chip",
         "record": {"head": "abcdef0123", "base": {"ran": true, "passed": 0, "failed": 1}, "change": {"ran": true, "passed": 1, "failed": 0}}},
        {"criterion": "Path pages cover both rows", "test": "e2e/charters.spec.ts › both rows",
         "record": {"head": "abcdef0123", "base": {"ran": true, "passed": 0, "failed": 1}, "change": {"ran": true, "passed": 3, "failed": 0}}}
    ])
    .to_string();
    let proven = d::criteria_met(&proofs, "abcdef0123").unwrap();
    assert!(proven);
    // What the last review still raised: the two generic findings, anchored
    // and named this time, and a concern it couldn't show.
    let mut fs = vec![
        finding(json!({"file": MAPS, "line": 120, "title": PARALLEL,
            "body": "`a.ts` adds `x` next to the existing `y`."})),
        size(VALIDATION_TEST),
        finding(json!({"file": MAPS, "line": 12, "category": "correctness",
            "evidence": "unverified-concern", "title": "Inactive docks may still show",
            "body": "Could a hidden dock still render?"})),
    ];
    let f = Facts {
        criteria_proven: proven,
        ..facts(&[], &[], &[])
    };
    let made = d::judge(&mut fs, &f);
    assert_eq!(made.len(), 3, "{made:?}");
    assert!(made.iter().all(|m| m.why.starts_with("Follow-up")));
    // On CoderHelm's own pull request nothing of it is work that holds it.
    let own_open = crate::passes::own_review::required(&fs, &[]);
    assert!(own_open.is_empty(), "{own_open:?}");
    let verdict = Verdict::from_holding(named_holding(&fs, &[], &[], false, &own_open, &[]));
    assert_eq!(verdict.event(), "APPROVE");
    let body = format!(
        "{}\n\nThe chip shows once.",
        verdict_heading(&verdict, fs.len())
    );
    assert!(body.starts_with("✅ **Approved with notes**"));
    assert!(one::check(&body).is_empty());
    // A shown defect still holds, criteria or not.
    let mut defect = vec![finding(json!({"file": MAPS, "line": 12, "category": "bug",
        "title": "Crash on an empty dock list", "body": "`docks[0].id` throws on []."}))];
    assert!(d::judge(&mut defect, &f).is_empty());
    assert!(defect[0].holds_merge());
}

/// The rules run on every review, after the findings are graded and before
/// the verdict, with the run's own ledger, people's words and proofs.
#[test]
fn the_review_runs_the_rules_before_its_verdict() {
    let src = include_str!("../review_pr.rs");
    let finalize = src
        .find("super::review_severity::finalize(&mut findings);")
        .unwrap();
    let judge = src.find("super::review_discipline::judge(").unwrap();
    let verdict = src.find("review_agent::decide_verdict(&findings").unwrap();
    assert!(finalize < judge && judge < verdict);
    let block = &src[finalize..judge];
    assert!(block.contains("deferred_items"));
    assert!(block.contains("people.push(ticket_context.clone())"));
    assert!(block.contains("criteria_met(&j, &head_sha)"));
}
