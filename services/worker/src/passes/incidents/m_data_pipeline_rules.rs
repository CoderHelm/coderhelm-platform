//! m. A data-pipeline pull request kept one column of an item stable by no
//! longer advancing `updated_at` on some update and upsert paths. Two
//! incremental readers depended on that timestamp: one took
//! `MAX(updated_at)` as its sync watermark, the other used
//! `GREATEST(parent.updated_at, MAX(child.updated_at))` as a change key. So
//! edited rows refetched every run and other changes stopped reaching the
//! search index. The same change picked a winner among sibling rows by start
//! date and broke ties differently from the full sync, which ordered by
//! start, `updated_at`, then id; and it skipped records without a parent
//! silently. Nothing showed the change on real data.
//!
//! Guarantee: the plan check sends back a plan that changes how a cursor
//! column is written without listing its readers, or picks a winner among
//! rows without naming the other path. Before the pull request, the
//! checklist fails a cursor write that can stop advancing, names the readers
//! code search finds, fails an ordering key that differs from the other
//! path's and a silent or per-row skip, and passes the fixed change. The
//! proof check holds a data change with a connected source until the
//! evidence lists records before and after, and the description shows them.

use super::fixture::*;
use crate::passes::feature::checklist::Status;
use crate::passes::feature::data_rules::{self, Facts};
use crate::passes::plan::PlanResult;
use crate::passes::plan_judge;
use crate::passes::proof::{self, RunEvidence};
use crate::passes::ticket_history::TicketHistory;

fn file(path: &str, patch: &str) -> (String, String, String) {
    (path.into(), "modified".into(), patch.into())
}

/// The shipped change: the upsert keeps `updated_at` unless the group
/// changed, the incremental path orders by start alone, and items without a
/// parent are skipped with nothing said.
fn shipped() -> Vec<(String, String, String)> {
    vec![
        file(
            "pipeline/sql/upsert_items.sql",
            "@@ -4,6 +4,6 @@\n ON CONFLICT (id) DO UPDATE SET\n   group_id = EXCLUDED.group_id,\n   first_seen_at = item.first_seen_at,\n-  updated_at = EXCLUDED.updated_at\n+  updated_at = CASE WHEN item.group_id IS DISTINCT FROM EXCLUDED.group_id THEN EXCLUDED.updated_at ELSE item.updated_at END\n",
        ),
        file(
            "pipeline/sql/incremental_current.sql",
            "@@ -1,3 +1,5 @@\n SELECT c.*\n FROM child c\n+WHERE c.parent_id = $1\n+ORDER BY c.start_date DESC\n+LIMIT 1\n",
        ),
        file(
            "pipeline/sync_items.py",
            "@@ -1,4 +1,6 @@\n def sync_items(rows):\n     for row in rows:\n+        if row.get(\"parent_id\") is None:\n+            continue\n         upsert(row)\n",
        ),
    ]
}

/// The fix: `GREATEST(existing, new)`, the full sync's ordering, the
/// missing parents collected and reported once, and tests for each.
fn fixed() -> Vec<(String, String, String)> {
    vec![
        file(
            "pipeline/sql/upsert_items.sql",
            "@@ -4,6 +4,6 @@\n ON CONFLICT (id) DO UPDATE SET\n   group_id = EXCLUDED.group_id,\n   first_seen_at = item.first_seen_at,\n-  updated_at = EXCLUDED.updated_at\n+  updated_at = GREATEST(item.updated_at, EXCLUDED.updated_at)\n",
        ),
        file(
            "pipeline/sql/incremental_current.sql",
            "@@ -1,3 +1,5 @@\n SELECT c.*\n FROM child c\n+WHERE c.parent_id = $1\n+ORDER BY c.start_date DESC NULLS LAST, c.updated_at DESC, c.id DESC\n+LIMIT 1\n",
        ),
        file(
            "pipeline/sync_items.py",
            "@@ -1,4 +1,14 @@\n def sync_items(rows):\n+    missing = set()\n     for row in rows:\n+        if row.get(\"parent_id\") is None:\n+            missing.add(row[\"id\"])\n+            continue\n         upsert(row)\n+\n+\n+\n+    if missing:\n+        logger.warning(\"%d items without a parent: %s\", len(missing), sorted(missing)[:20])\n+    summary[\"missing_parent\"] = len(missing)\n",
        ),
        (
            "pipeline/tests/test_items.py".into(),
            "added".into(),
            "@@ -0,0 +1,8 @@\n+def test_updated_at_still_advances_when_group_is_unchanged():\n+    assert after.updated_at > before.updated_at\n+\n+def test_tie_on_start_date_and_null_start_pick_the_same_child_in_both_modes():\n+    assert incremental(rows) == full(rows)\n".into(),
        ),
    ]
}

/// The full sync's selection, next to the incremental one.
fn full_sync() -> (String, String) {
    (
        "pipeline/sql/full_current.sql".into(),
        "SELECT DISTINCT ON (c.parent_id) c.*\nFROM child c\nORDER BY c.start_date DESC NULLS LAST, c.updated_at DESC, c.id DESC\n".into(),
    )
}

/// What code search finds for `updated_at` on the default branch.
fn search_hits() -> Vec<(String, Vec<String>)> {
    vec![
        (
            "pipeline/sql/incremental_watermark.sql".into(),
            vec!["3: SELECT MAX(updated_at) AS watermark FROM item".into()],
        ),
        (
            "search/change_key.sql".into(),
            vec!["7: GREATEST(p.updated_at, MAX(c.updated_at)) AS change_key".into()],
        ),
        (
            "pipeline/tests/test_items.py".into(),
            vec!["2: assert MAX(updated_at)".into()],
        ),
    ]
}

fn facts(files: &[(String, String, String)]) -> Facts {
    let mut f = Facts::from_diff(files);
    f.readers = vec![(
        "updated_at".into(),
        data_rules::readers("updated_at", &search_hits()),
    )];
    f.others = data_rules::other_selections(&f.selections, &[full_sync()]);
    f
}

fn plan(design_extra: &str) -> PlanResult {
    PlanResult {
        proposal: "Keep first_seen_at stable when an item is re-synced.".into(),
        design: format!(
            "Stop advancing updated_at in the upsert when only the group is unchanged.\n\
             Pick the latest child row per parent by start_date in the incremental sync.\n{design_extra}"
        ),
        tasks: "- [ ] pipeline/sql/upsert_items.sql: [modify] keep updated_at unchanged\n".into(),
        spec: String::new(),
        repo_tasks: vec![],
        fix: None,
    }
}

#[test]
fn the_plan_check_asks_for_the_readers_and_the_other_path() {
    let found = plan_judge::structural(&plan(""), &TicketHistory::default(), true);
    let points: Vec<&str> = found.iter().map(|f| f.point.as_str()).collect();
    assert!(points.contains(&"cursor_readers"), "{found:?}");
    assert!(points.contains(&"selection_parity"), "{found:?}");
    let lines: Vec<String> = found.iter().map(plan_judge::PlanFinding::line).collect();
    assert!(lines.iter().any(|l| l.contains("GREATEST(existing, new)")));
    // The plan that lists them passes.
    let done = plan(&format!(
        "\n### {}\n- pipeline/sql/incremental_watermark.sql (MAX watermark)\n- search/change_key.sql (GREATEST change key)\n### {}\n- pipeline/sql/full_current.sql orders by start_date DESC NULLS LAST, updated_at DESC, id DESC\n",
        data_rules::READERS_HEADING,
        data_rules::PARITY_HEADING
    ));
    let left = plan_judge::structural(&done, &TicketHistory::default(), true);
    assert!(
        !left
            .iter()
            .any(|f| f.point == "cursor_readers" || f.point == "selection_parity"),
        "{left:?}"
    );
}

#[test]
fn the_checklist_fails_the_shipped_change_and_passes_the_fix() {
    let f = facts(&shipped());
    let items = data_rules::items(&f, "Keep first_seen_at stable.");
    let mut end = Ending::default();
    for i in &items {
        end.said(i.line());
    }
    assert_eq!(items.len(), 3, "{items:?}");
    assert!(items.iter().all(|i| i.status == Status::Fail), "{items:?}");
    assert!(end.says("GREATEST(existing, new)"));
    assert!(end.says("pipeline/sql/incremental_watermark.sql"));
    assert!(end.says("search/change_key.sql"));
    assert!(!end.says("pipeline/tests/test_items.py"));
    assert!(end.says(
        "orders by `start_date desc` but `pipeline/sql/full_current.sql` orders by `start_date desc nulls last, updated_at desc, id desc`"
    ));
    assert!(end.says("skipped silently"));
    // The coder is sent back with every one.
    let fb = data_rules::feedback(&items);
    assert!(fb.contains(data_rules::CURSOR_ITEM));
    assert!(fb.contains(data_rules::PARITY_ITEM));
    assert!(fb.contains(data_rules::SKIP_ITEM));

    let text = format!(
        "### {}\n- pipeline/sql/incremental_watermark.sql\n- search/change_key.sql\n### {}\n- pipeline/sql/full_current.sql\n",
        data_rules::READERS_HEADING,
        data_rules::PARITY_HEADING
    );
    let items = data_rules::items(&facts(&fixed()), &text);
    assert_eq!(items.len(), 3);
    assert!(items.iter().all(|i| i.status == Status::Pass), "{items:?}");
}

#[test]
fn the_proof_check_holds_a_data_change_until_it_is_shown_on_real_data() {
    assert!(data_rules::touches_data(&shipped()));
    let ev = RunEvidence {
        sources: vec!["warehouse (database, read only)".into()],
        notes: "Evidence:\n- Confirmed: child rows carry start_date (L1)".into(),
        data_change: true,
        ..Default::default()
    };
    let gaps = proof::recorded_gaps(&ev);
    assert!(gaps.iter().any(|g| g.point == "real_data"), "{gaps:?}");
    let mut shown = ev.clone();
    shown.notes.push_str(
        "\n\nBefore and after (warehouse):\n\
         - parent 41, two children tied on start_date: before child 7 (incremental) vs child 8 (full), after child 8 in both\n\
         - parent 42, a child with no start_date: before child 9, after child 10 (same as the full sync)\n",
    );
    assert!(!proof::recorded_gaps(&shown)
        .iter()
        .any(|g| g.point == "real_data"));
}

#[test]
fn every_stage_carries_the_data_rules() {
    // The gate reads them for every ticket kind, before the feature-only
    // checks.
    let gate = fn_src(include_str!("../feature/mod.rs"), "\npub async fn gate(");
    let at = gate
        .find("data_facts(w, &files, &tree)")
        .expect("gate reads the data rules");
    assert!(
        at < gate
            .find("if feature\n            && carried.is_empty()")
            .unwrap()
    );
    // The planner and the coder are told them.
    let planner = crate::passes::feature::planner_block(crate::passes::triage::TicketIntent::Bug);
    assert!(planner.contains(data_rules::READERS_HEADING));
    assert!(planner.contains(data_rules::PARITY_HEADING));
    assert!(include_str!("../implement.rs").contains("data_rules::CODER_RULES"));
    // The plan judge and the proof judge know their points.
    assert!(plan_judge::SYSTEM.contains("cursor_readers"));
    assert!(plan_judge::SYSTEM.contains("selection_parity"));
    assert!(proof::JUDGE_SYSTEM.contains("real_data"));
}
