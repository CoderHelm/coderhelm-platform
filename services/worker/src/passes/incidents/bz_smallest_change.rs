//! bz. Two pull requests did far more than their tickets, and guessed
//! where they should have measured.
//!
//! A data-pipeline ticket asked to link four full-day charters to Fishing as
//! well as their own activity. The plan added a second overrides map and two
//! helpers beside the existing `CHARTER_MULTI_ACTIVITY_OVERRIDES`, and
//! switched the insert-only link sync to a replacing upsert that deleted links
//! nobody asked about. The build added warnings, summary counters and an
//! INFO-to-DEBUG change in shared code (the data rules' "report unexpected
//! data once per run" item asked for them on a four-entry fix), and a test
//! hundreds of lines long with fake hooks and an invented payload. The right
//! change: four map entries; an insert with `ON CONFLICT DO NOTHING` in the
//! three write branches; one small test.
//!
//! A web ticket embedded a vendor's booking portal in a frame. Its vendor
//! end-to-end tests were rewritten again and again on guesses (no inner
//! scroll, a click that adds to an order, invented test ids), and when the
//! criterion "no double scrollbars on desktop" met the vendor's behaviour,
//! the test was changed to expect the vendor's behaviour instead of the
//! page losing its footer and bottom padding.
//!
//! Guarantee: the plan writes Reuse, Must not change, Measure first,
//! Smallest change and Proof, and is sent back when it proposes a
//! structure parallel to an existing one, deleting or overwriting the
//! ticket doesn't ask for, logging changes, reporting, assertions about an
//! outside page with nothing measured, or a test change where a criterion
//! says the product must change (`plan_shape`). The coder's first write
//! follows a search and names what it reuses, and an edit that adds a twin
//! of an existing map or helper is refused without a reason
//! (`explore_guard`). After the build, a diff wider than its ticket goes
//! back to the coder to be made smaller, deletes where the base only added
//! with no excuse but the ticket (`minimal_change`, `habits_check`); the
//! reviewer finds the same, and a test bent to an outside page against a
//! criterion is a required finding (`review_rules`).

use crate::passes::explore_guard::ExploreGuard;
use crate::passes::habits_check::{self, PrFacts};
use crate::passes::minimal_change::{self, Kind};
use crate::passes::plan::PlanResult;
use crate::passes::plan_shape;
use crate::passes::review_rules;
use serde_json::json;

const MAP_FILE: &str = "dags/charters/activities.py";
const SYNC_FILE: &str = "dags/charters/sync_links.py";

const CHARTER_TICKET: &str = "Link four full-day charters to Fishing\n\nThese full-day charters should show under Fishing as well as their own activity: \"Dawn Reef Charter\", \"Fish and Sail\", \"Bay Explorers\", \"Tide Week\".";

const PORTAL_TICKET: &str = "Embed the booking portal on the slips page\n\nShow the vendor's booking portal in an iframe on the slips page.\n\nAcceptance criteria:\n- The portal loads inside the page\n- No double scrollbars on desktop\n";

const BASE_MAP: &str = "CHARTER_MULTI_ACTIVITY_OVERRIDES = {\n    \"harbor sampler charter\": [\"multi-activity\"],\n}\n\n\ndef get_charter_multi_activity_names(name):\n    return CHARTER_MULTI_ACTIVITY_OVERRIDES.get(name.lower(), [])\n";

const BASE_SYNC: &str = "def write_links(cur, charter_id, activities):\n    for activity_id in activities:\n        cur.execute(\n            \"INSERT INTO charter_activities (charter_id, activity_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n            (charter_id, activity_id),\n        )\n";

fn plan(design: &str, tasks: &str) -> PlanResult {
    PlanResult {
        design: design.into(),
        tasks: tasks.into(),
        ..Default::default()
    }
}

/// The plan the run wrote for the charters ticket, in its shape.
fn charters_plan_before() -> PlanResult {
    plan(
        "## Approach\nAdd `CHARTER_FISHING_OVERRIDES` and `get_charter_fishing_names` to hold the fishing links. \
         The link sync upserts each charter's links and deletes stale ones so the table matches. \
         Report skipped charters once per run with summary counters, and move the per-record INFO log to debug.\n",
        &format!("- [ ] {MAP_FILE}: add the fishing map and helper\n- [ ] {SYNC_FILE}: replace links per charter\n- [ ] tests/test_sync_links.py: cover the sync"),
    )
}

/// The small, correct plan.
fn charters_plan_after() -> PlanResult {
    plan(
        "## Reuse\n- `CHARTER_MULTI_ACTIVITY_OVERRIDES` and `get_charter_multi_activity_names` in dags/charters/activities.py\n\n\
         ## Must not change\nNo deletes: existing links stay. Logging and the other pipelines stay as they are.\n\n\
         ## Measure first\nNone: the change stays inside this repository.\n\n\
         ## Smallest change\n4 entries in `CHARTER_MULTI_ACTIVITY_OVERRIDES`; in the 3 write branches insert links for \
         `get_charter_multi_activity_names` with ON CONFLICT DO NOTHING.\n\n\
         ## Proof\nOne test: the 4 names get Fishing and their own activity, others only their own, nothing deleted.\n\n\
         ## Out of scope\nNone: the ticket names the four charters and nothing else.\n",
        &format!("- [ ] {MAP_FILE}: 4 entries\n- [ ] {SYNC_FILE}: insert the extra links\n- [ ] tests/test_activities.py: one test"),
    )
}

fn portal_plan_before() -> PlanResult {
    plan(
        "## Reuse\n- `SlipsPage` in web/src/pages/slips.tsx\n\n## Must not change\nThe header.\n\n\
         ## Measure first\nThe vendor portal renders in an iframe with no inner scroll; clicking a slip adds it to the booking total.\n\n\
         ## Smallest change\nAn iframe block on the slips page.\n\n\
         ## Proof\ne2e spec for the portal. If the frame scrolls, update the test to match the vendor.\n",
        "- [ ] web/src/pages/slips.tsx: embed\n- [ ] e2e/portal.spec.ts: spec",
    )
}

fn portal_plan_after() -> PlanResult {
    plan(
        "## Reuse\n- `SlipsPage` in web/src/pages/slips.tsx and its layout\n\n## Must not change\nThe header and other pages.\n\n\
         ## Measure first\nprobe_page on the portal at 1280x900: the embed script caps the frame at the window height below its top (640px) and the portal scrolls inside it; every slip button is disabled until booking opens; nights are `booking-night-tag-N`.\n\n\
         ## Smallest change\nAn iframe block on the slips page; drop the footer and bottom padding there so the frame fills the window, as the vendor's embed docs recommend.\n\n\
         ## Proof\nOne e2e spec from the probe record: the page itself has no scrollbar at 1280x900, the frame is 640px, night tags are listed.\n\n\
         ## Out of scope\n- Booking after it opens: decided with the default: the spec checks only what the probe saw.\n",
        "- [ ] web/src/pages/slips.tsx: embed, no footer\n- [ ] e2e/portal.spec.ts: spec from the probe",
    )
}

#[test]
fn the_plans_are_sent_back_until_they_are_the_small_change() {
    let existing = vec!["CHARTER_MULTI_ACTIVITY_OVERRIDES".to_string()];
    // Before: the charters plan has no sections, a parallel map, a replacing
    // sync, a log change and reporting.
    let g = plan_shape::plan_gaps(&charters_plan_before(), CHARTER_TICKET, false, &existing);
    let all = g.join("\n");
    assert!(all.contains("lacks `## Reuse`"), "{all}");
    assert!(
        all.contains(
            "`CHARTER_FISHING_OVERRIDES` next to the existing `CHARTER_MULTI_ACTIVITY_OVERRIDES`"
        ),
        "{all}"
    );
    assert!(all.contains("delete or overwrite"), "{all}");
    assert!(all.contains("changes logging"), "{all}");
    assert!(all.contains("adds reporting"), "{all}");
    // After: nothing to send back.
    assert!(
        plan_shape::plan_gaps(&charters_plan_after(), CHARTER_TICKET, false, &existing).is_empty()
    );
    // The portal plan guessed the outside page and planned to bend the
    // test to the vendor against a criterion.
    let g = plan_shape::plan_gaps(&portal_plan_before(), PORTAL_TICKET, false, &[]);
    let all = g.join("\n");
    assert!(all.contains("plans no measurement"), "{all}");
    assert!(
        all.contains("product change that meets the criterion first"),
        "{all}"
    );
    assert!(plan_shape::plan_gaps(&portal_plan_after(), PORTAL_TICKET, false, &[]).is_empty());
    // The planner is told the sections, and the ticket's own words are
    // searched before it plans.
    assert!(plan_shape::PLANNER.contains("## Must not change"));
    let terms = plan_shape::ticket_terms("Link four full-day charters to Fishing", CHARTER_TICKET);
    assert!(terms.contains(&"Fishing".to_string()), "{terms:?}");
    assert!(
        terms.contains(&"Dawn Reef Charter".to_string()),
        "{terms:?}"
    );
}

#[test]
fn the_coder_searches_first_and_never_adds_a_twin() {
    let g = ExploreGuard::for_build();
    let twin = format!(
        "{BASE_MAP}\nCHARTER_FISHING_OVERRIDES = {{\n    \"dawn reef charter\": [\"fishing\"],\n}}\n"
    );
    // The run's first write came before any search.
    let first = json!({"path": MAP_FILE, "edits": []});
    assert!(g.check_first_write(&first).is_err());
    g.saw("search_code", &json!({"query": "Fishing"}));
    let named = json!({"path": MAP_FILE, "reuses": "`CHARTER_MULTI_ACTIVITY_OVERRIDES` in dags/charters/activities.py"});
    assert!(g.check_first_write(&named).is_ok());
    // A second map beside the first is refused; entries in it are not.
    assert!(g.check_edit(MAP_FILE, BASE_MAP, &twin, &named).is_err());
    let entries = BASE_MAP.replace(
        "    \"harbor sampler charter\": [\"multi-activity\"],\n",
        "    \"harbor sampler charter\": [\"multi-activity\"],\n    \"dawn reef charter\": [\"fishing\", \"sailing\"],\n",
    );
    assert!(g.check_edit(MAP_FILE, BASE_MAP, &entries, &named).is_ok());
}

fn diff_before() -> Vec<(String, String, String)> {
    let map = "@@ -1,3 +1,14 @@\n CHARTER_MULTI_ACTIVITY_OVERRIDES = {\n+CHARTER_FISHING_OVERRIDES = {\n+    \"dawn reef charter\": [\"fishing\"],\n+}\n+def get_charter_fishing_names(name):\n+    return CHARTER_FISHING_OVERRIDES.get(name.lower(), [])\n";
    let sync = "@@ -1,6 +1,20 @@\n def write_links(cur, charter_id, activities):\n-    for activity_id in activities:\n+    cur.execute(\"DELETE FROM charter_activities WHERE charter_id = %s AND NOT (activity_id = ANY(%s))\", (charter_id, activities))\n+    for activity_id in activities:\n         cur.execute(\n-            \"INSERT INTO charter_activities (charter_id, activity_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n+            \"INSERT INTO charter_activities (charter_id, activity_id) VALUES (%s, %s) ON CONFLICT (charter_id, activity_id) DO UPDATE SET activity_id = EXCLUDED.activity_id\",\n+        linked += 1\n+        skipped += 1\n+        log.warning(\"skipped charters: %s\", skipped_ids)\n";
    let log = "@@ -1,3 +1,3 @@\n-logger.setLevel(logging.INFO)\n+logger.setLevel(logging.DEBUG)\n";
    let test: String = std::iter::once("+class FakeDbHook:\n".to_string())
        .chain((0..16).map(|i| format!("+    \"charter_field_{i}\": \"value\",\n")))
        .chain((0..420).map(|i| format!("+    assert rows[{i}]\n")))
        .collect();
    vec![
        (MAP_FILE.into(), "modified".into(), map.into()),
        (SYNC_FILE.into(), "modified".into(), sync.into()),
        ("dags/common/logs.py".into(), "modified".into(), log.into()),
        ("tests/test_sync_links.py".into(), "added".into(), test),
    ]
}

fn diff_after() -> Vec<(String, String, String)> {
    let map = "@@ -1,3 +1,7 @@\n CHARTER_MULTI_ACTIVITY_OVERRIDES = {\n     \"harbor sampler charter\": [\"multi-activity\"],\n+    \"dawn reef charter\": [\"fishing\", \"sailing\"],\n+    \"fish and sail\": [\"fishing\", \"sailing\"],\n+    \"bay explorers\": [\"fishing\", \"multi-activity\"],\n+    \"tide week\": [\"fishing\", \"multi-activity\"],\n }\n";
    let sync = "@@ -1,6 +1,11 @@\n def write_links(cur, charter_id, activities):\n-    for activity_id in activities:\n+    for activity_id in activities + extra_activities(charter_name):\n         cur.execute(\n             \"INSERT INTO charter_activities (charter_id, activity_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n";
    let test: String = (0..24)
        .map(|i| format!("+    assert links[{i}]\n"))
        .collect();
    vec![
        (MAP_FILE.into(), "modified".into(), map.into()),
        (SYNC_FILE.into(), "modified".into(), sync.into()),
        ("tests/test_activities.py".into(), "added".into(), test),
    ]
}

#[test]
fn the_wider_diff_goes_back_and_the_small_one_passes() {
    let base = vec![
        (MAP_FILE.to_string(), BASE_MAP.to_string()),
        (SYNC_FILE.to_string(), BASE_SYNC.to_string()),
    ];
    let found = minimal_change::check(&diff_before(), &base, CHARTER_TICKET, false);
    let kinds: Vec<Kind> = found.iter().map(|w| w.kind).collect();
    for k in [
        Kind::Parallel,
        Kind::Replacing,
        Kind::LogLevel,
        Kind::Reporting,
        Kind::TestSize,
        Kind::InventedPayload,
    ] {
        assert!(kinds.contains(&k), "{k:?} not found in {found:?}");
    }
    // The habits check's fix round gets each as a line to make smaller;
    // a reason excuses neither the deletes nor the invented payload.
    let notes = format!(
        "Wider than the ticket: {SYNC_FILE}: keeps the table in step\nWider than the ticket: tests/test_sync_links.py: thorough"
    );
    let facts = PrFacts {
        wider: minimal_change::standing(&found, &notes)
            .into_iter()
            .cloned()
            .collect(),
        ..Default::default()
    };
    let lines = habits_check::blocking(&facts, &PlanResult::default());
    assert!(
        lines
            .iter()
            .any(|l| l.contains("deletes or overwrites stored data")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("payload nobody observed")),
        "{lines:?}"
    );
    assert!(habits_check::fix_feedback(&lines).contains("make it smaller"));
    let open = habits_check::open_findings(&facts, &PlanResult::default());
    assert!(open
        .iter()
        .any(|f| f.point == "Deletes or overwrites where the code only added"));
    // The reviewer finds the same; the deletes block by rule.
    let review = review_rules::wider_findings(&found);
    let deletes = review
        .iter()
        .find(|f| f.title == "Deletes or overwrites where the code only added")
        .unwrap();
    assert_eq!(
        review_rules::mandatory(deletes),
        Some(review_rules::REQUIRED_WHY)
    );
    // The right change passes every check.
    assert!(
        minimal_change::check(&diff_after(), &base, CHARTER_TICKET, false).is_empty(),
        "{:?}",
        minimal_change::check(&diff_after(), &base, CHARTER_TICKET, false)
    );
}

#[test]
fn a_test_bent_to_the_vendor_against_a_criterion_is_a_required_finding() {
    let patch = "@@ -10,4 +10,4 @@\n   const frame = page.frameLocator('iframe');\n-  expect(await pageScrolls(page)).toBe(false);\n+  expect(await pageScrolls(page)).toBe(true);\n";
    let criteria = crate::passes::stop_judge::acceptance(PORTAL_TICKET);
    let flips = minimal_change::vendor_flips("e2e/portal.spec.ts", patch, true, &criteria);
    assert_eq!(flips.len(), 1);
    assert_eq!(flips[0].criterion, "No double scrollbars on desktop");
    let f = review_rules::vendor_flip_findings(&flips);
    assert_eq!(
        review_rules::mandatory(&f[0]),
        Some(review_rules::REQUIRED_WHY)
    );
    assert!(f[0].body.contains("Change the product side first"));
}

/// The shape the data-pipeline run really had: its plan named the right
/// map, then reused the existing multi-activity link helper, which deletes
/// every link it isn't given, on a sync path that only inserted. The run
/// deleted links nobody asked about. The helper's body says what it does,
/// so the plan is sent back; the plan that inserts with `ON CONFLICT DO
/// NOTHING` and reuses only the read-only lookup passes.
#[test]
fn a_replacing_helper_reused_on_an_insert_only_path_is_sent_back() {
    const HELPERS: &str = "CHARTER_MULTI_ACTIVITY_OVERRIDES = {\n    \"harbor sampler charter\": [\"multi-activity\"],\n}\n\n\ndef get_charter_multi_activity_names(name):\n    return CHARTER_MULTI_ACTIVITY_OVERRIDES.get(name.lower(), [])\n\n\ndef upsert_multi_activity_links(cur, group_id, activity_ids):\n    cur.execute(\"DELETE FROM charter_activities WHERE charter_id = %s AND NOT (activity_id = ANY(%s))\", (group_id, activity_ids))\n    for activity_id in activity_ids:\n        cur.execute(\"INSERT INTO charter_activities VALUES (%s, %s) ON CONFLICT DO NOTHING\", (group_id, activity_id))\n";
    let reuse_replacing = plan(
        "## Reuse\n- `CHARTER_MULTI_ACTIVITY_OVERRIDES` and `upsert_multi_activity_links` in dags/charters/activities.py (the other two sources already use them)\n\n\
         ## Must not change\nSingle-activity charters.\n\n## Measure first\nNone: the change stays inside this repository.\n\n\
         ## Smallest change\n4 entries in `CHARTER_MULTI_ACTIVITY_OVERRIDES`; call `upsert_multi_activity_links` in the 3 write branches.\n\n\
         ## Proof\nOne test of the links per charter.\n",
        &format!("- [ ] {MAP_FILE}: 4 entries\n- [ ] {SYNC_FILE}: link the extra activities\n- [ ] tests/test_activities.py: one test"),
    );
    let sources = |p: &PlanResult| -> Vec<(String, String, String)> {
        plan_shape::reused_functions(p)
            .into_iter()
            .map(|n| (n, MAP_FILE.to_string(), HELPERS.to_string()))
            .collect()
    };
    let planned = vec![
        (MAP_FILE.to_string(), HELPERS.to_string()),
        (SYNC_FILE.to_string(), BASE_SYNC.to_string()),
    ];
    let facts = plan_shape::write_facts(&sources(&reuse_replacing), &planned);
    assert!(facts.path_inserts_only, "{facts:?}");
    let g = plan_shape::write_gaps(&reuse_replacing, CHARTER_TICKET, &facts);
    assert!(
        g.iter().any(
            |l| l.contains("`upsert_multi_activity_links` without what it does to stored rows")
        ),
        "{g:?}"
    );
    assert!(
        g.iter()
            .any(|l| l.contains("on a path that only inserts today")),
        "{g:?}"
    );
    // Saying it replaces isn't enough when the ticket asks for no
    // replacement; a ticket that asks to replace the links may.
    let said = plan(
        &reuse_replacing.design.replace(
            "(the other two sources already use them)",
            "(it replaces a charter's links: DELETE then insert)",
        ),
        &reuse_replacing.tasks,
    );
    let g = plan_shape::write_gaps(&said, CHARTER_TICKET, &facts);
    assert_eq!(g.len(), 1, "{g:?}");
    assert!(
        plan_shape::write_gaps(&said, "Replace each charter's activity links", &facts).is_empty()
    );
    // The small plan reuses only the map and the read-only lookup.
    let small = charters_plan_after();
    let facts = plan_shape::write_facts(&sources(&small), &planned);
    assert!(plan_shape::write_gaps(&small, CHARTER_TICKET, &facts).is_empty());
}
