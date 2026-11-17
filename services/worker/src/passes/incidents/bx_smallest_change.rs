//! bx. Two pull requests did far more than their tickets, and guessed
//! where they should have measured.
//!
//! A data-pipeline ticket asked to link four full-day camps to Swimming as
//! well as their own sport. The plan added a second overrides map and two
//! helpers beside the existing `CAMP_MULTI_SPORT_OVERRIDES`, and switched
//! the insert-only link sync to a replacing upsert that deleted links
//! nobody asked about. The build added warnings, three summary counters
//! and an INFO-to-DEBUG change in shared code (the data rules' "report
//! unexpected data once per run" item asked for them on a four-entry fix),
//! and a 432-line test with fake hooks and an invented payload. The right
//! change: four map entries; an insert with `ON CONFLICT DO NOTHING` in the
//! three write branches; one small test.
//!
//! A web ticket embedded a vendor's booking portal in a frame. Its vendor
//! end-to-end tests were rewritten three times on guesses (no inner
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

const MAP_FILE: &str = "dags/camps/sports.py";
const SYNC_FILE: &str = "dags/camps/sync_links.py";

const CAMP_TICKET: &str = "Link four full-day camps to Swimming\n\nThese full-day camps should show under Swimming as well as their own sport: \"Aqua Day Camp\", \"Swim and Sail\", \"Pool Explorers\", \"Splash Week\".";

const PORTAL_TICKET: &str = "Embed the booking portal on the camps page\n\nShow the vendor's booking portal in an iframe on the camps page.\n\nAcceptance criteria:\n- The portal loads inside the page\n- No double scrollbars on desktop\n";

const BASE_MAP: &str = "CAMP_MULTI_SPORT_OVERRIDES = {\n    \"sports academy break camp\": [\"multi-sport\"],\n}\n\n\ndef get_camp_multi_sport_names(name):\n    return CAMP_MULTI_SPORT_OVERRIDES.get(name.lower(), [])\n";

const BASE_SYNC: &str = "def write_links(cur, camp_id, sports):\n    for sport_id in sports:\n        cur.execute(\n            \"INSERT INTO camp_sports (camp_id, sport_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n            (camp_id, sport_id),\n        )\n";

fn plan(design: &str, tasks: &str) -> PlanResult {
    PlanResult {
        design: design.into(),
        tasks: tasks.into(),
        ..Default::default()
    }
}

/// The plan the run wrote for the camps ticket, in its shape.
fn camps_plan_before() -> PlanResult {
    plan(
        "## Approach\nAdd `CAMP_SWIMMING_OVERRIDES` and `get_camp_swimming_names` to hold the swimming links. \
         The link sync upserts each camp's links and deletes stale ones so the table matches. \
         Report skipped camps once per run with summary counters, and move the per-record INFO log to debug.\n",
        &format!("- [ ] {MAP_FILE}: add the swimming map and helper\n- [ ] {SYNC_FILE}: replace links per camp\n- [ ] tests/test_sync_links.py: cover the sync"),
    )
}

/// The small, correct plan.
fn camps_plan_after() -> PlanResult {
    plan(
        "## Reuse\n- `CAMP_MULTI_SPORT_OVERRIDES` and `get_camp_multi_sport_names` in dags/camps/sports.py\n\n\
         ## Must not change\nNo deletes: existing links stay. Logging and the other pipelines stay as they are.\n\n\
         ## Measure first\nNone: the change stays inside this repository.\n\n\
         ## Smallest change\n4 entries in `CAMP_MULTI_SPORT_OVERRIDES`; in the 3 write branches insert links for \
         `get_camp_multi_sport_names` with ON CONFLICT DO NOTHING.\n\n\
         ## Proof\nOne test: the 4 names get Swimming and their own sport, others only their own, nothing deleted.\n",
        &format!("- [ ] {MAP_FILE}: 4 entries\n- [ ] {SYNC_FILE}: insert the extra links\n- [ ] tests/test_sports.py: one test"),
    )
}

fn portal_plan_before() -> PlanResult {
    plan(
        "## Reuse\n- `CampsPage` in web/src/pages/camps.tsx\n\n## Must not change\nThe header.\n\n\
         ## Measure first\nThe vendor portal renders in an iframe with no inner scroll; clicking a camp adds it to the order summary.\n\n\
         ## Smallest change\nAn iframe block on the camps page.\n\n\
         ## Proof\ne2e spec for the portal. If the frame scrolls, update the test to match the vendor.\n",
        "- [ ] web/src/pages/camps.tsx: embed\n- [ ] e2e/portal.spec.ts: spec",
    )
}

fn portal_plan_after() -> PlanResult {
    plan(
        "## Reuse\n- `CampsPage` in web/src/pages/camps.tsx and its layout\n\n## Must not change\nThe header and other pages.\n\n\
         ## Measure first\nprobe_page on the portal at 1280x900: the embed script caps the frame at the window height below its top (663px) and the portal scrolls inside it; every camp button is disabled until registration opens; weeks are `program-portals-segment-tag-guest-N`.\n\n\
         ## Smallest change\nAn iframe block on the camps page; drop the footer and bottom padding there so the frame fills the window, as the vendor's embed docs recommend.\n\n\
         ## Proof\nOne e2e spec from the probe record: the page itself has no scrollbar at 1280x900, the frame is 663px, week tags are listed.\n",
        "- [ ] web/src/pages/camps.tsx: embed, no footer\n- [ ] e2e/portal.spec.ts: spec from the probe",
    )
}

#[test]
fn the_plans_are_sent_back_until_they_are_the_small_change() {
    let existing = vec!["CAMP_MULTI_SPORT_OVERRIDES".to_string()];
    // Before: the camps plan has no sections, a parallel map, a replacing
    // sync, a log change and reporting.
    let g = plan_shape::plan_gaps(&camps_plan_before(), CAMP_TICKET, false, &existing);
    let all = g.join("\n");
    assert!(all.contains("lacks `## Reuse`"), "{all}");
    assert!(
        all.contains("`CAMP_SWIMMING_OVERRIDES` next to the existing `CAMP_MULTI_SPORT_OVERRIDES`"),
        "{all}"
    );
    assert!(all.contains("delete or overwrite"), "{all}");
    assert!(all.contains("changes logging"), "{all}");
    assert!(all.contains("adds reporting"), "{all}");
    // After: nothing to send back.
    assert!(plan_shape::plan_gaps(&camps_plan_after(), CAMP_TICKET, false, &existing).is_empty());
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
    let terms = plan_shape::ticket_terms("Link four full-day camps to Swimming", CAMP_TICKET);
    assert!(terms.contains(&"Swimming".to_string()), "{terms:?}");
    assert!(terms.contains(&"Aqua Day Camp".to_string()), "{terms:?}");
}

#[test]
fn the_coder_searches_first_and_never_adds_a_twin() {
    let g = ExploreGuard::for_build();
    let twin = format!(
        "{BASE_MAP}\nCAMP_SWIMMING_OVERRIDES = {{\n    \"aqua day camp\": [\"swimming\"],\n}}\n"
    );
    // The run's first write came before any search.
    let first = json!({"path": MAP_FILE, "edits": []});
    assert!(g.check_first_write(&first).is_err());
    g.saw("search_code", &json!({"query": "Swimming"}));
    let named =
        json!({"path": MAP_FILE, "reuses": "`CAMP_MULTI_SPORT_OVERRIDES` in dags/camps/sports.py"});
    assert!(g.check_first_write(&named).is_ok());
    // A second map beside the first is refused; entries in it are not.
    assert!(g.check_edit(MAP_FILE, BASE_MAP, &twin, &named).is_err());
    let entries = BASE_MAP.replace(
        "    \"sports academy break camp\": [\"multi-sport\"],\n",
        "    \"sports academy break camp\": [\"multi-sport\"],\n    \"aqua day camp\": [\"swimming\", \"sailing\"],\n",
    );
    assert!(g.check_edit(MAP_FILE, BASE_MAP, &entries, &named).is_ok());
}

fn diff_before() -> Vec<(String, String, String)> {
    let map = "@@ -1,3 +1,14 @@\n CAMP_MULTI_SPORT_OVERRIDES = {\n+CAMP_SWIMMING_OVERRIDES = {\n+    \"aqua day camp\": [\"swimming\"],\n+}\n+def get_camp_swimming_names(name):\n+    return CAMP_SWIMMING_OVERRIDES.get(name.lower(), [])\n";
    let sync = "@@ -1,6 +1,20 @@\n def write_links(cur, camp_id, sports):\n-    for sport_id in sports:\n+    cur.execute(\"DELETE FROM camp_sports WHERE camp_id = %s AND NOT (sport_id = ANY(%s))\", (camp_id, sports))\n+    for sport_id in sports:\n         cur.execute(\n-            \"INSERT INTO camp_sports (camp_id, sport_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n+            \"INSERT INTO camp_sports (camp_id, sport_id) VALUES (%s, %s) ON CONFLICT (camp_id, sport_id) DO UPDATE SET sport_id = EXCLUDED.sport_id\",\n+        linked += 1\n+        skipped += 1\n+        log.warning(\"skipped camps: %s\", skipped_ids)\n";
    let log = "@@ -1,3 +1,3 @@\n-logger.setLevel(logging.INFO)\n+logger.setLevel(logging.DEBUG)\n";
    let test: String = std::iter::once("+class FakePostgresHook:\n".to_string())
        .chain((0..16).map(|i| format!("+    \"camp_field_{i}\": \"value\",\n")))
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
    let map = "@@ -1,3 +1,7 @@\n CAMP_MULTI_SPORT_OVERRIDES = {\n     \"sports academy break camp\": [\"multi-sport\"],\n+    \"aqua day camp\": [\"swimming\", \"sailing\"],\n+    \"swim and sail\": [\"swimming\", \"sailing\"],\n+    \"pool explorers\": [\"swimming\", \"multi-sport\"],\n+    \"splash week\": [\"swimming\", \"multi-sport\"],\n }\n";
    let sync = "@@ -1,6 +1,11 @@\n def write_links(cur, camp_id, sports):\n-    for sport_id in sports:\n+    for sport_id in sports + extra_sports(camp_name):\n         cur.execute(\n             \"INSERT INTO camp_sports (camp_id, sport_id) VALUES (%s, %s) ON CONFLICT DO NOTHING\",\n";
    let test: String = (0..24)
        .map(|i| format!("+    assert links[{i}]\n"))
        .collect();
    vec![
        (MAP_FILE.into(), "modified".into(), map.into()),
        (SYNC_FILE.into(), "modified".into(), sync.into()),
        ("tests/test_sports.py".into(), "added".into(), test),
    ]
}

#[test]
fn the_wider_diff_goes_back_and_the_small_one_passes() {
    let base = vec![
        (MAP_FILE.to_string(), BASE_MAP.to_string()),
        (SYNC_FILE.to_string(), BASE_SYNC.to_string()),
    ];
    let found = minimal_change::check(&diff_before(), &base, CAMP_TICKET, false);
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
        minimal_change::check(&diff_after(), &base, CAMP_TICKET, false).is_empty(),
        "{:?}",
        minimal_change::check(&diff_after(), &base, CAMP_TICKET, false)
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
/// map, then reused the existing multi-sport link helper, which deletes
/// every link it isn't given, on a sync path that only inserted. The run
/// deleted links nobody asked about. The helper's body says what it does,
/// so the plan is sent back; the plan that inserts with `ON CONFLICT DO
/// NOTHING` and reuses only the read-only lookup passes.
#[test]
fn a_replacing_helper_reused_on_an_insert_only_path_is_sent_back() {
    const HELPERS: &str = "CAMP_MULTI_SPORT_OVERRIDES = {\n    \"sports academy break camp\": [\"multi-sport\"],\n}\n\n\ndef get_camp_multi_sport_names(name):\n    return CAMP_MULTI_SPORT_OVERRIDES.get(name.lower(), [])\n\n\ndef upsert_multi_sport_links(cur, group_id, sport_ids):\n    cur.execute(\"DELETE FROM camp_sports WHERE camp_id = %s AND NOT (sport_id = ANY(%s))\", (group_id, sport_ids))\n    for sport_id in sport_ids:\n        cur.execute(\"INSERT INTO camp_sports VALUES (%s, %s) ON CONFLICT DO NOTHING\", (group_id, sport_id))\n";
    let reuse_replacing = plan(
        "## Reuse\n- `CAMP_MULTI_SPORT_OVERRIDES` and `upsert_multi_sport_links` in dags/camps/sports.py (the other two sources already use them)\n\n\
         ## Must not change\nSingle-sport camps.\n\n## Measure first\nNone: the change stays inside this repository.\n\n\
         ## Smallest change\n4 entries in `CAMP_MULTI_SPORT_OVERRIDES`; call `upsert_multi_sport_links` in the 3 write branches.\n\n\
         ## Proof\nOne test of the links per camp.\n",
        &format!("- [ ] {MAP_FILE}: 4 entries\n- [ ] {SYNC_FILE}: link the extra sports\n- [ ] tests/test_sports.py: one test"),
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
    let g = plan_shape::write_gaps(&reuse_replacing, CAMP_TICKET, &facts);
    assert!(
        g.iter()
            .any(|l| l.contains("`upsert_multi_sport_links` without what it does to stored rows")),
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
            "(it replaces a camp's links: DELETE then insert)",
        ),
        &reuse_replacing.tasks,
    );
    let g = plan_shape::write_gaps(&said, CAMP_TICKET, &facts);
    assert_eq!(g.len(), 1, "{g:?}");
    assert!(plan_shape::write_gaps(&said, "Replace each camp's sport links", &facts).is_empty());
    // The small plan reuses only the map and the read-only lookup.
    let small = camps_plan_after();
    let facts = plan_shape::write_facts(&sources(&small), &planned);
    assert!(plan_shape::write_gaps(&small, CAMP_TICKET, &facts).is_empty());
}
