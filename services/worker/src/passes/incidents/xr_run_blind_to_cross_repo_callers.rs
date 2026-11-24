//! xr. Runs planned and reviewed one repository at a time. A ticket asked
//! to rename the booking endpoint in the booking service; the planner read
//! only that repository, planned the route and its test, and the reviewer
//! passed the pull request. The website and the phone app both call that
//! endpoint, behind the slip page and the slip screen: the app map Chat and
//! `/mcp` read had them, with their lines, and no run asked it.
//!
//! Guarantee: every pass that reads code (planner, coder and the rounds
//! through it, reviewer) is offered the map's lookups, `map_impact` among
//! them, beside the code graph's. A plan whose files reach other
//! repositories on the map goes back once until its design names them under
//! `## Affected elsewhere`, and a citation there that isn't a real place on
//! the map is left out. The reviewer is shown each endpoint the diff changes
//! that other repositories call, with every caller's file:line. With no map
//! nothing is offered and nothing is said.

use crate::passes::code_graph::{offer, Avail, RepoGraphs, Role};
use crate::passes::run_map::{planned, RunMap, AFFECTED_HEADING};

fn harbor() -> serde_json::Value {
    let mut g: serde_json::Value = serde_json::from_str(include_str!(
        "../../../floorplan/fixtures/harbor-graph.json"
    ))
    .unwrap();
    common::app_map::stamp_repos(
        &mut g,
        &common::app_map::folders(&[
            "harbor/harbor-web".into(),
            "harbor/harbor-api".into(),
            "harbor/harbor-mobile".into(),
            "harbor/harbor-infra".into(),
        ]),
    );
    g
}

const TASKS: &str =
    "- [ ] src/bookings.js: [modify] Rename the booking endpoint to /:id/reservations\n";

#[test]
fn the_planner_names_the_other_repositories_the_endpoint_reaches() {
    let map = RunMap::of(harbor(), "harbor/harbor-api").expect("the map holds the repository");
    // The planner is offered map_impact beside the graph's lookups.
    let graphs = RepoGraphs::with(
        "t",
        1,
        "harbor/harbor-api",
        Avail::Off,
        Vec::new(),
        "planner",
        0,
    )
    .with_map(RunMap::of(harbor(), "harbor/harbor-api"));
    let mut tools = Vec::new();
    let mut system = String::new();
    assert!(offer(&graphs, Role::Planner, &mut tools, &mut system));
    assert!(tools.iter().any(|t| t.name == "map_impact"));
    assert!(system.contains(AFFECTED_HEADING));

    // The plan as it was: the route and nothing else. It goes back with the
    // callers the map has.
    let files = planned("harbor/harbor-api", TASKS, &[]);
    let gaps = map.plan_gaps(&files, "## Approach\nRename the route.\n");
    assert_eq!(gaps.len(), 1);
    for at in [
        "harbor-web/lib/slips.ts:2",
        "harbor-mobile/app/screens/SlipDetail.tsx:6",
    ] {
        assert!(gaps[0].contains(at), "{}", gaps[0]);
    }

    // The revision names them; a made-up line is left out.
    let design = format!(
        "## Approach\nRename the route.\n\n{AFFECTED_HEADING}\n\
         - harbor-web calls POST /slips/:id/bookings at harbor-web/lib/slips.ts:2 (Slip page)\n\
         - harbor-mobile calls it at harbor-mobile/app/screens/SlipDetail.tsx:6 (SlipDetail, signed in)\n\
         - harbor-mobile also at harbor-mobile/app/lib/api.ts:400\n"
    );
    assert!(map.plan_gaps(&files, &design).is_empty());
    let (kept, dropped) = map.checked_design(&design);
    assert_eq!(dropped, 1);
    assert!(kept.contains("harbor-web/lib/slips.ts:2"));
    assert!(!kept.contains("api.ts:400"));
}

#[test]
fn the_reviewer_is_shown_the_endpoints_other_repositories_call() {
    let map = RunMap::of(harbor(), "harbor/harbor-api").unwrap();
    let patch = "@@ -4,3 +4,3 @@\n const router = express.Router();\n-router.post('/:id/bookings', async (req, res) => {\n+router.post('/:id/reservations', async (req, res) => {\n   await charge(req.params.id);\n";
    let section = map.review_section(&[("src/bookings.js".into(), patch.into())]);
    assert!(
        section.contains("POST /slips/:id/bookings (harbor-api/src/bookings.js:5)"),
        "{section}"
    );
    assert!(section.contains("called from harbor-web/lib/slips.ts:2 (harbor-web)"));
    assert!(
        section.contains("called from harbor-mobile/app/screens/SlipDetail.tsx:6 (harbor-mobile)")
    );
    assert!(section.contains("breaking change"));
}

#[test]
fn without_a_map_nothing_is_offered_or_said() {
    assert!(RunMap::of(harbor(), "harbor/not-on-the-map").is_none());
    let graphs = RepoGraphs::with(
        "t",
        1,
        "harbor/harbor-api",
        Avail::Off,
        Vec::new(),
        "coder",
        0,
    );
    let mut tools = Vec::new();
    let mut system = String::new();
    assert!(!offer(&graphs, Role::Coder, &mut tools, &mut system));
    assert!(tools.is_empty() && system.is_empty());
}
