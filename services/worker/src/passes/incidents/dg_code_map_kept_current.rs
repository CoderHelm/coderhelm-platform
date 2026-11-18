//! dg. Every run rediscovered the repository. The planner listed the tree
//! and read the README, the manifests and CI to learn the stack and the
//! test commands; then the coder, with none of that, did it again before
//! its first edit. The repository's agent file was written once at install
//! and again only when a top-level config file changed, and neither agent
//! was given it, so the same layout was paid for twice per run and nothing
//! said how much.
//!
//! Guarantee: each repository keeps a code map (`common::code_map`) on its
//! agent file record: architecture, stack and versions, build and test
//! commands, conventions, dependencies between packages, and one section
//! per top-level area, each stamped with the commit it describes. Before
//! the planner starts, the map is brought to the run's base
//! (`passes::code_map::refresh`): the same commit or tree reuses every
//! section; otherwise only the sections the comparison's paths touch are
//! read again and rebuilt, in one model call, and a section the reply left
//! out is retried on the next run. The planner and the coder both get the
//! map as context, and the build's tool calls before its first write are
//! kept on the run with the map's commit (`exploration`).

use common::code_map::{self as cm, Changed, CodeMap};

use super::fixture;

fn tree() -> Vec<String> {
    let mut paths: Vec<String> = fixture::monorepo().tree;
    paths.extend(
        [
            "README.md",
            "package.json",
            ".github/workflows/ci.yml",
            "apps/web/package.json",
            "apps/web/src/index.ts",
            "apps/studio/package.json",
            "e2e/home.spec.ts",
        ]
        .map(String::from),
    );
    paths.sort();
    paths.dedup();
    paths
}

/// The first run: every section, stamped with its base.
fn first_run(paths: &[String]) -> CodeMap {
    let areas = cm::areas_of(paths);
    let r = cm::plan_refresh(None, "base1", "tree1", &areas, None);
    assert_eq!(r.why, "new");
    let reply: String = r
        .rebuild
        .iter()
        .map(|id| format!("=== {id} ===\nWhat `{id}` holds.\n"))
        .collect();
    let built = cm::parse_reply(&reply, &r.rebuild);
    assert_eq!(built.len(), r.rebuild.len());
    cm::apply(None, &r, &built, "base1", "tree1", "t")
}

#[test]
fn the_map_is_built_once_and_only_what_changed_is_rebuilt() {
    let paths = tree();
    let map = first_run(&paths);
    assert!(map.section("area:apps").is_some() && map.section("area:e2e").is_some());
    let areas = cm::areas_of(&paths);

    // The next run, after ten merged pull requests that only edited code:
    // nothing is read again, no model call.
    let edits: Vec<Changed> = (0..10)
        .map(|i| Changed::new(&format!("apps/web/src/blocks/card{i}.tsx"), "modified"))
        .collect();
    let r = cm::plan_refresh(Some(&map), "base2", "tree2", &areas, Some(&edits));
    assert!(r.rebuild.is_empty(), "{r:?}");
    let map = cm::apply(Some(&map), &r, &[], "base2", "tree2", "t");
    assert!(map.sections.iter().all(|s| s.commit == "base2"));

    // A new end-to-end spec and a CI change: the e2e area and the build
    // section, nothing else.
    let changes = vec![
        Changed::new("e2e/checkout.spec.ts", "added"),
        Changed::new(".github/workflows/ci.yml", "modified"),
    ];
    let r = cm::plan_refresh(Some(&map), "base3", "tree3", &areas, Some(&changes));
    assert_eq!(r.rebuild, vec!["build".to_string(), "area:e2e".to_string()]);
    assert!(r.reuse.contains(&"area:apps".to_string()));
    let built = vec![("build".to_string(), "pnpm test; CI runs e2e".to_string())];
    let map = cm::apply(Some(&map), &r, &built, "base3", "tree3", "t");
    // The e2e section the reply left out keeps its older stamp, is named
    // as possibly out of date, and is rebuilt by the next run.
    assert_eq!(map.behind(), vec!["area:e2e".to_string()]);
    assert!(cm::render(&map).contains("`area:e2e`"));
    let next = cm::plan_refresh(Some(&map), "base3", "tree3", &areas, Some(&[]));
    assert_eq!(next.rebuild, vec!["area:e2e".to_string()]);
}

#[test]
fn the_planner_and_the_coder_start_from_the_map_and_the_run_keeps_the_count() {
    let plan = include_str!("../plan.rs");
    let refresh_at = plan
        .find("super::code_map::refresh(")
        .expect("the planner brings the map to the base");
    let system_at = plan
        .find("You are the planning engineer")
        .expect("the planner's system prompt");
    assert!(refresh_at < system_at, "the map is ready before the prompt");
    assert!(
        plan.contains("{code_map}"),
        "the planner's prompt carries it"
    );
    assert!(plan.contains("record_refresh("));

    let implement = include_str!("../implement.rs");
    assert!(implement.contains("super::code_map::block("));
    assert!(implement.contains("{code_map_block}"));
    assert!(implement.contains("record_exploration("));
    assert!(implement.contains("explore.counter.call()"));
    assert!(implement.contains("explore.counter.wrote()"));

    // What a run keeps: the calls before the first write and the map's
    // commit, so runs with and without a map compare.
    let c = cm::ExploreCounter::default();
    for _ in 0..4 {
        c.call();
    }
    c.wrote();
    let e = c.exploration(Some("base3".into()));
    assert_eq!(e.calls_before_first_write, Some(3));
    let json = serde_json::to_value(&e).unwrap();
    assert_eq!(json["map_commit"], "base3");
}
