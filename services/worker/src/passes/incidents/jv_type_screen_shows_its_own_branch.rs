//! jv. A website served most pages through one catch-all route, and a
//! shared renderer file drew each kind of page by its type. The map listed
//! one screen per type, but each one opened on the shared renderer file
//! alone: the screen itself and nothing else, never the blocks, the data
//! fetch or the requests of that kind of page. Its journeys stood on the
//! same shared file, so every journey through those pages looked alike.
//!
//! Guarantee: a screen a route picks by type stands on its own renderer
//! (the map's symbol for it, `s:<file>#<index>`), and what is behind it is
//! what that renderer's branch draws, its local helpers included, plus what
//! the resolver's function for that type loads: the query, the client that
//! fetches it and the outside service. The map's lookups describe that
//! symbol and count it as its file for impact. Journey candidates for the
//! screen stand on the renderer's card and reach what its loader fetches,
//! and a stored journey's steps inside that renderer move onto its card on
//! the next build, with no new trace.

use serde_json::Value;

use super::fixture::Ending;

fn library() -> Value {
    let mut g: Value = serde_json::from_str(include_str!(
        "../../../floorplan/fixtures/library-graph.json"
    ))
    .unwrap();
    common::app_map::stamp_repos(
        &mut g,
        &common::app_map::folders(&["library/library-web".to_string()]),
    );
    g
}

#[test]
fn a_page_picked_by_type_shows_its_own_renderer_and_what_it_reaches() {
    let g = library();
    let typed: Vec<&Value> = g["screens"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["fw"] == "dispatch")
        .collect();
    assert_eq!(typed.len(), 5);
    let mut end = Ending::default();
    for s in &typed {
        let id = s["id"].as_str().unwrap();
        let v = common::app_map::screen_view(&g, id).expect("the screen is on the map");
        let own = &v["behind"][0];
        // the screen itself is its renderer, described as that symbol
        assert_eq!(own["step"], "screen", "{id}");
        assert_eq!(own["symbol"], s["nav"]["renderer"], "{id}");
        assert_eq!(own["path"], "library-web/lib/renderers.tsx", "{id}");
        end.said(own["id"].as_str().unwrap());
    }
    // the shared renderer file is never what a page picked by type stands on
    end.never_says("f:library-web/lib/renderers.tsx");
    // each page's own form, and not another's
    let signup = common::app_map::screen_view(&g, "/members/join").unwrap();
    let ids: Vec<&str> = signup["behind"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|b| b["id"].as_str())
        .collect();
    assert!(ids.contains(&"f:library-web/components/SignupForm.tsx"));
    assert!(!ids.contains(&"f:library-web/components/CheckoutForm.tsx"));
    // a change to the renderer file reaches every page it draws
    let imp = common::app_map::impact(&g, "library-web/lib/renderers.tsx").unwrap();
    let hit: Vec<&str> = imp["screens"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    for n in ["Member signup", "Member checkout", "Notice page"] {
        assert!(hit.contains(&n), "{n} in {hit:?}");
    }
    // The sandbox builds it this way: the renderer and the type's loader
    // walked from their own code, and journeys standing on the renderer.
    let screens = include_str!("../../../floorplan/coderhelm/screens.mjs");
    assert!(screens.contains("{ loader: h.nav.loader, route: h.nav.dispatcher }"));
    assert!(screens
        .contains("push(symRef(file, symbol) || 'f:' + file, 'The screen itself', 'screen');"));
    let flows = include_str!("../../../floorplan/coderhelm/flows.mjs");
    assert!(flows.contains("'the page loads its content from it'"));
    assert!(flows.contains("cand = { view: spanView(c, ev) || st.view };"));
}
