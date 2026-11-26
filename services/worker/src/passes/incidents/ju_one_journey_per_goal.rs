//! ju. After a catch-all route was split into one screen per type, a
//! stored map could hold the same journey more than once: under numbered
//! ids with different step counts, or once from the catch-all route and
//! again from the renderer that replaced it. A retrace from a new or split screen
//! added its journey without retiring the one it replaced, and
//! near-identical journeys from different entries piled up.
//!
//! Guarantee: every journey names the entries it stands for (`from`), and
//! an entry's retrace replaces every journey from it, including one moved
//! there from a split catch-all (`app_map_flows::merge`). A journey from a
//! catch-all route names the screens it was split into (`split`), and a
//! trace of one of them with its goal retires it. Near-identical journeys
//! are merged into the most complete one, journeys that share a title but
//! differ are titled by how they differ, and no id is a numbered copy of a
//! title (`dedupe`, the same rules in the sandbox, `flows.mjs`, which
//! cleans a stored map on the next build with no model call).

use crate::passes::app_map_flows as fl;
use serde_json::{json, Value};
use std::collections::BTreeSet;

use super::fixture::Ending;

fn step(key: &str) -> Value {
    json!({"key": key, "kind": "screen", "view": "f:harbor-web/x.tsx", "note": "A step.", "evidence": [{"file": "harbor-web/x.tsx", "line": 2, "token": "x"}]})
}

fn journey(id: &str, title: &str, entry: &str, keys: &[&str]) -> Value {
    json!({"id": id, "title": title, "entry": entry, "origin": "trace", "steps": keys.iter().map(|k| step(k)).collect::<Vec<_>>()})
}

#[test]
fn a_retrace_retires_what_it_replaces_and_one_journey_stands_per_goal() {
    let tail = [
        "req:POST /api/berths/hold",
        "n:stripe",
        "hook:POST /stripe/webhook",
        "screen:web:/berths/booked",
    ];
    let with = |head: &[&str], extra: &[&str]| -> Vec<String> {
        head.iter()
            .chain(tail.iter())
            .chain(extra.iter())
            .map(|s| s.to_string())
            .collect()
    };
    fn refs(v: &[String]) -> Vec<&str> {
        v.iter().map(String::as_str).collect()
    }
    let full = with(
        &[
            "screen:web:/berths",
            "part:harbor-web/BerthForm.tsx#BerthForm",
            "req:GET /api/berths",
            "screen:web:/berths/pick",
            "req:POST /api/berths/quote",
        ],
        &[],
    );
    let short = with(
        &[
            "screen:web:/berths",
            "part:harbor-web/BerthForm.tsx#BerthForm",
        ],
        &[],
    );
    let longer = with(
        &[
            "screen:web:/berths",
            "part:harbor-web/BerthForm.tsx#BerthForm",
            "req:GET /api/berths",
            "screen:web:/berths/pick",
            "req:POST /api/berths/quote",
        ],
        &["screen:web:/account"],
    );
    let from_form = with(
        &[
            "part:harbor-web/BerthForm.tsx#BerthForm",
            "req:GET /api/berths",
            "screen:web:/berths/pick",
        ],
        &[],
    );
    let mut catch_all = journey(
        "book-an-event",
        "Book an event",
        "screen:web:/[...slug]",
        &[
            "screen:web:/[...slug]",
            "req:GET /api/events",
            "req:POST /api/events/book",
            "screen:web:/events/booked",
        ],
    );
    catch_all["split"] = json!(["screen:web:/events/(event)"]);
    // The stored map as the earlier release left it.
    let store = json!({"schema": fl::SCHEMA, "asked": {}, "flows": [
        journey("book-a-berth", "Book a berth", "screen:web:/berths", &refs(&full)),
        journey("book-a-berth-2", "Book a berth", "screen:web:/berths", &refs(&short)),
        journey("book-a-berth-3", "Book a berth", "screen:web:/berths/pick", &refs(&full[3..])),
        journey("a-book-a-berth", "Book a berth", "part:harbor-web/BerthForm.tsx#BerthForm", &refs(&from_form)),
        journey("a-book-a-berth-2", "Book a berth", "screen:web:/berths", &refs(&longer)),
        catch_all,
    ]});
    // The renderer split from the catch-all is traced.
    let todo = json!({"entries": [{"key": "screen:web:/events/(event)", "hash": "h"}]});
    let fresh = journey(
        "book-an-event",
        "Book an event",
        "screen:web:/events/(event)",
        &[
            "screen:web:/events/(event)",
            "req:POST /api/events/book",
            "screen:web:/events/booked",
        ],
    );
    let m = fl::merge(&store, &todo, vec![fresh]);
    let flows = m["flows"].as_array().unwrap();
    let got: Vec<(&str, &str)> = flows
        .iter()
        .map(|f| (f["id"].as_str().unwrap(), f["title"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [
            ("book-a-berth", "Book a berth"),
            ("book-an-event", "Book an event")
        ]
    );
    // The berth journey kept is the most complete, standing for every entry.
    assert_eq!(flows[0]["steps"].as_array().unwrap().len(), longer.len());
    assert_eq!(
        flows[0]["from"],
        json!([
            "part:harbor-web/BerthForm.tsx#BerthForm",
            "screen:web:/berths",
            "screen:web:/berths/pick"
        ])
    );
    // The renderer's trace retired the catch-all's journey.
    assert_eq!(flows[1]["entry"], "screen:web:/events/(event)");
    assert_eq!(flows[1]["steps"][0]["key"], "screen:web:/events/(event)");
    let mut end = Ending::default();
    for f in flows {
        end.said(f["id"].as_str().unwrap());
    }
    end.never_says("-2");
    end.never_says("-3");
    let titles: BTreeSet<String> = flows
        .iter()
        .map(|f| fl::title_words(f["title"].as_str().unwrap()))
        .collect();
    assert_eq!(titles.len(), flows.len());
    // The pipeline keeps every journey through this merge.
    let trace = include_str!("../app_map_trace.rs");
    assert!(trace.contains("fl::merge(store, &one, done.flows.clone())"));
    assert!(include_str!("../app_map_flows.rs").contains("let flows = dedupe(flows);"));
    assert!(include_str!("../../../floorplan/coderhelm/flows.mjs")
        .contains("const kept = dedupe(checked.kept);"));
}
