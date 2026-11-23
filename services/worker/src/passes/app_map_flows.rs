//! The app map's journeys (flows): the model's part.
//!
//! The sandbox (`floorplan/coderhelm/flows.mjs`, run by the driver) lists
//! every step a journey could take from the built map and the code, each
//! citing file:line and a token on that line, and re-checks the journeys
//! kept from the last build. When an entry is new or what it stands on
//! changed, it hands back `todo`: the candidate list, the cited lines as
//! they are now, and the entries to (re)write. Here, after the map is
//! ready, the team's light model names journeys and picks and orders steps,
//! citing candidate ids only. The checker then accepts only:
//!
//! - ids from the list, whose cited lines still hold their tokens;
//! - journeys that start at a screen and have 3 to [`MAX_STEPS`] steps,
//!   repeated steps folded into one.
//!
//! A step no extracted edge connects to an earlier one is kept but marked
//! `linked: false` (the page draws it dashed). Nothing runs when `todo` is
//! empty, so a rebuild that changed nothing a journey stands on costs no
//! tokens. Journeys are kept in `floorplan/coderhelm/flows.json` inside the
//! map's state, and shown (`graph.flows`) only when [`visible`].

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use serde_json::{json, Value};

/// Steps in one journey, at most.
pub const MAX_STEPS: usize = 15;
/// Where journeys are kept inside the map's state.
pub const STORE: &str = "floorplan/coderhelm/flows.json";
/// The store's format.
pub const SCHEMA: &str = "coderhelm/flows@1";

/// Whether journeys are shown on the map and to Chat. They are kept and
/// re-checked either way; `APP_MAP_JOURNEYS=on` or `off` overrides.
pub fn visible() -> bool {
    match std::env::var("APP_MAP_JOURNEYS").as_deref() {
        Ok("on") => true,
        Ok("off") => false,
        _ => SHOWN_BY_DEFAULT,
    }
}

/// Journeys are shown unless the install turns them off: they passed the
/// ship gate on the gold set (step recall, no invented and no out-of-order
/// steps).
const SHOWN_BY_DEFAULT: bool = false;

/// The instructions the model reads. The model chooses which candidates
/// belong to each journey; the checker puts them in order from the code.
pub const SYSTEM: &str = "You write user journeys for an app map. You see only a list of candidates that a static extractor found in the code; use no other knowledge of this code.

## The candidate list
One line per candidate; the first word is its id.
- `S..` screen: a routed screen, a step inside a page, or a form no router registers. `next[...]` are screens it goes to; `requests[...]` the requests it makes; `outside[...]` outside services it uses directly.
- `R..` request: a call the app makes and the endpoint that answers it. `outside[...]` are services its handler uses; `async[...]` callbacks it starts; `requests[...]` further requests its handler makes.
- `X..` outside service. `async[...]` are endpoints it calls back.
- `A..` async callback: a webhook endpoint an outside service calls, an event and its listener, or webhooks the app sends out.

## Task
The user message names one journey family in one app (for example sign in on the mobile app) and the screens it may start at.
1. Write the journeys of that family in that app: usually one; more only when the app has clearly separate ways to do it.
2. For each journey, choose every candidate that is part of it: the screens the person goes through (including the steps of a form), each request those screens make for this journey, the outside services behind those requests, the callbacks that follow, and the screen they land on. Choose by what the candidates connect to (next, requests, outside, async); leave out requests the screens make for other purposes.
3. You don't need to order the steps: the code puts them in order. List the entry screen first.
4. Every step cites one candidate id in `ref`, from the list only. `note` is one short sentence in plain words (under 15 words): what the person or the system does at that step. No arrows or symbols in place of words.

## Output
Only JSON, no prose:
{\"flows\": [{\"id\": \"sign-in\", \"title\": \"Sign in\", \"steps\": [{\"ref\": \"S3\", \"note\": \"...\"}]}]}";

/// The groups the model is asked about, one call each: a journey family
/// in one app with its own candidates. A todo from before groups is one
/// group.
pub fn groups(todo: &Value) -> Vec<Value> {
    match todo["groups"].as_array() {
        Some(g) if !g.is_empty() => g.clone(),
        _ => vec![json!({
            "family": "any",
            "app": "",
            "list": todo["list"],
            "entries": todo["entries"].as_array().into_iter().flatten().filter_map(|e| e["id"].as_str()).collect::<Vec<_>>(),
        })],
    }
}

/// What the model is asked for one group: its family and app, the screens
/// a journey may start at, the journeys already kept, and its candidates.
pub fn user_prompt(todo: &Value, group: &Value) -> String {
    let family = group["family"].as_str().unwrap_or("any");
    let app = group["app"].as_str().unwrap_or("");
    let entries: Vec<String> = group["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let id = e.as_str()?;
            let label = todo["table"][id]["label"].as_str().unwrap_or("");
            Some(format!("- {id} {label}"))
        })
        .collect();
    let keep: Vec<String> = todo["keep"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|k| Some(format!("- {}", k["title"].as_str()?)))
        .collect();
    let mut out = format!("## Journey family: {family}, in the app {app}\n\n");
    if !entries.is_empty() {
        out.push_str("## Screens it may start at\n");
        out.push_str(&entries.join("\n"));
        out.push_str("\n\n");
    }
    if !keep.is_empty() {
        out.push_str("## Journeys the map already has (don't write these again)\n");
        out.push_str(&keep.join("\n"));
        out.push_str("\n\n");
    }
    out.push_str("## Candidates\n");
    out.push_str(group["list"].as_str().unwrap_or(""));
    out
}

/// The JSON object in the model's reply (it may wrap it in a code fence).
pub fn parse_answer(text: &str) -> Option<Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    serde_json::from_str(text.get(start..=end)?).ok()
}

/// What the checker did, for the log and the eval.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Checked {
    pub steps: usize,
    pub unknown: usize,
    pub citation: usize,
    pub folded: usize,
    /// Steps the code's evidence couldn't place, kept where the model put
    /// them relative to the others (drawn dashed).
    pub model_ordered: usize,
    pub cut: usize,
    pub unlinked: usize,
    pub flows_dropped: usize,
}

/// Whether a cited token is on its line (plus or minus 3) in the window
/// the sandbox read: the lines around it as they are in this build.
fn cited(e: &Value) -> bool {
    let token = e["token"].as_str().unwrap_or("");
    let line = e["line"].as_u64().unwrap_or(0) as i64;
    let from = e["from"].as_u64().unwrap_or(1) as i64;
    if token.is_empty() || line < 1 {
        return false;
    }
    e["window"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .any(|(i, l)| {
            let at = from + i as i64;
            (at - line).abs() <= 3 && l.as_str().is_some_and(|l| l.contains(token))
        })
}

/// Plain words: no arrows or symbols standing in for them, one line,
/// short.
pub fn plain(s: &str, max: usize) -> String {
    let mut t = s.replace(['\n', '\r', '\t'], " ");
    for (from, to) in [
        ("-->", " then "),
        ("->", " then "),
        ("=>", " then "),
        ("\u{2192}", " then "),
        ("\u{21d2}", " then "),
        ("\u{279c}", " then "),
        ("\u{27f6}", " then "),
        ("\u{2190}", " from "),
    ] {
        t = t.replace(from, to);
    }
    let t = t.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&t, max).to_string()
}

fn slug(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    out.trim_end_matches('-').chars().take(60).collect()
}

/// The chosen steps in order. Each request goes with the services its
/// handler uses and the callbacks they start (right after it). Then the
/// blocks keep the model's order except where the code says otherwise:
/// - a screen comes before the requests it makes itself (a request belongs
///   to the latest screen the model lists that makes it, so a form's own
///   call belongs to the form, not the page around it);
/// - a screen's requests come in the order its code makes them;
/// - a request a route handler makes comes after that route;
/// - a screen comes before the screens it goes to (when that doesn't
///   contradict an order already set).
///
/// Returns (order, linked per step, how many blocks the code had nothing
/// to say about, so their place is the model's).
pub fn order(refs: &[String], table: &Value) -> (Vec<String>, Vec<bool>, usize) {
    let chosen: BTreeSet<&str> = refs.iter().map(String::as_str).collect();
    let pos = |id: &str| refs.iter().position(|r| r == id).unwrap_or(usize::MAX);
    let kind = |id: &str| table[id]["kind"].as_str().unwrap_or("").to_string();
    let next = |from: &str| -> Vec<String> {
        table[from]["next"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|x| x.as_str())
            .filter(|x| chosen.contains(x))
            .map(str::to_string)
            .collect()
    };
    let links = |from: &str, to: &str| next(from).iter().any(|x| x == to);
    let weak = |from: &str, to: &str| {
        table[from]["weak"]
            .as_array()
            .is_some_and(|w| w.iter().any(|x| x == to))
    };
    // Blocks: a request with its services and their callbacks; a screen
    // with the services it uses that no chosen request leads to; anything
    // else alone.
    let mut block_of: BTreeMap<String, usize> = BTreeMap::new();
    let mut blocks: Vec<Vec<String>> = Vec::new();
    for r in refs.iter().filter(|r| kind(r) == "request") {
        let i = blocks.len();
        let mut b = vec![r.clone()];
        block_of.insert(r.clone(), i);
        for o in next(r) {
            let k = kind(&o);
            if (k == "outside" || k == "async") && !block_of.contains_key(&o) {
                block_of.insert(o.clone(), i);
                b.push(o.clone());
                for a in next(&o) {
                    if kind(&a) == "async" && !block_of.contains_key(&a) {
                        block_of.insert(a.clone(), i);
                        b.push(a);
                    }
                }
            }
        }
        blocks.push(b);
    }
    for s in refs.iter().filter(|r| kind(r) == "screen") {
        let i = blocks.len();
        let mut b = vec![s.clone()];
        block_of.insert(s.clone(), i);
        for o in next(s) {
            let k = kind(&o);
            if (k == "outside" || k == "async") && !block_of.contains_key(&o) {
                block_of.insert(o.clone(), i);
                b.push(o);
            }
        }
        blocks.push(b);
    }
    for r in refs {
        if !block_of.contains_key(r) {
            block_of.insert(r.clone(), blocks.len());
            blocks.push(vec![r.clone()]);
        }
    }
    let head = |b: usize| blocks[b].iter().map(|x| pos(x)).min().unwrap_or(usize::MAX);
    // Constraints between blocks (a before b).
    let n = blocks.len();
    let mut after: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    // Blocks the code's evidence places (the entry pin doesn't count).
    let mut placed_by_code = vec![false; n];
    let reaches = |after: &Vec<BTreeSet<usize>>, from: usize, to: usize| {
        let mut stack = vec![from];
        let mut seen = BTreeSet::new();
        while let Some(x) = stack.pop() {
            if x == to {
                return true;
            }
            if seen.insert(x) {
                stack.extend(after[x].iter().copied());
            }
        }
        false
    };
    let add = |after: &mut Vec<BTreeSet<usize>>, by_code: &mut Vec<bool>, a: usize, b: usize| {
        if a != b && !reaches(after, b, a) {
            after[a].insert(b);
            by_code[a] = true;
            by_code[b] = true;
        }
    };
    let screens: Vec<&String> = refs.iter().filter(|r| kind(r) == "screen").collect();
    let requests: Vec<&String> = refs.iter().filter(|r| kind(r) == "request").collect();
    // The screen each request belongs to.
    let mut owned: BTreeMap<&String, Vec<&String>> = BTreeMap::new();
    for r in &requests {
        let own = screens.iter().rev().find(|s| links(s, r) && !weak(s, r));
        let any = screens.iter().rev().find(|s| links(s, r));
        if let Some(s) = own.or(any) {
            owned.entry(*s).or_default().push(*r);
        }
    }
    for (s, rs) in &owned {
        let mut rs: Vec<&&String> = rs.iter().collect();
        // In the order the screen's code makes them.
        let rank = |r: &String| next(s).iter().position(|x| x == r).unwrap_or(usize::MAX);
        rs.sort_by_key(|r| rank(r));
        let sb = block_of[*s];
        let mut prev = sb;
        for r in rs {
            let rb = block_of[r.as_str()];
            add(&mut after, &mut placed_by_code, sb, rb);
            add(&mut after, &mut placed_by_code, prev, rb);
            prev = rb;
        }
    }
    for a in &requests {
        for b in &requests {
            if a != b && links(a, b) && !owned.values().any(|v| v.contains(b)) {
                add(&mut after, &mut placed_by_code, block_of[*a], block_of[*b]);
            }
        }
    }
    // The entry (the first screen the model lists) comes first.
    if let Some(e) = screens.first() {
        let eb = block_of[e.as_str()];
        for b in 0..blocks.len() {
            if b != eb && !reaches(&after, b, eb) {
                after[eb].insert(b);
            }
        }
    }
    // Navigation, in the model's order of the screens.
    for a in &screens {
        for b in next(a) {
            if kind(&b) == "screen" {
                add(&mut after, &mut placed_by_code, block_of[*a], block_of[&b]);
            }
        }
    }
    // Stable topological order: of the blocks ready, the one the model
    // lists first.
    let mut indeg = vec![0usize; n];
    for a in &after {
        for b in a {
            indeg[*b] += 1;
        }
    }
    let constrained = placed_by_code;
    let mut done = vec![false; n];
    let mut seq: Vec<usize> = Vec::new();
    for _ in 0..n {
        let pick = (0..n)
            .filter(|i| !done[*i] && indeg[*i] == 0)
            .min_by_key(|i| head(*i))
            .or_else(|| (0..n).filter(|i| !done[*i]).min_by_key(|i| head(*i)));
        let Some(i) = pick else { break };
        done[i] = true;
        seq.push(i);
        for b in after[i].clone() {
            indeg[b] = indeg[b].saturating_sub(1);
        }
    }
    let mut placed = Vec::new();
    let mut linked = Vec::new();
    let mut by_model = 0;
    for (k, i) in seq.iter().enumerate() {
        if !constrained[*i] && k > 0 {
            by_model += 1;
        }
        for (j, x) in blocks[*i].iter().enumerate() {
            placed.push(x.clone());
            linked.push(k == 0 || j > 0 || constrained[*i]);
        }
    }
    (placed, linked, by_model)
}

/// The model's answer, checked against the candidate table: the journeys
/// accepted, as stored (`flows.json`), and what was refused. The model
/// chooses the steps; their order comes from the code ([`order`]).
pub fn check(answer: &Value, todo: &Value) -> (Vec<Value>, Checked) {
    let table = &todo["table"];
    let mut rep = Checked::default();
    let mut out: Vec<Value> = Vec::new();
    let mut ids: BTreeSet<String> = BTreeSet::new();
    for f in answer["flows"].as_array().into_iter().flatten() {
        let mut refs: Vec<String> = Vec::new();
        let mut notes: BTreeMap<String, String> = BTreeMap::new();
        for s in f["steps"].as_array().into_iter().flatten() {
            rep.steps += 1;
            let r = s["ref"]
                .as_str()
                .or(s.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let c = &table[r.as_str()];
            if c.is_null() {
                rep.unknown += 1;
                continue;
            }
            let ev = c["evidence"].as_array().cloned().unwrap_or_default();
            if ev.is_empty() || !ev.iter().all(cited) {
                rep.citation += 1;
                continue;
            }
            let note = plain(s["note"].as_str().unwrap_or(""), 160);
            if refs.contains(&r) {
                // The same step twice is one step.
                rep.folded += 1;
                let prev = notes.get(&r).cloned().unwrap_or_default();
                if !note.is_empty() && note != prev {
                    notes.insert(r.clone(), plain(&format!("{prev} {note}"), 240));
                }
                continue;
            }
            notes.insert(r.clone(), note);
            refs.push(r);
        }
        let (mut ordered, mut linked, by_model) = order(&refs, table);
        rep.model_ordered += by_model;
        if ordered.len() > MAX_STEPS {
            rep.cut += ordered.len() - MAX_STEPS;
            ordered.truncate(MAX_STEPS);
            linked.truncate(MAX_STEPS);
        }
        rep.unlinked += linked.iter().filter(|l| !**l).count();
        let steps: Vec<Value> = ordered
            .iter()
            .zip(&linked)
            .map(|(r, l)| {
                let c = &table[r.as_str()];
                json!({
                    "key": c["key"],
                    "kind": c["kind"],
                    "view": c["view"],
                    "note": notes.get(r).cloned().unwrap_or_default(),
                    "evidence": c["evidence"].as_array().into_iter().flatten().map(|e| json!({"file": e["file"], "line": e["line"], "token": e["token"]})).collect::<Vec<_>>(),
                    "linked": l,
                })
            })
            .collect();
        if steps.len() < 3 || steps[0]["kind"] != "screen" {
            rep.flows_dropped += 1;
            continue;
        }
        let title = plain(f["title"].as_str().unwrap_or(""), 80);
        let title = if title.is_empty() {
            "Journey".to_string()
        } else {
            title
        };
        let mut id = slug(f["id"].as_str().unwrap_or(&title));
        if id.is_empty() {
            id = slug(&title);
        }
        let base = id.clone();
        let mut n = 2;
        while !ids.insert(id.clone()) {
            id = format!("{base}-{n}");
            n += 1;
        }
        out.push(json!({
            "id": id,
            "title": title,
            "entry": steps[0]["key"],
            "origin": "model",
            "steps": steps,
        }));
    }
    (out, rep)
}

/// The store after a model run: journeys for the entries asked about are
/// replaced by the new ones (an entry the model wrote nothing for keeps
/// what it had), and each entry asked about is remembered with its hash so
/// it is not asked again until it changes.
pub fn merge(store: &Value, todo: &Value, new: Vec<Value>) -> Value {
    let hashes: BTreeMap<String, String> = todo["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| Some((e["key"].as_str()?.into(), e["hash"].as_str()?.into())))
        .collect();
    let written: BTreeSet<String> = new
        .iter()
        .filter_map(|f| f["entry"].as_str().map(str::to_string))
        .collect();
    let mut flows: Vec<Value> = store["flows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| {
            let e = f["entry"].as_str().unwrap_or("");
            !written.contains(e)
        })
        .cloned()
        .collect();
    let mut taken: BTreeSet<String> = flows
        .iter()
        .filter_map(|f| f["id"].as_str().map(str::to_string))
        .collect();
    for mut f in new {
        let entry = f["entry"].as_str().unwrap_or("").to_string();
        f["hash"] = json!(hashes.get(&entry).cloned().unwrap_or_default());
        let base = f["id"].as_str().unwrap_or("journey").to_string();
        let mut id = base.clone();
        let mut n = 2;
        while !taken.insert(id.clone()) {
            id = format!("{base}-{n}");
            n += 1;
        }
        f["id"] = json!(id);
        flows.push(f);
    }
    let mut asked: serde_json::Map<String, Value> =
        store["asked"].as_object().cloned().unwrap_or_default();
    for (k, h) in hashes {
        asked.insert(k, json!(h));
    }
    json!({ "schema": SCHEMA, "flows": flows, "asked": asked })
}

/// A stored journey as the viewer draws it (`graph.flows`): one step per
/// drawable hop, steps on the same chip folded into one.
pub fn for_viewer(f: &Value) -> Value {
    let mut steps: Vec<Value> = Vec::new();
    let mut linked: Vec<bool> = Vec::new();
    for s in f["steps"].as_array().into_iter().flatten() {
        let Some(r) = s["view"].as_str() else {
            continue;
        };
        let note = s["note"].as_str().unwrap_or("");
        let evidence: Vec<Value> = s["evidence"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|e| json!({"file": e["file"], "line": e["line"]}))
            .collect();
        if let Some(last) = steps.last_mut() {
            if last["ref"] == r {
                let prev = last["note"].as_str().unwrap_or("").to_string();
                last["note"] = json!(format!("{prev} Then {}", lower_first(note)));
                if let Some(ev) = last["evidence"].as_array_mut() {
                    ev.extend(evidence);
                }
                continue;
            }
        }
        steps.push(json!({"ref": r, "note": note, "evidence": evidence}));
        linked.push(s["linked"] != false);
    }
    json!({
        "id": f["id"],
        "title": f["title"],
        "description": f["description"].as_str().unwrap_or(""),
        "steps": steps,
        "custom": true,
        "coderhelm": {"entry": f["entry"], "linked": linked},
    })
}

fn lower_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// The map's flows with the store's journeys in place of the last ones
/// CoderHelm wrote (Floorplan's own flows are kept).
pub fn put_in_graph(graph: &mut Value, store: &Value) {
    let mut flows: Vec<Value> = graph["flows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| f.get("coderhelm").is_none())
        .cloned()
        .collect();
    flows.extend(
        store["flows"]
            .as_array()
            .into_iter()
            .flatten()
            .map(for_viewer),
    );
    graph["flows"] = json!(flows);
}

/// The map's state with one file replaced (or added).
pub fn replace_in_tgz(tgz: &[u8], path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(tgz));
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut out = tar::Builder::new(gz);
    let mut put = |p: &str, b: &[u8]| -> Result<(), String> {
        let mut h = tar::Header::new_gnu();
        h.set_size(b.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        out.append_data(&mut h, p, b).map_err(|e| e.to_string())
    };
    for e in ar.entries().map_err(|e| e.to_string())? {
        let mut e = e.map_err(|e| e.to_string())?;
        if e.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let p = e
            .path()
            .map_err(|e| e.to_string())?
            .to_string_lossy()
            .to_string();
        if p.trim_start_matches("./") == path {
            continue;
        }
        let mut buf = Vec::new();
        e.read_to_end(&mut buf).map_err(|e| e.to_string())?;
        put(&p, &buf)?;
    }
    put(path, body)?;
    out.into_inner()
        .and_then(|g| g.finish())
        .map_err(|e| e.to_string())
}

/// One file out of the map's state.
pub fn read_from_tgz(tgz: &[u8], path: &str) -> Option<Vec<u8>> {
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(tgz));
    for e in ar.entries().ok()? {
        let mut e = e.ok()?;
        let p = e.path().ok()?.to_string_lossy().to_string();
        if p.trim_start_matches("./") == path {
            let mut buf = Vec::new();
            e.read_to_end(&mut buf).ok()?;
            return Some(buf);
        }
    }
    None
}

/// Asks the model once and checks its answer. Shared by the worker step
/// and the live eval, so both run the same prompt and checker.
pub async fn ask(
    client: &crate::agent::anthropic::AnthropicClient,
    model_id: &str,
    todo: &Value,
    usage: &mut crate::models::TokenUsage,
) -> Result<(Vec<Value>, Checked, Value), String> {
    // One call per journey family in each app, a few at a time.
    let groups = groups(todo);
    let mut flows: Vec<Value> = Vec::new();
    let mut failed = Vec::new();
    for chunk in groups.chunks(4) {
        let calls = chunk.iter().map(|g| async move {
            let mut u = crate::models::TokenUsage::default();
            let text = crate::agent::anthropic::converse_simple(
                client,
                model_id,
                SYSTEM,
                &user_prompt(todo, g),
                &mut u,
            )
            .await
            .map_err(|e| e.to_string());
            (text, u)
        });
        for (text, u) in futures::future::join_all(calls).await {
            usage.merge(&u);
            match text.map(|t| parse_answer(&t)) {
                Ok(Some(a)) => flows.extend(a["flows"].as_array().cloned().unwrap_or_default()),
                Ok(None) => failed.push("the model's answer was not JSON".to_string()),
                Err(e) => failed.push(e),
            }
        }
    }
    if flows.is_empty() && !failed.is_empty() {
        return Err(failed.join("; "));
    }
    let answer = json!({ "flows": flows });
    let (out, rep) = check(&answer, todo);
    Ok((out, rep, answer))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A candidate table as the sandbox writes it, for a made-up harbor
    /// app: the sign-in screen, its request and the service behind it, a
    /// second screen, and a step whose token is no longer on its line.
    fn todo() -> Value {
        let ev = |file: &str, line: u64, token: &str, window: &[&str]| json!({"file": file, "line": line, "token": token, "from": line.saturating_sub(3).max(1), "window": window});
        json!({
            "list": "S1 screen web /login @ harbor-web/app/login/page.tsx:4 requests[R1]\nR1 request POST /api/session @ harbor-web/lib/session.ts:9 then harbor-web/app/api/session/route.ts:3 outside[X1]\nX1 outside Okta @ harbor-api/src/auth.js:1\nS2 screen web /slips @ harbor-web/app/slips/page.tsx:2",
            "entries": [{"id": "S1", "key": "screen:web:/login", "hash": "h1"}],
            "keep": [{"id": "book-a-slip", "title": "Book a slip"}],
            "table": {
                "S1": {"key": "screen:web:/login", "kind": "screen", "view": "f:harbor-web/app/login/page.tsx", "label": "web /login", "next": ["R1"],
                       "evidence": [ev("harbor-web/app/login/page.tsx", 4, "LoginPage", &["", "", "", "export default function LoginPage() {"])]},
                "R1": {"key": "req:POST /api/session", "kind": "request", "view": "r:2", "label": "POST /api/session", "next": ["X1"],
                       "evidence": [ev("harbor-web/lib/session.ts", 9, "/api/session", &["a", "b", "c", "d", "e", "f", "  await fetch('/api/session', {"]),
                                    ev("harbor-web/app/api/session/route.ts", 3, "POST", &["", "", "export async function POST(req) {"])]},
                "X1": {"key": "n:okta", "kind": "outside", "view": "n:okta", "label": "Okta", "next": [],
                       "evidence": [ev("harbor-api/src/auth.js", 1, "@okta/okta-auth-js", &["import { OktaAuth } from '@okta/okta-auth-js';"])]},
                "S2": {"key": "screen:web:/slips", "kind": "screen", "view": "f:harbor-web/app/slips/page.tsx", "label": "web /slips", "next": [],
                       "evidence": [ev("harbor-web/app/slips/page.tsx", 2, "Slips()", &["", "export default function Slips() {"])]},
            }
        })
    }

    #[test]
    fn the_checker_takes_only_listed_ids_whose_lines_still_hold_their_tokens() {
        let answer = json!({"flows": [
            {"id": "sign-in", "title": "Sign in", "steps": [
                {"ref": "S1", "note": "The boater opens the sign-in page."},
                {"ref": "R1", "note": "The page posts the email -> the session route."},
                {"ref": "R1", "note": "The route checks the password."},
                {"ref": "R9", "note": "An invented request."},
                {"ref": "X1", "note": "Okta checks the password."},
                {"ref": "S2", "note": "The boater lands on their slips."},
            ]},
            {"id": "too-short", "title": "Too short", "steps": [{"ref": "S1", "note": "a"}, {"ref": "R1", "note": "b"}]},
            {"id": "not-a-screen", "title": "Starts at a request", "steps": [{"ref": "R1", "note": "a"}, {"ref": "X1", "note": "b"}, {"ref": "S1", "note": "c"}]},
        ]});
        let (flows, rep) = check(&answer, &todo());
        // The journey listed from its request comes out from its screen:
        // the code orders it.
        assert_eq!(flows.len(), 2, "{flows:#?}");
        let f = &flows[0];
        assert_eq!(f["entry"], "screen:web:/login");
        let keys: Vec<&str> = f["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["key"].as_str().unwrap())
            .collect();
        assert_eq!(
            keys,
            [
                "screen:web:/login",
                "req:POST /api/session",
                "n:okta",
                "screen:web:/slips"
            ]
        );
        // Words, never arrows; the repeated request folded into one step.
        let note = f["steps"][1]["note"].as_str().unwrap();
        assert!(
            !note.contains("->") && note.contains("then the session route"),
            "{note}"
        );
        assert!(note.contains("checks the password"), "{note}");
        // S2 has no edge from an earlier step: kept, drawn dashed.
        assert_eq!(f["steps"][3]["linked"], false);
        assert_eq!(f["steps"][2]["linked"], true);
        assert_eq!(rep.unknown, 1);
        assert_eq!(rep.folded, 1);
        assert_eq!(rep.unlinked, 1);
        assert_eq!(rep.model_ordered, 1);
        assert_eq!(rep.flows_dropped, 1);
        let second: Vec<&str> = flows[1]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["key"].as_str().unwrap())
            .collect();
        assert_eq!(
            second,
            ["screen:web:/login", "req:POST /api/session", "n:okta"]
        );

        // A cited line that no longer holds its token is refused.
        let mut moved = todo();
        moved["table"]["X1"]["evidence"][0]["window"] = json!(["import { Auth } from './auth';"]);
        let (flows, rep) = check(&answer, &moved);
        // X1 is cited by two journeys; both are refused.
        assert_eq!(rep.citation, 2);
        assert!(flows[0]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["key"] != "n:okta"));
    }

    #[test]
    fn the_code_orders_the_chosen_steps_and_the_model_places_only_what_it_cannot() {
        let mut t = todo();
        t["table"]["R2"] = json!({"key": "req:GET /me", "kind": "request", "view": "r:3", "label": "GET /me", "next": ["X1"],
            "evidence": [{"file": "harbor-web/lib/me.ts", "line": 2, "token": "/me", "from": 1, "window": ["", "  fetch('/me')"]}]});
        // Listed out of order, the service twice: the code puts the screen,
        // its request and the service behind it in order, once.
        let answer = json!({"flows": [{"id": "sign-in", "title": "Sign in", "steps": [
            {"ref": "X1", "note": "Okta checks it."},
            {"ref": "R1", "note": "Posts the email."},
            {"ref": "S1", "note": "Opens the page."},
            {"ref": "X1", "note": "Okta answers again."},
            {"ref": "R2", "note": "Loads the boater."},
            {"ref": "S2", "note": "Lands on the slips."},
        ]}]});
        let (flows, rep) = check(&answer, &t);
        let keys: Vec<&str> = flows[0]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["key"].as_str().unwrap())
            .collect();
        // R2 and S2 have no edge from the steps before them: they keep the
        // model's place (after R1's service and after R2), drawn dashed.
        assert_eq!(
            keys,
            [
                "screen:web:/login",
                "req:POST /api/session",
                "n:okta",
                "req:GET /me",
                "screen:web:/slips"
            ]
        );
        assert_eq!(
            flows[0]["steps"][2]["note"],
            "Okta checks it. Okta answers again."
        );
        let linked: Vec<bool> = flows[0]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["linked"] == true)
            .collect();
        assert_eq!(linked, [true, true, true, false, false]);
        assert_eq!(rep.folded, 1);
        assert_eq!(rep.model_ordered, 2);
        // Order alone: a screen's requests by where its code makes them.
        let mut t = todo();
        t["table"]["S1"]["next"] = json!(["R2", "R1"]);
        t["table"]["R2"] = json!({"key": "req:GET /csrf", "kind": "request", "view": "r:4", "label": "GET /csrf", "next": [],
            "evidence": [{"file": "harbor-web/lib/csrf.ts", "line": 1, "token": "/csrf", "from": 1, "window": ["fetch('/csrf')"]}]});
        let (o, l, m) = order(
            &["R1".into(), "X1".into(), "S1".into(), "R2".into()],
            &t["table"],
        );
        assert_eq!(o, ["S1", "R2", "R1", "X1"]);
        assert_eq!(l, [true; 4]);
        assert_eq!(m, 0);
    }

    #[test]
    fn a_journey_has_at_most_fifteen_steps() {
        let mut t = todo();
        let mut steps = vec![json!({"ref": "S1", "note": "start"})];
        for i in 0..20 {
            let id = format!("S{}", i + 10);
            t["table"][&id] = json!({"key": format!("screen:web:/{i}"), "kind": "screen", "view": format!("f:p{i}.tsx"), "label": "x", "next": [],
                "evidence": [{"file": format!("p{i}.tsx"), "line": 1, "token": "Page", "from": 1, "window": ["export default function Page() {}"]}]});
            steps.push(json!({"ref": id, "note": "next"}));
        }
        let (flows, rep) = check(
            &json!({"flows": [{"id": "long", "title": "Long", "steps": steps}]}),
            &t,
        );
        assert_eq!(flows[0]["steps"].as_array().unwrap().len(), MAX_STEPS);
        assert_eq!(rep.cut, 21 - MAX_STEPS);
    }

    #[test]
    fn the_prompt_lists_the_entries_the_kept_journeys_and_the_candidates() {
        let t = todo();
        let p = user_prompt(&t, &groups(&t)[0]);
        assert!(p.contains("- S1 web /login"));
        assert!(p.contains("## Journey family: any"));
        assert!(p.contains("- Book a slip"));
        assert!(p.contains("R1 request POST /api/session"));
        assert!(!SYSTEM.contains('\u{2192}'));
        assert_eq!(
            parse_answer("Here:\n```json\n{\"flows\": []}\n```").unwrap(),
            json!({"flows": []})
        );
        assert!(parse_answer("no json").is_none());
    }

    #[test]
    fn a_merge_replaces_only_the_entries_asked_about_and_remembers_them() {
        let store = json!({"schema": SCHEMA, "flows": [
            {"id": "sign-in", "title": "Old sign in", "entry": "screen:web:/login", "hash": "h0", "steps": []},
            {"id": "book-a-slip", "title": "Book a slip", "entry": "screen:web:/slips/:id", "hash": "h2", "steps": []},
        ], "asked": {"screen:web:/slips/:id": "h2"}});
        let new = vec![
            json!({"id": "sign-in", "title": "Sign in", "entry": "screen:web:/login", "steps": []}),
        ];
        let m = merge(&store, &todo(), new);
        let titles: Vec<&str> = m["flows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["title"].as_str().unwrap())
            .collect();
        assert_eq!(titles, ["Book a slip", "Sign in"]);
        assert_eq!(m["flows"][1]["hash"], "h1");
        assert_eq!(m["asked"]["screen:web:/login"], "h1");
        assert_eq!(m["asked"]["screen:web:/slips/:id"], "h2");
        // The model wrote nothing for an entry: it keeps what it had.
        let m = merge(&store, &todo(), Vec::new());
        assert_eq!(m["flows"].as_array().unwrap().len(), 2);
        assert_eq!(m["asked"]["screen:web:/login"], "h1");
    }

    #[test]
    fn the_viewer_gets_one_step_per_hop_and_floorplans_own_flows_stay() {
        let stored = json!({"flows": [{"id": "sign-in", "title": "Sign in", "entry": "screen:web:/login", "steps": [
            {"view": "f:harbor-web/app/login/page.tsx", "note": "Opens the page.", "linked": true},
            {"view": "r:2", "note": "Posts the email.", "linked": true},
            {"view": "r:2", "note": "Checks it.", "linked": true},
            {"view": "n:okta", "note": "Okta answers.", "linked": false},
        ]}]});
        let mut g = json!({"flows": [{"id": "fp", "title": "Floorplan's", "steps": []}, {"id": "old", "coderhelm": {}, "steps": []}]});
        put_in_graph(&mut g, &stored);
        let flows = g["flows"].as_array().unwrap();
        assert_eq!(flows.len(), 2);
        assert_eq!(flows[0]["id"], "fp");
        let steps = flows[1]["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[1]["note"], "Posts the email. Then checks it.");
        assert_eq!(flows[1]["coderhelm"]["linked"], json!([true, true, false]));
    }

    /// Live: the team's light model on a gold set's candidates, through the
    /// same prompt and checker as the worker. One `<set>.todo.json` per set
    /// in the folder (written by the sandbox's `prepare`); writes
    /// `<set>.flows.json` (the checked journeys), `<set>.answer.json` and
    /// `<set>.usage.json` beside it.
    ///
    /// ```text
    /// APP_MAP_FLOWS_EVAL=/path EVAL_PROVIDER=bedrock EVAL_MODEL=claude-sonnet-5-5 \
    ///   cargo test -p worker live_journeys -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "calls the real model: set APP_MAP_FLOWS_EVAL and EVAL_PROVIDER=bedrock, run with --ignored"]
    async fn live_journeys_on_the_gold_set() {
        let dir = std::path::PathBuf::from(
            std::env::var("APP_MAP_FLOWS_EVAL").expect("APP_MAP_FLOWS_EVAL"),
        );
        let (client, model) = super::super::review_eval::eval_client().await;
        if std::env::var("EVAL_PROVIDER").is_ok_and(|p| p == "bedrock") {
            // As a Bedrock install sends it (CHAT_EVAL_BEDROCK's pattern):
            // Bedrock's request shape, no API-only fields.
            common::model_provider::set_current(
                common::model_provider::Settings {
                    provider: common::model_provider::Provider::Bedrock,
                    ..Default::default()
                },
                common::model_provider::Source::Deploy,
            );
        }
        let mut sets: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.strip_suffix(".todo.json").map(str::to_string)
            })
            .collect();
        sets.sort();
        for set in sets {
            let todo: Value = serde_json::from_slice(
                &std::fs::read(dir.join(format!("{set}.todo.json"))).unwrap(),
            )
            .unwrap();
            if groups(&todo)
                .iter()
                .all(|g| g["list"].as_str().unwrap_or("").is_empty())
            {
                continue;
            }
            let mut usage = crate::models::TokenUsage::default();
            let started = std::time::Instant::now();
            // A replayed answer (`<set>.replay.json`) goes through the same
            // checker with no model call: how the code orders a given set.
            let replay = std::fs::read(dir.join(format!("{set}.replay.json")))
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
            let (flows, rep, answer) = match replay {
                Some(a) => {
                    let (f, r) = check(&a, &todo);
                    (f, r, a)
                }
                None => ask(&client, &model, &todo, &mut usage).await.unwrap(),
            };
            println!(
                "{set}: {} journeys, {:?}, {} in / {} out tokens, ${:.4}, {:.0}s",
                flows.len(),
                rep,
                usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens,
                usage.output_tokens,
                usage.cost_usd,
                started.elapsed().as_secs_f64()
            );
            std::fs::write(
                dir.join(format!("{set}.flows.json")),
                serde_json::to_vec_pretty(&flows).unwrap(),
            )
            .unwrap();
            std::fs::write(
                dir.join(format!("{set}.answer.json")),
                serde_json::to_vec_pretty(&answer).unwrap(),
            )
            .unwrap();
            std::fs::write(
                dir.join(format!("{set}.usage.json")),
                serde_json::to_vec_pretty(&json!({
                    "model": model,
                    "input": usage.input_tokens,
                    "cache_read": usage.cache_read_tokens,
                    "cache_write": usage.cache_write_tokens,
                    "output": usage.output_tokens,
                    "cost_usd": usage.cost_usd,
                    "checked": rep,
                    "secs": started.elapsed().as_secs_f64(),
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    fn the_store_is_replaced_inside_the_maps_state() {
        let mut b = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (p, body) in [
            ("floorplan/architecture.json", &b"{}"[..]),
            (STORE, &b"{\"old\":1}"[..]),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, p, body).unwrap();
        }
        let tgz = b.into_inner().unwrap().finish().unwrap();
        let out = replace_in_tgz(&tgz, STORE, b"{\"new\":1}").unwrap();
        assert_eq!(read_from_tgz(&out, STORE).unwrap(), b"{\"new\":1}");
        assert_eq!(
            read_from_tgz(&out, "floorplan/architecture.json").unwrap(),
            b"{}"
        );
    }
}
