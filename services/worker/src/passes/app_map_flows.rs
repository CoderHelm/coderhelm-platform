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

/// The instructions the model reads.
pub const SYSTEM: &str = "You write user journeys for an app map. You see only a list of candidates that a static extractor found in the code; use no other knowledge of this code.

## The candidate list
One line per candidate; the first word is its id. Each cites file:line.
- `S..` screen: a routed screen, a step inside a page, or a form no router registers. `next[...]` are screens it goes to; `requests[...]` the requests it makes; `outside[...]` outside services it uses directly.
- `R..` request: a call the app makes and the endpoint that answers it (call site, then handler). `outside[...]` are services its handler uses; `async[...]` callbacks it starts; `requests[...]` further requests its handler makes.
- `X..` outside service. `async[...]` are endpoints it calls back.
- `A..` async callback: a webhook endpoint an outside service calls, an event and its listener, or webhooks the app sends out.

## Task
1. Write the user journeys this app has: at least one for every family of entries the user message lists (sign in, sign up, password reset, booking, checkout and the like). The same family in two apps (a website and a mobile app) is two journeys. Add any other end-to-end journey that is clearly present, such as cancel a booking or update a payment method. Write up to 16 journeys.
2. For each journey, pick and order steps the way a person lives them: the screen, each request it makes for this journey, the outside service behind each request, any callback that follows, then the next screen or the next step of the same form, and so on to where the journey ends. Start at a screen. Use 3 to 15 steps; prefer a complete journey to a short one.
3. After each request, add the outside service its `outside[...]` names (the same service may appear again after a later request), and after a sign-in or sign-up, the request that loads the person's details and the screen they land on, when listed.
4. Order requests by when the code makes them: `requests[...]` lists a screen's requests roughly in the order its code makes them. A payment or sign-up often ends with a callback (an `A..` webhook or event) after the outside service; put it after that service.
5. Every step cites one candidate id in `ref`, from the list only. Prefer candidates connected to the steps before them through next, requests, outside or async; a step with no such connection must be the obvious next screen.
6. `note` is one short sentence in plain words (under 15 words): what the person or the system does at that step. No arrows or symbols in place of words.

## Output
Only JSON, no prose:
{\"flows\": [{\"id\": \"sign-in\", \"title\": \"Sign in\", \"steps\": [{\"ref\": \"S3\", \"note\": \"...\"}]}], \"skipped\": [{\"flow\": \"...\", \"why\": \"...\"}]}";

/// What the model is asked: the entries to cover, the journeys already
/// kept, and the candidate list.
pub fn user_prompt(todo: &Value) -> String {
    let entries: Vec<String> = todo["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let id = e["id"].as_str()?;
            let label = todo["table"][id]["label"].as_str().unwrap_or("");
            let family = e["family"].as_str().unwrap_or("other");
            Some(format!("- {family}: {id} {label}"))
        })
        .collect();
    let keep: Vec<String> = todo["keep"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|k| Some(format!("- {}", k["title"].as_str()?)))
        .collect();
    let mut out = String::new();
    if !entries.is_empty() {
        out.push_str(
            "## Screens a journey may start at, by family (new or changed since the last map)\n",
        );
        out.push_str(&entries.join("\n"));
        out.push_str("\n\n");
    }
    if !keep.is_empty() {
        out.push_str("## Journeys the map already has (don't write these again)\n");
        out.push_str(&keep.join("\n"));
        out.push_str("\n\n");
    }
    out.push_str("## Candidates\n");
    out.push_str(todo["list"].as_str().unwrap_or(""));
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

/// The model's answer, checked against the candidate table: the journeys
/// accepted, as stored (`flows.json`), and what was refused.
pub fn check(answer: &Value, todo: &Value) -> (Vec<Value>, Checked) {
    let table = &todo["table"];
    let mut rep = Checked::default();
    let mut out: Vec<Value> = Vec::new();
    let mut ids: BTreeSet<String> = BTreeSet::new();
    for f in answer["flows"].as_array().into_iter().flatten() {
        let mut steps: Vec<Value> = Vec::new();
        let mut refs: Vec<String> = Vec::new();
        for s in f["steps"].as_array().into_iter().flatten() {
            rep.steps += 1;
            let r = s["ref"].as_str().unwrap_or("").trim().to_string();
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
            if refs.last() == Some(&r) {
                // The same step twice in a row is one step.
                rep.folded += 1;
                if let Some(last) = steps.last_mut() {
                    let prev = last["note"].as_str().unwrap_or("").to_string();
                    if !note.is_empty() && note != prev {
                        last["note"] = json!(plain(&format!("{prev} {note}"), 240));
                    }
                }
                continue;
            }
            if steps.len() >= MAX_STEPS {
                rep.cut += 1;
                continue;
            }
            let linked = refs.is_empty()
                || refs.iter().any(|p| {
                    table[p.as_str()]["next"]
                        .as_array()
                        .is_some_and(|n| n.iter().any(|x| x == r.as_str()))
                });
            if !linked {
                rep.unlinked += 1;
            }
            steps.push(json!({
                "key": c["key"],
                "kind": c["kind"],
                "view": c["view"],
                "note": note,
                "evidence": ev.iter().map(|e| json!({"file": e["file"], "line": e["line"], "token": e["token"]})).collect::<Vec<_>>(),
                "linked": linked,
            }));
            refs.push(r);
        }
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
    let text = crate::agent::anthropic::converse_simple(
        client,
        model_id,
        SYSTEM,
        &user_prompt(todo),
        usage,
    )
    .await
    .map_err(|e| e.to_string())?;
    let answer = parse_answer(&text).ok_or("the model's answer was not JSON")?;
    let (flows, rep) = check(&answer, todo);
    Ok((flows, rep, answer))
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
        assert_eq!(flows.len(), 1, "{flows:#?}");
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
        // Counted in every journey looked at, the dropped ones too.
        assert_eq!(rep.unlinked, 2);
        assert_eq!(rep.flows_dropped, 2);

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
        let p = user_prompt(&todo());
        assert!(p.contains("- other: S1 web /login"));
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
            if todo["list"].as_str().unwrap_or("").is_empty() {
                continue;
            }
            let mut usage = crate::models::TokenUsage::default();
            let started = std::time::Instant::now();
            let (flows, rep, answer) = ask(&client, &model, &todo, &mut usage).await.unwrap();
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
