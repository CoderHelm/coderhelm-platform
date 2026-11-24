//! The app map's journeys, traced: for each entry screen the sandbox found,
//! the light model reads the code with read-only tools (the snapshot's
//! files, and the map's own lookups) and follows the journey the way a
//! person would: the screen, the request it makes, the handler that answers,
//! the service code, the outside service, the callbacks, the screen it lands
//! on. Every step it gives cites one line that must hold its token, so
//! nothing it can't point at is kept. Steps are put on the map's nodes
//! where the sandbox's candidates cite the same lines, and where the code's
//! evidence orders those nodes (`app_map_flows::order`), evidence wins (the
//! conflicts are counted).
//!
//! The candidate picker (`app_map_flows::ask`) stays as the fallback for an
//! entry whose trace fails.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use common::app_map as am;
use futures::StreamExt;
use serde_json::{json, Value};

use super::app_map::Snapshot;
use super::app_map_flows as fl;
use crate::agent::llm::{ToolDefinition, ToolExecutor};

/// Whether the journeys step traces the code (`APP_MAP_JOURNEY_TRACER=on`)
/// or only picks from the candidates (the default until the tracer passes
/// the gold set's gate).
pub fn enabled() -> bool {
    std::env::var("APP_MAP_JOURNEY_TRACER").is_ok_and(|v| v == "on")
}

/// Tool calls one journey's trace may make.
pub const MAX_CALLS: usize = 25;
/// Journeys one entry may start (sign in and sign up from one landing).
pub const MAX_PER_ENTRY: usize = 3;
/// Tool calls the one retry adds.
pub const RETRY_CALLS: usize = 40;

/// Why a submitted journey looks cut short: it ends where a form hands its
/// work to the screen around it, or it reaches no outside service and no
/// server code at all.
pub fn stops_short(j: &Value) -> Option<String> {
    let steps = j["steps"].as_array()?;
    let last = steps.last()?;
    let tok = last["token"].as_str().unwrap_or("");
    if last["kind"] == "screen"
        && regex::Regex::new(r"^(props\.)?on[A-Z]\w*")
            .ok()?
            .is_match(tok)
    {
        return Some(format!("\"{}\" ends where the form hands its work to `{tok}`; follow that into the parent's handler.", j["title"].as_str().unwrap_or("A journey")));
    }
    let server = steps
        .iter()
        .any(|s| matches!(s["kind"].as_str(), Some("handler" | "service" | "outside")));
    if !server {
        return Some(format!(
            "\"{}\" reaches no server code or outside service.",
            j["title"].as_str().unwrap_or("A journey")
        ));
    }
    None
}
/// Journeys traced at once.
pub const CONCURRENCY: usize = 6;
/// How long one journey's trace may take.
pub const JOURNEY_SECS: u64 = 240;
/// The map lookups the tracer gets.
const MAP_TOOLS: [&str; 4] = ["map_screen", "map_file", "map_part", "map_impact"];
/// What a step can be.
pub const KINDS: [&str; 6] = [
    "screen", "call", "handler", "service", "outside", "callback",
];

pub const SYSTEM: &str = "You trace one user journey through a codebase for its app map, the way a person reading the code would, and give it as cited steps.

Start at the entry screen you are given. Follow what the user does there, in the order the code runs it:
- screen: the screen or form the user is on (cite its component or where it renders);
- call: where the code makes a request (the fetch, the client method, the SDK or RPC call). Cite the line that spells the request's path or RPC name, and make the token that path or name as written (`/api/auth/signup`, `com.example.server.createSession`);
- handler: the server code that answers that request (the route file's exported method, the controller method's decorator, the API handler);
- service: the server's own code the handler runs that matters to the journey (the service method that does the work);
- outside: where the code reaches an outside service (the client call, its SDK import, or the setting that holds its address; make the token the service's setting or package name, like `STRIPE_SECRET_KEY` or `@stripe/stripe-js`);
- callback: a webhook or event handler that runs later because of this journey;
- then the next screen the user lands on, and on through each request until the journey's outcome (signed in, booked, paid, the password changed).

- When a form hands its work to the screen around it (it calls a prop such as `onContinue(email, password)` or `onSubmit`), go to where that prop is passed (the parent renders `<Form onContinue={handleLogin} />`) and follow the parent's handler: that is where the requests are made.

How to work:
- Read the code with read_file, grep and list_files. The map lookups (map_screen for a screen's requests and the server code behind them, map_file, map_part, map_impact) tell you what the map already found; use them to save reads, then read the lines you cite.
- Each step cites one line: its file (the path as the tools show it), the line number, and a token copied exactly from that line (an identifier, a path string, a component name). Cite only lines you have read. Never guess a path or a line.
- Give every step on the way, not a summary: a request and its handler are two steps; a service the handler calls and the outside service behind it are two more. A journey usually has 8 to 15 steps: follow each request of the journey to its handler and on to the outside service, and go on to the screen the user lands on; don't stop after the first request.
- At most 25 steps. Stop at the journey's outcome.
- If this screen starts more than one journey of these kinds (sign in, sign up, password reset, booking, checkout), trace each one and submit each separately with submit (at most 3).
- You have about 25 tool calls a journey. Submit with submit before they run out; if it says a step is wrong, fix that step and submit again.";

/// The tools: the snapshot's three, four map lookups and submit.
pub fn tools() -> Vec<ToolDefinition> {
    let mut out: Vec<ToolDefinition> = super::app_map::tools()
        .into_iter()
        .filter(|t| t.name != "submit")
        .collect();
    for t in am::tool_defs() {
        let name = t["name"].as_str().unwrap_or("");
        if MAP_TOOLS.contains(&name) {
            out.push(ToolDefinition {
                name: name.to_string(),
                description: t["description"].as_str().unwrap_or("").to_string(),
                input_schema: t["input_schema"].clone(),
            });
        }
    }
    out.push(ToolDefinition {
        name: "submit".into(),
        description: "Give the journey: a short title (like \"Sign in on the web\") and its steps in order, each citing one line it rests on.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "steps": {"type": "array", "maxItems": fl::MAX_STEPS, "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": KINDS},
                        "file": {"type": "string"},
                        "line": {"type": "integer"},
                        "token": {"type": "string", "description": "Copied exactly from the cited line."},
                        "note": {"type": "string", "description": "What happens at this step, in a few plain words."}
                    },
                    "required": ["kind", "file", "line", "token", "note"]
                }}
            },
            "required": ["title", "steps"]
        }),
    });
    out
}

/// The line (1-based) within 3 of `line` that holds `token`.
pub fn token_line(text: &str, line: u64, token: &str) -> Option<u64> {
    if token.trim().is_empty() || line == 0 {
        return None;
    }
    let lines: Vec<&str> = text.lines().collect();
    let at = line as i64;
    let mut best: Option<(i64, u64)> = None;
    for d in -3i64..=3 {
        let l = at + d;
        if l >= 1 && (l as usize) <= lines.len() && lines[l as usize - 1].contains(token) {
            let score = d.abs();
            if best.is_none_or(|(s, _)| score < s) {
                best = Some((score, l as u64));
            }
        }
    }
    best.map(|(_, l)| l)
}

/// A submitted journey, checked against the snapshot: every step's file
/// exists and its line (within 3) holds its token. Returns the steps as
/// cited (lines moved onto the token) or what is wrong.
pub fn check_journey(snapshot: &Snapshot, input: &Value) -> Result<Value, Vec<String>> {
    let mut problems = Vec::new();
    let mut steps = Vec::new();
    let given = input["steps"].as_array().cloned().unwrap_or_default();
    if given.is_empty() {
        problems.push("steps is empty".to_string());
    }
    for (i, s) in given.iter().take(fl::MAX_STEPS).enumerate() {
        let n = i + 1;
        let kind = s["kind"].as_str().unwrap_or("");
        let file = s["file"].as_str().unwrap_or("").trim_start_matches("./");
        let token = s["token"].as_str().unwrap_or("").trim();
        let line = s["line"].as_u64().unwrap_or(0);
        if !KINDS.contains(&kind) {
            problems.push(format!(
                "step {n}: kind must be one of {}",
                KINDS.join(", ")
            ));
            continue;
        }
        let Some(b) = snapshot.files.get(file) else {
            problems.push(format!(
                "step {n}: there is no file {file} (paths are as list_files shows them)"
            ));
            continue;
        };
        if token.is_empty() || token.len() > 160 {
            problems.push(format!(
                "step {n}: the token must be a short piece of the cited line"
            ));
            continue;
        }
        let Some(at) = token_line(&String::from_utf8_lossy(b), line, token) else {
            problems.push(format!(
                "step {n}: {file}:{line} doesn't hold `{token}` (within 3 lines); read the file and cite the line that holds it"
            ));
            continue;
        };
        steps.push(json!({"kind": kind, "file": file, "line": at, "token": token, "note": fl::plain(s["note"].as_str().unwrap_or(""), 160)}));
    }
    if given.len() > fl::MAX_STEPS {
        problems.push(format!("at most {} steps", fl::MAX_STEPS));
    }
    if steps.first().is_some_and(|s| s["kind"] != "screen") {
        problems.push("the first step is the entry screen".to_string());
    }
    if problems.is_empty() {
        Ok(json!({"title": fl::plain(input["title"].as_str().unwrap_or(""), 80), "steps": steps}))
    } else {
        Err(problems)
    }
}

/// One journey's tools over the snapshot and the map.
pub struct TraceTools<'a> {
    pub snapshot: &'a Snapshot,
    pub lookups: &'a Mutex<am::Lookups>,
    pub answers: Mutex<Vec<Value>>,
    pub calls: AtomicUsize,
    /// Tool calls added for the one retry.
    pub extra: AtomicUsize,
    pub retried: std::sync::atomic::AtomicBool,
    /// What was accepted before the retry.
    pub before: Mutex<Vec<Value>>,
}

impl<'a> TraceTools<'a> {
    pub fn new(snapshot: &'a Snapshot, lookups: &'a Mutex<am::Lookups>) -> Self {
        Self {
            snapshot,
            lookups,
            answers: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            extra: AtomicUsize::new(0),
            retried: std::sync::atomic::AtomicBool::new(false),
            before: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl ToolExecutor for TraceTools<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        let done = self.answers.lock().unwrap_or_else(|e| e.into_inner()).len();
        if name != "submit"
            && n > MAX_CALLS * (done + 1).min(MAX_PER_ENTRY)
                + 5
                + self.extra.load(Ordering::Relaxed)
        {
            return Ok(json!(
                "Out of tool calls: submit the journey now with the steps you have read."
            ));
        }
        let text = match name {
            "read_file" => self.snapshot.read_file(input),
            "list_files" => self.snapshot.list_files(input),
            "grep" => self.snapshot.grep(input),
            "submit" => match check_journey(self.snapshot, input) {
                Ok(j) => {
                    let mut a = self.answers.lock().unwrap_or_else(|e| e.into_inner());
                    if a.len() >= MAX_PER_ENTRY {
                        "Not accepted: three journeys from one screen is the most. Reply with one short sentence and stop.".to_string()
                    } else {
                        a.push(j);
                        "Accepted. If this screen starts another journey you haven't given, trace and submit it; otherwise reply with one short sentence and stop.".to_string()
                    }
                }
                Err(p) => format!(
                    "Not accepted. Fix these steps and submit again:\n- {}",
                    p.join("\n- ")
                ),
            },
            m if MAP_TOOLS.contains(&m) => {
                let (v, _) = self
                    .lookups
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .lookup(m, input);
                common::truncate_str(&v.to_string(), 20_000).to_string()
            }
            _ => format!("No tool {name}."),
        };
        Ok(json!(text))
    }
}

/// What the map's candidates say near an entry: the entry, then what its
/// code leads to, two hops out (a head start, not the answer).
pub fn hints(todo: &Value, entry: &str) -> String {
    let table = &todo["table"];
    let line = |id: &str| {
        let c = &table[id];
        let at = c["evidence"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|e| format!("{}:{}", e["file"].as_str().unwrap_or(""), e["line"]))
            .collect::<Vec<_>>()
            .join(" then ");
        format!(
            "- {} {} @ {}",
            c["kind"].as_str().unwrap_or(""),
            c["label"].as_str().unwrap_or(""),
            at
        )
    };
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let mut frontier = vec![entry.to_string()];
    for _ in 0..3 {
        let mut next = Vec::new();
        for id in frontier {
            if !seen.insert(id.clone()) || table[&id].is_null() {
                continue;
            }
            out.push(line(&id));
            for b in table[&id]["next"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|x| x.as_str())
            {
                next.push(b.to_string());
            }
        }
        frontier = next;
        if out.len() >= 40 {
            break;
        }
    }
    out.truncate(40);
    out.join("\n")
}

/// The kind of journey a name speaks of (as the sandbox's `WORDS`).
pub fn family_of(text: &str) -> Option<&'static str> {
    const WORDS: [(&str, &str); 5] = [
        ("sign in", r"(?i)(log[-_ ]?in|sign[-_ ]?in|authenticat)"),
        (
            "sign up",
            r"(?i)(sign[-_ ]?up|register|create[-_ ]?account|request[-_ ]?membership|join|onboard)",
        ),
        (
            "password reset",
            r"(?i)(forgot|reset[-_ ]?password|recover|password[-_ ]?update|new[-_ ]?password|set[-_ ]?password)",
        ),
        (
            "booking",
            r"(?i)(book(ing)?|reserv|class[-_ ]?detail|schedul|appointment)",
        ),
        (
            "checkout",
            r"(?i)(checkout|purchase|payment|basket|cart|buy|subscri|billing|clipcard|credits?\b)",
        ),
    ];
    WORDS.iter().find_map(|(f, re)| {
        regex::Regex::new(re)
            .ok()
            .filter(|r| r.is_match(text))
            .map(|_| *f)
    })
}

/// The journeys an entry starts: its own kind, and the kinds of the screens
/// and forms it leads to directly (a sign-in landing that opens sign-up and
/// forgot-password forms starts those journeys too).
pub fn journeys_from(todo: &Value, entry: &Value) -> Vec<String> {
    let table = &todo["table"];
    let id = entry["id"].as_str().unwrap_or("");
    let own = entry["family"].as_str().unwrap_or("").replace('-', " ");
    // Sign in, sign up and password reset start from the same screens, and
    // booking and checkout do: a screen starts the journeys of its own
    // group that the screens within two steps of it are named for.
    const ACCOUNT: [&str; 3] = ["sign in", "sign up", "password reset"];
    let group = |f: &str| ACCOUNT.contains(&f);
    let mut out = vec![own.clone()];
    let mut frontier = vec![id.to_string()];
    let mut seen = BTreeSet::new();
    for _ in 0..2 {
        let mut next = Vec::new();
        for x in frontier {
            if !seen.insert(x.clone()) {
                continue;
            }
            for b in table[&x]["next"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|b| b.as_str())
            {
                if table[b]["kind"] == "screen" {
                    // The screen's own name, not the page it is a step of.
                    let label = table[b]["label"].as_str().unwrap_or("");
                    let name = label.split(" (").next().unwrap_or(label);
                    if let Some(f) = family_of(name) {
                        if group(f) == group(&own) && !out.iter().any(|o| o == f) {
                            out.push(f.to_string());
                        }
                    }
                    next.push(b.to_string());
                }
            }
        }
        frontier = next;
    }
    out.retain(|f| !f.is_empty());
    out.truncate(MAX_PER_ENTRY);
    out
}

/// The prompt for one entry.
pub fn user_prompt(todo: &Value, entry: &Value) -> String {
    let id = entry["id"].as_str().unwrap_or("");
    let c = &todo["table"][id];
    let ev = &c["evidence"][0];
    let kinds = journeys_from(todo, entry);
    let which = if kinds.len() > 1 {
        format!("Trace each of these journeys that starts at this screen, one submit each: {}. Skip one only if the code shows this screen doesn't start it.", kinds.join(", "))
    } else {
        format!(
            "Trace the {} journey that starts at this screen.",
            kinds.first().map(String::as_str).unwrap_or("user")
        )
    };
    format!(
        "{which} The app is {}.\n{} @ {}:{} (`{}`)\n\nWhat the map found near it (may be incomplete or include things this journey doesn't use):\n{}",
        entry["app"].as_str().unwrap_or("the app"),
        c["label"].as_str().unwrap_or(id),
        ev["file"].as_str().unwrap_or(""),
        ev["line"],
        ev["token"].as_str().unwrap_or(""),
        hints(todo, id)
    )
}

/// The candidate a traced step lands on: one that cites the same file
/// within 3 lines, of a kind the step can be.
fn node_for(table: &Value, step: &Value) -> Option<String> {
    let file = step["file"].as_str()?;
    let line = step["line"].as_i64()?;
    let want: &[&str] = match step["kind"].as_str()? {
        "screen" => &["screen"],
        "call" | "handler" => &["request"],
        "service" | "outside" => &["outside"],
        _ => &["async", "outside"],
    };
    let mut best: Option<(i64, String)> = None;
    for (id, c) in table.as_object()? {
        if !want.contains(&c["kind"].as_str().unwrap_or("")) {
            continue;
        }
        for e in c["evidence"].as_array().into_iter().flatten() {
            if e["file"].as_str() == Some(file) {
                let d = (e["line"].as_i64().unwrap_or(-99) - line).abs();
                if d <= 3 && best.as_ref().is_none_or(|(b, _)| d < *b) {
                    best = Some((d, id.clone()));
                }
            }
        }
    }
    best.map(|(_, id)| id)
}

/// How a trace's order compared with the code's evidence.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Traced {
    pub entries: usize,
    pub traced: usize,
    pub skipped_covered: usize,
    pub failed: usize,
    pub steps: usize,
    pub on_map: usize,
    pub rejected_submits: usize,
    pub order_conflicts: usize,
    pub tool_calls: usize,
}

/// A trace as a stored journey (`flows.json`'s shape): steps on map nodes
/// where the candidates cite the same lines, and those nodes put in the
/// order the code's evidence gives where it gives one. Returns the journey
/// and how many pairs of steps evidence reordered.
pub fn to_flow(
    journey: &Value,
    entry: &Value,
    todo: &Value,
    evidence_order: bool,
) -> (Value, usize) {
    let table = &todo["table"];
    let mut steps: Vec<Value> = Vec::new();
    let mut ids: Vec<Option<String>> = Vec::new();
    for s in journey["steps"].as_array().into_iter().flatten() {
        let id = node_for(table, s);
        let role = s["kind"].as_str().unwrap_or("");
        // The same node in the same role twice is one step.
        if id.is_some()
            && ids
                .iter()
                .zip(&steps)
                .any(|(x, st)| x == &id && st["role"] == role)
        {
            continue;
        }
        let ev = json!([{"file": s["file"], "line": s["line"], "token": s["token"]}]);
        let step = match &id {
            Some(id) => {
                let c = &table[id];
                json!({"key": c["key"], "kind": c["kind"], "role": role, "view": c["view"], "note": s["note"], "evidence": ev, "linked": true})
            }
            None => {
                let file = s["file"].as_str().unwrap_or("");
                let kind = match role {
                    "screen" => "screen",
                    "call" | "handler" => "request",
                    "service" | "outside" => "outside",
                    _ => "async",
                };
                json!({"key": format!("code:{file}#{}", s["token"].as_str().unwrap_or("")), "kind": kind, "role": role, "view": format!("f:{file}"), "note": s["note"], "evidence": ev, "linked": true})
            }
        };
        steps.push(step);
        ids.push(id);
    }
    // The entry is the screen the trace was asked about.
    let entry_id = entry["id"].as_str().unwrap_or("");
    if let Some(first) = steps.first_mut() {
        if ids[0].as_deref() != Some(entry_id) && !table[entry_id].is_null() {
            first["key"] = table[entry_id]["key"].clone();
            first["view"] = table[entry_id]["view"].clone();
            ids[0] = Some(entry_id.to_string());
        }
    }
    // Units: steps in a row on the same node (a request's call, then its
    // handler) move together.
    let mut units: Vec<(Option<String>, Vec<Value>)> = Vec::new();
    for (id, st) in ids.into_iter().zip(steps) {
        match units.last_mut() {
            Some((last, v)) if id.is_some() && *last == id => v.push(st),
            _ => units.push((id, vec![st])),
        }
    }
    let mut conflicts = 0;
    let mut seen = BTreeSet::new();
    let on_map: Vec<(usize, String)> = units
        .iter()
        .enumerate()
        .filter_map(|(i, (x, _))| x.clone().filter(|x| seen.insert(x.clone())).map(|x| (i, x)))
        .collect();
    // Evidence orders a request against the server work behind it, within
    // the stretch between two screens; where the user goes is the trace's.
    if evidence_order {
        let is_screen =
            |u: &(Option<String>, Vec<Value>)| u.1.first().is_some_and(|s| s["kind"] == "screen");
        let mut start = 0;
        while start < units.len() {
            let mut end = start;
            while end < units.len() && !is_screen(&units[end]) {
                end += 1;
            }
            let seg: Vec<(usize, String)> = on_map
                .iter()
                .filter(|(i, _)| *i >= start && *i < end)
                .cloned()
                .collect();
            if seg.len() > 1 {
                let refs: Vec<String> = seg.iter().map(|(_, x)| x.clone()).collect();
                // The screen before the stretch says which of its requests
                // its code makes first; it stays where it is.
                let screen = start
                    .checked_sub(1)
                    .and_then(|i| units[i].0.clone())
                    .filter(|s| !refs.contains(s));
                let with: Vec<String> =
                    screen.iter().cloned().chain(refs.iter().cloned()).collect();
                let (ord, _, _, _) = fl::order(&with, table);
                let ord: Vec<String> = ord.into_iter().filter(|x| refs.contains(x)).collect();
                let pos = |x: &String| refs.iter().position(|r| r == x).unwrap_or(0);
                let mut here = 0;
                for a in 0..ord.len() {
                    for b in a + 1..ord.len() {
                        if pos(&ord[a]) > pos(&ord[b]) {
                            here += 1;
                        }
                    }
                }
                if here > 0 {
                    conflicts += here;
                    let by_id: BTreeMap<String, (Option<String>, Vec<Value>)> = seg
                        .iter()
                        .map(|(i, x)| (x.clone(), units[*i].clone()))
                        .collect();
                    for ((slot, _), x) in seg.iter().zip(&ord) {
                        units[*slot] = by_id[x].clone();
                    }
                }
            }
            start = end + 1;
        }
    }
    let steps: Vec<Value> = units.into_iter().flat_map(|(_, v)| v).collect();
    let title = journey["title"].as_str().unwrap_or("").to_string();
    let title = if title.is_empty() {
        "Journey".to_string()
    } else {
        title
    };
    let flow = json!({
        "id": fl::slug(&title),
        "title": title,
        "entry": table[entry_id]["key"],
        "origin": "trace",
        "steps": steps,
    });
    (flow, conflicts)
}

/// One entry's traces: its journeys, with how many pairs of steps the
/// evidence reordered and the trace as the model gave it.
type EntryTraces = (Value, Vec<(Value, usize, Value)>);

/// What tracing a todo gave.
pub struct TraceOut {
    /// The stored journeys (ids made unique).
    pub flows: Vec<Value>,
    pub report: Traced,
    /// Entries whose trace failed (for the fallback).
    pub failed: Vec<Value>,
    /// Entries not started before `stop` (for the next step).
    pub left: Vec<Value>,
    /// The traces as the model gave them.
    pub raw: Vec<Value>,
}

/// Traces the todo's entries, a few at a time, with `tools` (as the
/// policy offers them). An entry that is already a step of a journey traced
/// in the same family and app is not traced again; none is started after
/// `stop`.
#[allow(clippy::too_many_arguments)]
pub async fn trace_all(
    client: &crate::agent::anthropic::AnthropicClient,
    model_id: &str,
    gate: &common::agent_policy::Gate,
    tools: &[ToolDefinition],
    snapshot: &Snapshot,
    graph: Value,
    todo: &Value,
    usage: &mut crate::models::TokenUsage,
    evidence_order: bool,
    stop: Option<std::time::Instant>,
) -> TraceOut {
    let lookups = Mutex::new(am::Lookups::new(graph));
    let entries: Vec<Value> = todo["entries"].as_array().cloned().unwrap_or_default();
    let covered: Mutex<BTreeSet<(String, String, String)>> = Mutex::new(BTreeSet::new());
    let rep = Mutex::new(Traced {
        entries: entries.len(),
        ..Default::default()
    });
    let usage_all = Mutex::new(crate::models::TokenUsage::default());
    let left = Mutex::new(Vec::new());
    let results: Vec<EntryTraces> = futures::stream::iter(entries.iter().cloned())
        .map(|entry| {
            let lookups = &lookups;
            let covered = &covered;
            let rep = &rep;
            let usage_all = &usage_all;
            let left = &left;
            async move {
                if stop.is_some_and(|s| std::time::Instant::now() >= s) {
                    left.lock().unwrap_or_else(|e| e.into_inner()).push(entry["id"].clone());
                    return (entry, Vec::new());
                }
                let key = |e: &Value| {
                    (
                        e["family"].as_str().unwrap_or("").to_string(),
                        e["app"].as_str().unwrap_or("").to_string(),
                        e["id"].as_str().unwrap_or("").to_string(),
                    )
                };
                if covered.lock().unwrap_or_else(|e| e.into_inner()).contains(&key(&entry)) {
                    rep.lock().unwrap_or_else(|e| e.into_inner()).skipped_covered += 1;
                    return (entry, Vec::new());
                }
                let exec = TraceTools::new(snapshot, lookups);
                let mut messages = vec![(
                    "user".to_string(),
                    vec![json!({"type": "text", "text": user_prompt(todo, &entry)})],
                )];
                let mut u = crate::models::TokenUsage::default();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(JOURNEY_SECS);
                // A trace that ends without submitting is asked once to
                // submit; one that stops at a hand-off or before any outside
                // service is asked once to go on, with more tool calls.
                for nudge in 0..3 {
                    if nudge > 0 {
                        let got = exec.answers.lock().unwrap_or_else(|e| e.into_inner()).clone();
                        let text = if got.is_empty() {
                            if nudge == 2 { break; }
                            "Submit the journey now with submit, citing the lines you have read.".to_string()
                        } else if let Some(why) = got.iter().find_map(stops_short) {
                            if exec.retried.swap(true, Ordering::Relaxed) { break; }
                            exec.extra.store(RETRY_CALLS, Ordering::Relaxed);
                            exec.answers.lock().unwrap_or_else(|e| e.into_inner()).clear();
                            *exec.before.lock().unwrap_or_else(|e| e.into_inner()) = got;
                            format!("{why} Go on reading: follow the code further and submit each journey again, whole, from the entry screen to its outcome. You have about {RETRY_CALLS} more tool calls.")
                        } else {
                            break;
                        };
                        messages.push(("user".to_string(), vec![json!({"type": "text", "text": text})]));
                    }
                    let run = crate::agent::anthropic::converse_tool_loop(
                        client,
                        model_id,
                        SYSTEM,
                        &mut messages,
                        tools,
                        &exec,
                        gate,
                        &mut u,
                        MAX_CALLS * MAX_PER_ENTRY + RETRY_CALLS + 8,
                        8_000,
                        Some(deadline),
                        None,
                        None,
                        None,
                        crate::agent::llm::AnswerForm::Auto,
                    );
                    match tokio::time::timeout(std::time::Duration::from_secs(JOURNEY_SECS + 30), run).await {
                        Ok(Err(e)) => tracing::warn!(entry = %entry["id"], error = %e, "App map: a journey's trace failed"),
                        Err(_) => break,
                        _ => {}
                    }
                }
                if let Ok(d) = std::env::var("APP_MAP_TRACE_LOG") {
                    let _ = std::fs::write(
                        format!("{d}/{}.json", entry["id"].as_str().unwrap_or("x")),
                        serde_json::to_vec_pretty(&json!(messages.iter().map(|(r, b)| json!({"role": r, "content": b})).collect::<Vec<_>>())).unwrap_or_default(),
                    );
                }
                usage_all.lock().unwrap_or_else(|e| e.into_inner()).merge(&u);
                let rejected = messages
                    .iter()
                    .flat_map(|(_, b)| b.iter())
                    .filter(|b| b["type"] == "tool_result" && b.to_string().contains("Not accepted."))
                    .count();
                let calls = exec.calls.load(Ordering::Relaxed);
                let mut answers = exec.answers.into_inner().unwrap_or_else(|e| e.into_inner());
                // A retry that gave nothing keeps what came before.
                if answers.is_empty() {
                    answers = exec.before.into_inner().unwrap_or_else(|e| e.into_inner());
                }
                let mut r = rep.lock().unwrap_or_else(|e| e.into_inner());
                r.rejected_submits += rejected;
                r.tool_calls += calls;
                if answers.is_empty() {
                    r.failed += 1;
                    return (entry, Vec::new());
                }
                r.traced += 1;
                drop(r);
                let made: Vec<(Value, usize, Value)> = answers
                    .into_iter()
                    .map(|j| {
                        let (f, c) = to_flow(&j, &entry, todo, evidence_order);
                        (f, c, j)
                    })
                    .collect();
                // The entries this journey passes through are covered.
                let (fam, app, _) = key(&entry);
                let keys: BTreeSet<String> = made
                    .iter()
                    .flat_map(|(f, _, _)| f["steps"].as_array().cloned().unwrap_or_default())
                    .filter_map(|s| s["key"].as_str().map(str::to_string))
                    .collect();
                let mut cov = covered.lock().unwrap_or_else(|e| e.into_inner());
                for e in todo["entries"].as_array().into_iter().flatten() {
                    if e["key"].as_str().is_some_and(|k| keys.contains(k)) {
                        cov.insert((fam.clone(), app.clone(), e["id"].as_str().unwrap_or("").to_string()));
                    }
                }
                (entry, made)
            }
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;
    usage.merge(&usage_all.into_inner().unwrap_or_else(|e| e.into_inner()));
    let mut rep = rep.into_inner().unwrap_or_else(|e| e.into_inner());
    let mut flows = Vec::new();
    let mut failed = Vec::new();
    let mut ids = BTreeSet::new();
    let mut raw = Vec::new();
    // In the todo's order, whatever order they finished in.
    let order: BTreeMap<String, usize> = entries
        .iter()
        .enumerate()
        .map(|(i, e)| (e["id"].as_str().unwrap_or("").to_string(), i))
        .collect();
    let mut results = results;
    results.sort_by_key(|(e, _)| {
        order
            .get(e["id"].as_str().unwrap_or(""))
            .copied()
            .unwrap_or(0)
    });
    let left_ids = left.into_inner().unwrap_or_else(|e| e.into_inner());
    let mut left = Vec::new();
    for (entry, got) in results {
        if left_ids.contains(&entry["id"]) {
            left.push(entry);
            continue;
        }
        if got.is_empty() {
            if !rep_skipped(&entry, &flows) {
                failed.push(entry);
            }
            continue;
        }
        let mut kept = 0;
        for (mut flow, conflicts, j) in got {
            raw.push(json!({"entry": entry, "journey": j}));
            let steps = flow["steps"].as_array().map(Vec::len).unwrap_or(0);
            if steps < 3 || flow["steps"][0]["kind"] != "screen" {
                continue;
            }
            kept += 1;
            rep.steps += steps;
            rep.on_map += flow["steps"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|s| !s["key"].as_str().unwrap_or("").starts_with("code:"))
                .count();
            rep.order_conflicts += conflicts;
            let base = flow["id"].as_str().unwrap_or("journey").to_string();
            let mut id = base.clone();
            let mut n = 2;
            while !ids.insert(id.clone()) {
                id = format!("{base}-{n}");
                n += 1;
            }
            flow["id"] = json!(id);
            flows.push(flow);
        }
        if kept == 0 {
            rep.failed += 1;
            failed.push(entry);
        }
    }
    TraceOut {
        flows,
        report: rep,
        failed,
        left,
        raw,
    }
}

/// Whether an entry was left untraced because a traced journey covers it.
fn rep_skipped(entry: &Value, flows: &[Value]) -> bool {
    let k = entry["key"].as_str().unwrap_or("");
    flows.iter().any(|f| {
        f["steps"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|s| s["key"].as_str() == Some(k))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Snapshot {
        let mut files = BTreeMap::new();
        files.insert(
            "harbor-web/app/login/page.tsx".to_string(),
            b"import { signIn } from '../lib/session';\n\nexport default function LoginPage() {\n  return <form onSubmit={() => signIn()} />;\n}\n".to_vec(),
        );
        files.insert(
            "harbor-web/lib/session.ts".to_string(),
            b"export async function signIn() {\n  await fetch('/api/session', { method: 'POST' });\n}\n".to_vec(),
        );
        Snapshot { files }
    }

    /// The tracer on the gold set (scratchpad only; customer code never
    /// leaves it): `APP_MAP_TRACE_EVAL=<dir>` holds `<set>.todo.json`,
    /// `<set>.graph.json` and `roots/<set>/`. Writes `<set>.flows.json`,
    /// `<set>.traces.json` (the raw traces) and `<set>.usage.json`. With
    /// `<set>.traces.json` already there, re-checks those traces with no
    /// model call. `TRACE_ORDER=trace` keeps the trace's own order.
    #[tokio::test]
    #[ignore]
    async fn live_traces_on_the_gold_set() {
        let dir = std::path::PathBuf::from(
            std::env::var("APP_MAP_TRACE_EVAL").expect("APP_MAP_TRACE_EVAL"),
        );
        let evidence_order = std::env::var("TRACE_ORDER").map_or(true, |v| v != "trace");
        let (client, model) = super::super::review_eval::eval_client().await;
        if std::env::var("EVAL_PROVIDER").is_ok_and(|p| p == "bedrock") {
            common::model_provider::set_current(
                common::model_provider::Settings {
                    provider: common::model_provider::Provider::Bedrock,
                    ..Default::default()
                },
                common::model_provider::Source::Deploy,
            );
        }
        let only: Vec<String> = std::env::var("TRACE_SETS")
            .map(|v| v.split(',').map(String::from).collect())
            .unwrap_or_default();
        let mut sets: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_suffix(".todo.json")
                    .map(String::from)
            })
            .filter(|s| only.is_empty() || only.contains(s))
            .collect();
        sets.sort();
        for set in sets {
            let todo: Value = serde_json::from_slice(
                &std::fs::read(dir.join(format!("{set}.todo.json"))).unwrap(),
            )
            .unwrap();
            let started = std::time::Instant::now();
            let mut usage = crate::models::TokenUsage::default();
            let saved = std::fs::read(dir.join(format!("{set}.traces.json")))
                .ok()
                .and_then(|b| serde_json::from_slice::<Vec<Value>>(&b).ok());
            let (flows, rep, raw) = if let Some(saved) = saved {
                let mut rep = Traced::default();
                let flows: Vec<Value> = saved
                    .iter()
                    .map(|t| {
                        let (f, c) = to_flow(&t["journey"], &t["entry"], &todo, evidence_order);
                        rep.order_conflicts += c;
                        f
                    })
                    .collect();
                (flows, rep, saved)
            } else {
                let mut files = BTreeMap::new();
                let root = dir.join("roots").join(&set);
                let mut stack = vec![root.clone()];
                while let Some(d) = stack.pop() {
                    for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                        let p = e.path();
                        let rel = p.strip_prefix(&root).unwrap().to_string_lossy().to_string();
                        if rel.starts_with("floorplan")
                            || rel.contains("node_modules")
                            || rel.ends_with(".git")
                        {
                            continue;
                        }
                        if p.is_dir() {
                            stack.push(p);
                        } else if e.metadata().map(|m| m.len() < 1_000_000).unwrap_or(false) {
                            files.insert(rel, std::fs::read(&p).unwrap_or_default());
                        }
                    }
                }
                let snapshot = Snapshot { files };
                let graph: Value = serde_json::from_slice(
                    &std::fs::read(dir.join(format!("{set}.graph.json"))).unwrap(),
                )
                .unwrap();
                let gate = common::agent_policy::Gate::new(
                    std::sync::Arc::new(common::agent_policy::store::MemStore::new()),
                    common::agent_policy::GateCtx::new(
                        "trace-eval",
                        common::agent_policy::Agent::Planner,
                    ),
                )
                .offering(tools().iter().map(|t| t.name.clone()));
                let out = trace_all(
                    &client,
                    &model,
                    &gate,
                    &tools(),
                    &snapshot,
                    graph,
                    &todo,
                    &mut usage,
                    evidence_order,
                    None,
                )
                .await;
                (out.flows, out.report, out.raw)
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
                dir.join(format!("{set}.traces.json")),
                serde_json::to_vec_pretty(&raw).unwrap(),
            )
            .unwrap();
            if usage.cost_usd > 0.0 {
                std::fs::write(
                    dir.join(format!("{set}.usage.json")),
                    serde_json::to_vec_pretty(&json!({"model": model, "input": usage.input_tokens, "cache_read": usage.cache_read_tokens, "cache_write": usage.cache_write_tokens, "output": usage.output_tokens, "cost_usd": usage.cost_usd, "traced": rep, "secs": started.elapsed().as_secs_f64()})).unwrap(),
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn a_trace_keeps_only_steps_whose_line_holds_their_token() {
        let s = snapshot();
        let ok = json!({"title": "Sign in", "steps": [
            {"kind": "screen", "file": "harbor-web/app/login/page.tsx", "line": 2, "token": "LoginPage", "note": "The sign-in page."},
            {"kind": "call", "file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/session", "note": "Posts the session."},
        ]});
        let j = check_journey(&s, &ok).unwrap();
        // The line moves onto the token.
        assert_eq!(j["steps"][0]["line"], 3);
        let bad = json!({"title": "Sign in", "steps": [
            {"kind": "screen", "file": "harbor-web/app/login/page.tsx", "line": 3, "token": "LoginPage", "note": ""},
            {"kind": "call", "file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/sessions/new", "note": ""},
            {"kind": "handler", "file": "harbor-api/src/session.ts", "line": 1, "token": "POST", "note": ""},
        ]});
        let p = check_journey(&s, &bad).unwrap_err();
        assert_eq!(p.len(), 2);
        assert!(p[0].starts_with("step 2: harbor-web/lib/session.ts:2 doesn't hold"));
        assert!(p[1].starts_with("step 3: there is no file harbor-api/src/session.ts"));
    }

    #[test]
    fn traced_steps_land_on_map_nodes_and_evidence_orders_them() {
        let todo = json!({"table": {
            "S1": {"key": "screen:web:/login", "kind": "screen", "view": "f:harbor-web/app/login/page.tsx", "label": "web /login", "next": ["R1", "R2"],
                   "evidence": [{"file": "harbor-web/app/login/page.tsx", "line": 3, "token": "LoginPage"}]},
            "R1": {"key": "req:GET /csrf", "kind": "request", "view": "r:1", "label": "GET /csrf", "next": [],
                   "evidence": [{"file": "harbor-web/lib/csrf.ts", "line": 1, "token": "/csrf"}]},
            "R2": {"key": "req:POST /api/session", "kind": "request", "view": "r:2", "label": "POST /api/session", "next": [],
                   "evidence": [{"file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/session"}]},
        }});
        let entry =
            json!({"id": "S1", "key": "screen:web:/login", "family": "sign-in", "app": "web"});
        // The trace puts the session post before the token fetch, and adds a
        // step the map has no node for.
        let journey = json!({"title": "Sign in", "steps": [
            {"kind": "screen", "file": "harbor-web/app/login/page.tsx", "line": 3, "token": "LoginPage", "note": "a"},
            {"kind": "call", "file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/session", "note": "b"},
            {"kind": "outside", "file": "harbor-api/src/okta.ts", "line": 4, "token": "OKTA_URL", "note": "c"},
            {"kind": "call", "file": "harbor-web/lib/csrf.ts", "line": 1, "token": "/csrf", "note": "d"},
        ]});
        let (flow, conflicts) = to_flow(&journey, &entry, &todo, true);
        let keys: Vec<&str> = flow["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["key"].as_str().unwrap())
            .collect();
        // The screen's code makes the token fetch first: evidence wins.
        assert_eq!(
            keys,
            [
                "screen:web:/login",
                "req:GET /csrf",
                "code:harbor-api/src/okta.ts#OKTA_URL",
                "req:POST /api/session"
            ]
        );
        assert_eq!(conflicts, 1);
        assert_eq!(flow["origin"], "trace");
        // Without the cross-check, the trace's own order.
        let (flow, conflicts) = to_flow(&journey, &entry, &todo, false);
        assert_eq!(flow["steps"][1]["key"], "req:POST /api/session");
        assert_eq!(conflicts, 0);
    }
}
