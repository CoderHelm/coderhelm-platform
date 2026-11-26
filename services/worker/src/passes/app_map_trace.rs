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

/// Whether the journeys step traces the code (the default) or only picks
/// from the candidates (`APP_MAP_JOURNEY_TRACER=off`).
pub fn enabled() -> bool {
    std::env::var("APP_MAP_JOURNEY_TRACER").map_or(true, |v| v != "off")
}

/// What one pass of tracing may cost (a first map's, or a later pass's).
pub const BUDGET_USD: f64 = 3.0;
/// What tracing may cost one team in a day, over all its passes.
pub const DAILY_BUDGET_USD: f64 = 10.0;
/// What one entry is taken to cost before any has finished.
pub const EST_ENTRY_USD: f64 = 0.25;

/// One entry finished: its journeys (kept ones only) and what it cost.
#[derive(Debug, Clone)]
pub struct Done {
    pub entry: Value,
    pub flows: Vec<Value>,
    pub cost_usd: f64,
    /// Not traced: a journey already traced passes through it.
    pub covered: bool,
}

/// A journey worth keeping: at least 3 steps, starting at a screen.
pub fn keepable(f: &Value) -> bool {
    f["steps"].as_array().map(Vec::len).unwrap_or(0) >= 3 && f["steps"][0]["kind"] == "screen"
}

/// One entry done, kept: its journeys go into the store (the entry marked
/// asked), and it leaves the queue. Returns the store and the queue after
/// it, both safe to write down at once: a pass that stops after this point
/// resumes with the next entry and redoes nothing.
pub fn checkpoint(store: &Value, todo: &Value, done: &Done) -> (Value, Value) {
    let id = done.entry["id"].clone();
    let mut one = todo.clone();
    one["entries"] = json!([done.entry]);
    let mut store = fl::merge(store, &one, done.flows.clone());
    let mut rest = todo.clone();
    let left: Vec<Value> = todo["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|e| e["id"] != id)
        .cloned()
        .collect();
    store["pending"] = json!(left.len());
    rest["entries"] = json!(left);
    (store, rest)
}

/// Whether one more entry may start: what the pass has spent, plus what
/// each running entry and this one will cost at the pass's average so far
/// (or the estimate before any has finished), stays within the budget.
pub fn fits_budget(spent: f64, largest: f64, running: usize, budget: f64) -> bool {
    // Each running entry and this one are held at the most an entry has
    // cost so far (or the estimate, whichever is more).
    let each = largest.max(EST_ENTRY_USD);
    spent + each * (running + 1) as f64 <= budget
}

/// What today's tracing has cost a team, from its stored `{day, usd}`
/// (another day counts as nothing yet).
pub fn spent_today(record: &Value, today: &str) -> f64 {
    if record["day"].as_str() == Some(today) {
        record["usd"].as_f64().unwrap_or(0.0)
    } else {
        0.0
    }
}

/// What the next pass may spend: the pass budget, within what's left of
/// the day's.
pub fn pass_budget(spent_today: f64) -> f64 {
    BUDGET_USD.min((DAILY_BUDGET_USD - spent_today).max(0.0))
}

/// Waits, in seconds, before reading a repository again within one pass
/// after GitHub refused it (a 403, a 404, a 5xx, no answer).
pub const READ_RETRY_SECS: [u64; 3] = [2, 6, 18];
/// A rate limit that resets within this many seconds is waited out within
/// the pass; a longer one is read again after its reset, on a later pass.
pub const READ_LIMIT_WAIT_SECS: u64 = 20;
/// A repository that still couldn't be read is tried again on a later pass
/// after this, doubled on each failure in a row up to [`REPO_RETRY_MAX_SECS`].
pub const REPO_RETRY_SECS: i64 = 5 * 60;
pub const REPO_RETRY_MAX_SECS: i64 = 60 * 60;
/// A pass that made no progress never queues the next one sooner than this.
pub const IDLE_SECS: i64 = 5 * 60;
/// The longest an SQS message can be held back (a later resume hops).
pub const MAX_DELAY_SECS: i64 = 900;

/// Why a repository couldn't be read in this pass.
#[derive(Debug, Clone, PartialEq)]
pub enum RepoFail {
    /// GitHub's rate limit, which resets in this many seconds.
    Limited { wait_secs: u64, why: String },
    /// Anything else, after every retry in the pass.
    Failed { why: String },
}

/// Reads one repository, trying again with backoff within the pass:
/// `delays` between attempts after a refusal, and a rate limit's own wait
/// when it resets within [`READ_LIMIT_WAIT_SECS`]. `sleep` waits that many
/// seconds (the tests' doesn't).
pub async fn read_repo<F, Fut, S, SFut>(
    mut fetch: F,
    mut sleep: S,
    delays: &[u64],
) -> Result<Vec<u8>, RepoFail>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>>,
    S: FnMut(u64) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let mut attempt = 0;
    loop {
        let e = match fetch().await {
            Ok(b) => return Ok(b),
            Err(e) => e,
        };
        let why = common::truncate_str(&e.to_string(), 300).to_string();
        if let Some(wait_secs) = crate::clients::github::rate_limited(&*e) {
            if wait_secs <= READ_LIMIT_WAIT_SECS && attempt < delays.len() {
                sleep(wait_secs).await;
                attempt += 1;
                continue;
            }
            return Err(RepoFail::Limited { wait_secs, why });
        }
        let Some(d) = delays.get(attempt) else {
            return Err(RepoFail::Failed { why });
        };
        sleep(*d).await;
        attempt += 1;
    }
}

/// Notes a repository that couldn't be read in the todo's `unread`
/// (`{repo: {until, fails, why}}`): a rate limit is read again after
/// GitHub's reset, anything else after a backoff that doubles with each
/// failure in a row.
pub fn note_unread(unread: &mut Value, repo: &str, fail: &RepoFail, now: i64) {
    let fails = unread[repo]["fails"].as_u64().unwrap_or(0) + 1;
    let (until, why) = match fail {
        RepoFail::Limited { wait_secs, why } => (now + *wait_secs as i64 + 5, why),
        RepoFail::Failed { why } => {
            let shift = (fails - 1).min(8) as u32;
            (
                now + (REPO_RETRY_SECS << shift).min(REPO_RETRY_MAX_SECS),
                why,
            )
        }
    };
    unread[repo] = json!({"until": until, "fails": fails, "why": why});
}

/// Whether a repository may be read in this pass: never refused, or its
/// retry time has come.
pub fn readable_now(unread: &Value, repo: &str, now: i64) -> bool {
    unread[repo]["until"].as_i64().is_none_or(|u| u <= now)
}

/// The repositories still waiting for their retry time, with it.
pub fn waiting_repos(unread: &Value, now: i64) -> BTreeMap<String, i64> {
    unread
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(r, v)| Some((r.clone(), v["until"].as_i64()?)))
        .filter(|(_, u)| *u > now)
        .collect()
}

/// The repositories (`owner/name`) an entry's journey reads: the folders of
/// the files its candidates cite (`files`, from the sandbox), or of the
/// table's candidates its row leads to (an older todo).
pub fn entry_repos(
    todo: &Value,
    entry: &Value,
    repo_of: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    let mut files: Vec<String> = entry["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f.as_str().map(str::to_string))
        .collect();
    if entry["files"].as_array().is_none() {
        let table = &todo["table"];
        let mut seen = BTreeSet::new();
        let mut stack: Vec<String> = entry["id"]
            .as_str()
            .map(str::to_string)
            .into_iter()
            .collect();
        while let Some(id) = stack.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let row = &table[&id];
            for e in row["evidence"].as_array().into_iter().flatten() {
                if let Some(f) = e["file"].as_str() {
                    files.push(f.to_string());
                }
            }
            for n in row["next"].as_array().into_iter().flatten() {
                if let Some(n) = n.as_str() {
                    stack.push(n.to_string());
                }
            }
        }
    }
    files
        .iter()
        .filter_map(|f| repo_of.get(f.split('/').next()?))
        .cloned()
        .collect()
}

/// The todo's entries split by whether their repositories can be read now:
/// those to trace in this pass, and those that wait (with the repository
/// each waits on).
pub fn split_ready(
    todo: &Value,
    repo_of: &BTreeMap<String, String>,
    unread: &Value,
    now: i64,
) -> (Vec<Value>, Vec<(Value, String)>) {
    let waiting = waiting_repos(unread, now);
    let mut ready = Vec::new();
    let mut wait = Vec::new();
    for e in todo["entries"].as_array().into_iter().flatten() {
        match entry_repos(todo, e, repo_of)
            .into_iter()
            .find(|r| waiting.contains_key(r))
        {
            Some(r) => wait.push((e.clone(), r)),
            None => ready.push(e.clone()),
        }
    }
    (ready, wait)
}

/// When the day's tracing budget is next whole again: the next UTC
/// midnight (`spent_today` keys the spend by UTC day).
pub fn budget_reset(now: i64) -> i64 {
    (now.div_euclid(86_400) + 1) * 86_400
}

/// Whether a pass with this budget can start even one entry.
pub fn can_start(budget: f64) -> bool {
    fits_budget(0.0, 0.0, 0, budget)
}

/// How a journeys pass ended, for when the next one may run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PassEnd {
    /// Entries left the queue in this pass (traced, picked from the
    /// candidates, or covered by another journey).
    pub progressed: bool,
    /// The pass stopped for the Lambda's time, not the budget.
    pub by_time: bool,
    /// Entries left whose repositories can be read.
    pub runnable: usize,
    /// When each repository an entry waits on is tried again.
    pub retry_at: Vec<i64>,
    /// What today's tracing has cost, this pass included.
    pub day_spent: f64,
}

/// When the next pass runs, or `None` when nothing is left. Something left
/// that can be traced: at once after a pass that ran out of time, a minute
/// after one that made progress, at the day's budget reset when not even
/// one entry fits what is left of it. Entries waiting on a repository: at
/// its retry time. The guard: a pass that made no progress never queues
/// the next one sooner than [`IDLE_SECS`], so nothing is re-run within
/// seconds while nothing can go on.
pub fn next_resume(p: &PassEnd, now: i64) -> Option<i64> {
    if p.runnable == 0 && p.retry_at.is_empty() {
        return None;
    }
    let mut at: Vec<i64> = p.retry_at.clone();
    if p.runnable > 0 {
        at.push(if !can_start(pass_budget(p.day_spent)) {
            budget_reset(now)
        } else if p.progressed && p.by_time {
            now
        } else if p.progressed {
            now + 60
        } else {
            now + IDLE_SECS
        });
    }
    let at = at.into_iter().min()?;
    Some(if p.progressed {
        at
    } else {
        at.max(now + IDLE_SECS)
    })
}

/// What a journeys message does, from the team's schedule (`{seq, at}`,
/// [`common::app_map::flows_next_key`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Take {
    /// Its time has come: run a pass.
    Run,
    /// Early: queued again (same turn) for this many seconds.
    Hop(i64),
    /// An older resume another one replaced: dropped.
    Stale,
}

/// One resume at a time: only the message whose turn is the schedule's
/// `seq` runs, and not before `at`; with no schedule yet (a team from
/// before it), the message runs.
pub fn take_message(turn: u32, next: &Value, now: i64) -> Take {
    let Some(seq) = next["seq"].as_u64() else {
        return Take::Run;
    };
    if u64::from(turn) != seq {
        return Take::Stale;
    }
    let at = next["at"].as_i64().unwrap_or(0);
    if at > now {
        Take::Hop((at - now).min(MAX_DELAY_SECS))
    } else {
        Take::Run
    }
}

/// The schedule after this one, for one resume at `at`: every message of
/// an earlier one is stale from here on.
pub fn reschedule(next: &Value, at: i64) -> Value {
    let seq = next["seq"].as_u64().unwrap_or(0) + 1;
    json!({"seq": seq % u64::from(u32::MAX), "at": at})
}

/// The message's turn for a schedule.
pub fn turn_of(next: &Value) -> u32 {
    next["seq"].as_u64().unwrap_or(0) as u32
}

/// Where a journey of this kind is done: the end state its last steps must
/// show (sign in and sign up: the signed-in screen; password reset: the
/// password set and the screen after; checkout and booking: the
/// confirmation). `None` when the journey reaches it.
pub fn short_of_end(family: &str, steps: &[Value]) -> Option<&'static str> {
    let calls = steps.iter().filter(|s| s["kind"] == "call").count();
    let last_call = steps.iter().rposition(|s| s["kind"] == "call");
    let screen_after = |i: Option<usize>| {
        steps
            .iter()
            .enumerate()
            .any(|(j, s)| s["kind"] == "screen" && i.is_some_and(|i| j > i))
    };
    match family {
        "sign in" | "sign up" if !screen_after(last_call) => {
            Some("the screen the user lands on once signed in")
        }
        "password reset" if calls < 2 || !saves_password(steps) || !screen_after(last_call) => Some(
            "the new password being set (the request that saves it, from the screen the emailed link or code opens: search the routes for reset-password, new-password or set-password) and the screen after it",
        ),
        "checkout" | "booking" if !screen_after(last_call) => {
            Some("the confirmation the user sees")
        }
        _ => None,
    }
}

/// The path a cited request goes to, as written: from its first slash,
/// with template parts as parameters and no query.
pub fn request_path(token: &str) -> Option<String> {
    let t = regex::Regex::new(r"\$\{[^}]*\}")
        .ok()?
        .replace_all(token, ":p");
    let at = t.find('/')?;
    let p: String = t[at..]
        .chars()
        .take_while(|c| !c.is_whitespace() && !matches!(c, '\'' | '"' | '`' | '?' | ')' | ','))
        .collect();
    (p.len() > 1).then_some(p)
}

/// Whether a route pattern that forwards every path under it (`/api/*`,
/// `/proxy*`, `/api/auth/[...nextauth]`) takes `path`.
pub fn pattern_takes(pattern: &str, path: &str) -> bool {
    let prefix = pattern
        .split(['*', '['])
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    !prefix.is_empty()
        && (path == prefix
            || path.starts_with(&format!("{prefix}/"))
            || (pattern.contains(&format!("{prefix}*")) && path.starts_with(prefix)))
}

/// Whether a journey has the request that saves a new password (after the
/// one that asks for the reset): its path, name or note says so.
fn saves_password(steps: &[Value]) -> bool {
    let re = regex::Regex::new(r"(?i)(change|update|set|reset|new|save)[-_ ]?password|password[-_/ ]?(reset|update|change)")
        .expect("pattern");
    steps
        .iter()
        .filter(|s| s["kind"] == "call")
        .skip(1)
        .any(|s| {
            re.is_match(s["token"].as_str().unwrap_or(""))
                || re.is_match(s["note"].as_str().unwrap_or(""))
        })
}

/// A request in the journey that goes through a route forwarding every
/// path (a proxy, a catch-all), found by where the request goes (its path
/// against the route's pattern, or the forwarding file itself), with no
/// outside service step after it: the service and where the map found it.
pub fn proxied_service_missing(
    todo: &Value,
    steps: &[Value],
    snapshot: Option<&Snapshot>,
) -> Option<String> {
    let table = &todo["table"];
    let routes = forwarding_routes(table, snapshot);
    for (i, s) in steps.iter().enumerate() {
        if !matches!(s["kind"].as_str(), Some("call" | "handler")) {
            continue;
        }
        let Some(r) = route_of(&routes, s) else {
            continue;
        };
        // That service, not just any outside step, comes after it.
        if steps[i + 1..]
            .iter()
            .any(|t| is_service(table, t, &r.service, &r.cite))
        {
            continue;
        }
        return Some(format!(
            "`{}` goes through a route that passes every request under `{}` on; the outside service behind it, {} (the map found it at {}:{}), isn't a step",
            s["token"].as_str().unwrap_or(""),
            r.pattern,
            table[&r.service]["label"].as_str().unwrap_or("the service"),
            r.cite["file"].as_str().unwrap_or(""),
            r.cite["line"]
        ));
    }
    None
}

/// The forwarding route a request step goes through: by its path against
/// the route's pattern, or by citing the forwarding file itself.
fn route_of<'r>(routes: &'r [Route], s: &Value) -> Option<&'r Route> {
    let path = request_path(s["token"].as_str().unwrap_or(""));
    let file = s["file"].as_str().unwrap_or("");
    routes.iter().find(|r| {
        file == r.file
            || path
                .as_deref()
                .is_some_and(|x| pattern_takes(&r.pattern, x))
    })
}

/// The kind of journey a trace is: from its title, else its entry's.
pub fn journey_family(journey: &Value, entry: &Value) -> String {
    title_family(journey["title"].as_str().unwrap_or(""))
        .map(str::to_string)
        .unwrap_or_else(|| entry["family"].as_str().unwrap_or("").replace('-', " "))
}

/// The kind a journey's title names: from what the user does (the words
/// before where it starts, "from the login form"), the narrower kinds
/// first, so a reset or a payment that starts at a sign-in form is that.
pub fn title_family(title: &str) -> Option<&'static str> {
    let lower = title.to_lowercase();
    let action = [" from ", " on the ", " in the ", " at the ", " via "]
        .iter()
        .filter_map(|w| lower.find(w))
        .min()
        .map_or(lower.as_str(), |i| &lower[..i]);
    [
        "password reset",
        "checkout",
        "booking",
        "sign in",
        "sign up",
    ]
    .iter()
    .copied()
    .find(|f| family_matches(f, action))
    .or_else(|| family_of(title))
}

/// A step cited where the map cites one of its candidates.
fn step_from(table: &Value, id: &str, which: usize, kind: &str, note: &str) -> Option<Value> {
    let e = &table[id]["evidence"][which];
    Some(
        json!({"kind": kind, "file": e["file"].as_str()?, "line": e["line"].as_u64()?, "token": e["token"].as_str()?, "note": note, "from_map": true}),
    )
}

/// A route that forwards every request under it (a proxy, a catch-all):
/// the file that forwards, the pattern it takes, the outside service it
/// passes requests on to, and where the code names that service.
struct Route {
    file: String,
    pattern: String,
    service: String,
    cite: Value,
    handler: Option<Value>,
}

/// Whether a token names a setting (an address read from the environment
/// or config, `VENDOR_API`), not a package.
fn is_setting(tok: &str) -> bool {
    tok.len() >= 3
        && tok.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && tok
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// The files a source file imports from the same code (relative, `src/`
/// or `@/` paths), as snapshot paths.
fn imports_of(snapshot: &Snapshot, file: &str) -> Vec<String> {
    let Some(b) = snapshot.files.get(file) else {
        return Vec::new();
    };
    let re = regex::Regex::new(r#"(?:from\s+|require\(\s*|import\s+)['"]([^'"]+)['"]"#)
        .expect("pattern");
    let text = String::from_utf8_lossy(b);
    re.captures_iter(&text)
        .filter_map(|m| resolve_import(snapshot, file, &m[1]))
        .collect()
}

/// The snapshot file an import in `file` names (relative, `src/` or `@/`).
fn resolve_import(snapshot: &Snapshot, file: &str, spec: &str) -> Option<String> {
    let dir = file.rsplit_once('/').map_or("", |(d, _)| d);
    let root = file.find("/src/").map_or(dir, |i| &file[..i]);
    let base = if spec.starts_with('.') {
        format!("{dir}/{spec}")
    } else if let Some(r) = spec.strip_prefix("src/") {
        format!("{root}/src/{r}")
    } else {
        format!("{root}/src/{}", spec.strip_prefix("@/")?)
    };
    let mut parts: Vec<&str> = Vec::new();
    for p in base.split('/') {
        match p {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            x => parts.push(x),
        }
    }
    let base = parts.join("/");
    ["", ".ts", ".tsx", ".js", ".jsx", "/index.ts", "/index.js"]
        .iter()
        .map(|ext| format!("{base}{ext}"))
        .find(|f| snapshot.files.contains_key(f))
}

/// The outside services the map names by a setting: (node, setting).
fn setting_nodes(table: &Value) -> Vec<(String, String)> {
    table
        .as_object()
        .into_iter()
        .flat_map(|o| o.iter())
        .filter(|(_, c)| c["kind"] == "outside")
        .filter_map(|(id, c)| {
            let t = c["evidence"][0]["token"].as_str()?;
            is_setting(t).then(|| (id.clone(), t.to_string()))
        })
        .collect()
}

/// The settings a line reads, as whole words.
fn settings_on<'s>(line: &str, settings: &'s [(String, String)]) -> Vec<&'s (String, String)> {
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    settings
        .iter()
        .filter(|(_, t)| {
            line.match_indices(t.as_str()).any(|(at, _)| {
                !line[..at].chars().next_back().is_some_and(word)
                    && !line[at + t.len()..].chars().next().is_some_and(word)
            })
        })
        .collect()
}

/// The outside service a service step's code reaches: the call on its
/// line (else the method it is in), followed through the injected
/// services it calls (`this.payments.charge(...)`, the member's class
/// imported from its file) to the first line that reads a service
/// setting. Where a called method isn't found, or calls nothing further,
/// the class's own file is read (a client set up in its constructor).
fn service_reached(
    snapshot: &Snapshot,
    settings: &[(String, String)],
    file: &str,
    line: usize,
    depth: usize,
    seen: &mut BTreeSet<String>,
) -> Option<(String, Value)> {
    if depth > 6 || !seen.insert(format!("{file}:{line}")) {
        return None;
    }
    let text = String::from_utf8_lossy(snapshot.files.get(file)?).to_string();
    let lines: Vec<&str> = text.lines().collect();
    let at = line.checked_sub(1).filter(|i| *i < lines.len())?;
    let hit_in = |from: usize, to: usize| -> Option<(String, Value)> {
        (from..to.min(lines.len())).find_map(|n| {
            settings_on(lines[n], settings)
                .first()
                .map(|(id, t)| (id.clone(), json!({"file": file, "line": n + 1, "token": t})))
        })
    };
    let calls = regex::Regex::new(r"this\.(\w+)\.(\w+)\s*[<(]").expect("pattern");
    // The method around the line: back to its start, on to its end.
    let def = regex::Regex::new(
        r"^\s*(?:public\s+|private\s+|protected\s+)?(?:static\s+)?(?:async\s+)?(\w+)\s*(?:<[^>]*>)?\(",
    )
    .expect("pattern");
    let keyword = |n: &str| {
        matches!(
            n,
            "if" | "for" | "while" | "switch" | "return" | "catch" | "function" | "await"
        )
    };
    // A route's decorator (`@Post('complete')`) names the method below it.
    let at = if lines[at].trim_start().starts_with('@') {
        (at..lines.len().min(at + 8))
            .find(|n| def.captures(lines[*n]).is_some_and(|c| !keyword(&c[1])))
            .unwrap_or(at)
    } else {
        at
    };
    let start = (0..=at)
        .rev()
        .find(|n| def.captures(lines[*n]).is_some_and(|c| !keyword(&c[1])))
        .unwrap_or(at);
    // Braces count once the parameters close (a parameter's type can have
    // braces of its own).
    let mut depth_b = 0i32;
    let mut parens = 0i32;
    let mut opened = false;
    let mut end = lines.len();
    for (n, l) in lines.iter().enumerate().skip(start) {
        for ch in l.chars() {
            match ch {
                '(' => parens += 1,
                ')' => parens -= 1,
                '{' if parens <= 0 => {
                    depth_b += 1;
                    opened = true;
                }
                '}' if parens <= 0 => depth_b -= 1,
                _ => {}
            }
        }
        if opened && depth_b <= 0 {
            end = n + 1;
            break;
        }
    }
    if depth > 0 {
        if let Some(h) = hit_in(start, end) {
            return Some(h);
        }
    }
    let on_line: Vec<(String, String)> = calls
        .captures_iter(lines[at])
        .map(|c| (c[1].to_string(), c[2].to_string()))
        .collect();
    let followed = if on_line.is_empty() {
        lines[start..end]
            .iter()
            .flat_map(|l| {
                calls
                    .captures_iter(l)
                    .map(|c| (c[1].to_string(), c[2].to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        on_line
    };
    for (member, method) in followed {
        let Some(class) = regex::Regex::new(&format!(r"\b{}\s*:\s*(\w+)", regex::escape(&member)))
            .ok()
            .and_then(|r| r.captures(&text).map(|c| c[1].to_string()))
        else {
            continue;
        };
        let Some(spec) = regex::Regex::new(&format!(
            r#"(?s)import\s*(?:type\s*)?\{{[^}}]*\b{}\b[^}}]*\}}\s*from\s*['"]([^'"]+)['"]"#,
            regex::escape(&class)
        ))
        .ok()
        .and_then(|r| r.captures(&text).map(|c| c[1].to_string())) else {
            continue;
        };
        let Some(next) = resolve_import(snapshot, file, &spec) else {
            continue;
        };
        let body = String::from_utf8_lossy(snapshot.files.get(&next)?).to_string();
        let found = body
            .lines()
            .position(|l| def.captures(l).is_some_and(|c| c[1] == *method) && !l.contains("this."));
        let reached = match found {
            Some(n) => service_reached(snapshot, settings, &next, n + 1, depth + 1, seen),
            None => None,
        };
        // The class's own file, where the method isn't found or calls on.
        let reached = reached.or_else(|| {
            body.lines().enumerate().find_map(|(n, l)| {
                settings_on(l, settings)
                    .first()
                    .map(|(id, t)| (id.clone(), json!({"file": next, "line": n + 1, "token": t})))
            })
        });
        if reached.is_some() {
            return reached;
        }
    }
    None
}

/// The outside service a forwarding file passes requests on to: the first
/// service setting its code reads, in the file or in what it imports (two
/// files deep), cited at that line. Without the code, the route's own
/// service setting the map ties to it.
fn service_behind(
    table: &Value,
    snapshot: Option<&Snapshot>,
    file: &str,
    next: &[String],
) -> Option<(String, Value)> {
    let settings: Vec<(&String, &str)> = table
        .as_object()
        .into_iter()
        .flat_map(|o| o.iter())
        .filter(|(_, c)| c["kind"] == "outside")
        .filter_map(|(id, c)| {
            let t = c["evidence"][0]["token"].as_str()?;
            is_setting(t).then_some((id, t))
        })
        .collect();
    if let Some(snap) = snapshot {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut level = vec![file.to_string()];
        for _ in 0..3 {
            let mut deeper = Vec::new();
            for f in level {
                if !seen.insert(f.clone()) {
                    continue;
                }
                let Some(b) = snap.files.get(&f) else {
                    continue;
                };
                let text = String::from_utf8_lossy(b);
                let mut hits: Vec<(&String, usize, &str)> = Vec::new();
                for (n, l) in text.lines().enumerate() {
                    for (id, t) in &settings {
                        let named = l.match_indices(*t).any(|(at, _)| {
                            let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
                            !l[..at].chars().next_back().is_some_and(word)
                                && !l[at + t.len()..].chars().next().is_some_and(word)
                        });
                        if named {
                            hits.push((id, n + 1, t));
                        }
                    }
                }
                // The route's own service first, then the first one read.
                if let Some((id, n, t)) = hits
                    .iter()
                    .find(|(id, _, _)| next.contains(id))
                    .or_else(|| hits.first())
                {
                    return Some(((*id).clone(), json!({"file": f, "line": n, "token": t})));
                }
                deeper.extend(imports_of(snap, &f));
            }
            level = deeper;
        }
    }
    next.iter()
        .find(|id| settings.iter().any(|(s, _)| s == id))
        .map(|id| (id.clone(), table[id.as_str()]["evidence"][0].clone()))
}

/// The forwarding routes the map knows, each with the outside service it
/// passes requests on to.
fn forwarding_routes(table: &Value, snapshot: Option<&Snapshot>) -> Vec<Route> {
    let mut routes: Vec<Route> = Vec::new();
    for c in table.as_object().into_iter().flat_map(|o| o.values()) {
        let (Some(f), Some(p)) = (c["all"].as_str(), c["pattern"].as_str()) else {
            continue;
        };
        if routes.iter().any(|r| r.file == f && r.pattern == p) {
            continue;
        }
        let next: Vec<String> = c["next"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|x| x.as_str().map(str::to_string))
            .filter(|x| table[x.as_str()]["kind"] == "outside")
            .collect();
        if let Some((service, cite)) = service_behind(table, snapshot, f, &next) {
            let handler = c["evidence"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|e| e["file"].as_str() == Some(f))
                .cloned();
            routes.push(Route {
                file: f.to_string(),
                pattern: p.to_string(),
                service,
                cite,
                handler,
            });
        }
    }
    routes
}

/// Whether a step is the outside service `id`: by the map's node, by the
/// setting or package its line names, or at the line the route names it.
fn is_service(table: &Value, step: &Value, id: &str, cite: &Value) -> bool {
    let tok = table[id]["evidence"][0]["token"].as_str().unwrap_or("");
    step["kind"] == "outside"
        && (node_for(table, step).as_deref() == Some(id)
            || (step["file"] == cite["file"] && step["line"] == cite["line"])
            || (!tok.is_empty()
                && step["token"]
                    .as_str()
                    .is_some_and(|x| !x.is_empty() && (x.contains(tok) || tok.contains(x)))))
}

/// The screen whose route takes `path`: exact segments beat route
/// parameters, a parameter beats a catch-all (`*`), and `(group)` segments
/// are not in the address. None when two screens take it equally well (the
/// screens a catch-all route dispatches to by a type the address does not
/// show).
fn screen_for_path<'a>(rows: &[(&'a String, &Value)], path: &str) -> Option<&'a String> {
    let want: Vec<String> = regex::Regex::new(r"\$\{[^}]*\}")
        .ok()?
        .replace_all(path.split(['?', '#']).next().unwrap_or(""), ":p")
        .split('/')
        .filter(|x| !x.is_empty())
        .map(str::to_string)
        .collect();
    let mut best: Option<(&'a String, u32)> = None;
    let mut tie = false;
    for (id, c) in rows.iter().filter(|(_, c)| c["kind"] == "screen") {
        let Some(url) = c["key"].as_str().and_then(|k| k.splitn(3, ':').nth(2)) else {
            continue;
        };
        let Some(score) = route_score(url, &want) else {
            continue;
        };
        match best {
            Some((_, b)) if score < b => {}
            Some((x, b)) if score == b => tie |= x != *id,
            _ => {
                best = Some((id, score));
                tie = false;
            }
        }
    }
    best.filter(|_| !tie).map(|(id, _)| id)
}

/// How well a route pattern takes an address's segments (higher is
/// closer), or None when it does not.
fn route_score(pattern: &str, want: &[String]) -> Option<u32> {
    let have: Vec<&str> = pattern
        .split('/')
        .filter(|x| !x.is_empty() && !(x.starts_with('(') && x.ends_with(')')))
        .collect();
    if have.is_empty() && !want.is_empty() {
        return None;
    }
    let mut score = 0;
    for (i, h) in have.iter().enumerate() {
        if *h == "*" || h.starts_with("[...") || h.starts_with("[[...") {
            return (i < want.len() || h.starts_with("[[")).then_some(score + 1);
        }
        let w = want.get(i)?;
        if h == w {
            score += 4;
        } else if h.starts_with(':') || h.starts_with('[') || w.starts_with(':') {
            score += 2;
        } else {
            return None;
        }
    }
    (have.len() == want.len()).then_some(score)
}

/// Where the code a journey cites builds the link its email sends (a path
/// with the reset in it, the token as a parameter): the path.
fn reset_link_in(snapshot: &Snapshot, files: &BTreeSet<String>) -> Option<String> {
    let re = regex::Regex::new(r#"['"`](?:\$\{[^}]*\})?(/[A-Za-z0-9_\-/${}.:]*?(?:reset|new|set|change|update)[-_]?password[A-Za-z0-9_\-/${}.:]*)"#).ok()?;
    for f in files {
        let Some(b) = snapshot.files.get(f) else {
            continue;
        };
        for l in String::from_utf8_lossy(b).lines() {
            if !l.to_lowercase().contains("url")
                && !l.to_lowercase().contains("link")
                && !l.contains("${")
            {
                continue;
            }
            if let Some(m) = re.captures(l) {
                return Some(m[1].to_string());
            }
        }
    }
    None
}

/// The app a candidate belongs to: the folder its first citation is in.
fn app_of(table: &Value, id: &str) -> String {
    table[id]["evidence"][0]["file"]
        .as_str()
        .unwrap_or("")
        .split('/')
        .next()
        .unwrap_or("")
        .to_string()
}

/// A journey's steps completed from the static map, where the code says
/// what comes next and the trace stopped short of it. Every step added is
/// cited where the map cites it (so the citation check holds):
/// - a request through a forwarding route (a proxy, a catch-all) gets the
///   outside service the route passes it on to, right after it;
/// - a password reset that never saves the new password gets the page the
///   reset link opens (the app's reset or password-update route, any app),
///   the request that saves the password and its handler;
/// - a journey that reaches a payment service gets the service's callbacks
///   (the webhooks the map ties to it) and the return page its code sends
///   the user back to;
/// - an outside service step the trace put before any request after a
///   screen goes after the request that uses it.
pub fn complete_journey(
    journey: &mut Value,
    family: &str,
    todo: &Value,
    snapshot: Option<&Snapshot>,
) -> usize {
    let table = &todo["table"];
    let mut steps: Vec<Value> = journey["steps"].as_array().cloned().unwrap_or_default();
    let before = steps.len();
    // A service step before any request after its screen: it runs when the
    // request that uses it does.
    let mut i = 0;
    while i < steps.len() {
        if steps[i]["kind"] == "outside" {
            let last_screen = steps[..i].iter().rposition(|s| s["kind"] == "screen");
            let call_before = steps[last_screen.map_or(0, |x| x + 1)..i]
                .iter()
                .any(|s| s["kind"] == "call");
            if !call_before {
                if let Some(c) = steps[i + 1..]
                    .iter()
                    .position(|s| s["kind"] == "call")
                    .map(|c| c + i + 1)
                {
                    let s = steps.remove(i);
                    let at = steps[c..]
                        .iter()
                        .position(|t| t["kind"] != "call" && t["kind"] != "handler")
                        .map_or(steps.len(), |p| p + c);
                    steps.insert(at, s);
                    continue;
                }
            }
        }
        i += 1;
    }
    let routes = forwarding_routes(table, snapshot);
    let rows: Vec<(&String, &Value)> = table
        .as_object()
        .map(|o| o.iter().collect())
        .unwrap_or_default();
    let saves = regex::Regex::new(r"(?i)(change|update|set|reset|new|save)[-_ ]?password|password[-_/ ]?(reset|update|change)").expect("pattern");
    let reset_page = regex::Regex::new(
        r"(?i)(reset|new|set|change|update)[-_ ]?password|password[-_ ]?(reset|update)",
    )
    .expect("pattern");
    // A reset that never saves the new password.
    if family == "password reset" && !saves_password(&steps) {
        let entry_app = steps
            .first()
            .and_then(|s| s["file"].as_str())
            .unwrap_or("")
            .split('/')
            .next()
            .unwrap_or("")
            .to_string();
        // The page the emailed link opens: where the code the journey cites
        // builds that link, matched to a screen's route; else the app's
        // reset or password-update route.
        let cited: BTreeSet<String> = steps
            .iter()
            .filter_map(|s| s["file"].as_str().map(str::to_string))
            .collect();
        let built = snapshot
            .and_then(|snap| reset_link_in(snap, &cited))
            .and_then(|p| screen_for_path(&rows, &p).cloned());
        let pages: Vec<&String> = rows
            .iter()
            .filter(|(_, c)| {
                c["kind"] == "screen"
                    && reset_page.is_match(c["key"].as_str().unwrap_or(""))
                    && !c["key"]
                        .as_str()
                        .unwrap_or("")
                        .to_lowercase()
                        .contains("forgot")
            })
            .map(|(id, _)| *id)
            .collect();
        // A routed page (a route the app registers) before a part of one.
        let mut pages = pages;
        pages.sort_by_key(|id| {
            !table[id.as_str()]["key"]
                .as_str()
                .unwrap_or("")
                .starts_with("screen:")
        });
        // The same app's page first, then another app's (an emailed link
        // opens the web page from a mobile flow).
        let mut ordered: Vec<&String> = built.iter().collect();
        ordered.extend(
            pages
                .iter()
                .filter(|id| app_of(table, id) == entry_app)
                .copied(),
        );
        ordered.extend(
            pages
                .iter()
                .filter(|id| app_of(table, id) != entry_app)
                .copied(),
        );
        // The request that saves the new password, for a page: the map's
        // (the page's own, else its app's), else the request its app's code
        // posts to a password-saving path, landing where the route that
        // takes the path does.
        let posts = regex::Regex::new(r#"(?i)\.(post|put|patch)\b[^(]*\(\s*['"`]([^'"`]*(?:change|update|set|reset|new|save)[-_]?password[^'"`]*)['"`]"#)
            .expect("pattern");
        let saving = |page: &str| -> Option<(Value, Option<Value>)> {
            let own = table[page]["next"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|x| x.as_str())
                .find(|x| {
                    table[*x]["kind"] == "request"
                        && saves.is_match(table[*x]["key"].as_str().unwrap_or(""))
                })
                .map(str::to_string);
            let app = app_of(table, page);
            let req = own.or_else(|| {
                rows.iter()
                    .find(|(id, c)| {
                        c["kind"] == "request"
                            && saves.is_match(c["key"].as_str().unwrap_or(""))
                            && app_of(table, id) == app
                    })
                    .map(|(id, _)| (*id).clone())
            });
            if let Some(req) = req {
                let call = step_from(table, &req, 0, "call", "It saves the new password.")?;
                let handler = (table[&req]["evidence"]
                    .as_array()
                    .is_some_and(|e| e.len() > 1))
                .then(|| {
                    step_from(
                        table,
                        &req,
                        1,
                        "handler",
                        "The server sets the new password.",
                    )
                })
                .flatten();
                return Some((call, handler));
            }
            let snap = snapshot?;
            for (f, b) in snap.files.range(format!("{app}/")..) {
                if !f.starts_with(&format!("{app}/")) {
                    break;
                }
                let lower = f.to_lowercase();
                if ["test", "spec", "mock", "fixture", "stories"]
                    .iter()
                    .any(|w| lower.contains(w))
                {
                    continue;
                }
                for (n, l) in String::from_utf8_lossy(b).lines().enumerate() {
                    if let Some(m) = posts.captures(l) {
                        let path = m[2].to_string();
                        let call = json!({"kind": "call", "file": f, "line": n + 1, "token": path, "note": "It saves the new password.", "from_map": true});
                        let handler = route_of(&routes, &call)
                            .and_then(|r| r.handler.clone())
                            .and_then(|h| {
                                Some(json!({"kind": "handler", "file": h["file"].as_str()?, "line": h["line"].as_u64()?, "token": h["token"].as_str()?, "note": "The server sets the new password.", "from_map": true}))
                            });
                        return Some((call, handler));
                    }
                }
            }
            None
        };
        if let Some((page, (call, handler))) = ordered
            .iter()
            .find_map(|p| saving(p).map(|r| ((*p).clone(), r)))
        {
            if let Some(st) =
                step_from(table, &page, 0, "screen", "The reset link opens this page.")
            {
                steps.push(st);
            }
            steps.push(call);
            steps.extend(handler);
        }
    }
    // A payment service's callbacks and the page it sends the user back to.
    let payment =
        regex::Regex::new(r"(?i)pay|checkout|adyen|stripe|braintree|paypal|square|billing")
            .expect("pattern");
    let services: Vec<String> = steps
        .iter()
        .filter(|s| s["kind"] == "outside")
        .filter_map(|s| node_for(table, s))
        .collect();
    let pays: Vec<&String> = services
        .iter()
        .filter(|id| {
            payment.is_match(&format!(
                "{} {}",
                table[id.as_str()]["key"].as_str().unwrap_or(""),
                table[id.as_str()]["label"].as_str().unwrap_or("")
            ))
        })
        .collect();
    // The payment service the journey's requests name (`/adyen/...`).
    let vendor = regex::Regex::new(
        r"(?i)\b(adyen|stripe|braintree|paypal|square|checkout\.com|mollie|klarna)\b",
    )
    .expect("pattern");
    let mut named: Vec<String> = Vec::new();
    for st in &steps {
        for m in vendor.find_iter(st["token"].as_str().unwrap_or("")) {
            let w = m.as_str().to_lowercase();
            if !named.contains(&w) {
                named.push(w);
            }
        }
    }
    let paying = !pays.is_empty() || !named.is_empty() || family == "checkout";
    if paying {
        let mut words: Vec<String> = pays
            .iter()
            .flat_map(|id| {
                table[id.as_str()]["label"]
                    .as_str()
                    .unwrap_or("")
                    .split_whitespace()
                    .map(|w| w.to_lowercase())
                    .collect::<Vec<_>>()
            })
            .filter(|w| w.len() >= 4)
            .collect();
        for w in named {
            if !words.contains(&w) {
                words.push(w);
            }
        }
        let mut hooks: Vec<String> = pays
            .iter()
            .flat_map(|id| {
                table[id.as_str()]["next"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
            })
            .filter_map(|x| x.as_str().map(str::to_string))
            .filter(|x| table[x.as_str()]["kind"] == "async")
            .collect();
        for (id, c) in &rows {
            let k = c["key"].as_str().unwrap_or("").to_lowercase();
            if c["kind"] == "async"
                && k.starts_with("hook:")
                && (words.iter().any(|w| k.contains(w.as_str())) || k.contains("membership"))
                && !hooks.contains(id)
            {
                hooks.push((*id).clone());
            }
        }
        // The return page: the path the journey's code builds for it.
        if let Some(snap) = snapshot {
            let ret = regex::Regex::new(r#"(?i)(return|redirect|success|callback)[-_]?(url|path|to)\b[^\n]*?['"`](?:\$\{[^}]*\})?(/[A-Za-z0-9_\-/]+)"#).expect("pattern");
            let files: BTreeSet<String> = steps
                .iter()
                .filter_map(|s| s["file"].as_str().map(str::to_string))
                .collect();
            let mut found: Option<String> = None;
            'files: for f in &files {
                let Some(b) = snap.files.get(f) else { continue };
                for l in String::from_utf8_lossy(b).lines() {
                    if let Some(m) = ret.captures(l) {
                        found = Some(m[3].to_string());
                        break 'files;
                    }
                }
            }
            if let Some(path) = found {
                let page = rows.iter().find(|(_, c)| {
                    c["kind"] == "screen"
                        && c["key"]
                            .as_str()
                            .and_then(|k| k.splitn(3, ':').nth(2))
                            .is_some_and(|u| u.trim_end_matches('/') == path.trim_end_matches('/'))
                });
                if let Some((id, _)) = page {
                    if !steps
                        .iter()
                        .any(|s| node_for(table, s).as_deref() == Some(id.as_str()))
                    {
                        if let Some(st) = step_from(
                            table,
                            id,
                            0,
                            "screen",
                            "The payment service sends the user back to this page.",
                        ) {
                            steps.push(st);
                        }
                    }
                }
            }
        }
        for h in hooks {
            if !steps
                .iter()
                .any(|s| node_for(table, s).as_deref() == Some(h.as_str()))
            {
                let label = table[&h]["label"].as_str().unwrap_or("its webhook");
                if let Some(st) = step_from(table, &h, 0, "callback", &format!("Later, {label}.")) {
                    steps.push(st);
                }
            }
        }
    }
    // The outside service behind a forwarding route, and the route's own
    // handler when the journey names the request but not where it lands.
    let mut i = 0;
    while i < steps.len() {
        let kind = steps[i]["kind"].as_str().unwrap_or("");
        let by_path = kind == "service"
            && request_path(steps[i]["token"].as_str().unwrap_or(""))
                .is_some_and(|x| routes.iter().any(|r| pattern_takes(&r.pattern, &x)));
        if matches!(kind, "call" | "handler") || by_path {
            // Once per journey: the first request through the route.
            if let Some(r) = route_of(&routes, &steps[i]).filter(|r| {
                !steps
                    .iter()
                    .any(|s| is_service(table, s, &r.service, &r.cite))
            }) {
                let mut at = i + 1;
                if kind != "handler" && steps.get(at).is_none_or(|t| t["kind"] != "handler") {
                    if let Some((f, l, t)) = r.handler.as_ref().and_then(|h| {
                        Some((
                            h["file"].as_str()?,
                            h["line"].as_u64()?,
                            h["token"].as_str()?,
                        ))
                    }) {
                        steps.insert(
                            at,
                            json!({"kind": "handler", "file": f, "line": l, "token": t, "note": format!("The route under `{}` takes it.", r.pattern), "from_map": true}),
                        );
                    }
                }
                while at < steps.len() && steps[at]["kind"] == "handler" {
                    at += 1;
                }
                let label = table[&r.service]["label"].as_str().unwrap_or("the service");
                if let (Some(f), Some(l), Some(t)) = (
                    r.cite["file"].as_str(),
                    r.cite["line"].as_u64(),
                    r.cite["token"].as_str(),
                ) {
                    steps.insert(
                        at,
                        json!({"kind": "outside", "file": f, "line": l, "token": t, "note": format!("The route passes it on to {label}."), "from_map": true}),
                    );
                }
            }
        }
        i += 1;
    }
    // The outside service the server code of a step reaches (a service,
    // or a handler), right after it (once per service).
    if let Some(snap) = snapshot {
        let settings = setting_nodes(table);
        let mut i = 0;
        while i < steps.len() {
            if matches!(steps[i]["kind"].as_str(), Some("service" | "handler"))
                && steps[i]["from_map"] != true
            {
                let (file, line) = (
                    steps[i]["file"].as_str().unwrap_or("").to_string(),
                    steps[i]["line"].as_u64().unwrap_or(0) as usize,
                );
                let mut seen = BTreeSet::new();
                if let Some((id, cite)) =
                    service_reached(snap, &settings, &file, line, 0, &mut seen)
                {
                    if !steps.iter().any(|s| is_service(table, s, &id, &cite)) {
                        let label = table[&id]["label"].as_str().unwrap_or("the service");
                        if let (Some(f), Some(l), Some(t)) = (
                            cite["file"].as_str(),
                            cite["line"].as_u64(),
                            cite["token"].as_str(),
                        ) {
                            steps.insert(
                                i + 1,
                                json!({"kind": "outside", "file": f, "line": l, "token": t, "note": format!("Its code calls {label}."), "from_map": true}),
                            );
                        }
                    }
                }
            }
            i += 1;
        }
    }
    // Over the cap, the trace's own in-server helper steps give way first
    // (latest first), so what the code says comes next is kept.
    while steps.len() > fl::MAX_STEPS {
        match steps
            .iter()
            .rposition(|s| s["kind"] == "service" && s["from_map"] != true)
        {
            Some(at) => {
                steps.remove(at);
            }
            None => break,
        }
    }
    steps.truncate(fl::MAX_STEPS);
    let added = steps.len().saturating_sub(before);
    journey["steps"] = json!(steps);
    added
}

/// What the map knows the entry screen's own code does that the journey
/// leaves out: the requests it makes itself (not a page's around it) and
/// the callbacks an outside service in the journey sends.
pub fn left_out(todo: &Value, entry: &str, steps: &[Value]) -> Vec<String> {
    let table = &todo["table"];
    let weak: Vec<&str> = table[entry]["weak"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str())
        .collect();
    let hit = |id: &str| {
        let c = &table[id];
        let path = c["key"]
            .as_str()
            .unwrap_or("")
            .split_once(' ')
            .map(|(_, p)| p.to_string());
        steps.iter().any(|s| {
            node_for(table, s).as_deref() == Some(id)
                || c["evidence"].as_array().into_iter().flatten().any(|e| {
                    e["file"] == s["file"]
                        && (e["line"].as_i64().unwrap_or(-99) - s["line"].as_i64().unwrap_or(0))
                            .abs()
                            <= 3
                })
                || path
                    .as_deref()
                    .zip(request_path(s["token"].as_str().unwrap_or("")))
                    .is_some_and(|(a, b)| a == b)
        })
    };
    let at = |id: &str| {
        let e = &table[id]["evidence"][0];
        format!(
            "{} at {}:{}",
            table[id]["label"].as_str().unwrap_or(id),
            e["file"].as_str().unwrap_or(""),
            e["line"]
        )
    };
    let mut out = Vec::new();
    for b in table[entry]["next"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str())
    {
        if table[b]["kind"] == "request" && !weak.contains(&b) && !hit(b) {
            out.push(at(b));
        }
        if out.len() >= 6 {
            break;
        }
    }
    // Callbacks of the outside services the journey reaches.
    for s in steps.iter().filter(|s| s["kind"] == "outside") {
        let Some(o) = node_for(table, s) else {
            continue;
        };
        for a in table[&o]["next"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|x| x.as_str())
        {
            if table[a]["kind"] == "async" && !hit(a) && !out.contains(&at(a)) {
                out.push(at(a));
            }
        }
    }
    out
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
pub const JOURNEY_SECS: u64 = 360;
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

- Start at the entry screen you are given: the first step cites it, in its file.
- Follow every request the journey's screens make, each in the order the code makes it (a check, then the save, then the confirmation), not one of them.
- When a handler passes a request on (a catch-all route like `@Post('*')`, a proxy middleware, a client wrapper), follow it to the client that calls the outside service and cite the setting that holds that service's address (`BOOKINGS_API_URL`, `STRIPE_SECRET_KEY`) as the outside step.
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
pub fn check_journey(
    snapshot: &Snapshot,
    input: &Value,
    entry_file: Option<&str>,
) -> Result<Value, Vec<String>> {
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
    if let (Some(f), Some(first)) = (entry_file, steps.first()) {
        if first["file"].as_str() != Some(f) {
            problems.push(format!(
                "the first step is the entry screen you were given, in {f}"
            ));
        }
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
    /// The entry screen's file: every journey starts there.
    pub entry_file: Option<String>,
    /// The entry's candidate id, for what the map knows it does.
    pub entry_id: Option<String>,
    /// Tool calls added for the one retry.
    pub extra: AtomicUsize,
    pub retried: std::sync::atomic::AtomicBool,
    /// What was accepted before the retry.
    pub before: Mutex<Vec<Value>>,
    /// The candidates and the entry's kind, for the structural checks.
    pub todo: Option<&'a Value>,
    pub family: String,
    /// Journeys sent back once for stopping short (title to the reason).
    pub sent_back: Mutex<BTreeMap<String, String>>,
    /// Journeys sent back and not yet resubmitted, kept as open.
    pub held: Mutex<Vec<Value>>,
}

impl<'a> TraceTools<'a> {
    pub fn new(snapshot: &'a Snapshot, lookups: &'a Mutex<am::Lookups>) -> Self {
        Self {
            snapshot,
            lookups,
            answers: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            entry_file: None,
            entry_id: None,
            extra: AtomicUsize::new(0),
            retried: std::sync::atomic::AtomicBool::new(false),
            before: Mutex::new(Vec::new()),
            todo: None,
            family: String::new(),
            sent_back: Mutex::new(BTreeMap::new()),
            held: Mutex::new(Vec::new()),
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
            "submit" => match check_journey(self.snapshot, input, self.entry_file.as_deref()) {
                Ok(mut j) => {
                    // A journey that stops short goes back once, with fresh
                    // tool calls; the second time it is kept, marked open.
                    let steps = j["steps"].as_array().cloned().unwrap_or_default();
                    let fam = title_family(j["title"].as_str().unwrap_or(""))
                        .map(str::to_string)
                        .unwrap_or_else(|| self.family.clone());
                    let mut why = Vec::new();
                    if let Some(end) = short_of_end(&fam, &steps) {
                        why.push(format!("it stops before {end}"));
                    }
                    if let Some(p) = self
                        .todo
                        .and_then(|t| proxied_service_missing(t, &steps, Some(self.snapshot)))
                    {
                        why.push(p);
                    }
                    if let (Some(t), Some(e)) = (self.todo, self.entry_id.as_deref()) {
                        let missing = left_out(t, e, &steps);
                        if !missing.is_empty() {
                            why.push(format!(
                                "the map shows this screen's code also makes these, which the journey leaves out: {} (add each where it runs; leave out one that belongs to another journey)",
                                missing.join("; ")
                            ));
                        }
                    }
                    let key = j["title"].as_str().unwrap_or("").to_string();
                    let mut sent = self.sent_back.lock().unwrap_or_else(|e| e.into_inner());
                    if !why.is_empty() && !sent.contains_key(&key) {
                        sent.insert(key, why.join("; "));
                        self.extra.fetch_add(MAX_CALLS, Ordering::Relaxed);
                        // Kept, open, in case the time runs out before the
                        // journey comes back; a resubmit replaces it.
                        let mut held = j.clone();
                        held["open"] = json!(true);
                        self.held
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(held);
                        return Ok(json!(format!(
                            "Not accepted yet: {}. Read on in the code and submit the whole journey again, from the entry screen to its end. You have {MAX_CALLS} more tool calls.",
                            why.join("; ")
                        )));
                    }
                    drop(sent);
                    if !why.is_empty() {
                        j["open"] = json!(true);
                    }
                    // The resubmit stands in for the journey held back.
                    let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
                    if !held.is_empty() {
                        held.remove(0);
                    }
                    drop(held);
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

/// The kinds of journey and the words a name of each has (as the
/// sandbox's `WORDS`).
const FAMILY_WORDS: [(&str, &str); 5] = [
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

/// The kind of journey a name speaks of.
pub fn family_of(text: &str) -> Option<&'static str> {
    FAMILY_WORDS.iter().find_map(|(f, re)| {
        regex::Regex::new(re)
            .ok()
            .filter(|r| r.is_match(text))
            .map(|_| *f)
    })
}

/// Whether `text` has the words of the kind `family`.
fn family_matches(family: &str, text: &str) -> bool {
    FAMILY_WORDS
        .iter()
        .find(|(f, _)| *f == family)
        .and_then(|(_, re)| regex::Regex::new(re).ok())
        .is_some_and(|r| r.is_match(text))
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
        // A screen that is one renderer of a file holding several takes the
        // lines it spans (a near citation still wins).
        let span = (c["span"][0].as_i64(), c["span"][1].as_i64());
        if let (Some(a), Some(b)) = span {
            let in_file = c["evidence"][0]["file"].as_str() == Some(file);
            if in_file && line >= a && line <= b && best.as_ref().is_none_or(|(d, _)| 3 < *d) {
                best = Some((3, id.clone()));
            }
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
    /// Entries left for a later pass by the budget.
    pub over_budget: usize,
    /// Journeys kept that stopped short of their end.
    pub open: usize,
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
        let mut step = step;
        if s["from_map"] == true {
            step["from_map"] = json!(true);
        }
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
                // A step completed from the map is already where the code
                // puts it (after the request it serves).
                .filter(|(i, _)| !units[*i].1.iter().all(|s| s["from_map"] == true))
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
    let mut steps: Vec<Value> = units.into_iter().flat_map(|(_, v)| v).collect();
    for st in &mut steps {
        if let Some(o) = st.as_object_mut() {
            o.remove("from_map");
        }
    }
    // A journey that stopped short says so, quietly, on its last step.
    if journey["open"] == true {
        if let Some(last) = steps.last_mut() {
            let note = last["note"].as_str().unwrap_or("").trim().to_string();
            last["note"] = json!(format!("{note} The journey may continue past here.")
                .trim()
                .to_string());
        }
    }
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
        // The entry it came from: its retrace replaces it.
        "from": [table[entry_id]["key"]],
        "origin": "trace",
        "open": journey["open"] == true,
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
    /// The traces as the model gave them (the eval saves them, to re-check
    /// a run with no model call).
    #[cfg_attr(not(test), expect(dead_code))]
    pub raw: Vec<Value>,
}

impl TraceOut {
    /// Nothing traced (every entry waits on a repository).
    pub fn empty() -> Self {
        Self {
            flows: Vec::new(),
            report: Traced::default(),
            failed: Vec::new(),
            left: Vec::new(),
            raw: Vec::new(),
        }
    }
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
    budget: f64,
    done: Option<&tokio::sync::mpsc::UnboundedSender<Done>>,
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
    let running = AtomicUsize::new(0);
    let finished = AtomicUsize::new(0);
    let largest = Mutex::new(0.0f64);
    let results: Vec<EntryTraces> = futures::stream::iter(entries.iter().cloned())
        .map(|entry| {
            let lookups = &lookups;
            let covered = &covered;
            let rep = &rep;
            let usage_all = &usage_all;
            let left = &left;
            let running = &running;
            let finished = &finished;
            let largest = &largest;
            async move {
                // The budget, checked before the entry starts and counting the
                // entries already running at what an entry has cost so far: a
                // pass never goes over; what doesn't fit waits for the next.
                {
                    let spent = usage_all.lock().unwrap_or_else(|e| e.into_inner()).cost_usd;
                    if !fits_budget(spent, *largest.lock().unwrap_or_else(|e| e.into_inner()), running.load(Ordering::Relaxed), budget) {
                        rep.lock().unwrap_or_else(|e| e.into_inner()).over_budget += 1;
                        left.lock().unwrap_or_else(|e| e.into_inner()).push(entry["id"].clone());
                        return (entry, Vec::new());
                    }
                    running.fetch_add(1, Ordering::Relaxed);
                }
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
                    if let Some(d) = done {
                        let _ = d.send(Done { entry: entry.clone(), flows: Vec::new(), cost_usd: 0.0, covered: true });
                    }
                    return (entry, Vec::new());
                }
                let mut exec = TraceTools::new(snapshot, lookups);
                exec.todo = Some(todo);
                exec.entry_id = entry["id"].as_str().map(str::to_string);
                exec.family = entry["family"].as_str().unwrap_or("").replace('-', " ");
                exec.entry_file = todo["table"][entry["id"].as_str().unwrap_or("")]["evidence"][0]["file"]
                    .as_str()
                    .map(str::to_string);
                let mut messages = vec![(
                    "user".to_string(),
                    vec![json!({"type": "text", "text": user_prompt(todo, &entry)})],
                )];
                let mut u = crate::models::TokenUsage::default();
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
                        None,
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
                running.fetch_sub(1, Ordering::Relaxed);
                finished.fetch_add(1, Ordering::Relaxed);
                {
                    let mut big = largest.lock().unwrap_or_else(|e| e.into_inner());
                    *big = big.max(u.cost_usd);
                }
                let rejected = messages
                    .iter()
                    .flat_map(|(_, b)| b.iter())
                    .filter(|b| b["type"] == "tool_result" && b.to_string().contains("Not accepted."))
                    .count();
                let calls = exec.calls.load(Ordering::Relaxed);
                let mut answers = exec.answers.into_inner().unwrap_or_else(|e| e.into_inner());
                answers.extend(exec.held.into_inner().unwrap_or_else(|e| e.into_inner()));
                // A retry that gave nothing keeps what came before.
                if answers.is_empty() {
                    answers = exec.before.into_inner().unwrap_or_else(|e| e.into_inner());
                }
                let mut r = rep.lock().unwrap_or_else(|e| e.into_inner());
                r.rejected_submits += rejected;
                r.tool_calls += calls;
                if answers.is_empty() {
                    r.failed += 1;
                    drop(r);
                    if let Some(d) = done {
                        let _ = d.send(Done { entry: entry.clone(), flows: Vec::new(), cost_usd: u.cost_usd, covered: false });
                    }
                    return (entry, Vec::new());
                }
                r.traced += 1;
                drop(r);
                let made: Vec<(Value, usize, Value)> = answers
                    .into_iter()
                    .map(|j| {
                        // What the static map says comes next, where the
                        // trace stopped short of it.
                        let mut done = j.clone();
                        let fam = journey_family(&done, &entry);
                        complete_journey(&mut done, &fam, todo, Some(snapshot));
                        let (f, c) = to_flow(&done, &entry, todo, evidence_order);
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
                drop(cov);
                // Each journey is handed on as it finishes, for the caller to
                // keep (a checkpoint) before the next one ends.
                if let Some(d) = done {
                    let kept: Vec<Value> = made.iter().map(|(f, _, _)| f.clone()).filter(keepable).collect();
                    let _ = d.send(Done { entry: entry.clone(), flows: kept, cost_usd: u.cost_usd, covered: false });
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
            if !keepable(&flow) {
                continue;
            }
            let steps = flow["steps"].as_array().map(Vec::len).unwrap_or(0);
            kept += 1;
            rep.steps += steps;
            rep.on_map += flow["steps"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|s| !s["key"].as_str().unwrap_or("").starts_with("code:"))
                .count();
            rep.order_conflicts += conflicts;
            rep.open += usize::from(flow["open"] == true);
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
        // `TRACE_BUDGET` caps the run as one pass would be (unset: no cap).
        let budget: f64 = std::env::var("TRACE_BUDGET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(f64::MAX);
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
            let snapshot = {
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
                Snapshot { files }
            };
            let (flows, rep, raw) = if let Some(saved) = saved {
                let mut rep = Traced::default();
                let flows: Vec<Value> = saved
                    .iter()
                    .map(|t| {
                        let mut done = t["journey"].clone();
                        let fam = journey_family(&done, &t["entry"]);
                        complete_journey(&mut done, &fam, &todo, Some(&snapshot));
                        let (f, c) = to_flow(&done, &t["entry"], &todo, evidence_order);
                        rep.order_conflicts += c;
                        f
                    })
                    .collect();
                (flows, rep, saved)
            } else {
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
                    budget,
                    None,
                )
                .await;
                let left: Vec<Value> = out.left.iter().map(|e| e["id"].clone()).collect();
                std::fs::write(
                    dir.join(format!("{set}.left.json")),
                    serde_json::to_vec_pretty(&left).unwrap(),
                )
                .unwrap();
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
    fn a_journey_that_stops_short_of_its_end_or_of_a_proxied_service_is_found() {
        let st = |kind: &str, file: &str, line: u64, token: &str| json!({"kind": kind, "file": file, "line": line, "token": token});
        let reset = vec![
            st("screen", "web/forgot.tsx", 3, "ForgotForm"),
            st("call", "web/recover.ts", 9, "/proxy/recover"),
            st("screen", "web/forgot.tsx", 20, "Check your email"),
        ];
        // One call and no new password set: short of a reset's end.
        assert!(short_of_end("password reset", &reset)
            .is_some_and(|e| e.starts_with("the new password being set")));
        let mut done = reset.clone();
        done.push(st("call", "web/reset.ts", 4, "/reset-password"));
        done.push(st("screen", "web/updated.tsx", 2, "PasswordUpdated"));
        assert_eq!(short_of_end("password reset", &done), None);
        // Two calls are not enough when none saves the new password.
        let mut two = reset.clone();
        two.push(st("call", "web/recover.ts", 30, "/proxy/resend"));
        two.push(st("screen", "web/forgot.tsx", 40, "Sent"));
        assert!(short_of_end("password reset", &two).is_some());
        // Signed in: a screen after the last call.
        assert!(short_of_end("sign in", &reset[..2]).is_some());
        assert_eq!(short_of_end("sign in", &reset), None);
        let todo = json!({"table": {
            "R1": {"key": "req:ALL /proxy*", "kind": "request", "label": "ALL /proxy*", "all": "api/proxy.module.ts", "pattern": "/proxy*", "next": ["X1"],
                   "evidence": [{"file": "web/recover.ts", "line": 9, "token": "/proxy/recover"}]},
            "X1": {"key": "n:vendor", "kind": "outside", "label": "Vendor API", "next": [],
                   "evidence": [{"file": "api/proxy.module.ts", "line": 12, "token": "VENDOR_API"}]},
        }});
        // Found by where the request goes, whatever line the trace cites.
        let mut elsewhere = reset.clone();
        elsewhere[1] = st("call", "web/other-client.ts", 40, "`${base}/proxy/recover`");
        assert!(proxied_service_missing(&todo, &elsewhere, None).is_some());
        assert!(
            pattern_takes("/vendor/*", "/vendor/persons/x")
                && pattern_takes("/m*", "/m/login")
                && !pattern_takes("/api/*", "/apix")
        );
        assert_eq!(
            request_path("`${apiBase}/a/b/${id}?x=1`").as_deref(),
            Some("/a/b/:p")
        );
        let p = proxied_service_missing(&todo, &reset, None).unwrap();
        assert!(
            p.contains("Vendor API (the map found it at api/proxy.module.ts:12)"),
            "{p}"
        );
        let mut with = reset.clone();
        with.insert(2, st("outside", "api/proxy.module.ts", 12, "VENDOR_API"));
        assert_eq!(proxied_service_missing(&todo, &with, None), None);
    }

    #[test]
    fn a_journey_is_completed_from_the_map_where_the_code_says_what_comes_next() {
        let files: BTreeMap<String, Vec<u8>> = [
            ("api/src/proxy/proxy.module.ts", "import { client } from './vendor-client';\napp.use('/proxy*', forward);\n"),
            ("api/src/proxy/vendor-client.ts", "export const base = process.env.VENDOR_API;\n"),
            ("web/src/actions/recover.ts", "x\nx\nclient.post('/proxy/recover', body);\n"),
            ("web/src/actions/save.ts", "client.post('/proxy/password/updatePassword', body);\n"),
            ("web/src/lib/session.ts", "const body = { returnUrl: `${origin}/pay/back` };\npost('/stripe/session', body);\n"),
            ("api/src/orders/orders.service.ts", "import { Ledger } from '../ledger/ledger.service';\nexport class Orders {\n  constructor(private readonly ledger: Ledger) {}\n  async place(order: { id: string }) {\n    return this.ledger.record(order);\n  }\n}\n"),
            ("api/src/ledger/ledger.service.ts", "export class Ledger {\n  private base = process.env.LEDGER_API;\n  async record(o: unknown) {\n    return fetch(this.base, o);\n  }\n}\n"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
        .collect();
        let snapshot = Snapshot { files };
        let todo = json!({"table": {
            "R1": {"key": "req:ALL /proxy*", "kind": "request", "label": "ALL /proxy*", "all": "api/src/proxy/proxy.module.ts", "pattern": "/proxy*", "next": ["X2"],
                   "evidence": [{"file": "web/src/actions/recover.ts", "line": 3, "token": "/proxy/recover"}, {"file": "api/src/proxy/proxy.module.ts", "line": 2, "token": "app.use"}]},
            "X1": {"key": "n:vendor", "kind": "outside", "label": "Vendor", "next": [],
                   "evidence": [{"file": "api/.env.sample", "line": 4, "token": "VENDOR_API"}]},
            "X2": {"key": "n:logger", "kind": "outside", "label": "Logger", "next": [],
                   "evidence": [{"file": "scripts/x.ts", "line": 9, "token": "@logger/node"}]},
            "S1": {"key": "screen:web:/reset-password/:token", "kind": "screen", "label": "/reset-password/:token", "next": [],
                   "evidence": [{"file": "web/src/app/reset-password/[token]/page.tsx", "line": 5, "token": "ResetPage"}]},
            "S2": {"key": "screen:web:/pay/back", "kind": "screen", "label": "/pay/back", "next": [],
                   "evidence": [{"file": "web/src/app/pay/back/page.tsx", "line": 3, "token": "PayBack"}]},
            "X3": {"key": "n:ledger", "kind": "outside", "label": "Ledger", "next": [],
                   "evidence": [{"file": "api/.env.sample", "line": 7, "token": "LEDGER_API"}]},
            "A1": {"key": "hook:POST /stripe/webhook", "kind": "async", "label": "POST /stripe/webhook", "next": [],
                   "evidence": [{"file": "api/src/pay/hooks.ts", "line": 7, "token": "'webhook'"}]},
        }});
        let st = |kind: &str, file: &str, line: u64, token: &str| json!({"kind": kind, "file": file, "line": line, "token": token});
        assert_eq!(
            title_family("Reset a forgotten password from the login form"),
            Some("password reset")
        );
        assert_eq!(title_family("Buy a plan to join"), Some("checkout"));
        // A reset that stops at "check your email" through a proxy.
        let mut reset = json!({"title": "Reset a forgotten password from the login form", "steps": [
            st("screen", "web/forgot.tsx", 3, "ForgotForm"),
            st("call", "web/src/actions/recover.ts", 3, "/proxy/recover"),
            st("screen", "web/forgot.tsx", 20, "Check your email"),
        ]});
        let fam = journey_family(&reset, &json!({}));
        assert!(complete_journey(&mut reset, &fam, &todo, Some(&snapshot)) > 0);
        let got: Vec<(String, String, u64)> = reset["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    s["kind"].as_str().unwrap().to_string(),
                    s["file"].as_str().unwrap().to_string(),
                    s["line"].as_u64().unwrap(),
                )
            })
            .collect();
        let want = |k: &str, f: &str, l: u64| (k.to_string(), f.to_string(), l);
        assert_eq!(
            got,
            vec![
                want("screen", "web/forgot.tsx", 3),
                want("call", "web/src/actions/recover.ts", 3),
                // The route's handler, then the service its code reads (not
                // the package the map also ties to it).
                want("handler", "api/src/proxy/proxy.module.ts", 2),
                want("outside", "api/src/proxy/vendor-client.ts", 1),
                want("screen", "web/forgot.tsx", 20),
                // The reset page, and the request its app's code posts to
                // save the password, landing on the route that takes it.
                want("screen", "web/src/app/reset-password/[token]/page.tsx", 5),
                want("call", "web/src/actions/save.ts", 1),
                want("handler", "api/src/proxy/proxy.module.ts", 2),
            ]
        );
        assert!(reset["steps"].as_array().unwrap()[2..]
            .iter()
            .filter(|s| s["file"] != "web/forgot.tsx")
            .all(|s| s["from_map"] == true));
        // Every added step's line holds its token.
        for s in reset["steps"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["from_map"] == true)
        {
            let f = s["file"].as_str().unwrap();
            if let Some(b) = snapshot.files.get(f) {
                let line = String::from_utf8_lossy(b)
                    .lines()
                    .nth(s["line"].as_u64().unwrap() as usize - 1)
                    .unwrap()
                    .to_string();
                assert!(line.contains(s["token"].as_str().unwrap()), "{f}: {line}");
            }
        }
        // A payment gets the page its code sends the user back to and the
        // payment service's webhook.
        let mut pay = json!({"title": "Buy a plan to join", "steps": [
            st("screen", "web/plan.tsx", 1, "Plan"),
            st("call", "web/src/lib/session.ts", 2, "/stripe/session"),
        ]});
        let fam = journey_family(&pay, &json!({}));
        complete_journey(&mut pay, &fam, &todo, Some(&snapshot));
        let files: Vec<&str> = pay["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["file"].as_str().unwrap())
            .collect();
        assert_eq!(
            files,
            vec![
                "web/plan.tsx",
                "web/src/lib/session.ts",
                "web/src/app/pay/back/page.tsx",
                "api/src/pay/hooks.ts"
            ]
        );
        // A service step gets the outside service its code reaches, through
        // the injected service it calls, cited where the setting is read.
        let mut order = json!({"title": "Place an order", "steps": [
            st("screen", "web/cart.tsx", 1, "Cart"),
            st("service", "api/src/orders/orders.service.ts", 4, "place"),
        ]});
        complete_journey(&mut order, "", &todo, Some(&snapshot));
        let last = order["steps"].as_array().unwrap().last().unwrap().clone();
        assert_eq!(
            (
                last["kind"].as_str(),
                last["file"].as_str(),
                last["line"].as_u64()
            ),
            (
                Some("outside"),
                Some("api/src/ledger/ledger.service.ts"),
                Some(2)
            )
        );
    }

    #[test]
    fn a_pass_never_starts_an_entry_past_its_budget() {
        // Nothing spent, two running at the estimate: room for one more.
        assert!(fits_budget(0.0, 0.0, 2, 1.0));
        assert!(!fits_budget(0.0, 0.0, 4, 1.0));
        // Held at the largest entry so far (0.40), 2 running.
        assert!(fits_budget(1.2, 0.4, 2, 2.5));
        assert!(!fits_budget(1.2, 0.4, 3, 2.5));
        // The day's budget caps the pass's.
        assert_eq!(pass_budget(0.0), BUDGET_USD);
        assert_eq!(pass_budget(DAILY_BUDGET_USD - 1.0), 1.0);
        assert_eq!(pass_budget(DAILY_BUDGET_USD + 1.0), 0.0);
        assert_eq!(
            spent_today(&json!({"day": "2026-01-02", "usd": 4.0}), "2026-01-02"),
            4.0
        );
        assert_eq!(
            spent_today(&json!({"day": "2026-01-01", "usd": 4.0}), "2026-01-02"),
            0.0
        );
    }

    /// A pass that stops after any journey resumes with the next entry: no
    /// entry is traced twice, none is lost, and the store ends the same.
    #[test]
    fn a_pass_killed_mid_map_resumes_without_redoing_work() {
        let entry = |id: &str| json!({"id": id, "key": format!("screen:web:/{id}"), "hash": format!("h-{id}"), "family": "sign-in", "app": "web"});
        let todo =
            json!({"entries": [entry("a"), entry("b"), entry("c"), entry("d")], "table": {}});
        let flow = |id: &str| {
            json!({"id": format!("j-{id}"), "title": id, "entry": format!("screen:web:/{id}"), "origin": "trace",
            "steps": [{"key": format!("screen:web:/{id}"), "kind": "screen"}, {"key": "req:GET /x", "kind": "request"}, {"key": "n:y", "kind": "outside"}]})
        };
        let traced = std::cell::RefCell::new(Vec::<String>::new());
        let trace = |e: &Value| {
            let id = e["id"].as_str().unwrap().to_string();
            traced.borrow_mut().push(id.clone());
            Done {
                entry: e.clone(),
                flows: vec![flow(&id)],
                cost_usd: 0.1,
                covered: false,
            }
        };
        // The first pass is killed after its second journey: what was
        // written down is the store and the queue after that checkpoint.
        let mut store = json!({"schema": fl::SCHEMA, "flows": []});
        let mut queue = todo.clone();
        for e in todo["entries"].as_array().unwrap().iter().take(2) {
            let (s, q) = checkpoint(&store, &queue, &trace(e));
            store = s;
            queue = q;
        }
        assert_eq!(store["pending"], 2);
        // The resumed pass reads them back and goes on.
        let saved_store: Value = serde_json::from_str(&store.to_string()).unwrap();
        let saved_queue: Value = serde_json::from_str(&queue.to_string()).unwrap();
        let (mut store, mut queue) = (saved_store, saved_queue);
        while let Some(e) = queue["entries"].as_array().and_then(|a| a.first()).cloned() {
            let (s, q) = checkpoint(&store, &queue, &trace(&e));
            store = s;
            queue = q;
        }
        assert_eq!(*traced.borrow(), ["a", "b", "c", "d"]);
        assert_eq!(store["pending"], 0);
        let ids: Vec<&str> = store["flows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["id"].as_str().unwrap())
            .collect();
        // Each id is its title's.
        assert_eq!(ids, ["a", "b", "c", "d"]);
        for id in ["a", "b", "c", "d"] {
            assert_eq!(
                store["asked"][format!("screen:web:/{id}")],
                format!("h-{id}")
            );
        }
    }

    #[test]
    fn a_trace_keeps_only_steps_whose_line_holds_their_token() {
        let s = snapshot();
        let ok = json!({"title": "Sign in", "steps": [
            {"kind": "screen", "file": "harbor-web/app/login/page.tsx", "line": 2, "token": "LoginPage", "note": "The sign-in page."},
            {"kind": "call", "file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/session", "note": "Posts the session."},
        ]});
        let j = check_journey(&s, &ok, None).unwrap();
        // The line moves onto the token.
        assert_eq!(j["steps"][0]["line"], 3);
        let bad = json!({"title": "Sign in", "steps": [
            {"kind": "screen", "file": "harbor-web/app/login/page.tsx", "line": 3, "token": "LoginPage", "note": ""},
            {"kind": "call", "file": "harbor-web/lib/session.ts", "line": 2, "token": "/api/sessions/new", "note": ""},
            {"kind": "handler", "file": "harbor-api/src/session.ts", "line": 1, "token": "POST", "note": ""},
        ]});
        let p = check_journey(&s, &bad, None).unwrap_err();
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

    /// An address reaches the screen whose pattern fits it best: an exact
    /// path a resolver gives one dispatched page beats the catch-all, and
    /// a catch-all's dispatched pages, which differ only by a type the
    /// address does not show, reach none of them.
    #[test]
    fn an_address_reaches_its_best_fitting_screen() {
        let ids: Vec<String> = ["S1", "S2", "S3", "S4", "S5"]
            .iter()
            .map(|x| x.to_string())
            .collect();
        let rows_v = [
            json!({"key": "screen:web:/*", "kind": "screen"}),
            json!({"key": "screen:web:/members/join", "kind": "screen"}),
            json!({"key": "screen:web:/booths/:segment2", "kind": "screen"}),
            json!({"key": "screen:web:/(notice)/*", "kind": "screen"}),
            json!({"key": "screen:web:/(story)/*", "kind": "screen"}),
        ];
        let rows: Vec<(&String, &Value)> = ids.iter().zip(rows_v.iter()).collect();
        assert_eq!(
            screen_for_path(&rows, "/members/join").map(String::as_str),
            Some("S2")
        );
        assert_eq!(
            screen_for_path(&rows, "/booths/${id}").map(String::as_str),
            Some("S3")
        );
        assert_eq!(
            screen_for_path(&rows, "/members/join?from=home").map(String::as_str),
            Some("S2")
        );
        // the catch-all and its two dispatched pages fit equally: none
        assert_eq!(screen_for_path(&rows, "/news/high-tide"), None);
        // with only the route itself it is the route
        let one: Vec<(&String, &Value)> = rows[..1].to_vec();
        assert_eq!(
            screen_for_path(&one, "/news/high-tide").map(String::as_str),
            Some("S1")
        );
        assert_eq!(route_score("/(notice)/*", &[]), None);
    }

    /// A traced step inside one renderer of a file that holds several
    /// lands on that renderer's screen, not on its neighbour's.
    #[test]
    fn a_step_inside_a_renderer_lands_on_its_screen() {
        let table = json!({
            "S1": {"key": "screen:web:/members/join", "kind": "screen", "span": [19, 23], "evidence": [{"file": "web/lib/renderers.tsx", "line": 19}]},
            "S2": {"key": "screen:web:/members/checkout", "kind": "screen", "span": [24, 28], "evidence": [{"file": "web/lib/renderers.tsx", "line": 24}]},
        });
        let at = |line: i64| {
            node_for(
                &table,
                &json!({"kind": "screen", "file": "web/lib/renderers.tsx", "line": line}),
            )
        };
        assert_eq!(at(21).as_deref(), Some("S1"));
        assert_eq!(at(27).as_deref(), Some("S2"));
        // a citation near a screen's own line still wins
        assert_eq!(at(25).as_deref(), Some("S2"));
        assert_eq!(at(40), None);
    }

    #[tokio::test]
    async fn a_rate_limit_waits_out_a_short_reset_and_defers_a_long_one_past_it() {
        use crate::clients::github::RateLimited;
        let now = 1_000_000;
        // A reset within the pass: waited out, then read.
        let tries = std::sync::atomic::AtomicUsize::new(0);
        let slept = Mutex::new(Vec::new());
        let got = read_repo(
            || {
                let n = tries.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 0 {
                        Err::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                            RateLimited::for_test(10),
                        ))
                    } else {
                        Ok(vec![7])
                    }
                }
            },
            |s| {
                slept.lock().unwrap().push(s);
                async {}
            },
            &READ_RETRY_SECS,
        )
        .await;
        assert_eq!(got, Ok(vec![7]));
        assert_eq!(*slept.lock().unwrap(), vec![10]);
        // A reset half an hour away: not slept on; read again after it.
        let slept = Mutex::new(Vec::new());
        let got = read_repo(
            || async {
                Err::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    RateLimited::for_test(1_800),
                ))
            },
            |s| {
                slept.lock().unwrap().push(s);
                async {}
            },
            &READ_RETRY_SECS,
        )
        .await;
        assert!(slept.lock().unwrap().is_empty());
        let fail = got.unwrap_err();
        assert!(matches!(
            fail,
            RepoFail::Limited {
                wait_secs: 1_800,
                ..
            }
        ));
        let mut unread = json!({});
        note_unread(&mut unread, "harbor/harbor-etl", &fail, now);
        let until = unread["harbor/harbor-etl"]["until"].as_i64().unwrap();
        assert!(until > now + 1_800, "after GitHub's reset");
        assert!(!readable_now(&unread, "harbor/harbor-etl", now + 1_800));
        assert!(readable_now(&unread, "harbor/harbor-etl", until));
        // The resume for the entries waiting on it is at that time, not sooner.
        let p = PassEnd {
            retry_at: vec![until],
            ..Default::default()
        };
        assert_eq!(next_resume(&p, now), Some(until));
        // Other refusals back off, doubling, up to the most.
        let other = RepoFail::Failed { why: "403".into() };
        let mut u = json!({});
        let mut waits = Vec::new();
        for _ in 0..6 {
            note_unread(&mut u, "r", &other, now);
            waits.push(u["r"]["until"].as_i64().unwrap() - now);
        }
        assert_eq!(waits, [300, 600, 1_200, 2_400, 3_600, 3_600]);
    }

    #[test]
    fn an_over_budget_pass_queues_one_resume_at_the_reset_and_nothing_sooner() {
        let now = 1_800_000_000 / 86_400 * 86_400 + 15 * 3_600;
        let reset = budget_reset(now);
        assert_eq!(reset - now, 9 * 3_600);
        // Less left of the day than one entry costs: the reset, nothing sooner.
        let spent = PassEnd {
            runnable: 158,
            day_spent: DAILY_BUDGET_USD - 0.1,
            ..Default::default()
        };
        assert!(!can_start(pass_budget(spent.day_spent)));
        assert_eq!(next_resume(&spent, now), Some(reset));
        // Even when a pass made progress before the budget ran out.
        let some = PassEnd {
            progressed: true,
            ..spent.clone()
        };
        assert_eq!(next_resume(&some, now), Some(reset));
        // Room left and progress: soon; out of time: at once.
        let room = PassEnd {
            progressed: true,
            runnable: 5,
            day_spent: 1.0,
            ..Default::default()
        };
        assert_eq!(next_resume(&room, now), Some(now + 60));
        let timed = PassEnd {
            by_time: true,
            ..room.clone()
        };
        assert_eq!(next_resume(&timed, now), Some(now));
        // No progress though something could run: never within seconds.
        let stuck = PassEnd {
            runnable: 5,
            day_spent: 1.0,
            ..Default::default()
        };
        assert_eq!(next_resume(&stuck, now), Some(now + IDLE_SECS));
        let soon = PassEnd {
            retry_at: vec![now + 30],
            ..Default::default()
        };
        assert_eq!(next_resume(&soon, now), Some(now + IDLE_SECS));
        // Nothing left: nothing queued.
        assert_eq!(next_resume(&PassEnd::default(), now), None);
        // The schedule: one message runs, at its time; every other is dropped.
        let first = reschedule(&Value::Null, reset);
        let turn = turn_of(&first);
        assert_eq!(
            take_message(turn, &Value::Null, now),
            Take::Run,
            "a team from before the schedule"
        );
        assert_eq!(take_message(turn, &first, now), Take::Hop(MAX_DELAY_SECS));
        assert_eq!(take_message(turn, &first, reset - 100), Take::Hop(100));
        assert_eq!(take_message(turn, &first, reset), Take::Run);
        assert_eq!(take_message(turn + 1, &first, reset), Take::Stale);
        assert_eq!(
            take_message(0, &first, reset),
            Take::Stale,
            "the old step's messages"
        );
        let after = reschedule(&first, now);
        assert_eq!(take_message(turn, &after, reset), Take::Stale);
    }

    #[test]
    fn an_entry_waits_only_on_the_repositories_its_journey_reads() {
        let repo_of: BTreeMap<String, String> = [
            ("harbor-web", "o/harbor-web"),
            ("harbor-api", "o/harbor-api"),
            ("harbor-etl", "o/harbor-etl"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        // An older todo with no files: the table's rows the entry leads to.
        let todo = json!({
            "entries": [{"id": "S1", "key": "screen:web:/login"}, {"id": "S2", "key": "screen:web:/export", "files": ["harbor-web/app/export.tsx", "harbor-etl/jobs/export.py"]}],
            "table": {"S1": {"evidence": [{"file": "harbor-web/app/login.tsx"}], "next": ["R1"]}, "R1": {"evidence": [{"file": "harbor-api/src/auth.ts"}], "next": ["S1"]}},
        });
        let e = |i: usize| todo["entries"][i].clone();
        assert_eq!(
            entry_repos(&todo, &e(0), &repo_of)
                .into_iter()
                .collect::<Vec<_>>(),
            ["o/harbor-api", "o/harbor-web"]
        );
        assert_eq!(
            entry_repos(&todo, &e(1), &repo_of)
                .into_iter()
                .collect::<Vec<_>>(),
            ["o/harbor-etl", "o/harbor-web"]
        );
        let mut unread = json!({});
        note_unread(
            &mut unread,
            "o/harbor-etl",
            &RepoFail::Failed { why: "404".into() },
            100,
        );
        let (ready, wait) = split_ready(&todo, &repo_of, &unread, 100);
        assert_eq!(ready.len(), 1);
        assert_eq!(wait[0].1, "o/harbor-etl");
        assert_eq!(waiting_repos(&unread, 100).len(), 1);
        assert!(waiting_repos(&unread, 100 + REPO_RETRY_SECS).is_empty());
    }
}
