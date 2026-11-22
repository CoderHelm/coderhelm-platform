//! The app map: CoderHelm's adapter around Floorplan (vendored at
//! `vendor/floorplan`, MIT, https://github.com/haydenschmitty/floorplan).
//!
//! Floorplan does the mapping: its extractor finds every repository's
//! files, imports, endpoints, requests and screens, matches requests to
//! endpoints across repositories, and its builder writes one map
//! (`floorplan/graph@1`) with Health, screens and the tour. The worker runs
//! it in the run sandbox (`worker::passes::app_map`, driver in
//! `services/worker/floorplan/driver.mjs`) and does the model jobs its
//! `workflow.json` describes with the team's own model, metered.
//!
//! This module is what CoderHelm adds, kept out of the vendored files:
//!
//! - **Storage**: the map, the run state and the step's job in the bucket
//!   (`app-map/{team}/...`), the build status in the settings table
//!   (`pk = AM#{team}`, `sk = STATUS`).
//! - **Overlays in Floorplan's own fields**, so its viewer shows them
//!   natively: CoderHelm's recent runs become the map's `recent` (tasks and
//!   items, the shape `recent-work.json` becomes), and CoderHelm's security,
//!   infrastructure, log and alert findings become `health.issues`, marked as
//!   CoderHelm's; Floorplan's own checks stay, marked as Floorplan's, and one
//!   that names the same file and line as a CoderHelm finding is dropped
//!   ([`with_overlays`]).
//! - **The page's copy** ([`for_viewer`]): the sandbox's folder left out, and
//!   marked as an exported, read-only map so the viewer never calls a server.
//! - **Lookups** for the read API and Ask ([`overview`], [`part_view`],
//!   [`file_view`], [`screen_view`], [`search`], [`sub_map`]), in the spirit
//!   of Floorplan's `graph-query.js`.
//! - **Answer checks** ([`check_answer`]): a model job's answer against the
//!   schema `workflow.json` gives it, before it is written.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// The Floorplan release vendored with this build.
pub const FLOORPLAN_VERSION: &str = include_str!("../../../vendor/floorplan/VERSION");
/// Floorplan's job file: the model jobs, their instructions and schemas.
pub const WORKFLOW_JSON: &str =
    include_str!("../../../vendor/floorplan/skills/floorplan/workflow.json");
/// The map format this adapter reads.
pub const GRAPH_SCHEMA: &str = "floorplan/graph@1";
/// Largest map served to the page (bytes of JSON).
pub const MAX_GRAPH_BYTES: usize = 25 * 1024 * 1024;

// ── Storage ────────────────────────────────────────────────────────────────

pub fn map_pk(team_id: &str) -> String {
    format!("AM#{team_id}")
}

fn s3_prefix(team_id: &str) -> String {
    // A team id may hold `#`: keep keys to plain characters.
    let safe: String = team_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("app-map/{safe}")
}

/// The built map (gzipped JSON).
pub fn graph_key(team_id: &str) -> String {
    format!("{}/graph.json.gz", s3_prefix(team_id))
}

/// Floorplan's own files from the last run (a gzipped tar of `floorplan/`).
pub fn state_key(team_id: &str) -> String {
    format!("{}/state.tgz", s3_prefix(team_id))
}

/// The run in progress: heads, answers waiting for the next build, attempts.
pub fn job_key(team_id: &str) -> String {
    format!("{}/job.json", s3_prefix(team_id))
}

pub const STATUS_SK: &str = "STATUS";
/// A build queued or running this long is treated as stuck.
pub const STALE_SECS: i64 = 45 * 60;

/// Where the team's map build is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// `queued`, `building`, `ready` or `failed`.
    pub state: String,
    pub at: i64,
    /// What the build is doing now, in plain words.
    #[serde(default)]
    pub step: String,
    #[serde(default)]
    pub error: String,
}

impl Status {
    pub fn is_stale(&self, now: i64) -> bool {
        matches!(self.state.as_str(), "queued" | "building") && now - self.at > STALE_SECS
    }
}

type Dynamo = aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue as A;

pub async fn status(dynamo: &Dynamo, table: &str, team_id: &str) -> Option<Status> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(map_pk(team_id)))
        .key("sk", A::S(STATUS_SK.into()))
        .send()
        .await
        .ok()?;
    serde_json::from_str(out.item()?.get("json")?.as_s().ok()?).ok()
}

pub async fn set_status(dynamo: &Dynamo, table: &str, team_id: &str, st: &Status) {
    let _ = dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(STATUS_SK.into()))
        .item("json", A::S(serde_json::to_string(st).unwrap_or_default()))
        .item("state", A::S(st.state.clone()))
        .item("at", A::N(st.at.to_string()))
        .send()
        .await;
}

/// Marks a build queued: once (a build queued or running is not queued
/// again unless stuck), or always with `force`. `Ok(true)`: send the message.
pub async fn mark_queued(
    dynamo: &Dynamo,
    table: &str,
    team_id: &str,
    force: bool,
) -> Result<bool, String> {
    let now = chrono::Utc::now().timestamp();
    let st = Status {
        state: "queued".into(),
        at: now,
        step: "Waiting to start".into(),
        error: String::new(),
    };
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .item("pk", A::S(map_pk(team_id)))
        .item("sk", A::S(STATUS_SK.into()))
        .item("json", A::S(serde_json::to_string(&st).unwrap_or_default()))
        .item("state", A::S("queued".into()))
        .item("at", A::N(now.to_string()));
    if !force {
        put = put
            .condition_expression(
                "attribute_not_exists(sk) OR NOT (#st IN (:q, :b)) OR #at < :cutoff",
            )
            .expression_attribute_names("#st", "state")
            .expression_attribute_names("#at", "at")
            .expression_attribute_values(":q", A::S("queued".into()))
            .expression_attribute_values(":b", A::S("building".into()))
            .expression_attribute_values(":cutoff", A::N((now - STALE_SECS).to_string()));
    }
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false)
            {
                Ok(false)
            } else {
                Err(format!(
                    "{}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(e)
                ))
            }
        }
    }
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = gz.write_all(bytes);
    gz.finish().unwrap_or_default()
}

/// Gunzips at most `max` bytes; `None` past it or on bad data.
pub fn gunzip(bytes: &[u8], max: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .take(max as u64 + 1)
        .read_to_end(&mut out)
        .ok()?;
    (out.len() <= max).then_some(out)
}

/// The team's map as built, if any. Only ever the team's own key.
pub async fn load_graph(s3: &aws_sdk_s3::Client, bucket: &str, team_id: &str) -> Option<Value> {
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key(graph_key(team_id))
        .send()
        .await
        .ok()?;
    let bytes = obj.body.collect().await.ok()?.into_bytes();
    let json = gunzip(&bytes, MAX_GRAPH_BYTES)?;
    let v: Value = serde_json::from_slice(&json).ok()?;
    (v["schema"] == GRAPH_SCHEMA).then_some(v)
}

// ── Repositories in the map ────────────────────────────────────────────────

/// The folder each repository gets in the map: its name, or `owner-name`
/// when two repositories share a name. Stable for the same set of repos.
pub fn folders(repos: &[String]) -> BTreeMap<String, String> {
    let mut by_name: HashMap<String, usize> = HashMap::new();
    for r in repos {
        let name = r.rsplit('/').next().unwrap_or(r).to_ascii_lowercase();
        *by_name.entry(name).or_default() += 1;
    }
    repos
        .iter()
        .map(|r| {
            let name = r.rsplit('/').next().unwrap_or(r);
            let clash = by_name
                .get(&name.to_ascii_lowercase())
                .copied()
                .unwrap_or(0)
                > 1;
            let folder = if clash {
                r.replace('/', "-")
            } else {
                name.to_string()
            };
            let folder: String = folder
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                        c
                    } else {
                        '-'
                    }
                })
                .collect();
            (r.clone(), folder)
        })
        .collect()
}

/// The map's `coderhelm.repos`: folder to `owner/name`, kept on the map so
/// overlays can place a run's or a finding's paths.
fn repo_of_folder(graph: &Value) -> HashMap<String, String> {
    graph["coderhelm"]["repos"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(f, r)| Some((f.clone(), r.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Records which folder is which repository on a freshly built map.
pub fn stamp_repos(graph: &mut Value, folders: &BTreeMap<String, String>) {
    let m: Map<String, Value> = folders
        .iter()
        .map(|(repo, folder)| (folder.clone(), json!(repo)))
        .collect();
    graph["coderhelm"] = json!({ "repos": m, "floorplan": FLOORPLAN_VERSION.trim() });
}

fn arr<'a>(v: &'a Value, k: &str) -> impl Iterator<Item = &'a Value> {
    v[k].as_array().into_iter().flatten()
}

fn files_set(graph: &Value) -> BTreeSet<String> {
    arr(graph, "files")
        .filter_map(|f| f["p"].as_str().map(str::to_string))
        .collect()
}

/// A repository path on the map (`folder/path`), when the map has that file.
/// `path` may already start with `owner/name/` (a run across several
/// repositories keeps paths that way).
pub fn map_path(graph: &Value, repo: &str, path: &str) -> Option<String> {
    let folders = repo_of_folder(graph);
    let files = files_set(graph);
    let path = path.trim_start_matches("./");
    for (folder, r) in &folders {
        if let Some(rest) = path.strip_prefix(&format!("{r}/")) {
            let p = format!("{folder}/{rest}");
            return files.contains(&p).then_some(p);
        }
    }
    let folder = folders
        .iter()
        .find(|(_, r)| r.eq_ignore_ascii_case(repo))
        .map(|(f, _)| f.clone())?;
    let p = format!("{folder}/{path}");
    files.contains(&p).then_some(p)
}

fn top_node(graph: &Value, id: &str) -> String {
    let parents: HashMap<&str, &str> = arr(graph, "nodes")
        .filter_map(|n| Some((n["id"].as_str()?, n["parent"].as_str()?)))
        .collect();
    let mut cur = id;
    for _ in 0..20 {
        match parents.get(cur) {
            Some(p) => cur = p,
            None => break,
        }
    }
    cur.to_string()
}

/// The top-level part holding most of a repository's files.
fn repo_node(graph: &Value, repo: &str) -> Option<String> {
    let folder = repo_of_folder(graph)
        .into_iter()
        .find(|(_, r)| r.eq_ignore_ascii_case(repo))
        .map(|(f, _)| f)?;
    let mut count: BTreeMap<String, usize> = BTreeMap::new();
    for f in arr(graph, "files") {
        if f["r"].as_str() == Some(folder.as_str()) {
            if let Some(n) = f["n"].as_str() {
                *count.entry(top_node(graph, n)).or_default() += 1;
            }
        }
    }
    count.into_iter().max_by_key(|(_, c)| *c).map(|(n, _)| n)
}

// ── Overlays ───────────────────────────────────────────────────────────────

/// A CoderHelm run, for the map's Recent work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub run_id: String,
    pub repo: String,
    pub ticket: String,
    pub title: String,
    /// Why, from the run's plan.
    pub why: String,
    pub pr_url: String,
    pub at: String,
    pub status: String,
    pub files: Vec<String>,
}

/// A CoderHelm finding, for the map's Health.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthItem {
    /// `security`, `infrastructure`, `logs` or `alert`.
    pub kind: String,
    pub id: String,
    pub title: String,
    /// As the finding says it (P0..P3, critical, important, high, ...).
    pub severity: String,
    pub repo: String,
    /// (path in the repository, line or 0).
    pub files: Vec<(String, u32)>,
    /// The dashboard page that shows it.
    pub link: String,
}

/// Floorplan's three levels from a finding's own word.
pub fn severity_level(s: &str) -> &'static str {
    match s.to_ascii_lowercase().as_str() {
        "p0" | "p1" | "critical" | "high" | "error" => "high",
        "p2" | "important" | "medium" | "moderate" | "warning" => "medium",
        _ => "low",
    }
}

/// The map's `recent` from CoderHelm's runs: one task per run, one item per
/// file it changed that is on the map. A run with no file on the map is left
/// out.
pub fn recent_from_runs(graph: &Value, runs: &[WorkItem]) -> Option<Value> {
    let mut tasks = Vec::new();
    let mut items = Vec::new();
    for w in runs {
        let mut ids: Vec<String> = w
            .files
            .iter()
            .filter_map(|f| map_path(graph, &w.repo, f))
            .map(|p| format!("f:{p}"))
            .collect();
        ids.dedup();
        if ids.is_empty() {
            continue;
        }
        let title = if w.ticket.is_empty() {
            crate::truncate_str(&w.title, 80).to_string()
        } else {
            format!("{}: {}", w.ticket, crate::truncate_str(&w.title, 70))
        };
        let mut summary = w.why.clone();
        if !w.pr_url.is_empty() {
            if !summary.is_empty() {
                summary.push(' ');
            }
            summary.push_str(&format!("Pull request: {}", w.pr_url));
        }
        // Nobody asked the map for this: Floorplan shows `request` as "You
        // asked", so it stays empty, as its workflow says.
        tasks
            .push(json!({ "title": title, "request": "", "summary": summary, "status": w.status }));
        let task = tasks.len() - 1;
        let what = if w.ticket.is_empty() {
            "this work".to_string()
        } else {
            w.ticket.clone()
        };
        for id in ids {
            items.push(json!({ "id": id, "task": task, "change": "changed", "note": format!("Changed by Coderhelm's run for {what}") }));
        }
    }
    (!items.is_empty()).then(|| {
        json!({ "updatedAt": chrono::Utc::now().to_rfc3339(), "tasks": tasks, "items": items, "source": "coderhelm" })
    })
}

/// CoderHelm's findings as Floorplan health issues. A finding in a
/// repository the map doesn't have is left out.
pub fn issues_from_findings(graph: &Value, found: &[HealthItem]) -> Vec<Value> {
    let label = |k: &str| match k {
        "security" => "security",
        "infrastructure" => "infrastructure",
        "logs" => "logs",
        _ => "alert",
    };
    found
        .iter()
        .filter_map(|h| {
            let placed: Vec<(String, u32)> = h
                .files
                .iter()
                .filter_map(|(f, line)| Some((map_path(graph, &h.repo, f)?, *line)))
                .collect();
            let mut items: Vec<String> = placed.iter().map(|(p, _)| format!("f:{p}")).collect();
            items.dedup();
            if items.is_empty() {
                items.push(format!("n:{}", repo_node(graph, &h.repo)?));
            }
            Some(json!({
                "id": format!("coderhelm:{}:{}", h.kind, h.id),
                "kind": format!("coderhelm-{}", h.kind),
                "severity": severity_level(&h.severity),
                "title": format!("Coderhelm {}: {}", label(&h.kind), h.title),
                "detail": format!("Found by Coderhelm. See it in Coderhelm: {}", h.link),
                "items": items,
                "notes": {},
                "locations": placed.iter().filter(|(_, l)| *l > 0).map(|(p, l)| json!({"file": p, "line": l})).collect::<Vec<_>>(),
                "fix": "",
                "source": "coderhelm",
                "link": h.link,
            }))
        })
        .collect()
}

fn locations(issue: &Value) -> BTreeSet<(String, u64)> {
    arr(issue, "locations")
        .filter_map(|l| {
            Some((
                l["file"].as_str()?.to_string(),
                l["line"].as_u64().unwrap_or(0),
            ))
        })
        .collect()
}

/// The map with CoderHelm's overlays in Floorplan's own fields. With
/// `runs`, they replace the map's recent work; with `found`, CoderHelm's
/// findings join Floorplan's checks (each marked with its `source`; a
/// Floorplan issue at the same file and line as a CoderHelm one is dropped).
/// Floorplan's own checks are always labelled as Floorplan's.
pub fn with_overlays(
    mut graph: Value,
    runs: Option<&[WorkItem]>,
    found: Option<&[HealthItem]>,
) -> Value {
    if let Some(runs) = runs {
        match recent_from_runs(&graph, runs) {
            Some(r) => graph["recent"] = r,
            None => {
                if let Some(o) = graph.as_object_mut() {
                    o.remove("recent");
                }
            }
        }
    }
    let mut issues: Vec<Value> = graph["health"]["issues"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for i in issues.iter_mut() {
        if i["source"].is_null() {
            i["source"] = json!("floorplan");
            if let Some(t) = i["title"].as_str() {
                i["title"] = json!(format!("Floorplan check: {t}"));
            }
        }
    }
    if let Some(found) = found {
        let ours = issues_from_findings(&graph, found);
        let taken: BTreeSet<(String, u64)> = ours.iter().flat_map(locations).collect();
        issues.retain(|i| i["source"] != "floorplan" || locations(i).is_disjoint(&taken));
        issues.extend(ours);
    }
    let (mut high, mut medium, mut low) = (0, 0, 0);
    for i in &issues {
        match i["severity"].as_str().unwrap_or("low") {
            "high" => high += 1,
            "medium" => medium += 1,
            _ => low += 1,
        }
    }
    if graph.get("health").is_some() || !issues.is_empty() {
        graph["health"]["issues"] = json!(issues);
        graph["health"]["summary"] = json!({"high": high, "medium": medium, "low": low});
    }
    graph
}

/// The copy the page gets: read-only (Floorplan's exported mode, so the
/// viewer never calls a server), naming no agent, without the sandbox's
/// folder.
pub fn for_viewer(mut graph: Value, now: &str) -> Value {
    graph["meta"]["root"] = Value::Null;
    graph["meta"]["agent"] = json!("none");
    graph["meta"]["exported"] = json!({ "at": now, "by": "Coderhelm" });
    graph
}

// ── Lookups (read API and Ask) ─────────────────────────────────────────────

fn node(graph: &Value, id: &str) -> Option<Value> {
    arr(graph, "nodes").find(|n| n["id"] == id).cloned()
}

/// A short record for a map id (`n:`, `f:`, `r:`).
fn describe_ref(graph: &Value, id: &str) -> Value {
    if let Some(n) = id.strip_prefix("n:") {
        if let Some(x) = node(graph, n) {
            return json!({"id": id, "label": x["label"], "kind": x["kind"], "summary": x["summary"]});
        }
    } else if let Some(p) = id.strip_prefix("f:") {
        if let Some(f) = arr(graph, "files").find(|f| f["p"] == p) {
            return json!({"id": id, "path": p, "name": f["name"], "description": f["d"], "part": f["n"]});
        }
    } else if let Some(i) = id.strip_prefix("r:").and_then(|i| i.parse::<usize>().ok()) {
        if let Some(r) = graph["routes"].get(i) {
            return json!({"id": id, "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "file": r["f"]});
        }
    }
    json!({ "id": id })
}

fn link_json(graph: &Value, l: &Value) -> Value {
    json!({
        "from": describe_ref(graph, l["a"].as_str().unwrap_or("")),
        "to": describe_ref(graph, l["b"].as_str().unwrap_or("")),
        "kind": l["k"],
        "labels": l["labels"],
        "inferred": l["inferred"],
    })
}

/// The whole map in short: parts, screens, Health counts, repositories.
pub fn overview(graph: &Value) -> Value {
    json!({
        "project": graph["project"],
        "parts": arr(graph, "nodes").map(|n| json!({"id": n["id"], "parent": n["parent"], "label": n["label"], "kind": n["kind"], "summary": n["summary"], "files": n["stats"]["files"]})).collect::<Vec<_>>(),
        "screens": arr(graph, "screens").take(80).map(|s| json!({"id": s["id"], "url": s["url"], "name": s["name"], "file": s["file"]})).collect::<Vec<_>>(),
        "health": graph["health"]["summary"],
        "repos": graph["meta"]["repos"],
        "counts": graph["meta"]["counts"],
    })
}

/// One part: its sub-parts, files, endpoints, and the lines in and out.
pub fn part_view(graph: &Value, id: &str) -> Option<Value> {
    let n = node(graph, id)?;
    let children: Vec<Value> = arr(graph, "nodes")
        .filter(|c| c["parent"] == id)
        .map(|c| json!({"id": c["id"], "label": c["label"], "kind": c["kind"], "summary": c["summary"]}))
        .collect();
    let mut inside: BTreeSet<String> = BTreeSet::from([id.to_string()]);
    let mut grew = true;
    while grew {
        grew = false;
        for c in arr(graph, "nodes") {
            if let (Some(cid), Some(p)) = (c["id"].as_str(), c["parent"].as_str()) {
                if inside.contains(p) && inside.insert(cid.to_string()) {
                    grew = true;
                }
            }
        }
    }
    let files: Vec<&Value> = arr(graph, "files")
        .filter(|f| f["n"].as_str().is_some_and(|n| inside.contains(n)))
        .collect();
    let paths: BTreeSet<&str> = files.iter().filter_map(|f| f["p"].as_str()).collect();
    let routes: Vec<Value> = arr(graph, "routes")
        .enumerate()
        .filter(|(_, r)| r["f"].as_str().is_some_and(|f| paths.contains(f)))
        .map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "file": r["f"], "line": r["l"]}))
        .collect();
    let mine = |x: &str| {
        x.strip_prefix("n:").is_some_and(|n| inside.contains(n))
            || x.strip_prefix("f:").is_some_and(|p| paths.contains(p))
    };
    let links: Vec<Value> = arr(graph, "links")
        .filter(|l| l["a"].as_str().is_some_and(mine) != l["b"].as_str().is_some_and(mine))
        .take(200)
        .map(|l| link_json(graph, l))
        .collect();
    Some(json!({
        "part": {"id": n["id"], "label": n["label"], "kind": n["kind"], "summary": n["summary"], "details": n["details"], "stats": n["stats"]},
        "children": children,
        "files": files.iter().take(400).map(|f| json!({"path": f["p"], "name": f["name"], "description": f["d"]})).collect::<Vec<_>>(),
        "endpoints": routes,
        "links": links,
    }))
}

/// One file: what it is, its endpoints, and what it connects to.
pub fn file_view(graph: &Value, path: &str) -> Option<Value> {
    let f = arr(graph, "files").find(|f| f["p"] == path)?;
    let id = format!("f:{path}");
    let links: Vec<Value> = arr(graph, "links")
        .filter(|l| l["a"] == id.as_str() || l["b"] == id.as_str())
        .take(200)
        .map(|l| link_json(graph, l))
        .collect();
    let routes: Vec<Value> = arr(graph, "routes")
        .enumerate()
        .filter(|(_, r)| r["f"] == path)
        .map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "description": r["d"], "line": r["l"]}))
        .collect();
    Some(json!({
        "file": {"path": f["p"], "name": f["name"], "description": f["d"], "part": f["n"], "repo": f["r"], "language": f["l"], "lines": f["loc"]},
        "endpoints": routes,
        "links": links,
    }))
}

/// Everything behind one screen, as Floorplan traced it, each step described.
pub fn screen_view(graph: &Value, id_or_url: &str) -> Option<Value> {
    let s = arr(graph, "screens").find(|s| s["id"] == id_or_url || s["url"] == id_or_url)?;
    let steps: Vec<Value> = arr(s, "behind")
        .map(|b| {
            let mut d = describe_ref(graph, b["id"].as_str().unwrap_or(""));
            d["note"] = b["note"].clone();
            d["step"] = b["kind"].clone();
            d
        })
        .collect();
    Some(json!({
        "screen": {"id": s["id"], "url": s["url"], "name": s["name"], "description": s["description"], "file": s["file"]},
        "behind": steps,
    }))
}

/// Parts, files, endpoints and screens whose names, descriptions or paths
/// hold every word of the query.
pub fn search(graph: &Value, q: &str) -> Value {
    let words: Vec<String> = q
        .split_whitespace()
        .map(|w| w.to_ascii_lowercase())
        .take(8)
        .collect();
    if words.is_empty() {
        return json!({"parts": [], "files": [], "endpoints": [], "screens": []});
    }
    let hit = |texts: &[&Value]| {
        let hay: String = texts
            .iter()
            .filter_map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        words.iter().all(|w| hay.contains(w))
    };
    json!({
        "parts": arr(graph, "nodes").filter(|n| hit(&[&n["label"], &n["summary"], &n["id"]])).take(20).map(|n| json!({"id": n["id"], "label": n["label"], "kind": n["kind"], "summary": n["summary"]})).collect::<Vec<_>>(),
        "files": arr(graph, "files").filter(|f| hit(&[&f["p"], &f["name"], &f["d"]])).take(30).map(|f| json!({"path": f["p"], "name": f["name"], "description": f["d"], "part": f["n"]})).collect::<Vec<_>>(),
        "endpoints": arr(graph, "routes").enumerate().filter(|(_, r)| hit(&[&r["m"], &r["p"], &r["name"], &r["d"]])).take(30).map(|(i, r)| json!({"id": format!("r:{i}"), "method": r["m"], "path": r["p"], "name": r["name"], "file": r["f"]})).collect::<Vec<_>>(),
        "screens": arr(graph, "screens").filter(|s| hit(&[&s["url"], &s["name"], &s["description"]])).take(20).map(|s| json!({"id": s["id"], "url": s["url"], "name": s["name"]})).collect::<Vec<_>>(),
    })
}

/// The top-level parts a set of map ids belongs to (for a mini map).
pub fn parts_of(graph: &Value, ids: &[String]) -> BTreeSet<String> {
    let part_of_file = |p: &str| {
        arr(graph, "files")
            .find(|f| f["p"] == p)
            .and_then(|f| f["n"].as_str())
            .map(|n| top_node(graph, n))
    };
    let mut out = BTreeSet::new();
    for id in ids {
        let part = if let Some(n) = id.strip_prefix("n:") {
            node(graph, n).map(|_| top_node(graph, n))
        } else if let Some(p) = id.strip_prefix("f:") {
            part_of_file(p)
        } else if let Some(i) = id.strip_prefix("r:").and_then(|i| i.parse::<usize>().ok()) {
            graph["routes"]
                .get(i)
                .and_then(|r| r["f"].as_str())
                .and_then(part_of_file)
        } else {
            node(graph, id).map(|_| top_node(graph, id))
        };
        out.extend(part);
    }
    out
}

/// A smaller map holding only `parts` (and their sub-parts), their files,
/// endpoints and the links among them: Ask's mini map, drawn by the same
/// viewer.
pub fn sub_map(graph: &Value, parts: &BTreeSet<String>) -> Value {
    let keep: BTreeSet<String> = arr(graph, "nodes")
        .filter_map(|n| n["id"].as_str())
        .filter(|id| parts.contains(&top_node(graph, id)))
        .map(str::to_string)
        .collect();
    let files: Vec<Value> = arr(graph, "files")
        .filter(|f| f["n"].as_str().is_some_and(|n| keep.contains(n)))
        .cloned()
        .collect();
    let paths: BTreeSet<String> = files
        .iter()
        .filter_map(|f| f["p"].as_str().map(str::to_string))
        .collect();
    let mut renumber: HashMap<usize, usize> = HashMap::new();
    let mut routes = Vec::new();
    for (i, r) in arr(graph, "routes").enumerate() {
        if r["f"].as_str().is_some_and(|f| paths.contains(f)) {
            renumber.insert(i, routes.len());
            routes.push(r.clone());
        }
    }
    let remap = |id: &str| -> Option<String> {
        if let Some(n) = id.strip_prefix("n:") {
            return keep.contains(n).then(|| id.to_string());
        }
        if let Some(p) = id.strip_prefix("f:") {
            return paths.contains(p).then(|| id.to_string());
        }
        let i = id.strip_prefix("r:")?.parse::<usize>().ok()?;
        renumber.get(&i).map(|n| format!("r:{n}"))
    };
    let links: Vec<Value> = arr(graph, "links")
        .filter_map(|l| {
            let a = remap(l["a"].as_str()?)?;
            let b = remap(l["b"].as_str()?)?;
            let mut l = l.clone();
            l["a"] = json!(a);
            l["b"] = json!(b);
            Some(l)
        })
        .collect();
    let mut out = graph.clone();
    out["nodes"] = json!(arr(graph, "nodes")
        .filter(|n| n["id"].as_str().is_some_and(|i| keep.contains(i)))
        .cloned()
        .collect::<Vec<_>>());
    out["files"] = json!(files);
    out["routes"] = json!(routes);
    out["links"] = json!(links);
    out["flows"] = json!([]);
    out["tour"] = json!([]);
    out["screens"] = json!([]);
    if let Some(o) = out.as_object_mut() {
        o.remove("recent");
        o.remove("health");
    }
    out
}

// ── Model answers ──────────────────────────────────────────────────────────

/// Problems with `v` against `schema`, for the JSON Schema subset Floorplan's
/// workflow uses (type, properties, required, items, enum, pattern,
/// minItems, minLength, additionalProperties). Empty when it fits.
pub fn check_answer(schema: &Value, v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    check_at(schema, v, "answer", &mut out);
    out.truncate(30);
    out
}

fn check_at(schema: &Value, v: &Value, at: &str, out: &mut Vec<String>) {
    if let Some(t) = schema["type"].as_str() {
        let ok = match t {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
            "number" => v.is_number(),
            "integer" => v.is_i64() || v.is_u64(),
            "boolean" => v.is_boolean(),
            "null" => v.is_null(),
            _ => true,
        };
        if !ok {
            out.push(format!("{at} should be a {t}"));
            return;
        }
    }
    if let Some(e) = schema["enum"].as_array() {
        if !e.contains(v) {
            out.push(format!("{at} should be one of {}", Value::Array(e.clone())));
        }
    }
    if let (Some(p), Some(s)) = (schema["pattern"].as_str(), v.as_str()) {
        if let Ok(re) = regex::Regex::new(p) {
            if !re.is_match(s) {
                out.push(format!("{at} doesn't match {p}"));
            }
        }
    }
    if let (Some(n), Some(s)) = (schema["minLength"].as_u64(), v.as_str()) {
        if (s.chars().count() as u64) < n {
            out.push(format!("{at} is too short"));
        }
    }
    if let Some(a) = v.as_array() {
        if let Some(n) = schema["minItems"].as_u64() {
            if (a.len() as u64) < n {
                out.push(format!("{at} needs at least {n} items"));
            }
        }
        if schema["items"].is_object() {
            for (i, x) in a.iter().enumerate() {
                check_at(&schema["items"], x, &format!("{at}[{i}]"), out);
            }
        }
    }
    if let Some(o) = v.as_object() {
        let props = schema["properties"].as_object();
        for r in arr(schema, "required").filter_map(|r| r.as_str()) {
            if !o.contains_key(r) {
                out.push(format!("{at}.{r} is missing"));
            }
        }
        for (k, x) in o {
            match props.and_then(|p| p.get(k)) {
                Some(s) => check_at(s, x, &format!("{at}.{k}"), out),
                None => match &schema["additionalProperties"] {
                    Value::Bool(false) => out.push(format!("{at}.{k} is not allowed")),
                    s if s.is_object() => check_at(s, x, &format!("{at}.{k}"), out),
                    _ => {}
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The map Floorplan builds for the made-up harbor app (a web app and an
    /// API in two repositories), written by
    /// services/worker/floorplan/make-fixture.mjs and checked against the
    /// vendored release by driver.test.mjs.
    fn harbor() -> Value {
        let mut g: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/fixtures/harbor-graph.json"
        ))
        .unwrap();
        stamp_repos(
            &mut g,
            &folders(&["harbor/harbor-web".into(), "harbor/harbor-api".into()]),
        );
        g
    }

    #[test]
    fn repositories_get_stable_folders() {
        let f = folders(&[
            "harbor/web".into(),
            "marina/web".into(),
            "harbor/api".into(),
        ]);
        assert_eq!(f["harbor/web"], "harbor-web");
        assert_eq!(f["marina/web"], "marina-web");
        assert_eq!(f["harbor/api"], "api");
        let g = harbor();
        assert_eq!(
            map_path(&g, "harbor/harbor-api", "src/billing.js").as_deref(),
            Some("harbor-api/src/billing.js")
        );
        assert_eq!(
            map_path(&g, "harbor/harbor-web", "harbor/harbor-api/src/bookings.js").as_deref(),
            Some("harbor-api/src/bookings.js")
        );
        assert_eq!(map_path(&g, "harbor/harbor-api", "src/missing.js"), None);
        assert_eq!(map_path(&g, "other/repo", "src/billing.js"), None);
    }

    #[test]
    fn runs_become_the_maps_own_recent_work() {
        let runs = vec![
            WorkItem {
                run_id: "r1".into(),
                repo: "harbor/harbor-api".into(),
                ticket: "DOCK-42".into(),
                title: "Stop double bookings".into(),
                why: "Two boaters could book one slip.".into(),
                pr_url: "https://github.example/harbor/harbor-api/pull/7".into(),
                files: vec!["src/bookings.js".into(), "README.md".into()],
                ..Default::default()
            },
            WorkItem {
                run_id: "r2".into(),
                repo: "elsewhere/x".into(),
                files: vec!["a.js".into()],
                ..Default::default()
            },
        ];
        let g = with_overlays(harbor(), Some(&runs), None);
        let r = &g["recent"];
        assert_eq!(r["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(r["tasks"][0]["title"], "DOCK-42: Stop double bookings");
        assert_eq!(r["tasks"][0]["request"], "");
        assert!(r["tasks"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("pull/7"));
        assert_eq!(
            r["items"],
            json!([{"id": "f:harbor-api/src/bookings.js", "task": 0, "change": "changed", "note": "Changed by Coderhelm's run for DOCK-42"}])
        );
        // No run on the map: no recent work at all.
        assert!(with_overlays(harbor(), Some(&runs[1..]), None)
            .get("recent")
            .is_none());
    }

    #[test]
    fn findings_join_floorplans_checks_labelled_and_never_twice() {
        let found = vec![
            HealthItem {
                kind: "security".into(),
                id: "f1".into(),
                title: "Prototype pollution in a parser".into(),
                severity: "P1".into(),
                repo: "harbor/harbor-api".into(),
                files: vec![("package.json".into(), 0), ("src/bookings.js".into(), 5)],
                link: "/security/detail?id=f1".into(),
            },
            HealthItem {
                kind: "logs".into(),
                id: "l1".into(),
                title: "Bookings time out".into(),
                severity: "warning".into(),
                repo: "harbor/harbor-web".into(),
                files: vec![],
                link: "/logs?finding=l1".into(),
            },
        ];
        let base = with_overlays(harbor(), None, None);
        let theirs = base["health"]["issues"].as_array().unwrap();
        assert!(!theirs.is_empty());
        assert!(theirs.iter().all(|i| i["source"] == "floorplan"
            && i["title"]
                .as_str()
                .unwrap()
                .starts_with("Floorplan check: ")));
        let g = with_overlays(harbor(), None, Some(&found));
        let issues = g["health"]["issues"].as_array().unwrap();
        let ours: Vec<&Value> = issues
            .iter()
            .filter(|i| i["source"] == "coderhelm")
            .collect();
        assert_eq!(ours.len(), 2);
        assert_eq!(ours[0]["severity"], "high");
        assert_eq!(ours[0]["items"], json!(["f:harbor-api/src/bookings.js"]));
        assert_eq!(ours[0]["link"], "/security/detail?id=f1");
        // A finding with no file sits on the repository's part.
        assert_eq!(ours[1]["items"], json!(["n:web"]));
        // Floorplan's check at the same file and line is not shown twice.
        assert!(!issues
            .iter()
            .any(|i| i["source"] == "floorplan" && i["id"] == "no-auth:harbor-api"));
        assert_eq!(g["health"]["summary"]["high"], 1);
    }

    #[test]
    fn the_page_gets_a_read_only_copy_without_the_sandbox_folder() {
        let g = for_viewer(harbor(), "2026-11-21T00:00:00Z");
        assert!(g["meta"]["root"].is_null());
        assert_eq!(g["meta"]["agent"], "none");
        assert_eq!(g["meta"]["exported"]["by"], "Coderhelm");
    }

    #[test]
    fn lookups_follow_the_map() {
        let g = harbor();
        let o = overview(&g);
        assert_eq!(o["parts"].as_array().unwrap().len(), 3);
        let p = part_view(&g, "api").unwrap();
        assert_eq!(p["endpoints"][0]["path"], "/slips/:id/bookings");
        assert!(p["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["from"]["path"] == "harbor-web/lib/slips.ts" && l["kind"] == "http"));
        assert!(part_view(&g, "nope").is_none());
        let f = file_view(&g, "harbor-api/src/billing.js").unwrap();
        assert!(f["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["to"]["label"] == "Stripe"));
        let s = screen_view(&g, "/slips/:id").unwrap();
        let steps: Vec<&str> = s["behind"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|b| b["step"].as_str())
            .collect();
        assert_eq!(steps.first(), Some(&"screen"));
        assert_eq!(steps.last(), Some(&"outside"));
        assert_eq!(search(&g, "book slip")["endpoints"][0]["method"], "POST");
        let parts = parts_of(&g, &["f:harbor-web/lib/slips.ts".into(), "r:0".into()]);
        assert_eq!(
            parts,
            BTreeSet::from(["api".to_string(), "web".to_string()])
        );
        let mini = sub_map(&g, &BTreeSet::from(["api".to_string()]));
        assert_eq!(mini["files"].as_array().unwrap().len(), 3);
        assert!(mini["links"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["a"].as_str().unwrap().starts_with("f:harbor-api")));
    }

    #[test]
    fn answers_are_checked_against_the_workflows_schemas() {
        let wf: Value = serde_json::from_str(WORKFLOW_JSON).unwrap();
        let arch: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/architecture.harbor.json"
        ))
        .unwrap();
        assert_eq!(
            check_answer(&wf["schemas"]["architecture"], &arch),
            Vec::<String>::new()
        );
        let bad = json!({"project": {}, "nodes": [{"id": 3}], "extra": true});
        let problems = check_answer(&wf["schemas"]["architecture"], &bad);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("project.name is missing")),
            "{problems:?}"
        );
        assert!(problems.iter().any(|p| p.contains("extra is not allowed")));
        let desc: Value = serde_json::from_str(include_str!(
            "../../worker/floorplan/descriptions.harbor.json"
        ))
        .unwrap();
        for d in desc.as_object().unwrap().values() {
            assert!(check_answer(&wf["schemas"]["descriptions"], d).is_empty());
        }
        // CoderHelm's recent work fits Floorplan's recent-work answer.
        let g = with_overlays(
            harbor(),
            Some(&[WorkItem {
                repo: "harbor/harbor-api".into(),
                title: "Tidy the server".into(),
                files: vec!["src/server.js".into()],
                ..Default::default()
            }]),
            None,
        );
        let as_answer = json!({"tasks": g["recent"]["tasks"].as_array().unwrap().iter().map(|t| json!({
            "title": t["title"], "request": t["request"], "summary": t["summary"],
            "items": [{"ref": "harbor-api/src/server.js", "change": "changed", "note": "Tidied."}]
        })).collect::<Vec<_>>()});
        assert_eq!(
            check_answer(&wf["schemas"]["recent_work"], &as_answer),
            Vec::<String>::new()
        );
        assert!(FLOORPLAN_VERSION.starts_with("floorplan--v"));
    }
}
