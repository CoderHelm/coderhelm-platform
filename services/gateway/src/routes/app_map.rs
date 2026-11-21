//! Dashboard API for the APP MAP (`common::app_map`): how the team's app
//! fits together across its repositories, read from what the worker keeps
//! in the team's own partition (`AM#{team}`). Every read is the signed-in
//! team's: a part, file or screen of another team is simply not found.
//!
//! - `GET /api/app-map`: the whole map and its build status (the first
//!   visit queues the first build);
//! - `GET /api/app-map/part?id=`, `/file?repo=&path=`, `/screen?id=`,
//!   `/search?q=`: one part, one file, everything behind one screen, search;
//! - `GET /api/app-map/overlays?days=`: CoderHelm's recent runs and its
//!   security, infrastructure, log and alert findings placed on the map;
//! - `POST /api/app-map/ask`: a question answered by Chat's agent with map
//!   lookups, returned with the parts and lines involved and numbered steps;
//! - `POST /api/app-map/rebuild`: an admin builds it again.

use std::collections::BTreeSet;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::app_map::{self as am, HealthItem, RepoMap, TeamMap, WorkItem};
use common::knowledge::answer::{Registry, SourceRef};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info};

use super::chat_agent;
use crate::models::Claims;
use crate::AppState;

async fn team_maps(state: &AppState, team_id: &str) -> Result<Vec<RepoMap>, StatusCode> {
    am::load_all(&state.dynamo, &state.config.settings_table_name, team_id)
        .await
        .map_err(|e| {
            error!("app map: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// Queues a whole-team build: once (`force` false) or again (an admin).
async fn queue_build(state: &AppState, team_id: &str, force: bool) -> bool {
    let table = &state.config.settings_table_name;
    match am::mark_queued(&state.dynamo, table, team_id, force).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(e) => {
            error!("app map: could not mark a build queued: {e}");
            return false;
        }
    }
    let msg = crate::models::WorkerMessage::AppMap(crate::models::AppMapMessage {
        team_id: team_id.to_string(),
        repo: String::new(),
    });
    let Ok(body) = serde_json::to_string(&msg) else {
        return false;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => {
            info!(team_id, "App map: build queued");
            true
        }
        Err(e) => {
            error!("app map: could not queue a build: {e}");
            am::set_status(
                &state.dynamo,
                table,
                team_id,
                &am::Status {
                    state: "failed".into(),
                    at: chrono::Utc::now().timestamp(),
                    error: "The build could not be started. Try again in a moment.".into(),
                    ..Default::default()
                },
            )
            .await;
            false
        }
    }
}

/// What the page shows about the build: `building` while there is nothing
/// to show yet, else `ready` (with `updating` while a refresh runs).
pub fn shown_state(st: Option<&am::Status>, has_map: bool, now: i64) -> (&'static str, bool) {
    let running =
        st.is_some_and(|s| matches!(s.state.as_str(), "queued" | "building") && !s.is_stale(now));
    match (has_map, st) {
        (true, _) => ("ready", running),
        (false, Some(s)) if s.state == "failed" => ("failed", false),
        (false, Some(_)) if running => ("building", true),
        (false, Some(s)) if s.state == "ready" => ("empty", false),
        _ => ("not_built", false),
    }
}

/// GET /api/app-map — the whole map, or where its first build is.
pub async fn get_map(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let table = &state.config.settings_table_name;
    let (maps, st) = tokio::join!(
        team_maps(&state, &claims.team_id),
        am::status(&state.dynamo, table, &claims.team_id),
    );
    let maps = maps?;
    let now = chrono::Utc::now().timestamp();
    let (mut shown, mut updating) = shown_state(st.as_ref(), !maps.is_empty(), now);
    if shown == "not_built" && queue_build(&state, &claims.team_id, false).await {
        shown = "building";
        updating = true;
    }
    let status = json!({
        "state": shown,
        "updating": updating,
        "done": st.as_ref().map(|s| s.done).unwrap_or(0),
        "total": st.as_ref().map(|s| s.total).unwrap_or(0),
        "note": st.as_ref().map(|s| s.error.clone()).unwrap_or_default(),
    });
    if maps.is_empty() {
        return Ok(Json(json!({ "status": status, "map": Value::Null })));
    }
    let map = am::assemble(&maps);
    Ok(Json(json!({ "status": status, "map": am::overview(&map) })))
}

#[derive(Deserialize, Default)]
pub struct MapQuery {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub days: Option<u32>,
}

fn bounded(s: &str, max: usize) -> Result<String, StatusCode> {
    let s = s.trim();
    if s.is_empty() || s.len() > max || s.contains('\0') {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(s.to_string())
}

/// One view of the signed-in team's map: `None` when what was asked for
/// isn't in it (another team's part, file or screen included).
pub fn view(maps: &[RepoMap], kind: &str, q: &MapQuery) -> Option<Value> {
    let map = am::assemble(maps);
    match kind {
        "part" => am::part_view(&map, maps, &q.id),
        "file" => am::file_view(&map, maps, &q.repo, &q.path),
        "screen" => am::screen_chain(&map, maps, &q.id),
        "search" => Some(am::search(&map, maps, &q.q)),
        _ => None,
    }
}

async fn one(
    state: &AppState,
    claims: &Claims,
    kind: &str,
    q: &MapQuery,
) -> Result<Json<Value>, StatusCode> {
    let maps = team_maps(state, &claims.team_id).await?;
    view(&maps, kind, q).map(Json).ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/app-map/part?id= — one part with its files, endpoints, screens,
/// requests (and how each was matched) and lines.
pub async fn part(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.id, 400)?;
    one(&state, &claims, "part", &q).await
}

/// GET /api/app-map/file?repo=&path= — one file.
pub async fn file(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.repo, 200)?;
    bounded(&q.path, 400)?;
    one(&state, &claims, "file", &q).await
}

/// GET /api/app-map/screen?id= — everything behind one screen.
pub async fn screen(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.id, 800)?;
    one(&state, &claims, "screen", &q).await
}

/// GET /api/app-map/search?q=
pub async fn search(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.q, 100)?;
    one(&state, &claims, "search", &q).await
}

/// POST /api/app-map/rebuild — admin. Builds the whole map again (parts
/// that didn't change keep their descriptions).
pub async fn rebuild(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?;
    if queue_build(&state, &claims.team_id, true).await {
        Ok(StatusCode::ACCEPTED)
    } else {
        Err(StatusCode::BAD_GATEWAY)
    }
}

// ── Overlays ───────────────────────────────────────────────────────────────

/// The `Why` of a plan's proposal: its first paragraph under a "Why"
/// heading, or its first paragraph.
pub fn why_of(proposal: &str) -> String {
    let mut lines = proposal.lines().map(str::trim);
    let mut under_why = false;
    let mut out = Vec::new();
    for l in lines.by_ref() {
        if l.starts_with('#') {
            if under_why && !out.is_empty() {
                break;
            }
            under_why = l.to_ascii_lowercase().contains("why");
            continue;
        }
        if under_why {
            if l.is_empty() && !out.is_empty() {
                break;
            }
            if !l.is_empty() {
                out.push(l);
            }
        }
    }
    if out.is_empty() {
        out = proposal
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .take(2)
            .collect();
    }
    common::truncate_str(&out.join(" "), 280).to_string()
}

async fn plan_why(state: &AppState, team_id: &str, ticket: &str, repo: &str) -> String {
    if ticket.is_empty() {
        return String::new();
    }
    let t = ticket.to_lowercase();
    let mut keys = Vec::new();
    if let Some((o, n)) = repo.split_once('/') {
        keys.push(format!(
            "teams/{team_id}/runs/{t}/openspec/{o}__{n}/proposal.md"
        ));
    }
    keys.push(format!("teams/{team_id}/runs/{t}/openspec/proposal.md"));
    for key in keys {
        if let Ok(out) = state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            if let Ok(b) = out.body.collect().await {
                return why_of(&String::from_utf8_lossy(&b.to_vec()));
            }
        }
    }
    String::new()
}

/// Runs of the last `days` days, newest first, at most 60.
async fn recent_runs(state: &AppState, team_id: &str, days: u32) -> Vec<WorkItem> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    let mut out = Vec::new();
    let mut start = None;
    for _ in 0..5 {
        let Ok(page) = state
            .dynamo
            .query()
            .table_name(&state.config.runs_table_name)
            .key_condition_expression("team_id = :t")
            .expression_attribute_values(":t", AttributeValue::S(team_id.to_string()))
            .projection_expression(
                "run_id, #s, ticket_id, title, repo, pr_url, created_at, files_modified",
            )
            .expression_attribute_names("#s", "status")
            .scan_index_forward(false)
            .limit(100)
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        let mut older = false;
        for it in page.items() {
            let s = |k: &str| {
                it.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            let at = s("created_at");
            if !at.is_empty() && at < cutoff {
                older = true;
                break;
            }
            let files: Vec<String> = it
                .get("files_modified")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default();
            if files.is_empty() {
                continue;
            }
            out.push(WorkItem {
                run_id: s("run_id"),
                repo: s("repo"),
                ticket: s("ticket_id"),
                title: s("title"),
                why: String::new(),
                pr_url: s("pr_url"),
                at,
                status: s("status"),
                files,
            });
            if out.len() >= 60 {
                return out;
            }
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() && !older => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> &'a str {
    let mut cur = v;
    for p in path {
        cur = &cur[*p];
    }
    cur.as_str().unwrap_or("")
}

/// A dependency finding, as the overlay places it: its manifest and the
/// code lines its evidence names.
pub fn security_item(f: &Value) -> HealthItem {
    let mut files: Vec<String> = Vec::new();
    let manifest = str_at(f, &["manifest"]);
    if !manifest.is_empty() {
        files.push(manifest.to_string());
    }
    for l in f["evidence"]["lines"].as_array().into_iter().flatten() {
        let file = l["file"].as_str().unwrap_or("");
        if !file.is_empty() && !files.iter().any(|x| x == file) {
            files.push(file.to_string());
        }
    }
    let title = [str_at(f, &["advisory", "title"]), str_at(f, &["package"])]
        .into_iter()
        .find(|t| !t.is_empty())
        .unwrap_or("A dependency advisory");
    let id = str_at(f, &["id"]);
    HealthItem {
        kind: "security".into(),
        id: id.to_string(),
        title: title.to_string(),
        severity: f["tier"].as_str().map(str::to_string).unwrap_or_default(),
        repo: str_at(f, &["repo"]).to_string(),
        files,
        link: format!("/security/detail?id={}", urlencoding::encode(id)),
    }
}

/// An infrastructure finding with the place its fix goes.
pub fn infra_item(r: &Value) -> Option<HealthItem> {
    let loc = &r["fix"]["location"];
    let repo = loc["repo"].as_str().unwrap_or("");
    if repo.is_empty() {
        return None;
    }
    let file = loc["file"].as_str().unwrap_or("");
    let id = str_at(r, &["id"]);
    Some(HealthItem {
        kind: "infrastructure".into(),
        id: id.to_string(),
        title: str_at(r, &["title"]).to_string(),
        severity: str_at(r, &["severity"]).to_string(),
        repo: repo.to_string(),
        files: if file.is_empty() {
            vec![]
        } else {
            vec![file.to_string()]
        },
        link: format!(
            "/security/infrastructure?finding={}",
            urlencoding::encode(id)
        ),
    })
}

const OPEN_SECURITY: &[&str] = &["open", "fixing", "pr_open", "no_fix"];

async fn health(state: &AppState, team_id: &str, days: u32) -> Vec<HealthItem> {
    let mut out = Vec::new();
    if let Ok(found) = super::security::load_findings(state, team_id).await {
        for (f, _) in found {
            let v = serde_json::to_value(&f).unwrap_or_default();
            if OPEN_SECURITY.contains(&str_at(&v, &["status"])) {
                out.push(security_item(&v));
            }
        }
    }
    if let Ok(records) = super::infra_security::load_records(state, team_id).await {
        for r in records {
            let v = serde_json::to_value(&r).unwrap_or_default();
            if matches!(str_at(&v, &["status"]), "fixed" | "dismissed" | "resolved") {
                continue;
            }
            out.extend(infra_item(&v));
        }
    }
    if let Ok(found) = common::log_findings::list_findings(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        200,
    )
    .await
    {
        for f in found {
            if f.fix_repo.is_empty() || matches!(f.status.as_str(), "dismissed" | "resolved") {
                continue;
            }
            out.push(HealthItem {
                kind: "logs".into(),
                id: f.rec_id.clone(),
                title: f.title.clone(),
                severity: format!("{:?}", f.severity).to_lowercase(),
                repo: f.fix_repo.clone(),
                files: Vec::new(),
                link: format!("/logs?finding={}", urlencoding::encode(&f.rec_id)),
            });
        }
    }
    // Alerts of the last `days` days that named a repository.
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    if let Ok(page) = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p AND begins_with(sk, :s)")
        .expression_attribute_values(":p", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":s", AttributeValue::S("ALERT_EVENT#".into()))
        .scan_index_forward(false)
        .limit(200)
        .send()
        .await
    {
        for it in page.items() {
            let s = |k: &str| {
                it.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            if s("repo").is_empty() || s("received_at") < cutoff {
                continue;
            }
            out.push(HealthItem {
                kind: "alert".into(),
                id: s("id"),
                title: s("title"),
                severity: s("kind"),
                repo: s("repo"),
                files: Vec::new(),
                link: format!("/alerts/detail?id={}", urlencoding::encode(&s("id"))),
            });
        }
    }
    out
}

/// GET /api/app-map/overlays?days=14 — Recent work and Health.
pub async fn overlays(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    let days = q.days.unwrap_or(14).clamp(1, 90);
    let (maps, runs, found) = tokio::join!(
        team_maps(&state, &claims.team_id),
        recent_runs(&state, &claims.team_id, days),
        health(&state, &claims.team_id, days),
    );
    let maps = maps?;
    let mut work = am::place_work(&maps, runs);
    // The plan's why, for the runs shown (bounded reads).
    for w in work.iter_mut().take(25) {
        w.item.why = plan_why(&state, &claims.team_id, &w.item.ticket, &w.item.repo).await;
    }
    let health = am::place_health(&maps, found);
    Ok(Json(
        json!({ "days": days, "work": work, "health": health }),
    ))
}

// ── Ask the map ────────────────────────────────────────────────────────────

/// The lookups the Ask agent has.
pub fn tool_defs() -> Vec<Value> {
    let obj = |props: Value, req: &[&str]| json!({"type": "object", "properties": props, "required": req});
    vec![
        json!({"name": "map_overview", "description": "Every part of the team's app (id, plain name, kind, what it does) and the lines between them, with how sure each line is.", "input_schema": obj(json!({}), &[])}),
        json!({"name": "map_find", "description": "Search the map: parts, screens, endpoints and files whose name or path contains the words.", "input_schema": obj(json!({"query": {"type": "string"}}), &["query"])}),
        json!({"name": "map_part", "description": "One part by id: its files, endpoints, screens, the requests it makes and how each was matched, and its lines in and out.", "input_schema": obj(json!({"id": {"type": "string"}}), &["id"])}),
        json!({"name": "map_screen", "description": "Everything behind one screen (by its id or its route, like /slips/:id): the files it reaches, the requests they make, the endpoints that answer them and the outside services behind them.", "input_schema": obj(json!({"screen": {"type": "string"}}), &["screen"])}),
        json!({"name": "map_file", "description": "One file by repository and path: its part, endpoints, screens, packages, imports and who calls it.", "input_schema": obj(json!({"repo": {"type": "string"}, "path": {"type": "string"}}), &["repo", "path"])}),
    ]
}

/// The Ask agent's toolbox over one team's map. Records the parts and lines
/// its lookups touched, for the mini map.
pub struct MapBox {
    pub maps: Vec<RepoMap>,
    pub map: TeamMap,
    pub defs: Vec<Value>,
    pub parts: BTreeSet<String>,
    pub outside: BTreeSet<String>,
}

impl MapBox {
    pub fn new(maps: Vec<RepoMap>) -> Self {
        let map = am::assemble(&maps);
        Self {
            maps,
            map,
            defs: tool_defs(),
            parts: BTreeSet::new(),
            outside: BTreeSet::new(),
        }
    }

    fn note_parts(&mut self, v: &Value) {
        let mut stack = vec![v];
        while let Some(x) = stack.pop() {
            match x {
                Value::Object(o) => {
                    for (k, val) in o {
                        if let Some(s) = val.as_str() {
                            if (k == "part" || k == "id" || k == "from" || k == "to")
                                && self.map.parts.iter().any(|p| p.id == s)
                            {
                                self.parts.insert(s.to_string());
                            }
                            if let Some(o) = s.strip_prefix("outside:") {
                                self.outside.insert(o.to_string());
                            }
                        }
                        stack.push(val);
                    }
                }
                Value::Array(a) => stack.extend(a.iter()),
                _ => {}
            }
        }
    }

    /// Runs one lookup: (text for the model, label).
    pub fn lookup(&mut self, name: &str, input: &Value) -> (Value, String) {
        let s = |k: &str| input[k].as_str().unwrap_or("").trim().to_string();
        match name {
            "map_overview" => {
                let mut v = am::overview(&self.map);
                v["screens"] = json!(self.map.screens.iter().take(60).collect::<Vec<_>>());
                (v, "Read the map".into())
            }
            "map_find" => (
                am::search(&self.map, &self.maps, &s("query")),
                format!("Searched the map for {}", s("query")),
            ),
            "map_part" => {
                let v = am::part_view(&self.map, &self.maps, &s("id")).unwrap_or_else(|| json!({"error": "No part has that id; use map_overview or map_find for ids."}));
                if v.get("part").is_some() {
                    self.parts.insert(s("id"));
                }
                (v, "Opened a part".into())
            }
            "map_screen" => {
                let want = s("screen");
                let id = self
                    .map
                    .screens
                    .iter()
                    .find(|x| x.id == want || x.route == want)
                    .map(|x| x.id.clone())
                    .unwrap_or_default();
                let v = am::screen_chain(&self.map, &self.maps, &id).unwrap_or_else(
                    || json!({"error": "No screen has that id or route; use map_find."}),
                );
                (v, format!("Followed the screen {want}"))
            }
            "map_file" => {
                let v = am::file_view(&self.map, &self.maps, &s("repo"), &s("path"))
                    .unwrap_or_else(|| json!({"error": "That file isn't on the map."}));
                (v, format!("Opened {}", s("path")))
            }
            _ => (json!({"error": "Unknown lookup."}), name.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl chat_agent::Toolbox for MapBox {
    fn defs(&self) -> &[Value] {
        &self.defs
    }

    async fn call(
        &mut self,
        reg: &mut Registry,
        _sink: &dyn chat_agent::Sink,
        name: &str,
        input: &Value,
    ) -> chat_agent::Called {
        let (v, label) = self.lookup(name, input);
        let is_error = v.get("error").is_some();
        if !is_error && name != "map_overview" && name != "map_find" {
            self.note_parts(&v);
        }
        let text = common::truncate_str(&v.to_string(), 30_000).to_string();
        let id = reg.add(SourceRef {
            kind: "app_map".into(),
            title: label.clone(),
            loc: input.to_string(),
            passage: common::truncate_str(&text, 1_200).to_string(),
            href: Some("/app-map".into()),
            ..Default::default()
        });
        chat_agent::Called::Done {
            text: format!("[{id}] {text}"),
            label,
            summary: String::new(),
            is_error,
            kind: "app_map".into(),
        }
    }
}

struct Quiet;

#[async_trait::async_trait]
impl chat_agent::Sink for Quiet {
    async fn send(&self, _kind: &str, _data: Value) {}
}

pub const ASK_SYSTEM: &str = "You answer questions about how a team's app fits together, from its app map. \
Use the map lookups (start with map_overview or map_find, then open parts, screens and files) and answer only \
from what they return. Write for someone who may not read code: plain words first, developer names in \
parentheses. When the question is about how something happens, answer with numbered steps, one line each, \
in the order it happens. Say plainly when a line on the map is a guess or a request isn't matched, and why. \
Cite each lookup you rely on with its id in brackets, like [S1]. No arrows.";

/// The numbered steps of an answer (`1. ...`).
pub fn steps_of(answer: &str) -> Vec<String> {
    answer
        .lines()
        .filter_map(|l| {
            let t = l.trim();
            let (n, rest) = t.split_once(['.', ')'])?;
            (!n.is_empty() && n.len() <= 2 && n.chars().all(|c| c.is_ascii_digit()))
                .then(|| rest.trim().to_string())
                .filter(|r| !r.is_empty())
        })
        .collect()
}

/// The mini map: the parts the answer's lookups touched (and the parts its
/// steps name), the lines among them, and their outside services.
pub fn mini_map(
    map: &TeamMap,
    parts: &BTreeSet<String>,
    outside: &BTreeSet<String>,
    answer: &str,
) -> Value {
    let mut ids: BTreeSet<String> = parts.clone();
    let low = answer.to_ascii_lowercase();
    for p in &map.parts {
        if low.contains(&p.name.to_ascii_lowercase())
            || (p.dev_name.len() > 3 && low.contains(&p.dev_name.to_ascii_lowercase()))
        {
            ids.insert(p.id.clone());
        }
    }
    let conns: Vec<&am::Connection> = map
        .connections
        .iter()
        .filter(|c| {
            ids.contains(&c.from)
                && (ids.contains(&c.to)
                    || c.to
                        .strip_prefix("outside:")
                        .is_some_and(|o| outside.contains(o)))
        })
        .collect();
    let used_out: BTreeSet<&str> = conns
        .iter()
        .filter_map(|c| c.to.strip_prefix("outside:"))
        .collect();
    json!({
        "parts": map.parts.iter().filter(|p| ids.contains(&p.id)).map(|p| json!({"id": p.id, "name": p.name, "kind": p.kind, "dev_name": p.dev_name, "repo": p.repo})).collect::<Vec<_>>(),
        "connections": conns,
        "outside": map.outside.iter().filter(|o| used_out.contains(o.id.as_str())).collect::<Vec<_>>(),
    })
}

#[derive(Deserialize)]
pub struct AskBody {
    pub question: String,
}

/// POST /api/app-map/ask — {question}. Chat's agent with the map lookups.
pub async fn ask(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<AskBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let fail = |code: StatusCode, msg: &str| (code, Json(json!({ "error": msg })));
    // Members and up, as Chat (the `brain_ask` capability).
    if claims.require_role(1).is_err() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Asking the map needs a member's role.",
        ));
    }
    let question: String = body.question.trim().chars().take(1_000).collect();
    if question.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Ask a question."));
    }
    let maps = team_maps(&state, &claims.team_id)
        .await
        .map_err(|c| fail(c, "The map couldn't be read."))?;
    if maps.is_empty() {
        return Err(fail(StatusCode::CONFLICT, "The map is still being built."));
    }
    let (api_key, _heavy, check_model_id) =
        super::brain::load_provider(&state, &claims.team_id).await;
    let mut tools = MapBox::new(maps);
    let model = chat_agent::ApiModel {
        http: reqwest::Client::new(),
        api_key: api_key.unwrap_or_default(),
        meter: crate::model_call::chat_meter(&state, &claims.team_id),
        // The team's everyday model: a map question needs lookups, not depth,
        // and the person is waiting.
        model_id: check_model_id.clone(),
        check_model_id,
        kind_of: |_| "app_map",
    };
    let mut reg = Registry::default();
    let ask = chat_agent::Ask {
        system: ASK_SYSTEM.to_string(),
        messages: vec![json!({"role": "user", "content": [{"type": "text", "text": question}]})],
        question: &question,
        limits: chat_agent::Limits {
            turns: 6,
            tool_calls: 10,
            retries: 0,
        },
        must_look: None,
    };
    let out = chat_agent::run(&model, &mut tools, &Quiet, &mut reg, ask)
        .await
        .map_err(|_| {
            fail(
                StatusCode::BAD_GATEWAY,
                "The model couldn't be reached. Try again in a moment.",
            )
        })?;
    let answer = out.fin.text.clone();
    Ok(Json(json!({
        "answer": answer,
        "grounded": out.fin.grounded,
        "steps": steps_of(&answer),
        "map": mini_map(&tools.map, &tools.parts, &tools.outside, &answer),
        "lookups": out.used,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::app_map::{build_parts, repo_map, RepoInput};
    use common::app_surface::scan;

    fn maps(repo: &str) -> Vec<RepoMap> {
        let files = [
            (
                "package.json",
                r#"{"name": "dock-api", "dependencies": {"express": "4"}}"#,
            ),
            (
                "src/slips.ts",
                "import express from 'express';\nrouter.get('/slips/:id', (req, res) => {});",
            ),
            (
                "web/app/slips/page.tsx",
                "export default () => fetch('/slips/1')",
            ),
        ];
        let input = RepoInput {
            repo: repo.into(),
            commit: "c1".into(),
            files: files.iter().map(|(p, _)| p.to_string()).collect(),
            surfaces: files
                .iter()
                .map(|(p, s)| (p.to_string(), scan(p, s)))
                .collect(),
            imports: vec![],
        };
        vec![repo_map(&input, build_parts(&input), "now")]
    }

    #[test]
    fn a_team_reads_only_its_own_map() {
        let mine = maps("harbor/dock");
        let q = |id: &str| MapQuery {
            id: id.into(),
            ..Default::default()
        };
        assert!(view(&mine, "part", &q("harbor/dock#.")).is_some());
        // Another team's part, file or screen: not found in this team's map.
        assert!(view(&mine, "part", &q("other/marina#.")).is_none());
        let f = MapQuery {
            repo: "other/marina".into(),
            path: "src/slips.ts".into(),
            ..Default::default()
        };
        assert!(view(&mine, "file", &f).is_none());
        assert!(view(
            &mine,
            "screen",
            &q("other/marina#web/app/slips/page.tsx#/slips")
        )
        .is_none());
        assert!(view(
            &[],
            "search",
            &MapQuery {
                q: "slips".into(),
                ..Default::default()
            }
        )
        .unwrap()["parts"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(am::map_pk("team-a"), "AM#team-a");
    }

    #[test]
    fn the_build_state_reads_plainly() {
        let st = |s: &str, at: i64| am::Status {
            state: s.into(),
            at,
            ..Default::default()
        };
        assert_eq!(shown_state(None, false, 100), ("not_built", false));
        assert_eq!(
            shown_state(Some(&st("building", 100)), false, 100),
            ("building", true)
        );
        assert_eq!(
            shown_state(Some(&st("building", 100)), true, 100),
            ("ready", true)
        );
        assert_eq!(
            shown_state(Some(&st("failed", 100)), false, 100),
            ("failed", false)
        );
        // Stuck past the limit: built again on the next visit.
        assert_eq!(
            shown_state(Some(&st("queued", 0)), false, am::STALE_SECS + 1),
            ("not_built", false)
        );
        assert_eq!(
            shown_state(Some(&st("ready", 100)), false, 100),
            ("empty", false)
        );
    }

    #[test]
    fn the_ask_lookups_answer_from_the_map_and_record_what_they_touched() {
        let mut b = MapBox::new(maps("harbor/dock"));
        let (v, _) = b.lookup("map_part", &json!({"id": "harbor/dock#."}));
        assert!(v["endpoints"].as_array().is_some_and(|e| !e.is_empty()));
        assert!(b.parts.contains("harbor/dock#."));
        let (v, _) = b.lookup("map_part", &json!({"id": "nope"}));
        assert!(v.get("error").is_some());
        let (v, _) = b.lookup("map_screen", &json!({"screen": "/slips"}));
        assert!(
            v["requests"].as_array().is_some_and(|r| !r.is_empty()),
            "{v}"
        );
        let defs = tool_defs();
        let names: Vec<&str> = defs.iter().filter_map(|d| d["name"].as_str()).collect();
        assert_eq!(
            names,
            vec![
                "map_overview",
                "map_find",
                "map_part",
                "map_screen",
                "map_file"
            ]
        );
    }

    #[test]
    fn answers_give_numbered_steps_and_a_mini_map() {
        let a = "Booking a slip:\n1. The slip screen asks the API [S1].\n2) The API reads the slip.\nNot a step.\n10. Done.";
        assert_eq!(
            steps_of(a),
            vec![
                "The slip screen asks the API [S1].",
                "The API reads the slip.",
                "Done."
            ]
        );
        let m = maps("harbor/dock");
        let map = am::assemble(&m);
        let mini = mini_map(
            &map,
            &BTreeSet::from(["harbor/dock#.".to_string()]),
            &BTreeSet::new(),
            "x",
        );
        assert_eq!(mini["parts"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn overlay_items_read_findings_and_plans() {
        let f = json!({"id": "f1", "repo": "harbor/dock", "manifest": "package.json", "tier": "P1",
            "advisory": {"title": "Prototype pollution"}, "evidence": {"lines": [{"file": "src/slips.ts", "line": 3}]}});
        let it = security_item(&f);
        assert_eq!(it.files, vec!["package.json", "src/slips.ts"]);
        assert_eq!(it.link, "/security/detail?id=f1");
        let r = json!({"id": "i1", "title": "Open bucket", "severity": "important", "fix": {"location": {"repo": "harbor/dock", "file": "infra/main.tf"}}});
        assert_eq!(infra_item(&r).unwrap().files, vec!["infra/main.tf"]);
        assert!(infra_item(&json!({"id": "i2"})).is_none());
        assert_eq!(why_of("# Proposal\n\n## Why\nBoaters can't book a slip twice.\nIt double charges.\n\n## What\nx"), "Boaters can't book a slip twice. It double charges.");
        assert_eq!(why_of("Fix the tide chart."), "Fix the tide chart.");
    }
}
