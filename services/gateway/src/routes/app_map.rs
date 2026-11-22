//! Dashboard API for the APP MAP: the team's map as Floorplan (vendored at
//! `vendor/floorplan`, MIT) built it in the worker (`common::app_map`), with
//! CoderHelm's Recent work and Health put into the map's own fields when it
//! is read, so Floorplan's viewer shows them natively. Every read is the
//! signed-in team's own map: another team's part, file or screen is simply
//! not found.
//!
//! - `GET /api/app-map?recent=1&health=1`: the build status and the map
//!   (the first visit starts the first build);
//! - `GET /api/app-map/part?id=`, `/file?path=`, `/screen?id=`, `/search?q=`:
//!   lookups;
//! - `POST /api/app-map/ask`: a question answered by Chat's agent with map
//!   lookups, with numbered steps and a small map of the parts involved;
//! - `POST /api/app-map/rebuild`: an admin builds it again.

use std::collections::BTreeSet;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::app_map::{self as am, HealthItem, WorkItem};
use common::knowledge::answer::{Registry, SourceRef};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info};

use super::chat_agent;
use crate::models::Claims;
use crate::AppState;

/// Queues a build: once (`force` false) or again (an admin).
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
        ..Default::default()
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
                    step: String::new(),
                    error: "The build could not be started. Try again in a moment.".into(),
                },
            )
            .await;
            false
        }
    }
}

/// What the page shows about the build: `building` while there is no map
/// yet, else `ready` (with `updating` while an update runs).
pub fn shown_state(st: Option<&am::Status>, has_map: bool, now: i64) -> (&'static str, bool) {
    let running =
        st.is_some_and(|s| matches!(s.state.as_str(), "queued" | "building") && !s.is_stale(now));
    match (has_map, st) {
        (true, _) => ("ready", running),
        (false, _) if running => ("building", true),
        (false, Some(s)) if s.state == "failed" => ("failed", false),
        (false, Some(s)) if s.state == "ready" => ("empty", false),
        _ => ("not_built", false),
    }
}

#[derive(Deserialize, Default)]
pub struct MapQuery {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub recent: Option<u8>,
    #[serde(default)]
    pub health: Option<u8>,
}

/// GET /api/app-map — the build status and the map, ready for the viewer.
pub async fn get_map(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    let table = &state.config.settings_table_name;
    let (graph, st) = tokio::join!(
        am::load_graph(&state.s3, &state.config.bucket_name, &claims.team_id),
        am::status(&state.dynamo, table, &claims.team_id),
    );
    let now = chrono::Utc::now().timestamp();
    let (mut shown, mut updating) = shown_state(st.as_ref(), graph.is_some(), now);
    if shown == "not_built" && queue_build(&state, &claims.team_id, false).await {
        shown = "building";
        updating = true;
    }
    let status = json!({
        "state": shown,
        "updating": updating,
        "step": st.as_ref().map(|s| s.step.clone()).unwrap_or_default(),
        "note": st.as_ref().map(|s| s.error.clone()).unwrap_or_default(),
        "floorplan": am::FLOORPLAN_VERSION.trim(),
    });
    let Some(graph) = graph else {
        return Ok(Json(json!({ "status": status, "graph": Value::Null })));
    };
    let days = 14;
    let (runs, found) = tokio::join!(
        async {
            if q.recent == Some(1) {
                Some(recent_runs(&state, &claims.team_id, days).await)
            } else {
                None
            }
        },
        async {
            if q.health == Some(1) {
                Some(health(&state, &claims.team_id, days).await)
            } else {
                None
            }
        },
    );
    let mut runs = runs;
    if let Some(runs) = runs.as_mut() {
        for w in runs.iter_mut().take(25) {
            w.why = plan_why(&state, &claims.team_id, &w.ticket, &w.repo).await;
        }
    }
    let graph = am::with_overlays(graph, runs.as_deref(), found.as_deref());
    let graph = am::for_viewer(graph, &chrono::Utc::now().to_rfc3339());
    Ok(Json(json!({ "status": status, "graph": graph })))
}

fn bounded(s: &str, max: usize) -> Result<(), StatusCode> {
    let s = s.trim();
    if s.is_empty() || s.len() > max || s.contains('\0') {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

/// One lookup in the signed-in team's map: `None` when what was asked for
/// isn't in it (another team's part, file or screen included).
pub fn view(graph: &Value, kind: &str, q: &MapQuery) -> Option<Value> {
    match kind {
        "part" => am::part_view(graph, q.id.trim()),
        "file" => am::file_view(graph, q.path.trim()),
        "screen" => am::screen_view(graph, q.id.trim()),
        "search" => Some(am::search(graph, &q.q)),
        _ => None,
    }
}

async fn one(
    state: &AppState,
    claims: &Claims,
    kind: &str,
    q: &MapQuery,
) -> Result<Json<Value>, StatusCode> {
    let graph = am::load_graph(&state.s3, &state.config.bucket_name, &claims.team_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    view(&graph, kind, q).map(Json).ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/app-map/part?id= — one part: files, endpoints, lines in and out.
pub async fn part(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.id, 300)?;
    one(&state, &claims, "part", &q).await
}

/// GET /api/app-map/file?path= — one file (`<repo folder>/<path>`).
pub async fn file(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.path, 600)?;
    one(&state, &claims, "file", &q).await
}

/// GET /api/app-map/screen?id= — everything behind one screen (its id or
/// its address).
pub async fn screen(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    bounded(&q.id, 600)?;
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

/// POST /api/app-map/rebuild — admin. Builds the map again (Floorplan's
/// refresh carries every description that still fits).
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

// ── Recent work and Health (CoderHelm's own records) ───────────────────────

/// The `Why` of a plan's proposal: its first paragraph under a "Why"
/// heading, or its first paragraph.
pub fn why_of(proposal: &str) -> String {
    let mut under_why = false;
    let mut out = Vec::new();
    for l in proposal.lines().map(str::trim) {
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

/// Runs of the last `days` days that changed files, newest first, at most 60.
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

/// A dependency finding: its manifest and the code lines its evidence names.
pub fn security_item(f: &Value) -> HealthItem {
    let mut files: Vec<(String, u32)> = Vec::new();
    let manifest = str_at(f, &["manifest"]);
    if !manifest.is_empty() {
        files.push((manifest.to_string(), 0));
    }
    for l in f["evidence"]["lines"].as_array().into_iter().flatten() {
        let file = l["file"].as_str().unwrap_or("");
        if !file.is_empty() {
            files.push((file.to_string(), l["line"].as_u64().unwrap_or(0) as u32));
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
        severity: f["tier"].as_str().unwrap_or("").to_string(),
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
            vec![(file.to_string(), loc["line"].as_u64().unwrap_or(0) as u32)]
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
            if !matches!(str_at(&v, &["status"]), "fixed" | "dismissed" | "resolved") {
                out.extend(infra_item(&v));
            }
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
                severity: "high".into(),
                repo: s("repo"),
                files: Vec::new(),
                link: format!("/alerts/detail?id={}", urlencoding::encode(&s("id"))),
            });
        }
    }
    out
}

// ── Ask the map ────────────────────────────────────────────────────────────

/// The Ask agent's lookups.
pub fn tool_defs() -> Vec<Value> {
    let obj = |props: Value, req: &[&str]| json!({"type": "object", "properties": props, "required": req});
    vec![
        json!({"name": "map_overview", "description": "The app map in short: every part (id, name, kind, what it does), the screens, the Health counts and the repositories.", "input_schema": obj(json!({}), &[])}),
        json!({"name": "map_find", "description": "Search the map: parts, files, endpoints and screens whose names, descriptions or paths hold every word.", "input_schema": obj(json!({"query": {"type": "string"}}), &["query"])}),
        json!({"name": "map_part", "description": "One part by id: its sub-parts, files, endpoints, and the lines in and out (imports, web requests matched to endpoints, outside services).", "input_schema": obj(json!({"id": {"type": "string"}}), &["id"])}),
        json!({"name": "map_screen", "description": "Everything behind one screen, by its id or its address (like /slips/:id), step by step: the files it uses, the requests it makes, the server code that answers and the outside services behind it.", "input_schema": obj(json!({"screen": {"type": "string"}}), &["screen"])}),
        json!({"name": "map_file", "description": "One file by its path on the map (repository folder first, like harbor-api/src/bookings.js): what it is, its endpoints and what it connects to.", "input_schema": obj(json!({"path": {"type": "string"}}), &["path"])}),
    ]
}

/// The Ask agent's toolbox over one team's map. Records the map ids its
/// lookups returned, for the small map.
pub struct MapBox {
    pub graph: Value,
    pub defs: Vec<Value>,
    pub ids: Vec<String>,
}

impl MapBox {
    pub fn new(graph: Value) -> Self {
        Self {
            graph,
            defs: tool_defs(),
            ids: Vec::new(),
        }
    }

    fn note(&mut self, v: &Value) {
        let mut stack = vec![v];
        while let Some(x) = stack.pop() {
            match x {
                Value::Object(o) => {
                    if let Some(id) = o.get("id").and_then(|i| i.as_str()) {
                        self.ids.push(id.to_string());
                    }
                    if let Some(p) = o.get("path").and_then(|i| i.as_str()) {
                        self.ids.push(format!("f:{p}"));
                    }
                    stack.extend(o.values());
                }
                Value::Array(a) => stack.extend(a.iter()),
                _ => {}
            }
        }
    }

    /// Runs one lookup: (what the model reads, label).
    pub fn lookup(&mut self, name: &str, input: &Value) -> (Value, String) {
        let s = |k: &str| input[k].as_str().unwrap_or("").trim().to_string();
        let (v, label) = match name {
            "map_overview" => (am::overview(&self.graph), "Read the map".to_string()),
            "map_find" => (am::search(&self.graph, &s("query")), format!("Searched the map for {}", s("query"))),
            "map_part" => (
                am::part_view(&self.graph, &s("id")).unwrap_or_else(|| json!({"error": "No part has that id; use map_overview or map_find for ids."})),
                "Opened a part".to_string(),
            ),
            "map_screen" => (
                am::screen_view(&self.graph, &s("screen")).unwrap_or_else(|| json!({"error": "No screen has that id or address; use map_overview."})),
                format!("Followed the screen {}", s("screen")),
            ),
            "map_file" => (
                am::file_view(&self.graph, &s("path")).unwrap_or_else(|| json!({"error": "That file isn't on the map."})),
                format!("Opened {}", s("path")),
            ),
            _ => (json!({"error": "Unknown lookup."}), name.to_string()),
        };
        if v.get("error").is_none() && !matches!(name, "map_overview" | "map_find") {
            self.note(&v);
        }
        (v, label)
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

pub const ASK_SYSTEM: &str = "You answer questions about how a team's app fits together, from its app map (made with Floorplan). \
Use the map lookups (start with map_overview or map_find, then open parts, screens and files) and answer only from what they return. \
Write for someone who may not read code: plain words first, file paths in parentheses. When the question is about how something happens, \
answer with numbered steps, one line each, in the order it happens. Say plainly when a connection is inferred rather than certain. \
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
    let graph = am::load_graph(&state.s3, &state.config.bucket_name, &claims.team_id)
        .await
        .ok_or_else(|| fail(StatusCode::CONFLICT, "The map is still being built."))?;
    let (api_key, _heavy, check_model_id) =
        super::brain::load_provider(&state, &claims.team_id).await;
    let mut tools = MapBox::new(graph);
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
        // A map answer always cites the map; it is never small talk.
        uncited_ok: false,
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
        "graph": mini_map(&tools),
    })))
}

/// The parts the answer's lookups touched, as a map the same viewer draws;
/// `null` when they touched none.
pub fn mini_map(tools: &MapBox) -> Value {
    let parts: BTreeSet<String> = am::parts_of(&tools.graph, &tools.ids);
    if parts.is_empty() {
        return Value::Null;
    }
    am::for_viewer(
        am::sub_map(&tools.graph, &parts),
        &chrono::Utc::now().to_rfc3339(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn harbor() -> Value {
        let mut g: Value = serde_json::from_str(include_str!(
            "../../../worker/floorplan/fixtures/harbor-graph.json"
        ))
        .unwrap();
        am::stamp_repos(
            &mut g,
            &am::folders(&["harbor/harbor-web".into(), "harbor/harbor-api".into()]),
        );
        g
    }

    #[test]
    fn a_team_reads_only_its_own_map() {
        let mine = harbor();
        let q = |id: &str| MapQuery {
            id: id.into(),
            ..Default::default()
        };
        assert!(view(&mine, "part", &q("api")).is_some());
        // Another team's part, file or screen is not in this team's map.
        assert!(view(&mine, "part", &q("marina-docks")).is_none());
        assert!(view(
            &mine,
            "file",
            &MapQuery {
                path: "marina/src/x.js".into(),
                ..Default::default()
            }
        )
        .is_none());
        assert!(view(&mine, "screen", &q("screen:marina:/berths")).is_none());
        assert!(view(&mine, "screen", &q("/slips/:id")).is_some());
        // Each team's map has its own keys.
        assert_ne!(am::graph_key("team-a"), am::graph_key("team-b"));
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
    fn the_ask_lookups_answer_from_the_map_and_draw_what_they_touched() {
        let mut b = MapBox::new(harbor());
        let (v, _) = b.lookup("map_screen", &json!({"screen": "/slips/:id"}));
        assert_eq!(
            v["behind"].as_array().unwrap().last().unwrap()["step"],
            "outside"
        );
        let (v, _) = b.lookup("map_part", &json!({"id": "nope"}));
        assert!(v.get("error").is_some());
        let mini = mini_map(&b);
        let parts: Vec<&str> = mini["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|n| n["id"].as_str())
            .collect();
        assert_eq!(parts, vec!["web", "api", "stripe"]);
        assert_eq!(mini["meta"]["exported"]["by"], "Coderhelm");
        assert!(mini_map(&MapBox::new(harbor())).is_null());
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
    fn answers_give_numbered_steps() {
        let a = "Booking a slip:\n1. The slip page sends the booking [S1].\n2) The booking service charges the card.\nNot a step.\n10. Done.";
        assert_eq!(
            steps_of(a),
            vec![
                "The slip page sends the booking [S1].",
                "The booking service charges the card.",
                "Done."
            ]
        );
    }

    #[test]
    fn findings_and_plans_read_into_overlay_items() {
        let f = json!({"id": "f1", "repo": "harbor/harbor-api", "manifest": "package.json", "tier": "P1",
            "advisory": {"title": "Prototype pollution"}, "evidence": {"lines": [{"file": "src/bookings.js", "line": 5}]}});
        let it = security_item(&f);
        assert_eq!(
            it.files,
            vec![
                ("package.json".to_string(), 0),
                ("src/bookings.js".to_string(), 5)
            ]
        );
        assert_eq!(it.link, "/security/detail?id=f1");
        let r = json!({"id": "i1", "title": "Open bucket", "severity": "important", "fix": {"location": {"repo": "harbor/harbor-api", "file": "infra/main.tf"}}});
        assert_eq!(
            infra_item(&r).unwrap().files,
            vec![("infra/main.tf".to_string(), 0)]
        );
        assert!(infra_item(&json!({"id": "i2"})).is_none());
        assert_eq!(why_of("# Proposal\n\n## Why\nBoaters can't book a slip twice.\nIt double charges.\n\n## What\nx"), "Boaters can't book a slip twice. It double charges.");
        // Placed on the map, in Floorplan's own Health field.
        let g = am::with_overlays(harbor(), None, Some(&[it]));
        assert!(g["health"]["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["source"] == "coderhelm"
                && i["items"] == json!(["f:harbor-api/src/bookings.js"])));
    }
}
