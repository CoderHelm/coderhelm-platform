//! Dashboard API for the APP MAP: the team's map as Floorplan (vendored at
//! `vendor/floorplan`, MIT) built it in the worker (`common::app_map`), with
//! CoderHelm's Recent work and Health put into the map's own fields when it
//! is read, so Floorplan's viewer shows them natively. Every read is the
//! signed-in team's own map: another team's part, file or screen is simply
//! not found.
//!
//! - `GET /api/app-map?recent=1&health=1`: the build status, a short-lived
//!   signed address of the page's copy of the map (the browser reads it from
//!   the bucket), and Recent work and Health as a small payload the page lays
//!   over it (the first visit starts the first build); `graph=0`: the status
//!   only;
//! - `GET /api/app-map/graph`: the page's copy through the API, as stored,
//!   for a browser that can't reach the bucket;
//! - `GET /api/app-map/part?id=`, `/file?path=`, `/screen?id=`, `/search?q=`:
//!   lookups;
//! - questions about the map are Chat's: Chat offers the map lookups
//!   (`common::app_map::tool_defs`), and the page's Ask box asks Chat;
//! - `POST /api/app-map/rebuild`: an admin builds it again.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::app_map::{self as am, HealthItem, WorkItem};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

/// Asks for a build, one at a time per team: `Ok(new)` when it is queued and
/// its message sent, `Err(running)` when a build is already queued or
/// running (that build stands and nothing is sent; `None` when the status
/// couldn't be read or the message couldn't be sent, and the build is
/// marked failed).
pub async fn queue_build(
    state: &AppState,
    team_id: &str,
) -> Result<am::Status, Option<am::Status>> {
    let table = &state.config.settings_table_name;
    let st = match am::queue_build(&state.dynamo, table, team_id).await {
        Ok(Ok(st)) => st,
        Ok(Err(running)) => {
            info!(team_id, "App map: a build is already queued or running");
            return Err(running);
        }
        Err(e) => {
            error!("app map: could not mark a build queued: {e}");
            return Err(None);
        }
    };
    send_build(state, team_id, st).await.ok_or(None)
}

/// Builds the map again when the one there now was made by another
/// extractor than this release's (`common::app_map::queue_for_extractor`):
/// once per release, through the same one-build-at-a-time conditional write,
/// so two pages read at once queue one build. The map there now is served
/// meanwhile. `None` when nothing was queued.
pub async fn queue_release_build(state: &AppState, team_id: &str) -> Option<am::Status> {
    let table = &state.config.settings_table_name;
    let st = match am::queue_release_build(&state.dynamo, table, team_id).await {
        Ok(Some(st)) => st,
        Ok(None) => return None,
        Err(e) => {
            error!("app map: could not mark a release's build queued: {e}");
            return None;
        }
    };
    info!(
        team_id,
        extractor = am::EXTRACTOR_VERSION,
        "App map: made by another extractor, built again"
    );
    send_build(state, team_id, st).await
}

/// Sends a queued build's first message; when it can't be sent, the build is
/// marked failed (`None`).
async fn send_build(state: &AppState, team_id: &str, st: am::Status) -> Option<am::Status> {
    let table = &state.config.settings_table_name;
    let msg = crate::models::WorkerMessage::AppMap(crate::models::AppMapMessage {
        team_id: team_id.to_string(),
        build: st.build.clone(),
        turn: 0,
        ..Default::default()
    });
    let body = serde_json::to_string(&msg).unwrap_or_default();
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
            Some(st)
        }
        Err(e) => {
            error!("app map: could not queue a build: {e}");
            let now = chrono::Utc::now().timestamp();
            let why = "The build couldn't be started. Try again in a moment.";
            let _ = am::update_status(&state.dynamo, table, team_id, |cur| {
                am::failed(cur, &st.build, why, now)
            })
            .await;
            None
        }
    }
}

/// What the page shows about the build: `building` while there is no map
/// yet, else `ready` (with `updating` while an update runs).
pub fn shown_state(st: Option<&am::Status>, has_map: bool, now: i64) -> (&'static str, bool) {
    let running = st.is_some_and(|s| s.is_running(now));
    match (has_map, st) {
        (true, _) => ("ready", running),
        (false, _) if running => ("building", true),
        (false, Some(s)) if s.state == "failed" => ("failed", false),
        (false, Some(s)) if s.state == "ready" => ("empty", false),
        _ => ("not_built", false),
    }
}

/// Whether reading the page asks for the map to be built again because the
/// map there now was made by another extractor than this release's (a
/// build already queued or running builds with this one anyway).
pub fn needs_release_build(st: Option<&am::Status>, has_map: bool, now: i64) -> bool {
    !st.is_some_and(|s| s.is_running(now))
        && am::made_by_another_extractor(st, has_map, am::EXTRACTOR_VERSION)
}

/// The map there now, as far as the status page needs it: when it was
/// written and how many repositories it covers.
#[derive(Debug, Clone, Copy, Default)]
pub struct MapFacts {
    pub has_map: bool,
    pub written_at: i64,
    pub repos: u32,
}

/// The build status the page reads: the state, the running build's step,
/// when it started and how long it usually takes for `team_repos`
/// repositories, and when the map there now was built.
pub fn status_view(st: Option<&am::Status>, map: MapFacts, team_repos: u32, now: i64) -> Value {
    let (shown, updating) = shown_state(st, map.has_map, now);
    let s = st.cloned().unwrap_or_default();
    let running = updating;
    let repos = if s.repos > 0 { s.repos } else { team_repos };
    let (low, high) = am::estimate_secs(repos);
    let built_at = if s.built > 0 { s.built } else { map.written_at };
    let built_repos = if s.built_repos > 0 {
        s.built_repos
    } else {
        map.repos
    };
    json!({
        "state": shown,
        "updating": updating,
        "step": if running { s.step.clone() } else { String::new() },
        // Kept after a build stops, so a failure can say where it stopped.
        "stage": s.stage,
        // How the last build ended (`failed` with a map there: an update
        // that didn't finish, and the map is the one before it).
        "last_build": s.state,
        "note": s.error,
        "started_at": if running { s.started.max(0) } else { 0 },
        "repos": repos,
        "estimate": if running { json!({ "low_secs": low, "high_secs": high }) } else { Value::Null },
        "built_at": if map.has_map { built_at } else { 0 },
        "built_repos": if map.has_map { built_repos } else { 0 },
        "now": now,
        "floorplan": am::FLOORPLAN_VERSION.trim(),
        // `release` while the map is built again because this release
        // extracts more than the one that made it; the page says so.
        "reason": if st.is_some_and(|s| am::rebuilding_for_release(s, now)) { "release" } else { "" },
        // A repository the journeys couldn't read, in one plain line (empty
        // once it can be read again).
        "unread": s.unread,
        "journeys_note": am::unread_line(&s.unread),
    })
}

/// What the Knowledge page says about the map: it is not embedded as
/// passages, runs and Chat look things up in it directly.
pub const KNOWLEDGE_NOTE: &str = "Not split into passages like a knowledge base: runs and Chat look things up in the map directly.";

/// The App map's card on the Knowledge page: its state (`built`,
/// `building`, `rebuilding`, `failed` or `not_built`), when it was built,
/// what it covers, where it is, and whether this person may build it again
/// (admins, one build at a time through `POST /api/app-map/rebuild`).
pub fn knowledge_view(
    st: Option<&am::Status>,
    map: MapFacts,
    coverage: Option<&am::Coverage>,
    can_rebuild: bool,
    now: i64,
) -> Value {
    let (shown, updating) = shown_state(st, map.has_map, now);
    let state = match (shown, updating) {
        ("ready", true) => "rebuilding",
        ("ready", false) => "built",
        ("building", _) => "building",
        ("failed", _) => "failed",
        _ => "not_built",
    };
    let s = st.cloned().unwrap_or_default();
    let built_at = if s.built > 0 { s.built } else { map.written_at };
    let coverage = coverage.filter(|c| c.counted).map(|c| {
        json!({
            "screens": c.screens,
            "outside_services": c.outside_services,
            "cloud_links": c.cloud_links,
            "repos": c.repos,
            "journeys": c.journeys,
        })
    });
    json!({
        "state": state,
        "built_at": if map.has_map { built_at } else { 0 },
        // With a map there, the last update that didn't finish.
        "last_failed": map.has_map && s.state == "failed",
        "note": if state == "failed" || (map.has_map && s.state == "failed") { s.error } else { String::new() },
        "repos": if map.has_map { map.repos } else { 0 },
        "coverage": coverage,
        "href": "/app-map",
        "can_rebuild": can_rebuild,
        "embedded": false,
        "explain": KNOWLEDGE_NOTE,
    })
}

/// [`knowledge_view`] for the signed-in team, from the status, when the map
/// was written and its small index (never the map itself). Reading it never
/// starts a build.
pub async fn knowledge_card(state: &AppState, claims: &Claims) -> Value {
    let bucket = &state.config.bucket_name;
    let team = claims.team_id.as_str();
    let (written, index, st) = tokio::join!(
        am::graph_written_at(&state.s3, bucket, team),
        am::load_index(&state.s3, bucket, team),
        am::status(&state.dynamo, &state.config.settings_table_name, team),
    );
    let index = index.map(|(i, _)| i);
    let facts = match written {
        Some(at) => MapFacts {
            has_map: true,
            written_at: at,
            repos: index
                .as_ref()
                .map(|i| i.repos.len() as u32)
                .or(st.as_ref().map(|s| s.built_repos))
                .unwrap_or(0),
        },
        None => MapFacts::default(),
    };
    let can_rebuild = claims.support_view.is_none() && claims.require_role(3).is_ok();
    knowledge_view(
        st.as_ref(),
        facts,
        index.as_ref().map(|i| &i.coverage),
        can_rebuild,
        chrono::Utc::now().timestamp(),
    )
}

/// The team's turned-on repositories, counted for the estimate (0 when
/// they couldn't be read).
async fn team_repo_count(state: &AppState, team_id: &str) -> u32 {
    super::repo_setup::team_repos(state, team_id)
        .await
        .map(|r| r.len() as u32)
        .unwrap_or(0)
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
    /// `graph=0`: the build status only, for a page watching a build.
    #[serde(default)]
    pub graph: Option<u8>,
}

/// How long the signed address of the page's copy of the map stays good.
pub const VIEW_URL_SECS: u64 = 15 * 60;

/// Where the page reads the map when the bucket can't be reached from the
/// browser (`GET /api/app-map/graph`, the same bytes through the API).
pub const VIEW_FALLBACK: &str = "/api/app-map/graph";

/// GET /api/app-map — the build status, where to read the map, and
/// CoderHelm's overlays for it.
///
/// The map itself never passes through here: the worker writes the page's
/// copy at build time (`common::app_map::view_key`, gzipped JSON the browser
/// unzips), and the answer carries a short-lived signed address for it
/// (`graph_url`). Recent work and Health come as a small payload
/// (`overlays`) placed with the map's index (`common::app_map::MapIndex`),
/// which the page lays over the map. A map built before the copy and the
/// index existed gets them written here once.
pub async fn get_map(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<MapQuery>,
) -> Result<Json<Value>, StatusCode> {
    let table = &state.config.settings_table_name;
    let bucket = &state.config.bucket_name;
    let team = claims.team_id.as_str();
    let status_only = q.graph == Some(0);
    let (written, index, mut st) = tokio::join!(
        am::graph_written_at(&state.s3, bucket, team),
        async {
            if status_only {
                None
            } else {
                am::load_index(&state.s3, bucket, team).await
            }
        },
        am::status(&state.dynamo, table, team),
    );
    let now = chrono::Utc::now().timestamp();
    // The index is written after the map (and the page's copy before the
    // index): one older than the map belongs to an earlier map. A copy made
    // the way an earlier release made it is written again from the map.
    let index = match (index, written) {
        (Some((idx, at)), Some(map_at)) if at + 1 >= map_at && idx.copy >= am::COPY_VERSION => {
            Some(idx)
        }
        (_, Some(_)) if !status_only => backfill(&state, team).await,
        _ => None,
    };
    let facts = match written {
        Some(at) => MapFacts {
            has_map: true,
            written_at: at,
            repos: index
                .as_ref()
                .map(|i| i.repos.len() as u32)
                .or(st.as_ref().map(|s| s.built_repos))
                .unwrap_or(0),
        },
        None => MapFacts::default(),
    };
    // A map made by another extractor than this release's is built again
    // (once), and served as it is until the new one is there.
    if needs_release_build(st.as_ref(), facts.has_map, now) {
        if let Some(new) = queue_release_build(&state, team).await {
            st = Some(new);
        }
    }
    // The first visit starts the first build.
    if shown_state(st.as_ref(), facts.has_map, now).0 == "not_built" {
        match queue_build(&state, team).await {
            Ok(new) => st = Some(new),
            Err(Some(running)) => st = Some(running),
            Err(None) => st = am::status(&state.dynamo, table, team).await,
        }
    }
    let running = st.as_ref().is_some_and(|s| s.is_running(now));
    let team_repos = if running && st.as_ref().is_some_and(|s| s.repos == 0) {
        team_repo_count(&state, team).await
    } else {
        0
    };
    let status = status_view(st.as_ref(), facts, team_repos, now);
    let (Some(idx), false) = (index, status_only) else {
        return Ok(Json(json!({ "status": status, "graph": Value::Null })));
    };
    let url = view_url(&state, team).await.map_err(|e| {
        error!("app map: could not sign the map's address: {e}");
        StatusCode::BAD_GATEWAY
    })?;
    let overlays = overlays_for(&state, team, &idx, &q).await;
    Ok(Json(json!({
        "status": status,
        "graph": Value::Null,
        "graph_url": url,
        "graph_fallback": VIEW_FALLBACK,
        "graph_at": facts.written_at,
        "overlays": overlays,
    })))
}

/// Writes the page's copy and the index of a map built before they existed
/// (or by a worker that didn't write them), once; `None` when the map can't
/// be read.
async fn backfill(state: &AppState, team: &str) -> Option<am::MapIndex> {
    let bucket = &state.config.bucket_name;
    let graph = am::load_graph(&state.s3, bucket, team).await?;
    match am::store_derived(&state.s3, bucket, team, &graph).await {
        Ok(idx) => {
            info!(team_id = team, "App map: wrote the page's copy and index");
            Some(idx)
        }
        Err(e) => {
            error!("{e}");
            None
        }
    }
}

/// A signed GET for the page's copy of the team's map.
async fn view_url(state: &AppState, team: &str) -> Result<String, String> {
    let cfg = aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(
        VIEW_URL_SECS,
    ))
    .map_err(|e| e.to_string())?;
    let req = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(am::view_key(team))
        .presigned(cfg)
        .await
        .map_err(|e| e.to_string())?;
    Ok(req.uri().to_string())
}

/// Plans read at once for Recent work's "why".
const WHY_AT_ONCE: usize = 8;

/// CoderHelm's Recent work and Health for the map (each when asked for),
/// placed with the map's index.
async fn overlays_for(state: &AppState, team: &str, idx: &am::MapIndex, q: &MapQuery) -> Value {
    use futures::StreamExt;
    let days = 14;
    let (runs, found) = tokio::join!(
        async {
            if q.recent != Some(1) {
                return None;
            }
            let mut runs = recent_runs(state, team, days).await;
            // Only runs that are on the map are shown: read the plans of
            // those, a few at a time.
            runs.retain(|w| w.files.iter().any(|f| idx.map_path(&w.repo, f).is_some()));
            let asks: Vec<(String, String)> = runs
                .iter()
                .take(25)
                .map(|w| (w.ticket.clone(), w.repo.clone()))
                .collect();
            let whys: Vec<String> = futures::stream::iter(asks)
                .map(|(ticket, repo)| async move { plan_why(state, team, &ticket, &repo).await })
                .buffered(WHY_AT_ONCE)
                .collect()
                .await;
            for (w, why) in runs.iter_mut().zip(whys) {
                w.why = why;
            }
            Some(runs)
        },
        async {
            if q.health == Some(1) {
                Some(health(state, team, days).await)
            } else {
                None
            }
        },
    );
    am::overlays(idx, runs.as_deref(), found.as_deref())
}

/// GET /api/app-map/graph — the page's copy of the map through the API, as
/// stored (gzipped JSON the browser unzips), for a browser that can't reach
/// the bucket. The bytes are passed on, never read.
pub async fn graph(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<axum::response::Response, StatusCode> {
    use axum::response::IntoResponse;
    let obj = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(am::view_key(&claims.team_id))
        .send()
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let bytes = obj
        .body
        .collect()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        .into_bytes();
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::CONTENT_ENCODING, "gzip"),
            (axum::http::header::CACHE_CONTROL, "private, no-store"),
        ],
        bytes,
    )
        .into_response())
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
    let mut graph = am::load_graph(&state.s3, &state.config.bucket_name, &claims.team_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    am::name_by_repo(&mut graph);
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
/// refresh carries every description that still fits), one build at a time:
/// while one is queued or running it answers 409 with that build's status
/// and starts nothing.
pub async fn rebuild(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(3)?;
    let team = &claims.team_id;
    let written = am::graph_written_at(&state.s3, &state.config.bucket_name, team).await;
    let facts = |s: &am::Status| MapFacts {
        has_map: written.is_some(),
        written_at: written.unwrap_or(0),
        repos: s.built_repos,
    };
    let now = chrono::Utc::now().timestamp();
    match queue_build(&state, team).await {
        Ok(st) => {
            let repos = if st.repos == 0 {
                team_repo_count(&state, team).await
            } else {
                0
            };
            Ok((
                StatusCode::ACCEPTED,
                Json(json!({ "status": status_view(Some(&st), facts(&st), repos, now) })),
            ))
        }
        Err(Some(running)) => Ok((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "The map is already being built. This build carries on; there is no need to start another.",
                "code": "already_building",
                "status": status_view(Some(&running), facts(&running), 0, now),
            })),
        )),
        Err(None) => Err(StatusCode::BAD_GATEWAY),
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

/// CoderHelm's open findings that name a repository, its four sources read
/// at once.
async fn health(state: &AppState, team_id: &str, days: u32) -> Vec<HealthItem> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    let (security, infra, logs, alerts) = tokio::join!(
        super::security::load_findings(state, team_id),
        super::infra_security::load_records(state, team_id),
        common::log_findings::list_findings(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
            200,
        ),
        state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":s", AttributeValue::S("ALERT_EVENT#".into()))
            .scan_index_forward(false)
            .limit(200)
            .send(),
    );
    let mut out = Vec::new();
    if let Ok(found) = security {
        for (f, _) in found {
            let v = serde_json::to_value(&f).unwrap_or_default();
            if OPEN_SECURITY.contains(&str_at(&v, &["status"])) {
                out.push(security_item(&v));
            }
        }
    }
    if let Ok(records) = infra {
        for r in records {
            let v = serde_json::to_value(&r).unwrap_or_default();
            if !matches!(str_at(&v, &["status"]), "fixed" | "dismissed" | "resolved") {
                out.extend(infra_item(&v));
            }
        }
    }
    if let Ok(found) = logs {
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
    if let Ok(page) = alerts {
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

    /// The Knowledge page's card: each state, when it was built, what it
    /// covers, the link, who may build it again, and the line saying it is
    /// looked up directly rather than split into passages.
    #[test]
    fn the_knowledge_card_says_where_the_map_stands() {
        let now = 1_000_000;
        let cov = am::MapIndex::of(&harbor()).coverage;
        let map = MapFacts {
            has_map: true,
            written_at: now - 3600,
            repos: 2,
        };
        let ready = am::Status {
            state: "ready".into(),
            at: now - 3600,
            built: now - 3600,
            ..Default::default()
        };
        let v = knowledge_view(Some(&ready), map, Some(&cov), true, now);
        assert_eq!(v["state"], "built");
        assert_eq!(v["built_at"], now - 3600);
        assert_eq!(
            v["coverage"]["screens"],
            json!({"harbor-mobile": 3, "web": 1})
        );
        assert_eq!(v["coverage"]["outside_services"], 4);
        assert_eq!(v["coverage"]["cloud_links"], 2);
        assert_eq!(v["href"], "/app-map");
        assert_eq!(v["can_rebuild"], true);
        assert_eq!(v["embedded"], false);
        assert_eq!(v["explain"], KNOWLEDGE_NOTE);
        // Building again with a map there; building the first; failed.
        let running = am::Status {
            state: "building".into(),
            at: now - 10,
            ..ready.clone()
        };
        assert_eq!(
            knowledge_view(Some(&running), map, None, false, now)["state"],
            "rebuilding"
        );
        assert_eq!(
            knowledge_view(Some(&running), MapFacts::default(), None, false, now)["state"],
            "building"
        );
        let failed = am::Status {
            state: "failed".into(),
            at: now - 10,
            error: "The build couldn't be started.".into(),
            ..Default::default()
        };
        let f = knowledge_view(Some(&failed), MapFacts::default(), None, false, now);
        assert_eq!(f["state"], "failed");
        assert_eq!(f["note"], "The build couldn't be started.");
        assert_eq!(f["built_at"], 0);
        assert_eq!(f["coverage"], Value::Null);
        // An update that failed keeps the map built, and says so.
        let kept = knowledge_view(Some(&failed), map, Some(&cov), false, now);
        assert_eq!(
            (kept["state"].as_str(), kept["last_failed"].as_bool()),
            (Some("built"), Some(true))
        );
        assert_eq!(
            knowledge_view(None, MapFacts::default(), None, false, now)["state"],
            "not_built"
        );
        // An index from before the counts: no counts rather than zeros.
        assert_eq!(
            knowledge_view(
                Some(&ready),
                map,
                Some(&am::Coverage::default()),
                false,
                now
            )["coverage"],
            Value::Null
        );
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
        assert!(view(&mine, "part", &q("library-wings")).is_none());
        assert!(view(
            &mine,
            "file",
            &MapQuery {
                path: "library/src/x.js".into(),
                ..Default::default()
            }
        )
        .is_none());
        assert!(view(&mine, "screen", &q("screen:library:/booths")).is_none());
        assert!(view(&mine, "screen", &q("/slots/:id")).is_some());
        // Each team's map has its own keys.
        assert_ne!(am::graph_key("team-a"), am::graph_key("team-b"));
        assert_eq!(am::map_pk("team-a"), "AM#team-a");
    }

    #[test]
    fn a_map_from_another_extractor_is_built_again_once_and_served_meanwhile() {
        let now = 10_000;
        let older = am::Status {
            state: "ready".into(),
            at: now - 60,
            built: now - 86_400,
            built_repos: 2,
            extractor: "0000000000000000".into(),
            ..Default::default()
        };
        // Made by another extractor: the page asks for one build.
        assert!(needs_release_build(Some(&older), true, now));
        let q = am::queue_for_extractor(Some(&older), now, "b1", am::EXTRACTOR_VERSION).unwrap();
        // The next read (or a second read at once, after the conditional
        // write) asks for none, and the old map is served, updating, with
        // the reason the page words.
        assert!(!needs_release_build(Some(&q), true, now + 1));
        assert!(am::queue_for_extractor(Some(&q), now + 1, "b2", am::EXTRACTOR_VERSION).is_none());
        let facts = MapFacts {
            has_map: true,
            written_at: now - 86_400,
            repos: 2,
        };
        let v = status_view(Some(&q), facts, 0, now + 1);
        assert_eq!(
            (v["state"].as_str(), v["updating"].as_bool()),
            (Some("ready"), Some(true))
        );
        assert_eq!(v["reason"], "release");
        // A build that failed is not asked for again on every visit.
        let running = am::take_turn(Some(&q), "b1", 0, now + 2).unwrap();
        let f = am::failed(Some(&running), "b1", "x", now + 3).unwrap();
        assert!(!needs_release_build(Some(&f), true, now + 4));
        assert_eq!(status_view(Some(&f), facts, 0, now + 4)["reason"], "");
        // Made by this extractor: nothing asked, no reason given.
        let current = am::Status {
            extractor: am::EXTRACTOR_VERSION.into(),
            ..older.clone()
        };
        assert!(!needs_release_build(Some(&current), true, now));
        assert_eq!(status_view(Some(&current), facts, 0, now)["reason"], "");
        // No map yet: the first visit's build, not this one.
        assert!(!needs_release_build(None, false, now));
        // A push's build running: it builds with this extractor anyway.
        let pushed = am::queue_next(Some(&older), now, "p1").unwrap();
        assert!(!needs_release_build(Some(&pushed), true, now + 1));
        assert_eq!(status_view(Some(&pushed), facts, 0, now + 1)["reason"], "");
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

    fn admin(team: &str) -> Claims {
        Claims {
            sub: "USER#harbor-admin".into(),
            team_id: team.into(),
            email: "admin@harbor.example".into(),
            role: "admin".into(),
            github_login: None,
            aud: "coderhelm-dashboard".into(),
            exp: 9999999999,
            iat: 1000000000,
            support_view: None,
            custom: None,
            api_key: None,
            held: false,
            via_okta: false,
            okta_sub: None,
            entra: None,
        }
    }

    async fn press(state: &Arc<AppState>, team: &str) -> (StatusCode, Value) {
        let (code, Json(body)) = rebuild(State(state.clone()), Extension(admin(team)))
            .await
            .unwrap();
        (code, body)
    }

    #[tokio::test]
    async fn a_second_build_is_refused_while_one_is_queued_or_running() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#harbor";
        // Three presses of "Build the map again" within seconds: one build.
        let (first, body) = press(&state, team).await;
        assert_eq!(first, StatusCode::ACCEPTED);
        assert_eq!(body["status"]["state"], "building");
        let started = body["status"]["started_at"].as_i64().unwrap();
        assert!(started > 0);
        for _ in 0..2 {
            let (code, body) = press(&state, team).await;
            assert_eq!(code, StatusCode::CONFLICT);
            assert_eq!(body["code"], "already_building");
            // The running build's own state comes back.
            assert_eq!(body["status"]["state"], "building");
            assert_eq!(body["status"]["started_at"], started);
        }
        let sent = f.store.sent();
        assert_eq!(
            sent.len(),
            1,
            "one build message, not one per press: {sent:?}"
        );
        let msg: Value = serde_json::from_str(&sent[0]).unwrap();
        let st = am::status(&state.dynamo, "settings", team).await.unwrap();
        assert_eq!(msg["build"], json!(st.build));
        assert_eq!(msg["turn"], 0);

        // The worker takes the start message once; the same message again
        // (or another build's) is skipped.
        let now = chrono::Utc::now().timestamp();
        let take = |turn: u32, build: String| {
            let dynamo = state.dynamo.clone();
            async move {
                am::update_status(&dynamo, "settings", team, |cur| {
                    am::take_turn(cur, &build, turn, now)
                })
                .await
                .unwrap()
            }
        };
        assert!(matches!(
            take(0, st.build.clone()).await,
            am::Updated::Written(_)
        ));
        assert!(matches!(
            take(0, st.build.clone()).await,
            am::Updated::Kept(_)
        ));
        assert!(matches!(
            take(0, "another".into()).await,
            am::Updated::Kept(_)
        ));
        // Still running: still refused.
        assert_eq!(press(&state, team).await.0, StatusCode::CONFLICT);
        // Once it has finished, a new build may start.
        am::update_status(&state.dynamo, "settings", team, |cur| {
            am::finished(cur, &st.build, 2, "", now)
        })
        .await
        .unwrap();
        assert_eq!(press(&state, team).await.0, StatusCode::ACCEPTED);
        assert_eq!(f.store.sent().len(), 2);
        // Another team's build is its own.
        assert_eq!(press(&state, "TEAM#library").await.0, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn presses_at_the_same_moment_still_start_one_build() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#harbor";
        let presses = (0..6).map(|_| press(&state, team));
        let codes: Vec<StatusCode> = futures::future::join_all(presses)
            .await
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        assert_eq!(
            codes.iter().filter(|c| **c == StatusCode::ACCEPTED).count(),
            1,
            "{codes:?}"
        );
        assert_eq!(f.store.sent().len(), 1);
    }

    #[tokio::test]
    async fn a_release_queues_one_build_for_a_map_another_extractor_made() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#harbor";
        let now = chrono::Utc::now().timestamp();
        let made_by = |extractor: &'static str| {
            let dynamo = state.dynamo.clone();
            async move {
                am::update_status(&dynamo, "settings", team, |_| {
                    Some(am::Status {
                        state: "ready".into(),
                        at: now - 60,
                        built: now - 86_400,
                        built_repos: 2,
                        extractor: extractor.into(),
                        ..Default::default()
                    })
                })
                .await
                .unwrap();
            }
        };
        // Made by this release's extractor: nothing is queued.
        made_by(am::EXTRACTOR_VERSION).await;
        assert!(queue_release_build(&state, team).await.is_none());
        assert!(f.store.sent().is_empty());
        // Made by another: six page reads at the same moment queue one build.
        made_by("0000000000000000").await;
        let reads = (0..6).map(|_| queue_release_build(&state, team));
        let queued = futures::future::join_all(reads)
            .await
            .into_iter()
            .flatten()
            .count();
        assert_eq!(queued, 1);
        assert_eq!(f.store.sent().len(), 1);
        let st = am::status(&state.dynamo, "settings", team).await.unwrap();
        assert_eq!(st.state, "queued");
        assert_eq!(st.asked_extractor, am::EXTRACTOR_VERSION);
        // The map there now is still the one shown, marked as updating.
        let v = status_view(
            Some(&st),
            MapFacts {
                has_map: true,
                written_at: now - 86_400,
                repos: 2,
            },
            0,
            now,
        );
        assert_eq!(
            (v["state"].as_str(), v["reason"].as_str()),
            (Some("ready"), Some("release"))
        );
        // A later read while it runs, and after it failed: none more.
        assert!(queue_release_build(&state, team).await.is_none());
        am::update_status(&state.dynamo, "settings", team, |cur| {
            am::failed(cur, &st.build, "x", now)
        })
        .await
        .unwrap();
        assert!(queue_release_build(&state, team).await.is_none());
        assert_eq!(f.store.sent().len(), 1);
    }

    #[test]
    fn the_status_says_when_it_started_how_long_it_takes_and_when_the_map_was_built() {
        let now = 50_000;
        let q = am::queue_next(None, now - 120, "b1").unwrap();
        let running = am::take_turn(Some(&q), "b1", 0, now - 100).unwrap();
        let running = am::progress(
            Some(&running),
            "b1",
            Some("parts"),
            "Working out the app's parts",
            now - 10,
        )
        .unwrap();
        let v = status_view(Some(&running), MapFacts::default(), 12, now);
        assert_eq!(
            v,
            json!({
                "state": "building", "updating": true, "step": "Working out the app's parts", "stage": "parts",
                "last_build": "building", "note": "", "started_at": now - 120, "repos": 12,
                "estimate": {"low_secs": 15 * 60, "high_secs": 30 * 60},
                "built_at": 0, "built_repos": 0, "now": now, "floorplan": am::FLOORPLAN_VERSION.trim(),
                "reason": "", "unread": [], "journeys_note": "",
            })
        );
        // Built: when, and from how many repositories; nothing running.
        let done = am::finished(Some(&running), "b1", 12, "", now - 5).unwrap();
        let facts = MapFacts {
            has_map: true,
            written_at: now - 6,
            repos: 12,
        };
        let v = status_view(Some(&done), facts, 0, now);
        assert_eq!(
            (v["state"].as_str(), v["updating"].as_bool()),
            (Some("ready"), Some(false))
        );
        assert_eq!(
            (v["built_at"].as_i64(), v["built_repos"].as_u64()),
            (Some(now - 5), Some(12))
        );
        assert_eq!(
            (v["started_at"].as_i64(), v["estimate"].is_null()),
            (Some(0), true)
        );
        // A repository the journeys couldn't read: one plain line, gone
        // once it can be read again.
        let mut waiting = done.clone();
        waiting.unread = vec!["harbor/harbor-etl".into()];
        let v = status_view(Some(&waiting), facts, 0, now);
        assert_eq!(v["state"], "ready");
        let line = v["journeys_note"].as_str().unwrap();
        assert!(
            line.starts_with("harbor-etl couldn't be read from GitHub just now"),
            "{line}"
        );
        assert!(line.contains("every other journey goes on"));
        waiting.unread.clear();
        assert_eq!(
            status_view(Some(&waiting), facts, 0, now)["journeys_note"],
            ""
        );
        // A map from before the status kept these: the map's own date and repositories.
        let v = status_view(
            Some(&am::Status {
                state: "ready".into(),
                at: 1,
                ..Default::default()
            }),
            facts,
            0,
            now,
        );
        assert_eq!(
            (v["built_at"].as_i64(), v["built_repos"].as_u64()),
            (Some(now - 6), Some(12))
        );
        // Failed: the reason in plain words.
        let f = am::failed(
            Some(&running),
            "b1",
            "GitHub didn't answer for harbor-api.",
            now,
        )
        .unwrap();
        let v = status_view(Some(&f), MapFacts::default(), 0, now);
        assert_eq!(
            (v["state"].as_str(), v["note"].as_str()),
            (Some("failed"), Some("GitHub didn't answer for harbor-api."))
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
        assert_eq!(why_of("# Proposal\n\n## Why\nPatrons can't book a slot twice.\nIt double charges.\n\n## What\nx"), "Patrons can't book a slot twice. It double charges.");
        // Placed on the map, in Floorplan's own Health field.
        let g = am::with_overlays(harbor(), None, Some(&[it]));
        assert!(g["health"]["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["source"] == "coderhelm"
                && i["items"] == json!(["f:harbor-api/src/bookings.js"])));
    }

    /// A made-up map the size of a large team's: `files` files in 12 parts
    /// across 9 repositories, three lines per file.
    fn big_map(files: usize) -> Value {
        let repos: Vec<String> = (0..9).map(|i| format!("bookshop/yard{i}")).collect();
        let folders = am::folders(&repos);
        let fs: Vec<Value> = (0..files)
            .map(|i| {
                let r = &repos[i % 9];
                json!({"p": format!("{}/src/area{}/booth_{i}_keeper.ts", folders[r], i % 40), "n": format!("part{}", i % 12), "r": folders[r], "l": "typescript", "loc": 120, "name": format!("Booth keeper {i}"), "d": "Keeps one booth's bookings, its gate code and the lockers that may use it, and tells the office when it changes."})
            })
            .collect();
        let links: Vec<Value> = (0..files * 3)
            .map(|i| json!({"a": format!("f:{}", fs[i % files]["p"].as_str().unwrap()), "b": format!("f:{}", fs[(i * 7 + 13) % files]["p"].as_str().unwrap()), "k": "import", "w": 1}))
            .collect();
        let mut g = json!({
            "schema": am::GRAPH_SCHEMA, "project": {"name": "Bookshop", "summary": "Booths and bookings."},
            "meta": {"root": "/tmp/x", "agent": "claude"},
            "nodes": (0..12).map(|i| json!({"id": format!("part{i}"), "parent": null, "label": format!("Wing area {i}"), "kind": "service"})).collect::<Vec<_>>(),
            "files": fs, "links": links, "routes": [], "flows": [], "tour": [], "screens": [],
            "health": {"issues": [{"id": "open-endpoints:x", "severity": "high", "title": "Open", "items": [], "locations": [{"file": "yard0/src/area0/booth_0_keeper.ts", "line": 3}]}]},
        });
        am::stamp_repos(&mut g, &folders);
        g
    }

    async fn read_map(state: &Arc<AppState>, team: &str) -> Value {
        let q = MapQuery {
            recent: Some(1),
            health: Some(1),
            ..Default::default()
        };
        let Json(body) = get_map(State(state.clone()), Extension(admin(team)), Query(q))
            .await
            .unwrap();
        body
    }

    /// The page gets where to read the map and a small payload of overlays,
    /// never the map: the copy and the index are read from the bucket (and
    /// written once for a map built before they existed), and the copy
    /// itself comes from the bucket, or through the API as stored.
    #[tokio::test]
    async fn the_page_gets_an_address_for_the_map_and_small_overlays() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#harbor";
        // No map yet: no address.
        let first = read_map(&state, "TEAM#library").await;
        assert!(first["graph_url"].is_null() && first["graph"].is_null());

        // A map built before the page's copy and its index existed.
        f.store.put_object(
            "bucket",
            &am::graph_key(team),
            am::gzip(harbor().to_string().as_bytes()),
        );
        let body = read_map(&state, team).await;
        assert_eq!(body["status"]["state"], "ready");
        assert!(body["graph"].is_null(), "the map itself is never sent");
        let url = body["graph_url"].as_str().unwrap();
        assert!(
            url.contains("/bucket/app-map/TEAM_harbor/view.json.gz?")
                && url.contains("X-Amz-Signature=")
                && url.contains(&format!("X-Amz-Expires={VIEW_URL_SECS}")),
            "{url}"
        );
        assert_eq!(body["graph_fallback"], VIEW_FALLBACK);
        assert!(body["graph_at"].as_i64().unwrap() > 0);
        // Written once, as the browser reads it.
        let view = f.store.object("bucket", &am::view_key(team)).unwrap();
        assert_eq!(view.content_type, "application/json");
        assert_eq!(view.content_encoding, "gzip");
        let copy: Value =
            serde_json::from_slice(&am::gunzip(&view.body, am::MAX_GRAPH_BYTES).unwrap()).unwrap();
        assert!(copy["meta"]["root"].is_null());
        assert_eq!(copy["meta"]["agent"], "none");
        assert!(f.store.object("bucket", &am::index_key(team)).is_some());
        // Overlays: no runs, Floorplan's own checks counted.
        assert!(body["overlays"]["recent"].is_null());
        let summary = &body["overlays"]["health"]["summary"];
        assert_eq!(summary, &copy["health"]["summary"]);
        // Read again: the copy is not written again.
        let at = view.at;
        read_map(&state, team).await;
        assert_eq!(
            f.store.object("bucket", &am::view_key(team)).unwrap().at,
            at
        );

        // Through the API: the stored bytes, for the browser to unzip.
        let r = graph(State(state.clone()), Extension(admin(team)))
            .await
            .unwrap();
        assert_eq!(r.headers()["content-encoding"], "gzip");
        assert_eq!(r.headers()["content-type"], "application/json");
        let bytes = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(bytes.to_vec(), view.body);
        // Another team's copy is not this team's.
        assert!(
            graph(State(state.clone()), Extension(admin("TEAM#library")))
                .await
                .is_err()
        );
    }

    /// A page's copy made by an earlier release (its System cards still under
    /// their old names) is written again from the stored map on the next
    /// read, once: the cards are titled by their repositories without a new
    /// build.
    #[tokio::test]
    async fn an_earlier_releases_copy_is_named_by_repository_without_a_build() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#harbor";
        let g = harbor();
        f.store.put_object(
            "bucket",
            &am::graph_key(team),
            am::gzip(g.to_string().as_bytes()),
        );
        let old_view = am::for_viewer(g.clone(), "then");
        f.store.put_object(
            "bucket",
            &am::view_key(team),
            am::gzip(old_view.to_string().as_bytes()),
        );
        let old_index = am::MapIndex {
            copy: 0,
            ..am::MapIndex::of(&g)
        };
        f.store.put_object(
            "bucket",
            &am::index_key(team),
            am::gzip(&serde_json::to_vec(&old_index).unwrap()),
        );
        let body = read_map(&state, team).await;
        assert!(body["graph_url"].is_string());
        let view = f.store.object("bucket", &am::view_key(team)).unwrap();
        let copy: Value =
            serde_json::from_slice(&am::gunzip(&view.body, am::MAX_GRAPH_BYTES).unwrap()).unwrap();
        let api = copy["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == "api")
            .unwrap();
        assert_eq!(api["label"], "harbor-api");
        assert_eq!(api["role"], "Booking service");
        let (idx, _) = am::load_index(&state.s3, "bucket", team).await.unwrap();
        assert_eq!(idx.copy, am::COPY_VERSION);
        // Read again: not written again.
        read_map(&state, team).await;
        assert_eq!(
            f.store.object("bucket", &am::view_key(team)).unwrap().at,
            view.at
        );
        // The page's lookups name the part the same way.
        let q = MapQuery {
            id: "api".into(),
            ..Default::default()
        };
        let Json(p) = part(State(state.clone()), Extension(admin(team)), Query(q))
            .await
            .unwrap();
        assert_eq!(p["part"]["label"], "harbor-api");
        assert_eq!(p["part"]["role"], "Booking service");
    }

    /// A page view at a large team's map size, through the real handler: the
    /// answer is small and quick, however large the map.
    #[tokio::test]
    async fn a_page_view_of_a_large_map_is_small_and_quick() {
        let f = crate::fake_aws::start().await;
        let state = crate::fake_aws::state(&f.url);
        let team = "TEAM#bookshop";
        let map = am::gzip(big_map(4_000).to_string().as_bytes());
        let map_len = map.len();
        f.store.put_object("bucket", &am::graph_key(team), map);
        let am::MapIndex { .. } = am::store_derived(
            &state.s3,
            "bucket",
            team,
            &am::load_graph(&state.s3, "bucket", team).await.unwrap(),
        )
        .await
        .unwrap();
        let t = std::time::Instant::now();
        let body = read_map(&state, team).await;
        let took = t.elapsed();
        let size = body.to_string().len();
        let view = f.store.object("bucket", &am::view_key(team)).unwrap();
        eprintln!(
            "app map page view, 4,000 files: {took:?}, {size} bytes answered; the map is {map_len} bytes gzipped, the page's copy {} bytes gzipped",
            view.body.len()
        );
        assert!(body["graph_url"].is_string());
        assert!(size < 1_000_000, "{size} bytes");
        // Debug build, every call through the local stand-in.
        assert!(took.as_millis() < 1_000, "{took:?}");
    }
}
