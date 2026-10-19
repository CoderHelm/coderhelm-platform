use std::collections::HashMap;
use std::path::PathBuf;

use aws_sdk_dynamodb::types::AttributeValue;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use mentedb::MenteDb;
use mentedb_core::memory::MemoryType;
use serde::{Deserialize, Serialize};
use tar::{Archive, Builder};
use tracing::info;

use crate::models::Claims;
use crate::AppState;

const MEMORY_PREFIX: &str = "teams";
const MEMORY_SUFFIX: &str = "memory.tar.gz";

fn memory_s3_key(team_id: &str, repo_owner: &str, repo_name: &str) -> String {
    format!("{team_id}/{MEMORY_PREFIX}/{repo_owner}/{repo_name}/{MEMORY_SUFFIX}")
}

fn local_dir(team_id: &str, repo_owner: &str, repo_name: &str) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/mentedb-gw/{team_id}/{repo_owner}/{repo_name}"
    ))
}

#[derive(Serialize)]
struct MemoryItem {
    id: String,
    content: String,
    memory_type: String,
    tags: Vec<String>,
    confidence: f32,
    created_at: u64,
    accessed_at: u64,
    access_count: u32,
    /// What it is about (a tag from `memory_meta::TOPICS`) and its label.
    topic: String,
    topic_label: String,
    /// Reaches the agents on every ticket.
    pinned: bool,
    /// Where it came from: a run (and its ticket) or a person.
    origin: common::memory_meta::Origin,
    /// How many tickets it was handed to, and the last one.
    uses: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used: Option<LastUse>,
}

#[derive(Serialize, Clone, Default)]
struct LastUse {
    at: String,
    run_id: String,
    title: String,
}

fn item_from(
    id: String,
    node: &mentedb_core::memory::MemoryNode,
    usage: &HashMap<String, (u64, LastUse)>,
) -> MemoryItem {
    let meta = common::memory_meta::parse(&node.tags);
    let (uses, last) = usage
        .get(&id)
        .cloned()
        .map(|(n, l)| (n, Some(l)))
        .unwrap_or((0, None));
    MemoryItem {
        content: node.content.clone(),
        memory_type: memory_type_str(&node.memory_type).to_string(),
        tags: node.tags.clone(),
        confidence: node.confidence,
        created_at: node.created_at,
        accessed_at: node.accessed_at,
        access_count: node.access_count,
        topic_label: common::memory_meta::topic_label(&meta.topic).to_string(),
        topic: meta.topic,
        pinned: meta.pinned,
        origin: meta.origin,
        uses,
        last_used: last,
        id,
    }
}

/// How often each memory of a repo was handed to an agent (written by the
/// worker on every recall).
async fn load_usage(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> HashMap<String, (u64, LastUse)> {
    let prefix = common::memory_meta::usage_prefix(owner, name);
    let mut out = HashMap::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.into()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.clone()))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(r) = r else { break };
        for i in r.items() {
            let s = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            let id = s("sk").strip_prefix(&prefix).unwrap_or("").to_string();
            let uses = i
                .get("uses")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            out.insert(
                id,
                (
                    uses,
                    LastUse {
                        at: s("last_used_at"),
                        run_id: s("last_run_id"),
                        title: s("last_title"),
                    },
                ),
            );
        }
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    out
}

#[derive(Serialize)]
struct MemoryListResponse {
    memories: Vec<MemoryItem>,
    total: usize,
    page: usize,
    page_size: usize,
}

#[derive(Deserialize)]
pub struct ListQuery {
    repo: String, // "owner/name"
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_page_size")]
    page_size: usize,
    #[serde(default)]
    search: Option<String>,
    #[serde(default)]
    memory_type: Option<String>,
    /// `run` or `person`.
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    pinned: Option<bool>,
    /// `newest` (default) or `most_used`.
    #[serde(default)]
    sort: Option<String>,
}

fn default_page() -> usize {
    1
}
fn default_page_size() -> usize {
    50
}

fn memory_type_str(t: &MemoryType) -> &'static str {
    match t {
        MemoryType::Episodic => "episodic",
        MemoryType::Semantic => "semantic",
        MemoryType::Procedural => "procedural",
        MemoryType::AntiPattern => "anti_pattern",
        MemoryType::Correction => "correction",
        MemoryType::Reasoning => "reasoning",
    }
}

fn parse_repo(repo: &str) -> Result<(&str, &str), (StatusCode, String)> {
    repo.split_once('/')
        .ok_or((StatusCode::BAD_REQUEST, "repo must be owner/name".into()))
}

/// Open MenteDB from S3 snapshot (read-only, no lock needed for browsing).
async fn open_db(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
) -> Result<(MenteDb, PathBuf), (StatusCode, String)> {
    let key = memory_s3_key(team_id, repo_owner, repo_name);
    let dir = local_dir(team_id, repo_owner, repo_name);

    // Clean up and recreate
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create temp dir: {e}"),
        )
    })?;

    // Download from S3
    match s3.get_object().bucket(bucket).key(&key).send().await {
        Ok(response) => {
            let body = response
                .body
                .collect()
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("S3 read error: {e}"),
                    )
                })?
                .into_bytes();
            let decoder = GzDecoder::new(body.as_ref());
            let mut archive = Archive::new(decoder);
            archive.unpack(&dir).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unpack error: {e}"),
                )
            })?;
        }
        Err(e) => {
            let is_not_found = e
                .as_service_error()
                .map(|se| se.is_no_such_key())
                .unwrap_or(false)
                || format!("{e}").contains("NoSuchKey")
                || format!("{e}").contains("404");
            if is_not_found {
                // No memories yet — return empty DB
                let db = MenteDb::open(&dir).map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("MenteDB open error: {e}"),
                    )
                })?;
                return Ok((db, dir));
            }
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 download error: {e}"),
            ));
        }
    }

    let db = MenteDb::open(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("MenteDB open error: {e}"),
        )
    })?;
    Ok((db, dir))
}

fn cleanup(dir: &PathBuf) {
    if dir.exists() {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Upload modified DB back to S3 (used after delete).
async fn upload_and_close(
    db: MenteDb,
    dir: &PathBuf,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
) -> Result<(), (StatusCode, String)> {
    db.flush()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Flush: {e}")))?;
    db.close()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Close: {e}")))?;

    let key = memory_s3_key(team_id, repo_owner, repo_name);
    let mut buf = Vec::new();
    {
        let encoder = GzEncoder::new(&mut buf, Compression::fast());
        let mut tar = Builder::new(encoder);
        tar.append_dir_all(".", dir)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Tar: {e}")))?;
        tar.into_inner()
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Tar finish: {e}"),
                )
            })?
            .finish()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Gz finish: {e}")))?;
    }

    s3.put_object()
        .bucket(bucket)
        .key(&key)
        .body(buf.into())
        .content_type("application/gzip")
        .send()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("S3 upload: {e}")))?;

    cleanup(dir);
    Ok(())
}

/// GET /api/memories?repo=owner/name&page=1&page_size=50&search=...&memory_type=...
pub async fn list_memories(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (owner, name) = parse_repo(&q.repo)?;
    let (db, dir) = open_db(
        &state.s3,
        &state.config.bucket_name,
        &claims.team_id,
        owner,
        name,
    )
    .await?;

    let ids = db.memory_ids();
    let usage = load_usage(&state, &claims.team_id, owner, name).await;

    // Collect all memories, apply filters
    let mut items: Vec<MemoryItem> = Vec::new();
    for id in ids {
        if let Ok(node) = db.get_memory(id) {
            // Type filter
            if let Some(ref type_filter) = q.memory_type {
                if memory_type_str(&node.memory_type) != type_filter.as_str() {
                    continue;
                }
            }
            // Text search filter
            if let Some(ref search) = q.search {
                let s = search.to_lowercase();
                let content_match = node.content.to_lowercase().contains(&s);
                let tag_match = node.tags.iter().any(|t| t.to_lowercase().contains(&s));
                if !content_match && !tag_match {
                    continue;
                }
            }
            let item = item_from(id.to_string(), &node, &usage);
            if q.source.as_deref().is_some_and(|s| s != item.origin.source)
                || q.topic.as_deref().is_some_and(|t| t != item.topic)
                || q.pinned.is_some_and(|p| p != item.pinned)
            {
                continue;
            }
            items.push(item);
        }
    }

    if q.sort.as_deref() == Some("most_used") {
        items.sort_by_key(|item| {
            (
                std::cmp::Reverse(item.uses),
                std::cmp::Reverse(item.created_at),
            )
        });
    } else {
        items.sort_by_key(|item| std::cmp::Reverse(item.created_at));
    }

    let filtered_total = items.len();
    let page = q.page.max(1);
    let page_size = q.page_size.clamp(1, 100);
    let start = (page - 1) * page_size;
    let paged: Vec<MemoryItem> = items.into_iter().skip(start).take(page_size).collect();

    let _ = db.close();
    cleanup(&dir);

    Ok(Json(MemoryListResponse {
        memories: paged,
        total: filtered_total,
        page,
        page_size,
    }))
}

/// DELETE /api/memories/:memory_id?repo=owner/name
#[derive(Deserialize)]
pub struct DeleteQuery {
    repo: String,
}

pub async fn delete_memory(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(memory_id): Path<String>,
    Query(q): Query<DeleteQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if claims.require_role(3).is_err() {
        return Err((StatusCode::FORBIDDEN, "Admin+ required".into()));
    }
    let (owner, name) = parse_repo(&q.repo)?;

    // Take the same fencing lock worker runs hold. An unlocked delete raced a
    // concurrent run's close_and_upload: the run's snapshot (still containing
    // the memory) overwrote the delete, silently resurrecting it.
    let lock_token = match common::memlock::acquire_lock(
        &state.dynamo,
        &state.config.table_name,
        &claims.team_id,
        owner,
        name,
    )
    .await
    {
        Ok(Some(token)) => token,
        Ok(None) => {
            return Err((
                StatusCode::CONFLICT,
                "A run is currently using this repo's memory — try again in a few minutes".into(),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Memory lock error: {e}"),
            ))
        }
    };

    // From here on, release the lock AND clean /tmp on EVERY path.
    let result: Result<_, (StatusCode, String)> = async {
        let (db, dir) = open_db(
            &state.s3,
            &state.config.bucket_name,
            &claims.team_id,
            owner,
            name,
        )
        .await?;

        let id = memory_id
            .parse()
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid memory ID".to_string()))?;

        // Propagate a real forget error instead of reporting a false "deleted"
        // — otherwise the memory survives, the snapshot re-uploads unchanged,
        // and the worker keeps injecting a memory the admin believes gone (the
        // silent-resurrection this whole lock change exists to prevent).
        db.forget(id).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Delete failed: {e}"),
            )
        })?;
        info!(memory_id = %memory_id, "Deleted memory via dashboard");

        upload_and_close(
            db,
            &dir,
            &state.s3,
            &state.config.bucket_name,
            &claims.team_id,
            owner,
            name,
        )
        .await?;
        Ok(())
    }
    .await;

    common::memlock::release_lock(
        &state.dynamo,
        &state.config.table_name,
        &claims.team_id,
        owner,
        name,
        Some(&lock_token),
    )
    .await;

    // Belt-and-suspenders /tmp cleanup: upload_and_close cleans on success;
    // this covers the error paths (bad ID, flush/upload failure) so unpacked
    // snapshots can't accumulate and exhaust a warm container's 512MB /tmp.
    cleanup(&local_dir(&claims.team_id, owner, name));

    result?;
    Ok(Json(serde_json::json!({"deleted": memory_id})))
}

/// GET /api/memories/stats?repo=owner/name
#[derive(Deserialize)]
pub struct StatsQuery {
    repo: String,
}

#[derive(Serialize)]
struct MemoryStats {
    total: usize,
    by_type: std::collections::HashMap<String, usize>,
}

pub async fn memory_stats(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<StatsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (owner, name) = parse_repo(&q.repo)?;
    let (db, dir) = open_db(
        &state.s3,
        &state.config.bucket_name,
        &claims.team_id,
        owner,
        name,
    )
    .await?;

    let ids = db.memory_ids();
    let total = ids.len();
    let mut by_type = std::collections::HashMap::new();

    for id in ids {
        if let Ok(node) = db.get_memory(id) {
            *by_type
                .entry(memory_type_str(&node.memory_type).to_string())
                .or_insert(0) += 1;
        }
    }

    let _ = db.close();
    cleanup(&dir);

    Ok(Json(MemoryStats { total, by_type }))
}

// ── Teaching, editing and pinning ───────────────────────────────────────────

/// Longest memory a person can write: memories are short, single facts.
const MAX_MEMORY_CHARS: usize = 1_000;

async fn embed(state: &AppState, text: &str) -> Result<Vec<f32>, (StatusCode, String)> {
    let payload = serde_json::json!({ "inputText": text, "dimensions": 1024, "normalize": true });
    let out = state
        .bedrock
        .invoke_model()
        .model_id("amazon.titan-embed-text-v2:0")
        .content_type("application/json")
        .body(aws_smithy_types::Blob::new(
            serde_json::to_vec(&payload).unwrap_or_default(),
        ))
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Couldn't read the memory's meaning: {e}"),
            )
        })?;
    let body: serde_json::Value = serde_json::from_slice(out.body().as_ref())
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Embedding response: {e}")))?;
    let v: Vec<f32> = body["embedding"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_f64().map(|f| f as f32))
                .collect()
        })
        .unwrap_or_default();
    if v.is_empty() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "The embedding came back empty".into(),
        ));
    }
    Ok(v)
}

fn type_for_topic(topic: &str) -> MemoryType {
    match common::memory_meta::topic_tag(topic) {
        "procedure" => MemoryType::Procedural,
        "anti-pattern" => MemoryType::AntiPattern,
        "correction" => MemoryType::Correction,
        _ => MemoryType::Semantic,
    }
}

fn clean_content(raw: &str) -> Result<String, (StatusCode, String)> {
    let c = raw.trim();
    if c.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Write what Coderhelm should remember.".into(),
        ));
    }
    if c.chars().count() > MAX_MEMORY_CHARS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Keep a memory under {MAX_MEMORY_CHARS} characters: one fact or rule per memory."
            ),
        ));
    }
    Ok(c.to_string())
}

/// Runs `f` on the repo's memory with the same lock worker runs hold, uploads
/// the result, and always releases the lock and cleans up.
async fn with_locked_memory<T, F, Fut>(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    f: F,
) -> Result<T, (StatusCode, String)>
where
    F: FnOnce(MenteDb) -> Fut,
    Fut: std::future::Future<Output = Result<(MenteDb, T), (StatusCode, String)>>,
{
    let token = match common::memlock::acquire_lock(
        &state.dynamo,
        &state.config.table_name,
        team_id,
        owner,
        name,
    )
    .await
    {
        Ok(Some(t)) => t,
        Ok(None) => {
            return Err((
                StatusCode::CONFLICT,
                "A run is using this repo's memory right now. Try again in a few minutes.".into(),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Memory lock error: {e}"),
            ))
        }
    };
    let result = async {
        let (db, dir) = open_db(&state.s3, &state.config.bucket_name, team_id, owner, name).await?;
        let (db, out) = f(db).await?;
        upload_and_close(
            db,
            &dir,
            &state.s3,
            &state.config.bucket_name,
            team_id,
            owner,
            name,
        )
        .await?;
        Ok(out)
    }
    .await;
    common::memlock::release_lock(
        &state.dynamo,
        &state.config.table_name,
        team_id,
        owner,
        name,
        Some(&token),
    )
    .await;
    cleanup(&local_dir(team_id, owner, name));
    result
}

#[derive(Deserialize)]
pub struct CreateBody {
    repo: String,
    content: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    pinned: bool,
}

/// POST /api/memories  { repo, content, topic?, pinned? } — a person teaches
/// Coderhelm something about this repo.
pub async fn create_memory(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if claims.require_role(3).is_err() {
        return Err((
            StatusCode::FORBIDDEN,
            "Only admins can change what Coderhelm remembers.".into(),
        ));
    }
    let (owner, name) = parse_repo(&body.repo)?;
    let content = clean_content(&body.content)?;
    let embedding = embed(&state, &content).await?;
    let mut tags = common::memory_meta::person_tags(&body.topic, &claims.email, body.pinned);
    tags.push(format!("repo:{owner}/{name}"));
    let memory_type = type_for_topic(&body.topic);
    let node_content = content.clone();
    let id = with_locked_memory(&state, &claims.team_id, owner, name, move |db| async move {
        let mut node = mentedb_core::memory::MemoryNode::new(
            mentedb_core::types::AgentId::nil(),
            memory_type,
            node_content,
            embedding,
        );
        node.tags = tags;
        node.confidence = 1.0;
        let new_id = node.id;
        let outcome = db.store_with_outcome(node).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Save failed: {e}"),
            )
        })?;
        // A near-verbatim copy of an existing memory is not stored again;
        // hand back the memory that already says it, since `new_id` does not
        // exist.
        let id = match outcome {
            mentedb::StoreOutcome::Stored => new_id,
            mentedb::StoreOutcome::Duplicate { of } => of,
        };
        Ok((db, id.to_string()))
    })
    .await?;
    info!(memory_id = %id, "Memory added via dashboard");
    Ok(Json(serde_json::json!({ "id": id })))
}

#[derive(Deserialize)]
pub struct UpdateBody {
    repo: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    pinned: Option<bool>,
}

/// PUT /api/memories/:id  { repo, content?, topic?, pinned? } — edit, re-file
/// or pin a memory. Where it came from is kept; who edited it is recorded.
pub async fn update_memory(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(memory_id): Path<String>,
    Json(body): Json<UpdateBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if claims.require_role(3).is_err() {
        return Err((
            StatusCode::FORBIDDEN,
            "Only admins can change what Coderhelm remembers.".into(),
        ));
    }
    let (owner, name) = parse_repo(&body.repo)?;
    let id: mentedb_core::types::MemoryId = memory_id
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid memory ID".to_string()))?;
    let content = body.content.as_deref().map(clean_content).transpose()?;
    // Embed before taking the lock, so a slow embedding never holds it.
    let embedding = match &content {
        Some(c) => Some(embed(&state, c).await?),
        None => None,
    };
    let editor = claims.email.clone();
    let topic = body.topic.clone();
    let pinned = body.pinned;
    with_locked_memory(&state, &claims.team_id, owner, name, move |db| async move {
        let mut node = db.get_memory(id).map_err(|_| {
            (
                StatusCode::NOT_FOUND,
                "That memory no longer exists.".to_string(),
            )
        })?;
        if let (Some(c), Some(e)) = (content, embedding) {
            node.content = c;
            node.embedding = e;
        }
        if let Some(t) = topic.as_deref() {
            node.memory_type = type_for_topic(t);
        }
        node.tags = common::memory_meta::retag(&node.tags, topic.as_deref(), pinned, &editor);
        // MenteDB has no in-place update: forget, then store the same id again.
        db.forget(id).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Edit failed: {e}"),
            )
        })?;
        db.store(node).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Edit failed: {e}"),
            )
        })?;
        Ok((db, ()))
    })
    .await?;
    info!(memory_id = %memory_id, "Memory edited via dashboard");
    Ok(Json(serde_json::json!({ "updated": memory_id })))
}

// ── What a ticket would recall ──────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PreviewBody {
    repo: String,
    text: String,
}

/// POST /api/memories/preview  { repo, text } — the memories the planner and
/// coding agent would get for a ticket like this, the same way a run recalls
/// them: everything pinned, plus the five most relevant above the floor.
pub async fn preview_recall(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PreviewBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (owner, name) = parse_repo(&body.repo)?;
    let text = body.text.trim();
    if text.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Describe a ticket to see what Coderhelm would remember.".into(),
        ));
    }
    let text = common::truncate_str(text, 4_000).to_string();
    let embedding = embed(&state, &text).await?;
    let usage = load_usage(&state, &claims.team_id, owner, name).await;
    let (db, dir) = open_db(
        &state.s3,
        &state.config.bucket_name,
        &claims.team_id,
        owner,
        name,
    )
    .await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let mut pinned = Vec::new();
    for id in db.memory_ids() {
        if let Ok(node) = db.get_memory(id) {
            if node.tags.iter().any(|t| t == common::memory_meta::PINNED) {
                pinned.push(item_from(id.to_string(), &node, &usage));
            }
        }
    }
    let mut recalled = Vec::new();
    if let Ok(results) = db.recall_hybrid_at(&embedding, Some(&text), 5, now, None, None, None) {
        for (id, score) in results {
            // The same floor a run uses (see the worker's recall_context).
            if score < RECALL_FLOOR {
                continue;
            }
            if let Ok(node) = db.get_memory(id) {
                if node.tags.iter().any(|t| t == common::memory_meta::PINNED) {
                    continue;
                }
                recalled.push(serde_json::json!({ "memory": item_from(id.to_string(), &node, &usage), "score": score }));
            }
        }
    }
    let _ = db.close();
    cleanup(&dir);
    Ok(Json(
        serde_json::json!({ "pinned": pinned, "recalled": recalled }),
    ))
}

/// Matches the worker's MIN_RELEVANCE.
const RECALL_FLOOR: f32 = 0.02;

// ── Agent profiles ──────────────────────────────────────────────────────────

/// One line in "what it knows".
#[derive(Serialize, Debug, Clone, PartialEq)]
struct Knows {
    label: String,
    detail: String,
}

/// Something an agent learned, with where it came from.
#[derive(Serialize, Debug, Clone, PartialEq)]
struct Lesson {
    text: String,
    source: String,
    at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    link: Option<String>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
struct RecentWork {
    title: String,
    at: String,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    link: Option<String>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
struct Profile {
    id: &'static str,
    name: &'static str,
    role: &'static str,
    uses_repo_memory: bool,
    knows: Vec<Knows>,
    learned: Vec<Lesson>,
    activity_label: String,
    activity_count: usize,
    recent: Vec<RecentWork>,
}

fn s_of(i: &HashMap<String, AttributeValue>, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn within_days(at: &str, days: i64) -> bool {
    chrono::DateTime::parse_from_rfc3339(at)
        .map(|t| {
            chrono::Utc::now()
                .signed_duration_since(t.with_timezone(&chrono::Utc))
                .num_days()
                < days
        })
        .unwrap_or(false)
}

/// The facts the profiles are built from, gathered once.
#[derive(Default)]
struct Facts {
    repo: String,
    memories: usize,
    pinned: usize,
    from_people: usize,
    recent_learned: Vec<Lesson>,
    most_used: Vec<Lesson>,
    repo_rules: String,
    team_rules: String,
    sources: Vec<String>,
    reviews: Vec<HashMap<String, AttributeValue>>,
    runs: Vec<HashMap<String, AttributeValue>>,
}

const DAYS: i64 = 30;

fn rules_line(label: &str, text: &str) -> Option<Knows> {
    let t = text.trim();
    (!t.is_empty()).then(|| Knows {
        label: label.into(),
        detail: common::truncate_str(&t.split_whitespace().collect::<Vec<_>>().join(" "), 400)
            .to_string(),
    })
}

fn memory_line(f: &Facts) -> Knows {
    Knows {
        label: "Memories about this repo".into(),
        detail: if f.memories == 0 {
            "None yet. They are learned as it finishes tickets, or a person adds them.".into()
        } else {
            format!(
                "{} in all: {} pinned (always used), {} added by people. The five most relevant come along on each ticket.",
                f.memories, f.pinned, f.from_people
            )
        },
    }
}

fn sources_line(f: &Facts) -> Knows {
    Knows {
        label: "Systems it can look things up in".into(),
        detail: if f.sources.is_empty() {
            "None connected for this repo.".into()
        } else {
            f.sources.join(", ")
        },
    }
}

fn pr_link(repo: &str, pr: &str) -> Option<String> {
    (!pr.is_empty() && pr != "0").then(|| format!("https://github.com/{repo}/pull/{pr}"))
}

/// Pure: the four profiles from the gathered facts.
fn build_profiles(f: &Facts) -> Vec<Profile> {
    // Reviewer: rules people wrote and lessons from ratings.
    let mut lessons = Vec::new();
    let mut reviews_recent = 0;
    let mut approve = 0;
    let mut changes = 0;
    let mut recent_reviews = Vec::new();
    for r in &f.reviews {
        let at = s_of(r, "created_at");
        let pr = r
            .get("pr_number")
            .and_then(|v| v.as_n().ok().or(v.as_s().ok()))
            .cloned()
            .unwrap_or_default();
        let verdict = s_of(r, "verdict");
        if within_days(&at, DAYS) && verdict != "QUESTION" {
            reviews_recent += 1;
            match verdict.as_str() {
                "APPROVE" => approve += 1,
                "REQUEST_CHANGES" => changes += 1,
                _ => {}
            }
        }
        if recent_reviews.len() < 5 && verdict != "QUESTION" {
            recent_reviews.push(RecentWork {
                title: format!("PR #{pr}"),
                at: at.clone(),
                outcome: if verdict == "APPROVE" {
                    "Approved".into()
                } else {
                    "Asked for changes".into()
                },
                link: pr_link(&f.repo, &pr),
            });
        }
        for c in r
            .get("rating_comments")
            .and_then(|v| v.as_l().ok())
            .cloned()
            .unwrap_or_default()
        {
            let Ok(raw) = c.as_s() else { continue };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
                continue;
            };
            let text = v["text"].as_str().unwrap_or("").trim();
            if text.is_empty() || lessons.len() >= 8 {
                continue;
            }
            lessons.push(Lesson {
                text: common::truncate_str(text, 600).to_string(),
                source: format!("A rating on PR #{pr}"),
                at: v["at"].as_str().unwrap_or(&at).to_string(),
                link: pr_link(&f.repo, &pr),
            });
        }
    }
    let mut reviewer_knows: Vec<Knows> = [
        rules_line("Review rules for this repo", &f.repo_rules),
        rules_line("Review rules for every repo", &f.team_rules),
    ]
    .into_iter()
    .flatten()
    .collect();
    reviewer_knows.push(Knows {
        label: "Lessons from ratings".into(),
        detail: if lessons.is_empty() {
            "None yet. A 👎 or a note on a review becomes a lesson for the next ones.".into()
        } else {
            format!(
                "{} note{} people left on its reviews; it reads them before every review.",
                lessons.len(),
                if lessons.len() == 1 { "" } else { "s" }
            )
        },
    });
    reviewer_knows.push(Knows {
        label: "The repo's own instructions".into(),
        detail: "AGENTS.md, CLAUDE.md and similar files, read at the PR's commit on every review."
            .into(),
    });
    reviewer_knows.push(sources_line(f));

    // Planner and coding agent: the repo's memory.
    let mut planned = 0;
    let mut merged = 0;
    let mut fixes = 0;
    let mut recent_runs = Vec::new();
    let mut recent_fixes = Vec::new();
    for r in &f.runs {
        let at = s_of(r, "created_at");
        let status = s_of(r, "status");
        let title = s_of(r, "title");
        let pushes: u64 = r
            .get("auto_fix_pushes")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let link = Some(s_of(r, "pr_url")).filter(|u| !u.is_empty());
        if within_days(&at, DAYS) {
            planned += 1;
            if status == "merged" {
                merged += 1;
            }
            if pushes > 0 {
                fixes += 1;
            }
        }
        if recent_runs.len() < 5 {
            recent_runs.push(RecentWork {
                title: title.clone(),
                at: at.clone(),
                outcome: status.clone(),
                link: link.clone(),
            });
        }
        if pushes > 0 && recent_fixes.len() < 5 {
            recent_fixes.push(RecentWork {
                title,
                at,
                outcome: format!("{pushes} fix{} pushed", if pushes == 1 { "" } else { "es" }),
                link,
            });
        }
    }
    let coder_learned = if f.recent_learned.is_empty() {
        f.most_used.clone()
    } else {
        f.recent_learned.clone()
    };

    vec![
        Profile {
            id: "reviewer",
            name: "Reviewer",
            role: "Reviews pull requests, comments on the exact lines, and approves when nothing is left to change.",
            uses_repo_memory: false,
            knows: reviewer_knows,
            learned: lessons,
            activity_label: format!("reviews in the last {DAYS} days ({approve} approved, {changes} asked for changes)"),
            activity_count: reviews_recent,
            recent: recent_reviews,
        },
        Profile {
            id: "planner",
            name: "Planner",
            role: "Reads a ticket, works out the root cause and writes the plan the coding agent follows.",
            uses_repo_memory: true,
            knows: vec![memory_line(f), sources_line(f)],
            learned: f.most_used.clone(),
            activity_label: format!("tickets planned in the last {DAYS} days"),
            activity_count: planned,
            recent: recent_runs.clone(),
        },
        Profile {
            id: "coder",
            name: "Coding agent",
            role: "Writes the change, runs the checks and opens the pull request. After each ticket it saves what it learned.",
            uses_repo_memory: true,
            knows: vec![memory_line(f), sources_line(f)],
            learned: coder_learned,
            activity_label: format!("pull requests merged in the last {DAYS} days"),
            activity_count: merged,
            recent: recent_runs,
        },
        Profile {
            id: "feedback",
            name: "Feedback agent",
            role: "Answers review comments on Coderhelm's pull requests and fixes the CI it broke.",
            uses_repo_memory: false,
            knows: vec![
                Knows {
                    label: "The pull request's conversation".into(),
                    detail: "Every comment and review thread on the PR, and the CI results.".into(),
                },
                sources_line(f),
            ],
            learned: Vec::new(),
            activity_label: format!("pull requests it pushed fixes to in the last {DAYS} days"),
            activity_count: fixes,
            recent: recent_fixes,
        },
    ]
}

async fn query_prefix(
    state: &AppState,
    table: &str,
    pk: &str,
    prefix: &str,
    limit: i32,
) -> Vec<HashMap<String, AttributeValue>> {
    state
        .dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
        .expression_attribute_values(":p", AttributeValue::S(prefix.into()))
        .scan_index_forward(false)
        .limit(limit)
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default()
}

async fn instructions(state: &AppState, team_id: &str, sk: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.into()))
        .key("sk", AttributeValue::S(sk.into()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().map(|i| s_of(i, "instructions")))
        .unwrap_or_default()
}

/// GET /api/memories/agents?repo=owner/name — what each agent knows about
/// this repo, what it learned and from where, and what it did lately.
pub async fn agent_profiles(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<StatsQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (owner, name) = parse_repo(&q.repo)?;
    let repo = format!("{owner}/{name}");
    let team = claims.team_id.as_str();
    let settings = state.config.settings_table_name.clone();
    let usage = load_usage(&state, team, owner, name).await;

    let mut f = Facts {
        repo: repo.clone(),
        ..Default::default()
    };
    let (db, dir) = open_db(&state.s3, &state.config.bucket_name, team, owner, name).await?;
    let mut items = Vec::new();
    for id in db.memory_ids() {
        if let Ok(node) = db.get_memory(id) {
            items.push(item_from(id.to_string(), &node, &usage));
        }
    }
    let _ = db.close();
    cleanup(&dir);
    f.memories = items.len();
    f.pinned = items.iter().filter(|i| i.pinned).count();
    f.from_people = items.iter().filter(|i| i.origin.source == "person").count();
    let lesson = |i: &MemoryItem| Lesson {
        text: i.content.clone(),
        source: if i.origin.title.is_empty() {
            if i.origin.source == "person" {
                format!("Added by {}", i.origin.added_by)
            } else {
                "Learned on a ticket".into()
            }
        } else {
            format!("Learned on \"{}\"", i.origin.title)
        },
        at: chrono::DateTime::from_timestamp_micros(i.created_at as i64)
            .map(|d| d.to_rfc3339())
            .unwrap_or_default(),
        link: None,
    };
    let mut by_new: Vec<&MemoryItem> = items.iter().filter(|i| i.origin.source == "run").collect();
    by_new.sort_by_key(|i| std::cmp::Reverse(i.created_at));
    f.recent_learned = by_new.iter().take(5).map(|i| lesson(i)).collect();
    let mut by_use: Vec<&MemoryItem> = items.iter().filter(|i| i.uses > 0).collect();
    by_use.sort_by_key(|i| std::cmp::Reverse(i.uses));
    f.most_used = by_use
        .iter()
        .take(5)
        .map(|i| {
            let mut l = lesson(i);
            l.source = format!(
                "{} · used on {} ticket{}",
                l.source,
                i.uses,
                if i.uses == 1 { "" } else { "s" }
            );
            l
        })
        .collect();

    f.repo_rules = instructions(&state, team, &format!("REVIEW_CONFIG#REPO#{repo}")).await;
    f.team_rules = instructions(&state, team, "REVIEW_CONFIG#GLOBAL").await;
    f.reviews = query_prefix(&state, &settings, team, &format!("REVIEW#{repo}#"), 200).await;
    f.runs = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .expression_attribute_values(":tr", AttributeValue::S(format!("{team}#{repo}")))
        .scan_index_forward(false)
        .limit(200)
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    f.sources = common::live::store::list_sources(&state.dynamo, &settings, team)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.usable_for(&repo))
        .map(|s| s.name)
        .collect();

    Ok(Json(
        serde_json::json!({ "repo": repo, "profiles": build_profiles(&f) }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    fn facts() -> Facts {
        let now = chrono::Utc::now().to_rfc3339();
        let rating = serde_json::json!({"at": now, "rating": "down", "text": "Ask for a flow test, not one per function."}).to_string();
        Facts {
            repo: "acme/web".into(),
            memories: 3,
            pinned: 1,
            from_people: 1,
            repo_rules: "  Prefer small PRs.\n  Flag missing migrations. ".into(),
            sources: vec!["Production database".into()],
            reviews: vec![
                HashMap::from([
                    ("created_at".into(), s(&now)),
                    ("verdict".into(), s("APPROVE")),
                    ("pr_number".into(), AttributeValue::N("12".into())),
                    (
                        "rating_comments".into(),
                        AttributeValue::L(vec![s(&rating)]),
                    ),
                ]),
                HashMap::from([
                    ("created_at".into(), s(&now)),
                    ("verdict".into(), s("REQUEST_CHANGES")),
                    ("pr_number".into(), AttributeValue::N("13".into())),
                ]),
            ],
            runs: vec![HashMap::from([
                ("created_at".into(), s(&now)),
                ("status".into(), s("merged")),
                ("title".into(), s("Fix login")),
                ("auto_fix_pushes".into(), AttributeValue::N("2".into())),
                ("pr_url".into(), s("https://github.com/acme/web/pull/14")),
            ])],
            ..Default::default()
        }
    }

    #[test]
    fn the_four_profiles_say_what_each_agent_really_uses() {
        let p = build_profiles(&facts());
        let ids: Vec<&str> = p.iter().map(|x| x.id).collect();
        assert_eq!(ids, vec!["reviewer", "planner", "coder", "feedback"]);
        // Only the planner and the coding agent read the repo's memory.
        assert_eq!(
            p.iter()
                .filter(|x| x.uses_repo_memory)
                .map(|x| x.id)
                .collect::<Vec<_>>(),
            vec!["planner", "coder"]
        );
        let reviewer = &p[0];
        assert_eq!(reviewer.activity_count, 2);
        assert!(reviewer
            .activity_label
            .contains("1 approved, 1 asked for changes"));
        assert_eq!(
            reviewer.knows[0],
            Knows {
                label: "Review rules for this repo".into(),
                detail: "Prefer small PRs. Flag missing migrations.".into()
            }
        );
        assert_eq!(reviewer.learned.len(), 1);
        assert_eq!(
            reviewer.learned[0].link.as_deref(),
            Some("https://github.com/acme/web/pull/12")
        );
        let coder = &p[2];
        assert_eq!(coder.activity_count, 1);
        assert!(coder.knows[0].detail.contains("1 pinned"));
        let feedback = &p[3];
        assert_eq!(feedback.activity_count, 1);
        assert_eq!(feedback.recent[0].outcome, "2 fixes pushed");
        assert_eq!(p[1].knows[1].detail, "Production database");
    }

    #[test]
    fn empty_facts_give_honest_empty_profiles() {
        let p = build_profiles(&Facts {
            repo: "acme/web".into(),
            ..Default::default()
        });
        assert!(p[1].knows[0].detail.starts_with("None yet."));
        assert_eq!(p[1].knows[1].detail, "None connected for this repo.");
        assert!(p[0].learned.is_empty());
        assert!(p.iter().all(|x| x.activity_count == 0));
    }

    #[test]
    fn written_memories_are_checked() {
        assert!(clean_content("   ").is_err());
        assert!(clean_content(&"a".repeat(MAX_MEMORY_CHARS + 1)).is_err());
        assert_eq!(
            clean_content("  Use pnpm, not npm. ").unwrap(),
            "Use pnpm, not npm."
        );
        assert!(matches!(
            type_for_topic("anti-pattern"),
            MemoryType::AntiPattern
        ));
        assert!(matches!(type_for_topic("whatever"), MemoryType::Semantic));
    }

    #[test]
    fn an_edit_keeps_the_same_memory_id() {
        // The edit path relies on forget-then-store with the same id.
        let dir = std::env::temp_dir().join(format!("mentedb-edit-{}", ulid::Ulid::new()));
        let db = MenteDb::open(&dir).unwrap();
        let mut node = mentedb_core::memory::MemoryNode::new(
            mentedb_core::types::AgentId::nil(),
            MemoryType::Semantic,
            "Use npm.".into(),
            vec![0.1; 1024],
        );
        node.tags = common::memory_meta::person_tags("procedure", "a@example.com", false);
        let id = node.id;
        db.store(node).unwrap();
        let mut edited = db.get_memory(id).unwrap();
        edited.content = "Use pnpm, not npm.".into();
        edited.tags = common::memory_meta::retag(&edited.tags, None, Some(true), "b@example.com");
        db.forget(id).unwrap();
        db.store(edited).unwrap();
        let back = db.get_memory(id).unwrap();
        assert_eq!(back.content, "Use pnpm, not npm.");
        assert!(common::memory_meta::parse(&back.tags).pinned);
        assert_eq!(db.memory_ids().len(), 1);
        let _ = db.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
