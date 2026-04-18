use std::path::PathBuf;
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

use crate::AppState;
use crate::models::Claims;

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
    mut db: MenteDb,
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
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Tar finish: {e}")))?
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
    let (mut db, dir) =
        open_db(&state.s3, &state.config.bucket_name, &claims.team_id, owner, name).await?;

    let ids = db.memory_ids();

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
            items.push(MemoryItem {
                id: id.to_string(),
                content: node.content.clone(),
                memory_type: memory_type_str(&node.memory_type).to_string(),
                tags: node.tags.clone(),
                confidence: node.confidence,
                created_at: node.created_at,
                accessed_at: node.accessed_at,
                access_count: node.access_count,
            });
        }
    }

    // Sort by created_at descending (newest first)
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let filtered_total = items.len();
    let page = q.page.max(1);
    let page_size = q.page_size.min(100).max(1);
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
    let (owner, name) = parse_repo(&q.repo)?;
    let (mut db, dir) =
        open_db(&state.s3, &state.config.bucket_name, &claims.team_id, owner, name).await?;

    let id = memory_id
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid memory ID".into()))?;

    let _ = db.forget(id);
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
    let (mut db, dir) =
        open_db(&state.s3, &state.config.bucket_name, &claims.team_id, owner, name).await?;

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
