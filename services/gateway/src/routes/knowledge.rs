//! Knowledge bases: a team's uploaded documents, searched by the brain chat
//! and by the agents a base is attached to.
//!
//! Everyone sees the bases their role may read; only admins create, change
//! and delete bases and their files. A file is uploaded straight from the
//! browser to the team's folder in the (KMS-encrypted) data bucket with a
//! short-lived signed URL bound to the file's size and type; once the upload
//! is in, the worker reads, splits and embeds it (`worker::passes::knowledge`)
//! and the file shows processing, ready, or failed with the reason. Formats,
//! limits, keys and search live in `common::knowledge`.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::knowledge::extract::{Kind, EXTENSIONS};
use common::knowledge::search::Store;
use common::knowledge::{
    self as kb, base_record, file_record, read_data, Base, FileMeta, FileStatus, AGENTS, MAX_BASES,
    MAX_FILES_PER_BASE, MAX_FILE_BYTES, READ_ROLES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong. Try again.",
    )
}

fn forbidden(_: StatusCode) -> (StatusCode, Json<Value>) {
    fail(StatusCode::FORBIDDEN, "Only admins manage knowledge bases.")
}

pub(crate) fn store(state: &AppState) -> kb::search::AwsStore {
    kb::search::AwsStore {
        dynamo: state.dynamo.clone(),
        s3: state.s3.clone(),
        table: state.config.settings_table_name.clone(),
        bucket: state.config.bucket_name.clone(),
    }
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// The role a person reads knowledge bases as, from the capabilities they
/// hold (built-in or custom): managing bases reads everything, asking the
/// brain reads what members may, anything else what their role allows.
pub(crate) fn reader_role(holder: &crate::permissions::Holder) -> &'static str {
    if holder.holds("knowledge_manage") {
        "admin"
    } else if holder.holds("brain_ask") {
        "member"
    } else {
        holder.floor()
    }
}

// ── Input checks ────────────────────────────────────────────────────────────

/// What a person may set on a base.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BaseInput {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub read_role: Option<String>,
    #[serde(default)]
    pub attach_team: bool,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub authoritative: bool,
}

/// `input` applied to `base`, or why it can't be.
pub fn apply_input(base: &mut Base, input: &BaseInput) -> Result<(), String> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err("Give the knowledge base a name.".into());
    }
    if name.chars().count() > 80 {
        return Err("Keep the name under 80 characters.".into());
    }
    if input.description.chars().count() > 1_000 {
        return Err("Keep the description under 1,000 characters.".into());
    }
    let role = input
        .read_role
        .clone()
        .unwrap_or_else(|| base.read_role.clone());
    let role = if role.is_empty() {
        "member".into()
    } else {
        role
    };
    if !READ_ROLES.contains(&role.as_str()) {
        return Err("Pick who can read it: viewers, members or admins.".into());
    }
    let mut agents: Vec<String> = Vec::new();
    for a in &input.agents {
        if !AGENTS.iter().any(|(k, _)| k == a) {
            return Err(format!(
                "{a} isn't an agent a knowledge base can be attached to."
            ));
        }
        if !agents.contains(a) {
            agents.push(a.clone());
        }
    }
    base.name = name.to_string();
    base.description = input.description.trim().to_string();
    base.read_role = role;
    base.attach_team = input.attach_team;
    base.agents = agents;
    base.authoritative = input.authoritative;
    Ok(())
}

/// The kind of an upload, or why it is refused.
pub fn check_upload(name: &str, size: u64, max: u64) -> Result<Kind, String> {
    let kind = Kind::of(name).ok_or_else(|| {
        format!(
            "This kind of file isn't supported. Upload one of: {}.",
            EXTENSIONS.join(", ")
        )
    })?;
    if size == 0 {
        return Err("The file is empty.".into());
    }
    if size > max {
        return Err(format!(
            "The file is larger than {} MB.",
            max / (1024 * 1024)
        ));
    }
    Ok(kind)
}

// ── Reading ─────────────────────────────────────────────────────────────────

async fn get_base_record(
    state: &AppState,
    team_id: &str,
    kb_id: &str,
) -> Result<Option<Base>, String> {
    if !kb::valid_id(kb_id) {
        return Ok(None);
    }
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(kb::base_sk(kb_id)))
        .send()
        .await
        .map_err(|e| format!("base: {e}"))?;
    Ok(r.item().and_then(read_data::<Base>))
}

async fn get_file_record(
    state: &AppState,
    team_id: &str,
    kb_id: &str,
    file_id: &str,
) -> Result<Option<FileMeta>, String> {
    if !kb::valid_id(kb_id) || !kb::valid_id(file_id) {
        return Ok(None);
    }
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(kb::file_sk(kb_id, file_id)))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("file: {e}"))?;
    Ok(r.item().and_then(read_data::<FileMeta>))
}

/// A file as the page shows it.
fn file_json(f: &FileMeta) -> Value {
    let (status, error) = f.shown_status(chrono::Utc::now());
    json!({
        "id": f.id,
        "name": f.name,
        "kind": f.kind,
        "size": f.size,
        "status": status,
        "error": error,
        "version": f.version,
        "searchable": f.ready_version > 0,
        "as_of": f.as_of,
        "uploaded_at": f.uploaded_at,
        "uploaded_by": f.uploaded_by,
        "chunks": f.chunks,
        "truncated": f.truncated,
    })
}

fn base_json(b: &Base, files: &[FileMeta]) -> Value {
    let now = chrono::Utc::now();
    let count = |s: FileStatus| files.iter().filter(|f| f.shown_status(now).0 == s).count();
    json!({
        "id": b.id,
        "name": b.name,
        "description": b.description,
        "read_role": b.read_role,
        "attach_team": b.attach_team,
        "agents": b.agents,
        "authoritative": b.authoritative,
        "created_at": b.created_at,
        "updated_at": b.updated_at,
        "files": files.len(),
        "ready": count(FileStatus::Ready),
        "processing": count(FileStatus::Processing) + count(FileStatus::Uploading),
        "failed": count(FileStatus::Failed),
    })
}

fn limits() -> Value {
    json!({
        "max_file_bytes": MAX_FILE_BYTES,
        "max_files": MAX_FILES_PER_BASE,
        "extensions": EXTENSIONS,
        "agents": AGENTS.iter().map(|(k, l)| json!({"key": k, "label": l})).collect::<Vec<_>>(),
        "read_roles": READ_ROLES,
    })
}

/// GET /api/knowledge — the bases the caller may read.
pub async fn list_bases(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = store(&state);
    let bases = st
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let mut out = Vec::new();
    let reader = reader_role(&claims.holder());
    for b in bases.iter().filter(|b| b.readable_by(reader)) {
        let files = st.files(&claims.team_id, &b.id).await.unwrap_or_default();
        out.push(base_json(b, &files));
    }
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(Json(json!({
        "bases": out,
        "can_manage": claims.holder().holds("knowledge_manage"),
        "limits": limits(),
    })))
}

/// GET /api/knowledge/:kb_id — one base and its files.
pub async fn get_base(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
) -> Reply {
    let base = get_base_record(&state, &claims.team_id, &kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .filter(|b| b.readable_by(reader_role(&claims.holder())))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    let mut files = store(&state)
        .files(&claims.team_id, &base.id)
        .await
        .map_err(|e| internal("knowledge files", e))?;
    files.sort_by(|a, b| b.uploaded_at.cmp(&a.uploaded_at));
    Ok(Json(json!({
        "base": base_json(&base, &files),
        "files": files.iter().map(file_json).collect::<Vec<_>>(),
        "can_manage": claims.holder().holds("knowledge_manage"),
        "limits": limits(),
    })))
}

// ── Managing (admins) ───────────────────────────────────────────────────────

/// POST /api/knowledge — create a base.
pub async fn create_base(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<BaseInput>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let existing = store(&state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    if existing.len() >= MAX_BASES {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("A team can have up to {MAX_BASES} knowledge bases."),
        ));
    }
    let mut base = Base {
        id: kb::new_id(),
        read_role: "member".into(),
        created_at: now(),
        created_by: claims.email.clone(),
        updated_at: now(),
        ..Default::default()
    };
    apply_input(&mut base, &input).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(base_record(&claims.team_id, &base)))
        .send()
        .await
        .map_err(|e| internal("create knowledge base", e.to_string()))?;
    info!(team_id = %claims.team_id, kb = %base.id, "Knowledge base created");
    Ok(Json(json!({ "base": base_json(&base, &[]) })))
}

/// PUT /api/knowledge/:kb_id — rename, re-describe, change who reads it and
/// which agents search it.
pub async fn update_base(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
    Json(input): Json<BaseInput>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let mut base = get_base_record(&state, &claims.team_id, &kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    apply_input(&mut base, &input).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    base.updated_at = now();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(base_record(&claims.team_id, &base)))
        .send()
        .await
        .map_err(|e| internal("update knowledge base", e.to_string()))?;
    let files = store(&state)
        .files(&claims.team_id, &base.id)
        .await
        .unwrap_or_default();
    Ok(Json(json!({ "base": base_json(&base, &files) })))
}

/// Deletes every object under `prefix` (inside the team's folder).
async fn delete_prefix(state: &AppState, team_id: &str, prefix: &str) -> Result<usize, String> {
    if !kb::owns_key(team_id, prefix) {
        return Err("not this team's folder".into());
    }
    let mut removed = 0;
    let mut token: Option<String> = None;
    loop {
        let list = state
            .s3
            .list_objects_v2()
            .bucket(&state.config.bucket_name)
            .prefix(prefix)
            .set_continuation_token(token.take())
            .send()
            .await
            .map_err(|e| format!("list: {e}"))?;
        for o in list.contents() {
            if let Some(k) = o.key() {
                state
                    .s3
                    .delete_object()
                    .bucket(&state.config.bucket_name)
                    .key(k)
                    .send()
                    .await
                    .map_err(|e| format!("delete: {e}"))?;
                removed += 1;
            }
        }
        match list.next_continuation_token() {
            Some(t) if list.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
            _ => break,
        }
    }
    Ok(removed)
}

async fn delete_record(state: &AppState, team_id: &str, sk: &str) -> Result<(), String> {
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("delete record: {e}"))
}

/// DELETE /api/knowledge/:kb_id — the base, its files and every passage.
pub async fn delete_base(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let base = get_base_record(&state, &claims.team_id, &kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    let files = store(&state)
        .files(&claims.team_id, &base.id)
        .await
        .map_err(|e| internal("knowledge files", e))?;
    let prefix = format!("{}{}/", kb::team_prefix(&claims.team_id), base.id);
    delete_prefix(&state, &claims.team_id, &prefix)
        .await
        .map_err(|e| internal("delete knowledge objects", e))?;
    for f in &files {
        delete_record(&state, &claims.team_id, &kb::file_sk(&base.id, &f.id))
            .await
            .map_err(|e| internal("delete knowledge file", e))?;
    }
    delete_record(&state, &claims.team_id, &kb::base_sk(&base.id))
        .await
        .map_err(|e| internal("delete knowledge base", e))?;
    info!(team_id = %claims.team_id, kb = %base.id, files = files.len(), "Knowledge base deleted");
    Ok(Json(json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct UploadBody {
    pub name: String,
    pub size: u64,
    /// Upload a new version of this file instead of a new file.
    #[serde(default)]
    pub replace: Option<String>,
}

/// A signed PUT for one object, bound to its size and type.
pub(crate) async fn presign_put(
    state: &AppState,
    key: &str,
    content_type: &str,
    size: u64,
) -> Result<Value, String> {
    let cfg =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(900))
            .map_err(|e| format!("presign config: {e}"))?;
    let req = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(key)
        .content_type(content_type)
        .content_length(size as i64)
        .presigned(cfg)
        .await
        .map_err(|e| format!("presign: {e}"))?;
    let headers: serde_json::Map<String, Value> = req
        .headers()
        .filter(|(k, _)| {
            !k.eq_ignore_ascii_case("content-length") && !k.eq_ignore_ascii_case("host")
        })
        .map(|(k, v)| (k.to_string(), json!(v)))
        .collect();
    Ok(json!({ "url": req.uri(), "method": "PUT", "headers": headers }))
}

/// POST /api/knowledge/:kb_id/files — start an upload (a new file or a new
/// version of one): the file's record and a signed upload URL.
pub async fn start_upload(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
    Json(body): Json<UploadBody>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let base = get_base_record(&state, &claims.team_id, &kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    let name = kb::clean_file_name(&body.name);
    let kind = check_upload(&name, body.size, MAX_FILE_BYTES)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let file = match body.replace.as_deref() {
        Some(file_id) => {
            let mut f = get_file_record(&state, &claims.team_id, &base.id, file_id)
                .await
                .map_err(|e| internal("knowledge file", e))?
                .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such file."))?;
            f.version += 1;
            f.name = name;
            f.kind = kind.key().into();
            f.size = body.size;
            f.status = FileStatus::Uploading;
            f.error.clear();
            f.uploaded_at = now();
            f.uploaded_by = claims.email.clone();
            f
        }
        None => {
            let count = store(&state)
                .files(&claims.team_id, &base.id)
                .await
                .map_err(|e| internal("knowledge files", e))?
                .len();
            if count >= MAX_FILES_PER_BASE {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    &format!("A knowledge base holds up to {MAX_FILES_PER_BASE} files."),
                ));
            }
            FileMeta {
                id: kb::new_id(),
                kb_id: base.id.clone(),
                name,
                kind: kind.key().into(),
                size: body.size,
                status: FileStatus::Uploading,
                version: 1,
                uploaded_at: now(),
                uploaded_by: claims.email.clone(),
                ..Default::default()
            }
        }
    };
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(file_record(&claims.team_id, &file)))
        .send()
        .await
        .map_err(|e| internal("knowledge file record", e.to_string()))?;
    let key = kb::source_key(&claims.team_id, &base.id, &file.id, file.version);
    let upload = presign_put(&state, &key, kind.content_type(), body.size)
        .await
        .map_err(|e| internal("knowledge upload url", e))?;
    Ok(Json(json!({ "file": file_json(&file), "upload": upload })))
}

/// POST /api/knowledge/:kb_id/files/:file_id/uploaded — the browser's upload
/// is in: check it and hand it to the worker.
pub async fn finish_upload(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, file_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let mut file = get_file_record(&state, &claims.team_id, &kb_id, &file_id)
        .await
        .map_err(|e| internal("knowledge file", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such file."))?;
    let key = kb::source_key(&claims.team_id, &kb_id, &file_id, file.version);
    let head = state
        .s3
        .head_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await;
    let size = match head {
        Ok(h) => h.content_length().unwrap_or(0) as u64,
        Err(_) => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "The upload didn't arrive. Try uploading the file again.",
            ))
        }
    };
    if size == 0 || size > MAX_FILE_BYTES {
        file.status = FileStatus::Failed;
        file.error = "The uploaded file is empty or larger than 25 MB.".into();
    } else {
        file.status = FileStatus::Processing;
        file.error.clear();
        file.size = size;
        file.processing_since = now();
    }
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(file_record(&claims.team_id, &file)))
        .send()
        .await
        .map_err(|e| internal("knowledge file record", e.to_string()))?;
    if file.status == FileStatus::Processing {
        if let Err(e) = start_ingest(&state, &claims.team_id, &file).await {
            warn!(error = %e, "Knowledge ingest not started");
            file.status = FileStatus::Failed;
            file.error = "Processing couldn't start. Upload the file again.".into();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.settings_table_name)
                .set_item(Some(file_record(&claims.team_id, &file)))
                .send()
                .await;
        }
    }
    Ok(Json(json!({ "file": file_json(&file) })))
}

/// Asks the worker to read the file (asynchronously: the answer is the
/// file's status, polled by the page).
async fn start_ingest(state: &AppState, team_id: &str, f: &FileMeta) -> Result<(), String> {
    let name = &state.config.worker_function_name;
    if name.is_empty() {
        return Err("no worker configured".into());
    }
    let payload = json!({ "knowledge_ingest": {
        "team_id": team_id, "kb_id": f.kb_id, "file_id": f.id, "version": f.version,
    }});
    state
        .lambda
        .invoke()
        .function_name(name)
        .invocation_type(aws_sdk_lambda::types::InvocationType::Event)
        .payload(aws_sdk_lambda::primitives::Blob::new(
            serde_json::to_vec(&payload).map_err(|e| e.to_string())?,
        ))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("worker invoke: {e}"))
}

/// Files whose index came from another embedding model (a model change):
/// read again from their stored upload with the current model. Search skips
/// them meanwhile; dimensions are never mixed.
pub(crate) async fn reembed(state: &AppState, team_id: &str, stale: &[kb::search::Stale]) {
    for s in stale {
        let Ok(Some(mut f)) = get_file_record(state, team_id, &s.kb_id, &s.file_id).await else {
            continue;
        };
        // A newer upload in progress re-indexes it anyway.
        if f.version != s.version || f.status == FileStatus::Processing {
            continue;
        }
        f.status = FileStatus::Processing;
        f.processing_since = now();
        let saved = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .set_item(Some(file_record(team_id, &f)))
            .send()
            .await;
        if saved.is_ok() {
            match start_ingest(state, team_id, &f).await {
                Ok(()) => info!(file = %f.id, "Knowledge file queued for re-embedding"),
                Err(e) => warn!(file = %f.id, error = %e, "Re-embedding not started"),
            }
        }
    }
}

/// DELETE /api/knowledge/:kb_id/files/:file_id — the file and its passages.
pub async fn delete_file(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, file_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let file = get_file_record(&state, &claims.team_id, &kb_id, &file_id)
        .await
        .map_err(|e| internal("knowledge file", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such file."))?;
    delete_prefix(
        &state,
        &claims.team_id,
        &kb::file_folder(&claims.team_id, &kb_id, &file.id),
    )
    .await
    .map_err(|e| internal("delete knowledge objects", e))?;
    delete_record(&state, &claims.team_id, &kb::file_sk(&kb_id, &file.id))
        .await
        .map_err(|e| internal("delete knowledge file", e))?;
    info!(team_id = %claims.team_id, kb = %kb_id, file = %file.id, "Knowledge file deleted");
    Ok(Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_read_by_the_capabilities_they_hold() {
        use crate::permissions::Holder;
        assert_eq!(reader_role(&Holder::builtin("viewer")), "viewer");
        assert_eq!(reader_role(&Holder::builtin("member")), "member");
        assert_eq!(reader_role(&Holder::builtin("admin")), "admin");
        assert_eq!(reader_role(&Holder::builtin("owner")), "admin");
        let caps = |c: &[&str]| c.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // A custom role holding only brain_read reads what viewers may.
        assert_eq!(
            reader_role(&Holder::custom(
                "r",
                Some(("Reader", &caps(&["brain_read"])))
            )),
            "viewer"
        );
        assert_eq!(
            reader_role(&Holder::custom("a", Some(("Asker", &caps(&["brain_ask"]))))),
            "member"
        );
        assert_eq!(
            reader_role(&Holder::custom(
                "k",
                Some(("Librarian", &caps(&["knowledge_manage"])))
            )),
            "admin"
        );
    }

    #[test]
    fn role_levels_match_the_permissions_table() {
        for r in crate::permissions::VALID_ROLES.iter().chain(&["stranger"]) {
            assert_eq!(kb::role_level(r), crate::permissions::role_level(r), "{r}");
        }
    }

    #[test]
    fn base_input_is_checked() {
        let mut b = Base {
            read_role: "member".into(),
            ..Default::default()
        };
        let ok = BaseInput {
            name: "  HR handbook ".into(),
            description: "Policies".into(),
            read_role: Some("viewer".into()),
            agents: vec!["planner".into(), "planner".into(), "reviewer".into()],
            authoritative: true,
            ..Default::default()
        };
        apply_input(&mut b, &ok).unwrap();
        assert_eq!(b.name, "HR handbook");
        assert_eq!(b.read_role, "viewer");
        assert_eq!(b.agents, ["planner", "reviewer"]);
        assert!(b.authoritative);
        let bad = |f: &dyn Fn(&mut BaseInput)| {
            let mut i = ok.clone();
            f(&mut i);
            apply_input(&mut b.clone(), &i).unwrap_err()
        };
        assert!(bad(&|i| i.name = " ".into()).contains("name"));
        assert!(bad(&|i| i.name = "x".repeat(81)).contains("80"));
        assert!(bad(&|i| i.read_role = Some("owner".into())).contains("read"));
        assert!(bad(&|i| i.agents = vec!["hacker".into()]).contains("hacker"));
    }

    #[test]
    fn uploads_are_limited_by_kind_and_size() {
        assert_eq!(
            check_upload("a.pdf", 10, MAX_FILE_BYTES).unwrap(),
            Kind::Pdf
        );
        assert!(check_upload("a.exe", 10, MAX_FILE_BYTES)
            .unwrap_err()
            .contains("supported"));
        assert!(check_upload("a.md", 0, MAX_FILE_BYTES)
            .unwrap_err()
            .contains("empty"));
        assert!(check_upload("a.md", MAX_FILE_BYTES + 1, MAX_FILE_BYTES)
            .unwrap_err()
            .contains("25 MB"));
    }
}
