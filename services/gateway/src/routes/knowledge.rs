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

/// Searches the team's connected Bedrock knowledge bases, as `actor` in the
/// team's AWS records.
pub(crate) fn retriever<'a>(state: &'a AppState, actor: &'a str) -> kb::remote::Bedrock<'a> {
    kb::remote::Bedrock {
        sts: &state.sts,
        dynamo: &state.dynamo,
        accounts_table: &state.config.aws_insights_table_name,
        actor,
    }
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
    /// On create only: the base's first connected source, so a base is made
    /// with what it holds in one step.
    #[serde(default)]
    pub source: Option<super::knowledge_sources::SourceInput>,
    /// On create only: a knowledge base to connect in place instead (it is
    /// searched live where it is and holds no files here).
    #[serde(default)]
    pub connect: Option<ConnectInput>,
}

/// A knowledge base to connect in place.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ConnectInput {
    /// `bedrock_kb`.
    #[serde(default)]
    pub provider: String,
    /// The Bedrock knowledge base's ARN.
    #[serde(default)]
    pub target: String,
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
        "source_id": f.source_id,
        "url": common::knowledge::answer::original_url(&f.url),
        "path": f.path,
    })
}

fn base_json(
    b: &Base,
    files: &[FileMeta],
    sources: &[common::knowledge::sources::Source],
) -> Value {
    let now = chrono::Utc::now();
    let count = |s: FileStatus| files.iter().filter(|f| f.shown_status(now).0 == s).count();
    let secs = now.timestamp();
    json!({
        "sources": sources.iter().map(|s| json!({
            "id": s.id,
            "kind": s.kind_key(),
            "name": s.name,
            "status": common::knowledge::sources::shown_status(s, secs),
            "paused": s.paused,
            "items": s.items,
            "last_success_at": s.last_success_at,
        })).collect::<Vec<_>>(),
        "kinds": super::knowledge_sources::kinds(files, sources),
        "uploads": files.iter().filter(|f| !f.is_synced()).count(),
        "synced": files.iter().filter(|f| f.is_synced()).count(),
        "id": b.id,
        "name": b.name,
        "description": b.description,
        "read_role": b.read_role,
        "attach_team": b.attach_team,
        "agents": b.agents,
        "authoritative": b.authoritative,
        "created_at": b.created_at,
        "updated_at": b.updated_at,
        "backend": backend_json(b),
        "files": files.len(),
        "ready": count(FileStatus::Ready),
        "processing": count(FileStatus::Processing) + count(FileStatus::Uploading),
        "failed": count(FileStatus::Failed),
    })
}

/// Where a base's passages are, for the page: `local`, or connected in
/// place with where it lives and how it is reached (never a private ID).
fn backend_json(b: &Base) -> Value {
    match b.remote() {
        None => json!({ "kind": "local" }),
        Some(r) => json!({
            "kind": "remote",
            "provider": r.provider,
            "label": r.provider.label(),
            "target": r.target,
            "region": r.region,
            "account": r.target.split(':').nth(4).unwrap_or(""),
            "access": match &r.access {
                kb::Access::Install => "install",
                kb::Access::Account { .. } => "account",
            },
            "hybrid": r.hybrid,
        }),
    }
}

/// Files and synced apps belong to bases kept here: a base connected in
/// place has none.
fn refuse_connected(base: &Base) -> Result<(), (StatusCode, Json<Value>)> {
    match base.remote() {
        Some(r) => Err(fail(
            StatusCode::BAD_REQUEST,
            &format!(
                "This knowledge base is connected in place: it is searched live in your AWS account, so nothing can be added to it here. Add documents to the {} itself.",
                r.provider.label()
            ),
        )),
        None => Ok(()),
    }
}

/// Refuses adding to `kb_id` when it is connected in place.
pub(crate) async fn refuse_if_connected(
    state: &AppState,
    team_id: &str,
    kb_id: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    match get_base_record(state, team_id, kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
    {
        Some(b) => refuse_connected(&b),
        None => Ok(()),
    }
}

/// A connection failure as the page shows it: what happened, what to do,
/// and (when the role only lacks the permission) the one-click update that
/// grants it.
fn connect_fail(r: &kb::remote::Refusal, grant: Option<Value>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": r.message,
            "fix": r.fix,
            "code": r.code,
            "grant": grant,
        })),
    )
}

/// The connected AWS accounts of the team (none when the install keeps no
/// such table).
async fn aws_accounts(
    state: &AppState,
    team_id: &str,
) -> Vec<common::live::aws_account::AwsAccount> {
    let table = &state.config.aws_insights_table_name;
    if table.is_empty() {
        return Vec::new();
    }
    common::live::aws_account::list(&state.dynamo, table, team_id)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "Could not read AWS accounts");
            Vec::new()
        })
}

/// Checks a knowledge base to connect, with one test search, and returns
/// how it is reached. Nothing is saved.
async fn check_connect(
    state: &AppState,
    claims: &Claims,
    existing: &[Base],
    input: &ConnectInput,
) -> Result<kb::Remote, (StatusCode, Json<Value>)> {
    if !input.provider.is_empty() && input.provider != "bedrock_kb" {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Only a Bedrock knowledge base can be connected in place.",
        ));
    }
    let arn =
        kb::remote::parse_kb_arn(&input.target).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    if let Some(same) = existing
        .iter()
        .find(|b| b.remote().is_some_and(|r| r.target == arn.arn()))
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("That knowledge base is already connected as {}.", same.name),
        ));
    }
    let accounts = aws_accounts(state, &claims.team_id).await;
    let own = common::live::aws::install_read_role();
    let access = kb::remote::access_for(&arn, own.as_deref(), &accounts)
        .map_err(|r| connect_fail(&r, None))?;
    let mut remote = kb::remote::bedrock_remote(&arn, access, true);
    remote.hybrid = test_remote(state, claims, existing, &accounts, &remote).await?;
    Ok(remote)
}

/// One test search of `remote` as the team: `Ok(hybrid)`, or what went
/// wrong in plain words (with the one-click grant when the team's role only
/// lacks the permission).
async fn test_remote(
    state: &AppState,
    claims: &Claims,
    existing: &[Base],
    accounts: &[common::live::aws_account::AwsAccount],
    remote: &kb::Remote,
) -> Result<bool, (StatusCode, Json<Value>)> {
    let actor = format!("knowledge-{}", claims.email);
    let session = kb::remote::session(
        &state.sts,
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
        remote,
        &actor,
    )
    .await
    .map_err(|r| connect_fail(&r, None))?;
    kb::remote::test_retrieve(&session, remote).await.map_err(|r| {
        // The team's role only lacks the permission: the update that lets
        // it search this knowledge base (and the ones already connected in
        // that account), in one click.
        let grant = match (&remote.access, r.code) {
            (kb::Access::Account { account }, "access") => {
                let mut kbs = kb::remote::arns_in(existing, account);
                kbs.push(remote.target.clone());
                kbs.sort();
                kbs.dedup();
                let region = accounts
                    .iter()
                    .find(|a| &a.account == account)
                    .map(|a| a.region.clone())
                    .unwrap_or_else(|| remote.region.clone());
                super::connectors::update_stack_url(&region, &kbs).map(|url| {
                    json!({
                        "url": url,
                        "stack_name": common::log_reader::update_stack_name_for(&kbs),
                        "account": account,
                    })
                })
            }
            _ => None,
        };
        info!(team_id = %claims.team_id, code = r.code, "A connected knowledge base's test search failed");
        connect_fail(&r, grant)
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
        let sources = super::knowledge_sources::sources_of(&state, &claims.team_id, &b.id).await;
        out.push(base_json(b, &files, &sources));
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
    let sources = super::knowledge_sources::sources_of(&state, &claims.team_id, &base.id).await;
    let now = chrono::Utc::now().timestamp();
    Ok(Json(json!({
        "base": base_json(&base, &files, &sources),
        "files": files.iter().map(file_json).collect::<Vec<_>>(),
        "sources": sources.iter().map(|s| super::knowledge_sources::source_json(s, now)).collect::<Vec<_>>(),
        "visibility_note": super::knowledge_sources::VISIBILITY_NOTE,
        "can_manage": claims.holder().holds("knowledge_manage"),
        "limits": limits(),
    })))
}

// ── Managing (admins) ───────────────────────────────────────────────────────

/// GET /api/knowledge/bedrock — what connecting a Bedrock knowledge base
/// starts from: on a self-hosted install, the knowledge bases in its own
/// account (or why they can't be listed); everywhere, the team's connected
/// AWS accounts, whose roles can search a pasted ARN.
pub async fn bedrock_bases(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let own = common::live::aws::install_read_role();
    let region = std::env::var("AWS_REGION").unwrap_or_default();
    let (bases, list_error) = if own.is_some() && !region.is_empty() {
        match kb::remote::list_install_bases(&region).await {
            Ok(b) => (Some(b), None),
            Err(r) => (
                None,
                Some(json!({ "message": r.message, "fix": r.fix, "code": r.code })),
            ),
        }
    } else {
        (None, None)
    };
    let connected: Vec<String> = store(&state)
        .bases(&claims.team_id)
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|b| b.remote().map(|r| r.target.clone()))
        .collect();
    let accounts: Vec<Value> = aws_accounts(&state, &claims.team_id)
        .await
        .iter()
        .map(|a| json!({ "account": a.account, "region": a.region }))
        .collect();
    Ok(Json(json!({
        "install": own.is_some(),
        "install_account": own.as_deref().and_then(|r| r.split(':').nth(4)).unwrap_or(""),
        "region": region,
        "bases": bases,
        "list_error": list_error,
        "accounts": accounts,
        "connected": connected,
    })))
}

/// POST /api/knowledge/:kb_id/check — one test search of a connected base,
/// now: it works, or what is wrong and what to do.
pub async fn check_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let existing = store(&state)
        .bases(&claims.team_id)
        .await
        .map_err(|e| internal("knowledge bases", e))?;
    let base = existing
        .iter()
        .find(|b| b.id == kb_id)
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    let remote = base.remote().ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "This knowledge base is kept here: there is no connection to check.",
        )
    })?;
    let accounts = aws_accounts(&state, &claims.team_id).await;
    let hybrid = test_remote(&state, &claims, &existing, &accounts, remote).await?;
    Ok(Json(
        json!({ "ok": true, "hybrid": hybrid, "checked_at": now() }),
    ))
}

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
    if let Some(c) = input
        .connect
        .as_ref()
        .filter(|c| !c.target.trim().is_empty())
    {
        if input.source.as_ref().is_some_and(|s| !s.kind.is_empty()) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "A knowledge base connected in place can't also sync an app.",
            ));
        }
        // One test search first: nothing is saved unless it works.
        base.backend = kb::Backend::Remote(check_connect(&state, &claims, &existing, c).await?);
    }
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(base_record(&claims.team_id, &base)))
        .send()
        .await
        .map_err(|e| internal("create knowledge base", e.to_string()))?;
    info!(team_id = %claims.team_id, kb = %base.id, connected = base.remote().is_some(), "Knowledge base created");
    let mut added = Value::Null;
    if let Some(src) = input.source.as_ref().filter(|s| !s.kind.is_empty()) {
        match super::knowledge_sources::add_source(&state, &claims, &base.id, src).await {
            Ok(v) => added = v,
            Err(e) => {
                // Nothing half made: the new base goes again.
                let _ = delete_record(&state, &claims.team_id, &kb::base_sk(&base.id)).await;
                return Err(e);
            }
        }
    }
    let sources = super::knowledge_sources::sources_of(&state, &claims.team_id, &base.id).await;
    Ok(Json(json!({
        "base": base_json(&base, &[], &sources),
        "source": added.get("source").cloned().unwrap_or(Value::Null),
        "setup": added.get("setup").cloned().unwrap_or(Value::Null),
    })))
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
    let sources = super::knowledge_sources::sources_of(&state, &claims.team_id, &base.id).await;
    Ok(Json(json!({ "base": base_json(&base, &files, &sources) })))
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
    for src in super::knowledge_sources::sources_of(&state, &claims.team_id, &base.id).await {
        super::knowledge_sources::remove_source(&state, &claims.team_id, &src)
            .await
            .map_err(|e| internal("delete knowledge source", e))?;
    }
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
    refuse_connected(&base)?;
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
                .iter()
                .filter(|f| !f.is_synced())
                .count();
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
    if file.is_synced() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This page is kept in sync with its app. Untick it in the source, or remove the source.",
        ));
    }
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

    #[test]
    fn a_connected_base_shows_where_it_lives_and_takes_no_files() {
        let remote = kb::remote::bedrock_remote(
            &kb::remote::parse_kb_arn(
                "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345",
            )
            .unwrap(),
            kb::Access::Account {
                account: "111122223333".into(),
            },
            false,
        );
        let b = Base {
            id: "kb".into(),
            name: "Runbooks".into(),
            backend: kb::Backend::Remote(remote),
            ..Default::default()
        };
        let v = base_json(&b, &[], &[]);
        assert_eq!(v["backend"]["kind"], "remote");
        assert_eq!(v["backend"]["label"], "Bedrock knowledge base");
        assert_eq!(v["backend"]["region"], "eu-west-1");
        assert_eq!(v["backend"]["account"], "111122223333");
        assert_eq!(v["backend"]["access"], "account");
        assert_eq!(v["backend"]["hybrid"], false);
        assert_eq!(v["files"], 0);
        let (code, body) = refuse_connected(&b).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("connected in place"));
        // A base kept here says so and takes files.
        let local = Base::default();
        assert_eq!(base_json(&local, &[], &[])["backend"]["kind"], "local");
        assert!(refuse_connected(&local).is_ok());
    }

    #[test]
    fn a_failed_connection_says_what_happened_what_to_do_and_offers_the_grant() {
        let r = kb::remote::refusal("AccessDeniedException", "not authorized to perform: bedrock:Retrieve because no identity-based policy allows the bedrock:Retrieve action");
        let (code, body) = connect_fail(
            &r,
            Some(json!({ "url": "https://console.aws.amazon.com/x", "stack_name": "s" })),
        );
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "The role in your account does not allow Retrieve on this knowledge base."
        );
        assert_eq!(body["code"], "access");
        assert!(body["fix"].as_str().unwrap().contains("bedrock:Retrieve"));
        assert_eq!(body["grant"]["stack_name"], "s");
        // The connect input reads as the dashboard sends it.
        let input: BaseInput = serde_json::from_value(json!({
            "name": "Runbooks", "connect": { "provider": "bedrock_kb", "target": "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345" }
        }))
        .unwrap();
        assert_eq!(input.connect.unwrap().provider, "bedrock_kb");
    }
}
