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

/// The team's Connected apps servers, which connected search tools are
/// searched through.
pub(crate) fn servers(state: &AppState, team: &str) -> kb::remote_mcp::RuntimeServers {
    kb::remote_mcp::RuntimeServers {
        rt: super::mcp_servers::runtime(state, team),
    }
}

/// Searches the team's connected bases: Bedrock knowledge bases as `actor`
/// in the team's AWS records, search tools through `servers` as `person`
/// (the asker's email) where each person signs in.
pub(crate) fn retriever<'a>(
    state: &'a AppState,
    actor: &'a str,
    servers: &'a kb::remote_mcp::RuntimeServers,
    person: Option<&str>,
) -> kb::remote::Connected<'a> {
    kb::remote::Connected {
        bedrock: kb::remote::Bedrock {
            sts: &state.sts,
            dynamo: &state.dynamo,
            accounts_table: &state.config.aws_insights_table_name,
            actor,
        },
        mcp: kb::remote_mcp::McpSearch {
            servers,
            person: person.map(str::to_string),
        },
    }
}

/// The person asking, for named readers ([`kb::Base::open_to`]).
pub(crate) fn person(claims: &Claims) -> kb::Person {
    kb::Person::new(&claims.email, claims.session_role())
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

/// The bases a person reads in Chat: those their reading role reaches
/// ([`reader_role`]) and that are open to them by name. The one rule Chat
/// (`routes::brain`) and the MCP knowledge tools (`mcp_knowledge`) share, so
/// an app never reads a base its person couldn't read in Chat.
pub(crate) fn chat_readable(
    bases: Vec<Base>,
    holder: &crate::permissions::Holder,
    me: &kb::Person,
) -> Vec<Base> {
    let reader = reader_role(holder);
    bases
        .into_iter()
        .filter(|b| b.readable_by(reader) && b.open_to(me))
        .collect()
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
    /// Only these people and roles read it (`user:<email>`, `role:<key>`);
    /// left out, the readers stay as they are.
    #[serde(default)]
    pub readers: Option<Vec<String>>,
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
    /// `bedrock_kb` (the default) or `mcp`.
    #[serde(default)]
    pub provider: String,
    /// The Bedrock knowledge base's ARN, or the Connected apps server's id.
    #[serde(default)]
    pub target: String,
    /// A search tool's tool that searches (picked when empty).
    #[serde(default)]
    pub search_tool: String,
    /// Its tool that reads a result in full (picked when empty).
    #[serde(default)]
    pub fetch_tool: String,
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
    if let Some(r) = &input.readers {
        base.readers = kb::clean_readers(r)?;
    }
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
        "readers": b.readers,
        "agent_access_note": b.agent_access_note(),
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
        Some(r) if r.provider == kb::Provider::Mcp => json!({
            "kind": "remote",
            "provider": r.provider,
            "label": r.provider.label(),
            "target": r.target,
            "server_name": r.name,
            "access": if r.per_person { "per_person" } else { "team" },
            "per_person": r.per_person,
            "search_tool": r.search_tool,
            "fetch_tool": r.fetch_tool,
        }),
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
                kb::Access::Connection { .. } => "team",
            },
            "hybrid": r.hybrid,
        }),
    }
}

/// Files and synced apps belong to bases kept here: a base connected in
/// place has none.
fn refuse_connected(base: &Base) -> Result<(), (StatusCode, Json<Value>)> {
    match base.remote() {
        Some(r) if r.provider == kb::Provider::Mcp => Err(fail(
            StatusCode::BAD_REQUEST,
            "This knowledge base is connected in place: it is searched live through its search tool, so nothing can be added to it here. Add documents to the search tool itself.",
        )),
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
    match input.provider.as_str() {
        "" | "bedrock_kb" => {}
        "mcp" => return check_mcp(state, claims, existing, input).await,
        _ => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Connect a Bedrock knowledge base or a search tool (MCP) in place.",
            ))
        }
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

/// Checks a search tool to connect: its server is on the team's Connected
/// apps, a search tool is there, and one test search reads (as the admin
/// connecting it, with their own sign-in where the server signs each person
/// in). Nothing is saved.
async fn check_mcp(
    state: &AppState,
    claims: &Claims,
    existing: &[Base],
    input: &ConnectInput,
) -> Result<kb::Remote, (StatusCode, Json<Value>)> {
    let id = input.target.trim();
    if !kb::valid_id(id) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Pick the search tool's server from Connected apps.",
        ));
    }
    if let Some(same) = existing.iter().find(|b| {
        b.remote()
            .is_some_and(|r| r.provider == kb::Provider::Mcp && r.target == id)
    }) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("That search tool is already connected as {}.", same.name),
        ));
    }
    let servers = servers(state, &claims.team_id);
    let checked = kb::remote_mcp::check(
        &servers,
        id,
        &claims.email,
        &input.search_tool,
        &input.fetch_tool,
    )
    .await
    .map_err(|r| {
        info!(team_id = %claims.team_id, code = r.code, "A search tool's test search failed");
        connect_fail(&r, None)
    })?;
    Ok(kb::remote_mcp::mcp_remote(id, &checked))
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
    let r = match kb::remote::test_retrieve(&session, remote).await {
        Ok(hybrid) => return Ok(hybrid),
        Err(r) => r,
    };
    // The team's role only lacks the permission: the update that lets it
    // search this knowledge base (and keeps what the team already chose in
    // that account), in one click.
    let grant = match (&remote.access, r.code) {
        (kb::Access::Account { account }, "access") => {
            let region = accounts
                .iter()
                .find(|a| &a.account == account)
                .map(|a| a.region.clone())
                .unwrap_or_else(|| remote.region.clone());
            grant_for(
                state,
                &claims.team_id,
                existing,
                account,
                &region,
                Some(&remote.target),
                None,
            )
            .await
        }
        _ => None,
    };
    info!(team_id = %claims.team_id, code = r.code, "A connected knowledge base's test search failed");
    Err(connect_fail(&r, grant))
}

/// The one-click update that lets the team's role in `account` search the
/// knowledge bases the team connected there, read the buckets it imports
/// from and write the knowledge base it copies its knowledge into, plus
/// `add_kb` and `add_bucket` (what is being added now).
pub(crate) async fn grant_for(
    state: &AppState,
    team: &str,
    bases: &[Base],
    account: &str,
    region: &str,
    add_kb: Option<&str>,
    add_bucket: Option<&str>,
) -> Option<Value> {
    let mut kbs = kb::remote::arns_in(bases, account);
    kbs.extend(add_kb.map(str::to_string));
    kbs.sort();
    kbs.dedup();
    let sources =
        kb::sources::store::team_sources(&state.dynamo, &state.config.settings_table_name, team)
            .await
            .unwrap_or_default();
    let mut buckets = kb::sources::s3::buckets_in(&sources, account);
    buckets.extend(add_bucket.map(str::to_string));
    buckets.sort();
    buckets.dedup();
    // The knowledge base the team copies its knowledge into, when it is in
    // this account (kb::bedrock_sync): the same stack lets the role write it.
    let sync_cfg = kb::bedrock_sync::load(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .ok()
        .flatten();
    let sync = kb::bedrock_sync::sync_arns_in(sync_cfg.as_ref(), account);
    let chosen = common::log_reader::Chosen::new(&kbs, &buckets).with_sync(&sync);
    super::connectors::update_stack_url(region, &chosen).map(|url| {
        json!({
            "url": url,
            "stack_name": common::log_reader::update_stack_name_for(&chosen),
            "account": account,
        })
    })
}

fn limits() -> Value {
    json!({
        "max_file_bytes": MAX_FILE_BYTES,
        "max_files": MAX_FILES_PER_BASE,
        "max_items_per_import": kb::sources::MAX_ITEMS_PER_IMPORT,
        "max_items_per_source": kb::sources::MAX_ITEMS_PER_SOURCE,
        "max_archive_bytes": kb::sources::archive::MAX_ARCHIVE_BYTES,
        "max_readers": kb::MAX_READERS,
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
    let manages = claims.holder().holds("knowledge_manage");
    let me = person(&claims);
    for b in bases
        .iter()
        .filter(|b| b.readable_by(reader) && (manages || b.open_to(&me)))
    {
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
        .filter(|b| {
            b.readable_by(reader_role(&claims.holder()))
                && (claims.holder().holds("knowledge_manage") || b.open_to(&person(&claims)))
        })
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
    if remote.provider == kb::Provider::Mcp {
        let servers = servers(&state, &claims.team_id);
        kb::remote_mcp::check(
            &servers,
            &remote.target,
            &claims.email,
            &remote.search_tool,
            &remote.fetch_tool,
        )
        .await
        .map_err(|r| connect_fail(&r, None))?;
        return Ok(Json(json!({ "ok": true, "checked_at": now() })));
    }
    let accounts = aws_accounts(&state, &claims.team_id).await;
    let hybrid = test_remote(&state, &claims, &existing, &accounts, remote).await?;
    Ok(Json(
        json!({ "ok": true, "hybrid": hybrid, "checked_at": now() }),
    ))
}

/// GET /api/knowledge/search-tools — the servers on the team's Connected
/// apps a knowledge base can be connected to, each with the tools that look
/// like its search and fetch (from its last tool listing), and which are
/// connected already.
pub async fn search_tools(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    let list = rt
        .store
        .list_servers(&claims.team_id)
        .await
        .map_err(|e| internal("mcp servers", e))?;
    let mut servers = Vec::new();
    for s in &list {
        let owner = kb::remote_mcp::owner_for(
            s.sharing == common::mcp_remote::store::Sharing::Personal
                && s.auth == common::mcp_remote::store::AuthKind::OAuth,
            Some(&claims.email),
        );
        let connected = match (s.auth, owner) {
            (common::mcp_remote::store::AuthKind::OAuth, Some(o)) => matches!(
                rt.store.get_token(&claims.team_id, &s.id, &o).await,
                Ok(Some(t)) if t.status == common::mcp_remote::store::Status::Connected
            ),
            _ => s.status == common::mcp_remote::store::Status::Connected,
        };
        let mut v = serde_json::to_value(kb::remote_mcp::listed(s)).unwrap_or_default();
        v["connected"] = json!(connected);
        servers.push(v);
    }
    let connected: Vec<String> = store(&state)
        .bases(&claims.team_id)
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|b| b.remote())
        .filter(|r| r.provider == kb::Provider::Mcp)
        .map(|r| r.target.clone())
        .collect();
    Ok(Json(json!({ "servers": servers, "connected": connected })))
}

#[derive(Debug, Deserialize)]
pub struct ImportQuery {
    #[serde(default)]
    pub arn: String,
}

/// GET /api/knowledge/bedrock/import?arn= — where a Bedrock knowledge
/// base's documents are and how each of its data sources is imported (an
/// S3 data source becomes an S3 source here). Read with the same role that
/// would search it; nothing in Bedrock changes.
pub async fn bedrock_import(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(q): axum::extract::Query<ImportQuery>,
) -> Reply {
    claims.require_role(3).map_err(forbidden)?;
    let arn = kb::remote::parse_kb_arn(&q.arn).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let accounts = aws_accounts(&state, &claims.team_id).await;
    let own = common::live::aws::install_read_role();
    let access = kb::remote::access_for(&arn, own.as_deref(), &accounts)
        .map_err(|r| connect_fail(&r, None))?;
    let remote = kb::remote::bedrock_remote(&arn, access, true);
    let actor = format!("knowledge-{}", claims.email);
    let session = kb::remote::session(
        &state.sts,
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
        &remote,
        &actor,
    )
    .await
    .map_err(|r| connect_fail(&r, None))?;
    let catalog = kb::sources::bedrock_import::Bedrock::new(&session, &arn.region);
    match kb::sources::bedrock_import::plan(&catalog, &arn).await {
        Ok(p) => Ok(Json(serde_json::to_value(p).unwrap_or_default())),
        Err(r) => {
            let grant = match (&remote.access, r.code) {
                (kb::Access::Account { account }, "access") => {
                    let bases = store(&state)
                        .bases(&claims.team_id)
                        .await
                        .unwrap_or_default();
                    let region = accounts
                        .iter()
                        .find(|a| &a.account == account)
                        .map(|a| a.region.clone())
                        .unwrap_or_else(|| arn.region.clone());
                    grant_for(
                        &state,
                        &claims.team_id,
                        &bases,
                        account,
                        &region,
                        Some(&arn.arn()),
                        None,
                    )
                    .await
                }
                _ => None,
            };
            Err(connect_fail(&r, grant))
        }
    }
}

/// What starting an archive upload takes.
#[derive(Debug, Deserialize)]
pub struct ArchiveBody {
    pub name: String,
    pub size: u64,
    /// Upload a newer export to this archive source.
    #[serde(default)]
    pub replace: Option<String>,
}

/// POST /api/knowledge/:kb_id/archives — start uploading an export archive
/// (a new archive source, or a newer export for one): the source and a
/// signed upload URL. Nothing is read until the upload is in.
pub async fn start_archive(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
    Json(body): Json<ArchiveBody>,
) -> Reply {
    use kb::sources::store::SyncStore;
    claims.require_role(3).map_err(forbidden)?;
    let base = get_base_record(&state, &claims.team_id, &kb_id)
        .await
        .map_err(|e| internal("knowledge base", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such knowledge base."))?;
    refuse_connected(&base)?;
    let name = kb::sources::archive::check_archive(&body.name, body.size)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let st = super::knowledge_sources::sync_store(&state);
    let mut src = match body.replace.as_deref() {
        Some(id) => {
            let s = st
                .source(&claims.team_id, &base.id, id)
                .await
                .map_err(|e| internal("knowledge source", e))?
                .filter(|s| s.kind == Some(kb::sources::Kind::Archive))
                .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such archive."))?;
            s
        }
        None => {
            let existing =
                super::knowledge_sources::sources_of(&state, &claims.team_id, &base.id).await;
            if existing.len() >= kb::sources::MAX_SOURCES_PER_BASE {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "A knowledge base can have up to {} sources.",
                        kb::sources::MAX_SOURCES_PER_BASE
                    ),
                ));
            }
            kb::sources::Source {
                id: kb::new_id(),
                kb_id: base.id.clone(),
                kind: Some(kb::sources::Kind::Archive),
                name: common::truncate_str(name.trim_end_matches(".zip"), 80).to_string(),
                selection: vec![kb::sources::Selected {
                    id: "archive".into(),
                    title: name.clone(),
                    kind: "file".into(),
                    ..Default::default()
                }],
                options: json!({}),
                created_at: now(),
                created_by: claims.email.clone(),
                // Paused until the upload is in: the sweep never reads a
                // missing archive.
                paused: true,
                ..Default::default()
            }
        }
    };
    let version = src.options["version"].as_u64().unwrap_or(0) as u32 + 1;
    src.options["pending"] = json!({ "version": version, "file_name": name, "size": body.size });
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("archive source", e))?;
    let key = kb::sources::archive::archive_key(&claims.team_id, &base.id, &src.id, version);
    let upload = presign_put(&state, &key, "application/zip", body.size)
        .await
        .map_err(|e| internal("archive upload url", e))?;
    info!(team_id = %claims.team_id, kb = %base.id, source = %src.id, version, "Archive upload started");
    Ok(Json(json!({
        "source": super::knowledge_sources::source_json(&src, chrono::Utc::now().timestamp()),
        "upload": upload,
        "note": kb::sources::archive::NOTE,
    })))
}

/// POST /api/knowledge/:kb_id/archives/:source_id/uploaded — the archive is
/// in: point the source at it and import it (a newer export re-reads only
/// what changed and removes what it no longer has).
pub async fn finish_archive(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((kb_id, source_id)): Path<(String, String)>,
) -> Reply {
    use kb::sources::store::SyncStore;
    claims.require_role(3).map_err(forbidden)?;
    let st = super::knowledge_sources::sync_store(&state);
    let mut src = st
        .source(&claims.team_id, &kb_id, &source_id)
        .await
        .map_err(|e| internal("knowledge source", e))?
        .filter(|s| s.kind == Some(kb::sources::Kind::Archive))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such archive."))?;
    let pending = src.options["pending"].clone();
    let version = pending["version"].as_u64().unwrap_or(0) as u32;
    if version == 0 {
        return Err(fail(StatusCode::BAD_REQUEST, "Start the upload first."));
    }
    let key = kb::sources::archive::archive_key(&claims.team_id, &kb_id, &source_id, version);
    let size = match state
        .s3
        .head_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(h) => h.content_length().unwrap_or(0).max(0) as u64,
        Err(_) => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "The upload didn't arrive. Try uploading the archive again.",
            ))
        }
    };
    if size == 0 || size > kb::sources::archive::MAX_ARCHIVE_BYTES {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "The uploaded archive is empty or too large.",
        ));
    }
    let old = kb::sources::archive::current_key(&claims.team_id, &src);
    let file_name = pending["file_name"]
        .as_str()
        .unwrap_or("archive.zip")
        .to_string();
    src.options = json!({
        "version": version,
        "key": key,
        "file_name": file_name,
        "size": size,
        "uploaded_at": now(),
        "uploaded_by": claims.email,
    });
    src.selection = vec![kb::sources::Selected {
        id: "archive".into(),
        title: file_name,
        kind: "file".into(),
        ..Default::default()
    }];
    src.paused = false;
    src.next_sync_at = chrono::Utc::now().timestamp();
    if src.status == kb::sources::Status::Error {
        src.status = kb::sources::Status::Queued;
        src.error = None;
    }
    st.put_source(&claims.team_id, &src)
        .await
        .map_err(|e| internal("archive source", e))?;
    st.put_registry(&claims.team_id, &src)
        .await
        .map_err(|e| internal("register archive source", e))?;
    // The export it replaces is no longer needed.
    if let Some(old) = old.filter(|o| *o != key) {
        st.delete_object(&claims.team_id, &old).await;
    }
    if let Err(e) =
        super::knowledge_sources::start_sync(&state, &claims.team_id, &kb_id, &source_id).await
    {
        warn!(error = %e, "Archive import not started; the sweep starts it");
    }
    info!(team_id = %claims.team_id, kb = %kb_id, source = %source_id, version, "Archive uploaded");
    Ok(Json(json!({
        "source": super::knowledge_sources::source_json(&src, chrono::Utc::now().timestamp()),
    })))
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
    fn a_connected_search_tool_shows_its_server_and_who_it_searches_as() {
        let remote = kb::remote_mcp::mcp_remote(
            "search-1",
            &kb::remote_mcp::Checked {
                server_name: "Docs search".into(),
                search_tool: "search".into(),
                fetch_tool: "fetch".into(),
                per_person: true,
            },
        );
        let b = Base {
            id: "kb".into(),
            name: "Product docs".into(),
            agents: vec!["coder".into()],
            backend: kb::Backend::Remote(remote),
            ..Default::default()
        };
        let v = base_json(&b, &[], &[]);
        assert_eq!(v["backend"]["provider"], "mcp");
        assert_eq!(v["backend"]["label"], "Search tool (MCP)");
        assert_eq!(v["backend"]["server_name"], "Docs search");
        assert_eq!(v["backend"]["access"], "per_person");
        assert_eq!(v["backend"]["search_tool"], "search");
        assert!(v["agent_access_note"]
            .as_str()
            .unwrap()
            .contains("own sign-in"));
        let (_, body) = refuse_connected(&b).unwrap_err();
        assert!(body["error"].as_str().unwrap().contains("search tool"));
        let input: BaseInput = serde_json::from_value(json!({
            "name": "Docs", "connect": { "provider": "mcp", "target": "search-1", "search_tool": "search" }
        }))
        .unwrap();
        let c = input.connect.unwrap();
        assert_eq!(
            (c.provider.as_str(), c.search_tool.as_str()),
            ("mcp", "search")
        );
    }

    #[test]
    fn readers_are_checked_and_the_attach_note_says_who_sees_results() {
        let mut b = Base {
            read_role: "member".into(),
            ..Default::default()
        };
        let input = BaseInput {
            name: "HR".into(),
            readers: Some(vec![
                " Ada@Example.com ".into(),
                "role:admin".into(),
                "user:ada@example.com".into(),
            ]),
            attach_team: true,
            ..Default::default()
        };
        apply_input(&mut b, &input).unwrap();
        assert_eq!(b.readers, ["user:ada@example.com", "role:admin"]);
        assert!(b.agent_access_note().contains("only in runs started by"));
        let v = base_json(&b, &[], &[]);
        assert_eq!(v["readers"][1], "role:admin");
        // Left out: unchanged. Empty: everyone the role allows.
        apply_input(
            &mut b,
            &BaseInput {
                name: "HR".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(b.readers.len(), 2);
        apply_input(
            &mut b,
            &BaseInput {
                name: "HR".into(),
                readers: Some(vec![]),
                attach_team: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(b.readers.is_empty());
        assert!(b
            .agent_access_note()
            .starts_with("Everyone who can start an agent run"));
        let bad = BaseInput {
            name: "HR".into(),
            readers: Some(vec!["not a person".into()]),
            ..Default::default()
        };
        assert!(apply_input(&mut b, &bad)
            .unwrap_err()
            .contains("isn't a person or a role"));
        // Not attached: no note.
        assert_eq!(Base::default().agent_access_note(), "");
        // Limits say what imports take.
        let l = limits();
        assert_eq!(l["max_items_per_import"], kb::sources::MAX_ITEMS_PER_IMPORT);
        assert_eq!(
            l["max_archive_bytes"],
            kb::sources::archive::MAX_ARCHIVE_BYTES
        );
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
