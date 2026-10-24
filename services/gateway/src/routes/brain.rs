//! The company brain: a chat that answers questions from the team's own
//! sources, with a citation on every claim.
//!
//! Sources a question may use (picked per question, see [`SOURCE_KINDS`]):
//! knowledge bases, connections (the MCP servers on the Connections page),
//! repositories and the code graph, memories, data sources, and team skills.
//! Knowledge is retrieved first, in code, before the model's first step;
//! tools are offered for live data. Every passage and tool result is
//! registered as a citable source (`common::knowledge::answer`), and the
//! answer's citations come only from that registry: an answer that cites
//! nothing is replaced by a plain "the sources don't cover this".
//!
//! Threads belong to the person who asked; the owner can share one with the
//! team (read-only for everyone else). Viewers read shared threads and can't
//! ask. Every model call is metered under feature `brain`.
//!
//! A new kind of source (a future plugin package) joins by adding a
//! [`SourceKind`], tools whose names map to it in [`kind_of_tool`], and a
//! branch in [`run_tool`] that registers what it read; the picker, the
//! citations and the metering need nothing else.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::sse::Event,
    Extension, Json,
};
use common::knowledge::answer::{finalize, Final, Registry, SourceRef};
use common::knowledge::chunk::chunk;
use common::knowledge::extract::{extract, Kind};
use common::knowledge::search::{search, term_overlap, terms, Store, Titan};
use common::knowledge::{self as kb, read_data, MAX_ATTACHMENT_BYTES};
use common::model_stream::SseParser;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use super::plan_chat_tools::{ChatScope, Used, WorkerTools};
use super::plans::{
    compact_tool_result_for_model, execute_github_tool, invoke_mcp_tool, is_empty_tool_result,
    load_enabled_plugins, load_mcp_tool_cache, sse_event, summarize_tool_result,
};
use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;
type Tx = tokio::sync::mpsc::Sender<Result<Event, std::convert::Infallible>>;

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

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

// ── Sources a question may use ──────────────────────────────────────────────

/// One kind of source in the picker.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceKind {
    pub key: &'static str,
    pub label: &'static str,
    pub about: &'static str,
}

pub const SOURCE_KINDS: &[SourceKind] = &[
    SourceKind {
        key: "knowledge",
        label: "Knowledge bases",
        about: "Documents your team uploaded",
    },
    SourceKind {
        key: "connections",
        label: "Connections",
        about: "Connected tools such as Notion, Linear or Sentry, read live",
    },
    SourceKind {
        key: "code",
        label: "Repositories",
        about: "Code search, files and the code graph",
    },
    SourceKind {
        key: "memories",
        label: "Memories",
        about: "What Coderhelm learned from past work",
    },
    SourceKind {
        key: "data_sources",
        label: "Data sources",
        about: "Logs, databases, metrics and docs you connected, read-only",
    },
    SourceKind {
        key: "skills",
        label: "Team skills",
        about: "Your written skills and team agents",
    },
];

/// What one question may read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Picks {
    /// Knowledge bases (ids); only those the asker may read are used.
    #[serde(default)]
    pub knowledge: Vec<String>,
    #[serde(default)]
    pub connections: bool,
    #[serde(default)]
    pub code: bool,
    #[serde(default)]
    pub memories: bool,
    #[serde(default)]
    pub data_sources: bool,
    #[serde(default)]
    pub skills: bool,
    /// The repository code, memories and data sources are read for.
    #[serde(default)]
    pub repo: Option<String>,
}

impl Picks {
    pub fn allows(&self, kind: &str) -> bool {
        match kind {
            "knowledge" => !self.knowledge.is_empty(),
            "connections" => self.connections,
            "code" => self.code,
            "memories" => self.memories,
            "data_sources" => self.data_sources,
            "skills" => self.skills,
            _ => false,
        }
    }
}

/// The kind of source a tool reads.
pub fn kind_of_tool(name: &str) -> &'static str {
    match name {
        KNOWLEDGE_TOOL => "knowledge",
        MEMORY_TOOL => "memories",
        "read_skill" | "read_skill_file" | "ask_team_agent" => "skills",
        n if n.starts_with("github__") || n.starts_with("graph_") => "code",
        n if n.contains("__") => "connections",
        _ => "data_sources",
    }
}

const KNOWLEDGE_TOOL: &str = "search_knowledge";
const MEMORY_TOOL: &str = "search_memories";

// ── Threads ─────────────────────────────────────────────────────────────────

/// A conversation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Thread {
    pub id: String,
    /// The asker's user id.
    pub owner: String,
    #[serde(default)]
    pub owner_email: String,
    pub title: String,
    #[serde(default)]
    pub shared: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub messages: u32,
}

/// One message of a thread.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub seq: u32,
    /// `user` or `assistant`.
    pub role: String,
    pub content: String,
    #[serde(default)]
    pub citations: Vec<SourceRef>,
    /// False when nothing supported an answer.
    #[serde(default = "yes")]
    pub grounded: bool,
    /// What the question could read.
    #[serde(default)]
    pub sources: Picks,
    /// Files attached to the question (names).
    #[serde(default)]
    pub attachments: Vec<String>,
    /// Tools the answer called (`{kind, label}`).
    #[serde(default)]
    pub used: Vec<Value>,
    pub at: String,
}

fn yes() -> bool {
    true
}

fn thread_sk(id: &str) -> String {
    format!("BRAIN#T#{id}")
}

fn message_prefix(thread_id: &str) -> String {
    format!("BRAIN#M#{thread_id}#")
}

fn message_sk(thread_id: &str, seq: u32) -> String {
    format!("BRAIN#M#{thread_id}#{seq:06}")
}

/// Anyone on the team reads a shared thread; only its owner reads a private one.
pub fn can_read(t: &Thread, user_id: &str) -> bool {
    t.owner == user_id || t.shared
}

/// Only the owner asks in, renames, shares or deletes a thread.
pub fn can_write(t: &Thread, user_id: &str) -> bool {
    t.owner == user_id
}

fn record<T: Serialize>(team_id: &str, sk: &str, v: &T) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S(team_id.to_string())),
        ("sk".into(), AttributeValue::S(sk.to_string())),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(v).unwrap_or_default()),
        ),
    ])
}

async fn put<T: Serialize>(state: &AppState, team_id: &str, sk: &str, v: &T) -> Result<(), String> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(record(team_id, sk, v)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("put: {e}"))
}

async fn query<T: for<'de> Deserialize<'de>>(
    state: &AppState,
    team_id: &str,
    prefix: &str,
) -> Result<Vec<T>, String> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.to_string()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("query: {e}"))?;
        out.extend(r.items().iter().filter_map(read_data::<T>));
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

async fn get_thread_record(
    state: &AppState,
    team_id: &str,
    id: &str,
) -> Result<Option<Thread>, String> {
    if !kb::valid_id(id) {
        return Ok(None);
    }
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(thread_sk(id)))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("thread: {e}"))?;
    Ok(r.item().and_then(read_data::<Thread>))
}

fn thread_json(t: &Thread, user_id: &str) -> Value {
    json!({
        "id": t.id,
        "title": t.title,
        "shared": t.shared,
        "mine": t.owner == user_id,
        "owner_email": t.owner_email,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
        "messages": t.messages,
    })
}

/// GET /api/brain/threads — your threads and the ones shared with the team.
pub async fn list_threads(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let mut threads: Vec<Thread> = query(&state, &claims.team_id, "BRAIN#T#")
        .await
        .map_err(|e| internal("brain threads", e))?;
    threads.retain(|t| can_read(t, &claims.sub));
    threads.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    threads.truncate(200);
    Ok(Json(json!({
        "threads": threads.iter().map(|t| thread_json(t, &claims.sub)).collect::<Vec<_>>(),
        "can_ask": claims.holder().holds("brain_ask"),
    })))
}

/// GET /api/brain/threads/:id — one thread's messages.
pub async fn get_thread(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let t = get_thread_record(&state, &claims.team_id, &id)
        .await
        .map_err(|e| internal("brain thread", e))?
        .filter(|t| can_read(t, &claims.sub))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
    let mut messages: Vec<Message> = query(&state, &claims.team_id, &message_prefix(&t.id))
        .await
        .map_err(|e| internal("brain messages", e))?;
    messages.sort_by_key(|m| m.seq);
    Ok(Json(json!({
        "thread": thread_json(&t, &claims.sub),
        "messages": messages,
        "can_ask": can_write(&t, &claims.sub) && claims.holder().holds("brain_ask"),
    })))
}

#[derive(Debug, Deserialize)]
pub struct ThreadUpdate {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub shared: Option<bool>,
}

/// PUT /api/brain/threads/:id — rename, or share with the team.
pub async fn update_thread(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<ThreadUpdate>,
) -> Reply {
    if claims.require_role(1).is_err() {
        return Err(fail(StatusCode::FORBIDDEN, "Viewers can't change threads."));
    }
    let mut t = get_thread_record(&state, &claims.team_id, &id)
        .await
        .map_err(|e| internal("brain thread", e))?
        .filter(|t| can_read(t, &claims.sub))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
    if !can_write(&t, &claims.sub) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the person who started a thread can change it.",
        ));
    }
    if let Some(title) = body.title {
        let title = title.trim();
        if title.is_empty() || title.chars().count() > 120 {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Give the thread a title under 120 characters.",
            ));
        }
        t.title = title.to_string();
    }
    if let Some(shared) = body.shared {
        t.shared = shared;
    }
    put(&state, &claims.team_id, &thread_sk(&t.id), &t)
        .await
        .map_err(|e| internal("brain thread save", e))?;
    Ok(Json(json!({ "thread": thread_json(&t, &claims.sub) })))
}

/// DELETE /api/brain/threads/:id — the thread and its messages.
pub async fn delete_thread(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    if claims.require_role(1).is_err() {
        return Err(fail(StatusCode::FORBIDDEN, "Viewers can't delete threads."));
    }
    let t = get_thread_record(&state, &claims.team_id, &id)
        .await
        .map_err(|e| internal("brain thread", e))?
        .filter(|t| can_read(t, &claims.sub))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
    if !can_write(&t, &claims.sub) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the person who started a thread can delete it.",
        ));
    }
    let messages: Vec<Message> = query(&state, &claims.team_id, &message_prefix(&t.id))
        .await
        .map_err(|e| internal("brain messages", e))?;
    for sk in messages
        .iter()
        .map(|m| message_sk(&t.id, m.seq))
        .chain(std::iter::once(thread_sk(&t.id)))
    {
        state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", AttributeValue::S(claims.team_id.clone()))
            .key("sk", AttributeValue::S(sk))
            .send()
            .await
            .map_err(|e| internal("brain delete", e.to_string()))?;
    }
    Ok(Json(json!({ "ok": true })))
}

// ── What can be picked ──────────────────────────────────────────────────────

/// The team's enabled repositories and their default branches.
async fn load_repos(state: &AppState, team_id: &str) -> HashMap<String, String> {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":p", AttributeValue::S("REPO#".into()))
        .limit(50)
        .send()
        .await;
    let mut out = HashMap::new();
    if let Ok(r) = r {
        for item in r.items() {
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let Some(name) = item.get("repo_name").and_then(|v| v.as_s().ok()) else {
                continue;
            };
            if enabled {
                let branch = item
                    .get("default_branch")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_else(|| "main".into());
                out.insert(name.clone(), branch);
            }
        }
    }
    out
}

/// GET /api/brain/sources — what the picker offers this person.
pub async fn sources(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = super::knowledge::store(&state);
    let (bases, plugins, repos) = tokio::join!(
        st.bases(&claims.team_id),
        load_enabled_plugins(&state, &claims.team_id),
        load_repos(&state, &claims.team_id),
    );
    let mut knowledge = Vec::new();
    for b in bases
        .map_err(|e| internal("knowledge bases", e))?
        .into_iter()
        .filter(|b| b.readable_by(super::knowledge::reader_role(&claims.holder())))
    {
        let files = st.files(&claims.team_id, &b.id).await.unwrap_or_default();
        let ready = files.iter().filter(|f| f.ready_version > 0).count();
        knowledge.push(json!({
            "id": b.id, "name": b.name, "ready": ready, "authoritative": b.authoritative,
        }));
    }
    knowledge.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let connections: Vec<Value> = plugins
        .iter()
        .filter(|(_, creds, _)| *creds)
        .map(|(id, _, _)| json!({ "id": id }))
        .collect();
    let mut repo_names: Vec<&String> = repos.keys().collect();
    repo_names.sort();
    Ok(Json(json!({
        "kinds": SOURCE_KINDS,
        "knowledge": knowledge,
        "connections": connections,
        "repos": repo_names,
        "can_ask": claims.holder().holds("brain_ask"),
    })))
}

/// POST /api/brain/token — a five-minute token for `/api/brain/stream`,
/// minted only for someone holding `brain_ask` (the auth middleware decides
/// this route from its capability). Its audience is the brain's own, so it
/// is never accepted as a session or a plan chat token.
pub async fn stream_token(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    if claims.require_role(1).is_err() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Your role can't ask the brain.",
        ));
    }
    let token = crate::auth::jwt::create_scoped_token(
        &claims,
        crate::auth::jwt::BRAIN_AUDIENCE,
        &state.secrets.jwt_secret,
        300,
    )
    .map_err(|e| internal("brain token", e.to_string()))?;
    Ok(Json(json!({ "token": token })))
}

#[derive(Debug, Deserialize)]
pub struct AttachmentBody {
    pub name: String,
    pub size: u64,
}

/// POST /api/brain/attachments — a signed upload for a file attached to a
/// question (kept in the asker's own folder).
pub async fn create_attachment(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<AttachmentBody>,
) -> Reply {
    if claims.require_role(1).is_err() {
        return Err(fail(StatusCode::FORBIDDEN, "Viewers can't ask the brain."));
    }
    let name = kb::clean_file_name(&body.name);
    let kind = super::knowledge::check_upload(&name, body.size, MAX_ATTACHMENT_BYTES)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let id = kb::new_id();
    let key = kb::attachment_key(&claims.team_id, &user_folder(&claims.sub), &id);
    let upload = super::knowledge::presign_put(&state, &key, kind.content_type(), body.size)
        .await
        .map_err(|e| internal("attachment upload url", e))?;
    Ok(Json(json!({ "id": id, "name": name, "upload": upload })))
}

/// The user id as a folder name (`USER#x` → `USER_x`).
fn user_folder(user_id: &str) -> String {
    user_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

// ── Asking ──────────────────────────────────────────────────────────────────

const BRAIN_SYSTEM: &str = r#"You are the company brain for a software team that uses Coderhelm. You answer questions using only the team's own sources: passages from their knowledge bases, files attached to the question, and the results of tools you call (connections, repositories and the code graph, memories, data sources, team skills).

Rules:
- Ground every claim. After each sentence that uses a source, cite it with its id in square brackets, like [S2] or [S1, S4]. Cite only ids you were given in <source> blocks. Never invent an id.
- Read the <source> passages first. Call tools when the question needs live data (current status, numbers, tickets, code) or when the passages don't cover it.
- If the sources don't answer the question, say plainly that the selected sources don't cover it and what you looked at. Do not answer from general knowledge, do not guess, and never make up names, numbers, dates, links or policies.
- When sources disagree, prefer the one marked source_of_truth, then the one with the newer as_of date. Say that they disagree and cite both. If two figures differ, give both; never average them.
- Mention a source's as-of date when the answer depends on how current it is.
- Lead with the answer, then the details. Keep it short: a few sentences or a list. Markdown is fine; no headings for short answers."#;

/// What one question carries.
#[derive(Debug, Deserialize)]
pub struct AskBody {
    #[serde(default)]
    pub thread_id: Option<String>,
    pub question: String,
    #[serde(default)]
    pub sources: Picks,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AttachmentRef {
    pub id: String,
    pub name: String,
}

/// Longest question.
const MAX_QUESTION_CHARS: usize = 8_000;
/// Files attached to one question.
const MAX_ATTACHMENTS: usize = 3;
/// Earlier messages sent with a question.
const HISTORY: usize = 12;
/// Tool-use turns per answer.
const MAX_TURNS: usize = 6;
/// Knowledge passages retrieved before the model's first step.
const RETRIEVE_K: usize = 8;

/// The question for retrieval: a short follow-up carries the question
/// before it, so "and for contractors?" still finds the right passages.
pub fn retrieval_query(question: &str, history: &[Message]) -> String {
    let q = question.trim();
    if q.chars().count() >= 60 {
        return q.to_string();
    }
    match history.iter().rev().find(|m| m.role == "user") {
        Some(prev) => format!("{} {q}", common::truncate_str(prev.content.trim(), 400)),
        None => q.to_string(),
    }
}

/// Everything one answer needs, loaded before streaming starts.
struct Run {
    state: Arc<AppState>,
    team_id: String,
    api_key: String,
    model_id: String,
    picks: Picks,
    /// Knowledge bases the asker may read, among those picked.
    kb_ids: Vec<String>,
    repos: HashMap<String, String>,
    github_token: Option<String>,
    mcp_tools: Vec<Value>,
    worker: WorkerTools,
    scope: ChatScope,
}

/// The tools this question may call.
fn tool_defs(run: &Run) -> Vec<Value> {
    let mut tools: Vec<Value> = Vec::new();
    if !run.kb_ids.is_empty() {
        tools.push(json!({
            "name": KNOWLEDGE_TOOL,
            "description": "Search the team's knowledge bases again with different words. Returns passages as <source> blocks you can cite.",
            "input_schema": {"type": "object", "properties": {
                "query": {"type": "string", "description": "What to look for"}
            }, "required": ["query"]}
        }));
    }
    let repo_enum: Vec<Value> = {
        let mut r: Vec<&String> = run.repos.keys().collect();
        r.sort();
        r.into_iter().map(|x| json!(x)).collect()
    };
    if run.picks.memories && !repo_enum.is_empty() {
        tools.push(json!({
            "name": MEMORY_TOOL,
            "description": "Search what Coderhelm learned from past work on a repository (decisions, conventions, fixes). Returns <source> blocks you can cite.",
            "input_schema": {"type": "object", "properties": {
                "repo": {"type": "string", "enum": repo_enum},
                "query": {"type": "string"}
            }, "required": ["repo", "query"]}
        }));
    }
    if run.picks.code && run.github_token.is_some() && !repo_enum.is_empty() {
        tools.push(json!({
            "name": "github__search_code",
            "description": "Search code in a repository. Returns matching paths and lines.",
            "input_schema": {"type": "object", "properties": {
                "repo": {"type": "string", "enum": repo_enum},
                "query": {"type": "string"}
            }, "required": ["repo", "query"]}
        }));
        tools.push(json!({
            "name": "github__read_file",
            "description": "Read a file from a repository's default branch.",
            "input_schema": {"type": "object", "properties": {
                "repo": {"type": "string", "enum": repo_enum},
                "path": {"type": "string"}
            }, "required": ["repo", "path"]}
        }));
    }
    if run.picks.connections {
        tools.extend(run.mcp_tools.iter().cloned());
    }
    tools.extend(
        run.worker
            .tools
            .iter()
            .filter(|t| {
                run.picks
                    .allows(kind_of_tool(t["name"].as_str().unwrap_or("")))
            })
            .cloned(),
    );
    if let Some(last) = tools.last_mut() {
        last["cache_control"] = json!({"type": "ephemeral"});
    }
    tools
}

/// What the dashboard shows for a tool call.
fn tool_label(name: &str, input: &Value) -> String {
    let s = |k: &str| input[k].as_str().unwrap_or("").to_string();
    match name {
        KNOWLEDGE_TOOL => format!(
            "Searched knowledge for \"{}\"",
            common::truncate_str(&s("query"), 60)
        ),
        MEMORY_TOOL => format!("Searched memories of {}", s("repo")),
        "github__search_code" => format!("Searched code in {}", s("repo")),
        "github__read_file" => format!("Read {}", s("path")),
        n if n.starts_with("graph_") => format!("Code graph: {}", n.trim_start_matches("graph_")),
        n if n.contains("__") => {
            let (server, tool) = n.split_once("__").unwrap_or((n, ""));
            format!("{server}: {}", tool.replace('_', " "))
        }
        n => n.replace('_', " "),
    }
}

fn live_stamp() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string()
}

/// Runs one tool and registers what it read. Returns the text the model
/// reads and a short summary for the dashboard.
async fn run_tool(
    run: &Run,
    reg: &mut Registry,
    used: &mut Used,
    name: &str,
    input: &Value,
) -> (String, String, bool) {
    let kind = kind_of_tool(name);
    if !run.picks.allows(kind) {
        return (
            "That source isn't turned on for this question.".into(),
            "Not allowed".into(),
            true,
        );
    }
    let state = &run.state;
    match name {
        KNOWLEDGE_TOOL => {
            let q = input["query"].as_str().unwrap_or("");
            let meter = crate::model_call::chat_meter(state, &run.team_id);
            let titan = Titan {
                client: &state.bedrock,
                meter: &meter,
            };
            let st = super::knowledge::store(state);
            match search(
                &st,
                &titan,
                &run.team_id,
                &run.kb_ids,
                q,
                6,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(found) if !found.hits.is_empty() => {
                    super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                    let ids = reg.add_hits(&found.hits);
                    (reg.render(&ids), format!("{} passages", ids.len()), false)
                }
                Ok(found) => {
                    super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                    (
                        "No passages matched.".into(),
                        "No passages matched".into(),
                        false,
                    )
                }
                Err(e) => {
                    warn!(error = %e, "Brain knowledge search failed");
                    (
                        "Error: the knowledge bases couldn't be searched.".into(),
                        "Search failed".into(),
                        true,
                    )
                }
            }
        }
        MEMORY_TOOL => {
            let repo = input["repo"].as_str().unwrap_or("");
            if !run.repos.contains_key(repo) {
                return (
                    "Error: pick one of the listed repositories.".into(),
                    "Unknown repository".into(),
                    true,
                );
            }
            let q = input["query"].as_str().unwrap_or("");
            match super::memory::recall_for_brain(state, &run.team_id, repo, q, 5).await {
                Ok(found) if !found.is_empty() => {
                    let ids: Vec<String> = found
                        .iter()
                        .map(|m| {
                            reg.add(SourceRef {
                                kind: "memory".into(),
                                title: format!("Memory of {repo}"),
                                loc: repo.to_string(),
                                as_of: m.created.clone(),
                                passage: m.content.clone(),
                                href: Some(format!(
                                    "/memory?repo={}&id={}",
                                    urlencoding::encode(repo),
                                    m.id
                                )),
                                ..Default::default()
                            })
                        })
                        .collect();
                    (reg.render(&ids), format!("{} memories", ids.len()), false)
                }
                Ok(_) => (
                    "No memories matched.".into(),
                    "No memories matched".into(),
                    false,
                ),
                Err(e) => {
                    warn!(error = %e, "Brain memory search failed");
                    (
                        "Error: memories couldn't be read.".into(),
                        "Memory read failed".into(),
                        true,
                    )
                }
            }
        }
        _ => {
            let result: Value = if run.worker.names.contains(name) {
                let (text, _) =
                    super::plan_chat_tools::call(state, &run.scope, used, name, input).await;
                json!(text)
            } else if let Some((server, tool)) = name.split_once("__") {
                if server == "github" {
                    json!(
                        execute_github_tool(
                            &state.http,
                            &run.github_token,
                            &run.repos,
                            tool,
                            input
                        )
                        .await
                    )
                } else {
                    match invoke_mcp_tool(state, &run.team_id, server, tool, input).await {
                        Ok(r) => r,
                        Err(e) => json!(format!("Error: {e}")),
                    }
                }
            } else {
                json!(format!("Unknown tool: {name}"))
            };
            let is_error = result.as_str().is_some_and(|s| s.starts_with("Error:"));
            if is_error || is_empty_tool_result(&result) {
                let text = if is_error {
                    result.as_str().unwrap_or("").to_string()
                } else {
                    "No results found.".into()
                };
                let summary = if is_error {
                    "Failed".into()
                } else {
                    "No results".into()
                };
                return (text, summary, is_error);
            }
            let passage = compact_tool_result_for_model(&result);
            let (title, loc) = match name {
                "github__read_file" => (
                    input["path"].as_str().unwrap_or("file").to_string(),
                    input["repo"].as_str().unwrap_or("").to_string(),
                ),
                _ => (
                    tool_label(name, input),
                    input["repo"].as_str().unwrap_or("").to_string(),
                ),
            };
            let id = reg.add(SourceRef {
                kind: match kind {
                    "connections" => "connection",
                    "data_sources" => "data_source",
                    "skills" => "skill",
                    other => other,
                }
                .into(),
                title,
                loc,
                as_of: live_stamp(),
                passage,
                ..Default::default()
            });
            (reg.render(&[id]), summarize_tool_result(&result), false)
        }
    }
}

/// Files attached to the question, as citable passages. Only the asker's
/// own uploads are read.
async fn read_attachments(
    state: &AppState,
    team_id: &str,
    user_id: &str,
    question: &str,
    atts: &[AttachmentRef],
    reg: &mut Registry,
) -> (Vec<String>, Vec<String>) {
    let mut ids = Vec::new();
    let mut problems = Vec::new();
    let qterms = terms(question);
    for a in atts.iter().take(MAX_ATTACHMENTS) {
        let name = kb::clean_file_name(&a.name);
        let Some(kind) = Kind::of(&name).filter(|_| kb::valid_id(&a.id)) else {
            problems.push(format!("{name}: not a supported file"));
            continue;
        };
        let key = kb::attachment_key(team_id, &user_folder(user_id), &a.id);
        if !kb::owns_key(team_id, &key) {
            continue;
        }
        let bytes = match state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(o) if o.content_length().unwrap_or(0) as u64 <= MAX_ATTACHMENT_BYTES => {
                match o.body.collect().await {
                    Ok(b) => b.into_bytes().to_vec(),
                    Err(_) => {
                        problems.push(format!("{name}: couldn't be read"));
                        continue;
                    }
                }
            }
            _ => {
                problems.push(format!("{name}: the upload wasn't found"));
                continue;
            }
        };
        let extracted = match tokio::task::spawn_blocking(move || extract(kind, &bytes)).await {
            Ok(Ok(x)) => x,
            Ok(Err(why)) => {
                problems.push(format!("{name}: {why}"));
                continue;
            }
            Err(_) => {
                problems.push(format!("{name}: couldn't be read"));
                continue;
            }
        };
        let mut chunks = chunk(&extracted.sections);
        // A long file: the passages sharing the most words with the question.
        if chunks.len() > 12 {
            chunks.sort_by(|a, b| {
                term_overlap(&qterms, &b.text).total_cmp(&term_overlap(&qterms, &a.text))
            });
            chunks.truncate(12);
            chunks.sort_by_key(|c| c.ord);
        }
        let as_of = extracted
            .as_of
            .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());
        for c in chunks {
            ids.push(reg.add(SourceRef {
                kind: "upload".into(),
                title: name.clone(),
                loc: c.loc,
                as_of: as_of.clone(),
                passage: c.text,
                ..Default::default()
            }));
        }
    }
    (ids, problems)
}

/// The team's model key and the model the brain uses (the heavy model,
/// like the plan chat).
async fn load_provider(state: &AppState, team_id: &str) -> (Option<String>, String) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("SETTINGS#MODEL_PROVIDER".into()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());
    let key = item
        .as_ref()
        .and_then(|i| i.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned();
    let model = item
        .as_ref()
        .and_then(|i| {
            i.get("heavy_model")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .or_else(|| i.get("primary_model").and_then(|v| v.as_s().ok()))
        })
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-5-5".to_string());
    (key, model)
}

async fn github_token(state: &AppState, team_id: &str) -> Option<String> {
    let id = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("META".into()))
        .projection_expression("github_installation_id")
        .send()
        .await
        .ok()?
        .item()?
        .get("github_installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&n| n > 0)?;
    crate::auth::github_app::get_installation_token(state, id)
        .await
        .inspect_err(|e| warn!(error = %e, "Brain: no GitHub token"))
        .ok()
}

/// The connections' tools (cached schemas), named `server__tool`.
async fn connection_tools(state: &AppState, team_id: &str) -> Vec<Value> {
    if state.config.mcp_proxy_function_name.is_empty() {
        return Vec::new();
    }
    let plugins = load_enabled_plugins(state, team_id).await;
    let mut out = Vec::new();
    for (server, creds, _) in plugins {
        if !creds {
            continue;
        }
        if let Some(tools) =
            load_mcp_tool_cache(&state.s3, &state.config.bucket_name, &server).await
        {
            for t in tools {
                out.push(json!({
                    "name": format!("{server}__{}", t["name"].as_str().unwrap_or("")),
                    "description": t["description"],
                    "input_schema": t["inputSchema"],
                }));
            }
        }
    }
    out
}

/// POST /api/brain/stream — ask a question; the answer streams back as
/// server-sent events (`thread`, `retrieved`, `tool_start`, `tool_result`,
/// `text_delta`, `answer`, `done`, or `error`). Authorized by the short-lived
/// token from `/api/brain/token` (its own audience); what the asker holds
/// is read again here, so the stream is decided by capability like every
/// other route.
#[allow(clippy::type_complexity)]
pub async fn brain_stream(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<AskBody>,
) -> Result<
    axum::response::sse::Sse<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<Event, std::convert::Infallible>> + Send>,
        >,
    >,
    (StatusCode, Json<Value>),
> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| fail(StatusCode::UNAUTHORIZED, "Sign in again."))?;
    let mut claims = crate::auth::jwt::validate_scoped_token(
        token,
        crate::auth::jwt::BRAIN_AUDIENCE,
        &state.secrets.jwt_secret,
    )
    .map_err(|_| fail(StatusCode::UNAUTHORIZED, "Sign in again."))?;
    // The token carries the member's stored role; what it holds is read now,
    // so a role changed since the token was minted is honored.
    let holder = super::roles::holder_for(&state, &claims).await;
    if !holder.holds("brain_ask") {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Your role can read shared threads but can't ask the brain.",
        ));
    }
    let reader = super::knowledge::reader_role(&holder);
    claims.role = holder.floor().to_string();
    let question = body.question.trim().to_string();
    if question.is_empty() || question.chars().count() > MAX_QUESTION_CHARS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Ask a question under 8,000 characters.",
        ));
    }

    // The thread: the asker's own, or a new one.
    let (mut thread, history) = match body.thread_id.as_deref().filter(|s| !s.is_empty()) {
        Some(id) => {
            let t = get_thread_record(&state, &claims.team_id, id)
                .await
                .map_err(|e| internal("brain thread", e))?
                .filter(|t| can_write(t, &claims.sub))
                .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
            let mut h: Vec<Message> = query(&state, &claims.team_id, &message_prefix(&t.id))
                .await
                .map_err(|e| internal("brain messages", e))?;
            h.sort_by_key(|m| m.seq);
            (t, h)
        }
        None => {
            let t = Thread {
                id: kb::new_id(),
                owner: claims.sub.clone(),
                owner_email: claims.email.clone(),
                title: common::truncate_str(&question.replace('\n', " "), 80).to_string(),
                shared: false,
                created_at: now(),
                updated_at: now(),
                messages: 0,
            };
            put(&state, &claims.team_id, &thread_sk(&t.id), &t)
                .await
                .map_err(|e| internal("brain thread save", e))?;
            (t, Vec::new())
        }
    };

    let (api_key, model_id) = load_provider(&state, &claims.team_id).await;
    let picks = body.sources.clone();
    let st = super::knowledge::store(&state);
    let readable: Vec<String> = st
        .bases(&claims.team_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|b| b.readable_by(reader) && picks.knowledge.contains(&b.id))
        .map(|b| b.id)
        .collect();
    let repos = load_repos(&state, &claims.team_id).await;
    let repo = picks
        .repo
        .clone()
        .filter(|r| repos.contains_key(r))
        .or_else(|| {
            let mut r: Vec<&String> = repos.keys().collect();
            r.sort();
            r.first().map(|s| s.to_string())
        })
        .unwrap_or_default();
    let scope = ChatScope {
        team_id: claims.team_id.clone(),
        chat_id: format!("brain-{}", thread.id),
        branch: repos.get(&repo).cloned().unwrap_or_default(),
        repo,
        first: history.is_empty(),
        brain: true,
    };
    let needs_worker = picks.skills || picks.data_sources || picks.code;
    let (worker, github, mcp_tools) = tokio::join!(
        async {
            if needs_worker {
                super::plan_chat_tools::setup(&state, &scope).await
            } else {
                WorkerTools::default()
            }
        },
        async {
            if picks.code {
                github_token(&state, &claims.team_id).await
            } else {
                None
            }
        },
        async {
            if picks.connections {
                connection_tools(&state, &claims.team_id).await
            } else {
                Vec::new()
            }
        },
    );
    let run = Run {
        state: state.clone(),
        team_id: claims.team_id.clone(),
        api_key: api_key.unwrap_or_default(),
        model_id,
        picks: picks.clone(),
        kb_ids: readable,
        repos,
        github_token: github,
        mcp_tools,
        worker,
        scope,
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(64);
    let user_id = claims.sub.clone();
    let attachments = body.attachments.clone();
    info!(team_id = %claims.team_id, thread = %thread.id, kbs = run.kb_ids.len(), "Brain question");
    tokio::spawn(async move {
        let _ = tx
            .send(sse_event(
                "thread",
                json!({ "id": thread.id, "title": thread.title }),
            ))
            .await;
        let (fin, used) = if run.api_key.is_empty() && !common::model_provider::is_bedrock() {
            let _ = tx
                .send(sse_event("error", json!({ "message": "Add a model provider key in Settings → AI models to use the brain." })))
                .await;
            let _ = tx.send(sse_event("done", json!({}))).await;
            return;
        } else {
            match answer(&run, &tx, &user_id, &question, &history, &attachments).await {
                Some(done) => done,
                // The model couldn't be reached: the error was sent, nothing is kept.
                None => {
                    let _ = tx.send(sse_event("done", json!({}))).await;
                    return;
                }
            }
        };
        let _ = tx
            .send(sse_event(
                "answer",
                json!({ "text": fin.text, "citations": fin.citations, "grounded": fin.grounded }),
            ))
            .await;
        // Keep the exchange.
        let seq = thread.messages;
        let names: Vec<String> = attachments
            .iter()
            .map(|a| kb::clean_file_name(&a.name))
            .collect();
        let user_msg = Message {
            seq,
            role: "user".into(),
            content: question.clone(),
            grounded: true,
            sources: run.picks.clone(),
            attachments: names,
            at: now(),
            ..Default::default()
        };
        let bot_msg = Message {
            seq: seq + 1,
            role: "assistant".into(),
            content: fin.text.clone(),
            citations: fin.citations.clone(),
            grounded: fin.grounded,
            used,
            at: now(),
            ..Default::default()
        };
        thread.messages = seq + 2;
        thread.updated_at = now();
        let saved = async {
            put(
                &run.state,
                &run.team_id,
                &message_sk(&thread.id, seq),
                &user_msg,
            )
            .await?;
            put(
                &run.state,
                &run.team_id,
                &message_sk(&thread.id, seq + 1),
                &bot_msg,
            )
            .await?;
            put(&run.state, &run.team_id, &thread_sk(&thread.id), &thread).await
        }
        .await;
        if let Err(e) = saved {
            error!(error = %e, "Brain answer not saved");
        }
        let _ = tx.send(sse_event("done", json!({}))).await;
    });

    let stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Event, std::convert::Infallible>> + Send>,
    > = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    Ok(axum::response::sse::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text(""),
    ))
}

/// The model's input for this question: the earlier exchange, then the
/// retrieved sources and the question.
fn build_messages(
    history: &[Message],
    sources_block: &str,
    notes: &str,
    question: &str,
) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let start = history.len().saturating_sub(HISTORY);
    for m in &history[start..] {
        if m.content.trim().is_empty() {
            continue;
        }
        let role = if m.role == "assistant" {
            "assistant"
        } else {
            "user"
        };
        // Two messages of the same role in a row are merged.
        if out.last().is_some_and(|l| l["role"] == role) {
            continue;
        }
        out.push(json!({ "role": role, "content": [{"type": "text", "text": m.content}] }));
    }
    if out.last().is_some_and(|l| l["role"] == "user") {
        out.pop();
    }
    if out.first().is_some_and(|f| f["role"] == "assistant") {
        out.remove(0);
    }
    let block = if sources_block.trim().is_empty() {
        "(no knowledge passages matched this question)".to_string()
    } else {
        sources_block.to_string()
    };
    out.push(json!({ "role": "user", "content": [{"type": "text", "text":
        format!("<sources>\n{block}</sources>\n{notes}\nQuestion: {question}")
    }]}));
    out
}

/// Retrieves, asks the model (with tools) and finalizes the answer. None
/// when the model couldn't be reached (the error was already sent).
async fn answer(
    run: &Run,
    tx: &Tx,
    user_id: &str,
    question: &str,
    history: &[Message],
    attachments: &[AttachmentRef],
) -> Option<(Final, Vec<Value>)> {
    let mut reg = Registry::default();
    let mut used_tools: Vec<Value> = Vec::new();
    let state = &run.state;

    // 1. Retrieve first, in code.
    let mut first_ids: Vec<String> = Vec::new();
    if !run.kb_ids.is_empty() {
        let meter = crate::model_call::chat_meter(state, &run.team_id);
        let titan = Titan {
            client: &state.bedrock,
            meter: &meter,
        };
        let st = super::knowledge::store(state);
        match search(
            &st,
            &titan,
            &run.team_id,
            &run.kb_ids,
            &retrieval_query(question, history),
            RETRIEVE_K,
            chrono::Utc::now(),
        )
        .await
        {
            Ok(found) => {
                super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                first_ids.extend(reg.add_hits(&found.hits));
            }
            Err(e) => warn!(error = %e, "Brain retrieval failed"),
        }
    }
    let (att_ids, problems) = read_attachments(
        state,
        &run.team_id,
        user_id,
        question,
        attachments,
        &mut reg,
    )
    .await;
    first_ids.extend(att_ids);
    let _ = tx
        .send(sse_event("retrieved", json!({
            "sources": first_ids.iter().filter_map(|id| reg.get(id)).cloned().collect::<Vec<_>>(),
        })))
        .await;

    let tools = tool_defs(run);
    // Nothing found and nothing to look up: say so without a model call.
    if reg.is_empty() && tools.is_empty() {
        return Some((finalize("", &reg), used_tools));
    }

    let mut notes = String::new();
    if !problems.is_empty() {
        notes.push_str(&format!(
            "\nAttached files that couldn't be read: {}\n",
            problems.join("; ")
        ));
    }
    if !run.worker.prompt.is_empty() && (run.picks.skills || run.picks.data_sources) {
        notes.push_str(&format!(
            "\nThe team's own tools for this question:{}\n",
            run.worker.prompt
        ));
    }
    let mut messages = build_messages(history, &reg.render(&first_ids), &notes, question);
    let http = reqwest::Client::new();
    let meter = crate::model_call::chat_meter(state, &run.team_id);
    let mut worker_used = Used::default();
    let mut last_text = String::new();

    for turn in 0..MAX_TURNS {
        let last_turn = turn + 1 == MAX_TURNS;
        let mut body = json!({
            "model": run.model_id,
            "max_tokens": 4096,
            "stream": true,
            "system": [{"type": "text", "text": BRAIN_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
        });
        if common::is_always_thinking_model(&run.model_id) {
            body["output_config"] = json!({"effort": "medium"});
        }
        if !tools.is_empty() && !last_turn {
            body["tools"] = json!(tools);
        }
        let (resp, mut flight) = match crate::model_call::open_brain_stream(
            &http,
            &run.api_key,
            &body,
            &meter,
            &run.model_id,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(transient = e.is_transient(), "Brain model call failed: {e}");
                let _ = tx
                        .send(sse_event("error", json!({ "message": "The model couldn't be reached. Try again in a moment." })))
                        .await;
                return None;
            }
        };
        let mut parser = SseParser::default();
        let mut stream = resp;
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            for ev in parser.push(&chunk) {
                flight.acc.feed(&ev);
                match ev["type"].as_str() {
                    Some("content_block_delta") if ev["delta"]["type"] == "text_delta" => {
                        if let Some(t) = ev["delta"]["text"].as_str() {
                            let _ = tx.send(sse_event("text_delta", json!({ "text": t }))).await;
                        }
                    }
                    Some("content_block_start") if ev["content_block"]["type"] == "tool_use" => {
                        let name = ev["content_block"]["name"].as_str().unwrap_or("");
                        let _ = tx
                            .send(sse_event("tool_start", json!({
                                "id": ev["content_block"]["id"], "name": name, "kind": kind_of_tool(name),
                            })))
                            .await;
                    }
                    _ => {}
                }
            }
        }
        flight.finish().await;
        let message = std::mem::take(&mut flight.acc).into_message();
        let content = message["content"].as_array().cloned().unwrap_or_default();
        let stop = message["stop_reason"].as_str().unwrap_or("").to_string();
        let text: String = content
            .iter()
            .filter(|b| b["type"] == "text")
            .filter_map(|b| b["text"].as_str())
            .collect();
        if !text.trim().is_empty() {
            last_text = text;
        }
        if !content.is_empty() {
            messages.push(json!({ "role": "assistant", "content": content }));
        }
        let tool_uses: Vec<Value> = content
            .iter()
            .filter(|b| b["type"] == "tool_use")
            .cloned()
            .collect();
        if stop != "tool_use" || tool_uses.is_empty() {
            break;
        }
        let mut results = Vec::new();
        for tu in &tool_uses {
            let name = tu["name"].as_str().unwrap_or("");
            let input = &tu["input"];
            let label = tool_label(name, input);
            let (text, summary, is_error) =
                run_tool(run, &mut reg, &mut worker_used, name, input).await;
            let _ = tx
                .send(sse_event(
                    "tool_result",
                    json!({
                        "id": tu["id"], "name": name, "label": label,
                        "status": if is_error { "error" } else { "success" }, "summary": summary,
                    }),
                ))
                .await;
            used_tools.push(json!({ "kind": kind_of_tool(name), "label": label }));
            results
                .push(json!({ "type": "tool_result", "tool_use_id": tu["id"], "content": text }));
        }
        messages.push(json!({ "role": "user", "content": results }));
        // The next turn's text replaces what was said before the tools.
        let _ = tx.send(sse_event("text_reset", json!({}))).await;
    }
    Some((finalize(&last_text, &reg), used_tools))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::knowledge::answer::NO_SOURCE_ANSWER;

    fn thread(owner: &str, shared: bool) -> Thread {
        Thread {
            id: "t1".into(),
            owner: owner.into(),
            title: "q".into(),
            shared,
            ..Default::default()
        }
    }

    #[test]
    fn threads_are_private_until_shared_and_only_the_owner_writes() {
        let private = thread("USER#a", false);
        assert!(can_read(&private, "USER#a"));
        assert!(!can_read(&private, "USER#b"));
        let shared = thread("USER#a", true);
        assert!(can_read(&shared, "USER#b"));
        assert!(!can_write(&shared, "USER#b"));
        assert!(can_write(&shared, "USER#a"));
    }

    #[test]
    fn tools_map_to_the_source_they_read() {
        assert_eq!(kind_of_tool("search_knowledge"), "knowledge");
        assert_eq!(kind_of_tool("search_memories"), "memories");
        assert_eq!(kind_of_tool("ask_team_agent"), "skills");
        assert_eq!(kind_of_tool("github__read_file"), "code");
        assert_eq!(kind_of_tool("graph_callers"), "code");
        assert_eq!(kind_of_tool("notion__search"), "connections");
        assert_eq!(kind_of_tool("logs_search"), "data_sources");
        // Every kind a tool maps to is one the picker offers.
        for t in [
            "search_knowledge",
            "search_memories",
            "read_skill",
            "github__x",
            "a__b",
            "x",
        ] {
            assert!(SOURCE_KINDS.iter().any(|k| k.key == kind_of_tool(t)), "{t}");
        }
    }

    #[test]
    fn picks_gate_each_kind() {
        let none = Picks::default();
        assert!(SOURCE_KINDS.iter().all(|k| !none.allows(k.key)));
        let p = Picks {
            knowledge: vec!["kb".into()],
            memories: true,
            ..Default::default()
        };
        assert!(p.allows("knowledge") && p.allows("memories"));
        assert!(!p.allows("connections") && !p.allows("code") && !p.allows("data_sources"));
        assert!(!p.allows("made-up"));
        let old: Picks = serde_json::from_str("{}").unwrap();
        assert_eq!(old, Picks::default());
    }

    #[test]
    fn follow_ups_carry_the_question_before_them() {
        let h = vec![Message {
            role: "user".into(),
            content: "How long is parental leave for employees?".into(),
            ..Default::default()
        }];
        assert_eq!(
            retrieval_query("and contractors?", &h),
            "How long is parental leave for employees? and contractors?"
        );
        let long = "x".repeat(80);
        assert_eq!(retrieval_query(&long, &h), long);
        assert_eq!(retrieval_query("hi", &[]), "hi");
    }

    #[test]
    fn messages_alternate_and_end_with_the_question() {
        let m = |role: &str, c: &str| Message {
            role: role.into(),
            content: c.into(),
            ..Default::default()
        };
        let h = vec![
            m("assistant", "stray"),
            m("user", "q1"),
            m("assistant", "a1"),
            m("user", "q2 unanswered"),
        ];
        let out = build_messages(&h, "<source id=\"S1\">x</source>\n", "", "q3");
        let roles: Vec<&str> = out.iter().map(|x| x["role"].as_str().unwrap()).collect();
        assert_eq!(roles, ["user", "assistant", "user"]);
        let last = out.last().unwrap()["content"][0]["text"].as_str().unwrap();
        assert!(
            last.contains("<sources>") && last.contains("S1") && last.ends_with("Question: q3")
        );
        let empty = build_messages(&[], "", "", "q");
        assert!(empty[0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("no knowledge passages matched"));
    }

    #[test]
    fn nothing_to_read_answers_plainly() {
        // The short-circuit path: no passages, no tools.
        let f = finalize("", &Registry::default());
        assert!(!f.grounded);
        assert_eq!(f.text, NO_SOURCE_ANSWER);
    }

    #[test]
    fn user_folders_are_plain() {
        assert_eq!(user_folder("USER#abc-1"), "USER_abc-1");
        assert!(kb::owns_key(
            "TEAM#x",
            &kb::attachment_key("TEAM#x", &user_folder("USER#a/b"), "id")
        ));
    }

    #[test]
    fn tool_labels_read_plainly() {
        assert_eq!(
            tool_label("github__read_file", &json!({"path": "src/a.rs"})),
            "Read src/a.rs"
        );
        assert_eq!(
            tool_label("notion__search_pages", &json!({})),
            "notion: search pages"
        );
        assert!(tool_label("search_knowledge", &json!({"query": "leave"})).contains("leave"));
    }
}
