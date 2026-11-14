//! The company brain: a chat that answers questions from the team's own
//! sources, with a citation on every claim.
//!
//! Sources a question may use (see [`SOURCE_KINDS`]): knowledge bases,
//! connections (the MCP servers on the Connections page), repositories and
//! the code graph, memories, data sources, team skills, and the team's own
//! work: tickets, pull requests, runs, findings and the other records
//! Coderhelm keeps (`chat_work`), read only and only what the asker could
//! see in the dashboard. A question about the team's work ("what's the
//! latest on", a ticket key, a PR number, a CVE id, "what's running") is
//! pointed at that source first (`chat_hints`), and an answer that says it
//! found nothing without looking there is sent back once (`chat_agent`). By default a
//! question may use every source the asker can see; the asker can narrow it
//! by mentioning a repository or a knowledge base. Knowledge is retrieved
//! first, in code, before the model's first step; the model then reads code
//! and live data with tools, over several steps, until it can answer
//! (`chat_agent`), and an answer that hands the asker work its tools could
//! do is sent back to the tools once. Every passage and tool result is
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
use common::knowledge::answer::{finalize, CodeRef, Final, Registry, SourceRef};
use common::knowledge::chunk::chunk;
use common::knowledge::extract::{extract, Kind};
use common::knowledge::search::{search, term_overlap, terms, Store, Titan};
use common::knowledge::{self as kb, read_data, MAX_ATTACHMENT_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{error, info, warn};

use super::chat_agent::{self, Called, Sink, Toolbox};
use super::chat_code;
use super::plan_chat_tools::{ChatScope, Used, WorkerTools};
use super::plans::{
    compact_tool_result_for_model, invoke_mcp_tool, is_empty_tool_result, load_enabled_plugins,
    load_mcp_tool_cache, summarize_tool_result,
};
use crate::models::Claims;
use crate::AppState;
use common::repo_read::{CodeReader, GitHubHost, RepoInfo};

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
        label: "Files and pages",
        about: "Documents and pages your team added to Knowledge",
    },
    SourceKind {
        key: "connections",
        label: "Connected apps",
        about: "Apps such as Notion, Linear or Sentry, read as you ask",
    },
    SourceKind {
        key: "code",
        label: "Code",
        about: "Your team's code and how it fits together",
    },
    SourceKind {
        key: "memories",
        label: "Past work",
        about: "What Coderhelm learned from earlier work",
    },
    SourceKind {
        key: "data_sources",
        label: "Logs and data",
        about: "Logs, databases and metrics you connected, only read",
    },
    SourceKind {
        key: "skills",
        label: "Team know-how",
        about: "Your team's written skills and helpers",
    },
    SourceKind {
        key: super::chat_work::SOURCE_KEY,
        label: super::chat_work::SOURCE_LABEL,
        about: super::chat_work::SOURCE_ABOUT,
    },
    SourceKind {
        key: common::plugins::brain::SOURCE_KEY,
        label: common::plugins::brain::SOURCE_LABEL,
        about: common::plugins::brain::SOURCE_ABOUT,
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
    /// Tickets, pull requests, runs, findings and the records Coderhelm keeps.
    #[serde(default)]
    pub work: bool,
    /// Installed plugins (ids) the question may ask; only active ones are used.
    #[serde(default)]
    pub plugins: Vec<String>,
    /// The repository skills and data sources are read for.
    #[serde(default)]
    pub repo: Option<String>,
    /// The repositories code may be read in; empty for all of the team's.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<String>,
    /// Chosen by Chat (everything the asker can see), not narrowed.
    #[serde(default)]
    pub auto: bool,
}

/// Something the asker mentioned with @ to narrow a question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mention {
    /// `repo`, `knowledge`, or `run` (context only: the run's state goes
    /// with the question; it doesn't narrow what Chat reads).
    pub kind: String,
    /// The repository (`owner/name`), knowledge base id or run id.
    pub id: String,
}

/// What a question reads. With mentions, only what was mentioned (a
/// repository's code and past work, or a knowledge base); with picks sent
/// by an older client, those; otherwise everything the asker can see.
/// `readable` are the knowledge bases the asker may read, `repos` the
/// team's repositories.
pub fn resolve_picks(
    sent: Option<&Picks>,
    mentions: &[Mention],
    readable: &[String],
    repos: &[String],
) -> Picks {
    let mentioned = |kind: &str| -> Vec<String> {
        mentions
            .iter()
            .filter(|m| m.kind == kind)
            .map(|m| m.id.clone())
            .collect()
    };
    let m_repos: Vec<String> = mentioned("repo")
        .into_iter()
        .filter(|r| repos.contains(r))
        .collect();
    let m_kbs: Vec<String> = mentioned("knowledge")
        .into_iter()
        .filter(|k| readable.contains(k))
        .collect();
    if !m_repos.is_empty() || !m_kbs.is_empty() {
        return Picks {
            knowledge: m_kbs,
            code: !m_repos.is_empty(),
            memories: !m_repos.is_empty(),
            // Where the team's work stands is never narrowed away: it's
            // where a status question's answer lives.
            work: true,
            repo: m_repos.first().cloned(),
            repos: m_repos,
            ..Default::default()
        };
    }
    if let Some(p) = sent.filter(|p| SOURCE_KINDS.iter().any(|k| p.allows(k.key))) {
        let mut p = p.clone();
        p.knowledge.retain(|k| readable.contains(k));
        p.repos.retain(|r| repos.contains(r));
        return p;
    }
    Picks {
        knowledge: readable.to_vec(),
        connections: true,
        code: !repos.is_empty(),
        memories: !repos.is_empty(),
        data_sources: true,
        skills: true,
        work: true,
        // Every active plugin, looked up when the question is asked.
        plugins: Vec::new(),
        repo: None,
        repos: Vec::new(),
        auto: true,
    }
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
            super::chat_work::SOURCE_KEY => self.work,
            common::plugins::brain::SOURCE_KEY => !self.plugins.is_empty(),
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
        common::plugins::brain::ASK_TOOL => common::plugins::brain::SOURCE_KEY,
        n if super::chat_work::is_work_tool(n) => super::chat_work::SOURCE_KEY,
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
    /// Pinned to the top of its owner's chat list.
    #[serde(default)]
    pub pinned: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub messages: u32,
    /// Where it was asked from: empty for Chat, `kb_page` for a knowledge
    /// base page's "Ask this knowledge base".
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
}

/// The one place besides Chat a thread can start: a knowledge base page.
pub const ORIGIN_KB_PAGE: &str = "kb_page";

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
    /// What the asker mentioned with @ to narrow the question.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<Mention>,
    /// Files attached to the question (names).
    #[serde(default)]
    pub attachments: Vec<String>,
    /// Tools the answer called (`{kind, label}`).
    #[serde(default)]
    pub used: Vec<Value>,
    pub at: String,
    /// The asker's rating of an answer: `up` or `down`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
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
        // Pins are the owner's own; a shared chat is never pinned for others.
        "pinned": t.pinned && t.owner == user_id,
        "mine": t.owner == user_id,
        "owner_email": t.owner_email,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
        "messages": t.messages,
        "origin": t.origin,
    })
}

/// GET /api/brain/threads — your threads and the ones shared with the team.
pub async fn list_threads(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let mut threads: Vec<Thread> = query(&state, &claims.team_id, "BRAIN#T#")
        .await
        .map_err(|e| internal("chat threads", e))?;
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
        .map_err(|e| internal("chat thread", e))?
        .filter(|t| can_read(t, &claims.sub))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
    let mut messages: Vec<Message> = query(&state, &claims.team_id, &message_prefix(&t.id))
        .await
        .map_err(|e| internal("chat messages", e))?;
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
    #[serde(default)]
    pub pinned: Option<bool>,
}

/// How many chats one person may pin.
pub const MAX_PINNED: usize = 5;

/// Whether one more chat may be pinned, given the owner's chats.
pub fn may_pin(threads: &[Thread], owner: &str, id: &str) -> bool {
    threads
        .iter()
        .filter(|t| t.owner == owner && t.pinned && t.id != id)
        .count()
        < MAX_PINNED
}

/// PUT /api/brain/threads/:id — rename, share with the team, or pin.
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
        .map_err(|e| internal("chat thread", e))?
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
    if let Some(pinned) = body.pinned {
        if pinned && !t.pinned {
            let all: Vec<Thread> = query(&state, &claims.team_id, "BRAIN#T#")
                .await
                .map_err(|e| internal("chat threads", e))?;
            if !may_pin(&all, &claims.sub, &t.id) {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    &format!("You can pin up to {MAX_PINNED} chats. Unpin one first."),
                ));
            }
        }
        t.pinned = pinned;
    }
    put(&state, &claims.team_id, &thread_sk(&t.id), &t)
        .await
        .map_err(|e| internal("chat thread save", e))?;
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
        .map_err(|e| internal("chat thread", e))?
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
        .map_err(|e| internal("chat messages", e))?;
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
            .map_err(|e| internal("chat delete", e.to_string()))?;
    }
    Ok(Json(json!({ "ok": true })))
}

// ── What can be picked ──────────────────────────────────────────────────────

/// The team's enabled repositories: name, default branch, and the role
/// (and description, when one is stored) the team gave each. Every member
/// who may ask reads every enabled repository; Coderhelm has no narrower
/// repository access to honor.
async fn load_repos(state: &AppState, team_id: &str) -> Vec<RepoInfo> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S("REPO#".into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(r) = r else {
            warn!(team_id, "Chat: couldn't read the team's repositories");
            break;
        };
        for item in r.items() {
            let s = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
            };
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let Some(name) = s("repo_name").filter(|n| n.contains('/')) else {
                continue;
            };
            if enabled {
                out.push(RepoInfo {
                    branch: s("default_branch").unwrap_or_else(|| "main".into()),
                    role: s("role"),
                    description: s("description"),
                    name,
                });
            }
        }
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() && out.len() < 500 => start = Some(k.clone()),
            _ => break,
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Repository names to default branches.
fn branches(repos: &[RepoInfo]) -> HashMap<String, String> {
    repos
        .iter()
        .map(|r| (r.name.clone(), r.branch.clone()))
        .collect()
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
        .filter(|b| {
            b.readable_by(super::knowledge::reader_role(&claims.holder()))
                && b.open_to(&super::knowledge::person(&claims))
        })
    {
        let files = st.files(&claims.team_id, &b.id).await.unwrap_or_default();
        let ready = files.iter().filter(|f| f.ready_version > 0).count();
        let sources = super::knowledge_sources::sources_of(&state, &claims.team_id, &b.id).await;
        let kinds = super::knowledge_sources::kinds(&files, &sources);
        knowledge.push(json!({
            "id": b.id, "name": b.name, "ready": ready, "authoritative": b.authoritative,
            // What the base holds: `upload` and each connected app (`notion`).
            "kind": kinds.iter().find(|k| *k != "upload").cloned().unwrap_or_else(|| "upload".into()),
            "kinds": kinds,
        }));
    }
    knowledge.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let remote = super::mcp_servers::runtime(&state, &claims.team_id)
        .usable(Some(&claims.email))
        .await;
    let mut connections: Vec<Value> = remote
        .iter()
        .map(|(r, _)| json!({ "id": r.id, "name": r.name }))
        .collect();
    connections.extend(
        plugins
            .iter()
            .filter(|(id, creds, _)| *creds && !remote.iter().any(|(r, _)| &r.id == id))
            .map(|(id, _, _)| json!({ "id": id })),
    );
    let repo_names: Vec<&String> = repos.iter().map(|r| &r.name).collect();
    let plugins = common::plugins::brain::sources(&plugin_kv(&state), &claims.team_id).await;
    let kinds: Vec<&SourceKind> = SOURCE_KINDS
        .iter()
        .filter(|k| k.key != common::plugins::brain::SOURCE_KEY || !plugins.is_empty())
        .collect();
    Ok(Json(json!({
        "kinds": kinds,
        "plugins": plugins,
        "knowledge": knowledge,
        "connections": connections,
        "repos": repo_names,
        // The repositories with what each is for, for @ mentions.
        "repositories": repos.iter().map(|r| json!({
            "name": r.name, "short": r.short(), "role": r.role,
        })).collect::<Vec<_>>(),
        "can_ask": claims.holder().holds("brain_ask"),
    })))
}

// ── Starter questions ───────────────────────────────────────────────────────

/// A recent run, as starter questions use it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecentRun {
    pub title: String,
    pub repo: String,
    pub id: String,
    pub status: String,
}

/// A question the empty chat offers: the words, where they come from, and
/// the sources it turns on when picked.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Starter {
    pub question: String,
    pub why: String,
    pub sources: Picks,
    /// Runs the question is about (as `run` mentions), sent back with it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<Mention>,
}

/// How many starter questions the empty chat shows.
pub const STARTERS: usize = 4;

fn short_repo(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

/// At most `max` characters, cut at a word with an ellipsis.
fn clip(text: &str, max: usize) -> String {
    let text = text.trim();
    if text.chars().count() <= max {
        return text.to_string();
    }
    let cut: String = text.chars().take(max).collect();
    let cut = cut.rsplit_once(' ').map_or(cut.as_str(), |(head, _)| head);
    format!("{}…", cut.trim_end_matches([',', '.', ':', ';', '-']))
}

/// Starter questions from the team's own data: a knowledge base the person
/// may read (`(id, name)`, ready ones first), a recent run's work in its
/// repository, how a repository is laid out, and what was learned from past
/// work. Each one turns on exactly the sources that can answer it. Nothing
/// is invented: with no knowledge, repositories or runs the list is empty.
pub fn starter_questions(
    bases: &[(String, String)],
    repos: &[String],
    runs: &[RecentRun],
) -> Vec<Starter> {
    let mut out: Vec<Starter> = Vec::new();
    let push = |s: Starter, out: &mut Vec<Starter>| {
        if out.len() < STARTERS && !out.iter().any(|o| o.question == s.question) {
            out.push(s);
        }
    };
    let base_q = |(id, name): &(String, String)| Starter {
        question: format!("What should I know from {}?", clip(name, 60)),
        why: format!("From {}", clip(name, 60)),
        sources: Picks {
            knowledge: vec![id.clone()],
            ..Default::default()
        },
        mentions: Vec::new(),
    };
    let code = |repo: &str| Picks {
        code: true,
        repo: Some(repo.to_string()),
        ..Default::default()
    };
    let run_qs: Vec<Starter> = runs
        .iter()
        .filter(|r| !r.title.trim().is_empty() && repos.contains(&r.repo))
        .map(|r| Starter {
            question: format!("What's the latest on “{}”?", clip(&r.title, 70)),
            why: format!("From recent work in {}", short_repo(&r.repo)),
            // Where it stands is in the ticket, its pull requests and runs;
            // the code is context.
            sources: Picks {
                work: true,
                ..code(&r.repo)
            },
            mentions: Vec::new(),
        })
        .collect();
    // The repository people work in most lately leads.
    let main_repo = runs
        .iter()
        .map(|r| &r.repo)
        .find(|r| repos.contains(r))
        .or(repos.first());

    // Runs waiting on an answer lead: they are what someone has to act on.
    let waiting: Vec<&RecentRun> = runs
        .iter()
        .filter(|r| r.status == common::run_status::NEEDS_INPUT && valid_run_id(&r.id))
        .take(MAX_RUN_CONTEXT)
        .collect();
    if !waiting.is_empty() {
        push(
            Starter {
                question: "What needs my answer?".into(),
                why: if waiting.len() == 1 {
                    "1 run is waiting on an answer".into()
                } else {
                    format!("{} runs are waiting on an answer", waiting.len())
                },
                sources: Picks::default(),
                mentions: waiting
                    .iter()
                    .map(|r| Mention {
                        kind: "run".into(),
                        id: r.id.clone(),
                    })
                    .collect(),
            },
            &mut out,
        );
    }
    if let Some(b) = bases.first() {
        push(base_q(b), &mut out);
    }
    if let Some(q) = run_qs.first() {
        push(q.clone(), &mut out);
    }
    if let Some(repo) = main_repo {
        push(
            Starter {
                question: format!("How is {} put together?", short_repo(repo)),
                why: format!("From the {} code", short_repo(repo)),
                sources: code(repo),
                mentions: Vec::new(),
            },
            &mut out,
        );
        push(
            Starter {
                question: format!(
                    "What should a new teammate know about {}?",
                    short_repo(repo)
                ),
                why: format!("From past work in {}", short_repo(repo)),
                sources: Picks {
                    memories: true,
                    repo: Some(repo.clone()),
                    ..Default::default()
                },
                mentions: Vec::new(),
            },
            &mut out,
        );
    }
    // Fill what is left with more of the same kinds.
    for b in bases.iter().skip(1) {
        push(base_q(b), &mut out);
    }
    for q in run_qs.into_iter().skip(1) {
        push(q, &mut out);
    }
    out
}

/// The team's latest run titles and repositories, newest first.
async fn recent_runs(state: &AppState, team_id: &str) -> Vec<RecentRun> {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", AttributeValue::S(team_id.to_string()))
        .projection_expression("#t, #r, run_id, #s")
        .expression_attribute_names("#t", "title")
        .expression_attribute_names("#r", "repo")
        .expression_attribute_names("#s", "status")
        .scan_index_forward(false)
        .limit(25)
        .send()
        .await;
    match r {
        Ok(r) => r
            .items()
            .iter()
            .map(|item| {
                let s = |k: &str| {
                    item.get(k)
                        .and_then(|v| v.as_s().ok())
                        .cloned()
                        .unwrap_or_default()
                };
                RecentRun {
                    title: s("title"),
                    repo: s("repo"),
                    id: s("run_id"),
                    status: s("status"),
                }
            })
            .collect(),
        Err(e) => {
            warn!(error = %e, "Chat starters: couldn't read recent runs");
            Vec::new()
        }
    }
}

/// GET /api/brain/starters — questions for the empty chat, built from the
/// knowledge this person may read, the team's repositories and recent runs.
pub async fn starters(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = super::knowledge::store(&state);
    let (bases, repos, runs) = tokio::join!(
        st.bases(&claims.team_id),
        load_repos(&state, &claims.team_id),
        recent_runs(&state, &claims.team_id),
    );
    let me = super::knowledge::person(&claims);
    let mut readable: Vec<_> = super::knowledge::chat_readable(
        bases.map_err(|e| internal("knowledge bases", e))?,
        &claims.holder(),
        &me,
    );
    readable.sort_by(|a, b| a.name.cmp(&b.name));
    // Bases with something to read come first; at most a few are checked.
    let mut ready = Vec::new();
    for b in readable.into_iter().take(6) {
        let files = st.files(&claims.team_id, &b.id).await.unwrap_or_default();
        if files.iter().any(|f| f.ready_version > 0) {
            ready.push((b.id, b.name));
        }
    }
    let repo_names: Vec<String> = repos.into_iter().map(|r| r.name).collect();
    Ok(Json(json!({
        "starters": starter_questions(&ready, &repo_names, &runs),
    })))
}

// ── Feedback on answers ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct FeedbackBody {
    /// `up`, `down`, or null to clear.
    #[serde(default)]
    pub rating: Option<String>,
}

/// Is `rating` one an answer may carry (`None` clears it)?
pub fn valid_rating(rating: Option<&str>) -> bool {
    matches!(rating, None | Some("up") | Some("down"))
}

/// PUT /api/brain/threads/:id/messages/:seq/feedback — the asker rates an
/// answer up or down (or clears the rating).
pub async fn rate_message(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((id, seq)): Path<(String, u32)>,
    Json(body): Json<FeedbackBody>,
) -> Reply {
    if claims.require_role(1).is_err() {
        return Err(fail(StatusCode::FORBIDDEN, "Viewers can't rate answers."));
    }
    if !valid_rating(body.rating.as_deref()) {
        return Err(fail(StatusCode::BAD_REQUEST, "Rate an answer up or down."));
    }
    let t = get_thread_record(&state, &claims.team_id, &id)
        .await
        .map_err(|e| internal("chat thread", e))?
        .filter(|t| can_read(t, &claims.sub))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
    if !can_write(&t, &claims.sub) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the person who asked can rate its answers.",
        ));
    }
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(message_sk(&t.id, seq)))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| internal("chat message", e.to_string()))?;
    let mut m = r
        .item()
        .and_then(read_data::<Message>)
        .filter(|m| m.role == "assistant")
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such answer."))?;
    m.feedback = body.rating;
    put(&state, &claims.team_id, &message_sk(&t.id, seq), &m)
        .await
        .map_err(|e| internal("chat message save", e))?;
    // A rated answer from named knowledge bases counts in their usage (a
    // thumbs down lists the question under "no good answer").
    if let Some(r) = m.feedback.as_deref() {
        let asked = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", AttributeValue::S(claims.team_id.clone()))
            .key(
                "sk",
                AttributeValue::S(message_sk(&t.id, seq.saturating_sub(1))),
            )
            .send()
            .await
            .ok()
            .and_then(|o| o.item().and_then(read_data::<Message>));
        if let Some(q) = asked {
            use common::knowledge::usage;
            let kbs: Vec<String> = q
                .mentions
                .iter()
                .filter(|x| x.kind == "knowledge")
                .map(|x| x.id.clone())
                .collect();
            let evs = usage::events(
                &kbs,
                if t.origin == ORIGIN_KB_PAGE {
                    usage::surface::PAGE
                } else {
                    usage::surface::CHAT
                },
                &claims.email,
                &t.id,
                &q.content,
                &[],
                &[],
                if r == "down" {
                    usage::outcome::RATED_DOWN
                } else {
                    usage::outcome::RATED_UP
                },
                &now(),
            );
            usage::log(
                &state.dynamo,
                &state.config.settings_table_name,
                &claims.team_id,
                &evs,
            )
            .await;
        }
    }
    info!(
        team_id = %claims.team_id,
        thread = %t.id,
        seq,
        rating = m.feedback.as_deref().unwrap_or("none"),
        grounded = m.grounded,
        "Chat: answer rated"
    );
    Ok(Json(json!({ "ok": true, "feedback": m.feedback })))
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
            "Your role can't ask questions.",
        ));
    }
    let token = crate::auth::jwt::create_scoped_token(
        &claims,
        crate::auth::jwt::BRAIN_AUDIENCE,
        &state.secrets.jwt_secret,
        300,
    )
    .map_err(|e| internal("chat token", e.to_string()))?;
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
        return Err(fail(StatusCode::FORBIDDEN, "Viewers can't ask questions."));
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

const CHAT_SYSTEM: &str = r#"You are Chat, the assistant for a software team that uses Coderhelm. You answer questions about the team's work from the team's own sources: their code (which you read with your tools), passages from their knowledge bases, files attached to the question, and the results of tools for connected apps, past work, data sources and team skills.

How to work:
- Research before you answer. You can take many steps: look a repository over, list folders, read files, search the code, follow imports, read more. Keep going until you can answer from what you read. Most code questions need several reads.
- The team's repositories are listed below. When the question names one, by its full or short name however it is written, it means that repository. Open it with github__repo_overview; don't search the code or the code graph for the repository's own name.
- "How is X put together?" (built, structured, organized): look it over, then read the entry points and the files that tie it together (app or server setup, routing, main modules, configuration). Explain the main parts, what each does, and how a request or a job moves through them.
- "Where is X handled?": search the code for the words and the likely names, read the matching files to confirm, and name the file and the function.
- "What does file Y do?": read it (in ranges when it is long) and the files it leans on when that matters.
- Never hand work back. Don't ask the person to open, read, paste or attach a file, to say which repository or who owns it, or to look something up: your tools do that. When a tool fails, try another way (another path, a search, the file listing). Say something isn't there only after you looked, and say where you looked.
- Ask the person only for what no tool can reach, such as a decision or a preference.
- The team's own work lives in the work tools (work_tickets, work_pull_requests, work_runs, work_findings, work_records), not in the code or the docs. "What's the latest on X", "where are we on", "status of", a ticket key, a pull request number, a run, "what's running", a CVE id or a package, a quoted title: look it up there first (the ticket, then its pull requests and Coderhelm's runs on it), then read code or docs only for context. Give a dated status: say when each record last changed. Never say there's nothing on it before you searched the tickets, pull requests and runs.
- Don't guess a field, column, type or property name. Before you query a data source, a CMS or a database, read its schema (its describe, schema or mapping tool) and query with the names it lists.

Citations:
- Ground every claim. After each sentence that uses a source, cite it with its id in square brackets, like [S2] or [S1, S4]. Cite only ids you were given in <source> blocks. Never invent an id.
- Answer from what the sources say, not from general knowledge. Never make up names, paths, numbers, dates, links or policies.
- When sources disagree, prefer the one marked source_of_truth, then the newer one. Say that they disagree and cite both.

How to write:
- Lead with the answer in a sentence or two, then the details. Use a short list or a few short sections when it helps; no headings for a short answer.
- Name real files and functions in backticks, like `src/app/page.tsx`. Quote code only when it helps: a short fenced block with its language and the file's path as its title, like ```ts title="src/app/page.tsx"
- Write for the whole team, in plain words; explain a technical term in a few words the first time."#;

/// The system prompt: how to work, and the repositories this question may
/// read (with what each is for).
pub fn system_prompt(repos: &[RepoInfo], can_read_code: bool) -> String {
    let mut s = CHAT_SYSTEM.to_string();
    if repos.is_empty() {
        return s;
    }
    s.push_str(
        "\n\nThe team's repositories you may read (owner/name, default branch, what it is for):",
    );
    for r in repos {
        s.push_str(&format!("\n- {} ({})", r.name, r.branch));
        if let Some(role) = r.role.as_deref() {
            s.push_str(&format!(", the {role} repository"));
        }
        if let Some(d) = r.description.as_deref() {
            s.push_str(&format!(": {}", common::truncate_str(d, 200)));
        }
    }
    if !can_read_code {
        s.push_str(
            "\n\nCoderhelm can't read these repositories right now (GitHub didn't give it access). \
             If the question needs the code, say that plainly and that an admin can check the GitHub \
             connection in Settings; don't ask the person to open or paste files.",
        );
    }
    s
}

/// What one question carries.
#[derive(Debug, Deserialize)]
pub struct AskBody {
    #[serde(default)]
    pub thread_id: Option<String>,
    pub question: String,
    /// What to read, from an older client. Left out, Chat reads everything
    /// the asker can see (narrowed by `mentions`).
    #[serde(default)]
    pub sources: Option<Picks>,
    /// Repositories and knowledge bases the asker mentioned with @.
    #[serde(default)]
    pub mentions: Vec<Mention>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
    /// `kb_page`: asked from a knowledge base page (counted against the
    /// page's hourly limit, and marked on a new thread).
    #[serde(default)]
    pub origin: Option<String>,
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
    /// The asker: remote connections run as them.
    user_email: String,
    api_key: String,
    model_id: String,
    /// The lighter model, for the answer check.
    check_model_id: String,
    picks: Picks,
    /// Knowledge bases the asker may read, among those picked.
    kb_ids: Vec<String>,
    /// Every enabled repository to its default branch.
    repos: HashMap<String, String>,
    /// The repositories code may be read in for this question.
    code_repos: Vec<RepoInfo>,
    github_token: Option<String>,
    mcp_tools: Vec<Value>,
    /// Active plugins among those picked.
    plugin_ids: Vec<String>,
    worker: WorkerTools,
    scope: ChatScope,
    /// Connections the asker can't use yet (offered as a sign-in card).
    missing: Vec<super::chat_policy::Missing>,
    is_admin: bool,
    /// What the runs the question is about are doing (from `run` mentions).
    run_note: String,
    /// What the knowledge searches found (`(base, item, title)`), for the
    /// bases' usage.
    found: std::sync::Mutex<Vec<(String, String, String)>>,
    /// The team's work (tickets, pull requests, runs, findings) as this
    /// asker may read it; `None` when the question doesn't read it.
    work: Option<super::chat_work::Ctx>,
}

impl Run {
    fn note_found(&self, hits: &[common::knowledge::search::Hit]) {
        if let Ok(mut f) = self.found.lock() {
            f.extend(
                hits.iter()
                    .map(|h| (h.kb_id.clone(), h.file_id.clone(), h.file_name.clone())),
            );
        }
    }
}

/// Most runs one question carries as context.
pub const MAX_RUN_CONTEXT: usize = 5;

/// A run id as the runs table writes them.
pub fn valid_run_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The run ids a question mentions, checked and at most a few.
pub fn mentioned_runs(mentions: &[Mention]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for m in mentions.iter().filter(|m| m.kind == "run") {
        if valid_run_id(&m.id) && !out.contains(&m.id) && out.len() < MAX_RUN_CONTEXT {
            out.push(m.id.clone());
        }
    }
    out
}

/// One run, as a question's context: what it is, where it stands, and what
/// it asked when it waits on an answer.
pub fn run_context_line(item: &HashMap<String, AttributeValue>) -> String {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let mut line = format!(
        "- Run {}: \"{}\" in {}, status {}",
        s("run_id"),
        clip(&s("title"), 160),
        s("repo"),
        s("status")
    );
    let pr = s("pr_url");
    if !pr.is_empty() {
        line.push_str(&format!(", pull request {pr}"));
    }
    let err = Some(s("error_message"))
        .filter(|e| !e.is_empty())
        .unwrap_or_else(|| s("error"));
    if !err.is_empty() {
        line.push_str(&format!(". It stopped with: {}", clip(&err, 400)));
    }
    let asked: Option<common::run_questions::Asked> = item
        .get(common::run_questions::FIELD_QUESTIONS)
        .and_then(|v| v.as_s().ok())
        .and_then(|j| serde_json::from_str(j).ok());
    if let Some(a) = asked.filter(|a| !a.questions.is_empty()) {
        line.push_str(&format!(
            ". It is waiting on answers to:\n{}",
            common::run_questions::plain(&a.questions)
        ));
    }
    line.push('\n');
    line
}

/// The context note for the runs a question is about, read from this team's
/// runs only (another team's id finds nothing).
async fn run_note(state: &AppState, team_id: &str, ids: &[String]) -> String {
    let mut lines = String::new();
    for id in ids {
        let r = state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", AttributeValue::S(team_id.to_string()))
            .key("run_id", AttributeValue::S(id.clone()))
            .send()
            .await;
        match r {
            Ok(r) => {
                if let Some(item) = r.item() {
                    lines.push_str(&run_context_line(item));
                }
            }
            Err(e) => warn!(error = %e, "Chat: couldn't read a run for context"),
        }
    }
    if lines.is_empty() {
        return String::new();
    }
    format!("\nThe question is about these runs of the team (their current state):\n{lines}")
}

/// The tools this question may call.
fn tool_defs(run: &Run) -> Vec<Value> {
    let mut tools: Vec<Value> = Vec::new();
    if run.picks.code && run.github_token.is_some() {
        tools.extend(chat_code::defs(&run.code_repos));
    }
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
    if run.picks.connections {
        tools.extend(
            run.mcp_tools
                .iter()
                .filter(|t| !chat_code::is_code_tool(t["name"].as_str().unwrap_or("")))
                .cloned(),
        );
    }
    if !run.plugin_ids.is_empty() {
        tools.push(common::plugins::brain::ask_tool(&run.plugin_ids));
    }
    if let Some(w) = run.work.as_ref().filter(|_| run.picks.work) {
        tools.extend(super::chat_work::defs(
            &w.readable,
            &w.repos,
            w.github_token.is_some(),
        ));
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
        n if chat_code::is_code_tool(n) => chat_code::label(n, input),
        KNOWLEDGE_TOOL => format!(
            "Searched knowledge for \u{201c}{}\u{201d}",
            common::truncate_str(&s("query"), 60)
        ),
        MEMORY_TOOL => format!("Searched past work in {}", short_repo(&s("repo"))),
        common::plugins::brain::ASK_TOOL => format!("Asked the {} plugin", s("plugin")),
        n if super::chat_work::is_work_tool(n) => super::chat_work::label(n, input),
        n if n.starts_with("graph_") => chat_code::graph_label(n, input),
        n if n.contains("__") => {
            let (server, tool) = n.split_once("__").unwrap_or((n, ""));
            format!("{server}: {}", tool.replace('_', " "))
        }
        n => n.replace('_', " "),
    }
}

/// The plugins store (settings table), for the plugins source.
fn plugin_kv(state: &AppState) -> common::plugins::kv::Dynamo {
    common::plugins::kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

/// The picked plugins that are installed and active (flag on).
async fn active_plugins(state: &AppState, team_id: &str, picked: &[String]) -> Vec<String> {
    if picked.is_empty() {
        return Vec::new();
    }
    common::plugins::brain::sources(&plugin_kv(state), team_id)
        .await
        .into_iter()
        .filter_map(|v| v["id"].as_str().map(str::to_string))
        .filter(|id| picked.iter().any(|p| p == id))
        .collect()
}

fn live_stamp() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string()
}

/// A code graph answer as sources, one per file it names (with the line,
/// linked on the default branch). Returns the source ids.
fn graph_sources(
    run: &Run,
    reg: &mut Registry,
    name: &str,
    input: &Value,
    text: &str,
) -> Vec<String> {
    let repo = input["repo"]
        .as_str()
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .unwrap_or(&run.scope.repo)
        .to_string();
    let branch = run
        .repos
        .get(&repo)
        .cloned()
        .unwrap_or_else(|| "HEAD".into());
    let places = chat_code::graph_places(text);
    if places.is_empty() {
        return vec![reg.add(SourceRef {
            kind: "code".into(),
            title: tool_label(name, input),
            loc: short_repo(&repo).to_string(),
            as_of: live_stamp(),
            passage: text.to_string(),
            ..Default::default()
        })];
    }
    places
        .into_iter()
        .take(12)
        .map(|(path, line)| {
            let passage = text
                .lines()
                .filter(|l| l.contains(&path))
                .take(4)
                .collect::<Vec<_>>()
                .join("\n");
            reg.add(SourceRef {
                kind: "code".into(),
                loc: if line > 0 {
                    format!("{} · line {line}", short_repo(&repo))
                } else {
                    short_repo(&repo).to_string()
                },
                url: Some(common::repo_read::blob_url(
                    &repo, &branch, &path, line, line,
                )),
                code: Some(CodeRef {
                    repo: repo.clone(),
                    path: path.clone(),
                    commit: branch.clone(),
                    start_line: line,
                    end_line: line,
                }),
                title: path,
                passage,
                ..Default::default()
            })
        })
        .collect()
}

/// Runs one tool other than the code tools and registers what it read.
/// The connected knowledge bases that didn't answer, for the model to say
/// so instead of claiming they hold nothing.
fn unreached_note(unreached: &[common::knowledge::search::Unreached]) -> String {
    unreached
        .iter()
        .map(|u| format!("\n(Couldn't search {} this time: {})", u.kb_name, u.reason))
        .collect()
}

/// Returns the text the model reads, a short summary for the dashboard, and
/// whether it failed.
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
            "That source isn't part of this question.".into(),
            "Not allowed".into(),
            true,
        );
    }
    let state = &run.state;
    if super::chat_work::is_work_tool(name) {
        return match &run.work {
            Some(w) => Box::pin(super::chat_work::call(w, reg, name, input)).await,
            None => (
                "That source isn't part of this question.".into(),
                "Not allowed".into(),
                true,
            ),
        };
    }
    match name {
        KNOWLEDGE_TOOL => {
            let q = input["query"].as_str().unwrap_or("");
            let meter = crate::model_call::chat_meter(state, &run.team_id);
            let titan = Titan {
                client: &state.bedrock,
                meter: &meter,
            };
            let st = super::knowledge::store(state);
            let servers = super::knowledge::servers(state, &run.team_id);
            match search(
                &st,
                &titan,
                &super::knowledge::retriever(state, "brain", &servers, Some(&run.user_email)),
                &run.team_id,
                &run.kb_ids,
                q,
                6,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(found) if !found.hits.is_empty() => {
                    run.note_found(&found.hits);
                    super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                    let ids = reg.add_hits(&found.hits);
                    (
                        format!("{}{}", reg.render(&ids), unreached_note(&found.unreached)),
                        format!("{} passages", ids.len()),
                        false,
                    )
                }
                Ok(found) => {
                    super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                    (
                        format!("No passages matched.{}", unreached_note(&found.unreached)),
                        "No passages matched".into(),
                        false,
                    )
                }
                Err(e) => {
                    warn!(error = %e, "Chat knowledge search failed");
                    (
                        "Error: the knowledge bases couldn't be searched.".into(),
                        "Search failed".into(),
                        true,
                    )
                }
            }
        }
        common::plugins::brain::ASK_TOOL => {
            let plugin = input["plugin"].as_str().unwrap_or("");
            if !run.plugin_ids.iter().any(|p| p == plugin) {
                return (
                    "Error: pick one of the listed plugins.".into(),
                    "Unknown plugin".into(),
                    true,
                );
            }
            // Read only: the plugin's own skills (no draft, no tool call).
            match common::plugins::brain::context(&plugin_kv(state), &run.team_id, plugin).await {
                Ok(text) => {
                    let id = reg.add(SourceRef {
                        kind: "plugin".into(),
                        title: format!("Plugin {plugin}"),
                        loc: plugin.to_string(),
                        as_of: live_stamp(),
                        passage: text,
                        href: Some(format!("/plugins/plugin?id={plugin}")),
                        ..Default::default()
                    });
                    (reg.render(&[id]), "Read the plugin's skills".into(), false)
                }
                Err(e) => (format!("Error: {e}"), "Plugin unavailable".into(), true),
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
                                title: format!("Past work in {}", short_repo(repo)),
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
                    (reg.render(&ids), format!("{} notes", ids.len()), false)
                }
                Ok(_) => (
                    "Nothing from past work matched.".into(),
                    "Nothing matched".into(),
                    false,
                ),
                Err(e) => {
                    warn!(error = %e, "Chat memory search failed");
                    (
                        "Error: past work couldn't be read.".into(),
                        "Couldn't read".into(),
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
                match invoke_mcp_tool(state, &run.team_id, &run.user_email, server, tool, input)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => json!(format!("Error: {e}")),
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
            if name.starts_with("graph_") {
                let text = result.as_str().unwrap_or("").to_string();
                let ids = graph_sources(run, reg, name, input, &text);
                let files = ids.len();
                return (
                    format!(
                        "{}\nThe graph is an index: read the files you rely on before you answer from them.",
                        reg.render(&ids)
                    ),
                    format!("{files} files"),
                    false,
                );
            }
            let passage = compact_tool_result_for_model(&result);
            let id = reg.add(SourceRef {
                kind: match kind {
                    "connections" => "connection",
                    "data_sources" => "data_source",
                    "skills" => "skill",
                    other => other,
                }
                .into(),
                title: tool_label(name, input),
                loc: input["repo"].as_str().unwrap_or("").to_string(),
                as_of: live_stamp(),
                passage,
                ..Default::default()
            });
            (reg.render(&[id]), summarize_tool_result(&result), false)
        }
    }
}

/// Chat's tools behind the agent permission gate: every call is asked of
/// the gate first, a connection the asker can't use yet is offered as a
/// sign-in card, and the code tools read through one [`CodeReader`].
struct BrainTools<'a> {
    run: &'a Run,
    reader: Option<CodeReader<'a>>,
    gate: common::agent_policy::Gate,
    defs: Vec<Value>,
    used: Used,
    /// Tools called so far, for the schema-first check.
    called: Vec<String>,
}

/// A query tool and the tool that reads its source's schema: a query before
/// the schema is read is answered with "read the schema first", once.
const SCHEMA_FIRST: &[(&str, &str)] = &[
    ("live_lookup", "live_describe"),
    ("db_query", "live_describe"),
    ("opensearch_search", "opensearch_mapping"),
];

/// The note for a query sent before its schema was read; `None` to run it.
pub fn schema_first(name: &str, called: &[String], offered: &[Value]) -> Option<String> {
    let (_, schema) = SCHEMA_FIRST.iter().find(|(q, _)| *q == name)?;
    let has = offered.iter().any(|t| t["name"] == *schema);
    let read = called.iter().any(|c| c == schema);
    // Once: a second try runs (the model may know the names from before).
    let tried = called.iter().any(|c| c == name);
    (has && !read && !tried).then(|| {
        format!(
            "Not run: read the source's schema first with {schema}, then query with the field, \
             column and type names it lists. Don't guess names."
        )
    })
}

#[async_trait::async_trait]
impl Toolbox for BrainTools<'_> {
    fn defs(&self) -> &[Value] {
        &self.defs
    }

    async fn call(
        &mut self,
        reg: &mut Registry,
        sink: &dyn Sink,
        name: &str,
        input: &Value,
    ) -> Called {
        let label = tool_label(name, input);
        let kind = kind_of_tool(name).to_string();
        if let Some(note) = schema_first(name, &self.called, &self.defs) {
            self.called.push(name.to_string());
            return Called::Done {
                text: note,
                label,
                summary: "Schema first".into(),
                is_error: true,
                kind,
            };
        }
        self.called.push(name.to_string());
        let verdict = if name == super::chat_policy::SIGN_IN_TOOL {
            let (text, event) = super::chat_policy::sign_in(
                &self.run.state,
                &self.run.team_id,
                &self.gate.ctx.thread_id,
                &self.run.missing,
                self.run.is_admin,
                input,
            )
            .await;
            super::chat_policy::Checked::Answer { text, event }
        } else {
            super::chat_policy::check(&self.gate, name, input).await
        };
        let (text, summary, is_error) = match verdict {
            super::chat_policy::Checked::Run if chat_code::is_code_tool(name) => {
                match (&self.reader, self.run.picks.allows("code")) {
                    (Some(reader), true) => {
                        let o = chat_code::run(reader, reg, name, input).await;
                        (o.text, o.summary, o.is_error)
                    }
                    _ => (
                        "Error: Coderhelm can't read the code for this question.".into(),
                        "Couldn't read".into(),
                        true,
                    ),
                }
            }
            super::chat_policy::Checked::Run => {
                run_tool(self.run, reg, &mut self.used, name, input).await
            }
            super::chat_policy::Checked::Answer { text, event } => {
                if let Some((k, v)) = event {
                    sink.send(&k, v).await;
                }
                (text, "Not run".to_string(), true)
            }
            super::chat_policy::Checked::Stop(note) => return Called::Stop(note),
        };
        Called::Done {
            text,
            label,
            summary,
            is_error,
            kind,
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

/// The team's model key, the model answers are written with (the heavy
/// model, like the plan chat) and the lighter one for the answer check.
pub(crate) async fn load_provider(
    state: &AppState,
    team_id: &str,
) -> (Option<String>, String, String) {
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
    let field = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .map(|s| common::model_ids::current(s).to_string())
    };
    let key = item
        .as_ref()
        .and_then(|i| i.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned();
    let primary = field("primary_model");
    let model = field("heavy_model")
        .or_else(|| primary.clone())
        .unwrap_or_else(|| "claude-sonnet-5-5".to_string());
    let check = primary.unwrap_or_else(|| model.clone());
    (key, model, check)
}

/// The team's GitHub App token (either table's spelling of the
/// installation, as every other reader of it does).
pub(crate) async fn github_token(state: &AppState, team_id: &str) -> Option<String> {
    let id = super::graph::installation_id(state, team_id).await?;
    crate::auth::github_app::get_installation_token(state, id)
        .await
        .inspect_err(|e| warn!(error = %e, "Chat: no GitHub token"))
        .ok()
}

/// The connections' tools, named `server__tool`: the remote MCP servers
/// this person may use (the team's shared ones and their own connections,
/// allowed tools only, as in the plan chat), and the catalog servers with a
/// saved key (cached schemas). A remote connection takes the place of the
/// same catalog server's key.
async fn connection_tools(state: &AppState, team_id: &str, user_email: &str) -> Vec<Value> {
    let remote = super::mcp_servers::runtime(state, team_id)
        .usable(Some(user_email))
        .await;
    let mut out = Vec::new();
    for (r, _) in &remote {
        for (name, description, input_schema) in common::mcp_remote::tools::agent_tools(r, false) {
            out.push(
                json!({ "name": name, "description": description, "input_schema": input_schema }),
            );
        }
    }
    if state.config.mcp_proxy_function_name.is_empty() {
        return out;
    }
    let plugins = load_enabled_plugins(state, team_id).await;
    for (server, creds, _) in plugins {
        if !creds || remote.iter().any(|(r, _)| r.id == server) {
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
/// `step`, `text_delta`, `text_reset`, `answer`, `done`, or `error`).
/// Authorized by the short-lived token from `/api/brain/token` (its own
/// audience); what the asker holds is read again here, so the stream is
/// decided by capability like every other route.
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
    let token = crate::auth::jwt::stream_token(&headers)
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
            "Your role can read shared chats but can't ask questions.",
        ));
    }
    let me = super::knowledge::person(&claims);
    claims.role = holder.floor().to_string();
    let question = body.question.trim().to_string();
    if question.is_empty() || question.chars().count() > MAX_QUESTION_CHARS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Ask a question under 8,000 characters.",
        ));
    }

    let from_page = body.origin.as_deref() == Some(ORIGIN_KB_PAGE);
    if from_page {
        super::knowledge_ask::count_ask(&state, &claims.team_id, &claims.email).await?;
    }

    // The thread: the asker's own, or a new one.
    let (mut thread, history) = match body.thread_id.as_deref().filter(|s| !s.is_empty()) {
        Some(id) => {
            let t = get_thread_record(&state, &claims.team_id, id)
                .await
                .map_err(|e| internal("chat thread", e))?
                .filter(|t| can_write(t, &claims.sub))
                .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such thread."))?;
            let mut h: Vec<Message> = query(&state, &claims.team_id, &message_prefix(&t.id))
                .await
                .map_err(|e| internal("chat messages", e))?;
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
                pinned: false,
                created_at: now(),
                updated_at: now(),
                messages: 0,
                origin: if from_page {
                    ORIGIN_KB_PAGE.to_string()
                } else {
                    String::new()
                },
            };
            put(&state, &claims.team_id, &thread_sk(&t.id), &t)
                .await
                .map_err(|e| internal("chat thread save", e))?;
            (t, Vec::new())
        }
    };

    let (api_key, model_id, check_model_id) = load_provider(&state, &claims.team_id).await;
    let st = super::knowledge::store(&state);
    let readable: Vec<String> = super::knowledge::chat_readable(
        st.bases(&claims.team_id).await.unwrap_or_default(),
        &holder,
        &me,
    )
    .into_iter()
    .map(|b| b.id)
    .collect();
    let all_repos = load_repos(&state, &claims.team_id).await;
    let repo_names: Vec<String> = all_repos.iter().map(|r| r.name.clone()).collect();
    let picks = resolve_picks(
        body.sources.as_ref(),
        &body.mentions,
        &readable,
        &repo_names,
    );
    let code_repos: Vec<RepoInfo> = if picks.code {
        all_repos
            .iter()
            .filter(|r| picks.repos.is_empty() || picks.repos.contains(&r.name))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    // The repository skills, data sources and the code graph start from: the
    // one named in the question, else the one picked, else the first.
    let named = common::repo_read::mentioned(&code_repos, &question)
        .first()
        .map(|r| r.name.clone());
    let repo = named
        .or_else(|| picks.repo.clone().filter(|r| repo_names.contains(r)))
        .or_else(|| code_repos.first().map(|r| r.name.clone()))
        .or_else(|| repo_names.first().cloned())
        .unwrap_or_default();
    let repos = branches(&all_repos);
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
            if (picks.code && !code_repos.is_empty()) || (picks.work && !all_repos.is_empty()) {
                github_token(&state, &claims.team_id).await
            } else {
                None
            }
        },
        async {
            if picks.connections {
                connection_tools(&state, &claims.team_id, &claims.email).await
            } else {
                Vec::new()
            }
        },
    );
    // Chosen by Chat: every active plugin; narrowed: the ones picked.
    let plugin_ids = if picks.auto {
        common::plugins::brain::sources(&plugin_kv(&state), &claims.team_id)
            .await
            .into_iter()
            .filter_map(|v| v["id"].as_str().map(str::to_string))
            .collect()
    } else {
        active_plugins(&state, &claims.team_id, &picks.plugins).await
    };
    let work = if picks.work {
        let (jira, installation_id) = tokio::join!(
            super::chat_work_tickets::load(&state, &claims.team_id),
            super::graph::installation_id(&state, &claims.team_id),
        );
        Some(super::chat_work::Ctx {
            state: state.clone(),
            claims: claims.clone(),
            readable: super::chat_work::readable(&holder),
            holder: holder.clone(),
            repos: repo_names.clone(),
            github_token: github.clone(),
            installation_id,
            jira,
        })
    } else {
        None
    };
    // Code reads only when the question reads code.
    let github = github.filter(|_| picks.code && !code_repos.is_empty());
    let run = Run {
        state: state.clone(),
        team_id: claims.team_id.clone(),
        user_email: claims.email.clone(),
        api_key: api_key.unwrap_or_default(),
        model_id,
        check_model_id,
        kb_ids: picks.knowledge.clone(),
        picks,
        repos,
        code_repos,
        github_token: github,
        mcp_tools,
        plugin_ids,
        worker,
        scope,
        missing: super::chat_policy::missing_connections(&state, &claims.team_id, &claims.email)
            .await,
        is_admin: claims.holder().holds("integrations"),
        run_note: run_note(&state, &claims.team_id, &mentioned_runs(&body.mentions)).await,
        found: std::sync::Mutex::new(Vec::new()),
        work,
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(256);
    let user_id = claims.sub.clone();
    let attachments = body.attachments.clone();
    let mentions = body.mentions.clone();
    let origin_page = from_page;
    info!(
        team_id = %claims.team_id,
        thread = %thread.id,
        kbs = run.kb_ids.len(),
        repos = run.code_repos.len(),
        code = run.github_token.is_some(),
        "Chat question"
    );
    tokio::spawn(async move {
        let sink = chat_agent::ChannelSink(tx.clone());
        sink.send("thread", json!({ "id": thread.id, "title": thread.title }))
            .await;
        if run.api_key.is_empty() && !common::model_provider::is_bedrock() {
            sink.send("error", json!({ "message": "Add a model provider key in your AI model settings to use Chat." }))
                .await;
            sink.send("done", json!({})).await;
            return;
        }
        let Some((fin, used)) =
            answer(&run, &sink, &user_id, &question, &history, &attachments).await
        else {
            // The model couldn't be reached: the error was sent, nothing is kept.
            sink.send("done", json!({})).await;
            return;
        };
        sink.send(
            "answer",
            json!({ "text": fin.text, "citations": fin.citations, "grounded": fin.grounded }),
        )
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
            mentions,
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
            error!(error = %e, "Chat answer not saved");
        }
        if !run.kb_ids.is_empty() {
            use common::knowledge::usage;
            let found = run.found.lock().map(|f| f.clone()).unwrap_or_default();
            let cited: Vec<String> = fin
                .citations
                .iter()
                .filter(|c| !c.base.is_empty())
                .map(|c| c.title.clone())
                .collect();
            let outcome = if found.is_empty() {
                usage::outcome::NO_HITS
            } else if fin.grounded {
                usage::outcome::ANSWERED
            } else {
                usage::outcome::NO_SOURCE
            };
            // Only the bases something came from, or the one the page asked.
            let mut kbs: Vec<String> = run
                .kb_ids
                .iter()
                .filter(|k| found.iter().any(|(b, _, _)| b == *k))
                .cloned()
                .collect();
            if kbs.is_empty() && run.kb_ids.len() == 1 {
                kbs = run.kb_ids.clone();
            }
            let evs = usage::events(
                &kbs,
                if origin_page {
                    usage::surface::PAGE
                } else {
                    usage::surface::CHAT
                },
                &run.user_email,
                &thread.id,
                &question,
                &found,
                &cited,
                outcome,
                &now(),
            );
            usage::log(
                &run.state.dynamo,
                &run.state.config.settings_table_name,
                &run.team_id,
                &evs,
            )
            .await;
        }
        sink.send("done", json!({})).await;
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
/// retrieved sources, any notes, and the question.
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

/// A note naming the repositories a question mentions, so the model opens
/// them instead of looking for their names in the code.
pub fn mention_note(repos: &[RepoInfo], question: &str) -> String {
    let named = common::repo_read::mentioned(repos, question);
    if named.is_empty() {
        return String::new();
    }
    let names: Vec<&str> = named.iter().map(|r| r.name.as_str()).collect();
    format!(
        "\nThe question names the team's repository {}. Read it with your code tools.\n",
        names.join(" and ")
    )
}

/// Retrieves, runs the answer loop and finalizes the answer. None when the
/// model couldn't be reached (the error was already sent).
async fn answer(
    run: &Run,
    sink: &chat_agent::ChannelSink,
    user_id: &str,
    question: &str,
    history: &[Message],
    attachments: &[AttachmentRef],
) -> Option<(Final, Vec<Value>)> {
    let mut reg = Registry::default();
    let state = &run.state;

    // 1. Retrieve knowledge first, in code.
    let mut first_ids: Vec<String> = Vec::new();
    if !run.kb_ids.is_empty() {
        let meter = crate::model_call::chat_meter(state, &run.team_id);
        let titan = Titan {
            client: &state.bedrock,
            meter: &meter,
        };
        let st = super::knowledge::store(state);
        let servers = super::knowledge::servers(state, &run.team_id);
        match search(
            &st,
            &titan,
            &super::knowledge::retriever(state, "brain", &servers, Some(&run.user_email)),
            &run.team_id,
            &run.kb_ids,
            &retrieval_query(question, history),
            RETRIEVE_K,
            chrono::Utc::now(),
        )
        .await
        {
            Ok(found) => {
                run.note_found(&found.hits);
                super::knowledge::reembed(state, &run.team_id, &found.stale).await;
                first_ids.extend(reg.add_hits(&found.hits));
            }
            Err(e) => warn!(error = %e, "Chat retrieval failed"),
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
    sink.send(
        "retrieved",
        json!({
            "sources": first_ids.iter().filter_map(|id| reg.get(id)).cloned().collect::<Vec<_>>(),
        }),
    )
    .await;

    let mut tools = tool_defs(run);
    // What the question names puts the tool that holds the answer first.
    let hints = super::chat_hints::read(question);
    super::chat_work::put_first(&mut tools, &hints);
    // Agent permissions: a connection the asker can't use yet is offered as
    // a sign-in card, and every call is asked of the gate first.
    if let Some(t) = super::chat_policy::sign_in_tool(&run.missing) {
        tools.insert(0, t);
    }
    let mut gate = super::chat_policy::gate_for(
        state,
        common::agent_policy::Agent::BrainChat,
        &run.team_id,
        &run.user_email,
        &run.scope.chat_id,
        &run.scope.repo,
        &tools,
    );
    super::chat_policy::offer(&mut gate, &mut tools).await;
    super::chat_policy::settle_connected(state, &run.team_id, &gate.ctx.thread_id, &run.user_email)
        .await;
    // Nothing found and nothing to look up: say so without a model call.
    if reg.is_empty() && tools.is_empty() {
        return Some((finalize("", &reg), Vec::new()));
    }

    let mut notes = mention_note(&run.code_repos, question);
    let offered: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    notes.push_str(&super::chat_work::hint_note(&hints, &offered));
    // A status question's answer is in the tickets, pull requests and runs:
    // an answer that found nothing without looking there goes back once.
    let must_look = hints.about_work().then(|| chat_agent::MustLook {
        tools: super::chat_work::STATUS_TOOLS
            .iter()
            .filter(|t| offered.contains(t))
            .map(|t| t.to_string())
            .collect(),
        what: "the team's tickets, pull requests and Coderhelm's runs".into(),
    });
    notes.push_str(&run.run_note);
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
    let messages = build_messages(history, &reg.render(&first_ids), &notes, question);
    let host_http = common::mcp_remote::http::GuardedHttps;
    let host = run.github_token.as_ref().map(|t| GitHubHost {
        http: &host_http,
        auth: format!("Bearer {t}"),
    });
    let mut toolbox = BrainTools {
        run,
        reader: host
            .as_ref()
            .map(|h| CodeReader::new(h, run.code_repos.clone())),
        gate,
        defs: tools,
        used: Used::default(),
        called: Vec::new(),
    };
    let model = chat_agent::ApiModel {
        http: reqwest::Client::new(),
        api_key: run.api_key.clone(),
        meter: crate::model_call::chat_meter(state, &run.team_id),
        model_id: run.model_id.clone(),
        check_model_id: run.check_model_id.clone(),
        kind_of: kind_of_tool,
    };
    let ask = chat_agent::Ask {
        system: system_prompt(&run.code_repos, run.github_token.is_some()),
        messages,
        question,
        limits: chat_agent::LIMITS,
        must_look,
    };
    let out = chat_agent::run(&model, &mut toolbox, sink, &mut reg, ask)
        .await
        .ok()?;
    info!(
        team_id = %run.team_id,
        calls = out.calls.len(),
        rechecked = out.rechecked,
        grounded = out.fin.grounded,
        cited = out.fin.citations.len(),
        "Chat answered"
    );
    Some((out.fin, out.used))
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
    fn plugins_are_their_own_source() {
        use common::plugins::brain::{ASK_TOOL, SOURCE_KEY};
        assert_eq!(kind_of_tool(ASK_TOOL), SOURCE_KEY);
        assert!(SOURCE_KINDS.iter().any(|k| k.key == SOURCE_KEY));
        let mut picks = Picks::default();
        assert!(!picks.allows(SOURCE_KEY));
        picks.plugins = vec!["marketing".into()];
        assert!(picks.allows(SOURCE_KEY));
        assert!(
            !picks.allows("connections"),
            "picking a plugin turns on nothing else"
        );
        assert_eq!(
            tool_label(ASK_TOOL, &json!({"plugin": "marketing"})),
            "Asked the marketing plugin"
        );
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

    fn run(title: &str, repo: &str) -> RecentRun {
        RecentRun {
            title: title.into(),
            repo: repo.into(),
            id: String::new(),
            status: "completed".into(),
        }
    }

    fn thread_of(owner: &str, id: &str, pinned: bool) -> Thread {
        Thread {
            id: id.into(),
            owner: owner.into(),
            pinned,
            ..Default::default()
        }
    }

    #[test]
    fn a_question_from_a_knowledge_base_page_is_marked_on_its_thread() {
        let body: AskBody = serde_json::from_value(json!({
            "question": "How do we backfill?",
            "mentions": [{"kind": "knowledge", "id": "kb1"}],
            "origin": "kb_page",
        }))
        .unwrap();
        assert_eq!(body.origin.as_deref(), Some(ORIGIN_KB_PAGE));
        let older: AskBody = serde_json::from_value(json!({"question": "q"})).unwrap();
        assert!(older.origin.is_none());
        let t = Thread {
            origin: ORIGIN_KB_PAGE.into(),
            ..thread("u1", false)
        };
        assert_eq!(thread_json(&t, "u1")["origin"], "kb_page");
        let saved: Thread =
            serde_json::from_str(&serde_json::to_string(&thread("u1", false)).unwrap()).unwrap();
        assert!(
            saved.origin.is_empty(),
            "Chat's own threads stay as they were"
        );
    }

    #[test]
    fn up_to_five_chats_are_pinned_per_person() {
        let mut ts: Vec<Thread> = (0..4)
            .map(|i| thread_of("u1", &format!("t{i}"), true))
            .collect();
        ts.push(thread_of("u2", "x", true));
        assert!(may_pin(&ts, "u1", "t9"), "4 pinned: a fifth is allowed");
        ts.push(thread_of("u1", "t4", true));
        assert!(!may_pin(&ts, "u1", "t9"), "5 pinned: a sixth is refused");
        assert!(
            may_pin(&ts, "u1", "t4"),
            "re-pinning one already pinned isn't counted twice"
        );
        assert!(may_pin(&ts, "u2", "y"), "another person's pins don't count");
        // A shared chat shows unpinned to everyone but its owner.
        let t = thread_of("u1", "t0", true);
        assert_eq!(thread_json(&t, "u2")["pinned"], false);
        assert_eq!(thread_json(&t, "u1")["pinned"], true);
    }

    #[test]
    fn waiting_runs_lead_the_starters_and_carry_their_ids() {
        let mut waiting = run("Add a Heading component", "acme/web");
        waiting.id = "01RUNWAITING".into();
        waiting.status = "needs_input".into();
        let repos = vec!["acme/web".to_string()];
        let s = starter_questions(&[], &repos, &[waiting, run("Fix login", "acme/web")]);
        assert_eq!(s[0].question, "What needs my answer?");
        assert_eq!(s[0].why, "1 run is waiting on an answer");
        assert_eq!(
            s[0].mentions,
            vec![Mention {
                kind: "run".into(),
                id: "01RUNWAITING".into()
            }]
        );
        // Without waiting runs there is no such starter.
        let s = starter_questions(&[], &repos, &[run("Fix login", "acme/web")]);
        assert!(s.iter().all(|x| x.question != "What needs my answer?"));
    }

    #[test]
    fn run_mentions_are_checked_capped_and_never_narrow_sources() {
        let m = |kind: &str, id: &str| Mention {
            kind: kind.into(),
            id: id.into(),
        };
        let ms = vec![
            m("run", "01A"),
            m("run", "01A"),
            m("run", "bad id;drop"),
            m("run", "01B"),
            m("repo", "acme/web"),
        ];
        assert_eq!(
            mentioned_runs(&ms),
            vec!["01A".to_string(), "01B".to_string()]
        );
        let many: Vec<Mention> = (0..9).map(|i| m("run", &format!("01R{i}"))).collect();
        assert_eq!(mentioned_runs(&many).len(), MAX_RUN_CONTEXT);
        // A run mention alone leaves Chat reading everything it may.
        let p = resolve_picks(None, &[m("run", "01A")], &[], &["acme/web".to_string()]);
        assert!(p.auto);
    }

    #[test]
    fn a_run_context_line_says_what_it_waits_on() {
        let mut item = HashMap::new();
        let put = |item: &mut HashMap<String, AttributeValue>, k: &str, v: &str| {
            item.insert(k.to_string(), AttributeValue::S(v.to_string()));
        };
        put(&mut item, "run_id", "01RUN");
        put(&mut item, "title", "Add a Heading component");
        put(&mut item, "repo", "acme/web");
        put(&mut item, "status", "needs_input");
        put(
            &mut item,
            "questions",
            r#"{"questions":[{"id":"q1","question":"Should h2 be the default?"}],"asked_at":""}"#,
        );
        let line = run_context_line(&item);
        assert!(line.contains("Run 01RUN"), "{line}");
        assert!(line.contains("status needs_input"), "{line}");
        assert!(line.contains("Should h2 be the default?"), "{line}");
    }

    #[test]
    fn starters_come_from_the_teams_own_data() {
        let bases = vec![
            ("kb1".to_string(), "HR handbook".to_string()),
            ("kb2".to_string(), "Runbooks".to_string()),
        ];
        let repos = vec!["acme/api".to_string(), "acme/web".to_string()];
        let runs = vec![
            run("", "acme/web"),
            run("Add rate limiting to login", "acme/web"),
            run("Old work", "gone/repo"),
        ];
        let s = starter_questions(&bases, &repos, &runs);
        assert_eq!(s.len(), STARTERS);
        assert_eq!(s[0].question, "What should I know from HR handbook?");
        assert_eq!(s[0].sources.knowledge, vec!["kb1".to_string()]);
        assert!(!s[0].sources.code);
        assert_eq!(
            s[1].question,
            "What's the latest on “Add rate limiting to login”?"
        );
        assert_eq!(s[1].why, "From recent work in web");
        assert!(s[1].sources.code);
        assert_eq!(s[1].sources.repo.as_deref(), Some("acme/web"));
        // The repository with the latest run leads, not the first by name.
        assert_eq!(s[2].question, "How is web put together?");
        assert!(s[3].sources.memories && !s[3].sources.code);
        // A run in a repository that is no longer connected is never offered.
        assert!(s.iter().all(|q| !q.question.contains("Old work")));
        // Every starter turns on at least one source.
        assert!(s
            .iter()
            .all(|q| SOURCE_KINDS.iter().any(|k| q.sources.allows(k.key))));
    }

    #[test]
    fn starters_are_empty_without_data_and_fill_from_what_there_is() {
        assert!(starter_questions(&[], &[], &[]).is_empty());
        let only_bases: Vec<(String, String)> = (1..=5)
            .map(|i| (format!("kb{i}"), format!("Base {i}")))
            .collect();
        let s = starter_questions(&only_bases, &[], &[]);
        assert_eq!(s.len(), STARTERS);
        assert!(s.iter().all(|q| q.sources.knowledge.len() == 1));
        let long = "word ".repeat(40);
        let s = starter_questions(&[], &["a/b".into()], &[run(&long, "a/b")]);
        assert!(s[0].question.contains('…'));
        assert!(s[0].question.chars().count() < 110);
    }

    #[test]
    fn ratings_are_up_down_or_cleared() {
        assert!(valid_rating(Some("up")) && valid_rating(Some("down")) && valid_rating(None));
        assert!(!valid_rating(Some("meh")) && !valid_rating(Some("")));
        // Old messages without a rating still read, and an unrated one writes none.
        let m: Message =
            serde_json::from_str(r#"{"seq":1,"role":"assistant","content":"x","at":"t"}"#).unwrap();
        assert_eq!(m.feedback, None);
        assert!(!serde_json::to_string(&m).unwrap().contains("feedback"));
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
    fn by_default_a_question_reads_everything_the_asker_can_see() {
        let kbs = vec!["kb1".to_string(), "kb2".to_string()];
        let repos = vec!["acme/api".to_string(), "acme/web".to_string()];
        let p = resolve_picks(None, &[], &kbs, &repos);
        assert!(p.auto && p.code && p.memories && p.connections && p.data_sources && p.skills);
        assert_eq!(p.knowledge, kbs);
        assert!(p.repos.is_empty(), "all repositories");
        // Without repositories, code isn't offered.
        assert!(!resolve_picks(None, &[], &kbs, &[]).code);
    }

    #[test]
    fn a_mention_narrows_the_question_to_what_was_mentioned() {
        let kbs = vec!["kb1".to_string()];
        let repos = vec!["acme/api".to_string(), "acme/web".to_string()];
        let m = |kind: &str, id: &str| Mention {
            kind: kind.into(),
            id: id.into(),
        };
        let p = resolve_picks(None, &[m("repo", "acme/web")], &kbs, &repos);
        assert!(!p.auto && p.code && p.memories && !p.connections);
        assert_eq!(p.repos, ["acme/web"]);
        assert!(p.knowledge.is_empty());
        let p = resolve_picks(None, &[m("knowledge", "kb1")], &kbs, &repos);
        assert_eq!(p.knowledge, ["kb1"]);
        assert!(!p.code);
        // A base the asker can't read, or a repository that isn't the team's,
        // narrows nothing: everything they can see is read instead.
        let p = resolve_picks(
            None,
            &[m("knowledge", "secret"), m("repo", "x/y")],
            &kbs,
            &repos,
        );
        assert!(p.auto);
        // An older client's picks are honored, minus what the asker can't read.
        let old = Picks {
            knowledge: vec!["kb1".into(), "secret".into()],
            ..Default::default()
        };
        let p = resolve_picks(Some(&old), &[], &kbs, &repos);
        assert_eq!(p.knowledge, ["kb1"]);
        assert!(!p.auto && !p.code);
    }

    #[test]
    fn the_system_prompt_lists_the_repositories_and_what_each_is_for() {
        let repos = vec![
            RepoInfo {
                role: Some("backend".into()),
                ..RepoInfo::new("acme/api", "main")
            },
            RepoInfo::new("acme/web", "trunk"),
        ];
        let p = system_prompt(&repos, true);
        assert!(p.contains("- acme/api (main), the backend repository"));
        assert!(p.contains("- acme/web (trunk)"));
        assert!(!p.contains("can't read these repositories"));
        assert!(system_prompt(&repos, false).contains("can't read these repositories"));
        assert!(mention_note(&repos, "How is api put together?").contains("acme/api"));
        assert_eq!(mention_note(&repos, "How do refunds work?"), "");
    }

    #[test]
    fn the_teams_work_is_its_own_source_and_read_by_default() {
        for t in super::super::chat_work::TOOLS {
            assert_eq!(kind_of_tool(t), super::super::chat_work::SOURCE_KEY);
        }
        let p = resolve_picks(None, &[], &[], &["acme/web".into()]);
        assert!(p.work && p.allows("work"));
        // Mentioning a repository narrows the code, never where work stands.
        let m = resolve_picks(
            None,
            &[Mention {
                kind: "repo".into(),
                id: "acme/web".into(),
            }],
            &[],
            &["acme/web".into()],
        );
        assert!(m.work && m.code);
        // An older client's picks without it leave it off.
        let old: Picks = serde_json::from_value(json!({"code": true})).unwrap();
        assert!(!old.work);
        // "What's the latest on <a run's title>" reads the work, not only code.
        let runs = vec![run("Embed the portal", "acme/web")];
        let q = starter_questions(&[], &["acme/web".into()], &runs);
        let latest = q
            .iter()
            .find(|s| s.question.starts_with("What's the latest"))
            .unwrap();
        assert!(latest.sources.work && latest.sources.code);
    }

    #[test]
    fn a_query_waits_for_its_schema_once() {
        let offered = vec![
            json!({"name": "live_describe"}),
            json!({"name": "live_lookup"}),
        ];
        let note = schema_first("live_lookup", &[], &offered).unwrap();
        assert!(note.contains("live_describe") && note.contains("Don't guess"));
        assert!(schema_first("live_lookup", &["live_describe".into()], &offered).is_none());
        // Once: the second try runs.
        assert!(schema_first("live_lookup", &["live_lookup".into()], &offered).is_none());
        // No schema tool offered: nothing to read first.
        assert!(schema_first("db_query", &[], &[json!({"name": "db_query"})]).is_none());
        assert!(schema_first("github__read_file", &[], &offered).is_none());
    }

    #[test]
    fn a_run_mention_says_why_it_stopped_from_the_runs_own_field() {
        let item = HashMap::from([
            ("run_id".to_string(), AttributeValue::S("r1".into())),
            ("status".to_string(), AttributeValue::S("failed".into())),
            (
                "error_message".to_string(),
                AttributeValue::S("The build broke".into()),
            ),
        ]);
        assert!(run_context_line(&item).contains("It stopped with: The build broke"));
    }

    #[test]
    fn tool_labels_read_plainly() {
        assert_eq!(
            tool_label("github__read_file", &json!({"path": "src/a.rs"})),
            "Read src/a.rs"
        );
        assert_eq!(
            tool_label("graph_definition", &json!({"name": "pay"})),
            "Found where \u{201c}pay\u{201d} is defined"
        );
        assert_eq!(
            tool_label("notion__search_pages", &json!({})),
            "notion: search pages"
        );
        assert!(tool_label("search_knowledge", &json!({"query": "leave"})).contains("leave"));
    }
}
