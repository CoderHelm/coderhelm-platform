//! Knowledge bases connected in place through a search tool the team
//! already runs behind an MCP server: Azure AI Search or Foundry IQ,
//! Pinecone Assistant, Glean, Atlassian Rovo, Dropbox Dash, a Bedrock
//! knowledge base behind an AgentCore gateway, or the team's own retrieval
//! server. The server is an entry on the team's Connected apps
//! (`mcp_remote`), so its sign-in, its sealed tokens and its address checks
//! are the ones agents already use.
//!
//! The contract is the one most search servers speak: a `search` tool that
//! takes a query and returns results (`id`, `title`, `url`, and often the
//! passage text), and an optional `fetch` tool that takes an id and returns
//! the document (`id`, `title`, `text`, `url`, `metadata`). Answers are read
//! as data wherever the server puts them ([`results`]): the first list of
//! results in its JSON (`results`, `retrievalResults`, `documents`, `hits`,
//! `snippets`, ...), each result's text from its usual field names. A result
//! without text is fetched, best first, up to [`FETCH_DEPTH`].
//!
//! Who it searches as: a server the team shares is searched with the
//! team's connection. A server that signs each person in is searched with
//! the asking person's own sign-in ([`super::Remote::per_person`]), so a
//! person only ever gets what the tool lets them see; a search with no
//! person (an agent run nobody started) skips it.
//!
//! Only the two tools picked when the base was connected are ever called,
//! and only through the read path that never needs the agents' allow list
//! (`Runtime::read_tool`). A tool the server marks as destructive is never
//! picked.

use std::sync::Arc;

use serde::Serialize;
use serde_json::{json, Value};

use super::search::{Hit, Retriever};
use super::sources::connector::ConnectorError;
use super::sources::mcp::{result_json, McpTools, RuntimeTools};
use super::{Access, Backend, Base, Provider, Remote};
use crate::mcp_remote::client::RemoteTool;
use crate::mcp_remote::runtime::Runtime;
use crate::mcp_remote::store::{Owner, Sharing};

/// Results whose text is missing are fetched, best first, up to this many.
pub const FETCH_DEPTH: usize = 5;
/// The longest passage kept from one result.
const MAX_TEXT: usize = 6_000;
/// The query the connection check sends.
const PROBE: &str = "Coderhelm connection check";

/// Why a search tool couldn't be searched, and what to do.
pub type Refusal = super::remote::Refusal;

fn refusal(code: &'static str, message: impl Into<String>, fix: impl Into<String>) -> Refusal {
    Refusal {
        code,
        message: message.into(),
        fix: fix.into(),
    }
}

// ── Picking the tools ────────────────────────────────────────────────────────

/// Tool names that search, best first (OpenAI's contract, Azure Foundry IQ,
/// AgentCore gateways, Pinecone Assistant and other common ones).
const SEARCH_NAMES: &[&str] = &[
    "search",
    "knowledge_base_retrieve",
    "retrieve",
    "search_documents",
    "search_knowledge",
    "semantic_search",
    "get_context",
    "query",
];
/// Tool names that read one result in full.
const FETCH_NAMES: &[&str] = &["fetch", "fetch_document", "get_document", "read_document"];

fn safe(t: &RemoteTool) -> bool {
    t.destructive != Some(true) && t.read_only != Some(false)
}

/// The tool most likely to be the server's search, if any.
pub fn guess_search(tools: &[RemoteTool]) -> Option<&RemoteTool> {
    let usable: Vec<&RemoteTool> = tools.iter().filter(|t| safe(t)).collect();
    SEARCH_NAMES
        .iter()
        .find_map(|n| usable.iter().copied().find(|t| t.name == *n))
        .or_else(|| {
            usable.iter().copied().find(|t| {
                let n = t.name.to_ascii_lowercase();
                n.ends_with("___retrieve") || n.ends_with("__retrieve")
            })
        })
        .or_else(|| {
            usable.iter().copied().find(|t| {
                let n = t.name.to_ascii_lowercase();
                n.contains("search") || n.contains("retrieve")
            })
        })
}

/// The tool most likely to be the server's fetch, if any.
pub fn guess_fetch(tools: &[RemoteTool]) -> Option<&RemoteTool> {
    let usable: Vec<&RemoteTool> = tools.iter().filter(|t| safe(t)).collect();
    FETCH_NAMES
        .iter()
        .find_map(|n| usable.iter().copied().find(|t| t.name == *n))
        .or_else(|| {
            usable
                .iter()
                .copied()
                .find(|t| t.name.to_ascii_lowercase().contains("fetch"))
        })
}

/// The tool's properties.
fn props(t: &RemoteTool) -> serde_json::Map<String, Value> {
    t.input_schema["properties"]
        .as_object()
        .cloned()
        .unwrap_or_default()
}

/// The argument a tool takes its main text in: a usual name, else its only
/// required string, else its first string.
fn text_arg(t: &RemoteTool, usual: &[&str]) -> String {
    let p = props(t);
    if let Some(n) = usual.iter().find(|n| p.contains_key(**n)) {
        return n.to_string();
    }
    let required: Vec<&str> = t.input_schema["required"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|n| p.get(*n).is_some_and(|v| v["type"] == "string"))
        .collect();
    if let [one] = required.as_slice() {
        return one.to_string();
    }
    p.iter()
        .find(|(_, v)| v["type"] == "string")
        .map(|(k, _)| k.clone())
        .unwrap_or_else(|| usual[0].to_string())
}

/// The arguments of one search for `query`, asking for `k` results where
/// the tool takes a count.
pub fn search_args(t: &RemoteTool, query: &str, k: usize) -> Value {
    let mut args = serde_json::Map::new();
    args.insert(
        text_arg(
            t,
            &[
                "query",
                "q",
                "queryString",
                "search",
                "text",
                "question",
                "input",
                "prompt",
            ],
        ),
        json!(crate::truncate_str(query, 1_000)),
    );
    let p = props(t);
    for n in [
        "limit",
        "top_k",
        "topK",
        "k",
        "max_results",
        "maxResults",
        "num_results",
        "count",
        "top",
        "numberOfResults",
    ] {
        if p.contains_key(n) {
            args.insert(n.to_string(), json!(k));
            break;
        }
    }
    Value::Object(args)
}

/// The arguments of one fetch of result `id`.
pub fn fetch_args(t: &RemoteTool, id: &str) -> Value {
    json!({ text_arg(t, &["id", "document_id", "documentId", "doc_id", "uri", "url"]): id })
}

// ── Reading answers ──────────────────────────────────────────────────────────

/// One result, as read from a server's answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Found {
    pub id: String,
    pub title: String,
    pub url: String,
    pub text: String,
    pub date: String,
}

/// List keys that hold results, best first.
const LIST_KEYS: &[&str] = &[
    "results",
    "retrievalResults",
    "documents",
    "hits",
    "items",
    "matches",
    "snippets",
    "chunks",
    "passages",
    "references",
    "data",
    "value",
];

fn str_at<'a>(v: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut at = v;
    for p in path {
        at = at.get(*p)?;
    }
    at.as_str().map(str::trim).filter(|s| !s.is_empty())
}

fn first_str(v: &Value, paths: &[&[&str]]) -> String {
    paths
        .iter()
        .find_map(|p| str_at(v, p))
        .unwrap_or("")
        .to_string()
}

/// A value's text: a string, `{text}`, or a list of either, joined.
fn text_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.trim().to_string(),
        Value::Object(m) => ["text", "content", "snippet", "value"]
            .iter()
            .find_map(|k| m.get(*k).map(text_of).filter(|t| !t.is_empty()))
            .unwrap_or_default(),
        Value::Array(a) => a
            .iter()
            .map(text_of)
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// One result object read.
pub fn read_one(v: &Value) -> Found {
    let text = [
        "text",
        "content",
        "chunk",
        "passage",
        "snippet",
        "snippets",
        "excerpt",
        "highlights",
        "body",
        "page_content",
        "summary",
    ]
    .iter()
    .find_map(|k| v.get(*k).map(text_of).filter(|t| !t.is_empty()))
    .unwrap_or_default();
    let url = [
        &["url"][..],
        &["link"],
        &["href"],
        &["source_url"],
        &["sourceUrl"],
        &["uri"],
        &["metadata", "url"],
        &["metadata", "source"],
        &["location", "webLocation", "url"],
        &["location", "confluenceLocation", "url"],
        &["location", "sharePointLocation", "url"],
        &["document", "url"],
        &["reference", "url"],
    ]
    .iter()
    .find_map(|p| str_at(v, p).filter(|u| u.starts_with("https://")))
    .unwrap_or("")
    .to_string();
    let id = first_str(
        v,
        &[
            &["id"],
            &["document_id"],
            &["documentId"],
            &["doc_id"],
            &["key"],
            &["metadata", "x-amz-bedrock-kb-source-uri"],
            &["location", "s3Location", "uri"],
            &["uri"],
            &["reference", "file", "id"],
            &["file", "id"],
        ],
    );
    let title = first_str(
        v,
        &[
            &["title"],
            &["name"],
            &["document_title"],
            &["file_name"],
            &["fileName"],
            &["metadata", "title"],
            &["metadata", "x-amz-bedrock-kb-title"],
            &["document", "title"],
            &["reference", "file", "name"],
            &["file", "name"],
        ],
    );
    let date = first_str(
        v,
        &[
            &["updated_at"],
            &["updatedAt"],
            &["last_modified"],
            &["lastModified"],
            &["modified"],
            &["date"],
            &["metadata", "updated_at"],
            &["metadata", "last_modified"],
            &["metadata", "lastModified"],
        ],
    );
    Found {
        id,
        title,
        url,
        text,
        date,
    }
}

/// The first list of result objects in `v`, depth first, a usual key
/// before any other.
fn result_list(v: &Value, depth: usize) -> Option<&Vec<Value>> {
    if depth > 8 {
        return None;
    }
    let objects = |a: &Vec<Value>| !a.is_empty() && a.iter().all(Value::is_object);
    match v {
        Value::Array(a) if objects(a) => Some(a),
        Value::Object(m) => {
            for k in LIST_KEYS {
                if let Some(Value::Array(a)) = m.get(*k) {
                    if objects(a) {
                        return Some(a);
                    }
                }
            }
            m.values().find_map(|x| result_list(x, depth + 1))
        }
        _ => None,
    }
}

/// Every result in a tool's answer, best first. An answer that is only
/// prose (no data in it) is one result: the prose.
pub fn results(provider: &str, answer: &Value) -> Result<Vec<Found>, Refusal> {
    let data = match result_json(provider, answer) {
        Ok(d) => d,
        Err(ConnectorError::Temporary(m)) if answer["isError"].as_bool() == Some(true) => {
            return Err(refusal(
                "refused",
                m,
                "Check the search tool's own settings, then try again.",
            ))
        }
        Err(_) => {
            let prose = text_of(&answer["content"]);
            if prose.is_empty() {
                return Ok(Vec::new());
            }
            return Ok(vec![Found {
                text: prose,
                ..Default::default()
            }]);
        }
    };
    let list: Vec<Value> = match result_list(&data, 0) {
        Some(a) => a.clone(),
        None if data.is_object() => vec![data.clone()],
        None => Vec::new(),
    };
    Ok(list
        .iter()
        .map(read_one)
        .filter(|f| !f.text.is_empty() || !f.id.is_empty() || !f.url.is_empty())
        .collect())
}

/// Results as hits of `base`, best first: each result is its own document
/// (its id, else its link, else its title), with its title as the place.
pub fn to_hits(base: &Base, found: &[Found]) -> Vec<Hit> {
    let mut out = Vec::new();
    for (i, f) in found.iter().enumerate() {
        let text = f.text.trim();
        if text.is_empty() {
            continue;
        }
        let document = [&f.id, &f.url, &f.title]
            .iter()
            .find(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("result-{i}"));
        let name = if f.title.is_empty() {
            document.clone()
        } else {
            f.title.clone()
        };
        out.push(Hit {
            kb_id: base.id.clone(),
            kb_name: base.name.clone(),
            file_id: crate::truncate_str(&document, 512).to_string(),
            file_name: crate::truncate_str(&name, 200).to_string(),
            loc: crate::truncate_str(&f.title, 300).to_string(),
            as_of: super::remote::date_of(&f.date),
            text: crate::truncate_str(text, MAX_TEXT).to_string(),
            score: 1.0 / (i as f32 + 1.0),
            authoritative: base.authoritative,
            ord: i as u32,
            url: super::answer::original_url(&f.url).unwrap_or_default(),
            source_kind: super::SourceKind::McpSearch.key().into(),
        });
    }
    out
}

// ── Opening the server ───────────────────────────────────────────────────────

/// A search tool's server, opened as one owner.
pub struct Opened {
    pub name: String,
    /// It signs each person in.
    pub personal: bool,
    pub tools: Arc<dyn McpTools>,
}

/// Opens a team's Connected apps server as an owner. The seam tests fake.
#[async_trait::async_trait]
pub trait Servers: Send + Sync {
    /// The team the servers belong to.
    fn team(&self) -> &str;
    /// Opens server `id` of [`Self::team`], as `person` when it signs each
    /// person in (`None`: nobody), else with the team's connection.
    async fn open(&self, id: &str, person: Option<&str>) -> Result<Opened, Refusal>;
}

/// The production [`Servers`]: the team's MCP runtime.
pub struct RuntimeServers {
    pub rt: Runtime,
}

#[async_trait::async_trait]
impl Servers for RuntimeServers {
    fn team(&self) -> &str {
        &self.rt.team
    }

    async fn open(&self, id: &str, person: Option<&str>) -> Result<Opened, Refusal> {
        let server = self
            .rt
            .store
            .get_server(&self.rt.team, id)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "Connected apps server unreadable");
                refusal(
                    "reach",
                    "Coderhelm couldn't read the team's Connected apps.",
                    "Try again in a minute.",
                )
            })?
            .ok_or_else(|| {
                refusal(
                    "not_found",
                    "The search tool's server isn't on Connected apps any more.",
                    "Add it again on Settings, Connected apps, then connect this knowledge base again.",
                )
            })?;
        let personal = server.sharing == Sharing::Personal
            && server.auth == crate::mcp_remote::store::AuthKind::OAuth;
        let owner = server.owner_for(person).ok_or_else(|| {
            refusal(
                "not_signed_in",
                format!(
                    "{} answers each person with their own sign-in, and this search has no person.",
                    server.name
                ),
                "It is searched when a person asks, or in runs a person started.",
            )
        })?;
        let name = server.name.clone();
        let label: &'static str = "The search tool";
        Ok(Opened {
            name,
            personal,
            tools: Arc::new(RuntimeTools::new(self.rt.clone(), server, owner, label)),
        })
    }
}

/// A connection failure in plain words.
fn conn_refusal(name: &str, e: ConnectorError) -> Refusal {
    match e {
        ConnectorError::Reconnect(m) => refusal(
            "not_signed_in",
            format!("{name} needs a sign-in ({}).", crate::truncate_str(&m, 200)),
            format!("Sign in to {name} on Settings, Connected apps, then try again."),
        ),
        ConnectorError::Forbidden(m) | ConnectorError::Config(m) => refusal(
            "refused",
            format!(
                "{name} refused the search ({}).",
                crate::truncate_str(&m, 200)
            ),
            "Check the search tool's own settings and who it lets search, then try again.",
        ),
        ConnectorError::NotFound(_) => refusal(
            "not_found",
            format!("{name} no longer answers at its address."),
            "Check the server's address on Settings, Connected apps.",
        ),
        ConnectorError::RateLimited(_) => refusal(
            "busy",
            format!("{name} is limiting searches right now."),
            "Try again in a minute.",
        ),
        ConnectorError::Temporary(m) => refusal(
            "reach",
            format!(
                "Coderhelm couldn't reach {name} ({}).",
                crate::truncate_str(&m, 200)
            ),
            "Try again. If it keeps failing, check the server on Settings, Connected apps.",
        ),
    }
}

/// The tool named `name` on the server (from its cached list, else asked).
async fn named(o: &Opened, name: &str) -> Result<RemoteTool, Refusal> {
    let tools = o
        .tools
        .tools()
        .await
        .map_err(|e| conn_refusal(&o.name, e))?;
    tools
        .into_iter()
        .find(|t| t.name == name && safe(t))
        .ok_or_else(|| {
            refusal(
                "no_search_tool",
                format!("{} no longer offers the tool {name}.", o.name),
                "Check the knowledge base again to pick its search tool.",
            )
        })
}

/// One search: up to `k` results, the text of the best ones without text
/// fetched.
pub async fn search_with(
    o: &Opened,
    search: &RemoteTool,
    fetch: Option<&RemoteTool>,
    query: &str,
    k: usize,
) -> Result<Vec<Found>, Refusal> {
    let answer = o
        .tools
        .call(search, &search_args(search, query, k))
        .await
        .map_err(|e| conn_refusal(&o.name, e))?;
    let mut found = results(&o.name, &answer)?;
    found.truncate(k);
    if let Some(fetch) = fetch {
        let missing: Vec<usize> = found
            .iter()
            .enumerate()
            .filter(|(_, f)| f.text.is_empty() && !f.id.is_empty())
            .map(|(i, _)| i)
            .take(FETCH_DEPTH)
            .collect();
        let fetched = futures::future::join_all(missing.iter().map(|i| {
            let id = found[*i].id.clone();
            async move {
                let a = o.tools.call(fetch, &fetch_args(fetch, &id)).await.ok()?;
                results(&o.name, &a).ok()?.into_iter().next()
            }
        }))
        .await;
        for (i, f) in missing.into_iter().zip(fetched) {
            if let Some(f) = f {
                let r = &mut found[i];
                r.text = f.text;
                if r.title.is_empty() {
                    r.title = f.title;
                }
                if r.url.is_empty() {
                    r.url = f.url;
                }
                if r.date.is_empty() {
                    r.date = f.date;
                }
            }
        }
    }
    Ok(found)
}

/// What the connection check settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Checked {
    pub server_name: String,
    pub search_tool: String,
    pub fetch_tool: String,
    pub per_person: bool,
}

/// Checks a search tool before its base is saved: the server answers, a
/// search tool is there (the one asked for, or the likeliest), and one test
/// search reads. Run as the admin connecting it (their own sign-in when the
/// server signs each person in).
pub async fn check(
    servers: &dyn Servers,
    server_id: &str,
    person: &str,
    search_tool: &str,
    fetch_tool: &str,
) -> Result<Checked, Refusal> {
    let o = servers.open(server_id, Some(person)).await?;
    let tools = o
        .tools
        .tools()
        .await
        .map_err(|e| conn_refusal(&o.name, e))?;
    let names = || {
        let mut n: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        n.truncate(12);
        n.join(", ")
    };
    let search = if search_tool.trim().is_empty() {
        guess_search(&tools).cloned().ok_or_else(|| {
            refusal(
                "no_search_tool",
                format!(
                    "{} has no tool that searches (its tools: {}).",
                    o.name,
                    names()
                ),
                "Pick the tool that searches, or connect a server that offers search.",
            )
        })?
    } else {
        tools
            .iter()
            .find(|t| t.name == search_tool.trim())
            .cloned()
            .ok_or_else(|| {
                refusal(
                    "no_search_tool",
                    format!("{} has no tool named {}.", o.name, search_tool.trim()),
                    format!("Pick one of its tools: {}.", names()),
                )
            })?
    };
    if !safe(&search) {
        return Err(refusal(
            "invalid",
            format!(
                "{} says {} changes things, so Coderhelm won't call it to search.",
                o.name, search.name
            ),
            "Pick a read-only tool that searches.",
        ));
    }
    let fetch = if fetch_tool.trim().is_empty() {
        guess_fetch(&tools)
            .filter(|t| t.name != search.name)
            .cloned()
    } else {
        let f = tools
            .iter()
            .find(|t| t.name == fetch_tool.trim() && safe(t))
            .cloned();
        if f.is_none() {
            return Err(refusal(
                "no_search_tool",
                format!(
                    "{} has no read-only tool named {}.",
                    o.name,
                    fetch_tool.trim()
                ),
                format!("Pick one of its tools, or none: {}.", names()),
            ));
        }
        f
    };
    search_with(&o, &search, fetch.as_ref(), PROBE, 1)
        .await
        .map_err(|r| {
            if r.code == "refused" {
                refusal(
                    "unreadable",
                    format!("{}'s test search failed: {}", o.name, r.message),
                    r.fix,
                )
            } else {
                r
            }
        })?;
    Ok(Checked {
        server_name: o.name,
        search_tool: search.name,
        fetch_tool: fetch.map(|t| t.name).unwrap_or_default(),
        per_person: o.personal,
    })
}

/// A connected search tool on server `server_id`, as checked.
pub fn mcp_remote(server_id: &str, c: &Checked) -> Remote {
    Remote {
        provider: Provider::Mcp,
        target: server_id.to_string(),
        region: String::new(),
        access: Access::Connection {
            server_id: server_id.to_string(),
        },
        hybrid: false,
        per_person: c.per_person,
        search_tool: c.search_tool.clone(),
        fetch_tool: c.fetch_tool.clone(),
        name: c.server_name.clone(),
    }
}

// ── The retriever ────────────────────────────────────────────────────────────

/// Searches connected search tools, as the team, for `person` (the asker's
/// email, when a person asked).
pub struct McpSearch<'a> {
    pub servers: &'a dyn Servers,
    pub person: Option<String>,
}

#[async_trait::async_trait]
impl Retriever for McpSearch<'_> {
    async fn retrieve(
        &self,
        team_id: &str,
        base: &Base,
        query: &str,
        k: usize,
    ) -> Result<Vec<Hit>, String> {
        let Backend::Remote(remote) = &base.backend else {
            return Ok(Vec::new());
        };
        let Access::Connection { server_id } = &remote.access else {
            return Err("This knowledge base isn't reached through a search tool.".into());
        };
        // A team only ever opens its own Connected apps.
        if self.servers.team() != team_id {
            return Err("That search tool isn't this team's.".into());
        }
        let person = self.person.as_deref().filter(|p| !p.trim().is_empty());
        if remote.per_person && person.is_none() {
            return Err(
                "it answers each person with their own sign-in, and this search has no person"
                    .into(),
            );
        }
        let o = self
            .servers
            .open(server_id, person)
            .await
            .map_err(|r| r.message)?;
        let search = named(&o, &remote.search_tool)
            .await
            .map_err(|r| r.message)?;
        let fetch = if remote.fetch_tool.is_empty() {
            None
        } else {
            named(&o, &remote.fetch_tool).await.ok()
        };
        let found = search_with(&o, &search, fetch.as_ref(), query, k.min(20))
            .await
            .map_err(|r| r.message)?;
        Ok(to_hits(base, &found))
    }
}

/// A Connected apps server as the "Connect a search tool" list shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Listed {
    pub id: String,
    pub name: String,
    pub url: String,
    pub sharing: &'static str,
    pub search_tool: Option<String>,
    pub fetch_tool: Option<String>,
    pub tools: Vec<Value>,
}

/// A server and its last listed tools, for the picker.
pub fn listed(s: &crate::mcp_remote::store::ServerRecord) -> Listed {
    Listed {
        id: s.id.clone(),
        name: s.name.clone(),
        url: s.url.clone(),
        sharing: s.sharing.as_str(),
        search_tool: guess_search(&s.tools).map(|t| t.name.clone()),
        fetch_tool: guess_fetch(&s.tools).map(|t| t.name.clone()),
        tools: s
            .tools
            .iter()
            .filter(|t| safe(t))
            .map(|t| json!({ "name": t.name, "description": crate::truncate_str(&t.description, 200) }))
            .collect(),
    }
}

/// Is `owner` the owner whose connection a person's search would use?
pub fn owner_for(personal: bool, person: Option<&str>) -> Option<Owner> {
    if personal {
        person.filter(|p| !p.trim().is_empty()).map(Owner::user)
    } else {
        Some(Owner::Team)
    }
}

#[cfg(test)]
mod tests {
    use super::super::sources::mcp::fake::{text_result, tool_with, FakeMcp};
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Answer = Arc<dyn Fn(&str, &Value) -> Result<Value, ConnectorError> + Send + Sync>;

    /// Servers of one team: each server's tools, answering per owner.
    struct FakeServers {
        team: String,
        personal: bool,
        made: Mutex<HashMap<String, Arc<FakeMcp>>>,
        /// Who each open was for.
        opened: Mutex<Vec<Option<String>>>,
        tools: Vec<RemoteTool>,
        answer: Answer,
    }

    impl FakeServers {
        fn new(
            personal: bool,
            tools: Vec<RemoteTool>,
            answer: impl Fn(&str, &Value) -> Result<Value, ConnectorError> + Send + Sync + 'static,
        ) -> Self {
            FakeServers {
                team: "TEAM#a".into(),
                personal,
                made: Mutex::new(HashMap::new()),
                opened: Mutex::new(Vec::new()),
                tools,
                answer: Arc::new(answer),
            }
        }
        fn calls(&self, id: &str) -> Vec<(String, Value)> {
            self.made.lock().unwrap()[id].calls()
        }
    }

    #[async_trait::async_trait]
    impl Servers for FakeServers {
        fn team(&self) -> &str {
            &self.team
        }
        async fn open(&self, id: &str, person: Option<&str>) -> Result<Opened, Refusal> {
            if id != "search-1" {
                return Err(refusal("not_found", "gone", "add it"));
            }
            self.opened.lock().unwrap().push(person.map(str::to_string));
            if self.personal && person.is_none() {
                return Err(refusal("not_signed_in", "no person", ""));
            }
            let answer = self.answer.clone();
            let who = person.unwrap_or("team").to_string();
            let m = Arc::new(FakeMcp::new(self.tools.clone(), move |t, a| {
                let mut a = a.clone();
                a["_as"] = json!(who.clone());
                answer(t, &a)
            }));
            self.made.lock().unwrap().insert(id.into(), m.clone());
            Ok(Opened {
                name: "Docs search".into(),
                personal: self.personal,
                tools: m,
            })
        }
    }

    fn base(per_person: bool) -> Base {
        let c = Checked {
            server_name: "Docs search".into(),
            search_tool: "search".into(),
            fetch_tool: "fetch".into(),
            per_person,
        };
        Base {
            id: "kb".into(),
            name: "Product docs".into(),
            backend: Backend::Remote(mcp_remote("search-1", &c)),
            ..Default::default()
        }
    }

    fn openai_tools() -> Vec<RemoteTool> {
        vec![tool_with("search", &["query"]), tool_with("fetch", &["id"])]
    }

    #[test]
    fn the_search_and_fetch_tools_are_found_by_their_usual_names() {
        let t = openai_tools();
        assert_eq!(guess_search(&t).unwrap().name, "search");
        assert_eq!(guess_fetch(&t).unwrap().name, "fetch");
        let azure = vec![tool_with("knowledge_base_retrieve", &["queries"])];
        assert_eq!(
            guess_search(&azure).unwrap().name,
            "knowledge_base_retrieve"
        );
        let agentcore = vec![tool_with("managed-kb___Retrieve", &["retrievalQuery"])];
        assert_eq!(
            guess_search(&agentcore).unwrap().name,
            "managed-kb___Retrieve"
        );
        let mut dangerous = tool_with("search_and_delete", &["query"]);
        dangerous.destructive = Some(true);
        assert!(
            guess_search(&[dangerous]).is_none(),
            "a destructive tool is never picked"
        );
        assert!(guess_search(&[tool_with("create_issue", &["title"])]).is_none());
    }

    #[test]
    fn arguments_follow_the_tool_schema() {
        let a = search_args(&tool_with("search", &["query", "limit"]), "leave policy", 5);
        assert_eq!(a, json!({"query": "leave policy", "limit": 5}));
        let a = search_args(&tool_with("get_context", &["question", "top_k"]), "x", 3);
        assert_eq!(a, json!({"question": "x", "top_k": 3}));
        let mut odd = tool_with("lookup", &["needle"]);
        odd.input_schema["required"] = json!(["needle"]);
        assert_eq!(search_args(&odd, "x", 3), json!({"needle": "x"}));
        assert_eq!(
            fetch_args(&tool_with("fetch", &["id"]), "doc-1"),
            json!({"id": "doc-1"})
        );
        assert_eq!(
            fetch_args(&tool_with("get_document", &["document_id"]), "d"),
            json!({"document_id": "d"})
        );
    }

    #[test]
    fn answers_in_the_usual_shapes_are_read() {
        // OpenAI's search contract, as structured content.
        let a = json!({"content": [], "structuredContent": {"results": [
            {"id": "d1", "title": "Leave policy", "url": "https://docs.example.com/leave", "text": "Sixteen weeks."},
            {"id": "d2", "title": "Holidays", "url": "https://docs.example.com/holidays"}
        ]}});
        let f = results("Docs", &a).unwrap();
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].text, "Sixteen weeks.");
        assert_eq!(f[1].text, "", "fetched later");
        // A Bedrock answer through an AgentCore gateway, as text JSON.
        let bedrock = json!({"retrievalResults": [{
            "content": {"text": "Drain the queue first."},
            "location": {"type": "S3", "s3Location": {"uri": "s3://ops/runbook.md"}},
            "metadata": {"x-amz-bedrock-kb-title": "Runbook"},
            "score": 0.8
        }]});
        let f = results("Gateway", &text_result(&bedrock)).unwrap();
        assert_eq!(f[0].text, "Drain the queue first.");
        assert_eq!(f[0].id, "s3://ops/runbook.md");
        assert_eq!(f[0].title, "Runbook");
        // Pinecone Assistant's context snippets.
        let pine = json!({"snippets": [{"content": "Use the v2 API.", "reference": {"file": {"name": "api.pdf", "id": "f9"}}}]});
        let f = results("Pinecone", &text_result(&pine)).unwrap();
        assert_eq!(
            (f[0].text.as_str(), f[0].title.as_str(), f[0].id.as_str()),
            ("Use the v2 API.", "api.pdf", "f9")
        );
        // Glean-style results with snippet lists.
        let glean = json!({"results": [{"title": "On-call", "url": "https://wiki.example.com/oncall", "snippets": [{"text": "Rotates Monday."}, "Pager first."]}]});
        let f = results("Glean", &text_result(&glean)).unwrap();
        assert_eq!(f[0].text, "Rotates Monday.\nPager first.");
        // Prose only: one result, the prose.
        let prose =
            json!({"content": [{"type": "text", "text": "The deploy freeze starts Friday."}]});
        let f = results("Docs", &prose).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].text, "The deploy freeze starts Friday.");
        // The server's own error is said plainly.
        let err =
            json!({"isError": true, "content": [{"type": "text", "text": "index not found"}]});
        let r = results("Docs", &err).unwrap_err();
        assert_eq!(r.code, "refused");
        assert!(r.message.contains("index not found"));
        // Nothing found is no results, not an error.
        assert!(
            results("Docs", &json!({"structuredContent": {"results": []}}))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn results_become_hits_with_document_title_date_and_link() {
        let b = base(false);
        let hits = to_hits(
            &b,
            &[
                Found {
                    id: "d1".into(),
                    title: "Leave policy".into(),
                    url: "https://docs.example.com/leave".into(),
                    text: "Sixteen weeks.".into(),
                    date: "2026-09-30T00:00:00Z".into(),
                },
                Found {
                    url: "http://insecure.example.com/x".into(),
                    text: "Plain link.".into(),
                    ..Default::default()
                },
                Found {
                    id: "empty".into(),
                    ..Default::default()
                },
            ],
        );
        assert_eq!(hits.len(), 2, "a result without text is dropped");
        assert_eq!(hits[0].file_id, "d1");
        assert_eq!(hits[0].file_name, "Leave policy");
        assert_eq!(hits[0].as_of, "2026-09-30");
        assert_eq!(hits[0].url, "https://docs.example.com/leave");
        assert_eq!(hits[0].source_kind, "mcp");
        assert!(hits[0].score > hits[1].score, "best first");
        assert_eq!(hits[1].url, "", "only an https link is a link");
        assert_eq!(hits[1].file_id, "http://insecure.example.com/x");
    }

    #[tokio::test]
    async fn a_search_fetches_results_without_text_best_first() {
        let s = FakeServers::new(false, openai_tools(), |tool, args| {
            Ok(match tool {
                "search" => text_result(&json!({"results": [
                    {"id": "d1", "title": "Leave policy", "url": "https://docs.example.com/leave"},
                    {"id": "d2", "title": "Holidays", "text": "Twelve days."}
                ]})),
                "fetch" => {
                    assert_eq!(args["id"], "d1");
                    text_result(
                        &json!({"id": "d1", "title": "Leave policy", "text": "Sixteen weeks of leave."}),
                    )
                }
                _ => unreachable!(),
            })
        });
        let r = McpSearch {
            servers: &s,
            person: None,
        };
        let hits = r
            .retrieve("TEAM#a", &base(false), "leave", 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].text, "Sixteen weeks of leave.");
        assert_eq!(hits[1].text, "Twelve days.");
        let calls = s.calls("search-1");
        assert_eq!(calls[0].0, "search");
        assert_eq!(calls[0].1["query"], "leave");
        assert_eq!(
            calls.iter().filter(|c| c.0 == "fetch").count(),
            1,
            "only the result without text"
        );
    }

    #[tokio::test]
    async fn a_per_person_search_uses_the_askers_own_sign_in_and_never_runs_without_one() {
        let s = FakeServers::new(true, openai_tools(), |tool, args| {
            assert_eq!(tool, "search");
            // Each person sees only their own documents.
            let who = args["_as"].as_str().unwrap_or("");
            Ok(text_result(
                &json!({"results": [{"id": who, "text": format!("for {who}")}]}),
            ))
        });
        let ada = McpSearch {
            servers: &s,
            person: Some("ada@example.com".into()),
        };
        let hits = ada.retrieve("TEAM#a", &base(true), "x", 5).await.unwrap();
        assert_eq!(hits[0].text, "for ada@example.com");
        assert_eq!(
            s.opened.lock().unwrap().last().unwrap().as_deref(),
            Some("ada@example.com")
        );
        let nobody = McpSearch {
            servers: &s,
            person: None,
        };
        let e = nobody
            .retrieve("TEAM#a", &base(true), "x", 5)
            .await
            .unwrap_err();
        assert!(e.contains("own sign-in"), "{e}");
        assert_eq!(
            s.opened.lock().unwrap().len(),
            1,
            "nothing was opened for nobody"
        );
    }

    #[tokio::test]
    async fn a_team_never_reaches_another_teams_search_tool() {
        let s = FakeServers::new(false, openai_tools(), |_, _| {
            Ok(text_result(&json!({"results": []})))
        });
        let r = McpSearch {
            servers: &s,
            person: None,
        };
        let e = r
            .retrieve("TEAM#b", &base(false), "x", 5)
            .await
            .unwrap_err();
        assert!(e.contains("isn't this team's"));
        assert!(s.opened.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn connecting_checks_the_tools_and_one_search_in_plain_words() {
        let s = FakeServers::new(false, openai_tools(), |_, _| {
            Ok(text_result(&json!({"results": []})))
        });
        let c = check(&s, "search-1", "admin@example.com", "", "")
            .await
            .unwrap();
        assert_eq!(
            (c.search_tool.as_str(), c.fetch_tool.as_str()),
            ("search", "fetch")
        );
        assert!(!c.per_person);
        assert_eq!(s.calls("search-1")[0].1["query"], PROBE);
        // A server without a search tool names its tools.
        let none = FakeServers::new(
            false,
            vec![tool_with("create_issue", &["title"])],
            |_, _| unreachable!(),
        );
        let r = check(&none, "search-1", "a@example.com", "", "")
            .await
            .unwrap_err();
        assert_eq!(r.code, "no_search_tool");
        assert!(r.message.contains("create_issue"), "{}", r.message);
        // A tool asked for by name that isn't there.
        let r = check(&s, "search-1", "a@example.com", "lookup", "")
            .await
            .unwrap_err();
        assert_eq!(r.code, "no_search_tool");
        // A server that isn't on Connected apps.
        assert_eq!(
            check(&s, "gone", "a@example.com", "", "")
                .await
                .unwrap_err()
                .code,
            "not_found"
        );
        // A sign-in that stopped working.
        let expired = FakeServers::new(false, openai_tools(), |_, _| {
            Err(ConnectorError::Reconnect("expired".into()))
        });
        let r = check(&expired, "search-1", "a@example.com", "", "")
            .await
            .unwrap_err();
        assert_eq!(r.code, "not_signed_in");
        assert!(r.fix.contains("Connected apps"));
        // The server's own error on the test search.
        let broken = FakeServers::new(false, openai_tools(), |_, _| {
            Ok(json!({"isError": true, "content": [{"type": "text", "text": "index missing"}]}))
        });
        let r = check(&broken, "search-1", "a@example.com", "", "")
            .await
            .unwrap_err();
        assert_eq!(r.code, "unreadable");
        assert!(r.message.contains("index missing"));
        // A destructive tool is refused even when named.
        let mut d = tool_with("search", &["query"]);
        d.destructive = Some(true);
        let bad = FakeServers::new(false, vec![d], |_, _| unreachable!());
        assert_eq!(
            check(&bad, "search-1", "a@example.com", "search", "")
                .await
                .unwrap_err()
                .code,
            "invalid"
        );
        let r = refusal("x", "a", "b");
        assert!(!format!("{}{}", r.message, r.fix).contains('\u{2192}'));
    }

    #[test]
    fn a_connected_search_tool_round_trips_without_any_token() {
        let b = base(true);
        let v = serde_json::to_value(&b).unwrap();
        assert_eq!(v["backend"]["provider"], "mcp");
        assert_eq!(v["backend"]["access"]["kind"], "connection");
        assert_eq!(v["backend"]["access"]["server_id"], "search-1");
        assert_eq!(v["backend"]["per_person"], true);
        assert_eq!(v["backend"]["search_tool"], "search");
        let back: Base = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(back, b);
        assert!(!v.to_string().contains("token"));
        assert_eq!(owner_for(true, None), None);
        assert_eq!(
            owner_for(true, Some("A@x.com")),
            Some(Owner::user("a@x.com"))
        );
        assert_eq!(owner_for(false, None), Some(Owner::Team));
    }
}
