//! Sanity through the Sanity sign-in: the same documents as [`super::sanity`],
//! read through Sanity's official remote MCP server (`https://mcp.sanity.io`)
//! with a team member's OAuth sign-in instead of an API token.
//!
//! The server registers Coderhelm dynamically, so this works in every
//! install, self-hosted ones included, with nothing to set up. Its token is
//! for the MCP server only: it is never sent to `api.sanity.io`.
//!
//! Tools used, all read-only (anything else is refused here):
//!
//! - `list_projects`: the picker's top level;
//! - `list_datasets` (`projectId`): a project's datasets;
//! - `query_documents` (`projectId`, `dataset`, `query`, `params`,
//!   `perspective`): document types, listings and documents, in GROQ;
//! - `whoami`: the account a sign-in belongs to, for "Connected as".
//!
//! The picker is project, then dataset, then document type. Ids carry the
//! project (`dataset:<project>:<dataset>`, `type:<project>:<dataset>:<type>`)
//! so a source knows which projects it reads without a separate setting.
//! One source can read datasets and types from several projects: each
//! project and dataset is listed, synced and queried on its own, and a
//! document's id is `<project>:<dataset>:<_id>` so ids never collide across
//! projects (ids saved as `<dataset>:<_id>`, before a source could read
//! several projects, are renamed to the source's first project on the next
//! sync).
//!
//! Changes: a document's `_rev` is its marker, as with the token. There is
//! no webhook through a sign-in, so syncs run on a schedule and each one
//! asks only for documents whose `_updatedAt` is after the newest one seen
//! (an incremental listing). Each project's dataset keeps its own cursor:
//! every [`FULL_EVERY_SECS`], and whenever what is picked in it changes, a
//! complete listing of it finds deleted documents too. A project the
//! sign-in can't read is named on the source while the others sync.
//! Drafts and release versions are never synced.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::connector::{matches, Connector, ConnectorError, Document, Listed, Listing, Node};
use super::mcp::{self, find_key, objects, result_json, scoped_args};
use super::sanity::{doc_sections, doc_url, type_map, valid_name, valid_project};
use super::{Kind, Source, MAX_ITEMS_PER_SOURCE};

/// The only tools this connector calls.
pub const READ_TOOLS: &[&str] = &[
    "list_projects",
    "list_datasets",
    "query_documents",
    "whoami",
];
/// The hosted server's address. Tokens from its sign-in go here only.
pub const MCP_URL: &str = "https://mcp.sanity.io";
/// Documents per query: well under the server's answer size cap.
pub const PAGE: usize = 100;
/// How often a complete listing runs (finding deleted documents).
pub const FULL_EVERY_SECS: i64 = 6 * 60 * 60;

const PROVIDER: &str = "Sanity";
const TITLE_FIELDS: &str = "coalesce(title, name, heading, label, _id)";
const NOT_DRAFT: &str = "!(_id in path(\"drafts.**\")) && !(_id in path(\"versions.**\"))";
const NOT_SYSTEM: &str =
    "!(_type match \"sanity.*\") && !(_type match \"system.*\") && !(_type match \"assist.*\")";

/// Is this the hosted Sanity MCP server (any path on it)?
pub fn is_sanity_mcp(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .is_some_and(|u| u.scheme() == "https" && u.host_str() == Some("mcp.sanity.io"))
}

/// A picker id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ref<'a> {
    Project(&'a str),
    Dataset {
        project: &'a str,
        dataset: &'a str,
    },
    Type {
        project: &'a str,
        dataset: &'a str,
        ty: &'a str,
    },
}

/// Reads `project:<p>`, `dataset:<p>:<d>` or `type:<p>:<d>:<t>`.
pub fn parse_ref(raw: &str) -> Option<Ref<'_>> {
    let (head, rest) = raw.split_once(':')?;
    let parts: Vec<&str> = rest.split(':').collect();
    let r = match (head, parts.as_slice()) {
        ("project", [p]) => Ref::Project(p),
        ("dataset", [p, d]) => Ref::Dataset {
            project: p,
            dataset: d,
        },
        ("type", [p, d, t]) => Ref::Type {
            project: p,
            dataset: d,
            ty: t,
        },
        _ => return None,
    };
    let ok = match r {
        Ref::Project(p) => valid_project(p),
        Ref::Dataset { project, dataset } => valid_project(project) && valid_name(dataset),
        Ref::Type {
            project,
            dataset,
            ty,
        } => valid_project(project) && valid_name(dataset) && valid_name(ty),
    };
    ok.then_some(r)
}

impl<'a> Ref<'a> {
    pub fn project(&self) -> &'a str {
        match self {
            Ref::Project(p) => p,
            Ref::Dataset { project, .. } | Ref::Type { project, .. } => project,
        }
    }
}

/// A dataset and the types picked in it (`None`: every content type).
pub type DatasetScope = (String, Option<Vec<String>>);

/// One project of a selection and what is picked in it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectScope {
    pub project: String,
    pub sets: Vec<DatasetScope>,
}

/// What a selection reads, grouped by project (in the order first picked).
/// A whole project can't be picked, and an empty selection reads nothing.
pub fn selection_scope(source: &Source) -> Result<Vec<ProjectScope>, String> {
    let mut out: Vec<ProjectScope> = Vec::new();
    for s in &source.selection {
        let r = parse_ref(&s.id)
            .ok_or_else(|| format!("\"{}\" isn't a Sanity dataset or type.", s.title))?;
        let (project, ds, ty) = match r {
            Ref::Project(_) => {
                return Err("Pick a dataset or document types, not a whole project.".into())
            }
            Ref::Dataset { project, dataset } => (project, dataset, None),
            Ref::Type {
                project,
                dataset,
                ty,
            } => (project, dataset, Some(ty)),
        };
        let p = match out.iter().position(|p| p.project == project) {
            Some(i) => i,
            None => {
                out.push(ProjectScope {
                    project: project.to_string(),
                    sets: Vec::new(),
                });
                out.len() - 1
            }
        };
        let sets = &mut out[p].sets;
        let at = match sets.iter().position(|(d, _)| d == ds) {
            Some(i) => i,
            None => {
                sets.push((ds.to_string(), Some(Vec::new())));
                sets.len() - 1
            }
        };
        match (ty, &mut sets[at].1) {
            (None, slot) => *slot = None,
            (Some(t), Some(list)) => {
                if !list.iter().any(|x| x == t) {
                    list.push(t.to_string())
                }
            }
            (Some(_), None) => {}
        }
    }
    if out.is_empty() {
        return Err("Pick at least one thing to sync.".into());
    }
    Ok(out)
}

/// Every project a selection reads, in the order first picked.
pub fn selection_projects(source: &Source) -> Result<Vec<String>, String> {
    Ok(selection_scope(source)?
        .into_iter()
        .map(|p| p.project)
        .collect())
}

/// The project a source read before a selection could span several: the
/// `project_id` saved on it, else the selection's only project. Items and
/// cursors saved then carry no project and belong to this one.
pub fn legacy_project(source: &Source) -> Option<String> {
    if let Some(p) = source.options["project_id"]
        .as_str()
        .filter(|p| valid_project(p))
    {
        return Some(p.to_string());
    }
    match selection_projects(source).ok()?.as_slice() {
        [one] => Some(one.clone()),
        _ => None,
    }
}

/// A document's stable id: `<project>:<dataset>:<_id>`, unique across the
/// projects of a source.
pub fn stable_id(project: &str, dataset: &str, id: &str) -> String {
    format!("{project}:{dataset}:{id}")
}

/// The project, dataset and `_id` in a stable id, or in an id saved before
/// ids carried the project (`<dataset>:<_id>`, read as `legacy`'s).
fn split_stable<'a>(ext: &'a str, legacy: Option<&'a str>) -> Option<(&'a str, &'a str, &'a str)> {
    let mut parts = ext.splitn(3, ':');
    let (a, b, c) = (parts.next()?, parts.next()?, parts.next());
    match c {
        Some(id) if valid_project(a) && valid_name(b) && !id.is_empty() => Some((a, b, id)),
        None if valid_name(a) && !b.is_empty() => Some((legacy?, a, b)),
        _ => None,
    }
}

/// Where the next listing of one project's dataset starts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    /// The newest `_updatedAt` seen.
    #[serde(default)]
    pub since: String,
    /// Unix seconds of the last complete listing.
    #[serde(default)]
    pub full_at: i64,
    /// The selection that listing covered (its hash).
    #[serde(default)]
    pub sel: String,
}

/// A source's cursor: one [`Cursor`] per `<project>:<dataset>`, so one
/// project's changes or failure never reset another's.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Cursors {
    #[serde(default)]
    pub sets: std::collections::BTreeMap<String, Cursor>,
}

fn set_key(project: &str, dataset: &str) -> String {
    format!("{project}:{dataset}")
}

fn hash_ids(mut ids: Vec<&str>) -> String {
    ids.sort();
    super::hex(&Sha256::digest(ids.join("\n").as_bytes()))[..16].to_string()
}

/// The hash of the whole selection (what a cursor saved before cursors were
/// per dataset covered).
fn selection_hash(source: &Source) -> String {
    hash_ids(source.selection.iter().map(|s| s.id.as_str()).collect())
}

/// The hash of what is picked in one project's dataset.
fn set_hash(source: &Source, project: &str, dataset: &str) -> String {
    hash_ids(
        source
            .selection
            .iter()
            .filter(|s| match parse_ref(&s.id) {
                Some(Ref::Dataset {
                    project: p,
                    dataset: d,
                })
                | Some(Ref::Type {
                    project: p,
                    dataset: d,
                    ..
                }) => p == project && d == dataset,
                _ => false,
            })
            .map(|s| s.id.as_str())
            .collect(),
    )
}

/// The source's cursors. One saved before cursors were per dataset (a
/// single `{since, full_at, sel}`) is read as that cursor for each dataset
/// of the [`legacy_project`]; it stays incremental only while the
/// selection is the one it covered.
pub fn read_cursors(source: &Source) -> Cursors {
    let Ok(v) = serde_json::from_str::<Value>(&source.cursor) else {
        return Cursors::default();
    };
    if v.get("sets").is_some() {
        return serde_json::from_value(v).unwrap_or_default();
    }
    let old: Cursor = serde_json::from_value(v).unwrap_or_default();
    let mut out = Cursors::default();
    let (Some(legacy), Ok(scope)) = (legacy_project(source), selection_scope(source)) else {
        return out;
    };
    if old.since.is_empty() {
        return out;
    }
    let same = old.sel == selection_hash(source);
    for p in scope.iter().filter(|p| p.project == legacy) {
        for (ds, _) in &p.sets {
            out.sets.insert(
                set_key(&p.project, ds),
                Cursor {
                    since: old.since.clone(),
                    full_at: old.full_at,
                    sel: if same {
                        set_hash(source, &p.project, ds)
                    } else {
                        String::new()
                    },
                },
            );
        }
    }
    out
}

/// `e`, saying which project it came from.
fn in_project(project: &str, e: ConnectorError) -> ConnectorError {
    let m = |s: String| format!("Sanity project {project}: {s}");
    match e {
        ConnectorError::Reconnect(s) => ConnectorError::Reconnect(m(s)),
        ConnectorError::Forbidden(s) => ConnectorError::Forbidden(m(s)),
        ConnectorError::NotFound(s) => ConnectorError::NotFound(m(s)),
        ConnectorError::RateLimited(s) => ConnectorError::RateLimited(m(s)),
        ConnectorError::Config(s) => ConnectorError::Config(m(s)),
        ConnectorError::Temporary(s) => ConnectorError::Temporary(m(s)),
    }
}

/// What a source shows when some of its projects couldn't be read and the
/// others synced.
fn projects_failed(failed: &[(String, ConnectorError)]) -> ConnectorError {
    let mut names: Vec<&str> = failed.iter().map(|(p, _)| p.as_str()).collect();
    names.dedup();
    let why = failed
        .first()
        .map(|(_, e)| e.to_string())
        .unwrap_or_default();
    let what = match names.as_slice() {
        [one] => format!("Sanity project {one} couldn't be read with this sign-in"),
        many => format!(
            "Sanity projects {} couldn't be read with this sign-in",
            many.join(", ")
        ),
    };
    let msg = format!("{what} ({why}). The other projects synced.");
    match failed.first().map(|(_, e)| e) {
        Some(ConnectorError::RateLimited(_)) => ConnectorError::RateLimited(msg),
        Some(ConnectorError::Temporary(_)) => ConnectorError::Temporary(msg),
        _ => ConnectorError::Forbidden(msg),
    }
}

/// How one dataset's listing went.
enum SetListing {
    /// Only what changed since its cursor.
    Changed(Vec<Listed>),
    /// Everything in it (`true`: more than a source keeps).
    All(Vec<Listed>, bool),
}

pub struct SanityMcp {
    pub tools: mcp::Tools,
    /// Unix seconds now.
    pub now: i64,
    /// Projects the team's repositories name (id, repository), offered when
    /// Sanity's own list leaves them out (see [`super::sanity_repos`]).
    pub hints: Vec<(String, String)>,
}

/// What the picker says when the sign-in works but Sanity lists no project
/// and none could be found another way.
pub const EMPTY_LIST: &str = "Sanity listed no projects for this sign-in. Sanity's list shows only projects the signed-in account is a member of by name, so a project reached through an organization role or an API token doesn't show. Type the project id in the search box (it's on sanity.io/manage, or `projectId` in your Studio's sanity.config) and Coderhelm opens it if this sign-in can read it.";

/// Most projects checked (by listing their datasets) for one picker view.
const MAX_CHECKED: usize = 8;

/// Keys a project's id is found under in Sanity's answers.
const ID_KEYS: &[&str] = &["id", "projectId", "project_id"];
const NAME_KEYS: &[&str] = &["displayName", "name", "title"];

/// The projects in a `list_projects` answer: every object (at any depth,
/// in the structured answer and in each text item) with a project id,
/// named by its display name when it has one.
pub fn projects_in(result: &Value) -> Result<Vec<(String, String)>, ConnectorError> {
    let mut answers: Vec<Value> = vec![result_json(PROVIDER, result)?];
    for t in result["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| c["text"].as_str())
    {
        if let Some(v) = mcp::data_in(t) {
            answers.push(v);
        }
    }
    let id_of = |m: &serde_json::Map<String, Value>| {
        ID_KEYS
            .iter()
            .find_map(|k| m.get(*k).and_then(Value::as_str))
            .filter(|id| valid_project(id))
            .map(str::to_string)
    };
    let mut out: Vec<(String, String)> = Vec::new();
    // Sanity's hosted server answers in tags, one element per project named
    // by its id: `<projects><abc123><name>Site</name>…</abc123></projects>`.
    for t in result["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| c["text"].as_str())
    {
        for (id, name) in tagged_projects(t) {
            if !out.iter().any(|(i, _)| *i == id) {
                out.push((id, name));
            }
        }
    }
    // A tagged answer names its projects by tag; its nested `<id>`s
    // (an organization's, say) are never projects.
    if !out.is_empty() {
        out.sort_by_key(|(_, name)| name.to_lowercase());
        return Ok(out);
    }
    for v in &answers {
        for m in objects(v, &|m| id_of(m).is_some()) {
            let id = id_of(m).unwrap_or_default();
            let name = NAME_KEYS
                .iter()
                .find_map(|k| m.get(*k).and_then(Value::as_str))
                .filter(|n| !n.trim().is_empty())
                .unwrap_or(&id)
                .to_string();
            if !out.iter().any(|(i, _)| *i == id) {
                out.push((id, name));
            }
        }
    }
    out.sort_by_key(|(_, name)| name.to_lowercase());
    Ok(out)
}

/// Projects in a tagged answer: each element directly inside `<projects>`
/// whose tag is a project id, named by its `<displayName>` or `<name>`
/// child. Anything else in the text is ignored.
fn tagged_projects(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(start) = text.find("<projects>") else {
        return out;
    };
    let body = &text[start + "<projects>".len()..];
    let body = &body[..body.find("</projects>").unwrap_or(body.len())];
    let mut rest = body;
    while let Some(open) = rest.find('<') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('>') else { break };
        let tag = after[..close].trim();
        let inner_start = &after[close + 1..];
        let end_tag = format!("</{tag}>");
        if tag.starts_with('/') || !valid_project(tag) {
            rest = inner_start;
            continue;
        }
        let Some(end) = inner_start.find(&end_tag) else {
            rest = inner_start;
            continue;
        };
        let inner = &inner_start[..end];
        let child = |name: &str| {
            let (o, c) = (format!("<{name}>"), format!("</{name}>"));
            let i = inner.find(&o)? + o.len();
            let j = inner[i..].find(&c)? + i;
            Some(inner[i..j].trim().to_string()).filter(|v| !v.is_empty())
        };
        // A project element holds fields; a bare `<name>`-like word with no
        // children is not a project.
        if inner.contains('<') {
            let name = child("displayName")
                .or_else(|| child("name"))
                .unwrap_or_else(|| tag.to_string());
            out.push((tag.to_string(), name));
        }
        rest = &inner_start[end + end_tag.len()..];
    }
    out
}

fn project_node(id: &str, title: String, detail: String) -> Node {
    Node {
        id: format!("project:{id}"),
        title,
        kind: "project".into(),
        has_children: true,
        selectable: false,
        detail,
        ..Default::default()
    }
}

impl SanityMcp {
    async fn find_tool(
        &self,
        name: &str,
    ) -> Result<crate::mcp_remote::client::RemoteTool, ConnectorError> {
        if !READ_TOOLS.contains(&name) {
            return Err(ConnectorError::Config(format!(
                "Coderhelm only reads from Sanity; it doesn't call {name}."
            )));
        }
        mcp::tool(self.tools.as_ref(), PROVIDER, &[name]).await
    }

    async fn call(&self, name: &str, args: Value) -> Result<Value, ConnectorError> {
        let t = self.find_tool(name).await?;
        let r = self.tools.call(&t, &args).await?;
        result_json(PROVIDER, &r)
    }

    /// Runs a GROQ query whose answer is an object `{<key>: ...}`, and
    /// returns what is under the key.
    pub async fn query(
        &self,
        project: &str,
        dataset: &str,
        key: &str,
        groq: &str,
        params: Value,
    ) -> Result<Value, ConnectorError> {
        if !valid_project(project) || !valid_name(dataset) {
            return Err(ConnectorError::Config(
                "The Sanity project or dataset isn't valid.".into(),
            ));
        }
        let t = self.find_tool("query_documents").await?;
        let mut rest = json!({ "query": format!("{{\"{key}\": {groq}}}") });
        if params.as_object().is_some_and(|m| !m.is_empty()) {
            rest["params"] = params;
        }
        if mcp::takes(&t, "perspective") {
            rest["perspective"] = json!("published");
        }
        if mcp::takes(&t, "limit") {
            rest["limit"] = json!(1);
        }
        let args = scoped_args(&t, project, Some(dataset), rest);
        let r = self.tools.call(&t, &args).await?;
        let v = result_json(PROVIDER, &r)?;
        find_key(&v, key).cloned().ok_or_else(|| {
            ConnectorError::Temporary(
                "Sanity's answer didn't include the documents asked for.".into(),
            )
        })
    }

    pub async fn projects(&self) -> Result<Vec<(String, String)>, ConnectorError> {
        let t = self.find_tool("list_projects").await?;
        let r = self.tools.call(&t, &json!({})).await?;
        let out = projects_in(&r)?;
        if out.is_empty() {
            // The answer's shape (never its text), so an empty picker can be
            // told apart from an answer this code doesn't read.
            tracing::warn!(
                provider = PROVIDER,
                shape = %mcp::answer_shape(&r),
                "Sanity's project list named no project"
            );
        }
        Ok(out)
    }

    /// The picker's top level: Sanity's project list, then projects the
    /// team's repositories name and a project id typed in the search box,
    /// each shown only when this sign-in can list its datasets.
    async fn top_level(&self, query: &str) -> Result<Vec<Node>, ConnectorError> {
        let listed = self.projects().await?;
        let typed = query.trim();
        let mut nodes: Vec<Node> = listed
            .iter()
            .filter(|(id, name)| matches(query, name) || matches(query, id))
            .map(|(id, name)| project_node(id, name.clone(), format!("Project {id}")))
            .collect();
        let mut extra: Vec<(String, String)> = Vec::new();
        for (id, repo) in &self.hints {
            if !listed.iter().any(|(i, _)| i == id)
                && !extra.iter().any(|(i, _)| i == id)
                && (matches(query, id) || matches(query, repo))
            {
                extra.push((id.clone(), format!("Project {id}, named in {repo}")));
            }
        }
        // A search that matches nothing listed or named may be a project id.
        if nodes.is_empty()
            && extra.is_empty()
            && valid_project(typed)
            && !listed.iter().any(|(i, _)| i == typed)
        {
            extra.push((typed.to_string(), format!("Project {typed}")));
        }
        for (id, detail) in extra.into_iter().take(MAX_CHECKED) {
            match self.datasets(&id).await {
                Ok(ds) if !ds.is_empty() => nodes.push(project_node(&id, id.clone(), detail)),
                Ok(_) => {}
                Err(e) => tracing::info!(
                    provider = PROVIDER,
                    error = %e,
                    "A Sanity project from the repositories or the search isn't readable with this sign-in"
                ),
            }
        }
        if nodes.is_empty() && typed.is_empty() {
            return Err(ConnectorError::Config(EMPTY_LIST.into()));
        }
        Ok(nodes)
    }

    pub async fn datasets(&self, project: &str) -> Result<Vec<String>, ConnectorError> {
        if !valid_project(project) {
            return Err(ConnectorError::Config(
                "That isn't a Sanity project id.".into(),
            ));
        }
        let t = self.find_tool("list_datasets").await?;
        let args = scoped_args(&t, project, None, json!({}));
        let r = self.tools.call(&t, &args).await?;
        let mut out = dataset_names(&result_json(PROVIDER, &r)?);
        if out.is_empty() {
            // Every Sanity project has a dataset, so an empty list is an answer
            // this code doesn't read. Say what it looked like (no content), and
            // ask once more with only the project id, the shape the hosted
            // server takes today, when the first ask used another.
            tracing::warn!(
                provider = PROVIDER,
                shape = %super::mcp::answer_shape(&r),
                args = ?args.as_object().map(|m| m.keys().cloned().collect::<Vec<_>>()),
                takes = ?t.input_schema["properties"].as_object().map(|m| m.keys().cloned().collect::<Vec<_>>()),
                "Sanity listed no datasets for a project"
            );
            if args.get("projectId").is_none() {
                let r = self
                    .tools
                    .call(&t, &json!({ "projectId": project }))
                    .await?;
                if let Ok(v) = result_json(PROVIDER, &r) {
                    out = dataset_names(&v);
                }
            }
        }
        if out.is_empty() {
            return Err(ConnectorError::Temporary(format!(
                "Sanity didn't list any datasets for project {project}. Check that your Sanity sign-in can open it in Sanity, then try again."
            )));
        }
        Ok(out)
    }

    async fn types(&self, project: &str, dataset: &str) -> Result<Vec<String>, ConnectorError> {
        let v = self
            .query(
                project,
                dataset,
                "chTypes",
                &format!("array::unique(*[{NOT_DRAFT}]._type)"),
                json!({}),
            )
            .await?;
        let mut out: Vec<String> = v
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|t| {
                !t.starts_with("sanity.") && !t.starts_with("system.") && !t.starts_with("assist.")
            })
            .map(str::to_string)
            .collect();
        out.sort();
        Ok(out)
    }

    /// The account the sign-in belongs to ("Ada Lovelace" or an email), for
    /// "Connected as". Empty when the server doesn't say.
    pub async fn account(&self) -> String {
        let Ok(v) = self.call("whoami", json!({})).await else {
            return String::new();
        };
        ["name", "displayName", "email"]
            .iter()
            .find_map(|k| find_key(&v, k).and_then(Value::as_str))
            .map(|s| crate::truncate_str(s.trim(), 120).to_string())
            .unwrap_or_default()
    }

    /// Per project, per dataset: the picked types, or `None` for every
    /// content type.
    fn scope(source: &Source) -> Result<Vec<ProjectScope>, ConnectorError> {
        selection_scope(source).map_err(ConnectorError::Config)
    }

    /// Can this sign-in read every one of `projects`? The first one it
    /// can't is named in the error.
    pub async fn check_projects(&self, projects: &[String]) -> Result<(), ConnectorError> {
        for p in projects {
            self.datasets(p).await.map_err(|e| in_project(p, e))?;
        }
        Ok(())
    }

    fn filter(types: &Option<Vec<String>>) -> (String, Value) {
        match types {
            Some(t) => (
                format!("_type in $types && {NOT_DRAFT}"),
                json!({ "types": t }),
            ),
            None => (format!("{NOT_DRAFT} && {NOT_SYSTEM}"), json!({})),
        }
    }

    /// `many`: the source reads several projects, so the place shown names
    /// the project too.
    fn listed(project: &str, ds: &str, many: bool, d: &Value) -> Option<Listed> {
        let (id, ty) = (d["_id"].as_str()?, d["_type"].as_str()?);
        Some(Listed {
            external_id: stable_id(project, ds, id),
            title: d["title"].as_str().unwrap_or(id).to_string(),
            url: String::new(),
            updated_at: d["_updatedAt"].as_str().unwrap_or("").to_string(),
            version: d["_rev"].as_str().unwrap_or("").to_string(),
            path: if many {
                format!("{project} / {ds} / {ty}")
            } else {
                format!("{ds} / {ty}")
            },
            hint: json!({ "project": project, "dataset": ds, "id": id, "type": ty }),
        })
    }

    /// Every document one dataset's pick covers, a page at a time by `_id`,
    /// at most `room` of them (`true`: there were more).
    async fn list_all(
        &self,
        project: &str,
        (ds, types): &DatasetScope,
        many: bool,
        room: usize,
    ) -> Result<(Vec<Listed>, bool), ConnectorError> {
        let projection = format!("{{_id, _type, _rev, _updatedAt, \"title\": {TITLE_FIELDS}}}");
        let mut items = Vec::new();
        let (filter, base) = Self::filter(types);
        let mut after = String::new();
        loop {
            let mut params = base.clone();
            params["after"] = json!(after);
            let v = self
                .query(
                    project,
                    ds,
                    "chPage",
                    &format!(
                        "*[{filter} && _id > $after] | order(_id asc) [0...{PAGE}] {projection}"
                    ),
                    params,
                )
                .await?;
            let page = v.as_array().cloned().unwrap_or_default();
            for d in &page {
                if let Some(l) = Self::listed(project, ds, many, d) {
                    items.push(l);
                }
            }
            if items.len() > room {
                items.truncate(room);
                return Ok((items, true));
            }
            match page.last().and_then(|d| d["_id"].as_str()) {
                Some(last) if page.len() >= PAGE && last > after.as_str() => {
                    after = last.to_string()
                }
                _ => break,
            }
        }
        Ok((items, false))
    }

    /// One dataset's documents changed after `since`, oldest first. `None`
    /// when more than `room` changed (a complete listing is due then).
    async fn list_changed(
        &self,
        project: &str,
        (ds, types): &DatasetScope,
        many: bool,
        since: &str,
        room: usize,
    ) -> Result<Option<Vec<Listed>>, ConnectorError> {
        let projection = format!("{{_id, _type, _rev, _updatedAt, \"title\": {TITLE_FIELDS}}}");
        let mut items = Vec::new();
        let (filter, base) = Self::filter(types);
        let (mut at, mut after) = (since.to_string(), String::new());
        loop {
            let mut params = base.clone();
            params["since"] = json!(at);
            params["after"] = json!(after);
            let v = self
                .query(
                    project,
                    ds,
                    "chPage",
                    &format!("*[{filter} && (_updatedAt > $since || (_updatedAt == $since && _id > $after))] | order(_updatedAt asc, _id asc) [0...{PAGE}] {projection}"),
                    params,
                )
                .await?;
            let page = v.as_array().cloned().unwrap_or_default();
            for d in &page {
                if let Some(l) = Self::listed(project, ds, many, d) {
                    items.push(l);
                }
            }
            if items.len() > room {
                return Ok(None);
            }
            let last = page.last();
            match (
                last.and_then(|d| d["_updatedAt"].as_str()),
                last.and_then(|d| d["_id"].as_str()),
            ) {
                (Some(u), Some(i)) if page.len() >= PAGE => {
                    at = u.to_string();
                    after = i.to_string();
                }
                _ => break,
            }
        }
        Ok(Some(items))
    }

    /// One dataset: only what changed when its cursor allows, else
    /// everything.
    async fn list_set(
        &self,
        project: &str,
        set: &DatasetScope,
        many: bool,
        prev: Option<&Cursor>,
        sel: &str,
        room: usize,
    ) -> Result<SetListing, ConnectorError> {
        if let Some(p) = prev.filter(|p| {
            !p.since.is_empty() && p.sel == sel && self.now - p.full_at < FULL_EVERY_SECS
        }) {
            if let Some(items) = self
                .list_changed(project, set, many, &p.since, room)
                .await?
            {
                return Ok(SetListing::Changed(items));
            }
        }
        let (items, more) = self.list_all(project, set, many, room).await?;
        Ok(SetListing::All(items, more))
    }

    /// The source's options as one project's documents read them: the
    /// Studio address is that project's (`studio_urls`), or the source's
    /// one Studio address when it reads one project or this is the project
    /// it was set for.
    fn options_for(source: &Source, project: &str, many: bool) -> Value {
        let mut o = source.options.clone();
        let own = source.options["studio_urls"][project]
            .as_str()
            .filter(|u| u.starts_with("https://"))
            .map(str::to_string);
        let shared = source.options["studio_url"].as_str().unwrap_or("");
        let studio = match own {
            Some(u) => u,
            None if !many || legacy_project(source).as_deref() == Some(project) => {
                shared.to_string()
            }
            None => String::new(),
        };
        if o.is_object() {
            o["studio_url"] = json!(studio);
        }
        o
    }
}

fn newest(items: &[Listed], or: &str) -> String {
    items
        .iter()
        .map(|i| i.updated_at.as_str())
        .chain(std::iter::once(or))
        .max()
        .unwrap_or("")
        .to_string()
}

/// The dataset names in a `list_datasets` answer (not the comments dataset).
fn dataset_names(v: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |n: &str| {
        if valid_name(n) && !out.iter().any(|x| x == n) {
            out.push(n.to_string());
        }
    };
    for m in objects(v, &|m| {
        m.get("name").and_then(Value::as_str).is_some()
            && m.get("datasetProfile").and_then(Value::as_str) != Some("comments")
    }) {
        push(m["name"].as_str().unwrap_or(""));
    }
    match find_key(v, "datasets") {
        Some(Value::Array(a)) => {
            for n in a.iter().filter_map(Value::as_str) {
                push(n);
            }
        }
        // Sanity's server answers in XML with each dataset as an element
        // named after it (`<datasets><production><aclMode>public</aclMode>
        // ...</production></datasets>`): the names are the keys.
        Some(Value::Object(m)) => {
            for (name, d) in m {
                if d.get("datasetProfile").and_then(Value::as_str) != Some("comments") {
                    push(name);
                }
            }
        }
        _ => {}
    }
    out.sort();
    out
}

#[async_trait::async_trait]
impl Connector for SanityMcp {
    fn kind(&self) -> Kind {
        Kind::Sanity
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        match parent.and_then(parse_ref) {
            None => self.top_level(query).await,
            Some(Ref::Project(p)) => Ok(self
                .datasets(p)
                .await?
                .into_iter()
                .filter(|d| matches(query, d))
                .map(|d| Node {
                    id: format!("dataset:{p}:{d}"),
                    title: d.clone(),
                    kind: "dataset".into(),
                    has_children: true,
                    selectable: true,
                    detail: "Every content type".into(),
                    ..Default::default()
                })
                .collect()),
            Some(Ref::Dataset { project, dataset })
            | Some(Ref::Type {
                project, dataset, ..
            }) => Ok(self
                .types(project, dataset)
                .await?
                .into_iter()
                .filter(|t| matches(query, t))
                .map(|t| Node {
                    id: format!("type:{project}:{dataset}:{t}"),
                    title: t.clone(),
                    kind: "type".into(),
                    has_children: false,
                    selectable: true,
                    detail: "Document type".into(),
                    ..Default::default()
                })
                .collect()),
        }
    }

    /// Every project and dataset of the selection is listed on its own
    /// cursor. A dataset listed in full makes the listing complete for its
    /// documents only; one listed from its cursor, or whose project couldn't
    /// be read, keeps what is stored for it (`partial`). A project that
    /// can't be read is named in the source's status while the others sync.
    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let scope = Self::scope(source)?;
        let many = scope.len() > 1;
        let prev = read_cursors(source);
        let mut next = Cursors::default();
        let mut items: Vec<Listed> = Vec::new();
        let mut partial: Vec<String> = Vec::new();
        let mut failed: Vec<(String, ConnectorError)> = Vec::new();
        let (mut any_full, mut truncated, mut sets) = (false, false, 0usize);
        for p in &scope {
            for set in &p.sets {
                sets += 1;
                let key = set_key(&p.project, &set.0);
                let prefix = format!("{key}:");
                let old = prev.sets.get(&key);
                let keep_old = |next: &mut Cursors| {
                    if let Some(c) = old {
                        next.sets.insert(key.clone(), c.clone());
                    }
                };
                let room = MAX_ITEMS_PER_SOURCE.saturating_sub(items.len());
                if room == 0 {
                    // The source is full: what is stored here stays.
                    truncated = true;
                    partial.push(prefix);
                    keep_old(&mut next);
                    continue;
                }
                let sel = set_hash(source, &p.project, &set.0);
                match self.list_set(&p.project, set, many, old, &sel, room).await {
                    Ok(SetListing::Changed(changed)) => {
                        let c = old.cloned().unwrap_or_default();
                        next.sets.insert(
                            key,
                            Cursor {
                                since: newest(&changed, &c.since),
                                ..c
                            },
                        );
                        partial.push(prefix);
                        items.extend(changed);
                    }
                    Ok(SetListing::All(all, more)) => {
                        next.sets.insert(
                            key,
                            Cursor {
                                since: newest(&all, ""),
                                full_at: self.now,
                                sel,
                            },
                        );
                        any_full = true;
                        if more {
                            // Beyond what a source keeps: nothing stored here
                            // is removed by this listing.
                            truncated = true;
                            partial.push(prefix);
                        }
                        items.extend(all);
                    }
                    Err(e) => {
                        tracing::warn!(
                            provider = PROVIDER,
                            project = %p.project,
                            dataset = %set.0,
                            error = %e,
                            "A Sanity project's dataset wasn't listed; the others sync"
                        );
                        keep_old(&mut next);
                        partial.push(prefix);
                        failed.push((p.project.clone(), e));
                    }
                }
            }
        }
        // Nothing could be read: the source stops with the reason (an
        // expired sign-in asks to sign in again, as before).
        if !failed.is_empty() && failed.len() == sets {
            let (project, e) = failed.swap_remove(0);
            return Err(if many { in_project(&project, e) } else { e });
        }
        if !any_full {
            truncated = truncated || source.truncated;
        }
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: any_full,
            partial,
            cursor: serde_json::to_string(&next).unwrap_or_default(),
            truncated,
            problem: (!failed.is_empty()).then(|| projects_failed(&failed)),
        })
    }

    async fn fetch(&self, source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let projects = selection_projects(source).map_err(ConnectorError::Config)?;
        let legacy = legacy_project(source);
        let from_id = split_stable(&item.external_id, legacy.as_deref());
        let project = item.hint["project"]
            .as_str()
            .or(from_id.map(|(p, _, _)| p))
            .or(legacy.as_deref())
            .unwrap_or("");
        if !projects.iter().any(|p| p == project) {
            return Err(ConnectorError::NotFound(
                "The document's Sanity project isn't picked any more.".into(),
            ));
        }
        let ds = item.hint["dataset"]
            .as_str()
            .or(from_id.map(|(_, d, _)| d))
            .unwrap_or("");
        let id = item.hint["id"]
            .as_str()
            .or(from_id.map(|(_, _, i)| i))
            .unwrap_or("");
        let v = self
            .query(
                project,
                ds,
                "chDoc",
                "*[_id == $id][0]",
                json!({ "id": id }),
            )
            .await?;
        if v.is_null() {
            return Err(ConnectorError::NotFound("The document was deleted.".into()));
        }
        let options = Self::options_for(source, project, projects.len() > 1);
        let ty = v["_type"].as_str().unwrap_or("");
        let map = type_map(&options, ty);
        let title = if map.title.is_empty() {
            item.title.clone()
        } else {
            super::text::at_path(&v, &map.title)
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| item.title.clone())
        };
        Ok(Document {
            url: doc_url(&v, &map, &options),
            updated_at: v["_updatedAt"]
                .as_str()
                .unwrap_or(&item.updated_at)
                .to_string(),
            sections: doc_sections(&v, &title, &map),
            title,
        })
    }

    /// Ids saved before they carried the project (`<dataset>:<_id>`) are the
    /// [`legacy_project`]'s.
    fn current_id(&self, source: &Source, external_id: &str) -> Option<String> {
        let legacy = legacy_project(source)?;
        let (p, d, i) = split_stable(external_id, Some(&legacy))?;
        let now = stable_id(p, d, i);
        (now != external_id).then_some(now)
    }
}

#[cfg(test)]
mod tests {
    use super::super::mcp::fake::{text_result, tool_with, FakeMcp};
    use super::super::{plan_sync, Selected};
    use super::*;
    use crate::knowledge::{FileMeta, FileStatus};
    use std::sync::Arc;

    fn server(
        handler: impl Fn(&str, &Value) -> Result<Value, ConnectorError> + Send + Sync + 'static,
    ) -> Arc<FakeMcp> {
        Arc::new(FakeMcp::new(
            vec![
                tool_with("list_projects", &[]),
                tool_with("list_datasets", &["projectId"]),
                tool_with(
                    "query_documents",
                    &["projectId", "dataset", "query", "params", "perspective"],
                ),
                tool_with("whoami", &[]),
                tool_with("create_documents", &["projectId", "dataset"]),
            ],
            handler,
        ))
    }

    fn source(ids: &[&str]) -> Source {
        Source {
            selection: ids
                .iter()
                .map(|i| Selected {
                    id: i.to_string(),
                    title: i.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Sanity's server, as it answered a real project: a sentence, then XML
    /// with each dataset as an element named after it.
    #[tokio::test]
    async fn datasets_named_by_their_elements_are_listed() {
        let reply = "Here are the datasets: \n<datasets>\n  <production>\n    <aclMode>public</aclMode>\n    <createdAt>2024-01-08T21:23:57.066Z</createdAt>\n  </production>\n  <staging>\n    <aclMode>private</aclMode>\n    <createdAt>2024-02-01T10:00:00.000Z</createdAt>\n  </staging>\n  <comments>\n    <aclMode>private</aclMode>\n    <datasetProfile>comments</datasetProfile>\n  </comments>\n</datasets>";
        let v = super::super::mcp::data_in(reply).expect("the XML is read");
        assert_eq!(dataset_names(&v), vec!["production", "staging"]);

        let mcp = server(move |tool, _| match tool {
            "list_datasets" => Ok(json!({"content": [{"type": "text", "text": reply}]})),
            _ => Ok(text_result(&json!([]))),
        });
        let c = SanityMcp {
            tools: mcp,
            now: 0,
            hints: Vec::new(),
        };
        let nodes = c
            .browse(Some("project:abc123"), "")
            .await
            .expect("the datasets are listed");
        let titles: Vec<&str> = nodes.iter().map(|n| n.title.as_str()).collect();
        assert_eq!(titles, vec!["production", "staging"]);
    }

    /// A query answered the way Sanity's server formats it: a sentence, then
    /// `<documents><document>` holding the JSON of the one result.
    #[tokio::test]
    async fn a_query_answered_in_sanitys_xml_is_read() {
        let mcp = server(|tool, _| match tool {
            "query_documents" => Ok(json!({"content": [{"type": "text", "text":
                "Query executed successfully. Found 1 total document, returning 1 (40 tokens): \n<documents>\n  <document>{\n  \"chTypes\": [\n    \"post\",\n    \"venue\"\n  ]\n}</document>\n</documents>\n<count>1</count>\n<totalAvailable>1</totalAvailable>\n"}]})),
            _ => Ok(text_result(&json!([]))),
        });
        let c = SanityMcp {
            tools: mcp,
            now: 0,
            hints: Vec::new(),
        };
        let v = c
            .query(
                "abc123",
                "production",
                "chTypes",
                "array::unique(*[]._type)",
                json!({}),
            )
            .await
            .expect("the answer is read");
        assert_eq!(v, json!(["post", "venue"]));
    }

    fn doc(id: &str, rev: &str, at: &str) -> Value {
        json!({"_id": id, "_type": "post", "_rev": rev, "_updatedAt": at, "title": format!("Post {id}")})
    }

    #[tokio::test]
    async fn a_project_with_no_readable_datasets_is_an_error_never_an_empty_list() {
        // The tool says it takes `resource` (the first ask); only a plain `projectId` answers.
        let f = Arc::new(FakeMcp::new(
            vec![
                tool_with("list_projects", &[]),
                tool_with("list_datasets", &["resource"]),
            ],
            |name, args| {
                Ok(match name {
                    "list_datasets"
                        if args.get("projectId").is_some() && args.get("resource").is_none() =>
                    {
                        text_result(&json!({"datasets": {"production": {"name": "production"}}}))
                    }
                    "list_datasets" => text_result(&json!({"datasets": {}})),
                    _ => unreachable!(),
                })
            },
        ));
        let c = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        };
        let kids = c.browse(Some("project:abc123"), "").await.unwrap();
        assert_eq!(
            kids.iter().map(|n| n.title.as_str()).collect::<Vec<_>>(),
            ["production"]
        );

        let empty = server(|_, _| Ok(text_result(&json!({"datasets": {}}))));
        let c = SanityMcp {
            tools: empty,
            now: 0,
            hints: Vec::new(),
        };
        let e = c.browse(Some("project:abc123"), "").await.unwrap_err();
        assert!(
            e.to_string()
                .contains("didn't list any datasets for project abc123"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn the_picker_lists_projects_then_datasets_then_types() {
        let f = server(|name, args| {
            Ok(match name {
                "list_projects" => text_result(&json!({"orgs": {"Organization o1": [
                    {"id": "zp7mbokg", "name": "Marketing site", "members": 3},
                    {"id": "abc123", "displayName": "Docs"}
                ]}})),
                "list_datasets" => {
                    assert_eq!(args["projectId"], "abc123");
                    text_result(
                        &json!({"datasets": {"production": {"name": "production"}, "comments": {"name": "comments", "datasetProfile": "comments"}}}),
                    )
                }
                "query_documents" => {
                    assert_eq!(args["dataset"], "production");
                    assert_eq!(args["perspective"], "published");
                    text_result(
                        &json!({"documents": [{"chTypes": ["post", "sanity.imageAsset", "author"]}]}),
                    )
                }
                _ => unreachable!(),
            })
        });
        let c = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        };
        let top = c.browse(None, "").await.unwrap();
        assert_eq!(
            top.iter().map(|n| n.title.as_str()).collect::<Vec<_>>(),
            ["Docs", "Marketing site"]
        );
        assert_eq!(top[0].id, "project:abc123");
        assert!(!top[0].selectable && top[0].has_children);
        let ds = c.browse(Some("project:abc123"), "").await.unwrap();
        assert_eq!(ds.len(), 1, "the comments dataset is left out");
        assert_eq!(ds[0].id, "dataset:abc123:production");
        let types = c.browse(Some(&ds[0].id), "").await.unwrap();
        assert_eq!(
            types.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            [
                "type:abc123:production:author",
                "type:abc123:production:post"
            ]
        );
    }

    #[test]
    fn project_ids_are_read_under_every_key_and_from_text_beside_structured_content() {
        let r = json!({
            "structuredContent": {"result": "ok"},
            "content": [{"type": "text", "text": "Found 2 projects: {\"projects\": [{\"projectId\": \"9ab8cd7e\", \"displayName\": \"Site\"}, {\"project_id\": \"zz11\"}]}"}]
        });
        assert_eq!(
            projects_in(&r).unwrap(),
            [
                ("9ab8cd7e".to_string(), "Site".to_string()),
                ("zz11".to_string(), "zz11".to_string())
            ]
        );
        let err =
            json!({"isError": true, "content": [{"type": "text", "text": "No projects found"}]});
        assert!(projects_in(&err).is_err(), "an error answer stays an error");
    }

    #[test]
    fn project_ids_are_read_from_a_tagged_answer() {
        let text = "Found 2 projects in 1 organization: \n<projects>\n  <5tubhumo>\n    <name>Mobile app</name>\n    <organization>\n      <id>o1</id>\n    </organization>\n  </5tubhumo>\n  <9ab8cd7e>\n    <displayName>Site</displayName>\n    <name>site</name>\n  </9ab8cd7e>\n</projects>";
        let r = json!({"content": [{"type": "text", "text": text}]});
        assert_eq!(
            projects_in(&r).unwrap(),
            [
                ("5tubhumo".to_string(), "Mobile app".to_string()),
                ("9ab8cd7e".to_string(), "Site".to_string())
            ]
        );
        assert!(tagged_projects("<projects></projects>").is_empty());
        assert!(
            tagged_projects("<projects><name>x</name></projects>").is_empty(),
            "a field with no children is never a project"
        );
        assert!(tagged_projects("no tags here").is_empty());
    }

    /// A sign-in whose project list is empty, but which reads `readable`.
    fn member_of_nothing(readable: &'static str) -> Arc<FakeMcp> {
        server(move |name, args| {
            Ok(match name {
                "list_projects" => text_result(&json!({"orgs": {}})),
                "list_datasets" if args["projectId"] == readable => {
                    text_result(&json!({"datasets": {"production": {"name": "production"}}}))
                }
                "list_datasets" => {
                    json!({"isError": true, "content": [{"type": "text", "text": "Project not found"}]})
                }
                _ => unreachable!(),
            })
        })
    }

    #[tokio::test]
    async fn projects_the_repositories_name_show_when_sanitys_list_leaves_them_out() {
        let f = member_of_nothing("9ab8cd7e");
        let c = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: vec![
                ("9ab8cd7e".into(), "acme/site".into()),
                ("nope1234".into(), "acme/other".into()),
            ],
        };
        let top = c.browse(None, "").await.unwrap();
        assert_eq!(top.len(), 1, "only the project this sign-in can read");
        assert_eq!(top[0].id, "project:9ab8cd7e");
        assert_eq!(top[0].detail, "Project 9ab8cd7e, named in acme/site");
        let ds = c.browse(Some(&top[0].id), "").await.unwrap();
        assert_eq!(ds[0].id, "dataset:9ab8cd7e:production");
    }

    #[tokio::test]
    async fn a_project_id_typed_in_the_search_opens_when_the_sign_in_reads_it() {
        let c = SanityMcp {
            tools: member_of_nothing("9ab8cd7e"),
            now: 0,
            hints: Vec::new(),
        };
        let found = c.browse(None, " 9ab8cd7e ").await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, "project:9ab8cd7e");
        assert!(
            c.browse(None, "nope1234").await.unwrap().is_empty(),
            "an id the sign-in can't read is not offered"
        );
    }

    #[tokio::test]
    async fn an_empty_list_says_why_and_what_to_do() {
        let c = SanityMcp {
            tools: member_of_nothing("9ab8cd7e"),
            now: 0,
            hints: Vec::new(),
        };
        let e = c.browse(None, "").await.unwrap_err();
        assert_eq!(e.to_string(), EMPTY_LIST);
        assert!(e
            .to_string()
            .contains("Type the project id in the search box"));
        assert!(!e.to_string().contains("hasn't shared anything"));
    }

    #[tokio::test]
    async fn the_picker_reads_sanitys_own_xml_answers() {
        use super::super::mcp::tests::{SANITY_DATASETS, SANITY_PROJECTS, SANITY_QUERY};
        let raw = |t: &str| json!({"content": [{"type": "text", "text": t}]});
        let f = server(move |name, _| {
            Ok(match name {
                "list_projects" => raw(SANITY_PROJECTS),
                "list_datasets" => raw(SANITY_DATASETS),
                "query_documents" => raw(SANITY_QUERY),
                _ => unreachable!(),
            })
        });
        let c = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        };
        let top = c.browse(None, "").await.unwrap();
        assert_eq!(
            top.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            ["project:abc123", "project:zp7mbokg"]
        );
        let searched = c.browse(None, "docs").await.unwrap();
        assert_eq!(
            searched.len(),
            1,
            "the picker's search reads the same answer"
        );
        let ds = c.browse(Some("project:abc123"), "").await.unwrap();
        assert_eq!(
            ds.iter().map(|n| n.title.as_str()).collect::<Vec<_>>(),
            ["production", "staging"]
        );
        let types = c.browse(Some(&ds[0].id), "").await.unwrap();
        assert_eq!(
            types.iter().map(|n| n.title.as_str()).collect::<Vec<_>>(),
            ["author", "post"]
        );
    }

    #[tokio::test]
    async fn the_first_listing_is_complete_and_pages_by_id() {
        let f = server(|_, args| {
            let q = args["query"].as_str().unwrap();
            assert!(q.starts_with("{\"chPage\": *[_type in $types"));
            assert!(q.contains("drafts.**") && q.contains("versions.**"));
            assert_eq!(args["params"]["types"], json!(["post"]));
            let after = args["params"]["after"].as_str().unwrap();
            let page: Vec<Value> = if after.is_empty() {
                (0..PAGE)
                    .map(|i| doc(&format!("d{i:03}"), "r1", "2026-10-01T00:00:00Z"))
                    .collect()
            } else {
                assert_eq!(after, format!("d{:03}", PAGE - 1));
                vec![doc("z1", "r2", "2026-10-03T00:00:00Z")]
            };
            Ok(text_result(&json!({"chPage": page})))
        });
        let c = SanityMcp {
            tools: f.clone(),
            now: 1_000,
            hints: Vec::new(),
        };
        let l = c
            .list(&source(&["type:abc123:production:post"]))
            .await
            .unwrap();
        assert!(l.complete);
        assert_eq!(l.items.len(), PAGE + 1);
        assert_eq!(l.items[0].external_id, "abc123:production:d000");
        assert_eq!(l.items[0].version, "r1");
        assert_eq!(l.items[0].path, "production / post");
        let cur: Cursors = serde_json::from_str(&l.cursor).unwrap();
        let cur = &cur.sets["abc123:production"];
        assert_eq!(cur.since, "2026-10-03T00:00:00Z");
        assert_eq!(cur.full_at, 1_000);
        assert_eq!(f.calls().len(), 2);
    }

    #[tokio::test]
    async fn later_syncs_ask_only_for_what_changed_since_the_last_one() {
        let f = server(|_, args| {
            let q = args["query"].as_str().unwrap();
            assert!(q.contains("_updatedAt > $since"), "{q}");
            assert_eq!(args["params"]["since"], "2026-10-03T00:00:00Z");
            Ok(text_result(
                &json!({"chPage": [doc("p2", "r9", "2026-10-04T00:00:00Z")]}),
            ))
        });
        let c = SanityMcp {
            tools: f.clone(),
            now: 2_000,
            hints: Vec::new(),
        };
        // A cursor saved before cursors were per dataset: read as this
        // project's, and still incremental (the selection is the same).
        let mut src = source(&["type:abc123:production:post"]);
        src.cursor = serde_json::to_string(&Cursor {
            since: "2026-10-03T00:00:00Z".into(),
            full_at: 1_000,
            sel: selection_hash(&src),
        })
        .unwrap();
        let l = c.list(&src).await.unwrap();
        assert!(
            !l.complete,
            "an incremental listing removes nothing by itself"
        );
        assert_eq!(l.items.len(), 1);
        let cur: Cursors = serde_json::from_str(&l.cursor).unwrap();
        assert_eq!(cur.sets.len(), 1, "saved again in the new form");
        let cur = &cur.sets["abc123:production"];
        assert_eq!(cur.since, "2026-10-04T00:00:00Z");
        assert_eq!(cur.full_at, 1_000);
        assert_eq!(cur.sel, set_hash(&src, "abc123", "production"));
        // The engine fetches only the changed document and removes nothing.
        let stored = |ext: &str, v: &str| FileMeta {
            external_id: ext.into(),
            remote_version: v.into(),
            status: FileStatus::Ready,
            ready_version: 1,
            ..Default::default()
        };
        let plan = plan_sync(
            &[
                stored("abc123:production:p1", "r1"),
                stored("abc123:production:p2", "r1"),
            ],
            &l,
        );
        assert_eq!(plan.fetch.len(), 1);
        assert!(plan.remove.is_empty());
    }

    #[tokio::test]
    async fn a_complete_listing_runs_again_after_six_hours_or_a_new_selection() {
        let f = server(|_, args| {
            assert!(args["query"].as_str().unwrap().contains("_id > $after"));
            Ok(text_result(
                &json!({"chPage": [doc("p1", "r1", "2026-10-01T00:00:00Z")]}),
            ))
        });
        let mut src = source(&["type:abc123:production:post"]);
        src.cursor = serde_json::to_string(&Cursor {
            since: "2026-10-03T00:00:00Z".into(),
            full_at: 0,
            sel: selection_hash(&src),
        })
        .unwrap();
        let l = SanityMcp {
            tools: f.clone(),
            now: FULL_EVERY_SECS + 1,
            hints: Vec::new(),
        }
        .list(&src)
        .await
        .unwrap();
        assert!(l.complete);
        let mut changed = src.clone();
        changed.cursor = serde_json::to_string(&Cursor {
            full_at: FULL_EVERY_SECS,
            ..serde_json::from_str(&src.cursor).unwrap()
        })
        .unwrap();
        changed.selection[0].id = "dataset:abc123:production".into();
        let l = SanityMcp {
            tools: f.clone(),
            now: FULL_EVERY_SECS + 2,
            hints: Vec::new(),
        }
        .list(&changed)
        .await
        .unwrap();
        assert!(l.complete);
    }

    #[tokio::test]
    async fn a_document_is_read_and_mapped_like_the_token_path() {
        let f = server(|_, args| {
            assert_eq!(args["params"]["id"], "p1");
            assert!(args["query"]
                .as_str()
                .unwrap()
                .starts_with("{\"chDoc\": *[_id == $id][0]"));
            Ok(
                json!({"content": [], "structuredContent": {"result": {"chDoc": {
                    "_id": "p1", "_type": "post", "_updatedAt": "2026-10-02T00:00:00Z",
                    "headline": "Returns policy", "slug": {"current": "returns"},
                    "body": [{"_type": "block", "style": "normal", "children": [{"text": "Thirty days."}]}]
                }}}}),
            )
        });
        let mut src = source(&["type:abc123:production:post"]);
        src.options = json!({"types": {"post": {"title": "headline", "body": ["body"], "url": "https://shop.example/help/{slug.current}"}}});
        let d = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        }
        .fetch(
            &src,
            &Listed {
                hint: json!({"dataset": "production", "id": "p1", "type": "post"}),
                title: "p1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(d.title, "Returns policy");
        assert_eq!(d.url, "https://shop.example/help/returns");
        assert!(d.sections.iter().any(|s| s.text.contains("Thirty days.")));
    }

    #[tokio::test]
    async fn a_deleted_document_is_gone_and_an_expired_sign_in_asks_to_sign_in_again() {
        let f = server(|_, _| Ok(text_result(&json!({"chDoc": null}))));
        let item = Listed {
            hint: json!({"dataset": "production", "id": "gone"}),
            ..Default::default()
        };
        let src = source(&["dataset:abc123:production"]);
        let e = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        }
        .fetch(&src, &item)
        .await
        .unwrap_err();
        assert!(e.item_gone());
        let expired = server(|_, _| Err(ConnectorError::Reconnect("The sign-in expired.".into())));
        let e = SanityMcp {
            tools: expired.clone(),
            now: 0,
            hints: Vec::new(),
        }
        .list(&src)
        .await
        .unwrap_err();
        assert!(matches!(e, ConnectorError::Reconnect(_)));
    }

    #[tokio::test]
    async fn only_read_tools_are_ever_called() {
        let f = server(|_, _| Ok(text_result(&json!({}))));
        let c = SanityMcp {
            tools: f.clone(),
            now: 0,
            hints: Vec::new(),
        };
        let e = c.call("create_documents", json!({})).await.unwrap_err();
        assert!(matches!(e, ConnectorError::Config(_)));
        assert!(f.calls().is_empty());
    }

    #[tokio::test]
    async fn the_account_comes_from_whoami() {
        let f = server(|_, _| {
            Ok(text_result(
                &json!({"user": {"name": "Ada Lovelace", "email": "ada@example.com"}}),
            ))
        });
        assert_eq!(
            SanityMcp {
                tools: f.clone(),
                now: 0,
                hints: Vec::new(),
            }
            .account()
            .await,
            "Ada Lovelace"
        );
    }

    #[test]
    fn ids_name_one_project() {
        assert_eq!(parse_ref("project:abc123"), Some(Ref::Project("abc123")));
        assert_eq!(
            parse_ref("type:abc123:production:post"),
            Some(Ref::Type {
                project: "abc123",
                dataset: "production",
                ty: "post"
            })
        );
        assert_eq!(
            parse_ref("dataset:production"),
            None,
            "the token path's ids aren't these"
        );
        assert_eq!(parse_ref("dataset:ABC:production"), None);
        assert_eq!(parse_ref("type:abc123:prod/../x:post"), None);
        assert_eq!(
            selection_projects(&source(&[
                "dataset:abc123:production",
                "type:abc123:staging:post"
            ]))
            .unwrap(),
            ["abc123"]
        );
        assert_eq!(
            selection_projects(&source(&[
                "dataset:abc123:production",
                "dataset:other1:production"
            ]))
            .unwrap(),
            ["abc123", "other1"],
            "one source reads several projects"
        );
        assert_eq!(
            selection_projects(&source(&["project:abc123"])).unwrap_err(),
            "Pick a dataset or document types, not a whole project."
        );
        assert_eq!(
            selection_projects(&source(&[])).unwrap_err(),
            "Pick at least one thing to sync."
        );
        assert!(is_sanity_mcp("https://mcp.sanity.io"));
        assert!(is_sanity_mcp("https://mcp.sanity.io/mcp"));
        assert!(!is_sanity_mcp("https://api.sanity.io"));
        assert!(!is_sanity_mcp("https://mcp.sanity.io.evil.example"));
    }

    /// Two projects, each answering for its own datasets. `fail` names a
    /// project the sign-in can't read.
    fn two_projects(fail: Option<&'static str>) -> Arc<FakeMcp> {
        server(move |_, args| {
            let (project, ds) = (
                args["projectId"].as_str().unwrap_or(""),
                args["dataset"].as_str().unwrap_or(""),
            );
            if Some(project) == fail {
                return Err(ConnectorError::Forbidden("Project not found".into()));
            }
            let q = args["query"].as_str().unwrap();
            if q.starts_with("{\"chDoc\"") {
                let id = args["params"]["id"].as_str().unwrap();
                return Ok(text_result(&json!({"chDoc": {
                    "_id": id, "_type": "post", "_updatedAt": "2026-10-02T00:00:00Z",
                    "title": format!("{project} {ds} {id}"),
                    "body": [{"_type": "block", "style": "normal", "children": [{"text": format!("Text of {id} in {project}.")}]}]
                }})));
            }
            let at = if project == "abc123" {
                "2026-10-02T00:00:00Z"
            } else {
                "2026-10-05T00:00:00Z"
            };
            let page = if q.contains("$since") {
                vec![doc("p2", "r2", "2026-10-06T00:00:00Z")]
            } else {
                vec![doc("p1", "r1", at)]
            };
            Ok(text_result(&json!({"chPage": page})))
        })
    }

    fn queried(f: &FakeMcp) -> Vec<(String, String, bool)> {
        f.calls()
            .iter()
            .map(|(_, a)| {
                (
                    a["projectId"].as_str().unwrap_or("").to_string(),
                    a["dataset"].as_str().unwrap_or("").to_string(),
                    a["query"].as_str().unwrap_or("").contains("$since"),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn one_source_lists_every_project_in_its_selection() {
        let f = two_projects(None);
        let c = SanityMcp {
            tools: f.clone(),
            now: 1_000,
            hints: Vec::new(),
        };
        let src = source(&["type:abc123:production:post", "dataset:other1:staging"]);
        let l = c.list(&src).await.unwrap();
        assert!(l.complete && l.partial.is_empty() && l.problem.is_none());
        let ids: Vec<&str> = l.items.iter().map(|i| i.external_id.as_str()).collect();
        assert_eq!(
            ids,
            ["abc123:production:p1", "other1:staging:p1"],
            "the same _id in two projects is two documents"
        );
        assert_eq!(l.items[1].path, "other1 / staging / post");
        assert_eq!(l.items[1].hint["project"], "other1");
        assert_eq!(
            queried(&f),
            [
                ("abc123".into(), "production".into(), false),
                ("other1".into(), "staging".into(), false)
            ]
        );
        let cur: Cursors = serde_json::from_str(&l.cursor).unwrap();
        assert_eq!(cur.sets["abc123:production"].since, "2026-10-02T00:00:00Z");
        assert_eq!(cur.sets["other1:staging"].since, "2026-10-05T00:00:00Z");
        // A document is read from its own project.
        c.fetch(&src, &l.items[1]).await.unwrap();
        let last = f.calls().last().unwrap().1.clone();
        assert_eq!(
            (last["projectId"].as_str(), last["dataset"].as_str()),
            (Some("other1"), Some("staging"))
        );
        // One outside the selection's projects is never read.
        let mut stray = l.items[1].clone();
        stray.hint["project"] = json!("zzz999");
        assert!(c.fetch(&src, &stray).await.unwrap_err().item_gone());
    }

    #[tokio::test]
    async fn each_project_keeps_its_own_cursor() {
        let f = two_projects(None);
        let src0 = source(&["type:abc123:production:post", "dataset:other1:staging"]);
        let mut src = src0.clone();
        let mut sets = std::collections::BTreeMap::new();
        sets.insert(
            "abc123:production".to_string(),
            Cursor {
                since: "2026-10-02T00:00:00Z".into(),
                full_at: 900,
                sel: set_hash(&src0, "abc123", "production"),
            },
        );
        // other1's complete listing is due (six hours old).
        sets.insert(
            "other1:staging".to_string(),
            Cursor {
                since: "2026-10-05T00:00:00Z".into(),
                full_at: 0,
                sel: set_hash(&src0, "other1", "staging"),
            },
        );
        src.cursor = serde_json::to_string(&Cursors { sets }).unwrap();
        let l = SanityMcp {
            tools: f.clone(),
            now: FULL_EVERY_SECS + 10,
            hints: Vec::new(),
        }
        .list(&src)
        .await
        .unwrap();
        assert_eq!(
            queried(&f),
            [
                ("abc123".into(), "production".into(), true),
                ("other1".into(), "staging".into(), false)
            ],
            "one project incremental, the other complete"
        );
        assert!(l.complete);
        assert_eq!(
            l.partial,
            ["abc123:production:"],
            "only the complete listing removes"
        );
        let cur: Cursors = serde_json::from_str(&l.cursor).unwrap();
        assert_eq!(cur.sets["abc123:production"].full_at, 900);
        assert_eq!(cur.sets["abc123:production"].since, "2026-10-06T00:00:00Z");
        assert_eq!(cur.sets["other1:staging"].full_at, FULL_EVERY_SECS + 10);
        // Picking more in one project resets only that project's cursor.
        let mut more = src.clone();
        more.selection.push(Selected {
            id: "type:abc123:production:author".into(),
            title: "author".into(),
            ..Default::default()
        });
        let f2 = two_projects(None);
        SanityMcp {
            tools: f2.clone(),
            now: 1_000,
            hints: Vec::new(),
        }
        .list(&more)
        .await
        .unwrap();
        assert_eq!(
            queried(&f2),
            [
                ("abc123".into(), "production".into(), false),
                ("other1".into(), "staging".into(), true)
            ]
        );
    }

    #[test]
    fn stable_ids_carry_the_project_and_old_ids_move_to_it() {
        let c = SanityMcp {
            tools: two_projects(None),
            now: 0,
            hints: Vec::new(),
        };
        let mut src = source(&["dataset:abc123:production"]);
        assert_eq!(
            stable_id("abc123", "production", "p1"),
            "abc123:production:p1"
        );
        assert_eq!(
            c.current_id(&src, "production:p1").as_deref(),
            Some("abc123:production:p1")
        );
        assert_eq!(c.current_id(&src, "abc123:production:p1"), None);
        // Several projects: the saved project_id says whose old ids are.
        src.selection.push(Selected {
            id: "dataset:other1:production".into(),
            ..Default::default()
        });
        assert_eq!(c.current_id(&src, "production:p1"), None, "no owner known");
        src.options = json!({"project_id": "other1"});
        assert_eq!(
            c.current_id(&src, "production:p1").as_deref(),
            Some("other1:production:p1")
        );
        assert_eq!(
            split_stable("other1:production:a.b-c", None),
            Some(("other1", "production", "a.b-c"))
        );
    }

    async fn run_sync(
        store: &crate::knowledge::sources::store::memory::MemSyncStore,
        c: &SanityMcp,
    ) -> crate::knowledge::sources::sync::Outcome {
        use crate::knowledge::search::testing::WordEmbedder;
        use crate::knowledge::sources::sync::{run, Ctx};
        let now = || 1_800_000_000;
        let ctx = Ctx {
            store,
            connector: c,
            embedder: &WordEmbedder,
            team: "TEAM#t",
            now: &now,
            used: None,
        };
        run(&ctx, "kb1", "s1").await
    }

    fn stored_item(src: &Source, ext: &str, v: &str) -> FileMeta {
        FileMeta {
            id: super::super::item_id(&src.id, ext),
            kb_id: src.kb_id.clone(),
            source_id: src.id.clone(),
            external_id: ext.into(),
            remote_version: v.into(),
            status: FileStatus::Ready,
            ready_version: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_re_sync_moves_old_ids_without_fetching_or_dropping_them() {
        use crate::knowledge::sources::store::memory::MemSyncStore;
        let store = MemSyncStore::default();
        let mut src = source(&["dataset:abc123:production", "dataset:other1:staging"]);
        src.id = "s1".into();
        src.kb_id = "kb1".into();
        src.kind = Some(Kind::Sanity);
        src.options = json!({"project_id": "abc123"});
        // Saved before ids carried the project.
        let old = stored_item(&src, "production:p1", "r1");
        store
            .items
            .lock()
            .unwrap()
            .insert(old.id.clone(), old.clone());
        store
            .sources
            .lock()
            .unwrap()
            .insert("s1".into(), src.clone());
        let f = two_projects(None);
        let c = SanityMcp {
            tools: f.clone(),
            now: 1_000,
            hints: Vec::new(),
        };
        let o = run_sync(&store, &c).await;
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.removed, 0, "the old document isn't dropped");
        assert_eq!(o.fetched, 1, "only the other project's new document");
        let items = store.items.lock().unwrap();
        let moved = &items[&old.id];
        assert_eq!(moved.external_id, "abc123:production:p1");
        assert_eq!(moved.ready_version, 1, "its index stays");
        let mut ids: Vec<&str> = items.values().map(|f| f.external_id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, ["abc123:production:p1", "other1:staging:p1"]);
    }

    #[tokio::test]
    async fn a_project_that_cant_be_read_is_named_and_the_others_sync() {
        use crate::knowledge::sources::store::memory::MemSyncStore;
        let store = MemSyncStore::default();
        let mut src = source(&["dataset:abc123:production", "dataset:other1:staging"]);
        src.id = "s1".into();
        src.kb_id = "kb1".into();
        src.kind = Some(Kind::Sanity);
        // other1 was synced before; its cursor and documents must survive.
        let kept = stored_item(&src, "other1:staging:p9", "r1");
        store
            .items
            .lock()
            .unwrap()
            .insert(kept.id.clone(), kept.clone());
        let mut sets = std::collections::BTreeMap::new();
        let other = Cursor {
            since: "2026-10-05T00:00:00Z".into(),
            full_at: 900,
            sel: set_hash(&src, "other1", "staging"),
        };
        sets.insert("other1:staging".to_string(), other.clone());
        src.cursor = serde_json::to_string(&Cursors { sets }).unwrap();
        store
            .sources
            .lock()
            .unwrap()
            .insert("s1".into(), src.clone());

        let c = SanityMcp {
            tools: two_projects(Some("other1")),
            now: 1_000,
            hints: Vec::new(),
        };
        let l = c.list(&src).await.unwrap();
        assert!(l.complete);
        assert_eq!(l.partial, ["other1:staging:"]);
        let p = l.problem.clone().expect("the failing project is reported");
        assert!(p.to_string().contains("Sanity project other1"), "{p}");
        assert!(p.to_string().contains("The other projects synced."));
        let cur: Cursors = serde_json::from_str(&l.cursor).unwrap();
        assert_eq!(cur.sets["other1:staging"], other, "its cursor is kept");
        assert_eq!(cur.sets["abc123:production"].full_at, 1_000);

        let o = run_sync(&store, &c).await;
        assert_eq!(o.indexed, 1, "abc123 synced");
        assert_eq!(o.removed, 0, "other1's documents stay");
        let items = store.items.lock().unwrap().clone();
        assert!(items.contains_key(&kept.id));
        assert!(items
            .values()
            .any(|f| f.external_id == "abc123:production:p1" && f.ready_version > 0));
        let s = store.sources.lock().unwrap()["s1"].clone();
        assert_eq!(s.status, super::super::Status::Error);
        let e = s.error.unwrap();
        assert!(e.message.contains("other1"), "{}", e.message);
        assert_ne!(e.action, "sign_in", "the sign-in itself still works");

        // When no project can be read, the reason stops the source as before.
        let expired = server(|_, _| Err(ConnectorError::Reconnect("The sign-in expired.".into())));
        let e = SanityMcp {
            tools: expired,
            now: 0,
            hints: Vec::new(),
        }
        .list(&src)
        .await
        .unwrap_err();
        assert!(matches!(e, ConnectorError::Reconnect(_)));
    }
}
