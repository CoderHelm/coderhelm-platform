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
//! so a source knows which project it reads without a separate setting.
//!
//! Changes: a document's `_rev` is its marker, as with the token. There is
//! no webhook through a sign-in, so syncs run on a schedule and each one
//! asks only for documents whose `_updatedAt` is after the newest one seen
//! (an incremental listing). Every [`FULL_EVERY_SECS`], and whenever the
//! selection changes, a complete listing finds deleted documents too.
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

/// The one project a selection reads (every entry must name the same one).
pub fn selection_project(source: &Source) -> Result<String, String> {
    let mut project: Option<&str> = None;
    for s in &source.selection {
        let r = parse_ref(&s.id)
            .ok_or_else(|| format!("\"{}\" isn't a Sanity dataset or type.", s.title))?;
        if matches!(r, Ref::Project(_)) {
            return Err("Pick a dataset or document types, not a whole project.".into());
        }
        match project {
            None => project = Some(r.project()),
            Some(p) if p != r.project() => {
                return Err("Pick from one Sanity project per source.".into())
            }
            _ => {}
        }
    }
    project
        .map(str::to_string)
        .ok_or_else(|| "Pick at least one thing to sync.".into())
}

/// Where the next listing starts.
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

fn selection_hash(source: &Source) -> String {
    let mut ids: Vec<&str> = source.selection.iter().map(|s| s.id.as_str()).collect();
    ids.sort();
    super::hex(&Sha256::digest(ids.join("\n").as_bytes()))[..16].to_string()
}

/// A dataset and the types picked in it (`None`: every content type).
type DatasetScope = (String, Option<Vec<String>>);

pub struct SanityMcp {
    pub tools: mcp::Tools,
    /// Unix seconds now.
    pub now: i64,
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
        let v = self.call("list_projects", json!({})).await?;
        let mut out: Vec<(String, String)> = Vec::new();
        for m in objects(&v, &|m| {
            m.get("id")
                .and_then(Value::as_str)
                .is_some_and(valid_project)
        }) {
            let id = m["id"].as_str().unwrap_or("").to_string();
            let name = m
                .get("displayName")
                .or(m.get("name"))
                .and_then(Value::as_str)
                .unwrap_or(&id)
                .to_string();
            if !out.iter().any(|(i, _)| *i == id) {
                out.push((id, name));
            }
        }
        out.sort_by_key(|(_, name)| name.to_lowercase());
        Ok(out)
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
        let v = result_json(PROVIDER, &r)?;
        let mut out: Vec<String> = Vec::new();
        let mut push = |n: &str| {
            if valid_name(n) && !out.iter().any(|x| x == n) {
                out.push(n.to_string());
            }
        };
        for m in objects(&v, &|m| {
            m.get("name").and_then(Value::as_str).is_some()
                && m.get("datasetProfile").and_then(Value::as_str) != Some("comments")
        }) {
            push(m["name"].as_str().unwrap_or(""));
        }
        if let Some(a) = find_key(&v, "datasets").and_then(Value::as_array) {
            for n in a.iter().filter_map(Value::as_str) {
                push(n);
            }
        }
        out.sort();
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

    /// Per dataset: the picked types, or `None` for every content type.
    fn scope(source: &Source) -> Result<(String, Vec<DatasetScope>), ConnectorError> {
        let project = selection_project(source).map_err(ConnectorError::Config)?;
        let mut sets: Vec<DatasetScope> = Vec::new();
        for s in &source.selection {
            let (ds, ty) = match parse_ref(&s.id) {
                Some(Ref::Dataset { dataset, .. }) => (dataset, None),
                Some(Ref::Type { dataset, ty, .. }) => (dataset, Some(ty)),
                _ => continue,
            };
            let at = match sets.iter().position(|(d, _)| d == ds) {
                Some(i) => i,
                None => {
                    sets.push((ds.to_string(), Some(Vec::new())));
                    sets.len() - 1
                }
            };
            match (ty, &mut sets[at].1) {
                (None, slot) => *slot = None,
                (Some(t), Some(list)) => list.push(t.to_string()),
                (Some(_), None) => {}
            }
        }
        Ok((project, sets))
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

    fn listed(ds: &str, d: &Value) -> Option<Listed> {
        let (id, ty) = (d["_id"].as_str()?, d["_type"].as_str()?);
        Some(Listed {
            external_id: format!("{ds}:{id}"),
            title: d["title"].as_str().unwrap_or(id).to_string(),
            url: String::new(),
            updated_at: d["_updatedAt"].as_str().unwrap_or("").to_string(),
            version: d["_rev"].as_str().unwrap_or("").to_string(),
            path: format!("{ds} / {ty}"),
            hint: json!({ "dataset": ds, "id": id, "type": ty }),
        })
    }

    /// Every document the selection covers, a page at a time by `_id`.
    async fn list_all(
        &self,
        project: &str,
        sets: &[DatasetScope],
    ) -> Result<(Vec<Listed>, bool), ConnectorError> {
        let projection = format!("{{_id, _type, _rev, _updatedAt, \"title\": {TITLE_FIELDS}}}");
        let mut items = Vec::new();
        for (ds, types) in sets {
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
                        &format!("*[{filter} && _id > $after] | order(_id asc) [0...{PAGE}] {projection}"),
                        params,
                    )
                    .await?;
                let page = v.as_array().cloned().unwrap_or_default();
                for d in &page {
                    if let Some(l) = Self::listed(ds, d) {
                        items.push(l);
                    }
                }
                if items.len() > MAX_ITEMS_PER_SOURCE {
                    items.truncate(MAX_ITEMS_PER_SOURCE);
                    return Ok((items, true));
                }
                match page.last().and_then(|d| d["_id"].as_str()) {
                    Some(last) if page.len() >= PAGE && last > after.as_str() => {
                        after = last.to_string()
                    }
                    _ => break,
                }
            }
        }
        Ok((items, false))
    }

    /// Documents changed after `since`, oldest first. `None` when more
    /// changed than a source keeps (a complete listing is due then).
    async fn list_changed(
        &self,
        project: &str,
        sets: &[DatasetScope],
        since: &str,
    ) -> Result<Option<Vec<Listed>>, ConnectorError> {
        let projection = format!("{{_id, _type, _rev, _updatedAt, \"title\": {TITLE_FIELDS}}}");
        let mut items = Vec::new();
        for (ds, types) in sets {
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
                    if let Some(l) = Self::listed(ds, d) {
                        items.push(l);
                    }
                }
                if items.len() > MAX_ITEMS_PER_SOURCE {
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
        }
        Ok(Some(items))
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

#[async_trait::async_trait]
impl Connector for SanityMcp {
    fn kind(&self) -> Kind {
        Kind::Sanity
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        match parent.and_then(parse_ref) {
            None => Ok(self
                .projects()
                .await?
                .into_iter()
                .filter(|(id, name)| matches(query, name) || matches(query, id))
                .map(|(id, name)| Node {
                    id: format!("project:{id}"),
                    title: name,
                    kind: "project".into(),
                    has_children: true,
                    selectable: false,
                    detail: format!("Project {id}"),
                    ..Default::default()
                })
                .collect()),
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

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let (project, sets) = Self::scope(source)?;
        let sel = selection_hash(source);
        let prev: Cursor = serde_json::from_str(&source.cursor).unwrap_or_default();
        let incremental =
            !prev.since.is_empty() && prev.sel == sel && self.now - prev.full_at < FULL_EVERY_SECS;
        if incremental {
            if let Some(items) = self.list_changed(&project, &sets, &prev.since).await? {
                let cursor = Cursor {
                    since: newest(&items, &prev.since),
                    ..prev.clone()
                };
                return Ok(Listing {
                    items,
                    deleted: Vec::new(),
                    complete: false,
                    cursor: serde_json::to_string(&cursor).unwrap_or_default(),
                    truncated: source.truncated,
                });
            }
        }
        let (items, truncated) = self.list_all(&project, &sets).await?;
        let cursor = Cursor {
            since: newest(&items, ""),
            full_at: self.now,
            sel,
        };
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: !truncated,
            cursor: serde_json::to_string(&cursor).unwrap_or_default(),
            truncated,
        })
    }

    async fn fetch(&self, source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let project = selection_project(source).map_err(ConnectorError::Config)?;
        let ds = item.hint["dataset"].as_str().unwrap_or("");
        let id = item.hint["id"].as_str().unwrap_or("");
        let v = self
            .query(
                &project,
                ds,
                "chDoc",
                "*[_id == $id][0]",
                json!({ "id": id }),
            )
            .await?;
        if v.is_null() {
            return Err(ConnectorError::NotFound("The document was deleted.".into()));
        }
        let ty = v["_type"].as_str().unwrap_or("");
        let map = type_map(&source.options, ty);
        let title = if map.title.is_empty() {
            item.title.clone()
        } else {
            super::text::at_path(&v, &map.title)
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| item.title.clone())
        };
        Ok(Document {
            url: doc_url(&v, &map, &source.options),
            updated_at: v["_updatedAt"]
                .as_str()
                .unwrap_or(&item.updated_at)
                .to_string(),
            sections: doc_sections(&v, &title, &map),
            title,
        })
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

    fn doc(id: &str, rev: &str, at: &str) -> Value {
        json!({"_id": id, "_type": "post", "_rev": rev, "_updatedAt": at, "title": format!("Post {id}")})
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
        };
        let l = c
            .list(&source(&["type:abc123:production:post"]))
            .await
            .unwrap();
        assert!(l.complete);
        assert_eq!(l.items.len(), PAGE + 1);
        assert_eq!(l.items[0].external_id, "production:d000");
        assert_eq!(l.items[0].version, "r1");
        let cur: Cursor = serde_json::from_str(&l.cursor).unwrap();
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
        };
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
        let cur: Cursor = serde_json::from_str(&l.cursor).unwrap();
        assert_eq!(cur.since, "2026-10-04T00:00:00Z");
        assert_eq!(cur.full_at, 1_000);
        // The engine fetches only the changed document and removes nothing.
        let stored = |ext: &str, v: &str| FileMeta {
            external_id: ext.into(),
            remote_version: v.into(),
            status: FileStatus::Ready,
            ready_version: 1,
            ..Default::default()
        };
        let plan = plan_sync(
            &[stored("production:p1", "r1"), stored("production:p2", "r1")],
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
        }
        .fetch(&src, &item)
        .await
        .unwrap_err();
        assert!(e.item_gone());
        let expired = server(|_, _| Err(ConnectorError::Reconnect("The sign-in expired.".into())));
        let e = SanityMcp {
            tools: expired.clone(),
            now: 0,
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
                now: 0
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
        assert!(selection_project(&source(&[
            "dataset:abc123:production",
            "type:abc123:staging:post"
        ]))
        .is_ok());
        assert!(selection_project(&source(&[
            "dataset:abc123:production",
            "dataset:other1:production"
        ]))
        .is_err());
        assert!(selection_project(&source(&["project:abc123"])).is_err());
        assert!(is_sanity_mcp("https://mcp.sanity.io"));
        assert!(is_sanity_mcp("https://mcp.sanity.io/mcp"));
        assert!(!is_sanity_mcp("https://api.sanity.io"));
        assert!(!is_sanity_mcp("https://mcp.sanity.io.evil.example"));
    }
}
