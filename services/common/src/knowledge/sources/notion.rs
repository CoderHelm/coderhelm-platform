//! Notion: pages and databases, with everything under them.
//!
//! The connection is a Notion OAuth sign-in or the team's saved Notion
//! integration token; either way it sees exactly the pages shared with it.
//!
//! Listing reads Notion's search across everything the connection can see
//! (100 per call, with each page's parent and last edit time) and works out
//! locally which pages sit under the selected ones, so a whole workspace is
//! listed in a handful of calls and a page newly added under a selected one
//! is picked up by the next sync. A page that is deleted, archived or no
//! longer shared drops out of search and is removed here.
//!
//! Fetching reads a page's blocks (nested blocks too, up to a depth) and
//! writes them as Markdown; a database row's properties come first.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use super::connector::{
    matches, send_json, Connector, ConnectorError, Document, Listed, Listing, Node,
};
use super::{text, Kind, Source};
use crate::mcp_remote::http::{Http, HttpRequest, Limit, Method};

pub const API: &str = "https://api.notion.com/v1";
pub const VERSION: &str = "2022-06-28";
/// Search pages read per listing at most (100 each).
const MAX_SEARCH_PAGES: usize = 60;
/// How deep nested blocks are read.
const MAX_BLOCK_DEPTH: usize = 4;
/// Block reads per page at most (100 blocks each).
const MAX_BLOCK_CALLS: usize = 40;

pub struct Notion<'a> {
    pub http: &'a dyn Http,
    /// `Bearer ...`.
    pub auth: String,
}

/// What Notion search says about one page or database.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub id: String,
    /// `page` or `database`.
    pub object: String,
    pub title: String,
    pub url: String,
    pub edited: String,
    /// The parent's id, or empty for the workspace.
    pub parent: String,
    /// Properties as text (database rows).
    pub props: String,
    /// Its icon: an emoji or an image address (empty for none).
    pub icon: String,
}

/// A Notion id without dashes, lowercased, so ids from different calls
/// compare equal.
pub fn norm(id: &str) -> String {
    id.chars()
        .filter(|c| *c != '-')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn plain(rich: &Value) -> String {
    rich.as_array()
        .map(|a| a.iter().filter_map(|t| t["plain_text"].as_str()).collect())
        .unwrap_or_default()
}

/// A page's or database's title.
pub fn title_of(obj: &Value) -> String {
    if obj["object"] == "database" {
        let t = plain(&obj["title"]);
        return if t.is_empty() {
            "Untitled database".into()
        } else {
            t
        };
    }
    if let Some(props) = obj["properties"].as_object() {
        for p in props.values() {
            if p["type"] == "title" {
                let t = plain(&p["title"]);
                if !t.is_empty() {
                    return t;
                }
            }
        }
    }
    "Untitled".into()
}

/// A database row's properties as "Name: value" lines (the title left out).
pub fn props_text(obj: &Value) -> String {
    let Some(props) = obj["properties"].as_object() else {
        return String::new();
    };
    let mut lines: Vec<String> = Vec::new();
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    for k in keys {
        let p = &props[k];
        let v = match p["type"].as_str().unwrap_or("") {
            "title" => continue,
            "rich_text" => plain(&p["rich_text"]),
            "select" | "status" => p[p["type"].as_str().unwrap_or("")]["name"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            "multi_select" => p["multi_select"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default(),
            "number" => p["number"]
                .as_f64()
                .map(|n| n.to_string())
                .unwrap_or_default(),
            "checkbox" => {
                if p["checkbox"].as_bool().unwrap_or(false) {
                    "Yes".into()
                } else {
                    "No".into()
                }
            }
            "date" => p["date"]["start"].as_str().unwrap_or("").to_string(),
            "url" | "email" | "phone_number" => p[p["type"].as_str().unwrap_or("")]
                .as_str()
                .unwrap_or("")
                .to_string(),
            "people" => p["people"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|u| u["name"].as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default(),
            _ => String::new(),
        };
        if !v.trim().is_empty() {
            lines.push(format!("{k}: {v}"));
        }
    }
    crate::truncate_str(&lines.join("\n"), 4_000).to_string()
}

fn parent_of(obj: &Value) -> String {
    let p = &obj["parent"];
    match p["type"].as_str().unwrap_or("") {
        "page_id" => norm(p["page_id"].as_str().unwrap_or("")),
        "database_id" => norm(p["database_id"].as_str().unwrap_or("")),
        "block_id" => norm(p["block_id"].as_str().unwrap_or("")),
        _ => String::new(),
    }
}

/// One search result as an [`Entry`].
pub fn entry(obj: &Value) -> Option<Entry> {
    let object = obj["object"].as_str()?.to_string();
    if obj["archived"] == true || obj["in_trash"] == true {
        return None;
    }
    Some(Entry {
        id: norm(obj["id"].as_str()?),
        title: title_of(obj),
        url: obj["url"].as_str().unwrap_or("").to_string(),
        edited: obj["last_edited_time"].as_str().unwrap_or("").to_string(),
        parent: parent_of(obj),
        props: if object == "page" {
            props_text(obj)
        } else {
            String::new()
        },
        icon: icon_of(&obj["icon"]),
        object,
    })
}

/// A page's or database's icon as an emoji or an `https://` image address
/// (an uploaded file, an external image, a custom emoji). Notion's own
/// named icons carry no address and come back empty.
pub fn icon_of(icon: &Value) -> String {
    let url = |u: &Value| {
        u.as_str()
            .filter(|u| u.starts_with("https://"))
            .unwrap_or("")
            .to_string()
    };
    match icon["type"].as_str().unwrap_or("") {
        "emoji" => icon["emoji"].as_str().unwrap_or("").to_string(),
        "external" => url(&icon["external"]["url"]),
        "file" => url(&icon["file"]["url"]),
        "custom_emoji" => url(&icon["custom_emoji"]["url"]),
        _ => String::new(),
    }
}

/// What the picker can say about each entry from one listing of everything
/// the connection sees: how many entries sit directly under it, how many
/// pages it brings in with everything inside (itself included), and the
/// titles above it.
pub struct Tree<'e> {
    by_id: HashMap<&'e str, &'e Entry>,
    children: HashMap<&'e str, Vec<&'e Entry>>,
}

impl<'e> Tree<'e> {
    pub fn new(entries: &'e [Entry]) -> Self {
        let by_id: HashMap<&str, &Entry> = entries.iter().map(|e| (e.id.as_str(), e)).collect();
        let mut children: HashMap<&str, Vec<&Entry>> = HashMap::new();
        for e in entries {
            if !e.parent.is_empty() {
                children.entry(e.parent.as_str()).or_default().push(e);
            }
        }
        Tree { by_id, children }
    }

    pub fn child_count(&self, id: &str) -> u64 {
        self.children.get(id).map_or(0, |c| c.len() as u64)
    }

    /// Pages under it and itself (a database counts its rows, not itself),
    /// the same pages a pick with everything inside syncs.
    pub fn item_count(&self, id: &str) -> u64 {
        let mut n = 0;
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = vec![id];
        while let Some(cur) = stack.pop() {
            if !seen.insert(cur) || seen.len() > 50_000 {
                continue;
            }
            if self.by_id.get(cur).is_some_and(|e| e.object == "page") {
                n += 1;
            }
            for c in self.children.get(cur).into_iter().flatten() {
                stack.push(c.id.as_str());
            }
        }
        n
    }

    /// Titles above it, top first.
    pub fn path(&self, id: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut cur = self.by_id.get(id).map(|e| e.parent.as_str());
        while let Some(p) = cur.filter(|p| !p.is_empty()) {
            let Some(e) = self.by_id.get(p) else { break };
            out.push(e.title.clone());
            if out.len() > 30 {
                break;
            }
            cur = Some(e.parent.as_str());
        }
        out.reverse();
        out
    }
}

/// The entries under each selected id (and the selected ones themselves),
/// worked out from parent pointers. Pages under a page that isn't shared
/// are reached only from a selected ancestor that is.
pub fn covered<'e>(entries: &'e [Entry], selection: &[(String, bool)]) -> Vec<(&'e Entry, String)> {
    let by_id: HashMap<&str, &Entry> = entries.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut children: HashMap<&str, Vec<&Entry>> = HashMap::new();
    for e in entries {
        children.entry(e.parent.as_str()).or_default().push(e);
    }
    let mut out: Vec<(&Entry, String)> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (root, deep) in selection {
        let Some(r) = by_id.get(root.as_str()) else {
            continue;
        };
        let mut stack: Vec<(&Entry, String, usize)> = vec![(r, String::new(), 0)];
        while let Some((e, path, depth)) = stack.pop() {
            if !seen.insert(e.id.as_str()) {
                continue;
            }
            if e.object == "page" {
                out.push((e, path.clone()));
            }
            if (*deep || e.object == "database" && depth == 0) && depth < 20 {
                let here = if path.is_empty() {
                    e.title.clone()
                } else {
                    format!("{path} / {}", e.title)
                };
                for c in children.get(e.id.as_str()).into_iter().flatten() {
                    stack.push((c, here.clone(), depth + 1));
                }
            }
        }
    }
    out
}

/// One block as Markdown lines (its children indented under it).
fn block_md(b: &Value, depth: usize) -> Option<String> {
    let kind = b["type"].as_str()?;
    let body = &b[kind];
    let rt = plain(&body["rich_text"]);
    let pad = "  ".repeat(depth);
    Some(match kind {
        "paragraph" => format!("{pad}{rt}"),
        "heading_1" => format!("# {rt}"),
        "heading_2" => format!("## {rt}"),
        "heading_3" => format!("### {rt}"),
        "bulleted_list_item" | "toggle" => format!("{pad}- {rt}"),
        "numbered_list_item" => format!("{pad}1. {rt}"),
        "to_do" => format!(
            "{pad}- [{}] {rt}",
            if body["checked"] == true { "x" } else { " " }
        ),
        "quote" => format!("{pad}> {rt}"),
        "callout" => format!("{pad}> {rt}"),
        "code" => format!("```{}\n{rt}\n```", body["language"].as_str().unwrap_or("")),
        "equation" => format!("{pad}{}", body["expression"].as_str().unwrap_or("")),
        "table_row" => {
            let cells: Vec<String> = body["cells"]
                .as_array()
                .map(|c| c.iter().map(plain).collect())
                .unwrap_or_default();
            format!("| {} |", cells.join(" | "))
        }
        "bookmark" | "embed" | "link_preview" => {
            let u = body["url"].as_str().unwrap_or("");
            let cap = plain(&body["caption"]);
            if cap.is_empty() {
                format!("{pad}{u}")
            } else {
                format!("{pad}{cap} ({u})")
            }
        }
        "child_page" => format!("{pad}Page: {}", body["title"].as_str().unwrap_or("")),
        "child_database" => format!("{pad}Database: {}", body["title"].as_str().unwrap_or("")),
        "divider" => "---".into(),
        _ => {
            if rt.is_empty() {
                return None;
            }
            format!("{pad}{rt}")
        }
    })
}

impl Notion<'_> {
    fn req(&self, method: Method, path: &str, body: Option<Value>) -> HttpRequest {
        let url = format!("{API}{path}");
        let r = match (method, body) {
            (Method::Post, Some(b)) => HttpRequest::json(&url, &b, Limit::Mcp),
            _ => {
                let mut g = HttpRequest::get(&url);
                g.limit = Limit::Mcp;
                g
            }
        };
        r.header("Authorization", &self.auth)
            .header("Notion-Version", VERSION)
    }

    /// Search results (pages and databases) the connection can see, newest
    /// edit first, at most `max_pages` calls.
    pub async fn search(
        &self,
        query: &str,
        max_pages: usize,
    ) -> Result<(Vec<Entry>, bool), ConnectorError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..max_pages {
            let mut body = json!({
                "page_size": 100,
                "sort": { "direction": "descending", "timestamp": "last_edited_time" }
            });
            if !query.trim().is_empty() {
                body["query"] = json!(query.trim());
            }
            if let Some(c) = &cursor {
                body["start_cursor"] = json!(c);
            }
            let v = send_json(
                self.http,
                self.req(Method::Post, "/search", Some(body)),
                "Notion",
            )
            .await?;
            out.extend(
                v["results"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(entry),
            );
            match v["next_cursor"].as_str() {
                Some(c) if v["has_more"] == true => cursor = Some(c.to_string()),
                _ => return Ok((out, true)),
            }
        }
        Ok((out, false))
    }

    /// Pages and databases directly under a page (its child page blocks) or
    /// in a database.
    async fn children(&self, id: &str, object: &str) -> Result<Vec<Node>, ConnectorError> {
        if object == "database" {
            let v = send_json(
                self.http,
                self.req(
                    Method::Post,
                    &format!("/databases/{id}/query"),
                    Some(json!({ "page_size": 100 })),
                ),
                "Notion",
            )
            .await?;
            return Ok(v["results"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(entry)
                .map(|e| node(&e, true))
                .collect());
        }
        let v = send_json(
            self.http,
            self.req(
                Method::Get,
                &format!("/blocks/{id}/children?page_size=100"),
                None,
            ),
            "Notion",
        )
        .await?;
        Ok(v["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| {
                let (object, title) = match b["type"].as_str()? {
                    "child_page" => ("page", b["child_page"]["title"].as_str()?),
                    "child_database" => ("database", b["child_database"]["title"].as_str()?),
                    _ => return None,
                };
                Some(Node {
                    id: format!("{object}:{}", norm(b["id"].as_str()?)),
                    title: if title.is_empty() {
                        "Untitled".into()
                    } else {
                        title.to_string()
                    },
                    kind: object.into(),
                    has_children: true,
                    selectable: true,
                    updated_at: b["last_edited_time"].as_str().unwrap_or("").to_string(),
                    detail: if object == "database" {
                        "Database".into()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                })
            })
            .collect())
    }

    /// A page's blocks as Markdown.
    async fn page_markdown(&self, id: &str) -> Result<String, ConnectorError> {
        let mut calls = 0usize;
        let lines = self.blocks(id, 0, &mut calls).await?;
        Ok(lines.join("\n"))
    }

    /// The Markdown lines of a block's children, each nested block's lines
    /// right under it.
    fn blocks<'b>(
        &'b self,
        id: &'b str,
        depth: usize,
        calls: &'b mut usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, ConnectorError>> + Send + 'b>,
    > {
        Box::pin(async move {
            let mut out: Vec<String> = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                *calls += 1;
                if *calls > MAX_BLOCK_CALLS {
                    break;
                }
                let mut path = format!("/blocks/{id}/children?page_size=100");
                if let Some(c) = &cursor {
                    path.push_str(&format!("&start_cursor={}", super::connector::enc(c)));
                }
                let v = send_json(self.http, self.req(Method::Get, &path, None), "Notion").await?;
                for b in v["results"].as_array().into_iter().flatten() {
                    if let Some(l) = block_md(b, depth) {
                        out.push(l);
                    }
                    let kind = b["type"].as_str().unwrap_or("");
                    if b["has_children"] == true
                        && depth < MAX_BLOCK_DEPTH
                        && !matches!(kind, "child_page" | "child_database")
                    {
                        if let Some(cid) = b["id"].as_str() {
                            let cid = norm(cid);
                            out.extend(self.blocks(&cid, depth + 1, calls).await?);
                        }
                    }
                }
                match v["next_cursor"].as_str() {
                    Some(c) if v["has_more"] == true => cursor = Some(c.to_string()),
                    _ => break,
                }
            }
            Ok(out)
        })
    }
}

fn node(e: &Entry, has_children: bool) -> Node {
    Node {
        id: format!("{}:{}", e.object, e.id),
        title: e.title.clone(),
        kind: e.object.clone(),
        has_children,
        selectable: true,
        url: e.url.clone(),
        updated_at: e.edited.clone(),
        detail: if e.object == "database" {
            "Database".into()
        } else {
            String::new()
        },
        icon: e.icon.clone(),
        ..Default::default()
    }
}

/// A node with what a whole listing knows about it: its counts (and no
/// chevron when nothing is under it) and, for a search result, its path.
fn counted(e: &Entry, tree: &Tree, with_path: bool) -> Node {
    let kids = tree.child_count(&e.id);
    Node {
        child_count: Some(kids),
        item_count: Some(tree.item_count(&e.id)),
        path: if with_path {
            tree.path(&e.id)
        } else {
            Vec::new()
        },
        ..node(e, kids > 0)
    }
}

/// `page:<id>` / `database:<id>` as (object, id).
pub fn split_id(raw: &str) -> (&str, String) {
    match raw.split_once(':') {
        Some((o, id)) if o == "page" || o == "database" => (o, norm(id)),
        _ => ("page", norm(raw)),
    }
}

#[async_trait::async_trait]
impl Connector for Notion<'_> {
    fn kind(&self) -> Kind {
        Kind::Notion
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        if let Some(p) = parent {
            let (object, id) = split_id(p);
            let mut kids = self.children(&id, object).await?;
            kids.retain(|n| matches(query, &n.title));
            // A page's child blocks keep the page's own order but carry no
            // icon and no counts; one listing of what the connection sees
            // fills them in (a failed or partial listing leaves them as is).
            if let Ok((entries, true)) = self.search("", 5).await {
                let tree = Tree::new(&entries);
                let by_id: HashMap<&str, &Entry> =
                    entries.iter().map(|e| (e.id.as_str(), e)).collect();
                for k in &mut kids {
                    let (_, kid) = split_id(&k.id);
                    if let Some(e) = by_id.get(kid.as_str()) {
                        let c = counted(e, &tree, false);
                        k.icon = c.icon;
                        k.child_count = c.child_count;
                        k.item_count = c.item_count;
                        k.has_children = c.has_children;
                        if k.updated_at.is_empty() {
                            k.updated_at = c.updated_at;
                        }
                    }
                }
            }
            return Ok(kids);
        }
        // One listing of everything the connection sees (newest edit
        // first) gives each entry's counts and path. Search words narrow it
        // here, on titles, the way Notion's own search matches them.
        let (entries, complete) = self.search("", 5).await?;
        let tree = Tree::new(&entries);
        let ids: HashSet<&str> = entries.iter().map(|e| e.id.as_str()).collect();
        let words = !query.trim().is_empty();
        // With no search words: the top-most pages the connection can see.
        // With words: every match, wherever it is, with the titles above it.
        let mut out: Vec<Node> = entries
            .iter()
            .filter(|e| {
                if words {
                    matches(query, &e.title)
                } else {
                    e.parent.is_empty() || !ids.contains(e.parent.as_str())
                }
            })
            .map(|e| {
                if complete {
                    counted(e, &tree, words)
                } else {
                    // Part of a big workspace: counts would be short, so
                    // none are given; paths are still right as far as known.
                    Node {
                        path: if words { tree.path(&e.id) } else { Vec::new() },
                        ..node(e, true)
                    }
                }
            })
            .collect();
        if words && !complete {
            // More than one listing holds: Notion's search finds the rest.
            let (found, _) = self.search(query, 2).await?;
            for e in &found {
                if !ids.contains(e.id.as_str()) {
                    out.push(node(e, true));
                }
            }
        }
        out.sort_by_key(|n| n.title.to_lowercase());
        Ok(out)
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let (entries, complete) = self.search("", MAX_SEARCH_PAGES).await?;
        let selection: Vec<(String, bool)> = source
            .selection
            .iter()
            .map(|s| (split_id(&s.id).1, s.include_children))
            .collect();
        let mut items: Vec<Listed> = covered(&entries, &selection)
            .into_iter()
            .map(|(e, path)| Listed {
                external_id: e.id.clone(),
                title: e.title.clone(),
                url: e.url.clone(),
                updated_at: e.edited.clone(),
                version: e.edited.clone(),
                path,
                hint: json!({ "props": e.props }),
            })
            .collect();
        let truncated = items.len() > super::MAX_ITEMS_PER_SOURCE;
        items.truncate(super::MAX_ITEMS_PER_SOURCE);
        Ok(Listing {
            items,
            deleted: Vec::new(),
            // A search cut short can't prove anything is gone.
            complete: complete && !truncated,
            cursor: String::new(),
            truncated,
        })
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let md = self.page_markdown(&item.external_id).await?;
        let props = item.hint["props"].as_str().unwrap_or("");
        let body = if props.is_empty() {
            md
        } else {
            format!("{props}\n\n{md}")
        };
        Ok(Document {
            title: item.title.clone(),
            url: item.url.clone(),
            updated_at: item.updated_at.clone(),
            sections: text::markdown(&item.title, &body),
        })
    }
}

/// The workspace a token belongs to (for routing Notion's webhooks), when
/// Notion says.
pub async fn workspace(http: &dyn Http, auth: &str) -> Option<String> {
    let n = Notion {
        http,
        auth: auth.to_string(),
    };
    let v = send_json(http, n.req(Method::Get, "/users/me", None), "Notion")
        .await
        .ok()?;
    v["bot"]["workspace_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(norm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};

    fn page(id: &str, title: &str, parent: Value, edited: &str) -> Value {
        json!({
            "object": "page", "id": id, "url": format!("https://www.notion.so/{id}"),
            "last_edited_time": edited, "parent": parent,
            "properties": { "Name": { "type": "title", "title": [{ "plain_text": title }] } }
        })
    }

    fn rig() -> Fake {
        let http = Fake::default();
        let mut eng = page(
            "aaaa-1",
            "Engineering",
            json!({"type":"workspace","workspace":true}),
            "2026-10-01T00:00:00.000Z",
        );
        eng["icon"] = json!({"type": "emoji", "emoji": "🛠️"});
        let results = json!({
            "results": [
                eng,
                page("bbbb-2", "Runbooks", json!({"type":"page_id","page_id":"aaaa1"}), "2026-10-02T00:00:00.000Z"),
                page("cccc-3", "Deploy", json!({"type":"page_id","page_id":"bbbb-2"}), "2026-10-03T00:00:00.000Z"),
                page("dddd-4", "Marketing", json!({"type":"workspace","workspace":true}), "2026-10-04T00:00:00.000Z"),
                {"object":"page","id":"eeee5","archived":true,"parent":{"type":"page_id","page_id":"bbbb2"}}
            ],
            "has_more": false, "next_cursor": null
        });
        http.on(Method::Post, &format!("{API}/search"), fjson(200, results));
        http.with(|r| {
            r.url.contains("/blocks/cccc3/children").then(|| {
                fjson(200, json!({
                    "results": [
                        {"id":"b1","type":"heading_2","has_children":false,"heading_2":{"rich_text":[{"plain_text":"Steps"}]}},
                        {"id":"b2","type":"bulleted_list_item","has_children":true,"bulleted_list_item":{"rich_text":[{"plain_text":"Build"}]}},
                        {"id":"b3","type":"code","has_children":false,"code":{"language":"bash","rich_text":[{"plain_text":"make deploy"}]}}
                    ],
                    "has_more": false
                }))
            })
        });
        http.with(|r| {
            r.url.contains("/blocks/aaaa1/children").then(|| {
                fjson(200, json!({
                    "results": [
                        {"id":"bbbb-2","type":"child_page","has_children":true,"child_page":{"title":"Runbooks"},"last_edited_time":"2026-10-02T00:00:00.000Z"},
                        {"id":"x1","type":"paragraph","has_children":false,"paragraph":{"rich_text":[{"plain_text":"Intro"}]}}
                    ],
                    "has_more": false
                }))
            })
        });
        http.with(|r| {
            r.url.contains("/blocks/b2/children").then(|| {
                fjson(200, json!({
                    "results": [{"id":"b4","type":"paragraph","has_children":false,"paragraph":{"rich_text":[{"plain_text":"cargo build"}]}}],
                    "has_more": false
                }))
            })
        });
        http
    }

    fn source(sel: &[(&str, bool)]) -> Source {
        Source {
            id: "s".into(),
            selection: sel
                .iter()
                .map(|(id, deep)| Selected {
                    id: format!("page:{id}"),
                    include_children: *deep,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_selected_page_covers_everything_under_it() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let l = n.list(&source(&[("aaaa1", true)])).await.unwrap();
        let ids: Vec<&str> = l.items.iter().map(|i| i.external_id.as_str()).collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"aaaa1") && ids.contains(&"bbbb2") && ids.contains(&"cccc3"));
        assert!(!ids.contains(&"dddd4"), "a page outside the selection");
        assert!(!ids.contains(&"eeee5"), "an archived page");
        assert!(l.complete);
        let deploy = l.items.iter().find(|i| i.external_id == "cccc3").unwrap();
        assert_eq!(deploy.path, "Engineering / Runbooks");
        assert_eq!(deploy.version, "2026-10-03T00:00:00.000Z");
        // Without children: just the page.
        let only = n.list(&source(&[("aaaa1", false)])).await.unwrap();
        assert_eq!(only.items.len(), 1);
        // Every request carries the version and the token.
        let sent = http.sent();
        assert!(sent
            .iter()
            .all(|r| r.header_value("Notion-Version") == Some(VERSION)));
    }

    #[tokio::test]
    async fn the_picker_shows_top_pages_then_children() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let top = n.browse(None, "").await.unwrap();
        let titles: Vec<&str> = top.iter().map(|n| n.title.as_str()).collect();
        assert_eq!(titles, ["Engineering", "Marketing"]);
        assert_eq!(top[0].id, "page:aaaa1");
    }

    #[tokio::test]
    async fn a_page_reads_as_markdown_with_nested_blocks() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let d = n
            .fetch(
                &source(&[]),
                &Listed {
                    external_id: "cccc3".into(),
                    title: "Deploy".into(),
                    url: "https://www.notion.so/cccc3".into(),
                    hint: json!({"props": "Status: Done"}),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let all: String = d.sections.iter().map(|s| s.text.clone()).collect();
        assert!(all.contains("Status: Done"));
        let build = all.find("- Build").unwrap();
        let cargo = all.find("cargo build").unwrap();
        let code = all.find("make deploy").unwrap();
        assert!(
            build < cargo && cargo < code,
            "children sit under their parent: {all}"
        );
        assert!(d.sections.iter().any(|s| s.loc == "Deploy > Steps"));
    }

    #[tokio::test]
    async fn top_pages_carry_their_icon_and_what_is_inside() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let top = n.browse(None, "").await.unwrap();
        let eng = &top[0];
        assert_eq!(eng.icon, "🛠️");
        assert_eq!(eng.child_count, Some(1), "Runbooks sits under it");
        assert_eq!(eng.item_count, Some(3), "itself, Runbooks and Deploy");
        assert!(eng.has_children);
        assert!(eng.path.is_empty());
        let marketing = &top[1];
        assert_eq!(marketing.child_count, Some(0));
        assert!(!marketing.has_children, "nothing to open: no chevron");
        // The new fields stay out of the answer when unknown, so an older
        // dashboard sees exactly the shape it knew.
        let v = serde_json::to_value(Node::default()).unwrap();
        for k in ["icon", "child_count", "item_count", "bytes", "path"] {
            assert!(v.get(k).is_none(), "{k} left out when unknown");
        }
        let old: Node = serde_json::from_value(json!({
            "id": "page:a", "title": "A", "kind": "page", "has_children": false, "selectable": true
        }))
        .unwrap();
        assert_eq!(old.child_count, None);
    }

    #[tokio::test]
    async fn search_results_say_where_they_are() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let found = n.browse(None, "dep").await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].title, "Deploy");
        assert_eq!(found[0].path, ["Engineering", "Runbooks"]);
        assert_eq!(found[0].item_count, Some(1));
        let top = n.browse(None, "eng").await.unwrap();
        assert_eq!(top[0].title, "Engineering");
        assert!(top[0].path.is_empty(), "a top page has nothing above it");
    }

    #[tokio::test]
    async fn opening_a_page_keeps_its_order_and_fills_in_icons_and_counts() {
        let http = rig();
        let n = Notion {
            http: &http,
            auth: "Bearer t".into(),
        };
        let kids = n.browse(Some("page:aaaa1"), "").await.unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].id, "page:bbbb2");
        assert_eq!(kids[0].child_count, Some(1));
        assert_eq!(kids[0].item_count, Some(2));
        assert_eq!(kids[0].updated_at, "2026-10-02T00:00:00.000Z");
    }

    #[test]
    fn icons_are_an_emoji_or_an_https_image() {
        assert_eq!(icon_of(&json!({"type":"emoji","emoji":"📘"})), "📘");
        assert_eq!(
            icon_of(&json!({"type":"external","external":{"url":"https://x.test/i.png"}})),
            "https://x.test/i.png"
        );
        assert_eq!(
            icon_of(
                &json!({"type":"file","file":{"url":"https://s3.test/i.png","expiry_time":"t"}})
            ),
            "https://s3.test/i.png"
        );
        assert_eq!(
            icon_of(
                &json!({"type":"custom_emoji","custom_emoji":{"id":"1","name":"a","url":"https://x.test/e.png"}})
            ),
            "https://x.test/e.png"
        );
        assert_eq!(
            icon_of(&json!({"type":"external","external":{"url":"http://x.test/i.png"}})),
            ""
        );
        assert_eq!(
            icon_of(&json!({"type":"icon","icon":{"name":"book","color":"gray"}})),
            ""
        );
        assert_eq!(icon_of(&Value::Null), "");
    }

    #[test]
    fn ids_compare_without_dashes() {
        assert_eq!(norm("A1B2-c3"), "a1b2c3");
        assert_eq!(split_id("database:ab-cd"), ("database", "abcd".into()));
        assert_eq!(split_id("abcd"), ("page", "abcd".into()));
    }

    #[test]
    fn row_properties_become_lines() {
        let row = json!({"properties":{
            "Name":{"type":"title","title":[{"plain_text":"T"}]},
            "Status":{"type":"status","status":{"name":"Done"}},
            "Tags":{"type":"multi_select","multi_select":[{"name":"a"},{"name":"b"}]},
            "Done":{"type":"checkbox","checkbox":true}
        }});
        assert_eq!(props_text(&row), "Done: Yes\nStatus: Done\nTags: a, b");
        assert_eq!(title_of(&row), "T");
    }
}
