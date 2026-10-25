//! Confluence Cloud: whole spaces, or a page and everything under it.
//!
//! Reached either with an Atlassian sign-in (OAuth, through
//! `api.atlassian.com/ex/confluence/<cloud id>`) or with the email and API
//! token the team already saved on the Integrations page (straight to its
//! `*.atlassian.net` site). Both use the same REST v2 calls.
//!
//! Listing reads every current page of each space the selection touches
//! (250 per call, each with its parent and version number) and works out the
//! picked page trees locally, so a page added under a picked one is synced
//! next time and a deleted, moved or restricted page drops out and is
//! removed. A page's version number is its marker.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::connector::{
    enc, matches, send_json, Connector, ConnectorError, Document, Listed, Listing, Node,
};
use super::{text, Kind, Source};
use crate::live::confluence::storage_to_text;
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

/// Pages listed per space at most (250 per call).
const MAX_CALLS_PER_SPACE: usize = 40;

pub struct Confluence<'a> {
    pub http: &'a dyn Http,
    /// Where `/wiki/api/v2/...` is served: `https://x.atlassian.net` with a
    /// token, `https://api.atlassian.com/ex/confluence/<cloud id>` with a
    /// sign-in.
    pub api_base: String,
    /// Where people open pages: `https://x.atlassian.net`.
    pub site: String,
    /// `Bearer ...` or `Basic ...`.
    pub auth: String,
}

/// A page from a space listing.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub id: String,
    pub title: String,
    pub parent: String,
    pub version: String,
    pub edited: String,
    pub webui: String,
}

fn page_of(v: &Value) -> Option<Page> {
    if v["status"].as_str().is_some_and(|s| s != "current") {
        return None;
    }
    Some(Page {
        id: v["id"].as_str()?.to_string(),
        title: v["title"].as_str().unwrap_or("Untitled").to_string(),
        parent: v["parentId"].as_str().unwrap_or("").to_string(),
        version: v["version"]["number"]
            .as_i64()
            .map(|n| n.to_string())
            .unwrap_or_default(),
        edited: v["version"]["createdAt"].as_str().unwrap_or("").to_string(),
        webui: v["_links"]["webui"].as_str().unwrap_or("").to_string(),
    })
}

/// `space:<id>` or `page:<id>@<space id>` as (kind, id, space id).
pub fn split_id(raw: &str) -> Option<(&str, &str, &str)> {
    let (kind, rest) = raw.split_once(':')?;
    let digits = |s: &str| !s.is_empty() && s.len() <= 24 && s.chars().all(|c| c.is_ascii_digit());
    match kind {
        "space" if digits(rest) => Some(("space", rest, rest)),
        "page" => {
            let (id, space) = rest.split_once('@')?;
            (digits(id) && digits(space)).then_some(("page", id, space))
        }
        _ => None,
    }
}

/// The pages a selection covers within one space's pages: a whole space, or
/// picked pages (with what is under them when asked).
pub fn covered<'p>(
    pages: &'p [Page],
    whole: bool,
    picks: &[(String, bool)],
) -> Vec<(&'p Page, String)> {
    let by_id: HashMap<&str, &Page> = pages.iter().map(|p| (p.id.as_str(), p)).collect();
    let mut kids: HashMap<&str, Vec<&Page>> = HashMap::new();
    for p in pages {
        kids.entry(p.parent.as_str()).or_default().push(p);
    }
    let path_of = |p: &Page| {
        let mut trail = Vec::new();
        let mut cur = by_id.get(p.parent.as_str()).copied();
        let mut guard = 0;
        while let Some(c) = cur {
            trail.push(c.title.clone());
            cur = by_id.get(c.parent.as_str()).copied();
            guard += 1;
            if guard > 30 {
                break;
            }
        }
        trail.reverse();
        trail.join(" / ")
    };
    if whole {
        return pages.iter().map(|p| (p, path_of(p))).collect();
    }
    let mut out = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (root, deep) in picks {
        let Some(r) = by_id.get(root.as_str()) else {
            continue;
        };
        let mut stack = vec![*r];
        while let Some(p) = stack.pop() {
            if !seen.insert(p.id.as_str()) {
                continue;
            }
            out.push((p, path_of(p)));
            if *deep {
                stack.extend(kids.get(p.id.as_str()).into_iter().flatten());
            }
        }
    }
    out
}

impl Confluence<'_> {
    fn get(&self, path_or_url: &str) -> HttpRequest {
        let url = if path_or_url.starts_with("https://") {
            path_or_url.to_string()
        } else {
            format!("{}{path_or_url}", self.api_base)
        };
        let mut r = HttpRequest::get(&url).header("Authorization", &self.auth);
        r.limit = Limit::Mcp;
        r
    }

    /// The next page of a v2 listing, from its `_links.next`.
    fn next(&self, v: &Value) -> Option<String> {
        let n = v["_links"]["next"].as_str()?;
        // `next` is a path on the site (`/wiki/api/v2/...`).
        n.starts_with("/wiki/").then(|| n.to_string())
    }

    async fn spaces(&self, query: &str) -> Result<Vec<Node>, ConnectorError> {
        let mut out = Vec::new();
        let mut path = Some("/wiki/api/v2/spaces?limit=250&status=current".to_string());
        let mut calls = 0;
        while let Some(p) = path.take() {
            calls += 1;
            let v = send_json(self.http, self.get(&p), "Confluence").await?;
            for s in v["results"].as_array().into_iter().flatten() {
                let (Some(id), Some(name)) = (s["id"].as_str(), s["name"].as_str()) else {
                    continue;
                };
                if !matches(query, name) && !matches(query, s["key"].as_str().unwrap_or("")) {
                    continue;
                }
                out.push(Node {
                    id: format!("space:{id}"),
                    title: name.to_string(),
                    kind: "space".into(),
                    has_children: true,
                    selectable: true,
                    url: format!(
                        "{}/wiki{}",
                        self.site,
                        s["_links"]["webui"].as_str().unwrap_or("")
                    ),
                    detail: format!("Space {}", s["key"].as_str().unwrap_or("")),
                    ..Default::default()
                });
            }
            if calls < 8 {
                path = self.next(&v);
            }
        }
        out.sort_by_key(|n| n.title.to_lowercase());
        Ok(out)
    }

    /// Every current page of a space.
    pub async fn space_pages(&self, space: &str) -> Result<(Vec<Page>, bool), ConnectorError> {
        let mut out = Vec::new();
        let mut path = Some(format!(
            "/wiki/api/v2/spaces/{space}/pages?limit=250&status=current"
        ));
        let mut calls = 0;
        while let Some(p) = path.take() {
            calls += 1;
            if calls > MAX_CALLS_PER_SPACE {
                return Ok((out, false));
            }
            let v = send_json(self.http, self.get(&p), "Confluence").await?;
            out.extend(
                v["results"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(page_of),
            );
            path = self.next(&v);
        }
        Ok((out, true))
    }

    async fn page_children(&self, id: &str, space: &str) -> Result<Vec<Node>, ConnectorError> {
        let v = send_json(
            self.http,
            self.get(&format!("/wiki/api/v2/pages/{id}/children?limit=250")),
            "Confluence",
        )
        .await?;
        Ok(v["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(page_of)
            .map(|p| self.page_node(&p, space))
            .collect())
    }

    fn page_node(&self, p: &Page, space: &str) -> Node {
        Node {
            id: format!("page:{}@{space}", p.id),
            title: p.title.clone(),
            kind: "page".into(),
            has_children: true,
            selectable: true,
            url: self.page_url(&p.webui, &p.id),
            updated_at: p.edited.clone(),
            detail: String::new(),
        }
    }

    fn page_url(&self, webui: &str, id: &str) -> String {
        if webui.starts_with('/') {
            format!("{}/wiki{webui}", self.site)
        } else {
            format!("{}/wiki/pages/viewpage.action?pageId={id}", self.site)
        }
    }

    async fn search_pages(&self, query: &str) -> Result<Vec<Node>, ConnectorError> {
        let cql = format!(
            "type = page AND title ~ \"{}\"",
            query.replace('\\', "\\\\").replace('"', "\\\"")
        );
        let v = send_json(
            self.http,
            self.get(&format!(
                "/wiki/rest/api/content/search?limit=25&expand=space&cql={}",
                enc(&cql)
            )),
            "Confluence",
        )
        .await?;
        Ok(v["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| {
                let id = r["id"].as_str()?;
                let space = r["space"]["id"].as_i64().map(|n| n.to_string())?;
                Some(Node {
                    id: format!("page:{id}@{space}"),
                    title: r["title"].as_str().unwrap_or("Untitled").to_string(),
                    kind: "page".into(),
                    has_children: true,
                    selectable: true,
                    url: self.page_url(r["_links"]["webui"].as_str().unwrap_or(""), id),
                    detail: r["space"]["name"].as_str().unwrap_or("").to_string(),
                    ..Default::default()
                })
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl Connector for Confluence<'_> {
    fn kind(&self) -> Kind {
        Kind::Confluence
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        match parent.and_then(split_id) {
            None if query.trim().is_empty() => self.spaces("").await,
            None => {
                // Spaces whose name matches, then pages whose title does.
                let mut out = self.spaces(query).await?;
                out.extend(self.search_pages(query).await?);
                Ok(out)
            }
            Some(("space", space, _)) => {
                let (pages, _) = self.space_pages(space).await?;
                let ids: HashSet<&str> = pages.iter().map(|p| p.id.as_str()).collect();
                // The space's top pages: those whose parent isn't a page of it.
                let mut out: Vec<Node> = pages
                    .iter()
                    .filter(|p| p.parent.is_empty() || !ids.contains(p.parent.as_str()))
                    .filter(|p| matches(query, &p.title))
                    .map(|p| self.page_node(p, space))
                    .collect();
                out.sort_by_key(|n| n.title.to_lowercase());
                Ok(out)
            }
            Some((_, id, space)) => {
                let mut kids = self.page_children(id, space).await?;
                kids.retain(|n| matches(query, &n.title));
                Ok(kids)
            }
        }
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        // Per space: the whole space, or the picked pages in it.
        let mut spaces: Vec<(String, bool, Vec<(String, bool)>)> = Vec::new();
        for s in &source.selection {
            let Some((kind, id, space)) = split_id(&s.id) else {
                return Err(ConnectorError::Config(format!(
                    "\"{}\" isn't a Confluence space or page.",
                    s.title
                )));
            };
            let at = match spaces.iter().position(|(sp, _, _)| sp == space) {
                Some(i) => i,
                None => {
                    spaces.push((space.to_string(), false, Vec::new()));
                    spaces.len() - 1
                }
            };
            if kind == "space" {
                spaces[at].1 = true;
            } else {
                spaces[at].2.push((id.to_string(), s.include_children));
            }
        }
        let mut items: Vec<Listed> = Vec::new();
        let mut complete = true;
        for (space, whole, picks) in &spaces {
            let (pages, all) = match self.space_pages(space).await {
                Ok(p) => p,
                // A space that is gone (or no longer visible) covers nothing.
                Err(ConnectorError::NotFound(_)) | Err(ConnectorError::Forbidden(_)) => {
                    (Vec::new(), true)
                }
                Err(e) => return Err(e),
            };
            complete &= all;
            for (p, path) in covered(&pages, *whole, picks) {
                items.push(Listed {
                    external_id: p.id.clone(),
                    title: p.title.clone(),
                    url: self.page_url(&p.webui, &p.id),
                    updated_at: p.edited.clone(),
                    version: p.version.clone(),
                    path,
                    hint: Value::Null,
                });
            }
        }
        let truncated = items.len() > super::MAX_ITEMS_PER_SOURCE;
        items.truncate(super::MAX_ITEMS_PER_SOURCE);
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: complete && !truncated,
            cursor: String::new(),
            truncated,
        })
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        if !item.external_id.chars().all(|c| c.is_ascii_digit()) {
            return Err(ConnectorError::NotFound("Not a Confluence page id.".into()));
        }
        let v = send_json(
            self.http,
            self.get(&format!(
                "/wiki/api/v2/pages/{}?body-format=storage",
                item.external_id
            )),
            "Confluence",
        )
        .await?;
        let title = v["title"].as_str().unwrap_or(&item.title).to_string();
        let body = storage_to_text(v["body"]["storage"]["value"].as_str().unwrap_or(""));
        Ok(Document {
            url: item.url.clone(),
            updated_at: v["version"]["createdAt"]
                .as_str()
                .unwrap_or(&item.updated_at)
                .to_string(),
            sections: text::markdown(&title, &body),
            title,
        })
    }
}

/// The Confluence sites an Atlassian sign-in can reach: (cloud id, site
/// address, name).
pub async fn accessible_sites(
    http: &dyn Http,
    auth: &str,
) -> Result<Vec<(String, String, String)>, ConnectorError> {
    let r = HttpRequest::get("https://api.atlassian.com/oauth/token/accessible-resources")
        .header("Authorization", auth);
    let v = send_json(http, r, "Atlassian").await?;
    Ok(v.as_array()
        .into_iter()
        .flatten()
        .filter(|s| {
            s["scopes"].as_array().is_none_or(|a| {
                a.iter()
                    .any(|x| x.as_str().is_some_and(|x| x.contains("confluence")))
            })
        })
        .filter_map(|s| {
            Some((
                s["id"].as_str()?.to_string(),
                s["url"].as_str()?.trim_end_matches('/').to_string(),
                s["name"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect())
}

/// Where the v2 API is for a sign-in to `cloud_id`.
pub fn oauth_base(cloud_id: &str) -> String {
    format!("https://api.atlassian.com/ex/confluence/{cloud_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use serde_json::json;

    fn pg(id: &str, title: &str, parent: &str, v: i64) -> Value {
        json!({"id": id, "status": "current", "title": title, "parentId": parent,
               "version": {"number": v, "createdAt": "2026-10-01T00:00:00Z"},
               "_links": {"webui": format!("/spaces/ENG/pages/{id}")}})
    }

    fn rig() -> Fake {
        let http = Fake::default();
        http.with(|r| {
            (r.url.contains("/spaces/77/pages") && !r.url.contains("cursor")).then(|| {
                fjson(200, json!({
                    "results": [pg("1", "Home", "", 3), pg("2", "Runbooks", "1", 1)],
                    "_links": {"next": "/wiki/api/v2/spaces/77/pages?limit=250&status=current&cursor=abc"}
                }))
            })
        });
        http.with(|r| {
            r.url.contains("cursor=abc").then(|| {
                fjson(200, json!({"results": [pg("3", "Deploy", "2", 7), pg("4", "Archive", "1", 2)], "_links": {}}))
            })
        });
        http
    }

    fn conn(http: &Fake) -> Confluence<'_> {
        Confluence {
            http,
            api_base: "https://acme.atlassian.net".into(),
            site: "https://acme.atlassian.net".into(),
            auth: "Basic x".into(),
        }
    }

    fn source(ids: &[(&str, bool)]) -> Source {
        Source {
            selection: ids
                .iter()
                .map(|(id, deep)| Selected {
                    id: id.to_string(),
                    title: id.to_string(),
                    include_children: *deep,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_page_tree_follows_pages_across_listing_pages() {
        let http = rig();
        let c = conn(&http);
        let l = c.list(&source(&[("page:2@77", true)])).await.unwrap();
        let ids: Vec<&str> = l.items.iter().map(|i| i.external_id.as_str()).collect();
        assert_eq!(ids, ["2", "3"]);
        assert!(l.complete);
        assert_eq!(l.items[1].version, "7");
        assert_eq!(l.items[1].path, "Home / Runbooks");
        assert_eq!(
            l.items[1].url,
            "https://acme.atlassian.net/wiki/spaces/ENG/pages/3"
        );
        let whole = c.list(&source(&[("space:77", true)])).await.unwrap();
        assert_eq!(whole.items.len(), 4);
        let one = c.list(&source(&[("page:2@77", false)])).await.unwrap();
        assert_eq!(one.items.len(), 1);
    }

    #[tokio::test]
    async fn a_page_reads_from_storage_format() {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .ends_with("/wiki/api/v2/pages/3?body-format=storage")
                .then(|| {
                    fjson(
                        200,
                        json!({"title": "Deploy", "version": {"createdAt": "2026-10-05T00:00:00Z"},
                    "body": {"storage": {"value": "<h2>Steps</h2><p>Run <code>make</code>.</p>"}}}),
                    )
                })
        });
        let c = conn(&http);
        let d = c
            .fetch(
                &Source::default(),
                &Listed {
                    external_id: "3".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(d.title, "Deploy");
        assert!(d.sections.iter().any(|s| s.text.contains("make")));
        assert_eq!(d.updated_at, "2026-10-05T00:00:00Z");
        assert!(c
            .fetch(
                &Source::default(),
                &Listed {
                    external_id: "../x".into(),
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }

    #[test]
    fn ids_are_checked() {
        assert_eq!(split_id("space:77"), Some(("space", "77", "77")));
        assert_eq!(split_id("page:5@77"), Some(("page", "5", "77")));
        assert_eq!(split_id("page:5"), None);
        assert_eq!(split_id("space:../x"), None);
    }
}
