//! Sanity: the published documents of the types you pick, mapped to text.
//!
//! The connection is the Sanity API token and project id the team saved on
//! the Integrations page (a Viewer token is enough to read). The picker shows
//! the project's datasets, then each dataset's document types; picking a
//! dataset takes every content type in it.
//!
//! How a type becomes text is set per type in the source's options
//! (`options.types.<type>`): which field is the title, which fields are the
//! body (Portable Text reads as paragraphs and headings), and a URL template
//! like `https://example.com/blog/{slug.current}` for citations. Without a
//! mapping, the title is the first of `title`, `name`, `heading`, `label`,
//! the body is every text field, and the link opens the document in the
//! Studio when `options.studio_url` is set.
//!
//! A document's `_rev` is its marker, so an unchanged document is never read
//! twice. Drafts are never synced. Sanity sends a webhook on every change
//! when one is registered (Coderhelm registers it when the token may; see
//! [`register_webhook`]).

use serde_json::{json, Value};

use super::connector::{
    enc, matches, send_json, Connector, ConnectorError, Document, Listed, Listing, Node,
};
use super::{text, Kind, Source};
use crate::knowledge::chunk::Section;
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

pub const API_VERSION: &str = "v2023-08-01";

pub struct Sanity<'a> {
    pub http: &'a dyn Http,
    pub project_id: String,
    /// `Bearer ...`.
    pub auth: String,
}

/// A project id: lowercase letters and digits only.
pub fn valid_project(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 40
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// A dataset or type name: letters, digits, `_`, `-`, `.`.
pub fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 100
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "_-.".contains(c))
}

/// `dataset:<name>` or `type:<dataset>:<type>` as (dataset, type).
pub fn split_id(raw: &str) -> Option<(&str, Option<&str>)> {
    if let Some(ds) = raw.strip_prefix("dataset:") {
        return valid_name(ds).then_some((ds, None));
    }
    let rest = raw.strip_prefix("type:")?;
    let (ds, ty) = rest.split_once(':')?;
    (valid_name(ds) && valid_name(ty)).then_some((ds, Some(ty)))
}

/// Types that are Sanity's own, never content.
fn system_type(t: &str) -> bool {
    t.starts_with("sanity.") || t.starts_with("system.") || t.starts_with("assist.")
}

const TITLE_FIELDS: &str = "coalesce(title, name, heading, label, _id)";

impl Sanity<'_> {
    fn query_url(&self, dataset: &str, groq: &str, params: &[(&str, Value)]) -> String {
        let mut u = format!(
            "https://{}.api.sanity.io/{API_VERSION}/data/query/{dataset}?perspective=published&query={}",
            self.project_id,
            enc(groq)
        );
        for (k, v) in params {
            u.push_str(&format!("&%24{k}={}", enc(&v.to_string())));
        }
        u
    }

    async fn query(
        &self,
        dataset: &str,
        groq: &str,
        params: &[(&str, Value)],
    ) -> Result<Value, ConnectorError> {
        if !valid_project(&self.project_id) {
            return Err(ConnectorError::Config(
                "The saved Sanity project id isn't valid.".into(),
            ));
        }
        if !valid_name(dataset) {
            return Err(ConnectorError::Config(format!(
                "\"{dataset}\" isn't a dataset name."
            )));
        }
        let mut r = HttpRequest::get(&self.query_url(dataset, groq, params))
            .header("Authorization", &self.auth);
        r.limit = Limit::Mcp;
        let v = send_json(self.http, r, "Sanity").await?;
        Ok(v["result"].clone())
    }

    async fn datasets(&self) -> Result<Vec<String>, ConnectorError> {
        if !valid_project(&self.project_id) {
            return Err(ConnectorError::Config(
                "The saved Sanity project id isn't valid.".into(),
            ));
        }
        let r = HttpRequest::get(&format!(
            "https://api.sanity.io/v2021-06-07/projects/{}/datasets",
            self.project_id
        ))
        .header("Authorization", &self.auth);
        let v = send_json(self.http, r, "Sanity").await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect())
    }

    async fn types(&self, dataset: &str) -> Result<Vec<(String, u64)>, ConnectorError> {
        let v = self
            .query(
                dataset,
                "array::unique(*[!(_id in path(\"drafts.**\"))]._type)",
                &[],
            )
            .await?;
        let mut out: Vec<(String, u64)> = v
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t.as_str())
            .filter(|t| !system_type(t))
            .map(|t| (t.to_string(), 0))
            .collect();
        out.sort();
        Ok(out)
    }
}

/// How one type maps to text.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypeMap {
    pub title: String,
    pub body: Vec<String>,
    pub url: String,
}

/// The mapping for `ty` from a source's options.
pub fn type_map(options: &Value, ty: &str) -> TypeMap {
    let m = &options["types"][ty];
    TypeMap {
        title: m["title"].as_str().unwrap_or("").to_string(),
        body: m["body"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        url: m["url"].as_str().unwrap_or("").to_string(),
    }
}

/// A document as sections, by its type's mapping.
pub fn doc_sections(doc: &Value, title: &str, map: &TypeMap) -> Vec<Section> {
    let mut sections = Vec::new();
    if map.body.is_empty() {
        let body = text::loose_text(doc, 0);
        sections.extend(text::markdown(title, &body));
    } else {
        for field in &map.body {
            let v = text::at_path(doc, field);
            let t = text::loose_text(v, 0);
            if t.trim().is_empty() {
                continue;
            }
            for mut s in text::markdown(title, &t) {
                if map.body.len() > 1 && s.loc == title {
                    s.loc = format!("{title} > {field}");
                }
                sections.push(s);
            }
        }
    }
    sections
}

/// The link a document's citations open.
pub fn doc_url(doc: &Value, map: &TypeMap, options: &Value) -> String {
    if !map.url.is_empty() {
        if let Some(u) = text::fill_template(&map.url, doc) {
            return u;
        }
    }
    let studio = options["studio_url"]
        .as_str()
        .unwrap_or("")
        .trim_end_matches('/');
    match (
        studio.starts_with("https://"),
        doc["_id"].as_str(),
        doc["_type"].as_str(),
    ) {
        (true, Some(id), Some(ty)) => format!("{studio}/intent/edit/id={id};type={ty}"),
        _ => String::new(),
    }
}

#[async_trait::async_trait]
impl Connector for Sanity<'_> {
    fn kind(&self) -> Kind {
        Kind::Sanity
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        match parent.and_then(split_id) {
            None => Ok(self
                .datasets()
                .await?
                .into_iter()
                .filter(|d| matches(query, d))
                .map(|d| Node {
                    id: format!("dataset:{d}"),
                    title: d.clone(),
                    kind: "dataset".into(),
                    has_children: true,
                    selectable: true,
                    detail: "Every content type".into(),
                    ..Default::default()
                })
                .collect()),
            Some((ds, _)) => Ok(self
                .types(ds)
                .await?
                .into_iter()
                .filter(|(t, _)| matches(query, t))
                .map(|(t, _)| Node {
                    id: format!("type:{ds}:{t}"),
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
        // Per dataset: every content type, or the picked ones.
        let mut sets: Vec<(String, Option<Vec<String>>)> = Vec::new();
        for s in &source.selection {
            let (ds, ty) = split_id(&s.id).ok_or_else(|| {
                ConnectorError::Config(format!("\"{}\" isn't a Sanity dataset or type.", s.title))
            })?;
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
        let limit = super::MAX_ITEMS_PER_SOURCE;
        let mut items = Vec::new();
        let mut truncated = false;
        for (ds, types) in &sets {
            let projection = format!("{{_id, _type, _rev, _updatedAt, \"title\": {TITLE_FIELDS}}}");
            let (groq, params) = match types {
                Some(t) => (
                    format!("*[_type in $types && !(_id in path(\"drafts.**\"))] | order(_updatedAt desc) [0...{}] {projection}", limit + 1),
                    vec![("types", json!(t))],
                ),
                None => (
                    format!("*[!(_id in path(\"drafts.**\")) && !(_type match \"sanity.*\") && !(_type match \"system.*\") && !(_type match \"assist.*\")] | order(_updatedAt desc) [0...{}] {projection}", limit + 1),
                    vec![],
                ),
            };
            let v = self.query(ds, &groq, &params).await?;
            for d in v.as_array().into_iter().flatten() {
                let (Some(id), Some(ty)) = (d["_id"].as_str(), d["_type"].as_str()) else {
                    continue;
                };
                items.push(Listed {
                    external_id: format!("{ds}:{id}"),
                    title: d["title"].as_str().unwrap_or(id).to_string(),
                    url: String::new(),
                    updated_at: d["_updatedAt"].as_str().unwrap_or("").to_string(),
                    version: d["_rev"].as_str().unwrap_or("").to_string(),
                    path: format!("{ds} / {ty}"),
                    hint: json!({ "dataset": ds, "id": id, "type": ty }),
                });
            }
        }
        if items.len() > limit {
            truncated = true;
            items.truncate(limit);
        }
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: !truncated,
            cursor: String::new(),
            truncated,
        })
    }

    async fn fetch(&self, source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let ds = item.hint["dataset"].as_str().unwrap_or("");
        let id = item.hint["id"].as_str().unwrap_or("");
        let v = self
            .query(ds, "*[_id == $id][0]", &[("id", json!(id))])
            .await?;
        if v.is_null() {
            return Err(ConnectorError::NotFound("The document was deleted.".into()));
        }
        let ty = v["_type"].as_str().unwrap_or("");
        let map = type_map(&source.options, ty);
        let title = if map.title.is_empty() {
            item.title.clone()
        } else {
            text::at_path(&v, &map.title)
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

/// Registers a webhook in the project that posts every create, update and
/// delete of the picked types to `url`, signed with `secret`. Needs a token
/// that may manage webhooks; a Viewer token is refused, and the source then
/// shows how to add the webhook by hand.
pub async fn register_webhook(
    http: &dyn Http,
    project_id: &str,
    auth: &str,
    dataset: &str,
    types: &[String],
    url: &str,
    secret: &str,
) -> Result<String, ConnectorError> {
    if !valid_project(project_id) || !valid_name(dataset) {
        return Err(ConnectorError::Config(
            "The Sanity project or dataset isn't valid.".into(),
        ));
    }
    let filter = if types.is_empty() {
        "!(_type match \"sanity.*\")".to_string()
    } else {
        format!("_type in {}", json!(types))
    };
    let body = json!({
        "type": "document",
        "name": "Coderhelm knowledge sync",
        "description": "Keeps a Coderhelm knowledge base up to date.",
        "url": url,
        "httpMethod": "POST",
        "apiVersion": "v2021-03-25",
        "includeDrafts": false,
        "dataset": dataset,
        "rule": { "on": ["create", "update", "delete"], "filter": filter, "projection": "{_id, _type, _rev}" },
        "secret": secret,
        "isDisabled": false
    });
    let r = HttpRequest::json(
        &format!("https://api.sanity.io/v2021-10-04/hooks/projects/{project_id}"),
        &body,
        Limit::Metadata,
    )
    .header("Authorization", auth);
    let v = send_json(http, r, "Sanity").await?;
    Ok(v["id"].as_str().unwrap_or("").to_string())
}

/// Removes a webhook Coderhelm registered (when the source is removed).
pub async fn delete_webhook(http: &dyn Http, project_id: &str, auth: &str, hook_id: &str) {
    if !valid_project(project_id) || !valid_name(hook_id) {
        return;
    }
    let mut r = HttpRequest::get(&format!(
        "https://api.sanity.io/v2021-10-04/hooks/projects/{project_id}/{hook_id}"
    ))
    .header("Authorization", auth);
    r.method = crate::mcp_remote::http::Method::Delete;
    let _ = http.send(r).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use crate::mcp_remote::http::Method;

    fn sanity(http: &Fake) -> Sanity<'_> {
        Sanity {
            http,
            project_id: "abc123".into(),
            auth: "Bearer t".into(),
        }
    }

    #[tokio::test]
    async fn picked_types_list_published_documents_with_their_revision() {
        let http = Fake::default();
        http.with(|r| {
            (r.url.contains("abc123.api.sanity.io") && r.url.contains("%24types")).then(|| {
                fjson(200, json!({"result": [
                    {"_id":"p1","_type":"post","_rev":"r9","_updatedAt":"2026-10-01T00:00:00Z","title":"Returns"},
                    {"_id":"p2","_type":"post","_rev":"r3","_updatedAt":"2026-09-01T00:00:00Z","title":"Shipping"}
                ]}))
            })
        });
        let s = sanity(&http);
        let src = Source {
            selection: vec![Selected {
                id: "type:production:post".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let l = s.list(&src).await.unwrap();
        assert_eq!(l.items.len(), 2);
        assert_eq!(l.items[0].external_id, "production:p1");
        assert_eq!(l.items[0].version, "r9");
        assert!(l.complete);
        let sent = &http.sent()[0];
        assert!(sent.url.contains("perspective=published"));
        assert!(sent.url.contains("drafts"));
    }

    #[tokio::test]
    async fn a_document_maps_to_text_and_a_link() {
        let http = Fake::default();
        http.with(|r| {
            r.url.contains("%24id").then(|| {
                fjson(200, json!({"result": {
                    "_id":"p1","_type":"post","_updatedAt":"2026-10-02T00:00:00Z",
                    "headline":"Returns policy","slug":{"current":"returns"},
                    "body":[{"_type":"block","style":"normal","children":[{"text":"Thirty days."}]}],
                    "internal":"do not index"
                }}))
            })
        });
        let s = sanity(&http);
        let src = Source {
            options: json!({"types": {"post": {"title": "headline", "body": ["body"], "url": "https://shop.example/help/{slug.current}"}}}),
            ..Default::default()
        };
        let d = s
            .fetch(
                &src,
                &Listed {
                    hint: json!({"dataset":"production","id":"p1","type":"post"}),
                    title: "p1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(d.title, "Returns policy");
        assert_eq!(d.url, "https://shop.example/help/returns");
        let all: String = d.sections.iter().map(|s| s.text.clone()).collect();
        assert!(all.contains("Thirty days.") && !all.contains("do not index"));
    }

    #[tokio::test]
    async fn a_deleted_document_is_gone() {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .contains("%24id")
                .then(|| fjson(200, json!({"result": null})))
        });
        let e = sanity(&http)
            .fetch(
                &Source::default(),
                &Listed {
                    hint: json!({"dataset":"production","id":"x"}),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(e.item_gone());
    }

    #[tokio::test]
    async fn webhooks_are_registered_for_the_picked_types() {
        let http = Fake::default();
        http.on(
            Method::Post,
            "https://api.sanity.io/v2021-10-04/hooks/projects/abc123",
            fjson(200, json!({"id":"h1"})),
        );
        let id = register_webhook(
            &http,
            "abc123",
            "Bearer t",
            "production",
            &["post".into()],
            "https://api.example/webhooks/knowledge/tok",
            "sec",
        )
        .await
        .unwrap();
        assert_eq!(id, "h1");
        let body: Value = serde_json::from_str(&http.sent()[0].body_text()).unwrap();
        assert_eq!(body["rule"]["filter"], "_type in [\"post\"]");
        assert_eq!(body["secret"], "sec");
        assert_eq!(body["includeDrafts"], false);
    }

    #[test]
    fn ids_and_names_are_checked() {
        assert_eq!(split_id("dataset:production"), Some(("production", None)));
        assert_eq!(
            split_id("type:production:post"),
            Some(("production", Some("post")))
        );
        assert_eq!(split_id("type:prod/../x:post"), None);
        assert!(!valid_project("ABC"));
        assert!(valid_project("abc123"));
        let studio = doc_url(
            &json!({"_id":"a","_type":"post"}),
            &TypeMap::default(),
            &json!({"studio_url":"https://acme.sanity.studio/"}),
        );
        assert_eq!(
            studio,
            "https://acme.sanity.studio/intent/edit/id=a;type=post"
        );
    }
}
