//! Google Drive: folders (with their subfolders) and single files.
//!
//! The connection is a Google sign-in with read-only Drive access. Google
//! Docs, Sheets and Slides are exported as text; PDFs, Word, Excel,
//! Markdown, CSV, HTML and text files are read like uploads. Other files
//! (images, videos) are skipped. A file's `modifiedTime` is its marker; a
//! file moved to the trash, deleted, or no longer shared drops out of the
//! listing and is removed.

use std::collections::HashSet;

use serde_json::{json, Value};

use super::connector::{
    enc, matches, send, send_json, Connector, ConnectorError, Document, Listed, Listing, Node,
};
use super::{Kind, Source};
use crate::knowledge::extract::{extract, Kind as FileKind};
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

pub const API: &str = "https://www.googleapis.com/drive/v3";
const FOLDER: &str = "application/vnd.google-apps.folder";
/// `iconLink` is Drive's own small file type icon, a public image address
/// (only the picker uses it).
const FIELDS: &str =
    "nextPageToken,files(id,name,mimeType,modifiedTime,webViewLink,parents,size,iconLink)";
/// Folder listings per sync at most.
const MAX_FOLDER_CALLS: usize = 300;

pub struct Drive<'a> {
    pub http: &'a dyn Http,
    /// `Bearer ...`.
    pub auth: String,
}

/// How a file is read: exported to a format, or downloaded and read as one
/// of the upload kinds. `None`: skipped.
pub fn reader(mime: &str, name: &str) -> Option<(Option<&'static str>, FileKind)> {
    match mime {
        "application/vnd.google-apps.document" => Some((Some("text/plain"), FileKind::Text)),
        "application/vnd.google-apps.presentation" => Some((Some("text/plain"), FileKind::Text)),
        "application/vnd.google-apps.spreadsheet" => Some((Some("text/csv"), FileKind::Csv)),
        "application/pdf" => Some((None, FileKind::Pdf)),
        _ => FileKind::of(name).map(|k| (None, k)),
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// `folder:<id>` / `file:<id>` as (kind, id).
pub fn split_id(raw: &str) -> Option<(&str, &str)> {
    let (k, id) = raw.split_once(':')?;
    (matches!(k, "folder" | "file") && valid_id(id)).then_some((k, id))
}

fn node(f: &Value) -> Option<Node> {
    let id = f["id"].as_str()?;
    let name = f["name"].as_str().unwrap_or("Untitled");
    let mime = f["mimeType"].as_str().unwrap_or("");
    let folder = mime == FOLDER;
    if !folder && reader(mime, name).is_none() {
        return None;
    }
    Some(Node {
        id: format!("{}:{id}", if folder { "folder" } else { "file" }),
        title: name.to_string(),
        kind: if folder { "folder" } else { "file" }.into(),
        has_children: folder,
        selectable: true,
        url: f["webViewLink"].as_str().unwrap_or("").to_string(),
        updated_at: f["modifiedTime"].as_str().unwrap_or("").to_string(),
        detail: type_label(mime, name).into(),
        icon: f["iconLink"]
            .as_str()
            .filter(|u| u.starts_with("https://"))
            .unwrap_or("")
            .to_string(),
        // Drive gives a size (as a string) for uploaded files only, not for
        // Docs, Sheets or Slides.
        bytes: f["size"]
            .as_str()
            .and_then(|b| b.parse().ok())
            .or_else(|| f["size"].as_u64()),
        ..Default::default()
    })
}

/// A file's type in a word or two, for the line under its name.
pub fn type_label(mime: &str, name: &str) -> &'static str {
    match mime {
        FOLDER => "Folder",
        "application/vnd.google-apps.document" => "Google Doc",
        "application/vnd.google-apps.spreadsheet" => "Google Sheet",
        "application/vnd.google-apps.presentation" => "Google Slides",
        "application/pdf" => "PDF",
        _ => match name
            .rsplit_once('.')
            .map(|(_, e)| e.to_ascii_lowercase())
            .as_deref()
        {
            Some("docx" | "doc") => "Word document",
            Some("xlsx" | "xls") => "Excel spreadsheet",
            Some("md" | "markdown") => "Markdown",
            Some("csv") => "CSV",
            Some("html" | "htm") => "Web page",
            Some("txt" | "text") => "Text",
            _ => "File",
        },
    }
}

impl Drive<'_> {
    fn get(&self, url: &str, limit: Limit) -> HttpRequest {
        let mut r = HttpRequest::get(url).header("Authorization", &self.auth);
        r.limit = limit;
        r
    }

    /// One page of files matching `q`.
    async fn files(&self, q: &str, token: Option<&str>) -> Result<Value, ConnectorError> {
        let mut url = format!(
            "{API}/files?pageSize=200&supportsAllDrives=true&includeItemsFromAllDrives=true&corpora=allDrives&fields={}&q={}",
            enc(FIELDS),
            enc(q)
        );
        if let Some(t) = token {
            url.push_str(&format!("&pageToken={}", enc(t)));
        }
        send_json(self.http, self.get(&url, Limit::Mcp), "Google Drive").await
    }

    async fn folder_files(&self, id: &str) -> Result<Vec<Value>, ConnectorError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let v = self
                .files(
                    &format!("'{id}' in parents and trashed = false"),
                    token.as_deref(),
                )
                .await?;
            out.extend(v["files"].as_array().cloned().unwrap_or_default());
            match v["nextPageToken"].as_str() {
                Some(t) if out.len() < 5_000 => token = Some(t.to_string()),
                _ => return Ok(out),
            }
        }
    }
}

#[async_trait::async_trait]
impl Connector for Drive<'_> {
    fn kind(&self) -> Kind {
        Kind::GoogleDrive
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        if let Some((_, id)) = parent.and_then(split_id) {
            let mut out: Vec<Node> = self
                .folder_files(id)
                .await?
                .iter()
                .filter_map(node)
                .collect();
            out.retain(|n| matches(query, &n.title));
            out.sort_by_key(|n| (n.kind != "folder", n.title.to_lowercase()));
            return Ok(out);
        }
        if !query.trim().is_empty() {
            let q = format!(
                "name contains '{}' and trashed = false",
                query.trim().replace('\\', "\\\\").replace('\'', "\\'")
            );
            let v = self.files(&q, None).await?;
            return Ok(v["files"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(node)
                .collect());
        }
        let mut out = vec![Node {
            id: "folder:root".into(),
            title: "My Drive".into(),
            kind: "folder".into(),
            has_children: true,
            selectable: true,
            ..Default::default()
        }];
        let drives = send_json(
            self.http,
            self.get(&format!("{API}/drives?pageSize=100"), Limit::Mcp),
            "Google Drive",
        )
        .await
        .unwrap_or_default();
        for d in drives["drives"].as_array().into_iter().flatten() {
            if let (Some(id), Some(name)) = (d["id"].as_str(), d["name"].as_str()) {
                out.push(Node {
                    id: format!("folder:{id}"),
                    title: name.to_string(),
                    kind: "folder".into(),
                    has_children: true,
                    selectable: true,
                    detail: "Shared drive".into(),
                    ..Default::default()
                });
            }
        }
        Ok(out)
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let mut items: Vec<Listed> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut calls = 0usize;
        let mut complete = true;
        let push = |f: &Value, path: &str, items: &mut Vec<Listed>, seen: &mut HashSet<String>| {
            let (Some(id), Some(name)) = (f["id"].as_str(), f["name"].as_str()) else {
                return;
            };
            let mime = f["mimeType"].as_str().unwrap_or("");
            if mime == FOLDER || reader(mime, name).is_none() || !seen.insert(id.to_string()) {
                return;
            }
            let edited = f["modifiedTime"].as_str().unwrap_or("").to_string();
            items.push(Listed {
                external_id: id.to_string(),
                title: name.to_string(),
                url: f["webViewLink"].as_str().unwrap_or("").to_string(),
                updated_at: edited.clone(),
                version: edited,
                path: path.to_string(),
                hint: json!({ "mime": mime }),
            });
        };
        for s in &source.selection {
            let Some((kind, id)) = split_id(&s.id) else {
                return Err(ConnectorError::Config(format!(
                    "\"{}\" isn't a Drive folder or file.",
                    s.title
                )));
            };
            if kind == "file" {
                let url = format!(
                    "{API}/files/{id}?supportsAllDrives=true&fields={}",
                    enc("id,name,mimeType,modifiedTime,webViewLink,trashed")
                );
                match send_json(self.http, self.get(&url, Limit::Metadata), "Google Drive").await {
                    Ok(f) if f["trashed"] != true => push(&f, "", &mut items, &mut seen),
                    Ok(_)
                    | Err(ConnectorError::NotFound(_))
                    | Err(ConnectorError::Forbidden(_)) => {}
                    Err(e) => return Err(e),
                }
                continue;
            }
            let mut stack: Vec<(String, String, usize)> =
                vec![(id.to_string(), s.title.clone(), 0)];
            while let Some((folder, path, depth)) = stack.pop() {
                calls += 1;
                if calls > MAX_FOLDER_CALLS || items.len() > super::MAX_ITEMS_PER_SOURCE {
                    complete = false;
                    break;
                }
                let files = match self.folder_files(&folder).await {
                    Ok(f) => f,
                    Err(ConnectorError::NotFound(_)) | Err(ConnectorError::Forbidden(_)) => {
                        continue
                    }
                    Err(e) => return Err(e),
                };
                for f in &files {
                    if f["mimeType"] == FOLDER {
                        if s.include_children && depth < 20 {
                            if let (Some(fid), Some(name)) = (f["id"].as_str(), f["name"].as_str())
                            {
                                stack.push((
                                    fid.to_string(),
                                    format!("{path} / {name}"),
                                    depth + 1,
                                ));
                            }
                        }
                    } else {
                        push(f, &path, &mut items, &mut seen);
                    }
                }
            }
        }
        let truncated = items.len() > super::MAX_ITEMS_PER_SOURCE;
        items.truncate(super::MAX_ITEMS_PER_SOURCE);
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: complete && !truncated,
            cursor: String::new(),
            truncated: truncated || !complete,
        })
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        if !valid_id(&item.external_id) {
            return Err(ConnectorError::NotFound("Not a Drive file id.".into()));
        }
        let mime = item.hint["mime"].as_str().unwrap_or("");
        let (export, kind) = reader(mime, &item.title)
            .ok_or_else(|| ConnectorError::NotFound("This kind of file isn't read.".into()))?;
        let url = match export {
            Some(to) => format!(
                "{API}/files/{}/export?mimeType={}",
                item.external_id,
                enc(to)
            ),
            None => format!(
                "{API}/files/{}?alt=media&supportsAllDrives=true",
                item.external_id
            ),
        };
        let r = send(self.http, self.get(&url, Limit::Document), "Google Drive").await?;
        let bytes = r.body;
        let extracted = tokio::task::spawn_blocking(move || extract(kind, &bytes))
            .await
            .map_err(|_| ConnectorError::Temporary("The file couldn't be read.".into()))?
            .map_err(ConnectorError::Config)?;
        let mut sections = extracted.sections;
        for s in sections.iter_mut() {
            if s.loc.is_empty() {
                s.loc = item.title.clone();
            }
        }
        Ok(Document {
            title: item.title.clone(),
            url: item.url.clone(),
            updated_at: item.updated_at.clone(),
            sections,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{json as fjson, resp, Fake};

    fn f(id: &str, name: &str, mime: &str) -> Value {
        json!({"id": id, "name": name, "mimeType": mime, "modifiedTime": "2026-10-01T00:00:00Z",
               "webViewLink": format!("https://docs.google.com/d/{id}")})
    }

    #[tokio::test]
    async fn the_picker_shows_drive_icons_types_and_sizes() {
        let http = Fake::default();
        http.with(|r| {
            r.url.contains("q=%27top%27").then(|| {
                fjson(
                    200,
                    json!({"files": [
                        {"id": "d1", "name": "Handbook", "mimeType": "application/vnd.google-apps.document",
                         "iconLink": "https://drive-thirdparty.googleusercontent.com/16/type/application/vnd.google-apps.document"},
                        {"id": "p1", "name": "Leave.pdf", "mimeType": "application/pdf", "size": "48213",
                         "iconLink": "http://insecure.test/i.png"},
                        {"id": "sub", "name": "Policies", "mimeType": FOLDER}
                    ]}),
                )
            })
        });
        let d = Drive {
            http: &http,
            auth: "Bearer t".into(),
        };
        let kids = d.browse(Some("folder:top"), "").await.unwrap();
        let titles: Vec<&str> = kids.iter().map(|n| n.title.as_str()).collect();
        assert_eq!(
            titles,
            ["Policies", "Handbook", "Leave.pdf"],
            "folders first"
        );
        assert_eq!(kids[0].detail, "Folder");
        assert_eq!(kids[1].detail, "Google Doc");
        assert!(kids[1].icon.starts_with("https://drive-thirdparty"));
        assert_eq!(kids[1].bytes, None, "a Google Doc has no size");
        assert_eq!(kids[2].detail, "PDF");
        assert_eq!(kids[2].bytes, Some(48_213));
        assert_eq!(kids[2].icon, "", "only https icons");
        assert!(http.sent()[0].url.contains("iconLink"));
    }

    #[tokio::test]
    async fn folders_are_walked_and_unreadable_files_skipped() {
        let http = Fake::default();
        http.with(|r| {
            (r.url.contains("q=%27top%27")).then(|| {
                fjson(
                    200,
                    json!({"files": [
                        f("d1", "Handbook", "application/vnd.google-apps.document"),
                        f("sub", "Policies", FOLDER),
                        f("img", "logo.png", "image/png")
                    ]}),
                )
            })
        });
        http.with(|r| {
            r.url.contains("q=%27sub%27").then(|| {
                fjson(
                    200,
                    json!({"files": [f("p1", "Leave.pdf", "application/pdf")]}),
                )
            })
        });
        let d = Drive {
            http: &http,
            auth: "Bearer t".into(),
        };
        let src = Source {
            selection: vec![Selected {
                id: "folder:top".into(),
                title: "Team".into(),
                include_children: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        let l = d.list(&src).await.unwrap();
        let ids: Vec<&str> = l.items.iter().map(|i| i.external_id.as_str()).collect();
        assert_eq!(ids, ["d1", "p1"]);
        assert_eq!(l.items[1].path, "Team / Policies");
        assert!(l.complete);
    }

    #[tokio::test]
    async fn docs_are_exported_as_text() {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .contains("/files/d1/export?mimeType=text%2Fplain")
                .then(|| resp(200, &[], "Vacation is 25 days."))
        });
        let d = Drive {
            http: &http,
            auth: "Bearer t".into(),
        };
        let doc = d
            .fetch(
                &Source::default(),
                &Listed {
                    external_id: "d1".into(),
                    title: "Handbook".into(),
                    hint: json!({"mime": "application/vnd.google-apps.document"}),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(doc.sections[0].text.contains("25 days"));
        assert_eq!(doc.sections[0].loc, "Handbook");
    }

    #[test]
    fn readers_and_ids() {
        assert!(reader("image/png", "a.png").is_none());
        assert_eq!(
            reader("text/markdown", "a.md").unwrap().1,
            FileKind::Markdown
        );
        assert_eq!(split_id("folder:abc_1-2"), Some(("folder", "abc_1-2")));
        assert_eq!(split_id("folder:a'b"), None);
    }
}
