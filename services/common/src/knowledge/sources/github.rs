//! GitHub: docs folders and Markdown files in the team's repositories,
//! read through the Coderhelm GitHub App the team already installed (no new
//! sign-in).
//!
//! The picker shows the repositories the app can see, then their folders
//! and files. Picking a repository takes every Markdown file in it; picking
//! a folder takes the Markdown under it. Listing reads the default branch's
//! whole tree in one call, and each file's blob sha is its marker, so only
//! files whose content changed are read again. A push to the default branch
//! starts a sync (the app's push webhook).

use serde_json::json;

use super::connector::{
    enc, matches, send, send_json, Connector, ConnectorError, Document, Listed, Listing, Node,
};
use super::{text, Kind, Source};
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

pub const API: &str = "https://api.github.com";

pub struct GitHub<'a> {
    pub http: &'a dyn Http,
    /// `Bearer <installation token>`.
    pub auth: String,
}

/// The file types synced from a repository.
pub fn is_doc(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let skip = [
        "node_modules/",
        "vendor/",
        ".github/",
        "target/",
        "dist/",
        "build/",
    ];
    if skip
        .iter()
        .any(|s| lower.starts_with(s) || lower.contains(&format!("/{s}")))
    {
        return false;
    }
    [".md", ".mdx", ".markdown", ".txt", ".rst", ".adoc"]
        .iter()
        .any(|e| lower.ends_with(e))
}

fn valid_repo(r: &str) -> bool {
    let mut parts = r.split('/');
    let ok = |p: Option<&str>| {
        p.is_some_and(|p| {
            !p.is_empty()
                && p.len() <= 100
                && p != "."
                && p != ".."
                && p.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        })
    };
    ok(parts.next()) && ok(parts.next()) && parts.next().is_none()
}

fn valid_path(p: &str) -> bool {
    p.len() <= 1_000
        && !p.split('/').any(|s| s == ".." || s == ".")
        && !p.starts_with('/')
        && !p.contains('\\')
}

/// `repo:<owner/name>`, `path:<owner/name>:<dir>`, `file:<owner/name>:<path>`
/// as (kind, repo, path).
pub fn split_id(raw: &str) -> Option<(&str, &str, &str)> {
    let (kind, rest) = raw.split_once(':')?;
    let (repo, path) = match kind {
        "repo" => (rest, ""),
        "path" | "file" => rest.split_once(':')?,
        _ => return None,
    };
    (valid_repo(repo) && valid_path(path)).then_some((kind, repo, path))
}

/// A path's segments percent-encoded for a URL.
fn path_url(p: &str) -> String {
    p.split('/')
        .map(|s| enc(s).replace('+', "%20"))
        .collect::<Vec<_>>()
        .join("/")
}

impl GitHub<'_> {
    fn get(&self, url: &str, accept: &str, limit: Limit) -> HttpRequest {
        let mut r = HttpRequest::get(url)
            .header("Authorization", &self.auth)
            .header("X-GitHub-Api-Version", "2022-11-28");
        r.headers.retain(|(k, _)| k != "Accept");
        r.headers.push(("Accept".into(), accept.into()));
        r.limit = limit;
        r
    }

    async fn default_branch(&self, repo: &str) -> Result<String, ConnectorError> {
        let v = send_json(
            self.http,
            self.get(
                &format!("{API}/repos/{repo}"),
                "application/vnd.github+json",
                Limit::Metadata,
            ),
            "GitHub",
        )
        .await?;
        Ok(v["default_branch"].as_str().unwrap_or("main").to_string())
    }

    /// Every file path of the branch with its blob sha, and whether the
    /// tree came back whole.
    async fn tree(
        &self,
        repo: &str,
        branch: &str,
    ) -> Result<(Vec<(String, String)>, bool), ConnectorError> {
        let v = send_json(
            self.http,
            self.get(
                &format!("{API}/repos/{repo}/git/trees/{}?recursive=1", enc(branch)),
                "application/vnd.github+json",
                Limit::Mcp,
            ),
            "GitHub",
        )
        .await?;
        let files = v["tree"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|e| e["type"] == "blob")
            .filter_map(|e| {
                Some((
                    e["path"].as_str()?.to_string(),
                    e["sha"].as_str()?.to_string(),
                ))
            })
            .collect();
        Ok((files, v["truncated"] != true))
    }
}

#[async_trait::async_trait]
impl Connector for GitHub<'_> {
    fn kind(&self) -> Kind {
        Kind::Github
    }

    async fn browse(&self, parent: Option<&str>, query: &str) -> Result<Vec<Node>, ConnectorError> {
        let Some((_, repo, path)) = parent.and_then(split_id) else {
            let v = send_json(
                self.http,
                self.get(
                    &format!("{API}/installation/repositories?per_page=100"),
                    "application/vnd.github+json",
                    Limit::Mcp,
                ),
                "GitHub",
            )
            .await?;
            let mut out: Vec<Node> = v["repositories"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|r| {
                    let full = r["full_name"].as_str()?;
                    matches(query, full).then(|| Node {
                        id: format!("repo:{full}"),
                        title: full.to_string(),
                        kind: "repo".into(),
                        has_children: true,
                        selectable: true,
                        url: r["html_url"].as_str().unwrap_or("").to_string(),
                        detail: "Every Markdown file".into(),
                        ..Default::default()
                    })
                })
                .collect();
            out.sort_by_key(|n| n.title.to_lowercase());
            return Ok(out);
        };
        let url = if path.is_empty() {
            format!("{API}/repos/{repo}/contents")
        } else {
            format!("{API}/repos/{repo}/contents/{}", path_url(path))
        };
        let v = send_json(
            self.http,
            self.get(&url, "application/vnd.github+json", Limit::Mcp),
            "GitHub",
        )
        .await?;
        let mut out: Vec<Node> = v
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| {
                let p = e["path"].as_str()?;
                let name = e["name"].as_str().unwrap_or(p);
                let dir = e["type"] == "dir";
                if (!dir && !is_doc(p)) || !matches(query, name) {
                    return None;
                }
                Some(Node {
                    id: format!("{}:{repo}:{p}", if dir { "path" } else { "file" }),
                    title: name.to_string(),
                    kind: if dir { "folder" } else { "file" }.into(),
                    has_children: dir,
                    selectable: true,
                    url: e["html_url"].as_str().unwrap_or("").to_string(),
                    ..Default::default()
                })
            })
            .collect();
        out.sort_by_key(|n| (n.kind != "folder", n.title.to_lowercase()));
        Ok(out)
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        // Per repository: the picked folders and files ("" = the whole repo).
        let mut repos: Vec<(String, Vec<(String, bool, bool)>)> = Vec::new();
        for s in &source.selection {
            let (kind, repo, path) = split_id(&s.id).ok_or_else(|| {
                ConnectorError::Config(format!(
                    "\"{}\" isn't a repository, folder or file.",
                    s.title
                ))
            })?;
            let at = match repos.iter().position(|(r, _)| r == repo) {
                Some(i) => i,
                None => {
                    repos.push((repo.to_string(), Vec::new()));
                    repos.len() - 1
                }
            };
            repos[at]
                .1
                .push((path.to_string(), kind == "file", s.include_children));
        }
        let mut items = Vec::new();
        let mut complete = true;
        for (repo, picks) in &repos {
            let branch = match self.default_branch(repo).await {
                Ok(b) => b,
                Err(ConnectorError::NotFound(_)) | Err(ConnectorError::Forbidden(_)) => continue,
                Err(e) => return Err(e),
            };
            let (files, whole) = self.tree(repo, &branch).await?;
            complete &= whole;
            for (path, sha) in files {
                let picked = picks.iter().any(|(p, file, deep)| {
                    if *file {
                        &path == p
                    } else if p.is_empty() {
                        *deep || !path.contains('/')
                    } else {
                        path.strip_prefix(&format!("{p}/"))
                            .is_some_and(|rest| *deep || !rest.contains('/'))
                    }
                });
                if !picked || !is_doc(&path) {
                    continue;
                }
                let title = path.rsplit('/').next().unwrap_or(&path).to_string();
                let dir = path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
                items.push(Listed {
                    external_id: format!("{repo}:{path}"),
                    title,
                    url: format!(
                        "https://github.com/{repo}/blob/{}/{}",
                        path_url(&branch),
                        path_url(&path)
                    ),
                    updated_at: String::new(),
                    version: sha,
                    path: if dir.is_empty() {
                        repo.clone()
                    } else {
                        format!("{repo} / {dir}")
                    },
                    hint: json!({ "repo": repo, "path": path, "branch": branch }),
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
        let repo = item.hint["repo"].as_str().unwrap_or("");
        let path = item.hint["path"].as_str().unwrap_or("");
        let branch = item.hint["branch"].as_str().unwrap_or("");
        if !valid_repo(repo) || !valid_path(path) || path.is_empty() {
            return Err(ConnectorError::NotFound(
                "Not a file in a repository.".into(),
            ));
        }
        let r = send(
            self.http,
            self.get(
                &format!(
                    "{API}/repos/{repo}/contents/{}?ref={}",
                    path_url(path),
                    enc(branch)
                ),
                "application/vnd.github.raw+json",
                Limit::Document,
            ),
            "GitHub",
        )
        .await?;
        let body = String::from_utf8_lossy(&r.body).into_owned();
        let title = first_heading(&body).unwrap_or_else(|| item.title.clone());
        Ok(Document {
            url: item.url.clone(),
            updated_at: item.updated_at.clone(),
            sections: text::markdown(&title, &strip_front_matter(&body)),
            title,
        })
    }
}

/// The text of the first `# ` heading.
fn first_heading(md: &str) -> Option<String> {
    strip_front_matter(md)
        .lines()
        .find_map(|l| l.strip_prefix("# ").map(|t| t.trim().to_string()))
        .filter(|t| !t.is_empty())
}

/// Markdown without its leading `---` front matter block.
fn strip_front_matter(md: &str) -> String {
    let t = md.trim_start_matches('\u{feff}');
    if let Some(rest) = t.strip_prefix("---\n") {
        if let Some(end) = rest.find("\n---") {
            return rest[end + 4..].trim_start_matches('\n').to_string();
        }
    }
    t.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{json as fjson, resp, Fake};

    fn rig() -> Fake {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .ends_with("/repos/acme/app")
                .then(|| fjson(200, json!({"default_branch": "main"})))
        });
        http.with(|r| {
            r.url.contains("/git/trees/main").then(|| {
                fjson(
                    200,
                    json!({"truncated": false, "tree": [
                        {"type":"blob","path":"README.md","sha":"s1"},
                        {"type":"blob","path":"docs/setup.md","sha":"s2"},
                        {"type":"blob","path":"docs/deep/more.mdx","sha":"s3"},
                        {"type":"blob","path":"docs/logo.png","sha":"s4"},
                        {"type":"blob","path":"node_modules/x/README.md","sha":"s5"},
                        {"type":"tree","path":"docs","sha":"t1"}
                    ]}),
                )
            })
        });
        http
    }

    fn src(id: &str, deep: bool) -> Source {
        Source {
            selection: vec![Selected {
                id: id.into(),
                include_children: deep,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn folders_take_the_markdown_under_them() {
        let http = rig();
        let g = GitHub {
            http: &http,
            auth: "Bearer t".into(),
        };
        let ids = |l: Listing| {
            l.items
                .into_iter()
                .map(|i| i.external_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(g.list(&src("path:acme/app:docs", true)).await.unwrap()),
            ["acme/app:docs/setup.md", "acme/app:docs/deep/more.mdx"]
        );
        assert_eq!(
            ids(g.list(&src("path:acme/app:docs", false)).await.unwrap()),
            ["acme/app:docs/setup.md"]
        );
        assert_eq!(
            ids(g.list(&src("repo:acme/app", true)).await.unwrap()).len(),
            3
        );
        let one = g
            .list(&src("file:acme/app:README.md", false))
            .await
            .unwrap();
        assert_eq!(one.items[0].version, "s1");
        assert_eq!(
            one.items[0].url,
            "https://github.com/acme/app/blob/main/README.md"
        );
    }

    #[tokio::test]
    async fn a_file_reads_without_front_matter() {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .contains("/contents/docs/setup.md?ref=main")
                .then(|| resp(200, &[], "---\ntitle: x\n---\n# Setup\n\nRun it."))
        });
        let g = GitHub {
            http: &http,
            auth: "Bearer t".into(),
        };
        let d = g
            .fetch(
                &Source::default(),
                &Listed {
                    title: "setup.md".into(),
                    hint: json!({"repo":"acme/app","path":"docs/setup.md","branch":"main"}),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(d.title, "Setup");
        assert!(!d.sections.iter().any(|s| s.text.contains("title: x")));
    }

    #[test]
    fn ids_are_checked() {
        assert_eq!(
            split_id("path:acme/app:docs/a b"),
            Some(("path", "acme/app", "docs/a b"))
        );
        assert_eq!(split_id("path:acme/app:../etc"), None);
        assert_eq!(split_id("repo:acme"), None);
        assert!(is_doc("docs/a.md") && !is_doc("src/a.rs") && !is_doc("web/node_modules/a.md"));
    }
}
