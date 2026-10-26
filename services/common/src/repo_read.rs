//! Reading the team's repositories, for Chat.
//!
//! Chat answers questions such as "how is web put together?" or "where are
//! refunds handled?" by reading the code itself: the repository's overview
//! (README, top of the tree, manifests, agent notes), files with line
//! ranges, file listings and code search. Every read is pinned to one commit
//! of the default branch, so a citation links to exactly the lines it quotes
//! even after the branch moves on.
//!
//! [`RepoHost`] is where the bytes come from: [`GitHubHost`] reads through
//! the team's GitHub App installation, [`MemHost`] is an in-memory
//! repository for tests and the Chat evals. [`CodeReader`] holds the
//! repositories one question may read and the snapshot of each it already
//! fetched, and does the work every tool needs (name lookup, overviews,
//! line ranges, listings, search with a fallback on file names).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::knowledge::sources::connector::enc;
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

/// GitHub's API.
pub const GITHUB_API: &str = "https://api.github.com";

/// A repository Chat may read.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct RepoInfo {
    /// `owner/name`.
    pub name: String,
    /// The default branch.
    pub branch: String,
    /// What it is for, as the team set it (`web`, `backend`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// A description, when one is stored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl RepoInfo {
    pub fn new(name: &str, branch: &str) -> Self {
        RepoInfo {
            name: name.to_string(),
            branch: branch.to_string(),
            ..Default::default()
        }
    }

    /// The name without the owner.
    pub fn short(&self) -> &str {
        self.name.rsplit('/').next().unwrap_or(&self.name)
    }
}

/// One file in a snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct TreeEntry {
    pub path: String,
    pub size: u64,
}

/// A repository at one commit: every file path.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub commit: String,
    pub files: Vec<TreeEntry>,
    /// The host cut the listing short (a very large repository).
    pub truncated: bool,
}

/// A code search match.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub repo: String,
    pub path: String,
    /// Matching lines with a little context.
    pub fragments: Vec<String>,
}

/// Why a read failed, in words the model can act on.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadError {
    /// No such repository, file or folder.
    NotFound(String),
    /// The host refused or couldn't be reached.
    Unavailable(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::NotFound(m) | ReadError::Unavailable(m) => f.write_str(m),
        }
    }
}

/// Where a repository's bytes come from.
#[async_trait::async_trait]
pub trait RepoHost: Send + Sync {
    /// The commit `branch` points at.
    async fn head(&self, repo: &str, branch: &str) -> Result<String, ReadError>;
    /// Every file at `commit`.
    async fn tree(&self, repo: &str, commit: &str) -> Result<Snapshot, ReadError>;
    /// A file's text at `commit`; `None` when there is no such file.
    async fn file(&self, repo: &str, commit: &str, path: &str)
        -> Result<Option<String>, ReadError>;
    /// Code search across `repos` (their default branches).
    async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, ReadError>;
}

// ── GitHub ──────────────────────────────────────────────────────────────────

/// Reads through the team's GitHub App installation.
pub struct GitHubHost<'a> {
    pub http: &'a dyn Http,
    /// `Bearer <installation token>`.
    pub auth: String,
}

/// A path's segments percent-encoded for a URL.
fn path_url(p: &str) -> String {
    p.split('/')
        .map(|s| enc(s).replace('+', "%20"))
        .collect::<Vec<_>>()
        .join("/")
}

impl GitHubHost<'_> {
    fn get(&self, url: &str, accept: &str, limit: Limit) -> HttpRequest {
        let mut r = HttpRequest::get(url)
            .header("Authorization", &self.auth)
            .header("X-GitHub-Api-Version", "2022-11-28");
        r.headers.retain(|(k, _)| k != "Accept");
        r.headers.push(("Accept".into(), accept.into()));
        r.limit = limit;
        r
    }

    async fn send(&self, req: HttpRequest) -> Result<(u16, Vec<u8>), ReadError> {
        let r = self
            .http
            .send(req)
            .await
            .map_err(|e| ReadError::Unavailable(format!("GitHub couldn't be reached: {e}")))?;
        Ok((r.status, r.body))
    }
}

fn github_status(status: u16, what: &str) -> ReadError {
    match status {
        404 | 410 => ReadError::NotFound(format!("{what} wasn't found on GitHub.")),
        401 | 403 => ReadError::Unavailable(format!(
            "GitHub didn't let Coderhelm read {what} (status {status})."
        )),
        _ => ReadError::Unavailable(format!("GitHub answered {status} for {what}.")),
    }
}

#[async_trait::async_trait]
impl RepoHost for GitHubHost<'_> {
    async fn head(&self, repo: &str, branch: &str) -> Result<String, ReadError> {
        let url = format!("{GITHUB_API}/repos/{repo}/commits/{}", enc(branch));
        let (status, body) = self
            .send(self.get(&url, "application/vnd.github.sha", Limit::Metadata))
            .await?;
        if !(200..300).contains(&status) {
            return Err(github_status(status, repo));
        }
        let sha = String::from_utf8_lossy(&body).trim().to_string();
        if sha.len() >= 7 && sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            Ok(sha)
        } else {
            Err(ReadError::Unavailable(format!(
                "GitHub didn't say which commit {repo} is on."
            )))
        }
    }

    async fn tree(&self, repo: &str, commit: &str) -> Result<Snapshot, ReadError> {
        let url = format!("{GITHUB_API}/repos/{repo}/git/trees/{commit}?recursive=1");
        let (status, body) = self
            .send(self.get(&url, "application/vnd.github+json", Limit::Mcp))
            .await?;
        if !(200..300).contains(&status) {
            return Err(github_status(status, repo));
        }
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let files = v["tree"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|e| e["type"] == "blob")
            .filter_map(|e| {
                Some(TreeEntry {
                    path: e["path"].as_str()?.to_string(),
                    size: e["size"].as_u64().unwrap_or(0),
                })
            })
            .collect();
        Ok(Snapshot {
            commit: commit.to_string(),
            files,
            truncated: v["truncated"] == true,
        })
    }

    async fn file(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<String>, ReadError> {
        let url = format!(
            "{GITHUB_API}/repos/{repo}/contents/{}?ref={commit}",
            path_url(path)
        );
        let (status, body) = self
            .send(self.get(&url, "application/vnd.github.raw+json", Limit::Mcp))
            .await?;
        match status {
            200..=299 => Ok(Some(String::from_utf8_lossy(&body).into_owned())),
            404 => Ok(None),
            s => Err(github_status(s, path)),
        }
    }

    async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, ReadError> {
        let scope: Vec<String> = repos.iter().map(|r| format!("repo:{r}")).collect();
        let q = format!("{query} {}", scope.join(" "));
        let url = format!("{GITHUB_API}/search/code?per_page=20&q={}", enc(&q));
        let (status, body) = self
            .send(self.get(&url, "application/vnd.github.text-match+json", Limit::Mcp))
            .await?;
        if !(200..300).contains(&status) {
            return Err(github_status(status, "code search"));
        }
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        Ok(v["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|i| {
                Some(SearchHit {
                    repo: i["repository"]["full_name"].as_str()?.to_string(),
                    path: i["path"].as_str()?.to_string(),
                    fragments: i["text_matches"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|m| m["fragment"].as_str().map(str::to_string))
                        .take(3)
                        .collect(),
                })
            })
            .collect())
    }
}

// ── In memory ───────────────────────────────────────────────────────────────

/// A repository held in memory: every file's text at one commit. For tests
/// and the Chat evals.
#[derive(Debug, Clone, Default)]
pub struct MemHost {
    /// `owner/name` to (commit, path to text).
    pub repos: BTreeMap<String, (String, BTreeMap<String, String>)>,
}

impl MemHost {
    pub fn with_repo(mut self, repo: &str, commit: &str, files: &[(&str, &str)]) -> Self {
        self.repos.insert(
            repo.to_string(),
            (
                commit.to_string(),
                files
                    .iter()
                    .map(|(p, t)| (p.to_string(), t.to_string()))
                    .collect(),
            ),
        );
        self
    }

    fn get(&self, repo: &str) -> Result<&(String, BTreeMap<String, String>), ReadError> {
        self.repos
            .get(repo)
            .ok_or_else(|| ReadError::NotFound(format!("{repo} wasn't found.")))
    }
}

#[async_trait::async_trait]
impl RepoHost for MemHost {
    async fn head(&self, repo: &str, _branch: &str) -> Result<String, ReadError> {
        Ok(self.get(repo)?.0.clone())
    }

    async fn tree(&self, repo: &str, commit: &str) -> Result<Snapshot, ReadError> {
        let (_, files) = self.get(repo)?;
        Ok(Snapshot {
            commit: commit.to_string(),
            files: files
                .iter()
                .map(|(p, t)| TreeEntry {
                    path: p.clone(),
                    size: t.len() as u64,
                })
                .collect(),
            truncated: false,
        })
    }

    async fn file(
        &self,
        repo: &str,
        _commit: &str,
        path: &str,
    ) -> Result<Option<String>, ReadError> {
        Ok(self.get(repo)?.1.get(path).cloned())
    }

    async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, ReadError> {
        let words: Vec<String> = query
            .split_whitespace()
            .map(|w| w.to_ascii_lowercase())
            .collect();
        let mut out = Vec::new();
        for repo in repos {
            let Ok((_, files)) = self.get(repo) else {
                continue;
            };
            for (path, text) in files {
                let lower = text.to_ascii_lowercase();
                if words.is_empty() || !words.iter().all(|w| lower.contains(w.as_str())) {
                    continue;
                }
                let fragments = text
                    .lines()
                    .filter(|l| {
                        let l = l.to_ascii_lowercase();
                        words.iter().any(|w| l.contains(w.as_str()))
                    })
                    .take(3)
                    .map(str::to_string)
                    .collect();
                out.push(SearchHit {
                    repo: repo.clone(),
                    path: path.clone(),
                    fragments,
                });
            }
        }
        Ok(out)
    }
}

// ── The reader ──────────────────────────────────────────────────────────────

/// Lines one read returns when no range is asked for.
pub const READ_LINES: usize = 400;
/// Most lines one read returns.
pub const MAX_READ_LINES: usize = 600;
/// Most characters one read returns.
const MAX_READ_CHARS: usize = 60_000;
/// Most entries a listing shows.
const MAX_LIST: usize = 300;

/// Files a repository's overview reads when they are at its top.
const OVERVIEW_DOCS: &[&str] = &[
    "README.md",
    "README.mdx",
    "README.rst",
    "README.txt",
    "README",
    "readme.md",
    "AGENTS.md",
    "CLAUDE.md",
    "ARCHITECTURE.md",
    "docs/ARCHITECTURE.md",
    "docs/architecture.md",
    "CONTRIBUTING.md",
];

/// Build and dependency files that say what a repository is made of.
const MANIFESTS: &[&str] = &[
    "package.json",
    "pnpm-workspace.yaml",
    "turbo.json",
    "nx.json",
    "lerna.json",
    "Cargo.toml",
    "pyproject.toml",
    "requirements.txt",
    "setup.py",
    "Pipfile",
    "go.mod",
    "Gemfile",
    "composer.json",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "settings.gradle",
    "Package.swift",
    "pubspec.yaml",
    "mix.exs",
    "deno.json",
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.yaml",
    "serverless.yml",
    "cdk.json",
    "Makefile",
];

/// Folders never worth listing.
const SKIP_DIRS: &[&str] = &[
    "node_modules/",
    "vendor/",
    "dist/",
    "build/",
    "target/",
    ".next/",
    "coverage/",
    "__pycache__/",
    ".venv/",
];

fn skipped(path: &str) -> bool {
    SKIP_DIRS
        .iter()
        .any(|d| path.starts_with(d) || path.contains(&format!("/{d}")))
}

/// A path as asked, or `None` when it tries to leave the repository.
pub fn clean_path(raw: &str) -> Option<String> {
    let p = raw.trim().trim_start_matches("./").trim_matches('/');
    if p.len() > 1_000 || p.contains('\\') || p.split('/').any(|s| s == ".." || s == ".") {
        return None;
    }
    Some(p.to_string())
}

/// Lowercase letters and digits only, for matching a name however it is
/// written ("Web App", "web-app" and "web_app" are the same).
fn squash(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// The repositories `text` names, by their full or short name as a whole
/// word (so "web" in "website" doesn't count), longest name first. Exact
/// names from the team's own list.
pub fn mentioned<'r>(repos: &'r [RepoInfo], text: &str) -> Vec<&'r RepoInfo> {
    let lower = text.to_ascii_lowercase();
    let is_word =
        |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/';
    let has_word = |needle: &str| {
        if needle.is_empty() {
            return false;
        }
        let mut from = 0;
        while let Some(i) = lower[from..].find(needle) {
            let at = from + i;
            let before = lower[..at].chars().next_back();
            let rest = &lower[at + needle.len()..];
            // A dot that ends a sentence ends the name too.
            let after_ok = match rest.chars().next() {
                None => true,
                Some('.') => rest[1..]
                    .chars()
                    .next()
                    .is_none_or(|c| !c.is_ascii_alphanumeric()),
                Some(c) => !is_word(c),
            };
            if before.is_none_or(|c| !is_word(c)) && after_ok {
                return true;
            }
            from = at + needle.len();
        }
        false
    };
    let mut out: Vec<&RepoInfo> = repos
        .iter()
        .filter(|r| {
            has_word(&r.name.to_ascii_lowercase()) || has_word(&r.short().to_ascii_lowercase())
        })
        .collect();
    out.sort_by_key(|r| std::cmp::Reverse(r.short().len()));
    out
}

/// A part of a file, as read.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FileSlice {
    pub repo: String,
    pub path: String,
    pub commit: String,
    /// 1-based, inclusive.
    pub start: usize,
    pub end: usize,
    pub total_lines: usize,
    /// The lines, unnumbered.
    pub text: String,
}

impl FileSlice {
    /// The lines numbered for the model, with where they came from.
    pub fn numbered(&self) -> String {
        let width = self.end.to_string().len();
        let mut out = format!(
            "{} in {} (lines {}-{} of {})\n",
            self.path, self.repo, self.start, self.end, self.total_lines
        );
        for (i, line) in self.text.lines().enumerate() {
            out.push_str(&format!("{:>width$} | {line}\n", self.start + i));
        }
        if self.end < self.total_lines {
            out.push_str(&format!(
                "(more below: read from line {} to continue)\n",
                self.end + 1
            ));
        }
        out
    }

    /// The page on GitHub at this commit, at these lines.
    pub fn url(&self) -> String {
        blob_url(&self.repo, &self.commit, &self.path, self.start, self.end)
    }
}

/// A file on GitHub at one commit, at lines `start` to `end` (0 for none).
pub fn blob_url(repo: &str, commit: &str, path: &str, start: usize, end: usize) -> String {
    let mut u = format!("https://github.com/{repo}/blob/{commit}/{}", path_url(path));
    if start > 0 {
        u.push_str(&format!("#L{start}"));
        if end > start {
            u.push_str(&format!("-L{end}"));
        }
    }
    u
}

/// A repository's overview: what the model reads, and the files it quotes.
#[derive(Debug, Clone, PartialEq)]
pub struct Overview {
    pub repo: RepoInfo,
    pub commit: String,
    /// The layout: top folders with file counts, languages, manifests.
    pub layout: String,
    /// README, agent notes and manifests, as read.
    pub files: Vec<FileSlice>,
}

/// A folder's listing.
#[derive(Debug, Clone, PartialEq)]
pub struct Listing {
    pub repo: String,
    pub commit: String,
    pub dir: String,
    pub text: String,
    pub count: usize,
}

/// The repositories one question may read, and what was already fetched.
pub struct CodeReader<'a> {
    host: &'a dyn RepoHost,
    repos: Vec<RepoInfo>,
    snaps: tokio::sync::Mutex<HashMap<String, Arc<Snapshot>>>,
}

impl<'a> CodeReader<'a> {
    pub fn new(host: &'a dyn RepoHost, mut repos: Vec<RepoInfo>) -> Self {
        repos.sort_by(|a, b| a.name.cmp(&b.name));
        CodeReader {
            host,
            repos,
            snaps: Default::default(),
        }
    }

    pub fn repos(&self) -> &[RepoInfo] {
        &self.repos
    }

    /// The repository a name means: `owner/name`, or the name alone,
    /// however it is cased or punctuated. `None` when it matches none, or
    /// when a bare name matches more than one owner's repository.
    pub fn resolve(&self, raw: &str) -> Option<&RepoInfo> {
        let raw = raw.trim().trim_end_matches(".git");
        if raw.is_empty() {
            return None;
        }
        if let Some(r) = self.repos.iter().find(|r| r.name.eq_ignore_ascii_case(raw)) {
            return Some(r);
        }
        let want = squash(raw.rsplit('/').next().unwrap_or(raw));
        if want.is_empty() {
            return None;
        }
        let mut found = self.repos.iter().filter(|r| squash(r.short()) == want);
        match (found.next(), found.next()) {
            (Some(r), None) => Some(r),
            _ => None,
        }
    }

    /// The repositories a question names (see [`mentioned`]).
    pub fn mentioned(&self, text: &str) -> Vec<&RepoInfo> {
        mentioned(&self.repos, text)
    }

    fn repo(&self, raw: &str) -> Result<&RepoInfo, ReadError> {
        self.resolve(raw).ok_or_else(|| {
            let names: Vec<&str> = self.repos.iter().map(|r| r.name.as_str()).collect();
            ReadError::NotFound(format!(
                "{raw} isn't one of the repositories you can read. They are: {}.",
                names.join(", ")
            ))
        })
    }

    async fn snapshot(&self, repo: &RepoInfo) -> Result<Arc<Snapshot>, ReadError> {
        if let Some(s) = self.snaps.lock().await.get(&repo.name) {
            return Ok(s.clone());
        }
        let commit = self.host.head(&repo.name, &repo.branch).await?;
        let snap = Arc::new(self.host.tree(&repo.name, &commit).await?);
        self.snaps
            .lock()
            .await
            .insert(repo.name.clone(), snap.clone());
        Ok(snap)
    }

    /// Files whose path looks like `path`: the same file name, or a path
    /// containing it. For a "not found" that says what is there instead.
    fn near(snap: &Snapshot, path: &str) -> Vec<String> {
        let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
        let lower = path.to_ascii_lowercase();
        let mut out: Vec<String> = snap
            .files
            .iter()
            .filter(|f| !skipped(&f.path))
            .filter(|f| {
                let p = f.path.to_ascii_lowercase();
                p.ends_with(&format!("/{name}"))
                    || p == name
                    || (lower.len() > 3 && p.contains(&lower))
            })
            .map(|f| f.path.clone())
            .take(8)
            .collect();
        out.sort();
        out
    }

    /// Lines `start` to `end` of a file (the first [`READ_LINES`] when no
    /// range is given; at most [`MAX_READ_LINES`]).
    pub async fn read(
        &self,
        repo: &str,
        path: &str,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<FileSlice, ReadError> {
        let info = self.repo(repo)?.clone();
        let path = clean_path(path)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| ReadError::NotFound("Give a file path inside the repository.".into()))?;
        let snap = self.snapshot(&info).await?;
        if !snap.files.iter().any(|f| f.path == path) {
            let is_dir = snap
                .files
                .iter()
                .any(|f| f.path.starts_with(&format!("{path}/")));
            if is_dir {
                return Err(ReadError::NotFound(format!(
                    "{path} is a folder in {}. List it to see its files.",
                    info.name
                )));
            }
            let near = Self::near(&snap, &path);
            return Err(ReadError::NotFound(if near.is_empty() {
                format!("There is no {path} in {}.", info.name)
            } else {
                format!(
                    "There is no {path} in {}. Files with a similar name: {}.",
                    info.name,
                    near.join(", ")
                )
            }));
        }
        let text = self
            .host
            .file(&info.name, &snap.commit, &path)
            .await?
            .ok_or_else(|| ReadError::NotFound(format!("There is no {path} in {}.", info.name)))?;
        if text.contains('\0') {
            return Err(ReadError::NotFound(format!(
                "{path} is a binary file, so it can't be read as text."
            )));
        }
        let lines: Vec<&str> = text.lines().collect();
        let total = lines.len().max(1);
        let from = start.unwrap_or(1).clamp(1, total);
        let to = end
            .unwrap_or(from + READ_LINES - 1)
            .clamp(from, total)
            .min(from + MAX_READ_LINES - 1);
        let mut body = String::new();
        let mut last = from - 1;
        for line in lines.iter().take(to).skip(from - 1) {
            if body.len() + line.len() > MAX_READ_CHARS {
                break;
            }
            body.push_str(line);
            body.push('\n');
            last += 1;
        }
        Ok(FileSlice {
            repo: info.name.clone(),
            path,
            commit: snap.commit.clone(),
            start: from,
            end: last.max(from),
            total_lines: if text.is_empty() { 0 } else { lines.len() },
            text: body,
        })
    }

    /// A folder's files and subfolders (with how many files each holds),
    /// narrowed to paths containing `pattern` when one is given (then the
    /// whole repository is searched by path).
    pub async fn list(&self, repo: &str, dir: &str, pattern: &str) -> Result<Listing, ReadError> {
        let info = self.repo(repo)?.clone();
        let dir = clean_path(dir)
            .ok_or_else(|| ReadError::NotFound("Give a folder inside the repository.".into()))?;
        let snap = self.snapshot(&info).await?;
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let pattern = pattern.trim().to_ascii_lowercase();
        let mut text = String::new();
        let count;
        if !pattern.is_empty() {
            let hits: Vec<&TreeEntry> = snap
                .files
                .iter()
                .filter(|f| f.path.starts_with(&prefix) && !skipped(&f.path))
                .filter(|f| f.path.to_ascii_lowercase().contains(&pattern))
                .collect();
            count = hits.len();
            text.push_str(&format!(
                "Files in {} whose path contains \"{pattern}\" ({count}):\n",
                info.name
            ));
            for f in hits.iter().take(MAX_LIST) {
                text.push_str(&format!("{}\n", f.path));
            }
        } else {
            let mut dirs: BTreeMap<String, usize> = BTreeMap::new();
            let mut files: Vec<&TreeEntry> = Vec::new();
            for f in snap.files.iter().filter(|f| f.path.starts_with(&prefix)) {
                let rest = &f.path[prefix.len()..];
                match rest.split_once('/') {
                    Some((d, _)) => *dirs.entry(d.to_string()).or_default() += 1,
                    None => files.push(f),
                }
            }
            if dirs.is_empty() && files.is_empty() {
                return Err(ReadError::NotFound(format!(
                    "There is no folder {dir} in {}.",
                    info.name
                )));
            }
            count = dirs.len() + files.len();
            let place = if dir.is_empty() {
                "the top".to_string()
            } else {
                dir.clone()
            };
            text.push_str(&format!("{} at {place}:\n", info.name));
            for (d, n) in dirs.iter().take(MAX_LIST) {
                let skip = if skipped(&format!("{d}/")) {
                    ", generated or vendored"
                } else {
                    ""
                };
                text.push_str(&format!("{prefix}{d}/  ({n} files{skip})\n"));
            }
            for f in files.iter().take(MAX_LIST) {
                text.push_str(&format!("{}  ({} bytes)\n", f.path, f.size));
            }
        }
        if count > MAX_LIST {
            text.push_str(&format!("(and {} more)\n", count - MAX_LIST));
        }
        if snap.truncated {
            text.push_str("(GitHub listed only part of this very large repository.)\n");
        }
        Ok(Listing {
            repo: info.name.clone(),
            commit: snap.commit.clone(),
            dir,
            text,
            count,
        })
    }

    /// What a repository is and how it is laid out: its top folders (and the
    /// folders under the main ones) with file counts, its languages, its
    /// manifests, and its README and agent notes as read.
    pub async fn overview(&self, repo: &str) -> Result<Overview, ReadError> {
        let info = self.repo(repo)?.clone();
        let snap = self.snapshot(&info).await?;
        let files: Vec<&TreeEntry> = snap.files.iter().filter(|f| !skipped(&f.path)).collect();

        let mut layout = format!("Repository {}", info.name);
        if let Some(role) = info.role.as_deref().filter(|r| !r.is_empty()) {
            layout.push_str(&format!(" (the team's {role} repository)"));
        }
        layout.push_str(&format!(
            "\nDefault branch {} at commit {}. {} files.\n",
            info.branch,
            &snap.commit[..snap.commit.len().min(7)],
            files.len()
        ));
        if let Some(d) = info.description.as_deref().filter(|d| !d.is_empty()) {
            layout.push_str(&format!("Description: {d}\n"));
        }

        // Top folders, and the folders one level under the biggest ones.
        let mut top: BTreeMap<String, usize> = BTreeMap::new();
        let mut second: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
        let mut top_files: Vec<&str> = Vec::new();
        for f in &files {
            let mut parts = f.path.splitn(3, '/');
            let first = parts.next().unwrap_or("");
            match (parts.next(), parts.next()) {
                (None, _) => top_files.push(&f.path),
                (Some(sub), rest) => {
                    *top.entry(first.to_string()).or_default() += 1;
                    if rest.is_some() {
                        *second
                            .entry(first.to_string())
                            .or_default()
                            .entry(sub.to_string())
                            .or_default() += 1;
                    }
                }
            }
        }
        layout.push_str("\nTop of the tree:\n");
        let mut big: Vec<(&String, &usize)> = top.iter().collect();
        big.sort_by(|a, b| b.1.cmp(a.1));
        let expand: Vec<&String> = big.iter().take(6).map(|(d, _)| *d).collect();
        for (d, n) in &top {
            layout.push_str(&format!("  {d}/  ({n} files)\n"));
            if expand.contains(&d) {
                if let Some(subs) = second.get(d) {
                    let mut subs: Vec<(&String, &usize)> = subs.iter().collect();
                    subs.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
                    for (s, m) in subs.iter().take(12) {
                        layout.push_str(&format!("    {d}/{s}/  ({m} files)\n"));
                    }
                    if subs.len() > 12 {
                        layout.push_str(&format!("    (and {} more folders)\n", subs.len() - 12));
                    }
                }
            }
        }
        for f in top_files.iter().take(40) {
            layout.push_str(&format!("  {f}\n"));
        }

        // Languages, by file extension.
        let mut langs: HashMap<&str, usize> = HashMap::new();
        for f in &files {
            if let Some(ext) = f
                .path
                .rsplit('/')
                .next()
                .and_then(|n| n.rsplit_once('.'))
                .map(|(_, e)| e)
            {
                if ext.len() <= 6 {
                    *langs.entry(ext).or_default() += 1;
                }
            }
        }
        let mut langs: Vec<(&str, usize)> = langs.into_iter().collect();
        langs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        if !langs.is_empty() {
            let shown: Vec<String> = langs
                .iter()
                .take(8)
                .map(|(e, n)| format!(".{e} {n}"))
                .collect();
            layout.push_str(&format!("\nFiles by type: {}\n", shown.join(", ")));
        }

        // Manifests deeper down (the packages of a monorepo), by path only.
        let nested: Vec<&str> = files
            .iter()
            .filter(|f| {
                f.path.contains('/')
                    && f.path.matches('/').count() <= 3
                    && MANIFESTS.iter().any(|m| f.path.ends_with(&format!("/{m}")))
            })
            .map(|f| f.path.as_str())
            .take(30)
            .collect();
        if !nested.is_empty() {
            layout.push_str("\nPackages and services (their manifests):\n");
            for p in &nested {
                layout.push_str(&format!("  {p}\n"));
            }
        }

        // README, agent notes and the top-level manifests, read.
        let mut read = Vec::new();
        let mut wanted: Vec<&str> = Vec::new();
        for d in OVERVIEW_DOCS {
            if wanted.iter().any(|w| w.eq_ignore_ascii_case(d)) {
                continue;
            }
            if let Some(f) = files.iter().find(|f| f.path.eq_ignore_ascii_case(d)) {
                wanted.push(&f.path);
            }
        }
        for m in MANIFESTS {
            if files.iter().any(|f| f.path == *m) {
                wanted.push(m);
            }
        }
        for path in wanted.into_iter().take(10) {
            let end = if MANIFESTS.contains(&path) { 120 } else { 160 };
            if let Ok(slice) = self.read(&info.name, path, Some(1), Some(end)).await {
                read.push(slice);
            }
        }
        if read.is_empty() {
            layout.push_str("\nThere is no README or manifest at the top of this repository.\n");
        }
        Ok(Overview {
            repo: info,
            commit: snap.commit.clone(),
            layout,
            files: read,
        })
    }

    /// Code search across `repos` (all the question may read when empty).
    /// When the host's search finds nothing or can't search, file names are
    /// matched instead, and the most likely files are read for the words.
    pub async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, ReadError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(ReadError::NotFound(
                "Give words or a name to search for.".into(),
            ));
        }
        let mut infos: Vec<RepoInfo> = Vec::new();
        for r in repos {
            infos.push(self.repo(r)?.clone());
        }
        if infos.is_empty() {
            infos = self.repos.clone();
        }
        let names: Vec<String> = infos.iter().map(|r| r.name.clone()).collect();
        let searched = self.host.search(&names, query).await;
        if let Ok(hits) = &searched {
            if !hits.is_empty() {
                return Ok(hits.clone());
            }
        }
        // Fallback: paths that contain a word of the query, then their text.
        let words: Vec<String> = query
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .filter(|w| w.len() >= 3)
            .map(|w| w.to_ascii_lowercase())
            .collect();
        let mut out = Vec::new();
        for info in &infos {
            let Ok(snap) = self.snapshot(info).await else {
                continue;
            };
            for f in snap.files.iter().filter(|f| !skipped(&f.path)) {
                let p = f.path.to_ascii_lowercase();
                if words.iter().any(|w| p.contains(w.as_str())) {
                    out.push(SearchHit {
                        repo: info.name.clone(),
                        path: f.path.clone(),
                        fragments: Vec::new(),
                    });
                }
                if out.len() >= 20 {
                    break;
                }
            }
        }
        match (out.is_empty(), searched) {
            (true, Err(e)) => Err(e),
            _ => Ok(out),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::{json as fjson, resp, Fake};
    use serde_json::json;

    fn shop() -> MemHost {
        MemHost::default()
            .with_repo(
                "acme/shop-web",
                "abc1234def",
                &[
                    ("README.md", "# Shop web\nThe storefront.\n"),
                    ("package.json", "{\"name\":\"shop-web\"}"),
                    ("src/app/page.tsx", "export default function Home() {}\n"),
                    (
                        "src/cart/checkout.ts",
                        "line1\nline2\nexport function checkout() {}\nline4\n",
                    ),
                    ("node_modules/x/index.js", "skip"),
                ],
            )
            .with_repo("acme/billing", "fff0000", &[("main.go", "package main\n")])
    }

    fn reader(host: &MemHost) -> CodeReader<'_> {
        CodeReader::new(
            host,
            vec![
                RepoInfo {
                    role: Some("web".into()),
                    ..RepoInfo::new("acme/shop-web", "main")
                },
                RepoInfo::new("acme/billing", "main"),
            ],
        )
    }

    #[test]
    fn names_resolve_however_they_are_written() {
        let host = shop();
        let r = reader(&host);
        assert_eq!(r.resolve("acme/shop-web").unwrap().name, "acme/shop-web");
        assert_eq!(r.resolve("Shop Web").unwrap().name, "acme/shop-web");
        assert_eq!(r.resolve("shop_web").unwrap().name, "acme/shop-web");
        assert_eq!(r.resolve("BILLING").unwrap().name, "acme/billing");
        assert!(r.resolve("payments").is_none());
        assert!(r.resolve("").is_none());
    }

    #[test]
    fn a_question_names_a_repository_by_its_whole_name() {
        let host = shop();
        let r = reader(&host);
        let names = |q: &str| {
            r.mentioned(q)
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names("How is shop-web put together?"), ["acme/shop-web"]);
        assert_eq!(names("what does acme/billing do."), ["acme/billing"]);
        assert_eq!(names("Where is billing."), ["acme/billing"]);
        assert!(names("Where is the billingservice?").is_empty());
        assert!(names("How do refunds work?").is_empty());
    }

    #[tokio::test]
    async fn reads_take_line_ranges_and_say_what_is_left() {
        let host = shop();
        let r = reader(&host);
        let s = r
            .read("shop-web", "src/cart/checkout.ts", Some(2), Some(3))
            .await
            .unwrap();
        assert_eq!((s.start, s.end, s.total_lines), (2, 3, 4));
        assert_eq!(s.text, "line2\nexport function checkout() {}\n");
        let n = s.numbered();
        assert!(n.contains("3 | export function checkout()"), "{n}");
        assert!(n.contains("read from line 4"));
        assert_eq!(
            s.url(),
            "https://github.com/acme/shop-web/blob/abc1234def/src/cart/checkout.ts#L2-L3"
        );
        let whole = r
            .read("acme/shop-web", "./README.md", None, None)
            .await
            .unwrap();
        assert_eq!((whole.start, whole.end), (1, 2));
    }

    #[tokio::test]
    async fn a_missing_file_points_at_what_is_there() {
        let host = shop();
        let r = reader(&host);
        let e = r
            .read("shop-web", "checkout.ts", None, None)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("src/cart/checkout.ts"), "{e}");
        let e = r
            .read("shop-web", "src/cart", None, None)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("is a folder"), "{e}");
        let e = r
            .read("shop-web", "../etc/passwd", None, None)
            .await
            .unwrap_err();
        assert!(matches!(e, ReadError::NotFound(_)));
        let e = r.read("payments", "x", None, None).await.unwrap_err();
        assert!(e.to_string().contains("acme/billing, acme/shop-web"), "{e}");
    }

    #[tokio::test]
    async fn the_overview_shows_the_layout_and_reads_readme_and_manifests() {
        let host = shop();
        let r = reader(&host);
        let o = r.overview("shop-web").await.unwrap();
        assert!(o.layout.contains("the team's web repository"));
        assert!(o.layout.contains("src/  (2 files)"), "{}", o.layout);
        assert!(o.layout.contains("src/cart/"), "{}", o.layout);
        assert!(!o.layout.contains("node_modules"), "{}", o.layout);
        let paths: Vec<&str> = o.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["README.md", "package.json"]);
        assert_eq!(o.commit, "abc1234def");
    }

    #[tokio::test]
    async fn listings_count_files_per_folder_and_match_by_path() {
        let host = shop();
        let r = reader(&host);
        let l = r.list("shop-web", "", "").await.unwrap();
        assert!(l.text.contains("src/  (2 files)"), "{}", l.text);
        let l = r.list("shop-web", "src", "").await.unwrap();
        assert!(l.text.contains("src/cart/  (1 files)"), "{}", l.text);
        let l = r.list("shop-web", "", "checkout").await.unwrap();
        assert!(l.text.contains("src/cart/checkout.ts"));
        assert!(r.list("shop-web", "nope", "").await.is_err());
    }

    #[tokio::test]
    async fn search_falls_back_to_file_names() {
        let host = shop();
        let r = reader(&host);
        let hits = r.search(&[], "export function checkout").await.unwrap();
        assert_eq!(hits[0].path, "src/cart/checkout.ts");
        // No text match: the path match still finds the file.
        let hits = r
            .search(&["shop-web".into()], "checkout flow")
            .await
            .unwrap();
        assert_eq!(hits[0].path, "src/cart/checkout.ts");
        assert!(r.search(&[], "  ").await.is_err());
    }

    #[tokio::test]
    async fn github_reads_are_pinned_to_the_head_commit() {
        let http = Fake::default();
        http.with(|r| {
            r.url
                .ends_with("/repos/acme/app/commits/main")
                .then(|| resp(200, &[], "0123456789abcdef0123456789abcdef01234567"))
        });
        http.with(|r| {
            r.url.contains("/git/trees/0123456789abcdef").then(|| {
                fjson(
                    200,
                    json!({"truncated": false, "tree": [
                        {"type":"blob","path":"README.md","size":10},
                        {"type":"blob","path":"src/main rs.rs","size":5},
                        {"type":"tree","path":"src"}
                    ]}),
                )
            })
        });
        http.with(|r| {
            (r.url
                .contains("/contents/src/main%20rs.rs?ref=0123456789abcdef")
                && r.header_value("accept") == Some("application/vnd.github.raw+json"))
            .then(|| resp(200, &[], "fn main() {}\n"))
        });
        http.with(|r| {
            r.url.contains("/search/code").then(|| {
                fjson(
                    200,
                    json!({"items": [{"path": "src/main rs.rs", "repository": {"full_name": "acme/app"},
                        "text_matches": [{"fragment": "fn main"}]}]}),
                )
            })
        });
        let gh = GitHubHost {
            http: &http,
            auth: "Bearer t".into(),
        };
        let r = CodeReader::new(&gh, vec![RepoInfo::new("acme/app", "main")]);
        let s = r.read("app", "src/main rs.rs", None, None).await.unwrap();
        assert_eq!(s.text, "fn main() {}\n");
        assert!(s.url().contains("/blob/0123456789abcdef"));
        let hits = r.search(&[], "main").await.unwrap();
        assert_eq!(hits[0].fragments, ["fn main"]);
        assert!(http
            .sent()
            .iter()
            .any(|q| q.url.contains("repo%3Aacme%2Fapp")));
        // The tree was fetched once for both reads.
        assert_eq!(
            http.sent()
                .iter()
                .filter(|q| q.url.contains("/git/trees/"))
                .count(),
            1
        );
    }
}
