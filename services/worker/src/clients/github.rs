use base64::{engine::general_purpose::STANDARD as B64, Engine};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use super::repo_snapshot::RepoSnapshot;

const API_BASE: &str = "https://api.github.com";

/// Tarballs larger than this are not snapshotted (Lambda memory guard);
/// search falls back to the GitHub Code Search API for such repos.
const MAX_TARBALL_BYTES: usize = 150 * 1024 * 1024;

/// Cached installation token.
struct CachedToken {
    token: String,
    expires_at: u64,
}

/// GitHub REST API client using installation token auth.
pub struct GitHubClient {
    app_id: String,
    private_key: String,
    installation_id: u64,
    http: Client,
    token_cache: Mutex<Option<CachedToken>>,
    /// Repo snapshots keyed by "owner/repo@ref". `None` marks a failed fetch
    /// so we don't re-download a too-large tarball on every search.
    snapshots: tokio::sync::RwLock<HashMap<String, Option<Arc<RepoSnapshot>>>>,
}

#[derive(Serialize)]
struct JwtClaims {
    iat: u64,
    exp: u64,
    iss: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

impl GitHubClient {
    pub fn new(
        app_id: &str,
        private_key: &str,
        installation_id: u64,
        http: &Client,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            app_id: app_id.to_string(),
            private_key: private_key.to_string(),
            installation_id,
            http: http.clone(),
            token_cache: Mutex::new(None),
            snapshots: tokio::sync::RwLock::new(HashMap::new()),
        })
    }

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn generate_jwt(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let now = Self::now_epoch();
        let claims = JwtClaims {
            iat: now.saturating_sub(60),
            exp: now + 600,
            iss: self.app_id.clone(),
        };
        let key = EncodingKey::from_rsa_pem(self.private_key.as_bytes())?;
        let token = encode(&Header::new(Algorithm::RS256), &claims, &key)?;
        Ok(token)
    }

    async fn get_installation_token(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Check cache
        {
            let cache = self.token_cache.lock().unwrap();
            if let Some(ref cached) = *cache {
                if Self::now_epoch() < cached.expires_at.saturating_sub(300) {
                    return Ok(cached.token.clone());
                }
            }
        }

        let jwt = self.generate_jwt()?;
        let resp: TokenResponse = self
            .http
            .post(format!(
                "{API_BASE}/app/installations/{}/access_tokens",
                self.installation_id
            ))
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut cache = self.token_cache.lock().unwrap();
        *cache = Some(CachedToken {
            token: resp.token.clone(),
            expires_at: Self::now_epoch() + 3600,
        });

        Ok(resp.token)
    }

    async fn auth_headers(
        &self,
    ) -> Result<
        Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let token = self.get_installation_token().await?;
        Ok(vec![
            (
                reqwest::header::AUTHORIZATION,
                format!("token {token}").parse()?,
            ),
            (
                reqwest::header::ACCEPT,
                "application/vnd.github+json".parse()?,
            ),
        ])
    }

    async fn get(
        &self,
        url: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    async fn post(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    async fn patch(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let headers = self.auth_headers().await?;
        let mut req = self.http.patch(url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    async fn put(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let headers = self.auth_headers().await?;
        let mut req = self.http.put(url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    // ─── Repo snapshot (tarball-backed local search + reads) ────

    /// Download the repo tarball for a ref. One core-API request; the 302
    /// redirect to codeload is followed automatically by reqwest.
    async fn download_tarball(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/tarball/{git_ref}");
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        let bytes = resp.bytes().await?;
        if bytes.len() > MAX_TARBALL_BYTES {
            return Err(format!(
                "tarball too large to snapshot ({} MB)",
                bytes.len() / (1024 * 1024)
            )
            .into());
        }
        Ok(bytes.to_vec())
    }

    fn snapshot_key(owner: &str, repo: &str, git_ref: &str) -> String {
        format!("{owner}/{repo}@{git_ref}")
    }

    /// Get or lazily build the in-memory snapshot for a repo@ref.
    /// Returns None when a previous fetch failed (too large / API error) so
    /// callers fall back to per-request API access.
    async fn snapshot(&self, owner: &str, repo: &str, git_ref: &str) -> Option<Arc<RepoSnapshot>> {
        let key = Self::snapshot_key(owner, repo, git_ref);
        if let Some(entry) = self.snapshots.read().await.get(&key) {
            return entry.clone();
        }
        let built = match self.download_tarball(owner, repo, git_ref).await {
            Ok(bytes) => match RepoSnapshot::from_tarball(&bytes) {
                Ok(snap) => Some(Arc::new(snap)),
                Err(e) => {
                    tracing::warn!(key, error = %e, "Failed to parse repo tarball; falling back to API");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(key, error = %e, "Failed to download repo tarball; falling back to API");
                None
            }
        };
        self.snapshots.write().await.insert(key, built.clone());
        built
    }

    /// Snapshot for a ref if one was already built (never triggers a download).
    async fn existing_snapshot(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Option<Arc<RepoSnapshot>> {
        self.snapshots
            .read()
            .await
            .get(&Self::snapshot_key(owner, repo, git_ref))
            .cloned()
            .flatten()
    }

    /// Mirror a committed write into any snapshot held for this ref.
    async fn mirror_write(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
        path: &str,
        content: &str,
    ) {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            snap.apply_write(path, content).await;
        }
    }

    /// Mirror a committed delete into any snapshot held for this ref.
    async fn mirror_delete(&self, owner: &str, repo: &str, git_ref: &str, path: &str) {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            snap.apply_delete(path).await;
        }
    }

    /// Drop a cached snapshot after a server-side mutation we can't mirror
    /// (merges). The next search re-downloads the tarball at the new head.
    async fn invalidate_snapshot(&self, owner: &str, repo: &str, git_ref: &str) {
        self.snapshots
            .write()
            .await
            .remove(&Self::snapshot_key(owner, repo, git_ref));
    }

    // ─── Tree / File reads ──────────────────────────────────────

    /// Get the repository's default branch name.
    pub async fn get_default_branch(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}");
        let data = self.get(&url).await?;
        let branch = data
            .get("default_branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main")
            .to_string();
        Ok(branch)
    }

    /// Get the repository's primary language and description from GitHub.
    pub async fn get_repo_info(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<(Option<String>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}");
        let data = self.get(&url).await?;
        let language = data
            .get("language")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let description = data
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok((language, description))
    }

    /// Get the full recursive file tree.
    pub async fn get_tree(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            return Ok(snap
                .tree()
                .await
                .into_iter()
                .map(|path| TreeEntry {
                    path,
                    entry_type: "blob".to_string(),
                    sha: String::new(),
                })
                .collect());
        }
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees/{git_ref}?recursive=1");
        let data = self.get(&url).await?;
        let tree: Vec<TreeEntry> =
            serde_json::from_value(data.get("tree").cloned().unwrap_or(serde_json::json!([])))?;
        Ok(tree)
    }

    /// Read a single file (base64 decoded). Served from the repo snapshot
    /// when one is held for this ref; API otherwise (and for binary or
    /// oversized files the snapshot doesn't index).
    pub async fn read_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            if let Some(content) = snap.read_file(path).await {
                return Ok(content);
            }
        }
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        let data = self.get(&url).await?;
        let content = data.get("content").and_then(|c| c.as_str()).unwrap_or("");
        let clean = content.replace('\n', "");
        let bytes = B64.decode(&clean)?;
        Ok(String::from_utf8(bytes)?)
    }

    /// Get the SHA of a file at a given ref (for update operations).
    pub async fn get_file_sha(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        let data = self.get(&url).await?;
        data.get("sha")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| "No SHA in response".into())
    }

    /// Read specific line range from a file (1-indexed, inclusive).
    pub async fn read_file_lines(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        start_line: usize,
        end_line: usize,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let full = self.read_file(owner, repo, path, git_ref).await?;
        let lines: Vec<&str> = full.lines().collect();
        let start = start_line.saturating_sub(1).min(lines.len());
        let end = end_line.min(lines.len());
        let mut result = String::new();
        for (i, line) in lines[start..end].iter().enumerate() {
            result.push_str(&format!("{:>4} | {}\n", start + i + 1, line));
        }
        if end < lines.len() {
            result.push_str(&format!("... ({} more lines)\n", lines.len() - end));
        }
        Ok(result)
    }

    /// Search code in a repository. Backed by an in-memory snapshot of the
    /// repo at `git_ref` (built from one tarball download on first search):
    /// no code-search rate limit, and results reflect the working branch
    /// including the agent's own commits — the Code Search API only indexes
    /// the default branch and lags pushes. Falls back to the API when the
    /// repo is too large to snapshot.
    pub async fn search_code(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
        query: &str,
    ) -> Result<Vec<SearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(snap) = self.snapshot(owner, repo, git_ref).await {
            return Ok(snap
                .search(query)
                .await
                .into_iter()
                .map(|m| SearchResult {
                    path: m.path,
                    matches: m.fragments,
                })
                .collect());
        }
        self.search_code_api(owner, repo, query).await
    }

    /// Legacy Code Search API path — default-branch only, 10 requests/min.
    async fn search_code_api(
        &self,
        owner: &str,
        repo: &str,
        query: &str,
    ) -> Result<Vec<SearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        let raw_query = format!("{query} repo:{owner}/{repo}");
        let encoded_query = urlencoding::encode(&raw_query);
        let url = format!("{API_BASE}/search/code?q={encoded_query}&per_page=10");
        // Use text-match+json accept header to get matching fragments
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            if k == reqwest::header::ACCEPT {
                req = req.header(k, "application/vnd.github.text-match+json");
            } else {
                req = req.header(k, v);
            }
        }
        let resp = req.send().await?.error_for_status()?;
        let data: serde_json::Value = resp.json().await?;
        let items = data
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let results: Vec<SearchResult> = items
            .iter()
            .filter_map(|item| {
                let path = item.get("path")?.as_str()?.to_string();
                let matches: Vec<String> = item
                    .get("text_matches")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|m| m.get("fragment").and_then(|f| f.as_str()))
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                Some(SearchResult { path, matches })
            })
            .collect();
        Ok(results)
    }

    /// List directory contents.
    pub async fn list_directory(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<Vec<DirEntry>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            if let Some(entries) = snap.list_directory(path).await {
                return Ok(entries
                    .into_iter()
                    .filter_map(|line| {
                        let (entry_type, name) = line.split_once(": ")?;
                        let full_path = if path.is_empty() || path == "." || path == "/" {
                            name.to_string()
                        } else {
                            format!("{}/{}", path.trim_matches('/'), name)
                        };
                        Some(DirEntry {
                            name: name.to_string(),
                            entry_type: entry_type.to_string(),
                            path: full_path,
                        })
                    })
                    .collect());
            }
        }
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        let data = self.get(&url).await?;
        let entries: Vec<DirEntry> = serde_json::from_value(data)?;
        Ok(entries)
    }

    // ─── Branch operations ──────────────────────────────────────

    /// Get SHA for a branch.
    pub async fn get_ref(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/ref/heads/{branch}");
        let data = self.get(&url).await?;
        let sha = data
            .pointer("/object/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in ref response")?;
        Ok(sha.to_string())
    }

    /// Create a new branch from an existing ref.
    pub async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let sha = self.get_ref(owner, repo, from_ref).await?;
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs");
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(&url).json(&serde_json::json!({
            "ref": format!("refs/heads/{branch}"),
            "sha": sha,
        }));
        for (k, v) in &headers {
            req = req.header(k.clone(), v.clone());
        }
        let resp = req.send().await?;

        if resp.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            // Branch already exists — reset it to the target ref
            let update_url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
            let data = self
                .patch(
                    &update_url,
                    &serde_json::json!({ "sha": sha, "force": true }),
                )
                .await?;
            let new_sha = data
                .pointer("/object/sha")
                .and_then(|v| v.as_str())
                .ok_or("Missing sha in update ref response")?;
            return Ok(new_sha.to_string());
        }

        let data: serde_json::Value = resp.error_for_status()?.json().await?;
        let new_sha = data
            .pointer("/object/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in create branch response")?;
        Ok(new_sha.to_string())
    }

    // ─── Merge ───────────────────────────────────────────────────

    /// Merge one branch into another via the GitHub Merges API.
    /// Returns Ok(true) if merge succeeded or was already up-to-date,
    /// Ok(false) if there are merge conflicts (HTTP 409).
    pub async fn merge_branch(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/merges");
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(&url).json(&serde_json::json!({
            "base": base,
            "head": head,
            "commit_message": format!("Merge {head} into {base}"),
        }));
        for (k, v) in &headers {
            req = req.header(k.clone(), v.clone());
        }
        let resp = req.send().await?;
        match resp.status().as_u16() {
            201 => {
                self.invalidate_snapshot(owner, repo, base).await;
                Ok(true) // merged
            }
            204 => Ok(true),  // already up-to-date
            409 => Ok(false), // conflict
            _ => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Err(format!("merge_branch failed: {status} {body}").into())
            }
        }
    }

    // ─── Single file write ──────────────────────────────────────

    /// Create or update a single file via Contents API.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        content: &str,
        branch: &str,
        message: &str,
        sha: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}");
        let encoded = B64.encode(content.as_bytes());
        let mut body = serde_json::json!({
            "message": message,
            "content": encoded,
            "branch": branch,
        });
        if let Some(s) = sha {
            body["sha"] = serde_json::json!(s);
        }
        let result = self.put(&url, &body).await?;
        self.mirror_write(owner, repo, branch, path, content).await;
        Ok(result)
    }

    // ─── Batch write (atomic multi-file commit) ─────────────────

    /// Atomic multi-file commit via git trees API.
    pub async fn batch_write(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        message: &str,
        files: &[FileOp],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let branch_sha = self.get_ref(owner, repo, branch).await?;

        let mut tree_entries = Vec::new();
        for f in files {
            match f {
                FileOp::Write { path, content } => {
                    // Create blob
                    let blob_url = format!("{API_BASE}/repos/{owner}/{repo}/git/blobs");
                    let blob = self
                        .post(
                            &blob_url,
                            &serde_json::json!({
                                "content": content,
                                "encoding": "utf-8",
                            }),
                        )
                        .await?;
                    let blob_sha = blob
                        .get("sha")
                        .and_then(|v| v.as_str())
                        .ok_or("Missing blob sha")?;
                    tree_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_sha,
                    }));
                }
                FileOp::Delete { path } => {
                    tree_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": null,
                    }));
                }
            }
        }

        // Create tree
        let tree_url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees");
        let tree = self
            .post(
                &tree_url,
                &serde_json::json!({
                    "base_tree": branch_sha,
                    "tree": tree_entries,
                }),
            )
            .await?;
        let tree_sha = tree
            .get("sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing tree sha")?;

        // Create commit
        let commit_url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits");
        let commit = self
            .post(
                &commit_url,
                &serde_json::json!({
                    "message": message,
                    "tree": tree_sha,
                    "parents": [branch_sha],
                }),
            )
            .await?;
        let commit_sha = commit
            .get("sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing commit sha")?
            .to_string();

        // Update branch ref
        let ref_url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
        self.patch(&ref_url, &serde_json::json!({"sha": &commit_sha}))
            .await?;

        for f in files {
            match f {
                FileOp::Write { path, content } => {
                    self.mirror_write(owner, repo, branch, path, content).await;
                }
                FileOp::Delete { path } => {
                    self.mirror_delete(owner, repo, branch, path).await;
                }
            }
        }

        Ok(commit_sha)
    }

    /// Create a merge commit with two parents (for conflict resolution).
    /// Builds a tree from the branch tree + overridden files, then creates
    /// a commit with both branch and base as parents.
    pub async fn create_merge_commit(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        branch_sha: &str,
        base_sha: &str,
        message: &str,
        resolved_files: &[FileOp],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Get the branch commit's tree as the base tree
        let commit_url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits/{branch_sha}");
        let commit_data = self.get(&commit_url).await?;
        let base_tree = commit_data
            .pointer("/tree/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing tree sha in branch commit")?;

        let mut tree_entries = Vec::new();
        for f in resolved_files {
            match f {
                FileOp::Write { path, content } => {
                    let blob_url = format!("{API_BASE}/repos/{owner}/{repo}/git/blobs");
                    let blob = self
                        .post(
                            &blob_url,
                            &serde_json::json!({
                                "content": content,
                                "encoding": "utf-8",
                            }),
                        )
                        .await?;
                    let blob_sha = blob
                        .get("sha")
                        .and_then(|v| v.as_str())
                        .ok_or("Missing blob sha")?;
                    tree_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_sha,
                    }));
                }
                FileOp::Delete { path } => {
                    tree_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": null,
                    }));
                }
            }
        }

        // Create tree with resolved files overlaid on branch tree
        let tree_url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees");
        let tree = self
            .post(
                &tree_url,
                &serde_json::json!({
                    "base_tree": base_tree,
                    "tree": tree_entries,
                }),
            )
            .await?;
        let tree_sha = tree
            .get("sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing tree sha")?;

        // Create merge commit with TWO parents
        let new_commit_url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits");
        let commit = self
            .post(
                &new_commit_url,
                &serde_json::json!({
                    "message": message,
                    "tree": tree_sha,
                    "parents": [branch_sha, base_sha],
                }),
            )
            .await?;
        let commit_sha = commit
            .get("sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing commit sha")?
            .to_string();

        // Update branch ref to point to the merge commit
        let ref_url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
        self.patch(&ref_url, &serde_json::json!({"sha": &commit_sha}))
            .await?;

        // Merge commit pulls in base content we can't mirror file-by-file.
        self.invalidate_snapshot(owner, repo, branch).await;

        Ok(commit_sha)
    }

    // ─── Diff ───────────────────────────────────────────────────

    /// Compare two refs.
    pub async fn get_diff(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}");
        self.get(&url).await
    }

    // ─── Issues ─────────────────────────────────────────────────

    /// Create a comment on an issue.
    pub async fn create_issue_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        body: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/comments");
        self.post(&url, &serde_json::json!({"body": body})).await
    }

    /// Edit an existing issue comment by comment ID.
    pub async fn edit_issue_comment(
        &self,
        owner: &str,
        repo: &str,
        comment_id: u64,
        body: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues/comments/{comment_id}");
        self.patch(&url, &serde_json::json!({"body": body})).await
    }

    // ─── Pull requests ─────────────────────────────────────────

    /// Get a single pull request.
    pub async fn get_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.get(&url).await
    }

    /// Create a pull request.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_pull_request(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
        draft: bool,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls");
        self.post(
            &url,
            &serde_json::json!({
                "title": title,
                "body": body,
                "head": head,
                "base": base,
                "draft": draft,
            }),
        )
        .await
    }

    /// Close a pull request.
    pub async fn close_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.patch(&url, &serde_json::json!({"state": "closed"}))
            .await
    }

    /// Mark a draft PR as ready for review using the GraphQL API.
    /// The REST `PATCH {"draft": false}` does NOT work — GitHub requires the
    /// `markPullRequestAsReady` GraphQL mutation.
    pub async fn mark_pr_ready(
        &self,
        node_id: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let query = serde_json::json!({
            "query": "mutation($id: ID!) { markPullRequestReadyForReview(input: { pullRequestId: $id }) { pullRequest { isDraft } } }",
            "variables": { "id": node_id }
        });
        let url = "https://api.github.com/graphql";
        let resp = self.post(url, &query).await?;
        if let Some(errors) = resp.get("errors") {
            return Err(format!("GraphQL errors: {}", errors).into());
        }
        Ok(resp)
    }

    /// Get review comments on a PR.
    pub async fn get_review_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/comments");
        let data = self.get(&url).await?;
        let comments: Vec<serde_json::Value> = serde_json::from_value(data)?;
        Ok(comments)
    }

    /// Reply to a review comment thread.
    pub async fn reply_to_review_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        comment_id: u64,
        body: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/comments/{comment_id}/replies"
        );
        self.post(&url, &serde_json::json!({"body": body})).await
    }

    /// Resolve a PR review thread by its GraphQL node ID.
    pub async fn resolve_review_thread(
        &self,
        thread_id: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let query = serde_json::json!({
            "query": "mutation($id: ID!) { resolveReviewThread(input: { threadId: $id }) { thread { isResolved } } }",
            "variables": { "id": thread_id }
        });
        self.post("https://api.github.com/graphql", &query).await
    }

    /// Get review thread IDs for specific comment node IDs.
    /// Queries the PR's review threads, builds a comment→thread map,
    /// and returns only entries matching the given comment_node_ids.
    pub async fn get_review_thread_ids(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        comment_node_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>>
    {
        let wanted: std::collections::HashSet<&str> = comment_node_ids.iter().copied().collect();
        let mut map = std::collections::HashMap::new();
        let mut cursor: Option<String> = None;

        loop {
            let after = cursor
                .as_deref()
                .map(|c| format!(", after: \"{}\"", c))
                .unwrap_or_default();
            let query_str = format!(
                r#"query {{ repository(owner: "{owner}", name: "{repo}") {{ pullRequest(number: {pr_number}) {{ reviewThreads(first: 50{after}) {{ pageInfo {{ hasNextPage endCursor }} nodes {{ id comments(first: 10) {{ nodes {{ id }} }} }} }} }} }} }}"#,
            );
            let resp = self
                .post(
                    "https://api.github.com/graphql",
                    &serde_json::json!({"query": query_str}),
                )
                .await?;

            let threads = resp
                .pointer("/data/repository/pullRequest/reviewThreads/nodes")
                .and_then(|v| v.as_array());

            if let Some(nodes) = threads {
                for thread in nodes {
                    let thread_id = thread["id"].as_str().unwrap_or("");
                    if let Some(comments) =
                        thread.pointer("/comments/nodes").and_then(|v| v.as_array())
                    {
                        for comment in comments {
                            if let Some(cid) = comment["id"].as_str() {
                                if wanted.contains(cid) {
                                    map.insert(cid.to_string(), thread_id.to_string());
                                }
                            }
                        }
                    }
                }
            }

            let has_next = resp
                .pointer("/data/repository/pullRequest/reviewThreads/pageInfo/hasNextPage")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if has_next {
                cursor = resp
                    .pointer("/data/repository/pullRequest/reviewThreads/pageInfo/endCursor")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(map)
    }

    // ─── Check runs ─────────────────────────────────────────────

    /// Download check run / action job logs.
    pub async fn get_check_run_logs(
        &self,
        owner: &str,
        repo: &str,
        job_id: u64,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/actions/jobs/{job_id}/logs");
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        Ok(resp.text().await?)
    }

    /// Download logs for a workflow run. GitHub returns a zip; we extract the first
    /// failed job's log text (or concat all logs if no clear failure).
    pub async fn get_workflow_run_logs(
        &self,
        owner: &str,
        repo: &str,
        run_id: u64,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // First, list jobs for this run and find the failed one(s)
        let jobs_url =
            format!("{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs?filter=latest");
        let jobs: serde_json::Value = self.get(&jobs_url).await?;
        let failed_jobs: Vec<u64> = jobs["jobs"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter(|j| j["conclusion"].as_str() == Some("failure"))
            .filter_map(|j| j["id"].as_u64())
            .collect();

        if failed_jobs.is_empty() {
            return Ok("(no failed jobs found)".to_string());
        }

        // Download logs for each failed job (typically just 1)
        let mut all_logs = String::new();
        for job_id in failed_jobs.iter().take(3) {
            match self.get_check_run_logs(owner, repo, *job_id).await {
                Ok(log) => {
                    if !all_logs.is_empty() {
                        all_logs.push_str("\n---\n");
                    }
                    all_logs.push_str(&log);
                }
                Err(e) => {
                    all_logs.push_str(&format!("(failed to download job {job_id} logs: {e})\n"));
                }
            }
        }

        Ok(all_logs)
    }

    /// List check runs for a git ref (branch name or SHA).
    pub async fn list_check_runs_for_ref(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url =
            format!("{API_BASE}/repos/{owner}/{repo}/commits/{git_ref}/check-runs?per_page=100");
        self.get(&url).await
    }

    /// Fetch annotations from failed check runs on a branch — useful as fallback
    /// when workflow logs are inaccessible (403). Annotations contain lint errors,
    /// test failures, etc. with file paths and line numbers.
    pub async fn get_check_run_annotations(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let checks = self.list_check_runs_for_ref(owner, repo, branch).await?;
        let mut all_annotations = String::new();

        for check in checks["check_runs"].as_array().unwrap_or(&vec![]) {
            let conclusion = check["conclusion"].as_str().unwrap_or("");
            if conclusion != "failure" {
                continue;
            }
            let check_name = check["name"].as_str().unwrap_or("unknown");
            let check_id = check["id"].as_u64().unwrap_or(0);
            if check_id == 0 {
                continue;
            }

            // Fetch annotations for this check run
            let url = format!(
                "{API_BASE}/repos/{owner}/{repo}/check-runs/{check_id}/annotations?per_page=50"
            );
            match self.get(&url).await {
                Ok(annotations) => {
                    let ann_array = annotations.as_array().cloned().unwrap_or_default();
                    if ann_array.is_empty() {
                        continue;
                    }
                    all_annotations.push_str(&format!("\n--- {check_name} ---\n"));
                    for ann in &ann_array {
                        let path = ann["path"].as_str().unwrap_or("");
                        let start_line = ann["start_line"].as_u64().unwrap_or(0);
                        let msg = ann["message"].as_str().unwrap_or("");
                        let level = ann["annotation_level"].as_str().unwrap_or("error");
                        all_annotations.push_str(&format!("{level}: {path}:{start_line}: {msg}\n"));
                    }
                }
                Err(_) => continue,
            }
        }

        // Also grab the output summary/text from failed checks (often has error details)
        for check in checks["check_runs"].as_array().unwrap_or(&vec![]) {
            let conclusion = check["conclusion"].as_str().unwrap_or("");
            if conclusion != "failure" {
                continue;
            }
            let check_name = check["name"].as_str().unwrap_or("unknown");
            if let Some(output) = check.get("output") {
                let summary = output["summary"].as_str().unwrap_or("");
                let text = output["text"].as_str().unwrap_or("");
                if !summary.is_empty() || !text.is_empty() {
                    all_annotations.push_str(&format!("\n--- {check_name} output ---\n"));
                    if !summary.is_empty() {
                        all_annotations.push_str(&format!("{summary}\n"));
                    }
                    if !text.is_empty() {
                        let truncated = if text.len() > 3000 {
                            &text[..3000]
                        } else {
                            text
                        };
                        all_annotations.push_str(&format!("{truncated}\n"));
                    }
                }
            }
        }

        Ok(all_annotations)
    }

    /// List pull requests (state: "open", "closed", "all").
    pub async fn list_pull_requests(
        &self,
        owner: &str,
        repo: &str,
        state: &str,
        per_page: u32,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls?state={state}&per_page={per_page}&sort=updated&direction=desc"
        );
        self.get(&url).await
    }

    /// Find an open PR for a specific head branch. Returns None if no open PR exists.
    pub async fn find_open_pr_for_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls?state=open&head={owner}:{branch}&per_page=1"
        );
        let data = self.get(&url).await?;
        Ok(data.as_array().and_then(|arr| arr.first().cloned()))
    }

    /// Find the most recent closed-but-unmerged PR for a head branch.
    /// A branch force-reset to base auto-closes its PR ("all commits
    /// removed"); reopening that PR preserves its number, comments, and
    /// review history instead of churning out a new PR per re-run.
    pub async fn find_reopenable_pr_for_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls?state=closed&head={owner}:{branch}&sort=created&direction=desc&per_page=5"
        );
        let data = self.get(&url).await?;
        Ok(data
            .as_array()
            .and_then(|arr| arr.iter().find(|pr| pr["merged_at"].is_null()).cloned()))
    }

    /// Reopen a closed pull request.
    pub async fn reopen_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.patch(&url, &serde_json::json!({"state": "open"}))
            .await?;
        Ok(())
    }

    /// List recent commits on a branch.
    pub async fn list_commits(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        per_page: u32,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url =
            format!("{API_BASE}/repos/{owner}/{repo}/commits?sha={branch}&per_page={per_page}");
        self.get(&url).await
    }

    /// Get file content from the default branch (HEAD).
    pub async fn get_file_content(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.read_file(owner, repo, path, "HEAD").await
    }

    /// Create a GitHub issue and return (issue_number, issue_url).
    pub async fn create_issue(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
    ) -> Result<(u64, String), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues");
        let payload = serde_json::json!({ "title": title, "body": body });
        let data = self.post(&url, &payload).await?;
        let number = data["number"].as_u64().unwrap_or(0);
        let html_url = data["html_url"].as_str().unwrap_or("").to_string();
        Ok((number, html_url))
    }

    /// Ensure a label exists in the repo, creating it if missing.
    pub async fn ensure_label(
        &self,
        owner: &str,
        repo: &str,
        label: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/labels/{label}");
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(());
        }
        // Label doesn't exist — create it
        let create_url = format!("{API_BASE}/repos/{owner}/{repo}/labels");
        let payload = serde_json::json!({ "name": label, "color": "7B61FF", "description": "Managed by Coderhelm" });
        self.post(&create_url, &payload).await?;
        Ok(())
    }

    /// Add a label to a GitHub issue.
    pub async fn add_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/labels");
        let payload = serde_json::json!({ "labels": [label] });
        self.post(&url, &payload).await?;
        Ok(())
    }
}

// ─── Types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub sha: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DirEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub path: String,
}

/// File operation for batch_write.
pub enum FileOp {
    Write { path: String, content: String },
    Delete { path: String },
}

/// Code search result.
pub struct SearchResult {
    pub path: String,
    pub matches: Vec<String>,
}
