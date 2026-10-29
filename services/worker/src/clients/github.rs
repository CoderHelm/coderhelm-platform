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

/// A repo snapshot plus the commit SHA it reflects. `base_sha` starts as the
/// branch tip the tarball was downloaded at and advances as the platform's own
/// commits are mirrored in — so at any moment it names exactly the last commit
/// this pass has "seen". Write paths compare the live tip against it to detect
/// foreign commits (e.g. a user's local push) that the snapshot never saw:
/// writing snapshot-era file bodies over those would silently revert them.
#[derive(Clone)]
struct CachedSnapshot {
    snap: Arc<RepoSnapshot>,
    base_sha: String,
}

/// A single inline PR review comment. `line` is the (1-indexed) line in the PR
/// diff the comment anchors to; for a multi-line range, set `start_line` (&lt; line).
/// `side` is "RIGHT" (added/context) or "LEFT" (deleted). The anchored line(s)
/// must be part of the diff.
#[derive(Debug, Clone)]
pub struct InlineComment {
    pub path: String,
    pub line: u64,
    pub start_line: Option<u64>,
    pub side: String,
    pub body: String,
}

/// Turn a non-2xx response into an error that CARRIES GitHub's response body.
/// `error_for_status()` discarded it, so a 422 surfaced only as "422
/// Unprocessable Entity" with no hint of which field GitHub rejected.
/// The message keeps the status line, so existing status checks still match.
async fn ok_or_body(
    resp: reqwest::Response,
) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let url = resp.url().clone();
    let body = resp.text().await.unwrap_or_default();
    Err(status_error_message(status, url.as_str(), &body).into())
}

fn status_error_message(status: reqwest::StatusCode, url: &str, body: &str) -> String {
    let kind = if status.is_client_error() {
        "client"
    } else {
        "server"
    };
    let body = common::truncate_str(body.trim(), 2000);
    if body.is_empty() {
        format!("HTTP status {kind} error ({status}) for url ({url})")
    } else {
        format!("HTTP status {kind} error ({status}) for url ({url}): {body}")
    }
}

/// Did this request fail because the thing does not exist? Matches the
/// status line `status_error_message` writes.
pub fn is_not_found(e: &(dyn std::error::Error + Send + Sync)) -> bool {
    e.to_string().contains("(404 Not Found)")
}

/// What a contents-API response says about a file's body.
#[derive(Debug, PartialEq)]
enum ContentsBody {
    /// The body, decoded.
    Text(String),
    /// Too large for the contents API (over 1 MB it sends `encoding: none`
    /// and no content): read the git blob with this sha instead.
    Blob(String),
}

/// Decode a contents-API response for `path`. A file whose `size` says it
/// has bytes never decodes to an empty string: it is either sent to the blob
/// API or reported as an error.
fn contents_body(path: &str, data: &serde_json::Value) -> Result<ContentsBody, String> {
    if data.is_array() {
        return Err(format!("'{path}' is a directory, not a file"));
    }
    let size = data.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
    let encoding = data.get("encoding").and_then(|e| e.as_str()).unwrap_or("");
    let content = data.get("content").and_then(|c| c.as_str()).unwrap_or("");
    let clean = content.replace(['\n', '\r'], "");
    if size > 0 && (clean.is_empty() || encoding == "none") {
        let sha = data
            .get("sha")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                format!(
                    "'{path}' is {size} bytes but GitHub sent neither its content nor a blob sha"
                )
            })?;
        return Ok(ContentsBody::Blob(sha.to_string()));
    }
    Ok(ContentsBody::Text(decode_base64_text(path, &clean)?))
}

/// Decode a git-blob response for `path`. Errors rather than returning an
/// empty string for a blob that has bytes.
fn blob_text(path: &str, data: &serde_json::Value) -> Result<String, String> {
    let size = data.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
    let content = data.get("content").and_then(|c| c.as_str()).unwrap_or("");
    let text = match data.get("encoding").and_then(|e| e.as_str()) {
        Some("utf-8") => content.to_string(),
        _ => decode_base64_text(path, &content.replace(['\n', '\r'], ""))?,
    };
    if text.is_empty() && size > 0 {
        return Err(format!(
            "'{path}' is {size} bytes but GitHub returned no content for it, even from the blob API"
        ));
    }
    Ok(text)
}

fn decode_base64_text(path: &str, clean: &str) -> Result<String, String> {
    let bytes = B64
        .decode(clean)
        .map_err(|e| format!("'{path}': content is not valid base64 ({e})"))?;
    String::from_utf8(bytes).map_err(|_| format!("'{path}' is not UTF-8 text"))
}

/// The refusal for writing `content` over a file of `existing_size` bytes
/// (None = the file does not exist), or None when the write is allowed.
/// Blank content over a file with bytes is refused: removal must be explicit.
fn empty_overwrite_refusal(
    path: &str,
    content: &str,
    existing_size: Option<u64>,
) -> Option<String> {
    let existing = existing_size.unwrap_or(0);
    (content.trim().is_empty() && existing > 0).then(|| {
        format!(
            "refused: this write would replace '{path}' ({existing} bytes) with empty content. \
             That usually means the source read came back empty, not that the file should be emptied. \
             Re-read the file; if it really should go, delete it explicitly instead of writing nothing."
        )
    })
}

/// What [`GitHubClient::start_branch`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchStart {
    /// The branch didn't exist; it starts at the base.
    Created,
    /// A leftover branch with no open pull request was moved to the base.
    Reset,
    /// The branch has an open pull request: kept at its head.
    KeptOpenPr,
}

impl BranchStart {
    /// The progress note for `branch` started from `base`.
    pub fn note(self, branch: &str, base: &str) -> String {
        match self {
            BranchStart::Created => format!("Created branch {branch} from {base}"),
            BranchStart::Reset => format!(
                "Reset branch {branch} to {base} (a leftover branch with no open pull request)"
            ),
            BranchStart::KeptOpenPr => format!(
                "Branch {branch} has an open pull request: continuing from its head, nothing reset"
            ),
        }
    }
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
    snapshots: tokio::sync::RwLock<HashMap<String, Option<CachedSnapshot>>>,
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

/// True when the issue/PR was closed by a bot account (the app's own push),
/// false when a person closed it or the closer is unknown.
pub fn closed_by_a_bot(issue: &serde_json::Value) -> bool {
    issue["closed_by"]["type"].as_str() == Some("Bot")
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

    /// An installation token (cached until shortly before it expires), for
    /// callers that make their own GitHub requests (knowledge sync).
    pub async fn installation_token(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.get_installation_token().await
    }

    async fn get_installation_token(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // A hybrid install without its own app: CoderHelm's broker (it caches).
        if common::hybrid::client::github_via_broker(&self.private_key) {
            return common::hybrid::client::github_token(&self.http, self.installation_id)
                .await
                .map_err(|e| e.to_string().into());
        }
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
        let mut req = self
            .http
            .post(format!(
                "{API_BASE}/app/installations/{}/access_tokens",
                self.installation_id
            ))
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        // Pin the token format (stateless JWT-style by default) instead of
        // following GitHub's rollout; tokens are handled as opaque strings.
        if let Some(mode) = common::stateless_s2s_token_mode() {
            req = req.header(common::STATELESS_S2S_TOKEN_HEADER, mode);
        }
        let resp: TokenResponse = req.send().await?.error_for_status()?.json().await?;

        let mut cache = self.token_cache.lock().unwrap();
        *cache = Some(CachedToken {
            token: resp.token.clone(),
            expires_at: Self::now_epoch() + 3600,
        });

        Ok(resp.token)
    }

    /// The GitHub login of this app's bot user ("{slug}[bot]") — the identity
    /// stamped as committer on every commit the platform creates via the API.
    /// Used to distinguish our own commits from foreign (user) commits.
    async fn app_bot_login(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        if common::hybrid::client::github_via_broker(&self.private_key) {
            self.get_installation_token().await?;
            return common::hybrid::client::bot_login()
                .ok_or_else(|| "the broker named no bot".into());
        }
        let jwt = self.generate_jwt()?;
        let resp: serde_json::Value = self
            .http
            .get(format!("{API_BASE}/app"))
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "coderhelm-worker")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let slug = resp
            .get("slug")
            .and_then(|v| v.as_str())
            .ok_or("Missing slug in GET /app response")?;
        Ok(format!("{slug}[bot]"))
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
        let resp = ok_or_body(req.send().await?).await?;
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
        let resp = ok_or_body(req.send().await?).await?;
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
        let resp = ok_or_body(req.send().await?).await?;
        Ok(resp.json().await?)
    }

    // ─── Repo snapshot (tarball-backed local search + reads) ────

    /// Download the repo tarball for a ref. One core-API request; the 302
    /// redirect to codeload is followed automatically by reqwest.
    pub async fn download_tarball(
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
    ///
    /// The branch ref is resolved to a commit SHA FIRST and the tarball is
    /// downloaded by that SHA (immutable), so the snapshot is stamped with
    /// exactly the commit it contains — no resolve/download race.
    async fn snapshot(&self, owner: &str, repo: &str, git_ref: &str) -> Option<Arc<RepoSnapshot>> {
        let key = Self::snapshot_key(owner, repo, git_ref);
        if let Some(entry) = self.snapshots.read().await.get(&key) {
            return entry.clone().map(|c| c.snap);
        }
        let built = match self.get_ref(owner, repo, git_ref).await {
            Ok(base_sha) => match self.download_tarball(owner, repo, &base_sha).await {
                Ok(bytes) => match RepoSnapshot::from_tarball(&bytes) {
                    Ok(snap) => Some(CachedSnapshot {
                        snap: Arc::new(snap),
                        base_sha,
                    }),
                    Err(e) => {
                        tracing::warn!(key, error = %e, "Failed to parse repo tarball; falling back to API");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(key, error = %e, "Failed to download repo tarball; falling back to API");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(key, error = %e, "Failed to resolve ref for snapshot; falling back to API");
                None
            }
        };
        self.snapshots.write().await.insert(key, built.clone());
        built.map(|c| c.snap)
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
            .map(|c| c.snap)
    }

    /// The commit SHA the cached snapshot for this ref reflects (its
    /// freshness stamp), if a snapshot exists. Write paths use this as the
    /// expected base: a live tip that differs means commits landed that this
    /// pass has never read.
    async fn snapshot_base_sha(&self, owner: &str, repo: &str, git_ref: &str) -> Option<String> {
        self.snapshots
            .read()
            .await
            .get(&Self::snapshot_key(owner, repo, git_ref))
            .cloned()
            .flatten()
            .map(|c| c.base_sha)
    }

    /// Advance the snapshot's freshness stamp to a commit the platform itself
    /// just created (whose content is mirrored via mirror_write/mirror_delete).
    async fn advance_snapshot_base(&self, owner: &str, repo: &str, git_ref: &str, new_sha: &str) {
        if let Some(Some(entry)) = self
            .snapshots
            .write()
            .await
            .get_mut(&Self::snapshot_key(owner, repo, git_ref))
        {
            entry.base_sha = new_sha.to_string();
        }
    }

    /// Files changed between two commits (compare API), for write-time
    /// conflict detection against foreign commits.
    async fn changed_files_between(
        &self,
        owner: &str,
        repo: &str,
        base_sha: &str,
        head_sha: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base_sha}...{head_sha}");
        let data = self.get(&url).await?;
        let files = data.get("files").and_then(|f| f.as_array());
        // Pagination fail-safe: the compare API returns at most 300 files in one
        // page. At the cap the list may be truncated, so a changed path beyond
        // it would be missed and its file silently reverted. Signal the caller
        // (which treats an error as "all paths conflict") rather than under-report.
        if files.map(|a| a.len()).unwrap_or(0) >= 300 {
            return Err(
                "compare file list truncated (>=300 files) — cannot verify write freshness".into(),
            );
        }
        Ok(files
            .map(|files| {
                files
                    .iter()
                    .filter_map(|f| f.get("filename").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default())
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
        // GitHub silently truncates trees over 100k entries / 7MB. A partial
        // tree makes agents conclude files don't exist — build the snapshot
        // (full tarball) for the complete listing instead.
        if data.get("truncated").and_then(|v| v.as_bool()) == Some(true) {
            if let Some(snap) = self.snapshot(owner, repo, git_ref).await {
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
            tracing::warn!(
                owner,
                repo,
                "git tree truncated and snapshot unavailable — file list incomplete"
            );
        }
        let tree: Vec<TreeEntry> =
            serde_json::from_value(data.get("tree").cloned().unwrap_or(serde_json::json!([])))?;
        Ok(tree)
    }

    /// Every file at a commit with its size in bytes, straight from the API.
    /// Errors when GitHub cuts the list short, so a caller never mistakes a
    /// partial list for the whole repository.
    pub async fn list_files_with_sizes(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees/{sha}?recursive=1");
        let data = self.get(&url).await?;
        if data.get("truncated").and_then(|v| v.as_bool()) == Some(true) {
            return Err("the repository is too large to list in one read".into());
        }
        Ok(data
            .get("tree")
            .and_then(|t| t.as_array())
            .into_iter()
            .flatten()
            .filter(|e| e.get("type").and_then(|t| t.as_str()) == Some("blob"))
            .filter_map(|e| {
                Some((
                    e.get("path")?.as_str()?.to_string(),
                    e.get("size").and_then(|s| s.as_u64()).unwrap_or(0),
                ))
            })
            .collect())
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
        self.read_file_live(owner, repo, path, git_ref).await
    }

    /// Like `read_file`, but a file that does not exist at `git_ref` is
    /// `Ok(None)`. Every other failure stays an error, so a caller deciding
    /// "deleted vs. present" never reads a failed fetch as a deletion.
    pub async fn read_file_if_exists(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        match self.read_file(owner, repo, path, git_ref).await {
            Ok(content) => Ok(Some(content)),
            Err(e) if is_not_found(&*e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Read a file straight from the LIVE API, bypassing the snapshot cache.
    /// Used where staleness is dangerous — the 409 reconcile must compare
    /// against what is ACTUALLY on the branch, not our (possibly stale) mirror.
    ///
    /// The contents API carries no body for files over 1 MB (`encoding:
    /// none`); those are read from their git blob. A file with bytes is never
    /// returned as an empty string: callers write what they read back to the
    /// branch, and an empty read once replaced a 28,915-line lockfile.
    pub async fn read_file_live(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        let data = self.get(&url).await?;
        match contents_body(path, &data)? {
            ContentsBody::Text(text) => Ok(text),
            ContentsBody::Blob(sha) => {
                let url = format!("{API_BASE}/repos/{owner}/{repo}/git/blobs/{sha}");
                let blob = self.get(&url).await?;
                Ok(blob_text(path, &blob)?)
            }
        }
    }

    /// Size in bytes of `path` at `git_ref`, or None when it does not exist.
    /// Metadata only: the contents API reports `size` even for files too
    /// large to carry a body.
    async fn existing_file_size(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        match self.get(&url).await {
            Ok(data) => Ok(Some(data.get("size").and_then(|s| s.as_u64()).unwrap_or(0))),
            Err(e) if is_not_found(&*e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Does `path` exist at `git_ref`? Metadata only, so a presence check on
    /// a large lockfile does not download it.
    pub async fn file_exists(&self, owner: &str, repo: &str, path: &str, git_ref: &str) -> bool {
        if let Some(snap) = self.existing_snapshot(owner, repo, git_ref).await {
            if snap.read_file(path).await.is_some() {
                return true;
            }
        }
        matches!(
            self.existing_file_size(owner, repo, path, git_ref).await,
            Ok(Some(_))
        )
    }

    /// Refuse to write empty content over a file that has bytes. Emptying a
    /// file is never what a write means here; it is what an empty or failed
    /// read looks like by the time it reaches a write (8 fix cycles once kept
    /// "restoring" a lockfile to nothing). Removing a file must be an explicit
    /// delete. Only empty writes cost the extra lookup.
    async fn guard_empty_overwrite(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !content.trim().is_empty() {
            return Ok(());
        }
        let existing = self
            .existing_file_size(owner, repo, path, git_ref)
            .await
            .map_err(|e| {
                format!(
                    "refusing to write empty content to '{path}': could not confirm the file is \
                     empty or absent on '{git_ref}' ({e}). Delete the file explicitly if that is the intent."
                )
            })?;
        match empty_overwrite_refusal(path, content, existing) {
            Some(msg) => Err(msg.into()),
            None => Ok(()),
        }
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
        // Guard against a model-supplied inverted range (start > end): clamp
        // end up to start so the slice can never panic.
        let end = end_line.min(lines.len()).max(start);
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

    /// Lines of code per language under `under` ("" for the whole
    /// repository), counted from the repository snapshot: one tarball
    /// download, no shell. See `passes::line_count`.
    pub async fn count_lines(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
        under: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let snap = self
            .snapshot(owner, repo, git_ref)
            .await
            .ok_or("the repository's snapshot isn't available (too large, or GitHub refused)")?;
        let mut counter = crate::passes::line_count::Counter::new(under);
        snap.visit(&mut |path, text| counter.add(path, text)).await;
        Ok(counter.report())
    }

    /// Cross-repo code search across a whole ORG (code search API + `org:`
    /// qualifier). One call surfaces consumers of a symbol in sibling repos
    /// without downloading them — the basis for cross-repo blast radius.
    /// Default-branch only and rate-limited (~10/min); callers must use it
    /// sparingly and treat any error as "no signal", never fatal. Returns
    /// (repo_full_name, path) hits.
    pub async fn search_org_code(
        &self,
        org: &str,
        query: &str,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let raw_query = format!("{query} org:{org}");
        let encoded_query = urlencoding::encode(&raw_query);
        let url = format!("{API_BASE}/search/code?q={encoded_query}&per_page=30");
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?.error_for_status()?;
        let data: serde_json::Value = resp.json().await?;
        let items = data
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(items
            .iter()
            .filter_map(|item| {
                let repo = item
                    .get("repository")?
                    .get("full_name")?
                    .as_str()?
                    .to_string();
                let path = item.get("path")?.as_str()?.to_string();
                Some((repo, path))
            })
            .collect())
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
        // Callers (reviewers, snapshot) also pass a full commit SHA. It is
        // already the answer; `git/ref/heads/<sha>` is always a 404.
        if is_commit_sha(branch) {
            return Ok(branch.to_ascii_lowercase());
        }
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/ref/heads/{branch}");
        let data = self.get(&url).await?;
        let sha = data
            .pointer("/object/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in ref response")?;
        Ok(sha.to_string())
    }

    /// The commit a plugin source's ref points at: "" is the default
    /// branch, `tags/<name>` a tag (an annotated tag is followed to its
    /// commit), a hex string of 7 to 40 characters a commit, anything else
    /// a branch.
    pub async fn resolve_ref(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let git_ref = git_ref.trim();
        if git_ref.is_empty() {
            let branch = self.get_default_branch(owner, repo).await?;
            return self.get_ref(owner, repo, &branch).await;
        }
        if let Some(tag) = git_ref.strip_prefix("tags/") {
            let url = format!("{API_BASE}/repos/{owner}/{repo}/git/ref/tags/{tag}");
            let data = self.get(&url).await?;
            let sha = data
                .pointer("/object/sha")
                .and_then(|v| v.as_str())
                .ok_or("Missing sha in tag ref")?;
            if data.pointer("/object/type").and_then(|v| v.as_str()) == Some("tag") {
                let t = self
                    .get(&format!("{API_BASE}/repos/{owner}/{repo}/git/tags/{sha}"))
                    .await?;
                return Ok(t
                    .pointer("/object/sha")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing sha in tag")?
                    .to_string());
            }
            return Ok(sha.to_string());
        }
        if (7..40).contains(&git_ref.len()) && git_ref.chars().all(|c| c.is_ascii_hexdigit()) {
            let c = self
                .get(&format!(
                    "{API_BASE}/repos/{owner}/{repo}/commits/{git_ref}"
                ))
                .await?;
            return Ok(c["sha"]
                .as_str()
                .ok_or("Missing sha in commit")?
                .to_string());
        }
        self.get_ref(owner, repo, git_ref).await
    }

    /// The tree SHA at `branch`'s tip. Trees are content-addressed: two equal
    /// tree SHAs mean byte-identical file sets, whatever commits lie between.
    pub async fn get_tree_sha(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let sha = self.get_ref(owner, repo, branch).await?;
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits/{sha}");
        let data = self.get(&url).await?;
        data.pointer("/tree/sha")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| "Missing tree sha in commit".into())
    }

    /// Fast-forward-only ref update (compare-and-swap): points `branch` at
    /// `sha` with `force:false`, so GitHub REJECTS a non-fast-forward move
    /// (another writer raced us). Returns Ok(false) on that conflict so the
    /// caller can rebuild on the new tip; Ok(true) on success. This is what
    /// turns concurrent branch writes from "silently drop a commit" into
    /// "serialize by retry."
    async fn update_ref_ff(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        sha: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
        let headers = self.auth_headers().await?;
        let mut req = self
            .http
            .patch(&url)
            .json(&serde_json::json!({"sha": sha, "force": false}));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        match resp.status().as_u16() {
            200 => Ok(true),
            409 | 422 => Ok(false), // non-fast-forward — someone advanced the ref
            _ => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Err(format!("update ref failed: {status} {body}").into())
            }
        }
    }

    /// Collapse all of `branch`'s commits into a SINGLE commit on top of
    /// `base_branch`, preserving the exact final tree (the diff is identical).
    /// Turns a run's 10-30 noisy per-tool-call commits into one clean commit.
    /// MUST only be called when no other writer is touching the branch (the
    /// sequential main flow before the PR exists) — it force-rewrites history.
    /// Best-effort: returns Err without mutating on any problem.
    pub async fn squash_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        base_branch: &str,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let base_sha = self.get_ref(owner, repo, base_branch).await?;
        let branch_sha = self.get_ref(owner, repo, branch).await?;
        if branch_sha == base_sha {
            return Ok(()); // nothing to squash
        }
        // FOREIGN-COMMIT GUARD: the force-rewrite below is only safe when
        // every commit being collapsed is our own. If anyone else (a user's
        // local push) has committed to this branch, squashing from our view
        // of the tree would erase their work — so we skip the cosmetic squash
        // and keep full history instead. Fails safe: any doubt → no squash.
        match self.app_bot_login().await {
            Ok(bot_login) => {
                let compare_url =
                    format!("{API_BASE}/repos/{owner}/{repo}/compare/{base_sha}...{branch_sha}");
                let cmp = self.get(&compare_url).await?;
                // Pagination fail-safe: the compare API returns at most 250
                // commits in one page. If the branch has more, a user commit
                // beyond the page would go undetected and the force-squash would
                // erase it — so any truncation means skip the squash.
                let total_commits = cmp
                    .get("total_commits")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let returned_commits = cmp
                    .get("commits")
                    .and_then(|c| c.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                if total_commits as usize > returned_commits || returned_commits >= 250 {
                    tracing::warn!(
                        branch,
                        total_commits,
                        returned_commits,
                        "skipping squash: too many commits to verify authorship (compare pagination) — preserving history"
                    );
                    return Ok(());
                }
                let foreign: Vec<String> = cmp
                    .get("commits")
                    .and_then(|c| c.as_array())
                    .map(|commits| {
                        commits
                            .iter()
                            .filter(|c| {
                                c.pointer("/committer/login").and_then(|v| v.as_str())
                                    != Some(bot_login.as_str())
                            })
                            .filter_map(|c| c.get("sha").and_then(|v| v.as_str()))
                            .map(|s| s[..7.min(s.len())].to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                if !foreign.is_empty() {
                    tracing::warn!(
                        branch,
                        foreign_commits = %foreign.join(", "),
                        "skipping squash: branch contains commits not authored by the app — preserving history"
                    );
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::warn!(branch, error = %e, "could not resolve app bot login — skipping squash (fail-safe)");
                return Ok(());
            }
        }
        // Tree of the branch's current tip = the full accumulated result.
        let commit_url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits/{branch_sha}");
        let tree_sha = self
            .get(&commit_url)
            .await?
            .pointer("/tree/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing tree sha in branch commit")?
            .to_string();
        // Don't squash if it would produce an empty diff vs base (guards
        // against a base that already contains the tree).
        let base_tree = self
            .get(&format!(
                "{API_BASE}/repos/{owner}/{repo}/git/commits/{base_sha}"
            ))
            .await?
            .pointer("/tree/sha")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if base_tree.as_deref() == Some(tree_sha.as_str()) {
            return Ok(()); // no net change
        }
        let new_commit = self
            .post(
                &format!("{API_BASE}/repos/{owner}/{repo}/git/commits"),
                &serde_json::json!({
                    "message": message,
                    "tree": tree_sha,
                    "parents": [base_sha],
                }),
            )
            .await?;
        let new_sha = new_commit
            .get("sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in squash commit")?;
        // Force-update: intentional history rewrite (single-writer context).
        let ref_url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
        self.patch(
            &ref_url,
            &serde_json::json!({"sha": new_sha, "force": true}),
        )
        .await?;
        self.invalidate_snapshot(owner, repo, branch).await;
        Ok(())
    }

    /// Create a new branch from an existing ref.
    pub async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.start_branch(owner, repo, branch, from_ref)
            .await
            .map(|(sha, _)| sha)
    }

    /// Creates `branch` at `from_ref`, or says what it did with a branch of
    /// that name that already exists ([`BranchStart`]), so a run's progress
    /// note never says "created" over work it kept.
    pub async fn start_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from_ref: &str,
    ) -> Result<(String, BranchStart), Box<dyn std::error::Error + Send + Sync>> {
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
            // Branch already exists. INVARIANT: a branch with an OPEN PR is
            // never force-reset to base — resetting makes the head identical to
            // base, which empties the PR's diff and makes GitHub AUTO-CLOSE the
            // PR (observed live: a conflict-resolution run wiped a PR this way).
            // With an open PR we continue from the branch's current head; only
            // a stale, PR-less leftover branch is reset. When GitHub can't say
            // whether it has an open PR, nothing is reset: a failed lookup
            // used to read as "no PR" and wiped the branch.
            let prs_url = format!(
                "{API_BASE}/repos/{owner}/{repo}/pulls?head={owner}:{branch}&state=open&per_page=1"
            );
            let has_open_pr = match self.get(&prs_url).await {
                Ok(v) => v.as_array().is_some_and(|a| !a.is_empty()),
                Err(e) => {
                    return Err(format!(
                        "branch {branch} already exists and GitHub couldn't say whether it has an \
                         open pull request ({e}); it was left as it is"
                    )
                    .into())
                }
            };
            if has_open_pr {
                let head = self.get_ref(owner, repo, branch).await?;
                tracing::info!(
                    branch,
                    "Branch has an open PR — continuing from its head, NOT resetting to base"
                );
                return Ok((head, BranchStart::KeptOpenPr));
            }
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
            return Ok((new_sha.to_string(), BranchStart::Reset));
        }

        let data: serde_json::Value = resp.error_for_status()?.json().await?;
        let new_sha = data
            .pointer("/object/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in create branch response")?;
        Ok((new_sha.to_string(), BranchStart::Created))
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
        message: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/merges");
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(&url).json(&serde_json::json!({
            "base": base,
            "head": head,
            "commit_message": message,
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

    // ─── Bringing a branch up to date with its base ─────────────

    /// Merges `head` into the branch `into` with the merges API and returns
    /// the new commit, `None` on a conflict (409). `Some(Null)` when `into`
    /// already had it (204).
    async fn merge_returning(
        &self,
        owner: &str,
        repo: &str,
        into: &str,
        head: &str,
        message: &str,
    ) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/merges");
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(&url).json(&serde_json::json!({
            "base": into,
            "head": head,
            "commit_message": message,
        }));
        for (k, v) in &headers {
            req = req.header(k.clone(), v.clone());
        }
        let resp = req.send().await?;
        match resp.status().as_u16() {
            201 => Ok(Some(resp.json().await?)),
            204 => Ok(Some(serde_json::Value::Null)),
            409 => Ok(None),
            _ => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Err(format!("merge failed: {status} {body}").into())
            }
        }
    }

    /// A commit object from the git data API (tree, parents, author).
    async fn git_commit(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get(&format!(
            "{API_BASE}/repos/{owner}/{repo}/git/commits/{sha}"
        ))
        .await
    }

    /// Writes a commit object and returns its sha.
    async fn new_commit(
        &self,
        owner: &str,
        repo: &str,
        body: serde_json::Value,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let c = self
            .post(
                &format!("{API_BASE}/repos/{owner}/{repo}/git/commits"),
                &body,
            )
            .await?;
        c["sha"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| "Missing sha in new commit".into())
    }

    /// Points a scratch ref at `sha`, rewriting it (scratch refs only).
    async fn move_scratch_ref(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        sha: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.patch(
            &format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}"),
            &serde_json::json!({"sha": sha, "force": true}),
        )
        .await?;
        Ok(())
    }

    /// Whether the branch is protected (classic branch protection).
    /// Readable with metadata access.
    pub async fn branch_is_protected(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let b = self
            .get(&format!(
                "{API_BASE}/repos/{owner}/{repo}/branches/{}",
                urlencoding::encode(branch)
            ))
            .await?;
        Ok(b["protected"].as_bool().unwrap_or(false))
    }

    /// The branch protection's force push setting: `Some(true)` when force
    /// pushes are allowed. Needs read access to administration; callers
    /// treat an error as not known.
    pub async fn force_pushes_allowed(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Option<bool>, Box<dyn std::error::Error + Send + Sync>> {
        let p = self
            .get(&format!(
                "{API_BASE}/repos/{owner}/{repo}/branches/{}/protection",
                urlencoding::encode(branch)
            ))
            .await?;
        Ok(p.pointer("/allow_force_pushes/enabled")
            .and_then(|v| v.as_bool()))
    }

    /// Whether the repository's merge settings allow squash merging. `None`
    /// when GitHub doesn't say.
    pub async fn allows_squash_merge(&self, owner: &str, repo: &str) -> Option<bool> {
        self.get(&format!("{API_BASE}/repos/{owner}/{repo}"))
            .await
            .ok()
            .and_then(|r| r["allow_squash_merge"].as_bool())
    }

    /// Whether every commit on `head` that `base` lacks (merge commits
    /// aside: a rebase drops them) was made by this app. Only then may the
    /// branch's history be rewritten. Any doubt (more commits than one page
    /// shows, an unknown committer) is `false`.
    pub async fn only_app_commits(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let bot = self.app_bot_login().await?;
        let cmp = self
            .get(&format!(
                "{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}"
            ))
            .await?;
        Ok(commits_all_by(&cmp, &bot))
    }

    /// Rebases `branch` onto `base` on GitHub's side: each of the branch's
    /// own commits (merge commits are dropped, as `git rebase` does) is
    /// replayed on top of the base with the merges API (a three-way merge
    /// against the commit's parent, which is a cherry-pick), keeping its
    /// message and author, on a scratch ref. The result must hold exactly
    /// the code a merge of the base would (that is the check), and the
    /// branch is then moved with a lease: only if it still points at
    /// `lease`. Nothing is pushed unless all of that holds.
    pub async fn rebase_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        base: &str,
        lease: &str,
    ) -> Result<RebaseResult, Box<dyn std::error::Error + Send + Sync>> {
        let base_sha = self.get_ref(owner, repo, base).await?;
        let cmp = self
            .get(&format!(
                "{API_BASE}/repos/{owner}/{repo}/compare/{base_sha}...{lease}"
            ))
            .await?;
        if cmp["behind_by"].as_u64() == Some(0) {
            return Ok(RebaseResult::UpToDate);
        }
        let Some(steps) = replay_steps(&cmp) else {
            return Ok(RebaseResult::NotClean(
                "too many commits to replay".to_string(),
            ));
        };
        let scratch = format!(
            "coderhelm-rebase/{branch}-{}",
            chrono::Utc::now().timestamp_millis()
        );
        self.create_branch(owner, repo, &scratch, &base_sha).await?;
        let result = self
            .replay(owner, repo, &scratch, &base_sha, lease, &steps)
            .await;
        let _ = self.delete_branch(owner, repo, &scratch).await;
        let (tip, tip_tree) = match result? {
            Ok(t) => t,
            Err(why) => return Ok(RebaseResult::NotClean(why)),
        };

        // The check: a rebase holds the same code as merging the base in.
        let check = format!("{scratch}-check");
        self.create_branch(owner, repo, &check, lease).await?;
        let merged = self
            .merge_returning(owner, repo, &check, &base_sha, "rebase check")
            .await;
        let _ = self.delete_branch(owner, repo, &check).await;
        let merged_tree = match merged? {
            Some(c) => c
                .pointer("/commit/tree/sha")
                .and_then(|v| v.as_str())
                .map(String::from),
            None => None,
        };
        if merged_tree.as_deref() != Some(tip_tree.as_str()) {
            return Ok(RebaseResult::NotClean(
                "the rebased code differs from a merge of the base".to_string(),
            ));
        }

        // The lease: only if nobody pushed since.
        if self.get_ref(owner, repo, branch).await? != lease {
            return Ok(RebaseResult::Moved);
        }
        self.move_scratch_ref(owner, repo, branch, &tip).await?;
        self.invalidate_snapshot(owner, repo, branch).await;
        Ok(RebaseResult::Rebased(tip))
    }

    /// Replays `steps` on the scratch ref starting at `base_sha`. The new
    /// tip and its tree, or why it couldn't (a step conflicted).
    #[allow(clippy::type_complexity)]
    async fn replay(
        &self,
        owner: &str,
        repo: &str,
        scratch: &str,
        base_sha: &str,
        lease: &str,
        steps: &[ReplayStep],
    ) -> Result<Result<(String, String), String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut tip = base_sha.to_string();
        let mut tip_tree = self.git_commit(owner, repo, base_sha).await?["tree"]["sha"]
            .as_str()
            .ok_or("Missing tree sha in base commit")?
            .to_string();
        for step in steps {
            // A stand-in for the commit's parent holding the tip's code:
            // merging the commit into it applies just the commit's change.
            let stand_in = self
                .new_commit(
                    owner,
                    repo,
                    serde_json::json!({
                        "message": "rebase step",
                        "tree": tip_tree,
                        "parents": [step.parent],
                    }),
                )
                .await?;
            self.move_scratch_ref(owner, repo, scratch, &stand_in)
                .await?;
            let applied = match self
                .merge_returning(owner, repo, scratch, &step.sha, "rebase step")
                .await?
            {
                None => {
                    return Ok(Err(format!(
                        "commit {} conflicts with the base",
                        &step.sha[..7.min(step.sha.len())]
                    )))
                }
                Some(serde_json::Value::Null) => continue,
                Some(c) => c
                    .pointer("/commit/tree/sha")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing tree sha in merge")?
                    .to_string(),
            };
            // A commit the base already carries replays to nothing.
            if applied == tip_tree {
                continue;
            }
            let mut body = serde_json::json!({
                "message": step.message,
                "tree": applied,
                "parents": [tip],
            });
            if !step.author.is_null() {
                body["author"] = step.author.clone();
            }
            tip = self.new_commit(owner, repo, body).await?;
            tip_tree = applied;
        }
        if tip == base_sha {
            return Ok(Err(format!(
                "nothing of {} is left after the base",
                &lease[..7.min(lease.len())]
            )));
        }
        Ok(Ok((tip, tip_tree)))
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
        self.guard_empty_overwrite(owner, repo, path, branch, content)
            .await?;
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}");
        let encoded = B64.encode(content.as_bytes());
        // FRESHNESS CONTRACT (same as batch_write): if the branch tip moved
        // past what this pass's snapshot reflects AND this file is among the
        // foreign changes, refuse — writing our (snapshot-era) body would
        // silently revert the newer commit. The caller must re-read and
        // reapply on top of current content.
        if let Some(expected) = self.snapshot_base_sha(owner, repo, branch).await {
            let live = self.get_ref(owner, repo, branch).await?;
            if live != expected {
                let foreign = self
                    .changed_files_between(owner, repo, &expected, &live)
                    .await
                    .unwrap_or_else(|_| vec![path.to_string()]);
                if foreign.iter().any(|p| p == path) {
                    // Drop the stale snapshot so the re-read this error asks
                    // for returns LIVE content; keeping it re-served the old
                    // body and the write was refused again, forever.
                    self.invalidate_snapshot(owner, repo, branch).await;
                    return Err(format!(
                        "branch advanced upstream: '{path}' was changed by a commit this pass has not read. \
                         Re-read the file and reapply the change on top of its current content."
                    )
                    .into());
                }
                self.invalidate_snapshot(owner, repo, branch).await;
            }
        }
        // A 409/422 mid-flight means the file changed under us DURING the
        // write — the old behavior (refetch sha, re-PUT the same body) would
        // steamroll that concurrent change. One retry is allowed only for the
        // stale-sha bookkeeping case where the file's CURRENT content already
        // equals what we are writing (idempotent replay); anything else is a
        // real conflict and errors out for the caller to re-read.
        let mut cur_sha = sha.map(|s| s.to_string());
        let mut retried = false;
        loop {
            let mut body = serde_json::json!({
                "message": message,
                "content": encoded,
                "branch": branch,
            });
            if let Some(ref s) = cur_sha {
                body["sha"] = serde_json::json!(s);
            }
            let headers = self.auth_headers().await?;
            let mut req = self.http.put(&url).json(&body);
            for (k, v) in headers {
                req = req.header(k, v);
            }
            let resp = req.send().await?;
            let status = resp.status().as_u16();
            if (200..300).contains(&status) {
                let result: serde_json::Value = resp.json().await?;
                self.mirror_write(owner, repo, branch, path, content).await;
                if let Some(new_sha) = result.pointer("/commit/sha").and_then(|v| v.as_str()) {
                    self.advance_snapshot_base(owner, repo, branch, new_sha)
                        .await;
                }
                return Ok(result);
            }
            if (status == 409 || status == 422) && !retried {
                retried = true;
                // Compare against LIVE content, not the snapshot — a snapshot
                // read here returns our own mirrored body and would falsely
                // "reconcile" (re-PUT) over a user's concurrent push, reverting
                // it. Only truly-idempotent replays (live == intended) proceed.
                let current_content = self.read_file_live(owner, repo, path, branch).await;
                let current_sha = self.get_file_sha(owner, repo, path, branch).await;
                match (current_content, current_sha) {
                    (Ok(cc), Ok(cs)) if cc == content => {
                        // Someone already wrote exactly this content — or our
                        // sha was stale bookkeeping. Safe to reconcile.
                        tracing::info!(
                            path,
                            "write_file conflict resolved: current content already matches"
                        );
                        cur_sha = Some(cs);
                        continue;
                    }
                    _ => {
                        // The snapshot may hold our own mirrored body; drop it
                        // so the re-read sees what is actually on the branch.
                        self.invalidate_snapshot(owner, repo, branch).await;
                        return Err(format!(
                            "write_file conflict: '{path}' changed concurrently on '{branch}'. \
                             Re-read the file and reapply the change on top of its current content."
                        )
                        .into());
                    }
                }
            }
            let body_txt = resp.text().await.unwrap_or_default();
            return Err(format!("write_file failed: {status} {body_txt}").into());
        }
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
        // An empty Write over a file with bytes is refused before anything is
        // created; a removal must come as FileOp::Delete.
        for f in files {
            if let FileOp::Write { path, content } = f {
                self.guard_empty_overwrite(owner, repo, path, branch, content)
                    .await?;
            }
        }
        // Blobs are content-addressed and immutable — create them once, reuse
        // across retries.
        let mut blob_entries = Vec::new();
        for f in files {
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
                        .ok_or("Missing blob sha")?
                        .to_string();
                    blob_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_sha,
                    }));
                }
                FileOp::Delete { path } => {
                    blob_entries.push(serde_json::json!({
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": null,
                    }));
                }
            }
        }

        // Build tree + commit ON THE CURRENT TIP and update the ref with a
        // fast-forward-only CAS. If another writer raced us, rebuild on their
        // new tip (base_tree = new ref preserves their commit) and retry.
        //
        // FRESHNESS CONTRACT: the CAS protects the ref transition, not file
        // content — these blob bodies came from this pass's snapshot. If the
        // live tip holds commits the snapshot never saw (a user's local push,
        // another writer), overlaying snapshot-era bodies would silently
        // revert them. So when the tip != the snapshot's stamp, we diff the
        // foreign range: disjoint from our write-set → safe, proceed on the
        // new tip (and drop the now-stale snapshot); overlapping → REFUSE the
        // write with the conflicting paths so the caller re-reads fresh state
        // instead of reverting someone's work.
        let write_paths: Vec<&str> = files
            .iter()
            .map(|f| match f {
                FileOp::Write { path, .. } => path.as_str(),
                FileOp::Delete { path } => path.as_str(),
            })
            .collect();
        let tree_url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees");
        let commit_url = format!("{API_BASE}/repos/{owner}/{repo}/git/commits");
        let mut commit_sha = String::new();
        let mut committed = false;
        for attempt in 0..6u32 {
            let branch_sha = self.get_ref(owner, repo, branch).await?;
            if let Some(expected) = self.snapshot_base_sha(owner, repo, branch).await {
                if branch_sha != expected {
                    let foreign = self
                        .changed_files_between(owner, repo, &expected, &branch_sha)
                        .await
                        .unwrap_or_else(|e| {
                            tracing::warn!(branch, error = %e, "compare failed during freshness check — treating all write paths as conflicting");
                            write_paths.iter().map(|p| p.to_string()).collect()
                        });
                    let conflicts: Vec<&String> = foreign
                        .iter()
                        .filter(|p| write_paths.contains(&p.as_str()))
                        .collect();
                    if !conflicts.is_empty() {
                        // Same as write_file: the re-read must see live content.
                        self.invalidate_snapshot(owner, repo, branch).await;
                        return Err(format!(
                            "branch advanced upstream: {} commit(s) landed after this pass read the repo, touching file(s) this write would overwrite: {}. \
                             Re-read the current content of these files and reapply the change on top of it.",
                            &branch_sha[..7.min(branch_sha.len())],
                            conflicts
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                        .into());
                    }
                    tracing::warn!(
                        branch,
                        expected_base = %expected,
                        live_tip = %branch_sha,
                        "branch advanced upstream in files disjoint from this write — proceeding on the new tip and dropping the stale snapshot"
                    );
                    self.invalidate_snapshot(owner, repo, branch).await;
                }
            }
            let tree = self
                .post(
                    &tree_url,
                    &serde_json::json!({ "base_tree": branch_sha, "tree": blob_entries }),
                )
                .await?;
            let tree_sha = tree
                .get("sha")
                .and_then(|v| v.as_str())
                .ok_or("Missing tree sha")?;
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
            commit_sha = commit
                .get("sha")
                .and_then(|v| v.as_str())
                .ok_or("Missing commit sha")?
                .to_string();
            if self.update_ref_ff(owner, repo, branch, &commit_sha).await? {
                committed = true;
                break;
            }
            tracing::warn!(
                branch,
                attempt,
                "batch_write ref conflict — another writer advanced the branch; rebuilding on new tip"
            );
            tokio::time::sleep(std::time::Duration::from_millis(300 * (attempt as u64 + 1))).await;
        }
        if !committed {
            return Err(
                "batch_write: branch kept advancing under concurrent writes (6 retries)".into(),
            );
        }

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
        // The snapshot now reflects this commit — advance its freshness stamp.
        self.advance_snapshot_base(owner, repo, branch, &commit_sha)
            .await;

        Ok(commit_sha)
    }

    /// Create a merge commit with two parents (for conflict resolution).
    /// Builds a tree from the branch tree + overridden files, then creates
    /// a commit with both branch and base as parents.
    #[allow(clippy::too_many_arguments)]
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

        // The merge tree overlays these files on the branch's tree, so an
        // empty resolution would empty the branch's copy. Same rule as
        // batch_write: removal must be an explicit FileOp::Delete.
        for f in resolved_files {
            if let FileOp::Write { path, content } = f {
                self.guard_empty_overwrite(owner, repo, path, branch_sha, content)
                    .await?;
            }
        }

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

        // Update branch ref with fast-forward-only CAS. If the branch advanced
        // during the (slow, LLM-driven) merge, the resolved content is stale —
        // fail cleanly so resolve_conflicts re-runs against the new tip rather
        // than clobbering the concurrent commit with a stale merge.
        if !self.update_ref_ff(owner, repo, branch, &commit_sha).await? {
            return Err(
                "create_merge_commit: branch advanced during conflict resolution; retry".into(),
            );
        }

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

    /// Add one or more labels to an issue/PR. Additive (GitHub keeps existing
    /// labels) and creates any label that doesn't exist yet. Used to hand a PR
    /// off to the repo's own label-triggered CI (e.g. a deploy workflow).
    pub async fn add_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        labels: &[String],
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/labels");
        self.post(&url, &serde_json::json!({ "labels": labels }))
            .await
    }

    /// Every label defined in the repo (name + description), all pages.
    pub async fn list_repo_labels(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::new();
        for page in 1..=10u32 {
            let url = format!("{API_BASE}/repos/{owner}/{repo}/labels?per_page=100&page={page}");
            let batch: Vec<serde_json::Value> = serde_json::from_value(self.get(&url).await?)?;
            let done = batch.len() < 100;
            out.extend(batch.iter().map(|l| {
                (
                    l["name"].as_str().unwrap_or("").to_string(),
                    l["description"].as_str().unwrap_or("").to_string(),
                )
            }));
            if done {
                break;
            }
        }
        Ok(out)
    }

    // ─── History (read-only) ───────────────────────────────────

    /// GET a path under `/repos/{owner}/{repo}/` (built by the history tool
    /// from checked inputs). Only ever a GET.
    pub async fn get_repo_path(
        &self,
        owner: &str,
        repo: &str,
        rest: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/{}",
            rest.trim_start_matches('/')
        );
        self.get(&url).await
    }

    /// One GraphQL query, refused unless it only reads.
    pub async fn graphql_read(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let read = common::live::graphql::check_read_only(query).map_err(|e| e.message)?;
        let v = self
            .post(
                &format!("{API_BASE}/graphql"),
                &serde_json::json!({ "query": read.text, "variables": variables }),
            )
            .await?;
        if let Some(errs) = v["errors"].as_array().filter(|e| !e.is_empty()) {
            let msg: Vec<&str> = errs.iter().filter_map(|e| e["message"].as_str()).collect();
            return Err(msg.join("; ").into());
        }
        Ok(v["data"].clone())
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

    /// Whether `login` has write access or higher to the repository, from
    /// GitHub's collaborator permission (the same check the comment commands
    /// make, `common::merge_gate::has_write_access`). False when the login
    /// isn't a plain GitHub login or the answer can't be read.
    pub async fn has_write_access(&self, owner: &str, repo: &str, login: &str) -> bool {
        let valid = !login.is_empty()
            && login.len() <= 39
            && login.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
        if !valid {
            return false;
        }
        let url = format!("{API_BASE}/repos/{owner}/{repo}/collaborators/{login}/permission");
        match self.get(&url).await {
            Ok(v) => common::merge_gate::has_write_access(&v),
            Err(e) => {
                tracing::info!(owner, repo, login, error = %e, "No permission answer for the commenter");
                false
            }
        }
    }

    /// The commits on a pull request (first 100).
    pub async fn list_pr_commits(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/commits?per_page=100");
        let v = self.get(&url).await?;
        Ok(v.as_array().cloned().unwrap_or_default())
    }

    /// The files one commit changes, as (path, patch), first 300.
    pub async fn commit_patches(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let v = self
            .get(&format!("{API_BASE}/repos/{owner}/{repo}/commits/{sha}"))
            .await?;
        Ok(v["files"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|f| {
                (
                    f["filename"].as_str().unwrap_or("").to_string(),
                    f["patch"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect())
    }

    /// An issue (title, body, author), as the issues endpoint returns it.
    pub async fn get_issue(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get(&format!(
            "{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}"
        ))
        .await
    }

    /// The login of whoever closed a PR, via the issues endpoint (the PR
    /// object doesn't carry `closed_by`). None if open, merged, or unknown.
    pub async fn get_pr_closed_by(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Option<String> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/issues/{pr_number}");
        let data = self.get(&url).await.ok()?;
        data.get("closed_by")
            .and_then(|v| v.get("login"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
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

    /// Post a review verdict on a PR. `event` is "APPROVE", "REQUEST_CHANGES",
    /// or "COMMENT". Note: GitHub rejects APPROVE/REQUEST_CHANGES from the same
    /// identity that authored the PR — the reviewer pass falls back to "COMMENT"
    /// in that case so the verdict is still recorded.
    pub async fn create_pr_review(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        event: &str,
        body: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/reviews");
        self.post(&url, &serde_json::json!({ "event": event, "body": body }))
            .await
    }

    /// Post a review with INLINE line-level comments in a single call. `commit_id`
    /// pins the review to the reviewed head so comments land on the right lines;
    /// each comment anchors to a line (or start_line..line range) that MUST be part
    /// of the diff or GitHub 422s the whole review. Callers should pre-filter to
    /// changed lines and fall back to `create_pr_review` (body-only) on error so a
    /// single bad anchor never drops the verdict.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_pr_review_inline(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        commit_id: &str,
        event: &str,
        body: &str,
        comments: &[InlineComment],
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/reviews");
        let comments_json: Vec<serde_json::Value> = comments
            .iter()
            .map(|c| {
                let mut o = serde_json::json!({
                    "path": c.path,
                    "line": c.line,
                    "side": c.side,
                    "body": c.body,
                });
                if let Some(sl) = c.start_line {
                    if sl < c.line {
                        o["start_line"] = serde_json::json!(sl);
                        o["start_side"] = serde_json::json!(c.side);
                    }
                }
                o
            })
            .collect();
        let mut payload = serde_json::json!({
            "event": event,
            "body": body,
            "comments": comments_json,
        });
        if !commit_id.is_empty() {
            payload["commit_id"] = serde_json::json!(commit_id);
        }
        self.post(&url, &payload).await
    }

    /// Merge a PR, but ONLY if its head still equals `sha` (a blast-radius guard:
    /// a commit pushed after the review moves the head and GitHub returns 409, so
    /// unreviewed code is never merged). A 405 means GitHub refused the merge
    /// (branch protection, required reviews or checks, a conflict); its message
    /// is kept so the caller can show the actual reason. Any other status is a
    /// hard error so the caller never treats a failure as a merge.
    /// `merge_method` is "squash" | "merge" | "rebase".
    pub async fn merge_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        sha: &str,
        merge_method: &str,
    ) -> Result<MergeOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/merge");
        let headers = self.auth_headers().await?;
        let mut req = self.http.put(&url).json(&serde_json::json!({
            "sha": sha,
            "merge_method": merge_method,
        }));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        if status == 200 {
            return Ok(MergeOutcome::Merged);
        }
        let text = resp.text().await.unwrap_or_default();
        let reason = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v["message"].as_str().map(str::to_string))
            .unwrap_or_else(|| text.clone());
        match status {
            405 => Ok(MergeOutcome::NotMergeable(reason)),
            409 => Ok(MergeOutcome::HeadMoved(reason)),
            _ => Err(format!("merge failed ({status}): {text}").into()),
        }
    }

    /// List open PRs for a repo (newest first), used by the review-reminder sweep.
    pub async fn list_open_pulls(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls?state=open&sort=created&direction=desc&per_page=100"
        );
        self.get(&url).await
    }

    /// The files an open PR changes, as (path, patch), first 100.
    pub async fn list_pull_files(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{number}/files?per_page=100");
        let v = self.get(&url).await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| {
                Some((
                    f["filename"].as_str()?.to_string(),
                    f["patch"].as_str().unwrap_or("").to_string(),
                ))
            })
            .collect())
    }

    /// Every submitted review on a PR, oldest first (the two-key auto-merge
    /// gate reads approvals and dismissals from it). All pages: GitHub lists
    /// reviews chronologically, so on a busy PR the newest ones sit past the
    /// first page.
    pub async fn list_pr_reviews(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get_pages(
            &format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/reviews"),
            10,
        )
        .await
    }

    /// The issue/PR timeline (labels, draft changes, reviews, ...), oldest first.
    pub async fn list_issue_timeline(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get_pages(
            &format!("{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/timeline"),
            10,
        )
        .await
    }

    /// The issue/PR events (closed, reopened, merged, ...), oldest first: a
    /// `closed` event carries who closed it (`actor`) and when.
    pub async fn list_issue_events(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get_pages(
            &format!("{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/events"),
            5,
        )
        .await
    }

    /// GET a list endpoint page by page (100 per page) and concatenate the
    /// arrays, up to `max_pages`. Stops at the first short page.
    async fn get_pages(
        &self,
        base_url: &str,
        max_pages: u32,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut items: Vec<serde_json::Value> = Vec::new();
        for page in 1..=max_pages {
            let batch = match self
                .get(&format!("{base_url}?per_page=100&page={page}"))
                .await?
            {
                serde_json::Value::Array(b) => b,
                other => return Err(format!("expected a JSON array, got {other}").into()),
            };
            let done = batch.len() < 100;
            items.extend(batch);
            if done {
                break;
            }
        }
        Ok(serde_json::Value::Array(items))
    }

    /// Dismiss a review on a PR (CoderHelm dismisses only its own stale
    /// "changes requested" reviews). A repo may restrict who can dismiss.
    pub async fn dismiss_review(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        review_id: u64,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/reviews/{review_id}/dismissals"
        );
        let headers = self.auth_headers().await?;
        let mut req = self.http.put(&url).json(&serde_json::json!({
            "message": message,
            "event": "DISMISS",
        }));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        ok_or_body(req.send().await?).await?;
        Ok(())
    }

    /// Replaces the body of a review already posted (no new notification).
    pub async fn update_pr_review(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        review_id: u64,
        body: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/reviews/{review_id}");
        let headers = self.auth_headers().await?;
        let mut req = self
            .http
            .put(&url)
            .json(&serde_json::json!({ "body": body }));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        ok_or_body(req.send().await?).await?;
        Ok(())
    }

    /// List tags on a repo (used to compute the next semver release tag).
    pub async fn list_tags(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/tags?per_page=100");
        self.get(&url).await
    }

    /// Commits in `base...head` (oldest first), up to `max` (GitHub caps a
    /// compare page at 250; we page with per_page=100).
    pub async fn compare_commits(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
        max: usize,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::new();
        for page in 1..=10u32 {
            let url = format!(
                "{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}?per_page=100&page={page}"
            );
            let data = self.get(&url).await?;
            let batch = data["commits"].as_array().cloned().unwrap_or_default();
            let done = batch.len() < 100;
            out.extend(batch);
            if done || out.len() >= max {
                break;
            }
        }
        out.truncate(max);
        Ok(out)
    }

    /// GitHub's comparison status of `head` against `base`: "ahead", "behind",
    /// "identical" or "diverged". `head` contains `base` when "ahead"/"identical".
    pub async fn compare_status(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}?per_page=1");
        Ok(self.get(&url).await?["status"]
            .as_str()
            .unwrap_or("")
            .to_string())
    }

    /// Recent commits on `sha` (newest first) — used when there is no previous
    /// release tag to compare against.
    pub async fn recent_commits(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
        max: usize,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/commits?sha={sha}&per_page={}",
            max.min(100)
        );
        Ok(self
            .get(&url)
            .await?
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    /// Pull requests associated with a commit (the PR that merged it).
    pub async fn pulls_for_commit(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=10");
        Ok(self
            .get(&url)
            .await?
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    /// The GitHub Release for `tag`, if one exists (any error → None).
    pub async fn get_release_by_tag(
        &self,
        owner: &str,
        repo: &str,
        tag: &str,
    ) -> Option<serde_json::Value> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/releases/tags/{tag}");
        self.get(&url).await.ok()
    }

    /// Create a GitHub Release for an existing tag.
    pub async fn create_release(
        &self,
        owner: &str,
        repo: &str,
        tag: &str,
        name: &str,
        body: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/releases");
        self.post(
            &url,
            &serde_json::json!({
                "tag_name": tag,
                "name": name,
                "body": body,
                "draft": false,
                "prerelease": false,
            }),
        )
        .await
    }

    /// Create an ANNOTATED tag pointing at `sha` (a release tag after an
    /// approved merge): first the tag OBJECT (tagger + message — what `git
    /// cat-file -t` reports as `tag`), then the ref pointing at that object.
    /// Lightweight tags (a bare ref at the commit) break org release
    /// conventions and some tag-triggered deploy setups, so we never cut them.
    /// Idempotency is the caller's job — GitHub 422s on a ref that already
    /// exists.
    pub async fn create_tag_ref(
        &self,
        owner: &str,
        repo: &str,
        tag: &str,
        sha: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let obj_url = format!("{API_BASE}/repos/{owner}/{repo}/git/tags");
        let tag_obj = self
            .post(
                &obj_url,
                &serde_json::json!({
                    "tag": tag,
                    "message": format!("Release {tag}\n\nCut automatically by CoderHelm after an approved merge."),
                    "object": sha,
                    "type": "commit",
                    "tagger": {
                        "name": "coderhelm[bot]",
                        "email": "coderhelm[bot]@users.noreply.github.com",
                        "date": chrono::Utc::now().to_rfc3339(),
                    }
                }),
            )
            .await?;
        let obj_sha = tag_obj["sha"]
            .as_str()
            .ok_or("Missing sha in tag object response")?;
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs");
        self.post(
            &url,
            &serde_json::json!({ "ref": format!("refs/tags/{tag}"), "sha": obj_sha }),
        )
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

    /// Conversation comments on an issue or PR, oldest first.
    pub async fn list_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let mut comments: Vec<serde_json::Value> = Vec::new();
        for page in 1..=5u32 {
            let url = format!(
                "{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100&page={page}"
            );
            let data = self.get(&url).await?;
            let batch: Vec<serde_json::Value> = serde_json::from_value(data)?;
            let done = batch.len() < 100;
            comments.extend(batch);
            if done {
                break;
            }
        }
        Ok(comments)
    }

    /// Get review comments on a PR.
    pub async fn get_review_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        // Paginate: GitHub's default page size is 30, and the bot's own
        // replies count toward it — an active PR blows past one page fast,
        // making newer reviewer comments invisible and already-answered
        // ones look unanswered.
        let mut comments: Vec<serde_json::Value> = Vec::new();
        for page in 1..=5u32 {
            let url = format!(
                "{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/comments?per_page=100&page={page}"
            );
            let data = self.get(&url).await?;
            let batch: Vec<serde_json::Value> = serde_json::from_value(data)?;
            let done = batch.len() < 100;
            comments.extend(batch);
            if done {
                break;
            }
        }
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
        let resp = self.post("https://api.github.com/graphql", &query).await?;
        // GraphQL reports failures in-band with HTTP 200.
        if let Some(errors) = resp.get("errors") {
            return Err(format!("GraphQL errors: {errors}").into());
        }
        Ok(resp)
    }

    /// CoderHelm's own inline review threads on a PR (the thread's first comment
    /// was written by the bot), open and resolved, each with the replies under it.
    pub async fn list_bot_review_threads(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<BotReviewThread>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..10 {
            let query = serde_json::json!({
                "query": "query($owner:String!,$name:String!,$pr:Int!,$after:String){ repository(owner:$owner,name:$name){ pullRequest(number:$pr){ reviewThreads(first:100, after:$after){ pageInfo{ hasNextPage endCursor } nodes{ id isResolved isOutdated path line originalLine resolvedBy{ login } comments(first:30){ nodes{ databaseId author{ login } body } } } } } } }",
                "variables": { "owner": owner, "name": repo, "pr": pr_number, "after": cursor }
            });
            let resp = self.post("https://api.github.com/graphql", &query).await?;
            if let Some(errors) = resp.get("errors") {
                return Err(format!("GraphQL errors: {errors}").into());
            }
            let threads = &resp["data"]["repository"]["pullRequest"]["reviewThreads"];
            for t in threads["nodes"].as_array().into_iter().flatten() {
                let comments = t["comments"]["nodes"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let Some(first) = comments.first() else {
                    continue;
                };
                let login =
                    |c: &serde_json::Value| c["author"]["login"].as_str().unwrap_or("").to_string();
                if !login(first).contains("coderhelm") {
                    continue;
                }
                let resolved = t["isResolved"].as_bool().unwrap_or(false);
                let resolved_by = t["resolvedBy"]["login"].as_str().unwrap_or("");
                out.push(BotReviewThread {
                    root_id: first["databaseId"].as_u64().unwrap_or(0),
                    replies: comments[1..]
                        .iter()
                        .map(|c| ThreadReply {
                            by_bot: login(c).contains("coderhelm"),
                            author: login(c),
                            body: c["body"].as_str().unwrap_or("").to_string(),
                        })
                        .collect(),
                    resolved,
                    resolved_by_person: resolved && !resolved_by.contains("coderhelm"),
                    id: t["id"].as_str().unwrap_or("").to_string(),
                    path: t["path"].as_str().unwrap_or("").to_string(),
                    line: t["line"]
                        .as_u64()
                        .or_else(|| t["originalLine"].as_u64())
                        .unwrap_or(0),
                    outdated: t["isOutdated"].as_bool().unwrap_or(false),
                    body: first["body"].as_str().unwrap_or("").to_string(),
                });
            }
            if !threads["pageInfo"]["hasNextPage"]
                .as_bool()
                .unwrap_or(false)
            {
                break;
            }
            cursor = threads["pageInfo"]["endCursor"].as_str().map(String::from);
        }
        Ok(out)
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

    /// Combined legacy commit-status (external CI that posts statuses rather than
    /// check-runs — e.g. a Terraform staging-apply status). Returns the roll-up
    /// `state` ("success" | "pending" | "failure" | "error") + the per-context list.
    pub async fn get_commit_status(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/commits/{git_ref}/status");
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
                        // char-safe: annotation text is arbitrary lint/test
                        // output; a raw byte slice panics on a multibyte char
                        // at the cut and (panic=abort) hard-kills the worker.
                        all_annotations
                            .push_str(&format!("{}\n", common::truncate_str(text, 3000)));
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

    /// List the files changed between base and head (compare API). The
    /// endpoint returns at most 300 files in one response; callers treat a
    /// near-cap result as "too large to trust" and fall back to unscoped.
    pub async fn compare_changed_files(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}");
        let data = self.get(&url).await?;
        Ok(data["files"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Lines added plus removed between two commits (the compare's files).
    pub async fn compare_line_changes(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}");
        let data = self.get(&url).await?;
        Ok(data["files"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|f| {
                        f["additions"].as_u64().unwrap_or(0) + f["deletions"].as_u64().unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0))
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
        let Some(pr) = data
            .as_array()
            .and_then(|arr| arr.iter().find(|pr| pr["merged_at"].is_null()).cloned())
        else {
            return Ok(None);
        };
        // Only a PR GitHub closed for us (a force-reset removed its commits) is
        // ours to reopen. A PR a person closed stays closed.
        let Some(number) = pr["number"].as_u64() else {
            return Ok(None);
        };
        let issue = self
            .get(&format!("{API_BASE}/repos/{owner}/{repo}/issues/{number}"))
            .await?;
        Ok(closed_by_a_bot(&issue).then_some(pr))
    }

    /// Update a pull request's title.
    pub async fn update_pr_title(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        title: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.patch(&url, &serde_json::json!({"title": title}))
            .await?;
        Ok(())
    }

    /// Replace a pull request's description.
    pub async fn update_pr_body(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.patch(&url, &serde_json::json!({"body": body})).await?;
        Ok(())
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

    // ─── Actions: E2E results ───────────────────────────────────

    /// POST that expects no response body (GitHub answers 201/204 empty).
    async fn post_empty(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let headers = self.auth_headers().await?;
        let mut req = self.http.post(url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        ok_or_body(req.send().await?).await?;
        Ok(())
    }

    /// Artifacts a workflow run produced: (id, name, size in bytes, expired).
    pub async fn list_run_artifacts(
        &self,
        owner: &str,
        repo: &str,
        run_id: u64,
    ) -> Result<Vec<(u64, String, u64, bool)>, Box<dyn std::error::Error + Send + Sync>> {
        let url =
            format!("{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts?per_page=100");
        let data = self.get(&url).await?;
        Ok(data["artifacts"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|a| {
                (
                    a["id"].as_u64().unwrap_or(0),
                    a["name"].as_str().unwrap_or("").to_string(),
                    a["size_in_bytes"].as_u64().unwrap_or(0),
                    a["expired"].as_bool().unwrap_or(false),
                )
            })
            .collect())
    }

    /// An artifact's zip. GitHub redirects to storage; the token is not sent
    /// along to the other host. Refused when bigger than `max_bytes`.
    pub async fn download_artifact(
        &self,
        owner: &str,
        repo: &str,
        artifact_id: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip");
        let headers = self.auth_headers().await?;
        let mut req = self.http.get(&url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let mut resp = req.send().await?.error_for_status()?;
        let mut buf = Vec::new();
        while let Some(chunk) = resp.chunk().await? {
            if buf.len() + chunk.len() > max_bytes {
                return Err(
                    format!("artifact larger than {} MB", max_bytes / (1024 * 1024)).into(),
                );
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }

    /// Re-runs only the failed jobs of a run.
    pub async fn rerun_failed_jobs(
        &self,
        owner: &str,
        repo: &str,
        run_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url =
            format!("{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs");
        self.post_empty(&url, &serde_json::json!({})).await
    }

    /// Deletes a branch. Used only for the short-lived branches Coderhelm
    /// makes itself to check a generated test against older code.
    pub async fn delete_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}");
        let headers = self.auth_headers().await?;
        let mut req = self.http.delete(&url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        ok_or_body(req.send().await?).await?;
        Ok(())
    }

    /// Files a compare changed, with their status and patch.
    pub async fn compare_files_with_patches(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<(String, String, String)>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/compare/{base}...{head}");
        let data = self.get(&url).await?;
        Ok(data["files"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|f| {
                (
                    f["filename"].as_str().unwrap_or("").to_string(),
                    f["status"].as_str().unwrap_or("").to_string(),
                    f["patch"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect())
    }

    /// The repository's branch names, at most `max` (100 per page).
    pub async fn list_branches(
        &self,
        owner: &str,
        repo: &str,
        max: usize,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = Vec::new();
        for page in 1..=10u32 {
            let url = format!("{API_BASE}/repos/{owner}/{repo}/branches?per_page=100&page={page}");
            let v = self.get(&url).await?;
            let batch: Vec<String> = v
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|b| b["name"].as_str().map(String::from))
                .collect();
            let done = batch.len() < 100;
            out.extend(batch);
            if done || out.len() >= max {
                break;
            }
        }
        out.truncate(max);
        Ok(out)
    }

    /// The files one commit changed.
    pub async fn commit_files(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/commits/{sha}");
        let v = self.get(&url).await?;
        Ok(v["files"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| f["filename"].as_str().map(String::from))
            .collect())
    }

    /// Starts a workflow by its file name on a branch, with inputs.
    pub async fn dispatch_workflow(
        &self,
        owner: &str,
        repo: &str,
        workflow_file: &str,
        git_ref: &str,
        inputs: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url =
            format!("{API_BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches");
        self.post_empty(
            &url,
            &serde_json::json!({ "ref": git_ref, "inputs": inputs }),
        )
        .await
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

    // ─── Test fixes: history, checks and re-runs ────────────────

    /// The installation this client acts for.
    pub fn installation_id(&self) -> u64 {
        self.installation_id
    }

    /// Commits on the default branch that touched `path`, newest first,
    /// since `since` (RFC 3339) when given.
    pub async fn commits_for_path(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        since: Option<&str>,
        per_page: u32,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let mut url = format!(
            "{API_BASE}/repos/{owner}/{repo}/commits?path={}&per_page={}",
            urlencoding::encode(path),
            per_page.min(100)
        );
        if let Some(s) = since.filter(|s| !s.is_empty()) {
            url.push_str(&format!("&since={}", urlencoding::encode(s)));
        }
        Ok(self
            .get(&url)
            .await?
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    /// One workflow run.
    pub async fn get_workflow_run(
        &self,
        owner: &str,
        repo: &str,
        run_id: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get(&format!(
            "{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}"
        ))
        .await
    }

    /// Runs of one workflow (by file name) on a branch, newest first.
    pub async fn list_workflow_runs(
        &self,
        owner: &str,
        repo: &str,
        workflow_file: &str,
        branch: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{API_BASE}/repos/{owner}/{repo}/actions/workflows/{}/runs?branch={}&per_page=20",
            urlencoding::encode(workflow_file),
            urlencoding::encode(branch)
        );
        Ok(self.get(&url).await?["workflow_runs"]
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    /// Runs a whole workflow run again (every job, so a skipped one is
    /// evaluated again).
    pub async fn rerun_workflow_run(
        &self,
        owner: &str,
        repo: &str,
        run_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.post_empty(
            &format!("{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/rerun"),
            &serde_json::json!({}),
        )
        .await
    }

    /// The rules that apply to a branch (rulesets), readable with metadata
    /// access.
    pub async fn branch_rules(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get(&format!(
            "{API_BASE}/repos/{owner}/{repo}/rules/branches/{}",
            urlencoding::encode(branch)
        ))
        .await
    }

    /// Classic branch protection's required status checks (needs read
    /// access to administration; callers treat an error as none).
    pub async fn required_status_checks(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.get(&format!(
            "{API_BASE}/repos/{owner}/{repo}/branches/{}/protection/required_status_checks",
            urlencoding::encode(branch)
        ))
        .await
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
/// How a rebase on GitHub's side went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseResult {
    /// The branch was moved to the rebased tip.
    Rebased(String),
    /// The branch already had everything of the base.
    UpToDate,
    /// The branch's commits didn't replay cleanly (why); nothing pushed.
    NotClean(String),
    /// Someone pushed to the branch meanwhile; nothing pushed.
    Moved,
}

/// One commit to replay onto the base.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayStep {
    pub sha: String,
    pub parent: String,
    pub message: String,
    /// The original author (name, email, date), kept on the new commit.
    pub author: serde_json::Value,
}

/// Most commits a rebase replays; more falls back to a merge.
pub const MAX_REPLAY: usize = 100;

/// The commits a rebase replays, oldest first, from a compare of the base
/// and the branch: merge commits are dropped (as `git rebase` does). `None`
/// when the compare doesn't list them all, or there are too many.
pub fn replay_steps(compare: &serde_json::Value) -> Option<Vec<ReplayStep>> {
    let commits = compare["commits"].as_array()?;
    let total = compare["total_commits"].as_u64().unwrap_or(0) as usize;
    if total > commits.len() || commits.len() >= 250 {
        return None;
    }
    let steps: Vec<ReplayStep> = commits
        .iter()
        .filter_map(|c| {
            let parents = c["parents"].as_array()?;
            if parents.len() != 1 {
                return None;
            }
            Some(ReplayStep {
                sha: c["sha"].as_str()?.to_string(),
                parent: parents[0]["sha"].as_str()?.to_string(),
                message: c
                    .pointer("/commit/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                author: c
                    .pointer("/commit/author")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            })
        })
        .collect();
    (steps.len() <= MAX_REPLAY).then_some(steps)
}

/// Whether a compare's commits (merge commits aside) were all committed by
/// `login`. A truncated list is `false`: an unseen commit may be someone's.
pub fn commits_all_by(compare: &serde_json::Value, login: &str) -> bool {
    let Some(commits) = compare["commits"].as_array() else {
        return false;
    };
    let total = compare["total_commits"].as_u64().unwrap_or(0) as usize;
    if total > commits.len() || commits.len() >= 250 {
        return false;
    }
    commits
        .iter()
        .filter(|c| c["parents"].as_array().map(|p| p.len()).unwrap_or(0) <= 1)
        .all(|c| c.pointer("/committer/login").and_then(|v| v.as_str()) == Some(login))
}

#[cfg(test)]
mod rebase_tests {
    use super::*;
    use serde_json::json;

    fn commit(sha: &str, parents: &[&str], by: &str) -> serde_json::Value {
        json!({
            "sha": sha,
            "parents": parents.iter().map(|p| json!({"sha": p})).collect::<Vec<_>>(),
            "commit": {"message": format!("msg {sha}"), "author": {"name": "a", "email": "a@x", "date": "2026-01-01T00:00:00Z"}},
            "committer": {"login": by},
        })
    }

    #[test]
    fn a_rebase_replays_the_branch_commits_and_drops_merges_of_the_base() {
        let cmp = json!({
            "total_commits": 3,
            "commits": [
                commit("c1", &["b0"], "app[bot]"),
                commit("m1", &["c1", "b1"], "web-flow"),
                commit("c2", &["m1"], "app[bot]"),
            ],
        });
        let steps = replay_steps(&cmp).unwrap();
        assert_eq!(
            steps
                .iter()
                .map(|s| (s.sha.as_str(), s.parent.as_str()))
                .collect::<Vec<_>>(),
            vec![("c1", "b0"), ("c2", "m1")]
        );
        assert_eq!(steps[0].message, "msg c1");
        assert_eq!(steps[0].author["email"], "a@x");
        // Merge commits don't make a branch someone else's.
        assert!(commits_all_by(&cmp, "app[bot]"));
    }

    #[test]
    fn a_person_s_commit_or_an_unseen_one_keeps_the_history() {
        let theirs = json!({
            "total_commits": 2,
            "commits": [commit("c1", &["b0"], "app[bot]"), commit("c2", &["c1"], "someone")],
        });
        assert!(!commits_all_by(&theirs, "app[bot]"));
        let truncated = json!({
            "total_commits": 300,
            "commits": [commit("c1", &["b0"], "app[bot]")],
        });
        assert!(!commits_all_by(&truncated, "app[bot]"));
        assert!(replay_steps(&truncated).is_none());
    }
}

/// Result of a SHA-guarded merge attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    Merged,
    /// GitHub refused the merge (405); carries GitHub's message.
    NotMergeable(String),
    /// The head moved past the reviewed SHA (409); carries GitHub's message.
    HeadMoved(String),
}

/// Did a person (not a bot) convert this PR back to draft after it was last
/// marked ready? Returns their login. `timeline` is the issue timeline,
/// oldest first. A PR opened as a draft has no conversion event at all.
pub fn human_converted_to_draft(timeline: &serde_json::Value) -> Option<String> {
    let events = timeline.as_array()?;
    let last = events.iter().rev().find(|e| {
        matches!(
            e["event"].as_str(),
            Some("convert_to_draft") | Some("ready_for_review")
        )
    })?;
    if last["event"].as_str() != Some("convert_to_draft") {
        return None;
    }
    let actor = &last["actor"];
    let login = actor["login"].as_str().unwrap_or("");
    let is_bot = actor["type"].as_str() == Some("Bot")
        || login.ends_with("[bot]")
        || login.to_ascii_lowercase().contains("coderhelm");
    (!is_bot && !login.is_empty()).then(|| login.to_string())
}

#[cfg(test)]
mod human_draft_tests {
    use super::human_converted_to_draft;
    use serde_json::json;

    fn ev(event: &str, login: &str, kind: &str) -> serde_json::Value {
        json!({"event": event, "actor": {"login": login, "type": kind}})
    }

    #[test]
    fn a_person_s_draft_after_coderhelm_marked_ready_is_respected() {
        let tl = json!([
            ev("ready_for_review", "coderhelm[bot]", "Bot"),
            ev("labeled", "someone", "User"),
            ev("convert_to_draft", "alice", "User"),
        ]);
        assert_eq!(human_converted_to_draft(&tl).as_deref(), Some("alice"));
    }

    #[test]
    fn ready_after_the_person_s_draft_clears_it() {
        let tl = json!([
            ev("convert_to_draft", "alice", "User"),
            ev("ready_for_review", "alice", "User"),
        ]);
        assert_eq!(human_converted_to_draft(&tl), None);
    }

    #[test]
    fn a_bot_conversion_or_none_is_not_a_person_s_draft() {
        assert_eq!(
            human_converted_to_draft(&json!([ev("convert_to_draft", "coderhelm[bot]", "Bot")])),
            None
        );
        assert_eq!(
            human_converted_to_draft(&json!([ev("labeled", "a", "User")])),
            None
        );
    }
}

pub struct SearchResult {
    pub path: String,
    pub matches: Vec<String>,
}

/// A comment written under the first one in a review thread.
#[derive(Debug, Clone)]
pub struct ThreadReply {
    pub author: String,
    pub by_bot: bool,
    pub body: String,
}

/// One of CoderHelm's own inline review threads.
#[derive(Debug, Clone)]
pub struct BotReviewThread {
    /// GraphQL node id (what resolveReviewThread takes).
    pub id: String,
    /// The REST id of the thread's first comment (what a reply's
    /// `in_reply_to_id` names); 0 when unknown.
    pub root_id: u64,
    pub path: String,
    pub line: u64,
    /// GitHub marks a thread outdated when its anchored lines changed.
    pub outdated: bool,
    /// The bot's original comment.
    pub body: String,
    /// What was written under it, oldest first.
    pub replies: Vec<ThreadReply>,
    pub resolved: bool,
    /// A person (not CoderHelm) resolved the thread.
    pub resolved_by_person: bool,
}

/// A full 40-character hex commit SHA (as opposed to a branch name).
pub(crate) fn is_commit_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod commit_sha_tests {
    use super::is_commit_sha;

    #[test]
    fn full_hex_sha_is_a_commit_not_a_branch() {
        assert!(is_commit_sha("0123456789abcdef0123456789abcdef01234567"));
        assert!(is_commit_sha("0123456789ABCDEF0123456789ABCDEF01234567"));
        assert!(!is_commit_sha("main"));
        assert!(!is_commit_sha("coderhelm/fix-login"));
        assert!(!is_commit_sha("0123456789abcdef")); // short SHA: let GitHub resolve
        assert!(!is_commit_sha("g123456789abcdef0123456789abcdef01234567"));
    }

    #[tokio::test]
    async fn get_ref_returns_a_sha_without_calling_github() {
        // No network and no credentials: if get_ref tried the refs API this
        // would error. A SHA is already resolved.
        let client = super::GitHubClient::new("1", "", 0, &reqwest::Client::new()).unwrap();
        let sha = "ABCDEF0123456789abcdef0123456789abcdef01";
        assert_eq!(
            client.get_ref("o", "r", sha).await.unwrap(),
            sha.to_ascii_lowercase()
        );
    }
}

#[cfg(test)]
mod token_format_tests {
    /// GitHub's stateless installation tokens are ~520-char JWT-style strings
    /// (`ghs_…` with two dots). They must work as an opaque Authorization value.
    #[test]
    fn stateless_installation_token_is_a_valid_auth_header() {
        let token = format!(
            "ghs_{}.{}.{}",
            "a".repeat(40),
            "b".repeat(300),
            "c-_9".repeat(44)
        );
        assert!(token.len() >= 520);
        let v: reqwest::header::HeaderValue = format!("token {token}").parse().unwrap();
        assert_eq!(v.to_str().unwrap(), format!("token {token}"));
    }
}

#[cfg(test)]
mod status_error_tests {
    use super::*;

    #[test]
    fn error_carries_status_and_github_body() {
        let m = status_error_message(
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "https://api.github.com/repos/o/r/pulls/1/reviews",
            r#"{"message":"Unprocessable Entity","errors":["Line could not be resolved"]}"#,
        );
        assert!(m.contains("422"));
        assert!(m.contains("client error"));
        assert!(m.contains("Line could not be resolved"));
    }

    #[test]
    fn empty_body_keeps_the_status_line() {
        let m = status_error_message(reqwest::StatusCode::BAD_GATEWAY, "u", "  ");
        assert_eq!(m, "HTTP status server error (502 Bad Gateway) for url (u)");
    }

    #[test]
    fn only_a_404_reads_as_not_found() {
        let missing: Box<dyn std::error::Error + Send + Sync> =
            status_error_message(reqwest::StatusCode::NOT_FOUND, "u", "").into();
        assert!(is_not_found(&*missing));
        // A 403 (rate limit, file too large) is a failed read, never "deleted".
        let forbidden: Box<dyn std::error::Error + Send + Sync> =
            status_error_message(reqwest::StatusCode::FORBIDDEN, "u", "too_large").into();
        assert!(!is_not_found(&*forbidden));
    }
}

#[cfg(test)]
mod file_body_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn small_file_decodes_from_the_contents_api() {
        // GitHub wraps base64 at 60 columns.
        let data = json!({"size": 11, "encoding": "base64", "sha": "abc",
                          "content": "aGVsbG8g\nd29ybGQ=\n"});
        assert_eq!(
            contents_body("a.txt", &data),
            Ok(ContentsBody::Text("hello world".into()))
        );
    }

    #[test]
    fn large_file_goes_to_the_blob_api_not_an_empty_string() {
        // The prod shape: a 1.2 MB pnpm-lock.yaml comes back with
        // `encoding: none` and an empty body.
        let data = json!({"size": 1_200_000, "encoding": "none", "sha": "b10b", "content": ""});
        assert_eq!(
            contents_body("pnpm-lock.yaml", &data),
            Ok(ContentsBody::Blob("b10b".into()))
        );
        // Same when the content key is missing outright.
        let data = json!({"size": 5, "sha": "b10b"});
        assert_eq!(
            contents_body("x", &data),
            Ok(ContentsBody::Blob("b10b".into()))
        );
        // No sha to fall back to: an error, never "".
        let data = json!({"size": 5, "encoding": "none"});
        assert!(contents_body("x", &data).is_err());
    }

    #[test]
    fn a_genuinely_empty_file_reads_as_empty() {
        let data = json!({"size": 0, "encoding": "base64", "sha": "e69d", "content": ""});
        assert_eq!(
            contents_body("x", &data),
            Ok(ContentsBody::Text(String::new()))
        );
    }

    #[test]
    fn a_directory_is_an_error_not_an_empty_file() {
        assert!(contents_body("src", &json!([{"name": "a.rs"}])).is_err());
    }

    #[test]
    fn blob_decodes_and_refuses_to_come_back_empty() {
        let data = json!({"size": 11, "encoding": "base64", "content": "aGVsbG8g\nd29ybGQ="});
        assert_eq!(blob_text("x", &data).unwrap(), "hello world");
        let data = json!({"size": 3, "encoding": "utf-8", "content": "abc"});
        assert_eq!(blob_text("x", &data).unwrap(), "abc");
        let data = json!({"size": 1_200_000, "encoding": "base64", "content": ""});
        assert!(blob_text("pnpm-lock.yaml", &data).is_err());
        let data = json!({"size": 0, "encoding": "base64", "content": ""});
        assert_eq!(blob_text("x", &data).unwrap(), "");
    }

    #[test]
    fn empty_content_never_replaces_a_file_with_bytes() {
        let refusal = empty_overwrite_refusal("pnpm-lock.yaml", "", Some(1_200_000));
        assert!(refusal
            .as_deref()
            .is_some_and(|m| m.contains("pnpm-lock.yaml") && m.contains("delete it explicitly")));
        // Whitespace is as empty as nothing.
        assert!(empty_overwrite_refusal("a", " \n", Some(10)).is_some());
        // Allowed: real content, a new file, or an already-empty file.
        assert!(empty_overwrite_refusal("a", "x", Some(10)).is_none());
        assert!(empty_overwrite_refusal("a", "", None).is_none());
        assert!(empty_overwrite_refusal("a", "", Some(0)).is_none());
    }

    #[tokio::test]
    async fn non_empty_writes_skip_the_lookup() {
        // No network and no credentials: a lookup here would error.
        let client = GitHubClient::new("1", "", 0, &reqwest::Client::new()).unwrap();
        assert!(client
            .guard_empty_overwrite("o", "r", "a", "main", "content")
            .await
            .is_ok());
        // An empty write that cannot verify the target is refused, not let through.
        assert!(client
            .guard_empty_overwrite("o", "r", "a", "main", "")
            .await
            .is_err());
    }
}

#[cfg(test)]
mod reopen_tests {
    use super::closed_by_a_bot;
    use serde_json::json;

    #[test]
    fn only_a_bot_closed_pr_is_reopened() {
        assert!(closed_by_a_bot(
            &json!({"closed_by": {"login": "app[bot]", "type": "Bot"}})
        ));
        assert!(!closed_by_a_bot(
            &json!({"closed_by": {"login": "someone", "type": "User"}})
        ));
        assert!(!closed_by_a_bot(&json!({"closed_by": null})));
        assert!(!closed_by_a_bot(&json!({})));
    }
}
