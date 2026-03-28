use base64::{engine::general_purpose::STANDARD as B64, Engine};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

const API_BASE: &str = "https://api.github.com";

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

    // ─── Tree / File reads ──────────────────────────────────────

    /// Get the full recursive file tree.
    pub async fn get_tree(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees/{git_ref}?recursive=1");
        let data = self.get(&url).await?;
        let tree: Vec<TreeEntry> = serde_json::from_value(
            data.get("tree").cloned().unwrap_or(serde_json::json!([])),
        )?;
        Ok(tree)
    }

    /// Read a single file (base64 decoded).
    pub async fn read_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        let data = self.get(&url).await?;
        let content = data
            .get("content")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        let clean = content.replace('\n', "");
        let bytes = B64.decode(&clean)?;
        Ok(String::from_utf8(bytes)?)
    }

    /// List directory contents.
    pub async fn list_directory(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> Result<Vec<DirEntry>, Box<dyn std::error::Error + Send + Sync>> {
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
        let data = self
            .post(
                &url,
                &serde_json::json!({
                    "ref": format!("refs/heads/{branch}"),
                    "sha": sha,
                }),
            )
            .await?;
        let new_sha = data
            .pointer("/object/sha")
            .and_then(|v| v.as_str())
            .ok_or("Missing sha in create branch response")?;
        Ok(new_sha.to_string())
    }

    // ─── Single file write ──────────────────────────────────────

    /// Create or update a single file via Contents API.
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
        self.put(&url, &body).await
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

    // ─── Pull requests ─────────────────────────────────────────

    /// Create a pull request.
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

    /// Mark a draft PR as ready for review.
    pub async fn mark_pr_ready(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}");
        self.patch(&url, &serde_json::json!({"draft": false}))
            .await
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

    /// Check if a path exists in the repo.
    pub async fn check_path_exists(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
    ) -> bool {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}");
        self.get(&url).await.is_ok()
    }
}

// ─── Types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub sha: String,
}

#[derive(Debug, Deserialize)]
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
