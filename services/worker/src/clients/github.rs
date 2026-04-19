use base64::{engine::general_purpose::STANDARD as B64, Engine};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

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

    /// Get the full recursive file tree.
    pub async fn get_tree(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
    ) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{API_BASE}/repos/{owner}/{repo}/git/trees/{git_ref}?recursive=1");
        let data = self.get(&url).await?;
        let tree: Vec<TreeEntry> =
            serde_json::from_value(data.get("tree").cloned().unwrap_or(serde_json::json!([])))?;
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

    /// Search code in a repository using GitHub Code Search API.
    /// Returns up to 10 results with file path and matching text fragments.
    pub async fn search_code(
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
            201 | 204 => Ok(true), // merged or already up-to-date
            409 => Ok(false),      // conflict
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
        let jobs_url = format!(
            "{API_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs?filter=latest"
        );
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
