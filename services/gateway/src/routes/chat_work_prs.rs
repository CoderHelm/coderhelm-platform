//! Chat's pull request lookup: the team's pull requests and branches, found
//! through the GitHub installation by ticket key, title words, author or
//! number, in the team's enabled repositories only.
//!
//! GitHub's budget is the installation's (`common::github_budget`): Chat's
//! requests are foreground work, so they go unless GitHub said the hour's
//! limit is spent, which the installation's record on the analytics table
//! says (written by the worker). Every request is conditional: a reply kept
//! from an earlier lookup is sent back with its ETag, and an unchanged one
//! (304) costs nothing. Requests per lookup are bounded ([`MAX_REQUESTS`]).

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use common::mcp_remote::http::{Http, HttpRequest, Limit};
use serde_json::Value;

use super::chat_work::{clip, words_of, Found};

/// Most GitHub requests one lookup makes.
pub const MAX_REQUESTS: usize = 14;
/// Most pull requests one lookup returns.
pub const MAX_RESULTS: usize = 8;
/// Pull requests read in full (checks, reviews) among those returned.
pub const MAX_DETAILED: usize = 3;
/// Repositories a number or branch is looked for in, without a search.
pub const MAX_REPO_PROBES: usize = 6;
/// Longest search query GitHub takes, kept under.
const MAX_QUERY: usize = 240;
/// Replies kept for conditional requests.
const CACHE_SIZE: usize = 400;
/// Largest reply kept for a conditional request. A search reply carries each
/// pull request's whole description (a security bundle's runs to 64 KB), so
/// one page of them is read whole but not kept.
const CACHE_MAX_BYTES: usize = 128 * 1024;
/// How much of a reply is read: eight pull requests whose descriptions are
/// GitHub's longest, with room for their labels and links.
const REPLY_LIMIT: Limit = Limit::Mcp;

// Eight hits whose descriptions are GitHub's longest (65,536 characters, up
// to 4 bytes each), as a security bundle's are, fit in one read: the
// metadata cap (256 KB) refused that page, so every title search that met
// them failed. The kept replies stay small.
const _: () = {
    assert!(REPLY_LIMIT.max_bytes() >= MAX_RESULTS * 65_536 * 4 + 64 * 1024);
    assert!(CACHE_MAX_BYTES * CACHE_SIZE <= 64 * 1024 * 1024);
};

/// Replies by URL with their ETag, shared by every lookup in this process.
static CACHE: LazyLock<Mutex<HashMap<String, (String, Value)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// What the model asked for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrAsk {
    pub ticket: Option<String>,
    pub number: Option<u64>,
    pub author: Option<String>,
    /// One of the team's repositories (`owner/name`), checked.
    pub repo: Option<String>,
    /// `open`, `closed`, `merged` or empty for any.
    pub state: String,
    pub words: Vec<String>,
}

fn safe_login(s: &str) -> bool {
    !s.is_empty() && s.len() <= 39 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn safe_key(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 40
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'#')
}

impl PrAsk {
    /// The ask, with the repository resolved among the team's (`None` when
    /// it names one the team doesn't have: nothing outside is searched).
    pub fn from_input(input: &Value, team_repos: &[String]) -> Result<PrAsk, String> {
        let s = |k: &str| {
            input[k]
                .as_str()
                .map(str::trim)
                .filter(|v| !v.is_empty() && v.len() <= 200)
                .map(str::to_string)
        };
        let repo = match s("repo") {
            Some(r) => Some(
                resolve_repo(&r, team_repos)
                    .ok_or_else(|| format!("{r} isn't one of the team's repositories."))?,
            ),
            None => None,
        };
        Ok(PrAsk {
            ticket: s("ticket").filter(|t| safe_key(t)),
            number: input["number"]
                .as_u64()
                .or_else(|| s("number").and_then(|n| n.trim_start_matches('#').parse().ok())),
            author: s("author").filter(|a| safe_login(a)),
            repo,
            state: s("state")
                .map(|v| v.to_ascii_lowercase())
                .filter(|v| ["open", "closed", "merged"].contains(&v.as_str()))
                .unwrap_or_default(),
            words: s("query").map(|q| words_of(&q)).unwrap_or_default(),
        })
    }
}

/// `owner/name` or a short name, as one of the team's repositories.
pub fn resolve_repo(r: &str, team_repos: &[String]) -> Option<String> {
    let r = r.trim().to_lowercase();
    team_repos
        .iter()
        .find(|t| {
            let t = t.to_lowercase();
            t == r || t.rsplit('/').next() == Some(r.as_str())
        })
        .cloned()
}

/// The search queries for an ask: the terms once, the repositories split so
/// each query stays under GitHub's length. At most three.
pub fn search_queries(ask: &PrAsk, repos: &[String]) -> Vec<String> {
    let mut terms: Vec<String> = vec!["is:pr".into()];
    match ask.state.as_str() {
        "open" => terms.push("is:open".into()),
        "closed" => terms.push("is:closed".into()),
        "merged" => terms.push("is:merged".into()),
        _ => {}
    }
    if let Some(a) = &ask.author {
        terms.push(format!("author:{a}"));
    }
    if let Some(t) = &ask.ticket {
        terms.push(format!("\"{t}\""));
    }
    terms.extend(ask.words.iter().take(6).cloned());
    let head = terms.join(" ");
    let mut out: Vec<String> = Vec::new();
    let mut cur = head.clone();
    for r in repos {
        let q = format!(" repo:{r}");
        if cur.len() + q.len() > MAX_QUERY && cur != head {
            out.push(std::mem::replace(&mut cur, head.clone()));
        }
        cur.push_str(&q);
    }
    if cur != head {
        out.push(cur);
    }
    out.truncate(3);
    out
}

/// One pull request as a citable record, from a search hit or the pull
/// request itself, with its checks and reviews when they were read.
pub fn record(repo: &str, pr: &Value, checks: Option<&Value>, reviews: Option<&Value>) -> Found {
    let number = pr["number"].as_u64().unwrap_or(0);
    let title = pr["title"].as_str().unwrap_or("");
    let merged = pr["merged_at"].as_str().is_some_and(|m| !m.is_empty())
        || pr["pull_request"]["merged_at"]
            .as_str()
            .is_some_and(|m| !m.is_empty());
    let state = if merged {
        "merged"
    } else {
        pr["state"].as_str().unwrap_or("unknown")
    };
    let draft = pr["draft"].as_bool().unwrap_or(false);
    let url = pr["html_url"].as_str().unwrap_or("").to_string();
    let mut lines = vec![format!(
        "Pull request #{number} in {repo}: {state}{}",
        if draft { " (draft)" } else { "" }
    )];
    lines.push(format!("Title: {}", clip(title, 200)));
    if let Some(a) = pr["user"]["login"].as_str() {
        lines.push(format!("Author: {a}"));
    }
    if let Some(b) = pr["head"]["ref"].as_str() {
        lines.push(format!("Branch: {b}"));
    }
    for (label, k) in [
        ("Opened", "created_at"),
        ("Last activity", "updated_at"),
        ("Merged", "merged_at"),
        ("Closed", "closed_at"),
    ] {
        if let Some(t) = pr[k].as_str().filter(|t| !t.is_empty()) {
            lines.push(format!("{label}: {t}"));
        }
    }
    if let Some(c) = checks {
        lines.push(format!("Checks: {}", checks_line(c)));
    }
    if let Some(r) = reviews {
        lines.push(format!("Reviews: {}", reviews_line(r)));
    }
    let body = pr["body"].as_str().unwrap_or("");
    if !body.trim().is_empty() {
        lines.push(format!("Description (start): {}", clip(body, 500)));
    }
    if !url.is_empty() {
        lines.push(format!("Link: {url}"));
    }
    Found {
        kind: "pull_request",
        title: format!("#{number} {}", clip(title, 100)),
        loc: repo.to_string(),
        as_of: pr["updated_at"].as_str().unwrap_or("").to_string(),
        passage: lines.join("\n"),
        href: None,
        url: common::knowledge::answer::original_url(&url),
    }
}

/// "3 passed, 1 failed (lint), 1 running" from check runs.
pub fn checks_line(v: &Value) -> String {
    let runs = v["check_runs"].as_array().cloned().unwrap_or_default();
    if runs.is_empty() {
        return "none reported".into();
    }
    let (mut ok, mut running, mut failed) = (0, 0, Vec::new());
    for r in &runs {
        match (r["status"].as_str(), r["conclusion"].as_str()) {
            (Some("completed"), Some("success" | "neutral" | "skipped")) => ok += 1,
            (Some("completed"), _) => failed.push(r["name"].as_str().unwrap_or("?").to_string()),
            _ => running += 1,
        }
    }
    let mut parts = vec![format!("{ok} passed")];
    if !failed.is_empty() {
        failed.truncate(5);
        parts.push(format!("{} failed ({})", failed.len(), failed.join(", ")));
    }
    if running > 0 {
        parts.push(format!("{running} running"));
    }
    parts.join(", ")
}

/// Each reviewer's latest verdict: "alice approved, bob asked for changes".
pub fn reviews_line(v: &Value) -> String {
    let mut latest: Vec<(String, String)> = Vec::new();
    for r in v.as_array().into_iter().flatten() {
        let who = r["user"]["login"].as_str().unwrap_or("?").to_string();
        let state = match r["state"].as_str().unwrap_or("") {
            "APPROVED" => "approved",
            "CHANGES_REQUESTED" => "asked for changes",
            "COMMENTED" => "commented",
            "DISMISSED" => "was dismissed",
            _ => continue,
        };
        latest.retain(|(w, _)| *w != who);
        latest.push((who, state.to_string()));
    }
    if latest.is_empty() {
        return "none yet".into();
    }
    latest
        .iter()
        .map(|(w, s)| format!("{w} {s}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The repository of a search hit (`.../repos/owner/name`).
fn repo_of_hit(hit: &Value) -> String {
    hit["repository_url"]
        .as_str()
        .and_then(|u| u.split("/repos/").nth(1))
        .unwrap_or("")
        .to_string()
}

/// GitHub through the installation, bounded and conditional.
pub struct Client<'a> {
    pub http: &'a dyn Http,
    pub auth: String,
    pub base: String,
    pub sent: usize,
    /// GitHub said the hour's limit is spent: why, and nothing more is sent.
    pub held: Option<String>,
}

impl Client<'_> {
    pub async fn get(&mut self, path: &str) -> Result<Option<Value>, String> {
        if let Some(why) = &self.held {
            return Err(why.clone());
        }
        if self.sent >= MAX_REQUESTS {
            return Err("this lookup made its GitHub requests".into());
        }
        let url = format!("{}{path}", self.base);
        let cached = CACHE.lock().ok().and_then(|c| c.get(&url).cloned());
        let mut req = HttpRequest::get(&url)
            .header("Authorization", &self.auth)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        req.limit = REPLY_LIMIT;
        if let Some((etag, _)) = &cached {
            req = req.header("If-None-Match", etag);
        }
        self.sent += 1;
        let r = self
            .http
            .send(req)
            .await
            .map_err(|e| format!("GitHub couldn't be reached: {e}"))?;
        match r.status {
            304 => Ok(cached.map(|(_, v)| v)),
            200 => {
                let v = r.json().ok_or("GitHub sent something unreadable")?;
                if let Some(etag) = r.header("etag").filter(|_| r.body.len() <= CACHE_MAX_BYTES) {
                    if let Ok(mut c) = CACHE.lock() {
                        if c.len() >= CACHE_SIZE {
                            c.clear();
                        }
                        c.insert(url, (etag.to_string(), v.clone()));
                    }
                }
                Ok(Some(v))
            }
            404 | 422 => Ok(None),
            403 | 429 if r.header("x-ratelimit-remaining") == Some("0") || r.status == 429 => {
                let why = "GitHub's rate limit is spent for now".to_string();
                self.held = Some(why.clone());
                Err(why)
            }
            s => Err(format!("GitHub answered {s}")),
        }
    }
}

/// The pull requests (and Coderhelm branches) that match, newest activity
/// first. `repos`: the team's enabled repositories; nothing else is read.
///
/// `known`: pull requests the team's matching runs opened (`owner/name`,
/// number), read first and directly, so a title finds its pull request
/// even when GitHub's search can't be read. Ones outside `repos` are skipped.
pub async fn find(
    client: &mut Client<'_>,
    ask: &PrAsk,
    repos: &[String],
    known: &[(String, u64)],
) -> Result<Vec<Found>, String> {
    let scope: Vec<String> = match &ask.repo {
        Some(r) => vec![r.clone()],
        None => repos.to_vec(),
    };
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut hits: Vec<(String, Value)> = Vec::new();
    if let Some(n) = ask.number {
        for r in scope.iter().take(MAX_REPO_PROBES) {
            if let Some(pr) = client.get(&format!("/repos/{r}/pulls/{n}")).await? {
                hits.push((r.clone(), pr));
            }
        }
    } else {
        let wanted = |repo: &str| scope.iter().find(|r| r.eq_ignore_ascii_case(repo)).cloned();
        for (repo, n) in known.iter().take(MAX_DETAILED) {
            if let Some(r) = wanted(repo) {
                if let Some(pr) = client.get(&format!("/repos/{r}/pulls/{n}")).await? {
                    hits.push((r, pr));
                }
            }
        }
        let mut search_failed: Option<String> = None;
        for q in search_queries(ask, &scope) {
            let path = format!(
                "/search/issues?q={}&sort=updated&order=desc&per_page={MAX_RESULTS}",
                urlencoding::encode(&q)
            );
            match client.get(&path).await {
                Ok(Some(v)) => {
                    for h in v["items"].as_array().into_iter().flatten() {
                        let repo = repo_of_hit(h);
                        // Only the team's repositories, whatever the search sent.
                        if let Some(r) = wanted(&repo) {
                            hits.push((r, h.clone()));
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    search_failed = Some(e);
                    break;
                }
            }
        }
        // What the runs found stands; a failed search alone is a failure.
        if let Some(e) = search_failed {
            if hits.is_empty() {
                return Err(e);
            }
            tracing::warn!(error = %e, found = hits.len(), "Chat: pull request search failed; kept what the runs opened");
        }
    }
    // One entry per pull request (a run's and the search's hit are the same
    // one; the full pull request comes first and is kept), newest first.
    let mut seen: Vec<(String, u64)> = Vec::new();
    hits.retain(|(r, h)| {
        let k = (r.to_lowercase(), h["number"].as_u64().unwrap_or(0));
        let fresh = !seen.contains(&k);
        seen.push(k);
        fresh
    });
    hits.sort_by(|a, b| b.1["updated_at"].as_str().cmp(&a.1["updated_at"].as_str()));
    hits.truncate(MAX_RESULTS);
    let mut out = Vec::new();
    for (i, (repo, hit)) in hits.iter().enumerate() {
        let n = hit["number"].as_u64().unwrap_or(0);
        if i >= MAX_DETAILED || n == 0 {
            out.push(record(repo, hit, None, None));
            continue;
        }
        // The pull request itself (head, merged), its checks and reviews.
        let pr = if hit["head"].is_object() {
            Some(hit.clone())
        } else {
            client
                .get(&format!("/repos/{repo}/pulls/{n}"))
                .await
                .ok()
                .flatten()
        };
        let pr = pr.unwrap_or_else(|| hit.clone());
        let checks = match pr["head"]["sha"].as_str() {
            Some(sha) => client
                .get(&format!(
                    "/repos/{repo}/commits/{sha}/check-runs?per_page=50"
                ))
                .await
                .ok()
                .flatten(),
            None => None,
        };
        let reviews = client
            .get(&format!("/repos/{repo}/pulls/{n}/reviews?per_page=50"))
            .await
            .ok()
            .flatten();
        out.push(record(repo, &pr, checks.as_ref(), reviews.as_ref()));
    }
    // Coderhelm's branches for the ticket that have no pull request yet.
    if let Some(t) = &ask.ticket {
        let prefix = format!("coderhelm/{}", t.to_lowercase());
        for r in scope.iter().take(MAX_REPO_PROBES) {
            let Ok(Some(v)) = client
                .get(&format!("/repos/{r}/git/matching-refs/heads/{prefix}"))
                .await
            else {
                continue;
            };
            for b in v.as_array().into_iter().flatten().take(3) {
                let name = b["ref"]
                    .as_str()
                    .unwrap_or("")
                    .trim_start_matches("refs/heads/");
                if name.is_empty()
                    || out
                        .iter()
                        .any(|f| f.passage.contains(&format!("Branch: {name}")))
                {
                    continue;
                }
                let url = format!("https://github.com/{r}/tree/{name}");
                out.push(Found {
                    kind: "pull_request",
                    title: format!("Branch {name}"),
                    loc: r.clone(),
                    as_of: super::chat_work::live_stamp(),
                    passage: format!(
                        "Branch {name} in {r}, with no pull request found for it.\nLink: {url}"
                    ),
                    href: None,
                    url: common::knowledge::answer::original_url(&url),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use common::mcp_remote::http::{HttpError, HttpResponse};
    use serde_json::json;

    /// How the fake answers a request: status and JSON body.
    pub type Answer = Box<dyn Fn(&HttpRequest) -> (u16, Value) + Send + Sync>;

    /// GitHub as a function of the URL; records what was sent.
    pub struct FakeGitHub {
        pub sent: Mutex<Vec<HttpRequest>>,
        pub answer: Answer,
    }

    #[async_trait::async_trait]
    impl Http for FakeGitHub {
        async fn send(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            self.sent.lock().unwrap().push(req.clone());
            let (status, v) = (self.answer)(&req);
            Ok(HttpResponse {
                status,
                headers: vec![("etag".into(), format!("\"{}\"", req.url.len()))],
                body: v.to_string().into_bytes(),
            })
        }
    }

    pub fn fake(f: impl Fn(&HttpRequest) -> (u16, Value) + Send + Sync + 'static) -> FakeGitHub {
        FakeGitHub {
            sent: Mutex::new(Vec::new()),
            answer: Box::new(f),
        }
    }

    pub fn client(http: &dyn Http) -> Client<'_> {
        Client {
            http,
            auth: "Bearer t".into(),
            base: "https://gh.test".into(),
            sent: 0,
            held: None,
        }
    }

    fn repos() -> Vec<String> {
        vec!["acme/web".into(), "acme/api".into()]
    }

    #[test]
    fn a_repository_outside_the_team_is_refused() {
        let e = PrAsk::from_input(&json!({"repo": "other/secret"}), &repos()).unwrap_err();
        assert!(e.contains("isn't one of the team's"));
        let a = PrAsk::from_input(&json!({"repo": "web"}), &repos()).unwrap();
        assert_eq!(a.repo.as_deref(), Some("acme/web"));
        // Search operators can't be smuggled in through the author or ticket.
        let a = PrAsk::from_input(
            &json!({"author": "x repo:other/secret", "ticket": "A-1 repo:x"}),
            &repos(),
        )
        .unwrap();
        assert!(a.author.is_none() && a.ticket.is_none());
    }

    #[test]
    fn queries_name_only_the_teams_repositories_and_stay_short() {
        let many: Vec<String> = (0..40)
            .map(|i| format!("acme/repository-number-{i}"))
            .collect();
        let a = PrAsk {
            ticket: Some("ABC-7".into()),
            ..Default::default()
        };
        let qs = search_queries(&a, &many);
        assert!(qs.len() <= 3);
        for q in &qs {
            assert!(q.len() <= MAX_QUERY + 40, "{q}");
            assert!(q.starts_with("is:pr \"ABC-7\""));
        }
    }

    #[tokio::test]
    async fn search_hits_outside_the_team_are_dropped_and_requests_are_bounded() {
        let gh = fake(|r| {
            if r.url.contains("/search/issues") {
                (
                    200,
                    json!({"items": [
                        {"number": 5, "title": "Embed portal", "state": "open", "updated_at": "2026-11-10T00:00:00Z",
                         "repository_url": "https://api.github.com/repos/acme/web", "html_url": "https://github.com/acme/web/pull/5"},
                        {"number": 9, "title": "Not ours", "state": "open", "updated_at": "2026-11-11T00:00:00Z",
                         "repository_url": "https://api.github.com/repos/other/secret", "html_url": "https://github.com/other/secret/pull/9"}
                    ]}),
                )
            } else if r.url.ends_with("/pulls/5") {
                (
                    200,
                    json!({"number": 5, "title": "Embed portal", "state": "open", "updated_at": "2026-11-10T00:00:00Z",
                    "head": {"sha": "abc", "ref": "coderhelm/abc-7"}, "html_url": "https://github.com/acme/web/pull/5", "user": {"login": "dev"}}),
                )
            } else if r.url.contains("/check-runs") {
                (
                    200,
                    json!({"check_runs": [
                    {"name": "build", "status": "completed", "conclusion": "success"},
                    {"name": "lint", "status": "completed", "conclusion": "failure"}]}),
                )
            } else if r.url.contains("/reviews") {
                (
                    200,
                    json!([{"user": {"login": "amy"}, "state": "APPROVED"}]),
                )
            } else {
                (404, json!({}))
            }
        });
        let mut c = client(&gh);
        let a = PrAsk {
            ticket: Some("ABC-7".into()),
            ..Default::default()
        };
        let out = find(&mut c, &a, &repos(), &[]).await.unwrap();
        let prs: Vec<&Found> = out.iter().filter(|f| f.title.starts_with('#')).collect();
        assert_eq!(prs.len(), 1, "{out:?}");
        let p = prs[0];
        assert!(
            p.passage.contains("Checks: 1 passed, 1 failed (lint)"),
            "{}",
            p.passage
        );
        assert!(p.passage.contains("Reviews: amy approved"));
        assert_eq!(p.url.as_deref(), Some("https://github.com/acme/web/pull/5"));
        assert!(c.sent <= MAX_REQUESTS);
        assert!(gh
            .sent
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.url.contains("other/secret")));
    }

    #[tokio::test]
    async fn a_title_finds_the_pull_request_its_run_opened_when_search_fails() {
        let gh = fake(|r| {
            if r.url.contains("/search/issues") {
                (403, json!({"message": "refused"}))
            } else if r.url.ends_with("/repos/acme/api/pulls/609") {
                (
                    200,
                    json!({"number": 609, "title": "fix(deps): security updates for api (3 packages, 4 advisories)",
                    "state": "open", "updated_at": "2026-11-14T00:00:00Z", "head": {"sha": "s1", "ref": "coderhelm/sec-1"},
                    "html_url": "https://github.com/acme/api/pull/609"}),
                )
            } else if r.url.contains("/check-runs") {
                (200, json!({"check_runs": []}))
            } else if r.url.contains("/reviews") {
                (200, json!([]))
            } else {
                (404, json!({}))
            }
        });
        let mut c = client(&gh);
        let a = PrAsk::from_input(
            &json!({"query": "fix(deps): security updates for api"}),
            &repos(),
        )
        .unwrap();
        // The run's pull request, and one in a repository the team doesn't have.
        let known = vec![
            ("acme/api".to_string(), 609),
            ("other/secret".to_string(), 1),
        ];
        let out = find(&mut c, &a, &repos(), &known).await.unwrap();
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].title.starts_with("#609 "));
        assert!(gh
            .sent
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.url.contains("other/secret")));
        // With nothing from the runs, the failed search is the answer.
        let mut c = client(&gh);
        let e = find(&mut c, &a, &repos(), &[]).await.unwrap_err();
        assert!(e.contains("403"), "{e}");
    }

    #[tokio::test]
    async fn a_pull_request_found_twice_is_listed_once() {
        let gh = fake(|r| {
            if r.url.contains("/search/issues") {
                (
                    200,
                    json!({"items": [{"number": 7, "title": "Bump", "state": "open", "updated_at": "2026-11-14T00:00:00Z",
                        "repository_url": "https://api.github.com/repos/acme/web", "html_url": "https://github.com/acme/web/pull/7"}]}),
                )
            } else if r.url.ends_with("/pulls/7") {
                (
                    200,
                    json!({"number": 7, "title": "Bump", "state": "open", "updated_at": "2026-11-14T00:00:00Z",
                    "head": {"sha": "s"}, "html_url": "https://github.com/acme/web/pull/7"}),
                )
            } else {
                (200, json!([]))
            }
        });
        let mut c = client(&gh);
        let a = PrAsk::from_input(&json!({"query": "bump"}), &repos()).unwrap();
        let out = find(&mut c, &a, &repos(), &[("ACME/web".into(), 7)])
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "{out:?}");
    }

    #[tokio::test]
    async fn an_unchanged_reply_is_read_from_the_cache() {
        let gh = fake(|r| {
            if r.header_value("If-None-Match").is_some() {
                (304, json!(null))
            } else {
                (
                    200,
                    json!({"number": 3, "title": "t", "state": "closed", "merged_at": "2026-11-01T00:00:00Z",
                    "html_url": "https://github.com/acme/api/pull/3"}),
                )
            }
        });
        let path = "/repos/acme/api/pulls/3-cache-test";
        let mut c = client(&gh);
        let first = c.get(path).await.unwrap().unwrap();
        let second = c.get(path).await.unwrap().unwrap();
        assert_eq!(first, second);
        let sent = gh.sent.lock().unwrap();
        assert!(sent[1].header_value("If-None-Match").is_some());
    }

    #[tokio::test]
    async fn a_spent_limit_sends_nothing_more() {
        let gh = fake(|_| (429, json!({})));
        let mut c = client(&gh);
        assert!(c.get("/a").await.is_err());
        assert!(c.get("/b").await.is_err());
        assert_eq!(gh.sent.lock().unwrap().len(), 1);
    }
}
