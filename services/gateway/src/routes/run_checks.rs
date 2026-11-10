//! What a run's page shows about its open pull request: the CI checks on
//! the pull request's head (GitHub check runs and commit statuses, required
//! ones marked from the branch's rules), and CoderHelm's latest review
//! verdict on it.
//!
//! The checks are read from GitHub when the page asks, never stored. Pages
//! poll, and an installation has hit GitHub's hourly limit before, so every
//! read goes through short in-memory caches: the pull request's head for
//! [`PR_TTL`], the checks on a commit for [`CHECKS_TTL`] (keyed by repository
//! and commit, see [`cache_key`]), the branch's required checks for
//! [`REQUIRED_TTL`], and the installation token for [`TOKEN_TTL`]. A read
//! GitHub refused is cached as "nothing" for as long, so a limited install is
//! not asked again on every poll. Anything that can't be read is `None`: the
//! run itself is always returned.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;
use tracing::warn;

use crate::AppState;

/// How long a pull request's head and state are reused.
pub const PR_TTL: Duration = Duration::from_secs(30);
/// How long the checks on one commit are reused.
pub const CHECKS_TTL: Duration = Duration::from_secs(45);
/// How long a branch's required checks are reused.
pub const REQUIRED_TTL: Duration = Duration::from_secs(600);
/// How long an installation token is reused (GitHub's last an hour).
pub const TOKEN_TTL: Duration = Duration::from_secs(45 * 60);
/// The most GitHub may take for the whole read before the run is returned
/// without its checks.
const READ_BUDGET: Duration = Duration::from_secs(6);
/// Entries one cache holds before expired ones are dropped.
const CACHE_CAP: usize = 2_000;

const API: &str = "https://api.github.com";

/// One check on the pull request's head.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct RunCheck {
    /// The check's name (a check run's name or a commit status's context).
    #[schema(example = "build")]
    pub name: String,
    /// `passed`, `failed`, `running`, `queued` or `skipped`. A required check
    /// that hasn't reported yet is `queued`.
    #[schema(example = "passed")]
    pub state: String,
    /// GitHub's word for how it ended (`success`, `failure`, `neutral`,
    /// `cancelled`, `timed_out`, `skipped`, `error`, ...). Null until it ends.
    #[schema(example = "success")]
    pub conclusion: Option<String>,
    /// The branch's rules require it before merging. False when they can't
    /// be read.
    #[schema(example = true)]
    pub required: bool,
    /// When it started (RFC 3339), when known.
    #[schema(example = "2026-10-22T14:18:00Z")]
    pub started_at: Option<String>,
    /// When it ended (RFC 3339). Null while it runs.
    #[schema(example = "2026-10-22T14:21:40Z")]
    pub completed_at: Option<String>,
    /// Its page on GitHub or in the CI system.
    #[schema(example = "https://github.com/acme/web/actions/runs/1/job/2")]
    pub url: Option<String>,
}

/// How many checks are in each state.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct RunCheckCounts {
    /// Passed.
    #[schema(example = 5)]
    pub passed: u32,
    /// Failed, timed out or cancelled.
    #[schema(example = 0)]
    pub failed: u32,
    /// Running now.
    #[schema(example = 1)]
    pub running: u32,
    /// Waiting to start, or required and not reported yet.
    #[schema(example = 0)]
    pub queued: u32,
    /// Skipped or neutral.
    #[schema(example = 1)]
    pub skipped: u32,
}

/// The CI checks on the run's open pull request's head.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct RunChecks {
    /// The commit they ran on: the pull request's head.
    #[schema(example = "9fceb02d0ae598e95dc970b74767f19372d61af8")]
    pub sha: String,
    /// Failed first, then running, queued, passed and skipped; by name
    /// within each.
    pub items: Vec<RunCheck>,
    /// How many checks are in each state.
    pub counts: RunCheckCounts,
}

/// CoderHelm's latest review verdict on the run's pull request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct RunPrReview {
    /// `APPROVE` or `REQUEST_CHANGES`.
    #[schema(example = "APPROVE")]
    pub verdict: String,
    /// The review's key, for `/api/reviewer/review?sk=`.
    #[schema(example = "REVIEW#acme/web#000318#2026-10-22T14:20:05+00:00")]
    pub review_sk: String,
    /// The review job that wrote it (`/api/reviewer/session?id=`). Null on
    /// older reviews.
    #[schema(example = "3f2c9a1e-5b7d-4c8e-9a0b-1c2d3e4f5a6b")]
    pub session_id: Option<String>,
    /// The commit it reviewed.
    #[schema(example = "9fceb02d0ae598e95dc970b74767f19372d61af8")]
    pub reviewed_sha: String,
    /// When it was written (RFC 3339).
    #[schema(example = "2026-10-22T14:20:05+00:00")]
    pub at: String,
}

// ─── Pure mapping ───────────────────────────────────────────────────

/// Pure: a check run's state from its `status` and `conclusion`.
pub fn check_run_state(status: &str, conclusion: &str) -> &'static str {
    match status {
        "completed" => match conclusion {
            "success" => "passed",
            "neutral" | "skipped" | "stale" => "skipped",
            _ => "failed",
        },
        "in_progress" => "running",
        _ => "queued",
    }
}

/// Pure: a commit status's state from its `state`.
pub fn status_state(state: &str) -> &'static str {
    match state {
        "success" => "passed",
        "pending" => "running",
        _ => "failed",
    }
}

fn rank(state: &str) -> u8 {
    match state {
        "failed" => 0,
        "running" => 1,
        "queued" => 2,
        "passed" => 3,
        _ => 4,
    }
}

fn opt(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(str::to_string)
}

/// Pure: the checks on a commit from a `check-runs` listing and the combined
/// status's `statuses`, without `required` marked. The newest run of each
/// name counts; a status whose context a check run already has is left out.
pub fn checks_of(check_runs: &[Value], statuses: &[Value]) -> Vec<RunCheck> {
    // name -> (started_at, id, check)
    let mut runs: Vec<(String, u64, RunCheck)> = Vec::new();
    for c in check_runs {
        let Some(name) = opt(&c["name"]) else {
            continue;
        };
        let status = c["status"].as_str().unwrap_or("");
        let conclusion = c["conclusion"].as_str().unwrap_or("");
        let started = c["started_at"].as_str().unwrap_or("").to_string();
        let id = c["id"].as_u64().unwrap_or(0);
        let check = RunCheck {
            name: name.clone(),
            state: check_run_state(status, conclusion).to_string(),
            conclusion: (status == "completed")
                .then(|| opt(&c["conclusion"]))
                .flatten(),
            required: false,
            started_at: opt(&c["started_at"]),
            completed_at: opt(&c["completed_at"]),
            url: opt(&c["html_url"]).or_else(|| opt(&c["details_url"])),
        };
        match runs.iter_mut().find(|(_, _, x)| x.name == name) {
            Some((at, rid, x)) => {
                if (started.as_str(), id) >= (at.as_str(), *rid) {
                    *at = started;
                    *rid = id;
                    *x = check;
                }
            }
            None => runs.push((started, id, check)),
        }
    }
    let mut out: Vec<RunCheck> = runs.into_iter().map(|(_, _, c)| c).collect();
    for s in statuses {
        let Some(name) = opt(&s["context"]) else {
            continue;
        };
        if out.iter().any(|c| c.name == name) {
            continue;
        }
        let state = s["state"].as_str().unwrap_or("");
        out.push(RunCheck {
            name,
            state: status_state(state).to_string(),
            conclusion: (state != "pending").then(|| opt(&s["state"])).flatten(),
            required: false,
            started_at: opt(&s["created_at"]),
            completed_at: if state == "pending" {
                None
            } else {
                opt(&s["updated_at"])
            },
            url: opt(&s["target_url"]),
        });
    }
    out
}

/// Pure: the checks with the branch's required ones marked, a required
/// check that hasn't reported added as `queued`, sorted, and counted.
pub fn with_required(sha: &str, mut items: Vec<RunCheck>, required: &[String]) -> RunChecks {
    for c in items.iter_mut() {
        c.required = required.iter().any(|r| r == &c.name);
    }
    for r in required {
        if !items.iter().any(|c| &c.name == r) {
            items.push(RunCheck {
                name: r.clone(),
                state: "queued".to_string(),
                conclusion: None,
                required: true,
                started_at: None,
                completed_at: None,
                url: None,
            });
        }
    }
    items.sort_by(|a, b| {
        rank(&a.state)
            .cmp(&rank(&b.state))
            .then_with(|| a.name.cmp(&b.name))
    });
    let mut counts = RunCheckCounts::default();
    for c in &items {
        match c.state.as_str() {
            "passed" => counts.passed += 1,
            "failed" => counts.failed += 1,
            "running" => counts.running += 1,
            "queued" => counts.queued += 1,
            _ => counts.skipped += 1,
        }
    }
    RunChecks {
        sha: sha.to_string(),
        items,
        counts,
    }
}

/// Pure: the cache key of the checks on one commit of one repository.
/// GitHub names are case-insensitive; a commit id is not.
pub fn cache_key(repo: &str, sha: &str) -> String {
    format!("{}@{sha}", repo.to_ascii_lowercase())
}

/// Pure: the pull request the run's checks are for: its primary one, while
/// it is open. `None` when the run has none or it has ended.
pub fn open_pr(prs: &[common::run_prs::RunPr], run_repo: &str) -> Option<(String, u64)> {
    let pr = common::run_prs::primary(prs, run_repo)?;
    (pr.outcome.is_empty() && pr.pr_number > 0 && pr.repo.contains('/'))
        .then(|| (pr.repo.clone(), pr.pr_number))
}

/// The opening words of an old error review: not a verdict on the code.
const LEGACY_FAILED_PREFIX: &str = "Automated review could not complete";

/// Pure: the latest verdict among a PR's review records, newest first.
/// Answers to questions and old error reviews are skipped.
pub fn latest_verdict(items: &[HashMap<String, AttributeValue>]) -> Option<RunPrReview> {
    items.iter().find_map(|it| {
        let get = |k: &str| {
            it.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let verdict = get("verdict");
        if verdict != "APPROVE" && verdict != "REQUEST_CHANGES" {
            return None;
        }
        if get("body").trim_start().starts_with(LEGACY_FAILED_PREFIX) {
            return None;
        }
        Some(RunPrReview {
            verdict,
            review_sk: get("sk"),
            session_id: common::review_session::session_id_of(it),
            reviewed_sha: get("head_sha"),
            at: get("created_at"),
        })
    })
}

// ─── Caches ─────────────────────────────────────────────────────────

/// A small map whose entries expire.
pub struct TtlCache<V> {
    map: Mutex<HashMap<String, (Instant, V)>>,
}

impl<V: Clone> TtlCache<V> {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// The value under `key`, unless it expired by `now`.
    pub fn get(&self, key: &str, now: Instant) -> Option<V> {
        let map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        map.get(key)
            .filter(|(until, _)| *until > now)
            .map(|(_, v)| v.clone())
    }

    /// Keep `v` under `key` for `ttl` from `now`.
    pub fn put(&self, key: String, v: V, ttl: Duration, now: Instant) {
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() >= CACHE_CAP {
            map.retain(|_, (until, _)| *until > now);
            if map.len() >= CACHE_CAP {
                map.clear();
            }
        }
        map.insert(key, (now + ttl, v));
    }
}

impl<V: Clone> Default for TtlCache<V> {
    fn default() -> Self {
        Self::new()
    }
}

/// A pull request's state, head commit and base branch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrHead {
    open: bool,
    sha: String,
    base: String,
}

static TOKENS: LazyLock<TtlCache<String>> = LazyLock::new(TtlCache::new);
static PR_HEADS: LazyLock<TtlCache<Option<PrHead>>> = LazyLock::new(TtlCache::new);
static CHECKS: LazyLock<TtlCache<Option<Vec<RunCheck>>>> = LazyLock::new(TtlCache::new);
static REQUIRED: LazyLock<TtlCache<Vec<String>>> = LazyLock::new(TtlCache::new);

// ─── GitHub reads ───────────────────────────────────────────────────

async fn token(state: &AppState, installation_id: u64) -> Option<String> {
    let key = installation_id.to_string();
    if let Some(t) = TOKENS.get(&key, Instant::now()) {
        return Some(t);
    }
    match crate::auth::github_app::get_installation_token(state, installation_id).await {
        Ok(t) => {
            TOKENS.put(key, t.clone(), TOKEN_TTL, Instant::now());
            Some(t)
        }
        Err(e) => {
            warn!(installation_id, error = %e, "Run checks: no installation token");
            None
        }
    }
}

/// GET a GitHub API path; `None` unless it answered 200.
async fn get_ok(state: &AppState, token: &str, path: &str) -> Option<Value> {
    match super::merge_control::gh_get(state, token, &format!("{API}{path}")).await {
        Ok((200, v)) => Some(v),
        Ok((code, _)) => {
            if code != 404 {
                warn!(path, code, "Run checks: GitHub refused a read");
            }
            None
        }
        Err(e) => {
            warn!(path, error = %e, "Run checks: GitHub read failed");
            None
        }
    }
}

async fn pr_head(state: &AppState, token: &str, repo: &str, pr: u64) -> Option<PrHead> {
    let key = format!("{}#{pr}", repo.to_ascii_lowercase());
    if let Some(h) = PR_HEADS.get(&key, Instant::now()) {
        return h;
    }
    let head = get_ok(state, token, &format!("/repos/{repo}/pulls/{pr}"))
        .await
        .and_then(|v| {
            Some(PrHead {
                open: v["state"].as_str() == Some("open"),
                sha: opt(&v["head"]["sha"])?,
                base: opt(&v["base"]["ref"])?,
            })
        });
    PR_HEADS.put(key, head.clone(), PR_TTL, Instant::now());
    head
}

/// Required checks of `base` from the branch's rules and its classic
/// protection, either of which may be unreadable (then it adds nothing).
async fn required_checks(state: &AppState, token: &str, repo: &str, base: &str) -> Vec<String> {
    let key = format!("{}:{base}", repo.to_ascii_lowercase());
    if let Some(r) = REQUIRED.get(&key, Instant::now()) {
        return r;
    }
    let branch = urlencoding::encode(base);
    let rules_path = format!("/repos/{repo}/rules/branches/{branch}");
    let protection_path =
        format!("/repos/{repo}/branches/{branch}/protection/required_status_checks");
    let (rules, protection) = tokio::join!(
        get_ok(state, token, &rules_path),
        get_ok(state, token, &protection_path),
    );
    let names = common::e2e::check_gate::required(rules.as_ref(), protection.as_ref());
    REQUIRED.put(key, names.clone(), REQUIRED_TTL, Instant::now());
    names
}

async fn commit_checks(
    state: &AppState,
    token: &str,
    repo: &str,
    sha: &str,
) -> Option<Vec<RunCheck>> {
    let key = cache_key(repo, sha);
    if let Some(c) = CHECKS.get(&key, Instant::now()) {
        return c;
    }
    let runs_path = format!("/repos/{repo}/commits/{sha}/check-runs?filter=latest&per_page=100");
    let status_path = format!("/repos/{repo}/commits/{sha}/status?per_page=100");
    let (runs, status) = tokio::join!(
        get_ok(state, token, &runs_path),
        get_ok(state, token, &status_path),
    );
    // The check runs are what CI reports; without them there is no answer.
    let checks = runs.map(|r| {
        let runs = r["check_runs"].as_array().cloned().unwrap_or_default();
        let statuses = status
            .as_ref()
            .and_then(|s| s["statuses"].as_array().cloned())
            .unwrap_or_default();
        checks_of(&runs, &statuses)
    });
    CHECKS.put(key, checks.clone(), CHECKS_TTL, Instant::now());
    checks
}

async fn read_checks(
    state: &AppState,
    installation_id: u64,
    repo: &str,
    pr: u64,
) -> Option<RunChecks> {
    let token = token(state, installation_id).await?;
    let head = pr_head(state, &token, repo, pr).await?;
    if !head.open || !head.sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let (items, required) = tokio::join!(
        commit_checks(state, &token, repo, &head.sha),
        required_checks(state, &token, repo, &head.base),
    );
    Some(with_required(&head.sha, items?, &required))
}

/// The checks on the head of the run's open pull request. `None` when there
/// is none, or GitHub can't be read in time.
pub async fn for_run(
    state: &AppState,
    team_id: &str,
    run_installation_id: u64,
    repo: &str,
    pr: u64,
) -> Option<RunChecks> {
    let valid = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    let (owner, name) = repo.split_once('/')?;
    if !valid(owner) || !valid(name) || owner == ".." || name == ".." {
        return None;
    }
    let installation_id = if run_installation_id > 0 {
        run_installation_id
    } else {
        super::api::get_team_installation_id(state, team_id)
            .await
            .ok()?
    };
    tokio::time::timeout(READ_BUDGET, read_checks(state, installation_id, repo, pr))
        .await
        .unwrap_or_else(|_| {
            warn!(repo, pr, "Run checks: GitHub took too long");
            None
        })
}

/// CoderHelm's latest verdict on `repo`#`pr`, from the team's review records
/// (newest first, as the reviewer pages list them).
pub async fn pr_review(
    state: &AppState,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> Option<RunPrReview> {
    let prefix = format!("REVIEW#{repo}#{pr:0>6}#");
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    // Answers to questions sit between verdicts; two pages reach far enough.
    for _ in 0..2 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":sk", AttributeValue::S(prefix.clone()))
            .projection_expression("sk, verdict, head_sha, created_at, session_id, body")
            .scan_index_forward(false)
            .limit(25)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| warn!(repo, pr, error = %e, "Run PR review: read failed"))
            .ok()?;
        if let Some(r) = latest_verdict(resp.items()) {
            return Some(r);
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(name: &str, status: &str, conclusion: Option<&str>, started: &str, id: u64) -> Value {
        json!({
            "id": id,
            "name": name,
            "status": status,
            "conclusion": conclusion,
            "started_at": started,
            "completed_at": if status == "completed" { json!("2026-11-10T10:05:00Z") } else { Value::Null },
            "html_url": format!("https://github.com/o/r/runs/{id}"),
        })
    }

    #[test]
    fn check_runs_map_to_five_states() {
        assert_eq!(check_run_state("completed", "success"), "passed");
        for c in [
            "failure",
            "cancelled",
            "timed_out",
            "action_required",
            "startup_failure",
            "",
        ] {
            assert_eq!(check_run_state("completed", c), "failed", "{c}");
        }
        for c in ["neutral", "skipped", "stale"] {
            assert_eq!(check_run_state("completed", c), "skipped", "{c}");
        }
        assert_eq!(check_run_state("in_progress", ""), "running");
        for s in ["queued", "waiting", "requested", "pending", ""] {
            assert_eq!(check_run_state(s, ""), "queued", "{s}");
        }
        assert_eq!(status_state("success"), "passed");
        assert_eq!(status_state("pending"), "running");
        assert_eq!(status_state("failure"), "failed");
        assert_eq!(status_state("error"), "failed");
    }

    #[test]
    fn counts_cover_every_state_and_required_checks_are_marked() {
        let runs = vec![
            run(
                "build",
                "completed",
                Some("success"),
                "2026-11-10T10:00:00Z",
                1,
            ),
            // An older failed run of the same name is replaced by the rerun.
            run(
                "test",
                "completed",
                Some("failure"),
                "2026-11-10T09:00:00Z",
                2,
            ),
            run("test", "in_progress", None, "2026-11-10T10:01:00Z", 3),
            run(
                "lint",
                "completed",
                Some("failure"),
                "2026-11-10T10:00:00Z",
                4,
            ),
            run(
                "deploy",
                "completed",
                Some("skipped"),
                "2026-11-10T10:00:00Z",
                5,
            ),
            run("e2e", "queued", None, "", 6),
            json!({"name": "", "status": "completed"}),
        ];
        let statuses = vec![
            json!({"context": "ci/legacy", "state": "success", "target_url": "https://ci.example.com/1", "created_at": "2026-11-10T10:00:00Z", "updated_at": "2026-11-10T10:02:00Z"}),
            json!({"context": "ci/pending", "state": "pending", "created_at": "2026-11-10T10:00:00Z", "updated_at": "2026-11-10T10:00:00Z"}),
            // A status named like a check run is left to the check run.
            json!({"context": "build", "state": "failure"}),
        ];
        let items = checks_of(&runs, &statuses);
        assert_eq!(items.len(), 7);
        let required = vec![
            "build".to_string(),
            "test".to_string(),
            "security".to_string(),
        ];
        let c = with_required("abc", items, &required);
        assert_eq!(c.sha, "abc");
        assert_eq!(
            c.counts,
            RunCheckCounts {
                passed: 2,
                failed: 1,
                running: 2,
                queued: 2,
                skipped: 1,
            }
        );
        let by = |n: &str| c.items.iter().find(|i| i.name == n).unwrap();
        assert!(by("build").required && by("test").required);
        assert!(!by("lint").required && !by("ci/legacy").required);
        assert_eq!(by("test").state, "running");
        assert_eq!(by("test").conclusion, None);
        assert_eq!(by("test").completed_at, None);
        assert_eq!(by("lint").conclusion.as_deref(), Some("failure"));
        assert_eq!(
            by("build").url.as_deref(),
            Some("https://github.com/o/r/runs/1")
        );
        assert_eq!(
            by("ci/legacy").url.as_deref(),
            Some("https://ci.example.com/1")
        );
        assert_eq!(by("ci/legacy").conclusion.as_deref(), Some("success"));
        assert_eq!(
            by("ci/legacy").completed_at.as_deref(),
            Some("2026-11-10T10:02:00Z")
        );
        assert_eq!(by("ci/pending").completed_at, None);
        assert_eq!(by("deploy").state, "skipped");
        // A required check that never reported waits as queued.
        let sec = by("security");
        assert_eq!(
            (sec.state.as_str(), sec.required, sec.url.clone()),
            ("queued", true, None)
        );
        // Failed first.
        assert_eq!(c.items[0].name, "lint");
        assert_eq!(c.items.last().unwrap().name, "deploy");
    }

    #[test]
    fn unreadable_branch_rules_leave_every_check_optional() {
        let items = checks_of(&[run("build", "completed", Some("success"), "t", 1)], &[]);
        let c = with_required("abc", items, &[]);
        assert!(c.items.iter().all(|i| !i.required));
        assert_eq!(c.counts.passed, 1);
        // No checks at all is an empty list, not an error.
        let none = with_required("abc", checks_of(&[], &[]), &[]);
        assert!(none.items.is_empty());
        assert_eq!(none.counts, RunCheckCounts::default());
    }

    #[test]
    fn the_cache_key_is_the_repository_and_the_commit() {
        assert_eq!(cache_key("Acme/Web", "9fceb02"), "acme/web@9fceb02");
        assert_eq!(
            cache_key("acme/web", "9fceb02"),
            cache_key("ACME/WEB", "9fceb02")
        );
        assert_ne!(
            cache_key("acme/web", "9fceb02"),
            cache_key("acme/web", "9FCEB02")
        );
        assert_ne!(cache_key("acme/web", "a"), cache_key("acme/api", "a"));
    }

    #[test]
    fn the_cache_forgets_after_its_ttl() {
        let cache: TtlCache<Option<u32>> = TtlCache::new();
        let t0 = Instant::now();
        assert_eq!(cache.get("k", t0), None);
        cache.put("k".into(), Some(7), CHECKS_TTL, t0);
        assert_eq!(cache.get("k", t0 + Duration::from_secs(44)), Some(Some(7)));
        assert_eq!(cache.get("k", t0 + CHECKS_TTL), None);
        // A refused read is remembered as nothing, so it isn't asked again.
        cache.put("gone".into(), None, CHECKS_TTL, t0);
        assert_eq!(cache.get("gone", t0), Some(None));
    }

    #[test]
    fn only_an_open_pull_request_gets_checks() {
        use common::run_prs::RunPr;
        let pr = |repo: &str, n: u64, outcome: &str| RunPr {
            repo: repo.into(),
            pr_number: n,
            branch: "b".into(),
            url: String::new(),
            outcome: outcome.into(),
        };
        assert_eq!(open_pr(&[], "acme/web"), None);
        assert_eq!(
            open_pr(&[pr("acme/api", 3, ""), pr("acme/web", 7, "")], "acme/web"),
            Some(("acme/web".into(), 7))
        );
        assert_eq!(open_pr(&[pr("acme/web", 7, "merged")], "acme/web"), None);
        assert_eq!(open_pr(&[pr("acme/web", 7, "closed")], "acme/web"), None);
        assert_eq!(open_pr(&[pr("acme/web", 0, "")], "acme/web"), None);
    }

    #[test]
    fn the_latest_verdict_skips_answers_and_error_reviews() {
        let rec = |sk: &str, verdict: &str, body: &str, session: Option<&str>| {
            let mut m: HashMap<String, AttributeValue> = [
                ("sk", sk),
                ("verdict", verdict),
                ("body", body),
                ("head_sha", "abc"),
                ("created_at", "2026-11-10T10:00:00+00:00"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect();
            if let Some(s) = session {
                m.insert("session_id".into(), AttributeValue::S(s.into()));
            }
            m
        };
        let job = "3f2c9a1e-5b7d-4c8e-9a0b-1c2d3e4f5a6b";
        let items = vec![
            rec("REVIEW#a/b#000007#4", "QUESTION", "An answer", Some(job)),
            rec(
                "REVIEW#a/b#000007#3",
                "REQUEST_CHANGES",
                "Automated review could not complete: x",
                None,
            ),
            rec("REVIEW#a/b#000007#2", "APPROVE", "Fine", Some(job)),
            rec("REVIEW#a/b#000007#1", "REQUEST_CHANGES", "Fix it", None),
        ];
        let r = latest_verdict(&items).unwrap();
        assert_eq!(
            r,
            RunPrReview {
                verdict: "APPROVE".into(),
                review_sk: "REVIEW#a/b#000007#2".into(),
                session_id: Some(job.into()),
                reviewed_sha: "abc".into(),
                at: "2026-11-10T10:00:00+00:00".into(),
            }
        );
        // An older review has no session.
        assert_eq!(latest_verdict(&items[3..]).unwrap().session_id, None);
        // Only answers: no verdict.
        assert_eq!(latest_verdict(&items[..1]), None);
        assert_eq!(latest_verdict(&[]), None);
    }
}
