//! The Tests setup page: per repo, what is already in place to send E2E
//! results, what has arrived, and a one-click pull request for the rest.
//!
//! - `GET /api/tests/setup` checks each tracked repo: it reads the workflow
//!   files on the default branch through the GitHub app (kept for ten
//!   minutes, or read again with `?refresh=1`), the setup pull request's
//!   live state and CI checks, and what ingestion recorded (results per
//!   workflow and quarantine calls, `common::e2e::ci_setup::CiState`).
//! - `POST /api/tests/setup/preview` returns the exact diff the pull request
//!   would hold. Nothing is written.
//! - `POST /api/tests/setup/pr` hands the change to the worker, which opens
//!   the pull request (`passes::e2e_setup`).
//! - When the setup pull request closes, [`on_pr_closed`] records it and
//!   checks the repo again, so the page moves on without anyone pressing
//!   anything. The next CI run updates the results through ingestion.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use base64::Engine as _;
use common::e2e::ci_setup::{
    likely_cause, package_scripts, pick_workflow, plan, pr_request, quarantine_snippet,
    reporter_snippet, reporter_writes_results, runner_from_package, status_of, test_command,
    unified_diff, upload_snippet, Options, Runner, SetupPr, SetupRequest, SetupStatus, UploadState,
    WorkflowFacts, JEST_CONFIGS, PLAYWRIGHT_CONFIGS, SETUP_BRANCH, VITEST_CONFIGS, WORKFLOW_DIR,
};
use common::e2e::messages::TestAction;
use common::e2e::store::Store;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::{Claims, WorkerMessage};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// A workflow check is reused for this long unless asked to read again.
const CHECK_FRESH_SECS: i64 = 600;
/// Workflow files read per repo, at most.
const MAX_WORKFLOWS: usize = 25;
/// Repos checked per request, at most.
const MAX_REPOS: usize = 30;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    tracing::error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn store(state: &AppState) -> Result<Store<'_>, (StatusCode, Json<Value>)> {
    let s = Store::new(&state.dynamo, &state.config.tests_table_name);
    if s.enabled() {
        Ok(s)
    } else {
        Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "E2E results aren't available here yet.",
        ))
    }
}

fn admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if matches!(claims.role.as_str(), "admin" | "owner") {
        Ok(())
    } else {
        Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins can open setup pull requests.",
        ))
    }
}

/// A repo name GitHub accepts: `owner/name`, letters, digits, `-`, `_`, `.`.
fn valid_repo(repo: &str) -> bool {
    let ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 100
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
    };
    matches!(repo.split_once('/'), Some((o, n)) if ok(o) && ok(n) && !n.contains(".."))
}

// ── GitHub reads ─────────────────────────────────────────────────────────

struct Gh<'a> {
    state: &'a AppState,
    token: String,
}

impl Gh<'_> {
    async fn get(&self, path: &str) -> Result<(u16, Value), String> {
        let resp = self
            .state
            .http
            .get(format!("https://api.github.com{path}"))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        Ok((status, resp.json::<Value>().await.unwrap_or(Value::Null)))
    }

    async fn file(&self, repo: &str, path: &str, git_ref: &str) -> Option<String> {
        let (status, v) = self
            .get(&format!(
                "/repos/{repo}/contents/{path}?ref={}",
                urlencoding::encode(git_ref)
            ))
            .await
            .ok()?;
        if status != 200 {
            return None;
        }
        let b64: String = v["content"].as_str()?.split_whitespace().collect();
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        String::from_utf8(bytes).ok()
    }

    async fn first_file(
        &self,
        repo: &str,
        paths: &[&str],
        git_ref: &str,
    ) -> Option<(String, String)> {
        for p in paths {
            if let Some(t) = self.file(repo, p, git_ref).await {
                return Some((p.to_string(), t));
            }
        }
        None
    }
}

async fn team_installation(state: &AppState, team: &str) -> Option<u64> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".into()),
        )
        .send()
        .await
        .ok()?;
    out.item()?
        .get("github_install_id")?
        .as_n()
        .ok()?
        .parse()
        .ok()
}

async fn gh_for(state: &AppState, installation_id: u64) -> Option<Gh<'_>> {
    match crate::auth::github_app::get_installation_token(state, installation_id).await {
        Ok(token) => Some(Gh { state, token }),
        Err(e) => {
            warn!(error = %e, "No GitHub token for the Tests setup check");
            None
        }
    }
}

// ── checking a repo ──────────────────────────────────────────────────────

/// What reading a repo's files found. Kept on the setup record.
#[derive(Debug, Clone, Default, serde::Serialize, Deserialize)]
struct Check {
    default_branch: String,
    #[serde(default)]
    workflow_path: Option<String>,
    #[serde(default)]
    facts: Option<WorkflowFacts>,
    #[serde(default)]
    runner: Option<Runner>,
    #[serde(default)]
    reporter_ok: Option<bool>,
    #[serde(default)]
    reporter_file: Option<String>,
    #[serde(default)]
    test_command: Option<String>,
    /// A pull request can add the upload step / the quarantine step, or why not.
    #[serde(default)]
    auto_upload: bool,
    #[serde(default)]
    auto_quarantine: bool,
    #[serde(default)]
    auto_repair: bool,
    #[serde(default)]
    auto_why: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// The files a check reads, kept for a preview.
struct Files {
    workflow: Option<(String, String)>,
    package_json: Option<String>,
}

async fn read_repo(gh: &Gh<'_>, repo: &str) -> Result<(Check, Files), String> {
    let (status, info) = gh.get(&format!("/repos/{repo}")).await?;
    if status != 200 {
        return Err("Coderhelm's GitHub app can't see this repository.".into());
    }
    let branch = info["default_branch"]
        .as_str()
        .unwrap_or("main")
        .to_string();
    let mut check = Check {
        default_branch: branch.clone(),
        ..Default::default()
    };
    let (status, listing) = gh
        .get(&format!(
            "/repos/{repo}/contents/{WORKFLOW_DIR}?ref={}",
            urlencoding::encode(&branch)
        ))
        .await?;
    let mut files = Vec::new();
    if status == 200 {
        for f in listing.as_array().into_iter().flatten().take(MAX_WORKFLOWS) {
            let Some(path) = f["path"].as_str() else {
                continue;
            };
            if !(path.ends_with(".yml") || path.ends_with(".yaml")) {
                continue;
            }
            if let Some(text) = gh.file(repo, path, &branch).await {
                files.push((path.to_string(), text));
            }
        }
    }
    let pkg = gh.file(repo, "package.json", &branch).await;
    let scripts = package_scripts(pkg.as_deref());
    let picked = pick_workflow(&files, &scripts);
    let runner = picked
        .as_ref()
        .map(|(_, f)| f.job.runner)
        .or_else(|| pkg.as_deref().and_then(runner_from_package));
    check.runner = runner;
    if let Some(r) = runner {
        let configs: &[&str] = match r {
            Runner::Playwright => PLAYWRIGHT_CONFIGS,
            Runner::Vitest => VITEST_CONFIGS,
            Runner::Jest => JEST_CONFIGS,
            Runner::Cypress => &["cypress.config.ts", "cypress.config.js"],
            Runner::Junit => &[],
        };
        let config = gh.first_file(repo, configs, &branch).await;
        check.reporter_file = config.as_ref().map(|(p, _)| p.clone());
        check.reporter_ok =
            reporter_writes_results(r, config.as_ref().map(|(_, t)| t.as_str()), pkg.as_deref());
    }
    let mut workflow = None;
    if let Some((path, facts)) = picked {
        let text = files
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, t)| t.clone())
            .unwrap_or_default();
        check.test_command = test_command(&text, &facts.job);
        let try_plan = |o: Options| plan(&text, &scripts, o);
        match try_plan(Options {
            upload: true,
            quarantine: false,
            repair: false,
        }) {
            Ok(p) => check.auto_upload = p.changed(),
            Err(e) => check.auto_why = Some(e),
        }
        if facts.job.runner.can_skip_quarantined() {
            match try_plan(Options {
                upload: false,
                quarantine: true,
                repair: false,
            }) {
                Ok(p) => check.auto_quarantine = p.changed(),
                Err(e) => {
                    check.auto_why.get_or_insert(e);
                }
            }
        }
        check.auto_repair = try_plan(Options {
            upload: false,
            quarantine: false,
            repair: true,
        })
        .is_ok_and(|p| p.changed());
        check.workflow_path = Some(path.clone());
        check.facts = Some(facts);
        workflow = Some((path, text));
    }
    Ok((
        check,
        Files {
            workflow,
            package_json: pkg,
        },
    ))
}

fn fresh(checked_at: &str, merged_at: Option<&str>) -> bool {
    let Ok(at) = chrono::DateTime::parse_from_rfc3339(checked_at) else {
        return false;
    };
    // A setup pull request merged after the last check: read again.
    if merged_at
        .and_then(|m| chrono::DateTime::parse_from_rfc3339(m).ok())
        .is_some_and(|m| m > at)
    {
        return false;
    }
    (chrono::Utc::now() - at.with_timezone(&chrono::Utc)).num_seconds() < CHECK_FRESH_SECS
}

/// The setup pull request's state and checks, read live from GitHub.
async fn live_pr(gh: &Gh<'_>, repo: &str, pr: &mut SetupPr) -> Option<&'static str> {
    let n = pr.number?;
    let (status, v) = gh.get(&format!("/repos/{repo}/pulls/{n}")).await.ok()?;
    if status != 200 {
        return None;
    }
    if v["merged"].as_bool() == Some(true) {
        pr.status = "merged".into();
        pr.merged_at = v["merged_at"].as_str().map(str::to_string);
    } else if v["state"].as_str() == Some("closed") {
        pr.status = "closed".into();
    }
    let sha = v["head"]["sha"].as_str()?;
    pr.head_sha = Some(sha.to_string());
    let (status, runs) = gh
        .get(&format!(
            "/repos/{repo}/commits/{sha}/check-runs?per_page=100"
        ))
        .await
        .ok()?;
    if status != 200 {
        return None;
    }
    Some(ci_rollup(
        runs["check_runs"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[]),
    ))
}

/// One word for a pull request's checks: "failure" wins, then "pending".
fn ci_rollup(runs: &[Value]) -> &'static str {
    if runs.is_empty() {
        return "none";
    }
    let failed = runs.iter().any(|r| {
        matches!(
            r["conclusion"].as_str(),
            Some("failure" | "cancelled" | "timed_out" | "action_required" | "startup_failure")
        )
    });
    if failed {
        "failure"
    } else if runs
        .iter()
        .any(|r| r["status"].as_str() != Some("completed"))
    {
        "pending"
    } else {
        "success"
    }
}

fn step_state(facts: Option<&WorkflowFacts>) -> (&'static str, String) {
    match facts.map(|f| f.upload) {
        None => ("missing", "No workflow runs the tests yet.".into()),
        Some(UploadState::Found) => ("done", "In the workflow on the default branch.".into()),
        Some(UploadState::Missing) => ("missing", "Not found in the workflow.".into()),
        Some(UploadState::OnlyOnSuccess) => (
            "partial",
            "Found, but it runs only when every test passes.".into(),
        ),
        Some(UploadState::OtherFiles) => (
            "partial",
            "Found, but it uploads other files than the results.".into(),
        ),
    }
}

/// The page's view of one repo.
async fn repo_view(
    st: &Store<'_>,
    gh: Option<&Gh<'_>>,
    team: &str,
    repo: &str,
    refresh: bool,
) -> Value {
    let mut record = st.setup(team, repo).await.unwrap_or_else(|_| json!({}));
    let mut pr: Option<SetupPr> = serde_json::from_value(record["pr"].clone()).ok();
    let mut ci_word: Option<&str> = None;
    // The pull request first: a merge makes the workflow check stale.
    if let (Some(gh), Some(p)) = (gh, pr.as_mut()) {
        if p.status == "open" || (refresh && p.number.is_some()) {
            ci_word = live_pr(gh, repo, p).await;
        }
    }
    let cached: Option<Check> = serde_json::from_value(record["check"].clone()).ok();
    let checked_at = record["checked_at"].as_str().unwrap_or("").to_string();
    let merged_at = pr.as_ref().and_then(|p| p.merged_at.clone());
    let mut check_error = None;
    let check = match (&cached, gh) {
        (Some(c), _) if !refresh && fresh(&checked_at, merged_at.as_deref()) => Some(c.clone()),
        (_, Some(gh)) => match read_repo(gh, repo).await {
            Ok((c, _)) => {
                record["check"] = json!(c);
                record["checked_at"] = json!(chrono::Utc::now().to_rfc3339());
                Some(c)
            }
            Err(e) => {
                check_error = Some(e);
                cached.clone()
            }
        },
        (c, None) => c.clone(),
    };
    if let Some(p) = &pr {
        record["pr"] = json!(p);
    }
    if let Err(e) = st.put_setup(team, repo, &record).await {
        warn!(error = %e, "Could not keep the Tests setup check");
    }

    let ci = st.ci_state(team, repo).await.unwrap_or_default();
    let check = check.unwrap_or_default();
    let facts = check.facts.as_ref();
    let seen = check
        .workflow_path
        .as_deref()
        .and_then(|p| ci.workflows.get(p))
        .cloned();
    let newest = ci.newest_results().cloned();
    let status = status_of(
        facts,
        seen.as_ref(),
        newest.is_some(),
        pr.as_ref().is_some_and(SetupPr::is_open),
    );
    let (upload_state, upload_detail) = step_state(facts);
    let runner = check.runner;
    let quarantine = match (runner, facts) {
        (Some(r), _) if !r.can_skip_quarantined() => {
            json!({ "state": "unsupported", "detail": "Skipping quarantined tests in CI works with Playwright. Coderhelm still treats a run where only quarantined tests failed as passing." })
        }
        (_, Some(f)) if f.quarantine => {
            json!({ "state": "done", "detail": "In the workflow on the default branch." })
        }
        (_, Some(_)) => json!({ "state": "missing", "detail": "Not found in the workflow." }),
        _ => json!({ "state": "missing", "detail": "No workflow runs the tests yet." }),
    };
    let reporter = match check.reporter_ok {
        Some(true) => {
            json!({ "state": "done", "file": check.reporter_file, "detail": "Writes a report Coderhelm reads." })
        }
        Some(false) => {
            json!({ "state": "missing", "file": check.reporter_file, "detail": "No reporter that writes a results file." })
        }
        None => {
            json!({ "state": "unknown", "file": check.reporter_file, "detail": "No config to check." })
        }
    };
    let cause = (status == SetupStatus::NothingReceived)
        .then(|| facts.zip(seen.as_ref()))
        .flatten()
        .map(|(f, s)| {
            let (text, fixable) = likely_cause(f, s, check.reporter_ok);
            json!({ "text": text, "fixable": fixable && check.auto_repair, "last_run_url": s.last_empty_url })
        });
    let snip_runner = runner.unwrap_or(Runner::Playwright);
    let (reporter_file, reporter_code) = reporter_snippet(snip_runner);
    let results = seen
        .as_ref()
        .filter(|s| s.last_results_at.is_some())
        .or(newest.as_ref())
        .map(|s| json!({ "last_at": s.last_results_at, "run_url": s.last_results_url, "tests": s.last_tests, "empty_runs": seen.as_ref().map_or(0, |x| x.empty_runs) }))
        .unwrap_or_else(|| json!({ "last_at": null, "run_url": null, "tests": 0, "empty_runs": seen.as_ref().map_or(0, |x| x.empty_runs) }));
    json!({
        "repo": repo,
        "default_branch": check.default_branch,
        "status": status,
        "runner": runner,
        "runners": Runner::ALL.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
        "workflow": facts.map(|f| json!({
            "path": check.workflow_path,
            "job_id": f.job.job_id,
            "job_name": f.job.job_name,
            "step_name": f.job.step_name,
            "test_command": check.test_command,
        })),
        "steps": {
            "upload": { "state": upload_state, "detail": upload_detail, "paths": facts.map(|f| f.upload_paths.clone()).unwrap_or_default() },
            "reporter": reporter,
            "quarantine": quarantine,
        },
        "results": results,
        "quarantine_fetch": ci.quarantine_fetched_at.as_ref().map(|at| json!({ "last_at": at, "run_url": ci.quarantine_run_url })),
        "cause": cause,
        "pr": pr.as_ref().map(|p| { let mut v = json!(p); v["ci"] = json!(ci_word); v }),
        "can_auto": {
            "upload": check.auto_upload,
            "quarantine": check.auto_quarantine,
            "repair": check.auto_repair,
            "why": check.auto_why,
        },
        "snippets": {
            "upload": upload_snippet(snip_runner, facts.is_some_and(|f| f.job.matrix)),
            "quarantine": quarantine_snippet(check.test_command.as_deref()),
            "reporter": { "file": check.reporter_file.as_deref().unwrap_or(reporter_file), "code": reporter_code },
            // For the runner tabs under "Do it myself".
            "by_runner": Runner::ALL.iter().map(|r| {
                let (file, code) = reporter_snippet(*r);
                (r.as_str().to_string(), json!({
                    "upload": upload_snippet(*r, facts.is_some_and(|f| f.job.matrix)),
                    "reporter": { "file": file, "code": code },
                }))
            }).collect::<serde_json::Map<String, Value>>(),
        },
        "checked_at": record["checked_at"],
        "error": check_error.or(check.error),
    })
}

/// The repos the page shows: tracked by E2E settings, enabled for the team.
async fn tracked_repos(
    state: &AppState,
    st: &Store<'_>,
    team: &str,
) -> Result<Vec<String>, (StatusCode, Json<Value>)> {
    let settings = st
        .settings(team)
        .await
        .map_err(|e| internal("e2e settings", e))?;
    let mut repos = BTreeSet::new();
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S("REPO#".into()),
        )
        .send()
        .await
        .map_err(|e| internal("repos", e))?;
    for item in out.items() {
        let enabled = item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(true);
        if let Some(name) = item.get("repo_name").and_then(|v| v.as_s().ok()) {
            if enabled && name.contains('/') && settings.covers(name) {
                repos.insert(name.clone());
            }
        }
    }
    for r in st.repos(team).await.map_err(|e| internal("e2e repos", e))? {
        if let Some(name) = r["repo"].as_str() {
            if settings.covers(name) {
                repos.insert(name.to_string());
            }
        }
    }
    Ok(repos.into_iter().take(MAX_REPOS).collect())
}

/// GET /api/tests/setup?repo=&refresh=1
pub async fn status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let team = &claims.team_id;
    let refresh = params
        .get("refresh")
        .is_some_and(|v| v == "1" || v == "true");
    let mut repos = tracked_repos(&state, &st, team).await?;
    if let Some(only) = params
        .get("repo")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        repos.retain(|r| r == only);
    }
    let gh = match team_installation(&state, team).await {
        Some(id) => gh_for(&state, id).await,
        None => None,
    };
    let mut out = Vec::new();
    for r in &repos {
        out.push(repo_view(&st, gh.as_ref(), team, r, refresh).await);
    }
    Ok(Json(json!({ "repos": out, "github": gh.is_some() })))
}

#[derive(Deserialize)]
pub struct SetupBody {
    repo: String,
    #[serde(default)]
    quarantine: bool,
    #[serde(default)]
    repair: bool,
}

struct Prepared {
    installation_id: u64,
    check: Check,
    path: String,
    yaml: String,
    plan: common::e2e::ci_setup::Plan,
    options: Options,
    facts: WorkflowFacts,
}

async fn prepare(
    state: &AppState,
    claims: &Claims,
    body: &SetupBody,
) -> Result<Prepared, (StatusCode, Json<Value>)> {
    if !valid_repo(&body.repo) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That isn't a repository name.",
        ));
    }
    let st = store(state)?;
    if !tracked_repos(state, &st, &claims.team_id)
        .await?
        .contains(&body.repo)
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "That repository isn't tracked for E2E results.",
        ));
    }
    let installation_id = team_installation(state, &claims.team_id)
        .await
        .ok_or_else(|| {
            fail(
                StatusCode::CONFLICT,
                "Install Coderhelm's GitHub app first.",
            )
        })?;
    let gh = gh_for(state, installation_id)
        .await
        .ok_or_else(|| fail(StatusCode::BAD_GATEWAY, "GitHub didn't answer. Try again."))?;
    let (check, files) = read_repo(&gh, &body.repo)
        .await
        .map_err(|e| fail(StatusCode::BAD_GATEWAY, &e))?;
    let (path, yaml) = files.workflow.ok_or_else(|| {
        fail(
            StatusCode::CONFLICT,
            "No workflow in this repository runs E2E tests, so there is nothing to change.",
        )
    })?;
    let options = Options {
        upload: !body.repair,
        quarantine: body.quarantine && !body.repair,
        repair: body.repair,
    };
    let scripts = package_scripts(files.package_json.as_deref());
    let planned = plan(&yaml, &scripts, options).map_err(|e| fail(StatusCode::CONFLICT, &e))?;
    if !planned.changed() {
        return Err(fail(
            StatusCode::CONFLICT,
            "Everything is already in the workflow. Nothing to change.",
        ));
    }
    let facts = check
        .facts
        .clone()
        .ok_or_else(|| fail(StatusCode::CONFLICT, "The workflow couldn't be read."))?;
    Ok(Prepared {
        installation_id,
        check,
        path,
        yaml,
        plan: planned,
        options,
        facts,
    })
}

/// POST /api/tests/setup/preview: the exact change, nothing written.
pub async fn preview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SetupBody>,
) -> Reply {
    admin(&claims)?;
    let p = prepare(&state, &claims, &body).await?;
    let req = pr_request(
        &p.path,
        &p.check.default_branch,
        &p.facts,
        &p.plan,
        body.repair,
    );
    Ok(Json(json!({
        "repo": body.repo,
        "path": p.path,
        "branch": req.branch,
        "base": req.base,
        "title": req.title,
        "body": req.body,
        "added": p.plan.added,
        "notes": p.plan.notes,
        "diff": unified_diff(&p.path, &p.yaml, &p.plan.yaml),
    })))
}

/// POST /api/tests/setup/pr: open the pull request (the worker does it).
pub async fn open_pr(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SetupBody>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let current: Option<SetupPr> = serde_json::from_value(
        st.setup(&claims.team_id, &body.repo)
            .await
            .map_err(|e| internal("e2e setup", e))?["pr"]
            .clone(),
    )
    .ok();
    if current
        .as_ref()
        .is_some_and(|p| matches!(p.status.as_str(), "queued" | "reading" | "opening"))
    {
        return Err(fail(
            StatusCode::CONFLICT,
            "A setup pull request is already being opened.",
        ));
    }
    let p = prepare(&state, &claims, &body).await?;
    let now = chrono::Utc::now().to_rfc3339();
    let pr = SetupPr {
        status: "queued".into(),
        kind: if body.repair { "repair" } else { "setup" }.into(),
        quarantine: p.options.quarantine,
        workflow_path: p.path.clone(),
        actor: claims.email.clone(),
        created_at: now.clone(),
        updated_at: now,
        ..Default::default()
    };
    st.put_setup_pr(&claims.team_id, &body.repo, &pr)
        .await
        .map_err(|e| internal("e2e setup save", e))?;
    let message = WorkerMessage::TestAction(TestAction {
        team_id: claims.team_id.clone(),
        repo: body.repo.clone(),
        action: "setup".into(),
        target_id: p.path.clone(),
        actor: claims.email.clone(),
        setup: Some(SetupRequest {
            installation_id: p.installation_id,
            default_branch: p.check.default_branch.clone(),
            workflow_path: p.path,
            options: p.options,
        }),
    });
    super::github_webhook::send_to_queue_after(&state, &state.config.ticket_queue_url, &message, 0)
        .await
        .map_err(|s| internal("e2e setup queue", format!("{s:?}")))?;
    info!(team = %claims.team_id, repo = %body.repo, repair = body.repair, "Setup pull request queued");
    Ok(Json(json!({ "pr": pr })))
}

/// A closed setup pull request: record it, and check the repo again so the
/// page shows the steps on the default branch without a manual refresh.
pub async fn on_pr_closed(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    merged: bool,
) {
    let pr = &payload["pull_request"];
    if pr["head"]["ref"].as_str() != Some(SETUP_BRANCH) || state.config.tests_table_name.is_empty()
    {
        return;
    }
    let repo = payload["repository"]["full_name"].as_str().unwrap_or("");
    let st = Store::new(&state.dynamo, &state.config.tests_table_name);
    let Ok(record) = st.setup(team_id, repo).await else {
        return;
    };
    let Ok(mut setup) = serde_json::from_value::<SetupPr>(record["pr"].clone()) else {
        return;
    };
    if setup.number.is_some() && setup.number != pr["number"].as_u64() {
        return;
    }
    setup.status = if merged { "merged" } else { "closed" }.into();
    setup.merged_at = pr["merged_at"].as_str().map(str::to_string);
    setup.updated_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = st.put_setup_pr(team_id, repo, &setup).await {
        warn!(error = %e, "Could not record the setup pull request closing");
        return;
    }
    if merged {
        if let Some(gh) = gh_for(state, installation_id).await {
            let _ = repo_view(&st, Some(&gh), team_id, repo, true).await;
        }
        info!(
            repo,
            "Setup pull request merged; checked the workflow again"
        );
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn repo_names_are_checked_before_any_github_path() {
        assert!(valid_repo("acme/web-app"));
        assert!(valid_repo("a.b/c_d"));
        for bad in ["acme", "acme/", "/x", "a/b/c", "a/..", "a/b?x=1", "a b/c"] {
            assert!(!valid_repo(bad), "{bad}");
        }
    }

    #[test]
    fn checks_roll_up_to_one_word() {
        assert_eq!(ci_rollup(&[]), "none");
        let run = |status: &str, conclusion: Option<&str>| json!({ "status": status, "conclusion": conclusion });
        assert_eq!(ci_rollup(&[run("completed", Some("success"))]), "success");
        assert_eq!(
            ci_rollup(&[run("completed", Some("success")), run("in_progress", None)]),
            "pending"
        );
        assert_eq!(
            ci_rollup(&[run("in_progress", None), run("completed", Some("failure"))]),
            "failure"
        );
    }

    #[test]
    fn a_check_is_read_again_after_its_pull_request_merges() {
        let now = chrono::Utc::now();
        let checked = (now - chrono::Duration::seconds(60)).to_rfc3339();
        assert!(fresh(&checked, None));
        assert!(!fresh(&checked, Some(&now.to_rfc3339())));
        let old = (now - chrono::Duration::seconds(CHECK_FRESH_SECS + 5)).to_rfc3339();
        assert!(!fresh(&old, None));
        assert!(!fresh("", None));
    }
}
