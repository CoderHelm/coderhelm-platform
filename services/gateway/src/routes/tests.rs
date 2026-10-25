//! E2E test intelligence: runs, tests, failure groups, coverage and the
//! actions a team can take on a test.
//!
//! Results arrive two ways: the worker reads the reports of every finished
//! workflow run (enqueued from the GitHub webhook, [`enqueue_ingest`]), and CI
//! jobs whose results never reach GitHub (a device farm, say) upload them here
//! signed with a GitHub Actions OIDC token ([`ci_upload`]). The same token lets
//! CI ask which tests are quarantined ([`ci_quarantine`]). No long-lived
//! secret is involved: GitHub signs the token for one repo and one run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use common::e2e::detect::can_quarantine;
use common::e2e::messages::{TestAction, TestIngest};
use common::e2e::model::{Run, Settings, State as TestState, StateEvent};
use common::e2e::stats::impact;
use common::e2e::store::Store;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::models::{Claims, WorkerMessage};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": message, "code": crate::openapi::default_code(status) })),
    )
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if matches!(claims.role.as_str(), "admin" | "owner") {
        Ok(())
    } else {
        Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins can change E2E settings or act on tests.",
        ))
    }
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

async fn queue(state: &AppState, message: WorkerMessage, delay: i32) -> Result<(), String> {
    super::github_webhook::send_to_queue_after(
        state,
        &state.config.ticket_queue_url,
        &message,
        delay,
    )
    .await
    .map(|_| ())
    .map_err(|s| format!("queue: {s}"))
}

// ── Ingest from the GitHub webhook ────────────────────────────────────────

/// Enqueues reading the reports of a finished workflow run, when the team has
/// E2E results turned on for the repo. Never fails the webhook.
pub async fn enqueue_ingest(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) {
    if state.config.tests_table_name.is_empty() || payload["action"].as_str() != Some("completed") {
        return;
    }
    let wf = &payload["workflow_run"];
    let conclusion = wf["conclusion"].as_str().unwrap_or("");
    // Nothing ran, so there is nothing to read.
    if matches!(conclusion, "skipped" | "neutral" | "stale" | "") {
        return;
    }
    let repo = &payload["repository"];
    let full = repo["full_name"].as_str().unwrap_or("");
    let Some((owner, name)) = full.split_once('/') else {
        return;
    };
    let st = Store::new(&state.dynamo, &state.config.tests_table_name);
    let settings = match st.settings(team_id).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "Could not read E2E settings");
            return;
        }
    };
    if !settings.covers(full) {
        return;
    }
    let default_branch = repo["default_branch"]
        .as_str()
        .unwrap_or("main")
        .to_string();
    let repo_id = repo["id"].as_u64().unwrap_or(0);
    if let Err(e) = st
        .upsert_repo(team_id, full, installation_id, repo_id, &default_branch)
        .await
    {
        warn!(error = %e, "Could not record the repo for E2E results");
    }
    let msg = TestIngest {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        repo_id,
        default_branch,
        workflow: wf["name"].as_str().unwrap_or("").to_string(),
        workflow_path: wf["path"].as_str().unwrap_or("").to_string(),
        run_id: wf["id"].as_u64().unwrap_or(0),
        run_attempt: wf["run_attempt"].as_u64().unwrap_or(1) as u32,
        head_sha: wf["head_sha"].as_str().unwrap_or("").to_string(),
        head_branch: wf["head_branch"].as_str().unwrap_or("").to_string(),
        event: wf["event"].as_str().unwrap_or("").to_string(),
        pr_numbers: wf["pull_requests"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["number"].as_u64())
            .collect(),
        conclusion: conclusion.to_string(),
        html_url: wf["html_url"].as_str().unwrap_or("").to_string(),
        started_at: wf["run_started_at"]
            .as_str()
            .or(wf["created_at"].as_str())
            .unwrap_or("")
            .to_string(),
        updated_at: wf["updated_at"].as_str().unwrap_or("").to_string(),
        upload_key: None,
    };
    if msg.run_id == 0 {
        return;
    }
    // Artifacts are listed a few seconds after the run completes.
    if let Err(e) = queue(state, WorkerMessage::TestIngest(msg), 20).await {
        warn!(error = %e, "Could not enqueue reading E2E results");
    }
}

// ── Settings ──────────────────────────────────────────────────────────────

pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = store(&state)?;
    let s = st
        .settings(&claims.team_id)
        .await
        .map_err(|e| internal("e2e settings", e))?;
    Ok(Json(json!(s)))
}

pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Settings>,
) -> Reply {
    admin(&claims)?;
    body.validate()
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let st = store(&state)?;
    let s = body.sanitized();
    st.put_settings(&claims.team_id, &s)
        .await
        .map_err(|e| internal("e2e save settings", e))?;
    info!(team = %claims.team_id, enabled = s.enabled, ticket_after_days = s.ticket_after_days, "E2E settings saved");
    Ok(Json(json!(s)))
}

// ── Summary and runs ──────────────────────────────────────────────────────

fn q<'a>(map: &'a HashMap<String, String>, k: &str) -> Option<&'a str> {
    map.get(k)
        .map(String::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

async fn repos_of(
    st: &Store<'_>,
    team: &str,
    only: Option<&str>,
) -> Result<Vec<String>, (StatusCode, Json<Value>)> {
    let mut repos: Vec<String> = st
        .repos(team)
        .await
        .map_err(|e| internal("e2e repos", e))?
        .iter()
        .filter_map(|r| r["repo"].as_str().map(str::to_string))
        .collect();
    repos.sort();
    if let Some(r) = only {
        repos.retain(|x| x == r);
    }
    Ok(repos)
}

pub async fn summary(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let team = &claims.team_id;
    let only = q(&params, "repo");
    let all_repos = repos_of(&st, team, None).await?;
    let repos: Vec<String> = all_repos
        .iter()
        .filter(|r| only.is_none_or(|o| o == r.as_str()))
        .cloned()
        .collect();

    let tests = st
        .summaries(team, only)
        .await
        .map_err(|e| internal("e2e summaries", e))?;
    let mut counts: HashMap<&str, u32> = ["healthy", "flaky", "broken", "quarantined", "fixed"]
        .into_iter()
        .map(|k| (k, 0))
        .collect();
    let mut minutes = 0.0;
    for t in &tests {
        if let Some(c) = t["state"].as_str().and_then(|s| counts.get_mut(s)) {
            *c += 1;
        }
        minutes += t["minutes_lost_7d"].as_f64().unwrap_or(0.0);
    }

    let now = chrono::Utc::now();
    let since30 = (now - chrono::Duration::days(30)).to_rfc3339();
    let since7 = now - chrono::Duration::days(7);
    let mut runs: Vec<Run> = Vec::new();
    for r in &repos {
        runs.extend(
            st.runs_since(team, r, &since30)
                .await
                .map_err(|e| internal("e2e runs", e))?,
        );
    }
    let in_week =
        |r: &&Run| chrono::DateTime::parse_from_rfc3339(&r.started_at).is_ok_and(|t| t >= since7);
    let week: Vec<&Run> = runs.iter().filter(in_week).collect();
    let mut blocked: Vec<(String, u64)> = week
        .iter()
        .filter(|r| r.status == "failed" && !r.rescued && !r.bad_run)
        .filter_map(|r| r.pr.map(|p| (r.repo.clone(), p)))
        .collect();
    blocked.sort();
    blocked.dedup();
    let mut rescued: Vec<(String, u64)> = week
        .iter()
        .filter(|r| r.rescued)
        .filter_map(|r| r.pr.map(|p| (r.repo.clone(), p)))
        .collect();
    rescued.sort();
    rescued.dedup();
    let rate = |rs: &[&Run]| {
        let (mut passed_runs, mut tests_total, mut flaky) = (0u32, 0u32, 0u32);
        for r in rs {
            if r.status == "passed" || r.rescued {
                passed_runs += 1;
            }
            tests_total += r.passed + r.failed + r.flaky;
            flaky += r.flaky;
        }
        let pass = if rs.is_empty() {
            0.0
        } else {
            passed_runs as f64 / rs.len() as f64
        };
        let flake = if tests_total == 0 {
            0.0
        } else {
            flaky as f64 / tests_total as f64
        };
        (
            (pass * 1000.0).round() / 1000.0,
            (flake * 1000.0).round() / 1000.0,
        )
    };
    let (pass7, flake7) = rate(&week);
    let mut trend = Vec::new();
    for d in (0..30).rev() {
        let day = (now - chrono::Duration::days(d))
            .format("%Y-%m-%d")
            .to_string();
        let on_day: Vec<&Run> = runs
            .iter()
            .filter(|r| r.started_at.starts_with(&day))
            .collect();
        let (p, f) = rate(&on_day);
        trend.push(json!({ "day": day, "runs": on_day.len(), "pass_rate": p, "flake_rate": f }));
    }
    Ok(Json(json!({
        "repos": all_repos,
        "counts": counts,
        "prs_blocked_7d": blocked.len(),
        "ci_minutes_lost_7d": (minutes * 10.0).round() / 10.0,
        "prs_rescued_7d": rescued.len(),
        "runs_7d": week.len(),
        "pass_rate_7d": pass7,
        "flake_rate_7d": flake7,
        "trend": trend,
    })))
}

pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let team = &claims.team_id;
    let limit: i32 = q(&params, "limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(50)
        .clamp(1, 200);
    let branch = q(&params, "branch");
    let pr: Option<u64> = q(&params, "pr").and_then(|p| p.parse().ok());
    let repos = repos_of(&st, team, q(&params, "repo")).await?;
    // A cursor is `repo|sort key`, so paging works for one repo at a time;
    // across repos the newest page of each is merged.
    let (cursor_repo, cursor_sk) = match q(&params, "before").and_then(|b| b.split_once('|')) {
        Some((r, sk)) => (Some(r.to_string()), Some(sk.to_string())),
        None => (None, None),
    };
    let mut all: Vec<Run> = Vec::new();
    let mut next = None;
    for r in &repos {
        if cursor_repo.as_deref().is_some_and(|c| c != r) {
            continue;
        }
        let (runs, n) = st
            .runs(team, r, limit, cursor_sk.as_deref())
            .await
            .map_err(|e| internal("e2e list runs", e))?;
        if repos.len() == 1 || cursor_repo.is_some() {
            next = n.map(|sk| format!("{r}|{sk}"));
        }
        all.extend(runs);
    }
    all.retain(|r| branch.is_none_or(|b| r.branch == b) && pr.is_none_or(|p| r.pr == Some(p)));
    all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    all.truncate(limit as usize);
    Ok(Json(json!({ "runs": all, "next": next })))
}

pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(run_key): Path<String>,
) -> Reply {
    let st = store(&state)?;
    let team = &claims.team_id;
    let (repo, started_at) = st
        .run_ref(team, &run_key)
        .await
        .map_err(|e| internal("e2e run ref", e))?
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "That run isn't here. It may have expired.",
            )
        })?;
    let run = st
        .get_run(team, &repo, &started_at, &run_key)
        .await
        .map_err(|e| internal("e2e get run", e))?
        .ok_or_else(|| {
            fail(
                StatusCode::NOT_FOUND,
                "That run isn't here. It may have expired.",
            )
        })?;
    let key = common::e2e::store::run_detail_key(team, &repo, &run_key);
    let executions: Value = match state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(o) => match o.body.collect().await {
            Ok(b) => serde_json::from_slice(&b.into_bytes()).unwrap_or(json!([])),
            Err(_) => json!([]),
        },
        Err(_) => json!([]),
    };
    Ok(Json(json!({ "run": run, "executions": executions })))
}

// ── Tests ─────────────────────────────────────────────────────────────────

pub async fn list_tests(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let mut tests = st
        .summaries(&claims.team_id, q(&params, "repo"))
        .await
        .map_err(|e| internal("e2e list tests", e))?;
    if let Some(s) = q(&params, "state") {
        tests.retain(|t| {
            t["state"].as_str() == Some(s)
                || t["flags"]
                    .as_array()
                    .is_some_and(|f| f.iter().any(|x| x.as_str() == Some(s)))
        });
    }
    if let Some(text) = q(&params, "q").map(str::to_lowercase) {
        tests.retain(|t| {
            ["title", "file", "owner"].iter().any(|k| {
                t[*k]
                    .as_str()
                    .is_some_and(|v| v.to_lowercase().contains(&text))
            })
        });
    }
    match q(&params, "sort").unwrap_or("impact") {
        "flake_rate" => tests.sort_by(|a, b| {
            b["flake_rate_30d"]
                .as_f64()
                .partial_cmp(&a["flake_rate_30d"].as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        "slowest" => tests.sort_by_key(|t| std::cmp::Reverse(t["p95_ms"].as_u64().unwrap_or(0))),
        "recent" => tests.sort_by(|a, b| b["last_seen"].as_str().cmp(&a["last_seen"].as_str())),
        _ => tests.sort_by(|a, b| {
            let rank = |t: &Value| match t["state"].as_str() {
                Some("broken") => 3,
                Some("flaky") => 2,
                Some("quarantined") => 1,
                _ => 0,
            };
            (rank(b), impact(b)).cmp(&(rank(a), impact(a)))
        }),
    }
    // Paging by offset over a list that is ranked in memory.
    let limit: usize = q(&params, "limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let start: usize = q(&params, "before")
        .and_then(|b| b.parse().ok())
        .unwrap_or(0);
    let page: Vec<Value> = tests.iter().skip(start).take(limit).cloned().collect();
    let next = (start + limit < tests.len()).then(|| (start + limit).to_string());
    Ok(Json(json!({ "tests": page, "next": next })))
}

pub async fn get_test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let st = store(&state)?;
    let r = st
        .record(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e get test", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That test isn't here."))?;
    let mut history: Vec<Value> = r
        .history
        .iter()
        .rev()
        .take(50)
        .map(|h| json!({ "run_key": h.run_key, "sha": h.sha, "branch": h.branch, "pr": h.pr, "at": h.at, "outcome": h.outcome, "bad_run": h.bad_run, "attempts": h.attempts }))
        .collect();
    history.truncate(50);
    let mut events = r.events.clone();
    events.reverse();
    Ok(Json(json!({
        "test": common::e2e::stats::summary(&r, chrono::Utc::now()),
        "history": history,
        "events": events,
    })))
}

#[derive(Deserialize)]
pub struct QuarantineBody {
    on: bool,
    #[serde(default)]
    reason: Option<String>,
}

fn event(from: &str, to: &str, reason: String, actor: &str) -> StateEvent {
    StateEvent {
        at: chrono::Utc::now().to_rfc3339(),
        from: from.to_string(),
        to: to.to_string(),
        reason,
        actor: actor.to_string(),
    }
}

pub async fn quarantine(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<QuarantineBody>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let mut r = st
        .record(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e get test", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That test isn't here."))?;
    let reason = body
        .reason
        .as_deref()
        .map(|s| common::truncate_str(s.trim(), 300).to_string())
        .filter(|s| !s.is_empty());
    if body.on {
        can_quarantine(&r).map_err(|why| fail(StatusCode::BAD_REQUEST, why))?;
        if !r.quarantined {
            r.quarantined = true;
            r.quarantined_since = Some(chrono::Utc::now().to_rfc3339());
            r.quarantined_by = Some(claims.email.clone());
            let from = r.health.as_str().to_string();
            r.events.push(event(
                &from,
                "quarantined",
                reason.unwrap_or_else(|| "Quarantined by a person.".into()),
                &claims.email,
            ));
        }
    } else if r.quarantined {
        r.quarantined = false;
        r.quarantined_since = None;
        r.quarantined_by = None;
        let to = r.health.as_str().to_string();
        r.events.push(event(
            "quarantined",
            &to,
            reason.unwrap_or_else(|| "Taken out of quarantine by a person.".into()),
            &claims.email,
        ));
    }
    st.put_record(&claims.team_id, &r)
        .await
        .map_err(|e| internal("e2e save test", e))?;
    info!(team = %claims.team_id, test = %id, on = body.on, "Test quarantine changed");
    Ok(Json(
        json!({ "test": common::e2e::stats::summary(&r, chrono::Utc::now()) }),
    ))
}

#[derive(Deserialize)]
pub struct PinBody {
    never_quarantine: bool,
}

pub async fn pin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<PinBody>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let mut r = st
        .record(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e get test", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That test isn't here."))?;
    r.never_quarantine = body.never_quarantine;
    if body.never_quarantine && r.quarantined {
        r.quarantined = false;
        r.quarantined_since = None;
        r.quarantined_by = None;
        let to = r.health.as_str().to_string();
        r.events.push(event(
            "quarantined",
            &to,
            "Pinned as never quarantine.".into(),
            &claims.email,
        ));
    }
    st.put_record(&claims.team_id, &r)
        .await
        .map_err(|e| internal("e2e save test", e))?;
    Ok(Json(
        json!({ "test": common::e2e::stats::summary(&r, chrono::Utc::now()) }),
    ))
}

pub async fn fix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let mut r = st
        .record(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e get test", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That test isn't here."))?;
    if r.health != TestState::Flaky {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Only flaky tests get a fix PR. A broken test is a real regression: fix the code it tests.",
        ));
    }
    if matches!(
        r.fix_status.as_deref(),
        Some("reproducing" | "fixing" | "burn_in")
    ) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "A fix for this test is already in progress.",
        ));
    }
    r.fix_status = Some("reproducing".into());
    st.put_record(&claims.team_id, &r)
        .await
        .map_err(|e| internal("e2e save test", e))?;
    queue(
        &state,
        WorkerMessage::TestAction(TestAction {
            team_id: claims.team_id.clone(),
            repo: r.repo.clone(),
            action: "fix".into(),
            target_id: id,
            actor: claims.email.clone(),
        }),
        0,
    )
    .await
    .map_err(|e| internal("e2e queue fix", e))?;
    Ok(Json(json!({ "queued": true })))
}

// ── Failure groups, coverage, recommendations ─────────────────────────────

pub async fn failure_groups(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let groups = st
        .groups(&claims.team_id, q(&params, "repo"))
        .await
        .map_err(|e| internal("e2e groups", e))?;
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let week_start = common::e2e::store::day_minus(&today, 6);
    let mut out: Vec<Value> = groups
        .iter()
        .map(|g| {
            let failures_7d: u64 = g["days"]
                .as_object()
                .map(|d| {
                    d.iter()
                        .filter(|(day, _)| day.as_str() >= week_start.as_str())
                        .filter_map(|(_, n)| n.as_u64())
                        .sum()
                })
                .unwrap_or(0);
            let ids: Vec<Value> = g["test_ids"].as_array().cloned().unwrap_or_default();
            json!({
                "signature": g["signature"],
                "repo": g["repo"],
                "message": g["message"],
                "selector": g["selector"],
                "step": g["step"],
                "tests": ids.len(),
                "failures_7d": failures_7d,
                "last_seen": g["last_seen"],
                "test_ids": ids.into_iter().take(5).collect::<Vec<_>>(),
                "cause": g["cause"],
            })
        })
        .filter(|g| g["failures_7d"].as_u64().unwrap_or(0) > 0)
        .collect();
    out.sort_by_key(|g| {
        std::cmp::Reverse((
            g["failures_7d"].as_u64().unwrap_or(0),
            g["tests"].as_u64().unwrap_or(0),
        ))
    });
    Ok(Json(json!({ "groups": out })))
}

pub async fn coverage(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state)?;
    let team = &claims.team_id;
    let repos = repos_of(&st, team, q(&params, "repo")).await?;
    let mut areas = Vec::new();
    for r in &repos {
        if let Some(c) = st
            .coverage(team, r)
            .await
            .map_err(|e| internal("e2e coverage", e))?
        {
            areas.extend(c["areas"].as_array().cloned().unwrap_or_default());
        }
    }
    let mut recs = st
        .recommendations(team, q(&params, "repo"))
        .await
        .map_err(|e| internal("e2e recs", e))?;
    recs.retain(|r| r.status != "dismissed");
    recs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(Json(json!({ "areas": areas, "recommendations": recs })))
}

pub async fn accept_recommendation(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let mut rec = st
        .recommendation(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e rec", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That recommendation isn't here."))?;
    if rec.status != "open" {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That recommendation was already handled.",
        ));
    }
    rec.status = "accepted".into();
    st.put_recommendation(&claims.team_id, &rec)
        .await
        .map_err(|e| internal("e2e save rec", e))?;
    queue(
        &state,
        WorkerMessage::TestAction(TestAction {
            team_id: claims.team_id.clone(),
            repo: rec.repo.clone(),
            action: "generate".into(),
            target_id: id,
            actor: claims.email.clone(),
        }),
        0,
    )
    .await
    .map_err(|e| internal("e2e queue generate", e))?;
    Ok(Json(json!({ "queued": true })))
}

#[derive(Deserialize, Default)]
pub struct DismissBody {
    #[serde(default)]
    reason: Option<String>,
}

pub async fn dismiss_recommendation(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<DismissBody>>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state)?;
    let mut rec = st
        .recommendation(&claims.team_id, &id)
        .await
        .map_err(|e| internal("e2e rec", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That recommendation isn't here."))?;
    rec.status = "dismissed".into();
    if let Some(reason) = body
        .and_then(|b| b.0.reason)
        .filter(|r| !r.trim().is_empty())
    {
        rec.why = format!(
            "{} (Dismissed: {})",
            rec.why,
            common::truncate_str(reason.trim(), 200)
        );
    }
    st.put_recommendation(&claims.team_id, &rec)
        .await
        .map_err(|e| internal("e2e save rec", e))?;
    Ok(Json(json!({ "ok": true })))
}

// ── CI calls signed with a GitHub Actions OIDC token ──────────────────────

const OIDC_ISSUER: &str = "https://token.actions.githubusercontent.com";
const OIDC_AUDIENCE: &str = "coderhelm";
/// Largest upload accepted (the request limit in front of the gateway is 6 MB).
const MAX_UPLOAD: usize = 6 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub struct OidcClaims {
    pub repository: String,
    #[serde(default)]
    pub repository_id: String,
    #[serde(rename = "ref", default)]
    pub git_ref: String,
    #[serde(default)]
    pub sha: String,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub run_attempt: String,
    #[serde(default)]
    pub workflow: String,
    #[serde(default)]
    pub event_name: String,
}

struct Jwks {
    keys: Vec<Value>,
    fetched: Instant,
}

static JWKS: RwLock<Option<Jwks>> = RwLock::const_new(None);

async fn jwks(http: &reqwest::Client, refresh: bool) -> Result<Vec<Value>, String> {
    if let Some(j) = JWKS.read().await.as_ref() {
        // A refresh (an unknown key id) refetches at most once a minute, so
        // tokens with made-up key ids can't turn each request into a fetch.
        let fresh_for = if refresh { 60 } else { 3600 };
        if j.fetched.elapsed() < Duration::from_secs(fresh_for) {
            return Ok(j.keys.clone());
        }
    }
    let v: Value = http
        .get(format!("{OIDC_ISSUER}/.well-known/jwks"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| format!("jwks: {e}"))?
        .json()
        .await
        .map_err(|e| format!("jwks: {e}"))?;
    let keys = v["keys"].as_array().cloned().unwrap_or_default();
    *JWKS.write().await = Some(Jwks {
        keys: keys.clone(),
        fetched: Instant::now(),
    });
    Ok(keys)
}

/// Checks a GitHub Actions OIDC token: signed by GitHub, for Coderhelm, not
/// expired. Returns its claims.
pub async fn verify_oidc(http: &reqwest::Client, token: &str) -> Result<OidcClaims, String> {
    use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
    let header = decode_header(token).map_err(|e| format!("token header: {e}"))?;
    if header.alg != Algorithm::RS256 {
        return Err("unexpected token algorithm".into());
    }
    let kid = header.kid.ok_or("token has no key id")?;
    let find = |keys: &[Value]| {
        keys.iter()
            .find(|k| k["kid"].as_str() == Some(kid.as_str()))
            .cloned()
    };
    let key = match find(&jwks(http, false).await?) {
        Some(k) => k,
        // GitHub rotates keys; look again once before refusing.
        None => find(&jwks(http, true).await?).ok_or("unknown signing key")?,
    };
    let decoding = DecodingKey::from_rsa_components(
        key["n"].as_str().ok_or("key has no modulus")?,
        key["e"].as_str().ok_or("key has no exponent")?,
    )
    .map_err(|e| format!("key: {e}"))?;
    let mut v = Validation::new(Algorithm::RS256);
    v.set_audience(&[OIDC_AUDIENCE]);
    v.set_issuer(&[OIDC_ISSUER]);
    v.leeway = 30;
    decode::<OidcClaims>(token, &decoding, &v)
        .map(|d| d.claims)
        .map_err(|e| format!("token: {e}"))
}

/// The teams a signed CI call speaks for: teams that receive results for the
/// repo, where the repo id matches (so a re-created repo with the same name
/// can't speak for the old one) and E2E results are on.
async fn teams_for(
    state: &AppState,
    claims: &OidcClaims,
) -> Result<Vec<(String, Value)>, (StatusCode, Json<Value>)> {
    let st = store(state)?;
    let teams = st
        .teams_for_repo(&claims.repository)
        .await
        .map_err(|e| internal("e2e teams for repo", e))?;
    let mut out = Vec::new();
    for (team, rec) in teams {
        let id_ok =
            rec["repo_id"].as_u64().map(|id| id.to_string()) == Some(claims.repository_id.clone());
        if !id_ok {
            continue;
        }
        let settings = st.settings(&team).await.unwrap_or_default();
        if settings.covers(&claims.repository) {
            out.push((team, rec));
        }
    }
    if out.is_empty() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "This repository isn't set up for E2E results in Coderhelm. Turn them on in the dashboard; the repo appears after its first workflow run.",
        ));
    }
    Ok(out)
}

async fn signed(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<OidcClaims, (StatusCode, Json<Value>)> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            fail(
                StatusCode::UNAUTHORIZED,
                "Send a GitHub Actions OIDC token (audience coderhelm) as a Bearer token.",
            )
        })?;
    verify_oidc(&state.http, token).await.map_err(|e| {
        warn!(error = %e, "Refused a CI call");
        fail(
            StatusCode::UNAUTHORIZED,
            "That token isn't a valid GitHub Actions OIDC token for Coderhelm.",
        )
    })
}

/// The PR a run belongs to, from `refs/pull/N/merge`.
fn pr_of(git_ref: &str) -> Option<u64> {
    git_ref
        .strip_prefix("refs/pull/")?
        .split('/')
        .next()?
        .parse()
        .ok()
}

/// A report that was taken in.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct CiUploadResult {
    /// Always true: the report is stored and queued for reading.
    #[schema(example = true)]
    pub ok: bool,
}

/// A quarantined test.
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
pub struct QuarantinedTest {
    /// CoderHelm's id for the test.
    #[schema(example = "e2e/checkout.spec.ts::checkout › applies a discount code")]
    id: String,
    /// Its title, `describe › title`.
    #[schema(example = "checkout › applies a discount code")]
    title: String,
    /// Its file.
    #[schema(example = "e2e/checkout.spec.ts")]
    file: String,
    /// The Playwright project, when there is one.
    #[schema(example = "chromium")]
    project: Option<String>,
    /// `playwright`, `cucumber` or `junit`.
    #[schema(example = "playwright")]
    framework: String,
}

/// The tests quarantined for the calling repository.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Quarantine {
    /// Every quarantined test.
    #[schema(value_type = Vec<QuarantinedTest>)]
    pub tests: Vec<Value>,
    /// A pattern for Playwright's `--grep-invert` that skips them; empty when none are.
    #[schema(example = "checkout applies a discount code")]
    pub playwright_grep_invert: String,
}

/// POST /webhooks/ci/tests/results?name=report.json — a report from a CI job
/// whose results never reach GitHub (a device farm, say).
#[utoipa::path(
    post,
    path = "/webhooks/ci/tests/results",
    tag = "CI",
    operation_id = "uploadTestResults",
    summary = "Upload a test report",
    description = "Takes one test report from a CI job whose results never reach GitHub (a device \
        farm, say), signed with the job's GitHub Actions OIDC token instead of a CoderHelm secret. \
        Request the token with the audience `coderhelm`. CoderHelm checks it was issued for a \
        workflow run of a repository connected to your team with E2E results turned on, then reads \
        the report like one from GitHub: flaky tests, failure groups and quarantine.",
    params(("name" = Option<String>, Query, description = "The report's file name. Must end in `.json` (Playwright or Cucumber JSON) or `.xml` (JUnit XML). Default `report.json`.", example = "results.json")),
    request_body(content = String, description = "The report file, at most 6 MB.", content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "The report is stored and queued.", body = CiUploadResult),
        (status = 400, description = "Not a report CoderHelm can read, or it has no tests.", body = crate::openapi::ErrorBody),
        (status = 401, description = "The token is not a valid GitHub Actions OIDC token for CoderHelm.", body = crate::openapi::ErrorBody),
        (status = 403, description = "The repository is not connected, or E2E results are off.", body = crate::openapi::ErrorBody),
        (status = 413, description = "Empty, or larger than 6 MB.", body = crate::openapi::ErrorBody),
    ),
    security(("github_oidc" = [])),
)]
pub async fn ci_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<CiUploadResult>, (StatusCode, Json<Value>)> {
    let claims = signed(&state, &headers).await?;
    if body.is_empty() || body.len() > MAX_UPLOAD {
        return Err(fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Send one report of at most 6 MB.",
        ));
    }
    let name = q(&params, "name").unwrap_or("report.json");
    let lower = name.to_lowercase();
    if !(lower.ends_with(".json") || lower.ends_with(".xml")) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Send a Playwright JSON, Cucumber JSON or JUnit XML report.",
        ));
    }
    if common::e2e::parse::parse_any(name, &body, &common::e2e::parse::Links::default())
        .is_none_or(|t| t.is_empty())
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That file isn't a test report we can read, or it has no tests.",
        ));
    }
    let teams = teams_for(&state, &claims).await?;
    let (owner, repo_name) = claims.repository.split_once('/').unwrap_or(("", ""));
    let branch = claims
        .git_ref
        .strip_prefix("refs/heads/")
        .map(str::to_string)
        .unwrap_or_else(|| claims.git_ref.clone());
    let run_id: u64 = claims.run_id.parse().unwrap_or(0);
    let run_attempt: u32 = claims.run_attempt.parse().unwrap_or(1);
    let now = chrono::Utc::now().to_rfc3339();
    for (team, rec) in teams {
        let key = common::e2e::store::upload_key(&team, &claims.repository, name);
        state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .body(body.to_vec().into())
            .send()
            .await
            .map_err(|e| internal("e2e store upload", e.to_string()))?;
        let msg = TestIngest {
            team_id: team.clone(),
            installation_id: rec["installation_id"].as_u64().unwrap_or(0),
            repo_owner: owner.to_string(),
            repo_name: repo_name.to_string(),
            repo_id: rec["repo_id"].as_u64().unwrap_or(0),
            default_branch: rec["default_branch"].as_str().unwrap_or("main").to_string(),
            workflow: claims.workflow.clone(),
            workflow_path: String::new(),
            run_id,
            run_attempt,
            head_sha: claims.sha.clone(),
            head_branch: branch.clone(),
            event: claims.event_name.clone(),
            pr_numbers: pr_of(&claims.git_ref).into_iter().collect(),
            conclusion: "completed".into(),
            html_url: format!(
                "https://github.com/{}/actions/runs/{run_id}",
                claims.repository
            ),
            started_at: now.clone(),
            updated_at: now.clone(),
            upload_key: Some(key),
        };
        queue(&state, WorkerMessage::TestIngest(msg), 0)
            .await
            .map_err(|e| internal("e2e queue upload", e))?;
    }
    info!(repo = %claims.repository, run_id, "E2E report uploaded");
    Ok(Json(CiUploadResult { ok: true }))
}

/// Escapes a title for use inside a Playwright `--grep-invert` pattern.
fn regex_escape(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if "\\^$.|?*+()[]{}/".contains(c) {
                vec!['\\', c]
            } else {
                vec![c]
            }
        })
        .collect()
}

/// A test's title as Playwright's `--grep` sees it. Coderhelm keeps titles as
/// `describe › title`; Playwright matches the title path joined by spaces.
fn grep_pattern(title: &str) -> String {
    title
        .split(" › ")
        .map(regex_escape)
        .collect::<Vec<_>>()
        .join(" ")
}

/// GET /webhooks/ci/tests/quarantine — the tests quarantined for the calling
/// repo, so a CI step can keep their failures from failing the build.
#[utoipa::path(
    get,
    path = "/webhooks/ci/tests/quarantine",
    tag = "CI",
    operation_id = "listQuarantinedTests",
    summary = "List quarantined tests",
    description = "Returns the tests quarantined for the repository the OIDC token was issued to, \
        so a CI step can keep their failures from failing the build. For Playwright, pass \
        `playwright_grep_invert` to `--grep-invert`.",
    responses(
        (status = 200, description = "The quarantined tests.", body = Quarantine),
        (status = 401, description = "The token is not a valid GitHub Actions OIDC token for CoderHelm.", body = crate::openapi::ErrorBody),
        (status = 403, description = "The repository is not connected, or E2E results are off.", body = crate::openapi::ErrorBody),
    ),
    security(("github_oidc" = [])),
)]
pub async fn ci_quarantine(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Quarantine>, (StatusCode, Json<Value>)> {
    let claims = signed(&state, &headers).await?;
    let teams = teams_for(&state, &claims).await?;
    let st = store(&state)?;
    let mut tests = Vec::new();
    for (team, _) in teams {
        let all = st
            .summaries(&team, Some(&claims.repository))
            .await
            .map_err(|e| internal("e2e quarantine list", e))?;
        tests.extend(all.into_iter().filter(|t| t["state"].as_str() == Some("quarantined")).map(|t| {
            json!({ "id": t["id"], "title": t["title"], "file": t["file"], "project": t["project"], "framework": t["framework"] })
        }));
    }
    let grep_invert = if tests.is_empty() {
        String::new()
    } else {
        tests
            .iter()
            .filter_map(|t| t["title"].as_str())
            .map(grep_pattern)
            .collect::<Vec<_>>()
            .join("|")
    };
    Ok(Json(Quarantine {
        tests,
        playwright_grep_invert: grep_invert,
    }))
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn pull_request_refs_name_their_pr() {
        assert_eq!(pr_of("refs/pull/42/merge"), Some(42));
        assert_eq!(pr_of("refs/heads/main"), None);
    }

    #[test]
    fn titles_are_escaped_for_grep() {
        assert_eq!(
            regex_escape("pays (card) $5.00?"),
            "pays \\(card\\) \\$5\\.00\\?"
        );
    }

    #[test]
    fn grep_patterns_match_playwrights_title_path() {
        assert_eq!(
            grep_pattern("checkout › pays (card)"),
            "checkout pays \\(card\\)"
        );
        assert_eq!(grep_pattern("login"), "login");
    }

    #[tokio::test]
    async fn a_forged_token_is_refused_before_any_lookup() {
        let http = reqwest::Client::new();
        for t in [
            "",
            "not.a.jwt",
            "eyJhbGciOiJIUzI1NiJ9.eyJyZXBvc2l0b3J5IjoiYS9iIn0.x",
        ] {
            assert!(verify_oidc(&http, t).await.is_err(), "{t}");
        }
    }
}
