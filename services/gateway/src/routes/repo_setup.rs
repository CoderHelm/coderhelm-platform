//! Setup: pick repos, review what CoderHelm recommends, apply
//! (`common::setup` has the registry, the plans, apply and undo; the worker
//! analyzes each repo, `worker::passes::setup`).
//!
//! - `GET /api/setup`: the home (every connected repo's state, the new
//!   recommendations, the team's Setup settings, the last run);
//! - `GET /api/setup/registry`: every managed setting;
//! - `POST /api/setup/runs`: start a run (`guided`, or `auto`: "Do it for
//!   me", which applies when the analysis is done). Admin;
//! - `GET /api/setup/runs`, `GET /api/setup/runs/:id`: the history, a run;
//! - `PUT /api/setup/runs/:id/decisions`: the review choice and picks. Admin;
//! - `POST /api/setup/runs/:id/apply`: apply, or retry the failed repos.
//!   Admin, and each change needs its setting's own capability;
//! - `POST /api/setup/runs/:id/undo`: undo the run, or chosen items. Admin;
//! - `PUT /api/setup/settings`: `auto_setup_new_repos`. Admin.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Path, State};
use axum::{http::StatusCode, Extension, Json};
use common::plugins::kv::{Dynamo as DynamoKv, Expect};
use common::setup::apply::{self, Actor, ApplyRefused};
use common::setup::plan::{self, Choice, Decision, ItemDecision};
use common::setup::registry::{self, Effect, REGISTRY};
use common::setup::runs::{self, Mode, RepoDoc, Run, RunState};
use common::setup::store::{Cached, DynamoStore};
use common::setup::view;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, SetupAnalyzeMessage, WorkerMessage};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(code: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({ "error": msg })))
}

fn internal(what: &str, e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    error!(what, error = %e, "Setup failed");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong. Try again.",
    )
}

fn store(state: &AppState, team: &str) -> DynamoStore {
    DynamoStore {
        client: state.dynamo.clone(),
        settings_table: state.config.settings_table_name.clone(),
        e2e_table: state.config.tests_table_name.clone(),
        team: team.to_string(),
    }
}

fn kv(state: &AppState) -> DynamoKv {
    DynamoKv {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

/// Most repos one run takes.
const MAX_RUN_REPOS: usize = 100;

/// The team's connected repos, sorted.
pub(crate) async fn team_repos(state: &AppState, team: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team.to_string()))
            .expression_attribute_values(":p", AttributeValue::S("REPO#".into()))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        for i in r.items() {
            let on = i
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            if let Some(repo) = i
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.strip_prefix("REPO#"))
            {
                if on && repo.contains('/') {
                    out.push(repo.to_string());
                }
            }
        }
        start = r.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// The permission a capability needs that `claims` lacks, by its name.
fn missing_for(claims: &Claims) -> impl Fn(&str) -> Option<String> + Send + Sync {
    missing_in(claims.holder())
}

fn missing_in(holder: crate::permissions::Holder) -> impl Fn(&str) -> Option<String> + Send + Sync {
    move |cap: &str| {
        (!holder.holds(cap)).then(|| {
            crate::permissions::capability(cap)
                .map(|c| c.short_label.to_string())
                .unwrap_or_else(|| cap.to_string())
        })
    }
}

fn actor<'a>(
    claims: &'a Claims,
    missing: &'a (dyn Fn(&str) -> Option<String> + Send + Sync),
) -> Actor<'a> {
    Actor {
        email: &claims.email,
        kind: common::plugins::audit::ActorKind::Person,
        missing,
    }
}

/// Builds the code graph where a write asked for it (as the reviewer and
/// graph settings pages do).
async fn run_effects(state: &AppState, team: &str, effects: &[Effect]) {
    let mut done: Vec<&str> = Vec::new();
    for Effect::BuildGraph { repo } in effects {
        if done.contains(&repo.as_str()) {
            continue;
        }
        done.push(repo);
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        if common::code_graph::is_enabled(
            &state.dynamo,
            &state.config.settings_table_name,
            team,
            repo,
        )
        .await
        {
            super::graph::enqueue_full_index(state, team, owner, name, false).await;
        }
    }
}

fn state_word(s: RunState) -> Value {
    serde_json::to_value(s).unwrap_or(Value::Null)
}

/// A run as the dashboard reads it, for the person viewing.
fn run_view(run: &Run, docs: &[RepoDoc], claims: &Claims) -> Value {
    run_view_with(run, docs, claims, &HashMap::new(), 0)
}

/// A run with who saved each kept value (`set_by`, keyed by repo and
/// setting) and the team's repo count, for the team-wide section.
fn run_view_with(
    run: &Run,
    docs: &[RepoDoc],
    claims: &Claims,
    set_by: &HashMap<(String, String), registry::SetBy>,
    team_repo_count: usize,
) -> Value {
    let missing = missing_for(claims);
    let state = runs::state_of(run, docs);
    let mut overwrites = Vec::new();
    let repos: Vec<Value> = docs
        .iter()
        .map(|d| {
            let items: Vec<Value> = d
                .items
                .iter()
                .map(|i| {
                    let ex = run
                        .decisions
                        .iter()
                        .find(|x| x.repo == d.repo && x.setting_id == i.setting_id);
                    let (decision, value) = plan::decide(i, run.choice, ex);
                    let mut i = i.clone();
                    i.decision = decision;
                    i.value = (decision == Decision::Edit).then_some(value).flatten();
                    i.needs = missing(&i.capability);
                    if let Some(b) = set_by.get(&(d.repo.clone(), i.setting_id.clone())) {
                        i.current.set_by = Some(b.clone());
                    }
                    i.worth_a_look = plan::worth_a_look(&i);
                    serde_json::to_value(i).unwrap_or(Value::Null)
                })
                .collect();
            overwrites.extend(plan::overwrites(
                &d.repo,
                &d.items,
                run.choice,
                &run.decisions,
            ));
            json!({
                "repo": d.repo,
                "state": d.state,
                "progress": d.progress,
                "items": items,
                "error": d.error,
                "warning": d.warning,
                "tokens_used": d.tokens_used,
                "summary": d.result.as_ref().map(|r| &r.summary),
                "changes": d.changes,
            })
        })
        .collect();
    let results: Vec<Value> = docs
        .iter()
        .filter_map(|d| d.result.as_ref())
        .map(|r| serde_json::to_value(r).unwrap_or(Value::Null))
        .collect();
    json!({
        "run_id": run.run_id,
        "state": state_word(state),
        "mode": run.mode,
        "automatic": run.automatic,
        "created_at": run.created_at,
        "by": run.by,
        "choice": run.choice,
        "only_new": run.only_new,
        "estimate_tokens": run.estimate_tokens,
        "tokens_used": docs.iter().map(|d| d.tokens_used).sum::<u64>(),
        "repos": repos,
        "results": if results.is_empty() { Value::Null } else { Value::Array(results) },
        "overwrites": overwrites,
        "undo": run.undo,
        "applied_at": run.applied_at,
        "team": team_section(run, docs, team_repo_count),
    })
}

/// The team-wide settings of a run, once: one value for every repository,
/// so they are shown and undone apart from any one repo. Each lists the
/// repo whose record holds its change (that's the repo undo names).
fn team_section(run: &Run, docs: &[RepoDoc], repo_count: usize) -> Value {
    let mut items: Vec<Value> = Vec::new();
    for e in REGISTRY.iter().filter(|e| e.scope == registry::Scope::Team) {
        if !run.scope.iter().any(|s| s == e.id) {
            continue;
        }
        let change = docs.iter().find_map(|d| {
            d.changes
                .iter()
                .rev()
                .find(|c| c.setting_id == e.id)
                .map(|c| (d.repo.clone(), c.clone()))
        });
        let Some(item) = docs
            .iter()
            .find_map(|d| d.items.iter().find(|i| i.setting_id == e.id))
        else {
            continue;
        };
        items.push(json!({
            "setting_id": e.id,
            "title": e.title,
            "feature": e.feature,
            "current": item.current,
            "recommended": item.recommended,
            "source": item.source,
            "evidence": item.evidence,
            "changed_in": change.as_ref().map(|(r, _)| r),
            "change": change.map(|(_, c)| c),
        }));
    }
    json!({ "repo_count": repo_count, "items": items })
}

/// A run in the history: no plans, just where it stands.
fn run_line(run: &Run, docs: &[RepoDoc]) -> Value {
    let results: Vec<&runs::RepoResult> = docs.iter().filter_map(|d| d.result.as_ref()).collect();
    json!({
        "run_id": run.run_id,
        "state": state_word(runs::state_of(run, docs)),
        "mode": run.mode,
        "automatic": run.automatic,
        "created_at": run.created_at,
        "by": run.by,
        "repos": run.repos,
        "changed": results.iter().map(|r| r.changed).sum::<usize>(),
        "failed": results.iter().filter(|r| r.state == runs::ResultState::Failed).count(),
        "applied_at": run.applied_at,
    })
}

// ── Home ────────────────────────────────────────────────────────────────

/// GET /api/setup
pub async fn home(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let team = claims.team_id.clone();
    let kv = kv(&state);
    let st = store(&state, &team);
    let cached = Cached::new(&st);
    let repos = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let seen = runs::seen(&kv, &team)
        .await
        .map_err(|e| internal("setup seen", e))?;
    let new_ids = runs::new_ids(seen.as_ref());
    let settings = runs::settings(&kv, &team)
        .await
        .map_err(|e| internal("setup settings", e))?;
    let history = runs::list(&kv, &team, 10)
        .await
        .map_err(|e| internal("setup runs", e))?;
    // The newest run that names each repo, with that repo's record.
    let mut latest: HashMap<String, (Run, RepoDoc)> = HashMap::new();
    for run in &history {
        for d in runs::repos_of(&kv, &team, run).await.unwrap_or_default() {
            latest
                .entry(d.repo.clone())
                .or_insert_with(|| (run.clone(), d));
        }
    }
    let features = registry::features();
    let mut out = Vec::new();
    for repo in repos.iter().take(300) {
        let applied = runs::applied(&kv, &team, repo)
            .await
            .map_err(|e| internal("setup applied", e))?;
        let current = registry::read_all(&cached, repo)
            .await
            .map_err(|e| internal("setup read", e))?;
        let on = |f: &registry::Feature| {
            REGISTRY
                .iter()
                .filter(|e| e.primary && e.feature == *f)
                .any(|e| match &current[e.id].value {
                    Value::Bool(b) => *b,
                    Value::String(s) => !s.trim().is_empty(),
                    Value::Number(n) => n.as_u64() != Some(0),
                    _ => false,
                })
        };
        let features_on = features.iter().filter(|f| on(f)).count();
        let planned = last_plan(
            &kv,
            &team,
            repo,
            &history,
            applied.as_ref(),
            latest.get(repo),
        )
        .await;
        let missing = missing_for(&claims);
        let v = view::repo_view(
            repo,
            &current,
            applied.as_ref(),
            planned.as_ref().map(plan_ref),
            &missing,
        );
        let custom_count = REGISTRY
            .iter()
            .filter(|e| plan::is_custom(e, &current[e.id], applied.as_ref()))
            .count();
        let new_count = new_ids.len();
        let last = latest.get(repo);
        let mut error = None;
        let state_word = match last {
            Some((run, d))
                if runs::state_of(run, std::slice::from_ref(d)) == RunState::Analyzing
                    && d.state == runs::RepoState::Analyzing =>
            {
                "analyzing"
            }
            Some((run, d))
                if run.state != Some(RunState::Undone)
                    && (d.state == runs::RepoState::Failed
                        || d.result
                            .as_ref()
                            .is_some_and(|r| r.state == runs::ResultState::Failed)) =>
            {
                error = d
                    .error
                    .clone()
                    .or_else(|| d.result.as_ref().and_then(|r| r.error.clone()));
                "failed"
            }
            _ if applied.is_none() => "not_set_up",
            _ if custom_count > 0 => "custom",
            _ if new_count > 0 => "update_available",
            _ => "set_up",
        };
        out.push(json!({
            "repo": repo,
            "state": state_word,
            "features_on": features_on,
            "features_total": features.len(),
            "last_run_at": applied.as_ref().map(|a| a.at.clone()),
            "new_count": new_count,
            "custom_count": custom_count,
            "error": error,
            "counts": v.counts,
            "summary": v.summary,
        }));
    }
    let last_run = match history.first() {
        Some(run) => {
            let docs = runs::repos_of(&kv, &team, run).await.unwrap_or_default();
            let mut line = run_line(run, &docs);
            line["summaries"] = json!(docs
                .iter()
                .filter_map(|d| d
                    .result
                    .as_ref()
                    .map(|r| json!({"repo": d.repo, "summary": r.summary.text, "state": r.state})))
                .collect::<Vec<_>>());
            line
        }
        None => Value::Null,
    };
    Ok(Json(json!({
        "repos": out,
        "new_recommendations": new_ids
            .iter()
            .filter_map(|id| registry::get(id))
            .map(|e| json!({"setting_id": e.id, "title": e.title, "feature": e.feature, "since": e.since}))
            .collect::<Vec<_>>(),
        "auto_setup_new_repos": settings.auto_setup_new_repos,
        "last_run": last_run,
    })))
}

/// GET /api/setup/registry
pub async fn get_registry(
    State(_state): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
) -> Reply {
    let entries: Vec<Value> = REGISTRY
        .iter()
        .map(|e| {
            let mut v = serde_json::to_value(e).unwrap_or(Value::Null);
            v["default"] = registry::default_value(e);
            v["recommended_default"] = registry::policy_value(e);
            v["feature_title"] = json!(e.feature.title());
            v
        })
        .collect();
    Ok(Json(json!({ "entries": entries })))
}

// ── Runs ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateRun {
    /// A list of `owner/name`, `"all"` or `"not_set_up"`.
    pub repos: Value,
    #[serde(default)]
    pub only_new: bool,
    #[serde(default)]
    pub mode: Mode,
    /// `smart` (the default for Do it for me: the recommendation wherever
    /// nobody chose the value for this repo), `keep_mine` (the default for a
    /// guided run) or `use_recommended`.
    #[serde(default)]
    pub overwrite: Option<Choice>,
}

/// Why the team can't spend `estimate` tokens now, if it can't.
async fn over_budget(state: &AppState, team: &str, estimate: u64) -> Option<String> {
    if estimate == 0 {
        return None;
    }
    let limits = common::token_limits::load(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .ok()?;
    if limits.is_empty() {
        return None;
    }
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let totals = common::token_limits::load_month_usage(
        &state.dynamo,
        &state.config.analytics_table_name,
        team,
        &month,
    )
    .await
    .unwrap_or_default();
    limits
        .uses(&totals)
        .into_iter()
        .find(|u| u.used.saturating_add(estimate) > u.max)
        .map(|u| {
            format!(
                "Setup would use about {} tokens and the team's monthly {} limit has {} left. Raise the limit in Settings, or set up fewer repos.",
                common::token_limits::short_tokens(estimate),
                u.kind.label(),
                common::token_limits::short_tokens(u.max.saturating_sub(u.used))
            )
        })
}

/// The repos, the settings and the token estimate a run would have.
async fn plan_run(
    state: &AppState,
    team: &str,
    repos_in: &Value,
    only_new: bool,
) -> Result<(Vec<String>, Vec<String>, u64), (StatusCode, Json<Value>)> {
    let kv = kv(state);
    let connected = team_repos(state, team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let mut repos: Vec<String> = match repos_in {
        Value::String(s) if s == "all" => connected.clone(),
        Value::String(s) if s == "not_set_up" => {
            let mut out = Vec::new();
            for r in &connected {
                if runs::applied(&kv, team, r)
                    .await
                    .map_err(|e| internal("setup applied", e))?
                    .is_none()
                {
                    out.push(r.clone());
                }
            }
            out
        }
        Value::Array(list) => {
            let mut out = Vec::new();
            for r in list.iter().filter_map(|v| v.as_str()) {
                match connected.iter().find(|c| c.eq_ignore_ascii_case(r)) {
                    Some(c) => out.push(c.clone()),
                    None => {
                        return Err(fail(
                            StatusCode::BAD_REQUEST,
                            &format!("{r} isn't a connected repo."),
                        ))
                    }
                }
            }
            out
        }
        _ => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Send repos: a list, \"all\" or \"not_set_up\".",
            ))
        }
    };
    repos.sort();
    repos.dedup();
    if repos.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "There are no repos to set up.",
        ));
    }
    if repos.len() > MAX_RUN_REPOS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("Set up at most {MAX_RUN_REPOS} repos at a time."),
        ));
    }
    let scope: Vec<String> = if only_new {
        let seen = runs::seen(&kv, team)
            .await
            .map_err(|e| internal("setup seen", e))?;
        runs::new_ids(seen.as_ref())
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        REGISTRY.iter().map(|e| e.id.to_string()).collect()
    };
    if scope.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "There's nothing new to review.",
        ));
    }
    let ids: Vec<&str> = scope.iter().map(String::as_str).collect();
    let estimate = if common::setup::ai::needed(&ids) {
        repos.len() as u64 * common::setup::ESTIMATE_TOKENS_PER_REPO
    } else {
        0
    };
    Ok((repos, scope, estimate))
}

/// POST /api/setup/runs
pub async fn create_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateRun>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can run Setup."))?;
    let team = claims.team_id.clone();
    let kv = kv(&state);
    let (repos, scope, estimate) = plan_run(&state, &team, &body.repos, body.only_new).await?;
    if let Some(why) = over_budget(&state, &team, estimate).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": why, "estimate_tokens": estimate })),
        ));
    }
    let choice = body.overwrite.unwrap_or(match body.mode {
        Mode::Auto => Choice::Smart,
        Mode::Guided => Choice::KeepMine,
    });
    if choice == Choice::Decide && body.mode == Mode::Auto {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Do it for me takes smart, keep_mine or use_recommended.",
        ));
    }
    let mut run = Run::new(
        &claims.email,
        body.mode,
        choice,
        body.only_new,
        repos.clone(),
        scope,
        estimate,
    );
    let missing = missing_for(&claims);
    run.lacks = REGISTRY
        .iter()
        .filter_map(|e| missing(e.capability).map(|n| (e.capability.to_string(), n)))
        .collect::<BTreeMap<_, _>>();
    runs::put_run(&kv, &team, &run, Expect::Absent)
        .await
        .map_err(|e| internal("setup create run", e))?;
    for r in &repos {
        runs::put_repo(&kv, &team, &RepoDoc::new(&run.run_id, r))
            .await
            .map_err(|e| internal("setup create run", e))?;
    }
    for r in &repos {
        let msg = WorkerMessage::SetupAnalyze(SetupAnalyzeMessage {
            team_id: team.clone(),
            run_id: run.run_id.clone(),
            repo: r.clone(),
        });
        if super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &msg)
            .await
            .is_err()
        {
            apply::fail_analysis(
                &kv,
                &team,
                &run.run_id,
                r,
                "The analysis couldn't be started. Try again.",
            )
            .await;
        }
    }
    info!(team = %team, run = %run.run_id, repos = repos.len(), mode = ?run.mode, estimate, by = %claims.email, "Setup run started");
    Ok(Json(json!({
        "run_id": run.run_id,
        "estimate_tokens": estimate,
        "mode": run.mode,
    })))
}

/// GET /api/setup/runs
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let kv = kv(&state);
    let list = runs::list(&kv, &claims.team_id, 50)
        .await
        .map_err(|e| internal("setup runs", e))?;
    let mut out = Vec::new();
    for run in &list {
        let docs = runs::repos_of(&kv, &claims.team_id, run)
            .await
            .unwrap_or_default();
        out.push(run_line(run, &docs));
    }
    Ok(Json(json!({ "runs": out })))
}

async fn load(
    state: &AppState,
    team: &str,
    id: &str,
) -> Result<(Run, Vec<RepoDoc>), (StatusCode, Json<Value>)> {
    if id.is_empty() || id.len() > 40 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(fail(StatusCode::NOT_FOUND, "No such run."));
    }
    let kv = kv(state);
    let (run, _) = runs::get_run(&kv, team, id)
        .await
        .map_err(|e| internal("setup run", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such run."))?;
    let docs = runs::repos_of(&kv, team, &run)
        .await
        .map_err(|e| internal("setup run", e))?;
    Ok((run, docs))
}

/// GET /api/setup/runs/:id
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let (run, docs) = load(&state, &claims.team_id, &id).await?;
    let st = store(&state, &claims.team_id);
    let cached = Cached::new(&st);
    let mut by = HashMap::new();
    for d in &docs {
        for i in d.items.iter().filter(|i| i.current.set) {
            if let Some(e) = registry::get(&i.setting_id) {
                by.insert(
                    (d.repo.clone(), i.setting_id.clone()),
                    registry::set_by(&cached, e, &d.repo).await,
                );
            }
        }
    }
    let count = team_repos(&state, &claims.team_id)
        .await
        .map(|r| r.len())
        .unwrap_or(0);
    Ok(Json(run_view_with(&run, &docs, &claims, &by, count)))
}

#[derive(Deserialize)]
pub struct DecisionsBody {
    #[serde(default)]
    pub choice: Option<Choice>,
    #[serde(default)]
    pub items: Vec<ItemDecision>,
}

/// PUT /api/setup/runs/:id/decisions
pub async fn put_decisions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<DecisionsBody>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can change a Setup run."))?;
    let team = claims.team_id.clone();
    let (run, docs) = load(&state, &team, &id).await?;
    if runs::state_of(&run, &docs) != RunState::Ready {
        return Err(fail(
            StatusCode::CONFLICT,
            "Choices can change only while the run waits for review.",
        ));
    }
    let mut picks = Vec::new();
    for d in body.items {
        let Some(doc) = docs.iter().find(|x| x.repo == d.repo) else {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                &format!("{} isn't in this run.", d.repo),
            ));
        };
        if !doc.items.iter().any(|i| i.setting_id == d.setting_id) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                &format!("{} isn't in this run's plan for {}.", d.setting_id, d.repo),
            ));
        }
        let e = registry::get(&d.setting_id)
            .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "Unknown setting."))?;
        let value = match (d.decision, &d.value) {
            (Decision::Edit, Some(v)) => {
                Some(registry::check(e, v).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?)
            }
            (Decision::Edit, None) => {
                return Err(fail(StatusCode::BAD_REQUEST, "An edit needs a value."))
            }
            _ => None,
        };
        picks.push(ItemDecision { value, ..d });
    }
    let kv = kv(&state);
    let run = runs::update_run(&kv, &team, &id, |r| {
        if let Some(c) = body.choice {
            r.choice = c;
        }
        for p in &picks {
            r.decisions
                .retain(|x| !(x.repo == p.repo && x.setting_id == p.setting_id));
            r.decisions.push(p.clone());
        }
        Ok(())
    })
    .await
    .map_err(|e| internal("setup decisions", e))?;
    Ok(Json(run_view(&run, &docs, &claims)))
}

#[derive(Deserialize, Default)]
pub struct ApplyBody {
    /// Only these repos (a retry of the failed ones).
    #[serde(default)]
    pub repos: Option<Vec<String>>,
}

/// POST /api/setup/runs/:id/apply
pub async fn apply_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<ApplyBody>>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can apply a Setup run."))?;
    let team = claims.team_id.clone();
    load(&state, &team, &id).await?;
    let body = body.map(|b| b.0).unwrap_or_default();
    let all = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let st = store(&state, &team);
    let kv = kv(&state);
    let missing = missing_for(&claims);
    let who = actor(&claims, &missing);
    let (run, _, effects) = apply::apply_run(&st, &kv, &id, body.repos.as_deref(), &all, &who)
        .await
        .map_err(|e| match e {
            ApplyRefused::NotFound => fail(StatusCode::NOT_FOUND, &e.to_string()),
            ApplyRefused::NotReady(_) | ApplyRefused::Busy => {
                fail(StatusCode::CONFLICT, &e.to_string())
            }
            ApplyRefused::Store(m) => internal("setup apply", m),
        })?;
    run_effects(&state, &team, &effects).await;
    let docs = runs::repos_of(&kv, &team, &run)
        .await
        .map_err(|e| internal("setup run", e))?;
    info!(team = %team, run = %id, by = %claims.email, "Setup run applied");
    Ok(Json(run_view(&run, &docs, &claims)))
}

#[derive(Deserialize)]
pub struct UndoItem {
    pub repo: String,
    pub setting_id: String,
}

#[derive(Deserialize, Default)]
pub struct UndoBody {
    /// Only these changes; the whole run when left out.
    #[serde(default)]
    pub items: Option<Vec<UndoItem>>,
}

/// POST /api/setup/runs/:id/undo
pub async fn undo_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<UndoBody>>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can undo a Setup run."))?;
    let team = claims.team_id.clone();
    load(&state, &team, &id).await?;
    let items: Option<Vec<(String, String)>> = body
        .and_then(|b| b.0.items)
        .map(|v| v.into_iter().map(|i| (i.repo, i.setting_id)).collect());
    if items.as_ref().is_some_and(|i| i.is_empty()) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Pick at least one change to undo.",
        ));
    }
    let all = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let st = store(&state, &team);
    let kv = kv(&state);
    let missing = missing_for(&claims);
    let who = actor(&claims, &missing);
    let report = apply::undo(&st, &kv, &id, items.as_deref(), &all, &who)
        .await
        .map_err(|e| fail(StatusCode::CONFLICT, &e))?;
    let (run, docs) = load(&state, &team, &id).await?;
    info!(team = %team, run = %id, restored = report.restored, skipped = report.skipped.len(), by = %claims.email, "Setup run undone");
    let mut v = run_view(&run, &docs, &claims);
    v["undo"] = json!(report);
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct SettingsBody {
    pub auto_setup_new_repos: bool,
}

/// PUT /api/setup/settings
pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SettingsBody>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can change Setup settings."))?;
    let s = runs::TeamSettings {
        auto_setup_new_repos: body.auto_setup_new_repos,
        updated_by: claims.email.clone(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    runs::put_settings(&kv(&state), &claims.team_id, &s)
        .await
        .map_err(|e| internal("setup settings", e))?;
    if body.auto_setup_new_repos {
        warn!(team = %claims.team_id, by = %claims.email, "Setup: new repos will be set up automatically");
    }
    Ok(Json(
        json!({ "auto_setup_new_repos": s.auto_setup_new_repos }),
    ))
}

#[derive(Deserialize)]
pub struct EstimateBody {
    pub repos: Value,
    #[serde(default)]
    pub only_new: bool,
}

/// POST /api/setup/estimate: what a run over these repos would cost, and
/// whether the team's limits allow it, before any run is made.
pub async fn estimate(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<EstimateBody>,
) -> Reply {
    let (repos, _, estimate) =
        plan_run(&state, &claims.team_id, &body.repos, body.only_new).await?;
    let reason = over_budget(&state, &claims.team_id, estimate).await;
    Ok(Json(json!({
        "repos": repos.len(),
        "estimate_tokens": estimate,
        "over_budget": reason.is_some(),
        "reason": reason,
    })))
}

#[derive(Deserialize)]
pub struct SeenBody {
    pub setting_ids: Vec<String>,
}

/// POST /api/setup/seen: the team has reviewed these settings (a "new"
/// recommendation dismissed for everyone).
pub async fn mark_seen(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SeenBody>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can dismiss recommendations."))?;
    if let Some(bad) = body
        .setting_ids
        .iter()
        .find(|id| registry::get(id).is_none())
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("Unknown setting {bad}."),
        ));
    }
    let kv = kv(&state);
    runs::mark_seen(&kv, &claims.team_id, &body.setting_ids)
        .await
        .map_err(|e| internal("setup seen", e))?;
    let seen = runs::seen(&kv, &claims.team_id)
        .await
        .map_err(|e| internal("setup seen", e))?;
    let left = runs::new_ids(seen.as_ref());
    Ok(Json(json!({ "new_recommendations": left })))
}

#[derive(Deserialize)]
pub struct SwitchOnBody {
    pub repo: String,
    pub setting_id: String,
}

/// POST /api/setup/runs/:id/switch-on: a preview becomes the real setting
/// (stale cleanup closes, security fixes open pull requests, auto-merge
/// merges). Audited and recorded like any change, so undo puts it back.
pub async fn switch_on(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<SwitchOnBody>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can switch a preview on."))?;
    let team = claims.team_id.clone();
    load(&state, &team, &id).await?;
    let all = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let st = store(&state, &team);
    let kv = kv(&state);
    let missing = missing_for(&claims);
    let who = actor(&claims, &missing);
    let change = apply::switch_on(&st, &kv, &id, &body.repo, &body.setting_id, &all, &who)
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    if let Some(c) = &change {
        if c.setting_id == "auto_merge.enabled" {
            run_effects(
                &state,
                &team,
                &[Effect::BuildGraph {
                    repo: body.repo.clone(),
                }],
            )
            .await;
        }
    }
    info!(team = %team, run = %id, repo = %body.repo, setting = %body.setting_id, by = %claims.email, "Setup preview switched on");
    let (run, docs) = load(&state, &team, &id).await?;
    let mut v = run_view(&run, &docs, &claims);
    v["switched_on"] = json!(change);
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct UseRecommendedBody {
    pub items: Vec<UndoItem>,
}

/// POST /api/setup/runs/:id/use-recommended: settings the run kept take
/// their recommendation now (one, or a whole group). Each change is audited
/// and recorded with its `before` value, so its own undo puts it back.
pub async fn use_recommended(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<UseRecommendedBody>,
) -> Reply {
    let allowed = claims.require_role(3); // admin+
    allowed.map_err(|c| fail(c, "Only admins can change a Setup run."))?;
    if body.items.is_empty() || body.items.len() > 500 {
        return Err(fail(StatusCode::BAD_REQUEST, "Pick at least one setting."));
    }
    let team = claims.team_id.clone();
    load(&state, &team, &id).await?;
    let all = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let st = store(&state, &team);
    let kv = kv(&state);
    let missing = missing_for(&claims);
    let who = actor(&claims, &missing);
    let items: Vec<(String, String)> = body
        .items
        .into_iter()
        .map(|i| (i.repo, i.setting_id))
        .collect();
    let (done, skipped) = apply::use_recommended(&st, &kv, &id, &items, &all, &who)
        .await
        .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    let effects: Vec<Effect> = done
        .iter()
        .filter(|(_, c)| {
            c.setting_id.starts_with("reviewer.") || c.setting_id == "code_graph.enabled"
        })
        .map(|(r, _)| Effect::BuildGraph { repo: r.clone() })
        .collect();
    run_effects(&state, &team, &effects).await;
    info!(team = %team, run = %id, changed = done.len(), skipped = skipped.len(), by = %claims.email, "Setup: kept settings set to the recommendation");
    let (run, docs) = load(&state, &team, &id).await?;
    let mut v = run_view(&run, &docs, &claims);
    v["used"] = json!(done
        .iter()
        .map(|(r, c)| json!({"repo": r, "change": c}))
        .collect::<Vec<_>>());
    v["skipped"] = json!(skipped);
    Ok(Json(v))
}

/// The newest plan Setup made for `repo` among `history` (newest first):
/// the record with items, and its run. Falls back to the run that last
/// applied to it (`SETUP_APPLIED#`), which may be older than `history`.
async fn last_plan(
    kv: &DynamoKv,
    team: &str,
    repo: &str,
    history: &[Run],
    applied: Option<&plan::Applied>,
    known: Option<&(Run, RepoDoc)>,
) -> Option<(Run, RepoDoc)> {
    if let Some((r, d)) = known.filter(|(_, d)| !d.items.is_empty()) {
        return Some((r.clone(), d.clone()));
    }
    for run in history.iter().filter(|r| r.repos.iter().any(|x| x == repo)) {
        if let Ok(Some(d)) = runs::get_repo(kv, team, &run.run_id, repo).await {
            if !d.items.is_empty() {
                return Some((run.clone(), d));
            }
        }
    }
    let id = applied
        .map(|a| a.run_id.as_str())
        .filter(|s| !s.is_empty())?;
    let (run, _) = runs::get_run(kv, team, id).await.ok().flatten()?;
    let d = runs::get_repo(kv, team, id, repo).await.ok().flatten()?;
    (!d.items.is_empty()).then_some((run, d))
}

fn plan_ref<'a>(p: &'a (Run, RepoDoc)) -> view::LastPlan<'a> {
    let (run, d) = p;
    view::LastPlan {
        run_id: &run.run_id,
        at: run.applied_at.as_deref().unwrap_or(&run.created_at),
        choice: run.choice,
        decisions: &run.decisions,
        items: &d.items,
        applied: run.applied_at.is_some()
            && run.state != Some(RunState::Undone)
            && d.result.is_some(),
        warning: d.warning.as_deref(),
    }
}

/// GET /api/setup/repos/:owner/:name: one repo's features, each on, a
/// preview, off or not used here, with its settings in words and why.
pub async fn repo_view(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    let repo = format!("{owner}/{name}");
    let team = claims.team_id.clone();
    let repos = team_repos(&state, &team)
        .await
        .map_err(|e| internal("setup repos", e))?;
    let Some(repo) = repos
        .iter()
        .find(|r| r.eq_ignore_ascii_case(&repo))
        .cloned()
    else {
        return Err(fail(StatusCode::NOT_FOUND, "That repo isn't connected."));
    };
    let kv = kv(&state);
    let st = store(&state, &team);
    let cached = Cached::new(&st);
    let current = registry::read_all(&cached, &repo)
        .await
        .map_err(|e| internal("setup read", e))?;
    let applied = runs::applied(&kv, &team, &repo)
        .await
        .map_err(|e| internal("setup applied", e))?;
    let history = runs::list(&kv, &team, 10)
        .await
        .map_err(|e| internal("setup runs", e))?;
    let last = last_plan(&kv, &team, &repo, &history, applied.as_ref(), None).await;
    let missing = missing_for(&claims);
    let v = view::repo_view(
        &repo,
        &current,
        applied.as_ref(),
        last.as_ref().map(plan_ref),
        &missing,
    );
    let mut out = serde_json::to_value(v).unwrap_or(Value::Null);
    out["team_repo_count"] = json!(repos.len());
    // Each feature's settings say whether they are this repo's own or the
    // team's (one value for every repository).
    if let Some(fs) = out["features"].as_array_mut() {
        for f in fs {
            if let Some(ss) = f["settings"].as_array_mut() {
                for s in ss {
                    let scope = s["setting_id"]
                        .as_str()
                        .and_then(registry::get)
                        .map(|e| e.scope);
                    s["scope"] = json!(scope);
                }
            }
        }
    }
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{resolve_access, routed_handler, Access, Holder};

    #[test]
    fn every_registry_capability_is_a_real_one() {
        for e in REGISTRY {
            assert!(
                crate::permissions::capability(e.capability).is_some(),
                "{}: {}",
                e.id,
                e.capability
            );
        }
    }

    #[test]
    fn each_change_needs_its_settings_capability() {
        let admin = missing_in(Holder::builtin("admin"));
        assert!(REGISTRY.iter().all(|e| admin(e.capability).is_none()));
        let caps = vec!["setup".to_string(), "agent_settings".to_string()];
        let custom = missing_in(Holder::custom("r1", Some(("Setup runner", &caps))));
        assert_eq!(custom("security").as_deref(), Some("Manage security fixes"));
        assert_eq!(custom("agent_settings"), None);
        let member = missing_in(Holder::builtin("member"));
        assert!(member("agent_settings").is_some());
    }

    #[test]
    fn writes_need_the_setup_capability_and_reads_are_open() {
        for (method, path) in [
            ("POST", "/setup/runs"),
            ("PUT", "/setup/runs/:id/decisions"),
            ("POST", "/setup/runs/:id/apply"),
            ("POST", "/setup/runs/:id/undo"),
            ("PUT", "/setup/settings"),
            ("POST", "/setup/seen"),
            ("POST", "/setup/runs/:id/switch-on"),
            ("POST", "/setup/runs/:id/use-recommended"),
        ] {
            let h = routed_handler(method, path).unwrap_or_else(|| panic!("{method} {path}"));
            for role in ["viewer", "member"] {
                assert!(
                    matches!(
                        resolve_access(&Holder::builtin(role), Some(h)),
                        Access::Deny { .. }
                    ),
                    "{role} {h}"
                );
            }
            assert!(matches!(
                resolve_access(&Holder::builtin("admin"), Some(h)),
                Access::Allow { .. }
            ));
        }
        for path in [
            "/setup",
            "/setup/runs",
            "/setup/runs/:id",
            "/setup/registry",
            "/setup/repos/:owner/:name",
        ] {
            let h = routed_handler("GET", path).unwrap();
            assert!(matches!(
                resolve_access(&Holder::builtin("viewer"), Some(h)),
                Access::Allow { .. }
            ));
        }
    }
}
