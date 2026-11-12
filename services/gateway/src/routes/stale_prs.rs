//! Stale pull request cleanup from the dashboard and from a PR comment
//! (`common::stale_prs` has the rules, `worker::passes::stale_prs` the
//! sweep).
//!
//! - `GET /api/reviewer/stale/config?repo=` — the team's settings, the
//!   repository's own (when it has them) and what applies.
//! - `PUT /api/reviewer/stale/config` — save the team's settings, or one
//!   repository's (`config: null` drops them so it follows the team). Admin.
//! - `GET /api/reviewer/stale?repo=` — the latest plan for the repository
//!   (every repository's without one), the actions taken and the pull
//!   requests kept open.
//! - `POST /api/reviewer/stale/preview` — work the plan out again now,
//!   without changing anything on GitHub.
//! - `PUT /api/reviewer/stale/keep-open` — keep one pull request open, or let
//!   it be closed again. Admin.
//! - `@coderhelm keep open` on a pull request keeps it open, from its author
//!   or anyone with write access.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::stale_prs::{self as stale, Action, StaleConfig};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, StalePrsPreviewMessage, WorkerMessage};
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

#[derive(Deserialize)]
pub struct RepoQuery {
    #[serde(default)]
    pub repo: Option<String>,
}

fn repo_param(repo: Option<&str>) -> Result<Option<String>, StatusCode> {
    match repo.map(str::trim).filter(|r| !r.is_empty()) {
        None => Ok(None),
        Some(r) => {
            super::reviewer::validate_repo(r)?;
            if !r.contains('/') {
                return Err(StatusCode::BAD_REQUEST);
            }
            Ok(Some(r.to_string()))
        }
    }
}

async fn get(state: &AppState, team_id: &str, sk: &str) -> Result<Option<Item>, StatusCode> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map(|o| o.item().cloned())
        .map_err(|e| {
            error!(sk, error = %e, "stale PRs: read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

fn config_of(item: Option<&Item>) -> Option<StaleConfig> {
    item.and_then(|i| StaleConfig::from_json(&s(i, stale::ATTR_CONFIG)))
}

/// GET /api/reviewer/stale/config?repo=owner/name
pub async fn get_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_param(q.repo.as_deref())?;
    let team = config_of(get(&state, &claims.team_id, stale::TEAM_SK).await?.as_ref());
    let own = match &repo {
        Some(r) => config_of(
            get(&state, &claims.team_id, &stale::repo_sk(r))
                .await?
                .as_ref(),
        ),
        None => None,
    };
    let effective = stale::effective(team.as_ref(), own.as_ref());
    // Every repository with its own settings, so "all repositories" can say
    // which ones are not off (Setup saves the settings per repository).
    let repo_configs = match &repo {
        Some(_) => vec![],
        None => repo_configs_of(&read_prefix(&state, &claims.team_id, stale::REPO_PREFIX).await?),
    };
    Ok(Json(json!({
        "team": team.clone().unwrap_or_default(),
        "team_saved": team.is_some(),
        "repo": own,
        "repo_saved": own.is_some(),
        "effective": effective,
        "repo_configs": repo_configs,
        "defaults": StaleConfig::default(),
    })))
}

/// Pure: `{repo, config}` for each stored repository settings record, by repository.
fn repo_configs_of(items: &[Item]) -> Vec<Value> {
    let mut out: Vec<(String, StaleConfig)> = items
        .iter()
        .filter_map(|i| {
            let sk = s(i, "sk");
            let repo = stale::repo_of_sk(&sk)?.to_string();
            Some((repo, config_of(Some(i))?))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter()
        .map(|(repo, config)| json!({ "repo": repo, "config": config }))
        .collect()
}

#[derive(Deserialize)]
pub struct PutConfig {
    /// Absent: the team's settings.
    #[serde(default)]
    pub repo: Option<String>,
    /// `null` with a repo: drop the repository's own settings.
    pub config: Option<StaleConfig>,
}

/// PUT /api/reviewer/stale/config
pub async fn put_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PutConfig>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let repo = repo_param(body.repo.as_deref())?;
    let sk = repo
        .as_deref()
        .map(stale::repo_sk)
        .unwrap_or_else(|| stale::TEAM_SK.to_string());
    let Some(cfg) = body.config.map(StaleConfig::sanitized) else {
        if repo.is_none() {
            return Err(StatusCode::BAD_REQUEST);
        }
        state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "stale PRs: could not drop the repository settings");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        info!(team = %claims.team_id, repo = ?repo, by = %claims.email, "Stale PR settings: repository follows the team");
        return Ok(Json(json!({ "config": null })));
    };
    let text = serde_json::to_string(&cfg).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The same item Setup writes (`common::setup::store::config_item`).
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(common::setup::store::config_item(
            &claims.team_id,
            &sk,
            &text,
            &claims.email,
            &chrono::Utc::now().to_rfc3339(),
        )))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "stale PRs: could not save the settings");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    info!(team = %claims.team_id, repo = ?repo, enabled = cfg.enabled, dry_run = cfg.dry_run, by = %claims.email, "Stale PR settings saved");
    Ok(Json(json!({ "config": cfg })))
}

/// GET /api/reviewer/stale?repo=owner/name
///
/// With a repository: its latest plan (`plan`), the log and its kept-open
/// pull requests. Without one: every repository's latest plan (`plans`),
/// the log and every kept-open pull request, so one page can list them all.
/// Each plan item carries its `repo`, `days_quiet` and, when it is marked or
/// about to be, `closes_at`.
pub async fn get_activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_param(q.repo.as_deref())?;
    let now = chrono::Utc::now();
    let plan = match &repo {
        Some(r) => get(&state, &claims.team_id, &stale::preview_sk(r))
            .await?
            .map(|i| plan_json(&i, r, &now)),
        None => None,
    };
    let plans = match &repo {
        Some(_) => plan.iter().cloned().collect::<Vec<_>>(),
        None => read_prefix(&state, &claims.team_id, stale::PREVIEW_PREFIX)
            .await?
            .iter()
            .filter_map(|i| {
                let r = s(i, "sk").strip_prefix(stale::PREVIEW_PREFIX)?.to_string();
                (!r.is_empty()).then(|| plan_json(i, &r, &now))
            })
            .collect(),
    };
    // A plan is a snapshot of the last sweep: it says the repository's mode
    // as its settings are now (the one the settings page shows), so a row
    // marked while it was on reads "nothing closes it" once it is off.
    let team = config_of(get(&state, &claims.team_id, stale::TEAM_SK).await?.as_ref());
    let own: HashMap<String, StaleConfig> =
        read_prefix(&state, &claims.team_id, stale::REPO_PREFIX)
            .await?
            .iter()
            .filter_map(|i| {
                let sk = s(i, "sk");
                Some((
                    stale::repo_of_sk(&sk)?.to_ascii_lowercase(),
                    config_of(Some(i))?,
                ))
            })
            .collect();
    let current = |r: &str| stale::effective(team.as_ref(), own.get(&r.to_ascii_lowercase()));
    let plans: Vec<Value> = plans
        .into_iter()
        .map(|p| with_current_settings(p, &current))
        .collect();
    let plan = plan.map(|p| with_current_settings(p, &current));
    let log = read_log(&state, &claims.team_id, repo.as_deref(), 50).await?;
    let pfx = match &repo {
        Some(r) => stale::exempt_prefix(r),
        None => stale::EXEMPT_PREFIX.to_string(),
    };
    let kept: Vec<Value> = read_prefix(&state, &claims.team_id, &pfx)
        .await?
        .iter()
        .map(kept_json)
        .collect();
    Ok(Json(
        json!({ "plan": plan, "plans": plans, "log": log, "kept_open": kept }),
    ))
}

/// Pure: a saved plan with the repository's mode as its settings are now.
/// The items stay: what carries the label, and what it would do once on.
fn with_current_settings(mut plan: Value, current: &dyn Fn(&str) -> StaleConfig) -> Value {
    let repo = plan["repo"].as_str().unwrap_or("").to_string();
    let cfg = current(&repo);
    plan["enabled"] = json!(cfg.enabled);
    // Off: what it lists is what it would do, never "closing now".
    plan["dry_run"] = json!(cfg.dry_run || !cfg.enabled);
    plan
}

/// One repository's saved plan, its items with the repository, the days
/// without activity and the closing date.
fn plan_json(i: &Item, repo: &str, now: &chrono::DateTime<chrono::Utc>) -> Value {
    let items: Vec<Value> = serde_json::from_str::<Vec<Value>>(&s(i, "plan"))
        .unwrap_or_default()
        .into_iter()
        .map(|it| plan_item_json(it, repo, now))
        .collect();
    json!({
        "repo": repo,
        "computed_at": s(i, "computed_at"),
        "dry_run": i.get("dry_run").and_then(|v| v.as_bool().ok()).copied().unwrap_or(true),
        "enabled": i.get("enabled").and_then(|v| v.as_bool().ok()).copied().unwrap_or(false),
        "stopped": s(i, "stopped"),
        "items": items,
    })
}

/// Pure: one plan item with `repo`, `days_quiet` (whole days since the last
/// activity, as of now) and `closes_at` (when it closes: the date it was
/// marked plus the grace period, or now for one closing on this sweep).
fn plan_item_json(mut it: Value, repo: &str, now: &chrono::DateTime<chrono::Utc>) -> Value {
    let days_quiet = it["last_activity"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| (*now - t.with_timezone(&chrono::Utc)).num_days().max(0));
    let closes_at = match it["action"].as_str() {
        Some("wait" | "mark" | "close") => it["next_at"].as_str().unwrap_or("").to_string(),
        _ => String::new(),
    };
    if let Some(o) = it.as_object_mut() {
        o.insert("repo".into(), json!(repo));
        o.insert("days_quiet".into(), json!(days_quiet));
        o.insert(
            "closes_at".into(),
            if closes_at.is_empty() {
                Value::Null
            } else {
                json!(closes_at)
            },
        );
    }
    it
}

fn kept_json(i: &Item) -> Value {
    json!({
        "repo": s(i, "repo"),
        "pr": n(i, "pr_number"),
        "title": s(i, "title"),
        "url": s(i, "url"),
        "by": s(i, "by"),
        "via": s(i, "via"),
        "at": s(i, "at"),
    })
}

/// Every record under a sort key prefix, paged (bounded).
async fn read_prefix(state: &AppState, team_id: &str, pfx: &str) -> Result<Vec<Item>, StatusCode> {
    let mut out = vec![];
    let mut start = None;
    for _ in 0..20 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(pfx))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!(pfx, error = %e, "stale PRs: could not read");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        out.extend(resp.items().iter().cloned());
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

async fn read_log(
    state: &AppState,
    team_id: &str,
    repo: Option<&str>,
    want: usize,
) -> Result<Vec<Value>, StatusCode> {
    let mut out = vec![];
    let mut start = None;
    for _ in 0..10 {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":pfx", attr_s(stale::LOG_PREFIX))
            .scan_index_forward(false)
            .limit(100)
            .set_exclusive_start_key(start.take());
        if let Some(r) = repo {
            q = q
                .filter_expression("#r = :r")
                .expression_attribute_names("#r", "repo")
                .expression_attribute_values(":r", attr_s(r));
        }
        let resp = q.send().await.map_err(|e| {
            error!(error = %e, "stale PRs: could not read the log");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        for i in resp.items() {
            out.push(json!({
                "at": s(i, "at"),
                "repo": s(i, "repo"),
                "pr": n(i, "pr_number"),
                "action": s(i, "action"),
                "title": s(i, "title"),
                "url": s(i, "url"),
                "author": s(i, "author"),
                "why": s(i, "why"),
                "by": s(i, "by"),
                "run_id": s(i, "run_id"),
            }));
        }
        if out.len() >= want {
            break;
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out.truncate(want);
    Ok(out)
}

#[derive(Deserialize)]
pub struct PreviewBody {
    pub repo: String,
}

/// POST /api/reviewer/stale/preview — the worker works the plan out now.
pub async fn preview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PreviewBody>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    let repo = repo_param(Some(&body.repo))?.ok_or(StatusCode::BAD_REQUEST)?;
    let msg = WorkerMessage::StalePrsPreview(StalePrsPreviewMessage {
        team_id: claims.team_id.clone(),
        repo: repo.clone(),
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &msg).await?;
    info!(team = %claims.team_id, repo, "Stale PR preview queued");
    Ok((StatusCode::ACCEPTED, Json(json!({ "queued": true }))))
}

#[derive(Deserialize)]
pub struct KeepOpenBody {
    pub repo: String,
    pub pr: u64,
    pub keep: bool,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub url: String,
}

/// One kept-open record, and its log line.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn keep_open(
    state: &AppState,
    team_id: &str,
    repo: &str,
    pr: u64,
    title: &str,
    url: &str,
    by: &str,
    via: &str,
) -> Result<(), StatusCode> {
    let now = chrono::Utc::now();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&stale::exempt_sk(repo, pr)))
        .item("repo", attr_s(repo))
        .item("pr_number", AttributeValue::N(pr.to_string()))
        .item("title", attr_s(common::truncate_str(title, 300)))
        .item("url", attr_s(url))
        .item("by", attr_s(by))
        .item("via", attr_s(via))
        .item("at", attr_s(&now.to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "stale PRs: could not keep the pull request open");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    log(
        state,
        team_id,
        repo,
        pr,
        Action::KeptOpen,
        title,
        url,
        by,
        via,
    )
    .await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn log(
    state: &AppState,
    team_id: &str,
    repo: &str,
    pr: u64,
    action: Action,
    title: &str,
    url: &str,
    by: &str,
    via: &str,
) {
    let now = chrono::Utc::now();
    let why = match (action, via) {
        (Action::KeptOpen, "comment") => format!("{by} commented @coderhelm keep open"),
        (Action::KeptOpen, _) => format!("{by} kept it open from the dashboard"),
        _ => format!("{by} let it be closed again"),
    };
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&stale::log_sk(&now, repo, pr)))
        .item("repo", attr_s(repo))
        .item("pr_number", AttributeValue::N(pr.to_string()))
        .item("action", attr_s(action.as_str()))
        .item("title", attr_s(common::truncate_str(title, 300)))
        .item("url", attr_s(url))
        .item("why", attr_s(&why))
        .item("by", attr_s(by))
        .item("at", attr_s(&now.to_rfc3339()))
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + stale::LOG_TTL_DAYS * 86_400).to_string()),
        )
        .send()
        .await
    {
        warn!(error = %e, "stale PRs: could not log");
    }
}

/// PUT /api/reviewer/stale/keep-open
pub async fn put_keep_open(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<KeepOpenBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let repo = repo_param(Some(&body.repo))?.ok_or(StatusCode::BAD_REQUEST)?;
    if body.pr == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let url = if body.url.starts_with("https://github.com/") {
        body.url.clone()
    } else {
        format!("https://github.com/{repo}/pull/{}", body.pr)
    };
    if body.keep {
        keep_open(
            &state,
            &claims.team_id,
            &repo,
            body.pr,
            &body.title,
            &url,
            &claims.email,
            "dashboard",
        )
        .await?;
    } else {
        state
            .dynamo
            .delete_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&stale::exempt_sk(&repo, body.pr)))
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, "stale PRs: could not drop the kept-open record");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        log(
            &state,
            &claims.team_id,
            &repo,
            body.pr,
            Action::AllowedToClose,
            &body.title,
            &url,
            &claims.email,
            "dashboard",
        )
        .await;
    }
    info!(team = %claims.team_id, repo, pr = body.pr, keep = body.keep, by = %claims.email, "Stale PR kept-open changed");
    Ok(Json(json!({ "pr": body.pr, "keep": body.keep })))
}

/// Pure: may this commenter keep the pull request open? Its author, or
/// anyone with write access.
pub(crate) fn may_keep_open(commenter: &str, pr_author: &str, permission: Option<&Value>) -> bool {
    (!commenter.is_empty() && commenter.eq_ignore_ascii_case(pr_author))
        || permission.is_some_and(common::merge_gate::has_write_access)
}

/// `@coderhelm keep open` on a pull request.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn on_keep_open_comment(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr: &Value,
    commenter: &str,
    comment_id: u64,
) -> Result<StatusCode, StatusCode> {
    let number = pr["number"].as_u64().unwrap_or(0);
    if number == 0 || !super::merge_control::valid_login(commenter) {
        return Ok(StatusCode::OK);
    }
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(owner, name, error = %e, "keep open: no installation token");
            return Err(StatusCode::BAD_GATEWAY);
        }
    };
    let api = format!("https://api.github.com/repos/{owner}/{name}");
    let author = pr["user"]["login"].as_str().unwrap_or("");
    let permission = if commenter.eq_ignore_ascii_case(author) {
        None
    } else {
        match super::merge_control::gh_get(
            state,
            &token,
            &format!("{api}/collaborators/{commenter}/permission"),
        )
        .await
        {
            Ok((code, body)) if code < 300 => Some(body),
            _ => None,
        }
    };
    if !may_keep_open(commenter, author, permission.as_ref()) {
        info!(
            owner,
            name,
            pr = number,
            commenter,
            "keep open: refused (not the author, no write access)"
        );
        super::merge_control::gh_post(
            state,
            &token,
            &format!("{api}/issues/{number}/comments"),
            &json!({ "body": format!("@{commenter} Only the author of this pull request or someone with write access can keep it open. Nothing changed.") }),
        )
        .await;
        return Ok(StatusCode::OK);
    }
    let repo = format!("{owner}/{name}");
    let title = pr["title"].as_str().unwrap_or("");
    let url = pr["html_url"].as_str().unwrap_or("");
    keep_open(
        state, team_id, &repo, number, title, url, commenter, "comment",
    )
    .await?;
    super::merge_control::gh_post(
        state,
        &token,
        &format!("{api}/issues/comments/{comment_id}/reactions"),
        &json!({ "content": "+1" }),
    )
    .await;
    info!(
        owner,
        name,
        pr = number,
        commenter,
        "keep open: kept open from a comment"
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_plan_says_the_settings_as_they_are_now() {
        let plan = json!({"repo": "acme/web", "enabled": true, "dry_run": false,
            "items": [{"pr": 1, "action": "wait"}]});
        let off = StaleConfig {
            enabled: false,
            ..StaleConfig::default()
        };
        let p = with_current_settings(plan.clone(), &|_| off.clone());
        assert_eq!(p["enabled"], false, "swept while on, it says off once off");
        assert_eq!(p["dry_run"], true, "nothing closes while off");
        assert_eq!(p["items"].as_array().unwrap().len(), 1);
        let preview = StaleConfig {
            enabled: true,
            dry_run: true,
            ..StaleConfig::default()
        };
        let p = with_current_settings(plan, &|_| preview.clone());
        assert_eq!(p["enabled"], true);
        assert_eq!(p["dry_run"], true);
        assert_eq!(p["items"].as_array().unwrap().len(), 1);
    }

    use super::*;

    fn cfg_item(sk: &str, json_text: &str) -> Item {
        HashMap::from([
            ("pk".to_string(), attr_s("t_1")),
            ("sk".to_string(), attr_s(sk)),
            (stale::ATTR_CONFIG.to_string(), attr_s(json_text)),
        ])
    }

    #[test]
    fn repo_configs_list_each_repository_with_its_own_settings() {
        let items = vec![
            cfg_item(
                "STALE_PR_CONFIG#REPO#acme/web",
                r#"{"enabled":true,"dry_run":true}"#,
            ),
            cfg_item("STALE_PR_CONFIG#REPO#acme/api", r#"{"enabled":false}"#),
            // The team's record and a broken one are not repositories.
            cfg_item(stale::TEAM_SK, r#"{"enabled":true}"#),
            cfg_item("STALE_PR_CONFIG#REPO#acme/bad", "not json"),
        ];
        let v = repo_configs_of(&items);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0]["repo"], "acme/api");
        assert_eq!(v[0]["config"]["enabled"], false);
        assert_eq!(v[1]["repo"], "acme/web");
        assert_eq!(v[1]["config"]["enabled"], true);
        assert_eq!(v[1]["config"]["dry_run"], true);
        // A record without a stale_days keeps the new default, 7.
        assert_eq!(v[1]["config"]["stale_days"], 7);
    }

    #[test]
    fn repo_params_are_checked() {
        assert_eq!(repo_param(None).unwrap(), None);
        assert_eq!(repo_param(Some(" ")).unwrap(), None);
        assert_eq!(
            repo_param(Some("acme/web")).unwrap().as_deref(),
            Some("acme/web")
        );
        assert!(repo_param(Some("acme")).is_err());
        assert!(repo_param(Some("acme/../x")).is_err());
        assert!(repo_param(Some("acme/web#1")).is_err());
    }

    #[test]
    fn plan_items_carry_their_repo_days_quiet_and_closing_date() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-11T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let wait = plan_item_json(
            json!({"pr": 4, "action": "wait", "last_activity": "2026-10-20T11:00:00Z", "next_at": "2026-11-14T09:00:00Z"}),
            "acme/web",
            &now,
        );
        assert_eq!(wait["repo"], "acme/web");
        assert_eq!(wait["days_quiet"], 22);
        assert_eq!(wait["closes_at"], "2026-11-14T09:00:00Z");
        let exempt = plan_item_json(
            json!({"pr": 5, "action": "exempt", "last_activity": "2026-11-12T00:00:00Z", "next_at": "x"}),
            "acme/web",
            &now,
        );
        assert_eq!(exempt["days_quiet"], 0, "a clock ahead never goes negative");
        assert!(exempt["closes_at"].is_null());
        let bad = plan_item_json(json!({"pr": 6, "action": "mark"}), "acme/api", &now);
        assert!(bad["days_quiet"].is_null() && bad["closes_at"].is_null());
    }

    #[test]
    fn a_saved_plan_names_its_repo_and_state() {
        let mut i = Item::new();
        i.insert(
            "plan".into(),
            attr_s(r#"[{"pr":1,"action":"close","next_at":"2026-11-11T12:00:00Z"}]"#),
        );
        i.insert("dry_run".into(), AttributeValue::Bool(false));
        i.insert("enabled".into(), AttributeValue::Bool(true));
        let now = chrono::Utc::now();
        let p = plan_json(&i, "acme/web", &now);
        assert_eq!(p["repo"], "acme/web");
        assert_eq!(p["dry_run"], false);
        assert_eq!(p["enabled"], true);
        assert_eq!(p["items"][0]["repo"], "acme/web");
        assert_eq!(p["items"][0]["closes_at"], "2026-11-11T12:00:00Z");
        let empty = plan_json(&Item::new(), "acme/x", &now);
        assert_eq!(empty["dry_run"], true);
        assert_eq!(empty["items"], json!([]));
    }

    #[test]
    fn the_author_or_a_writer_may_keep_it_open() {
        let write = json!({"role_name": "write", "permission": "write"});
        let read = json!({"role_name": "read", "permission": "read"});
        assert!(may_keep_open("Ana", "ana", None));
        assert!(may_keep_open("bo", "ana", Some(&write)));
        assert!(!may_keep_open("bo", "ana", Some(&read)));
        assert!(!may_keep_open("bo", "ana", None));
        assert!(!may_keep_open("", "", None));
    }

    #[test]
    fn a_saved_body_is_sanitized_and_null_drops_a_repo_override() {
        let b: PutConfig =
            serde_json::from_value(json!({"repo": "acme/web", "config": null})).unwrap();
        assert!(b.config.is_none());
        let b: PutConfig = serde_json::from_value(
            json!({"config": {"enabled": true, "stale_days": 0, "label": ""}}),
        )
        .unwrap();
        let c = b.config.unwrap().sanitized();
        assert!(c.enabled && c.stale_days == 1 && c.label == "stale" && c.dry_run);
    }
}
