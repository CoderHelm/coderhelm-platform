//! Stale pull request cleanup from the dashboard and from a PR comment
//! (`common::stale_prs` has the rules, `worker::passes::stale_prs` the
//! sweep).
//!
//! - `GET /api/reviewer/stale/config?repo=` — the team's settings, the
//!   repository's own (when it has them) and what applies.
//! - `PUT /api/reviewer/stale/config` — save the team's settings, or one
//!   repository's (`config: null` drops them so it follows the team). Admin.
//! - `GET /api/reviewer/stale?repo=` — the latest plan for the repository,
//!   the actions taken and the pull requests kept open.
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
    Ok(Json(json!({
        "team": team.clone().unwrap_or_default(),
        "team_saved": team.is_some(),
        "repo": own,
        "effective": effective,
        "defaults": StaleConfig::default(),
    })))
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
pub async fn get_activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_param(q.repo.as_deref())?;
    let plan = match &repo {
        Some(r) => get(&state, &claims.team_id, &stale::preview_sk(r)).await?.map(|i| {
            let items: Value = serde_json::from_str(&s(&i, "plan")).unwrap_or(json!([]));
            json!({
                "computed_at": s(&i, "computed_at"),
                "dry_run": i.get("dry_run").and_then(|v| v.as_bool().ok()).copied().unwrap_or(true),
                "stopped": s(&i, "stopped"),
                "items": items,
            })
        }),
        None => None,
    };
    let log = read_log(&state, &claims.team_id, repo.as_deref(), 50).await?;
    let kept = match &repo {
        Some(r) => read_kept(&state, &claims.team_id, r).await?,
        None => vec![],
    };
    Ok(Json(json!({ "plan": plan, "log": log, "kept_open": kept })))
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

async fn read_kept(state: &AppState, team_id: &str, repo: &str) -> Result<Vec<Value>, StatusCode> {
    let resp = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
        .expression_attribute_values(":p", attr_s(team_id))
        .expression_attribute_values(":pfx", attr_s(&stale::exempt_prefix(repo)))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "stale PRs: could not read the kept-open list");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(resp
        .items()
        .iter()
        .map(|i| {
            json!({
                "pr": n(i, "pr_number"),
                "title": s(i, "title"),
                "url": s(i, "url"),
                "by": s(i, "by"),
                "via": s(i, "via"),
                "at": s(i, "at"),
            })
        })
        .collect())
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
    use super::*;

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
