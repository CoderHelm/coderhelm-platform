//! A team's skills from a repository: which repositories are connected, what
//! was read from them, and each item's switch.
//!
//! Admins connect a repository the team's GitHub App already covers; the
//! worker reads its default branch (merged code only) now, on every push to
//! it, and every 20 minutes when it moved. Everyone on the team can see the
//! items; only admins connect, disconnect, sync or switch them. Reading and
//! limits live in `common::skills`. Nothing from a repository is ever run.
//!
//! An agent's page shows the questions Coderhelm's agents put to it and the
//! answers, and lets an admin try it with a question of their own. A try runs
//! in the worker exactly as an agent's ask does: no tools, one turn.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::skills::{
    new_ask_id, usage_stats, valid_repo, AccessFilter, AgentAsk, Kind, SkillAskMessage, SkillStore,
    SkillSyncMessage, ACCESS_DAYS, MAX_REPOS, STAT_DAYS,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

use crate::models::{Claims, WorkerMessage};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
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
            "Only admins can manage skills.",
        ))
    }
}

fn store<'a>(state: &'a AppState, claims: &'a Claims) -> SkillStore<'a> {
    SkillStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: &claims.team_id,
    }
}

async fn queue_sync(state: &AppState, team: &str, repo: &str) -> bool {
    let message = WorkerMessage::SkillSync(SkillSyncMessage {
        team_id: team.to_string(),
        repo: repo.to_string(),
        force: true,
        ..Default::default()
    });
    crate::routes::github_webhook::send_to_queue(state, &state.config.ticket_queue_url, &message)
        .await
        .is_ok()
}

/// GET /api/skills — the connected repositories and every item read from
/// them (without their text).
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = store(&state, &claims);
    let repos = st
        .repos()
        .await
        .map_err(|e| internal("list skill repos", e))?;
    let items = st.items().await.map_err(|e| internal("list skills", e))?;
    Ok(Json(json!({
        "repos": repos.iter().map(|r| r.json()).collect::<Vec<_>>(),
        "items": items.iter().map(|i| i.json()).collect::<Vec<_>>(),
        "can_manage": matches!(claims.role.as_str(), "admin" | "owner"),
    })))
}

/// GET /api/skills/stats — how the team's skills and agents were used over
/// the last 30 days: reads, asks, who made them, and what went unused.
pub async fn stats(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = store(&state, &claims);
    let today = chrono::Utc::now().date_naive();
    let since = (today - chrono::Duration::days(STAT_DAYS - 1))
        .format("%Y-%m-%d")
        .to_string();
    let (items, reads, asks) = tokio::try_join!(st.items(), st.read_days(&since), st.recent_asks())
        .map_err(|e| internal("skill stats", e))?;
    Ok(Json(usage_stats(&items, &reads, &asks, today)))
}

/// The access log filter a request's query asks for. Unknown values are
/// kept as given and simply match nothing.
fn access_filter(q: &HashMap<String, String>) -> AccessFilter {
    let get = |k: &str| q.get(k).map(|v| v.trim().to_string()).unwrap_or_default();
    AccessFilter {
        repo: get("repo"),
        path: get("path"),
        by: get("by"),
        action: get("action"),
        outcome: get("outcome"),
        q: common::truncate_str(&get("q"), 200).to_string(),
        since: get("since"),
        until: get("until"),
    }
}

/// GET /api/skills/activity?[repo=&path=][&by=][&action=][&outcome=][&q=]
/// [&since=&until=][&limit=][&before=] — every read and ask, newest first:
/// what was read or asked, by whom, during which work, why, and how it went.
/// `next` is the cursor for the page after, or null at the end.
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let limit = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
    let before = q
        .get("before")
        .map(String::as_str)
        .filter(|b| !b.is_empty());
    let (list, next) = store(&state, &claims)
        .access_page(&access_filter(&q), limit, before)
        .await
        .map_err(|e| internal("skill access log", e))?;
    Ok(Json(json!({
        "events": list.iter().map(|a| a.json()).collect::<Vec<_>>(),
        "next": next,
        "kept_days": ACCESS_DAYS,
    })))
}

/// Whether the team's GitHub App covers `repo` (it has a repository record,
/// which only the app's installation writes).
async fn team_has_repo(state: &AppState, team: &str, repo: &str) -> Result<bool, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.repos_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("REPO#{repo}")),
        )
        .projection_expression("sk")
        .send()
        .await
        .map(|o| o.item().is_some())
        .map_err(|e| format!("repo lookup: {e}"))
}

/// POST /api/skills/repos `{repo: "owner/name"}` — connects a repository the
/// team's GitHub App covers and reads it now.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let repo = body["repo"].as_str().unwrap_or("").trim().to_string();
    if !valid_repo(&repo) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Give the repository as owner/name.",
        ));
    }
    let st = store(&state, &claims);
    let existing = st
        .repos()
        .await
        .map_err(|e| internal("list skill repos", e))?;
    let already = existing.iter().any(|r| r.repo.eq_ignore_ascii_case(&repo));
    if !already && existing.len() >= MAX_REPOS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("A team can connect at most {MAX_REPOS} skill repositories."),
        ));
    }
    if !team_has_repo(&state, &claims.team_id, &repo)
        .await
        .map_err(|e| internal("check repo", e))?
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Coderhelm's GitHub App isn't installed on that repository. Add it to the app's \
             repositories in GitHub, then try again.",
        ));
    }
    let installation_id = crate::routes::api::get_team_installation_id(&state, &claims.team_id)
        .await
        .map_err(|_| {
            fail(
                StatusCode::BAD_REQUEST,
                "This team has no GitHub App installation.",
            )
        })?;
    let by = if claims.email.is_empty() {
        claims.sub.clone()
    } else {
        claims.email.clone()
    };
    st.connect(&repo, installation_id, &by)
        .await
        .map_err(|e| internal("connect skill repo", e))?;
    let queued = queue_sync(&state, &claims.team_id, &repo).await;
    info!(team_id = %claims.team_id, repo = %repo, "Skill repository connected");
    Ok(Json(json!({ "ok": true, "repo": repo, "syncing": queued })))
}

/// DELETE /api/skills/repos/:owner/:name — disconnects a repository and
/// forgets everything read from it.
pub async fn disconnect(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    admin(&claims)?;
    let repo = format!("{owner}/{name}");
    if !valid_repo(&repo) {
        return Err(fail(StatusCode::BAD_REQUEST, "That isn't a repository."));
    }
    store(&state, &claims)
        .disconnect(&repo)
        .await
        .map_err(|e| internal("disconnect skill repo", e))?;
    info!(team_id = %claims.team_id, repo = %repo, "Skill repository disconnected");
    Ok(Json(json!({ "ok": true })))
}

/// POST /api/skills/repos/:owner/:name/sync — reads the repository again now.
pub async fn sync(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    admin(&claims)?;
    let repo = format!("{owner}/{name}");
    let st = store(&state, &claims);
    if !valid_repo(&repo)
        || st
            .repo(&repo)
            .await
            .map_err(|e| internal("get skill repo", e))?
            .is_none()
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "That repository isn't connected.",
        ));
    }
    if !queue_sync(&state, &claims.team_id, &repo).await {
        return Err(fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't start the sync. Try again.",
        ));
    }
    Ok(Json(json!({ "ok": true, "syncing": true })))
}

/// GET /api/skills/item?repo=&path= — one item with its text and the files
/// kept next to it.
pub async fn item(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let repo = q.get("repo").map(String::as_str).unwrap_or("");
    let path = q.get("path").map(String::as_str).unwrap_or("");
    if !valid_repo(repo) || path.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Give a repo and a path."));
    }
    let Some((meta, body, compat, translated)) = store(&state, &claims)
        .item_with_compat(repo, path)
        .await
        .map_err(|e| internal("get skill", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "No such skill."));
    };
    let mut out = meta.json();
    // The full check (steps, what each relies on) and the translated text,
    // when the item was checked. `compat.current` says whether it was its
    // current text.
    if let Some(full) = serde_json::from_str::<Value>(&compat)
        .ok()
        .filter(|v| v.is_object())
    {
        out["compat"] = full_compat(meta.compat_json(), &full);
    }
    out["translated"] = json!(translated);
    if meta.kind == Kind::Agent {
        // Exactly what the agent is told when it's asked, so the page shows
        // what it gets rather than a description of it.
        let (_, system) = common::skills::agent_system_prompt(&body, "reviewer");
        out["prompt"] = json!(system);
    }
    out["body"] = json!(body);
    Ok(Json(out))
}

/// The short check with the full one's steps, counts and cost added, and
/// its list of what to connect as `needs_detail` (the short `needs` stays a
/// list of names).
fn full_compat(short: Value, full: &Value) -> Value {
    let mut c = short;
    if let (Some(obj), Some(full)) = (c.as_object_mut(), full.as_object()) {
        for (k, v) in full {
            if k == "needs" {
                obj.insert("needs_detail".into(), v.clone());
            } else {
                obj.entry(k.clone()).or_insert(v.clone());
            }
        }
    }
    c
}

/// PUT /api/skills/item `{repo, path, enabled}` — switches an item on or off
/// for the agents. Kept across syncs.
pub async fn set_enabled(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let repo = body["repo"].as_str().unwrap_or("");
    let path = body["path"].as_str().unwrap_or("");
    let Some(on) = body["enabled"].as_bool() else {
        return Err(fail(StatusCode::BAD_REQUEST, "Say enabled: true or false."));
    };
    if !valid_repo(repo) || path.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Give a repo and a path."));
    }
    let found = store(&state, &claims)
        .set_enabled(repo, path, on, &claims.email)
        .await
        .map_err(|e| internal("switch skill", e))?;
    if !found {
        return Err(fail(StatusCode::NOT_FOUND, "No such skill."));
    }
    info!(team_id = %claims.team_id, repo, path, enabled = on, "Skill switched");
    Ok(Json(json!({ "ok": true, "enabled": on })))
}

/// POST /api/skills/recheck `{repo, path?}` — checks a repository's items
/// (or one) again against what the team has in Coderhelm, now. One model
/// call per item, done by the worker.
pub async fn recheck(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let repo = body["repo"].as_str().unwrap_or("").to_string();
    let path = body["path"].as_str().unwrap_or("").trim().to_string();
    let st = store(&state, &claims);
    if !valid_repo(&repo)
        || st
            .repo(&repo)
            .await
            .map_err(|e| internal("get skill repo", e))?
            .is_none()
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "That repository isn't connected.",
        ));
    }
    if !path.is_empty()
        && st
            .item(&repo, &path)
            .await
            .map_err(|e| internal("get skill", e))?
            .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "No such skill."));
    }
    let message = WorkerMessage::SkillSync(SkillSyncMessage {
        team_id: claims.team_id.clone(),
        repo: repo.clone(),
        recheck: true,
        path: path.clone(),
        ..Default::default()
    });
    if crate::routes::github_webhook::send_to_queue(
        &state,
        &state.config.ticket_queue_url,
        &message,
    )
    .await
    .is_err()
    {
        return Err(fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't start the check. Try again.",
        ));
    }
    info!(team_id = %claims.team_id, repo = %repo, path = %path, "Skill re-check queued");
    Ok(Json(json!({ "ok": true, "checking": true })))
}

/// PUT /api/skills/translated `{repo, path, enabled}` — whether agents read
/// the translated version of an item instead of its text as written. Kept
/// across syncs; agents only get a translation made from the current text.
pub async fn set_use_translated(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let repo = body["repo"].as_str().unwrap_or("");
    let path = body["path"].as_str().unwrap_or("");
    let Some(on) = body["enabled"].as_bool() else {
        return Err(fail(StatusCode::BAD_REQUEST, "Say enabled: true or false."));
    };
    if !valid_repo(repo) || path.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Give a repo and a path."));
    }
    let st = store(&state, &claims);
    if on {
        match st
            .item_with_compat(repo, path)
            .await
            .map_err(|e| internal("get skill", e))?
        {
            None => return Err(fail(StatusCode::NOT_FOUND, "No such skill.")),
            Some((_, _, _, t)) if t.trim().is_empty() => {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "There's no translated version yet. Check it first.",
                ))
            }
            Some(_) => {}
        }
    }
    let found = st
        .set_use_translated(repo, path, on, &claims.email)
        .await
        .map_err(|e| internal("switch translated", e))?;
    if !found {
        return Err(fail(StatusCode::NOT_FOUND, "No such skill."));
    }
    info!(team_id = %claims.team_id, repo, path, enabled = on, "Skill translated version switched");
    Ok(Json(json!({ "ok": true, "use_translated": on })))
}

/// Tries one agent may take in a day, so the try box can't run up a bill.
const TRIES_PER_DAY: usize = 20;
const TRY_QUESTION_CHARS: usize = 8_000;
const TRY_CONTEXT_CHARS: usize = 16_000;

fn repo_and_path(
    q: &HashMap<String, String>,
) -> Result<(String, String), (StatusCode, Json<Value>)> {
    let repo = q.get("repo").map(String::as_str).unwrap_or("");
    let path = q.get("path").map(String::as_str).unwrap_or("");
    if !valid_repo(repo) || path.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Give a repo and a path."));
    }
    Ok((repo.to_string(), path.to_string()))
}

/// GET /api/skills/asks?repo=&path=[&id=] — an agent's recent asks, newest
/// first, or the one `id` names (the page waits on a try with it).
/// GET /api/skills/asks?run_id= — the asks made during one run.
pub async fn asks(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    if let Some(run_id) = q.get("run_id").filter(|r| !r.is_empty()) {
        let list = store(&state, &claims)
            .asks_for_run(run_id)
            .await
            .map_err(|e| internal("list run asks", e))?;
        return Ok(Json(json!({
            "asks": list.iter().map(|a| a.json()).collect::<Vec<_>>(),
        })));
    }
    let (repo, path) = repo_and_path(&q)?;
    let st = store(&state, &claims);
    if let Some(id) = q.get("id") {
        let Some(a) = st
            .ask(&repo, &path, id)
            .await
            .map_err(|e| internal("get agent ask", e))?
        else {
            return Err(fail(StatusCode::NOT_FOUND, "No such ask."));
        };
        return Ok(Json(a.json()));
    }
    let list = st
        .asks(&repo, &path, 30)
        .await
        .map_err(|e| internal("list agent asks", e))?;
    let now = chrono::Utc::now();
    let tried = list.iter().filter(|a| try_today(a, now)).count();
    Ok(Json(json!({
        "asks": list.iter().map(|a| a.json()).collect::<Vec<_>>(),
        "tries_per_day": TRIES_PER_DAY,
        "tries_left": TRIES_PER_DAY.saturating_sub(tried),
    })))
}

/// Whether an ask was an admin's try made in the last day.
fn try_today(a: &AgentAsk, now: chrono::DateTime<chrono::Utc>) -> bool {
    a.by.ends_with("(try)")
        && chrono::DateTime::parse_from_rfc3339(&a.at)
            .map(|at| now.signed_duration_since(at) < chrono::Duration::days(1))
            .unwrap_or(false)
}

/// Whether a try is still waiting for its answer. An answer takes at most two
/// minutes, so one pending longer than five was lost and doesn't block.
fn waiting(a: &AgentAsk, now: chrono::DateTime<chrono::Utc>) -> bool {
    a.outcome == "pending"
        && chrono::DateTime::parse_from_rfc3339(&a.at)
            .map(|at| now.signed_duration_since(at) < chrono::Duration::minutes(5))
            .unwrap_or(false)
}

/// POST /api/skills/asks `{repo, path, question, context?}` — an admin tries
/// a switched-on agent. Returns the ask's id; the answer lands on it.
pub async fn try_agent(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let repo = body["repo"].as_str().unwrap_or("").to_string();
    let path = body["path"].as_str().unwrap_or("").to_string();
    if !valid_repo(&repo) || path.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "Give a repo and a path."));
    }
    let question = body["question"].as_str().unwrap_or("").trim().to_string();
    if question.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Write a question for the agent.",
        ));
    }
    if question.len() > TRY_QUESTION_CHARS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That question is too long. Keep it under 8,000 characters.",
        ));
    }
    let context = body["context"].as_str().unwrap_or("").trim().to_string();
    if context.len() > TRY_CONTEXT_CHARS {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That context is too long. Keep it under 16,000 characters.",
        ));
    }
    let st = store(&state, &claims);
    match st
        .item(&repo, &path)
        .await
        .map_err(|e| internal("get skill", e))?
    {
        Some((m, _)) if m.kind != Kind::Agent => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Only agents can be asked a question.",
            ))
        }
        Some((m, _)) if !m.enabled => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "Switch this agent on to try it.",
            ))
        }
        Some(_) => {}
        None => return Err(fail(StatusCode::NOT_FOUND, "No such agent.")),
    }
    if st
        .repo(&repo)
        .await
        .map_err(|e| internal("get skill repo", e))?
        .is_none()
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "That repository isn't connected.",
        ));
    }
    let recent = st
        .asks(&repo, &path, 50)
        .await
        .map_err(|e| internal("list agent asks", e))?;
    let now = chrono::Utc::now();
    if recent.iter().any(|a| waiting(a, now)) {
        return Err(fail(
            StatusCode::CONFLICT,
            "A try is still waiting for its answer.",
        ));
    }
    if recent.iter().filter(|a| try_today(a, now)).count() >= TRIES_PER_DAY {
        return Err(fail(
            StatusCode::TOO_MANY_REQUESTS,
            "This agent has been tried 20 times today. Try again tomorrow.",
        ));
    }
    let ask = AgentAsk {
        id: new_ask_id(),
        repo: repo.clone(),
        path: path.clone(),
        by: format!("{} (try)", claims.email),
        question: question.clone(),
        context_chars: context.len() as i64,
        outcome: "pending".into(),
        ..Default::default()
    };
    st.put_ask(&ask)
        .await
        .map_err(|e| internal("save agent try", e))?;
    let message = WorkerMessage::SkillAsk(SkillAskMessage {
        team_id: claims.team_id.clone(),
        repo,
        path,
        id: ask.id.clone(),
        question,
        context,
    });
    if crate::routes::github_webhook::send_to_queue(
        &state,
        &state.config.ticket_queue_url,
        &message,
    )
    .await
    .is_err()
    {
        let failed = AgentAsk {
            outcome: "failed".into(),
            answer: "Couldn't reach the agent. Try again.".into(),
            ..ask
        };
        let _ = st.put_ask(&failed).await;
        return Err(fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't reach the agent. Try again.",
        ));
    }
    info!(team_id = %claims.team_id, agent = %ask.path, "Agent try queued");
    Ok(Json(json!({ "id": ask.id })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(by: &str, hours_ago: i64) -> AgentAsk {
        AgentAsk {
            by: by.into(),
            at: (chrono::Utc::now() - chrono::Duration::hours(hours_ago)).to_rfc3339(),
            ..Default::default()
        }
    }

    #[test]
    fn an_items_page_gets_the_full_check_with_names_kept_short() {
        let short = json!({ "status": "needs_tools", "needs": ["Monitoring"], "current": true });
        let full = json!({
            "status": "ignored", "steps": [{ "title": "a" }],
            "needs": [{ "name": "Monitoring", "fix": "Connect it" }], "cost_usd": 0.02
        });
        let c = full_compat(short, &full);
        assert_eq!(c["status"], "needs_tools", "the short form leads");
        assert_eq!(c["needs"], json!(["Monitoring"]));
        assert_eq!(c["needs_detail"][0]["fix"], "Connect it");
        assert_eq!(c["steps"][0]["title"], "a");
        assert_eq!(c["cost_usd"], 0.02);
        assert_eq!(c["current"], true);
    }

    #[test]
    fn only_an_admins_tries_in_the_last_day_count() {
        let now = chrono::Utc::now();
        assert!(try_today(&ask("a@b.co (try)", 2), now));
        assert!(
            !try_today(&ask("a@b.co (try)", 25), now),
            "older than a day"
        );
        assert!(
            !try_today(&ask("reviewer", 1), now),
            "an agent's ask isn't a try"
        );
        assert!(!try_today(
            &AgentAsk {
                by: "a@b.co (try)".into(),
                ..Default::default()
            },
            now
        ));
    }

    #[test]
    fn a_lost_try_stops_blocking_after_five_minutes() {
        let now = chrono::Utc::now();
        let pending = |minutes: i64| AgentAsk {
            outcome: "pending".into(),
            at: (now - chrono::Duration::minutes(minutes)).to_rfc3339(),
            ..Default::default()
        };
        assert!(waiting(&pending(1), now));
        assert!(!waiting(&pending(6), now));
        assert!(!waiting(
            &AgentAsk {
                outcome: "answered".into(),
                ..pending(1)
            },
            now
        ));
    }
}
