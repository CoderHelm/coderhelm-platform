//! Dashboard API for the CODE GRAPH — its own feature, used by the planner,
//! the coding agent, CI fixes, review replies and the reviewer. Every repo the
//! team turned on gets a graph unless it is turned off for that repo. Read
//! endpoints power the /graph page (each repo's build status, load-bearing
//! files, symbol search, neighborhood explorer); the PUT turns a repo's graph
//! on or off and the POST rebuilds it. Storage: see `common::code_graph`.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

fn item_s(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn item_n(item: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Reject repo path params that could inject into a DynamoDB key.
fn validate_repo(repo: &str) -> Result<(), StatusCode> {
    if repo.is_empty()
        || repo.len() > 200
        || repo.contains("..")
        || repo.contains('\0')
        || !repo
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

async fn get_graph_item(
    state: &AppState,
    pk: &str,
    sk: &str,
) -> Option<HashMap<String, AttributeValue>> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(pk))
        .key("sk", attr_s(sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
}

/// Bounded prefix query in a graph partition.
async fn query_prefix(
    state: &AppState,
    pk: &str,
    prefix: &str,
    limit: i32,
) -> Vec<HashMap<String, AttributeValue>> {
    state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(pk))
        .expression_attribute_values(":p", attr_s(prefix))
        .limit(limit)
        .send()
        .await
        .map(|o| o.items().to_vec())
        .unwrap_or_default()
}

/// Every repo the code graph page lists: the repos the team turned on, the
/// reviewer's repos, and any repo whose graph switch was set. Lowercased keys
/// dedupe; the first spelling seen wins.
async fn graph_repos(state: &AppState, team_id: &str) -> Result<Vec<String>, StatusCode> {
    let mut repos: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut push = |r: String, repos: &mut Vec<String>| {
        if !r.is_empty() && seen.insert(r.to_lowercase()) {
            repos.push(r);
        }
    };
    let turned_on = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":p", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("graph repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    for it in turned_on.items() {
        if it.get("enabled").and_then(|v| v.as_bool().ok()) == Some(&true) {
            push(item_s(it, "sk").replace("REPO#", ""), &mut repos);
        }
    }
    let reviewer = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":p", attr_s("REVIEW_CONFIG#REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("graph repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    for it in reviewer.items() {
        push(
            item_s(it, "sk").replace("REVIEW_CONFIG#REPO#", ""),
            &mut repos,
        );
    }
    let switches =
        common::code_graph::switches(&state.dynamo, &state.config.settings_table_name, team_id)
            .await
            .map_err(|e| {
                error!("graph repos: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    for r in switches.into_keys() {
        push(r, &mut repos);
    }
    repos.sort_by_key(|r| r.to_lowercase());
    Ok(repos)
}

/// One repo's row: its switch, build status and the built graph's stats.
async fn repo_row(state: &AppState, team_id: &str, repo: &str) -> Value {
    use common::code_graph as cg;
    let table = &state.config.settings_table_name;
    let pk = cg::graph_pk(team_id, repo);
    let (enabled, status, meta) = tokio::join!(
        cg::is_enabled(&state.dynamo, table, team_id, repo),
        cg::status(&state.dynamo, table, team_id, repo),
        get_graph_item(state, &pk, cg::META_SK),
    );
    let shown = cg::shown(
        enabled,
        status.as_ref(),
        meta.is_some(),
        chrono::Utc::now().timestamp(),
    );
    let reason = match &shown {
        cg::Shown::Failed(why) => why.clone(),
        _ => String::new(),
    };
    // A rebuild or update running over a built graph.
    let updating = meta.is_some()
        && matches!(
            status.as_ref().map(|s| s.state),
            Some(cg::BuildState::Queued | cg::BuildState::Indexing)
        )
        && !status
            .as_ref()
            .is_some_and(|s| s.is_stale(chrono::Utc::now().timestamp()));
    json!({
        "repo": repo,
        "graph_enabled": enabled,
        "state": shown.as_str(),
        "reason": reason,
        "updating": updating,
        "indexed": meta.is_some(),
        "files": meta.as_ref().map(|m| item_n(m, "files")).unwrap_or(0),
        "symbols": meta.as_ref().map(|m| item_n(m, "symbols")).unwrap_or(0),
        "branch": meta.as_ref().map(|m| item_s(m, "branch")).unwrap_or_default(),
        "indexed_sha": meta.as_ref().map(|m| item_s(m, "indexed_sha")).unwrap_or_default(),
        "updated_at": meta.as_ref().map(|m| item_s(m, "updated_at")).unwrap_or_default(),
    })
}

/// GET /api/graph/repos — every repo CoderHelm works in, with its graph's
/// switch, build status (off, not built, queued, indexing, ready, failed with
/// the reason) and, once built, its stats. Drives the /graph page.
pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let repos = graph_repos(&state, &claims.team_id).await?;
    let rows =
        futures::future::join_all(repos.iter().map(|r| repo_row(&state, &claims.team_id, r))).await;
    Ok(Json(json!({ "repos": rows })))
}

#[derive(serde::Deserialize)]
pub struct RepoQuery {
    pub repo: String,
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub path: String,
}

/// GET /api/graph/status?repo=owner/name — META + the precomputed top-files
/// summary (load-bearing files by PageRank).
pub async fn status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo(&q.repo)?;
    let pk = format!("CG#{}#{}", claims.team_id, q.repo);
    let Some(meta) = get_graph_item(&state, &pk, "META").await else {
        return Ok(Json(json!({ "indexed": false })));
    };
    let top: Value = get_graph_item(&state, &pk, "SUMMARY")
        .await
        .map(|s| serde_json::from_str(&item_s(&s, "top_files")).unwrap_or(json!([])))
        .unwrap_or(json!([]));
    Ok(Json(json!({
        "indexed": true,
        "files": item_n(&meta, "files"),
        "symbols": item_n(&meta, "symbols"),
        "names_referenced": item_n(&meta, "names_referenced"),
        "branch": item_s(&meta, "branch"),
        "indexed_sha": item_s(&meta, "indexed_sha"),
        "updated_at": item_s(&meta, "updated_at"),
        "top_files": top,
    })))
}

/// GET /api/graph/symbol?repo=owner/name&q=name — prefix search over symbol
/// definitions; exact matches also return their caller files.
pub async fn symbol(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo(&q.repo)?;
    let term: String = q.q.trim().chars().take(100).collect();
    if term.is_empty() {
        return Ok(Json(json!({ "definitions": [], "callers": [] })));
    }
    let pk = format!("CG#{}#{}", claims.team_id, q.repo);
    let defs: Vec<Value> = query_prefix(&state, &pk, &format!("NM#{term}"), 25)
        .await
        .into_iter()
        .filter_map(|it| {
            let sk = item_s(&it, "sk");
            let mut parts = sk.splitn(3, '#').skip(1);
            let name = parts.next()?.to_string();
            let path = parts.next()?.to_string();
            Some(json!({
                "name": name,
                "path": path,
                "kind": item_s(&it, "kind"),
                "line": item_n(&it, "line"),
                "rank": it.get("rank").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()).unwrap_or(0.0),
            }))
        })
        .collect();
    let callers: Vec<String> = query_prefix(&state, &pk, &format!("R#{term}#"), 60)
        .await
        .into_iter()
        .filter_map(|it| {
            let sk = item_s(&it, "sk");
            sk.splitn(3, '#').nth(2).map(|s| s.to_string())
        })
        .collect();
    Ok(Json(json!({ "definitions": defs, "callers": callers })))
}

/// GET /api/graph/neighborhood?repo=owner/name&path=src/x.ts — one file's
/// import/importer edges + its top symbols with their caller files. Bounded;
/// feeds the visual explorer (click a node → fetch ITS neighborhood).
pub async fn neighborhood(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo(&q.repo)?;
    let path: String = q.path.trim().chars().take(300).collect();
    if path.is_empty() || path.contains("..") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let pk = format!("CG#{}#{}", claims.team_id, q.repo);
    let imports: Vec<String> = query_prefix(&state, &pk, &format!("I#{path}#"), 40)
        .await
        .into_iter()
        .filter_map(|it| {
            let sk = item_s(&it, "sk");
            sk.strip_prefix(&format!("I#{path}#"))
                .map(|s| s.to_string())
        })
        .collect();
    let importers: Vec<String> = query_prefix(&state, &pk, &format!("RI#{path}#"), 40)
        .await
        .into_iter()
        .filter_map(|it| {
            let sk = item_s(&it, "sk");
            sk.strip_prefix(&format!("RI#{path}#"))
                .map(|s| s.to_string())
        })
        .collect();
    // The file's own symbols (from its manifest) + who references them.
    let mut symbols = Vec::new();
    if let Some(man) = get_graph_item(&state, &pk, &format!("F#{path}")).await {
        if let Some(AttributeValue::Ss(sks)) = man.get("sks") {
            for sk in sks.iter().filter(|s| s.starts_with("NM#")).take(12) {
                if let Some(name) = sk.split('#').nth(1) {
                    let callers: Vec<String> = query_prefix(&state, &pk, &format!("R#{name}#"), 20)
                        .await
                        .into_iter()
                        .filter_map(|it| {
                            let s = item_s(&it, "sk");
                            s.splitn(3, '#').nth(2).map(|x| x.to_string())
                        })
                        .filter(|f| f != &path)
                        .collect();
                    symbols.push(json!({ "name": name, "callers": callers }));
                }
            }
        }
    }
    Ok(Json(json!({
        "path": path,
        "imports": imports,
        "importers": importers,
        "symbols": symbols,
    })))
}

/// The team's GitHub App installation (either table spelling).
async fn installation_id(state: &AppState, team_id: &str) -> Option<u64> {
    let from = |it: HashMap<String, AttributeValue>| {
        it.get("github_installation_id")
            .or_else(|| it.get("github_install_id"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|&n| n > 0)
    };
    for table in [&state.config.teams_table_name, &state.config.table_name] {
        let key = if table == &state.config.teams_table_name {
            "team_id"
        } else {
            "pk"
        };
        let found = state
            .dynamo
            .get_item()
            .table_name(table)
            .key(key, attr_s(team_id))
            .key("sk", attr_s("META"))
            .send()
            .await
            .ok()
            .and_then(|o| o.item().cloned())
            .and_then(from);
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Queue a FULL graph build for (team, owner/name): once (a build already
/// queued or running is not queued again), or always with `force` (an
/// admin's Rebuild). Shared by the /graph page and the reviewer setup.
/// Returns whether a build was queued; failure is logged, never fails the
/// caller's write.
pub(crate) async fn enqueue_full_index(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    force: bool,
) -> bool {
    let repo = format!("{owner}/{name}");
    let table = &state.config.settings_table_name;
    match common::code_graph::mark_queued(&state.dynamo, table, team_id, &repo, force).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(e) => {
            error!("Code graph: could not mark a build queued: {e}");
            return false;
        }
    }
    let Some(installation_id) = installation_id(state, team_id).await else {
        error!("Code graph: no GitHub installation found for team — index not enqueued");
        common::code_graph::set_status(
            &state.dynamo,
            table,
            team_id,
            &repo,
            common::code_graph::BuildState::Failed,
            "The GitHub App isn't installed for this team.",
        )
        .await;
        return false;
    };
    let msg = crate::models::WorkerMessage::GraphIndex(crate::models::GraphIndexMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        branch: String::new(), // worker resolves the default branch
        changed_files: None,   // full index
    });
    let sent = match serde_json::to_string(&msg) {
        Ok(body) => state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    match sent {
        Ok(()) => {
            info!(owner, name, "Code graph: full index enqueued");
            true
        }
        Err(e) => {
            error!("Code graph: index enqueue failed: {e}");
            common::code_graph::unmark_queued(&state.dynamo, table, team_id, &repo).await;
            false
        }
    }
}

/// PUT /api/graph/config/:owner/:name — {enabled: bool}. Admin. Turns the
/// repo's graph on or off (its own switch; the reviewer config is not
/// touched). Turning it on queues the first build when there is none.
pub async fn update_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo(&repo)?;
    let enabled = body["enabled"].as_bool().ok_or(StatusCode::BAD_REQUEST)?;
    common::code_graph::set_enabled(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &repo,
        enabled,
        &claims.email,
    )
    .await
    .map_err(|e| {
        error!("graph update_config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if enabled {
        enqueue_full_index(&state, &claims.team_id, &owner, &name, false).await;
    }
    Ok(StatusCode::OK)
}

/// POST /api/graph/rebuild/:owner/:name — admin. Builds the repo's graph again
/// from its default branch (also the retry after a failed build).
pub async fn rebuild(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo(&repo)?;
    if !common::code_graph::is_enabled(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &repo,
    )
    .await
    {
        // Turned off: turn it on first.
        return Err(StatusCode::CONFLICT);
    }
    if enqueue_full_index(&state, &claims.team_id, &owner, &name, true).await {
        Ok(StatusCode::ACCEPTED)
    } else {
        Err(StatusCode::BAD_GATEWAY)
    }
}
