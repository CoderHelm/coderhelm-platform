//! Dashboard API for the CODE GRAPH — its own feature (used by the reviewer
//! AND the PR-maker), not part of the reviewer. Read endpoints power the
//! /graph page (status, load-bearing files, symbol search, neighborhood
//! explorer); the PUT toggles indexing per repo and kicks the initial full
//! index on enable. Storage is the settings table:
//!   config → pk=team_id, sk=REVIEW_CONFIG#REPO#{owner}/{name} (graph_enabled)
//!   graph  → pk=CG#{team_id}#{owner}/{name}, sk=META | SUMMARY | NM#/R#/I#/RI#/F#

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

/// GET /api/graph/repos — every repo with a reviewer config, its graph flag,
/// and (when indexed) the graph's stats. Drives the /graph repo picker.
pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let configs = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":p", attr_s("REVIEW_CONFIG#REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("graph list_repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .items()
        .to_vec();

    let mut repos = Vec::new();
    for cfg in configs {
        let repo = item_s(&cfg, "sk").replace("REVIEW_CONFIG#REPO#", "");
        let enabled = cfg
            .get("graph_enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);
        let pk = format!("CG#{}#{repo}", claims.team_id);
        let meta = get_graph_item(&state, &pk, "META").await;
        repos.push(json!({
            "repo": repo,
            "graph_enabled": enabled,
            "indexed": meta.is_some(),
            "files": meta.as_ref().map(|m| item_n(m, "files")).unwrap_or(0),
            "symbols": meta.as_ref().map(|m| item_n(m, "symbols")).unwrap_or(0),
            "branch": meta.as_ref().map(|m| item_s(m, "branch")).unwrap_or_default(),
            "indexed_sha": meta.as_ref().map(|m| item_s(m, "indexed_sha")).unwrap_or_default(),
            "updated_at": meta.as_ref().map(|m| item_s(m, "updated_at")).unwrap_or_default(),
        }));
    }
    repos.sort_by(|a, b| a["repo"].as_str().cmp(&b["repo"].as_str()));
    Ok(Json(json!({ "repos": repos })))
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

/// Enqueue a FULL graph index for (team, owner/name). Shared by the /graph
/// toggle and the reviewer-config transition. Best-effort: failure is logged,
/// never fails the caller's write.
pub(crate) async fn enqueue_full_index(state: &AppState, team_id: &str, owner: &str, name: &str) {
    let installation_id = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|it| {
            it.get("github_installation_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
        });
    let Some(installation_id) = installation_id else {
        error!("Code graph: no GitHub installation found for team — index not enqueued");
        return;
    };
    let msg = crate::models::WorkerMessage::GraphIndex(crate::models::GraphIndexMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        branch: String::new(), // worker resolves the default branch
        changed_files: None,   // full index
    });
    if let Ok(body) = serde_json::to_string(&msg) {
        if let Err(e) = state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
        {
            error!("Code graph: initial index enqueue failed: {e}");
        } else {
            info!(owner, name, "Code graph: full index enqueued");
        }
    }
}

/// PUT /api/graph/config/:owner/:name — {enabled: bool}. Admin. A targeted
/// update (never clobbers the rest of the repo's config); enabling kicks the
/// initial full index.
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
    let sk = format!("REVIEW_CONFIG#REPO#{repo}");
    let prior = get_graph_item(&state, &claims.team_id, &sk)
        .await
        .and_then(|it| {
            it.get("graph_enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
        })
        .unwrap_or(false);
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET graph_enabled = :e")
        .expression_attribute_values(":e", AttributeValue::Bool(enabled))
        .send()
        .await
        .map_err(|e| {
            error!("graph update_config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if enabled && !prior {
        enqueue_full_index(&state, &claims.team_id, &owner, &name).await;
    }
    Ok(StatusCode::OK)
}
