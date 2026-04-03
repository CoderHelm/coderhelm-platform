use axum::extract::Query;
use axum::response::IntoResponse;
use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::auth::jwt;
use crate::models::{
    Claims, FeedbackMessage, InfraAnalyzeMessage, NotificationPrefs, OnboardMessage, OnboardRepo,
    TicketMessage, TicketSource, WorkerMessage,
};
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;

/// GET /api/me — return current user info.
pub async fn me(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch user: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    // Load team status from META record
    let team_status = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|i| i.get("status").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_else(|| "active".to_string());

    // Derive auth provider from user record
    let auth_provider = if item
        .get("github_login")
        .and_then(|v| v.as_s().ok())
        .is_some()
    {
        "github"
    } else if claims.sub.starts_with("USER#Google_") {
        "google"
    } else {
        "email"
    };

    Ok(Json(json!({
        "user_id": claims.sub,
        "team_id": claims.team_id,
        "github_login": claims.github_login,
        "email": claims.email,
        "avatar_url": item.get("avatar_url").and_then(|v| v.as_s().ok()),
        "role": item.get("role").and_then(|v| v.as_s().ok()).unwrap_or(&claims.role),
        "status": team_status,
        "auth_provider": auth_provider,
    })))
}

/// GET /api/teams — list all teams the current user has access to.
pub async fn list_teams(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let github_id = claims
        .sub
        .strip_prefix("USER#")
        .ok_or(StatusCode::BAD_REQUEST)?;

    // Try GSI1 (GitHub users) first, then fall back to GSI2 (email users)
    let gsi1_result = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi1")
        .key_condition_expression("gsi1pk = :gk")
        .expression_attribute_values(":gk", attr_s(&format!("GHUSER#{github_id}")))
        .send()
        .await
        .ok();

    let (team_ids, _gsi2_items) = if let Some(ref result) = gsi1_result {
        let ids: Vec<String> = result
            .items()
            .iter()
            .filter_map(|item| item.get("gsi1sk").and_then(|v| v.as_s().ok()).cloned())
            .collect();
        if !ids.is_empty() {
            (ids, vec![])
        } else {
            // Fall back to GSI2 (email)
            let all_items = state
                .dynamo
                .query()
                .table_name(&state.config.users_table_name)
                .index_name("gsi2")
                .key_condition_expression("gsi2pk = :pk")
                .expression_attribute_values(":pk", attr_s(&format!("EMAIL#{}", claims.email)))
                .send()
                .await
                .map(|r| r.items().to_vec())
                .unwrap_or_default();

            // Process any pending invites — convert them to real user records
            let real_user_sk = format!("USER#{github_id}");
            let now = chrono::Utc::now().to_rfc3339();
            for item in &all_items {
                let is_invite = item
                    .get("status")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s == "invited")
                    .unwrap_or(false);
                if !is_invite {
                    continue;
                }
                let invite_tid = match item.get("pk").and_then(|v| v.as_s().ok()) {
                    Some(t) => t.to_string(),
                    None => continue,
                };
                let invite_role = item
                    .get("role")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_else(|| "member".to_string());

                // Create real user record in the inviting team
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.users_table_name)
                    .item("pk", attr_s(&invite_tid))
                    .item("sk", attr_s(&real_user_sk))
                    .item("email", attr_s(&claims.email))
                    .item("role", attr_s(&invite_role))
                    .item("avatar_url", attr_s(""))
                    .item("updated_at", attr_s(&now))
                    .item("gsi2pk", attr_s(&format!("EMAIL#{}", claims.email)))
                    .item("gsi2sk", attr_s(&invite_tid))
                    .send()
                    .await;

                // Delete the invite placeholder
                if let Some(invite_sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
                    let _ = state
                        .dynamo
                        .delete_item()
                        .table_name(&state.config.users_table_name)
                        .key("pk", attr_s(&invite_tid))
                        .key("sk", attr_s(invite_sk))
                        .send()
                        .await;
                    info!(
                        email = %claims.email,
                        team_id = %invite_tid,
                        role = %invite_role,
                        "Processed pending invite on list_teams"
                    );
                }
            }

            let ids: Vec<String> = all_items
                .iter()
                .filter_map(|item| item.get("pk").and_then(|v| v.as_s().ok()).cloned())
                .collect();
            // Deduplicate (invite + real record may both point to same team)
            let mut unique_ids: Vec<String> = Vec::new();
            for id in ids {
                if !unique_ids.contains(&id) {
                    unique_ids.push(id);
                }
            }
            (unique_ids, all_items)
        }
    } else {
        (vec![], vec![])
    };

    // Load org name + status for each team
    let mut teams: Vec<Value> = Vec::new();
    for tid in &team_ids {
        let meta = state
            .dynamo
            .get_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(tid))
            .key("sk", attr_s("META"))
            .send()
            .await
            .ok()
            .and_then(|r| r.item);

        let org = meta
            .as_ref()
            .and_then(|item| item.get("github_org"))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| claims.email.clone());

        let status = meta
            .as_ref()
            .and_then(|item| item.get("status"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "active".to_string());

        teams.push(json!({
            "team_id": tid,
            "org": org,
            "status": status,
            "current": *tid == claims.team_id,
        }));
    }

    Ok(Json(json!({ "teams": teams })))
}

/// POST /api/teams/switch — switch to a different team.
pub async fn switch_team(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<axum::response::Response, StatusCode> {
    let target_team = body["team_id"].as_str().ok_or(StatusCode::BAD_REQUEST)?;

    // Verify user exists in target team and get their role there
    let target_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(target_team))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .item
        .ok_or(StatusCode::FORBIDDEN)?;

    let target_role = target_item
        .get("role")
        .and_then(|v| v.as_s().ok())
        .map_or("member", |v| v);

    // Re-issue JWT with new team_id and correct role
    let token = jwt::create_token(
        &claims.sub,
        target_team,
        &claims.email,
        target_role,
        claims.github_login.as_deref(),
        &state.secrets.jwt_secret,
        86400,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cookie = format!(
        "coderhelm_session={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=86400"
    );

    Ok((
        [(axum::http::header::SET_COOKIE, cookie)],
        Json(json!({ "team_id": target_team })),
    )
        .into_response())
}

/// PUT /api/teams/rename — rename the current team (owner only).
pub async fn rename_team(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let new_name = body["name"]
        .as_str()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && s.len() <= 100)
        .ok_or(StatusCode::BAD_REQUEST)?;

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .update_expression("SET github_org = :name")
        .expression_attribute_values(":name", attr_s(new_name))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to rename team: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team = %claims.team_id, new_name = %new_name, "Team renamed");
    Ok(Json(json!({ "status": "renamed", "name": new_name })))
}

/// GET /api/allowlist — list allowed emails (owner only).
pub async fn list_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s("ALLOWLIST"))
        .expression_attribute_values(":prefix", attr_s("EMAIL#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let emails: Vec<String> = result
        .items()
        .iter()
        .filter_map(|item| item.get("email").and_then(|v| v.as_s().ok()).cloned())
        .collect();

    Ok(Json(json!({ "emails": emails })))
}

/// POST /api/allowlist — add an email to the allowlist (owner only).
pub async fn add_to_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let email = body["email"]
        .as_str()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && (s.contains('@') || s.starts_with("*@")))
        .ok_or(StatusCode::BAD_REQUEST)?;

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s("ALLOWLIST"))
        .item("sk", attr_s(&format!("EMAIL#{email}")))
        .item("email", attr_s(&email))
        .item("added_by", attr_s(&claims.email))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to add to allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %email, added_by = %claims.email, "Added to allowlist");
    Ok(Json(json!({ "status": "added", "email": email })))
}

/// DELETE /api/allowlist — remove an email from the allowlist (owner only).
pub async fn remove_from_allowlist(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }

    let email = body["email"]
        .as_str()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s("ALLOWLIST"))
        .key("sk", attr_s(&format!("EMAIL#{email}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to remove from allowlist: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(email = %email, removed_by = %claims.email, "Removed from allowlist");
    Ok(Json(json!({ "status": "removed", "email": email })))
}

#[derive(serde::Deserialize)]
pub struct RunsQuery {
    source: Option<String>,
    limit: Option<i32>,
}

/// GET /api/runs — list runs for the team (from runs table).
pub async fn list_runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<RunsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let query_limit = params.limit.unwrap_or(50).min(100);
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .scan_index_forward(false) // newest first (ULID sorts lexicographically)
        .limit(query_limit)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let runs: Vec<Value> = result
        .items()
        .iter()
        .filter(|item| {
            // Exclude archived runs from the list
            item.get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str() != "archived")
                .unwrap_or(true)
        })
        .filter(|item| {
            if let Some(ref source) = params.source {
                item.get("ticket_source")
                    .and_then(|v| v.as_s().ok())
                    .map(|s| s == source)
                    .unwrap_or(false)
            } else {
                true
            }
        })
        .map(|item| {
            json!({
                "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                "status": item.get("status").and_then(|v| v.as_s().ok()),
                "ticket_source": item.get("ticket_source").and_then(|v| v.as_s().ok()),
                "ticket_id": item.get("ticket_id").and_then(|v| v.as_s().ok()),
                "title": item.get("title").and_then(|v| v.as_s().ok()),
                "repo": item.get("repo").and_then(|v| v.as_s().ok()),
                "pr_url": item.get("pr_url").and_then(|v| v.as_s().ok()),
                "cost_usd": item.get("cost_usd").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()),
                "tokens_in": item.get("tokens_in").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "tokens_out": item.get("tokens_out").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "duration_s": item.get("duration_s").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "current_pass": item.get("current_pass").and_then(|v| v.as_s().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({ "runs": runs })))
}

/// GET /api/runs/:run_id — get single run detail (from runs table).
pub async fn get_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(json!({
        "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
        "status": item.get("status").and_then(|v| v.as_s().ok()),
        "ticket_source": item.get("ticket_source").and_then(|v| v.as_s().ok()),
        "ticket_id": item.get("ticket_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "repo": item.get("repo").and_then(|v| v.as_s().ok()),
        "branch": item.get("branch").and_then(|v| v.as_s().ok()),
        "pr_url": item.get("pr_url").and_then(|v| v.as_s().ok()),
        "pr_number": item.get("pr_number").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "current_pass": item.get("current_pass").and_then(|v| v.as_s().ok()),
        "tokens_in": item.get("tokens_in").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "tokens_out": item.get("tokens_out").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "cost_usd": item.get("cost_usd").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<f64>().ok()),
        "files_modified": item.get("files_modified").and_then(|v| v.as_l().ok()).map(|list| list.iter().filter_map(|v| v.as_s().ok().map(|s| s.as_str())).collect::<Vec<_>>()),
        "mcp_servers": item.get("mcp_servers").and_then(|v| v.as_l().ok()).map(|list| list.iter().filter_map(|v| v.as_s().ok().map(|s| s.as_str())).collect::<Vec<_>>()),
        "duration_s": item.get("duration_s").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "error": item.get("error_message").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
        "pass_history": item.get("pass_history").and_then(|v| v.as_l().ok()).map(|list| {
            list.iter().filter_map(|entry| {
                let m = entry.as_m().ok()?;
                Some(json!({
                    "pass": m.get("pass").and_then(|v| v.as_s().ok()),
                    "started_at": m.get("started_at").and_then(|v| v.as_s().ok()),
                }))
            }).collect::<Vec<_>>()
        }),
    })))
}

/// GET /api/runs/:run_id/openspec — fetch the four openspec files from S3.
pub async fn get_run_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // First verify the run belongs to this team
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("ticket_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for openspec: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    let ticket_id = item
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?;

    let prefix = format!(
        "teams/{}/runs/{}/openspec",
        claims.team_id,
        ticket_id.to_lowercase()
    );

    let mut files = serde_json::Map::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        if let Ok(output) = state
            .s3
            .get_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .send()
            .await
        {
            if let Ok(bytes) = output.body.collect().await {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    files.insert(name.replace(".md", ""), Value::String(text));
                }
            }
        }
    }

    Ok(Json(Value::Object(files)))
}

/// POST /api/runs/:run_id/retry — re-enqueue a failed run to the ticket queue.
pub async fn retry_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Block if token limit exceeded
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // Fetch the failed run
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    if status != "failed" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Prefer stored installation_id; fall back to reading from team META
    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(&state, &claims.team_id).await?;
    }
    let issue_number = item
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let ticket_source = item
        .get("ticket_source")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("github");
    let source = match ticket_source {
        "jira" => TicketSource::Jira,
        _ => TicketSource::Github,
    };

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        source,
        ticket_id: item
            .get("ticket_id")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        title: item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        body: String::new(), // body is re-fetched from GitHub by the worker
        repo_owner: parts[0].to_string(),
        repo_name: parts[1].to_string(),
        issue_number,
        sender: claims
            .github_login
            .clone()
            .unwrap_or_else(|| claims.email.clone()),
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize retry message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue retry: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(run_id, "Run retried — dispatched to SQS");

    Ok(Json(json!({ "status": "retrying" })))
}

/// POST /api/runs/:run_id/re-review — re-enqueue feedback for a completed run.
pub async fn re_review_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Block if token limit exceeded
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch run for re-review: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    if status == "merged" {
        return Err(StatusCode::CONFLICT);
    }

    let pr_number = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if pr_number == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let repo = item
        .get("repo")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let mut installation_id = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    if installation_id == 0 {
        installation_id = get_team_installation_id(&state, &claims.team_id).await?;
    }

    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        run_id: run_id.clone(),
        repo_owner: parts[0].to_string(),
        repo_name: parts[1].to_string(),
        pr_number,
        review_id: 0,
        review_body: String::new(),
        comments: vec![],
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize re-review message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enqueue re-review: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Reset status back to completed (clear any failed state)
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression("SET #s = :s, status_run_id = :sri, updated_at = :t, current_pass = :p")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("running"))
        .expression_attribute_values(":sri", attr_s(&format!("running#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":p", attr_s("feedback"))
        .send()
        .await;

    info!(run_id, pr_number, "Re-review dispatched to feedback queue");
    Ok(Json(json!({ "status": "re-reviewing" })))
}

/// POST /api/runs/:run_id/cancel — cancel a running job.
pub async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&claims.team_id))
        .key("run_id", attr_s(&run_id))
        .update_expression("SET #s = :s, status_run_id = :sri, updated_at = :t")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("cancelled"))
        .expression_attribute_values(":sri", attr_s(&format!("cancelled#{run_id}")))
        .expression_attribute_values(":t", attr_s(&now))
        .condition_expression("#s = :running")
        .expression_attribute_values(":running", attr_s("running"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to cancel run {run_id}: {e}");
            StatusCode::BAD_REQUEST
        })?;

    info!(run_id, "Run cancelled by user");
    Ok(Json(json!({ "status": "cancelled" })))
}

/// GET /api/repos — list repos configured for this team.
pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repos: Vec<Value> = result
        .items()
        .iter()
        .filter_map(|item| {
            let name = item.get("repo_name").and_then(|v| v.as_s().ok())?;
            Some(json!({
                "name": name,
                "enabled": item.get("enabled").and_then(|v| v.as_bool().ok()),
                "ticket_source": item.get("ticket_source").and_then(|v| v.as_s().ok()),
                "onboard_status": item.get("onboard_status").and_then(|v| v.as_s().ok()),
                "onboard_error": item.get("onboard_error").and_then(|v| v.as_s().ok()),
            }))
        })
        .collect();

    Ok(Json(json!({ "repos": repos })))
}

/// POST /api/repos/sync — fetch repos from GitHub API and write missing ones to DynamoDB.
/// Used when the initial installation webhook didn't include repos (e.g. "All repositories" access).
pub async fn sync_repos(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Get installation_id from META record
    let meta = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch team meta: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let installation_id = meta
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or_else(|| {
            warn!("No installation_id found for team");
            StatusCode::NOT_FOUND
        })?;

    // Fetch repos from GitHub API
    let token = crate::auth::github_app::get_installation_token(&state, installation_id)
        .await
        .map_err(|e| {
            error!("Failed to get installation token: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let mut fetched: Vec<OnboardRepo> = Vec::new();
    let mut page = 1u32;
    loop {
        let url =
            format!("https://api.github.com/installation/repositories?per_page=100&page={page}");
        let resp = state
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await
            .map_err(|e| {
                error!("GitHub API request failed: {e}");
                StatusCode::BAD_GATEWAY
            })?
            .error_for_status()
            .map_err(|e| {
                error!("GitHub API error: {e}");
                StatusCode::BAD_GATEWAY
            })?;

        let body: Value = resp.json().await.map_err(|e| {
            error!("Failed to parse GitHub response: {e}");
            StatusCode::BAD_GATEWAY
        })?;

        if let Some(arr) = body["repositories"].as_array() {
            for r in arr {
                if let Some(full_name) = r["full_name"].as_str() {
                    let parts: Vec<&str> = full_name.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        fetched.push(OnboardRepo {
                            owner: parts[0].to_string(),
                            name: parts[1].to_string(),
                            default_branch: r["default_branch"]
                                .as_str()
                                .unwrap_or("main")
                                .to_string(),
                        });
                    }
                }
            }
            if arr.len() < 100 {
                break;
            }
        } else {
            break;
        }
        page += 1;
    }

    // Write any repos not already in DynamoDB
    let now = chrono::Utc::now().to_rfc3339();
    let mut added = 0u64;
    for repo in &fetched {
        let full = format!("{}/{}", repo.owner, repo.name);
        let sk = format!("REPO#{full}");

        // Only write if not already present
        let exists = state
            .dynamo
            .get_item()
            .table_name(&state.config.repos_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .projection_expression("pk")
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned())
            .is_some();

        if !exists {
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.repos_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s(&sk))
                .item("repo_name", attr_s(&full))
                .item("enabled", AttributeValue::Bool(false))
                .item("ticket_source", attr_s("github"))
                .item("created_at", attr_s(&now))
                .send()
                .await;
            added += 1;
        }
    }

    info!(total = fetched.len(), added, "Synced repos from GitHub API");

    Ok(Json(json!({ "total": fetched.len(), "added": added })))
}

/// GET /api/integrations/jira/check — quick Jira integration readiness check.
pub async fn get_jira_integration_check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let repos_result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("REPO#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query repos for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let repo_items = repos_result.items();
    let repo_count = repo_items.len();
    let enabled_repos: Vec<_> = repo_items
        .iter()
        .filter(|item| {
            item.get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true)
        })
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?;
            Some(sk.strip_prefix("REPO#")?.to_string())
        })
        .collect();
    let enabled_repo_count = enabled_repos.len();

    let runs_result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .scan_index_forward(false)
        .limit(25)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs for Jira check: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let jira_runs: Vec<_> = runs_result
        .items()
        .iter()
        .filter(|item| {
            item.get("ticket_source")
                .and_then(|v| v.as_s().ok())
                .map(|v| v == "jira")
                .unwrap_or(false)
        })
        .collect();
    let jira_seen = !jira_runs.is_empty();
    let last_jira_event_at = jira_runs
        .first()
        .and_then(|item| item.get("created_at"))
        .and_then(|v| v.as_s().ok())
        .cloned();

    // Count actual Jira webhook events from the events table
    let jira_event_count = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .select(aws_sdk_dynamodb::types::Select::Count)
        .send()
        .await
        .map(|r| r.count())
        .unwrap_or(0);

    // Load per-team JIRA secret from DynamoDB
    let team_secret = load_jira_secret(&state, &claims.team_id).await;
    let secret_configured = team_secret.is_some() || state.secrets.jira_webhook_secret.is_some();
    let ready = secret_configured && enabled_repo_count > 0;

    // Load installation_id from team META
    let meta_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);

    let installation_id = meta_result
        .as_ref()
        .and_then(|item| item.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let webhook_url = team_secret
        .as_ref()
        .map(|(token, _)| format!("https://api.coderhelm.com/webhooks/jira/{token}"));

    Ok(Json(json!({
        "ready": ready,
        "secret_configured": secret_configured,
        "configured_repos": {
            "total": repo_count,
            "enabled": enabled_repo_count
        },
        "enabled_repos": enabled_repos,
        "jira_events_seen": jira_seen,
        "last_jira_event_at": last_jira_event_at,
        "jira_event_count": jira_event_count,
        "installation_id": installation_id,
        "team_id": claims.team_id,
        "webhook_url": webhook_url,
    })))
}

/// POST /api/integrations/jira/secret — generate a Jira webhook token + signing secret.
/// Creates an opaque URL token (stored in jira-tokens table) and a separate HMAC
/// signing secret the user pastes into Jira's webhook config for signature verification.
pub async fn generate_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    use rand::Rng;

    let token: String = rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    let webhook_secret: String = rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();

    let now = chrono::Utc::now().to_rfc3339();

    // Delete old token from jira-tokens table if one exists
    let old = load_jira_secret(&state, &claims.team_id).await;
    if let Some(old_token) = old.as_ref().map(|(t, _)| t) {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(old_token))
            .send()
            .await;
    }

    // Load installation_id
    let install_id = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|item| {
            item.get("github_install_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
        })
        .unwrap_or(0);

    // Store token + webhook_secret on team record (main table)
    state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_config_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("JIRA_SECRET"))
        .item("secret", attr_s(&token))
        .item("webhook_secret", attr_s(&webhook_secret))
        .item("created_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to store JIRA token: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Store token → team mapping in jira-tokens table (with conditional write)
    let put_result = state
        .dynamo
        .put_item()
        .table_name(&state.config.jira_tokens_table_name)
        .item("token", attr_s(&token))
        .item("team_id", attr_s(&claims.team_id))
        .item("installation_id", AttributeValue::N(install_id.to_string()))
        .item("webhook_secret", attr_s(&webhook_secret))
        .item("created_at", attr_s(&now))
        .condition_expression("attribute_not_exists(#t)")
        .expression_attribute_names("#t", "token")
        .send()
        .await;

    if let Err(e) = put_result {
        if e.as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false)
        {
            error!("JIRA token collision detected — client should retry");
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s("JIRA_SECRET"))
                .send()
                .await;
            return Err(StatusCode::CONFLICT);
        }
        error!("Failed to store JIRA token lookup: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    info!(team_id = %claims.team_id, "Generated Jira webhook token + signing secret");
    Ok(Json(
        json!({ "token": token, "webhook_secret": webhook_secret }),
    ))
}

/// DELETE /api/integrations/jira/secret — remove the Jira webhook token.
pub async fn delete_jira_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    // Delete token from jira-tokens table first
    let old = load_jira_secret(&state, &claims.team_id).await;
    if let Some((old_token, _)) = old {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA_SECRET"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete JIRA token: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team_id = %claims.team_id, "Deleted Jira webhook token");
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/integrations/jira — remove the entire Jira integration.
pub async fn delete_jira_integration(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    let tid = &claims.team_id;

    // 1. Delete the webhook token from the jira-tokens lookup table
    if let Some((old_token, _)) = load_jira_secret(&state, tid).await {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    // 2. Delete all items in the jira-config table for this team (config, secret, projects)
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_config_table_name)
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", attr_s(tid))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query jira config for delete: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in result.items() {
        if let (Some(pk), Some(sk)) = (item.get("pk"), item.get("sk")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_config_table_name)
                .key("pk", pk.clone())
                .key("sk", sk.clone())
                .send()
                .await;
        }
    }

    // 3. Delete jira events for this team
    if let Ok(events) = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .send()
        .await
    {
        for item in events.items() {
            if let (Some(tid_attr), Some(eid)) = (item.get("team_id"), item.get("event_id")) {
                let _ = state
                    .dynamo
                    .delete_item()
                    .table_name(&state.config.jira_events_table_name)
                    .key("team_id", tid_attr.clone())
                    .key("event_id", eid.clone())
                    .send()
                    .await;
            }
        }
    }

    info!(team_id = %tid, "Deleted entire Jira integration");
    Ok(StatusCode::NO_CONTENT)
}

/// Load per-team JIRA token and webhook secret from DynamoDB.
/// Returns (token, webhook_secret) if configured.
pub async fn load_jira_secret(state: &AppState, team_id: &str) -> Option<(String, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("JIRA_SECRET".to_string()))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)?;
    let token = item.get("secret").and_then(|v| v.as_s().ok()).cloned()?;
    let webhook_secret = item
        .get("webhook_secret")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    Some((token, webhook_secret))
}

/// GET /api/integrations/jira/events — list recent Jira webhook events for the Events tab.
pub async fn get_jira_events(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let limit: i32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .min(100);

    let events =
        crate::routes::jira_webhook::list_jira_events(&state, &claims.team_id, limit).await?;

    Ok(Json(json!({ "events": events, "total": events.len() })))
}

/// POST /integrations/jira/forge-register — called by the Forge admin page to register trigger URLs.
/// Public endpoint, verified by matching installation_id against the team's META record.
pub async fn forge_register_urls(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let team_id = body
        .get("team_id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let installation_id = body
        .get("installation_id")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let list_projects_url = body
        .get("list_projects_url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let create_ticket_url = body
        .get("create_ticket_url")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let add_comment_url = body
        .get("add_comment_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let site_url = body.get("site_url").and_then(|v| v.as_str()).unwrap_or("");

    // Verify team exists and installation_id matches
    let meta = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .map_err(|e| {
            error!("forge-register: failed to query team META: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item
        .ok_or_else(|| {
            warn!("forge-register: team not found: {team_id}");
            StatusCode::NOT_FOUND
        })?;

    let stored_id = meta
        .get("github_install_id")
        .and_then(|v| v.as_n().ok())
        .map_or("", |v| v.as_str());
    if stored_id != installation_id {
        warn!("forge-register: installation_id mismatch for {team_id}");
        return Err(StatusCode::FORBIDDEN);
    }

    // Save trigger URLs to JIRA#config
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("JIRA#config"))
        .update_expression(
            "SET list_projects_url = :lpu, create_ticket_url = :ctu, add_comment_url = :acu, site_url = :su, updated_at = :ua",
        )
        .expression_attribute_values(":lpu", attr_s(list_projects_url))
        .expression_attribute_values(":ctu", attr_s(create_ticket_url))
        .expression_attribute_values(":acu", attr_s(add_comment_url))
        .expression_attribute_values(":su", attr_s(site_url))
        .expression_attribute_values(":ua", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("forge-register: failed to save URLs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team_id, "Forge trigger URLs registered");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/integrations/jira/config — get Jira config (trigger URLs, default project, label, projects).
pub async fn get_jira_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Load JIRA#config item
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to load Jira config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item;

    let get_str = |item: &std::collections::HashMap<String, AttributeValue>, key: &str| {
        item.get(key)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };

    let (
        default_project,
        trigger_label,
        list_projects_url,
        create_ticket_url,
        add_comment_url,
        site_url,
    ) = if let Some(ref item) = config_item {
        (
            get_str(item, "default_project"),
            get_str(item, "trigger_label"),
            get_str(item, "list_projects_url"),
            get_str(item, "create_ticket_url"),
            get_str(item, "add_comment_url"),
            get_str(item, "site_url"),
        )
    } else {
        (
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        )
    };

    // Load enabled projects (JIRA#PROJECT#<key> items)
    let projects_result = state
        .dynamo
        .query()
        .table_name(&state.config.jira_config_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("JIRA#PROJECT#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query Jira projects: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let projects: Vec<Value> = projects_result
        .items()
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.as_s().ok()?;
            let key = sk.strip_prefix("JIRA#PROJECT#")?;
            let name = item
                .get("project_name")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or(key);
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            Some(json!({ "key": key, "name": name, "enabled": enabled }))
        })
        .collect();

    Ok(Json(json!({
        "default_project": default_project,
        "trigger_label": trigger_label,
        "list_projects_url": list_projects_url,
        "create_ticket_url": create_ticket_url,
        "add_comment_url": add_comment_url,
        "site_url": site_url,
        "projects": projects,
    })))
}

/// PUT /api/integrations/jira/config — update Jira config.
pub async fn update_jira_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let now = chrono::Utc::now().to_rfc3339();

    let mut update_expr = vec!["updated_at = :ua".to_string()];
    let mut attr_vals: Vec<(String, AttributeValue)> = vec![(":ua".to_string(), attr_s(&now))];

    for (field, key) in [
        ("default_project", "default_project"),
        ("trigger_label", "trigger_label"),
        ("list_projects_url", "list_projects_url"),
        ("create_ticket_url", "create_ticket_url"),
        ("add_comment_url", "add_comment_url"),
        ("site_url", "site_url"),
    ] {
        if let Some(val) = body.get(field).and_then(|v| v.as_str()) {
            update_expr.push(format!("{key} = :{key}"));
            attr_vals.push((format!(":{key}"), attr_s(val)));
        }
    }

    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .update_expression(format!("SET {}", update_expr.join(", ")));

    for (k, v) in attr_vals {
        update = update.expression_attribute_values(k, v);
    }

    update.send().await.map_err(|e| {
        error!("Failed to update Jira config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    info!(team_id = %claims.team_id, "Updated Jira config");
    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/integrations/jira/projects — save enabled/disabled project list.
pub async fn update_jira_projects(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let projects = body
        .get("projects")
        .and_then(|v| v.as_array())
        .ok_or(StatusCode::BAD_REQUEST)?;

    for proj in projects {
        let key = proj
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or(StatusCode::BAD_REQUEST)?;
        let name = proj.get("name").and_then(|v| v.as_str()).unwrap_or(key);
        let enabled = proj
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        state
            .dynamo
            .put_item()
            .table_name(&state.config.jira_config_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s(&format!("JIRA#PROJECT#{key}")))
            .item("project_name", attr_s(name))
            .item("enabled", AttributeValue::Bool(enabled))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to save Jira project {key}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    info!(team_id = %claims.team_id, count = projects.len(), "Updated Jira projects");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/integrations/jira/projects/fetch — proxy call to Forge web trigger to list projects.
pub async fn fetch_jira_projects(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Load the list-projects URL from Jira config
    let config_item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to load Jira config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item
        .ok_or(StatusCode::NOT_FOUND)?;

    let url = config_item
        .get("list_projects_url")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            warn!(team_id = %claims.team_id, "No list_projects_url configured");
            StatusCode::NOT_FOUND
        })?;

    let resp = state
        .http
        .post(url)
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to call Forge list-projects: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    if !resp.status().is_success() {
        error!(
            status = resp.status().as_u16(),
            "Forge list-projects returned error"
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    let body: Value = resp.json().await.map_err(|e| {
        error!("Failed to parse Forge list-projects response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(body))
}

/// POST /api/integrations/jira/check — validate a candidate Jira payload before wiring automation.
pub async fn validate_jira_integration_payload(
    Extension(_claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let top = |key: &str| body.get(key).and_then(|v| v.as_str()).map(str::to_string);
    let nested = |path1: &str, path2: &str| {
        body.get(path1)
            .and_then(|v| v.get(path2))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };

    let repo_owner = top("repo_owner").or_else(|| nested("coderhelm", "repo_owner"));
    let repo_name = top("repo_name").or_else(|| nested("coderhelm", "repo_name"));

    let installation_id = body
        .get("installation_id")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            body.get("coderhelm")
                .and_then(|v| v.get("installation_id"))
                .and_then(|v| v.as_u64())
        });
    let team_id = top("team_id").or_else(|| nested("coderhelm", "team_id"));

    let issue_key = body
        .get("issue")
        .and_then(|v| v.get("key"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let issue_summary = body
        .get("issue")
        .and_then(|v| v.get("fields"))
        .and_then(|v| v.get("summary"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("issue")
                .and_then(|v| v.get("title"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });

    let mut missing = Vec::new();
    if repo_owner.is_none() {
        missing.push("repo_owner (or coderhelm.repo_owner)");
    }
    if repo_name.is_none() {
        missing.push("repo_name (or coderhelm.repo_name)");
    }
    if installation_id.is_none() && team_id.is_none() {
        missing.push("installation_id (or team_id)");
    }
    if issue_key.is_none() {
        missing.push("issue.key (or issue.id)");
    }
    if issue_summary.is_none() {
        missing.push("issue.fields.summary (or issue.title)");
    }

    let valid = missing.is_empty();

    Ok(Json(json!({
        "valid": valid,
        "missing": missing,
        "normalized_preview": {
            "repo_owner": repo_owner,
            "repo_name": repo_name,
            "installation_id": installation_id,
            "team_id": team_id,
            "ticket_id": issue_key,
            "title": issue_summary,
            "event": body.get("webhookEvent").and_then(|v| v.as_str()).unwrap_or("unknown")
        },
        "next_step": if valid {
            "Payload shape looks good. Point Jira automation/webhook to /webhooks/jira."
        } else {
            "Fix missing fields above and run this check again."
        }
    })))
}

/// POST /api/repos/:owner/:name — update repo config.
pub async fn update_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let enabled = body["enabled"].as_bool().unwrap_or(true);

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.repos_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&format!("REPO#{repo}")))
        .item("repo_name", attr_s(&repo))
        .item(
            "enabled",
            aws_sdk_dynamodb::types::AttributeValue::Bool(enabled),
        )
        .item("ticket_source", attr_s("github"));

    if enabled {
        put = put.item("onboard_status", attr_s("pending"));
    }

    put.send().await.map_err(|e| {
        error!("Failed to update repo: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // When enabling a repo, trigger onboarding to generate voice, instructions, etc.
    if enabled {
        let installation_id = get_team_installation_id(&state, &claims.team_id).await?;

        let message = WorkerMessage::Onboard(OnboardMessage {
            team_id: claims.team_id.clone(),
            installation_id,
            repos: vec![OnboardRepo {
                owner: owner.clone(),
                name: name.clone(),
                default_branch: "main".to_string(),
            }],
        });

        if let Ok(body) = serde_json::to_string(&message) {
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched onboarding for newly enabled repo");
        }

        // Trigger per-repo infrastructure analysis
        let per_repo_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: Some(repo.clone()),
        });
        if let Ok(body) = serde_json::to_string(&per_repo_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let sk = format!("INFRA#REPO#{repo}");
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s(&sk))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched per-repo infra analysis");
        }

        // Also trigger team-wide infrastructure analysis
        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched team-wide infra analysis");
        }
    } else {
        // When disabling a repo, remove its per-repo infra and refresh team-wide
        let sk = format!("INFRA#REPO#{repo}");
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.infra_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .send()
            .await;

        let infra_msg = WorkerMessage::InfraAnalyze(InfraAnalyzeMessage {
            team_id: claims.team_id.clone(),
            triggered_by: claims.display_name(),
            repo: None,
        });
        if let Ok(body) = serde_json::to_string(&infra_msg) {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .put_item()
                .table_name(&state.config.infra_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s("INFRA#analysis"))
                .item("status", attr_s("pending"))
                .item("has_infra", AttributeValue::Bool(false))
                .item("updated_at", attr_s(&now))
                .send()
                .await;
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(&body)
                .send()
                .await;
            info!(repo = %repo, "Dispatched infra refresh after repo disable");
        }
    }

    Ok(StatusCode::OK)
}

/// DELETE /api/repos/:owner/:name — remove a repo from this team.
pub async fn delete_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.repos_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("REPO#{repo}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete repo: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/stats — pre-computed analytics (O(1) read from analytics table).
pub async fn get_stats(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Read current month + all-time aggregates in parallel
    let now = chrono::Utc::now();
    let month_period = now.format("%Y-%m").to_string();

    let (month_result, alltime_result) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s(&month_period))
            .send(),
        state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s("ALL_TIME"))
            .send()
    );

    let month = month_result.map_err(|e| {
        error!("Failed to read monthly stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let alltime = alltime_result.map_err(|e| {
        error!("Failed to read all-time stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let extract = |item: Option<
        &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    >| {
        let total: u64 = item
            .and_then(|i| i.get("total_runs"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let completed: u64 = item
            .and_then(|i| i.get("completed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let failed: u64 = item
            .and_then(|i| i.get("failed"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let cost: f64 = item
            .and_then(|i| i.get("total_cost_usd"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0.0);
        let tokens_in: u64 = item
            .and_then(|i| i.get("total_tokens_in"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        let tokens_out: u64 = item
            .and_then(|i| i.get("total_tokens_out"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        json!({
            "total_runs": total,
            "completed": completed,
            "failed": failed,
            "in_progress": total.saturating_sub(completed + failed),
            "total_cost_usd": cost,
            "total_tokens_in": tokens_in,
            "total_tokens_out": tokens_out,
            "merge_rate": if total > 0 { completed as f64 / total as f64 } else { 0.0 },
        })
    };

    Ok(Json(json!({
        "month": extract(month.item()),
        "all_time": extract(alltime.item()),
    })))
}

/// GET /api/stats/history — last 6 months of analytics for charts.
pub async fn get_stats_history(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now();
    // Build period keys for the last 6 months
    let periods: Vec<String> = (0..6)
        .map(|i| {
            let d = now - chrono::Duration::days(i * 30);
            d.format("%Y-%m").to_string()
        })
        .collect();

    // Batch get all 6 months
    let mut months = Vec::new();
    for period in &periods {
        let result = state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&claims.team_id))
            .key("period", attr_s(period))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to read stats for {period}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let item = result.item();
        let get_n = |key: &str| -> u64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0)
        };
        let get_f = |key: &str| -> f64 {
            item.and_then(|i| i.get(key))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0.0)
        };
        months.push(json!({
            "period": period,
            "total_runs": get_n("total_runs"),
            "completed": get_n("completed"),
            "failed": get_n("failed"),
            "total_cost_usd": get_f("total_cost_usd"),
            "total_tokens_in": get_n("total_tokens_in"),
            "total_tokens_out": get_n("total_tokens_out"),
        }));
    }

    // Reverse so oldest is first (for chart x-axis)
    months.reverse();

    Ok(Json(json!({ "months": months })))
}

// ─── Notification preferences ───────────────────────────────────────

/// GET /api/notifications — get notification preferences for calling user.
pub async fn get_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prefs = match result.item() {
        Some(item) => NotificationPrefs {
            email_run_complete: item
                .get("email_run_complete")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            email_run_failed: item
                .get("email_run_failed")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            email_weekly_summary: item
                .get("email_weekly_summary")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
        },
        None => NotificationPrefs::default(),
    };

    Ok(Json(json!({
        "email_run_complete": prefs.email_run_complete,
        "email_run_failed": prefs.email_run_failed,
        "email_weekly_summary": prefs.email_weekly_summary,
    })))
}

/// PUT /api/notifications — update notification preferences.
pub async fn update_notification_prefs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let sk = format!("NOTIFICATIONS#{}", claims.sub);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item(
            "email_run_complete",
            attr_bool(body["email_run_complete"].as_bool().unwrap_or(true)),
        )
        .item(
            "email_run_failed",
            attr_bool(body["email_run_failed"].as_bool().unwrap_or(true)),
        )
        .item(
            "email_weekly_summary",
            attr_bool(body["email_weekly_summary"].as_bool().unwrap_or(true)),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update notification prefs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Instructions ───────────────────────────────────────────────────

/// Look up the GitHub installation ID from the team's META record in the main table.
async fn get_team_installation_id(
    state: &AppState,
    team_id: &str,
) -> Result<u64, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("META"))
        .projection_expression("github_install_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch team META for installation_id: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    result
        .item()
        .and_then(|i| i.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&id| id > 0)
        .ok_or_else(|| {
            warn!(team_id, "No github_install_id found for team");
            StatusCode::BAD_REQUEST
        })
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_bool(val: bool) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::Bool(val)
}

const MAX_INSTRUCTIONS_BYTES: usize = 10_240; // 10KB limit

/// GET /api/instructions/global — get global custom instructions.
pub async fn get_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "INSTRUCTIONS#GLOBAL").await
}

/// PUT /api/instructions/global — update global custom instructions.
pub async fn update_global_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "INSTRUCTIONS#GLOBAL", content).await
}

/// GET /api/instructions/repo/:owner/:name — get per-repo custom instructions.
pub async fn get_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/instructions/repo/:owner/:name — update per-repo custom instructions.
pub async fn update_repo_instructions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("INSTRUCTIONS#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

/// Validate repo path parameter to prevent DynamoDB key injection.
fn validate_repo_name(repo: &str) -> Result<(), StatusCode> {
    if repo.is_empty()
        || repo.len() > 200
        || repo.contains("..")
        || repo.contains('\0')
        || !repo
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

async fn get_instructions_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let content = result
        .item()
        .and_then(|item| item.get("content"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();

    Ok(Json(json!({ "content": content })))
}

async fn update_instructions_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
    content: &str,
) -> Result<StatusCode, StatusCode> {
    if content.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item("content", attr_s(content))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update instructions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Must-rules ─────────────────────────────────────────────────────

const MAX_RULES_BYTES: usize = 10_240; // 10KB limit

/// GET /api/rules/global — get global must-rules.
pub async fn get_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_rules_inner(&state, &claims.team_id, "RULES#GLOBAL").await
}

/// PUT /api/rules/global — update global must-rules.
pub async fn update_global_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    update_rules_inner(&state, &claims.team_id, "RULES#GLOBAL", rules).await
}

/// GET /api/rules/repo/:owner/:name — get per-repo must-rules.
pub async fn get_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("RULES#REPO#{repo}");
    get_rules_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/rules/repo/:owner/:name — update per-repo must-rules.
pub async fn update_repo_rules(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let rules = body["rules"].as_array().ok_or(StatusCode::BAD_REQUEST)?;
    let sk = format!("RULES#REPO#{repo}");
    update_rules_inner(&state, &claims.team_id, &sk, rules).await
}

async fn get_rules_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rules: Vec<String> = result
        .item()
        .and_then(|item| item.get("rules"))
        .and_then(|v| v.as_l().ok())
        .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();

    Ok(Json(json!({ "rules": rules })))
}

async fn update_rules_inner(
    state: &AppState,
    team_id: &str,
    sk: &str,
    rules: &[Value],
) -> Result<StatusCode, StatusCode> {
    let rule_strings: Vec<String> = rules
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let serialized = serde_json::to_string(&rule_strings).unwrap_or_default();
    if serialized.len() > MAX_RULES_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let rule_attrs: Vec<aws_sdk_dynamodb::types::AttributeValue> = rule_strings
        .iter()
        .map(|s| aws_sdk_dynamodb::types::AttributeValue::S(s.clone()))
        .collect();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item(
            "rules",
            aws_sdk_dynamodb::types::AttributeValue::L(rule_attrs),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update rules: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

// ─── Voice settings ─────────────────────────────────────────────────

/// GET /api/voice/global — get global voice settings.
pub async fn get_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "VOICE#GLOBAL").await
}

/// PUT /api/voice/global — update global voice settings.
pub async fn update_global_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "VOICE#GLOBAL", content).await
}

/// GET /api/voice/repo/:owner/:name — get voice/tone settings for a repo.
pub async fn get_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("VOICE#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/voice/repo/:owner/:name — update voice/tone settings for a repo.
pub async fn update_repo_voice(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("VOICE#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

// ─── Agents context ─────────────────────────────────────────────────

/// GET /api/agents/global — get global agents context.
pub async fn get_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    get_instructions_inner(&state, &claims.team_id, "AGENTS#GLOBAL").await
}

/// PUT /api/agents/global — update global agents context.
pub async fn update_global_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let content = body["content"].as_str().unwrap_or("");
    update_instructions_inner(&state, &claims.team_id, "AGENTS#GLOBAL", content).await
}

/// GET /api/agents/repo/:owner/:name — get agents context for a repo.
pub async fn get_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let sk = format!("AGENTS#REPO#{repo}");
    get_instructions_inner(&state, &claims.team_id, &sk).await
}

/// PUT /api/agents/repo/:owner/:name — update agents context for a repo.
pub async fn update_repo_agents(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;
    let content = body["content"].as_str().unwrap_or("");
    let sk = format!("AGENTS#REPO#{repo}");
    update_instructions_inner(&state, &claims.team_id, &sk, content).await
}

/// POST /api/repos/:owner/:name/regenerate — re-run onboard (voice + agents) for a repo.
pub async fn regenerate_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    super::billing::require_active_subscription(&state, &claims.team_id).await?;
    let repo = format!("{owner}/{name}");
    validate_repo_name(&repo)?;

    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let installation_id = get_team_installation_id(&state, &claims.team_id).await?;

    let message = WorkerMessage::Onboard(OnboardMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        repos: vec![OnboardRepo {
            owner: parts[0].to_string(),
            name: parts[1].to_string(),
            default_branch: "main".to_string(),
        }],
    });

    let body = serde_json::to_string(&message).map_err(|e| {
        error!("Failed to serialize onboard message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send regenerate message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(repo = %repo, "Dispatched regeneration to SQS");
    Ok(Json(json!({ "status": "regenerating" })))
}

/// GET /api/settings/budget — get monthly budget cap.
pub async fn get_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#BUDGET"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch budget: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let max_budget_cents = result
        .item()
        .and_then(|i| i.get("max_budget_cents"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0); // 0 = no cap

    Ok(Json(json!({ "max_budget_cents": max_budget_cents })))
}

/// PUT /api/settings/budget — set monthly budget cap.
/// Only allowed for users with an active subscription (overage doesn't apply to free tier).
pub async fn update_budget(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    // Block free-tier users from setting a budget cap
    let billing = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let subscription_status = billing
        .as_ref()
        .and_then(|i| i.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map_or("none", |v| v);

    if subscription_status != "active" {
        return Err(StatusCode::FORBIDDEN);
    }

    let max_budget_cents = body["max_budget_cents"].as_u64().unwrap_or(0);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("SETTINGS#BUDGET"))
        .item(
            "max_budget_cents",
            aws_sdk_dynamodb::types::AttributeValue::N(max_budget_cents.to_string()),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update budget: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/settings/workflow — get workflow preferences.
pub async fn get_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#WORKFLOW"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let commit_openspec = result
        .item()
        .and_then(|i| i.get("commit_openspec"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false); // default: off

    let default_destination = result
        .item()
        .and_then(|i| i.get("default_destination"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "github".to_string());

    let allow_plan_log_analyzer = result
        .item()
        .and_then(|i| i.get("allow_plan_log_analyzer"))
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false); // default: off

    Ok(Json(json!({
        "commit_openspec": commit_openspec,
        "default_destination": default_destination,
        "allow_plan_log_analyzer": allow_plan_log_analyzer,
    })))
}

/// PUT /api/settings/workflow — update workflow preferences.
pub async fn update_workflow_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    let commit_openspec = body["commit_openspec"].as_bool().unwrap_or(false);
    let default_destination = body["default_destination"]
        .as_str()
        .filter(|d| *d == "github" || *d == "jira")
        .unwrap_or("github");
    let allow_plan_log_analyzer = body["allow_plan_log_analyzer"].as_bool().unwrap_or(false);

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s("SETTINGS#WORKFLOW"))
        .item(
            "commit_openspec",
            aws_sdk_dynamodb::types::AttributeValue::Bool(commit_openspec),
        )
        .item("default_destination", attr_s(default_destination))
        .item(
            "allow_plan_log_analyzer",
            aws_sdk_dynamodb::types::AttributeValue::Bool(allow_plan_log_analyzer),
        )
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update workflow settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// GET /api/health — system health check (stale runs, DLQ depth, queue depths).
pub async fn health(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now();
    let mut checks: Vec<Value> = Vec::new();
    let mut status = "healthy";

    // 1. Crashed runs — "running" for over 10 minutes (worker likely died)
    let crash_cutoff = (now - chrono::Duration::minutes(10)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("#s = :running AND created_at < :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .expression_attribute_values(":running", attr_s("running"))
        .expression_attribute_values(":cutoff", attr_s(&crash_cutoff))
        .limit(20)
        .send()
        .await
    {
        let crashed: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = crashed.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "crashed_runs",
            "status": if count == 0 { "ok" } else { "critical" },
            "count": count,
            "items": crashed,
        }));
    }

    // 2. Stale queued — sitting in queue for over 30 minutes
    let stale_cutoff = (now - chrono::Duration::minutes(30)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("#s = :queued AND created_at < :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .expression_attribute_values(":queued", attr_s("queued"))
        .expression_attribute_values(":cutoff", attr_s(&stale_cutoff))
        .limit(20)
        .send()
        .await
    {
        let stale_runs: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "status": item.get("status").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = stale_runs.len();
        if count > 0 {
            status = "degraded";
        }
        checks.push(json!({
            "name": "stale_queued",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": stale_runs,
        }));
    }

    // 3. DLQ depth
    if !state.config.dlq_url.is_empty() {
        if let Ok(attrs) = state
            .sqs
            .get_queue_attributes()
            .queue_url(&state.config.dlq_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
        {
            let depth: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            if depth > 0 {
                status = "degraded";
            }
            checks.push(json!({
                "name": "dlq",
                "status": if depth == 0 { "ok" } else { "critical" },
                "depth": depth,
            }));
        }
    }

    // 4. Queue depths (tickets, ci-fix, feedback)
    for (name, url) in [
        ("tickets", &state.config.ticket_queue_url),
        ("ci_fix", &state.config.ci_fix_queue_url),
        ("feedback", &state.config.feedback_queue_url),
    ] {
        if let Ok(attrs) = state
            .sqs
            .get_queue_attributes()
            .queue_url(url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(
                aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .send()
            .await
        {
            let visible: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let in_flight: u64 = attrs
                .attributes()
                .and_then(|a| {
                    a.get(
                        &aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
                    )
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            checks.push(json!({
                "name": name,
                "status": "ok",
                "visible": visible,
                "in_flight": in_flight,
            }));
        }
    }

    // 5. Recent failed runs (last 24h)
    let error_cutoff = (now - chrono::Duration::hours(24)).to_rfc3339();
    if let Ok(result) = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("#s = :failed AND created_at > :cutoff")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(&claims.team_id))
        .expression_attribute_values(":failed", attr_s("failed"))
        .expression_attribute_values(":cutoff", attr_s(&error_cutoff))
        .limit(50)
        .send()
        .await
    {
        let failed_runs: Vec<Value> = result
            .items()
            .iter()
            .map(|item| {
                json!({
                    "run_id": item.get("run_id").and_then(|v| v.as_s().ok()),
                    "title": item.get("title").and_then(|v| v.as_s().ok()),
                    "error": item.get("error").and_then(|v| v.as_s().ok()),
                    "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                })
            })
            .collect();

        let count = failed_runs.len();
        checks.push(json!({
            "name": "failed_runs_24h",
            "status": if count == 0 { "ok" } else { "warning" },
            "count": count,
            "items": failed_runs,
        }));
    }

    Ok(Json(json!({
        "status": status,
        "checked_at": now.to_rfc3339(),
        "checks": checks,
    })))
}

/// POST /api/account/reset — wipe all team data across all tables.
/// Keeps the TEAM META record so the account identity remains.
pub async fn reset_account(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, StatusCode> {
    if claims.role != "owner" {
        return Err(StatusCode::FORBIDDEN);
    }
    info!(team_id = %claims.team_id, user = %claims.email, "Account reset requested");

    let tid = &claims.team_id;

    // Helper: delete all items with pk=team_id from a table
    async fn wipe_table(
        dynamo: &aws_sdk_dynamodb::Client,
        table: &str,
        tid: &str,
    ) -> Result<(), StatusCode> {
        let result = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(
                ":pk",
                aws_sdk_dynamodb::types::AttributeValue::S(tid.to_string()),
            )
            .projection_expression("pk, sk")
            .send()
            .await
            .map_err(|e| {
                error!("Failed to query {table} for reset: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        for item in result.items() {
            if let (Some(pk), Some(sk)) = (item.get("pk"), item.get("sk")) {
                let _ = dynamo
                    .delete_item()
                    .table_name(table)
                    .key("pk", pk.clone())
                    .key("sk", sk.clone())
                    .send()
                    .await;
            }
        }
        Ok(())
    }

    // Delete jira token from jira-tokens table (keyed differently)
    if let Some((old_token, _)) = load_jira_secret(&state, tid).await {
        let _ = state
            .dynamo
            .delete_item()
            .table_name(&state.config.jira_tokens_table_name)
            .key("token", attr_s(&old_token))
            .send()
            .await;
    }

    // Wipe all domain tables (pk/sk keyed)
    let tables = [
        &state.config.plans_table_name,
        &state.config.jira_config_table_name,
        &state.config.repos_table_name,
        &state.config.settings_table_name,
        &state.config.infra_table_name,
        &state.config.billing_table_name,
    ];

    for table in &tables {
        wipe_table(&state.dynamo, table, tid).await?;
    }

    // Wipe jira-events table (keyed by team_id, event_id)
    let jira_events = state
        .dynamo
        .query()
        .table_name(&state.config.jira_events_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .projection_expression("team_id, event_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query jira-events for reset: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in jira_events.items() {
        if let (Some(pk), Some(sk)) = (item.get("team_id"), item.get("event_id")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.jira_events_table_name)
                .key("team_id", pk.clone())
                .key("event_id", sk.clone())
                .send()
                .await;
        }
    }

    // Wipe runs table (keyed by team_id, run_id)
    let runs = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .expression_attribute_values(":tid", attr_s(tid))
        .projection_expression("team_id, run_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query runs for reset: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for item in runs.items() {
        if let (Some(pk), Some(sk)) = (item.get("team_id"), item.get("run_id")) {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", pk.clone())
                .key("run_id", sk.clone())
                .send()
                .await;
        }
    }

    // Delete WELCOME_SENT from main table (keep META, TEAM)
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tid))
        .key("sk", attr_s("WELCOME_SENT"))
        .send()
        .await;

    info!(team_id = %claims.team_id, "Account data reset complete");
    Ok(StatusCode::OK)
}
