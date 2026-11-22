use axum::{extract::State, http::StatusCode, Extension, Json};
use common::model_stream::accumulate_delta;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::{Claims, PlanExecuteMessage, PlanTaskContinueMessage, WorkerMessage};
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl ToString) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Query parameters of `GET /api/plans`.
#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PlansQuery {
    /// Plans per page, 1 to 100. Default 20.
    #[param(value_type = Option<u32>, minimum = 1, maximum = 100, example = 20)]
    limit: Option<String>,
    /// `next` from the previous page.
    cursor: Option<String>,
}

/// A page of plans.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct PlansPage {
    /// Plans, newest first.
    pub plans: Vec<Plan>,
    /// Pass as `cursor` to read the next page; null on the last page.
    #[schema(example = "PLAN#01JB8V0K3M5P7R9T1W3Y5A7C9E")]
    pub next: Option<String>,
    /// The same as `next`, kept for older clients.
    #[schema(deprecated, example = "PLAN#01JB8V0K3M5P7R9T1W3Y5A7C9E")]
    pub next_cursor: Option<String>,
}

/// GET /api/plans — list plans for the team (paginated).
#[utoipa::path(
    get,
    path = "/api/plans",
    tag = "Plans",
    operation_id = "listPlans",
    summary = "List plans",
    description = "Lists the team's plans, newest first, one page at a time. A listed plan has \
        no tasks; get one plan for its tasks.",
    params(PlansQuery),
    responses(
        (status = 200, description = "A page of plans.", body = PlansPage),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn list_plans(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(query): axum::extract::Query<PlansQuery>,
) -> Result<Json<PlansPage>, StatusCode> {
    let limit: i32 = query
        .limit
        .as_deref()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .clamp(1, 100);

    // Plan rows (`PLAN#<id>`) share the partition with their task rows
    // (`PLAN#<id>#TASK#…`), and DynamoDB applies `Limit` before any filter.
    // A single limited query therefore counted task rows against the page:
    // a page could hold a couple of plans, or none, plus a cursor. Read
    // query pages until `limit` plans are collected, and hand back the sort
    // key of the last plan returned as the cursor.
    type Key = std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;
    let mut start: Option<Key> = query
        .cursor
        .as_deref()
        .filter(|c| !c.is_empty())
        .map(|cursor| {
            Key::from([
                ("pk".to_string(), attr_s(&claims.team_id)),
                ("sk".to_string(), attr_s(cursor)),
            ])
        });
    let mut plans: Vec<Plan> = Vec::new();
    let mut next_cursor: Option<String> = None;
    // Rows written before the derived state existed, or open work last
    // derived a while ago, are derived again here: this is the backfill, a
    // few plans per page so the list stays fast.
    let now = chrono::Utc::now().to_rfc3339();
    let store = plan_store(&state);
    let mut refreshed = 0usize;
    'pages: loop {
        let result = state
            .dynamo
            .query()
            .table_name(&state.config.plans_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(&claims.team_id))
            .expression_attribute_values(":prefix", attr_s("PLAN#"))
            .scan_index_forward(false)
            .limit(100)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                error!("Failed to query plans: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        for item in result.items() {
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            if !is_plan_row(sk) {
                continue;
            }
            if plans.len() >= limit as usize {
                // One more plan exists past this page: keep the cursor.
                break 'pages;
            }
            next_cursor = Some(sk.to_string());
            let mut plan = plan_of(item);
            if refreshed < LIST_REFRESHES
                && common::plan_sync::needs_refresh(item, &now, LIST_STALE_MINUTES)
            {
                refreshed += 1;
                let id = plan.plan_id.clone().unwrap_or_default();
                match common::plan_sync::refresh(&store, &claims.team_id, &id, &now).await {
                    Ok(Some(r)) => apply_view(&mut plan, &r),
                    Ok(None) => {}
                    Err(e) => {
                        warn!(plan_id = %id, error = %e, "Could not refresh the plan's state")
                    }
                }
            }
            plans.push(plan);
        }
        match result.last_evaluated_key() {
            Some(key) => start = Some(key.clone()),
            None => {
                // Read to the end: nothing after the last plan.
                next_cursor = None;
                break;
            }
        }
    }

    Ok(Json(PlansPage {
        plans,
        next: next_cursor.clone(),
        next_cursor,
    }))
}

/// Plans the list derives again per page, at most.
const LIST_REFRESHES: usize = 8;
/// An open plan last derived this long ago is derived again by the list.
const LIST_STALE_MINUTES: i64 = 10;

/// Where plan state is read and written (`common::plan_sync`).
pub(crate) fn plan_store(state: &AppState) -> common::plan_sync::Dynamo {
    common::plan_sync::Dynamo {
        dynamo: state.dynamo.clone(),
        plans_table: state.config.plans_table_name.clone(),
        runs_table: state.config.runs_table_name.clone(),
        settings_table: state.config.settings_table_name.clone(),
    }
}

/// Derives a plan's state again and writes it. Best effort.
pub(crate) async fn refresh_plan(
    state: &AppState,
    team_id: &str,
    plan_id: &str,
) -> Option<common::plan_sync::Refreshed> {
    let now = chrono::Utc::now().to_rfc3339();
    match common::plan_sync::refresh(&plan_store(state), team_id, plan_id, &now).await {
        Ok(r) => r,
        Err(e) => {
            warn!(plan_id, error = %e, "Could not refresh the plan's state");
            None
        }
    }
}

/// A plan's numbers from a fresh derivation.
fn apply_view(plan: &mut Plan, r: &common::plan_sync::Refreshed) {
    let v = &r.plan;
    plan.state = Some(v.state.code().to_string());
    plan.status = Some(v.state.legacy_status().to_string());
    plan.summary = Some(v.summary.clone());
    plan.counts = Some(v.counts.clone());
    plan.needs_count = v.needs.len() as u64;
    plan.cost_usd = Some(round_cents(v.cost_usd));
    plan.attempts = Some(v.attempts as u64);
    plan.criteria_met = Some(r.criteria.iter().filter(|c| c.met).count() as u64);
    plan.criteria_count = Some(r.criteria.len() as u64);
    plan.last_activity_at = Some(v.last_activity.clone()).filter(|s| !s.is_empty());
}

fn round_cents(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// A GitHub pull request event moved a plan task's work: refresh its plan.
/// Pull requests opened, reviewed, checked, merged and closed all land
/// here (`github_webhook::dispatch`). Only runs that belong to a plan cost
/// more than the one lookup.
pub(crate) async fn on_pr_event(
    state: &AppState,
    team_id: &str,
    event_type: &str,
    payload: &Value,
) {
    let repo = payload["repository"]["full_name"].as_str().unwrap_or("");
    let Some((owner, name)) = repo.split_once('/') else {
        return;
    };
    let numbers: Vec<u64> = match event_type {
        "pull_request" | "pull_request_review" => payload["pull_request"]["number"]
            .as_u64()
            .into_iter()
            .collect(),
        "check_suite" if payload["action"].as_str() == Some("completed") => payload["check_suite"]
            ["pull_requests"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|p| p["number"].as_u64())
                    .take(5)
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    for number in numbers {
        let Some(run) =
            super::github_webhook::lookup_run_item_by_pr(state, team_id, owner, name, number).await
        else {
            continue;
        };
        let Some((plan_id, _)) = common::plan_sync::plan_of_run(&run) else {
            continue;
        };
        let run_id = common::paged::run_id_of(&run).unwrap_or_default();
        let facts = pr_event_facts(event_type, payload);
        if !run_id.is_empty() && !facts.is_empty() {
            let mut u = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(team_id))
                .key("run_id", attr_s(&run_id))
                .condition_expression("attribute_exists(run_id)");
            let mut parts = Vec::new();
            for (i, (k, v)) in facts.into_iter().enumerate() {
                parts.push(format!("{k} = :f{i}"));
                u = u.expression_attribute_values(format!(":f{i}"), v);
            }
            if let Err(e) = u
                .update_expression(format!("SET {}", parts.join(", ")))
                .send()
                .await
            {
                warn!(run_id, error = %e, "Could not record the pull request's facts on the run");
            }
        }
        refresh_plan(state, team_id, &plan_id).await;
    }
}

/// What a pull request event tells about the pull request, as run fields:
/// its size, and a person's review verdict.
pub(crate) fn pr_event_facts(
    event_type: &str,
    payload: &Value,
) -> Vec<(&'static str, aws_sdk_dynamodb::types::AttributeValue)> {
    let mut out = Vec::new();
    let pr = &payload["pull_request"];
    if let (Some(a), Some(d)) = (pr["additions"].as_u64(), pr["deletions"].as_u64()) {
        out.push(("pr_additions", attr_n(a)));
        out.push(("pr_deletions", attr_n(d)));
    }
    if event_type == "pull_request_review" && payload["action"].as_str() == Some("submitted") {
        let by_bot = payload["review"]["user"]["type"].as_str() == Some("Bot")
            || payload["review"]["user"]["login"]
                .as_str()
                .is_some_and(|l| l.ends_with("[bot]") || l.contains("coderhelm"));
        let verdict = match payload["review"]["state"].as_str().unwrap_or("") {
            s if s.eq_ignore_ascii_case("approved") => Some("approved"),
            s if s.eq_ignore_ascii_case("changes_requested") => Some("changes_requested"),
            _ => None,
        };
        if let (false, Some(v)) = (by_bot, verdict) {
            out.push(("pr_review", attr_s(v)));
        }
    }
    out
}

/// A plan's own row, as opposed to one of its task rows.
fn is_plan_row(sk: &str) -> bool {
    sk.starts_with("PLAN#") && !sk.contains("#TASK#")
}

/// A new plan.
#[derive(serde::Deserialize, utoipa::ToSchema)]
#[schema(example = json!({
    "title": "Rate-limit the login form",
    "description": "Brute-force attempts are showing up in the auth logs.",
    "repo": "acme/web",
    "tasks": [
        {
            "title": "Limit login attempts per IP",
            "description": "Allow 10 attempts per IP per 15 minutes; answer 429 after that.",
            "acceptance_criteria": "The 11th attempt within 15 minutes gets 429 and a Retry-After header."
        }
    ]
}))]
pub struct CreatePlan {
    /// What the plan is for. 1 to 500 characters.
    #[schema(example = "Rate-limit the login form")]
    pub title: String,
    /// More about it, in Markdown.
    #[serde(default)]
    #[schema(example = "Brute-force attempts are showing up in the auth logs.")]
    pub description: Option<String>,
    /// The repository its tasks work in, `owner/name`, unless a task names its own.
    #[serde(default)]
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// Where each task becomes a ticket: a GitHub issue (`github`, the
    /// default) or a Jira ticket (`jira`).
    #[serde(default)]
    pub destination: Option<PlanDestination>,
    /// The plan's tasks, in order. Each starts as `draft`.
    #[serde(default)]
    #[schema(example = json!([{"title": "Limit login attempts per IP", "description": "Allow 10 attempts per IP per 15 minutes; answer 429 after that.", "acceptance_criteria": "The 11th attempt within 15 minutes gets 429 and a Retry-After header."}]))]
    pub tasks: Option<Vec<NewPlanTask>>,
    /// What done means for the whole plan: short checkable lines. Each task
    /// names the ones it covers (`covers`); without a list, each task's
    /// acceptance criteria are the plan's.
    #[serde(default)]
    #[schema(example = json!(["The 11th login attempt within 15 minutes gets 429", "The page says when to try again"]))]
    pub success_criteria: Option<Vec<String>>,
    /// The plan chat it came from (dashboard only).
    #[serde(default)]
    #[schema(ignore)]
    pub chat_id: Option<String>,
    /// MCP servers used while planning (dashboard only).
    #[serde(default)]
    #[schema(ignore)]
    pub mcp_servers: Option<Vec<String>>,
}

/// Where a plan's tasks become tickets: `github` (GitHub issues) or `jira`.
#[derive(serde::Deserialize, utoipa::ToSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanDestination {
    Github,
    Jira,
}

/// A task in a new plan.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct NewPlanTask {
    /// What to do. Defaults to "Untitled task".
    #[serde(default)]
    #[schema(example = "Limit login attempts per IP")]
    pub title: Option<String>,
    /// How, in Markdown. The coder reads it as the ticket body.
    #[serde(default)]
    #[schema(example = "Allow 10 attempts per IP per 15 minutes; answer 429 after that.")]
    pub description: Option<String>,
    /// How to tell it is done.
    #[serde(default)]
    #[schema(example = "The 11th attempt within 15 minutes gets 429 and a Retry-After header.")]
    pub acceptance_criteria: Option<String>,
    /// The repository for this task, `owner/name`, when not the plan's.
    #[serde(default)]
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// Where this task becomes a ticket, when not where the plan's do.
    #[serde(default)]
    pub destination: Option<PlanDestination>,
    /// With `jira`: the Jira project key to file the ticket in.
    #[serde(default)]
    #[schema(example = "WEB")]
    pub jira_project: Option<String>,
    /// The plan's success criteria this task covers, by id (`C1` is the
    /// first of `success_criteria`).
    #[serde(default)]
    #[schema(example = json!(["C1"]))]
    pub covers: Option<Vec<String>>,
}

/// The plan that was made.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct CreatedPlan {
    /// The new plan's id.
    #[schema(example = "01JB8V0K3M5P7R9T1W3Y5A7C9E")]
    pub plan_id: String,
}

/// POST /api/plans — create a new plan.
#[utoipa::path(
    post,
    path = "/api/plans",
    tag = "Plans",
    operation_id = "createPlan",
    summary = "Create a plan",
    description = "Creates a plan, with its tasks if you send them. The plan and its tasks start \
        as `draft`; nothing runs until the plan is executed. Not idempotent: sending the same \
        request twice makes two plans.",
    request_body(content = CreatePlan, description = "The plan.", content_type = "application/json"),
    responses(
        (status = 200, description = "The plan was made.", body = CreatedPlan),
        (status = 400, description = "The body is not valid JSON, or the title is missing or longer than 500 characters.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["plans"])),
)]
pub async fn create_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(raw): Json<Value>,
) -> Result<Json<CreatedPlan>, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    let body: CreatePlan = serde_json::from_value(raw).map_err(|_| StatusCode::BAD_REQUEST)?;
    let title = body.title.as_str();
    let description = body.description.as_deref().unwrap_or("");
    let repo = body.repo.as_deref().unwrap_or("");
    let destination = match body.destination {
        Some(PlanDestination::Jira) => "jira",
        _ => "github",
    };

    if title.is_empty() || title.len() > 500 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let plan_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("PLAN#{plan_id}");

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("repo", attr_s(repo))
        .item("destination", attr_s(destination))
        .item("status", attr_s("draft"))
        .item("task_count", attr_n(0))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now));

    // The chat the plan was made in, so its page can say what the chat used.
    let chat_id = super::plan_chat_tools::chat_id(body.chat_id.as_deref());
    if chat_id != "unsaved" {
        put = put.item("chat_id", attr_s(&chat_id));
    }
    // Tasks a plan chat wrote are CoderHelm's; tasks sent another way are
    // the sender's (the drift marks say who wrote what).
    let author = if chat_id != "unsaved" {
        "agent"
    } else {
        "person"
    };
    if let Some(list) = criteria_list(body.success_criteria.as_deref()) {
        put = put.item("criteria", list);
    }

    // Store which MCP servers were used during chat planning
    if let Some(servers) = &body.mcp_servers {
        let server_list: Vec<aws_sdk_dynamodb::types::AttributeValue> =
            servers.iter().map(|s| attr_s(s)).collect();
        if !server_list.is_empty() {
            put = put.item(
                "mcp_servers",
                aws_sdk_dynamodb::types::AttributeValue::L(server_list),
            );
        }
    }

    put.send().await.map_err(|e| {
        error!("Failed to create plan: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Create tasks if provided inline
    if let Some(tasks) = &body.tasks {
        for (i, task) in tasks.iter().enumerate() {
            let task_title = task.title.as_deref().unwrap_or("Untitled task");
            let task_desc = task.description.as_deref().unwrap_or("");
            let task_criteria = task.acceptance_criteria.as_deref().unwrap_or("");
            let task_repo = task.repo.as_deref().unwrap_or("");
            let task_id = ulid::Ulid::new().to_string();
            let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

            let mut put = state
                .dynamo
                .put_item()
                .table_name(&state.config.plans_table_name)
                .item("pk", attr_s(&claims.team_id))
                .item("sk", attr_s(&task_sk))
                .item("plan_id", attr_s(&plan_id))
                .item("task_id", attr_s(&task_id))
                .item("title", attr_s(task_title))
                .item("description", attr_s(task_desc))
                .item("acceptance_criteria", attr_s(task_criteria))
                .item("status", attr_s("draft"))
                .item("task_order", attr_n(i))
                .item("created_at", attr_s(&now));

            if !task_repo.is_empty() {
                put = put.item("repo", attr_s(task_repo));
            }
            let task_dest = match task.destination {
                Some(PlanDestination::Jira) => "jira",
                Some(PlanDestination::Github) => "github",
                None => destination,
            };
            put = put.item("destination", attr_s(task_dest));
            if let Some(jp) = task.jira_project.as_deref() {
                put = put.item("jira_project", attr_s(jp));
            }
            put = put.item("added_by_kind", attr_s(author));
            if let Some(list) = criteria_list(task.covers.as_deref()) {
                put = put.item("covers", list);
            }

            put.send().await.map_err(|e| {
                error!("Failed to create task: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }

        // Update task count
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&sk))
            .update_expression("SET task_count = :c")
            .expression_attribute_values(":c", attr_n(tasks.len()))
            .send()
            .await
            .ok();
    }

    track_plan_usage(&state, &claims.team_id).await;

    Ok(Json(CreatedPlan { plan_id }))
}

/// Increment total_plans in analytics (plans are unlimited, no overage billing).
async fn track_plan_usage(state: &Arc<AppState>, team_id: &str) {
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in &[month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(team_id))
            .key("period", attr_s(period))
            .update_expression("ADD total_plans :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }
}

/// A plan with its tasks.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct PlanWithTasks {
    #[serde(flatten)]
    pub plan: Plan,
    /// The plan's tasks, in order.
    pub tasks: Vec<PlanTask>,
    /// The success criteria, each with the tasks that cover it and the
    /// evidence the work gave it: `{id, text, tasks, met, evidence: [{task_id,
    /// run_id, state, words, by, tests}]}`. `state` is the criteria trace's
    /// proof: `proven`, `not_the_ticket`, `failing`, `missing`, `unverified`
    /// or `not_run`.
    #[schema(value_type = Vec<Object>, example = json!([{"id": "C1", "text": "The 11th login attempt within 15 minutes gets 429", "tasks": ["01JB8V0P8Q2S4U6W8Y0A2C4E6G"], "met": true, "evidence": [{"task_id": "01JB8V0P8Q2S4U6W8Y0A2C4E6G", "run_id": "01JB8X6F4M2C9QZ3T7V5W1K0HA", "state": "proven", "words": "proven", "by": "tests with and without the change"}]}]))]
    pub criteria: Vec<common::plan_state::Criterion>,
    /// What waits on a person, in task order, each the exact ask:
    /// `{task_id, kind, ask, detail, url, run_id, action}`. `kind` is
    /// `answer`, `review`, `approval`, `checks`, `merge`, `failed`,
    /// `closed_pr` or `approve_rest`.
    #[schema(value_type = Vec<Object>, example = json!([{"task_id": "01JB8V0P8Q2S4U6W8Y0A2C4E6G", "kind": "approval", "ask": "Approve pull request #318", "detail": "This repo's auto-merge setting requires an approving review from a person.", "url": "https://github.com/acme/web/pull/318", "run_id": "01JB8X6F4M2C9QZ3T7V5W1K0HA", "action": "open_pr"}]))]
    pub needs: Vec<common::plan_state::NeedsItem>,
    /// What starting the draft tasks, or retrying one, is expected to take,
    /// from the team's recent runs: `{tasks, runs_low, runs_high, cost_low,
    /// cost_high, per_run_low, per_run_high, sample}`. Null when nothing is
    /// left to start or retry.
    #[schema(value_type = Option<StartEstimate>)]
    pub estimate: Option<common::plan_sync::Estimate>,
}

/// GET /api/plans/:plan_id — get plan with all tasks.
#[utoipa::path(
    get,
    path = "/api/plans/{plan_id}",
    tag = "Plans",
    operation_id = "getPlan",
    summary = "Get a plan",
    description = "Returns one plan with its tasks in order. Once the plan runs, each task shows \
        the ticket it became and the run working on it (`run_id`), which you can follow with \
        `GET /api/runs/{run_id}`.",
    params(("plan_id" = String, Path, description = "The plan's id.", example = "01JB8V0K3M5P7R9T1W3Y5A7C9E")),
    responses(
        (status = 200, description = "The plan and its tasks.", body = PlanWithTasks),
        (status = 400, description = "The id is not a plan id.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No plan with this id in the team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["view"])),
)]
pub async fn get_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<PlanWithTasks>, StatusCode> {
    validate_plan_id(&plan_id)?;

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Separate plan from tasks
    let mut plan_data: Option<Plan> = None;
    let mut tasks: Vec<PlanTask> = Vec::new();

    for item in items {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            tasks.push(task_of(item));
        } else {
            plan_data = Some(plan_of(item));
        }
    }

    let mut plan = plan_data.ok_or(StatusCode::NOT_FOUND)?;
    // Sort tasks by order
    tasks.sort_by_key(|t| t.order);

    // Where each task stands, from its runs and pull requests now.
    let mut criteria = Vec::new();
    let mut needs = Vec::new();
    if let Some(r) = refresh_plan(&state, &claims.team_id, &plan_id).await {
        apply_view(&mut plan, &r);
        for task in tasks.iter_mut() {
            let id = task.task_id.clone().unwrap_or_default();
            if let Some(d) = r.tasks.iter().find(|d| d.task_id == id) {
                apply_task_view(task, d);
            }
        }
        criteria = r.criteria;
        needs = r.plan.needs;
    }
    let to_start = tasks
        .iter()
        .filter(|t| matches!(t.state.as_deref(), Some("draft")))
        .count();
    let to_retry = tasks
        .iter()
        .any(|t| matches!(t.state.as_deref(), Some("failed")));
    let estimate = if to_start > 0 || to_retry {
        let recent = common::plan_sync::recent_costs(
            &state.dynamo,
            &state.config.runs_table_name,
            &claims.team_id,
        )
        .await;
        Some(common::plan_sync::estimate(to_start, &recent))
    } else {
        None
    };

    Ok(Json(PlanWithTasks {
        plan,
        tasks,
        criteria,
        needs,
        estimate,
    }))
}

/// A task's state from a fresh derivation.
fn apply_task_view(task: &mut PlanTask, d: &common::plan_sync::TaskDetail) {
    let v = &d.view;
    task.state = Some(v.state.code().to_string());
    task.state_detail = Some(v.detail.clone());
    task.state_reason = Some(v.reason.clone()).filter(|s| !s.is_empty());
    task.attempts = Some(v.attempts as u64);
    task.cost_usd = Some(round_cents(v.cost_usd));
    task.runs = Some(v.runs.clone());
    task.prs = Some(v.prs.clone());
    task.needs = v.needs.clone();
    task.drift = Some(v.drift.clone());
    task.covers = Some(d.covers.clone());
    task.ticket_filed = Some(v.ticket_filed);
    task.last_activity_at = Some(v.last_activity.clone()).filter(|s| !s.is_empty());
    if let Some(last) = v.runs.last() {
        task.run_id = Some(last.run_id.clone());
    }
    if let Some(pr) = v.prs.first() {
        task.pr_url = Some(pr.url.clone());
    }
}

/// A list of short lines as a DynamoDB list, blank lines dropped; `None`
/// when nothing is left.
fn criteria_list(lines: Option<&[String]>) -> Option<aws_sdk_dynamodb::types::AttributeValue> {
    let list: Vec<aws_sdk_dynamodb::types::AttributeValue> = lines?
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .take(50)
        .map(|l| attr_s(common::truncate_str(l, 600)))
        .collect();
    (!list.is_empty()).then_some(aws_sdk_dynamodb::types::AttributeValue::L(list))
}

/// PUT /api/plans/:plan_id — update plan metadata.
pub async fn update_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    validate_plan_id(&plan_id)?;
    let sk = format!("PLAN#{plan_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let mut update_parts = vec!["updated_at = :now".to_string()];
    let mut expr_values = vec![(":now".to_string(), attr_s(&now))];

    if let Some(title) = body["title"].as_str() {
        update_parts.push("title = :t".to_string());
        expr_values.push((":t".to_string(), attr_s(title)));
    }
    if let Some(desc) = body["description"].as_str() {
        update_parts.push("description = :d".to_string());
        expr_values.push((":d".to_string(), attr_s(desc)));
    }
    if let Some(status) = body["status"].as_str() {
        update_parts.push("#st = :s".to_string());
        expr_values.push((":s".to_string(), attr_s(status)));
    }
    if let Some(dest) = body["destination"].as_str() {
        if dest == "github" || dest == "jira" {
            update_parts.push("destination = :dest".to_string());
            expr_values.push((":dest".to_string(), attr_s(dest)));
        }
    }
    if let Some(list) = body["success_criteria"].as_array() {
        let lines: Vec<String> = list
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        update_parts.push("criteria = :crit".to_string());
        expr_values.push((
            ":crit".to_string(),
            criteria_list(Some(&lines))
                .unwrap_or(aws_sdk_dynamodb::types::AttributeValue::L(Vec::new())),
        ));
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .update_expression(&update_expr)
        .condition_expression("attribute_exists(pk)");

    // "status" is a reserved word in DynamoDB
    if body["status"].is_string() {
        req = req.expression_attribute_names("#st", "status");
    }

    for (name, val) in expr_values {
        req = req.expression_attribute_values(&name, val);
    }

    req.send().await.map_err(|e| {
        error!("Failed to update plan: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Propagate destination change to all tasks
    if let Some(dest) = body["destination"].as_str() {
        if dest == "github" || dest == "jira" {
            let task_prefix = format!("PLAN#{plan_id}#TASK#");
            let result = state
                .dynamo
                .query()
                .table_name(&state.config.plans_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
                .expression_attribute_values(":pk", attr_s(&claims.team_id))
                .expression_attribute_values(":prefix", attr_s(&task_prefix))
                .projection_expression("pk, sk")
                .send()
                .await
                .map_err(|e| {
                    error!("Failed to query tasks for destination update: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            for item in result.items() {
                if let Some(task_sk) = item.get("sk").and_then(|v| v.as_s().ok()) {
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.plans_table_name)
                        .key("pk", attr_s(&claims.team_id))
                        .key("sk", attr_s(task_sk))
                        .update_expression("SET destination = :dest")
                        .expression_attribute_values(":dest", attr_s(dest))
                        .send()
                        .await;
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

/// DELETE /api/plans/:plan_id — delete plan and all tasks.
pub async fn delete_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<StatusCode, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(3)?;
    validate_plan_id(&plan_id)?;

    // Query all items for this plan (plan + tasks)
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .projection_expression("pk, sk")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan for deletion: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Delete each item
    for item in result.items() {
        let pk = item
            .get("pk")
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let sk = item
            .get("sk")
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        state
            .dynamo
            .delete_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", pk)
            .key("sk", sk)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to delete plan item: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/tasks — add a task to a plan.
pub async fn add_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;

    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    let description = body["description"].as_str().unwrap_or("");
    let acceptance_criteria = body["acceptance_criteria"].as_str().unwrap_or("");
    let task_repo = body["repo"].as_str().unwrap_or("");
    let order = body["order"].as_u64().unwrap_or(0);

    let task_id = ulid::Ulid::new().to_string();
    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&task_sk))
        .item("plan_id", attr_s(&plan_id))
        .item("task_id", attr_s(&task_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("acceptance_criteria", attr_s(acceptance_criteria))
        .item("status", attr_s("draft"))
        .item("task_order", attr_n(order))
        .item("created_at", attr_s(&now))
        .item("added_by_kind", attr_s("person"))
        .item("added_by", attr_s(&claims.display_name()));

    if !task_repo.is_empty() {
        put = put.item("repo", attr_s(task_repo));
    }
    if let Some(list) = body["covers"].as_array() {
        let ids: Vec<String> = list
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        if let Some(l) = criteria_list(Some(&ids)) {
            put = put.item("covers", l);
        }
    }
    if let Some(dest) = body["destination"].as_str() {
        put = put.item("destination", attr_s(dest));
    }
    if let Some(jp) = body["jira_project"].as_str() {
        put = put.item("jira_project", attr_s(jp));
    }
    if let Some(dep) = body["depends_on"].as_str() {
        if !dep.is_empty() {
            put = put.item("depends_on", attr_s(dep));
        }
    }

    put.send().await.map_err(|e| {
        error!("Failed to create task: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Increment task count
    let plan_sk = format!("PLAN#{plan_id}");
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression(
            "SET task_count = if_not_exists(task_count, :zero) + :one, updated_at = :now",
        )
        .expression_attribute_values(":zero", attr_n(0))
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .ok();

    Ok(Json(json!({ "task_id": task_id })))
}

/// PUT /api/plans/:plan_id/tasks/:task_id — update a task.
pub async fn update_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

    let mut update_parts = Vec::new();
    let mut expr_values = Vec::new();

    if let Some(title) = body["title"].as_str() {
        update_parts.push("title = :t");
        expr_values.push((":t", attr_s(title)));
    }
    if let Some(desc) = body["description"].as_str() {
        update_parts.push("description = :d");
        expr_values.push((":d", attr_s(desc)));
    }
    if let Some(criteria) = body["acceptance_criteria"].as_str() {
        update_parts.push("acceptance_criteria = :ac");
        expr_values.push((":ac", attr_s(criteria)));
    }
    if let Some(repo) = body["repo"].as_str() {
        update_parts.push("repo = :r");
        expr_values.push((":r", attr_s(repo)));
    }
    if let Some(order) = body["order"].as_u64() {
        update_parts.push("task_order = :o");
        expr_values.push((":o", attr_n(order)));
    }
    if let Some(status) = body["status"].as_str() {
        update_parts.push("#st = :s");
        expr_values.push((":s", attr_s(status)));
    }
    if let Some(dest) = body["destination"].as_str() {
        update_parts.push("destination = :dest");
        expr_values.push((":dest", attr_s(dest)));
    }
    if let Some(jp) = body["jira_project"].as_str() {
        update_parts.push("jira_project = :jp");
        expr_values.push((":jp", attr_s(jp)));
    }
    if let Some(dep) = body["depends_on"].as_str() {
        if dep.is_empty() {
            update_parts.push("depends_on = :dep");
            expr_values.push((":dep", attr_s("")));
        } else {
            update_parts.push("depends_on = :dep");
            expr_values.push((":dep", attr_s(dep)));
        }
    }

    if let Some(list) = body["covers"].as_array() {
        let ids: Vec<String> = list
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        update_parts.push("covers = :cov");
        expr_values.push((
            ":cov",
            criteria_list(Some(&ids))
                .unwrap_or(aws_sdk_dynamodb::types::AttributeValue::L(Vec::new())),
        ));
    }

    if update_parts.is_empty() {
        return Ok(StatusCode::OK);
    }
    // A change to what the task says is marked with who made it, so the
    // plan page can show what moved after approval.
    let what_changed = [
        "title",
        "description",
        "acceptance_criteria",
        "repo",
        "depends_on",
    ]
    .iter()
    .any(|k| body.get(*k).is_some_and(|v| v.is_string()));
    let now = chrono::Utc::now().to_rfc3339();
    let by = claims.display_name();
    if what_changed {
        update_parts.push("edited_at = :ea");
        expr_values.push((":ea", attr_s(&now)));
        update_parts.push("edited_by = :eb");
        expr_values.push((":eb", attr_s(&by)));
        update_parts.push("edited_by_kind = :ek");
        expr_values.push((":ek", attr_s("person")));
    }

    let update_expr = format!("SET {}", update_parts.join(", "));

    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .update_expression(&update_expr)
        .condition_expression("attribute_exists(pk)");

    if body["status"].is_string() {
        req = req.expression_attribute_names("#st", "status");
    }

    for (name, val) in expr_values {
        req = req.expression_attribute_values(name, val);
    }

    req.send().await.map_err(|e| {
        error!("Failed to update task: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}

/// DELETE /api/plans/:plan_id/tasks/:task_id — remove a task.
pub async fn delete_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Decrement task count
    let plan_sk = format!("PLAN#{plan_id}");
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&plan_sk))
        .update_expression(
            "SET task_count = if_not_exists(task_count, :one) - :one, updated_at = :now",
        )
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":now", attr_s(&now))
        .send()
        .await
        .ok();

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/tasks/:task_id/approve — approve a task and
/// start it now. The plan runs with its approved tasks and stays open for
/// the rest: approving one task never finishes the plan.
pub async fn approve_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;
    // A task starts a paid run, like any other start.
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    // 1. Approve it and queue it, before the message that starts it: the
    //    worker never finds a task it is filing still marked approved.
    let out = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, approved_at = :now, approved_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("queued"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        // Only a task that has not started: approving a filed task again
        // would file a second ticket and start a second run.
        .condition_expression("attribute_exists(pk) AND #st IN (:draft, :rejected, :approved)")
        .expression_attribute_values(":draft", attr_s("draft"))
        .expression_attribute_values(":rejected", attr_s("rejected"))
        .expression_attribute_values(":approved", attr_s("approved"))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await
        .map_err(|e| {
            if aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                .to_string()
                .contains("ConditionalCheckFailed")
            {
                return StatusCode::CONFLICT;
            }
            error!("Failed to approve task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    record_plan_task_reward(
        &state,
        &claims.team_id,
        &plan_id,
        &task_id,
        common::rewards::RewardKind::PlanTaskApproved,
        out.attributes(),
        &now,
    )
    .await;

    // 2. The plan is running; when it first ran is kept (drift counts from
    //    it). Its state is derived from its tasks below, never set to done.
    mark_plan_running(&state, &claims, &plan_id, &now).await;

    // 3. Start this one task.
    let plan_msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        triggered_by: claims.display_name(),
        tasks: vec![task_id.clone()],
    });
    if let Err(code) = send_plan_message(&state, &plan_msg).await {
        // Not started: back to approved, so Approve and start picks it up.
        set_task_word(&state, &claims.team_id, &plan_id, &task_id, "approved").await;
        return Err(code);
    }

    info!(plan_id, task_id, "Task approved and dispatched to worker");
    refresh_plan(&state, &claims.team_id, &plan_id).await;

    Ok(Json(json!({ "dispatched": true, "task_id": task_id })))
}

/// Marks a plan running; keeps when and by whom it first ran.
async fn mark_plan_running(state: &AppState, claims: &Claims, plan_id: &str, now: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("PLAN#{plan_id}")))
        .condition_expression("attribute_exists(sk)")
        .update_expression(
            "SET #st = :s, executed_at = if_not_exists(executed_at, :now), \
             executed_by = if_not_exists(executed_by, :by), updated_at = :now",
        )
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("executing"))
        .expression_attribute_values(":now", attr_s(now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .send()
        .await;
}

/// Sends a plan message to the worker's queue.
async fn send_plan_message(state: &AppState, msg: &WorkerMessage) -> Result<(), StatusCode> {
    let body = serde_json::to_string(msg).map_err(|e| {
        error!("Failed to serialize plan message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| {
            error!("Failed to send plan message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// Sets a task row's own word (`status`).
async fn set_task_word(state: &AppState, team_id: &str, plan_id: &str, task_id: &str, word: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("PLAN#{plan_id}#TASK#{task_id}")))
        .condition_expression("attribute_exists(sk)")
        .update_expression("SET #st = :s")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s(word))
        .send()
        .await;
}

/// A plan task approved or rejected, as a reward signal for the planner.
async fn record_plan_task_reward(
    state: &AppState,
    team_id: &str,
    plan_id: &str,
    task_id: &str,
    kind: common::rewards::RewardKind,
    task: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    at: &str,
) {
    let get = |k: &str| {
        task.and_then(|t| t.get(k))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let title = get("title");
    let ev = common::rewards::RewardEvent::new(
        team_id,
        kind,
        common::rewards::event::AGENT_PLANNER,
        at,
        &format!("{}#{plan_id}#{task_id}", kind.key()),
    )
    .with_repo(&get("repo"), 0)
    .with_detail(&format!("Plan task: {title}"))
    .with_source("dashboard:plan");
    let store =
        common::rewards::store::Store::new(&state.dynamo, &state.config.settings_table_name);
    if let Err(e) = store.record(&ev).await {
        tracing::warn!(error = %e, "Could not record the plan task outcome");
    }
}

/// POST /api/plans/:plan_id/tasks/:task_id/reject — reject a task.
pub async fn reject_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;

    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let out = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .update_expression("SET #st = :s, rejected_at = :now, rejected_by = :by")
        .expression_attribute_names("#st", "status")
        .expression_attribute_values(":s", attr_s("rejected"))
        .expression_attribute_values(":now", attr_s(&now))
        .expression_attribute_values(":by", attr_s(&claims.display_name()))
        .condition_expression("attribute_exists(pk)")
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to reject task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    record_plan_task_reward(
        &state,
        &claims.team_id,
        &plan_id,
        &task_id,
        common::rewards::RewardKind::PlanTaskRejected,
        out.attributes(),
        &now,
    )
    .await;

    Ok(StatusCode::OK)
}

/// POST /api/plans/:plan_id/approve-and-execute — approve all draft tasks and execute.
#[utoipa::path(
    post,
    path = "/api/plans/{plan_id}/approve-and-execute",
    tag = "Plans",
    operation_id = "approveAndExecutePlan",
    summary = "Approve and run a plan",
    description = "Approves every `draft` task of the plan and runs the plan: each approved task \
        becomes a GitHub issue or Jira ticket and is queued as a run, in task order. The plan \
        becomes `executing`. Rejected tasks are skipped. Answers `400` when no task is left to \
        run, so calling it again after a successful call does not run anything twice.",
    params(("plan_id" = String, Path, description = "The plan's id.", example = "01JB8V0K3M5P7R9T1W3Y5A7C9E")),
    responses(
        (status = 200, description = "The plan is running.", body = PlanExecution),
        (status = 400, description = "The id is not a plan id, or the plan has no draft or approved tasks.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["plans"])),
)]
pub async fn approve_all_and_execute(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<PlanExecution>, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    validate_plan_id(&plan_id)?;
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let now = chrono::Utc::now().to_rfc3339();
    let mut task_ids: Vec<(String, u64)> = Vec::new();

    for item in result.items() {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
        if !sk.contains("#TASK#") {
            continue;
        }
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
        let tid = item
            .get("task_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let order = item
            .get("task_order")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0u64);

        // Approve draft tasks, include already-approved tasks
        if status == "draft" {
            let task_sk = format!("PLAN#{plan_id}#TASK#{tid}");
            state
                .dynamo
                .update_item()
                .table_name(&state.config.plans_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s(&task_sk))
                .update_expression("SET #st = :s, approved_at = :now, approved_by = :by")
                .expression_attribute_names("#st", "status")
                .expression_attribute_values(":s", attr_s("approved"))
                .expression_attribute_values(":now", attr_s(&now))
                .expression_attribute_values(":by", attr_s(&claims.display_name()))
                .send()
                .await
                .ok();
            task_ids.push((tid, order));
        } else if status == "approved" {
            task_ids.push((tid, order));
        }
    }

    if task_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    task_ids.sort_by_key(|t| t.1);
    let ids: Vec<String> = task_ids.iter().map(|(tid, _)| tid.clone()).collect();

    mark_plan_running(&state, &claims, &plan_id, &now).await;
    start_tasks(&state, &claims, &plan_id, &ids).await?;

    super::webhooks::emit_plan_approved(
        &state,
        &claims,
        &plan_id,
        &super::webhooks::plan_title(result.items(), &plan_id),
        task_ids.len(),
    )
    .await;
    refresh_plan(&state, &claims.team_id, &plan_id).await;

    Ok(Json(PlanExecution {
        status: "executing".into(),
        tasks_queued: task_ids.len(),
    }))
}

/// POST /api/plans/:plan_id/tasks/:task_id/force-run — force-run a waiting task.
pub async fn force_run_task(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((plan_id, task_id)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    validate_plan_id(&plan_id)?;
    validate_plan_id(&task_id)?;
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // Verify task is in "waiting" status
    let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&task_sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get task: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let status = item
        .item()
        .and_then(|i| i.get("status"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("");

    if status != "waiting" && status != "failed" {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Reset task status to "waiting" so the worker picks it up
    if status == "failed" {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.plans_table_name)
            .key("pk", attr_s(&claims.team_id))
            .key("sk", attr_s(&task_sk))
            .update_expression("SET #s = :s REMOVE failure_reason")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr_s("waiting"))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to reset task status: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    // Send PlanTaskContinue message to the worker
    let msg = WorkerMessage::PlanTaskContinue(PlanTaskContinueMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.clone(),
        tasks: vec![task_id.clone()],
    });

    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(serde_json::to_string(&msg).map_err(|e| {
            error!("Failed to serialize force-run message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send force-run message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "status": "running" })))
}

/// POST /api/plans/:plan_id/execute — execute the plan (creates issues, queues tasks).
pub async fn execute_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // plans.rs had NO role gates — any authed viewer could dispatch paid
    // agent runs or delete the team's plans.
    claims.require_role(1)?;
    if super::github_webhook::check_run_budget(&state, &claims.team_id)
        .await
        .is_some()
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    validate_plan_id(&plan_id)?;

    // Get plan + all tasks
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Collect approved task IDs (worker reads full task data from DynamoDB)
    let mut approved_task_ids: Vec<(String, u64)> = Vec::new(); // (task_id, order)

    for item in result.items() {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str())
                .unwrap_or("");
            if status == "approved" {
                let tid = item
                    .get("task_id")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default();
                let order = item
                    .get("task_order")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0u64);
                approved_task_ids.push((tid, order));
            }
        }
    }

    if approved_task_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST); // No approved tasks
    }

    // Sort by order
    approved_task_ids.sort_by_key(|t| t.1);
    let ids: Vec<String> = approved_task_ids
        .iter()
        .map(|(tid, _)| tid.clone())
        .collect();
    let now = chrono::Utc::now().to_rfc3339();
    mark_plan_running(&state, &claims, &plan_id, &now).await;
    start_tasks(&state, &claims, &plan_id, &ids).await?;
    refresh_plan(&state, &claims.team_id, &plan_id).await;

    Ok(Json(json!({
        "status": "executing",
        "tasks_queued": approved_task_ids.len(),
    })))
}

/// Queues approved tasks, then sends the one message that starts them. The
/// tasks are marked queued first: the worker files their tickets and moves
/// them on, and a `queued` written after the message could land on a task
/// the worker had already filed. When the message can't be sent, they go
/// back to approved, so starting again finds them.
async fn start_tasks(
    state: &AppState,
    claims: &Claims,
    plan_id: &str,
    ids: &[String],
) -> Result<(), StatusCode> {
    for id in ids {
        set_task_word(state, &claims.team_id, plan_id, id, "queued").await;
    }
    let msg = WorkerMessage::PlanExecute(PlanExecuteMessage {
        team_id: claims.team_id.clone(),
        plan_id: plan_id.to_string(),
        triggered_by: claims.display_name(),
        tasks: ids.to_vec(),
    });
    if let Err(code) = send_plan_message(state, &msg).await {
        for id in ids {
            set_task_word(state, &claims.team_id, plan_id, id, "approved").await;
        }
        return Err(code);
    }
    Ok(())
}

/// A plan that started running.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct PlanExecution {
    /// Always `executing`.
    #[schema(example = "executing")]
    pub status: String,
    /// How many tasks were queued.
    #[schema(example = 3)]
    pub tasks_queued: usize,
}

// ─── Helpers ────────────────────────────────────────────────────────

fn validate_plan_id(id: &str) -> Result<(), StatusCode> {
    if id.is_empty() || id.len() > 30 || !id.chars().all(|c| c.is_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn plan_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    serde_json::to_value(plan_of(item)).unwrap_or_default()
}

fn task_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    serde_json::to_value(task_of(item)).unwrap_or_default()
}

/// A plan.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct Plan {
    /// The plan's id.
    #[schema(example = "01JB8V0K3M5P7R9T1W3Y5A7C9E")]
    pub plan_id: Option<String>,
    /// What the plan is for.
    #[schema(example = "Rate-limit the login form")]
    pub title: Option<String>,
    /// More about it, in Markdown.
    #[schema(example = "Brute-force attempts are showing up in the auth logs.")]
    pub description: Option<String>,
    /// The repository its tasks work in, `owner/name`.
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// The older status word, kept for older clients: `draft`, `executing`,
    /// or `done` once every task merged or was dropped. Read `state`.
    #[schema(example = "executing")]
    pub status: Option<String>,
    /// Where the plan stands, from its tasks' work: `drafting` (nothing
    /// approved yet), `running`, `needs_you` (a task asks a person
    /// something, failed, or drafts remain once the rest finished), `done`
    /// (every task merged or dropped) or `stalled` (open work and nothing
    /// moved for 3 days).
    #[schema(example = "running")]
    pub state: Option<String>,
    /// The count line: merged first, then the states with tasks in them.
    #[schema(example = "2 of 5 merged, 1 needs you, 1 in review, 1 working")]
    pub summary: Option<String>,
    /// How many tasks are in each state.
    #[schema(value_type = Option<TaskCounts>)]
    pub counts: Option<common::plan_state::Counts>,
    /// How many things wait on a person.
    #[schema(example = 1)]
    pub needs_count: u64,
    /// What its tasks' runs cost so far, in US dollars, every attempt counted.
    #[schema(example = 18.4)]
    pub cost_usd: Option<f64>,
    /// Runs its tasks took, every attempt counted.
    #[schema(example = 6)]
    pub attempts: Option<u64>,
    /// Success criteria the work proved, of `criteria_count`.
    #[schema(example = 3)]
    pub criteria_met: Option<u64>,
    /// How many success criteria it has.
    #[schema(example = 5)]
    pub criteria_count: Option<u64>,
    /// When anything last happened on its tasks (RFC 3339).
    #[schema(example = "2026-10-23T09:12:00+00:00")]
    pub last_activity_at: Option<String>,
    /// The plan's success criteria as written, when it has its own list.
    #[schema(example = json!(["The 11th login attempt within 15 minutes gets 429"]))]
    pub success_criteria: Option<Vec<String>>,
    /// How many tasks it has.
    #[schema(example = 3)]
    pub task_count: u64,
    /// Input tokens its runs used so far.
    #[schema(example = 412300)]
    pub tokens_in: Option<u64>,
    /// Output tokens its runs used so far.
    #[schema(example = 38120)]
    pub tokens_out: Option<u64>,
    /// `github` or `jira`: where its tasks become tickets.
    #[schema(example = "github")]
    pub destination: Option<String>,
    /// MCP servers used while planning.
    #[schema(example = json!(["docs-search"]))]
    pub mcp_servers: Option<Vec<String>>,
    /// The plan chat it came from.
    #[schema(example = "01JB8TZ6C8E0G2J4L6N8Q0S2U4")]
    pub chat_id: Option<String>,
    /// When it was made (RFC 3339).
    #[schema(example = "2026-10-22T13:40:00+00:00")]
    pub created_at: Option<String>,
    /// When it last changed (RFC 3339).
    #[schema(example = "2026-10-22T13:55:00+00:00")]
    pub updated_at: Option<String>,
    /// When it started running (RFC 3339).
    #[schema(example = "2026-10-22T13:55:00+00:00")]
    pub executed_at: Option<String>,
    /// Who started it running.
    #[schema(example = "octocat")]
    pub executed_by: Option<String>,
}

/// How many tasks are in each state (the shape of
/// `common::plan_state::Counts`, for the API document).
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
pub struct TaskCounts {
    /// Not approved yet.
    #[schema(example = 0)]
    draft: u64,
    /// Approved and waiting to start.
    #[schema(example = 1)]
    queued: u64,
    /// Its run is working.
    #[schema(example = 1)]
    working: u64,
    /// Its run asks a person for something.
    #[schema(example = 1)]
    needs_you: u64,
    /// Failed, with a reason.
    #[schema(example = 0)]
    failed: u64,
    /// A pull request is open.
    #[schema(example = 1)]
    in_review: u64,
    /// A pull request merged.
    #[schema(example = 2)]
    merged: u64,
    /// Dropped by a person.
    #[schema(example = 0)]
    dropped: u64,
}

/// Something a person has to do for the plan to go on (the shape of
/// `common::plan_state::NeedsItem`, for the API document).
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
pub struct NeedsYou {
    /// The task it is about; empty for the plan's drafts.
    #[schema(example = "01JB8V0P8Q2S4U6W8Y0A2C4E6G")]
    task_id: String,
    /// `answer`, `review`, `approval`, `checks`, `merge`, `failed`,
    /// `closed_pr` or `approve_rest`.
    #[schema(example = "approval")]
    kind: String,
    /// What to do, one line.
    #[schema(example = "Approve pull request #318")]
    ask: String,
    /// Why, or what is open.
    #[schema(
        example = "This repo's auto-merge setting requires an approving review from a person."
    )]
    detail: Option<String>,
    /// Where to do it.
    #[schema(example = "https://github.com/acme/web/pull/318")]
    url: Option<String>,
    /// The run it comes from.
    #[schema(example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")]
    run_id: Option<String>,
    /// The button: `answer`, `open_pr`, `open_link`, `retry` or `approve`.
    #[schema(example = "open_pr")]
    action: String,
}

/// What starting tasks, or retrying one, is expected to take, from the
/// team's recent finished runs (the shape of `common::plan_sync::Estimate`).
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
pub struct StartEstimate {
    /// Tasks it would start.
    #[schema(example = 3)]
    tasks: u64,
    /// Runs expected: one per task.
    #[schema(example = 3)]
    runs_low: u64,
    /// Runs expected with the attempts a ticket took lately.
    #[schema(example = 4)]
    runs_high: u64,
    /// The low end of the cost, in US dollars.
    #[schema(example = 6.3)]
    cost_low: f64,
    /// The high end of the cost, in US dollars.
    #[schema(example = 19.8)]
    cost_high: f64,
    /// One run's typical cost, low end (a Retry or Start now).
    #[schema(example = 2.1)]
    per_run_low: f64,
    /// One run's typical cost, high end.
    #[schema(example = 4.95)]
    per_run_high: f64,
    /// Finished runs the numbers come from; 0 means no history yet.
    #[schema(example = 40)]
    sample: u64,
}

/// A task in a plan.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct PlanTask {
    /// The task's id.
    #[schema(example = "01JB8V0P8Q2S4U6W8Y0A2C4E6G")]
    pub task_id: Option<String>,
    /// The plan it belongs to.
    #[schema(example = "01JB8V0K3M5P7R9T1W3Y5A7C9E")]
    pub plan_id: Option<String>,
    /// What to do.
    #[schema(example = "Limit login attempts per IP")]
    pub title: Option<String>,
    /// How, in Markdown.
    #[schema(example = "Allow 10 attempts per IP per 15 minutes; answer 429 after that.")]
    pub description: Option<String>,
    /// How to tell it is done.
    #[schema(example = "The 11th attempt within 15 minutes gets 429 and a Retry-After header.")]
    pub acceptance_criteria: Option<String>,
    /// The task row's own word: `draft`, `approved`, `rejected`, `queued`,
    /// `waiting` (on a task it depends on), `running` (filing its ticket),
    /// `filed` (older tasks say `done`) or `failed` (the ticket could not be
    /// filed). Filing the ticket finishes nothing: read `state`.
    #[schema(example = "filed")]
    pub status: Option<String>,
    /// Where the task stands, from its runs and pull requests: `draft`,
    /// `queued`, `working`, `needs_you`, `failed`, `in_review`, `merged` or
    /// `dropped`.
    #[schema(example = "in_review")]
    pub state: Option<String>,
    /// One line: what it is doing, what it waits on, or what blocks it.
    #[schema(example = "Checks are running on pull request #318")]
    pub state_detail: Option<String>,
    /// Why it failed (`failed` only).
    #[schema(example = json!(null))]
    pub state_reason: Option<String>,
    /// Runs it took, every attempt counted.
    #[schema(example = 2)]
    pub attempts: Option<u64>,
    /// What its runs cost, in US dollars.
    #[schema(example = 7.2)]
    pub cost_usd: Option<f64>,
    /// Every attempt, oldest first: `{run_id, status, created_at, cost_usd}`.
    #[schema(value_type = Option<Vec<Object>>, example = json!([{"run_id": "01JB8X6F4M2C9QZ3T7V5W1K0HA", "status": "completed", "created_at": "2026-10-22T14:02:11+00:00", "cost_usd": 3.6}]))]
    pub runs: Option<Vec<common::plan_state::RunLine>>,
    /// Its pull requests, newest attempt first: `{number, url, repo, state,
    /// checks, review, tone, additions, deletions, run_id}`. `tone` is the
    /// colour: `ready`, `waiting`, `failing`, `merged` or `closed`.
    #[schema(value_type = Option<Vec<Object>>, example = json!([{"number": 318, "url": "https://github.com/acme/web/pull/318", "repo": "acme/web", "state": "open", "checks": "running", "review": "none", "tone": "waiting", "additions": 120, "deletions": 8, "run_id": "01JB8X6F4M2C9QZ3T7V5W1K0HA"}]))]
    pub prs: Option<Vec<common::plan_state::PrChip>>,
    /// What it waits on a person for, when it does.
    #[schema(value_type = Option<NeedsYou>)]
    pub needs: Option<common::plan_state::NeedsItem>,
    /// Changes after the plan was approved, and work outside its scope:
    /// `{kind, detail, by, by_name, at}`, `kind` one of `added`, `changed`,
    /// `dropped`, `scope`; `by` is `agent` or `person`.
    #[schema(value_type = Option<Vec<Object>>, example = json!([{"kind": "added", "detail": "Added after the plan was approved", "by": "person", "by_name": "octocat", "at": "2026-10-22T15:00:00+00:00"}]))]
    pub drift: Option<Vec<common::plan_state::Drift>>,
    /// The plan's success criteria it covers (`C1`, ...).
    #[schema(example = json!(["C1"]))]
    pub covers: Option<Vec<String>>,
    /// Its ticket was filed.
    #[schema(example = true)]
    pub ticket_filed: Option<bool>,
    /// When anything last happened on it (RFC 3339).
    #[schema(example = "2026-10-23T09:12:00+00:00")]
    pub last_activity_at: Option<String>,
    /// Its repository, when not the plan's.
    #[schema(example = "acme/web")]
    pub repo: Option<String>,
    /// Its place in the plan, from 0.
    #[schema(example = 0)]
    pub order: u64,
    /// The GitHub issue it became.
    #[schema(example = 412)]
    pub issue_number: Option<u64>,
    /// That issue's address.
    #[schema(example = "https://github.com/acme/web/issues/412")]
    pub issue_url: Option<String>,
    /// Its newest run (every attempt is in `runs`).
    #[schema(example = "01JB8X6F4M2C9QZ3T7V5W1K0HA")]
    pub run_id: Option<String>,
    /// The newest pull request (every one is in `prs`).
    #[schema(example = "https://github.com/acme/web/pull/318")]
    pub pr_url: Option<String>,
    /// When it was approved (RFC 3339).
    #[schema(example = "2026-10-22T13:54:10+00:00")]
    pub approved_at: Option<String>,
    /// Who approved it.
    #[schema(example = "octocat")]
    pub approved_by: Option<String>,
    /// When it was rejected (RFC 3339).
    #[schema(example = json!(null))]
    pub rejected_at: Option<String>,
    /// Who rejected it.
    #[schema(example = json!(null))]
    pub rejected_by: Option<String>,
    /// `github` or `jira`.
    #[schema(example = "github")]
    pub destination: Option<String>,
    /// The Jira project it is filed in.
    #[schema(example = json!(null))]
    pub jira_project: Option<String>,
    /// The Jira ticket it became.
    #[schema(example = json!(null))]
    pub jira_ticket_key: Option<String>,
    /// That ticket's address.
    #[schema(example = json!(null))]
    pub jira_ticket_url: Option<String>,
    /// The task it waits for, when it depends on one.
    #[schema(example = json!(null))]
    pub depends_on: Option<String>,
    /// When it was made (RFC 3339).
    #[schema(example = "2026-10-22T13:40:00+00:00")]
    pub created_at: Option<String>,
}

type PlanItem = std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>;

fn item_str(item: &PlanItem, k: &str) -> Option<String> {
    item.get(k).and_then(|v| v.as_s().ok()).cloned()
}

fn item_u64(item: &PlanItem, k: &str) -> Option<u64> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
}

fn plan_of(item: &PlanItem) -> Plan {
    let mcp_servers: Vec<String> = item
        .get("mcp_servers")
        .and_then(|v| v.as_l().ok())
        .map(|list| list.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    let stored = common::plan_sync::stored(item);
    Plan {
        plan_id: item_str(item, "plan_id"),
        title: item_str(item, "title"),
        description: item_str(item, "description"),
        repo: item_str(item, "repo"),
        status: item_str(item, "status"),
        task_count: item_u64(item, "task_count").unwrap_or(0),
        tokens_in: item_u64(item, "total_tokens_in"),
        tokens_out: item_u64(item, "total_tokens_out"),
        destination: item_str(item, "destination"),
        state: stored.state.map(|s| s.code().to_string()),
        summary: Some(stored.summary.clone()).filter(|s| !s.is_empty()),
        counts: stored.state.map(|_| stored.counts.clone()),
        needs_count: stored.needs_count,
        cost_usd: stored.state.map(|_| round_cents(stored.cost_usd)),
        attempts: stored.state.map(|_| stored.attempts),
        criteria_met: stored.state.map(|_| stored.criteria_met),
        criteria_count: stored.state.map(|_| stored.criteria_count),
        last_activity_at: Some(stored.last_activity_at.clone()).filter(|s| !s.is_empty()),
        success_criteria: item
            .get("criteria")
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect()),
        mcp_servers: (!mcp_servers.is_empty()).then_some(mcp_servers),
        chat_id: item_str(item, "chat_id"),
        created_at: item_str(item, "created_at"),
        updated_at: item_str(item, "updated_at"),
        executed_at: item_str(item, "executed_at"),
        executed_by: item_str(item, "executed_by"),
    }
}

fn task_of(item: &PlanItem) -> PlanTask {
    PlanTask {
        task_id: item_str(item, "task_id"),
        plan_id: item_str(item, "plan_id"),
        title: item_str(item, "title"),
        description: item_str(item, "description"),
        acceptance_criteria: item_str(item, "acceptance_criteria"),
        status: item_str(item, "status"),
        state: item_str(item, "state"),
        state_detail: item_str(item, "state_detail"),
        state_reason: item_str(item, "state_reason")
            .filter(|s| !s.is_empty())
            .or_else(|| item_str(item, "failure_reason")),
        attempts: item_u64(item, "attempts"),
        cost_usd: item
            .get("cost_usd")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<f64>().ok())
            .map(round_cents),
        runs: None,
        prs: item_str(item, "prs").and_then(|s| serde_json::from_str(&s).ok()),
        needs: None,
        drift: None,
        covers: item
            .get("covers")
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect()),
        ticket_filed: item
            .get("ticket_filed")
            .and_then(|v| v.as_bool().ok())
            .copied(),
        last_activity_at: None,
        repo: item_str(item, "repo"),
        order: item_u64(item, "task_order").unwrap_or(0),
        issue_number: item_u64(item, "issue_number"),
        issue_url: item_str(item, "issue_url"),
        run_id: item_str(item, "run_id"),
        pr_url: item_str(item, "pr_url"),
        approved_at: item_str(item, "approved_at"),
        approved_by: item_str(item, "approved_by"),
        rejected_at: item_str(item, "rejected_at"),
        rejected_by: item_str(item, "rejected_by"),
        destination: item_str(item, "destination"),
        jira_project: item_str(item, "jira_project"),
        jira_ticket_key: item_str(item, "jira_ticket_key"),
        jira_ticket_url: item_str(item, "jira_ticket_url"),
        depends_on: item_str(item, "depends_on"),
        created_at: item_str(item, "created_at"),
    }
}

// ─── Plan Openspec ────────────────────────────────────────────────────

/// POST /api/plans/:plan_id/openspec/generate — generate openspec markdown files from plan data.
pub async fn generate_plan_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;

    // Fetch plan + tasks (same query as get_plan)
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan for openspec: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let mut plan_data: Option<Value> = None;
    let mut tasks: Vec<Value> = Vec::new();

    for item in items {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            tasks.push(task_from_item(item));
        } else {
            plan_data = Some(plan_from_item(item));
        }
    }

    let plan = plan_data.ok_or(StatusCode::NOT_FOUND)?;
    tasks.sort_by_key(|t| t["order"].as_u64().unwrap_or(0));

    let title = plan["title"].as_str().unwrap_or("Untitled Plan");
    let description = plan["description"].as_str().unwrap_or("");
    let repo = plan["repo"].as_str().unwrap_or("");
    let destination = plan["destination"].as_str().unwrap_or("github");
    let task_count = tasks.len();

    // Generate proposal.md
    let mut proposal = format!(
        "# Proposal: {title}\n\n## Summary\n{description}\n\n## Scope\n- Repository: {repo}\n- Tasks: {task_count}\n- Destination: {destination}\n\n## Task Breakdown\n"
    );
    for t in &tasks {
        let t_title = t["title"].as_str().unwrap_or("");
        let t_desc = t["description"].as_str().unwrap_or("");
        proposal.push_str(&format!("### {t_title}\n{t_desc}\n\n"));
    }

    // Generate design.md
    let mut design = format!(
        "# Technical Design: {title}\n\n## Overview\n{description}\n\n## Changes by Repository\n"
    );
    let mut repos_map: std::collections::BTreeMap<String, Vec<&Value>> =
        std::collections::BTreeMap::new();
    for t in &tasks {
        let t_repo = t["repo"].as_str().unwrap_or(repo).to_string();
        repos_map.entry(t_repo).or_default().push(t);
    }
    for (r, repo_tasks) in &repos_map {
        let r_label = if r.is_empty() {
            "(default)"
        } else {
            r.as_str()
        };
        design.push_str(&format!("### {r_label}\n"));
        for t in repo_tasks {
            let t_title = t["title"].as_str().unwrap_or("");
            design.push_str(&format!("- {t_title}\n"));
        }
        design.push('\n');
    }
    design.push_str("## Task Dependencies\n");
    let mut has_deps = false;
    for t in &tasks {
        if let Some(dep) = t["depends_on"].as_str().filter(|s| !s.is_empty()) {
            let t_title = t["title"].as_str().unwrap_or("");
            let dep_title = tasks
                .iter()
                .find(|d| d["task_id"].as_str() == Some(dep))
                .and_then(|d| d["title"].as_str())
                .unwrap_or(dep);
            design.push_str(&format!("- **{t_title}** depends on **{dep_title}**\n"));
            has_deps = true;
        }
    }
    if !has_deps {
        design.push_str("No explicit task dependencies.\n");
    }

    // Generate tasks.md
    let mut tasks_md = format!("# Task Checklist: {title}\n\n");
    for t in &tasks {
        let t_title = t["title"].as_str().unwrap_or("");
        let t_repo = t["repo"].as_str().unwrap_or(repo);
        let t_desc = t["description"].as_str().unwrap_or("");
        let t_ac = t["acceptance_criteria"].as_str().unwrap_or("");
        tasks_md.push_str(&format!("- [ ] **{t_title}** ({t_repo})\n"));
        if !t_desc.is_empty() {
            tasks_md.push_str(&format!("  {t_desc}\n"));
        }
        if !t_ac.is_empty() {
            tasks_md.push_str(&format!("  Acceptance: {t_ac}\n"));
        }
        tasks_md.push('\n');
    }

    // Generate spec.md
    let mut spec = format!("# Acceptance Specification: {title}\n\n");
    for t in &tasks {
        let t_ac = t["acceptance_criteria"].as_str().unwrap_or("");
        if !t_ac.is_empty() {
            let t_title = t["title"].as_str().unwrap_or("");
            spec.push_str(&format!("## {t_title}\n\n### Criteria\n{t_ac}\n\n"));
        }
    }

    // Store in S3
    let prefix = format!("teams/{}/plans/{plan_id}/openspec", claims.team_id);
    let bucket = &state.config.bucket_name;
    let files = [
        ("proposal.md", proposal),
        ("design.md", design),
        ("tasks.md", tasks_md),
        ("spec.md", spec),
    ];

    for (name, content) in &files {
        let key = format!("{prefix}/{name}");
        state
            .s3
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(content.clone().into_bytes().into())
            .content_type("text/markdown")
            .send()
            .await
            .map_err(|e| {
                error!("Failed to upload openspec {name}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    // Update plan with openspec_generated_at timestamp
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("PLAN#{plan_id}")))
        .update_expression("SET openspec_generated_at = :ts")
        .expression_attribute_values(":ts", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            warn!("Failed to update openspec_generated_at: {e}");
        });

    info!(plan_id = %plan_id, team_id = %claims.team_id, "Openspec generated for plan");

    Ok(Json(json!({
        "files": ["proposal.md", "design.md", "tasks.md", "spec.md"]
    })))
}

/// GET /api/plans/:plan_id/openspec — fetch the four openspec files from S3.
pub async fn get_plan_openspec(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_plan_id(&plan_id)?;

    let prefix = format!("teams/{}/plans/{plan_id}/openspec", claims.team_id);
    let bucket = &state.config.bucket_name;

    let mut files = serde_json::Map::new();
    for name in &["proposal.md", "design.md", "tasks.md", "spec.md"] {
        let key = format!("{prefix}/{name}");
        if let Ok(output) = state.s3.get_object().bucket(bucket).key(&key).send().await {
            if let Ok(bytes) = output.body.collect().await {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    files.insert(name.replace(".md", ""), Value::String(text));
                }
            }
        }
    }

    if files.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(Value::Object(files)))
}

// ─── Plan Templates ───────────────────────────────────────────────────

fn validate_template_id(id: &str) -> Result<(), StatusCode> {
    if id.is_empty() || id.len() > 30 || !id.chars().all(|c| c.is_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn template_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    let tags: Value = item
        .get("tags")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    let task_count = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Vec<Value>>(s).ok())
        .map(|v| v.len())
        .unwrap_or(0);

    json!({
        "template_id": item.get("template_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "category": item.get("category").and_then(|v| v.as_s().ok()),
        "tags": tags,
        "task_count": task_count,
        "usage_count": item.get("usage_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "created_by": item.get("created_by").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
    })
}

fn template_full_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    let tags: Value = item
        .get("tags")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    let task_templates: Value = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!([]));

    json!({
        "template_id": item.get("template_id").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "description": item.get("description").and_then(|v| v.as_s().ok()),
        "category": item.get("category").and_then(|v| v.as_s().ok()),
        "tags": tags,
        "task_templates": task_templates,
        "usage_count": item.get("usage_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "created_by": item.get("created_by").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
    })
}

/// GET /api/plans/templates — list templates for the team (paginated).
pub async fn list_templates(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let limit: i32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .min(100);

    let mut query = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("TEMPLATE#"))
        .scan_index_forward(false)
        .limit(limit);

    if let Some(cursor) = params.get("cursor").filter(|c| !c.is_empty()) {
        let lek = std::collections::HashMap::from([
            ("pk".to_string(), attr_s(&claims.team_id)),
            ("sk".to_string(), attr_s(cursor)),
        ]);
        query = query.set_exclusive_start_key(Some(lek));
    }

    let result = query.send().await.map_err(|e| {
        error!("Failed to query templates: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let templates: Vec<Value> = result.items().iter().map(template_from_item).collect();

    let next_cursor = result
        .last_evaluated_key()
        .and_then(|k| k.get("sk"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());

    Ok(Json(json!({
        "templates": templates,
        "next_cursor": next_cursor,
    })))
}

/// POST /api/plans/templates — create a new template.
pub async fn create_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    let title = body["title"].as_str().ok_or(StatusCode::BAD_REQUEST)?;
    if title.is_empty() || title.len() > 200 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let description = body["description"].as_str().unwrap_or("");
    let category = body["category"].as_str().unwrap_or("");

    let tags_json = body["tags"]
        .as_array()
        .map(|arr| {
            let strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            serde_json::to_string(&strs).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|| "[]".to_string());

    let task_templates_json = body["task_templates"]
        .as_array()
        .map(|arr| serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string()))
        .unwrap_or_else(|| "[]".to_string());

    let template_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("TEMPLATE#{template_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("template_id", attr_s(&template_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("category", attr_s(category))
        .item("tags", attr_s(&tags_json))
        .item("task_templates", attr_s(&task_templates_json))
        .item("usage_count", attr_n(0))
        .item("created_by", attr_s(&claims.display_name()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "template_id": template_id })))
}

/// POST /api/plans/templates/from-plan/:plan_id — create a template from an existing plan.
pub async fn create_template_from_plan(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plan_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_plan_id(&plan_id)?;

    // Fetch the plan + tasks
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s(&format!("PLAN#{plan_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query plan for template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let items = result.items();
    if items.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let mut plan_title = String::new();
    let mut plan_description = String::new();
    let mut task_templates: Vec<Value> = Vec::new();

    for item in items {
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        if sk.contains("#TASK#") {
            let task = json!({
                "title": item.get("title").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "description": item.get("description").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "acceptance_criteria": item.get("acceptance_criteria").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "repo": item.get("repo").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or(""),
                "order": item.get("task_order").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
            });
            task_templates.push(task);
        } else {
            plan_title = item
                .get("title")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
            plan_description = item
                .get("description")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
        }
    }

    // Sort tasks by order
    task_templates.sort_by_key(|t| t["order"].as_u64().unwrap_or(0));

    // Allow body overrides
    let title = body["title"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or(plan_title);
    if title.is_empty() || title.len() > 200 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let description = body["description"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or(plan_description);
    let category = body["category"].as_str().unwrap_or("");

    let tags_json = body["tags"]
        .as_array()
        .map(|arr| {
            let strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            serde_json::to_string(&strs).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|| "[]".to_string());

    let task_templates_json =
        serde_json::to_string(&task_templates).unwrap_or_else(|_| "[]".to_string());

    let template_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let sk = format!("TEMPLATE#{template_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item("template_id", attr_s(&template_id))
        .item("title", attr_s(&title))
        .item("description", attr_s(&description))
        .item("category", attr_s(category))
        .item("tags", attr_s(&tags_json))
        .item("task_templates", attr_s(&task_templates_json))
        .item("usage_count", attr_n(0))
        .item("created_by", attr_s(&claims.display_name()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create template from plan: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "template_id": template_id })))
}

/// GET /api/plans/templates/:template_id — get full template including task_templates.
pub async fn get_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_template_id(&template_id)?;

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(template_full_from_item(item)))
}

/// DELETE /api/plans/templates/:template_id — delete a template.
pub async fn delete_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_template_id(&template_id)?;

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

/// POST /api/plans/templates/:template_id/use — create a plan from a template.
pub async fn use_template(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(template_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    validate_template_id(&template_id)?;

    // Fetch the template
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to get template for use: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    let tmpl_title = item
        .get("title")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let tmpl_description = item
        .get("description")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();

    let task_templates: Vec<Value> = item
        .get("task_templates")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    // Allow body overrides
    let title = body["title"].as_str().unwrap_or(&tmpl_title);
    let description = body["description"].as_str().unwrap_or(&tmpl_description);
    let repo = body["repo"].as_str().unwrap_or("");
    let destination = match body["destination"].as_str() {
        Some("jira") => "jira",
        _ => "github",
    };

    // Create the plan
    let plan_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let plan_sk = format!("PLAN#{plan_id}");

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&plan_sk))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(title))
        .item("description", attr_s(description))
        .item("repo", attr_s(repo))
        .item("destination", attr_s(destination))
        .item("status", attr_s("draft"))
        .item("task_count", attr_n(task_templates.len()))
        .item("created_at", attr_s(&now))
        .item("updated_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create plan from template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Create tasks from task_templates
    for (i, task) in task_templates.iter().enumerate() {
        let task_title = task["title"].as_str().unwrap_or("Untitled task");
        let task_desc = task["description"].as_str().unwrap_or("");
        let task_criteria = task["acceptance_criteria"].as_str().unwrap_or("");
        let task_repo = task["repo"].as_str().unwrap_or(repo);
        let order = task["order"].as_u64().unwrap_or(i as u64);
        let task_id = ulid::Ulid::new().to_string();
        let task_sk = format!("PLAN#{plan_id}#TASK#{task_id}");

        let mut put = state
            .dynamo
            .put_item()
            .table_name(&state.config.plans_table_name)
            .item("pk", attr_s(&claims.team_id))
            .item("sk", attr_s(&task_sk))
            .item("plan_id", attr_s(&plan_id))
            .item("task_id", attr_s(&task_id))
            .item("title", attr_s(task_title))
            .item("description", attr_s(task_desc))
            .item("acceptance_criteria", attr_s(task_criteria))
            .item("status", attr_s("draft"))
            .item("task_order", attr_n(order))
            .item("created_at", attr_s(&now));

        if !task_repo.is_empty() {
            put = put.item("repo", attr_s(task_repo));
        }
        if !destination.is_empty() {
            put = put.item("destination", attr_s(destination));
        }

        put.send().await.map_err(|e| {
            error!("Failed to create task from template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    // Increment usage_count on the template
    state
        .dynamo
        .update_item()
        .table_name(&state.config.plans_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TEMPLATE#{template_id}")))
        .update_expression("ADD usage_count :one")
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await
        .ok();

    // Track plan usage
    track_plan_usage(&state, &claims.team_id).await;

    Ok(Json(json!({ "plan_id": plan_id })))
}

// ─── Plan Chat ────────────────────────────────────────────────────────

const PLAN_CHAT_SYSTEM: &str = r#"You are a planning assistant for Coderhelm, an autonomous AI coding agent.
Your job is to help the user break down a feature, epic, or large piece of work into a structured plan.

When the user describes what they want to build, you should:
1. If the user already specified the repo, tech constraints, and scope — go straight to generating the plan.
2. Only ask clarifying questions when truly critical info is missing (e.g. no repo specified at all).
3. Never ask more than 2 clarifying questions before generating a plan.

CRITICAL — Conversation continuity:
- When the user adds new requirements or follow-up messages, incorporate ALL previous requests into the updated plan.
- Never forget or drop earlier requests. The plan should be the complete, cumulative result of everything discussed.

When generating the final plan, output it in this EXACT JSON format inside a code fence:

```json
{
  "title": "Short epic title",
  "description": "1-2 sentence overview",
  "repo": "owner/repo",
  "tasks": [
    {
      "title": "Concise task title",
      "description": "What to build and why. Be specific about files, APIs, UI components.",
      "acceptance_criteria": "- Bullet list\n- Of verifiable criteria",
      "repo": "owner/repo",
      "order": 0
    }
  ]
}
```

Rules:
- A plan can span MULTIPLE repositories. The top-level "repo" is the primary repo. Each task has an optional "repo" field for targeting a different repo.
- Tasks should be independently implementable (one PR each)
- Order matters — Coderhelm works on them sequentially
- Each task title should be a GitHub issue title (imperative, max 60 chars)
- Acceptance criteria should be machine-verifiable where possible
- 2-5 tasks is ideal. Fewer, bigger tasks are better than many small ones.
- Combine related work (e.g. code changes + README update + tests) into a single task
- Only split into separate tasks when there are truly independent streams of work
- If the user mentions a specific repo, USE IT — don't ask again
- Be direct and action-oriented, not verbose

Note: Each task will go through Coderhelm's full pipeline: OpenSpec generation
(proposal, design, tasks, spec documents), then implementation. Plan accordingly —
each task becomes a full engineering effort with its own spec and PR."#;

/// POST /api/plans/chat/token — Issue a short-lived JWT for the streaming endpoint.
/// The frontend calls this before opening the SSE stream to stream.coderhelm.com.
pub async fn stream_token(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
                             // Mint a short-lived token (5 minutes) scoped for streaming
                             // The record's role, never the role this request runs as: the token
                             // must not carry more than the member's own role.
                             // Scoped to the plan chat stream: it is never a session, and a session
                             // is never a stream token.
    let token = crate::auth::jwt::create_scoped_token(
        &claims,
        crate::auth::jwt::PLAN_CHAT_AUDIENCE,
        &state.secrets.jwt_secret,
        300, // 5 minutes
    )
    .map_err(|e| {
        error!("Failed to create stream token: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(json!({ "token": token })))
}

/// Strip ```json...``` code blocks from a chat message, keeping surrounding text.
/// Used to trim stale plan JSONs from older assistant messages.
fn strip_plan_json(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    let mut pos = 0;
    while let Some(start) = content[pos..].find("```json") {
        let abs_start = pos + start;
        result.push_str(&content[pos..abs_start]);
        if let Some(end) = content[abs_start + 7..].find("```") {
            let abs_end = abs_start + 7 + end + 3;
            result.push_str("[plan updated]");
            pos = abs_end;
        } else {
            // Unclosed fence — keep as-is
            result.push_str(&content[abs_start..]);
            return result;
        }
    }
    result.push_str(&content[pos..]);
    result
}

/// Shared context for plan chat — loaded once, used by both streaming and non-streaming.
struct PlanChatContext {
    system_prompt: String,
    messages: Vec<Value>,
    tools: Vec<Value>,
    /// GitHub installation token for codebase tools (None if not available)
    github_token: Option<String>,
    /// Repo name → default branch mapping for codebase tools
    repo_branches: std::collections::HashMap<String, String>,
    /// The team's skills, agents and data source tools, run in the worker.
    worker: super::plan_chat_tools::WorkerTools,
    scope: super::plan_chat_tools::ChatScope,
}

/// Load all context needed for plan chat (DynamoDB, S3, prompt assembly, message conversion).
/// Shared between `plan_chat` and `plan_chat_stream` to eliminate duplication.
async fn load_plan_chat_context(
    state: &AppState,
    team_id: &str,
    user_email: &str,
    messages_input: &[Value],
    body: &Value,
) -> Result<PlanChatContext, StatusCode> {
    // Load org context, repo list, log analyzer flag, enabled plugins, templates, and team META in parallel
    let (
        org_context_result,
        repo_list_result,
        allow_plan_log_analyzer,
        enabled_plugins,
        tmpl_result,
        team_meta_result,
    ) = tokio::join!(
        state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s("AGENTS#GLOBAL"))
            .send(),
        state
            .dynamo
            .query()
            .table_name(&state.config.repos_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("REPO#"))
            .limit(50)
            .send(),
        load_allow_plan_log_analyzer(state, team_id),
        load_enabled_plugins(state, team_id),
        state
            .dynamo
            .query()
            .table_name(&state.config.plans_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":prefix", attr_s("TEMPLATE#"))
            .limit(10)
            .send(),
        // Load team META for github_installation_id
        state
            .dynamo
            .get_item()
            .table_name(&state.config.teams_table_name)
            .key("team_id", attr_s(team_id))
            .key("sk", attr_s("META"))
            .projection_expression("github_installation_id")
            .send(),
    );

    let org_context = org_context_result
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("content").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    let mut repo_branches: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let repo_list: Vec<String> = repo_list_result
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| {
                    let name = item
                        .get("repo_name")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| s.to_string())?;
                    let branch = item
                        .get("default_branch")
                        .and_then(|v| v.as_s().ok())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "main".to_string());
                    repo_branches.insert(name.clone(), branch);
                    Some(name)
                })
                .collect()
        })
        .unwrap_or_default();

    // The chat's repository (the one picked, else the first), for the team's
    // skills, agents and data sources run in the worker.
    let chat_repo = body["repo"]
        .as_str()
        .filter(|r| repo_list.iter().any(|x| x == r))
        .map(str::to_string)
        .or_else(|| repo_list.first().cloned())
        .unwrap_or_default();
    let scope = super::plan_chat_tools::ChatScope {
        team_id: team_id.to_string(),
        chat_id: super::plan_chat_tools::chat_id(body["chat_id"].as_str()),
        branch: repo_branches.get(&chat_repo).cloned().unwrap_or_default(),
        repo: chat_repo,
        first: messages_input.len() <= 1,
        brain: false,
        mcp: false,
    };
    let worker = super::plan_chat_tools::setup(state, &scope).await;

    // Get GitHub installation token for codebase tools
    let installation_id_opt = team_meta_result
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| {
            i.get("github_installation_id")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
        })
        .filter(|&id| id > 0);

    info!(
        team_id = team_id,
        installation_id = ?installation_id_opt,
        repos = repo_list.len(),
        "plan chat: loading github token"
    );

    let github_token = if let Some(installation_id) = installation_id_opt {
        match crate::auth::github_app::get_installation_token(state, installation_id).await {
            Ok(token) => {
                info!(team_id = team_id, "plan chat: github token acquired");
                Some(token)
            }
            Err(e) => {
                warn!(
                    team_id = team_id,
                    "Failed to get GitHub installation token for plan chat: {e}"
                );
                None
            }
        }
    } else {
        warn!(
            team_id = team_id,
            "No github_installation_id found for plan chat"
        );
        None
    };

    let template_lines: Vec<String> = tmpl_result
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter_map(|item| {
                    let title = item.get("title")?.as_s().ok()?;
                    let desc = item
                        .get("description")
                        .and_then(|v| v.as_s().ok())
                        .unwrap_or(&String::new())
                        .clone();
                    Some(format!("- {title}: {desc}"))
                })
                .collect()
        })
        .unwrap_or_default();

    // Conditionally load log analyzer context
    let log_analyzer_context = if allow_plan_log_analyzer {
        Some(load_log_analyzer_context(state, team_id).await)
    } else {
        None
    };

    // Load MCP tool definitions from S3 cache (always load — let the model decide when to use them)
    let mut mcp_tools: Vec<McpToolDef> = Vec::new();
    let mcp_proxy_fn = &state.config.mcp_proxy_function_name;
    if !mcp_proxy_fn.is_empty() {
        let cache_futures: Vec<_> = enabled_plugins
            .iter()
            .filter(|(_, has_creds, _)| *has_creds)
            .map(|(server_id, _, _)| {
                let s3 = &state.s3;
                let bucket = &state.config.bucket_name;
                async move {
                    let cache = load_mcp_tool_cache(s3, bucket, server_id).await;
                    (server_id.clone(), cache)
                }
            })
            .collect();
        let caches = futures::future::join_all(cache_futures).await;
        for (server_id, cache) in caches {
            if let Some(tools) = cache {
                for tool in tools {
                    mcp_tools.push(McpToolDef {
                        server_id: server_id.clone(),
                        name: tool["name"].as_str().unwrap_or("").to_string(),
                        description: tool["description"].as_str().unwrap_or("").to_string(),
                        input_schema: tool["inputSchema"].clone(),
                    });
                }
            }
        }
    }

    // Remote MCP servers this person may use (the team's shared ones, and
    // their own personal connections): only allowed, approved tools. A
    // remote connection takes the place of the same catalog server's key.
    let remote = super::mcp_servers::runtime(state, team_id)
        .usable(Some(user_email))
        .await;
    mcp_tools.retain(|t| !remote.iter().any(|(r, _)| r.id == t.server_id));
    let mut remote_lines = Vec::new();
    for (r, _) in &remote {
        for (name, description, input_schema) in common::mcp_remote::tools::agent_tools(r, false) {
            let tool = name
                .split_once("__")
                .map(|(_, t)| t.to_string())
                .unwrap_or_default();
            mcp_tools.push(McpToolDef {
                server_id: r.id.clone(),
                name: tool,
                description,
                input_schema,
            });
        }
        let mut line = format!("- {} ({}, connected)", r.id, r.name);
        if !r.custom_prompt.trim().is_empty() {
            line.push_str(&format!(" — {}", r.custom_prompt.trim()));
        }
        remote_lines.push(line);
    }

    // Build system prompt
    let mut system_prompt = PLAN_CHAT_SYSTEM.to_string();
    if !org_context.is_empty() {
        // Cap org_context to avoid prompt bloat
        let capped = if org_context.len() > 2000 {
            &org_context[..org_context[..2000].rfind('\n').unwrap_or(2000)]
        } else {
            &org_context
        };
        system_prompt.push_str(&format!("\n\nThe user's organization context:\n{capped}"));
    }
    if !repo_list.is_empty() {
        system_prompt.push_str(&format!(
            "\n\nAvailable repositories (use these exact names for the \"repo\" field):\n{}\n\nFor cross-repo changes, set the \"repo\" field on each task to the appropriate repository.",
            repo_list
                .iter()
                .map(|r| format!("- {r}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    if !mcp_tools.is_empty() {
        let mut plugin_lines: Vec<String> = enabled_plugins
            .iter()
            .filter(|(id, has_creds, _)| *has_creds && !remote.iter().any(|(r, _)| &r.id == id))
            .map(|(id, _, custom_prompt)| {
                let mut line = format!("- {id} (connected)");
                if let Some(prompt) = custom_prompt {
                    line.push_str(&format!(" — {prompt}"));
                }
                line
            })
            .collect();
        plugin_lines.extend(remote_lines);
        system_prompt.push_str(&format!(
            "\n\nYou have tool-call access to the following MCP servers.\n\
             CRITICAL tool-use rules for MCP servers:\n\
             - ONLY call MCP tools when the user EXPLICITLY asks for data from that system \
             (e.g. \"pull the tasks from Notion\", \"check Sentry for errors\").\n\
             - NEVER call MCP tools on greetings, general planning questions, or messages that \
             don't specifically mention an external system by name.\n\
             - NEVER call MCP tools speculatively or \"just in case\" there might be relevant data.\n\
             - If a tool returns no results, do NOT retry — tell the user and continue planning.\n\
             \n\
             Connected servers:\n{}",
            plugin_lines.join("\n")
        ));
    }
    if github_token.is_some() && !repo_list.is_empty() {
        system_prompt.push_str(
            "\n\nYou have direct access to the team's GitHub repositories via built-in tools:\n\
             - search_code: Search for code patterns (functions, imports, error handlers, etc.)\n\
             - read_file: Read specific files\n\
             - list_directory: Browse repo structure\n\n\
             USE THESE TOOLS when the user asks about code, architecture, or when you need to \
             understand the codebase to create a better plan. Don't ask the user to paste code — \
             look it up yourself. When scoping work, search the codebase to understand what exists \
             before breaking it into tasks.",
        );
    }
    system_prompt.push_str(&super::plan_chat_tools::prompt(&worker));
    if let Some(context) = log_analyzer_context.filter(|c| !c.is_empty()) {
        system_prompt.push_str(&format!(
            "\n\nAWS Log Analyzer context (enabled in workflow settings):\n{context}"
        ));
    }
    if !template_lines.is_empty() {
        system_prompt.push_str("\n\nThe team has these plan templates available:\n");
        system_prompt.push_str(&template_lines.join("\n"));
        system_prompt.push_str("\nIf the user's request matches a template pattern, mention it and ask if they'd like to use it as a starting point.");
    }

    // Convert messages to Anthropic format with history trimming:
    // Strip plan JSON blocks from older assistant messages to reduce token waste.
    let mut api_messages: Vec<Value> = Vec::new();
    let msg_count = messages_input.len();
    for (i, msg) in messages_input.iter().enumerate() {
        let role_str = msg["role"].as_str().unwrap_or("user");
        let raw_content = msg["content"].as_str().unwrap_or("");
        if raw_content.is_empty() {
            continue;
        }
        // Trim plan JSON from older assistant messages (keep the latest intact)
        let content = if role_str == "assistant" && i < msg_count.saturating_sub(1) {
            strip_plan_json(raw_content)
        } else {
            raw_content.to_string()
        };
        api_messages.push(json!({
            "role": role_str,
            "content": [{"type": "text", "text": content}]
        }));
    }

    // Build Anthropic tools from MCP tools + built-in GitHub codebase tools
    let mut tools: Vec<Value> = mcp_tools
        .iter()
        .map(|t| {
            json!({
                "name": format!("{}__{}", t.server_id, t.name),
                "description": t.description,
                "input_schema": t.input_schema
            })
        })
        .collect();

    // Add built-in GitHub codebase tools if we have a valid token
    if github_token.is_some() && !repo_list.is_empty() {
        let repo_enum: Vec<Value> = repo_list.iter().map(|r| json!(r)).collect();
        tools.push(json!({
            "name": "github__search_code",
            "description": "Search for code patterns across a repository. Returns matching file paths and line content. Use this to find where functions, classes, imports, or patterns are used.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string", "description": "Repository (owner/name)", "enum": repo_enum },
                    "query": { "type": "string", "description": "Search query (e.g. 'NewRelic.recordError', 'catch (error)', 'import { Something }')" }
                },
                "required": ["repo", "query"]
            }
        }));
        tools.push(json!({
            "name": "github__read_file",
            "description": "Read a file from the repository. Returns the full file content. Use for reading specific files you've found via search or know the path of.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string", "description": "Repository (owner/name)", "enum": repo_enum },
                    "path": { "type": "string", "description": "File path (e.g. 'src/utils/errors.ts')" }
                },
                "required": ["repo", "path"]
            }
        }));
        tools.push(json!({
            "name": "github__list_directory",
            "description": "List files and directories at a path in the repository. Use to explore repo structure.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "string", "description": "Repository (owner/name)", "enum": repo_enum },
                    "path": { "type": "string", "description": "Directory path (e.g. 'src/services' or '' for root)", "default": "" }
                },
                "required": ["repo"]
            }
        }));
    }

    tools.extend(worker.tools.iter().cloned());

    // Add cache_control to last tool for prompt caching
    if let Some(last) = tools.last_mut() {
        last["cache_control"] = json!({"type": "ephemeral"});
    }

    info!(
        team_id = team_id,
        tools_count = tools.len(),
        mcp_plugins = enabled_plugins.len(),
        messages_count = api_messages.len(),
        repos = repo_list.len(),
        has_github_token = github_token.is_some(),
        "load_plan_chat_context: context loaded"
    );

    Ok(PlanChatContext {
        system_prompt,
        messages: api_messages,
        tools,
        github_token,
        repo_branches,
        worker,
        scope,
    })
}

/// POST /api/plans/chat — AI-powered plan generation chat.
pub async fn plan_chat(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ (viewers are read-only)
    let messages_input = body["messages"].as_array().ok_or(StatusCode::BAD_REQUEST)?;

    if messages_input.is_empty() || messages_input.len() > 20 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let ctx = load_plan_chat_context(
        &state,
        &claims.team_id,
        &claims.email,
        messages_input,
        &body,
    )
    .await?;
    let mut api_messages = ctx.messages;
    // Agent permissions: every call is asked of the gate; a connection the
    // person can't use yet is offered as a sign-in card.
    let mut ctx_tools = ctx.tools.clone();
    let missing =
        super::chat_policy::missing_connections(&state, &claims.team_id, &claims.email).await;
    if let Some(t) = super::chat_policy::sign_in_tool(&missing) {
        ctx_tools.insert(0, t);
    }
    let mut gate = super::chat_policy::gate(
        &state,
        &claims.team_id,
        &claims.email,
        &ctx.scope.chat_id,
        &ctx.scope.repo,
        &ctx_tools,
    );
    super::chat_policy::offer(&mut gate, &mut ctx_tools).await;
    let thread = gate.ctx.thread_id.clone();
    super::chat_policy::settle_connected(&state, &claims.team_id, &thread, &claims.email).await;
    let is_admin = claims.holder().holds("integrations");
    let mut cards: Vec<Value> = Vec::new();

    // Load team's Anthropic API key
    let provider_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    // On Bedrock the deployment's own role calls the model: no team key.
    let api_key = provider_result
        .as_ref()
        .and_then(|item| item.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .or_else(|| common::model_provider::is_bedrock().then(String::new))
        .ok_or_else(|| {
            error!("No Anthropic API key configured for team");
            StatusCode::BAD_REQUEST
        })?;

    let model_id = provider_result
        .as_ref()
        .and_then(|item| {
            // Prefer heavy_model for plan chat (better reasoning for planning)
            item.get("heavy_model")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .or_else(|| item.get("primary_model").and_then(|v| v.as_s().ok()))
        })
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-5-5".to_string());

    // Agentic loop — up to 5 tool-use turns for plan chat (gateway has 30s timeout)
    let max_turns = 5;
    let mut mcp_servers_used: std::collections::HashSet<String> = std::collections::HashSet::new();
    let http = reqwest::Client::new();
    let meter = crate::model_call::chat_meter(&state, &claims.team_id);
    let mut worker_used = super::plan_chat_tools::Used::default();
    let mut used_items: Vec<Value> = Vec::new();

    for turn in 0..max_turns {
        let mut body = json!({
            "model": model_id,
            "max_tokens": 8192,
            "system": [{"type": "text", "text": ctx.system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": api_messages,
        });
        // No sampling params: current models reject non-default temperature.
        // Always-thinking models: explicit effort (defaults differ per model).
        if common::is_always_thinking_model(&model_id) {
            body["output_config"] = json!({"effort": "medium"});
        }
        if !ctx_tools.is_empty() {
            body["tools"] = json!(ctx_tools);
        }

        // Transient failures (5xx incl. 520-524, 408, 429, 529, no response,
        // a reply cut off) are retried; a person is waiting, so the waits are
        // short. Every attempt is counted as it happens, broken ones too.
        let response = crate::model_call::chat_turn(&http, &api_key, &body, &meter, &model_id)
            .await
            .map_err(|e| {
                error!(status = ?e.status(), transient = e.is_transient(), "Anthropic request failed: {e}");
                StatusCode::BAD_GATEWAY
            })?;

        let content = response["content"].as_array().cloned().unwrap_or_default();
        let stop_reason = response["stop_reason"].as_str().unwrap_or("");

        // Add assistant message to history
        api_messages.push(json!({"role": "assistant", "content": content}));

        // Extract tool uses
        let tool_uses: Vec<_> = content
            .iter()
            .filter(|b| b["type"].as_str() == Some("tool_use"))
            .cloned()
            .collect();

        if tool_uses.is_empty() || stop_reason == "end_turn" {
            // Extract final text and return
            let text = content
                .iter()
                .filter_map(|b| {
                    if b["type"].as_str() == Some("text") {
                        b["text"].as_str().map(String::from)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("");

            let servers: Vec<&str> = mcp_servers_used.iter().map(|s| s.as_str()).collect();
            return Ok(Json(
                json!({ "content": text, "mcp_servers": servers, "used": used_items, "cards": cards }),
            ));
        }

        // Every call is asked of the policy gate first. Calls it doesn't let
        // run get its answer instead of a result (and a card when it asks).
        let mut worker_results: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut allowed: Vec<Value> = Vec::new();
        for tu in &tool_uses {
            let name = tu["name"].as_str().unwrap_or("");
            let id = tu["id"].as_str().unwrap_or("").to_string();
            if name == super::chat_policy::SIGN_IN_TOOL {
                let (text, event) = super::chat_policy::sign_in(
                    &state,
                    &claims.team_id,
                    &thread,
                    &missing,
                    is_admin,
                    &tu["input"],
                )
                .await;
                if let Some((kind, v)) = event {
                    cards.push(json!({ "kind": kind, "data": v }));
                }
                worker_results.insert(id, text);
                continue;
            }
            match super::chat_policy::check(&gate, name, &tu["input"]).await {
                super::chat_policy::Checked::Run => allowed.push(tu.clone()),
                super::chat_policy::Checked::Answer { text, event } => {
                    if let Some((kind, v)) = event {
                        cards.push(json!({ "kind": kind, "data": v }));
                    }
                    worker_results.insert(id, text);
                }
                super::chat_policy::Checked::Stop(note) => {
                    return Ok(Json(
                        json!({ "content": note, "mcp_servers": [], "used": used_items, "cards": cards }),
                    ));
                }
            }
        }
        let tool_uses = allowed;

        // The team's skills, agents and data sources run in the worker, one
        // at a time so their limits hold.
        for tu in &tool_uses {
            let name = tu["name"].as_str().unwrap_or("");
            if ctx.worker.names.contains(name) {
                let (text, used) = super::plan_chat_tools::call(
                    &state,
                    &ctx.scope,
                    &mut worker_used,
                    name,
                    &tu["input"],
                )
                .await;
                if let Some(u) = used {
                    used_items.push(u);
                }
                worker_results.insert(tu["id"].as_str().unwrap_or("").to_string(), text);
            }
        }
        let tool_uses: Vec<Value> = tool_uses
            .into_iter()
            .filter(|tu| !ctx.worker.names.contains(tu["name"].as_str().unwrap_or("")))
            .collect();

        // Execute tool calls in parallel (built-in GitHub tools + MCP proxy tools)
        let github_token_ref = &ctx.github_token;
        let repo_branches_ref = &ctx.repo_branches;
        let http_ref = &state.http;
        let tool_futures: Vec<_> = tool_uses.iter().map(|tu| {
            let full_name = tu["name"].as_str().unwrap_or("").to_string();
            let input = tu["input"].clone();
            let tool_use_id = tu["id"].as_str().unwrap_or("").to_string();
            let state_ref = &state;
            let team_id_ref = &claims.team_id;
            let email_ref = &claims.email;
            async move {
                let result = if let Some((server_id, tool_name)) = full_name.split_once("__") {
                    if server_id == "github" {
                        // Built-in GitHub codebase tool
                        let output = execute_github_tool(
                            http_ref, github_token_ref, repo_branches_ref,
                            tool_name, &input,
                        ).await;
                        (Some("github".to_string()), json!(output))
                    } else {
                        match invoke_mcp_tool(state_ref, team_id_ref, email_ref, server_id, tool_name, &input).await {
                            Ok(result) => (Some(server_id.to_string()), result),
                            Err(e) => {
                                warn!(tool = full_name.as_str(), error = %e, "MCP tool call failed");
                                (Some(server_id.to_string()), json!(format!("Error: {e}")))
                            }
                        }
                    }
                } else {
                    (None, json!(format!("Unknown tool: {full_name}")))
                };
                (tool_use_id, result.0, result.1)
            }
        }).collect();
        let tool_results_raw = futures::future::join_all(tool_futures).await;

        let mut tool_results: Vec<Value> = worker_results
            .into_iter()
            .map(|(id, text)| json!({"type": "tool_result", "tool_use_id": id, "content": text}))
            .collect();
        for (tool_use_id, server_id, result) in tool_results_raw {
            if let Some(sid) = server_id {
                mcp_servers_used.insert(sid);
            }
            let model_result = if is_empty_tool_result(&result) {
                "No results found.".to_string()
            } else {
                compact_tool_result_for_model(&result)
            };
            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": model_result
            }));
        }

        api_messages.push(json!({"role": "user", "content": tool_results}));

        info!(turn = turn + 1, "Plan chat tool use turn complete");
    }

    // Hit max turns — extract last text
    let text = api_messages
        .last()
        .and_then(|msg| msg["content"].as_array())
        .and_then(|content| {
            content.iter().find_map(|b| {
                if b["type"].as_str() == Some("text") {
                    b["text"].as_str().map(String::from)
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    let servers: Vec<&str> = mcp_servers_used.iter().map(|s| s.as_str()).collect();
    Ok(Json(
        json!({ "content": text, "mcp_servers": servers, "used": used_items }),
    ))
}

// ---------------------------------------------------------------------------
// Streaming variant: POST /api/plans/chat/stream → SSE
// ---------------------------------------------------------------------------

/// Send a single SSE event formatted as `event: {type}\ndata: {json}\n\n`.
pub(crate) fn sse_event(
    event_type: &str,
    data: Value,
) -> Result<axum::response::sse::Event, std::convert::Infallible> {
    Ok(axum::response::sse::Event::default()
        .event(event_type)
        .data(serde_json::to_string(&data).unwrap_or_default()))
}

/// Anthropic SSE streaming for plan chat.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::collapsible_match)]
/// The plan chat's agent permissions for one message.
struct ChatPolicy {
    gate: common::agent_policy::Gate,
    missing: Vec<super::chat_policy::Missing>,
    is_admin: bool,
}

#[allow(clippy::too_many_arguments)]
async fn run_anthropic_stream(
    state: &Arc<AppState>,
    tx: &tokio::sync::mpsc::Sender<Result<axum::response::sse::Event, std::convert::Infallible>>,
    api_key: &str,
    model_id: &str,
    system_prompt: &str,
    tools: &[Value],
    messages: &mut Vec<Value>,
    team_id: &str,
    user_email: &str,
    github_token: &Option<String>,
    repo_branches: &std::collections::HashMap<String, String>,
    worker: &super::plan_chat_tools::WorkerTools,
    scope: &super::plan_chat_tools::ChatScope,
    policy: &ChatPolicy,
) {
    use futures::StreamExt;
    let mut worker_used = super::plan_chat_tools::Used::default();

    let http = reqwest::Client::new();
    let meter = crate::model_call::chat_meter(state, team_id);
    let max_turns = 5;
    let mut total_input_tokens: u64 = 0;
    let mut total_output_tokens: u64 = 0;

    info!(
        model = model_id,
        tools = tools.len(),
        messages = messages.len(),
        "run_anthropic_stream: starting agentic loop"
    );

    for turn in 0..max_turns {
        let mut body = json!({
            "model": model_id,
            "max_tokens": 8192,
            "stream": true,
            "system": [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
        });
        // No sampling params: current models reject non-default temperature.
        // Always-thinking models: explicit effort (defaults differ per model).
        if common::is_always_thinking_model(model_id) {
            body["output_config"] = json!({"effort": "medium"});
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }

        // Retried by status until a stream starts (5xx incl. 520-524, 408,
        // 429, 529, no response); a stream already flowing is never resent.
        let (resp, mut flight) = match crate::model_call::open_chat_stream(
            &http, api_key, &body, &meter, model_id,
        )
        .await
        {
            Ok(r) => {
                info!(turn = turn, "run_anthropic_stream: Anthropic returned 200");
                r
            }
            Err(e) if e.status().is_some() => {
                error!(transient = e.is_transient(), "Anthropic stream error: {e}");
                let _ = tx
                    .send(sse_event(
                        "error",
                        json!({"message": format!("Anthropic API error: {}", e.status().unwrap_or(0))}),
                    ))
                    .await;
                return;
            }
            Err(e) => {
                error!("Anthropic stream request failed: {e}");
                let _ = tx
                    .send(sse_event(
                        "error",
                        json!({"message": format!("AI service error: {e}")}),
                    ))
                    .await;
                return;
            }
        };

        // Process SSE stream from Anthropic
        let mut stream = resp;
        let mut buffer = String::new();
        let mut assistant_text = String::new();
        let mut tool_uses: Vec<(String, String, String)> = Vec::new(); // (id, name, input_json)
        let mut active_tool_id = String::new();
        let mut active_tool_name = String::new();
        let mut active_tool_input = String::new();
        let mut stop_reason = String::new();
        // Every content block by stream index, rebuilt verbatim (thinking text +
        // signature included) — thinking blocks must go back unchanged.
        let mut blocks: std::collections::BTreeMap<u64, Value> = std::collections::BTreeMap::new();
        let mut block_json: std::collections::HashMap<u64, String> =
            std::collections::HashMap::new();

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => break,
            };
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(line_end) = buffer.find('\n') {
                let line = buffer[..line_end].trim().to_string();
                buffer = buffer[line_end + 1..].to_string();

                if !line.starts_with("data: ") {
                    continue;
                }
                let data = &line[6..];
                if data == "[DONE]" {
                    break;
                }

                let event: Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // The call's usage (and the estimate if the stream breaks).
                flight.acc.feed(&event);

                match event["type"].as_str() {
                    Some("content_block_start") => {
                        if let Some(cb) = event.get("content_block") {
                            let idx = event["index"].as_u64().unwrap_or(0);
                            blocks.insert(idx, cb.clone());
                            if cb["type"].as_str() == Some("tool_use") {
                                active_tool_id = cb["id"].as_str().unwrap_or("").to_string();
                                active_tool_name = cb["name"].as_str().unwrap_or("").to_string();
                                active_tool_input.clear();
                                let server = active_tool_name
                                    .split("__")
                                    .next()
                                    .unwrap_or("")
                                    .to_string();
                                let _ = tx
                                    .send(sse_event(
                                        "tool_start",
                                        json!({"id": &active_tool_id, "name": &active_tool_name, "server": server}),
                                    ))
                                    .await;
                            }
                        }
                    }
                    Some("content_block_delta") => {
                        if let Some(delta) = event.get("delta") {
                            let idx = event["index"].as_u64().unwrap_or(0);
                            if let Some(block) = blocks.get_mut(&idx) {
                                accumulate_delta(block, delta, block_json.entry(idx).or_default());
                            }
                            match delta["type"].as_str() {
                                Some("text_delta") => {
                                    if let Some(text) = delta["text"].as_str() {
                                        assistant_text.push_str(text);
                                        let _ = tx
                                            .send(sse_event("text_delta", json!({"text": text})))
                                            .await;
                                    }
                                }
                                Some("input_json_delta") => {
                                    if let Some(partial) = delta["partial_json"].as_str() {
                                        active_tool_input.push_str(partial);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Some("content_block_stop") => {
                        let idx = event["index"].as_u64().unwrap_or(0);
                        if let (Some(block), Some(raw)) =
                            (blocks.get_mut(&idx), block_json.get(&idx))
                        {
                            if block["type"].as_str() == Some("tool_use") {
                                block["input"] = serde_json::from_str(raw).unwrap_or(json!({}));
                            }
                        }
                        if !active_tool_id.is_empty() {
                            tool_uses.push((
                                active_tool_id.clone(),
                                active_tool_name.clone(),
                                active_tool_input.clone(),
                            ));
                            active_tool_id.clear();
                            active_tool_name.clear();
                            active_tool_input.clear();
                        }
                    }
                    Some("message_delta") => {
                        if let Some(sr) = event["delta"]["stop_reason"].as_str() {
                            stop_reason = sr.to_string();
                        }
                    }
                    _ => {}
                }
            }
        }

        // Count this call, once, even if the stream broke off: a missing
        // final output count is estimated from what arrived (marked so),
        // never message_start's placeholder.
        let (turn_usage, _estimated) = flight.finish().await;
        total_input_tokens += turn_usage.uncached_input;
        total_output_tokens += turn_usage.output;

        // Assistant turn for conversation state: every block, in order, verbatim.
        let assistant_content: Vec<Value> = blocks.into_values().collect();
        if stop_reason == "refusal" && assistant_text.is_empty() {
            let _ = tx
                .send(sse_event(
                    "text_delta",
                    json!({"text": "The model declined this request. Try rephrasing it."}),
                ))
                .await;
        }
        if !assistant_content.is_empty() {
            messages.push(json!({"role": "assistant", "content": assistant_content}));
        }

        if stop_reason != "tool_use" || tool_uses.is_empty() {
            info!(
                turn = turn + 1,
                stop_reason = %stop_reason,
                text_len = assistant_text.len(),
                input_tokens = total_input_tokens,
                output_tokens = total_output_tokens,
                "run_anthropic_stream: finished"
            );
            let _ = tx
                .send(sse_event(
                    "usage",
                    json!({
                        "input_tokens": total_input_tokens,
                        "output_tokens": total_output_tokens,
                        "turns": turn + 1,
                    }),
                ))
                .await;
            let _ = tx.send(sse_event("done", json!({}))).await;
            return;
        }

        // Execute tools
        let mut tool_results: Vec<Value> = Vec::new();
        for (tu_id, tu_name, tu_input_str) in &tool_uses {
            let input: Value = serde_json::from_str(tu_input_str).unwrap_or(json!({}));
            let _ = tx
                .send(sse_event(
                    "tool_call",
                    json!({"id": tu_id, "name": tu_name, "input": input}),
                ))
                .await;

            // Asked of the policy gate first: an ask shows a card in the
            // chat, a refusal is the tool's answer, a pause ends the reply.
            let verdict = if tu_name == super::chat_policy::SIGN_IN_TOOL {
                let (text, event) = super::chat_policy::sign_in(
                    state,
                    team_id,
                    &policy.gate.ctx.thread_id,
                    &policy.missing,
                    policy.is_admin,
                    &input,
                )
                .await;
                super::chat_policy::Checked::Answer { text, event }
            } else {
                super::chat_policy::check(&policy.gate, tu_name, &input).await
            };
            match verdict {
                super::chat_policy::Checked::Run => {}
                super::chat_policy::Checked::Answer { text, event } => {
                    if let Some((kind, v)) = event {
                        let _ = tx.send(sse_event(&kind, v)).await;
                    }
                    let _ = tx
                        .send(sse_event(
                            "tool_result",
                            json!({"id": tu_id, "name": tu_name, "status": "error", "summary": common::truncate_str(&text, 120), "used": null}),
                        ))
                        .await;
                    tool_results.push(json!({
                        "type": "tool_result",
                        "tool_use_id": tu_id,
                        "content": text,
                        "is_error": true
                    }));
                    continue;
                }
                super::chat_policy::Checked::Stop(note) => {
                    let _ = tx
                        .send(sse_event(
                            "text_delta",
                            json!({"text": format!("\n\n{note}")}),
                        ))
                        .await;
                    let _ = tx.send(sse_event("done", json!({}))).await;
                    return;
                }
            }

            let mut used = None;
            let result = if worker.names.contains(tu_name.as_str()) {
                let (text, u) =
                    super::plan_chat_tools::call(state, scope, &mut worker_used, tu_name, &input)
                        .await;
                used = u;
                json!(text)
            } else if let Some((server_id, tool_name)) = tu_name.split_once("__") {
                if server_id == "github" {
                    let output =
                        execute_github_tool(&http, github_token, repo_branches, tool_name, &input)
                            .await;
                    json!(output)
                } else {
                    match invoke_mcp_tool(state, team_id, user_email, server_id, tool_name, &input)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(tool = tu_name.as_str(), error = %e, "MCP tool call failed");
                            json!(format!("Error: {e}"))
                        }
                    }
                }
            } else {
                json!(format!("Unknown tool: {tu_name}"))
            };
            let is_error = result
                .as_str()
                .map(|s| s.starts_with("Error:"))
                .unwrap_or(false);
            let is_empty = is_empty_tool_result(&result);
            let result_str = if is_empty {
                "No results found.".to_string()
            } else {
                compact_tool_result_for_model(&result)
            };
            let summary = if is_empty {
                "No results found".to_string()
            } else {
                summarize_tool_result(&result)
            };
            let status = if is_error { "error" } else { "success" };
            let _ = tx
                .send(sse_event(
                    "tool_result",
                    json!({"id": tu_id, "name": tu_name, "status": status, "summary": summary, "used": used}),
                ))
                .await;

            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": tu_id,
                "content": result_str
            }));
        }

        messages.push(json!({"role": "user", "content": tool_results}));
        info!(
            turn = turn + 1,
            "Anthropic streaming tool use turn complete"
        );
    }

    // Hit max turns
    let _ = tx
        .send(sse_event(
            "usage",
            json!({
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "turns": max_turns,
            }),
        ))
        .await;
    let _ = tx.send(sse_event("done", json!({}))).await;
}

#[allow(clippy::collapsible_match)]
pub async fn plan_chat_stream(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Result<
    axum::response::sse::Sse<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<axum::response::sse::Event, std::convert::Infallible>,
                    > + Send,
            >,
        >,
    >,
    StatusCode,
> {
    // The token issued by /api/plans/chat/token (auth::jwt::stream_token).
    let token = crate::auth::jwt::stream_token(&headers).ok_or_else(|| {
        warn!("plan_chat_stream: no stream token");
        StatusCode::UNAUTHORIZED
    })?;

    let claims = crate::auth::jwt::validate_scoped_token(
        token,
        crate::auth::jwt::PLAN_CHAT_AUDIENCE,
        &state.secrets.jwt_secret,
    )
    .map_err(|e| {
        warn!("plan_chat_stream: JWT validation failed: {e}");
        StatusCode::UNAUTHORIZED
    })?;

    let messages_input = body["messages"].as_array().ok_or_else(|| {
        warn!("plan_chat_stream: missing messages array in body");
        StatusCode::BAD_REQUEST
    })?;
    if messages_input.is_empty() || messages_input.len() > 20 {
        warn!(
            count = messages_input.len(),
            "plan_chat_stream: invalid message count"
        );
        return Err(StatusCode::BAD_REQUEST);
    }

    info!(
        team_id = %claims.team_id,
        message_count = messages_input.len(),
        "plan_chat_stream: starting"
    );

    let ctx = load_plan_chat_context(&state, &claims.team_id, &claims.email, messages_input, &body).await.inspect_err(|&s| {
        error!(status = %s, team_id = %claims.team_id, "plan_chat_stream: load_plan_chat_context failed");
    })?;
    let mut api_messages = ctx.messages;

    // Load team's Anthropic API key (required)
    let provider_result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("SETTINGS#MODEL_PROVIDER"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    // On Bedrock the deployment's own role calls the model: no team key.
    let anthropic_api_key = provider_result
        .as_ref()
        .and_then(|item| item.get("api_key"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .or_else(|| common::model_provider::is_bedrock().then(String::new));

    let model_id = provider_result
        .as_ref()
        .and_then(|item| {
            // Prefer heavy_model for plan chat (better reasoning for planning)
            item.get("heavy_model")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .or_else(|| item.get("primary_model").and_then(|v| v.as_s().ok()))
        })
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-5-5".to_string());

    // Move everything into the streaming task
    let team_id = claims.team_id.clone();
    let user_email = claims.email.clone();
    let system_prompt = ctx.system_prompt;
    let tools = ctx.tools;
    let github_token = ctx.github_token;
    let repo_branches = ctx.repo_branches;
    let worker = ctx.worker;
    let scope = ctx.scope;
    // Agent permissions: the gate, and the sign-in card's connections.
    let missing =
        super::chat_policy::missing_connections(&state, &claims.team_id, &claims.email).await;
    let mut tools = tools;
    if let Some(t) = super::chat_policy::sign_in_tool(&missing) {
        tools.insert(0, t);
    }
    let gate = super::chat_policy::gate(
        &state,
        &claims.team_id,
        &claims.email,
        &scope.chat_id,
        &scope.repo,
        &tools,
    );
    super::chat_policy::settle_connected(
        &state,
        &claims.team_id,
        &gate.ctx.thread_id,
        &claims.email,
    )
    .await;
    let chat_policy = ChatPolicy {
        gate,
        missing,
        is_admin: claims.holder().holds("integrations"),
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<axum::response::sse::Event, std::convert::Infallible>,
    >(64);

    info!(
        team_id = %team_id,
        model = %model_id,
        tools_count = tools.len(),
        has_api_key = anthropic_api_key.is_some(),
        "plan_chat_stream: spawning streaming task"
    );

    // Spawn the agentic streaming loop
    tokio::spawn(async move {
        if let Some(ref api_key) = anthropic_api_key {
            run_anthropic_stream(
                &state,
                &tx,
                api_key,
                &model_id,
                &system_prompt,
                &tools,
                &mut api_messages,
                &team_id,
                &user_email,
                &github_token,
                &repo_branches,
                &worker,
                &scope,
                &chat_policy,
            )
            .await;
        } else {
            warn!(team_id = %team_id, "plan_chat_stream: no Anthropic API key configured");
            let _ = tx
                .send(sse_event(
                    "error",
                    json!({ "message": "Anthropic API key not configured. Please set up your API key in Settings under Model Provider." }),
                ))
                .await;
        }
    });

    let stream: std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>
                + Send,
        >,
    > = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    Ok(axum::response::sse::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text(""),
    ))
}

/// Check if a tool result is effectively empty (no meaningful data returned).
pub(crate) fn is_empty_tool_result(result: &Value) -> bool {
    // Notion-style: {"object":"list","results":[]}
    if let Some(arr) = result.get("results").and_then(|v| v.as_array()) {
        if arr.is_empty() {
            return true;
        }
    }
    // Generic empty array/object or null
    match result {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        Value::String(s) => s.is_empty() || s == "null" || s == "[]" || s == "{}",
        _ => false,
    }
}

/// Produce a short summary of a tool result for the UI.
pub(crate) fn summarize_tool_result(result: &Value) -> String {
    let s = result.to_string();
    if s.len() <= 120 {
        s
    } else {
        format!("{}…", s.chars().take(117).collect::<String>())
    }
}

/// Keep tool results compact before feeding back into the model to avoid runaway context growth.
pub(crate) fn compact_tool_result_for_model(result: &Value) -> String {
    let s = result.to_string();
    let max_chars = 3000usize;
    if s.chars().count() <= max_chars {
        return s;
    }
    let truncated: String = s.chars().take(max_chars).collect();
    format!("{truncated}… [truncated]")
}

async fn load_allow_plan_log_analyzer(state: &AppState, team_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("SETTINGS#WORKFLOW"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| {
            i.get("allow_plan_log_analyzer")
                .and_then(|v| v.as_bool().ok())
                .copied()
        })
        .unwrap_or(false)
}

async fn load_log_analyzer_context(state: &AppState, team_id: &str) -> String {
    let result = match state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":prefix", attr_s("REC#"))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await
    {
        Ok(v) => v,
        Err(_) => return String::new(),
    };

    let mut lines: Vec<String> = Vec::new();
    for item in result.items() {
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");
        if status == "dismissed" {
            continue;
        }

        let title = item
            .get("title")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("Untitled recommendation");
        let severity = item
            .get("severity")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("unknown");
        let summary = item
            .get("summary")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        lines.push(format!("- [{severity}] {title}: {summary}"));
    }

    lines.join("\n")
}

// ---------------------------------------------------------------------------
// MCP tool helpers for plan_chat
// ---------------------------------------------------------------------------

pub(crate) struct McpToolDef {
    pub(crate) server_id: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) input_schema: Value,
}

/// Return the MCP configs table name, falling back to settings table for migration compat.
fn mcp_table(state: &AppState) -> &str {
    let t = &state.config.mcp_configs_table_name;
    if t.is_empty() {
        &state.config.settings_table_name
    } else {
        t
    }
}

/// Load enabled plugins for the team, returning (server_id, has_credentials, custom_prompt).
/// A catalog server the team signed in to with its own app counts as
/// switched on with credentials.
pub(crate) async fn load_enabled_plugins(
    state: &AppState,
    team_id: &str,
) -> Vec<(String, bool, Option<String>)> {
    let mut out = load_switched_on_plugins(state, team_id).await;
    let rt = super::mcp_servers::runtime(state, team_id);
    for id in common::mcp_catalog::ids() {
        if !common::mcp_remote::own_app::connected(&rt, id).await {
            continue;
        }
        match out.iter_mut().find(|(p, _, _)| p == id) {
            Some(p) => p.1 = true,
            None => out.push((id.to_string(), true, None)),
        }
    }
    out
}

async fn load_switched_on_plugins(
    state: &AppState,
    team_id: &str,
) -> Vec<(String, bool, Option<String>)> {
    state
        .dynamo
        .query()
        .table_name(mcp_table(state))
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":prefix", attr_s("PLUGIN#"))
        .send()
        .await
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| {
                    let sk = item.get("sk")?.as_s().ok()?;
                    sk.strip_prefix("PLUGIN#").map(|id| {
                        let has_creds = item
                            .get("has_credentials")
                            .and_then(|v| v.as_bool().ok())
                            .copied()
                            .unwrap_or(false);
                        let custom_prompt = item
                            .get("custom_prompt")
                            .and_then(|v| v.as_s().ok())
                            .map(|s| s.to_string());
                        (id.to_string(), has_creds, custom_prompt)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Load cached MCP tool schemas from S3 for a given server.
pub(crate) async fn load_mcp_tool_cache(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    server_id: &str,
) -> Option<Vec<Value>> {
    let key = format!("config/mcp-tools/{server_id}.json");
    let resp = s3.get_object().bucket(bucket).key(&key).send().await.ok()?;
    let bytes = resp.body.collect().await.ok()?.into_bytes();
    let parsed: Value = serde_json::from_slice(&bytes).ok()?;
    parsed["tools"].as_array().cloned()
}

/// Invoke an MCP tool via the MCP proxy Lambda.
pub(crate) async fn invoke_mcp_tool(
    state: &AppState,
    team_id: &str,
    user_email: &str,
    server_id: &str,
    tool_name: &str,
    tool_input: &Value,
) -> Result<Value, String> {
    // A remote server this person may use: through its own connection,
    // allowed tools only, audited.
    let rt = super::mcp_servers::runtime(state, team_id);
    if let Some((server, owner)) = rt
        .usable(Some(user_email))
        .await
        .into_iter()
        .find(|(r, _)| r.id == server_id)
    {
        let out = rt
            .call_tool(
                &server,
                &owner,
                tool_name,
                tool_input,
                &format!("plan chat ({user_email})"),
            )
            .await?;
        return Ok(json!({
            "content": [{"type": "text", "text": out.text}],
            "isError": out.is_error,
        }));
    }
    // Find catalog entry for this server
    let entry = common::mcp_catalog::find(server_id)
        .ok_or_else(|| format!("Unknown MCP server: {server_id}"))?;
    let npx_package = entry.npx_package;

    // The team's own-app token or its saved key, opened for this call only.
    let env_vars = super::plugins::package_env(state, team_id, entry).await?;

    // Build proxy Lambda payload
    let payload = json!({
        "action": "call_tool",
        "server_id": server_id,
        "npx_package": npx_package,
        "env_vars": env_vars,
        "tool_name": tool_name,
        "tool_input": tool_input,
    });

    let resp = state
        .lambda
        .invoke()
        .function_name(&state.config.mcp_proxy_function_name)
        .payload(aws_sdk_lambda::primitives::Blob::new(
            serde_json::to_vec(&payload).map_err(|e| format!("serialize: {e}"))?,
        ))
        .send()
        .await
        .map_err(|e| format!("Lambda invoke failed: {e}"))?;

    let response_payload = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();

    let response: Value =
        serde_json::from_slice(&response_payload).map_err(|e| format!("parse response: {e}"))?;

    if let Some(err) = response.get("error").and_then(|v| v.as_str()) {
        return Err(err.to_string());
    }

    Ok(response.get("result").cloned().unwrap_or(json!(null)))
}

/// Execute a built-in GitHub codebase tool (search_code, read_file, list_directory).
/// Uses the GitHub REST API directly with the team's installation token.
pub(crate) async fn execute_github_tool(
    http: &reqwest::Client,
    github_token: &Option<String>,
    repo_branches: &std::collections::HashMap<String, String>,
    tool_name: &str,
    input: &Value,
) -> String {
    let token = match github_token {
        Some(t) => t,
        None => return "Error: GitHub integration not configured for this team.".to_string(),
    };

    let repo = match input["repo"].as_str() {
        Some(r) => r,
        None => return "Error: 'repo' parameter is required.".to_string(),
    };

    let (owner, name) = match repo.split_once('/') {
        Some(parts) => parts,
        None => return format!("Error: invalid repo format '{repo}', expected 'owner/name'."),
    };

    let default_branch = repo_branches
        .get(repo)
        .map(|s| s.as_str())
        .unwrap_or("main");

    match tool_name {
        "search_code" => {
            let query = match input["query"].as_str() {
                Some(q) => q,
                None => return "Error: 'query' parameter is required.".to_string(),
            };
            github_search_code(http, token, owner, name, query).await
        }
        "read_file" => {
            let path = match input["path"].as_str() {
                Some(p) => p,
                None => return "Error: 'path' parameter is required.".to_string(),
            };
            github_read_file(http, token, owner, name, path, default_branch).await
        }
        "list_directory" => {
            let path = input["path"].as_str().unwrap_or("");
            github_list_directory(http, token, owner, name, path, default_branch).await
        }
        _ => format!("Error: unknown GitHub tool '{tool_name}'."),
    }
}

async fn github_search_code(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    name: &str,
    query: &str,
) -> String {
    let search_query = format!("{query} repo:{owner}/{name}");
    let resp = http
        .get("https://api.github.com/search/code")
        .query(&[("q", &search_query), ("per_page", &"20".to_string())])
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github.text-match+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            match r.json::<Value>().await {
                Ok(data) => {
                    let items = data["items"].as_array().cloned().unwrap_or_default();
                    if items.is_empty() {
                        return "No results found.".to_string();
                    }
                    let mut output = format!("{} results:\n", items.len());
                    for item in &items {
                        let path = item["path"].as_str().unwrap_or("");
                        output.push_str(&format!("\n## {path}\n"));
                        if let Some(matches) = item["text_matches"].as_array() {
                            for m in matches {
                                if let Some(frag) = m["fragment"].as_str() {
                                    output.push_str(&format!("```\n{frag}\n```\n"));
                                }
                            }
                        }
                    }
                    // Cap output to avoid bloating context
                    if output.len() > 12000 {
                        output.truncate(12000);
                        output.push_str("\n... (truncated)");
                    }
                    output
                }
                Err(e) => format!("Error parsing search results: {e}"),
            }
        }
        Ok(r) => format!("GitHub API error: {}", r.status()),
        Err(e) => format!("Request failed: {e}"),
    }
}

async fn github_read_file(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    name: &str,
    path: &str,
    branch: &str,
) -> String {
    let url = format!(
        "https://api.github.com/repos/{owner}/{name}/contents/{}?ref={}",
        common::repo_read::path_url(path),
        urlencoding::encode(branch)
    );
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github.raw+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.text().await {
            Ok(content) => {
                if content.len() > 32000 {
                    format!(
                        "{}... (truncated, {} bytes total)",
                        &content[..32000],
                        content.len()
                    )
                } else {
                    content
                }
            }
            Err(e) => format!("Error reading response: {e}"),
        },
        Ok(r) if r.status().as_u16() == 404 => format!("File not found: {path}"),
        Ok(r) => format!("GitHub API error: {}", r.status()),
        Err(e) => format!("Request failed: {e}"),
    }
}

async fn github_list_directory(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    name: &str,
    path: &str,
    branch: &str,
) -> String {
    let url = if path.is_empty() {
        format!(
            "https://api.github.com/repos/{owner}/{name}/contents?ref={}",
            urlencoding::encode(branch)
        )
    } else {
        format!(
            "https://api.github.com/repos/{owner}/{name}/contents/{}?ref={}",
            common::repo_read::path_url(path),
            urlencoding::encode(branch)
        )
    };
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.json::<Value>().await {
            Ok(Value::Array(items)) => {
                let mut output = String::new();
                for item in &items {
                    let name = item["name"].as_str().unwrap_or("");
                    let item_type = item["type"].as_str().unwrap_or("file");
                    let icon = if item_type == "dir" { "📁" } else { "📄" };
                    output.push_str(&format!("{icon} {name}\n"));
                }
                if output.is_empty() {
                    "Empty directory.".to_string()
                } else {
                    output
                }
            }
            Ok(_) => "Path is a file, not a directory. Use read_file instead.".to_string(),
            Err(e) => format!("Error parsing directory listing: {e}"),
        },
        Ok(r) if r.status().as_u16() == 404 => format!("Directory not found: {path}"),
        Ok(r) => format!("GitHub API error: {}", r.status()),
        Err(e) => format!("Request failed: {e}"),
    }
}

#[cfg(test)]
mod list_tests {
    use super::is_plan_row;

    #[test]
    fn task_rows_do_not_count_as_plans() {
        assert!(is_plan_row("PLAN#01PLAN"));
        assert!(!is_plan_row("PLAN#01PLAN#TASK#01TASK"));
        assert!(!is_plan_row("TEMPLATE#01T"));
        assert!(!is_plan_row(""));
    }
}

#[cfg(test)]
mod stream_block_tests {
    use super::*;

    #[test]
    fn thinking_block_rebuilt_with_signature() {
        let mut block = json!({"type": "thinking", "thinking": "", "signature": ""});
        let mut raw = String::new();
        accumulate_delta(
            &mut block,
            &json!({"type": "thinking_delta", "thinking": "a"}),
            &mut raw,
        );
        accumulate_delta(
            &mut block,
            &json!({"type": "thinking_delta", "thinking": "b"}),
            &mut raw,
        );
        accumulate_delta(
            &mut block,
            &json!({"type": "signature_delta", "signature": "s=="}),
            &mut raw,
        );
        assert_eq!(
            block,
            json!({"type": "thinking", "thinking": "ab", "signature": "s=="})
        );
    }

    #[test]
    fn text_and_tool_json_accumulate() {
        let mut text = json!({"type": "text", "text": ""});
        let mut raw = String::new();
        accumulate_delta(
            &mut text,
            &json!({"type": "text_delta", "text": "he"}),
            &mut raw,
        );
        accumulate_delta(
            &mut text,
            &json!({"type": "text_delta", "text": "llo"}),
            &mut raw,
        );
        assert_eq!(text["text"], "hello");

        let mut tool = json!({"type": "tool_use", "id": "t", "name": "n", "input": {}});
        let mut raw = String::new();
        accumulate_delta(
            &mut tool,
            &json!({"type": "input_json_delta", "partial_json": "{\"a\":"}),
            &mut raw,
        );
        accumulate_delta(
            &mut tool,
            &json!({"type": "input_json_delta", "partial_json": "1}"}),
            &mut raw,
        );
        assert_eq!(raw, "{\"a\":1}");
    }
}

#[cfg(test)]
mod start_tests {
    /// The body of `pub async fn <name>` in this file.
    fn body(name: &str) -> String {
        let src = include_str!("plans.rs");
        let start = src
            .find(&format!("pub async fn {name}("))
            .unwrap_or_else(|| panic!("{name} not found"));
        let rest = &src[start..];
        let end = rest[1..]
            .find("\npub async fn ")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        rest[..end].to_string()
    }

    /// Every route that starts a task's work.
    const STARTS: [&str; 4] = [
        "approve_task",
        "approve_all_and_execute",
        "execute_plan",
        "force_run_task",
    ];

    #[test]
    fn every_route_that_starts_work_checks_the_run_budget() {
        for name in STARTS {
            let b = body(name);
            assert!(
                b.contains("check_run_budget"),
                "{name} starts paid runs without checking the team's run budget"
            );
            let check = b.find("check_run_budget").unwrap();
            let send = ["start_tasks(", "send_plan_message(", ".send_message()"]
                .iter()
                .filter_map(|s| b.find(s))
                .min()
                .unwrap_or_else(|| panic!("{name} starts nothing"));
            assert!(check < send, "{name} checks the budget after starting");
        }
    }

    #[test]
    fn tasks_are_queued_before_the_message_that_starts_them() {
        // `queued` written after the message could land on a task the worker
        // had already filed.
        let helper = body("approve_task");
        let queued = helper.find(r#"attr_s("queued")"#).expect("queued");
        let sent = helper.find("send_plan_message(").expect("sent");
        assert!(queued < sent);
        let src = include_str!("plans.rs");
        let start = src.find("async fn start_tasks(").unwrap();
        let st = &src[start..start + src[start..].find("\n}\n").unwrap()];
        assert!(st.find(r#""queued""#).unwrap() < st.find("send_plan_message(").unwrap());
        for name in ["approve_all_and_execute", "execute_plan"] {
            let b = body(name);
            assert!(b.contains("start_tasks("), "{name}");
            assert!(!b.contains(".send_message()"), "{name} sends on its own");
        }
    }

    #[test]
    fn approving_a_task_never_finishes_the_plan() {
        for name in STARTS {
            let b = body(name);
            assert!(!b.contains(r#"attr_s("done")"#), "{name} writes done");
        }
        assert!(body("approve_task").contains("mark_plan_running("));
    }

    #[test]
    fn a_review_or_size_on_a_pr_event_is_kept_on_the_run() {
        let p = serde_json::json!({
            "action": "submitted",
            "pull_request": {"number": 4, "additions": 120, "deletions": 8},
            "review": {"state": "approved", "user": {"login": "someone", "type": "User"}},
        });
        let f = super::pr_event_facts("pull_request_review", &p);
        let keys: Vec<&str> = f.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, ["pr_additions", "pr_deletions", "pr_review"]);
        let bot = serde_json::json!({
            "action": "submitted",
            "pull_request": {"number": 4},
            "review": {"state": "changes_requested", "user": {"login": "coderhelm[bot]", "type": "Bot"}},
        });
        assert!(super::pr_event_facts("pull_request_review", &bot).is_empty());
    }
}
