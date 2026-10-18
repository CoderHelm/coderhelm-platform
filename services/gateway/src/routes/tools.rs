//! A team's own tools: named, typed, reviewed reads its agents can call.
//!
//! Admins write or receive (from repo files) a definition, see it checked
//! and run once with its example inputs, and another admin approves that
//! exact version. Agents only ever run approved, switched-on versions; any
//! edit is a new version that waits for approval again. Running, checking
//! and the read-only locks live in `common::live::tools*`.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::live::catalog;
use common::live::store::{self, Activity, Kind, Source};
use common::live::tools::{self, can_approve, validate, ToolDef, ToolKind};
use common::live::tools_run::{activity_text, run_tool, source_info, ToolRun};
use common::live::tools_store::{ToolStore, MAX_TOOLS};
use common::live::{secret, StepError};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

const PREVIEW_CHARS: usize = 12_000;

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
        Err(fail(StatusCode::FORBIDDEN, "Only admins can manage tools."))
    }
}

fn store<'a>(state: &'a AppState, claims: &'a Claims) -> ToolStore<'a> {
    ToolStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: &claims.team_id,
    }
}

/// Reads a definition from `{definition: "<yaml>"}` or `{def: {...}}`.
fn def_from(body: &Value) -> Result<ToolDef, (StatusCode, Json<Value>)> {
    if let Some(text) = body["definition"].as_str() {
        return tools::parse_def(text).map_err(|e| fail(StatusCode::BAD_REQUEST, &e));
    }
    serde_json::from_value::<ToolDef>(body["def"].clone()).map_err(|e| {
        fail(
            StatusCode::BAD_REQUEST,
            &format!("That definition can't be read: {e}"),
        )
    })
}

async fn find_source(
    state: &AppState,
    team: &str,
    name_or_id: &str,
) -> Result<Option<Source>, (StatusCode, Json<Value>)> {
    let sources = store::list_sources(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("list data sources", e))?;
    Ok(sources
        .into_iter()
        .find(|s| s.id == name_or_id || s.name.eq_ignore_ascii_case(name_or_id)))
}

async fn open_key(
    state: &AppState,
    team: &str,
    s: &Source,
) -> Result<String, (StatusCode, Json<Value>)> {
    if s.kind != Kind::Api || s.key_sealed.is_empty() {
        return Ok(String::new());
    }
    secret::open(&state.kms, team, &s.key_sealed)
        .await
        .map_err(|e| internal("open data source key", e))
}

async fn admin_count(state: &AppState, team: &str) -> usize {
    let r = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.into()),
        )
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S("USER#".into()),
        )
        .send()
        .await;
    match r {
        Ok(o) => o
            .items()
            .iter()
            .filter(|i| {
                matches!(
                    i.get("role")
                        .and_then(|v| v.as_s().ok())
                        .map(String::as_str),
                    Some("admin" | "owner")
                )
            })
            .count(),
        // Unknown → treat as many admins, so self-approval stays refused.
        Err(_) => usize::MAX,
    }
}

async fn record(
    state: &AppState,
    team: &str,
    source: &Source,
    actor: &str,
    text: &str,
    outcome: &Result<ToolRun, StepError>,
) {
    let (label, detail, ms) = match outcome {
        Ok(r) => ("ok", r.note.clone(), r.ms),
        Err(e) if e.is_refusal() => ("refused", e.message.clone(), 0),
        Err(e) => ("failed", e.message.clone(), 0),
    };
    let a = Activity {
        source_id: source.id.clone(),
        source_name: source.name.clone(),
        actor: actor.to_string(),
        repo: String::new(),
        pr: 0,
        query: text.to_string(),
        outcome: label,
        detail,
        ms,
    };
    if let Err(e) =
        store::log_activity(&state.dynamo, &state.config.settings_table_name, team, &a).await
    {
        warn!(error = %e, "Could not record a tool run");
    }
}

/// Real names from the source's saved description to use instead of the ones
/// a tool's read failed on. Empty when there is no description or nothing
/// close.
async fn name_hints(
    state: &AppState,
    team: &str,
    source: &Source,
    def: &ToolDef,
    e: &StepError,
) -> Vec<String> {
    if !catalog::is_name_error(&e.message) || !source.schema_available {
        return Vec::new();
    }
    let Ok(out) = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(Source::schema_s3_key(team, &source.id))
        .send()
        .await
    else {
        return Vec::new();
    };
    let Ok(bytes) = out.body.collect().await else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&bytes.into_bytes()).into_owned();
    let c = catalog::Catalog::parse(&text);
    match def.kind {
        ToolKind::Sql => catalog::sql_hints(&c, &e.message, &def.read, &source.hidden_fields),
        ToolKind::Graphql => catalog::graphql_hints(&c, &e.message, &source.hidden_fields),
        _ => Vec::new(),
    }
}

fn run_json(r: &Result<ToolRun, StepError>) -> Value {
    match r {
        Ok(r) => {
            json!({ "ok": true, "sent": r.sent, "result": r.result, "hidden_count": r.hidden_count, "ms": r.ms as u64, "note": r.note })
        }
        Err(e) => {
            json!({ "ok": false, "refused": e.is_refusal(), "step": e.step, "message": e.message, "fix": e.fix })
        }
    }
}

fn example_inputs(def: &ToolDef) -> Value {
    Value::Object(
        def.inputs
            .iter()
            .map(|(k, s)| (k.clone(), s.example.clone()))
            .collect(),
    )
}

/// GET /api/tools
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state, &claims);
    let mut out = Vec::new();
    for t in st.list().await.map_err(|e| internal("list tools", e))? {
        let latest = st
            .version(&t.name, &t.latest)
            .await
            .map_err(|e| internal("tool version", e))?;
        let approved = if t.approved.is_empty() || t.approved == t.latest {
            None
        } else {
            st.version(&t.name, &t.approved)
                .await
                .map_err(|e| internal("tool version", e))?
        };
        let mut v = t.json();
        v["latest_version"] = latest.map(|x| x.json()).unwrap_or(Value::Null);
        v["approved_version"] = approved.map(|x| x.json()).unwrap_or(Value::Null);
        out.push(v);
    }
    let pending = st
        .asks()
        .await
        .map_err(|e| internal("list asks", e))?
        .into_iter()
        .filter(|a| a["status"] == "pending")
        .count();
    Ok(Json(json!({ "tools": out, "pending_asks": pending })))
}

/// POST /api/tools/validate  { definition | def } — checks only, runs nothing.
pub async fn check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let def = match def_from(&body) {
        Ok(d) => d,
        Err((_, Json(e))) => return Ok(Json(json!({ "ok": false, "problems": [e["error"]] }))),
    };
    let Some(source) = find_source(&state, &claims.team_id, &def.source).await? else {
        return Ok(Json(
            json!({ "ok": false, "problems": [format!("No connected source is called {}.", def.source)] }),
        ));
    };
    Ok(Json(match validate(&def, &source_info(&source)) {
        Ok(p) => {
            json!({ "ok": true, "sent": p.text, "hash": tools::content_hash(&def), "function": def.function_name(), "input_schema": tools::input_schema(&def) })
        }
        Err(problems) => json!({ "ok": false, "problems": problems }),
    }))
}

/// Checks, dry-runs with the examples, and saves a version. Shared by the
/// dashboard and by repo-file proposals.
pub async fn save_checked(
    state: &AppState,
    team: &str,
    def: &ToolDef,
    author: &str,
    origin: &str,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let st = ToolStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team,
    };
    if st
        .tool(&def.name)
        .await
        .map_err(|e| internal("get tool", e))?
        .is_none()
        && st
            .list()
            .await
            .map_err(|e| internal("list tools", e))?
            .len()
            >= MAX_TOOLS
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "You've reached the limit of tools. Remove one first.",
        ));
    }
    let Some(source) = find_source(state, team, &def.source).await? else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "That source isn't connected.", "problems": [format!("No connected source is called {}.", def.source)] }),
            ),
        ));
    };
    if let Err(problems) = validate(def, &source_info(&source)) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The tool didn't pass its checks.", "problems": problems })),
        ));
    }
    let key = open_key(state, team, &source).await?;
    let inputs = example_inputs(def);
    let run = run_tool(
        &state.sts,
        def,
        &inputs,
        &source,
        &key,
        author,
        PREVIEW_CHARS,
    )
    .await;
    record(
        state,
        team,
        &source,
        author,
        &activity_text(def, &inputs),
        &run,
    )
    .await;
    // The dry run with the examples is the save-time check that the read
    // really runs: a tool the source can't run is not saved at all.
    if let Err(e) = &run {
        if tools::example_failure_blocks_save(def, e) {
            let mut problems = vec![format!("The example run failed: {} {}", e.message, e.fix)];
            problems.extend(name_hints(state, team, &source, def, e).await);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "The tool's read doesn't run against its source, so it wasn't saved.",
                    "problems": problems,
                    "example": run_json(&run),
                })),
            ));
        }
    }
    let example = run_json(&run);
    let (tool, version) = st
        .save_version(def, author, origin, &example.to_string(), run.is_ok())
        .await
        .map_err(|e| internal("save tool", e))?;
    info!(team, tool = %def.name, hash = %version.hash, "Tool version saved");
    Ok(json!({ "tool": tool.json(), "version": version.json(), "example": example }))
}

/// POST /api/tools  { definition | def }
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let def = def_from(&body)?;
    save_checked(&state, &claims.team_id, &def, &claims.email, "dashboard")
        .await
        .map(Json)
}

/// GET /api/tools/:name
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state, &claims);
    let tool = st
        .tool(&name)
        .await
        .map_err(|e| internal("get tool", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That tool doesn't exist."))?;
    let versions: Vec<Value> = st
        .versions(&name)
        .await
        .map_err(|e| internal("tool versions", e))?
        .iter()
        .map(|v| v.json())
        .collect();
    Ok(Json(json!({ "tool": tool.json(), "versions": versions })))
}

/// POST /api/tools/:name/approve  { hash }
pub async fn approve(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let hash = body["hash"].as_str().unwrap_or("");
    let st = store(&state, &claims);
    let version = st
        .version(&name, hash)
        .await
        .map_err(|e| internal("tool version", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That version doesn't exist."))?;
    can_approve(
        &version.author,
        &claims.email,
        admin_count(&state, &claims.team_id).await,
    )
    .map_err(|e| fail(StatusCode::FORBIDDEN, &e))?;
    if !version.example_ok {
        return Err(fail(StatusCode::BAD_REQUEST, "This version's example run didn't work, so it can't be approved. Fix it and save again."));
    }
    // Checked again against the source as it is now.
    let source = find_source(&state, &claims.team_id, &version.def.source)
        .await?
        .ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "That tool's source is no longer connected.",
            )
        })?;
    if let Err(p) = validate(&version.def, &source_info(&source)) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "The tool no longer passes its checks.", "problems": p })),
        ));
    }
    st.approve(&name, hash, &claims.email)
        .await
        .map_err(|e| internal("approve tool", e))?;
    info!(team = %claims.team_id, tool = %name, hash, by = %claims.email, "Tool version approved");
    let tool = st.tool(&name).await.map_err(|e| internal("get tool", e))?;
    Ok(Json(json!({ "tool": tool.map(|t| t.json()) })))
}

/// PUT /api/tools/:name  { enabled }
pub async fn update(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state, &claims);
    if let Some(on) = body["enabled"].as_bool() {
        st.set_enabled(&name, on)
            .await
            .map_err(|e| internal("switch tool", e))?;
    }
    let tool = st.tool(&name).await.map_err(|e| internal("get tool", e))?;
    Ok(Json(json!({ "tool": tool.map(|t| t.json()) })))
}

/// DELETE /api/tools/:name
pub async fn remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Reply {
    admin(&claims)?;
    store(&state, &claims)
        .delete(&name)
        .await
        .map_err(|e| internal("delete tool", e))?;
    Ok(Json(json!({ "ok": true })))
}

/// POST /api/tools/:name/preview  { inputs, hash? } — runs one version as a
/// person, recorded like any lookup.
pub async fn preview(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let st = store(&state, &claims);
    let tool = st
        .tool(&name)
        .await
        .map_err(|e| internal("get tool", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That tool doesn't exist."))?;
    let hash = body["hash"]
        .as_str()
        .map(str::to_string)
        .unwrap_or(tool.latest);
    let version = st
        .version(&name, &hash)
        .await
        .map_err(|e| internal("tool version", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That version doesn't exist."))?;
    let source = find_source(&state, &claims.team_id, &version.def.source)
        .await?
        .ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "That tool's source is no longer connected.",
            )
        })?;
    let key = open_key(&state, &claims.team_id, &source).await?;
    let inputs = body.get("inputs").cloned().unwrap_or(Value::Null);
    let run = run_tool(
        &state.sts,
        &version.def,
        &inputs,
        &source,
        &key,
        &claims.email,
        PREVIEW_CHARS,
    )
    .await;
    record(
        &state,
        &claims.team_id,
        &source,
        &claims.email,
        &activity_text(&version.def, &inputs),
        &run,
    )
    .await;
    Ok(Json(run_json(&run)))
}

/// GET /api/tools/asks
pub async fn asks(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    admin(&claims)?;
    let asks = store(&state, &claims)
        .asks()
        .await
        .map_err(|e| internal("list asks", e))?;
    Ok(Json(json!({ "asks": asks })))
}

/// POST /api/tools/asks/:id  { approve: bool }
pub async fn decide(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let approve = body["approve"]
        .as_bool()
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "Say approve: true or false."))?;
    store(&state, &claims)
        .decide(&id, approve, &claims.email)
        .await
        .map_err(|e| fail(StatusCode::CONFLICT, &e))?;
    info!(team = %claims.team_id, ask = %id, approve, by = %claims.email, "Tool ask decided");
    Ok(Json(json!({ "ok": true })))
}
