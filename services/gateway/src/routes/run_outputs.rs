//! A run's outputs: the files it kept (`common::outputs`), listed and
//! downloaded on the run's page.
//!
//! `GET /api/runs/:run_id/outputs` lists them; `GET
//! /api/runs/:run_id/outputs/file?name=<name>` returns one. Both read only
//! the signed-in team's run (the run record is looked up under the team
//! first), and a name is checked by `common::outputs::clean_name`, so a
//! request can never reach another run's or team's files. HTML is served as
//! a download, never as a page of this site.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::error;

use crate::models::Claims;
use crate::AppState;

async fn team_run(state: &AppState, team_id: &str, run_id: &str) -> Result<(), StatusCode> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(run_id.to_string()),
        )
        .projection_expression("run_id")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to read a run for its outputs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    r.item().map(|_| ()).ok_or(StatusCode::NOT_FOUND)
}

/// The run's outputs, by name, with their size.
pub async fn list_outputs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    team_run(&state, &claims.team_id, &run_id).await?;
    let prefix = common::outputs::prefix(&claims.team_id, &run_id);
    let page = state
        .s3
        .list_objects_v2()
        .bucket(&state.config.bucket_name)
        .prefix(&prefix)
        .max_keys(common::outputs::MAX_FILES as i32 + 1)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list a run's outputs: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let mut outputs: Vec<Value> = page
        .contents()
        .iter()
        .filter_map(|o| {
            let name = o.key()?.strip_prefix(&prefix)?.to_string();
            Some(json!({ "name": name, "bytes": o.size().unwrap_or(0) }))
        })
        .collect();
    outputs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(Json(json!({ "outputs": outputs })))
}

#[derive(Deserialize)]
pub struct OutputName {
    pub name: String,
}

/// One output's content.
pub async fn get_output(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(run_id): Path<String>,
    Query(q): Query<OutputName>,
) -> Result<Response, StatusCode> {
    let name = common::outputs::clean_name(&q.name).map_err(|_| StatusCode::BAD_REQUEST)?;
    team_run(&state, &claims.team_id, &run_id).await?;
    let obj = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(common::outputs::key(&claims.team_id, &run_id, &name))
        .send()
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let bytes = obj
        .body
        .collect()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_bytes();
    Ok((
        [
            (
                header::CONTENT_TYPE,
                common::outputs::content_type(&name).to_string(),
            ),
            (
                header::CONTENT_DISPOSITION,
                common::outputs::disposition(&name),
            ),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
        ],
        bytes,
    )
        .into_response())
}
