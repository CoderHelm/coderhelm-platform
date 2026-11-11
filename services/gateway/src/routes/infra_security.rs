//! Infrastructure vulnerabilities on the Security page
//! (`common::infra_vuln` has the model, `worker::passes::infra_feeds` reads
//! the feeds).
//!
//! - `GET /api/security/infrastructure/sources` — the public feeds: when
//!   each was last read, how many items, and its last error. Any team
//!   member.

use std::sync::Arc;

use axum::extract::State;
use axum::{http::StatusCode, Extension, Json};
use common::infra_vuln::store;
use serde_json::Value;

use crate::models::Claims;
use crate::AppState;

/// GET /api/security/infrastructure/sources
pub async fn sources(
    State(state): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let st = store::statuses(&state.dynamo, &state.config.settings_table_name).await;
    Ok(Json(store::sources_view(&st, chrono::Utc::now())))
}
