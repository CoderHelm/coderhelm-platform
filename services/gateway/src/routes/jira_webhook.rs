use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use std::sync::Arc;
use tracing::info;

use crate::AppState;

/// Jira webhook handler — placeholder for Phase 2.
pub async fn handle(
    State(_state): State<Arc<AppState>>,
    _headers: HeaderMap,
    _body: Bytes,
) -> StatusCode {
    info!("Jira webhook received — not yet implemented");
    StatusCode::OK
}
