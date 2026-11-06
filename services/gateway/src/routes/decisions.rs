//! Analytics of the stop judge's decisions: "Unnecessary stops" and
//! "Nuisance blocks" (rate, count, examples), and the confirmed-wrong
//! decisions as replay cases to add to the suite
//! (`common::decision_log`, `services/worker/src/passes/replay`).

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{Duration, Utc};
use serde_json::{json, Value};

use crate::models::Claims;
use crate::AppState;

#[derive(serde::Deserialize)]
pub struct DecisionsQuery {
    days: Option<i64>,
}

fn window(days: Option<i64>) -> Result<(i64, String), StatusCode> {
    let days = match days.unwrap_or(30) {
        d @ (7 | 30 | 90) => d,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    Ok((days, (Utc::now() - Duration::days(days)).to_rfc3339()))
}

/// GET /api/analytics/decisions?days=30
pub async fn get_decisions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<DecisionsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let (days, since) = window(q.days)?;
    let records = common::decision_log::since(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &since,
    )
    .await;
    let summary = common::decision_log::summarize(&records);
    Ok(Json(json!({
        "days": days,
        "summary": summary,
    })))
}

/// GET /api/analytics/decisions/export?days=90: every confirmed-wrong
/// decision as a replay case (names as the team wrote them: scrub before
/// committing one).
pub async fn export_decisions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<DecisionsQuery>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?;
    let (_, since) = window(q.days.or(Some(90)))?;
    let records = common::decision_log::since(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &since,
    )
    .await;
    let cases: Vec<Value> = records
        .iter()
        .filter(|r| r.confirmed_wrong())
        .map(common::decision_log::fixture)
        .collect();
    Ok(Json(json!({ "cases": cases })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_three_windows() {
        assert_eq!(window(None).unwrap().0, 30);
        assert_eq!(window(Some(7)).unwrap().0, 7);
        assert_eq!(window(Some(90)).unwrap().0, 90);
        assert!(window(Some(12)).is_err());
    }
}
