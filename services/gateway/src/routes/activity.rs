//! The summary at the top of the activity pages: how often the team's
//! skills were read and its agents asked, and how many lookups ran against
//! its data sources, with how many went wrong. Counts only; each links into
//! the matching filtered log on the dashboard.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::skills::SkillStore;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// The window a summary covers, in days: 1 to 30, 7 when not given.
fn window(q: &HashMap<String, String>) -> i64 {
    q.get("days")
        .and_then(|d| d.parse::<i64>().ok())
        .unwrap_or(7)
        .clamp(1, 30)
}

/// GET /api/activity/summary?days=7 — counts over the last `days` days.
/// `lookups` is null for someone who isn't an admin, since only admins see
/// the lookup log.
pub async fn summary(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let days = window(&q);
    let since = (chrono::Utc::now() - chrono::Duration::days(days))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let table = &state.config.settings_table_name;
    let st = SkillStore {
        dynamo: &state.dynamo,
        table,
        team: &claims.team_id,
    };
    let failed = |e: String| {
        error!(error = %e, "activity summary");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Something went wrong on our side. Try again." })),
        )
    };
    let skills = st.access_counts(&since).await.map_err(failed)?;
    let lookups = if matches!(claims.role.as_str(), "admin" | "owner") {
        let (ok, refused, failed_n) =
            common::live::store::count_activity(&state.dynamo, table, &claims.team_id, &since)
                .await
                .map_err(failed)?;
        json!({ "total": ok + refused + failed_n, "ok": ok, "refused": refused, "failed": failed_n })
    } else {
        Value::Null
    };
    Ok(Json(json!({
        "days": days,
        "since": since,
        "skills": {
            "reads": skills.reads,
            "asks": skills.asks,
            "read_problems": skills.read_problems,
            "ask_problems": skills.ask_problems,
            "applies": skills.applies,
        },
        "lookups": lookups,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_is_a_week_unless_asked_and_stays_in_bounds() {
        let q = |d: &str| HashMap::from([("days".to_string(), d.to_string())]);
        assert_eq!(window(&HashMap::new()), 7);
        assert_eq!(window(&q("1")), 1);
        assert_eq!(window(&q("90")), 30);
        assert_eq!(window(&q("0")), 1);
        assert_eq!(window(&q("soon")), 7);
    }
}
