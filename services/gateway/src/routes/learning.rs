//! The Learning page: what CoderHelm learned from outcomes, and the admin
//! controls over it (see `common::rewards`).
//!
//! Everything is read and written under the caller's own team. Anyone on the
//! team may look; only admins pin a prompt variant, revert or restore a
//! behavior change, or tune the signal weights.

use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::rewards::{bandit, event::AGENTS, store::Store, summary};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;

fn store(state: &AppState) -> Store {
    Store::new(&state.dynamo, &state.config.settings_table_name)
}

fn internal(e: String) -> StatusCode {
    error!(error = %e, "Learning store error");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[derive(Deserialize)]
pub struct LearningQuery {
    days: Option<u32>,
    repo: Option<String>,
}

/// The repository filter: empty, or a valid `owner/name`.
pub fn repo_filter(raw: Option<&str>) -> Result<String, StatusCode> {
    let r = raw.unwrap_or("").trim();
    if r.is_empty() {
        return Ok(String::new());
    }
    if common::review_lessons::valid_scope(r) && r != common::review_lessons::TEAM_SCOPE {
        Ok(r.to_string())
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

pub fn days_of(raw: Option<u32>) -> u32 {
    raw.unwrap_or(30).clamp(1, 180)
}

/// GET /api/learning — rewards, calibration, changes, examples and variants.
pub async fn get_learning(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<LearningQuery>,
) -> Result<Json<Value>, StatusCode> {
    let repo = repo_filter(q.repo.as_deref())?;
    let days = days_of(q.days);
    let team = claims.team_id.as_str();
    let st = store(&state);
    let since = (chrono::Utc::now() - chrono::Duration::days(days as i64)).to_rfc3339();
    let weights = st.weights(team).await;
    let events = st.events_since(team, &since).await.map_err(internal)?;
    let categories = st.all_categories(team).await.map_err(internal)?;
    let changes = st.changes(team).await.map_err(internal)?;
    let exemplars = st
        .exemplars(team, (!repo.is_empty()).then_some(repo.as_str()))
        .await
        .map_err(internal)?;
    let mut arms = vec![];
    for agent in AGENTS {
        if bandit::arms_for(agent).is_empty() {
            continue;
        }
        let stats = st
            .arm_stats(team, agent, &weights)
            .await
            .map_err(internal)?;
        arms.push((agent.to_string(), st.pin(team, agent).await, stats));
    }
    let can_manage = crate::permissions::role_level(&claims.role) >= 3;
    Ok(Json(summary::build(&summary::Inputs {
        team_id: team,
        days,
        repo: &repo,
        can_manage,
        weights: &weights,
        events: &events,
        categories: &categories,
        changes: &changes,
        exemplars: &exemplars,
        arms: &arms,
    })))
}

#[derive(Deserialize)]
pub struct PinBody {
    agent: String,
    arm: Option<String>,
}

/// Checks a pin request: a known agent with variants, and one of its arms.
pub fn check_pin(agent: &str, arm: Option<&str>) -> Result<(), StatusCode> {
    if bandit::arms_for(agent).is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    match arm {
        Some(a) if bandit::arm(agent, a).is_none() => Err(StatusCode::BAD_REQUEST),
        _ => Ok(()),
    }
}

/// POST /api/learning/pin — pin (or with `arm: null` unpin) a prompt variant.
pub async fn pin_variant(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PinBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let arm = body.arm.as_deref().filter(|a| !a.is_empty());
    check_pin(&body.agent, arm)?;
    store(&state)
        .set_pin(&claims.team_id, &body.agent, arm, &claims.display_name())
        .await
        .map_err(internal)?;
    info!(team_id = %claims.team_id, agent = %body.agent, arm = ?arm, "Prompt variant pinned");
    Ok(Json(json!({"ok": true})))
}

#[derive(Deserialize)]
pub struct RevertBody {
    id: Option<String>,
    repo: Option<String>,
}

/// A change id as the log writes them (a ULID).
pub fn valid_change_id(id: &str) -> bool {
    id.len() == 26 && id.chars().all(|c| c.is_ascii_alphanumeric())
}

/// POST /api/learning/revert — turn one change off, or every active change
/// of a repository (`{"repo": "owner/name"}`).
pub async fn revert_change(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RevertBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let st = store(&state);
    let by = claims.display_name();
    match (body.id.as_deref(), body.repo.as_deref()) {
        (Some(id), None) => {
            if !valid_change_id(id) {
                return Err(StatusCode::BAD_REQUEST);
            }
            if !st
                .revert(&claims.team_id, id, &by)
                .await
                .map_err(internal)?
            {
                return Err(StatusCode::NOT_FOUND);
            }
            Ok(Json(json!({"reverted": 1})))
        }
        (None, Some(repo)) => {
            let repo = repo_filter(Some(repo))?;
            if repo.is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            let n = st
                .revert_repo(&claims.team_id, &repo, &by)
                .await
                .map_err(internal)?;
            info!(team_id = %claims.team_id, repo = %repo, n, "Learning changes reverted for a repository");
            Ok(Json(json!({"reverted": n})))
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Deserialize)]
pub struct RestoreBody {
    id: String,
}

/// POST /api/learning/restore — undo a revert.
pub async fn restore_change(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    if !valid_change_id(&body.id) {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !store(&state)
        .restore(&claims.team_id, &body.id, &claims.display_name())
        .await
        .map_err(internal)?
    {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({"ok": true})))
}

#[derive(Deserialize)]
pub struct WeightsBody {
    weights: serde_json::Map<String, Value>,
}

/// PUT /api/learning/weights — the team's signal weights (`{}` resets).
pub async fn put_weights(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<WeightsBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let w = common::rewards::score::Weights::validate(&body.weights)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    store(&state)
        .put_weights(&claims.team_id, &w, &claims.display_name())
        .await
        .map_err(internal)?;
    Ok(Json(json!({"weights": w.effective()})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_filters_are_owner_and_name_only() {
        assert_eq!(repo_filter(None), Ok(String::new()));
        assert_eq!(repo_filter(Some(" ")), Ok(String::new()));
        assert_eq!(repo_filter(Some("acme/web")), Ok("acme/web".into()));
        assert_eq!(repo_filter(Some("*")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("acme")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("a/b/c")), Err(StatusCode::BAD_REQUEST));
        assert_eq!(repo_filter(Some("../x")), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn days_are_bounded() {
        assert_eq!(days_of(None), 30);
        assert_eq!(days_of(Some(0)), 1);
        assert_eq!(days_of(Some(9999)), 180);
    }

    #[test]
    fn pins_name_a_real_agent_and_arm() {
        assert_eq!(check_pin("reviewer", Some("focused")), Ok(()));
        assert_eq!(check_pin("reviewer", None), Ok(()));
        assert_eq!(
            check_pin("reviewer", Some("bogus")),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(check_pin("coder", None), Err(StatusCode::BAD_REQUEST));
        assert_eq!(check_pin("nope", Some("x")), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn change_ids_are_ulids() {
        assert!(valid_change_id(&ulid::Ulid::new().to_string()));
        assert!(!valid_change_id("../../x"));
        assert!(!valid_change_id(""));
    }
}
