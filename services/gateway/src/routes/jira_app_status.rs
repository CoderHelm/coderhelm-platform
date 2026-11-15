//! Whether the team's Jira site runs the latest Coderhelm Jira app
//! (`common::jira_app_version`).
//!
//! Every Forge event and registration carries the app's version; [`record`]
//! keeps the latest seen in the team's `JIRA#config`. The latest release is
//! the pointer the release workflow publishes (`launch/jira-app.json`, read
//! without credentials and kept for a few minutes). [`for_team`] compares
//! the two for the Integrations page's Jira card and the run page.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::jira_app_version::{self as v, Behind, Latest, Seen};
use serde_json::{json, Value};
use tracing::warn;

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// How long a read of the pointer is kept, and a failed read.
const LATEST_TTL: Duration = Duration::from_secs(600);
const LATEST_RETRY: Duration = Duration::from_secs(60);

fn s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

/// Keeps what a Forge event or registration says about the app (its
/// version, or none) on the team's `JIRA#config`. `stored` is the record
/// already read, when there is one: an unchanged, recent sighting is not
/// written again. Never fails the caller.
pub async fn record(state: &AppState, team_id: &str, stored: Option<&Item>, payload: &Value) {
    let now = chrono::Utc::now();
    let seen = Seen::from_payload(payload, &now.to_rfc3339());
    if !seen.needs_write(stored.and_then(Seen::from_item).as_ref(), now) {
        return;
    }
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", s(team_id))
        .key("sk", s("JIRA#config"))
        // Only on a record that is there: a sighting never creates one.
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_names("#v", v::FIELD_VERSION)
        .expression_attribute_names("#t", v::FIELD_SEEN_AT)
        .expression_attribute_values(":v", s(&seen.version))
        .expression_attribute_values(":t", s(&seen.seen_at));
    let expr = if seen.app_id.is_empty() {
        "SET #v = :v, #t = :t"
    } else {
        req = req
            .expression_attribute_names("#a", v::FIELD_APP_ID)
            .expression_attribute_values(":a", s(&seen.app_id));
        "SET #v = :v, #t = :t, #a = :a"
    };
    if let Err(e) = req.update_expression(expr).send().await {
        warn!(team_id, error = %e, "Could not keep the Jira app's version");
    }
}

fn release_url() -> String {
    std::env::var("JIRA_APP_RELEASE_URL")
        .ok()
        .filter(|u| u.starts_with("https://"))
        .unwrap_or_else(|| v::DEFAULT_RELEASE_URL.to_string())
}

static LATEST: Mutex<Option<(Instant, Option<Latest>)>> = Mutex::new(None);

fn cached() -> Option<Option<Latest>> {
    let guard = LATEST.lock().ok()?;
    let (at, latest) = guard.as_ref()?;
    let ttl = if latest.is_some() {
        LATEST_TTL
    } else {
        LATEST_RETRY
    };
    (at.elapsed() < ttl).then(|| latest.clone())
}

/// The latest released version, from the published pointer. `None` when it
/// can't be read (the comparison then waits; a site that reports no version
/// is still behind).
pub async fn latest(state: &AppState) -> Option<Latest> {
    if let Some(hit) = cached() {
        return hit;
    }
    let fetched = async {
        let res = state
            .http
            .get(release_url())
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .ok()?;
        if !res.status().is_success() {
            return None;
        }
        Latest::parse(&res.text().await.ok()?)
    }
    .await;
    if fetched.is_none() {
        warn!("Could not read the Jira app's latest release");
    }
    if let Ok(mut g) = LATEST.lock() {
        *g = Some((Instant::now(), fetched.clone()));
    }
    fetched
}

/// Whether the site in `config` (the team's `JIRA#config`) is behind.
pub async fn for_config(state: &AppState, config: Option<&Item>) -> Option<Behind> {
    let item = config?;
    let seen = Seen::from_item(item)?;
    let site = item
        .get("site_url")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let latest = latest(state).await;
    v::assess(Some(&seen), latest.as_ref(), &site)
}

/// Whether the team's site is behind, reading its `JIRA#config`.
pub async fn for_team(state: &AppState, team_id: &str) -> Option<Behind> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", s(team_id))
        .key("sk", s("JIRA#config"))
        .send()
        .await
        .ok()?
        .item;
    for_config(state, item.as_ref()).await
}

/// GET /api/integrations/jira/app: `{ "behind": null }` when the team's
/// Jira site runs the latest app (or nothing is known), otherwise what it
/// runs, what it lacks, where a site admin accepts the update (Atlassian
/// Administration, Connected apps) and the installation link when published.
pub async fn get_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?;
    let behind = Box::pin(for_team(&state, &claims.team_id)).await;
    Ok(Json(json!({ "behind": behind })))
}
