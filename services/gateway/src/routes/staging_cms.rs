//! A repository's staging CMS write connection (Settings, Repositories):
//! optional, staging only. When a feature's schema change makes the content
//! its end-to-end tests read out of date, CoderHelm updates that content on
//! staging through this connection; without one the run says exactly which
//! content must change. See `common::staging_cms` for the rules (never
//! production, the token sealed and never returned).

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::staging_cms::{self, StagingCms};
use serde_json::{json, Value};
use tracing::error;

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": message, "code": crate::openapi::default_code(status) })),
    )
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn repo_of(owner: &str, name: &str) -> Result<String, (StatusCode, Json<Value>)> {
    let repo = format!("{owner}/{name}");
    let ok = repo.len() <= 200
        && !repo.contains("..")
        && repo
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/');
    if ok {
        Ok(repo)
    } else {
        Err(fail(
            StatusCode::BAD_REQUEST,
            "That isn't a repository name.",
        ))
    }
}

fn forbidden() -> (StatusCode, Json<Value>) {
    fail(
        StatusCode::FORBIDDEN,
        "Only admins can change a repository's staging CMS connection.",
    )
}

/// GET /api/repos/:owner/:name/staging-cms
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(|_| forbidden())?;
    let repo = repo_of(&owner, &name)?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(staging_cms::sk(&repo)))
        .send()
        .await
        .map_err(|e| internal("Failed to read the staging CMS connection", e.to_string()))?
        .item;
    Ok(Json(match item.as_ref().and_then(StagingCms::from_item) {
        Some(c) => c.view(
            item.as_ref()
                .and_then(|i| i.get(staging_cms::TOKEN_ATTR))
                .and_then(|v| v.as_s().ok())
                .is_some_and(|s| !s.is_empty()),
        ),
        None => json!({ "configured": false, "repo": repo, "providers": staging_cms::PROVIDERS }),
    }))
}

/// PUT /api/repos/:owner/:name/staging-cms
/// Body: `{provider, project_id, dataset, production_dataset?, token?}`. The
/// token is required the first time and kept when left out later.
pub async fn put(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(|_| forbidden())?;
    let repo = repo_of(&owner, &name)?;
    let s = |k: &str| body[k].as_str().unwrap_or("").trim().to_string();
    let c = StagingCms {
        repo: repo.clone(),
        provider: s("provider"),
        project_id: s("project_id"),
        dataset: s("dataset"),
        production_dataset: s("production_dataset"),
    };
    c.validate()
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let token = s("token");
    if token.len() > 500 {
        return Err(fail(StatusCode::BAD_REQUEST, "The token is too long."));
    }
    let prior_sealed = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(staging_cms::sk(&repo)))
        .send()
        .await
        .map_err(|e| internal("Failed to read the staging CMS connection", e.to_string()))?
        .item
        .and_then(|i| {
            i.get(staging_cms::TOKEN_ATTR)
                .and_then(|v| v.as_s().ok().cloned())
        })
        .filter(|s| !s.is_empty());
    let sealed = if token.is_empty() {
        prior_sealed.ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "Enter a token with write access to the staging dataset.",
            )
        })?
    } else {
        let cipher = super::alerts::cipher(&state);
        staging_cms::seal(&cipher, &claims.team_id, &repo, &token)
            .await
            .map_err(|e| internal("Failed to seal the staging CMS token", e))?
    };
    let mut item = c.item(&claims.team_id);
    item.insert(staging_cms::TOKEN_ATTR.into(), AttributeValue::S(sealed));
    item.insert("updated_by".into(), AttributeValue::S(claims.email.clone()));
    item.insert(
        "updated_at".into(),
        AttributeValue::S(chrono::Utc::now().to_rfc3339()),
    );
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| internal("Failed to save the staging CMS connection", e.to_string()))?;
    Ok(Json(c.view(true)))
}

/// DELETE /api/repos/:owner/:name/staging-cms
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(|_| forbidden())?;
    let repo = repo_of(&owner, &name)?;
    state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(claims.team_id.clone()))
        .key("sk", AttributeValue::S(staging_cms::sk(&repo)))
        .send()
        .await
        .map_err(|e| internal("Failed to remove the staging CMS connection", e.to_string()))?;
    Ok(Json(json!({ "configured": false, "repo": repo })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_names_are_checked() {
        assert_eq!(repo_of("acme", "web").unwrap(), "acme/web");
        assert!(repo_of("acme", "..").is_err());
        assert!(repo_of("acme", "we b").is_err());
    }
}
