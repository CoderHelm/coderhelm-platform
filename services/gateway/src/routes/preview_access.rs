//! A repository's preview access (Settings, Repositories, Preview access):
//! optional. The preview's address template, for a host that publishes it
//! nowhere CoderHelm reads, and how an automated browser gets through a
//! protected preview, with one secret the team stores. See
//! `common::preview_access` for the rules (the secret sealed, never
//! returned, sent only to the preview's own host).

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::preview_access::{self, Kind, PreviewAccess};
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
        "Only admins can change a repository's preview access.",
    )
}

/// What the request asks to keep, checked: the access, and the secret to
/// seal ("" keeps the one saved). `prior`: the kind saved before and
/// whether a secret is saved.
pub fn read_body(
    repo: &str,
    body: &Value,
    prior: Option<(Kind, bool)>,
) -> Result<(PreviewAccess, String), String> {
    let s = |k: &str| body[k].as_str().unwrap_or("").trim().to_string();
    let kind = Kind::parse(&s("kind")).ok_or(
        "Pick how the preview is protected: none, a header, a query parameter or a password.",
    )?;
    let a = PreviewAccess {
        repo: repo.to_string(),
        kind,
        address: s("address"),
    };
    a.validate()?;
    let secret = body["secret"].as_str().unwrap_or("").trim().to_string();
    if kind == Kind::None {
        return Ok((a, String::new()));
    }
    if secret.is_empty() {
        // A saved secret is kept only for the same kind: another kind's
        // secret has another shape.
        return match prior {
            Some((k, true)) if k == kind => Ok((a, String::new())),
            _ => Err("Enter the secret the preview takes.".into()),
        };
    }
    preview_access::check_secret(kind, &secret)?;
    Ok((a, secret))
}

async fn read_item(
    state: &AppState,
    team_id: &str,
    repo: &str,
) -> Result<Option<std::collections::HashMap<String, AttributeValue>>, (StatusCode, Json<Value>)> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(preview_access::sk(repo)))
        .send()
        .await
        .map(|r| r.item)
        .map_err(|e| internal("Failed to read the preview access", e.to_string()))
}

fn sealed_of(item: &std::collections::HashMap<String, AttributeValue>) -> Option<String> {
    item.get(preview_access::SECRET_ATTR)
        .and_then(|v| v.as_s().ok().cloned())
        .filter(|s| !s.is_empty())
}

/// GET /api/repos/:owner/:name/preview-access
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(|_| forbidden())?;
    let repo = repo_of(&owner, &name)?;
    let item = read_item(&state, &claims.team_id, &repo).await?;
    Ok(Json(
        match item.as_ref().and_then(PreviewAccess::from_item) {
            Some(a) => a.view(item.as_ref().and_then(sealed_of).is_some()),
            None => json!({ "configured": false, "repo": repo }),
        },
    ))
}

/// PUT /api/repos/:owner/:name/preview-access
/// Body: `{kind, address?, secret?}`. The secret is required the first
/// time for a protected kind and kept when left out later.
pub async fn put(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((owner, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(|_| forbidden())?;
    let repo = repo_of(&owner, &name)?;
    let prior = read_item(&state, &claims.team_id, &repo).await?;
    let prior_sealed = prior.as_ref().and_then(sealed_of);
    let prior_kind = prior
        .as_ref()
        .and_then(PreviewAccess::from_item)
        .map(|a| (a.kind, prior_sealed.is_some()));
    let (a, secret) =
        read_body(&repo, &body, prior_kind).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let sealed = if a.kind == Kind::None {
        None
    } else if secret.is_empty() {
        prior_sealed
    } else {
        let cipher = super::alerts::cipher(&state);
        Some(
            preview_access::seal(&cipher, &claims.team_id, &repo, &secret)
                .await
                .map_err(|e| internal("Failed to seal the preview access secret", e))?,
        )
    };
    let mut item = a.item(&claims.team_id);
    let secret_set = sealed.is_some();
    if let Some(s) = sealed {
        item.insert(preview_access::SECRET_ATTR.into(), AttributeValue::S(s));
    }
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
        .map_err(|e| internal("Failed to save the preview access", e.to_string()))?;
    Ok(Json(a.view(secret_set)))
}

/// DELETE /api/repos/:owner/:name/preview-access
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
        .key("sk", AttributeValue::S(preview_access::sk(&repo)))
        .send()
        .await
        .map_err(|e| internal("Failed to remove the preview access", e.to_string()))?;
    Ok(Json(json!({ "configured": false, "repo": repo })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_protected_kind_needs_its_secret_the_first_time_and_keeps_it_after() {
        let body = json!({"kind": "header", "secret": "X-Bypass: abc123"});
        let (a, secret) = read_body("acme/web", &body, None).unwrap();
        assert_eq!(a.kind, Kind::Header);
        assert_eq!(secret, "X-Bypass: abc123");
        let keep = json!({"kind": "header"});
        assert!(read_body("acme/web", &keep, None).is_err());
        assert_eq!(
            read_body("acme/web", &keep, Some((Kind::Header, true)))
                .unwrap()
                .1,
            ""
        );
        // Another kind's secret has another shape: a new one is asked for.
        let other = json!({"kind": "basic"});
        assert!(read_body("acme/web", &other, Some((Kind::Header, true))).is_err());
    }

    #[test]
    fn the_address_alone_is_enough_and_is_checked() {
        let ok = json!({"kind": "none", "address": "https://pr-{pr}.preview.example.com"});
        let (a, secret) = read_body("acme/web", &ok, None).unwrap();
        assert_eq!(a.address, "https://pr-{pr}.preview.example.com");
        assert!(secret.is_empty());
        let fixed = json!({"kind": "none", "address": "https://staging.example.com"});
        assert!(read_body("acme/web", &fixed, None).is_err());
        let bad = json!({"kind": "header", "secret": "nocolon"});
        assert!(read_body("acme/web", &bad, None).is_err());
        assert!(read_body("acme/web", &json!({"kind": "magic"}), None).is_err());
    }

    #[test]
    fn repository_names_are_checked() {
        assert_eq!(repo_of("acme", "web").unwrap(), "acme/web");
        assert!(repo_of("acme", "..").is_err());
    }
}
