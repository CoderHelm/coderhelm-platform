//! The API keys page: list, create and revoke the team's API keys.
//!
//! Admins (and custom roles holding `api_keys`) manage keys. The rules a key
//! follows once made are in `crate::api_keys`. These routes are for the
//! dashboard only: an API key can't manage keys (`openapi::PUBLIC_ROUTES`
//! doesn't list them).

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::api_keys::{
    self as keys, KeyStore, StoredKey, DEFAULT_RATE_PER_MINUTE, MAX_EXPIRY_DAYS, MAX_KEYS_PER_TEAM,
    MAX_NAME, MAX_RATE_PER_MINUTE, MIN_RATE_PER_MINUTE,
};
use crate::models::Claims;
use crate::routes::roles::{self, AuditEntry};
use crate::AppState;

type ApiError = (StatusCode, Json<Value>);

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

/// Admins, and custom roles holding `api_keys` (which run as admin here).
fn admin(claims: &Claims) -> Result<(), ApiError> {
    claims
        .require_role(3)
        .map_err(|s| fail(s, "Only admins can manage API keys."))
}

/// A key as the page shows it. Never the hash, never the secret.
pub fn key_view(k: &StoredKey, now: chrono::DateTime<chrono::Utc>) -> Value {
    let status = if k.revoked_at.is_some() {
        "revoked"
    } else if k.is_expired(now) {
        "expired"
    } else {
        "active"
    };
    json!({
        "id": k.id,
        "name": k.name,
        "hint": k.hint,
        "scopes": k.scopes,
        "rate_limit_per_minute": k.rate_limit_per_minute,
        "created_at": k.created_at,
        "created_by": k.created_by_name,
        "expires_at": k.expires_at,
        "last_used_at": k.last_used_at,
        "revoked_at": k.revoked_at,
        "revoked_by": k.revoked_by,
        "status": status,
    })
}

/// GET /api/api-keys — the team's keys, newest first, and what the create
/// form offers this person.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    let now = chrono::Utc::now();
    let all = keys::store(&state)
        .list(&claims.team_id)
        .await
        .map_err(|e| internal("Failed to list API keys", e))?;
    let actor = claims.holder();
    let scopes: Vec<Value> = keys::key_scopes()
        .iter()
        .map(|c| {
            json!({
                "key": c.key,
                "label": c.label,
                "always": c.key == keys::BASE_SCOPE,
                "allowed": actor.holds(c.key),
            })
        })
        .collect();
    Ok(Json(json!({
        "keys": all.iter().map(|k| key_view(k, now)).collect::<Vec<_>>(),
        "scopes": scopes,
        "limits": {
            "default_rate_per_minute": DEFAULT_RATE_PER_MINUTE,
            "min_rate_per_minute": MIN_RATE_PER_MINUTE,
            "max_rate_per_minute": MAX_RATE_PER_MINUTE,
            "max_expiry_days": MAX_EXPIRY_DAYS,
            "max_keys": MAX_KEYS_PER_TEAM,
        },
    })))
}

#[derive(Deserialize, Default, Debug)]
pub struct CreateBody {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Days until the key stops working; none for a key that doesn't expire.
    #[serde(default)]
    pub expires_in_days: Option<i64>,
    #[serde(default)]
    pub rate_limit_per_minute: Option<u32>,
}

/// A new key's settings, checked. Returns the cleaned name, the scopes, the
/// expiry and the rate limit.
pub fn check_create(
    actor: &crate::permissions::Holder,
    body: &CreateBody,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(String, Vec<String>, Option<String>, u32), String> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err("Give the key a name, so you know later what uses it.".into());
    }
    if name.chars().count() > MAX_NAME {
        return Err(format!("Keep the name to {MAX_NAME} characters."));
    }
    let scopes = keys::checked_scopes(actor, &body.scopes).map_err(|e| e.message())?;
    let expires_at = match body.expires_in_days {
        None => None,
        Some(d) if (1..=MAX_EXPIRY_DAYS).contains(&d) => Some(
            (now + chrono::Duration::days(d)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ),
        Some(_) => {
            return Err(format!(
                "A key can last from 1 to {MAX_EXPIRY_DAYS} days, or never expire."
            ))
        }
    };
    let rate = body
        .rate_limit_per_minute
        .unwrap_or(DEFAULT_RATE_PER_MINUTE);
    if !(MIN_RATE_PER_MINUTE..=MAX_RATE_PER_MINUTE).contains(&rate) {
        return Err(format!(
            "Pick a rate limit from {MIN_RATE_PER_MINUTE} to {MAX_RATE_PER_MINUTE} requests a minute."
        ));
    }
    // In table order, so the page lists them the same way every time.
    let ordered: Vec<String> = keys::key_scopes()
        .iter()
        .filter(|c| scopes.contains(c.key))
        .map(|c| c.key.to_string())
        .collect();
    Ok((name.to_string(), ordered, expires_at, rate))
}

/// POST /api/api-keys — make a key. The token is in this answer only.
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateBody>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if claims.support_view.is_some() || claims.api_key.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Keys are made from a signed-in session.",
        ));
    }
    let now = chrono::Utc::now();
    let (name, scopes, expires_at, rate) = check_create(&claims.holder(), &body, now)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let store = keys::store(&state);
    let live = store
        .list(&claims.team_id)
        .await
        .map_err(|e| internal("Failed to count API keys", e))?
        .iter()
        .filter(|k| k.is_live(now))
        .count();
    if live >= MAX_KEYS_PER_TEAM {
        return Err(fail(
            StatusCode::CONFLICT,
            &format!(
                "A team can have {MAX_KEYS_PER_TEAM} live keys. Revoke one you no longer use."
            ),
        ));
    }

    let token = keys::generate();
    let key = StoredKey {
        id: token.id.clone(),
        team_id: claims.team_id.clone(),
        name: name.clone(),
        hash: token.hash,
        scopes: scopes.clone(),
        created_by: claims.sub.clone(),
        created_by_name: claims.display_name(),
        created_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        expires_at: expires_at.clone(),
        revoked_at: None,
        revoked_by: None,
        last_used_at: None,
        rate_limit_per_minute: rate,
        hint: keys::token_hint(&token.token),
    };
    // Nothing changes without a record of it: the log entry is written first.
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "api_key_created",
            role_id: &key.id,
            role_name: &name,
            detail: format!(
                "Scopes: {}. {rate} requests a minute. {}",
                scopes.join(", "),
                match &expires_at {
                    Some(at) => format!("Expires {}.", &at[..10]),
                    None => "Doesn't expire.".into(),
                }
            ),
        },
    )
    .await?;
    store
        .create(&key)
        .await
        .map_err(|e| internal("Failed to save API key", e))?;
    info!(team_id = %claims.team_id, key_id = %key.id, "API key created");
    Ok(Json(json!({
        "key": key_view(&key, now),
        "token": token.token,
    })))
}

/// DELETE /api/api-keys/:key_id — revoke a key. It stops working at once.
pub async fn revoke(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    admin(&claims)?;
    if key_id.len() != 26 || !key_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(fail(StatusCode::NOT_FOUND, "No such key."));
    }
    let store = keys::store(&state);
    let Some(existing) = store
        .get(&key_id)
        .await
        .map_err(|e| internal("Failed to read API key", e))?
        .filter(|k| k.team_id == claims.team_id)
    else {
        return Err(fail(StatusCode::NOT_FOUND, "No such key."));
    };
    if existing.revoked_at.is_some() {
        return Err(fail(StatusCode::CONFLICT, "This key is already revoked."));
    }
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "api_key_revoked",
            role_id: &existing.id,
            role_name: &existing.name,
            detail: format!("Key {} revoked.", existing.hint),
        },
    )
    .await?;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let done = store
        .revoke(&claims.team_id, &key_id, &claims.display_name(), &now)
        .await
        .map_err(|e| internal("Failed to revoke API key", e))?;
    if !done {
        return Err(fail(StatusCode::CONFLICT, "This key is already revoked."));
    }
    info!(team_id = %claims.team_id, key_id = %key_id, "API key revoked");
    Ok(Json(json!({ "status": "revoked", "id": key_id })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::Holder;

    fn now() -> chrono::DateTime<chrono::Utc> {
        crate::api_keys::tests::now()
    }

    fn body(name: &str, scopes: &[&str]) -> CreateBody {
        CreateBody {
            name: name.into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_new_key_is_checked_before_it_is_made() {
        let admin = Holder::builtin("admin");
        let (name, scopes, exp, rate) =
            check_create(&admin, &body("  CI deploys ", &["plans", "runs"]), now()).unwrap();
        assert_eq!(name, "CI deploys");
        // Table order, with view always there.
        assert_eq!(scopes, vec!["view", "plans", "runs"]);
        assert_eq!(exp, None);
        assert_eq!(rate, DEFAULT_RATE_PER_MINUTE);

        assert!(check_create(&admin, &body(" ", &[]), now()).is_err());
        assert!(check_create(&admin, &body(&"x".repeat(61), &[]), now()).is_err());
        // Escalation: a scope no public route needs, or one the maker lacks.
        assert!(check_create(&admin, &body("k", &["people"]), now()).is_err());
        let viewer = Holder::builtin("viewer");
        let err = check_create(&viewer, &body("k", &["runs"]), now()).unwrap_err();
        assert!(err.contains("your own role doesn't include it"), "{err}");
    }

    #[test]
    fn expiry_and_rate_limit_stay_in_range() {
        let admin = Holder::builtin("admin");
        let mut b = body("k", &[]);
        b.expires_in_days = Some(30);
        let (_, _, exp, _) = check_create(&admin, &b, now()).unwrap();
        assert_eq!(exp.as_deref(), Some("2026-11-23T12:00:30Z"));
        for bad in [0, -1, MAX_EXPIRY_DAYS + 1] {
            b.expires_in_days = Some(bad);
            assert!(check_create(&admin, &b, now()).is_err(), "{bad}");
        }
        b.expires_in_days = None;
        for (rate, ok) in [
            (MIN_RATE_PER_MINUTE, true),
            (MAX_RATE_PER_MINUTE, true),
            (5, false),
            (5000, false),
        ] {
            b.rate_limit_per_minute = Some(rate);
            assert_eq!(check_create(&admin, &b, now()).is_ok(), ok, "{rate}");
        }
    }

    #[test]
    fn the_page_never_sees_the_hash() {
        let t = keys::generate();
        let k = StoredKey {
            id: t.id.clone(),
            team_id: "TEAM#a".into(),
            name: "ci".into(),
            hash: t.hash.clone(),
            scopes: vec!["view".into()],
            created_by: "USER#a".into(),
            created_by_name: "a@example.com".into(),
            created_at: "2026-10-01T00:00:00Z".into(),
            expires_at: Some("2026-10-02T00:00:00Z".into()),
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
            rate_limit_per_minute: 600,
            hint: keys::token_hint(&t.token),
        };
        let v = key_view(&k, now());
        let text = v.to_string();
        assert!(!text.contains(&t.hash));
        assert!(!text.contains(&t.token));
        assert_eq!(v["status"], "expired");
        assert_eq!(v["created_by"], "a@example.com");
    }
}
