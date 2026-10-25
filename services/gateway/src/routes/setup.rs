//! Where GitHub's app manifest flow lands on a self-hosted install
//! (docs/self-host/setup-checks.md is the contract).
//!
//! `GET /api/setup/github/callback`: the install guide on docs.coderhelm.com
//! posts a GitHub App manifest to github.com whose `redirect_url` is this
//! route. GitHub sends the browser here with a one-time code; the install
//! trades it for the new app's keys, stores them in its own secret and
//! restarts its services to read them, then sends the browser back to the
//! guide. The keys never pass through CoderHelm's servers.
//!
//! It accepts only the one-time setup key the Launch Stack stored
//! (`setup_token` in the secret, compared in constant time), only while no
//! GitHub App exists, and deletes the key once used. The guide's progress
//! checks are the public `GET /api/setup/status` (routes::setup_checks).
//! Not on the hosted deployment (`SETUP_ENDPOINTS` unset: 404).

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use subtle::ConstantTimeEq;
use tracing::{error, info, warn};

use crate::AppState;

/// Where the browser goes after the manifest flow, unless the install says otherwise.
pub const DEFAULT_RETURN_URL: &str = "https://docs.coderhelm.com/self-host/install/";

fn enabled() -> bool {
    std::env::var("SETUP_ENDPOINTS").is_ok_and(|v| v == "1")
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.trim().is_empty())
}

// ── Pure rules (tested below) ──────────────────────────────────────────────

/// The setup key the browser brought back equals the stored one (constant time).
pub fn state_matches(stored: Option<&str>, got: &str) -> bool {
    match stored {
        Some(s) if s.len() >= 32 && s.len() == got.len() => {
            s.as_bytes().ct_eq(got.as_bytes()).into()
        }
        _ => false,
    }
}

/// GitHub's one-time manifest code, as GitHub makes them.
pub fn valid_code(code: &str) -> bool {
    (8..=128).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// True when the secret already holds a GitHub App.
pub fn has_app(secret: &Value) -> bool {
    let s = |k: &str| {
        secret
            .get(k)
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty())
    };
    s("github_app_id") && s("github_private_key")
}

/// The secret with the new app's keys in it and the setup key gone (it is one-time).
pub fn merge_app_keys(secret: &Value, app: &Value) -> Result<Value, &'static str> {
    let mut out: Map<String, Value> = secret.as_object().cloned().unwrap_or_default();
    let get = |k: &str| app.get(k).filter(|v| !v.is_null());
    let id = match get("id") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => return Err("id"),
    };
    let text = |k: &'static str| {
        get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or(k)
    };
    out.insert("github_app_id".into(), json!(id));
    out.insert("github_private_key".into(), json!(text("pem")?));
    out.insert(
        "github_webhook_secret".into(),
        json!(text("webhook_secret")?),
    );
    out.insert("github_client_id".into(), json!(text("client_id")?));
    out.insert("github_client_secret".into(), json!(text("client_secret")?));
    out.insert(
        "github_app_slug".into(),
        json!(get("slug").and_then(Value::as_str).unwrap_or("")),
    );
    out.remove("setup_token");
    Ok(Value::Object(out))
}

/// Where the browser goes after the callback: the guide, with the outcome.
pub fn return_url(base: &str, outcome: &str, slug: Option<&str>) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    let mut u = format!("{base}{sep}github={}", urlencoding::encode(outcome));
    if let Some(s) = slug.filter(|s| !s.is_empty()) {
        u.push_str(&format!("&app={}", urlencoding::encode(s)));
    }
    u
}

// ── The install's secret ───────────────────────────────────────────────────

static SM: tokio::sync::OnceCell<aws_sdk_secretsmanager::Client> =
    tokio::sync::OnceCell::const_new();

async fn secrets_client() -> &'static aws_sdk_secretsmanager::Client {
    SM.get_or_init(|| async {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_secretsmanager::Client::new(&cfg)
    })
    .await
}

fn secret_name() -> String {
    std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".to_string())
}

/// The secret as it is now (not as it was when this function started).
async fn read_secret() -> Result<Value, String> {
    let r = secrets_client()
        .await
        .get_secret_value()
        .secret_id(secret_name())
        .send()
        .await
        .map_err(|e| aws_sdk_secretsmanager::error::DisplayErrorContext(&e).to_string())?;
    serde_json::from_str(r.secret_string().unwrap_or("{}")).map_err(|e| e.to_string())
}

async fn write_secret(value: &Value) -> Result<String, String> {
    let r = secrets_client()
        .await
        .put_secret_value()
        .secret_id(secret_name())
        .secret_string(value.to_string())
        .send()
        .await
        .map_err(|e| aws_sdk_secretsmanager::error::DisplayErrorContext(&e).to_string())?;
    Ok(r.version_id().unwrap_or_default().to_string())
}

/// Restarts the services so they read the new keys (the updater does it).
async fn refresh_services(state: &AppState, revision: &str) -> bool {
    let Some(name) = env("UPDATER_FUNCTION_NAME") else {
        return false;
    };
    let payload = json!({ "action": "refresh_secrets", "revision": revision }).to_string();
    state
        .lambda
        .invoke()
        .function_name(name)
        .invocation_type(aws_sdk_lambda::types::InvocationType::Event)
        .payload(aws_sdk_lambda::primitives::Blob::new(payload))
        .send()
        .await
        .map_err(|e| {
            error!(error = %aws_sdk_lambda::error::DisplayErrorContext(&e), "setup: service restart not started")
        })
        .is_ok()
}

#[derive(Deserialize, Default)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
}

/// GET /api/setup/github/callback?code=&state=
pub async fn github_callback(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CallbackQuery>,
) -> Response {
    if !enabled() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let base = env("SETUP_RETURN_URL").unwrap_or_else(|| DEFAULT_RETURN_URL.to_string());
    let back = |outcome: &str, slug: Option<&str>| {
        Redirect::to(&return_url(&base, outcome, slug)).into_response()
    };

    let secret = match read_secret().await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "setup: cannot read the install's secret");
            return back("store_failed", None);
        }
    };
    if has_app(&secret) {
        return back("exists", secret["github_app_slug"].as_str());
    }
    if !state_matches(
        secret.get("setup_token").and_then(Value::as_str),
        q.state.as_deref().unwrap_or(""),
    ) {
        warn!("setup: a GitHub App callback without this install's setup key was refused");
        return back("bad_state", None);
    }
    let code = q.code.unwrap_or_default();
    if !valid_code(&code) {
        return back("bad_code", None);
    }
    let conv = state
        .http
        .post(format!(
            "https://api.github.com/app-manifests/{code}/conversions"
        ))
        .header("accept", "application/vnd.github+json")
        .header("user-agent", "coderhelm-setup")
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let app: Value = match conv {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => v,
            Err(_) => return back("github_refused", None),
        },
        Ok(r) => {
            warn!(status = %r.status(), "setup: GitHub refused the manifest conversion");
            return back("github_refused", None);
        }
        Err(_) => return back("github_unreachable", None),
    };
    let next = match merge_app_keys(&secret, &app) {
        Ok(v) => v,
        Err(field) => {
            warn!(field, "setup: GitHub's answer is missing a field");
            return back("github_refused", None);
        }
    };
    let slug = app["slug"].as_str().unwrap_or_default().to_string();
    let version = match write_secret(&next).await {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "setup: storing the GitHub App's keys failed");
            return back("store_failed", Some(&slug));
        }
    };
    let restarted = refresh_services(&state, &version).await;
    info!(app = %slug, restarted, "setup: GitHub App created and stored");
    back(
        if restarted {
            "created"
        } else {
            "created_restart_needed"
        },
        Some(&slug),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_setup_key_must_match_exactly() {
        let k = "a".repeat(40);
        assert!(state_matches(Some(&k), &k));
        assert!(!state_matches(Some(&k), &"a".repeat(39)));
        assert!(!state_matches(Some(&k), &"b".repeat(40)));
        assert!(!state_matches(None, &k));
        assert!(
            !state_matches(Some("short"), "short"),
            "short keys are never accepted"
        );
        assert!(!state_matches(Some(""), ""));
    }

    #[test]
    fn codes_look_like_githubs() {
        assert!(valid_code("a1b2c3d4e5f6a7b8c9d0"));
        assert!(!valid_code("short"));
        assert!(!valid_code("../../etc/passwd-and-more"));
        assert!(!valid_code(&"a".repeat(129)));
    }

    #[test]
    fn app_keys_replace_the_setup_key() {
        let secret = json!({ "jwt_secret": "j", "github_app_id": "", "setup_token": "t" });
        let app = json!({ "id": 123, "slug": "coderhelm-prod", "pem": "-----BEGIN RSA PRIVATE KEY-----", "webhook_secret": "w", "client_id": "Iv1", "client_secret": "cs" });
        let out = merge_app_keys(&secret, &app).unwrap();
        assert_eq!(out["github_app_id"], "123");
        assert_eq!(out["github_app_slug"], "coderhelm-prod");
        assert_eq!(out["jwt_secret"], "j", "other keys stay");
        assert!(
            out.get("setup_token").is_none(),
            "the setup key is one-time"
        );
        assert!(has_app(&out));
        assert!(!has_app(&secret));
        assert_eq!(merge_app_keys(&secret, &json!({ "id": 1 })), Err("pem"));
        assert_eq!(merge_app_keys(&secret, &json!({})), Err("id"));
    }

    #[test]
    fn back_to_the_guide_with_the_outcome() {
        assert_eq!(
            return_url(DEFAULT_RETURN_URL, "created", Some("my app")),
            "https://docs.coderhelm.com/self-host/install/?github=created&app=my%20app"
        );
        assert_eq!(
            return_url("http://localhost:3000/x?a=1", "bad_state", None),
            "http://localhost:3000/x?a=1&github=bad_state"
        );
    }
}
