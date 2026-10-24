//! The install's version and updates (self-hosted installs).
//!
//! Install admins (`PLATFORM_ADMIN_EMAILS`) who are also team admins see
//! which release runs, whether a newer one is out, and its notes; they turn
//! auto-update on or off, start an update, roll back, and send a test email.
//! The decisions and the history live in the updater Lambda
//! (lambda/updater/handler.py); these routes check who is asking and pass
//! the request on. The hosted deployment has no updater: the version route
//! answers with the build's own stamp and `self_hosted: false`.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use tracing::{error, warn};

use super::api::is_platform_admin;
use crate::models::Claims;
use crate::AppState;

/// The updater's function name, when this install has one.
fn updater() -> Option<String> {
    std::env::var("UPDATER_FUNCTION_NAME")
        .ok()
        .filter(|n| !n.trim().is_empty())
}

/// What this build says about itself (stamped at deploy).
pub fn stamp() -> Value {
    let v = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    json!({
        "version": v("CODERHELM_VERSION"),
        "commit": v("CODERHELM_COMMIT"),
        "released_at": v("CODERHELM_RELEASED_AT"),
        "config_fingerprint": v("CODERHELM_CONFIG_FINGERPRINT"),
    })
}

/// An install admin, or 403 (each handler checks the team admin role first).
fn allowed(claims: &Claims) -> Result<(), StatusCode> {
    if !is_platform_admin(claims) {
        warn!(email = %claims.email, "System route refused: not an install admin");
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

/// The updater's answer, or the status code that stands for its failure.
async fn call(state: &AppState, payload: Value) -> Result<Value, StatusCode> {
    let Some(name) = updater() else {
        return Err(StatusCode::NOT_FOUND);
    };
    let resp = state
        .lambda
        .invoke()
        .function_name(name)
        .payload(aws_sdk_lambda::primitives::Blob::new(payload.to_string()))
        .send()
        .await
        .map_err(|e| {
            error!(error = %aws_sdk_lambda::error::DisplayErrorContext(&e), "updater invoke failed");
            StatusCode::BAD_GATEWAY
        })?;
    if let Some(err) = resp.function_error() {
        error!(err, "updater failed");
        return Err(StatusCode::BAD_GATEWAY);
    }
    let body: Value = resp
        .payload()
        .and_then(|p| serde_json::from_slice(p.as_ref()).ok())
        .unwrap_or(Value::Null);
    Ok(body)
}

/// The updater's `{ok, error}` as an HTTP answer: a refused request is a 400.
fn answer(body: Value) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if body.get("ok").and_then(Value::as_bool) == Some(false) {
        let msg = body.get("error").cloned().unwrap_or(json!("refused"));
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))));
    }
    Ok(Json(body))
}

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn status_only(s: StatusCode) -> (StatusCode, Json<Value>) {
    (s, Json(json!({})))
}

/// GET /api/system/version: this build, the latest release, settings, history.
pub async fn version(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims).map_err(status_only)?;
    if updater().is_none() {
        return Ok(Json(json!({
            "self_hosted": false,
            "current": stamp(),
            "latest": null,
            "update_available": false,
        })));
    }
    let body = call(&state, json!({ "action": "status" }))
        .await
        .map_err(status_only)?;
    let mut out = answer(body)?.0;
    out["self_hosted"] = json!(true);
    Ok(Json(out))
}

/// The settings a request may change, nothing else.
pub fn settings_from(body: &Value) -> Result<Value, &'static str> {
    let Some(obj) = body.as_object() else {
        return Err("send an object of settings");
    };
    const KEYS: &[&str] = &["auto", "channel", "day", "hour", "timezone", "majors"];
    if let Some(k) = obj.keys().find(|k| !KEYS.contains(&k.as_str())) {
        let _ = k;
        return Err("unknown setting");
    }
    Ok(Value::Object(obj.clone()))
}

/// PUT /api/system/auto-update: on or off, channel, window, majors.
pub async fn set_auto_update(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims).map_err(status_only)?;
    let settings =
        settings_from(&body).map_err(|m| (StatusCode::BAD_REQUEST, Json(json!({ "error": m }))))?;
    let body = call(
        &state,
        json!({ "action": "settings", "settings": settings, "actor": claims.email }),
    )
    .await
    .map_err(status_only)?;
    answer(body)
}

/// POST /api/system/update: update to `version` (default: the latest).
pub async fn update_now(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims).map_err(status_only)?;
    let version = body
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_string);
    let body = call(
        &state,
        json!({ "action": "update_now", "version": version, "actor": claims.email }),
    )
    .await
    .map_err(status_only)?;
    answer(body)
}

/// POST /api/system/rollback: back to the version before the last update.
pub async fn rollback(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims).map_err(status_only)?;
    let body = call(
        &state,
        json!({ "action": "rollback", "actor": claims.email }),
    )
    .await
    .map_err(status_only)?;
    answer(body)
}

/// POST /api/system/test-email: sends the caller a test email from the
/// install's sender (the setup checklist's email check).
pub async fn test_email(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(status_only)?;
    allowed(&claims).map_err(status_only)?;
    use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
    let text = Content::builder()
        .data("Email from your CoderHelm install works. Run results, invites and update notices will arrive like this one.")
        .build()
        .map_err(|_| status_only(StatusCode::INTERNAL_SERVER_ERROR))?;
    let subject = Content::builder()
        .data("CoderHelm test email")
        .build()
        .map_err(|_| status_only(StatusCode::INTERNAL_SERVER_ERROR))?;
    let message = Message::builder()
        .subject(subject)
        .body(Body::builder().text(text).build())
        .build();
    match state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(Destination::builder().to_addresses(&claims.email).build())
        .content(EmailContent::builder().simple(message).build())
        .send()
        .await
    {
        Ok(_) => Ok(Json(json!({ "sent": true, "to": claims.email }))),
        Err(e) => {
            let reason = aws_sdk_sesv2::error::DisplayErrorContext(&e).to_string();
            warn!(reason = %reason, "test email not sent");
            // The usual reasons: an unverified sender, or the SES sandbox.
            Ok(Json(
                json!({ "sent": false, "reason": short_reason(&reason) }),
            ))
        }
    }
}

/// A plain reason for a failed send, without the SDK's noise.
pub fn short_reason(raw: &str) -> &'static str {
    let r = raw.to_ascii_lowercase();
    if r.contains("not verified") || r.contains("sandbox") {
        "The sender or the recipient isn't verified in Amazon SES (new accounts start in the SES sandbox)."
    } else if r.contains("accessdenied") || r.contains("not authorized") {
        "The service isn't allowed to send from this address."
    } else {
        "Amazon SES refused the email. See the gateway log for the reason."
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_settings_pass() {
        assert!(settings_from(&json!({"auto": true, "hour": 3})).is_ok());
        assert_eq!(
            settings_from(&json!({"auto": true, "cmd": "rm"})),
            Err("unknown setting")
        );
        assert_eq!(
            settings_from(&json!([1])),
            Err("send an object of settings")
        );
    }

    #[test]
    fn refusals_from_the_updater_become_400s() {
        let r = answer(json!({"ok": false, "error": "an update is already running"}));
        let (code, body) = r.unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(body.0["error"], "an update is already running");
        assert!(answer(json!({"ok": true, "version": "v1.2.0"})).is_ok());
    }

    #[test]
    fn send_failures_read_plainly() {
        assert!(
            short_reason("MessageRejected: Email address is not verified").contains("SES sandbox")
        );
        assert!(short_reason("AccessDeniedException").contains("isn't allowed"));
        assert!(short_reason("boom").contains("gateway log"));
    }

    #[test]
    fn the_stamp_reads_the_deploy_env() {
        // Unset in tests: every field is null, never a made-up version.
        let s = stamp();
        assert!(s["version"].is_null() || s["version"].is_string());
    }
}
