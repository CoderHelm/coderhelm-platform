//! Release evidence from self-hosted installs (`/hybrid/releases/evidence`,
//! signed as the install). An install whose admin turned the report on
//! (the updater's `report` setting) says how a release did there: the
//! version, how the post-update watch ended, and how many runs and Chat
//! answers finished on it. Stable promotion reads these rows
//! (.github/scripts/auto_release.py): a beta becomes stable only once a
//! live install shows it installed, healthy and doing real work.
//!
//! Only these fields are accepted (anything else is refused): no team, no
//! person, no repository, no text. Stored on the settings table, one row per
//! install and version: `pk` = [`PK`], `sk` = `<version>#<install id>`,
//! expiring [`KEEP_DAYS`] after the last report.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use super::hybrid::{self, fail};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// The partition every report lives in (one Query reads them all).
pub const PK: &str = "RELEASE_EVIDENCE";
const KEEP_DAYS: i64 = 180;
/// How a post-update watch can end, as an install reports it.
pub const WATCH: &[&str] = &["healthy", "rolled_back"];

/// One report, exactly these fields.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub version: String,
    pub watch: String,
    pub runs: u32,
    pub answers: u32,
    /// This install uses Chat (an answer finished on some release).
    pub chat: bool,
}

/// Why a report is refused, or None when it is fine.
pub fn problem(r: &Report) -> Option<&'static str> {
    if common::release_use::version_of(&r.version).as_deref() != Some(r.version.as_str()) {
        return Some("version");
    }
    if !WATCH.contains(&r.watch.as_str()) {
        return Some("watch");
    }
    None
}

/// The row a report is kept as.
pub fn item(
    install: &str,
    r: &Report,
    at: &str,
    now_secs: i64,
) -> std::collections::HashMap<String, AttributeValue> {
    let s = |v: &str| AttributeValue::S(v.to_string());
    let n = |v: i64| AttributeValue::N(v.to_string());
    std::collections::HashMap::from([
        ("pk".to_string(), s(PK)),
        ("sk".to_string(), s(&format!("{}#{install}", r.version))),
        ("version".to_string(), s(&r.version)),
        ("watch".to_string(), s(&r.watch)),
        ("runs".to_string(), n(r.runs.into())),
        ("answers".to_string(), n(r.answers.into())),
        ("chat".to_string(), AttributeValue::Bool(r.chat)),
        ("at".to_string(), s(at)),
        ("ttl".to_string(), n(now_secs + KEEP_DAYS * 86_400)),
    ])
}

/// POST /hybrid/releases/evidence (signed).
pub async fn evidence(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Reply {
    let install = hybrid::signed(&state, &headers, &method, &uri, &body).await?;
    let r: Report = serde_json::from_slice(&body).map_err(|_| {
        fail(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "Send version, watch, runs, answers and chat only.",
        )
    })?;
    if let Some(why) = problem(&r) {
        return Err(fail(StatusCode::BAD_REQUEST, "bad_request", why));
    }
    let now = chrono::Utc::now();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(item(
            &install.id,
            &r,
            &now.to_rfc3339(),
            now.timestamp(),
        )))
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "Release evidence not stored");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "not_stored",
                "Not stored; send it again later.",
            )
        })?;
    info!(version = %r.version, watch = %r.watch, "Release evidence from an install");
    Ok(Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(json: &str) -> Result<Report, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn a_report_is_counts_and_the_watch_only() {
        let r = report(
            r#"{"version":"v0.9.1-beta.20300101","watch":"healthy","runs":3,"answers":2,"chat":true}"#,
        )
        .unwrap();
        assert_eq!(problem(&r), None);
        // Anything that could say who runs it is refused outright.
        for extra in [
            r#","team":"t""#,
            r#","repo":"harbor/harbor""#,
            r#","text":"x""#,
        ] {
            let j = format!(
                r#"{{"version":"v0.9.1","watch":"healthy","runs":1,"answers":0,"chat":false{extra}}}"#
            );
            assert!(report(&j).is_err(), "{extra}");
        }
        let row = item("inst_1", &r, "2030-01-01T00:00:00Z", 0);
        let mut keys: Vec<&String> = row.keys().collect();
        keys.sort();
        assert_eq!(
            keys,
            ["answers", "at", "chat", "pk", "runs", "sk", "ttl", "version", "watch"]
        );
        assert_eq!(row["sk"].as_s().unwrap(), "v0.9.1-beta.20300101#inst_1");
    }

    #[test]
    fn bad_values_are_refused() {
        let mut r =
            report(r#"{"version":"v1.2.3","watch":"healthy","runs":0,"answers":0,"chat":false}"#)
                .unwrap();
        r.watch = "watching".into();
        assert_eq!(problem(&r), Some("watch"));
        r.watch = "rolled_back".into();
        assert_eq!(problem(&r), None);
        r.version = "latest".into();
        assert_eq!(problem(&r), Some("version"));
        assert!(report(
            r#"{"version":"v1.2.3","watch":"healthy","runs":-1,"answers":0,"chat":false}"#
        )
        .is_err());
    }
}
