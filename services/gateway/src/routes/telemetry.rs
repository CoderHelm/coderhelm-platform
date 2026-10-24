//! Opt-in telemetry from self-hosted installs.
//!
//! The self-host installer sends one small ping per install, upgrade, health
//! check or uninstall, only when the operator turned telemetry on (it is off
//! by default). A ping says which release runs and how it is set up, never
//! who runs it: a random install id the installer made, the version, the
//! event, and a few setup choices. No IP address, account, domain, email or
//! repository is accepted or stored.
//!
//! Stored on the settings table, one item per install (pk TELEMETRY,
//! sk INSTALL#<id>), expiring 400 days after its last ping.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use std::sync::Arc;
use tracing::warn;

use crate::AppState;

const EVENTS: &[&str] = &["install", "upgrade", "health", "uninstall"];
const KEEP_DAYS: i64 = 400;

/// One ping, exactly these fields (anything else is refused).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ping {
    pub install_id: String,
    pub version: String,
    pub event: String,
    #[serde(default)]
    pub layout: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
}

/// Why a ping is refused, or None when it is fine.
pub fn problem(p: &Ping) -> Option<&'static str> {
    let uuid = p.install_id.len() == 36
        && p.install_id.chars().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        });
    if !uuid {
        return Some("install_id must be a UUID");
    }
    let short = |s: &str, n: usize| {
        !s.is_empty()
            && s.len() <= n
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-+".contains(c))
    };
    if !short(&p.version, 40) {
        return Some("version");
    }
    if !EVENTS.contains(&p.event.as_str()) {
        return Some("event");
    }
    if p.layout
        .as_deref()
        .is_some_and(|l| !["single-origin", "split"].contains(&l))
    {
        return Some("layout");
    }
    if p.provider
        .as_deref()
        .is_some_and(|m| !["anthropic", "bedrock"].contains(&m))
    {
        return Some("provider");
    }
    if p.region.as_deref().is_some_and(|r| !short(r, 20)) {
        return Some("region");
    }
    None
}

/// POST /api/telemetry/ping (public; the body is the whole credential-free ping).
pub async fn ping(State(state): State<Arc<AppState>>, Json(p): Json<Ping>) -> StatusCode {
    if let Some(why) = problem(&p) {
        warn!(why, "Telemetry ping refused");
        return StatusCode::BAD_REQUEST;
    }
    let now = chrono::Utc::now();
    let s = |v: &str| AttributeValue::S(v.to_string());
    let values = std::collections::HashMap::from([
        (":v".to_string(), s(&p.version)),
        (":e".to_string(), s(&p.event)),
        (":t".to_string(), s(&now.to_rfc3339())),
        (":one".to_string(), AttributeValue::N("1".into())),
        (
            ":ttl".to_string(),
            AttributeValue::N((now.timestamp() + KEEP_DAYS * 86_400).to_string()),
        ),
        (":l".to_string(), s(p.layout.as_deref().unwrap_or(""))),
        (":p".to_string(), s(p.provider.as_deref().unwrap_or(""))),
        (":r".to_string(), s(p.region.as_deref().unwrap_or(""))),
    ]);
    let result = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s("TELEMETRY"))
        .key("sk", s(&format!("INSTALL#{}", p.install_id)))
        .update_expression(
            "SET version = :v, last_event = :e, last_seen = :t, first_seen = if_not_exists(first_seen, :t), \
             layout = :l, provider = :p, aws_region = :r, #ttl = :ttl ADD pings :one",
        )
        .expression_attribute_names("#ttl", "ttl")
        .set_expression_attribute_values(Some(values))
        .send()
        .await;
    match result {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!(error = %e, "Telemetry ping not stored");
            StatusCode::NO_CONTENT
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ping(json: &str) -> Result<Ping, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn a_good_ping_passes() {
        let p = ping(r#"{"install_id":"0b9a5f1e-3c2d-4e5f-8a9b-0c1d2e3f4a5b","version":"v1.2.0","event":"install","layout":"single-origin","provider":"bedrock","region":"eu-west-1"}"#).unwrap();
        assert_eq!(problem(&p), None);
    }

    #[test]
    fn extra_fields_are_refused() {
        assert!(ping(r#"{"install_id":"0b9a5f1e-3c2d-4e5f-8a9b-0c1d2e3f4a5b","version":"v1","event":"install","email":"a@b.c"}"#).is_err());
    }

    #[test]
    fn bad_values_are_refused() {
        let base = |id: &str, v: &str, e: &str| Ping {
            install_id: id.into(),
            version: v.into(),
            event: e.into(),
            layout: None,
            provider: None,
            region: None,
        };
        let id = "0b9a5f1e-3c2d-4e5f-8a9b-0c1d2e3f4a5b";
        assert_eq!(
            problem(&base("x", "v1", "install")),
            Some("install_id must be a UUID")
        );
        assert_eq!(
            problem(&base(id, "v1 <script>", "install")),
            Some("version")
        );
        assert_eq!(problem(&base(id, "v1", "deploy")), Some("event"));
        let mut p = base(id, "v1", "upgrade");
        p.provider = Some("openai".into());
        assert_eq!(problem(&p), Some("provider"));
    }
}
