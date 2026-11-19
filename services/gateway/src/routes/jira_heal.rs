//! The Jira app's links put right by the system, never by a person.
//!
//! A site's app registers its trigger links when it is installed, when an
//! admin saves its settings, on a major update, and (from this change) on
//! the first event after any version change. A site that gained a trigger
//! before that (Forge rolls a minor version out without telling the app)
//! still lacks the link here, and a self-hosted install linked through
//! CoderHelm copied the hosted links once, when it linked.
//!
//! [`heal`] is what every reader that finds a link missing calls: it asks
//! the app, through its project-list trigger, to register all of its links
//! again (`common::jira_connection::REGISTER_AGAIN`), and on a linked
//! install copies the hosted settings again afterwards. It is cheap to call
//! often: once per team per [`QUIET`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use common::jira_connection::Connection;
use common::mcp_remote::http::{GuardedHttps, Http, HttpRequest, Limit};
use tracing::{info, warn};

use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// How long after one attempt the same team's next one waits.
pub const QUIET: Duration = Duration::from_secs(10 * 60);

static LAST: Mutex<Option<HashMap<String, Instant>>> = Mutex::new(None);

/// Whether this team may be tried now; marks it tried when so.
pub fn take_turn(team_id: &str, now: Instant) -> bool {
    let Ok(mut g) = LAST.lock() else {
        return false;
    };
    let map = g.get_or_insert_with(HashMap::new);
    match map.get(team_id) {
        Some(at) if now.duration_since(*at) < QUIET => false,
        _ => {
            map.insert(team_id.to_string(), now);
            true
        }
    }
}

/// Whether the team's record lacks a link the app always registers, or the
/// event reports another app version than the one last seen.
pub fn wants_heal(item: Option<&Item>, event_version: Option<&str>) -> bool {
    let Some(item) = item else {
        return false;
    };
    if !common::jira_connection::missing_registrations(item).is_empty() {
        return true;
    }
    let stored = item
        .get(common::jira_app_version::FIELD_VERSION)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.trim())
        .unwrap_or("");
    let reported = event_version
        .and_then(common::jira_app_version::Version::parse)
        .map(|v| v.to_string())
        .unwrap_or_default();
    !reported.is_empty() && !stored.is_empty() && reported != stored
}

/// Asks the app to register its links again (when it lacks one), then, on
/// a self-hosted install linked through CoderHelm, copies the hosted
/// settings again. True when anything was refreshed. At most once per team
/// per [`QUIET`]; never fails the caller.
pub async fn heal(state: &AppState, team_id: &str, item: Option<&Item>) -> bool {
    if !take_turn(team_id, Instant::now()) {
        return false;
    }
    let conn = Connection::from_config(item, false);
    let mut asked = false;
    if let Some((url, body)) = conn.register_again(item) {
        // On a linked install this is CoderHelm's proxy link: the site's app
        // registers with CoderHelm, which the copy below then picks up.
        match GuardedHttps
            .send(HttpRequest::json(&url, &body, Limit::Metadata))
            .await
        {
            Ok(r) if r.status == 200 => asked = true,
            Ok(r) => warn!(
                team_id,
                status = r.status,
                "Jira app: asking it to register again was refused"
            ),
            Err(e) => warn!(team_id, error = %e, "Jira app: couldn't ask it to register again"),
        }
    }
    let copied = super::hybrid_install::refresh_jira_copy(state).await;
    if asked || copied {
        info!(team_id, asked, copied, "Jira app: links refreshed");
    }
    asked || copied
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    #[test]
    fn a_missing_link_or_a_new_version_asks_for_a_heal() {
        let full = item(&[
            (
                "list_projects_url",
                "https://x.hello.atlassian-dev.net/x1/lp",
            ),
            (
                "create_ticket_url",
                "https://x.hello.atlassian-dev.net/x1/ct",
            ),
            ("add_comment_url", "https://x.hello.atlassian-dev.net/x1/ac"),
            ("get_issues_url", "https://x.hello.atlassian-dev.net/x1/gi"),
            ("forge_secret", "s"),
            ("app_version", "7.3.0"),
        ]);
        assert!(!wants_heal(Some(&full), Some("7.3.0")));
        assert!(!wants_heal(Some(&full), None));
        assert!(wants_heal(Some(&full), Some("7.4.0")));
        let mut lacking = full.clone();
        lacking.insert("get_issues_url".into(), AttributeValue::S(String::new()));
        assert!(wants_heal(Some(&lacking), Some("7.3.0")));
        assert!(!wants_heal(None, Some("7.4.0")));
    }

    #[test]
    fn one_try_per_team_per_quiet_spell() {
        let t0 = Instant::now();
        assert!(take_turn("TEAM#heal-a", t0));
        assert!(!take_turn("TEAM#heal-a", t0 + Duration::from_secs(60)));
        assert!(take_turn("TEAM#heal-b", t0));
        assert!(take_turn(
            "TEAM#heal-a",
            t0 + QUIET + Duration::from_secs(1)
        ));
    }
}
