//! Which version of the Coderhelm Jira app a site runs, and what it lacks.
//!
//! Forge rolls a minor version out to every site by itself, but a site on an
//! older major version stays there until its Jira admin accepts the update.
//! Such a site keeps working, minus everything built since, and nobody can
//! tell from Jira. So the app reports its own version (Forge's
//! `appVersion`, `coderhelm.app_version` on every event and `app_version` on
//! every registration), the gateway keeps the latest seen per site in the
//! team's `JIRA#config`, and the release workflow publishes the version it
//! deployed (`launch/jira-app.json` in the public bucket, beside the
//! platform's own release pointers). [`assess`] compares the two.
//!
//! A site that sends no version at all runs a build from before the app
//! reported it. On the hosted app every site on the current major version
//! gets the reporting build within minutes of its release, so a site still
//! silent after that is on an older major version, whose builds lack every
//! feature in the table.
//!
//! The table (`jira_app_features.json`, read by the release workflow too)
//! names the first released version with each feature, from the app's git
//! history and the version each release run deployed.

use std::collections::HashMap;
use std::fmt;
use std::sync::OnceLock;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_json::Value;

/// `JIRA#config` field: the app version the site last reported, normalized
/// (`6.11.0`), or empty when its events carry none.
pub const FIELD_VERSION: &str = "app_version";
/// `JIRA#config` field: when an event or registration from the site last
/// arrived (RFC 3339). Absent until the first one after this change.
pub const FIELD_SEEN_AT: &str = "app_version_seen_at";
/// `JIRA#config` field: the Forge app the site runs (its ARI), when sent.
pub const FIELD_APP_ID: &str = "app_id";

/// Where the release workflow publishes the production version it deployed.
pub const DEFAULT_RELEASE_URL: &str =
    "https://coderhelm-public.s3.amazonaws.com/launch/jira-app.json";

/// How long a stored sighting stays fresh: the same version seen again
/// within this time is not written again.
pub const SEEN_REFRESH_SECS: i64 = 3600;

/// A Forge app version: `major.minor.patch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(pub u64, pub u64, pub u64);

impl Version {
    /// `6.10.0`, `v6.10` or `6`; anything else (empty, words, a fourth
    /// part, a pre-release suffix) is `None`.
    pub fn parse(s: &str) -> Option<Version> {
        let s = s.trim();
        let s = s.strip_prefix(['v', 'V']).unwrap_or(s);
        if s.is_empty() {
            return None;
        }
        let mut parts = [0u64; 3];
        for (n, p) in s.split('.').enumerate() {
            if n == 3 || p.is_empty() || p.len() > 9 || !p.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            parts[n] = p.parse().ok()?;
        }
        Some(Version(parts[0], parts[1], parts[2]))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

/// One feature and the first version that has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    pub since: Version,
    pub name: String,
}

/// The table, oldest first.
pub fn features() -> &'static [Feature] {
    static TABLE: OnceLock<Vec<Feature>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let rows: Vec<Value> =
            serde_json::from_str(include_str!("jira_app_features.json")).unwrap_or_default();
        let mut out: Vec<Feature> = rows
            .iter()
            .filter_map(|r| {
                Some(Feature {
                    since: Version::parse(r.get("since")?.as_str()?)?,
                    name: r.get("feature")?.as_str()?.to_string(),
                })
            })
            .collect();
        out.sort_by_key(|f| f.since);
        out
    })
}

/// What a site on `seen` lacks: every feature newer than it, or all of them
/// when the version is unknown (a build from before the app reported it).
pub fn missing_since(seen: Option<Version>) -> Vec<String> {
    features()
        .iter()
        .filter(|f| seen.is_none_or(|v| f.since > v))
        .map(|f| f.name.clone())
        .collect()
}

/// The production version the release workflow last deployed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Latest {
    pub version: Version,
    /// The Forge app it belongs to; a site running another app (a
    /// self-hosted copy) is never compared with it.
    pub app_id: String,
}

impl Latest {
    /// From the published pointer: `{"version": "6.11.0", "appId": "ari:..."}`.
    pub fn parse(body: &str) -> Option<Latest> {
        let v: Value = serde_json::from_str(body).ok()?;
        Some(Latest {
            version: Version::parse(v.get("version")?.as_str()?)?,
            app_id: v
                .get("appId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string(),
        })
    }
}

/// What the gateway last saw from a site.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Seen {
    /// Normalized, or empty when the site sent none.
    pub version: String,
    pub app_id: String,
    pub seen_at: String,
}

fn field(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

impl Seen {
    /// From `JIRA#config`; `None` when no event has arrived since sites
    /// started being tracked (nothing is known about the site).
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<Seen> {
        let seen_at = field(item, FIELD_SEEN_AT);
        if seen_at.is_empty() {
            return None;
        }
        Some(Seen {
            version: field(item, FIELD_VERSION),
            app_id: field(item, FIELD_APP_ID),
            seen_at,
        })
    }

    /// From what the app sent: the version normalized (empty when absent or
    /// not a version), and the app id.
    pub fn reported(version: Option<&str>, app_id: Option<&str>, now: &str) -> Seen {
        Seen {
            version: version
                .and_then(Version::parse)
                .map(|v| v.to_string())
                .unwrap_or_default(),
            app_id: app_id.unwrap_or("").trim().chars().take(200).collect(),
            seen_at: now.to_string(),
        }
    }

    /// The version and app id from a Forge event (`coderhelm.app_version`,
    /// `coderhelm.app_id`) or registration (`app_version`, `app_id`).
    pub fn from_payload(payload: &Value, now: &str) -> Seen {
        let get = |a: &str, b: &str| {
            payload
                .pointer(a)
                .or_else(|| payload.pointer(b))
                .and_then(Value::as_str)
        };
        Seen::reported(
            get("/coderhelm/app_version", "/app_version"),
            get("/coderhelm/app_id", "/app_id"),
            now,
        )
    }

    /// Whether `self` (just reported) must be written over `stored`: a
    /// different version or app, or the stored sighting is old.
    pub fn needs_write(&self, stored: Option<&Seen>, now: chrono::DateTime<chrono::Utc>) -> bool {
        let Some(s) = stored else { return true };
        if s.version != self.version || (!self.app_id.is_empty() && s.app_id != self.app_id) {
            return true;
        }
        chrono::DateTime::parse_from_rfc3339(&s.seen_at)
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() >= SEEN_REFRESH_SECS)
            .unwrap_or(true)
    }
}

/// A site behind the latest release, and what to tell the team.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Behind {
    /// The version the site runs; empty when it reports none.
    pub version: String,
    /// The latest released version; empty when the pointer couldn't be read.
    pub latest: String,
    /// What the site's version lacks, from the table.
    pub missing: Vec<String>,
    /// The site's Manage apps page, where its admin accepts the update.
    pub manage_url: String,
    /// When the site was last heard from.
    pub seen_at: String,
    /// The Integrations page's notice.
    pub message: String,
    /// The run page's line, for a run on a ticket from the site.
    pub run_note: String,
}

/// The site's Manage apps page (Jira settings, Apps, Manage apps), or empty
/// when the site isn't an Atlassian Cloud address.
pub fn manage_apps_url(site: &str) -> String {
    crate::live::confluence::check_site(site)
        .map(|s| format!("{s}/plugins/servlet/upm"))
        .unwrap_or_default()
}

/// The admin's steps in Jira, said the same way everywhere.
pub const ADMIN_STEPS: &str =
    "A Jira admin needs to accept the update: Jira Settings, Apps, Manage apps, Coderhelm, Update.";

fn running(version: &str, latest: &str) -> String {
    let v = if version.is_empty() {
        "a version from before the app reported its number".to_string()
    } else {
        format!("version {version}")
    };
    if latest.is_empty() {
        format!("({v})")
    } else {
        format!("({v}, latest {latest})")
    }
}

/// Whether the site is behind, from what it last reported and the latest
/// release. `None` when it is current, never seen, runs another app than
/// the published one, or reports a version that can't be compared.
pub fn assess(seen: Option<&Seen>, latest: Option<&Latest>, site: &str) -> Option<Behind> {
    let seen = seen?;
    let reported = Version::parse(&seen.version);
    let missing = match reported {
        // Before the app reported its version: behind, whatever the pointer says.
        None => missing_since(None),
        Some(v) => {
            let l = latest?;
            let other_app =
                !l.app_id.is_empty() && !seen.app_id.is_empty() && l.app_id != seen.app_id;
            if other_app || v >= l.version {
                return None;
            }
            missing_since(Some(v))
        }
    };
    let version = reported.map(|v| v.to_string()).unwrap_or_default();
    let latest = latest.map(|l| l.version.to_string()).unwrap_or_default();
    let what = running(&version, &latest);
    let lacks = if missing.is_empty() {
        String::new()
    } else {
        format!(" Missing: {}.", missing.join(", "))
    };
    let message =
        format!("Your Jira site runs an older Coderhelm app {what}.{lacks} {ADMIN_STEPS}");
    let run_note = if missing.is_empty() {
        format!(
            "Comments on this ticket may be incomplete: its Jira site runs an older Coderhelm app {what}. A Jira admin can accept the update in Manage apps."
        )
    } else {
        format!(
            "Comments on this ticket may be incomplete: its Jira site runs an older Coderhelm app {what} without {}. A Jira admin can accept the update in Manage apps.",
            missing.join(", ")
        )
    };
    Some(Behind {
        version,
        latest,
        missing,
        manage_url: manage_apps_url(site),
        seen_at: seen.seen_at.clone(),
        message,
        run_note,
    })
}

/// What a site on major version `major` lacks (the release workflow knows
/// only each installation's major version).
pub fn missing_on_major(major: u64) -> Vec<String> {
    features()
        .iter()
        .filter(|f| f.since.0 > major)
        .map(|f| f.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const APP: &str = "ari:cloud:ecosystem::app/00000000-0000-0000-0000-000000000001";
    const SITE: &str = "https://yourcompany.atlassian.net";

    fn latest(v: &str) -> Latest {
        Latest {
            version: Version::parse(v).unwrap(),
            app_id: APP.into(),
        }
    }

    fn seen(v: &str) -> Seen {
        Seen {
            version: v.into(),
            app_id: APP.into(),
            seen_at: "2026-11-14T10:00:00Z".into(),
        }
    }

    #[test]
    fn versions_parse_and_compare_by_number() {
        assert_eq!(Version::parse("6.10.0"), Some(Version(6, 10, 0)));
        assert_eq!(Version::parse(" v6.9 "), Some(Version(6, 9, 0)));
        assert_eq!(Version::parse("7"), Some(Version(7, 0, 0)));
        for bad in [
            "",
            "v",
            "6.x",
            "6..1",
            "1.2.3.4",
            "6.1.0-rc.1",
            "latest",
            "-1",
        ] {
            assert_eq!(Version::parse(bad), None, "{bad}");
        }
        assert!(
            Version::parse("6.10.0") > Version::parse("6.9.0"),
            "numbers, not text"
        );
        assert!(Version::parse("5.20.0") < Version::parse("6.0.0"));
        assert_eq!(Version(6, 10, 0).to_string(), "6.10.0");
    }

    #[test]
    fn the_table_is_read_and_each_version_lacks_what_came_after_it() {
        let names: Vec<&str> = features().iter().map(|f| f.name.as_str()).collect();
        for f in [
            "full comment reading",
            "mentions",
            "in-place edits",
            "threaded replies",
            "issue type",
            "extended issue fields",
        ] {
            assert!(names.contains(&f), "{f} is in the table");
        }
        assert_eq!(
            missing_since(None).len(),
            features().len(),
            "no version: all of it"
        );
        assert_eq!(
            missing_since(Version::parse("6.5.0")).len(),
            features().len()
        );
        let at_8 = missing_since(Version::parse("6.8.0"));
        assert!(!at_8.contains(&"full comment reading".to_string()));
        assert!(!at_8.contains(&"issue type".to_string()));
        assert!(at_8.contains(&"mentions".to_string()));
        assert!(at_8.contains(&"threaded replies".to_string()));
        assert_eq!(
            missing_since(Version::parse("6.10.0")),
            Vec::<String>::new()
        );
        assert_eq!(missing_on_major(5).len(), features().len());
        assert!(missing_on_major(6).is_empty());
    }

    #[test]
    fn an_older_version_is_behind_and_says_what_it_lacks() {
        let b = assess(Some(&seen("6.8.0")), Some(&latest("6.11.0")), SITE).unwrap();
        assert_eq!(b.version, "6.8.0");
        assert_eq!(b.latest, "6.11.0");
        assert_eq!(
            b.manage_url,
            "https://yourcompany.atlassian.net/plugins/servlet/upm"
        );
        assert!(b.message.starts_with(
            "Your Jira site runs an older Coderhelm app (version 6.8.0, latest 6.11.0). Missing: "
        ));
        assert!(b.message.contains("threaded replies"));
        assert!(b.message.ends_with(
            "A Jira admin needs to accept the update: Jira Settings, Apps, Manage apps, Coderhelm, Update."
        ));
        assert!(b
            .run_note
            .starts_with("Comments on this ticket may be incomplete"));
        assert!(!b.message.contains('→') && !b.run_note.contains('→'));
    }

    #[test]
    fn a_site_that_reports_no_version_is_behind_with_everything_missing() {
        let b = assess(Some(&seen("")), Some(&latest("6.11.0")), SITE).unwrap();
        assert_eq!(b.version, "");
        assert_eq!(b.missing.len(), features().len());
        assert!(b
            .message
            .contains("(a version from before the app reported its number, latest 6.11.0)"));
        // Even without the pointer.
        let b = assess(Some(&seen("")), None, SITE).unwrap();
        assert_eq!(b.latest, "");
        assert!(b
            .message
            .contains("(a version from before the app reported its number)."));
    }

    #[test]
    fn current_unknown_or_another_app_is_not_behind() {
        assert_eq!(
            assess(Some(&seen("6.11.0")), Some(&latest("6.11.0")), SITE),
            None
        );
        assert_eq!(
            assess(Some(&seen("6.12.0")), Some(&latest("6.11.0")), SITE),
            None,
            "pointer behind"
        );
        assert_eq!(
            assess(None, Some(&latest("6.11.0")), SITE),
            None,
            "never seen"
        );
        assert_eq!(
            assess(Some(&seen("6.8.0")), None, SITE),
            None,
            "nothing to compare with"
        );
        let mut copy = seen("1.2.0");
        copy.app_id = "ari:cloud:ecosystem::app/00000000-0000-0000-0000-000000000002".into();
        assert_eq!(
            assess(Some(&copy), Some(&latest("6.11.0")), SITE),
            None,
            "a self-hosted copy"
        );
        // Behind the latest, but newer than every feature: no Missing sentence.
        let b = assess(Some(&seen("6.11.0")), Some(&latest("6.12.0")), SITE).unwrap();
        assert!(b.missing.is_empty());
        assert!(!b.message.contains("Missing"));
    }

    #[test]
    fn the_pointer_and_payloads_are_read() {
        let l = Latest::parse(r#"{"version":"6.11.0","appId":" x ","publishedAt":"t"}"#).unwrap();
        assert_eq!(l.version, Version(6, 11, 0));
        assert_eq!(l.app_id, "x");
        assert_eq!(Latest::parse(r#"{"version":"soon"}"#), None);
        assert_eq!(Latest::parse("<html>"), None);

        let event = serde_json::json!({"coderhelm": {"app_version": "6.11.0", "app_id": APP}});
        let s = Seen::from_payload(&event, "now");
        assert_eq!((s.version.as_str(), s.app_id.as_str()), ("6.11.0", APP));
        let register = serde_json::json!({"app_version": "v6.12", "app_id": APP});
        assert_eq!(Seen::from_payload(&register, "now").version, "6.12.0");
        let old = serde_json::json!({"coderhelm": {"team_id": "t"}});
        assert_eq!(Seen::from_payload(&old, "now").version, "");
        let junk = serde_json::json!({"coderhelm": {"app_version": "<script>"}});
        assert_eq!(Seen::from_payload(&junk, "now").version, "");
    }

    #[test]
    fn a_sighting_is_written_when_it_changes_or_is_old() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-14T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let stored = seen("6.11.0");
        let mut same = seen("6.11.0");
        same.seen_at = "later".into();
        assert!(
            !same.needs_write(Some(&stored), now),
            "same, 30 minutes ago"
        );
        assert!(seen("6.12.0").needs_write(Some(&stored), now));
        assert!(same.needs_write(None, now));
        let later = now + chrono::Duration::hours(2);
        assert!(same.needs_write(Some(&stored), later));

        let mut item = HashMap::new();
        assert_eq!(Seen::from_item(&item), None);
        item.insert(
            FIELD_SEEN_AT.into(),
            AttributeValue::S("2026-11-14T10:00:00Z".into()),
        );
        item.insert(FIELD_VERSION.into(), AttributeValue::S("6.11.0".into()));
        item.insert(FIELD_APP_ID.into(), AttributeValue::S(APP.into()));
        assert_eq!(Seen::from_item(&item), Some(stored));
    }

    #[test]
    fn manage_apps_is_on_the_site_only() {
        assert_eq!(
            manage_apps_url("https://YourCompany.atlassian.net/"),
            "https://yourcompany.atlassian.net/plugins/servlet/upm"
        );
        assert_eq!(manage_apps_url("https://example.com"), "");
        assert_eq!(manage_apps_url(""), "");
    }
}
