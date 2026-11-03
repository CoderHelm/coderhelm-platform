//! The sender name an install admin sets in Settings, Emails: who the
//! install's email says it is from.
//!
//! Two values, both optional, stored at [`SETTINGS_PK`]/[`SENDER_SK`]:
//!
//! - the sender name, the whole From name on the install's own Amazon SES
//!   (`Acme Robots`), used as typed once it passes [`clean_sender_name`];
//! - the organization, the label CoderHelm's relay puts in front of
//!   "CoderHelm" in the From name and subjects (`Acme CoderHelm: ...`). It
//!   must pass [`naming::org_label`] here, and the relay checks it again.
//!
//! Either one, once set, wins over the config's `EMAIL_FROM_NAME` and
//! `ORGANIZATION_NAME`; with neither set nothing changes ([`ses_name`],
//! [`relay_org`]).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use super::{naming, SETTINGS_PK};

pub const SENDER_SK: &str = "SETTINGS#EMAIL_SENDER";

/// The most characters a sender name keeps.
pub const SENDER_MAX: usize = 64;

/// The stored sender name and organization. Empty: not set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SenderName {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub organization: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
}

/// Characters that are invisible or turn text around (zero-width spaces,
/// direction overrides), which would let a name read differently than it
/// is written.
fn invisible(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}'
            | '\u{061C}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2069}'
            | '\u{FEFF}'
    )
}

/// A sender name for the install's own SES, tidied (spacing collapsed), or
/// why it can't be one. Empty clears it. Refused: line breaks and other
/// control or invisible characters, the quotes, backslashes and angle
/// brackets that would break the From header, more than [`SENDER_MAX`]
/// characters, and an AWS address.
pub fn clean_sender_name(raw: &str) -> Result<String, &'static str> {
    if raw.chars().any(|c| c.is_control() || invisible(c)) {
        return Err("The sender name must be one line, without control characters.");
    }
    if raw.chars().any(|c| matches!(c, '"' | '\\' | '<' | '>')) {
        return Err("The sender name can't contain quotes, backslashes or angle brackets.");
    }
    let s = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() > SENDER_MAX {
        return Err("The sender name can be at most 64 characters.");
    }
    if naming::is_aws_host(&s) {
        return Err("The sender name can't be an AWS address.");
    }
    Ok(s)
}

/// An organization label, tidied, or why CoderHelm's relay wouldn't use it
/// ([`naming::org_label`]). Empty clears it.
pub fn clean_organization(raw: &str) -> Result<String, &'static str> {
    if raw.trim().is_empty() {
        return Ok(String::new());
    }
    naming::org_label(raw).ok_or(
        "Use up to 40 letters, digits, spaces and - . & ' with no address, link or word that reads as CoderHelm's own mail (Support, Billing, Security, ...).",
    )
}

fn set(v: &str) -> Option<&str> {
    Some(v.trim()).filter(|v| !v.is_empty())
}

/// The install's own SES From name. In this order: the stored sender name,
/// the stored organization (`Acme CoderHelm`), the config's
/// `EMAIL_FROM_NAME` and `ORGANIZATION_NAME`, the install's own address
/// ([`naming::ses_display_name`]).
pub fn ses_name(
    stored: Option<&SenderName>,
    env_from_name: Option<&str>,
    env_org: Option<&str>,
    app_host: &str,
    coderhelm_zone: &str,
) -> String {
    let name = stored
        .and_then(|s| set(&s.sender_name))
        .and_then(|n| clean_sender_name(n).ok());
    let org = stored.and_then(|s| set(&s.organization));
    match (name.as_deref(), org) {
        (Some(n), _) => {
            naming::ses_display_name(Some(n), org.or(env_org), app_host, coderhelm_zone)
        }
        (None, Some(o)) => naming::ses_display_name(None, Some(o), app_host, coderhelm_zone),
        (None, None) => naming::ses_display_name(env_from_name, env_org, app_host, coderhelm_zone),
    }
}

/// The organization sent to CoderHelm's relay for the From name and
/// subjects: the stored organization, else the stored sender name with a
/// trailing "CoderHelm" dropped, else the config's ([`naming::org_from_env`]
/// passes it in). The relay applies [`naming::org_label`] to whatever comes.
pub fn relay_org(stored: Option<&SenderName>, env_org: Option<String>) -> Option<String> {
    let stored_org = stored
        .and_then(|s| set(&s.organization))
        .map(str::to_string);
    stored_org
        .or_else(|| naming::org_from(None, stored.and_then(|s| set(&s.sender_name))))
        .or(env_org)
}

pub fn from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<SenderName> {
    let data = item.get("data")?.as_s().ok()?;
    serde_json::from_str(data).ok()
}

static CACHE: Mutex<Option<(Instant, Option<SenderName>)>> = Mutex::new(None);
const CACHE_FOR: Duration = Duration::from_secs(30);

/// The stored sender name (cached 30 seconds).
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
) -> Result<Option<SenderName>, String> {
    if let Some((at, s)) = CACHE.lock().ok().and_then(|g| g.clone()) {
        if at.elapsed() < CACHE_FOR {
            return Ok(s);
        }
    }
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(SETTINGS_PK.into()))
        .key("sk", AttributeValue::S(SENDER_SK.into()))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("email sender name: {e}"))?;
    let s = r.item().and_then(from_item);
    if let Ok(mut g) = CACHE.lock() {
        *g = Some((Instant::now(), s.clone()));
    }
    Ok(s)
}

/// The stored sender name for a send. A store that can't be read leaves
/// the config's names in force rather than failing the email.
pub async fn load_for_send(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Option<SenderName> {
    load(dynamo, table).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Email sender name unreadable: the config's name is used");
        None
    })
}

/// Drops the cache (after an admin saves).
pub fn forget() {
    if let Ok(mut g) = CACHE.lock() {
        *g = None;
    }
}

pub async fn save(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    s: &SenderName,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(SETTINGS_PK.into()))
        .item("sk", AttributeValue::S(SENDER_SK.into()))
        .item(
            "data",
            AttributeValue::S(serde_json::to_string(s).unwrap_or_default()),
        )
        .send()
        .await
        .map_err(|e| format!("email sender name: {e}"))?;
    forget();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZONE: &str = "coderhelm.com";

    fn stored(name: &str, org: &str) -> SenderName {
        SenderName {
            sender_name: name.into(),
            organization: org.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_sender_name_that_would_break_the_header_is_refused() {
        for bad in [
            "Acme\r\nBcc: x@example.com",
            "Acme\nRobots",
            "Acme\rRobots",
            "Acme\u{0}",
            "Acme\tRobots",
            "Acme \u{202E}stobor",
            "Acme\u{200B}Robots",
            "\"Acme\" <x@example.com>",
            "Acme \"Robots\"",
            "Acme <Robots>",
            "Acme\\Robots",
            "d1example.cloudfront.net",
            "abc.execute-api.us-east-1.amazonaws.com",
        ] {
            assert!(clean_sender_name(bad).is_err(), "{bad:?}");
        }
        assert!(clean_sender_name(&"x".repeat(SENDER_MAX + 1)).is_err());
        assert_eq!(
            clean_sender_name(&"x".repeat(SENDER_MAX)).unwrap().len(),
            SENDER_MAX
        );
        for good in [
            "Acme Robots",
            "AT&T Builds",
            "O'Brien & Co.",
            "Société Générale",
        ] {
            assert_eq!(clean_sender_name(good).unwrap(), good, "{good}");
        }
        // Spacing is tidied, and empty clears it.
        assert_eq!(
            clean_sender_name("  Acme   Robots ").unwrap(),
            "Acme Robots"
        );
        assert_eq!(clean_sender_name("   ").unwrap(), "");
    }

    #[test]
    fn a_sender_name_keeps_the_rfc_2047_encoding() {
        let name = ses_name(Some(&stored("Société Générale", "")), None, None, "x", ZONE);
        let from = naming::from_header(&name, "ch@example.com");
        assert!(from.starts_with("=?UTF-8?B?"), "{from}");
        assert!(from.ends_with(" <ch@example.com>"));
    }

    #[test]
    fn the_organization_follows_the_relays_rules() {
        for bad in [
            "CoderHelm",
            "C0derHe1m Labs",
            "Acme Support",
            "Billing",
            "Security Team",
            "acme.com",
            "Acme\r\nBcc: x@y.com",
            "\u{0405}ecurity",
        ] {
            assert!(clean_organization(bad).is_err(), "{bad:?}");
        }
        assert_eq!(clean_organization(" Acme   Corp ").unwrap(), "Acme Corp");
        assert_eq!(clean_organization("").unwrap(), "");
    }

    #[test]
    fn the_setting_wins_over_the_config_which_wins_over_the_address() {
        let host = "acme.coderhelm.com";
        // Nothing set anywhere: the install's own address.
        assert_eq!(ses_name(None, None, None, host, ZONE), "acme CoderHelm");
        // The config, as before.
        assert_eq!(
            ses_name(None, Some("Config Name"), None, host, ZONE),
            "Config Name"
        );
        assert_eq!(
            ses_name(None, None, Some("Config Org"), host, ZONE),
            "Config Org CoderHelm"
        );
        // An empty stored setting changes nothing.
        assert_eq!(
            ses_name(Some(&stored("", "")), Some("Config Name"), None, host, ZONE),
            "Config Name"
        );
        // The stored organization over both config values.
        assert_eq!(
            ses_name(
                Some(&stored("", "Acme")),
                Some("Config Name"),
                Some("Config Org"),
                host,
                ZONE
            ),
            "Acme CoderHelm"
        );
        // The stored sender name over everything.
        assert_eq!(
            ses_name(
                Some(&stored("Acme Robots", "Acme")),
                Some("Config Name"),
                Some("Config Org"),
                host,
                ZONE
            ),
            "Acme Robots"
        );
        // A stored name that no longer passes (written before a rule
        // tightened) falls through instead of reaching the header.
        assert_eq!(
            ses_name(
                Some(&stored("A\r\nB", "")),
                Some("Config Name"),
                None,
                host,
                ZONE
            ),
            "Config Name"
        );
        // An AWS address still never becomes the name.
        assert_eq!(
            ses_name(Some(&stored("", "")), None, None, "d1.cloudfront.net", ZONE),
            "CoderHelm"
        );
    }

    #[test]
    fn the_relay_gets_the_stored_organization_first() {
        let env = || Some("Config Org".to_string());
        assert_eq!(relay_org(None, env()).as_deref(), Some("Config Org"));
        assert_eq!(relay_org(None, None), None);
        assert_eq!(
            relay_org(Some(&stored("", "")), env()).as_deref(),
            Some("Config Org")
        );
        assert_eq!(
            relay_org(Some(&stored("Acme Robots", "Acme")), env()).as_deref(),
            Some("Acme")
        );
        assert_eq!(
            relay_org(Some(&stored("Acme CoderHelm", "")), env()).as_deref(),
            Some("Acme")
        );
        assert_eq!(
            relay_org(Some(&stored("CoderHelm", "")), env()).as_deref(),
            Some("Config Org")
        );
    }

    #[test]
    fn the_relay_rules_still_apply_to_what_is_sent() {
        // Whatever the install sends, a name that reads as CoderHelm's own
        // mail never becomes the relay's label: the relay falls back to the
        // install's address.
        let org = relay_org(Some(&stored("CoderHelm Support", "")), None);
        assert_eq!(org.as_deref(), Some("CoderHelm Support"));
        assert_eq!(
            naming::label(org.as_deref(), None, Some("acme.coderhelm.com"), ZONE).as_deref(),
            Some("acme")
        );
        let org = relay_org(Some(&stored("", "Acme")), None);
        assert_eq!(
            naming::subject_prefix(
                naming::label(org.as_deref(), None, Some("acme.coderhelm.com"), ZONE).as_deref()
            ),
            "Acme CoderHelm: "
        );
    }

    #[test]
    fn it_round_trips_through_the_store_format() {
        let s = SenderName {
            sender_name: "Acme Robots".into(),
            organization: "Acme".into(),
            updated_by: "admin@example.com".into(),
            updated_at: "2026-11-03T00:00:00Z".into(),
        };
        let mut item = std::collections::HashMap::new();
        item.insert(
            "data".to_string(),
            AttributeValue::S(serde_json::to_string(&s).unwrap()),
        );
        assert_eq!(from_item(&item), Some(s));
        assert_eq!(serde_json::to_string(&SenderName::default()).unwrap(), "{}");
    }
}
