//! Webhook endpoints: where a team's events go, which events, and the
//! signing secrets.
//!
//! One item per endpoint on the settings table: `pk` is the team, `sk` is
//! `WEBHOOK_ENDPOINT#<id>`. The signing secret is sealed with KMS, bound to
//! the team, the endpoint and its purpose (an item copied anywhere else will
//! not open), and only its last four characters are kept in the clear. A
//! rotated-out secret is kept, sealed the same way, until its overlap ends;
//! both sign until then.
//!
//! Health is kept on the item with conditional updates, so two deliveries
//! finishing at once never undo each other: `failing_since` is set by the
//! first failure after a success and removed by the next success.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};

use super::{catalog, sign};
use crate::mcp_credentials::Cipher;

pub const SK_PREFIX: &str = "WEBHOOK_ENDPOINT#";
pub const ID_PREFIX: &str = "ep_";
/// Endpoints one team may have.
pub const MAX_PER_TEAM: usize = 20;
pub const MAX_DESCRIPTION: usize = 200;
/// The default and longest overlap when a secret is rotated.
pub const DEFAULT_OVERLAP_HOURS: i64 = 24;
pub const MAX_OVERLAP_HOURS: i64 = 7 * 24;

type Item = HashMap<String, AttributeValue>;

/// Why an endpoint is off.
pub const DISABLED_MANUAL: &str = "manual";
pub const DISABLED_FAILING: &str = "failing";

/// An endpoint as the API shows it (no secrets).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Endpoint {
    pub id: String,
    pub url: String,
    pub description: String,
    /// Event types it receives; `["*"]` is every event.
    pub events: Vec<String>,
    pub enabled: bool,
    /// Why it's off: `manual` (a person) or `failing` (turned off after
    /// failing for days). Empty while on.
    #[serde(default)]
    pub disabled_reason: String,
    pub secret_hint: String,
    /// While a rotated-out secret still signs too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_secret_expires_at: Option<String>,
    /// The first failure since the last success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing_since: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub created_by: String,
    pub updated_at: String,
}

impl Endpoint {
    /// Receives this event type (on, and subscribed to it or to everything).
    pub fn wants(&self, event_type: &str) -> bool {
        self.enabled
            && event_type != catalog::TEST_EVENT
            && self.events.iter().any(|e| e == "*" || e == event_type)
    }

    pub fn host(&self) -> String {
        crate::public_urls::host_of(&self.url).to_string()
    }
}

/// An endpoint with its sealed secrets, as stored.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Stored {
    pub endpoint: Endpoint,
    pub secret_sealed: String,
    pub previous_secret_sealed: String,
}

/// The encryption context of an endpoint's secrets.
pub fn context(team_id: &str, endpoint_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("purpose".to_string(), "webhook-signing-secret".to_string()),
        ("endpoint".to_string(), endpoint_id.to_string()),
    ])
}

pub async fn seal(
    cipher: &dyn Cipher,
    team_id: &str,
    endpoint_id: &str,
    secret: &str,
) -> Result<String, String> {
    let blob = cipher
        .encrypt(context(team_id, endpoint_id), secret.as_bytes())
        .await?;
    Ok(STANDARD.encode(blob))
}

pub async fn open(
    cipher: &dyn Cipher,
    team_id: &str,
    endpoint_id: &str,
    sealed: &str,
) -> Result<String, String> {
    let bytes = STANDARD
        .decode(sealed)
        .map_err(|_| "the stored signing secret is not valid".to_string())?;
    let plain = cipher
        .decrypt(context(team_id, endpoint_id), &bytes)
        .await?;
    String::from_utf8(plain).map_err(|_| "the stored signing secret is not text".to_string())
}

/// The secrets that sign a request now: the current one, and the previous
/// one while its overlap lasts.
pub async fn signing_secrets(
    cipher: &dyn Cipher,
    team_id: &str,
    s: &Stored,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<String>, String> {
    let id = &s.endpoint.id;
    let mut out = vec![open(cipher, team_id, id, &s.secret_sealed).await?];
    if previous_live(s, now) {
        match open(cipher, team_id, id, &s.previous_secret_sealed).await {
            Ok(prev) => out.push(prev),
            Err(e) => {
                tracing::warn!(endpoint = %id, error = %e, "Could not open the previous signing secret")
            }
        }
    }
    Ok(out)
}

/// The rotated-out secret still signs.
pub fn previous_live(s: &Stored, now: chrono::DateTime<chrono::Utc>) -> bool {
    !s.previous_secret_sealed.is_empty()
        && s.endpoint
            .previous_secret_expires_at
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .is_some_and(|t| t > now)
}

/// What a person may set on an endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Input {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub events: Option<Vec<String>>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// A refused input, with the field and the words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Invalid {
    pub field: &'static str,
    pub message: String,
}

fn invalid(field: &'static str, message: impl Into<String>) -> Invalid {
    Invalid {
        field,
        message: message.into(),
    }
}

/// Checks a URL: https, no username or password, and not a private, local
/// or metadata address when written as one. The name is resolved and checked
/// again before every send ([`super::deliver`]).
pub fn check_url(raw: &str) -> Result<String, Invalid> {
    let ep = crate::live::net::parse_endpoint(raw).map_err(|e| invalid("url", e.message))?;
    Ok(ep.url.to_string())
}

/// Checks and normalizes a subscription list: known types, `*` alone.
pub fn check_events(events: &[String]) -> Result<Vec<String>, Invalid> {
    let mut list: Vec<String> = events
        .iter()
        .map(|e| e.trim().to_string())
        .filter(|e| !e.is_empty())
        .collect();
    list.sort();
    list.dedup();
    if list.is_empty() {
        return Err(invalid("events", "Pick at least one event, or all events."));
    }
    let unknown = catalog::unknown_types(&list);
    if !unknown.is_empty() {
        return Err(invalid(
            "events",
            format!("Not an event type: {}.", unknown.join(", ")),
        ));
    }
    if list.iter().any(|e| e == "*") {
        return Ok(vec!["*".into()]);
    }
    Ok(list)
}

fn check_description(d: &str) -> Result<String, Invalid> {
    let d = d.trim();
    if d.chars().count() > MAX_DESCRIPTION {
        return Err(invalid(
            "description",
            format!("Keep the description under {MAX_DESCRIPTION} characters."),
        ));
    }
    Ok(d.to_string())
}

/// A new endpoint from a create request (without its secret yet).
pub fn new_endpoint(input: &Input, by: &str, now: &str) -> Result<Endpoint, Invalid> {
    let url = check_url(input.url.as_deref().unwrap_or(""))?;
    let events = check_events(input.events.as_deref().unwrap_or(&["*".to_string()]))?;
    let description = check_description(input.description.as_deref().unwrap_or(""))?;
    let enabled = input.enabled.unwrap_or(true);
    Ok(Endpoint {
        id: format!(
            "{ID_PREFIX}{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        ),
        url,
        description,
        events,
        enabled,
        disabled_reason: if enabled {
            String::new()
        } else {
            DISABLED_MANUAL.into()
        },
        created_at: now.into(),
        created_by: by.into(),
        updated_at: now.into(),
        ..Default::default()
    })
}

/// Applies an update request to an endpoint. Turning one back on clears its
/// failure record, so it gets a fresh N days.
pub fn apply_input(e: &mut Endpoint, input: &Input, now: &str) -> Result<(), Invalid> {
    if let Some(u) = &input.url {
        e.url = check_url(u)?;
    }
    if let Some(d) = &input.description {
        e.description = check_description(d)?;
    }
    if let Some(ev) = &input.events {
        e.events = check_events(ev)?;
    }
    if let Some(on) = input.enabled {
        if on && !e.enabled {
            e.failing_since = None;
        }
        e.enabled = on;
        e.disabled_reason = if on {
            String::new()
        } else {
            DISABLED_MANUAL.into()
        };
    }
    e.updated_at = now.into();
    Ok(())
}

// ─── Items ──────────────────────────────────────────────────────────────────

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn get_s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn get_opt(item: &Item, k: &str) -> Option<String> {
    Some(get_s(item, k)).filter(|v| !v.is_empty())
}

pub fn sk(id: &str) -> String {
    format!("{SK_PREFIX}{id}")
}

/// The item for a stored endpoint.
pub fn to_item(team_id: &str, st: &Stored) -> Item {
    let e = &st.endpoint;
    let mut item = HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(&sk(&e.id))),
        ("id".to_string(), s(&e.id)),
        ("url".to_string(), s(&e.url)),
        ("description".to_string(), s(&e.description)),
        (
            "events".to_string(),
            AttributeValue::L(e.events.iter().map(|x| s(x)).collect()),
        ),
        ("enabled".to_string(), AttributeValue::Bool(e.enabled)),
        ("disabled_reason".to_string(), s(&e.disabled_reason)),
        ("secret_sealed".to_string(), s(&st.secret_sealed)),
        ("secret_hint".to_string(), s(&e.secret_hint)),
        ("created_at".to_string(), s(&e.created_at)),
        ("created_by".to_string(), s(&e.created_by)),
        ("updated_at".to_string(), s(&e.updated_at)),
    ]);
    let mut opt = |k: &str, v: &Option<String>| {
        if let Some(v) = v.as_deref().filter(|v| !v.is_empty()) {
            item.insert(k.to_string(), s(v));
        }
    };
    opt("prev_secret_expires_at", &e.previous_secret_expires_at);
    opt("failing_since", &e.failing_since);
    opt("last_success_at", &e.last_success_at);
    opt("last_failure_at", &e.last_failure_at);
    if !st.previous_secret_sealed.is_empty() {
        item.insert("prev_secret_sealed".into(), s(&st.previous_secret_sealed));
    }
    item
}

/// A stored endpoint from its item.
pub fn from_item(item: &Item) -> Option<Stored> {
    let id = get_s(item, "id");
    if id.is_empty() || !get_s(item, "sk").starts_with(SK_PREFIX) {
        return None;
    }
    let events = item
        .get("events")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    Some(Stored {
        endpoint: Endpoint {
            id,
            url: get_s(item, "url"),
            description: get_s(item, "description"),
            events,
            enabled: item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            disabled_reason: get_s(item, "disabled_reason"),
            secret_hint: get_s(item, "secret_hint"),
            previous_secret_expires_at: get_opt(item, "prev_secret_expires_at"),
            failing_since: get_opt(item, "failing_since"),
            last_success_at: get_opt(item, "last_success_at"),
            last_failure_at: get_opt(item, "last_failure_at"),
            created_at: get_s(item, "created_at"),
            created_by: get_s(item, "created_by"),
            updated_at: get_s(item, "updated_at"),
        },
        secret_sealed: get_s(item, "secret_sealed"),
        previous_secret_sealed: get_s(item, "prev_secret_sealed"),
    })
}

// ─── Storage ────────────────────────────────────────────────────────────────

fn err<E: std::fmt::Debug>(e: E) -> String {
    format!("{e:?}").chars().take(300).collect()
}

/// Every endpoint of a team.
pub async fn list(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<Vec<Stored>, String> {
    let r = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", s(team_id))
        .expression_attribute_values(":p", s(SK_PREFIX))
        .send()
        .await
        .map_err(err)?;
    let mut out: Vec<Stored> = r.items().iter().filter_map(from_item).collect();
    out.sort_by(|a, b| a.endpoint.created_at.cmp(&b.endpoint.created_at));
    Ok(out)
}

pub async fn get(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
) -> Result<Option<Stored>, String> {
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk(id)))
        .consistent_read(true)
        .send()
        .await
        .map_err(err)?;
    Ok(r.item().and_then(from_item))
}

/// Writes a whole endpoint (create and person-made changes).
pub async fn put(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    st: &Stored,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(to_item(team_id, st)))
        .send()
        .await
        .map(|_| ())
        .map_err(err)
}

pub async fn delete(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk(id)))
        .send()
        .await
        .map(|_| ())
        .map_err(err)
}

/// Notes a delivered attempt: the failure record ends.
pub async fn note_success(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
    at: &str,
) {
    let r = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk(id)))
        .update_expression("SET last_success_at = :t REMOVE failing_since")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":t", s(at))
        .send()
        .await;
    if let Err(e) = r {
        tracing::debug!(endpoint = id, error = %err(e), "Could not note a webhook success");
    }
}

/// Notes a failed attempt; returns when the failures began (this attempt,
/// when it is the first since a success). `None` when the endpoint is gone.
pub async fn note_failure(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
    at: &str,
) -> Option<String> {
    let r = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk(id)))
        .update_expression(
            "SET last_failure_at = :t, failing_since = if_not_exists(failing_since, :t)",
        )
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":t", s(at))
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .ok()?;
    r.attributes()
        .and_then(|a| a.get("failing_since"))
        .and_then(|v| v.as_s().ok())
        .cloned()
}

/// Turns an endpoint off because it kept failing. True only for the call
/// that turned it off (so admins are told once).
pub async fn disable_failing(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
    at: &str,
) -> bool {
    dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk(id)))
        .update_expression("SET enabled = :off, disabled_reason = :r, updated_at = :t")
        .condition_expression("attribute_exists(sk) AND enabled = :on")
        .expression_attribute_values(":off", AttributeValue::Bool(false))
        .expression_attribute_values(":on", AttributeValue::Bool(true))
        .expression_attribute_values(":r", s(DISABLED_FAILING))
        .expression_attribute_values(":t", s(at))
        .send()
        .await
        .is_ok()
}

/// A new secret for an endpoint: sealed, with its hint. Returned in the clear
/// once, for the person who made it.
pub async fn fresh_secret(
    cipher: &dyn Cipher,
    team_id: &str,
    endpoint_id: &str,
) -> Result<(String, String), String> {
    let secret = sign::new_secret();
    let sealed = seal(cipher, team_id, endpoint_id, &secret).await?;
    Ok((secret, sealed))
}

/// Rotates the secret in `st`: the current one becomes the previous one,
/// signing beside the new one for `overlap_hours`.
pub fn rotate(
    st: &mut Stored,
    new_secret: &str,
    new_sealed: String,
    overlap_hours: i64,
    now: chrono::DateTime<chrono::Utc>,
) {
    let hours = overlap_hours.clamp(0, MAX_OVERLAP_HOURS);
    if hours > 0 {
        st.previous_secret_sealed = std::mem::take(&mut st.secret_sealed);
        st.endpoint.previous_secret_expires_at = Some(
            (now + chrono::Duration::hours(hours))
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
    } else {
        st.previous_secret_sealed.clear();
        st.endpoint.previous_secret_expires_at = None;
    }
    st.secret_sealed = new_sealed;
    st.endpoint.secret_hint = sign::hint(new_secret);
    st.endpoint.updated_at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reversible "encryption" that refuses a different context.
    pub(crate) struct Fake;
    #[async_trait::async_trait]
    impl Cipher for Fake {
        async fn encrypt(&self, c: HashMap<String, String>, p: &[u8]) -> Result<Vec<u8>, String> {
            let mut out = format!("{}|{}|", c["team"], c["endpoint"]).into_bytes();
            out.extend(p.iter().map(|b| b ^ 0x5a));
            Ok(out)
        }
        async fn decrypt(&self, c: HashMap<String, String>, b: &[u8]) -> Result<Vec<u8>, String> {
            let head = format!("{}|{}|", c["team"], c["endpoint"]).into_bytes();
            if !b.starts_with(&head) {
                return Err("InvalidCiphertextException".into());
            }
            Ok(b[head.len()..].iter().map(|x| x ^ 0x5a).collect())
        }
    }

    fn input(url: &str, events: &[&str]) -> Input {
        Input {
            url: Some(url.into()),
            description: Some(" Deploy bot ".into()),
            events: Some(events.iter().map(|e| e.to_string()).collect()),
            enabled: None,
        }
    }

    #[test]
    fn endpoints_need_https_public_addresses_and_real_events() {
        let e = new_endpoint(
            &input(
                "https://hooks.example.com/coderhelm",
                &["run.failed", "run.completed", "run.failed"],
            ),
            "a@example.com",
            "t",
        )
        .unwrap();
        assert!(e.id.starts_with("ep_"));
        assert_eq!(e.events, vec!["run.completed", "run.failed"]);
        assert_eq!(e.description, "Deploy bot");
        assert!(e.enabled && e.disabled_reason.is_empty());
        for bad in [
            "http://hooks.example.com/x",
            "https://localhost/x",
            "https://127.0.0.1/x",
            "https://10.0.0.8/x",
            "https://169.254.169.254/latest/meta-data/",
            "https://[fd00::1]/x",
            "https://[::ffff:169.254.169.254]/",
            "https://user:pw@hooks.example.com/",
            "not a url",
        ] {
            let r = new_endpoint(&input(bad, &["*"]), "a", "t");
            assert_eq!(r.unwrap_err().field, "url", "{bad}");
        }
        let all = new_endpoint(
            &input("https://h.example.com", &["run.failed", "*"]),
            "a",
            "t",
        )
        .unwrap();
        assert_eq!(all.events, vec!["*"]);
        assert_eq!(
            new_endpoint(&input("https://h.example.com", &["run.exploded"]), "a", "t")
                .unwrap_err()
                .field,
            "events"
        );
        assert_eq!(
            new_endpoint(&input("https://h.example.com", &[]), "a", "t")
                .unwrap_err()
                .field,
            "events"
        );
        let mut long = input("https://h.example.com", &["*"]);
        long.description = Some("x".repeat(MAX_DESCRIPTION + 1));
        assert_eq!(
            new_endpoint(&long, "a", "t").unwrap_err().field,
            "description"
        );
    }

    #[test]
    fn an_endpoint_wants_what_it_subscribed_to_while_on() {
        let mut e =
            new_endpoint(&input("https://h.example.com", &["run.failed"]), "a", "t").unwrap();
        assert!(e.wants("run.failed"));
        assert!(!e.wants("run.completed"));
        assert!(
            !e.wants(catalog::TEST_EVENT),
            "test events go only where sent"
        );
        e.events = vec!["*".into()];
        assert!(e.wants("review.completed"));
        e.enabled = false;
        assert!(!e.wants("review.completed"));
    }

    #[test]
    fn turning_an_endpoint_back_on_starts_its_failure_count_again() {
        let mut e = new_endpoint(&input("https://h.example.com", &["*"]), "a", "t").unwrap();
        e.enabled = false;
        e.disabled_reason = DISABLED_FAILING.into();
        e.failing_since = Some("2026-10-01T00:00:00Z".into());
        apply_input(
            &mut e,
            &Input {
                enabled: Some(true),
                ..Default::default()
            },
            "t2",
        )
        .unwrap();
        assert!(e.enabled && e.disabled_reason.is_empty() && e.failing_since.is_none());
        apply_input(
            &mut e,
            &Input {
                enabled: Some(false),
                ..Default::default()
            },
            "t3",
        )
        .unwrap();
        assert_eq!(e.disabled_reason, DISABLED_MANUAL);
        assert!(apply_input(
            &mut e,
            &Input {
                url: Some("https://192.168.0.1/".into()),
                ..Default::default()
            },
            "t4"
        )
        .is_err());
    }

    #[tokio::test]
    async fn secrets_round_trip_sealed_to_their_team_and_endpoint() {
        let (secret, sealed) = fresh_secret(&Fake, "TEAM#a", "ep_1").await.unwrap();
        assert!(!sealed.contains(&secret));
        assert_eq!(
            open(&Fake, "TEAM#a", "ep_1", &sealed).await.unwrap(),
            secret
        );
        assert!(open(&Fake, "TEAM#b", "ep_1", &sealed).await.is_err());
        assert!(open(&Fake, "TEAM#a", "ep_2", &sealed).await.is_err());
    }

    #[tokio::test]
    async fn a_rotated_secret_signs_beside_the_new_one_until_the_overlap_ends() {
        let now = chrono::Utc::now();
        let (old, old_sealed) = fresh_secret(&Fake, "T", "ep_1").await.unwrap();
        let mut st = Stored {
            endpoint: Endpoint {
                id: "ep_1".into(),
                ..Default::default()
            },
            secret_sealed: old_sealed,
            previous_secret_sealed: String::new(),
        };
        assert_eq!(
            signing_secrets(&Fake, "T", &st, now).await.unwrap(),
            vec![old.clone()]
        );
        let (new, new_sealed) = fresh_secret(&Fake, "T", "ep_1").await.unwrap();
        rotate(&mut st, &new, new_sealed, 24, now);
        assert_eq!(st.endpoint.secret_hint, sign::hint(&new));
        assert_eq!(
            signing_secrets(&Fake, "T", &st, now).await.unwrap(),
            vec![new.clone(), old.clone()]
        );
        let later = now + chrono::Duration::hours(25);
        assert_eq!(
            signing_secrets(&Fake, "T", &st, later).await.unwrap(),
            vec![new.clone()]
        );
        // No overlap: the old secret stops at once.
        let (newer, newer_sealed) = fresh_secret(&Fake, "T", "ep_1").await.unwrap();
        rotate(&mut st, &newer, newer_sealed, 0, now);
        assert_eq!(
            signing_secrets(&Fake, "T", &st, now).await.unwrap(),
            vec![newer]
        );
        // The overlap is capped.
        let (x, xs) = fresh_secret(&Fake, "T", "ep_1").await.unwrap();
        rotate(&mut st, &x, xs, 10_000, now);
        let until = chrono::DateTime::parse_from_rfc3339(
            st.endpoint.previous_secret_expires_at.as_deref().unwrap(),
        )
        .unwrap();
        assert!(
            until
                <= now + chrono::Duration::hours(MAX_OVERLAP_HOURS) + chrono::Duration::seconds(1)
        );
    }

    #[test]
    fn an_endpoint_round_trips_through_its_item() {
        let mut e = new_endpoint(
            &input("https://h.example.com/x", &["run.failed"]),
            "a@example.com",
            "2026-10-26T00:00:00Z",
        )
        .unwrap();
        e.secret_hint = "whsec_…abcd".into();
        e.failing_since = Some("2026-10-26T01:00:00Z".into());
        let st = Stored {
            endpoint: e,
            secret_sealed: "c2VhbGVk".into(),
            previous_secret_sealed: "b2xk".into(),
        };
        let item = to_item("TEAM#a", &st);
        assert_eq!(
            item["sk"].as_s().unwrap(),
            &format!("WEBHOOK_ENDPOINT#{}", st.endpoint.id)
        );
        assert_eq!(from_item(&item).unwrap(), st);
        // The API view never carries a secret.
        let view = serde_json::to_string(&st.endpoint).unwrap();
        assert!(!view.contains("c2VhbGVk") && !view.contains("b2xk"));
    }
}
