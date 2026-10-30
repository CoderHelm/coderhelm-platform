//! Email counts by kind: sends, deliveries, bounces, complaints and
//! rejections, per UTC day, read from Amazon SES's own events.
//!
//! Two places write them, the same way:
//!
//! - **CoderHelm's mail relay** (hosted side of hybrid): its configuration
//!   set publishes every event to a topic the worker reads. Each relayed
//!   email carries the `install` and `template` tags, so the counts land on
//!   that install's partition (`HYBRID#<install>`), and the install reads
//!   them through the signed `GET /hybrid/email/status`.
//! - **An install's own SES**: its configuration set publishes the same
//!   events to its own worker. Its emails carry the `category` tag; mail
//!   without one is Amazon Cognito's (sign-up codes and password resets,
//!   once Cognito sends through the install's SES), told apart by subject.
//!   Counts land on `SYSTEM`.
//!
//! Items (settings table, beside the records): `EMAILSTAT#<yyyy-mm-dd>`
//! with one counter per event and kind (`delivery_alerts`), kept
//! [`DAY_RETENTION_DAYS`]; and `EMAILSTAT#LAST` with when each event last
//! happened. Counts are of recipients, as SES reports them.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_json::Value;

/// The kinds of email counted apart, with their labels.
pub const KINDS: &[(&str, &str)] = &[
    ("sign_in", "Sign-in and invites"),
    ("password_reset", "Password resets"),
    ("notices", "Run and review notices"),
    ("weekly_summary", "Weekly summary"),
    ("alerts", "Alerts"),
    ("other", "Other"),
];

/// The SES events counted, as the counters name them.
pub const EVENTS: &[&str] = &["send", "delivery", "bounce", "complaint", "reject"];

/// The message tag that names an email's kind on the install's own SES.
pub const KIND_TAG: &str = "category";

pub const DAY_PREFIX: &str = "EMAILSTAT#D#";
pub const LAST_SK: &str = "EMAILSTAT#LAST";
/// Days a day's counts are kept.
pub const DAY_RETENTION_DAYS: i64 = 120;
/// Where an install's own counts live.
pub const OWN_PK: &str = "SYSTEM";

pub fn day_sk(day: &str) -> String {
    format!("{DAY_PREFIX}{day}")
}

/// The kind of an email, from its template id or Deliveries event
/// (`team_invite`, `token-warning`, `run_complete`, ...).
pub fn kind_of(template: &str) -> &'static str {
    let t = template.trim().to_ascii_lowercase().replace('-', "_");
    match t.as_str() {
        "sign_in" | "team_invite" | "invite" | "welcome" | "verification" | "sign_up" => "sign_in",
        "password_reset" | "forgot_password" => "password_reset",
        "weekly_summary" => "weekly_summary",
        "token_warning"
        | "token_limit_reached"
        | "knowledge_signin"
        | "webhook_disabled"
        | "relay_paused"
        | "alert" => "alerts",
        t if t.starts_with("two_factor") => "alerts",
        "run_complete" | "run_failed" | "review" | "review_complete" | "plugin_approval"
        | "support_update" | "update_notice" => "notices",
        t if t.starts_with("run_") || t.starts_with("review_") => "notices",
        _ => "other",
    }
}

/// A known kind as it is, anything else as `other`.
fn known_kind(k: &str) -> &'static str {
    KINDS
        .iter()
        .find(|(id, _)| *id == k)
        .map(|(id, _)| *id)
        .unwrap_or("other")
}

/// One SES event, as counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counted {
    /// One of [`EVENTS`].
    pub event: &'static str,
    pub kind: &'static str,
    /// The install a relayed email was for (its `install` tag).
    pub install: Option<String>,
    /// How many recipients it counts for (at least one).
    pub recipients: u64,
}

fn tag<'a>(v: &'a Value, name: &str) -> Option<&'a str> {
    v.pointer("/mail/tags")?
        .get(name)?
        .as_array()?
        .first()?
        .as_str()
}

/// Reads one SES event (event publishing's `eventType`, or a feedback
/// notification's `notificationType`). None for anything not counted.
pub fn read_event(v: &Value) -> Option<Counted> {
    let t = v
        .get("eventType")
        .or_else(|| v.get("notificationType"))?
        .as_str()?;
    let event = match t {
        "Send" => "send",
        "Delivery" => "delivery",
        "Bounce" => "bounce",
        "Complaint" => "complaint",
        "Reject" => "reject",
        _ => return None,
    };
    let kind = if let Some(k) = tag(v, KIND_TAG) {
        known_kind(k)
    } else if let Some(t) = tag(v, super::relay::TEMPLATE_TAG) {
        kind_of(t)
    } else {
        // Untagged mail on an install is Amazon Cognito's.
        let subject = v
            .pointer("/mail/commonHeaders/subject")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        if subject.contains("reset") || subject.contains("password") {
            "password_reset"
        } else {
            "sign_in"
        }
    };
    let install = tag(v, super::relay::INSTALL_TAG)
        .filter(|id| crate::hybrid::sign::is_install_id(id))
        .map(str::to_string);
    let listed = |p: &str| v.pointer(p).and_then(Value::as_array).map(Vec::len);
    let recipients = match event {
        "bounce" => listed("/bounce/bouncedRecipients"),
        "complaint" => listed("/complaint/complainedRecipients"),
        "delivery" => listed("/delivery/recipients"),
        _ => listed("/mail/destination"),
    }
    .unwrap_or(1)
    .max(1) as u64;
    Some(Counted {
        event,
        kind,
        install,
        recipients,
    })
}

/// The counter one event adds to (`<event>_<kind>`).
pub fn counter(event: &str, kind: &str) -> String {
    format!("{event}_{kind}")
}

/// Adds one event to the day's counts and notes when it happened.
pub async fn count(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    c: &Counted,
    now: chrono::DateTime<chrono::Utc>,
) {
    let day = now.format("%Y-%m-%d").to_string();
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let ttl = now.timestamp() + DAY_RETENTION_DAYS * 86_400;
    let r = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(day_sk(&day)))
        .update_expression("ADD #c :n SET #t = if_not_exists(#t, :ttl)")
        .expression_attribute_names("#c", counter(c.event, c.kind))
        .expression_attribute_names("#t", "ttl")
        .expression_attribute_values(":n", AttributeValue::N(c.recipients.to_string()))
        .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await;
    if let Err(e) = r {
        tracing::warn!(error = %e, "email stats: count failed");
    }
    let _ = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(LAST_SK.into()))
        .update_expression("SET #l = :at")
        .expression_attribute_names("#l", format!("last_{}_at", c.event))
        .expression_attribute_values(":at", AttributeValue::S(at))
        .send()
        .await;
}

/// One kind's counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct KindCounts {
    /// `sign_in`, `password_reset`, `notices`, `weekly_summary`, `alerts` or `other`.
    pub kind: String,
    pub label: String,
    /// Emails SES accepted for sending (per recipient).
    pub sends: u64,
    /// Emails the receiving server accepted.
    pub deliveries: u64,
    pub bounces: u64,
    pub complaints: u64,
    /// Emails SES refused to send (a virus, or a suppressed address).
    pub rejections: u64,
}

impl KindCounts {
    fn add(&mut self, event: &str, n: u64) {
        match event {
            "send" => self.sends += n,
            "delivery" => self.deliveries += n,
            "bounce" => self.bounces += n,
            "complaint" => self.complaints += n,
            "reject" => self.rejections += n,
            _ => {}
        }
    }
    fn merge(&mut self, o: &KindCounts) {
        self.sends += o.sends;
        self.deliveries += o.deliveries;
        self.bounces += o.bounces;
        self.complaints += o.complaints;
        self.rejections += o.rejections;
    }
}

/// The counts over some days, by kind, with when each event last happened.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    /// Every kind, in a fixed order (zeros included).
    pub kinds: Vec<KindCounts>,
    pub totals: KindCounts,
    /// When SES last accepted an email for sending.
    pub last_send_at: Option<String>,
    /// When an email was last delivered.
    pub last_delivery_at: Option<String>,
    pub last_bounce_at: Option<String>,
    pub last_complaint_at: Option<String>,
    pub last_reject_at: Option<String>,
}

impl Stats {
    fn empty() -> Self {
        Stats {
            kinds: KINDS
                .iter()
                .map(|(k, l)| KindCounts {
                    kind: (*k).into(),
                    label: (*l).into(),
                    ..Default::default()
                })
                .collect(),
            totals: KindCounts {
                kind: "all".into(),
                label: "All email".into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Adds another sender's counts (an install that switched senders).
    pub fn merge(&mut self, o: &Stats) {
        for k in &o.kinds {
            if let Some(mine) = self.kinds.iter_mut().find(|m| m.kind == k.kind) {
                mine.merge(k);
            }
        }
        self.totals.merge(&o.totals);
        let later = |a: &mut Option<String>, b: &Option<String>| {
            if b.as_deref() > a.as_deref() {
                *a = b.clone();
            }
        };
        later(&mut self.last_send_at, &o.last_send_at);
        later(&mut self.last_delivery_at, &o.last_delivery_at);
        later(&mut self.last_bounce_at, &o.last_bounce_at);
        later(&mut self.last_complaint_at, &o.last_complaint_at);
        later(&mut self.last_reject_at, &o.last_reject_at);
    }
}

type Item = HashMap<String, AttributeValue>;

/// The counts in day items from `from` to `to` (inclusive, `yyyy-mm-dd`),
/// with the last-event times. Pure.
pub fn from_items(days: &[Item], last: Option<&Item>, from: &str, to: &str) -> Stats {
    let mut out = Stats::empty();
    for i in days {
        let Some(day) = i
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.strip_prefix(DAY_PREFIX))
        else {
            continue;
        };
        if day < from || day > to {
            continue;
        }
        for (name, v) in i {
            let Some(n) = v.as_n().ok().and_then(|n| n.parse::<u64>().ok()) else {
                continue;
            };
            let Some((event, kind)) = name.split_once('_') else {
                continue;
            };
            if !EVENTS.contains(&event) {
                continue;
            }
            let kind = known_kind(kind);
            if let Some(k) = out.kinds.iter_mut().find(|k| k.kind == kind) {
                k.add(event, n);
            }
            out.totals.add(event, n);
        }
    }
    let t = |k: &str| {
        last.and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .cloned()
    };
    out.last_send_at = t("last_send_at");
    out.last_delivery_at = t("last_delivery_at");
    out.last_bounce_at = t("last_bounce_at");
    out.last_complaint_at = t("last_complaint_at");
    out.last_reject_at = t("last_reject_at");
    out
}

/// Reads the counts for the days from `from` to `to` (inclusive).
pub async fn read(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    from: &str,
    to: &str,
) -> Result<Stats, String> {
    let days = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :p AND sk BETWEEN :a AND :b")
        .expression_attribute_values(":p", AttributeValue::S(pk.into()))
        .expression_attribute_values(":a", AttributeValue::S(day_sk(from)))
        .expression_attribute_values(":b", AttributeValue::S(day_sk(to)))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let last = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(LAST_SK.into()))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(from_items(days.items(), last.item(), from, to))
}

/// Only when each event last happened (no day counts).
pub async fn read_last(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
) -> Result<Stats, String> {
    let last = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(LAST_SK.into()))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(from_items(&[], last.item(), "", ""))
}

/// The first and last UTC day a period covers, inclusive: `days` days
/// ending today. A 24-hour period covers yesterday and today.
pub fn day_range(days: i64, now: chrono::DateTime<chrono::Utc>) -> (String, String) {
    let to = now.date_naive();
    let from = to - chrono::Duration::days(days.max(1) - 1);
    (
        from.format("%Y-%m-%d").to_string(),
        to.format("%Y-%m-%d").to_string(),
    )
}

/// Who sends Amazon Cognito's sign-up codes and password resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CognitoSender {
    /// Cognito's own address, limited to about 50 emails a day per account.
    CognitoDefault,
    /// The install's own SES identity (`email.cognitoUsesSes`).
    Ses,
}

/// Cognito's own sender's daily limit (AWS's documented quota).
pub const COGNITO_DEFAULT_DAILY: u64 = 50;

/// The deployed Cognito sender (`COGNITO_EMAIL_SENDER`, set by the deploy
/// from `email.cognitoUsesSes`). None when the deploy didn't say.
pub fn cognito_sender_from(v: Option<&str>) -> Option<CognitoSender> {
    match v.map(str::trim) {
        Some("ses") => Some(CognitoSender::Ses),
        Some("cognito_default") => Some(CognitoSender::CognitoDefault),
        _ => None,
    }
}

pub fn cognito_sender() -> Option<CognitoSender> {
    cognito_sender_from(std::env::var("COGNITO_EMAIL_SENDER").ok().as_deref())
}

/// Sums the counters of every kind for one event (for tests and callers
/// that want one number).
pub fn event_total(s: &Stats, event: &str) -> u64 {
    let k = &s.totals;
    match event {
        "send" => k.sends,
        "delivery" => k.deliveries,
        "bounce" => k.bounces,
        "complaint" => k.complaints,
        "reject" => k.rejections,
        _ => 0,
    }
}

/// The counts as the signed relay status carries them (and back).
pub fn to_json(s: &Stats) -> Value {
    serde_json::to_value(s).unwrap_or(Value::Null)
}

pub fn from_json(v: &Value) -> Option<Stats> {
    let mut out = Stats::empty();
    let num = |o: &Value, k: &str| o[k].as_u64().unwrap_or(0);
    for k in v["kinds"].as_array()? {
        let kind = known_kind(k["kind"].as_str().unwrap_or(""));
        if let Some(m) = out.kinds.iter_mut().find(|m| m.kind == kind) {
            m.sends += num(k, "sends");
            m.deliveries += num(k, "deliveries");
            m.bounces += num(k, "bounces");
            m.complaints += num(k, "complaints");
            m.rejections += num(k, "rejections");
        }
    }
    let mut totals = out.totals.clone();
    for k in &out.kinds {
        totals.merge(k);
    }
    out.totals = totals;
    let t = |k: &str| v[k].as_str().map(str::to_string);
    out.last_send_at = t("last_send_at");
    out.last_delivery_at = t("last_delivery_at");
    out.last_bounce_at = t("last_bounce_at");
    out.last_complaint_at = t("last_complaint_at");
    out.last_reject_at = t("last_reject_at");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn n(v: u64) -> AttributeValue {
        AttributeValue::N(v.to_string())
    }

    #[test]
    fn every_email_has_a_kind() {
        assert_eq!(kind_of("team_invite"), "sign_in");
        assert_eq!(kind_of("welcome"), "sign_in");
        assert_eq!(kind_of("token-warning"), "alerts");
        assert_eq!(kind_of("two_factor_lockout"), "alerts");
        assert_eq!(kind_of("webhook_disabled"), "alerts");
        assert_eq!(kind_of("weekly_summary"), "weekly_summary");
        assert_eq!(kind_of("run_complete"), "notices");
        assert_eq!(kind_of("plugin_approval"), "notices");
        assert_eq!(kind_of("update_notice"), "notices");
        assert_eq!(kind_of("test_email"), "other");
        for (k, _) in KINDS {
            assert_eq!(known_kind(k), *k);
        }
        // Every template the relay sends has a kind that isn't a guess.
        for t in super::super::templates::TEMPLATES {
            if t.id != "test_email" {
                assert_ne!(kind_of(t.id), "other", "{}", t.id);
            }
        }
    }

    #[test]
    fn events_are_read_with_their_kind_and_install() {
        let relayed = json!({
            "eventType": "Delivery",
            "mail": { "destination": ["a@example.com"], "tags": { "install": ["ins_abcdefghijklmnopqrstuvwx"], "template": ["weekly_summary"] } },
            "delivery": { "recipients": ["a@example.com"] }
        });
        let c = read_event(&relayed).unwrap();
        assert_eq!(c.event, "delivery");
        assert_eq!(c.kind, "weekly_summary");
        assert_eq!(c.install.as_deref(), Some("ins_abcdefghijklmnopqrstuvwx"));
        assert_eq!(c.recipients, 1);

        let own = json!({
            "eventType": "Bounce",
            "mail": { "destination": ["a@example.com", "b@example.com"], "tags": { "category": ["alerts"] } },
            "bounce": { "bounceType": "Transient", "bouncedRecipients": [{"emailAddress": "a@example.com"}, {"emailAddress": "b@example.com"}] }
        });
        let c = read_event(&own).unwrap();
        assert_eq!((c.event, c.kind, c.recipients), ("bounce", "alerts", 2));
        assert!(c.install.is_none());

        let cognito_reset = json!({
            "eventType": "Send",
            "mail": { "destination": ["a@example.com"], "commonHeaders": { "subject": "Reset your password" } }
        });
        assert_eq!(read_event(&cognito_reset).unwrap().kind, "password_reset");
        let cognito_code = json!({
            "eventType": "Send",
            "mail": { "destination": ["a@example.com"], "commonHeaders": { "subject": "Your verification code" } }
        });
        assert_eq!(read_event(&cognito_code).unwrap().kind, "sign_in");
        let reject =
            json!({ "eventType": "Reject", "mail": { "tags": { "category": ["nonsense"] } } });
        let c = read_event(&reject).unwrap();
        assert_eq!((c.event, c.kind), ("reject", "other"));
        // Not counted: opens, clicks, delays, anything else.
        assert!(read_event(&json!({ "eventType": "Open", "mail": {} })).is_none());
        assert!(read_event(&json!({ "type": "run" })).is_none());
    }

    #[test]
    fn counts_add_up_by_kind_and_day() {
        let day = |d: &str, fields: &[(&str, u64)]| {
            let mut i: Item = HashMap::new();
            i.insert("sk".into(), AttributeValue::S(day_sk(d)));
            i.insert("ttl".into(), n(9_999_999_999));
            for (k, v) in fields {
                i.insert((*k).into(), n(*v));
            }
            i
        };
        let items = vec![
            day(
                "2026-10-28",
                &[
                    ("send_notices", 5),
                    ("delivery_notices", 4),
                    ("bounce_notices", 1),
                ],
            ),
            day(
                "2026-10-29",
                &[
                    ("send_sign_in", 3),
                    ("delivery_sign_in", 3),
                    ("complaint_weekly_summary", 1),
                    ("reject_alerts", 2),
                    ("send_bogus", 7),
                ],
            ),
            day("2026-10-20", &[("send_notices", 100)]),
        ];
        let mut last: Item = HashMap::new();
        last.insert(
            "last_send_at".into(),
            AttributeValue::S("2026-10-29T10:00:00Z".into()),
        );
        let s = from_items(&items, Some(&last), "2026-10-28", "2026-10-29");
        let k = |id: &str| s.kinds.iter().find(|k| k.kind == id).unwrap().clone();
        assert_eq!(
            (
                k("notices").sends,
                k("notices").deliveries,
                k("notices").bounces
            ),
            (5, 4, 1)
        );
        assert_eq!(k("sign_in").sends, 3);
        assert_eq!(k("weekly_summary").complaints, 1);
        assert_eq!(k("alerts").rejections, 2);
        // An unknown kind counts as other, never lost.
        assert_eq!(k("other").sends, 7);
        assert_eq!(s.totals.sends, 15);
        assert_eq!(event_total(&s, "delivery"), 7);
        assert_eq!(s.kinds.len(), KINDS.len());
        assert_eq!(s.last_send_at.as_deref(), Some("2026-10-29T10:00:00Z"));
        // Through JSON (the relay status) and back.
        let back = from_json(&to_json(&s)).unwrap();
        assert_eq!(back, s);
        let mut both = s.clone();
        both.merge(&back);
        assert_eq!(both.totals.sends, 30);
    }

    #[test]
    fn periods_cover_whole_days() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-30T08:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            day_range(2, now),
            ("2026-10-29".into(), "2026-10-30".into())
        );
        assert_eq!(
            day_range(7, now),
            ("2026-10-24".into(), "2026-10-30".into())
        );
    }

    #[test]
    fn the_cognito_sender_comes_from_the_deploy() {
        assert_eq!(cognito_sender_from(Some("ses")), Some(CognitoSender::Ses));
        assert_eq!(
            cognito_sender_from(Some("cognito_default")),
            Some(CognitoSender::CognitoDefault)
        );
        assert_eq!(cognito_sender_from(None), None);
        assert_eq!(cognito_sender_from(Some("x")), None);
    }
}
