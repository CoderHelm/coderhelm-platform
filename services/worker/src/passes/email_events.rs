//! Bounces and complaints for CoderHelm's mail relay (hosted side).
//!
//! The relay's SES configuration set publishes bounce and complaint events
//! to an SNS topic that delivers them, raw, to the ticket queue. Each event
//! names the install in its `install` message tag. A permanent bounce or a
//! complaint suppresses that address for that install, is counted, shows in
//! the install's Deliveries, and, when the install's rate over seven days is
//! too high ([`relay::should_pause`]), pauses the relay for that install and
//! tells its admins.
//!
//! Every event (sends and deliveries too) is also counted by kind
//! (common::mail::stats): on the hosted side for the install it was for, on
//! a self-hosted install for its own SES.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine;
use chrono::{Duration, Utc};
use common::deliveries::{self, Delivery, Direction, Status};
use common::hybrid::relay as hybrid_relay;
use common::mail::{relay, stats, templates};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::WorkerState;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The counters' field for an event.
pub fn field(kind: relay::EventKind) -> &'static str {
    match kind {
        relay::EventKind::Bounce => "bounced",
        relay::EventKind::Complaint => "complained",
    }
}

/// The install's counts over the last [`relay::RATE_WINDOW_DAYS`] days, from
/// its day items. Pure.
pub fn window(
    items: &[HashMap<String, AttributeValue>],
    today: chrono::NaiveDate,
) -> relay::Counts {
    let from = today - Duration::days(relay::RATE_WINDOW_DAYS - 1);
    let num = |i: &HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let mut c = relay::Counts::default();
    for i in items {
        let Some(day) = i
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .and_then(|sk| sk.strip_prefix("EMAILDAY#"))
            .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        else {
            continue;
        };
        if day >= from && day <= today {
            c.sent += num(i, "sent");
            c.bounced += num(i, "bounced");
            c.complained += num(i, "complained");
        }
    }
    c
}

/// Where one SES event is counted: the install a relayed email was for
/// (hosted side), or this install's own partition for its own SES. None for
/// the hosted deployment's own mail, which isn't counted here.
pub fn stats_pk(c: &stats::Counted, hosted: bool) -> Option<String> {
    match (&c.install, hosted) {
        (Some(id), true) => Some(hybrid_relay::install_pk(id)),
        (None, false) => Some(stats::OWN_PK.to_string()),
        _ => None,
    }
}

pub async fn handle(state: &WorkerState, raw: &Value) {
    // Every send, delivery, bounce, complaint and rejection is counted by
    // kind, for the install's Deliveries page.
    if let Some(c) = stats::read_event(raw) {
        let hosted = common::mail::deployment() == common::mail::Deployment::Hosted;
        if let Some(pk) = stats_pk(&c, hosted) {
            stats::count(
                &state.dynamo,
                &state.config.settings_table_name,
                &pk,
                &c,
                Utc::now(),
            )
            .await;
        }
    }
    let Some(ev) = relay::parse_event(raw) else {
        info!("Mail event without an install, or not a permanent bounce or complaint: ignored");
        return;
    };
    let table = &state.config.settings_table_name;
    let pk = hybrid_relay::install_pk(&ev.install_id);
    let now = Utc::now();
    for r in &ev.recipients {
        let hash = relay::recipient_hash(&ev.install_id, r);
        let _ = state
            .dynamo
            .put_item()
            .table_name(table)
            .item("pk", s(&pk))
            .item("sk", s(&relay::suppressed_sk(&hash)))
            .item("kind", s(field(ev.kind)))
            .item("at", s(&now.to_rfc3339()))
            .send()
            .await
            .map_err(|e| warn!(error = %e, "mail event: suppression write failed"));
        let mut d = Delivery::new(
            Direction::Inbound,
            "email_relay",
            &format!("email.{}", field(ev.kind)),
            &relay::redact(r),
        );
        d.finish(
            Status::Failed,
            "Address suppressed",
            match ev.kind {
                relay::EventKind::Bounce => "email_bounced",
                relay::EventKind::Complaint => "email_complained",
            },
        );
        let mut item = deliveries::to_item(&pk, &d, now.timestamp());
        item.insert(
            "ttl".into(),
            AttributeValue::N((now.timestamp() + relay::DELIVERY_TTL_SECS).to_string()),
        );
        let _ = state
            .dynamo
            .put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await;
    }
    let day = now.format("%Y-%m-%d").to_string();
    let _ = state
        .dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(&pk))
        .key("sk", s(&relay::day_sk(&day)))
        .update_expression("ADD #f :n")
        .expression_attribute_names("#f", field(ev.kind))
        .expression_attribute_values(":n", AttributeValue::N(ev.recipients.len().to_string()))
        .send()
        .await;
    audit(
        state,
        &ev.install_id,
        "email_suppress",
        json!({ "kind": field(ev.kind), "count": ev.recipients.len() }),
    )
    .await;

    let items = state
        .dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :p AND begins_with(sk, :d)")
        .expression_attribute_values(":p", s(&pk))
        .expression_attribute_values(":d", s("EMAILDAY#"))
        .consistent_read(true)
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    let counts = window(&items, now.date_naive());
    let Some(reason) = relay::should_pause(counts) else {
        return;
    };
    let paused = state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(&pk))
        .item("sk", s(relay::PAUSE_SK))
        .item("reason", s(reason))
        .item("at", s(&now.to_rfc3339()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    if paused.is_err() {
        // Already paused (or the write failed): admins were told then.
        return;
    }
    warn!(install = %ev.install_id, reason, sent = counts.sent, "mail relay paused for an install");
    audit(
        state,
        &ev.install_id,
        "email_auto_pause",
        json!({ "reason": reason, "sent": counts.sent, "bounced": counts.bounced, "complained": counts.complained }),
    )
    .await;
    tell_admins(state, &ev.install_id, reason).await;
}

async fn audit(state: &WorkerState, install: &str, action: &str, detail: Value) {
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s("HYBRIDAUDIT"))
        .item("sk", s(&ulid::Ulid::new().to_string()))
        .item("install_id", s(install))
        .item("action", s(action))
        .item("detail", s(&detail.to_string()))
        .item("at", s(&Utc::now().to_rfc3339()))
        .item(
            "ttl",
            AttributeValue::N((Utc::now().timestamp() + 400 * 86_400).to_string()),
        )
        .send()
        .await;
}

/// Emails the install's listed admins (their sealed addresses) that the
/// relay paused, from the relay's own sender.
async fn tell_admins(state: &WorkerState, install: &str, reason: &str) {
    let Some(from) = std::env::var("HYBRID_EMAIL_FROM")
        .ok()
        .filter(|f| relay::valid_address(f))
    else {
        return;
    };
    let items = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :p AND begins_with(sk, :t)")
        .expression_attribute_values(":p", s(&hybrid_relay::install_pk(install)))
        .expression_attribute_values(":t", s("EMAILTO#"))
        .send()
        .await
        .map(|r| r.items().to_vec())
        .unwrap_or_default();
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    };
    let Ok((t, v)) = templates::check(
        "relay_paused",
        json!({ "reason": reason })
            .as_object()
            .unwrap_or(&Default::default()),
        &[],
        false,
    ) else {
        return;
    };
    let email = templates::render(t, &v, "your install", None);
    use common::mcp_credentials::Cipher;
    for i in items {
        let admin = i
            .get("kind")
            .and_then(|k| k.as_s().ok())
            .map(String::as_str)
            == Some("admin");
        let Some(sealed) = i
            .get("sealed")
            .and_then(|v| v.as_s().ok())
            .filter(|_| admin)
        else {
            continue;
        };
        let Ok(blob) = base64::engine::general_purpose::STANDARD.decode(sealed) else {
            continue;
        };
        let Ok(plain) = cipher.decrypt(relay::admin_context(install), &blob).await else {
            continue;
        };
        let to = String::from_utf8_lossy(&plain).to_string();
        if !relay::valid_address(&to) {
            continue;
        }
        use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
        let c = |x: &str| Content::builder().data(x).charset("UTF-8").build();
        let (Ok(subj), Ok(text), Ok(html)) = (c(&email.subject), c(&email.text), c(&email.html))
        else {
            continue;
        };
        let sent = state
            .ses
            .send_email()
            .from_email_address(relay::from_header("CoderHelm mail service", &from))
            .destination(Destination::builder().to_addresses(&to).build())
            .content(
                EmailContent::builder()
                    .simple(
                        Message::builder()
                            .subject(subj)
                            .body(Body::builder().text(text).html(html).build())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        if let Err(e) = sent {
            warn!(install, error = %aws_sdk_sesv2::error::DisplayErrorContext(&e), "relay pause notice not sent");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(d: &str, sent: u64, bounced: u64, complained: u64) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("sk".to_string(), s(&format!("EMAILDAY#{d}"))),
            ("sent".to_string(), AttributeValue::N(sent.to_string())),
            (
                "bounced".to_string(),
                AttributeValue::N(bounced.to_string()),
            ),
            (
                "complained".to_string(),
                AttributeValue::N(complained.to_string()),
            ),
        ])
    }

    #[test]
    fn the_window_counts_only_the_last_seven_days() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 26).unwrap();
        let items = vec![
            day("2026-10-26", 10, 1, 0),
            day("2026-10-20", 20, 2, 1),
            day("2026-10-19", 500, 400, 50),
        ];
        let c = window(&items, today);
        assert_eq!(
            c,
            relay::Counts {
                sent: 30,
                bounced: 3,
                complained: 1
            }
        );
        assert_eq!(relay::should_pause(c), Some("bounces"));
    }

    #[test]
    fn events_count_for_the_install_they_were_sent_for() {
        let relayed = stats::Counted {
            event: "delivery",
            kind: "alerts",
            install: Some("ins_abcdefghijklmnopqrstuvwx".into()),
            recipients: 1,
        };
        let own = stats::Counted {
            install: None,
            ..relayed.clone()
        };
        assert_eq!(
            stats_pk(&relayed, true).as_deref(),
            Some("HYBRID#ins_abcdefghijklmnopqrstuvwx")
        );
        assert_eq!(stats_pk(&own, false).as_deref(), Some("SYSTEM"));
        // The hosted deployment's own mail, and an install tag on an
        // install's own SES, count nowhere.
        assert_eq!(stats_pk(&own, true), None);
        assert_eq!(stats_pk(&relayed, false), None);
    }

    #[test]
    fn the_pause_notice_is_a_fixed_template_installs_cannot_send() {
        let t = templates::get("relay_paused").unwrap();
        assert!(!t.install_may_send);
        let params = json!({"reason": "bounces"});
        let (t, v) =
            templates::check("relay_paused", params.as_object().unwrap(), &[], false).unwrap();
        let r = templates::render(t, &v, "your install", None);
        assert!(r.text.contains("bounces"));
        assert!(!r.text.contains("http"));
    }
}
