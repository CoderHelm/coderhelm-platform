//! The hosted event relay: forward one event to a linked install, re-signed,
//! and record each attempt on the delivery.
//!
//! The gateway forwards right away when an event arrives; a failure is
//! retried by the worker (a `relay_retry` job on the tickets queue, delayed
//! by [`crate::deliveries::next_retry_secs`]) for about 24 hours. The event's
//! body is held in the data bucket ([`payload_key`]) only while it may still
//! be retried, and deleted when it is delivered or given up on (the bucket's
//! lifecycle rule removes anything left after two days).

use std::time::Instant;

use crate::deliveries::{self, Attempt, Delivery, Status};

use super::sign;

/// The install's relay intake, from its origin (`https://…`).
pub fn target(origin: &str) -> String {
    format!("{}/webhooks/relay", origin.trim_end_matches('/'))
}

/// Where a held event body lives in the data bucket.
pub fn payload_key(delivery_id: &str) -> String {
    format!("relay/{delivery_id}.json")
}

/// The settings-table partition for an install's records on the hosted side.
pub fn install_pk(install_id: &str) -> String {
    format!("HYBRID#{install_id}")
}

/// One forward to send.
pub struct Forward<'a> {
    pub origin: &'a str,
    pub relay_key: &'a str,
    pub source: &'a str,
    pub event: &'a str,
    /// The id the install uses to refuse replays (GitHub's delivery id, or ours).
    pub delivery_id: &'a str,
    pub body: &'a [u8],
}

/// Sends one forward and returns the attempt (with the HTTP status when the
/// install answered).
pub async fn send(http: &reqwest::Client, f: &Forward<'_>) -> Attempt {
    let ts = chrono::Utc::now().timestamp();
    let started = Instant::now();
    let r = http
        .post(target(f.origin))
        .timeout(std::time::Duration::from_secs(8))
        .header("content-type", "application/json")
        .header("user-agent", "coderhelm-relay")
        .header(
            sign::RELAY_HEADER,
            sign::sign_relay(f.relay_key, ts, f.delivery_id, f.body),
        )
        .header(sign::RELAY_SOURCE_HEADER, f.source)
        .header(sign::RELAY_EVENT_HEADER, f.event)
        .body(f.body.to_vec())
        .send()
        .await;
    let duration_ms = started.elapsed().as_millis() as u64;
    let at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    match r {
        Ok(resp) => {
            let code = resp.status().as_u16();
            let error = if resp.status().is_success() {
                String::new()
            } else {
                let text = resp.text().await.unwrap_or_default();
                let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                v["error"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| text.chars().take(200).collect())
            };
            Attempt {
                at,
                code: Some(code),
                duration_ms,
                error,
            }
        }
        Err(e) => Attempt {
            at,
            code: None,
            duration_ms,
            error: if e.is_timeout() {
                "timed out after 8 seconds".into()
            } else {
                "could not connect".into()
            },
        },
    }
}

/// What to do after an attempt: done, retry in N seconds, or give up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Delivered,
    /// The install already had it (a replay refusal means the first copy landed).
    AlreadyDelivered,
    RetryIn(i64),
    GiveUp,
}

/// Applies an attempt to the delivery record and decides what comes next.
pub fn apply(d: &mut Delivery, a: Attempt) -> Next {
    let code = a.code;
    d.push_attempt(a);
    let n = d.attempts.len();
    match code {
        Some(c) if (200..300).contains(&c) => {
            d.finish(Status::Handled, "Forwarded to the install", "");
            d.next_retry_at = None;
            Next::Delivered
        }
        Some(409) => {
            d.finish(Status::Handled, "The install already had it", "");
            d.next_retry_at = None;
            Next::AlreadyDelivered
        }
        // A refusal that won't change on its own (bad request, unknown event)
        // stops early; signature and server errors are retried.
        Some(c) if (400..500).contains(&c) && c != 401 && c != 403 && c != 408 && c != 429 => {
            d.finish(
                Status::Failed,
                "Refused by the install",
                deliveries::reason_for_http("relay", code),
            );
            d.next_retry_at = None;
            Next::GiveUp
        }
        _ => match deliveries::next_retry_secs(n) {
            Some(secs) => {
                d.finish(
                    Status::Pending,
                    "Retrying",
                    deliveries::reason_for_http("relay", code),
                );
                d.next_retry_at = Some(
                    (chrono::Utc::now() + chrono::Duration::seconds(secs))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                );
                Next::RetryIn(secs)
            }
            None => {
                d.finish(Status::Failed, "Gave up after 24 hours", "relay_gave_up");
                d.next_retry_at = None;
                Next::GiveUp
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deliveries::Direction;

    fn att(code: Option<u16>) -> Attempt {
        Attempt {
            at: "t".into(),
            code,
            duration_ms: 12,
            error: String::new(),
        }
    }

    #[test]
    fn targets_and_keys() {
        assert_eq!(
            target("https://d1abcdef234567.cloudfront.net/"),
            "https://d1abcdef234567.cloudfront.net/webhooks/relay"
        );
        assert_eq!(payload_key("abc"), "relay/abc.json");
        assert_eq!(install_pk("ins_x"), "HYBRID#ins_x");
    }

    #[test]
    fn a_success_finishes_the_delivery() {
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        assert_eq!(apply(&mut d, att(Some(202))), Next::Delivered);
        assert_eq!(d.status, Status::Handled);
        assert_eq!(d.attempts.len(), 1);
        assert!(d.next_retry_at.is_none());
    }

    #[test]
    fn a_replay_refusal_means_it_already_landed() {
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        assert_eq!(apply(&mut d, att(Some(409))), Next::AlreadyDelivered);
        assert!(d.status.is_ok());
    }

    #[test]
    fn unreachable_and_server_errors_retry_with_backoff_then_give_up() {
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        assert_eq!(apply(&mut d, att(None)), Next::RetryIn(30));
        assert_eq!(d.status, Status::Pending);
        assert_eq!(d.reason, "relay_unreachable");
        assert!(d.next_retry_at.is_some());
        assert_eq!(apply(&mut d, att(Some(503))), Next::RetryIn(120));
        assert_eq!(d.reason, "relay_server_error");
        assert_eq!(apply(&mut d, att(Some(401))), Next::RetryIn(600));
        assert_eq!(d.reason, "relay_signature");
        let mut last = Next::Delivered;
        for _ in 0..20 {
            last = apply(&mut d, att(None));
            if last == Next::GiveUp {
                break;
            }
        }
        assert_eq!(last, Next::GiveUp);
        assert_eq!(d.status, Status::Failed);
        assert_eq!(d.reason, "relay_gave_up");
    }

    #[test]
    fn a_lasting_refusal_stops_early() {
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        assert_eq!(apply(&mut d, att(Some(422))), Next::GiveUp);
        assert_eq!(d.reason, "relay_refused");
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        assert!(matches!(apply(&mut d, att(Some(429))), Next::RetryIn(_)));
    }
}
