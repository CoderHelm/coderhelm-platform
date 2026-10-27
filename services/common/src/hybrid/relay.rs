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

/// The HTTP client for every request the hosted side makes to an install's
/// address (relayed events and ownership proofs). It never follows a
/// redirect: the install's CloudFront is the only place these requests may
/// go, and a redirect from it (to the Lambda runtime API, a metadata address
/// or anything else inside AWS) is answered as the redirect status itself.
pub fn install_client() -> &'static reqwest::Client {
    static C: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("coderhelm-relay")
            .build()
            .expect("a client with no redirects")
    })
}

/// A client for requests to a name an install chose itself (its own
/// domain), which could point anywhere: the name is resolved first, every
/// address it answers with must be public (no private, loopback,
/// link-local, metadata or unique-local address, IPv4-mapped ones
/// included; the outbound webhook sender's rule), and the connection goes
/// to the checked address only, so a second lookup can't move it. No
/// redirects, no proxy.
pub async fn pinned_client(
    dns: &dyn crate::webhooks::deliver::Resolve,
    host: &str,
    port: u16,
) -> Result<reqwest::Client, String> {
    let addrs = dns.lookup(host, port).await?;
    let addr = crate::webhooks::deliver::pick_address(&addrs)?;
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("coderhelm-relay")
        .resolve(host, addr)
        .no_proxy()
        .build()
        .map_err(|e| e.to_string())
}

/// How much of an install's answer is ever read (the rest is dropped).
pub const MAX_ANSWER_BYTES: usize = 16 * 1024;

/// Reads at most [`MAX_ANSWER_BYTES`] of an answer.
pub async fn read_capped(mut resp: reqwest::Response) -> Vec<u8> {
    let mut out = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        let room = MAX_ANSWER_BYTES - out.len();
        out.extend_from_slice(&chunk[..chunk.len().min(room)]);
        if out.len() >= MAX_ANSWER_BYTES {
            break;
        }
    }
    out
}

/// Sends one forward and returns the attempt (with the HTTP status when the
/// install answered).
pub async fn send(f: &Forward<'_>) -> Attempt {
    let ts = chrono::Utc::now().timestamp();
    let started = Instant::now();
    let r = install_client()
        .post(target(f.origin))
        .timeout(std::time::Duration::from_secs(8))
        .header("content-type", "application/json")
        .header(
            sign::RELAY_HEADER,
            sign::sign_relay_routed(f.relay_key, ts, f.delivery_id, f.source, f.event, f.body),
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
            } else if resp.status().is_redirection() {
                "answered with a redirect, which the relay never follows".into()
            } else {
                let text = String::from_utf8_lossy(&read_capped(resp).await).to_string();
                let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                v["error"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or(text)
                    .chars()
                    .take(200)
                    .collect()
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

    struct FakeDns(Vec<&'static str>);

    #[async_trait::async_trait]
    impl crate::webhooks::deliver::Resolve for FakeDns {
        async fn lookup(
            &self,
            _host: &str,
            port: u16,
        ) -> Result<Vec<std::net::SocketAddr>, String> {
            Ok(self
                .0
                .iter()
                .map(|ip| std::net::SocketAddr::new(ip.parse().unwrap(), port))
                .collect())
        }
    }

    /// An install's own domain is fetched only when every address it
    /// resolves to is public.
    #[tokio::test]
    async fn a_domain_proof_never_reaches_inside_a_network() {
        for inside in [
            vec!["10.0.0.5"],
            vec!["127.0.0.1"],
            vec!["169.254.169.254"],
            vec!["fd00:ec2::254"],
            vec!["::ffff:10.0.0.1"],
            vec!["fe80::1"],
            vec!["93.184.216.34", "192.168.1.1"],
            vec![],
        ] {
            assert!(
                pinned_client(&FakeDns(inside.clone()), "helm.example.org", 443)
                    .await
                    .is_err(),
                "{inside:?}"
            );
        }
        assert!(
            pinned_client(&FakeDns(vec!["93.184.216.34"]), "helm.example.org", 443)
                .await
                .is_ok()
        );
    }
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

#[cfg(test)]
mod redirect_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A one-route HTTP server that answers every request with `answer` and
    /// counts how many connections it took.
    async fn serve(answer: String) -> (String, Arc<AtomicUsize>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                h.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 8192];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(answer.as_bytes()).await;
                let _ = s.shutdown().await;
            }
        });
        (addr, hits)
    }

    /// An install's CloudFront answers with a redirect to an internal
    /// address: the relay must never follow it (no request to anything but
    /// the install's own verified address).
    #[tokio::test]
    async fn the_relay_never_follows_a_redirect() {
        let (internal, internal_hits) = serve(
            "HTTP/1.1 500 Internal\r\ncontent-length: 21\r\nconnection: close\r\n\r\n{\"error\":\"internal\"}"
                .to_string(),
        )
        .await;
        for code in [301, 302, 303, 307, 308] {
            let (install, _) = serve(format!(
                "HTTP/1.1 {code} Moved\r\nlocation: {internal}/2018-06-01/runtime/invocation/next\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            ))
            .await;
            let a = send(&Forward {
                origin: &install,
                relay_key: "k",
                source: "github",
                event: "push",
                delivery_id: "d1",
                body: b"{}",
            })
            .await;
            assert_eq!(a.code, Some(code), "{code}");
            assert!(!a.error.contains("internal"), "{code}: {}", a.error);
        }
        assert_eq!(internal_hits.load(Ordering::SeqCst), 0);
    }

    /// A huge error answer is cut, never read whole.
    #[tokio::test]
    async fn a_large_error_answer_is_cut() {
        let big = "x".repeat(200_000);
        let (install, _) = serve(format!(
            "HTTP/1.1 400 Bad\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{big}",
            big.len()
        ))
        .await;
        let a = send(&Forward {
            origin: &install,
            relay_key: "k",
            source: "github",
            event: "push",
            delivery_id: "d1",
            body: b"{}",
        })
        .await;
        assert_eq!(a.code, Some(400));
        assert!(a.error.chars().count() <= 200);
    }
}
