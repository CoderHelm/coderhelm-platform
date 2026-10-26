//! Sending one event to one endpoint, and what happens after.
//!
//! - **Where:** the endpoint's host is resolved again before every send and
//!   every address it answers with must be public (no private, local,
//!   link-local or metadata address). The connection is pinned to the
//!   address that was checked, so a name that changes its answer between
//!   the check and the connection (DNS rebinding) can't reach inside a
//!   network. Redirects are never followed.
//! - **How long:** [`TIMEOUT_SECS`] for the whole request.
//! - **Retries:** a timeout, a connection failure, `408`, `429` or any `5xx`
//!   is tried again on [`BACKOFF`] (about a day in all). Any other answer
//!   that isn't `2xx` is final: a `4xx` won't change by itself.
//! - **Failing endpoints:** an endpoint whose deliveries have failed for
//!   [`DISABLE_AFTER_DAYS`] days with no success in between is turned off,
//!   and the team's admins are told.
//! - **Record:** every attempt is kept on the delivery's record in
//!   Deliveries ([`crate::deliveries`]), with the headers (signature
//!   removed) and a preview of the body.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::deliveries::{self, Attempt, Delivery, Direction, Redeliver, Status};

use super::{endpoint, sign};

pub const SOURCE: &str = "webhook";
pub const TIMEOUT_SECS: u64 = 10;
pub const DISABLE_AFTER_DAYS: i64 = 5;
pub const USER_AGENT: &str = "CoderHelm-Webhooks/1.0 (+https://docs.coderhelm.com/api/webhooks/)";
/// Seconds to wait after the Nth failed attempt (N from 1). Ten attempts in
/// all, over about 24.6 hours.
pub const BACKOFF: &[i64] = &[5, 60, 300, 1800, 3600, 10800, 21600, 21600, 28800];

/// When to try again after `attempts` failed tries, or None to stop.
pub fn next_retry_secs(attempts: usize) -> Option<i64> {
    BACKOFF.get(attempts.saturating_sub(1)).copied()
}

/// How an answer counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Delivered,
    /// Try again later.
    Retry,
    /// Final: trying again won't help.
    Refused,
}

/// Classifies an attempt's HTTP status (None: no answer).
pub fn classify(code: Option<u16>) -> Class {
    match code {
        Some(c) if (200..300).contains(&c) => Class::Delivered,
        None | Some(408) | Some(429) => Class::Retry,
        Some(c) if c >= 500 => Class::Retry,
        Some(_) => Class::Refused,
    }
}

/// The reason code for a failed attempt.
pub fn reason_code(code: Option<u16>, timed_out: bool, blocked: bool) -> &'static str {
    match code {
        _ if blocked => "webhook_blocked",
        None if timed_out => "webhook_timeout",
        None => "webhook_unreachable",
        Some(429) => "webhook_rate_limited",
        Some(c) if c >= 500 => "webhook_server_error",
        Some(c) if (300..400).contains(&c) => "webhook_redirect",
        Some(410) => "webhook_endpoint_gone",
        Some(_) => "webhook_refused",
    }
}

/// What comes after an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Delivered,
    RetryIn(i64),
    Stop,
}

/// What one attempt found, beyond its [`Attempt`].
#[derive(Debug, Clone, Default)]
pub struct Sent {
    pub attempt: Attempt,
    pub timed_out: bool,
    /// Refused before sending: the address isn't public.
    pub blocked: bool,
    /// The request's headers (the signature is removed when recorded).
    pub headers: Vec<(String, String)>,
}

/// Applies an attempt to its delivery record and decides what's next.
/// `retry` is false for a person's Resend and for test events: those are
/// tried once.
pub fn apply(
    d: &mut Delivery,
    sent: &Sent,
    retry: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Next {
    let code = sent.attempt.code;
    d.push_attempt(sent.attempt.clone());
    let n = d.attempts.len();
    let class = if sent.blocked {
        Class::Refused
    } else {
        classify(code)
    };
    match class {
        Class::Delivered => {
            d.finish(
                Status::Handled,
                &format!("Delivered ({})", code.unwrap_or(200)),
                "",
            );
            d.next_retry_at = None;
            Next::Delivered
        }
        Class::Retry if retry => match next_retry_secs(n) {
            Some(secs) => {
                d.finish(
                    Status::Pending,
                    "Retrying",
                    reason_code(code, sent.timed_out, false),
                );
                d.next_retry_at = Some(
                    (now + chrono::Duration::seconds(secs))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                );
                Next::RetryIn(secs)
            }
            None => {
                d.finish(
                    Status::Failed,
                    "Gave up after about a day",
                    "webhook_gave_up",
                );
                d.next_retry_at = None;
                Next::Stop
            }
        },
        _ => {
            let outcome = match code {
                Some(c) => format!("Refused ({c})"),
                None if sent.blocked => "Not sent".to_string(),
                None => "No answer".to_string(),
            };
            d.finish(
                Status::Failed,
                &outcome,
                reason_code(code, sent.timed_out, sent.blocked),
            );
            d.next_retry_at = None;
            Next::Stop
        }
    }
}

/// An endpoint failing since `failing_since` should be turned off now.
pub fn should_disable(failing_since: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(failing_since)
        .map(|t| now.signed_duration_since(t) >= chrono::Duration::days(DISABLE_AFTER_DAYS))
        .unwrap_or(false)
}

/// A new delivery record for an event to an endpoint.
pub fn new_record(
    ep: &endpoint::Endpoint,
    event_id: &str,
    event_type: &str,
    body: &[u8],
) -> Delivery {
    let mut d =
        Delivery::new(Direction::Outbound, SOURCE, event_type, &ep.host()).with_payload(body);
    d.external_id = event_id.into();
    d.endpoint_id = ep.id.clone();
    d.status = Status::Pending;
    d.redeliver = Some(Redeliver::Webhook);
    d
}

// ─── Sending ────────────────────────────────────────────────────────────────

/// Looks up a host's addresses. The system resolver in production; a fake in
/// tests (a name that answers differently each time).
#[async_trait::async_trait]
pub trait Resolve: Send + Sync {
    async fn lookup(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, String>;
}

pub struct SystemDns;

#[async_trait::async_trait]
impl Resolve for SystemDns {
    async fn lookup(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
        tokio::net::lookup_host((host, port))
            .await
            .map(|a| a.collect())
            .map_err(|e| e.to_string())
    }
}

/// How requests are made.
pub struct Sender<'a> {
    pub dns: &'a dyn Resolve,
    pub timeout: Duration,
    /// Tests only: allow `http://` and loopback, so a local server can answer.
    #[cfg(test)]
    pub local_for_tests: bool,
}

impl<'a> Sender<'a> {
    pub fn new(dns: &'a dyn Resolve) -> Self {
        Self {
            dns,
            timeout: Duration::from_secs(TIMEOUT_SECS),
            #[cfg(test)]
            local_for_tests: false,
        }
    }
}

/// One request to send.
pub struct Request<'a> {
    pub url: &'a str,
    pub event_id: &'a str,
    pub body: &'a [u8],
    /// The current secret first, then a rotated-out one while it still signs.
    pub secrets: &'a [String],
    pub timestamp: i64,
}

fn now_text() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn blocked(message: &str) -> Sent {
    Sent {
        attempt: Attempt {
            at: now_text(),
            code: None,
            duration_ms: 0,
            error: message.chars().take(300).collect(),
        },
        blocked: true,
        ..Default::default()
    }
}

/// The address to connect to: every answer must be public, and the first is
/// used. Pure, for the tests.
pub fn pick_address(addrs: &[SocketAddr]) -> Result<SocketAddr, String> {
    if addrs.is_empty() {
        return Err("The address's name doesn't point anywhere.".into());
    }
    if addrs
        .iter()
        .any(|a| !crate::live::net::is_public_ip(a.ip()))
    {
        return Err(
            "The address's name points inside a private network (or to a local or metadata address), so nothing was sent."
                .into(),
        );
    }
    Ok(addrs[0])
}

/// Checks, when an endpoint is saved, that its name resolves only to public
/// addresses (it is checked again before every send).
pub async fn check_address(dns: &dyn Resolve, url: &str) -> Result<(), String> {
    let ep = crate::live::net::parse_endpoint(url).map_err(|e| e.message)?;
    if ep.host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(());
    }
    let addrs = dns.lookup(&ep.host, ep.port).await.map_err(|_| {
        format!(
            "We couldn't find {} on the internet. Check the address.",
            ep.host
        )
    })?;
    pick_address(&addrs).map(|_| ())
}

/// Signs and sends one request. Never errors: every outcome is an attempt.
pub async fn send(sender: &Sender<'_>, req: &Request<'_>) -> Sent {
    #[cfg(test)]
    let local = sender.local_for_tests;
    #[cfg(not(test))]
    let local = false;

    let (url, host, port) = if local {
        let Ok(u) = reqwest::Url::parse(req.url) else {
            return blocked("Not a web address.");
        };
        let host = u.host_str().unwrap_or("").to_string();
        let port = u.port_or_known_default().unwrap_or(80);
        (u, host, port)
    } else {
        match crate::live::net::parse_endpoint(req.url) {
            Ok(ep) => (ep.url, ep.host, ep.port),
            Err(e) => return blocked(&e.message),
        }
    };
    let addr = if local {
        match sender.dns.lookup(&host, port).await {
            Ok(a) if !a.is_empty() => a[0],
            _ => return blocked("The address's name doesn't point anywhere."),
        }
    } else {
        let looked = match host.parse::<std::net::IpAddr>() {
            Ok(ip) => Ok(vec![SocketAddr::new(ip, port)]),
            Err(_) => sender.dns.lookup(&host, port).await,
        };
        match looked {
            Ok(addrs) => match pick_address(&addrs) {
                Ok(a) => a,
                Err(e) => return blocked(&e),
            },
            Err(_) => {
                return Sent {
                    attempt: Attempt {
                        at: now_text(),
                        code: None,
                        duration_ms: 0,
                        error: format!("Couldn't find {host} (DNS)."),
                    },
                    ..Default::default()
                }
            }
        }
    };

    let refs: Vec<&str> = req.secrets.iter().map(String::as_str).collect();
    let signature = match sign::header(&refs, req.event_id, req.timestamp, req.body) {
        Ok(s) => s,
        Err(e) => return blocked(&format!("Couldn't sign: {e}")),
    };
    let headers = vec![
        ("content-type".to_string(), "application/json".to_string()),
        ("user-agent".to_string(), USER_AGENT.to_string()),
        (sign::ID_HEADER.to_string(), req.event_id.to_string()),
        (
            sign::TIMESTAMP_HEADER.to_string(),
            req.timestamp.to_string(),
        ),
        (sign::SIGNATURE_HEADER.to_string(), signature),
    ];

    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(sender.timeout)
        .connect_timeout(sender.timeout.min(Duration::from_secs(5)))
        .resolve(&host, addr)
        .no_proxy()
        .build()
    {
        Ok(c) => c,
        Err(e) => return blocked(&format!("Couldn't prepare the request: {e}")),
    };
    let mut rb = client.post(url).body(req.body.to_vec());
    for (k, v) in &headers {
        rb = rb.header(k, v);
    }
    let started = Instant::now();
    let r = rb.send().await;
    let duration_ms = started.elapsed().as_millis() as u64;
    let at = now_text();
    match r {
        Ok(resp) => {
            let code = resp.status().as_u16();
            let error = if resp.status().is_success() {
                String::new()
            } else {
                let text = resp.text().await.unwrap_or_default();
                format!(
                    "Answered {code}: {}",
                    text.chars().take(200).collect::<String>()
                )
                .trim_end_matches(": ")
                .to_string()
            };
            Sent {
                attempt: Attempt {
                    at,
                    code: Some(code),
                    duration_ms,
                    error,
                },
                headers,
                ..Default::default()
            }
        }
        Err(e) => {
            let timed_out = e.is_timeout();
            Sent {
                attempt: Attempt {
                    at,
                    code: None,
                    duration_ms,
                    error: if timed_out {
                        format!("No answer within {} seconds.", sender.timeout.as_secs())
                    } else {
                        "Couldn't connect.".into()
                    },
                },
                timed_out,
                headers,
                ..Default::default()
            }
        }
    }
}

/// Keeps the request's headers on the record (the signature is removed).
pub fn record_headers(d: &mut Delivery, sent: &Sent) {
    if !sent.headers.is_empty() {
        d.headers =
            deliveries::redact_headers(sent.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn att(code: Option<u16>) -> Sent {
        Sent {
            attempt: Attempt {
                at: "t".into(),
                code,
                duration_ms: 3,
                error: String::new(),
            },
            ..Default::default()
        }
    }

    fn record() -> Delivery {
        let ep = endpoint::Endpoint {
            id: "ep_1".into(),
            url: "https://hooks.example.com/x".into(),
            ..Default::default()
        };
        new_record(&ep, "evt_1", "run.failed", b"{}")
    }

    #[test]
    fn answers_are_classified_like_the_spec_says() {
        assert_eq!(classify(Some(200)), Class::Delivered);
        assert_eq!(classify(Some(204)), Class::Delivered);
        for c in [
            None,
            Some(408),
            Some(429),
            Some(500),
            Some(502),
            Some(503),
            Some(599),
        ] {
            assert_eq!(classify(c), Class::Retry, "{c:?}");
        }
        for c in [
            Some(301),
            Some(400),
            Some(401),
            Some(403),
            Some(404),
            Some(410),
            Some(422),
        ] {
            assert_eq!(classify(c), Class::Refused, "{c:?}");
        }
    }

    #[test]
    fn retries_back_off_for_about_a_day_then_stop() {
        let total: i64 = BACKOFF.iter().sum();
        assert!((22 * 3600..=26 * 3600).contains(&total), "{total}");
        assert!(BACKOFF.windows(2).all(|w| w[0] <= w[1]), "never shorter");
        let now = chrono::Utc::now();
        let mut d = record();
        assert_eq!(d.target, "hooks.example.com");
        assert_eq!(apply(&mut d, &att(Some(503)), true, now), Next::RetryIn(5));
        assert_eq!(
            (d.status, d.reason.as_str()),
            (Status::Pending, "webhook_server_error")
        );
        assert!(d.next_retry_at.is_some());
        assert_eq!(apply(&mut d, &att(None), true, now), Next::RetryIn(60));
        assert_eq!(d.reason, "webhook_unreachable");
        assert_eq!(
            apply(&mut d, &att(Some(429)), true, now),
            Next::RetryIn(300)
        );
        assert_eq!(d.reason, "webhook_rate_limited");
        let mut last = Next::Delivered;
        for _ in 0..20 {
            last = apply(&mut d, &att(Some(500)), true, now);
            if last == Next::Stop {
                break;
            }
        }
        assert_eq!(last, Next::Stop);
        assert_eq!(d.attempts.len(), BACKOFF.len() + 1);
        assert_eq!(
            (d.status, d.reason.as_str()),
            (Status::Failed, "webhook_gave_up")
        );
    }

    #[test]
    fn a_4xx_stops_at_once_but_408_and_429_retry() {
        let now = chrono::Utc::now();
        for (code, reason) in [
            (400, "webhook_refused"),
            (404, "webhook_refused"),
            (410, "webhook_endpoint_gone"),
            (302, "webhook_redirect"),
        ] {
            let mut d = record();
            assert_eq!(apply(&mut d, &att(Some(code)), true, now), Next::Stop);
            assert_eq!((d.status, d.reason.as_str()), (Status::Failed, reason));
        }
        let mut d = record();
        assert!(matches!(
            apply(&mut d, &att(Some(408)), true, now),
            Next::RetryIn(_)
        ));
        let mut d = record();
        assert_eq!(apply(&mut d, &att(Some(200)), true, now), Next::Delivered);
        assert_eq!(d.status, Status::Handled);
        // A Resend or a test is tried once.
        let mut d = record();
        assert_eq!(apply(&mut d, &att(Some(503)), false, now), Next::Stop);
        assert_eq!(d.status, Status::Failed);
        // A blocked address is final.
        let mut d = record();
        let mut b = att(None);
        b.blocked = true;
        assert_eq!(apply(&mut d, &b, true, now), Next::Stop);
        assert_eq!(d.reason, "webhook_blocked");
        let mut t = att(None);
        t.timed_out = true;
        let mut d = record();
        apply(&mut d, &t, true, now);
        assert_eq!(d.reason, "webhook_timeout");
    }

    #[test]
    fn an_endpoint_is_turned_off_after_days_of_failures() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-26T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(!should_disable("2026-10-22T12:00:01Z", now));
        assert!(should_disable("2026-10-21T12:00:00Z", now));
        assert!(should_disable("2026-10-01T00:00:00Z", now));
        assert!(!should_disable("garbage", now));
    }

    #[test]
    fn every_reason_reads_plainly() {
        for c in [
            "webhook_blocked",
            "webhook_timeout",
            "webhook_unreachable",
            "webhook_rate_limited",
            "webhook_server_error",
            "webhook_redirect",
            "webhook_endpoint_gone",
            "webhook_refused",
            "webhook_gave_up",
            "webhook_disabled",
        ] {
            assert_eq!(deliveries::reason(c).code, c);
        }
    }

    /// Answers each name with the next list of addresses (a name that
    /// changes its answer, as in DNS rebinding).
    struct Rebinding {
        answers: Mutex<Vec<Vec<SocketAddr>>>,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Resolve for Rebinding {
        async fn lookup(&self, _host: &str, _port: u16) -> Result<Vec<SocketAddr>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut a = self.answers.lock().unwrap();
            if a.is_empty() {
                return Err("NXDOMAIN".into());
            }
            Ok(a.remove(0))
        }
    }

    fn req<'a>(url: &'a str, secrets: &'a [String]) -> Request<'a> {
        Request {
            url,
            event_id: "evt_1",
            body: b"{}",
            secrets,
            timestamp: 1_800_000_000,
        }
    }

    #[tokio::test]
    async fn private_addresses_are_refused_at_send_time_after_dns() {
        let secrets = vec![sign::new_secret()];
        // The name was public when the endpoint was saved; now it answers
        // with the metadata address. Nothing is sent.
        let dns = Rebinding {
            answers: Mutex::new(vec![
                vec!["169.254.169.254:443".parse().unwrap()],
                vec![
                    "93.184.216.34:443".parse().unwrap(),
                    "10.0.0.7:443".parse().unwrap(),
                ],
                vec!["[::1]:443".parse().unwrap()],
                vec![],
            ]),
            calls: AtomicUsize::new(0),
        };
        let s = Sender::new(&dns);
        for _ in 0..3 {
            let sent = send(&s, &req("https://hooks.example.com/in", &secrets)).await;
            assert!(sent.blocked, "{:?}", sent.attempt);
            assert!(sent.attempt.error.contains("private network"));
            assert!(sent.headers.is_empty(), "nothing was sent");
        }
        assert_eq!(dns.calls.load(Ordering::SeqCst), 3);
        // Literal private addresses never reach DNS.
        for url in [
            "https://127.0.0.1/x",
            "https://[fe80::1]/x",
            "https://169.254.169.254/latest",
            "http://hooks.example.com/",
        ] {
            let sent = send(&s, &req(url, &secrets)).await;
            assert!(sent.blocked, "{url}");
        }
        assert_eq!(dns.calls.load(Ordering::SeqCst), 3);
        // The same rule when an endpoint is saved.
        let at_save = Rebinding {
            answers: Mutex::new(vec![
                vec!["10.1.2.3:443".parse().unwrap()],
                vec!["93.184.216.34:443".parse().unwrap()],
            ]),
            calls: AtomicUsize::new(0),
        };
        assert!(check_address(&at_save, "https://hooks.example.com/")
            .await
            .unwrap_err()
            .contains("private network"));
        assert!(check_address(&at_save, "https://hooks.example.com/")
            .await
            .is_ok());
        assert!(check_address(&at_save, "https://hooks.example.com/")
            .await
            .unwrap_err()
            .contains("couldn't find"));
        assert_eq!(
            pick_address(&["8.8.8.8:443".parse().unwrap()]).unwrap(),
            "8.8.8.8:443".parse().unwrap()
        );
        assert!(pick_address(&[]).is_err());
    }

    /// A one-request HTTP server on loopback that answers `status` after
    /// `delay`, and hands back the raw request it got.
    async fn serve_once(status: u16, delay: Duration) -> (u16, tokio::task::JoinHandle<String>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            let (mut sock, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 16384];
            let mut got = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap_or(0);
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).to_string();
                if n == 0 || (text.contains("\r\n\r\n") && text.ends_with("{}")) {
                    break;
                }
            }
            tokio::time::sleep(delay).await;
            let resp =
                format!("HTTP/1.1 {status} X\r\ncontent-length: 2\r\nconnection: close\r\n\r\nno");
            let _ = sock.write_all(resp.as_bytes()).await;
            String::from_utf8_lossy(&got).to_string()
        });
        (port, h)
    }

    struct Loopback;
    #[async_trait::async_trait]
    impl Resolve for Loopback {
        async fn lookup(&self, _h: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
            Ok(vec![SocketAddr::from(([127, 0, 0, 1], port))])
        }
    }

    #[tokio::test]
    async fn a_request_carries_standard_webhooks_headers_that_verify() {
        let (port, server) = serve_once(204, Duration::ZERO).await;
        let secrets = vec![sign::new_secret(), sign::new_secret()];
        let mut s = Sender::new(&Loopback);
        s.local_for_tests = true;
        let url = format!("http://receiver.test:{port}/hook");
        let sent = send(&s, &req(&url, &secrets)).await;
        assert_eq!(sent.attempt.code, Some(204), "{:?}", sent.attempt);
        let raw = server.await.unwrap().to_ascii_lowercase();
        let header = |name: &str| {
            raw.lines()
                .find_map(|l| l.strip_prefix(&format!("{name}: ")))
                .unwrap_or("")
                .trim()
                .to_string()
        };
        assert_eq!(header("webhook-id"), "evt_1");
        assert_eq!(header("webhook-timestamp"), "1800000000");
        assert!(header("user-agent").starts_with("coderhelm-webhooks/"));
        // Read the signature from what was sent, with its case intact.
        let sig = sent
            .headers
            .iter()
            .find(|(k, _)| k == sign::SIGNATURE_HEADER)
            .unwrap()
            .1
            .clone();
        assert_eq!(
            sig.split(' ').count(),
            2,
            "both secrets sign during a rotation"
        );
        for secret in &secrets {
            assert!(
                sign::verify(secret, "evt_1", "1800000000", &sig, b"{}", 1_800_000_000).is_ok()
            );
        }
        let mut d = record();
        record_headers(&mut d, &sent);
        assert_eq!(d.headers["webhook-signature"], "[redacted]");
        assert_eq!(d.headers["webhook-id"], "evt_1");
    }

    #[tokio::test]
    async fn a_slow_endpoint_times_out_and_is_retried() {
        let (port, _server) = serve_once(200, Duration::from_secs(3)).await;
        let secrets = vec![sign::new_secret()];
        let mut s = Sender::new(&Loopback);
        s.local_for_tests = true;
        s.timeout = Duration::from_millis(300);
        let sent = send(&s, &req(&format!("http://receiver.test:{port}/"), &secrets)).await;
        assert!(sent.timed_out && sent.attempt.code.is_none());
        let mut d = record();
        assert!(matches!(
            apply(&mut d, &sent, true, chrono::Utc::now()),
            Next::RetryIn(5)
        ));
        assert_eq!(d.reason, "webhook_timeout");
    }

    #[tokio::test]
    async fn a_server_error_is_recorded_with_its_code() {
        let (port, _server) = serve_once(503, Duration::ZERO).await;
        let secrets = vec![sign::new_secret()];
        let mut s = Sender::new(&Loopback);
        s.local_for_tests = true;
        let sent = send(&s, &req(&format!("http://receiver.test:{port}/"), &secrets)).await;
        assert_eq!(sent.attempt.code, Some(503));
        assert!(sent.attempt.error.starts_with("Answered 503"));
    }
}
