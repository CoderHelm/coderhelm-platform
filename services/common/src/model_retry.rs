//! Retrying calls to the model API on failures that pass on their own.
//!
//! What is worth retrying is decided by the HTTP status and the kind of
//! network error, never by the text of an error message. Every 5xx (the
//! Cloudflare 520–524 errors in front of the API included), 408 request
//! timeout, 429 rate limit and 529 overloaded is transient; so is a request
//! that never got a response (connect, timeout, a body cut off mid-read).
//! Anything else (400, 401, 403, 404, 413, 422) is the caller's own problem
//! and fails at once.

use std::time::Duration;

/// Waits (seconds) before each retry for background work: 2, 4, 8, 16, 32.
pub const WORKER_BACKOFF_SECS: [u64; 5] = [2, 4, 8, 16, 32];

/// Waits (seconds) before each retry for a request a person is waiting on.
pub const INTERACTIVE_BACKOFF_SECS: [u64; 3] = [1, 2, 4];

/// Waits (seconds) before a job that failed on a transient model error is
/// sent again: 2, 10 and 15 minutes (the longest one SQS delay holds). One
/// policy for every pass: review, ticket run, feedback, CI-fix resume.
pub const REQUEUE_DELAYS_SECS: [u64; 3] = [120, 600, 900];

/// The wait before requeue number `retries_so_far + 1`, or None once the
/// policy is spent (the caller then takes its terminal path).
pub fn requeue_delay(retries_so_far: u32) -> Option<u64> {
    REQUEUE_DELAYS_SECS.get(retries_so_far as usize).copied()
}

/// The longest `retry-after` the server may ask for that is honored as is.
const MAX_RETRY_AFTER_SECS: u64 = 60;

/// True when a response with this status may succeed if sent again.
pub fn is_transient_status(status: u16) -> bool {
    matches!(status, 408 | 429) || (500..=599).contains(&status)
}

/// True when a request failed without a usable response for a reason that may
/// pass: it could not connect, timed out, or the body was cut off.
pub fn is_transient_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request() || e.is_body() || e.is_decode()
}

/// The wait before retry `attempt` (0-based): the schedule's step, give or
/// take a quarter, so many callers failing together don't retry together.
/// `retry_after` from the server wins when it is longer and not absurd.
pub fn backoff(schedule: &[u64], attempt: usize, retry_after: Option<u64>) -> Duration {
    let base = schedule.get(attempt).copied().unwrap_or(0);
    let jittered = jitter(Duration::from_secs(base));
    match retry_after.filter(|s| *s <= MAX_RETRY_AFTER_SECS) {
        Some(s) if Duration::from_secs(s) > jittered => Duration::from_secs(s),
        _ => jittered,
    }
}

/// `d` scaled by a factor in [0.75, 1.25).
fn jitter(d: Duration) -> Duration {
    if d.is_zero() {
        return d;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.subsec_nanos())
        .unwrap_or(0);
    // Mix the clock so consecutive calls don't land on the same fraction.
    let mixed = (u64::from(nanos)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
    let frac = (mixed % 1000) as f64 / 1000.0; // [0, 1)
    d.mul_f64(0.75 + frac * 0.5)
}

/// What went wrong with a model call: one type for every model path (the
/// worker's client, the gateway's plan chat and its stream, the key check),
/// so whether to retry, requeue or give up is decided from the kind, never
/// from the text of a message.
#[derive(Debug)]
pub struct ModelCallError {
    pub kind: ModelErrorKind,
    /// What the API or the transport said, for logs.
    pub message: String,
}

/// The kind of a model call failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelErrorKind {
    /// The API answered with this non-success status.
    Status(u16),
    /// No usable response: could not connect, timed out, body cut off.
    Network,
    /// The model declined to answer.
    Refusal,
    /// The answer stopped at the token limit before it finished.
    Truncated,
    /// The answer arrived but wasn't in the shape asked for.
    BadOutput,
}

impl ModelCallError {
    pub fn new(kind: ModelErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// A failure that may pass if the call is made again later.
    pub fn is_transient(&self) -> bool {
        match self.kind {
            ModelErrorKind::Status(s) => is_transient_status(s),
            ModelErrorKind::Network => true,
            ModelErrorKind::Refusal | ModelErrorKind::Truncated | ModelErrorKind::BadOutput => {
                false
            }
        }
    }

    /// The HTTP status, when the API answered.
    pub fn status(&self) -> Option<u16> {
        match self.kind {
            ModelErrorKind::Status(s) => Some(s),
            _ => None,
        }
    }
}

impl std::fmt::Display for ModelCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ModelErrorKind::Status(s) => write!(f, "Anthropic API error ({s}): {}", self.message),
            ModelErrorKind::Network => write!(f, "Anthropic API request failed: {}", self.message),
            ModelErrorKind::Refusal => write!(f, "The model declined: {}", self.message),
            ModelErrorKind::Truncated => write!(f, "The answer was cut off: {}", self.message),
            ModelErrorKind::BadOutput => write!(f, "The answer wasn't usable: {}", self.message),
        }
    }
}

impl std::error::Error for ModelCallError {}

impl From<reqwest::Error> for ModelCallError {
    fn from(e: reqwest::Error) -> Self {
        ModelCallError::new(ModelErrorKind::Network, e.to_string())
    }
}

/// The model call failure inside `e` (itself or anywhere in its source
/// chain), if it is one.
pub fn model_error<'a>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a ModelCallError> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(m) = err.downcast_ref::<ModelCallError>() {
            return Some(m);
        }
        cur = err.source();
    }
    None
}

/// True when `e` is a model call failure that may pass on a later try.
pub fn is_transient_model_error(e: &(dyn std::error::Error + 'static)) -> bool {
    model_error(e).is_some_and(ModelCallError::is_transient)
}

/// A response that arrived: its status, `retry-after` (seconds) and body.
#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// This (unsuccessful) response as a model call failure.
    pub fn into_error(self) -> ModelCallError {
        let message = self.text();
        ModelCallError::new(ModelErrorKind::Status(self.status), message)
    }
}

/// Sends the request `build` makes, and sends it again after each wait in
/// `schedule` while the failure is transient. Returns the last response
/// (successful or not) once it is final, or the network error when the last
/// attempt never got one. `label` names the caller in the logs.
pub async fn send<F>(build: F, schedule: &[u64], label: &str) -> Result<Reply, ModelCallError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let attempts = schedule.len() + 1;
    for attempt in 0..attempts {
        let last = attempt + 1 == attempts;
        let outcome = match build().send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok());
                match resp.bytes().await {
                    Ok(body) => Ok((
                        Reply {
                            status,
                            body: body.to_vec(),
                        },
                        retry_after,
                    )),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };
        match outcome {
            Ok((reply, retry_after)) => {
                if last || !is_transient_status(reply.status) {
                    return Ok(reply);
                }
                let wait = backoff(schedule, attempt, retry_after);
                tracing::warn!(
                    caller = label,
                    status = reply.status,
                    attempt = attempt + 1,
                    wait_ms = wait.as_millis() as u64,
                    "Model API transient status, retrying"
                );
                tokio::time::sleep(wait).await;
            }
            Err(e) => {
                if last || !is_transient_error(&e) {
                    return Err(e.into());
                }
                let wait = backoff(schedule, attempt, None);
                tracing::warn!(
                    caller = label,
                    error = %e,
                    attempt = attempt + 1,
                    wait_ms = wait.as_millis() as u64,
                    "Model API request failed, retrying"
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
    unreachable!("the last attempt always returns")
}

/// Like [`send`] for a streaming request: retries only while no successful
/// response has arrived, and hands back the live response (body unread) once
/// one does, so the caller can stream it. A final failure comes back as the
/// status and its body text.
pub async fn send_streaming<F>(
    build: F,
    schedule: &[u64],
    label: &str,
) -> Result<reqwest::Response, ModelCallError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let attempts = schedule.len() + 1;
    for attempt in 0..attempts {
        let last = attempt + 1 == attempts;
        match build().send().await {
            Ok(resp) if resp.status().is_success() => return Ok(resp),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok());
                let body = resp.text().await.unwrap_or_default();
                if last || !is_transient_status(status) {
                    return Err(ModelCallError::new(ModelErrorKind::Status(status), body));
                }
                let wait = backoff(schedule, attempt, retry_after);
                tracing::warn!(
                    caller = label,
                    status,
                    attempt = attempt + 1,
                    "Model API transient status, retrying"
                );
                tokio::time::sleep(wait).await;
            }
            Err(e) => {
                if last || !is_transient_error(&e) {
                    return Err(e.into());
                }
                let wait = backoff(schedule, attempt, None);
                tracing::warn!(caller = label, error = %e, attempt = attempt + 1, "Model API request failed, retrying");
                tokio::time::sleep(wait).await;
            }
        }
    }
    unreachable!("the last attempt always returns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn statuses_are_classified_by_code() {
        for s in [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529] {
            assert!(is_transient_status(s), "{s}");
        }
        for s in [200, 400, 401, 403, 404, 413, 422] {
            assert!(!is_transient_status(s), "{s}");
        }
    }

    #[test]
    fn backoff_follows_the_schedule_with_jitter() {
        for (i, base) in WORKER_BACKOFF_SECS.iter().enumerate() {
            let d = backoff(&WORKER_BACKOFF_SECS, i, None).as_secs_f64();
            let b = *base as f64;
            assert!(d >= b * 0.75 && d < b * 1.25, "{i}: {d}");
        }
        // A longer retry-after from the server wins; an absurd one doesn't.
        assert_eq!(backoff(&[1], 0, Some(30)), Duration::from_secs(30));
        assert!(backoff(&[1], 0, Some(3600)) < Duration::from_secs(2));
    }

    /// A one-route server that answers each request with the next status in
    /// `statuses` (the last one repeats). Returns its URL and the hit count.
    async fn server(statuses: Vec<u16>) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let n = h.fetch_add(1, Ordering::SeqCst);
                let status = *statuses.get(n).or(statuses.last()).unwrap();
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = if status == 200 {
                    "{\"ok\":true}"
                } else {
                    "error code: 520"
                };
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        (format!("http://{addr}/v1/messages"), hits)
    }

    #[tokio::test]
    async fn a_520_is_retried_until_it_succeeds() {
        let (url, hits) = server(vec![520, 529, 200]).await;
        let http = reqwest::Client::new();
        let reply = send(|| http.post(&url), &[0, 0, 0], "test").await.unwrap();
        assert_eq!(reply.status, 200);
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_client_error_is_not_retried() {
        let (url, hits) = server(vec![400, 200]).await;
        let http = reqwest::Client::new();
        let reply = send(|| http.post(&url), &[0, 0], "test").await.unwrap();
        assert_eq!(reply.status, 400);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_stop_after_the_schedule() {
        let (url, hits) = server(vec![520]).await;
        let http = reqwest::Client::new();
        let reply = send(|| http.post(&url), &[0, 0], "test").await.unwrap();
        assert_eq!(reply.status, 520);
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        let failed = send_streaming(|| http.post(&url), &[0], "test").await;
        let failed = failed.unwrap_err();
        assert_eq!(failed.kind, ModelErrorKind::Status(520));
        assert!(failed.is_transient());
    }

    #[tokio::test]
    async fn a_refused_connection_is_retried() {
        // Bind then drop: nothing listens on the port.
        let addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let http = reqwest::Client::new();
        let url = format!("http://{addr}/");
        let tries = AtomicUsize::new(0);
        let r = send(
            || {
                tries.fetch_add(1, Ordering::SeqCst);
                http.get(&url)
            },
            &[0, 0],
            "test",
        )
        .await;
        let e = r.unwrap_err();
        assert_eq!(e.kind, ModelErrorKind::Network);
        assert!(e.is_transient());
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn retry_decisions_come_from_the_kind_never_the_text() {
        // A 400 whose message happens to mention 529 and "overloaded" is not
        // transient; a 520 with an empty body is.
        let e = ModelCallError::new(ModelErrorKind::Status(400), "overloaded 529 timed out");
        assert!(!e.is_transient());
        assert!(ModelCallError::new(ModelErrorKind::Status(520), "").is_transient());
        for k in [
            ModelErrorKind::Refusal,
            ModelErrorKind::Truncated,
            ModelErrorKind::BadOutput,
        ] {
            assert!(!ModelCallError::new(k, "").is_transient());
        }
        // Found through a boxed error and a wrapping source chain.
        let boxed: Box<dyn std::error::Error + Send + Sync> =
            Box::new(ModelCallError::new(ModelErrorKind::Status(529), "x"));
        assert!(is_transient_model_error(boxed.as_ref()));
        #[derive(Debug)]
        struct Wrap(ModelCallError);
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "wrapped")
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let w = Wrap(ModelCallError::new(ModelErrorKind::Network, "reset"));
        assert!(is_transient_model_error(&w));
        let plain: Box<dyn std::error::Error + Send + Sync> = "520 in a string".into();
        assert!(!is_transient_model_error(plain.as_ref()));
    }
}
