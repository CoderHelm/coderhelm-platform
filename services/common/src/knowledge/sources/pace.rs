//! Reading a provider at a pace it accepts: one layer for every knowledge
//! source.
//!
//! Providers limit how fast one account may read (Sanity's MCP server and
//! Notion answer `429`, Google Drive answers `403 rateLimitExceeded`, S3
//! answers `SlowDown`) and, under load, answer late, answer `502` to `504`
//! or drop the connection. Every call a source makes goes through a
//! [`Gate`] for its provider (the HTTP connectors through
//! [`super::connector::send`], MCP-based sources through [`Paced`]):
//!
//! - at most [`Policy::concurrency`] calls are in flight to one provider at
//!   once (shared by every sync in this process that reads it);
//! - calls start at least a gap apart ([`Policy::min_gap_ms`]: Notion's
//!   documented three requests a second, for example), and the gap widens
//!   after each rate limit (halving the pace, up to [`Policy::max_gap_ms`])
//!   and narrows again, slowly, as calls succeed;
//! - a refusal that is passing ([`ConnectorError::is_busy`]) is asked again:
//!   after the provider's `Retry-After` when it gives one (every call to
//!   that provider waits it out, since the limit is the account's), else
//!   after an exponential backoff with full jitter;
//! - after [`Policy::retries`] more tries, or when the provider asks for a
//!   wait longer than [`Policy::max_wait_secs`], the error goes back to the
//!   caller with the wait it should keep, so a sync parks the item as
//!   "waiting to retry" instead of holding a worker ([`super::sync`]).
//!
//! Permanent errors (not found, no access, a bad request) are returned at
//! once, never retried.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use rand::Rng;
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::connector::{Busy, ConnectorError};
use super::mcp::{McpTools, Tools};
use crate::mcp_remote::client::RemoteTool;
use crate::mcp_remote::http::HttpResponse;

/// How hard one provider is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Calls in flight to the provider at once.
    pub concurrency: usize,
    /// More tries of one call after a passing refusal.
    pub retries: usize,
    /// The first backoff (milliseconds); each next one doubles, with jitter.
    pub base_ms: u64,
    /// The longest backoff (milliseconds).
    pub max_backoff_ms: u64,
    /// The longest wait held inside a call (seconds). A provider asking for
    /// longer gets the wait handed to the caller, which continues later.
    pub max_wait_secs: u64,
    /// The gap between call starts while the provider is calm (milliseconds).
    pub min_gap_ms: u64,
    /// The widest gap after repeated rate limits (milliseconds).
    pub max_gap_ms: u64,
}

impl Policy {
    /// Any provider without tuning of its own.
    pub const DEFAULT: Policy = Policy {
        concurrency: 4,
        retries: 4,
        base_ms: 500,
        max_backoff_ms: 16_000,
        max_wait_secs: 30,
        min_gap_ms: 0,
        max_gap_ms: 4_000,
    };

    /// Sanity's MCP server: two calls at once (a sync of two projects with
    /// four at once was refused with `429` for most of its documents).
    pub const SANITY: Policy = Policy {
        concurrency: 2,
        min_gap_ms: 100,
        ..Policy::DEFAULT
    };

    /// Notion: an average of three requests a second per connection.
    pub const NOTION: Policy = Policy {
        concurrency: 3,
        min_gap_ms: 340,
        ..Policy::DEFAULT
    };

    /// A website: polite to someone else's server.
    pub const WEB: Policy = Policy {
        concurrency: 2,
        min_gap_ms: 250,
        ..Policy::DEFAULT
    };

    /// A search a person waits for (a tool connected in place): a short
    /// wait or two, never the minutes a sync can afford.
    pub const SEARCH: Policy = Policy {
        retries: 2,
        max_wait_secs: 5,
        max_backoff_ms: 2_000,
        ..Policy::DEFAULT
    };

    /// The policy for a provider by the name its errors use.
    pub fn for_provider(provider: &str) -> Policy {
        match provider {
            "Notion" => Policy::NOTION,
            "Sanity" => Policy {
                // Sanity's HTTP API: 25 requests a second.
                min_gap_ms: 50,
                ..Policy::DEFAULT
            },
            "The website" | "Website" => Policy::WEB,
            _ => Policy::DEFAULT,
        }
    }
}

/// The pace shared by everything reading one provider.
struct Pace {
    /// The gap between call starts now.
    gap: Duration,
    /// When the next call may start.
    next: Instant,
}

/// One provider's gate: its in-flight limit, its pace and its retries.
pub struct Gate {
    policy: Policy,
    permits: Semaphore,
    pace: Mutex<Pace>,
    in_flight: AtomicUsize,
    /// The most calls ever in flight at once (for tests and logs).
    peak: AtomicUsize,
}

/// Gates by provider, so two syncs reading the same provider in one
/// process share its limit. `key` names the provider and whose access
/// (an MCP server and sign-in, an API host).
pub fn gate_for(key: &str, policy: &Policy) -> Arc<Gate> {
    static GATES: std::sync::OnceLock<Mutex<HashMap<String, Weak<Gate>>>> =
        std::sync::OnceLock::new();
    let mut gates = GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    gates.retain(|_, g| g.strong_count() > 0);
    // Tests each run on their own thread and clock (some paused): a gate
    // is never shared between two of them.
    #[cfg(test)]
    let key = &format!("{key}#{:?}", std::thread::current().id());
    if let Some(g) = gates.get(key).and_then(Weak::upgrade) {
        return g;
    }
    let g = Arc::new(Gate::new(*policy));
    gates.insert(key.to_string(), Arc::downgrade(&g));
    g
}

/// The wait and kind of a passing refusal.
fn busy_parts(e: &ConnectorError) -> (Option<u64>, bool) {
    match e {
        ConnectorError::Busy(b) => (b.retry_after, b.rate_limited),
        ConnectorError::RateLimited(_) => (None, true),
        _ => (None, false),
    }
}

/// `e` with the wait the caller should keep before it asks again.
fn with_wait(e: ConnectorError, secs: u64) -> ConnectorError {
    match e {
        ConnectorError::Busy(b) => ConnectorError::Busy(Busy {
            retry_after: Some(b.retry_after.unwrap_or(secs).max(secs)),
            ..b
        }),
        ConnectorError::RateLimited(m) => ConnectorError::rate_limited(m, Some(secs)),
        other => other,
    }
}

impl Gate {
    pub fn new(policy: Policy) -> Self {
        Gate {
            policy,
            permits: Semaphore::new(policy.concurrency.max(1)),
            pace: Mutex::new(Pace {
                gap: Duration::from_millis(policy.min_gap_ms),
                next: Instant::now(),
            }),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// The most calls that were ever in flight at once.
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// The gap between call starts now.
    pub fn gap(&self) -> Duration {
        self.pace.lock().unwrap_or_else(|p| p.into_inner()).gap
    }

    /// Waits for this call's turn, and books the next one a gap later.
    async fn turn(&self) {
        let at = {
            let mut p = self.pace.lock().unwrap_or_else(|p| p.into_inner());
            let at = p.next.max(Instant::now());
            p.next = at + p.gap;
            at
        };
        tokio::time::sleep_until(at).await;
    }

    /// Every call through this gate waits `secs` before starting.
    fn hold_all(&self, secs: u64) {
        let mut p = self.pace.lock().unwrap_or_else(|p| p.into_inner());
        let until = Instant::now() + Duration::from_secs(secs);
        if until > p.next {
            p.next = until;
        }
    }

    /// A rate limit: half the pace.
    fn slow_down(&self) {
        let mut p = self.pace.lock().unwrap_or_else(|p| p.into_inner());
        let wider = (p.gap * 2).max(Duration::from_millis(250));
        p.gap = wider.min(Duration::from_millis(self.policy.max_gap_ms));
    }

    /// A call that went through: the pace comes back a little.
    fn calm(&self) {
        let mut p = self.pace.lock().unwrap_or_else(|p| p.into_inner());
        let floor = Duration::from_millis(self.policy.min_gap_ms);
        if p.gap > floor {
            p.gap = (p.gap - p.gap / 16).max(floor);
        }
    }

    /// The backoff before try `attempt` + 1: full jitter over an
    /// exponentially growing window.
    fn backoff(&self, attempt: usize) -> Duration {
        let cap = self
            .policy
            .base_ms
            .saturating_mul(1u64 << attempt.min(20))
            .min(self.policy.max_backoff_ms)
            .max(1);
        Duration::from_millis(rand::rng().random_range(cap / 2..=cap))
    }

    /// One try, inside the in-flight limit and after its turn.
    async fn once<T, Fut>(&self, call: Fut) -> Result<T, ConnectorError>
    where
        Fut: Future<Output = Result<T, ConnectorError>>,
    {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| ConnectorError::Temporary("The reader stopped.".into()))?;
        self.turn().await;
        let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        let r = call.await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        r
    }

    /// Runs `call` (a fresh try each time it is called) within this gate's
    /// limit and pace, asking again after a passing refusal. `what` names
    /// the call in the log.
    pub async fn run<T, F, Fut>(&self, what: &str, mut call: F) -> Result<T, ConnectorError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ConnectorError>>,
    {
        let mut attempt = 0;
        loop {
            let e = match self.once(call()).await {
                Ok(v) => {
                    self.calm();
                    return Ok(v);
                }
                Err(e) if e.is_busy() => e,
                Err(e) => return Err(e),
            };
            let (after, rate_limited) = busy_parts(&e);
            if rate_limited {
                self.slow_down();
            }
            if let Some(a) = after {
                // The limit is the account's: every call waits it out.
                self.hold_all(a.min(self.policy.max_wait_secs));
            }
            let too_long = after.is_some_and(|a| a > self.policy.max_wait_secs);
            if attempt >= self.policy.retries || too_long {
                // Handed back: at least the wait the provider named, else the
                // next backoff, rounded up to a second.
                let next = after.unwrap_or_else(|| self.backoff(attempt).as_secs().max(1));
                tracing::info!(
                    call = what,
                    tries = attempt + 1,
                    wait_secs = next,
                    rate_limited,
                    "provider still refusing; handed back to try later"
                );
                return Err(with_wait(e, next));
            }
            if after.is_none() {
                tokio::time::sleep(self.backoff(attempt)).await;
            }
            attempt += 1;
        }
    }
}

/// How long a provider asks to wait: `Retry-After` in seconds or as an HTTP
/// date, else a rate limit's reset time (`X-RateLimit-Reset` as unix
/// seconds or RFC 3339, GitHub's and Atlassian's), cut to 15 minutes.
pub fn retry_after_of(r: &HttpResponse) -> Option<u64> {
    let now = chrono::Utc::now().timestamp();
    let date = |v: &str| {
        chrono::DateTime::parse_from_rfc2822(v)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(v))
            .ok()
            .map(|t| (t.timestamp() - now).max(0) as u64)
    };
    let after = r.header("retry-after").and_then(|v| {
        let v = v.trim();
        v.parse::<u64>().ok().or_else(|| date(v))
    });
    let reset = || {
        r.header("x-ratelimit-reset").and_then(|v| {
            let v = v.trim();
            match v.parse::<i64>() {
                // Unix seconds (GitHub), or seconds from now.
                Ok(n) if n > 1_000_000_000 => Some((n - now).max(0) as u64),
                Ok(n) => Some(n.max(0) as u64),
                Err(_) => date(v),
            }
        })
    };
    after
        .or_else(reset)
        .map(|s| s.min(crate::mcp_remote::client::MAX_RETRY_AFTER_SECS))
}

/// An [`McpTools`] that reads its server through a [`Gate`].
pub struct Paced {
    inner: Tools,
    gate: Arc<Gate>,
}

impl Paced {
    /// Paces `inner`, sharing the gate of every [`Paced`] with the same
    /// `server` key (its address and whose sign-in).
    pub fn new(inner: Tools, policy: Policy, server: &str) -> Self {
        Paced {
            inner,
            gate: gate_for(server, &policy),
        }
    }

    /// The most calls that were ever in flight at once.
    pub fn peak(&self) -> usize {
        self.gate.peak()
    }

    /// The gap between call starts now.
    pub fn gap(&self) -> Duration {
        self.gate.gap()
    }
}

#[async_trait::async_trait]
impl McpTools for Paced {
    async fn tools(&self) -> Result<Vec<RemoteTool>, ConnectorError> {
        self.gate.run("tools/list", || self.inner.tools()).await
    }

    async fn call(&self, tool: &RemoteTool, args: &Value) -> Result<Value, ConnectorError> {
        self.gate
            .run(&tool.name, || self.inner.call(tool, args))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::mcp::fake::tool_with;
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicU64;

    /// A server whose calls take 50 ms, answering from a script of
    /// results (the last repeats), counting calls in flight.
    struct Slow {
        script: Mutex<Vec<Result<Value, ConnectorError>>>,
        calls: AtomicUsize,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        /// When each call started (milliseconds since the test began).
        starts: Mutex<Vec<u64>>,
        t0: Instant,
        elapsed_ms: AtomicU64,
        latency_ms: u64,
    }

    impl Slow {
        fn new(script: Vec<Result<Value, ConnectorError>>) -> Arc<Self> {
            Self::taking(script, 50)
        }

        fn taking(script: Vec<Result<Value, ConnectorError>>, latency_ms: u64) -> Arc<Self> {
            Arc::new(Slow {
                script: Mutex::new(script),
                calls: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                starts: Mutex::new(Vec::new()),
                t0: Instant::now(),
                elapsed_ms: AtomicU64::new(0),
                latency_ms,
            })
        }
    }

    #[async_trait::async_trait]
    impl McpTools for Slow {
        async fn tools(&self) -> Result<Vec<RemoteTool>, ConnectorError> {
            Ok(vec![tool_with("query_documents", &[])])
        }
        async fn call(&self, _: &RemoteTool, _: &Value) -> Result<Value, ConnectorError> {
            let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            self.starts
                .lock()
                .unwrap()
                .push(self.t0.elapsed().as_millis() as u64);
            tokio::time::sleep(Duration::from_millis(self.latency_ms)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.elapsed_ms
                .store(self.t0.elapsed().as_millis() as u64, Ordering::SeqCst);
            let mut s = self.script.lock().unwrap();
            if s.len() > 1 {
                s.remove(0)
            } else {
                s[0].clone()
            }
        }
    }

    fn key(name: &str) -> String {
        format!(
            "https://mcp.example.com/{name}#{}",
            rand::rng().random::<u64>()
        )
    }

    fn ok() -> Result<Value, ConnectorError> {
        Ok(json!({"ok": true}))
    }

    fn limited(after: Option<u64>) -> Result<Value, ConnectorError> {
        Err(ConnectorError::rate_limited("slow down", after))
    }

    /// A burst of `429`s that name a wait, then success: the call waits out
    /// each `Retry-After` and returns the answer, never an error.
    #[tokio::test(start_paused = true)]
    async fn a_burst_of_429s_with_retry_after_is_waited_out() {
        let server = Slow::new(vec![
            limited(Some(2)),
            limited(Some(3)),
            limited(Some(2)),
            ok(),
        ]);
        let p = Paced::new(server.clone(), Policy::SANITY, &key("burst"));
        let t = tool_with("query_documents", &[]);
        let r = p.call(&t, &json!({})).await;
        assert_eq!(r.unwrap(), json!({"ok": true}));
        assert_eq!(server.calls.load(Ordering::SeqCst), 4);
        let starts = server.starts.lock().unwrap().clone();
        // Each try started no sooner than the wait the server named.
        for (i, want) in [2_000u64, 3_000, 2_000].iter().enumerate() {
            let gap = starts[i + 1] - starts[i];
            assert!(
                gap >= *want,
                "try {} came {gap} ms after, asked {want}",
                i + 2
            );
        }
        assert!(
            p.gap() > Duration::from_millis(Policy::SANITY.min_gap_ms),
            "the pace slowed after 429s"
        );
    }

    /// Without a `Retry-After`, the backoff grows from try to try.
    #[tokio::test(start_paused = true)]
    async fn without_retry_after_the_backoff_grows() {
        let server = Slow::new(vec![limited(None), limited(None), limited(None), ok()]);
        let p = Paced::new(server.clone(), Policy::SANITY, &key("backoff"));
        let t = tool_with("query_documents", &[]);
        p.call(&t, &json!({})).await.unwrap();
        let s = server.starts.lock().unwrap().clone();
        let gaps: Vec<u64> = s.windows(2).map(|w| w[1] - w[0]).collect();
        // Full jitter over 0.5 s, 1 s, 2 s windows: each at least half its window.
        assert!(gaps[0] >= 250 + 50 && gaps[2] >= 1_000 + 50, "{gaps:?}");
    }

    /// A server that keeps refusing: the error goes back with a wait, after
    /// the policy's tries; a permanent error is never asked again.
    #[tokio::test(start_paused = true)]
    async fn persistent_refusals_are_handed_back_with_a_wait() {
        let server = Slow::new(vec![limited(None)]);
        let p = Paced::new(server.clone(), Policy::SANITY, &key("persist"));
        let t = tool_with("query_documents", &[]);
        let e = p.call(&t, &json!({})).await.unwrap_err();
        assert_eq!(
            server.calls.load(Ordering::SeqCst),
            Policy::SANITY.retries + 1
        );
        match e {
            ConnectorError::Busy(b) => {
                assert!(b.rate_limited);
                assert!(b.retry_after.is_some_and(|s| s >= 1));
            }
            other => panic!("{other:?}"),
        }
        // A wait longer than a worker should hold is handed back at once.
        let server = Slow::new(vec![limited(Some(600))]);
        let p = Paced::new(server.clone(), Policy::SANITY, &key("long"));
        let e = p.call(&t, &json!({})).await.unwrap_err();
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            e,
            ConnectorError::Busy(Busy {
                retry_after: Some(600),
                ..
            })
        ));
        // Not found is final.
        let server = Slow::new(vec![Err(ConnectorError::NotFound("gone".into()))]);
        let p = Paced::new(server.clone(), Policy::SANITY, &key("gone"));
        assert!(matches!(
            p.call(&t, &json!({})).await,
            Err(ConnectorError::NotFound(_))
        ));
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    }

    /// Twenty calls at once through a limit of two: never more than two at
    /// the server, and two wrappers on the same server share the limit.
    /// (Each call takes a second here, far longer than the gap between
    /// starts, so calls do overlap.)
    #[tokio::test(start_paused = true)]
    async fn concurrency_never_exceeds_the_limit() {
        let server = Slow::taking(vec![ok()], 1_000);
        let k = key("conc");
        let a = Arc::new(Paced::new(server.clone(), Policy::SANITY, &k));
        let b = Arc::new(Paced::new(server.clone(), Policy::SANITY, &k));
        let t = tool_with("query_documents", &[]);
        let calls = (0..20).map(|i| {
            let p = if i % 2 == 0 { a.clone() } else { b.clone() };
            let t = t.clone();
            async move { p.call(&t, &json!({})).await }
        });
        let all = futures::future::join_all(calls).await;
        assert!(all.iter().all(Result::is_ok));
        assert_eq!(server.calls.load(Ordering::SeqCst), 20);
        assert_eq!(
            server.peak.load(Ordering::SeqCst),
            Policy::SANITY.concurrency
        );
        assert!(a.peak() <= Policy::SANITY.concurrency);
    }
}
