//! Whether one of CoderHelm's tools is working, so one broken tool is one
//! problem, not one failure per step.
//!
//! A run's outside steps once all failed on the same error: every check
//! step ran the same broken request and reported its error as its own, and
//! each became an item for a person. The rules:
//!
//! - a failure is **diagnosed once** ([`diagnose`]): bad input (the request
//!   CoderHelm built is wrong: fix it in code), the provider, credentials, a
//!   rate limit, the network;
//! - a failure worth retrying (provider, rate limit, network) is retried
//!   with backoff ([`retry_after`]); a bad input or an auth failure is not;
//! - the tool is then **degraded** for the run and, for a short window
//!   ([`WINDOW`]), for the installation: every step that would use it takes
//!   its alternative path instead of failing on the same error
//!   ([`Health::degraded`]);
//! - the failure is **reported once**, as CoderHelm's own problem
//!   ([`Health::take_reports`]): the run page and an error log line the
//!   health alerts read, never once per step, never to a person.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a tool degraded on one run stays degraded for the next runs of
/// the same installation.
pub const WINDOW: Duration = Duration::from_secs(10 * 60);

/// What a tool's failure was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Diagnosis {
    /// The request CoderHelm built was refused as malformed (400, 422, a
    /// parse error): CoderHelm's bug, fixed in code, never retried as is.
    BadInput,
    /// Credentials were refused (401, 403).
    Auth,
    /// Too many requests (429, a rate-limit message).
    RateLimit,
    /// The provider failed (5xx).
    Provider,
    /// The network: a timeout, a refused connection, DNS.
    Network,
    /// Anything else.
    Other,
}

impl Diagnosis {
    pub fn words(self) -> &'static str {
        match self {
            Diagnosis::BadInput => "the request CoderHelm built was refused as malformed",
            Diagnosis::Auth => "the service refused CoderHelm's credentials",
            Diagnosis::RateLimit => "the service limited CoderHelm's requests",
            Diagnosis::Provider => "the service failed",
            Diagnosis::Network => "the service could not be reached",
            Diagnosis::Other => "the tool failed",
        }
    }

    /// Whether trying the same request again can work.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Diagnosis::RateLimit | Diagnosis::Provider | Diagnosis::Network
        )
    }
}

/// Reads an error message once.
pub fn diagnose(error: &str) -> Diagnosis {
    let l = error.to_lowercase();
    let has = |w: &[&str]| w.iter().any(|x| l.contains(x));
    if has(&["429", "rate limit", "too many requests", "secondary rate"]) {
        Diagnosis::RateLimit
    } else if has(&["401", "403", "unauthorized", "forbidden", "bad credentials"]) {
        Diagnosis::Auth
    } else if has(&[
        "400",
        "bad request",
        "422",
        "unprocessable",
        "parse error",
        "invalid query",
        "malformed",
    ]) {
        Diagnosis::BadInput
    } else if has(&["500", "502", "503", "504", "internal server error"]) {
        Diagnosis::Provider
    } else if has(&[
        "timed out",
        "timeout",
        "connection refused",
        "dns",
        "error sending request",
    ]) {
        Diagnosis::Network
    } else {
        Diagnosis::Other
    }
}

/// The waits before each retry of a retryable failure.
pub fn retry_after(d: Diagnosis) -> &'static [Duration] {
    const SHORT: &[Duration] = &[Duration::from_secs(2), Duration::from_secs(8)];
    const LONG: &[Duration] = &[Duration::from_secs(20)];
    match d {
        Diagnosis::Provider | Diagnosis::Network => SHORT,
        Diagnosis::RateLimit => LONG,
        _ => &[],
    }
}

/// A tool's failure, as reported once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    pub tool: String,
    pub diagnosis: Diagnosis,
    /// The first error, as the tool gave it.
    pub detail: String,
}

impl Problem {
    /// The one line the run page shows.
    pub fn line(&self) -> String {
        format!(
            "CoderHelm's own problem: its `{}` tool failed ({}: {}). The steps that needed it \
             used another way; nobody needs to act.",
            self.tool,
            self.diagnosis.words(),
            crate::truncate_str(&one_line(&self.detail), 200)
        )
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

type Shared = HashMap<(u64, String), (Problem, Instant)>;

static SHARED: Mutex<Option<Shared>> = Mutex::new(None);

/// One run's view of its tools: what failed in it, what the installation's
/// other runs found degraded, and what is left to report.
#[derive(Debug, Default)]
pub struct Health {
    installation: u64,
    failed: HashMap<String, Problem>,
    reported: Vec<String>,
}

impl Health {
    pub fn new(installation: u64) -> Health {
        Health {
            installation,
            ..Default::default()
        }
    }

    /// Records a failure of `tool` (after its retries). The first failure
    /// is the one diagnosed and reported; later ones change nothing.
    pub fn failed(&mut self, tool: &str, error: &str) -> Diagnosis {
        if let Some(p) = self.failed.get(tool) {
            return p.diagnosis;
        }
        let p = Problem {
            tool: tool.to_string(),
            diagnosis: diagnose(error),
            detail: error.to_string(),
        };
        let d = p.diagnosis;
        if let Ok(mut g) = SHARED.lock() {
            g.get_or_insert_with(HashMap::new).insert(
                (self.installation, tool.to_string()),
                (p.clone(), Instant::now()),
            );
        }
        self.failed.insert(tool.to_string(), p);
        d
    }

    /// Whether `tool` is degraded for this run: it failed here, or on
    /// another run of the installation within [`WINDOW`].
    pub fn degraded(&mut self, tool: &str) -> Option<Problem> {
        if let Some(p) = self.failed.get(tool) {
            return Some(p.clone());
        }
        let shared = SHARED.lock().ok().and_then(|g| {
            g.as_ref()
                .and_then(|m| m.get(&(self.installation, tool.to_string())).cloned())
        });
        match shared {
            Some((p, at)) if at.elapsed() < WINDOW => {
                // Seen elsewhere: this run reports it too, once.
                self.failed.insert(tool.to_string(), p.clone());
                Some(p)
            }
            _ => None,
        }
    }

    /// The tools degraded for this run, for the planner.
    pub fn degraded_tools(&self) -> Vec<String> {
        let mut t: Vec<String> = self.failed.keys().cloned().collect();
        t.sort();
        t
    }

    /// The problems not reported yet, each once.
    pub fn take_reports(&mut self) -> Vec<Problem> {
        let mut out: Vec<Problem> = self
            .failed
            .values()
            .filter(|p| !self.reported.contains(&p.tool))
            .cloned()
            .collect();
        out.sort_by(|a, b| a.tool.cmp(&b.tool));
        for p in &out {
            self.reported.push(p.tool.clone());
        }
        out
    }

    /// Forgets the installation's shared state (tests).
    #[cfg(test)]
    pub fn reset_shared() {
        if let Ok(mut g) = SHARED.lock() {
            *g = None;
        }
    }
}

/// Which of `tools` another run of the installation found degraded within
/// [`WINDOW`].
pub fn degraded_now(installation: u64, tools: &[&str]) -> Vec<String> {
    let mut h = Health::new(installation);
    tools
        .iter()
        .filter(|t| h.degraded(t).is_some())
        .map(|t| t.to_string())
        .collect()
}

/// The planner's lines for the degraded tools: plan around them.
pub fn prompt_section(tools: &[String]) -> String {
    if tools.is_empty() {
        return String::new();
    }
    format!(
        "\n## Tools that are not working right now\n{} failed on this installation in the last \
         few minutes. Plan the steps that would use them another way (read the workflow run's \
         result, run the end-to-end spec, fetch the page from the sandbox); never give their \
         work to a person.\n",
        tools
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failure_is_diagnosed_from_its_message() {
        assert_eq!(
            diagnose("the read API returned 400 Bad Request"),
            Diagnosis::BadInput
        );
        assert_eq!(diagnose("HTTP 429 Too Many Requests"), Diagnosis::RateLimit);
        assert_eq!(diagnose("401 Unauthorized"), Diagnosis::Auth);
        assert_eq!(diagnose("503 Service Unavailable"), Diagnosis::Provider);
        assert_eq!(diagnose("operation timed out"), Diagnosis::Network);
        assert!(!Diagnosis::BadInput.retryable());
        assert!(retry_after(Diagnosis::BadInput).is_empty());
        assert_eq!(retry_after(Diagnosis::Provider).len(), 2);
    }

    #[test]
    fn one_tool_failure_is_one_report_and_degrades_the_tool_for_the_installation() {
        Health::reset_shared();
        let mut h = Health::new(42);
        assert!(h.degraded("check_url").is_none());
        h.failed("check_url", "the read API returned 400 Bad Request");
        h.failed("check_url", "the read API returned 400 Bad Request");
        h.failed("check_url", "timed out");
        assert!(h.degraded("check_url").is_some());
        let r = h.take_reports();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].diagnosis, Diagnosis::BadInput);
        assert!(
            r[0].line().contains("nobody needs to act"),
            "{}",
            r[0].line()
        );
        assert!(h.take_reports().is_empty(), "reported once");
        // Another run of the same installation finds it degraded.
        let mut other = Health::new(42);
        assert!(other.degraded("check_url").is_some());
        assert_eq!(other.take_reports().len(), 1);
        // Another installation does not.
        assert!(Health::new(7).degraded("check_url").is_none());
        assert!(prompt_section(&h.degraded_tools()).contains("`check_url`"));
    }
}
