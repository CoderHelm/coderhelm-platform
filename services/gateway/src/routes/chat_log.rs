//! What Chat logs about its own work: each tool call that failed, and each
//! answer's turns, whether the limit forced it, how long it took and what
//! the question was read as. Structured fields only, never the question,
//! the answer or a tool's text.
//!
//! Two code searches in one answer came back "couldn't read", and the
//! answer stopped on its lookup budget. Neither showed in the logs: the
//! failed calls were only on the saved message, and "Chat answered" carried
//! no turns, no forced flag and no time. Finding that a question stopped on
//! the budget meant reading the answers one by one.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::Value;
use tracing::{info, warn};

use super::chat_agent::{self, ModelDown, Sink, Turn};

/// A failed tool call's kind, from its result: a fixed word, never the
/// text itself.
pub fn error_kind(text: &str) -> &'static str {
    let t = text.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| t.contains(w));
    if has(&["not run: read the source's schema first"]) {
        "schema_first"
    } else if has(&[
        "isn't part of this question",
        "not allowed",
        "denied",
        "forbidden",
        "403",
    ]) {
        "not_allowed"
    } else if has(&["401", "sign in", "sign-in", "reconnect", "unauthorized"]) {
        "sign_in"
    } else if has(&["429", "rate limit", "too many requests"]) {
        "rate_limited"
    } else if has(&["timed out", "timeout", "took too long"]) {
        "timeout"
    } else if has(&["not found", "404", "no such", "doesn't exist"]) {
        "not_found"
    } else if has(&["pick one of", "invalid", "unknown", "missing", "required"]) {
        "bad_input"
    } else if has(&["couldn't be reached", "unavailable", "502", "503", "504"]) {
        "unavailable"
    } else {
        "failed"
    }
}

/// Logs one failed tool call: the tool, its source kind and the error's
/// kind.
pub fn tool_failed(team_id: &str, tool: &str, kind: &str, text: &str) {
    warn!(
        team_id,
        tool,
        source_kind = kind,
        error_kind = error_kind(text),
        "Chat tool call failed"
    );
}

/// The answer model, counting its turns and whether one was forced to
/// answer by the limit (the turn carries `chat_agent::ANSWER_NOW`).
pub struct Counted<'a> {
    inner: &'a dyn chat_agent::Model,
    turns: AtomicUsize,
    forced: AtomicBool,
}

impl<'a> Counted<'a> {
    pub fn new(inner: &'a dyn chat_agent::Model) -> Self {
        Counted {
            inner,
            turns: AtomicUsize::new(0),
            forced: AtomicBool::new(false),
        }
    }

    pub fn turns(&self) -> usize {
        self.turns.load(Ordering::Relaxed)
    }

    pub fn forced(&self) -> bool {
        self.forced.load(Ordering::Relaxed)
    }
}

/// The turn's last message tells the model it must answer now.
fn forced_turn(body: &Value) -> bool {
    body["messages"]
        .as_array()
        .and_then(|m| m.last())
        .is_some_and(|m| m.to_string().contains(chat_agent::ANSWER_NOW))
}

#[async_trait::async_trait]
impl chat_agent::Model for Counted<'_> {
    async fn turn(&self, body: Value, sink: &dyn Sink) -> Result<Turn, ModelDown> {
        self.turns.fetch_add(1, Ordering::Relaxed);
        if forced_turn(&body) {
            self.forced.store(true, Ordering::Relaxed);
        }
        self.inner.turn(body, sink).await
    }

    async fn ask(&self, body: Value) -> Option<String> {
        self.inner.ask(body).await
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn check_id(&self) -> &str {
        self.inner.check_id()
    }
}

/// One answer, as logged.
#[derive(Debug, Clone, PartialEq)]
pub struct Answered {
    pub intent: &'static str,
    pub turns: usize,
    pub forced_answer: bool,
    pub calls: usize,
    pub tool_errors: usize,
    pub secs: f64,
    pub rechecked: bool,
    pub grounded: bool,
    pub cited: usize,
}

impl Answered {
    /// What the model and the clock say about the answer; the loop's own
    /// counts are added with [`Answered::with`].
    pub fn of(
        model: &Counted<'_>,
        started: Instant,
        intent: &'static str,
        tool_errors: usize,
    ) -> Answered {
        Answered {
            intent,
            turns: model.turns(),
            forced_answer: model.forced(),
            calls: 0,
            tool_errors,
            secs: (started.elapsed().as_millis() as f64 / 100.0).round() / 10.0,
            rechecked: false,
            grounded: false,
            cited: 0,
        }
    }

    pub fn with(self, out: &chat_agent::Answered) -> Answered {
        Answered {
            calls: out.calls.len(),
            rechecked: out.rechecked,
            grounded: out.fin.grounded,
            cited: out.fin.citations.len(),
            ..self
        }
    }

    pub fn log(&self, team_id: &str) {
        info!(
            team_id,
            intent = self.intent,
            turns = self.turns,
            forced_answer = self.forced_answer,
            calls = self.calls,
            tool_errors = self.tool_errors,
            secs = self.secs,
            rechecked = self.rechecked,
            grounded = self.grounded,
            cited = self.cited,
            "Chat answered"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Echo;

    #[async_trait::async_trait]
    impl chat_agent::Model for Echo {
        async fn turn(&self, _body: Value, _sink: &dyn Sink) -> Result<Turn, ModelDown> {
            Ok(Turn {
                content: vec![json!({"type": "text", "text": "ok"})],
                stop: "end_turn".into(),
            })
        }
        async fn ask(&self, _body: Value) -> Option<String> {
            None
        }
        fn id(&self) -> &str {
            "m"
        }
        fn check_id(&self) -> &str {
            "c"
        }
    }

    struct Quiet;

    #[async_trait::async_trait]
    impl Sink for Quiet {
        async fn send(&self, _event: &str, _data: Value) {}
    }

    fn body(last: &str) -> Value {
        json!({"messages": [
            {"role": "user", "content": [{"type": "text", "text": "why is the export slow?"}]},
            {"role": "user", "content": [{"type": "text", "text": last}]}
        ]})
    }

    /// A forced answer is logged as forced, with its turns counted.
    #[tokio::test]
    async fn a_forced_answer_is_logged_forced() {
        use chat_agent::Model;
        let m = Counted::new(&Echo);
        m.turn(body("tool results"), &Quiet).await.unwrap();
        assert!(!m.forced());
        m.turn(body(chat_agent::ANSWER_NOW), &Quiet).await.unwrap();
        assert_eq!(m.turns(), 2);
        assert!(m.forced());
        let a = Answered::of(&m, Instant::now(), "lookup", 1);
        assert!(a.forced_answer);
        assert_eq!((a.turns, a.tool_errors), (2, 1));
        assert_eq!(a.intent, "lookup");
    }

    #[test]
    fn error_kinds_are_fixed_words() {
        assert_eq!(error_kind("Error: GitHub answered 403"), "not_allowed");
        assert_eq!(
            error_kind("Not run: read the source's schema first with live_describe"),
            "schema_first"
        );
        assert_eq!(error_kind("Error: the request timed out"), "timeout");
        assert_eq!(
            error_kind("Error: pick one of the listed repositories."),
            "bad_input"
        );
        assert_eq!(error_kind("Error: GitHub answered 503"), "unavailable");
        assert_eq!(error_kind("Couldn't read that file"), "failed");
    }
}
