//! The agent log: what each model call of a run did, step by step, kept so
//! the run page can show it (`GET /api/runs/:run_id/agent-log`).
//!
//! # Where it is kept
//!
//! One S3 object per model call (or per group of calls a pass logs
//! together), at [`log_key`]: `runs/{team}/{run}/conversation/{pass}-{ms}.json`,
//! where `ms` is when the log started, in epoch milliseconds. A pass that
//! runs again (a second CI fix, a retried build, each plan round) gets a new
//! file instead of replacing the last one. Logs written before this format
//! are `runs/{team}/{run}/conversation/{pass}.json`, a bare entry array;
//! [`parse_file`] reads both.
//!
//! # The file
//!
//! ```text
//! { "format": 2, "pass": "ci_fix", "step": "coder" | null, "round": 2 | null,
//!   "started_at": "<RFC 3339>", "ended_at": "<RFC 3339>", "entries": [Entry] }
//! ```
//!
//! # Entries (in the order they happened)
//!
//! Every entry has `role`, `turn` (1-based within its model call) and, in
//! this format, `call` (1-based: which model call of the file it belongs to,
//! since `turn` starts again at 1 in each call).
//!
//! - `call_started`: a new model call begins. `started_at`, `model`,
//!   `max_turns`, `turn: 0`.
//! - `system_note`: a message the worker put in the conversation, not the
//!   model or a tool. `kind` ([`NoteKind`]), `text`, `started_at`.
//! - `assistant`: one model response. `content` is the Messages API blocks,
//!   verbatim (`text`, `thinking`, `tool_use` with its full `input`), `usage`,
//!   `stop_reason` on the final answer, `started_at` / `ended_at` around the
//!   request.
//! - `tool_results`: the results of that response's tool calls. `content` is
//!   one object per call:
//!   `{ type: "tool_result", tool_use_id, tool, content, content_encoding,
//!      is_error?, started_at, duration_ms, outcome: {status, summary?} }`.
//!
//! # Tool result content
//!
//! `content` is kept exactly as the model saw it. A tool that returned a
//! value has it serialized as JSON, and almost every tool returns a string,
//! so the stored text is a JSON-encoded string (quoted, `\n` escaped):
//! `content_encoding: "json"`, and one `JSON.parse` gives the text. Errors and
//! refusals are plain text: `content_encoding: "text"`. [`content_encoding`]
//! tells old entries apart the same way. Results over [`MAX_RESULT_BYTES`]
//! are cut and end with `... [truncated, N bytes total]`.
//!
//! # Secrets
//!
//! Tool inputs, results, the agent's text and every summary pass through
//! [`redact_entry`] before a log is written: credential-shaped strings and
//! the values of secret-named fields are replaced by `[redacted]`.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::LazyLock;

/// This format's `format` number. Bare arrays (no number) are format 1.
pub const FORMAT: u64 = 2;

/// The most of one tool result a log keeps (the S3 size cap per result).
pub const MAX_RESULT_BYTES: usize = 102_400;

/// The most characters of an outcome summary.
pub const MAX_SUMMARY: usize = 200;

/// How a tool call ended, decided by the tool itself where it knows (an
/// edit that matched nothing ran fine but did nothing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeStatus {
    /// Did what was asked.
    Ok,
    /// Ran, but did not do what was asked: no edits applied, a write
    /// rejected by a guard, sandbox checks failed, nothing to verify with.
    SoftFail,
    /// Never ran: the agent policy said no, or GitHub was paused.
    Refused,
    /// Failed with an error.
    Error,
}

/// A tool call's outcome: its status and, where the tool knows one, a short
/// plain summary ("12 matches in 3 files", "+4/-1 lines", "3 of 4 checks
/// passed").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutcome {
    pub status: OutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl ToolOutcome {
    fn new(status: OutcomeStatus, summary: impl Into<String>) -> Self {
        let s: String = summary.into();
        let s = s.trim();
        Self {
            status,
            summary: (!s.is_empty()).then(|| {
                if s.chars().count() > MAX_SUMMARY {
                    format!("{}…", s.chars().take(MAX_SUMMARY).collect::<String>())
                } else {
                    s.to_string()
                }
            }),
        }
    }
    pub fn ok(summary: impl Into<String>) -> Self {
        Self::new(OutcomeStatus::Ok, summary)
    }
    pub fn soft_fail(summary: impl Into<String>) -> Self {
        Self::new(OutcomeStatus::SoftFail, summary)
    }
    pub fn refused(summary: impl Into<String>) -> Self {
        Self::new(OutcomeStatus::Refused, summary)
    }
    pub fn error(summary: impl Into<String>) -> Self {
        Self::new(OutcomeStatus::Error, summary)
    }
    /// The outcome when the tool said nothing about it: from how the call
    /// ended only, never from its text.
    pub fn default_for(is_error: bool) -> Self {
        if is_error {
            Self::error("")
        } else {
            Self::ok("")
        }
    }
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// A message the worker put into the conversation itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteKind {
    /// "You have used N of M tool-use turns."
    TurnBudget,
    /// The deadline is close: wrap up.
    WrapUp,
    /// Every tool step is used: tools are off, answer now.
    ToolsOff,
}

impl NoteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NoteKind::TurnBudget => "turn_budget",
            NoteKind::WrapUp => "wrap_up",
            NoteKind::ToolsOff => "tools_off",
        }
    }
}

/// Now, as the log writes times: RFC 3339 in UTC with milliseconds.
pub fn now() -> String {
    rfc3339(chrono::Utc::now())
}

pub fn rfc3339(t: chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// The S3 key of one log: `runs/{team}/{run}/conversation/{pass}-{ms}.json`.
pub fn log_key(team_id: &str, run_id: &str, pass: &str, started_ms: i64) -> String {
    format!(
        "{}{}-{:013}.json",
        log_prefix(team_id, run_id),
        safe_name(pass),
        started_ms.max(0)
    )
}

/// The S3 prefix every log of a run sits under.
pub fn log_prefix(team_id: &str, run_id: &str) -> String {
    format!("runs/{team_id}/{run_id}/conversation/")
}

/// A pass name fit for a key: no slashes or spaces.
fn safe_name(pass: &str) -> String {
    let s: String = pass
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() {
        "agent".into()
    } else {
        s
    }
}

/// The body of one log file (see the module docs).
pub fn file_body(
    pass: &str,
    step: Option<&str>,
    round: Option<u32>,
    started_at: &str,
    ended_at: &str,
    entries: Vec<Value>,
) -> Value {
    json!({
        "format": FORMAT,
        "pass": pass,
        "step": step,
        "round": round,
        "started_at": started_at,
        "ended_at": ended_at,
        "entries": entries,
    })
}

/// When the first timed entry of a log started, if any has a time.
pub fn first_started_at(entries: &[Value]) -> Option<String> {
    entries
        .iter()
        .find_map(|e| e.get("started_at").and_then(|v| v.as_str()))
        .map(String::from)
}

/// One log as the API returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct PassLog {
    /// The file's name without `.json`: unique within the run.
    pub id: String,
    pub pass: String,
    pub step: Option<String>,
    /// 1-based: the how-manyth log of this pass in the run.
    pub round: Option<u32>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    /// 1 for a bare array, 2 for this format.
    pub format: u64,
    pub entries: Value,
    /// The object's last-modified time: the order of last resort.
    pub stored_at: Option<String>,
}

impl PassLog {
    pub fn to_value(&self) -> Value {
        json!({
            "id": self.id,
            "pass": self.pass,
            "round": self.round,
            "step": self.step,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "format": self.format,
            "conversation": self.entries,
        })
    }
}

/// Reads one stored log: `name` is its key without the run's prefix,
/// `stored_at` the object's last-modified time. A bare array is an old log
/// whose pass is its file name. Anything else is not a log.
pub fn parse_file(name: &str, body: Value, stored_at: Option<String>) -> Option<PassLog> {
    let id = name.trim_end_matches(".json").to_string();
    match body {
        Value::Array(_) => Some(PassLog {
            pass: id.clone(),
            id,
            step: None,
            round: None,
            started_at: None,
            ended_at: None,
            format: 1,
            entries: with_encodings(body),
            stored_at,
        }),
        Value::Object(mut m) => {
            let entries = m.remove("entries").filter(|e| e.is_array())?;
            let s = |m: &serde_json::Map<String, Value>, k: &str| {
                m.get(k)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            };
            Some(PassLog {
                pass: s(&m, "pass").unwrap_or_else(|| id.clone()),
                step: s(&m, "step"),
                round: m.get("round").and_then(|v| v.as_u64()).map(|n| n as u32),
                started_at: s(&m, "started_at").or_else(|| first_started_at_v(&entries)),
                ended_at: s(&m, "ended_at"),
                format: m.get("format").and_then(|v| v.as_u64()).unwrap_or(FORMAT),
                entries: with_encodings(entries),
                id,
                stored_at,
            })
        }
        _ => None,
    }
}

fn first_started_at_v(entries: &Value) -> Option<String> {
    entries.as_array().and_then(|a| first_started_at(a))
}

/// An RFC 3339 time in the log's own form (UTC, milliseconds, `Z`), so
/// times written different ways compare as text.
pub fn normalize_time(t: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(t.trim())
        .ok()
        .map(|d| rfc3339(d.with_timezone(&chrono::Utc)))
}

/// Puts the logs in the order they ran and numbers each pass's rounds.
///
/// A log's start is its own `started_at`; an old log has none, so it takes
/// the start of its pass in `pass_history` (`(pass, started_at)` in run
/// order; the k-th old log of a pass takes the k-th start), then the time it
/// was stored. Every start is returned in the log's own time form.
pub fn order_logs(mut logs: Vec<PassLog>, pass_history: &[(String, String)]) -> Vec<PassLog> {
    let mut used: std::collections::HashMap<String, usize> = Default::default();
    for log in logs.iter_mut().filter(|l| l.started_at.is_none()) {
        let k = used.entry(log.pass.clone()).or_default();
        let start = pass_history
            .iter()
            .filter(|(p, _)| p == &log.pass)
            .nth(*k)
            .map(|(_, t)| t.clone());
        if start.is_some() {
            *k += 1;
        }
        log.started_at = start.or_else(|| log.stored_at.clone());
    }
    for log in logs.iter_mut() {
        if let Some(t) = log.started_at.as_deref().and_then(normalize_time) {
            log.started_at = Some(t);
        }
    }
    logs.sort_by(|a, b| {
        a.started_at
            .as_deref()
            .unwrap_or("")
            .cmp(b.started_at.as_deref().unwrap_or(""))
            .then_with(|| a.id.cmp(&b.id))
    });
    let mut rounds: std::collections::HashMap<String, u32> = Default::default();
    for log in logs.iter_mut() {
        let n = rounds.entry(log.pass.clone()).or_default();
        *n += 1;
        if log.round.is_none() {
            log.round = Some(*n);
        }
    }
    logs
}

/// `"json"` when a tool result's stored text is a JSON-encoded value (what a
/// tool returned), `"text"` when it is plain text (an error, a refusal).
pub fn content_encoding(content: &str) -> &'static str {
    let t = content.trim();
    let looks = (t.starts_with('"') && t.ends_with('"') && t.len() >= 2)
        || t.starts_with('{')
        || t.starts_with('[');
    if looks && serde_json::from_str::<Value>(t).is_ok() {
        "json"
    } else {
        "text"
    }
}

/// The text a stored tool result shows: decoded once when it is a JSON
/// string, as stored otherwise.
pub fn decode_content(content: &str) -> String {
    match serde_json::from_str::<Value>(content) {
        Ok(Value::String(s)) => s,
        _ => content.to_string(),
    }
}

/// Adds `content_encoding` to every tool result that lacks one.
fn with_encodings(mut entries: Value) -> Value {
    if let Some(list) = entries.as_array_mut() {
        for e in list.iter_mut() {
            if e.get("role").and_then(|r| r.as_str()) != Some("tool_results") {
                continue;
            }
            if let Some(results) = e.get_mut("content").and_then(|c| c.as_array_mut()) {
                for r in results.iter_mut() {
                    if r.get("content_encoding").is_some() {
                        continue;
                    }
                    let enc = r
                        .get("content")
                        .and_then(|c| c.as_str())
                        .map(content_encoding);
                    if let (Some(enc), Some(obj)) = (enc, r.as_object_mut()) {
                        obj.insert("content_encoding".into(), json!(enc));
                    }
                }
            }
        }
    }
    entries
}

/// Cuts a tool result's stored text to [`MAX_RESULT_BYTES`].
pub fn cap_result(content: &str) -> String {
    if content.len() > MAX_RESULT_BYTES {
        format!(
            "{}... [truncated, {} bytes total]",
            crate::truncate_str(content, MAX_RESULT_BYTES),
            content.len()
        )
    } else {
        content.to_string()
    }
}

/// How many lines one text adds and removes against another, by counting
/// lines (each distinct line's count in each). A move counts as nothing.
pub fn line_delta(old: &str, new: &str) -> (usize, usize) {
    let mut counts: std::collections::HashMap<&str, i64> = Default::default();
    for l in old.lines() {
        *counts.entry(l).or_default() -= 1;
    }
    for l in new.lines() {
        *counts.entry(l).or_default() += 1;
    }
    let add = counts
        .values()
        .filter(|&&n| n > 0)
        .map(|&n| n as usize)
        .sum();
    let del = counts
        .values()
        .filter(|&&n| n < 0)
        .map(|&n| (-n) as usize)
        .sum();
    (add, del)
}

/// Credential shapes, found anywhere in text.
static SECRET_TEXT: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        // Private key blocks, whole.
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)",
        // Tokens by their prefix.
        r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}",
        r"\bgithub_pat_[A-Za-z0-9_]{20,}",
        r"\bxox[abprs]-[A-Za-z0-9-]{10,}",
        r"\bsk-ant-[A-Za-z0-9_-]{16,}",
        r"\bsk-[A-Za-z0-9]{32,}",
        r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
        r"\b(?:chk|whsec)_[A-Za-z0-9]{16,}",
        r"\bglpat-[A-Za-z0-9_-]{20,}",
        r"\bAIza[0-9A-Za-z_-]{35}\b",
        // JSON web tokens.
        r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}",
    ]
    .iter()
    .map(|p| Regex::new(p).expect("valid pattern"))
    .collect()
});

/// The word after `Bearer` / `Basic` in an authorization value.
static AUTH_SCHEME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{16,}").expect("valid pattern")
});

/// `user:password@` in an address.
static URL_CREDS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"([a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@").expect("valid pattern")
});

/// `password = "..."`, `API_KEY=...`, `"client_secret": "..."`: a field named
/// for a secret set to a literal of 8 characters or more. A type or variable
/// after the name (`token: String`) is not a literal and is kept.
static SECRET_ASSIGN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)(\b[\w-]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential)[\w-]*["']?\s*[:=]\s*)(?:"([^"\s]{8,})"|'([^'\s]{8,})'|([^\s"',;&}{)(]{8,}))"#,
    )
    .expect("valid pattern")
});

const REDACTED: &str = "[redacted]";

/// Text with every credential-shaped value replaced by `[redacted]`.
pub fn redact_text(text: &str) -> String {
    let mut out = std::borrow::Cow::Borrowed(text);
    for re in SECRET_TEXT.iter() {
        if re.is_match(&out) {
            out = std::borrow::Cow::Owned(re.replace_all(&out, REDACTED).into_owned());
        }
    }
    if AUTH_SCHEME.is_match(&out) {
        out = std::borrow::Cow::Owned(AUTH_SCHEME.replace_all(&out, "$1 [redacted]").into_owned());
    }
    if URL_CREDS.is_match(&out) {
        out = std::borrow::Cow::Owned(URL_CREDS.replace_all(&out, "$1[redacted]@").into_owned());
    }
    if SECRET_ASSIGN.is_match(&out) {
        out = std::borrow::Cow::Owned(
            SECRET_ASSIGN
                .replace_all(&out, |c: &regex::Captures| {
                    let value = c
                        .get(2)
                        .or_else(|| c.get(3))
                        .or_else(|| c.get(4))
                        .map(|m| m.as_str())
                        .unwrap_or("");
                    // A name, a number or a placeholder is not a secret.
                    let digits = value.chars().any(|ch| ch.is_ascii_digit());
                    let letters = value.chars().any(|ch| ch.is_ascii_alphabetic());
                    let quoted = c.get(2).is_some() || c.get(3).is_some();
                    if (digits && letters) || (quoted && !value.contains('{')) {
                        let q = if c.get(2).is_some() {
                            "\""
                        } else if c.get(3).is_some() {
                            "'"
                        } else {
                            ""
                        };
                        format!("{}{q}{REDACTED}{q}", &c[1])
                    } else {
                        c[0].to_string()
                    }
                })
                .into_owned(),
        );
    }
    out.into_owned()
}

/// A tool call's input with the value of every secret-named field masked and
/// every string passed through [`redact_text`].
pub fn redact_input(input: &Value) -> Value {
    let mut v = crate::deliveries::redact_json(input);
    redact_strings(&mut v);
    v
}

fn redact_strings(v: &mut Value) {
    match v {
        Value::String(s) => {
            let clean = redact_text(s);
            if clean != *s {
                *s = clean;
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact_strings),
        Value::Object(m) => m.values_mut().for_each(redact_strings),
        _ => {}
    }
}

fn redact_field(obj: &mut Value, key: &str) {
    if let Some(Value::String(s)) = obj.get_mut(key) {
        let clean = redact_text(s);
        if clean != *s {
            *s = clean;
        }
    }
}

/// One log entry with secrets removed from what the agent wrote and read:
/// text and thinking blocks, tool inputs, tool results, outcome summaries and
/// system notes. Times, ids, usage and thinking signatures are left alone.
pub fn redact_entry(entry: &mut Value) {
    let role = entry
        .get("role")
        .and_then(|r| r.as_str())
        .unwrap_or("")
        .to_string();
    match role.as_str() {
        "assistant" => {
            if let Some(blocks) = entry.get_mut("content").and_then(|c| c.as_array_mut()) {
                for b in blocks.iter_mut() {
                    match b.get("type").and_then(|t| t.as_str()) {
                        Some("text") => redact_field(b, "text"),
                        Some("thinking") => redact_field(b, "thinking"),
                        Some("tool_use") => {
                            if let Some(input) = b.get("input") {
                                let clean = redact_input(input);
                                b["input"] = clean;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        "tool_results" => {
            if let Some(results) = entry.get_mut("content").and_then(|c| c.as_array_mut()) {
                for r in results.iter_mut() {
                    redact_field(r, "content");
                    if let Some(o) = r.get_mut("outcome") {
                        redact_field(o, "summary");
                    }
                }
            }
        }
        "system_note" => redact_field(entry, "text"),
        _ => {}
    }
}

/// One live tool event as the API returns it, from its stored `payload` and
/// `timestamp` (when the call ended). Old events have no `pass`,
/// `tool_use_id` or `started_at`: those come back null.
pub fn live_event(payload: &Value, timestamp: &str) -> Value {
    let s = |k: &str| {
        payload
            .get(k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    json!({
        "tool": s("tool").unwrap_or(""),
        "input_summary": s("input_summary").unwrap_or(""),
        "duration_ms": payload.get("duration_ms").and_then(|v| v.as_u64()).unwrap_or(0),
        "is_error": payload.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false),
        "timestamp": timestamp,
        "pass": s("pass"),
        "tool_use_id": s("tool_use_id"),
        "started_at": s("started_at"),
        "outcome": payload.get("outcome").filter(|o| o.is_object()).cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_serializes_status_and_optional_summary() {
        assert_eq!(
            ToolOutcome::soft_fail("NO edits applied").to_value(),
            json!({"status": "soft_fail", "summary": "NO edits applied"})
        );
        assert_eq!(
            ToolOutcome::default_for(false).to_value(),
            json!({"status": "ok"})
        );
        assert_eq!(
            ToolOutcome::default_for(true).to_value(),
            json!({"status": "error"})
        );
        assert_eq!(
            ToolOutcome::refused("policy").to_value()["status"],
            "refused"
        );
        let long = ToolOutcome::ok("x".repeat(500));
        assert_eq!(
            long.summary.unwrap().chars().count(),
            MAX_SUMMARY + 1,
            "cut with an ellipsis"
        );
    }

    #[test]
    fn keys_are_unique_per_start_and_safe() {
        assert_eq!(
            log_key("t", "r", "ci_fix", 1_700_000_000_123),
            "runs/t/r/conversation/ci_fix-1700000000123.json"
        );
        assert_eq!(
            log_key("t", "r", "implement_o/web app", 5),
            "runs/t/r/conversation/implement_o_web_app-0000000000005.json"
        );
        assert_ne!(
            log_key("t", "r", "ci_fix", 1),
            log_key("t", "r", "ci_fix", 2)
        );
    }

    #[test]
    fn reads_old_bare_arrays_and_new_files() {
        let old = parse_file(
            "implement.json",
            json!([{"turn": 1, "role": "tool_results", "content": [
                {"type": "tool_result", "tool_use_id": "a", "content": "\"hi\\nthere\""},
                {"type": "tool_result", "tool_use_id": "b", "content": "Error: boom", "is_error": true}
            ]}]),
            Some("2026-11-10T10:00:00.000Z".into()),
        )
        .unwrap();
        assert_eq!(old.pass, "implement");
        assert_eq!(old.format, 1);
        assert_eq!(old.started_at, None);
        assert_eq!(old.entries[0]["content"][0]["content_encoding"], "json");
        assert_eq!(old.entries[0]["content"][1]["content_encoding"], "text");

        let new = parse_file(
            "ci_fix-0001700000000000.json",
            file_body(
                "ci_fix",
                Some("coder"),
                None,
                "2026-11-10T10:00:00.000Z",
                "2026-11-10T10:05:00.000Z",
                vec![json!({"role": "call_started", "call": 1, "turn": 0})],
            ),
            None,
        )
        .unwrap();
        assert_eq!(new.pass, "ci_fix");
        assert_eq!(new.id, "ci_fix-0001700000000000");
        assert_eq!(new.step.as_deref(), Some("coder"));
        assert_eq!(new.format, FORMAT);
        assert_eq!(new.started_at.as_deref(), Some("2026-11-10T10:00:00.000Z"));
        let v = new.to_value();
        assert_eq!(v["conversation"][0]["role"], "call_started");
        assert!(v.get("ended_at").is_some());

        assert!(parse_file("x.json", json!("nope"), None).is_none());
        assert!(parse_file("x.json", json!({"no": "entries"}), None).is_none());
    }

    fn log(id: &str, pass: &str, started: Option<&str>, stored: Option<&str>) -> PassLog {
        PassLog {
            id: id.into(),
            pass: pass.into(),
            step: None,
            round: None,
            started_at: started.map(String::from),
            ended_at: None,
            format: 2,
            entries: json!([]),
            stored_at: stored.map(String::from),
        }
    }

    #[test]
    fn orders_by_start_not_by_name_and_numbers_rounds() {
        // S3 lists by name: ci_fix sorts before implement and plan.
        let logs = vec![
            log("ci_fix-2", "ci_fix", Some("2026-11-10T10:30:00.000Z"), None),
            log("ci_fix-1", "ci_fix", Some("2026-11-10T10:20:00.000Z"), None),
            log(
                "implement-1",
                "implement",
                Some("2026-11-10T10:10:00.000Z"),
                None,
            ),
            log("plan-2", "plan", Some("2026-11-10T10:05:00.000Z"), None),
            log("plan-1", "plan", Some("2026-11-10T10:01:00.000Z"), None),
        ];
        let out = order_logs(logs, &[]);
        let got: Vec<(&str, Option<u32>)> = out.iter().map(|l| (l.id.as_str(), l.round)).collect();
        assert_eq!(
            got,
            vec![
                ("plan-1", Some(1)),
                ("plan-2", Some(2)),
                ("implement-1", Some(1)),
                ("ci_fix-1", Some(1)),
                ("ci_fix-2", Some(2)),
            ]
        );
    }

    #[test]
    fn old_logs_take_their_start_from_pass_history_then_storage() {
        let history = vec![
            ("plan".to_string(), "2026-11-10T10:00:00.000Z".to_string()),
            (
                "implement".to_string(),
                "2026-11-10T10:10:00.000Z".to_string(),
            ),
        ];
        let logs = vec![
            // Stored after implement ended, but no pass of that name.
            log("ci_fix", "ci_fix", None, Some("2026-11-10T10:40:00.000Z")),
            log(
                "implement",
                "implement",
                None,
                Some("2026-11-10T10:30:00.000Z"),
            ),
            log(
                "security-1",
                "security",
                Some("2026-11-10T10:35:00.000Z"),
                None,
            ),
        ];
        let out = order_logs(logs, &history);
        let ids: Vec<&str> = out.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(ids, vec!["implement", "security-1", "ci_fix"]);
        assert_eq!(
            out[0].started_at.as_deref(),
            Some("2026-11-10T10:10:00.000Z")
        );
        assert_eq!(
            out[2].started_at.as_deref(),
            Some("2026-11-10T10:40:00.000Z")
        );
    }

    #[test]
    fn times_written_different_ways_sort_as_instants() {
        // pass_history holds `to_rfc3339()` (+00:00, nanoseconds); S3 has
        // whole seconds; the log has milliseconds and Z.
        let history = vec![(
            "plan".to_string(),
            "2026-11-10T10:00:00.900000000+00:00".to_string(),
        )];
        let logs = vec![
            log("plan", "plan", None, None),
            log(
                "implement-1",
                "implement",
                Some("2026-11-10T10:00:00.950Z"),
                None,
            ),
            log("triage", "triage", None, Some("2026-11-10T10:00:00Z")),
        ];
        let out = order_logs(logs, &history);
        let ids: Vec<&str> = out.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(ids, vec!["triage", "plan", "implement-1"]);
        assert_eq!(
            out[1].started_at.as_deref(),
            Some("2026-11-10T10:00:00.900Z")
        );
        assert_eq!(
            out[0].started_at.as_deref(),
            Some("2026-11-10T10:00:00.000Z")
        );
    }

    #[test]
    fn a_stored_round_is_kept() {
        let mut l = log("plan-1", "plan", Some("2026-11-10T10:00:00.000Z"), None);
        l.round = Some(3);
        let out = order_logs(vec![l], &[]);
        assert_eq!(out[0].round, Some(3));
    }

    #[test]
    fn content_encodings_and_decoding() {
        assert_eq!(content_encoding("\"Wrote a.rs\""), "json");
        assert_eq!(content_encoding("{\"a\":1}"), "json");
        assert_eq!(content_encoding("Error: Missing path"), "text");
        assert_eq!(content_encoding("\"unterminated"), "text");
        assert_eq!(decode_content("\"line 1\\nline 2\""), "line 1\nline 2");
        assert_eq!(decode_content("Error: x"), "Error: x");
        assert_eq!(decode_content("{\"a\":1}"), "{\"a\":1}");
    }

    #[test]
    fn results_are_capped() {
        let big = "x".repeat(MAX_RESULT_BYTES + 10);
        let c = cap_result(&big);
        assert!(c.ends_with(&format!("[truncated, {} bytes total]", big.len())));
        assert_eq!(cap_result("small"), "small");
    }

    #[test]
    fn line_delta_counts_adds_and_removes() {
        assert_eq!(line_delta("a\nb\nc", "a\nB\nc\nd"), (2, 1));
        assert_eq!(line_delta("", "a\nb"), (2, 0));
        assert_eq!(line_delta("a\nb", "b\na"), (0, 0));
    }

    #[test]
    fn credentials_are_redacted_and_code_is_kept() {
        let gh = format!("ghp_{}", "a".repeat(36));
        assert_eq!(
            redact_text(&format!("push with {gh} now")),
            "push with [redacted] now"
        );
        assert_eq!(
            redact_text("AWS AKIAABCDEFGHIJKLMNOP key"),
            "AWS [redacted] key"
        );
        assert_eq!(
            redact_text("Authorization: Bearer abcdefghijklmnop1234"),
            "Authorization: Bearer [redacted]"
        );
        assert_eq!(
            redact_text("git clone https://bob:hunter2@github.com/o/web"),
            "git clone https://[redacted]@github.com/o/web"
        );
        assert_eq!(
            redact_text("password = \"correcthorse\""),
            "password = \"[redacted]\""
        );
        assert_eq!(redact_text("API_KEY=abc123def456"), "API_KEY=[redacted]");
        assert!(redact_text(
            "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"
        )
        .contains("[redacted]"));
        assert!(!redact_text(
            "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"
        )
        .contains("MIIabc"));
        // Code that names secrets without holding one is left alone.
        for code in [
            "fn login(token: String, password: &str) -> bool",
            "let max_tokens = 16384;",
            "const api_key = process.env.API_KEY;",
            "secret: Option<Secret>,",
        ] {
            assert_eq!(redact_text(code), code, "{code}");
        }
    }

    #[test]
    fn entries_are_redacted_where_the_agent_wrote_and_read() {
        let tok = format!("ghs_{}", "b".repeat(36));
        let mut assistant = json!({
            "role": "assistant", "turn": 1,
            "usage": {"input_tokens": 5, "output_tokens": 7},
            "content": [
                {"type": "text", "text": format!("using {tok}")},
                {"type": "thinking", "thinking": format!("the token is {tok}"), "signature": "sig"},
                {"type": "tool_use", "id": "t1", "name": "check_url",
                 "input": {"urls": [format!("https://x:{tok}@h.io")], "api_key": "plainvalue"}}
            ]
        });
        redact_entry(&mut assistant);
        let s = assistant.to_string();
        assert!(!s.contains(&tok), "{s}");
        assert!(!s.contains("plainvalue"), "{s}");
        assert_eq!(assistant["usage"]["input_tokens"], 5, "usage kept");
        assert_eq!(assistant["content"][1]["signature"], "sig");

        let mut results = json!({
            "role": "tool_results", "turn": 1,
            "content": [{"type": "tool_result", "tool_use_id": "t1",
                         "content": format!("\"TOKEN={tok}\""),
                         "outcome": {"status": "ok", "summary": format!("read {tok}")}}]
        });
        redact_entry(&mut results);
        assert!(!results.to_string().contains(&tok));
        assert_eq!(results["content"][0]["tool_use_id"], "t1");

        let mut note = json!({"role": "system_note", "text": format!("x {tok}")});
        redact_entry(&mut note);
        assert_eq!(note["text"], "x [redacted]");
    }

    #[test]
    fn live_events_old_and_new() {
        let old = live_event(
            &json!({"tool": "read_file", "input_summary": "a.rs", "duration_ms": 12, "is_error": false}),
            "2026-11-10T10:00:00Z",
        );
        assert_eq!(old["tool"], "read_file");
        assert_eq!(old["pass"], Value::Null);
        assert_eq!(old["tool_use_id"], Value::Null);
        assert_eq!(old["started_at"], Value::Null);
        let new = live_event(
            &json!({"tool": "edit_file", "input_summary": "a.rs", "duration_ms": 40,
                    "is_error": false, "pass": "implement", "tool_use_id": "toolu_1",
                    "started_at": "2026-11-10T09:59:59.960Z",
                    "outcome": {"status": "soft_fail", "summary": "no edits applied"}}),
            "2026-11-10T10:00:00Z",
        );
        assert_eq!(new["pass"], "implement");
        assert_eq!(new["tool_use_id"], "toolu_1");
        assert_eq!(new["started_at"], "2026-11-10T09:59:59.960Z");
        assert_eq!(new["outcome"]["status"], "soft_fail");
        assert_eq!(new["duration_ms"], 40);
    }
}
