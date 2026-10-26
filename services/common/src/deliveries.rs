//! Deliveries: every event that comes in and every message that goes out,
//! with what happened to it, for the Deliveries page.
//!
//! One record per delivery on the settings table: `pk` is the team
//! (`TEAM#…`, or `HYBRID#<install>` for the hosted relay's own records),
//! `sk` is `DELIVERY#<ulid>` so a query lists newest first. Records expire
//! after [`RETENTION_DAYS`] (`ttl`). Headers are kept with secrets removed;
//! a payload is kept only as a short preview with secret-looking fields
//! masked ([`redact_json`], [`PREVIEW_BYTES`]).
//!
//! The existing Jira event log and alert events stay where they are; the
//! gateway maps them into the same shape when it lists ([`Delivery`]), so
//! nothing is written twice.

use std::collections::BTreeMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How long delivery records are kept.
pub const RETENTION_DAYS: i64 = 14;
/// The most of a payload a record keeps (after masking).
pub const PREVIEW_BYTES: usize = 4096;
/// The most attempts one record lists.
pub const MAX_ATTEMPTS_KEPT: usize = 20;

pub const SK_PREFIX: &str = "DELIVERY#";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Inbound,
    Outbound,
}

/// Where a delivery stands. `Handled` and `Ignored` are both fine; `Failed`
/// needs a look; `Pending` is being retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Received,
    Pending,
    Handled,
    Ignored,
    Failed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Pending => "pending",
            Self::Handled => "handled",
            Self::Ignored => "ignored",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "received" => Self::Received,
            "pending" => Self::Pending,
            "handled" => Self::Handled,
            "ignored" => Self::Ignored,
            "failed" => Self::Failed,
            _ => return None,
        })
    }

    pub fn is_ok(self) -> bool {
        matches!(self, Self::Handled | Self::Ignored)
    }
}

/// One try at a delivery (an outbound send, a relay forward, a redelivery).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub at: String,
    /// The answer's HTTP status, when there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// What Redeliver does for a record, when it is safe to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Redeliver {
    /// Ask GitHub to send the delivery again (its own redelivery API).
    Github,
    /// Forward the held payload to the install again (hosted relay).
    Relay,
    /// Send a test message to the same channel (the original isn't kept).
    NotifyTest,
}

/// One delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub id: String,
    pub direction: Direction,
    /// `github`, `relay`, `jira`, `alerts`, `slack`, `teams`, `email`, `broker`, `knowledge`.
    pub source: String,
    /// The event or message type, e.g. `pull_request.opened`.
    pub event: String,
    /// Where it went: a team, an install, a host, an email domain.
    pub target: String,
    pub status: Status,
    /// What happened, in a few words (`Queued a review`, `Forwarded`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
    /// The reason code when it failed ([`reason`] has the words and the fix).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// The provider's own id (GitHub's `X-GitHub-Delivery`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    pub created_at: String,
    pub duration_ms: u64,
    #[serde(default)]
    pub attempts: Vec<Attempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_preview: String,
    #[serde(default)]
    pub payload_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redeliver: Option<Redeliver>,
}

impl Delivery {
    pub fn new(direction: Direction, source: &str, event: &str, target: &str) -> Self {
        Self {
            id: ulid::Ulid::new().to_string(),
            direction,
            source: source.into(),
            event: event.into(),
            target: target.into(),
            status: Status::Received,
            outcome: String::new(),
            reason: String::new(),
            external_id: String::new(),
            repo: String::new(),
            pr: None,
            created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            duration_ms: 0,
            attempts: vec![],
            next_retry_at: None,
            headers: BTreeMap::new(),
            payload_preview: String::new(),
            payload_bytes: 0,
            redeliver: None,
        }
    }

    /// Keeps the headers, with secrets removed.
    pub fn with_headers<'a>(
        mut self,
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        self.headers = redact_headers(headers);
        self
    }

    /// Keeps a masked, size-limited preview of a JSON (or text) payload.
    pub fn with_payload(mut self, body: &[u8]) -> Self {
        self.payload_bytes = body.len();
        self.payload_preview = preview(body);
        self
    }

    pub fn finish(&mut self, status: Status, outcome: &str, reason: &str) {
        self.status = status;
        self.outcome = outcome.into();
        self.reason = reason.into();
    }

    /// Adds an attempt (newest last), keeping at most [`MAX_ATTEMPTS_KEPT`].
    pub fn push_attempt(&mut self, a: Attempt) {
        self.duration_ms = a.duration_ms;
        self.attempts.push(a);
        if self.attempts.len() > MAX_ATTEMPTS_KEPT {
            let drop = self.attempts.len() - MAX_ATTEMPTS_KEPT;
            self.attempts.drain(0..drop);
        }
    }
}

const SECRET_WORDS: &[&str] = &[
    "authorization",
    "cookie",
    "secret",
    "token",
    "password",
    "passwd",
    "signature",
    "private_key",
    "privatekey",
    "api_key",
    "apikey",
    "x-api-key",
    "credential",
    "webhook_url",
    "session",
];

fn secret_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    SECRET_WORDS.iter().any(|w| n.contains(w))
}

/// Headers with every secret-looking one replaced by `[redacted]`, names lowercased.
pub fn redact_headers<'a>(
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let k = k.to_ascii_lowercase();
            let v = if secret_name(&k) {
                "[redacted]".to_string()
            } else {
                v.chars().take(300).collect()
            };
            (k, v)
        })
        .collect()
}

/// A JSON value with every secret-looking field's value masked.
pub fn redact_json(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| {
                    let masked = secret_name(k) && !v.is_object() && !v.is_array() && !v.is_null();
                    (
                        k.clone(),
                        if masked {
                            Value::String("[redacted]".into())
                        } else {
                            redact_json(v)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(redact_json).collect()),
        Value::String(s) if looks_like_secret(s) => Value::String("[redacted]".into()),
        other => other.clone(),
    }
}

/// Strings that are credentials whatever their field is called.
fn looks_like_secret(s: &str) -> bool {
    let t = s.trim();
    [
        "ghs_",
        "ghp_",
        "gho_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
        "sk-ant-",
        "AKIA",
        "chk_",
    ]
    .iter()
    .any(|p| t.starts_with(p))
        || t.starts_with("https://hooks.slack.com/")
        || t.contains("-----BEGIN")
}

/// A preview of a payload: masked JSON, pretty enough to read, cut at
/// [`PREVIEW_BYTES`] on a character boundary. Non-JSON bodies are not kept.
pub fn preview(body: &[u8]) -> String {
    let Ok(v) = serde_json::from_slice::<Value>(body) else {
        return if body.is_empty() {
            String::new()
        } else {
            format!("[{} bytes, not JSON: not kept]", body.len())
        };
    };
    let text = serde_json::to_string_pretty(&redact_json(&v)).unwrap_or_default();
    if text.len() <= PREVIEW_BYTES {
        return text;
    }
    let mut end = PREVIEW_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n… cut at {} of {} bytes",
        &text[..end],
        PREVIEW_BYTES,
        text.len()
    )
}

/// A failure reason in plain words, with the fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Reason {
    pub code: &'static str,
    pub message: &'static str,
    pub fix: &'static str,
}

const REASONS: &[Reason] = &[
    Reason { code: "signature_mismatch", message: "GitHub's signature didn't match this install's webhook secret.", fix: "The secret in the GitHub App's settings (Webhook secret) must be the one your install has. Run the doctor command from Check your install, or create the app again." },
    Reason { code: "no_team", message: "No team is linked to this GitHub installation, so the event had nowhere to go.", fix: "Open Settings, GitHub and install the app from there, or link the installation." },
    Reason { code: "bad_payload", message: "The body wasn't JSON or named no installation.", fix: "Nothing to do for GitHub's own deliveries; a hand-made request needs installation.id." },
    Reason { code: "enqueue_failed", message: "The event was understood but its work couldn't be queued.", fix: "Usually temporary. Press Redeliver; if it keeps failing, check the queue in Settings, System." },
    Reason { code: "handler_error", message: "The event was understood but handling it failed.", fix: "Press Redeliver. If it keeps failing, contact support with the delivery id." },
    Reason { code: "relay_unreachable", message: "Your install didn't answer the forward (connection failed or timed out).", fix: "Check that your install is running: open its address. CoderHelm keeps retrying for 24 hours." },
    Reason { code: "relay_signature", message: "Your install refused the forward's signature.", fix: "The relay keys differ: register the install again in Settings, System, Hybrid." },
    Reason { code: "relay_server_error", message: "Your install answered the forward with an error.", fix: "Open the delivery on your install's Deliveries page for its own reason. CoderHelm retries meanwhile." },
    Reason { code: "relay_refused", message: "Your install refused the forward.", fix: "Usually the event isn't one it handles yet: update CoderHelm. Redeliver once it's updated." },
    Reason { code: "relay_gave_up", message: "Forwarding stopped after 24 hours of failures.", fix: "Fix what the attempts show, then press Redeliver while the payload is still held, or redeliver from GitHub." },
    Reason { code: "relay_not_linked", message: "The installation is linked to an install that was removed or turned off.", fix: "Link it again in Settings, System, Hybrid, or uninstall the app on GitHub." },
    Reason { code: "replayed", message: "This delivery was already received once; the copy was refused.", fix: "Nothing to do: the first copy was handled." },
    Reason { code: "signature_stale", message: "The forward was more than 5 minutes old when it arrived.", fix: "Usually a slow retry; the next one passes. Check your install's clock if it keeps happening." },
    Reason { code: "chat_webhook_gone", message: "The Slack or Teams webhook doesn't exist any more.", fix: "Paste a new incoming webhook in Settings, Notifications." },
    Reason { code: "chat_rejected", message: "Slack or Teams refused the message.", fix: "Check the webhook in Settings, Notifications and send a test." },
    Reason { code: "chat_unreachable", message: "Slack or Teams didn't answer in time.", fix: "Usually temporary. Send a test from Settings, Notifications." },
    Reason { code: "email_sandbox", message: "Amazon SES only sends to confirmed addresses on this account (the SES sandbox).", fix: "Request production access in Amazon SES, or confirm the recipient." },
    Reason { code: "email_denied", message: "The service isn't allowed to send from this address.", fix: "Confirm the sender in Amazon SES (Settings, System shows how)." },
    Reason { code: "email_failed", message: "Amazon SES refused the email.", fix: "Send a test email from Settings, System for the reason." },
    Reason { code: "broker_not_linked", message: "That GitHub installation isn't linked to this install.", fix: "Link the installation again in Settings, System, Hybrid." },
    Reason { code: "broker_repository_not_allowed", message: "That repository isn't one this install was given.", fix: "Add it on GitHub and in Settings, System, Hybrid." },
    Reason { code: "broker_refused", message: "CoderHelm's token broker refused the request.", fix: "Open Settings, System, Hybrid: it shows the link's state and how to fix it." },
    Reason { code: "broker_unreachable", message: "CoderHelm's token broker didn't answer.", fix: "Usually temporary; the next request retries. status.coderhelm.com shows any outage." },
    Reason { code: "jira_signature", message: "The Jira webhook's signature didn't match.", fix: "Copy the webhook secret from Settings, Jira into Jira's webhook again." },
    Reason { code: "jira_limit", message: "The team's monthly token limit was reached, so the ticket wasn't started.", fix: "Raise the limit in Settings, Usage, or wait for next month." },
];

/// The words and fix for a reason code (a generic one for unknown codes).
pub fn reason(code: &str) -> Reason {
    REASONS
        .iter()
        .copied()
        .find(|r| r.code == code)
        .unwrap_or(Reason {
            code: "unknown",
            message: "The delivery failed.",
            fix: "Press Redeliver where it's offered, or contact support with the delivery id.",
        })
}

/// The reason code for a failed HTTP forward or send.
pub fn reason_for_http(source: &str, code: Option<u16>) -> &'static str {
    match (source, code) {
        ("relay", None) => "relay_unreachable",
        ("relay", Some(401 | 403)) => "relay_signature",
        ("relay", Some(409)) => "replayed",
        ("relay", Some(c)) if c >= 500 => "relay_server_error",
        ("relay", Some(_)) => "relay_refused",
        ("slack" | "teams", Some(404 | 410)) => "chat_webhook_gone",
        ("slack" | "teams", None) => "chat_unreachable",
        ("slack" | "teams", Some(_)) => "chat_rejected",
        _ => "unknown",
    }
}

/// The record for one email send: the recipient's domain only, never the address.
pub fn email(kind: &str, to: &str, result: &Result<(), String>) -> Delivery {
    let domain = to.rsplit('@').next().unwrap_or("").to_ascii_lowercase();
    let mut d = Delivery::new(Direction::Outbound, "email", kind, &domain);
    match result {
        Ok(()) => d.finish(Status::Handled, "Sent", ""),
        Err(e) => {
            let r = e.to_ascii_lowercase();
            let reason = if r.contains("not verified") || r.contains("sandbox") {
                "email_sandbox"
            } else if r.contains("accessdenied") || r.contains("not authorized") {
                "email_denied"
            } else {
                "email_failed"
            };
            let at = d.created_at.clone();
            d.push_attempt(Attempt {
                at,
                code: None,
                duration_ms: 0,
                error: reason_text(reason).into(),
            });
            d.finish(Status::Failed, "Not sent", reason);
        }
    }
    d
}

fn reason_text(code: &str) -> &'static str {
    reason(code).message
}

/// The HTTP status in a notifier's error ("webhook returned 404 Not Found: …").
pub fn status_in(error: &str) -> Option<u16> {
    let rest = error.split("returned ").nth(1)?;
    rest.split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

/// The delivery records for one chat notification (a row per channel that
/// was configured). The webhook URL is a secret and is never kept.
pub fn from_chat(what: &str, sent: &crate::notify::Delivery, duration_ms: u64) -> Vec<Delivery> {
    let mut out = vec![];
    for (source, result) in [("teams", &sent.teams), ("slack", &sent.slack)] {
        let Some(result) = result else { continue };
        let mut d = Delivery::new(
            Direction::Outbound,
            source,
            what,
            if source == "teams" {
                "Microsoft Teams"
            } else {
                "Slack"
            },
        );
        let at = d.created_at.clone();
        match result {
            Ok(()) => {
                d.push_attempt(Attempt {
                    at,
                    code: Some(200),
                    duration_ms,
                    error: String::new(),
                });
                d.finish(Status::Handled, "Posted", "");
            }
            Err(e) => {
                let code = status_in(e);
                d.push_attempt(Attempt {
                    at,
                    code,
                    duration_ms,
                    error: e.chars().take(300).collect(),
                });
                d.finish(Status::Failed, "Not posted", reason_for_http(source, code));
            }
        }
        out.push(d);
    }
    out
}

/// Records one chat notification's deliveries.
pub async fn record_chat(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    what: &str,
    sent: &crate::notify::Delivery,
    duration_ms: u64,
) {
    for d in from_chat(what, sent, duration_ms) {
        record(dynamo, table, pk, &d).await;
    }
}

/// When the next relay retry is due after `attempts` tries, or None when
/// it's time to give up (about 24 hours in).
pub fn next_retry_secs(attempts: usize) -> Option<i64> {
    const BACKOFF: &[i64] = &[
        30, 120, 600, 900, 900, 1800, 3600, 3600, 7200, 14400, 21600, 21600, 21600,
    ];
    BACKOFF.get(attempts.saturating_sub(1)).copied()
}

/// The health line at the top of the page.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Summary {
    pub total: usize,
    pub ok: usize,
    pub failed: usize,
    pub pending: usize,
    /// Share of finished deliveries that went fine, 0..=1 (None with none).
    pub success_rate: Option<f64>,
    pub oldest_pending_at: Option<String>,
    pub last_failure_at: Option<String>,
}

/// The summary of deliveries created at or after `since` (RFC 3339).
pub fn summarize(items: &[Delivery], since: &str) -> Summary {
    let mut s = Summary::default();
    for d in items.iter().filter(|d| d.created_at.as_str() >= since) {
        s.total += 1;
        match d.status {
            Status::Failed => {
                s.failed += 1;
                if s.last_failure_at
                    .as_deref()
                    .is_none_or(|t| d.created_at.as_str() > t)
                {
                    s.last_failure_at = Some(d.created_at.clone());
                }
            }
            Status::Pending | Status::Received => {
                s.pending += 1;
                if s.oldest_pending_at
                    .as_deref()
                    .is_none_or(|t| d.created_at.as_str() < t)
                {
                    s.oldest_pending_at = Some(d.created_at.clone());
                }
            }
            _ => s.ok += 1,
        }
    }
    let finished = s.ok + s.failed;
    s.success_rate = (finished > 0).then(|| s.ok as f64 / finished as f64);
    s
}

/// The page's filters.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Filter {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    /// Only deliveries at or after this time (RFC 3339).
    #[serde(default)]
    pub since: Option<String>,
    /// A delivery id, GitHub delivery id, repository or PR number.
    #[serde(default)]
    pub q: Option<String>,
}

impl Filter {
    pub fn matches(&self, d: &Delivery) -> bool {
        if let Some(s) = self
            .source
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "all")
        {
            if d.source != s {
                return false;
            }
        }
        if let Some(st) = self
            .status
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "all")
        {
            let ok = match st {
                "ok" => d.status.is_ok(),
                "pending" => matches!(d.status, Status::Pending | Status::Received),
                other => d.status.as_str() == other,
            };
            if !ok {
                return false;
            }
        }
        if let Some(since) = self.since.as_deref().filter(|s| !s.is_empty()) {
            if d.created_at.as_str() < since {
                return false;
            }
        }
        if let Some(q) = self.q.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
            let q = q.trim_start_matches('#').to_ascii_lowercase();
            let pr = d.pr.map(|p| p.to_string()).unwrap_or_default();
            let hay = [
                d.id.to_ascii_lowercase(),
                d.external_id.to_ascii_lowercase(),
                d.repo.to_ascii_lowercase(),
                d.event.to_ascii_lowercase(),
            ];
            if pr != q && !hay.iter().any(|h| h.contains(&q)) {
                return false;
            }
        }
        true
    }
}

/// The `ttl` for a record created now.
pub fn ttl_from(now_epoch: i64) -> i64 {
    now_epoch + RETENTION_DAYS * 86_400
}

/// The settings-table item for a delivery.
pub fn to_item(
    pk: &str,
    d: &Delivery,
    now_epoch: i64,
) -> std::collections::HashMap<String, AttributeValue> {
    let s = |v: &str| AttributeValue::S(v.to_string());
    let mut item = std::collections::HashMap::new();
    item.insert("pk".into(), s(pk));
    item.insert("sk".into(), s(&format!("{SK_PREFIX}{}", d.id)));
    item.insert("status".into(), s(d.status.as_str()));
    item.insert("source".into(), s(&d.source));
    item.insert("created_at".into(), s(&d.created_at));
    item.insert(
        "data".into(),
        s(&serde_json::to_string(d).unwrap_or_default()),
    );
    item.insert(
        "ttl".into(),
        AttributeValue::N(ttl_from(now_epoch).to_string()),
    );
    item
}

/// A delivery from its settings-table item.
pub fn from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Delivery> {
    let data = item.get("data")?.as_s().ok()?;
    serde_json::from_str(data).ok()
}

/// Writes (or replaces) one delivery record. Failures are logged and never
/// fail the delivery itself: the record is for people, the work matters more.
pub async fn record(dynamo: &aws_sdk_dynamodb::Client, table: &str, pk: &str, d: &Delivery) {
    if table.is_empty() || pk.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp();
    if let Err(e) = dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(to_item(pk, d, now)))
        .send()
        .await
    {
        tracing::warn!(error = %e, delivery = %d.id, source = %d.source, "Could not record a delivery");
    }
}

/// One record by id.
pub async fn get(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    id: &str,
) -> Result<Option<Delivery>, String> {
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(format!("{SK_PREFIX}{id}")))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(r.item().and_then(from_item))
}

/// The newest records, at most `limit` (scans up to 1,000 to fill a filtered page).
pub async fn list(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<Delivery>, String> {
    let mut out = Vec::new();
    let mut start = None;
    let mut read = 0usize;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :d)")
            .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
            .expression_attribute_values(":d", AttributeValue::S(SK_PREFIX.into()))
            .scan_index_forward(false)
            .limit(200)
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        for item in r.items() {
            read += 1;
            if let Some(d) = from_item(item) {
                if filter.matches(&d) {
                    out.push(d);
                }
            }
        }
        start = r.last_evaluated_key().cloned();
        if out.len() >= limit || start.is_none() || read >= 1000 {
            break;
        }
    }
    out.truncate(limit);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn headers_lose_their_secrets() {
        let h = redact_headers([
            ("X-Hub-Signature-256", "sha256=abc"),
            ("Authorization", "Bearer x"),
            ("X-GitHub-Event", "push"),
            ("X-CoderHelm-Relay-Signature", "t=1,d=2,v1=3"),
            ("Cookie", "a=b"),
        ]);
        assert_eq!(h["x-hub-signature-256"], "[redacted]");
        assert_eq!(h["authorization"], "[redacted]");
        assert_eq!(h["x-coderhelm-relay-signature"], "[redacted]");
        assert_eq!(h["cookie"], "[redacted]");
        assert_eq!(h["x-github-event"], "push");
    }

    #[test]
    fn payloads_are_masked_and_cut() {
        let v = json!({
            "action": "opened",
            "installation": {"id": 1, "access_token": "abc"},
            "hook": {"config": {"secret": "s", "url": "https://example.org"}},
            "comment": {"body": "token ghp_0123456789"},
            "note": "ghs_abcdef",
            "list": [{"password": "p"}],
            "issue": {"key": "ABC-1"}
        });
        let r = redact_json(&v);
        assert_eq!(r["installation"]["access_token"], "[redacted]");
        assert_eq!(r["hook"]["config"]["secret"], "[redacted]");
        assert_eq!(r["note"], "[redacted]");
        assert_eq!(r["list"][0]["password"], "[redacted]");
        assert_eq!(r["issue"]["key"], "ABC-1", "a ticket key is not a secret");
        assert_eq!(r["action"], "opened");
        let big = json!({"text": "x".repeat(10_000)});
        let p = preview(big.to_string().as_bytes());
        assert!(p.len() < PREVIEW_BYTES + 100 && p.contains("cut at"));
        assert_eq!(preview(b"not json"), "[8 bytes, not JSON: not kept]");
        assert_eq!(preview(b""), "");
        // Multi-byte characters are cut on a boundary.
        let wide = json!({"text": "é".repeat(5000)});
        assert!(preview(wide.to_string().as_bytes()).contains("cut at"));
    }

    #[test]
    fn a_record_round_trips_through_its_item() {
        let mut d = Delivery::new(
            Direction::Inbound,
            "github",
            "pull_request.opened",
            "TEAM#1",
        )
        .with_headers([("X-GitHub-Event", "pull_request")])
        .with_payload(br#"{"number": 7}"#);
        d.external_id = "72d3162e".into();
        d.repo = "example-org/web".into();
        d.pr = Some(7);
        d.finish(Status::Handled, "Queued a review", "");
        let item = to_item("TEAM#1", &d, 1_800_000_000);
        assert_eq!(item["sk"].as_s().unwrap(), &format!("DELIVERY#{}", d.id));
        assert_eq!(
            item["ttl"].as_n().unwrap(),
            &(1_800_000_000 + 14 * 86_400).to_string()
        );
        assert_eq!(item["status"].as_s().unwrap(), "handled");
        assert_eq!(from_item(&item).unwrap(), d);
    }

    #[test]
    fn attempts_are_bounded_and_retries_back_off_then_stop() {
        let mut d = Delivery::new(Direction::Outbound, "relay", "push", "ins_x");
        for i in 0..30 {
            d.push_attempt(Attempt {
                at: i.to_string(),
                code: Some(502),
                duration_ms: i,
                error: String::new(),
            });
        }
        assert_eq!(d.attempts.len(), MAX_ATTEMPTS_KEPT);
        assert_eq!(d.attempts.last().unwrap().at, "29");
        assert_eq!(d.duration_ms, 29);
        assert_eq!(next_retry_secs(1), Some(30));
        let total: i64 = (1..20).map_while(next_retry_secs).sum();
        assert!(
            (20 * 3600..=30 * 3600).contains(&total),
            "about a day of retries: {total}"
        );
        assert_eq!(next_retry_secs(14), None);
    }

    #[test]
    fn emails_keep_the_domain_only() {
        let ok = email("invite", "Someone@Example.com", &Ok(()));
        assert_eq!(
            (ok.target.as_str(), ok.status),
            ("example.com", Status::Handled)
        );
        let sandbox = email(
            "invite",
            "a@example.com",
            &Err("MessageRejected: Email address is not verified".into()),
        );
        assert_eq!(sandbox.reason, "email_sandbox");
        assert!(!format!("{sandbox:?}").contains("a@example.com"));
        assert_eq!(
            email("x", "a@b.c", &Err("AccessDeniedException".into())).reason,
            "email_denied"
        );
        assert_eq!(
            email("x", "a@b.c", &Err("boom".into())).reason,
            "email_failed"
        );
    }

    #[test]
    fn chat_notifications_become_one_row_per_channel() {
        let sent = crate::notify::Delivery {
            teams: Some(Ok(())),
            slack: Some(Err("webhook returned 404 Not Found: no_service".into())),
        };
        let rows = from_chat("alert", &sent, 120);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].source.as_str(), rows[0].status),
            ("teams", Status::Handled)
        );
        assert_eq!(
            (
                rows[1].source.as_str(),
                rows[1].status,
                rows[1].reason.as_str()
            ),
            ("slack", Status::Failed, "chat_webhook_gone")
        );
        assert_eq!(rows[1].attempts[0].code, Some(404));
        let none = crate::notify::Delivery::default();
        assert!(from_chat("alert", &none, 0).is_empty());
        let timeout = crate::notify::Delivery {
            teams: Some(Err("operation timed out".into())),
            slack: None,
        };
        assert_eq!(from_chat("x", &timeout, 0)[0].reason, "chat_unreachable");
        assert_eq!(status_in("webhook returned 500 Internal"), Some(500));
        assert_eq!(status_in("nope"), None);
    }

    #[test]
    fn failures_read_plainly_with_a_fix() {
        for code in [
            "relay_unreachable",
            "signature_mismatch",
            "chat_webhook_gone",
            "email_sandbox",
            "broker_not_linked",
        ] {
            let r = reason(code);
            assert_eq!(r.code, code);
            assert!(r.message.ends_with('.') && r.fix.ends_with('.'));
        }
        assert_eq!(reason("nope").code, "unknown");
        assert_eq!(reason_for_http("relay", None), "relay_unreachable");
        assert_eq!(reason_for_http("relay", Some(401)), "relay_signature");
        assert_eq!(reason_for_http("relay", Some(503)), "relay_server_error");
        assert_eq!(reason_for_http("relay", Some(422)), "relay_refused");
        assert_eq!(reason_for_http("slack", Some(410)), "chat_webhook_gone");
        assert_eq!(reason_for_http("teams", None), "chat_unreachable");
        // Every code a reason_for_http gives has words.
        for c in [None, Some(401), Some(409), Some(500), Some(400)] {
            assert_ne!(reason(reason_for_http("relay", c)).code, "unknown");
        }
    }

    fn at(t: &str, st: Status, source: &str) -> Delivery {
        let mut d = Delivery::new(Direction::Inbound, source, "push", "TEAM#1");
        d.created_at = t.into();
        d.status = st;
        d
    }

    #[test]
    fn the_summary_counts_the_last_day() {
        let items = vec![
            at("2026-10-25T10:00:00Z", Status::Handled, "github"),
            at("2026-10-25T11:00:00Z", Status::Failed, "github"),
            at("2026-10-25T12:00:00Z", Status::Pending, "relay"),
            at("2026-10-25T09:00:00Z", Status::Ignored, "github"),
            at("2026-10-23T09:00:00Z", Status::Failed, "github"),
        ];
        let s = summarize(&items, "2026-10-24T12:00:00Z");
        assert_eq!((s.total, s.ok, s.failed, s.pending), (4, 2, 1, 1));
        assert!((s.success_rate.unwrap() - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(s.oldest_pending_at.as_deref(), Some("2026-10-25T12:00:00Z"));
        assert_eq!(s.last_failure_at.as_deref(), Some("2026-10-25T11:00:00Z"));
        assert_eq!(summarize(&[], "x").success_rate, None);
    }

    #[test]
    fn filters_find_by_source_status_time_and_search() {
        let mut d = at("2026-10-25T10:00:00Z", Status::Failed, "github");
        d.repo = "example-org/web".into();
        d.pr = Some(42);
        d.external_id = "abc-123".into();
        d.id = "01JBDELIVERYID".into();
        let f = |source: Option<&str>, status: Option<&str>, q: Option<&str>| Filter {
            source: source.map(Into::into),
            status: status.map(Into::into),
            since: None,
            q: q.map(Into::into),
        };
        assert!(f(Some("github"), Some("failed"), None).matches(&d));
        assert!(!f(Some("relay"), None, None).matches(&d));
        assert!(!f(None, Some("ok"), None).matches(&d));
        assert!(f(None, None, Some("#42")).matches(&d));
        assert!(f(None, None, Some("WEB")).matches(&d));
        assert!(f(None, None, Some("abc-123")).matches(&d));
        assert!(
            !f(None, None, Some("4")).matches(&d),
            "a PR number matches whole"
        );
        assert!(f(Some("all"), Some("all"), Some("")).matches(&d));
        let since = Filter {
            since: Some("2026-10-26T00:00:00Z".into()),
            ..Default::default()
        };
        assert!(!since.matches(&d));
    }
}
