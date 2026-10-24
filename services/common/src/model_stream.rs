//! Anthropic Messages calls read as a stream (SSE), metered per attempt.
//!
//! A non-streaming call that dies after Anthropic started work (a timeout, a
//! dropped connection, the Lambda's time running out) is billed by Anthropic
//! and was recorded as nothing. Streamed, the usage arrives as the call runs:
//! `message_start` carries the input, cache-read and cache-write tokens at
//! once, and `message_delta` carries the output count. So:
//!
//! - every attempt that got a reply is recorded, each as its own call,
//!   retried ones included;
//! - an attempt that broke off is recorded with what arrived; if the final
//!   output count never came, the output is estimated from the bytes of
//!   text, thinking and tool input received ([`BYTES_PER_OUTPUT_TOKEN`]) and
//!   the record is marked `estimated`;
//! - before the Lambda's time runs out ([`set_deadline_epoch_ms`]) an
//!   in-flight call is cancelled through the same path, so its usage is
//!   recorded too, and a call dropped by its caller (an outer timeout) records
//!   itself on the way out ([`settle_dropped_calls`] waits for those writes).
//!
//! [`Accumulator`] rebuilds the final message (every block verbatim, thinking
//! signatures included), the same JSON the non-streaming API returns.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};

use futures::StreamExt;
use serde_json::{json, Map, Value};
use tracing::warn;

use crate::model_retry::{backoff, is_transient_error, is_transient_status};
use crate::model_retry::{ModelCallError, ModelErrorKind};
use crate::usage::{current_feature, CacheTtl, CallRecord, CallUsage, UsageMeter};

/// Bytes of streamed text, thinking or tool-input JSON per output token,
/// for estimating the output of a reply that broke off before its final
/// count. Claude's tokenizer averages about 3.5 characters per token on
/// English prose and fewer on code and JSON; 3 errs toward counting more,
/// never less, so a broken reply is not undercounted.
pub const BYTES_PER_OUTPUT_TOKEN: u64 = 3;

/// Bytes of request body per input token, used only when a reply broke off
/// before `message_start` (the input count never arrived). The body is
/// JSON, so this overcounts a little; such a record is marked estimated.
pub const BYTES_PER_INPUT_TOKEN: u64 = 4;

// ── SSE parsing ─────────────────────────────────────────────────────────────

/// Splits an SSE byte stream into the JSON of its `data:` lines. Bytes are
/// kept until a whole line is in, so a UTF-8 character split across chunks
/// is never mangled.
#[derive(Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    pub fn push(&mut self, chunk: &[u8]) -> Vec<Value> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(i) = self.buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=i).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim();
            if let Some(data) = line.strip_prefix("data:") {
                if let Ok(v) = serde_json::from_str::<Value>(data.trim()) {
                    out.push(v);
                }
            }
        }
        out
    }
}

// ── Rebuilding the message ──────────────────────────────────────────────────

/// Append one `content_block_delta` to its block. Tool input JSON is kept
/// raw in `partial_json` until the block stops.
pub fn accumulate_delta(block: &mut Value, delta: &Value, partial_json: &mut String) {
    let append = |block: &mut Value, field: &str, piece: &str| {
        let cur = block[field].as_str().unwrap_or("").to_string();
        block[field] = Value::String(cur + piece);
    };
    match delta["type"].as_str() {
        Some("text_delta") => append(block, "text", delta["text"].as_str().unwrap_or("")),
        Some("thinking_delta") => {
            append(block, "thinking", delta["thinking"].as_str().unwrap_or(""))
        }
        Some("signature_delta") => {
            block["signature"] = delta["signature"].clone();
        }
        Some("input_json_delta") => {
            partial_json.push_str(delta["partial_json"].as_str().unwrap_or(""));
        }
        _ => {}
    }
}

/// The bytes of model output in one delta (text, thinking, tool input).
fn output_bytes(delta: &Value) -> u64 {
    let s = match delta["type"].as_str() {
        Some("text_delta") => delta["text"].as_str(),
        Some("thinking_delta") => delta["thinking"].as_str(),
        Some("input_json_delta") => delta["partial_json"].as_str(),
        _ => None,
    };
    s.map_or(0, |s| s.len() as u64)
}

/// One streamed reply, rebuilt event by event.
#[derive(Default, Debug)]
pub struct Accumulator {
    /// `message_start` arrived (the input counts are known).
    started: bool,
    /// The final `message_delta` (with its stop reason) arrived.
    finished: bool,
    head: Map<String, Value>,
    blocks: BTreeMap<u64, Value>,
    block_json: HashMap<u64, String>,
    stop_reason: Option<String>,
    stop_details: Option<Value>,
    usage: Map<String, Value>,
    /// The latest cumulative output count from a `message_delta`.
    delta_output: Option<u64>,
    received_bytes: u64,
    /// An `error` event in the stream (e.g. `overloaded_error`).
    error: Option<Value>,
}

impl Accumulator {
    pub fn feed(&mut self, ev: &Value) {
        match ev["type"].as_str() {
            Some("message_start") => {
                self.started = true;
                if let Some(m) = ev["message"].as_object() {
                    for (k, v) in m {
                        match k.as_str() {
                            "usage" => {
                                if let Some(u) = v.as_object() {
                                    self.usage = u.clone();
                                }
                            }
                            "content" | "stop_reason" | "stop_sequence" => {}
                            _ => {
                                self.head.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
            }
            Some("content_block_start") => {
                let idx = ev["index"].as_u64().unwrap_or(0);
                self.blocks
                    .insert(idx, ev.get("content_block").cloned().unwrap_or(json!({})));
            }
            Some("content_block_delta") => {
                let idx = ev["index"].as_u64().unwrap_or(0);
                let delta = &ev["delta"];
                self.received_bytes += output_bytes(delta);
                if let Some(block) = self.blocks.get_mut(&idx) {
                    accumulate_delta(block, delta, self.block_json.entry(idx).or_default());
                }
            }
            Some("content_block_stop") => {
                let idx = ev["index"].as_u64().unwrap_or(0);
                self.close_block(idx);
            }
            Some("message_delta") => {
                if let Some(sr) = ev["delta"]["stop_reason"].as_str() {
                    self.stop_reason = Some(sr.to_string());
                    self.finished = true;
                }
                if let Some(d) = ev["delta"].get("stop_details").filter(|d| !d.is_null()) {
                    self.stop_details = Some(d.clone());
                }
                // Counts here are cumulative for the message: take the latest
                // value of each, never add them up.
                if let Some(u) = ev["usage"].as_object() {
                    for (k, v) in u {
                        if !v.is_null() {
                            self.usage.insert(k.clone(), v.clone());
                        }
                    }
                    if let Some(o) = u.get("output_tokens").and_then(|v| v.as_u64()) {
                        self.delta_output = Some(o);
                    }
                }
            }
            Some("error") => self.error = Some(ev["error"].clone()),
            _ => {}
        }
    }

    fn close_block(&mut self, idx: u64) {
        if let (Some(block), Some(raw)) = (self.blocks.get_mut(&idx), self.block_json.get(&idx)) {
            if block["type"].as_str() == Some("tool_use") {
                block["input"] = serde_json::from_str(raw).unwrap_or(json!({}));
            }
        }
    }

    pub fn started(&self) -> bool {
        self.started
    }

    /// The reply ended with its final usage.
    pub fn finished(&self) -> bool {
        self.finished
    }

    /// The `error` event the stream carried, if any.
    pub fn stream_error(&self) -> Option<&Value> {
        self.error.as_ref()
    }

    /// Bytes of output received so far.
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes
    }

    /// The text received so far (text blocks only).
    pub fn text(&self) -> String {
        self.blocks
            .values()
            .filter(|b| b["type"] == "text")
            .filter_map(|b| b["text"].as_str())
            .collect()
    }

    /// The usage to record, and whether it is an estimate. With the final
    /// `message_delta`: exactly what Anthropic reported. Without: the input
    /// parts from `message_start` (or, if even that never came, estimated
    /// from `request_bytes`), and the output as the larger of the last
    /// cumulative count and the bytes received ÷ [`BYTES_PER_OUTPUT_TOKEN`].
    /// `message_start`'s own output count is a placeholder and never used.
    pub fn usage(&self, request_bytes: u64) -> (CallUsage, bool) {
        let v = Value::Object(self.usage.clone());
        let reported = CallUsage::from_api(&v, CacheTtl::FiveMinutes);
        if self.finished {
            return (reported, false);
        }
        let estimate = self.received_bytes.div_ceil(BYTES_PER_OUTPUT_TOKEN);
        let output = self.delta_output.unwrap_or(0).max(estimate);
        let mut u = CallUsage { output, ..reported };
        if !self.started {
            u = CallUsage {
                uncached_input: request_bytes.div_ceil(BYTES_PER_INPUT_TOKEN),
                output,
                ..Default::default()
            };
        }
        (u, true)
    }

    /// The message as the non-streaming API returns it. Blocks still open
    /// are closed as they stand.
    pub fn into_message(mut self) -> Value {
        let open: Vec<u64> = self.block_json.keys().copied().collect();
        for idx in open {
            if self
                .blocks
                .get(&idx)
                .is_some_and(|b| b["input"].is_null() || b["input"] == json!({}))
            {
                self.close_block(idx);
            }
        }
        let mut m = self.head;
        m.insert(
            "content".into(),
            Value::Array(self.blocks.into_values().collect()),
        );
        m.insert("stop_reason".into(), json!(self.stop_reason));
        if let Some(d) = self.stop_details {
            m.insert("stop_details".into(), d);
        }
        m.insert("usage".into(), Value::Object(self.usage));
        Value::Object(m)
    }
}

// ── The deadline ────────────────────────────────────────────────────────────

static DEADLINE_MS: AtomicI64 = AtomicI64::new(0);

tokio::task_local! {
    static TASK_DEADLINE: tokio::time::Instant;
}

/// The time (ms since the epoch) by which in-flight model calls are
/// cancelled and recorded: the Lambda's deadline less a margin for the
/// writes. `None` clears it. One event at a time runs in a Lambda, so this
/// is per process.
pub fn set_deadline_epoch_ms(ms: Option<i64>) {
    DEADLINE_MS.store(ms.unwrap_or(0), Ordering::SeqCst);
}

/// Run `fut` with its own deadline (tests, and callers with a tighter one).
pub async fn with_deadline<F: std::future::Future>(at: tokio::time::Instant, fut: F) -> F::Output {
    TASK_DEADLINE.scope(at, fut).await
}

fn deadline() -> Option<tokio::time::Instant> {
    let task = TASK_DEADLINE.try_with(|d| *d).ok();
    let global = match DEADLINE_MS.load(Ordering::SeqCst) {
        0 => None,
        ms => {
            let left = ms - chrono::Utc::now().timestamp_millis();
            let now = tokio::time::Instant::now();
            Some(if left <= 0 {
                now
            } else {
                now + std::time::Duration::from_millis(left as u64)
            })
        }
    };
    match (task, global) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

async fn until(d: Option<tokio::time::Instant>) {
    match d {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

fn past(d: Option<tokio::time::Instant>) -> bool {
    d.is_some_and(|d| tokio::time::Instant::now() >= d)
}

fn deadline_error() -> ModelCallError {
    ModelCallError::new(
        ModelErrorKind::Network,
        "stopped before the Lambda's time ran out",
    )
}

// ── Recording a dropped call ────────────────────────────────────────────────

fn dropped_writes() -> &'static Mutex<Vec<tokio::task::JoinHandle<()>>> {
    static W: OnceLock<Mutex<Vec<tokio::task::JoinHandle<()>>>> = OnceLock::new();
    W.get_or_init(|| Mutex::new(Vec::new()))
}

/// Wait for the usage writes of calls dropped by their callers (an outer
/// timeout dropped the future mid-stream). The worker calls this before it
/// returns, so the Lambda isn't frozen with a write in flight.
pub async fn settle_dropped_calls() {
    let handles: Vec<_> = dropped_writes()
        .lock()
        .map(|mut v| std::mem::take(&mut *v))
        .unwrap_or_default();
    for h in handles {
        let _ = h.await;
    }
}

/// One attempt's reply as it streams in. Recorded exactly once: by
/// [`InFlight::finish`], or, if the future is dropped first, on drop.
pub struct InFlight {
    meter: UsageMeter,
    template: CallRecord,
    request_bytes: u64,
    pub acc: Accumulator,
    done: bool,
}

impl InFlight {
    pub fn new(
        meter: &UsageMeter,
        model_id: &str,
        request_id: Option<String>,
        request_bytes: u64,
    ) -> Self {
        let mut template = CallRecord::now(
            &meter.team_id,
            current_feature(),
            model_id,
            CallUsage::default(),
        );
        if let Some(id) = request_id.filter(|s| !s.is_empty()) {
            template.call_id = id;
        }
        Self {
            meter: meter.clone(),
            template,
            request_bytes,
            acc: Accumulator::default(),
            done: false,
        }
    }

    fn record(&mut self) -> CallRecord {
        self.done = true;
        let (usage, estimated) = self.acc.usage(self.request_bytes);
        CallRecord {
            usage,
            estimated,
            ..self.template.clone()
        }
    }

    /// Record this attempt (final or broken off) and return its usage.
    pub async fn finish(&mut self) -> (CallUsage, bool) {
        let rec = self.record();
        let out = (rec.usage, rec.estimated);
        if rec.estimated {
            warn!(model = %rec.model, call_id = %rec.call_id, output = rec.usage.output,
                received_bytes = self.acc.received_bytes(),
                "Model reply broke off; recording its usage with the output estimated");
        }
        self.meter.record_call(rec).await;
        out
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let rec = self.record();
        let meter = self.meter.clone();
        warn!(model = %rec.model, call_id = %rec.call_id,
            "Model call dropped mid-reply; recording its usage with the output estimated");
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let h = rt.spawn(async move { meter.record_call(rec).await });
            if let Ok(mut v) = dropped_writes().lock() {
                v.push(h);
            }
        }
    }
}

// ── One metered call ────────────────────────────────────────────────────────

/// A finished call: the rebuilt message and the usage of every attempt.
#[derive(Debug)]
pub struct Streamed {
    pub message: Value,
    pub request_id: Option<String>,
    /// The successful attempt's usage.
    pub usage: CallUsage,
    /// Attempts that broke off and were sent again, each already recorded.
    pub retried: Vec<CallUsage>,
}

fn api_error_text(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(v) if v["error"]["type"].is_string() => format!(
            "{}: {}",
            v["error"]["type"].as_str().unwrap_or(""),
            v["error"]["message"].as_str().unwrap_or("")
        ),
        _ => body.to_string(),
    }
}

/// The failure an `error` event in the stream stands for.
fn stream_error_kind(err: &Value) -> ModelCallError {
    let t = err["type"].as_str().unwrap_or("");
    let status = match t {
        "overloaded_error" => 529,
        "rate_limit_error" => 429,
        "api_error" => 500,
        "timeout_error" => 504,
        _ => 0,
    };
    let message = format!("{t}: {}", err["message"].as_str().unwrap_or(""));
    if status == 0 {
        ModelCallError::new(ModelErrorKind::Network, message)
    } else {
        ModelCallError::new(ModelErrorKind::Status(status), message)
    }
}

/// Send a Messages request with `"stream": true` (the caller's builder sets
/// it), read the reply as it arrives, and record every attempt's usage on
/// `meter`. Transient failures are retried on `schedule` like
/// [`crate::model_retry::send`], and so is a reply that broke off; each
/// attempt that got a reply is its own recorded call. `request_bytes` is the
/// body size, for the rare estimate when no `message_start` arrived.
pub async fn send_metered<F>(
    build: F,
    schedule: &[u64],
    label: &str,
    meter: &UsageMeter,
    model_id: &str,
    request_bytes: u64,
) -> Result<Streamed, ModelCallError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let attempts = schedule.len() + 1;
    let mut retried = Vec::new();
    for attempt in 0..attempts {
        let last = attempt + 1 == attempts;
        let d = deadline();
        if past(d) {
            return Err(deadline_error());
        }
        let sent = tokio::select! {
            r = build().send() => r,
            _ = until(d) => return Err(deadline_error()),
        };
        let resp = match sent {
            Err(e) => {
                if last || !is_transient_error(&e) {
                    return Err(e.into());
                }
                warn!(caller = label, error = %e, attempt = attempt + 1, "Model API request failed, retrying");
                tokio::time::sleep(backoff(schedule, attempt, None)).await;
                continue;
            }
            Ok(r) => r,
        };
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok());
            let body = resp.text().await.unwrap_or_default();
            if last || !is_transient_status(status) {
                return Err(ModelCallError::new(
                    ModelErrorKind::Status(status),
                    api_error_text(&body),
                ));
            }
            warn!(
                caller = label,
                status,
                attempt = attempt + 1,
                "Model API transient status, retrying"
            );
            tokio::time::sleep(backoff(schedule, attempt, retry_after)).await;
            continue;
        }
        let request_id = resp
            .headers()
            .get("request-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let mut flight = InFlight::new(meter, model_id, request_id.clone(), request_bytes);
        let mut parser = SseParser::default();
        let mut stream = resp.bytes_stream();
        let mut broke: Option<ModelCallError> = None;
        loop {
            let next = tokio::select! {
                n = stream.next() => n,
                _ = until(d) => {
                    broke = Some(deadline_error());
                    break;
                }
            };
            match next {
                None => break,
                Some(Err(e)) => {
                    broke = Some(e.into());
                    break;
                }
                Some(Ok(chunk)) => {
                    for ev in parser.push(&chunk) {
                        flight.acc.feed(&ev);
                    }
                    if flight.acc.stream_error().is_some() {
                        break;
                    }
                }
            }
        }
        let (usage, estimated) = flight.finish().await;
        if !estimated && flight.acc.stream_error().is_none() {
            let acc = std::mem::take(&mut flight.acc);
            return Ok(Streamed {
                message: acc.into_message(),
                request_id,
                usage,
                retried,
            });
        }
        let err = match (broke, flight.acc.stream_error()) {
            (_, Some(e)) => stream_error_kind(e),
            (Some(e), None) => e,
            (None, None) => ModelCallError::new(
                ModelErrorKind::Network,
                "the reply ended before its final usage",
            ),
        };
        if last || !err.is_transient() || past(deadline()) {
            return Err(err);
        }
        retried.push(usage);
        warn!(caller = label, error = %err, attempt = attempt + 1, "Model reply broke off, retrying");
        tokio::time::sleep(backoff(schedule, attempt, None)).await;
    }
    unreachable!("the last attempt always returns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::{MemoryFallback, MemoryStore, UsageMeter};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ev(v: Value) -> String {
        format!("event: {}\ndata: {}\n\n", v["type"].as_str().unwrap(), v)
    }

    fn start(input: u64, read: u64, write: u64) -> String {
        ev(
            json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"stop_reason":null,
            "usage":{"input_tokens":input,"cache_read_input_tokens":read,"cache_creation_input_tokens":write,"output_tokens":1}}}),
        )
    }

    fn text_block(text: &str) -> String {
        ev(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        ) + &ev(
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        )
    }

    fn end(output: u64) -> String {
        ev(json!({"type":"content_block_stop","index":0}))
            + &ev(
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":output}}),
            )
            + &ev(json!({"type":"message_stop"}))
    }

    /// What the server does on each request, in order (the last repeats).
    #[derive(Clone)]
    enum Reply {
        /// A 200 stream with this body, then close.
        Stream(String),
        /// A 200 stream with this body, then hang (never close).
        Hang(String),
    }

    async fn server(replies: Vec<Reply>) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let n = h.fetch_add(1, Ordering::SeqCst);
                let reply = replies.get(n).or(replies.last()).unwrap().clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let _ = sock.read(&mut buf).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nrequest-id: req_{n}\r\nconnection: close\r\n\r\n"
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    match reply {
                        Reply::Stream(body) => {
                            let _ = sock.write_all(body.as_bytes()).await;
                            let _ = sock.shutdown().await;
                        }
                        Reply::Hang(body) => {
                            let _ = sock.write_all(body.as_bytes()).await;
                            let _ = sock.flush().await;
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}/v1/messages"), hits)
    }

    async fn call(
        url: &str,
        schedule: &[u64],
        meter: &UsageMeter,
    ) -> Result<Streamed, ModelCallError> {
        let http = reqwest::Client::new();
        send_metered(
            || http.post(url).body("{\"stream\":true}"),
            schedule,
            "test",
            meter,
            "claude-opus-5-5",
            400,
        )
        .await
    }

    #[tokio::test]
    async fn a_whole_reply_is_rebuilt_and_recorded_once_with_its_request_id() {
        let body = start(10, 2000, 300) + &text_block("hello") + &end(42);
        let (url, _) = server(vec![Reply::Stream(body)]).await;
        let (meter, calls) = UsageMeter::memory("t1");
        let out = call(&url, &[], &meter).await.unwrap();
        assert_eq!(out.message["content"][0]["text"], "hello");
        assert_eq!(out.message["stop_reason"], "end_turn");
        assert_eq!(out.message["usage"]["output_tokens"], 42);
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].call_id, "req_0");
        assert!(!got[0].estimated);
        assert_eq!(
            got[0].usage,
            CallUsage {
                uncached_input: 10,
                cache_read: 2000,
                cache_write_5m: 300,
                cache_write_1h: 0,
                output: 42
            }
        );
    }

    #[tokio::test]
    async fn a_drop_after_message_start_records_the_input_and_estimates_the_output() {
        let (url, _) = server(vec![Reply::Stream(start(10, 2000, 300))]).await;
        let (meter, calls) = UsageMeter::memory("t1");
        let err = call(&url, &[], &meter).await.unwrap_err();
        assert!(err.is_transient());
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert!(got[0].estimated);
        assert_eq!(got[0].usage.uncached_input, 10);
        assert_eq!(got[0].usage.cache_read, 2000);
        assert_eq!(got[0].usage.cache_write_5m, 300);
        // Never message_start's placeholder output of 1.
        assert_eq!(got[0].usage.output, 0);
    }

    #[tokio::test]
    async fn a_drop_after_some_deltas_estimates_the_output_from_the_bytes() {
        let text = "x".repeat(300);
        let (url, _) = server(vec![Reply::Stream(start(10, 0, 0) + &text_block(&text))]).await;
        let (meter, calls) = UsageMeter::memory("t1");
        call(&url, &[], &meter).await.unwrap_err();
        let got = calls.lock().unwrap().clone();
        assert!(got[0].estimated);
        assert_eq!(got[0].usage.output, 300 / BYTES_PER_OUTPUT_TOKEN);
    }

    #[test]
    fn a_cumulative_output_count_wins_over_a_smaller_estimate() {
        let mut acc = Accumulator::default();
        let mut p = SseParser::default();
        let body = start(5, 0, 0)
            + &text_block("abc")
            + &ev(json!({"type":"message_delta","delta":{},"usage":{"output_tokens":900}}));
        for e in p.push(body.as_bytes()) {
            acc.feed(&e);
        }
        let (u, est) = acc.usage(0);
        assert!(est);
        assert_eq!(u.output, 900);
        assert_eq!(u.uncached_input, 5);
    }

    #[tokio::test]
    async fn every_attempt_is_counted_when_a_broken_reply_is_retried() {
        let ok = start(10, 0, 0) + &text_block("done") + &end(7);
        let (url, hits) = server(vec![
            Reply::Stream(start(10, 0, 0) + &text_block("partial")),
            Reply::Stream(ok),
        ])
        .await;
        let (meter, calls) = UsageMeter::memory("t1");
        let out = call(&url, &[0], &meter).await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(out.retried.len(), 1);
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 2, "both attempts are billed, both are counted");
        assert!(got[0].estimated && !got[1].estimated);
        assert_ne!(got[0].call_id, got[1].call_id);
        assert_eq!(got[1].usage.output, 7);
    }

    #[tokio::test]
    async fn the_deadline_cancels_an_in_flight_call_and_records_it() {
        let (url, _) = server(vec![Reply::Hang(
            start(10, 500, 0) + &text_block("thinking about it"),
        )])
        .await;
        let (meter, calls) = UsageMeter::memory("t1");
        let at = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
        let began = std::time::Instant::now();
        let err = with_deadline(at, call(&url, &[0, 0], &meter))
            .await
            .unwrap_err();
        assert!(began.elapsed() < std::time::Duration::from_secs(5));
        assert!(err.message.contains("time ran out"), "{err}");
        let got = calls.lock().unwrap().clone();
        assert_eq!(
            got.len(),
            1,
            "cancelled once, not retried past the deadline"
        );
        assert!(got[0].estimated);
        assert_eq!(got[0].usage.uncached_input, 10);
        assert_eq!(got[0].usage.cache_read, 500);
        assert_eq!(
            got[0].usage.output,
            "thinking about it".len() as u64 / BYTES_PER_OUTPUT_TOKEN + 1
        );
    }

    #[tokio::test]
    async fn a_call_dropped_by_its_caller_still_records_its_usage() {
        let (url, _) = server(vec![Reply::Hang(start(10, 0, 0) + &text_block("abcdef"))]).await;
        let (meter, calls) = UsageMeter::memory("t1");
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            call(&url, &[], &meter),
        )
        .await;
        assert!(r.is_err(), "the outer timeout dropped the call");
        settle_dropped_calls().await;
        let got = calls.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert!(got[0].estimated);
        assert_eq!(got[0].usage.uncached_input, 10);
        assert_eq!(got[0].usage.output, 2);
    }

    #[tokio::test]
    async fn an_error_event_mid_stream_is_recorded_and_retryable() {
        let body = start(10, 0, 0)
            + &ev(
                json!({"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}),
            );
        let (url, _) = server(vec![Reply::Stream(body)]).await;
        let (meter, calls) = UsageMeter::memory("t1");
        let err = call(&url, &[], &meter).await.unwrap_err();
        assert_eq!(err.kind, ModelErrorKind::Status(529));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_write_goes_to_the_fallback_and_replays_once() {
        let store = Arc::new(MemoryStore::default());
        store.failing.store(true, Ordering::SeqCst);
        let fallback = Arc::new(MemoryFallback::default());
        let meter = UsageMeter::with_store("t1", store.clone())
            .with_fallback(fallback.clone())
            .without_waits();
        let body = start(10, 0, 0) + &text_block("hi") + &end(3);
        let (url, _) = server(vec![Reply::Stream(body)]).await;
        call(&url, &[], &meter).await.unwrap();
        assert!(store.attempts.load(Ordering::SeqCst) >= 3, "retried first");
        assert!(store.calls.lock().unwrap().is_empty());
        let held = fallback.held.lock().unwrap().clone();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].call_id, "req_0");
        // The table is back; the queued message is delivered twice.
        store.failing.store(false, Ordering::SeqCst);
        let first = crate::usage::replay(&*store, &held[0]).await.unwrap();
        let again = crate::usage::replay(&*store, &held[0]).await.unwrap();
        assert_eq!(first, crate::usage::Added::Counted);
        assert_eq!(again, crate::usage::Added::AlreadyCounted);
        let got = store.calls.lock().unwrap().clone();
        assert_eq!(got.len(), 1, "added once");
        assert!(got[0].recovered);
        assert_eq!(got[0].usage.output, 3);
    }

    #[test]
    fn utf8_split_across_chunks_survives() {
        let line = ev(
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"héllo"}}),
        );
        let bytes = line.as_bytes();
        let cut = line.find('é').unwrap() + 1; // inside the two-byte é
        let mut p = SseParser::default();
        assert!(p.push(&bytes[..cut]).is_empty());
        let evs = p.push(&bytes[cut..]);
        assert_eq!(evs[0]["delta"]["text"], "héllo");
    }

    #[test]
    fn tool_use_and_thinking_blocks_are_rebuilt_verbatim() {
        let mut acc = Accumulator::default();
        let body = start(1, 0, 0)
            + &ev(
                json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            )
            + &ev(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hmm"}}),
            )
            + &ev(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}),
            )
            + &ev(json!({"type":"content_block_stop","index":0}))
            + &ev(
                json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tu1","name":"read","input":{}}}),
            )
            + &ev(
                json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"pa"}}),
            )
            + &ev(
                json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"th\":\"a\"}"}}),
            )
            + &ev(json!({"type":"content_block_stop","index":1}))
            + &ev(
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
            );
        for e in SseParser::default().push(body.as_bytes()) {
            acc.feed(&e);
        }
        assert!(acc.finished());
        let m = acc.into_message();
        assert_eq!(
            m["content"][0],
            json!({"type":"thinking","thinking":"hmm","signature":"sig"})
        );
        assert_eq!(m["content"][1]["input"], json!({"path":"a"}));
        assert_eq!(m["stop_reason"], "tool_use");
        assert_eq!(m["id"], "msg_1");
    }
}
