//! Chat's answer loop: ask the model, run the tools it calls, and go round
//! again (bounded) until it answers; then check the answer doesn't hand the
//! person work its tools could have done, and finalize the citations.
//!
//! The budget fits the kind of question ([`limits_for`]): a diagnosis (an
//! alert, an error, an incident) gets more turns under the same ceiling on
//! calls, and its live evidence (the alarm's metric and traffic, the
//! service's logs for the window, its resource metrics) before the code
//! ([`EvidenceFirst`]): a code read past a small allowance, before that
//! evidence is in, is answered with where to look instead of being run. A
//! tool result too long to send whole keeps its head and tail and says what
//! was cut ([`fit_result`]); an answer given over a cut-off result it never
//! narrowed goes back once to read the rest.
//!
//! Two checks run in code before the model's check: an answer that says it
//! found nothing, to a question about the team's work (a ticket, a pull
//! request, a run, "what's the latest"), when it never looked in the
//! tickets, pull requests or runs ([`MustLook`]); and an answer that says it
//! guessed a field name instead of reading the schema. Either is sent back
//! once, like a hand-back.
//!
//! The loop knows three things only through traits, so the same code runs
//! in production and in the Chat evals: the [`Model`] (the metered model
//! calls in `model_call`), the [`Toolbox`] (Chat's tools behind the agent
//! permission gate) and the [`Sink`] (the answer stream).

use common::knowledge::answer::{finalize, finalize_reply, Final, Registry};
use common::model_stream::SseParser;
use common::usage::UsageMeter;
use futures::StreamExt;
use serde_json::{json, Value};
use tracing::{error, info};

pub use common::chat_evidence::{
    evidence_first_note, fit_result, read_the_rest, was_cut, EvidenceFirst,
};

/// Where the answer's events go (`text_delta`, `tool_start`, ...).
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, kind: &str, data: Value);
}

/// One model turn, as it ended.
#[derive(Debug, Clone, Default)]
pub struct Turn {
    pub content: Vec<Value>,
    pub stop: String,
}

/// The model couldn't be reached (already logged).
#[derive(Debug)]
pub struct ModelDown;

#[async_trait::async_trait]
pub trait Model: Send + Sync {
    /// One streamed turn: text deltas and tool starts go to `sink` as they
    /// arrive.
    async fn turn(&self, body: Value, sink: &dyn Sink) -> Result<Turn, ModelDown>;
    /// A small call read whole (the answer check). `None` on any failure.
    async fn ask(&self, body: Value) -> Option<String>;
    /// The model answers are written with.
    fn id(&self) -> &str;
    /// The model small checks use.
    fn check_id(&self) -> &str;
}

/// What one tool call came to.
pub enum Called {
    Done {
        /// What the model reads.
        text: String,
        /// The step's label and summary.
        label: String,
        summary: String,
        is_error: bool,
        /// The source kind (`code`, `knowledge`, ...).
        kind: String,
    },
    /// Stop the answer with this note (agents are paused).
    Stop(String),
}

#[async_trait::async_trait]
pub trait Toolbox: Send {
    fn defs(&self) -> &[Value];
    async fn call(
        &mut self,
        reg: &mut Registry,
        sink: &dyn Sink,
        name: &str,
        input: &Value,
    ) -> Called;
}

/// How far one answer may go. No count decides when an answer stops: it
/// stops when the model answers and the checks in code pass. These are a
/// ceiling on cost and time that a normal question, a diagnosis included,
/// never reaches.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Model turns, at most.
    pub turns: usize,
    /// Tool calls, all turns together, at most.
    pub tool_calls: usize,
    /// Times an answer that handed work back (or left a result cut off) is
    /// sent back to the tools.
    pub retries: usize,
    /// After this long the answer goes on in the background: the asker is
    /// told it is still being checked and the answer is kept as pending, to
    /// be replaced in place when it finishes ([`Background`]).
    pub foreground: std::time::Duration,
    /// The whole answer's time, at most. Under the streaming function's own
    /// limit, so the finished answer is always kept.
    pub ceiling: std::time::Duration,
}

pub const LIMITS: Limits = Limits {
    turns: 60,
    tool_calls: 120,
    retries: 1,
    foreground: std::time::Duration::from_secs(90),
    ceiling: std::time::Duration::from_secs(270),
};

/// A diagnosis: one more send-back covers a result left cut off.
pub const DIAGNOSIS_LIMITS: Limits = Limits {
    retries: 2,
    ..LIMITS
};

/// The budget for a kind of question.
pub fn limits_for(intent: common::chat_intent::Intent) -> Limits {
    match intent {
        common::chat_intent::Intent::Diagnosis => DIAGNOSIS_LIMITS,
        _ => LIMITS,
    }
}

/// Where an answer that outlasts [`Limits::foreground`] goes on: told once,
/// with the holding line the asker sees, so it can keep the answer as
/// pending; the finished answer replaces it in place.
#[async_trait::async_trait]
pub trait Background: Send + Sync {
    async fn started(&self, holding: &str);
}

/// What the asker sees while the answer goes on in the background.
pub const STILL_CHECKING: &str = "Still checking: the lookups that decide this are running. This answer updates here when they finish.";

/// The note sent back when an answer stops short of checks it can still run.
pub const FINISH_THE_CHECKS: &str = "<check>Your answer lists checks you didn't run. There is no lookup limit: run those checks now with your tools (together in one turn when they are independent), then answer from what they show, with citations. Name a check as not done only when it ran and failed, with the reason it gave.</check>";

/// One question for the loop.
pub struct Ask<'a> {
    pub system: String,
    pub messages: Vec<Value>,
    pub question: &'a str,
    pub limits: Limits,
    /// Where this question's answer would be, when the question says so.
    pub must_look: Option<MustLook>,
    /// A reply that cites nothing is kept as written (small talk only).
    pub uncited_ok: bool,
    /// A diagnosis: the live evidence before the code.
    pub evidence: Option<EvidenceFirst>,
    /// Where an answer that outlasts the foreground goes on.
    pub background: Option<&'a dyn Background>,
    /// A diagnosis: an answer that hedges instead of concluding goes back
    /// to run the lookup that settles it.
    pub firm: bool,
}

/// The tools a question's answer lives in (a status question: the tickets,
/// pull requests and runs). An answer that says it found nothing without
/// calling any of them is sent back once; with `always`, any answer that
/// didn't call one is ("what needs me" is only ever answered from its tool).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MustLook {
    pub tools: Vec<String>,
    /// Where to look, in a few words ("the ticket, its pull requests and runs").
    pub what: String,
    /// Send back any answer that didn't look there, found or not.
    pub always: bool,
}

/// The note sent back when an answer found nothing without looking where
/// the answer would be.
pub fn look_there(what: &str, tools: &[String]) -> String {
    format!(
        "<check>Your answer says nothing was found, but you didn't look where the answer would \
         be: {what}. Look there now with {}, then answer with citations. If those don't have it \
         either, say which places you searched.</check>",
        tools.join(", ")
    )
}

/// The note sent back when an answer didn't read the one place its answer
/// comes from.
pub fn answer_from(what: &str, tools: &[String]) -> String {
    format!(
        "<check>This question is answered from {what}, and you didn't look there. Call {} now, \
         then answer the question that was asked, with citations.</check>",
        tools.join(", ")
    )
}

/// The note sent back when an answer cites nothing it read this turn.
pub fn cite_something(defs: &[Value]) -> String {
    let setup = if defs.iter().any(|d| d["name"] == super::chat_setup::SETUP) {
        " If the question is about Coderhelm itself (what it does, what is connected, what you can do), call coderhelm_setup and cite it."
    } else {
        ""
    };
    format!(
        "<check>Your answer cites nothing read in this answer, so it can't be shown as written. \
         Look up what the answer rests on with your tools and cite it.{setup} Then answer the \
         question that was asked, first, sized to it. If nothing your tools reach holds the \
         answer, say exactly where you looked.</check>"
    )
}

/// The note sent back when an answer guessed a field or type name.
pub const READ_THE_SCHEMA: &str = "<check>Your answer says it guessed a field, column or type \
name. Don't guess: read the source's schema first (its describe, schema or mapping tool), then \
query with the names it lists, and answer with citations.</check>";

/// The checks made in code: `Some(note)` when the answer must look again.
pub fn checked_in_code(
    answer: &str,
    must_look: Option<&MustLook>,
    calls: &[String],
    defs: &[Value],
) -> Option<String> {
    use super::chat_hints::{says_it_guessed, says_nothing_found};
    if let Some(m) = must_look {
        let offered: Vec<String> = m
            .tools
            .iter()
            .filter(|t| defs.iter().any(|d| d["name"] == t.as_str()))
            .cloned()
            .collect();
        let looked = calls.iter().any(|c| m.tools.contains(c));
        if !offered.is_empty() && !looked && m.always {
            return Some(answer_from(&m.what, &offered));
        }
        if !offered.is_empty() && !looked && says_nothing_found(answer) {
            return Some(look_there(&m.what, &offered));
        }
    }
    says_it_guessed(answer).then(|| READ_THE_SCHEMA.to_string())
}

/// What the loop gives back.
pub struct Answered {
    pub fin: Final,
    /// The steps taken (`{kind, label}`), as kept on the message.
    pub used: Vec<Value>,
    /// The answer was sent back once because it handed work to the person.
    pub rechecked: bool,
    /// Every tool call made, by name.
    pub calls: Vec<String>,
}

/// What the model is told on the turn it must answer: only at the ceiling
/// on cost and time, which a normal question never reaches.
pub const ANSWER_NOW: &str =
    "Answer now, from what you read, with citations: lead with the conclusion the evidence supports. For a check that ran and failed, name it with the reason it gave.";

/// The note added when an answer handed work back.
pub fn nudge(what: &str) -> String {
    format!(
        "<check>Your answer asks the person to do something your tools can do ({what}). \
         Don't hand it back. Use your tools now (look the repository over, list and read the \
         files, search the code, search knowledge) until you can answer from what you read, \
         then answer with citations. Only say something isn't there after you looked.</check>"
    )
}

const CHECK_SYSTEM: &str = "You check answers written by a team's Chat assistant before the person sees them. \
The assistant had the tools listed below. Decide whether the answer hands the person work the assistant could \
have done itself with those tools: for example asking them to open, read, paste or attach a file or README, \
to look something up in the code or the docs, to say which repository or who owns it, or to search somewhere \
the tools reach. Asking the person to make a decision, give a preference, or provide information no tool can \
reach is fine, and so is suggesting a next step after a complete answer. Reply with JSON only: \
{\"hands_back\": true or false, \"what\": \"the work handed back, in a few words\"}.";

/// The body of the answer check.
pub fn check_body(model_id: &str, question: &str, answer: &str, tools: &[Value]) -> Value {
    let list: Vec<String> = tools
        .iter()
        .filter_map(|t| {
            let name = t["name"].as_str()?;
            let about = t["description"].as_str().unwrap_or("");
            Some(format!("- {name}: {}", common::truncate_str(about, 160)))
        })
        .collect();
    json!({
        "model": model_id,
        "max_tokens": 200,
        "system": format!("{CHECK_SYSTEM}\n\nTools:\n{}", list.join("\n")),
        "messages": [{"role": "user", "content": [{"type": "text", "text":
            format!("Question:\n{question}\n\nAnswer:\n{answer}")
        }]}],
    })
}

/// The check's verdict: `Some(what)` when the answer handed work back.
pub fn read_check(reply: &str) -> Option<String> {
    let start = reply.find('{')?;
    let end = reply.rfind('}')?;
    let v: Value = serde_json::from_str(reply.get(start..=end)?).ok()?;
    if v["hands_back"] == true {
        let what = v["what"].as_str().unwrap_or("").trim();
        Some(if what.is_empty() {
            "work the tools can do".to_string()
        } else {
            common::truncate_str(what, 200).to_string()
        })
    } else {
        None
    }
}

fn text_of(content: &[Value]) -> String {
    content
        .iter()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect()
}

/// Asks, runs tools and checks until there's an answer. `Err` only when the
/// model couldn't be reached (the error event was sent).
pub async fn run(
    model: &dyn Model,
    tools: &mut dyn Toolbox,
    sink: &dyn Sink,
    reg: &mut Registry,
    ask: Ask<'_>,
) -> Result<Answered, ModelDown> {
    let Ask {
        system,
        mut messages,
        question,
        limits,
        must_look,
        uncited_ok,
        evidence,
        background,
        firm,
    } = ask;
    let mut firmed = 0;
    let started = std::time::Instant::now();
    let mut handed_back = false;
    let mut used: Vec<Value> = Vec::new();
    let mut calls: Vec<String> = Vec::new();
    // A result that came back cut off and no later call of the same tool
    // narrowed: `(tool, label)`.
    let mut cut: Vec<(String, String)> = Vec::new();
    let mut last_text = String::new();
    let mut retries = 0;
    let has_tools = !tools.defs().is_empty();

    for turn in 0..limits.turns {
        let elapsed = started.elapsed();
        // Past the foreground, the answer goes on in the background: the
        // asker is told once and the answer is kept as pending.
        if turn > 0 && !handed_back && elapsed >= limits.foreground {
            if let Some(b) = background {
                handed_back = true;
                info!("Chat: the answer goes on in the background");
                sink.send("pending", json!({ "text": STILL_CHECKING }))
                    .await;
                b.started(STILL_CHECKING).await;
            }
        }
        // Only at the ceiling on cost and time must the turn answer. The
        // tools stay defined (the history holds tool calls), and the model
        // is told to answer now; a tool call on this turn is not run.
        let must_answer = turn + 1 == limits.turns
            || calls.len() >= limits.tool_calls
            || elapsed >= limits.ceiling;
        if must_answer && turn > 0 {
            if let Some(content) = messages
                .last_mut()
                .filter(|m| m["role"] == "user")
                .and_then(|m| m["content"].as_array_mut())
            {
                content.push(json!({"type": "text", "text": ANSWER_NOW}));
            }
        }
        let mut body = json!({
            "model": model.id(),
            "max_tokens": 8192,
            "stream": true,
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
        });
        if common::is_always_thinking_model(model.id()) {
            body["output_config"] = json!({"effort": "medium"});
        }
        if has_tools {
            body["tools"] = json!(tools.defs());
        }
        let t = model.turn(body, sink).await?;
        let text = text_of(&t.content);
        if !text.trim().is_empty() {
            last_text = text.clone();
        }
        if !t.content.is_empty() {
            messages.push(json!({ "role": "assistant", "content": t.content }));
        }
        let tool_uses: Vec<Value> = t
            .content
            .iter()
            .filter(|b| b["type"] == "tool_use")
            .cloned()
            .collect();
        if t.stop != "tool_use" || tool_uses.is_empty() || must_answer {
            // An answer that stops short of checks it can still run goes
            // back to run them: no count ends an answer early.
            if has_tools && !must_answer && super::chat_hints::says_unfinished(&text) {
                info!("Chat: the answer stopped short, it runs the checks it listed");
                messages.push(
                    json!({ "role": "user", "content": [{"type": "text", "text": FINISH_THE_CHECKS}] }),
                );
                sink.send("text_reset", json!({})).await;
                continue;
            }
            // A diagnosis that hedges runs the lookup that settles it.
            if has_tools
                && firm
                && !must_answer
                && firmed < 2
                && common::chat_evidence::says_hedged(&text)
            {
                firmed += 1;
                info!("Chat: the diagnosis hedged, it runs the lookup that settles it");
                messages.push(json!({ "role": "user", "content": [{"type": "text", "text": common::chat_evidence::FIRM_UP}] }));
                sink.send("text_reset", json!({})).await;
                continue;
            }
            // An answer. Check it doesn't hand back work the tools can do.
            if has_tools && retries < limits.retries && !must_answer && !text.trim().is_empty() {
                // In code first; the model's check only when that passes;
                // then an answer that cites nothing (it would be replaced by
                // the plain no-source line) looks once with its tools.
                let cited = uncited_ok || finalize(&text, reg).grounded;
                let left_cut = cut.first().map(|(_, label)| read_the_rest(label));
                let in_code = checked_in_code(&text, must_look.as_ref(), &calls, tools.defs());
                let note = match in_code.or(left_cut) {
                    Some(note) => Some(note),
                    None => model
                        .ask(check_body(model.check_id(), question, &text, tools.defs()))
                        .await
                        .and_then(|r| read_check(&r))
                        .map(|what| nudge(&what))
                        .or_else(|| (!cited).then(|| cite_something(tools.defs()))),
                };
                if let Some(note) = note {
                    retries += 1;
                    info!(note = %common::truncate_str(&note, 120), "Chat: the answer looks again");
                    messages.push(
                        json!({ "role": "user", "content": [{"type": "text", "text": note}] }),
                    );
                    sink.send("text_reset", json!({})).await;
                    sink.send(
                        "step",
                        json!({ "id": format!("check-{retries}"), "label": "Looked further before answering", "status": "success" }),
                    )
                    .await;
                    continue;
                }
            }
            break;
        }
        let mut results = Vec::new();
        for tu in &tool_uses {
            let name = tu["name"].as_str().unwrap_or("");
            let input = &tu["input"];
            let held = evidence.as_ref().is_some_and(|e| e.holds(name, &calls));
            calls.push(name.to_string());
            // Another call of the same tool follows a cut-off result up.
            cut.retain(|(t, _)| t != name);
            let before = reg.len();
            let (text, label, summary, is_error, kind) = if calls.len() > limits.tool_calls {
                (
                    "Not run: this answer is at its ceiling on cost and time. Answer from what you read."
                        .to_string(),
                    name.replace('_', " "),
                    "Skipped".to_string(),
                    true,
                    String::new(),
                )
            } else if let Some(e) = evidence.as_ref().filter(|_| held) {
                (
                    evidence_first_note(&e.live),
                    name.replace('_', " "),
                    "Live evidence first".to_string(),
                    true,
                    String::new(),
                )
            } else {
                match tools.call(reg, sink, name, input).await {
                    Called::Done {
                        text,
                        label,
                        summary,
                        is_error,
                        kind,
                    } => (text, label, summary, is_error, kind),
                    Called::Stop(note) => {
                        sink.send("text_delta", json!({ "text": note })).await;
                        return Ok(Answered {
                            fin: finalize_reply(&note, reg, true),
                            used,
                            rechecked: retries > 0,
                            calls,
                        });
                    }
                }
            };
            if !is_error && was_cut(&text) {
                cut.push((name.to_string(), label.clone()));
            }
            // What this call registered, so the answer's citations show as
            // it streams.
            let found: Vec<_> = (before + 1..=reg.len())
                .filter_map(|n| reg.get(&format!("S{n}")).cloned())
                .collect();
            let repo = input["repo"]
                .as_str()
                .map(|r| r.rsplit('/').next().unwrap_or(r).trim())
                .unwrap_or("");
            sink.send(
                "tool_result",
                json!({
                    "id": tu["id"], "name": name, "label": label, "kind": kind, "repo": repo,
                    "status": if is_error { "error" } else { "success" }, "summary": summary,
                    "sources": found,
                }),
            )
            .await;
            let mut step = json!({ "kind": kind, "label": label, "name": name, "repo": repo, "summary": summary });
            // A saved answer shows its failed lookups the way the live one did.
            if is_error {
                step["status"] = json!("error");
            }
            used.push(step);
            let mut r = json!({ "type": "tool_result", "tool_use_id": tu["id"], "content": text });
            if is_error {
                r["is_error"] = json!(true);
            }
            results.push(r);
        }
        messages.push(json!({ "role": "user", "content": results }));
        // The next turn's text replaces what was said before the tools.
        sink.send("text_reset", json!({})).await;
    }
    Ok(Answered {
        fin: finalize_reply(&last_text, reg, uncited_ok),
        used,
        rechecked: retries > 0,
        calls,
    })
}

// ── The production model ────────────────────────────────────────────────────

/// The team's model, every call metered under feature `brain`.
pub struct ApiModel {
    pub http: reqwest::Client,
    pub api_key: String,
    pub meter: UsageMeter,
    pub model_id: String,
    pub check_model_id: String,
    /// Tells the dashboard which kind of source a tool reads.
    pub kind_of: fn(&str) -> &'static str,
}

#[async_trait::async_trait]
impl Model for ApiModel {
    async fn turn(&self, body: Value, sink: &dyn Sink) -> Result<Turn, ModelDown> {
        let (mut stream, mut flight) = match crate::model_call::open_brain_stream(
            &self.http,
            &self.api_key,
            &body,
            &self.meter,
            &self.model_id,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(transient = e.is_transient(), "Chat model call failed: {e}");
                sink.send(
                    "error",
                    json!({ "message": "The model couldn't be reached. Try again in a moment." }),
                )
                .await;
                return Err(ModelDown);
            }
        };
        let mut parser = SseParser::default();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            for ev in parser.push(&chunk) {
                flight.acc.feed(&ev);
                match ev["type"].as_str() {
                    Some("content_block_delta") if ev["delta"]["type"] == "text_delta" => {
                        if let Some(t) = ev["delta"]["text"].as_str() {
                            sink.send("text_delta", json!({ "text": t })).await;
                        }
                    }
                    Some("content_block_start") if ev["content_block"]["type"] == "tool_use" => {
                        let name = ev["content_block"]["name"].as_str().unwrap_or("");
                        sink.send(
                            "tool_start",
                            json!({ "id": ev["content_block"]["id"], "name": name, "kind": (self.kind_of)(name) }),
                        )
                        .await;
                    }
                    _ => {}
                }
            }
        }
        flight.finish().await;
        let message = std::mem::take(&mut flight.acc).into_message();
        Ok(Turn {
            content: message["content"].as_array().cloned().unwrap_or_default(),
            stop: message["stop_reason"].as_str().unwrap_or("").to_string(),
        })
    }

    async fn ask(&self, body: Value) -> Option<String> {
        let m = crate::model_call::brain_turn(
            &self.http,
            &self.api_key,
            &body,
            &self.meter,
            &self.check_model_id,
        )
        .await
        .inspect_err(|e| tracing::warn!("Chat answer check failed: {e}"))
        .ok()?;
        Some(text_of(
            m["content"].as_array().map(Vec::as_slice).unwrap_or(&[]),
        ))
    }

    fn id(&self) -> &str {
        &self.model_id
    }

    fn check_id(&self) -> &str {
        &self.check_model_id
    }
}

/// The answer stream as a [`Sink`].
pub struct ChannelSink(
    pub tokio::sync::mpsc::Sender<Result<axum::response::sse::Event, std::convert::Infallible>>,
);

#[async_trait::async_trait]
impl Sink for ChannelSink {
    async fn send(&self, kind: &str, data: Value) {
        let _ = self.0.send(super::plans::sse_event(kind, data)).await;
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! A scripted model, a recording sink, and Chat's code tools over an
    //! in-memory repository: what the evals run the real loop with.

    use super::*;
    use crate::routes::chat_code;
    use common::repo_read::{CodeReader, RepoInfo};
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct Recorder(pub Mutex<Vec<(String, Value)>>);

    #[async_trait::async_trait]
    impl Sink for Recorder {
        async fn send(&self, kind: &str, data: Value) {
            self.0.lock().unwrap().push((kind.to_string(), data));
        }
    }

    impl Recorder {
        pub fn kinds(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|(k, _)| k.clone())
                .collect()
        }
    }

    /// A model that plays back turns. A turn is a function of the request,
    /// so it can answer from what the tools returned.
    pub type Step = Box<dyn Fn(&Value) -> Turn + Send + Sync>;

    pub struct Scripted {
        pub steps: Mutex<std::collections::VecDeque<Step>>,
        pub checks: Mutex<std::collections::VecDeque<String>>,
        pub bodies: Mutex<Vec<Value>>,
    }

    impl Scripted {
        pub fn new(steps: Vec<Step>, checks: Vec<&str>) -> Self {
            Scripted {
                steps: Mutex::new(steps.into()),
                checks: Mutex::new(checks.into_iter().map(str::to_string).collect()),
                bodies: Mutex::new(Vec::new()),
            }
        }
    }

    pub fn calls(id: &str, name: &str, input: Value) -> Turn {
        Turn {
            content: vec![json!({"type": "tool_use", "id": id, "name": name, "input": input})],
            stop: "tool_use".into(),
        }
    }

    pub fn say(text: &str) -> Turn {
        Turn {
            content: vec![json!({"type": "text", "text": text})],
            stop: "end_turn".into(),
        }
    }

    #[async_trait::async_trait]
    impl Model for Scripted {
        async fn turn(&self, body: Value, sink: &dyn Sink) -> Result<Turn, ModelDown> {
            self.bodies.lock().unwrap().push(body.clone());
            let step = self.steps.lock().unwrap().pop_front();
            let t = step.map(|s| s(&body)).unwrap_or_else(|| say(""));
            for b in &t.content {
                if b["type"] == "text" {
                    sink.send("text_delta", json!({ "text": b["text"] })).await;
                }
            }
            Ok(t)
        }
        async fn ask(&self, _body: Value) -> Option<String> {
            self.checks.lock().unwrap().pop_front()
        }
        fn id(&self) -> &str {
            "claude-test"
        }
        fn check_id(&self) -> &str {
            "claude-test-small"
        }
    }

    /// Chat's code tools over a reader, as a toolbox.
    pub struct CodeBox<'a> {
        pub reader: CodeReader<'a>,
        pub defs: Vec<Value>,
    }

    impl<'a> CodeBox<'a> {
        pub fn new(host: &'a dyn common::repo_read::RepoHost, repos: Vec<RepoInfo>) -> Self {
            let defs = chat_code::defs(&repos);
            CodeBox {
                reader: CodeReader::new(host, repos),
                defs,
            }
        }
    }

    #[async_trait::async_trait]
    impl Toolbox for CodeBox<'_> {
        fn defs(&self) -> &[Value] {
            &self.defs
        }
        async fn call(
            &mut self,
            reg: &mut Registry,
            _sink: &dyn Sink,
            name: &str,
            input: &Value,
        ) -> Called {
            // The evals read code through the same layer Chat and /mcp do.
            let (text, summary, is_error) =
                crate::lookups::code(&self.reader, reg, name, input).await;
            Called::Done {
                text,
                label: chat_code::label(name, input),
                summary,
                is_error,
                kind: "code".into(),
            }
        }
    }

    /// The last tool result the model was sent, as text.
    pub fn last_result(body: &Value) -> String {
        body["messages"]
            .as_array()
            .and_then(|m| m.last())
            .and_then(|m| m["content"].as_array())
            .map(|c| {
                c.iter()
                    .filter_map(|b| b["content"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default()
    }

    /// The source ids (`S3`) in a block of rendered sources whose title is `path`.
    pub fn id_for(text: &str, path: &str) -> Option<String> {
        let needle = format!("title=\"{path}\"");
        text.lines().find(|l| l.contains(&needle)).and_then(|l| {
            let at = l.find("id=\"")? + 4;
            Some(l[at..].split('"').next()?.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::routes::chat_code::{OVERVIEW, READ_FILE, SEARCH_CODE};
    use common::repo_read::{MemHost, RepoInfo};
    use std::sync::Mutex;

    fn host() -> MemHost {
        MemHost::default().with_repo(
            "acme/shop-web",
            "c0ffee12",
            &[
                ("README.md", "# Shop web\nThe storefront, a Next.js app.\n"),
                (
                    "package.json",
                    "{\"name\":\"shop-web\",\"dependencies\":{\"next\":\"15\"}}",
                ),
                (
                    "src/pay.ts",
                    "export function pay() {\n  return charge();\n}\n",
                ),
            ],
        )
    }

    fn ask(q: &str) -> Ask<'_> {
        Ask {
            system: "test".into(),
            messages: vec![json!({"role": "user", "content": [{"type": "text", "text": q}]})],
            question: q,
            limits: LIMITS,
            must_look: None,
            uncited_ok: false,
            evidence: None,
            background: None,
            firm: false,
        }
    }

    #[tokio::test]
    async fn tools_run_over_several_turns_until_it_answers() {
        let h = host();
        let mut tb = CodeBox::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("t1", OVERVIEW, json!({"repo": "shop-web"}))),
                Box::new(|_| {
                    calls(
                        "t2",
                        READ_FILE,
                        json!({"repo": "shop-web", "path": "src/pay.ts"}),
                    )
                }),
                Box::new(|b| {
                    let id = id_for(&last_result(b), "src/pay.ts").unwrap();
                    say(&format!("Payments start in `pay` [{id}]."))
                }),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(
            &model,
            &mut tb,
            &sink,
            &mut reg,
            ask("Where is paying handled?"),
        )
        .await
        .unwrap();
        assert!(out.fin.grounded);
        assert_eq!(out.calls, [OVERVIEW, READ_FILE]);
        assert_eq!(out.fin.citations[0].title, "src/pay.ts");
        assert!(sink.kinds().iter().filter(|k| *k == "tool_result").count() == 2);
        assert!(!out.rechecked);
    }

    #[tokio::test]
    async fn an_answer_that_hands_work_back_is_sent_back_to_the_tools() {
        let h = host();
        let mut tb = CodeBox::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let model = Scripted::new(
            vec![
                Box::new(|_| {
                    say("I couldn't find it. Open src/pay.ts yourself and tell me what it says.")
                }),
                Box::new(|_| {
                    calls(
                        "t1",
                        READ_FILE,
                        json!({"repo": "shop-web", "path": "src/pay.ts"}),
                    )
                }),
                Box::new(|b| {
                    let id = id_for(&last_result(b), "src/pay.ts").unwrap();
                    say(&format!("`pay` calls `charge` [{id}]."))
                }),
            ],
            vec![
                r#"{"hands_back": true, "what": "open a file"}"#,
                r#"{"hands_back": false}"#,
            ],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(&model, &mut tb, &sink, &mut reg, ask("What does pay do?"))
            .await
            .unwrap();
        assert!(out.rechecked);
        assert!(out.fin.text.contains("charge"), "{}", out.fin.text);
        let bodies = model.bodies.lock().unwrap();
        let nudged = bodies[1]["messages"].to_string();
        assert!(nudged.contains("open a file") && nudged.contains("<check>"));
        assert!(sink.kinds().contains(&"text_reset".to_string()));
    }

    #[tokio::test]
    async fn the_check_runs_once_and_never_loops() {
        let h = host();
        let mut tb = CodeBox::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let model = Scripted::new(
            vec![
                Box::new(|_| say("Open it yourself.")),
                Box::new(|_| say("Still, open it yourself.")),
            ],
            vec![
                r#"{"hands_back": true, "what": "open a file"}"#,
                r#"{"hands_back": true}"#,
            ],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(&model, &mut tb, &sink, &mut reg, ask("q"))
            .await
            .unwrap();
        assert_eq!(model.bodies.lock().unwrap().len(), 2);
        // Nothing cited: the plain no-source answer, never the hand-back.
        assert!(!out.fin.grounded);
    }

    #[tokio::test]
    async fn the_budget_ends_with_an_answer_and_no_more_tools() {
        let h = host();
        let mut tb = CodeBox::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let steps: Vec<Step> = (0..20)
            .map(|i| -> Step {
                Box::new(move |_| calls(&format!("t{i}"), SEARCH_CODE, json!({"query": "pay"})))
            })
            .collect();
        let model = Scripted::new(steps, vec![]);
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let limits = Limits { turns: 4, ..LIMITS };
        let a = Ask { limits, ..ask("q") };
        run(&model, &mut tb, &sink, &mut reg, a).await.unwrap();
        let bodies = model.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 4);
        // The last turn is told to answer; the tools stay defined for the history.
        assert!(bodies[3]["messages"].to_string().contains(ANSWER_NOW));
        assert!(bodies[3]["tools"].is_array());
        assert!(!bodies[2]["messages"].to_string().contains(ANSWER_NOW));
    }

    // ── A diagnosis finishes ────────────────────────────────────────────────

    const METRICS: &str = "metrics_query";
    const LOGS: &str = "logs_search";

    /// A made-up service's live tools and its code: metrics, logs, and a
    /// code search. A metrics read asked with `"big": true` comes back long.
    struct DiagBox {
        defs: Vec<Value>,
    }

    impl DiagBox {
        fn new() -> Self {
            let def =
                |n: &str| json!({"name": n, "description": n, "input_schema": {"type": "object"}});
            DiagBox {
                defs: vec![def(METRICS), def(LOGS), def(SEARCH_CODE)],
            }
        }
    }

    #[async_trait::async_trait]
    impl Toolbox for DiagBox {
        fn defs(&self) -> &[Value] {
            &self.defs
        }
        async fn call(
            &mut self,
            reg: &mut Registry,
            _sink: &dyn Sink,
            name: &str,
            input: &Value,
        ) -> Called {
            let body = match name {
                METRICS if input["big"] == true => {
                    format!("p99 by minute: {}", "02:41 3.9s 104 req; ".repeat(1_000))
                }
                METRICS => format!("quayside-api p99 {} (sample)", input["what"]),
                LOGS => "02:41 GET /v1/tides/forecast 3.8s upstream".to_string(),
                _ => "alarms.tf: p99 > 2s for 3 of 3 periods".to_string(),
            };
            let id = reg.add(common::knowledge::answer::SourceRef {
                kind: "data_source".into(),
                title: format!("{name} {}", input["what"]),
                passage: body.clone(),
                ..Default::default()
            });
            let text = fit_result(&reg.render_with(&id, &body));
            Called::Done {
                text,
                label: name.replace('_', " "),
                summary: "Read".into(),
                is_error: false,
                kind: "data_sources".into(),
            }
        }
    }

    fn diagnosis(q: &str) -> Ask<'_> {
        Ask {
            evidence: EvidenceFirst::over(vec![METRICS.into(), LOGS.into()]),
            firm: true,
            limits: DIAGNOSIS_LIMITS,
            ..ask(q)
        }
    }

    /// The case that went wrong: an alert that needs more lookups than the
    /// old fixed count (12 turns) still ends with a conclusion, and the
    /// model is never told to stop.
    #[tokio::test]
    async fn an_alert_diagnosis_past_the_old_count_finishes_with_a_conclusion() {
        let mut tb = DiagBox::new();
        let mut steps: Vec<Step> = (0..16)
            .map(|i| -> Step {
                Box::new(move |_| {
                    calls(
                        &format!("m{i}"),
                        METRICS,
                        json!({"what": format!("window {i}")}),
                    )
                })
            })
            .collect();
        steps.push(Box::new(|_| {
            say("One path is slow all day and the alarm fires only when traffic is low [S1, S16].")
        }));
        let model = Scripted::new(steps, vec![r#"{"hands_back": false}"#]);
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(
            &model,
            &mut tb,
            &sink,
            &mut reg,
            diagnosis("why is the p99 alarm firing?"),
        )
        .await
        .unwrap();
        assert_eq!(out.calls.len(), 16);
        assert!(
            out.fin.text.starts_with("One path is slow all day"),
            "{}",
            out.fin.text
        );
        let bodies = model.bodies.lock().unwrap();
        assert!(bodies
            .iter()
            .all(|b| !b["messages"].to_string().contains(ANSWER_NOW)));
    }

    /// No answer lists checks it didn't run while it could still run them:
    /// "Not checked (I ran out of lookups)" goes back to run them.
    #[tokio::test]
    async fn an_answer_that_stops_short_runs_the_checks_it_listed() {
        let mut tb = DiagBox::new();
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("m", METRICS, json!({"what": "p99 and requests"}))),
                Box::new(|_| {
                    say("Likely a slow upstream [S1].\n\n**Not checked (I ran out of lookups)**\n- the service logs around 02:40")
                }),
                Box::new(|b| {
                    assert!(b["messages"]
                        .to_string()
                        .contains("There is no lookup limit"));
                    calls("l", LOGS, json!({"what": "02:35-02:50"}))
                }),
                Box::new(|_| {
                    say("The forecast call is slow at 02:41 [S2], and traffic is low then [S1].")
                }),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let a = Ask {
            limits: Limits {
                retries: 0,
                ..LIMITS
            },
            ..diagnosis("why did it fire?")
        };
        let out = run(&model, &mut tb, &sink, &mut reg, a).await.unwrap();
        assert_eq!(out.calls, [METRICS, LOGS]);
        assert!(
            !common::chat_evidence::says_unfinished(&out.fin.text),
            "{}",
            out.fin.text
        );
        assert!(out.fin.text.starts_with("The forecast call is slow"));
    }

    /// A diagnosis reads its live evidence before more code: the third code
    /// read waits, and is told where to look.
    #[tokio::test]
    async fn a_diagnosis_reads_the_live_evidence_before_more_code() {
        let mut tb = DiagBox::new();
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("c1", SEARCH_CODE, json!({"query": "alarm"}))),
                Box::new(|_| calls("c2", SEARCH_CODE, json!({"query": "p99"}))),
                Box::new(|_| calls("c3", SEARCH_CODE, json!({"query": "threshold"}))),
                Box::new(|b| {
                    let r = last_result(b);
                    assert!(
                        r.contains("Not run: this is a diagnosis") && r.contains(METRICS),
                        "{r}"
                    );
                    calls("m", METRICS, json!({"what": "p99"}))
                }),
                Box::new(|_| say("The p99 crossed 2 s at 02:41 on 104 requests [S3].")),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(
            &model,
            &mut tb,
            &sink,
            &mut reg,
            diagnosis("why did the alarm fire?"),
        )
        .await
        .unwrap();
        assert_eq!(out.calls, [SEARCH_CODE, SEARCH_CODE, SEARCH_CODE, METRICS]);
        assert_eq!(out.used[2]["summary"], "Live evidence first");
    }

    /// A cut-off result is narrowed and read, never answered from in part.
    #[tokio::test]
    async fn a_cut_off_result_is_narrowed_before_the_answer() {
        let mut tb = DiagBox::new();
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("m1", METRICS, json!({"what": "all night", "big": true}))),
                Box::new(|b| {
                    assert!(last_result(b).contains("[cut off:"));
                    say("The p99 was high overnight [S1].")
                }),
                Box::new(|b| {
                    assert!(b["messages"].to_string().contains("was cut off"));
                    calls("m2", METRICS, json!({"what": "02:35-02:50"}))
                }),
                Box::new(|_| say("The p99 crossed 2 s at 02:41 on 104 requests [S2].")),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(
            &model,
            &mut tb,
            &sink,
            &mut reg,
            diagnosis("why did the alarm fire?"),
        )
        .await
        .unwrap();
        assert_eq!(out.calls, [METRICS, METRICS]);
        assert!(out.rechecked);
        assert!(out.fin.text.contains("02:41"));
    }

    /// A diagnosis that hedges runs the lookup that settles it.
    #[tokio::test]
    async fn a_hedged_diagnosis_runs_the_lookup_that_settles_it() {
        let mut tb = DiagBox::new();
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("m1", METRICS, json!({"what": "p99"}))),
                Box::new(|_| say("Probably one or two slow requests at night [S1].")),
                Box::new(|b| {
                    assert!(b["messages"]
                        .to_string()
                        .contains("break the metric down by path"));
                    calls("m2", METRICS, json!({"what": "by path"}))
                }),
                Box::new(|_| say("GET /v1/tides/forecast is slow all day [S2].")),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let out = run(
            &model,
            &mut tb,
            &sink,
            &mut reg,
            diagnosis("why did the alarm fire?"),
        )
        .await
        .unwrap();
        assert_eq!(out.calls, [METRICS, METRICS]);
        assert!(!common::chat_evidence::says_hedged(&out.fin.text));
    }

    #[derive(Default)]
    struct Kept(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl Background for Kept {
        async fn started(&self, holding: &str) {
            self.0.lock().unwrap().push(holding.to_string());
        }
    }

    /// Past the foreground the answer goes on: the asker is told once, the
    /// answer is kept as pending, and it still finishes with the conclusion.
    #[tokio::test]
    async fn past_the_foreground_the_answer_goes_on_and_finishes() {
        let mut tb = DiagBox::new();
        let model = Scripted::new(
            vec![
                Box::new(|_| calls("m1", METRICS, json!({"what": "p99"}))),
                Box::new(|_| calls("m2", METRICS, json!({"what": "requests"}))),
                Box::new(|_| calls("l", LOGS, json!({"what": "window"}))),
                Box::new(|_| {
                    say("The forecast path is slow all day; at night it sets the p99 [S1, S2, S3].")
                }),
            ],
            vec![r#"{"hands_back": false}"#],
        );
        let sink = Recorder::default();
        let mut reg = Registry::default();
        let kept = Kept::default();
        let a = Ask {
            limits: Limits {
                foreground: std::time::Duration::ZERO,
                ..DIAGNOSIS_LIMITS
            },
            background: Some(&kept),
            ..diagnosis("why did the alarm fire?")
        };
        let out = run(&model, &mut tb, &sink, &mut reg, a).await.unwrap();
        assert_eq!(*kept.0.lock().unwrap(), [STILL_CHECKING]);
        assert_eq!(sink.kinds().iter().filter(|k| *k == "pending").count(), 1);
        assert_eq!(out.calls, [METRICS, METRICS, LOGS]);
        assert!(out
            .fin
            .text
            .starts_with("The forecast path is slow all day"));
    }

    #[test]
    fn no_count_ends_an_answer_and_a_diagnosis_has_room_to_finish() {
        use common::chat_intent::Intent;
        // The ceiling is far past what a diagnosis takes, and the same for
        // every kind of question; a diagnosis gets one more send-back.
        assert!(LIMITS.turns >= 50 && LIMITS.tool_calls >= 100);
        assert!(LIMITS.ceiling > LIMITS.foreground);
        // Under the streaming function's five minutes, so the answer is kept.
        assert!(LIMITS.ceiling < std::time::Duration::from_secs(300));
        assert_eq!(limits_for(Intent::Diagnosis).retries, 2);
        assert_eq!(limits_for(Intent::Lookup).turns, LIMITS.turns);
        assert!(!ANSWER_NOW.contains("didn't get to"));
        assert!(!ANSWER_NOW.to_lowercase().contains("lookups"));
    }

    #[test]
    fn the_check_reads_json_even_with_words_around_it() {
        assert_eq!(
            read_check("Sure: {\"hands_back\": true, \"what\": \"paste the README\"}"),
            Some("paste the README".into())
        );
        assert_eq!(read_check("{\"hands_back\": false}"), None);
        assert_eq!(read_check("not json"), None);
        let b = check_body(
            "m",
            "q?",
            "a.",
            &[json!({"name": "github__read_file", "description": "Read a file"})],
        );
        assert!(b["system"]
            .as_str()
            .unwrap()
            .contains("github__read_file: Read a file"));
    }
}
