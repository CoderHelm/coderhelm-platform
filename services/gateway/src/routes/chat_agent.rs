//! Chat's answer loop: ask the model, run the tools it calls, and go round
//! again (bounded) until it answers; then check the answer doesn't hand the
//! person work its tools could have done, and finalize the citations.
//!
//! The loop knows three things only through traits, so the same code runs
//! in production and in the Chat evals: the [`Model`] (the metered model
//! calls in `model_call`), the [`Toolbox`] (Chat's tools behind the agent
//! permission gate) and the [`Sink`] (the answer stream).

use common::knowledge::answer::{finalize, Final, Registry};
use common::model_stream::SseParser;
use common::usage::UsageMeter;
use futures::StreamExt;
use serde_json::{json, Value};
use tracing::{error, info};

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

/// How far one answer may go.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Model turns.
    pub turns: usize,
    /// Tool calls, all turns together.
    pub tool_calls: usize,
    /// Times an answer that handed work back is sent back to the tools.
    pub retries: usize,
}

pub const LIMITS: Limits = Limits {
    turns: 12,
    tool_calls: 30,
    retries: 1,
};

/// One question for the loop.
pub struct Ask<'a> {
    pub system: String,
    pub messages: Vec<Value>,
    pub question: &'a str,
    pub limits: Limits,
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

/// What the model is told on the turn it must answer.
pub const ANSWER_NOW: &str =
    "You have used all the lookups this answer may make. Answer now from what you read, with citations, and say what you didn't get to check.";

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
    } = ask;
    let mut used: Vec<Value> = Vec::new();
    let mut calls: Vec<String> = Vec::new();
    let mut last_text = String::new();
    let mut retries = 0;
    let has_tools = !tools.defs().is_empty();

    for turn in 0..limits.turns {
        // The last turn (or a spent tool budget) must answer. The tools stay
        // defined (the history holds tool calls), and the model is told to
        // answer now; a tool call on this turn is not run.
        let must_answer = turn + 1 == limits.turns || calls.len() >= limits.tool_calls;
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
            // An answer. Check it doesn't hand back work the tools can do.
            if has_tools && retries < limits.retries && !must_answer && !text.trim().is_empty() {
                let verdict = model
                    .ask(check_body(model.check_id(), question, &text, tools.defs()))
                    .await
                    .and_then(|r| read_check(&r));
                if let Some(what) = verdict {
                    retries += 1;
                    info!(what = %what, "Chat: answer handed work back; looking again");
                    messages.push(json!({ "role": "user", "content": [{"type": "text", "text": nudge(&what)}] }));
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
            calls.push(name.to_string());
            let before = reg.len();
            let (text, label, summary, is_error, kind) = if calls.len() > limits.tool_calls {
                (
                    "Error: this answer has used all its lookups. Answer from what you read."
                        .to_string(),
                    name.replace('_', " "),
                    "Skipped".to_string(),
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
                            fin: finalize(&note, reg),
                            used,
                            rechecked: retries > 0,
                            calls,
                        });
                    }
                }
            };
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
            used.push(json!({ "kind": kind, "label": label, "name": name, "repo": repo, "summary": summary }));
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
        fin: finalize(&last_text, reg),
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
            let out = chat_code::run(&self.reader, reg, name, input).await;
            Called::Done {
                text: out.text,
                label: chat_code::label(name, input),
                summary: out.summary,
                is_error: out.is_error,
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
        let limits = Limits {
            turns: 4,
            tool_calls: 30,
            retries: 1,
        };
        let a = Ask { limits, ..ask("q") };
        run(&model, &mut tb, &sink, &mut reg, a).await.unwrap();
        let bodies = model.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 4);
        // The last turn is told to answer; the tools stay defined for the history.
        assert!(bodies[3]["messages"]
            .to_string()
            .contains("used all the lookups"));
        assert!(bodies[3]["tools"].is_array());
        assert!(!bodies[2]["messages"]
            .to_string()
            .contains("used all the lookups"));
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
