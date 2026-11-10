use serde_json::Value;

/// Options for controlling the converse loop.
#[derive(Clone, Copy)]
pub struct ConverseOptions {
    pub max_turns: usize,
    pub max_tokens: i32,
    /// Optional deadline (wall-clock Instant). When <120s remain, the loop
    /// injects a "wrap up now" message and allows one final turn before exiting.
    pub deadline: Option<std::time::Instant>,
    /// Reasoning effort on always-thinking models ("low" | "medium" | "high" |
    /// "xhigh" | "max"); None → "high". Ignored by older models. Set "low" for
    /// short single-answer calls so thinking doesn't eat a small `max_tokens`.
    pub effort: Option<&'static str>,
}

const DEFAULT_MAX_TURNS: usize = 40;

impl Default for ConverseOptions {
    fn default() -> Self {
        Self {
            max_turns: DEFAULT_MAX_TURNS,
            max_tokens: 16384,
            deadline: None,
            effort: None,
        }
    }
}

/// One tool call that ended (or was refused), as a loop observer sees it.
pub struct ToolEnded<'a> {
    pub name: &'a str,
    pub input: &'a Value,
    /// The model's id for the call (`toolu_…`), the same as in the log.
    pub tool_use_id: &'a str,
    /// When the call started (RFC 3339, UTC, milliseconds).
    pub started_at: &'a str,
    pub duration_ms: u64,
    /// It failed or was refused.
    pub is_error: bool,
    pub outcome: &'a common::agent_log::ToolOutcome,
}

/// A tool call as a [`ToolCallFn`] gets it: the input as a short summary
/// ([`input_summary`]), never the whole input.
pub struct ToolEvent<'a> {
    pub tool: &'a str,
    pub input_summary: &'a str,
    pub tool_use_id: &'a str,
    pub started_at: &'a str,
    pub duration_ms: u64,
    pub is_error: bool,
    pub outcome: &'a common::agent_log::ToolOutcome,
}

/// Observer invoked after each tool call.
pub type OnToolCall = dyn Fn(&ToolEvent<'_>) + Send + Sync;

/// Watches a tool loop as it runs: what the agent wrote before each batch
/// of tool calls, and each call once it ended (or was refused). Called
/// inline, so an observer only records and never waits.
pub trait LoopObserver: Send + Sync {
    /// The text the agent wrote in a turn that goes on to call tools.
    fn on_text(&self, _text: &str) {}
    /// One tool call ended: its name, its input, how long it took, and
    /// whether it failed or was refused.
    fn on_tool_call(&self, name: &str, input: &Value, duration_ms: u64, is_error: bool);
    /// One tool call ended, with its id, start and outcome. The loop calls
    /// this; by default it is [`LoopObserver::on_tool_call`].
    fn on_tool_ended(&self, call: &ToolEnded<'_>) {
        self.on_tool_call(call.name, call.input, call.duration_ms, call.is_error);
    }
}

/// A plain [`OnToolCall`] closure as a loop observer: it sees each call with
/// a short summary of its input ([`input_summary`]), never the text.
pub struct ToolCallFn(pub Box<OnToolCall>);

impl LoopObserver for ToolCallFn {
    fn on_tool_call(&self, name: &str, input: &Value, duration_ms: u64, is_error: bool) {
        let outcome = common::agent_log::ToolOutcome::default_for(is_error);
        self.on_tool_ended(&ToolEnded {
            name,
            input,
            tool_use_id: "",
            started_at: "",
            duration_ms,
            is_error,
            outcome: &outcome,
        });
    }
    fn on_tool_ended(&self, call: &ToolEnded<'_>) {
        let summary = input_summary(call.input);
        (self.0)(&ToolEvent {
            tool: call.name,
            input_summary: &summary,
            tool_use_id: call.tool_use_id,
            started_at: call.started_at,
            duration_ms: call.duration_ms,
            is_error: call.is_error,
            outcome: call.outcome,
        });
    }
}

/// A tool call's input in one short line: its path, pattern or command when
/// it has one, else the input itself cut to 200 bytes.
pub fn input_summary(input: &Value) -> String {
    if let Some(path) = input.get("path").and_then(|v| v.as_str()) {
        path.to_string()
    } else if let Some(pattern) = input.get("pattern").and_then(|v| v.as_str()) {
        format!("pattern: {pattern}")
    } else if let Some(command) = input.get("command").and_then(|v| v.as_str()) {
        command.chars().take(200).collect()
    } else {
        let s = input.to_string();
        if s.len() > 200 {
            format!("{}…", common::truncate_str(&s, 200))
        } else {
            s
        }
    }
}

pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// Trait for executing tools. Implemented by the orchestrator.
#[async_trait::async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}
