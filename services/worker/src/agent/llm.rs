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

/// Observer invoked after each tool call: (tool_name, duration_ms, input_summary, is_error).
pub type OnToolCall = dyn Fn(&str, u64, &str, bool) + Send + Sync;

/// Watches a tool loop as it runs: what the agent wrote before each batch
/// of tool calls, and each call once it ended (or was refused). Called
/// inline, so an observer only records and never waits.
pub trait LoopObserver: Send + Sync {
    /// The text the agent wrote in a turn that goes on to call tools.
    fn on_text(&self, _text: &str) {}
    /// One tool call ended: its name, its input, how long it took, and
    /// whether it failed or was refused.
    fn on_tool_call(&self, name: &str, input: &Value, duration_ms: u64, is_error: bool);
}

/// A plain [`OnToolCall`] closure as a loop observer: it sees each call with
/// a short summary of its input ([`input_summary`]), never the text.
pub struct ToolCallFn(pub Box<OnToolCall>);

impl LoopObserver for ToolCallFn {
    fn on_tool_call(&self, name: &str, input: &Value, duration_ms: u64, is_error: bool) {
        (self.0)(name, duration_ms, &input_summary(input), is_error);
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
