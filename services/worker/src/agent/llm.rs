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

/// A tool call's input in one short line the run page reads back
/// (`parseInputSummary`): `key: value` for the fields that name what the
/// call acts on (its path, with the lines of a range; its query, pattern or
/// command), else the input itself as JSON cut to 200 bytes. A bare path
/// read back as no fields at all, and the live log drew "Edited" and "Read"
/// with no file, and an empty name in a list of files.
pub fn input_summary(input: &Value) -> String {
    let field = |k: &str| input.get(k).and_then(|v| v.as_str()).map(str::trim);
    let mut parts: Vec<String> = Vec::new();
    if let Some(path) = field("path").filter(|p| !p.is_empty()) {
        parts.push(format!("path: {path}"));
        for k in ["start_line", "end_line"] {
            if let Some(n) = input.get(k).and_then(|v| v.as_u64()) {
                parts.push(format!("{k}: {n}"));
            }
        }
    }
    for k in ["query", "pattern", "command", "url", "host"] {
        if let Some(v) = field(k).filter(|v| !v.is_empty()) {
            parts.push(format!("{k}: {}", v.chars().take(200).collect::<String>()));
            break;
        }
    }
    if !parts.is_empty() {
        return parts.join(", ");
    }
    let s = input.to_string();
    if s.len() > 200 {
        format!("{}…", common::truncate_str(&s, 200))
    } else {
        s
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

#[cfg(test)]
mod input_summary_tests {
    use super::input_summary;
    use serde_json::json;

    /// Every summary names what the call acted on, as `key: value`.
    #[test]
    fn the_summary_names_the_file_or_query() {
        assert_eq!(
            input_summary(&json!({"path": "src/a.ts", "edits": "[...]", "message": "m"})),
            "path: src/a.ts"
        );
        assert_eq!(
            input_summary(&json!({"path": "src/a.ts", "start_line": 3, "end_line": 9})),
            "path: src/a.ts, start_line: 3, end_line: 9"
        );
        assert_eq!(
            input_summary(&json!({"query": "scrollWidth <= window.innerWidth"})),
            "query: scrollWidth <= window.innerWidth"
        );
        assert_eq!(
            input_summary(&json!({"command": "npm test"})),
            "command: npm test"
        );
        assert_eq!(input_summary(&json!({})), "{}");
    }
}
