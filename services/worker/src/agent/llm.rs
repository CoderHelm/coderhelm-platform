use serde_json::Value;

/// Options for controlling the converse loop.
pub struct ConverseOptions {
    pub max_turns: usize,
    pub max_tokens: i32,
}

const DEFAULT_MAX_TURNS: usize = 40;

impl Default for ConverseOptions {
    fn default() -> Self {
        Self {
            max_turns: DEFAULT_MAX_TURNS,
            max_tokens: 16384,
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
