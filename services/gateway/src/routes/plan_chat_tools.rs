//! The plan chat's team skills, team agents, always-apply standards and
//! read-only data sources. They run in the worker, exactly as a planning
//! run's do (same checks, limits and logging); the chat calls it once per
//! message for the tools and what to tell the model, and once per tool call.

use std::collections::HashSet;

use serde_json::{json, Value};
use tracing::warn;

use crate::AppState;

/// What the chat is about, for the worker.
#[derive(Debug, Clone, Default)]
pub struct ChatScope {
    pub team_id: String,
    pub chat_id: String,
    pub repo: String,
    pub branch: String,
    /// The chat's first message: the standards it was given are logged then.
    pub first: bool,
    /// The brain chat (its worker calls are metered as the brain's).
    pub brain: bool,
}

/// The worker's tools for this chat message, and what's told to the model.
#[derive(Debug, Clone, Default)]
pub struct WorkerTools {
    pub tools: Vec<Value>,
    pub names: HashSet<String>,
    pub prompt: String,
}

/// A chat id the dashboard sent, kept only when it's a plain id.
pub fn chat_id(raw: Option<&str>) -> String {
    raw.map(str::trim)
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 64
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
        .unwrap_or("unsaved")
        .to_string()
}

async fn invoke(state: &AppState, payload: Value) -> Result<Value, String> {
    let name = &state.config.worker_function_name;
    if name.is_empty() {
        return Err("no worker configured".into());
    }
    let body = serde_json::to_vec(&json!({ "plan_chat": payload }))
        .map_err(|e| format!("serialize: {e}"))?;
    let resp = state
        .lambda
        .invoke()
        .function_name(name)
        .payload(aws_sdk_lambda::primitives::Blob::new(body))
        .send()
        .await
        .map_err(|e| format!("worker invoke failed: {e}"))?;
    if let Some(err) = resp.function_error() {
        return Err(format!("worker error: {err}"));
    }
    let bytes = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();
    serde_json::from_slice(&bytes).map_err(|e| format!("parse: {e}"))
}

/// The worker's tools for one chat message. None when the worker can't be
/// reached: the chat goes on with its own tools.
pub async fn setup(state: &AppState, scope: &ChatScope) -> WorkerTools {
    let r = invoke(
        state,
        json!({
            "op": "setup",
            "team_id": scope.team_id,
            "chat_id": scope.chat_id,
            "repo": scope.repo,
            "branch": scope.branch,
            "record_applied": scope.first,
            "feature": if scope.brain { "brain" } else { "" },
        }),
    )
    .await;
    match r {
        Ok(v) => from_setup(&v),
        Err(e) => {
            warn!(error = %e, "Plan chat: no skills or data source tools this message");
            WorkerTools::default()
        }
    }
}

/// The tools a setup answer gives, writes and anything named like a GitHub
/// or MCP tool left out (those are the chat's own).
pub fn from_setup(v: &Value) -> WorkerTools {
    let tools: Vec<Value> = v["tools"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| {
            let n = t["name"].as_str().unwrap_or("");
            !n.is_empty() && !n.contains("__") && !common::skills::AGENT_WRITE_TOOLS.contains(&n)
        })
        .collect();
    let names = tools
        .iter()
        .filter_map(|t| t["name"].as_str().map(str::to_string))
        .collect();
    WorkerTools {
        tools,
        names,
        prompt: v["prompt"].as_str().unwrap_or("").to_string(),
    }
}

/// Counts of what this chat message used, so its limits hold across calls.
#[derive(Debug, Default)]
pub struct Used {
    pub reads: usize,
    pub asks: usize,
}

/// Runs one worker tool. Returns the text for the model and what the chat
/// shows for it (`{kind, label, href}`), if anything.
pub async fn call(
    state: &AppState,
    scope: &ChatScope,
    used: &mut Used,
    name: &str,
    input: &Value,
) -> (String, Option<Value>) {
    let r = invoke(
        state,
        json!({
            "op": "call",
            "team_id": scope.team_id,
            "chat_id": scope.chat_id,
            "repo": scope.repo,
            "branch": scope.branch,
            "name": name,
            "input": input,
            "reads_used": used.reads,
            "asks_used": used.asks,
            "feature": if scope.brain { "brain" } else { "" },
        }),
    )
    .await;
    match name {
        "read_skill" | "read_skill_file" => used.reads += 1,
        "ask_team_agent" => used.asks += 1,
        _ => {}
    }
    match r {
        Ok(v) => (
            v["text"].as_str().unwrap_or("").to_string(),
            v.get("used").filter(|u| u.is_object()).cloned(),
        ),
        Err(e) => {
            warn!(tool = name, error = %e, "Plan chat worker tool failed");
            (
                "Error: that couldn't be read right now. Continue without it.".into(),
                None,
            )
        }
    }
}

/// The system prompt's part about the worker's tools.
pub fn prompt(w: &WorkerTools) -> String {
    if w.tools.is_empty() {
        return String::new();
    }
    format!(
        "\n\nWhen it helps the plan, use the team's own guidance and data the same way Coderhelm's \
         planner does: read a fitting team skill with read_skill, ask a fitting team agent with \
         ask_team_agent, and look facts up in the team's connected data sources. Each is \
         read-only and logged; don't call them on greetings or questions that don't need them.{}",
        w.prompt
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chat_id_is_kept_only_when_plain() {
        assert_eq!(chat_id(Some("01JABC-x_1")), "01JABC-x_1");
        assert_eq!(chat_id(Some("a b")), "unsaved");
        assert_eq!(chat_id(Some(&"x".repeat(65))), "unsaved");
        assert_eq!(chat_id(None), "unsaved");
    }

    #[test]
    fn setup_keeps_reads_and_drops_writes_and_others_names() {
        let v = json!({
            "tools": [
                {"name": "read_skill", "description": "d", "input_schema": {}},
                {"name": "ask_team_agent", "description": "d", "input_schema": {}},
                {"name": "logs_search", "description": "d", "input_schema": {}},
                {"name": "confluence_append", "description": "d", "input_schema": {}},
                {"name": "github__read_file", "description": "d", "input_schema": {}},
                {"name": "write_file", "description": "d", "input_schema": {}}
            ],
            "prompt": "\n\n## Team skills\n- skill:debug"
        });
        let w = from_setup(&v);
        let mut names: Vec<&String> = w.names.iter().collect();
        names.sort();
        assert_eq!(names, ["ask_team_agent", "logs_search", "read_skill"]);
        assert!(prompt(&w).contains("## Team skills"));
        assert_eq!(prompt(&WorkerTools::default()), "");
    }
}
