//! Plugins as a source in the brain chat.
//!
//! The brain chat picks the sources a question may read. An active plugin
//! is one more source: picking it lets the brain ask that plugin's agent,
//! which answers from the plugin's skills (and the knowledge bases attached
//! to the plugin) with read-only tools only. A plugin source never saves a
//! draft or calls a write or consequential tool from the brain.
//!
//! The brain joins a new source kind by its key, the tools that map to it
//! and a branch that runs them; these are the plugin's.

use serde_json::{json, Value};

use super::install::{self, Installed};
use super::kv::Kv;

/// The source kind's key in the brain's picker.
pub const SOURCE_KEY: &str = "plugins";
pub const SOURCE_LABEL: &str = "Plugins";
pub const SOURCE_ABOUT: &str = "Installed plugins answer from their own skills";
/// The tool that asks a plugin: `ask_plugin`.
pub const ASK_TOOL: &str = "ask_plugin";

/// True for a tool name that belongs to the plugins source.
pub fn is_plugin_tool(name: &str) -> bool {
    name == ASK_TOOL || name.starts_with("plugin__")
}

/// The plugins a question may use: active ones, by id, name and
/// description.
pub async fn sources(kv: &dyn Kv, team: &str) -> Vec<Value> {
    if !super::flag::enabled(kv, team).await {
        return Vec::new();
    }
    install::list(kv, team)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(Installed::active)
        .map(|p| json!({ "id": p.id, "name": p.name, "description": p.description }))
        .collect()
}

/// The `ask_plugin` tool, limited to the picked plugins.
pub fn ask_tool(picked: &[String]) -> Value {
    json!({
        "name": ASK_TOOL,
        "description": "Ask one of the picked plugins a question. It answers from its own skills and attached knowledge, read only. Cite what it says as a source.",
        "input_schema": { "type": "object", "properties": {
            "plugin": { "type": "string", "enum": picked },
            "question": { "type": "string" }
        }, "required": ["plugin", "question"] }
    })
}

/// What a plugin knows, as text the brain cites: its skills, read in full
/// (they are short), headed by the plugin's description. Read only.
pub async fn context(kv: &dyn Kv, team: &str, plugin: &str) -> Result<String, String> {
    let (p, _) = install::get(kv, team, plugin).await.map_err(|e| e.error)?;
    if !p.active() {
        return Err("That plugin is turned off.".into());
    }
    let snap = install::current_snapshot(kv, team, &p)
        .await
        .map_err(|e| e.error)?;
    let mut out = format!(
        "# {} {}\n{}\n",
        p.name, snap.manifest.version, snap.manifest.description
    );
    for s in snap.skills() {
        let body = super::package::blob(kv, team, &s.blob)
            .await
            .unwrap_or_default();
        out.push_str(&format!(
            "\n## {}\n{}\n",
            s.name,
            crate::truncate_str(&body, 6_000)
        ));
    }
    Ok(crate::truncate_str(&out, 24_000).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_tools_are_their_own_source() {
        assert!(is_plugin_tool("ask_plugin"));
        assert!(!is_plugin_tool("sentry__list_issues"));
        assert_eq!(
            ask_tool(&["marketing".into()])["input_schema"]["properties"]["plugin"]["enum"][0],
            "marketing"
        );
    }
}
