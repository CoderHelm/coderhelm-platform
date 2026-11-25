//! Whether a tool a skill run is granted only reads: decided once, shown on
//! the schedule, never ticked by hand.
//!
//! In order:
//! 1. **Never grantable**: a tool whose name says it sends, publishes or
//!    deletes ([`classify_external`]), or whose server marks it destructive
//!    (`destructiveHint`). No admin can grant it.
//! 2. **An admin's word**, read or write, when one was given.
//! 3. **The server's own annotations**: `readOnlyHint` true is a read,
//!    false a write.
//! 4. **A model's reading** of the tool's description and input schema,
//!    when the server says nothing: one cheap call per server version,
//!    cached ([`RiskJudge`]).
//! 5. Otherwise a write: an unknown tool is never taken as a read.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::plugins::connect::ToolDef;
use crate::tool_registry::{classify_external, Risk};

/// How a tool's risk was decided, for the schedule's page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// Its name says it sends, publishes or deletes.
    Name,
    /// The server's `readOnlyHint` / `destructiveHint`.
    Annotation,
    /// A model read its description and input schema.
    Model,
    /// An admin said so.
    Admin,
    /// Nothing said: taken as a write.
    Unknown,
}

/// A model's verdict on one tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub risk: Risk,
    pub why: String,
}

/// Reads tools a server doesn't annotate. `version` names the server's exact
/// build (its commit, its package version, or a hash of its tool list), so
/// a verdict is reused until the server changes.
#[async_trait::async_trait]
pub trait RiskJudge: Send + Sync {
    async fn judge(
        &self,
        team: &str,
        version: &str,
        tools: &[ToolDef],
    ) -> BTreeMap<String, Verdict>;
}

/// The decision for one tool.
pub fn decide(tool: &ToolDef, admin: Option<Risk>, judged: Option<&Verdict>) -> (Risk, Basis) {
    if classify_external(&tool.name) == Risk::Consequential {
        return (Risk::Consequential, Basis::Name);
    }
    if tool.destructive == Some(true) {
        return (Risk::Consequential, Basis::Annotation);
    }
    if let Some(r) = admin.filter(|r| *r != Risk::Consequential) {
        return (r, Basis::Admin);
    }
    match tool.read_only {
        Some(true) => return (Risk::Read, Basis::Annotation),
        Some(false) => return (Risk::Write, Basis::Annotation),
        None => {}
    }
    match judged {
        Some(v) => (v.risk, Basis::Model),
        None => (Risk::Write, Basis::Unknown),
    }
}

/// A server's version key from its tool list, when it has no other: the
/// same tools, described the same way, are the same version.
pub fn list_version(server: &str, tools: &[ToolDef]) -> String {
    let mut parts: Vec<String> = tools
        .iter()
        .map(|t| format!("{}\u{1}{}\u{1}{}", t.name, t.description, t.input_schema))
        .collect();
    parts.sort();
    format!(
        "{server}#{}",
        &crate::content_hash(&parts.join("\u{2}"))[..16]
    )
}

/// The model's instructions for [`RiskJudge`]: one line per tool back.
pub const JUDGE_SYSTEM: &str = "You classify tools an automated agent may call, from each tool's \
name, description and input schema only. For each tool answer one of: read (it only looks \
something up and changes nothing anywhere), write (it creates or changes something), \
consequential (it sends, publishes, deletes, pays, deploys or otherwise reaches people or can't \
be undone). When unsure, it is not read. Answer JSON only: an object mapping each tool name to \
{\"risk\": \"read|write|consequential\", \"why\": \"a few words\"}.";

/// What the model is shown.
pub fn judge_prompt(tools: &[ToolDef]) -> String {
    let list: Vec<Value> = tools
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "description": crate::truncate_str(&t.description, 1_500),
                "input_schema": t.input_schema,
            })
        })
        .collect();
    serde_json::to_string_pretty(&list).unwrap_or_default()
}

/// The model's answer, read strictly: tools it didn't name, or named with a
/// risk that isn't one of the three, have no verdict.
pub fn read_verdicts(answer: &str, tools: &[ToolDef]) -> BTreeMap<String, Verdict> {
    let text = answer.trim();
    let start = text.find('{').unwrap_or(0);
    let end = text.rfind('}').map(|e| e + 1).unwrap_or(text.len());
    let v: Value = serde_json::from_str(&text[start..end.max(start)]).unwrap_or(Value::Null);
    let mut out = BTreeMap::new();
    for t in tools {
        let e = &v[&t.name];
        let Some(risk) = e["risk"].as_str().and_then(Risk::parse) else {
            continue;
        };
        out.insert(
            t.name.clone(),
            Verdict {
                risk,
                why: crate::truncate_str(e["why"].as_str().unwrap_or(""), 120).to_string(),
            },
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn td(name: &str, ro: Option<bool>, destructive: Option<bool>) -> ToolDef {
        ToolDef {
            name: name.into(),
            description: format!("{name} tool"),
            input_schema: serde_json::json!({"type": "object"}),
            read_only: ro,
            destructive,
        }
    }

    #[test]
    fn annotations_decide_and_a_model_fills_in_what_they_dont_say() {
        let model = Verdict {
            risk: Risk::Read,
            why: "looks up counts".into(),
        };
        assert_eq!(
            decide(&td("traffic_counts", None, None), None, Some(&model)),
            (Risk::Read, Basis::Model)
        );
        assert_eq!(
            decide(&td("traffic_counts", None, None), None, None),
            (Risk::Write, Basis::Unknown)
        );
        assert_eq!(
            decide(&td("list_pages", Some(true), None), None, None),
            (Risk::Read, Basis::Annotation)
        );
        assert_eq!(
            decide(&td("run_query", Some(false), None), None, Some(&model)),
            (Risk::Write, Basis::Annotation)
        );
        // An admin overrides a model or an annotation, never a delete.
        assert_eq!(
            decide(&td("run_query", Some(false), None), Some(Risk::Read), None),
            (Risk::Read, Basis::Admin)
        );
        assert_eq!(
            decide(
                &td("set_banner", Some(false), Some(true)),
                Some(Risk::Read),
                None
            ),
            (Risk::Consequential, Basis::Annotation)
        );
        assert_eq!(
            decide(&td("send_report", Some(true), None), Some(Risk::Read), None),
            (Risk::Consequential, Basis::Name)
        );
    }

    #[test]
    fn the_models_answer_is_read_strictly_and_versions_follow_the_tool_list() {
        let tools = [
            td("a_counts", None, None),
            td("b_change", None, None),
            td("c", None, None),
        ];
        let v = read_verdicts(
            "Here: {\"a_counts\": {\"risk\": \"read\", \"why\": \"counts\"}, \"b_change\": {\"risk\": \"maybe\"}}",
            &tools,
        );
        assert_eq!(v.len(), 1);
        assert_eq!(v["a_counts"].risk, Risk::Read);
        assert!(read_verdicts("no json", &tools).is_empty());
        assert_eq!(
            list_version("s", &tools),
            list_version("s", &[tools[2].clone(), tools[0].clone(), tools[1].clone()])
        );
        assert_ne!(list_version("s", &tools), list_version("s", &tools[..2]));
        assert!(judge_prompt(&tools).contains("b_change"));
    }
}
