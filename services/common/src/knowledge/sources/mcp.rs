//! Reading a provider through its official remote MCP server, with a
//! person's sign-in instead of an API key.
//!
//! A knowledge source can sync through the same per-person OAuth sign-in
//! that Connected apps uses for agents (`mcp_remote`): the provider's MCP
//! server registers Coderhelm dynamically, so it works in every install
//! with no app to set up. The source records whose sign-in it reads with
//! ([`super::SignIn`]); each sync opens that person's connection, which the
//! runtime renews before it expires.
//!
//! [`McpTools`] is the one seam: [`RuntimeTools`] in production (the token
//! goes to the server's own address only, never to the provider's other
//! APIs, whose audience it isn't), a fake MCP server in tests. Connectors
//! call only the read-only tools they name in their own allow list.
//!
//! Tool results are read as data: `structuredContent` when the server sends
//! it, else the JSON inside the text content (servers often put a sentence
//! before it). Queries wrap what they ask for under a key of their own
//! ([`find_key`]), so the answer is found wherever the server nests it.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::OnceCell;

use super::connector::ConnectorError;
use crate::mcp_remote::client::RemoteTool;
use crate::mcp_remote::runtime::{ConnError, Runtime};
use crate::mcp_remote::store::{Owner, ServerRecord};

/// Calling one MCP server's tools with one person's connection.
#[async_trait::async_trait]
pub trait McpTools: Send + Sync {
    /// The tools the server lists (asked once per session).
    async fn tools(&self) -> Result<Vec<RemoteTool>, ConnectorError>;
    /// Calls `tool` and returns its whole result.
    async fn call(&self, tool: &RemoteTool, args: &Value) -> Result<Value, ConnectorError>;
}

/// The production [`McpTools`]: the runtime, one server record and the
/// owner whose sign-in is used.
pub struct RuntimeTools {
    pub rt: Runtime,
    pub server: ServerRecord,
    pub owner: Owner,
    /// What the provider is called in errors ("Sanity").
    pub provider: &'static str,
    listed: OnceCell<Vec<RemoteTool>>,
}

impl RuntimeTools {
    pub fn new(rt: Runtime, server: ServerRecord, owner: Owner, provider: &'static str) -> Self {
        Self {
            rt,
            server,
            owner,
            provider,
            listed: OnceCell::new(),
        }
    }
}

/// A connection failure as a connector error.
pub fn conn_error(provider: &str, e: ConnError) -> ConnectorError {
    match e {
        ConnError::NotConnected => {
            ConnectorError::Reconnect(format!("Nobody is signed in to {provider} for this."))
        }
        ConnError::NeedsReconnect(why) | ConnError::Failed(why) => ConnectorError::Reconnect(why),
        ConnError::Temporary(why) => ConnectorError::Temporary(why),
    }
}

#[async_trait::async_trait]
impl McpTools for RuntimeTools {
    async fn tools(&self) -> Result<Vec<RemoteTool>, ConnectorError> {
        self.listed
            .get_or_try_init(|| async {
                self.rt
                    .list_tools(&self.server, &self.owner)
                    .await
                    .map(|(t, _, _)| t)
                    .map_err(|e| conn_error(self.provider, e))
            })
            .await
            .cloned()
    }

    async fn call(&self, tool: &RemoteTool, args: &Value) -> Result<Value, ConnectorError> {
        self.rt
            .read_tool(&self.server, &self.owner, tool, args)
            .await
            .map_err(|e| conn_error(self.provider, e))
    }
}

/// The tool named one of `names` (servers rename tools now and then; the
/// first name is the current one).
pub async fn tool(
    t: &dyn McpTools,
    provider: &str,
    names: &[&str],
) -> Result<RemoteTool, ConnectorError> {
    let listed = t.tools().await?;
    names
        .iter()
        .find_map(|n| listed.iter().find(|x| x.name == *n))
        .cloned()
        .ok_or_else(|| {
            ConnectorError::Temporary(format!(
                "{provider}'s MCP server no longer offers {}. Coderhelm needs an update to read it.",
                names.first().copied().unwrap_or("a tool it needs")
            ))
        })
}

/// Does the tool take this top-level argument?
pub fn takes(tool: &RemoteTool, arg: &str) -> bool {
    tool.input_schema["properties"].get(arg).is_some()
}

/// The data in a tool result: its `structuredContent`, else the first JSON
/// value found in its text. A result the server marks as an error becomes
/// an error with the server's own words.
pub fn result_json(provider: &str, result: &Value) -> Result<Value, ConnectorError> {
    let texts: Vec<&str> = result["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| c["text"].as_str())
        .collect();
    if result["isError"].as_bool() == Some(true) {
        let said = crate::truncate_str(texts.join(" ").trim(), 300).to_string();
        return Err(ConnectorError::Temporary(if said.is_empty() {
            format!("{provider} reported an error.")
        } else {
            format!("{provider} said: {said}")
        }));
    }
    if let Some(s) = result.get("structuredContent").filter(|s| !s.is_null()) {
        return Ok(s.clone());
    }
    for t in texts {
        if let Some(v) = json_in(t) {
            return Ok(v);
        }
    }
    Err(ConnectorError::Temporary(format!(
        "{provider} answered in a form Coderhelm can't read."
    )))
}

/// The first JSON object or array in `text`: the whole text when it is
/// JSON, else from the first `{` or `[` that starts a complete value.
pub fn json_in(text: &str) -> Option<Value> {
    let t = text.trim();
    if let Ok(v) = serde_json::from_str::<Value>(t) {
        if v.is_object() || v.is_array() {
            return Some(v);
        }
    }
    for (i, c) in t.char_indices() {
        if c != '{' && c != '[' {
            continue;
        }
        let mut it = serde_json::Deserializer::from_str(&t[i..]).into_iter::<Value>();
        if let Some(Ok(v)) = it.next() {
            if v.is_object() || v.is_array() {
                return Some(v);
            }
        }
    }
    None
}

/// The first value under `key`, searching objects and arrays depth first
/// (a server may wrap a query's answer as `{result: ...}`, `{documents:
/// [...]}`, or not at all).
pub fn find_key<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    match v {
        Value::Object(m) => {
            if let Some(x) = m.get(key) {
                return Some(x);
            }
            m.values().find_map(|x| find_key(x, key))
        }
        Value::Array(a) => a.iter().find_map(|x| find_key(x, key)),
        _ => None,
    }
}

/// Every object in `v` (at any depth) for which `keep` holds.
pub fn objects<'a>(
    v: &'a Value,
    keep: &dyn Fn(&serde_json::Map<String, Value>) -> bool,
) -> Vec<&'a serde_json::Map<String, Value>> {
    let mut out = Vec::new();
    fn walk<'a>(
        v: &'a Value,
        keep: &dyn Fn(&serde_json::Map<String, Value>) -> bool,
        out: &mut Vec<&'a serde_json::Map<String, Value>>,
    ) {
        match v {
            Value::Object(m) => {
                if keep(m) {
                    out.push(m);
                }
                for x in m.values() {
                    walk(x, keep, out);
                }
            }
            Value::Array(a) => {
                for x in a {
                    walk(x, keep, out);
                }
            }
            _ => {}
        }
    }
    walk(v, keep, &mut out);
    out
}

/// Arguments that name a project and dataset, in whichever shape the tool
/// takes: `projectId`/`dataset` at the top (the hosted server today) or a
/// `resource` object (its earlier shape).
pub fn scoped_args(
    tool: &RemoteTool,
    project: &str,
    dataset: Option<&str>,
    mut rest: Value,
) -> Value {
    if !rest.is_object() {
        rest = json!({});
    }
    if takes(tool, "resource") && !takes(tool, "projectId") {
        rest["resource"] = json!({
            "target": "dataset",
            "projectId": project,
            "dataset": dataset.unwrap_or(""),
        });
    } else {
        rest["projectId"] = json!(project);
        if let Some(d) = dataset {
            if takes(tool, "dataset") || !takes(tool, "projectId") {
                rest["dataset"] = json!(d);
            }
        }
    }
    rest
}

/// A shared handle, so a connector and its caller can both hold the tools.
pub type Tools = Arc<dyn McpTools>;

#[cfg(test)]
pub mod fake {
    //! A fake MCP server for connector tests: a list of tools, and a
    //! handler that answers each call (or fails it).

    use std::sync::Mutex;

    use super::*;

    type Handler = Box<dyn Fn(&str, &Value) -> Result<Value, ConnectorError> + Send + Sync>;

    pub struct FakeMcp {
        pub listed: Vec<RemoteTool>,
        handler: Handler,
        pub calls: Mutex<Vec<(String, Value)>>,
    }

    pub fn tool_with(name: &str, props: &[&str]) -> RemoteTool {
        let mut p = serde_json::Map::new();
        for k in props {
            p.insert((*k).to_string(), json!({"type": "string"}));
        }
        RemoteTool {
            name: name.into(),
            title: None,
            description: String::new(),
            input_schema: json!({"type": "object", "properties": p}),
            read_only: Some(true),
            destructive: None,
        }
    }

    impl FakeMcp {
        pub fn new(
            listed: Vec<RemoteTool>,
            handler: impl Fn(&str, &Value) -> Result<Value, ConnectorError> + Send + Sync + 'static,
        ) -> Self {
            Self {
                listed,
                handler: Box::new(handler),
                calls: Mutex::new(Vec::new()),
            }
        }

        pub fn calls(&self) -> Vec<(String, Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// A tool result whose text is a sentence and then JSON, as servers
    /// often answer.
    pub fn text_result(v: &Value) -> Value {
        json!({"content": [{"type": "text", "text": format!("Here is what you asked for:\n{v}")}]})
    }

    #[async_trait::async_trait]
    impl McpTools for FakeMcp {
        async fn tools(&self) -> Result<Vec<RemoteTool>, ConnectorError> {
            Ok(self.listed.clone())
        }
        async fn call(&self, tool: &RemoteTool, args: &Value) -> Result<Value, ConnectorError> {
            self.calls
                .lock()
                .unwrap()
                .push((tool.name.clone(), args.clone()));
            (self.handler)(&tool.name, args)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::tool_with;
    use super::*;

    #[test]
    fn json_is_found_after_a_sentence_or_in_structured_content() {
        let r = json!({"content": [{"type": "text", "text": "Found 2 documents: \n{\"a\": [1, 2]}\ntrailing"}]});
        assert_eq!(result_json("Sanity", &r).unwrap()["a"][1], 2);
        let r = json!({"content": [], "structuredContent": {"b": true}});
        assert_eq!(result_json("Sanity", &r).unwrap()["b"], true);
        let r = json!({"content": [{"type": "text", "text": "<datasets><x/></datasets>"}]});
        assert!(matches!(
            result_json("Sanity", &r),
            Err(ConnectorError::Temporary(_))
        ));
        let r =
            json!({"isError": true, "content": [{"type": "text", "text": "No access to project"}]});
        let e = result_json("Sanity", &r).unwrap_err();
        assert_eq!(e.to_string(), "Sanity said: No access to project");
    }

    #[test]
    fn a_wrapped_answer_is_found_at_any_depth() {
        let v = json!({"documents": {"document": [{"chPage": [{"_id": "a"}]}]}});
        assert_eq!(find_key(&v, "chPage").unwrap()[0]["_id"], "a");
        assert!(find_key(&v, "missing").is_none());
    }

    #[test]
    fn scoped_arguments_follow_the_tool_schema() {
        let now = tool_with("query_documents", &["projectId", "dataset", "query"]);
        let a = scoped_args(&now, "p1", Some("production"), json!({"query": "*"}));
        assert_eq!(a["projectId"], "p1");
        assert_eq!(a["dataset"], "production");
        let before = tool_with("query_documents", &["resource", "query"]);
        let a = scoped_args(&before, "p1", Some("production"), json!({"query": "*"}));
        assert_eq!(a["resource"]["projectId"], "p1");
        assert_eq!(a["resource"]["dataset"], "production");
        assert!(a.get("projectId").is_none());
        let datasets = tool_with("list_datasets", &["projectId"]);
        let a = scoped_args(&datasets, "p1", None, Value::Null);
        assert_eq!(a, json!({"projectId": "p1"}));
    }

    #[test]
    fn connection_failures_ask_for_a_new_sign_in() {
        assert!(matches!(
            conn_error("Sanity", ConnError::NeedsReconnect("expired".into())),
            ConnectorError::Reconnect(_)
        ));
        assert!(matches!(
            conn_error("Sanity", ConnError::NotConnected),
            ConnectorError::Reconnect(_)
        ));
        assert!(matches!(
            conn_error("Sanity", ConnError::Temporary("down".into())),
            ConnectorError::Temporary(_)
        ));
    }
}
