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
//! before it), else the XML a server formats its answer as (Sanity's server
//! answers `<sentence>: ` then its data as XML elements, with each document
//! as JSON inside a `<document>` element). Queries wrap what they ask for under a key of their own
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

/// The data in a tool result: its `structuredContent`, else the data in its
/// text (JSON, or the XML some servers format their answer as). A result the
/// server marks as an error becomes an error with the server's own words.
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
    let joined = texts.join("\n");
    for t in texts
        .iter()
        .copied()
        .chain(std::iter::once(joined.as_str()))
    {
        if let Some(v) = data_in(t) {
            return Ok(v);
        }
    }
    tracing::warn!(
        provider,
        shape = %answer_shape(result),
        "remote MCP answer not readable"
    );
    Err(ConnectorError::Temporary(format!(
        "{provider}'s answer couldn't be read. Try again in a minute; if it keeps happening, disconnect {provider} and connect it again."
    )))
}

/// The data in one text: JSON when it is, or starts with, a JSON value;
/// else the XML elements in it when they come before any JSON; else the
/// first JSON value after a sentence; else the XML anywhere in it.
pub fn data_in(text: &str) -> Option<Value> {
    let t = text.trim();
    if let Ok(v) = serde_json::from_str::<Value>(t) {
        if v.is_object() || v.is_array() {
            return Some(v);
        }
    }
    let first_tag = t.find('<');
    let first_json = t.find(['{', '[']);
    if first_tag.is_some() && (first_json.is_none() || first_tag < first_json) {
        if let Some(v) = xml_in(t) {
            return Some(v);
        }
    }
    json_in(t).or_else(|| xml_in(t))
}

/// What an unreadable answer looked like, for the log: each content item's
/// type and length, and the start of the text with anything that looks
/// like a token or a long id taken out. Never the full answer.
pub fn answer_shape(result: &Value) -> String {
    let items: Vec<String> = result["content"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let kind = c["type"].as_str().unwrap_or("?");
            let len = c["text"].as_str().map(str::len).unwrap_or(0);
            format!("{kind}:{len}")
        })
        .collect();
    let first = result["content"][0]["text"].as_str().unwrap_or("");
    let preview: String = redact(first).chars().take(120).collect();
    format!(
        "items=[{}] structured={} preview={preview:?}",
        items.join(","),
        result
            .get("structuredContent")
            .is_some_and(|v| !v.is_null())
    )
}

/// Takes out anything token-like: runs of 16 or more letters, digits,
/// `_`, `-`, `.`, `+`, `/` or `=` (keys, tokens, signatures, long ids), and
/// whatever follows "Bearer ".
fn redact(text: &str) -> String {
    let mut out = String::new();
    let mut run = String::new();
    let flush = |run: &mut String, out: &mut String| {
        if run.chars().count() >= 16 {
            out.push_str("[redacted]");
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for c in text.chars() {
        if c.is_ascii_alphanumeric() || "_-.+/=".contains(c) {
            run.push(c);
        } else {
            flush(&mut run, &mut out);
            out.push(c);
        }
    }
    flush(&mut run, &mut out);
    match out.find("Bearer ") {
        Some(i) => format!("{}Bearer [redacted]", &out[..i]),
        None => out,
    }
}

/// The data in XML text such as Sanity's server answers with
/// (`Found 2 projects: \n<orgs>...</orgs>`), as JSON: an element with
/// elements inside becomes an object, repeated elements an array, and an
/// element holding text its text (or the JSON it holds). Element names are
/// taken as written, spaces included (`<Organization o1>`). None when no
/// element is found.
pub fn xml_in(text: &str) -> Option<Value> {
    let start = text.find('<')?;
    let (v, _) = xml_nodes(&text[start..], 0)?;
    v.as_object().is_some_and(|m| !m.is_empty()).then_some(v)
}

/// Sibling elements up to a closing tag (or the end), and what follows.
fn xml_nodes(mut s: &str, depth: usize) -> Option<(Value, &str)> {
    if depth > 64 {
        return None;
    }
    let mut map = serde_json::Map::new();
    loop {
        s = s.trim_start();
        if s.is_empty() || s.starts_with("</") {
            return Some((Value::Object(map), s));
        }
        if !s.starts_with('<') {
            // Text after the elements (a closing sentence): stop here.
            return Some((Value::Object(map), s));
        }
        if s.starts_with("<?") || s.starts_with("<!") {
            let end = s.find('>')?;
            s = &s[end + 1..];
            continue;
        }
        let gt = s.find('>')?;
        let tag = &s[1..gt];
        if tag.is_empty() || tag.contains('<') {
            return None;
        }
        if let Some(name) = tag.strip_suffix('/') {
            insert(&mut map, name.trim(), Value::String(String::new()));
            s = &s[gt + 1..];
            continue;
        }
        let name = tag.trim();
        let body = &s[gt + 1..];
        let (end, close_len) = closing(body, name)?;
        let inner = &body[..end];
        insert(&mut map, name, xml_value(inner, depth));
        s = &body[end + close_len..];
    }
}

/// Where `name`'s closing tag is in `body` (elements of the same name
/// nested inside counted), and its length. A name with attributes closes
/// with its first word.
fn closing(body: &str, name: &str) -> Option<(usize, usize)> {
    let mut found = None;
    for n in [name, name.split_whitespace().next().unwrap_or(name)] {
        let open = format!("<{n}>");
        let close = format!("</{n}>");
        let (mut at, mut level) = (0usize, 0usize);
        while let Some(i) = body[at..].find(&close) {
            let opens = body[at..at + i].matches(&open).count();
            level += opens;
            if level == 0 {
                found = Some((at + i, close.len()));
                break;
            }
            level -= 1;
            at += i + close.len();
        }
        if found.is_some() {
            break;
        }
    }
    found
}

fn xml_value(inner: &str, depth: usize) -> Value {
    let t = inner.trim();
    if t.starts_with('<') {
        if let Some((v, rest)) = xml_nodes(t, depth + 1) {
            if rest.trim().is_empty() && v.as_object().is_some_and(|m| !m.is_empty()) {
                return v;
            }
        }
    }
    if t.starts_with('{') || t.starts_with('[') {
        for candidate in [t.to_string(), unescape(t)] {
            if let Ok(v) = serde_json::from_str::<Value>(&candidate) {
                return v;
            }
        }
    }
    Value::String(unescape(t))
}

fn insert(map: &mut serde_json::Map<String, Value>, name: &str, v: Value) {
    match map.get_mut(name) {
        Some(Value::Array(a)) => a.push(v),
        Some(old) => {
            let first = old.take();
            *old = Value::Array(vec![first, v]);
        }
        None => {
            map.insert(name.to_string(), v);
        }
    }
}

fn unescape(t: &str) -> String {
    t.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
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
pub(crate) mod tests {
    use super::fake::tool_with;
    use super::*;

    #[test]
    fn json_is_found_after_a_sentence_or_in_structured_content() {
        let r = json!({"content": [{"type": "text", "text": "Found 2 documents: \n{\"a\": [1, 2]}\ntrailing"}]});
        assert_eq!(result_json("Sanity", &r).unwrap()["a"][1], 2);
        let r = json!({"content": [], "structuredContent": {"b": true}});
        assert_eq!(result_json("Sanity", &r).unwrap()["b"], true);
        let r = json!({"content": [{"type": "text", "text": "Done."}]});
        let e = result_json("Sanity", &r).unwrap_err();
        assert!(matches!(e, ConnectorError::Temporary(_)));
        assert!(e
            .to_string()
            .contains("disconnect Sanity and connect it again"));
        let r =
            json!({"isError": true, "content": [{"type": "text", "text": "No access to project"}]});
        let e = result_json("Sanity", &r).unwrap_err();
        assert_eq!(e.to_string(), "Sanity said: No access to project");
    }

    // Sanity's server answers a sentence, then its data as XML (its
    // formatResponse: fast-xml-parser's XMLBuilder, format on, entities
    // off), each document as pretty JSON inside <document>.
    pub(crate) const SANITY_PROJECTS: &str = "Found 2 projects in 1 organization: \n<orgs>\n  <Organization o1>\n    <id>zp7mbokg</id>\n    <name>Marketing site</name>\n    <createdAt>2024-01-01T00:00:00.000Z</createdAt>\n    <members>3</members>\n  </Organization o1>\n  <Organization o1>\n    <id>abc123</id>\n    <name>Docs</name>\n    <createdAt>2024-02-01T00:00:00.000Z</createdAt>\n    <members>1</members>\n  </Organization o1>\n</orgs>\n";
    pub(crate) const SANITY_DATASETS: &str = "Here are the datasets: \n<datasets>\n  <production>\n    <name>production</name>\n    <aclMode>public</aclMode>\n    <createdAt>2024-01-01T00:00:00.000Z</createdAt>\n  </production>\n  <staging>\n    <name>staging</name>\n    <aclMode>private</aclMode>\n  </staging>\n</datasets>\n";
    pub(crate) const SANITY_QUERY: &str = "Query executed successfully. Found 1 total document, returning 1 (40 tokens): \n<documents>\n  <document>{\n  \"chTypes\": [\n    \"post\",\n    \"author\"\n  ],\n  \"html\": \"<p>a & b</p>\"\n}</document>\n</documents>\n<count>1</count>\n<totalAvailable>1</totalAvailable>\n<tokensUsed>40</tokensUsed>\n";

    fn text(t: &str) -> Value {
        json!({"content": [{"type": "text", "text": t}]})
    }

    #[test]
    fn sanitys_xml_answers_are_read() {
        let v = result_json("Sanity", &text(SANITY_PROJECTS)).unwrap();
        let p = &v["orgs"]["Organization o1"];
        assert_eq!(p[0]["id"], "zp7mbokg");
        assert_eq!(p[1]["name"], "Docs");
        let v = result_json("Sanity", &text(SANITY_DATASETS)).unwrap();
        assert_eq!(v["datasets"]["production"]["name"], "production");
        assert_eq!(v["datasets"]["staging"]["aclMode"], "private");
        let v = result_json("Sanity", &text(SANITY_QUERY)).unwrap();
        assert_eq!(find_key(&v, "chTypes").unwrap(), &json!(["post", "author"]));
        assert_eq!(
            find_key(&v, "html").unwrap(),
            "<p>a & b</p>",
            "markup inside a document's JSON is kept"
        );
        assert_eq!(v["count"], "1");
    }

    #[test]
    fn several_documents_and_a_lone_project_read_as_expected() {
        let two = "Found 2: \n<documents>\n  <document>{\"a\": 1}</document>\n  <document>{\"a\": 2}</document>\n</documents>\n";
        let v = result_json("Sanity", &text(two)).unwrap();
        assert_eq!(
            v["documents"]["document"][1]["a"], 2,
            "every document, not only the first"
        );
        let one = "Found 1 project in 1 organization: \n<orgs>\n  <Organization o9>\n    <id>p1</id>\n  </Organization o9>\n</orgs>\n";
        let v = result_json("Sanity", &text(one)).unwrap();
        assert_eq!(find_key(&v, "id").unwrap(), "p1");
        let escaped = "x: \n<a><b>1 &lt; 2 &amp;&amp; ok</b><c/></a>";
        let v = result_json("Sanity", &text(escaped)).unwrap();
        assert_eq!(v["a"]["b"], "1 < 2 && ok");
        assert_eq!(v["a"]["c"], "");
    }

    #[test]
    fn an_answer_split_over_several_items_is_read() {
        let r = json!({"content": [
            {"type": "text", "text": "Here are the datasets:"},
            {"type": "text", "text": "<datasets><production><name>production</name></production></datasets>"}
        ]});
        let v = result_json("Sanity", &r).unwrap();
        assert_eq!(v["datasets"]["production"]["name"], "production");
    }

    #[test]
    fn unreadable_answers_are_refused_and_logged_without_tokens() {
        for t in ["Done.", "<<<", "<a>unclosed", ""] {
            assert!(result_json("Sanity", &text(t)).is_err(), "{t:?}");
        }
        let r = text(
            "Authorization failed for Bearer skAbCdEfGhIjKlMnOpQrStUvWxYz0123456789 please retry",
        );
        let shape = answer_shape(&r);
        assert!(shape.contains("items=[text:"), "{shape}");
        assert!(!shape.contains("skAbCdEf"), "{shape}");
        let r = text("key zzfakeABCDEFGHIJKLMNOPQRSTUV and short id abc123");
        let shape = answer_shape(&r);
        assert!(!shape.contains("ABCDEFGHIJ"), "{shape}");
        assert!(shape.contains("abc123"), "short words stay: {shape}");
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
