//! A Streamable HTTP MCP client that speaks both eras of the protocol.
//!
//! Modern servers (revision 2026-07-28) are stateless: every request is one
//! POST carrying its protocol version, client info and capabilities in
//! `params._meta`, mirrored into `MCP-Protocol-Version`, `Mcp-Method` and
//! `Mcp-Name` headers (and `Mcp-Param-*` for tool parameters the server marks
//! with `x-mcp-header`). Legacy servers (2025-11-25 and earlier) want an
//! `initialize` handshake and an `Mcp-Session-Id`.
//!
//! As the spec describes, a modern request is tried first; a `4xx` whose body
//! is not a recognized modern error means a legacy server, and the client
//! falls back to `initialize`. The era found is remembered on the server's
//! record so later calls go straight to it (and re-probe if it stops
//! working). Replies may be plain JSON or an event stream; both are read.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::discovery::{parse_challenge, Challenge};
use super::http::{Http, HttpError, HttpRequest, HttpResponse, Limit, Method};

pub const MODERN_VERSION: &str = "2026-07-28";
pub const LEGACY_VERSION: &str = "2025-11-25";
/// Legacy versions a server may answer `initialize` with.
pub const LEGACY_ACCEPTED: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];
const CLIENT_NAME: &str = "coderhelm";
const CLIENT_VERSION: &str = "1.0.0";
const MAX_PAGES: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Era {
    Modern,
    Legacy,
}

impl Era {
    pub fn as_str(self) -> &'static str {
        match self {
            Era::Modern => "modern",
            Era::Legacy => "legacy",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "modern" => Some(Era::Modern),
            "legacy" => Some(Era::Legacy),
            _ => None,
        }
    }
}

/// One tool a server offers, with the hints it gives about what it does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteTool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub input_schema: Value,
    /// `annotations.readOnlyHint`, when the server gives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// `annotations.destructiveHint`, when the server gives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive: Option<bool>,
}

/// A tool the server listed that can't be offered, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedTool {
    pub name: String,
    pub reason: String,
}

/// What a tool call returned, as text for the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolOutput {
    pub text: String,
    pub is_error: bool,
}

/// Why a request to the server failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// `401`: no token, or the token isn't accepted.
    Unauthorized(Option<Challenge>),
    /// `403`: the token lacks a scope, or the key lacks access.
    Forbidden(Option<Challenge>),
    /// The request never got an answer, or our guard refused it.
    Unreachable(String),
    /// The server answered, but not with a usable reply.
    Protocol(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Unauthorized(_) => f.write_str("The server didn't accept the sign-in."),
            CallError::Forbidden(c) => match c.as_ref().and_then(|c| c.scope.as_deref()) {
                Some(s) => write!(f, "The server needs more access ({s})."),
                None => f.write_str("The server refused this request (forbidden)."),
            },
            CallError::Unreachable(m) | CallError::Protocol(m) => f.write_str(m),
        }
    }
}

impl From<HttpError> for CallError {
    fn from(e: HttpError) -> Self {
        CallError::Unreachable(e.to_string())
    }
}

/// How to sign each request: a header name and value (`Authorization:
/// Bearer ...` or a static key header). Never printed.
#[derive(Clone)]
pub struct AuthHeader(pub String, pub String);

impl std::fmt::Debug for AuthHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuthHeader({}, ..)", self.0)
    }
}

/// One server, at one address, signed one way.
pub struct Conn<'a> {
    pub http: &'a dyn Http,
    pub url: &'a str,
    pub auth: Option<AuthHeader>,
    /// The era found last time, if any.
    pub era: Option<Era>,
}

// ── Header values ──────────────────────────────────────────────────────

/// Encodes a value for an `Mcp-Name` or `Mcp-Param-*` header: plain when it
/// is safe visible ASCII, else the `=?base64?...?=` sentinel form.
pub fn header_value(v: &str) -> String {
    let safe = !v.is_empty()
        && v.bytes().all(|b| (0x20..=0x7e).contains(&b) || b == b'\t')
        && v.trim() == v
        && !(v.starts_with("=?base64?") && v.ends_with("?="));
    if safe {
        v.to_string()
    } else {
        format!("=?base64?{}?=", STANDARD.encode(v.as_bytes()))
    }
}

fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn count_marks(v: &Value) -> usize {
    match v {
        Value::Object(m) => {
            m.contains_key("x-mcp-header") as usize + m.values().map(count_marks).sum::<usize>()
        }
        Value::Array(a) => a.iter().map(count_marks).sum(),
        _ => 0,
    }
}

/// The `x-mcp-header` marks of a tool's input schema, as (header name,
/// property path). A mark anywhere but on a primitive property reached only
/// through `properties` keys makes the tool invalid (the spec says such a
/// tool must not be offered).
pub fn header_marks(schema: &Value) -> Result<Vec<(String, Vec<String>)>, String> {
    fn walk(
        node: &Value,
        path: &mut Vec<String>,
        out: &mut Vec<(String, Vec<String>)>,
    ) -> Result<(), String> {
        let Some(props) = node.get("properties").and_then(|p| p.as_object()) else {
            return Ok(());
        };
        for (k, child) in props {
            path.push(k.clone());
            if let Some(mark) = child.get("x-mcp-header") {
                let name = mark.as_str().unwrap_or("");
                if !is_token(name) {
                    return Err(format!("parameter {k} has an invalid header name"));
                }
                let ty = child.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if !matches!(ty, "string" | "integer" | "boolean") {
                    return Err(format!("parameter {k} is marked for a header but isn't a string, integer or boolean"));
                }
                out.push((name.to_string(), path.clone()));
            }
            walk(child, path, out)?;
            path.pop();
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(schema, &mut Vec::new(), &mut out)?;
    if out.len() != count_marks(schema) {
        return Err("a header mark sits somewhere other than a plain property".into());
    }
    let mut seen = std::collections::HashSet::new();
    for (n, _) in &out {
        if !seen.insert(n.to_ascii_lowercase()) {
            return Err(format!("header {n} is marked twice"));
        }
    }
    Ok(out)
}

/// The `Mcp-Param-*` headers for one call.
pub fn param_headers(schema: &Value, args: &Value) -> Vec<(String, String)> {
    let Ok(marks) = header_marks(schema) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (name, path) in marks {
        let mut v = args;
        for p in &path {
            v = &v[p.as_str()];
        }
        let text = match v {
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
            _ => continue,
        };
        out.push((format!("Mcp-Param-{name}"), header_value(&text)));
    }
    out
}

// ── Event streams ──────────────────────────────────────────────────────

/// The `data` of each complete event in an event stream.
pub fn sse_events(buf: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(buf).replace("\r\n", "\n");
    let mut out = Vec::new();
    let complete = match text.rfind("\n\n") {
        Some(i) => &text[..i + 2],
        None => return out,
    };
    for block in complete.split("\n\n") {
        let data: Vec<&str> = block
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|d| d.strip_prefix(' ').unwrap_or(d))
            .collect();
        if !data.is_empty() {
            out.push(data.join("\n"));
        }
    }
    out
}

fn is_response(v: &Value) -> bool {
    v.get("id").is_some_and(|i| !i.is_null())
        && (v.get("result").is_some() || v.get("error").is_some())
}

/// True once an event stream holds a JSON-RPC response (so reading can stop).
pub fn sse_has_response(buf: &[u8]) -> bool {
    sse_events(buf)
        .iter()
        .filter_map(|d| serde_json::from_str::<Value>(d).ok())
        .any(|v| is_response(&v))
}

/// The JSON-RPC response with `id` in a reply, JSON or event stream.
pub fn find_response(r: &HttpResponse, id: i64) -> Option<Value> {
    let matches = |v: &Value| is_response(v) && v["id"].as_i64() == Some(id);
    if r.is_sse() {
        sse_events(&r.body)
            .iter()
            .filter_map(|d| serde_json::from_str::<Value>(d).ok())
            .find(matches)
    } else {
        let v = r.json()?;
        if let Some(a) = v.as_array() {
            return a.iter().find(|x| matches(x)).cloned();
        }
        matches(&v).then_some(v)
    }
}

// ── Requests ───────────────────────────────────────────────────────────

const MODERN_ERRORS: &[i64] = &[-32020, -32021, -32022];

fn rpc_error_text(e: &Value) -> String {
    let code = e["code"].as_i64().unwrap_or(0);
    let msg = e["message"].as_str().unwrap_or("error");
    format!(
        "The server answered with an error ({code}: {}).",
        crate::truncate_str(msg, 300)
    )
}

fn challenge_of(r: &HttpResponse) -> Option<Challenge> {
    r.header("www-authenticate").and_then(parse_challenge)
}

fn auth_failure(r: &HttpResponse) -> Option<CallError> {
    match r.status {
        401 => Some(CallError::Unauthorized(challenge_of(r))),
        403 => Some(CallError::Forbidden(challenge_of(r))),
        _ => None,
    }
}

enum Modern {
    Done(Value),
    /// Not a modern server: use `initialize`.
    Legacy,
}

impl Conn<'_> {
    fn base(&self, body: &Value) -> HttpRequest {
        let mut r = HttpRequest::json(self.url, body, Limit::Mcp)
            .header("Accept", "application/json, text/event-stream");
        if let Some(AuthHeader(k, v)) = &self.auth {
            r = r.header(k, v);
        }
        r
    }

    async fn modern(
        &self,
        method: &str,
        mut params: Value,
        extra: &[(String, String)],
    ) -> Result<Modern, CallError> {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": MODERN_VERSION,
            "io.modelcontextprotocol/clientInfo": {"name": CLIENT_NAME, "version": CLIENT_VERSION},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let mut req = self
            .base(&body)
            .header("MCP-Protocol-Version", MODERN_VERSION)
            .header("Mcp-Method", method);
        for (k, v) in extra {
            req = req.header(k, v);
        }
        let r = self.http.send(req).await?;
        if let Some(e) = auth_failure(&r) {
            return Err(e);
        }
        if (200..300).contains(&r.status) {
            let resp = find_response(&r, 1).ok_or_else(|| {
                CallError::Protocol("The server's reply had no answer in it.".into())
            })?;
            if let Some(e) = resp.get("error") {
                return Err(CallError::Protocol(rpc_error_text(e)));
            }
            return Ok(Modern::Done(resp["result"].clone()));
        }
        if matches!(r.status, 400 | 404 | 405) {
            let err = r.json().and_then(|v| v.get("error").cloned());
            let code = err.as_ref().and_then(|e| e["code"].as_i64());
            let modern = code.is_some_and(|c| MODERN_ERRORS.contains(&c))
                || (r.status == 404 && code == Some(-32601));
            if !modern || self.era == Some(Era::Legacy) {
                return Ok(Modern::Legacy);
            }
            let err = err.unwrap_or_default();
            if code == Some(-32022) {
                // The server is modern but wants another version: if it
                // names only versions from before 2026, it also speaks legacy.
                let supported: Vec<String> = err["data"]["supported"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| s.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                if supported
                    .iter()
                    .any(|s| LEGACY_ACCEPTED.contains(&s.as_str()))
                {
                    return Ok(Modern::Legacy);
                }
            }
            return Err(CallError::Protocol(rpc_error_text(&err)));
        }
        Err(CallError::Protocol(format!(
            "The server answered {} to an MCP request.",
            r.status
        )))
    }

    async fn legacy(&self, method: &str, params: Value) -> Result<Value, CallError> {
        let init = json!({
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {
                "protocolVersion": LEGACY_VERSION,
                "capabilities": {},
                "clientInfo": {"name": CLIENT_NAME, "version": CLIENT_VERSION},
            }
        });
        let r = self.http.send(self.base(&init)).await?;
        if let Some(e) = auth_failure(&r) {
            return Err(e);
        }
        if !(200..300).contains(&r.status) {
            return Err(CallError::Protocol(format!(
                "The server answered {} when Coderhelm said hello. Check that this is the MCP endpoint (often ending in /mcp).",
                r.status
            )));
        }
        let resp = find_response(&r, 0)
            .ok_or_else(|| CallError::Protocol("The server didn't answer the hello.".into()))?;
        if let Some(e) = resp.get("error") {
            return Err(CallError::Protocol(rpc_error_text(e)));
        }
        let version = resp["result"]["protocolVersion"]
            .as_str()
            .unwrap_or(LEGACY_VERSION)
            .to_string();
        if !LEGACY_ACCEPTED.contains(&version.as_str()) {
            return Err(CallError::Protocol(format!(
                "The server speaks MCP version {version}, which Coderhelm doesn't support yet."
            )));
        }
        let session = r.header("mcp-session-id").map(str::to_string);
        let with_session = |mut req: HttpRequest| {
            req = req.header("MCP-Protocol-Version", &version);
            if let Some(s) = &session {
                req = req.header("Mcp-Session-Id", s);
            }
            req
        };
        // The server may not care about this notification; it's sent anyway.
        let _ = self
            .http
            .send(with_session(self.base(
                &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )))
            .await;
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let r = self.http.send(with_session(self.base(&body))).await?;
        if let Some(s) = &session {
            // Ends the session; a failure here changes nothing.
            let mut del = HttpRequest::get(self.url);
            del.method = Method::Delete;
            del.headers.clear();
            let del = with_session(match &self.auth {
                Some(AuthHeader(k, v)) => del.header(k, v),
                None => del,
            });
            let _ = s;
            let _ = self.http.send(del).await;
        }
        if let Some(e) = auth_failure(&r) {
            return Err(e);
        }
        if !(200..300).contains(&r.status) {
            return Err(CallError::Protocol(format!(
                "The server answered {} to {method}.",
                r.status
            )));
        }
        let resp = find_response(&r, 1)
            .ok_or_else(|| CallError::Protocol("The server's reply had no answer in it.".into()))?;
        if let Some(e) = resp.get("error") {
            return Err(CallError::Protocol(rpc_error_text(e)));
        }
        Ok(resp["result"].clone())
    }

    /// Sends one request in whichever era the server speaks.
    pub async fn request(
        &self,
        method: &str,
        params: Value,
        extra: &[(String, String)],
    ) -> Result<(Value, Era), CallError> {
        if self.era != Some(Era::Legacy) {
            match self.modern(method, params.clone(), extra).await? {
                Modern::Done(v) => return Ok((v, Era::Modern)),
                Modern::Legacy => {}
            }
        }
        match self.legacy(method, params.clone()).await {
            Ok(v) => Ok((v, Era::Legacy)),
            // Remembered as legacy but no longer: try modern once.
            Err(CallError::Protocol(_)) if self.era == Some(Era::Legacy) => {
                match (Conn {
                    http: self.http,
                    url: self.url,
                    auth: self.auth.clone(),
                    era: None,
                })
                .modern(method, params, extra)
                .await?
                {
                    Modern::Done(v) => Ok((v, Era::Modern)),
                    Modern::Legacy => Err(CallError::Protocol(
                        "The server stopped answering MCP requests.".into(),
                    )),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Every tool the server lists (following pages), plus the ones that
    /// can't be offered.
    pub async fn list_tools(&self) -> Result<(Vec<RemoteTool>, Vec<SkippedTool>, Era), CallError> {
        let mut tools = Vec::new();
        let mut skipped = Vec::new();
        let mut cursor: Option<String> = None;
        let mut era = self.era;
        for _ in 0..MAX_PAGES {
            let params = match &cursor {
                Some(c) => json!({"cursor": c}),
                None => json!({}),
            };
            let conn = Conn {
                http: self.http,
                url: self.url,
                auth: self.auth.clone(),
                era,
            };
            let (result, e) = conn.request("tools/list", params, &[]).await?;
            era = Some(e);
            for t in result["tools"].as_array().cloned().unwrap_or_default() {
                match parse_tool(&t) {
                    Ok(tool) => tools.push(tool),
                    Err(s) => skipped.push(s),
                }
            }
            cursor = result["nextCursor"]
                .as_str()
                .filter(|c| !c.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        Ok((tools, skipped, era.unwrap_or(Era::Modern)))
    }

    /// Calls one tool.
    pub async fn call_tool(
        &self,
        tool: &RemoteTool,
        args: &Value,
    ) -> Result<(ToolOutput, Era), CallError> {
        let mut extra = vec![("Mcp-Name".to_string(), header_value(&tool.name))];
        extra.extend(param_headers(&tool.input_schema, args));
        let args = if args.is_object() {
            args.clone()
        } else {
            json!({})
        };
        let (result, era) = self
            .request(
                "tools/call",
                json!({"name": tool.name, "arguments": args}),
                &extra,
            )
            .await?;
        match result["resultType"].as_str() {
            None | Some("complete") => {}
            Some("input_required") => {
                return Err(CallError::Protocol(
                    "The tool asked for input Coderhelm can't give (a question or an extra sign-in step).".into(),
                ))
            }
            Some(other) => {
                return Err(CallError::Protocol(format!(
                    "The server sent a reply of a type Coderhelm doesn't know ({other})."
                )))
            }
        }
        Ok((output_of(&result), era))
    }
}

/// Reads one tool from a `tools/list` result.
pub fn parse_tool(t: &Value) -> Result<RemoteTool, SkippedTool> {
    let name = t["name"].as_str().unwrap_or("").to_string();
    if name.is_empty() {
        return Err(SkippedTool {
            name: "(no name)".into(),
            reason: "the server listed a tool without a name".into(),
        });
    }
    let schema = match t.get("inputSchema") {
        Some(s) if s.is_object() => s.clone(),
        _ => json!({"type": "object", "properties": {}}),
    };
    if let Err(why) = header_marks(&schema) {
        return Err(SkippedTool { name, reason: why });
    }
    let hint = |k: &str| t["annotations"][k].as_bool();
    Ok(RemoteTool {
        title: t["title"]
            .as_str()
            .or(t["annotations"]["title"].as_str())
            .map(str::to_string),
        description: t["description"].as_str().unwrap_or("").to_string(),
        input_schema: schema,
        read_only: hint("readOnlyHint"),
        destructive: hint("destructiveHint"),
        name,
    })
}

/// A tool result as text.
pub fn output_of(result: &Value) -> ToolOutput {
    let mut parts = Vec::new();
    for c in result["content"].as_array().cloned().unwrap_or_default() {
        match c["type"].as_str().unwrap_or("") {
            "text" => parts.push(c["text"].as_str().unwrap_or("").to_string()),
            "resource" => {
                let r = &c["resource"];
                parts.push(
                    r["text"].as_str().map(str::to_string).unwrap_or_else(|| {
                        format!("[resource {}]", r["uri"].as_str().unwrap_or(""))
                    }),
                )
            }
            "resource_link" => parts.push(format!(
                "[link {} {}]",
                c["name"].as_str().unwrap_or(""),
                c["uri"].as_str().unwrap_or("")
            )),
            "image" => parts.push("[image omitted]".into()),
            "audio" => parts.push("[audio omitted]".into()),
            _ => {}
        }
    }
    if parts.is_empty() {
        if let Some(s) = result.get("structuredContent") {
            parts.push(s.to_string());
        }
    }
    ToolOutput {
        text: parts.join("\n"),
        is_error: result["isError"].as_bool().unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::super::http::fake::{json as fjson, resp, Fake};
    use super::*;

    const URL: &str = "https://mcp.example.com/mcp";

    fn modern_tools() -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": [
            {"name": "list_issues", "description": "List", "inputSchema": {"type": "object"}, "annotations": {"readOnlyHint": true}},
            {"name": "create_issue", "inputSchema": {"type": "object", "properties": {"team": {"type": "string", "x-mcp-header": "Team"}}}, "annotations": {"readOnlyHint": false, "destructiveHint": false}},
            {"name": "bad", "inputSchema": {"type": "object", "properties": {"xs": {"type": "array", "items": {"type": "string", "x-mcp-header": "X"}}}}}
        ]}})
    }

    fn conn<'a>(f: &'a Fake, era: Option<Era>) -> Conn<'a> {
        Conn {
            http: f,
            url: URL,
            auth: Some(AuthHeader("Authorization".into(), "Bearer tok".into())),
            era,
        }
    }

    #[tokio::test]
    async fn a_modern_server_gets_meta_and_headers() {
        let f = Fake::default();
        f.with(|r| {
            (r.header_value("Mcp-Method") == Some("tools/list")).then(|| fjson(200, modern_tools()))
        });
        let (tools, skipped, era) = conn(&f, None).list_tools().await.unwrap();
        assert_eq!(era, Era::Modern);
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].read_only, Some(true));
        assert_eq!(tools[1].read_only, Some(false));
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "bad");
        let sent = &f.sent()[0];
        assert_eq!(
            sent.header_value("MCP-Protocol-Version"),
            Some(MODERN_VERSION)
        );
        assert_eq!(sent.header_value("Authorization"), Some("Bearer tok"));
        assert!(sent
            .header_value("Accept")
            .unwrap()
            .contains("text/event-stream"));
        let body: Value = serde_json::from_slice(sent.body.as_ref().unwrap()).unwrap();
        assert_eq!(
            body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            MODERN_VERSION
        );
        assert!(body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"].is_object());
    }

    #[tokio::test]
    async fn tool_calls_mirror_name_and_marked_params() {
        let f = Fake::default();
        f.with(|r| {
            (r.header_value("Mcp-Method") == Some("tools/call")).then(|| {
                resp(
                    200,
                    &[("content-type", "text/event-stream")],
                    "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"made it\"}]}}\n\n",
                )
            })
        });
        let tool = parse_tool(&modern_tools()["result"]["tools"][1]).unwrap();
        let (out, _) = conn(&f, Some(Era::Modern))
            .call_tool(&tool, &json!({"team": "Plätform"}))
            .await
            .unwrap();
        assert_eq!(out.text, "made it");
        let sent = &f.sent()[0];
        assert_eq!(sent.header_value("Mcp-Name"), Some("create_issue"));
        assert_eq!(
            sent.header_value("Mcp-Param-Team"),
            Some(header_value("Plätform").as_str())
        );
        assert!(sent
            .header_value("Mcp-Param-Team")
            .unwrap()
            .starts_with("=?base64?"));
    }

    #[tokio::test]
    async fn a_legacy_server_falls_back_to_initialize() {
        let f = Fake::default();
        f.with(|r| {
            let body: Value = serde_json::from_slice(r.body.as_ref()?).ok()?;
            match body["method"].as_str()? {
                "initialize" => Some(resp(
                    200,
                    &[("content-type", "application/json"), ("mcp-session-id", "sess-1")],
                    &json!({"jsonrpc": "2.0", "id": 0, "result": {"protocolVersion": "2025-06-18", "capabilities": {}}}).to_string(),
                )),
                "notifications/initialized" => Some(resp(202, &[], "")),
                "tools/list" if r.header_value("Mcp-Session-Id") == Some("sess-1") => {
                    Some(fjson(200, json!({"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "t1"}]}})))
                }
                // A legacy server rejects the modern request with a bare 400.
                "tools/list" => Some(resp(400, &[], "Bad Request: no session")),
                _ => None,
            }
        });
        let (tools, _, era) = conn(&f, None).list_tools().await.unwrap();
        assert_eq!(era, Era::Legacy);
        assert_eq!(tools[0].name, "t1");
        let list = f
            .sent()
            .into_iter()
            .find(|r| {
                r.header_value("Mcp-Session-Id").is_some() && r.body_text().contains("tools/list")
            })
            .unwrap();
        assert_eq!(
            list.header_value("MCP-Protocol-Version"),
            Some("2025-06-18")
        );
        // The session is closed afterwards.
        assert!(f.sent().iter().any(|r| r.method == Method::Delete));
    }

    #[tokio::test]
    async fn a_modern_error_is_not_mistaken_for_legacy() {
        let f = Fake::default();
        f.with(|_| Some(fjson(400, json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32020, "message": "Header mismatch"}}))));
        let e = conn(&f, None).list_tools().await.unwrap_err();
        assert!(matches!(e, CallError::Protocol(m) if m.contains("-32020")));
        assert!(f
            .sent()
            .iter()
            .all(|r| !r.body_text().contains("initialize")));
    }

    #[tokio::test]
    async fn auth_failures_carry_the_challenge() {
        let f = Fake::default();
        f.with(|_| Some(resp(401, &[("www-authenticate", "Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\"")], "")));
        match conn(&f, None).list_tools().await.unwrap_err() {
            CallError::Unauthorized(Some(c)) => assert!(c.resource_metadata.is_some()),
            other => panic!("{other:?}"),
        }
        let f = Fake::default();
        f.with(|_| {
            Some(resp(
                403,
                &[(
                    "www-authenticate",
                    "Bearer error=\"insufficient_scope\", scope=\"write\"",
                )],
                "",
            ))
        });
        let e = conn(&f, None).list_tools().await.unwrap_err();
        assert!(e.to_string().contains("write"));
    }

    #[tokio::test]
    async fn input_required_results_are_not_passed_off_as_answers() {
        let f = Fake::default();
        f.with(|_| Some(fjson(200, json!({"jsonrpc": "2.0", "id": 1, "result": {"resultType": "input_required", "inputRequests": {}}}))));
        let tool = RemoteTool {
            name: "x".into(),
            title: None,
            description: String::new(),
            input_schema: json!({}),
            read_only: None,
            destructive: None,
        };
        assert!(conn(&f, Some(Era::Modern))
            .call_tool(&tool, &json!({}))
            .await
            .is_err());
    }

    #[test]
    fn header_values_are_encoded_when_unsafe() {
        assert_eq!(header_value("us-west1"), "us-west1");
        assert_eq!(
            header_value("Hello, 世界"),
            "=?base64?SGVsbG8sIOS4lueVjA==?="
        );
        assert_eq!(header_value(" padded "), "=?base64?IHBhZGRlZCA=?=");
        assert_eq!(header_value("line1\nline2"), "=?base64?bGluZTEKbGluZTI=?=");
        assert_eq!(
            header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
    }

    #[test]
    fn header_marks_follow_the_rules() {
        let ok = json!({"type": "object", "properties": {"a": {"type": "object", "properties": {"region": {"type": "string", "x-mcp-header": "Region"}}}}});
        assert_eq!(
            header_marks(&ok).unwrap(),
            vec![(
                "Region".to_string(),
                vec!["a".to_string(), "region".to_string()]
            )]
        );
        assert_eq!(
            param_headers(&ok, &json!({"a": {"region": "eu"}})),
            vec![("Mcp-Param-Region".to_string(), "eu".to_string())]
        );
        assert!(param_headers(&ok, &json!({})).is_empty());
        for bad in [
            json!({"properties": {"n": {"type": "number", "x-mcp-header": "N"}}}),
            json!({"properties": {"n": {"type": "string", "x-mcp-header": "bad name"}}}),
            json!({"properties": {"a": {"type": "string", "x-mcp-header": "X"}, "b": {"type": "string", "x-mcp-header": "x"}}}),
            json!({"anyOf": [{"properties": {"n": {"type": "string", "x-mcp-header": "N"}}}]}),
        ] {
            assert!(header_marks(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn event_streams_are_read_only_when_complete() {
        assert!(!sse_has_response(
            b"data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}"
        ));
        assert!(sse_has_response(
            b": keepalive\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\r\n\r\n"
        ));
        assert!(!sse_has_response(
            b"data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n"
        ));
        assert_eq!(sse_events(b"data: a\ndata: b\n\n"), vec!["a\nb"]);
    }

    #[test]
    fn outputs_become_text() {
        let o = output_of(
            &json!({"content": [{"type": "text", "text": "a"}, {"type": "image", "data": "..."}], "isError": true}),
        );
        assert_eq!(o.text, "a\n[image omitted]");
        assert!(o.is_error);
        assert_eq!(
            output_of(&json!({"structuredContent": {"n": 1}})).text,
            "{\"n\":1}"
        );
    }
}
