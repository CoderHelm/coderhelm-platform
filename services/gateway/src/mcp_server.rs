//! CoderHelm's MCP server: `/mcp`, Streamable HTTP, for Claude, Cursor,
//! VS Code and any other MCP client a person adds CoderHelm to.
//!
//! Each tool is a dashboard action, and runs as one: a tool call becomes a
//! request to the same API route the dashboard calls (`/api/runs`,
//! `/api/runs/:id/retry`, `/api/reviewer/re-review`, ...), sent through the
//! same router and the same auth middleware, as the person who connected
//! the app. So a call is checked exactly like that person's click: their
//! membership and current role (custom roles included), the team's
//! two-factor rule, the capability the route belongs to, and the route's own
//! role check. On top, the connection's scopes: `runs:read` reaches only the
//! routes the reading tools call, `runs:write` adds the routes of the tools
//! that act ([`decide`]). Every call is written to the team's log of access
//! changes before it runs.
//!
//! The server is stateless: no `Mcp-Session-Id`, answers are JSON (never a
//! stream), and `GET /mcp` answers 405 (no server-sent events).

use axum::{
    body::{Body, Bytes},
    extract::{Extension, State},
    http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tower::ServiceExt;
use tracing::{error, warn};

use crate::mcp_oauth::{self, AccessRefusal, StoredGrant, READ_SCOPE, WRITE_SCOPE};
use crate::models::Claims;
use crate::permissions::{self, Access, Capability, Holder};
use crate::routes::roles::{self, AuditEntry};
use crate::AppState;

/// The protocol versions this server speaks, newest first.
pub const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];

/// The routes the reading tools call. A token with `runs:read` reaches
/// these and nothing else.
pub const READ_HANDLERS: &[&str] = &[
    "api::list_runs",
    "api::get_run",
    "reviewer::list_reviews",
    "reviewer::get_review",
];

/// The routes the acting tools call, reached with `runs:write`.
pub const WRITE_HANDLERS: &[&str] = &[
    "run_start::start_run",
    "api::retry_run",
    "plans::create_plan",
    "plans::approve_all_and_execute",
    "reviewer::re_review",
];

/// The API router the tools call through, as main() builds it.
#[derive(Clone)]
pub struct McpApi(pub Router);

/// Who a tool call's API request is for: set by [`call`] on the requests it
/// makes, and only there (a request from outside never carries it). The
/// auth middleware checks it like a session (`middleware::auth`).
#[derive(Clone, Debug)]
pub struct McpCaller {
    /// The person, from their member record now. `iat` is when they
    /// connected the app: sessions ended after that end the connection too.
    pub claims: Claims,
    pub grant: StoredGrant,
    pub scopes: BTreeSet<&'static str>,
    /// The routes the tool being called may reach: a call reaches nothing
    /// another tool would.
    pub tool_handlers: &'static [&'static str],
}

/// Why an app's request to a route was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// No tool calls this route.
    NotForApps,
    /// The connection wasn't given this scope.
    Scope(&'static str),
    /// The person's role doesn't include it.
    Role(&'static Capability),
}

impl Refusal {
    pub fn response(self) -> Response {
        let (code, msg) = match self {
            Refusal::NotForApps => (
                "not_available_to_apps",
                "Connected apps can't call this part of CoderHelm.".to_string(),
            ),
            Refusal::Scope(s) => (
                "insufficient_scope",
                format!("This connection wasn't allowed to do this. Connect the app again and allow \"{}\".", mcp_oauth::scope_label(s)),
            ),
            Refusal::Role(c) => (
                "missing_capability",
                format!(
                    "Your role doesn't include this ({}). Ask an admin or owner if you need it.",
                    c.label
                ),
            ),
        };
        (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": msg, "code": code })),
        )
            .into_response()
    }
}

/// The scope a route needs, when a tool calls it.
pub fn scope_for_handler(handler: &str) -> Option<&'static str> {
    if READ_HANDLERS.contains(&handler) {
        Some(READ_SCOPE)
    } else if WRITE_HANDLERS.contains(&handler) {
        Some(WRITE_SCOPE)
    } else {
        None
    }
}

/// The decision for one of an app's requests: the route must be one the
/// tool being called calls, the connection must hold its scope, and the person's role must
/// allow it exactly as it would in the dashboard (`resolve_access`). On
/// success, the built-in role the handler runs as.
pub fn decide(
    scopes: &BTreeSet<&'static str>,
    tool_handlers: &[&str],
    holder: &Holder,
    handler: Option<&str>,
) -> Result<&'static str, Refusal> {
    let handler = handler.ok_or(Refusal::NotForApps)?;
    if !tool_handlers.contains(&handler) {
        return Err(Refusal::NotForApps);
    }
    let scope = scope_for_handler(handler).ok_or(Refusal::NotForApps)?;
    if !scopes.contains(scope) {
        return Err(Refusal::Scope(scope));
    }
    match permissions::resolve_access(holder, Some(handler)) {
        Access::Allow { effective_role } => Ok(effective_role),
        Access::Deny { capability } => Err(Refusal::Role(capability)),
    }
}

// ── Tools ───────────────────────────────────────────────────────────

/// One tool, and the routes it may call.
pub struct Tool {
    pub name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    /// Needs `runs:write`; otherwise `runs:read`.
    pub write: bool,
    /// Every route the tool can call.
    pub handlers: &'static [&'static str],
    schema: fn() -> Value,
}

impl Tool {
    pub fn scope(&self) -> &'static str {
        if self.write {
            WRITE_SCOPE
        } else {
            READ_SCOPE
        }
    }

    fn listing(&self) -> Value {
        json!({
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "inputSchema": (self.schema)(),
            "annotations": {
                "title": self.title,
                "readOnlyHint": !self.write,
                // Nothing here deletes or overwrites: starting, retrying and
                // reviewing add work.
                "destructiveHint": false,
                "idempotentHint": !self.write,
                // The acting tools open issues, branches and pull requests
                // on GitHub; the reading ones stay inside CoderHelm.
                "openWorldHint": self.write,
            },
        })
    }
}

fn obj(props: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": false,
    })
}

pub const TOOLS: &[Tool] = &[
    Tool {
        name: "list_runs",
        title: "List runs",
        description: "Lists the team's CoderHelm runs, newest first: status, ticket, repository, pull request and a link to each in the dashboard. Filter by status, repository and start time.",
        write: false,
        handlers: &["api::list_runs"],
        schema: || {
            obj(
                json!({
                    "status": { "type": "string", "description": "Only runs in these statuses, comma separated: pending, running, awaiting_ci, completed, failed, needs_input, needs_outside_change, covered_by_pr, cancelled, retried, closed." },
                    "repo": { "type": "string", "description": "Only runs in this repository, owner/name." },
                    "since": { "type": "string", "description": "Only runs started at or after this time (RFC 3339, or a date like 2026-11-01)." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Most runs to return. Default 20." },
                    "cursor": { "type": "string", "description": "next from an earlier answer, to read older runs." }
                }),
                &[],
            )
        },
    },
    Tool {
        name: "get_run",
        title: "Get a run",
        description: "One run in full: its status and current step, the steps it went through, its pull requests, what it is waiting on (holds and open questions), its error if it stopped, and its latest notes.",
        write: false,
        handlers: &["api::get_run"],
        schema: || {
            obj(
                json!({ "run_id": { "type": "string", "description": "The run's id, from list_runs." } }),
                &["run_id"],
            )
        },
    },
    Tool {
        name: "start_run",
        title: "Start a run",
        description: "Starts CoderHelm on a piece of work. Give either a ticket (a GitHub issue URL such as https://github.com/acme/web/issues/42, or a Jira key such as WEB-42 that CoderHelm has seen before), or a task in plain words with the repository (owner/name) it is for. CoderHelm opens a pull request when it is done.",
        write: true,
        handlers: &[
            "run_start::start_run",
            "plans::create_plan",
            "plans::approve_all_and_execute",
        ],
        schema: || {
            obj(
                json!({
                    "ticket": { "type": "string", "description": "A GitHub issue URL, owner/name#number, or a Jira key." },
                    "task": { "type": "string", "description": "What to do, in plain words. Needs repo." },
                    "repo": { "type": "string", "description": "The repository for task, owner/name." },
                    "title": { "type": "string", "description": "A short title for task. Default: its first line." }
                }),
                &[],
            )
        },
    },
    Tool {
        name: "retry_run",
        title: "Retry a run",
        description: "Starts a run's ticket again, the same as Retry in the dashboard. Works on runs that failed, need input, were cancelled or closed; a run whose last fix round didn't finish resumes that round.",
        write: true,
        handlers: &["api::retry_run"],
        schema: || {
            obj(
                json!({ "run_id": { "type": "string", "description": "The run's id, from list_runs." } }),
                &["run_id"],
            )
        },
    },
    Tool {
        name: "list_reviews",
        title: "List reviews",
        description: "Lists CoderHelm's pull request reviews, newest first: repository, pull request, verdict, risk and how many findings each has.",
        write: false,
        handlers: &["reviewer::list_reviews"],
        schema: || {
            obj(
                json!({
                    "repo": { "type": "string", "description": "Only reviews in this repository, owner/name." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Most reviews to return. Default 20." },
                    "cursor": { "type": "string", "description": "next from an earlier answer, to read older reviews." }
                }),
                &[],
            )
        },
    },
    Tool {
        name: "get_review",
        title: "Get a review",
        description: "One review in full, with its findings. Give review_id from list_reviews, or a pull request URL for that pull request's latest review.",
        write: false,
        handlers: &["reviewer::get_review", "reviewer::list_reviews"],
        schema: || {
            obj(
                json!({
                    "review_id": { "type": "string", "description": "review_id from list_reviews." },
                    "pr_url": { "type": "string", "description": "A pull request URL, such as https://github.com/acme/web/pull/318." }
                }),
                &[],
            )
        },
    },
    Tool {
        name: "request_review",
        title: "Ask for a review",
        description: "Asks CoderHelm's reviewer to review a pull request's current head now, the same as Re-review in the dashboard. The review is posted on the pull request in a few minutes.",
        write: true,
        handlers: &["reviewer::re_review"],
        schema: || {
            obj(
                json!({ "pr_url": { "type": "string", "description": "The pull request, such as https://github.com/acme/web/pull/318 or acme/web#318." } }),
                &["pr_url"],
            )
        },
    },
];

pub fn find_tool(name: &str) -> Option<&'static Tool> {
    TOOLS.iter().find(|t| t.name == name)
}

// ── What the tools read from their arguments ────────────────────────

/// `owner/name` and a number from a GitHub URL (`.../issues/N` or
/// `.../pull/N`) or `owner/name#N`.
pub fn parse_github_ref(raw: &str, kind: &str) -> Option<(String, u64)> {
    let raw = raw.trim();
    if let Some((repo, num)) = raw.split_once('#') {
        let n: u64 = num.parse().ok()?;
        return valid_repo(repo)
            .then(|| (repo.to_string(), n))
            .filter(|_| n > 0);
    }
    let u = url::Url::parse(raw).ok()?;
    if u.host_str() != Some("github.com") && u.host_str() != Some("www.github.com") {
        return None;
    }
    let parts: Vec<&str> = u.path_segments()?.filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        [owner, name, k, num, ..] if *k == kind => {
            let repo = format!("{owner}/{name}");
            let n: u64 = num.parse().ok()?;
            (valid_repo(&repo) && n > 0).then_some((repo, n))
        }
        _ => None,
    }
}

pub fn valid_repo(repo: &str) -> bool {
    let Some((o, n)) = repo.split_once('/') else {
        return false;
    };
    let ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 100
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    ok(o) && ok(n)
}

/// A time from `since`: RFC 3339, or a date (midnight UTC).
pub fn parse_since(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let raw = raw.trim();
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(t.with_timezone(&chrono::Utc));
    }
    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc())
}

fn arg<'a>(args: &'a Value, k: &str) -> Option<&'a str> {
    args.get(k)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn q(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// A short account of a call's arguments for the log. Never a task's text.
pub fn audit_detail(tool: &str, args: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    for k in [
        "run_id",
        "ticket",
        "repo",
        "status",
        "since",
        "pr_url",
        "review_id",
    ] {
        if let Some(v) = arg(args, k) {
            parts.push(format!("{k} {}", common::truncate_str(v, 120)));
        }
    }
    if arg(args, "task").is_some() {
        parts.push("a task in words".into());
    }
    if parts.is_empty() {
        tool.to_string()
    } else {
        format!("{tool}: {}", parts.join(", "))
    }
}

// ── Calling the API as the person ───────────────────────────────────

/// A refused or failed API request, as a tool shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    pub status: u16,
    pub message: String,
}

fn default_message(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 => "CoderHelm couldn't do that with what was sent. Check the arguments.",
        401 => "This connection no longer works. Sign in to CoderHelm again from the app.",
        403 => "Your role in CoderHelm doesn't allow this.",
        404 => "CoderHelm found nothing like that in your team.",
        409 => "Something is already in progress for this.",
        429 => "The team has used its token limit for now. An admin can raise it in Settings.",
        _ => "Something went wrong on CoderHelm's side. Try again shortly.",
    }
}

/// One request to the API router as `caller`.
pub async fn call(
    api: &McpApi,
    caller: &McpCaller,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Result<Value, CallError> {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .map_err(|e| CallError {
            status: 500,
            message: e.to_string(),
        })?;
    req.extensions_mut().insert(caller.clone());
    let res = match api.0.clone().oneshot(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if status.is_success() {
        return Ok(value);
    }
    let message = value
        .get("error")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| value.get("reason").and_then(Value::as_str))
        .unwrap_or_else(|| default_message(status))
        .to_string();
    Err(CallError {
        status: status.as_u16(),
        message,
    })
}

fn link(path: &str) -> String {
    format!("{}{path}", common::public_urls::app_url_from_env())
}

/// A run in a list, as a tool shows it.
fn run_line(r: &Value) -> Value {
    let id = r["run_id"].as_str().unwrap_or("");
    json!({
        "run_id": id,
        "status": r["status"],
        "title": r["title"],
        "ticket": r["ticket_id"],
        "repo": r["repo"],
        "pull_request": r["pr_url"],
        "current_step": r["current_pass"],
        "started_by": r["trigger"]["label"],
        "created_at": r["created_at"],
        "url": link(&format!("/runs/detail?id={}", q(id))),
    })
}

fn keep_set(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::String(s) => !s.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

/// A run in full, as `get_run` shows it: what someone deciding what to do
/// next needs, without the run's internals.
pub fn run_detail(r: &Value) -> Value {
    let id = r["run_id"].as_str().unwrap_or("");
    let mut holds = serde_json::Map::new();
    for k in [
        "waiting_on",
        "draft_hold",
        "github_wait",
        "policy_approval",
        "round_incomplete",
        "codegen_env_missing",
        "sign_in_prompts",
        "tooling_failure",
    ] {
        if keep_set(&r[k]) {
            holds.insert(k.to_string(), r[k].clone());
        }
    }
    let open_questions: Vec<Value> = r["questions"]
        .as_array()
        .map(|qs| {
            qs.iter()
                .filter(|q| !q["answered"].as_bool().unwrap_or(false))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    if !open_questions.is_empty() {
        holds.insert("questions".into(), Value::Array(open_questions));
    }
    let notes = r["progress_notes"].as_array().cloned().unwrap_or_default();
    let latest: Vec<Value> = notes.iter().rev().take(15).rev().cloned().collect();
    let mut out = json!({
        "run_id": id,
        "status": r["status"],
        "title": r["title"],
        "ticket": r["ticket_id"],
        "ticket_source": r["ticket_source"],
        "repo": r["repo"],
        "branch": r["branch"],
        "current_step": r["current_pass"],
        "started_by": r["trigger"],
        "pull_requests": if keep_set(&r["prs"]) { r["prs"].clone() } else { r["pr_urls"].clone() },
        "pull_request": r["pr_url"],
        "steps": r["pass_history"],
        "holds": holds,
        "error": r["error"],
        "stop_kind": r["stop_kind"],
        "policy_note": r["policy_note"],
        "outside_change_report": r["outside_change_report"],
        "covered_by": r["covered_by"],
        "files_changed": r["files_modified"],
        "log_summary": { "notes_total": notes.len(), "latest_notes": latest },
        "cost_usd": r["cost_usd"],
        "tokens_in": r["tokens_in"],
        "tokens_out": r["tokens_out"],
        "duration_s": r["duration_s"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "closed_by": r["closed_by"],
        "close_reason": r["close_reason"],
        "continued_run_id": r["continued_run_id"],
        "url": link(&format!("/runs/detail?id={}", q(id))),
    });
    if let Some(o) = out.as_object_mut() {
        o.retain(|_, v| keep_set(v) || v.is_number() || v.is_boolean());
    }
    out
}

fn review_line(r: &Value) -> Value {
    let sk = r["sk"].as_str().unwrap_or("");
    let findings = match &r["findings"] {
        Value::Array(a) => a.len(),
        Value::Object(o) => o
            .get("items")
            .and_then(Value::as_array)
            .map_or(o.len(), Vec::len),
        _ => 0,
    };
    json!({
        "review_id": sk,
        "repo": r["repo"],
        "pr_number": r["pr_number"],
        "pull_request": format!("https://github.com/{}/pull/{}", r["repo"].as_str().unwrap_or(""), r["pr_number"]),
        "pr_title": r["pr_title"],
        "verdict": r["verdict"],
        "risk": r["risk"],
        "findings": findings,
        "created_at": r["created_at"],
        "url": link(&format!("/reviewer/detail?sk={}", q(sk))),
    })
}

fn review_detail(r: &Value) -> Value {
    let mut out = review_line(r);
    if let Some(o) = out.as_object_mut() {
        o.insert("findings".into(), r["findings"].clone());
        o.insert("summary".into(), r["body"].clone());
        o.insert("action_summary".into(), r["action_summary"].clone());
        o.insert("head_sha".into(), r["head_sha"].clone());
        o.insert("posted_as".into(), r["posted_as"].clone());
    }
    out
}

/// A tool's answer, or what went wrong (shown to the model as a tool error).
type ToolResult = Result<Value, String>;

fn from_call(e: CallError) -> String {
    e.message
}

async fn list_runs(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let want = args["limit"].as_u64().unwrap_or(20).clamp(1, 100) as usize;
    let since = match arg(args, "since") {
        Some(s) => Some(
            parse_since(s).ok_or("since must be a time (RFC 3339) or a date like 2026-11-01.")?,
        ),
        None => None,
    };
    let mut cursor = arg(args, "cursor").map(str::to_string);
    let mut runs: Vec<Value> = Vec::new();
    let mut next: Option<String> = None;
    // A few pages at most: the API reads a few hundred runs per page.
    for _ in 0..5 {
        let mut uri = format!("/api/runs?limit={}", want.min(100));
        if let Some(s) = arg(args, "status") {
            uri.push_str(&format!("&status={}", q(s)));
        }
        if let Some(r) = arg(args, "repo") {
            uri.push_str(&format!("&repo={}", q(r)));
        }
        if let Some(cur) = &cursor {
            uri.push_str(&format!("&cursor={}", q(cur)));
        }
        let page = call(api, c, Method::GET, &uri, None)
            .await
            .map_err(from_call)?;
        let mut older = false;
        for r in page["runs"].as_array().cloned().unwrap_or_default() {
            if let Some(since) = since {
                let at = r["created_at"]
                    .as_str()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
                if at.is_some_and(|t| t < since) {
                    older = true;
                    break;
                }
            }
            if runs.len() < want {
                runs.push(run_line(&r));
            }
        }
        next = page["next"].as_str().map(str::to_string);
        if older {
            next = None;
        }
        if runs.len() >= want || next.is_none() {
            break;
        }
        cursor = next.clone();
    }
    Ok(json!({ "runs": runs, "next": next }))
}

async fn get_run(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let id = arg(args, "run_id").ok_or("Give run_id.")?;
    if id.len() > 64 {
        return Err("That isn't a run id.".into());
    }
    let r = call(api, c, Method::GET, &format!("/api/runs/{}", q(id)), None)
        .await
        .map_err(from_call)?;
    Ok(run_detail(&r))
}

async fn start_run(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    if let Some(ticket) = arg(args, "ticket") {
        let r = call(
            api,
            c,
            Method::POST,
            "/api/runs/start",
            Some(json!({ "ticket": ticket })),
        )
        .await
        .map_err(from_call)?;
        return Ok(json!({
            "status": r["status"],
            "ticket": r["ticket_id"],
            "message": r["message"],
            "runs": link(&format!("/runs?q={}", q(r["ticket_id"].as_str().unwrap_or("")))),
        }));
    }
    let task = arg(args, "task").ok_or("Give a ticket, or a task with its repo.")?;
    let repo = arg(args, "repo").ok_or("Give the repository for the task, owner/name.")?;
    if !valid_repo(repo) {
        return Err("repo must be owner/name.".into());
    }
    let title: String = arg(args, "title")
        .unwrap_or_else(|| task.lines().next().unwrap_or(task))
        .chars()
        .take(200)
        .collect();
    let plan = call(
        api,
        c,
        Method::POST,
        "/api/plans",
        Some(json!({
            "title": title,
            "description": task,
            "repo": repo,
            "tasks": [{ "title": title, "description": task, "repo": repo }],
        })),
    )
    .await
    .map_err(from_call)?;
    let plan_id = plan["plan_id"].as_str().unwrap_or("").to_string();
    if plan_id.is_empty() {
        return Err("CoderHelm couldn't make the plan for this task.".into());
    }
    call(
        api,
        c,
        Method::POST,
        &format!("/api/plans/{}/approve-and-execute", q(&plan_id)),
        None,
    )
    .await
    .map_err(|e| format!("The plan was made but didn't start: {}", e.message))?;
    Ok(json!({
        "status": "queued",
        "plan_id": plan_id,
        "message": "CoderHelm opened an issue for the task and queued a run on it. It shows in list_runs within a minute.",
        "plan": link(&format!("/plans/detail?id={}", q(&plan_id))),
    }))
}

async fn retry_run(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let id = arg(args, "run_id").ok_or("Give run_id.")?;
    if id.len() > 64 {
        return Err("That isn't a run id.".into());
    }
    let r = call(
        api,
        c,
        Method::POST,
        &format!("/api/runs/{}/retry", q(id)),
        None,
    )
    .await
    .map_err(|e| {
        if e.status == 400 {
            "This run can't be retried now: only runs that failed, need input, were cancelled or closed can.".to_string()
        } else {
            e.message
        }
    })?;
    Ok(
        json!({ "status": r["status"], "run_id": id, "url": link(&format!("/runs/detail?id={}", q(id))) }),
    )
}

async fn list_reviews(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let limit = args["limit"].as_u64().unwrap_or(20).clamp(1, 100);
    let mut uri = format!("/api/reviewer/reviews?limit={limit}");
    if let Some(r) = arg(args, "repo") {
        if !valid_repo(r) {
            return Err("repo must be owner/name.".into());
        }
        uri.push_str(&format!("&repo={}", q(r)));
    }
    if let Some(cur) = arg(args, "cursor") {
        uri.push_str(&format!("&cursor={}", q(cur)));
    }
    let page = call(api, c, Method::GET, &uri, None)
        .await
        .map_err(from_call)?;
    let reviews: Vec<Value> = page["reviews"]
        .as_array()
        .map(|a| a.iter().map(review_line).collect())
        .unwrap_or_default();
    Ok(json!({ "reviews": reviews, "next": page["next"] }))
}

async fn get_review(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let sk = match (arg(args, "review_id"), arg(args, "pr_url")) {
        (Some(id), _) => id.to_string(),
        (None, Some(pr)) => {
            let (repo, number) = parse_github_ref(pr, "pull").ok_or(
                "pr_url must be a pull request URL such as https://github.com/acme/web/pull/318.",
            )?;
            let mut cursor: Option<String> = None;
            let mut found = None;
            for _ in 0..5 {
                let mut uri = format!("/api/reviewer/reviews?limit=100&repo={}", q(&repo));
                if let Some(cur) = &cursor {
                    uri.push_str(&format!("&cursor={}", q(cur)));
                }
                let page = call(api, c, Method::GET, &uri, None)
                    .await
                    .map_err(from_call)?;
                found = page["reviews"].as_array().and_then(|a| {
                    a.iter()
                        .find(|r| r["pr_number"].as_u64() == Some(number))
                        .and_then(|r| r["sk"].as_str().map(str::to_string))
                });
                cursor = page["next"].as_str().map(str::to_string);
                if found.is_some() || cursor.is_none() {
                    break;
                }
            }
            found.ok_or(
                "CoderHelm hasn't reviewed that pull request yet. request_review asks for one.",
            )?
        }
        (None, None) => return Err("Give review_id, or pr_url.".into()),
    };
    let r = call(
        api,
        c,
        Method::GET,
        &format!("/api/reviewer/review?sk={}", q(&sk)),
        None,
    )
    .await
    .map_err(from_call)?;
    Ok(review_detail(&r))
}

async fn request_review(api: &McpApi, c: &McpCaller, args: &Value) -> ToolResult {
    let pr = arg(args, "pr_url").ok_or("Give pr_url.")?;
    let (repo, number) = parse_github_ref(pr, "pull")
        .ok_or("pr_url must be a pull request URL such as https://github.com/acme/web/pull/318, or acme/web#318.")?;
    let r = call(
        api,
        c,
        Method::POST,
        "/api/reviewer/re-review",
        Some(json!({ "repo": repo, "pr": number })),
    )
    .await
    .map_err(from_call)?;
    if r["status"] == "skipped" {
        return Err(r["reason"]
            .as_str()
            .unwrap_or("The reviewer skipped this pull request.")
            .to_string());
    }
    Ok(json!({
        "status": r["status"],
        "pull_request": format!("https://github.com/{repo}/pull/{number}"),
        "message": "The review is queued. It is posted on the pull request in a few minutes; get_review with this pr_url reads it.",
    }))
}

async fn run_tool(api: &McpApi, c: &McpCaller, name: &str, args: &Value) -> ToolResult {
    match name {
        "list_runs" => list_runs(api, c, args).await,
        "get_run" => get_run(api, c, args).await,
        "start_run" => start_run(api, c, args).await,
        "retry_run" => retry_run(api, c, args).await,
        "list_reviews" => list_reviews(api, c, args).await,
        "get_review" => get_review(api, c, args).await,
        "request_review" => request_review(api, c, args).await,
        _ => Err(format!("There is no tool called {name}.")),
    }
}

// ── HTTP ────────────────────────────────────────────────────────────

/// The bearer token of a request (anything but an API key, which never
/// reaches `/mcp`).
fn bearer(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = v.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|t| !t.is_empty())
}

fn quoted(s: &str) -> String {
    s.replace('\\', "").replace('"', "'")
}

/// `401`, with where to sign in (RFC 9728 5.1). A first sign-in asks for
/// reading only; acting is asked for when a tool needs it, or allowed on the
/// consent page.
pub fn unauthorized(refusal: Option<AccessRefusal>) -> Response {
    let mut challenge = format!(
        "Bearer resource_metadata=\"{}\", scope=\"{READ_SCOPE}\"",
        quoted(&mcp_oauth::resource_metadata_url())
    );
    let description = refusal.map(AccessRefusal::description);
    if let Some(d) = description {
        challenge.push_str(&format!(
            ", error=\"invalid_token\", error_description=\"{}\"",
            quoted(d)
        ));
    }
    let mut res = (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": if refusal.is_some() { "invalid_token" } else { "unauthorized" },
            "error_description": description.unwrap_or("Sign in to CoderHelm to use this server."),
        })),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&challenge) {
        res.headers_mut().insert(header::WWW_AUTHENTICATE, v);
    }
    res
}

/// `403` for a tool that needs a scope the token lacks: the client may ask
/// the person for it (step-up, MCP authorization).
pub fn insufficient_scope(needed: &'static str) -> Response {
    let challenge = format!(
        "Bearer error=\"insufficient_scope\", scope=\"{}\", resource_metadata=\"{}\", error_description=\"This needs {}.\"",
        mcp_oauth::scope_string([READ_SCOPE, needed]),
        quoted(&mcp_oauth::resource_metadata_url()),
        needed
    );
    let mut res = (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "insufficient_scope",
            "error_description": format!("This needs the {needed} scope. Connect the app to CoderHelm again and allow it."),
        })),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&challenge) {
        res.headers_mut().insert(header::WWW_AUTHENTICATE, v);
    }
    res
}

fn rpc_result(id: &Value, result: Value) -> Response {
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

fn rpc_error(status: StatusCode, id: &Value, code: i64, message: &str) -> Response {
    (
        status,
        Json(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })),
    )
        .into_response()
}

/// The version to answer `initialize` with: the client's, when this server
/// speaks it, else the newest.
pub fn negotiate(asked: Option<&str>) -> &'static str {
    asked
        .and_then(|a| PROTOCOL_VERSIONS.iter().find(|v| **v == a).copied())
        .unwrap_or(PROTOCOL_VERSIONS[0])
}

pub fn initialize_result(asked: Option<&str>) -> Value {
    json!({
        "protocolVersion": negotiate(asked),
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": {
            "name": "coderhelm",
            "title": "CoderHelm",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "instructions": "CoderHelm turns tickets into pull requests and reviews pull requests. Use list_runs and get_run to see work and why a run is waiting; start_run to start work from a GitHub issue, a Jira key or a task in words; retry_run to start a stopped run again; list_reviews, get_review and request_review for pull request reviews. Everything happens as the person who connected this app, with their role in their CoderHelm team.",
    })
}

pub fn tools_list() -> Value {
    json!({ "tools": TOOLS.iter().map(Tool::listing).collect::<Vec<_>>() })
}

/// The person behind a connection, from their member record now. `None`
/// when they are no longer an active member of its team.
async fn member_claims(state: &AppState, g: &StoredGrant) -> Result<Option<Claims>, String> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.users_table_name)
        .key("pk", AttributeValue::S(g.team_id.clone()))
        .key("sk", AttributeValue::S(g.user_id.clone()))
        .send()
        .await
        .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
    let Some(item) = r.item() else {
        return Ok(None);
    };
    let get = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    if get("status").as_deref() == Some("invited") {
        return Ok(None);
    }
    Ok(Some(Claims {
        sub: g.user_id.clone(),
        team_id: g.team_id.clone(),
        email: get("email").unwrap_or_else(|| g.email.clone()),
        role: get("role").unwrap_or_else(|| "member".into()),
        github_login: get("github_login"),
        aud: mcp_oauth::ACCESS_AUDIENCE.into(),
        exp: 0,
        iat: g.created_at_secs,
        support_view: None,
        custom: None,
        api_key: None,
    }))
}

/// Whether to write `last_used_at` for this connection now: once a minute
/// per connection per instance.
fn due_to_note(id: &str) -> bool {
    static SEEN: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    let Ok(mut seen) = SEEN.get_or_init(Default::default).lock() else {
        return false;
    };
    let now = Instant::now();
    match seen.get(id) {
        Some(at) if now.duration_since(*at) < Duration::from_secs(60) => false,
        _ => {
            if seen.len() > 10_000 {
                seen.retain(|_, at| now.duration_since(*at) < Duration::from_secs(60));
            }
            seen.insert(id.to_string(), now);
            true
        }
    }
}

/// `POST /mcp`: one JSON-RPC message from an MCP client.
pub async fn post(
    State(state): State<Arc<AppState>>,
    Extension(api): Extension<McpApi>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = bearer(&headers) else {
        return unauthorized(None);
    };
    let store = mcp_oauth::store(&state);
    let now = chrono::Utc::now();
    let authorized = match mcp_oauth::authenticate(
        &store,
        token,
        &state.secrets.jwt_secret,
        now.timestamp().max(0) as u64,
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(refusal)) => return unauthorized(Some(refusal)),
        Err(e) => {
            error!(error = %e, "MCP connection not read");
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let grant = authorized.grant;
    if crate::middleware::rate_limit::allow(
        &crate::middleware::rate_limit::ClientIp(grant.id.clone()),
        "mcp",
        300,
        60,
    )
    .is_err()
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "Too many calls. Wait a minute and try again." })),
        )
            .into_response();
    }
    if let Some(v) = headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
    {
        if !PROTOCOL_VERSIONS.contains(&v) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("MCP protocol version {v} isn't supported. This server speaks {}.", PROTOCOL_VERSIONS.join(", ")) })),
            )
                .into_response();
        }
    }
    if due_to_note(&grant.id) {
        let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        if let Err(e) = mcp_oauth::OAuthStore::note_use(&store, &grant, &at).await {
            warn!(error = %e, "MCP connection use not noted");
        }
    }

    let msg: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return rpc_error(
                StatusCode::BAD_REQUEST,
                &Value::Null,
                -32700,
                "The body isn't JSON.",
            )
        }
    };
    if msg.is_array() {
        return rpc_error(
            StatusCode::BAD_REQUEST,
            &Value::Null,
            -32600,
            "Send one JSON-RPC message per request.",
        );
    }
    let id = msg.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        // A response or a notification from the client: nothing to answer.
        return StatusCode::ACCEPTED.into_response();
    };
    if msg.get("id").is_none() {
        return StatusCode::ACCEPTED.into_response();
    }
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    match method {
        "initialize" => rpc_result(&id, initialize_result(params["protocolVersion"].as_str())),
        "ping" => rpc_result(&id, json!({})),
        "tools/list" => rpc_result(&id, tools_list()),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let Some(t) = find_tool(name) else {
                return rpc_error(
                    StatusCode::OK,
                    &id,
                    -32602,
                    &format!("There is no tool called {name}."),
                );
            };
            if !authorized.scopes.contains(t.scope()) {
                return insufficient_scope(t.scope());
            }
            let args = match &params["arguments"] {
                Value::Null => json!({}),
                Value::Object(_) => params["arguments"].clone(),
                _ => return rpc_error(StatusCode::OK, &id, -32602, "arguments must be an object."),
            };
            let claims = match member_claims(&state, &grant).await {
                Ok(Some(c)) => c,
                Ok(None) => return unauthorized(Some(AccessRefusal::Revoked)),
                Err(e) => {
                    error!(error = %e, "Member record not read for an MCP call");
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
            };
            // Written before the call runs: nothing happens without a record.
            if roles::record(
                &state,
                &claims,
                AuditEntry {
                    action: "mcp_call",
                    role_id: &grant.id,
                    role_name: &grant.client_name,
                    detail: audit_detail(t.name, &args),
                },
            )
            .await
            .is_err()
            {
                return rpc_result(
                    &id,
                    json!({
                        "content": [{ "type": "text", "text": "CoderHelm couldn't record this call in the team's log, so it didn't run it. Try again." }],
                        "isError": true,
                    }),
                );
            }
            let caller = McpCaller {
                claims,
                grant,
                scopes: authorized.scopes,
                tool_handlers: t.handlers,
            };
            match run_tool(&api, &caller, t.name, &args).await {
                Ok(v) => rpc_result(
                    &id,
                    json!({
                        "content": [{ "type": "text", "text": serde_json::to_string_pretty(&v).unwrap_or_default() }],
                        "structuredContent": v,
                        "isError": false,
                    }),
                ),
                Err(message) => rpc_result(
                    &id,
                    json!({ "content": [{ "type": "text", "text": message }], "isError": true }),
                ),
            }
        }
        _ => rpc_error(
            StatusCode::OK,
            &id,
            -32601,
            &format!("{method} isn't supported by this server."),
        ),
    }
}

/// `GET /mcp` and `DELETE /mcp`: no event stream and no sessions here.
pub async fn not_allowed() -> Response {
    let mut res = (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": "This MCP server answers POST only." })),
    )
        .into_response();
    res.headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom(caps: &[&str]) -> Holder {
        let caps: Vec<String> = caps.iter().map(|c| c.to_string()).collect();
        Holder::custom("r1", Some(("Custom", &caps)))
    }

    fn read() -> BTreeSet<&'static str> {
        BTreeSet::from([READ_SCOPE])
    }

    fn write() -> BTreeSet<&'static str> {
        BTreeSet::from([READ_SCOPE, WRITE_SCOPE])
    }

    #[test]
    fn every_tool_calls_only_routed_handlers_its_scope_reaches() {
        let routed: BTreeSet<&str> = permissions::api_route_handlers()
            .values()
            .map(String::as_str)
            .collect();
        for t in TOOLS {
            assert!(!t.handlers.is_empty(), "{}", t.name);
            for h in t.handlers {
                assert!(
                    routed.contains(h),
                    "{} calls {h}, which main.rs doesn't route",
                    t.name
                );
                let scope =
                    scope_for_handler(h).unwrap_or_else(|| panic!("{h} isn't reachable by apps"));
                if !t.write {
                    assert_eq!(scope, READ_SCOPE, "the reading tool {} calls {h}", t.name);
                }
            }
        }
        // Every reachable route belongs to a tool.
        for h in READ_HANDLERS.iter().chain(WRITE_HANDLERS) {
            assert!(
                TOOLS.iter().any(|t| t.handlers.contains(h)),
                "{h} has no tool"
            );
        }
    }

    #[test]
    fn annotations_say_which_tools_only_read() {
        let list = tools_list();
        let tools = list["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 7);
        for t in tools {
            let name = t["name"].as_str().unwrap();
            let read_only = t["annotations"]["readOnlyHint"].as_bool().unwrap();
            let want = matches!(
                name,
                "list_runs" | "get_run" | "list_reviews" | "get_review"
            );
            assert_eq!(read_only, want, "{name}");
            assert_eq!(t["annotations"]["destructiveHint"], false, "{name}");
            assert_eq!(t["inputSchema"]["type"], "object", "{name}");
        }
        assert_eq!(find_tool("start_run").unwrap().scope(), WRITE_SCOPE);
        assert_eq!(find_tool("get_review").unwrap().scope(), READ_SCOPE);
    }

    /// For every tool and every route it calls: allowed exactly when the
    /// connection holds the tool's scope and the person's role allows the
    /// route in the dashboard.
    #[test]
    fn each_tool_is_authorized_like_the_same_dashboard_action() {
        let holders = [
            Holder::builtin("viewer"),
            Holder::builtin("member"),
            Holder::builtin("admin"),
            Holder::builtin("owner"),
            custom(&[]),
            custom(&["runs"]),
            custom(&["plans", "reviews"]),
        ];
        for t in TOOLS {
            for holder in &holders {
                for scopes in [read(), write()] {
                    for h in t.handlers {
                        let dashboard = matches!(
                            permissions::resolve_access(holder, Some(h)),
                            Access::Allow { .. }
                        );
                        let got = decide(&scopes, t.handlers, holder, Some(h));
                        let want = scopes.contains(t.scope()) && dashboard;
                        assert_eq!(
                            got.is_ok(),
                            want,
                            "{} via {h} for {} with {scopes:?}",
                            t.name,
                            holder.role
                        );
                        if !scopes.contains(t.scope()) {
                            assert_eq!(got, Err(Refusal::Scope(WRITE_SCOPE)));
                        }
                    }
                }
            }
        }
    }

    /// Every route any tool may call.
    const ALL: &[&str] = &[
        "api::list_runs",
        "api::get_run",
        "reviewer::list_reviews",
        "reviewer::get_review",
        "run_start::start_run",
        "api::retry_run",
        "plans::create_plan",
        "plans::approve_all_and_execute",
        "reviewer::re_review",
    ];

    fn decide_any(
        scopes: &BTreeSet<&'static str>,
        holder: &Holder,
        handler: Option<&str>,
    ) -> Result<&'static str, Refusal> {
        decide(scopes, ALL, holder, handler)
    }

    #[test]
    fn a_tool_reaches_only_its_own_routes() {
        let owner = Holder::builtin("owner");
        let list = find_tool("list_runs").unwrap();
        assert!(decide(&write(), list.handlers, &owner, Some("api::list_runs")).is_ok());
        // Not another tool's, even one the connection's scopes reach.
        assert_eq!(
            decide(&write(), list.handlers, &owner, Some("api::retry_run")),
            Err(Refusal::NotForApps)
        );
        let get = find_tool("get_review").unwrap();
        assert!(decide(
            &read(),
            get.handlers,
            &owner,
            Some("reviewer::list_reviews")
        )
        .is_ok());
        assert_eq!(
            decide(&write(), get.handlers, &owner, Some("reviewer::re_review")),
            Err(Refusal::NotForApps)
        );
    }

    #[test]
    fn the_decisions_people_would_expect() {
        let viewer = Holder::builtin("viewer");
        let member = Holder::builtin("member");
        // A viewer reads, and can't act even with the write scope.
        assert_eq!(
            decide_any(&write(), &viewer, Some("api::list_runs")),
            Ok("viewer")
        );
        match decide_any(&write(), &viewer, Some("api::retry_run")) {
            Err(Refusal::Role(c)) => assert_eq!(c.key, "runs"),
            other => panic!("{other:?}"),
        }
        match decide_any(&write(), &viewer, Some("reviewer::re_review")) {
            Err(Refusal::Role(c)) => assert_eq!(c.key, "reviews"),
            other => panic!("{other:?}"),
        }
        // A member acts only with the write scope.
        assert_eq!(
            decide_any(&read(), &member, Some("api::retry_run")),
            Err(Refusal::Scope(WRITE_SCOPE))
        );
        assert_eq!(
            decide_any(&write(), &member, Some("api::retry_run")),
            Ok("member")
        );
        assert_eq!(
            decide_any(&write(), &member, Some("run_start::start_run")),
            Ok("member")
        );
        // A custom role with runs but not plans starts from a ticket, not from words.
        let runs_only = custom(&["runs"]);
        assert!(decide_any(&write(), &runs_only, Some("run_start::start_run")).is_ok());
        assert!(matches!(
            decide_any(&write(), &runs_only, Some("plans::create_plan")),
            Err(Refusal::Role(c)) if c.key == "plans"
        ));
        // Nothing outside the tools, whatever the role: not the dashboard's
        // own routes, not managing keys or connections, not an unknown route.
        let owner = Holder::builtin("owner");
        for h in [
            "api::sync_repos",
            "api_keys::create",
            "mcp_oauth::list_connections",
            "mcp_oauth::decide_request",
            "users::invite_user",
            "api::cancel_run",
        ] {
            assert_eq!(
                decide_any(&write(), &owner, Some(h)),
                Err(Refusal::NotForApps),
                "{h}"
            );
        }
        assert_eq!(decide_any(&write(), &owner, None), Err(Refusal::NotForApps));
    }

    #[test]
    fn github_refs_and_times_are_read() {
        assert_eq!(
            parse_github_ref("https://github.com/acme/web/issues/42", "issues"),
            Some(("acme/web".into(), 42))
        );
        assert_eq!(
            parse_github_ref("https://github.com/acme/web/pull/318/files", "pull"),
            Some(("acme/web".into(), 318))
        );
        assert_eq!(
            parse_github_ref("acme/web#7", "pull"),
            Some(("acme/web".into(), 7))
        );
        assert_eq!(
            parse_github_ref("https://github.com/acme/web/issues/42", "pull"),
            None
        );
        assert_eq!(
            parse_github_ref("https://evil.example/acme/web/pull/1", "pull"),
            None
        );
        assert_eq!(parse_github_ref("acme/web#0", "pull"), None);
        assert_eq!(parse_github_ref("acme/we b#3", "pull"), None);
        assert!(parse_since("2026-11-01").is_some());
        assert!(parse_since("2026-11-01T10:00:00Z").is_some());
        assert!(parse_since("yesterday").is_none());
    }

    #[test]
    fn the_log_line_names_the_call_but_never_a_tasks_words() {
        let d = audit_detail(
            "start_run",
            &json!({ "task": "secret plans", "repo": "acme/web" }),
        );
        assert_eq!(d, "start_run: repo acme/web, a task in words");
        assert_eq!(audit_detail("list_runs", &json!({})), "list_runs");
    }

    #[test]
    fn a_run_shows_its_holds_and_latest_notes() {
        let notes: Vec<Value> = (0..20)
            .map(|i| json!({ "text": format!("n{i}") }))
            .collect();
        let r = json!({
            "run_id": "r1",
            "status": "needs_input",
            "questions": [{ "text": "Which?", "answered": false }, { "text": "Done", "answered": true }],
            "draft_hold": { "reason": "CI" },
            "waiting_on": null,
            "progress_notes": notes,
            "pr_url": "https://github.com/acme/web/pull/1",
            "tokens_in": 0,
        });
        let d = run_detail(&r);
        assert_eq!(d["holds"]["questions"].as_array().unwrap().len(), 1);
        assert_eq!(d["holds"]["draft_hold"]["reason"], "CI");
        assert!(d["holds"].get("waiting_on").is_none());
        assert_eq!(d["log_summary"]["notes_total"], 20);
        assert_eq!(
            d["log_summary"]["latest_notes"].as_array().unwrap().len(),
            15
        );
        assert_eq!(d["log_summary"]["latest_notes"][14]["text"], "n19");
        assert_eq!(d["pull_request"], "https://github.com/acme/web/pull/1");
        assert!(d["url"].as_str().unwrap().ends_with("/runs/detail?id=r1"));
    }

    #[test]
    fn the_client_is_told_where_to_sign_in_and_what_to_ask_for() {
        let res = unauthorized(None);
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let h = res.headers()[header::WWW_AUTHENTICATE].to_str().unwrap();
        assert!(h.starts_with("Bearer resource_metadata=\""), "{h}");
        assert!(h.contains("/.well-known/oauth-protected-resource/mcp\""));
        assert!(h.contains("scope=\"runs:read\""));
        let res = unauthorized(Some(AccessRefusal::Expired));
        let h = res.headers()[header::WWW_AUTHENTICATE].to_str().unwrap();
        assert!(h.contains("error=\"invalid_token\""));
        let res = insufficient_scope(WRITE_SCOPE);
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let h = res.headers()[header::WWW_AUTHENTICATE].to_str().unwrap();
        assert!(
            h.contains("error=\"insufficient_scope\"")
                && h.contains("scope=\"runs:read runs:write\"")
        );
    }

    /// `/mcp` as main() mounts it: refusals come before any store read.
    #[tokio::test]
    async fn mcp_over_http_refuses_without_a_good_token_and_answers_post_only() {
        use axum::routing::post;
        let state = crate::tests::hosted_state();
        let app = Router::new()
            .route(
                "/mcp",
                post(super::post)
                    .get(not_allowed)
                    .delete(not_allowed)
                    .layer(axum::Extension(McpApi(Router::new()))),
            )
            .with_state(state);
        let send = |method: &str, auth: Option<&str>| {
            let mut b = Request::builder()
                .method(method)
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(a) = auth {
                b = b.header(header::AUTHORIZATION, a);
            }
            b.body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
            ))
            .unwrap()
        };
        let r = app.clone().oneshot(send("POST", None)).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert!(r.headers()[header::WWW_AUTHENTICATE]
            .to_str()
            .unwrap()
            .contains("resource_metadata="));
        // A session token, an API key or garbage is not an access token.
        let session =
            crate::auth::jwt::create_token("u", "t", "e@x.com", "owner", None, "test", 300)
                .unwrap();
        for bad in [format!("Bearer {session}"), "Bearer nope".to_string()] {
            let r = app.clone().oneshot(send("POST", Some(&bad))).await.unwrap();
            assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "{bad}");
            assert!(r.headers()[header::WWW_AUTHENTICATE]
                .to_str()
                .unwrap()
                .contains("invalid_token"));
        }
        // An expired access token, signed right, is refused before its connection is read.
        let g = crate::mcp_oauth::StoredGrant {
            id: "01jbgrant00000000000000001".into(),
            team_id: "TEAM#a".into(),
            user_id: "USER#a".into(),
            email: String::new(),
            client_id: "mcpc_1".into(),
            client_name: String::new(),
            redirect_host: String::new(),
            scopes: vec![READ_SCOPE.into()],
            created_at: String::new(),
            created_at_secs: 0,
            mfa: false,
            via: None,
            refresh_hash: String::new(),
            prev_refresh_hash: None,
            refresh_expires_at: String::new(),
            last_used_at: None,
            revoked_at: None,
            revoked_by: None,
        };
        let old = crate::mcp_oauth::issue_access(&g, &read(), "https://x", "test", 1_000).unwrap();
        let r = app
            .clone()
            .oneshot(send("POST", Some(&format!("Bearer {old}"))))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        for m in ["GET", "DELETE"] {
            let r = app.clone().oneshot(send(m, None)).await.unwrap();
            assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED, "{m}");
            assert_eq!(r.headers()[header::ALLOW], "POST");
        }
    }

    #[test]
    fn the_protocol_version_is_the_clients_when_spoken_here() {
        assert_eq!(negotiate(Some("2025-06-18")), "2025-06-18");
        assert_eq!(negotiate(Some("1999-01-01")), PROTOCOL_VERSIONS[0]);
        assert_eq!(negotiate(None), PROTOCOL_VERSIONS[0]);
        let r = initialize_result(Some("2025-03-26"));
        assert_eq!(r["capabilities"]["tools"]["listChanged"], false);
        assert_eq!(r["serverInfo"]["name"], "coderhelm");
    }
}
