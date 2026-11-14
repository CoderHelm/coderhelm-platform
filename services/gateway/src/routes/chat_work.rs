//! Chat's "Tickets, pull requests and runs" source: the team's own work as
//! Coderhelm keeps it and as the team's Jira and GitHub hold it. Every tool
//! only reads:
//!
//! - [`TICKETS`]: tickets by key, title words or text through the team's
//!   Jira app (status, assignee, latest comments, linked pull requests), and
//!   from the runs' records when Jira can't be reached.
//! - [`PULL_REQUESTS`]: pull requests and Coderhelm's branches in the team's
//!   repositories, through the GitHub installation (`chat_work_prs`).
//! - [`RUNS`]: runs, current and past (`chat_work_runs`).
//! - [`FINDINGS`]: security findings (dependency advisories and CVEs),
//!   infrastructure findings and quarantined tests.
//! - [`RECORDS`]: reviews and their verdicts, alerts, log findings and
//!   plans (`chat_work_records`).
//!
//! Each tool is offered only when the asker could see what it reads in the
//! dashboard: the route that shows it is named ([`Kind::handler`]) and
//! decided by `permissions::resolve_access`, exactly as the route itself is.
//! Reads are of the asker's team only, bounded, newest first, and each
//! record is registered as a citable source with a link to its page.

use serde::Serialize;
use serde_json::{json, Value};

use crate::permissions::{resolve_access, Access, Holder};

pub const SOURCE_KEY: &str = "work";
pub const SOURCE_LABEL: &str = "Tickets, pull requests and runs";
pub const SOURCE_ABOUT: &str =
    "Tickets, pull requests, Coderhelm's runs, security and infrastructure findings, reviews and alerts";

pub const TICKETS: &str = "work_tickets";
pub const PULL_REQUESTS: &str = "work_pull_requests";
pub const RUNS: &str = "work_runs";
pub const FINDINGS: &str = "work_findings";
pub const RECORDS: &str = "work_records";

/// Every tool of this source, in the order they are offered.
pub const TOOLS: [&str; 5] = [TICKETS, PULL_REQUESTS, RUNS, FINDINGS, RECORDS];

/// The tools a status question's answer lives in.
pub const STATUS_TOOLS: [&str; 3] = [TICKETS, PULL_REQUESTS, RUNS];

pub fn is_work_tool(name: &str) -> bool {
    TOOLS.contains(&name)
}

/// One record found, before it is registered as a source.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Found {
    /// `ticket`, `pull_request`, `run`, `finding` or `record`.
    pub kind: &'static str,
    pub title: String,
    pub loc: String,
    pub as_of: String,
    pub passage: String,
    /// The record's page in the dashboard.
    pub href: Option<String>,
    /// The record outside Coderhelm (the pull request, the Jira issue).
    pub url: Option<String>,
}

/// At most `max` characters, on one line when short.
pub fn clip(text: &str, max: usize) -> String {
    let t = text.trim();
    if t.chars().count() <= max {
        return t.to_string();
    }
    let cut: String = t.chars().take(max).collect();
    format!("{}…", cut.trim_end())
}

/// Words of a title search: lower case, no punctuation, no little words.
pub fn words_of(q: &str) -> Vec<String> {
    const SMALL: &[&str] = &[
        "the", "and", "for", "on", "of", "to", "in", "a", "an", "is", "it", "at", "by", "with",
        "what", "whats", "latest", "status", "about", "from", "this", "that",
    ];
    let mut out: Vec<String> = Vec::new();
    for w in q
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .map(|w| w.trim_matches('-'))
        .filter(|w| w.chars().count() >= 2 && !SMALL.contains(w))
    {
        if out.len() < 12 && !out.iter().any(|o| o == w) {
            out.push(w.to_string());
        }
    }
    out
}

pub fn live_stamp() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string()
}

// ── What the asker may read ─────────────────────────────────────────────────

/// A kind of record, and the dashboard route that shows it: the asker may
/// read it in Chat exactly when that route would let them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Kind {
    pub key: &'static str,
    pub label: &'static str,
    /// The routed handler (`file::function`) whose check decides.
    pub handler: &'static str,
    /// The tool that reads it.
    pub tool: &'static str,
}

pub const KINDS: &[Kind] = &[
    Kind {
        key: "runs",
        label: "runs",
        handler: "api::list_runs",
        tool: RUNS,
    },
    Kind {
        key: "tickets",
        label: "tickets",
        // A ticket is read as the runs list shows it, and through Jira.
        handler: "api::list_runs",
        tool: TICKETS,
    },
    Kind {
        key: "pull_requests",
        label: "pull requests",
        handler: "reviewer::open_prs",
        tool: PULL_REQUESTS,
    },
    Kind {
        key: "security",
        label: "security findings",
        handler: "security::list_findings",
        tool: FINDINGS,
    },
    Kind {
        key: "infra",
        label: "infrastructure findings",
        handler: "infra_security::findings",
        tool: FINDINGS,
    },
    Kind {
        key: "tests",
        label: "quarantined tests",
        handler: "tests::list_tests",
        tool: FINDINGS,
    },
    Kind {
        key: "reviews",
        label: "reviews",
        handler: "reviewer::list_reviews",
        tool: RECORDS,
    },
    Kind {
        key: "alerts",
        label: "alerts",
        handler: "alerts::list_events",
        tool: RECORDS,
    },
    Kind {
        key: "log_findings",
        label: "log findings",
        handler: "log_findings::list_findings",
        tool: RECORDS,
    },
    Kind {
        key: "plans",
        label: "plans",
        handler: "plans::list_plans",
        tool: RECORDS,
    },
];

pub fn kind(key: &str) -> Option<&'static Kind> {
    KINDS.iter().find(|k| k.key == key)
}

/// The kinds of record this person may read: the route that shows each
/// allows them.
pub fn readable(holder: &Holder) -> Vec<&'static str> {
    KINDS
        .iter()
        .filter(|k| {
            matches!(
                resolve_access(holder, Some(k.handler)),
                Access::Allow { .. }
            )
        })
        .map(|k| k.key)
        .collect()
}

/// Whether `kind` is among what may be read.
pub fn may(readable: &[&str], kind: &str) -> bool {
    readable.contains(&kind)
}

// ── The tools ───────────────────────────────────────────────────────────────

/// The definitions of the tools this person may use. `readable`: the
/// record kinds they may read; `repos`: the team's repositories.
pub fn defs(readable: &[&str], repos: &[String], can_github: bool) -> Vec<Value> {
    let repo = json!({
        "type": "string",
        "description": format!("One of the team's repositories (owner/name or the short name): {}",
            repos.iter().take(40).cloned().collect::<Vec<_>>().join(", "))
    });
    let mut out = Vec::new();
    if may(readable, "tickets") {
        out.push(json!({
            "name": TICKETS,
            "description": "Find the team's tickets (Jira) by key, title words or text. Returns each ticket's status, assignee, priority, when it last changed, its latest comments and the pull requests linked to it, with a link. Use it first for any question about a ticket: \"what's the latest on <title>\", a ticket key like ABC-123, \"where are we on\", \"who is working on\". When Jira can't be reached it answers from Coderhelm's runs on the ticket.",
            "input_schema": {"type": "object", "properties": {
                "key": {"type": "string", "description": "A ticket key, like ABC-123"},
                "query": {"type": "string", "description": "Words of the title or text; a quoted title as written"}
            }}
        }));
    }
    if may(readable, "pull_requests") && can_github {
        out.push(json!({
            "name": PULL_REQUESTS,
            "description": "Find the team's pull requests and Coderhelm's branches on GitHub by ticket key, title words, author or number. Returns state (open, merged, closed, draft), checks, each reviewer's verdict, last activity, branch and a link. Use it for \"is it merged\", \"what's the PR for\", \"what's blocking\", a PR number, and after finding a ticket.",
            "input_schema": {"type": "object", "properties": {
                "ticket": {"type": "string", "description": "A ticket key the pull request names"},
                "query": {"type": "string", "description": "Words of the title"},
                "number": {"type": "integer", "description": "A pull request number"},
                "author": {"type": "string", "description": "A GitHub login"},
                "repo": repo,
                "state": {"type": "string", "enum": ["open", "closed", "merged", "any"]}
            }}
        }));
    }
    if may(readable, "runs") {
        out.push(json!({
            "name": RUNS,
            "description": "Find Coderhelm's runs, current and past, newest first: what each worked on (title, ticket, repository), where it stands (status, current step), its pull requests, when it started and last changed, why it stopped, what it waits on, its latest notes, and its page. Use status \"active\" for \"what's running now\". Find by ticket key, pull request number, repository, title words, status or the last few days, or read one run by its id.",
            "input_schema": {"type": "object", "properties": {
                "ticket": {"type": "string", "description": "A ticket key or id"},
                "query": {"type": "string", "description": "Words of the title"},
                "pr": {"type": "integer", "description": "A pull request number"},
                "repo": repo,
                "status": {"type": "string", "enum": ["any", "active", "running", "waiting", "failed", "done", "stopped"],
                    "description": "active: working or waiting on the team; running: working now; waiting: on an answer or an outside change; failed; done (finished, merged); stopped (cancelled, closed, retried)"},
                "days": {"type": "integer", "description": "Only runs started in the last this many days"},
                "run_id": {"type": "string", "description": "One run's id"}
            }}
        }));
    }
    let finding_kinds: Vec<&str> = ["security", "infra", "tests"]
        .into_iter()
        .filter(|k| may(readable, k))
        .collect();
    if !finding_kinds.is_empty() {
        out.push(json!({
            "name": FINDINGS,
            "description": "Find the team's findings: security (dependency advisories and CVEs: id, package, severity, repository, state, the pull request fixing it), infra (infrastructure findings: what, where, severity, state) and tests (quarantined tests and why). Search by a CVE or advisory id, a package, a repository or words; open ones first unless you ask for all. Use it for \"is CVE-... fixed in <repo>\", \"which infra findings are open\", \"what's quarantined\".",
            "input_schema": {"type": "object", "properties": {
                "kind": {"type": "string", "enum": finding_kinds},
                "query": {"type": "string", "description": "A CVE or advisory id, a package or words"},
                "repo": repo,
                "state": {"type": "string", "enum": ["open", "fixed", "dismissed", "any"]}
            }, "required": ["kind"]}
        }));
    }
    let record_kinds: Vec<&str> = ["reviews", "alerts", "log_findings", "plans"]
        .into_iter()
        .filter(|k| may(readable, k))
        .collect();
    if !record_kinds.is_empty() {
        out.push(json!({
            "name": RECORDS,
            "description": "Find other records Coderhelm keeps, newest first: reviews (Coderhelm's pull request reviews and their verdicts), alerts (alerts that came in and what was done), log_findings (problems found in the logs and their fixes), plans (plans and their tickets). Search by words, a repository or a pull request number.",
            "input_schema": {"type": "object", "properties": {
                "kind": {"type": "string", "enum": record_kinds},
                "query": {"type": "string"},
                "repo": repo,
                "pr": {"type": "integer"}
            }, "required": ["kind"]}
        }));
    }
    out
}

/// What the dashboard shows for a call.
pub fn label(name: &str, input: &Value) -> String {
    let s = |k: &str| clip(input[k].as_str().unwrap_or(""), 60);
    let what = [s("key"), s("ticket"), s("query"), s("run_id")]
        .into_iter()
        .find(|v| !v.is_empty())
        .or_else(|| input["number"].as_u64().map(|n| format!("#{n}")))
        .or_else(|| input["pr"].as_u64().map(|n| format!("#{n}")));
    let about = |noun: &str| match &what {
        Some(w) => format!("Looked up {noun} for \u{201c}{w}\u{201d}"),
        None => format!("Looked up {noun}"),
    };
    match name {
        TICKETS => about("tickets"),
        PULL_REQUESTS => about("pull requests"),
        RUNS if input["status"] == "active" && what.is_none() => {
            "Looked up what's running now".into()
        }
        RUNS => about("runs"),
        FINDINGS | RECORDS => {
            let k = input["kind"]
                .as_str()
                .and_then(kind)
                .map_or("records", |k| k.label);
            about(k)
        }
        _ => name.replace('_', " "),
    }
}

/// The note that puts the right tool first for a question, from what it
/// names (`chat_hints`). Empty when it names nothing of the team's work.
pub fn hint_note(h: &super::chat_hints::Hints, offered: &[&str]) -> String {
    let has = |t: &str| offered.contains(&t);
    let mut steps: Vec<String> = Vec::new();
    if h.running && has(RUNS) {
        steps.push(format!("{RUNS} with status \"active\""));
    }
    for t in &h.tickets {
        if has(TICKETS) {
            steps.push(format!("{TICKETS} with key \"{t}\""));
        }
        if has(PULL_REQUESTS) {
            steps.push(format!("{PULL_REQUESTS} with ticket \"{t}\""));
        }
        if has(RUNS) {
            steps.push(format!("{RUNS} with ticket \"{t}\""));
        }
    }
    for q in &h.quoted {
        if has(TICKETS) {
            steps.push(format!("{TICKETS} with query \"{q}\""));
        }
        if has(RUNS) {
            steps.push(format!("{RUNS} with query \"{q}\""));
        }
    }
    for n in &h.prs {
        if has(PULL_REQUESTS) {
            steps.push(format!("{PULL_REQUESTS} with number {n}"));
        }
        if has(RUNS) {
            steps.push(format!("{RUNS} with pr {n}"));
        }
    }
    for a in &h.advisories {
        if has(FINDINGS) {
            steps.push(format!(
                "{FINDINGS} with kind \"security\" and query \"{a}\""
            ));
        }
    }
    if steps.is_empty() && h.status {
        if has(TICKETS) {
            steps.push(format!("{TICKETS} with the question's words"));
        }
        if has(RUNS) {
            steps.push(format!("{RUNS} with the question's words"));
        }
    }
    if steps.is_empty() {
        return String::new();
    }
    steps.truncate(8);
    format!(
        "\nThis question is about the team's own work. Look it up where it lives first: {}. \
         Then read code or docs only for context. Give a dated status (when each record last changed).\n",
        steps.join("; ")
    )
}

/// The order tools are offered in: the ones the question's hints name first.
pub fn put_first(tools: &mut [Value], h: &super::chat_hints::Hints) {
    if !h.about_work() {
        return;
    }
    let rank = |t: &Value| -> usize {
        match t["name"].as_str().unwrap_or("") {
            FINDINGS if h.findings => 0,
            RUNS if h.running => 0,
            TICKETS => 1,
            PULL_REQUESTS => 2,
            RUNS => 3,
            FINDINGS => 4,
            RECORDS => 5,
            _ => 9,
        }
    };
    tools.sort_by_key(rank);
}

// ── Running a call ──────────────────────────────────────────────────────────

/// What one question's work tools read with: the asker, what they may read,
/// and the team's connections. Built once per question.
pub struct Ctx {
    pub state: std::sync::Arc<crate::AppState>,
    /// The asker's claims (their team).
    pub claims: crate::models::Claims,
    pub holder: Holder,
    /// The record kinds they may read ([`readable`]).
    pub readable: Vec<&'static str>,
    /// The team's enabled repositories.
    pub repos: Vec<String>,
    pub github_token: Option<String>,
    /// The installation the token is for (its GitHub budget).
    pub installation_id: Option<u64>,
    pub jira: Option<super::chat_work_tickets::JiraApp>,
}

/// The installation's spent-limit time, from the worker's record of it.
async fn limited_until(state: &crate::AppState, installation_id: u64) -> Option<u64> {
    let table = &state.config.analytics_table_name;
    if table.is_empty() {
        return None;
    }
    let item = state
        .dynamo
        .get_item()
        .table_name(table)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(common::github_budget::partition(
                installation_id,
            )),
        )
        .key(
            "period",
            aws_sdk_dynamodb::types::AttributeValue::S(common::github_budget::STATE_KEY.into()),
        )
        .send()
        .await
        .ok()?
        .item?;
    item.get("limited_until")?.as_n().ok()?.parse().ok()
}

/// Registers what a call found and renders it for the model. Returns the
/// text, the step's summary and whether it failed.
pub fn register(
    reg: &mut common::knowledge::answer::Registry,
    found: Vec<Found>,
    noun: &str,
    note: &str,
) -> (String, String, bool) {
    use common::knowledge::answer::SourceRef;
    if found.is_empty() {
        return (
            format!("No {noun} matched.{note}"),
            format!("No {noun} matched"),
            false,
        );
    }
    let ids: Vec<String> = found
        .into_iter()
        .map(|f| {
            reg.add(SourceRef {
                kind: f.kind.into(),
                title: f.title,
                loc: f.loc,
                as_of: f.as_of,
                passage: f.passage,
                href: f.href,
                url: f.url,
                ..Default::default()
            })
        })
        .collect();
    (
        format!("{}{note}", reg.render(&ids)),
        format!("{} {noun}", ids.len()),
        false,
    )
}

fn fail(text: &str) -> (String, String, bool) {
    (format!("Error: {text}"), "Couldn't read".into(), true)
}

/// Runs one work tool for this asker. Every branch checks the kind is one
/// they may read before reading anything.
pub async fn call(
    ctx: &Ctx,
    reg: &mut common::knowledge::answer::Registry,
    name: &str,
    input: &Value,
) -> (String, String, bool) {
    let refused = || {
        (
            "Your role can't see these in the dashboard, so Chat doesn't read them.".to_string(),
            "Not allowed".to_string(),
            true,
        )
    };
    let team = ctx.claims.team_id.as_str();
    match name {
        RUNS => {
            if !may(&ctx.readable, "runs") {
                return refused();
            }
            let ask = super::chat_work_runs::RunAsk::from_input(input, chrono::Utc::now());
            match Box::pin(super::chat_work_runs::find(&ctx.state, team, &ask)).await {
                Ok(found) => register(reg, found, "runs", ""),
                Err(e) => {
                    tracing::warn!(error = %e, "Chat: runs lookup failed");
                    fail("the runs couldn't be read just now.")
                }
            }
        }
        TICKETS => {
            if !may(&ctx.readable, "tickets") {
                return refused();
            }
            Box::pin(tickets(ctx, reg, input)).await
        }
        PULL_REQUESTS => {
            if !may(&ctx.readable, "pull_requests") {
                return refused();
            }
            let Some(token) = &ctx.github_token else {
                return fail("Coderhelm can't reach GitHub for this team right now.");
            };
            let ask = match super::chat_work_prs::PrAsk::from_input(input, &ctx.repos) {
                Ok(a) => a,
                Err(e) => return fail(&e),
            };
            let now = chrono::Utc::now().timestamp().max(0) as u64;
            let held = match ctx.installation_id {
                Some(id) => {
                    super::chat_work_prs::hold_from_budget(limited_until(&ctx.state, id).await, now)
                }
                None => None,
            };
            let http = common::mcp_remote::http::GuardedHttps;
            let mut client = super::chat_work_prs::Client {
                http: &http,
                auth: format!("Bearer {token}"),
                base: "https://api.github.com".into(),
                sent: 0,
                held,
            };
            match Box::pin(super::chat_work_prs::find(&mut client, &ask, &ctx.repos)).await {
                Ok(found) => register(reg, found, "pull requests", ""),
                Err(e) => fail(&format!("GitHub couldn't be searched: {e}.")),
            }
        }
        FINDINGS | RECORDS => {
            let k = input["kind"].as_str().unwrap_or("");
            let Some(kind) = kind(k).filter(|x| x.tool == name) else {
                return fail("pick one of the listed kinds.");
            };
            if !may(&ctx.readable, kind.key) {
                return refused();
            }
            let default_state = if name == FINDINGS { "open" } else { "any" };
            let ask = super::chat_work_records::RecordAsk::from_input(input, default_state);
            match Box::pin(super::chat_work_records::fetch(
                &ctx.state,
                &ctx.claims,
                &ctx.holder,
                &ask,
            ))
            .await
            {
                Ok(rows) => register(
                    reg,
                    super::chat_work_records::pick(kind.key, &rows, &ask),
                    kind.label,
                    if name == FINDINGS && ask.state == "open" {
                        "\n(Only open ones: ask with state \"any\" for fixed and dismissed ones too.)"
                    } else {
                        ""
                    },
                ),
                Err(e) => fail(&e),
            }
        }
        _ => fail("unknown tool."),
    }
}

/// Tickets: from Jira by key or text, and from the runs that worked on them.
async fn tickets(
    ctx: &Ctx,
    reg: &mut common::knowledge::answer::Registry,
    input: &Value,
) -> (String, String, bool) {
    use super::chat_work_runs as runs;
    use super::chat_work_tickets as t;
    let key = input["key"]
        .as_str()
        .map(|k| k.trim().to_ascii_uppercase())
        .filter(|k| t::valid_key(k));
    let query = input["query"]
        .as_str()
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .map(str::to_string);
    if key.is_none() && query.is_none() {
        return fail("give a ticket key or words of its title.");
    }
    // The runs on it: by key, else by the title's words.
    let mut ask = runs::RunAsk::from_input(&json!({}), chrono::Utc::now());
    match &key {
        Some(k) => ask.ticket = Some(k.to_lowercase()),
        None => ask.words = words_of(query.as_deref().unwrap_or("")),
    }
    let found_runs = Box::pin(runs::find(&ctx.state, &ctx.claims.team_id, &ask))
        .await
        .unwrap_or_default();
    let site = ctx
        .jira
        .as_ref()
        .map(|j| j.site.clone())
        .unwrap_or_default();
    let Some(app) = &ctx.jira else {
        return register(
            reg,
            t::from_runs(&found_runs, &site, "The team's Jira isn't connected to Coderhelm, so this is what Coderhelm's runs know."),
            "tickets",
            "",
        );
    };
    let http = common::mcp_remote::http::GuardedHttps;
    let mut keys: Vec<String> = key.iter().cloned().collect();
    let mut issues: Vec<Value> = Vec::new();
    let mut jira_error: Option<String> = None;
    if keys.is_empty() {
        // Jira's text search, then the keys the matching runs worked on.
        match t::ask_jira(&http, app, &[], query.as_deref()).await {
            Ok(found) => issues = found,
            Err(e) => jira_error = Some(e),
        }
        for k in t::keys_of(&found_runs) {
            if !issues.iter().any(|i| i["key"] == k.as_str()) {
                keys.push(k);
            }
        }
    }
    if !keys.is_empty() && jira_error.is_none() {
        match t::ask_jira(&http, app, &keys, None).await {
            Ok(found) => issues.extend(found),
            Err(e) => jira_error = Some(e),
        }
    }
    issues.truncate(t::MAX_RESULTS);
    let mut out: Vec<Found> = issues
        .iter()
        .map(|i| t::from_jira(i, &site, &found_runs))
        .collect();
    // Tickets the runs know that Jira didn't return (another tracker, or
    // Jira out of reach).
    let why = match &jira_error {
        Some(e) => format!("Jira couldn't be read ({e}), so this is what Coderhelm's runs know."),
        None => "Jira didn't return this ticket, so this is what Coderhelm's runs know.".into(),
    };
    for r in t::from_runs(&found_runs, &site, &why) {
        let k = r.title.split(' ').next().unwrap_or("").to_string();
        if !out
            .iter()
            .any(|o| o.title.split(' ').next() == Some(k.as_str()))
            && out.len() < t::MAX_RESULTS
        {
            out.push(r);
        }
    }
    let note = if found_runs.is_empty() {
        String::new()
    } else {
        format!(
            "\n(Coderhelm has {} run(s) on these: look them up with {RUNS} for what each did and why it stopped.)",
            found_runs.len()
        )
    };
    register(reg, out, "tickets", &note)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<&'static str> {
        KINDS.iter().map(|k| k.key).collect()
    }

    #[test]
    fn every_kind_names_a_routed_handler_and_its_tool() {
        let routed: Vec<&String> = crate::permissions::api_route_handlers().values().collect();
        for k in KINDS {
            assert!(
                routed.iter().any(|h| h.as_str() == k.handler),
                "{} names {}, which no route serves",
                k.key,
                k.handler
            );
            assert!(is_work_tool(k.tool));
        }
    }

    #[test]
    fn a_viewer_reads_only_what_the_dashboard_shows_a_viewer() {
        let viewer = readable(&Holder::builtin("viewer"));
        let admin = readable(&Holder::builtin("admin"));
        assert!(viewer.contains(&"runs") && viewer.contains(&"tickets"));
        for k in KINDS {
            let allowed = matches!(
                resolve_access(&Holder::builtin("viewer"), Some(k.handler)),
                Access::Allow { .. }
            );
            assert_eq!(viewer.contains(&k.key), allowed, "{}", k.key);
        }
        assert_eq!(admin.len(), KINDS.len(), "{admin:?}");
        // A role without "see runs" reads no runs or tickets; the security
        // pages are open to every role in the dashboard, so they stay open.
        let none = Holder {
            role: "custom:x".into(),
            caps: Default::default(),
            custom: None,
        };
        let r = readable(&none);
        assert!(!r.contains(&"runs") && !r.contains(&"tickets"), "{r:?}");
        assert!(r.contains(&"security"));
    }

    #[test]
    fn tools_are_offered_only_for_what_may_be_read() {
        let repos = vec!["acme/web".to_string()];
        let only_runs = defs(&["runs"], &repos, true);
        let names: Vec<&str> = only_runs
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert_eq!(names, [RUNS]);
        let every = defs(&all(), &repos, true);
        assert_eq!(every.len(), TOOLS.len());
        // No GitHub token: no pull request tool.
        let no_gh = defs(&all(), &repos, false);
        assert!(!no_gh.iter().any(|t| t["name"] == PULL_REQUESTS));
        // A finding kind not readable isn't in the tool's choices.
        let some = defs(&["infra"], &repos, true);
        assert_eq!(
            some[0]["input_schema"]["properties"]["kind"]["enum"],
            json!(["infra"])
        );
        for t in &every {
            assert!(t["description"].as_str().unwrap().len() > 40);
        }
    }

    #[test]
    fn the_hints_put_the_right_tool_first_and_say_where_to_look() {
        let h = super::super::chat_hints::read(
            "What's the latest on \u{201c}Embed the booking portal on the signup pages\u{201d}?",
        );
        let note = hint_note(&h, &TOOLS);
        assert!(
            note.contains(
                "work_tickets with query \"Embed the booking portal on the signup pages\""
            ),
            "{note}"
        );
        assert!(note.contains("work_runs with query"));
        let mut tools = vec![
            json!({"name": "github__search_code"}),
            json!({"name": RUNS}),
            json!({"name": TICKETS}),
        ];
        put_first(&mut tools, &h);
        assert_eq!(tools[0]["name"], TICKETS);
        let h = super::super::chat_hints::read("what's running now?");
        put_first(&mut tools, &h);
        assert_eq!(tools[0]["name"], RUNS);
        assert!(hint_note(&h, &TOOLS).contains("status \"active\""));
        let h = super::super::chat_hints::read("How is harbor-api put together?");
        assert!(hint_note(&h, &TOOLS).is_empty());
    }

    #[test]
    fn labels_read_plainly() {
        assert_eq!(
            label(TICKETS, &json!({"query": "Embed the portal"})),
            "Looked up tickets for \u{201c}Embed the portal\u{201d}"
        );
        assert_eq!(
            label(RUNS, &json!({"status": "active"})),
            "Looked up what's running now"
        );
        assert_eq!(
            label(FINDINGS, &json!({"kind": "infra"})),
            "Looked up infrastructure findings"
        );
        assert_eq!(
            words_of("Embed the Portal, on sign-up pages!"),
            ["embed", "portal", "sign-up", "pages"]
        );
    }
}
