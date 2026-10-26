//! The event catalog: every event type CoderHelm sends to a team's webhook
//! endpoints, the shape of its `data`, and an example.
//!
//! The catalog is the contract. The OpenAPI document's `webhooks` section,
//! the endpoint settings page's event picker and the docs are all made from
//! it, and the tests below check that each example matches its own schema.
//! A field is only ever added to an event within one [`API_VERSION`]; a
//! rename or removal means a new version.

use serde_json::{json, Map, Value};

/// The version every event carries as `api_version`.
pub const API_VERSION: &str = "2026-10-26";

/// The event sent by "Send test event". Never emitted by real work, and
/// delivered only to the endpoint it was sent to.
pub const TEST_EVENT: &str = "webhook.test";

/// A field's type.
#[derive(Debug, Clone, Copy)]
pub enum Kind {
    String,
    /// An RFC 3339 time.
    Time,
    /// An absolute URL.
    Url,
    Integer,
    Boolean,
    /// One of these strings.
    Enum(&'static [&'static str]),
    StringArray,
}

/// One field of an event's `data`.
#[derive(Debug, Clone, Copy)]
pub struct Field {
    pub name: &'static str,
    pub kind: Kind,
    /// May be `null` (it is always present).
    pub nullable: bool,
    pub description: &'static str,
}

const fn f(name: &'static str, kind: Kind, description: &'static str) -> Field {
    Field {
        name,
        kind,
        nullable: false,
        description,
    }
}

const fn opt(name: &'static str, kind: Kind, description: &'static str) -> Field {
    Field {
        name,
        kind,
        nullable: true,
        description,
    }
}

/// One event type.
#[derive(Debug, Clone, Copy)]
pub struct EventType {
    pub name: &'static str,
    /// Groups the picker and the docs by what the event is about.
    pub group: &'static str,
    pub summary: &'static str,
    /// When it is sent, in plain words.
    pub description: &'static str,
    pub fields: &'static [Field],
    /// Sent only to teams with this feature turned on.
    pub feature: Option<&'static str>,
}

pub const RUN_STATUSES: &[&str] = &[
    "running",
    "awaiting_ci",
    "needs_input",
    "needs_outside_change",
    "completed",
    "merged",
    "failed",
    "cancelled",
    "closed",
];

/// A run, as every `run.*` event carries it.
pub const RUN_FIELDS: &[Field] = &[
    f(
        "run_id",
        Kind::String,
        "The run's id. `GET /api/runs/{run_id}` returns the whole run.",
    ),
    f(
        "status",
        Kind::Enum(RUN_STATUSES),
        "The run's status when the event happened.",
    ),
    f("title", Kind::String, "The ticket or task title."),
    f(
        "repo",
        Kind::String,
        "`owner/name` of the repository the run works in.",
    ),
    f(
        "source",
        Kind::String,
        "Where the work came from: `github`, `jira` or `alert` today; more may be added.",
    ),
    f(
        "ticket_id",
        Kind::String,
        "The ticket's key or number at its source (empty for a task without one).",
    ),
    opt(
        "pr_number",
        Kind::Integer,
        "The pull request CoderHelm opened, once there is one.",
    ),
    opt("pr_url", Kind::Url, "The pull request's address on GitHub."),
    opt("branch", Kind::String, "The branch CoderHelm works on."),
    opt(
        "error",
        Kind::String,
        "Why the run failed or what it needs, in plain words.",
    ),
    f("url", Kind::Url, "The run in the CoderHelm dashboard."),
    f("created_at", Kind::Time, "When the run started."),
    f("updated_at", Kind::Time, "When the run last changed."),
];

const PR_FIELDS: &[Field] = &[
    f("run_id", Kind::String, "The run that opened it."),
    f("repo", Kind::String, "`owner/name`."),
    f("number", Kind::Integer, "The pull request number."),
    f("url", Kind::Url, "The pull request on GitHub."),
    f("title", Kind::String, "The run's title (the ticket or task the pull request is for)."),
    f("branch", Kind::String, "The head branch."),
    opt("base_branch", Kind::String, "The branch it merges into, when the work named one (otherwise the repository's default branch)."),
    f("draft", Kind::Boolean, "Opened as a draft (CoderHelm marks it ready when its checks pass)."),
];

const REVIEW_FIELDS: &[Field] = &[
    f("repo", Kind::String, "`owner/name`."),
    f("pr_number", Kind::Integer, "The pull request reviewed."),
    f("pr_url", Kind::Url, "The pull request on GitHub."),
    f("head_sha", Kind::String, "The commit the review is for."),
    f("verdict", Kind::Enum(&["approve", "request_changes", "comment"]), "What the reviewer decided."),
    f("posted_as", Kind::Enum(&["approve", "request_changes", "comment"]), "How the review was posted on GitHub. On CoderHelm's own pull requests GitHub allows only `comment`, whatever the verdict."),
    f("risk", Kind::String, "The reviewer's risk rating (`low`, `medium`, `high`), or `n/a`."),
    f("trigger", Kind::String, "What asked for the review: `opened`, `synchronize`, `labeled`, `requested`, `run` and others."),
    f("url", Kind::Url, "The review in the CoderHelm dashboard."),
];

const CI_FIX_STARTED: &[Field] = &[
    f(
        "run_id",
        Kind::String,
        "The run whose pull request failed its checks.",
    ),
    f("repo", Kind::String, "`owner/name`."),
    f("pr_number", Kind::Integer, "The pull request."),
    f("branch", Kind::String, "The branch the fix is pushed to."),
    f(
        "attempt",
        Kind::Integer,
        "Which fix this is for the run, from 1.",
    ),
    f(
        "failing_checks",
        Kind::StringArray,
        "The checks that failed.",
    ),
];

const CI_FIX_COMPLETED: &[Field] = &[
    f("run_id", Kind::String, "The run."),
    f("repo", Kind::String, "`owner/name`."),
    f("pr_number", Kind::Integer, "The pull request."),
    f("branch", Kind::String, "The branch."),
    f("attempt", Kind::Integer, "Which fix this was, from 1."),
    f("outcome", Kind::Enum(&["pushed", "no_changes", "failed"]), "`pushed`: a fix was pushed and the checks run again. `no_changes`: nothing to change, so the pull request goes to people. `failed`: the fix could not be made."),
    f("files_changed", Kind::Integer, "Files the fix changed."),
];

const ALERT_FIELDS: &[Field] = &[
    f("alert_id", Kind::String, "The alert's id in CoderHelm."),
    f("kind", Kind::String, "What sent it, e.g. `cloudwatch_alarm`, `datadog`, `sentry`, `generic`."),
    f("title", Kind::String, "The alert's title."),
    f("severity", Kind::Enum(&["alarm", "warning", "resolved", "info"]), "How it reads at a glance."),
    f("outcome", Kind::String, "What CoderHelm did with it: `run_started` (a fix run was queued), `notified`, `duplicate`, `in_flight` (a run for it is already working), `recovered`, `no_open_alert`, `not_alarm`, `no_match`, `route_paused`, `budget`, `alert_budget`, `daily_cap`, `misconfigured` or `error`. More may be added."),
    opt("repo", Kind::String, "The repository the alert is routed to, if any."),
    f("ticket_id", Kind::String, "The alert's ticket: the same alert repeating has the same one, and a run started for it carries it as its `ticket_id`."),
    f("received_at", Kind::Time, "When CoderHelm received it."),
];

const FINDING_FIELDS: &[Field] = &[
    f("finding_id", Kind::String, "The finding's id."),
    f(
        "account_id",
        Kind::String,
        "The AWS account whose logs it was found in.",
    ),
    f("title", Kind::String, "What is wrong, in a line."),
    f(
        "severity",
        Kind::Enum(&["critical", "warning", "info"]),
        "How bad it is.",
    ),
    opt(
        "log_group",
        Kind::String,
        "The log group the matching lines are in.",
    ),
    f("url", Kind::Url, "The finding on the Logs page."),
];

const FINDING_RESOLVED: &[Field] = &[
    f("finding_id", Kind::String, "The finding's id."),
    f("account_id", Kind::String, "The AWS account."),
    f("title", Kind::String, "What was wrong."),
    f(
        "severity",
        Kind::Enum(&["critical", "warning", "info"]),
        "How bad it was.",
    ),
    f(
        "resolved_at",
        Kind::Time,
        "When a complete analysis no longer saw it.",
    ),
    f("url", Kind::Url, "The finding on the Logs page."),
];

const RELEASE_FIELDS: &[Field] = &[
    f("repo", Kind::String, "`owner/name`."),
    f("tag", Kind::String, "The release tag."),
    f("sha", Kind::String, "The commit the tag points at."),
    opt(
        "release_url",
        Kind::Url,
        "The GitHub Release, when one was written.",
    ),
    f(
        "channels",
        Kind::StringArray,
        "Where the notes went: `github`, `confluence`, `email`.",
    ),
];

const PLAN_FIELDS: &[Field] = &[
    f("plan_id", Kind::String, "The plan's id. `GET /api/plans/{plan_id}` returns it."),
    f("title", Kind::String, "The plan's title."),
    f("task_count", Kind::Integer, "Tasks approved to run."),
    f("approved_by", Kind::String, "Who approved it: their GitHub login or email, or `api_key:<id>` for a request made with an API key."),
    f("url", Kind::Url, "The plan in the dashboard."),
];

const PLUGIN_RUN_FIELDS: &[Field] = &[
    f("run_id", Kind::String, "The plugin run's id."),
    f("plugin_id", Kind::String, "The plugin."),
    f(
        "status",
        Kind::Enum(&["completed", "failed"]),
        "How it ended.",
    ),
    f(
        "trigger",
        Kind::String,
        "What started it: `manual`, `schedule`, `event` or `api`.",
    ),
    opt("error", Kind::String, "Why it failed."),
];

const TEST_FIELDS: &[Field] = &[
    f("message", Kind::String, "A note that this is a test."),
    f("endpoint_id", Kind::String, "The endpoint it was sent to."),
];

/// Every event type, in the order the docs and the picker list them.
pub const EVENTS: &[EventType] = &[
    EventType {
        name: "run.created",
        group: "Runs",
        summary: "A run started",
        description: "CoderHelm started work on a ticket, task or alert.",
        fields: RUN_FIELDS,
        feature: None,
    },
    EventType {
        name: "run.completed",
        group: "Runs",
        summary: "A run finished",
        description: "The run finished: its pull request passed its checks and is ready for people, or no code change was needed.",
        fields: RUN_FIELDS,
        feature: None,
    },
    EventType {
        name: "run.failed",
        group: "Runs",
        summary: "A run failed",
        description: "The run stopped on an error. `error` says why.",
        fields: RUN_FIELDS,
        feature: None,
    },
    EventType {
        name: "run.needs_input",
        group: "Runs",
        summary: "A run is waiting for a person",
        description: "The run asked a question on its ticket or pull request and waits for an answer. `error` holds the question.",
        fields: RUN_FIELDS,
        feature: None,
    },
    EventType {
        name: "pull_request.opened",
        group: "Pull requests",
        summary: "CoderHelm opened a pull request",
        description: "A run opened a pull request. Pull requests people open are not sent.",
        fields: PR_FIELDS,
        feature: None,
    },
    EventType {
        name: "review.completed",
        group: "Reviews",
        summary: "A review was posted",
        description: "CoderHelm's reviewer posted a review on a pull request, with its verdict.",
        fields: REVIEW_FIELDS,
        feature: None,
    },
    EventType {
        name: "ci_fix.started",
        group: "CI fixes",
        summary: "A CI fix started",
        description: "A pull request CoderHelm opened failed its checks, and CoderHelm started fixing it.",
        fields: CI_FIX_STARTED,
        feature: None,
    },
    EventType {
        name: "ci_fix.completed",
        group: "CI fixes",
        summary: "A CI fix finished",
        description: "The CI fix finished. `outcome` says whether a fix was pushed.",
        fields: CI_FIX_COMPLETED,
        feature: None,
    },
    EventType {
        name: "alert.received",
        group: "Alerts",
        summary: "An alert arrived",
        description: "An alert came in on one of the team's alert addresses, and CoderHelm decided what to do with it.",
        fields: ALERT_FIELDS,
        feature: None,
    },
    EventType {
        name: "log_finding.created",
        group: "Logs",
        summary: "A new log finding",
        description: "The log analysis found a new problem in a connected AWS account's logs.",
        fields: FINDING_FIELDS,
        feature: None,
    },
    EventType {
        name: "log_finding.resolved",
        group: "Logs",
        summary: "A log finding went away",
        description: "A complete log analysis no longer saw a finding that was open.",
        fields: FINDING_RESOLVED,
        feature: None,
    },
    EventType {
        name: "release_notes.published",
        group: "Releases",
        summary: "Release notes went out",
        description: "CoderHelm wrote and published the notes for a release tag.",
        fields: RELEASE_FIELDS,
        feature: None,
    },
    EventType {
        name: "plan.approved",
        group: "Plans",
        summary: "A plan was approved",
        description: "Someone approved a plan and started its tasks (Approve and run in the dashboard, or the API).",
        fields: PLAN_FIELDS,
        feature: None,
    },
    EventType {
        name: "plugin.run.completed",
        group: "Plugins",
        summary: "A plugin run finished",
        description: "A plugin run finished or failed. Sent only to teams with plugins turned on.",
        fields: PLUGIN_RUN_FIELDS,
        feature: Some("plugins"),
    },
    EventType {
        name: TEST_EVENT,
        group: "Testing",
        summary: "A test event",
        description: "Sent by Send test event in Settings, Webhooks (or the API), to that endpoint only. Never sent by real work.",
        fields: TEST_FIELDS,
        feature: None,
    },
];

/// One event type by name.
pub fn find(name: &str) -> Option<&'static EventType> {
    EVENTS.iter().find(|e| e.name == name)
}

/// The event types an endpoint can subscribe to (all but the test event,
/// which every endpoint gets when someone sends it).
pub fn subscribable() -> impl Iterator<Item = &'static EventType> {
    EVENTS.iter().filter(|e| e.name != TEST_EVENT)
}

fn kind_schema(k: Kind) -> Value {
    match k {
        Kind::String => json!({ "type": "string" }),
        Kind::Time => json!({ "type": "string", "format": "date-time" }),
        Kind::Url => json!({ "type": "string", "format": "uri" }),
        Kind::Integer => json!({ "type": "integer" }),
        Kind::Boolean => json!({ "type": "boolean" }),
        Kind::Enum(values) => json!({ "type": "string", "enum": values }),
        Kind::StringArray => json!({ "type": "array", "items": { "type": "string" } }),
    }
}

/// The JSON Schema (2020-12, as OpenAPI 3.1 uses) of an event's `data`.
pub fn data_schema(e: &EventType) -> Value {
    let mut props = Map::new();
    for field in e.fields {
        let mut s = kind_schema(field.kind);
        if field.nullable {
            let t = s["type"].clone();
            s["type"] = json!([t, "null"]);
            if let Some(values) = s.get_mut("enum").and_then(|v| v.as_array_mut()) {
                values.push(Value::Null);
            }
        }
        s["description"] = json!(field.description);
        props.insert(field.name.into(), s);
    }
    json!({
        "type": "object",
        "properties": props,
        "required": e.fields.iter().map(|f| f.name).collect::<Vec<_>>(),
    })
}

/// The JSON Schema of a whole event (the envelope around `data`).
pub fn event_schema(e: &EventType) -> Value {
    json!({
        "type": "object",
        "required": ["id", "type", "created_at", "team_id", "api_version", "data"],
        "properties": {
            "id": { "type": "string", "description": "The event's id (`evt_` and a ULID). The same on every retry and every endpoint: use it to ignore an event you already handled. Also sent as the `webhook-id` header." },
            "type": { "type": "string", "const": e.name, "description": "The event type." },
            "created_at": { "type": "string", "format": "date-time", "description": "When it happened." },
            "team_id": { "type": "string", "description": "The CoderHelm team it belongs to (as `GET /api/me` returns it)." },
            "api_version": { "type": "string", "const": API_VERSION, "description": "The version of this event's shape." },
            "data": data_schema(e),
        },
    })
}

const EXAMPLE_RUN_ID: &str = "01JBRUN7Q2X9M3V6T8W1K4HZ5C";
const EXAMPLE_TEAM: &str = "TEAM#7c1f0e2a";

fn run_example(status: &str, pr: bool, error: Option<&str>) -> Value {
    json!({
        "run_id": EXAMPLE_RUN_ID,
        "status": status,
        "title": "Add rate limiting to the export endpoint",
        "repo": "example-org/api",
        "source": "jira",
        "ticket_id": "ENG-142",
        "pr_number": if pr { json!(318) } else { Value::Null },
        "pr_url": if pr { json!("https://github.com/example-org/api/pull/318") } else { Value::Null },
        "branch": if pr { json!("coderhelm/eng-142-rate-limit-export") } else { Value::Null },
        "error": error,
        "url": format!("https://coderhelm.example.com/runs/detail/?id={EXAMPLE_RUN_ID}"),
        "created_at": "2026-10-26T14:02:11.000Z",
        "updated_at": "2026-10-26T14:31:47.000Z",
    })
}

/// An example `data` for an event type.
pub fn example_data(name: &str) -> Value {
    match name {
        "run.created" => run_example("running", false, None),
        "run.completed" => run_example("completed", true, None),
        "run.failed" => run_example(
            "failed",
            false,
            Some("The repository's install step failed: npm ERR! 404 Not Found - @example/internal-ui"),
        ),
        "run.needs_input" => run_example(
            "needs_input",
            false,
            Some("Should the limit apply per API key or per team?"),
        ),
        "pull_request.opened" => json!({
            "run_id": EXAMPLE_RUN_ID,
            "repo": "example-org/api",
            "number": 318,
            "url": "https://github.com/example-org/api/pull/318",
            "title": "Add rate limiting to the export endpoint",
            "branch": "coderhelm/eng-142-rate-limit-export",
            "base_branch": "main",
            "draft": true,
        }),
        "review.completed" => json!({
            "repo": "example-org/web",
            "pr_number": 902,
            "pr_url": "https://github.com/example-org/web/pull/902",
            "head_sha": "9f1c2ab7e04d3c5a8b6f0e1d2c3b4a5f6e7d8c9b",
            "verdict": "request_changes",
            "posted_as": "request_changes",
            "risk": "medium",
            "trigger": "opened",
            "url": "https://coderhelm.example.com/reviewer/?repo=example-org%2Fweb&pr=902",
        }),
        "ci_fix.started" => json!({
            "run_id": EXAMPLE_RUN_ID,
            "repo": "example-org/api",
            "pr_number": 318,
            "branch": "coderhelm/eng-142-rate-limit-export",
            "attempt": 1,
            "failing_checks": ["test (20.x)", "lint"],
        }),
        "ci_fix.completed" => json!({
            "run_id": EXAMPLE_RUN_ID,
            "repo": "example-org/api",
            "pr_number": 318,
            "branch": "coderhelm/eng-142-rate-limit-export",
            "attempt": 1,
            "outcome": "pushed",
            "files_changed": 2,
        }),
        "alert.received" => json!({
            "alert_id": "01JBALERT3K8P2N6Q9R1S4T7V0",
            "kind": "cloudwatch_alarm",
            "title": "ALARM: api-5xx-rate",
            "severity": "alarm",
            "outcome": "run_started",
            "repo": "example-org/api",
            "ticket_id": "ALERT-3f9c2a71",
            "received_at": "2026-10-26T09:14:03.000Z",
        }),
        "log_finding.created" => json!({
            "finding_id": "01JBFIND6W3X8Y1Z4A7B0C2D5E",
            "account_id": "123456789012",
            "title": "Unhandled TimeoutError in the invoice worker",
            "severity": "critical",
            "log_group": "/aws/lambda/invoice-worker",
            "url": "https://coderhelm.example.com/logs/?finding=01JBFIND6W3X8Y1Z4A7B0C2D5E",
        }),
        "log_finding.resolved" => json!({
            "finding_id": "01JBFIND6W3X8Y1Z4A7B0C2D5E",
            "account_id": "123456789012",
            "title": "Unhandled TimeoutError in the invoice worker",
            "severity": "critical",
            "resolved_at": "2026-10-27T06:00:12.000Z",
            "url": "https://coderhelm.example.com/logs/?finding=01JBFIND6W3X8Y1Z4A7B0C2D5E",
        }),
        "release_notes.published" => json!({
            "repo": "example-org/api",
            "tag": "v2.14.0",
            "sha": "4b7e9d1c0a2f3e5d6c8b9a0f1e2d3c4b5a6f7e8d",
            "release_url": "https://github.com/example-org/api/releases/tag/v2.14.0",
            "channels": ["github", "email"],
        }),
        "plan.approved" => json!({
            "plan_id": "01JBPLAN2F5G8H1J4K7L0M3N6P",
            "title": "Move exports to the background queue",
            "task_count": 4,
            "approved_by": "lead@example.com",
            "url": "https://coderhelm.example.com/plans/detail/?id=01JBPLAN2F5G8H1J4K7L0M3N6P",
        }),
        "plugin.run.completed" => json!({
            "run_id": "01JBPRUN9Q2R5S8T1U4V7W0X3Y",
            "plugin_id": "release-announcer",
            "status": "completed",
            "trigger": "schedule",
            "error": null,
        }),
        TEST_EVENT => json!({
            "message": "A test event from CoderHelm. Nothing happened.",
            "endpoint_id": "ep_01JBENDPT4A7B0C3D6E9F2G5H8",
        }),
        _ => json!({}),
    }
}

/// A whole example event of a type.
pub fn example_event(name: &str) -> Value {
    json!({
        "id": "evt_01JBEVNT8M1N4P7Q0R3S6T9V2W",
        "type": name,
        "created_at": "2026-10-26T14:31:47.512Z",
        "team_id": EXAMPLE_TEAM,
        "api_version": API_VERSION,
        "data": example_data(name),
    })
}

/// Whether a stored subscription list names only real event types (`*` is
/// every event).
pub fn unknown_types(types: &[String]) -> Vec<String> {
    types
        .iter()
        .filter(|t| t.as_str() != "*" && !subscribable().any(|e| e.name == t.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
pub(crate) fn conforms(schema: &Value, v: &Value) -> Result<(), String> {
    if let Some(c) = schema.get("const") {
        if c != v {
            return Err(format!("expected {c}, got {v}"));
        }
    }
    let types: Vec<&str> = match &schema["type"] {
        Value::String(s) => vec![s.as_str()],
        Value::Array(a) => a.iter().filter_map(|t| t.as_str()).collect(),
        _ => vec![],
    };
    let ok_type = types.iter().any(|t| match *t {
        "string" => v.is_string(),
        "integer" => v.is_i64() || v.is_u64(),
        "boolean" => v.is_boolean(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        "null" => v.is_null(),
        _ => false,
    });
    if !types.is_empty() && !ok_type {
        return Err(format!("{v} is not {types:?}"));
    }
    if let Some(values) = schema.get("enum").and_then(|e| e.as_array()) {
        if !values.contains(v) {
            return Err(format!("{v} is not one of {values:?}"));
        }
    }
    if let (Some(props), Some(obj)) = (
        schema.get("properties").and_then(|p| p.as_object()),
        v.as_object(),
    ) {
        for req in schema["required"].as_array().into_iter().flatten() {
            let k = req.as_str().unwrap_or("");
            if !obj.contains_key(k) {
                return Err(format!("missing {k}"));
            }
        }
        for k in obj.keys() {
            if !props.contains_key(k) {
                return Err(format!("{k} is not in the schema"));
            }
        }
        for (k, s) in props {
            if let Some(x) = obj.get(k) {
                conforms(s, x).map_err(|e| format!("{k}: {e}"))?;
            }
        }
    }
    if let (Some(items), Some(arr)) = (schema.get("items"), v.as_array()) {
        for x in arr {
            conforms(items, x)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_example_matches_its_schema() {
        for e in EVENTS {
            let ex = example_event(e.name);
            conforms(&event_schema(e), &ex).unwrap_or_else(|err| panic!("{}: {err}", e.name));
        }
    }

    #[test]
    fn names_are_unique_dotted_and_described() {
        let mut seen = std::collections::BTreeSet::new();
        for e in EVENTS {
            assert!(seen.insert(e.name), "{} twice", e.name);
            assert!(e.name.contains('.') && e.name == e.name.to_ascii_lowercase());
            assert!(!e.description.is_empty() && !e.summary.is_empty());
            for field in e.fields {
                assert!(
                    field.description.ends_with('.'),
                    "{}.{}",
                    e.name,
                    field.name
                );
            }
        }
        assert!(find("run.completed").is_some());
        assert!(find("run.nope").is_none());
        assert!(!subscribable().any(|e| e.name == TEST_EVENT));
    }

    /// Every source file of the services and the log analyzer, without
    /// their test modules, as (path, text).
    fn sources() -> Vec<(String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut out = Vec::new();
        let mut stack = vec![
            root.join("common/src"),
            root.join("gateway/src"),
            root.join("worker/src"),
        ];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&p).unwrap();
                    // Code only: a test module's literals don't count.
                    let code = text
                        .split("#[cfg(test)]\nmod tests")
                        .next()
                        .unwrap()
                        .to_string();
                    out.push((p.display().to_string(), code));
                }
            }
        }
        let py = root.join("../lambda/log-analyzer/handler.py");
        out.push((
            py.display().to_string(),
            std::fs::read_to_string(py).unwrap(),
        ));
        out
    }

    /// The catalog lists only events something really sends: each type is
    /// named at an emit in the code (the run events through
    /// `worker::passes::webhooks::emit_run`, by the status written).
    #[test]
    fn every_event_type_has_an_emit_in_the_code() {
        let src = sources();
        let named = |needle: &str| {
            src.iter()
                .filter(|(p, _)| !p.ends_with("webhooks/catalog.rs"))
                .any(|(_, t)| t.contains(needle))
        };
        for e in subscribable() {
            assert!(
                named(&format!("\"{}\"", e.name)),
                "{} is in the catalog but nothing emits it",
                e.name
            );
        }
        for status in ["running", "completed", "failed", "needs_input"] {
            assert!(
                named(&format!(
                    "emit_run(state, &msg.team_id, run_id, \"{status}\")"
                )),
                "no run is emitted with status {status}"
            );
        }
    }

    /// The log analyzer builds events itself, in Python: same version, same
    /// envelope.
    #[test]
    fn the_log_analyzer_sends_the_same_version_and_envelope() {
        let py = sources().pop().unwrap().1;
        assert!(
            py.contains(&format!("WEBHOOK_API_VERSION = \"{API_VERSION}\"")),
            "lambda/log-analyzer/handler.py WEBHOOK_API_VERSION must be {API_VERSION}"
        );
        for key in [
            "\"id\":",
            "\"type\":",
            "\"created_at\":",
            "\"team_id\":",
            "\"api_version\":",
            "\"data\":",
        ] {
            assert!(py.contains(key), "the analyzer's envelope has no {key}");
        }
        let msg: crate::messages::WorkerMessage = serde_json::from_value(json!({
            "type": "webhook_emit",
            "event": example_event("log_finding.created"),
        }))
        .unwrap();
        assert!(matches!(
            msg,
            crate::messages::WorkerMessage::WebhookEmit(_)
        ));
    }

    #[test]
    fn subscriptions_name_real_types() {
        assert!(unknown_types(&["*".into(), "run.failed".into()]).is_empty());
        assert_eq!(
            unknown_types(&["run.exploded".into(), TEST_EVENT.into()]),
            vec!["run.exploded".to_string(), TEST_EVENT.to_string()]
        );
    }

    #[test]
    fn a_nullable_field_allows_null_and_its_type() {
        let s = data_schema(find("run.completed").unwrap());
        assert_eq!(
            s["properties"]["pr_number"]["type"],
            json!(["integer", "null"])
        );
        assert!(conforms(&s["properties"]["pr_number"], &Value::Null).is_ok());
        assert!(conforms(&s["properties"]["pr_number"], &json!("x")).is_err());
        assert!(conforms(&s["properties"]["status"], &json!("exploded")).is_err());
    }
}
