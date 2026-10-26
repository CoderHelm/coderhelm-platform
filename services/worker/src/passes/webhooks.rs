//! Outbound webhooks on the worker: emitting events from the work, sending
//! them (`webhook_emit`), the retries (`webhook_send`), and telling admins
//! when an endpoint is turned off.
//!
//! The rules live in `common::webhooks`; this file only connects them to
//! the worker's clients.

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_dynamodb::types::AttributeValue;
use common::messages::{WebhookEmitMessage, WebhookSendMessage};
use common::webhooks::{self, endpoint, event, Mode};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Queues one event for the team's endpoints. Never fails or slows the work
/// for long: a failure is logged.
pub async fn emit(state: &WorkerState, team_id: &str, event_type: &str, data: Value) {
    webhooks::emit_event(
        &state.sqs,
        &state.config.webhook_queue_url,
        team_id,
        event_type,
        data,
    )
    .await;
}

/// The dashboard's address.
fn app_url(state: &WorkerState) -> &'static str {
    common::public_urls::app_url(&state.config.stage)
}

/// A run's `data`, from its record (pure: the tests check it against the
/// catalog's schema).
pub fn run_data(item: &HashMap<String, AttributeValue>, status: &str, app: &str) -> Value {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let opt = |k: &str| Some(s(k)).filter(|v| !v.is_empty());
    let pr = item
        .get("pr_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|n| *n > 0);
    let run_id = s("run_id");
    let error = opt("error_message").or_else(|| opt("error"));
    let created = opt("created_at").unwrap_or_default();
    json!({
        "run_id": run_id,
        "status": status,
        "title": s("title"),
        "repo": s("repo"),
        "source": opt("ticket_source").unwrap_or_else(|| "github".into()),
        "ticket_id": s("ticket_id"),
        "pr_number": pr,
        "pr_url": if pr.is_some() { opt("pr_url") } else { None },
        "branch": opt("branch"),
        "error": if matches!(status, "failed" | "needs_input") { error } else { None },
        "url": format!("{}/runs/detail/?id={}", app.trim_end_matches('/'), urlencoding::encode(&run_id)),
        "created_at": created.clone(),
        "updated_at": opt("updated_at").unwrap_or(created),
    })
}

/// The `run.*` event a status write means, if any.
pub fn run_event_for(status: &str) -> Option<&'static str> {
    match status {
        "completed" | "success" => Some("run.completed"),
        "failed" => Some("run.failed"),
        "needs_input" => Some("run.needs_input"),
        "running" => Some("run.created"),
        _ => None,
    }
}

/// Emits a `run.*` event for a run, read back from its record so every run
/// event carries the same fields. `status` is what the work just wrote.
pub async fn emit_run(state: &WorkerState, team_id: &str, run_id: &str, status: &str) {
    let Some(event_type) = run_event_for(status) else {
        return;
    };
    if state.config.webhook_queue_url.is_empty() {
        return;
    }
    let read = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.into()))
        .key("run_id", AttributeValue::S(run_id.into()))
        .send();
    let item = match tokio::time::timeout(Duration::from_secs(3), read).await {
        Ok(Ok(r)) => r.item().cloned().unwrap_or_default(),
        _ => {
            warn!(run_id, "Could not read the run for its webhook event");
            HashMap::new()
        }
    };
    let mut item = item;
    item.entry("run_id".into())
        .or_insert_with(|| AttributeValue::S(run_id.into()));
    let shown = if status == "success" {
        "completed"
    } else {
        status
    };
    emit(
        state,
        team_id,
        event_type,
        run_data(&item, shown, app_url(state)),
    )
    .await;
}

/// `pull_request.opened`'s `data` (pure).
pub fn pr_opened_data(
    run_id: &str,
    repo: &str,
    title: &str,
    base_branch: &str,
    pr: &super::pr::PrResult,
) -> Value {
    json!({
        "run_id": run_id,
        "repo": repo,
        "number": pr.pr_number,
        "url": pr.pr_url,
        "title": title,
        "branch": pr.branch,
        "base_branch": Some(base_branch).filter(|b| !b.is_empty()),
        "draft": pr.draft,
    })
}

/// Emits `pull_request.opened` for a PR a run just opened.
pub async fn emit_pr_opened(
    state: &WorkerState,
    msg: &common::messages::TicketMessage,
    run_id: &str,
    repo: &str,
    pr: &super::pr::PrResult,
) {
    emit(
        state,
        &msg.team_id,
        "pull_request.opened",
        pr_opened_data(run_id, repo, &msg.title, &msg.base_branch, pr),
    )
    .await;
}

/// A GitHub review event or verdict in the catalog's words.
pub fn verdict_word(v: &str) -> &'static str {
    match v.to_ascii_uppercase().as_str() {
        "APPROVE" | "APPROVED" => "approve",
        "REQUEST_CHANGES" | "CHANGES_REQUESTED" => "request_changes",
        _ => "comment",
    }
}

/// `review.completed`'s `data` (pure).
#[allow(clippy::too_many_arguments)]
pub fn review_data(
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    verdict: &str,
    posted_as: &str,
    risk: &str,
    trigger: &str,
    app: &str,
) -> Value {
    json!({
        "repo": repo,
        "pr_number": pr_number,
        "pr_url": format!("https://github.com/{repo}/pull/{pr_number}"),
        "head_sha": head_sha,
        "verdict": verdict_word(verdict),
        "posted_as": verdict_word(posted_as),
        "risk": if risk.trim().is_empty() { "n/a".to_string() } else { risk.trim().to_ascii_lowercase() },
        "trigger": trigger,
        "url": format!(
            "{}/reviewer/?repo={}&pr={pr_number}",
            app.trim_end_matches('/'),
            urlencoding::encode(repo)
        ),
    })
}

/// Emits `review.completed` for a review the reviewer posted.
pub async fn emit_review(
    state: &WorkerState,
    msg: &common::messages::ReviewMessage,
    head_sha: &str,
    verdict: &str,
    posted_as: &str,
    risk: &str,
) {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    emit(
        state,
        &msg.team_id,
        "review.completed",
        review_data(
            &repo,
            msg.pr_number,
            head_sha,
            verdict,
            posted_as,
            risk,
            &msg.trigger,
            app_url(state),
        ),
    )
    .await;
}

/// Where a CI fix is.
pub struct CiFix<'a> {
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub repo: &'a str,
    pub pr_number: u64,
    pub branch: &'a str,
    /// From 1.
    pub attempt: usize,
}

/// `ci_fix.started`'s `data` (pure).
pub fn ci_fix_started_data(f: &CiFix<'_>, failing_checks: &[String]) -> Value {
    json!({
        "run_id": f.run_id,
        "repo": f.repo,
        "pr_number": f.pr_number,
        "branch": f.branch,
        "attempt": f.attempt,
        "failing_checks": failing_checks,
    })
}

/// `ci_fix.completed`'s `data` (pure). `outcome` is `pushed`, `no_changes`
/// or `failed`.
pub fn ci_fix_completed_data(f: &CiFix<'_>, outcome: &str, files_changed: usize) -> Value {
    json!({
        "run_id": f.run_id,
        "repo": f.repo,
        "pr_number": f.pr_number,
        "branch": f.branch,
        "attempt": f.attempt,
        "outcome": outcome,
        "files_changed": files_changed,
    })
}

pub async fn emit_ci_fix_started(state: &WorkerState, f: &CiFix<'_>, failing_checks: &[String]) {
    emit(
        state,
        f.team_id,
        "ci_fix.started",
        ci_fix_started_data(f, failing_checks),
    )
    .await;
}

pub async fn emit_ci_fix_completed(
    state: &WorkerState,
    f: &CiFix<'_>,
    outcome: &str,
    files: usize,
) {
    emit(
        state,
        f.team_id,
        "ci_fix.completed",
        ci_fix_completed_data(f, outcome, files),
    )
    .await;
}

/// `plugin.run.completed`'s `data`, for a run that ended (pure). A refusal
/// is a failure with its reason.
pub fn plugin_run_data(rec: &common::plugins::run::RunRecord) -> Option<Value> {
    use common::plugins::run::RunStatus;
    let status = match rec.status {
        RunStatus::Completed => "completed",
        RunStatus::Failed | RunStatus::Refused => "failed",
        RunStatus::Queued | RunStatus::Running => return None,
    };
    Some(json!({
        "run_id": rec.run_id,
        "plugin_id": rec.plugin_id,
        "status": status,
        "trigger": rec.trigger,
        "error": if status == "failed" && !rec.reason.is_empty() { Some(rec.reason.as_str()) } else { None },
    }))
}

fn ctx<'a>(
    state: &'a WorkerState,
    cipher: &'a common::mcp_credentials::Kms<'a>,
) -> webhooks::Ctx<'a> {
    webhooks::Ctx {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        cipher,
        sqs: &state.sqs,
        queue_url: &state.config.webhook_queue_url,
        dns: &webhooks::deliver::SystemDns,
    }
}

fn kms(state: &WorkerState) -> common::mcp_credentials::Kms<'_> {
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

/// `webhook_emit`: keep the event and send it to every endpoint that wants it.
pub async fn dispatch(state: &WorkerState, msg: WebhookEmitMessage) -> Result<(), Error> {
    let cipher = kms(state);
    let c = ctx(state, &cipher);
    let turned_off = webhooks::dispatch(&c, &msg.event).await?;
    for ep in turned_off {
        notify_disabled(state, &msg.event.team_id, &ep).await;
    }
    Ok(())
}

/// What a `webhook_send` should do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    Now,
    /// Not yet: queue it again with this delay.
    Wait(i64),
}

pub fn due(due_at: i64, now: i64) -> Due {
    if due_at > now + 1 {
        Due::Wait(due_at - now)
    } else {
        Due::Now
    }
}

/// `webhook_send`: one more try at a delivery, when it is due.
pub async fn send(state: &WorkerState, msg: WebhookSendMessage) -> Result<(), Error> {
    let cipher = kms(state);
    let c = ctx(state, &cipher);
    let now = chrono::Utc::now().timestamp();
    if let Due::Wait(secs) = due(msg.due_at, now) {
        webhooks::schedule(&c, &msg, secs).await?;
        return Ok(());
    }
    let table = &state.config.settings_table_name;
    let Some(mut d) =
        common::deliveries::get(&state.dynamo, table, &msg.team_id, &msg.delivery_id).await?
    else {
        info!(delivery = %msg.delivery_id, "Webhook delivery record is gone; not retrying");
        return Ok(());
    };
    if d.status.is_ok() || d.status == common::deliveries::Status::Failed {
        // Already delivered (a Resend got there first) or given up.
        return Ok(());
    }
    let Some(st) = endpoint::get(&state.dynamo, table, &msg.team_id, &msg.endpoint_id).await?
    else {
        d.finish(
            common::deliveries::Status::Failed,
            "Endpoint deleted",
            "webhook_disabled",
        );
        d.next_retry_at = None;
        common::deliveries::record(&state.dynamo, table, &msg.team_id, &d).await;
        return Ok(());
    };
    let Some(kept) = event::get(&state.dynamo, table, &msg.team_id, &msg.event_id).await? else {
        d.finish(
            common::deliveries::Status::Failed,
            "The event is no longer kept",
            "webhook_gave_up",
        );
        d.next_retry_at = None;
        common::deliveries::record(&state.dynamo, table, &msg.team_id, &d).await;
        return Ok(());
    };
    let out = webhooks::attempt(
        &c,
        &msg.team_id,
        &st,
        &kept.event.id,
        &kept.body,
        &mut d,
        Mode::Automatic,
    )
    .await;
    if out.disabled_now {
        notify_disabled(state, &msg.team_id, &st.endpoint).await;
    }
    Ok(())
}

/// The chat message for a turned-off endpoint.
pub fn disabled_message(ep: &endpoint::Endpoint, app: &str) -> common::notify::Message {
    use common::notify::{Block, Button, Message, Text, Tone};
    let since = ep.failing_since.as_deref().unwrap_or("days ago");
    Message {
        summary: format!("CoderHelm turned off the webhook endpoint {}", ep.host()),
        blocks: vec![
            Block::Text(Text::new("A webhook endpoint was turned off").bold().color(Tone::Warning)),
            Block::Text(Text::new(format!(
                "Every delivery to {} has failed since {since}, so CoderHelm stopped sending to it. Fix it, turn it back on in Settings, Webhooks, and replay what it missed.",
                ep.url
            ))),
        ],
        buttons: vec![Button::new(
            "Open Webhooks",
            format!("{}/settings/webhooks/", app.trim_end_matches('/')),
        )],
        full_width: false,
    }
}

/// Tells the team's admins (email) and the team's chat channel, when it has
/// one turned on, that an endpoint was turned off.
async fn notify_disabled(state: &WorkerState, team_id: &str, ep: &endpoint::Endpoint) {
    warn!(team_id, endpoint = %ep.id, "Webhook endpoint turned off after failing for days");
    let since = ep.failing_since.clone().unwrap_or_default();
    let days = chrono::DateTime::parse_from_rfc3339(&since)
        .map(|t| (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_days())
        .unwrap_or(webhooks::deliver::DISABLE_AFTER_DAYS);
    let event = crate::clients::email::EmailEvent::WebhookDisabled {
        url: ep.url.clone(),
        host: ep.host(),
        description: ep.description.clone(),
        failing_since: since,
        days,
    };
    if let Err(e) = crate::clients::email::send_notification(state, team_id, event).await {
        warn!(team_id, error = %e, "Could not email admins about a turned-off webhook endpoint");
    }
    // The team's chat channel, when alert notifications are on for it.
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.into()))
        .key(
            "sk",
            AttributeValue::S(common::alert_notify::TEAM_SETTINGS_SK.into()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());
    let Some(item) = item else { return };
    let on = item
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    if !on {
        return;
    }
    let channels = common::notify::store::open_all(&cipher, team_id, &item).await;
    if channels.is_empty() {
        return;
    }
    common::notify::deliver_recorded(
        &common::notify::Http(&state.http),
        &channels,
        &disabled_message(ep, app_url(state)),
        "webhook_disabled",
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| {
                let v = if *k == "pr_number" {
                    AttributeValue::N(v.to_string())
                } else {
                    AttributeValue::S(v.to_string())
                };
                (k.to_string(), v)
            })
            .collect()
    }

    fn schema(name: &str) -> Value {
        webhooks::catalog::data_schema(webhooks::catalog::find(name).unwrap())
    }

    fn conforms(schema: &Value, v: &Value) -> bool {
        // The fields are exactly the schema's, and types and enums match.
        let props = schema["properties"].as_object().unwrap();
        let obj = v.as_object().unwrap();
        props.keys().all(|k| obj.contains_key(k))
            && obj.keys().all(|k| props.contains_key(k))
            && props.iter().all(|(k, p)| {
                let x = &obj[k];
                let types: Vec<&str> = match &p["type"] {
                    Value::String(s) => vec![s.as_str()],
                    Value::Array(a) => a.iter().filter_map(|t| t.as_str()).collect(),
                    _ => vec![],
                };
                let ok = types.iter().any(|t| match *t {
                    "string" => x.is_string(),
                    "integer" => x.is_u64() || x.is_i64(),
                    "null" => x.is_null(),
                    "boolean" => x.is_boolean(),
                    "array" => x.is_array(),
                    _ => false,
                });
                let in_enum = p["enum"].as_array().is_none_or(|e| e.contains(x));
                ok && in_enum
            })
    }

    #[test]
    fn run_events_carry_the_documented_fields() {
        let rec = item(&[
            ("run_id", "01JBRUN"),
            ("title", "Add a limit"),
            ("repo", "example-org/api"),
            ("ticket_source", "jira"),
            ("ticket_id", "ENG-1"),
            ("pr_number", "318"),
            ("pr_url", "https://github.com/example-org/api/pull/318"),
            ("branch", "coderhelm/eng-1"),
            ("error_message", "boom"),
            ("created_at", "2026-10-26T14:02:11Z"),
            ("updated_at", "2026-10-26T14:31:47Z"),
        ]);
        for (status, ev) in [
            ("running", "run.created"),
            ("completed", "run.completed"),
            ("failed", "run.failed"),
            ("needs_input", "run.needs_input"),
        ] {
            let d = run_data(&rec, status, "https://app.example.com/");
            assert!(conforms(&schema(ev), &d), "{ev}: {d}");
            assert_eq!(run_event_for(status), Some(ev));
        }
        let done = run_data(&rec, "completed", "https://app.example.com");
        assert_eq!(
            done["error"],
            Value::Null,
            "only a failure or a question carries one"
        );
        assert_eq!(
            done["url"],
            "https://app.example.com/runs/detail/?id=01JBRUN"
        );
        assert_eq!(done["pr_number"], 318);
        let failed = run_data(&rec, "failed", "https://app.example.com");
        assert_eq!(failed["error"], "boom");
        // A fresh run: no PR yet, nothing optional set.
        let fresh = run_data(
            &item(&[("run_id", "r"), ("created_at", "t")]),
            "running",
            "https://a",
        );
        assert!(conforms(&schema("run.created"), &fresh), "{fresh}");
        assert_eq!(fresh["pr_number"], Value::Null);
        assert_eq!(fresh["updated_at"], "t");
        assert_eq!(run_event_for("awaiting_ci"), None);
        assert_eq!(run_event_for("success"), Some("run.completed"));
    }

    #[test]
    fn pr_opened_carries_the_documented_fields() {
        let pr = super::super::pr::PrResult {
            pr_number: 318,
            pr_url: "https://github.com/example-org/api/pull/318".into(),
            branch: "coderhelm/eng-1".into(),
            draft: true,
            node_id: "PR_x".into(),
        };
        let d = pr_opened_data("r1", "example-org/api", "Add a limit", "", &pr);
        assert!(conforms(&schema("pull_request.opened"), &d), "{d}");
        assert_eq!(d["base_branch"], Value::Null);
        let d = pr_opened_data("r1", "example-org/api", "Add a limit", "main", &pr);
        assert_eq!(d["base_branch"], "main");
        assert_eq!(d["number"], 318);
    }

    #[test]
    fn review_events_carry_the_verdict_in_plain_words() {
        let d = review_data(
            "example-org/web",
            902,
            "9f1c",
            "REQUEST_CHANGES",
            "COMMENT",
            "Medium",
            "opened",
            "https://app.example.com",
        );
        assert!(conforms(&schema("review.completed"), &d), "{d}");
        assert_eq!(d["verdict"], "request_changes");
        assert_eq!(d["posted_as"], "comment");
        assert_eq!(d["risk"], "medium");
        assert_eq!(
            d["url"],
            "https://app.example.com/reviewer/?repo=example-org%2Fweb&pr=902"
        );
        assert_eq!(verdict_word("APPROVE"), "approve");
        assert_eq!(verdict_word("N/A"), "comment");
        assert_eq!(
            review_data("o/r", 1, "s", "APPROVE", "APPROVE", "", "run", "https://a")["risk"],
            "n/a"
        );
    }

    #[test]
    fn ci_fix_events_carry_the_documented_fields() {
        let f = CiFix {
            team_id: "T",
            run_id: "r1",
            repo: "example-org/api",
            pr_number: 318,
            branch: "coderhelm/x",
            attempt: 2,
        };
        let started = ci_fix_started_data(&f, &["lint".into(), "test (20.x)".into()]);
        assert!(conforms(&schema("ci_fix.started"), &started), "{started}");
        for outcome in ["pushed", "no_changes", "failed"] {
            let done = ci_fix_completed_data(&f, outcome, 3);
            assert!(conforms(&schema("ci_fix.completed"), &done), "{done}");
        }
        assert!(!conforms(
            &schema("ci_fix.completed"),
            &ci_fix_completed_data(&f, "exploded", 0)
        ));
    }

    #[test]
    fn plugin_runs_report_how_they_ended() {
        use common::plugins::run::{RunRecord, RunStatus};
        let mut rec = RunRecord {
            run_id: "01JBPRUN".into(),
            plugin_id: "release-announcer".into(),
            trigger: "schedule".into(),
            status: RunStatus::Completed,
            ..Default::default()
        };
        let d = plugin_run_data(&rec).unwrap();
        assert!(conforms(&schema("plugin.run.completed"), &d), "{d}");
        assert_eq!(d["error"], Value::Null);
        rec.status = RunStatus::Refused;
        rec.reason = "The plugin is turned off.".into();
        let d = plugin_run_data(&rec).unwrap();
        assert_eq!(
            (d["status"].as_str(), d["error"].as_str()),
            (Some("failed"), Some("The plugin is turned off."))
        );
        rec.status = RunStatus::Running;
        assert!(plugin_run_data(&rec).is_none());
    }

    #[test]
    fn a_retry_waits_until_it_is_due() {
        assert_eq!(due(1000, 1000), Due::Now);
        assert_eq!(due(1001, 1000), Due::Now);
        assert_eq!(due(5000, 1000), Due::Wait(4000));
        assert_eq!(due(0, 1000), Due::Now);
    }

    #[test]
    fn the_turned_off_message_says_what_to_do() {
        let ep = endpoint::Endpoint {
            id: "ep_1".into(),
            url: "https://hooks.example.com/in".into(),
            failing_since: Some("2026-10-20T00:00:00Z".into()),
            ..Default::default()
        };
        let m = disabled_message(&ep, "https://app.example.com/");
        assert!(m.summary.contains("hooks.example.com"));
        assert_eq!(
            m.buttons[0].url,
            "https://app.example.com/settings/webhooks/"
        );
        let text = format!("{:?}", m.blocks);
        assert!(text.contains("2026-10-20") && text.contains("turn it back on"));
        assert!(!text.contains('→'));
    }
}
