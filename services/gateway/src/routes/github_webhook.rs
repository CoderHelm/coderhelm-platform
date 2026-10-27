use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use common::pr_outcome::human_commit_count;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::auth::verify::verify_github_signature;
use crate::models::{
    AwaitMergeMessage, FeedbackMessage, GraphIndexMessage, MarkReadyMessage, OnboardMessage,
    OnboardRepo, PlanTaskContinueMessage, ReleaseNotesMessage, ResumeMessage, ReviewMessage,
    TicketMessage, TicketSource, WorkerMessage,
};
use crate::AppState;
use common::deliveries::{self, Delivery, Direction, Status};
use common::merge_gate;

/// Look up team_id by GitHub installation_id using the teams table GSI.
/// Returns None if no team has linked this installation yet.
pub async fn resolve_team_by_installation(
    state: &AppState,
    installation_id: u64,
) -> Option<String> {
    let result = match state
        .dynamo
        .query()
        .table_name(&state.config.teams_table_name)
        .index_name("github-installation-index")
        .key_condition_expression("github_installation_id = :iid")
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .limit(10)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(installation_id, error = %e, "Failed to query teams table for installation");
            return None;
        }
    };

    let items = result.items();

    // Several teams can carry the same installation (older sign-ins created a
    // fresh team per person). Pick the OWNER deterministically — never
    // whichever DynamoDB lists first, which routed webhooks to the wrong team.
    if items.len() > 1 {
        let mut candidates: Vec<InstallationCandidate> = items
            .iter()
            .filter_map(|item| {
                let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
                Some(InstallationCandidate {
                    team_id: s("team_id")?,
                    linked_at: s("installation_linked_at").filter(|v| !v.is_empty()),
                    created_at: s("created_at").unwrap_or_default(),
                    members: 0,
                })
            })
            .collect();
        // Member counts are only needed for records linked before
        // `installation_linked_at` existed.
        // A count that fails routes nowhere (the delivery fails visibly and
        // GitHub redelivers) rather than to a team that merely looked biggest
        // because the real one could not be counted.
        if candidates.iter().all(|c| c.linked_at.is_none()) {
            for c in candidates.iter_mut() {
                c.members = match state
                    .dynamo
                    .query()
                    .table_name(&state.config.users_table_name)
                    .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
                    .expression_attribute_values(":pk", attr_s(&c.team_id))
                    .expression_attribute_values(":u", attr_s("USER#"))
                    .select(aws_sdk_dynamodb::types::Select::Count)
                    .send()
                    .await
                {
                    Ok(r) => r.count,
                    Err(e) => {
                        tracing::error!(installation_id, team_id = %c.team_id, error = %e, "Could not count team members — not routing this delivery");
                        return None;
                    }
                };
            }
        }
        let owner = pick_installation_owner(&candidates);
        if first_time_seen_shared(installation_id) {
            tracing::debug!(
                installation_id,
                team_count = candidates.len(),
                owner = owner.as_deref().unwrap_or(""),
                "Installation is linked to several teams — routing to its owner"
            );
        }
        return owner;
    }

    items
        .first()
        .and_then(|item| item.get("team_id").and_then(|v| v.as_s().ok()).cloned())
}

/// One team that carries an installation, as the owner choice sees it.
#[derive(Debug, Clone)]
pub struct InstallationCandidate {
    pub team_id: String,
    /// When this team linked the installation (set on link since the owner
    /// rule exists; absent on older records).
    pub linked_at: Option<String>,
    pub created_at: String,
    pub members: i32,
}

/// The team that owns an installation shared by several teams:
/// 1. the FIRST to link it, when link times are recorded;
/// 2. otherwise (records from before link times) the team with the most
///    members — what routing used before, so existing tenants keep their
///    webhooks — then the oldest team, then the smallest id. Always the same
///    answer for the same records.
pub fn pick_installation_owner(c: &[InstallationCandidate]) -> Option<String> {
    if let Some(first) = c
        .iter()
        .filter(|x| x.linked_at.is_some())
        .min_by(|a, b| (&a.linked_at, &a.team_id).cmp(&(&b.linked_at, &b.team_id)))
    {
        return Some(first.team_id.clone());
    }
    c.iter()
        .min_by(|a, b| {
            b.members
                .cmp(&a.members)
                // Empty created_at sorts last, not first.
                .then_with(|| a.created_at.is_empty().cmp(&b.created_at.is_empty()))
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.team_id.cmp(&b.team_id))
        })
        .map(|x| x.team_id.clone())
}

/// True the first time this process sees `installation_id` shared, so the
/// warning is logged once per installation, not on every webhook.
fn first_time_seen_shared(installation_id: u64) -> bool {
    static SEEN: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<u64>>> =
        std::sync::OnceLock::new();
    SEEN.get_or_init(Default::default)
        .lock()
        .map(|mut s| s.insert(installation_id))
        .unwrap_or(false)
}

/// Look up ALL team_ids linked to a GitHub installation_id.
pub async fn resolve_all_teams_by_installation(
    state: &AppState,
    installation_id: u64,
) -> Vec<String> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.teams_table_name)
        .index_name("github-installation-index")
        .key_condition_expression("github_installation_id = :iid")
        .expression_attribute_values(
            ":iid",
            aws_sdk_dynamodb::types::AttributeValue::N(installation_id.to_string()),
        )
        .send()
        .await;

    match result {
        Ok(r) => r
            .items()
            .iter()
            .filter_map(|item| item.get("team_id").and_then(|v| v.as_s().ok()).cloned())
            .collect(),
        Err(e) => {
            tracing::error!(installation_id, error = %e, "Failed to query teams for uninstall");
            vec![]
        }
    }
}

#[utoipa::path(
    post,
    path = "/webhooks/github",
    tag = "Webhooks",
    operation_id = "receiveGithubEvent",
    summary = "GitHub App events",
    description = "Where GitHub sends every event of the CoderHelm GitHub App. Installing the app \
        sets this up; you only call it yourself when you self-host. Each request is signed with \
        the app's webhook secret in `X-Hub-Signature-256` (`sha256=` and the hex HMAC-SHA256 of \
        the raw body), compared in constant time. The event type comes from `X-GitHub-Event` and \
        the body must name an installation. Events CoderHelm doesn't act on, and events from an \
        installation not linked to a team, are accepted with `200` and ignored.",
    params(
        ("X-GitHub-Event" = String, Header, description = "The event type, such as `issues`, `pull_request` or `push`.", example = "issues"),
        ("X-GitHub-Delivery" = Option<String>, Header, description = "GitHub's id for this delivery."),
    ),
    request_body(content = Object, description = "The event payload, as GitHub defines it for each event type. It must include `installation.id`.", content_type = "application/json"),
    responses(
        (status = 200, description = "Accepted."),
        (status = 400, description = "The body is not JSON or names no installation."),
        (status = 401, description = "The signature is missing or wrong."),
    ),
    security(("github_signature" = [])),
)]
pub async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let event_type = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let mut d = inbound_delivery(&headers, &body, &event_type);

    // Verify signature
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok());
    let verified = signature.is_some_and(|sig| {
        verify_github_signature(&state.secrets.github_webhook_secret, &body, sig)
    });
    if !verified {
        warn!("Invalid GitHub webhook signature");
        // Recorded on a self-hosted install, where a mismatch is a setup
        // problem its admins fix; on the hosted side it's just noise.
        if super::setup_checks::self_hosted() {
            d.finish(
                Status::Failed,
                "Signature didn't match",
                "signature_mismatch",
            );
            super::deliveries::save(&state, super::deliveries::UNROUTED, &d).await;
        }
        return Err(StatusCode::UNAUTHORIZED);
    }

    let payload: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse webhook body: {e}");
        StatusCode::BAD_REQUEST
    })?;

    let installation_id = payload["installation"]["id"]
        .as_u64()
        .ok_or(StatusCode::BAD_REQUEST)?;

    info!(event_type, installation_id, "GitHub webhook received");

    // Hosted side of hybrid: an installation linked to a self-hosted install
    // is forwarded there (re-signed) instead of being handled here.
    let action = payload["action"].as_str().unwrap_or("");
    if let Some(code) = super::hybrid::relay_github(
        &state,
        installation_id,
        &event_type,
        action,
        &headers,
        &body,
    )
    .await
    {
        return Ok(code);
    }

    process(&state, &event_type, &payload, installation_id, &mut d).await
}

/// The delivery record for one GitHub event, before anything is decided.
pub(crate) fn inbound_delivery(headers: &HeaderMap, body: &[u8], event_type: &str) -> Delivery {
    let payload: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let action = payload["action"].as_str().unwrap_or("");
    let event = if action.is_empty() {
        event_type.to_string()
    } else {
        format!("{event_type}.{action}")
    };
    let mut d = Delivery::new(Direction::Inbound, "github", &event, "")
        .with_headers(
            headers
                .iter()
                .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?))),
        )
        .with_payload(body);
    d.external_id = headers
        .get("x-github-delivery")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    d.repo = payload["repository"]["full_name"]
        .as_str()
        .unwrap_or("")
        .to_string();
    d.pr = payload["pull_request"]["number"]
        .as_u64()
        .or_else(|| {
            payload["issue"]["pull_request"]
                .is_object()
                .then(|| payload["issue"]["number"].as_u64())
                .flatten()
        })
        .or_else(|| payload["check_run"]["pull_requests"][0]["number"].as_u64());
    d.redeliver = Some(deliveries::Redeliver::Github);
    d
}

/// The event types CoderHelm acts on (the rest are acknowledged and ignored).
pub const HANDLED_EVENTS: &[&str] = &[
    "installation",
    "installation_repositories",
    "issues",
    "issue_comment",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "push",
    "check_run",
    "check_suite",
    "repository",
    "workflow_run",
];

/// Handles one verified GitHub event (from GitHub, or from CoderHelm's relay
/// on a hybrid install) and records the delivery with what happened.
pub(crate) async fn process(
    state: &AppState,
    event_type: &str,
    payload: &Value,
    installation_id: u64,
    d: &mut Delivery,
) -> Result<StatusCode, StatusCode> {
    let started = std::time::Instant::now();
    let (r, pk) = route(state, event_type, payload, installation_id, d).await;
    d.duration_ms = started.elapsed().as_millis() as u64;
    match (&r, d.status) {
        (Err(code), _) => {
            let reason = if code.as_u16() >= 500 {
                "enqueue_failed"
            } else {
                "handler_error"
            };
            d.finish(Status::Failed, "Handling failed", reason);
        }
        (Ok(_), Status::Received) => {
            if HANDLED_EVENTS.contains(&event_type) {
                d.finish(Status::Handled, "Handled", "");
            } else {
                d.finish(Status::Ignored, "Not an event CoderHelm acts on", "");
            }
        }
        _ => {}
    }
    let pk = if pk.is_empty() && super::setup_checks::self_hosted() {
        super::deliveries::UNROUTED.to_string()
    } else {
        pk
    };
    if !pk.is_empty() {
        super::deliveries::save(state, &pk, d).await;
    }
    r
}

/// Routes a verified event; returns the result and the team it went to.
async fn route(
    state: &AppState,
    event_type: &str,
    payload: &Value,
    installation_id: u64,
    d: &mut Delivery,
) -> (Result<StatusCode, StatusCode>, String) {
    // Installation events handle their own team resolution/creation
    if event_type == "installation" || event_type == "installation_repositories" {
        let r = if event_type == "installation" {
            handle_installation(state, payload, installation_id).await
        } else {
            handle_installation_repos(state, payload, installation_id).await
        };
        let team = resolve_team_by_installation(state, installation_id)
            .await
            .unwrap_or_default();
        d.target = team.clone();
        return (r, team);
    }

    // For all other events, resolve team_id from the installation
    let team_id = match resolve_team_by_installation(state, installation_id).await {
        Some(tid) => tid,
        None => {
            warn!(
                installation_id,
                event_type, "No team linked to this GitHub installation"
            );
            d.finish(Status::Failed, "No team for this installation", "no_team");
            return (Ok(StatusCode::OK), String::new());
        }
    };
    d.target = team_id.clone();
    let team_id = team_id.as_str();
    let r = dispatch(state, event_type, payload, installation_id, team_id).await;
    (r, team_id.to_string())
}

async fn dispatch(
    state: &AppState,
    event_type: &str,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    // People's pushback on a CoderHelm review (a reply, a PR comment, a
    // dismissal, a close over its change request) queues one judgment per PR.
    super::review_lessons::on_webhook(state, event_type, payload, installation_id, team_id).await;

    match event_type {
        "issues" => handle_issue_event(state, payload, installation_id, team_id).await,
        "issue_comment" => handle_issue_comment(state, payload, installation_id, team_id).await,
        "pull_request" => handle_pull_request(state, payload, installation_id, team_id).await,
        "pull_request_review" => handle_pr_review(state, payload, installation_id, team_id).await,
        "pull_request_review_comment" => {
            handle_review_comment(state, payload, installation_id, team_id).await
        }
        "pull_request_review_thread" => Ok(StatusCode::OK),
        "push" => handle_push(state, payload, installation_id, team_id).await,
        "check_run" => handle_check_run(state, payload, installation_id, team_id).await,
        "check_suite" => handle_check_suite(state, payload, installation_id, team_id).await,
        "repository" => handle_repository_event(state, payload, installation_id, team_id).await,
        "workflow_run" => handle_workflow_run(state, payload, installation_id, team_id).await,
        // Log-only events — acknowledge but no action needed
        "workflow_dispatch" | "workflow_job" => {
            info!(event_type, "Workflow event received — logged");
            Ok(StatusCode::OK)
        }
        "sub_issues" => {
            info!(event_type, "Sub-issue event received — logged");
            Ok(StatusCode::OK)
        }
        "meta" => {
            info!("GitHub App webhook deleted (meta event)");
            Ok(StatusCode::OK)
        }
        "security_advisory" => {
            info!("Security advisory event received — logged");
            Ok(StatusCode::OK)
        }
        _ => {
            info!(event_type, "Ignoring unhandled event type");
            Ok(StatusCode::OK)
        }
    }
}

async fn handle_issue_event(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    // Trigger on: issue assigned to coderhelm[bot], or labeled "coderhelm"
    let is_assigned_to_bot = action == "assigned"
        && payload["assignee"]["login"]
            .as_str()
            .map(|l| l.contains("coderhelm"))
            .unwrap_or(false);

    let is_labeled = action == "labeled"
        && payload["label"]["name"]
            .as_str()
            .map(|l| l.eq_ignore_ascii_case("coderhelm"))
            .unwrap_or(false);

    if !is_assigned_to_bot && !is_labeled {
        return Ok(StatusCode::OK);
    }
    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Github,
        ticket_id: format!("GH-{}", issue["number"].as_u64().unwrap_or(0)),
        title: issue["title"].as_str().unwrap_or("").to_string(),
        body: issue["body"].as_str().unwrap_or("").to_string(),
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        issue_number: issue["number"].as_u64().unwrap_or(0),
        sender: payload["sender"]["login"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        requester: common::requester::Requester::github(
            payload["sender"]["login"].as_str().unwrap_or(""),
        ),
        image_attachments: vec![],
        ..Default::default()
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // Trigger gate: block while a run is in flight; on a completed latest
    // run, re-trigger only if the issue content changed (re-labeling an
    // edited issue reworks it; re-labeling an unchanged one is a no-op).
    // Failed/needs_input runs no longer block — the old any-run-exists check
    // made GitHub tickets unretriable by re-label.
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    let context_hash = common::ticket_context_hash(
        issue["title"].as_str().unwrap_or(""),
        issue["body"].as_str().unwrap_or(""),
        &[],
    );
    match super::trigger_gate::gate_ticket_trigger(
        state,
        team_id,
        &ticket_id_str,
        Some(&context_hash),
        false,
        false,
    )
    .await
    {
        super::trigger_gate::TicketGate::Enqueue => {}
        super::trigger_gate::TicketGate::OpenPr(pr) => {
            // The issue's PR is open: the edit is feedback on it, never a
            // second run.
            let note = super::trigger_gate::edit_note(
                &pr,
                issue["title"].as_str().unwrap_or(""),
                issue["body"].as_str().unwrap_or(""),
            );
            info!(ticket_id = %ticket_id_str, pr = pr.pr_number, "Issue edited while its PR is open — sending to that PR");
            let feedback = WorkerMessage::Feedback(crate::models::FeedbackMessage {
                team_id: team_id.to_string(),
                installation_id,
                run_id: pr.run_id.clone(),
                repo_owner: pr.repo_owner.clone(),
                repo_name: pr.repo_name.clone(),
                pr_number: pr.pr_number,
                review_id: 0,
                review_body: note,
                comments: vec![],
                trigger_author: String::new(),
                ..Default::default()
            });
            return send_to_queue(state, &state.config.ticket_queue_url, &feedback).await;
        }
        _ => {
            info!(ticket_id = %ticket_id_str, "Skipping — trigger gate blocked");
            return Ok(StatusCode::OK);
        }
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// A comment addressed to the reviewer: `@coderhelm …` anywhere outside
/// quoted text and code (any case — GitHub autocompletes the display name,
/// e.g. `@CoderHelm`), or `/coderhelm …` at the start. One parser for every
/// surface: conversation comments, inline thread replies, review bodies.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReviewerAsk {
    /// The exact re-review command (`re-review`, `re review`, `rereview`,
    /// `review`, `review again`, or the bare mention): a fast path that
    /// skips the worker's model classification.
    pub rereview: bool,
    /// Anything else, as written. What it asks for (another look, the
    /// approval, a question, a fix) is decided by the worker's model, never
    /// by keywords here.
    pub question: Option<String>,
}

/// The comment without quoted lines (`> …`), fenced code blocks and inline
/// code: a mention someone quoted or pasted is not addressed to CoderHelm.
pub(crate) fn addressed_text(body: &str) -> String {
    let mut out = String::with_capacity(body.len());
    let mut in_fence = false;
    for line in body.lines() {
        let t = line.trim_start();
        if t.starts_with("```") || t.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || t.starts_with('>') {
            continue;
        }
        // Drop `inline code` spans (every other piece between backticks).
        for (i, part) in line.split('`').enumerate() {
            if i % 2 == 0 {
                out.push_str(part);
            }
        }
        out.push('\n');
    }
    out
}

/// The exact re-review commands (after the mention is stripped).
fn is_rereview_command(text: &str) -> bool {
    let t = text
        .trim()
        .trim_end_matches(['.', '!', '?'])
        .trim()
        .to_lowercase();
    matches!(
        t.as_str(),
        "" | "re-review" | "re review" | "rereview" | "review" | "review again"
    )
}

pub(crate) fn parse_reviewer_ask(body: &str) -> Option<ReviewerAsk> {
    const MENTION: &str = "@coderhelm";
    const SLASH: &str = "/coderhelm";
    // Addressed only when the mention is outside quotes and code.
    let addressed = addressed_text(body).to_ascii_lowercase();
    if !addressed.trim_start().starts_with(SLASH) && !addressed.contains(MENTION) {
        return None;
    }
    // The ask as written, without the quoted lines it replies to.
    let body: String = body
        .lines()
        .filter(|l| !l.trim_start().starts_with('>'))
        .collect::<Vec<_>>()
        .join("\n");
    let body = body.trim();
    // ASCII lowercasing keeps byte offsets identical to `body`.
    let lower = body.to_ascii_lowercase();
    // Strip every address token (and a trailing "[bot]") to recover the message.
    let mut cleaned = String::with_capacity(body.len());
    let mut i = 0;
    while i < body.len() {
        if lower[i..].starts_with(MENTION) {
            i += MENTION.len();
            if lower[i..].starts_with("[bot]") {
                i += "[bot]".len();
            }
            cleaned.push(' ');
            continue;
        }
        let ch = body[i..].chars().next().unwrap_or(' ');
        cleaned.push(ch);
        i += ch.len_utf8();
    }
    let mut cleaned = cleaned.trim().to_string();
    if cleaned.to_ascii_lowercase().starts_with(SLASH) {
        cleaned = cleaned[SLASH.len()..].trim().to_string();
    }
    let rereview = is_rereview_command(&cleaned);
    Some(ReviewerAsk {
        rereview,
        question: if rereview { None } else { Some(cleaned) },
    })
}

/// For an `edited` comment: the ask only when the edit added it (the old
/// text had none), so an edit runs a command once and a typo fix to a
/// comment that already asked doesn't ask again.
pub(crate) fn ask_added_by_edit(old_body: &str, new_body: &str) -> Option<ReviewerAsk> {
    if parse_reviewer_ask(old_body).is_some() {
        return None;
    }
    parse_reviewer_ask(new_body)
}

/// A reply inside an inline review thread (`pull_request_review_comment`). Same
/// asks as a conversation comment — `@coderhelm re-review` / `@coderhelm <question>` —
/// and the answer goes back into that thread. On CoderHelm's own PRs only a
/// re-review ask is handled here; other thread replies there are fix requests,
/// handled by the feedback path (pull_request_review).
async fn handle_review_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    match payload["action"].as_str() {
        Some("created") => {}
        Some("edited") => {
            let old = payload["changes"]["body"]["from"].as_str().unwrap_or("");
            let new = payload["comment"]["body"].as_str().unwrap_or("");
            if ask_added_by_edit(old, new).is_none() {
                return Ok(StatusCode::OK);
            }
        }
        _ => return Ok(StatusCode::OK),
    }
    let commenter = payload["comment"]["user"]["login"].as_str().unwrap_or("");
    if commenter.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }
    let body = payload["comment"]["body"].as_str().unwrap_or("");
    let Some(ask) = parse_reviewer_ask(body) else {
        return Ok(StatusCode::OK);
    };
    if !may_start_work(state, team_id, &payload["comment"], "review thread reply").await {
        return Ok(StatusCode::OK);
    }
    let pr = &payload["pull_request"];
    if let Some(control) = ask.question.as_deref().and_then(merge_gate::parse_control) {
        let repo = &payload["repository"];
        return super::merge_control::on_comment(
            state,
            team_id,
            installation_id,
            repo["owner"]["login"].as_str().unwrap_or(""),
            repo["name"].as_str().unwrap_or(""),
            pr["number"].as_u64().unwrap_or(0),
            commenter,
            control,
            super::merge_control::CommentRef::ReviewThread(
                payload["comment"]["id"].as_u64().unwrap_or(0),
            ),
        )
        .await;
    }
    let bot_pr = pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm");
    if bot_pr && !ask.rereview {
        return Ok(StatusCode::OK);
    }
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let comment_id = payload["comment"]["id"].as_u64().unwrap_or(0);
    let cfg = load_review_config(state, team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed || pr_number == 0 || comment_id == 0 {
        info!(
            owner,
            name, pr_number, "Reviewer: thread reply ignored — review is off for this repo"
        );
        return Ok(StatusCode::OK);
    }
    if let Some(reason) = check_run_budget(state, team_id).await {
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }
    info!(
        owner,
        name,
        pr_number,
        is_rereview = ask.rereview,
        "Reviewer: thread reply → review job"
    );
    if ask.rereview {
        record_rereview(
            state,
            team_id,
            owner,
            name,
            pr_number,
            &format!("reply#{comment_id}"),
        )
        .await;
    }
    let message = WorkerMessage::Review(ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha: String::new(),
        label: cfg.label,
        question: ask.question,
        trigger: "reply".to_string(),
        dedup_key: format!("reply#{comment_id}"),
        reply_to_comment_id: Some(comment_id),
        requested_by: commenter.to_string(),
        ..Default::default()
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// True when the comment's author is a bot account, read from GitHub's
/// structured `comment.user.type` field (not the login text).
fn is_bot_comment(payload: &serde_json::Value) -> bool {
    payload["comment"]["user"]["type"].as_str() == Some("Bot")
}

/// Whether the author of a comment or review (`item` is the `comment` or
/// `review` object) may start a run, re-run, review or fix pass. People with
/// standing in the repository may, and so may a person whose GitHub account
/// is linked to a member of this team. Bot accounts are apps the
/// repository's owners installed (CI, other review tools) and keep working
/// as before. Anyone else is ignored, with an info log.
pub(crate) async fn may_start_work(
    state: &AppState,
    team_id: &str,
    item: &Value,
    surface: &str,
) -> bool {
    if common::standing::shown_by_github(item) {
        return true;
    }
    let association = item["author_association"].as_str().unwrap_or("");
    let login = item["user"]["login"].as_str().unwrap_or("");
    if let Some(github_id) = item["user"]["id"].as_u64() {
        if is_linked_team_member(state, team_id, github_id).await {
            return true;
        }
    }
    info!(
        team_id,
        login,
        association,
        surface,
        "Ignoring request from a person without standing in the repository"
    );
    false
}

/// Is this GitHub account linked to a user of `team_id`? A failed read counts
/// as no (fail closed).
async fn is_linked_team_member(state: &AppState, team_id: &str, github_id: u64) -> bool {
    match state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .index_name("gsi1")
        .key_condition_expression("gsi1pk = :gpk AND gsi1sk = :team")
        .expression_attribute_values(":gpk", attr_s(&format!("GHUSER#{github_id}")))
        .expression_attribute_values(":team", attr_s(team_id))
        .limit(1)
        .send()
        .await
    {
        Ok(r) => !r.items().is_empty(),
        Err(e) => {
            warn!(team_id, github_id, error = %e, "Could not check team membership — treating as not a member");
            false
        }
    }
}

async fn handle_issue_comment(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "created" && action != "edited" {
        return Ok(StatusCode::OK);
    }
    // An edit counts only when it adds a command to the reviewer; every
    // other edit is ignored (the original comment was already handled).
    if action == "edited" {
        let old = payload["changes"]["body"]["from"].as_str().unwrap_or("");
        let new = payload["comment"]["body"].as_str().unwrap_or("");
        if ask_added_by_edit(old, new).is_none() {
            return Ok(StatusCode::OK);
        }
    }

    // Comments from bot accounts (CI coverage/preview/E2E reports, other apps)
    // are never instructions. They used to reach the feedback fixer as
    // review_id 0 and spawn unbudgeted fix cycles.
    if is_bot_comment(payload) {
        return Ok(StatusCode::OK);
    }
    let body = payload["comment"]["body"].as_str().unwrap_or("");
    let commenter = payload["comment"]["user"]["login"].as_str().unwrap_or("");

    // A comment addressed to the bot ("@CoderHelm …" in any case, or "/coderhelm …").
    let ask = if commenter.contains("coderhelm") {
        None
    } else {
        parse_reviewer_ask(body)
    };

    // `@coderhelm stop auto-merge` / `@coderhelm resume auto-merge` on any PR
    // (people's and CoderHelm's own): only from someone with write access.
    if payload["issue"]["pull_request"].is_object() {
        if let Some(control) = ask
            .as_ref()
            .and_then(|a| a.question.as_deref())
            .and_then(merge_gate::parse_control)
        {
            let repo = &payload["repository"];
            return super::merge_control::on_comment(
                state,
                team_id,
                installation_id,
                repo["owner"]["login"].as_str().unwrap_or(""),
                repo["name"].as_str().unwrap_or(""),
                payload["issue"]["number"].as_u64().unwrap_or(0),
                commenter,
                control,
                super::merge_control::CommentRef::Issue(
                    payload["comment"]["id"].as_u64().unwrap_or(0),
                ),
            )
            .await;
        }
    }

    // Only a comment on CoderHelm's own PR, or one addressed to CoderHelm,
    // can start work (a run, a review, a fix pass). Such a comment counts
    // only from a person with standing in the repository or on the team.
    let on_bot_pr = payload["issue"]["pull_request"].is_object()
        && payload["issue"]["user"]["login"]
            .as_str()
            .unwrap_or("")
            .contains("coderhelm")
        && !commenter.contains("coderhelm");
    let lower = body.to_ascii_lowercase();
    let addressed = ask.is_some()
        || lower.trim_start().starts_with("/coderhelm")
        || lower.contains("@coderhelm");
    if !on_bot_pr && !addressed {
        return Ok(StatusCode::OK);
    }
    if !may_start_work(state, team_id, &payload["comment"], "comment").await {
        return Ok(StatusCode::OK);
    }

    // On a PR CoderHelm opened, every person's comment reaches it. One that
    // addresses CoderHelm (`@coderhelm …`) goes to the reviewer when review is
    // on for the repo: a model reads what it asks for there (another look,
    // the approval, a question, a fix) and a fix request continues to the fix
    // loop, which answers the person on the PR. Other conversation goes
    // straight to the fix loop, as before.
    if payload["issue"]["pull_request"].is_object() {
        let pr_user = payload["issue"]["user"]["login"].as_str().unwrap_or("");
        let bot_pr = pr_user.contains("coderhelm") && !commenter.contains("coderhelm");
        let review_takes_ask = if bot_pr && ask.is_some() {
            let repo = &payload["repository"];
            let cfg = load_review_config(
                state,
                team_id,
                repo["owner"]["login"].as_str().unwrap_or(""),
                repo["name"].as_str().unwrap_or(""),
            )
            .await?;
            cfg.enabled && !cfg.killed
        } else {
            false
        };
        if bot_pr && !review_takes_ask {
            let repo = &payload["repository"];
            let owner = repo["owner"]["login"].as_str().unwrap_or("");
            let name = repo["name"].as_str().unwrap_or("");
            let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);

            let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
            if run_id.is_empty() {
                warn!(pr_number, "No run found for PR comment — skipping");
                return Ok(StatusCode::OK);
            }

            // Write event for resume flow
            write_run_event(
                state,
                &run_id,
                "pr_comment",
                &serde_json::json!({
                    "commenter": commenter,
                    "body": body,
                    "pr_number": pr_number,
                }),
            )
            .await;

            info!(pr_number, commenter, "PR comment → feedback queue");
            let message = WorkerMessage::Feedback(FeedbackMessage {
                team_id: team_id.to_string(),
                installation_id,
                run_id,
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                review_id: 0,
                review_body: body.to_string(),
                comments: vec![],
                // A person who addressed CoderHelm gets a reply on the PR;
                // other conversation is applied without a reply to each remark.
                trigger_author: if ask.is_some() {
                    commenter.to_string()
                } else {
                    String::new()
                },
                ..Default::default()
            });
            // Budget gate — the review-feedback path checks this, but the comment
            // path didn't, letting over-limit teams keep spending via PR comments.
            if check_run_budget(state, team_id).await.is_some() {
                info!(team_id, "PR comment feedback skipped — token limit reached");
                return Ok(StatusCode::OK);
            }
            return send_to_queue(state, &state.config.feedback_queue_url, &message).await;
        }

        // Reviewer reply-handling: on a PR the reviewer covers, a human addressing
        // the bot ("@coderhelm re-review" or "@coderhelm <question>") re-runs the
        // review or answers the question — instead of opening a brand-new ticket.
        // Requires an explicit mention/command so unrelated PR chatter never fires.
        if let Some(ask) = ask {
            let repo = &payload["repository"];
            let owner = repo["owner"]["login"].as_str().unwrap_or("");
            let name = repo["name"].as_str().unwrap_or("");
            let pr_number = payload["issue"]["number"].as_u64().unwrap_or(0);
            let cfg = load_review_config(state, team_id, owner, name).await?;
            if cfg.enabled && !cfg.killed && pr_number != 0 {
                if let Some(reason) = check_run_budget(state, team_id).await {
                    post_limit_comment(state, installation_id, owner, name, pr_number, &reason)
                        .await;
                    return Ok(StatusCode::OK);
                }
                // Re-review → fresh verdict; anything else → a question the reviewer
                // answers. Empty head_sha makes the worker resolve the PR's current
                // head, so a reply always targets the latest code.
                let is_rereview = ask.rereview;
                let question = ask.question;
                info!(
                    owner,
                    name, pr_number, is_rereview, "Reviewer: reply → review job"
                );
                // Idempotency key = this comment's id. A webhook GitHub delivers
                // more than once (at-least-once) carries the SAME comment id, so
                // the worker runs the review exactly once; a genuinely new
                // `@coderhelm re-review` comment has a new id and re-runs.
                let comment_id = payload["comment"]["id"].as_u64().unwrap_or(0);
                if is_rereview {
                    record_rereview(
                        state,
                        team_id,
                        owner,
                        name,
                        pr_number,
                        &format!("reply#{comment_id}"),
                    )
                    .await;
                }
                let message = WorkerMessage::Review(ReviewMessage {
                    team_id: team_id.to_string(),
                    installation_id,
                    repo_owner: owner.to_string(),
                    repo_name: name.to_string(),
                    pr_number,
                    head_sha: String::new(),
                    label: cfg.label,
                    question,
                    trigger: "reply".to_string(),
                    dedup_key: format!("reply#{comment_id}"),
                    reply_to_comment_id: None,
                    requested_by: commenter.to_string(),
                    ..Default::default()
                });
                return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            } else {
                info!(
                    owner,
                    name,
                    pr_number,
                    "Reviewer: @coderhelm reply ignored — review is off for this repo"
                );
            }
            return Ok(StatusCode::OK);
        }
    }

    // Trigger on `/coderhelm` slash command or @coderhelm mention (issues or non-bot PRs)
    let lower_body = body.to_ascii_lowercase();
    let is_slash = lower_body.trim_start().starts_with("/coderhelm");
    let is_mention = lower_body.contains("@coderhelm");
    if !is_slash && !is_mention {
        return Ok(StatusCode::OK);
    }

    let issue = &payload["issue"];
    let repo = &payload["repository"];

    let message = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Github,
        ticket_id: format!("GH-{}", issue["number"].as_u64().unwrap_or(0)),
        title: issue["title"].as_str().unwrap_or("").to_string(),
        body: issue["body"].as_str().unwrap_or("").to_string(),
        repo_owner: repo["owner"]["login"].as_str().unwrap_or("").to_string(),
        repo_name: repo["name"].as_str().unwrap_or("").to_string(),
        issue_number: issue["number"].as_u64().unwrap_or(0),
        sender: payload["sender"]["login"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        requester: common::requester::Requester::github(
            payload["sender"]["login"].as_str().unwrap_or(""),
        ),
        image_attachments: vec![],
        ..Default::default()
    });

    // Check usage limits before dispatching
    if let Some(reason) = check_run_budget(state, team_id).await {
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let number = issue["number"].as_u64().unwrap_or(0);
        post_limit_comment(state, installation_id, owner, name, number, &reason).await;
        return Ok(StatusCode::OK);
    }

    // Explicit human command: skip the content-hash check but still block
    // while a run is in flight — a repeated "/coderhelm" comment used to
    // enqueue a duplicate run with no dedup at all.
    let ticket_id_str = format!("GH-{}", issue["number"].as_u64().unwrap_or(0));
    if matches!(
        super::trigger_gate::gate_ticket_trigger(state, team_id, &ticket_id_str, None, true, false)
            .await,
        super::trigger_gate::TicketGate::SkipInFlight
    ) {
        info!(ticket_id = %ticket_id_str, "Skipping slash command — run already in flight");
        return Ok(StatusCode::OK);
    }

    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// Track PR merges for Coderhelm branches — updates run status to "merged".
async fn handle_pull_request(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    // Reviewer agent triggers (opt-in, off by default per repo):
    //  - human PRs: the review label added, or a new commit on a labeled PR.
    //  - CoderHelm's own PRs: auto-reviewed on open/reopen/push with no label
    //    needed (it can't self-approve, so review_pr posts a COMMENT review).
    // Native "Re-request review" button. GitHub fires `pull_request/review_requested`
    // with the reviewer being re-requested. Treat it like an explicit "@coderhelm
    // re-review": if CoderHelm is the requested reviewer and the repo is enabled,
    // enqueue a fresh review of the CURRENT head (empty head_sha → the worker
    // resolves it), bypassing the label/draft/dedup gates since it's a deliberate ask.
    if action == "review_requested" {
        let reviewer = payload["requested_reviewer"]["login"]
            .as_str()
            .unwrap_or("");
        if !reviewer.contains("coderhelm") {
            return Ok(StatusCode::OK);
        }
        let repo = &payload["repository"];
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);
        let cfg = load_review_config(state, team_id, owner, name).await?;
        if !cfg.enabled || cfg.killed || pr_number == 0 {
            return Ok(StatusCode::OK);
        }
        if let Some(reason) = check_run_budget(state, team_id).await {
            post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
            return Ok(StatusCode::OK);
        }
        info!(
            owner,
            name, pr_number, "Reviewer: native re-request review → review job"
        );
        // Dedup a redelivered `review_requested` by the head it targets (the
        // payload carries it even though the worker resolves head fresh).
        let req_head = payload["pull_request"]["head"]["sha"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("pr{pr_number}"));
        record_rereview(
            state,
            team_id,
            owner,
            name,
            pr_number,
            &format!("rerequest#{req_head}"),
        )
        .await;
        let message = WorkerMessage::Review(ReviewMessage {
            team_id: team_id.to_string(),
            installation_id,
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            pr_number,
            head_sha: String::new(),
            label: cfg.label,
            question: None,
            trigger: "rerequest".to_string(),
            dedup_key: format!("rerequest#{req_head}"),
            reply_to_comment_id: None,
            requested_by: String::new(),
            ..Default::default()
        });
        return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    }

    if action == "labeled"
        || action == "synchronize"
        || action == "opened"
        || action == "reopened"
        || action == "ready_for_review"
    {
        if action == "ready_for_review" {
            // A gate held only because the PR was a draft moves on now.
            let repo = &payload["repository"];
            let pr = &payload["pull_request"];
            rearm_gate(
                state,
                team_id,
                installation_id,
                repo["owner"]["login"].as_str().unwrap_or(""),
                repo["name"].as_str().unwrap_or(""),
                pr["number"].as_u64().unwrap_or(0),
                pr["head"]["sha"].as_str().unwrap_or(""),
                merge_gate::wakes_on_review,
                "marked ready for review",
            )
            .await?;
        }
        return handle_review_trigger(state, payload, installation_id, team_id, action).await;
    }

    let merged = payload["pull_request"]["merged"].as_bool().unwrap_or(false);
    if action != "closed" {
        return Ok(StatusCode::OK);
    }
    // A Tests setup pull request: record it and check the workflow again.
    super::tests_setup::on_pr_closed(state, payload, installation_id, team_id, merged).await;

    // A stopped auto-merge has no loop running to see the PR close: record it
    // here (any PR, not only CoderHelm's).
    {
        let repo = &payload["repository"];
        let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);
        if pr_number != 0 {
            if let Err(e) = merge_gate::finish_stopped(
                &state.dynamo,
                &state.config.settings_table_name,
                team_id,
                repo["owner"]["login"].as_str().unwrap_or(""),
                repo["name"].as_str().unwrap_or(""),
                pr_number,
                merged,
            )
            .await
            {
                warn!(pr_number, error = %e, "Could not record a stopped auto-merge's PR as closed");
            }
        }
    }

    // A merged revert of one of CoderHelm's pull requests (any author) is a
    // strong signal against that change.
    if merged {
        record_revert(state, payload, team_id).await;
    }

    // Only track our PRs
    let pr_user = payload["pull_request"]["user"]["login"]
        .as_str()
        .unwrap_or("");
    if !pr_user.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    let pr_number = payload["pull_request"]["number"].as_u64().unwrap_or(0);
    if pr_number == 0 {
        return Ok(StatusCode::OK);
    }

    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");

    // The run that opened this PR: its own PR, or one of a multi-repo run's.
    let found = lookup_run_item_by_pr(state, team_id, owner, name, pr_number).await;
    // A multi-repo run's other PR: its own outcome goes on its entry in
    // `prs`; the run's status and outcome follow the PR its record names.
    let is_primary = found
        .as_ref()
        .is_some_and(|i| common::run_prs::is_primary(i, &format!("{owner}/{name}"), pr_number));
    if let Some(item) = found.as_ref().filter(|_| !is_primary) {
        record_secondary_pr_outcome(state, team_id, item, owner, name, pr_number, merged).await;
    }
    if let Some(item) = found.as_ref() {
        let run_id = item
            .get("run_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        if !run_id.is_empty() && !merged {
            // Closed without merging: the run keeps its status; analytics
            // record the outcome so merge rate counts it honestly.
            let pr = &payload["pull_request"];
            if is_primary {
                let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(team_id))
                .key("run_id", attr_s(&run_id))
                .update_expression("SET pr_outcome = :o, pr_closed_at = :c, pr_opened_at = :op")
                .expression_attribute_values(":o", attr_s("closed"))
                .expression_attribute_values(":c", attr_s(pr["closed_at"].as_str().unwrap_or("")))
                .expression_attribute_values(":op", attr_s(pr["created_at"].as_str().unwrap_or("")))
                .send()
                .await
                .map_err(|e| error!(team_id, run_id, pr_number, error = %e, "Could not record the closed PR on the run"));
            }
            info!(
                team_id,
                run_id, pr_number, "PR closed without merging — outcome recorded"
            );
            // Learn from it: the worker reads the ticket, the plan's findings
            // and what people wrote, and records a lesson for the repository
            // when the fix belonged outside the code.
            let lesson = WorkerMessage::Lesson(crate::models::LessonMessage {
                team_id: team_id.to_string(),
                installation_id,
                run_id: run_id.clone(),
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                trigger: "pr_closed".to_string(),
                ..Default::default()
            });
            if let Err(e) = send_to_queue(state, &state.config.ticket_queue_url, &lesson).await {
                warn!(team_id, run_id, pr_number, status = ?e, "Could not queue the closed PR's lesson");
            }
            queue_reward_outcome(
                state,
                team_id,
                installation_id,
                &run_id,
                owner,
                name,
                pr_number,
            )
            .await;
            return Ok(StatusCode::OK);
        }
        if !run_id.is_empty() && !is_primary {
            // A multi-repo run's other PR merged: scored like any other; the
            // run's status follows the PR its record names.
            queue_reward_outcome(
                state,
                team_id,
                installation_id,
                &run_id,
                owner,
                name,
                pr_number,
            )
            .await;
        } else if !run_id.is_empty() {
            let now = chrono::Utc::now().to_rfc3339();
            let pr = &payload["pull_request"];
            // Commits people pushed on top of Coderhelm's: none means it
            // merged as Coderhelm wrote it.
            let human_commits =
                count_human_commits(state, installation_id, owner, name, pr_number).await;
            let run_status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                .unwrap_or("");
            let mut update = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(team_id))
                .key("run_id", attr_s(&run_id))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":o", attr_s("merged"))
                .expression_attribute_values(
                    ":ma",
                    attr_s(pr["merged_at"].as_str().unwrap_or(&now)),
                )
                .expression_attribute_values(":op", attr_s(pr["created_at"].as_str().unwrap_or("")))
                .expression_attribute_values(
                    ":mb",
                    attr_s(pr["merged_by"]["login"].as_str().unwrap_or("")),
                );
            let mut expr =
                "SET updated_at = :t, pr_outcome = :o, merged_at = :ma, pr_opened_at = :op, merged_by = :mb"
                    .to_string();
            // The merge commit, so an alert naming it can be tied to this run.
            if let Some(sha) = pr["merge_commit_sha"].as_str().filter(|s| !s.is_empty()) {
                expr.push_str(", merge_commit_sha = :mcs");
                update = update.expression_attribute_values(":mcs", attr_s(sha));
            }
            // A run a person closed stays closed; the merge is still recorded
            // (analytics count it as merged through `pr_outcome`).
            if merge_sets_status(run_status) {
                expr.push_str(", #status = :s, status_run_id = :sri");
                update = update
                    .expression_attribute_names("#status", "status")
                    .expression_attribute_values(":s", attr_s("merged"))
                    .expression_attribute_values(":sri", attr_s(&format!("merged#{run_id}")));
            }
            if let Some(n) = human_commits {
                expr.push_str(", human_commits = :hc");
                update = update.expression_attribute_values(":hc", attr_n(n));
            }
            if let Err(e) = update.update_expression(expr).send().await {
                error!(team_id, run_id, pr_number, error = %e, "Could not record the merge on the run");
            } else {
                info!(team_id, run_id, pr_number, "PR merged — run status updated");
            }
            queue_reward_outcome(
                state,
                team_id,
                installation_id,
                &run_id,
                owner,
                name,
                pr_number,
            )
            .await;

            // ── Plan task dependency continuation ──
            // If this run's issue belongs to a plan task, check for waiting dependents.
            if let Some(issue_num) = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
            {
                if let Err(e) = trigger_plan_dependents(state, team_id, issue_num).await {
                    warn!(
                        team_id,
                        issue_num,
                        error = %e,
                        "Failed to check plan task dependents"
                    );
                }
            }
        }
    }

    Ok(StatusCode::OK)
}

/// A multi-repo run's PR that isn't the one its record names closed: its
/// entry in `prs` gets the outcome, and it is scored and learned from like
/// any other (with its own repository and number).
#[allow(clippy::too_many_arguments)]
async fn record_secondary_pr_outcome(
    state: &AppState,
    team_id: &str,
    item: &common::paged::Item,
    owner: &str,
    name: &str,
    pr_number: u64,
    merged: bool,
) {
    let Some(run_id) = common::paged::run_id_of(item) else {
        return;
    };
    let prs = common::run_prs::from_item(item);
    if let Some(i) = common::run_prs::position(&prs, &format!("{owner}/{name}"), pr_number) {
        if item.contains_key(common::run_prs::FIELD) {
            let outcome = if merged { "merged" } else { "closed" };
            if let Err(e) = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key("team_id", attr_s(team_id))
                .key("run_id", attr_s(&run_id))
                .update_expression(format!("SET prs[{i}].outcome = :o, updated_at = :t"))
                .expression_attribute_values(":o", attr_s(outcome))
                .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                .send()
                .await
            {
                warn!(team_id, run_id, pr_number, error = %e, "Could not record the PR's outcome on its run");
            }
        }
    }
    info!(
        team_id,
        run_id, pr_number, merged, "A multi-repo run's other PR closed"
    );
}

/// Hands a closed CoderHelm pull request to the worker to score (merged as
/// written or edited, closed, time to merge, CI rounds, comments left open).
async fn queue_reward_outcome(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    run_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) {
    let msg = WorkerMessage::RewardOutcome(common::rewards::RewardOutcomeMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id: run_id.to_string(),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
    });
    if let Err(e) = send_to_queue(state, &state.config.ticket_queue_url, &msg).await {
        warn!(team_id, run_id, pr_number, status = ?e, "Could not queue the PR's reward outcome");
    }
}

fn reward_store(state: &AppState) -> common::rewards::store::Store {
    common::rewards::store::Store::new(&state.dynamo, &state.config.settings_table_name)
}

/// A re-review ask on a PR CoderHelm reviewed, as a reward signal (weight 0
/// by default: recorded and shown, scored only if the team chooses).
pub(crate) async fn record_rereview(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
    dedup: &str,
) {
    let ev = common::rewards::RewardEvent::new(
        team_id,
        common::rewards::RewardKind::RereviewRequested,
        common::rewards::event::AGENT_REVIEWER,
        &chrono::Utc::now().to_rfc3339(),
        &format!("rereview#{owner}/{name}#{dedup}"),
    )
    .with_repo(&format!("{owner}/{name}"), pr_number)
    .with_detail(&format!("Re-review asked on PR #{pr_number}"))
    .with_source("webhook:github");
    if let Err(e) = reward_store(state).record(&ev).await {
        warn!(error = %e, "Could not record the re-review ask");
    }
}

/// When a merged pull request is GitHub's revert of one of CoderHelm's,
/// records that change as reverted.
async fn record_revert(state: &AppState, payload: &Value, team_id: &str) {
    let pr = &payload["pull_request"];
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let full = format!("{owner}/{name}");
    let Some(target) = common::rewards::mapping::revert_target(
        pr["title"].as_str().unwrap_or(""),
        pr["body"].as_str().unwrap_or(""),
        &full,
    ) else {
        return;
    };
    let run_id = lookup_run_by_pr(state, team_id, owner, name, target).await;
    if run_id.is_empty() {
        return; // not one of CoderHelm's
    }
    let at = pr["merged_at"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let mut ev = common::rewards::RewardEvent::new(
        team_id,
        common::rewards::RewardKind::PrReverted,
        common::rewards::event::AGENT_CODER,
        &at,
        &format!("reverted#{full}#{target}"),
    )
    .with_repo(&full, target)
    .with_detail(&format!(
        "PR #{target} was reverted by #{}",
        pr["number"].as_u64().unwrap_or(0)
    ))
    .with_source("webhook:pull_request");
    ev.run_id = run_id;
    if let Err(e) = reward_store(state).record(&ev).await {
        warn!(error = %e, "Could not record the revert");
    } else {
        info!(team_id, repo = %full, pr = target, "A CoderHelm change was reverted");
    }
}

/// Counts people's commits on a merged pull request. `None` when GitHub
/// couldn't be asked, so analytics leave the run out rather than guess.
async fn count_human_commits(
    state: &AppState,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Option<u64> {
    let token = crate::auth::github_app::get_installation_token(state, installation_id)
        .await
        .ok()?;
    let url = format!(
        "https://api.github.com/repos/{owner}/{name}/pulls/{pr_number}/commits?per_page=100"
    );
    let commits: Vec<Value> = state
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    Some(human_commit_count(&commits))
}

async fn handle_pr_review(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let pr_head = pr["head"]["sha"].as_str().unwrap_or("");

    // A dismissal — of CoderHelm's approval or of a person's — withdraws a
    // key. Re-evaluate the gate at once, so it never merges on a withdrawn
    // key and its status comment says so (fail closed).
    if action == "dismissed" {
        rearm_gate(
            state,
            team_id,
            installation_id,
            owner,
            name,
            pr_number,
            pr_head,
            merge_gate::wakes_on_review,
            "review dismissed",
        )
        .await?;
        return Ok(StatusCode::OK);
    }
    if action != "submitted" {
        return Ok(StatusCode::OK);
    }

    let review_state = payload["review"]["state"].as_str().unwrap_or("");

    // Ignore reviews submitted by the bot itself — its APPROVE verdict already
    // armed the gate at review time (review_pr → run_on_approve).
    let reviewer = payload["review"]["user"]["login"].as_str().unwrap_or("");
    if reviewer.contains("coderhelm") {
        return Ok(StatusCode::OK);
    }

    // A person's request for changes (or a comment) can withdraw the person
    // key: wake a gate already armed on this head so it re-reads the reviews.
    // Waiting on people never polls; these events are what move it.
    if review_state != "approved" {
        rearm_gate(
            state,
            team_id,
            installation_id,
            owner,
            name,
            pr_number,
            pr_head,
            merge_gate::wakes_on_review,
            "review submitted",
        )
        .await?;
    }

    // A human APPROVED review is the second key for armed auto-merge. Re-arm the
    // gate so an approval that arrives AFTER the bot review still triggers the
    // merge — the review-time arming only had the bot's own key. This applies to
    // human PRs too, not just bot PRs. The worker's await_merge gate remains the
    // sole authority on whether auto_merge is on, both keys are present, and every
    // CI check is green before it merges (it waits for pending checks like a
    // staging deploy and never merges on failing CI). Here we only gate on the
    // repo having review enabled, so approvals on non-review repos are ignored.
    if review_state == "approved" {
        let cfg = load_review_config(state, team_id, owner, name).await?;
        let head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        let base_branch = pr["base"]["ref"].as_str().unwrap_or("").to_string();
        if cfg.enabled && !cfg.killed && pr_number != 0 && !head_sha.is_empty() {
            let self_authored = pr["user"]["login"]
                .as_str()
                .unwrap_or("")
                .contains("coderhelm");
            info!(
                owner,
                name, pr_number, "Human approval — arming auto-merge gate"
            );
            let message = WorkerMessage::AwaitMerge(AwaitMergeMessage {
                team_id: team_id.to_string(),
                installation_id,
                repo_owner: owner.to_string(),
                repo_name: name.to_string(),
                pr_number,
                head_sha,
                base_branch,
                self_authored,
                attempts: 0,
                chain_id: String::new(),
                error_retries: 0,
            });
            // A lost arming is a lost merge: a failed enqueue fails the
            // delivery so GitHub shows it and can redeliver.
            return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
        }
        return Ok(StatusCode::OK);
    }

    // Feedback loop: act on "changes_requested" or "commented" reviews (covers
    // formal change-request reviews and standalone single-line comments / thread
    // replies). This path is bot-PR only.
    if review_state != "changes_requested" && review_state != "commented" {
        return Ok(StatusCode::OK);
    }
    let pr_user = pr["user"]["login"].as_str().unwrap_or("");
    if !pr_user.contains("coderhelm") {
        // A review body that addresses the reviewer on a person's PR is the
        // same ask as a comment: one command surface for every comment type.
        let body = payload["review"]["body"].as_str().unwrap_or("");
        let review_id = payload["review"]["id"].as_u64().unwrap_or(0);
        if let Some(ask) = parse_reviewer_ask(body) {
            if !may_start_work(state, team_id, &payload["review"], "review").await {
                return Ok(StatusCode::OK);
            }
            let cfg = load_review_config(state, team_id, owner, name).await?;
            if cfg.enabled && !cfg.killed && pr_number != 0 && review_id != 0 {
                if let Some(reason) = check_run_budget(state, team_id).await {
                    post_limit_comment(state, installation_id, owner, name, pr_number, &reason)
                        .await;
                    return Ok(StatusCode::OK);
                }
                info!(
                    owner,
                    name,
                    pr_number,
                    is_rereview = ask.rereview,
                    "Reviewer: review body → review job"
                );
                if ask.rereview {
                    record_rereview(
                        state,
                        team_id,
                        owner,
                        name,
                        pr_number,
                        &format!("review#{review_id}"),
                    )
                    .await;
                }
                let message = WorkerMessage::Review(ReviewMessage {
                    team_id: team_id.to_string(),
                    installation_id,
                    repo_owner: owner.to_string(),
                    repo_name: name.to_string(),
                    pr_number,
                    head_sha: String::new(),
                    label: cfg.label,
                    question: ask.question,
                    trigger: "reply".to_string(),
                    dedup_key: format!("review#{review_id}"),
                    reply_to_comment_id: None,
                    requested_by: reviewer.to_string(),
                    ..Default::default()
                });
                return send_to_queue(state, &state.config.ticket_queue_url, &message).await;
            }
        }
        return Ok(StatusCode::OK);
    }

    // A review on CoderHelm's own PR starts a fix pass.
    if !may_start_work(state, team_id, &payload["review"], "review").await {
        return Ok(StatusCode::OK);
    }
    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(team_id, "PR review skipped — token limit reached");
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
    if run_id.is_empty() {
        warn!(pr_number, "No run found for PR — skipping feedback");
        return Ok(StatusCode::OK);
    }

    // Write event to events table for the resume flow
    let review_body = payload["review"]["body"].as_str().unwrap_or("").to_string();
    let review_id = payload["review"]["id"].as_u64().unwrap_or(0);
    write_run_event(
        state,
        &run_id,
        "pr_review",
        &serde_json::json!({
            "review_state": review_state,
            "reviewer": reviewer,
            "review_id": review_id,
            "review_body": review_body,
            "pr_number": pr_number,
        }),
    )
    .await;

    // Also send to feedback queue for immediate processing
    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        review_id,
        // A review with a written body is addressed to CoderHelm on its own
        // PR: the fix loop answers it on the PR when there is no inline
        // thread to answer in.
        trigger_author: if review_body.trim().is_empty() {
            String::new()
        } else {
            reviewer.to_string()
        },
        review_body,
        comments: vec![],
        ..Default::default()
    });

    send_to_queue(state, &state.config.feedback_queue_url, &message).await
}

/// Re-arm an existing merge gate for this PR from its stored record, when the
/// record is bound to `head_sha` and its state passes `wake`. No record,
/// another head, or a state `wake` rejects: nothing to do. A failed read or
/// enqueue is returned as 500 so GitHub shows the failed delivery and can
/// redeliver it.
#[allow(clippy::too_many_arguments)]
async fn rearm_gate(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
    head_sha: &str,
    wake: fn(&str) -> bool,
    reason: &str,
) -> Result<(), StatusCode> {
    if pr_number == 0 || head_sha.is_empty() {
        return Ok(());
    }
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&merge_gate::record_sk(owner, name, pr_number)))
        .send()
        .await
        .map_err(|e| {
            error!(owner, name, pr_number, error = %e, "Could not read merge gate record");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(());
    };
    let get_s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let gate_state = get_s("state");
    if get_s("head_sha") != head_sha || !wake(&gate_state) {
        return Ok(());
    }
    info!(owner, name, pr_number, gate_state = %gate_state, reason, "Re-arming merge gate");
    let message = WorkerMessage::AwaitMerge(AwaitMergeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha: head_sha.to_string(),
        base_branch: get_s("base_branch"),
        self_authored: item
            .get("self_authored")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        attempts: 0,
        chain_id: String::new(),
        error_retries: 0,
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message)
        .await
        .map(|_| ())
}

async fn handle_check_run(
    _state: &AppState,
    payload: &Value,
    _installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    let conclusion = payload["check_run"]["conclusion"].as_str().unwrap_or("");
    if conclusion != "failure" {
        return Ok(StatusCode::OK);
    }

    let branch = payload["check_run"]["check_suite"]["head_branch"]
        .as_str()
        .unwrap_or("");

    // Individual check_run failures are logged but NOT acted on directly.
    // The workflow_run completion event handles the aggregate CI result, and
    // decides there whether the branch is CoderHelm's (a team's branch
    // pattern can be anything, so no prefix check here).
    let check_name = payload["check_run"]["name"].as_str().unwrap_or("unknown");
    debug!(
        team_id,
        branch, check_name, "check_run failed — will be handled by workflow_run completion"
    );

    Ok(StatusCode::OK)
}

async fn handle_installation(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    match action {
        "created" => {
            let org = payload["installation"]["account"]["login"]
                .as_str()
                .unwrap_or("unknown");
            info!(installation_id, org, "GitHub App installed");

            // Only a team that already holds this installation (a reinstall)
            // is synced here. A new installation is never linked from a
            // webhook: who sent it, or which team once added a repo from the
            // same org, says nothing about which team may have it. It is
            // linked when an admin signs in with GitHub and GitHub confirms
            // they can access it (auth::github_callback).
            let team_id = resolve_team_by_installation(state, installation_id).await;
            if team_id.is_none() {
                info!(
                    installation_id,
                    org, "New installation not linked — waiting for an admin's GitHub sign-in"
                );
            }

            // Sync repos if we have a linked team
            if let Some(team_id) = team_id {
                let mut repos = extract_repos_from_installation(payload);
                if repos.is_empty() {
                    repos = fetch_installation_repos(state, installation_id).await;
                }

                let now = chrono::Utc::now().to_rfc3339();
                for repo in &repos {
                    let full = format!("{}/{}", repo.owner, repo.name);
                    let _ = state
                        .dynamo
                        .put_item()
                        .table_name(&state.config.repos_table_name)
                        .item("pk", attr_s(&team_id))
                        .item("sk", attr_s(&format!("REPO#{full}")))
                        .item("repo_name", attr_s(&full))
                        .item(
                            "enabled",
                            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
                        )
                        .item("ticket_source", attr_s("github"))
                        .item("created_at", attr_s(&now))
                        .send()
                        .await;
                }

                if !repos.is_empty() {
                    let onboard = WorkerMessage::Onboard(OnboardMessage {
                        team_id,
                        installation_id,
                        repos,
                    });
                    let _ = send_to_queue(state, &state.config.ticket_queue_url, &onboard).await;
                }
            }

            Ok(StatusCode::CREATED)
        }
        "deleted" => {
            info!(
                installation_id,
                "GitHub App uninstalled — removing installation link"
            );

            // Find ALL teams linked to this installation and remove the link
            let team_ids = resolve_all_teams_by_installation(state, installation_id).await;
            let now = chrono::Utc::now().to_rfc3339();
            for team_id in &team_ids {
                // Remove from teams table
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.teams_table_name)
                    .key("team_id", attr_s(team_id))
                    .key("sk", attr_s("META"))
                    .update_expression(
                        "REMOVE github_installation_id, github_org SET updated_at = :t",
                    )
                    .expression_attribute_values(":t", attr_s(&now))
                    .send()
                    .await;

                // Remove from main table
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(team_id))
                    .key("sk", attr_s("META"))
                    .update_expression("REMOVE github_install_id, github_org SET updated_at = :t")
                    .expression_attribute_values(":t", attr_s(&now))
                    .send()
                    .await;

                info!(
                    team_id,
                    "GitHub installation unlinked from team (both tables)"
                );
            }
            if team_ids.is_empty() {
                info!(
                    installation_id,
                    "No teams found for uninstalled installation"
                );
            }

            Ok(StatusCode::OK)
        }
        _ => Ok(StatusCode::OK),
    }
}

async fn handle_installation_repos(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");

    let team_id = match resolve_team_by_installation(state, installation_id).await {
        Some(tid) => tid,
        None => {
            warn!(
                installation_id,
                "installation_repositories event but no team linked"
            );
            return Ok(StatusCode::OK);
        }
    };

    if action == "removed" {
        let removed: Vec<String> = payload["repositories_removed"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|r| r["full_name"].as_str().map(|s| s.to_string()))
            .collect();

        for full_name in &removed {
            let _ = state
                .dynamo
                .delete_item()
                .table_name(&state.config.repos_table_name)
                .key("pk", attr_s(&team_id))
                .key("sk", attr_s(&format!("REPO#{full_name}")))
                .send()
                .await;
            info!(repo = %full_name, "Removed repo (access revoked)");
        }

        return Ok(StatusCode::OK);
    }

    if action != "added" {
        return Ok(StatusCode::OK);
    }

    let repos: Vec<OnboardRepo> = payload["repositories_added"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|r| {
            let full_name = r["full_name"].as_str()?;
            let parts: Vec<&str> = full_name.splitn(2, '/').collect();
            if parts.len() != 2 {
                return None;
            }
            Some(OnboardRepo {
                owner: parts[0].to_string(),
                name: parts[1].to_string(),
                default_branch: r["default_branch"].as_str().unwrap_or("main").to_string(),
            })
        })
        .collect();

    if repos.is_empty() {
        return Ok(StatusCode::OK);
    }

    // Write REPO# items so the dashboard can list them
    let now = chrono::Utc::now().to_rfc3339();
    for repo in &repos {
        let full = format!("{}/{}", repo.owner, repo.name);
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.repos_table_name)
            .item("pk", attr_s(&team_id))
            .item("sk", attr_s(&format!("REPO#{full}")))
            .item("repo_name", attr_s(&full))
            .item(
                "enabled",
                aws_sdk_dynamodb::types::AttributeValue::Bool(false),
            )
            .item("ticket_source", attr_s("github"))
            .item("created_at", attr_s(&now))
            .send()
            .await;
    }

    let onboard = WorkerMessage::Onboard(OnboardMessage {
        team_id,
        installation_id,
        repos,
    });

    send_to_queue(state, &state.config.ticket_queue_url, &onboard).await
}

/// Handle check_suite events — delegated to workflow_run handler.
async fn handle_check_suite(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }
    let suite = &payload["check_suite"];
    let branch = suite["head_branch"].as_str().unwrap_or("");
    let conclusion = suite["conclusion"].as_str().unwrap_or("");
    let head_sha = suite["head_sha"].as_str().unwrap_or("");

    // CI finished on a PR's head: wake that PR's merge gate if it is waiting
    // on (or was stopped by) CI, on any branch. The gate re-reads every check
    // itself, so a flaky job re-run to green, or a deploy slower than the
    // gate's poll window, still merges.
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    for pr in suite["pull_requests"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        rearm_gate(
            state,
            team_id,
            installation_id,
            owner,
            name,
            pr["number"].as_u64().unwrap_or(0),
            head_sha,
            merge_gate::wakes_on_ci,
            "check suite completed",
        )
        .await?;
    }

    // CI result handling for CoderHelm's own runs is done by the
    // workflow_run handler; this is for observability.
    debug!(
        branch,
        conclusion, "check_suite completed — workflow_run handler will process"
    );
    Ok(StatusCode::OK)
}

/// A workflow conclusion that means a check failed (as opposed to skipped,
/// cancelled or neutral, which are ignored before this).
fn is_failed_conclusion(conclusion: &str) -> bool {
    matches!(conclusion, "failure" | "timed_out" | "startup_failure")
}

/// Only a run that finished as completed is reopened: a cancelled or failed
/// run stays as a person or the run left it.
fn reopens_for_ci(run_status: &str) -> bool {
    run_status == "completed"
}

/// Send a CI failure on CoderHelm's own PR to the run that opened it, when
/// that run already finished. Best effort: no run, a run that didn't complete,
/// or a spent token budget is a logged no-op.
async fn reopen_finished_run_for_ci(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
    workflow_name: &str,
) {
    let run_id = lookup_run_by_pr(state, team_id, owner, name, pr_number).await;
    if run_id.is_empty() {
        info!(
            pr_number,
            workflow_name, "CI failed on own PR but no run found — not fixing"
        );
        return;
    }
    let status = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get("status").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();
    if !reopens_for_ci(&status) {
        info!(
            pr_number,
            run_id, status, workflow_name, "CI failed on own PR; run not completed — not reopening"
        );
        return;
    }
    if check_run_budget(state, team_id).await.is_some() {
        info!(
            pr_number,
            run_id, "CI failed on own PR; token limit reached — not fixing"
        );
        return;
    }
    info!(
        pr_number,
        run_id,
        workflow_name,
        "CI failed on own PR after its run finished — sending to the fix loop"
    );
    let message = WorkerMessage::Feedback(FeedbackMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        // 0: feedback reads the PR's failing checks itself.
        review_id: 0,
        review_body: String::new(),
        comments: vec![],
        trigger_author: String::new(),
        ..Default::default()
    });
    let _ = send_to_queue(state, &state.config.feedback_queue_url, &message).await;
}

/// Handle workflow_run events — write CI events and trigger resume for coderhelm branches.
async fn handle_workflow_run(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    if action != "completed" {
        return Ok(StatusCode::OK);
    }

    // E2E results are read for every branch when the team turned them on.
    super::tests::enqueue_ingest(state, payload, installation_id, team_id).await;

    let wf = &payload["workflow_run"];
    let branch = wf["head_branch"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let repo_owner = repo["owner"]["login"].as_str().unwrap_or("");
    let repo_name = repo["name"].as_str().unwrap_or("");
    if !is_own_branch(state, team_id, repo_owner, repo_name, branch).await {
        return Ok(StatusCode::OK);
    }

    let conclusion = wf["conclusion"].as_str().unwrap_or("");
    let workflow_name = wf["name"].as_str().unwrap_or("unknown");

    // Skip "skipped" workflows — they're not failures, just conditional workflows
    // that didn't apply (e.g., deploy only runs on main). Writing them as ci_failed
    // pollutes the events and confuses the CI fix loop.
    if conclusion == "skipped"
        || conclusion == "cancelled"
        || conclusion == "neutral"
        || conclusion == "stale"
    {
        info!(
            branch,
            conclusion, workflow_name, "Ignoring non-actionable workflow conclusion"
        );
        return Ok(StatusCode::OK);
    }

    // Find the run_id for this branch
    let run_id = match find_run_by_branch(state, team_id, repo_owner, repo_name, branch).await {
        Some(id) => id,
        None => {
            // No run is waiting on CI. A failure on CoderHelm's own PR after its
            // run finished (a check that started after the run completed, like
            // E2E a label kicked off) goes to the run's fix loop: feedback reads
            // the failing checks and fixes them, within the run's fix budget.
            if is_failed_conclusion(conclusion) {
                for pr in wf["pull_requests"]
                    .as_array()
                    .map(|a| a.as_slice())
                    .unwrap_or(&[])
                {
                    let pr_number = pr["number"].as_u64().unwrap_or(0);
                    if pr_number == 0 {
                        continue;
                    }
                    reopen_finished_run_for_ci(
                        state,
                        team_id,
                        installation_id,
                        repo_owner,
                        repo_name,
                        pr_number,
                        workflow_name,
                    )
                    .await;
                }
                return Ok(StatusCode::OK);
            }
            // Fallback: still handle success via MarkReady for backward compat
            if conclusion == "success" {
                let prs = wf["pull_requests"].as_array().cloned().unwrap_or_default();
                for pr in &prs {
                    let pr_number = pr["number"].as_u64().unwrap_or(0);
                    if pr_number == 0 {
                        continue;
                    }
                    info!(
                        branch,
                        pr_number, "CI passed (no run found) — marking PR ready directly"
                    );
                    let message = WorkerMessage::MarkReady(MarkReadyMessage {
                        team_id: team_id.to_string(),
                        installation_id,
                        repo_owner: repo_owner.to_string(),
                        repo_name: repo_name.to_string(),
                        pr_number,
                    });
                    let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
                }
            }
            return Ok(StatusCode::OK);
        }
    };

    let now = chrono::Utc::now();
    let event_type = if conclusion == "success" {
        "ci_passed"
    } else {
        "ci_failed"
    };

    // Write event to events table
    let event_sk = format!("EVENT#{}#{}", now.format("%Y%m%dT%H%M%S%.3fZ"), event_type,);

    let mut logs_url = String::new();
    if let Some(url) = wf.get("logs_url").and_then(|v| v.as_str()) {
        logs_url = url.to_string();
    }

    let event_payload = serde_json::json!({
        "conclusion": conclusion,
        "workflow_name": workflow_name,
        "branch": branch,
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "logs_url": logs_url,
        "workflow_run_id": wf["id"].as_u64().unwrap_or(0),
        // The commit this result is for. The CI fixer ignores a failure whose
        // head_sha is no longer the branch head (stale: a newer push exists).
        "head_sha": wf["head_sha"].as_str().unwrap_or(""),
    });

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("RUN#{run_id}")))
        .item("sk", attr_s(&event_sk))
        .item("event_type", attr_s(event_type))
        .item("payload", attr_s(&event_payload.to_string()))
        .item(
            "processed",
            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
        )
        .item("created_at", attr_s(&now.to_rfc3339()))
        .item("expires_at", attr_n(now.timestamp() as u64 + 30 * 86400))
        .send()
        .await
    {
        error!("Failed to write CI event: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    info!(
        run_id,
        branch, event_type, workflow_name, "CI event recorded — sending Resume"
    );

    // Send Resume message with a delay — CI repos often have multiple workflows
    // (lint, test, build, deploy) that finish within seconds of each other.
    // The 60s delay lets them all land as events before the worker picks up.
    let message = WorkerMessage::Resume(ResumeMessage {
        team_id: team_id.to_string(),
        run_id: run_id.clone(),
        installation_id,
        ..Default::default()
    });
    let body = serde_json::to_string(&message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The self-healing CI loop hinges on THIS one message — a discarded send
    // strands the run in awaiting_ci with no re-drive. Surface the failure so
    // GitHub retries the webhook (and the awaiting_ci reaper is the backstop).
    if let Err(e) = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .delay_seconds(60)
        .send()
        .await
    {
        error!(run_id, error = %e, "Failed to enqueue CI resume — returning 500 so GitHub retries");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    Ok(StatusCode::OK)
}

/// Look up a run by branch name. Queries repo-index GSI and filters for matching branch
/// with status "awaiting_ci" or "running".
/// Whether `branch` is one CoderHelm made for a run. The default pattern's
/// prefix is the fast path (every branch before teams could choose one); for
/// a team with its own branch pattern, the run records are the marker: a
/// branch is CoderHelm's when a run of this team was on it. A person's branch
/// that happens to match the team's pattern is never taken for one.
async fn is_own_branch(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    branch: &str,
) -> bool {
    use common::git_conventions as gc;
    if branch.is_empty() {
        return false;
    }
    if branch.starts_with(gc::DEFAULT_BRANCH_PREFIX) {
        return true;
    }
    let custom = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(gc::SETTINGS_SK))
        .send()
        .await
        .ok()
        .and_then(|o| o.item.map(|i| gc::GitConventions::from_item(&i)))
        .is_some_and(|c| c.branch.is_some());
    custom && run_on_branch_exists(state, team_id, owner, name, branch).await
}

/// Whether any run of this team (any status) worked on `branch` in
/// `owner/name`: its primary branch, or one of a multi-repo run's branches.
async fn run_on_branch_exists(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    branch: &str,
) -> bool {
    let team_repo = format!("{team_id}#{owner}/{name}");
    let primary = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("branch = :b")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":b", attr_s(branch))
        .scan_index_forward(false)
        .limit(200)
        .send()
        .await
        .map(|r| !r.items().is_empty())
        .unwrap_or(false);
    if primary {
        return true;
    }
    state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression("contains(branches, :b)")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":b", attr_s(branch))
        .scan_index_forward(false)
        .limit(200)
        .send()
        .await
        .map(|r| !r.items().is_empty())
        .unwrap_or(false)
}

async fn find_run_by_branch(
    state: &AppState,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
    branch: &str,
) -> Option<String> {
    // Fast path: query repo-index GSI (works for primary repo)
    let team_repo = format!("{team_id}#{repo_owner}/{repo_name}");
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("branch = :b AND (#s = :s1 OR #s = :s2)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":s1", attr_s("awaiting_ci"))
        .expression_attribute_values(":s2", attr_s("running"))
        .scan_index_forward(false)
        .limit(50)
        .send()
        .await
        .ok()?;

    if let Some(run_id) = result
        .items()
        .first()
        .and_then(|item| item.get("run_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
    {
        return Some(run_id);
    }

    // Fallback: a multi-repo run's other branches, by repository and branch
    // (`branch_keys`); records from before them by branch name alone.
    let fallback = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression(
            "(contains(branch_keys, :bk) OR (attribute_not_exists(branch_keys) AND contains(branches, :b))) \
             AND (#s = :s1 OR #s = :s2)",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(
            ":bk",
            attr_s(&common::run_prs::branch_key(
                &format!("{repo_owner}/{repo_name}"),
                branch,
            )),
        )
        .expression_attribute_values(":b", attr_s(branch))
        .expression_attribute_values(":s1", attr_s("awaiting_ci"))
        .expression_attribute_values(":s2", attr_s("running"))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await
        .ok()?;

    fallback
        .items()
        .first()
        .and_then(|item| item.get("run_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
}

/// Handle repository events — track renames, deletions, visibility changes.
async fn handle_repository_event(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let action = payload["action"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let repo_name = repo["full_name"].as_str().unwrap_or("unknown");

    match action {
        "deleted" | "archived" => {
            info!(
                action,
                repo_name, installation_id, "Repository removed — deactivating repo record"
            );
            let parts: Vec<&str> = repo_name.splitn(2, '/').collect();
            if parts.len() == 2 {
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.repos_table_name)
                    .key("pk", attr_s(team_id))
                    .key("sk", attr_s(&format!("REPO#{}", repo_name)))
                    .update_expression("SET #status = :s, updated_at = :t")
                    .expression_attribute_names("#status", "status")
                    .expression_attribute_values(":s", attr_s("inactive"))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
            }
            Ok(StatusCode::OK)
        }
        "renamed" => {
            info!(
                action,
                repo_name, installation_id, "Repository renamed — logged"
            );
            Ok(StatusCode::OK)
        }
        "unarchived" => {
            info!(
                action,
                repo_name, installation_id, "Repository unarchived — logged"
            );
            Ok(StatusCode::OK)
        }
        _ => {
            info!(action, repo_name, "Repository event — no action needed");
            Ok(StatusCode::OK)
        }
    }
}

/// Fetch repos for an installation via GitHub API (used when "All repositories" is selected
/// and the webhook payload doesn't include the repo list).
pub async fn fetch_installation_repos(state: &AppState, installation_id: u64) -> Vec<OnboardRepo> {
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to get installation token for repo fetch: {e}");
            return vec![];
        }
    };

    let mut repos = Vec::new();
    let mut page = 1u32;
    loop {
        let url =
            format!("https://api.github.com/installation/repositories?per_page=100&page={page}");
        let resp = state
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .send()
            .await;

        let body: Value = match resp {
            Ok(r) => match r.error_for_status() {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to parse installation repos response: {e}");
                        break;
                    }
                },
                Err(e) => {
                    error!("GitHub API error fetching installation repos: {e}");
                    break;
                }
            },
            Err(e) => {
                error!("HTTP error fetching installation repos: {e}");
                break;
            }
        };

        let page_repos = body["repositories"].as_array();
        if let Some(arr) = page_repos {
            for r in arr {
                if let Some(full_name) = r["full_name"].as_str() {
                    let parts: Vec<&str> = full_name.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        repos.push(OnboardRepo {
                            owner: parts[0].to_string(),
                            name: parts[1].to_string(),
                            default_branch: r["default_branch"]
                                .as_str()
                                .unwrap_or("main")
                                .to_string(),
                        });
                    }
                }
            }
            // Stop when we got fewer than a full page
            if arr.len() < 100 {
                break;
            }
        } else {
            break;
        }
        page += 1;
    }

    info!(count = repos.len(), "Fetched repos from GitHub API");
    repos
}

fn extract_repos_from_installation(payload: &Value) -> Vec<OnboardRepo> {
    payload["repositories"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|r| {
            let full_name = r["full_name"].as_str()?;
            let parts: Vec<&str> = full_name.splitn(2, '/').collect();
            if parts.len() != 2 {
                return None;
            }
            Some(OnboardRepo {
                owner: parts[0].to_string(),
                name: parts[1].to_string(),
                default_branch: r["default_branch"].as_str().unwrap_or("main").to_string(),
            })
        })
        .collect()
}

/// Per-repo reviewer-agent config. OFF by default — the bulletproof default so
/// the reviewer never touches a repo until a human explicitly opts it in.
pub(crate) struct ReviewConfig {
    pub(crate) enabled: bool,
    /// PR label that triggers a review.
    pub(crate) label: String,
    /// Hard kill switch — blocks ALL reviewer action regardless of `enabled`.
    pub(crate) killed: bool,
    /// Merge on its own once the gate's conditions hold (`await_merge`).
    pub(crate) auto_merge: bool,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            label: "ch-review".to_string(),
            killed: false,
            auto_merge: false,
        }
    }
}

/// The repo's reviewer config. A failed read is an error (the webhook
/// fails and GitHub shows it for redelivery), never "review off": that used
/// to drop reviews silently on a DynamoDB blip.
pub(crate) async fn load_review_config(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<ReviewConfig, StatusCode> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!(owner, name, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Reviewer config read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(ReviewConfig::default());
    };
    let d = ReviewConfig::default();
    Ok(ReviewConfig {
        enabled: item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(d.enabled),
        label: item
            .get("label")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or(d.label),
        killed: item
            .get("killed")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(d.killed),
        auto_merge: item
            .get("auto_merge")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(d.auto_merge),
    })
}

/// Push to a branch: if the repo's code graph is enabled and the push is to the
/// DEFAULT branch, enqueue an INCREMENTAL graph index for exactly the files the
/// push touched (added/modified/removed across its commits). This is what keeps
/// the graph permanently fresh without scheduled rebuilds.
/// A tag pushed by a person or by CI (`refs/tags/…` push, `created`). When the
/// repo has release notes on, queue the same release-notes job CoderHelm runs
/// after cutting its own tag. Tags CoderHelm cuts itself are skipped here (the
/// tag cut already queued them; the worker's per-tag record would no-op anyway).
/// The worker checks the tag is on the repo's release branch before writing.
#[allow(clippy::too_many_arguments)]
async fn handle_tag_push(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    owner: &str,
    name: &str,
    tag: &str,
) -> Result<StatusCode, StatusCode> {
    let created = payload["created"].as_bool().unwrap_or(false);
    let deleted = payload["deleted"].as_bool().unwrap_or(false);
    if !created || deleted || tag.is_empty() {
        return Ok(StatusCode::OK);
    }
    let sender = payload["sender"]["login"].as_str().unwrap_or("");
    let pusher = payload["pusher"]["name"].as_str().unwrap_or("");
    if is_coderhelm_actor(sender) || is_coderhelm_actor(pusher) {
        return Ok(StatusCode::OK);
    }
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let enabled = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .map(|it| {
            let b = |k: &str| {
                it.get(k)
                    .and_then(|v| v.as_bool().ok())
                    .copied()
                    .unwrap_or(false)
            };
            b("release_notes") && !b("killed")
        })
        .unwrap_or(false);
    if !enabled {
        return Ok(StatusCode::OK);
    }
    info!(
        owner,
        name, tag, sender, "Release notes: tag pushed outside CoderHelm → release notes job"
    );
    let message = WorkerMessage::ReleaseNotes(ReleaseNotesMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        tag: tag.to_string(),
        // The tag resolves as a ref everywhere the worker needs a commit.
        sha: tag.to_string(),
        resend: String::new(),
        check_branch: true,
        ..Default::default()
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// The CoderHelm app's own GitHub identity (`coderhelm[bot]`, any case).
fn is_coderhelm_actor(login: &str) -> bool {
    login.to_ascii_lowercase().starts_with("coderhelm")
}

/// Tool definition files a push added or changed, at most 50.
fn tool_definition_paths(payload: &Value) -> Vec<String> {
    let mut paths: Vec<String> = payload["commits"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|c| {
            ["added", "modified"]
                .into_iter()
                .flat_map(move |k| c[k].as_array().cloned().unwrap_or_default())
        })
        .filter_map(|p| p.as_str().map(String::from))
        .filter(|p| {
            p.starts_with(".coderhelm/tools/")
                && (p.ends_with(".yaml") || p.ends_with(".yml") || p.ends_with(".json"))
                && !p.contains("..")
        })
        .collect();
    paths.sort();
    paths.dedup();
    paths.truncate(50);
    paths
}

async fn handle_push(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"]
        .as_str()
        .or_else(|| repo["owner"]["name"].as_str())
        .unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");
    let default_branch = repo["default_branch"].as_str().unwrap_or("main");
    let git_ref = payload["ref"].as_str().unwrap_or("");
    if owner.is_empty() || name.is_empty() {
        return Ok(StatusCode::OK);
    }
    // Plugin sources on the team's installation: any push (a branch or a
    // tag a plugin may track) syncs them; wording applies at once, widened
    // versions wait for an admin. The worker reads only what moved.
    super::plugin_pkgs::on_push(state, team_id, installation_id, &format!("{owner}/{name}")).await;
    // A new tag → release notes (for tags CoderHelm didn't cut itself).
    if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
        return handle_tag_push(state, payload, installation_id, team_id, owner, name, tag).await;
    }
    // Only the branch the graph tracks.
    if git_ref != format!("refs/heads/{default_branch}") {
        return Ok(StatusCode::OK);
    }
    // Knowledge sources reading this repository's docs sync now.
    super::knowledge_sources::on_push(state, team_id, &format!("{owner}/{name}")).await;
    // Team tool definitions in `.coderhelm/tools/` become proposals waiting
    // for an admin's approval. A merge never turns a tool on by itself.
    let tool_paths = tool_definition_paths(payload);
    if !tool_paths.is_empty() {
        let sha = payload["after"].as_str().unwrap_or("").to_string();
        let message = WorkerMessage::ToolProposals(crate::models::ToolProposalsMessage {
            team_id: team_id.to_string(),
            installation_id,
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            sha,
            paths: tool_paths,
        });
        let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    }
    // A connected skill repository: its default branch moved, so the team's
    // skills are read again (the merged code only).
    let full = format!("{owner}/{name}");
    let skill_repo = common::skills::SkillStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: team_id,
    }
    .repo(&full)
    .await;
    if let Ok(Some(_)) = skill_repo {
        let message = WorkerMessage::SkillSync(common::skills::SkillSyncMessage {
            team_id: team_id.to_string(),
            repo: full,
            ..Default::default()
        });
        let _ = send_to_queue(state, &state.config.ticket_queue_url, &message).await;
    }
    // The code graph is on for every repo unless turned off for it. A push
    // updates a graph that exists; a repo without one gets its first build
    // when work starts on it (or when it is turned on), never from a push to
    // a repo nobody uses.
    let full_repo = format!("{owner}/{name}");
    let table = &state.config.settings_table_name;
    if !common::code_graph::is_enabled(&state.dynamo, table, team_id, &full_repo).await
        || !common::code_graph::has_graph(&state.dynamo, table, team_id, &full_repo).await
    {
        return Ok(StatusCode::OK);
    }
    // Every file any commit in this push touched (deduped, bounded — a huge
    // push falls back to a FULL index rather than a 1000-file incremental).
    let mut files: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for c in payload["commits"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        for key in ["added", "modified", "removed"] {
            for f in c[key].as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
                if let Some(p) = f.as_str() {
                    if seen.insert(p.to_string()) {
                        files.push(p.to_string());
                    }
                }
            }
        }
    }
    if files.is_empty() {
        return Ok(StatusCode::OK);
    }
    let changed_files = if files.len() > 300 { None } else { Some(files) };
    info!(
        owner,
        name,
        full = changed_files.is_none(),
        "Push on default branch → code-graph index"
    );
    let message = WorkerMessage::GraphIndex(GraphIndexMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        branch: default_branch.to_string(),
        changed_files,
    });
    send_to_queue(state, &state.config.ticket_queue_url, &message).await
}

/// Reviewer trigger: enqueue a code-review job when the repo's review label is
/// added to an open PR, or a new commit lands on an already-labeled PR.
/// Idempotency (one review per head SHA) is enforced worker-side by the review
/// run claim, so a re-label or duplicate webhook is harmless.
async fn handle_review_trigger(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
    action: &str,
) -> Result<StatusCode, StatusCode> {
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let head_sha = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    if pr_number == 0 || head_sha.is_empty() || pr["state"].as_str() != Some("open") {
        return Ok(StatusCode::OK);
    }
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");

    let cfg = load_review_config(state, team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed {
        return Ok(StatusCode::OK);
    }

    // Review is gated on the repo's trigger label for EVERY PR, including
    // CoderHelm's own (bot-authored) PRs — nothing is reviewed until the label is
    // present. `labeled`: the just-added label must BE the trigger label.
    // `opened`/`reopened`/`ready_for_review`/`synchronize`: the PR must already
    // carry the label (covers a PR opened with it, and re-review on a new commit).
    // Draft PRs are NEVER reviewed here (see the draft gate below): CoderHelm opens
    // its own PRs as draft and self-labels them at once, and a human may label a
    // draft to queue it. Either way the review waits for the PR to actually be
    // ready — GitHub re-fires this path as `ready_for_review` (draft=false) once the
    // author clicks "Ready for review" or the pipeline marks it ready.
    let is_bot_pr = pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm");
    let carries_label = pr["labels"]
        .as_array()
        .map(|ls| {
            ls.iter()
                .any(|l| l["name"].as_str() == Some(cfg.label.as_str()))
        })
        .unwrap_or(false);
    let triggered = match action {
        "labeled" => payload["label"]["name"].as_str() == Some(cfg.label.as_str()),
        "synchronize" | "opened" | "reopened" | "ready_for_review" => carries_label,
        _ => false,
    };
    if !triggered {
        if action == "synchronize" {
            info!(
                owner,
                name,
                pr_number,
                label = %cfg.label,
                "Reviewer skipped — new commit on a PR without the review label"
            );
        }
        return Ok(StatusCode::OK);
    }

    // Draft gate — never review a PR that is still a draft. This is the difference
    // between "reviewed the instant CoderHelm self-labeled its own half-built draft"
    // and "reviewed once the PR is genuinely ready". A draft carrying the label is a
    // queued request, not a live one: when it flips to ready GitHub fires
    // `ready_for_review` (draft=false) and we land back here with the label present.
    // An explicit `@coderhelm review` comment still works on a draft — that path is
    // separate (handle_issue_comment) — for a human who deliberately wants an early
    // look.
    if pr["draft"].as_bool().unwrap_or(false) {
        info!(
            owner,
            name,
            pr_number,
            action,
            "Reviewer skipped — PR is a draft; will review on ready_for_review"
        );
        return Ok(StatusCode::OK);
    }

    // Same-head dedup — collapse a burst of events for ONE commit into ONE review.
    // GitHub fires several `pull_request` events (opened, labeled, synchronize,
    // ready_for_review) for the same head within a second — CoderHelm's own
    // self-label on PR creation is a frequent trigger. Each event used to enqueue
    // its own review, so one commit was reviewed N times (observed: 4 identical
    // reviews of the same head in 1.3s). The claim is per (repo, pr, head): only the
    // first event through wins. A genuinely NEW commit has a different head → a
    // different key → reviews normally; an explicit `@coderhelm review` comment
    // bypasses this path entirely (handle_issue_comment).
    if !claim_review_slot(state, team_id, owner, name, pr_number, &head_sha).await {
        info!(
            owner,
            name,
            pr_number,
            action,
            "Reviewer skipped — this head was already claimed by a sibling event (dedup)"
        );
        return Ok(StatusCode::OK);
    }

    // Budget gate — same as every other enqueue path, so an over-limit team
    // can't keep spending tokens via label-triggered reviews.
    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(team_id, pr_number, "Reviewer skipped — token limit reached");
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    info!(
        owner,
        name, pr_number, is_bot_pr, action, "Reviewer: trigger matched — enqueuing review job"
    );
    let dedup_key = format!("head#{head_sha}");
    let message = WorkerMessage::Review(ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha,
        label: cfg.label,
        question: None,
        trigger: action.to_string(),
        dedup_key,
        reply_to_comment_id: None,
        requested_by: String::new(),
        ..Default::default()
    });
    send_to_queue_after(
        state,
        &state.config.ticket_queue_url,
        &message,
        review_delay_secs(action),
    )
    .await
}

/// How long a review started by a pull_request event waits in the queue. A push
/// (`synchronize`) waits [`REVIEW_PUSH_SETTLE_SECS`] so a burst of commits settles
/// first: by the time the worker reads the PR, GitHub lists the real latest
/// head, every superseded commit's review is skipped before it starts, and only
/// the last commit is reviewed. Undelayed, two commits pushed 2s apart were both
/// reviewed and the older one's verdict posted last. Other
/// events (label added, opened, ready for review) are single, deliberate steps
/// and start at once.
fn review_delay_secs(action: &str) -> i32 {
    if action == "synchronize" {
        REVIEW_PUSH_SETTLE_SECS
    } else {
        0
    }
}

const REVIEW_PUSH_SETTLE_SECS: i32 = 60;

/// How long a review claim blocks re-review of the SAME head. Sized to comfortably
/// outlast the burst of webhook events GitHub fires for one commit (seconds), while
/// still self-expiring so a stuck/failed review can be re-driven later (push a new
/// commit, or `@coderhelm review`). The claim is keyed by head_sha, so a new commit
/// is never blocked regardless of this window.
const REVIEW_DEDUP_TTL_SECS: u64 = 600;

/// Atomically claim the review slot for (team, repo, pr, head). Returns `true` if
/// THIS caller won the claim (should proceed to enqueue), `false` if a sibling
/// event already claimed this exact head (skip — it is already being reviewed).
/// Uses a conditional write so concurrent webhook deliveries race safely: exactly
/// one wins. Fails OPEN on any non-conditional error so a transient DynamoDB blip
/// never silently drops a legitimate review.
async fn claim_review_slot(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
    head_sha: &str,
) -> bool {
    let sk = format!("REVIEWCLAIM#{owner}/{name}#{pr_number:06}#{head_sha}");
    // An expired claim is taken over (common::claim compares `ttl` itself;
    // DynamoDB's sweeper deletes expired items hours late).
    let c = common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &sk,
        REVIEW_DEDUP_TTL_SECS,
    )
    .await;
    if let common::claim::Claim::Failed(e) = &c {
        warn!(owner, name, pr_number, error = %e, "Review dedup claim errored — allowing review (fail-open)");
    }
    c.won_or_failed_open()
}

pub(crate) async fn send_to_queue(
    state: &AppState,
    queue_url: &str,
    message: &WorkerMessage,
) -> Result<StatusCode, StatusCode> {
    send_to_queue_after(state, queue_url, message, 0).await
}

/// [`send_to_queue`], delivered to the worker after `delay_secs` (SQS allows up
/// to 900).
pub(crate) async fn send_to_queue_after(
    state: &AppState,
    queue_url: &str,
    message: &WorkerMessage,
    delay_secs: i32,
) -> Result<StatusCode, StatusCode> {
    let body = serde_json::to_string(message).map_err(|e| {
        error!("Failed to serialize message: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state
        .sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(&body)
        .delay_seconds(delay_secs)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send SQS message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!("Dispatched to SQS");
    // Every review job the gateway sends shows as queued on the dashboard
    // until the worker picks it up.
    if let WorkerMessage::Review(r) = message {
        common::review_live::mark_queued(
            &state.dynamo,
            &state.config.settings_table_name,
            &r.team_id,
            &r.repo_owner,
            &r.repo_name,
            r.pr_number,
            &r.trigger,
            r.question.as_deref().is_some_and(|q| !q.trim().is_empty()),
            i64::from(delay_secs),
        )
        .await;
    }
    Ok(StatusCode::ACCEPTED)
}

/// Write a run event to the events table for the resume flow.
async fn write_run_event(
    state: &AppState,
    run_id: &str,
    event_type: &str,
    payload: &serde_json::Value,
) {
    if state.config.events_table_name.is_empty() {
        return;
    }
    let now = chrono::Utc::now();
    let event_sk = format!("EVENT#{}#{}", now.format("%Y%m%dT%H%M%S%.3fZ"), event_type);

    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("RUN#{run_id}")))
        .item("sk", attr_s(&event_sk))
        .item("event_type", attr_s(event_type))
        .item("payload", attr_s(&payload.to_string()))
        .item(
            "processed",
            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
        )
        .item("created_at", attr_s(&now.to_rfc3339()))
        .item("expires_at", attr_n(now.timestamp() as u64 + 30 * 86400))
        .send()
        .await
    {
        error!("Failed to write run event: {e}");
    }
}

/// After a PR merges, check if the merged run's issue belongs to a plan task.
/// If so, find waiting tasks that depend on it and dispatch them to the worker.
async fn trigger_plan_dependents(
    state: &AppState,
    team_id: &str,
    issue_number: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use aws_sdk_dynamodb::types::AttributeValue;

    // Query all items in the plans table for this team (plans + tasks share pk)
    let mut exclusive_start_key = None;
    let mut matched_plan_id = String::new();
    let mut matched_task_id = String::new();

    'outer: loop {
        let mut query = state
            .dynamo
            .query()
            .table_name(&state.config.plans_table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()));

        if let Some(key) = exclusive_start_key.take() {
            query = query.set_exclusive_start_key(Some(key));
        }

        let result = query.send().await?;

        for item in result.items() {
            // Only look at task items (sk contains #TASK#)
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if !sk.contains("#TASK#") {
                continue;
            }

            let item_issue = item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            if item_issue == issue_number {
                // Parse plan_id and task_id from sk: PLAN#<plan_id>#TASK#<task_id>
                let parts: Vec<&str> = sk.split('#').collect();
                if parts.len() >= 4 {
                    matched_plan_id = parts[1].to_string();
                    matched_task_id = parts[3].to_string();
                }
                break 'outer;
            }
        }

        match result.last_evaluated_key() {
            Some(key) => exclusive_start_key = Some(key.clone()),
            None => break,
        }
    }

    if matched_plan_id.is_empty() || matched_task_id.is_empty() {
        return Ok(()); // Not a plan task — nothing to do
    }

    info!(
        team_id,
        plan_id = %matched_plan_id,
        task_id = %matched_task_id,
        "Merged PR belongs to plan task — checking for waiting dependents"
    );

    // Query all tasks in this plan to find those waiting on the matched task
    let tasks_result = state
        .dynamo
        .query()
        .table_name(&state.config.plans_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk_prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(
            ":sk_prefix",
            AttributeValue::S(format!("PLAN#{}#TASK#", matched_plan_id)),
        )
        .send()
        .await?;

    let mut tasks_to_continue: Vec<String> = Vec::new();
    for item in tasks_result.items() {
        let status = item
            .get("status")
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");
        let depends_on = item
            .get("depends_on")
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or("");

        if status == "waiting" && depends_on == matched_task_id {
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                .unwrap_or("");
            let parts: Vec<&str> = sk.split('#').collect();
            if parts.len() >= 4 {
                tasks_to_continue.push(parts[3].to_string());
            }
        }
    }

    if tasks_to_continue.is_empty() {
        return Ok(());
    }

    info!(
        team_id,
        plan_id = %matched_plan_id,
        tasks = tasks_to_continue.len(),
        "Triggering waiting plan tasks"
    );

    let message = WorkerMessage::PlanTaskContinue(PlanTaskContinueMessage {
        team_id: team_id.to_string(),
        plan_id: matched_plan_id,
        tasks: tasks_to_continue,
    });

    let body = serde_json::to_string(&message)?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(&body)
        .send()
        .await?;

    Ok(())
}

/// Look up run_id from the runs table by PR number using the repo-index GSI.
async fn lookup_run_by_pr(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> String {
    lookup_run_item_by_pr(state, team_id, owner, name, pr_number)
        .await
        .as_ref()
        .and_then(common::paged::run_id_of)
        .unwrap_or_default()
}

/// The run that opened `owner/name#pr_number`: its record's own PR (the
/// repo-index GSI), else one of a multi-repo run's PRs, matched by
/// repository AND number (`pr_keys`). A number alone matched a PR of the
/// same number in an unrelated repository.
async fn lookup_run_item_by_pr(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Option<common::paged::Item> {
    let team_repo = format!("{team_id}#{owner}/{name}");
    // Limit applies BEFORE the filter, so a single page only examined the
    // repo's newest 50 runs and dropped events for older PRs. Follow
    // LastEvaluatedKey until a match or the end.
    let primary = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(50);
    match common::paged::query_first_match(primary, |i| Some(i.clone())).await {
        Ok(Some(item)) => return Some(item),
        Ok(None) => {}
        Err(e) => error!("Failed to query run by PR number: {e}"),
    }

    // Fallback: a multi-repo run's other PRs, by repository and number.
    // Records from before `pr_keys` are matched by the PR's URL.
    let repo = format!("{owner}/{name}");
    let fallback = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .key_condition_expression("team_id = :tid")
        .filter_expression(
            "contains(pr_keys, :k) OR (attribute_not_exists(pr_keys) AND contains(pr_urls, :u))",
        )
        .expression_attribute_values(":tid", attr_s(team_id))
        .expression_attribute_values(":k", attr_s(&common::run_prs::pr_key(&repo, pr_number)))
        .expression_attribute_values(
            ":u",
            attr_s(&format!("https://github.com/{repo}/pull/{pr_number}")),
        )
        .scan_index_forward(false)
        .limit(100);
    match common::paged::query_first_match(fallback, |i| Some(i.clone())).await {
        Ok(found) => found,
        Err(e) => {
            error!("Failed to query run by PR number (fallback): {e}");
            None
        }
    }
}

// DynamoDB attribute helpers
fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}

/// Why a reached limit stops new work, naming the limit. The old single
/// limit keeps its old wording.
pub fn limit_reason(hit: &common::token_limits::LimitUse) -> String {
    use common::token_limits::{short_tokens, LimitKind};
    let settings = format!(
        "[Settings → Token Limit]({}/settings/budget)",
        common::public_urls::app_url_from_env()
    );
    match hit.kind {
        LimitKind::Legacy { .. } => {
            let limit_label = if hit.max >= 1_000_000 {
                format!("{}M", hit.max / 1_000_000)
            } else {
                format!("{}K", hit.max / 1_000)
            };
            format!(
                "You've used all **{limit_label}** tokens this month. \
                 Increase your token limit in {settings} to continue.",
            )
        }
        kind => format!(
            "You've reached this month's **{}** limit ({} of {}). \
             Raise or clear that limit in {settings} to continue.",
            kind.label(),
            short_tokens(hit.used),
            short_tokens(hit.max),
        ),
    }
}

/// Check whether this team has budget remaining. Returns Some(reason) if
/// blocked: each set limit (new tokens, cache read, cache write, or the old
/// single limit) is checked on its own, and the reason names the one reached.
pub async fn check_run_budget(state: &AppState, team_id: &str) -> Option<String> {
    let limits =
        common::token_limits::load(&state.dynamo, &state.config.settings_table_name, team_id)
            .await
            .ok()?;
    if limits.is_empty() {
        return None;
    }
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let totals = common::token_limits::load_month_usage(
        &state.dynamo,
        &state.config.analytics_table_name,
        team_id,
        &month,
    )
    .await
    .ok()?;
    limits.reached(&totals).map(|hit| limit_reason(&hit))
}

#[cfg(test)]
mod limit_reason_tests {
    use super::limit_reason;
    use common::token_limits::{LimitKind, LimitUse};

    #[test]
    fn the_skip_comment_names_the_limit_reached() {
        let r = limit_reason(&LimitUse {
            kind: LimitKind::CacheRead,
            max: 50_000_000,
            used: 51_200_000,
        });
        assert!(
            r.starts_with(
                "You've reached this month's **cache read tokens** limit (51.2M of 50M)."
            ),
            "{r}"
        );
        // The old single limit keeps its old wording.
        let r = limit_reason(&LimitUse {
            kind: LimitKind::Legacy { counts_cache: true },
            max: 200_000_000,
            used: 200_000_001,
        });
        assert!(
            r.starts_with("You've used all **200M** tokens this month. Increase your token limit"),
            "{r}"
        );
    }
}

/// Post a comment on a GitHub issue explaining why the run was skipped.
async fn post_limit_comment(
    state: &AppState,
    installation_id: u64,
    owner: &str,
    repo: &str,
    issue_number: u64,
    reason: &str,
) {
    if owner.is_empty() || repo.is_empty() || issue_number == 0 {
        return;
    }

    // Get installation access token
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get installation token for limit comment: {e}");
            return;
        }
    };

    let url = format!("https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments");
    let body = serde_json::json!({
        "body": format!("⚠️ **Coderhelm — run skipped**\n\n{reason}")
    });

    if let Err(e) = state
        .http
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(&body)
        .send()
        .await
    {
        warn!("Failed to post limit comment: {e}");
    }
}

#[cfg(test)]
mod review_delay_tests {

    #[test]
    fn only_peoples_commits_count_as_edits() {
        let commits = vec![
            serde_json::json!({"author": {"login": "coderhelm[bot]", "type": "Bot"}}),
            serde_json::json!({"author": {"login": "dependabot[bot]", "type": "Bot"}}),
            serde_json::json!({"author": {"login": "a-person", "type": "User"}}),
            serde_json::json!({"author": null, "committer": {"login": "another-person"}}),
            serde_json::json!({"author": null, "committer": null}),
        ];
        assert_eq!(super::human_commit_count(&commits), 2);
        assert_eq!(super::human_commit_count(&[]), 0);
    }

    use super::{review_delay_secs, REVIEW_PUSH_SETTLE_SECS};

    #[test]
    fn only_push_reviews_wait_for_the_burst_to_settle() {
        assert_eq!(review_delay_secs("synchronize"), REVIEW_PUSH_SETTLE_SECS);
        for action in ["labeled", "opened", "reopened", "ready_for_review"] {
            assert_eq!(review_delay_secs(action), 0, "{action}");
        }
        // Within SQS's 900s per-message cap.
        assert!((1..=900).contains(&REVIEW_PUSH_SETTLE_SECS));
    }
}

#[cfg(test)]
mod bot_comment_tests {
    use super::is_bot_comment;
    use serde_json::json;

    #[test]
    fn bot_accounts_are_detected_by_user_type() {
        let ci = json!({"comment": {"user": {"login": "github-actions[bot]", "type": "Bot"}}});
        assert!(is_bot_comment(&ci));
        let person = json!({"comment": {"user": {"login": "some-dev", "type": "User"}}});
        assert!(!is_bot_comment(&person));
        // A person whose login merely ends in "bot" is still a person.
        let named = json!({"comment": {"user": {"login": "robot-fan", "type": "User"}}});
        assert!(!is_bot_comment(&named));
        assert!(!is_bot_comment(&json!({})));
    }
}

#[cfg(test)]
mod reviewer_ask_tests {
    use super::{parse_reviewer_ask, ReviewerAsk};

    fn rereview() -> Option<ReviewerAsk> {
        Some(ReviewerAsk {
            rereview: true,
            question: None,
        })
    }

    fn ask(q: &str) -> Option<ReviewerAsk> {
        Some(ReviewerAsk {
            rereview: false,
            question: Some(q.into()),
        })
    }

    #[test]
    fn mention_is_case_insensitive() {
        assert_eq!(parse_reviewer_ask("@CoderHelm re-review"), rereview());
        assert_eq!(parse_reviewer_ask("@CODERHELM rereview"), rereview());
        assert_eq!(parse_reviewer_ask("@coderhelm review again"), rereview());
        assert_eq!(parse_reviewer_ask("@coderhelm re review!"), rereview());
        assert_eq!(parse_reviewer_ask("@coderhelm[bot] re-review"), rereview());
        assert_eq!(parse_reviewer_ask("@CoderHelm"), rereview());
    }

    /// Only the exact command is a fast path; what any other ask means is
    /// for the worker's model to read, so it is passed on as written.
    #[test]
    fn every_other_ask_goes_to_the_model() {
        assert_eq!(
            parse_reviewer_ask("@coderhelm can you approve?"),
            ask("can you approve?")
        );
        assert_eq!(
            parse_reviewer_ask("@coderhelm all addressed, please take another look"),
            ask("all addressed, please take another look")
        );
        assert_eq!(
            parse_reviewer_ask("@CODERHELM rereview please"),
            ask("rereview please")
        );
    }

    #[test]
    fn quoted_text_and_code_are_not_addressed_to_the_reviewer() {
        assert_eq!(parse_reviewer_ask("> @coderhelm re-review\nthanks!"), None);
        assert_eq!(
            parse_reviewer_ask("run `@coderhelm re-review` to retry"),
            None
        );
        assert_eq!(
            parse_reviewer_ask("```\n@coderhelm re-review\n```\nok"),
            None
        );
        assert_eq!(
            parse_reviewer_ask("> old text\n@coderhelm re-review"),
            rereview()
        );
    }

    #[test]
    fn an_edit_runs_a_command_only_when_it_adds_one() {
        use super::ask_added_by_edit;
        assert_eq!(
            ask_added_by_edit("looks good", "@coderhelm re-review"),
            rereview()
        );
        assert!(
            ask_added_by_edit("looks good", "looks good, @coderhelm can you approve?").is_some()
        );
        assert_eq!(
            ask_added_by_edit("@coderhelm re-review", "@coderhelm re-review (typo fixed)"),
            None
        );
        assert_eq!(ask_added_by_edit("a", "b"), None);
    }

    #[test]
    fn slash_command() {
        assert_eq!(parse_reviewer_ask("/coderhelm review"), rereview());
        assert_eq!(parse_reviewer_ask("  /CoderHelm re-review"), rereview());
    }

    #[test]
    fn question_keeps_original_text() {
        assert_eq!(
            parse_reviewer_ask("@CoderHelm why is `refund` unsafe here?"),
            Some(ReviewerAsk {
                rereview: false,
                question: Some("why is `refund` unsafe here?".into()),
            })
        );
    }

    #[test]
    fn unaddressed_comments_are_ignored() {
        assert_eq!(parse_reviewer_ask("lgtm, merging"), None);
        assert_eq!(parse_reviewer_ask("see coderhelm docs"), None);
    }

    #[test]
    fn non_ascii_body_is_safe() {
        assert_eq!(
            parse_reviewer_ask("@CoderHelm ¿por qué? 🚀"),
            Some(ReviewerAsk {
                rereview: false,
                question: Some("¿por qué? 🚀".into()),
            })
        );
    }
}

#[cfg(test)]
mod merge_control_command_tests {
    use super::*;
    use common::merge_gate::{parse_control, Control};

    fn control(body: &str) -> Option<Control> {
        parse_reviewer_ask(body)?
            .question
            .as_deref()
            .and_then(parse_control)
    }

    #[test]
    fn stop_and_resume_comments_with_their_aliases() {
        assert_eq!(control("@coderhelm stop auto-merge"), Some(Control::Stop));
        assert_eq!(control("@CoderHelm pause auto-merge"), Some(Control::Stop));
        assert_eq!(
            control("@coderhelm[bot] cancel automerge"),
            Some(Control::Stop)
        );
        assert_eq!(control("/coderhelm stop auto-merge"), Some(Control::Stop));
        assert_eq!(
            control("@coderhelm resume auto-merge"),
            Some(Control::Rearm)
        );
        assert_eq!(
            control("@coderhelm re-arm auto-merge"),
            Some(Control::Rearm)
        );
        assert_eq!(control("@coderhelm arm auto merge"), Some(Control::Rearm));
        // Reviews and questions are not commands, and a quoted or pasted
        // command is not addressed to CoderHelm.
        assert_eq!(control("@coderhelm re-review"), None);
        assert_eq!(control("@coderhelm why did auto-merge stop?"), None);
        assert_eq!(control("> @coderhelm stop auto-merge\nAgreed"), None);
        assert_eq!(
            control("Comment `@coderhelm stop auto-merge` to stop it"),
            None
        );
        assert_eq!(control("stop auto-merge"), None);
    }
}

#[cfg(test)]
mod tool_paths_tests {
    use super::tool_definition_paths;
    use serde_json::json;

    #[test]
    fn only_tool_files_in_the_tools_folder_are_proposals() {
        let p = json!({"commits": [
            {"added": [".coderhelm/tools/find-order.yaml", "src/app.ts"], "modified": [".coderhelm/tools/x.json"]},
            {"added": [".coderhelm/tools/../secrets.yaml", ".coderhelm/tools/notes.md", ".coderhelm/other.yaml"], "modified": [".coderhelm/tools/find-order.yaml"]}
        ]});
        assert_eq!(
            tool_definition_paths(&p),
            vec![
                ".coderhelm/tools/find-order.yaml",
                ".coderhelm/tools/x.json"
            ]
        );
        assert!(tool_definition_paths(&json!({})).is_empty());
    }
}

#[cfg(test)]
mod installation_owner_tests {
    use super::*;

    fn c(id: &str, linked: Option<&str>, created: &str, members: i32) -> InstallationCandidate {
        InstallationCandidate {
            team_id: id.into(),
            linked_at: linked.map(String::from),
            created_at: created.into(),
            members,
        }
    }

    #[test]
    fn first_linked_team_owns_the_installation() {
        let v = vec![
            c("TEAM#b", Some("2026-09-02T00:00:00Z"), "2026-01-01", 9),
            c("TEAM#a", Some("2026-09-01T00:00:00Z"), "2026-08-01", 1),
            c("TEAM#c", None, "2025-01-01", 20),
        ];
        assert_eq!(pick_installation_owner(&v).as_deref(), Some("TEAM#a"));
    }

    #[test]
    fn older_records_keep_member_count_routing() {
        let v = vec![
            c("TEAM#orphan", None, "2026-10-01", 1),
            c("TEAM#real", None, "2026-03-01", 6),
        ];
        assert_eq!(pick_installation_owner(&v).as_deref(), Some("TEAM#real"));
    }

    #[test]
    fn ties_break_by_oldest_then_id_whatever_the_order() {
        let a = c("TEAM#x", None, "2026-05-01", 2);
        let b = c("TEAM#y", None, "2026-04-01", 2);
        let d = c("TEAM#z", None, "", 2);
        let one = pick_installation_owner(&[a.clone(), b.clone(), d.clone()]);
        let two = pick_installation_owner(&[d, b, a]);
        assert_eq!(one.as_deref(), Some("TEAM#y"));
        assert_eq!(one, two);
        let same = vec![
            c("TEAM#2", None, "2026-01-01", 1),
            c("TEAM#1", None, "2026-01-01", 1),
        ];
        assert_eq!(pick_installation_owner(&same).as_deref(), Some("TEAM#1"));
    }

    #[test]
    fn shared_installation_is_logged_once() {
        assert!(first_time_seen_shared(987_654_321));
        assert!(!first_time_seen_shared(987_654_321));
    }
}

/// Whether a merged PR moves its run to `merged`. A run a person closed keeps
/// its status.
fn merge_sets_status(run_status: &str) -> bool {
    run_status != common::run_status::CLOSED
}

#[cfg(test)]
mod merge_status_tests {
    use super::merge_sets_status;

    #[test]
    fn a_merge_keeps_a_closed_run_closed() {
        assert!(!merge_sets_status("closed"));
        let others = common::run_status::ALL
            .iter()
            .filter(|s| **s != common::run_status::CLOSED);
        for s in others {
            assert!(merge_sets_status(s), "{s}");
        }
    }
}

#[cfg(test)]
mod own_pr_ci_after_run_tests {
    use super::{is_failed_conclusion, reopens_for_ci};

    #[test]
    fn only_real_failures_reopen_a_finished_run() {
        for c in ["failure", "timed_out", "startup_failure"] {
            assert!(is_failed_conclusion(c), "{c}");
        }
        for c in [
            "success",
            "skipped",
            "cancelled",
            "neutral",
            "stale",
            "action_required",
            "",
        ] {
            assert!(!is_failed_conclusion(c), "{c}");
        }
    }

    #[test]
    fn only_a_completed_run_is_reopened() {
        assert!(reopens_for_ci("completed"));
        for s in [
            "cancelled",
            "closed",
            "failed",
            "awaiting_ci",
            "running",
            "needs_outside_change",
            "",
        ] {
            assert!(!reopens_for_ci(s), "{s}");
        }
    }
}
