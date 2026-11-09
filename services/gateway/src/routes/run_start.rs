//! POST /api/runs/start — a person starts CoderHelm on a ticket from outside
//! its tracker (a connected app's `start_run`, or a script).
//!
//! - A GitHub issue (its URL or `owner/name#N`): read from GitHub with the
//!   team's app, and queued exactly as labeling it `coderhelm` would queue
//!   it, with the person as the one who asked.
//! - A Jira key: Jira sends CoderHelm its tickets (CoderHelm can't read Jira
//!   itself), so a key CoderHelm has seen starts again from its latest run,
//!   as Retry does; one it hasn't seen is refused with how to start it.
//!
//! Like any person's start: the team's token limit applies, and a run
//! already working on the ticket blocks a second one (`trigger_gate`, as an
//! explicit ask).

use crate::models::{TicketMessage, TicketSource, WorkerMessage};
use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use super::trigger_gate::{self, TicketGate};
use crate::models::Claims;
use crate::AppState;

type ApiError = (StatusCode, Json<Value>);

fn fail(code: StatusCode, msg: &str) -> ApiError {
    (code, Json(json!({ "error": msg })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

/// A ticket, as a person names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketRef {
    Github { repo: String, number: u64 },
    Jira(String),
}

/// A Jira key: a project key (a letter, then letters, digits or `_`) and a
/// number, `WEB-42`.
fn jira_key(raw: &str) -> Option<String> {
    let key = raw.trim().to_ascii_uppercase();
    let (project, number) = key.rsplit_once('-')?;
    let mut chars = project.chars();
    let project_ok = chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && project.len() <= 20
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    let number_ok =
        !number.is_empty() && number.len() <= 9 && number.chars().all(|c| c.is_ascii_digit());
    (project_ok && number_ok).then_some(key)
}

pub fn parse_ticket(raw: &str) -> Option<TicketRef> {
    if let Some((repo, number)) = crate::mcp_server::parse_github_ref(raw, "issues") {
        return Some(TicketRef::Github { repo, number });
    }
    jira_key(raw).map(TicketRef::Jira)
}

#[derive(Deserialize)]
pub struct StartBody {
    ticket: String,
}

/// What the gate's answer means for a person asking.
fn gate_refusal(gate: &TicketGate) -> Option<ApiError> {
    match gate {
        TicketGate::Enqueue => None,
        TicketGate::SkipInFlight => Some(fail(
            StatusCode::CONFLICT,
            "A run is already working on this ticket. Follow it in the runs list.",
        )),
        TicketGate::OpenPr(pr) => Some(fail(
            StatusCode::CONFLICT,
            &format!(
                "This ticket's pull request #{} is still open. Comment there to change it.",
                pr.pr_number
            ),
        )),
        TicketGate::SkipUnchanged => Some(fail(
            StatusCode::CONFLICT,
            "CoderHelm already finished this ticket and it hasn't changed since.",
        )),
    }
}

/// POST /api/runs/start
pub async fn start_run(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<StartBody>,
) -> Result<Json<Value>, ApiError> {
    // member+
    if claims.require_role(1).is_err() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Your role doesn't allow starting runs.",
        ));
    }
    let Some(ticket) = parse_ticket(&body.ticket) else {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Give a GitHub issue (https://github.com/owner/name/issues/42 or owner/name#42) or a Jira key such as WEB-42.",
        ));
    };
    if let Some(reason) = super::github_webhook::check_run_budget(&state, &claims.team_id).await {
        return Err(fail(StatusCode::TOO_MANY_REQUESTS, &reason));
    }
    let (message, ticket_id) = match ticket {
        TicketRef::Github { repo, number } => github_ticket(&state, &claims, &repo, number).await?,
        TicketRef::Jira(key) => jira_ticket(&state, &claims, &key).await?,
    };
    super::github_webhook::send_to_queue(
        &state,
        &state.config.ticket_queue_url,
        &WorkerMessage::Ticket(message),
    )
    .await
    .map_err(|s| fail(s, "CoderHelm couldn't queue the run. Try again."))?;
    info!(team_id = %claims.team_id, ticket_id = %ticket_id, "Run started by a person from outside the tracker");
    Ok(Json(json!({
        "status": "queued",
        "ticket_id": ticket_id,
        "message": format!("CoderHelm queued a run for {ticket_id}. It shows in the runs list within a minute."),
    })))
}

async fn github_ticket(
    state: &AppState,
    claims: &Claims,
    repo: &str,
    number: u64,
) -> Result<(TicketMessage, String), ApiError> {
    let installation_id = super::api::get_team_installation_id(state, &claims.team_id)
        .await
        .map_err(|_| {
            fail(
                StatusCode::BAD_REQUEST,
                "Connect GitHub to CoderHelm first (Settings, GitHub).",
            )
        })?;
    let token = crate::auth::github_app::get_installation_token(state, installation_id)
        .await
        .map_err(|e| internal("GitHub token for a start", e))?;
    let res = state
        .http
        .get(format!(
            "https://api.github.com/repos/{repo}/issues/{number}"
        ))
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "coderhelm")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send()
        .await
        .map_err(|e| internal("GitHub issue not read", e))?;
    if matches!(res.status().as_u16(), 403 | 404 | 410) {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "CoderHelm can't see that issue. Check the address, and that the repository is connected to CoderHelm.",
        ));
    }
    if !res.status().is_success() {
        return Err(internal("GitHub issue not read", res.status()));
    }
    let issue: Value = res
        .json()
        .await
        .map_err(|e| internal("GitHub issue not readable", e))?;
    if issue["pull_request"].is_object() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "That's a pull request, not an issue. Ask for a review of it instead.",
        ));
    }
    let (owner, name) = repo.split_once('/').unwrap_or((repo, ""));
    let ticket_id = format!("GH-{number}");
    let title = issue["title"].as_str().unwrap_or("");
    let body = issue["body"].as_str().unwrap_or("");
    let hash = common::ticket_context_hash(title, body, &[]);
    let gate = trigger_gate::gate_ticket_trigger(
        state,
        &claims.team_id,
        &ticket_id,
        Some(&hash),
        true,
        false,
    )
    .await;
    if let Some(refused) = gate_refusal(&gate) {
        return Err(refused);
    }
    Ok((
        TicketMessage {
            team_id: claims.team_id.clone(),
            installation_id,
            source: TicketSource::Github,
            ticket_id: ticket_id.clone(),
            title: title.to_string(),
            body: body.to_string(),
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            issue_number: number,
            sender: claims
                .github_login
                .clone()
                .unwrap_or_else(|| claims.email.clone()),
            requester: common::requester::Requester::member(
                claims.github_login.as_deref(),
                &claims.email,
            ),
            image_attachments: vec![],
            ticket_type: common::messages::ticket_type_of_github(&issue),
            ..Default::default()
        },
        ticket_id,
    ))
}

async fn jira_ticket(
    state: &AppState,
    claims: &Claims,
    key: &str,
) -> Result<(TicketMessage, String), ApiError> {
    let runs = common::log_findings::runs_for_ticket(
        &state.dynamo,
        &state.config.runs_table_name,
        &claims.team_id,
        key,
    )
    .await
    .map_err(|e| internal("Runs of a ticket not read", e))?;
    let Some(latest) = runs.iter().max_by(|a, b| a.run_id.cmp(&b.run_id)) else {
        return Err(fail(
            StatusCode::NOT_FOUND,
            &format!("CoderHelm hasn't received {key} from Jira yet. Start it from Jira the way your team does, or give a task and a repository instead."),
        ));
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(claims.team_id.clone()))
        .key("run_id", AttributeValue::S(latest.run_id.clone()))
        .send()
        .await
        .map_err(|e| internal("Run not read", e))?
        .item
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That ticket's last run is gone."))?;
    let gate =
        trigger_gate::gate_ticket_trigger(state, &claims.team_id, key, None, true, false).await;
    if let Some(refused) = gate_refusal(&gate) {
        return Err(refused);
    }
    let message = super::api::retry_message(state, claims, &item)
        .await
        .map_err(|s| fail(s, "CoderHelm couldn't start that ticket again."))?;
    // A person asked: a stale lock from an earlier run never holds it back.
    super::api::delete_ticket_lock(state, &claims.team_id, key).await;
    Ok((message, key.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tickets_are_github_issues_or_jira_keys() {
        assert_eq!(
            parse_ticket("https://github.com/acme/web/issues/42"),
            Some(TicketRef::Github {
                repo: "acme/web".into(),
                number: 42
            })
        );
        assert_eq!(
            parse_ticket("acme/web#42"),
            Some(TicketRef::Github {
                repo: "acme/web".into(),
                number: 42
            })
        );
        assert_eq!(
            parse_ticket("web-42"),
            Some(TicketRef::Jira("WEB-42".into()))
        );
        assert_eq!(
            parse_ticket("OPS_2-7"),
            Some(TicketRef::Jira("OPS_2-7".into()))
        );
        for bad in [
            "",
            "42",
            "WEB-",
            "-42",
            "2WEB-4",
            "WEB-4x",
            "https://github.com/acme/web/pull/42",
            "https://gitlab.com/acme/web/issues/42",
        ] {
            assert_eq!(parse_ticket(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_blocked_ticket_says_why() {
        assert!(gate_refusal(&TicketGate::Enqueue).is_none());
        let (code, body) = gate_refusal(&TicketGate::SkipInFlight).unwrap();
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("already working"));
    }
}
