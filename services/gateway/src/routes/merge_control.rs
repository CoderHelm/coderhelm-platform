//! Stop and re-arm a PR's auto-merge gate, from a PR comment
//! (`@coderhelm stop auto-merge` / `@coderhelm resume auto-merge`) or the
//! dashboard. Both land in [`apply`], which writes the gate record through
//! `common::merge_gate::{stop, rearm}` and hands the gate to the worker:
//!
//! - A comment counts only from someone with write access (or higher) to the
//!   repo, read from GitHub with the installation token. Anyone else gets a
//!   polite reply and nothing changes.
//! - The dashboard needs a signed-in member of the team whose config holds
//!   the repo, and a POST (the session cookie is `SameSite=Strict`). Links in
//!   the PR's status comment only open that page: they carry no secret and
//!   change nothing on their own.
//! - A re-arm never merges by itself. The gate evaluates every condition
//!   again on the PR's current head; with no CoderHelm verdict on that head
//!   a review is queued (or the person is told it is missing).
//!
//! Every stop and re-arm is logged on the gate record (who, how, when).

use axum::http::StatusCode;
use common::merge_gate::{self, Actor, Control, RearmNext, RearmOutcome, StopOutcome};
use serde_json::Value;
use tracing::{info, warn};

use crate::models::{AwaitMergeMessage, ReviewMessage, WorkerMessage};
use crate::AppState;

/// The outcome of a stop or re-arm, for a reply on the PR or the dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlResult {
    /// "stopped", "already_stopped", "rearmed", "rechecking", "finished",
    /// "off" or "not_found".
    pub status: &'static str,
    pub message: String,
    /// Done as asked (the comment gets a 👍 instead of a reply).
    pub ok: bool,
}

fn result(status: &'static str, ok: bool, message: impl Into<String>) -> ControlResult {
    ControlResult {
        status,
        message: message.into(),
        ok,
    }
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// A GitHub login is letters, digits and single hyphens; anything else never
/// goes into a GitHub API path.
pub(crate) fn valid_login(login: &str) -> bool {
    !login.is_empty()
        && login.len() <= 39
        && login.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

pub(crate) async fn gh_get(
    state: &AppState,
    token: &str,
    url: &str,
) -> Result<(u16, Value), String> {
    let resp = state
        .http
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    Ok((status, body))
}

async fn gh_post(state: &AppState, token: &str, url: &str, body: &Value) {
    let res = state
        .http
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .json(body)
        .send()
        .await;
    match res {
        Ok(r) if !r.status().is_success() => {
            warn!(url, status = r.status().as_u16(), "GitHub write refused")
        }
        Err(e) => warn!(url, error = %e, "GitHub write failed"),
        _ => {}
    }
}

/// Stop or re-arm one PR's gate for `team_id`. The caller has already
/// decided the actor may (write access for a comment, team membership for
/// the dashboard). Reads the PR with the team's installation token, so a
/// repo the installation can't see is "not_found".
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
    control: Control,
    actor: &Actor,
) -> Result<ControlResult, StatusCode> {
    let cfg = super::github_webhook::load_review_config(state, team_id, owner, name).await?;
    if !cfg.auto_merge {
        return Ok(result(
            "off",
            false,
            format!("Auto-merge is off for {owner}/{name}, so there is nothing to stop or re-arm."),
        ));
    }
    let token = crate::auth::github_app::get_installation_token(state, installation_id)
        .await
        .map_err(|e| {
            warn!(owner, name, error = %e, "merge control: no installation token");
            StatusCode::BAD_GATEWAY
        })?;
    let (code, pr) = gh_get(
        state,
        &token,
        &format!("https://api.github.com/repos/{owner}/{name}/pulls/{pr_number}"),
    )
    .await
    .map_err(|e| {
        warn!(owner, name, pr_number, error = %e, "merge control: PR read failed");
        StatusCode::BAD_GATEWAY
    })?;
    if code == 404 {
        return Ok(result(
            "not_found",
            false,
            "CoderHelm can't see that pull request.",
        ));
    }
    if code >= 300 {
        warn!(
            owner,
            name, pr_number, code, "merge control: PR read refused"
        );
        return Err(StatusCode::BAD_GATEWAY);
    }
    if pr["state"].as_str() != Some("open") {
        let what = if pr["merged"].as_bool() == Some(true) {
            "merged"
        } else {
            "closed"
        };
        return Ok(result(
            "finished",
            false,
            format!("This pull request is already {what}; there is nothing to stop or re-arm."),
        ));
    }
    let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
    let base = pr["base"]["ref"].as_str().unwrap_or("").to_string();
    let self_authored = pr["user"]["login"]
        .as_str()
        .unwrap_or("")
        .contains("coderhelm");
    let now = chrono::Utc::now().timestamp();
    let table = &state.config.settings_table_name;
    let db_err = |e: Box<dyn std::error::Error + Send + Sync>| {
        warn!(owner, name, pr_number, error = %e, "merge control: gate record write failed");
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let gate_msg = WorkerMessage::AwaitMerge(AwaitMergeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        head_sha: head.clone(),
        base_branch: base,
        self_authored,
        attempts: 0,
        chain_id: String::new(),
        error_retries: 0,
    });
    match control {
        Control::Stop => {
            let out = merge_gate::stop(
                &state.dynamo,
                table,
                team_id,
                owner,
                name,
                pr_number,
                actor,
                now,
            )
            .await
            .map_err(db_err)?;
            match out {
                StopOutcome::Stopped => {
                    info!(
                        owner,
                        name,
                        pr_number,
                        via = actor.via,
                        "Auto-merge stopped by a person"
                    );
                    // The worker puts the stop on the PR's status comment. The
                    // stop itself is already in force.
                    if let Err(e) = super::github_webhook::send_to_queue(
                        state,
                        &state.config.ticket_queue_url,
                        &gate_msg,
                    )
                    .await
                    {
                        warn!(owner, name, pr_number, status = ?e, "merge control: status update not queued");
                    }
                    Ok(result(
                        "stopped",
                        true,
                        "Auto-merge stopped. It won't merge until someone re-arms it.",
                    ))
                }
                StopOutcome::AlreadyStopped(st) => Ok(result(
                    "already_stopped",
                    true,
                    format!(
                        "Auto-merge was already stopped by {}.",
                        merge_gate::public_name(&st.by, st.is_login)
                    ),
                )),
                StopOutcome::Finished(_) => Ok(result(
                    "finished",
                    false,
                    "Auto-merge already finished on this pull request.",
                )),
            }
        }
        Control::Rearm => {
            let out = merge_gate::rearm(
                &state.dynamo,
                table,
                team_id,
                owner,
                name,
                pr_number,
                actor,
                now,
            )
            .await
            .map_err(db_err)?;
            let rearmed = match out {
                RearmOutcome::Rearmed => true,
                RearmOutcome::NotStopped => false,
                RearmOutcome::Finished(_) => {
                    return Ok(result(
                        "finished",
                        false,
                        "Auto-merge already finished on this pull request.",
                    ))
                }
            };
            info!(
                owner,
                name,
                pr_number,
                rearmed,
                via = actor.via,
                "Auto-merge re-armed by a person"
            );
            let verdict = merge_gate::bot_verdict_at_head(
                &state.dynamo,
                table,
                team_id,
                owner,
                name,
                pr_number,
                &head,
            )
            .await
            .map_err(db_err)?;
            let next = merge_gate::rearm_next(verdict.as_deref(), cfg.enabled && !cfg.killed);
            if next == RearmNext::QueueReview {
                let review = WorkerMessage::Review(ReviewMessage {
                    team_id: team_id.to_string(),
                    installation_id,
                    repo_owner: owner.to_string(),
                    repo_name: name.to_string(),
                    pr_number,
                    head_sha: String::new(),
                    label: cfg.label.clone(),
                    question: None,
                    trigger: "rerequest".to_string(),
                    dedup_key: format!("rearm#{head}"),
                    reply_to_comment_id: None,
                    requested_by: if actor.is_login {
                        actor.by.clone()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                });
                super::github_webhook::send_to_queue(
                    state,
                    &state.config.ticket_queue_url,
                    &review,
                )
                .await?;
            }
            // The gate checks every condition again on the current head.
            super::github_webhook::send_to_queue(state, &state.config.ticket_queue_url, &gate_msg)
                .await?;
            Ok(rearm_message(rearmed, next, short(&head)))
        }
    }
}

/// Pure: what a re-arm says. It names what the gate still needs, never
/// promises a merge.
pub(crate) fn rearm_message(rearmed: bool, next: RearmNext, head: &str) -> ControlResult {
    let lead = if rearmed {
        "Auto-merge re-armed."
    } else {
        "Auto-merge wasn't stopped; it checks this pull request again now."
    };
    let status = if rearmed { "rearmed" } else { "rechecking" };
    match next {
        RearmNext::Evaluate => result(
            status,
            true,
            format!(
                "{lead} Before merging it checks CoderHelm's approval of commit {head}, the \
                 required approvals and CI again."
            ),
        ),
        RearmNext::QueueReview => result(
            status,
            true,
            format!(
                "{lead} CoderHelm hasn't reviewed commit {head} yet, so a review is queued. It \
                 merges only once CoderHelm approves that commit and the approvals and CI are in."
            ),
        ),
        RearmNext::NeedsReview => result(
            status,
            false,
            format!(
                "{lead} CoderHelm hasn't reviewed commit {head} and review is off for this repo, \
                 so it won't merge until CoderHelm approves this commit."
            ),
        ),
    }
}

/// Where a comment that asked came from (for the 👍).
#[derive(Debug, Clone, Copy)]
pub(crate) enum CommentRef {
    /// A conversation comment on the PR.
    Issue(u64),
    /// A reply in an inline review thread.
    ReviewThread(u64),
}

/// A `@coderhelm stop|resume auto-merge` comment: check the commenter's
/// access, apply it, then react 👍 (done) or reply (refused, nothing to do,
/// or something the person should know).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn on_comment(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
    commenter: &str,
    control: Control,
    comment: CommentRef,
) -> Result<StatusCode, StatusCode> {
    if pr_number == 0 || !valid_login(commenter) {
        return Ok(StatusCode::OK);
    }
    let token = match crate::auth::github_app::get_installation_token(state, installation_id).await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(owner, name, error = %e, "merge control: no installation token");
            return Err(StatusCode::BAD_GATEWAY);
        }
    };
    let api = format!("https://api.github.com/repos/{owner}/{name}");
    let reply = |text: String| {
        let token = token.clone();
        let url = format!("{api}/issues/{pr_number}/comments");
        async move {
            gh_post(state, &token, &url, &serde_json::json!({ "body": text })).await;
        }
    };
    let allowed = match gh_get(
        state,
        &token,
        &format!("{api}/collaborators/{commenter}/permission"),
    )
    .await
    {
        Ok((code, body)) if code < 300 => merge_gate::has_write_access(&body),
        Ok((code, _)) => {
            info!(
                owner,
                name, commenter, code, "merge control: no permission answer"
            );
            false
        }
        Err(e) => {
            warn!(owner, name, commenter, error = %e, "merge control: permission read failed");
            false
        }
    };
    if !allowed {
        info!(
            owner,
            name, pr_number, commenter, "merge control: refused (no write access)"
        );
        reply(refusal(commenter, control)).await;
        return Ok(StatusCode::OK);
    }
    let actor = Actor {
        by: commenter.to_string(),
        is_login: true,
        via: merge_gate::VIA_COMMENT,
    };
    let out = apply(
        state,
        team_id,
        installation_id,
        owner,
        name,
        pr_number,
        control,
        &actor,
    )
    .await?;
    if out.ok {
        let url = match comment {
            CommentRef::Issue(id) => format!("{api}/issues/comments/{id}/reactions"),
            CommentRef::ReviewThread(id) => format!("{api}/pulls/comments/{id}/reactions"),
        };
        gh_post(state, &token, &url, &serde_json::json!({ "content": "+1" })).await;
    }
    // The status comment shows a stop or re-arm; anything else (already
    // stopped, a queued review, a refusal) gets a short reply.
    if !out.ok || !matches!(out.status, "stopped" | "rearmed") || out.message.contains("queued") {
        reply(format!("@{commenter} {}", out.message)).await;
    }
    Ok(StatusCode::OK)
}

/// Pure: the reply to someone without write access.
pub(crate) fn refusal(commenter: &str, control: Control) -> String {
    let what = match control {
        Control::Stop => "stop",
        Control::Rearm => "re-arm",
    };
    format!(
        "@{commenter} only people with write access to this repository can {what} auto-merge, \
         so nothing changed. Someone with write access can comment `{}` or use CoderHelm.",
        match control {
            Control::Stop => merge_gate::STOP_COMMAND,
            Control::Rearm => merge_gate::RESUME_COMMAND,
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logins_are_checked_before_they_reach_a_url() {
        assert!(valid_login("a-reviewer"));
        assert!(valid_login("Dev42"));
        assert!(!valid_login(""));
        assert!(!valid_login("a/../../orgs"));
        assert!(!valid_login("bot[bot]"));
        assert!(!valid_login(&"a".repeat(40)));
    }

    #[test]
    fn a_read_only_commenter_is_refused_politely() {
        let r = refusal("a-reader", Control::Stop);
        assert!(r.starts_with("@a-reader only people with write access"));
        assert!(r.contains("nothing changed"));
        assert!(r.contains("`@coderhelm stop auto-merge`"));
        assert!(refusal("a-reader", Control::Rearm).contains("`@coderhelm resume auto-merge`"));
    }

    #[test]
    fn rearm_says_what_the_gate_still_needs() {
        let r = rearm_message(true, RearmNext::Evaluate, "abc1234");
        assert_eq!(r.status, "rearmed");
        assert!(
            r.ok && r
                .message
                .contains("checks CoderHelm's approval of commit abc1234")
        );
        let r = rearm_message(true, RearmNext::QueueReview, "abc1234");
        assert!(r.ok && r.message.contains("review is queued"));
        // No verdict and review off: said plainly, and never a merge promise.
        let r = rearm_message(false, RearmNext::NeedsReview, "abc1234");
        assert_eq!(r.status, "rechecking");
        assert!(!r.ok && r.message.contains("won't merge until CoderHelm approves"));
    }
}
