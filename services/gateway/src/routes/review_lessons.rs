//! The reviewer learns from people's pushback (see `common::review_lessons`).
//!
//! Webhooks: a person replying under a review comment, commenting on a PR
//! CoderHelm reviewed, dismissing its review, or approving, merging or
//! closing the PR while its "changes requested" stands queues one judgment
//! per PR after a quiet period. Whether any of it disputes a finding is decided by the worker's
//! model, never here.
//!
//! Dashboard: the team's lessons are listed (anyone on the team) and a wrong
//! one can be deleted (admins). Both are scoped to the caller's team.

use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use common::review_lessons as rl;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::{Claims, ReviewLessonMessage, WorkerMessage};
use crate::AppState;

/// A webhook event that may carry pushback on a review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LessonTrigger {
    pub owner: String,
    pub name: String,
    pub pr_number: u64,
    pub trigger: &'static str,
    /// Counts only while CoderHelm's latest verdict requests changes.
    pub needs_changes_requested: bool,
}

/// A person: not a bot account, and not CoderHelm.
fn is_person(user: &Value) -> bool {
    let login = user["login"].as_str().unwrap_or("");
    !login.is_empty() && user["type"].as_str() != Some("Bot") && !login.contains("coderhelm")
}

/// Pure: the event as a lesson trigger, if it is one.
pub fn detect(event_type: &str, payload: &Value) -> Option<LessonTrigger> {
    let action = payload["action"].as_str().unwrap_or("");
    let repo = &payload["repository"];
    let make = |pr: &Value, trigger, needs_changes_requested| {
        let pr_number = pr.as_u64().unwrap_or(0);
        let owner = repo["owner"]["login"].as_str().unwrap_or("");
        let name = repo["name"].as_str().unwrap_or("");
        (pr_number != 0 && !owner.is_empty() && !name.is_empty()).then(|| LessonTrigger {
            owner: owner.to_string(),
            name: name.to_string(),
            pr_number,
            trigger,
            needs_changes_requested,
        })
    };
    let has_body = |v: &Value| !v.as_str().unwrap_or("").trim().is_empty();
    match event_type {
        // A reply inside a review thread.
        "pull_request_review_comment" => {
            let c = &payload["comment"];
            (action == "created"
                && is_person(&c["user"])
                && c["in_reply_to_id"].as_u64().is_some()
                && has_body(&c["body"]))
            .then_some(())?;
            make(
                &payload["pull_request"]["number"],
                rl::TRIGGER_THREAD_REPLY,
                false,
            )
        }
        // A comment in a PR's conversation.
        "issue_comment" => {
            let c = &payload["comment"];
            (action == "created"
                && payload["issue"]["pull_request"].is_object()
                && is_person(&c["user"])
                && has_body(&c["body"]))
            .then_some(())?;
            make(&payload["issue"]["number"], rl::TRIGGER_PR_COMMENT, false)
        }
        "pull_request_review" => {
            let r = &payload["review"];
            if action == "dismissed" {
                let mine = r["user"]["login"]
                    .as_str()
                    .unwrap_or("")
                    .contains("coderhelm");
                (mine && is_person(&payload["sender"])).then_some(())?;
                return make(
                    &payload["pull_request"]["number"],
                    rl::TRIGGER_DISMISSED,
                    false,
                );
            }
            let state = r["state"].as_str().unwrap_or("");
            // A person approving over CoderHelm's standing block, words or
            // not: the strongest sign the block was wrong.
            if action == "submitted" && state == "approved" {
                is_person(&r["user"]).then_some(())?;
                return make(
                    &payload["pull_request"]["number"],
                    rl::TRIGGER_APPROVED_OVER_BLOCK,
                    true,
                );
            }
            (action == "submitted"
                && is_person(&r["user"])
                && matches!(state, "commented" | "changes_requested")
                && has_body(&r["body"]))
            .then_some(())?;
            make(
                &payload["pull_request"]["number"],
                rl::TRIGGER_REVIEW_BODY,
                false,
            )
        }
        "pull_request" => {
            let pr = &payload["pull_request"];
            (action == "closed" && is_person(&payload["sender"])).then_some(())?;
            if pr["merged"].as_bool().unwrap_or(false) {
                // A person merged over CoderHelm's standing block.
                make(&pr["number"], rl::TRIGGER_MERGED_OVER_BLOCK, true)
            } else {
                make(&pr["number"], rl::TRIGGER_CLOSED, true)
            }
        }
        _ => None,
    }
}

/// Pure: does the PR qualify, given CoderHelm's review records' verdicts
/// (newest first)? It must have reviewed the PR; a close also needs its
/// latest verdict to request changes.
pub fn qualifies(verdicts_newest_first: &[String], needs_changes_requested: bool) -> bool {
    if verdicts_newest_first.is_empty() {
        return false;
    }
    !needs_changes_requested
        || verdicts_newest_first
            .iter()
            .find(|v| v.as_str() == "APPROVE" || v.as_str() == "REQUEST_CHANGES")
            .is_some_and(|v| v == "REQUEST_CHANGES")
}

/// CoderHelm's verdicts on this PR, newest first (empty when it never
/// reviewed it).
async fn verdicts(state: &AppState, team_id: &str, t: &LessonTrigger) -> Vec<String> {
    let prefix = format!("REVIEW#{}/{}#{:06}#", t.owner, t.name, t.pr_number);
    match state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":p", attr_s(&prefix))
        .scan_index_forward(false)
        .limit(10)
        .send()
        .await
    {
        Ok(r) => r
            .items()
            .iter()
            .map(|i| {
                i.get("verdict")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            })
            .collect(),
        Err(e) => {
            warn!(error = %e, "Could not read the PR's reviews for a lesson trigger");
            Vec::new()
        }
    }
}

fn attr_s(v: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(v.to_string())
}

/// Queues a judgment when the event is pushback on a PR CoderHelm reviewed.
/// Best-effort: never fails the webhook.
pub async fn on_webhook(
    state: &AppState,
    event_type: &str,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) {
    let Some(t) = detect(event_type, payload) else {
        return;
    };
    if !qualifies(
        &verdicts(state, team_id, &t).await,
        t.needs_changes_requested,
    ) {
        return;
    }
    let event_at = match rl::mark_event(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &t.owner,
        &t.name,
        t.pr_number,
        t.trigger,
    )
    .await
    {
        Ok(at) => at,
        Err(e) => {
            warn!(error = %e, "Could not record pushback on a review");
            return;
        }
    };
    let msg = WorkerMessage::ReviewLesson(ReviewLessonMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: t.owner.clone(),
        repo_name: t.name.clone(),
        pr_number: t.pr_number,
        trigger: t.trigger.to_string(),
        event_at,
    });
    match super::github_webhook::send_to_queue_after(
        state,
        &state.config.ticket_queue_url,
        &msg,
        rl::DEBOUNCE_SECS,
    )
    .await
    {
        Ok(_) => info!(
            owner = %t.owner,
            name = %t.name,
            pr = t.pr_number,
            trigger = t.trigger,
            "Pushback on a review → judgment queued"
        ),
        Err(e) => warn!(status = ?e, "Could not queue the review lesson judgment"),
    }
}

#[derive(Deserialize)]
pub struct ListQuery {
    /// `owner/name`: that repository's lessons and the team's. All when absent.
    pub repo: Option<String>,
}

/// GET /api/reviewer/lessons — what the reviewer learned from the team's
/// pushback, newest first.
pub async fn list_lessons(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(0)?;
    let table = &state.config.settings_table_name;
    let res = match q.repo.as_deref().filter(|r| !r.is_empty()) {
        Some(repo) => {
            let (owner, name) = repo
                .split_once('/')
                .filter(|_| rl::valid_scope(repo) && repo != rl::TEAM_SCOPE)
                .ok_or(StatusCode::BAD_REQUEST)?;
            rl::for_repo(&state.dynamo, table, &claims.team_id, owner, name).await
        }
        None => rl::list_all(&state.dynamo, table, &claims.team_id).await,
    };
    let mut lessons = res.map_err(|e| {
        error!(error = %e, "Could not list review lessons");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    lessons.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(Json(json!({ "lessons": lessons })))
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    pub sk: String,
}

/// Pure: the key a delete may touch — always the caller's own team, and only
/// a lesson's sort key.
pub fn delete_key(claims: &Claims, sk: &str) -> Result<(String, String), StatusCode> {
    if !rl::is_lesson_sk(sk) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok((claims.team_id.clone(), sk.to_string()))
}

/// DELETE /api/reviewer/lessons?sk=… — removes a wrong lesson.
pub async fn delete_lesson(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<DeleteQuery>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let (pk, sk) = delete_key(&claims, &q.sk)?;
    match state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&pk))
        .key("sk", attr_s(&sk))
        .condition_expression("attribute_exists(sk)")
        .send()
        .await
    {
        Ok(_) => {
            info!(team_id = %pk, sk = %sk, by = %claims.display_name(), "Review lesson deleted");
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => {
            let missing = e
                .as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception());
            if missing {
                Err(StatusCode::NOT_FOUND)
            } else {
                error!(error = %e, "Could not delete a review lesson");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> Value {
        json!({"owner": {"login": "acme"}, "name": "web"})
    }
    fn person(login: &str) -> Value {
        json!({"login": login, "type": "User"})
    }
    fn bot(login: &str) -> Value {
        json!({"login": login, "type": "Bot"})
    }

    #[test]
    fn a_persons_thread_reply_triggers_and_a_bots_does_not() {
        let mut p = json!({
            "action": "created",
            "repository": repo(),
            "pull_request": {"number": 42},
            "comment": {"user": person("dev-one"), "in_reply_to_id": 10, "body": "This is intended."},
        });
        let t = detect("pull_request_review_comment", &p).unwrap();
        assert_eq!(t.trigger, rl::TRIGGER_THREAD_REPLY);
        assert_eq!(
            (t.owner.as_str(), t.name.as_str(), t.pr_number),
            ("acme", "web", 42)
        );
        assert!(!t.needs_changes_requested);

        p["comment"]["user"] = bot("ci-helper[bot]");
        assert_eq!(detect("pull_request_review_comment", &p), None);
        p["comment"]["user"] = person("coderhelm-dev");
        assert_eq!(detect("pull_request_review_comment", &p), None);
        // A new top-level comment is not a reply.
        p["comment"]["user"] = person("dev-one");
        p["comment"]["in_reply_to_id"] = Value::Null;
        assert_eq!(detect("pull_request_review_comment", &p), None);
        // An edit is not news.
        p["comment"]["in_reply_to_id"] = json!(10);
        p["action"] = json!("edited");
        assert_eq!(detect("pull_request_review_comment", &p), None);
    }

    #[test]
    fn a_pr_comment_by_a_person_triggers_but_not_on_an_issue() {
        let mut p = json!({
            "action": "created",
            "repository": repo(),
            "issue": {"number": 7, "pull_request": {}},
            "comment": {"user": person("dev-one"), "body": "Thanks!"},
        });
        // Whether it disputes anything is the model's call, not the gateway's.
        assert_eq!(
            detect("issue_comment", &p).unwrap().trigger,
            rl::TRIGGER_PR_COMMENT
        );
        p["issue"] = json!({"number": 7});
        assert_eq!(detect("issue_comment", &p), None);
        p["issue"] = json!({"number": 7, "pull_request": {}});
        p["comment"]["user"] = bot("preview[bot]");
        assert_eq!(detect("issue_comment", &p), None);
    }

    #[test]
    fn dismissing_coderhelms_review_triggers() {
        let mut p = json!({
            "action": "dismissed",
            "repository": repo(),
            "pull_request": {"number": 5},
            "review": {"user": bot("coderhelm[bot]"), "state": "dismissed"},
            "sender": person("lead"),
        });
        assert_eq!(
            detect("pull_request_review", &p).unwrap().trigger,
            rl::TRIGGER_DISMISSED
        );
        // A person's own review dismissed: nothing about CoderHelm.
        p["review"]["user"] = person("other");
        assert_eq!(detect("pull_request_review", &p), None);
        // CoderHelm dismissing its own review.
        p["review"]["user"] = bot("coderhelm[bot]");
        p["sender"] = bot("coderhelm[bot]");
        assert_eq!(detect("pull_request_review", &p), None);
    }

    #[test]
    fn a_persons_review_with_words_triggers() {
        let mut p = json!({
            "action": "submitted",
            "repository": repo(),
            "pull_request": {"number": 5},
            "review": {"user": person("lead"), "state": "commented", "body": "The bot is wrong here."},
        });
        assert_eq!(
            detect("pull_request_review", &p).unwrap().trigger,
            rl::TRIGGER_REVIEW_BODY
        );
        p["review"]["body"] = json!("");
        assert_eq!(detect("pull_request_review", &p), None);
    }

    #[test]
    fn a_person_approving_needs_a_standing_change_request() {
        let mut p = json!({
            "action": "submitted",
            "repository": repo(),
            "pull_request": {"number": 5},
            "review": {"user": person("lead"), "state": "approved", "body": ""},
        });
        // Words or not: approving over the block is the pushback.
        let t = detect("pull_request_review", &p).unwrap();
        assert_eq!(t.trigger, rl::TRIGGER_APPROVED_OVER_BLOCK);
        assert!(t.needs_changes_requested);
        p["review"]["body"] = json!("LGTM");
        assert_eq!(
            detect("pull_request_review", &p).unwrap().trigger,
            rl::TRIGGER_APPROVED_OVER_BLOCK
        );
        // CoderHelm's own approval, or a bot's, is not a person's.
        p["review"]["user"] = bot("coderhelm[bot]");
        assert_eq!(detect("pull_request_review", &p), None);
        p["review"]["user"] = bot("release-bot[bot]");
        assert_eq!(detect("pull_request_review", &p), None);
        // It counts only while CoderHelm's latest verdict requests changes.
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(qualifies(
            &v(&["REQUEST_CHANGES"]),
            t.needs_changes_requested
        ));
        assert!(!qualifies(
            &v(&["APPROVE", "REQUEST_CHANGES"]),
            t.needs_changes_requested
        ));
    }

    #[test]
    fn a_person_merging_needs_a_standing_change_request() {
        let mut p = json!({
            "action": "closed",
            "repository": repo(),
            "pull_request": {"number": 9, "merged": true},
            "sender": person("lead"),
        });
        let t = detect("pull_request", &p).unwrap();
        assert_eq!(t.trigger, rl::TRIGGER_MERGED_OVER_BLOCK);
        assert!(t.needs_changes_requested);
        // CoderHelm's own auto-merge is not pushback.
        p["sender"] = bot("coderhelm[bot]");
        assert_eq!(detect("pull_request", &p), None);
        // Opening or editing a merged-looking payload isn't a merge.
        p["sender"] = person("lead");
        p["action"] = json!("edited");
        assert_eq!(detect("pull_request", &p), None);
    }

    #[test]
    fn closing_unmerged_needs_a_standing_change_request() {
        let mut p = json!({
            "action": "closed",
            "repository": repo(),
            "pull_request": {"number": 9, "merged": false},
            "sender": person("dev-one"),
        });
        let t = detect("pull_request", &p).unwrap();
        assert_eq!(t.trigger, rl::TRIGGER_CLOSED);
        assert!(t.needs_changes_requested);
        p["pull_request"]["merged"] = json!(true);
        assert_eq!(
            detect("pull_request", &p).unwrap().trigger,
            rl::TRIGGER_MERGED_OVER_BLOCK
        );
        p["pull_request"]["merged"] = json!(false);
        p["sender"] = bot("coderhelm[bot]");
        assert_eq!(detect("pull_request", &p), None);
    }

    #[test]
    fn only_prs_coderhelm_reviewed_qualify() {
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(!qualifies(&[], false));
        assert!(qualifies(&v(&["COMMENT"]), false));
        assert!(qualifies(&v(&["REQUEST_CHANGES", "APPROVE"]), true));
        assert!(qualifies(&v(&["COMMENT", "REQUEST_CHANGES"]), true));
        assert!(!qualifies(&v(&["APPROVE", "REQUEST_CHANGES"]), true));
        assert!(!qualifies(&v(&["COMMENT"]), true));
    }

    fn claims(team: &str, role: &str) -> Claims {
        Claims {
            sub: "USER#1".into(),
            team_id: team.into(),
            email: "a@example.com".into(),
            role: role.into(),
            github_login: None,
            aud: "coderhelm-dashboard".into(),
            exp: 0,
            iat: 0,
        }
    }

    #[test]
    fn a_delete_only_touches_a_lesson_of_the_callers_team() {
        let c = claims("TEAM#1", "admin");
        assert_eq!(
            delete_key(&c, "REVIEWLESSON#acme/web#01ABC").unwrap(),
            (
                "TEAM#1".to_string(),
                "REVIEWLESSON#acme/web#01ABC".to_string()
            )
        );
        assert_eq!(
            delete_key(&c, "REVIEW#acme/web#000001#2026"),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            delete_key(&c, "REVIEW_CONFIG#REPO#acme/web"),
            Err(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn deleting_needs_an_admin_and_listing_anyone_on_the_team() {
        assert_eq!(
            claims("T", "member").require_role(3),
            Err(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            claims("T", "viewer").require_role(3),
            Err(StatusCode::FORBIDDEN)
        );
        assert!(claims("T", "admin").require_role(3).is_ok());
        assert!(claims("T", "owner").require_role(3).is_ok());
        assert!(claims("T", "viewer").require_role(0).is_ok());
    }
}
