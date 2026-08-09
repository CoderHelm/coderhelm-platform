//! Dashboard API for the reviewer agent: per-repo config, the reviews list +
//! detail, and rating/learning feedback. All routes are authenticated (Claims);
//! writes require admin. Storage is the settings table:
//!   config  → pk=team_id, sk=REVIEW_CONFIG#REPO#{owner}/{name}
//!   reviews → pk=team_id, sk=REVIEW#{owner}/{name}#{pr:06}#{created_at}

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Query, State};
use axum::{http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

use crate::models::Claims;
use crate::AppState;

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}
fn attr_n(val: impl std::fmt::Display) -> AttributeValue {
    AttributeValue::N(val.to_string())
}
fn attr_bool(val: bool) -> AttributeValue {
    AttributeValue::Bool(val)
}

/// Reject repo path params that could inject into a DynamoDB key.
fn validate_repo(repo: &str) -> Result<(), StatusCode> {
    if repo.is_empty()
        || repo.len() > 200
        || repo.contains("..")
        || repo.contains('\0')
        || !repo
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

fn item_bool(item: &HashMap<String, AttributeValue>, k: &str, d: bool) -> bool {
    item.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(d)
}
fn item_str(item: &HashMap<String, AttributeValue>, k: &str, d: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| d.to_string())
}
fn item_num(item: &HashMap<String, AttributeValue>, k: &str, d: u64) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(d)
}

/// GET /api/reviewer/config/:owner/:name — current reviewer config (defaults
/// applied so the UI always renders a full, safe form).
pub async fn get_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let repo = format!("{owner}/{name}");
    validate_repo(&repo)?;
    let sk = format!("REVIEW_CONFIG#REPO#{repo}");
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch reviewer config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let empty = HashMap::new();
    let item = result.item().cloned().unwrap_or(empty);
    let log_groups: Vec<String> = item
        .get("health_log_groups")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();

    Ok(Json(json!({
        "enabled": item_bool(&item, "enabled", false),
        "label": item_str(&item, "label", "ch-review"),
        "killed": item_bool(&item, "killed", false),
        "instructions": item_str(&item, "instructions", ""),
        "auto_merge": item_bool(&item, "auto_merge", false),
        "merge_method": item_str(&item, "merge_method", "squash"),
        "require_human_approval": item_bool(&item, "require_human_approval", true),
        "auto_tag": item_bool(&item, "auto_tag", false),
        "tag_prefix": item_str(&item, "tag_prefix", "v"),
        "health_check": item_bool(&item, "health_check", false),
        "health_wait_secs": item_num(&item, "health_wait_secs", 90),
        "health_log_groups": log_groups,
    })))
}

/// PUT /api/reviewer/config/:owner/:name — write the canonical config item.
/// Admin only. Values are sanitized to the safe enum/range on the way in.
pub async fn update_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path((owner, name)): axum::extract::Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<StatusCode, StatusCode> {
    claims.require_role(3)?; // admin+
    let repo = format!("{owner}/{name}");
    validate_repo(&repo)?;
    let sk = format!("REVIEW_CONFIG#REPO#{repo}");

    let instructions = body["instructions"].as_str().unwrap_or("");
    if instructions.len() > 20_000 {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let label = {
        let l = body["label"].as_str().unwrap_or("ch-review").trim();
        if l.is_empty() || l.len() > 100 {
            "ch-review".to_string()
        } else {
            l.to_string()
        }
    };
    let merge_method = match body["merge_method"].as_str().unwrap_or("squash") {
        m @ ("squash" | "merge" | "rebase") => m,
        _ => "squash",
    };
    let tag_prefix = {
        let t = body["tag_prefix"].as_str().unwrap_or("v");
        if t.len() > 50 {
            "v"
        } else {
            t
        }
    };
    let health_wait_secs = body["health_wait_secs"].as_u64().unwrap_or(90).min(300);
    let log_groups: Vec<AttributeValue> = body["health_log_groups"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .take(20)
                .map(attr_s)
                .collect()
        })
        .unwrap_or_default();

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&sk))
        .item(
            "enabled",
            attr_bool(body["enabled"].as_bool().unwrap_or(false)),
        )
        .item("label", attr_s(&label))
        .item(
            "killed",
            attr_bool(body["killed"].as_bool().unwrap_or(false)),
        )
        .item("instructions", attr_s(instructions))
        .item(
            "auto_merge",
            attr_bool(body["auto_merge"].as_bool().unwrap_or(false)),
        )
        .item("merge_method", attr_s(merge_method))
        .item(
            "require_human_approval",
            attr_bool(body["require_human_approval"].as_bool().unwrap_or(true)),
        )
        .item(
            "auto_tag",
            attr_bool(body["auto_tag"].as_bool().unwrap_or(false)),
        )
        .item("tag_prefix", attr_s(tag_prefix))
        .item(
            "health_check",
            attr_bool(body["health_check"].as_bool().unwrap_or(false)),
        )
        .item("health_wait_secs", attr_n(health_wait_secs))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()));
    put = put.item("health_log_groups", AttributeValue::L(log_groups));

    put.send().await.map_err(|e| {
        error!("Failed to update reviewer config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::OK)
}

#[derive(serde::Deserialize)]
pub struct ListReviewsQuery {
    repo: Option<String>,
    limit: Option<i32>,
}

/// Truncate to at most `max` bytes on a char boundary (String::truncate panics
/// mid-UTF8), appending an ellipsis note when it actually cut.
fn truncate_on_boundary(body: &mut String, max: usize) {
    if body.len() <= max {
        return;
    }
    let mut cut = max;
    while cut > 0 && !body.is_char_boundary(cut) {
        cut -= 1;
    }
    body.truncate(cut);
    body.push_str("\n… (truncated — open the review for the full text)");
}

fn review_item_to_json(item: &HashMap<String, AttributeValue>, truncate_body: bool) -> Value {
    let mut body = item_str(item, "body", "");
    if truncate_body {
        truncate_on_boundary(&mut body, 4000);
    }
    let comments: Vec<Value> = item
        .get("rating_comments")
        .and_then(|v| v.as_l().ok())
        .map(|l| {
            l.iter()
                .filter_map(|v| v.as_s().ok())
                .filter_map(|s| serde_json::from_str::<Value>(s).ok())
                .collect()
        })
        .unwrap_or_default();
    json!({
        "sk": item_str(item, "sk", ""),
        "repo": item_str(item, "repo", ""),
        "pr_number": item_num(item, "pr_number", 0),
        "head_sha": item_str(item, "head_sha", ""),
        "verdict": item_str(item, "verdict", ""),
        "risk": item_str(item, "risk", ""),
        "body": body,
        "posted_as": item_str(item, "posted_as", ""),
        "trigger": item_str(item, "trigger", ""),
        "action_summary": item_str(item, "action_summary", ""),
        "thumbs_up": item_num(item, "thumbs_up", 0),
        "thumbs_down": item_num(item, "thumbs_down", 0),
        "rating_comments": comments,
        "created_at": item_str(item, "created_at", ""),
    })
}

/// GET /api/reviewer/reviews?repo=&limit= — list reviews (newest first). `repo`
/// filters to one repo; omitted lists the whole team.
pub async fn list_reviews(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListReviewsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let prefix = match q.repo.as_deref() {
        Some(r) => {
            validate_repo(r)?;
            format!("REVIEW#{r}#")
        }
        None => "REVIEW#".to_string(),
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 500);

    let result = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :pfx)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":pfx", attr_s(&prefix))
        .scan_index_forward(false)
        .limit(limit)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list reviews: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut reviews: Vec<Value> = result
        .items()
        .iter()
        .map(|i| review_item_to_json(i, true))
        .collect();
    // sk sorts by (repo, pr, time); re-sort by created_at for a true recency feed.
    reviews.sort_by(|a, b| {
        b["created_at"]
            .as_str()
            .unwrap_or("")
            .cmp(a["created_at"].as_str().unwrap_or(""))
    });

    Ok(Json(json!({ "reviews": reviews })))
}

#[derive(serde::Deserialize)]
pub struct ReviewKeyQuery {
    sk: String,
}

/// GET /api/reviewer/review?sk= — one review, full body.
pub async fn get_review(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ReviewKeyQuery>,
) -> Result<Json<Value>, StatusCode> {
    if !q.sk.starts_with("REVIEW#") || q.sk.len() > 400 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&q.sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch review: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(review_item_to_json(item, false)))
}

#[derive(serde::Deserialize)]
pub struct RateBody {
    sk: String,
    /// "up" | "down" | "none" (comment-only).
    rating: Option<String>,
    comment: Option<String>,
}

/// POST /api/reviewer/review/rate — attach a 👍/👎 and/or a learning note to a
/// review. Any authenticated team member may rate. The bump + note are applied
/// atomically so the ratings feed and analytics stay consistent.
pub async fn rate_review(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RateBody>,
) -> Result<StatusCode, StatusCode> {
    if !body.sk.starts_with("REVIEW#") || body.sk.len() > 400 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rating = body.rating.as_deref().unwrap_or("none");
    let comment = body.comment.as_deref().unwrap_or("").trim();
    if comment.len() > 4000 {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    if rating != "up" && rating != "down" && comment.is_empty() {
        return Err(StatusCode::BAD_REQUEST); // nothing to do
    }

    let mut set_parts: Vec<&str> = vec![];
    let mut add_parts: Vec<&str> = vec![];
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&body.sk))
        // Only rate an existing review record.
        .condition_expression("attribute_exists(sk)");

    match rating {
        "up" => {
            add_parts.push("thumbs_up :one");
            req = req.expression_attribute_values(":one", attr_n(1));
        }
        "down" => {
            add_parts.push("thumbs_down :one");
            req = req.expression_attribute_values(":one", attr_n(1));
        }
        _ => {}
    }

    if !comment.is_empty() {
        let entry = json!({
            "by": claims.display_name(),
            "text": comment,
            "rating": rating,
            "at": chrono::Utc::now().to_rfc3339(),
        })
        .to_string();
        set_parts.push("rating_comments = list_append(if_not_exists(rating_comments, :empty), :c)");
        req = req
            .expression_attribute_values(":empty", AttributeValue::L(vec![]))
            .expression_attribute_values(":c", AttributeValue::L(vec![attr_s(&entry)]));
    }

    let mut expr = String::new();
    if !set_parts.is_empty() {
        expr.push_str("SET ");
        expr.push_str(&set_parts.join(", "));
    }
    if !add_parts.is_empty() {
        if !expr.is_empty() {
            expr.push(' ');
        }
        expr.push_str("ADD ");
        expr.push_str(&add_parts.join(", "));
    }
    req = req.update_expression(expr);

    match req.send().await {
        Ok(_) => Ok(StatusCode::OK),
        Err(e) => {
            // A missing record fails the condition check → 404, not 500.
            let msg = format!("{e:?}");
            if msg.contains("ConditionalCheckFailed") {
                Err(StatusCode::NOT_FOUND)
            } else {
                error!("Failed to rate review: {e}");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_repo_accepts_normal_and_rejects_injection() {
        assert!(validate_repo("Chelsea-Piers-Engineering/speedboat").is_ok());
        assert!(validate_repo("a_b.c/d-e").is_ok());
        assert!(validate_repo("").is_err());
        assert!(validate_repo("foo/../bar").is_err());
        assert!(validate_repo("foo\0bar").is_err());
        assert!(validate_repo("foo bar/baz").is_err()); // space
    }

    #[test]
    fn truncate_never_splits_a_multibyte_char() {
        // 2001 '★' (3 bytes each) — a naive truncate(4000) would land mid-char.
        let mut s = "★".repeat(2001);
        truncate_on_boundary(&mut s, 4000);
        // Still valid UTF-8 (no panic) and actually shortened.
        assert!(s.contains("truncated"));
        assert!(s.starts_with('★'));
    }

    #[test]
    fn truncate_leaves_short_bodies_untouched() {
        let mut s = "short review".to_string();
        truncate_on_boundary(&mut s, 4000);
        assert_eq!(s, "short review");
    }
}
