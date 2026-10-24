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
use tracing::{error, info, warn};

use crate::models::{Claims, ReleaseNotesMessage, ReviewMessage, TagSweepMessage, WorkerMessage};
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
pub(crate) fn validate_repo(repo: &str) -> Result<(), StatusCode> {
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

/// GET /api/reviewer/org-config — team-wide review instructions applied to
/// EVERY repo's review (sk REVIEW_CONFIG#GLOBAL), on top of per-repo config.
pub async fn get_org_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("REVIEW_CONFIG#GLOBAL"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .unwrap_or_default();
    let web_domains: Vec<String> = item
        .get(common::web_domains::ATTR)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    Ok(Json(json!({
        "instructions": item_str(&item, "instructions", ""),
        // The team's own public sites agents may check pages on (check_url);
        // a domain covers its subdomains.
        "web_domains": web_domains,
    })))
}

/// The `web_domains` a PUT sets: `None` when the body doesn't send them (so
/// they're kept), `Err` with the entries that aren't domains.
fn web_domains_from(body: &Value) -> Result<Option<Vec<String>>, Vec<String>> {
    let Some(raw) = body.get(common::web_domains::ATTR) else {
        return Ok(None);
    };
    let list: Vec<String> = raw
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let bad = common::web_domains::rejected(&list);
    if !bad.is_empty() {
        return Err(bad);
    }
    Ok(Some(common::web_domains::clean_all(&list)))
}

/// PUT /api/reviewer/org-config — set team-wide review instructions and the
/// team's public domains (admin). A field left out of the body is kept.
pub async fn update_org_config(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(3)?; // admin+
    let domains = match web_domains_from(&body) {
        Ok(d) => d,
        Err(bad) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!(
                        "These aren't domain names: {}. List bare domains like example.com.",
                        bad.join(", ")
                    )
                })),
            ))
        }
    };
    let mut sets = vec!["updated_at = :u"];
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s("REVIEW_CONFIG#GLOBAL"))
        .expression_attribute_values(":u", attr_s(&chrono::Utc::now().to_rfc3339()));
    if let Some(i) = body.get("instructions").and_then(|v| v.as_str()) {
        let instructions: String = i.chars().take(16_000).collect();
        sets.push("instructions = :i");
        req = req.expression_attribute_values(":i", attr_s(&instructions));
    }
    if let Some(d) = &domains {
        sets.push("web_domains = :w");
        req = req.expression_attribute_values(
            ":w",
            AttributeValue::L(d.iter().map(|s| attr_s(s)).collect()),
        );
        info!(team = %claims.team_id, domains = d.len(), by = %claims.email, "Team web domains set");
    }
    req.update_expression(format!("SET {}", sets.join(", ")))
        .send()
        .await
        .map_err(|e| {
            error!("update org review config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok((StatusCode::OK, Json(json!({ "web_domains": domains }))))
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
        "tag_mode": item_str(&item, "tag_mode", "semver"),
        "tag_prefix": item_str(&item, "tag_prefix", "v"),
        "tag_batch_minutes": item_num(&item, "tag_batch_minutes", 15),
        "health_check": item_bool(&item, "health_check", false),
        "verify_tests": item_bool(&item, "verify_tests", false),
        "require_tests": item_bool(&item, "require_tests", false),
        "auto_labels": item_bool(&item, "auto_labels", false),
        "auto_label_allow": item_str(&item, "auto_label_allow", ""),
        "auto_label_requires": item_str(&item, "auto_label_requires", ""),
        "auto_label_guide": item_str(&item, "auto_label_guide", ""),
        "release_notes": item_bool(&item, "release_notes", false),
        "release_notes_branch": item_str(&item, "release_notes_branch", ""),
        "release_notes_confluence_space": item_str(&item, "release_notes_confluence_space", ""),
        "release_notes_confluence_parent_id": item_str(&item, "release_notes_confluence_parent_id", ""),
        "release_notes_confluence_container": item_str(&item, "release_notes_confluence_container", "Changelog"),
        "release_notes_email_webhook_url": item_str(&item, "release_notes_email_webhook_url", ""),
        // Never returned; the dashboard only learns whether one is set.
        "release_notes_email_webhook_secret_set": !item_str(&item, "release_notes_email_webhook_secret", "").is_empty(),
        "release_notes_guide": item_str(&item, "release_notes_guide", ""),
        "release_notes_instructions": item_str(&item, "release_notes_instructions", ""),
        "deploy_label": item_str(&item, "deploy_label", ""),
        "health_log_groups": log_groups,
        "reminders_enabled": item_bool(&item, "reminders_enabled", false),
        // Where reminders and release notes failures post. Shown only as a
        // hint: a webhook posts as the team.
        "webhooks": common::notify::store::view(&item),
        "reminder_cooldown_hours": item_num(&item, "reminder_cooldown_hours", 4),
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
    let tag_mode = match body["tag_mode"].as_str().unwrap_or("semver") {
        m @ ("semver" | "date") => m,
        _ => "semver",
    };
    let tag_prefix = {
        let t = body["tag_prefix"].as_str().unwrap_or("v");
        if t.len() > 50 {
            "v"
        } else {
            t
        }
    };
    // Optional deploy label the reviewer adds once a PR is cleared to merge (to
    // trigger the repo's own deploy CI). Operator-set free text; trimmed and
    // capped to GitHub's 50-char label limit. Empty = no label step.
    // Comma-separated list of labels to add once a PR is cleared to merge (each
    // triggers the repo's own CI — deploy/staging/preview/E2E). Capped generously
    // to fit several labels; trimming of individual entries happens worker-side.
    let deploy_label: String = body["deploy_label"]
        .as_str()
        .unwrap_or("")
        .trim()
        .chars()
        .take(200)
        .collect();
    // Reminder channels (Teams, Slack): each kept unless the body sets or
    // clears it, and only on its channel's webhook hosts, so the worker never
    // posts to an arbitrary or internal endpoint.
    let channels: common::notify::store::ChannelsInput =
        serde_json::from_value(body.clone()).map_err(|_| StatusCode::BAD_REQUEST)?;
    channels.check().map_err(super::alerts::save_error)?;
    // Reviewer-picked CI labels: free text, bounded. The worker parses + enforces
    // (allow-list and companion rules) — nothing outside `auto_label_allow` is
    // ever added.
    let bounded = |k: &str, max: usize| -> String {
        body[k]
            .as_str()
            .unwrap_or("")
            .trim()
            .chars()
            .take(max)
            .collect()
    };
    let auto_label_allow = bounded("auto_label_allow", 1_000);
    let auto_label_requires = bounded("auto_label_requires", 2_000);
    let auto_label_guide = bounded("auto_label_guide", 5_000);
    // Release notes. The email webhook must be https (it's POSTed to by the
    // worker); the Confluence parent is a numeric page id.
    let rn_email = bounded("release_notes_email_webhook_url", 1_000);
    if !rn_email.is_empty() && !rn_email.starts_with("https://") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rn_parent = bounded("release_notes_confluence_parent_id", 30);
    if !rn_parent.chars().all(|c| c.is_ascii_digit()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rn_branch = bounded("release_notes_branch", 200);
    let rn_space = bounded("release_notes_confluence_space", 100);
    let rn_container = bounded("release_notes_confluence_container", 200);
    let rn_guide = bounded("release_notes_guide", 300);
    let rn_instructions = bounded("release_notes_instructions", 5_000);
    let reminder_cooldown_hours = body["reminder_cooldown_hours"]
        .as_u64()
        .unwrap_or(4)
        .clamp(1, 168);
    // Release-tag batch window in minutes: 0 = tag every merge immediately,
    // otherwise merges within the window share one release tag. Capped at 6h so
    // a bad value can't strand a merge undeployed for days.
    let tag_batch_minutes = body["tag_batch_minutes"].as_u64().unwrap_or(15).min(360);
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
        .item("tag_mode", attr_s(tag_mode))
        .item("tag_prefix", attr_s(tag_prefix))
        .item("tag_batch_minutes", attr_n(tag_batch_minutes))
        .item(
            "health_check",
            attr_bool(body["health_check"].as_bool().unwrap_or(false)),
        )
        .item(
            "verify_tests",
            attr_bool(body["verify_tests"].as_bool().unwrap_or(false)),
        )
        .item("deploy_label", attr_s(&deploy_label))
        .item(
            "require_tests",
            attr_bool(body["require_tests"].as_bool().unwrap_or(false)),
        )
        .item(
            "auto_labels",
            attr_bool(body["auto_labels"].as_bool().unwrap_or(false)),
        )
        .item("auto_label_allow", attr_s(&auto_label_allow))
        .item("auto_label_requires", attr_s(&auto_label_requires))
        .item("auto_label_guide", attr_s(&auto_label_guide))
        .item(
            "release_notes",
            attr_bool(body["release_notes"].as_bool().unwrap_or(false)),
        )
        .item("release_notes_branch", attr_s(&rn_branch))
        .item("release_notes_confluence_space", attr_s(&rn_space))
        .item("release_notes_confluence_parent_id", attr_s(&rn_parent))
        .item("release_notes_confluence_container", attr_s(&rn_container))
        .item("release_notes_email_webhook_url", attr_s(&rn_email))
        .item("release_notes_guide", attr_s(&rn_guide))
        .item("release_notes_instructions", attr_s(&rn_instructions))
        .item(
            "reminders_enabled",
            attr_bool(body["reminders_enabled"].as_bool().unwrap_or(false)),
        )
        .item("reminder_cooldown_hours", attr_n(reminder_cooldown_hours))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()));
    // Read the prior item BEFORE the overwrite: its write-only secret and its
    // legacy `graph_enabled` are carried over. The code graph is its OWN
    // feature with its own switch (`common::code_graph`); this page never
    // turns it on or off.
    let prior = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned());
    let legacy_graph = prior
        .as_ref()
        .and_then(|it| it.get("graph_enabled").cloned());
    // The email-webhook secret is write-only: a PUT sets it only when the body
    // carries a new value, clears it on `release_notes_email_webhook_secret_clear`,
    // and otherwise keeps the stored one (GET never returns it, so the page
    // can't send it back).
    let prior_secret = prior
        .as_ref()
        .map(|it| item_str(it, "release_notes_email_webhook_secret", ""))
        .unwrap_or_default();
    let new_secret = bounded("release_notes_email_webhook_secret", 500);
    let rn_secret = if body["release_notes_email_webhook_secret_clear"].as_bool() == Some(true) {
        String::new()
    } else if !new_secret.is_empty() {
        new_secret
    } else {
        prior_secret
    };
    put = put.item("release_notes_email_webhook_secret", attr_s(&rn_secret));
    for (k, v) in common::notify::store::save_attrs(
        &super::alerts::cipher(&state),
        &claims.team_id,
        prior.as_ref(),
        &channels,
    )
    .await
    .map_err(super::alerts::save_error)?
    {
        put = put.item(k, v);
    }
    if let Some(v) = legacy_graph {
        put = put.item("graph_enabled", v);
    }
    put = put.item("health_log_groups", AttributeValue::L(log_groups));

    put.send().await.map_err(|e| {
        error!("Failed to update reviewer config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // A repo the reviewer is set up for gets its code graph built (once),
    // unless the graph was turned off for it.
    if common::code_graph::is_enabled(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &format!("{owner}/{name}"),
    )
    .await
    {
        crate::routes::graph::enqueue_full_index(&state, &claims.team_id, &owner, &name, false)
            .await;
    }
    Ok(StatusCode::OK)
}

#[derive(serde::Deserialize)]
pub struct ListReviewsQuery {
    repo: Option<String>,
    limit: Option<i32>,
    /// `next` from the previous page.
    cursor: Option<String>,
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

/// The structured findings kept on a review record (a JSON list), or null
/// when the record has none or they can't be read.
fn review_findings(item: &HashMap<String, AttributeValue>) -> Value {
    item.get("findings")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .filter(Value::is_array)
        .unwrap_or(Value::Null)
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
        // The PR's title and the structured findings, on reviews written
        // since they were kept; older records have neither.
        "pr_title": item_str(item, "pr_title", ""),
        "findings": if truncate_body { Value::Null } else { review_findings(item) },
        // Team standards the review was given in full ("always apply").
        "applied_standards": item
            .get("applied_standards")
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect::<Vec<_>>())
            .unwrap_or_default(),
    })
}

/// GET /api/reviewer/reviews?repo=&limit=&cursor= — reviews, newest first, one
/// page at a time. `repo` filters to one repo; omitted lists the whole team.
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
    let limit = q.limit.unwrap_or(50).clamp(1, 200) as usize;
    let page = super::paging::newest_first(
        &state,
        &state.config.settings_table_name,
        &claims.team_id,
        &prefix,
        "created_at",
        q.cursor.as_deref(),
        limit,
    )
    .await?;
    let reviews: Vec<Value> = page
        .items
        .iter()
        .map(|i| review_item_to_json(i, true))
        .collect();
    Ok(Json(json!({ "reviews": reviews, "next": page.next })))
}

#[derive(serde::Deserialize)]
pub struct LiveQuery {
    repo: Option<String>,
}

/// Most PRs `GET /api/reviewer/live` returns.
const LIVE_MAX_ROWS: usize = 200;

/// `owner/repo` and the PR number from a per-PR sort key
/// (`{PREFIX}{owner}/{repo}#{pr:06}`).
fn pr_from_sk<'a>(sk: &'a str, prefix: &str) -> Option<(&'a str, u64)> {
    let (repo, pr) = sk.strip_prefix(prefix)?.rsplit_once('#')?;
    Some((repo, pr.parse().ok()?))
}

fn item_i64(item: &HashMap<String, AttributeValue>, k: &str) -> Option<i64> {
    item.get(k)?.as_n().ok()?.parse().ok()
}

/// RFC 3339 time of epoch seconds (empty for none).
fn rfc3339(secs: Option<i64>) -> String {
    secs.and_then(|s| chrono::DateTime::from_timestamp(s, 0))
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

/// Pure: one row per PR from the reviewer's live records and the auto-merge
/// gate records, each with the one status it shows
/// (`common::review_live::pr_status`). In-flight first, then waiting, then
/// done; newest first within each.
fn live_rows(
    live: &[HashMap<String, AttributeValue>],
    gates: &[HashMap<String, AttributeValue>],
    now: i64,
) -> Vec<Value> {
    use common::review_live as rl;
    type Pair<'a> = (
        Option<&'a HashMap<String, AttributeValue>>,
        Option<&'a HashMap<String, AttributeValue>>,
    );
    let mut prs: std::collections::BTreeMap<(String, u64), Pair> = Default::default();
    // DynamoDB deletes expired items late; an expired one is gone already.
    let current =
        |it: &&HashMap<String, AttributeValue>| item_i64(it, "ttl").is_none_or(|t| t >= now);
    let (live, gates): (Vec<_>, Vec<_>) = (
        live.iter().filter(current).collect(),
        gates.iter().filter(current).collect(),
    );
    for it in live {
        if let Some((repo, pr)) = pr_from_sk(&item_str(it, "sk", ""), rl::SK_PREFIX) {
            prs.entry((repo.to_string(), pr)).or_default().0 = Some(it);
        }
    }
    for it in gates {
        if let Some((repo, pr)) = pr_from_sk(&item_str(it, "sk", ""), common::merge_gate::SK_PREFIX)
        {
            prs.entry((repo.to_string(), pr)).or_default().1 = Some(it);
        }
    }
    let mut rows: Vec<(u8, String, Value)> = prs
        .into_iter()
        .map(|((repo, pr), (l, g))| {
            let phase = l
                .map(|l| {
                    rl::effective_phase(&item_str(l, "phase", ""), item_i64(l, "expires_at"), now)
                        .to_string()
                })
                .unwrap_or_default();
            let gate_state = g.map(|g| item_str(g, "state", "")).unwrap_or_default();
            let st = rl::pr_status(&phase, &gate_state);
            let review = l.map(|l| {
                json!({
                    "phase": phase,
                    "kind": item_str(l, "kind", "review"),
                    "trigger": item_str(l, "trigger", ""),
                    "head_sha": item_str(l, "head_sha", ""),
                    "attempt": item_num(l, "attempt", 0),
                    "reason": item_str(l, "reason", ""),
                    "queued_at": rfc3339(item_i64(l, "queued_at")),
                    "started_at": rfc3339(item_i64(l, "started_at")),
                    "finished_at": rfc3339(item_i64(l, "finished_at")),
                })
            });
            let gate = g.map(|g| {
                let checklist: Vec<Value> =
                    common::merge_gate::checklist(&item_str(g, "status_body", ""))
                        .into_iter()
                        .map(|c| json!({ "mark": c.mark, "text": c.text }))
                        .collect();
                json!({
                    "state": gate_state,
                    "detail": item_str(g, "detail", ""),
                    "head_sha": item_str(g, "head_sha", ""),
                    "armed_at": rfc3339(item_i64(g, "armed_at")),
                    "checklist": checklist,
                    "stopped": stopped_json(g),
                })
            });
            // RFC 3339 in UTC from one writer each, so strings order by time.
            let updated_at = [
                l.map(|l| item_str(l, "updated_at", "")),
                g.map(|g| item_str(g, "updated_at", "")),
                g.map(|g| rfc3339(item_i64(g, "armed_at"))),
            ]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or_default();
            let rank = match st.group {
                rl::GROUP_ACTIVE => 0,
                rl::GROUP_WAITING => 1,
                _ => 2,
            };
            let row = json!({
                "repo": repo,
                "pr_number": pr,
                "pr_title": l.map(|l| item_str(l, "pr_title", "")).unwrap_or_default(),
                "status": st.status,
                "group": st.group,
                "updated_at": updated_at,
                "review": review,
                "gate": gate,
            });
            (rank, updated_at, row)
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
    rows.into_iter()
        .take(LIVE_MAX_ROWS)
        .map(|(_, _, row)| row)
        .collect()
}

/// GET /api/reviewer/live?repo= — where the reviewer is with each PR it
/// handles: queued, being reviewed, retrying, waiting for CI or people
/// (auto-merge), merged, closed, or reviewed. One row per PR, from the
/// review status records and the auto-merge gate records (both expire on
/// their own).
pub async fn live_prs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<LiveQuery>,
) -> Result<Json<Value>, StatusCode> {
    let suffix = match q.repo.as_deref().filter(|r| !r.is_empty()) {
        Some(r) => {
            validate_repo(r)?;
            format!("{r}#")
        }
        None => String::new(),
    };
    let live = items_under(
        &state,
        &claims.team_id,
        &format!("{}{suffix}", common::review_live::SK_PREFIX),
    )
    .await?;
    let gates = items_under(
        &state,
        &claims.team_id,
        &format!("{}{suffix}", common::merge_gate::SK_PREFIX),
    )
    .await?;
    let now = chrono::Utc::now().timestamp();
    Ok(Json(json!({ "prs": live_rows(&live, &gates, now) })))
}

/// A person's stop on a gate record, for the dashboard (null when none).
fn stopped_json(g: &HashMap<String, AttributeValue>) -> Value {
    match common::merge_gate::stopped(g) {
        Some(st) => json!({
            "by": st.by,
            "is_login": st.is_login,
            "via": st.via,
            "at": rfc3339(Some(st.at)),
        }),
        None => Value::Null,
    }
}

#[derive(serde::Deserialize)]
pub struct GateQuery {
    /// "owner/name"
    repo: String,
    pr: u64,
}

/// `owner` and `name` of a validated "owner/name".
fn split_repo(repo: &str) -> Result<(&str, &str), StatusCode> {
    validate_repo(repo)?;
    let (owner, name) = repo.split_once('/').ok_or(StatusCode::BAD_REQUEST)?;
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok((owner, name))
}

/// The team's reviewer config of one repo, if the team has one. Keyed by the
/// caller's own team: another team's repo is simply not there.
async fn team_repo_config(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<Option<HashMap<String, AttributeValue>>, StatusCode> {
    Ok(state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("REVIEW_CONFIG#REPO#{owner}/{name}")))
        .send()
        .await
        .map_err(|e| {
            error!(owner, name, error = %e, "Reviewer config read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned())
}

/// Pure: may this dashboard user stop or re-arm auto-merge on the repo? Only
/// a member (or above; viewers only read) of the team whose config holds the
/// repo. Another team's repo is "not found", never a 403 that confirms it
/// exists.
pub(crate) fn authorize_gate_control(
    role_level: u8,
    team_has_repo: bool,
) -> Result<(), StatusCode> {
    if !team_has_repo {
        return Err(StatusCode::NOT_FOUND);
    }
    if role_level < 1 {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

/// Pure: one PR's auto-merge gate for the dashboard's PR page: its state,
/// checklist, who stopped it, and every stop and re-arm (newest first).
fn gate_detail(
    repo: &str,
    pr: u64,
    auto_merge: bool,
    gate: Option<&HashMap<String, AttributeValue>>,
    live: Option<&HashMap<String, AttributeValue>>,
    now: i64,
) -> Value {
    use common::review_live as rl;
    let phase = live
        .map(|l| {
            rl::effective_phase(&item_str(l, "phase", ""), item_i64(l, "expires_at"), now)
                .to_string()
        })
        .unwrap_or_default();
    let gate_state = gate.map(|g| item_str(g, "state", "")).unwrap_or_default();
    let st = rl::pr_status(&phase, &gate_state);
    let gate = gate.map(|g| {
        let checklist: Vec<Value> = common::merge_gate::checklist(&item_str(g, "status_body", ""))
            .into_iter()
            .map(|c| json!({ "mark": c.mark, "text": c.text }))
            .collect();
        let mut controls: Vec<Value> = common::merge_gate::controls(g)
            .into_iter()
            .map(|c| {
                json!({
                    "action": c.action,
                    "by": c.by,
                    "is_login": c.is_login,
                    "via": c.via,
                    "at": rfc3339(Some(c.at)),
                    "outcome": c.outcome,
                })
            })
            .collect();
        controls.reverse();
        json!({
            "state": gate_state,
            "detail": item_str(g, "detail", ""),
            "head_sha": item_str(g, "head_sha", ""),
            "armed_at": rfc3339(item_i64(g, "armed_at")),
            "updated_at": item_str(g, "updated_at", ""),
            "checklist": checklist,
            "stopped": stopped_json(g),
            "controls": controls,
        })
    });
    json!({
        "repo": repo,
        "pr_number": pr,
        "pr_title": live.map(|l| item_str(l, "pr_title", "")).unwrap_or_default(),
        "auto_merge": auto_merge,
        "status": st.status,
        "group": st.group,
        "gate": gate,
    })
}

/// GET /api/reviewer/gate?repo=owner/name&pr=N — one PR's auto-merge gate:
/// state, checklist, the stop if any and the audit log of stops and re-arms.
pub async fn get_gate(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<GateQuery>,
) -> Result<Json<Value>, StatusCode> {
    let (owner, name) = split_repo(&q.repo)?;
    if q.pr == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let cfg = team_repo_config(&state, &claims.team_id, owner, name)
        .await?
        .ok_or(StatusCode::NOT_FOUND)?;
    let get = |sk: String| {
        let state = state.clone();
        let team = claims.team_id.clone();
        async move {
            state
                .dynamo
                .get_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", attr_s(&team))
                .key("sk", attr_s(&sk))
                .consistent_read(true)
                .send()
                .await
                .map(|r| r.item().cloned())
                .map_err(|e| {
                    error!(error = %e, "Gate read failed");
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        }
    };
    let gate = get(common::merge_gate::record_sk(owner, name, q.pr)).await?;
    let live = get(common::review_live::record_sk(owner, name, q.pr)).await?;
    let now = chrono::Utc::now().timestamp();
    let mut out = gate_detail(
        &q.repo,
        q.pr,
        item_bool(&cfg, "auto_merge", false),
        gate.as_ref(),
        live.as_ref(),
        now,
    );
    // From what the person holds, not the role this read runs as.
    out["can_control"] = json!(claims.holder().holds("reviews"));
    Ok(Json(out))
}

#[derive(serde::Deserialize)]
pub struct GateControlBody {
    /// "owner/name"
    repo: String,
    pr: u64,
    /// "stop" or "rearm".
    action: String,
}

/// POST /api/reviewer/gate — stop or re-arm one PR's auto-merge from the
/// dashboard. Member or above of the team that owns the repo. A re-arm
/// never merges by itself: the gate checks every condition again.
pub async fn control_gate(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<GateControlBody>,
) -> Result<Json<Value>, StatusCode> {
    let (owner, name) = split_repo(&body.repo)?;
    let control =
        common::merge_gate::Control::from_action(&body.action).ok_or(StatusCode::BAD_REQUEST)?;
    if body.pr == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let cfg = team_repo_config(&state, &claims.team_id, owner, name).await?;
    authorize_gate_control(claims.role_level(), cfg.is_some())?;
    let installation_id = super::api::get_team_installation_id(&state, &claims.team_id).await?;
    let actor = common::merge_gate::Actor {
        by: claims
            .github_login
            .clone()
            .unwrap_or_else(|| claims.email.clone()),
        is_login: claims.github_login.is_some(),
        via: common::merge_gate::VIA_DASHBOARD,
    };
    let out = super::merge_control::apply(
        &state,
        &claims.team_id,
        installation_id,
        owner,
        name,
        body.pr,
        control,
        &actor,
    )
    .await?;
    if out.status == "not_found" {
        return Err(StatusCode::NOT_FOUND);
    }
    info!(
        owner,
        name,
        pr = body.pr,
        action = control.as_str(),
        status = out.status,
        "Reviewer: dashboard auto-merge control"
    );
    Ok(Json(json!({
        "status": out.status,
        "ok": out.ok,
        "message": out.message,
    })))
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
    claims.require_role(1)?; // member+ (viewers are read-only)
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
    req = req
        .update_expression(expr)
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew);

    match req.send().await {
        Ok(out) => {
            // A rating is a reward signal: one per person, review and
            // direction, with the reason they gave.
            let kind = match rating {
                "up" => Some(common::rewards::RewardKind::RatingUp),
                "down" => Some(common::rewards::RewardKind::RatingDown),
                _ => None,
            };
            if let (Some(kind), Some(review)) = (kind, out.attributes()) {
                let by = claims.display_name();
                let ev = common::rewards::mapping::review_event(
                    &claims.team_id,
                    kind,
                    &body.sk,
                    review,
                    &chrono::Utc::now().to_rfc3339(),
                    &format!("rating#{}#{by}#{rating}", body.sk),
                )
                .with_detail(if comment.is_empty() {
                    "Rated on the dashboard"
                } else {
                    comment
                })
                .with_source("dashboard:rate");
                let store = common::rewards::store::Store::new(
                    &state.dynamo,
                    &state.config.settings_table_name,
                );
                if let Err(e) = store.record(&ev).await {
                    warn!(error = %e, "Could not record the rating as a reward signal");
                }
            }
            // Keep the analytics copy in step. Older reviews may have none yet
            // (the backfill copies their ratings), so a miss is fine.
            let field = match rating {
                "up" => Some("thumbs_up"),
                "down" => Some("thumbs_down"),
                _ => None,
            };
            if let (Some(field), Some(stat_sk)) = (
                field,
                common::review_stats::stat_sk_from_review_sk(&body.sk),
            ) {
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.settings_table_name)
                    .key("pk", attr_s(&claims.team_id))
                    .key("sk", attr_s(&stat_sk))
                    .condition_expression("attribute_exists(sk)")
                    .update_expression(format!("ADD {field} :one"))
                    .expression_attribute_values(":one", attr_n(1))
                    .send()
                    .await;
            }
            Ok(StatusCode::OK)
        }
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
    fn gate_control_is_refused_to_other_teams_and_viewers() {
        // Another team's repo has no config under the caller's team.
        assert_eq!(authorize_gate_control(4, false), Err(StatusCode::NOT_FOUND));
        assert_eq!(authorize_gate_control(0, false), Err(StatusCode::NOT_FOUND));
        // A viewer of the owning team reads only.
        assert_eq!(authorize_gate_control(0, true), Err(StatusCode::FORBIDDEN));
        assert_eq!(authorize_gate_control(1, true), Ok(()));
        assert_eq!(authorize_gate_control(3, true), Ok(()));
    }

    #[test]
    fn gate_detail_shows_the_stop_and_the_audit_log_newest_first() {
        let now = 1_000_000;
        let entry = |action: &str, by: &str, via: &str, at: i64| {
            attr_s(
                &serde_json::to_string(&common::merge_gate::ControlEntry {
                    action: action.into(),
                    by: by.into(),
                    is_login: true,
                    via: via.into(),
                    at,
                    outcome: String::new(),
                })
                .unwrap(),
            )
        };
        let gate = item(&[
            ("sk", attr_s("MERGEGATE#your-team/web#000007")),
            ("state", attr_s("stopped")),
            ("stopped_by", attr_s("a-reviewer")),
            ("stopped_login", AttributeValue::Bool(true)),
            ("stopped_via", attr_s("comment")),
            ("stopped_at", attr_n(now - 60)),
            (
                "controls",
                AttributeValue::L(vec![
                    entry("stop", "a-maintainer", "dashboard", now - 600),
                    entry("rearm", "a-maintainer", "dashboard", now - 300),
                    entry("stop", "a-reviewer", "comment", now - 60),
                ]),
            ),
            ("status_body", attr_s("x\n- ✅ CoderHelm approved\n")),
        ]);
        let d = gate_detail("your-team/web", 7, true, Some(&gate), None, now);
        assert_eq!(d["status"], "stopped");
        assert_eq!(d["group"], "waiting");
        assert_eq!(d["auto_merge"], true);
        assert_eq!(d["gate"]["stopped"]["by"], "a-reviewer");
        assert_eq!(d["gate"]["stopped"]["via"], "comment");
        let log = d["gate"]["controls"].as_array().unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0]["by"], "a-reviewer");
        assert_eq!(log[1]["action"], "rearm");
        assert_eq!(d["gate"]["checklist"][0]["text"], "CoderHelm approved");
        // No gate yet: still a page, nothing stopped.
        let d = gate_detail("your-team/web", 8, true, None, None, now);
        assert_eq!(d["gate"], Value::Null);
    }

    #[test]
    fn a_review_shows_its_pr_title_and_structured_findings() {
        let mut item: HashMap<String, AttributeValue> = HashMap::from([
            (
                "sk".into(),
                attr_s("REVIEW#example-org/web#000007#2026-10-01T00:00:00Z"),
            ),
            ("repo".into(), attr_s("example-org/web")),
            ("pr_title".into(), attr_s("Add login page")),
            (
                "findings".into(),
                attr_s(
                    r#"[{"severity":"high","kind":"defect","blocks":true,"file":"a.ts","line":3,"title":"T","body":"B"}]"#,
                ),
            ),
        ]);
        let full = review_item_to_json(&item, false);
        assert_eq!(full["pr_title"], "Add login page");
        assert_eq!(full["findings"][0]["file"], "a.ts");
        assert_eq!(full["findings"][0]["blocks"], true);
        // The list page leaves them out.
        assert!(review_item_to_json(&item, true)["findings"].is_null());
        // Older records, and unreadable ones, have none.
        item.insert("findings".into(), attr_s("not json"));
        assert!(review_item_to_json(&item, false)["findings"].is_null());
        item.remove("findings");
        item.remove("pr_title");
        let old = review_item_to_json(&item, false);
        assert!(old["findings"].is_null());
        assert_eq!(old["pr_title"], "");
    }

    #[test]
    fn open_pr_rows_keep_what_the_picker_shows() {
        let gh = json!([
            {"number": 42, "title": "Fix login", "user": {"login": "dev1"}, "draft": false, "updated_at": "2026-10-20T10:00:00Z", "body": "long"},
            {"number": 7, "title": "WIP", "user": null, "draft": true},
            {"title": "not a pr"}
        ]);
        let rows = open_pr_rows(&gh);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            json!({"number": 42, "title": "Fix login", "author": "dev1", "draft": false, "updated_at": "2026-10-20T10:00:00Z"})
        );
        assert_eq!(rows[1]["author"], "");
        assert_eq!(rows[1]["draft"], true);
        // An error object from GitHub is no rows, not a panic.
        assert!(open_pr_rows(&json!({"message": "Not Found"})).is_empty());
    }

    #[test]
    fn validate_repo_accepts_normal_and_rejects_injection() {
        assert!(validate_repo("Example-Org/web-app").is_ok());
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

    fn item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn live_rows_join_review_status_and_gate_per_pr() {
        let now = 1_000_000;
        let live = vec![
            // Being reviewed now, and auto-merge was waiting on CI before.
            item(&[
                ("sk", attr_s("REVIEWLIVE#acme/web#000012")),
                ("phase", attr_s("reviewing")),
                ("expires_at", attr_n(now + 600)),
                ("pr_title", attr_s("Add login page")),
                ("updated_at", attr_s("2026-01-01T10:00:00+00:00")),
            ]),
            // A crashed review: past its expiry, it shows as stalled.
            item(&[
                ("sk", attr_s("REVIEWLIVE#acme/api#000003")),
                ("phase", attr_s("reviewing")),
                ("expires_at", attr_n(now - 1)),
                ("updated_at", attr_s("2026-01-01T09:00:00+00:00")),
            ]),
            // Finished long ago; its record expired but wasn't deleted yet.
            item(&[
                ("sk", attr_s("REVIEWLIVE#acme/api#000001")),
                ("phase", attr_s("done")),
                ("ttl", attr_n(now - 10)),
            ]),
        ];
        let body = "x\n- ✅ CoderHelm approved\n- ⏳ Waiting for a person's approval\n";
        let gates = vec![
            item(&[
                ("sk", attr_s("MERGEGATE#acme/web#000012")),
                ("state", attr_s("waiting_ci")),
            ]),
            item(&[
                ("sk", attr_s("MERGEGATE#acme/web#000009")),
                ("state", attr_s("waiting_approval")),
                ("status_body", attr_s(body)),
                ("updated_at", attr_s("2026-01-01T08:00:00+00:00")),
            ]),
            item(&[
                ("sk", attr_s("MERGEGATE#acme/web#000007")),
                ("state", attr_s("merged")),
                ("updated_at", attr_s("2026-01-01T11:00:00+00:00")),
            ]),
        ];
        let rows = live_rows(&live, &gates, now);
        let summary: Vec<(String, u64, String, String)> = rows
            .iter()
            .map(|r| {
                (
                    r["repo"].as_str().unwrap().to_string(),
                    r["pr_number"].as_u64().unwrap(),
                    r["status"].as_str().unwrap().to_string(),
                    r["group"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let row = |repo: &str, pr: u64, status: &str, group: &str| {
            (repo.to_string(), pr, status.to_string(), group.to_string())
        };
        assert_eq!(
            summary,
            vec![
                row("acme/web", 12, "reviewing", "active"),
                // Waiting: the stalled review (newer) before the gate on people.
                row("acme/api", 3, "stalled", "waiting"),
                row("acme/web", 9, "waiting_people", "waiting"),
                row("acme/web", 7, "merged", "done"),
            ]
        );
        assert_eq!(rows[0]["pr_title"], "Add login page");
        assert_eq!(rows[0]["gate"]["state"], "waiting_ci");
        assert_eq!(rows[2]["review"], Value::Null);
        assert_eq!(rows[2]["gate"]["checklist"][1]["mark"], "waiting");
        assert_eq!(
            rows[2]["gate"]["checklist"][1]["text"],
            "Waiting for a person's approval"
        );
    }

    #[test]
    fn pr_keys_parse_or_are_skipped() {
        assert_eq!(
            pr_from_sk("MERGEGATE#acme/web#000042", "MERGEGATE#"),
            Some(("acme/web", 42))
        );
        assert_eq!(pr_from_sk("MERGEGATE#acme/web", "MERGEGATE#"), None);
        assert_eq!(pr_from_sk("REVIEW#acme/web#000042", "MERGEGATE#"), None);
    }

    #[test]
    fn truncate_leaves_short_bodies_untouched() {
        let mut s = "short review".to_string();
        truncate_on_boundary(&mut s, 4000);
        assert_eq!(s, "short review");
    }
}

#[derive(serde::Deserialize)]
pub struct ReReviewBody {
    /// "owner/name"
    pub repo: String,
    pub pr: u64,
}

/// Double-click window: one dashboard click ⇒ one review job, even if the
/// button is pressed again (or the request retried) within this many seconds.
const RE_REVIEW_DEDUP_SECS: i64 = 30;

/// POST /api/reviewer/re-review — review a PR's CURRENT head now, from the
/// dashboard (no GitHub label/comment needed). Same job the GitHub
/// "@coderhelm re-review" path enqueues: trigger "rerequest" (explicit ask →
/// the worker bypasses the label/draft gates), empty head_sha (worker resolves
/// the live head), and a dedup key so a double click can't double-review.
pub async fn re_review(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ReReviewBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+
    validate_repo(&body.repo)?;
    let (owner, name) = body.repo.split_once('/').ok_or(StatusCode::BAD_REQUEST)?;
    if owner.is_empty() || name.is_empty() || name.contains('/') || body.pr == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let cfg =
        super::github_webhook::load_review_config(&state, &claims.team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed {
        return Ok(Json(json!({
            "status": "skipped",
            "reason": "Review is off for this repo — enable it in Reviewer settings."
        })));
    }
    if let Some(reason) = super::github_webhook::check_run_budget(&state, &claims.team_id).await {
        return Ok(Json(json!({ "status": "skipped", "reason": reason })));
    }
    let installation_id = super::api::get_team_installation_id(&state, &claims.team_id).await?;

    let window = chrono::Utc::now().timestamp() / RE_REVIEW_DEDUP_SECS;
    let message = WorkerMessage::Review(ReviewMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: body.pr,
        head_sha: String::new(),
        label: cfg.label,
        question: None,
        trigger: "rerequest".to_string(),
        dedup_key: format!("dashboard#pr{}#{window}", body.pr),
        reply_to_comment_id: None,
        requested_by: String::new(),
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &message).await?;
    super::github_webhook::record_rereview(
        &state,
        &claims.team_id,
        owner,
        name,
        body.pr,
        &format!("dashboard#{window}"),
    )
    .await;
    info!(
        owner,
        name,
        pr = body.pr,
        "Reviewer: dashboard re-review → review job"
    );
    Ok(Json(json!({ "status": "queued" })))
}

#[derive(serde::Deserialize)]
pub struct OpenPrsQuery {
    repo: String,
}

/// Most open PRs `GET /api/reviewer/open-prs` returns (one GitHub page).
const OPEN_PRS_MAX: usize = 100;

/// Pure: the dashboard's rows from GitHub's `GET /repos/{o}/{n}/pulls`
/// answer. Number, title, author, draft flag and last update; anything that
/// isn't a PR object is skipped.
fn open_pr_rows(pulls: &Value) -> Vec<Value> {
    pulls
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|p| {
            let number = p.get("number")?.as_u64()?;
            Some(json!({
                "number": number,
                "title": p.get("title").and_then(Value::as_str).unwrap_or(""),
                "author": p.pointer("/user/login").and_then(Value::as_str).unwrap_or(""),
                "draft": p.get("draft").and_then(Value::as_bool).unwrap_or(false),
                "updated_at": p.get("updated_at").and_then(Value::as_str).unwrap_or(""),
            }))
        })
        .take(OPEN_PRS_MAX)
        .collect()
}

/// GET /api/reviewer/open-prs?repo=owner/name — the repo's open pull
/// requests, most recently updated first, read with the team's GitHub app
/// installation. Feeds the "Review a PR" picker so nobody types a number.
/// A repo the installation can't see answers 404.
pub async fn open_prs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<OpenPrsQuery>,
) -> Result<Json<Value>, StatusCode> {
    validate_repo(&q.repo)?;
    let (owner, name) = q.repo.split_once('/').ok_or(StatusCode::BAD_REQUEST)?;
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return Err(StatusCode::BAD_REQUEST);
    }
    let installation_id = super::api::get_team_installation_id(&state, &claims.team_id).await?;
    let token = crate::auth::github_app::get_installation_token(&state, installation_id)
        .await
        .map_err(|e| {
            error!("open-prs: installation token failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;
    let url = format!(
        "https://api.github.com/repos/{owner}/{name}/pulls?state=open&sort=updated&direction=desc&per_page={OPEN_PRS_MAX}"
    );
    let resp = state
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-bot")
        .send()
        .await
        .map_err(|e| {
            error!("open-prs: GitHub request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;
    match resp.status().as_u16() {
        200 => {}
        404 | 403 => return Err(StatusCode::NOT_FOUND),
        s => {
            error!(owner, name, status = s, "open-prs: GitHub refused");
            return Err(StatusCode::BAD_GATEWAY);
        }
    }
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    Ok(Json(json!({ "prs": open_pr_rows(&body) })))
}

/// GET /api/releases?repo=owner/name&cursor= — release-notes records, newest
/// first, one page at a time.
pub async fn list_releases(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let prefix = match params.get("repo").filter(|r| !r.is_empty()) {
        Some(repo) => {
            validate_repo(repo)?;
            format!("RELEASE#{repo}#")
        }
        None => "RELEASE#".to_string(),
    };
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(25)
        .clamp(1, 100);
    let page = super::paging::newest_first(
        &state,
        &state.config.settings_table_name,
        &claims.team_id,
        &prefix,
        "updated_at",
        params.get("cursor").map(String::as_str),
        limit,
    )
    .await?;
    let releases: Vec<Value> = page
        .items
        .iter()
        .map(|item| {
            let sk = item_str(item, "sk", "");
            // RELEASE#{owner}/{name}#{tag}
            let rest = sk.trim_start_matches("RELEASE#");
            let (repo, tag) = rest.rsplit_once('#').unwrap_or((rest, ""));
            json!({
                "repo": repo,
                "tag": tag,
                "prev_tag": item_str(item, "prev_tag", ""),
                "pr_count": item_str(item, "pr_count", "0").parse::<u64>().unwrap_or(0),
                "headline": item_str(item, "headline", ""),
                "entry_md": item_str(item, "entry_md", ""),
                "github_release_md": item_str(item, "github_release_md", ""),
                "unclear": item_str(item, "unclear", ""),
                "github_release_url": item_str(item, "github_release_url", ""),
                "confluence_url": item_str(item, "confluence_url", ""),
                "email_sent_at": item_str(item, "email_sent_at", ""),
                "updated_at": item_str(item, "updated_at", ""),
                "last_error": item_str(item, "last_error", ""),
                "failed_at": item_str(item, "failed_at", ""),
            })
        })
        .collect();
    Ok(Json(json!({ "releases": releases, "next": page.next })))
}

#[derive(serde::Deserialize)]
pub struct ReleaseSendBody {
    /// "owner/name"
    pub repo: String,
    pub tag: String,
    /// "" = write + publish whatever is missing (also: generate for an existing
    /// tag); "email" = send the email again; "all" = rewrite and re-publish.
    #[serde(default)]
    pub resend: String,
}

/// POST /api/releases/send — write/publish release notes for a tag from the
/// dashboard (first time for an older tag, or again). Member+.
pub async fn send_release(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ReleaseSendBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?;
    validate_repo(&body.repo)?;
    let (owner, name) = body.repo.split_once('/').ok_or(StatusCode::BAD_REQUEST)?;
    let tag = body.tag.trim();
    if tag.is_empty()
        || tag.len() > 200
        || !tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+' | '/'))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let resend = match body.resend.as_str() {
        r @ ("" | "email" | "all") => r,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let installation_id = super::api::get_team_installation_id(&state, &claims.team_id).await?;
    let message = WorkerMessage::ReleaseNotes(ReleaseNotesMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        tag: tag.to_string(),
        // The tag resolves as a ref everywhere the worker needs a commit.
        sha: tag.to_string(),
        resend: resend.to_string(),
        // Asked for explicitly from the dashboard: no branch check.
        check_branch: false,
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &message).await?;
    info!(
        owner,
        name, tag, resend, "Release notes: queued from dashboard"
    );
    Ok(Json(json!({ "status": "queued" })))
}

/// All of a team's items under `prefix` (small sets: one per repo).
async fn items_under(
    state: &AppState,
    team_id: &str,
    prefix: &str,
) -> Result<Vec<HashMap<String, AttributeValue>>, StatusCode> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(team_id))
            .expression_attribute_values(":p", attr_s(prefix))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| {
                error!(error = %e, prefix, "Settings query failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        out.extend(page.items.unwrap_or_default());
        start = page.last_evaluated_key;
        if start.is_none() {
            return Ok(out);
        }
    }
}

/// GET /api/releases/pending?repo= — releases waiting out the batch window
/// (when each fires, how many merges it holds) and releases that couldn't be
/// cut after their retries (the error), per repo.
pub async fn pending_releases(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let repo = params.get("repo").filter(|r| !r.is_empty());
    if let Some(r) = repo {
        validate_repo(r)?;
    }
    let suffix = repo.map(|r| r.as_str()).unwrap_or("");
    let now = chrono::Utc::now().timestamp() as u64;
    let waiting: Vec<Value> = items_under(&state, &claims.team_id, &format!("TAGSWEEP#{suffix}"))
        .await?
        .iter()
        // A marker past its window by an hour is a sweep that died; its TTL
        // removes it.
        .filter(|i| item_num(i, "fires_at", 0) + 3_600 > now)
        .map(|i| {
            json!({
                "repo": item_str(i, "sk", "").trim_start_matches("TAGSWEEP#"),
                "fires_at": item_num(i, "fires_at", 0),
                "merges": item_num(i, "merges", 1),
                "base_branch": item_str(i, "base_branch", ""),
            })
        })
        .collect();
    let blocked: Vec<Value> = items_under(&state, &claims.team_id, &format!("TAGFAIL#{suffix}"))
        .await?
        .iter()
        .map(|i| {
            json!({
                "repo": item_str(i, "sk", "").trim_start_matches("TAGFAIL#"),
                "error": item_str(i, "error", ""),
                "failed_at": item_str(i, "failed_at", ""),
                "attempts": item_num(i, "attempts", 1),
                "base_branch": item_str(i, "base_branch", ""),
            })
        })
        .collect();
    Ok(Json(json!({ "waiting": waiting, "blocked": blocked })))
}

#[derive(serde::Deserialize)]
pub struct ReleaseNowBody {
    /// "owner/name"
    pub repo: String,
}

/// POST /api/releases/now — cut the repo's release tag now instead of at the
/// end of the batch window, or retry one that was blocked. Member+.
///
/// With a release waiting, the sweep carries that window's id: whichever of
/// this and the delayed sweep claims the window first releases it, the other
/// stops. With nothing waiting it tags the default branch's HEAD (no-op when
/// that commit is already tagged).
pub async fn release_now(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<ReleaseNowBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?;
    validate_repo(&body.repo)?;
    let (owner, name) = body.repo.split_once('/').ok_or(StatusCode::BAD_REQUEST)?;
    if owner.is_empty() || name.is_empty() || name.contains('/') {
        return Err(StatusCode::BAD_REQUEST);
    }
    let installation_id = super::api::get_team_installation_id(&state, &claims.team_id).await?;
    let marker = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TAGSWEEP#{owner}/{name}")))
        .send()
        .await
        .map_err(|e| {
            error!(error = %e, "Release now: marker read failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item;
    let blocked = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("TAGFAIL#{owner}/{name}")))
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let from = marker.as_ref().or(blocked.as_ref());
    let sweep_id = marker
        .as_ref()
        .map(|m| item_str(m, "sweep_id", ""))
        .unwrap_or_default();
    let message = WorkerMessage::TagSweep(TagSweepMessage {
        team_id: claims.team_id.clone(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        base_branch: from
            .map(|m| item_str(m, "base_branch", ""))
            .unwrap_or_default(),
        pr_number: from.map(|m| item_num(m, "pr_number", 0)).unwrap_or(0),
        delay_remaining_secs: 0,
        // A marker from before ids existed can't be claimed safely by id:
        // treat it as nothing waiting (the delayed sweep then finds HEAD
        // tagged and cuts nothing).
        claimed: sweep_id.is_empty(),
        sweep_id,
        manual: true,
        attempt: 0,
    });
    super::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &message).await?;
    info!(owner, name, "Release now: queued from dashboard");
    Ok(Json(json!({ "status": "queued" })))
}

#[cfg(test)]
mod web_domains_tests {
    use super::web_domains_from;
    use serde_json::json;

    #[test]
    fn domains_are_kept_when_left_out_and_cleaned_when_sent() {
        assert_eq!(web_domains_from(&json!({"instructions": "x"})), Ok(None));
        assert_eq!(
            web_domains_from(&json!({"web_domains": ["https://WWW.Example.com/", "example.org"]})),
            Ok(Some(vec!["www.example.com".into(), "example.org".into()]))
        );
        assert_eq!(
            web_domains_from(&json!({"web_domains": []})),
            Ok(Some(vec![]))
        );
        assert_eq!(
            web_domains_from(
                &json!({"web_domains": ["example.com", "169.254.169.254", "intranet"]})
            ),
            Err(vec!["169.254.169.254".into(), "intranet".into()])
        );
    }
}
