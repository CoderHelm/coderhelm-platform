use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::models::Claims;
use crate::AppState;

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

// ────────────────────────────────────────────────────────────────
// AWS Connections
// ────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct CreateConnectionRequest {
    role_arn: String,
    region: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct UpdateConnectionRequest {
    role_arn: Option<String>,
    region: Option<String>,
    log_groups: Option<Vec<String>>,
}

/// Validate that role_arn matches expected format: arn:aws:iam::<12-digit>:role/CoderHelmLogReader
fn validate_role_arn(arn: &str) -> Result<&str, StatusCode> {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^arn:aws:iam::\d{12}:role/CoderHelmLogReader$").unwrap()
    });
    if RE.is_match(arn) {
        Ok(arn)
    } else {
        warn!(
            role_arn = arn,
            "Invalid role ARN — must be CoderHelmLogReader"
        );
        Err(StatusCode::BAD_REQUEST)
    }
}

/// Pure: an AWS region name such as `us-east-1` or `ap-southeast-2`.
pub fn valid_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    region.len() <= 25
        && parts.len() >= 3
        && parts[0].len() == 2
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
        && parts
            .last()
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
}

/// Pure: a CloudWatch log group name (AWS allows `[.\-_/#A-Za-z0-9]`, 1–512).
pub fn valid_log_group(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 512
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/' | '#'))
}

/// The team's External ID for the CoderHelmLogReader role trust policy:
/// created once per team and stored, so the console stack, a manually made
/// role and a page reload all agree on it, and a client can't pick it.
async fn team_external_id(state: &AppState, team_id: &str) -> Result<String, StatusCode> {
    let table = &state.config.aws_insights_table_name;
    let read = || async {
        state
            .dynamo
            .get_item()
            .table_name(table)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s("AWS_EXTERNAL_ID"))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to read external id: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })
            .map(|o| {
                o.item()
                    .and_then(|i| i.get("external_id"))
                    .and_then(|v| v.as_s().ok())
                    .cloned()
            })
    };
    if let Some(id) = read().await? {
        return Ok(id);
    }
    // A team that connected before this was stored keeps the ID its
    // existing role already trusts.
    let inherited = state
        .dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":p", attr_s("AWS_CONN#"))
        .send()
        .await
        .ok()
        .and_then(|o| {
            o.items()
                .iter()
                .find_map(|i| i.get("external_id").and_then(|v| v.as_s().ok()).cloned())
        });
    let fresh = inherited.unwrap_or_else(|| Uuid::new_v4().to_string());
    let put = state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s("AWS_EXTERNAL_ID"))
        .item("external_id", attr_s(&fresh))
        .item("created_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    match put {
        Ok(_) => Ok(fresh),
        // Another request created it first: use theirs.
        Err(_) => read().await?.ok_or(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Extract AWS account ID from role ARN.
fn account_id_from_arn(arn: &str) -> Option<&str> {
    // arn:aws:iam::123456789012:role/Name → extract 123456789012
    arn.split(':').nth(4)
}

/// POST /api/aws-connections — create a new AWS connection
pub async fn create_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateConnectionRequest>,
) -> Result<Json<Value>, StatusCode> {
    // Only owner/admin can manage AWS connections
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    validate_role_arn(&body.role_arn)?;

    let account_id = account_id_from_arn(&body.role_arn).ok_or(StatusCode::BAD_REQUEST)?;
    let region = body.region.as_deref().unwrap_or("us-east-1");
    if !valid_region(region) {
        return Err(StatusCode::BAD_REQUEST);
    }
    // The team's stored External ID, never one the client chooses.
    let external_id = team_external_id(&state, &claims.team_id).await?;
    let now = chrono::Utc::now().to_rfc3339();
    let conn_id = format!("AWS_CONN#{account_id}");

    // Test AssumeRole before saving
    let sts = aws_sdk_sts::Client::new(
        &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
    );

    let assume_result = sts
        .assume_role()
        .role_arn(&body.role_arn)
        .role_session_name("coderhelm-validation")
        .external_id(&external_id)
        .duration_seconds(900)
        .send()
        .await;

    if let Err(e) = assume_result {
        warn!(role_arn = body.role_arn, error = %e, "AssumeRole validation failed");
        return Ok(Json(json!({
            "error": "unable_to_assume_role",
            "message": "Could not assume the provided role. Make sure the trust policy includes our account ID (654654210434) and the External ID matches.",
            "external_id": external_id,
        })));
    }

    // Verify the assumed account matches the ARN
    let creds = assume_result.unwrap().credentials;
    if creds.is_none() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Save connection. Reconnecting an account keeps its selected log groups.
    state
        .dynamo
        .update_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&conn_id))
        .update_expression(
            "SET role_arn = :r, external_id = :x, #region = :reg, account_id = :a, #s = :active, \
             log_groups = if_not_exists(log_groups, :empty), created_at = if_not_exists(created_at, :t), \
             updated_at = :t, created_by = if_not_exists(created_by, :u) REMOVE last_error, last_error_at",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#region", "region")
        .expression_attribute_values(":r", attr_s(&body.role_arn))
        .expression_attribute_values(":x", attr_s(&external_id))
        .expression_attribute_values(":reg", attr_s(region))
        .expression_attribute_values(":a", attr_s(account_id))
        .expression_attribute_values(":active", attr_s("active"))
        .expression_attribute_values(":empty", AttributeValue::L(vec![]))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":u", attr_s(&claims.email))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to save AWS connection: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(
        team_id = claims.team_id,
        account_id, "AWS connection created"
    );

    Ok(Json(json!({
        "connection_id": account_id,
        "external_id": external_id,
        "status": "active",
        "region": region,
    })))
}

/// GET /api/aws-connections — list all AWS connections for team
pub async fn list_connections(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let is_admin = is_admin_or_owner(&claims.role);
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.aws_insights_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("AWS_CONN#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list AWS connections: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let connections: Vec<Value> = result
        .items()
        .iter()
        .filter_map(|item| {
            let account_id = item.get("account_id")?.as_s().ok()?;
            let log_groups: Vec<String> = item
                .get("log_groups")
                .and_then(|v| v.as_l().ok())
                .map(|l| {
                    l.iter()
                        .filter_map(|v| v.as_s().ok().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let external_id = if is_admin {
                item.get("external_id").and_then(|v| v.as_s().ok()).map(|s| s.to_string())
            } else {
                None
            };

            Some(json!({
                "connection_id": account_id,
                "role_arn": item.get("role_arn")?.as_s().ok()?,
                "external_id": external_id,
                "region": item.get("region").and_then(|v| v.as_s().ok()).map(|v| v.as_str()).unwrap_or("us-east-1"),
                "status": item.get("status").and_then(|v| v.as_s().ok()).map(|v| v.as_str()).unwrap_or("active"),
                "log_groups": log_groups,
                "error_message": item.get("last_error").and_then(|v| v.as_s().ok()),
                "last_error_at": item.get("last_error_at").and_then(|v| v.as_s().ok()),
                "last_analyzed_at": item.get("last_analyzed_at").and_then(|v| v.as_s().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
                "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
            }))
        })
        .collect();

    Ok(Json(json!({ "connections": connections })))
}

/// PUT /api/aws-connections/:id — update an AWS connection
pub async fn update_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(connection_id): Path<String>,
    Json(body): Json<UpdateConnectionRequest>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    let sk = format!("AWS_CONN#{connection_id}");
    let now = chrono::Utc::now().to_rfc3339();

    let mut update_expr = vec!["updated_at = :t".to_string()];
    let mut expr_values = vec![(":t".to_string(), attr_s(&now))];

    if let Some(ref role_arn) = body.role_arn {
        validate_role_arn(role_arn)?;
        update_expr.push("role_arn = :r".to_string());
        expr_values.push((":r".to_string(), attr_s(role_arn)));
    }

    if let Some(ref region) = body.region {
        if !valid_region(region) {
            return Err(StatusCode::BAD_REQUEST);
        }
        update_expr.push("region = :reg".to_string());
        expr_values.push((":reg".to_string(), attr_s(region)));
    }

    if let Some(ref log_groups) = body.log_groups {
        if log_groups.len() > 200 || !log_groups.iter().all(|g| valid_log_group(g)) {
            return Err(StatusCode::BAD_REQUEST);
        }
        update_expr.push("log_groups = :lg".to_string());
        let lg_vals: Vec<AttributeValue> = log_groups.iter().map(|g| attr_s(g)).collect();
        expr_values.push((":lg".to_string(), AttributeValue::L(lg_vals)));
    }

    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .update_expression(format!("SET {}", update_expr.join(", ")))
        .condition_expression("attribute_exists(pk)");

    for (k, v) in expr_values {
        update = update.expression_attribute_values(k, v);
    }

    update.send().await.map_err(|e| {
        error!("Failed to update AWS connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    info!(
        team_id = claims.team_id,
        connection_id, "AWS connection updated"
    );

    Ok(Json(json!({ "status": "updated" })))
}

/// DELETE /api/aws-connections/:id — remove an AWS connection
pub async fn delete_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(connection_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    let sk = format!("AWS_CONN#{connection_id}");

    state
        .dynamo
        .delete_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to delete AWS connection: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(
        team_id = claims.team_id,
        connection_id, "AWS connection deleted"
    );

    Ok(Json(json!({ "status": "deleted" })))
}

/// POST /api/aws-connections/:id/test — validate role assumption works
pub async fn test_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(connection_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let sk = format!("AWS_CONN#{connection_id}");

    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch AWS connection: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;
    let role_arn = item
        .get("role_arn")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let external_id = item
        .get("external_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let sts = aws_sdk_sts::Client::new(
        &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
    );

    match sts
        .assume_role()
        .role_arn(role_arn)
        .role_session_name("coderhelm-test")
        .external_id(external_id)
        .duration_seconds(900)
        .send()
        .await
    {
        Ok(_) => {
            // Update status to active
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.aws_insights_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s(&sk))
                .update_expression("SET #s = :s, updated_at = :t REMOVE last_error, last_error_at")
                .expression_attribute_names("#s", "status")
                .expression_attribute_values(":s", attr_s("active"))
                .expression_attribute_values(":t", attr_s(&now))
                .send()
                .await;

            Ok(Json(
                json!({ "status": "connected", "message": "Successfully assumed role" }),
            ))
        }
        Err(e) => {
            warn!(role_arn, error = %e, "AssumeRole test failed");
            // Update status to error
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.aws_insights_table_name)
                .key("pk", attr_s(&claims.team_id))
                .key("sk", attr_s(&sk))
                .update_expression("SET #s = :s, updated_at = :t, last_error = :e")
                .expression_attribute_names("#s", "status")
                .expression_attribute_values(":s", attr_s("error"))
                .expression_attribute_values(":t", attr_s(&now))
                .expression_attribute_values(":e", attr_s(&format!("{e}")))
                .send()
                .await;

            Ok(Json(json!({
                "status": "error",
                "message": format!("Could not assume role: {e}"),
            })))
        }
    }
}

/// GET /api/aws-connections/:id/log-groups — discover log groups in customer account
pub async fn discover_log_groups(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(connection_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let sk = format!("AWS_CONN#{connection_id}");

    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch AWS connection: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    let role_arn = item
        .get("role_arn")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let external_id = item
        .get("external_id")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let region = item
        .get("region")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("us-east-1");

    // Assume role
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sts = aws_sdk_sts::Client::new(&aws_config);

    let assumed = sts
        .assume_role()
        .role_arn(role_arn)
        .role_session_name("coderhelm-discover")
        .external_id(external_id)
        .duration_seconds(900)
        .send()
        .await
        .map_err(|e| {
            error!("AssumeRole failed for log group discovery: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let creds = assumed
        .credentials()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // Build CW Logs client with assumed creds
    let assumed_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .credentials_provider(aws_sdk_sts::config::Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            Some(std::time::SystemTime::now() + std::time::Duration::from_secs(900)),
            "coderhelm-assumed",
        ))
        .load()
        .await;

    let cw_logs = aws_sdk_cloudwatchlogs::Client::new(&assumed_config);

    let mut log_groups: Vec<Value> = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = cw_logs.describe_log_groups().limit(50);
        if let Some(ref token) = next_token {
            req = req.next_token(token);
        }

        let resp = req.send().await.map_err(|e| {
            error!("DescribeLogGroups failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

        for lg in resp.log_groups() {
            if let Some(name) = lg.log_group_name() {
                log_groups.push(json!({
                    "name": name,
                    "stored_bytes": lg.stored_bytes(),
                    "retention_days": lg.retention_in_days(),
                }));
            }
        }

        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() || log_groups.len() >= 500 {
            break;
        }
    }

    Ok(Json(json!({ "log_groups": log_groups })))
}

/// GET /api/aws-connections/cfn-url — get CloudFormation quick-create URL
pub async fn get_cfn_url(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    let external_id = team_external_id(&state, &claims.team_id).await?;
    let template_url = "https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader.yaml";
    let stack_name = "CoderHelmLogReader";

    let cfn_url = format!(
        "https://console.aws.amazon.com/cloudformation/home#/stacks/quickcreate?stackName={}&templateURL={}&param_ExternalId={}&param_CoderHelmAccountId=654654210434",
        stack_name,
        urlencoding::encode(template_url),
        external_id,
    );

    Ok(Json(json!({
        "cfn_url": cfn_url,
        "external_id": external_id,
        "template_url": template_url,
    })))
}

// ────────────────────────────────────────────────────────────────
// Recommendations
// ────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct RecommendationsQuery {
    status: Option<String>,
    severity: Option<String>,
    limit: Option<i32>,
    /// `next` from the previous page.
    cursor: Option<String>,
}

/// GET /api/recommendations — list recommendations for team
pub async fn list_recommendations(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<RecommendationsQuery>,
) -> Result<Json<Value>, StatusCode> {
    let limit = params.limit.unwrap_or(50).clamp(1, 100) as usize;
    let valid = |v: &Option<String>| {
        v.as_deref()
            .is_none_or(|x| x.len() <= 20 && x.chars().all(|c| c.is_ascii_lowercase()))
    };
    if !valid(&params.status) || !valid(&params.severity) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Filters run in DynamoDB, which applies them after its page limit, so
    // keep reading pages until this page is full or the items run out.
    let mut filters = Vec::new();
    if params.status.is_some() {
        filters.push("#s = :status");
    }
    if params.severity.is_some() {
        filters.push("severity = :sev");
    }
    let mut start = params
        .cursor
        .as_deref()
        .filter(|c| c.starts_with("REC#") && c.len() <= 400)
        .map(|c| {
            std::collections::HashMap::from([
                ("pk".to_string(), attr_s(&claims.team_id)),
                ("sk".to_string(), attr_s(c)),
            ])
        });
    let mut items = Vec::new();
    let mut next: Option<String> = None;
    for _ in 0..10 {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.aws_insights_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
            .expression_attribute_values(":pk", attr_s(&claims.team_id))
            .expression_attribute_values(":prefix", attr_s("REC#"))
            .scan_index_forward(false)
            .limit(100)
            .set_exclusive_start_key(start.take());
        if !filters.is_empty() {
            q = q.filter_expression(filters.join(" AND "));
        }
        if let Some(st) = &params.status {
            q = q
                .expression_attribute_names("#s", "status")
                .expression_attribute_values(":status", attr_s(st));
        }
        if let Some(sev) = &params.severity {
            q = q.expression_attribute_values(":sev", attr_s(sev));
        }
        let out = q.send().await.map_err(|e| {
            error!("Failed to list recommendations: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        for it in out.items() {
            if items.len() == limit {
                break;
            }
            items.push(it.clone());
        }
        let page_end = out.last_evaluated_key().cloned();
        if items.len() == limit {
            // Resume after the last item returned.
            next = items
                .last()
                .and_then(|i| i.get("sk"))
                .and_then(|v| v.as_s().ok())
                .cloned();
            break;
        }
        match page_end {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => break,
        }
    }
    if next.is_none() {
        if let Some(k) = start {
            next = k.get("sk").and_then(|v| v.as_s().ok()).cloned();
        }
    }

    let recommendations: Vec<Value> = items.iter().filter_map(rec_from_item).collect();

    Ok(Json(
        json!({ "recommendations": recommendations, "next": next }),
    ))
}

/// POST /api/recommendations/:id/plan — create a plan from a recommendation
pub async fn create_plan_from_recommendation(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(rec_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+ — same as dismiss
    if rec_id.len() > 64 || !rec_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let sk = format!("REC#{rec_id}");

    // Fetch the recommendation
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch recommendation: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    // A second click opens the plan already made, instead of another one.
    if let Some(existing) = item.get("plan_id").and_then(|v| v.as_s().ok()) {
        return Ok(Json(json!({ "plan_id": existing, "status": "exists" })));
    }

    let title = item
        .get("title")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("Untitled recommendation");
    let summary = item
        .get("summary")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("");
    let suggested_action = item
        .get("suggested_action")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("");
    let source_log_group = item
        .get("source_log_group")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("");
    let error_pattern = item
        .get("error_pattern")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("");
    let severity = item
        .get("severity")
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .unwrap_or("info");

    let mut description = format!("## Log Analysis Finding ({severity})\n\n{summary}\n\n");
    if !source_log_group.is_empty() {
        description.push_str(&format!("**Affected Service** (from log group `{source_log_group}`):\nLook for the code that deploys to or runs in this log group.\n\n"));
    }
    if !error_pattern.is_empty() {
        description.push_str(&format!(
            "**Error Pattern:**\n```\n{error_pattern}\n```\n\n"
        ));
    }
    description.push_str(&format!("### Suggested Fix\n\n{suggested_action}\n\n"));
    description.push_str("### Instructions\n\n");
    description.push_str(
        "1. Search the codebase for the function/service that produces this error pattern.\n",
    );
    description.push_str("2. Implement the suggested fix.\n");
    description.push_str("3. Add error handling or logging improvements to prevent recurrence.\n");

    // Create plan via plans table
    let plan_id = ulid::Ulid::new().to_string().to_lowercase();
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.plans_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&format!("PLAN#{plan_id}")))
        .item("plan_id", attr_s(&plan_id))
        .item("title", attr_s(&format!("Fix: {title}")))
        .item("description", attr_s(&description))
        .item("destination", attr_s("github"))
        .item("status", attr_s("draft"))
        .item("task_count", AttributeValue::N("0".to_string()))
        .item("source", attr_s("log_analyzer"))
        .item("source_rec_id", attr_s(&rec_id))
        .item("created_at", attr_s(&now))
        .item("created_by", attr_s(&claims.email))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to create plan from recommendation: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Mark recommendation as approved
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        // Approved findings are kept (no TTL) so the plan's source stays visible.
        .update_expression("SET #s = :s, plan_id = :pid, updated_at = :t REMOVE #ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("approved"))
        .expression_attribute_values(":pid", attr_s(&plan_id))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;

    info!(
        team_id = claims.team_id,
        rec_id, plan_id, "Plan created from recommendation"
    );

    Ok(Json(json!({
        "plan_id": plan_id,
        "status": "created",
    })))
}

/// POST /api/recommendations/:id/dismiss — dismiss a recommendation
pub async fn dismiss_recommendation(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(rec_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+
    let sk = format!("REC#{rec_id}");
    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #s = :s, updated_at = :t, dismissed_by = :u")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("dismissed"))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":u", attr_s(&claims.email))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to dismiss recommendation: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team_id = claims.team_id, rec_id, "Recommendation dismissed");

    Ok(Json(json!({ "status": "dismissed" })))
}

// ────────────────────────────────────────────────────────────────
// Teams notifications for new recommendations
// ────────────────────────────────────────────────────────────────

/// Settings-table key of the recommendation notification setting (read by
/// the log analyzer Lambda).
const REC_NOTIFY_SK: &str = "REC_NOTIFY";

/// GET /api/recommendations/notify — where new findings post (admin+).
pub async fn get_rec_notify(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(REC_NOTIFY_SK))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to read recommendation notify setting: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .item()
        .cloned()
        .unwrap_or_default();
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    Ok(Json(json!({
        "notify_mode": s("notify_mode").unwrap_or_else(|| "off".to_string()),
        "teams_webhook_url": s("teams_webhook_url").unwrap_or_default(),
    })))
}

#[derive(serde::Deserialize)]
pub struct RecNotifyBody {
    notify_mode: String,
    #[serde(default)]
    teams_webhook_url: String,
}

/// PUT /api/recommendations/notify — "team" (the team's alert channel),
/// "custom" (its own channel) or "off" (admin+).
pub async fn put_rec_notify(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RecNotifyBody>,
) -> Result<Json<Value>, StatusCode> {
    use common::alert_notify::{valid_webhook_url, MODE_CUSTOM, MODE_OFF, MODE_TEAM};
    claims.require_role(3)?;
    let mode = body.notify_mode.trim();
    if ![MODE_TEAM, MODE_CUSTOM, MODE_OFF].contains(&mode) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let url = body.teams_webhook_url.trim();
    if mode == MODE_CUSTOM && !valid_webhook_url(url) {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(REC_NOTIFY_SK))
        .item("notify_mode", attr_s(mode))
        .item(
            "teams_webhook_url",
            attr_s(if mode == MODE_CUSTOM { url } else { "" }),
        )
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .item("updated_by", attr_s(&claims.email))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to save recommendation notify setting: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    info!(
        team_id = claims.team_id,
        mode, "Recommendation notifications saved"
    );
    Ok(Json(json!({ "ok": true })))
}

// ────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────

fn is_admin_or_owner(role: &str) -> bool {
    role == "owner" || role == "admin"
}

fn rec_from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Value> {
    let rec_id = item.get("sk")?.as_s().ok()?.strip_prefix("REC#")?;

    Some(json!({
        "rec_id": rec_id,
        "status": item.get("status").and_then(|v| v.as_s().ok()),
        "severity": item.get("severity").and_then(|v| v.as_s().ok()),
        "title": item.get("title").and_then(|v| v.as_s().ok()),
        "summary": item.get("summary").and_then(|v| v.as_s().ok()),
        "suggested_action": item.get("suggested_action").and_then(|v| v.as_s().ok()),
        "source_log_group": item.get("source_log_group").and_then(|v| v.as_s().ok()),
        "source_account_id": item.get("source_account_id").and_then(|v| v.as_s().ok()),
        "error_pattern": item.get("error_pattern").and_then(|v| v.as_s().ok()),
        "error_hash": item.get("error_hash").and_then(|v| v.as_s().ok()),
        "error_count": item.get("error_count").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
        "plan_id": item.get("plan_id").and_then(|v| v.as_s().ok()),
        "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
        "updated_at": item.get("updated_at").and_then(|v| v.as_s().ok()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regions_and_log_groups_are_validated() {
        assert!(valid_region("us-east-1"));
        assert!(valid_region("ap-southeast-2"));
        assert!(valid_region("us-gov-west-1"));
        assert!(!valid_region("us-east"));
        assert!(!valid_region("US-EAST-1"));
        assert!(!valid_region("us-east-1.evil.com"));
        assert!(valid_log_group("/aws/lambda/my-fn"));
        assert!(valid_log_group("API-Gateway-Execution-Logs_abc/prod"));
        assert!(!valid_log_group(""));
        assert!(!valid_log_group("bad name"));
        assert!(!valid_log_group(&"a".repeat(513)));
    }
}
