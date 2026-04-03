use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tracing::warn;

use crate::AppState;

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

fn attr_n(val: impl ToString) -> AttributeValue {
    AttributeValue::N(val.to_string())
}

/// Extract client IP from X-Forwarded-For header (set by API Gateway).
fn client_ip(req: &Request) -> String {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Rate limit middleware for auth endpoints.
/// Allows 20 requests per IP per 5-minute window using DynamoDB atomic counters.
pub async fn rate_limit_auth(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    check_rate_limit(&state, &client_ip(&req), "auth", 20, 300).await?;
    Ok(next.run(req).await)
}

/// Rate limit middleware for webhook endpoints.
/// Allows 100 requests per source IP per 1-minute window.
pub async fn rate_limit_webhook(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    check_rate_limit(&state, &client_ip(&req), "webhook", 100, 60).await?;
    Ok(next.run(req).await)
}

/// DynamoDB-based sliding window rate limiter.
/// Uses atomic increment with TTL for automatic cleanup.
async fn check_rate_limit(
    state: &AppState,
    ip: &str,
    scope: &str,
    max_requests: i64,
    window_secs: u64,
) -> Result<(), StatusCode> {
    let now = chrono::Utc::now().timestamp() as u64;
    let window_key = now / window_secs;
    let pk = format!("RATE#{scope}#{ip}");
    let sk = format!("W#{window_key}");
    let ttl = (now + window_secs * 2) as i64; // TTL: 2 windows for cleanup

    let result = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&pk))
        .key("sk", attr_s(&sk))
        .update_expression("SET #c = if_not_exists(#c, :zero) + :one, #ttl = :ttl")
        .expression_attribute_names("#c", "count")
        .expression_attribute_names("#ttl", "expires_at")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":zero", attr_n(0))
        .expression_attribute_values(":ttl", attr_n(ttl))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
        .send()
        .await;

    match result {
        Ok(output) => {
            let count = output
                .attributes()
                .and_then(|a| a.get("count"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<i64>().ok())
                .unwrap_or(1);

            if count > max_requests {
                warn!(ip, scope, count, "Rate limit exceeded");
                Err(StatusCode::TOO_MANY_REQUESTS)
            } else {
                Ok(())
            }
        }
        Err(e) => {
            // Fail open — don't block requests if DynamoDB is unavailable
            warn!("Rate limit check failed: {e}");
            Ok(())
        }
    }
}
