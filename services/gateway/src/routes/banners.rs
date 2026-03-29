use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

use crate::models::Claims;
use crate::AppState;

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

/// GET /api/banners — fetch active notification banners (global + tenant-specific).
///
/// DynamoDB schema (main table):
///   Global:     pk=BANNER#GLOBAL,       sk=<banner_id>
///   Per-tenant: pk=BANNER#<tenant_id>,  sk=<banner_id>
///
/// Item attributes: message, banner_type (info|warning|error), dismissible,
///   link_text, link_url, starts_at, expires_at, created_at, active
pub async fn list_banners(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let now = chrono::Utc::now().to_rfc3339();

    // Fetch global banners
    let global = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk")
        .filter_expression("active = :t")
        .expression_attribute_values(":pk", attr_s("BANNER#GLOBAL"))
        .expression_attribute_values(":t", attr_s("true"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query global banners: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Fetch tenant-specific banners
    let tenant = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk")
        .filter_expression("active = :t")
        .expression_attribute_values(":pk", attr_s(&format!("BANNER#{}", claims.tenant_id)))
        .expression_attribute_values(":t", attr_s("true"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query tenant banners: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut banners: Vec<Value> = Vec::new();

    for item in global.items().iter().chain(tenant.items().iter()) {
        // Skip expired banners
        if let Some(expires) = item
            .get("expires_at")
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
        {
            if expires.as_str() < now.as_str() {
                continue;
            }
        }
        // Skip banners that haven't started yet
        if let Some(starts) = item
            .get("starts_at")
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
        {
            if starts.as_str() > now.as_str() {
                continue;
            }
        }

        let banner_id = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .map_or("unknown", |v| v.as_str());

        banners.push(json!({
            "id": banner_id,
            "message": item.get("message").and_then(|v| v.as_s().ok()),
            "type": item.get("banner_type").and_then(|v| v.as_s().ok()).map_or("info", |v| v.as_str()),
            "dismissible": item.get("dismissible").and_then(|v| v.as_s().ok()).map(|v| v == "true").unwrap_or(true),
            "link_text": item.get("link_text").and_then(|v| v.as_s().ok()),
            "link_url": item.get("link_url").and_then(|v| v.as_s().ok()),
        }));
    }

    Ok(Json(json!({ "banners": banners })))
}
