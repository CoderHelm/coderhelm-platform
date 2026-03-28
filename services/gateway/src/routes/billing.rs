use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::error;

use crate::models::Claims;
use crate::AppState;

/// GET /api/billing — get billing overview (subscription status, plan, payment method, balance).
pub async fn get_billing(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    // Fetch billing record
    let billing = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch billing: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = billing.item();

    let subscription_status = item
        .and_then(|i| i.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map_or("none", |v| v);

    let stripe_customer_id = item
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .map_or("", |v| v);

    // Get current month usage from analytics
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let usage = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("tenant_id", attr_s(&claims.tenant_id))
        .key("period", attr_s(&month))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let current_usage_cost: f64 = usage
        .as_ref()
        .and_then(|i| i.get("total_cost_usd"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0.0);

    let current_runs: u64 = usage
        .as_ref()
        .and_then(|i| i.get("total_runs"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    // Get recent payments (last 5)
    let payments = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("PAYMENT#"))
        .scan_index_forward(false)
        .limit(5)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query payments: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let recent_payments: Vec<Value> = payments
        .items()
        .iter()
        .map(|item| {
            json!({
                "invoice_number": item.get("invoice_number").and_then(|v| v.as_s().ok()),
                "amount_cents": item.get("amount_cents").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "status": item.get("status").and_then(|v| v.as_s().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({
        "subscription_status": subscription_status,
        "plan_id": item.and_then(|i| i.get("plan_id")).and_then(|v| v.as_s().ok()),
        "has_payment_method": !stripe_customer_id.is_empty(),
        "last_payment_at": item.and_then(|i| i.get("last_payment_at")).and_then(|v| v.as_s().ok()),
        "payment_retry_count": item.and_then(|i| i.get("payment_retry_count")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "last_failure_reason": item.and_then(|i| i.get("last_failure_reason")).and_then(|v| v.as_s().ok()),
        "access_until": item.and_then(|i| i.get("access_until")).and_then(|v| v.as_s().ok()),
        "cancelled_at": item.and_then(|i| i.get("cancelled_at")).and_then(|v| v.as_s().ok()),
        "current_period": {
            "month": month,
            "usage_cost": current_usage_cost,
            "total_runs": current_runs,
        },
        "recent_payments": recent_payments,
    })))
}

/// POST /api/billing/portal — create a Stripe Customer Portal session.
/// This lets users update their credit card, view invoices, and cancel.
pub async fn create_portal_session(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Get the Stripe customer ID for this tenant
    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    // Create portal session via Stripe API
    let response = state
        .http
        .post("https://api.stripe.com/v1/billing_portal/sessions")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("customer", customer_id.as_str()),
            ("return_url", "https://app.d3ftly.com/dashboard/billing"),
        ])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe portal request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let url = body["url"].as_str().ok_or_else(|| {
        error!("Stripe portal response missing URL field");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(json!({ "url": url })))
}

/// POST /api/billing/subscribe — create a Stripe Subscription (incomplete) for Stripe Elements.
/// Returns the client_secret for the PaymentIntent so the frontend can confirm with card Element.
pub async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let price_id = body["price_id"].as_str().ok_or(StatusCode::BAD_REQUEST)?;

    // Check if already has an active subscription
    let billing = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch billing: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = billing.item();

    let current_status = item
        .and_then(|i| i.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map_or("none", |v| v);

    if current_status == "active" {
        return Err(StatusCode::CONFLICT); // Already subscribed, use portal to manage
    }

    // Get or create Stripe customer
    let customer_id = match item
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
    {
        Some(cid) => cid.to_string(),
        None => create_stripe_customer(&state, stripe_key, &claims.tenant_id).await?,
    };

    // Create subscription with payment_behavior=default_incomplete
    // This creates the subscription + PaymentIntent but doesn't charge yet.
    // The frontend uses the client_secret with Stripe Elements to confirm payment.
    let response = state
        .http
        .post("https://api.stripe.com/v1/subscriptions")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("customer", customer_id.as_str()),
            ("items[0][price]", price_id),
            ("payment_behavior", "default_incomplete"),
            (
                "payment_settings[save_default_payment_method]",
                "on_subscription",
            ),
            ("expand[]", "latest_invoice.payment_intent"),
            ("metadata[tenant_id]", &claims.tenant_id),
        ])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe subscription request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let sub: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if let Some(err) = sub["error"]["message"].as_str() {
        error!("Stripe error creating subscription: {err}");
        return Err(StatusCode::BAD_GATEWAY);
    }

    let subscription_id = sub["id"].as_str().unwrap_or("");
    let client_secret = sub["latest_invoice"]["payment_intent"]["client_secret"]
        .as_str()
        .ok_or_else(|| {
            error!("Stripe subscription response missing client_secret");
            StatusCode::BAD_GATEWAY
        })?;

    // Store the pending subscription
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(
            "SET stripe_customer_id = :cid, stripe_subscription_id = :sid, \
             subscription_status = :status, updated_at = :t",
        )
        .expression_attribute_values(":cid", attr_s(&customer_id))
        .expression_attribute_values(":sid", attr_s(subscription_id))
        .expression_attribute_values(":status", attr_s("incomplete"))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to store pending subscription: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Also store the reverse mapping: Stripe customer → tenant
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&format!("STRIPE#{customer_id}")))
        .item("sk", attr_s("MAPPING"))
        .item("tenant_id", attr_s(&claims.tenant_id))
        .item("created_at", attr_s(&now))
        .send()
        .await;

    Ok(Json(json!({
        "subscription_id": subscription_id,
        "client_secret": client_secret,
    })))
}

/// Create a Stripe customer for a tenant.
async fn create_stripe_customer(
    state: &AppState,
    stripe_key: &str,
    tenant_id: &str,
) -> Result<String, StatusCode> {
    // Get tenant name/email for the customer record
    let tenant = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s("TENANT"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch tenant: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let name = tenant
        .item()
        .and_then(|i| i.get("name"))
        .and_then(|v| v.as_s().ok())
        .map_or(tenant_id, |v| v.as_str());

    let response = state
        .http
        .post("https://api.stripe.com/v1/customers")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[("name", name), ("metadata[tenant_id]", tenant_id)])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe customer creation failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let customer: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe customer response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    customer["id"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            error!("Stripe customer response missing ID");
            StatusCode::BAD_GATEWAY
        })
}

/// GET /api/billing/invoices — list all invoices for this tenant.
pub async fn list_invoices(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("INVOICE#"))
        .scan_index_forward(false)
        .limit(50)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to query invoices: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let invoices: Vec<Value> = result
        .items()
        .iter()
        .map(|item| {
            json!({
                "invoice_id": item.get("stripe_invoice_id").and_then(|v| v.as_s().ok()),
                "invoice_number": item.get("invoice_number").and_then(|v| v.as_s().ok()),
                "amount_cents": item.get("amount_cents").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "period": item.get("period").and_then(|v| v.as_s().ok()),
                "status": item.get("status").and_then(|v| v.as_s().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    Ok(Json(json!({ "invoices": invoices })))
}

/// GET /api/billing/invoices/:invoice_id/pdf — download invoice PDF.
/// Uses Stripe's hosted invoice PDF URL, proxied via a presigned redirect.
pub async fn download_invoice_pdf(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(invoice_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    // Fetch invoice record to get Stripe PDF URL
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&format!("INVOICE#{invoice_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch invoice: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let item = result.item().ok_or(StatusCode::NOT_FOUND)?;

    // Stripe provides a hosted invoice PDF URL directly
    let pdf_url = item
        .get("stripe_pdf_url")
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?;

    if pdf_url.is_empty()
        || !pdf_url.starts_with("https://pay.stripe.com/")
            && !pdf_url.starts_with("https://invoice.stripe.com/")
    {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(json!({ "pdf_url": pdf_url })))
}

// ─── Helpers ────────────────────────────────────────────────────────

async fn get_stripe_customer_id(state: &AppState, tenant_id: &str) -> Result<String, StatusCode> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch billing record: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    result
        .item()
        .and_then(|item| item.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}
