use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, warn};

use crate::models::Claims;
use crate::AppState;

// ─── Usage limits (included in Pro) ─────────────────────────────────
pub const INCLUDED_TOKENS: u64 = 5_000_000; // 5M tokens (in+out)
pub const FREE_TIER_TOKENS: u64 = 500_000; // 500K tokens for free tier
pub const OVERAGE_PER_1K_TOKENS_CENTS: u64 = 5; // $0.05 per 1K tokens ($50/1M)

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

    let current_plans: u64 = usage
        .as_ref()
        .and_then(|i| i.get("total_plans"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let current_tokens_in: u64 = usage
        .as_ref()
        .and_then(|i| i.get("total_tokens_in"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let current_tokens_out: u64 = usage
        .as_ref()
        .and_then(|i| i.get("total_tokens_out"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let current_tokens = current_tokens_in + current_tokens_out;

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

    let tokens_overage = current_tokens.saturating_sub(INCLUDED_TOKENS);
    let tokens_overage_1k = tokens_overage / 1000;
    let estimated_overage_cents = tokens_overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;

    Ok(Json(json!({
        "subscription_status": subscription_status,
        "plan_id": item.and_then(|i| i.get("plan_id")).and_then(|v| v.as_s().ok()),
        "has_payment_method": !stripe_customer_id.is_empty(),
        "stripe_publishable_key": state.secrets.stripe_publishable_key.as_deref().unwrap_or(""),
        "last_payment_at": item.and_then(|i| i.get("last_payment_at")).and_then(|v| v.as_s().ok()),
        "payment_retry_count": item.and_then(|i| i.get("payment_retry_count")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "last_failure_reason": item.and_then(|i| i.get("last_failure_reason")).and_then(|v| v.as_s().ok()),
        "access_until": item.and_then(|i| i.get("access_until")).and_then(|v| v.as_s().ok()),
        "cancelled_at": item.and_then(|i| i.get("cancelled_at")).and_then(|v| v.as_s().ok()),
        "limits": {
            "tokens": INCLUDED_TOKENS,
            "overage_per_1k_tokens_cents": OVERAGE_PER_1K_TOKENS_CENTS,
        },
        "current_period": {
            "month": month,
            "usage_cost": current_usage_cost,
            "total_runs": current_runs,
            "total_tokens": current_tokens,
            "total_plans": current_plans,
            "estimated_overage_cents": estimated_overage_cents,
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

/// POST /api/billing/subscribe — create an embedded Stripe Checkout Session.
/// Returns a client_secret the frontend uses to render Stripe checkout inline.
/// On success, the `invoice.payment_succeeded` webhook activates the subscription.
pub async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let price_id = body["price_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or(state
            .secrets
            .stripe_price_id
            .as_deref()
            .filter(|s| !s.is_empty()))
        .ok_or_else(|| {
            error!("No stripe_price_id provided in request body or secrets");
            StatusCode::BAD_REQUEST
        })?;

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
        return Err(StatusCode::CONFLICT);
    }

    // Cancel any existing incomplete subscription before creating a new one
    if current_status == "incomplete" || current_status == "past_due" {
        if let Some(old_sub_id) = item
            .and_then(|i| i.get("stripe_subscription_id"))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
        {
            warn!("Cancelling stale {current_status} subscription {old_sub_id}");
            let _ = state
                .http
                .delete(format!(
                    "https://api.stripe.com/v1/subscriptions/{old_sub_id}"
                ))
                .header("Authorization", format!("Bearer {stripe_key}"))
                .send()
                .await;
        }
    }

    // Fetch user email for Stripe
    let user_email = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("email").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    // Get or create Stripe customer
    let customer_id = match item
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
    {
        Some(cid) => cid.to_string(),
        None => create_stripe_customer(&state, stripe_key, &claims.tenant_id, &user_email).await?,
    };

    // Store customer + reverse mapping before checkout so webhooks can resolve tenant
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression("SET stripe_customer_id = :cid, updated_at = :t")
        .expression_attribute_values(":cid", attr_s(&customer_id))
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to store billing customer: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

    // Create Stripe Checkout Session in embedded mode — renders inline via iframe
    let mut form_params: Vec<(&str, &str)> = vec![
        ("mode", "subscription"),
        ("ui_mode", "embedded_page"),
        ("customer", &customer_id),
        ("line_items[0][price]", price_id),
        ("line_items[0][quantity]", "1"),
        ("return_url", "https://app.d3ftly.com/billing/?success=true"),
        ("metadata[tenant_id]", &claims.tenant_id),
        ("subscription_data[metadata][tenant_id]", &claims.tenant_id),
    ];
    if !user_email.is_empty() {
        form_params.push(("customer_email", &user_email));
    }
    let response = state
        .http
        .post("https://api.stripe.com/v1/checkout/sessions")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&form_params)
        .send()
        .await
        .map_err(|e| {
            error!("Stripe checkout session request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let session: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe checkout response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if let Some(err) = session["error"]["message"].as_str() {
        error!("Stripe error creating checkout session: {err}");
        return Err(StatusCode::BAD_GATEWAY);
    }

    let client_secret = session["client_secret"].as_str().ok_or_else(|| {
        error!("Stripe checkout session missing client_secret");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(json!({ "client_secret": client_secret })))
}

/// Create a Stripe customer for a tenant.
async fn create_stripe_customer(
    state: &AppState,
    stripe_key: &str,
    tenant_id: &str,
    email: &str,
) -> Result<String, StatusCode> {
    // Get tenant name for the customer record
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

    let mut form_params = vec![("name", name), ("metadata[tenant_id]", tenant_id)];
    if !email.is_empty() {
        form_params.push(("email", email));
    }
    let response = state
        .http
        .post("https://api.stripe.com/v1/customers")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&form_params)
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

/// POST /api/billing/cancel — cancel the current subscription at period end.
pub async fn cancel_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Get subscription ID
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

    let sub_id = billing
        .item()
        .and_then(|i| i.get("stripe_subscription_id"))
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?
        .to_string();

    // Cancel at period end (not immediately)
    let response = state
        .http
        .post(format!("https://api.stripe.com/v1/subscriptions/{sub_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[("cancel_at_period_end", "true")])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe cancel request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if body["error"].is_object() {
        error!(
            "Stripe error cancelling subscription: {}",
            body["error"]["message"]
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    Ok(Json(json!({ "status": "cancelling" })))
}

/// POST /api/billing/reactivate — undo a pending cancellation.
pub async fn reactivate_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

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

    let sub_id = billing
        .item()
        .and_then(|i| i.get("stripe_subscription_id"))
        .and_then(|v| v.as_s().ok())
        .ok_or(StatusCode::NOT_FOUND)?
        .to_string();

    let response = state
        .http
        .post(format!("https://api.stripe.com/v1/subscriptions/{sub_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[("cancel_at_period_end", "false")])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe reactivate request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if body["error"].is_object() {
        error!(
            "Stripe error reactivating subscription: {}",
            body["error"]["message"]
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    Ok(Json(json!({ "status": "active" })))
}

/// POST /api/billing/payment-method — create a SetupIntent for updating the payment method.
/// Returns client_secret for Stripe Elements to collect new card details.
pub async fn create_setup_intent(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    let response = state
        .http
        .post("https://api.stripe.com/v1/setup_intents")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("customer", customer_id.as_str()),
            ("payment_method_types[]", "card"),
            ("usage", "off_session"),
        ])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe setup intent request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let client_secret = body["client_secret"].as_str().ok_or_else(|| {
        error!("Stripe setup intent response missing client_secret");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(json!({ "client_secret": client_secret })))
}

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

/// Report metered usage to Stripe via Billing Meter Events.
/// `event_name` maps to the meter: "d3ftly_plans_overage" or "d3ftly_tokens_overage".
/// `quantity` is the number of units OVER the included limit (only overages).
pub async fn report_stripe_usage(
    state: &AppState,
    tenant_id: &str,
    usage_type: &str, // "plans_overage" or "runs_overage"
    quantity: u64,
) {
    if quantity == 0 {
        return;
    }

    let stripe_key = match &state.secrets.stripe_secret_key {
        Some(k) => k.clone(),
        None => return,
    };

    // Get the Stripe customer ID for this tenant
    let billing = match state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(tenant_id))
        .key("sk", attr_s("BILLING"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to fetch billing for usage report: {e}");
            return;
        }
    };

    let customer_id = match billing
        .item()
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
    {
        Some(id) => id.to_string(),
        None => return,
    };

    let event_name = format!("d3ftly_{usage_type}");
    let ts = chrono::Utc::now().timestamp().to_string();
    let qty = quantity.to_string();

    if let Err(e) = state
        .http
        .post("https://api.stripe.com/v1/billing/meter_events")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("event_name", event_name.as_str()),
            ("timestamp", ts.as_str()),
            ("payload[value]", qty.as_str()),
            ("payload[stripe_customer_id]", customer_id.as_str()),
        ])
        .send()
        .await
    {
        warn!("Failed to report Stripe meter event for {usage_type}: {e}");
    }
}
