use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

// ─── Usage limits (included in Pro) ─────────────────────────────────
pub const INCLUDED_TOKENS: u64 = 1_000_000; // 1M tokens (in+out) included in Pro

// ─── Rate limiting ──────────────────────────────────────────────────
const BILLING_ACTION_COOLDOWN_SECS: i64 = 10; // Min seconds between billing write actions per tenant

/// DynamoDB-based per-tenant cooldown for billing write actions.
/// Returns Err(429) if the tenant performed a billing action too recently.
async fn enforce_billing_cooldown(
    state: &AppState,
    tenant_id: &str,
    action: &str,
) -> Result<(), StatusCode> {
    let now = chrono::Utc::now();
    let key = format!("RATE#{action}");

    // Try a conditional put: only succeeds if no recent entry exists
    let result = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(tenant_id))
        .item("sk", attr_s(&key))
        .item(
            "expires_at",
            attr_s(&(now + chrono::Duration::seconds(BILLING_ACTION_COOLDOWN_SECS)).to_rfc3339()),
        )
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N(
                (now.timestamp() + BILLING_ACTION_COOLDOWN_SECS + 60).to_string(),
            ),
        )
        .condition_expression("attribute_not_exists(pk) OR expires_at < :now")
        .expression_attribute_values(":now", attr_s(&now.to_rfc3339()))
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            warn!(tenant_id, action, "Billing rate limit hit");
            Err(StatusCode::TOO_MANY_REQUESTS)
        }
    }
}

/// Validate that a Stripe ID has the expected prefix (e.g. "pm_", "cus_", "sub_").
fn validate_stripe_id(id: &str, prefix: &str) -> Result<(), StatusCode> {
    if !id.starts_with(prefix) || id.len() > 255 {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}
pub const FREE_TIER_TOKENS: u64 = 500_000; // 500K tokens for free tier
pub const OVERAGE_PER_1K_TOKENS_CENTS: u64 = 1000; // $10.00 per 1K tokens

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

    let mut subscription_status = item
        .and_then(|i| i.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map_or("none", |v| v)
        .to_string();

    // Resolve stripe_customer_id: prefer billing record, fall back to reverse mapping
    let mut stripe_customer_id = item
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_default();

    if stripe_customer_id.is_empty() {
        // Try to restore from reverse mapping table
        if let Some(cid) = lookup_customer_id_for_tenant(&state, &claims.tenant_id).await {
            stripe_customer_id = cid.clone();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.table_name)
                .key("pk", attr_s(&claims.tenant_id))
                .key("sk", attr_s("BILLING"))
                .update_expression("SET stripe_customer_id = :cid, updated_at = :t")
                .expression_attribute_values(":cid", attr_s(&cid))
                .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                .send()
                .await;
            info!(tenant_id = %claims.tenant_id, "Restored stripe_customer_id from reverse mapping");
        }
    }

    // Reconcile with Stripe: if we have a customer, check their actual subscription status
    if !stripe_customer_id.is_empty() {
        if let Some(stripe_key) = state.secrets.stripe_secret_key.as_deref() {
            let stripe_has_active =
                has_active_stripe_subscription(&state, stripe_key, &stripe_customer_id).await;

            let dynamo_says_active = subscription_status == "active";

            if stripe_has_active && !dynamo_says_active {
                // Stripe active, DynamoDB not → restore to active
                warn!(
                    tenant_id = %claims.tenant_id,
                    dynamo_status = %subscription_status,
                    "Reconcile: Stripe has active sub but DynamoDB disagrees — restoring to active"
                );
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(&claims.tenant_id))
                    .key("sk", attr_s("BILLING"))
                    .update_expression("SET subscription_status = :s, updated_at = :t")
                    .expression_attribute_values(":s", attr_s("active"))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
                subscription_status = "active".to_string();
            } else if !stripe_has_active && dynamo_says_active {
                // Stripe NOT active, DynamoDB says active → downgrade to free
                warn!(
                    tenant_id = %claims.tenant_id,
                    "Reconcile: DynamoDB says active but Stripe has no active sub — setting to free"
                );
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.table_name)
                    .key("pk", attr_s(&claims.tenant_id))
                    .key("sk", attr_s("BILLING"))
                    .update_expression(
                        "SET subscription_status = :s, updated_at = :t, previous_status = :prev",
                    )
                    .expression_attribute_values(":s", attr_s("free"))
                    .expression_attribute_values(":prev", attr_s("active"))
                    .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await;
                subscription_status = "free".to_string();
            }
        }
    }

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
        .table_name(&state.config.events_table_name)
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
                "invoice_id": item.get("stripe_invoice_id").and_then(|v| v.as_s().ok()),
                "invoice_number": item.get("invoice_number").and_then(|v| v.as_s().ok()),
                "amount_cents": item.get("amount_cents").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "status": item.get("status").and_then(|v| v.as_s().ok()),
                "created_at": item.get("created_at").and_then(|v| v.as_s().ok()),
            })
        })
        .collect();

    let is_pro = subscription_status == "active";
    let token_limit = if is_pro {
        INCLUDED_TOKENS
    } else {
        FREE_TIER_TOKENS
    };

    let tokens_overage = current_tokens.saturating_sub(token_limit);
    let tokens_overage_1k = tokens_overage / 1000;
    let raw_overage_cents = tokens_overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;

    // Read budget cap and apply it to the displayed estimate
    let budget = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s("SETTINGS#BUDGET"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let max_budget_cents: u64 = budget
        .as_ref()
        .and_then(|i| i.get("max_budget_cents"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let estimated_overage_cents = if max_budget_cents > 0 {
        raw_overage_cents.min(max_budget_cents)
    } else {
        raw_overage_cents
    };

    Ok(Json(json!({
        "subscription_status": subscription_status,
        "previous_status": item.and_then(|i| i.get("previous_status")).and_then(|v| v.as_s().ok()),
        "plan_id": item.and_then(|i| i.get("plan_id")).and_then(|v| v.as_s().ok()),
        "has_payment_method": !stripe_customer_id.is_empty(),
        "stripe_publishable_key": state.secrets.stripe_publishable_key.as_deref().unwrap_or(""),
        "last_payment_at": item.and_then(|i| i.get("last_payment_at")).and_then(|v| v.as_s().ok()),
        "payment_retry_count": item.and_then(|i| i.get("payment_retry_count")).and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
        "last_failure_reason": item.and_then(|i| i.get("last_failure_reason")).and_then(|v| v.as_s().ok()),
        "access_until": item.and_then(|i| i.get("access_until")).and_then(|v| v.as_s().ok()),
        "cancelled_at": item.and_then(|i| i.get("cancelled_at")).and_then(|v| v.as_s().ok()),
        "limits": {
            "tokens": token_limit,
            "overage_per_1k_tokens_cents": OVERAGE_PER_1K_TOKENS_CENTS,
            "max_budget_cents": max_budget_cents,
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
            ("return_url", "https://app.coderhelm.com/dashboard/billing"),
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

/// POST /api/billing/subscribe — create a Stripe Subscription with incomplete status.
/// Returns a client_secret the frontend uses to confirm payment via Stripe Elements.
/// On success, the `invoice.payment_succeeded` webhook activates the subscription.
pub async fn create_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    enforce_billing_cooldown(&state, &claims.tenant_id, "subscribe").await?;

    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;
    let price_id = state
        .secrets
        .stripe_price_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            error!("stripe_price_id not configured in secrets");
            StatusCode::SERVICE_UNAVAILABLE
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
        .table_name(&state.config.users_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&claims.sub))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("email").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default();

    // Get or create Stripe customer — check billing record, then reverse mapping, then create new
    let customer_id = match item
        .and_then(|i| i.get("stripe_customer_id"))
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
    {
        Some(cid) => cid.to_string(),
        None => {
            // Try reverse mapping before creating a brand new customer
            if let Some(existing_cid) =
                lookup_customer_id_for_tenant(&state, &claims.tenant_id).await
            {
                info!(
                    tenant_id = %claims.tenant_id,
                    customer_id = %existing_cid,
                    "Recovered Stripe customer from reverse mapping"
                );
                existing_cid
            } else {
                create_stripe_customer(&state, stripe_key, &claims.tenant_id, &user_email).await?
            }
        }
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
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&format!("STRIPE#{customer_id}")))
        .item("sk", attr_s("MAPPING"))
        .item("tenant_id", attr_s(&claims.tenant_id))
        .item("created_at", attr_s(&now))
        .send()
        .await;

    // Create Stripe Subscription with incomplete status — frontend confirms payment via Elements
    // First, cancel any incomplete subscriptions on the Stripe customer to prevent duplicates
    // (handles race conditions where DynamoDB status hasn't been updated yet)
    cancel_incomplete_subscriptions(&state, stripe_key, &customer_id).await;

    // Fail-safe: check Stripe directly for any active subscription on this customer.
    // This prevents double-charging if DynamoDB was incorrectly set to "free"
    // (e.g. from a stale subscription.deleted webhook).
    if has_active_stripe_subscription(&state, stripe_key, &customer_id).await {
        warn!(
            tenant_id = %claims.tenant_id,
            customer_id,
            "Blocked subscribe — customer already has an active Stripe subscription"
        );
        // Restore DynamoDB to active so the dashboard reflects reality
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression("SET subscription_status = :s, updated_at = :t")
            .expression_attribute_values(":s", attr_s("active"))
            .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
        return Err(StatusCode::CONFLICT);
    }

    // If customer has a saved default payment method, skip the card form
    // and let Stripe auto-charge it.
    let default_pm = get_customer_default_pm(&state, stripe_key, &customer_id).await;
    let has_saved_pm = default_pm.is_some();

    let mut form_params: Vec<(&str, &str)> = vec![
        ("customer", &customer_id),
        ("items[0][price]", price_id),
        (
            "payment_settings[save_default_payment_method]",
            "on_subscription",
        ),
        ("payment_settings[payment_method_types][]", "card"),
        ("expand[]", "latest_invoice.payment_intent"),
        ("metadata[tenant_id]", &claims.tenant_id),
    ];

    // Only require frontend confirmation if there's no saved PM.
    // With a saved PM, Stripe charges it automatically.
    if !has_saved_pm {
        form_params.push(("payment_behavior", "default_incomplete"));
    }

    if let Some(ref pm_id) = default_pm {
        form_params.push(("default_payment_method", pm_id));
    }

    // Overage billing is handled by the worker via direct invoice items
    // (not via metered subscription items)

    let response = state
        .http
        .post("https://api.stripe.com/v1/subscriptions")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .header("Stripe-Version", "2023-10-16")
        .form(&form_params)
        .send()
        .await
        .map_err(|e| {
            error!("Stripe subscription request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let subscription: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe subscription response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if let Some(err) = subscription["error"]["message"].as_str() {
        error!("Stripe error creating subscription: {err}");
        return Err(StatusCode::BAD_GATEWAY);
    }

    // When a saved PM was charged, the subscription may already be active.
    let sub_status = subscription["status"].as_str().unwrap_or("incomplete");
    let sub_id = subscription["id"].as_str().unwrap_or("");

    if !sub_id.is_empty() {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression(
                "SET stripe_subscription_id = :sid, subscription_status = :s, updated_at = :t",
            )
            .expression_attribute_values(":sid", attr_s(sub_id))
            .expression_attribute_values(":s", attr_s(sub_status))
            .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await;
    }

    // If the subscription is already active (saved PM auto-charged), no client_secret needed
    if sub_status == "active" {
        info!(
            tenant_id = %claims.tenant_id,
            sub_id,
            "Subscription auto-activated with saved payment method"
        );
        return Ok(Json(json!({ "already_active": true })));
    }

    let client_secret = subscription["latest_invoice"]["payment_intent"]["client_secret"]
        .as_str()
        .ok_or_else(|| {
            error!("Stripe subscription missing payment_intent client_secret");
            StatusCode::BAD_GATEWAY
        })?;

    Ok(Json(json!({ "client_secret": client_secret })))
}

/// Cancel all incomplete/past_due subscriptions for a Stripe customer.
/// Prevents duplicates when concurrent subscribe requests arrive.
async fn cancel_incomplete_subscriptions(state: &AppState, stripe_key: &str, customer_id: &str) {
    let encoded_cid = urlencoding::encode(customer_id);
    for status in &["incomplete", "past_due"] {
        let url = format!(
            "https://api.stripe.com/v1/subscriptions?customer={encoded_cid}&status={status}&limit=10"
        );
        let Ok(resp) = state
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {stripe_key}"))
            .send()
            .await
        else {
            continue;
        };
        let Ok(body) = resp.json::<Value>().await else {
            continue;
        };
        if let Some(subs) = body["data"].as_array() {
            for sub in subs {
                if let Some(sub_id) = sub["id"].as_str() {
                    warn!("Cancelling stale {status} subscription {sub_id} for customer {customer_id}");
                    let _ = state
                        .http
                        .delete(format!("https://api.stripe.com/v1/subscriptions/{sub_id}"))
                        .header("Authorization", format!("Bearer {stripe_key}"))
                        .send()
                        .await;
                }
            }
        }
    }
}

/// Check if the Stripe customer already has an active subscription.
/// Returns true if at least one active subscription exists — blocks duplicate creation.
async fn has_active_stripe_subscription(
    state: &AppState,
    stripe_key: &str,
    customer_id: &str,
) -> bool {
    let encoded_cid = urlencoding::encode(customer_id);
    let url = format!(
        "https://api.stripe.com/v1/subscriptions?customer={encoded_cid}&status=active&limit=1"
    );
    let Ok(resp) = state
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
    else {
        return false;
    };
    let Ok(body) = resp.json::<Value>().await else {
        return false;
    };
    body["data"].as_array().is_some_and(|subs| !subs.is_empty())
}

/// Look up the Stripe customer ID for a tenant from the reverse mapping table.
/// This recovers the customer ID even if the billing record lost it.
async fn lookup_customer_id_for_tenant(state: &AppState, tenant_id: &str) -> Option<String> {
    // Scan the events table for STRIPE#<cid> → MAPPING rows where tenant_id matches.
    // There are very few MAPPING rows (one per customer) so this scan is cheap.
    let result = state
        .dynamo
        .scan()
        .table_name(&state.config.events_table_name)
        .filter_expression("sk = :sk AND tenant_id = :tid")
        .expression_attribute_values(":sk", attr_s("MAPPING"))
        .expression_attribute_values(":tid", attr_s(tenant_id))
        .limit(1)
        .send()
        .await
        .ok()?;

    result
        .items()
        .first()
        .and_then(|item| item.get("pk").and_then(|v| v.as_s().ok()))
        .and_then(|pk| pk.strip_prefix("STRIPE#"))
        .map(|cid| cid.to_string())
}

/// Get a payment method for the customer from Stripe.
/// Checks default PM first, then falls back to listing attached payment methods.
async fn get_customer_default_pm(
    state: &AppState,
    stripe_key: &str,
    customer_id: &str,
) -> Option<String> {
    let url = format!("https://api.stripe.com/v1/customers/{customer_id}");
    let resp = state
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .ok()?;
    let customer: Value = resp.json().await.ok()?;

    // 1. Check invoice_settings.default_payment_method
    if let Some(pm) = customer["invoice_settings"]["default_payment_method"]
        .as_str()
        .filter(|s| !s.is_empty())
    {
        return Some(pm.to_string());
    }

    // 2. Check default_source
    if let Some(src) = customer["default_source"]
        .as_str()
        .filter(|s| !s.is_empty())
    {
        return Some(src.to_string());
    }

    // 3. Fall back to listing attached payment methods (picks the most recent card)
    let encoded_cid = urlencoding::encode(customer_id);
    let list_url = format!(
        "https://api.stripe.com/v1/payment_methods?customer={encoded_cid}&type=card&limit=1"
    );
    let list_resp = state
        .http
        .get(&list_url)
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .ok()?;
    let list_body: Value = list_resp.json().await.ok()?;
    list_body["data"][0]["id"].as_str().map(|s| s.to_string())
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
        .table_name(&state.config.events_table_name)
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
        .filter(|item| {
            let status = item.get("status").and_then(|v| v.as_s().ok()).map(|s| s.as_str()).unwrap_or("");
            matches!(status, "paid" | "partially_paid" | "refunded" | "partially_refunded")
        })
        .map(|item| {
            json!({
                "invoice_id": item.get("stripe_invoice_id").and_then(|v| v.as_s().ok()),
                "invoice_number": item.get("invoice_number").and_then(|v| v.as_s().ok()),
                "amount_cents": item.get("amount_cents").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
                "amount_refunded_cents": item.get("amount_refunded_cents").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()),
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
        .table_name(&state.config.events_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&format!("INVOICE#{invoice_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to fetch invoice: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Try DynamoDB first, fall back to Stripe API
    let pdf_url = result
        .item()
        .and_then(|i| i.get("stripe_pdf_url"))
        .and_then(|v| v.as_s().ok())
        .filter(|u| !u.is_empty())
        .map(|s| s.to_string());

    if let Some(ref url) = pdf_url {
        if url.starts_with("https://pay.stripe.com/")
            || url.starts_with("https://invoice.stripe.com/")
        {
            return Ok(Json(json!({ "pdf_url": url })));
        }
    }

    // Fallback: fetch PDF URL from Stripe API directly
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let resp = state
        .http
        .get(format!("https://api.stripe.com/v1/invoices/{invoice_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe invoice fetch failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = resp.json().await.map_err(|e| {
        error!("Failed to parse Stripe invoice response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let stripe_pdf = body["invoice_pdf"]
        .as_str()
        .filter(|u| !u.is_empty())
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(json!({ "pdf_url": stripe_pdf })))
}

// ─── Helpers ────────────────────────────────────────────────────────

/// POST /api/billing/cancel — cancel the current subscription.
/// Body: { "immediately": true } to cancel now, or omit/false to cancel at period end.
pub async fn cancel_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, StatusCode> {
    enforce_billing_cooldown(&state, &claims.tenant_id, "cancel").await?;

    let immediately = body
        .as_ref()
        .and_then(|b| b["immediately"].as_bool())
        .unwrap_or(false);

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

    if immediately {
        // Cancel immediately — deletes the subscription
        let response = state
            .http
            .delete(format!("https://api.stripe.com/v1/subscriptions/{sub_id}"))
            .header("Authorization", format!("Bearer {stripe_key}"))
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

        // Update DynamoDB immediately so UI reflects the change without waiting for webhook
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression(
                "SET subscription_status = :status, cancelled_at = :t, \
                 access_until = :end, updated_at = :t, previous_status = :prev",
            )
            .expression_attribute_values(":status", attr_s("free"))
            .expression_attribute_values(":prev", attr_s("cancelled"))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":end", attr_s("now"))
            .send()
            .await;

        Ok(Json(json!({ "status": "cancelled" })))
    } else {
        // Cancel at period end
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

        // Store access_until so UI shows "Cancelling" immediately
        let access_until = body["current_period_end"]
            .as_u64()
            .filter(|&ts| ts > 0)
            .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, 0))
            .map(|dt| dt.format("%B %d, %Y").to_string())
            .unwrap_or_else(|| "the end of your current billing period".to_string());
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.table_name)
            .key("pk", attr_s(&claims.tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression("SET access_until = :end, updated_at = :t")
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":end", attr_s(&access_until))
            .send()
            .await;

        Ok(Json(json!({ "status": "cancelling" })))
    }
}

/// POST /api/billing/reactivate — undo a pending cancellation.
pub async fn reactivate_subscription(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    enforce_billing_cooldown(&state, &claims.tenant_id, "reactivate").await?;

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

/// GET /api/billing/payment-methods — list payment methods for this customer.
pub async fn list_payment_methods(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    // Get customer to find default PM
    let cust_resp = state
        .http
        .get(format!("https://api.stripe.com/v1/customers/{customer_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .ok();
    let cust_body: Option<Value> = match cust_resp {
        Some(r) => r.json().await.ok(),
        None => None,
    };
    let default_pm = cust_body
        .as_ref()
        .and_then(|c| {
            c["invoice_settings"]["default_payment_method"]
                .as_str()
                .filter(|s| !s.is_empty())
        })
        .map(|s| s.to_string())
        .unwrap_or_default();

    let response = state
        .http
        .get(format!(
            "https://api.stripe.com/v1/customers/{customer_id}/payment_methods"
        ))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe list payment methods failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let methods: Vec<Value> = body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|pm| {
                    let pm_type = pm["type"].as_str().unwrap_or("unknown");
                    let pm_id = pm["id"].as_str().unwrap_or("");
                    let mut result = json!({
                        "id": pm_id,
                        "type": pm_type,
                        "is_default": pm_id == default_pm,
                    });
                    if pm_type == "card" {
                        result["card"] = json!({
                            "brand": pm["card"]["brand"].as_str(),
                            "last4": pm["card"]["last4"].as_str(),
                            "exp_month": pm["card"]["exp_month"].as_u64(),
                            "exp_year": pm["card"]["exp_year"].as_u64(),
                        });
                    } else if pm_type == "us_bank_account" {
                        result["us_bank_account"] = json!({
                            "bank_name": pm["us_bank_account"]["bank_name"].as_str(),
                            "last4": pm["us_bank_account"]["last4"].as_str(),
                            "account_type": pm["us_bank_account"]["account_type"].as_str(),
                        });
                    }
                    result
                })
                .collect()
        })
        .unwrap_or_default();

    // Auto-set default: if exactly one method and no default is set, make it default
    if methods.len() == 1 && default_pm.is_empty() {
        if let Some(pm_id) = methods[0]["id"].as_str() {
            let _ = state
                .http
                .post(format!("https://api.stripe.com/v1/customers/{customer_id}"))
                .header("Authorization", format!("Bearer {stripe_key}"))
                .form(&[("invoice_settings[default_payment_method]", pm_id)])
                .send()
                .await;
            // Patch the response so UI sees it as default immediately
            if let Some(first) = methods.first() {
                let mut patched = methods.clone();
                patched[0] = {
                    let mut m = first.clone();
                    m["is_default"] = json!(true);
                    m
                };
                return Ok(Json(
                    json!({ "payment_methods": patched, "default_payment_method": pm_id }),
                ));
            }
        }
    }

    Ok(Json(
        json!({ "payment_methods": methods, "default_payment_method": if default_pm.is_empty() { Value::Null } else { json!(default_pm) } }),
    ))
}

/// PUT /api/billing/payment-methods/:pm_id/default — set a payment method as the default.
pub async fn set_default_payment_method(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(pm_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_stripe_id(&pm_id, "pm_")?;

    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    // Verify the payment method belongs to this customer
    let pm_response = state
        .http
        .get(format!("https://api.stripe.com/v1/payment_methods/{pm_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe get payment method failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let pm_body: Value = pm_response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let pm_customer = pm_body["customer"].as_str().unwrap_or("");
    if pm_customer != customer_id {
        return Err(StatusCode::FORBIDDEN);
    }

    // Update customer's default payment method
    let response = state
        .http
        .post(format!("https://api.stripe.com/v1/customers/{customer_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[("invoice_settings[default_payment_method]", pm_id.as_str())])
        .send()
        .await
        .map_err(|e| {
            error!("Failed to set default payment method: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    if !response.status().is_success() {
        error!("Stripe returned error setting default PM");
        return Err(StatusCode::BAD_GATEWAY);
    }

    info!(pm_id, "Default payment method updated");
    Ok(Json(json!({ "status": "ok" })))
}

/// DELETE /api/billing/payment-methods/:pm_id — detach a payment method from the customer.
pub async fn delete_payment_method(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(pm_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    validate_stripe_id(&pm_id, "pm_")?;

    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Verify the payment method belongs to this customer
    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    let pm_response = state
        .http
        .get(format!("https://api.stripe.com/v1/payment_methods/{pm_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe get payment method failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let pm_body: Value = pm_response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    let pm_customer = pm_body["customer"].as_str().unwrap_or("");
    if pm_customer != customer_id {
        warn!(
            "Payment method {pm_id} does not belong to customer {customer_id} (belongs to {pm_customer})"
        );
        return Err(StatusCode::FORBIDDEN);
    }

    // Detach the payment method
    let response = state
        .http
        .post(format!(
            "https://api.stripe.com/v1/payment_methods/{pm_id}/detach"
        ))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe detach payment method failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if body["error"].is_object() {
        error!(
            "Stripe error detaching payment method: {}",
            body["error"]["message"]
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    Ok(Json(json!({ "status": "detached" })))
}

/// PUT /api/billing/email — update the billing email on the Stripe customer.
pub async fn update_billing_email(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let email = body["email"]
        .as_str()
        .filter(|s| !s.is_empty() && s.contains('@'))
        .ok_or(StatusCode::BAD_REQUEST)?;

    let stripe_key = state.secrets.stripe_secret_key.as_deref().ok_or_else(|| {
        error!("Stripe secret key not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let customer_id = get_stripe_customer_id(&state, &claims.tenant_id).await?;

    let response = state
        .http
        .post(format!("https://api.stripe.com/v1/customers/{customer_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[("email", email)])
        .send()
        .await
        .map_err(|e| {
            error!("Stripe update customer email failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let resp_body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    if resp_body["error"].is_object() {
        error!(
            "Stripe error updating email: {}",
            resp_body["error"]["message"]
        );
        return Err(StatusCode::BAD_GATEWAY);
    }

    Ok(Json(
        json!({ "email": resp_body["email"].as_str().unwrap_or(email) }),
    ))
}

/// GET /api/billing/customer — get Stripe customer details (email, name).
pub async fn get_billing_customer(
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
        .get(format!("https://api.stripe.com/v1/customers/{customer_id}"))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await
        .map_err(|e| {
            error!("Stripe get customer failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let body: Value = response.json().await.map_err(|e| {
        error!("Failed to parse Stripe response: {e}");
        StatusCode::BAD_GATEWAY
    })?;

    Ok(Json(json!({
        "email": body["email"].as_str(),
        "name": body["name"].as_str(),
    })))
}

/// Check if tenant has an active subscription. Returns Ok(()) if active, 402 if not.
pub async fn require_active_subscription(
    state: &AppState,
    tenant_id: &str,
) -> Result<(), StatusCode> {
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

    let status = result
        .item()
        .and_then(|item| item.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("none");

    match status {
        "active" | "free" => Ok(()),
        _ => {
            warn!("Tenant {tenant_id} blocked: subscription_status={status}");
            Err(StatusCode::PAYMENT_REQUIRED)
        }
    }
}

/// Like require_active_subscription but excludes free-tier accounts.
pub async fn require_paid_subscription(
    state: &AppState,
    tenant_id: &str,
) -> Result<(), StatusCode> {
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

    let status = result
        .item()
        .and_then(|item| item.get("subscription_status"))
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("none");

    match status {
        "active" => Ok(()),
        _ => {
            warn!("Tenant {tenant_id} requires paid plan: subscription_status={status}");
            Err(StatusCode::PAYMENT_REQUIRED)
        }
    }
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
