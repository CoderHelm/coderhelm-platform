use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::AppState;

use super::billing::{report_stripe_usage, INCLUDED_TOKENS};

/// Stripe webhook handler — processes payment events.
/// Stripe automatically retries failed webhook deliveries with exponential backoff.
pub async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // Verify Stripe signature
    let signature = match headers
        .get("stripe-signature")
        .and_then(|v| v.to_str().ok())
    {
        Some(sig) => sig.to_string(),
        None => {
            warn!("Missing Stripe signature header");
            return StatusCode::UNAUTHORIZED;
        }
    };

    let webhook_secret = match &state.secrets.stripe_webhook_secret {
        Some(s) => s.clone(),
        None => {
            error!("Stripe webhook secret not configured");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    // Verify HMAC-SHA256 signature (Stripe v1 scheme)
    if !verify_stripe_signature(body_str, &signature, &webhook_secret) {
        warn!("Invalid Stripe webhook signature");
        return StatusCode::UNAUTHORIZED;
    }

    let event: Value = match serde_json::from_str(body_str) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to parse Stripe event: {e}");
            return StatusCode::BAD_REQUEST;
        }
    };

    let event_type = event["type"].as_str().unwrap_or("unknown");
    let event_id = event["id"].as_str().unwrap_or("unknown");

    info!(event_type, event_id, "Stripe webhook received");

    // Idempotency: check if we've already processed this event
    if is_event_processed(&state, event_id).await {
        info!(event_id, "Stripe event already processed, skipping");
        return StatusCode::OK;
    }

    let result = match event_type {
        // Subscription activated (first payment succeeded)
        // With Stripe Elements, we no longer use checkout.session.completed.
        // The subscription starts as "incomplete" and becomes "active" when the
        // first invoice.payment_succeeded fires — handled below.

        // Successful payment
        "invoice.payment_succeeded" => {
            handle_payment_succeeded(&state, &event["data"]["object"]).await
        }

        // Payment failed — Stripe has built-in smart retries, we just notify
        "invoice.payment_failed" => handle_payment_failed(&state, &event["data"]["object"]).await,

        // Subscription cancelled (by user or non-payment)
        "customer.subscription.deleted" => {
            handle_subscription_cancelled(&state, &event["data"]["object"]).await
        }

        // Subscription updated (plan change, card change, etc.)
        "customer.subscription.updated" => {
            handle_subscription_updated(&state, &event["data"]["object"]).await
        }

        // Invoice finalized — ready for download
        "invoice.finalized" => handle_invoice_finalized(&state, &event["data"]["object"]).await,

        // Payment method updated
        "payment_method.attached" | "customer.updated" => {
            info!(event_type, "Payment method event — no action needed");
            Ok(())
        }

        _ => {
            info!(event_type, "Ignoring unhandled Stripe event");
            Ok(())
        }
    };

    // Mark event as processed (idempotency)
    mark_event_processed(&state, event_id).await;

    match result {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            error!(event_type, error = %e, "Failed to process Stripe event");
            // Return 500 so Stripe retries
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

// ─── Event handlers ─────────────────────────────────────────────────

// checkout.session.completed handler removed — subscription activation now handled
// by invoice.payment_succeeded + subscription_status transition from "incomplete" → "active".

async fn handle_payment_succeeded(
    state: &AppState,
    invoice: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = invoice["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let amount_cents = invoice["amount_paid"].as_u64().unwrap_or(0);
    let amount = format!("{:.2}", amount_cents as f64 / 100.0);
    let invoice_number = invoice["number"].as_str().unwrap_or("N/A");
    let invoice_id = invoice["id"].as_str().unwrap_or("");

    let now = chrono::Utc::now();

    // Record payment in main table
    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&tenant_id))
        .item("sk", attr_s(&format!("PAYMENT#{invoice_id}")))
        .item("invoice_number", attr_s(invoice_number))
        .item("amount_cents", attr_n(amount_cents))
        .item("status", attr_s("paid"))
        .item("stripe_invoice_id", attr_s(invoice_id))
        .item("created_at", attr_s(&now.to_rfc3339()))
        .send()
        .await?;

    // Reset retry counter on success
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(
            "SET last_payment_at = :t, payment_retry_count = :zero, \
             subscription_status = :status, updated_at = :t",
        )
        .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
        .expression_attribute_values(":zero", attr_n(0))
        .expression_attribute_values(":status", attr_s("active"))
        .send()
        .await?;

    // Get card details for email
    let card_last4 = invoice["default_payment_method"]["card"]["last4"]
        .as_str()
        .or_else(|| invoice["charge"]["payment_method_details"]["card"]["last4"].as_str())
        .unwrap_or("****");

    let plan_name = invoice["lines"]["data"][0]["description"]
        .as_str()
        .unwrap_or("d3ftly Pro");

    // Send payment receipt email
    send_billing_email(
        state,
        &tenant_id,
        "payment-receipt",
        &serde_json::json!({
            "amount": amount,
            "invoice_number": invoice_number,
            "date": now.format("%B %d, %Y").to_string(),
            "plan_name": plan_name,
            "card_last4": card_last4,
            "invoice_url": format!("https://app.d3ftly.com/dashboard/billing/invoices/{}", invoice_id),
        }),
    )
    .await;

    info!(tenant_id, amount, invoice_number, "Payment succeeded");
    Ok(())
}

async fn handle_payment_failed(
    state: &AppState,
    invoice: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = invoice["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let amount_cents = invoice["amount_due"].as_u64().unwrap_or(0);
    let amount = format!("{:.2}", amount_cents as f64 / 100.0);
    let attempt_count = invoice["attempt_count"].as_u64().unwrap_or(1);

    // Stripe uses smart retries: ~1 day, ~3 days, ~5 days, ~7 days
    let next_retry = match attempt_count {
        1 => "1 day",
        2 => "3 days",
        3 => "5 days",
        _ => "7 days (final attempt)",
    };

    let failure_reason = invoice["last_finalization_error"]["message"]
        .as_str()
        .or_else(|| invoice["status_transitions"]["finalized_at"].as_str())
        .unwrap_or("Your card was declined");

    let card_last4 = invoice["default_payment_method"]["card"]["last4"]
        .as_str()
        .unwrap_or("****");

    // Update billing record with retry count
    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(
            "SET payment_retry_count = :rc, subscription_status = :status, \
             last_failure_reason = :reason, updated_at = :t",
        )
        .expression_attribute_values(":rc", attr_n(attempt_count))
        .expression_attribute_values(":status", attr_s("past_due"))
        .expression_attribute_values(":reason", attr_s(failure_reason))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await?;

    // Send payment failed email
    send_billing_email(
        state,
        &tenant_id,
        "payment-failed",
        &serde_json::json!({
            "amount": amount,
            "card_last4": card_last4,
            "failure_reason": failure_reason,
            "next_retry": next_retry,
        }),
    )
    .await;

    warn!(tenant_id, amount, attempt_count, "Payment failed");
    Ok(())
}

async fn handle_subscription_cancelled(
    state: &AppState,
    subscription: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = subscription["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;

    // current_period_end = when access expires
    let period_end = subscription["current_period_end"].as_u64().unwrap_or(0);
    let access_until = chrono::DateTime::from_timestamp(period_end as i64, 0)
        .map(|dt| dt.format("%B %d, %Y").to_string())
        .unwrap_or_else(|| "immediately".to_string());

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(
            "SET subscription_status = :status, cancelled_at = :t, \
             access_until = :end, updated_at = :t",
        )
        .expression_attribute_values(":status", attr_s("cancelled"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":end", attr_s(&access_until))
        .send()
        .await?;

    // Send cancellation email
    send_billing_email(
        state,
        &tenant_id,
        "subscription-cancelled",
        &serde_json::json!({ "access_until": access_until }),
    )
    .await;

    info!(tenant_id, access_until, "Subscription cancelled");
    Ok(())
}

async fn handle_subscription_updated(
    state: &AppState,
    subscription: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = subscription["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let status = subscription["status"].as_str().unwrap_or("unknown");
    let plan_id = subscription["items"]["data"][0]["price"]["id"]
        .as_str()
        .unwrap_or("");

    state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression("SET subscription_status = :status, plan_id = :plan, updated_at = :t")
        .expression_attribute_values(":status", attr_s(status))
        .expression_attribute_values(":plan", attr_s(plan_id))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await?;

    info!(tenant_id, status, plan_id, "Subscription updated");
    Ok(())
}

async fn handle_invoice_finalized(
    state: &AppState,
    invoice: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = invoice["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let invoice_number = invoice["number"].as_str().unwrap_or("N/A");
    let invoice_id = invoice["id"].as_str().unwrap_or("");
    let amount_cents = invoice["amount_due"].as_u64().unwrap_or(0);
    let amount = format!("{:.2}", amount_cents as f64 / 100.0);

    let period_start = invoice["period_start"].as_u64().unwrap_or(0);
    let period_end = invoice["period_end"].as_u64().unwrap_or(0);

    let fmt_date = |ts: u64| {
        chrono::DateTime::from_timestamp(ts as i64, 0)
            .map(|dt| dt.format("%b %d, %Y").to_string())
            .unwrap_or_default()
    };

    let period = format!("{} — {}", fmt_date(period_start), fmt_date(period_end));
    let plan_name = invoice["lines"]["data"][0]["description"]
        .as_str()
        .unwrap_or("d3ftly Pro");

    // Store invoice record
    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s(&tenant_id))
        .item("sk", attr_s(&format!("INVOICE#{invoice_id}")))
        .item("invoice_number", attr_s(invoice_number))
        .item("amount_cents", attr_n(amount_cents))
        .item("period", attr_s(&period))
        .item("status", attr_s("finalized"))
        .item("stripe_invoice_id", attr_s(invoice_id))
        .item(
            "stripe_pdf_url",
            attr_s(invoice["invoice_pdf"].as_str().unwrap_or("")),
        )
        .item("created_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await?;

    // Get usage stats for the billing period
    let month = chrono::DateTime::from_timestamp(period_start as i64, 0)
        .map(|dt| dt.format("%Y-%m").to_string())
        .unwrap_or_default();

    let analytics = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key("tenant_id", attr_s(&tenant_id))
        .key("period", attr_s(&month))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

    let total_runs = analytics
        .as_ref()
        .and_then(|i| i.get("total_runs"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);

    let usage_cost = analytics
        .as_ref()
        .and_then(|i| i.get("total_cost_usd"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .unwrap_or(0.0);

    let total_tokens_in: u64 = analytics
        .as_ref()
        .and_then(|i| i.get("total_tokens_in"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let total_tokens_out: u64 = analytics
        .as_ref()
        .and_then(|i| i.get("total_tokens_out"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let total_tokens = total_tokens_in + total_tokens_out;

    // Report metered overages to Stripe before invoice is final
    let tokens_overage_1k = total_tokens.saturating_sub(INCLUDED_TOKENS) / 1000;
    report_stripe_usage(state, &tenant_id, "tokens_overage", tokens_overage_1k).await;

    // Send invoice email
    send_billing_email(
        state,
        &tenant_id,
        "invoice-ready",
        &serde_json::json!({
            "invoice_number": invoice_number,
            "amount": amount,
            "period": period,
            "plan_name": plan_name,
            "total_runs": total_runs,
            "usage_cost": format!("{:.2}", usage_cost),
            "invoice_url": format!("https://app.d3ftly.com/dashboard/billing/invoices/{}", invoice_id),
        }),
    )
    .await;

    info!(tenant_id, invoice_number, amount, "Invoice finalized");
    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────

/// Resolve tenant_id from Stripe customer_id using reverse mapping.
async fn resolve_tenant(
    state: &AppState,
    customer_id: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s(&format!("STRIPE#{customer_id}")))
        .key("sk", attr_s("MAPPING"))
        .send()
        .await?;

    result
        .item()
        .and_then(|item| item.get("tenant_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .ok_or_else(|| format!("No tenant found for Stripe customer {customer_id}").into())
}

/// Send a billing email to all users under a tenant.
async fn send_billing_email(
    state: &AppState,
    tenant_id: &str,
    template_suffix: &str,
    data: &Value,
) {
    let template_name = format!("{}-{}", state.config.ses_template_prefix, template_suffix);

    // Query all users for this tenant
    let users = match state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(tenant_id))
        .expression_attribute_values(":prefix", attr_s("USER#"))
        .projection_expression("email")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to query tenant users for billing email: {e}");
            return;
        }
    };

    for item in users.items() {
        let email = match item.get("email").and_then(|v| v.as_s().ok()) {
            Some(e) if !e.is_empty() => e.clone(),
            _ => continue,
        };

        let template_data = serde_json::to_string(data).unwrap_or_default();

        if let Err(e) = state
            .ses
            .send_email()
            .from_email_address(&state.config.ses_from_address)
            .destination(
                aws_sdk_sesv2::types::Destination::builder()
                    .to_addresses(&email)
                    .build(),
            )
            .content(
                aws_sdk_sesv2::types::EmailContent::builder()
                    .template(
                        aws_sdk_sesv2::types::Template::builder()
                            .template_name(&template_name)
                            .template_data(&template_data)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
        {
            error!(email, error = %e, "Failed to send billing email");
        }
    }
}

/// Check if a Stripe event has already been processed (idempotency).
async fn is_event_processed(state: &AppState, event_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", attr_s("STRIPE_EVENTS"))
        .key("sk", attr_s(event_id))
        .projection_expression("pk")
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .is_some()
}

/// Mark a Stripe event as processed. TTL of 48h to auto-cleanup.
async fn mark_event_processed(state: &AppState, event_id: &str) {
    let ttl = chrono::Utc::now().timestamp() + 172_800; // 48 hours
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", attr_s("STRIPE_EVENTS"))
        .item("sk", attr_s(event_id))
        .item("ttl", attr_n(ttl))
        .item("processed_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
}

/// Verify Stripe webhook signature (v1 HMAC-SHA256).
fn verify_stripe_signature(payload: &str, header: &str, secret: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // Parse header: t=TIMESTAMP,v1=SIG
    let mut timestamp = "";
    let mut signature = "";
    for part in header.split(',') {
        if let Some(t) = part.strip_prefix("t=") {
            timestamp = t;
        } else if let Some(s) = part.strip_prefix("v1=") {
            signature = s;
        }
    }

    if timestamp.is_empty() || signature.is_empty() {
        return false;
    }

    // Check timestamp tolerance (5 min)
    if let Ok(ts) = timestamp.parse::<i64>() {
        let now = chrono::Utc::now().timestamp();
        if (now - ts).unsigned_abs() > 300 {
            warn!("Stripe webhook timestamp too old");
            return false;
        }
    }

    // Compute expected signature
    let signed_payload = format!("{timestamp}.{payload}");
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(signed_payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    // Constant-time comparison
    subtle::ConstantTimeEq::ct_eq(expected.as_bytes(), signature.as_bytes()).into()
}

fn attr_s(val: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(val.to_string())
}

fn attr_n(val: impl std::fmt::Display) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(val.to_string())
}
