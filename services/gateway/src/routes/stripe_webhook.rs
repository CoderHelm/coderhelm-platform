use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::AppState;

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

        // Refund processed
        "charge.refunded" => handle_charge_refunded(&state, &event["data"]["object"]).await,

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

    // Record payment in events table
    state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&tenant_id))
        .item("sk", attr_s(&format!("PAYMENT#{invoice_id}")))
        .item("invoice_number", attr_s(invoice_number))
        .item("amount_cents", attr_n(amount_cents))
        .item("status", attr_s("paid"))
        .item("stripe_invoice_id", attr_s(invoice_id))
        .item("created_at", attr_s(&now.to_rfc3339()))
        .send()
        .await?;

    // Reset retry counter on success and store subscription ID
    let sub_id = invoice["subscription"].as_str().unwrap_or("");
    let mut update_expr = "SET last_payment_at = :t, payment_retry_count = :zero, \
             subscription_status = :status, updated_at = :t"
        .to_string();
    if !sub_id.is_empty() {
        update_expr.push_str(", stripe_subscription_id = :sid");
    }
    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.billing_table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(&update_expr)
        .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
        .expression_attribute_values(":zero", attr_n(0))
        .expression_attribute_values(":status", attr_s("active"));
    if !sub_id.is_empty() {
        update = update.expression_attribute_values(":sid", attr_s(sub_id));
    }
    update.send().await?;

    // Get card details for email
    let card_last4 = invoice["default_payment_method"]["card"]["last4"]
        .as_str()
        .or_else(|| invoice["charge"]["payment_method_details"]["card"]["last4"].as_str())
        .unwrap_or("****");

    let plan_name = invoice["lines"]["data"][0]["description"]
        .as_str()
        .unwrap_or("Coderhelm Pro");

    // Send combined payment receipt + invoice email
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
            "invoice_url": format!("https://app.coderhelm.com/dashboard/billing/invoices/{}", invoice_id),
        }),
    )
    .await;

    // Update invoice record status to paid
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.events_table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s(&format!("INVOICE#{invoice_id}")))
        .update_expression("SET #s = :s, paid_at = :t")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s("paid"))
        .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
        .send()
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
        .table_name(&state.config.billing_table_name)
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
    let cancelled_sub_id = subscription["id"].as_str().unwrap_or("");

    // Only cancel if this is the CURRENT subscription — ignore stale/old subscription deletions
    if !cancelled_sub_id.is_empty() {
        let billing_item = state
            .dynamo
            .get_item()
            .table_name(&state.config.billing_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s("BILLING"))
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned());

        let current_sub_id = billing_item
            .as_ref()
            .and_then(|item| item.get("stripe_subscription_id"))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        let current_status = billing_item
            .as_ref()
            .and_then(|item| item.get("subscription_status"))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or("");

        // Skip if: (a) we have a different sub_id stored (stale deletion), OR
        // (b) the tenant is already on free/incomplete — nothing to cancel
        if !current_sub_id.is_empty() && current_sub_id != cancelled_sub_id {
            info!(
                tenant_id,
                cancelled_sub_id,
                current_sub_id,
                "Ignoring subscription.deleted for old/stale subscription"
            );
            return Ok(());
        }

        if matches!(current_status, "free" | "incomplete_expired" | "incomplete") {
            info!(
                tenant_id,
                cancelled_sub_id,
                current_status,
                "Ignoring subscription.deleted — tenant already inactive"
            );
            return Ok(());
        }
    }

    // current_period_end = when access expires
    let now_ts = chrono::Utc::now().timestamp() as u64;
    let period_end_ts = subscription["current_period_end"].as_u64().unwrap_or(0);
    // If period end is 0 (missing), in the past, or within 60s of now → immediate cancellation
    let immediate = period_end_ts == 0 || period_end_ts <= now_ts + 60;

    let access_until = if immediate {
        "now".to_string()
    } else {
        chrono::DateTime::from_timestamp(period_end_ts as i64, 0)
            .map(|dt| dt.format("%B %d, %Y").to_string())
            .unwrap_or_else(|| "the end of your current billing period".to_string())
    };

    state
        .dynamo
        .update_item()
        .table_name(&state.config.billing_table_name)
        .key("pk", attr_s(&tenant_id))
        .key("sk", attr_s("BILLING"))
        .update_expression(
            "SET subscription_status = :status, cancelled_at = :t, \
             access_until = :end, updated_at = :t, previous_status = :prev",
        )
        .expression_attribute_values(":status", attr_s("free"))
        .expression_attribute_values(":prev", attr_s("cancelled"))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":end", attr_s(&access_until))
        .send()
        .await?;

    // Send cancellation email — use different template for immediate vs period-end
    let template = if immediate {
        "subscription-cancelled-immediately"
    } else {
        "subscription-cancelled"
    };
    send_billing_email(
        state,
        &tenant_id,
        template,
        &serde_json::json!({ "access_until": access_until, "immediate": immediate }),
    )
    .await;

    info!(tenant_id, access_until, immediate, "Subscription cancelled");
    Ok(())
}

async fn handle_subscription_updated(
    state: &AppState,
    subscription: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = subscription["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let status = subscription["status"].as_str().unwrap_or("unknown");
    let sub_id = subscription["id"].as_str().unwrap_or("");
    let plan_id = subscription["items"]["data"][0]["price"]["id"]
        .as_str()
        .unwrap_or("");

    let cancel_at_period_end = subscription["cancel_at_period_end"]
        .as_bool()
        .unwrap_or(false);
    let now = chrono::Utc::now().to_rfc3339();

    if status == "active" && cancel_at_period_end {
        // Cancelling at period end — record when access expires
        let access_until = subscription["cancel_at"]
            .as_u64()
            .filter(|&ts| ts > 0)
            .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, 0))
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        state
            .dynamo
            .update_item()
            .table_name(&state.config.billing_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression(
                "SET subscription_status = :status, plan_id = :plan, \
                 access_until = :end, updated_at = :t, stripe_subscription_id = :sid",
            )
            .expression_attribute_values(":status", attr_s(status))
            .expression_attribute_values(":plan", attr_s(plan_id))
            .expression_attribute_values(":end", attr_s(&access_until))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":sid", attr_s(sub_id))
            .send()
            .await?;
    } else if status == "active" {
        // Reactivated or normal update — clear cancellation fields
        state
            .dynamo
            .update_item()
            .table_name(&state.config.billing_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression(
                "SET subscription_status = :status, plan_id = :plan, \
                 updated_at = :t, stripe_subscription_id = :sid \
                 REMOVE access_until, cancelled_at",
            )
            .expression_attribute_values(":status", attr_s(status))
            .expression_attribute_values(":plan", attr_s(plan_id))
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":sid", attr_s(sub_id))
            .send()
            .await?;
    } else {
        // Non-active status (incomplete, incomplete_expired, past_due, etc.)
        // Only store sub_id if it's a recoverable state (past_due) — NOT for terminal states
        // that could overwrite a valid active subscription's ID
        let is_terminal = matches!(status, "incomplete_expired" | "canceled");
        let update_expr = if is_terminal {
            "SET subscription_status = :status, plan_id = :plan, updated_at = :t"
        } else {
            "SET subscription_status = :status, plan_id = :plan, \
                 updated_at = :t, stripe_subscription_id = :sid"
        };
        let mut update = state
            .dynamo
            .update_item()
            .table_name(&state.config.billing_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s("BILLING"))
            .update_expression(update_expr)
            .expression_attribute_values(":status", attr_s(status))
            .expression_attribute_values(":plan", attr_s(plan_id))
            .expression_attribute_values(":t", attr_s(&now));
        if !is_terminal {
            update = update.expression_attribute_values(":sid", attr_s(sub_id));
        }
        update.send().await?;
    }

    info!(
        tenant_id,
        status, plan_id, cancel_at_period_end, "Subscription updated"
    );
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

    // Store invoice record
    state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
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

    // NOTE: Overage meter events are reported incrementally per run by the worker.
    // No reporting here — invoice.finalized is too late to affect the current invoice.

    // Invoice email is NOT sent here — it's sent from handle_payment_succeeded
    // so users only receive the email once payment is confirmed.

    info!(
        tenant_id,
        invoice_number, amount, "Invoice finalized (record stored, email deferred to payment)"
    );
    Ok(())
}

async fn handle_charge_refunded(
    state: &AppState,
    charge: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let customer_id = charge["customer"].as_str().unwrap_or("");
    let tenant_id = resolve_tenant(state, customer_id).await?;
    let charge_id = charge["id"].as_str().unwrap_or("");
    let invoice_id = charge["invoice"].as_str().unwrap_or("");
    let amount_refunded_cents = charge["amount_refunded"].as_u64().unwrap_or(0);
    let amount_refunded = format!("{:.2}", amount_refunded_cents as f64 / 100.0);

    let now = chrono::Utc::now();

    // Store refund record
    state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", attr_s(&tenant_id))
        .item("sk", attr_s(&format!("REFUND#{charge_id}")))
        .item("amount_cents", attr_n(amount_refunded_cents))
        .item("stripe_charge_id", attr_s(charge_id))
        .item("stripe_invoice_id", attr_s(invoice_id))
        .item("status", attr_s("refunded"))
        .item("created_at", attr_s(&now.to_rfc3339()))
        .send()
        .await?;

    // Update the corresponding invoice/payment record status if we have the invoice_id
    if !invoice_id.is_empty() {
        let amount_total = charge["amount"].as_u64().unwrap_or(0);
        let new_status = if amount_refunded_cents >= amount_total {
            "refunded"
        } else {
            "partially_refunded"
        };

        // Update invoice record
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.events_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s(&format!("INVOICE#{invoice_id}")))
            .update_expression("SET #s = :s, refunded_at = :t, amount_refunded_cents = :r")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr_s(new_status))
            .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
            .expression_attribute_values(":r", attr_n(amount_refunded_cents))
            .send()
            .await;

        // Update payment record
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.events_table_name)
            .key("pk", attr_s(&tenant_id))
            .key("sk", attr_s(&format!("PAYMENT#{invoice_id}")))
            .update_expression("SET #s = :s, refunded_at = :t, amount_refunded_cents = :r")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr_s(new_status))
            .expression_attribute_values(":t", attr_s(&now.to_rfc3339()))
            .expression_attribute_values(":r", attr_n(amount_refunded_cents))
            .send()
            .await;
    }

    // Send refund notification email
    send_billing_email(
        state,
        &tenant_id,
        "refund-processed",
        &serde_json::json!({
            "amount": amount_refunded,
            "date": now.format("%B %d, %Y").to_string(),
        }),
    )
    .await;

    info!(tenant_id, amount_refunded, charge_id, "Charge refunded");
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
        .table_name(&state.config.events_table_name)
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
        .table_name(&state.config.users_table_name)
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
        .table_name(&state.config.events_table_name)
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
        .table_name(&state.config.events_table_name)
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
