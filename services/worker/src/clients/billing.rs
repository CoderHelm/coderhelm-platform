use tracing::{info, warn};

use crate::WorkerState;

/// Included tokens in the Pro plan (5M in+out).
const INCLUDED_TOKENS: u64 = 5_000_000;

/// Price per 1K overage tokens in cents.
const OVERAGE_PER_1K_TOKENS_CENTS: u64 = 5;

/// Billing threshold in cents — create an invoice when unbilled overage exceeds this.
const BILLING_THRESHOLD_CENTS: u64 = 10_000;

/// Report token overage to Stripe via direct invoice items.
/// Creates and pays an invoice when unbilled overage exceeds the threshold ($100).
/// Budget enforcement (blocking new runs) is handled at the gateway, not here.
pub async fn report_token_overage(state: &WorkerState, team_id: &str, tokens_used: u64) {
    if tokens_used == 0 {
        return;
    }

    let stripe_key = match &state.secrets.stripe_secret_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return,
    };

    // Get the Stripe customer ID for this team
    let billing = match state
        .dynamo
        .get_item()
        .table_name(&state.config.billing_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("BILLING".to_string()),
        )
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("Failed to fetch billing for usage report: {e}");
            return;
        }
    };

    let item = match billing.item() {
        Some(i) => i,
        None => {
            info!("[billing] no BILLING record for {team_id}, skipping overage report");
            return;
        }
    };

    let customer_id = match item
        .get("stripe_customer_id")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
    {
        Some(id) => id.to_string(),
        None => {
            info!("[billing] no stripe_customer_id for {team_id}");
            return;
        }
    };

    let status = item
        .get("subscription_status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("none");

    if status != "active" {
        info!(
            "[billing] subscription not active ({status}) for {team_id}, skipping overage report"
        );
        return;
    }

    // Get current month's cumulative token usage
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let analytics = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "period",
            aws_sdk_dynamodb::types::AttributeValue::S(month.clone()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());

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

    // Only report overage tokens (tokens above the included limit)
    if total_tokens <= INCLUDED_TOKENS {
        return;
    }

    let current_overage = total_tokens.saturating_sub(INCLUDED_TOKENS);
    let cumulative_overage_1k = current_overage.div_ceil(1000);

    info!("[billing] overage check for {team_id}: {cumulative_overage_1k} x 1K tokens total");

    // Check unbilled overage and create invoice if threshold exceeded
    create_overage_invoice_if_needed(
        state,
        team_id,
        &customer_id,
        &stripe_key,
        cumulative_overage_1k,
        &month,
    )
    .await;
}

/// Create an invoice for unbilled overage when it exceeds the threshold.
/// Tracks `overage_billed_{month}` in the BILLING record per billing period.
/// Uses a conditional DynamoDB update to prevent double-billing from concurrent workers.
async fn create_overage_invoice_if_needed(
    state: &WorkerState,
    team_id: &str,
    customer_id: &str,
    stripe_key: &str,
    cumulative_overage_1k: u64,
    month: &str,
) {
    let billed_attr = format!("overage_billed_{}", month.replace('-', "_"));

    // Read how many 1K-units we've already billed this month
    let billing = match state
        .dynamo
        .get_item()
        .table_name(&state.config.billing_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("BILLING".to_string()),
        )
        .projection_expression("#b")
        .expression_attribute_names("#b", &billed_attr)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("[billing] Failed to read billed counter: {e}");
            return;
        }
    };

    let already_billed: u64 = billing
        .item()
        .and_then(|i| i.get(&billed_attr))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    let unbilled = cumulative_overage_1k.saturating_sub(already_billed);
    let unbilled_cents = unbilled * OVERAGE_PER_1K_TOKENS_CENTS;

    if unbilled_cents < BILLING_THRESHOLD_CENTS {
        info!(
            "[billing] unbilled overage ${:.2} below threshold for {team_id}",
            unbilled_cents as f64 / 100.0,
        );
        return;
    }

    info!(
        "[billing] unbilled overage {unbilled} units (${:.2}) exceeds threshold — claiming for {team_id}",
        unbilled_cents as f64 / 100.0
    );

    // Atomically claim these units BEFORE creating the invoice.
    // If another worker already updated the counter, the condition fails and we skip.
    let new_billed = already_billed + unbilled;
    let claim_result = state
        .dynamo
        .update_item()
        .table_name(&state.config.billing_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("BILLING".to_string()),
        )
        .update_expression("SET #b = :new_val")
        .condition_expression(if already_billed == 0 {
            "attribute_not_exists(#b) OR #b = :old_val"
        } else {
            "#b = :old_val"
        })
        .expression_attribute_names("#b", &billed_attr)
        .expression_attribute_values(
            ":new_val",
            aws_sdk_dynamodb::types::AttributeValue::N(new_billed.to_string()),
        )
        .expression_attribute_values(
            ":old_val",
            aws_sdk_dynamodb::types::AttributeValue::N(already_billed.to_string()),
        )
        .send()
        .await;

    if let Err(e) = &claim_result {
        // ConditionalCheckFailedException means another worker already claimed these units
        let err_str = format!("{e}");
        if err_str.contains("ConditionalCheckFailed") {
            info!("[billing] Another worker already claimed overage for {team_id}, skipping");
            return;
        }
        warn!("[billing] Failed to claim overage units: {e}");
        return;
    }

    // Units claimed — now create and pay the invoice.
    // If invoice creation fails, roll back the counter so units can be billed next time.

    // Create an invoice item for the unbilled overage
    let amount = (unbilled * OVERAGE_PER_1K_TOKENS_CENTS).to_string();
    let description = format!(
        "Token overage: {} x 1K tokens @ ${:.2}/1K",
        unbilled,
        OVERAGE_PER_1K_TOKENS_CENTS as f64 / 100.0
    );
    let ii_resp = state
        .http
        .post("https://api.stripe.com/v1/invoiceitems")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("customer", customer_id),
            ("amount", amount.as_str()),
            ("currency", "usd"),
            ("description", description.as_str()),
        ])
        .send()
        .await;

    match ii_resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            let body = r.text().await.unwrap_or_default();
            warn!("[billing] Invoice item creation failed: {body}");
            rollback_billed_counter(state, team_id, &billed_attr, already_billed).await;
            return;
        }
        Err(e) => {
            warn!("[billing] Failed to create invoice item: {e}");
            rollback_billed_counter(state, team_id, &billed_attr, already_billed).await;
            return;
        }
    }

    // Create invoice with pending items
    let inv_resp = state
        .http
        .post("https://api.stripe.com/v1/invoices")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("customer", customer_id),
            ("pending_invoice_items_behavior", "include"),
        ])
        .send()
        .await;

    let invoice_id = match inv_resp {
        Ok(r) if r.status().is_success() => {
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            match body["id"].as_str() {
                Some(id) => id.to_string(),
                None => {
                    warn!("[billing] Invoice created but no ID in response");
                    rollback_billed_counter(state, team_id, &billed_attr, already_billed).await;
                    return;
                }
            }
        }
        Ok(r) => {
            let body = r.text().await.unwrap_or_default();
            warn!("[billing] Invoice creation failed: {body}");
            rollback_billed_counter(state, team_id, &billed_attr, already_billed).await;
            return;
        }
        Err(e) => {
            warn!("[billing] Failed to create invoice: {e}");
            rollback_billed_counter(state, team_id, &billed_attr, already_billed).await;
            return;
        }
    };

    // Finalize
    let fin_resp = state
        .http
        .post(format!(
            "https://api.stripe.com/v1/invoices/{invoice_id}/finalize"
        ))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await;

    match fin_resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            let body = r.text().await.unwrap_or_default();
            warn!("[billing] Failed to finalize invoice {invoice_id}: {body}");
            // Don't rollback — invoice exists in Stripe, manual cleanup needed
            return;
        }
        Err(e) => {
            warn!("[billing] Failed to finalize invoice {invoice_id}: {e}");
            return;
        }
    }

    // Pay immediately
    let pay_resp = state
        .http
        .post(format!(
            "https://api.stripe.com/v1/invoices/{invoice_id}/pay"
        ))
        .header("Authorization", format!("Bearer {stripe_key}"))
        .send()
        .await;

    match pay_resp {
        Ok(r) if r.status().is_success() => {
            info!(
                "[billing] Invoice {invoice_id} paid: {unbilled} units (${:.2}) for {team_id}",
                unbilled_cents as f64 / 100.0
            );
        }
        Ok(r) => {
            let body = r.text().await.unwrap_or_default();
            warn!("[billing] Failed to pay invoice {invoice_id}: {body}");
            // Don't rollback — invoice is finalized, will retry via Stripe's own retry logic
        }
        Err(e) => {
            warn!("[billing] Failed to pay invoice {invoice_id}: {e}");
        }
    }
}

/// Roll back the billed counter to its previous value when invoice creation fails.
async fn rollback_billed_counter(
    state: &WorkerState,
    team_id: &str,
    billed_attr: &str,
    old_value: u64,
) {
    let update = if old_value == 0 {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.billing_table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S("BILLING".to_string()),
            )
            .update_expression("REMOVE #b")
            .expression_attribute_names("#b", billed_attr)
            .send()
            .await
    } else {
        state
            .dynamo
            .update_item()
            .table_name(&state.config.billing_table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S("BILLING".to_string()),
            )
            .update_expression("SET #b = :v")
            .expression_attribute_names("#b", billed_attr)
            .expression_attribute_values(
                ":v",
                aws_sdk_dynamodb::types::AttributeValue::N(old_value.to_string()),
            )
            .send()
            .await
    };
    if let Err(e) = update {
        warn!("[billing] Failed to rollback billed counter: {e}");
    }
}
