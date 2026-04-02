use tracing::warn;

use crate::WorkerState;

/// Included tokens in the Pro plan (1M in+out).
const INCLUDED_TOKENS: u64 = 1_000_000;

/// $10.00 per 1K overage tokens.
const OVERAGE_PER_1K_TOKENS_CENTS: u64 = 1000;

/// Report token overage to Stripe via Billing Meter Events.
/// Checks current month's cumulative usage against the included limit,
/// and only reports the overage portion.
pub async fn report_token_overage(state: &WorkerState, tenant_id: &str, tokens_used: u64) {
    if tokens_used == 0 {
        return;
    }

    let stripe_key = match &state.secrets.stripe_secret_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return,
    };

    // Get the Stripe customer ID for this tenant
    let billing = match state
        .dynamo
        .get_item()
        .table_name(&state.config.billing_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.to_string()),
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
        None => return,
    };

    let customer_id = match item
        .get("stripe_customer_id")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
    {
        Some(id) => id.to_string(),
        None => return,
    };

    let status = item
        .get("subscription_status")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("none");

    if status != "active" {
        return;
    }

    // Get current month's cumulative token usage
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    let analytics = state
        .dynamo
        .get_item()
        .table_name(&state.config.analytics_table_name)
        .key(
            "tenant_id",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.to_string()),
        )
        .key("period", aws_sdk_dynamodb::types::AttributeValue::S(month))
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

    // Check budget cap
    let budget = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#BUDGET".to_string()),
        )
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

    let current_overage = total_tokens.saturating_sub(INCLUDED_TOKENS);
    let current_overage_1k = current_overage / 1000;
    let current_overage_cents = current_overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;

    // If budget cap is set and we've already reported up to the cap, stop
    if max_budget_cents > 0 && current_overage_cents >= max_budget_cents {
        // Check if we already exceeded the cap before this run
        let prev_total = total_tokens.saturating_sub(tokens_used);
        let prev_overage_1k = prev_total.saturating_sub(INCLUDED_TOKENS) / 1000;
        let prev_overage_cents = prev_overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;
        if prev_overage_cents >= max_budget_cents {
            return; // Already at cap, nothing new to report
        }
    }

    // Calculate what overage was already reported vs. what's new from this run
    let prev_total = total_tokens.saturating_sub(tokens_used);
    let prev_overage = prev_total.saturating_sub(INCLUDED_TOKENS);
    let current_overage = total_tokens.saturating_sub(INCLUDED_TOKENS);
    let new_overage = current_overage.saturating_sub(prev_overage);

    if new_overage == 0 {
        return;
    }

    // Cap at budget limit if set
    let mut overage_1k = new_overage.div_ceil(1000);
    if max_budget_cents > 0 {
        let prev_overage_1k = prev_overage / 1000;
        let prev_reported_cents = prev_overage_1k * OVERAGE_PER_1K_TOKENS_CENTS;
        let remaining_budget = max_budget_cents.saturating_sub(prev_reported_cents);
        let max_reportable_1k = remaining_budget / OVERAGE_PER_1K_TOKENS_CENTS;
        overage_1k = overage_1k.min(max_reportable_1k);
        if overage_1k == 0 {
            return;
        }
    }
    let ts = chrono::Utc::now().timestamp().to_string();
    let qty = overage_1k.to_string();

    if let Err(e) = state
        .http
        .post("https://api.stripe.com/v1/billing/meter_events")
        .header("Authorization", format!("Bearer {stripe_key}"))
        .form(&[
            ("event_name", "coderhelm_token_overage"),
            ("timestamp", ts.as_str()),
            ("payload[value]", qty.as_str()),
            ("payload[stripe_customer_id]", customer_id.as_str()),
        ])
        .send()
        .await
    {
        warn!("Failed to report Stripe meter event: {e}");
    }
}
