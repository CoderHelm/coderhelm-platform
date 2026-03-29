use tracing::warn;

use crate::WorkerState;

/// Included tokens in the Pro plan (5M in+out).
const INCLUDED_TOKENS: u64 = 5_000_000;

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
        .table_name(&state.config.table_name)
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
        .key(
            "period",
            aws_sdk_dynamodb::types::AttributeValue::S(month),
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

    // Calculate what overage was already reported vs. what's new from this run
    let prev_total = total_tokens.saturating_sub(tokens_used);
    let prev_overage = prev_total.saturating_sub(INCLUDED_TOKENS);
    let current_overage = total_tokens.saturating_sub(INCLUDED_TOKENS);
    let new_overage = current_overage.saturating_sub(prev_overage);

    if new_overage == 0 {
        return;
    }

    // Report in units of 1K tokens (round up)
    let overage_1k = (new_overage + 999) / 1000;
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
