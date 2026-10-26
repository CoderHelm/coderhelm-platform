use aws_sdk_sesv2::types::{Destination, EmailContent, Template};
use serde_json::json;
use tracing::{info, warn};

use crate::WorkerState;

/// Notification event types that trigger emails.
pub enum EmailEvent {
    TokenWarning80 {
        month: String,
        /// Which limit: "new tokens", "cache read tokens", "cache write
        /// tokens", or "token" for the old single limit.
        limit_name: String,
        used_tokens: String,
        limit_tokens: String,
        usage_pct: u8,
    },
    TokenLimitReached {
        month: String,
        limit_name: String,
        used_tokens: String,
        limit_tokens: String,
    },
    /// A webhook endpoint failed for days and was turned off. Goes to the
    /// team's admins and owners only.
    WebhookDisabled {
        url: String,
        host: String,
        description: String,
        failing_since: String,
        days: i64,
    },
}

impl EmailEvent {
    /// Only admins and owners get it.
    fn admins_only(&self) -> bool {
        matches!(self, EmailEvent::WebhookDisabled { .. })
    }
}

/// Send a templated email to all users of a team who have the notification enabled.
///
/// 1. Queries all USER# records under the team to get emails.
/// 2. For each user, checks their NOTIFICATIONS# preferences.
/// 3. Sends via SES templated email only if the preference is enabled.
pub async fn send_notification(
    state: &WorkerState,
    team_id: &str,
    event: EmailEvent,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (template_suffix, pref_field, template_data) = match &event {
        EmailEvent::TokenWarning80 {
            month,
            limit_name,
            used_tokens,
            limit_tokens,
            usage_pct,
        } => (
            "token-warning",
            "email_token_warning",
            json!({
                "month": month,
                "limit_name": limit_name,
                "used_tokens": used_tokens,
                "limit_tokens": limit_tokens,
                "usage_pct": usage_pct,
            })
            .to_string(),
        ),
        EmailEvent::TokenLimitReached {
            month,
            limit_name,
            used_tokens,
            limit_tokens,
        } => (
            "token-limit-reached",
            "email_token_warning",
            json!({
                "month": month,
                "limit_name": limit_name,
                "used_tokens": used_tokens,
                "limit_tokens": limit_tokens,
            })
            .to_string(),
        ),
        EmailEvent::WebhookDisabled {
            url,
            host,
            description,
            failing_since,
            days,
        } => (
            "webhook-disabled",
            "",
            json!({
                "url": url,
                "host": host,
                "description": description,
                "failing_since": failing_since,
                "days": days,
            })
            .to_string(),
        ),
    };

    let template_name = format!("{}-{}", state.config.ses_template_prefix, template_suffix);

    // Get all users under this team
    let users = state
        .dynamo
        .query()
        .table_name(&state.config.users_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .expression_attribute_values(
            ":prefix",
            aws_sdk_dynamodb::types::AttributeValue::S("USER#".to_string()),
        )
        .send()
        .await?;

    for user_item in users.items() {
        let email = match user_item.get("email").and_then(|v| v.as_s().ok()) {
            Some(e) if !e.is_empty() => e.clone(),
            _ => continue,
        };

        let user_id = match user_item.get("sk").and_then(|v| v.as_s().ok()) {
            Some(id) => id.clone(),
            None => continue,
        };
        if event.admins_only() {
            let role = user_item
                .get("role")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str)
                .unwrap_or("");
            if !matches!(role, "admin" | "owner") {
                continue;
            }
        }

        // Check notification preferences (skip for welcome emails — always send)
        if !pref_field.is_empty() {
            let pref_sk = format!("NOTIFICATIONS#{user_id}");
            let prefs = state
                .dynamo
                .get_item()
                .table_name(&state.config.settings_table_name)
                .key(
                    "pk",
                    aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
                )
                .key("sk", aws_sdk_dynamodb::types::AttributeValue::S(pref_sk))
                .send()
                .await?;

            let enabled = prefs
                .item()
                .and_then(|item| item.get(pref_field))
                .and_then(|v| v.as_bool().ok())
                .unwrap_or(&true); // default: notifications ON

            if !enabled {
                info!(email = %email, pref = pref_field, "Skipping — user opted out");
                continue;
            }
        }

        // Send templated email
        let result = state
            .ses
            .send_email()
            .from_email_address(&state.config.ses_from_address)
            .destination(Destination::builder().to_addresses(&email).build())
            .content(
                EmailContent::builder()
                    .template(
                        Template::builder()
                            .template_name(&template_name)
                            .template_data(&template_data)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;

        let outcome = result
            .as_ref()
            .map(|_| ())
            .map_err(|e| aws_sdk_sesv2::error::DisplayErrorContext(e).to_string());
        common::deliveries::record(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &common::deliveries::email(template_suffix, &email, &outcome),
        )
        .await;
        match result {
            Ok(_) => info!(email = %email, template = %template_name, "Email sent"),
            Err(e) => warn!(email = %email, error = %e, "Failed to send email — continuing"),
        }
    }

    Ok(())
}
