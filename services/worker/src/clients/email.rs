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
    // The relay form: the same numbers, a fixed template (common::mail).
    let usage_link = format!(
        "{}/settings/usage/",
        common::public_urls::app_url_from_env()
    );
    let relay_params = |v: serde_json::Value| v.as_object().cloned().unwrap_or_default();
    let (relay_template, params) = match &event {
        EmailEvent::TokenWarning80 {
            month,
            limit_name,
            used_tokens,
            limit_tokens,
            usage_pct,
        } => (
            "token_warning",
            relay_params(json!({
                "month": month, "limit_name": limit_name, "used": used_tokens,
                "limit": limit_tokens, "pct": usage_pct, "link": usage_link,
            })),
        ),
        EmailEvent::TokenLimitReached {
            month,
            limit_name,
            used_tokens,
            limit_tokens,
        } => (
            "token_limit_reached",
            relay_params(json!({
                "month": month, "limit_name": limit_name, "used": used_tokens,
                "limit": limit_tokens, "link": usage_link,
            })),
        ),
        EmailEvent::WebhookDisabled { days, .. } => (
            "webhook_disabled",
            relay_params(json!({
                "days": days,
                "link": format!("{}/settings/webhooks/", common::public_urls::app_url_from_env()),
            })),
        ),
    };

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

        let mail = common::mail::Mail {
            kind: template_suffix.to_string(),
            to: email.clone(),
            template: relay_template,
            params: params.clone(),
            ses: common::mail::SesBody::Template {
                name: template_name.clone(),
                data: template_data.clone(),
            },
        };
        let result = common::mail::send(&sender(state), &mail).await;
        common::deliveries::record(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &common::mail::delivery(template_suffix, &email, &result),
        )
        .await;
        match result {
            Ok(_) => info!(template = %template_name, "Email sent"),
            Err(e) => warn!(code = %e.code, error = %e.message, "Failed to send email, continuing"),
        }
    }

    Ok(())
}

/// The worker's sender for `common::mail::send` (SES, or CoderHelm's relay
/// on a self-hosted install that chose it).
pub fn sender(state: &WorkerState) -> common::mail::Sender<'_> {
    common::mail::Sender {
        ses: &state.ses,
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        from: &state.config.ses_from_address,
    }
}
