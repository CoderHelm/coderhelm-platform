use aws_sdk_sesv2::types::{Destination, EmailContent, Template};
use serde_json::json;
use tracing::{info, warn};

use crate::WorkerState;

/// Notification event types that trigger emails.
pub enum EmailEvent {
    RunComplete {
        run_id: String,
        title: String,
        repo: String,
        pr_url: String,
        files_modified: usize,
        duration: String,
        tokens: String,
    },
    RunFailed {
        run_id: String,
        title: String,
        repo: String,
        error: String,
    },
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
        EmailEvent::RunComplete {
            run_id,
            title,
            repo,
            pr_url,
            files_modified,
            duration,
            tokens,
        } => (
            "run-complete",
            "email_run_complete",
            json!({
                "run_id": run_id,
                "title": title,
                "repo": repo,
                "pr_url": pr_url,
                "files_modified": files_modified,
                "duration": duration,
                "tokens": tokens,
            })
            .to_string(),
        ),
        EmailEvent::RunFailed {
            run_id,
            title,
            repo,
            error,
        } => (
            "run-failed",
            "email_run_failed",
            json!({
                "run_id": run_id,
                "title": title,
                "repo": repo,
                "error": error,
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

        match result {
            Ok(_) => info!(email = %email, template = %template_name, "Email sent"),
            Err(e) => warn!(email = %email, error = %e, "Failed to send email — continuing"),
        }
    }

    Ok(())
}
