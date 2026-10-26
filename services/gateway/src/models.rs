use serde::{Deserialize, Serialize};

/// The worker queue messages, shared with the worker (`common::messages`).
pub use common::messages::*;

/// App configuration from environment variables.
pub struct Config {
    pub stage: String,
    pub table_name: String,
    pub teams_table_name: String,
    pub runs_table_name: String,
    pub analytics_table_name: String,
    pub events_table_name: String,
    pub users_table_name: String,
    pub jira_tokens_table_name: String,
    pub jira_events_table_name: String,
    pub plans_table_name: String,
    pub jira_config_table_name: String,
    pub repos_table_name: String,
    pub settings_table_name: String,
    pub aws_insights_table_name: String,
    pub infra_table_name: String,
    pub banners_table_name: String,
    pub mcp_configs_table_name: String,
    pub waitlist_table_name: String,
    pub bucket_name: String,
    pub ticket_queue_url: String,
    /// Plugin schedules: the EventBridge Scheduler group, the role it sends
    /// with, and the tickets queue it sends to.
    pub plugin_schedule_group: String,
    pub plugin_scheduler_role_arn: String,
    pub ticket_queue_arn: String,
    pub ci_fix_queue_url: String,
    pub feedback_queue_url: String,
    /// Outbound webhook events and retries. Empty: nothing is emitted.
    pub webhook_queue_url: String,
    pub dlq_url: String,
    pub ses_from_address: String,
    pub ses_template_prefix: String,
    pub model_id: String,
    pub cognito_user_pool_id: String,
    pub cognito_client_id: String,
    pub cognito_domain: String,
    pub mcp_proxy_function_name: String,
    /// The worker, called directly for the plan chat's skills, agents and
    /// data source tools. Empty: the chat goes without them.
    pub worker_function_name: String,
    pub traces_table_name: String,
    /// KMS key that seals teams' data-source keys. Empty disables saving them.
    pub secrets_key_id: String,
    /// E2E results. Empty disables the feature.
    pub tests_table_name: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            stage: std::env::var("STAGE").unwrap_or_else(|_| "dev".to_string()),
            table_name: std::env::var("TABLE_NAME").expect("TABLE_NAME required"),
            teams_table_name: std::env::var("TEAMS_TABLE_NAME").expect("TEAMS_TABLE_NAME required"),
            runs_table_name: std::env::var("RUNS_TABLE_NAME").expect("RUNS_TABLE_NAME required"),
            analytics_table_name: std::env::var("ANALYTICS_TABLE_NAME")
                .expect("ANALYTICS_TABLE_NAME required"),
            events_table_name: std::env::var("EVENTS_TABLE_NAME")
                .expect("EVENTS_TABLE_NAME required"),
            users_table_name: std::env::var("USERS_TABLE_NAME").expect("USERS_TABLE_NAME required"),
            jira_tokens_table_name: std::env::var("JIRA_TOKENS_TABLE_NAME")
                .expect("JIRA_TOKENS_TABLE_NAME required"),
            jira_events_table_name: std::env::var("JIRA_EVENTS_TABLE_NAME")
                .expect("JIRA_EVENTS_TABLE_NAME required"),
            plans_table_name: std::env::var("PLANS_TABLE_NAME").expect("PLANS_TABLE_NAME required"),
            jira_config_table_name: std::env::var("JIRA_CONFIG_TABLE_NAME")
                .expect("JIRA_CONFIG_TABLE_NAME required"),
            repos_table_name: std::env::var("REPOS_TABLE_NAME").expect("REPOS_TABLE_NAME required"),
            settings_table_name: std::env::var("SETTINGS_TABLE_NAME")
                .expect("SETTINGS_TABLE_NAME required"),
            aws_insights_table_name: std::env::var("AWS_INSIGHTS_TABLE_NAME")
                .expect("AWS_INSIGHTS_TABLE_NAME required"),
            infra_table_name: std::env::var("INFRA_TABLE_NAME").expect("INFRA_TABLE_NAME required"),
            banners_table_name: std::env::var("BANNERS_TABLE_NAME")
                .expect("BANNERS_TABLE_NAME required"),
            mcp_configs_table_name: std::env::var("MCP_CONFIGS_TABLE_NAME").unwrap_or_default(),
            waitlist_table_name: std::env::var("WAITLIST_TABLE_NAME")
                .expect("WAITLIST_TABLE_NAME required"),
            bucket_name: std::env::var("BUCKET_NAME").expect("BUCKET_NAME required"),
            ticket_queue_url: std::env::var("TICKET_QUEUE_URL").expect("TICKET_QUEUE_URL required"),
            plugin_schedule_group: std::env::var("PLUGIN_SCHEDULE_GROUP").unwrap_or_default(),
            plugin_scheduler_role_arn: std::env::var("PLUGIN_SCHEDULER_ROLE_ARN")
                .unwrap_or_default(),
            ticket_queue_arn: std::env::var("TICKET_QUEUE_ARN").unwrap_or_default(),
            ci_fix_queue_url: std::env::var("CI_FIX_QUEUE_URL").expect("CI_FIX_QUEUE_URL required"),
            feedback_queue_url: std::env::var("FEEDBACK_QUEUE_URL")
                .expect("FEEDBACK_QUEUE_URL required"),
            webhook_queue_url: std::env::var("WEBHOOK_QUEUE_URL").unwrap_or_default(),
            dlq_url: std::env::var("DLQ_URL").unwrap_or_default(),
            ses_from_address: std::env::var("SES_FROM_ADDRESS")
                .unwrap_or_else(|_| "notifications@coderhelm.com".to_string()),
            ses_template_prefix: std::env::var("SES_TEMPLATE_PREFIX")
                .unwrap_or_else(|_| "coderhelm-prod".to_string()),
            model_id: std::env::var("MODEL_ID").unwrap_or_else(|_| "claude-sonnet-5-5".to_string()),
            cognito_user_pool_id: std::env::var("COGNITO_USER_POOL_ID")
                .expect("COGNITO_USER_POOL_ID required"),
            cognito_client_id: std::env::var("COGNITO_CLIENT_ID")
                .expect("COGNITO_CLIENT_ID required"),
            cognito_domain: std::env::var("COGNITO_DOMAIN").expect("COGNITO_DOMAIN required"),
            mcp_proxy_function_name: std::env::var("MCP_PROXY_FUNCTION_NAME").unwrap_or_default(),
            worker_function_name: std::env::var("WORKER_FUNCTION_NAME").unwrap_or_default(),
            traces_table_name: std::env::var("TRACES_TABLE_NAME").unwrap_or_default(),
            secrets_key_id: std::env::var("SECRETS_KEY_ID").unwrap_or_default(),
            tests_table_name: std::env::var("TESTS_TABLE_NAME").unwrap_or_default(),
        }
    }
}

/// Secrets loaded from AWS Secrets Manager.
#[derive(Deserialize)]
pub struct Secrets {
    /// The install's own GitHub App. Empty on a hybrid install that uses
    /// CoderHelm's app through the relay and the token broker.
    #[serde(default)]
    pub github_app_id: String,
    #[serde(default)]
    pub github_private_key: String,
    #[serde(default)]
    pub github_webhook_secret: String,
    #[serde(default)]
    pub github_client_id: String,
    #[serde(default)]
    pub github_client_secret: String,
    pub jwt_secret: String,
    #[serde(default)]
    pub google_client_id: Option<String>,
    #[serde(default)]
    pub google_client_secret: Option<String>,
    #[serde(default)]
    pub jira_webhook_secret: Option<String>,
    /// Knowledge sources: the install's own OAuth apps for one-click
    /// sign-in. Each is optional; without one, that source uses the team's
    /// saved key where it has one (docs/self-host/knowledge-sources.md).
    #[serde(default)]
    pub notion_client_id: Option<String>,
    #[serde(default)]
    pub notion_client_secret: Option<String>,
    /// The verification token of the install's Notion webhook subscription.
    #[serde(default)]
    pub notion_webhook_token: Option<String>,
    #[serde(default)]
    pub atlassian_client_id: Option<String>,
    #[serde(default)]
    pub atlassian_client_secret: Option<String>,
    /// Google Drive; falls back to `google_client_id` / `google_client_secret`
    /// (the Google sign-in app) when unset.
    #[serde(default)]
    pub google_drive_client_id: Option<String>,
    #[serde(default)]
    pub google_drive_client_secret: Option<String>,
    /// Hosted side of hybrid only: the key every install's registration
    /// secret and relay key are derived from (common::hybrid::sign). Absent
    /// on self-hosted installs, which then never serve `/hybrid/*`.
    #[serde(default)]
    pub hybrid_master_key: Option<String>,
    /// A hybrid install's registration with the hosted side (written by
    /// Settings, System, Hybrid; common::hybrid::client).
    #[serde(default)]
    pub hybrid_install_id: Option<String>,
    #[serde(default)]
    pub hybrid_install_secret: Option<String>,
    #[serde(default)]
    pub hybrid_relay_key: Option<String>,
}

impl Secrets {
    pub async fn load(
        client: &aws_sdk_secretsmanager::Client,
        name: &str,
    ) -> Result<Self, lambda_http::Error> {
        let response = client.get_secret_value().secret_id(name).send().await?;
        let secret_string = response
            .secret_string()
            .ok_or("Secret has no string value")?;
        let secrets: Secrets = serde_json::from_str(secret_string)?;
        Ok(secrets)
    }
}

/// JWT claims for authenticated dashboard sessions.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Claims {
    pub sub: String,     // user_id (USER#<cognito_sub> or USER#<github_id>)
    pub team_id: String, // TEAM#<id>
    pub email: String,
    pub role: String, // owner, admin, member, viewer
    #[serde(default)]
    pub github_login: Option<String>,
    #[serde(default = "default_aud")]
    pub aud: String,
    pub exp: u64,
    pub iat: u64,
    /// Set only on a support-view session: a Coderhelm platform admin reading
    /// one team's workspace, read-only, under that team's grant. Never set on
    /// a normal session token (the auth middleware drops it there).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_view: Option<SupportView>,
    /// Set by the auth middleware, never read from or written to a token: the
    /// member's custom role when they have one. `role` then holds the built-in
    /// role this request runs as (see `permissions::resolve_access`).
    #[serde(skip)]
    pub custom: Option<crate::permissions::Holder>,
    /// Set by the auth middleware on a request made with a team API key,
    /// never read from or written to a token. `sub` is then `APIKEY#<id>`,
    /// and `email` and `github_login` are those of the admin who made it.
    #[serde(skip)]
    pub api_key: Option<ApiKeyRef>,
}

/// The API key a request was made with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRef {
    pub id: String,
    pub name: String,
    /// What the key holds on this request (its scopes cut to its maker).
    pub scopes: Vec<String>,
    pub rate_limit_per_minute: u32,
    pub expires_at: Option<String>,
    pub created_by: String,
}

/// What a support-view session may see: one team, until a time, under one
/// grant. Revoking or replacing the grant ends the session.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SupportView {
    pub team_id: String,
    /// Unix seconds; the session ends then even if the grant runs longer.
    pub until: u64,
    pub grant_id: String,
    /// The platform admin's own user id; their own session must be present.
    pub admin_sub: String,
    pub admin_email: String,
}

fn default_aud() -> String {
    "coderhelm-dashboard".to_string()
}

impl Claims {
    /// Display name: prefer github_login, fall back to email.
    pub fn display_name(&self) -> String {
        self.github_login
            .clone()
            .unwrap_or_else(|| self.email.clone())
    }

    /// Numeric role level: owner=4, admin=3, billing=2, member=1, viewer=0.
    pub fn role_level(&self) -> u8 {
        crate::permissions::role_level(&self.role)
    }

    /// What this person holds: their custom role, or their built-in role.
    pub fn holder(&self) -> crate::permissions::Holder {
        self.custom
            .clone()
            .unwrap_or_else(|| crate::permissions::Holder::builtin(&self.role))
    }

    /// The role on their record (`custom:<id>` for a custom role), for
    /// anything that mints a token: never the role a request runs as.
    pub fn session_role(&self) -> &str {
        self.custom.as_ref().map_or(&self.role, |h| &h.role)
    }

    /// Require minimum role level, return 403 if insufficient.
    /// Levels: 1=member+, 2=billing+, 3=admin+, 4=owner only.
    pub fn require_role(&self, min_level: u8) -> Result<(), axum::http::StatusCode> {
        if self.role_level() >= min_level {
            Ok(())
        } else {
            Err(axum::http::StatusCode::FORBIDDEN)
        }
    }
}

/// DynamoDB item types.
#[derive(Serialize, Deserialize, Debug)]
#[allow(dead_code)]
pub struct Team {
    pub pk: String, // TEAM#<install_id>
    pub sk: String, // META
    pub github_install_id: u64,
    pub github_org: String,
    pub plan: String,       // "free" | "supporter"
    pub status: String,     // "active" | "suspended"
    pub run_count_mtd: u32, // month-to-date run count
    pub created_at: String,
}

/// Notification preferences (stored in main table: pk=TEAM#<id>, sk=NOTIFICATIONS#<user_id>)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationPrefs {
    pub email_run_complete: bool,
    pub email_run_failed: bool,
    pub email_weekly_summary: bool,
    /// Token usage emails at 80% and 100% of the monthly limit. The worker reads
    /// this exact field name before sending them.
    pub email_token_warning: bool,
}

impl Default for NotificationPrefs {
    fn default() -> Self {
        Self {
            email_run_complete: true,
            email_run_failed: true,
            email_weekly_summary: true,
            email_token_warning: true,
        }
    }
}

impl NotificationPrefs {
    /// Read stored preferences. A missing field means the email is on, which is
    /// also what the worker assumes when it decides whether to send.
    pub fn from_item(
        item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    ) -> Self {
        let flag = |key: &str| {
            item.get(key)
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true)
        };
        Self {
            email_run_complete: flag("email_run_complete"),
            email_run_failed: flag("email_run_failed"),
            email_weekly_summary: flag("email_weekly_summary"),
            email_token_warning: flag("email_token_warning"),
        }
    }

    /// Apply a request body on top of the stored preferences: a field the body
    /// leaves out keeps its stored value, so a client that does not know about
    /// a setting cannot change it by accident.
    pub fn merged_with(&self, body: &serde_json::Value) -> Self {
        let pick = |key: &str, current: bool| body[key].as_bool().unwrap_or(current);
        Self {
            email_run_complete: pick("email_run_complete", self.email_run_complete),
            email_run_failed: pick("email_run_failed", self.email_run_failed),
            email_weekly_summary: pick("email_weekly_summary", self.email_weekly_summary),
            email_token_warning: pick("email_token_warning", self.email_token_warning),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_serde() {
        let json = r#"{
            "github_app_id":"1",
            "github_private_key":"k",
            "github_webhook_secret":"ws",
            "github_client_id":"ci",
            "github_client_secret":"cs",
            "jwt_secret":"js"
        }"#;
        let s: Secrets = serde_json::from_str(json).unwrap();
        assert_eq!(s.github_app_id, "1");
        assert!(s.jira_webhook_secret.is_none());
    }

    #[test]
    fn notification_prefs_default_on_when_missing() {
        let prefs = NotificationPrefs::from_item(&std::collections::HashMap::new());
        assert!(prefs.email_token_warning);
        assert!(prefs.email_weekly_summary);
    }

    #[test]
    fn notification_prefs_keep_token_warning() {
        use aws_sdk_dynamodb::types::AttributeValue;
        let mut item = std::collections::HashMap::new();
        item.insert(
            "email_token_warning".to_string(),
            AttributeValue::Bool(false),
        );
        let stored = NotificationPrefs::from_item(&item);
        assert!(!stored.email_token_warning);

        // A body without the field keeps what was stored.
        let kept = stored.merged_with(&serde_json::json!({ "email_weekly_summary": false }));
        assert!(!kept.email_token_warning);
        assert!(!kept.email_weekly_summary);
        assert!(kept.email_run_complete);

        // A body with the field changes it, and the response carries it.
        let on = stored.merged_with(&serde_json::json!({ "email_token_warning": true }));
        assert!(on.email_token_warning);
        let json = serde_json::to_value(&on).unwrap();
        assert_eq!(json["email_token_warning"], serde_json::json!(true));
    }

    #[test]
    fn claims_roundtrip() {
        let c = Claims {
            sub: "user1".into(),
            team_id: "TEAM#1".into(),
            email: "user@example.com".into(),
            role: "owner".into(),
            github_login: Some("octocat".into()),
            aud: "coderhelm-dashboard".into(),
            exp: 9999999999,
            iat: 1000000000,
            support_view: None,
            custom: None,
            api_key: None,
        };
        let json = serde_json::to_string(&c).unwrap();
        let parsed: Claims = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user1");
        assert_eq!(parsed.aud, "coderhelm-dashboard");
    }
}
