use serde::{Deserialize, Serialize};

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
    pub ci_fix_queue_url: String,
    pub feedback_queue_url: String,
    pub dlq_url: String,
    pub ses_from_address: String,
    pub ses_template_prefix: String,
    pub model_id: String,
    pub cognito_user_pool_id: String,
    pub cognito_client_id: String,
    pub cognito_domain: String,
    pub mcp_proxy_function_name: String,
    pub traces_table_name: String,
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
            ci_fix_queue_url: std::env::var("CI_FIX_QUEUE_URL").expect("CI_FIX_QUEUE_URL required"),
            feedback_queue_url: std::env::var("FEEDBACK_QUEUE_URL")
                .expect("FEEDBACK_QUEUE_URL required"),
            dlq_url: std::env::var("DLQ_URL").unwrap_or_default(),
            ses_from_address: std::env::var("SES_FROM_ADDRESS")
                .unwrap_or_else(|_| "notifications@coderhelm.com".to_string()),
            ses_template_prefix: std::env::var("SES_TEMPLATE_PREFIX")
                .unwrap_or_else(|_| "coderhelm-prod".to_string()),
            model_id: std::env::var("MODEL_ID")
                .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string()),
            cognito_user_pool_id: std::env::var("COGNITO_USER_POOL_ID")
                .expect("COGNITO_USER_POOL_ID required"),
            cognito_client_id: std::env::var("COGNITO_CLIENT_ID")
                .expect("COGNITO_CLIENT_ID required"),
            cognito_domain: std::env::var("COGNITO_DOMAIN").expect("COGNITO_DOMAIN required"),
            mcp_proxy_function_name: std::env::var("MCP_PROXY_FUNCTION_NAME").unwrap_or_default(),
            traces_table_name: std::env::var("TRACES_TABLE_NAME").unwrap_or_default(),
        }
    }
}

/// Secrets loaded from AWS Secrets Manager.
#[derive(Deserialize)]
pub struct Secrets {
    pub github_app_id: String,
    pub github_private_key: String,
    pub github_webhook_secret: String,
    pub github_client_id: String,
    pub github_client_secret: String,
    pub jwt_secret: String,
    #[serde(default)]
    pub google_client_id: Option<String>,
    #[serde(default)]
    pub google_client_secret: Option<String>,
    #[serde(default)]
    pub jira_webhook_secret: Option<String>,
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

/// SQS message types sent from gateway → worker.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum WorkerMessage {
    #[serde(rename = "ticket")]
    Ticket(TicketMessage),
    #[serde(rename = "ci_fix")]
    CiFix(CiFixMessage),
    #[serde(rename = "feedback")]
    Feedback(FeedbackMessage),
    #[serde(rename = "onboard")]
    Onboard(OnboardMessage),
    #[serde(rename = "mark_ready")]
    MarkReady(MarkReadyMessage),
    #[serde(rename = "plan_execute")]
    PlanExecute(PlanExecuteMessage),
    #[serde(rename = "plan_task_continue")]
    PlanTaskContinue(PlanTaskContinueMessage),
    #[serde(rename = "infra_analyze")]
    InfraAnalyze(InfraAnalyzeMessage),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarkReadyMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PlanExecuteMessage {
    pub team_id: String,
    pub plan_id: String,
    pub triggered_by: String,
    pub tasks: Vec<String>, // ordered task_ids
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PlanTaskContinueMessage {
    pub team_id: String,
    pub plan_id: String,
    pub tasks: Vec<String>, // task_ids to process
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfraAnalyzeMessage {
    pub team_id: String,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TicketMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub source: TicketSource,
    pub ticket_id: String,
    pub title: String,
    pub body: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub issue_number: u64,
    pub sender: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TicketSource {
    Github,
    Jira,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CiFixMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub run_id: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub branch: String,
    pub pr_number: u64,
    pub check_run_id: u64,
    pub attempt: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FeedbackMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub run_id: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    pub review_id: u64,
    pub review_body: String,
    pub comments: Vec<ReviewComment>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReviewComment {
    pub path: String,
    pub line: Option<u64>,
    pub body: String,
    #[serde(default)]
    pub comment_id: Option<u64>,
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
}

impl Default for NotificationPrefs {
    fn default() -> Self {
        Self {
            email_run_complete: true,
            email_run_failed: true,
            email_weekly_summary: true,
        }
    }
}

// --- Onboard types ---

#[derive(Serialize, Deserialize, Debug)]
pub struct OnboardMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repos: Vec<OnboardRepo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OnboardRepo {
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_message_roundtrip() {
        let msg = WorkerMessage::Ticket(TicketMessage {
            team_id: "TEAM#1".into(),
            installation_id: 1,
            source: TicketSource::Github,
            ticket_id: "GH-42".into(),
            title: "Fix bug".into(),
            body: "details".into(),
            repo_owner: "org".into(),
            repo_name: "repo".into(),
            issue_number: 42,
            sender: "user".into(),
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WorkerMessage::Ticket(_)));
    }

    #[test]
    fn onboard_message_roundtrip() {
        let msg = WorkerMessage::Onboard(OnboardMessage {
            team_id: "TEAM#1".into(),
            installation_id: 1,
            repos: vec![OnboardRepo {
                owner: "org".into(),
                name: "repo".into(),
                default_branch: "main".into(),
            }],
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WorkerMessage::Onboard(_)));
    }

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
        };
        let json = serde_json::to_string(&c).unwrap();
        let parsed: Claims = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user1");
        assert_eq!(parsed.aud, "coderhelm-dashboard");
    }
}
