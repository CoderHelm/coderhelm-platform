use serde::{Deserialize, Serialize};

pub struct Config {
    pub stage: String,
    pub table_name: String,
    pub runs_table_name: String,
    pub analytics_table_name: String,
    pub bucket_name: String,
    pub secrets_name: String,
    pub model_id: String,
    pub light_model_id: String,
    pub ses_from_address: String,
    pub ses_template_prefix: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            stage: std::env::var("STAGE").unwrap_or_else(|_| "dev".to_string()),
            table_name: std::env::var("TABLE_NAME").expect("TABLE_NAME required"),
            runs_table_name: std::env::var("RUNS_TABLE_NAME").expect("RUNS_TABLE_NAME required"),
            analytics_table_name: std::env::var("ANALYTICS_TABLE_NAME")
                .expect("ANALYTICS_TABLE_NAME required"),
            bucket_name: std::env::var("BUCKET_NAME").expect("BUCKET_NAME required"),
            secrets_name: std::env::var("SECRETS_NAME")
                .unwrap_or_else(|_| "coderhelm/prod/secrets".to_string()),
            model_id: std::env::var("MODEL_ID").expect("MODEL_ID required"),
            light_model_id: std::env::var("LIGHT_MODEL_ID")
                .unwrap_or_else(|_| "us.anthropic.claude-sonnet-4-6-v1".to_string()),
            ses_from_address: std::env::var("SES_FROM_ADDRESS")
                .unwrap_or_else(|_| "notifications@coderhelm.com".to_string()),
            ses_template_prefix: std::env::var("SES_TEMPLATE_PREFIX")
                .unwrap_or_else(|_| "coderhelm-prod".to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Secrets {
    pub github_app_id: String,
    pub github_private_key: String,
    #[serde(default)]
    pub stripe_secret_key: Option<String>,
}

impl Secrets {
    pub async fn load(
        client: &aws_sdk_secretsmanager::Client,
        name: &str,
    ) -> Result<Self, lambda_runtime::Error> {
        let response = client.get_secret_value().secret_id(name).send().await?;
        let secret_string = response
            .secret_string()
            .ok_or("Secret has no string value")?;
        let secrets: Secrets = serde_json::from_str(secret_string)?;
        Ok(secrets)
    }
}

/// SQS message types (must match gateway models).
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
    #[serde(rename = "infra_analyze")]
    InfraAnalyze(InfraAnalyzeMessage),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarkReadyMessage {
    pub tenant_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PlanExecuteMessage {
    pub tenant_id: String,
    pub plan_id: String,
    pub triggered_by: String,
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfraAnalyzeMessage {
    pub tenant_id: String,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TicketMessage {
    pub tenant_id: String,
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
    pub tenant_id: String,
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
    pub tenant_id: String,
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
}

/// Token usage tracking with prompt cache metrics.
#[derive(Debug, Default)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

impl TokenUsage {
    pub fn add(&mut self, input: u64, output: u64, cache_read: u64, cache_write: u64) {
        self.input_tokens += input;
        self.output_tokens += output;
        self.cache_read_tokens += cache_read;
        self.cache_write_tokens += cache_write;
    }

    /// Estimated cost using blended rates across Opus (implement) and Sonnet (other passes).
    /// Actual Bedrock billing is model-specific; this is an approximation for display.
    /// Blended: ~60% of input goes to Opus, ~40% to Sonnet (implement is the heaviest pass).
    pub fn estimated_cost(&self) -> f64 {
        // Blended input rate: weighted average of Opus ($15/M) and Sonnet ($3/M)
        let input_rate = 10.0 / 1_000_000.0;
        // Blended output rate: weighted average of Opus ($75/M) and Sonnet ($15/M)
        let output_rate = 50.0 / 1_000_000.0;

        let input_cost = self.input_tokens as f64 * input_rate;
        let output_cost = self.output_tokens as f64 * output_rate;
        let cache_read_cost = self.cache_read_tokens as f64 * input_rate * 0.1;
        let cache_write_cost = self.cache_write_tokens as f64 * input_rate * 1.25;

        input_cost + output_cost + cache_read_cost + cache_write_cost
    }
}

// --- Onboard types ---

#[derive(Serialize, Deserialize, Debug)]
pub struct OnboardMessage {
    pub tenant_id: String,
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
            tenant_id: "TENANT#1".into(),
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
    fn ci_fix_message_roundtrip() {
        let msg = WorkerMessage::CiFix(CiFixMessage {
            tenant_id: "TENANT#1".into(),
            installation_id: 1,
            run_id: "run1".into(),
            repo_owner: "org".into(),
            repo_name: "repo".into(),
            branch: "coderhelm/fix".into(),
            pr_number: 10,
            check_run_id: 99,
            attempt: 1,
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WorkerMessage::CiFix(_)));
    }

    #[test]
    fn feedback_message_roundtrip() {
        let msg = WorkerMessage::Feedback(FeedbackMessage {
            tenant_id: "TENANT#1".into(),
            installation_id: 1,
            run_id: "run1".into(),
            repo_owner: "org".into(),
            repo_name: "repo".into(),
            pr_number: 5,
            review_id: 100,
            review_body: "LGTM".into(),
            comments: vec![ReviewComment {
                path: "src/main.rs".into(),
                line: Some(10),
                body: "nit".into(),
            }],
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WorkerMessage::Feedback(_)));
    }

    #[test]
    fn onboard_message_roundtrip() {
        let msg = WorkerMessage::Onboard(OnboardMessage {
            tenant_id: "TENANT#1".into(),
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
    fn token_usage_cost() {
        let mut usage = TokenUsage::default();
        usage.add(1000, 500, 200, 100);
        assert_eq!(usage.input_tokens, 1000);
        assert_eq!(usage.output_tokens, 500);
        assert_eq!(usage.cache_read_tokens, 200);
        assert_eq!(usage.cache_write_tokens, 100);
        assert!(usage.estimated_cost() > 0.0);
    }

    #[test]
    fn secrets_requires_fields() {
        let json = r#"{"github_app_id":"1","github_private_key":"k"}"#;
        let s: Secrets = serde_json::from_str(json).unwrap();
        assert_eq!(s.github_app_id, "1");
    }
}
