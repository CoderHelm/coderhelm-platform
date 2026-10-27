use serde::Deserialize;

pub struct Config {
    pub stage: String,
    /// EventBridge Scheduler group for plugin schedules, the role it sends
    /// with, and the tickets queue it sends to (plugins only).
    pub plugin_schedule_group: String,
    pub plugin_scheduler_role_arn: String,
    pub ticket_queue_arn: String,
    pub table_name: String,
    pub teams_table_name: String,
    pub runs_table_name: String,
    pub analytics_table_name: String,
    pub users_table_name: String,
    pub plans_table_name: String,
    pub jira_config_table_name: String,
    pub repos_table_name: String,
    pub settings_table_name: String,
    /// Connected AWS accounts and what each can do (logs, metrics, Airflow).
    /// Empty: agents use only the older separate connections.
    pub aws_insights_table_name: String,
    pub infra_table_name: String,
    pub mcp_configs_table_name: String,
    pub traces_table_name: String,
    pub checkpoints_table_name: String,
    pub events_table_name: String,
    pub bucket_name: String,
    pub secrets_name: String,
    pub model_id: String,
    pub light_model_id: String,
    pub ses_from_address: String,
    pub ses_template_prefix: String,
    pub mcp_proxy_function_name: String,
    /// The KMS key a refreshed remote MCP token is sealed with. Empty: the
    /// worker can't renew a sign-in (agents skip that server until it's
    /// renewed in the dashboard).
    pub secrets_key_id: String,
    pub ci_fix_queue_url: String,
    pub ticket_queue_url: String,
    pub feedback_queue_url: String,
    /// Outbound webhook events and their retries. Empty: nothing is emitted.
    pub webhook_queue_url: String,
    /// The shared dead-letter queue (the DLQ cleanup reads it). Empty: the
    /// cleanup does nothing.
    pub dlq_url: String,
    /// Execution sandbox: S3 bucket for transient source tarballs and the
    /// CodeBuild project that runs the repo's real build/lint/tests. Both empty
    /// => sandbox disabled (the agent falls back to blind CI, never worse).
    pub sandbox_bucket_name: String,
    pub sandbox_project_name: String,
    /// E2E results. Empty disables the feature.
    pub tests_table_name: String,
    /// The Monday weekly summary goes out only when this is true.
    pub weekly_summary_enabled: bool,
    /// Send the scheduled summary only to teams that have a rollout list.
    pub weekly_summary_listed_teams_only: bool,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            stage: std::env::var("STAGE").unwrap_or_else(|_| "dev".to_string()),
            plugin_schedule_group: std::env::var("PLUGIN_SCHEDULE_GROUP").unwrap_or_default(),
            plugin_scheduler_role_arn: std::env::var("PLUGIN_SCHEDULER_ROLE_ARN")
                .unwrap_or_default(),
            ticket_queue_arn: std::env::var("TICKET_QUEUE_ARN").unwrap_or_default(),
            table_name: std::env::var("TABLE_NAME").expect("TABLE_NAME required"),
            teams_table_name: std::env::var("TEAMS_TABLE_NAME").expect("TEAMS_TABLE_NAME required"),
            runs_table_name: std::env::var("RUNS_TABLE_NAME").expect("RUNS_TABLE_NAME required"),
            analytics_table_name: std::env::var("ANALYTICS_TABLE_NAME")
                .expect("ANALYTICS_TABLE_NAME required"),
            users_table_name: std::env::var("USERS_TABLE_NAME").expect("USERS_TABLE_NAME required"),
            plans_table_name: std::env::var("PLANS_TABLE_NAME").expect("PLANS_TABLE_NAME required"),
            jira_config_table_name: std::env::var("JIRA_CONFIG_TABLE_NAME")
                .expect("JIRA_CONFIG_TABLE_NAME required"),
            repos_table_name: std::env::var("REPOS_TABLE_NAME").expect("REPOS_TABLE_NAME required"),
            settings_table_name: std::env::var("SETTINGS_TABLE_NAME")
                .expect("SETTINGS_TABLE_NAME required"),
            aws_insights_table_name: std::env::var("AWS_INSIGHTS_TABLE_NAME").unwrap_or_default(),
            infra_table_name: std::env::var("INFRA_TABLE_NAME").expect("INFRA_TABLE_NAME required"),
            mcp_configs_table_name: std::env::var("MCP_CONFIGS_TABLE_NAME").unwrap_or_default(),
            traces_table_name: std::env::var("TRACES_TABLE_NAME").unwrap_or_default(),
            checkpoints_table_name: std::env::var("CHECKPOINTS_TABLE_NAME").unwrap_or_default(),
            events_table_name: std::env::var("EVENTS_TABLE_NAME").unwrap_or_default(),
            bucket_name: std::env::var("BUCKET_NAME").expect("BUCKET_NAME required"),
            secrets_name: std::env::var("SECRETS_NAME")
                .unwrap_or_else(|_| "coderhelm/prod/secrets".to_string()),
            model_id: std::env::var("MODEL_ID").expect("MODEL_ID required"),
            light_model_id: std::env::var("LIGHT_MODEL_ID")
                .unwrap_or_else(|_| "claude-sonnet-5-5".to_string()),
            ses_from_address: std::env::var("SES_FROM_ADDRESS")
                .unwrap_or_else(|_| common::public_urls::default_from_address().to_string()),
            ses_template_prefix: std::env::var("SES_TEMPLATE_PREFIX")
                .unwrap_or_else(|_| "coderhelm-prod".to_string()),
            mcp_proxy_function_name: std::env::var("MCP_PROXY_FUNCTION_NAME").unwrap_or_default(),
            secrets_key_id: std::env::var("SECRETS_KEY_ID").unwrap_or_default(),
            ci_fix_queue_url: std::env::var("CI_FIX_QUEUE_URL").unwrap_or_default(),
            ticket_queue_url: std::env::var("TICKET_QUEUE_URL").unwrap_or_default(),
            feedback_queue_url: std::env::var("FEEDBACK_QUEUE_URL").unwrap_or_default(),
            webhook_queue_url: std::env::var("WEBHOOK_QUEUE_URL").unwrap_or_default(),
            dlq_url: std::env::var("DLQ_URL").unwrap_or_default(),
            sandbox_bucket_name: std::env::var("SANDBOX_BUCKET_NAME").unwrap_or_default(),
            sandbox_project_name: std::env::var("SANDBOX_PROJECT_NAME").unwrap_or_default(),
            tests_table_name: std::env::var("TESTS_TABLE_NAME").unwrap_or_default(),
            weekly_summary_enabled: std::env::var("WEEKLY_SUMMARY_ENABLED")
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            // Anything but an explicit "false" keeps the safe default.
            weekly_summary_listed_teams_only: std::env::var("WEEKLY_SUMMARY_LISTED_TEAMS_ONLY")
                .map(|v| !v.trim().eq_ignore_ascii_case("false"))
                .unwrap_or(true),
        }
    }
}

#[derive(Deserialize)]
pub struct Secrets {
    /// Empty on a hybrid install that uses CoderHelm's GitHub App (tokens
    /// come from the hosted broker; common::hybrid::client).
    #[serde(default)]
    pub github_app_id: String,
    #[serde(default)]
    pub github_private_key: String,
    /// Signs unsubscribe links (a key derived from it, never the secret
    /// itself); the gateway checks them with the same secret.
    #[serde(default)]
    pub jwt_secret: String,
    /// Hosted side of hybrid: the key relay keys are derived from.
    #[serde(default)]
    pub hybrid_master_key: Option<String>,
    /// A hybrid install's registration with the hosted side.
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
    ) -> Result<Self, lambda_runtime::Error> {
        let response = client.get_secret_value().secret_id(name).send().await?;
        let secret_string = response
            .secret_string()
            .ok_or("Secret has no string value")?;
        let secrets: Secrets = serde_json::from_str(secret_string)?;
        Ok(secrets)
    }
}

pub use common::messages::*;

/// One run's (or one job's) token usage with prompt cache metrics. Per-run
/// totals only: the team's monthly counters are written per model call by
/// `common::usage` (never from this struct), so nothing is counted twice.
#[derive(Debug, Default, Clone)]
pub struct TokenUsage {
    /// Uncached input (Anthropic `input_tokens`).
    pub input_tokens: u64,
    /// Output, thinking included.
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    /// All cache writes (5-minute + 1-hour).
    pub cache_write_tokens: u64,
    /// The 1-hour part of `cache_write_tokens`.
    pub cache_write_1h_tokens: u64,
    /// USD, each call priced at its own model's rates.
    pub cost_usd: f64,
    pub tool_calls: u64,
    pub tool_names: Vec<String>,
}

impl TokenUsage {
    /// Add one model call's usage, priced at `model_id`'s rates.
    pub fn add_call(&mut self, model_id: &str, call: &common::usage::CallUsage) {
        self.input_tokens += call.uncached_input;
        self.output_tokens += call.output;
        self.cache_read_tokens += call.cache_read;
        self.cache_write_tokens += call.cache_write();
        self.cache_write_1h_tokens += call.cache_write_1h;
        self.cost_usd += call.cost_usd(model_id);
    }

    /// Add another usage's tokens and cost (tool calls are not merged).
    pub fn merge(&mut self, other: &TokenUsage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        self.cache_write_1h_tokens += other.cache_write_1h_tokens;
        self.cost_usd += other.cost_usd;
    }

    /// All four parts.
    pub fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_write_tokens
    }

    pub fn record_tool_call(&mut self, name: &str, duration_ms: u64) {
        self.tool_calls += 1;
        if !self.tool_names.contains(&name.to_string()) {
            self.tool_names.push(name.to_string());
        }
        let _ = duration_ms; // tracked for logging, not stored in cumulative
    }

    /// USD for this usage: the sum of each call at its own model's price.
    pub fn estimated_cost(&self) -> f64 {
        self.cost_usd
    }

    /// Rebuild a saved usage. Old checkpoints carry no cost: their tokens are
    /// priced at `model_id`'s rates.
    pub fn restored(
        model_id: &str,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        cost_usd: Option<f64>,
    ) -> Self {
        let call = common::usage::CallUsage {
            uncached_input: input,
            cache_read,
            cache_write_5m: cache_write,
            cache_write_1h: 0,
            output,
        };
        let mut u = TokenUsage::default();
        u.add_call(model_id, &call);
        if let Some(c) = cost_usd {
            u.cost_usd = c;
        }
        u
    }
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
            requester: None,
            base_branch: "main".into(),
            image_attachments: vec![],
            continuation: 0,
            continuation_run_id: None,
            first_attempt_ms: 0,
            model_retries: 0,
            ticket_update_note: None,
            ticket_history_note: None,
            plan_cache_key: None,
            team_instructions: String::new(),
            person_direction: None,
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, WorkerMessage::Ticket(_)));
    }

    #[test]
    fn feedback_message_roundtrip() {
        let msg = WorkerMessage::Feedback(FeedbackMessage {
            team_id: "TEAM#1".into(),
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
                comment_id: None,
                node_id: None,
                is_context: false,
            }],
            trigger_author: "alice".into(),
            defer_count: 2,
            model_retries: 0,
        });
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: WorkerMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            WorkerMessage::Feedback(f) => assert_eq!(f.defer_count, 2),
            _ => panic!("expected feedback"),
        }
    }

    #[test]
    fn messages_queued_before_new_fields_still_parse() {
        let gate = r#"{"type":"await_merge","team_id":"T","installation_id":1,"repo_owner":"o","repo_name":"r","pr_number":2,"head_sha":"h","base_branch":"main","attempts":3}"#;
        match serde_json::from_str::<WorkerMessage>(gate).unwrap() {
            WorkerMessage::AwaitMerge(m) => {
                assert!(m.chain_id.is_empty());
                assert_eq!(m.error_retries, 0);
            }
            _ => panic!("expected await_merge"),
        }
        let feedback = r#"{"type":"feedback","team_id":"T","installation_id":1,"run_id":"x","repo_owner":"o","repo_name":"r","pr_number":2,"review_id":0,"review_body":"hi","comments":[]}"#;
        match serde_json::from_str::<WorkerMessage>(feedback).unwrap() {
            WorkerMessage::Feedback(m) => assert!(m.trigger_author.is_empty()),
            _ => panic!("expected feedback"),
        }
    }

    #[test]
    fn feedback_message_defaults_defer_count_when_absent() {
        // Messages from the gateway carry no defer_count — it must default to 0.
        let json = r#"{"type":"feedback","team_id":"TEAM#1","installation_id":1,
            "run_id":"r","repo_owner":"o","repo_name":"n","pr_number":1,
            "review_id":1,"review_body":"","comments":[]}"#;
        let parsed: WorkerMessage = serde_json::from_str(json).unwrap();
        match parsed {
            WorkerMessage::Feedback(f) => assert_eq!(f.defer_count, 0),
            _ => panic!("expected feedback"),
        }
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
    fn token_usage_cost() {
        let mut usage = TokenUsage::default();
        let call = common::usage::CallUsage {
            uncached_input: 1000,
            cache_read: 200,
            cache_write_5m: 100,
            cache_write_1h: 0,
            output: 500,
        };
        usage.add_call("claude-opus-5-5", &call);
        usage.add_call("claude-sonnet-5-5", &call);
        assert_eq!(usage.input_tokens, 2000);
        assert_eq!(usage.output_tokens, 1000);
        assert_eq!(usage.cache_read_tokens, 400);
        assert_eq!(usage.cache_write_tokens, 200);
        assert_eq!(usage.total_tokens(), 3600);
        // Each call at its own model's price, not one blended rate.
        let want = call.cost_usd("claude-opus-5-5") + call.cost_usd("claude-sonnet-5-5");
        assert!((usage.estimated_cost() - want).abs() < 1e-12);
        let mut merged = TokenUsage::default();
        merged.merge(&usage);
        assert_eq!(merged.total_tokens(), usage.total_tokens());
        assert!((merged.cost_usd - usage.cost_usd).abs() < 1e-12);
    }

    #[test]
    fn restored_usage_keeps_a_saved_cost_or_prices_the_tokens() {
        let r = TokenUsage::restored("claude-opus-5-5", 10, 20, 30, 40, Some(1.5));
        assert_eq!(r.total_tokens(), 100);
        assert_eq!(r.cost_usd, 1.5);
        let r = TokenUsage::restored("claude-opus-5-5", 1_000_000, 0, 0, 0, None);
        assert!((r.cost_usd - 4.0).abs() < 1e-9);
    }

    #[test]
    fn secrets_requires_fields() {
        let json = r#"{"github_app_id":"1","github_private_key":"k"}"#;
        let s: Secrets = serde_json::from_str(json).unwrap();
        assert_eq!(s.github_app_id, "1");
    }
}
