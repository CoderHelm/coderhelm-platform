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
            ci_fix_queue_url: std::env::var("CI_FIX_QUEUE_URL").expect("CI_FIX_QUEUE_URL required"),
            feedback_queue_url: std::env::var("FEEDBACK_QUEUE_URL")
                .expect("FEEDBACK_QUEUE_URL required"),
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
    #[serde(rename = "tool_proposals")]
    ToolProposals(ToolProposalsMessage),
    #[serde(rename = "hidden_suggest")]
    HiddenSuggest(HiddenSuggestMessage),
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
    #[serde(rename = "resume")]
    Resume(ResumeMessage),
    #[serde(rename = "review")]
    Review(ReviewMessage),
    #[serde(rename = "await_merge")]
    AwaitMerge(AwaitMergeMessage),
    /// Code-graph index pass (gateway → worker): full on enable, incremental on
    /// a push to the graph's branch.
    #[serde(rename = "graph_index")]
    GraphIndex(GraphIndexMessage),
    #[serde(rename = "release_notes")]
    ReleaseNotes(ReleaseNotesMessage),
    /// Cut the repo's release tag now (the dashboard's "Release now").
    #[serde(rename = "tag_sweep")]
    TagSweep(TagSweepMessage),
    /// Read the test reports of a finished workflow run or an upload.
    #[serde(rename = "test_ingest")]
    TestIngest(common::e2e::messages::TestIngest),
    /// Fix a flaky test, generate a recommended test, or file a ticket.
    #[serde(rename = "test_action")]
    TestAction(common::e2e::messages::TestAction),
    /// Read a connected skill repository's default branch again.
    #[serde(rename = "skill_sync")]
    SkillSync(common::skills::SkillSyncMessage),
    /// The one PascalCase tag on the wire; pinned as is (the worker reads it
    /// this way). See `common::dlq::WORKER_MESSAGE_TYPES`.
    #[serde(rename = "SkillAsk")]
    SkillAsk(common::skills::SkillAskMessage),
    /// CoderHelm's PR was closed without merging: the worker reads what
    /// happened and records a lesson when the fix belonged outside the code.
    #[serde(rename = "lesson")]
    Lesson(LessonMessage),
    /// People pushed back on a CoderHelm review: the worker judges whether the
    /// reviewer was wrong and keeps the lesson.
    #[serde(rename = "review_lesson")]
    ReviewLesson(ReviewLessonMessage),
    /// A person commented on a ticket CoderHelm worked on: the worker reads
    /// what the comment is before anything runs.
    #[serde(rename = "jira_comment")]
    JiraComment(JiraCommentMessage),
}

/// A ticket comment for the worker to read (must match the worker's
/// `JiraCommentMessage`). `action` is what the comment would start if it
/// asks for work: a ticket run or feedback on the open PR.
#[derive(Serialize, Deserialize, Debug)]
pub struct JiraCommentMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub ticket_id: String,
    pub title: String,
    /// The ticket's latest run.
    pub run_id: String,
    pub run_status: String,
    pub pr_number: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub author: String,
    pub comment: String,
    /// The ticket's content hash now (a done ticket stays done until it changes).
    pub context_hash: String,
    pub action: Option<Box<WorkerMessage>>,
}

/// A lesson job (must match the worker's `LessonMessage`).
#[derive(Serialize, Deserialize, Debug)]
pub struct LessonMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub run_id: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// `pr_closed` or `comment`.
    pub trigger: String,
}

/// A judgment of people's pushback on a review (must match the worker's
/// `ReviewLessonMessage`).
#[derive(Serialize, Deserialize, Debug)]
pub struct ReviewLessonMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    pub trigger: String,
    /// The event's time (ms) on the PR's pending record.
    pub event_at: i64,
}

/// A code-graph index job. `changed_files: None` ⇒ full index; `Some(paths)` ⇒
/// incremental. Empty `branch` ⇒ the worker resolves the repo's default branch.
#[derive(Serialize, Deserialize, Debug)]
pub struct GraphIndexMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub branch: String,
    #[serde(default)]
    pub changed_files: Option<Vec<String>>,
}

/// Armed auto-merge job (gateway → worker). Enqueued when a human approval lands
/// on a review-enabled PR; the worker's await_merge gate is the sole authority on
/// whether auto-merge is actually on, whether both keys are present, and whether
/// every CI check is green before it merges. Bound to the reviewed head SHA.
#[derive(Serialize, Deserialize, Debug)]
pub struct AwaitMergeMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    pub head_sha: String,
    pub base_branch: String,
    /// The PR is CoderHelm's own — forces the human-approval key on.
    #[serde(default)]
    pub self_authored: bool,
    #[serde(default)]
    pub attempts: u32,
    /// Which arming this tick belongs to. Every arming starts a new chain; a
    /// tick whose chain is no longer the record's current one stops, so one
    /// PR never runs two polling loops. Empty on a fresh arming.
    #[serde(default)]
    pub chain_id: String,
    /// Evaluations in a row that failed on an error (GitHub or DynamoDB).
    /// They are retried on their own schedule and never end the gate.
    #[serde(default)]
    pub error_retries: u32,
}

/// A label-triggered code-review job for an existing PR. Keyed by (pr_number,
/// head_sha) so re-labels and new commits are idempotent per commit.
#[derive(Serialize, Deserialize, Debug)]
pub struct ReviewMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// Head commit the review is bound to — the review verdict + any auto-action
    /// only applies to this SHA (a later push invalidates it).
    pub head_sha: String,
    /// The label that triggered the review (for audit).
    pub label: String,
    /// Present when a human replied asking a question — the pass answers instead
    /// of posting a verdict. None → normal verdict review.
    #[serde(default)]
    pub question: Option<String>,
    /// What triggered the review: "label" | "synchronize" | "reply". Audit only.
    #[serde(default)]
    pub trigger: String,
    /// Stable idempotency key for the TRIGGER (e.g. the comment id for a reply,
    /// the head sha for a label event). GitHub webhooks and SQS are both
    /// at-least-once, so the same trigger can arrive twice; the worker claims
    /// this key once and drops any duplicate — one user action ⇒ one review.
    /// Empty ⇒ no dedup (old in-flight messages / paths that don't set it).
    #[serde(default)]
    pub dedup_key: String,
    /// Set when the ask came from a reply inside an inline review thread: the
    /// answer is posted back into that thread instead of the conversation.
    #[serde(default)]
    pub reply_to_comment_id: Option<u64>,
    /// Login of the person whose comment asked for this job (empty for
    /// label, push and sweep triggers). On CoderHelm's own PRs a fix request
    /// goes on to the fix loop, which answers this person on the PR.
    #[serde(default)]
    pub requested_by: String,
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
pub struct ResumeMessage {
    pub team_id: String,
    pub run_id: String,
    pub installation_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ImageAttachment {
    pub s3_key: String,
    pub media_type: String,
    pub filename: String,
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
    /// Who asked for the work, as one identity per person (analytics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester: Option<common::requester::Requester>,
    #[serde(default)]
    pub image_attachments: Vec<ImageAttachment>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TicketSource {
    Github,
    Jira,
    /// A monitoring alert (e.g. a CloudWatch alarm) routed to a repo.
    Alert,
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
    /// Login of the person whose comment or review body addressed CoderHelm.
    /// Empty for automated feedback (CI results, CoderHelm's own review). When
    /// set and there is no inline thread to answer in, the fix loop replies on
    /// the PR addressed to them.
    #[serde(default)]
    pub trigger_author: String,
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
    /// Set only on a support-view session: a Coderhelm platform admin reading
    /// one team's workspace, read-only, under that team's grant. Never set on
    /// a normal session token (the auth middleware drops it there).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_view: Option<SupportView>,
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
            requester: None,
            image_attachments: vec![],
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
        };
        let json = serde_json::to_string(&c).unwrap();
        let parsed: Claims = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user1");
        assert_eq!(parsed.aud, "coderhelm-dashboard");
    }
}

/// Write/publish release notes for a tag (dashboard send). Mirrors the worker's
/// ReleaseNotesMessage.
#[derive(Serialize, Deserialize, Debug)]
pub struct ReleaseNotesMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub tag: String,
    pub sha: String,
    #[serde(default)]
    pub resend: String,
    /// Set for tags CoderHelm didn't cut (pushed by a person or CI): only write
    /// notes when the tag's commit is on the repo's release branch.
    #[serde(default)]
    pub check_branch: bool,
}

/// A release-tag sweep. Mirrors the worker's TagSweepMessage.
#[derive(Serialize, Deserialize, Debug)]
pub struct TagSweepMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    /// Empty: the worker uses the repo's default branch.
    pub base_branch: String,
    pub pr_number: u64,
    pub delay_remaining_secs: u64,
    /// The waiting window's id: whichever sweep claims it first releases it.
    pub sweep_id: String,
    pub manual: bool,
    /// No window to claim (nothing was waiting).
    pub claimed: bool,
    pub attempt: u32,
}

/// Work out suggested hidden fields for one database source from its table
/// and column names. `force` asks again even when the names haven't changed.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HiddenSuggestMessage {
    pub team_id: String,
    pub source_id: String,
    #[serde(default)]
    pub force: bool,
}

/// Tool definitions changed in a repo's default branch: each becomes a
/// proposed version waiting for an admin's approval. Never approved here.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ToolProposalsMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub sha: String,
    pub paths: Vec<String>,
}

/// The gateway → worker contract (see `common::dlq::CONTRACT_JSON`). The
/// worker's test parses the same samples, so a tag the worker can't read
/// fails a test here or there instead of reaching the DLQ in production.
#[cfg(test)]
mod worker_contract_tests {
    use super::WorkerMessage;
    use serde_json::Value;

    fn samples() -> serde_json::Map<String, Value> {
        let v: Value = serde_json::from_str(common::dlq::CONTRACT_JSON).unwrap();
        v["gateway"].as_object().unwrap().clone()
    }

    /// Every tag the gateway's enum has, read from serde itself.
    fn gateway_tags() -> Vec<String> {
        let err = serde_json::from_str::<WorkerMessage>(r#"{"type":"__no_such_type__"}"#)
            .unwrap_err()
            .to_string();
        common::dlq::accepted_tags(&err)
    }

    #[test]
    fn every_gateway_message_type_has_a_contract_sample() {
        let mut tags = gateway_tags();
        tags.sort();
        let mut sampled: Vec<String> = samples().keys().cloned().collect();
        sampled.sort();
        assert_eq!(
            tags, sampled,
            "every WorkerMessage variant needs a sample in services/common/contract/worker_messages.json"
        );
    }

    #[test]
    fn every_gateway_tag_is_one_the_worker_reads() {
        for tag in gateway_tags() {
            assert!(
                common::dlq::WORKER_MESSAGE_TYPES.contains(&tag.as_str()),
                "the gateway sends `{tag}` but the worker has no such type"
            );
        }
    }

    /// The samples are byte-for-byte what the gateway sends, so the worker's
    /// test parses real gateway output. `UPDATE_WORKER_CONTRACT=1` rewrites
    /// the gateway section from the gateway's own serialization.
    #[test]
    fn the_gateway_serializes_each_sample_exactly() {
        let mut out = serde_json::Map::new();
        let mut errors = Vec::new();
        for (tag, body) in samples() {
            let msg: WorkerMessage = match serde_json::from_value(body.clone()) {
                Ok(m) => m,
                Err(e) => {
                    errors.push(format!("{tag}: the gateway can't parse its sample: {e}"));
                    continue;
                }
            };
            let again = serde_json::to_value(&msg).unwrap();
            assert_eq!(
                again["type"],
                tag.as_str(),
                "{tag} serializes to another tag"
            );
            if again != body {
                errors.push(format!("{tag}: the gateway sends {again}"));
            }
            out.insert(tag, again);
        }
        if std::env::var("UPDATE_WORKER_CONTRACT").is_ok() {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../common/contract/worker_messages.json");
            let mut v: Value = serde_json::from_str(common::dlq::CONTRACT_JSON).unwrap();
            v["gateway"] = Value::Object(out);
            std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
            return;
        }
        assert!(errors.is_empty(), "{}", errors.join("\n"));
    }
}
