use serde::{Deserialize, Serialize};

pub struct Config {
    pub stage: String,
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
    pub ci_fix_queue_url: String,
    pub ticket_queue_url: String,
    pub feedback_queue_url: String,
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
                .unwrap_or_else(|_| "notifications@coderhelm.com".to_string()),
            ses_template_prefix: std::env::var("SES_TEMPLATE_PREFIX")
                .unwrap_or_else(|_| "coderhelm-prod".to_string()),
            mcp_proxy_function_name: std::env::var("MCP_PROXY_FUNCTION_NAME").unwrap_or_default(),
            ci_fix_queue_url: std::env::var("CI_FIX_QUEUE_URL").unwrap_or_default(),
            ticket_queue_url: std::env::var("TICKET_QUEUE_URL").unwrap_or_default(),
            feedback_queue_url: std::env::var("FEEDBACK_QUEUE_URL").unwrap_or_default(),
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
    pub github_app_id: String,
    pub github_private_key: String,
    /// Signs unsubscribe links (a key derived from it, never the secret
    /// itself); the gateway checks them with the same secret.
    #[serde(default)]
    pub jwt_secret: String,
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
    /// Periodic sweep (EventBridge → SQS) that nudges Teams about PRs awaiting
    /// review. Carries no fields — the sweep scans per-repo reviewer config.
    #[serde(rename = "review_reminder")]
    ReviewReminder,
    /// Periodic sweep (EventBridge → SQS) that re-runs reviews stuck on a
    /// review that didn't finish (a service error) for the PR's current head.
    #[serde(rename = "review_sweep")]
    ReviewSweep,
    /// Async post-merge health check (self-scheduled with SQS delay). Waits for
    /// the base-branch deploy checks to complete, then alerts only on failures
    /// that are NEW vs. the pre-merge baseline.
    #[serde(rename = "health_check")]
    HealthCheck(HealthCheckMessage),
    /// Armed auto-merge (self-scheduled). Merges the PR only once BOTH keys are
    /// present (bot APPROVE + human approval) AND every CI check is green — waits
    /// for pending checks (e.g. a staging deploy); never merges on failing CI.
    #[serde(rename = "await_merge")]
    AwaitMerge(AwaitMergeMessage),
    /// Coalesced release-tag sweep (self-scheduled with SQS delay). One is armed
    /// per repo by the first merge in a batch window; later merges in the window
    /// fold into it. When it fires it cuts ONE release tag at the default
    /// branch's current HEAD — skipping if that commit is already tagged.
    #[serde(rename = "tag_sweep")]
    TagSweep(TagSweepMessage),
    /// Release notes for a release tag CoderHelm just cut (or a re-send from the
    /// dashboard): GitHub Release + Confluence changelog entry + IT email.
    #[serde(rename = "release_notes")]
    ReleaseNotes(ReleaseNotesMessage),
    /// Code-graph index pass: full (on enable / repair) or incremental (a push
    /// to the default branch re-indexes only the changed files).
    #[serde(rename = "graph_index")]
    GraphIndex(GraphIndexMessage),
    /// Read the test reports of a finished workflow run or an upload.
    #[serde(rename = "test_ingest")]
    TestIngest(common::e2e::messages::TestIngest),
    /// Fix a flaky test, generate a recommended test, or file a ticket.
    #[serde(rename = "test_action")]
    TestAction(common::e2e::messages::TestAction),
    /// Read a connected skill repository's default branch again (a push to
    /// it, a new connection, or an admin's "Sync now").
    #[serde(rename = "skill_sync")]
    SkillSync(common::skills::SkillSyncMessage),
    /// The one PascalCase tag on the wire. It was left without a rename when
    /// it was added; gateway and worker agree on it, so it is pinned as is.
    #[serde(rename = "SkillAsk")]
    SkillAsk(common::skills::SkillAskMessage),
    /// Daily (EventBridge → SQS): archive to S3, then delete, DLQ messages the
    /// worker can't read once they are `common::dlq::CLEANUP_AFTER_DAYS` old.
    #[serde(rename = "dlq_cleanup")]
    DlqCleanup,
    /// The Monday weekly summary emails (EventBridge → SQS). Sends nothing
    /// unless WEEKLY_SUMMARY_ENABLED is true.
    #[serde(rename = "weekly_summary")]
    WeeklySummary,
    /// One test weekly summary for a team, to one address only.
    #[serde(rename = "weekly_summary_test")]
    WeeklySummaryTest(crate::passes::weekly_summary::WeeklySummaryTestMessage),
    /// Daily (EventBridge → SQS): compare our recorded usage with Anthropic's
    /// usage and cost report. Off unless the admin-key secret exists.
    #[serde(rename = "usage_reconcile")]
    UsageReconcile(UsageReconcileMessage),
    /// A model call whose usage couldn't be written when it was made (the
    /// worker or gateway queued it, `common::usage::SqsFallback`). Added to
    /// the team's counters once, ever, by its call id.
    #[serde(rename = "usage_record")]
    UsageRecord(common::usage::CallRecord),
    /// A lesson on where a fix belonged: CoderHelm's PR was closed without
    /// merging, or a person said the fix was outside the code.
    #[serde(rename = "lesson")]
    Lesson(LessonMessage),
    /// Hourly (EventBridge → SQS): start fix runs for new log findings of
    /// teams that turned automatic fixes on. Does nothing for anyone else.
    #[serde(rename = "log_autofix")]
    LogAutofix,
    /// People pushed back on a CoderHelm review (a reply, a dismissal, a close
    /// over its change request): judge whether the reviewer was wrong.
    #[serde(rename = "review_lesson")]
    ReviewLesson(ReviewLessonMessage),
    /// CoderHelm's PR closed: score what happened to it.
    #[serde(rename = "reward_outcome")]
    RewardOutcome(common::rewards::RewardOutcomeMessage),
    /// A person commented on a ticket CoderHelm worked on: read what the
    /// comment is before anything runs.
    #[serde(rename = "jira_comment")]
    JiraComment(JiraCommentMessage),
}

/// A ticket comment to read (gateway → worker). `action` is what it would
/// start if it asks for work: a ticket run, or feedback on the open PR.
#[derive(Serialize, Deserialize, Debug)]
pub struct JiraCommentMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub ticket_id: String,
    #[serde(default)]
    pub title: String,
    /// The ticket's latest run.
    pub run_id: String,
    pub run_status: String,
    #[serde(default)]
    pub pr_number: u64,
    #[serde(default)]
    pub repo_owner: String,
    #[serde(default)]
    pub repo_name: String,
    pub author: String,
    pub comment: String,
    /// The ticket's content hash now.
    #[serde(default)]
    pub context_hash: String,
    #[serde(default)]
    pub action: Option<Box<WorkerMessage>>,
}

/// A lesson job (gateway on a closed PR, or the feedback pass on a comment).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LessonMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub run_id: String,
    pub repo_owner: String,
    pub repo_name: String,
    #[serde(default)]
    pub pr_number: u64,
    /// `pr_closed` or `comment`.
    pub trigger: String,
    /// The lesson, when it was already judged (a comment, or a retry).
    #[serde(default)]
    pub lesson: Option<String>,
    /// The system the fix belonged in, when it was already judged.
    #[serde(default)]
    pub lesson_where: Option<String>,
    /// Retries while another run held the repository's memory.
    #[serde(default)]
    pub attempts: u32,
}

/// A judgment of people's pushback on a CoderHelm review (gateway → worker,
/// after a quiet period). Only the job for the PR's latest event judges.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReviewLessonMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// What started it (`common::review_lessons::TRIGGER_*`).
    #[serde(default)]
    pub trigger: String,
    /// The event's time (ms) on the PR's pending record.
    #[serde(default)]
    pub event_at: i64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct UsageReconcileMessage {
    /// `YYYY-MM-DD` (UTC) to compare; empty = yesterday.
    #[serde(default)]
    pub day: String,
}

impl WorkerMessage {
    /// The usage feature every model call made while handling this message
    /// is counted under (`common::usage::feature`).
    pub fn usage_feature(&self) -> &'static str {
        use common::usage::feature as f;
        match self {
            WorkerMessage::Ticket(t) if matches!(t.source, TicketSource::Alert) => f::ALERT,
            WorkerMessage::Ticket(_) | WorkerMessage::Resume(_) | WorkerMessage::MarkReady(_) => {
                f::RUN
            }
            WorkerMessage::Feedback(_) | WorkerMessage::JiraComment(_) => f::FEEDBACK,
            WorkerMessage::Lesson(_) | WorkerMessage::RewardOutcome(_) => f::MEMORY,
            WorkerMessage::Onboard(_) => f::ONBOARD,
            WorkerMessage::PlanExecute(_) | WorkerMessage::PlanTaskContinue(_) => f::PLAN,
            WorkerMessage::InfraAnalyze(_) => f::INFRA,
            WorkerMessage::Review(_)
            | WorkerMessage::ReviewLesson(_)
            | WorkerMessage::ReviewReminder
            | WorkerMessage::ReviewSweep => f::REVIEW,
            WorkerMessage::SkillSync(_) => f::SKILL_SYNC,
            WorkerMessage::SkillAsk(_) => f::AGENT_ASK,
            WorkerMessage::ReleaseNotes(_) | WorkerMessage::TagSweep(_) => f::RELEASE_NOTES,
            WorkerMessage::TestIngest(_) | WorkerMessage::TestAction(_) => f::TESTS,
            WorkerMessage::ToolProposals(_) => f::LIVE_SOURCES,
            WorkerMessage::HiddenSuggest(_) => f::DATA_SOURCE,
            WorkerMessage::HealthCheck(_)
            | WorkerMessage::AwaitMerge(_)
            | WorkerMessage::GraphIndex(_)
            | WorkerMessage::WeeklySummary
            | WorkerMessage::WeeklySummaryTest(_)
            | WorkerMessage::UsageReconcile(_)
            | WorkerMessage::UsageRecord(_)
            | WorkerMessage::DlqCleanup
            | WorkerMessage::LogAutofix => f::OTHER,
        }
    }
}

/// A code-graph index job. `changed_files: None` ⇒ full index of the branch
/// head; `Some(paths)` ⇒ incremental (delete + re-extract just those files).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GraphIndexMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    /// Branch the graph tracks (the repo's default branch).
    pub branch: String,
    #[serde(default)]
    pub changed_files: Option<Vec<String>>,
}

/// A coalesced release-tag sweep. Carries only routing + the batch clock; the
/// per-repo tag config is re-read at fire time so late config edits win.
/// Write + publish release notes for one release tag. Idempotent per tag: each
/// step's result is recorded, so a retry or re-send only redoes what's missing
/// (or what `resend` asks for).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReleaseNotesMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub tag: String,
    /// Commit the tag points at.
    pub sha: String,
    /// Dashboard re-send: "" (normal run), "email" (send the email again),
    /// "all" (regenerate the notes and re-publish every step).
    #[serde(default)]
    pub resend: String,
    /// Set for tags CoderHelm didn't cut (pushed by a person or CI): only write
    /// notes when the tag's commit is on the repo's release branch.
    #[serde(default)]
    pub check_branch: bool,
    /// Failed runs so far; a failure retries with a backoff before it alerts.
    #[serde(default)]
    pub attempt: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TagSweepMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    /// Default/base branch whose HEAD receives the release tag.
    pub base_branch: String,
    /// PR that opened this batch window — where any health alert lands.
    pub pr_number: u64,
    /// Seconds still to wait before the sweep fires. A single SQS message delay
    /// caps at 900s (15 min), so longer windows chain, decrementing this each
    /// hop; the sweep runs when it reaches 0.
    #[serde(default)]
    pub delay_remaining_secs: u64,
    /// The window this sweep belongs to (stored on the repo's marker). The
    /// sweep that removes the marker with its own id is the one that cuts the
    /// tag, so a "Release now" and the delayed sweep never both release.
    /// Empty on sweeps armed before ids existed.
    #[serde(default)]
    pub sweep_id: String,
    /// Asked for from the dashboard: cut even when auto-tagging is off.
    #[serde(default)]
    pub manual: bool,
    /// A retry after a failed cut: the marker was already handled.
    #[serde(default)]
    pub claimed: bool,
    /// Failed cuts so far (retries back off, then the failure is recorded).
    #[serde(default)]
    pub attempt: u32,
}

/// An armed auto-merge job. Bound to the reviewed head; a later commit aborts it
/// (the fresh review re-arms).
#[derive(Serialize, Deserialize, Debug, Clone)]
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

/// Post-merge health check job. Baseline = checks already failing at merge time,
/// so pre-existing failures are ignored ("not a previous error").
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthCheckMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    pub base_branch: String,
    pub merge_sha: String,
    /// Check-run names already failing at merge — excluded from new-failure alerts.
    #[serde(default)]
    pub baseline_failed: Vec<String>,
    /// How many times this check has re-enqueued while checks were still running
    /// (bounds the adaptive wait — no user-set timer).
    #[serde(default)]
    pub attempts: u32,
    /// Epoch seconds when the deploy checks FIRST went green. A green result
    /// starts the ≥15-minute watch window; 💚 posts only after the window ends
    /// with no new failures. None until first observed green.
    #[serde(default)]
    pub green_since: Option<i64>,
}

/// A label-triggered code-review job for an existing PR. Keyed by (pr_number,
/// head_sha) so re-labels and new commits are idempotent per commit.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReviewMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    pub head_sha: String,
    pub label: String,
    /// Present when a human replied asking a question — the pass answers instead
    /// of posting a verdict. Empty/None → normal verdict review.
    #[serde(default)]
    pub question: Option<String>,
    /// What triggered the review: "label" | "synchronize" | "reply". Audit only.
    #[serde(default)]
    pub trigger: String,
    /// Stable idempotency key for the TRIGGER (comment id for a reply, head sha
    /// for a label event, etc.). Claimed once at review entry so a duplicate
    /// webhook / SQS delivery of the same trigger produces exactly one review.
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
    /// How many times this review already failed for a service error and
    /// was sent again (0 on the first run).
    #[serde(default)]
    pub attempt: u32,
    /// Seconds still to wait before this job runs: a retry waits longer than
    /// one SQS delay allows, so it is sent on in hops until this reaches 0.
    #[serde(default)]
    pub wait_secs: u64,
    /// Set when CoderHelm's own reply on the PR concluded something the
    /// standing formal verdict doesn't match; the review checks it.
    #[serde(default)]
    pub reconcile_note: Option<String>,
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
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PlanTaskContinueMessage {
    pub team_id: String,
    pub plan_id: String,
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfraAnalyzeMessage {
    pub team_id: String,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResumeMessage {
    pub team_id: String,
    pub run_id: String,
    pub installation_id: u64,
    /// Times this resume was sent again after a transient model error.
    #[serde(default)]
    pub model_retries: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ImageAttachment {
    pub s3_key: String,
    pub media_type: String,
    pub filename: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    #[serde(default = "default_branch")]
    pub base_branch: String,
    #[serde(default)]
    pub image_attachments: Vec<ImageAttachment>,
    // --- Auto-continuation (bounded resume of a timed-out run) ---
    /// How many times this ticket has already been auto-continued. Hard cap.
    #[serde(default)]
    pub continuation: u32,
    /// Reuse this run_id across continuations so it's ONE run, not duplicates.
    #[serde(default)]
    pub continuation_run_id: Option<String>,
    /// Epoch ms of the first attempt — the wall-clock runaway backstop.
    #[serde(default)]
    pub first_attempt_ms: u64,
    /// Times this run was sent again after a transient model error (a 5xx,
    /// 429, 529 or network failure that outlasted the client's own retries).
    /// The run keeps its run_id across these, like a continuation.
    #[serde(default)]
    pub model_retries: u32,
    /// Set in-process when the ticket changed under an open PR: the implement
    /// agent gets this note (old→new ticket diff + PR diff) and updates the
    /// existing PR instead of starting over. Never serialized.
    #[serde(skip)]
    pub ticket_update_note: Option<String>,
    /// Set in-process: the ticket's history with CoderHelm (earlier PRs and
    /// how they ended, lessons, what people wrote), for the planner. Never
    /// serialized.
    #[serde(skip)]
    pub ticket_history_note: Option<String>,
    /// Set in-process: the key a plan written for this run is cached under
    /// (see `plan::plan_cache_key`). Never serialized.
    #[serde(skip)]
    pub plan_cache_key: Option<String>,
    /// Set in-process, once per run: the team's own instructions for this
    /// repository (see `passes::load_team_instructions`). Every agent that
    /// plans, writes, proves or describes the change gets them. Never
    /// serialized.
    #[serde(skip)]
    pub team_instructions: String,
    /// A person's direction that started this run (a reply on the ticket
    /// telling CoderHelm where the fix is and to go ahead), as
    /// "author: words". The planner's trace checks then inform, not block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub person_direction: Option<String>,
}

fn default_branch() -> String {
    "main".to_string()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TicketSource {
    Github,
    Jira,
    /// A monitoring alert (e.g. a CloudWatch alarm) routed to a repo.
    Alert,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    /// How many times this feedback has been re-enqueued because the per-run
    /// writer slot was busy. Absent on messages from the gateway (defaults to 0);
    /// bounds the defer loop so a wedged slot can't churn indefinitely.
    #[serde(default)]
    pub defer_count: u32,
    /// Times this feedback was sent again after a transient model error.
    #[serde(default)]
    pub model_retries: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReviewComment {
    pub path: String,
    pub line: Option<u64>,
    pub body: String,
    #[serde(default)]
    pub comment_id: Option<u64>,
    /// GraphQL node ID for resolving the review thread.
    #[serde(default)]
    pub node_id: Option<String>,
    /// When true, this comment is prior thread context — not a comment to reply to.
    #[serde(default)]
    pub is_context: bool,
}

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

/// The gateway → worker contract (see `common::dlq::CONTRACT_JSON`): every
/// message the gateway or a schedule sends is a type this enum reads.
#[cfg(test)]
mod worker_contract_tests {
    use super::WorkerMessage;
    use serde_json::Value;
    use std::collections::BTreeSet;

    fn contract() -> Value {
        serde_json::from_str(common::dlq::CONTRACT_JSON).unwrap()
    }

    /// Every tag this enum accepts, read from serde itself.
    fn worker_tags() -> BTreeSet<String> {
        let err = serde_json::from_str::<WorkerMessage>(r#"{"type":"__no_such_type__"}"#)
            .unwrap_err()
            .to_string();
        common::dlq::accepted_tags(&err).into_iter().collect()
    }

    #[test]
    fn the_shared_type_list_is_exactly_this_enums_tags() {
        let listed: BTreeSet<String> = common::dlq::WORKER_MESSAGE_TYPES
            .iter()
            .map(|t| t.to_string())
            .collect();
        assert_eq!(
            worker_tags(),
            listed,
            "common::dlq::WORKER_MESSAGE_TYPES must list exactly the tags WorkerMessage reads"
        );
    }

    /// Each sample (the gateway's exact output, and each scheduled job) parses
    /// here and serializes back to the same tag.
    #[test]
    fn the_worker_reads_every_gateway_and_scheduled_message() {
        let c = contract();
        let mut n = 0;
        for section in ["gateway", "scheduled"] {
            for (tag, body) in c[section].as_object().unwrap() {
                let msg: WorkerMessage = serde_json::from_value(body.clone()).unwrap_or_else(|e| {
                    panic!("the worker can't read the {section} message `{tag}`: {e}")
                });
                let again = serde_json::to_value(&msg).unwrap();
                assert_eq!(again["type"], tag.as_str(), "{section}.{tag}");
                n += 1;
            }
        }
        assert!(n >= 25, "only {n} samples");
    }

    /// Every message an EventBridge schedule in infra sends is in the
    /// contract's scheduled section (so the test above parses it).
    #[test]
    fn every_scheduled_message_in_infra_is_in_the_contract() {
        let c = contract();
        let infra = [
            include_str!("../../../infra/lib/api-stack.ts"),
            include_str!("../../../infra/lib/worker-stack.ts"),
        ];
        let mut found = 0;
        for src in infra {
            for chunk in src.split("RuleTargetInput.fromObject(").skip(1) {
                let Some(t) = chunk
                    .split("type:")
                    .nth(1)
                    .and_then(|r| r.split('"').nth(1))
                else {
                    continue;
                };
                assert!(
                    c["scheduled"].get(t).is_some(),
                    "infra schedules `{t}`, which is missing from the contract's scheduled section"
                );
                found += 1;
            }
        }
        assert!(found >= 5, "found only {found} schedules");
    }

    #[test]
    fn a_pascal_case_skill_sync_is_refused() {
        let e = serde_json::from_str::<WorkerMessage>(
            r#"{"type":"SkillSync","team_id":"TEAM#team-a","repo":"acme/skills","recheck":true}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("unknown variant `SkillSync`"), "{e}");
    }
}
