//! The messages the worker reads from its queues, in one place.
//!
//! The gateway sends them, EventBridge schedules send them, and the worker
//! sends some to itself (retries, chained waits). Both services use these
//! types (`gateway::models` and `worker::models` re-export them), so the wire
//! contract cannot drift between two copies.

use serde::{Deserialize, Serialize};

/// Send one test weekly summary email for a team to one address.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct WeeklySummaryTestMessage {
    pub team_id: String,
    pub to: String,
}

/// Every message on the worker queues. The gateway, the schedules and the
/// worker all read and write this one enum, so a tag one side sends is always
/// one the other reads.
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
    /// Hosted side of hybrid: forward a held event to a self-hosted install
    /// again (self-scheduled with SQS delay, about 24 hours of backoff).
    #[serde(rename = "relay_retry")]
    RelayRetry(RelayRetryMessage),
    /// An event for the team's webhook endpoints (`crate::webhooks`), put on
    /// the webhooks queue by `emit_event` wherever the work happened.
    #[serde(rename = "webhook_emit")]
    WebhookEmit(WebhookEmitMessage),
    /// One more try at a webhook delivery (self-scheduled with SQS delay;
    /// a wait longer than SQS allows is carried by `due_at`).
    #[serde(rename = "webhook_send")]
    WebhookSend(WebhookSendMessage),
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
    TestIngest(crate::e2e::messages::TestIngest),
    /// Fix a flaky test, generate a recommended test, or file a ticket.
    #[serde(rename = "test_action")]
    TestAction(crate::e2e::messages::TestAction),
    /// Read a connected skill repository's default branch again (a push to
    /// it, a new connection, or an admin's "Sync now").
    #[serde(rename = "skill_sync")]
    SkillSync(crate::skills::SkillSyncMessage),
    /// The one PascalCase tag on the wire. It was left without a rename when
    /// it was added; gateway and worker agree on it, so it is pinned as is.
    #[serde(rename = "SkillAsk")]
    SkillAsk(crate::skills::SkillAskMessage),
    /// Daily (EventBridge → SQS): archive to S3, then delete, DLQ messages the
    /// worker can't read once they are `crate::dlq::CLEANUP_AFTER_DAYS` old.
    #[serde(rename = "dlq_cleanup")]
    DlqCleanup,
    /// Every 15 minutes (EventBridge → SQS): queue a sync for each knowledge
    /// source whose next scheduled sync is due.
    #[serde(rename = "knowledge_sync_sweep")]
    KnowledgeSyncSweep,
    /// Sync one knowledge source (queued by the sweep).
    #[serde(rename = "knowledge_sync")]
    KnowledgeSync(crate::knowledge::sources::SyncMessage),
    /// The Monday weekly summary emails (EventBridge → SQS). Sends nothing
    /// unless WEEKLY_SUMMARY_ENABLED is true.
    #[serde(rename = "weekly_summary")]
    WeeklySummary,
    /// One test weekly summary for a team, to one address only.
    #[serde(rename = "weekly_summary_test")]
    WeeklySummaryTest(WeeklySummaryTestMessage),
    /// Daily (EventBridge → SQS): compare our recorded usage with Anthropic's
    /// usage and cost report. Off unless the admin-key secret exists.
    #[serde(rename = "usage_reconcile")]
    UsageReconcile(UsageReconcileMessage),
    /// A model call whose usage couldn't be written when it was made (the
    /// worker or gateway queued it, `crate::usage::SqsFallback`). Added to
    /// the team's counters once, ever, by its call id.
    #[serde(rename = "usage_record")]
    UsageRecord(crate::usage::CallRecord),
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
    /// CoderHelm's PR closed: score what happened to it (`crate::rewards`).
    #[serde(rename = "reward_outcome")]
    RewardOutcome(crate::rewards::RewardOutcomeMessage),
    /// A person commented on a ticket CoderHelm worked on: read what the
    /// comment is before anything runs.
    #[serde(rename = "jira_comment")]
    JiraComment(JiraCommentMessage),
    /// Every few minutes (EventBridge → SQS): send again the work kept while
    /// a team's agents were paused, once the pause has ended (an until-time
    /// passing needs nobody to click anything).
    #[serde(rename = "agent_resume_sweep")]
    AgentResumeSweep,
    /// One plugin run: a brief, a schedule or a release (see
    /// `plugins::run`).
    #[serde(rename = "plugin_run")]
    PluginRun(PluginRunMessage),
    /// Run an approved draft's consequential tool (now, or when its one-off
    /// schedule fires).
    #[serde(rename = "plugin_publish")]
    PluginPublish(PluginPublishMessage),
    /// Read a plugin source again, and offer its plugins' new versions (a
    /// push webhook, or an admin's "Sync now").
    #[serde(rename = "plugin_sync")]
    PluginSync(PluginSyncMessage),
    /// Every 15 minutes (EventBridge → SQS): poll the plugin sources that
    /// no webhook covers.
    #[serde(rename = "plugin_sweep")]
    PluginSweep,
    /// A plugin's recurring `schedule` trigger fired.
    #[serde(rename = "plugin_trigger")]
    PluginTrigger(PluginTriggerMessage),
    /// Read a published draft's numbers back, as a lesson for later runs.
    #[serde(rename = "plugin_results")]
    PluginResults(PluginResultsMessage),
}

/// One plugin run. `hash` is the plugin version it was asked against; the
/// run refuses when the plugin moved to another version meanwhile.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PluginRunMessage {
    pub team_id: String,
    pub run_id: String,
    pub plugin_id: String,
    #[serde(default)]
    pub hash: String,
    /// `manual`, `schedule` or `release`.
    pub trigger: String,
    /// The brief form's values, or the release's notes.
    #[serde(default)]
    pub inputs: std::collections::BTreeMap<String, serde_json::Value>,
    /// The repository the run is about, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// An email, `slack:<user>`, `teams:<user>`, `schedule` or `release`.
    pub requested_by: String,
    /// The agent the trigger names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

/// Publish one approved draft version.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PluginPublishMessage {
    pub team_id: String,
    pub artifact_id: String,
    pub version: u32,
    pub approval_id: String,
}

/// Sync a plugin source (and offer its plugins' new versions), or one
/// installed plugin. `reason`: `webhook`, `admin` or `poll`.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PluginSyncMessage {
    pub team_id: String,
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub plugin_id: String,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub reason: String,
}

/// A plugin's recurring schedule fired: trigger `trigger` of its manifest.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PluginTriggerMessage {
    pub team_id: String,
    pub plugin_id: String,
    pub trigger: usize,
}

/// Read a published draft's results.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PluginResultsMessage {
    pub team_id: String,
    pub artifact_id: String,
}

/// A ticket comment to read (gateway → worker). `action` is what it would
/// start if it asks for work: a ticket run, or feedback on the open PR.
#[derive(Serialize, Deserialize, Debug, Default)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ReviewLessonMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
    /// What started it (`crate::review_lessons::TRIGGER_*`).
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
    /// is counted under (`crate::usage::feature`).
    pub fn usage_feature(&self) -> &'static str {
        use crate::usage::feature as f;
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
            WorkerMessage::KnowledgeSync(_) => f::EMBEDDING,
            WorkerMessage::HiddenSuggest(_) => f::DATA_SOURCE,
            WorkerMessage::HealthCheck(_)
            | WorkerMessage::AwaitMerge(_)
            | WorkerMessage::RelayRetry(_)
            | WorkerMessage::WebhookEmit(_)
            | WorkerMessage::WebhookSend(_)
            | WorkerMessage::GraphIndex(_)
            | WorkerMessage::WeeklySummary
            | WorkerMessage::WeeklySummaryTest(_)
            | WorkerMessage::UsageReconcile(_)
            | WorkerMessage::UsageRecord(_)
            | WorkerMessage::DlqCleanup
            | WorkerMessage::KnowledgeSyncSweep
            | WorkerMessage::AgentResumeSweep
            | WorkerMessage::LogAutofix => f::OTHER,
            WorkerMessage::PluginRun(_)
            | WorkerMessage::PluginPublish(_)
            | WorkerMessage::PluginSync(_)
            | WorkerMessage::PluginSweep
            | WorkerMessage::PluginTrigger(_)
            | WorkerMessage::PluginResults(_) => f::PLUGIN,
        }
    }

    /// Work that runs an agent for one team, as (team, what it is, run id):
    /// what "Pause all agents" holds back. Sweeps and bookkeeping aren't.
    pub fn agent_work(&self) -> Option<(&str, String, String)> {
        match self {
            WorkerMessage::Ticket(t) => Some((
                &t.team_id,
                format!(
                    "Run for {}: {}",
                    t.ticket_id,
                    crate::truncate_str(&t.title, 120)
                ),
                t.continuation_run_id.clone().unwrap_or_default(),
            )),
            WorkerMessage::Feedback(m) => Some((
                &m.team_id,
                format!("Feedback on pull request #{}", m.pr_number),
                m.run_id.clone(),
            )),
            WorkerMessage::Resume(m) => Some((&m.team_id, "Resume a run".into(), m.run_id.clone())),
            WorkerMessage::Review(m) => Some((
                &m.team_id,
                format!(
                    "Review of {}/{} #{}",
                    m.repo_owner, m.repo_name, m.pr_number
                ),
                String::new(),
            )),
            WorkerMessage::PlanExecute(m) => {
                Some((&m.team_id, format!("Plan {}", m.plan_id), String::new()))
            }
            WorkerMessage::PlanTaskContinue(m) => Some((
                &m.team_id,
                format!("Plan {} tasks", m.plan_id),
                String::new(),
            )),
            WorkerMessage::JiraComment(m) => {
                Some((&m.team_id, "A ticket comment".into(), String::new()))
            }
            WorkerMessage::SkillAsk(m) => {
                Some((&m.team_id, "Try a team agent".into(), String::new()))
            }
            WorkerMessage::TestAction(m) => Some((&m.team_id, "A test fix".into(), String::new())),
            WorkerMessage::PluginRun(m) => Some((
                &m.team_id,
                format!("Plugin {} run", m.plugin_id),
                m.run_id.clone(),
            )),
            _ => None,
        }
    }
}

/// A code-graph index job. `changed_files: None` ⇒ full index of the branch
/// head; `Some(paths)` ⇒ incremental (delete + re-extract just those files).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

/// An event for the team's webhook endpoints.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookEmitMessage {
    /// The event's team, at the top so a dead-lettered message is the team's.
    #[serde(default)]
    pub team_id: String,
    pub event: crate::webhooks::Event,
}

/// One webhook delivery to try (again).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct WebhookSendMessage {
    pub team_id: String,
    pub endpoint_id: String,
    pub event_id: String,
    /// The delivery record the attempt goes on.
    pub delivery_id: String,
    /// Unix seconds: not before this.
    #[serde(default)]
    pub due_at: i64,
}

/// A relay forward to try again (hosted side of hybrid).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RelayRetryMessage {
    pub install_id: String,
    /// The relay's delivery record (`HYBRID#<install>`, `DELIVERY#<id>`).
    pub delivery_id: String,
    /// The id the install refuses replays by (GitHub's delivery id).
    pub relay_delivery_id: String,
    pub event: String,
    #[serde(default = "github_source")]
    pub source: String,
}

fn github_source() -> String {
    "github".into()
}

/// Post-merge health check job. Baseline = checks already failing at merge time,
/// so pre-existing failures are ignored ("not a previous error").
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MarkReadyMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct PlanExecuteMessage {
    pub team_id: String,
    pub plan_id: String,
    pub triggered_by: String,
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct PlanTaskContinueMessage {
    pub team_id: String,
    pub plan_id: String,
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct InfraAnalyzeMessage {
    pub team_id: String,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ResumeMessage {
    pub team_id: String,
    pub run_id: String,
    pub installation_id: u64,
    /// Times this resume was sent again after a transient model error.
    #[serde(default)]
    pub model_retries: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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
    pub requester: Option<crate::requester::Requester>,
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

impl Default for TicketMessage {
    /// An empty ticket on `main`, as a message without `base_branch` reads.
    fn default() -> Self {
        Self {
            team_id: String::new(),
            installation_id: 0,
            source: TicketSource::Github,
            ticket_id: String::new(),
            title: String::new(),
            body: String::new(),
            repo_owner: String::new(),
            repo_name: String::new(),
            issue_number: 0,
            sender: String::new(),
            requester: None,
            base_branch: default_branch(),
            image_attachments: Vec::new(),
            continuation: 0,
            continuation_run_id: None,
            first_attempt_ms: 0,
            model_retries: 0,
            ticket_update_note: None,
            ticket_history_note: None,
            plan_cache_key: None,
            team_instructions: String::new(),
            person_direction: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum TicketSource {
    #[default]
    Github,
    Jira,
    /// A monitoring alert (e.g. a CloudWatch alarm) routed to a repo.
    Alert,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
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

// --- Onboard types ---

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct OnboardMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repos: Vec<OnboardRepo>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OnboardRepo {
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

/// Work out suggested hidden fields for one database source from its table
/// and column names. `force` asks again even when the names haven't changed.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct HiddenSuggestMessage {
    pub team_id: String,
    pub source_id: String,
    #[serde(default)]
    pub force: bool,
}

/// Tool definitions changed in a repo's default branch: each becomes a
/// proposed version waiting for an admin's approval. Never approved here.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ToolProposalsMessage {
    pub team_id: String,
    pub installation_id: u64,
    pub repo_owner: String,
    pub repo_name: String,
    pub sha: String,
    pub paths: Vec<String>,
}

/// The gateway → worker contract (see `crate::dlq::CONTRACT_JSON`): every
/// message the gateway or a schedule sends is a type this enum reads.
#[cfg(test)]
mod worker_contract_tests {
    use super::WorkerMessage;
    use serde_json::Value;
    use std::collections::BTreeSet;

    fn contract() -> Value {
        serde_json::from_str(crate::dlq::CONTRACT_JSON).unwrap()
    }

    /// Every tag this enum accepts, read from serde itself.
    fn worker_tags() -> BTreeSet<String> {
        let err = serde_json::from_str::<WorkerMessage>(r#"{"type":"__no_such_type__"}"#)
            .unwrap_err()
            .to_string();
        crate::dlq::accepted_tags(&err).into_iter().collect()
    }

    #[test]
    fn the_shared_type_list_is_exactly_this_enums_tags() {
        let listed: BTreeSet<String> = crate::dlq::WORKER_MESSAGE_TYPES
            .iter()
            .map(|t| t.to_string())
            .collect();
        assert_eq!(
            worker_tags(),
            listed,
            "crate::dlq::WORKER_MESSAGE_TYPES must list exactly the tags WorkerMessage reads"
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
