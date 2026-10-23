//! Label-triggered PR code review. A PR gains the repo's review label (or a
//! human replies to the bot) → gateway enqueues a `Review` job → this pass
//! fetches the diff, asks the model for a verdict + risk + findings against the
//! repo's rules/instructions, posts a GitHub review (APPROVE / REQUEST_CHANGES,
//! never a bare COMMENT; GitHub forbids either verdict on the bot's own PR, so
//! there it posts as a COMMENT whose body carries the verdict), persists a review record for
//! the dashboard, and — only when the repo opts in — runs post-approval actions.
//!
//! Never approves on an error. A review that didn't finish (a model API
//! failure, an unreadable answer, a GitHub error) has no verdict: it is never
//! posted as a block. It is sent again with a delay, and once every retry has
//! failed the PR gets a short note while the previous verdict stands.
//! Auto-merge/tag/deploy + the health guard live in `review_actions` and ship
//! OFF by default; this pass only ever *reviews* unless a repo turns them on.

use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::{ReviewMessage, TokenUsage};
use crate::passes::{attr_n, attr_s, review_agent, review_risk};
use crate::WorkerState;
use tracing::{info, warn};

/// Per-repo reviewer config (worker-side mirror of the gateway's). OFF by
/// default — a second gate so a mis-fired enqueue still can't review a repo the
/// owner never opted in.
struct ReviewConfig {
    enabled: bool,
    killed: bool,
    instructions: String,
    /// Run the affected tests/build in the sandbox and attach pass/fail receipts.
    verify_tests: bool,
    /// New or changed behavior must come with new/updated tests (blocking finding).
    require_tests: bool,
    /// Reviewer picks CI labels for the PR (see review_labels).
    auto_labels: bool,
    label_rules: super::review_labels::LabelRules,
}

/// The repo's reviewer config. A failed read is an error (the review job
/// fails and is retried), never "review off".
async fn load_config(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<ReviewConfig, Box<dyn std::error::Error + Send + Sync>> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .map_err(|e| {
            format!(
                "reviewer config read failed: {}",
                aws_sdk_dynamodb::error::DisplayErrorContext(&e)
            )
        })?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(ReviewConfig {
            enabled: false,
            killed: false,
            instructions: String::new(),
            verify_tests: false,
            require_tests: false,
            auto_labels: false,
            label_rules: super::review_labels::LabelRules::default(),
        });
    };
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    Ok(ReviewConfig {
        enabled: item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        killed: item
            .get("killed")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        instructions: item
            .get("instructions")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        verify_tests: item
            .get("verify_tests")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        require_tests: item
            .get("require_tests")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        auto_labels: item
            .get("auto_labels")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        label_rules: super::review_labels::LabelRules::parse(
            &s("auto_label_allow"),
            &s("auto_label_requires"),
            &s("auto_label_guide"),
        ),
    })
}

/// True when the repo turned on "Require tests" (and review isn't off/killed).
/// The same switch makes the reviewer block untested behavior changes and makes
/// CoderHelm's own coding passes write those tests.
pub(crate) async fn tests_required(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> bool {
    // Off when the config can't be read: the coding passes then just don't
    // add the policy's tests (the reviewer itself fails and retries).
    load_config(state, team_id, owner, name)
        .await
        .is_ok_and(|cfg| cfg.enabled && !cfg.killed && cfg.require_tests)
}

/// The repo's review trigger label IF the reviewer is enabled (and not killed),
/// else None. Lets CoderHelm self-label the PRs it opens: the reviewer gates
/// EVERY PR on the label, so a bot PR must carry it to be reviewed + self-fixed.
/// Defaults to `ch-review` when the field is unset.
pub(crate) async fn enabled_trigger_label(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Option<String> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())?;
    let enabled = item
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let killed = item
        .get("killed")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    if !enabled || killed {
        return None;
    }
    Some(
        item.get("label")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ch-review".to_string()),
    )
}

/// Team-wide (org-level) review instructions applied to EVERY repo's review, on
/// top of the per-repo config + AGENTS.md. Stored at sk `REVIEW_CONFIG#GLOBAL`.
/// Empty when unset.
async fn load_org_instructions(state: &WorkerState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("REVIEW_CONFIG#GLOBAL"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|it| it.get("instructions").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default()
}

const RATING_FOOTER: &str =
    "\n\n---\n_Was this review helpful? Rate it 👍 / 👎 and add notes in the \
CoderHelm dashboard so the reviewer learns. Reply **@coderhelm re-review** to re-run against the \
latest commit, or **@coderhelm <question>** to ask._";

/// How long a processed review trigger stays claimed (consume-once dedup). Long
/// enough to outlast any duplicate webhook / SQS redelivery of the SAME trigger
/// (seconds–minutes apart), short enough that a review which errored out can be
/// re-driven for the same head once it expires. Keys that are unique per action
/// (a comment id) are unaffected by the length; head-based keys get this window.
const REVIEW_JOB_DEDUP_TTL_SECS: u64 = 6 * 3600;

/// How long one review job may run: below the worker's 15-minute Lambda
/// timeout, so a slow review fails (and is retried) instead of being killed.
const REVIEW_BUDGET_SECS: u64 = 13 * 60;

/// The job claim while a review runs: past the budget plus the queue's
/// visibility timeout, so a job killed outright can be redelivered and run.
const REVIEW_LEASE_SECS: u64 = 17 * 60;

/// The longest delay one SQS message can carry.
const SQS_MAX_DELAY_SECS: u64 = 900;

/// What to do after a review didn't finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailurePlan {
    /// Send the job again after this many seconds.
    Retry { delay_secs: u64 },
    /// Every retry failed: tell the PR, keep the previous verdict.
    GiveUp,
}

/// The plan for a review that failed on run `attempt` (0 = the first run).
pub(crate) fn failure_plan(attempt: u32) -> FailurePlan {
    match common::model_retry::requeue_delay(attempt) {
        Some(delay_secs) => FailurePlan::Retry { delay_secs },
        None => FailurePlan::GiveUp,
    }
}

/// The job that retries `msg` after `delay_secs`, and the delay of its first
/// SQS hop (the rest rides in `wait_secs`). A verdict review is pinned to the
/// head it was reviewing, so a newer commit's own review supersedes it; a
/// question keeps resolving the latest head. Each retry has its own dedup key.
pub(crate) fn retry_message(
    msg: &ReviewMessage,
    reviewed_head: &str,
    delay_secs: u64,
) -> (ReviewMessage, u64) {
    let mut next = msg.clone();
    next.attempt = msg.attempt + 1;
    if next.question.is_none() && !reviewed_head.is_empty() {
        next.head_sha = reviewed_head.to_string();
    }
    // The original trigger's key, without an earlier retry's prefix.
    let base = match msg.dedup_key.strip_prefix("retry#") {
        Some(rest) => rest
            .split_once('#')
            .map(|(_, k)| k)
            .unwrap_or(rest)
            .to_string(),
        None if msg.dedup_key.is_empty() => format!("head#{reviewed_head}"),
        None => msg.dedup_key.clone(),
    };
    next.dedup_key = format!("retry#{}#{base}", next.attempt);
    let hop = delay_secs.min(SQS_MAX_DELAY_SECS);
    next.wait_secs = delay_secs - hop;
    (next, hop)
}

/// The note on the PR once every retry failed. It is a plain comment, not a
/// review: it neither blocks nor clears, and the previous verdict stands.
pub(crate) fn gave_up_note(msg: &ReviewMessage, head: &str) -> String {
    let short = &head[..head.len().min(7)];
    if msg.question.is_some() {
        "I couldn't answer this because of a service error on my side. Ask again          (mention **@coderhelm**) and I'll pick it up."
            .to_string()
    } else if short.is_empty() {
        "CoderHelm's review didn't finish because of a service error, so there's no new          verdict and the previous one is unchanged. It runs again on the next push, or comment          **@coderhelm re-review** to run it now."
            .to_string()
    } else {
        format!(
            "CoderHelm's review of `{short}` didn't finish because of a service error, so              there's no new verdict and the previous one is unchanged. It runs again on the next              push, or comment **@coderhelm re-review** to run it now."
        )
    }
}

/// Sends a review job to the worker queue with an SQS delay (at most 900s).
pub(crate) async fn enqueue_review(
    state: &WorkerState,
    msg: &ReviewMessage,
    delay_secs: u64,
) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        warn!(
            pr = msg.pr_number,
            "TICKET_QUEUE_URL not set; can't send a review job"
        );
        return false;
    }
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(super::review_sweep::review_job_body(msg))
        .delay_seconds(delay_secs.min(SQS_MAX_DELAY_SECS) as i32)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %aws_sdk_sqs::error::DisplayErrorContext(&e), "Could not send the review job");
            false
        }
    }
}

/// A review job as the dashboard sees it (`common::review_live`): its id, and
/// when it started. Only this job may end the status it set.
pub(crate) struct LiveJob {
    pub id: String,
    pub started_at: i64,
}

impl LiveJob {
    pub(crate) fn start() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            started_at: chrono::Utc::now().timestamp(),
        }
    }

    /// The PR is being reviewed by this job now.
    async fn reviewing(&self, state: &WorkerState, msg: &ReviewMessage, head: &str, title: &str) {
        common::review_live::mark_reviewing(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &common::review_live::Reviewing {
                job: &self.id,
                head_sha: head,
                title,
                trigger: &msg.trigger,
                is_question: msg.question.is_some(),
                attempt: msg.attempt,
                budget_secs: REVIEW_LEASE_SECS as i64,
            },
        )
        .await;
    }

    async fn finished(
        &self,
        state: &WorkerState,
        msg: &ReviewMessage,
        how: common::review_live::Finished<'_>,
    ) {
        common::review_live::mark_finished(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &self.id,
            self.started_at,
            how,
        )
        .await;
    }
}

/// Show a review job sent by the worker itself (the stuck-review sweep) as
/// queued on the dashboard.
pub(crate) async fn note_queued(state: &WorkerState, msg: &ReviewMessage, delay_secs: u64) {
    common::review_live::mark_queued(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
        &msg.trigger,
        msg.question.is_some(),
        delay_secs as i64,
    )
    .await;
}

/// Runs one review job. A job that fails is retried or, after the last
/// retry, reported on the PR; it never posts a verdict for a failure.
pub async fn run(
    state: &WorkerState,
    msg: ReviewMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A retry that must wait longer than one SQS delay: send it on.
    if msg.wait_secs > 0 {
        let mut next = msg.clone();
        let hop = msg.wait_secs.min(SQS_MAX_DELAY_SECS);
        next.wait_secs = msg.wait_secs - hop;
        if !enqueue_review(state, &next, hop).await {
            return Err("could not send a waiting review retry on".into());
        }
        return Ok(());
    }
    let job = LiveJob::start();
    let mut reviewed_head = msg.head_sha.clone();
    match run_budgeted(state, msg.clone(), &mut reviewed_head, &job).await {
        Ok(Outcome::Done { .. }) => {
            job.finished(state, &msg, common::review_live::Finished::Done)
                .await;
            Ok(())
        }
        Ok(Outcome::Skipped { reason }) => {
            job.finished(
                state,
                &msg,
                common::review_live::Finished::Skipped { reason: &reason },
            )
            .await;
            Ok(())
        }
        // A reply that disagrees with the standing formal verdict: the formal
        // review runs first, in this job, and the reply is posted after it so
        // the two never contradict each other.
        Ok(Outcome::ReviewThenReply { review, reply }) => {
            let mut head = review.head_sha.clone();
            let formal = match run_budgeted(state, (*review).clone(), &mut head, &job).await {
                Ok(Outcome::Done { posted, .. }) => FormalResult::Posted(posted),
                Ok(Outcome::ReviewThenReply { .. }) | Ok(Outcome::Skipped { .. }) => {
                    FormalResult::Posted(None)
                }
                Err(e) => {
                    review_failed(state, &review, &head, &e.to_string(), &job).await;
                    FormalResult::Failed
                }
            };
            let out = post_reply(state, &msg, *reply, formal).await;
            // A failed formal review already shows its retry.
            if formal != FormalResult::Failed {
                job.finished(state, &msg, common::review_live::Finished::Done)
                    .await;
            }
            out
        }
        Err(e) => {
            review_failed(state, &msg, &reviewed_head, &e.to_string(), &job).await;
            Ok(())
        }
    }
}

/// What one run of a review job did.
pub(crate) enum Outcome {
    /// Finished. `posted` is the formal review event it posted, if any;
    /// `run_review` is the verdict for the run that asked (trigger "run").
    Done {
        posted: Option<&'static str>,
        run_review: Option<RunReview>,
    },
    /// Nothing to review: a push after CoderHelm approved changed no code or
    /// data. The approval stands; `reason` is shown in the live status.
    Skipped { reason: String },
    /// A reply is ready, but its conclusion disagrees with CoderHelm's
    /// standing formal verdict: run `review` first, then post `reply`.
    ReviewThenReply {
        review: Box<ReviewMessage>,
        reply: Box<PendingReply>,
    },
}

/// A reply waiting for the formal review it depends on.
pub(crate) struct PendingReply {
    pub head_sha: String,
    pub answer: String,
    pub consulted: String,
    pub applied_standards: Vec<String>,
}

/// How the formal review that a reply waited for went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormalResult {
    Posted(Option<&'static str>),
    Failed,
}

/// The reply as posted after the formal review it waited for. It never
/// stands next to a formal verdict that says the opposite: when the formal
/// review still requests changes, the reply points at it instead.
pub(crate) fn reply_after_formal(answer: &str, head: &str, formal: FormalResult) -> String {
    let short = &head[..head.len().min(7)];
    match formal {
        FormalResult::Posted(Some("REQUEST_CHANGES")) => format!(
            "I re-ran the full review on `{short}` before answering, and it still requests \
             changes; the review above says what's left."
        ),
        FormalResult::Posted(Some("APPROVE")) => {
            format!("{answer}\n\nThe formal review on `{short}` is updated: approved.")
        }
        FormalResult::Posted(Some(_)) => format!(
            "{answer}\n\nThe formal review on `{short}` is updated: nothing in it holds the merge."
        ),
        FormalResult::Posted(None) => answer.to_string(),
        FormalResult::Failed => format!(
            "I re-ran the formal review on `{short}`, but it didn't finish because of a service \
             error. It retries on its own and the verdict updates when it does."
        ),
    }
}

/// Posts a reply that waited for a formal review, and records it.
async fn post_reply(
    state: &WorkerState,
    msg: &ReviewMessage,
    reply: PendingReply,
    formal: FormalResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let answer = reply_after_formal(&reply.answer, &reply.head_sha, formal);
    send_reply(
        state,
        &github,
        msg,
        &reply.head_sha,
        &answer,
        &reply.consulted,
        &reply.applied_standards,
        &TokenUsage::default(),
    )
    .await
}

/// Posts a reply where it was asked (the inline thread, or the PR
/// conversation) and records it as a QUESTION record.
#[allow(clippy::too_many_arguments)]
async fn send_reply(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReviewMessage,
    head_sha: &str,
    answer: &str,
    consulted: &str,
    applied_standards: &[String],
    usage: &TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = format!("{answer}{consulted}{RATING_FOOTER}");
    match msg.reply_to_comment_id {
        Some(comment_id) => {
            github
                .reply_to_review_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    comment_id,
                    &body,
                )
                .await?;
        }
        None => {
            github
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &body)
                .await?;
        }
    }
    let record_sk = store_review_record(
        state,
        msg,
        head_sha,
        "QUESTION",
        "N/A",
        answer,
        "COMMENT",
        "",
        &common::review_stats::ReviewCounts {
            posted: true,
            ..Default::default()
        },
    )
    .await;
    record_review_usage(state, &msg.team_id, Some(&record_sk), usage).await;
    note_review_standards(state, &msg.team_id, &record_sk, applied_standards).await;
    info!(pr = msg.pr_number, "Reviewer answered a question");
    Ok(())
}

/// A review didn't finish: log it (for the failed-review metric), then send
/// it again or, once every retry failed, leave a note on the PR. Never a
/// REQUEST_CHANGES, and never touches an earlier verdict.
async fn review_failed(
    state: &WorkerState,
    msg: &ReviewMessage,
    head: &str,
    reason: &str,
    job: &LiveJob,
) {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    tracing::error!(
        team_id = %msg.team_id,
        repo = %repo,
        pr = msg.pr_number,
        head = %head,
        attempt = msg.attempt,
        trigger = %msg.trigger,
        reason = %common::truncate_str(reason, 1000),
        "review_failed: the review didn't finish"
    );
    // The one verdict function: a review that didn't finish posts nothing
    // formal, whatever stands on the PR.
    let decision = super::review_verdict::decide(
        super::review_verdict::Input::Failed,
        &super::review_verdict::Context::default(),
    );
    debug_assert_eq!(decision, super::review_verdict::Decision::Nothing);
    let plan = failure_plan(msg.attempt);
    if let FailurePlan::Retry { delay_secs } = plan {
        let (next, hop) = retry_message(msg, head, delay_secs);
        if enqueue_review(state, &next, hop).await {
            super::review_sweep::note_failure(state, msg, head, reason, false).await;
            job.finished(
                state,
                msg,
                common::review_live::Finished::Retrying {
                    reason,
                    retry_in_secs: delay_secs as i64,
                    budget_secs: REVIEW_LEASE_SECS as i64,
                },
            )
            .await;
            info!(
                pr = msg.pr_number,
                attempt = next.attempt,
                delay_secs,
                "Review sent again after a failure"
            );
            return;
        }
    }
    tracing::error!(
        team_id = %msg.team_id,
        repo = %repo,
        pr = msg.pr_number,
        head = %head,
        attempts = msg.attempt + 1,
        "review_gave_up: every retry of the review failed"
    );
    super::review_sweep::note_failure(state, msg, head, reason, msg.question.is_none()).await;
    job.finished(state, msg, common::review_live::Finished::Failed { reason })
        .await;
    // Tell the PR once per head (or per question).
    let once = format!(
        "REVIEWFAILNOTE#{repo}#{:0>6}#{}",
        msg.pr_number,
        if msg.question.is_some() {
            msg.dedup_key.as_str()
        } else {
            head
        }
    );
    if !claim_once(state, &msg.team_id, &once).await {
        return;
    }
    let github = match GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        Ok(g) => g,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not tell the PR the review failed");
            return;
        }
    };
    let note = gave_up_note(msg, head);
    let posted = match (msg.question.is_some(), msg.reply_to_comment_id) {
        (true, Some(cid)) => github
            .reply_to_review_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, cid, &note)
            .await
            .map(|_| ()),
        _ => github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &note)
            .await
            .map(|_| ()),
    };
    if let Err(e) = posted {
        warn!(pr = msg.pr_number, error = %e, "Could not tell the PR the review failed");
    }
}

async fn run_once(
    state: &WorkerState,
    mut msg: ReviewMessage,
    reviewed_head: &mut String,
    live_job: &LiveJob,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    const DONE: Outcome = Outcome::Done {
        posted: None,
        run_review: None,
    };
    let job_started = chrono::Utc::now().to_rfc3339();
    let cfg = load_config(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await?;
    if !cfg.enabled || cfg.killed {
        info!(
            pr = msg.pr_number,
            "Reviewer disabled/killed for repo — skipping"
        );
        return Ok(DONE);
    }

    // Consume-once idempotency. GitHub webhooks AND SQS are both at-least-once, so
    // the same trigger (a `@coderhelm re-review` comment, a native re-request, a
    // label event) can be delivered more than once — each delivery would post
    // another full review + auto-merge-arm (observed: one re-review comment
    // produced two approvals + two arms). The gateway stamps a `dedup_key` that is
    // stable per user action (comment id / head sha); the first delivery claims it
    // and any duplicate is dropped here, before any GitHub/model work. Empty key
    // (old in-flight messages) or a transient DynamoDB error falls through to
    // processing — `claim_once_ttl` fails OPEN, so a real review is never lost. The
    // short TTL lets a failed review be re-driven later.
    if !msg.dedup_key.is_empty() {
        let sk = format!(
            "REVIEWJOB#{}/{}#{:06}#{}",
            msg.repo_owner, msg.repo_name, msg.pr_number, msg.dedup_key
        );
        // A lease while the review runs; extended to the dedup window when it
        // finishes, released when it fails (see `run`).
        if !claim_once_ttl(state, &msg.team_id, &sk, REVIEW_LEASE_SECS).await {
            info!(
                pr = msg.pr_number,
                dedup_key = %msg.dedup_key,
                "Skipping duplicate review job — this trigger was already processed"
            );
            return Ok(DONE);
        }
    }

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // PR metadata. head_sha may be empty on reply-triggered reviews — resolve it
    // from the PR so a reply always targets the latest commit.
    let pr = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;

    // Coalesce rapid commits: if a newer commit has landed since this verdict
    // review was enqueued, skip it — the newer commit's own review covers the
    // latest code. Without this, a burst (e.g. an auto-fix loop pushing several
    // commits in a minute) makes the reviewer post a review per intermediate
    // commit ("asking too fast"). Reply/question reviews resolve the head fresh
    // (msg.head_sha empty) and are never skipped here.
    let current_head = pr["head"]["sha"].as_str().unwrap_or("");
    if superseded(&msg, current_head) {
        info!(
            pr = msg.pr_number,
            enqueued = %msg.head_sha,
            current = current_head,
            "Skipping superseded review — a newer commit exists"
        );
        return Ok(DONE);
    }

    let title = pr["title"].as_str().unwrap_or("");
    let pr_body = pr["body"].as_str().unwrap_or("");
    let base = pr["base"]["sha"].as_str().unwrap_or("");
    let base_branch = pr["base"]["ref"].as_str().unwrap_or("main");
    let pr_author = pr["user"]["login"].as_str().unwrap_or("");
    let head_sha = if msg.head_sha.is_empty() {
        pr["head"]["sha"].as_str().unwrap_or("").to_string()
    } else {
        msg.head_sha.clone()
    };
    *reviewed_head = head_sha.clone();

    // A reply asking for a re-review and the author's push of the same commit
    // arrive together and used to give two reviews of one commit, sometimes
    // with opposite verdicts a minute apart. A reply review takes the same
    // per-commit claim push reviews use; if a push review claimed this commit
    // in the last few minutes, that review answers it.
    if msg.trigger == "reply"
        && msg.question.is_none()
        && !head_sha.is_empty()
        && !claim_recent_head(state, &msg, &head_sha).await
    {
        info!(pr = msg.pr_number, head = %head_sha, "Reply re-review skipped — this commit is already being reviewed");
        return Ok(DONE);
    }

    // Real work starts here: the dashboard shows the PR as being reviewed.
    live_job.reviewing(state, &msg, &head_sha, title).await;

    // Build the diff (base...head compare gives per-file patches), bounded.
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, base, &head_sha)
        .await?;
    let diff = review_agent::format_diff(&compare, 40_000);

    // Read AGENTS.md/etc. at the PR head so a PR that edits them is reviewed
    // against its own new rules.
    let repo_instructions =
        super::load_repo_instructions_at_ref(&github, &msg.repo_owner, &msg.repo_name, &head_sha)
            .await;
    let extra = if cfg.instructions.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Repo review focus (owner-provided)\n{}",
            cfg.instructions
        )
    };
    // Org-wide standards apply to every repo, layered under the per-repo focus.
    let org_instructions = load_org_instructions(state, &msg.team_id).await;
    let org_block = if org_instructions.trim().is_empty() {
        String::new()
    } else {
        format!("\n\n## Org-wide review standards (apply to every repo)\n{org_instructions}")
    };
    // The team's working rules for this repository are the standard every PR
    // in it is held to, a person's or CoderHelm's. Loaded once per review.
    let team_instructions =
        super::load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let org_block = format!(
        "{org_block}{}",
        review_agent::team_rules_block(&team_instructions)
    );

    // No model key set up is the team's setup, not a service error: there
    // is nothing to retry.
    let provider = match ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(team_id = %msg.team_id, pr = msg.pr_number, error = %e, "Review skipped: no usable model provider");
            return Ok(DONE);
        }
    };
    let mut usage = TokenUsage::default();

    let repo_slug = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let self_authored = pr_author.contains("coderhelm");
    let records =
        super::review_sweep::review_records(state, &msg.team_id, &repo_slug, msg.pr_number, 10)
            .await;
    let standing = super::review_sweep::standing_review(&records);

    // What the person's comment asks for, read by a model in context: an
    // ask for another look or for the approval gets a formal review of the
    // head, in this job; anything else gets a reply.
    if let Some(comment) = msg.question.clone().filter(|q| !q.trim().is_empty()) {
        let last_reply = match github
            .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            Ok(c) => own_comments(&c, 1).pop(),
            Err(_) => None,
        };
        let kind = classify_ask(
            state,
            &provider,
            &comment,
            standing.as_ref(),
            last_reply.as_deref(),
            &mut usage,
        )
        .await;
        let route = review_agent::route_ask(kind, self_authored);
        info!(pr = msg.pr_number, kind = ?kind, route = ?route, "Reviewer ask classified");
        match route {
            review_agent::AskRoute::FormalReview => {
                msg.question = None;
                msg.reply_to_comment_id = None;
            }
            review_agent::AskRoute::FixLoop => {
                // CoderHelm's own PR: the fix loop changes the code and answers
                // the person on the PR. With no run to hand it to, the reviewer
                // answers instead, so the person is never left unanswered.
                if forward_ask_to_fix_loop(state, &msg, &comment).await {
                    record_review_usage(state, &msg.team_id, None, &usage).await;
                    return Ok(DONE);
                }
            }
            review_agent::AskRoute::Reply => {}
        }
    }

    // ── Reply-with-a-question mode: answer, don't vote. ──
    if let Some(question) = msg.question.as_ref().filter(|q| !q.trim().is_empty()) {
        let system = reply_system_prompt(
            &msg.repo_owner,
            &msg.repo_name,
            &super::format_instructions_block(&repo_instructions),
            &org_block,
            &extra,
        );
        // What CoderHelm already said on this PR, so the answer builds on it
        // instead of drip-feeding new items each round.
        let earlier_replies = match github
            .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            Ok(c) => own_comments(&c, 3),
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not list earlier replies");
                Vec::new()
            }
        };
        let earlier_section = if earlier_replies.is_empty() {
            String::new()
        } else {
            let mut s = String::from("\n\n## Your earlier replies on this PR (oldest first)\n");
            for r in &earlier_replies {
                s.push_str(&format!(
                    "- {}\n",
                    common::truncate_str(r.trim(), 1500).replace('\n', " ")
                ));
            }
            s
        };
        let prompt = format!(
            "PR #{}: {title}\n\n{pr_body}{earlier_section}\n\n## Question from a reviewer\n{question}\n\n## Diff (base...head)\n{diff}",
            msg.pr_number,
        );
        // The same read-only tools a review has, so an answer rests on the code
        // instead of on what the diff happens to show.
        let live = super::live_sources::LiveSources::load(
            state,
            super::live_sources::Role::Reviewer,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            msg.pr_number,
        )
        .await;
        let system = match &live {
            Some(l) => format!("{system}{}", l.prompt_section()),
            None => system,
        };
        let history = super::github_history::GithubHistory::new(
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            super::plan::fetch_team_repos(state, &msg.team_id).await,
            "reviewer",
            msg.pr_number,
        );
        let system = format!("{system}{}", super::github_history::prompt_line());
        let ops = super::ops_tools::OpsTools::load(
            state,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            "reviewer",
            msg.pr_number,
        )
        .await;
        let system = match &ops {
            Some(o) => format!("{system}{}", o.prompt_section()),
            None => system,
        };
        let skills = super::skills::TeamSkills::load(
            state,
            &msg.team_id,
            super::skills::Reader::Reviewer,
            format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
            super::skills::Work::new(
                format!("{}/{}", msg.repo_owner, msg.repo_name),
                // An answer to a question is a reply about the code.
                &["reviewer", "replies"],
                changed_files(&compare).into_iter().collect(),
            ),
        )
        .await
        .map(|s| {
            s.with_task(&format!(
                "Answering a question on the pull request: {question}"
            ))
        });
        let system = match &skills {
            Some(s) => {
                s.record_applied(state).await;
                format!("{system}{}", s.prompt_section())
            }
            None => system,
        };
        let web = super::web_check::WebCheck::load(
            state,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            "reviewer",
            msg.pr_number,
            &format!("{pr_body}\n{question}"),
        )
        .await;
        let system = format!("{system}{}", web.prompt_section());
        let graphs = super::code_graph::RepoGraphs::load(
            state,
            &msg.team_id,
            msg.installation_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            Vec::new(),
            "reviewer",
            msg.pr_number,
        )
        .await;
        let graph_tools = graphs.tools();
        let system = if graph_tools.is_empty() {
            system
        } else {
            format!(
                "{system}{}",
                super::code_graph::prompt_line(super::code_graph::Role::Reviewer)
            )
        };
        let executor = review_agent::PrReviewToolExecutor {
            github: &github,
            owner: &msg.repo_owner,
            repo: &msg.repo_name,
            head_sha: &head_sha,
            state,
            graph: Some(&graphs),
            history: Some(&history),
            ops: ops.as_ref(),
            live: live.as_ref(),
            skills: skills.as_ref(),
            web: Some(&web),
        };
        let mut tools = review_agent::review_tools();
        tools.push(super::github_history::tool());
        tools.push(super::web_check::tool());
        tools.extend(graph_tools);
        if let Some(o) = &ops {
            tools.extend(o.tools());
        }
        if let Some(l) = &live {
            tools.extend(l.tools());
        }
        if let Some(s) = &skills {
            tools.extend(s.tools());
            s.offer(&tools);
        }
        let mut messages = vec![(
            "user".to_string(),
            vec![serde_json::json!({"type": "text", "text": prompt})],
        )];
        let answer = provider::converse(
            state,
            &provider,
            provider.heavy_model_id(),
            &system,
            &mut messages,
            &tools,
            &executor,
            &mut usage,
            crate::agent::llm::ConverseOptions {
                max_turns: 12,
                max_tokens: 2048,
                deadline: None,
                effort: None,
            },
            None,
            None,
        )
        .await;
        if let Some(s) = &skills {
            s.drain_usage_into(&mut usage);
        }
        // A failed answer is retried (see `run`), never posted as an error.
        let mut answer = match answer {
            Ok(a) => a,
            Err(e) => {
                record_review_usage(state, &msg.team_id, None, &usage).await;
                return Err(format!("the model call failed: {e}").into());
            }
        };
        // What the answer concludes, judged by the model: whether CoderHelm's
        // formal verdict must follow it, and whether it drip-feeds new
        // non-blocking items after an earlier reply said the code was fine.
        let conclusion =
            reply_conclusion(&provider, &earlier_replies, question, &answer, &mut usage).await;
        if let Some(c) = conclusion.as_ref().filter(|c| c.new_nonblocking_items) {
            info!(
                pr = msg.pr_number,
                "Reply dropped new non-blocking items raised after the code was called fine"
            );
            answer = c.revised_reply.clone();
        }
        let consulted = skills
            .as_ref()
            .map(|s| s.consulted_markdown(super::skills::app_url(&state.config.stage)))
            .unwrap_or_default();
        let applied = skills
            .as_ref()
            .map(|s| s.applied_names())
            .unwrap_or_default();
        // The one verdict function decides whether the formal review must
        // follow this reply; if so it runs first, in this job.
        let decision = conclusion.as_ref().map(|c| {
            super::review_verdict::decide(
                super::review_verdict::Input::Reply {
                    change: c.verdict_change,
                },
                &super::review_verdict::Context {
                    standing: standing.clone(),
                    self_authored,
                },
            )
        });
        if decision == Some(super::review_verdict::Decision::Review) {
            let c = conclusion.as_ref().expect("checked above");
            info!(
                pr = msg.pr_number,
                head = %head_sha,
                change = ?c.verdict_change,
                standing = ?standing,
                "Reply conclusion differs from the standing verdict; formal review runs first"
            );
            record_review_usage(state, &msg.team_id, None, &usage).await;
            return Ok(Outcome::ReviewThenReply {
                review: Box::new(reconcile_message(&msg, &head_sha, &c.reason)),
                reply: Box::new(PendingReply {
                    head_sha: head_sha.clone(),
                    answer,
                    consulted,
                    applied_standards: applied,
                }),
            });
        }
        send_reply(
            state, &github, &msg, &head_sha, &answer, &consulted, &applied, &usage,
        )
        .await?;
        return Ok(DONE);
    }

    // ── Verdict mode (agentic: walk the repo, structured findings, self-critique) ──

    // A push after CoderHelm approved an earlier commit is reviewed from that
    // commit: only what changed since. One that changed no code or data is
    // not reviewed at all and the approval stands. A push after a request
    // for changes (or questions) gets a full review.
    let mut review_diff = diff.clone();
    // The PR's own change is the one CoderHelm approved: every commit since
    // brought in only the base branch (merges of work already merged there).
    let mut own_change_unchanged = false;
    if let Some(approved) = super::review_scope::approved_base(
        &msg.trigger,
        msg.question.is_some(),
        &records,
        &head_sha,
    ) {
        match github
            .get_diff(&msg.repo_owner, &msg.repo_name, &approved, &head_sha)
            .await
        {
            Ok(since) if super::review_scope::builds_on(&since) => {
                let short = &approved[..approved.len().min(7)];
                let unconfirmed = review_agent::unconfirmed_external_facts(pr_body, &[]);
                // Merges of the base branch change code, but not the PR's:
                // compare the PR's own change now with the one approved.
                own_change_unchanged = match github
                    .get_diff(&msg.repo_owner, &msg.repo_name, base, &approved)
                    .await
                {
                    Ok(at_approval) => super::review_scope::same_own_change(&at_approval, &compare),
                    Err(e) => {
                        warn!(pr = msg.pr_number, error = %e, "Could not read the PR's change at the approval — reviewing what changed since");
                        false
                    }
                };
                let nothing_new =
                    !super::review_scope::touches_code(&since) || own_change_unchanged;
                if nothing_new && unconfirmed.is_empty() {
                    info!(pr = msg.pr_number, approved = %approved, head = %head_sha, own_change_unchanged, "Push changed none of the PR's code or data since the approval — review skipped, approval carried");
                    // Many repos dismiss approvals when commits are pushed:
                    // post the approval again on this head, so the PR never
                    // sits without a visible verdict.
                    carry_approval(
                        state,
                        &github,
                        &msg,
                        &head_sha,
                        &approved,
                        own_change_unchanged,
                        standing.clone(),
                        self_authored,
                    )
                    .await;
                    // The auto-merge gate is bound to a head: carry the
                    // approval to this one, as a review of it would.
                    let _ = super::review_actions::run_on_approve(
                        state,
                        &github,
                        &msg.team_id,
                        msg.installation_id,
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.pr_number,
                        &head_sha,
                        base_branch,
                        self_authored,
                    )
                    .await;
                    let reason = if own_change_unchanged {
                        format!(
                            "Not reviewed: the commits since CoderHelm approved {short} only \
                             bring in the base branch, and the PR's own change is the one \
                             approved, so that approval stands."
                        )
                    } else {
                        format!(
                            "Not reviewed: nothing but docs, comments or whitespace changed \
                             since CoderHelm approved {short}, so that approval stands."
                        )
                    };
                    return Ok(Outcome::Skipped { reason });
                }
                if own_change_unchanged {
                    // Reviewed again only to triage the facts the description
                    // marks unconfirmed: the change itself is the approved one.
                    info!(pr = msg.pr_number, approved = %approved, head = %head_sha, "Only the base branch came in since the approval — re-checking the PR's unconfirmed facts");
                    review_diff = format!(
                        "(CoderHelm approved {short} earlier. Every commit since only merges the \
                         base branch: the PR's own change below is the one approved. Review \
                         nothing new in it; triage the facts the description marks \
                         unconfirmed.)\n{diff}"
                    );
                } else {
                    info!(pr = msg.pr_number, approved = %approved, head = %head_sha, files = ?super::review_scope::code_files(&since), "Reviewing only what changed since the approval");
                    review_diff = format!(
                        "(CoderHelm approved {short} earlier. This review covers only what \
                         changed since then; the rest of the PR was reviewed and approved.)\n{}",
                        review_agent::format_diff(&since, 40_000)
                    );
                }
            }
            Ok(_) => {
                info!(pr = msg.pr_number, approved = %approved, "Approved commit isn't an ancestor of the head (rebased) — full review");
            }
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not read the diff since the approval — full review");
            }
        }
    }
    // Fold this team's past 👎 feedback into the prompt so the reviewer learns and
    // stops repeating rejected comment styles ("leave comments to learn").
    // People's pushback on this PR still waiting out its quiet period is
    // judged now, so this review already has what was learned from it.
    super::review_lessons::settle_before_review(state, &provider, &github, &msg, &mut usage).await;
    // Lessons from the team's own corrections of earlier reviews (repository
    // and team-wide); given to the reviewer and to its critic.
    let team_lessons =
        super::review_lessons::recall(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let learning =
        load_learning_context(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let instructions_block = format!(
        "{}{org_block}{extra}{learning}{team_lessons}",
        super::format_instructions_block(&repo_instructions)
    );
    let changed = review_agent::changed_right_lines(&compare);

    // Code graph: if this repo is indexed, compute the EXACT impacted-file set
    // for the PR's changed files and inject it into the prompt (agents under-use
    // structural tools unless the result is put in front of them), and hand the
    // agent the graph lookup tools for deeper digging.
    let graphs = super::code_graph::RepoGraphs::load(
        state,
        &msg.team_id,
        msg.installation_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        Vec::new(),
        "reviewer",
        msg.pr_number,
    )
    .await;
    let graph_context = if let Some(g) = graphs.ready() {
        let changed_paths: Vec<String> = compare["files"]
            .as_array()
            .map(|fs| {
                fs.iter()
                    .filter_map(|f| f["filename"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let impacted = g.impacted_by(state, &changed_paths).await;
        if impacted.is_empty() {
            String::new()
        } else {
            let lines: Vec<String> = impacted
                .iter()
                .take(40)
                .map(|(f, why)| format!("- {f} — {why}"))
                .collect();
            format!(
                "\n## Impacted files (from the code graph — files that import or reference \
                 symbols defined in the changed files; check these for breakage)\n{}\n",
                lines.join("\n")
            )
        }
    } else {
        String::new()
    };

    // CI evidence: the PR's ACTUAL check results for this head, injected into
    // the prompt. A reviewer claim like "this cannot install" is refutable by a
    // green install job on the same head — that signal must be in front of the
    // model, not discoverable-in-theory. Best-effort: absent on API failure.
    let ci_context = match github
        .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &head_sha)
        .await
    {
        Ok(v) => {
            let runs = v["check_runs"].as_array().cloned().unwrap_or_default();
            if runs.is_empty() {
                String::new()
            } else {
                let lines: Vec<String> = runs
                    .iter()
                    .take(30)
                    .map(|r| {
                        format!(
                            "- {}: {}",
                            r["name"].as_str().unwrap_or("?"),
                            r["conclusion"]
                                .as_str()
                                .unwrap_or_else(|| r["status"].as_str().unwrap_or("?"))
                        )
                    })
                    .collect();
                format!(
                    "\n## CI results on this PR's head ({})\n{}\n",
                    &head_sha[..head_sha.len().min(7)],
                    lines.join("\n")
                )
            }
        }
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not fetch check runs for review context");
            String::new()
        }
    };
    // For CoderHelm's own PR: what its planner looked up and found about
    // where the fix belongs, so a diff that overrides source data can be
    // judged against it.
    let plan_context = plan_evidence_for_pr(state, &msg).await;
    let extra_context = format!("{graph_context}{ci_context}{plan_context}");

    // CoderHelm's own threads from earlier reviews, with the replies under them.
    // Still-open ones: the re-review reports each as fixed or answered (→ resolved
    // below) or open (not re-posted). Ones a person resolved are settled and are
    // shown so the same concern isn't raised again.
    let bot_threads = match github
        .list_bot_review_threads(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not list earlier review threads");
            Vec::new()
        }
    };
    let prior_threads: Vec<&crate::clients::github::BotReviewThread> =
        bot_threads.iter().filter(|t| !t.resolved).collect();
    let settled_threads: Vec<&crate::clients::github::BotReviewThread> = bot_threads
        .iter()
        .filter(|t| t.resolved_by_person)
        .collect();
    // What people wrote in the PR conversation, including the text of a
    // re-review ask.
    let discussion = match github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(c) => human_comments(&c, review_agent::MAX_DISCUSSION_COMMENTS),
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not list PR conversation comments");
            Vec::new()
        }
    };
    let answered_context = format!(
        "{}{}",
        review_agent::format_settled_threads(&settled_threads),
        review_agent::format_discussion(&discussion)
    );
    let prior_context = format!(
        "{}{answered_context}",
        review_agent::format_prior_threads(&prior_threads)
    );

    // Repo-defined CI labels the reviewer may pick (opt-in). Candidates are the
    // repo's live labels filtered by the allow-list, so the set follows whatever
    // labels the team has today.
    let current_labels: Vec<String> = pr["labels"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| l["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let repo_labels: Vec<super::review_labels::RepoLabel> = if cfg.auto_labels {
        match github
            .list_repo_labels(&msg.repo_owner, &msg.repo_name)
            .await
        {
            Ok(ls) => ls
                .into_iter()
                .map(|(name, description)| super::review_labels::RepoLabel { name, description })
                .collect(),
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not list repo labels — skipping label picks");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    // Allowed labels: the settings' list, or (when empty) the labels the repo's
    // own docs name — read in full here, since the prompt copy is size-capped.
    let label_rules = if repo_labels.is_empty() || !cfg.label_rules.allow.is_empty() {
        cfg.label_rules.clone()
    } else {
        let docs =
            super::load_repo_docs_full(&github, &msg.repo_owner, &msg.repo_name, &head_sha).await;
        super::review_labels::effective_rules(&cfg.label_rules, &repo_labels, &docs)
    };
    let label_candidates = super::review_labels::candidates(&repo_labels, &label_rules);
    let label_context =
        super::review_labels::prompt_section(&label_candidates, &current_labels, &label_rules);
    let tests_context = if cfg.require_tests {
        review_agent::REQUIRE_TESTS_SECTION
    } else {
        ""
    };
    let reconcile_context = msg
        .reconcile_note
        .as_deref()
        .filter(|n| !n.trim().is_empty())
        .map(|n| {
            format!(
                "\n## Your own reply on this PR concluded\n{}\nThe formal review on the PR \
                 doesn't match it yet. Check it against the code at this head and give the \
                 verdict the code supports.\n",
                common::truncate_str(n.trim(), 1500)
            )
        })
        .unwrap_or_default();
    let prior_context = format!("{prior_context}{label_context}{tests_context}{reconcile_context}");

    // Repository history through the GitHub App: no setup.
    let history = super::github_history::GithubHistory::new(
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        super::plan::fetch_team_repos(state, &msg.team_id).await,
        "reviewer",
        msg.pr_number,
    );
    let ops = super::ops_tools::OpsTools::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "reviewer",
        msg.pr_number,
    )
    .await;
    // Live systems the team connected for this repo, if any.
    let live = super::live_sources::LiveSources::load(
        state,
        super::live_sources::Role::Reviewer,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        msg.pr_number,
    )
    .await;
    let skills = super::skills::TeamSkills::load(
        state,
        &msg.team_id,
        super::skills::Reader::Reviewer,
        format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
        super::skills::Work::new(
            format!("{}/{}", msg.repo_owner, msg.repo_name),
            // A review judges the code and writes the comments.
            &["reviewer", "review_comments"],
            changed_files(&compare).into_iter().collect(),
        ),
    )
    .await
    .map(|s| s.with_task(&format!("Reviewing: {title}")));
    if let Some(s) = &skills {
        s.record_applied(state).await;
    }

    // Checking the team's own public pages (a redirect target, a docs link):
    // hosts in the diff's URLs, the team's listed domains and hosts its
    // repositories use.
    let web = super::web_check::WebCheck::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "reviewer",
        msg.pr_number,
        &format!("{pr_body}\n{diff}"),
    )
    .await;

    // 0) The team's code-review agents read the diff first (at most two, in
    // parallel, each with a time limit). Their notes reach the reviewer as
    // advice it checks; a note blocks nothing unless the reviewer agrees.
    let agent_notes = match &skills {
        Some(s) if !s.reviewers_for("code").is_empty() => {
            let host = super::agent_tools::Host {
                reads: Some(super::agent_tools::RepoReads {
                    github: &github,
                    owner: &msg.repo_owner,
                    repo: &msg.repo_name,
                    git_ref: &head_sha,
                    http: &state.http,
                }),
                parent: None,
                offered: &[],
            };
            let draft = format!("## Pull request: {title}\n\n{review_diff}");
            let reviews = s
                .review(state, "code", "pull request's code change", &draft, &host)
                .await;
            info!(
                pr = msg.pr_number,
                reviewers = reviews.len(),
                with_gaps = reviews.iter().filter(|r| r.gaps.is_some()).count(),
                cost_usd = super::skills::reviews_cost(&reviews),
                "Code reviewed by team agents"
            );
            super::skills::reviewer_advice(&reviews)
        }
        _ => String::new(),
    };
    let extra_context = format!("{extra_context}{agent_notes}");

    // 1) High-recall generation with repo-walking tools.
    let output = review_agent::generate_review(
        state,
        &provider,
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &head_sha,
        title,
        pr_body,
        &review_diff,
        &instructions_block,
        Some(&graphs),
        &extra_context,
        &prior_context,
        Some(&history),
        ops.as_ref(),
        live.as_ref(),
        skills.as_ref(),
        Some(&web),
        &mut usage,
    )
    .await;
    // A review that didn't finish has no verdict: the job is retried (see
    // `run`), and nothing is posted for it. Its tokens still count.
    let output = match output {
        Ok(o) => o,
        Err(reason) => {
            record_review_usage(state, &msg.team_id, None, &usage).await;
            return Err(reason.into());
        }
    };
    let fixed_threads: Vec<&crate::clients::github::BotReviewThread> =
        settled_prior_threads(&prior_threads, &output.prior);
    let still_open = prior_threads.len() - fixed_threads.len();
    // Of those, the ones that hold the merge: a question the change doesn't
    // rest on, or an unverified concern, left open is still shown, but
    // doesn't request changes.
    let still_open_holding = prior_threads
        .iter()
        .filter(|t| !fixed_threads.iter().any(|f| f.id == t.id))
        .filter(|t| review_agent::thread_holds_merge(&t.body))
        .count();
    // Label picking is live when there's something to pick: existing allowed
    // labels, or an allowed family whose labels may not exist yet.
    let labels_live = !label_candidates.is_empty() || !label_rules.families().is_empty();
    let label_decision = if !labels_live {
        super::review_labels::LabelDecision::default()
    } else {
        super::review_labels::decide(&output.labels, &repo_labels, &current_labels, &label_rules)
    };
    for (name, why) in &label_decision.refused {
        info!(pr = msg.pr_number, label = %name, why = %why, "Reviewer label refused");
    }
    // Every label decision is logged — including "added nothing" — with the
    // reviewer's own reason, so "why didn't it add labels" is answerable from logs.
    if cfg.auto_labels {
        let picked: Vec<&str> = output.labels.iter().map(|l| l.name.as_str()).collect();
        let to_add: Vec<&str> = label_decision.add.iter().map(|(n, _)| n.as_str()).collect();
        info!(
            pr = msg.pr_number,
            candidates = label_candidates.len(),
            on_pr = ?current_labels,
            picked = ?picked,
            to_add = ?to_add,
            tests_note = %common::truncate_str(output.tests_note.trim(), 600),
            "Reviewer label decision"
        );
    }

    // 2) Critic pass drops weak/false findings.
    let live_evidence =
        live.as_ref().map(|l| l.evidence()).unwrap_or_default() + &web.evidence() + &plan_context;
    let mut findings = review_agent::critique_findings(
        state,
        &provider,
        &review_diff,
        output.findings,
        review_agent::CriticContext {
            require_tests: cfg.require_tests,
            answered: &answered_context,
            live_evidence: &live_evidence,
            team_instructions: &team_instructions,
            lessons: &team_lessons,
        },
        &mut usage,
    )
    .await;

    // 2b) A suggestion is a one-click commit of its text over the anchored
    // lines; keep it only when that is a safe in-place edit of the file at this
    // head.
    let mut head_files = std::collections::HashMap::new();
    for f in findings.iter().filter(|f| f.suggestion.is_some()) {
        if head_files.contains_key(&f.file) {
            continue;
        }
        if let Ok(content) = github
            .read_file(&msg.repo_owner, &msg.repo_name, &f.file, &head_sha)
            .await
        {
            head_files.insert(f.file.clone(), content);
        }
    }
    let dropped = review_agent::vet_suggestions(&mut findings, &head_files);
    if dropped > 0 {
        info!(
            pr = msg.pr_number,
            dropped, "Reviewer dropped unsafe suggestions"
        );
    }

    // 2c) A blocking finding that rests only on a live lookup that failed or
    // came back empty rests on nothing: it is kept as a note saying so.
    let lookups: Vec<super::proof::Lookup> = live
        .as_ref()
        .map(|l| l.lookups())
        .unwrap_or_default()
        .into_iter()
        .chain(ops.as_ref().map(|o| o.lookups()).unwrap_or_default())
        .collect();
    let on_failed_lookups = review_agent::enforce_failed_lookups(&mut findings, &lookups);
    if on_failed_lookups > 0 {
        info!(
            pr = msg.pr_number,
            on_failed_lookups, "Blocking findings resting on failed lookups made non-blocking"
        );
    }

    // 3) Map to inline comments (only diff-anchored lines) + summary bullets.
    // With "Require tests" on, a missing/stale-tests finding blocks the merge
    // whatever severity the model gave it (it sometimes rated them medium/low
    // and the PR was approved without the tests).
    if cfg.require_tests {
        review_agent::enforce_required_tests(&mut findings);
    }
    review_agent::enforce_source_data_override(&mut findings);
    // A request to test something by hand is a note, never a block.
    review_agent::enforce_manual_checks_are_notes(&mut findings);
    // Verdict hysteresis. Once CoderHelm approved an earlier commit, a new
    // finding only holds the merge when it is in a file changed since that
    // approval; one about code that hasn't changed is still worth saying, but
    // as a note, not a flip back to "changes requested". Without this, 225
    // approve-then-request-changes flips landed on 123 PRs in two months.
    let mut held_since_approval: Vec<review_agent::Finding> = Vec::new();
    if let Some(prev) = last_review(state, &msg)
        .await
        .filter(|p| p.verdict == "APPROVE" && !p.head_sha.is_empty() && p.head_sha != head_sha)
    {
        if let Ok(since) = github
            .get_diff(&msg.repo_owner, &msg.repo_name, &prev.head_sha, &head_sha)
            .await
        {
            // When the PR's own change is the one approved (only the base
            // branch came in), nothing of the PR's changed: every finding is
            // about approved code.
            let touched = if own_change_unchanged {
                std::collections::HashSet::new()
            } else {
                changed_files(&since)
            };
            let (keep, held) = hold_unchanged(std::mem::take(&mut findings), &touched);
            findings = keep;
            if !held.is_empty() {
                info!(
                    pr = msg.pr_number,
                    held = held.len(),
                    "Findings on code unchanged since approval kept as notes"
                );
            }
            held_since_approval = held;
        }
    }
    let hunks = review_agent::right_line_hunks(&compare);
    let postable = review_agent::to_postable(&findings, &changed, &hunks);

    // 4) Optional sandbox verification ("receipts"): actually run the affected
    // tests/build. A hard failure forces REQUEST_CHANGES.
    let mut verify_md = String::new();
    let mut verify_failed = false;
    if cfg.verify_tests {
        let changed_files: Vec<String> = compare["files"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if let Some((passed, md)) = review_agent::verify_in_sandbox(
            state,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &head_sha,
            &changed_files,
        )
        .await
        {
            verify_md = md;
            verify_failed = !passed;
        }
    }

    // Verdict: always APPROVE or REQUEST_CHANGES. Changes are requested for
    // what holds the merge: a verified defect (or a policy like required
    // tests), a question or an unconfirmed fact the change's correctness
    // rests on, an earlier requested change still open, or a failed
    // verification. Everything else (notes, concerns it couldn't verify,
    // environment notes such as a staging system without data) is posted
    // with the approval.
    let unconfirmed = review_agent::unconfirmed_external_facts(pr_body, &output.unconfirmed_facts);
    let verdict: &'static str =
        review_agent::decide_verdict(&findings, still_open_holding, &unconfirmed, verify_failed);
    let blocking_facts = unconfirmed.iter().filter(|f| f.blocks).count();
    if !unconfirmed.is_empty() {
        info!(
            pr = msg.pr_number,
            unconfirmed = unconfirmed.len(),
            blocking = blocking_facts,
            "Unconfirmed facts in the PR's evidence triaged"
        );
    }
    // Computed, explainable risk (blast-radius-weighted) — overrides the model's
    // guess and drives the displayed level.
    let risk_report = review_risk::assess(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &head_sha,
        &compare,
    )
    .await;
    let risk = risk_report.level.to_string();

    // GitHub forbids APPROVE / REQUEST_CHANGES on your OWN PR → downgrade to COMMENT.
    let self_authored = pr_author.contains("coderhelm");
    let effective_event = if self_authored { "COMMENT" } else { verdict };
    let verdict_line = review_agent::verdict_heading(
        verdict,
        &review_agent::HeadingCounts {
            defects: findings
                .iter()
                .filter(|f| f.evidence() == review_agent::Evidence::VerifiedDefect)
                .count(),
            blocking_questions: findings
                .iter()
                .filter(|f| f.evidence() == review_agent::Evidence::Question && f.holds_merge())
                .count(),
            blocking_facts,
            prior_open: still_open_holding,
            verify_failed,
            notes: findings.iter().filter(|f| !f.holds_merge()).count()
                + (unconfirmed.len() - blocking_facts)
                + held_since_approval.len(),
        },
    );
    let summary = if output.summary.trim().is_empty() {
        "Automated review complete.".to_string()
    } else {
        short_overview(&output.summary)
    };
    let mut full_body = format!("{verdict_line}\n\n{summary}\n\n{}", risk_report.markdown());
    full_body.push_str(&review_agent::unconfirmed_markdown(&unconfirmed));
    if !verify_md.is_empty() {
        full_body.push_str(&format!("\n\n{verify_md}"));
    }
    if !postable.unanchored_md.is_empty() {
        full_body.push_str(&format!(
            "\n\n#### Additional findings\n{}",
            postable.unanchored_md
        ));
    }
    if !held_since_approval.is_empty() {
        full_body.push_str("\n\n#### Also worth a look\nOn code that hasn't changed since CoderHelm approved it, so these don't change the verdict:\n");
        for f in &held_since_approval {
            full_body.push_str(&format!("- `{}:{}` {}\n", f.file, f.line, f.title));
        }
    }
    full_body.push_str(&super::review_labels::markdown(
        labels_live,
        &label_decision,
        &output.tests_note,
    ));
    full_body.push_str(
        &super::e2e::triage_markdown(
            state,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            msg.pr_number,
            &head_sha,
            &compare["files"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|f| f["filename"].as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        )
        .await,
    );
    if !prior_threads.is_empty() {
        full_body.push_str(&format!(
            "\n\n#### Earlier comments\n{} settled and resolved · {} still open",
            fixed_threads.len(),
            still_open
        ));
    }
    if let Some(s) = &skills {
        full_body.push_str(&s.consulted_markdown(super::skills::app_url(&state.config.stage)));
    }
    full_body.push_str(RATING_FOOTER);

    // Check the head again before speaking: the review takes minutes, and a
    // commit pushed seconds before this one started may not have been the PR's
    // head yet when the check above ran. Posting now would put a verdict on code
    // that's already replaced — and if the newer commit's review finished first,
    // overturn it (APPROVE on the head, then REQUEST_CHANGES on
    // the commit before it).
    if let Ok(latest) = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        let latest_head = latest["head"]["sha"].as_str().unwrap_or("");
        if moved_on(&msg, &head_sha, latest_head) {
            info!(
                pr = msg.pr_number,
                reviewed = %msg.head_sha,
                current = latest_head,
                "Not posting superseded review — the PR moved on while reviewing"
            );
            // The tokens were still spent: count them against the team.
            record_review_usage(state, &msg.team_id, None, &usage).await;
            return Ok(DONE);
        }
    }

    // De-spam: only comment when there's news. If CoderHelm's last recorded verdict
    // for this PR equals this one (e.g. still APPROVE while the self-fix loop keeps
    // pushing commits, or a duplicate same-head event), record the new verdict but
    // stay SILENT — no repeated "Approved"/"armed" comments, which is the main
    // source of reviewer spam. An explicit reply / re-review (msg.question) always
    // speaks, and a flip to/from REQUEST_CHANGES always speaks (verdict changed).
    let prev = last_review(state, &msg).await;
    let prev_verdict = prev.as_ref().map(|p| p.verdict.clone());
    // An explicit human ask (a reply/question or the native "Re-request review"
    // button) always speaks, even if the verdict is unchanged — the person asked to
    // see it. Only webhook-driven re-reviews (a self-fix commit, a re-label) get
    // silenced when the verdict didn't change.
    let explicit = msg.question.is_some()
        || matches!(
            msg.trigger.as_str(),
            "reply" | "rerequest" | "sweep" | "reconcile"
        );
    // News = the verdict changed, OR there are new findings, OR earlier comments
    // got fixed. A same-verdict re-review that fixed one of three issues (and
    // found nothing new) used to be silently dropped here.
    let has_news = prev_verdict.as_deref() != Some(verdict)
        || !findings.is_empty()
        || !fixed_threads.is_empty()
        || !label_decision.add.is_empty();
    // A person pushed a commit: they hear that it was reviewed, even when the
    // verdict and the open findings are unchanged — otherwise they are left
    // asking for a re-review. Only CoderHelm's own commits and duplicate
    // reviews of the same commit stay quiet.
    let new_human_commit = prev.as_ref().is_some_and(|p| p.head_sha != head_sha)
        && !head_commit_by_bot(&compare, &head_sha);
    // A push and a re-review ask for the same commit run two reviews; the
    // first to finish speaks for both.
    let already_answered = prev
        .as_ref()
        .is_some_and(|p| p.head_sha == head_sha && p.created_at >= job_started);
    let decision = super::review_verdict::decide(
        super::review_verdict::Input::Review {
            verdict,
            has_news: has_news || new_human_commit,
            explicit,
            already_answered,
        },
        &super::review_verdict::Context {
            standing: standing.clone(),
            self_authored,
        },
    );
    let should_comment = matches!(decision, super::review_verdict::Decision::Post { .. });
    if already_answered {
        info!(pr = msg.pr_number, head = %head_sha, "Another review of this commit was just posted — not posting a duplicate");
    }
    let mut posted_event: Option<&'static str> = None;
    if let super::review_verdict::Decision::Post {
        event,
        dismiss_stale,
    } = decision
    {
        super::review_verdict::post(
            &github,
            &msg,
            &head_sha,
            event,
            dismiss_stale,
            &full_body,
            &postable.inline,
        )
        .await?;
        posted_event = Some(event);
        // Resolve the earlier threads this head fixed (after the review is up, so
        // the "N fixed" note is visible when they collapse).
        for t in &fixed_threads {
            if let Err(e) = github.resolve_review_thread(&t.id).await {
                warn!(pr = msg.pr_number, thread = %t.id, error = %e, "Failed to resolve fixed thread");
            }
        }
    }

    // Add the picked labels in ONE call (each label event re-runs the repo's CI).
    // Add-only: the issues API appends, it never replaces the PR's labels.
    if !label_decision.add.is_empty() {
        let names: Vec<String> = label_decision.add.iter().map(|(n, _)| n.clone()).collect();
        match github
            .add_labels(&msg.repo_owner, &msg.repo_name, msg.pr_number, &names)
            .await
        {
            Ok(_) => info!(pr = msg.pr_number, labels = ?names, "Reviewer added labels"),
            Err(e) => warn!(pr = msg.pr_number, error = %e, "Failed to add reviewer labels"),
        }
    }

    // Persist: store the summary + a compact findings digest for the dashboard.
    let record_body = {
        let mut b = summary.clone();
        for f in &findings {
            b.push_str(&format!(
                "\n\n- [{}] {}:{} — {}\n  {}",
                f.severity, f.file, f.line, f.title, f.body
            ));
        }
        b
    };
    let record_sk = store_review_record(
        state,
        &msg,
        &head_sha,
        verdict,
        &risk,
        &record_body,
        effective_event,
        "",
        &common::review_stats::ReviewCounts {
            findings: findings.len(),
            prior_open: prior_threads.len(),
            prior_fixed: fixed_threads.len(),
            prior_settled_by_person: settled_threads.len(),
            posted: should_comment,
            priors_known: true,
        },
    )
    .await;
    record_review_usage(state, &msg.team_id, Some(&record_sk), &usage).await;
    if let Some(s) = &skills {
        note_review_standards(state, &msg.team_id, &record_sk, &s.applied_names()).await;
    }
    note_review_details(state, &msg.team_id, &record_sk, title, &findings).await;
    info!(
        pr = msg.pr_number,
        verdict = verdict,
        risk = %risk,
        findings = findings.len(),
        inline = postable.inline.len(),
        prior_open = prior_threads.len(),
        prior_fixed = fixed_threads.len(),
        prior_settled_by_person = settled_threads.len(),
        discussion = discussion.len(),
        model_verdict = %output.verdict,
        posted_as = effective_event,
        posted = should_comment,
        "{}",
        if should_comment {
            "Reviewer posted verdict"
        } else {
            "Reviewer verdict unchanged — not posted (nothing new)"
        }
    );

    // ── Post-approval actions (opt-in, off by default) ──
    if verdict == "APPROVE" {
        // Self-authored (CoderHelm's own) PRs CAN auto-merge, but run_on_approve
        // forces the two-key human-approval gate on for them — the bot approving
        // its own code is never the second key. Human PRs use the repo's config.
        let report = super::review_actions::run_on_approve(
            state,
            &github,
            &msg.team_id,
            msg.installation_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &head_sha,
            base_branch,
            self_authored,
        )
        .await;
        // Arm regardless (the gate is head-bound, so a new commit must re-arm).
        // Nothing is posted here: the gate keeps ONE status comment on the PR,
        // edited in place, instead of an "armed" note per verdict.
        if should_comment && !report.summary.is_empty() {
            // Attach the action summary to the record we JUST wrote — an
            // UPDATE, not a second put. A second put (timestamp-keyed) showed
            // up as a duplicate "review" in the dashboard for every armed
            // APPROVE (observed on three PRs before being root-caused here).
            let upd = state
                .dynamo
                .update_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", attr_s(&msg.team_id))
                .key("sk", attr_s(&record_sk))
                .update_expression("SET action_summary = :a")
                .expression_attribute_values(
                    ":a",
                    attr_s(&common::head_tail_str(&report.summary, 8_000)),
                )
                .send()
                .await;
            if let Err(e) = upd {
                warn!(pr = msg.pr_number, error = %e, "Failed to attach action summary (non-fatal)");
            }
        }
    } else if self_authored && verdict == "REQUEST_CHANGES" && msg.trigger == TRIGGER_RUN {
        // The run that asked for this review acts on it itself.
        info!(
            pr = msg.pr_number,
            "Own PR's review requested changes — the run handles it"
        );
    } else if self_authored && verdict == "REQUEST_CHANGES" {
        // CoderHelm reviewed its OWN PR and wants changes → hand the findings to the
        // run's feedback loop so it applies the fixes. Two guards keep this review↔fix
        // loop from running away (it once flip-flopped a verdict into 67 commits / 44
        // reviews):
        //   (1) Hysteresis — if the PREVIOUS verdict was APPROVE, do NOT auto-fix this
        //       REQUEST_CHANGES. An approve→request flip on the bot's own code is
        //       almost always reviewer non-determinism, not a real regression; the
        //       findings stay visible for a human to judge.
        //   (2) Convergence cap — bound the self-review→fix rounds per PR. Past the
        //       cap, stop auto-fixing and hand off to a human (announced once).
        if prev_verdict.as_deref() == Some("APPROVE") {
            info!(
                pr = msg.pr_number,
                "Self-review flipped APPROVE→REQUEST_CHANGES — holding for a human, not re-fixing (hysteresis)"
            );
        } else if claim_self_review_round(
            state,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
        )
        .await
        {
            // Under the cap — apply the fixes. A new commit re-triggers the review.
            feed_review_back_to_run(state, &msg, &record_body).await;
        } else if claim_once(
            state,
            &msg.team_id,
            &format!(
                "SELFREVIEWHANDOFF#{}/{}#{:0>6}",
                msg.repo_owner, msg.repo_name, msg.pr_number
            ),
        )
        .await
        {
            // Cap reached — stop the auto-fix loop and hand to a human, once.
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &format!(
                        "🛑 I've self-fixed this PR {MAX_SELF_REVIEW_FIX_ROUNDS} times without the \
                         review converging, so I'm stopping the automatic fix loop and handing it \
                         to a human. My latest findings are above."
                    ),
                )
                .await;
            info!(
                pr = msg.pr_number,
                "Self-review→fix cap reached — handed off to a human"
            );
        }
    }

    let run_review = (msg.trigger == TRIGGER_RUN).then(|| RunReview {
        verdict,
        body: record_body.clone(),
        override_finding: findings
            .iter()
            .find(|f| {
                f.category
                    .eq_ignore_ascii_case(review_agent::SOURCE_DATA_OVERRIDE)
                    && f.severity == "blocking"
            })
            .map(|f| format!("{} {}", f.title.trim(), f.body.trim())),
    });
    Ok(Outcome::Done {
        posted: posted_event,
        run_review,
    })
}

/// The trigger of a review a ticket run asks for on its own PR before it
/// completes (see [`review_for_run`]).
pub(crate) const TRIGGER_RUN: &str = "run";

/// The body of an approval carried to a new head without a review: the
/// verdict and why in one line.
pub(crate) fn carried_approval_body(approved: &str, only_base_merged: bool) -> String {
    let short = &approved[..approved.len().min(7)];
    let why = if only_base_merged {
        format!(
            "the new commits only bring in the base branch; the PR's own change is the same as \
             the approved `{short}`."
        )
    } else {
        format!(
            "the new commits change only docs, comments or whitespace since the approved \
             `{short}`."
        )
    };
    format!("✅ **Approved**: {why}")
}

/// Posts the approval of `approved` again on `head` (a COMMENT on
/// CoderHelm's own PR, where GitHub refuses a verdict) and records it, so
/// the head carries a visible verdict and later pushes are reviewed from it.
/// Best-effort: a failure is logged and the auto-merge gate still runs.
#[allow(clippy::too_many_arguments)]
async fn carry_approval(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReviewMessage,
    head: &str,
    approved: &str,
    only_base_merged: bool,
    standing: Option<super::review_verdict::StandingReview>,
    self_authored: bool,
) {
    let super::review_verdict::Decision::Post {
        event,
        dismiss_stale,
    } = super::review_verdict::decide(
        super::review_verdict::Input::Carried,
        &super::review_verdict::Context {
            standing,
            self_authored,
        },
    )
    else {
        return;
    };
    let heading = carried_approval_body(approved, only_base_merged);
    let body = format!("{heading}{RATING_FOOTER}");
    if let Err(e) =
        super::review_verdict::post(github, msg, head, event, dismiss_stale, &body, &[]).await
    {
        warn!(pr = msg.pr_number, head = %head, error = %e, "Could not post the carried approval");
        return;
    }
    store_review_record(
        state,
        msg,
        head,
        "APPROVE",
        "N/A",
        &heading,
        event,
        "",
        &common::review_stats::ReviewCounts {
            posted: true,
            ..Default::default()
        },
    )
    .await;
    info!(pr = msg.pr_number, head = %head, approved = %approved, event, "Approval carried to the new head");
}

/// The PR reviewer's verdict on a run's own PR, for the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReview {
    pub verdict: &'static str,
    /// The review's summary and findings.
    pub body: String,
    /// A kept source-data-override finding (title and body): the diff
    /// overrides data another system owns, so the fix belongs there.
    pub override_finding: Option<String>,
}

impl RunReview {
    /// The verdict holds the merge.
    pub fn blocks(&self) -> bool {
        self.verdict == "REQUEST_CHANGES"
    }
}

/// One review authority. A ticket run whose own checks passed asks the PR
/// reviewer for its verdict on the head before it completes, so a run never
/// ends as "review passed" while the reviewer requests changes. The review is
/// the same one the PR would get when marked ready (same per-head job key,
/// so that later trigger doesn't review it twice), posted as usual.
///
/// `None` when the repo's reviewer is off, or the review didn't finish within
/// `budget` (its job claim is given up, so the ready-for-review trigger
/// reviews it as before).
#[allow(clippy::too_many_arguments)]
pub async fn review_for_run(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    budget: std::time::Duration,
) -> Option<RunReview> {
    let label = enabled_trigger_label(state, team_id, owner, repo).await?;
    if head_sha.is_empty() || budget.as_secs() < 60 {
        return None;
    }
    let msg = ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        head_sha: head_sha.to_string(),
        label,
        question: None,
        trigger: TRIGGER_RUN.to_string(),
        // The key the ready-for-review trigger uses for this head.
        dedup_key: format!("head#{head_sha}"),
        reply_to_comment_id: None,
        requested_by: String::new(),
        attempt: 0,
        wait_secs: 0,
        reconcile_note: None,
    };
    let mut reviewed_head = head_sha.to_string();
    let job = LiveJob::start();
    let out = tokio::time::timeout(
        budget.min(std::time::Duration::from_secs(REVIEW_BUDGET_SECS)),
        run_once(state, msg.clone(), &mut reviewed_head, &job),
    )
    .await;
    use common::review_live::Finished;
    match out {
        Ok(Ok(Outcome::Done { run_review, .. })) => {
            settle_job_claim(state, &msg, true).await;
            job.finished(state, &msg, Finished::Done).await;
            run_review
        }
        Ok(Ok(Outcome::ReviewThenReply { .. })) => {
            settle_job_claim(state, &msg, true).await;
            job.finished(state, &msg, Finished::Done).await;
            None
        }
        Ok(Ok(Outcome::Skipped { reason })) => {
            settle_job_claim(state, &msg, true).await;
            job.finished(state, &msg, Finished::Skipped { reason: &reason })
                .await;
            None
        }
        Ok(Err(e)) => {
            warn!(pr = pr_number, error = %e, "Run's review of its own PR didn't finish");
            settle_job_claim(state, &msg, false).await;
            let reason = e.to_string();
            job.finished(state, &msg, Finished::Failed { reason: &reason })
                .await;
            None
        }
        Err(_) => {
            warn!(pr = pr_number, "Run's review of its own PR ran out of time");
            settle_job_claim(state, &msg, false).await;
            let reason = "the review ran out of time";
            job.finished(state, &msg, Finished::Failed { reason }).await;
            None
        }
    }
}

/// How many self-review → self-fix rounds CoderHelm runs on its OWN PR before it
/// gives up and hands to a human. Bounds the review↔fix loop so a flip-flopping
/// verdict can't churn commits/reviews forever (one PR hit 67 commits / 44 reviews
/// oscillating APPROVE↔REQUEST_CHANGES).
const MAX_SELF_REVIEW_FIX_ROUNDS: u32 = 3;

/// Claim one self-review→fix round for this PR (atomic conditional increment).
/// True while under MAX_SELF_REVIEW_FIX_ROUNDS (proceed with the fix); false once
/// the cap is reached — or on ANY error (fail CLOSED, like claim_auto_fix_slot:
/// never keep looping when the bound can't be confirmed). Self-expires via TTL so
/// a genuinely fresh re-run of the ticket later starts over.
async fn claim_self_review_round(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
) -> bool {
    let sk = format!("SELFREVIEWROUNDS#{owner}/{repo}#{pr:0>6}");
    let ttl = chrono::Utc::now().timestamp() as u64 + 3 * 86_400;
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #ttl = :ttl ADD rounds :one")
        .condition_expression("attribute_not_exists(rounds) OR rounds < :max")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":max", attr_n(u64::from(MAX_SELF_REVIEW_FIX_ROUNDS)))
        .expression_attribute_values(":ttl", attr_n(ttl))
        .send()
        .await
        .is_ok()
}

/// Perform a one-time action for a key (e.g. post the handoff notice once).
/// Conditional put: true only for the first caller. Fails OPEN (acts) on a
/// transient error so a real one-time notice is never lost. TTL-expiring.
async fn claim_once(state: &WorkerState, team_id: &str, sk: &str) -> bool {
    claim_once_ttl(state, team_id, sk, 7 * 86_400).await
}

/// `claim_once` with a caller-chosen TTL. Short TTLs suit dedup markers that
/// should self-heal (a failed review can be re-driven once the marker expires);
/// long TTLs suit one-shot handoffs. Fails OPEN on a transient error.
pub(crate) async fn claim_once_ttl(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
    ttl_secs: u64,
) -> bool {
    // Honors the claim's own `ttl` (an expired claim is taken over), unlike
    // a bare attribute_not_exists that waits for DynamoDB's late sweeper.
    common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        sk,
        ttl_secs,
    )
    .await
    .won_or_failed_open()
}

/// The dedup claim of a review job (its lease while it runs).
fn job_claim_sk(msg: &ReviewMessage) -> Option<String> {
    (!msg.dedup_key.is_empty()).then(|| {
        format!(
            "REVIEWJOB#{}/{}#{:06}#{}",
            msg.repo_owner, msg.repo_name, msg.pr_number, msg.dedup_key
        )
    })
}

/// A finished job keeps its claim for the dedup window, so a redelivered
/// copy is dropped; a failed one gives it up (its retry has its own key).
async fn settle_job_claim(state: &WorkerState, msg: &ReviewMessage, finished: bool) {
    let Some(sk) = job_claim_sk(msg) else {
        return;
    };
    let table = &state.config.settings_table_name;
    if finished {
        common::claim::extend(
            &state.dynamo,
            table,
            &msg.team_id,
            &sk,
            REVIEW_JOB_DEDUP_TTL_SECS,
        )
        .await;
    } else {
        common::claim::release(&state.dynamo, table, &msg.team_id, &sk).await;
    }
}

/// Runs one review job inside the time budget: a job still running near
/// the Lambda timeout is a failure (retried), not a silent kill.
async fn run_budgeted(
    state: &WorkerState,
    msg: ReviewMessage,
    reviewed_head: &mut String,
    live_job: &LiveJob,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let job = msg.clone();
    let out = match tokio::time::timeout(
        std::time::Duration::from_secs(REVIEW_BUDGET_SECS),
        run_once(state, msg, reviewed_head, live_job),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(format!("the review ran past its {REVIEW_BUDGET_SECS}s budget").into()),
    };
    settle_job_claim(state, &job, out.is_ok()).await;
    out
}

/// Hand a person's fix request on CoderHelm's own PR to the run's fix loop,
/// addressed to them so the loop replies on the PR. False when there is no
/// run or no queue (the caller answers the person itself).
async fn forward_ask_to_fix_loop(state: &WorkerState, msg: &ReviewMessage, ask: &str) -> bool {
    if state.config.feedback_queue_url.is_empty() {
        warn!("FEEDBACK_QUEUE_URL not set — cannot route a fix request");
        return false;
    }
    let Some(run_id) = lookup_run_by_pr(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await
    else {
        info!(
            pr = msg.pr_number,
            "Fix request on CoderHelm's PR but no run found — answering it as a question"
        );
        return false;
    };
    let body = serde_json::json!({
        "type": "feedback",
        "team_id": msg.team_id,
        "installation_id": msg.installation_id,
        "run_id": run_id,
        "repo_owner": msg.repo_owner,
        "repo_name": msg.repo_name,
        "pr_number": msg.pr_number,
        "review_id": 0,
        "review_body": ask,
        "comments": [],
        "trigger_author": msg.requested_by,
    });
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(body.to_string())
        .send()
        .await
    {
        Ok(_) => {
            info!(pr = msg.pr_number, run_id = %run_id, "Fix request on CoderHelm's PR → fix loop");
            true
        }
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Failed to enqueue the fix request");
            false
        }
    }
}

/// Route a self-review's requested changes into the originating run's feedback
/// loop so CoderHelm fixes them. Best-effort: no run found (PR opened outside a
/// tracked run) or no queue configured → a logged no-op, never an error.
async fn feed_review_back_to_run(state: &WorkerState, msg: &ReviewMessage, review_body: &str) {
    let Some(run_id) = lookup_run_by_pr(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await
    else {
        info!(
            pr = msg.pr_number,
            "Self-review requested changes but no run found — skipping feedback"
        );
        return;
    };
    if state.config.feedback_queue_url.is_empty() {
        warn!("FEEDBACK_QUEUE_URL not set — cannot route self-review feedback");
        return;
    }
    let body = serde_json::json!({
        "type": "feedback",
        "team_id": msg.team_id,
        "installation_id": msg.installation_id,
        "run_id": run_id,
        "repo_owner": msg.repo_owner,
        "repo_name": msg.repo_name,
        "pr_number": msg.pr_number,
        "review_id": 0,
        "review_body": format!("Automated reviewer requested changes:\n\n{review_body}"),
        "comments": [],
    });
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(body.to_string())
        .send()
        .await
    {
        Ok(_) => info!(
            pr = msg.pr_number,
            run_id = %run_id,
            "Reviewer requested changes on own PR → fed back to run"
        ),
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Failed to enqueue self-review feedback")
        }
    }
}

/// Build a "past feedback to learn from" block from this repo's recent review
/// records — the 👎 ratings and human notes the team left. Reused so the reviewer
/// stops repeating comment styles the team rejected. Best-effort, capped, empty
/// on any error.
async fn load_learning_context(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> String {
    let prefix = format!("REVIEW#{owner}/{name}#");
    let Ok(res) = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :pfx)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":pfx", attr_s(&prefix))
        .scan_index_forward(false)
        .limit(60)
        .send()
        .await
    else {
        return String::new();
    };

    let mut notes: Vec<String> = vec![];
    for item in res.items() {
        if notes.len() >= 15 {
            break;
        }
        let comments = item
            .get("rating_comments")
            .and_then(|v| v.as_l().ok())
            .cloned()
            .unwrap_or_default();
        for c in comments {
            let Ok(s) = c.as_s() else { continue };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
                continue;
            };
            let text = v["text"].as_str().unwrap_or("").trim();
            if text.is_empty() {
                continue;
            }
            let rating = v["rating"].as_str().unwrap_or("");
            let tag = if rating == "down" { "👎" } else { "📝" };
            notes.push(format!("- {tag} {}", common::truncate_str(text, 1000)));
            if notes.len() >= 15 {
                break;
            }
        }
    }
    if notes.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Past reviewer feedback from this team (learn from it — don't repeat rejected styles)\n{}",
        notes.join("\n")
    )
}

/// What the planner of CoderHelm's run behind this PR found about where the
/// fix belongs (empty for a person's PR or a plan without a decision).
async fn plan_evidence_for_pr(state: &WorkerState, msg: &ReviewMessage) -> String {
    let Some(run_id) = lookup_run_by_pr(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await
    else {
        return String::new();
    };
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("fix_decision")
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    item.as_ref()
        .and_then(|i| i.get("fix_decision"))
        .and_then(|v| v.as_s().ok())
        .and_then(|j| serde_json::from_str::<super::fix_location::FixDecision>(j).ok())
        .map(|d| super::fix_location::reviewer_context(&d))
        .unwrap_or_default()
}

/// Newest run for a PR, via the runs-table repo-index GSI. None if untracked.
async fn lookup_run_by_pr(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Option<String> {
    let team_repo = format!("{team_id}#{owner}/{name}");
    // Limit applies before the filter: page through until a match or the end.
    let query = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(50);
    common::paged::query_first_match(query, common::paged::run_id_of)
        .await
        .ok()
        .flatten()
}

/// CoderHelm's most recent review of a PR.
struct LastReview {
    verdict: String,
    head_sha: String,
    created_at: String,
}

/// The most recent verdict CoderHelm recorded for this PR (any head), or None if
/// it has never reviewed it. Records are keyed sk=REVIEW#{repo}#{pr:06}#{rfc3339},
/// so a descending query yields newest-first. Question answers are skipped.
async fn last_review(state: &WorkerState, msg: &ReviewMessage) -> Option<LastReview> {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let prefix = format!("REVIEW#{repo}#{:0>6}#", msg.pr_number);
    let resp = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
        .expression_attribute_values(":pk", attr_s(&msg.team_id))
        .expression_attribute_values(":sk", attr_s(&prefix))
        .scan_index_forward(false)
        .consistent_read(true)
        .limit(10)
        .send()
        .await
        .ok()?;
    let get = |i: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
               k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    resp.items().iter().find_map(|i| {
        let verdict = get(i, "verdict");
        // An old error review is not a verdict on the code.
        if get(i, "body")
            .trim_start()
            .starts_with(super::review_sweep::LEGACY_FAILED_PREFIX)
        {
            return None;
        }
        (verdict == "APPROVE" || verdict == "REQUEST_CHANGES").then(|| LastReview {
            verdict,
            head_sha: get(i, "head_sha"),
            created_at: get(i, "created_at"),
        })
    })
}

/// Pure: was the head commit authored by CoderHelm? Reads the compare
/// response's commit list (oldest first).
fn head_commit_by_bot(compare: &serde_json::Value, head_sha: &str) -> bool {
    compare["commits"]
        .as_array()
        .and_then(|cs| {
            cs.iter()
                .rev()
                .find(|c| c["sha"].as_str() == Some(head_sha))
                .or_else(|| cs.last())
        })
        .and_then(|c| c["author"]["login"].as_str())
        .is_some_and(|login| login.contains("coderhelm"))
}

/// Persist a review record to the settings table so the dashboard can list it and
/// ratings/actions can attach. Keyed pk=team_id, sk=REVIEW#{repo}#{pr:06}#{ts}.
/// Best-effort: a storage failure must never break the actual GitHub review.
/// Returns the record's sort key so follow-up steps can UPDATE this record
/// (e.g. attach the auto-merge action summary) instead of writing a second one
/// — one review execution must never produce two dashboard records.
#[allow(clippy::too_many_arguments)]
async fn store_review_record(
    state: &WorkerState,
    msg: &ReviewMessage,
    head_sha: &str,
    verdict: &str,
    risk: &str,
    body: &str,
    posted_as: &str,
    action_summary: &str,
    counts: &common::review_stats::ReviewCounts,
) -> String {
    let created_at = chrono::Utc::now().to_rfc3339();
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let sk = format!("REVIEW#{repo}#{:0>6}#{created_at}", msg.pr_number);
    // Truncate the stored body so a huge review can't blow the 400KB item limit.
    let body = common::head_tail_str(body, 30_000);

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(&sk))
        .item("record_type", attr_s("review"))
        .item("repo", attr_s(&repo))
        .item("pr_number", attr_n(msg.pr_number))
        .item("head_sha", attr_s(head_sha))
        .item("verdict", attr_s(verdict))
        .item("risk", attr_s(risk))
        .item("body", attr_s(&body))
        .item("posted_as", attr_s(posted_as))
        .item("trigger", attr_s(&msg.trigger))
        .item("thumbs_up", attr_n(0))
        .item("thumbs_down", attr_n(0))
        .item("created_at", attr_s(&created_at));
    if !action_summary.is_empty() {
        put = put.item(
            "action_summary",
            attr_s(&common::head_tail_str(action_summary, 8_000)),
        );
    }
    if let Err(e) = put.send().await {
        warn!(pr = msg.pr_number, error = %e, "Failed to persist review record (non-fatal)");
    }
    // A small, time-ordered copy for analytics (no body).
    let stat = common::review_stats::stat_item(
        &msg.team_id,
        &created_at,
        &repo,
        msg.pr_number,
        verdict,
        posted_as,
        risk,
        &msg.trigger,
        counts,
    );
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(stat))
        .send()
        .await
    {
        warn!(pr = msg.pr_number, error = %e, "Failed to persist review stats (non-fatal)");
    }
    sk
}

/// Names the team standards a review was given in full on its record, for
/// its page. Nothing is written when there were none.
async fn note_review_standards(
    state: &WorkerState,
    team_id: &str,
    record_sk: &str,
    names: &[String],
) {
    if names.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(record_sk))
        .condition_expression("attribute_exists(sk)")
        .update_expression("SET applied_standards = :n")
        .expression_attribute_values(
            ":n",
            aws_sdk_dynamodb::types::AttributeValue::L(names.iter().map(|n| attr_s(n)).collect()),
        )
        .send()
        .await
    {
        warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Failed to note the review's team standards (non-fatal)");
    }
}

/// The review's findings as the dashboard shows them: one object per
/// finding with its severity, what it rests on (`defect`, `concern` or
/// `question`), whether it holds the merge, and where it points. Bodies are
/// cut so the list stays well under the item limit.
fn findings_record(findings: &[review_agent::Finding]) -> String {
    let list: Vec<serde_json::Value> = findings
        .iter()
        .map(|f| {
            let kind = match f.evidence() {
                review_agent::Evidence::VerifiedDefect => "defect",
                review_agent::Evidence::UnverifiedConcern => "concern",
                review_agent::Evidence::Question => "question",
            };
            serde_json::json!({
                "severity": f.severity.trim().to_ascii_lowercase(),
                "kind": kind,
                "blocks": f.holds_merge(),
                "file": f.file,
                "line": f.line,
                "title": f.title,
                "body": common::head_tail_str(&f.body, 4_000),
            })
        })
        .collect();
    serde_json::Value::Array(list).to_string()
}

/// Puts the PR's title and the structured findings on the review record, for
/// its page. Best-effort, like the record itself.
async fn note_review_details(
    state: &WorkerState,
    team_id: &str,
    record_sk: &str,
    pr_title: &str,
    findings: &[review_agent::Finding],
) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(record_sk))
        .condition_expression("attribute_exists(sk)")
        .update_expression("SET pr_title = :t, findings = :f")
        .expression_attribute_values(":t", attr_s(&common::head_tail_str(pr_title, 500)))
        .expression_attribute_values(":f", attr_s(&findings_record(findings)))
        .send()
        .await
    {
        warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Failed to note the review's title and findings (non-fatal)");
    }
}

/// Cost of a review as stored (4 decimals, like run records).
fn review_cost_str(usage: &TokenUsage) -> String {
    format!("{:.4}", usage.estimated_cost())
}

/// True when the review used the model at all.
fn has_usage(usage: &TokenUsage) -> bool {
    usage.total_tokens() > 0
}

/// Persist what a review used on its REVIEW# record and count the review on the
/// team's analytics item. Its tokens and cost reach the team's counters per
/// model call (common::usage), so they are not added again here.
async fn record_review_usage(
    state: &WorkerState,
    team_id: &str,
    record_sk: Option<&str>,
    usage: &TokenUsage,
) {
    if !has_usage(usage) {
        return;
    }
    let cost = review_cost_str(usage);
    if let Some(sk) = record_sk {
        if let Err(e) = state
            .dynamo
            .update_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s(sk))
            // Never create a partial record if the put above failed.
            .condition_expression("attribute_exists(sk)")
            .update_expression(
                "SET tokens_in = :ti, tokens_out = :to, cache_read_tokens = :crt, \
                 cache_write_tokens = :cwt, cost_usd = :c",
            )
            .expression_attribute_values(":ti", attr_n(usage.input_tokens))
            .expression_attribute_values(":to", attr_n(usage.output_tokens))
            .expression_attribute_values(":crt", attr_n(usage.cache_read_tokens))
            .expression_attribute_values(":cwt", attr_n(usage.cache_write_tokens))
            .expression_attribute_values(":c", attr_n(&cost))
            .send()
            .await
        {
            warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Failed to store review cost (non-fatal)");
        }
    }
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in [month.as_str(), "ALL_TIME"] {
        if let Err(e) = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(team_id))
            .key("period", attr_s(period))
            // Tokens and cost are already counted per model call
            // (common::usage, feature "review"); only the review count here.
            .update_expression("ADD reviews :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await
        {
            warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Failed to add review cost to team usage (non-fatal)");
        }
    }
}

/// What the reviewer is told when it answers a person on a PR.
pub(crate) fn reply_system_prompt(
    owner: &str,
    repo: &str,
    instructions: &str,
    org: &str,
    extra: &str,
) -> String {
    format!(
        "You are a senior engineer answering a question about an open PR in {}/{}. Use the diff \
             and PR description as ground truth; be concrete and cite file:line. If the answer \
             isn't determinable from the diff, say so. \
             You have read-only tools to read and search the repository at the PR head. Use them \
             to check anything the answer depends on: never ask a person to search, grep or look \
             something up that you can check yourself, and never say you can't see a file. \
             Answer what was asked and nothing more, in at most five short sentences or bullets, \
             with no headings and no tables. This is a reply, not a review: do not add new \
             concerns or re-raise earlier ones. When the \
             person confirms a fact or a decision, accept it and say so in one line. \
             If you say what is left before merge, give every remaining item in this one reply. \
             Once you or an earlier reply of yours has said the code looks fine, don't bring up \
             new non-blocking items (description wording or counts, files the description \
             doesn't mention, style); raise something new only if it is a real defect in the \
             code. A request to test something by hand is a note for the people shipping it, \
             not a reason to hold the merge. {}{}{}{}{}{}",
        owner,
        repo,
        review_agent::CAPABILITIES_RULE,
        review_agent::COPY_RULE,
        review_agent::TONE_RULE,
        instructions,
        org,
        extra,
    )
}

/// The last `max` conversation comments CoderHelm wrote, oldest first.
pub(crate) fn own_comments(comments: &[serde_json::Value], max: usize) -> Vec<String> {
    let own: Vec<String> = comments
        .iter()
        .filter(|c| {
            c["user"]["type"].as_str() == Some("Bot")
                && c["user"]["login"]
                    .as_str()
                    .is_some_and(|l| l.contains("coderhelm"))
        })
        .filter_map(|c| c["body"].as_str().map(|b| b.trim().to_string()))
        .filter(|b| !b.is_empty())
        .collect();
    let skip = own.len().saturating_sub(max);
    own.into_iter().skip(skip).collect()
}

/// The job that re-runs the formal review of `head` after a reply concluded
/// something the standing verdict doesn't match.
pub(crate) fn reconcile_message(msg: &ReviewMessage, head: &str, reason: &str) -> ReviewMessage {
    let mut job = msg.clone();
    job.question = None;
    job.reply_to_comment_id = None;
    job.head_sha = head.to_string();
    job.trigger = "reconcile".to_string();
    job.dedup_key = format!("reconcile#{head}#{}", msg.dedup_key);
    job.attempt = 0;
    job.wait_secs = 0;
    job.reconcile_note = Some(reason.to_string());
    job
}

/// What a person's comment to CoderHelm asks for, judged by the team's
/// light model with structured output (metered like every call). A failed
/// check is read as a question: the person gets an answer, and nothing about
/// the formal review changes.
async fn classify_ask(
    state: &WorkerState,
    provider: &ModelProvider,
    comment: &str,
    standing: Option<&super::review_verdict::StandingReview>,
    last_reply: Option<&str>,
    usage: &mut TokenUsage,
) -> review_agent::AskKind {
    let standing_line = match standing {
        Some(s) if s.failed => {
            "Changes requested by a review that didn't finish (a service error).".to_string()
        }
        Some(s) if s.verdict == "REQUEST_CHANGES" => "Changes requested.".to_string(),
        Some(s) => format!(
            "{} on {}.",
            s.verdict,
            &s.head_sha[..s.head_sha.len().min(7)]
        ),
        None => "No formal review yet.".to_string(),
    };
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        review_agent::ASK_SYSTEM,
        &review_agent::ask_prompt(comment, &standing_line, last_reply),
        &review_agent::ask_schema(),
        true,
        "low",
        512,
        usage,
    )
    .await
    {
        Ok(v) => review_agent::parse_ask(&v),
        Err(e) => {
            warn!(error = %e, "Could not classify the ask; answering it as a question");
            review_agent::AskKind::Question
        }
    }
}

/// The model's reading of a reply about to be posted (see
/// [`review_agent::ReplyConclusion`]). None when the check itself failed:
/// the reply is posted as written and the verdict left as it is.
async fn reply_conclusion(
    provider: &ModelProvider,
    earlier_replies: &[String],
    question: &str,
    answer: &str,
    usage: &mut TokenUsage,
) -> Option<review_agent::ReplyConclusion> {
    match crate::agent::anthropic::converse_json(
        &provider.client(),
        provider.primary_model_id(),
        review_agent::REPLY_CONCLUSION_SYSTEM,
        &review_agent::reply_conclusion_prompt(earlier_replies, question, answer),
        &review_agent::reply_conclusion_schema(),
        true,
        "low",
        4096,
        usage,
    )
    .await
    {
        Ok(v) => Some(review_agent::parse_reply_conclusion(&v)),
        Err(e) => {
            warn!(error = %e, "Could not check the reply's conclusion");
            None
        }
    }
}

/// The last `max` conversation comments written by people, as `(author, body)`
/// pairs, oldest first. Bot comments are left out.
pub(crate) fn human_comments(comments: &[serde_json::Value], max: usize) -> Vec<(String, String)> {
    let human: Vec<(String, String)> = comments
        .iter()
        .filter(|c| c["user"]["type"].as_str() != Some("Bot"))
        .filter_map(|c| {
            let author = c["user"]["login"].as_str()?;
            let body = c["body"].as_str()?.trim();
            (!author.contains("coderhelm") && !body.is_empty())
                .then(|| (author.to_string(), body.to_string()))
        })
        .collect();
    let skip = human.len().saturating_sub(max);
    human.into_iter().skip(skip).collect()
}

/// Earlier threads the model reported fixed or answered. Indices that are out
/// of range or duplicated are ignored; anything else stays open.
fn settled_prior_threads<'a>(
    prior: &[&'a crate::clients::github::BotReviewThread],
    statuses: &[review_agent::PriorStatus],
) -> Vec<&'a crate::clients::github::BotReviewThread> {
    let mut seen = std::collections::HashSet::new();
    statuses
        .iter()
        .filter(|s| s.is_settled() && s.index < prior.len() && seen.insert(s.index))
        .map(|s| prior[s.index])
        .collect()
}

#[cfg(test)]
mod review_usage_tests {
    use super::*;

    #[test]
    fn empty_usage_is_not_recorded() {
        assert!(!has_usage(&TokenUsage::default()));
        let u = TokenUsage {
            output_tokens: 1,
            ..Default::default()
        };
        assert!(has_usage(&u));
    }

    #[test]
    fn cost_matches_the_run_formula() {
        let mut u = TokenUsage::default();
        u.add_call(
            "claude-opus-5-5",
            &common::usage::CallUsage {
                uncached_input: 100_000,
                output: 10_000,
                ..Default::default()
            },
        );
        assert_eq!(review_cost_str(&u), format!("{:.4}", u.estimated_cost()));
        // Opus 5.5: 0.1M × $4 + 0.01M × $20.
        assert_eq!(review_cost_str(&u), "0.6000");
        assert!(review_cost_str(&u).parse::<f64>().unwrap() > 0.0);
    }
}

#[cfg(test)]
mod prior_thread_tests {
    use super::*;
    use crate::clients::github::BotReviewThread;
    use review_agent::PriorStatus;

    fn t(id: &str) -> BotReviewThread {
        BotReviewThread {
            id: id.into(),
            path: "a.rs".into(),
            line: 1,
            outdated: false,
            body: "x".into(),
            replies: vec![],
            resolved: false,
            resolved_by_person: false,
        }
    }
    fn st(index: usize, status: &str) -> PriorStatus {
        PriorStatus {
            index,
            status: status.into(),
        }
    }

    #[test]
    fn only_explicit_fixed_or_answered_in_range_once() {
        let threads = [t("A"), t("B"), t("C"), t("D")];
        let prior: Vec<&BotReviewThread> = threads.iter().collect();
        let statuses = vec![
            st(0, "fixed"),
            st(1, "open"),
            st(0, "FIXED"), // duplicate
            st(9, "fixed"), // out of range
            st(3, "answered"),
        ];
        let got: Vec<&str> = settled_prior_threads(&prior, &statuses)
            .iter()
            .map(|t| t.id.as_str())
            .collect();
        assert_eq!(got, vec!["A", "D"]);
    }

    #[test]
    fn missing_statuses_mean_nothing_resolved() {
        let threads = [t("A"), t("B")];
        let prior: Vec<&BotReviewThread> = threads.iter().collect();
        assert!(settled_prior_threads(&prior, &[]).is_empty());
    }

    #[test]
    fn replies_and_resolved_threads_reach_the_prompt() {
        let mut open = t("A");
        open.replies = vec![crate::clients::github::ThreadReply {
            author: "dev".into(),
            by_bot: false,
            body: "The API change is live\nin production.".into(),
        }];
        let s = review_agent::format_prior_threads(&[&open]);
        assert!(s.contains("0: a.rs:1 — x"));
        assert!(s.contains("reply from dev: The API change is live in production."));
        assert!(s.contains("\"answered\""));

        let mut resolved = t("B");
        resolved.resolved = true;
        resolved.resolved_by_person = true;
        let s = review_agent::format_settled_threads(&[&resolved]);
        assert!(s.contains("a person resolved"));
        assert!(s.contains("- a.rs:1 — x"));
        assert!(review_agent::format_settled_threads(&[]).is_empty());
    }

    #[test]
    fn only_the_latest_human_comments_are_kept() {
        let c = |login: &str, kind: &str, body: &str| serde_json::json!({"user": {"login": login, "type": kind}, "body": body});
        let comments = vec![
            c("dev", "User", "first"),
            c("coderhelm[bot]", "Bot", "review"),
            c("ci-bot", "Bot", "build ok"),
            c("dev", "User", "  "),
            c(
                "lead",
                "User",
                "@coderhelm re-review — the API change is deployed",
            ),
            c("dev", "User", "third"),
        ];
        let got = human_comments(&comments, 2);
        assert_eq!(
            got,
            vec![
                (
                    "lead".to_string(),
                    "@coderhelm re-review — the API change is deployed".to_string()
                ),
                ("dev".to_string(), "third".to_string()),
            ]
        );
        let s = review_agent::format_discussion(&got);
        assert!(s.contains("- lead: @coderhelm re-review — the API change is deployed"));
    }
}

/// A verdict review is superseded once the PR's head is no longer the commit it
/// was enqueued for; that commit's own review covers the latest code. Reviews
/// that answer a question, or that resolve the head themselves (empty
/// head_sha), always speak.
/// How recent another review of the same commit must be for a reply
/// re-review to stand down.
const SAME_COMMIT_WINDOW_SECS: i64 = 90;

/// Takes the per-commit review claim (the same item the gateway writes for
/// push reviews). False when another review claimed this commit within the
/// last few minutes; an older claim doesn't stop an explicit re-review.
async fn claim_recent_head(state: &WorkerState, msg: &ReviewMessage, head_sha: &str) -> bool {
    let sk = format!(
        "REVIEWCLAIM#{}/{}#{:06}#{head_sha}",
        msg.repo_owner, msg.repo_name, msg.pr_number
    );
    let now = chrono::Utc::now();
    let existing = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&sk))
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|o| {
            o.item()
                .and_then(|i| i.get("created_at"))
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t).ok());
    if let Some(at) = existing {
        if (now - at.with_timezone(&chrono::Utc)).num_seconds() < SAME_COMMIT_WINDOW_SECS {
            return false;
        }
    }
    // Claim it, so a push event for this commit arriving now stands down.
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(&sk))
        .item("created_at", attr_s(&now.to_rfc3339()))
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N((now.timestamp() + 6 * 3600).to_string()),
        )
        .send()
        .await;
    true
}

/// Files a compare response says changed.
fn changed_files(compare: &serde_json::Value) -> std::collections::HashSet<String> {
    compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Splits findings into those in a file changed since the approval (they keep
/// their weight) and those on unchanged files (kept as notes).
fn hold_unchanged(
    findings: Vec<review_agent::Finding>,
    touched: &std::collections::HashSet<String>,
) -> (Vec<review_agent::Finding>, Vec<review_agent::Finding>) {
    findings
        .into_iter()
        .partition(|f| touched.contains(&f.file))
}

/// The review's overview, held to what it is asked to be: a few sentences.
/// Models sometimes write essays with headings here; the details belong in
/// the findings, which are posted on the lines they are about.
fn short_overview(text: &str) -> String {
    const MAX_SENTENCES: usize = 4;
    const MAX_CHARS: usize = 700;
    let prose: String = text
        .lines()
        .map(str::trim)
        .filter(|l| {
            !l.is_empty() && !l.starts_with('#') && !l.starts_with('|') && !l.starts_with("```")
        })
        .collect::<Vec<_>>()
        .join(" ");
    let mut out = String::new();
    for piece in prose.split_inclusive(['.', '!', '?']).take(MAX_SENTENCES) {
        if out.len() + piece.len() > MAX_CHARS {
            break;
        }
        out.push_str(piece);
    }
    let out = out.trim().to_string();
    if out.is_empty() {
        common::truncate_str(&prose, MAX_CHARS).trim().to_string()
    } else {
        out
    }
}

/// Whether the PR moved past the commit this review looked at. Unlike
/// [`superseded`], this also covers re-reviews asked for in a reply, which
/// resolve the head when they start: an author who answers a finding and
/// pushes the fix a minute later must not get a "request changes" on the code
/// they just replaced (the new commit gets its own review).
fn moved_on(msg: &ReviewMessage, reviewed_head: &str, latest_head: &str) -> bool {
    msg.question.is_none()
        && !reviewed_head.is_empty()
        && !latest_head.is_empty()
        && reviewed_head != latest_head
}

fn superseded(msg: &ReviewMessage, current_head: &str) -> bool {
    msg.question.is_none()
        && !msg.head_sha.is_empty()
        && !current_head.is_empty()
        && current_head != msg.head_sha
}

#[cfg(test)]
mod superseded_tests {
    use super::{moved_on, superseded};
    use crate::models::ReviewMessage;

    fn msg(head: &str, question: Option<&str>) -> ReviewMessage {
        ReviewMessage {
            team_id: "T".into(),
            installation_id: 1,
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 1322,
            head_sha: head.into(),
            label: "ch-review".into(),
            question: question.map(String::from),
            trigger: "synchronize".into(),
            dedup_key: String::new(),
            reply_to_comment_id: None,
            requested_by: String::new(),
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
        }
    }

    #[test]
    fn only_a_verdict_for_an_older_commit_is_superseded() {
        // b20e26f's review must not post once cc15ab8 is the head.
        assert!(superseded(&msg("b20e26f", None), "cc15ab8"));
        assert!(!superseded(&msg("cc15ab8", None), "cc15ab8"));
        // A question is answered whatever the head; an empty head_sha targets the latest.
        assert!(!superseded(&msg("b20e26f", Some("why?")), "cc15ab8"));
        assert!(!superseded(&msg("", None), "cc15ab8"));
        // An unreadable head never silences a review.
        assert!(!superseded(&msg("b20e26f", None), ""));
    }

    #[test]
    fn the_overview_stays_a_few_sentences() {
        let essay = "# Re-review of PR 12\n\nThe description now matches. But the change isn't only copy. It hardcodes a price. It adds policy claims. It drops the tax note.\n\n## Blocking\n\n| a | b |\n|---|---|";
        let s = super::short_overview(essay);
        assert_eq!(s, "The description now matches. But the change isn't only copy. It hardcodes a price. It adds policy claims.");
        assert_eq!(super::short_overview("Looks good."), "Looks good.");
        let long = "word ".repeat(400);
        assert!(super::short_overview(&long).len() <= 700);
    }

    #[test]
    fn findings_on_unchanged_files_are_held_after_an_approval() {
        let f = |file: &str| crate::passes::review_agent::Finding {
            file: file.into(),
            ..Default::default()
        };
        let touched: std::collections::HashSet<String> = ["src/a.ts".to_string()].into();
        let (keep, held) = super::hold_unchanged(vec![f("src/a.ts"), f("src/b.ts")], &touched);
        assert_eq!(keep.len(), 1);
        assert_eq!(held[0].file, "src/b.ts");
        let compare =
            serde_json::json!({"files": [{"filename": "src/a.ts"}, {"filename": "x.md"}]});
        assert_eq!(super::changed_files(&compare).len(), 2);
    }

    #[test]
    fn a_reply_review_does_not_post_on_code_already_replaced() {
        // A re-review asked for in a reply resolves the head when it starts
        // (msg.head_sha empty), so `superseded` can't see it move; `moved_on`
        // compares the head it actually reviewed.
        assert!(moved_on(&msg("", None), "1c20c41", "36732132"));
        assert!(!moved_on(&msg("", None), "36732132", "36732132"));
        // A question is an answer, not a verdict; it is always posted.
        assert!(!moved_on(&msg("", Some("why?")), "1c20c41", "36732132"));
        assert!(!moved_on(&msg("", None), "1c20c41", ""));
    }
}

#[cfg(test)]
mod verdict_tests {
    use super::review_agent::{decide_verdict, thread_holds_merge, Finding, NON_BLOCKING_MARK};

    fn finding(category: &str, evidence: Option<&str>, body: &str) -> Finding {
        Finding {
            file: "infra/redirects.tf".into(),
            line: 12,
            severity: "medium".into(),
            category: category.into(),
            title: "Redirect targets".into(),
            body: body.into(),
            evidence: evidence.map(String::from),
            ..Default::default()
        }
    }

    /// The record's findings carry what the page shows: severity, kind,
    /// whether it holds the merge, and where it points.
    #[test]
    fn the_findings_record_says_what_each_finding_is_and_where() {
        let mut q = finding("question", None, "Is the field ever null?");
        q.blocks = Some(true);
        let list = vec![
            finding("bug", None, "The handler reads `id` before the check."),
            finding("bug", Some("unverified-concern"), "Might time out."),
            q,
        ];
        let v: serde_json::Value = serde_json::from_str(&super::findings_record(&list)).unwrap();
        let a = v.as_array().unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a[0]["kind"], "defect");
        assert_eq!(a[0]["blocks"], true);
        assert_eq!(a[0]["severity"], "medium");
        assert_eq!(a[0]["file"], "infra/redirects.tf");
        assert_eq!(a[0]["line"], 12);
        assert_eq!(a[0]["title"], "Redirect targets");
        assert_eq!(a[0]["body"], "The handler reads `id` before the check.");
        assert_eq!(a[1]["kind"], "concern");
        assert_eq!(a[1]["blocks"], false);
        assert_eq!(a[2]["kind"], "question");
        assert_eq!(a[2]["blocks"], true);
        assert_eq!(super::findings_record(&[]), "[]");
    }

    /// A carried approval says it is one, and why, in its one line.
    #[test]
    fn a_carried_approval_names_the_approved_commit_and_why() {
        let merged = super::carried_approval_body("abc1234def", true);
        assert_eq!(
            merged,
            "✅ **Approved**: the new commits only bring in the base branch; the PR's own \
             change is the same as the approved `abc1234`."
        );
        let docs = super::carried_approval_body("abc1234def", false);
        assert!(docs.starts_with("✅ **Approved**: "), "{docs}");
        assert!(docs.contains("only docs, comments or whitespace"));
        for b in [merged, docs] {
            assert_eq!(b.lines().count(), 1);
        }
    }

    #[test]
    fn approves_only_with_nothing_left_to_change() {
        assert_eq!(decide_verdict(&[], 0, &[], false), "APPROVE");
        // A low + a nit, both shown in the code
        let defects = [
            finding("bug", Some("verified-defect"), "off by one"),
            finding("convention", None, "nit"),
        ];
        assert_eq!(decide_verdict(&defects, 0, &[], false), "REQUEST_CHANGES");
        // an earlier requested change still open
        assert_eq!(decide_verdict(&[], 1, &[], false), "REQUEST_CHANGES");
        assert_eq!(decide_verdict(&[], 0, &[], true), "REQUEST_CHANGES");
    }

    /// The case that held a PR for three pushes: six 301 targets changed in a
    /// redirects map, and the only finding asked a person to confirm they
    /// work because the reviewer couldn't open a page.
    #[test]
    fn a_please_confirm_finding_never_requests_changes() {
        let ask = "Please confirm the six new 301 targets work on the live site before merge; \
                   I couldn't check this myself.";
        for f in [
            finding("question", None, ask),
            finding("question", Some("verified-defect"), ask),
            finding("correctness", Some("question"), ask),
        ] {
            assert!(!f.holds_merge(), "{:?}", f.evidence);
            // A question the change doesn't rest on is a note on an approval.
            assert_eq!(decide_verdict(&[f], 0, &[], false), "APPROVE");
        }
        // A concern the reviewer couldn't verify is said with the approval.
        let concern = finding("correctness", Some("unverified-concern"), ask);
        assert!(!concern.holds_merge());
        assert_eq!(decide_verdict(&[concern], 0, &[], false), "APPROVE");
        // Its open thread from the earlier push doesn't hold the next one.
        let old_thread = "💬 **Confirm redirect targets** (question)\n\nPlease confirm …";
        assert!(!thread_holds_merge(old_thread));
        assert!(!thread_holds_merge(&format!(
            "💬 **X** (correctness, not verified)\n\nbody\n\n{NON_BLOCKING_MARK}"
        )));
        assert!(thread_holds_merge(
            "🔴 **Wrong target** (bug)\n\n/docs → 404"
        ));
    }

    /// Questions and suggestions the author answered or accepted are settled,
    /// and non-blocking notes (a line count in the description) ride along
    /// with the approval: only verified defects, required tests, failed
    /// checks and questions the change rests on hold it back.
    #[test]
    fn answered_or_accepted_items_and_notes_do_not_block_approval() {
        use super::review_agent::PriorStatus;
        for status in ["fixed", "answered", "accepted", "ACCEPTED"] {
            let p = PriorStatus {
                index: 0,
                status: status.into(),
            };
            assert!(p.is_settled(), "{status}");
        }
        assert!(!PriorStatus {
            index: 0,
            status: "open".into()
        }
        .is_settled());
        // Every earlier thread settled, a description nit left: approve.
        let nit = Finding {
            severity: "nit".into(),
            category: "convention".into(),
            evidence: Some("unverified-concern".into()),
            title: "Description says +39 lines, diff shows +41".into(),
            ..Default::default()
        };
        assert_eq!(
            decide_verdict(std::slice::from_ref(&nit), 0, &[], false),
            "APPROVE"
        );
        // A required test or a failed check still blocks.
        let mut tests = Finding {
            category: "tests".into(),
            ..Default::default()
        };
        super::review_agent::enforce_required_tests(std::slice::from_mut(&mut tests));
        assert_eq!(
            decide_verdict(&[nit, tests], 0, &[], false),
            "REQUEST_CHANGES"
        );
        assert_eq!(decide_verdict(&[], 0, &[], true), "REQUEST_CHANGES");
    }

    /// check_url ran: all six targets returned 200 → nothing to ask, the
    /// reviewer reports the check in its summary and has no finding.
    #[tokio::test]
    async fn checked_targets_that_load_approve() {
        use super::super::web_check::{check, HostPolicy};
        let fake = fixture::six_targets(200);
        let gate = HostPolicy {
            domains: vec!["example.com".into()],
            ..Default::default()
        };
        for u in fixture::TARGETS {
            let c = check(u, &gate, &fake).await.unwrap();
            assert_eq!(c.status(), Some(200), "{u}");
        }
        assert_eq!(decide_verdict(&[], 0, &[], false), "APPROVE");
    }

    /// check_url ran and a target is a 404: that is a verified defect with
    /// the check as its evidence, and it requests changes.
    #[tokio::test]
    async fn a_checked_target_that_404s_requests_changes_with_the_evidence() {
        use super::super::web_check::{check, HostPolicy};
        let fake = fixture::six_targets(404);
        let gate = HostPolicy {
            domains: vec!["example.com".into()],
            ..Default::default()
        };
        let c = check(fixture::TARGETS[2], &gate, &fake).await.unwrap();
        assert_eq!(c.status(), Some(404));
        let evidence = format!("check_url: {}", c.line());
        let f = finding(
            "bug",
            Some("verified-defect"),
            &format!("The new 301 target is missing. {evidence}"),
        );
        assert!(f.holds_merge());
        assert!(f.body.contains("404"));
        assert_eq!(decide_verdict(&[f], 0, &[], false), "REQUEST_CHANGES");
    }

    mod fixture {
        use super::super::super::web_check::{Method, Resp, Transport};
        use reqwest::Url;
        use std::net::{IpAddr, SocketAddr};

        pub const TARGETS: [&str; 6] = [
            "https://www.example.com/pricing",
            "https://www.example.com/docs/start",
            "https://www.example.com/blog/launch",
            "https://www.example.com/careers",
            "https://www.example.com/legal/terms",
            "https://www.example.com/contact",
        ];

        /// Every target answers `status`; the site resolves publicly.
        pub struct Site(pub u16);

        pub fn six_targets(status: u16) -> Site {
            Site(status)
        }

        #[async_trait::async_trait]
        impl Transport for Site {
            async fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
                Ok(vec!["93.184.215.14".parse().unwrap()])
            }
            async fn send(&self, _m: Method, url: &Url, _a: SocketAddr) -> Result<Resp, String> {
                assert!(TARGETS.contains(&url.as_str()), "{url}");
                Ok(Resp {
                    status: self.0,
                    content_type: "text/plain".into(),
                    ..Default::default()
                })
            }
        }
    }
}

#[cfg(test)]
mod head_author_tests {
    use super::head_commit_by_bot;
    use serde_json::json;

    #[test]
    fn head_commit_author_is_read_from_the_compare() {
        let compare = json!({"commits": [
            {"sha": "a", "author": {"login": "namwork"}},
            {"sha": "b", "author": {"login": "coderhelm[bot]"}},
        ]});
        assert!(head_commit_by_bot(&compare, "b"));
        assert!(!head_commit_by_bot(&compare, "a"));
        // A commit with no linked GitHub account is not CoderHelm's.
        assert!(!head_commit_by_bot(
            &json!({"commits": [{"sha": "c", "author": null}]}),
            "c"
        ));
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::passes::review_verdict::StandingReview;
    use review_agent::{parse_reply_conclusion, VerdictChange};

    fn msg() -> ReviewMessage {
        ReviewMessage {
            team_id: "T".into(),
            installation_id: 1,
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            head_sha: "a1b2c3d4".into(),
            label: "ch-review".into(),
            question: None,
            trigger: "synchronize".into(),
            dedup_key: "head#a1b2c3d4".into(),
            reply_to_comment_id: None,
            requested_by: String::new(),
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
        }
    }

    /// A review that fails (a 520 that outlasted the client's retries, an
    /// unreadable answer) is sent again at 2, 10 and 30 minutes, then the
    /// PR is told. Nothing on this path is a verdict.
    #[test]
    fn a_failed_review_is_requeued_then_reported_never_blocking() {
        let mut m = msg();
        let mut delays = vec![];
        while let FailurePlan::Retry { delay_secs } = failure_plan(m.attempt) {
            let (next, hop) = retry_message(&m, "a1b2c3d4", delay_secs);
            assert!(hop <= SQS_MAX_DELAY_SECS);
            assert_eq!(hop + next.wait_secs, delay_secs);
            assert_eq!(next.head_sha, "a1b2c3d4", "pinned to the reviewed head");
            assert_eq!(next.attempt, m.attempt + 1);
            assert_eq!(
                next.dedup_key,
                format!("retry#{}#head#a1b2c3d4", next.attempt)
            );
            assert!(next.question.is_none());
            delays.push(delay_secs);
            m = next;
        }
        assert_eq!(failure_plan(m.attempt), FailurePlan::GiveUp);
        assert_eq!(delays, common::model_retry::REQUEUE_DELAYS_SECS.to_vec());
        assert_eq!(m.attempt, 3);
        // The final word is a plain comment that keeps the previous verdict
        // and says how to run it again.
        let note = gave_up_note(&m, "a1b2c3d4");
        assert!(note.contains("`a1b2c3d`"));
        assert!(note.contains("service error"));
        assert!(note.contains("@coderhelm re-review"));
        assert!(note.contains("previous one is unchanged"));
        assert!(!note.contains("Requesting"));
        // A failed question keeps resolving the latest head.
        let mut q = msg();
        q.head_sha.clear();
        q.question = Some("why?".into());
        q.dedup_key = "reply#9".into();
        let (next, _) = retry_message(&q, "a1b2c3d4", 120);
        assert!(next.head_sha.is_empty());
        assert_eq!(next.dedup_key, "retry#1#reply#9");
        assert!(gave_up_note(&q, "").contains("Ask again"));
    }

    /// The incident: the reply says nothing in the diff needs changing while
    /// CoderHelm's error review still requests changes → the formal review
    /// runs first, in the same job, and the reply follows it.
    #[test]
    fn a_nothing_blocking_reply_over_an_error_review_runs_the_formal_review_first() {
        use crate::passes::review_verdict::{decide, Context, Decision, Input};
        let c = parse_reply_conclusion(&serde_json::json!({
            "verdict_change": "clear",
            "reason": "The cold-start fix is in place; nothing in the diff needs changing.",
            "new_nonblocking_items": false,
            "revised_reply": ""
        }));
        assert_eq!(c.verdict_change, VerdictChange::Clear);
        let ctx = Context {
            standing: Some(StandingReview {
                verdict: "REQUEST_CHANGES".into(),
                failed: true,
                head_sha: "a1b2c3d4".into(),
            }),
            self_authored: false,
        };
        assert_eq!(
            decide(
                Input::Reply {
                    change: c.verdict_change
                },
                &ctx
            ),
            Decision::Review
        );
        let mut q = msg();
        q.question = Some("is this fine to merge?".into());
        q.reply_to_comment_id = Some(9);
        q.dedup_key = "reply#9".into();
        let job = reconcile_message(&q, "a1b2c3d4", &c.reason);
        assert!(job.question.is_none() && job.reply_to_comment_id.is_none());
        assert_eq!(job.trigger, "reconcile");
        assert_eq!(job.head_sha, "a1b2c3d4");
        assert_eq!(job.dedup_key, "reconcile#a1b2c3d4#reply#9");
        assert_eq!(job.reconcile_note.as_deref(), Some(c.reason.as_str()));
        // The reply never contradicts the formal verdict posted before it.
        let fine = "Nothing in the diff needs changing; fine to merge.";
        let after = reply_after_formal(fine, "a1b2c3d4", FormalResult::Posted(Some("APPROVE")));
        assert!(after.starts_with(fine) && after.contains("approved"));
        let blocked = reply_after_formal(
            fine,
            "a1b2c3d4",
            FormalResult::Posted(Some("REQUEST_CHANGES")),
        );
        assert!(!blocked.contains(fine) && blocked.contains("still requests changes"));
        let failed = reply_after_formal(fine, "a1b2c3d4", FormalResult::Failed);
        assert!(!failed.contains(fine) && failed.contains("retries"));
    }

    #[test]
    fn a_reply_that_drip_feeds_new_nits_is_replaced_by_its_revision() {
        let c = parse_reply_conclusion(&serde_json::json!({
            "verdict_change": "clear",
            "reason": "r",
            "new_nonblocking_items": true,
            "revised_reply": "Looks fine to merge."
        }));
        assert!(c.new_nonblocking_items);
        assert_eq!(c.revised_reply, "Looks fine to merge.");
        // Flagged without a revision: nothing to post instead, keep it.
        let c = parse_reply_conclusion(&serde_json::json!({
            "verdict_change": "none", "reason": "", "new_nonblocking_items": true, "revised_reply": " "
        }));
        assert!(!c.new_nonblocking_items);
        assert_eq!(
            parse_reply_conclusion(&serde_json::json!({})).verdict_change,
            VerdictChange::None
        );
    }

    #[test]
    fn earlier_replies_are_coderhelms_latest_comments() {
        let c = |login: &str, kind: &str, body: &str| serde_json::json!({"user": {"login": login, "type": kind}, "body": body});
        let comments = vec![
            c("coderhelm[bot]", "Bot", "first"),
            c("dev", "User", "question"),
            c("coderhelm[bot]", "Bot", "second"),
            c("coderhelm[bot]", "Bot", "third"),
        ];
        assert_eq!(own_comments(&comments, 2), vec!["second", "third"]);
    }
}

#[cfg(test)]
mod reply_prompt_tests {
    /// The reply knows it can approve (the incident's reply said it only had
    /// read access, so merging "stays with you after approval").
    #[test]
    fn the_reply_prompt_states_what_coderhelm_can_do() {
        let p = super::reply_system_prompt("o", "r", "", "", "");
        assert!(p.contains(super::review_agent::CAPABILITIES_RULE));
        assert!(p.contains("approve or request changes"));
        assert!(p.contains("never tell a person you can't approve"));
        assert!(p.contains("auto-merge policy"));
        assert!(p.contains("test something by hand is a note"));
    }

    /// The team's instructions ride with the org standards into both the
    /// reply and the formal review (`instructions_block`), for any PR.
    #[test]
    fn the_reviewer_prompt_carries_the_teams_instructions_when_set() {
        let team = "Every test uses the fixtures helper in tests/support.";
        let org = super::review_agent::team_rules_block(team);
        let p = super::reply_system_prompt("acme", "web", "", &org, "");
        assert!(p.contains("## Your team's instructions"));
        assert!(p.contains(team));
        let none = super::reply_system_prompt(
            "acme",
            "web",
            "",
            &super::review_agent::team_rules_block(""),
            "",
        );
        assert!(!none.contains("team's instructions"));
    }
}
