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

/// Whether CoderHelm may add `label` to a pull request in this repo by
/// itself: the reviewer is on with label picking, and the team's settings
/// allow the label by name or family (`review_labels::LabelRules`). Labels
/// the repo's docs merely mention are never added this way, and neither are
/// deploy, staging or production labels: a person adds those.
pub(crate) async fn may_add_label(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    label: &str,
) -> bool {
    // Deploy labels cost a shared environment, and the team's label rules
    // decide when one is worth it (the reviewer weighs them per change):
    // never added just because a workflow asks for one.
    // A pattern or a bare prefix (`E2E:`) is never a label: an `E2E:*`
    // family allows `E2E:nav`, not `E2E:` itself.
    if label.contains('*')
        || common::e2e::workflow_gate::is_bare_prefix(label)
        || common::e2e::workflow_gate::is_deploy_label(label)
    {
        return false;
    }
    load_config(state, team_id, owner, name)
        .await
        .is_ok_and(|cfg| {
            cfg.enabled && !cfg.killed && cfg.auto_labels && cfg.label_rules.allowed(label)
        })
}

/// The labels the team's settings name exactly (not as a family), for
/// choosing one label among several a workflow condition matches. Empty
/// when CoderHelm may not add labels in this repo.
pub(crate) async fn named_labels(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Vec<String> {
    load_config(state, team_id, owner, name)
        .await
        .ok()
        .filter(|cfg| cfg.enabled && !cfg.killed && cfg.auto_labels)
        .map(|cfg| {
            cfg.label_rules
                .allow
                .into_iter()
                .filter(|l| !l.contains('*'))
                .collect()
        })
        .unwrap_or_default()
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

/// The review's last line: rate it (one click on the PR's review page, where
/// the rating reaches the reviewer's learning) or talk to it.
pub(crate) fn rating_footer(app: &str, owner: &str, name: &str, pr: u64) -> String {
    format!(
        "\n\n---\n**Was this review right?** [Rate it 👍 / 👎]({app}/reviewer/pr?repo={owner}/{name}&pr={pr}) \
         so the reviewer learns · reply `@coderhelm re-review` or `@coderhelm <question>`"
    )
}

/// How long a processed review trigger stays claimed (consume-once dedup). Long
/// enough to outlast any duplicate webhook / SQS redelivery of the SAME trigger
/// (seconds–minutes apart), short enough that a review which errored out can be
/// re-driven for the same head once it expires. Keys that are unique per action
/// (a comment id) are unaffected by the length; head-based keys get this window.
const REVIEW_JOB_DEDUP_TTL_SECS: u64 = 6 * 3600;

/// How long a person's re-review ask holds the per-commit review claim: the
/// gateway's window for push reviews, so the two keep each other out alike.
const REVIEW_HEAD_CLAIM_SECS: u64 = 600;

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
    /// GitHub's rate limit: send the job again once it resets, in this many
    /// seconds. Spends no attempt.
    WaitForLimit { delay_secs: u64 },
    /// GitHub's limit held through every wait: the job ends without a note
    /// on the PR (nothing failed on CoderHelm's side); the next push or a
    /// re-review asks again.
    LimitOutlasted,
}

/// Waits for GitHub's rate limit one review job takes (a day of hourly
/// limits) before it ends quietly.
pub(crate) const MAX_LIMIT_WAITS: u32 = 24;

/// The plan for a review that failed on run `attempt` (0 = the first run).
pub(crate) fn failure_plan(attempt: u32) -> FailurePlan {
    match common::model_retry::requeue_delay(attempt) {
        Some(delay_secs) => FailurePlan::Retry { delay_secs },
        None => FailurePlan::GiveUp,
    }
}

/// The plan for a review that failed, given whether GitHub's rate limit was
/// the cause (`limit_wait`: seconds until it resets). A rate limit waits for
/// the reset and never spends an attempt; anything else follows
/// [`failure_plan`].
pub(crate) fn failure_plan_for(msg: &ReviewMessage, limit_wait: Option<u64>) -> FailurePlan {
    match limit_wait {
        Some(_) if msg.limit_waits >= MAX_LIMIT_WAITS => FailurePlan::LimitOutlasted,
        Some(wait) => FailurePlan::WaitForLimit {
            delay_secs: wait.max(1) + common::github_wait::RESET_SLACK_SECS,
        },
        None => failure_plan(msg.attempt),
    }
}

/// How long GitHub's rate limit holds for this failed review, when it was
/// the cause: the error says so, or the installation is limited right now.
pub(crate) fn limit_wait_of(
    e: &(dyn std::error::Error + Send + Sync + 'static),
    installation_id: u64,
) -> Option<u64> {
    use crate::clients::github;
    use common::github_wait::Cause;
    match github::outside_wait(e) {
        Some((Cause::RateLimit, wait)) => Some(wait),
        Some((Cause::Outage, _)) => None,
        None => github::outside_for(installation_id)
            .filter(|(c, _)| *c == Cause::RateLimit)
            .map(|(_, secs)| secs),
    }
}

/// The trigger's own dedup key, without any retry's or wait's prefix.
fn base_dedup_key(msg: &ReviewMessage, reviewed_head: &str) -> String {
    let mut key = msg.dedup_key.as_str();
    loop {
        let rest = key
            .strip_prefix("retry#")
            .or_else(|| key.strip_prefix("limit#"));
        match rest {
            Some(rest) => key = rest.split_once('#').map(|(_, k)| k).unwrap_or(rest),
            None => break,
        }
    }
    if key.is_empty() {
        format!("head#{reviewed_head}")
    } else {
        key.to_string()
    }
}

/// The job that waits for GitHub's rate limit to reset, and the delay of its
/// first SQS hop. It keeps its attempt count, and has its own dedup key.
pub(crate) fn limit_wait_message(
    msg: &ReviewMessage,
    reviewed_head: &str,
    delay_secs: u64,
) -> (ReviewMessage, u64) {
    let mut next = msg.clone();
    next.limit_waits = msg.limit_waits + 1;
    if next.question.is_none() && !reviewed_head.is_empty() {
        next.head_sha = reviewed_head.to_string();
    }
    next.dedup_key = format!(
        "limit#{}#{}",
        next.limit_waits,
        base_dedup_key(msg, reviewed_head)
    );
    let hop = delay_secs.min(SQS_MAX_DELAY_SECS);
    next.wait_secs = delay_secs - hop;
    (next, hop)
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
    let base = base_dedup_key(msg, reviewed_head);
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

use super::review_session::NotReviewed;

/// A review job as the dashboard sees it (`common::review_live`): its id, and
/// when it started. Only this job may end the status it set. It also keeps
/// the job's session (`common::review_session`) and the PR's "CoderHelm
/// review" check run (`review_session`), which it completes when it ends.
pub(crate) struct LiveJob {
    pub id: String,
    pub started_at: i64,
    /// What this job showed and decided so far.
    session: std::sync::Mutex<Session>,
}

/// What a job's session knows besides the stored record.
#[derive(Default)]
struct Session {
    /// The check run it holds, and the commit it is on.
    check: Option<(u64, String)>,
    /// The session record is open (its page exists).
    opened: bool,
    /// The verdict, its finding count and the posted review's link.
    verdict: Option<(&'static str, usize, String)>,
    /// Why the job ended without a verdict, when it said so.
    not_posted: Option<String>,
    /// How its check ends when the job ends without a new review: the
    /// conclusion and title (the summary is the job's reason).
    settled: Option<(&'static str, String)>,
    /// Ending without a new verdict gives the commit back the check it showed
    /// before this job: a person's comment, or a re-review with nothing
    /// changed, says nothing about the verdict.
    restore: bool,
}

/// The key a job's check is held under: the job's dedup key (what the
/// gateway queued the check for), or the job's own id.
fn check_key<'a>(msg: &'a ReviewMessage, job: &'a str) -> &'a str {
    if msg.dedup_key.is_empty() {
        job
    } else {
        &msg.dedup_key
    }
}

fn check_target<'a>(
    msg: &'a ReviewMessage,
    head: &'a str,
) -> super::review_session::CheckTarget<'a> {
    super::review_session::CheckTarget {
        installation: msg.installation_id,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        pr: msg.pr_number,
        head,
    }
}

/// The commit carrying the job's check: the one it is pinned to, or the one the
/// gateway queued its check on.
fn check_head(msg: &ReviewMessage) -> &str {
    if msg.head_sha.is_empty() {
        &msg.check_head
    } else {
        &msg.head_sha
    }
}

/// What the job does, in the words of its check.
fn check_work(msg: &ReviewMessage) -> common::review_check::Work {
    common::review_check::Work::of(&msg.trigger, msg.question.is_some())
}

fn pr_page(state: &WorkerState, msg: &ReviewMessage) -> String {
    common::review_check::pr_page(
        common::public_urls::app_url(&state.config.stage),
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
}

/// The check waiting for job `msg` ends before the job took it over (the
/// reviewer is off, say). Nothing when no check waits for it.
async fn close_waiting_check(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReviewMessage,
    title: &str,
    summary: &str,
) {
    if msg.dedup_key.is_empty() {
        return;
    }
    let close = common::review_check::Close {
        conclusion: "neutral",
        title: title.to_string(),
        summary: summary.to_string(),
        details_url: Some(pr_page(state, msg)),
    };
    common::review_check::close_waiting(
        github,
        &check_target(msg, check_head(msg)),
        &msg.dedup_key,
        &close,
    )
    .await;
}

impl LiveJob {
    pub(crate) fn start() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            started_at: chrono::Utc::now().timestamp(),
            session: Default::default(),
        }
    }

    /// The job's session id, once its session page exists.
    fn session_id(&self) -> Option<String> {
        self.session().opened.then(|| self.id.clone())
    }

    fn session(&self) -> std::sync::MutexGuard<'_, Session> {
        self.session.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The PR is being reviewed by this job now: its status, and its session
    /// with the first step.
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
        let short = &head[..head.len().min(7)];
        let first = if msg.question.is_some() {
            format!("Started reading a comment on the pull request at `{short}`")
        } else {
            format!("Started reviewing `{short}`")
        };
        common::review_session::open(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &common::review_session::Open {
                job: &self.id,
                owner: &msg.repo_owner,
                repo: &msg.repo_name,
                pr: msg.pr_number,
                head_sha: head,
                title,
                trigger: &msg.trigger,
                is_question: msg.question.is_some(),
                attempt: msg.attempt,
                budget_secs: REVIEW_LEASE_SECS as i64,
            },
            &first,
        )
        .await;
        self.session().opened = true;
    }

    /// One more step in the session. `detail` may be empty.
    async fn step(&self, state: &WorkerState, msg: &ReviewMessage, text: &str, detail: &str) {
        common::review_session::step(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &self.id,
            text,
            detail,
        )
        .await;
    }

    /// The job starts: take over the check the gateway queued for it on its
    /// commit, in progress at once. Nothing when no check waits for this job,
    /// or a reply the worker hasn't yet found is for CoderHelm.
    async fn start_check(&self, state: &WorkerState, github: &GitHubClient, msg: &ReviewMessage) {
        let head = check_head(msg);
        if head.is_empty() || self.session().check.is_some() {
            return;
        }
        let Some(id) = common::review_check::start(
            github,
            &check_target(msg, head),
            &msg.dedup_key,
            &common::review_check::started_title(check_work(msg), head),
            &pr_page(state, msg),
        )
        .await
        else {
            return;
        };
        let mut s = self.session();
        s.check = Some((id, head.to_string()));
        s.restore = msg.question.is_some();
        if msg.question.is_some() {
            s.settled = Some(super::review_session::not_reviewed(NotReviewed::Answered {
                head,
            }));
        }
    }

    /// The job knows the PR's head now. A check queued on a commit the PR has
    /// left goes back to what it showed. A person's ask with no check on the
    /// head (a reply only the worker could tell was for CoderHelm, a head that
    /// moved, a gateway that couldn't reach GitHub) shows it now.
    async fn follow_head(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        msg: &ReviewMessage,
        head: &str,
    ) {
        let held = self.session().check.clone();
        if let Some((id, old)) = held {
            if old == head {
                return;
            }
            let close = common::review_check::Close {
                conclusion: "neutral",
                title: format!(
                    "Replaced by the review of {}",
                    common::review_check::short(head)
                ),
                summary: format!(
                    "The pull request moved on to {} before this finished.",
                    common::review_check::short(head)
                ),
                details_url: Some(pr_page(state, msg)),
            };
            common::review_check::close_restoring(github, &check_target(msg, &old), id, &close)
                .await;
            self.session().check = None;
        }
        if msg.head_sha.is_empty() && !msg.dedup_key.is_empty() && !head.is_empty() {
            let work = check_work(msg);
            let target = check_target(msg, head);
            let details = pr_page(state, msg);
            let now = chrono::Utc::now().timestamp();
            common::review_check::queue_work(github, &target, work, &msg.dedup_key, now, &details)
                .await;
            let mut with_head = msg.clone();
            with_head.check_head = head.to_string();
            self.start_check(state, github, &with_head).await;
        }
    }

    /// The session page exists now: the check links it, so the person can
    /// watch what CoderHelm is doing.
    async fn link_session(&self, state: &WorkerState, github: &GitHubClient, msg: &ReviewMessage) {
        let held = self.session().check.clone();
        let Some((id, head)) = held else {
            return;
        };
        let title = common::review_check::started_title(check_work(msg), &head);
        let key = check_key(msg, &self.id).to_string();
        self.say_on_check(state, github, msg, id, &head, &key, &title)
            .await;
    }

    /// The check says what the job does now, linking its session.
    #[allow(clippy::too_many_arguments)]
    async fn say_on_check(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        msg: &ReviewMessage,
        id: u64,
        head: &str,
        key: &str,
        title: &str,
    ) {
        let details = super::review_session::session_url(
            common::public_urls::app_url(&state.config.stage),
            &self.id,
        );
        common::review_check::progress(github, &check_target(msg, head), id, key, title, &details)
            .await;
    }

    /// The reply is being answered.
    async fn answering(&self, state: &WorkerState, github: &GitHubClient, msg: &ReviewMessage) {
        let held = self.session().check.clone();
        if let Some((id, head)) = held {
            let title = format!(
                "Answering your reply on {}",
                common::review_check::short(&head)
            );
            let key = check_key(msg, &self.id).to_string();
            self.say_on_check(state, github, msg, id, &head, &key, &title)
                .await;
        }
    }

    /// The job answered a person and the verdict stands: its check goes back
    /// to the verdict the commit showed.
    fn answered(&self, head: &str, why: &str) {
        let mut s = self.session();
        s.settled = Some(super::review_session::not_reviewed(NotReviewed::Answered {
            head,
        }));
        s.not_posted = Some(why.to_string());
        s.restore = true;
    }

    /// How the check ends if the job ends without a new review: `conclusion`
    /// and `title`, the job's reason as its summary.
    fn settle(&self, why: super::review_session::NotReviewed<'_>) {
        let mut s = self.session();
        s.settled = Some(super::review_session::not_reviewed(why));
        s.restore = false;
    }

    /// The job hands its review to job `next_key`, due at `due`: its check
    /// goes with it, queued again and saying why. False when it holds none
    /// or GitHub refused (the check is then completed when this job ends).
    async fn hand_off_check(
        &self,
        state: &WorkerState,
        msg: &ReviewMessage,
        next_key: &str,
        due: i64,
        title: &str,
        summary: &str,
    ) -> bool {
        let Some((id, head)) = self.session().check.clone() else {
            return false;
        };
        let github = match GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            Ok(g) => g,
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not hand the CoderHelm review check on");
                return false;
            }
        };
        let handed = common::review_check::hand_off(
            &github,
            &check_target(msg, &head),
            id,
            next_key,
            due,
            title,
            summary,
        )
        .await;
        if handed {
            self.session().check = None;
        }
        handed
    }

    /// A formal review starts: the "CoderHelm review" check on `head` shows
    /// it in progress, linking to this session: the check this job already
    /// holds, the one open on the commit, or a new one. One check per
    /// commit; a refusal (no checks:write) leaves the review exactly as it
    /// was.
    async fn open_check(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        msg: &ReviewMessage,
        head: &str,
        files: &[String],
    ) {
        let details = super::review_session::session_url(
            common::public_urls::app_url(&state.config.stage),
            &self.id,
        );
        let target = check_target(msg, head);
        let key = check_key(msg, &self.id);
        let title = super::review_session::reading_title(files, head);
        let held = self.session().check.clone();
        if let Some((id, held_head)) = held {
            if held_head == head {
                common::review_check::progress(github, &target, id, key, &title, &details).await;
                return;
            }
        }
        let Some(id) =
            common::review_check::take_over_or_open(github, &target, key, &title, &details).await
        else {
            return;
        };
        self.session().check = Some((id, head.to_string()));
        common::review_session::set_check_run(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &self.id,
            id,
        )
        .await;
    }

    /// The review's verdict, and the posted review (empty when nothing new
    /// was posted).
    async fn verdict(
        &self,
        state: &WorkerState,
        msg: &ReviewMessage,
        verdict: &'static str,
        findings: usize,
        review_url: &str,
    ) {
        self.session().verdict = Some((verdict, findings, review_url.to_string()));
        common::review_session::note_verdict(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &self.id,
            &common::review_session::Verdict {
                verdict,
                findings,
                review_url,
            },
        )
        .await;
    }

    /// The job ends without a verdict on purpose: why, for its check and
    /// session.
    fn not_posted(&self, reason: &str) {
        self.session().not_posted = Some(reason.to_string());
    }

    /// Record how the job ended: its session, its check (never left in
    /// progress), and the PR's status.
    async fn finished(
        &self,
        state: &WorkerState,
        msg: &ReviewMessage,
        how: common::review_live::Finished<'_>,
    ) {
        use super::review_session::CheckEnd;
        use common::review_live::Finished;
        use common::review_session as rs;
        let (check, verdict, not_posted, settled, opened, restore) = {
            let s = self.session();
            (
                s.check.clone(),
                s.verdict.clone(),
                s.not_posted.clone(),
                s.settled.clone(),
                s.opened,
                s.restore,
            )
        };
        // The session's status and last step, and the check's ending.
        let (status, reason, last, mut end) = match &how {
            Finished::Done => match (&verdict, &not_posted) {
                (Some((v, findings, url)), _) => (
                    rs::STATUS_DONE,
                    None,
                    "Finished".to_string(),
                    CheckEnd::Reviewed {
                        verdict: v,
                        findings: *findings,
                        review_url: url,
                    },
                ),
                (None, Some(why)) => (
                    rs::STATUS_SKIPPED,
                    Some(why.as_str()),
                    format!("Finished without a verdict: {why}"),
                    CheckEnd::Skipped { reason: why },
                ),
                (None, None) => (
                    rs::STATUS_DONE,
                    None,
                    "Finished".to_string(),
                    CheckEnd::Skipped {
                        reason: "Finished without a verdict for this commit.",
                    },
                ),
            },
            Finished::Skipped { reason } => (
                rs::STATUS_SKIPPED,
                Some(*reason),
                format!("Not reviewed: {reason}"),
                CheckEnd::Skipped { reason },
            ),
            Finished::Retrying { reason, .. } => (
                rs::STATUS_RETRYING,
                Some(*reason),
                "Didn't finish because of a service error; sent again".to_string(),
                CheckEnd::Retrying { reason },
            ),
            Finished::Failed { reason } => (
                rs::STATUS_FAILED,
                Some(*reason),
                "Didn't finish: every try hit a service error".to_string(),
                CheckEnd::Failed { reason },
            ),
        };
        // Every job ends here, and says how in one line: a verdict, a skip
        // with its reason, a retry or a failure. A job once ended between
        // "Reviewing PR from label" and its first step with no line at all.
        let (repo_slug, head) = (
            format!("{}/{}", msg.repo_owner, msg.repo_name),
            check_head(msg).to_string(),
        );
        match &how {
            Finished::Failed { .. } | Finished::Retrying { .. } => warn!(
                team_id = %msg.team_id, repo = %repo_slug, pr = msg.pr_number, head = %head,
                trigger = %msg.trigger, dedup_key = %msg.dedup_key, job = %self.id,
                outcome = status, ending = %last, "review_job_ended"
            ),
            _ => info!(
                team_id = %msg.team_id, repo = %repo_slug, pr = msg.pr_number, head = %head,
                trigger = %msg.trigger, dedup_key = %msg.dedup_key, job = %self.id,
                outcome = status, ending = %last, "review_job_ended"
            ),
        }
        // An ending without a new review that the job named itself (a
        // standing verdict carried, a newer commit's review).
        if let (CheckEnd::Skipped { reason }, Some((conclusion, title))) = (end, &settled) {
            end = CheckEnd::Settled {
                conclusion,
                title,
                reason,
            };
        }
        rs::close(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &self.id,
            status,
            reason,
            &last,
        )
        .await;
        if let Some((id, head)) = check {
            // A job that stopped before its session opened links the PR's
            // reviews page instead of a session page that doesn't exist.
            let pr_reviews = pr_page(state, msg);
            let details = if opened {
                super::review_session::session_url(
                    common::public_urls::app_url(&state.config.stage),
                    &self.id,
                )
            } else {
                pr_reviews.clone()
            };
            let mut close = super::review_session::check_close(
                end,
                &format!(
                    "https://github.com/{}/{}/pull/{}",
                    msg.repo_owner, msg.repo_name, msg.pr_number
                ),
                &details,
            );
            if !opened {
                close.details_url = Some(pr_reviews);
            }
            let target = check_target(msg, &head);
            match GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                msg.installation_id,
                &state.http,
            ) {
                // No new verdict from a job that only answered: the commit
                // shows the one that stands, and the job's link.
                Ok(github) if restore && verdict.is_none() => {
                    common::review_check::close_restoring(&github, &target, id, &close).await
                }
                Ok(github) => common::review_check::close(&github, &target, id, &close).await,
                Err(e) => {
                    warn!(pr = msg.pr_number, error = %e, "Could not complete the CoderHelm review check")
                }
            }
        }
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
        info!(pr = msg.pr_number, trigger = %msg.trigger, wait_secs = next.wait_secs, hop, "Review job waits on: sent on for its next hop");
        return Ok(());
    }
    let job = LiveJob::start();
    let mut reviewed_head = msg.head_sha.clone();
    match run_budgeted(state, msg.clone(), &mut reviewed_head, &job).await {
        Ok(Outcome::Done { posted, .. }) => {
            job.finished(state, &msg, common::review_live::Finished::Done)
                .await;
            // A run's hand-off (or the ready gate's ask) gave the verdict
            // the draft decision waits for: decide it now, not at the next
            // sweep.
            if posted.is_some() && msg.trigger == super::review_scope::HANDOFF_TRIGGER {
                super::ready::request(state, &msg).await;
            }
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
                    let limit = limit_wait_of(&*e, review.installation_id);
                    review_failed(state, &review, &head, &e.to_string(), limit, &job).await;
                    FormalResult::Failed
                }
            };
            let out = post_reply(state, &msg, &job, *reply, formal).await;
            // A failed formal review already shows its retry.
            if formal != FormalResult::Failed {
                job.finished(state, &msg, common::review_live::Finished::Done)
                    .await;
            }
            out
        }
        Err(e) => {
            let limit = limit_wait_of(&*e, msg.installation_id);
            review_failed(state, &msg, &reviewed_head, &e.to_string(), limit, &job).await;
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
    job: &LiveJob,
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
        job,
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
    job: &LiveJob,
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
        job,
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
    record_review_usage(state, &msg.team_id, &record_sk, usage).await;
    note_review_standards(state, &msg.team_id, &record_sk, applied_standards).await;
    info!(pr = msg.pr_number, "Reviewer answered a question");
    Ok(())
}

/// A review didn't finish: log it (for the failed-review metric), then send
/// it again or, once every retry failed, leave a note on the PR. Never a
/// REQUEST_CHANGES, and never touches an earlier verdict. GitHub's rate
/// limit (`limit_wait`: seconds until it resets) is not a failure of the
/// review: the job waits for the reset, spends no attempt, and never ends in
/// the gave-up note.
async fn review_failed(
    state: &WorkerState,
    msg: &ReviewMessage,
    head: &str,
    reason: &str,
    limit_wait: Option<u64>,
    job: &LiveJob,
) {
    match failure_plan_for(msg, limit_wait) {
        FailurePlan::WaitForLimit { delay_secs } => {
            let (next, hop) = limit_wait_message(msg, head, delay_secs);
            if enqueue_review(state, &next, hop).await {
                let due = chrono::Utc::now().timestamp() + delay_secs as i64;
                let text = common::github_wait::waiting_text(
                    common::github_wait::Cause::RateLimit,
                    due.max(0) as u64,
                );
                let (title, summary) = super::review_session::limit_wait(&text);
                job.hand_off_check(state, msg, &next.dedup_key, due, &title, &summary)
                    .await;
                job.finished(
                    state,
                    msg,
                    common::review_live::Finished::Retrying {
                        reason: &text,
                        retry_in_secs: delay_secs as i64,
                        budget_secs: REVIEW_LEASE_SECS as i64,
                    },
                )
                .await;
                info!(
                    pr = msg.pr_number,
                    waits = next.limit_waits,
                    delay_secs,
                    "review_limit_wait: the review waits for GitHub's rate limit to reset"
                );
                return;
            }
            // The queue refused: end like an outlasted limit, quietly.
        }
        FailurePlan::LimitOutlasted => {}
        FailurePlan::Retry { .. } | FailurePlan::GiveUp => {
            return review_failed_for_service(state, msg, head, reason, job).await;
        }
    }
    // GitHub's limit: no note on the PR, and the failed-review metric is not
    // counted (nothing failed on CoderHelm's side).
    warn!(
        pr = msg.pr_number,
        waits = msg.limit_waits,
        "review_limit_outlasted: GitHub's rate limit held; the review ends without a note"
    );
    let text = format!(
        "GitHub's rate limit for this installation held through {} waits; the review runs again \
         on the next push, or comment @coderhelm re-review",
        msg.limit_waits
    );
    job.finished(
        state,
        msg,
        common::review_live::Finished::Failed { reason: &text },
    )
    .await;
}

/// A review that didn't finish for a service error (not GitHub's rate
/// limit): sent again by [`failure_plan`], then the gave-up note.
async fn review_failed_for_service(
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
            // The commit's check waits for the retry instead of ending.
            let due = chrono::Utc::now().timestamp() + delay_secs as i64;
            let (title, summary) = super::review_session::retry_wait(reason, due);
            job.hand_off_check(state, msg, &next.dedup_key, due, &title, &summary)
                .await;
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
        // A check queued before the reviewer was turned off doesn't wait on.
        if let Ok(github) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            msg.installation_id,
            &state.http,
        ) {
            close_waiting_check(
                state,
                &github,
                &msg,
                "Not reviewed: the reviewer is off for this repository",
                "The reviewer was turned off for this repository before this review started, \
                 so there is no new verdict for this commit.",
            )
            .await;
        }
        live_job.not_posted("the reviewer is off for this repository");
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
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // A reply that may answer one of CoderHelm's findings, mention or not:
    // what it answers, checked before the job is claimed so a reply in a
    // person's own thread that didn't address CoderHelm leaves no claim
    // (an edit that adds the mention is then handled).
    let answered = if super::review_thread_reply::is_answer_trigger(&msg.trigger) {
        match super::review_thread_reply::find(&github, &msg).await? {
            Some(f) => Some(f),
            None if msg.mentioned => None,
            None => {
                info!(
                    pr = msg.pr_number,
                    trigger = %msg.trigger,
                    reason = "it doesn't answer CoderHelm's review and doesn't address CoderHelm",
                    "Reviewer: reply not handled"
                );
                live_job.not_posted(
                    "the reply doesn't answer CoderHelm's review and doesn't address CoderHelm",
                );
                return Ok(DONE);
            }
        }
    } else {
        None
    };

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
            // A redelivered webhook may have queued a check for it again; the
            // copy that is running keeps its own.
            close_waiting_check(
                state,
                &github,
                &msg,
                "Already handled",
                "A repeat delivery of something CoderHelm already handled.",
            )
            .await;
            live_job.not_posted("a repeat delivery of a job CoderHelm already handled");
            return Ok(DONE);
        }
    }

    // The commit's check, queued when the review was scheduled, shows the
    // job in progress from its first step; every ending below completes it.
    live_job.start_check(state, &github, &msg).await;

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
        let newer = common::review_check::short(current_head);
        live_job.not_posted(&format!(
            "A newer commit ({newer}) was pushed before this review started; the review of \
             {newer} covers the latest code."
        ));
        live_job.settle(NotReviewed::Superseded {
            newer: current_head,
        });
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
    live_job.follow_head(state, &github, &msg, &head_sha).await;

    // CoderHelm's own push while its run is still fixing the PR: the run
    // asks for the verdict itself when CI is green, and hands the PR over
    // for review if it ends without one. A person's push is reviewed.
    if msg.trigger == super::review_scope::TRIGGER_PUSH && super::is_own_login(pr_author) {
        if let Some(reason) = own_loop_hold(state, &github, &msg, &head_sha).await {
            info!(pr = msg.pr_number, head = %head_sha, "CoderHelm's own push during its run's fix loop — not reviewed now");
            // The gateway took this head's review claim when it queued the
            // push. Nothing reviews the head now, so the claim goes: a run's
            // hand-off of this same head must not read it as "already being
            // reviewed".
            common::claim::release(
                &state.dynamo,
                &state.config.settings_table_name,
                &msg.team_id,
                &super::review_policy::head_claim_sk(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &head_sha,
                ),
            )
            .await;
            live_job.settle(NotReviewed::OwnPushDuringRun);
            return Ok(Outcome::Skipped { reason });
        }
    }

    // A review started by a description edit reads the description as it is
    // now. When later edits left text this commit was already reviewed with
    // (an edit after the queued review read it queues its own), it doesn't
    // run again; the text it reviews is marked so the same text isn't.
    if msg.trigger == common::review_description::TRIGGER {
        if let Some(fresh) = description_to_mark(&msg, pr_body) {
            let sk = common::review_description::seen_sk(
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
                &head_sha,
                &fresh,
            );
            let claim = common::claim::claim(
                &state.dynamo,
                &state.config.settings_table_name,
                &msg.team_id,
                &sk,
                common::review_description::SEEN_TTL_SECS,
            )
            .await;
            if claim == common::claim::Claim::Held {
                info!(pr = msg.pr_number, head = %head_sha, "Description already reviewed at this commit — not reviewed again");
                live_job.settle(NotReviewed::DescriptionSeen { head: &head_sha });
                return Ok(Outcome::Skipped {
                    reason: "This description was already reviewed at this commit.".into(),
                });
            }
        }
    }

    // One review per head commit: a push, label or ready event for a commit
    // CoderHelm already reviewed returns that verdict, with no model work.
    let repo_slug = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let records =
        super::review_sweep::review_records(state, &msg.team_id, &repo_slug, msg.pr_number, 10)
            .await;
    if msg.question.is_none() {
        if let super::review_policy::Start::ReturnExisting { verdict } = super::review_policy::start(
            &msg.trigger,
            &head_sha,
            &super::review_fingerprint::description_part(pr_body),
            &records,
        ) {
            info!(pr = msg.pr_number, head = %head_sha, trigger = %msg.trigger, verdict = %verdict, "Commit already reviewed — its verdict stands, not reviewed again");
            live_job.settle(NotReviewed::AlreadyReviewed {
                head: &head_sha,
                verdict: &verdict,
            });
            return Ok(Outcome::Skipped {
                reason: format!(
                    "Already reviewed this commit: the verdict ({}) stands.",
                    verdict.to_lowercase().replace('_', " ")
                ),
            });
        }
        if super::review_policy::start(
            &msg.trigger,
            &head_sha,
            &super::review_fingerprint::description_part(pr_body),
            &records,
        ) == super::review_policy::Start::DescriptionChanged
        {
            info!(pr = msg.pr_number, head = %head_sha, trigger = %msg.trigger, "Commit already reviewed, but its description changed — reviewed as a description edit");
            msg.trigger = common::review_description::TRIGGER.to_string();
        }
    }

    // A run's hand-off reviews its pull request, draft or not. It once
    // skipped a draft until it left draft, while the ready gate kept the
    // draft until CoderHelm's review approved the head: each waited on the
    // other and the pull request was held for good. A person's draft still
    // waits for its ready-for-review event (the gateway's rule); this is
    // CoderHelm's own finished work.
    //
    // A run's hand-off and the PR's ready-for-review event can ask for the
    // same commit together: the hand-off takes the per-commit claim the
    // gateway's push and ready reviews take. When another review holds it,
    // the hand-off waits for that review and looks again (its verdict then
    // stands, `review_policy::start`); after the last wait it reviews, so a
    // claim whose review never came can't stop it.
    if msg.trigger == super::review_scope::HANDOFF_TRIGGER && !head_sha.is_empty() {
        let sk = super::review_policy::head_claim_sk(
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &head_sha,
        );
        if common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &sk,
            REVIEW_HEAD_CLAIM_SECS,
        )
        .await
            == common::claim::Claim::Held
        {
            if let Some(later) = super::review_policy::deferred_ask(&msg) {
                if enqueue_review(state, &later, super::review_policy::ASK_WAIT_SECS).await {
                    note_queued(state, &later, super::review_policy::ASK_WAIT_SECS).await;
                    info!(pr = msg.pr_number, head = %head_sha, dedup_key = %later.dedup_key, "Hand-off review waits for the running review of this commit, then looks again");
                    live_job.settle(NotReviewed::HeldByAnotherReview { head: &head_sha });
                    return Ok(Outcome::Skipped {
                        reason: "This commit is already being reviewed; the hand-off looks again \
                                 after that review."
                            .into(),
                    });
                }
            }
            info!(pr = msg.pr_number, head = %head_sha, "Hand-off review runs now (waited enough, or couldn't wait)");
        }
    }

    // Real work starts here: the dashboard shows the PR as being reviewed.
    live_job.reviewing(state, &msg, &head_sha, title).await;
    live_job.link_session(state, &github, &msg).await;

    // Build the diff (base...head compare gives per-file patches), bounded.
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, base, &head_sha)
        .await?;
    let diff = review_agent::format_diff(&compare, 40_000);
    // The changed files in the order GitHub lists them, for the session.
    let files_read: Vec<String> = compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    live_job
        .step(
            state,
            &msg,
            &format!(
                "Read the change: {} file{} changed",
                files_read.len(),
                if files_read.len() == 1 { "" } else { "s" }
            ),
            &files_read
                .iter()
                .take(20)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .await;

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
        Ok(p) => p
            .for_run(
                "",
                &format!("{}/{}", msg.repo_owner, msg.repo_name),
                "",
                &format!("Review of pull request #{}", msg.pr_number),
            )
            .in_thread(&format!(
                "review:{}/{}#{}",
                msg.repo_owner, msg.repo_name, msg.pr_number
            )),
        Err(e) => {
            tracing::error!(team_id = %msg.team_id, pr = msg.pr_number, error = %e, "Review skipped: no usable model provider");
            live_job.not_posted(
                "No model provider is set up for this team (Settings), so the review couldn't \
                 run; any earlier verdict stands.",
            );
            live_job.settle(NotReviewed::NoModelProvider);
            return Ok(DONE);
        }
    };
    let mut usage = TokenUsage::default();

    let self_authored = pr_author.contains("coderhelm");
    let standing = super::review_sweep::standing_review(&records);
    // Where a re-review ask made in a thread is answered, if it is answered
    // from the standing verdict.
    let asked_in = msg.reply_to_comment_id;

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
            msg.outstanding_work.as_deref(),
            &mut usage,
        )
        .await;
        let route = review_agent::route_ask(kind, self_authored);
        info!(pr = msg.pr_number, kind = ?kind, route = ?route, "Reviewer ask classified");
        match route {
            review_agent::AskRoute::FormalReview => {
                msg.question = None;
                msg.reply_to_comment_id = None;
                live_job
                    .step(
                        state,
                        &msg,
                        "Read the comment: it asks for a review, so a full review runs",
                        "",
                    )
                    .await;
            }
            review_agent::AskRoute::FixLoop => {
                // CoderHelm's own PR: the fix loop changes the code and answers
                // the person on the PR. With no run to hand it to, the reviewer
                // answers instead, so the person is never left unanswered.
                if forward_ask_to_fix_loop(state, &msg, &comment).await {
                    live_job.not_posted(
                        "the ask went to the run's fix loop, which answers on the pull request",
                    );
                    return Ok(DONE);
                }
            }
            review_agent::AskRoute::Reply => {}
        }
    }

    // ── Reply-with-a-question mode: answer, don't vote. ──
    if let Some(question) = msg.question.as_ref().filter(|q| !q.trim().is_empty()) {
        live_job.answering(state, &github, &msg).await;
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
        // A person answering one of CoderHelm's findings: the finding, the
        // thread so far and the answer, instead of a bare question.
        let asked = match &answered {
            Some(f) => super::review_thread_reply::answer_prompt(f, &msg.requested_by, question),
            None => question.clone(),
        };
        let question = &asked;
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
        let mcp = super::remote_mcp::RemoteMcp::load(
            state,
            &msg.team_id,
            None,
            format!(
                "reviewer, {}/{}#{}",
                msg.repo_owner, msg.repo_name, msg.pr_number
            ),
        )
        .await;
        let system = match &mcp {
            Some(m) => format!("{system}{}", m.prompt_section()),
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
        let knowledge =
            super::knowledge::KnowledgeTools::load(state, &msg.team_id, "reviewer", None).await;
        let system = match &knowledge {
            Some(k) => format!("{system}{}", k.prompt_section()),
            None => system,
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
            mcp: mcp.as_ref(),
            knowledge: knowledge.as_ref(),
        };
        let mut tools = review_agent::review_tools();
        if let Some(k) = &knowledge {
            tools.extend(k.tools());
        }
        tools.push(super::github_history::tool());
        tools.push(super::web_check::tool());
        tools.extend(graph_tools);
        if let Some(o) = &ops {
            tools.extend(o.tools());
        }
        if let Some(l) = &live {
            tools.extend(l.tools());
        }
        if let Some(m) = &mcp {
            tools.extend(m.tools());
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
            common::agent_policy::Agent::Reviewer,
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
                return Err(format!("the model call failed: {e}").into());
            }
        };
        // An answer that hit its token limit ends on its last whole sentence.
        answer = common::prose::end_on_sentence(&answer).to_string();
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
            answer = common::prose::end_on_sentence(&c.revised_reply).to_string();
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
        // A reading of a comment can't clear or add a block: only something
        // the verdict rests on, more than people's words, starts a formal
        // review from a reply.
        let more_than_words = match conclusion.as_ref().map(|c| c.verdict_change) {
            Some(review_agent::VerdictChange::Clear | review_agent::VerdictChange::Block) => {
                let now = read_fingerprint(state, &github, &msg, &head_sha, pr_body, base).await;
                let stored = super::review_policy::review_of_head(&records, &head_sha)
                    .and_then(|r| r.fingerprint.as_ref());
                super::review_fingerprint::more_than_words(&super::review_fingerprint::changes(
                    stored, &now,
                ))
            }
            _ => false,
        };
        let decision = conclusion.as_ref().map(|c| {
            super::review_verdict::decide(
                super::review_verdict::Input::Reply {
                    change: c.verdict_change,
                    more_than_words,
                },
                &super::review_verdict::Context {
                    standing: standing.clone(),
                    self_authored,
                },
            )
        });
        // An answered finding: recorded, and when the reply concludes the
        // answer settles it, its thread is resolved. A settled finding while
        // the standing verdict requests changes re-runs the formal review.
        let mut decision = decision;
        let mut settle_reason = String::new();
        if let Some(f) = &answered {
            let settled = conclusion.as_ref().is_some_and(|c| c.settles_finding);
            let mut resolved = f.thread.as_ref().is_some_and(|t| t.resolved);
            if settled {
                match super::review_thread_reply::settle(&github, &msg, f).await {
                    Some(note) => answer = format!("{}\n\n{note}", answer.trim_end()),
                    None => resolved = f.thread.is_some(),
                }
                let holds = standing
                    .as_ref()
                    .is_some_and(|s| s.verdict == "REQUEST_CHANGES");
                if holds && decision != Some(super::review_verdict::Decision::Review) {
                    decision = Some(super::review_verdict::Decision::Review);
                    settle_reason =
                        "A person's answer settled a finding while the review requested changes."
                            .to_string();
                }
            }
            super::review_thread_reply::record(state, &msg, f, settled, resolved, &answer).await;
            info!(
                pr = msg.pr_number,
                settled, resolved, "Reviewer: answered a reply to its finding"
            );
        }
        if decision == Some(super::review_verdict::Decision::Review) {
            let reason = if settle_reason.is_empty() {
                conclusion
                    .as_ref()
                    .map(|c| c.reason.clone())
                    .unwrap_or_default()
            } else {
                settle_reason
            };
            info!(
                pr = msg.pr_number,
                head = %head_sha,
                change = ?conclusion.as_ref().map(|c| c.verdict_change),
                standing = ?standing,
                "Reply conclusion differs from the standing verdict; formal review runs first"
            );
            live_job.not_posted("Re-reviewing because what the verdict rests on changed.");
            return Ok(Outcome::ReviewThenReply {
                review: Box::new(reconcile_message(&msg, &head_sha, &reason)),
                reply: Box::new(PendingReply {
                    head_sha: head_sha.clone(),
                    answer,
                    consulted,
                    applied_standards: applied,
                }),
            });
        }
        // The answer reads otherwise than the verdict, and nothing it rests on
        // changed: the verdict stands, and the answer says so.
        if let (Some(c), Some(st)) = (&conclusion, &standing) {
            if !self_authored && super::review_verdict::reply_disagrees(c.verdict_change, st) {
                info!(pr = msg.pr_number, head = %head_sha, change = ?c.verdict_change, standing = %st.verdict, "Reply reads otherwise than the verdict; nothing it rests on changed, the verdict stands");
                answer = format!(
                    "{}\n\n{}",
                    answer.trim_end(),
                    super::review_policy::verdict_stands_note(&head_sha, &st.verdict)
                );
            }
        }
        send_reply(
            state, &github, &msg, live_job, &head_sha, &answer, &consulted, &applied, &usage,
        )
        .await?;
        live_job
            .step(state, &msg, "Answered on the pull request", "")
            .await;
        live_job.answered(
            &head_sha,
            "CoderHelm answered on the pull request; the verdict on this commit is unchanged.",
        );
        return Ok(DONE);
    }

    // ── Verdict mode (agentic: walk the repo, structured findings, self-critique) ──

    // What the verdict rests on, read before anything else: compared with what
    // the last review of this commit rested on (never the commit alone), and
    // the base of what this review stores.
    let read_now = read_fingerprint(state, &github, &msg, &head_sha, pr_body, base).await;
    let review_before = super::review_policy::review_of_head(&records, &head_sha).cloned();
    let changed_since = super::review_fingerprint::changes(
        review_before.as_ref().and_then(|r| r.fingerprint.as_ref()),
        &read_now,
    );
    // A person asked again and nothing changed: the standing verdict is the
    // answer, with no model review, and the threads stay as they are.
    if super::review_policy::answers_from_standing(
        &msg.trigger,
        &changed_since,
        review_before.is_some(),
    ) {
        if let Some(prev) = &review_before {
            info!(pr = msg.pr_number, head = %head_sha, trigger = %msg.trigger, verdict = %prev.verdict, "Re-review ask with nothing changed — answered from the standing verdict, no model review");
            let answer =
                super::review_policy::standing_answer(&head_sha, &prev.verdict, &prev.holding);
            let mut asked = msg.clone();
            asked.reply_to_comment_id = asked_in;
            send_reply(
                state,
                &github,
                &asked,
                live_job,
                &head_sha,
                &answer,
                "",
                &[],
                &usage,
            )
            .await?;
            live_job
                .step(
                    state,
                    &msg,
                    "Nothing changed: answered from the standing verdict",
                    "",
                )
                .await;
            live_job.answered(
                &head_sha,
                "Nothing the verdict rests on changed since CoderHelm's last review of this \
                 commit, so it answered from that verdict without reviewing again.",
            );
            return Ok(DONE);
        }
    }

    // A person's re-review ask and a push review of the same commit used to
    // run together and post opposite verdicts seconds apart. The ask takes
    // the per-commit claim the gateway takes for push reviews: while another
    // review of this commit is still running, that one answers.
    if super::review_policy::takes_head_claim(&msg.trigger) && !head_sha.is_empty() {
        let sk = super::review_policy::head_claim_sk(
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &head_sha,
        );
        match common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &sk,
            REVIEW_HEAD_CLAIM_SECS,
        )
        .await
        {
            // Another review of this commit is still running. The person's
            // ask is answered after it, with its own visible verdict: the
            // ask waits and runs again (and runs anyway after the last wait).
            common::claim::Claim::Held
                if super::review_policy::review_of_head(&records, &head_sha).is_none() =>
            {
                if let Some(later) = super::review_policy::deferred_ask(&msg) {
                    if enqueue_review(state, &later, super::review_policy::ASK_WAIT_SECS).await {
                        info!(pr = msg.pr_number, head = %head_sha, trigger = %msg.trigger, dedup_key = %later.dedup_key, "Re-review ask waits for the running review of this commit, then answers");
                        let short = common::review_check::short(&head_sha);
                        live_job
                            .hand_off_check(
                                state,
                                &msg,
                                &later.dedup_key,
                                chrono::Utc::now().timestamp()
                                    + super::review_policy::ASK_WAIT_SECS as i64,
                                &format!("Waiting for the running review of {short}, then answering the ask"),
                                "Another review of this commit is running; the ask is answered \
                                 right after it, in this check.",
                            )
                            .await;
                        return Ok(Outcome::Skipped {
                            reason: "This commit is already being reviewed; your ask is answered \
                                     right after that review."
                                .into(),
                        });
                    }
                }
                info!(pr = msg.pr_number, head = %head_sha, "Re-review ask runs now (waited enough, or couldn't wait)");
            }
            common::claim::Claim::Failed(e) => {
                warn!(pr = msg.pr_number, error = %e, "Review head claim errored — reviewing (fail-open)");
            }
            // Won, or held by a review of this commit that already finished:
            // a person asked again, so it runs (and keeps its verdict unless
            // something is new, see review_policy::stabilize).
            _ => {}
        }
    }

    // The PR shows the review in progress, linked to this session.
    live_job
        .open_check(state, &github, &msg, &head_sha, &files_read)
        .await;

    // A push after CoderHelm approved an earlier commit is reviewed from that
    // commit: only what changed since. One that changed no code or data, or
    // left the PR's own change as approved (merges of the base branch, or a
    // rebase onto it), is not reviewed at all and the approval stands. A
    // push after a review that didn't approve is reviewed from that review's
    // commit too: only the hunks changed since, with the earlier findings'
    // status; it never approves without a full review first.
    let mut review_diff = diff.clone();
    // The PR's own change is the one CoderHelm reviewed: every commit since
    // brought in only the base branch (merges of work already merged there,
    // or a rebase onto it).
    let mut own_change_unchanged = false;
    // The commit a hunks-only re-review starts from.
    let mut since_review: Option<String> = None;
    let unconfirmed_in_body = review_agent::unconfirmed_external_facts(pr_body, &[]);
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
            Ok(since) => {
                let short = &approved[..approved.len().min(7)];
                let ahead = super::review_scope::builds_on(&since);
                // Merges of the base branch change code, but not the PR's,
                // and a rebase onto it leaves the approved commit off the
                // branch: compare the PR's own change now with the one
                // approved, whichever happened.
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
                    (ahead && !super::review_scope::touches_code(&since)) || own_change_unchanged;
                if nothing_new && unconfirmed_in_body.is_empty() {
                    info!(pr = msg.pr_number, approved = %approved, head = %head_sha, own_change_unchanged, "Push changed none of the PR's code or data since the approval — review skipped, approval carried");
                    live_job.settle(NotReviewed::UnchangedSinceApproval {
                        approved: &approved,
                    });
                    // Many repos dismiss approvals when commits are pushed:
                    // post the approval again on this head, so the PR never
                    // sits without a visible verdict.
                    carry_approval(
                        state,
                        &github,
                        &msg,
                        live_job,
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
                        "(CoderHelm approved {short} earlier. Every commit since only brings in \
                         the base branch: the PR's own change below is the one approved. Review \
                         nothing new in it; triage the facts the description marks \
                         unconfirmed.)\n{diff}"
                    );
                } else if ahead {
                    info!(pr = msg.pr_number, approved = %approved, head = %head_sha, files = ?super::review_scope::code_files(&since), "Reviewing only what changed since the approval");
                    review_diff = format!(
                        "(CoderHelm approved {short} earlier. This review covers only what \
                         changed since then; the rest of the PR was reviewed and approved.)\n{}",
                        review_agent::format_diff(&since, 40_000)
                    );
                } else {
                    info!(pr = msg.pr_number, approved = %approved, "Approved commit isn't an ancestor of the head (rebased) and the change differs — full review");
                }
            }
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not read the diff since the approval — full review");
            }
        }
    } else if let Some(reviewed) = super::review_scope::reviewed_base(
        &msg.trigger,
        msg.question.is_some(),
        msg.full_review,
        &records,
        &head_sha,
    ) {
        let short = reviewed[..reviewed.len().min(7)].to_string();
        own_change_unchanged = match github
            .get_diff(&msg.repo_owner, &msg.repo_name, base, &reviewed)
            .await
        {
            Ok(at_review) => super::review_scope::same_own_change(&at_review, &compare),
            Err(_) => false,
        };
        if own_change_unchanged && unconfirmed_in_body.is_empty() {
            let verdict = records
                .iter()
                .find(|r| r.is_review() && !r.failed())
                .map(|r| r.verdict.to_lowercase().replace('_', " "))
                .unwrap_or_default();
            info!(pr = msg.pr_number, reviewed = %reviewed, head = %head_sha, "Push left the PR's own change as reviewed — review skipped, verdict stands");
            let standing_verdict = records
                .iter()
                .find(|r| r.is_review() && !r.failed())
                .map(|r| r.verdict.clone())
                .unwrap_or_default();
            live_job.settle(NotReviewed::UnchangedSinceReview {
                reviewed: &reviewed,
                verdict: &standing_verdict,
            });
            return Ok(Outcome::Skipped {
                reason: format!(
                    "Not reviewed: the commits since CoderHelm reviewed {short} only bring in \
                     the base branch, and the PR's own change is the one reviewed, so that \
                     review's verdict ({verdict}) stands."
                ),
            });
        }
        if !own_change_unchanged {
            match github
                .get_diff(&msg.repo_owner, &msg.repo_name, &reviewed, &head_sha)
                .await
            {
                Ok(since) if super::review_scope::builds_on(&since) => {
                    info!(pr = msg.pr_number, reviewed = %reviewed, head = %head_sha, files = ?super::review_scope::code_files(&since), "Reviewing only the hunks changed since the previous review");
                    review_diff = format!(
                        "{}\n{}",
                        super::review_scope::since_review_note(&reviewed),
                        review_agent::format_diff(&since, 40_000)
                    );
                    since_review = Some(reviewed.clone());
                }
                Ok(_) => {
                    info!(pr = msg.pr_number, reviewed = %reviewed, "Reviewed commit isn't an ancestor of the head (rebased) — full review");
                }
                Err(e) => {
                    warn!(pr = msg.pr_number, error = %e, "Could not read the diff since the previous review — full review");
                }
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
    // What outcomes taught: this repo's calibration, the prompt variant the
    // bandit picked, good and avoid examples, and evidence-backed lessons.
    let reward_paths: Vec<String> = changed_files(&compare).into_iter().collect();
    let reward = super::rewards::review_learning(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        msg.pr_number,
        &reward_paths,
        &format!("{title}\n{}", reward_paths.join("\n")),
    )
    .await;
    let instructions_block = format!(
        "{}{org_block}{extra}{learning}{team_lessons}{}{}",
        super::format_instructions_block(&repo_instructions),
        reward.context,
        reward.addendum,
    );
    let critic_lessons = format!("{team_lessons}{}", reward.context);
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
        .list_ci_for_ref(&msg.repo_owner, &msg.repo_name, &head_sha)
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
    // Checks that fail on the base branch too were not caused by this PR:
    // the reviewer is told, and a finding resting on one is a note.
    let base_failing: Vec<String> = if base.is_empty() {
        Vec::new()
    } else {
        match github
            .list_ci_for_ref(&msg.repo_owner, &msg.repo_name, base)
            .await
        {
            Ok(v) => super::review_policy::failing_checks(&v),
            Err(e) => {
                warn!(pr = msg.pr_number, error = %e, "Could not fetch the base branch's check runs");
                Vec::new()
            }
        }
    };
    let ci_context = if base_failing.is_empty() {
        ci_context
    } else {
        format!(
            "{ci_context}\n## Checks failing on the base branch too ({})\n{}\nThese fail without \
             this PR's change: never block on them. Mention one as a note at most, with \
             `caused_by_diff`: false.\n",
            &base[..base.len().min(7)],
            base_failing
                .iter()
                .map(|n| format!("- {n}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    // For CoderHelm's own PR: what its planner looked up and found about
    // where the fix belongs, so a diff that overrides source data can be
    // judged against it.
    let plan_context = plan_evidence_for_pr(state, &msg).await;
    let quality_context = code_quality_context(&github, &msg, &compare).await;
    let extra_context = format!("{graph_context}{ci_context}{quality_context}{plan_context}");

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
        .filter(|t| settled_by_answer_or_person(t))
        .collect();
    // What people wrote in the PR conversation, including the text of a
    // re-review ask.
    let conversation_raw = match github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not list PR conversation comments");
            Vec::new()
        }
    };
    let discussion = human_comments(&conversation_raw, review_agent::MAX_DISCUSSION_COMMENTS);
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
    // What this review is in the PR's history: a second look at a commit
    // already reviewed keeps its verdict unless something is new; a later
    // round blocks only on what changed since the round before.
    let same_head_before = super::review_policy::review_of_head(&records, &head_sha).cloned();
    let review_round = super::review_policy::round(&records, &head_sha);
    let history_context =
        super::review_policy::prompt_section(same_head_before.as_ref(), &review_round);
    let prior_context = format!(
        "{prior_context}{label_context}{tests_context}{reconcile_context}{history_context}"
    );

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
    // Shared remote MCP servers, read-only tools only (a review is for no
    // one person, so personal connections aren't used).
    let mcp = super::remote_mcp::RemoteMcp::load(
        state,
        &msg.team_id,
        None,
        format!(
            "reviewer, {}/{}#{}",
            msg.repo_owner, msg.repo_name, msg.pr_number
        ),
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

    // Knowledge bases the team attached to the reviewer.
    let knowledge =
        super::knowledge::KnowledgeTools::load(state, &msg.team_id, "reviewer", None).await;
    // 1) High-recall generation with repo-walking tools.
    live_job
        .step(
            state,
            &msg,
            "Reviewing the code",
            "Against the repository's rules, the team's standards and what earlier reviews taught.",
        )
        .await;
    // What the reviewer does from here, live on the session page.
    let activity =
        super::review_activity::ActivityFeed::start(super::review_activity::SessionTarget {
            dynamo: state.dynamo.clone(),
            table: state.config.settings_table_name.clone(),
            team_id: msg.team_id.clone(),
            job: live_job.id.clone(),
        });
    let observer = super::review_activity::ActivityObserver(activity.recorder().clone());
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
        mcp.as_ref(),
        knowledge.as_ref(),
        Some(&observer),
        &mut usage,
    )
    .await;
    // A review that didn't finish has no verdict: the job is retried (see
    // `run`), and nothing is posted for it. Its tokens still count.
    let output = match output {
        Ok(o) => o,
        Err(reason) => {
            return Err(reason.into());
        }
    };
    let fixed_threads: Vec<&crate::clients::github::BotReviewThread> =
        settled_prior_threads(&prior_threads, &output.prior);
    // Comments the code now fixes (or the author accepted) earned reward;
    // answered-only ones are not counted as acted on.
    {
        let acted: Vec<&crate::clients::github::BotReviewThread> = output
            .prior
            .iter()
            .filter(|s| {
                s.status.eq_ignore_ascii_case("fixed") || s.status.eq_ignore_ascii_case("accepted")
            })
            .filter_map(|s| prior_threads.get(s.index).copied())
            .collect();
        super::rewards::record_accepted(
            state,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            msg.pr_number,
            &acted,
        )
        .await;
    }
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
    let drafted = output.findings.len();
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
            lessons: &critic_lessons,
        },
        &mut usage,
    )
    .await;
    live_job
        .step(
            state,
            &msg,
            &format!("Checked the findings: kept {} of {drafted}", findings.len()),
            "A second pass drops findings it can't back up.",
        )
        .await;
    // Categories this repo keeps dismissing are quieted; security and
    // correctness never are.
    // A repo that requires tests keeps its tests findings whatever the counts.
    let calibration: Vec<_> = reward
        .calibration
        .iter()
        .filter(|s| !(cfg.require_tests && s.category.eq_ignore_ascii_case("tests")))
        .cloned()
        .collect();
    let (calib_dropped, calib_downgraded) =
        super::rewards::apply_calibration(&mut findings, &calibration);
    if calib_dropped + calib_downgraded > 0 {
        info!(
            pr = msg.pr_number,
            dropped = calib_dropped,
            downgraded = calib_downgraded,
            "Reviewer calibration applied"
        );
    }

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

    // 2b2) Ground the classifier's fields in what each finding shows: a
    // traced code path or a documented exit/status code is verified, and a
    // cited team rule must be quoted from the team's rules (before the checks
    // that may make a finding a note).
    let grounded = super::review_severity::ground(&mut findings, &team_instructions);
    if grounded != Default::default() {
        info!(
            pr = msg.pr_number,
            documented = grounded.documented,
            traced = grounded.traced,
            unquoted_rules = grounded.unquoted_rules,
            main_path = grounded.main_path,
            "Findings grounded for the severity classifier"
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

    // 3) What the repository requires: with "Require tests" on, a missing or
    // stale test blocks unless a checked exemption applies (a package without
    // a test runner is not one), and a finding that breaks a rule the team
    // wrote as required (MUST, NEVER, ...) blocks. Nothing after this makes
    // such a finding a note (`review_rules::mandatory`).
    let rules_policy = super::review_rules::Policy::new(cfg.require_tests, &team_instructions);
    let changed_list: Vec<String> = {
        let mut v: Vec<String> = changed_files(&compare).into_iter().collect();
        v.sort();
        v
    };
    let ruled = super::review_rules::apply(&mut findings, &rules_policy, &changed_list);
    if ruled != Default::default() {
        info!(
            pr = msg.pr_number,
            required = ruled.required,
            exempt = ruled.exempt,
            refused_exemptions = ruled.refused,
            "Findings held to the repository's required rules"
        );
    }
    review_agent::enforce_source_data_override(&mut findings);
    // A request to test something by hand is a note, never a block.
    review_agent::enforce_manual_checks_are_notes(&mut findings);
    // Rule 3: only a high-severity defect caused by the diff holds the merge.
    let made_notes = super::review_policy::apply_blocking_policy(&mut findings, &base_failing);
    if made_notes > 0 {
        info!(
            pr = msg.pr_number,
            made_notes, "Blockers that aren't defects caused by the diff made notes"
        );
    }
    // What people wrote since CoderHelm's previous review (conversation,
    // inline replies): new information that may justify a new blocker on
    // code reviewed before, or a changed verdict on the same commit.
    let mut people_wrote = conversation_raw.clone();
    if let Ok(c) = github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        people_wrote.extend(c);
    }
    let previous_review_at = records
        .iter()
        .find(|r| r.is_review() && !r.failed())
        .map(|r| r.created_at.clone())
        .unwrap_or_default();
    let spoke_since_previous =
        super::review_policy::person_spoke_since(&people_wrote, &previous_review_at);
    // Rule 4: every blocker belongs in the first review of the code it is
    // about. A new one on lines unchanged since the previous reviewed head is
    // a note (225 approve-then-request-changes flips on 123 PRs, and 199
    // blockers first raised in round 3 or later, came from this).
    let changed_since_previous = match review_round.previous_head.as_deref() {
        Some(prev) if prev != head_sha => {
            if own_change_unchanged {
                // Only the base branch came in: none of the PR's code changed.
                Some(std::collections::HashMap::new())
            } else {
                match github
                    .get_diff(&msg.repo_owner, &msg.repo_name, prev, &head_sha)
                    .await
                {
                    Ok(since) if super::review_scope::builds_on(&since) => {
                        Some(super::review_policy::changed_lines(&since))
                    }
                    Ok(_) => None,
                    Err(e) => {
                        warn!(pr = msg.pr_number, error = %e, "Could not read the diff since the previous review — no late-blocker check");
                        None
                    }
                }
            }
        }
        _ => None,
    };
    let earlier_threads: Vec<(String, u64)> = bot_threads
        .iter()
        .map(|t| (t.path.clone(), t.line))
        .collect();
    let late = super::review_policy::hold_late_blockers(
        &mut findings,
        &super::review_policy::UpFront {
            round: &review_round,
            changed_since_previous: changed_since_previous.as_ref(),
            earlier_threads: &earlier_threads,
            person_spoke: spoke_since_previous.is_some(),
        },
    );
    if !late.held.is_empty() {
        info!(
            pr = msg.pr_number,
            round = review_round.number,
            held = late.held.len(),
            "New blockers on code unchanged since the previous review kept as notes"
        );
        if late.self_lesson {
            super::review_lessons::learn_raise_early(
                state,
                &msg.team_id,
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
            )
            .await;
        }
    }
    let held_late = late.held;
    // Severity second pass: an independent judge checks every note against
    // the blocking rubric (one batched call); a note it calls blocking holds
    // the merge. Then each finding's severity is set to read like its class.
    let second = super::review_severity::second_pass(
        state,
        &provider,
        &mut findings,
        &review_diff,
        title,
        pr_body,
        &team_instructions,
        &mut usage,
    )
    .await;
    super::review_severity::finalize(&mut findings);
    info!(
        pr = msg.pr_number,
        checked = second.checked,
        raised = second.raised,
        blocking = findings.iter().filter(|f| f.holds_merge()).count(),
        notes = findings.iter().filter(|f| !f.holds_merge()).count(),
        "Review severity second pass"
    );

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
            &msg.team_id,
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
    // Rule 2: a repeat review on an identical fingerprint doesn't flip on model
    // noise; anything changed, the verdict follows what holds the merge now.
    let stable = super::review_policy::stabilize(
        same_head_before.as_ref(),
        verdict,
        &mut findings,
        &changed_since,
        &output.change_of_mind,
        super::review_policy::trigger_kind(&msg.trigger)
            == super::review_policy::TriggerKind::Explicit,
    );
    if stable.held {
        info!(pr = msg.pr_number, head = %head_sha, kept = stable.verdict, model_verdict = verdict, "Verdict kept on an unchanged commit — nothing new since the last review");
    }
    if let Some(from) = &stable.flipped_from {
        info!(pr = msg.pr_number, head = %head_sha, from = %from, to = stable.verdict, reason = %stable.note.as_deref().unwrap_or(""), "Verdict changed on an unchanged commit");
    }
    // Never APPROVE while a finding holds the merge. On CoderHelm's own PR,
    // every finding that isn't strictly optional is its own to fix before
    // the PR is marked ready or the merge armed: the review requests changes
    // and lists them at the top of the description (`own_review`).
    let mut own_open = if self_authored {
        super::own_review::required(&findings, &held_late)
    } else {
        Vec::new()
    };
    // A hunks-only re-review reports the earlier findings by status, not as
    // findings again: those still open that hold the merge stay listed.
    if self_authored && since_review.is_some() {
        for t in prior_threads
            .iter()
            .filter(|t| !fixed_threads.iter().any(|f| f.id == t.id))
            .filter(|t| review_agent::thread_holds_merge(&t.body))
        {
            let line = super::own_review::thread_line(&t.path, t.line, &t.body);
            if !own_open.contains(&line) {
                own_open.push(line);
            }
        }
    }
    // The one verdict the rest of this review may use: what holds the merge,
    // by name. Nothing named holding means approve, whatever came before.
    let open_holding: Vec<(String, u64, String)> = prior_threads
        .iter()
        .filter(|t| !fixed_threads.iter().any(|f| f.id == t.id))
        .filter(|t| review_agent::thread_holds_merge(&t.body))
        .map(|t| (t.path.clone(), t.line, t.body.clone()))
        .collect();
    let mut final_verdict =
        super::review_verdict::Verdict::from_holding(super::review_policy::named_holding(
            &findings,
            &open_holding,
            &unconfirmed,
            verify_failed,
            &own_open,
            &stable.kept,
        ));
    // A request for changes on someone's PR is a costly decision: the stop
    // judge sees the whole picture and either holds on a named correctness,
    // security or data-loss problem, or the review approves with the
    // holdings as notes and its assumption written down.
    let mut review_judged: Option<super::stop_judge::Judged> = None;
    let mut judge_note = String::new();
    // What blocks by rule (a required rule, a failing check, a critical path
    // with no proof) isn't a judgment: no judge lifts it, so a verdict held
    // only by rule isn't put to one.
    let only_by_rule = final_verdict.holding().iter().all(|h| {
        matches!(
            h.kind,
            super::review_verdict::HoldKind::Required
                | super::review_verdict::HoldKind::Verification
        )
    });
    if !self_authored && final_verdict.event() == "REQUEST_CHANGES" && !only_by_rule {
        let picture = block_picture(
            title,
            pr_body,
            &compare,
            &findings,
            &final_verdict,
            &prior_threads,
            &fixed_threads,
            verify_failed,
            &verify_md,
        );
        let judged = super::stop_judge::decide(
            state,
            &provider,
            &mut usage,
            &msg.team_id,
            &super::stop_judge::pr_subject(&msg.repo_owner, &msg.repo_name, msg.pr_number),
            &head_sha,
            picture,
        )
        .await;
        if let Some(a) = judged.assumption() {
            info!(pr = msg.pr_number, head = %head_sha, "Stop judge: approve with notes instead of requesting changes");
            for f in findings
                .iter_mut()
                .filter(|f| f.holds_merge() && super::review_rules::mandatory(f).is_none())
            {
                super::review_policy::demote(
                    f,
                    "FYI, not a block: on the whole picture this is not a named correctness, \
                     security or data-loss problem to hold the merge on.",
                );
            }
            // What blocks by rule still holds; the rest is the judge's call.
            let by_rule: Vec<super::review_verdict::Holding> = final_verdict
                .holding()
                .iter()
                .filter(|h| {
                    matches!(
                        h.kind,
                        super::review_verdict::HoldKind::Required
                            | super::review_verdict::HoldKind::Verification
                    )
                })
                .cloned()
                .collect();
            judge_note = if by_rule.is_empty() {
                format!(
                    "\n\n**FYI:** on the whole picture nothing here holds the merge. Assumed: {}",
                    a.trim()
                )
            } else {
                format!(
                    "\n\n**FYI:** apart from what blocks by rule, nothing here holds the merge. \
                     Assumed: {}",
                    a.trim()
                )
            };
            final_verdict = super::review_verdict::Verdict::from_holding(by_rule);
        }
        review_judged = Some(judged);
    }
    let verdict = final_verdict.event();
    // A hunks-only re-review never approves: the whole PR is reviewed first.
    if super::review_scope::needs_full_before_approval(since_review.is_some(), verdict) {
        let mut full = msg.clone();
        full.full_review = true;
        full.dedup_key = format!("full#{head_sha}");
        // Its model calls are already on the team's counters (per call).
        if enqueue_review(state, &full, 0).await {
            info!(pr = msg.pr_number, head = %head_sha, "Hunks-only re-review found nothing holding the merge — full review queued before approving");
            live_job
                .hand_off_check(
                    state,
                    &msg,
                    &full.dedup_key,
                    chrono::Utc::now().timestamp(),
                    &format!(
                        "Reviewing the whole pull request at {} before approving",
                        common::review_check::short(&head_sha)
                    ),
                    "The changes since the previous review settle what it found; the whole \
                     pull request is reviewed again before it is approved, in this check.",
                )
                .await;
            live_job.settle(NotReviewed::FullReviewFollows { head: &head_sha });
            return Ok(Outcome::Skipped {
                reason: "The changes since the previous review settle what it found; the whole \
                         pull request is reviewed again before it is approved."
                    .into(),
            });
        }
        warn!(
            pr = msg.pr_number,
            "Could not queue the full review — posting this one"
        );
    }
    // The description this review leaves: CoderHelm's own PR gets its open
    // findings written into it.
    let mut description_after = pr_body.to_string();
    if self_authored {
        let section = super::own_review::section(&own_open);
        let updated = super::completeness::with_section(
            pr_body,
            super::completeness::Section::OpenFindings,
            section,
        );
        if updated != pr_body {
            if let Err(e) = github
                .update_pr_body(&msg.repo_owner, &msg.repo_name, msg.pr_number, &updated)
                .await
            {
                warn!(pr = msg.pr_number, error = %e, "Could not update the own PR's open review findings");
            } else {
                description_after = updated;
            }
        }
        if !own_open.is_empty() {
            info!(
                pr = msg.pr_number,
                open = own_open.len(),
                "Own PR has open review findings — CoderHelm fixes them before ready"
            );
        }
    }
    // Inline comments from the findings as they stand after every rule, so a
    // demoted finding is posted as a note.
    let hunks = review_agent::right_line_hunks(&compare);
    // Blockers first, everywhere the findings are listed.
    findings.sort_by_key(|f| super::review_severity::tier(f) as u8);
    let postable = review_agent::to_postable(&findings, &changed, &hunks);
    // The comments the review keeps close its activity.
    for f in &findings {
        activity.recorder().comment(&f.file, f.line, &f.title);
    }
    activity.close().await;
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
    let effective_event = if self_authored { "COMMENT" } else { verdict };
    let verdict_line = review_agent::verdict_heading(
        &final_verdict,
        findings.iter().filter(|f| !f.holds_merge()).count()
            + (unconfirmed.len() - blocking_facts)
            + held_late.len(),
    );
    // One line naming what decided the verdict, on the review and its record.
    let verdict_why = {
        use super::review_severity::{tier, Tier};
        let should_fix = findings
            .iter()
            .filter(|f| tier(f) == Tier::ShouldFix)
            .count();
        let suggestions = findings
            .iter()
            .filter(|f| tier(f) == Tier::Suggestion)
            .count()
            + (unconfirmed.len() - blocking_facts)
            + held_late.len();
        final_verdict.why(should_fix, suggestions)
    };
    let verdict_line = format!("{verdict_line}\n\n_{verdict_why}_");
    let summary = if output.summary.trim().is_empty() {
        "Automated review complete.".to_string()
    } else {
        short_overview(&output.summary)
    };
    // A review the description edit started says so first.
    let verdict_line = if msg.trigger == common::review_description::TRIGGER {
        format!(
            "{verdict_line}\n\n_{}_",
            common::review_description::SUMMARY_NOTE
        )
    } else {
        verdict_line
    };
    // The review leads with what the PR does, file by file.
    let overview = common::review_overview::build(
        &output.overview,
        &common::review_overview::changed_files(&compare),
    );
    let overview_md = common::review_overview::markdown(&overview);
    let verdict_line = if overview_md.is_empty() {
        verdict_line
    } else {
        format!("{overview_md}\n{verdict_line}")
    };
    let mut full_body = match &stable.note {
        Some(n) => format!(
            "{verdict_line}\n\n{n}\n\n{summary}\n\n{}",
            risk_report.markdown()
        ),
        None => format!("{verdict_line}\n\n{summary}\n\n{}", risk_report.markdown()),
    };
    full_body.push_str(&judge_note);
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
    if !held_late.is_empty() {
        full_body.push_str("\n\n#### Also worth a look\nOn code that hasn't changed since my previous review, so these are notes and don't change the verdict (they should have been raised then):\n");
        for f in &held_late {
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
    full_body.push_str(&rating_footer(
        common::public_urls::app_url(&state.config.stage),
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    ));

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
            live_job.not_posted(
                "a newer commit was pushed while reviewing; its own review covers the latest code.",
            );
            live_job.settle(NotReviewed::Superseded { newer: latest_head });
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
        || super::review_policy::trigger_kind(&msg.trigger)
            == super::review_policy::TriggerKind::Explicit;
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
    // A person's ask is never silenced by it: it always gets its own answer.
    let already_answered = !explicit
        && prev
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
    if already_answered {
        info!(pr = msg.pr_number, head = %head_sha, "Another review of this commit was just posted — not posting a duplicate");
    }
    // One formal verdict per (PR, head, trigger kind): a conditional write,
    // so two jobs for the same trigger never both post.
    let verdict_sk = super::review_policy::verdict_claim_sk(
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
        &head_sha,
        &msg.trigger,
        &msg.dedup_key,
    );
    let decision = match decision {
        super::review_verdict::Decision::Post { .. }
            if common::claim::claim(
                &state.dynamo,
                &state.config.settings_table_name,
                &msg.team_id,
                &verdict_sk,
                REVIEW_JOB_DEDUP_TTL_SECS,
            )
            .await
                == common::claim::Claim::Held =>
        {
            info!(pr = msg.pr_number, head = %head_sha, trigger = %msg.trigger, "A verdict for this commit and trigger was already posted — not posting another");
            super::review_verdict::Decision::Nothing
        }
        d => d,
    };
    let should_comment = matches!(decision, super::review_verdict::Decision::Post { .. });
    let mut posted_event: Option<&'static str> = None;
    // The posted review's link, for the session and the check.
    let mut review_url = String::new();
    if let super::review_verdict::Decision::Post {
        event,
        dismiss_stale,
    } = decision
    {
        match super::review_verdict::post(
            &github,
            &msg,
            &head_sha,
            event,
            dismiss_stale,
            &full_body,
            &postable.inline,
            &final_verdict,
            review_judged.as_ref(),
        )
        .await
        {
            Ok(url) => review_url = url,
            Err(e) => {
                // Not posted: a retry of this job may post it.
                common::claim::release(
                    &state.dynamo,
                    &state.config.settings_table_name,
                    &msg.team_id,
                    &verdict_sk,
                )
                .await;
                return Err(e);
            }
        }
        posted_event = Some(event);
    } else if !already_answered && !head_sha.is_empty() {
        // Nothing new to post for this commit (same verdict, CoderHelm's own
        // push): the earlier review must not keep standing with what it
        // said about the older commit. Its body becomes this review.
        super::review_verdict::refresh_standing(&github, &msg, &head_sha, &full_body).await;
    }
    // Resolve the earlier threads this head fixed, posted or not (after the
    // review is up when there is one, so the "N fixed" note is visible when
    // they collapse): a thread the new commits settled never stays open.
    for t in &fixed_threads {
        if let Err(e) = github.resolve_review_thread(&t.id).await {
            warn!(pr = msg.pr_number, thread = %t.id, error = %e, "Failed to resolve fixed thread");
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
        live_job,
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
    record_review_usage(state, &msg.team_id, &record_sk, &usage).await;
    if let Some(s) = &skills {
        note_review_standards(state, &msg.team_id, &record_sk, &s.applied_names()).await;
    }
    note_review_details(state, &msg.team_id, &record_sk, title, &findings).await;
    // What this verdict rests on, for the next ask: what it read, with its own
    // threads as this review left them.
    let rests_on = super::review_fingerprint::Fingerprint {
        description: super::review_fingerprint::description_part(&description_after),
        threads: match github
            .list_bot_review_threads(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            Ok(t) => Some(super::review_fingerprint::threads_part(
                t.iter().map(|t| (t.id.clone(), t.resolved)),
            )),
            Err(_) => None,
        },
        ..read_now.clone()
    };
    note_review_state(
        state,
        &msg.team_id,
        &record_sk,
        &rests_on,
        &final_verdict.names(),
        &verdict_why,
    )
    .await;
    live_job
        .verdict(state, &msg, verdict, findings.len(), &review_url)
        .await;
    common::review_session::set_overview(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
        &live_job.id,
        &overview,
    )
    .await;
    let posted_step = if should_comment {
        "Posted the review on GitHub"
    } else if already_answered {
        "Not posted: another review of this commit was just posted"
    } else {
        "Nothing new to post: the standing review was brought up to date"
    };
    live_job.step(state, &msg, posted_step, &review_url).await;
    super::rewards::note_review_variant(
        state,
        &msg.team_id,
        &record_sk,
        &reward.arm,
        provider.review_model_id(),
    )
    .await;
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

    // A block changes what the auto-merge panel must name; it can't merge on it.
    if verdict == "REQUEST_CHANGES" {
        super::await_merge::refresh_panel(
            state,
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
    }
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
        //   (3) A finished run's findings are the ready decision's: it queues one
        //       review-fix round per head under the run's own round budget, or
        //       names them for a person. Feeding them back here too would fix
        //       them twice, outside that budget.
        let finished_run = if own_open.is_empty() {
            false
        } else {
            match lookup_run_by_pr(
                state,
                &msg.team_id,
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
            )
            .await
            {
                Some(run_id) => super::resume::run_finished(state, &msg.team_id, &run_id).await,
                None => false,
            }
        };
        if finished_run {
            info!(
                pr = msg.pr_number,
                open = own_open.len(),
                "Own PR of a finished run has open findings — the ready decision queues their round"
            );
            if msg.trigger != super::review_scope::HANDOFF_TRIGGER {
                super::ready::request(state, &msg).await;
            }
        } else if prev_verdict.as_deref() == Some("APPROVE") {
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
        open_findings: own_open.clone(),
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
    job: &LiveJob,
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
    let body = format!(
        "{heading}{}",
        rating_footer(
            common::public_urls::app_url(&state.config.stage),
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number
        )
    );
    if let Err(e) = super::review_verdict::post(
        github,
        msg,
        head,
        event,
        dismiss_stale,
        &body,
        &[],
        &super::review_verdict::Verdict::approve(),
        None,
    )
    .await
    {
        warn!(pr = msg.pr_number, head = %head, error = %e, "Could not post the carried approval");
        return;
    }
    store_review_record(
        state,
        msg,
        job,
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
    /// The findings the run must fix before the PR is marked ready: every
    /// one that isn't strictly optional (`own_review::required`).
    pub open_findings: Vec<String>,
}

impl RunReview {
    /// The verdict holds the merge, or the review left findings the run
    /// must fix first.
    pub fn blocks(&self) -> bool {
        self.verdict == "REQUEST_CHANGES" || !self.open_findings.is_empty()
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
        limit_waits: 0,
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
        thread_root_id: None,
        mentioned: false,
        requested_by: String::new(),
        check_head: String::new(),
        attempt: 0,
        wait_secs: 0,
        reconcile_note: None,
        outstanding_work: None,
        description_hash: None,
        full_review: false,
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
            settle_job_claim(state, &msg, false).await;
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
/// oscillating APPROVE↔REQUEST_CHANGES). The same cap as the run's own fix
/// rounds (`own_review::MAX_FIX_ROUNDS`).
const MAX_SELF_REVIEW_FIX_ROUNDS: u32 = super::own_review::MAX_FIX_ROUNDS;

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
    settle_job_claim(state, &job, keeps_job_claim(&out)).await;
    out
}

/// Whether a job keeps its dedup claim for the dedup window. A job that
/// reviewed (or answered) keeps it, so a redelivered copy is dropped. A job
/// that failed gives it up for its retry, and so does one that ended without
/// reviewing: the same key used by a later job (a run's own review takes the
/// key of a push review of the same head) must run. A push review CoderHelm
/// skipped as its own push during the run once kept `head#<sha>` for six
/// hours, and the run's own review of that head was dropped as a repeat.
pub(crate) fn keeps_job_claim<E>(out: &Result<Outcome, E>) -> bool {
    matches!(
        out,
        Ok(Outcome::Done { .. } | Outcome::ReviewThenReply { .. })
    )
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

/// GitHub Code Quality's open findings on the files the pull request
/// changes, as a prompt section (see `common::code_quality`). Empty when
/// there are none, when the installation can't read Code quality (no
/// request once that is known), or when the repository doesn't run it.
async fn code_quality_context(
    github: &GitHubClient,
    msg: &ReviewMessage,
    compare: &serde_json::Value,
) -> String {
    let changed: Vec<String> = compare["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f["filename"].as_str().map(str::to_string))
        .collect();
    if changed.is_empty() {
        return String::new();
    }
    let path = common::code_quality::list_path(&msg.repo_owner, &msg.repo_name);
    match github.get_granted("code_quality", &path).await {
        Ok(Some(v)) => {
            let all = v.as_array().map(|a| a.as_slice()).unwrap_or(&[]);
            common::code_quality::context(&common::code_quality::on_changed_files(all, &changed))
        }
        Ok(None) => String::new(),
        // Code quality off for the repository (a 404) is the common case.
        Err(e) => {
            tracing::debug!(pr = msg.pr_number, error = %e, "No code quality findings read");
            String::new()
        }
    }
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
pub(crate) async fn lookup_run_by_pr(
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

/// Why a push to CoderHelm's own PR isn't reviewed now, when it isn't: the
/// push is CoderHelm's and the run that owns the PR is still in its review
/// and fix loop (`review_scope::own_loop_holds_push`). Any lookup that fails
/// reviews the push.
async fn own_loop_hold(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReviewMessage,
    head: &str,
) -> Option<String> {
    let commits = github
        .list_pr_commits(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
        .ok()?;
    let by_coderhelm =
        super::review_scope::head_commit_login(&commits, head).is_some_and(super::is_own_login);
    if !by_coderhelm {
        return None;
    }
    let run_id = lookup_run_by_pr(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(&run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()?;
    let status = item
        .item()
        .and_then(|i| i.get("status"))
        .and_then(|v| v.as_s().ok())
        .cloned();
    super::review_scope::own_loop_holds_push(&msg.trigger, true, true, status.as_deref()).then(
        || {
            "Not reviewed now: CoderHelm pushed this while its run is still fixing the pull \
             request. The run asks for the review itself when its checks pass, and the pull \
             request is reviewed when the run hands it over."
                .to_string()
        },
    )
}

/// The review a run asks for when it hands its PR over without the PR
/// reviewer's verdict on its last head: CoderHelm's own pushes during the
/// run weren't reviewed (`own_loop_hold`). The job reads the PR's head when
/// it runs, and a head already reviewed returns its verdict with no model
/// work (`review_policy::start`). Best-effort.
pub(crate) async fn review_on_handoff(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    (owner, repo, pr_number): (&str, &str, u64),
    run_id: &str,
) {
    if pr_number == 0 || owner.is_empty() || repo.is_empty() {
        return;
    }
    let Some(label) = enabled_trigger_label(state, team_id, owner, repo).await else {
        return;
    };
    let msg = ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        label,
        trigger: super::review_scope::HANDOFF_TRIGGER.to_string(),
        dedup_key: format!("handoff#{run_id}"),
        ..Default::default()
    };
    if enqueue_review(state, &msg, 0).await {
        // The dashboard shows it queued, like a review the gateway sends.
        note_queued(state, &msg, 0).await;
        info!(
            pr = pr_number,
            run_id, "Run handed its PR over — review queued"
        );
    } else {
        warn!(pr = pr_number, run_id, "Run handed its PR over, but its review couldn't be queued; the ready gate asks for it again");
    }
}

/// What asking for a review of a held draft's head did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyReviewAsk {
    /// A review of the head is queued now.
    Queued,
    /// One was queued for this head before (it runs, or ran).
    AlreadyQueued,
    /// The reviewer is off for the repository: no review comes.
    ReviewerOff,
    /// The queue refused; the next decision asks again.
    Failed,
}

/// How long one ask for a head's review counts: a review that never came
/// (it failed every retry) is asked for again after this.
const READY_REVIEW_ASK_SECS: u64 = 2 * 3600;

/// The ready gate holds a draft for CoderHelm's approval on `head` and no
/// review of that head exists: one review is queued, once per head. It runs
/// as a run's hand-off does (drafts reviewed, the head claim respected), and
/// its verdict asks for the ready decision again.
pub(crate) async fn review_for_ready(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    (owner, repo, pr_number): (&str, &str, u64),
    head: &str,
) -> ReadyReviewAsk {
    let Some(label) = enabled_trigger_label(state, team_id, owner, repo).await else {
        return ReadyReviewAsk::ReviewerOff;
    };
    let sk = format!("READYREVIEW#{owner}/{repo}#{pr_number:06}#{head}");
    let table = &state.config.settings_table_name;
    if common::claim::claim(&state.dynamo, table, team_id, &sk, READY_REVIEW_ASK_SECS).await
        == common::claim::Claim::Held
    {
        return ReadyReviewAsk::AlreadyQueued;
    }
    let msg = ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        head_sha: head.to_string(),
        label,
        trigger: super::review_scope::HANDOFF_TRIGGER.to_string(),
        dedup_key: format!("ready#{head}"),
        ..Default::default()
    };
    if !enqueue_review(state, &msg, 0).await {
        common::claim::release(&state.dynamo, table, team_id, &sk).await;
        return ReadyReviewAsk::Failed;
    }
    note_queued(state, &msg, 0).await;
    info!(
        pr = pr_number,
        head, "The ready gate waits for CoderHelm's review of this head — review queued"
    );
    ReadyReviewAsk::Queued
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

/// What one review record holds besides the PR it is on.
struct ReviewRecord<'a> {
    head_sha: &'a str,
    verdict: &'a str,
    risk: &'a str,
    body: &'a str,
    posted_as: &'a str,
    action_summary: &'a str,
    /// RFC 3339; also the end of the sort key.
    created_at: &'a str,
    /// The job (session) that wrote it, when its session page exists.
    session_id: Option<&'a str>,
}

/// Pure: a review record's sort key and item.
fn review_record_item(
    msg: &ReviewMessage,
    r: &ReviewRecord<'_>,
) -> (
    String,
    std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let sk = format!("REVIEW#{repo}#{:0>6}#{}", msg.pr_number, r.created_at);
    // Truncate the stored body so a huge review can't blow the 400KB item limit.
    let body = common::head_tail_str(r.body, 30_000);
    let mut item: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> = [
        ("pk", attr_s(&msg.team_id)),
        ("sk", attr_s(&sk)),
        ("record_type", attr_s("review")),
        ("repo", attr_s(&repo)),
        ("pr_number", attr_n(msg.pr_number)),
        ("head_sha", attr_s(r.head_sha)),
        ("verdict", attr_s(r.verdict)),
        ("risk", attr_s(r.risk)),
        ("body", attr_s(&body)),
        ("posted_as", attr_s(r.posted_as)),
        ("trigger", attr_s(&msg.trigger)),
        ("thumbs_up", attr_n(0)),
        ("thumbs_down", attr_n(0)),
        ("created_at", attr_s(r.created_at)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    if !r.action_summary.is_empty() {
        item.insert(
            "action_summary".to_string(),
            attr_s(&common::head_tail_str(r.action_summary, 8_000)),
        );
    }
    if let Some(id) = r.session_id.filter(|id| !id.is_empty()) {
        item.insert(
            common::review_session::REVIEW_SESSION_ID.to_string(),
            attr_s(id),
        );
    }
    (sk, item)
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
    job: &LiveJob,
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
    let session_id = job.session_id();
    let (sk, item) = review_record_item(
        msg,
        &ReviewRecord {
            head_sha,
            verdict,
            risk,
            body,
            posted_as,
            action_summary,
            created_at: &created_at,
            session_id: session_id.as_deref(),
        },
    );
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(item))
        .send()
        .await
    {
        warn!(pr = msg.pr_number, error = %e, "Failed to persist review record (non-fatal)");
    }
    // The session that wrote it points back at it, for its page.
    if let Some(id) = &session_id {
        common::review_session::link_review(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            id,
            &sk,
            verdict != "QUESTION",
        )
        .await;
    }
    // A posted review is an event for the team's webhooks (an answer to a
    // question isn't a review).
    if counts.posted && verdict != "QUESTION" {
        super::webhooks::emit_review(state, msg, head_sha, verdict, posted_as, risk).await;
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
    // The PR on the team's "PRs reviewed" count (once per month and once in
    // all time). Every stored review passes here, so every path counts.
    let team_counts = common::team_counts::DynamoCounts {
        dynamo: state.dynamo.clone(),
        table: state.config.analytics_table_name.clone(),
    };
    if let Err(e) = common::team_counts::count_review(
        &team_counts,
        &msg.team_id,
        &repo,
        msg.pr_number,
        verdict,
        counts.posted,
        &created_at,
    )
    .await
    {
        warn!(pr = msg.pr_number, error = %e, "Failed to count the reviewed PR (non-fatal)");
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
            let check = f.severity_check.as_ref();
            serde_json::json!({
                "severity": f.severity.trim().to_ascii_lowercase(),
                "model_severity": f.model_severity.as_deref().unwrap_or(&f.severity).trim().to_ascii_lowercase(),
                "kind": kind,
                "blocks": f.holds_merge(),
                "tier": super::review_severity::tier(f).as_str(),
                "category": f.category,
                "required": f.required.as_deref().unwrap_or(""),
                "critical_path": f.critical_path.as_deref().unwrap_or(""),
                "main_path": f.main_path.as_deref().unwrap_or(""),
                "class_reason": super::review_severity::classify(f).reason(),
                "severity_checked": check.is_some(),
                "severity_check_reason": check.map(|c| c.reason.as_str()).unwrap_or(""),
                "severity_check_raised": check.is_some_and(|c| c.blocking),
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
/// What the verdict on this record rests on, and what holds it by name.
async fn note_review_state(
    state: &WorkerState,
    team_id: &str,
    record_sk: &str,
    rests_on: &super::review_fingerprint::Fingerprint,
    holding: &[String],
    verdict_reason: &str,
) {
    let as_json = |r: serde_json::Result<String>| r.unwrap_or_default();
    let upd = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(record_sk))
        .update_expression("SET fingerprint = :f, holding = :h, verdict_reason = :w")
        .expression_attribute_values(":f", attr_s(&as_json(serde_json::to_string(rests_on))))
        .expression_attribute_values(":h", attr_s(&as_json(serde_json::to_string(holding))))
        .expression_attribute_values(":w", attr_s(verdict_reason))
        .send()
        .await;
    if let Err(e) = upd {
        warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not store what the verdict rests on (the next ask reviews afresh)");
    }
}

/// Reads everything a verdict rests on (`review_fingerprint`). A part that
/// can't be read is `None`, which counts as changed.
async fn read_fingerprint(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReviewMessage,
    head: &str,
    pr_body: &str,
    base: &str,
) -> super::review_fingerprint::Fingerprint {
    use super::review_fingerprint as fp;
    let (owner, repo, pr) = (&msg.repo_owner, &msg.repo_name, msg.pr_number);
    let threads = github
        .list_bot_review_threads(owner, repo, pr)
        .await
        .ok()
        .map(|t| fp::threads_part(t.iter().map(|t| (t.id.clone(), t.resolved))));
    let head_runs = github.list_ci_for_ref(owner, repo, head).await.ok();
    let base_failing = if base.is_empty() {
        Some(Vec::new())
    } else {
        github
            .list_ci_for_ref(owner, repo, base)
            .await
            .ok()
            .map(|v| super::review_policy::failing_checks(&v))
    };
    let plan = plan_evidence_for_pr(state, msg).await;
    let results = match (head_runs, base_failing) {
        (Some(h), Some(b)) => Some(fp::results_part(&h, &b, &plan)),
        _ => None,
    };
    let comments = github.list_issue_comments(owner, repo, pr).await.ok();
    let inline = github.get_review_comments(owner, repo, pr).await.ok();
    let reviews = github.list_pr_reviews(owner, repo, pr).await.ok();
    let people = match (comments, inline, reviews) {
        (Some(mut all), Some(inline), Some(reviews)) => {
            all.extend(inline);
            all.extend(reviews.as_array().cloned().unwrap_or_default());
            Some(fp::people_part(&all))
        }
        _ => None,
    };
    fp::Fingerprint {
        head: head.to_string(),
        description: fp::description_part(pr_body),
        threads,
        results,
        people,
    }
}

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

/// Persist what a review used on its REVIEW# record. Its tokens and cost
/// reach the team's counters per model call (common::usage), and the PR is
/// counted where the review is stored (`store_review_record`), so nothing is
/// added to the team's analytics items here.
async fn record_review_usage(
    state: &WorkerState,
    team_id: &str,
    record_sk: &str,
    usage: &TokenUsage,
) {
    if !has_usage(usage) {
        return;
    }
    let cost = review_cost_str(usage);
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(record_sk))
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
    _state: &WorkerState,
    provider: &ModelProvider,
    comment: &str,
    standing: Option<&super::review_verdict::StandingReview>,
    last_reply: Option<&str>,
    outstanding: Option<&str>,
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
        provider.judgment_model_id(),
        review_agent::ASK_SYSTEM,
        &review_agent::ask_prompt(comment, &standing_line, last_reply, outstanding),
        &review_agent::ask_schema(),
        true,
        common::stop_judge::JUDGMENT_EFFORT,
        4096,
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
        provider.judgment_model_id(),
        review_agent::REPLY_CONCLUSION_SYSTEM,
        &review_agent::reply_conclusion_prompt(earlier_replies, question, answer),
        &review_agent::reply_conclusion_schema(),
        true,
        common::stop_judge::JUDGMENT_EFFORT,
        8192,
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

/// A resolved thread a person settled: they resolved it, or they answered
/// it and CoderHelm resolved it on that answer. Shown to a re-review so the
/// same concern isn't raised again.
pub(crate) fn settled_by_answer_or_person(t: &crate::clients::github::BotReviewThread) -> bool {
    t.resolved && (t.resolved_by_person || t.replies.iter().any(|r| !r.by_bot))
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
        .filter(|s| {
            s.is_settled()
                && s.index < prior.len()
                && super::review_rules::settles(&prior[s.index].body, &s.status)
                && seen.insert(s.index)
        })
        .map(|s| prior[s.index])
        .collect()
}

/// What the stop judge sees for a proposed request for changes: the PR's
/// title and description, the files it changes, every finding as evidence
/// (checked when it blocks and traces its mechanism), the verification run,
/// and the earlier threads with where each stands.
#[allow(clippy::too_many_arguments)]
fn block_picture(
    title: &str,
    pr_body: &str,
    compare: &serde_json::Value,
    findings: &[review_agent::Finding],
    verdict: &super::review_verdict::Verdict,
    prior_threads: &[&crate::clients::github::BotReviewThread],
    fixed_threads: &[&crate::clients::github::BotReviewThread],
    verify_failed: bool,
    verify_md: &str,
) -> super::stop_judge::Picture {
    use super::stop_judge::{Evidence, Kind, Picture, Thread};
    let mut p = Picture::new(
        Kind::Block,
        "review",
        &format!(
            "the review would request changes on: {}",
            verdict.names().join("; ")
        ),
    );
    p.ticket_title = title.to_string();
    p.ticket_body = pr_body.to_string();
    p.acceptance = super::stop_judge::acceptance(pr_body);
    p.diff_summary = compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| {
                    Some(format!(
                        "{} ({}, +{} -{})",
                        f["filename"].as_str()?,
                        f["status"].as_str().unwrap_or(""),
                        f["additions"].as_u64().unwrap_or(0),
                        f["deletions"].as_u64().unwrap_or(0)
                    ))
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    for f in findings {
        let class = super::review_severity::classify(f);
        p.evidence.push(Evidence {
            source: format!("review finding ({})", class.reason()),
            finding: format!("{}: {}", f.title, common::truncate_str(&f.body, 1_500)),
            location: if f.line > 0 {
                format!("{}:{}", f.file, f.line)
            } else {
                f.file.clone()
            },
            checked: class.blocks() && super::review_severity::traces_mechanism(f),
            ..Default::default()
        });
    }
    if verify_failed {
        p.evidence.push(Evidence {
            source: "verification run".into(),
            finding: format!(
                "the change's affected tests or build failed in the sandbox: {}",
                common::truncate_str(verify_md, 1_500)
            ),
            location: "the verification run".into(),
            checked: true,
            ..Default::default()
        });
    }
    for t in prior_threads {
        let fixed = fixed_threads.iter().any(|f| f.id == t.id);
        p.threads.push(Thread {
            summary: format!(
                "{}:{} {}",
                t.path,
                t.line,
                common::truncate_str(&t.body, 600)
            ),
            open: !fixed && !t.resolved,
            person_last: t.replies.last().is_some_and(|r| !r.by_bot),
            ..Default::default()
        });
    }
    p
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
            root_id: 0,
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
    fn a_thread_coderhelm_resolved_on_a_persons_answer_counts_as_settled() {
        use crate::clients::github::ThreadReply;
        let mut a = t("A");
        assert!(!settled_by_answer_or_person(&a));
        a.resolved = true;
        // Resolved by CoderHelm because the code fixed it: not a person's answer.
        assert!(!settled_by_answer_or_person(&a));
        a.replies.push(ThreadReply {
            author: "author".into(),
            by_bot: false,
            body: "Yes, it uses our own key.".into(),
        });
        assert!(settled_by_answer_or_person(&a));
        let mut b = t("B");
        b.resolved = true;
        b.resolved_by_person = true;
        assert!(settled_by_answer_or_person(&b));
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

/// The review's overview, held to what it is asked to be: a few sentences.
/// Models sometimes write essays with headings here; the details belong in
/// the findings, which are posted on the lines they are about.
fn short_overview(text: &str) -> String {
    const MAX_SENTENCES: usize = 4;
    const MAX_BYTES: usize = 700;
    let prose: String = text
        .lines()
        .map(str::trim)
        .filter(|l| {
            !l.is_empty() && !l.starts_with('#') && !l.starts_with('|') && !l.starts_with("```")
        })
        .collect::<Vec<_>>()
        .join(" ");
    common::prose::first_sentences(&prose, MAX_SENTENCES, MAX_BYTES).to_string()
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

/// A verdict review is superseded once the PR's head is no longer the commit it
/// was enqueued for; that commit's own review covers the latest code. Reviews
/// that answer a question, or that resolve the head themselves (empty
/// head_sha), always speak.
/// For a review started by a description edit: the hash of the description
/// the review reads, when it isn't the one the edit's webhook carried (that
/// one the gateway already marked). `None` for every other trigger.
fn description_to_mark(msg: &ReviewMessage, pr_body: &str) -> Option<String> {
    if msg.trigger != common::review_description::TRIGGER {
        return None;
    }
    let fresh = common::review_description::body_hash(pr_body);
    (msg.description_hash.as_deref() != Some(fresh.as_str())).then_some(fresh)
}

fn superseded(msg: &ReviewMessage, current_head: &str) -> bool {
    msg.question.is_none()
        && !msg.head_sha.is_empty()
        && !current_head.is_empty()
        && current_head != msg.head_sha
}

#[cfg(test)]
mod overview_tests {
    use super::short_overview;

    /// The reported cut: every dot counted as a sentence end, so the fourth
    /// "sentence" could stop inside a path.
    #[test]
    fn the_overview_never_stops_inside_a_path() {
        let text = "This moves the upload into services/worker/src/main.rs and reads \
                    lambda/handler.py from the source paths listed in deploy/config.yaml and \
                    infra/lib/stack.ts. Nothing else changes.";
        assert_eq!(short_overview(text), text);
    }

    #[test]
    fn the_overview_keeps_four_whole_sentences() {
        let out = short_overview("One. Two. Three. Four. Five.");
        assert_eq!(out, "One. Two. Three. Four.");
        // Too long for the cap: whole sentences, or the one sentence whole.
        let s = format!("{} end.", "word ".repeat(200));
        assert_eq!(short_overview(&s), s);
    }
}

#[cfg(test)]
mod superseded_tests {
    use super::{description_to_mark, moved_on, superseded};
    use crate::models::ReviewMessage;

    fn msg(head: &str, question: Option<&str>) -> ReviewMessage {
        ReviewMessage {
            limit_waits: 0,
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
            thread_root_id: None,
            mentioned: false,
            requested_by: String::new(),
            check_head: String::new(),
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
            outstanding_work: None,
            description_hash: None,
            full_review: false,
        }
    }

    #[test]
    fn a_description_review_marks_the_text_it_reads_when_it_is_newer() {
        use common::review_description::{body_hash, TRIGGER};
        let mut m = msg("cc15ab8", None);
        m.trigger = TRIGGER.into();
        m.description_hash = Some(body_hash("ran it on a restore: 4,210 rows"));
        // The text the webhook carried: the gateway already marked it.
        assert_eq!(
            description_to_mark(&m, "ran it on a restore:  4,210 rows"),
            None
        );
        // A later edit: the review reads and marks that text.
        assert_eq!(
            description_to_mark(&m, "ran it on a restore: 4,210 rows, 0 errors"),
            Some(body_hash("ran it on a restore: 4,210 rows, 0 errors"))
        );
        // Other triggers never mark a description.
        m.trigger = "synchronize".into();
        assert_eq!(description_to_mark(&m, "anything"), None);
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
        // No sentence end to cut on: kept, never cut mid-sentence.
        let long = "word ".repeat(400);
        assert_eq!(super::short_overview(&long), long.trim_end());
    }

    #[test]
    fn changed_files_lists_every_file() {
        let compare =
            serde_json::json!({"files": [{"filename": "src/a.ts"}, {"filename": "x.md"}]});
        assert_eq!(super::changed_files(&compare).len(), 2);
    }

    #[test]
    fn the_rating_footer_is_one_line_with_a_link() {
        let f = super::rating_footer("https://app.example.com", "acme", "web", 7);
        let last = f.trim().lines().last().unwrap();
        assert_eq!(f.trim().lines().count(), 2, "a rule and one line");
        assert!(last.contains("](https://app.example.com/reviewer/pr?repo=acme/web&pr=7)"));
        assert!(last.contains("👍 / 👎"));
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
            severity: "high".into(),
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
        assert_eq!(a[0]["severity"], "high");
        assert_eq!(a[0]["file"], "infra/redirects.tf");
        assert_eq!(a[0]["line"], 12);
        assert_eq!(a[0]["title"], "Redirect targets");
        assert_eq!(a[0]["body"], "The handler reads `id` before the check.");
        assert_eq!(a[1]["kind"], "concern");
        assert_eq!(a[1]["blocks"], false);
        assert_eq!(a[2]["kind"], "question");
        // A question is an FYI note, even one the change rests on.
        assert_eq!(a[2]["blocks"], false);
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
        // The same defect at medium severity still blocks: severity is only
        // an input. A preference the reviewer inferred is a note.
        let mut medium = finding("bug", Some("verified-defect"), "off by one");
        medium.severity = "medium".into();
        assert_eq!(decide_verdict(&[medium], 0, &[], false), "REQUEST_CHANGES");
        let pref = finding("convention", Some("verified-defect"), "name it `count`");
        assert_eq!(decide_verdict(&[pref], 0, &[], false), "APPROVE");
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
        // A required test missing blocks, a nit alongside it doesn't; a
        // failed verification run (the change's own tests failing) blocks.
        let mut tests = Finding {
            file: "src/a.ts".into(),
            category: "tests".into(),
            ..Default::default()
        };
        crate::passes::review_rules::apply(
            std::slice::from_mut(&mut tests),
            &crate::passes::review_rules::Policy::new(true, ""),
            &["src/a.ts".to_string()],
        );
        assert_eq!(
            decide_verdict(&[nit.clone(), tests], 0, &[], false),
            "REQUEST_CHANGES"
        );
        assert_eq!(decide_verdict(&[nit], 0, &[], false), "APPROVE");
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
            limit_waits: 0,
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
            thread_root_id: None,
            mentioned: false,
            requested_by: String::new(),
            check_head: String::new(),
            attempt: 0,
            wait_secs: 0,
            reconcile_note: None,
            outstanding_work: None,
            description_hash: None,
            full_review: false,
        }
    }

    #[test]
    fn a_review_record_names_the_session_that_wrote_it() {
        let job = LiveJob::start();
        // No session page yet: no link.
        assert_eq!(job.session_id(), None);
        job.session().opened = true;
        let id = job.session_id().unwrap();
        assert_eq!(id, job.id);
        let record = |session_id: Option<&str>| {
            review_record_item(
                &msg(),
                &ReviewRecord {
                    head_sha: "a1b2c3d4",
                    verdict: "APPROVE",
                    risk: "LOW",
                    body: "Looks right.",
                    posted_as: "APPROVE",
                    action_summary: "",
                    created_at: "2026-11-10T12:00:00+00:00",
                    session_id,
                },
            )
        };
        let (sk, item) = record(Some(&id));
        assert_eq!(sk, "REVIEW#o/r#000042#2026-11-10T12:00:00+00:00");
        assert_eq!(item["sk"].as_s().unwrap(), &sk);
        assert_eq!(item["verdict"].as_s().unwrap(), "APPROVE");
        assert!(!item.contains_key("action_summary"));
        // What the review page reads back is the job's id.
        assert_eq!(
            common::review_session::session_id_of(&item).as_deref(),
            Some(id.as_str())
        );
        // A job without a session page writes no link.
        let (_, item) = record(None);
        assert!(!item.contains_key(common::review_session::REVIEW_SESSION_ID));
        assert_eq!(common::review_session::session_id_of(&item), None);
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

    /// The incident: a review failed on GitHub's rate limit at 20:07, was
    /// retried at 20:17 and 20:32 into the same limit, and gave up with a
    /// "service error" note. A rate limit now waits for the reset, spends no
    /// attempt, and never ends in the gave-up note.
    #[test]
    fn a_rate_limited_review_waits_for_the_reset_and_never_gives_up_noisily() {
        let mut m = msg();
        // Two attempts already spent on real service errors.
        m.attempt = 2;
        let reset_in = 1_700;
        let plan = failure_plan_for(&m, Some(reset_in));
        let FailurePlan::WaitForLimit { delay_secs } = plan else {
            panic!("{plan:?}");
        };
        assert!(delay_secs > reset_in, "after the reset, not before");
        assert!(delay_secs <= reset_in + 10);
        let (next, hop) = limit_wait_message(&m, "a1b2c3d4", delay_secs);
        assert_eq!(next.attempt, 2, "a rate limit spends no attempt");
        assert_eq!(next.limit_waits, 1);
        assert_eq!(hop, SQS_MAX_DELAY_SECS, "one SQS delay at a time");
        assert_eq!(hop + next.wait_secs, delay_secs);
        assert_eq!(next.dedup_key, "limit#1#head#a1b2c3d4");
        // Waiting again keeps the trigger's key, never stacks prefixes.
        let (again, _) = limit_wait_message(&next, "a1b2c3d4", 60);
        assert_eq!(again.dedup_key, "limit#2#head#a1b2c3d4");
        // A service error after a wait is still one of the 3 retries.
        let (retry, _) = retry_message(&again, "a1b2c3d4", 120);
        assert_eq!(retry.dedup_key, "retry#3#head#a1b2c3d4");
        assert_eq!(failure_plan_for(&retry, None), FailurePlan::GiveUp);
        // The limit holding through every wait ends quietly: never GiveUp,
        // the only plan that posts the gave-up note.
        let mut long = msg();
        long.limit_waits = MAX_LIMIT_WAITS;
        assert_eq!(
            failure_plan_for(&long, Some(60)),
            FailurePlan::LimitOutlasted
        );
        for waits in 0..=MAX_LIMIT_WAITS {
            for attempt in 0..5 {
                let mut x = msg();
                x.limit_waits = waits;
                x.attempt = attempt;
                assert_ne!(failure_plan_for(&x, Some(30)), FailurePlan::GiveUp);
            }
        }
        // The gave-up note is only sent from the service-error path.
        let src = include_str!("review_pr.rs");
        let f = src.find("\nasync fn review_failed(").unwrap();
        let body = &src[f..f + src[f..]
            .find("\nasync fn review_failed_for_service(")
            .unwrap()];
        assert!(!body.contains("gave_up_note("));
        assert!(body.contains("review_failed_for_service("));
    }

    #[test]
    fn a_rate_limit_is_read_from_the_error_or_the_installation() {
        let limited: Box<dyn std::error::Error + Send + Sync> = Box::new(
            crate::clients::github::GitHubPaused::after(common::github_wait::Cause::RateLimit, 900),
        );
        assert!(limit_wait_of(&*limited, 1).is_some_and(|w| w >= 900));
        let outage: Box<dyn std::error::Error + Send + Sync> = Box::new(
            crate::clients::github::GitHubPaused::after(common::github_wait::Cause::Outage, 60),
        );
        assert_eq!(limit_wait_of(&*outage, 777_000_001), None);
        let other: Box<dyn std::error::Error + Send + Sync> = "model overloaded".into();
        assert_eq!(limit_wait_of(&*other, 777_000_002), None);
        crate::clients::github::mark_limited(
            777_000_003,
            chrono::Utc::now().timestamp() as u64 + 400,
        );
        assert!(limit_wait_of(&*other, 777_000_003).is_some_and(|w| w <= 400));
        let (title, summary) =
            crate::passes::review_session::limit_wait(&common::github_wait::waiting_text(
                common::github_wait::Cause::RateLimit,
                1_793_999_100,
            ));
        assert_eq!(
            title,
            "Review paused: Waiting for GitHub's hourly limit to reset at 21:05 UTC; continues by itself"
        );
        assert!(summary.contains("nothing failed"));
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
                    change: c.verdict_change,
                    more_than_words: true,
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
