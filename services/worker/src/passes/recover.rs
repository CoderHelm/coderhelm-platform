//! One recovery path for every pass error, before anything is marked failed.
//!
//! A pass's error is read by `common::run_recovery` (transient, a state
//! CoderHelm repairs, something only a person has, or unknown) and
//! [`recover`] acts on it:
//!
//! - transient: the same run tries again in a fresh step after a backoff;
//! - repairable: the repair runs (the checkpoint back to the plan for a
//!   branch that isn't in the run's repository, the run's own missing branch
//!   created from its base, a deleted base moved to the default branch, the
//!   base merged into a branch behind it, its own closed pull request
//!   reopened, a missing label created, a new token), then the same pass
//!   resumes from its checkpoint: never planned again, and no external write
//!   is repeated (branches start with `start_branch`, pull requests are
//!   found by their branch before one is opened, follow-ups are deduped);
//! - the same error again with no progress takes the next strategy, never
//!   the same one twice; the run's spend, step and pass ceilings still bound
//!   every try (`send_next_step`);
//! - needs a person, an unknown error met again after its one retry, or a
//!   repair with no strategy left: the run stops as "needs you", naming what
//!   the person does, in plain words.
//!
//! The orchestrator calls [`after_error`] for every error a ticket run's
//! passes return, and the self-heal sweep ([`self_heal_sweep`], on deploy
//! and every 15 minutes) calls [`recover`] for runs left failed on an error
//! that is transient or repairable, once per run per deploy.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::run_recovery::{
    self as rr, Classified, Ledger, NeedsYou, Next, Repair, Strategy, FIELD_LEDGER,
    FIELD_RESUME_MESSAGE,
};
use tracing::{info, warn};

use super::{add_progress_note, attr_n, attr_s, steps};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// What a repair reads and writes on GitHub.
#[async_trait::async_trait]
pub trait RepairHost: Send + Sync {
    /// `Ok(false)` only when GitHub says the branch isn't there.
    async fn ref_exists(&self, owner: &str, repo: &str, branch: &str) -> Result<bool, String>;
    async fn default_branch(&self, owner: &str, repo: &str) -> Result<String, String>;
    async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from: &str,
    ) -> Result<(), String>;
    /// Merges `base` into `branch`: `Ok(false)` on a conflict.
    async fn merge_base_into(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        base: &str,
    ) -> Result<bool, String>;
    async fn reopen_pr(&self, owner: &str, repo: &str, number: u64) -> Result<(), String>;
    async fn create_label(&self, owner: &str, repo: &str, name: &str) -> Result<(), String>;
    async fn refresh_token(&self);
}

/// What a repair reads and writes on the run.
#[async_trait::async_trait]
pub trait RunStore: Send + Sync {
    async fn ledger(&self) -> Ledger;
    async fn save_ledger(&self, ledger: &Ledger);
    /// The run's checkpoint: its pass and branch.
    async fn checkpoint(&self) -> Option<(String, String)>;
    /// The checkpoint goes back to the plan.
    async fn back_to_plan(&self);
    async fn note(&self, text: &str);
}

/// Where the run is when the error comes.
#[derive(Debug, Clone)]
pub struct At<'a> {
    pub owner: &'a str,
    pub repo: &'a str,
    pub base: &'a str,
    /// `run_recovery::progress_mark`.
    pub mark: &'a str,
    pub now: &'a str,
}

/// What the run does after [`recover`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovered {
    /// The same pass goes on from its checkpoint after `delay_secs`;
    /// `base` when a repair moved the run onto another base branch.
    Resume {
        delay_secs: u32,
        base: Option<String>,
        note: String,
    },
    NeedsYou(NeedsYou),
}

/// A repair that worked: what it did, and a new base when it moved one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repaired {
    pub did: String,
    pub base: Option<String>,
}

/// Acts on one error (see the module comment).
pub async fn recover(
    host: &dyn RepairHost,
    store: &dyn RunStore,
    c: &Classified,
    at: &At<'_>,
) -> Recovered {
    let mut ledger = store.ledger().await;
    match rr::next(c, &ledger, at.mark) {
        Next::NeedsYou(n) => {
            store
                .note(&format!("Stopped for a person: {} {}.", n.detail, n.action))
                .await;
            Recovered::NeedsYou(n)
        }
        Next::Retry { delay_secs } => {
            ledger.record(c, "retry", at.mark, at.now);
            store.save_ledger(&ledger).await;
            let note = format!(
                "{} Trying the same pass again from its checkpoint in {delay_secs} seconds.",
                c.plain
            );
            store.note(&note).await;
            Recovered::Resume {
                delay_secs,
                base: None,
                note,
            }
        }
        Next::Repair { repair, strategy } => {
            ledger.record(c, strategy.code(), at.mark, at.now);
            store.save_ledger(&ledger).await;
            match repair_with(host, store, &repair, strategy, at).await {
                Ok(done) => {
                    let note = format!(
                        "{} Repaired: {}. The pass goes on from its checkpoint.",
                        c.plain, done.did
                    );
                    store.note(&note).await;
                    Recovered::Resume {
                        delay_secs: steps::STEP_DELAY_SECS.max(0) as u32,
                        base: done.base,
                        note,
                    }
                }
                Err(why) => {
                    // The next try at the same mark takes the next strategy.
                    let note = format!(
                        "{} The repair ({}) didn't work: {why}. The next try takes another way.",
                        c.plain,
                        strategy.code().replace('_', " ")
                    );
                    store.note(&note).await;
                    Recovered::Resume {
                        delay_secs: rr::backoff_secs(0),
                        base: None,
                        note,
                    }
                }
            }
        }
    }
}

/// Whether `branch` names a branch CoderHelm made (its own prefix).
fn own_branch(branch: &str) -> bool {
    branch.starts_with("coderhelm/") || branch.starts_with("coderhelm-")
}

async fn repair_with(
    host: &dyn RepairHost,
    store: &dyn RunStore,
    repair: &Repair,
    strategy: Strategy,
    at: &At<'_>,
) -> Result<Repaired, String> {
    let did = |s: String| Ok(Repaired { did: s, base: None });
    match (repair, strategy) {
        (
            Repair::MissingRef {
                owner,
                repo,
                base,
                head,
            },
            _,
        ) => {
            // The repository the error names, else the run's.
            let (o, r) = if owner.is_empty() {
                (at.owner, at.repo)
            } else {
                (owner.as_str(), repo.as_str())
            };
            let base = if base.is_empty() {
                at.base
            } else {
                base.as_str()
            };
            // A base that is gone: the repository's default branch.
            if !base.is_empty() && !host.ref_exists(o, r, base).await? {
                let default = host.default_branch(o, r).await?;
                if default.is_empty() || default == base || !host.ref_exists(o, r, &default).await?
                {
                    return Err(format!("{o}/{r} has no other base branch to use"));
                }
                return Ok(Repaired {
                    did: format!(
                        "the base branch `{base}` is gone from {o}/{r}, so the run goes on against \
                         its default branch `{default}`"
                    ),
                    base: Some(default),
                });
            }
            match strategy {
                Strategy::ReconcileCheckpoint => {
                    let (pass, branch) = store.checkpoint().await.unwrap_or_default();
                    let branch_here =
                        branch.is_empty() || host.ref_exists(at.owner, at.repo, &branch).await?;
                    if !branch_here || (!head.is_empty() && branch == *head) {
                        store.back_to_plan().await;
                        return did(format!(
                            "the checkpoint ({pass}) named `{}`, a branch that isn't in {}/{}; the \
                             pass starts again from the plan on {}'s own branch, created before \
                             anything compares it",
                            if branch.is_empty() { head.as_str() } else { branch.as_str() },
                            at.owner,
                            at.repo,
                            at.repo
                        ));
                    }
                    if !head.is_empty() && host.ref_exists(o, r, head).await? {
                        return did(format!("`{head}` is in {o}/{r} now"));
                    }
                    Err("the checkpoint names a branch that is there".into())
                }
                Strategy::RecreateBranch => {
                    if head.is_empty() || !own_branch(head) {
                        return Err(format!(
                            "`{head}` isn't a branch CoderHelm made, so it isn't created"
                        ));
                    }
                    if host.ref_exists(o, r, head).await? {
                        return did(format!("`{head}` is in {o}/{r} now"));
                    }
                    host.create_branch(o, r, head, base).await?;
                    did(format!("created `{head}` in {o}/{r} from `{base}`"))
                }
                _ => {
                    store.back_to_plan().await;
                    did("the pass starts again from the plan".into())
                }
            }
        }
        (
            Repair::BranchBehind {
                owner,
                repo,
                branch,
            },
            Strategy::UpdateFromBase,
        ) => {
            let (o, r) = if owner.is_empty() {
                (at.owner, at.repo)
            } else {
                (owner.as_str(), repo.as_str())
            };
            let branch = if branch.is_empty() {
                store.checkpoint().await.unwrap_or_default().1
            } else {
                branch.clone()
            };
            if branch.is_empty() {
                return Err("no branch to bring up to date".into());
            }
            if host.merge_base_into(o, r, &branch, at.base).await? {
                did(format!("merged `{}` into `{branch}`", at.base))
            } else {
                Err(format!("`{branch}` conflicts with `{}`", at.base))
            }
        }
        (
            Repair::ClosedOwnPr {
                owner,
                repo,
                number,
            },
            Strategy::ReopenPr,
        ) => {
            if *number == 0 {
                return Err("the error didn't name the pull request".into());
            }
            let (o, r) = if owner.is_empty() {
                (at.owner, at.repo)
            } else {
                (owner.as_str(), repo.as_str())
            };
            host.reopen_pr(o, r, *number).await?;
            did(format!("reopened pull request #{number}"))
        }
        (Repair::MissingLabel { owner, repo, name }, Strategy::CreateLabel) => {
            if name.is_empty() {
                return Err("the error didn't name the label".into());
            }
            let (o, r) = if owner.is_empty() {
                (at.owner, at.repo)
            } else {
                (owner.as_str(), repo.as_str())
            };
            host.create_label(o, r, name).await?;
            did(format!("created the label `{name}`"))
        }
        (Repair::ExpiredToken, _) => {
            host.refresh_token().await;
            did("fetched a new access token".into())
        }
        (_, _) => {
            store.back_to_plan().await;
            did("the pass starts again from the plan".into())
        }
    }
}

// ── GitHub and the run record ───────────────────────────────────────────────

#[async_trait::async_trait]
impl RepairHost for GitHubClient {
    async fn ref_exists(&self, owner: &str, repo: &str, branch: &str) -> Result<bool, String> {
        GitHubClient::ref_exists(self, owner, repo, branch)
            .await
            .map_err(|e| e.to_string())
    }
    async fn default_branch(&self, owner: &str, repo: &str) -> Result<String, String> {
        self.get_default_branch(owner, repo)
            .await
            .map_err(|e| e.to_string())
    }
    async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from: &str,
    ) -> Result<(), String> {
        self.start_branch(owner, repo, branch, from)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn merge_base_into(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        base: &str,
    ) -> Result<bool, String> {
        self.merge_branch(
            owner,
            repo,
            branch,
            base,
            &format!("Merge {base} into {branch}"),
        )
        .await
        .map_err(|e| e.to_string())
    }
    async fn reopen_pr(&self, owner: &str, repo: &str, number: u64) -> Result<(), String> {
        self.reopen_pull_request(owner, repo, number)
            .await
            .map_err(|e| e.to_string())
    }
    async fn create_label(&self, owner: &str, repo: &str, name: &str) -> Result<(), String> {
        GitHubClient::create_label(self, owner, repo, name)
            .await
            .map_err(|e| e.to_string())
    }
    async fn refresh_token(&self) {
        self.forget_token();
    }
}

/// The run record and its checkpoint in DynamoDB.
pub struct DynamoRun<'a> {
    pub state: &'a WorkerState,
    pub team_id: &'a str,
    pub run_id: &'a str,
}

#[async_trait::async_trait]
impl RunStore for DynamoRun<'_> {
    async fn ledger(&self) -> Ledger {
        self.state
            .dynamo
            .get_item()
            .table_name(&self.state.config.runs_table_name)
            .key("team_id", attr_s(self.team_id))
            .key("run_id", attr_s(self.run_id))
            .projection_expression("#l")
            .expression_attribute_names("#l", FIELD_LEDGER)
            .send()
            .await
            .ok()
            .and_then(|o| {
                o.item()
                    .and_then(|i| i.get(FIELD_LEDGER))
                    .and_then(|v| v.as_s().ok())
                    .map(|s| Ledger::from_json(s))
            })
            .unwrap_or_default()
    }
    async fn save_ledger(&self, ledger: &Ledger) {
        if let Err(e) = self
            .state
            .dynamo
            .update_item()
            .table_name(&self.state.config.runs_table_name)
            .key("team_id", attr_s(self.team_id))
            .key("run_id", attr_s(self.run_id))
            .update_expression("SET #l = :l")
            .condition_expression("attribute_exists(run_id)")
            .expression_attribute_names("#l", FIELD_LEDGER)
            .expression_attribute_values(":l", attr_s(&ledger.to_json()))
            .send()
            .await
        {
            warn!(run_id = self.run_id, error = %e, "Could not keep the run's recovery attempts");
        }
    }
    async fn checkpoint(&self) -> Option<(String, String)> {
        super::load_checkpoint(self.state, self.team_id, self.run_id)
            .await
            .map(|(p, b, _)| (p, b))
    }
    async fn back_to_plan(&self) {
        super::checkpoint_back_to_plan(self.state, self.team_id, self.run_id).await;
    }
    async fn note(&self, text: &str) {
        add_progress_note(self.state, self.team_id, self.run_id, text).await;
    }
}

/// Whether GitHub says `branch` isn't in `owner/repo` (any other answer,
/// an error included, is not "missing": nothing is reset on a guess).
pub async fn branch_missing(github: &GitHubClient, owner: &str, repo: &str, branch: &str) -> bool {
    if owner.is_empty() || repo.is_empty() {
        return false;
    }
    matches!(github.ref_exists(owner, repo, branch).await, Ok(false))
}

/// The run's note when its checkpoint names a branch the repository
/// doesn't have.
pub fn stale_checkpoint_note(branch: &str, owner: &str, repo: &str) -> String {
    format!(
        "The checkpoint names `{branch}`, a branch that isn't in {owner}/{repo} (another \
         repository's, or never pushed): the pass starts again from the plan on this \
         repository's own branch"
    )
}

/// What [`after_error`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterError {
    /// The same run goes on in a fresh step (after a backoff or a repair).
    Continued,
    /// Only a person can get past it: the caller stops the run for them
    /// ([`stop_for_person`]) unless a held draft takes the work.
    NeedsYou(NeedsYou),
    /// Nothing done (the run already ended, or no step could be queued):
    /// the caller's one ending applies (`fail_run`, which reads the error
    /// the same way).
    NotHandled,
}

/// The one recovery path for an error a ticket run's passes returned.
pub async fn after_error(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    err: &(dyn std::error::Error + 'static),
    checkpoint: &Option<(String, String, u8)>,
) -> AfterError {
    if !still_running(state, &msg.team_id, run_id).await {
        return AfterError::NotHandled;
    }
    let c = rr::classify(&common::run_fail::error_text(err));
    let github = match GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        Ok(g) => g,
        Err(_) => return AfterError::NotHandled,
    };
    let step_mark = steps::load(state, &msg.team_id, run_id)
        .await
        .mark
        .unwrap_or_default();
    let mark = rr::progress_mark(
        checkpoint.as_ref().map_or("", |(p, _, _)| p.as_str()),
        &step_mark,
    );
    let now = chrono::Utc::now().to_rfc3339();
    let store = DynamoRun {
        state,
        team_id: &msg.team_id,
        run_id,
    };
    info!(run_id, kind = c.kind.code(), signature = %c.signature, "A pass error: recovering");
    let at = At {
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        base: &msg.base_branch,
        mark: &mark,
        now: &now,
    };
    match recover(&github, &store, &c, &at).await {
        Recovered::NeedsYou(n) => AfterError::NeedsYou(n),
        Recovered::Resume {
            delay_secs,
            base,
            note,
        } => {
            let mut next = msg.clone();
            if let Some(b) = base {
                next.base_branch = b;
            }
            let why = common::truncate_str(&note, 300).to_string();
            match super::send_next_step(state, &next, run_id, &why, false, delay_secs as i32).await
            {
                super::Handoff::Continued => AfterError::Continued,
                super::Handoff::Stopped(why) => {
                    warn!(run_id, %why, "Recovery couldn't queue the next step");
                    AfterError::NotHandled
                }
            }
        }
    }
}

async fn still_running(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|o| {
            o.item()
                .and_then(|i| i.get("status"))
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .is_some_and(|s| s == common::run_status::RUNNING)
}

/// Stops the run as "needs you": the one action a person takes and why, in
/// plain words, never a raw error. Written by the one ending write
/// (`common::run_fail`), with the decision the recovery path made.
pub async fn stop_for_person(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    needs: &NeedsYou,
    usage: &TokenUsage,
    duration: u64,
) {
    let delta = usage.unrecorded();
    let failure = common::run_fail::RunFailure {
        reason: &needs.detail,
        pass: None,
        only_from: None,
        extra: vec![("duration_s", attr_n(duration))],
        add: super::usage_adds(&delta),
        needs: Some(needs),
    };
    match common::run_fail::fail_run(
        &state.dynamo,
        &state.config.runs_table_name,
        &msg.team_id,
        run_id,
        &failure,
    )
    .await
    {
        Ok(true) => {
            usage.mark_recorded(&delta);
            super::webhooks::emit_run(state, &msg.team_id, run_id, "needs_input").await;
        }
        Ok(false) => {}
        Err(e) => warn!(run_id, error = %e, "Could not stop the run for a person"),
    }
}

/// Keeps the continuation the self-heal sweep sends on the run record (the
/// same run, one step on, from its checkpoint and own plan). A message too
/// large for the record is left out: the sweep builds one from the record.
pub async fn keep_resume_message(state: &WorkerState, msg: &TicketMessage, run_id: &str) {
    let first = if msg.first_attempt_ms > 0 {
        msg.first_attempt_ms
    } else {
        chrono::Utc::now().timestamp_millis().max(0) as u64
    };
    let next = steps::next_message(msg, run_id, first);
    let Ok(body) = serde_json::to_string(&crate::models::WorkerMessage::Ticket(next)) else {
        return;
    };
    if body.len() > MAX_RESUME_MESSAGE_BYTES {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #m = :m")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_names("#m", FIELD_RESUME_MESSAGE)
        .expression_attribute_values(":m", attr_s(&body))
        .send()
        .await;
}

/// The largest continuation kept on a run record.
const MAX_RESUME_MESSAGE_BYTES: usize = 100_000;

// ── The self-heal sweep ─────────────────────────────────────────────────────

/// How far back the sweep looks for failed runs.
pub const SWEEP_DAYS: i64 = 14;
/// Runs the sweep resumes in one pass at most.
pub const SWEEP_MAX: usize = 50;

/// What the sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    pub failed: usize,
    /// Failed runs now waiting for another change.
    pub waiting: usize,
    pub resumed: usize,
    pub handed: usize,
    pub skipped: usize,
}

/// The first start of each release (its code version) sends one self-heal
/// sweep: one conditional write claims the release, so of all the workers
/// starting at once only one sends it. Every 15 minutes the schedule sends
/// it too.
pub async fn on_new_release(state: &WorkerState) {
    if state.config.checkpoints_table_name.is_empty() || state.config.ticket_queue_url.is_empty() {
        return;
    }
    let version = super::proof::checks_version();
    let ttl = chrono::Utc::now().timestamp().max(0) as u64 + 30 * 86_400;
    let claimed = state
        .dynamo
        .put_item()
        .table_name(&state.config.checkpoints_table_name)
        .item("team_id", attr_s(RELEASE_KEY))
        .item("sk", attr_s(&format!("SELF_HEAL#{version}")))
        .item("ttl", attr_n(ttl))
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await
        .is_ok();
    if !claimed {
        return;
    }
    let body = serde_json::json!({ "type": "self_heal_sweep" }).to_string();
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(60)
        .send()
        .await
    {
        Ok(_) => info!(version, "New release: the self-heal sweep is queued"),
        Err(e) => warn!(error = %e, "New release: the self-heal sweep couldn't be queued"),
    }
}

/// The partition holding the release claims (no team has this id).
const RELEASE_KEY: &str = "SYSTEM#releases";

/// How a failed run the sweep found is resumed.
#[derive(Debug, Clone)]
pub enum Resend {
    /// A ticket step: the same run, from its checkpoint and own plan.
    Ticket(Box<TicketMessage>),
    /// A run with its pull request open: its resume (CI, reviews).
    Resume { pr_number: u64 },
}

fn item_s<'a>(item: &'a HashMap<String, AttributeValue>, k: &str) -> &'a str {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("")
}

fn item_n(item: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// The error a failed run stopped on, read the way the run would read it.
pub fn failed_run_error(item: &HashMap<String, AttributeValue>) -> Classified {
    let detail = item_s(item, rr::FIELD_ERROR_DETAIL);
    let text = if detail.is_empty() {
        format!(
            "{} {}",
            item_s(item, "error_message"),
            item_s(item, "error")
        )
    } else {
        detail.to_string()
    };
    rr::classify(text.trim())
}

/// The message that resumes a failed run from its record: the kept
/// continuation, else one built from the record (the same run, one step
/// on). A run with its pull request open resumes as a resume.
pub fn resend_for(item: &HashMap<String, AttributeValue>) -> Option<Resend> {
    let run_id = item_s(item, "run_id");
    let team_id = item_s(item, "team_id");
    if run_id.is_empty() || team_id.is_empty() {
        return None;
    }
    let pr_number = item_n(item, "pr_number");
    if pr_number > 0 {
        return Some(Resend::Resume { pr_number });
    }
    let kept = item_s(item, FIELD_RESUME_MESSAGE);
    if !kept.is_empty() {
        if let Ok(crate::models::WorkerMessage::Ticket(t)) = serde_json::from_str(kept) {
            return Some(Resend::Ticket(Box::new(t)));
        }
    }
    let (owner, name) = item_s(item, "repo").split_once('/')?;
    let source = match item_s(item, "ticket_source") {
        "jira" => crate::models::TicketSource::Jira,
        "alert" => crate::models::TicketSource::Alert,
        _ => crate::models::TicketSource::Github,
    };
    let step = item_n(item, "run_step") as u32;
    Some(Resend::Ticket(Box::new(TicketMessage {
        team_id: team_id.to_string(),
        installation_id: item_n(item, "installation_id"),
        source,
        ticket_id: item_s(item, "ticket_id").to_string(),
        title: item_s(item, "title").to_string(),
        body: item_s(item, "ticket_body").to_string(),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        issue_number: item_n(item, "issue_number"),
        sender: item_s(item, "sender").to_string(),
        base_branch: item_s(item, "base_branch").to_string(),
        continuation: step.max(1).saturating_add(1),
        continuation_run_id: Some(run_id.to_string()),
        ..Default::default()
    })))
}

/// Runs the sweep resumes: failed, on an error that is now transient or
/// repairable, and not already resumed by the sweep at `version`.
pub fn sweep_wants(item: &HashMap<String, AttributeValue>, version: &str) -> bool {
    if item_s(item, "status") != common::run_status::FAILED {
        return false;
    }
    let ledger = Ledger::from_json(item_s(item, FIELD_LEDGER));
    !ledger.healed_at_version(version) && failed_run_error(item).kind.resumable()
}

/// On deploy and every 15 minutes: every run left failed on an error that
/// is now transient or repairable is resumed from its checkpoint, through
/// the same [`recover`] a running run takes, at most once per run per
/// deploy (the code version the sweep resumed it at is kept on its ledger).
pub async fn self_heal_sweep(state: &WorkerState) -> SweepReport {
    let version = super::proof::checks_version();
    let mut report = SweepReport::default();
    let since = (chrono::Utc::now() - chrono::Duration::days(SWEEP_DAYS)).to_rfc3339();
    // Each team's runs, read by key to the last page: a capped scan of the
    // runs table stopped before the later runs.
    let filter = super::team_runs::Filter {
        expr: "#s = :failed AND updated_at >= :since",
        names: vec![("#s", "status")],
        values: vec![
            (":failed", attr_s(common::run_status::FAILED)),
            (":since", attr_s(&since)),
        ],
        projection: None,
    };
    let found = super::team_runs::matching(state, "Self-heal sweep", &filter).await;
    report.failed = found.len();
    for item in found {
        if report.resumed + report.handed >= SWEEP_MAX {
            break;
        }
        // A run that failed because its plan "waits for nothing" while it
        // waits for another change: it waits for that change, with no new
        // plan before it merges.
        if waits_for_another_change(&item, version) {
            if claim_for_version(state, &item, version).await
                && super::change_wait::wait_failed_run(state, &item).await
            {
                report.waiting += 1;
            } else {
                report.skipped += 1;
            }
            continue;
        }
        if !sweep_wants(&item, version) {
            report.skipped += 1;
            continue;
        }
        match heal_one(state, &item, version).await {
            Some(true) => report.resumed += 1,
            Some(false) => report.handed += 1,
            None => report.skipped += 1,
        }
    }
    // Runs waiting for another change: their dependency read again.
    super::change_wait::check_all(state, None).await;
    info!(
        failed = report.failed,
        waiting = report.waiting,
        resumed = report.resumed,
        handed = report.handed,
        skipped = report.skipped,
        "Self-heal sweep done"
    );
    report
}

/// A failed run whose plan stopped because what it changes doesn't exist
/// yet (another change makes it), not already handled at `version`.
pub fn waits_for_another_change(item: &HashMap<String, AttributeValue>, version: &str) -> bool {
    item_s(item, "status") == common::run_status::FAILED
        && !Ledger::from_json(item_s(item, FIELD_LEDGER)).healed_at_version(version)
        && !failed_run_error(item).kind.resumable()
        && common::change_wait::target_missing(item_s(item, "error_message"))
}

/// Marks the run handled at `version` (one conditional write).
async fn claim_for_version(
    state: &WorkerState,
    item: &HashMap<String, AttributeValue>,
    version: &str,
) -> bool {
    let mut ledger = Ledger::from_json(item_s(item, FIELD_LEDGER));
    let before = ledger.to_json();
    ledger.mark_healed(version);
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(item_s(item, "team_id")))
        .key("run_id", attr_s(item_s(item, "run_id")))
        .update_expression("SET #l = :l")
        .condition_expression("#s = :failed AND (attribute_not_exists(#l) OR #l = :before)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#l", FIELD_LEDGER)
        .expression_attribute_values(":failed", attr_s(common::run_status::FAILED))
        .expression_attribute_values(":l", attr_s(&ledger.to_json()))
        .expression_attribute_values(":before", attr_s(&before))
        .send()
        .await
        .is_ok()
}

/// Claims the run for this deploy (once per run per deploy) and recovers
/// it. `Some(true)`: resumed; `Some(false)`: stopped for a person; `None`:
/// left as it is.
async fn heal_one(
    state: &WorkerState,
    item: &HashMap<String, AttributeValue>,
    version: &str,
) -> Option<bool> {
    let team_id = item_s(item, "team_id");
    let run_id = item_s(item, "run_id");
    let mut ledger = Ledger::from_json(item_s(item, FIELD_LEDGER));
    let before = ledger.to_json();
    ledger.mark_healed(version);
    // The claim: one conditional write, so two sweeps never both resume it.
    let claimed = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #l = :l")
        .condition_expression("#s = :failed AND (attribute_not_exists(#l) OR #l = :before)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#l", FIELD_LEDGER)
        .expression_attribute_values(":failed", attr_s(common::run_status::FAILED))
        .expression_attribute_values(":l", attr_s(&ledger.to_json()))
        .expression_attribute_values(":before", attr_s(&before))
        .send()
        .await
        .is_ok();
    if !claimed {
        return None;
    }
    let resend = resend_for(item)?;
    let (owner, repo) = item_s(item, "repo").split_once('/')?;
    let installation = item_n(item, "installation_id");
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation,
        &state.http,
    )
    .ok()?;
    let checkpoint = super::load_checkpoint(state, team_id, run_id).await;
    let step_mark = steps::load(state, team_id, run_id)
        .await
        .mark
        .unwrap_or_default();
    let mark = rr::progress_mark(
        checkpoint.as_ref().map_or("", |(p, _, _)| p.as_str()),
        &step_mark,
    );
    let now = chrono::Utc::now().to_rfc3339();
    let c = failed_run_error(item);
    let store = DynamoRun {
        state,
        team_id,
        run_id,
    };
    let base = item_s(item, "base_branch");
    let at = At {
        owner,
        repo,
        base: if base.is_empty() { "main" } else { base },
        mark: &mark,
        now: &now,
    };
    match recover(&github, &store, &c, &at).await {
        Recovered::NeedsYou(n) => {
            let msg = match &resend {
                Resend::Ticket(t) => (**t).clone(),
                Resend::Resume { .. } => TicketMessage {
                    team_id: team_id.to_string(),
                    ..Default::default()
                },
            };
            stop_for_person(state, &msg, run_id, &n, &TokenUsage::default(), 0).await;
            Some(false)
        }
        Recovered::Resume {
            delay_secs,
            base: new_base,
            ..
        } => {
            let body = match resend {
                Resend::Ticket(mut t) => {
                    if let Some(b) = new_base {
                        t.base_branch = b;
                    }
                    serde_json::to_string(&crate::models::WorkerMessage::Ticket(*t)).ok()?
                }
                Resend::Resume { pr_number } => {
                    info!(
                        run_id,
                        pr_number, "Self-heal: resuming the run on its pull request"
                    );
                    // Back to waiting on its checks, where the resume picks
                    // the run up.
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.runs_table_name)
                        .key("team_id", attr_s(team_id))
                        .key("run_id", attr_s(run_id))
                        .update_expression("SET #s = :a, status_run_id = :sri, current_pass = :a")
                        .condition_expression("#s = :failed")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":a", attr_s(common::run_status::AWAITING_CI))
                        .expression_attribute_values(
                            ":sri",
                            attr_s(&format!("{}#{run_id}", common::run_status::AWAITING_CI)),
                        )
                        .expression_attribute_values(":failed", attr_s(common::run_status::FAILED))
                        .send()
                        .await;
                    serde_json::json!({
                        "type": "resume",
                        "team_id": team_id,
                        "run_id": run_id,
                        "installation_id": installation,
                    })
                    .to_string()
                }
            };
            let sent = state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(body)
                .delay_seconds((delay_secs as i32).clamp(0, 900))
                .send()
                .await
                .is_ok();
            if sent {
                add_progress_note(
                    state,
                    team_id,
                    run_id,
                    "Resumed by itself after a release: the error it stopped on is one CoderHelm \
                     gets past, so the run goes on from its checkpoint",
                )
                .await;
            }
            Some(sent)
        }
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    //! A GitHub and a run record in memory, for the replay tests.
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct FakeHost {
        /// `owner/repo` to its branches.
        pub branches: Mutex<BTreeMap<String, BTreeSet<String>>>,
        pub default: Mutex<BTreeMap<String, String>>,
        pub created: Mutex<Vec<String>>,
        pub merged: Mutex<Vec<String>>,
        pub reopened: Mutex<Vec<u64>>,
        pub labels: Mutex<Vec<String>>,
        pub tokens: Mutex<u32>,
        /// A merge that conflicts.
        pub conflicts: bool,
    }

    impl FakeHost {
        pub fn with(repos: &[(&str, &[&str])]) -> FakeHost {
            let h = FakeHost::default();
            for (r, bs) in repos {
                h.branches
                    .lock()
                    .unwrap()
                    .insert(r.to_string(), bs.iter().map(|b| b.to_string()).collect());
                h.default
                    .lock()
                    .unwrap()
                    .insert(r.to_string(), bs.first().unwrap_or(&"main").to_string());
            }
            h
        }
        pub fn has(&self, repo: &str, branch: &str) -> bool {
            self.branches
                .lock()
                .unwrap()
                .get(repo)
                .is_some_and(|b| b.contains(branch))
        }
        pub fn add(&self, repo: &str, branch: &str) {
            self.branches
                .lock()
                .unwrap()
                .entry(repo.to_string())
                .or_default()
                .insert(branch.to_string());
        }
        pub fn remove(&self, repo: &str, branch: &str) {
            if let Some(b) = self.branches.lock().unwrap().get_mut(repo) {
                b.remove(branch);
            }
        }
    }

    #[async_trait::async_trait]
    impl RepairHost for FakeHost {
        async fn ref_exists(&self, owner: &str, repo: &str, branch: &str) -> Result<bool, String> {
            Ok(self.has(&format!("{owner}/{repo}"), branch))
        }
        async fn default_branch(&self, owner: &str, repo: &str) -> Result<String, String> {
            Ok(self
                .default
                .lock()
                .unwrap()
                .get(&format!("{owner}/{repo}"))
                .cloned()
                .unwrap_or_else(|| "main".into()))
        }
        async fn create_branch(
            &self,
            owner: &str,
            repo: &str,
            branch: &str,
            _from: &str,
        ) -> Result<(), String> {
            let r = format!("{owner}/{repo}");
            self.add(&r, branch);
            self.created.lock().unwrap().push(format!("{r}:{branch}"));
            Ok(())
        }
        async fn merge_base_into(
            &self,
            owner: &str,
            repo: &str,
            branch: &str,
            base: &str,
        ) -> Result<bool, String> {
            if self.conflicts {
                return Ok(false);
            }
            self.merged
                .lock()
                .unwrap()
                .push(format!("{owner}/{repo}:{base}>{branch}"));
            Ok(true)
        }
        async fn reopen_pr(&self, _owner: &str, _repo: &str, number: u64) -> Result<(), String> {
            self.reopened.lock().unwrap().push(number);
            Ok(())
        }
        async fn create_label(&self, _owner: &str, _repo: &str, name: &str) -> Result<(), String> {
            self.labels.lock().unwrap().push(name.to_string());
            Ok(())
        }
        async fn refresh_token(&self) {
            *self.tokens.lock().unwrap() += 1;
        }
    }

    #[derive(Default)]
    pub struct FakeRun {
        pub ledger: Mutex<Ledger>,
        pub checkpoint: Mutex<Option<(String, String)>>,
        pub notes: Mutex<Vec<String>>,
        pub back_to_plan: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl RunStore for FakeRun {
        async fn ledger(&self) -> Ledger {
            self.ledger.lock().unwrap().clone()
        }
        async fn save_ledger(&self, ledger: &Ledger) {
            *self.ledger.lock().unwrap() = ledger.clone();
        }
        async fn checkpoint(&self) -> Option<(String, String)> {
            self.checkpoint.lock().unwrap().clone()
        }
        async fn back_to_plan(&self) {
            *self.checkpoint.lock().unwrap() = Some(("plan".into(), String::new()));
            *self.back_to_plan.lock().unwrap() += 1;
        }
        async fn note(&self, text: &str) {
            self.notes.lock().unwrap().push(text.to_string());
        }
    }
}

#[cfg(test)]
pub(crate) mod replay {
    //! A run's implement pass, step by step, against the fakes: the same
    //! order of reads and writes the pipeline makes (its checkpoint guard,
    //! each repository's branch made before it is compared, the compare's
    //! 404 read as `get_diff` reads it, writes, one pull request per
    //! branch), with errors injected where the incidents met them. Each
    //! error goes through the real [`recover`], as `after_error` sends it.
    use super::fakes::*;
    use super::*;
    use crate::clients::github::{compare_404, Compare404};
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    /// An error the replay injects.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Fault {
        /// GitHub answers 502 after `repo`'s write, before its pull request.
        ServerErrorAfterWrite(String),
        /// A person deletes `repo`'s branch after its write.
        BranchDeletedAfterWrite(String),
        /// An error nobody classified, at the start of the step.
        Unclassified(String),
    }

    pub struct World {
        pub host: FakeHost,
        pub run: FakeRun,
        pub owner: String,
        /// The run's repository (the record's `repo`).
        pub run_repo: String,
        pub base: String,
        /// The plan's repositories, in order: more than one is a
        /// multi-repo run.
        pub repos: Vec<String>,
        pub ticket: String,
        /// Whether the pipeline guards its checkpoint (`branch_missing`).
        /// Off replays the release that failed.
        pub guard: bool,
        pub plan_saved: bool,
        pub plans: u32,
        pub tickets: BTreeSet<String>,
        pub ticket_writes: u32,
        pub repo_checkpoints: BTreeMap<String, String>,
        pub writes: BTreeMap<(String, String), u32>,
        pub prs: BTreeMap<(String, String), String>,
        pub pr_writes: u32,
        pub faults: VecDeque<Fault>,
        pub steps: u32,
        pub stopped: Option<NeedsYou>,
    }

    fn http(code: &str, owner: &str, repo: &str, path: &str) -> String {
        format!(
            "HTTP status {} error ({code}) for url (https://api.github.com/repos/{owner}/{repo}/{path}): {{\"message\":\"{}\"}}",
            if code.starts_with('5') { "server" } else { "client" },
            code.split_once(' ').map_or("", |c| c.1)
        )
    }

    impl World {
        pub fn new(owner: &str, run_repo: &str, repos: &[&str], ticket: &str) -> World {
            let host = FakeHost::default();
            for r in repos {
                host.add(&format!("{owner}/{r}"), "main");
            }
            World {
                host,
                run: FakeRun::default(),
                owner: owner.into(),
                run_repo: run_repo.into(),
                base: "main".into(),
                repos: repos.iter().map(|r| r.to_string()).collect(),
                ticket: ticket.into(),
                guard: true,
                plan_saved: false,
                plans: 0,
                tickets: BTreeSet::new(),
                ticket_writes: 0,
                repo_checkpoints: BTreeMap::new(),
                writes: BTreeMap::new(),
                prs: BTreeMap::new(),
                pr_writes: 0,
                faults: VecDeque::new(),
                steps: 0,
                stopped: None,
            }
        }

        fn full(&self, repo: &str) -> String {
            format!("{}/{repo}", self.owner)
        }

        /// The branch each repository's own name gives (the pipeline's
        /// `resolve_run_branch`: `-{repo}` in a multi-repo run).
        pub fn branch_for(&self, repo: &str) -> String {
            if self.repos.len() > 1 {
                format!("coderhelm/{}-{}", self.ticket, repo.to_lowercase())
            } else {
                format!("coderhelm/{}", self.ticket)
            }
        }

        fn file_ticket(&mut self, title: &str) {
            // Follow-ups are deduped by what they say.
            if self.tickets.insert(title.to_string()) {
                self.ticket_writes += 1;
            }
        }

        fn open_pr(&mut self, repo: &str, branch: &str) -> Result<(), String> {
            let full = self.full(repo);
            if !self.host.has(&full, branch) {
                return Err(http(
                    "404 Not Found",
                    &self.owner,
                    repo,
                    &format!("git/ref/heads/{branch}"),
                ));
            }
            // A pull request is found by its branch before one is opened.
            let key = (full, branch.to_string());
            if !self.prs.contains_key(&key) {
                self.prs.insert(key, self.base.clone());
                self.pr_writes += 1;
            }
            Ok(())
        }

        /// One step of the run.
        pub fn step(&mut self) -> Result<(), String> {
            self.steps += 1;
            if let Some(Fault::Unclassified(t)) = self.faults.front().cloned() {
                self.faults.pop_front();
                return Err(t);
            }
            // A step of the same run takes up its own plan; only the first
            // plans (and files its follow-up).
            if !self.plan_saved {
                self.plans += 1;
                self.plan_saved = true;
                *self.run.checkpoint.lock().unwrap() = Some(("plan".into(), String::new()));
            }
            self.file_ticket("follow-up: seed the mapping rows");
            let run_full = self.full(&self.run_repo.clone());
            let cp = self
                .run
                .checkpoint
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            // The checkpoint guard: a branch that isn't in the run's
            // repository sends the pass back to the plan.
            let cp = if self.guard && !cp.1.is_empty() && !self.host.has(&run_full, &cp.1) {
                *self.run.checkpoint.lock().unwrap() = Some(("plan".into(), String::new()));
                ("plan".to_string(), String::new())
            } else {
                cp
            };
            let skip = !cp.1.is_empty() && matches!(cp.0.as_str(), "implement" | "security" | "pr");
            if skip {
                // Checkpoint resume: the branch's diff, read as `get_diff`
                // reads a 404.
                let base_there = self.host.has(&run_full, &self.base);
                let head_there = self.host.has(&run_full, &cp.1);
                if !self.guard && !head_there {
                    // The release that failed returned the 404 itself.
                    return Err(http(
                        "404 Not Found",
                        &self.owner,
                        &self.run_repo,
                        &format!("compare/{}...{}", self.base, cp.1),
                    ));
                }
                match compare_404(base_there, head_there) {
                    Compare404::BaseGone => {
                        return Err(rr::deleted_base_error(
                            &self.owner,
                            &self.run_repo,
                            &self.base,
                        ))
                    }
                    Compare404::NoDiffYet | Compare404::AsIs => {}
                }
                let run_repo = self.run_repo.clone();
                return self.open_pr(&run_repo, &cp.1);
            }
            for repo in self.repos.clone() {
                let full = self.full(&repo);
                let branch = self.branch_for(&repo);
                if self.repo_checkpoints.get(&full).map(String::as_str) == Some("pr")
                    && self.prs.contains_key(&(full.clone(), branch.clone()))
                {
                    continue;
                }
                // The branch is made before anything compares it, from
                // the base (a base that's gone is GitHub's 404 on it).
                if !self.host.has(&full, &branch) {
                    if !self.host.has(&full, &self.base) {
                        return Err(http(
                            "404 Not Found",
                            &self.owner,
                            &repo,
                            &format!("git/ref/heads/{}", self.base),
                        ));
                    }
                    self.host.add(&full, &branch);
                }
                let done_before = self.repo_checkpoints.contains_key(&full);
                if !done_before {
                    *self
                        .writes
                        .entry((full.clone(), branch.clone()))
                        .or_default() += 1;
                    self.repo_checkpoints
                        .insert(full.clone(), "implement".into());
                    // A multi-repo run's own checkpoint is written once every
                    // repository is done; each keeps its own meanwhile.
                    if full == run_full && self.repos.len() == 1 {
                        *self.run.checkpoint.lock().unwrap() =
                            Some(("implement".into(), branch.clone()));
                    }
                }
                match self.faults.front().cloned() {
                    Some(Fault::ServerErrorAfterWrite(r)) if r == repo => {
                        self.faults.pop_front();
                        return Err(http("502 Bad Gateway", &self.owner, &repo, "pulls"));
                    }
                    Some(Fault::BranchDeletedAfterWrite(r)) if r == repo => {
                        self.faults.pop_front();
                        self.host.remove(&full, &branch);
                        self.repo_checkpoints.remove(&full);
                    }
                    _ => {}
                }
                self.open_pr(&repo, &branch)?;
                self.repo_checkpoints.insert(full.clone(), "pr".into());
            }
            let own = self.branch_for(&self.run_repo.clone());
            *self.run.checkpoint.lock().unwrap() = Some(("pr".into(), own));
            Ok(())
        }

        /// Steps until the run finishes or stops for a person: every error
        /// goes through [`recover`] at the run's mark, as `after_error`.
        pub async fn run_to_end(&mut self, max_steps: u32) -> bool {
            while self.steps < max_steps {
                let err = match self.step() {
                    Ok(()) => return true,
                    Err(e) => e,
                };
                let c = rr::classify(&err);
                let cp = self
                    .run
                    .checkpoint
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_default();
                let mark = rr::progress_mark(&cp.0, &format!("{}:{}", cp.1, self.prs.len()));
                let (owner, repo, base) =
                    (self.owner.clone(), self.run_repo.clone(), self.base.clone());
                let at = At {
                    owner: &owner,
                    repo: &repo,
                    base: &base,
                    mark: &mark,
                    now: "t",
                };
                match recover(&self.host, &self.run, &c, &at).await {
                    Recovered::Resume { base, .. } => {
                        if let Some(b) = base {
                            self.base = b;
                        }
                    }
                    Recovered::NeedsYou(n) => {
                        self.stopped = Some(n);
                        return false;
                    }
                }
            }
            false
        }

        /// Every repository has exactly one pull request, the plan was
        /// made once, and the follow-up was filed once.
        pub fn finished_once(&self) {
            assert!(self.stopped.is_none(), "stopped: {:?}", self.stopped);
            assert_eq!(self.plans, 1, "planned again");
            assert_eq!(self.ticket_writes, 1, "a follow-up filed twice");
            assert_eq!(self.pr_writes as usize, self.repos.len(), "{:?}", self.prs);
            for r in &self.repos {
                let key = (self.full(r), self.branch_for(r));
                assert!(self.prs.contains_key(&key), "no pull request for {r}");
                assert!(self.host.has(&key.0, &key.1), "{r} has no branch");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use common::handback::HumanOnly;

    fn at<'a>(mark: &'a str) -> At<'a> {
        At {
            owner: "acme",
            repo: "site",
            base: "main",
            mark,
            now: "t",
        }
    }

    #[tokio::test]
    async fn a_deleted_base_moves_to_the_default_branch() {
        let host = FakeHost::with(&[("acme/site", &["trunk", "coderhelm/tk-1"])]);
        let run = FakeRun::default();
        let c = rr::classify(&rr::deleted_base_error("acme", "site", "main"));
        match recover(&host, &run, &c, &at("m")).await {
            Recovered::Resume { base, .. } => assert_eq!(base.as_deref(), Some("trunk")),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_expired_token_is_fetched_again_then_needs_you() {
        let host = FakeHost::default();
        let run = FakeRun::default();
        let c = rr::classify("HTTP status client error (401 Unauthorized) for url (https://api.github.com/repos/acme/site/pulls): Bad credentials");
        for _ in 0..2 {
            assert!(matches!(
                recover(&host, &run, &c, &at("m")).await,
                Recovered::Resume { .. }
            ));
        }
        assert_eq!(*host.tokens.lock().unwrap(), 2);
        match recover(&host, &run, &c, &at("m")).await {
            Recovered::NeedsYou(n) => {
                assert_eq!(n.why, HumanOnly::Credential);
                assert!(n.action.contains("GitHub app"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_conflict_merges_the_base_then_restarts_from_the_plan() {
        let host = FakeHost::with(&[("acme/site", &["main", "coderhelm/tk-1"])]);
        let run = FakeRun::default();
        *run.checkpoint.lock().unwrap() = Some(("implement".into(), "coderhelm/tk-1".into()));
        let c = rr::classify("HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/acme/site/git/refs/heads/coderhelm/tk-1): Update is not a fast forward");
        assert!(matches!(
            recover(&host, &run, &c, &at("m")).await,
            Recovered::Resume { .. }
        ));
        assert_eq!(host.merged.lock().unwrap().len(), 1);
        assert!(matches!(
            recover(&host, &run, &c, &at("m")).await,
            Recovered::Resume { .. }
        ));
        assert_eq!(*run.back_to_plan.lock().unwrap(), 1);
        assert!(matches!(
            recover(&host, &run, &c, &at("m")).await,
            Recovered::NeedsYou(_)
        ));
    }

    #[tokio::test]
    async fn a_closed_own_pr_is_reopened_and_a_missing_label_created() {
        let host = FakeHost::default();
        let run = FakeRun::default();
        let c = rr::classify("HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/acme/site/pulls/12/merge): Pull Request is closed");
        assert!(matches!(
            recover(&host, &run, &c, &at("m")).await,
            Recovered::Resume { .. }
        ));
        assert_eq!(*host.reopened.lock().unwrap(), vec![12]);
        let c = rr::classify("Label `deploy:staging` does not exist in acme/site");
        assert!(matches!(
            recover(&host, &run, &c, &at("m")).await,
            Recovered::Resume { .. }
        ));
        assert_eq!(*host.labels.lock().unwrap(), vec!["deploy:staging"]);
    }

    fn item(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| {
                let v = if ["run_step", "installation_id", "pr_number", "issue_number"].contains(k)
                {
                    AttributeValue::N(v.to_string())
                } else {
                    AttributeValue::S(v.to_string())
                };
                (k.to_string(), v)
            })
            .collect()
    }

    #[test]
    fn the_sweep_resumes_failed_runs_it_can_get_past_once_per_deploy() {
        let failed = item(&[
            ("team_id", "TEAM#t"),
            ("run_id", "R1"),
            ("status", "failed"),
            ("repo", "acme/infra"),
            ("run_step", "4"),
            ("installation_id", "7"),
            ("ticket_id", "TK-1"),
            ("base_branch", "main"),
            ("error_message", "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/infra/compare/main...coderhelm/tk-1-web): {\"message\":\"Not Found\"}"),
        ]);
        assert!(sweep_wants(&failed, "v1"));
        let mut l = Ledger::default();
        l.mark_healed("v1");
        let mut healed = failed.clone();
        healed.insert(FIELD_LEDGER.into(), AttributeValue::S(l.to_json()));
        assert!(!sweep_wants(&healed, "v1"), "once per run per deploy");
        assert!(sweep_wants(&healed, "v2"), "again after the next deploy");
        let mut unknown = failed.clone();
        unknown.insert(
            "error_message".into(),
            AttributeValue::S("frobnicator broke".into()),
        );
        assert!(!sweep_wants(&unknown, "v1"));
        let mut done = failed.clone();
        done.insert("status".into(), AttributeValue::S("needs_input".into()));
        assert!(!sweep_wants(&done, "v1"));
        // The same run, one step on, on its own repository and base.
        match resend_for(&failed) {
            Some(Resend::Ticket(t)) => {
                assert_eq!(t.continuation_run_id.as_deref(), Some("R1"));
                assert_eq!(t.continuation, 5);
                assert!(t.same_run());
                assert_eq!(
                    (t.repo_owner.as_str(), t.repo_name.as_str()),
                    ("acme", "infra")
                );
                assert_eq!(t.base_branch, "main");
                assert_eq!(t.installation_id, 7);
            }
            other => panic!("{other:?}"),
        }
        let mut with_pr = failed.clone();
        with_pr.insert("pr_number".into(), AttributeValue::N("9".into()));
        assert!(matches!(
            resend_for(&with_pr),
            Some(Resend::Resume { pr_number: 9 })
        ));
    }
}
