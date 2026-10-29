//! CoderHelm runs a pull request's release steps itself, in order, and
//! tracks each one.
//!
//! The steps come from [`super::release_plan::steps`]: dry runs, the expand
//! migration in each environment, the deploys with their real triggers, the
//! production tag, the contract migration, the cache purge. This is the
//! outside work's "CoderHelm after the pull request" owner, extended to wait
//! for approvals and deploys:
//!
//! - Every step that changes something is checked by the team's agent
//!   permissions ([`common::agent_policy::Gate`]) under its own tool name.
//!   Staging writes and deploys are writes: they run when the team allows
//!   them. Production writes, the production tag or deploy and a cache purge
//!   are consequential: they raise one approval card unless the team allowed
//!   them. A step waiting on a card is parked (status `approval`) and the
//!   next tick asks again, which reuses the same card and runs once a person
//!   approved it. Nothing is skipped and nothing quietly becomes a note.
//! - A step waits for the steps it depends on; one that failed or was
//!   declined holds everything after it. The contract migration runs only
//!   after CoderHelm has seen the production deploy's workflow succeed.
//! - Each started workflow is waited for; a step with a count query is
//!   verified by it (the count before, and 0 after) on the environment's
//!   dataset, read only.
//! - A step CoderHelm has no way to do (no workflow to start, a permission
//!   denied by the team's rules) is reported as not done, with the access to
//!   grant.
//!
//! State is one record per pull request in the settings table; ticks are
//! self-scheduled SQS messages (`release_steps`), one chain per record. The
//! pull request's release section is rewritten from the record each tick.

use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use super::release_plan::{self, Kind, On, Planned, Status, Step};
use crate::clients::github::GitHubClient;
use crate::models::ReleaseStepsMessage;
use crate::WorkerState;
use common::agent_policy::approvals::Status as ApprovalStatus;
use common::agent_policy::{Agent, GateCtx, Outcome, Request, Wait};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Seconds between ticks while a workflow runs.
const RUNNING_DELAY: i32 = 60;
/// Seconds between ticks while waiting on a person (an approval, the merge).
const WAITING_DELAY: i32 = 300;
/// How long a release keeps ticking before it parks (and says so).
const MAX_AGE_SECS: i64 = 21 * 24 * 3600;
/// How long a started workflow may take to show up before the step fails.
const RUN_APPEAR_SECS: i64 = 30 * 60;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One pull request's release.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Record {
    pub steps: Vec<Step>,
    /// The lines before the steps (deploy triggers, counts, live items).
    #[serde(default)]
    pub context: Vec<String>,
    pub head_branch: String,
    pub base_branch: String,
    pub chain_id: String,
    pub armed_at: i64,
    #[serde(default)]
    pub parked: bool,
}

impl Record {
    /// The PR section, rendered from the record.
    pub fn lines(&self) -> Vec<String> {
        let mut out = self.context.clone();
        out.extend(release_plan::note_lines(
            &release_plan::Release::default(),
            &self.steps,
            &release_plan::Mechanics::default(),
        ));
        if self.parked {
            out.push(
                "Parked: the release waited longer than three weeks. Re-run the ticket to pick it up again."
                    .to_string(),
            );
        }
        out
    }
}

fn record_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!("RELEASE#{owner}/{repo}#{pr:06}")
}

// ── Deciding, without side effects ──────────────────────────────────────────

/// What a tick does with one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing now (done, or waiting on another step or the merge).
    Wait,
    /// A step it depends on failed, was declined or can't be done.
    Held(String),
    /// Ask the gate and start it.
    Start,
    /// It is running: check its workflow run.
    Check,
}

/// What a tick does with step `i`.
pub fn next(steps: &[Step], i: usize, merged: bool) -> Next {
    let s = &steps[i];
    if s.status.finished() {
        return Next::Wait;
    }
    for id in &s.after {
        let Some(d) = steps.iter().find(|x| &x.id == id) else {
            continue;
        };
        if d.status.finished() && d.status != Status::Done {
            return Next::Held(format!("waits on \"{}\", which is not done", d.what));
        }
        if d.status != Status::Done {
            // Held anywhere up the chain holds this one too.
            let di = steps.iter().position(|x| x.id == d.id).unwrap_or(i);
            if di != i {
                if let Next::Held(why) = next(steps, di, merged) {
                    return Next::Held(why);
                }
            }
            return Next::Wait;
        }
    }
    if s.after_merge && !merged {
        return Next::Wait;
    }
    if s.status == Status::Running {
        Next::Check
    } else {
        Next::Start
    }
}

/// The workflow run a step waits on: for a deploy started by a merge or a
/// tag, the newest run on that commit; for a started workflow, the newest
/// one created after it was started.
pub fn pick_run<'a>(runs: &'a [Value], started_at: i64, sha: &str) -> Option<&'a Value> {
    runs.iter().find(|r| {
        if !sha.is_empty() {
            return r["head_sha"].as_str() == Some(sha);
        }
        let created = r["created_at"]
            .as_str()
            .and_then(|c| chrono::DateTime::parse_from_rfc3339(c).ok())
            .map(|t| t.timestamp())
            .unwrap_or(0);
        created + 60 >= started_at
    })
}

/// A finished run's outcome (its link either way); `None` while it runs.
pub fn run_outcome(run: &Value) -> Option<Result<String, String>> {
    if run["status"].as_str() != Some("completed") {
        return None;
    }
    let link = run["html_url"].as_str().unwrap_or("").to_string();
    Some(match run["conclusion"].as_str() {
        Some("success") => Ok(link),
        Some(c) => Err(format!("the workflow run ended `{c}`: {link}")),
        None => Err(format!(
            "the workflow run ended without a conclusion: {link}"
        )),
    })
}

/// When to tick again: soon while a workflow runs, slowly while waiting on
/// a person; `None` when every step has finished or is held.
pub fn delay(steps: &[Step], merged: bool) -> Option<i32> {
    let open: Vec<usize> = (0..steps.len())
        .filter(|i| {
            matches!(
                next(steps, *i, merged),
                Next::Start | Next::Check | Next::Wait
            )
        })
        .filter(|i| !steps[*i].status.finished())
        .collect();
    if open.is_empty() {
        return None;
    }
    if open.iter().any(|i| steps[*i].status == Status::Running) {
        Some(RUNNING_DELAY)
    } else {
        Some(WAITING_DELAY)
    }
}

/// The gate's arguments for a step: the same for every ask of that step, so
/// a waiting card is reused, never doubled.
pub fn gate_args(owner: &str, repo: &str, pr: u64, s: &Step) -> Value {
    serde_json::json!({
        "repo": format!("{owner}/{repo}"),
        "pull_request": pr,
        "step": s.id,
        "environment": s.env.word(),
        "how": s.how,
    })
}

// ── Arming ──────────────────────────────────────────────────────────────────

/// Saves a pull request's release and starts its ticks. The record is
/// replaced (a re-run re-derives it), keeping the progress of steps that
/// are still the same.
#[allow(clippy::too_many_arguments)]
pub async fn arm(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    run_id: &str,
    head_branch: &str,
    base_branch: &str,
    planned: &Planned,
) {
    if planned.steps.is_empty() {
        return;
    }
    let mut context = planned.lines();
    context.retain(|l| !l.starts_with(|c: char| c.is_ascii_digit()));
    let mut steps = planned.steps.clone();
    if let Ok(Some(old)) = load(state, team_id, owner, repo, pr_number).await {
        for s in steps.iter_mut() {
            if let Some(o) = old
                .steps
                .iter()
                .find(|o| o.id == s.id && o.how == s.how && s.status == Status::Pending)
            {
                *s = o.clone();
            }
        }
    }
    let rec = Record {
        steps,
        context,
        head_branch: head_branch.to_string(),
        base_branch: base_branch.to_string(),
        chain_id: ulid::Ulid::new().to_string(),
        armed_at: now(),
        parked: false,
    };
    if let Err(e) = save(state, team_id, owner, repo, pr_number, &rec).await {
        warn!(pr = pr_number, error = %e, "release: could not save the release record");
        return;
    }
    let msg = ReleaseStepsMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        run_id: run_id.to_string(),
        chain_id: rec.chain_id.clone(),
        attempts: 0,
    };
    send(state, &msg, 0).await;
}

/// After the pull request opens: derives the release, reports it once on
/// the ticket (the same lines the PR shows), and arms the steps.
#[allow(clippy::too_many_arguments)]
pub async fn after_pr(
    state: &WorkerState,
    msg: &crate::models::TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    plan: &super::plan::PlanResult,
    branch: &str,
    pr_number: u64,
) {
    let Some(planned) = release_plan::for_run(state, msg, github, plan, branch).await else {
        return;
    };
    if planned.steps.is_empty() {
        return;
    }
    let lines = planned.lines();
    let body = lines
        .iter()
        .map(|l| {
            if l.starts_with(|c: char| c.is_ascii_digit()) {
                l.clone()
            } else {
                format!("- {l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let title = "Release steps (done by CoderHelm)";
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("{title}: {} step(s) armed", planned.steps.len()),
    )
    .await;
    super::post_ticket_notice(
        state,
        msg,
        run_id,
        github,
        NOTICE_RELEASE,
        &format!("**{title}**\n\n{body}"),
        &super::fix_location::jira_text(&body),
        title,
    )
    .await;
    arm(
        state,
        &msg.team_id,
        msg.installation_id,
        &msg.repo_owner,
        &msg.repo_name,
        pr_number,
        run_id,
        branch,
        &msg.base_branch,
        &planned,
    )
    .await;
}

/// Notice key: a ticket's release steps, one comment per ticket.
const NOTICE_RELEASE: &str = "release_steps";

async fn load(
    state: &WorkerState,
    team: &str,
    owner: &str,
    repo: &str,
    pr: u64,
) -> Result<Option<Record>, BoxError> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(record_sk(owner, repo, pr)))
        .send()
        .await?;
    let Some(data) = r
        .item()
        .and_then(|i| i.get("data"))
        .and_then(|v| v.as_s().ok())
    else {
        return Ok(None);
    };
    Ok(serde_json::from_str(data).ok())
}

async fn save(
    state: &WorkerState,
    team: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    rec: &Record,
) -> Result<(), BoxError> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S(team.to_string()))
        .item("sk", AttributeValue::S(record_sk(owner, repo, pr)))
        .item("data", AttributeValue::S(serde_json::to_string(rec)?))
        .item("updated_at", AttributeValue::N(now().to_string()))
        .send()
        .await?;
    Ok(())
}

async fn send(state: &WorkerState, msg: &ReleaseStepsMessage, delay: i32) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut v = match serde_json::to_value(msg) {
        Ok(v) => v,
        Err(_) => return false,
    };
    if let Some(o) = v.as_object_mut() {
        o.insert("type".into(), serde_json::json!("release_steps"));
    }
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(v.to_string())
        .delay_seconds(delay.clamp(0, 900))
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "release: enqueue failed");
            false
        }
    }
}

// ── A tick ──────────────────────────────────────────────────────────────────

/// One tick: moves every step that can move, saves, updates the PR, and
/// schedules the next tick while anything is left.
pub async fn run(state: &WorkerState, msg: ReleaseStepsMessage) -> Result<(), BoxError> {
    let (owner, repo, pr) = (&msg.repo_owner, &msg.repo_name, msg.pr_number);
    let Some(mut rec) = load(state, &msg.team_id, owner, repo, pr).await? else {
        return Ok(());
    };
    if rec.chain_id != msg.chain_id || rec.parked {
        return Ok(());
    }
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let p = github.get_pull_request(owner, repo, pr).await?;
    let merged = p["merged"].as_bool().unwrap_or(false);
    if p["state"].as_str() == Some("closed") && !merged {
        info!(
            pr,
            "release: the pull request closed without merging; release stopped"
        );
        return Ok(());
    }
    let merge_sha = p["merge_commit_sha"].as_str().unwrap_or("").to_string();
    let ctx = GateCtx::new(&msg.team_id, Agent::Coder)
        .repo(&format!("{owner}/{repo}"))
        .run(&msg.run_id)
        .task(&format!("Release steps for pull request #{pr}"));
    let gate = crate::agent::policy::gate(state, ctx, None).with_wait(Wait::Return);
    let store = crate::agent::policy::store(state);
    let before = rec.lines();
    for i in 0..rec.steps.len() {
        match next(&rec.steps, i, merged) {
            Next::Wait | Next::Held(_) => {}
            Next::Check => {
                let r = rec.clone();
                check(
                    state,
                    &github,
                    owner,
                    repo,
                    &r,
                    &mut rec.steps[i],
                    &merge_sha,
                )
                .await
            }
            Next::Start => {
                let s = rec.steps[i].clone();
                // A declined card ends the step; it is never asked again.
                if s.status == Status::Approval && !s.approval_id.is_empty() {
                    if let Ok(Some(a)) = store.get_approval(&msg.team_id, &s.approval_id).await {
                        if a.status == ApprovalStatus::Denied {
                            let st = &mut rec.steps[i];
                            st.status = Status::Declined;
                            st.detail = format!(
                                "{} declined it{}",
                                if a.decided_by.is_empty() {
                                    "A person"
                                } else {
                                    a.decided_by.as_str()
                                },
                                if a.note.is_empty() {
                                    String::new()
                                } else {
                                    format!(": \"{}\"", a.note)
                                }
                            );
                            continue;
                        }
                    }
                }
                if s.tool.is_empty() {
                    // A deploy that runs on merge: only waited for.
                    let st = &mut rec.steps[i];
                    st.status = Status::Running;
                    st.started_at = now();
                    st.detail = format!("waiting for `{}` on the merge commit", st.workflow);
                    continue;
                }
                let args = gate_args(owner, repo, pr, &s);
                match gate
                    .check(Request {
                        tool: &s.tool,
                        args: &args,
                        why: &s.what,
                    })
                    .await
                {
                    Outcome::Run { .. } => {
                        let r = rec.clone();
                        start(
                            state,
                            &github,
                            &msg.team_id,
                            (owner, repo),
                            &r,
                            &mut rec.steps[i],
                            merged,
                            &merge_sha,
                        )
                        .await
                    }
                    Outcome::Pending { approval, .. } => {
                        let st = &mut rec.steps[i];
                        st.status = Status::Approval;
                        st.approval_id = approval.id.clone();
                        st.detail = format!(
                            "approval card `{}` on the Agent permissions page, asked {}",
                            approval.id,
                            approval
                                .created_at
                                .get(..10)
                                .unwrap_or(&approval.created_at)
                        );
                    }
                    Outcome::Refused { message, .. } => {
                        let st = &mut rec.steps[i];
                        st.status = Status::NoAccess;
                        st.detail = format!(
                            "the team's agent permissions don't allow `{}` ({}). Grant it: allow \
                             or ask for `{}` on the Agent permissions page",
                            st.tool,
                            message.trim_end_matches('.'),
                            st.tool
                        );
                    }
                    // Paused: nothing moves; the next tick tries again.
                    Outcome::Stop(_) => break,
                }
            }
        }
    }
    let age = now() - rec.armed_at;
    let again = delay(&rec.steps, merged);
    if again.is_some() && age > MAX_AGE_SECS {
        rec.parked = true;
    }
    save(state, &msg.team_id, owner, repo, pr, &rec).await?;
    let after = rec.lines();
    if after != before {
        let body = p["body"].as_str().unwrap_or("");
        let updated = release_plan::put_section(body, &release_plan::section(&after));
        if updated != body {
            if let Err(e) = github.update_pr_body(owner, repo, pr, &updated).await {
                warn!(pr, error = %e, "release: could not update the PR's release section");
            }
        }
        if !msg.run_id.is_empty() {
            for s in &rec.steps {
                let changed = !before
                    .iter()
                    .any(|l| l.contains(&s.what) && l.contains(&s.detail));
                if changed && s.status.finished() {
                    super::add_progress_note(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        &format!("Release step: {} ({:?}) {}", s.what, s.status, s.detail),
                    )
                    .await;
                }
            }
        }
    }
    if let (Some(d), false) = (again, rec.parked) {
        let mut next_msg = msg.clone();
        next_msg.attempts += 1;
        send(state, &next_msg, d).await;
    }
    Ok(())
}

/// The count a step's query returns now on its environment's dataset.
async fn count(state: &WorkerState, s: &Step) -> Result<u64, String> {
    if s.project_id.is_empty() || s.dataset.is_empty() {
        return Err("no dataset is named for this environment".into());
    }
    let src = super::outside_work::ContentSource {
        project_id: s.project_id.clone(),
        dataset: s.dataset.clone(),
        origin: String::new(),
    };
    let v = super::outside_work::public_query(&state.http, &src, &s.verify).await?;
    release_plan::count_of(&v.to_string()).ok_or_else(|| format!("the query returned {v}"))
}

/// Starts a step the gate allowed: counts before, then dispatches its
/// workflow or cuts the production tag.
#[allow(clippy::too_many_arguments)]
async fn start(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    (owner, repo): (&str, &str),
    rec: &Record,
    s: &mut Step,
    merged: bool,
    merge_sha: &str,
) {
    if !s.verify.trim().is_empty() && s.before.is_empty() {
        s.before = match count(state, s).await {
            Ok(n) => n.to_string(),
            Err(e) => format!("could not count: {e}"),
        };
    }
    s.started_at = now();
    let git_ref = if merged {
        rec.base_branch.as_str()
    } else {
        rec.head_branch.as_str()
    };
    if s.kind == Kind::Deploy && matches!(s.trigger, Some(On::Tag(_))) {
        let cfg = super::review_actions::OnApproveConfig::load(state, team_id, owner, repo).await;
        match super::review_actions::cut_tag_if_new(github, &cfg, owner, repo, merge_sha).await {
            Ok(Some(tag)) => {
                s.tag = tag;
            }
            Ok(None) => {
                // Already tagged (the team's own tagging on merge).
                s.tag = github
                    .list_tags(owner, repo)
                    .await
                    .ok()
                    .and_then(|v| v.as_array().cloned())
                    .unwrap_or_default()
                    .iter()
                    .find(|t| t["commit"]["sha"].as_str() == Some(merge_sha))
                    .and_then(|t| t["name"].as_str().map(String::from))
                    .unwrap_or_default();
            }
            Err(e) => {
                s.status = Status::Failed;
                s.detail = format!("cutting the release tag failed: {e}");
                return;
            }
        }
        s.status = Status::Running;
        s.detail = format!("tagged `{}`; waiting for `{}`", s.tag, s.workflow);
        return;
    }
    if s.workflow.is_empty() {
        s.status = Status::NoAccess;
        if s.detail.is_empty() {
            s.detail = release_plan::NO_MIGRATION_RUNNER.to_string();
        }
        return;
    }
    let file = s
        .workflow
        .rsplit('/')
        .next()
        .unwrap_or(&s.workflow)
        .to_string();
    let inputs: serde_json::Map<String, Value> = s
        .inputs
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    match github
        .dispatch_workflow(owner, repo, &file, git_ref, &Value::Object(inputs))
        .await
    {
        Ok(()) => {
            s.status = Status::Running;
            s.detail = format!("started `{}` on `{git_ref}`", s.workflow);
        }
        Err(e) => {
            s.status = Status::Failed;
            s.detail = format!("starting `{}` failed: {e}", s.workflow);
        }
    }
}

/// Checks a running step's workflow run, and verifies it once it passed.
async fn check(
    state: &WorkerState,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    rec: &Record,
    s: &mut Step,
    merge_sha: &str,
) {
    let file = s
        .workflow
        .rsplit('/')
        .next()
        .unwrap_or(&s.workflow)
        .to_string();
    // Deploys on merge or tag run on the merge commit; started workflows run
    // on the branch they were started on.
    let (branch, sha) = match &s.trigger {
        Some(On::Merge(b)) => (b.clone(), merge_sha.to_string()),
        Some(On::Tag(_)) => (s.tag.clone(), merge_sha.to_string()),
        _ if s.kind == Kind::Deploy || s.kind == Kind::Purge => {
            (rec.base_branch.clone(), String::new())
        }
        _ => {
            let merged_ref = if s.after_merge {
                &rec.base_branch
            } else {
                &rec.head_branch
            };
            (merged_ref.clone(), String::new())
        }
    };
    let runs = github
        .list_workflow_runs(owner, repo, &file, &branch)
        .await
        .unwrap_or_default();
    let Some(run) = pick_run(&runs, s.started_at, &sha) else {
        if now() - s.started_at > RUN_APPEAR_SECS {
            s.status = Status::Failed;
            s.detail = format!(
                "no run of `{}` appeared on `{branch}` within 30 minutes",
                s.workflow
            );
        }
        return;
    };
    s.run_id = run["id"].as_u64().unwrap_or(0);
    match run_outcome(run) {
        None => {
            s.detail = format!(
                "`{}` is running: {}",
                s.workflow,
                run["html_url"].as_str().unwrap_or("")
            );
        }
        Some(Err(e)) => {
            s.status = Status::Failed;
            s.detail = e;
        }
        Some(Ok(link)) => {
            if s.verify.trim().is_empty() {
                s.status = Status::Done;
                s.detail = format!("`{}` succeeded: {link}", s.workflow);
                return;
            }
            match count(state, s).await {
                Ok(0) => {
                    s.after_count = "0".into();
                    s.status = Status::Done;
                    s.detail = format!("`{}` succeeded: {link}", s.workflow);
                }
                Ok(n) => {
                    s.after_count = n.to_string();
                    s.status = Status::Failed;
                    s.detail = format!(
                        "`{}` succeeded ({link}) but `{}` still returns {n}, not 0",
                        s.workflow, s.verify
                    );
                }
                Err(e) => {
                    s.after_count = format!("could not count: {e}");
                    s.status = Status::Failed;
                    s.detail = format!(
                        "`{}` succeeded ({link}) but the count failed: {e}",
                        s.workflow
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::release_plan::{
        Env, FieldChange, Impact, Mechanics, PlannedMigration, Release,
    };

    fn plan() -> Vec<Step> {
        let m = release_plan::mechanics(
            &[
                (
                    ".github/workflows/deploy.yml".into(),
                    "name: Deploy\non:\n  push:\n    branches: [main]\n    tags: ['v*']\njobs:\n  staging:\n    if: github.ref == 'refs/heads/main'\n    environment: staging\n    steps:\n      - run: deploy\n  production:\n    if: startsWith(github.ref, 'refs/tags/')\n    environment: production\n    steps:\n      - run: deploy\n".into(),
                ),
                (
                    ".github/workflows/migrate.yml".into(),
                    "name: Migrate\non:\n  workflow_dispatch:\n    inputs:\n      dataset:\n        options: [staging, production]\n      migration: {}\njobs:\n  m:\n    steps:\n      - run: npx sanity migration run\n".into(),
                ),
            ],
            None,
        );
        let r = Release {
            fields: vec![FieldChange {
                old: vec!["oldA".into()],
                new: "newFlag".into(),
                types: vec![],
            }],
            migrations: vec![
                PlannedMigration {
                    file: "m/expand.ts".into(),
                    phase: "expand".into(),
                },
                PlannedMigration {
                    file: "m/contract.ts".into(),
                    phase: "contract".into(),
                },
            ],
            impact: vec![Impact::default()],
            ..Default::default()
        };
        release_plan::steps(&r, &m)
    }

    fn set(steps: &mut [Step], id: &str, st: Status) {
        steps.iter_mut().find(|s| s.id == id).unwrap().status = st;
    }

    fn idx(steps: &[Step], id: &str) -> usize {
        steps.iter().position(|s| s.id == id).unwrap()
    }

    #[test]
    fn steps_run_in_order_and_wait_for_the_merge() {
        let mut s = plan();
        assert_eq!(next(&s, idx(&s, "expand_staging"), false), Next::Start);
        assert_eq!(next(&s, idx(&s, "expand_production"), false), Next::Wait);
        set(&mut s, "expand_staging", Status::Done);
        assert_eq!(next(&s, idx(&s, "expand_production"), false), Next::Start);
        set(&mut s, "expand_production", Status::Done);
        // Deploys and the contract wait for the merge.
        assert_eq!(next(&s, idx(&s, "deploy_staging"), false), Next::Wait);
        assert_eq!(next(&s, idx(&s, "deploy_staging"), true), Next::Start);
        assert_eq!(s[idx(&s, "deploy_staging")].tool, "");
        assert_eq!(
            s[idx(&s, "deploy_production")].tool,
            "release_production_tag"
        );
    }

    #[test]
    fn the_contract_runs_only_after_the_production_deploy_succeeded() {
        let mut s = plan();
        for id in ["expand_staging", "expand_production", "deploy_staging"] {
            set(&mut s, id, Status::Done);
        }
        set(&mut s, "deploy_production", Status::Running);
        assert_eq!(next(&s, idx(&s, "contract_staging"), true), Next::Wait);
        set(&mut s, "deploy_production", Status::Failed);
        assert!(matches!(
            next(&s, idx(&s, "contract_staging"), true),
            Next::Held(_)
        ));
        set(&mut s, "deploy_production", Status::Done);
        assert_eq!(next(&s, idx(&s, "contract_staging"), true), Next::Start);
        assert_eq!(next(&s, idx(&s, "contract_production"), true), Next::Wait);
        set(&mut s, "contract_staging", Status::Done);
        assert_eq!(next(&s, idx(&s, "contract_production"), true), Next::Start);
        assert_eq!(
            s[idx(&s, "contract_production")].tool,
            "release_production_migration"
        );
        assert_eq!(
            s[idx(&s, "contract_production")].inputs[0],
            ("dataset".into(), "production".into())
        );
    }

    #[test]
    fn a_declined_step_holds_the_rest_and_nothing_ticks_forever() {
        let mut s = plan();
        set(&mut s, "expand_staging", Status::Declined);
        assert!(matches!(
            next(&s, idx(&s, "expand_production"), true),
            Next::Held(_)
        ));
        assert_eq!(delay(&s, true), None);
        let mut s = plan();
        set(&mut s, "expand_staging", Status::Running);
        assert_eq!(delay(&s, false), Some(RUNNING_DELAY));
        set(&mut s, "expand_staging", Status::Approval);
        assert_eq!(delay(&s, false), Some(WAITING_DELAY));
    }

    #[test]
    fn no_migration_workflow_is_reported_with_the_access_to_grant() {
        let m = Mechanics::default();
        let r = Release {
            migrations: vec![PlannedMigration {
                file: "m/x.ts".into(),
                phase: "expand".into(),
            }],
            ..Default::default()
        };
        let s = release_plan::steps(&r, &m);
        let e = s
            .iter()
            .find(|x| x.kind == Kind::Expand && x.env == Env::Production)
            .unwrap();
        assert_eq!(e.status, Status::NoAccess);
        assert!(e
            .detail
            .contains("Grant it: add a workflow with `workflow_dispatch`"));
        let lines = release_plan::note_lines(&r, &s, &m).join("\n");
        assert!(lines.contains("Not done: CoderHelm can't do this yet."));
    }

    #[test]
    fn the_run_a_step_waits_on_is_found() {
        let runs = vec![
            serde_json::json!({"head_sha": "abc", "created_at": "2026-10-28T10:00:00Z", "status": "completed", "conclusion": "success", "html_url": "u1"}),
            serde_json::json!({"head_sha": "def", "created_at": "2026-10-28T09:00:00Z", "status": "in_progress", "html_url": "u2"}),
        ];
        assert_eq!(pick_run(&runs, 0, "def").unwrap()["html_url"], "u2");
        let started = chrono::DateTime::parse_from_rfc3339("2026-10-28T09:59:30Z")
            .unwrap()
            .timestamp();
        assert_eq!(pick_run(&runs, started, "").unwrap()["html_url"], "u1");
        assert_eq!(run_outcome(&runs[0]), Some(Ok("u1".into())));
        assert_eq!(run_outcome(&runs[1]), None);
        let failed =
            serde_json::json!({"status": "completed", "conclusion": "failure", "html_url": "u3"});
        assert!(run_outcome(&failed)
            .unwrap()
            .unwrap_err()
            .contains("`failure`"));
    }

    #[test]
    fn the_gate_sees_the_same_call_every_tick() {
        let s = plan();
        let a = gate_args("o", "r", 7, &s[0]);
        assert_eq!(a, gate_args("o", "r", 7, &s[0]));
        assert_eq!(a["environment"], "staging");
    }

    #[test]
    fn every_release_tool_has_a_risk() {
        use common::agent_policy::{registry::builtin, Risk};
        for t in [
            "release_dry_run",
            "release_staging_migration",
            "release_staging_deploy",
        ] {
            assert_eq!(builtin(t), Some(Risk::Write), "{t}");
        }
        for t in [
            "release_production_migration",
            "release_production_deploy",
            "release_production_tag",
            "release_cache_purge",
        ] {
            assert_eq!(builtin(t), Some(Risk::Consequential), "{t}");
        }
    }
}
