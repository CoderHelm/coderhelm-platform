//! Async, baseline-aware post-merge health check. Enqueued (with an SQS
//! delay) by the on-approve auto-merge; it does NOT block the review.
//!
//! It judges the MERGE COMMIT's own deploy, never the branch head: the check
//! runs on `merge_sha` plus the Actions workflow runs whose `head_sha` is the
//! merge commit (push runs, never pull request runs). Other merges moving the
//! branch head don't change what it reads. When a later commit's run
//! supersedes this commit's deploy (the workflow cancelled or skipped it), it
//! follows the next run of that workflow whose commit contains the merge
//! commit, and says so in the result.
//!
//! It waits as long as the deploy actually runs: no attempt count ends it.
//! Each tick is a NEW queue message (never a re-receive of this one), with a
//! back-off of 1, 2, 5, then 10 minutes. The deploy's own limits bound the
//! wait (GitHub stops a job at its timeout, 6 hours at most); an 8-hour safety
//! ceiling after the merge is logged loudly and names the job still running.
//!
//! It posts a verdict only:
//!   - 💚 after the deploy completed and stayed clean for the watch window,
//!     which starts when the merge commit's deploy completes;
//!   - 🔴 on check failures that are NEW vs. the pre-merge baseline, naming
//!     each failing check or job with its link;
//!   - nothing when the commit has no check or deploy at all after a grace
//!     period.
//!
//! One comment per merge, edited in place on a later verdict. It reports; it
//! never rolls back a shared branch.

use crate::clients::github::GitHubClient;
use crate::models::{FollowedDeploy, HealthCheckMessage};
use crate::WorkerState;
use std::collections::HashSet;
use tracing::{error, info, warn};

/// A green deploy does NOT end the check: it STARTS the watch window. A
/// deploy that looks fine one minute after it finishes can start failing ten
/// minutes in; 💚 posts only after the merge commit stays clean this long.
const WATCH_WINDOW_SECS: i64 = 15 * 60;
/// How long a merge commit may show no check or run at all before the check
/// concludes there is nothing to watch (and stays quiet).
const NO_DEPLOY_GRACE_SECS: i64 = 15 * 60;
/// Safety ceiling after the merge. GitHub itself stops a job after its
/// timeout (6 hours at most), so a deploy still running here is stuck; the
/// comment then names the job and since when. Logged as an error.
const SAFETY_CEILING_SECS: i64 = 8 * 60 * 60;
/// SQS caps a message delay at 15 minutes.
const MAX_DELAY_SECS: i64 = 900;
/// Back-off between ticks while the deploy runs: 1, 2, 5, then 10 minutes.
const BACKOFF_SECS: [i64; 4] = [60, 120, 300, 600];
/// During the watch window, look again at least this often so a failure
/// that appears mid-window is reported quickly.
const WATCH_TICK_SECS: i64 = 300;
/// How many superseded hops to follow in one tick (a run cancelled by a run
/// that was itself cancelled by a later one).
const MAX_FOLLOW_HOPS: usize = 5;

const FAILING: [&str; 4] = ["failure", "timed_out", "cancelled", "startup_failure"];

/// One check or job of the deploy being judged.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Job {
    pub name: String,
    pub status: String,
    pub conclusion: String,
    pub url: String,
    pub started_at: String,
    pub completed_at: String,
    /// The check suite it belongs to (a workflow run's `check_suite_id`).
    pub suite_id: u64,
    pub head_sha: String,
}

impl Job {
    pub fn from_check_run(v: &serde_json::Value) -> Job {
        let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
        Job {
            name: s("name"),
            status: s("status"),
            conclusion: s("conclusion"),
            url: s("html_url"),
            started_at: s("started_at"),
            completed_at: s("completed_at"),
            suite_id: v["check_suite"]["id"].as_u64().unwrap_or(0),
            head_sha: s("head_sha"),
        }
    }

    /// A workflow run that has no jobs to show yet (queued, waiting on a
    /// concurrency group) or never started one (startup failure).
    fn from_run(r: &WfRun) -> Job {
        Job {
            name: r.name.clone(),
            status: r.status.clone(),
            conclusion: r.conclusion.clone(),
            url: r.url.clone(),
            started_at: if r.run_started_at.is_empty() {
                r.created_at.clone()
            } else {
                r.run_started_at.clone()
            },
            completed_at: if r.status == "completed" {
                r.updated_at.clone()
            } else {
                String::new()
            },
            suite_id: r.suite_id,
            head_sha: r.head_sha.clone(),
        }
    }

    pub fn done(&self) -> bool {
        self.status == "completed"
    }

    pub fn failed(&self) -> bool {
        self.done() && FAILING.contains(&self.conclusion.as_str())
    }
}

/// One Actions workflow run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WfRun {
    pub id: u64,
    pub workflow_id: u64,
    pub name: String,
    pub status: String,
    pub conclusion: String,
    pub url: String,
    pub head_sha: String,
    pub head_branch: String,
    pub event: String,
    pub created_at: String,
    pub run_started_at: String,
    pub updated_at: String,
    pub suite_id: u64,
}

impl WfRun {
    pub fn from_json(v: &serde_json::Value) -> WfRun {
        let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
        WfRun {
            id: v["id"].as_u64().unwrap_or(0),
            workflow_id: v["workflow_id"].as_u64().unwrap_or(0),
            name: s("name"),
            status: s("status"),
            conclusion: s("conclusion"),
            url: s("html_url"),
            head_sha: s("head_sha"),
            head_branch: s("head_branch"),
            event: s("event"),
            created_at: s("created_at"),
            run_started_at: s("run_started_at"),
            updated_at: s("updated_at"),
            suite_id: v["check_suite_id"].as_u64().unwrap_or(0),
        }
    }

    /// A deploy of the commit itself, not a pull request's checks.
    fn deploys_commit(&self) -> bool {
        !self.event.starts_with("pull_request")
    }

    /// The workflow dropped this run for a later one (concurrency).
    pub fn superseded(&self) -> bool {
        self.status == "completed" && matches!(self.conclusion.as_str(), "cancelled" | "skipped")
    }
}

/// The later runs of a superseded run's workflow that could have deployed
/// its commit: same workflow, push, created after it, a different commit;
/// oldest first. The first one whose commit contains the merge commit is the
/// deploy that includes it.
pub fn successor_candidates(superseded: &WfRun, later: &[WfRun]) -> Vec<WfRun> {
    let mut out: Vec<WfRun> = later
        .iter()
        .filter(|r| {
            r.workflow_id == superseded.workflow_id
                && r.id != superseded.id
                && r.event == "push"
                && r.head_sha != superseded.head_sha
                && r.created_at >= superseded.created_at
        })
        .cloned()
        .collect();
    out.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    out
}

/// A superseded run of the merge commit, followed to the run that deployed it.
#[derive(Debug, Clone, Default)]
pub struct Follow {
    pub superseded: WfRun,
    pub successor: WfRun,
    /// The successor's check runs (filtered to its suite by `assemble`).
    pub successor_checks: Vec<Job>,
}

/// The jobs that make up the merge commit's deploy: the commit's own check
/// runs and workflow runs, with each superseded run replaced by the run that
/// deployed the commit. Anything on another commit is ignored, so other
/// merges moving the branch head never change the verdict.
pub fn assemble(
    merge_sha: &str,
    own_checks: &[Job],
    own_runs: &[WfRun],
    follows: &[Follow],
) -> Vec<Job> {
    let followed_suites: HashSet<u64> = follows.iter().map(|f| f.superseded.suite_id).collect();
    let mut jobs: Vec<Job> = own_checks
        .iter()
        .filter(|j| j.head_sha.is_empty() || j.head_sha == merge_sha)
        .filter(|j| j.suite_id == 0 || !followed_suites.contains(&j.suite_id))
        .cloned()
        .collect();
    let runs = own_runs
        .iter()
        .filter(|r| r.head_sha == merge_sha && r.deploys_commit())
        .filter(|r| !followed_suites.contains(&r.suite_id));
    for r in runs {
        if !jobs.iter().any(|j| j.suite_id == r.suite_id) {
            jobs.push(Job::from_run(r));
        }
    }
    for f in follows {
        let theirs: Vec<Job> = f
            .successor_checks
            .iter()
            .filter(|j| j.suite_id == f.successor.suite_id)
            .cloned()
            .collect();
        if theirs.is_empty() {
            jobs.push(Job::from_run(&f.successor));
        } else {
            jobs.extend(theirs);
        }
    }
    jobs
}

/// What the check does after one look.
#[derive(Debug, Clone, PartialEq)]
pub enum Next {
    /// Look again in `delay` seconds (a new queue message).
    Wait {
        delay: i64,
        green_since: Option<i64>,
    },
    /// Post (or update) the one comment for this merge.
    Post(Verdict),
    /// Nothing to watch: no check or deploy ever showed up for the commit.
    Quiet,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// The deploy completed and stayed clean for the watch window.
    Green,
    /// Checks failing that were not failing before the merge.
    Failed(Vec<Job>),
    /// The safety ceiling passed with these jobs still running.
    StillRunning(Vec<Job>),
}

/// The delay before the next tick while the deploy runs.
pub fn backoff(ticks: u32) -> i64 {
    let i = (ticks as usize).min(BACKOFF_SECS.len() - 1);
    BACKOFF_SECS[i].min(MAX_DELAY_SECS)
}

fn epoch(ts: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.timestamp())
}

/// One look at the merge commit's deploy. No branch of this ever gives up
/// because of time while the deploy is still running, except the safety
/// ceiling, which names what is running.
pub fn decide(
    now: i64,
    merged_at: i64,
    ticks: u32,
    green_since: Option<i64>,
    jobs: &[Job],
    baseline: &HashSet<String>,
) -> Next {
    if jobs.is_empty() {
        if now - merged_at < NO_DEPLOY_GRACE_SECS {
            return Next::Wait {
                delay: backoff(ticks),
                green_since: None,
            };
        }
        return Next::Quiet;
    }
    let running: Vec<Job> = jobs.iter().filter(|j| !j.done()).cloned().collect();
    if !running.is_empty() {
        if now - merged_at >= SAFETY_CEILING_SECS {
            return Next::Post(Verdict::StillRunning(running));
        }
        return Next::Wait {
            delay: backoff(ticks),
            green_since: None,
        };
    }
    let new_failures: Vec<Job> = jobs
        .iter()
        .filter(|j| j.failed() && !baseline.contains(&j.name))
        .cloned()
        .collect();
    if !new_failures.is_empty() {
        return Next::Post(Verdict::Failed(new_failures));
    }
    // Completed and clean: the window starts when the deploy completed.
    let completed = jobs.iter().filter_map(|j| epoch(&j.completed_at)).max();
    let since = completed.or(green_since).unwrap_or(now).min(now);
    let watched = now - since;
    if watched >= WATCH_WINDOW_SECS {
        return Next::Post(Verdict::Green);
    }
    Next::Wait {
        delay: (WATCH_WINDOW_SECS - watched).clamp(30, WATCH_TICK_SECS),
        green_since: Some(since),
    }
}

/// The hidden marker that ties a comment to one merge.
pub fn marker(merge_sha: &str) -> String {
    format!("<!-- coderhelm:post-merge-health {merge_sha} -->")
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

fn job_line(j: &Job) -> String {
    let what = if j.done() {
        j.conclusion.replace('_', " ")
    } else {
        j.status.replace('_', " ")
    };
    let since = if j.started_at.is_empty() {
        String::new()
    } else {
        format!(", started {}", j.started_at)
    };
    if j.url.is_empty() {
        format!("- **{}** ({what}{since})", j.name)
    } else {
        format!("- [**{}**]({}) ({what}{since})", j.name, j.url)
    }
}

/// The comment for one verdict on one merge.
pub fn render(merge_sha: &str, verdict: &Verdict, follows: &[Follow]) -> String {
    let sha = short(merge_sha);
    let mut out = match verdict {
        Verdict::Green => format!(
            "💚 **Post-merge health check passed**: the deploy of `{sha}` completed and stayed \
             green for the full {}-minute watch window (no new failures vs. the pre-merge \
             baseline).",
            WATCH_WINDOW_SECS / 60
        ),
        Verdict::Failed(jobs) => format!(
            "🔴 **Post-merge health check FAILED**: the merge of `{sha}` has NEW failing \
             check(s) that were not failing before the merge:\n\n{}\n\nFailures that were \
             already there before the merge were ignored. A person should decide on a rollback.",
            jobs.iter().map(job_line).collect::<Vec<_>>().join("\n")
        ),
        Verdict::StillRunning(jobs) => format!(
            "⏳ **Post-merge health check: the deploy of `{sha}` is still running {} hours after \
             the merge.** GitHub normally stops a job long before this, so it looks stuck:\n\n{}\n\n\
             No new failures so far. Check the run above.",
            SAFETY_CEILING_SECS / 3600,
            jobs.iter().map(job_line).collect::<Vec<_>>().join("\n")
        ),
    };
    for f in follows {
        let name = if f.successor.name.is_empty() {
            "the deploy".to_string()
        } else {
            format!("**{}**", f.successor.name)
        };
        let link = if f.successor.url.is_empty() {
            name
        } else {
            format!("[{name}]({})", f.successor.url)
        };
        out.push_str(&format!(
            "\n\nThis commit's own run was superseded by a later merge, so I followed {link} \
             for `{}`, which includes `{sha}`.",
            short(&f.successor.head_sha)
        ));
    }
    out.push_str("\n\n");
    out.push_str(&marker(merge_sha));
    out
}

/// The next tick: a NEW queue message (its own receive count) with the
/// state carried forward. Returns the body and its delay.
pub fn tick_message(msg: &HealthCheckMessage, delay: i64) -> Option<(String, i32)> {
    let mut next = msg.clone();
    next.attempts += 1;
    let mut v = serde_json::to_value(&next).ok()?;
    v.as_object_mut()?
        .insert("type".to_string(), serde_json::json!("health_check"));
    Some((v.to_string(), delay.clamp(0, MAX_DELAY_SECS) as i32))
}

/// Names of base-branch check runs currently in a failing conclusion.
pub async fn failing_checks(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<String> {
    let Ok(v) = github.list_check_runs_for_ref(owner, repo, git_ref).await else {
        return vec![];
    };
    v["check_runs"]
        .as_array()
        .map(|runs| {
            runs.iter()
                .map(Job::from_check_run)
                .filter(|j| j.failed())
                .map(|j| j.name)
                .collect()
        })
        .unwrap_or_default()
}

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

async fn check_runs_of(
    github: &GitHubClient,
    msg: &HealthCheckMessage,
    sha: &str,
) -> Result<Vec<Job>, BoxErr> {
    let v = github
        .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, sha)
        .await?;
    Ok(v["check_runs"]
        .as_array()
        .map(|a| a.iter().map(Job::from_check_run).collect())
        .unwrap_or_default())
}

async fn runs_of(
    github: &GitHubClient,
    msg: &HealthCheckMessage,
    sha: &str,
) -> Result<Vec<WfRun>, BoxErr> {
    Ok(github
        .list_workflow_runs_for_sha(&msg.repo_owner, &msg.repo_name, sha)
        .await?
        .iter()
        .map(WfRun::from_json)
        .filter(|r| r.head_sha == sha)
        .collect())
}

/// The run that deployed `superseded`'s commit: the first later run of the
/// same workflow on the same branch whose commit contains the merge commit.
async fn find_successor(
    github: &GitHubClient,
    msg: &HealthCheckMessage,
    superseded: &WfRun,
) -> Result<Option<WfRun>, BoxErr> {
    let branch = if superseded.head_branch.is_empty() {
        msg.base_branch.as_str()
    } else {
        superseded.head_branch.as_str()
    };
    let later: Vec<WfRun> = github
        .list_push_runs_since(
            &msg.repo_owner,
            &msg.repo_name,
            superseded.workflow_id,
            branch,
            &superseded.created_at,
        )
        .await?
        .iter()
        .map(WfRun::from_json)
        .collect();
    for c in successor_candidates(superseded, &later) {
        let status = github
            .compare_status(&msg.repo_owner, &msg.repo_name, &msg.merge_sha, &c.head_sha)
            .await?;
        if matches!(status.as_str(), "ahead" | "identical") {
            return Ok(Some(c));
        }
    }
    Ok(None)
}

/// Reads the merge commit's deploy, following superseded runs. Returns the
/// jobs, the follows (for the comment) and the follows to carry forward.
async fn read_deploy(
    github: &GitHubClient,
    msg: &HealthCheckMessage,
) -> Result<(Vec<Job>, Vec<Follow>, Vec<FollowedDeploy>), BoxErr> {
    let own_checks = check_runs_of(github, msg, &msg.merge_sha).await?;
    let own_runs = runs_of(github, msg, &msg.merge_sha).await?;
    let mut follows: Vec<Follow> = Vec::new();
    let mut carried: Vec<FollowedDeploy> = Vec::new();
    for run in own_runs
        .iter()
        .filter(|r| r.deploys_commit() && r.superseded())
    {
        let mut current = run.clone();
        let mut found: Option<WfRun> = None;
        for _ in 0..MAX_FOLLOW_HOPS {
            // Reuse the run found on an earlier tick, if any.
            let known = msg
                .followed
                .iter()
                .find(|f| f.superseded_run_id == current.id)
                .cloned();
            let next = match known {
                Some(k) => github
                    .get_workflow_run(&msg.repo_owner, &msg.repo_name, k.run_id)
                    .await
                    .ok()
                    .map(|v| WfRun::from_json(&v)),
                None => None,
            };
            let next = match next {
                Some(n) => Some(n),
                None => find_successor(github, msg, &current).await?,
            };
            let Some(next) = next else { break };
            carried.push(FollowedDeploy {
                superseded_run_id: current.id,
                run_id: next.id,
                head_sha: next.head_sha.clone(),
            });
            let again = next.superseded();
            found = Some(next.clone());
            if !again {
                break;
            }
            current = next;
        }
        if let Some(successor) = found {
            let successor_checks = check_runs_of(github, msg, &successor.head_sha).await?;
            follows.push(Follow {
                superseded: run.clone(),
                successor,
                successor_checks,
            });
        }
    }
    let jobs = assemble(&msg.merge_sha, &own_checks, &own_runs, &follows);
    Ok((jobs, follows, carried))
}

pub async fn run(state: &WorkerState, msg: HealthCheckMessage) -> Result<(), BoxErr> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let now = chrono::Utc::now().timestamp();
    let mut msg = msg;
    let merged_at = *msg.merged_at.get_or_insert(now);

    let (jobs, follows, carried) = match read_deploy(&github, &msg).await {
        Ok(r) => r,
        Err(e) => {
            // A failed read is no verdict: look again on the next tick.
            if now - merged_at < SAFETY_CEILING_SECS {
                warn!(pr = msg.pr_number, error = %e, "Health check: could not read the deploy, looking again");
                send_tick(state, &msg, backoff(msg.attempts)).await;
            } else {
                error!(pr = msg.pr_number, sha = %msg.merge_sha, error = %e,
                    "Health check: safety ceiling reached and the deploy could not be read");
            }
            return Ok(());
        }
    };
    msg.followed = carried;

    let baseline: HashSet<String> = msg.baseline_failed.iter().cloned().collect();
    match decide(
        now,
        merged_at,
        msg.attempts,
        msg.green_since,
        &jobs,
        &baseline,
    ) {
        Next::Wait { delay, green_since } => {
            msg.green_since = green_since;
            info!(
                pr = msg.pr_number,
                sha = %msg.merge_sha,
                running = jobs.iter().filter(|j| !j.done()).count(),
                watching = green_since.is_some(),
                delay,
                "Health check: looking again"
            );
            send_tick(state, &msg, delay).await;
        }
        Next::Quiet => {
            info!(
                pr = msg.pr_number,
                sha = %msg.merge_sha,
                "Health check: no checks or deploy for the merge commit, nothing to report"
            );
        }
        Next::Post(verdict) => {
            if let Verdict::StillRunning(running) = &verdict {
                error!(
                    pr = msg.pr_number,
                    sha = %msg.merge_sha,
                    running = ?running.iter().map(|j| (&j.name, &j.url, &j.started_at)).collect::<Vec<_>>(),
                    "Health check: SAFETY CEILING reached with the deploy still running"
                );
            }
            let body = render(&msg.merge_sha, &verdict, &follows);
            upsert_comment(&github, &msg, &body).await;
            info!(pr = msg.pr_number, sha = %msg.merge_sha, ?verdict, "Health check reported");
        }
    }
    Ok(())
}

/// One comment per merge: edit the one already posted for this merge, or
/// post it.
async fn upsert_comment(github: &GitHubClient, msg: &HealthCheckMessage, body: &str) {
    let mark = marker(&msg.merge_sha);
    let existing = github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
        .ok()
        .and_then(|cs| {
            cs.iter()
                .find(|c| c["body"].as_str().is_some_and(|b| b.contains(&mark)))
                .and_then(|c| c["id"].as_u64())
        });
    let res = match existing {
        Some(id) => {
            github
                .edit_issue_comment(&msg.repo_owner, &msg.repo_name, id, body)
                .await
        }
        None => {
            github
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, body)
                .await
        }
    };
    if let Err(e) = res {
        warn!(pr = msg.pr_number, error = %e, "Health check: could not post the verdict");
    }
}

/// Schedule the first health check after a merge (SQS-delayed). Captures nothing
/// itself — the caller passes the pre-merge `baseline_failed`. Returns false if
/// the queue isn't configured.
#[allow(clippy::too_many_arguments)]
pub async fn schedule(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    base_branch: &str,
    merge_sha: &str,
    baseline_failed: Vec<String>,
) -> bool {
    let msg = HealthCheckMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: repo.to_string(),
        pr_number,
        base_branch: base_branch.to_string(),
        merge_sha: merge_sha.to_string(),
        baseline_failed,
        attempts: 0,
        green_since: None,
        merged_at: Some(chrono::Utc::now().timestamp()),
        followed: Vec::new(),
    };
    send_tick(state, &msg, backoff(0)).await
}

/// Send the next tick as a new message. The handler always completes the
/// message it received, so the check never relies on SQS re-delivering it.
/// Returns true on a successful send.
async fn send_tick(state: &WorkerState, msg: &HealthCheckMessage, delay: i64) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let Some((body, delay)) = tick_message(msg, delay) else {
        warn!("Health check: failed to serialize the next tick");
        return false;
    };
    for attempt in 1..=3 {
        match state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body.clone())
            .delay_seconds(delay)
            .send()
            .await
        {
            Ok(_) => return true,
            Err(e) => warn!(attempt, error = %e, "Health check: sending the next tick failed"),
        }
    }
    error!(pr = msg.pr_number, sha = %msg.merge_sha, "Health check: could not send the next tick");
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn job(name: &str, status: &str, conclusion: &str, suite: u64, sha: &str) -> Job {
        Job {
            name: name.into(),
            status: status.into(),
            conclusion: conclusion.into(),
            url: format!("https://github.com/o/r/actions/runs/{suite}/job/1"),
            started_at: "2026-10-28T15:00:00Z".into(),
            completed_at: if status == "completed" {
                "2026-10-28T15:45:00Z".into()
            } else {
                String::new()
            },
            suite_id: suite,
            head_sha: sha.into(),
        }
    }

    fn t(s: &str) -> i64 {
        epoch(s).unwrap()
    }

    fn none() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn backoff_grows_to_ten_minutes_and_stays_under_the_sqs_cap() {
        let d: Vec<i64> = (0..8).map(backoff).collect();
        assert_eq!(&d[..5], &[60, 120, 300, 600, 600]);
        assert!(d.iter().all(|x| *x <= 900));
    }

    #[test]
    fn a_running_deploy_waits_at_any_tick_count_before_the_ceiling() {
        let merged = t("2026-10-28T15:00:00Z");
        let jobs = vec![job("deploy-prod", "in_progress", "", 1, "m")];
        for ticks in [0, 15, 40, 400] {
            let now = merged + 6 * 3600;
            assert!(matches!(
                decide(now, merged, ticks, None, &jobs, &none()),
                Next::Wait { .. }
            ));
        }
    }

    #[test]
    fn green_window_starts_when_the_deploy_completes() {
        let merged = t("2026-10-28T15:00:00Z");
        let jobs = vec![job("deploy", "completed", "success", 1, "m")];
        // Completed at 15:45: at 15:50 the window has 10 minutes left.
        match decide(t("2026-10-28T15:50:00Z"), merged, 9, None, &jobs, &none()) {
            Next::Wait { delay, green_since } => {
                assert_eq!(green_since, Some(t("2026-10-28T15:45:00Z")));
                assert!(delay <= WATCH_TICK_SECS);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            decide(t("2026-10-28T16:00:00Z"), merged, 12, None, &jobs, &none()),
            Next::Post(Verdict::Green)
        );
    }

    #[test]
    fn no_deploy_waits_out_the_grace_period_then_stays_quiet() {
        let merged = 1_000_000;
        assert!(matches!(
            decide(merged + 300, merged, 2, None, &[], &none()),
            Next::Wait { .. }
        ));
        assert_eq!(
            decide(merged + NO_DEPLOY_GRACE_SECS, merged, 5, None, &[], &none()),
            Next::Quiet
        );
    }

    #[test]
    fn new_failures_are_reported_and_baseline_ones_ignored() {
        let merged = t("2026-10-28T15:00:00Z");
        let jobs = vec![
            job("lint", "completed", "failure", 1, "m"),
            job("e2e", "completed", "failure", 2, "m"),
            job("deploy", "completed", "success", 3, "m"),
        ];
        let baseline: HashSet<String> = ["lint".to_string()].into();
        match decide(t("2026-10-28T15:50:00Z"), merged, 9, None, &jobs, &baseline) {
            Next::Post(Verdict::Failed(f)) => {
                assert_eq!(f.len(), 1);
                assert_eq!(f[0].name, "e2e");
                let body = render("abcdef1234", &Verdict::Failed(f), &[]);
                assert!(body.contains("[**e2e**](https://github.com/o/r/actions/runs/2/job/1)"));
                assert!(!body.contains("lint"));
            }
            other => panic!("{other:?}"),
        }
        // Only baseline failures: green, not failed.
        let only_old = vec![
            job("lint", "completed", "failure", 1, "m"),
            job("deploy", "completed", "success", 3, "m"),
        ];
        assert_eq!(
            decide(
                t("2026-10-28T16:10:00Z"),
                merged,
                9,
                None,
                &only_old,
                &baseline
            ),
            Next::Post(Verdict::Green)
        );
    }

    #[test]
    fn the_safety_ceiling_names_the_running_job_and_since_when() {
        let merged = t("2026-10-28T15:00:00Z");
        let jobs = vec![
            job("build", "completed", "success", 1, "m"),
            job("deploy-prod", "in_progress", "", 2, "m"),
        ];
        let v = decide(
            merged + SAFETY_CEILING_SECS,
            merged,
            60,
            None,
            &jobs,
            &none(),
        );
        let Next::Post(verdict @ Verdict::StillRunning(_)) = v else {
            panic!("{v:?}")
        };
        let body = render("abcdef1234", &verdict, &[]);
        assert!(body.contains("[**deploy-prod**](https://github.com/o/r/actions/runs/2/job/1)"));
        assert!(body.contains("started 2026-10-28T15:00:00Z"));
        assert!(!body.contains("build"));
        assert!(!body.to_lowercase().contains("inconclusive"));
    }

    #[test]
    fn every_comment_carries_the_merge_marker() {
        for v in [
            Verdict::Green,
            Verdict::Failed(vec![job("e2e", "completed", "failure", 1, "m")]),
            Verdict::StillRunning(vec![job("d", "queued", "", 1, "m")]),
        ] {
            let body = render("abcdef1234", &v, &[]);
            assert!(body.ends_with(&marker("abcdef1234")));
            assert!(!body.to_lowercase().contains("inconclusive"));
        }
    }

    #[test]
    fn a_tick_is_a_new_message_with_its_state() {
        let msg = HealthCheckMessage {
            pr_number: 7,
            merge_sha: "m".into(),
            attempts: 3,
            merged_at: Some(100),
            green_since: Some(200),
            followed: vec![FollowedDeploy {
                superseded_run_id: 1,
                run_id: 2,
                head_sha: "n".into(),
            }],
            ..Default::default()
        };
        let (body, delay) = tick_message(&msg, 5000).unwrap();
        assert_eq!(delay, 900, "SQS caps a delay at 15 minutes");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["type"], "health_check");
        assert_eq!(v["attempts"], 4);
        let back: crate::models::WorkerMessage = serde_json::from_str(&body).unwrap();
        let crate::models::WorkerMessage::HealthCheck(back) = back else {
            panic!()
        };
        assert_eq!(back.merged_at, Some(100));
        assert_eq!(back.green_since, Some(200));
        assert_eq!(back.followed, msg.followed);
    }

    #[test]
    fn an_old_message_without_the_new_fields_still_reads() {
        let raw = r#"{"type":"health_check","team_id":"T","installation_id":1,"repo_owner":"o","repo_name":"r","pr_number":1,"base_branch":"main","merge_sha":"m","attempts":14}"#;
        let m: crate::models::WorkerMessage = serde_json::from_str(raw).unwrap();
        let crate::models::WorkerMessage::HealthCheck(m) = m else {
            panic!()
        };
        assert_eq!(m.merged_at, None);
        assert!(m.followed.is_empty());
    }

    #[test]
    fn successor_candidates_are_later_push_runs_of_the_same_workflow_oldest_first() {
        let cancelled = WfRun {
            id: 10,
            workflow_id: 5,
            head_sha: "m".into(),
            event: "push".into(),
            created_at: "2026-10-28T15:00:00Z".into(),
            status: "completed".into(),
            conclusion: "cancelled".into(),
            ..Default::default()
        };
        let mk = |id, wf, sha: &str, ev: &str, at: &str| WfRun {
            id,
            workflow_id: wf,
            head_sha: sha.into(),
            event: ev.into(),
            created_at: at.into(),
            ..Default::default()
        };
        let later = vec![
            mk(14, 5, "q", "push", "2026-10-28T15:20:00Z"),
            mk(12, 5, "n", "push", "2026-10-28T15:05:00Z"),
            mk(11, 6, "n", "push", "2026-10-28T15:04:00Z"),
            mk(13, 5, "p", "pull_request", "2026-10-28T15:06:00Z"),
            mk(9, 5, "o", "push", "2026-10-28T14:00:00Z"),
            cancelled.clone(),
        ];
        let ids: Vec<u64> = successor_candidates(&cancelled, &later)
            .iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, vec![12, 14]);
    }
}
