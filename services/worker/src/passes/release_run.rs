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

use super::release_plan::{self, Env, Kind, On, Planned, Status, Step};
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

/// The release steps a person must unblock (no workflow deploys an
/// environment, ...), one line each: what, and the grant that lifts it.
pub fn person_lines(steps: &[Step]) -> Vec<String> {
    steps
        .iter()
        .filter(|s| s.status == Status::NoAccess)
        .map(|s| {
            let what = s.what.split_whitespace().collect::<Vec<_>>().join(" ");
            let why = s.detail.split_whitespace().collect::<Vec<_>>().join(" ");
            format!(
                "- {}: {}.",
                what.trim_end_matches('.'),
                why.trim_end_matches('.')
            )
        })
        .collect()
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
    arm_steps(
        state,
        team_id,
        installation_id,
        (owner, repo, pr_number),
        run_id,
        (head_branch, base_branch),
        planned.steps.clone(),
        context,
    )
    .await;
}

/// Saves `steps` as the pull request's release (with a fresh tick chain,
/// so any older chain stops) and starts its ticks. Steps unchanged from
/// the saved release keep their progress.
#[allow(clippy::too_many_arguments)]
async fn arm_steps(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    (owner, repo, pr_number): (&str, &str, u64),
    run_id: &str,
    (head_branch, base_branch): (&str, &str),
    mut steps: Vec<Step>,
    context: Vec<String>,
) {
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
    let title = "Release steps (done by CoderHelm)";
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "{title}: {} {} armed (listed in the pull request description)",
            planned.steps.len(),
            if planned.steps.len() == 1 {
                "step"
            } else {
                "steps"
            }
        ),
    )
    .await;
    // The steps are CoderHelm's own work: they live in the description and
    // on the run page. Only a step nobody can do until a person grants
    // something is said on the ticket, one line each.
    let asks = person_lines(&planned.steps);
    let sunk = common::person_gate::sink(&format!("**Needed from you:**\n{}", asks.join("\n")));
    if !asks.is_empty() && !sunk.human.is_empty() {
        let body = sunk.text;
        super::post_ticket_notice(
            state,
            msg,
            run_id,
            github,
            NOTICE_RELEASE,
            &body,
            &super::fix_location::jira_text(&body),
            title,
            &[],
        )
        .await;
    }
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

/// The status slot the release keeps in the pull request's one status
/// comment: said only while a step waits on a person's approval.
const SLOT_RELEASE: &str = "release";

/// The deploys a merge goes through, laid over the release already armed
/// (`old`): each deploy step that is not done and now names another
/// workflow or trigger (it waited on a deploy that never runs for this
/// change) is pointed at `fresh`'s, a deploy missing from it is added
/// (staging's before production's, which then waits for it), and every
/// other step keeps what it has. `None` when nothing changes.
pub fn refollow(old: &[Step], fresh: &[Step]) -> Option<Vec<Step>> {
    let mut out = old.to_vec();
    for f in fresh.iter().filter(|f| f.kind == Kind::Deploy) {
        match out.iter().position(|o| o.id == f.id) {
            Some(i) => {
                let o = &out[i];
                if o.status == Status::Done || (o.how == f.how && o.workflow == f.workflow) {
                    continue;
                }
                let after = o.after.clone();
                out[i] = Step { after, ..f.clone() };
            }
            None => {
                let mut s = f.clone();
                s.after.retain(|a| out.iter().any(|o| &o.id == a));
                let at = match f.env {
                    // Before the first production deploy, which then waits
                    // for staging's.
                    Env::Staging => out
                        .iter()
                        .position(|o| o.kind == Kind::Deploy && o.env == Env::Production),
                    Env::Production => None,
                };
                match at {
                    Some(at) => {
                        if !out[at].after.contains(&s.id) {
                            out[at].after.push(s.id.clone());
                        }
                        out.insert(at, s);
                    }
                    None => out.push(s),
                }
            }
        }
    }
    (out != old).then_some(out)
}

/// A merged pull request, whoever merged it: its release follows the
/// deploys that run for the files it changed. A release the plan armed is
/// re-pointed where it waited on a deploy that never runs for this change
/// ([`refollow`]); a pull request with none gets its deploys followed
/// ([`release_plan::follow_steps`]). Each finished step is said on the run
/// page in one line; a step that needs a person's approval is said once on
/// the pull request. Safe to call again: an unchanged release is left as
/// it is, its tick chain running.
pub async fn follow_merge(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    installation_id: u64,
    (owner, repo, pr): (&str, &str, u64),
    run_id: &str,
) {
    let p = match github.get_pull_request(owner, repo, pr).await {
        Ok(p) => p,
        Err(e) => {
            warn!(pr, error = %e, "release: could not read the merged pull request");
            return;
        }
    };
    let merge_sha = p["merge_commit_sha"].as_str().unwrap_or("");
    if !p["merged"].as_bool().unwrap_or(false) || merge_sha.is_empty() {
        return;
    }
    let base = p["base"]["ref"].as_str().unwrap_or("");
    let head = p["head"]["ref"].as_str().unwrap_or("");
    let changed: Vec<String> = match github.list_pull_files(owner, repo, pr).await {
        Ok(f) => f.into_iter().map(|(path, _)| path).collect(),
        Err(e) => {
            warn!(pr, error = %e, "release: could not list the merged pull request's files");
            Vec::new()
        }
    };
    let m = super::outside_work::load(github, owner, repo, merge_sha, &[], false)
        .await
        .release
        .for_change(&changed);
    let fresh = release_plan::follow_steps(&m);
    let old = load(state, team_id, owner, repo, pr).await.ok().flatten();
    let (steps, mut context) = match &old {
        None if fresh.is_empty() => return,
        None => (fresh, Vec::new()),
        Some(o) => match refollow(&o.steps, &fresh) {
            // Unchanged: armed again all the same, so a tick chain that
            // stopped before the merge (a tick GitHub's budget held) goes on
            // from the merge. A fresh chain id ends any chain still running.
            None if rearm(&o.steps, true) => (o.steps.clone(), o.context.clone()),
            None => return,
            Some(s) => (s, o.context.clone()),
        },
    };
    // The deploy line names the deploys this release now waits on.
    context.retain(|l| !l.starts_with("How it deploys:"));
    let lines = release_plan::note_lines(&release_plan::Release::default(), &steps, &m);
    context.splice(
        0..0,
        lines
            .into_iter()
            .filter(|l| !l.starts_with(|c: char| c.is_ascii_digit())),
    );
    let names: Vec<String> = steps
        .iter()
        .filter(|s| s.kind == Kind::Deploy && !s.status.finished())
        .map(|s| format!("{} ({})", s.env.word(), s.how))
        .collect();
    info!(
        pr,
        run_id,
        steps = steps.len(),
        "release: following the merge's deploys"
    );
    if !run_id.is_empty() && !names.is_empty() {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("Following the merge's deploys: {}", names.join("; then ")),
        )
        .await;
    }
    arm_steps(
        state,
        team_id,
        installation_id,
        (owner, repo, pr),
        run_id,
        (head, base),
        steps,
        context,
    )
    .await;
}

/// The one short line said on the pull request while a release step waits
/// for a person's approval; `None` when none waits.
pub fn approval_line(steps: &[Step]) -> Option<String> {
    let s = steps.iter().find(|s| s.status == Status::Approval)?;
    let what = match (&s.trigger, s.env) {
        (Some(On::Tag(p)), _) => format!("The production release tag (`{p}`)"),
        (_, env) => format!("The {} step \"{}\"", env.word(), s.what),
    };
    Some(format!(
        "{what} waits for a person's approval on the Agent permissions page, as the team's release \
         settings ask. CoderHelm does the rest once it is approved."
    ))
}

/// Release tags newest first: `(name, commit)` of every tag shaped like a
/// version under `prefix` (pre-releases left out), highest version first.
pub fn releases_newest_first(tags: &Value, prefix: &str) -> Vec<(String, String)> {
    fn version(t: &str, prefix: &str) -> Option<Vec<u64>> {
        let core = t.strip_prefix(prefix).or_else(|| t.strip_prefix('v'))?;
        core.split('.')
            .map(|p| p.parse::<u64>().ok())
            .collect::<Option<Vec<u64>>>()
            .filter(|v| !v.is_empty())
    }
    let mut out: Vec<(Vec<u64>, String, String)> = tags
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter_map(|t| {
            let name = t["name"].as_str()?;
            let sha = t["commit"]["sha"].as_str()?;
            Some((version(name, prefix)?, name.to_string(), sha.to_string()))
        })
        .collect();
    out.sort_by(|a, b| b.0.cmp(&a.0));
    out.into_iter().map(|(_, n, s)| (n, s)).collect()
}

/// How many of the newest release tags are compared with a merge to find
/// the first one that contains it.
const TAGS_LOOKED_AT: usize = 5;

/// The production deploy of a repository whose team releases merged changes
/// (its `auto_tag` release setting): the release tag that first contains
/// this merge, once there is one; until then the merge waits for the
/// repository's batched release (opened here when no window waits, so a
/// missed push never leaves the merge out), or says why that release is
/// blocked. Never a card of its own: the team's setting already said yes.
async fn team_release(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &ReleaseStepsMessage,
    cfg: &super::review_actions::OnApproveConfig,
    base_branch: &str,
    s: &mut Step,
    merge_sha: &str,
) {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let tags = match github.list_tags(owner, repo).await {
        Ok(t) => t,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "release: could not read the release tags");
            return;
        }
    };
    let mut first: Option<(String, String)> = None;
    for (name, sha) in releases_newest_first(&tags, &cfg.tag_prefix)
        .into_iter()
        .take(TAGS_LOOKED_AT)
    {
        let contains = sha == merge_sha
            || match github.compare_status(owner, repo, merge_sha, &sha).await {
                Ok(st) => super::release_notes::tag_on_branch(&st),
                Err(e) => {
                    warn!(pr = msg.pr_number, error = %e, "release: could not compare the merge with a release tag");
                    return;
                }
            };
        if !contains {
            break;
        }
        first = Some((name, sha));
    }
    if let Some((tag, sha)) = first {
        s.tag = tag;
        s.tag_sha = sha;
        s.status = Status::Running;
        s.started_at = now();
        s.detail = format!(
            "in the release tag `{}`; waiting for `{}`",
            s.tag, s.workflow
        );
        return;
    }
    if let Some(why) =
        super::review_actions::release_blocked(state, &msg.team_id, owner, repo).await
    {
        s.detail = format!(
            "merged; the release is blocked: {}. \"Release now\" on the Releases page releases it",
            why.trim().trim_end_matches('.')
        );
        return;
    }
    super::review_actions::schedule_tag_sweep(
        state,
        cfg,
        &msg.team_id,
        msg.installation_id,
        owner,
        repo,
        msg.pr_number,
        base_branch,
        false,
    )
    .await;
    s.detail = format!(
        "merged, deploying: in the repository's next release tag, one tag for everything merged \
         (within about {} minutes once `{base_branch}` is green)",
        cfg.tag_batch_minutes
    );
}

/// The one line said when a production deploy finished in this tick
/// (`was` is its status before it): deployed, in which tag and when, or
/// that it didn't. None for anything else.
pub fn deploy_outcome_line(
    s: &Step,
    was: Status,
    owner: &str,
    repo: &str,
    at: chrono::DateTime<chrono::Utc>,
) -> Option<String> {
    if s.kind != Kind::Deploy || s.env != Env::Production || was == s.status {
        return None;
    }
    let tag = if s.tag.is_empty() {
        String::new()
    } else {
        format!(" in `{}`", s.tag)
    };
    let run = if s.run_id > 0 {
        format!(
            " (https://github.com/{owner}/{repo}/actions/runs/{})",
            s.run_id
        )
    } else {
        String::new()
    };
    match s.status {
        Status::Done => Some(format!(
            "Deployed to production{tag} at {} UTC{run}.",
            at.format("%H:%M")
        )),
        Status::Failed => Some(format!(
            "Merged, but the production deploy{tag} did not succeed: {}. The post-merge check \
             follows it up.",
            s.detail.trim_end_matches('.')
        )),
        _ => None,
    }
}

/// Notice key: the production deploy's outcome, once per ticket.
const NOTICE_DEPLOYED: &str = "release_deployed";

/// Says `line` once on the run's ticket (Jira or GitHub issue).
async fn say_on_ticket(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    run_id: &str,
    line: &str,
) {
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .send()
        .await
    {
        Ok(o) => o.item.unwrap_or_default(),
        Err(e) => {
            warn!(run_id, error = %e, "release: could not read the run for its ticket");
            return;
        }
    };
    let Some(msg) = ticket_of_run(team_id, run_id, &item) else {
        return;
    };
    super::post_ticket_notice(
        state,
        &msg,
        run_id,
        github,
        NOTICE_DEPLOYED,
        line,
        &super::fix_location::jira_text(line),
        "Released",
        &[],
    )
    .await;
}

/// The ticket a run record names, as a message the ticket notices take.
/// None when the run has no ticket to speak on.
pub fn ticket_of_run(
    team_id: &str,
    run_id: &str,
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Option<crate::models::TicketMessage> {
    use crate::models::TicketSource;
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let source = match s("ticket_source").as_str() {
        "jira" => TicketSource::Jira,
        "github" | "" if n("issue_number") > 0 => TicketSource::Github,
        _ => return None,
    };
    let repo = s("repo");
    let (owner, name) = repo.split_once('/')?;
    Some(crate::models::TicketMessage {
        github_wait: Default::default(),
        team_id: team_id.to_string(),
        installation_id: n("installation_id"),
        source,
        ticket_id: s("ticket_id"),
        title: s("title"),
        body: String::new(),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        issue_number: n("issue_number"),
        sender: String::new(),
        requester: None,
        base_branch: s("base_branch"),
        image_attachments: vec![],
        continuation: 0,
        continuation_run_id: None,
        first_attempt_ms: 0,
        resume_from: None,
        retry_of_run: false,
        model_retries: 0,
        ticket_type: None,
        ticket_update_note: None,
        ticket_history_note: None,
        plan_cache_key: None,
        team_instructions: String::new(),
        coder_learning: String::new(),
        person_direction: None,
        answers: Vec::new(),
        related: Vec::new(),
        new_run_id: Some(run_id.to_string()),
        followup_of: None,
        after_merge: false,
        merged_note: None,
    })
}

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
    let Some(rec) = load(state, &msg.team_id, owner, repo, pr).await? else {
        return Ok(());
    };
    if rec.chain_id != msg.chain_id || rec.parked {
        return Ok(());
    }
    let armed_at = rec.armed_at;
    match tick(state, &msg, rec).await {
        Ok(()) => Ok(()),
        Err(e) => {
            // A tick that failed (GitHub's budget held, a GitHub error) never
            // ends the release: the same chain is tried again, at the reset
            // when GitHub asked to wait.
            if now() - armed_at <= MAX_AGE_SECS {
                let mut next_msg = msg.clone();
                next_msg.attempts += 1;
                let d = retry_delay(crate::clients::github::rate_limited(&*e));
                warn!(pr, delay = d, error = %e, "release: the tick failed; trying again");
                send(state, &next_msg, d).await;
            }
            Err(e)
        }
    }
}

/// When a failed tick is tried again: just past GitHub's wait when it asked
/// for one (within a queue delay's bounds), else the waiting pace.
pub fn retry_delay(limited: Option<u64>) -> i32 {
    match limited {
        Some(w) => (w + 5).clamp(60, 900) as i32,
        None => WAITING_DELAY,
    }
}

/// Whether an unchanged release is armed again on a merge: a step is still
/// to do once it merged.
pub fn rearm(steps: &[Step], merged: bool) -> bool {
    (0..steps.len()).any(|i| matches!(next(steps, i, merged), Next::Start | Next::Check))
}

async fn tick(
    state: &WorkerState,
    msg: &ReleaseStepsMessage,
    mut rec: Record,
) -> Result<(), BoxError> {
    let (owner, repo, pr) = (&msg.repo_owner, &msg.repo_name, msg.pr_number);
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
    let was: Vec<Status> = rec.steps.iter().map(|s| s.status).collect();
    let asked_before = approval_line(&rec.steps);
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
                if s.kind == Kind::Check {
                    let st = &mut rec.steps[i];
                    page_check(
                        state,
                        &github,
                        &msg.team_id,
                        owner,
                        repo,
                        pr,
                        &msg.run_id,
                        st,
                    )
                    .await;
                    continue;
                }
                // The team releases merged changes itself (its batched release
                // tag): the production deploy goes out in that tag, with no
                // card of its own, and is followed to the end.
                if merged && s.kind == Kind::Deploy && matches!(s.trigger, Some(On::Tag(_))) {
                    let cfg = super::review_actions::OnApproveConfig::load(
                        state,
                        &msg.team_id,
                        owner,
                        repo,
                    )
                    .await;
                    if cfg.auto_tag {
                        let base = rec.base_branch.clone();
                        team_release(
                            state,
                            &github,
                            msg,
                            &cfg,
                            &base,
                            &mut rec.steps[i],
                            &merge_sha,
                        )
                        .await;
                        continue;
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
            if let Err(e) =
                crate::passes::description::save(&github, owner, repo, pr, &updated).await
            {
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
    // A production deploy that finished this tick is said once on the run
    // and on its ticket: deployed (in which tag, when), or that it failed.
    if !msg.run_id.is_empty() {
        for (s, w) in rec.steps.iter().zip(&was) {
            if let Some(line) = deploy_outcome_line(s, *w, owner, repo, chrono::Utc::now()) {
                super::add_progress_note(state, &msg.team_id, &msg.run_id, &line).await;
                say_on_ticket(state, &github, &msg.team_id, &msg.run_id, &line).await;
            }
        }
    }
    // A step waiting on a person's approval is said once, in the pull
    // request's one status comment; once decided the line is dropped.
    let asked = approval_line(&rec.steps);
    if asked != asked_before {
        let say = match &asked {
            Some(line) => common::conversation::Say::status(SLOT_RELEASE, line.clone()),
            None => {
                common::conversation::Say::status(SLOT_RELEASE, "The release approval was decided.")
                    .for_nobody()
            }
        };
        if let Err(e) = super::conversation::github(&github, owner, repo, pr, say).await {
            warn!(pr, error = %e, "release: could not update the approval line");
        }
    }
    if let (Some(d), false) = (again, rec.parked) {
        let mut next_msg = msg.clone();
        next_msg.attempts += 1;
        send(state, &next_msg, d).await;
    }
    Ok(())
}

/// A release check: each page loaded (`check_url`) and searched for its
/// terms; done when all return 200 with every term.
#[allow(clippy::too_many_arguments)]
async fn page_check(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    run_id: &str,
    s: &mut Step,
) {
    let web = super::web_check::WebCheck::load(
        state,
        team_id,
        &format!("{owner}/{repo}"),
        "CoderHelm (release steps)",
        pr,
        &s.urls.join(" "),
    )
    .await
    .with_run(run_id, "release steps".to_string());
    let mut bad: Vec<String> = Vec::new();
    for u in &s.urls {
        match web.verify_find(state, github, u, &s.find).await {
            Ok(c) => {
                let st = c.status().unwrap_or(0);
                let absent: Vec<&str> = c
                    .found
                    .iter()
                    .filter(|(_, h)| h.is_empty())
                    .map(|(t, _)| t.as_str())
                    .collect();
                if st != 200 || !absent.is_empty() {
                    bad.push(format!(
                        "{u}: {st}{}",
                        if absent.is_empty() {
                            String::new()
                        } else {
                            format!(", lacks \"{}\"", absent.join("\", \""))
                        }
                    ));
                }
            }
            Err(e) => bad.push(format!("{u}: {}", e.text())),
        }
    }
    if bad.is_empty() {
        s.status = Status::Done;
        s.detail = format!("{} page(s) return 200 with every term", s.urls.len());
    } else {
        s.status = Status::Failed;
        s.detail = bad.join("; ");
    }
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
                s.tag_sha = merge_sha.to_string();
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
                s.tag_sha = merge_sha.to_string();
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
        // A batched tag can point past this merge: its run is on the tag's
        // commit.
        Some(On::Tag(_)) if !s.tag_sha.is_empty() => (s.tag.clone(), s.tag_sha.clone()),
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

    #[test]
    fn a_runs_ticket_is_read_from_its_record() {
        let mut item = std::collections::HashMap::new();
        let put = |m: &mut std::collections::HashMap<String, AttributeValue>, k: &str, v: &str| {
            m.insert(k.to_string(), AttributeValue::S(v.to_string()));
        };
        put(&mut item, "ticket_source", "jira");
        put(&mut item, "ticket_id", "ABC-1");
        put(&mut item, "repo", "o/r");
        put(&mut item, "base_branch", "main");
        item.insert("installation_id".into(), AttributeValue::N("7".into()));
        let m = ticket_of_run("T", "R", &item).unwrap();
        assert!(matches!(m.source, crate::models::TicketSource::Jira));
        assert_eq!((m.ticket_id.as_str(), m.repo_name.as_str()), ("ABC-1", "r"));
        assert_eq!(m.installation_id, 7);
        // A GitHub issue needs its number; an alert has no ticket to speak on.
        put(&mut item, "ticket_source", "github");
        assert!(ticket_of_run("T", "R", &item).is_none());
        item.insert("issue_number".into(), AttributeValue::N("12".into()));
        assert_eq!(ticket_of_run("T", "R", &item).unwrap().issue_number, 12);
        put(&mut item, "ticket_source", "alert");
        assert!(ticket_of_run("T", "R", &item).is_none());
    }

    #[test]
    fn a_merge_deploy_is_said_without_a_tag() {
        let mut s = plan()
            .into_iter()
            .find(|s| s.id == "deploy_production")
            .unwrap();
        s.trigger = Some(On::Merge("main".into()));
        s.status = Status::Done;
        let at = chrono::Utc::now();
        let line = deploy_outcome_line(&s, Status::Running, "o", "r", at).unwrap();
        assert!(line.starts_with("Deployed to production at "), "{line}");
        // Staging's deploy is not said on the ticket.
        let st = plan()
            .into_iter()
            .find(|s| s.id == "deploy_staging")
            .unwrap();
        let mut st2 = st.clone();
        st2.status = Status::Done;
        assert!(deploy_outcome_line(&st2, Status::Running, "o", "r", at).is_none());
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
