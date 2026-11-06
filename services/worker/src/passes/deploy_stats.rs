//! Deploys CoderHelm started (`common::deploy_stats`): a completed workflow
//! run, from its webhook or read back from GitHub's run history, recorded
//! when CoderHelm's app triggered it and it deployed somewhere.
//!
//! The backfill reads each repository's run history a week at a time and
//! saves where it stands after every page, so a pass that runs out of time,
//! or that GitHub's rate limit pauses, continues from that page. A
//! repository that keeps failing is given up on after a few tries and named
//! as not read; the team is marked done once every repository is read or
//! named. One pass reads at a time per team (a lease on the progress item).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Duration, Utc};
use common::deploy_stats::{self as ds, JobDef};
use common::messages::{DeployRunMessage, DeployStatsBackfillMessage};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::clients::github::{is_not_found, rate_limited, GitHubClient};
use crate::WorkerState;

type GhError = Box<dyn std::error::Error + Send + Sync>;
type Item = HashMap<String, AttributeValue>;

fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn num(n: impl ToString) -> AttributeValue {
    AttributeValue::N(n.to_string())
}

/// A workflow file's jobs are read again after this long.
const WORKFLOW_CACHE_SECS: i64 = 3600;
/// A backfill pass stops here and queues the next one, well inside the
/// worker's 15 minutes.
const PASS_BUDGET_SECS: u64 = 600;
/// How long a pass holds the team's lease: longer than any pass can run.
const LEASE_SECS: i64 = 960;
/// Run history is read a week at a time.
const CHUNK_DAYS: i64 = 7;
/// GitHub returns at most 1,000 runs for one filtered query: a range with
/// more is halved until it fits (down to an hour).
const RUNS_CAP: u64 = 1000;
const MIN_RANGE_SECS: i64 = 3600;
/// A repository that fails this many passes in a row, reading nothing, is
/// given up on and named as not read.
const MAX_REPO_ATTEMPTS: u32 = 3;
/// A repository GitHub's rate limit stops this many times in a row before it
/// reads a page is given up on too.
const MAX_REPO_PAUSES: u32 = 24;
/// The longest delay a queued message can carry.
const MAX_DELAY_SECS: u64 = 900;
/// The wait before a pass tries the repositories that failed again.
const RETRY_DELAY_SECS: u64 = 120;
/// How many times a webhook's run waits out a rate limit before it is
/// dropped.
const MAX_RUN_ATTEMPTS: u32 = 5;
/// The most GitHub requests one backfill pass makes, whatever the budget.
const MAX_PASS_REQUESTS: usize = 1000;
/// The wait before the pass after one that used its requests.
const CAP_DELAY_SECS: u64 = 300;

/// GitHub refused because the app has no access to Actions here.
fn is_no_access(e: &(dyn std::error::Error + Send + Sync)) -> bool {
    let m = e.to_string();
    m.contains("(403 Forbidden)") && m.contains("not accessible by integration")
}

/// Passes a rate limit up; anything else is swallowed by the caller.
fn only_pause(e: GhError) -> Result<(), GhError> {
    if rate_limited(&*e).is_some() {
        Err(e)
    } else {
        Ok(())
    }
}

fn secs(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// One page of one range of a repository's run history: where a read
/// stands, saved after every page.
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub page: u32,
}

impl Cursor {
    /// GitHub's `created` filter for the range.
    pub fn created(&self) -> String {
        format!("{}..{}", secs(self.from), secs(self.to))
    }

    fn week_from(from: DateTime<Utc>, now: DateTime<Utc>) -> Option<Cursor> {
        (from < now).then(|| Cursor {
            from,
            to: (from + Duration::days(CHUNK_DAYS)).min(now),
            page: 1,
        })
    }

    /// The first range of the window.
    pub fn first(since: DateTime<Utc>, now: DateTime<Utc>) -> Option<Cursor> {
        Self::week_from(since, now)
    }

    /// The range after this one, None once the window is read to `now`.
    pub fn next(&self, now: DateTime<Utc>) -> Option<Cursor> {
        Self::week_from(self.to, now)
    }

    /// Where a repository's read continues: the saved page, unless it fell
    /// out of the window, else the window's start.
    pub fn resume(
        saved: Option<&Cursor>,
        since: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<Cursor> {
        match saved {
            Some(c) if c.to > since && c.from < now => Some(c.clone()),
            _ => Self::first(since, now),
        }
    }

    /// The first half of this range, read alone when the range holds more
    /// runs than GitHub returns. None when it is already an hour or less.
    pub fn halved(&self) -> Option<Cursor> {
        let span = self.to - self.from;
        (span.num_seconds() > MIN_RANGE_SECS).then(|| Cursor {
            from: self.from,
            to: self.from + span / 2,
            page: 1,
        })
    }
}

/// Where one repository's backfill stands, from its marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RepoState {
    #[default]
    New,
    Reading,
    Done,
    NoAccess,
    Gone,
    NotRead,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RepoMark {
    pub state: RepoState,
    pub cursor: Option<Cursor>,
    pub attempts: u32,
    pub pauses: u32,
    pub reason: String,
}

impl RepoMark {
    /// Read from the repository's marker. A marker from before `read_state`
    /// existed was only written once the repository was read.
    pub fn from_item(item: Option<&Item>) -> RepoMark {
        let Some(i) = item else {
            return RepoMark::default();
        };
        let s = |k: &str| i.get(k).and_then(|v| v.as_s().ok()).cloned();
        let n = |k: &str| {
            i.get(k)
                .and_then(|v| v.as_n().ok())
                .and_then(|x| x.parse::<u32>().ok())
                .unwrap_or(0)
        };
        let flag = |k: &str| i.get(k).and_then(|v| v.as_bool().ok()) == Some(&true);
        let state = match s("read_state").as_deref() {
            Some("reading") => RepoState::Reading,
            Some("done") => RepoState::Done,
            Some("no_access") => RepoState::NoAccess,
            Some("gone") => RepoState::Gone,
            Some("not_read") => RepoState::NotRead,
            Some(_) => RepoState::Reading,
            None if flag("no_access") => RepoState::NoAccess,
            None if flag("gone") => RepoState::Gone,
            None => RepoState::Done,
        };
        let cursor = match (
            s("cursor_from").as_deref().and_then(parse_time),
            s("cursor_to").as_deref().and_then(parse_time),
        ) {
            (Some(from), Some(to)) => Some(Cursor {
                from,
                to,
                page: n("cursor_page").max(1),
            }),
            _ => None,
        };
        RepoMark {
            state,
            cursor,
            attempts: n("attempts"),
            pauses: n("pauses"),
            reason: s("reason").unwrap_or_default(),
        }
    }
}

/// What a pass does with a repository whose read stopped on an error.
#[derive(Debug, Clone, PartialEq)]
pub enum AfterError {
    /// GitHub's rate limit: save, and continue in a pass after this wait.
    Pause(u64),
    /// Try again in the next pass.
    Retry,
    /// Stop trying: the repository is named as not read, with this reason.
    GiveUp(String),
}

/// A plain reason from an error: GitHub's status when it gave one.
fn plain_reason(message: &str) -> String {
    let status = message
        .split_once("error (")
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(s, _)| s.to_string());
    match status {
        Some(s) => format!("GitHub answered {s}"),
        None => common::truncate_str(message.trim(), 160).to_string(),
    }
}

/// `wait`: GitHub's rate limit asked for this wait. `progressed`: the pass
/// read at least a page of this repository before the error, which starts
/// the count of failures in a row again.
pub fn after_error(
    wait: Option<u64>,
    message: &str,
    mark: &RepoMark,
    progressed: bool,
) -> AfterError {
    if let Some(w) = wait {
        let pauses = if progressed { 1 } else { mark.pauses + 1 };
        return if pauses >= MAX_REPO_PAUSES {
            AfterError::GiveUp("GitHub kept refusing for its rate limit".into())
        } else {
            AfterError::Pause(w)
        };
    }
    let attempts = if progressed { 1 } else { mark.attempts + 1 };
    if attempts >= MAX_REPO_ATTEMPTS {
        AfterError::GiveUp(plain_reason(message))
    } else {
        AfterError::Retry
    }
}

/// What the deploy reads need from GitHub and storage, so the reading is the
/// same code under test.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// GET a path under the repository.
    async fn get(&self, rest: &str) -> Result<Value, GhError>;
    /// A workflow file's jobs, None when it can't be read. An error only for
    /// GitHub's rate limit.
    async fn workflow_jobs(&self, path: &str) -> Result<Option<Vec<JobDef>>, GhError>;
    /// Writes one deploy record.
    async fn save_deploy(&self, item: Item) -> bool;
    /// Saves where the repository's backfill stands.
    async fn save_cursor(&self, cursor: &Cursor);
    /// While GitHub's budget for the installation is below the share
    /// background reads may spend: the seconds until it refills.
    fn headroom_wait(&self) -> Option<u64>;
    /// GitHub requests this pass has made so far.
    fn requests(&self) -> usize;
}

/// What the worker knows about one repository while it reads its deploys.
pub struct RepoReads<S: Source> {
    pub src: S,
    pub team_id: String,
    /// `owner/name`
    pub repo: String,
    pub bot: String,
    pub default_branch: String,
    pub setting: HashMap<String, String>,
    /// Workflow jobs by path, read once per pass.
    workflows: HashMap<String, Option<Vec<JobDef>>>,
    /// Whether a pushed ref is a tag, by name, asked once per pass.
    tags: HashMap<String, bool>,
}

impl<S: Source> RepoReads<S> {
    pub fn new(
        src: S,
        team_id: String,
        repo: String,
        bot: String,
        default_branch: String,
        setting: HashMap<String, String>,
    ) -> Self {
        RepoReads {
            src,
            team_id,
            repo,
            bot,
            default_branch,
            setting,
            workflows: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    async fn jobs_of(&mut self, path: &str) -> Result<Option<Vec<JobDef>>, GhError> {
        if path.is_empty() {
            return Ok(None);
        }
        if let Some(j) = self.workflows.get(path) {
            return Ok(j.clone());
        }
        let j = self.src.workflow_jobs(path).await?;
        self.workflows.insert(path.to_string(), j.clone());
        Ok(j)
    }

    async fn is_tag(&mut self, name: &str) -> Result<bool, GhError> {
        if let Some(t) = self.tags.get(name) {
            return Ok(*t);
        }
        let path: Vec<String> = name
            .split('/')
            .map(|s| urlencoding::encode(s).into_owned())
            .collect();
        let t = match self
            .src
            .get(&format!("git/ref/tags/{}", path.join("/")))
            .await
        {
            Ok(_) => true,
            Err(e) => {
                only_pause(e)?;
                false
            }
        };
        self.tags.insert(name.to_string(), t);
        Ok(t)
    }

    async fn merged_by_us(&self, run: &Value) -> Result<bool, GhError> {
        let sha = run["head_sha"].as_str().unwrap_or("");
        if sha.is_empty() {
            return Ok(false);
        }
        match self.src.get(&format!("commits/{sha}/pulls")).await {
            Ok(v) => Ok(ds::merged_by_coderhelm(
                v.as_array().map(|a| a.as_slice()).unwrap_or(&[]),
                &self.bot,
            )),
            Err(e) => only_pause(e).map(|_| false),
        }
    }

    /// Records a completed run when CoderHelm started it and it deployed.
    /// Returns how many deploys it wrote. Everything known without asking
    /// GitHub is checked first: who started it, then whether its workflow can
    /// deploy at all (the setting, its name, its file, its jobs read once per
    /// workflow). Only a run that may be a deploy costs requests of its own.
    /// An error only for GitHub's rate limit.
    pub async fn record(&mut self, run: &Value) -> Result<usize, GhError> {
        if run["status"].as_str().is_some_and(|s| s != "completed") {
            return Ok(0);
        }
        let branch = run["head_branch"].as_str().unwrap_or("").to_string();
        let event = run["event"].as_str().unwrap_or("");
        let started = ds::started_by_coderhelm(run, &self.bot);
        // A merge queue's push of a PR CoderHelm may have merged.
        let queue_push = !started
            && ds::trigger(run).1 == "Bot"
            && event == "push"
            && !branch.is_empty()
            && branch == self.default_branch;
        if !started && !queue_push {
            return Ok(0);
        }
        let path = run["path"].as_str().unwrap_or("").to_string();
        let file = path.rsplit('/').next().unwrap_or("").to_ascii_lowercase();
        let named = self.setting.contains_key(&file)
            || ds::says_deploy(run["name"].as_str().unwrap_or(""))
            || ds::says_deploy(&file);
        let jobs = self.jobs_of(&path).await?;
        let maybe = named
            || jobs
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(|j| !j.environment.is_empty() || ds::says_deploy(&j.name));
        if !maybe {
            return Ok(0);
        }
        if queue_push && !self.merged_by_us(run).await? {
            return Ok(0);
        }
        let is_tag =
            event == "push" && branch != self.default_branch && self.is_tag(&branch).await?;
        let cause = ds::cause_of(run, &self.default_branch, is_tag);
        let run_id = run["id"].as_u64().unwrap_or(0);
        let ran: Option<Vec<Value>> = match self
            .src
            .get(&format!("actions/runs/{run_id}/jobs?per_page=100"))
            .await
        {
            Ok(v) => v["jobs"].as_array().cloned(),
            Err(e) => {
                only_pause(e)?;
                warn!(run_id, "Deploys: could not read a run's jobs");
                None
            }
        };
        let targets = ds::run_targets(run, jobs.as_deref(), ran.as_deref(), &self.setting);
        let mut written = 0;
        for t in &targets {
            let item = ds::stat_item(&self.team_id, &self.repo, run, cause, t);
            if self.src.save_deploy(item).await {
                written += 1;
            }
        }
        Ok(written)
    }
}

/// How one repository's backfill ended.
#[derive(Debug, PartialEq)]
pub enum RepoBackfill {
    Done,
    NoAccess,
    OutOfTime,
    /// The installation's budget is below the background share: wait this
    /// long for it to refill.
    Headroom(u64),
    /// The pass made its most requests.
    Capped,
}

/// Reads a repository's run history from `start` to `now`, saving the
/// cursor after every page. `progressed` is set once a page was read.
pub async fn backfill_repo<S: Source>(
    reads: &mut RepoReads<S>,
    start: Option<Cursor>,
    now: DateTime<Utc>,
    out_of_time: &(dyn Fn() -> bool + Send + Sync),
    progressed: &mut bool,
) -> Result<RepoBackfill, GhError> {
    let mut written = 0usize;
    let mut cur = start;
    while let Some(c) = cur.take() {
        if out_of_time() {
            return Ok(RepoBackfill::OutOfTime);
        }
        // Live work keeps its share of the budget: background reads yield
        // before every page, and stop at the pass's most requests.
        if let Some(wait) = reads.src.headroom_wait() {
            return Ok(RepoBackfill::Headroom(wait));
        }
        if reads.src.requests() >= MAX_PASS_REQUESTS {
            return Ok(RepoBackfill::Capped);
        }
        let rest = format!(
            "actions/runs?actor={}&status=completed&created={}&per_page=100&page={}",
            urlencoding::encode(&reads.bot),
            urlencoding::encode(&c.created()),
            c.page
        );
        let v = match reads.src.get(&rest).await {
            Ok(v) => v,
            Err(e) if is_no_access(&*e) => return Ok(RepoBackfill::NoAccess),
            Err(e) => return Err(e),
        };
        if c.page == 1 && v["total_count"].as_u64().unwrap_or(0) > RUNS_CAP {
            if let Some(h) = c.halved() {
                reads.src.save_cursor(&h).await;
                cur = Some(h);
                continue;
            }
            warn!(repo = %reads.repo, range = %c.created(), "Deploys backfill: more runs in an hour than GitHub lists");
        }
        let runs = v["workflow_runs"].as_array().cloned().unwrap_or_default();
        for run in &runs {
            written += reads.record(run).await?;
        }
        *progressed = true;
        let next = if runs.len() < 100 || c.page >= 10 {
            c.next(now)
        } else {
            Some(Cursor {
                page: c.page + 1,
                ..c
            })
        };
        if let Some(n) = &next {
            reads.src.save_cursor(n).await;
        }
        cur = next;
    }
    info!(repo = %reads.repo, written, "Deploys backfill: repository read");
    Ok(RepoBackfill::Done)
}

/// The live source: GitHub, and the settings table.
struct Live<'a> {
    state: &'a WorkerState,
    team_id: String,
    github: &'a GitHubClient,
    owner: String,
    name: String,
    mark_sk: String,
    /// GitHub requests made by the pass, shared by its repositories.
    requests: &'a AtomicUsize,
}

#[async_trait::async_trait]
impl Source for Live<'_> {
    fn headroom_wait(&self) -> Option<u64> {
        self.github.background_wait()
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    async fn get(&self, rest: &str) -> Result<Value, GhError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.github
            .get_repo_path(&self.owner, &self.name, rest)
            .await
    }

    async fn workflow_jobs(&self, path: &str) -> Result<Option<Vec<JobDef>>, GhError> {
        workflow_jobs(
            self.requests,
            self.state,
            &self.team_id,
            self.github,
            &self.owner,
            &self.name,
            path,
        )
        .await
    }

    async fn save_deploy(&self, item: Item) -> bool {
        match self
            .state
            .dynamo
            .put_item()
            .table_name(&self.state.config.settings_table_name)
            .set_item(Some(item))
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                warn!(error = %e, "Deploys: could not record a deploy");
                false
            }
        }
    }

    async fn save_cursor(&self, c: &Cursor) {
        update(
            self.state,
            &self.team_id,
            &self.mark_sk,
            &[
                ("read_state", av("reading")),
                ("cursor_from", av(&secs(c.from))),
                ("cursor_to", av(&secs(c.to))),
                ("cursor_page", num(c.page)),
            ],
            &[],
        )
        .await;
    }
}

/// A workflow file's jobs, cached in settings for an hour. An error only for
/// GitHub's rate limit.
async fn workflow_jobs(
    requests: &AtomicUsize,
    state: &WorkerState,
    team_id: &str,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    path: &str,
) -> Result<Option<Vec<JobDef>>, GhError> {
    let table = &state.config.settings_table_name;
    let sk = format!("DWF#{owner}/{repo}#{path}").to_ascii_lowercase();
    let now = Utc::now().timestamp();
    if let Ok(r) = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", av(team_id))
        .key("sk", av(&sk))
        .send()
        .await
    {
        if let Some(i) = r.item() {
            let at: i64 = i
                .get("checked_at")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if now - at < WORKFLOW_CACHE_SECS {
                let jobs: Vec<Value> = i
                    .get("jobs")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                return Ok(Some(
                    jobs.iter()
                        .map(|j| JobDef {
                            key: j["key"].as_str().unwrap_or("").into(),
                            name: j["name"].as_str().unwrap_or("").into(),
                            environment: j["environment"].as_str().unwrap_or("").into(),
                        })
                        .collect(),
                ));
            }
        }
    }
    requests.fetch_add(1, Ordering::Relaxed);
    let text = match github.read_file_live(owner, repo, path, "HEAD").await {
        Ok(t) => t,
        Err(e) => {
            if rate_limited(&*e).is_some() {
                return Err(e);
            }
            warn!(path, error = %e, "Deploys: could not read a workflow file");
            return Ok(None);
        }
    };
    let Some(jobs) = ds::parse_workflow(&text) else {
        return Ok(None);
    };
    let stored: Vec<Value> = jobs
        .iter()
        .map(|j| json!({"key": j.key, "name": j.name, "environment": j.environment}))
        .collect();
    let _ = state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", av(team_id))
        .item("sk", av(&sk))
        .item("jobs", av(&Value::Array(stored).to_string()))
        .item("checked_at", num(now))
        .item("ttl", num(now + 86_400))
        .send()
        .await;
    Ok(Some(jobs))
}

/// The repository's deploy workflows setting, from its review config.
async fn deploy_workflows_setting(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
) -> HashMap<String, String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(&format!("REVIEW_CONFIG#REPO#{repo}")))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());
    ds::parse_deploy_workflows(
        item.as_ref()
            .and_then(|i| i.get("deploy_workflows"))
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .unwrap_or(""),
    )
}

async fn live_reads<'a>(
    state: &'a WorkerState,
    team_id: &str,
    github: &'a GitHubClient,
    requests: &'a AtomicUsize,
    owner: &str,
    name: &str,
    bot: &str,
) -> Result<RepoReads<Live<'a>>, GhError> {
    requests.fetch_add(1, Ordering::Relaxed);
    let default_branch = github.get_default_branch(owner, name).await?;
    let full = format!("{owner}/{name}");
    Ok(RepoReads::new(
        Live {
            state,
            team_id: team_id.to_string(),
            github,
            owner: owner.to_string(),
            name: name.to_string(),
            mark_sk: repo_sk(&full),
            requests,
        },
        team_id.to_string(),
        full.clone(),
        bot.to_string(),
        default_branch,
        deploy_workflows_setting(state, team_id, &full).await,
    ))
}

fn repo_sk(full_name: &str) -> String {
    format!(
        "{}{}",
        ds::BACKFILL_REPO_PREFIX,
        full_name.to_ascii_lowercase()
    )
}

/// Sends a message to the tickets queue, after `delay` seconds.
async fn send(state: &WorkerState, body: String, delay: u64) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay.min(MAX_DELAY_SECS) as i32)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Deploys: could not queue a message");
            false
        }
    }
}

/// Records one completed workflow run from its webhook. GitHub's rate limit
/// sends the run back to wait, a few times, instead of dropping it.
pub async fn record_run(state: &WorkerState, msg: &DeployRunMessage) -> Result<(), String> {
    let Some(github) = super::plan_chat::github_for(state, &msg.team_id).await else {
        return Err("no GitHub installation for the team".into());
    };
    let requests = AtomicUsize::new(0);
    let outcome: Result<usize, GhError> = async {
        let bot = github.app_bot_login().await?;
        let mut reads = live_reads(
            state,
            &msg.team_id,
            &github,
            &requests,
            &msg.repo_owner,
            &msg.repo_name,
            &bot,
        )
        .await?;
        reads.record(&msg.run).await
    }
    .await;
    let run_id = msg.run["id"].as_u64().unwrap_or(0);
    match outcome {
        Ok(n) => {
            if n > 0 {
                info!(run_id, deploys = n, "Deploy recorded");
            }
            Ok(())
        }
        Err(e) => match rate_limited(&*e) {
            Some(wait) if msg.attempts < MAX_RUN_ATTEMPTS => {
                let mut again = msg.clone();
                again.attempts += 1;
                let body = serde_json::to_string(&crate::models::WorkerMessage::DeployRun(again))
                    .map_err(|e| e.to_string())?;
                if send(state, body, wait).await {
                    info!(run_id, wait, "Deploy run waits out GitHub's rate limit");
                    Ok(())
                } else {
                    Err(format!("rate limited and could not queue again: {e}"))
                }
            }
            _ => Err(e.to_string()),
        },
    }
}

/// SET (and REMOVE) attributes of one settings item, creating it when new.
async fn update(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
    sets: &[(&str, AttributeValue)],
    removes: &[&str],
) {
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(sk));
    let mut set = vec!["#at = :at".to_string()];
    req = req
        .expression_attribute_names("#at", "at")
        .expression_attribute_values(":at", av(&Utc::now().to_rfc3339()));
    for (i, (k, v)) in sets.iter().enumerate() {
        set.push(format!("#s{i} = :s{i}"));
        req = req
            .expression_attribute_names(format!("#s{i}"), *k)
            .expression_attribute_values(format!(":s{i}"), v.clone());
    }
    let mut expr = format!("SET {}", set.join(", "));
    if !removes.is_empty() {
        let names: Vec<String> = (0..removes.len()).map(|i| format!("#r{i}")).collect();
        for (i, k) in removes.iter().enumerate() {
            req = req.expression_attribute_names(format!("#r{i}"), *k);
        }
        expr.push_str(&format!(" REMOVE {}", names.join(", ")));
    }
    if let Err(e) = req.update_expression(expr).send().await {
        warn!(sk, error = %e, "Deploys backfill: could not write a marker");
    }
}

const CURSOR_FIELDS: [&str; 3] = ["cursor_from", "cursor_to", "cursor_page"];

/// Marks a repository finished: read, no access, gone, or not read.
async fn finish_repo(state: &WorkerState, team_id: &str, sk: &str, how: &str, reason: &str) {
    let mut sets = vec![("read_state", av(how))];
    if how == "no_access" {
        sets.push(("no_access", AttributeValue::Bool(true)));
    }
    if !reason.is_empty() {
        sets.push(("reason", av(reason)));
    }
    update(state, team_id, sk, &sets, &CURSOR_FIELDS).await;
}

/// Takes the team's lease for this pass: free, expired, or already this
/// pass's own (a pass the backfill queued for itself). None while another
/// pass holds it; else when GitHub's rate limit lets reading resume (epoch
/// seconds, 0 when it isn't waiting).
async fn claim_lease(
    state: &WorkerState,
    team_id: &str,
    lease: &str,
) -> Result<Option<i64>, String> {
    let now = Utc::now().timestamp();
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(ds::BACKFILL_PROGRESS_SK))
        .update_expression("SET #l = :l, #u = :u")
        .condition_expression("attribute_not_exists(#u) OR #u < :now OR #l = :l")
        .expression_attribute_names("#l", "lease")
        .expression_attribute_names("#u", "lease_until")
        .expression_attribute_values(":l", av(lease))
        .expression_attribute_values(":u", num(now + LEASE_SECS))
        .expression_attribute_values(":now", num(now))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await;
    match r {
        Ok(out) => Ok(Some(
            out.attributes()
                .and_then(|a| a.get("resume_at"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
        )),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            Ok(None)
        }
        Err(e) => Err(format!("claim the backfill lease: {e}")),
    }
}

async fn progress(state: &WorkerState, team_id: &str, sets: &[(&str, AttributeValue)]) {
    update(state, team_id, ds::BACKFILL_PROGRESS_SK, sets, &[]).await;
}

/// How a pass ended.
enum PassEnd {
    /// Every repository read or named: the team is marked done.
    Finished,
    /// Continue in another pass after this many seconds.
    Again(u64),
}

/// Queues the pass that continues this one, holding the lease until it runs.
/// Lets the lease go when it can't be queued, so the next ask starts over.
async fn queue_next_pass(state: &WorkerState, team_id: &str, lease: &str, delay: u64) {
    let delay = delay.min(MAX_DELAY_SECS);
    let until = Utc::now().timestamp() + delay as i64 + LEASE_SECS;
    progress(state, team_id, &[("lease_until", num(until))]).await;
    let body = serde_json::to_string(&crate::models::WorkerMessage::DeployStatsBackfill(
        DeployStatsBackfillMessage {
            team_id: team_id.to_string(),
            lease: Some(lease.to_string()),
        },
    ))
    .unwrap_or_default();
    if !send(state, body, delay).await {
        progress(state, team_id, &[("lease_until", num(0))]).await;
    }
}

/// Reads a team's deploys of the last `BACKFILL_DAYS` from GitHub's run
/// history. Safe to repeat: a record is keyed by its run, each repository
/// saves its cursor after every page, and one pass reads at a time. A pass
/// that runs out of time, or that GitHub's rate limit pauses, queues the
/// next one; the team is marked done once every repository is read or named
/// as not readable.
pub async fn backfill(state: &WorkerState, msg: &DeployStatsBackfillMessage) -> Result<(), String> {
    let team_id = msg.team_id.as_str();
    let done = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", av(team_id))
        .key("sk", av(ds::BACKFILL_MARKER_SK))
        .send()
        .await
        .map_err(|e| format!("read backfill marker: {e}"))?;
    if done.item().is_some() {
        return Ok(());
    }
    let lease = msg
        .lease
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let Some(resume_at) = claim_lease(state, team_id, &lease).await? else {
        info!(team_id, "Deploys backfill: another pass is reading");
        return Ok(());
    };
    // GitHub asked for a longer wait than one queued delay covers: wait on
    // without asking it anything.
    let wait = resume_at - Utc::now().timestamp();
    if wait > 0 {
        queue_next_pass(state, team_id, &lease, wait as u64).await;
        return Ok(());
    }
    match backfill_pass(state, team_id).await {
        Ok(PassEnd::Finished) => {
            update(
                state,
                team_id,
                ds::BACKFILL_PROGRESS_SK,
                &[],
                &["lease", "lease_until"],
            )
            .await;
            Ok(())
        }
        Ok(PassEnd::Again(delay)) => {
            queue_next_pass(state, team_id, &lease, delay).await;
            Ok(())
        }
        Err(e) => {
            progress(state, team_id, &[("lease_until", num(0))]).await;
            Err(e)
        }
    }
}

async fn backfill_pass(state: &WorkerState, team_id: &str) -> Result<PassEnd, String> {
    let table = &state.config.settings_table_name;
    let started = Instant::now();
    let out_of_time = move || started.elapsed().as_secs() > PASS_BUDGET_SECS;
    let Some(github) = super::plan_chat::github_for(state, team_id).await else {
        return Err("no GitHub installation for the team".into());
    };
    let bot = match github.app_bot_login().await {
        Ok(b) => b,
        Err(e) => {
            return match rate_limited(&*e) {
                Some(wait) => Ok(PassEnd::Again(wait)),
                None => Err(format!("bot login: {e}")),
            }
        }
    };
    // Below the background share of the budget: wait for the reset before
    // spending anything.
    if let Some(wait) = github.background_wait() {
        info!(
            team_id,
            wait, "Deploys backfill: leaving GitHub's budget to live work"
        );
        progress(
            state,
            team_id,
            &[("resume_at", num(Utc::now().timestamp() + wait as i64))],
        )
        .await;
        return Ok(PassEnd::Again(wait));
    }
    let requests = AtomicUsize::new(0);
    let now = Utc::now();
    let since = now - Duration::days(ds::BACKFILL_DAYS);
    let repos = super::plan::fetch_team_repo_list(state, team_id).await;
    let total = repos.len();
    let mut read = 0usize;
    let mut no_access = Vec::<String>::new();
    let mut not_read = HashMap::<String, AttributeValue>::new();
    let mut retry_later = false;
    let counts = |read: usize| [("repos_total", num(total)), ("repos_read", num(read))];
    for repo in &repos {
        let Some((owner, name)) = repo.full_name.split_once('/') else {
            read += 1;
            continue;
        };
        let sk = repo_sk(&repo.full_name);
        let mark = RepoMark::from_item(
            state
                .dynamo
                .get_item()
                .table_name(table)
                .key("pk", av(team_id))
                .key("sk", av(&sk))
                .send()
                .await
                .ok()
                .and_then(|r| r.item().cloned())
                .as_ref(),
        );
        match mark.state {
            RepoState::Done | RepoState::Gone => {
                read += 1;
                continue;
            }
            RepoState::NoAccess => {
                no_access.push(repo.full_name.clone());
                read += 1;
                continue;
            }
            RepoState::NotRead => {
                not_read.insert(repo.full_name.clone(), av(&mark.reason));
                read += 1;
                continue;
            }
            RepoState::New | RepoState::Reading => {}
        }
        if out_of_time() {
            progress(state, team_id, &counts(read)).await;
            return Ok(PassEnd::Again(0));
        }
        let mut progressed = false;
        let outcome = match live_reads(state, team_id, &github, &requests, owner, name, &bot).await
        {
            Ok(mut reads) => {
                backfill_repo(
                    &mut reads,
                    Cursor::resume(mark.cursor.as_ref(), since, now),
                    now,
                    &out_of_time,
                    &mut progressed,
                )
                .await
            }
            Err(e) => Err(e),
        };
        match outcome {
            Ok(RepoBackfill::Done) => {
                finish_repo(state, team_id, &sk, "done", "").await;
                read += 1;
            }
            Ok(RepoBackfill::NoAccess) => {
                finish_repo(state, team_id, &sk, "no_access", "").await;
                no_access.push(repo.full_name.clone());
                read += 1;
            }
            Ok(RepoBackfill::OutOfTime) => {
                info!(
                    team_id,
                    "Deploys backfill: out of time, continuing in the next pass"
                );
                progress(state, team_id, &counts(read)).await;
                return Ok(PassEnd::Again(0));
            }
            Ok(RepoBackfill::Headroom(wait)) => {
                info!(
                    team_id,
                    wait, "Deploys backfill: leaving GitHub's budget to live work"
                );
                let mut sets = counts(read).to_vec();
                sets.push(("resume_at", num(Utc::now().timestamp() + wait as i64)));
                progress(state, team_id, &sets).await;
                return Ok(PassEnd::Again(wait));
            }
            Ok(RepoBackfill::Capped) => {
                info!(
                    team_id,
                    requests = requests.load(Ordering::Relaxed),
                    "Deploys backfill: the pass made its most requests"
                );
                progress(state, team_id, &counts(read)).await;
                return Ok(PassEnd::Again(CAP_DELAY_SECS));
            }
            // Gone or no longer installed: nothing to read.
            Err(e) if is_not_found(&*e) => {
                finish_repo(state, team_id, &sk, "gone", "").await;
                read += 1;
            }
            Err(e) => {
                let message = e.to_string();
                match after_error(rate_limited(&*e), &message, &mark, progressed) {
                    AfterError::Pause(wait) => {
                        let pauses = if progressed { 1 } else { mark.pauses + 1 };
                        update(
                            state,
                            team_id,
                            &sk,
                            &[("read_state", av("reading")), ("pauses", num(pauses))],
                            &[],
                        )
                        .await;
                        info!(team_id, repo = %repo.full_name, wait, "Deploys backfill: paused by GitHub's rate limit");
                        let resume_at = Utc::now().timestamp() + wait as i64;
                        let mut sets = counts(read).to_vec();
                        sets.push(("resume_at", num(resume_at)));
                        progress(state, team_id, &sets).await;
                        return Ok(PassEnd::Again(wait));
                    }
                    AfterError::Retry => {
                        let attempts = if progressed { 1 } else { mark.attempts + 1 };
                        warn!(repo = %repo.full_name, attempts, error = %message, "Deploys backfill: could not read run history; trying again");
                        update(
                            state,
                            team_id,
                            &sk,
                            &[
                                ("read_state", av("reading")),
                                ("attempts", num(attempts)),
                                ("reason", av(&plain_reason(&message))),
                            ],
                            &[],
                        )
                        .await;
                        retry_later = true;
                    }
                    AfterError::GiveUp(reason) => {
                        warn!(repo = %repo.full_name, error = %message, "Deploys backfill: giving up on a repository");
                        finish_repo(state, team_id, &sk, "not_read", &reason).await;
                        not_read.insert(repo.full_name.clone(), av(&reason));
                        read += 1;
                    }
                }
            }
        }
        progress(state, team_id, &counts(read)).await;
    }
    progress(state, team_id, &counts(read)).await;
    if retry_later {
        return Ok(PassEnd::Again(RETRY_DELAY_SECS));
    }
    let mut req = state
        .dynamo
        .put_item()
        .table_name(table)
        .item("pk", av(team_id))
        .item("sk", av(ds::BACKFILL_MARKER_SK))
        .item("at", av(&Utc::now().to_rfc3339()))
        .item("since", av(&secs(since)))
        .item("repos", num(total));
    if !no_access.is_empty() {
        req = req.item("no_access", AttributeValue::Ss(no_access));
    }
    if !not_read.is_empty() {
        req = req.item("not_read", AttributeValue::M(not_read));
    }
    req.send()
        .await
        .map_err(|e| format!("write backfill marker: {e}"))?;
    info!(team_id, repos = total, "Deploys backfill done");
    Ok(PassEnd::Finished)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    const BOT: &str = "coderhelm[bot]";

    fn t(s: &str) -> DateTime<Utc> {
        parse_time(s).unwrap()
    }

    #[test]
    fn the_backfill_reads_the_whole_window_a_week_at_a_time() {
        let now = t("2026-11-05T12:00:00Z");
        let since = now - Duration::days(ds::BACKFILL_DAYS);
        let mut ranges = Vec::new();
        let mut c = Cursor::first(since, now);
        while let Some(x) = c {
            ranges.push(x.created());
            c = x.next(now);
        }
        assert_eq!(ranges.len(), 26);
        assert_eq!(ranges[0], "2026-05-07T12:00:00Z..2026-05-14T12:00:00Z");
        assert!(ranges.last().unwrap().ends_with("..2026-11-05T12:00:00Z"));
        // Each range starts where the last ended: nothing skipped.
        for w in ranges.windows(2) {
            assert_eq!(w[0].split("..").nth(1), w[1].split("..").next());
        }
        assert!(Cursor::first(now, now).is_none());
    }

    #[test]
    fn a_read_resumes_from_its_saved_page() {
        let now = t("2026-11-05T12:00:00Z");
        let since = now - Duration::days(ds::BACKFILL_DAYS);
        let saved = Cursor {
            from: t("2026-08-01T00:00:00Z"),
            to: t("2026-08-08T00:00:00Z"),
            page: 4,
        };
        // The saved page, not the window's start.
        assert_eq!(
            Cursor::resume(Some(&saved), since, now),
            Some(saved.clone())
        );
        // A cursor that fell out of the window starts over at its start.
        let old = Cursor {
            from: since - Duration::days(30),
            to: since - Duration::days(23),
            page: 2,
        };
        assert_eq!(
            Cursor::resume(Some(&old), since, now),
            Cursor::first(since, now)
        );
        assert_eq!(Cursor::resume(None, since, now), Cursor::first(since, now));
        // Saved and read back from the marker.
        let item: Item = HashMap::from([
            ("read_state".to_string(), av("reading")),
            ("cursor_from".to_string(), av("2026-08-01T00:00:00Z")),
            ("cursor_to".to_string(), av("2026-08-08T00:00:00Z")),
            ("cursor_page".to_string(), num(4)),
            ("attempts".to_string(), num(1)),
        ]);
        let m = RepoMark::from_item(Some(&item));
        assert_eq!(m.state, RepoState::Reading);
        assert_eq!(m.cursor, Some(saved));
        assert_eq!(m.attempts, 1);
    }

    #[test]
    fn markers_from_before_the_cursor_still_read_as_finished() {
        assert_eq!(RepoMark::from_item(None).state, RepoState::New);
        let done: Item = HashMap::from([("at".to_string(), av("x"))]);
        assert_eq!(RepoMark::from_item(Some(&done)).state, RepoState::Done);
        let no: Item = HashMap::from([("no_access".to_string(), AttributeValue::Bool(true))]);
        assert_eq!(RepoMark::from_item(Some(&no)).state, RepoState::NoAccess);
        let gone: Item = HashMap::from([("gone".to_string(), AttributeValue::Bool(true))]);
        assert_eq!(RepoMark::from_item(Some(&gone)).state, RepoState::Gone);
        let given_up: Item = HashMap::from([
            ("read_state".to_string(), av("not_read")),
            ("reason".to_string(), av("GitHub answered 502 Bad Gateway")),
        ]);
        let m = RepoMark::from_item(Some(&given_up));
        assert_eq!(m.state, RepoState::NotRead);
        assert_eq!(m.reason, "GitHub answered 502 Bad Gateway");
    }

    #[test]
    fn a_range_with_more_runs_than_github_lists_is_halved() {
        let c = Cursor {
            from: t("2026-08-01T00:00:00Z"),
            to: t("2026-08-08T00:00:00Z"),
            page: 1,
        };
        let h = c.halved().unwrap();
        assert_eq!(h.created(), "2026-08-01T00:00:00Z..2026-08-04T12:00:00Z");
        let hour = Cursor {
            from: c.from,
            to: c.from + Duration::hours(1),
            page: 1,
        };
        assert!(hour.halved().is_none());
    }

    #[test]
    fn failures_are_bounded_and_rate_limits_pause() {
        let fresh = RepoMark::default();
        let msg = "HTTP status server error (502 Bad Gateway) for url (u)";
        // A rate limit is a pause, never a failure.
        assert_eq!(
            after_error(Some(300), "x", &fresh, false),
            AfterError::Pause(300)
        );
        assert_eq!(after_error(None, msg, &fresh, false), AfterError::Retry);
        let twice = RepoMark {
            attempts: MAX_REPO_ATTEMPTS - 1,
            ..Default::default()
        };
        assert_eq!(
            after_error(None, msg, &twice, false),
            AfterError::GiveUp("GitHub answered 502 Bad Gateway".into())
        );
        // A pass that read pages before failing starts the count again.
        assert_eq!(after_error(None, msg, &twice, true), AfterError::Retry);
        let stuck = RepoMark {
            pauses: MAX_REPO_PAUSES - 1,
            ..Default::default()
        };
        assert!(matches!(
            after_error(Some(60), "x", &stuck, false),
            AfterError::GiveUp(_)
        ));
        assert_eq!(
            after_error(Some(60), "x", &stuck, true),
            AfterError::Pause(60)
        );
    }

    #[test]
    fn missing_actions_access_is_told_apart_from_other_refusals() {
        let no: GhError = "HTTP status client error (403 Forbidden) for url (u): {\"message\":\"Resource not accessible by integration\"}".into();
        assert!(is_no_access(&*no));
        let limited: GhError =
            "HTTP status client error (403 Forbidden) for url (u): API rate limit exceeded".into();
        assert!(!is_no_access(&*limited));
        assert!(rate_limited(&*limited).is_some());
        let gone: GhError = "HTTP status client error (404 Not Found) for url (u)".into();
        assert!(!is_no_access(&*gone));
    }

    /// A fake GitHub: run history by `created` range and page, workflow
    /// files by path, and every request it was asked.
    #[derive(Default)]
    struct Fake {
        runs: HashMap<(String, u32), Value>,
        workflows: HashMap<String, Vec<JobDef>>,
        asked: Mutex<Vec<String>>,
        workflow_reads: AtomicUsize,
        saved: Mutex<Vec<Cursor>>,
        deploys: AtomicUsize,
        /// Requests containing this answer with GitHub's rate limit.
        limit_on: Option<String>,
        /// The installation's budget as GitHub's last response said (the
        /// clock reads 1,000).
        budget: Mutex<Option<crate::clients::github::Budget>>,
        /// Requests the pass made before this repository.
        extra_requests: usize,
    }

    fn limited() -> GhError {
        "HTTP status client error (429 Too Many Requests) for url (u)".into()
    }

    #[async_trait::async_trait]
    impl Source for &Fake {
        async fn get(&self, rest: &str) -> Result<Value, GhError> {
            self.asked.lock().unwrap().push(rest.to_string());
            if self.limit_on.as_deref().is_some_and(|l| rest.contains(l)) {
                return Err(limited());
            }
            if rest.starts_with("actions/runs?") {
                let created = rest
                    .split("created=")
                    .nth(1)
                    .and_then(|s| s.split('&').next())
                    .map(|s| urlencoding::decode(s).unwrap().into_owned())
                    .unwrap();
                let page: u32 = rest.rsplit("page=").next().unwrap().parse().unwrap();
                return Ok(self
                    .runs
                    .get(&(created, page))
                    .cloned()
                    .unwrap_or(json!({"total_count": 0, "workflow_runs": []})));
            }
            if rest.contains("/jobs") {
                return Ok(json!({"jobs": [{"name": "deploy", "conclusion": "success"}]}));
            }
            Err("HTTP status client error (404 Not Found) for url (u)".into())
        }

        async fn workflow_jobs(&self, path: &str) -> Result<Option<Vec<JobDef>>, GhError> {
            self.workflow_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.workflows.get(path).cloned())
        }

        async fn save_deploy(&self, _item: Item) -> bool {
            self.deploys.fetch_add(1, Ordering::SeqCst);
            true
        }

        async fn save_cursor(&self, cursor: &Cursor) {
            self.saved.lock().unwrap().push(cursor.clone());
        }

        fn headroom_wait(&self) -> Option<u64> {
            crate::clients::github::background_wait(*self.budget.lock().unwrap(), 1_000)
        }

        fn requests(&self) -> usize {
            self.asked.lock().unwrap().len() + self.extra_requests
        }
    }

    fn bot_run(id: u64, event: &str, branch: &str, name: &str, path: &str) -> Value {
        json!({
            "id": id, "event": event, "head_branch": branch, "name": name,
            "path": path, "status": "completed", "conclusion": "success",
            "head_sha": "abc", "created_at": "2026-10-30T01:02:03Z",
            "actor": {"login": BOT, "type": "Bot"},
            "triggering_actor": {"login": BOT, "type": "Bot"},
        })
    }

    fn reads(f: &Fake) -> RepoReads<&Fake> {
        RepoReads::new(
            f,
            "TEAM#t".into(),
            "acme/web".into(),
            BOT.into(),
            "main".into(),
            HashMap::new(),
        )
    }

    #[tokio::test]
    async fn a_run_that_cannot_deploy_costs_no_request_of_its_own() {
        let mut f = Fake::default();
        f.workflows.insert(
            ".github/workflows/ci.yml".into(),
            vec![JobDef {
                key: "test".into(),
                name: "test".into(),
                environment: String::new(),
            }],
        );
        let mut r = reads(&f);
        // CoderHelm's pushes to its own PR branch, a CI workflow: no tag
        // lookup, no jobs read, and the workflow file read once.
        for id in 0..50 {
            let run = bot_run(
                id,
                "push",
                "coderhelm/fix-1",
                "CI",
                ".github/workflows/ci.yml",
            );
            assert_eq!(r.record(&run).await.unwrap(), 0);
        }
        assert!(f.asked.lock().unwrap().is_empty(), "{:?}", f.asked);
        assert_eq!(f.workflow_reads.load(Ordering::SeqCst), 1);
        // A person's run is never read further.
        let mut person = bot_run(99, "push", "main", "Deploy", "d.yml");
        person["triggering_actor"] = json!({"login": "someone", "type": "User"});
        assert_eq!(r.record(&person).await.unwrap(), 0);
        assert_eq!(f.workflow_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_deploy_run_is_recorded_and_its_tag_asked_once() {
        let f = Fake::default();
        let mut r = reads(&f);
        for id in 0..3 {
            let run = bot_run(id, "push", "v1.2.3", "Deploy production", "deploy.yml");
            assert_eq!(r.record(&run).await.unwrap(), 1);
        }
        let asked = f.asked.lock().unwrap().clone();
        assert_eq!(
            asked
                .iter()
                .filter(|a| a.starts_with("git/ref/tags/"))
                .count(),
            1
        );
        assert_eq!(asked.iter().filter(|a| a.contains("/jobs")).count(), 3);
        assert_eq!(f.deploys.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_rate_limit_stops_the_read_with_its_cursor_saved() {
        let now = t("2026-11-05T12:00:00Z");
        let start = Cursor {
            from: t("2026-10-20T00:00:00Z"),
            to: t("2026-10-27T00:00:00Z"),
            page: 1,
        };
        let full: Vec<Value> = (0..100)
            .map(|i| bot_run(i, "push", "fix", "CI", "ci.yml"))
            .collect();
        let mut f = Fake {
            limit_on: Some("page=2".into()),
            ..Default::default()
        };
        f.runs.insert(
            (start.created(), 1),
            json!({"total_count": 150, "workflow_runs": full}),
        );
        let mut r = reads(&f);
        let mut progressed = false;
        let e = backfill_repo(&mut r, Some(start.clone()), now, &|| false, &mut progressed)
            .await
            .unwrap_err();
        // A pause, not a failure: the pass requeues and continues at page 2.
        assert!(rate_limited(&*e).is_some());
        assert!(progressed);
        let saved = f.saved.lock().unwrap().clone();
        assert_eq!(
            saved.last(),
            Some(&Cursor {
                page: 2,
                ..start.clone()
            })
        );
        assert_eq!(
            after_error(
                rate_limited(&*e),
                &e.to_string(),
                &RepoMark::default(),
                true
            ),
            AfterError::Pause(60)
        );
        // The next pass picks up at that page and reads to the end.
        let f2 = Fake::default();
        let mut r2 = reads(&f2);
        let mut p2 = false;
        let resumed = Cursor::resume(saved.last(), now - Duration::days(182), now);
        let end = backfill_repo(&mut r2, resumed, now, &|| false, &mut p2)
            .await
            .unwrap();
        assert_eq!(end, RepoBackfill::Done);
        let asked = f2.asked.lock().unwrap().clone();
        assert!(asked[0].contains("page=2"), "{asked:?}");
        assert!(asked[0].contains(&urlencoding::encode(&start.created()).into_owned()));
        // Then the weeks after it, up to now.
        assert!(asked.len() >= 2);
    }

    /// The backfill took an installation's whole hourly budget and every
    /// live job failed for an hour. Background reads yield at half.
    #[tokio::test]
    async fn a_pass_yields_when_the_budget_is_down_to_half() {
        use crate::clients::github::Budget;
        let now = t("2026-11-05T12:00:00Z");
        let start = Cursor::first(now - Duration::days(30), now).unwrap();
        let f = Fake::default();
        *f.budget.lock().unwrap() = Some(Budget {
            remaining: 2_400,
            limit: 5_000,
            reset: 1_000 + 1_800,
        });
        let mut r = reads(&f);
        let mut progressed = false;
        let end = backfill_repo(&mut r, Some(start.clone()), now, &|| false, &mut progressed)
            .await
            .unwrap();
        // Not a request spent: wait for the reset.
        assert_eq!(end, RepoBackfill::Headroom(1_800));
        assert!(f.asked.lock().unwrap().is_empty());
        assert!(!progressed);
        // With headroom it reads.
        *f.budget.lock().unwrap() = Some(Budget {
            remaining: 2_500,
            limit: 5_000,
            reset: 2_800,
        });
        let end = backfill_repo(&mut r, Some(start), now, &|| false, &mut progressed)
            .await
            .unwrap();
        assert_eq!(end, RepoBackfill::Done);
        assert!(!f.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_pass_stops_at_its_most_requests() {
        let now = t("2026-11-05T12:00:00Z");
        let start = Cursor::first(now - Duration::days(30), now).unwrap();
        let f = Fake {
            extra_requests: MAX_PASS_REQUESTS - 2,
            ..Default::default()
        };
        let mut r = reads(&f);
        let mut progressed = false;
        let end = backfill_repo(&mut r, Some(start.clone()), now, &|| false, &mut progressed)
            .await
            .unwrap();
        assert_eq!(end, RepoBackfill::Capped);
        // Two pages read, then it stops with the next one saved.
        assert_eq!(f.asked.lock().unwrap().len(), 2);
        let next = start.next(now).and_then(|c| c.next(now)).unwrap();
        assert_eq!(f.saved.lock().unwrap().last(), Some(&next));
    }

    #[tokio::test]
    async fn out_of_time_keeps_the_cursor_for_the_next_pass() {
        let now = t("2026-11-05T12:00:00Z");
        let start = Cursor::first(now - Duration::days(30), now).unwrap();
        let f = Fake::default();
        let mut r = reads(&f);
        let calls = AtomicUsize::new(0);
        // Time runs out after the first range.
        let out = || calls.fetch_add(1, Ordering::SeqCst) >= 1;
        let mut progressed = false;
        let end = backfill_repo(&mut r, Some(start.clone()), now, &out, &mut progressed)
            .await
            .unwrap();
        assert_eq!(end, RepoBackfill::OutOfTime);
        assert_eq!(
            f.saved.lock().unwrap().last(),
            start.next(now).as_ref(),
            "the next week is where the next pass starts"
        );
    }
}
