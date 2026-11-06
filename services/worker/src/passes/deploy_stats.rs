//! Deploys CoderHelm started (`common::deploy_stats`): a completed workflow
//! run, from its webhook or read back from GitHub's run history, recorded
//! when CoderHelm's app triggered it and it deployed somewhere.

use std::collections::HashMap;
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::deploy_stats::{self as ds, Cause, JobDef};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

type GhError = Box<dyn std::error::Error + Send + Sync>;

fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// A workflow file's jobs are read again after this long.
const WORKFLOW_CACHE_SECS: i64 = 3600;
/// A backfill pass stops here and queues the next one, well inside the
/// worker's 15 minutes.
const PASS_BUDGET_SECS: u64 = 600;
/// Run history is read a week at a time: GitHub returns at most 1,000 runs
/// for one filtered query.
const CHUNK_DAYS: i64 = 7;

/// GitHub refused because the app has no access to Actions here.
fn is_no_access(e: &(dyn std::error::Error + Send + Sync)) -> bool {
    let m = e.to_string();
    m.contains("(403 Forbidden)") && m.contains("not accessible by integration")
}

/// The `created` filters that cover `since` to `now`, a week each.
pub fn created_ranges(since: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = since;
    while from < now {
        let to = (from + Duration::days(CHUNK_DAYS)).min(now);
        out.push(format!(
            "{}..{}",
            from.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            to.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ));
        from = to;
    }
    out
}

/// What the worker knows about one repository while it reads its deploys.
struct RepoReads<'a> {
    github: &'a GitHubClient,
    owner: String,
    name: String,
    bot: String,
    default_branch: String,
    setting: HashMap<String, String>,
    /// Workflow jobs by path, read once per pass.
    workflows: HashMap<String, Option<Vec<JobDef>>>,
}

impl RepoReads<'_> {
    fn full(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }

    async fn jobs_of(
        &mut self,
        state: &WorkerState,
        team_id: &str,
        path: &str,
    ) -> Option<Vec<JobDef>> {
        if path.is_empty() {
            return None;
        }
        if let Some(j) = self.workflows.get(path) {
            return j.clone();
        }
        let j = workflow_jobs(state, team_id, self.github, &self.owner, &self.name, path).await;
        self.workflows.insert(path.to_string(), j.clone());
        j
    }

    /// Whether CoderHelm caused this run, and how.
    async fn cause(&self, run: &Value) -> Option<Cause> {
        let branch = run["head_branch"].as_str().unwrap_or("");
        let event = run["event"].as_str().unwrap_or("");
        let ours = if ds::started_by_coderhelm(run, &self.bot) {
            true
        } else {
            // A merge queue's push of a PR CoderHelm merged.
            let (_, kind) = ds::trigger(run);
            kind == "Bot"
                && event == "push"
                && branch == self.default_branch
                && self.merged_by_us(run).await
        };
        if !ours {
            return None;
        }
        let is_tag = event == "push"
            && branch != self.default_branch
            && self
                .github
                .get_repo_path(
                    &self.owner,
                    &self.name,
                    &format!("git/ref/tags/{}", urlencoding::encode(branch)),
                )
                .await
                .is_ok();
        Some(ds::cause_of(run, &self.default_branch, is_tag))
    }

    async fn merged_by_us(&self, run: &Value) -> bool {
        let sha = run["head_sha"].as_str().unwrap_or("");
        if sha.is_empty() {
            return false;
        }
        match self
            .github
            .get_repo_path(&self.owner, &self.name, &format!("commits/{sha}/pulls"))
            .await
        {
            Ok(v) => ds::merged_by_coderhelm(
                v.as_array().map(|a| a.as_slice()).unwrap_or(&[]),
                &self.bot,
            ),
            Err(_) => false,
        }
    }

    /// Records a completed run when CoderHelm started it and it deployed.
    /// Returns how many deploys it wrote.
    async fn record(&mut self, state: &WorkerState, team_id: &str, run: &Value) -> usize {
        if run["status"].as_str().is_some_and(|s| s != "completed") {
            return 0;
        }
        let Some(cause) = self.cause(run).await else {
            return 0;
        };
        let path = run["path"].as_str().unwrap_or("").to_string();
        let jobs = self.jobs_of(state, team_id, &path).await;
        let file = path.rsplit('/').next().unwrap_or("").to_ascii_lowercase();
        let maybe = self.setting.contains_key(&file)
            || ds::says_deploy(run["name"].as_str().unwrap_or(""))
            || ds::says_deploy(&file)
            || jobs
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(|j| !j.environment.is_empty() || ds::says_deploy(&j.name));
        if !maybe {
            return 0;
        }
        let run_id = run["id"].as_u64().unwrap_or(0);
        let ran: Option<Vec<Value>> = match self
            .github
            .get_repo_path(
                &self.owner,
                &self.name,
                &format!("actions/runs/{run_id}/jobs?per_page=100"),
            )
            .await
        {
            Ok(v) => v["jobs"].as_array().cloned(),
            Err(e) => {
                warn!(run_id, error = %e, "Deploys: could not read a run's jobs");
                None
            }
        };
        let targets = ds::run_targets(run, jobs.as_deref(), ran.as_deref(), &self.setting);
        let repo = self.full();
        let mut written = 0;
        for t in &targets {
            let item = ds::stat_item(team_id, &repo, run, cause, t);
            match state
                .dynamo
                .put_item()
                .table_name(&state.config.settings_table_name)
                .set_item(Some(item))
                .send()
                .await
            {
                Ok(_) => written += 1,
                Err(e) => warn!(run_id, error = %e, "Deploys: could not record a deploy"),
            }
        }
        written
    }
}

/// A workflow file's jobs, cached in settings for an hour.
async fn workflow_jobs(
    state: &WorkerState,
    team_id: &str,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    path: &str,
) -> Option<Vec<JobDef>> {
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
                return Some(
                    jobs.iter()
                        .map(|j| JobDef {
                            key: j["key"].as_str().unwrap_or("").into(),
                            name: j["name"].as_str().unwrap_or("").into(),
                            environment: j["environment"].as_str().unwrap_or("").into(),
                        })
                        .collect(),
                );
            }
        }
    }
    let text = match github.read_file_live(owner, repo, path, "HEAD").await {
        Ok(t) => t,
        Err(e) => {
            warn!(path, error = %e, "Deploys: could not read a workflow file");
            return None;
        }
    };
    let jobs = ds::parse_workflow(&text)?;
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
        .item("checked_at", AttributeValue::N(now.to_string()))
        .item("ttl", AttributeValue::N((now + 86_400).to_string()))
        .send()
        .await;
    Some(jobs)
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

async fn repo_reads<'a>(
    state: &WorkerState,
    team_id: &str,
    github: &'a GitHubClient,
    owner: &str,
    name: &str,
    bot: &str,
) -> Result<RepoReads<'a>, GhError> {
    let default_branch = github.get_default_branch(owner, name).await?;
    Ok(RepoReads {
        github,
        owner: owner.to_string(),
        name: name.to_string(),
        bot: bot.to_string(),
        default_branch,
        setting: deploy_workflows_setting(state, team_id, &format!("{owner}/{name}")).await,
        workflows: HashMap::new(),
    })
}

/// Records one completed workflow run from its webhook.
pub async fn record_run(
    state: &WorkerState,
    msg: &common::messages::DeployRunMessage,
) -> Result<(), String> {
    let Some(github) = super::plan_chat::github_for(state, &msg.team_id).await else {
        return Err("no GitHub installation for the team".into());
    };
    let bot = github
        .app_bot_login()
        .await
        .map_err(|e| format!("bot login: {e}"))?;
    let mut reads = repo_reads(
        state,
        &msg.team_id,
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &bot,
    )
    .await
    .map_err(|e| format!("read repository: {e}"))?;
    let n = reads.record(state, &msg.team_id, &msg.run).await;
    if n > 0 {
        info!(
            run_id = msg.run["id"].as_u64().unwrap_or(0),
            deploys = n,
            "Deploy recorded"
        );
    }
    Ok(())
}

/// How one repository's backfill ended.
enum RepoBackfill {
    Done,
    NoAccess,
    OutOfTime,
}

async fn backfill_repo(
    state: &WorkerState,
    team_id: &str,
    reads: &mut RepoReads<'_>,
    since: chrono::DateTime<Utc>,
    started: Instant,
) -> Result<RepoBackfill, GhError> {
    let mut written = 0usize;
    for range in created_ranges(since, Utc::now()) {
        for page in 1..=10u32 {
            if started.elapsed().as_secs() > PASS_BUDGET_SECS {
                return Ok(RepoBackfill::OutOfTime);
            }
            let rest = format!(
                "actions/runs?actor={}&status=completed&created={}&per_page=100&page={page}",
                urlencoding::encode(&reads.bot),
                urlencoding::encode(&range)
            );
            let v = match reads
                .github
                .get_repo_path(&reads.owner, &reads.name, &rest)
                .await
            {
                Ok(v) => v,
                Err(e) if is_no_access(&*e) => return Ok(RepoBackfill::NoAccess),
                Err(e) => return Err(e),
            };
            let runs = v["workflow_runs"].as_array().cloned().unwrap_or_default();
            for run in &runs {
                written += reads.record(state, team_id, run).await;
            }
            if runs.len() < 100 {
                break;
            }
        }
    }
    info!(repo = %reads.full(), written, "Deploys backfill: repository read");
    Ok(RepoBackfill::Done)
}

async fn mark(state: &WorkerState, team_id: &str, sk: &str, extra: &[(&str, AttributeValue)]) {
    let mut req = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", av(team_id))
        .item("sk", av(sk))
        .item("at", av(&Utc::now().to_rfc3339()));
    for (k, v) in extra {
        req = req.item(*k, v.clone());
    }
    if let Err(e) = req.send().await {
        warn!(sk, error = %e, "Deploys backfill: could not write a marker");
    }
}

async fn queue_next_pass(state: &WorkerState, team_id: &str) {
    if state.config.ticket_queue_url.is_empty() {
        return;
    }
    let body = json!({"type": "deploy_stats_backfill", "team_id": team_id}).to_string();
    if let Err(e) = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        warn!(error = %e, "Deploys backfill: could not queue the next pass");
    }
}

/// Reads a team's deploys of the last `BACKFILL_DAYS` from GitHub's run
/// history. Safe to repeat: a record is keyed by its run, each repository is
/// marked when read, and a pass that runs out of time queues the next one.
/// The team is marked done only when every repository was read.
pub async fn backfill(state: &WorkerState, team_id: &str) -> Result<(), String> {
    let table = &state.config.settings_table_name;
    let started = Instant::now();
    let done = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", av(team_id))
        .key("sk", av(ds::BACKFILL_MARKER_SK))
        .send()
        .await
        .map_err(|e| format!("read backfill marker: {e}"))?;
    if done.item().is_some() {
        return Ok(());
    }
    let Some(github) = super::plan_chat::github_for(state, team_id).await else {
        return Err("no GitHub installation for the team".into());
    };
    let bot = github
        .app_bot_login()
        .await
        .map_err(|e| format!("bot login: {e}"))?;
    let since = Utc::now() - Duration::days(ds::BACKFILL_DAYS);
    let repos = super::plan::fetch_team_repo_list(state, team_id).await;
    let (mut no_access, mut failed) = (Vec::<String>::new(), 0usize);
    for repo in &repos {
        let Some((owner, name)) = repo.full_name.split_once('/') else {
            continue;
        };
        let repo_sk = format!(
            "{}{}",
            ds::BACKFILL_REPO_PREFIX,
            repo.full_name.to_ascii_lowercase()
        );
        let prior = state
            .dynamo
            .get_item()
            .table_name(table)
            .key("pk", av(team_id))
            .key("sk", av(&repo_sk))
            .send()
            .await
            .ok()
            .and_then(|r| r.item().cloned());
        if let Some(p) = prior {
            if p.get("no_access").and_then(|v| v.as_bool().ok()) == Some(&true) {
                no_access.push(repo.full_name.clone());
            }
            continue;
        }
        let mut reads = match repo_reads(state, team_id, &github, owner, name, &bot).await {
            Ok(r) => r,
            Err(e) if crate::clients::github::is_not_found(&*e) => {
                // Gone or no longer installed: nothing to read.
                mark(
                    state,
                    team_id,
                    &repo_sk,
                    &[("gone", AttributeValue::Bool(true))],
                )
                .await;
                continue;
            }
            Err(e) => {
                warn!(repo = %repo.full_name, error = %e, "Deploys backfill: could not read the repository");
                failed += 1;
                continue;
            }
        };
        match backfill_repo(state, team_id, &mut reads, since, started).await {
            Ok(RepoBackfill::Done) => mark(state, team_id, &repo_sk, &[]).await,
            Ok(RepoBackfill::NoAccess) => {
                no_access.push(repo.full_name.clone());
                mark(
                    state,
                    team_id,
                    &repo_sk,
                    &[("no_access", AttributeValue::Bool(true))],
                )
                .await;
            }
            Ok(RepoBackfill::OutOfTime) => {
                info!(
                    team_id,
                    "Deploys backfill: out of time, continuing in the next pass"
                );
                queue_next_pass(state, team_id).await;
                return Ok(());
            }
            Err(e) => {
                warn!(repo = %repo.full_name, error = %e, "Deploys backfill: could not read run history");
                failed += 1;
            }
        }
    }
    if failed > 0 {
        return Err(format!(
            "{failed} repositories could not be read; the next request continues"
        ));
    }
    let mut extra = vec![
        (
            "since",
            av(&since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        ),
        ("repos", AttributeValue::N(repos.len().to_string())),
    ];
    if !no_access.is_empty() {
        extra.push(("no_access", AttributeValue::Ss(no_access)));
    }
    mark(state, team_id, ds::BACKFILL_MARKER_SK, &extra).await;
    info!(team_id, repos = repos.len(), "Deploys backfill done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backfill_reads_the_whole_window_a_week_at_a_time() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-05T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let since = now - Duration::days(ds::BACKFILL_DAYS);
        let r = created_ranges(since, now);
        assert_eq!(r.len(), 26);
        assert_eq!(r[0], "2026-05-07T12:00:00Z..2026-05-14T12:00:00Z");
        assert!(r.last().unwrap().ends_with("..2026-11-05T12:00:00Z"));
        // Each range starts where the last ended: nothing skipped.
        for w in r.windows(2) {
            assert_eq!(w[0].split("..").nth(1), w[1].split("..").next());
        }
        assert!(created_ranges(now, now).is_empty());
    }

    #[test]
    fn missing_actions_access_is_told_apart_from_other_refusals() {
        let no: GhError = "HTTP status client error (403 Forbidden) for url (u): {\"message\":\"Resource not accessible by integration\"}".into();
        assert!(is_no_access(&*no));
        let limited: GhError =
            "HTTP status client error (403 Forbidden) for url (u): API rate limit exceeded".into();
        assert!(!is_no_access(&*limited));
        let gone: GhError = "HTTP status client error (404 Not Found) for url (u)".into();
        assert!(!is_no_access(&*gone));
    }
}
