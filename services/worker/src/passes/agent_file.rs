//! The agent file sweep: writes a repository's agent file again by itself
//! when it went stale (`common::agent_file` has the rules).
//!
//! It rides on the review-reminder tick (every 20 minutes). Per team:
//!
//! - the team's agent files are read, only for repositories turned on;
//! - a push may have removed a whole top-level folder: those are checked
//!   against the default branch and, when gone, make the file stale;
//! - due files ([`common::agent_file::due`]) are taken pushes first, then
//!   the oldest; a file only old ([`Due::Age`]) is regenerated only if its
//!   default branch has commits since it was written;
//! - a file someone saved by hand is marked out of date, never replaced;
//! - a team over its monthly token limit is skipped;
//! - each regeneration takes the repository's claim for 24 hours
//!   ([`common::agent_file::sweep_claim_sk`]) before it is queued, so two
//!   workers, or two ticks, never queue the same repository twice; at most
//!   [`PER_TEAM_PER_TICK`] per team per sweep.
//!
//! The queued message ([`regenerate`]) takes a short lease on the
//! repository while it runs, checks the record again (written meanwhile,
//! edited meanwhile, the budget), writes the agent file only (never the
//! voice, which people edit on the Voice page) and the team's overview.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Utc};
use common::agent_file::{self as af, Due, Reason, Record, PER_TEAM_PER_TICK};
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{OnboardMessage, OnboardRepo, TokenUsage, WorkerMessage};
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// GitHub reads (age and folder checks) per team per sweep.
const MAX_GITHUB_CHECKS: usize = 6;
/// How long one regeneration's lease lasts.
const RUN_LEASE_SECS: u64 = 15 * 60;

/// What the sweep needs from storage, GitHub and the queue, so its
/// decisions can be tested without them.
#[async_trait::async_trait]
pub trait Env: Send + Sync {
    async fn teams(&self) -> Vec<String>;
    async fn records(&self, team: &str) -> Vec<Record>;
    /// Repositories turned on for the team, with their default branch
    /// (empty when not recorded).
    async fn enabled(&self, team: &str) -> HashMap<String, String>;
    /// Why the team is over its token limit, if it is.
    async fn over_budget(&self, team: &str) -> Option<String>;
    async fn installation(&self, team: &str) -> Option<u64>;
    /// Takes a claim; true only when this caller won it.
    async fn claim(&self, team: &str, sk: &str, ttl_secs: u64) -> bool;
    async fn release(&self, team: &str, sk: &str);
    /// When the newest commit on the default branch was made (None: unknown).
    async fn newest_commit(&self, iid: u64, repo: &str, branch: &str) -> Option<DateTime<Utc>>;
    /// The top-level folders on the default branch now (None: unknown).
    async fn top_dirs(&self, iid: u64, repo: &str, branch: &str) -> Option<Vec<String>>;
    async fn note_age_checked(&self, team: &str, repo: &str);
    async fn settle_dirs(&self, team: &str, repo: &str, gone: &[String], at: DateTime<Utc>);
    async fn mark_out_of_date(&self, team: &str, repo: &str, reasons: &[String]);
    async fn enqueue(&self, msg: &OnboardMessage) -> bool;
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Report {
    pub teams: usize,
    pub queued: usize,
    pub marked_out_of_date: usize,
    pub skipped_budget: usize,
    pub held: usize,
}

/// Why an agent file is due, in the words the dashboard shows.
fn reasons_for(r: &Record, due: Due) -> Vec<String> {
    match due {
        Due::Push => r.stale_reasons.clone(),
        Due::Age => vec![match r.updated_at {
            Some(t) => format!("commits since {}", t.format("%Y-%m-%d")),
            None => "commits since it was written".to_string(),
        }],
    }
}

/// One sweep over every team.
pub async fn sweep_with(env: &dyn Env, now: DateTime<Utc>) -> Report {
    let mut report = Report::default();
    for team in env.teams().await {
        report.teams += 1;
        sweep_team(env, &team, now, &mut report).await;
    }
    report
}

async fn sweep_team(env: &dyn Env, team: &str, now: DateTime<Utc>, report: &mut Report) {
    let enabled = env.enabled(team).await;
    if enabled.is_empty() {
        return;
    }
    let mut records: Vec<Record> = env
        .records(team)
        .await
        .into_iter()
        .filter(|r| enabled.contains_key(&r.repo))
        .collect();
    let mut iid: Option<Option<u64>> = None;
    let mut checks = 0usize;

    // Folders a push removed files from: gone ones make the file stale.
    for r in records.iter_mut().filter(|r| !r.dirs_to_check.is_empty()) {
        if checks >= MAX_GITHUB_CHECKS {
            break;
        }
        let Some(id) = *iid.get_or_insert(env.installation(team).await) else {
            return;
        };
        checks += 1;
        let Some(now_dirs) = env.top_dirs(id, &r.repo, &enabled[&r.repo]).await else {
            continue;
        };
        let gone: Vec<String> = r
            .dirs_to_check
            .iter()
            .filter(|d| !now_dirs.contains(d))
            .cloned()
            .collect();
        let at = r.dirs_pushed_at.unwrap_or(now);
        env.settle_dirs(team, &r.repo, &gone, at).await;
        if !gone.is_empty() {
            let labels = gone.iter().map(|d| af::removed_dir_label(d)).collect();
            af::apply_push(
                r,
                &af::Changes {
                    labels,
                    ..Default::default()
                },
                at,
            );
        }
        r.dirs_to_check.clear();
    }

    let due: Vec<(Record, Due)> = records
        .into_iter()
        .filter_map(|r| af::due(&r, now).map(|d| (r, d)))
        .collect();
    if due.is_empty() {
        return;
    }
    let mut budget_checked = false;
    let mut queued = 0usize;
    for (r, d) in af::pick(due, usize::MAX) {
        if queued >= PER_TEAM_PER_TICK {
            break;
        }
        if r.edited && r.out_of_date {
            // Already waiting on a person: nothing to check or mark.
            continue;
        }
        let branch = enabled[&r.repo].clone();
        if d == Due::Age {
            // Old: only worth writing again if the branch moved since.
            if checks >= MAX_GITHUB_CHECKS {
                continue;
            }
            let Some(id) = *iid.get_or_insert(env.installation(team).await) else {
                return;
            };
            checks += 1;
            let Some(newest) = env.newest_commit(id, &r.repo, &branch).await else {
                continue;
            };
            if r.updated_at.is_some_and(|t| newest <= t) {
                env.note_age_checked(team, &r.repo).await;
                continue;
            }
        }
        let reasons = reasons_for(&r, d);
        if r.edited {
            // Never replace a person's text: say it is out of date.
            env.mark_out_of_date(team, &r.repo, &reasons).await;
            report.marked_out_of_date += 1;
            continue;
        }
        if !budget_checked {
            budget_checked = true;
            if let Some(why) = env.over_budget(team).await {
                info!(team_id = team, why = %why, "Agent file sweep: team over its token limit");
                report.skipped_budget += 1;
                return;
            }
        }
        let Some(id) = *iid.get_or_insert(env.installation(team).await) else {
            return;
        };
        let claim = af::sweep_claim_sk(&r.repo);
        if !env
            .claim(team, &claim, af::MIN_HOURS_BETWEEN as u64 * 3600)
            .await
        {
            report.held += 1;
            continue;
        }
        let Some((owner, name)) = r.repo.split_once('/') else {
            continue;
        };
        let msg = OnboardMessage {
            team_id: team.to_string(),
            installation_id: id,
            repos: vec![OnboardRepo {
                owner: owner.to_string(),
                name: name.to_string(),
                default_branch: branch,
            }],
            reason: d.reason().as_str().to_string(),
            changed: reasons,
            queued_at: now.to_rfc3339(),
        };
        if env.enqueue(&msg).await {
            queued += 1;
            report.queued += 1;
            info!(team_id = team, repo = %r.repo, reason = d.reason().as_str(), "Agent file regeneration queued");
        } else {
            env.release(team, &claim).await;
        }
    }
}

/// What a queued regeneration does once it holds its lease. Pure.
#[derive(Debug, PartialEq, Eq)]
pub enum RunDecision {
    Generate,
    /// Saved by hand meanwhile: marked out of date instead.
    OutOfDate,
    Skip(&'static str),
}

pub fn run_decision(
    record: Option<&Record>,
    queued_at: Option<DateTime<Utc>>,
    over_budget: bool,
) -> RunDecision {
    let Some(r) = record.filter(|r| r.has_content) else {
        return RunDecision::Skip("no agent file to refresh");
    };
    if r.edited {
        return RunDecision::OutOfDate;
    }
    if let (Some(q), Some(u)) = (queued_at, r.updated_at) {
        if u > q {
            return RunDecision::Skip("written since it was queued");
        }
    }
    if over_budget {
        return RunDecision::Skip("the team is over its token limit");
    }
    RunDecision::Generate
}

pub(crate) async fn load_record(state: &WorkerState, team_id: &str, repo: &str) -> Option<Record> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(af::sk(repo)))
        .send()
        .await
        .ok()?;
    out.item().and_then(Record::from_item)
}

/// A regeneration the sweep queued: the agent file only.
pub async fn regenerate(
    state: &WorkerState,
    msg: OnboardMessage,
    reason: Reason,
    usage: &mut TokenUsage,
) -> Result<(), Error> {
    let table = &state.config.settings_table_name;
    let queued_at = DateTime::parse_from_rfc3339(&msg.queued_at)
        .ok()
        .map(|t| t.with_timezone(&Utc));
    let mut wrote = false;
    for repo in &msg.repos {
        let full = format!("{}/{}", repo.owner, repo.name);
        let lease = af::run_claim_sk(&full);
        if common::claim::claim(&state.dynamo, table, &msg.team_id, &lease, RUN_LEASE_SECS).await
            != common::claim::Claim::Won
        {
            info!(repo = %full, "Agent file regeneration already running");
            continue;
        }
        let record = load_record(state, &msg.team_id, &full).await;
        let over = super::check_budget_exceeded(state, &msg.team_id)
            .await
            .is_some();
        let result =
            match run_decision(record.as_ref(), queued_at, over) {
                RunDecision::Skip(why) => {
                    info!(repo = %full, why, "Agent file regeneration skipped");
                    if over {
                        // Let a later sweep try again once the limit allows.
                        common::claim::release(
                            &state.dynamo,
                            table,
                            &msg.team_id,
                            &af::sweep_claim_sk(&full),
                        )
                        .await;
                    }
                    Ok(())
                }
                RunDecision::OutOfDate => {
                    af::mark_out_of_date(&state.dynamo, table, &msg.team_id, &full, &msg.changed)
                        .await
                        .map_err(Error::from)
                }
                RunDecision::Generate => regenerate_one(state, &msg, repo, reason, usage)
                    .await
                    .map(|_| {
                        wrote = true;
                    }),
            };
        common::claim::release(&state.dynamo, table, &msg.team_id, &lease).await;
        if let Err(e) = result {
            warn!(repo = %full, error = %e, "Agent file regeneration failed");
        }
    }
    if wrote {
        let provider = ModelProvider::load_for_team(&state.dynamo, table, &msg.team_id).await?;
        super::onboard::refresh_global(state, &msg.team_id, &provider, usage).await;
    }
    Ok(())
}

async fn regenerate_one(
    state: &WorkerState,
    msg: &OnboardMessage,
    repo: &OnboardRepo,
    reason: Reason,
    usage: &mut TokenUsage,
) -> Result<(), Error> {
    let table = &state.config.settings_table_name;
    let provider = ModelProvider::load_for_team(&state.dynamo, table, &msg.team_id).await?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let mut repo = repo.clone();
    if repo.default_branch.is_empty() {
        repo.default_branch = github.get_default_branch(&repo.owner, &repo.name).await?;
    }
    let full = format!("{}/{}", repo.owner, repo.name);
    let global =
        super::onboard::load_instructions(state, &msg.team_id, "INSTRUCTIONS#GLOBAL").await;
    let own = super::onboard::load_instructions(
        state,
        &msg.team_id,
        &format!("INSTRUCTIONS#REPO#{full}"),
    )
    .await;
    let started = Utc::now();
    let (content, top_dirs) =
        super::onboard::generate_agents_md(&github, &repo, &global, &own, &provider, usage).await?;
    super::onboard::store_agents_md(
        state,
        &msg.team_id,
        &full,
        &content,
        reason,
        &msg.changed,
        &top_dirs,
        started,
    )
    .await?;
    info!(repo = %full, reason = reason.as_str(), changed = ?msg.changed, "Agent file written again");
    Ok(())
}

// ─── The live environment ──────────────────────────────────────────────

struct Live<'a> {
    state: &'a WorkerState,
}

impl Live<'_> {
    fn github(&self, iid: u64) -> Option<GitHubClient> {
        GitHubClient::new(
            &self.state.secrets.github_app_id,
            &self.state.secrets.github_private_key,
            iid,
            &self.state.http,
        )
        .ok()
    }

    async fn branch(&self, gh: &GitHubClient, repo: &str, branch: &str) -> Option<String> {
        if !branch.is_empty() {
            return Some(branch.to_string());
        }
        let (owner, name) = repo.split_once('/')?;
        gh.get_default_branch(owner, name).await.ok()
    }
}

#[async_trait::async_trait]
impl Env for Live<'_> {
    async fn teams(&self) -> Vec<String> {
        let s = self.state;
        match common::settings_index::members(
            &s.dynamo,
            &s.config.settings_table_name,
            &af::index_spec(),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "Agent file sweep: could not read the team index");
                vec![]
            }
        }
    }

    async fn records(&self, team: &str) -> Vec<Record> {
        let s = self.state;
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let page = s
                .dynamo
                .query()
                .table_name(&s.config.settings_table_name)
                .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
                .expression_attribute_values(":p", AttributeValue::S(team.to_string()))
                .expression_attribute_values(":pfx", AttributeValue::S(af::SK_PREFIX.to_string()))
                .set_exclusive_start_key(start.take())
                .send()
                .await;
            let Ok(page) = page else {
                break;
            };
            out.extend(page.items().iter().filter_map(Record::from_item));
            match page.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        out
    }

    async fn enabled(&self, team: &str) -> HashMap<String, String> {
        let s = self.state;
        let mut out = HashMap::new();
        let mut start = None;
        loop {
            let page = s
                .dynamo
                .query()
                .table_name(&s.config.repos_table_name)
                .key_condition_expression("pk = :p AND begins_with(sk, :pfx)")
                .expression_attribute_values(":p", AttributeValue::S(team.to_string()))
                .expression_attribute_values(":pfx", AttributeValue::S("REPO#".to_string()))
                .set_exclusive_start_key(start.take())
                .send()
                .await;
            let Ok(page) = page else {
                break;
            };
            for item in page.items() {
                let on = item.get("enabled").and_then(|v| v.as_bool().ok()) == Some(&true);
                let repo = item
                    .get("sk")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|sk| sk.strip_prefix("REPO#"));
                if let (true, Some(repo)) = (on, repo) {
                    let branch = item
                        .get("default_branch")
                        .and_then(|v| v.as_s().ok())
                        .cloned()
                        .unwrap_or_default();
                    out.insert(repo.to_string(), branch);
                }
            }
            match page.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        out
    }

    async fn over_budget(&self, team: &str) -> Option<String> {
        super::check_budget_exceeded(self.state, team).await
    }

    async fn installation(&self, team: &str) -> Option<u64> {
        super::review_reminder::team_installation_id(self.state, team).await
    }

    async fn claim(&self, team: &str, sk: &str, ttl_secs: u64) -> bool {
        let s = self.state;
        common::claim::claim(&s.dynamo, &s.config.settings_table_name, team, sk, ttl_secs).await
            == common::claim::Claim::Won
    }

    async fn release(&self, team: &str, sk: &str) {
        let s = self.state;
        common::claim::release(&s.dynamo, &s.config.settings_table_name, team, sk).await;
    }

    async fn newest_commit(&self, iid: u64, repo: &str, branch: &str) -> Option<DateTime<Utc>> {
        let gh = self.github(iid)?;
        let branch = self.branch(&gh, repo, branch).await?;
        let (owner, name) = repo.split_once('/')?;
        let commits = gh.list_commits(owner, name, &branch, 1).await.ok()?;
        commits
            .get(0)?
            .pointer("/commit/committer/date")?
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc))
    }

    async fn top_dirs(&self, iid: u64, repo: &str, branch: &str) -> Option<Vec<String>> {
        let gh = self.github(iid)?;
        let branch = self.branch(&gh, repo, branch).await?;
        let (owner, name) = repo.split_once('/')?;
        let entries = gh.list_directory(owner, name, "", &branch).await.ok()?;
        Some(
            entries
                .into_iter()
                .filter(|e| e.entry_type == "dir")
                .map(|e| e.name)
                .collect(),
        )
    }

    async fn note_age_checked(&self, team: &str, repo: &str) {
        let s = self.state;
        af::note_age_checked(&s.dynamo, &s.config.settings_table_name, team, repo).await;
    }

    async fn settle_dirs(&self, team: &str, repo: &str, gone: &[String], at: DateTime<Utc>) {
        let s = self.state;
        if let Err(e) = af::settle_dirs(
            &s.dynamo,
            &s.config.settings_table_name,
            team,
            repo,
            gone,
            at,
        )
        .await
        {
            warn!(repo, error = %e, "Agent file sweep: could not record the folder check");
        }
    }

    async fn mark_out_of_date(&self, team: &str, repo: &str, reasons: &[String]) {
        let s = self.state;
        if let Err(e) = af::mark_out_of_date(
            &s.dynamo,
            &s.config.settings_table_name,
            team,
            repo,
            reasons,
        )
        .await
        {
            warn!(repo, error = %e, "Agent file sweep: could not mark it out of date");
        }
    }

    async fn enqueue(&self, msg: &OnboardMessage) -> bool {
        let s = self.state;
        if s.config.ticket_queue_url.is_empty() {
            return false;
        }
        let body = match serde_json::to_string(&WorkerMessage::Onboard(OnboardMessage {
            team_id: msg.team_id.clone(),
            installation_id: msg.installation_id,
            repos: msg.repos.clone(),
            reason: msg.reason.clone(),
            changed: msg.changed.clone(),
            queued_at: msg.queued_at.clone(),
        })) {
            Ok(b) => b,
            Err(_) => return false,
        };
        match s
            .sqs
            .send_message()
            .queue_url(&s.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                warn!(error = %e, "Agent file sweep: could not queue a regeneration");
                false
            }
        }
    }
}

/// The sweep on the review-reminder tick.
pub async fn sweep(state: &WorkerState) -> Report {
    sweep_with(&Live { state }, Utc::now()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn rec(repo: &str, updated: &str) -> Record {
        Record {
            repo: repo.into(),
            has_content: true,
            updated_at: Some(t(updated)),
            ..Default::default()
        }
    }

    fn stale(repo: &str, updated: &str, pushed: &str, why: &str) -> Record {
        Record {
            stale_reasons: vec![why.into()],
            last_push_at: Some(t(pushed)),
            ..rec(repo, updated)
        }
    }

    /// Storage shared by every worker in a test: claims are atomic.
    #[derive(Default)]
    struct Shared {
        claims: Mutex<HashSet<(String, String)>>,
    }

    struct Fake<'a> {
        shared: &'a Shared,
        records: Vec<Record>,
        enabled: Vec<&'static str>,
        over_budget: bool,
        newest: Option<DateTime<Utc>>,
        dirs_now: Vec<String>,
        queued: Mutex<Vec<OnboardMessage>>,
        out_of_date: Mutex<Vec<String>>,
        age_checked: Mutex<Vec<String>>,
        settled: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl<'a> Fake<'a> {
        fn new(shared: &'a Shared, records: Vec<Record>) -> Self {
            Fake {
                shared,
                records,
                enabled: vec!["acme/web", "acme/api", "acme/app", "acme/cli", "acme/docs"],
                over_budget: false,
                newest: Some(t("2026-11-01T00:00:00Z")),
                dirs_now: vec!["src".into()],
                queued: Mutex::default(),
                out_of_date: Mutex::default(),
                age_checked: Mutex::default(),
                settled: Mutex::default(),
            }
        }
        fn queued_repos(&self) -> Vec<String> {
            self.queued
                .lock()
                .unwrap()
                .iter()
                .map(|m| format!("{}/{}", m.repos[0].owner, m.repos[0].name))
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl Env for Fake<'_> {
        async fn teams(&self) -> Vec<String> {
            vec!["TEAM#1".into()]
        }
        async fn records(&self, _: &str) -> Vec<Record> {
            self.records.clone()
        }
        async fn enabled(&self, _: &str) -> HashMap<String, String> {
            self.enabled
                .iter()
                .map(|r| (r.to_string(), "main".to_string()))
                .collect()
        }
        async fn over_budget(&self, _: &str) -> Option<String> {
            self.over_budget
                .then(|| "Monthly token limit exceeded".to_string())
        }
        async fn installation(&self, _: &str) -> Option<u64> {
            Some(7)
        }
        async fn claim(&self, team: &str, sk: &str, _: u64) -> bool {
            self.shared
                .claims
                .lock()
                .unwrap()
                .insert((team.to_string(), sk.to_string()))
        }
        async fn release(&self, team: &str, sk: &str) {
            self.shared
                .claims
                .lock()
                .unwrap()
                .remove(&(team.to_string(), sk.to_string()));
        }
        async fn newest_commit(&self, _: u64, _: &str, _: &str) -> Option<DateTime<Utc>> {
            self.newest
        }
        async fn top_dirs(&self, _: u64, _: &str, _: &str) -> Option<Vec<String>> {
            Some(self.dirs_now.clone())
        }
        async fn note_age_checked(&self, _: &str, repo: &str) {
            self.age_checked.lock().unwrap().push(repo.into());
        }
        async fn settle_dirs(&self, _: &str, repo: &str, gone: &[String], _: DateTime<Utc>) {
            self.settled
                .lock()
                .unwrap()
                .push((repo.into(), gone.to_vec()));
        }
        async fn mark_out_of_date(&self, _: &str, repo: &str, _: &[String]) {
            self.out_of_date.lock().unwrap().push(repo.into());
        }
        async fn enqueue(&self, msg: &OnboardMessage) -> bool {
            self.queued.lock().unwrap().push(OnboardMessage {
                team_id: msg.team_id.clone(),
                installation_id: msg.installation_id,
                repos: msg.repos.clone(),
                reason: msg.reason.clone(),
                changed: msg.changed.clone(),
                queued_at: msg.queued_at.clone(),
            });
            true
        }
    }

    const NOW: &str = "2026-11-10T12:00:00Z";

    #[tokio::test]
    async fn a_settled_push_queues_one_regeneration_with_its_reasons() {
        let shared = Shared::default();
        let env = Fake::new(
            &shared,
            vec![stale(
                "acme/web",
                "2026-11-01T00:00:00Z",
                "2026-11-10T11:00:00Z",
                "package.json",
            )],
        );
        let r = sweep_with(&env, t(NOW)).await;
        assert_eq!(r.queued, 1);
        let q = env.queued.lock().unwrap();
        assert_eq!(q[0].reason, "push");
        assert_eq!(q[0].changed, vec!["package.json".to_string()]);
        assert_eq!(q[0].repos[0].default_branch, "main");
    }

    #[tokio::test]
    async fn a_push_still_settling_waits() {
        let shared = Shared::default();
        let env = Fake::new(
            &shared,
            vec![stale(
                "acme/web",
                "2026-11-01T00:00:00Z",
                "2026-11-10T11:45:00Z",
                "CI",
            )],
        );
        assert_eq!(sweep_with(&env, t(NOW)).await.queued, 0);
    }

    #[tokio::test]
    async fn the_claim_prevents_a_double_run() {
        // Two workers sweep the same team at once (a duplicate tick).
        let shared = Shared::default();
        let records = vec![stale(
            "acme/web",
            "2026-11-01T00:00:00Z",
            "2026-11-10T11:00:00Z",
            "go.mod",
        )];
        let a = Fake::new(&shared, records.clone());
        let b = Fake::new(&shared, records);
        let (ra, rb) = tokio::join!(sweep_with(&a, t(NOW)), sweep_with(&b, t(NOW)));
        assert_eq!(ra.queued + rb.queued, 1);
        assert_eq!(ra.held + rb.held, 1);
        // The next tick, the claim still held for the day: nothing again.
        let c = Fake::new(
            &shared,
            vec![stale(
                "acme/web",
                "2026-11-01T00:00:00Z",
                "2026-11-10T11:00:00Z",
                "go.mod",
            )],
        );
        assert_eq!(sweep_with(&c, t("2026-11-10T12:20:00Z")).await.queued, 0);
    }

    #[tokio::test]
    async fn the_backstop_regenerates_an_old_file_whose_branch_moved() {
        let shared = Shared::default();
        let env = Fake::new(&shared, vec![rec("acme/web", "2026-04-02T00:00:00Z")]);
        let r = sweep_with(&env, t(NOW)).await;
        assert_eq!(r.queued, 1);
        let q = env.queued.lock().unwrap();
        assert_eq!(q[0].reason, "age");
        assert_eq!(q[0].changed, vec!["commits since 2026-04-02".to_string()]);
    }

    #[tokio::test]
    async fn the_backstop_leaves_an_old_file_with_no_new_commits() {
        let shared = Shared::default();
        let mut env = Fake::new(&shared, vec![rec("acme/web", "2026-04-02T00:00:00Z")]);
        env.newest = Some(t("2026-04-01T00:00:00Z"));
        assert_eq!(sweep_with(&env, t(NOW)).await.queued, 0);
        assert_eq!(
            *env.age_checked.lock().unwrap(),
            vec!["acme/web".to_string()]
        );
    }

    #[tokio::test]
    async fn a_few_repositories_per_team_per_tick_pushes_first() {
        let shared = Shared::default();
        let env = Fake::new(
            &shared,
            vec![
                rec("acme/api", "2026-01-01T00:00:00Z"),
                rec("acme/app", "2026-02-01T00:00:00Z"),
                rec("acme/cli", "2026-03-01T00:00:00Z"),
                stale(
                    "acme/docs",
                    "2026-11-01T00:00:00Z",
                    "2026-11-10T10:00:00Z",
                    "README.md",
                ),
                rec("acme/web", "2026-04-01T00:00:00Z"),
            ],
        );
        assert_eq!(sweep_with(&env, t(NOW)).await.queued, PER_TEAM_PER_TICK);
        assert_eq!(
            env.queued_repos(),
            vec!["acme/docs", "acme/api", "acme/app"]
        );
    }

    #[tokio::test]
    async fn hand_edits_are_never_replaced() {
        let shared = Shared::default();
        let mut edited = stale(
            "acme/web",
            "2026-11-01T00:00:00Z",
            "2026-11-10T11:00:00Z",
            "package.json",
        );
        edited.edited = true;
        let mut old_edited = rec("acme/api", "2026-03-01T00:00:00Z");
        old_edited.edited = true;
        let env = Fake::new(&shared, vec![edited.clone(), old_edited]);
        let r = sweep_with(&env, t(NOW)).await;
        assert_eq!(r.queued, 0);
        assert_eq!(r.marked_out_of_date, 2);
        // Already marked: not marked again.
        edited.out_of_date = true;
        let again = Fake::new(&shared, vec![edited]);
        assert_eq!(sweep_with(&again, t(NOW)).await.marked_out_of_date, 0);
        // And a regeneration queued before the edit doesn't write over it.
        let mut r = rec("acme/web", "2026-11-01T00:00:00Z");
        r.edited = true;
        assert_eq!(
            run_decision(Some(&r), Some(t("2026-11-02T00:00:00Z")), false),
            RunDecision::OutOfDate
        );
    }

    #[tokio::test]
    async fn over_budget_teams_are_skipped() {
        let shared = Shared::default();
        let mut env = Fake::new(
            &shared,
            vec![stale(
                "acme/web",
                "2026-11-01T00:00:00Z",
                "2026-11-10T11:00:00Z",
                "Cargo.toml",
            )],
        );
        env.over_budget = true;
        let r = sweep_with(&env, t(NOW)).await;
        assert_eq!((r.queued, r.skipped_budget), (0, 1));
        // No claim was spent: the next tick under budget queues it.
        env.over_budget = false;
        assert_eq!(sweep_with(&env, t(NOW)).await.queued, 1);
        assert_eq!(
            run_decision(Some(&rec("acme/web", "2026-11-01T00:00:00Z")), None, true),
            RunDecision::Skip("the team is over its token limit")
        );
    }

    #[tokio::test]
    async fn repositories_turned_off_are_left_alone() {
        let shared = Shared::default();
        let mut env = Fake::new(
            &shared,
            vec![stale(
                "acme/old",
                "2026-01-01T00:00:00Z",
                "2026-11-10T10:00:00Z",
                "CI",
            )],
        );
        env.enabled = vec!["acme/web"];
        assert_eq!(sweep_with(&env, t(NOW)).await.queued, 0);
    }

    #[tokio::test]
    async fn a_removed_top_level_folder_makes_the_file_stale() {
        let shared = Shared::default();
        let mut r = rec("acme/web", "2026-11-01T00:00:00Z");
        r.dirs_to_check = vec!["docs".into(), "src".into()];
        r.dirs_pushed_at = Some(t("2026-11-10T11:00:00Z"));
        let env = Fake::new(&shared, vec![r]);
        let rep = sweep_with(&env, t(NOW)).await;
        assert_eq!(
            *env.settled.lock().unwrap(),
            vec![("acme/web".to_string(), vec!["docs".to_string()])]
        );
        assert_eq!(rep.queued, 1);
        assert_eq!(
            env.queued.lock().unwrap()[0].changed,
            vec!["removing the docs folder".to_string()]
        );
    }

    #[test]
    fn a_regeneration_written_meanwhile_is_skipped() {
        let r = rec("acme/web", "2026-11-10T12:05:00Z");
        assert_eq!(
            run_decision(Some(&r), Some(t(NOW)), false),
            RunDecision::Skip("written since it was queued")
        );
        assert_eq!(
            run_decision(Some(&r), Some(t("2026-11-10T12:10:00Z")), false),
            RunDecision::Generate
        );
        assert_eq!(
            run_decision(None, None, false),
            RunDecision::Skip("no agent file to refresh")
        );
    }
}
