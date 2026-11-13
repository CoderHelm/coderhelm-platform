//! Automatic fix PRs for new log findings (EventBridge, then SQS, hourly, and at
//! once when the log analyzer's watch stores a new finding).
//!
//! For every team that turned automatic fixes on, the open findings the
//! sweep hasn't decided yet go through [`common::log_findings::screen`] (new
//! since it was turned on, severity, log groups, quiet hours). Each is
//! diagnosed first ([`common::log_incident`]): what the lines say, what the
//! account and the code say about the resource behind them, and whether it
//! is still happening. Findings with one cause become one incident, and the
//! sweep acts once per incident:
//! - it stopped on its own: one card says so, nothing changes;
//! - the fix is a setting or infrastructure in a repo: one fix run there,
//!   with the diagnosis on its ticket (its PR card carries the incident);
//! - what's left is only a person's (a secret outside the code, a pipeline
//!   CoderHelm can only read): one card says what was found and what is left;
//! - the application code: its repo is worked out ([`common::log_repo`]);
//!   decisive evidence starts the run and saves the repo for the log group as
//!   learned, so it is never asked again; genuinely split evidence asks once
//!   per log group.
//!
//! A finding that doesn't start records why, and the Logs page shows it. Off
//! by default: nothing runs for a team that never turned it on.

use chrono::{DateTime, Utc};
use common::log_findings::{self as lf, AutoFix, Finding, Skip, Start, StartEnv, Starter};
use tracing::{info, warn};

use crate::models::{TicketMessage, TicketSource, WorkerMessage};
use crate::WorkerState;

/// What one sweep did.
#[derive(Debug, Default)]
pub struct Report {
    pub teams: usize,
    pub started: usize,
    pub skipped: usize,
}

/// The worker's side of a start.
struct WorkerEnv<'a> {
    state: &'a WorkerState,
    finding: &'a Finding,
    now: DateTime<Utc>,
}

#[async_trait::async_trait]
impl StartEnv for WorkerEnv<'_> {
    async fn runs(&self, team_id: &str, ticket_id: &str) -> Result<Vec<lf::RunRef>, String> {
        lf::runs_for_ticket(
            &self.state.dynamo,
            &self.state.config.runs_table_name,
            team_id,
            ticket_id,
        )
        .await
    }
    async fn budget_refusal(&self, team_id: &str) -> Option<String> {
        super::check_budget_exceeded(self.state, team_id).await
    }
    async fn claim(&self, team_id: &str, fingerprint: &str) -> bool {
        lf::claim_start(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team_id,
            fingerprint,
        )
        .await
    }
    async fn release(&self, team_id: &str, fingerprint: &str) {
        lf::release_start(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team_id,
            fingerprint,
        )
        .await
    }
    async fn take_daily_slot(&self, team_id: &str, cap: u32) -> bool {
        lf::take_daily_slot(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team_id,
            cap,
            self.now,
        )
        .await
    }
    async fn send(&self, team_id: &str, st: &Start) -> Result<(), String> {
        if self.state.config.ticket_queue_url.is_empty() {
            return Err("no ticket queue".into());
        }
        let installation_id = super::review_reminder::team_installation_id(self.state, team_id)
            .await
            .filter(|id| *id > 0)
            .ok_or("the GitHub app isn't installed")?;
        let (owner, name) = st.repo.split_once('/').ok_or("bad repo")?;
        lf::record_start(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            &self.state.config.aws_insights_table_name,
            team_id,
            self.finding,
            st,
            self.now,
        )
        .await?;
        let msg = WorkerMessage::Ticket(TicketMessage {
            github_wait: Default::default(),
            team_id: team_id.to_string(),
            installation_id,
            source: TicketSource::Alert,
            ticket_id: st.ticket_id.clone(),
            title: st.title.clone(),
            body: st.body.clone(),
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            issue_number: 0,
            sender: "log finding".into(),
            requester: Some(common::requester::Requester::coderhelm(lf::AUTO_REQUESTER)),
            base_branch: "main".into(),
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
            new_run_id: None,
        });
        let body = serde_json::to_string(&msg).map_err(|e| e.to_string())?;
        self.state
            .sqs
            .send_message()
            .queue_url(&self.state.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
            .map_err(|e| format!("queue: {e}"))?;
        Ok(())
    }
}

/// Pure: the order findings are tried in: most severe first, then oldest, so
/// the daily cap goes to what matters most.
pub fn order(findings: &mut [Finding]) {
    findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.created_at.cmp(&b.created_at))
    });
}

/// Sample lines for the ticket, best effort.
async fn samples(state: &WorkerState, team_id: &str, f: &Finding) -> Vec<String> {
    let session = match lf::session_for(
        &state.dynamo,
        &state.sts,
        &state.config.aws_insights_table_name,
        team_id,
        &f.source_account_id,
    )
    .await
    {
        Ok(Some(s)) => s,
        _ => return vec![],
    };
    lf::fetch_samples(&session, f, std::time::Duration::from_secs(20))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| format!("{} {}", s.timestamp, s.message).trim().to_string())
        .collect()
}

/// The resolver's model call, metered on the team's client as log analysis.
struct WorkerJudge {
    provider: crate::agent::provider::ModelProvider,
    model: String,
}

#[async_trait::async_trait]
impl common::log_repo::live::Judge for WorkerJudge {
    async fn judge(&self, system: &str, user: &str) -> Result<String, String> {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "repo": {"type": ["string", "null"]},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                "reason": {"type": "string"},
                "evidence": {"type": "array", "items": {"type": "string"}},
                "candidates": {"type": "array", "items": {"type": "object", "properties": {
                    "repo": {"type": "string"}, "reason": {"type": "string"}
                }, "required": ["repo", "reason"], "additionalProperties": false}}
            },
            "required": ["repo", "confidence", "reason", "evidence", "candidates"],
            "additionalProperties": false
        });
        let mut usage = crate::models::TokenUsage::default();
        let v = common::usage::with_feature(
            common::usage::feature::LOG_ANALYSIS,
            crate::agent::anthropic::converse_json(
                &self.provider.client(),
                &self.model,
                system,
                user,
                &schema,
                true,
                "low",
                1024,
                &mut usage,
            ),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(v.to_string())
    }
}

/// The repo for a finding with no saved one.
async fn resolve_repo(
    state: &WorkerState,
    team_id: &str,
    f: &Finding,
    cfg: &AutoFix,
    findings: &[Finding],
) -> common::log_repo::Resolution {
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
    let token = match super::review_reminder::team_installation_id(state, team_id)
        .await
        .filter(|id| *id > 0)
    {
        Some(id) => match crate::clients::github::GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            id,
            &state.http,
        ) {
            Ok(gh) => gh.installation_token().await.ok(),
            Err(_) => None,
        },
        None => None,
    };
    let http = common::mcp_remote::http::GuardedHttps;
    let host = token.map(|t| common::repo_read::GitHubHost {
        http: &http,
        auth: format!("Bearer {t}"),
    });
    let team = common::log_repo::live::Team {
        dynamo: &state.dynamo,
        sts: &state.sts,
        settings_table: &state.config.settings_table_name,
        repos_table: &state.config.repos_table_name,
        runs_table: &state.config.runs_table_name,
        insights_table: &state.config.aws_insights_table_name,
        team_id,
    };
    let host_ref = host.as_ref().map(|h| h as &dyn common::repo_read::RepoHost);
    match provider {
        Ok(provider) => {
            let judge = WorkerJudge {
                model: provider.primary_model_id().to_string(),
                provider,
            };
            common::log_repo::live::for_finding(&team, host_ref, &judge, f, cfg, findings, false)
                .await
        }
        Err(e) => {
            warn!(team_id, error = %e, "No model provider to work out the repo");
            struct NoModel;
            #[async_trait::async_trait]
            impl common::log_repo::live::Judge for NoModel {
                async fn judge(&self, _: &str, _: &str) -> Result<String, String> {
                    Err("no model provider".into())
                }
            }
            common::log_repo::live::for_finding(&team, host_ref, &NoModel, f, cfg, findings, false)
                .await
        }
    }
}

/// Most findings diagnosed in one sweep (each reads AWS and the code).
const MAX_DIAGNOSED: usize = 12;

/// The team's code host and repos, read once per sweep.
struct Code {
    token: Option<String>,
    repos: Vec<String>,
}

impl Code {
    async fn load(state: &WorkerState, team_id: &str) -> Code {
        let token = match super::review_reminder::team_installation_id(state, team_id)
            .await
            .filter(|id| *id > 0)
        {
            Some(id) => match crate::clients::github::GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                id,
                &state.http,
            ) {
                Ok(gh) => gh.installation_token().await.ok(),
                Err(_) => None,
            },
            None => None,
        };
        let repos = common::log_repo::live::load_repos(
            &state.dynamo,
            &state.config.repos_table_name,
            team_id,
        )
        .await
        .into_iter()
        .map(|r| r.name)
        .collect();
        Code { token, repos }
    }
}

/// Work out the finding's incident diagnosis (read only).
async fn diagnose(
    state: &WorkerState,
    team_id: &str,
    code: &Code,
    f: &Finding,
    now: DateTime<Utc>,
) -> common::log_incident::Diagnosis {
    let session = lf::session_for(
        &state.dynamo,
        &state.sts,
        &state.config.aws_insights_table_name,
        team_id,
        &f.source_account_id,
    )
    .await
    .ok()
    .flatten();
    let lines: Vec<String> = match &session {
        Some(s) => lf::fetch_samples(s, f, std::time::Duration::from_secs(15))
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.message)
            .collect(),
        None => vec![],
    };
    let http = common::mcp_remote::http::GuardedHttps;
    let host = code.token.as_ref().map(|t| common::repo_read::GitHubHost {
        http: &http,
        auth: format!("Bearer {t}"),
    });
    let look = super::log_look::LiveLook {
        session,
        host: host.as_ref().map(|h| h as &dyn common::repo_read::RepoHost),
        repos: code.repos.clone(),
    };
    common::log_incident::diagnose(
        &look,
        &common::log_incident::Input {
            finding: Some(f),
            samples: lines,
        },
        now,
    )
    .await
}

/// Post the incident's one card where the team's findings post, once per
/// incident and outcome.
async fn post_card(
    state: &WorkerState,
    team_id: &str,
    inc: &common::log_incident::Incident,
    titles: &[(String, String)],
    action: &common::log_incident::Action,
    outcome: &str,
    now: DateTime<Utc>,
) {
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let channels = lf::channels(
        &state.dynamo,
        &cipher,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
    if channels.is_empty() {
        return;
    }
    if !lf::claim_incident_card(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &inc.id,
        outcome,
        now,
    )
    .await
    {
        return;
    }
    let base = super::skills::app_url(&state.config.stage);
    let url = format!("{base}/logs?incident={}&finding={}", inc.id, inc.lead);
    let msg = common::log_incident::incident_message(inc, titles, action, &url);
    let started = std::time::Instant::now();
    let sent = common::notify::deliver(
        &common::notify::Http(&state.http),
        &channels,
        &msg,
        "log_incident",
    )
    .await;
    common::deliveries::record_chat(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        "log_incident",
        &sent,
        started.elapsed().as_millis() as u64,
    )
    .await;
}

/// Pure: who in an incident may start its fix: the lead when the team's
/// rules let it, else the first member they let (lead first).
pub fn starter_of<'a>(
    inc: &common::log_incident::Incident,
    screened: &'a [(Finding, Result<Option<String>, Skip>)],
) -> Option<&'a (Finding, Result<Option<String>, Skip>)> {
    inc.members
        .iter()
        .find_map(|m| screened.iter().find(|(f, r)| &f.rec_id == m && r.is_ok()))
}

/// Most stopped findings whose repo is read for a change in one sweep.
const MAX_CHANGE_LOOKUPS: usize = 10;

/// For each open burst the analyzer's checks say stopped, look for a
/// change in the repo saved for its log group that landed as it stopped: a
/// production deploy on record, or a commit on the default branch. Kept on
/// the finding ([`lf::pick_change`]), it lets the analyzer resolve the burst
/// after a day instead of three, with that evidence in the reason. For every
/// team with a saved repo, whether or not automatic fixes are on. Best
/// effort: nothing found or unreadable leaves the default period.
async fn note_changes(state: &WorkerState, team_id: &str, cfg: &AutoFix, findings: &[Finding]) {
    let wanted: Vec<(&Finding, String)> = findings
        .iter()
        .filter_map(|f| lf::change_wanted(f, cfg).map(|repo| (f, repo)))
        .take(MAX_CHANGE_LOOKUPS)
        .collect();
    if wanted.is_empty() {
        return;
    }
    let code = Code::load(state, team_id).await;
    let http = common::mcp_remote::http::GuardedHttps;
    let host = code.token.as_ref().map(|t| common::repo_read::GitHubHost {
        http: &http,
        auth: format!("Bearer {t}"),
    });
    for (f, repo) in wanted {
        if !code.repos.contains(&repo) {
            continue;
        }
        let activity = f.activity();
        let Some(since) = lf::change_window_start(&f.last_seen_at) else {
            continue;
        };
        let until = &activity.checked_at;
        let mut candidates = match lf::deploys_between(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &since,
            until,
        )
        .await
        {
            Ok(records) => lf::deploy_changes(&repo, &records),
            Err(e) => {
                warn!(team_id, rec_id = %f.rec_id, error = %e, "Could not read the team's deploy records for a stopped finding");
                Vec::new()
            }
        };
        if let Some(host) = &host {
            match common::repo_read::RepoHost::commits_between(host, &repo, &since, until).await {
                Ok(commits) => candidates.extend(lf::merge_changes(&repo, &commits)),
                Err(e) => {
                    warn!(team_id, rec_id = %f.rec_id, repo = %repo, error = %e, "Could not read the repo's commits for a stopped finding")
                }
            }
        }
        let Some(change) = lf::pick_change(&candidates, &f.last_seen_at, &activity) else {
            continue;
        };
        match lf::record_change(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
            &f.rec_id,
            &change,
        )
        .await
        {
            Ok(()) => {
                info!(team_id, rec_id = %f.rec_id, repo = %repo, kind = %change.kind, at = %change.at, "A change in the repo explains why the errors stopped")
            }
            Err(e) => {
                warn!(team_id, rec_id = %f.rec_id, error = %e, "Could not keep the change on the finding")
            }
        }
    }
}

async fn sweep_team(
    state: &WorkerState,
    team_id: &str,
    cfg: &AutoFix,
    mut findings: Vec<Finding>,
    report: &mut Report,
) {
    order(&mut findings);
    let now = Utc::now();
    // The findings the team's setting covers and nobody decided yet.
    let screened: Vec<(Finding, Result<Option<String>, Skip>)> = findings
        .iter()
        .filter_map(|f| match lf::screen(cfg, f, now) {
            Err(Skip::Disabled | Skip::NotEligible | Skip::NotNew) => None,
            // Said already; looked at again every sweep, nothing to redo.
            Err(Skip::QuietHours | Skip::Stopped(_)) if f.auto_status == "waiting" => None,
            r => Some((f.clone(), r)),
        })
        .collect();
    if screened.is_empty() {
        return;
    }
    let code = Code::load(state, team_id).await;
    let mut items = Vec::new();
    for (i, (f, _)) in screened.iter().enumerate() {
        let d = if i < MAX_DIAGNOSED {
            diagnose(state, team_id, &code, f, now).await
        } else {
            common::log_incident::Diagnosis::default()
        };
        items.push((f.clone(), d));
    }
    let titles: Vec<(String, String)> = items
        .iter()
        .map(|(f, _)| (f.rec_id.clone(), f.title.clone()))
        .collect();
    for inc in common::log_incident::group(&items) {
        act(
            state, team_id, cfg, &inc, &screened, &findings, &titles, report, now,
        )
        .await;
    }
}

/// Record a decision on a finding (and log it).
async fn record(
    state: &WorkerState,
    team_id: &str,
    f: &Finding,
    skip: &Skip,
    now: DateTime<Utc>,
    report: &mut Report,
) {
    let again = matches!(skip, Skip::Watching | Skip::RepoPending) && f.auto_status == "waiting";
    if !skip.recorded() || again {
        return;
    }
    report.skipped += 1;
    info!(team_id, rec_id = %f.rec_id, reason = %skip.reason(), "Automatic fix not started");
    if let Err(e) = lf::record_skip(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        &f.rec_id,
        skip,
        now,
    )
    .await
    {
        warn!(team_id, rec_id = %f.rec_id, error = %e, "Could not record why the automatic fix didn't start");
    }
}

/// Act once on an incident: say it recovered, start its one fix, or say
/// what only a person can do; every member records the outcome.
#[allow(clippy::too_many_arguments)]
async fn act(
    state: &WorkerState,
    team_id: &str,
    cfg: &AutoFix,
    inc: &common::log_incident::Incident,
    screened: &[(Finding, Result<Option<String>, Skip>)],
    all: &[Finding],
    titles: &[(String, String)],
    report: &mut Report,
    now: DateTime<Utc>,
) {
    use common::log_incident::{plan, Action, Plan};
    let member = |id: &str| screened.iter().find(|(f, _)| f.rec_id == id);
    let lead_title = member(&inc.lead)
        .map(|(f, _)| f.title.clone())
        .unwrap_or_default();

    // Nobody the team's rules let start: each records its own reason, as
    // before; nothing posts.
    let Some((starter, screen)) = starter_of(inc, screened) else {
        for m in &inc.members {
            if let Some((f, Err(skip))) = member(m) {
                record(state, team_id, f, skip, now, report).await;
            }
        }
        return;
    };

    let mut action: Option<Action> = None;
    let mut started_by: Option<String> = None;
    let mut starter_skip: Option<Skip> = None;
    match plan(&inc.diagnosis) {
        Plan::Recovered => action = Some(Action::Recovered),
        Plan::Watching => action = Some(Action::Watching),
        Plan::ForAPerson(why, reason) => action = Some(Action::ForAPerson { why, reason }),
        Plan::FixIn(p) => {
            match start_fix(state, team_id, cfg, inc, titles, starter, &p.repo, now).await {
                Ok(()) => {
                    started_by = Some(starter.rec_id.clone());
                    action = Some(Action::Started { repo: p.repo });
                }
                Err(skip) => starter_skip = Some(skip),
            }
        }
        Plan::Code => {
            let repo = match screen {
                Ok(Some(repo)) => Ok(repo.clone()),
                _ => {
                    let r = resolve_repo(state, team_id, starter, cfg, all).await;
                    match r.repo.clone() {
                        Some(repo) if r.is_sure() => {
                            info!(team_id, rec_id = %starter.rec_id, repo = %repo, reason = %r.reason, "Repo worked out for an automatic fix");
                            if r.source == common::log_repo::Source::Resolved {
                                learn(
                                    state,
                                    team_id,
                                    &starter.source_log_group,
                                    &repo,
                                    &r.reason,
                                    now,
                                )
                                .await;
                            }
                            Ok(repo)
                        }
                        guess => Err((r.clone(), guess)),
                    }
                }
            };
            match repo {
                Ok(repo) => {
                    match start_fix(state, team_id, cfg, inc, titles, starter, &repo, now).await {
                        Ok(()) => {
                            started_by = Some(starter.rec_id.clone());
                            action = Some(Action::Started { repo });
                        }
                        Err(skip) => starter_skip = Some(skip),
                    }
                }
                // The evidence couldn't be weighed (the model wasn't
                // reachable): that is CoderHelm's to retry, not a question
                // for a person. The next sweep weighs it again.
                Err((r, _)) if r.unweighed => starter_skip = Some(Skip::RepoPending),
                Err((r, guess)) => {
                    // The question stays on the finding, so its row offers
                    // the choice in one click.
                    if let Err(e) = lf::record_repo_ask(
                        &state.dynamo,
                        &state.config.aws_insights_table_name,
                        team_id,
                        &starter.rec_id,
                        &common::log_repo::ask_json(&r),
                    )
                    .await
                    {
                        warn!(team_id, rec_id = %starter.rec_id, error = %e, "Could not keep the repo question on the finding");
                    }
                    // Genuinely split: ask once for the log group, never per
                    // finding.
                    if lf::claim_repo_ask(
                        &state.dynamo,
                        &state.config.settings_table_name,
                        team_id,
                        &starter.source_account_id,
                        &starter.source_log_group,
                        now,
                    )
                    .await
                    {
                        action = Some(Action::WhichRepo {
                            guess: guess.clone(),
                            candidates: r.candidates.iter().map(|c| c.repo.clone()).collect(),
                            reason: r.reason.clone(),
                        });
                    }
                    starter_skip = Some(Skip::RepoUnsure(guess.unwrap_or_default()));
                }
            }
        }
    }

    if started_by.is_some() {
        report.started += 1;
    }
    // Keep the incident on every member, with what was done.
    let sentence = action
        .as_ref()
        .map(|a| a.sentence(&inc.diagnosis))
        .or_else(|| starter_skip.as_ref().map(Skip::reason))
        .unwrap_or_default();
    let json = inc.to_json(titles, &sentence);
    for m in &inc.members {
        if let Err(e) = lf::record_incident(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
            m,
            &inc.id,
            &json,
        )
        .await
        {
            warn!(team_id, rec_id = %m, error = %e, "Could not keep the incident on the finding");
        }
    }

    // Every member records the one outcome.
    for m in &inc.members {
        let Some((f, own)) = member(m) else { continue };
        if started_by.as_deref() == Some(m.as_str()) {
            continue;
        }
        let skip = if f.rec_id == starter.rec_id {
            match (&starter_skip, &action) {
                (Some(s), _) => s.clone(),
                (None, Some(Action::Recovered)) => {
                    Skip::Recovered(inc.diagnosis.recovered_at.clone())
                }
                (None, Some(Action::ForAPerson { reason, .. })) => Skip::ForAPerson(reason.clone()),
                (None, Some(Action::Watching)) => Skip::Watching,
                _ => continue,
            }
        } else {
            match (&action, own) {
                (Some(Action::Recovered), _) => Skip::Recovered(inc.diagnosis.recovered_at.clone()),
                (Some(Action::Watching), _) => Skip::Watching,
                (Some(Action::ForAPerson { reason, .. }), _) => Skip::ForAPerson(reason.clone()),
                (Some(Action::Started { .. }), _) => Skip::InIncident(lead_title.clone()),
                (_, Err(s)) => s.clone(),
                (_, Ok(_)) => Skip::InIncident(lead_title.clone()),
            }
        };
        record(state, team_id, f, &skip, now, report).await;
    }

    // One card. A started fix posts when its pull request opens (its card
    // carries the incident), so nothing posts now.
    if let Some(a) = &action {
        let outcome = match a {
            Action::Recovered => Some("recovered"),
            Action::Watching => Some("watching"),
            Action::ForAPerson { .. } => Some("person"),
            Action::WhichRepo { .. } => Some("repo"),
            Action::Started { .. } => None,
        };
        if let Some(outcome) = outcome {
            post_card(state, team_id, inc, titles, a, outcome, now).await;
        }
    }
}

/// Save a repo worked out from decisive evidence for the log group, so it
/// is never asked again; the Logs page shows it as learned.
async fn learn(
    state: &WorkerState,
    team_id: &str,
    log_group: &str,
    repo: &str,
    reason: &str,
    now: DateTime<Utc>,
) {
    match lf::learn_mapping(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        log_group,
        repo,
        reason,
        now,
    )
    .await
    {
        Ok(true) => info!(team_id, log_group, repo, "Learned the log group's repo"),
        Ok(false) => {}
        Err(e) => warn!(team_id, log_group, error = %e, "Could not save the learned repo"),
    }
}

/// Start the incident's one fix from `f` in `repo`, with the diagnosis on
/// its ticket.
#[allow(clippy::too_many_arguments)]
async fn start_fix(
    state: &WorkerState,
    team_id: &str,
    cfg: &AutoFix,
    inc: &common::log_incident::Incident,
    titles: &[(String, String)],
    f: &Finding,
    repo: &str,
    now: DateTime<Utc>,
) -> Result<(), Skip> {
    let lines = samples(state, team_id, f).await;
    // The ticket reads the incident from the finding.
    let mut f = f.clone();
    f.incident_id = inc.id.clone();
    f.incident = inc.to_json(titles, "").to_string();
    let env = WorkerEnv {
        state,
        finding: &f,
        now,
    };
    lf::start(
        &env,
        team_id,
        &f,
        lf::Request {
            repo,
            starter: Starter::Auto,
            daily_cap: cfg.daily_cap,
            samples: &lines,
        },
        now,
    )
    .await
    .map(|_| {
        info!(team_id, rec_id = %f.rec_id, ticket = %f.ticket_id(), incident = %inc.id, "Fix run started automatically from a log finding");
    })
}

pub async fn run(state: &WorkerState) -> Result<Report, String> {
    // Every team with a setting: it holds the repo saved for each log group,
    // which stopped findings are checked against whether or not automatic
    // fixes are on.
    let teams = lf::teams_with_setting(&state.dynamo, &state.config.settings_table_name).await?;
    let mut report = Report {
        teams: teams.iter().filter(|(_, cfg)| cfg.enabled).count(),
        ..Default::default()
    };
    for (team_id, cfg) in &teams {
        if !cfg.enabled && cfg.overrides.iter().all(|o| o.repo.is_empty()) {
            continue;
        }
        let findings = match lf::list_findings(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
            500,
        )
        .await
        {
            Ok(f) => f,
            Err(e) => {
                warn!(team_id, error = %e, "Could not read the team's log findings");
                continue;
            }
        };
        note_changes(state, team_id, cfg, &findings).await;
        if cfg.enabled {
            sweep_team(state, team_id, cfg, findings, &mut report).await;
        }
    }
    Ok(report)
}

/// The card when a fix run from a log finding opens its pull request
/// (started automatically or by a person): posted where the findings post,
/// which is the team's alert channel unless the team chose otherwise. Posts
/// once per finding ([`lf::announce_fix_pr`]). Best effort.
pub async fn notify_pr(
    state: &WorkerState,
    team_id: &str,
    ticket: &lf::TicketRecord,
    pr: &lf::FixPr,
) {
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let channels = lf::channels(
        &state.dynamo,
        &cipher,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
    if channels.is_empty() {
        info!(
            team_id,
            pr = pr.number,
            "No channel for log findings; the fix PR card isn't posted"
        );
        return;
    }
    let finding = lf::load_finding(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        &ticket.rec_id,
    )
    .await
    .ok()
    .flatten();
    let base = super::skills::app_url(&state.config.stage);
    let finding_url = lf::finding_url(base, &ticket.rec_id);
    let cards = lf::DynamoFixCards {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    };
    let started = std::time::Instant::now();
    if let Some(sent) = lf::announce_fix_pr(
        &cards,
        &common::notify::Http(&state.http),
        &channels,
        team_id,
        ticket,
        finding.as_ref(),
        pr,
        &finding_url,
        Utc::now(),
    )
    .await
    {
        common::deliveries::record_chat(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            "log_fix_pr",
            &sent,
            started.elapsed().as_millis() as u64,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_most_severe_and_oldest_go_first() {
        let f = |id: &str, sev: lf::Severity, at: &str| Finding {
            rec_id: id.into(),
            severity: sev,
            created_at: at.into(),
            ..Default::default()
        };
        let mut v = vec![
            f("a", lf::Severity::Warning, "2026-10-20T00:00:00Z"),
            f("b", lf::Severity::Critical, "2026-10-21T00:00:00Z"),
            f("c", lf::Severity::Critical, "2026-10-20T00:00:00Z"),
            f("d", lf::Severity::Info, "2026-10-19T00:00:00Z"),
        ];
        order(&mut v);
        let ids: Vec<&str> = v.iter().map(|f| f.rec_id.as_str()).collect();
        assert_eq!(ids, ["c", "b", "a", "d"]);
    }

    #[test]
    fn the_incident_starts_from_its_lead_or_the_first_member_the_rules_let() {
        let f = |id: &str| Finding {
            rec_id: id.into(),
            ..Default::default()
        };
        let inc = common::log_incident::Incident {
            lead: "dns".into(),
            members: vec!["dns".into(), "idx".into(), "web".into()],
            ..Default::default()
        };
        let screened = vec![
            (f("web"), Err(Skip::BelowSeverity)),
            (f("idx"), Ok(None)),
            (f("dns"), Ok(None)),
        ];
        assert_eq!(starter_of(&inc, &screened).unwrap().0.rec_id, "dns");
        // The lead is below the team's floor: the next member starts it.
        let screened = vec![
            (f("dns"), Err(Skip::BelowSeverity)),
            (f("idx"), Ok(Some("acme/ledger-api".into()))),
        ];
        assert_eq!(starter_of(&inc, &screened).unwrap().0.rec_id, "idx");
        // Nobody may: nothing starts and nothing posts.
        let screened = vec![(f("dns"), Err(Skip::NotIncluded))];
        assert!(starter_of(&inc, &screened).is_none());
    }

    #[test]
    fn the_sweep_message_parses() {
        let m: WorkerMessage = serde_json::from_str(r#"{"type":"log_autofix"}"#).unwrap();
        assert!(matches!(m, WorkerMessage::LogAutofix));
    }
}
