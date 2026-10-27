//! Automatic fix PRs for new log findings (EventBridge → SQS, hourly).
//!
//! For every team that turned automatic fixes on, each open finding the sweep
//! hasn't decided yet goes through [`common::log_findings::screen`] (new since
//! it was turned on, severity, log groups, repo, quiet hours) and then
//! [`common::log_findings::start`] (one open run or PR per fingerprint, the
//! team's token limit, the daily cap). A finding that doesn't start records
//! why ("daily cap reached", "already fixing", ...), and the Logs page shows
//! it. Off by default: nothing runs for a team that never turned it on.
//!
//! A log group with no saved repo has its repo worked out
//! ([`common::log_repo`]): a high-confidence answer starts the run there;
//! anything less records the best guess, posts a "which repo?" message
//! where the team's findings post, and leaves the start to a person.

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
            model_retries: 0,
            ticket_update_note: None,
            ticket_history_note: None,
            plan_cache_key: None,
            team_instructions: String::new(),
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

/// Ask a person to check the repo (where the team's findings post).
async fn ask_for_repo(
    state: &WorkerState,
    team_id: &str,
    f: &Finding,
    r: &common::log_repo::Resolution,
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
    let base = super::skills::app_url(&state.config.stage);
    let url = format!("{base}/logs?finding={}", f.rec_id);
    let candidates: Vec<String> = r.candidates.iter().map(|c| c.repo.clone()).collect();
    let msg = lf::repo_check_message(f, r.repo.as_deref(), &r.reason, &candidates, &url);
    common::notify::deliver(
        &common::notify::Http(&state.http),
        &channels,
        &msg,
        "log_fix_repo",
    )
    .await;
}

async fn sweep_team(state: &WorkerState, team_id: &str, cfg: &AutoFix, report: &mut Report) {
    let mut findings = match lf::list_findings(
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
            return;
        }
    };
    order(&mut findings);
    for f in &findings {
        let now = Utc::now();
        let screened = match lf::screen(cfg, f, now) {
            Ok(Some(repo)) => Ok(repo),
            Ok(None) => {
                let r = resolve_repo(state, team_id, f, cfg, &findings).await;
                match r.repo.clone() {
                    Some(repo) if r.is_sure() => {
                        info!(team_id, rec_id = %f.rec_id, repo = %repo, reason = %r.reason, "Repo worked out for an automatic fix");
                        Ok(repo)
                    }
                    guess => {
                        ask_for_repo(state, team_id, f, &r).await;
                        Err(Skip::RepoUnsure(guess.unwrap_or_default()))
                    }
                }
            }
            Err(skip) => Err(skip),
        };
        let outcome = match screened {
            Ok(repo) => {
                let lines = samples(state, team_id, f).await;
                let env = WorkerEnv {
                    state,
                    finding: f,
                    now,
                };
                lf::start(
                    &env,
                    team_id,
                    f,
                    lf::Request {
                        repo: &repo,
                        starter: Starter::Auto,
                        daily_cap: cfg.daily_cap,
                        samples: &lines,
                    },
                    now,
                )
                .await
                .map(|_| ())
            }
            Err(skip) => Err(skip),
        };
        match outcome {
            Ok(()) => {
                report.started += 1;
                info!(team_id, rec_id = %f.rec_id, ticket = %f.ticket_id(), "Fix run started automatically from a log finding");
            }
            Err(skip) if skip.recorded() => {
                // Don't rewrite the same wait every hour.
                if skip == Skip::QuietHours && f.auto_status == "waiting" {
                    continue;
                }
                report.skipped += 1;
                info!(team_id, rec_id = %f.rec_id, reason = %skip.reason(), "Automatic fix not started");
                if let Err(e) = lf::record_skip(
                    &state.dynamo,
                    &state.config.aws_insights_table_name,
                    team_id,
                    &f.rec_id,
                    &skip,
                    now,
                )
                .await
                {
                    warn!(team_id, rec_id = %f.rec_id, error = %e, "Could not record why the automatic fix didn't start");
                }
            }
            Err(_) => {}
        }
    }
}

pub async fn run(state: &WorkerState) -> Result<Report, String> {
    let teams = lf::teams_with_autofix(&state.dynamo, &state.config.settings_table_name).await?;
    let mut report = Report {
        teams: teams.len(),
        ..Default::default()
    };
    for (team_id, cfg) in &teams {
        sweep_team(state, team_id, cfg, &mut report).await;
    }
    Ok(report)
}

/// The follow-up when a fix run from a log finding opens its pull request:
/// posted where the team's findings post (Teams, Slack). Best effort.
pub async fn notify_pr(
    state: &WorkerState,
    team_id: &str,
    ticket: &lf::TicketRecord,
    repo: &str,
    number: u64,
    title: &str,
    url: &str,
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
    let base = super::skills::app_url(&state.config.stage);
    let finding_url = if ticket.rec_id.is_empty() {
        format!("{base}/logs")
    } else {
        format!("{base}/logs?finding={}", ticket.rec_id)
    };
    let msg = lf::fix_message(ticket, repo, number, title, url, &finding_url);
    common::notify::deliver_recorded(
        &common::notify::Http(&state.http),
        &channels,
        &msg,
        "log_fix_pr",
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await;
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
    fn the_sweep_message_parses() {
        let m: WorkerMessage = serde_json::from_str(r#"{"type":"log_autofix"}"#).unwrap();
        assert!(matches!(m, WorkerMessage::LogAutofix));
    }
}
