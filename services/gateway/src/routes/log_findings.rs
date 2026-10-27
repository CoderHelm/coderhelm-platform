//! The Logs page: the log analyzer's findings in one list, with their fix runs.
//!
//! - `GET  /api/log-findings` — every finding with its status (new,
//!   acknowledged, fixing, fixed, ignored), its fix run and pull request,
//!   counts per status and whether AWS is connected.
//! - `GET  /api/log-findings/:id/samples` — a few recent lines of the error,
//!   read live with the team's log reader role and scrubbed; never stored.
//! - `POST /api/log-findings/:id/fix` — start a fix run (a person's "Create
//!   fix PR"), under the rules in [`common::log_findings::start`].
//! - `POST /api/log-findings/:id/{ignore,acknowledge,reopen}`.
//! - `GET|PUT /api/log-findings/autofix` — the team's automatic-fix setting.
//!   The worker's hourly sweep applies it.
//! - `GET  /api/log-findings/:id/repo` — which repo the finding's code lives
//!   in: its saved mapping, or worked out from the evidence
//!   ([`common::log_repo`]); `?refresh=1` works it out again.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::Utc;
use common::log_findings::{self as lf, AutoFix, Finding, Skip, Start, StartEnv, Starter};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::{Claims, TicketMessage, TicketSource, WorkerMessage};
use crate::AppState;

type Reply = (StatusCode, Json<Value>);

/// `error` is the sentence the dashboard shows; `code` is for programs.
fn fail(status: StatusCode, code: &str, message: &str) -> Reply {
    (status, Json(json!({ "error": message, "code": code })))
}

fn internal(what: &str, e: impl std::fmt::Display) -> Reply {
    error!(error = %e, "{what}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "Something went wrong. Try again.",
    )
}

fn attr(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn forbidden(status: StatusCode) -> Reply {
    fail(status, "forbidden", "Your role can't do this.")
}

fn role(claims: &Claims, level: u8) -> Result<(), Reply> {
    claims
        .require_role(level)
        .map_err(|c| fail(c, "forbidden", "Your role can't do this."))
}

/// `account → region` of the team's AWS connections.
async fn connections(state: &AppState, team_id: &str) -> Result<HashMap<String, String>, Reply> {
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.aws_insights_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", attr(team_id))
        .expression_attribute_values(":p", attr("AWS_CONN#"))
        .send()
        .await
        .map_err(|e| internal("Could not read AWS connections", e))?;
    Ok(out
        .items()
        .iter()
        .filter_map(|i| {
            let account = i.get("account_id")?.as_s().ok()?.clone();
            let region = i
                .get("region")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_else(|| "us-east-1".into());
            Some((account, region))
        })
        .collect())
}

/// Pure: one finding as the page shows it.
pub fn finding_json(
    f: &Finding,
    fix: lf::FixState,
    run: Option<&lf::RunRef>,
    cfg: &AutoFix,
    region: Option<&str>,
) -> Value {
    let status = lf::display_status(f, fix);
    let mapped = cfg.policy(&f.source_log_group).repo;
    let repo = Some(f.fix_repo.clone())
        .filter(|r| !r.is_empty())
        .or(mapped.clone());
    let fix_json = (!f.fix_ticket_id.is_empty()).then(|| {
        json!({
            "state": fix,
            "ticket_id": f.fix_ticket_id,
            "run_id": run.map(|r| r.run_id.as_str()).filter(|s| !s.is_empty()),
            "run_status": run.map(|r| r.status.as_str()).filter(|s| !s.is_empty()),
            "pr_number": run.map(|r| r.pr_number).filter(|n| *n > 0),
            "pr_url": run.map(|r| r.pr_url.as_str()).filter(|s| !s.is_empty()),
            "auto": f.fix_auto,
            "started_at": f.fix_started_at,
        })
    });
    let auto = (!f.auto_status.is_empty())
        .then(|| json!({ "status": f.auto_status, "reason": f.auto_reason }));
    json!({
        "rec_id": f.rec_id,
        "title": f.title,
        "summary": f.summary,
        "suggested_action": f.suggested_action,
        "severity": f.severity.as_str(),
        "status": status,
        "log_group": f.source_log_group,
        "account_id": f.source_account_id,
        "error_pattern": f.error_pattern,
        "error_count": f.error_count,
        // Lines, only once the analyzer counts lines (see Finding::events_seen).
        "seen_count": f.events_seen(),
        "first_seen": f.first_seen_at,
        "last_seen": f.last_seen_at,
        "suspected_location": (!f.suspected_location.is_empty()).then_some(&f.suspected_location),
        "plan_id": (!f.plan_id.is_empty()).then_some(&f.plan_id),
        "repo": repo,
        "repo_mapped": mapped.is_some(),
        "console_url": region.and_then(|r| lf::console_url(r, &f.source_log_group)),
        "fix": fix_json,
        "auto": auto,
        // Where the lines are: this group's streams and the other groups the
        // same error was counted in, never a list of the account's groups.
        "log_streams": f.log_streams.iter().map(|c| json!({ "log_stream": c.name, "lines": c.lines })).collect::<Vec<_>>(),
        "also_in": f.also_in.iter().map(|c| json!({ "log_group": c.name, "lines": c.lines })).collect::<Vec<_>>(),
        "reopened_at": (!f.reopened_at.is_empty()).then_some(&f.reopened_at),
        "reopen_count": f.reopen_count,
    })
}

#[derive(Deserialize, Default)]
pub struct ListQuery {
    status: Option<String>,
    severity: Option<String>,
    log_group: Option<String>,
    q: Option<String>,
}

/// Pure: whether a finding passes the page's filters.
pub fn matches(row: &Value, q: &ListQuery) -> bool {
    let field = |k: &str| row[k].as_str().unwrap_or("");
    let one_of = |want: &Option<String>, have: &str| {
        want.as_deref()
            .filter(|w| !w.is_empty())
            .is_none_or(|w| w.split(',').any(|x| x == have))
    };
    let text = q.q.as_deref().map(str::trim).unwrap_or("").to_lowercase();
    one_of(&q.status, field("status"))
        && one_of(&q.severity, field("severity"))
        && q.log_group
            .as_deref()
            .filter(|g| !g.is_empty())
            .is_none_or(|g| field("log_group") == g)
        && (text.is_empty()
            || ["title", "summary", "log_group", "error_pattern"]
                .iter()
                .any(|k| field(k).to_lowercase().contains(&text)))
}

/// Most findings read for the page.
const MAX_FINDINGS: usize = 500;

/// GET /api/log-findings — the findings, their fixes and counts.
pub async fn list_findings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, Reply> {
    let team = claims.team_id.as_str();
    let conns = connections(&state, team).await?;
    let findings = lf::list_findings(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team,
        MAX_FINDINGS,
    )
    .await
    .map_err(|e| internal("Could not list log findings", e))?;
    let cfg = lf::load_autofix(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("Could not read the automatic-fix setting", e))?;

    // Each fix ticket's runs, read once even when findings share a ticket.
    let now = Utc::now();
    let mut runs: HashMap<String, Vec<lf::RunRef>> = HashMap::new();
    for t in findings
        .iter()
        .map(|f| f.fix_ticket_id.as_str())
        .filter(|t| !t.is_empty())
        .collect::<HashSet<_>>()
    {
        match lf::runs_for_ticket(&state.dynamo, &state.config.runs_table_name, team, t).await {
            Ok(r) => {
                runs.insert(t.to_string(), r);
            }
            Err(e) => warn!(ticket = t, error = %e, "Could not read a fix ticket's runs"),
        }
    }

    let mut rows = Vec::new();
    let mut counts: HashMap<&str, u64> = lf::STATUSES.iter().map(|s| (*s, 0)).collect();
    let mut groups = std::collections::BTreeSet::new();
    for f in &findings {
        let (state_, run) = if f.fix_ticket_id.is_empty() {
            (lf::FixState::None, None)
        } else {
            lf::fix_state(
                runs.get(&f.fix_ticket_id).map(Vec::as_slice).unwrap_or(&[]),
                &f.fix_started_at,
                now,
            )
        };
        let row = finding_json(
            f,
            state_,
            run.as_ref(),
            &cfg,
            conns.get(&f.source_account_id).map(String::as_str),
        );
        if let Some(c) = counts.get_mut(row["status"].as_str().unwrap_or("")) {
            *c += 1;
        }
        if !f.source_log_group.is_empty() {
            groups.insert(f.source_log_group.clone());
        }
        if matches(&row, &q) {
            rows.push(row);
        }
    }
    Ok(Json(json!({
        "findings": rows,
        "counts": counts,
        "log_groups": groups,
        "aws_connected": !conns.is_empty(),
    })))
}

async fn finding_or_404(state: &AppState, team: &str, id: &str) -> Result<Finding, Reply> {
    lf::load_finding(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team,
        id,
    )
    .await
    .map_err(|e| internal("Could not read the finding", e))?
    .ok_or_else(|| fail(StatusCode::NOT_FOUND, "not_found", "That finding is gone."))
}

/// Sample lines, best effort: an empty list when the role or the search fails.
async fn samples(
    state: &AppState,
    team: &str,
    f: &Finding,
    wait_secs: u64,
) -> Result<Vec<lf::Sample>, String> {
    let Some(session) = lf::session_for(
        &state.dynamo,
        &state.sts,
        &state.config.aws_insights_table_name,
        team,
        &f.source_account_id,
    )
    .await?
    else {
        return Ok(vec![]);
    };
    lf::fetch_samples(&session, f, std::time::Duration::from_secs(wait_secs)).await
}

/// GET /api/log-findings/:id/samples — recent lines of the error (member+).
pub async fn get_samples(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    let f = finding_or_404(&state, &claims.team_id, &id).await?;
    match samples(&state, &claims.team_id, &f, 20).await {
        Ok(lines) => Ok(Json(json!({ "samples": lines }))),
        Err(e) => {
            warn!(team_id = %claims.team_id, error = %e, "Could not read sample lines");
            Ok(Json(json!({
                "samples": [],
                "error": "CoderHelm couldn't read the log group just now.",
            })))
        }
    }
}

/// The gateway's side of a start.
struct GatewayEnv<'a> {
    state: &'a AppState,
    claims: &'a Claims,
    finding: &'a Finding,
}

#[async_trait::async_trait]
impl StartEnv for GatewayEnv<'_> {
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
        super::github_webhook::check_run_budget(self.state, team_id).await
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
    async fn take_daily_slot(&self, _team_id: &str, _cap: u32) -> bool {
        // A person's start never counts against the automatic cap.
        true
    }
    async fn send(&self, team_id: &str, st: &Start) -> Result<(), String> {
        let installation_id = super::api::get_team_installation_id(self.state, team_id)
            .await
            .map_err(|_| "the GitHub app isn't installed".to_string())?;
        let (owner, name) = st.repo.split_once('/').ok_or("bad repo")?;
        lf::record_start(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            &self.state.config.aws_insights_table_name,
            team_id,
            self.finding,
            st,
            Utc::now(),
        )
        .await?;
        let message = WorkerMessage::Ticket(TicketMessage {
            team_id: team_id.to_string(),
            installation_id,
            source: TicketSource::Alert,
            ticket_id: st.ticket_id.clone(),
            title: st.title.clone(),
            body: st.body.clone(),
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            issue_number: 0,
            sender: self
                .claims
                .github_login
                .clone()
                .unwrap_or_else(|| self.claims.email.clone()),
            requester: common::requester::Requester::member(
                self.claims.github_login.as_deref(),
                &self.claims.email,
            ),
            image_attachments: vec![],
            ..Default::default()
        });
        let body = serde_json::to_string(&message).map_err(|e| e.to_string())?;
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

#[derive(Deserialize, Default)]
pub struct FixRequest {
    /// `owner/name`. Empty: the repo mapped to the finding's log group.
    #[serde(default)]
    repo: String,
    /// Map the finding's log group to `repo` for later fixes (admin+).
    #[serde(default)]
    remember: bool,
}

/// Pure: the HTTP answer for a start that didn't happen.
pub fn skip_reply(skip: &Skip) -> Reply {
    let (code, error) = match skip {
        Skip::AlreadyFixing => (StatusCode::CONFLICT, "already_fixing"),
        Skip::Budget(_) => (StatusCode::TOO_MANY_REQUESTS, "budget"),
        Skip::NoRepo => (StatusCode::BAD_REQUEST, "no_repo"),
        Skip::DailyCap => (StatusCode::TOO_MANY_REQUESTS, "daily_cap"),
        _ => (StatusCode::BAD_GATEWAY, "could_not_start"),
    };
    let message = match skip {
        Skip::AlreadyFixing => {
            "A fix run or pull request is already open for this error.".to_string()
        }
        Skip::NoRepo => "Choose the repo this log group's code lives in.".to_string(),
        other => {
            let r = other.reason();
            let mut c = r.chars();
            c.next()
                .map(|f| f.to_uppercase().collect::<String>() + c.as_str() + ".")
                .unwrap_or_default()
        }
    };
    fail(code, error, &message)
}

// ─── Which repo ─────────────────────────────────────────────────────────────

/// The resolver's model call: the team's own model, metered as log analysis.
pub(crate) struct GatewayJudge {
    http: reqwest::Client,
    api_key: String,
    model: String,
    meter: common::usage::UsageMeter,
}

impl GatewayJudge {
    pub(crate) async fn for_team(state: &AppState, team_id: &str) -> GatewayJudge {
        let (key, _heavy, primary) = super::brain::load_provider(state, team_id).await;
        GatewayJudge {
            http: reqwest::Client::new(),
            api_key: key.unwrap_or_default(),
            model: primary,
            meter: crate::model_call::chat_meter(state, team_id),
        }
    }
}

#[async_trait::async_trait]
impl common::log_repo::live::Judge for GatewayJudge {
    async fn judge(&self, system: &str, user: &str) -> Result<String, String> {
        if self.api_key.is_empty() && !common::model_provider::is_bedrock() {
            return Err("no model provider key".into());
        }
        let body = json!({
            "model": self.model,
            "max_tokens": 1024,
            "system": system,
            "messages": [{ "role": "user", "content": user }],
        });
        let m = crate::model_call::log_repo_turn(
            &self.http,
            &self.api_key,
            &body,
            &self.meter,
            &self.model,
        )
        .await
        .map_err(|e| e.message)?;
        Ok(m["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| b["text"].as_str())
            .collect::<Vec<_>>()
            .join(""))
    }
}

/// Where the team's records are, for the resolver.
pub(crate) fn team_ref<'a>(
    state: &'a AppState,
    team_id: &'a str,
) -> common::log_repo::live::Team<'a> {
    common::log_repo::live::Team {
        dynamo: &state.dynamo,
        sts: &state.sts,
        settings_table: &state.config.settings_table_name,
        repos_table: &state.config.repos_table_name,
        runs_table: &state.config.runs_table_name,
        insights_table: &state.config.aws_insights_table_name,
        team_id,
    }
}

#[derive(Deserialize, Default)]
pub struct RepoQuery {
    #[serde(default)]
    refresh: Option<String>,
}

/// GET /api/log-findings/:id/repo — the repo a fix for this finding goes
/// to, how sure, and why (member+).
pub async fn get_repo(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Query(q): Query<RepoQuery>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    let team = claims.team_id.as_str();
    let f = finding_or_404(&state, team, &id).await?;
    let cfg = lf::load_autofix(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("Could not read the automatic-fix setting", e))?;
    let findings = lf::list_findings(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team,
        MAX_FINDINGS,
    )
    .await
    .unwrap_or_default();
    let judge = GatewayJudge::for_team(&state, team).await;
    let token = super::brain::github_token(&state, team).await;
    let http = common::mcp_remote::http::GuardedHttps;
    let host = token.map(|t| common::repo_read::GitHubHost {
        http: &http,
        auth: format!("Bearer {t}"),
    });
    let r = common::log_repo::live::for_finding(
        &team_ref(&state, team),
        host.as_ref().map(|h| h as &dyn common::repo_read::RepoHost),
        &judge,
        &f,
        &cfg,
        &findings,
        matches!(q.refresh.as_deref(), Some("1" | "true")),
    )
    .await;
    Ok(Json(common::log_repo::to_json(&r)))
}

/// POST /api/log-findings/:id/fix — start a fix run from a finding (member+).
pub async fn create_fix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    body: Option<Json<FixRequest>>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    let req = body.map(|b| b.0).unwrap_or_default();
    let team = claims.team_id.as_str();
    let f = finding_or_404(&state, team, &id).await?;
    let mut cfg = lf::load_autofix(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("Could not read the automatic-fix setting", e))?;
    let repo = if req.repo.trim().is_empty() {
        cfg.policy(&f.source_log_group).repo.unwrap_or_default()
    } else {
        req.repo.trim().to_string()
    };
    if !lf::valid_repo(&repo) {
        return Err(skip_reply(&Skip::NoRepo));
    }
    // Only a repo the team's GitHub App covers.
    match super::skills::team_has_repo(&state, team, &repo).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "unknown_repo",
                "That repo isn't one CoderHelm can work in for this team.",
            ))
        }
        Err(e) => return Err(internal("Could not check the repo", e)),
    }
    let lines: Vec<String> = samples(&state, team, &f, 12)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| format!("{} {}", s.timestamp, s.message).trim().to_string())
        .collect();
    let env = GatewayEnv {
        state: &state,
        claims: &claims,
        finding: &f,
    };
    let st = lf::start(
        &env,
        team,
        &f,
        lf::Request {
            repo: &repo,
            starter: Starter::Person(claims.email.clone()),
            daily_cap: cfg.daily_cap,
            samples: &lines,
        },
        Utc::now(),
    )
    .await
    .map_err(|s| skip_reply(&s))?;
    info!(team_id = team, rec_id = %f.rec_id, ticket = %st.ticket_id, repo = %st.repo, "Fix run started from a log finding");

    // Remember the mapping for next time, when an admin asked.
    if req.remember && claims.require_role(3).is_ok() && !f.source_log_group.is_empty() {
        match cfg
            .overrides
            .iter_mut()
            .find(|o| o.log_group == f.source_log_group)
        {
            Some(o) => o.repo = repo.clone(),
            None => cfg.overrides.push(lf::GroupOverride {
                log_group: f.source_log_group.clone(),
                repo: repo.clone(),
                ..Default::default()
            }),
        }
        if cfg.validate().is_ok() {
            if let Err(e) = save_autofix(&state, team, &cfg, &claims.email).await {
                warn!(error = ?e.1, "Could not remember the log group's repo");
            }
        }
    }
    Ok(Json(json!({
        "status": "fixing",
        "ticket_id": st.ticket_id,
        "repo": st.repo,
    })))
}

async fn set_status(
    state: &AppState,
    claims: &Claims,
    id: &str,
    action: &str,
) -> Result<Json<Value>, Reply> {
    role(claims, 1)?;
    let team = claims.team_id.as_str();
    finding_or_404(state, team, id).await?;
    let now = Utc::now().to_rfc3339();
    let u = state
        .dynamo
        .update_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", attr(team))
        .key("sk", attr(&format!("{}{id}", lf::REC_PREFIX)))
        .condition_expression("attribute_exists(pk)")
        .expression_attribute_values(":t", attr(&now));
    let u = match action {
        "ignore" => u
            .update_expression("SET #s = :s, updated_at = :t, dismissed_by = :u")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr("dismissed"))
            .expression_attribute_values(":u", attr(&claims.email)),
        "acknowledge" => u
            .update_expression("SET acknowledged_at = :t, acknowledged_by = :u, updated_at = :t")
            .expression_attribute_values(":u", attr(&claims.email)),
        // Back to open: ignored or acknowledged findings return to New.
        _ => u
            .update_expression("SET #s = :s, updated_at = :t REMOVE acknowledged_at, acknowledged_by, dismissed_by")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", attr("pending")),
    };
    u.send()
        .await
        .map_err(|e| internal("Could not update the finding", e))?;
    info!(team_id = team, rec_id = id, action, "Log finding updated");
    Ok(Json(json!({ "ok": true })))
}

/// POST /api/log-findings/:id/ignore (member+)
pub async fn ignore_finding(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    set_status(&state, &claims, &id, "ignore").await
}

/// POST /api/log-findings/:id/acknowledge (member+)
pub async fn acknowledge_finding(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    set_status(&state, &claims, &id, "acknowledge").await
}

/// POST /api/log-findings/:id/reopen (member+)
pub async fn reopen_finding(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(1).map_err(forbidden)?;
    set_status(&state, &claims, &id, "reopen").await
}

/// GET /api/log-findings/autofix — the setting and today's automatic runs.
pub async fn get_autofix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, Reply> {
    let team = claims.team_id.as_str();
    let cfg = lf::load_autofix(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("Could not read the automatic-fix setting", e))?;
    let today = lf::runs_today(
        &state.dynamo,
        &state.config.settings_table_name,
        team,
        Utc::now(),
    )
    .await;
    Ok(Json(json!({ "autofix": cfg, "runs_today": today })))
}

async fn save_autofix(state: &AppState, team: &str, cfg: &AutoFix, by: &str) -> Result<(), Reply> {
    let config = serde_json::to_string(cfg).map_err(|e| internal("Could not save", e))?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr(team))
        .item("sk", attr(lf::AUTOFIX_SK))
        .item("config", attr(&config))
        .item("updated_at", attr(&Utc::now().to_rfc3339()))
        .item("updated_by", attr(by))
        .send()
        .await
        .map_err(|e| internal("Could not save the automatic-fix setting", e))?;
    Ok(())
}

/// Pure: the setting to store from what a person sent and what was saved:
/// `enabled_at` is the server's, moved only when fixes are turned on.
pub fn next_autofix(mut sent: AutoFix, prior: &AutoFix, now: &str) -> AutoFix {
    sent.enabled_at = if sent.enabled && !prior.enabled {
        now.to_string()
    } else if sent.enabled {
        prior.enabled_at.clone()
    } else {
        String::new()
    };
    sent.overrides.retain(|o| !o.log_group.trim().is_empty());
    sent
}

/// PUT /api/log-findings/autofix — save the setting (admin+).
pub async fn put_autofix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(sent): Json<AutoFix>,
) -> Result<Json<Value>, Reply> {
    claims.require_role(3).map_err(|c| {
        fail(
            c,
            "forbidden",
            "Only an admin or the owner can change this.",
        )
    })?;
    let team = claims.team_id.as_str();
    let prior = lf::load_autofix(&state.dynamo, &state.config.settings_table_name, team)
        .await
        .map_err(|e| internal("Could not read the automatic-fix setting", e))?;
    let cfg = next_autofix(sent, &prior, &Utc::now().to_rfc3339());
    cfg.validate()
        .map_err(|m| fail(StatusCode::BAD_REQUEST, "invalid", &m))?;
    for repo in cfg
        .overrides
        .iter()
        .map(|o| o.repo.as_str())
        .filter(|r| !r.is_empty())
        .collect::<HashSet<_>>()
    {
        match super::skills::team_has_repo(&state, team, repo).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "unknown_repo",
                    &format!("{repo} isn't one CoderHelm can work in for this team."),
                ))
            }
            Err(e) => return Err(internal("Could not check a repo", e)),
        }
    }
    save_autofix(&state, team, &cfg, &claims.email).await?;
    info!(
        team_id = team,
        enabled = cfg.enabled,
        cap = cfg.daily_cap,
        "Automatic log fixes saved"
    );
    Ok(Json(json!({ "ok": true, "autofix": cfg })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding() -> Finding {
        Finding {
            rec_id: "01abc".into(),
            status: "pending".into(),
            severity: lf::Severity::Warning,
            title: "Checkout timeouts".into(),
            source_log_group: "/aws/lambda/shop-checkout".into(),
            source_account_id: "111122223333".into(),
            error_pattern: "Task timed out".into(),
            created_at: "2026-10-20T12:00:00Z".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_row_carries_its_fix_run_and_pr() {
        let mut f = finding();
        f.fix_ticket_id = "LOG-ABCDEF12".into();
        f.fix_repo = "acme/shop".into();
        f.fix_auto = true;
        let run = lf::RunRef {
            run_id: "01J".into(),
            status: "completed".into(),
            pr_number: 42,
            pr_url: "https://github.com/acme/shop/pull/42".into(),
            ..Default::default()
        };
        let row = finding_json(
            &f,
            lf::FixState::PrOpen,
            Some(&run),
            &AutoFix::default(),
            Some("us-east-1"),
        );
        assert_eq!(row["status"], "fixing");
        assert_eq!(row["fix"]["pr_number"], 42);
        assert_eq!(row["fix"]["state"], "pr_open");
        assert_eq!(row["fix"]["auto"], true);
        assert_eq!(row["repo"], "acme/shop");
        assert!(row["console_url"]
            .as_str()
            .unwrap()
            .contains("logsV2:log-groups"));
        let plain = finding_json(
            &finding(),
            lf::FixState::None,
            None,
            &AutoFix::default(),
            None,
        );
        assert!(
            plain["fix"].is_null() && plain["console_url"].is_null() && plain["repo"].is_null()
        );
    }

    #[test]
    fn a_row_names_its_streams_other_groups_and_reopen() {
        let mut f = finding();
        f.log_streams = vec![lf::Counted {
            name: "web/web/a1".into(),
            lines: 30,
        }];
        f.also_in = vec![lf::Counted {
            name: "/aws/lambda/jobs".into(),
            lines: 4,
        }];
        f.reopened_at = "2026-10-27T06:00:00+00:00".into();
        f.reopen_count = 1;
        let row = finding_json(&f, lf::FixState::None, None, &AutoFix::default(), None);
        assert_eq!(
            row["log_streams"],
            json!([{ "log_stream": "web/web/a1", "lines": 30 }])
        );
        assert_eq!(
            row["also_in"],
            json!([{ "log_group": "/aws/lambda/jobs", "lines": 4 }])
        );
        assert_eq!(row["reopened_at"], "2026-10-27T06:00:00+00:00");
        assert_eq!(row["reopen_count"], 1);
        assert_eq!(row["status"], "new");
        let plain = finding_json(
            &finding(),
            lf::FixState::None,
            None,
            &AutoFix::default(),
            None,
        );
        assert!(plain["reopened_at"].is_null());
        assert_eq!(plain["log_streams"], json!([]));
    }

    #[test]
    fn a_row_shows_the_lines_own_times_and_count() {
        let mut f = finding();
        f.first_seen_at = "2026-10-24T09:12:00.000+00:00".into();
        f.last_seen_at = "2026-10-25T05:59:00.000+00:00".into();
        f.seen_count = Some(44534);
        f.count_basis = "events".into();
        let row = finding_json(&f, lf::FixState::None, None, &AutoFix::default(), None);
        assert_eq!(row["first_seen"], "2026-10-24T09:12:00.000+00:00");
        assert_eq!(row["last_seen"], "2026-10-25T05:59:00.000+00:00");
        assert_eq!(row["seen_count"], 44534);
        // An older item counted analyses: no line count is claimed.
        f.count_basis.clear();
        let row = finding_json(&f, lf::FixState::None, None, &AutoFix::default(), None);
        assert!(row["seen_count"].is_null());
    }

    #[test]
    fn filters_combine() {
        let row = finding_json(
            &finding(),
            lf::FixState::None,
            None,
            &AutoFix::default(),
            None,
        );
        let q = |status: &str, sev: &str, group: &str, text: &str| ListQuery {
            status: Some(status.into()),
            severity: Some(sev.into()),
            log_group: Some(group.into()),
            q: Some(text.into()),
        };
        assert!(matches(&row, &ListQuery::default()));
        assert!(matches(&row, &q("new,fixing", "", "", "")));
        assert!(!matches(&row, &q("ignored", "", "", "")));
        assert!(matches(
            &row,
            &q("", "warning", "/aws/lambda/shop-checkout", "TIMEOUT")
        ));
        assert!(!matches(&row, &q("", "critical", "", "")));
        assert!(!matches(&row, &q("", "", "/aws/lambda/other", "")));
        assert!(!matches(&row, &q("", "", "", "database")));
    }

    #[test]
    fn turning_fixes_on_stamps_the_time_once() {
        let off = AutoFix::default();
        let on = AutoFix {
            enabled: true,
            enabled_at: "client value".into(),
            ..Default::default()
        };
        let first = next_autofix(on.clone(), &off, "2026-10-22T00:00:00Z");
        assert_eq!(first.enabled_at, "2026-10-22T00:00:00Z");
        let again = next_autofix(on.clone(), &first, "2026-10-23T00:00:00Z");
        assert_eq!(again.enabled_at, "2026-10-22T00:00:00Z");
        let turned_off = next_autofix(off.clone(), &again, "2026-10-24T00:00:00Z");
        assert_eq!(turned_off.enabled_at, "");
    }

    #[test]
    fn refusals_answer_with_their_reason() {
        let (code, Json(body)) = skip_reply(&Skip::DailyCap);
        assert_eq!(
            (code, body["code"].as_str()),
            (StatusCode::TOO_MANY_REQUESTS, Some("daily_cap"))
        );
        assert_eq!(body["error"], "Daily cap reached.");
        let (code, Json(body)) = skip_reply(&Skip::AlreadyFixing);
        assert_eq!(code, StatusCode::CONFLICT);
        assert_eq!(body["code"], "already_fixing");
        let (code, Json(body)) = skip_reply(&Skip::Budget("monthly limit".into()));
        assert_eq!(code, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            body["error"],
            "The team's token limit was reached (monthly limit)."
        );
    }
}
